from __future__ import annotations

"""
utils_pkg/auto_exit_worker/exit_engine.py

God-Tier Auto Exit Worker — V3+++ LOCK-IN (production-first)
============================================================

ROLE (DEDUP-ALIGNED)
- utils_pkg/exit_engine.py is the *canonical exit brain* (decisions, orchestration).
- THIS module is the *canonical exit executor* (queue + crash-safe SELL execution).
- No independent “exit IQ” logic lives here — only execute requests.

Guarantees (HARD):
✅ Producer API signature-stable: enqueue_exit_request(...)
✅ Consumer never throws (per-item failures isolated)
✅ Always JSON-loggable payloads
✅ Crash safety: inflight reclaim + DONE idempotency
✅ Mint lock prevents concurrent sells on same mint
✅ Retry w/ exponential backoff + jitter + DLQ fail-fast
✅ Best-effort telemetry hooks (core_utils / data_logger / ai_db)
✅ Best-effort callbacks: ai_sell_logic.on_sell_filled and/or exit_engine.on_sell_filled

Contract/Test mode rules (CRITICAL):
- In pytest/contract/offline mode the worker must NEVER sleep or block.
  It must return immediately when the next item is not due yet.

Queue semantics (Redis):
  - EXIT_QUEUE_KEY:     ZSET rid -> score (due_ms + priority bias + tie-break)
  - EXIT_DATA_KEY:      HASH rid -> JSON payload
  - EXIT_INFLIGHT_KEY:  ZSET rid -> claimed_ms
  - EXIT_DONE_KEY:      ZSET rid -> done_ms (idempotency)
  - EXIT_DLQ_KEY:       LIST JSON payloads that exceeded retries or fail-fast

Notes:
- amount_raw is authoritative by default (computed upstream).
- sell_pct is informational unless meta explicitly requests worker-side scaling.
- This file is intentionally self-contained and uses only lazy imports for optional hooks.
"""

import asyncio
import contextlib
import inspect
import json
import os
import random
import secrets
import time
from dataclasses import dataclass, field
from typing import Any

# =============================================================================
# Truthy / falsy parsing
# =============================================================================
_TRUE = {"1", "true", "t", "yes", "y", "on"}
_FALSE = {"0", "false", "f", "no", "n", "off"}


def _in_pytest() -> bool:
    return bool(os.getenv("PYTEST_CURRENT_TEST"))


def _contract_no_sleep() -> bool:
    """
    Contract/offline mode => NEVER sleep or block.
    We treat these as "no sleep" environments:
      - pytest running
      - PYTEST_MODE=1
      - CONTRACT_TEST=1
      - NETWORK=OFF/0/false
      - NO_NETWORK=1
      - DISABLE_NETWORK=1
      - SAFE_MODE=1
    """
    if _in_pytest():
        return True
    if os.getenv("PYTEST_MODE", "").strip().lower() in _TRUE:
        return True
    if os.getenv("CONTRACT_TEST", "").strip() == "1":
        return True
    v = os.getenv("NETWORK", "").strip().lower()
    if v in _FALSE:
        return True
    if os.getenv("NO_NETWORK", "").strip().lower() in _TRUE:
        return True
    if os.getenv("DISABLE_NETWORK", "").strip().lower() in _TRUE:
        return True
    return os.getenv("SAFE_MODE", "").strip().lower() in _TRUE


# =============================================================================
# Logger (logging_setup compatible; safe fallback)
# =============================================================================
try:
    from logging_setup import get_logger  # type: ignore

    log = get_logger("auto_exit_worker.exit_engine")  # type: ignore
except Exception:  # pragma: no cover
    import logging

    log = logging.getLogger("auto_exit_worker.exit_engine")
    if not log.handlers:
        logging.basicConfig(level=logging.INFO)


def _install_log_shims() -> None:
    if not hasattr(log, "event"):

        def _event_fallback(name: str, **fields: Any) -> None:
            with contextlib.suppress(Exception):
                log.info("%s %s", name, fields)

        log.event = _event_fallback  # type: ignore[attr-defined]

    if not hasattr(log, "every"):

        def _every_fallback(key: str, n: int = 60, **fields: Any) -> None:
            now = time.time()
            store = getattr(_every_fallback, "_store", {})  # type: ignore[attr-defined]
            last = float(store.get(key, 0.0))
            if now - last >= float(n):
                store[key] = now
                _every_fallback._store = store  # type: ignore[attr-defined]
                with contextlib.suppress(Exception):
                    log.event(key, **fields)  # type: ignore[attr-defined]

        log.every = _every_fallback  # type: ignore[attr-defined]


_install_log_shims()


def _event(name: str, level: str = "info", **fields: Any) -> None:
    with contextlib.suppress(Exception):
        log.event(name, **fields)  # type: ignore[attr-defined]
        return
    msg = f"{name} {fields}"
    with contextlib.suppress(Exception):
        if level == "warning":
            log.warning(msg)
        elif level == "error":
            log.error(msg)
        else:
            log.info(msg)


def _short_exc(e: BaseException, n: int = 280) -> str:
    s = f"{type(e).__name__}: {e}"
    return (s[:n] + "…") if len(s) > n else s


# =============================================================================
# Env helpers
# =============================================================================
def _env_bool(key: str, default: bool = False) -> bool:
    v = os.getenv(key)
    if v is None:
        return default
    s = str(v).strip().lower()
    if s in _TRUE:
        return True
    if s in _FALSE:
        return False
    return default


def _env_int(key: str, default: int) -> int:
    try:
        return int(float(str(os.getenv(key, str(default))).strip()))
    except Exception:
        return default


def _env_float(key: str, default: float) -> float:
    try:
        return float(str(os.getenv(key, str(default))).strip())
    except Exception:
        return default


def _env_str(key: str, default: str = "") -> str:
    return (os.getenv(key, default) or default).strip()


def _clamp(x: float, lo: float, hi: float) -> float:
    return lo if x < lo else hi if x > hi else x


# =============================================================================
# Config (computed once; safe defaults)
# =============================================================================
EXIT_QUEUE_KEY = _env_str("EXIT_QUEUE_KEY", "exit_queue") or "exit_queue"
EXIT_DATA_KEY = (
    _env_str("EXIT_QUEUE_DATA_KEY", f"{EXIT_QUEUE_KEY}:data") or f"{EXIT_QUEUE_KEY}:data"
)
EXIT_INFLIGHT_KEY = (
    _env_str("EXIT_INFLIGHT_KEY", f"{EXIT_QUEUE_KEY}:inflight") or f"{EXIT_QUEUE_KEY}:inflight"
)
EXIT_DONE_KEY = _env_str("EXIT_DONE_KEY", f"{EXIT_QUEUE_KEY}:done") or f"{EXIT_QUEUE_KEY}:done"
EXIT_DLQ_KEY = _env_str("EXIT_DLQ_KEY", f"{EXIT_QUEUE_KEY}:dlq") or f"{EXIT_QUEUE_KEY}:dlq"

EXIT_MINT_LOCK_PREFIX = _env_str("EXIT_MINT_LOCK_PREFIX", "exit_lock") or "exit_lock"

AUTO_EXIT_WORKER_ENABLED = _env_bool("AUTO_EXIT_WORKER_ENABLED", False)
KILL_SWITCH = _env_bool("KILL_SWITCH", False)
DRYRUN = (
    _env_bool("DRYRUN", False) or _env_bool("DRY_RUN", False) or _env_bool("PAPER_TRADING", False)
)

# pacing
IDLE_SLEEP_S = max(0.05, _env_float("EXIT_WORKER_IDLE_SLEEP_S", 0.50))
ERR_SLEEP_S = max(0.10, _env_float("EXIT_WORKER_ERR_SLEEP_S", 1.00))
MAX_SLEEP_S = max(0.25, _env_float("EXIT_WORKER_MAX_SLEEP_S", 5.00))

# concurrency
WORKER_CONCURRENCY = max(1, _env_int("EXIT_WORKER_CONCURRENCY", 1))

# reclaim/visibility
VISIBILITY_TIMEOUT_S = max(5.0, _env_float("EXIT_VISIBILITY_TIMEOUT_S", 45.0))
REAP_EVERY_S = max(1.0, _env_float("EXIT_REAP_EVERY_S", 5.0))
REAP_BATCH = max(5, _env_int("EXIT_REAP_BATCH", 50))

# done retention
DONE_TTL_S = max(600.0, _env_float("EXIT_DONE_TTL_S", 21600.0))  # 6h default
DONE_PRUNE_EVERY_S = max(10.0, _env_float("EXIT_DONE_PRUNE_EVERY_S", 60.0))

# retries/jitter
MAX_ATTEMPTS = max(1, _env_int("EXIT_MAX_ATTEMPTS", 3))
RETRY_BASE_MS = max(50, _env_int("EXIT_RETRY_BASE_MS", 750))
RETRY_CAP_MS = max(RETRY_BASE_MS, _env_int("EXIT_RETRY_CAP_MS", 12000))
RETRY_JITTER_FRAC = _clamp(_env_float("EXIT_RETRY_JITTER_FRAC", 0.30), 0.0, 0.95)

# lock TTL/keepalive
MINT_LOCK_TTL_MS = max(2000, _env_int("EXIT_MINT_LOCK_TTL_MS", 20000))
LOCK_KEEPALIVE = _env_bool("EXIT_MINT_LOCK_KEEPALIVE", True)

# hard execution cap
EXEC_TIMEOUT_S = max(0.0, _env_float("EXIT_EXEC_TIMEOUT_S", 40.0))

# slippage defaults
DEFAULT_SLIPPAGE_BPS = _env_int("AI_SELL_SLIPPAGE_BPS", _env_int("PF_SLIPPAGE_BPS", 200))
DEFAULT_SLIPPAGE_BPS = max(5, min(5000, DEFAULT_SLIPPAGE_BPS))

# data cleanup
DELETE_DATA_ON_DONE = _env_bool("EXIT_DELETE_DATA_ON_DONE", True)

# fail-fast patterns (DLQ immediately)
FAIL_FAST_PATTERNS = [
    s.strip().lower() for s in _env_str("EXIT_FAIL_FAST_PATTERNS", "").split(",") if s.strip()
]

_SOL_MINT = "So11111111111111111111111111111111111111112"


# =============================================================================
# Lazy import helper (avoid cycles)
# =============================================================================
_MODS: dict[str, Any] = {}


def _get_mod(name: str) -> Any:
    if name in _MODS:
        return _MODS[name]
    try:
        import importlib

        m = importlib.import_module(name)
    except Exception:
        m = None
    _MODS[name] = m
    return m


# =============================================================================
# JSON safety (decision objects always loggable)
# =============================================================================
def _jsonable(x: Any, *, depth: int = 6, max_items: int = 200) -> Any:
    if depth <= 0:
        return str(x)
    if x is None or isinstance(x, str | int | float | bool):
        return x
    if isinstance(x, bytes | bytearray):
        return x[:256].decode("utf-8", "ignore")
    if isinstance(x, list | tuple | set):
        seq = list(x)
        out = [_jsonable(v, depth=depth - 1, max_items=max_items) for v in seq[:max_items]]
        if len(seq) > max_items:
            out.append(f"...(+{len(seq) - max_items} more)")
        return out
    if isinstance(x, dict):
        items = list(x.items())
        outd: dict[str, Any] = {}
        for k, v in items[:max_items]:
            outd[str(k)] = _jsonable(v, depth=depth - 1, max_items=max_items)
        if len(items) > max_items:
            outd["..."] = f"(+{len(items) - max_items} more)"
        return outd
    return str(x)


def _json_dumps(payload: Any) -> str:
    try:
        return json.dumps(_jsonable(payload), separators=(",", ":"), ensure_ascii=False)
    except Exception:
        return json.dumps({"_": str(payload)}, separators=(",", ":"), ensure_ascii=False)


def _json_loads_best(raw: str) -> Any:
    try:
        return json.loads(raw)
    except Exception:
        return None


# =============================================================================
# Core utils (redis, init, optional metrics)
# =============================================================================
cu = _get_mod("utils_pkg.core_utils")


async def _maybe_await(x: Any) -> Any:
    if asyncio.iscoroutine(x):
        return await x
    return x


async def _get_redis() -> Any:
    """
    Best-effort redis getter:
      - core_utils.get_redis_async()
      - core_utils.get_redis()
      - core_utils.redis
    """
    if not cu:
        return None
    for nm in ("get_redis_async", "get_redis"):
        fn = getattr(cu, nm, None)
        if callable(fn):
            with contextlib.suppress(Exception):
                out = fn()
                return await _maybe_await(out)
    return getattr(cu, "redis", None)


async def _init_resources_best_effort() -> None:
    if not cu:
        return
    fn = getattr(cu, "init_resources", None)
    if callable(fn):
        with contextlib.suppress(Exception):
            await _maybe_await(fn())


def _now_ms() -> int:
    return int(time.time() * 1000)


def _rid(ts_ms: int) -> str:
    return f"exit:{ts_ms}:{secrets.token_hex(6)}"


def _score(due_ms: int, priority: int, ts_ms: int) -> float:
    """
    ZSET score:
      - primary: due_ms
      - tiny bias: higher priority goes earlier (priority 0 best)
      - tiny tie-break: ts_ms to reduce reorder
    """
    pr = max(0, min(100, int(priority)))
    pr_bias = pr / 1000.0  # 0..0.1
    tie = float(int(ts_ms)) / 1e12  # tiny
    return float(int(due_ms)) - pr_bias + tie


def _is_fail_fast(err: str) -> bool:
    if not FAIL_FAST_PATTERNS:
        return False
    low = (err or "").lower()
    return any(p in low for p in FAIL_FAST_PATTERNS)


def _retry_delay_ms(attempts: int) -> int:
    raw = RETRY_BASE_MS * (2 ** max(0, attempts - 1))
    raw = min(raw, RETRY_CAP_MS)
    jitter = 1.0 + random.uniform(-RETRY_JITTER_FRAC, RETRY_JITTER_FRAC)
    return int(max(25, raw * jitter))


def _filter_kwargs(fn: Any, kwargs: dict[str, Any]) -> dict[str, Any]:
    try:
        sig = inspect.signature(fn)
        allowed = set(sig.parameters.keys())
        return {k: v for k, v in kwargs.items() if k in allowed}
    except Exception:
        return kwargs


# =============================================================================
# Data model
# =============================================================================
@dataclass
class ExitRequest:
    rid: str
    ts_ms: int
    due_ms: int
    mint: str
    amount_raw: int
    reason: str = "exit"
    sell_pct: float = 1.0
    symbol: str = ""
    priority: int = 5
    attempts: int = 0
    meta: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "rid": str(self.rid),
            "ts_ms": int(self.ts_ms),
            "due_ms": int(self.due_ms),
            "mint": str(self.mint),
            "amount_raw": int(self.amount_raw),
            "reason": str(self.reason),
            "sell_pct": float(self.sell_pct),
            "symbol": str(self.symbol),
            "priority": int(self.priority),
            "attempts": int(self.attempts),
            "meta": _jsonable(dict(self.meta or {})),
        }

    def to_json(self) -> str:
        return _json_dumps(self.to_dict())

    @staticmethod
    def from_json(raw: str) -> ExitRequest | None:
        d = _json_loads_best(raw)
        if not isinstance(d, dict):
            return None
        try:
            ts = int(d.get("ts_ms") or 0)
            due = int(d.get("due_ms") or ts or 0)
            mint = str(d.get("mint") or "").strip()
            rid = str(d.get("rid") or "").strip()
            amt = int(d.get("amount_raw") or 0)
            if not rid or not mint or amt <= 0:
                return None
            return ExitRequest(
                rid=rid,
                ts_ms=ts,
                due_ms=due,
                mint=mint,
                amount_raw=amt,
                reason=str(d.get("reason") or "exit"),
                sell_pct=float(d.get("sell_pct") or 1.0),
                symbol=str(d.get("symbol") or ""),
                priority=int(d.get("priority") or 5),
                attempts=int(d.get("attempts") or 0),
                meta=dict(d.get("meta") or {}),
            )
        except Exception:
            return None


# =============================================================================
# Redis helpers (best-effort, never throw)
# =============================================================================
def _decode(x: Any) -> str:
    if isinstance(x, bytes | bytearray):
        return x.decode("utf-8", "ignore")
    return str(x)


async def _hget_str(r: Any, key: str, field: str) -> str | None:
    try:
        v = await _maybe_await(r.hget(key, field))
        if v is None:
            return None
        return _decode(v)
    except Exception:
        return None


async def _hset_str(r: Any, key: str, field: str, value: str) -> None:
    with contextlib.suppress(Exception):
        await _maybe_await(r.hset(key, field, value))


async def _hdel(r: Any, key: str, field: str) -> None:
    with contextlib.suppress(Exception):
        await _maybe_await(r.hdel(key, field))


async def _zadd(r: Any, key: str, member: str, score: float) -> None:
    with contextlib.suppress(Exception):
        await _maybe_await(r.zadd(key, {member: float(score)}))


async def _zrem(r: Any, key: str, member: str) -> None:
    with contextlib.suppress(Exception):
        await _maybe_await(r.zrem(key, member))


async def _zscore(r: Any, key: str, member: str) -> float | None:
    try:
        sc = await _maybe_await(r.zscore(key, member))
        if sc is None:
            return None
        return float(sc)
    except Exception:
        return None


async def _zremrangebyscore(r: Any, key: str, min_s: str, max_s: int) -> int:
    try:
        n = await _maybe_await(r.zremrangebyscore(key, min_s, max_s))
        return int(n or 0)
    except Exception:
        return 0


# =============================================================================
# Atomic claim Lua (non-blocking)
# =============================================================================
_CLAIM_LUA = r"""
-- KEYS[1] = queue zset
-- KEYS[2] = inflight zset
-- ARGV[1] = now_ms
-- Returns:
--   nil                         if empty
--   {0, next_score}             if not due yet
--   {1, rid, score}             if claimed
local q = KEYS[1]
local infl = KEYS[2]
local now = tonumber(ARGV[1])

local a = redis.call('ZRANGE', q, 0, 0, 'WITHSCORES')
if (a == nil) or (#a == 0) then
  return nil
end

local rid = a[1]
local sc = tonumber(a[2])
if sc == nil then
  redis.call('ZREM', q, rid)
  return nil
end

if sc > now then
  return {0, sc}
end

local rem = redis.call('ZREM', q, rid)
if rem == 0 then
  return nil
end

redis.call('ZADD', infl, now, rid)
return {1, rid, sc}
"""


async def _claim_one(r: Any, now_ms: int) -> tuple[str, str | None, float | None]:
    """
    Returns:
      ("empty", None, None)
      ("not_due", None, next_score)
      ("claimed", rid, score)
    """
    try:
        res = await _maybe_await(
            r.eval(_CLAIM_LUA, 2, EXIT_QUEUE_KEY, EXIT_INFLIGHT_KEY, int(now_ms))
        )
        if res is None:
            return "empty", None, None
        if isinstance(res, list | tuple) and len(res) >= 2 and int(res[0]) == 0:
            return "not_due", None, float(res[1])
        if isinstance(res, list | tuple) and len(res) >= 3 and int(res[0]) == 1:
            return "claimed", _decode(res[1]), float(res[2])
    except Exception as e:
        _event("EXIT_CLAIM_ERR", level="warning", err=_short_exc(e))
    return "empty", None, None


async def _is_done(r: Any, rid: str) -> bool:
    return (await _zscore(r, EXIT_DONE_KEY, rid)) is not None


async def _mark_done(r: Any, rid: str, done_ms: int) -> None:
    await _zadd(r, EXIT_DONE_KEY, rid, float(done_ms))


async def _ack_inflight(r: Any, rid: str) -> None:
    await _zrem(r, EXIT_INFLIGHT_KEY, rid)


async def _push_dlq(r: Any, req: ExitRequest, *, err: str) -> None:
    payload = req.to_dict()
    payload["dlq_err"] = (err or "")[:700]
    payload["dlq_ts_ms"] = _now_ms()
    with contextlib.suppress(Exception):
        await _maybe_await(r.rpush(EXIT_DLQ_KEY, _json_dumps(payload)))
    _event("EXIT_TO_DLQ", level="warning", rid=req.rid, mint=req.mint, err=(err or "")[:240])


async def _load_req(r: Any, rid: str) -> ExitRequest | None:
    raw = await _hget_str(r, EXIT_DATA_KEY, rid)
    if not raw:
        return None
    req = ExitRequest.from_json(raw)
    if req and not req.rid:
        req.rid = rid
    return req


async def _requeue(r: Any, req: ExitRequest, *, delay_ms: int) -> None:
    now = _now_ms()
    req.due_ms = int(now + max(0, int(delay_ms)))
    sc = _score(req.due_ms, req.priority, req.ts_ms)
    await _hset_str(r, EXIT_DATA_KEY, req.rid, req.to_json())
    await _zadd(r, EXIT_QUEUE_KEY, req.rid, sc)
    _event(
        "EXIT_REENQUEUE",
        rid=req.rid,
        mint=req.mint,
        attempts=req.attempts,
        delay_ms=int(delay_ms),
        due_ms=req.due_ms,
    )


# =============================================================================
# Mint lock (prevents concurrent sells on same mint)
# =============================================================================
async def _acquire_mint_lock(r: Any, mint: str, rid: str) -> bool:
    """
    Fail-open: if Redis ops fail, do NOT block selling.
    """
    k = f"{EXIT_MINT_LOCK_PREFIX}:{mint}"
    try:
        ok = await _maybe_await(r.set(k, rid, nx=True, px=int(MINT_LOCK_TTL_MS)))
        return bool(ok)
    except Exception:
        return True


async def _release_mint_lock(r: Any, mint: str, rid: str) -> None:
    k = f"{EXIT_MINT_LOCK_PREFIX}:{mint}"
    try:
        cur = await _maybe_await(r.get(k))
        cur_s = _decode(cur) if cur is not None else ""
        if cur_s == rid:
            await _maybe_await(r.delete(k))
    except Exception:
        return


async def _mint_lock_keepalive_task(r: Any, mint: str, rid: str) -> None:
    # Contract mode should not spawn sleepers
    if _contract_no_sleep():
        return
    if not LOCK_KEEPALIVE:
        return
    k = f"{EXIT_MINT_LOCK_PREFIX}:{mint}"
    sleep_s = max(0.25, float(MINT_LOCK_TTL_MS) / 2000.0)  # ~half TTL
    while True:
        await asyncio.sleep(sleep_s)
        try:
            cur = await _maybe_await(r.get(k))
            cur_s = _decode(cur) if cur is not None else ""
            if cur_s != rid:
                return
            if hasattr(r, "pexpire"):
                await _maybe_await(r.pexpire(k, int(MINT_LOCK_TTL_MS)))
            else:
                await _maybe_await(r.expire(k, max(1, int(MINT_LOCK_TTL_MS / 1000))))
        except asyncio.CancelledError:
            raise
        except Exception:
            return


# =============================================================================
# Producer API (SIGNATURE-STABLE)
# =============================================================================
async def enqueue_exit_request(
    *,
    mint: str,
    amount_raw: int,
    reason: str = "exit",
    sell_pct: float = 1.0,
    symbol: str = "",
    priority: int = 5,
    meta: dict[str, Any] | None = None,
    **extra: Any,
) -> str | None:
    """
    Producer API used across the bot.

    Contract:
      - mint: required
      - amount_raw: required > 0 (authoritative by default)
      - reason/sell_pct/symbol/priority: optional
      - meta + extra: captured and stored (JSON-safe)
    """
    mint_s = str(mint or "").strip()
    if not mint_s:
        return None

    try:
        amt = int(amount_raw)
    except Exception:
        return None
    if amt <= 0:
        return None

    sp = float(sell_pct or 1.0)
    sp = _clamp(sp, 0.0, 1.0)

    ts = _now_ms()
    rid = _rid(ts)

    m = dict(meta or {})
    for k, v in (extra or {}).items():
        if k not in m:
            m[k] = v
    m_safe = _jsonable(m)

    req = ExitRequest(
        rid=rid,
        ts_ms=ts,
        due_ms=ts,
        mint=mint_s,
        amount_raw=amt,
        reason=str(reason or "exit"),
        sell_pct=sp,
        symbol=str(symbol or ""),
        priority=int(priority),
        attempts=0,
        meta=m_safe if isinstance(m_safe, dict) else {"meta": m_safe},
    )

    r = await _get_redis()
    if r is None:
        _event("EXIT_ENQUEUE_NO_REDIS", level="warning", mint=mint_s, rid=rid)
        return None

    await _hset_str(r, EXIT_DATA_KEY, rid, req.to_json())
    await _zadd(r, EXIT_QUEUE_KEY, rid, _score(req.due_ms, req.priority, req.ts_ms))

    _event(
        "EXIT_ENQUEUE_OK",
        rid=rid,
        mint=mint_s,
        amount_raw=amt,
        priority=req.priority,
        due_ms=req.due_ms,
    )
    return rid


# =============================================================================
# Execution path (sell)
# =============================================================================
def _should_apply_sell_pct(req: ExitRequest) -> bool:
    """
    Default: DO NOT scale amount_raw by sell_pct (amount_raw already exact).
    Enable worker-side scaling ONLY if meta explicitly requests it.
    """
    m = req.meta or {}
    for k in ("apply_sell_pct", "amount_raw_is_full_position", "worker_scale_amount"):
        v = str(m.get(k) or "").strip().lower()
        if v in _TRUE:
            return True
    return False


def _norm_slippage_bps(req: ExitRequest) -> int:
    m = req.meta or {}
    val = m.get("slippage_bps")
    if val is None:
        val = m.get("slippage")
    if val is None:
        val = DEFAULT_SLIPPAGE_BPS
    try:
        bps = int(float(str(val).strip()))
    except Exception:
        bps = int(DEFAULT_SLIPPAGE_BPS)
    return max(5, min(5000, bps))


async def _sell_via_lightning(
    req: ExitRequest, *, amount_raw: int, slippage_bps: int
) -> tuple[bool, str]:
    try:
        L = _get_mod("utils_pkg.lightning_impl")
        fn = getattr(L, "sell_via_lightning", None) if L else None
        if not callable(fn):
            return False, "missing lightning_impl.sell_via_lightning"
        kwargs = _filter_kwargs(
            fn, dict(mint=req.mint, amount_raw=int(amount_raw), slippage_bps=int(slippage_bps))
        )
        sig = await _maybe_await(fn(**kwargs))
        if sig:
            return True, str(sig)
        return False, "sell_via_lightning returned empty sig"
    except Exception as e:
        return False, f"sell_via_lightning error: {_short_exc(e)}"


async def _sell_via_router(
    req: ExitRequest, *, amount_raw: int, slippage_bps: int
) -> tuple[bool, str]:
    try:
        router = _get_mod("utils_pkg.router")
        if not router:
            return False, "missing utils_pkg.router"

        for name in ("sell_token_raw", "sell_token", "sell", "swap_sell", "execute_sell"):
            fn = getattr(router, name, None)
            if callable(fn):
                kwargs = _filter_kwargs(
                    fn,
                    dict(mint=req.mint, amount_raw=int(amount_raw), slippage_bps=int(slippage_bps)),
                )
                sig = await _maybe_await(fn(**kwargs))
                if sig:
                    return True, str(sig)

        fn2 = getattr(router, "swap", None)
        if callable(fn2):
            kwargs = dict(
                input_mint=req.mint,
                output_mint=_SOL_MINT,
                amount_raw=int(amount_raw),
                slippage_bps=int(slippage_bps),
                side="sell",
            )
            kwargs = _filter_kwargs(fn2, kwargs)
            sig = await _maybe_await(fn2(**kwargs))
            if sig:
                return True, str(sig)

        return False, "router had no supported sell method / returned empty sig"
    except Exception as e:
        return False, f"router sell error: {_short_exc(e)}"


async def _sell_via_pump_trader(
    req: ExitRequest, *, amount_raw: int, slippage_bps: int
) -> tuple[bool, str]:
    try:
        pump_trader = _get_mod("utils_pkg.pump_trader")
        if not pump_trader:
            return False, "missing utils_pkg.pump_trader"
        for name in ("sell_token_raw", "sell_token", "sell", "exit_position"):
            fn = getattr(pump_trader, name, None)
            if callable(fn):
                kwargs = _filter_kwargs(
                    fn,
                    dict(mint=req.mint, amount_raw=int(amount_raw), slippage_bps=int(slippage_bps)),
                )
                sig = await _maybe_await(fn(**kwargs))
                if sig:
                    return True, str(sig)
        return False, "pump_trader had no supported sell method / returned empty sig"
    except Exception as e:
        return False, f"pump_trader sell error: {_short_exc(e)}"


async def _execute_exit(req: ExitRequest) -> tuple[bool, str]:
    """
    Returns (ok, signature_or_error).
    """
    if DRYRUN:
        return True, "DRYRUN"

    await _init_resources_best_effort()

    amt_raw = int(req.amount_raw)
    if amt_raw <= 0:
        return False, "amount_raw<=0"

    if req.sell_pct < 1.0 and _should_apply_sell_pct(req):
        amt_raw = max(1, int(float(req.amount_raw) * float(req.sell_pct)))

    slippage_bps = _norm_slippage_bps(req)

    async def _run_chain() -> tuple[bool, str]:
        errors: list[str] = []

        ok, out = await _sell_via_lightning(req, amount_raw=amt_raw, slippage_bps=slippage_bps)
        if ok:
            return True, out
        errors.append(f"lightning: {out}")

        ok, out = await _sell_via_router(req, amount_raw=amt_raw, slippage_bps=slippage_bps)
        if ok:
            return True, out
        errors.append(f"router: {out}")

        ok, out = await _sell_via_pump_trader(req, amount_raw=amt_raw, slippage_bps=slippage_bps)
        if ok:
            return True, out
        errors.append(f"pump: {out}")

        err = " | ".join(errors)
        return False, (err[:800] + "…") if len(err) > 800 else err

    if EXEC_TIMEOUT_S and EXEC_TIMEOUT_S > 0:
        try:
            return await asyncio.wait_for(_run_chain(), timeout=float(EXEC_TIMEOUT_S))
        except TimeoutError:
            return False, f"execution timeout after {EXEC_TIMEOUT_S:.1f}s"
    return await _run_chain()


# =============================================================================
# Telemetry + callbacks (best-effort, never throw)
# =============================================================================
async def _notify_sell_filled(req: ExitRequest, sig: str) -> None:
    ev = {
        "mint": req.mint,
        "amount_raw": int(req.amount_raw),
        "sell_pct": float(req.sell_pct),
        "rid": req.rid,
        "signature": sig,
        "reason": req.reason,
        "symbol": req.symbol,
        "meta": _jsonable(dict(req.meta or {})),
        "ts": time.time(),
        "dryrun": int(bool(DRYRUN)),
        "source": "auto_exit_worker",
    }

    for mod_name, fn_name in (
        ("utils_pkg.ai_sell_logic", "on_sell_filled"),
        ("utils_pkg.exit_engine", "on_sell_filled"),
    ):
        with contextlib.suppress(Exception):
            m = _get_mod(mod_name)
            fn = getattr(m, fn_name, None) if m else None
            if callable(fn):
                await _maybe_await(fn(dict(ev)))


async def _telemetry(req: ExitRequest, ok: bool, sig_or_err: str) -> None:
    payload = {
        "type": "exit_result",
        "ok": int(bool(ok)),
        "rid": req.rid,
        "mint": req.mint,
        "reason": req.reason,
        "sig_or_err": str(sig_or_err)[:700],
        "attempts": int(req.attempts),
        "ts": time.time(),
        "meta": _jsonable(dict(req.meta or {})),
        "symbol": req.symbol,
        "sell_pct": float(req.sell_pct),
        "amount_raw": int(req.amount_raw),
        "dryrun": int(bool(DRYRUN)),
        "source": "auto_exit_worker",
    }

    if cu:
        fn = getattr(cu, "record_sell_result", None)
        if callable(fn):
            with contextlib.suppress(Exception):
                out = fn(
                    ok=bool(ok),
                    reason=str(req.reason),
                    mint=req.mint,
                    sig=(sig_or_err if ok else None),
                    meta=dict(payload),
                )
                await _maybe_await(out)

        inc = (
            getattr(cu, "inc_counter", None)
            or getattr(cu, "metric_inc", None)
            or getattr(cu, "metrics_inc", None)
        )
        if callable(inc):
            with contextlib.suppress(Exception):
                await _maybe_await(inc("exit_worker.exec_ok" if ok else "exit_worker.exec_fail", 1))

    with contextlib.suppress(Exception):
        dl = _get_mod("utils_pkg.data_logger")
        if dl:
            for nm in ("log_exit_result", "log_sell_result", "log_event"):
                fn = getattr(dl, nm, None)
                if callable(fn):
                    await _maybe_await(fn(dict(payload)))
                    break

    with contextlib.suppress(Exception):
        adb = _get_mod("utils_pkg.ai_db")
        if adb:
            for nm in ("log_event", "insert_event", "write_event"):
                fn = getattr(adb, nm, None)
                if callable(fn):
                    await _maybe_await(fn(dict(payload)))
                    break


# =============================================================================
# Consumer: process one (testable)
# =============================================================================
async def process_one(*, worker_id: str = "w0") -> bool:
    """
    Claims exactly one due request and processes it.
    Returns True if something was processed (success/failure/requeue/dlq),
    False if queue empty or next item not due yet.

    IMPORTANT:
      - In contract/offline/pytest mode: NEVER sleep or block.
    """
    r = await _get_redis()
    if r is None:
        return False

    now = _now_ms()
    status, rid, next_score = await _claim_one(r, now)

    if status == "empty":
        return False

    if status == "not_due":
        # ✅ CRITICAL FIX: never sleep in tests/contract/offline mode
        if _contract_no_sleep():
            return False

        if next_score is not None:
            wait_ms = max(1, int(float(next_score)) - int(now))
            await asyncio.sleep(min(MAX_SLEEP_S, max(0.001, wait_ms / 1000.0)))
        return False

    if status != "claimed" or not rid:
        return False

    req = await _load_req(r, rid)
    if req is None:
        await _ack_inflight(r, rid)
        _event("EXIT_PAYLOAD_MISSING", level="warning", rid=rid, worker=worker_id)
        return True

    req.mint = str(req.mint or "").strip()
    if not req.rid:
        req.rid = rid

    if not req.rid or not req.mint or int(req.amount_raw or 0) <= 0:
        await _ack_inflight(r, req.rid or rid)
        await _push_dlq(r, req, err="invalid payload fields")
        await _hdel(r, EXIT_DATA_KEY, req.rid or rid)
        return True

    if await _is_done(r, req.rid):
        await _ack_inflight(r, req.rid)
        if DELETE_DATA_ON_DONE:
            await _hdel(r, EXIT_DATA_KEY, req.rid)
        _event("EXIT_SKIP_DONE", rid=req.rid, mint=req.mint, worker=worker_id)
        return True

    got_lock = await _acquire_mint_lock(r, req.mint, req.rid)
    if not got_lock:
        await _ack_inflight(r, req.rid)
        req.attempts = int(req.attempts or 0)
        await _requeue(r, req, delay_ms=200 + random.randint(0, 350))
        return True

    _event(
        "EXIT_CLAIM_OK",
        rid=req.rid,
        mint=req.mint,
        reason=req.reason,
        attempts=int(req.attempts),
        priority=int(req.priority),
        worker=worker_id,
        dryrun=int(bool(DRYRUN)),
    )

    keepalive_task: asyncio.Task | None = None
    ok = False
    sig_or_err = ""

    try:
        # Do not spawn sleepers in contract mode
        if LOCK_KEEPALIVE and not _contract_no_sleep():
            keepalive_task = asyncio.create_task(
                _mint_lock_keepalive_task(r, req.mint, req.rid),
                name=f"exit_lock_keepalive:{req.mint}",
            )

        ok, sig_or_err = await _execute_exit(req)

    except asyncio.CancelledError:
        with contextlib.suppress(Exception):
            await _ack_inflight(r, req.rid)
            await _release_mint_lock(r, req.mint, req.rid)
            await _requeue(r, req, delay_ms=250 + random.randint(0, 250))
        raise
    except Exception as e:
        ok = False
        sig_or_err = _short_exc(e)
    finally:
        if keepalive_task is not None:
            keepalive_task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await keepalive_task

    with contextlib.suppress(Exception):
        await _telemetry(req, ok=ok, sig_or_err=sig_or_err)

    if ok:
        done_ms = _now_ms()
        await _ack_inflight(r, req.rid)
        await _mark_done(r, req.rid, done_ms)
        if DELETE_DATA_ON_DONE:
            await _hdel(r, EXIT_DATA_KEY, req.rid)

        await _release_mint_lock(r, req.mint, req.rid)

        _event(
            "EXIT_EXEC_OK", rid=req.rid, mint=req.mint, sig=str(sig_or_err)[:160], worker=worker_id
        )

        with contextlib.suppress(Exception):
            await _notify_sell_filled(req, str(sig_or_err))

        return True

    # failure
    req.attempts = int(req.attempts or 0) + 1
    err_str = str(sig_or_err or "unknown_error")[:900]

    _event(
        "EXIT_EXEC_FAIL",
        level="warning",
        rid=req.rid,
        mint=req.mint,
        err=err_str[:260],
        attempts=req.attempts,
        worker=worker_id,
    )

    await _ack_inflight(r, req.rid)
    await _release_mint_lock(r, req.mint, req.rid)

    if _is_fail_fast(err_str):
        await _push_dlq(r, req, err=err_str)
        await _hdel(r, EXIT_DATA_KEY, req.rid)
        return True

    if req.attempts < int(MAX_ATTEMPTS):
        delay = _retry_delay_ms(req.attempts)
        await _requeue(r, req, delay_ms=delay)
        return True

    await _push_dlq(r, req, err=err_str)
    await _hdel(r, EXIT_DATA_KEY, req.rid)
    return True


# Compatibility aliases (some code/tests look for these names)
async def run_once(*, worker_id: str = "w0") -> bool:
    return await process_one(worker_id=worker_id)


async def tick(*, worker_id: str = "w0") -> bool:
    return await process_one(worker_id=worker_id)


# =============================================================================
# Maintenance: reclaim stale inflight + prune done (crash safe)
# =============================================================================
async def reclaim_stale_inflight() -> int:
    r = await _get_redis()
    if r is None:
        return 0

    now = _now_ms()
    cutoff = now - int(VISIBILITY_TIMEOUT_S * 1000)
    reclaimed = 0

    try:
        stale = await _maybe_await(
            r.zrangebyscore(EXIT_INFLIGHT_KEY, "-inf", cutoff, start=0, num=int(REAP_BATCH))
        )
        if not stale:
            return 0

        for rid_b in stale:
            rid = _decode(rid_b)

            if await _is_done(r, rid):
                await _zrem(r, EXIT_INFLIGHT_KEY, rid)
                if DELETE_DATA_ON_DONE:
                    await _hdel(r, EXIT_DATA_KEY, rid)
                continue

            req = await _load_req(r, rid)

            await _zrem(r, EXIT_INFLIGHT_KEY, rid)

            if req is None:
                _event("EXIT_RECLAIM_MISSING_PAYLOAD", level="warning", rid=rid)
                continue

            req.attempts = int(req.attempts or 0) + 1
            delay = 250 + random.randint(0, 900)
            await _requeue(r, req, delay_ms=delay)
            reclaimed += 1

        if reclaimed:
            _event("EXIT_RECLAIMED", level="warning", count=reclaimed, cutoff_ms=int(cutoff))
    except Exception as e:
        _event("EXIT_RECLAIM_ERR", level="warning", err=_short_exc(e))

    return reclaimed


async def prune_done() -> int:
    r = await _get_redis()
    if r is None:
        return 0
    now = _now_ms()
    cutoff = now - int(DONE_TTL_S * 1000)
    return await _zremrangebyscore(r, EXIT_DONE_KEY, "-inf", cutoff)


# =============================================================================
# Worker loops
# =============================================================================
async def _runner_loop(worker_id: str) -> None:
    # In contract mode we never run infinite loops; process_one should be called directly.
    if _contract_no_sleep():
        return

    backoff = IDLE_SLEEP_S
    while True:
        try:
            did = await process_one(worker_id=worker_id)
            if not did:
                await asyncio.sleep(backoff)
                backoff = min(MAX_SLEEP_S, backoff * 1.15)
            else:
                backoff = IDLE_SLEEP_S
        except asyncio.CancelledError:
            raise
        except Exception as e:
            _event("EXIT_RUNNER_ERR", level="warning", err=_short_exc(e), worker=worker_id)
            await asyncio.sleep(ERR_SLEEP_S)


async def _maintenance_loop() -> None:
    if _contract_no_sleep():
        return

    last_reap = 0.0
    last_prune = 0.0
    while True:
        try:
            now = time.time()
            if now - last_reap >= REAP_EVERY_S:
                last_reap = now
                with contextlib.suppress(Exception):
                    await reclaim_stale_inflight()

            if now - last_prune >= DONE_PRUNE_EVERY_S:
                last_prune = now
                with contextlib.suppress(Exception):
                    n = await prune_done()
                    if n:
                        _event("EXIT_DONE_PRUNED", count=int(n))

            await asyncio.sleep(0.25)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            _event("EXIT_MAINT_ERR", level="warning", err=_short_exc(e))
            await asyncio.sleep(ERR_SLEEP_S)


async def worker_loop() -> None:
    """
    Main entry point for Render 24/7 worker.

    Contract/test safety:
      - If contract/offline/pytest mode is detected, we DO NOT run forever.
        We do a single maintenance tick + single process_one and return.
    """
    if KILL_SWITCH:
        _event("EXIT_WORKER_KILL_SWITCH", level="warning")
        return

    # Contract/test mode: never infinite loop
    if _contract_no_sleep():
        with contextlib.suppress(Exception):
            await reclaim_stale_inflight()
        with contextlib.suppress(Exception):
            await prune_done()
        with contextlib.suppress(Exception):
            await process_one(worker_id="contract")
        return

    if not AUTO_EXIT_WORKER_ENABLED:
        _event("EXIT_WORKER_DISABLED", key=EXIT_QUEUE_KEY)
        return

    _event(
        "EXIT_WORKER_START",
        key=EXIT_QUEUE_KEY,
        inflight=EXIT_INFLIGHT_KEY,
        done=EXIT_DONE_KEY,
        dlq=EXIT_DLQ_KEY,
        data=EXIT_DATA_KEY,
        dryrun=int(bool(DRYRUN)),
        concurrency=int(WORKER_CONCURRENCY),
        vis_timeout_s=float(VISIBILITY_TIMEOUT_S),
        max_attempts=int(MAX_ATTEMPTS),
        slippage_bps=int(DEFAULT_SLIPPAGE_BPS),
        delete_data_on_done=int(bool(DELETE_DATA_ON_DONE)),
        exec_timeout_s=float(EXEC_TIMEOUT_S),
        lock_keepalive=int(bool(LOCK_KEEPALIVE)),
        lock_ttl_ms=int(MINT_LOCK_TTL_MS),
    )

    maint = asyncio.create_task(_maintenance_loop(), name="exit_maint")
    runners = [
        asyncio.create_task(_runner_loop(f"w{i}"), name=f"exit_w{i}")
        for i in range(int(WORKER_CONCURRENCY))
    ]

    try:
        while True:
            log.every(  # type: ignore[attr-defined]
                "EXIT_WORKER_HEARTBEAT",
                30,
                enabled=int(bool(AUTO_EXIT_WORKER_ENABLED)),
                dryrun=int(bool(DRYRUN)),
                concurrency=int(WORKER_CONCURRENCY),
                queue=EXIT_QUEUE_KEY,
                inflight=EXIT_INFLIGHT_KEY,
                done=EXIT_DONE_KEY,
                dlq=EXIT_DLQ_KEY,
            )
            await asyncio.sleep(1.0)
    except asyncio.CancelledError:
        raise
    finally:
        for t in [maint, *runners]:
            t.cancel()
        with contextlib.suppress(Exception):
            await asyncio.gather(maint, *runners, return_exceptions=True)


__all__ = [
    "ExitRequest",
    "enqueue_exit_request",
    "process_one",
    "prune_done",
    "reclaim_stale_inflight",
    "run_once",
    "tick",
    "worker_loop",
]
