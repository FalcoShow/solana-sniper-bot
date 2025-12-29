# utils_pkg/auto_exit_worker/exit_engine.py
from __future__ import annotations

import asyncio
import contextlib
import json
import os
import random
import secrets
import time
from dataclasses import dataclass, field
from typing import Any, Dict, Optional, Tuple, Callable

from logging_setup import get_logger

log = get_logger(__name__)

# Prefer core_utils shared infra (TLS-safe redis, session, wallet, etc.)
from utils_pkg import core_utils as cu  # type: ignore


# =============================================================================
# God-Tier Auto Exit Worker — V3+++ LOCK-IN (Queue semantics + retries + jitter + no double-selling)
# =============================================================================
#
# DESIGN GOALS
# ------------
# 1) Reliable queue semantics:
#    - Main queue is a ZSET of rids (not raw JSON), scored by "due time" (delay/retries).
#    - Payload JSON stored in HASH keyed by rid.
#    - Claim uses an atomic Lua script: peek min, only claim if due, move to inflight ZSET.
#    - Inflight reaper returns stale claims back to queue (crash-safe).
#
# 2) No double-selling:
#    - Per-request idempotency via DONE_ZSET (rid -> done_ts).
#    - Per-mint lock (Redis SET NX with TTL) ensures only one sell per mint executes at a time.
#
# 3) Retries with jitter:
#    - Exponential backoff with jitter, cap, and attempt counter.
#    - After MAX_ATTEMPTS -> DLQ list (stores payload + error).
#
# 4) Best-effort integrations:
#    - Primary execution path: lightning_impl.sell_via_lightning
#    - Optional fallbacks: router / pump_trader if present.
#    - Best-effort callbacks: ai_sell_logic.on_sell_filled to release reservations.
#    - Best-effort metrics: core_utils.record_sell_result(...)
#
# SAFETY
# ------
# - Never raises to callers (worker loop is resilient).
# - If Redis is unavailable, enqueue returns None and worker sleeps/retries.
# - Amount selection:
#     * amount_raw is treated as authoritative by default (safe for ai_sell_logic which pre-computes).
#     * optional scaling by sell_pct only when explicitly indicated.
#


# =============================================================================
# Keys + flags
# =============================================================================
EXIT_QUEUE_KEY = (os.getenv("EXIT_QUEUE_KEY", "exit_queue") or "exit_queue").strip()
EXIT_QUEUE_DATA_KEY = (os.getenv("EXIT_QUEUE_DATA_KEY", f"{EXIT_QUEUE_KEY}:data") or f"{EXIT_QUEUE_KEY}:data").strip()

EXIT_INFLIGHT_KEY = (os.getenv("EXIT_INFLIGHT_KEY", f"{EXIT_QUEUE_KEY}:inflight") or f"{EXIT_QUEUE_KEY}:inflight").strip()
EXIT_DONE_KEY = (os.getenv("EXIT_DONE_KEY", f"{EXIT_QUEUE_KEY}:done") or f"{EXIT_QUEUE_KEY}:done").strip()
EXIT_DLQ_KEY = (os.getenv("EXIT_DLQ_KEY", f"{EXIT_QUEUE_KEY}:dlq") or f"{EXIT_QUEUE_KEY}:dlq").strip()

EXIT_MINT_LOCK_PREFIX = (os.getenv("EXIT_MINT_LOCK_PREFIX", "exit_lock") or "exit_lock").strip()

AUTO_EXIT_WORKER_ENABLED = (os.getenv("AUTO_EXIT_WORKER_ENABLED", "0") or "0").strip().lower() in {"1", "true", "yes", "y", "on"}
DRYRUN = (os.getenv("DRYRUN", "0") or "0").strip().lower() in {"1", "true", "yes", "y", "on"}

# Polling
POLL_IDLE_SLEEP_S = float(os.getenv("EXIT_WORKER_IDLE_SLEEP_S", "0.5") or 0.5)
POLL_ERR_SLEEP_S = float(os.getenv("EXIT_WORKER_ERR_SLEEP_S", "1.0") or 1.0)
POLL_MAX_SLEEP_S = float(os.getenv("EXIT_WORKER_MAX_SLEEP_S", "5.0") or 5.0)

# Claim / visibility
VISIBILITY_TIMEOUT_S = float(os.getenv("EXIT_VISIBILITY_TIMEOUT_S", "45") or 45.0)  # inflight stale claim -> requeue
REAP_EVERY_S = float(os.getenv("EXIT_REAP_EVERY_S", "5") or 5.0)
REAP_BATCH = int(float(os.getenv("EXIT_REAP_BATCH", "50") or 50))

# Idempotency retention
DONE_TTL_S = float(os.getenv("EXIT_DONE_TTL_S", "21600") or 21600.0)  # 6h
DONE_PRUNE_EVERY_S = float(os.getenv("EXIT_DONE_PRUNE_EVERY_S", "60") or 60.0)

# Trading config
DEFAULT_SLIPPAGE_BPS = int(float(os.getenv("AI_SELL_SLIPPAGE_BPS", os.getenv("PF_SLIPPAGE_BPS", "50")) or 50))
DEFAULT_SLIPPAGE_BPS = max(5, min(5000, DEFAULT_SLIPPAGE_BPS))
MAX_ATTEMPTS = int(float(os.getenv("EXIT_MAX_ATTEMPTS", "3") or 3))

# Backoff / jitter
RETRY_BASE_MS = int(float(os.getenv("EXIT_RETRY_BASE_MS", "750") or 750))
RETRY_CAP_MS = int(float(os.getenv("EXIT_RETRY_CAP_MS", "12000") or 12000))
RETRY_JITTER_FRAC = float(os.getenv("EXIT_RETRY_JITTER_FRAC", "0.30") or 0.30)

# Per-mint lock (prevents concurrent sells on same mint)
MINT_LOCK_TTL_MS = int(float(os.getenv("EXIT_MINT_LOCK_TTL_MS", "20000") or 20000))  # 20s

# Optional: fail-fast patterns (DLQ immediately)
FAIL_FAST_PATTERNS = [
    s.strip().lower()
    for s in (os.getenv("EXIT_FAIL_FAST_PATTERNS", "") or "").split(",")
    if s.strip()
]

# Optional: delete payload data after completion
DELETE_DATA_ON_DONE = (os.getenv("EXIT_DELETE_DATA_ON_DONE", "1") or "1").strip().lower() in {"1", "true", "yes", "y", "on"}


# =============================================================================
# Data model
# =============================================================================
@dataclass
class ExitRequest:
    rid: str
    ts_ms: int
    mint: str
    amount_raw: int
    reason: str = "exit"
    sell_pct: float = 1.0
    symbol: str = ""
    priority: int = 5
    meta: Dict[str, Any] = field(default_factory=dict)
    attempts: int = 0
    due_ms: int = 0  # next eligible time (ms)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "rid": self.rid,
            "ts_ms": int(self.ts_ms),
            "due_ms": int(self.due_ms or self.ts_ms),
            "mint": self.mint,
            "amount_raw": int(self.amount_raw),
            "reason": self.reason,
            "sell_pct": float(self.sell_pct),
            "symbol": self.symbol,
            "priority": int(self.priority),
            "meta": dict(self.meta or {}),
            "attempts": int(self.attempts),
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), separators=(",", ":"), ensure_ascii=False)

    @staticmethod
    def from_json(s: str) -> "ExitRequest":
        d = json.loads(s)
        rid = str(d.get("rid") or "")
        ts_ms = int(d.get("ts_ms") or 0)
        due_ms = int(d.get("due_ms") or ts_ms or 0)
        return ExitRequest(
            rid=rid,
            ts_ms=ts_ms,
            due_ms=due_ms,
            mint=str(d.get("mint") or ""),
            amount_raw=int(d.get("amount_raw") or 0),
            reason=str(d.get("reason") or "exit"),
            sell_pct=float(d.get("sell_pct") or 1.0),
            symbol=str(d.get("symbol") or ""),
            priority=int(d.get("priority") or 5),
            meta=dict(d.get("meta") or {}),
            attempts=int(d.get("attempts") or 0),
        )


def _now_ms() -> int:
    return int(time.time() * 1000)


def _score(due_ms: int, priority: int, ts_ms: int) -> float:
    """
    Score = due time, with tiny negative fractional bias for priority,
    and a tiny tie-breaker for FIFO-ish ordering.

    IMPORTANT:
    - due_ms dominates scheduling (retries/delays).
    - smaller priority executes slightly earlier when due times are same.
    - ts_ms tie-breaker is extremely tiny to reduce reorder.
    """
    # priority in [0..100], scaled to < 0.2 to avoid shifting due by meaningful ms
    pr_bias = float(max(0, min(100, int(priority)))) / 1000.0  # 0..0.1
    # make *smaller priority* earlier: subtract bias
    return float(int(due_ms)) - pr_bias + (float(int(ts_ms)) / 1e12)


def _log_info(event: str, **extra: Any) -> None:
    with contextlib.suppress(Exception):
        log.info(event, extra=extra)
        return
    with contextlib.suppress(Exception):
        log.info("%s %s", event, extra)


def _log_warn(event: str, **extra: Any) -> None:
    with contextlib.suppress(Exception):
        log.warning(event, extra=extra)
        return
    with contextlib.suppress(Exception):
        log.warning("%s %s", event, extra)


def _log_err(event: str, **extra: Any) -> None:
    with contextlib.suppress(Exception):
        log.error(event, extra=extra)
        return
    with contextlib.suppress(Exception):
        log.error("%s %s", event, extra)


def _short_err(e: BaseException, n: int = 240) -> str:
    s = f"{type(e).__name__}: {e}"
    return (s[:n] + "…") if len(s) > n else s


def _is_fail_fast(err: str) -> bool:
    if not FAIL_FAST_PATTERNS:
        return False
    low = (err or "").lower()
    return any(p in low for p in FAIL_FAST_PATTERNS)


# =============================================================================
# Redis helpers + Lua scripts
# =============================================================================
_CLAIM_LUA = r"""
-- KEYS[1] = queue zset
-- KEYS[2] = inflight zset
-- ARGV[1] = now_ms (number)
-- returns:
--   nil                      if empty
--   {0, next_score}          if not due yet (next_score > now)
--   {1, rid, score}          if claimed
local q = KEYS[1]
local infl = KEYS[2]
local now = tonumber(ARGV[1])

local a = redis.call('ZRANGE', q, 0, 0, 'WITHSCORES')
if (a == nil) or (#a == 0) then
  return nil
end

local rid = a[1]
local sc = tonumber(a[2])
if (sc == nil) then
  -- corrupt score: remove it and return nil
  redis.call('ZREM', q, rid)
  return nil
end

if sc > now then
  return {0, sc}
end

-- attempt to remove from queue; if raced, return nil
local rem = redis.call('ZREM', q, rid)
if rem == 0 then
  return nil
end

-- add to inflight with claimed timestamp score
redis.call('ZADD', infl, now, rid)
return {1, rid, sc}
"""

# Requeue stale inflight: move oldest claims back to queue (due now)
# We'll implement in Python for clarity (ZRANGEBYSCORE inflight <= now - vis, then ZREM+ZADD queue)


async def _redis() -> Any:
    return await cu.get_redis_async()


async def _zscore(r: Any, key: str, member: str) -> Optional[float]:
    with contextlib.suppress(Exception):
        sc = await r.zscore(key, member)
        if sc is None:
            return None
        return float(sc)
    return None


async def _hget_str(r: Any, key: str, field: str) -> Optional[str]:
    with contextlib.suppress(Exception):
        v = await r.hget(key, field)
        if v is None:
            return None
        if isinstance(v, (bytes, bytearray)):
            return v.decode("utf-8", "ignore")
        return str(v)
    return None


async def _hset_str(r: Any, key: str, field: str, value: str) -> None:
    with contextlib.suppress(Exception):
        await r.hset(key, field, value)


async def _hdel(r: Any, key: str, field: str) -> None:
    with contextlib.suppress(Exception):
        await r.hdel(key, field)


# =============================================================================
# Producer: enqueue (signature-stable + permissive extras)
# =============================================================================
async def enqueue_exit_request(
    *,
    mint: str,
    amount_raw: int,
    reason: str = "exit",
    sell_pct: float = 1.0,
    symbol: str = "",
    priority: int = 5,
    meta: Optional[Dict[str, Any]] = None,
    # accept extra fields (to keep wiring stable across modules)
    **extra: Any,
) -> Optional[str]:
    """
    Enqueue an exit request.
    Queue member is RID (string). Payload is stored in EXIT_QUEUE_DATA_KEY hash.

    IMPORTANT:
    - amount_raw should be the raw amount intended to sell (preferred).
    - sell_pct is informational unless meta indicates apply_sell_pct.
    """
    try:
        mint = str(mint or "").strip()
        if not mint:
            return None

        amt = int(amount_raw)
        if amt <= 0:
            return None

        sp = float(sell_pct or 1.0)
        if sp <= 0:
            return None
        if sp > 1.0:
            sp = 1.0

        ts = _now_ms()
        rid = f"exit:{ts}:{secrets.token_hex(6)}"
        due = ts

        m = dict(meta or {})
        # merge extra -> meta
        for k, v in (extra or {}).items():
            # avoid overwriting important keys unless missing
            if k not in m:
                m[k] = v

        req = ExitRequest(
            rid=rid,
            ts_ms=ts,
            due_ms=due,
            mint=mint,
            amount_raw=amt,
            reason=str(reason or "exit"),
            sell_pct=sp,
            symbol=str(symbol or ""),
            priority=int(priority),
            meta=m,
            attempts=0,
        )

        r = await _redis()
        if r is None:
            _log_warn("EXIT_ENQUEUE_NO_REDIS", rid=rid, mint=mint)
            return None

        await _hset_str(r, EXIT_QUEUE_DATA_KEY, rid, req.to_json())
        sc = _score(req.due_ms, req.priority, req.ts_ms)
        with contextlib.suppress(Exception):
            await r.zadd(EXIT_QUEUE_KEY, {rid: sc})

        _log_info(
            "EXIT_ENQUEUE_OK",
            rid=rid,
            mint=mint,
            key=EXIT_QUEUE_KEY,
            due_ms=req.due_ms,
            priority=req.priority,
        )
        return rid
    except Exception as e:
        _log_warn("EXIT_ENQUEUE_FAIL", err=_short_err(e), mint=str(mint)[:48])
        return None


# =============================================================================
# Claim / Ack / Requeue / DLQ
# =============================================================================
async def _claim_one(now_ms: int) -> Tuple[str, Optional[str], Optional[float]]:
    """
    Returns:
      ("empty", None, None)
      ("not_due", None, next_score)
      ("claimed", rid, score)
    """
    r = await _redis()
    if r is None:
        return "empty", None, None

    try:
        # eval on redis client
        res = await r.eval(_CLAIM_LUA, 2, EXIT_QUEUE_KEY, EXIT_INFLIGHT_KEY, int(now_ms))
        if res is None:
            return "empty", None, None
        if isinstance(res, (list, tuple)) and len(res) >= 2 and int(res[0]) == 0:
            return "not_due", None, float(res[1])
        if isinstance(res, (list, tuple)) and len(res) >= 3 and int(res[0]) == 1:
            rid = res[1]
            if isinstance(rid, (bytes, bytearray)):
                rid = rid.decode("utf-8", "ignore")
            return "claimed", str(rid), float(res[2])
    except Exception as e:
        _log_warn("EXIT_CLAIM_ERR", err=_short_err(e))
        return "empty", None, None

    return "empty", None, None


async def _ack_inflight(r: Any, rid: str) -> None:
    with contextlib.suppress(Exception):
        await r.zrem(EXIT_INFLIGHT_KEY, rid)


async def _mark_done(r: Any, rid: str, done_ms: int) -> None:
    with contextlib.suppress(Exception):
        await r.zadd(EXIT_DONE_KEY, {rid: float(done_ms)})


async def _is_done(r: Any, rid: str) -> bool:
    sc = await _zscore(r, EXIT_DONE_KEY, rid)
    return sc is not None


async def _push_dlq(r: Any, req: ExitRequest, *, err: str) -> None:
    payload = req.to_dict()
    payload["dlq_err"] = (err or "")[:500]
    payload["dlq_ts_ms"] = _now_ms()
    with contextlib.suppress(Exception):
        await r.rpush(EXIT_DLQ_KEY, json.dumps(payload, separators=(",", ":"), ensure_ascii=False))
    _log_warn("EXIT_TO_DLQ", rid=req.rid, mint=req.mint, err=(err or "")[:220])


def _retry_delay_ms(attempts: int) -> int:
    """
    attempts is 1..N for retries (after a failure).
    """
    # exponential: base * 2^(attempts-1)
    base = max(50, int(RETRY_BASE_MS))
    cap = max(base, int(RETRY_CAP_MS))
    raw = base * (2 ** max(0, attempts - 1))
    raw = min(raw, cap)
    # jitter +- jitter_frac
    jf = max(0.0, min(0.95, float(RETRY_JITTER_FRAC)))
    jitter = 1.0 + random.uniform(-jf, jf)
    return int(max(25, raw * jitter))


async def _requeue(r: Any, req: ExitRequest, *, delay_ms: int) -> None:
    now = _now_ms()
    req.due_ms = int(now + max(0, int(delay_ms)))
    sc = _score(req.due_ms, req.priority, req.ts_ms)

    # write updated payload then requeue
    await _hset_str(r, EXIT_QUEUE_DATA_KEY, req.rid, req.to_json())

    with contextlib.suppress(Exception):
        await r.zadd(EXIT_QUEUE_KEY, {req.rid: sc})

    _log_info("EXIT_REENQUEUE", rid=req.rid, mint=req.mint, attempts=req.attempts, due_ms=req.due_ms, delay_ms=int(delay_ms))


async def _load_req(r: Any, rid: str) -> Optional[ExitRequest]:
    raw = await _hget_str(r, EXIT_QUEUE_DATA_KEY, rid)
    if not raw:
        return None
    try:
        return ExitRequest.from_json(raw)
    except Exception:
        return None


# =============================================================================
# Per-mint lock (prevents concurrent sells / oversell races)
# =============================================================================
async def _acquire_mint_lock(r: Any, mint: str, rid: str) -> bool:
    k = f"{EXIT_MINT_LOCK_PREFIX}:{mint}"
    try:
        # aioredis-like: set(name, value, nx=True, px=ttl_ms)
        ok = await r.set(k, rid, nx=True, px=int(MINT_LOCK_TTL_MS))
        return bool(ok)
    except Exception:
        return True  # fail-open (don't block exits if lock op fails)


async def _release_mint_lock(r: Any, mint: str, rid: str) -> None:
    k = f"{EXIT_MINT_LOCK_PREFIX}:{mint}"
    # delete only if owned
    try:
        cur = await r.get(k)
        if isinstance(cur, (bytes, bytearray)):
            cur = cur.decode("utf-8", "ignore")
        if cur == rid:
            await r.delete(k)
    except Exception:
        return


# =============================================================================
# Execution path (sell)
# =============================================================================
def _should_apply_sell_pct(req: ExitRequest) -> bool:
    """
    Default behavior: DO NOT apply sell_pct again (amount_raw is authoritative),
    because ai_sell_logic already computes amount_raw to sell.

    If caller wants worker-side scaling, set one of:
      meta.apply_sell_pct = 1/true
      meta.amount_raw_is_full_position = 1/true
      meta.worker_scale_amount = 1/true
    """
    m = req.meta or {}
    for k in ("apply_sell_pct", "amount_raw_is_full_position", "worker_scale_amount"):
        v = m.get(k)
        if str(v).strip().lower() in {"1", "true", "yes", "y", "on"}:
            return True
    # If clearly produced by ai_sell_logic, we assume amount_raw is exact.
    src = str(m.get("source") or m.get("producer") or "")
    if "ai_sell_logic" in src:
        return False
    return False


async def _call_best_effort(fn: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
    res = fn(*args, **kwargs)
    if asyncio.iscoroutine(res):
        return await res
    return res


async def _execute_exit(req: ExitRequest) -> Tuple[bool, str]:
    """
    Executes the exit.

    Returns (ok, signature_or_error).
    In DRYRUN, returns ok immediately.
    """
    if DRYRUN:
        return True, "DRYRUN"

    # Ensure shared resources exist (wallet/session/etc.)
    await cu.init_resources()

    amt_raw = int(req.amount_raw)
    if amt_raw <= 0:
        return False, "amount_raw<=0"

    if float(req.sell_pct) < 1.0 and _should_apply_sell_pct(req):
        amt_raw = int(max(1, int(float(req.amount_raw) * float(req.sell_pct))))

    slippage_bps = int(req.meta.get("slippage_bps") or req.meta.get("slippage") or DEFAULT_SLIPPAGE_BPS)
    slippage_bps = max(5, min(5000, slippage_bps))

    # 1) lightning_impl.sell_via_lightning
    try:
        from utils_pkg import lightning_impl as L  # type: ignore

        fn = getattr(L, "sell_via_lightning", None)
        if callable(fn):
            sig = await _call_best_effort(
                fn,
                mint=req.mint,
                token_program=None,
                amount_raw=int(amt_raw),
                slippage_bps=int(slippage_bps),
            )
            if sig:
                return True, str(sig)
            return False, "sell_via_lightning returned empty sig"
    except Exception as e:
        return False, f"sell_via_lightning error: {str(e)[:220]}"

    # 2) router fallback (best-effort)
    try:
        from utils_pkg import router  # type: ignore

        for name in ("sell_token_raw", "sell_token", "sell", "swap_sell", "execute_sell"):
            fn = getattr(router, name, None)
            if callable(fn):
                sig = await _call_best_effort(fn, mint=req.mint, amount_raw=int(amt_raw), slippage_bps=int(slippage_bps))
                if sig:
                    return True, str(sig)
        # sometimes router exposes generic swap
        fn2 = getattr(router, "swap", None)
        if callable(fn2):
            sig = await _call_best_effort(
                fn2,
                input_mint=req.mint,
                output_mint="So11111111111111111111111111111111111111112",
                amount_raw=int(amt_raw),
                slippage_bps=int(slippage_bps),
                side="sell",
            )
            if sig:
                return True, str(sig)
    except Exception:
        pass

    # 3) pump_trader fallback (best-effort)
    try:
        from utils_pkg import pump_trader  # type: ignore

        for name in ("sell_token_raw", "sell_token", "sell", "exit_position"):
            fn = getattr(pump_trader, name, None)
            if callable(fn):
                sig = await _call_best_effort(fn, mint=req.mint, amount_raw=int(amt_raw), slippage_bps=int(slippage_bps))
                if sig:
                    return True, str(sig)
    except Exception:
        pass

    return False, "no supported exit path available"


async def _notify_sell_filled(req: ExitRequest, sig: str) -> None:
    """
    Best-effort hook: let ai_sell_logic release reserved_raw / close state.
    """
    try:
        from utils_pkg import ai_sell_logic  # type: ignore

        fn = getattr(ai_sell_logic, "on_sell_filled", None)
        if callable(fn):
            ev = {
                "mint": req.mint,
                "amount_raw": int(req.amount_raw),
                "sell_pct": float(req.sell_pct),
                "rid": req.rid,
                "signature": sig,
                "reason": req.reason,
                "symbol": req.symbol,
                "meta": dict(req.meta or {}),
                "ts": time.time(),
            }
            out = fn(ev)
            if asyncio.iscoroutine(out):
                await out
    except Exception:
        return


# =============================================================================
# Process one
# =============================================================================
async def process_one() -> bool:
    """
    Claims exactly one due request and processes it.
    Returns:
      - False if nothing processed (empty or not due)
      - True if processed (success/failure/requeue/dlq)
    """
    r = await _redis()
    if r is None:
        return False

    now = _now_ms()
    status, rid, next_score = await _claim_one(now)

    if status == "empty":
        return False

    if status == "not_due":
        # sleep until next due (bounded)
        if next_score is not None:
            # score is approx due_ms; add 1ms to avoid busy loop
            wait_ms = max(1, int(next_score) - int(now))
            await asyncio.sleep(min(float(POLL_MAX_SLEEP_S), max(0.001, wait_ms / 1000.0)))
        return False

    if status != "claimed" or not rid:
        return False

    # We have rid in inflight; load payload
    req = await _load_req(r, rid)
    if req is None:
        # payload missing/corrupt: ack inflight and DLQ stub
        await _ack_inflight(r, rid)
        _log_warn("EXIT_PAYLOAD_MISSING", rid=rid)
        return True

    # Hard validation
    req.mint = str(req.mint or "").strip()
    if not req.rid or not req.mint or int(req.amount_raw or 0) <= 0:
        await _ack_inflight(r, rid)
        await _push_dlq(r, req, err="invalid payload fields")
        await _hdel(r, EXIT_QUEUE_DATA_KEY, rid)
        return True

    # Idempotency: if already done, ack and drop
    if await _is_done(r, req.rid):
        await _ack_inflight(r, req.rid)
        if DELETE_DATA_ON_DONE:
            await _hdel(r, EXIT_QUEUE_DATA_KEY, req.rid)
        _log_info("EXIT_SKIP_DONE", rid=req.rid, mint=req.mint)
        return True

    # Mint lock (prevents concurrent sells on same mint)
    got_lock = await _acquire_mint_lock(r, req.mint, req.rid)
    if not got_lock:
        # requeue soon
        await _ack_inflight(r, req.rid)
        req.attempts = int(req.attempts or 0)
        await _requeue(r, req, delay_ms=200 + random.randint(0, 300))
        return True

    _log_info("EXIT_CLAIM_OK", rid=req.rid, mint=req.mint, reason=req.reason, attempts=req.attempts, priority=req.priority)

    ok = False
    sig_or_err = ""
    try:
        ok, sig_or_err = await _execute_exit(req)
    except asyncio.CancelledError:
        # Put it back quickly and re-raise
        with contextlib.suppress(Exception):
            await _ack_inflight(r, req.rid)
            await _release_mint_lock(r, req.mint, req.rid)
            await _requeue(r, req, delay_ms=250 + random.randint(0, 250))
        raise
    except Exception as e:
        ok = False
        sig_or_err = _short_err(e)

    # Best-effort outcome logging (won't crash worker)
    with contextlib.suppress(Exception):
        cu.record_sell_result(
            ok=bool(ok),
            reason=str(req.reason if ok else f"{req.reason} | FAIL"),
            mint=req.mint,
            sig=(sig_or_err if ok and sig_or_err else None),
            meta={
                **(req.meta or {}),
                "rid": req.rid,
                "symbol": req.symbol,
                "amount_raw": int(req.amount_raw),
                "sell_pct": float(req.sell_pct),
                "attempts": int(req.attempts),
                "dryrun": bool(DRYRUN),
            },
        )

    if ok:
        done_ms = _now_ms()
        await _ack_inflight(r, req.rid)
        await _mark_done(r, req.rid, done_ms)
        if DELETE_DATA_ON_DONE:
            await _hdel(r, EXIT_QUEUE_DATA_KEY, req.rid)

        await _release_mint_lock(r, req.mint, req.rid)

        _log_info("EXIT_EXEC_OK", rid=req.rid, mint=req.mint, sig=str(sig_or_err)[:120])

        # Best-effort: notify ai_sell_logic to release reserved_raw
        with contextlib.suppress(Exception):
            await _notify_sell_filled(req, str(sig_or_err))

        return True

    # Failure path
    req.attempts = int(req.attempts or 0) + 1
    err_str = str(sig_or_err or "unknown_error")[:500]
    _log_warn("EXIT_EXEC_FAIL", rid=req.rid, mint=req.mint, err=err_str[:220], attempts=req.attempts)

    await _ack_inflight(r, req.rid)
    await _release_mint_lock(r, req.mint, req.rid)

    # Fail-fast to DLQ (optional)
    if _is_fail_fast(err_str):
        await _push_dlq(r, req, err=err_str)
        await _hdel(r, EXIT_QUEUE_DATA_KEY, req.rid)
        return True

    if req.attempts < int(MAX_ATTEMPTS):
        delay = _retry_delay_ms(req.attempts)
        await _requeue(r, req, delay_ms=delay)
        return True

    await _push_dlq(r, req, err=err_str)
    await _hdel(r, EXIT_QUEUE_DATA_KEY, req.rid)
    return True


# =============================================================================
# Reaper: reclaim stale inflight + prune done
# =============================================================================
async def reclaim_stale_inflight() -> int:
    """
    Moves stale inflight rids back to queue (due now), preserving payload.
    Returns number reclaimed.
    """
    r = await _redis()
    if r is None:
        return 0

    now = _now_ms()
    cutoff = now - int(max(1, VISIBILITY_TIMEOUT_S) * 1000)
    reclaimed = 0

    try:
        # get up to batch stale inflight
        stale = await r.zrangebyscore(EXIT_INFLIGHT_KEY, "-inf", cutoff, start=0, num=int(REAP_BATCH))
        if not stale:
            return 0

        for rid_b in stale:
            rid = rid_b.decode("utf-8", "ignore") if isinstance(rid_b, (bytes, bytearray)) else str(rid_b)
            req = await _load_req(r, rid)
            # remove from inflight regardless
            with contextlib.suppress(Exception):
                await r.zrem(EXIT_INFLIGHT_KEY, rid)

            if req is None:
                _log_warn("EXIT_RECLAIM_MISSING_PAYLOAD", rid=rid)
                continue

            # bump attempts slightly (stuck execution)
            req.attempts = int(req.attempts or 0) + 1
            # requeue with small delay to avoid thrash
            delay = 250 + random.randint(0, 750)
            await _requeue(r, req, delay_ms=delay)
            reclaimed += 1

        if reclaimed:
            _log_warn("EXIT_RECLAIMED", count=reclaimed, cutoff_ms=int(cutoff))
        return reclaimed
    except Exception as e:
        _log_warn("EXIT_RECLAIM_ERR", err=_short_err(e))
        return 0


async def prune_done() -> int:
    """
    Prunes done records older than DONE_TTL_S.
    Returns number pruned (best-effort).
    """
    r = await _redis()
    if r is None:
        return 0
    try:
        now = _now_ms()
        cutoff = now - int(max(60.0, DONE_TTL_S) * 1000)
        # remove by score
        n = await r.zremrangebyscore(EXIT_DONE_KEY, "-inf", cutoff)
        return int(n or 0)
    except Exception:
        return 0


# =============================================================================
# Worker loop
# =============================================================================
async def worker_loop() -> None:
    """
    Main 24/7 loop. Safe to run as an asyncio Task.
    """
    if not AUTO_EXIT_WORKER_ENABLED:
        _log_info("EXIT_WORKER_DISABLED", key=EXIT_QUEUE_KEY)
        return

    _log_info(
        "EXIT_WORKER_START",
        key=EXIT_QUEUE_KEY,
        dryrun=bool(DRYRUN),
        vis_timeout_s=float(VISIBILITY_TIMEOUT_S),
        max_attempts=int(MAX_ATTEMPTS),
        slippage_bps=int(DEFAULT_SLIPPAGE_BPS),
    )

    last_reap = 0.0
    last_prune = 0.0

    while True:
        try:
            now_s = time.time()

            # periodic reaper + prune
            if now_s - last_reap >= float(REAP_EVERY_S):
                last_reap = now_s
                with contextlib.suppress(Exception):
                    await reclaim_stale_inflight()

            if now_s - last_prune >= float(DONE_PRUNE_EVERY_S):
                last_prune = now_s
                with contextlib.suppress(Exception):
                    await prune_done()

            did = await process_one()
            if not did:
                await asyncio.sleep(float(POLL_IDLE_SLEEP_S))

        except asyncio.CancelledError:
            _log_info("EXIT_WORKER_STOP")
            raise
        except Exception as e:
            _log_warn("EXIT_WORKER_ERR", err=_short_err(e))
            await asyncio.sleep(float(POLL_ERR_SLEEP_S))
