from __future__ import annotations

"""
utils_pkg/velocity_worker.py (P0 — production, God-Tier Overhaul)
================================================================

Purpose
-------
"Velocity" detects short-window acceleration candidates (volume + price + activity).

DEFAULT (recommended):
  - QUEUE mode: publish candidates to Redis for the canonical entry pipeline.

OPTIONAL:
  - TRADE mode: execute buys directly via Jupiter (only if VELOCITY_MODE=TRADE).

P0 Lock-in Principles
---------------------
- Safe-by-default (no trading unless explicitly enabled)
- No network I/O at import time (aiohttp only imported at runtime)
- Optional integrations are lazy + best-effort (never crash the worker):
    * risk_manager gate + ticket sizing
    * Redis distributed cooldown + lock
    * router_guard / routeguard preflight checks
    * jup_quote_cache prewarming
    * core_utils Jupiter swap + Telegram notifications
- Graceful shutdown (SIGINT/SIGTERM where supported)
- Observability: structured logger if available + best-effort event emission

Public API (LOCKED)
-------------------
- VelocityCandidate
- run_velocity(stop_event: asyncio.Event) -> None
- main() -> None
"""

import asyncio
import contextlib
import dataclasses
import inspect
import json
import logging
import os
import random
import signal
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Protocol, TypeVar

T = TypeVar("T")

# =============================================================================
# Optional import + callable resolution (cached; safe)
# =============================================================================
_MOD_CACHE: dict[str, Any] = {}
_CALL_CACHE: dict[tuple[str, tuple[str, ...]], Callable[..., Any] | None] = {}
_TMOD_CACHE: dict[str, Any] = {}


def _import_optional(module: str) -> Any | None:
    if module in _MOD_CACHE:
        return _MOD_CACHE[module]
    try:
        import importlib

        mod = importlib.import_module(module)
    except Exception:
        mod = None
    _MOD_CACHE[module] = mod
    return mod


def _get_callable(module: str, names: tuple[str, ...]) -> Callable[..., Any] | None:
    key = (module, names)
    if key in _CALL_CACHE:
        return _CALL_CACHE[key]
    mod = _import_optional(module)
    if mod is None:
        _CALL_CACHE[key] = None
        return None
    for name in names:
        fn = getattr(mod, name, None)
        if callable(fn):
            _CALL_CACHE[key] = fn
            return fn
    _CALL_CACHE[key] = None
    return None


def _filter_kwargs(fn: Callable[..., Any], kwargs: dict[str, Any]) -> dict[str, Any]:
    try:
        sig = inspect.signature(fn)
        allowed = set(sig.parameters.keys())
        return {k: v for k, v in kwargs.items() if k in allowed}
    except Exception:
        return kwargs


async def _maybe_await(x: Any) -> Any:
    return await x if asyncio.iscoroutine(x) else x


# =============================================================================
# Logging + telemetry (best-effort)
# =============================================================================
def _mk_logger() -> Any:
    # Prefer canonical get_logger if present.
    for mod_name in ("utils_pkg.logging_setup", "logging_setup"):
        mod = _import_optional(mod_name)
        if mod is None:
            continue
        fn = getattr(mod, "get_logger", None)
        if callable(fn):
            with contextlib.suppress(Exception):
                return fn("velocity")  # type: ignore[misc]
    lg = logging.getLogger("velocity")
    if not lg.handlers:
        logging.basicConfig(level=logging.INFO)
    return lg


log = _mk_logger()


def _telemetry() -> Any | None:
    # Lazy import + cache.
    if "telemetry" in _TMOD_CACHE:
        return _TMOD_CACHE["telemetry"]
    mod = _import_optional("utils_pkg.telemetry")
    _TMOD_CACHE["telemetry"] = mod
    return mod


def _emit(name: str, **fields: Any) -> None:
    """
    Best-effort event emission.
    Prefers utils_pkg.telemetry.event if available; falls back to logger.
    Never raises.
    """
    try:
        tmod = _telemetry()
        if tmod is not None:
            fn = getattr(tmod, "event", None)
            if callable(fn):
                with contextlib.suppress(Exception):
                    fn(name, **fields)
                    return
    except Exception:
        pass

    with contextlib.suppress(Exception):
        if hasattr(log, "event") and callable(log.event):
            log.event(name, **fields)  # type: ignore[attr-defined]
            return
    with contextlib.suppress(Exception):
        log.info("%s %s", name, fields)


# -----------------------------------------------------------------------------
# Mypy-safe rate limit + cooldown caches (module-level, typed)
# -----------------------------------------------------------------------------
_EMIT_EVERY_STORE: dict[str, float] = {}
_COOLDOWN_MEM: dict[str, int] = {}


def _emit_every(key: str, seconds: int = 30, msg: str | None = None, **fields: Any) -> None:
    """
    Rate-limited event emission (fallback when no logger.every).

    NOTE: Uses a module-level cache (not function attributes) for mypy correctness.
    """
    try:
        if hasattr(log, "every") and callable(log.every):
            log.every(key, seconds, msg=msg, **fields)  # type: ignore[attr-defined]
            return
    except Exception:
        pass

    now_mono = time.monotonic()
    last = float(_EMIT_EVERY_STORE.get(key, 0.0))
    if (now_mono - last) >= float(max(1, int(seconds))):
        _EMIT_EVERY_STORE[key] = now_mono
        _emit(msg or key, **fields)


# =============================================================================
# Env helpers (safe)
# =============================================================================
_TRUE = {"1", "true", "t", "yes", "y", "on"}
_FALSE = {"0", "false", "f", "no", "n", "off"}


def _envs(name: str, default: str = "") -> str:
    raw = os.getenv(name)
    return default if raw is None else str(raw).strip()


def _envb(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None or str(raw).strip() == "":
        return bool(default)
    s = str(raw).strip().lower()
    if s in _TRUE:
        return True
    if s in _FALSE:
        return False
    return bool(default)


def _envi(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None:
        return int(default)
    s = str(raw).strip()
    if s == "":
        return int(default)
    try:
        return int(float(s))
    except Exception:
        return int(default)


def _envf(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None:
        return float(default)
    s = str(raw).strip()
    if s == "":
        return float(default)
    try:
        return float(s)
    except Exception:
        return float(default)


# =============================================================================
# Redis typing (minimal)
# =============================================================================
class RedisLike(Protocol):
    async def get(self, key: str) -> Any: ...
    async def set(self, key: str, value: Any, **kwargs: Any) -> Any: ...
    async def delete(self, key: str) -> Any: ...
    async def rpush(self, key: str, value: Any) -> Any: ...
    async def expire(self, key: str, ttl: int) -> Any: ...


# =============================================================================
# Constants / config (env)
# =============================================================================
SOL_MINT = "So11111111111111111111111111111111111111112"

VELOCITY_ENABLED = _envb("VELOCITY_ENABLED", True)

# SAFE DEFAULT: QUEUE mode (no direct trading).
VELOCITY_MODE = _envs("VELOCITY_MODE", "QUEUE").upper()
if VELOCITY_MODE not in ("QUEUE", "TRADE"):
    VELOCITY_MODE = "QUEUE"

POLL_SEC = max(3, _envi("VELOCITY_POLL_SEC", 20))
JITTER_SEC = max(0.0, _envf("VELOCITY_JITTER_SEC", 1.0))

COOLDOWN_SEC = max(0, _envi("VELOCITY_COOLDOWN_SEC", 300))
LOCK_TTL_SEC = max(10, _envi("VELOCITY_LOCK_TTL_SEC", 120))

MAX_SCAN_PAIRS = max(10, _envi("VELOCITY_MAX_SCAN_PAIRS", _envi("VELOCITY_MAX_CANDIDATES", 120)))
MAX_EMIT_PER_TICK = max(1, _envi("VELOCITY_MAX_EMIT_PER_TICK", 25))
MAX_TRADES_PER_TICK = max(1, _envi("VELOCITY_MAX_TRADES_PER_POLL", 1))

# Velocity gates (compat aliases)
ACCEL_MIN = _envf("VELOCITY_ACCEL_MIN", _envf("VELOCITY_VOL_RATIO_MIN", 2.0))
PRICE_CHANGE_M5_MIN = _envf("VELOCITY_PRICE_CHANGE_MIN", 2.0)
BUYS_M5_MIN = max(0, _envi("VELOCITY_BUYS5M_MIN", 6))
SCORE_MIN = _envf("VELOCITY_SCORE_MIN", 0.65)

# Shared gates
MIN_LIQ_USD = _envf("VELOCITY_MIN_LIQ_USD", _envf("AI_MIN_LIQ_USD", 20_000.0))
MAX_IMPACT_BPS = _envf("VELOCITY_MAX_IMPACT_BPS", _envf("AI_MAX_IMPACT_BPS", 150.0))

# Slippage
JUPITER_MAX_SLIPPAGE = _envf("JUPITER_MAX_SLIPPAGE", _envf("JUPITER_SLIPPAGE", 0.005))
SLIPPAGE_BPS = max(1, int(JUPITER_MAX_SLIPPAGE * 10_000))

# Queue publishing
REDIS_QUEUE_KEY = _envs("VELOCITY_QUEUE_KEY", "entry_queue:velocity")
REDIS_COOLDOWN_KEY_PREFIX = _envs("VELOCITY_COOLDOWN_KEY_PREFIX", "cd:velocity:")
REDIS_LOCK_KEY_PREFIX = _envs("VELOCITY_LOCK_KEY_PREFIX", "lock:velocity:")

# Quote prewarm
PREWARM_ENABLED = _envb("VELOCITY_PREWARM_ENABLED", True)
PREWARM_TOP_N = max(0, _envi("VELOCITY_PREWARM_TOP_N", 8))

# Notifications
TELEGRAM_ON_START = _envb("VELOCITY_TELEGRAM_ON_START", True)
TELEGRAM_ON_CANDIDATE = _envb("VELOCITY_TELEGRAM_ON_CANDIDATE", False)
TELEGRAM_ON_TRADE = _envb("VELOCITY_TELEGRAM_ON_TRADE", True)

DEX_SEARCH_API = _envs(
    "VELOCITY_DEX_SEARCH_API",
    "https://api.dexscreener.com/latest/dex/search?q=sol",
)


# =============================================================================
# Data model
# =============================================================================
@dataclass(frozen=True, slots=True)
class VelocityCandidate:
    ts: float
    source: str
    mint: str
    symbol: str
    pair_url: str

    liquidity_usd: float
    vol_m5_usd: float
    vol_m30_usd: float
    accel: float
    price_change_m5: float
    buys_m5: int
    sells_m5: int

    score: float
    est_impact_bps: float

    raw: dict[str, Any]


# =============================================================================
# Small numeric helpers
# =============================================================================
def _safe_float(x: Any, default: float = 0.0) -> float:
    try:
        return float(x)
    except Exception:
        return float(default)


def _safe_int(x: Any, default: int = 0) -> int:
    try:
        return int(x)
    except Exception:
        return int(default)


def _impact_bps(liq_usd: float, ticket_usd: float) -> float:
    if liq_usd <= 0.0:
        return 9e9
    return (float(ticket_usd) / float(liq_usd)) * 10_000.0


def _compute_score(accel: float, pct_m5: float, buys_m5: int) -> float:
    # accel gate mapping
    a = max(0.0, min(1.0, (float(accel) - float(ACCEL_MIN)) / max(1e-9, float(ACCEL_MIN) * 2.0)))
    # +5m pct mapping
    p = max(0.0, min(1.0, (float(pct_m5) - float(PRICE_CHANGE_M5_MIN)) / 20.0))
    # buys mapping
    b = max(0.0, min(1.0, (float(buys_m5) - float(BUYS_M5_MIN)) / 30.0))
    s = 0.45 * a + 0.35 * p + 0.20 * b
    return max(0.0, min(1.0, s))


def _now_ts() -> float:
    return time.time()


def _lamports(sol: float) -> int:
    return int(max(0.0, float(sol)) * 1_000_000_000.0)


# =============================================================================
# Redis compatibility helpers (works with redis-py asyncio + aioredis-ish)
# =============================================================================
async def _redis_set(
    r: RedisLike, key: str, value: str, *, ex: int | None = None, nx: bool = False
) -> bool:
    try:
        out = await _maybe_await(r.set(key, value, ex=ex, nx=nx))
        return bool(out) or out is None or out == "OK"
    except TypeError:
        # Some clients don't accept ex/nx kwargs.
        out2 = await _maybe_await(r.set(key, value))
        if ex is not None:
            with contextlib.suppress(Exception):
                await _maybe_await(r.expire(key, int(ex)))
        return bool(out2) or out2 is None or out2 == "OK"
    except Exception:
        return False


async def _cooldown_ok(r: RedisLike | None, mint: str) -> bool:
    """
    Per-mint cooldown across workers (Redis) or local fallback (memory).

    NOTE: Local fallback uses a module-level dict for mypy correctness.
    """
    if COOLDOWN_SEC <= 0:
        return True

    now = int(_now_ts())

    if r is None:
        last = int(_COOLDOWN_MEM.get(mint, 0))
        if (now - last) >= COOLDOWN_SEC:
            _COOLDOWN_MEM[mint] = now
            return True
        return False

    key = f"{REDIS_COOLDOWN_KEY_PREFIX}{mint}"
    return bool(await _redis_set(r, key, str(now), ex=COOLDOWN_SEC, nx=True))


async def _try_lock(r: RedisLike | None, mint: str) -> bool:
    if r is None:
        return True
    key = f"{REDIS_LOCK_KEY_PREFIX}{mint}"
    return bool(await _redis_set(r, key, "1", ex=LOCK_TTL_SEC, nx=True))


async def _unlock(r: RedisLike | None, mint: str) -> None:
    if r is None:
        return
    key = f"{REDIS_LOCK_KEY_PREFIX}{mint}"
    with contextlib.suppress(Exception):
        await _maybe_await(r.delete(key))


# =============================================================================
# Optional dependency adapters (lazy, best-effort)
# =============================================================================
async def _init_resources_best_effort() -> None:
    fn = _get_callable("utils_pkg.core_utils", ("init_resources",))
    if callable(fn):
        with contextlib.suppress(Exception):
            await _maybe_await(fn())


async def _close_resources_best_effort() -> None:
    fn = _get_callable("utils_pkg.core_utils", ("close_resources",))
    if callable(fn):
        with contextlib.suppress(Exception):
            await _maybe_await(fn())


async def _redis() -> RedisLike | None:
    fn = _get_callable(
        "utils_pkg.core_utils",
        ("get_redis_async", "get_redis", "get_redis_client", "get_redis_pool"),
    )
    if fn is None:
        return None
    try:
        r = await _maybe_await(fn())
        return r  # type: ignore[return-value]
    except Exception:
        _emit_every("REDIS_GET_FAIL", 20, msg="REDIS_GET_FAIL")
        return None


async def _tg(msg: str) -> None:
    fn = _get_callable("utils_pkg.core_utils", ("send_telegram", "telegram_send"))
    if fn is None:
        _emit_every("TG_DISABLED", 60, msg="TG_DISABLED", preview=msg[:200])
        return
    try:
        await _maybe_await(fn(msg))
    except Exception:
        _emit_every("TG_FAIL", 30, msg="TG_FAIL")


async def _usd_per_sol() -> float:
    fn = _get_callable("utils_pkg.core_utils", ("convert_sol_to_usd", "sol_to_usd"))
    if fn is None:
        return 0.0
    try:
        out = await _maybe_await(fn(1.0))
        return float(out or 0.0)
    except Exception:
        return 0.0


async def _allow_new_entry(r: RedisLike | None) -> bool:
    fn = _get_callable("utils_pkg.risk_manager", ("should_allow_new_entry",))
    if fn is None:
        return True
    try:
        try:
            out = fn(r)
        except TypeError:
            out = fn()
        out = await _maybe_await(out)
        return bool(out)
    except Exception:
        # Fail-open to avoid deadlocking the system.
        return True


async def _ticket_usd(r: RedisLike | None) -> float:
    fallback = _envf("VELOCITY_TICKET_USD", _envf("BUY_TICKET_USD", _envf("PF_TICKET_USD", 20.0)))
    fn = _get_callable("utils_pkg.risk_manager", ("current_limits",))
    if fn is None:
        return float(max(0.5, min(fallback, 500.0)))

    try:
        try:
            out = fn(r)
        except TypeError:
            out = fn()
        out = await _maybe_await(out)
        if isinstance(out, dict):
            t = out.get("ticket_usd", fallback)
            return float(max(0.5, min(float(t), 500.0)))
    except Exception:
        pass

    return float(max(0.5, min(fallback, 500.0)))


async def _note_spend(r: RedisLike | None, usd: float) -> None:
    fn = _get_callable("utils_pkg.risk_manager", ("note_spend",))
    if fn is None:
        return
    try:
        try:
            out = fn(r, float(usd))
        except TypeError:
            out = fn(float(usd))
        await _maybe_await(out)
    except Exception:
        _emit_every("NOTE_SPEND_FAIL", 30, msg="NOTE_SPEND_FAIL")


async def _prewarm_quotes_best_effort(mints: list[str], amount_sol: float) -> None:
    if not PREWARM_ENABLED or PREWARM_TOP_N <= 0:
        return
    fn = _get_callable("jup_quote_cache", ("prewarm_quotes",))
    if fn is None:
        return
    try:
        await _maybe_await(fn(mints, float(amount_sol), int(SLIPPAGE_BPS)))
        _emit_every("PREWARM_OK", 30, msg="PREWARM_OK", mints_count=len(mints))
    except Exception:
        _emit_every("PREWARM_FAIL", 30, msg="PREWARM_FAIL")


async def _router_guard_allows_trade(
    mint: str, lamports_in: int
) -> tuple[bool, str, dict[str, Any]]:
    """
    Prefer utils_pkg.router_guard.preflight() if present.
    Interprets contract:
      (False, None) -> blocked
      (True, None)  -> fallback-only allowed (NOT ok for TRADE mode)
      (True, dict)  -> allowed for Jupiter
    """
    fn = _get_callable("utils_pkg.router_guard", ("preflight",))
    if fn is None:
        return True, "router_guard_missing", {}

    try:
        out = fn(mint=mint, lamports_in=int(lamports_in))
        out = await _maybe_await(out)
        if isinstance(out, tuple) and len(out) == 2:
            ok = bool(out[0])
            q = out[1]
            if not ok:
                return False, "router_guard_blocked", {}
            if q is None:
                return False, "router_guard_fallback_only", {}
            if isinstance(q, dict):
                return True, "router_guard_ok", dict(q)
            return True, "router_guard_ok_non_dict", {}
        if isinstance(out, bool):
            return bool(out), "router_guard_bool", {}
        return True, "router_guard_unknown_return", {}
    except Exception as err:
        # Fail-open (visibility only). The TRADE path will still be guarded by swaps failing if broken.
        return True, f"router_guard_error_ignored:{type(err).__name__}", {}


async def _routeguard_posttrade_watchdog(
    mint: str, amount_sol: float, meta: dict[str, Any], tx_sig: str
) -> None:
    fn = _get_callable("utils_pkg.routeguard", ("posttrade_watchdog", "watchdog"))
    if fn is None:
        fn = _get_callable("route_guard", ("posttrade_watchdog", "watchdog"))
    if fn is None:
        return
    try:
        # Support multiple legacy signatures.
        try:
            out = fn(mint, amount_sol, int(SLIPPAGE_BPS), meta)  # type: ignore[misc]
        except TypeError:
            try:
                out = fn(mint=mint, tx_sig=tx_sig, reason="velocity")  # type: ignore[misc]
            except TypeError:
                out = fn(mint)  # type: ignore[misc]
        await _maybe_await(out)
    except Exception:
        _emit_every("POST_WATCHDOG_FAIL", 30, msg="POST_WATCHDOG_FAIL")


async def _swap_buy(mint: str, amount_sol: float) -> tuple[bool, str, dict[str, Any]]:
    """
    Jupiter buy using core_utils.swap_token_jupiter (supports new + legacy signatures).
    Returns (ok, signature, meta).
    """
    fn = _get_callable("utils_pkg.core_utils", ("swap_token_jupiter",))
    if fn is None:
        return False, "", {"err": "missing_swap_token_jupiter"}

    try:
        # Prefer keyword-rich signature first.
        try:
            out = fn(
                input_mint=SOL_MINT,
                output_mint=mint,
                amount_sol=float(amount_sol),
                amount_float=float(amount_sol),
                slippage=float(JUPITER_MAX_SLIPPAGE),
                slippage_bps=int(SLIPPAGE_BPS),
                buy=True,
            )
        except TypeError:
            # Legacy positional: (in_mint, out_mint, amount_sol, buy_bool, slippage_float)
            out = fn(SOL_MINT, mint, float(amount_sol), True, float(JUPITER_MAX_SLIPPAGE))

        out = await _maybe_await(out)

        if isinstance(out, tuple) and len(out) >= 2:
            ok = bool(out[0])
            sig = str(out[1] or "")
            meta = out[2] if (len(out) >= 3 and isinstance(out[2], dict)) else {}
            return ok, sig, dict(meta)

        if isinstance(out, dict):
            ok = bool(out.get("ok", False))
            sig = str(out.get("sig") or out.get("signature") or "")
            return ok, sig, dict(out)

        return False, "", {"err": "swap_bad_return", "raw": str(out)[:240]}
    except Exception as err:
        return False, "", {"err": "swap_exception", "exc": str(err)[:240]}


# =============================================================================
# DEX fetch + candidate build
# =============================================================================
async def _fetch_pairs(http: Any) -> list[dict[str, Any]]:
    """
    Dexscreener search endpoint returns {"pairs":[...]}.
    NOTE: aiohttp is imported only in run_velocity (not at import time).
    """
    try:
        async with http.get(DEX_SEARCH_API) as resp:
            if resp.status != 200:
                with contextlib.suppress(Exception):
                    txt = await resp.text()
                _emit_every(
                    "DEX_FETCH_BAD",
                    10,
                    msg="DEX_FETCH_BAD",
                    status=int(resp.status),
                    preview=(txt[:240] if isinstance(txt, str) else ""),
                )
                return []
            js = await resp.json()
            pairs = js.get("pairs", []) if isinstance(js, dict) else []
            return pairs if isinstance(pairs, list) else []
    except asyncio.CancelledError:
        raise
    except Exception as err:
        _emit_every("DEX_FETCH_ERR", 10, msg="DEX_FETCH_ERR", err=str(err)[:240])
        return []


def _build_candidates(pairs: list[dict[str, Any]], ticket_usd: float) -> list[VelocityCandidate]:
    out: list[VelocityCandidate] = []

    for p in pairs[: max(10, MAX_SCAN_PAIRS)]:
        base_raw = p.get("baseToken")
        base: dict[str, Any] = base_raw if isinstance(base_raw, dict) else {}
        mint = str(base.get("address") or "").strip()
        if not mint:
            continue

        symbol = str(base.get("symbol") or base.get("name") or "UNK").strip()
        pair_url = str(p.get("url") or "").strip()

        liq_raw = p.get("liquidity")
        liq_dict: dict[str, Any] = liq_raw if isinstance(liq_raw, dict) else {}
        liq = _safe_float(liq_dict.get("usd", 0.0))
        if liq < MIN_LIQ_USD:
            continue

        vol_raw = p.get("volume")
        vol_dict: dict[str, Any] = vol_raw if isinstance(vol_raw, dict) else {}
        vol_m5 = _safe_float(vol_dict.get("m5", 0.0))
        vol_m30 = _safe_float(vol_dict.get("m30", 0.0))
        denom = max(1e-9, (vol_m30 / 6.0))
        accel = vol_m5 / denom
        if accel < ACCEL_MIN:
            continue

        pc_raw = p.get("priceChange")
        pc_dict: dict[str, Any] = pc_raw if isinstance(pc_raw, dict) else {}
        pct5 = _safe_float(pc_dict.get("m5", 0.0))
        if pct5 < PRICE_CHANGE_M5_MIN:
            continue

        txns_raw = p.get("txns")
        txns_dict: dict[str, Any] = txns_raw if isinstance(txns_raw, dict) else {}
        m5_raw = txns_dict.get("m5")
        m5_dict: dict[str, Any] = m5_raw if isinstance(m5_raw, dict) else {}
        buys5 = _safe_int(m5_dict.get("buys", 0))
        sells5 = _safe_int(m5_dict.get("sells", 0))
        if buys5 < BUYS_M5_MIN:
            continue

        imp = _impact_bps(liq, ticket_usd)
        if imp > MAX_IMPACT_BPS:
            continue

        score = _compute_score(accel, pct5, buys5)
        if score < SCORE_MIN:
            continue

        out.append(
            VelocityCandidate(
                ts=_now_ts(),
                source="dexscreener_velocity",
                mint=mint,
                symbol=symbol,
                pair_url=pair_url,
                liquidity_usd=float(liq),
                vol_m5_usd=float(vol_m5),
                vol_m30_usd=float(vol_m30),
                accel=float(accel),
                price_change_m5=float(pct5),
                buys_m5=int(buys5),
                sells_m5=int(sells5),
                score=float(score),
                est_impact_bps=float(imp),
                raw={"pair": p},
            )
        )

    out.sort(key=lambda c: (c.score, c.liquidity_usd, c.buys_m5), reverse=True)
    return out


async def _publish(r: RedisLike | None, c: VelocityCandidate) -> None:
    payload = json.dumps(dataclasses.asdict(c), separators=(",", ":"), ensure_ascii=False)
    if r is None:
        _emit_every("QUEUE_NO_REDIS", 15, msg="QUEUE_NO_REDIS", preview=payload[:240])
        return
    try:
        await _maybe_await(r.rpush(REDIS_QUEUE_KEY, payload))
    except Exception as err:
        _emit_every("QUEUE_PUBLISH_FAIL", 10, msg="QUEUE_PUBLISH_FAIL", err=str(err)[:240])


# =============================================================================
# Main loop
# =============================================================================
async def run_velocity(stop_event: asyncio.Event) -> None:
    if not VELOCITY_ENABLED:
        _emit_every("VELOCITY_DISABLED", 10, msg="VELOCITY_DISABLED")
        while not stop_event.is_set():
            await asyncio.sleep(5)
        return

    r = await _redis()

    if TELEGRAM_ON_START:
        await _tg(
            "⚡ Velocity worker started\n"
            f"• mode={VELOCITY_MODE}\n"
            f"• poll={POLL_SEC}s jitter={JITTER_SEC}s cooldown={COOLDOWN_SEC}s lock_ttl={LOCK_TTL_SEC}s\n"
            f"• gates: accel>={ACCEL_MIN:.2f} +5m>={PRICE_CHANGE_M5_MIN:.2f}% buys5>={BUYS_M5_MIN} "
            f"liq>={MIN_LIQ_USD:,.0f} impact<={MAX_IMPACT_BPS:.0f}bps score>={SCORE_MIN:.2f}"
        )

    # IMPORTANT: aiohttp import happens ONLY at runtime.
    import aiohttp  # type: ignore

    timeout = aiohttp.ClientTimeout(total=20)
    async with aiohttp.ClientSession(timeout=timeout) as http:
        while not stop_event.is_set():
            t0 = time.monotonic()
            emitted = 0
            traded = 0

            try:
                if not await _allow_new_entry(r):
                    await asyncio.sleep(float(POLL_SEC))
                    continue

                ticket_usd = await _ticket_usd(r)

                pairs = await _fetch_pairs(http)
                if not pairs:
                    await asyncio.sleep(float(POLL_SEC))
                    continue

                # Prewarm quotes (best-effort) using top liquidity pairs
                if PREWARM_ENABLED and PREWARM_TOP_N > 0:
                    usd_per_sol = await _usd_per_sol()
                    if usd_per_sol > 0.0:
                        amount_sol = float(ticket_usd) / float(usd_per_sol)
                        top = sorted(
                            pairs,
                            key=lambda x: _safe_float(
                                (
                                    (x.get("liquidity") or {})
                                    if isinstance(x.get("liquidity"), dict)
                                    else {}
                                ).get("usd", 0.0)
                            ),
                            reverse=True,
                        )[: max(1, PREWARM_TOP_N)]
                        mints: list[str] = []
                        for p in top:
                            base_raw = p.get("baseToken")
                            base: dict[str, Any] = base_raw if isinstance(base_raw, dict) else {}
                            addr = base.get("address")
                            if addr:
                                mints.append(str(addr))
                        if mints and amount_sol > 0.0:
                            await _prewarm_quotes_best_effort(mints, float(amount_sol))

                cands = _build_candidates(pairs, ticket_usd)
                if not cands:
                    await asyncio.sleep(float(POLL_SEC))
                    continue

                for c in cands:
                    if VELOCITY_MODE == "QUEUE" and emitted >= MAX_EMIT_PER_TICK:
                        break
                    if VELOCITY_MODE == "TRADE" and traded >= MAX_TRADES_PER_TICK:
                        break

                    if not await _cooldown_ok(r, c.mint):
                        continue
                    if not await _try_lock(r, c.mint):
                        continue

                    try:
                        if VELOCITY_MODE == "QUEUE":
                            await _publish(r, c)
                            emitted += 1
                            _emit(
                                "VELOCITY_QUEUE",
                                mint=c.mint,
                                symbol=c.symbol,
                                score=c.score,
                                accel=c.accel,
                            )

                            if TELEGRAM_ON_CANDIDATE:
                                await _tg(
                                    "⚡ Velocity candidate queued\n"
                                    f"• {c.symbol} ({c.mint})\n"
                                    f"• score={c.score:.2f} accel={c.accel:.2f}\n"
                                    f"• liq=${c.liquidity_usd:,.0f} vol5=${c.vol_m5_usd:,.0f} "
                                    f"+5m={c.price_change_m5:.2f}% buys5={c.buys_m5}\n"
                                    f"• impact≈{c.est_impact_bps:.0f} bps\n"
                                    f"• {c.pair_url}"
                                )
                        else:
                            usd_per_sol = await _usd_per_sol()
                            if usd_per_sol <= 0.0:
                                _emit_every("USD_PER_SOL_MISSING", 10, msg="USD_PER_SOL_MISSING")
                                continue

                            amount_sol = float(ticket_usd) / float(usd_per_sol)
                            if amount_sol <= 0.0:
                                continue

                            ok_rg, why, pre_meta = await _router_guard_allows_trade(
                                c.mint, _lamports(amount_sol)
                            )
                            if not ok_rg:
                                _emit("VELOCITY_ROUTE_BLOCK", mint=c.mint, symbol=c.symbol, why=why)
                                await _tg(
                                    "⛔ Velocity router_guard blocked\n"
                                    f"• {c.symbol} ({c.mint})\n"
                                    f"• why={why}\n"
                                    f"• impact≈{c.est_impact_bps:.0f} bps slippage_bps={SLIPPAGE_BPS}"
                                )
                                continue

                            ok, sig, meta = await _swap_buy(c.mint, amount_sol)
                            if not ok or not sig:
                                _emit("VELOCITY_SWAP_FAIL", mint=c.mint, symbol=c.symbol, meta=meta)
                                continue

                            await _note_spend(r, float(ticket_usd))
                            traded += 1

                            _emit(
                                "VELOCITY_BUY",
                                mint=c.mint,
                                symbol=c.symbol,
                                sig=sig,
                                ticket_usd=ticket_usd,
                                amount_sol=amount_sol,
                            )

                            if TELEGRAM_ON_TRADE:
                                await _tg(
                                    "⚡ BUY (velocity)\n"
                                    f"• {c.symbol} ({c.mint})\n"
                                    f"• ticket=${ticket_usd:,.2f} amount≈{amount_sol:.6f} SOL\n"
                                    f"• score={c.score:.2f} accel={c.accel:.2f}\n"
                                    f"• liq=${c.liquidity_usd:,.0f} impact≈{c.est_impact_bps:.0f}bps\n"
                                    f"• tx={sig}"
                                )

                            await _routeguard_posttrade_watchdog(c.mint, amount_sol, pre_meta, sig)

                    finally:
                        await _unlock(r, c.mint)

                dt = time.monotonic() - t0
                _emit_every(
                    "VELOCITY_TICK",
                    10,
                    msg="VELOCITY_TICK",
                    pairs=len(pairs),
                    cands=len(cands),
                    emitted=emitted,
                    traded=traded,
                    dt=f"{dt:.2f}",
                    mode=VELOCITY_MODE,
                )

            except asyncio.CancelledError:
                raise
            except Exception as err:
                _emit_every("VELOCITY_LOOP_ERR", 5, msg="VELOCITY_LOOP_ERR", err=str(err)[:240])

            sleep_for = max(1.0, float(POLL_SEC)) + (
                random.uniform(0.0, float(JITTER_SEC)) if JITTER_SEC > 0 else 0.0
            )
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(stop_event.wait(), timeout=sleep_for)


async def main() -> None:
    await _init_resources_best_effort()

    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()

    def _stop(*_: Any) -> None:
        stop_event.set()

    for sig_name in ("SIGINT", "SIGTERM"):
        sig = getattr(signal, sig_name, None)
        if sig is None:
            continue
        with contextlib.suppress(NotImplementedError):
            loop.add_signal_handler(sig, _stop)

    try:
        await run_velocity(stop_event)
    finally:
        await _close_resources_best_effort()


__all__ = ["VelocityCandidate", "run_velocity", "main"]

if __name__ == "__main__":  # pragma: no cover
    with contextlib.suppress(KeyboardInterrupt):
        asyncio.run(main())
