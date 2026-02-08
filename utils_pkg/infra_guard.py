# -*- coding: utf-8 -*-
from __future__ import annotations

"""
utils_pkg/infra_guard.py

Infra Guard (V3+++ / quant-grade) — Upstream Health Gating (P0 MUST)
===================================================================

High-level role
---------------
Single canonical infra health brain for the whole stack:

  • Continuously probe:
       - Solana RPC health / latency
       - Jupiter quote API health / latency (optional)

  • Classify infra state:
       - HEALTHY   → allow entries
       - DEGRADED  → optionally block entries (configurable)
       - HALTED    → block entries (always), optionally trip breakers

  • Persist infra snapshots to Redis (canonical keys, backwards compatible):
       - infra:halt          "0"/"1"   (TTL)
       - infra:degraded      "0"/"1"   (TTL)
       - infra:last_state    "healthy" | "degraded" | "halted"
       - infra:last_reason   short text
       - infra:last_snapshot JSON (TTL)
       - infra:manual_halt   "0"/"1"   (no TTL)
       - infra:manual_reason free text (no TTL)

  • Provide helpers for the rest of the system:
       - async infra_snapshot(redis=None)        -> dict
       - async infra_features(redis=None)        -> dict[str,float]
       - async attach_infra_features(meta, ...)  -> None
       - async is_halted(redis=None)             -> bool
       - async is_degraded(redis=None)           -> bool
       - async should_block_entries(redis=None)  -> bool
       - async set_infra_halt(reason=None)       -> None
       - async clear_infra_halt()                -> None
       - async quick_check()                     -> dict (no Redis writes)
       - async run_infra_guard(stop_evt=None)    -> None

Dedup / wiring rules
--------------------
- This module does NOT decide strategy. It only publishes infra state.
- guardrails/risk_manager should read infra:* keys (or call should_block_entries()).
- circuit_breakers may consume infra state (or be tripped on HALTED transitions).

Safety principles
-----------------
- Never throw exceptions to callers; infra guard must be resilient.
- No network I/O at import time.
- Redis optional (degrades gracefully).
- Transition-based alerting (no spam), rate-limited.

Run
---
    python -m utils_pkg.infra_guard
"""

import asyncio
import contextlib
import json
import logging
import os
import random
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from functools import lru_cache
from typing import Any, Protocol, cast

import aiohttp

# =============================================================================
# Logging (Phase A: single canonical signature; EventLogger-compatible return)
# =============================================================================


class EventLogger(Protocol):
    def debug(self, msg: str, *args: Any, **kwargs: Any) -> None: ...
    def info(self, msg: str, *args: Any, **kwargs: Any) -> None: ...
    def warning(self, msg: str, *args: Any, **kwargs: Any) -> None: ...
    def error(self, msg: str, *args: Any, **kwargs: Any) -> None: ...
    def exception(self, msg: str, *args: Any, **kwargs: Any) -> None: ...
    def log(self, level: int, msg: str, *args: Any, **kwargs: Any) -> None: ...


def get_logger(name: str | None = None) -> EventLogger:
    """
    Canonical logger getter for infra_guard.

    IMPORTANT (Phase A):
      - Single function definition (no conditional variants).
      - Returns EventLogger-like object (structural typing), not stdlib-only Logger.
    """
    nm = (name or "sniper.infra_guard").strip() or "sniper.infra_guard"

    # 1) Preferred: typed compat (your Phase A foundation)
    with contextlib.suppress(Exception):
        from utils_pkg.log_compat import get_event_logger  # type: ignore

        return cast(EventLogger, get_event_logger(nm))

    # 2) Canonical root logging_setup.py
    with contextlib.suppress(Exception):
        from logging_setup import get_logger as gl  # type: ignore

        return cast(EventLogger, gl(nm))

    # 3) Legacy compat import location
    with contextlib.suppress(Exception):
        from utils_pkg.logging_setup import get_logger as gl2  # type: ignore

        try:
            return cast(EventLogger, gl2(nm))
        except TypeError:
            return cast(EventLogger, gl2())  # type: ignore[call-arg]

    # 4) Last resort: stdlib logging
    lg = logging.getLogger(nm)
    if not lg.handlers:
        logging.basicConfig(level=logging.INFO)
    return cast(EventLogger, lg)


log: EventLogger = get_logger("sniper.infra_guard")


# =============================================================================
# Best-effort wiring imports (NO network I/O at import time)
# =============================================================================


def _core_utils() -> Any | None:
    with contextlib.suppress(Exception):
        from utils_pkg import core_utils as cu  # type: ignore

        return cu
    return None


def _circuit_breakers() -> Any | None:
    with contextlib.suppress(Exception):
        from utils_pkg import circuit_breakers as cb  # type: ignore

        return cb
    return None


def _redis_asyncio_module() -> Any | None:
    with contextlib.suppress(Exception):
        import redis.asyncio as redis_async  # type: ignore

        return redis_async
    return None


async def _maybe_await(x: Any) -> Any:
    if asyncio.iscoroutine(x):
        return await x
    return x


async def _send_telegram(msg: str) -> None:
    """
    Best-effort Telegram sender. Never raises.
    """
    try:
        cu = _core_utils()
        if cu is None:
            return
        fn = getattr(cu, "send_telegram", None)
        if not callable(fn):
            return
        out = fn(msg)
        await _maybe_await(out)
    except Exception:
        return


# =============================================================================
# Env helpers
# =============================================================================

_TRUE = {"1", "true", "yes", "y", "on", "t"}
_FALSE = {"0", "false", "no", "n", "off", "f"}


def _envs(key: str, default: str = "") -> str:
    v = os.getenv(key)
    return (default if v is None else str(v)).strip()


def _envb(key: str, default: bool = False) -> bool:
    v = _envs(key, "")
    if v == "":
        return bool(default)
    s = v.lower().strip()
    if s in _TRUE:
        return True
    if s in _FALSE:
        return False
    return bool(default)


def _envi(key: str, default: int) -> int:
    v = _envs(key, "")
    if v == "":
        return int(default)
    try:
        return int(float(v.replace("_", "")))
    except Exception:
        return int(default)


def _envf(key: str, default: float) -> float:
    v = _envs(key, "")
    if v == "":
        return float(default)
    try:
        return float(v.replace("_", ""))
    except Exception:
        return float(default)


def _now() -> float:
    return time.time()


def _iso_utc() -> str:
    return datetime.now(UTC).isoformat()


def _clamp(x: float, lo: float, hi: float) -> float:
    try:
        xf = float(x)
    except Exception:
        return float(lo)
    if xf < lo:
        return float(lo)
    if xf > hi:
        return float(hi)
    return float(xf)


def _norm_between(x: float, lo: float, hi: float) -> float:
    if hi <= lo:
        return 0.5
    return _clamp((float(x) - lo) / (hi - lo), 0.0, 1.0)


def _percentile(vals: list[float], p: float) -> float:
    """
    Simple percentile (p in [0..100]).
    """
    if not vals:
        return 0.0
    pp = _clamp(p, 0.0, 100.0)
    s = sorted(vals)
    if len(s) == 1:
        return float(s[0])
    k = (len(s) - 1) * (pp / 100.0)
    f = int(k)
    c = min(f + 1, len(s) - 1)
    if f == c:
        return float(s[f])
    return float(s[f] + (s[c] - s[f]) * (k - f))


# =============================================================================
# Config (lazy; cacheable; backwards compatible env keys)
# =============================================================================


@dataclass(frozen=True, slots=True)
class _Cfg:
    # RPC
    rpc_urls: list[str]
    enable_rpc_check: bool
    rpc_timeout_s: float
    rpc_warn_ms: int
    rpc_halt_ms: int

    # Jupiter
    jup_base: str
    jup_enable: bool
    jup_timeout_s: float
    usdc_mint: str
    sol_mint: str
    jup_test_amount: int
    jup_warn_ms: int
    jup_halt_ms: int

    # Redis
    redis_url: str

    # Loop cadence
    poll_sec: int
    jitter_sec: float

    # Score smoothing / thresholds
    score_decay: float
    degraded_score: float
    halt_score: float

    # Behavior toggles
    fail_open: bool
    block_on_degraded: bool

    # Alerts
    tg_alerts: bool
    alert_on_transition: bool
    alert_cooldown_s: int
    debug_log: bool

    # Redis TTLs
    key_ttl_sec: int
    snapshot_ttl_sec: int

    # Circuit-breakers behavior
    trip_breaker_on_halt: bool
    breaker_cooldown_s: float
    publish_degraded_to_cb: bool
    degraded_ttl_sec: int

    # History
    hist_max: int
    rpc_probe_fanout: int


@lru_cache(maxsize=1)
def _cfg() -> _Cfg:
    # RPC URL(s)
    rpc_url = _envs("RPC_URL", "")
    rpc_urls = [u.strip() for u in _envs("RPC_URLS", "").split(",") if u.strip()]
    if rpc_url and rpc_url not in rpc_urls:
        rpc_urls = [rpc_url, *rpc_urls]
    if not rpc_urls and rpc_url:
        rpc_urls = [rpc_url]

    # Jupiter
    jup_base = _envs(
        "JUPITER_SWAP_API", _envs("JUPITER_QUOTE_API", "https://quote-api.jup.ag/v6")
    ).rstrip("/")
    jup_enable = _envb("INFRA_ENABLE_JUP_CHECK", True)
    jup_timeout_s = _envf("INFRA_JUP_TIMEOUT_S", 8.0)

    usdc_mint = _envs("USDC_MINT", "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v")
    sol_mint = _envs("SOL_MINT", "So11111111111111111111111111111111111111112")
    jup_test_amount = _envi("INFRA_JUP_TEST_AMOUNT", 1_000_000)

    # Redis
    redis_url = _envs("REDIS_URL", _envs("UPSTASH_REDIS_URL", "redis://localhost:6379"))

    # Cadence
    poll_sec = max(5, _envi("INFRA_POLL_SEC", 20))
    jitter_sec = _clamp(_envf("INFRA_JITTER_SEC", 0.8), 0.0, 10.0)

    # Timeouts
    rpc_timeout_s = _envf("INFRA_RPC_TIMEOUT_S", 8.0)

    # Lat thresholds (ms)
    rpc_warn_ms = _envi("INFRA_LATENCY_WARN_MS", 800)
    rpc_halt_ms = _envi("INFRA_LATENCY_MAX_MS", 1200)
    jup_warn_ms = _envi("INFRA_JUP_WARN_MS", 1000)
    jup_halt_ms = _envi("INFRA_JUP_MAX_MS", 1500)

    # Flap control
    score_decay = _clamp(_envf("INFRA_SCORE_DECAY", 0.65), 0.05, 0.99)
    degraded_score = _clamp(_envf("INFRA_DEGRADED_SCORE", 1.2), 0.0, 50.0)
    halt_score = _clamp(_envf("INFRA_HALT_SCORE", 3.2), 0.0, 100.0)

    # Behavior
    fail_open = _envb("INFRA_FAIL_OPEN", False)
    block_on_degraded = _envb("INFRA_BLOCK_ON_DEGRADED", False)
    enable_rpc_check = _envb("INFRA_ENABLE_RPC_CHECK", True)

    # Alerts
    tg_alerts = _envb("INFRA_TELEGRAM_ALERTS", True)
    alert_on_transition = _envb("INFRA_ALERT_ON_TRANSITION", True)
    alert_cooldown_s = _envi("INFRA_ALERT_COOLDOWN_S", 90)
    debug_log = _envb("INFRA_DEBUG_LOG", False)

    # Redis TTLs
    key_ttl_sec = max(poll_sec * 3, _envi("INFRA_KEY_TTL_SEC", poll_sec * 3))
    snapshot_ttl_sec = max(300, _envi("INFRA_SNAPSHOT_TTL_SEC", 3600))

    # Breakers
    trip_breaker_on_halt = _envb("INFRA_TRIP_BREAKER_ON_HALT", True)
    breaker_cooldown_s = _envf("INFRA_BREAKER_COOLDOWN_S", float(poll_sec * 2))

    publish_degraded_to_cb = _envb("INFRA_PUBLISH_DEGRADED", True)
    degraded_ttl_sec = max(poll_sec * 3, _envi("INFRA_DEGRADED_TTL_SEC", poll_sec * 3))

    # History
    hist_max = max(10, _envi("INFRA_HIST_MAX", 60))
    rpc_probe_fanout = _clamp(_envf("INFRA_RPC_PROBE_FANOUT", 3.0), 1.0, 10.0)
    rpc_probe_fanout_i = int(rpc_probe_fanout)

    return _Cfg(
        rpc_urls=rpc_urls,
        enable_rpc_check=enable_rpc_check,
        rpc_timeout_s=float(rpc_timeout_s),
        rpc_warn_ms=int(rpc_warn_ms),
        rpc_halt_ms=int(rpc_halt_ms),
        jup_base=str(jup_base),
        jup_enable=bool(jup_enable),
        jup_timeout_s=float(jup_timeout_s),
        usdc_mint=str(usdc_mint),
        sol_mint=str(sol_mint),
        jup_test_amount=int(jup_test_amount),
        jup_warn_ms=int(jup_warn_ms),
        jup_halt_ms=int(jup_halt_ms),
        redis_url=str(redis_url),
        poll_sec=int(poll_sec),
        jitter_sec=float(jitter_sec),
        score_decay=float(score_decay),
        degraded_score=float(degraded_score),
        halt_score=float(halt_score),
        fail_open=bool(fail_open),
        block_on_degraded=bool(block_on_degraded),
        tg_alerts=bool(tg_alerts),
        alert_on_transition=bool(alert_on_transition),
        alert_cooldown_s=int(alert_cooldown_s),
        debug_log=bool(debug_log),
        key_ttl_sec=int(key_ttl_sec),
        snapshot_ttl_sec=int(snapshot_ttl_sec),
        trip_breaker_on_halt=bool(trip_breaker_on_halt),
        breaker_cooldown_s=float(breaker_cooldown_s),
        publish_degraded_to_cb=bool(publish_degraded_to_cb),
        degraded_ttl_sec=int(degraded_ttl_sec),
        hist_max=int(hist_max),
        rpc_probe_fanout=int(rpc_probe_fanout_i),
    )


def reload_config() -> None:
    _cfg.cache_clear()


# =============================================================================
# Canonical Redis keys (KEEP EXACT)
# =============================================================================

KEY_HALTED = "infra:halt"
KEY_DEGRADED = "infra:degraded"
KEY_LAST_STATE = "infra:last_state"
KEY_LAST_REASON = "infra:last_reason"
KEY_LAST_SNAPSHOT = "infra:last_snapshot"
KEY_MANUAL_HALT = "infra:manual_halt"
KEY_MANUAL_REASON = "infra:manual_reason"

STATE_HEALTHY = "healthy"
STATE_DEGRADED = "degraded"
STATE_HALTED = "halted"


# =============================================================================
# Redis helpers (best-effort)
# =============================================================================

_redis_cached: Any | None = None
_redis_cached_at: float = 0.0
_REDIS_REFRESH_S = 5.0
_redis_lock = asyncio.Lock()


async def _get_redis() -> Any | None:
    """
    Prefer core_utils async redis getter; otherwise core_utils.redis; otherwise create one.
    Never raises.
    """
    global _redis_cached, _redis_cached_at

    try:
        now = _now()
        if _redis_cached is not None and (now - _redis_cached_at) < _REDIS_REFRESH_S:
            return _redis_cached

        async with _redis_lock:
            now2 = _now()
            if _redis_cached is not None and (now2 - _redis_cached_at) < _REDIS_REFRESH_S:
                return _redis_cached

            _redis_cached_at = now2
            cfg = _cfg()

            # 1) core_utils.get_redis_async()
            cu = _core_utils()
            if cu is not None:
                fn = getattr(cu, "get_redis_async", None)
                if callable(fn):
                    r = fn()
                    r = await _maybe_await(r)
                    if r is not None:
                        _redis_cached = r
                        return _redis_cached

                # 2) core_utils.redis shared singleton
                r2 = getattr(cu, "redis", None)
                if r2 is not None:
                    _redis_cached = r2
                    return _redis_cached

            # 3) local redis.asyncio client
            redis_async = _redis_asyncio_module()
            if redis_async is None or not cfg.redis_url:
                _redis_cached = None
                return None

            _redis_cached = redis_async.from_url(cfg.redis_url, decode_responses=True)
            return _redis_cached
    except Exception:
        _redis_cached = None
        return None


async def _redis_setex(redis: Any, key: str, value: str, ex: int) -> None:
    try:
        await redis.set(key, value, ex=int(ex))
    except Exception:
        return


async def _redis_set(redis: Any, key: str, value: str) -> None:
    try:
        await redis.set(key, value)
    except Exception:
        return


async def _redis_get(redis: Any, key: str) -> str | None:
    try:
        v = await redis.get(key)
        if v is None:
            return None
        return str(v)
    except Exception:
        return None


# =============================================================================
# Probes
# =============================================================================


async def _probe_rpc(session: aiohttp.ClientSession, rpc_url: str) -> tuple[int, bool, str]:
    """
    Probe Solana RPC via getHealth.
    Returns (latency_ms, ok, detail).
    """
    cfg = _cfg()
    if not rpc_url:
        return 10_000, bool(cfg.fail_open), "missing_rpc_url"

    payload = {"jsonrpc": "2.0", "id": 1, "method": "getHealth"}
    t0 = time.perf_counter()
    try:
        timeout = aiohttp.ClientTimeout(total=float(cfg.rpc_timeout_s))
        async with session.post(rpc_url, json=payload, timeout=timeout) as r:
            txt = await r.text()
            ms = int((time.perf_counter() - t0) * 1000)
            if r.status != 200:
                return ms, bool(cfg.fail_open), f"http_{r.status}"

            ok = True
            try:
                j = json.loads(txt) if txt else {}
                if isinstance(j, dict) and j.get("error"):
                    ok = False
                if isinstance(j, dict) and j.get("result") not in (None, "ok"):
                    if j.get("result") != "ok":
                        ok = False
            except Exception:
                ok = bool(cfg.fail_open)

            return ms, bool(ok), "ok" if ok else "rpc_error"
    except Exception as e:
        if cfg.debug_log:
            with contextlib.suppress(Exception):
                log.debug("RPC probe error %s: %s", rpc_url, e)
        return 10_000, bool(cfg.fail_open), "exception"


async def _rpc_latency(session: aiohttp.ClientSession) -> tuple[int, bool, str, str]:
    """
    Probe across multiple RPC URLs; return the best successful one (lowest ms).
    Returns (lat_ms, ok, detail, chosen_url).
    """
    cfg = _cfg()
    if not cfg.enable_rpc_check:
        return 0, True, "rpc_check_disabled", ""

    if not cfg.rpc_urls:
        return 10_000, bool(cfg.fail_open), "no_rpc_urls", ""

    urls = cfg.rpc_urls[:]
    random.shuffle(urls)
    urls = urls[: max(1, min(cfg.rpc_probe_fanout, len(urls)))]

    results = await asyncio.gather(*[_probe_rpc(session, u) for u in urls], return_exceptions=True)

    best_ok: bool | None = None
    best_ms = 10_000
    best_detail = "no_ok"
    best_url = urls[0] if urls else ""

    for i, res in enumerate(results):
        if isinstance(res, BaseException):
            continue
        ms, ok, detail = res
        if ok:
            if best_ok is None or ms < best_ms:
                best_ok = True
                best_ms = ms
                best_detail = detail
                best_url = urls[i]
        else:
            if best_ok is None and ms < best_ms:
                best_ms = ms
                best_detail = detail
                best_url = urls[i]

    if best_ok is True:
        return int(best_ms), True, best_detail, best_url
    return int(best_ms), bool(cfg.fail_open), best_detail, best_url


async def _jup_latency(session: aiohttp.ClientSession) -> tuple[int, bool, str]:
    """
    Probe Jupiter quote API via /quote.
    Returns (latency_ms, ok, detail).
    """
    cfg = _cfg()
    if not cfg.jup_enable:
        return 0, True, "jup_check_disabled"

    params = {
        "inputMint": cfg.sol_mint,
        "outputMint": cfg.usdc_mint,
        "amount": str(int(cfg.jup_test_amount)),
        "slippageBps": "50",
    }

    t0 = time.perf_counter()
    try:
        timeout = aiohttp.ClientTimeout(total=float(cfg.jup_timeout_s))
        async with session.get(f"{cfg.jup_base}/quote", params=params, timeout=timeout) as r:
            txt = await r.text()
            ms = int((time.perf_counter() - t0) * 1000)
            if r.status != 200:
                return ms, bool(cfg.fail_open), f"http_{r.status}"

            ok = True
            try:
                j = json.loads(txt) if txt else {}
                if isinstance(j, dict) and "data" in j and not j.get("data"):
                    ok = False
                if isinstance(j, dict) and j.get("error"):
                    ok = False
            except Exception:
                ok = bool(cfg.fail_open)

            return ms, bool(ok), "ok" if ok else "bad_payload"
    except Exception as e:
        if cfg.debug_log:
            with contextlib.suppress(Exception):
                log.debug("JUP probe error: %s", e)
        return 10_000, bool(cfg.fail_open), "exception"


# =============================================================================
# Snapshot / state logic
# =============================================================================


@dataclass(slots=True)
class InfraSnapshot:
    ts: str
    state: str
    reason: str

    rpc_url: str
    rpc_latency_ms: int
    rpc_ok: bool
    rpc_detail: str

    jup_latency_ms: int
    jup_ok: bool
    jup_detail: str

    bad_score: float
    warn_score: float

    rpc_p95_ms: int
    jup_p95_ms: int

    manual_halt: bool

    def to_dict(self) -> dict[str, Any]:
        return {
            "ts": self.ts,
            "state": self.state,
            "reason": self.reason,
            "rpc_url": self.rpc_url,
            "rpc_latency_ms": int(self.rpc_latency_ms),
            "rpc_ok": bool(self.rpc_ok),
            "rpc_detail": str(self.rpc_detail),
            "jup_latency_ms": int(self.jup_latency_ms),
            "jup_ok": bool(self.jup_ok),
            "jup_detail": str(self.jup_detail),
            "bad_score": float(self.bad_score),
            "warn_score": float(self.warn_score),
            "rpc_p95_ms": int(self.rpc_p95_ms),
            "jup_p95_ms": int(self.jup_p95_ms),
            "manual_halt": bool(self.manual_halt),
        }


def _classify_sample(
    *, rpc_ms: int, rpc_ok: bool, jup_ms: int, jup_ok: bool
) -> tuple[bool, bool, str]:
    """
    Returns: (sample_bad, sample_warn, reason_fragment)
    """
    cfg = _cfg()
    reasons: list[str] = []

    rpc_hard = (not rpc_ok) or (rpc_ms > cfg.rpc_halt_ms)
    rpc_warn = (rpc_ms > cfg.rpc_warn_ms) and (not rpc_hard)
    if rpc_hard:
        reasons.append(f"RPC bad(ok={rpc_ok}, {rpc_ms}ms)")
    elif rpc_warn:
        reasons.append(f"RPC slow({rpc_ms}ms)")

    jup_hard = False
    jup_warn = False
    if cfg.jup_enable:
        jup_hard = (not jup_ok) or (jup_ms > cfg.jup_halt_ms)
        jup_warn = (jup_ms > cfg.jup_warn_ms) and (not jup_hard)
        if jup_hard:
            reasons.append(f"JUP bad(ok={jup_ok}, {jup_ms}ms)")
        elif jup_warn:
            reasons.append(f"JUP slow({jup_ms}ms)")
    else:
        reasons.append("JUP disabled")

    sample_bad = bool(rpc_hard or jup_hard)
    sample_warn = bool(sample_bad or rpc_warn or jup_warn)

    if not reasons:
        reasons.append("within thresholds")

    return sample_bad, sample_warn, "; ".join(reasons)


def _state_from_scores(*, bad_score: float, warn_score: float) -> str:
    cfg = _cfg()
    if bad_score >= cfg.halt_score:
        return STATE_HALTED
    if warn_score >= cfg.degraded_score:
        return STATE_DEGRADED
    return STATE_HEALTHY


# =============================================================================
# Public read helpers
# =============================================================================


async def infra_snapshot(redis: Any | None = None) -> dict[str, Any]:
    """
    Read last snapshot from Redis; returns a safe default if missing.
    """
    cfg = _cfg()
    try:
        if redis is None:
            redis = await _get_redis()
        if not redis:
            return {
                "ts": _iso_utc(),
                "state": STATE_HEALTHY,
                "reason": "no redis; default healthy",
                "rpc_latency_ms": 0,
                "rpc_ok": True,
                "jup_latency_ms": 0,
                "jup_ok": True,
                "bad_score": 0.0,
                "warn_score": 0.0,
                "manual_halt": False,
            }

        raw = await _redis_get(redis, KEY_LAST_SNAPSHOT)
        if raw:
            with contextlib.suppress(Exception):
                j = json.loads(raw)
                if isinstance(j, dict):
                    return j

        st = await _redis_get(redis, KEY_LAST_STATE) or STATE_HEALTHY
        rsn = await _redis_get(redis, KEY_LAST_REASON) or "no snapshot"
        halted = (await _redis_get(redis, KEY_HALTED) or "0") == "1"
        degraded = (await _redis_get(redis, KEY_DEGRADED) or "0") == "1"

        return {
            "ts": _iso_utc(),
            "state": STATE_HALTED if halted else (STATE_DEGRADED if degraded else str(st)),
            "reason": str(rsn),
            "fail_open": bool(cfg.fail_open),
        }
    except Exception:
        return {"ts": _iso_utc(), "state": STATE_HEALTHY, "reason": "snapshot_failed_default"}


async def is_halted(redis: Any | None = None) -> bool:
    try:
        if redis is None:
            redis = await _get_redis()
        if not redis:
            return False
        manual = await _redis_get(redis, KEY_MANUAL_HALT)
        if str(manual or "0") == "1":
            return True
        v = await _redis_get(redis, KEY_HALTED)
        return str(v or "0") == "1"
    except Exception:
        return False


async def is_degraded(redis: Any | None = None) -> bool:
    try:
        if redis is None:
            redis = await _get_redis()
        if not redis:
            return False
        v = await _redis_get(redis, KEY_DEGRADED)
        return str(v or "0") == "1"
    except Exception:
        return False


async def should_block_entries(redis: Any | None = None) -> bool:
    """
    Canonical helper:
      - Always block when HALTED
      - Optionally block when DEGRADED
    """
    cfg = _cfg()
    try:
        if await is_halted(redis):
            return True
        return bool(cfg.block_on_degraded and await is_degraded(redis))
    except Exception:
        return False


# =============================================================================
# Manual overrides
# =============================================================================


async def set_infra_halt(reason: str | None = None) -> None:
    """
    Persist manual halt override. No TTL.
    """
    cfg = _cfg()
    try:
        redis = await _get_redis()
        if not redis:
            return

        await _redis_set(redis, KEY_MANUAL_HALT, "1")
        if reason:
            await _redis_set(redis, KEY_MANUAL_REASON, str(reason)[:300])

        # Keep canonical flags aligned (TTL so if guard dies, it still blocks for a bit)
        await _redis_setex(redis, KEY_HALTED, "1", cfg.key_ttl_sec)
        await _redis_setex(redis, KEY_DEGRADED, "1", cfg.key_ttl_sec)
        await _redis_set(redis, KEY_LAST_STATE, STATE_HALTED)
        await _redis_set(
            redis,
            KEY_LAST_REASON,
            f"manual_halt: {str(reason)[:200] if reason else ''}".strip(),
        )

        snap = await infra_snapshot(redis)
        snap["state"] = STATE_HALTED
        if reason:
            snap["reason"] = f"manual_halt: {reason}"
        await _redis_setex(redis, KEY_LAST_SNAPSHOT, json.dumps(snap), cfg.snapshot_ttl_sec)

        if cfg.tg_alerts:
            await _send_telegram("🚨 *MANUAL INFRA HALT SET*\n" + f"• reason: {reason or 'n/a'}")
    except Exception:
        return


async def clear_infra_halt() -> None:
    """
    Clear manual halt override. The next guard tick will re-classify.
    """
    cfg = _cfg()
    try:
        redis = await _get_redis()
        if not redis:
            return

        await _redis_set(redis, KEY_MANUAL_HALT, "0")
        await _redis_set(redis, KEY_MANUAL_REASON, "cleared")
        await _redis_set(redis, KEY_LAST_REASON, "manual clear_infra_halt()")

        if cfg.tg_alerts:
            await _send_telegram("🟢 Manual infra halt cleared.")
    except Exception:
        return


# =============================================================================
# AI / FeatureBuilder helpers
# =============================================================================


async def infra_features(redis: Any | None = None) -> dict[str, float]:
    """
    Numeric infra features (model-friendly).
    """
    cfg = _cfg()
    snap = await infra_snapshot(redis)

    state = str(snap.get("state", STATE_HEALTHY))
    rpc_ms = float(snap.get("rpc_latency_ms", 0.0) or 0.0)
    jup_ms = float(snap.get("jup_latency_ms", 0.0) or 0.0)
    bad_score = float(snap.get("bad_score", 0.0) or 0.0)
    warn_score = float(snap.get("warn_score", 0.0) or 0.0)

    is_h = 1.0 if state == STATE_HEALTHY else 0.0
    is_d = 1.0 if state in (STATE_DEGRADED, STATE_HALTED) else 0.0
    is_x = 1.0 if state == STATE_HALTED else 0.0

    rpc_norm = _norm_between(rpc_ms, 0.0, float(max(1, cfg.rpc_halt_ms)))
    jup_norm = _norm_between(jup_ms, 0.0, float(max(1, cfg.jup_halt_ms)))

    health_score = 1.0 * is_h + 0.5 * (1.0 if state == STATE_DEGRADED else 0.0)
    stability_score = 1.0 / (1.0 + max(0.0, warn_score))

    return {
        "infra_state_is_healthy": is_h,
        "infra_state_is_degraded": is_d,
        "infra_state_is_halted": is_x,
        "infra_rpc_latency_ms": float(rpc_ms),
        "infra_jup_latency_ms": float(jup_ms),
        "infra_rpc_latency_norm": float(rpc_norm),
        "infra_jup_latency_norm": float(jup_norm),
        "infra_bad_score": float(bad_score),
        "infra_warn_score": float(warn_score),
        "infra_health_score": _clamp(float(health_score), 0.0, 1.0),
        "infra_stability_score": _clamp(float(stability_score), 0.0, 1.0),
    }


async def attach_infra_features(meta: dict[str, Any], redis: Any | None = None) -> None:
    """
    Mutates meta dict with infra_* numeric features.
    """
    try:
        meta.update(await infra_features(redis))
    except Exception:
        return


# =============================================================================
# One-shot check (no Redis writes)
# =============================================================================


async def quick_check() -> dict[str, Any]:
    """
    One-shot infra check without writing to Redis.
    """
    cfg = _cfg()
    total_timeout = max(float(cfg.rpc_timeout_s), float(cfg.jup_timeout_s)) + 2.0
    timeout = aiohttp.ClientTimeout(total=float(total_timeout))

    async with aiohttp.ClientSession(timeout=timeout) as session:
        rpc_ms, rpc_ok, rpc_detail, rpc_url = await _rpc_latency(session)
        jup_ms, jup_ok, jup_detail = await _jup_latency(session)

    sample_bad, sample_warn, frag = _classify_sample(
        rpc_ms=rpc_ms, rpc_ok=rpc_ok, jup_ms=jup_ms, jup_ok=jup_ok
    )

    bad_score = 1.0 if sample_bad else 0.0
    warn_score = 1.0 if sample_warn else 0.0
    state = _state_from_scores(bad_score=bad_score, warn_score=warn_score)

    snap = InfraSnapshot(
        ts=_iso_utc(),
        state=state,
        reason=frag,
        rpc_url=rpc_url,
        rpc_latency_ms=rpc_ms,
        rpc_ok=rpc_ok,
        rpc_detail=rpc_detail,
        jup_latency_ms=jup_ms,
        jup_ok=jup_ok,
        jup_detail=jup_detail,
        bad_score=bad_score,
        warn_score=warn_score,
        rpc_p95_ms=rpc_ms,
        jup_p95_ms=jup_ms,
        manual_halt=False,
    )
    return snap.to_dict()


# =============================================================================
# Main guard loop
# =============================================================================


async def run_infra_guard(stop_evt: asyncio.Event | None = None) -> None:
    """
    Main infra guard loop:
      - probes RPC + JUP
      - maintains smooth scores to avoid flapping
      - writes redis keys (TTL)
      - sends transition alerts (rate-limited)
      - optionally trips circuit breaker on HALTED transitions
    """
    cfg = _cfg()
    redis = await _get_redis()

    bad_score: float = 0.0
    warn_score: float = 0.0

    rpc_hist: list[float] = []
    jup_hist: list[float] = []

    last_state: str | None = None
    last_alert_at: float = 0.0
    last_trip_state: str | None = None

    with contextlib.suppress(Exception):
        log.info(
            "INFRA_GUARD_START poll=%ss rpc_check=%s jup_check=%s fail_open=%s block_on_degraded=%s",
            cfg.poll_sec,
            cfg.enable_rpc_check,
            cfg.jup_enable,
            cfg.fail_open,
            cfg.block_on_degraded,
        )

    if cfg.tg_alerts:
        await _send_telegram("🛡️ Infra guard started.")

    total_timeout = max(float(cfg.rpc_timeout_s), float(cfg.jup_timeout_s)) + 2.0
    session_timeout = aiohttp.ClientTimeout(total=float(total_timeout))

    async with aiohttp.ClientSession(timeout=session_timeout) as session:
        while True:
            if stop_evt is not None and stop_evt.is_set():
                with contextlib.suppress(Exception):
                    log.info("INFRA_GUARD_STOP stop_evt")
                if cfg.tg_alerts:
                    await _send_telegram("🛡️ Infra guard stopped (stop_evt).")
                return

            # Manual halt overrides (read first)
            manual_active = False
            manual_reason = ""
            if redis:
                try:
                    mv = await _redis_get(redis, KEY_MANUAL_HALT)
                    manual_active = str(mv or "0") == "1"
                    if manual_active:
                        manual_reason = str((await _redis_get(redis, KEY_MANUAL_REASON)) or "")
                except Exception:
                    manual_active = False
                    manual_reason = ""

            # Probes
            rpc_ms, rpc_ok, rpc_detail, rpc_url = await _rpc_latency(session)
            jup_ms, jup_ok, jup_detail = await _jup_latency(session)

            # Hist
            rpc_hist.append(float(rpc_ms))
            if len(rpc_hist) > cfg.hist_max:
                rpc_hist = rpc_hist[-cfg.hist_max :]

            if cfg.jup_enable:
                jup_hist.append(float(jup_ms))
                if len(jup_hist) > cfg.hist_max:
                    jup_hist = jup_hist[-cfg.hist_max :]

            # Optional breakers telemetry
            cb = _circuit_breakers()
            if cb is not None:
                with contextlib.suppress(Exception):
                    cb.record_error(
                        bool(rpc_ok), kind="rpc_probe", where=rpc_url, ms=rpc_ms, detail=rpc_detail
                    )
                if cfg.jup_enable:
                    with contextlib.suppress(Exception):
                        cb.record_error(
                            bool(jup_ok),
                            kind="jup_probe",
                            where=cfg.jup_base,
                            ms=jup_ms,
                            detail=jup_detail,
                        )

            # Classify + smooth
            sample_bad, sample_warn, frag = _classify_sample(
                rpc_ms=rpc_ms, rpc_ok=rpc_ok, jup_ms=jup_ms, jup_ok=jup_ok
            )

            bad_score = bad_score * cfg.score_decay + (1.0 if sample_bad else 0.0)
            warn_score = warn_score * cfg.score_decay + (1.0 if sample_warn else 0.0)

            state = _state_from_scores(bad_score=bad_score, warn_score=warn_score)

            reason = frag
            if manual_active:
                state = STATE_HALTED
                reason = (
                    f"manual_halt: {manual_reason[:200]}; {frag}"
                    if manual_reason
                    else f"manual_halt; {frag}"
                )

            rpc_p95 = int(_percentile(rpc_hist, 95.0))
            jup_p95 = int(_percentile(jup_hist, 95.0)) if jup_hist else int(jup_ms)

            snap = InfraSnapshot(
                ts=_iso_utc(),
                state=state,
                reason=reason,
                rpc_url=rpc_url,
                rpc_latency_ms=int(rpc_ms),
                rpc_ok=bool(rpc_ok),
                rpc_detail=str(rpc_detail),
                jup_latency_ms=int(jup_ms),
                jup_ok=bool(jup_ok),
                jup_detail=str(jup_detail),
                bad_score=float(bad_score),
                warn_score=float(warn_score),
                rpc_p95_ms=rpc_p95,
                jup_p95_ms=jup_p95,
                manual_halt=bool(manual_active),
            )

            # Persist Redis
            if redis:
                try:
                    halted_flag = "1" if state == STATE_HALTED else "0"
                    degraded_flag = "1" if state in (STATE_DEGRADED, STATE_HALTED) else "0"

                    await _redis_setex(redis, KEY_HALTED, halted_flag, cfg.key_ttl_sec)
                    await _redis_setex(redis, KEY_DEGRADED, degraded_flag, cfg.key_ttl_sec)
                    await _redis_set(redis, KEY_LAST_STATE, state)
                    await _redis_set(redis, KEY_LAST_REASON, reason[:300])
                    await _redis_setex(
                        redis, KEY_LAST_SNAPSHOT, json.dumps(snap.to_dict()), cfg.snapshot_ttl_sec
                    )
                except Exception as e:
                    if cfg.debug_log:
                        with contextlib.suppress(Exception):
                            log.debug("infra_guard redis write error: %s", e)

            # Publish degraded signal into circuit_breakers shared key (optional)
            if (
                cfg.publish_degraded_to_cb
                and cb is not None
                and state in (STATE_DEGRADED, STATE_HALTED)
            ):
                with contextlib.suppress(Exception):
                    cb.set_degraded(
                        True, reason=f"infra_guard:{state}", ttl_sec=int(cfg.degraded_ttl_sec)
                    )

            # Trip breaker on transition into HALTED
            if (
                cfg.trip_breaker_on_halt
                and cb is not None
                and state == STATE_HALTED
                and last_trip_state != STATE_HALTED
            ):
                last_trip_state = STATE_HALTED
                with contextlib.suppress(Exception):
                    cb.trip_breaker(seconds=float(cfg.breaker_cooldown_s), reason="infra_halted")

            if state != STATE_HALTED and last_trip_state == STATE_HALTED:
                last_trip_state = state

            # Transition alerting (rate-limited)
            if (not cfg.alert_on_transition) or (state != last_state):
                now = _now()
                if (now - last_alert_at) >= float(cfg.alert_cooldown_s):
                    last_alert_at = now
                    last_state = state

                    with contextlib.suppress(Exception):
                        log.info(
                            "INFRA_STATE_CHANGE new=%s rpc=%sms(ok=%s) jup=%sms(ok=%s, enabled=%s) bad=%.2f warn=%.2f reason=%s",
                            state,
                            rpc_ms,
                            rpc_ok,
                            jup_ms,
                            jup_ok,
                            cfg.jup_enable,
                            bad_score,
                            warn_score,
                            reason,
                        )

                    if cfg.tg_alerts:
                        icon = (
                            "🟢"
                            if state == STATE_HEALTHY
                            else ("🟠" if state == STATE_DEGRADED else "🚨")
                        )
                        with contextlib.suppress(Exception):
                            await _send_telegram(
                                f"{icon} *INFRA {state.upper()}*\n"
                                f"• RPC: {rpc_ms}ms (ok={rpc_ok})\n"
                                f"• RPC p95: {rpc_p95}ms\n"
                                f"• JUP: {jup_ms}ms (ok={jup_ok}, enabled={cfg.jup_enable})\n"
                                f"• JUP p95: {jup_p95}ms\n"
                                f"• scores: bad={bad_score:.2f} warn={warn_score:.2f}\n"
                                f"• manual: {manual_active}\n"
                                f"• reason: {reason[:280]}"
                            )

            # Sleep with jitter (stop_evt responsive)
            sleep_s = float(cfg.poll_sec) + (random.random() * float(cfg.jitter_sec))
            end = _now() + sleep_s
            while _now() < end:
                if stop_evt is not None and stop_evt.is_set():
                    break
                await asyncio.sleep(0.25)


# =============================================================================
# Module CLI
# =============================================================================

if __name__ == "__main__":
    asyncio.run(run_infra_guard())


__all__ = [
    "attach_infra_features",
    "clear_infra_halt",
    "get_logger",
    "infra_features",
    "infra_snapshot",
    "is_degraded",
    "is_halted",
    "quick_check",
    "reload_config",
    "run_infra_guard",
    "set_infra_halt",
    "should_block_entries",
]
