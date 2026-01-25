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
import os
import random
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

import aiohttp

# ----------------------------
# Logging (best-effort)
# ----------------------------
try:  # pragma: no cover
    from logging_setup import get_logger  # type: ignore
except Exception:  # pragma: no cover
    import logging

    def get_logger(name: str = "sniper.infra_guard"):
        lg = logging.getLogger(name)
        if not lg.handlers:
            logging.basicConfig(level=logging.INFO)
        return lg


log = get_logger("sniper.infra_guard")

# ----------------------------
# Core utils (best-effort)
# ----------------------------
try:  # pragma: no cover
    from utils_pkg import core_utils as cu  # type: ignore
except Exception:  # pragma: no cover
    cu = None  # type: ignore


async def _send_telegram(msg: str) -> None:
    """
    Best-effort Telegram sender. Never raises.
    """
    try:
        if cu is None:
            return
        fn = getattr(cu, "send_telegram", None)
        if fn is None:
            return
        r = fn(msg)
        if asyncio.iscoroutine(r):
            await r
    except Exception:
        return


# Optional standalone redis async client
try:  # pragma: no cover
    import redis.asyncio as aioredis  # type: ignore
except Exception:  # pragma: no cover
    aioredis = None  # type: ignore


# Optional circuit breakers integration (best-effort)
try:  # pragma: no cover
    from utils_pkg import circuit_breakers as cb  # type: ignore
except Exception:  # pragma: no cover
    cb = None  # type: ignore


# =============================================================================
# Env helpers
# =============================================================================

_TRUE = {"1", "true", "yes", "y", "on", "t"}
_FALSE = {"0", "false", "no", "n", "off", "f"}


def _envs(key: str, default: str = "") -> str:
    try:
        v = os.getenv(key)
        return (v if v is not None else default).strip()
    except Exception:
        return str(default).strip()


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
        x = float(x)
    except Exception:
        return float(lo)
    if x < lo:
        return float(lo)
    if x > hi:
        return float(hi)
    return float(x)


def _norm_between(x: float, lo: float, hi: float) -> float:
    if hi <= lo:
        return 0.5
    return _clamp((x - lo) / (hi - lo), 0.0, 1.0)


def _percentile(vals: list[float], p: float) -> float:
    """
    Simple percentile (p in [0..100]).
    """
    if not vals:
        return 0.0
    p = _clamp(p, 0.0, 100.0)
    s = sorted(vals)
    if len(s) == 1:
        return float(s[0])
    k = (len(s) - 1) * (p / 100.0)
    f = int(k)
    c = min(f + 1, len(s) - 1)
    if f == c:
        return float(s[f])
    return float(s[f] + (s[c] - s[f]) * (k - f))


# =============================================================================
# Config (backwards compatible)
# =============================================================================

# RPC URL(s)
RPC_URL = _envs("RPC_URL", "")
RPC_URLS = [u.strip() for u in _envs("RPC_URLS", "").split(",") if u.strip()]
if RPC_URL and RPC_URL not in RPC_URLS:
    RPC_URLS = [RPC_URL, *RPC_URLS]
RPC_URLS = RPC_URLS or ([] if not RPC_URL else [RPC_URL])

# Jupiter
JUP_BASE = _envs(
    "JUPITER_SWAP_API", _envs("JUPITER_QUOTE_API", "https://quote-api.jup.ag/v6")
).rstrip("/")
JUP_ENABLE = _envb("INFRA_ENABLE_JUP_CHECK", True)
JUP_TIMEOUT_S = _envf("INFRA_JUP_TIMEOUT_S", 8.0)

# Mints for lightweight quote
USDC_MINT = _envs("USDC_MINT", "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v")
SOL_MINT = _envs("SOL_MINT", "So11111111111111111111111111111111111111112")
JUP_TEST_AMOUNT = _envi("INFRA_JUP_TEST_AMOUNT", 1_000_000)  # lamports (0.001 SOL)

# Redis
REDIS_URL = _envs("REDIS_URL", _envs("UPSTASH_REDIS_URL", "redis://localhost:6379"))

# Poll cadence
POLL_SEC = max(5, _envi("INFRA_POLL_SEC", 20))
JITTER_SEC = _clamp(_envf("INFRA_JITTER_SEC", 0.8), 0.0, 10.0)

# Timeouts
RPC_TIMEOUT_S = _envf("INFRA_RPC_TIMEOUT_S", 8.0)

# Latency thresholds (ms)
RPC_WARN_MS = _envi("INFRA_LATENCY_WARN_MS", 800)
RPC_HALT_MS = _envi("INFRA_LATENCY_MAX_MS", 1200)

JUP_WARN_MS = _envi("INFRA_JUP_WARN_MS", 1000)
JUP_HALT_MS = _envi("INFRA_JUP_MAX_MS", 1500)

# Score smoothing (flap control)
# bad_score := bad_score*DECAY + sample_bad (0/1)  ; warn_score similar
SCORE_DECAY = _clamp(_envf("INFRA_SCORE_DECAY", 0.65), 0.05, 0.99)
DEGRADED_SCORE = _clamp(_envf("INFRA_DEGRADED_SCORE", 1.2), 0.0, 50.0)
HALT_SCORE = _clamp(_envf("INFRA_HALT_SCORE", 3.2), 0.0, 100.0)

# Behavior toggles
FAIL_OPEN = _envb("INFRA_FAIL_OPEN", False)
BLOCK_ON_DEGRADED = _envb("INFRA_BLOCK_ON_DEGRADED", False)
ENABLE_RPC_CHECK = _envb("INFRA_ENABLE_RPC_CHECK", True)

# Alerts
TG_ALERTS = _envb("INFRA_TELEGRAM_ALERTS", True)
ALERT_ON_TRANSITION = _envb("INFRA_ALERT_ON_TRANSITION", True)
ALERT_COOLDOWN_S = _envi("INFRA_ALERT_COOLDOWN_S", 90)

# Redis TTLs
KEY_TTL_SEC = max(POLL_SEC * 3, _envi("INFRA_KEY_TTL_SEC", POLL_SEC * 3))
SNAPSHOT_TTL_SEC = max(300, _envi("INFRA_SNAPSHOT_TTL_SEC", 3600))

# Optional: trip breaker when HALTED
TRIP_BREAKER_ON_HALT = _envb("INFRA_TRIP_BREAKER_ON_HALT", True)
BREAKER_COOLDOWN_S = _envf("INFRA_BREAKER_COOLDOWN_S", float(POLL_SEC * 2))

# Optional: publish degraded mode signal to circuit_breakers shared key (TTL refreshed)
PUBLISH_DEGRADED_TO_CB = _envb("INFRA_PUBLISH_DEGRADED", True)
DEGRADED_TTL_SEC = max(POLL_SEC * 3, _envi("INFRA_DEGRADED_TTL_SEC", POLL_SEC * 3))

# Debug
DEBUG_LOG = _envb("INFRA_DEBUG_LOG", False)

# Canonical Redis keys (keep EXACT for backwards compatibility)
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
# Redis helpers
# =============================================================================

_redis_cached: Any | None = None
_redis_cached_at: float = 0.0
_REDIS_REFRESH_S = 5.0


async def _get_redis() -> Any | None:
    """
    Prefer core_utils async redis getter; otherwise shared redis; otherwise create one.
    Never raises.
    """
    global _redis_cached, _redis_cached_at
    try:
        now = _now()
        if _redis_cached is not None and (now - _redis_cached_at) < _REDIS_REFRESH_S:
            return _redis_cached

        _redis_cached_at = now

        # 1) core_utils.get_redis_async()
        if cu is not None:
            fn = getattr(cu, "get_redis_async", None)
            if callable(fn):
                r = fn()
                if asyncio.iscoroutine(r):
                    r = await r
                if r is not None:
                    _redis_cached = r
                    return _redis_cached

            # 2) core_utils.redis (shared singleton)
            r2 = getattr(cu, "redis", None)
            if r2 is not None:
                _redis_cached = r2
                return _redis_cached

        # 3) local aioredis client
        if aioredis is None:
            _redis_cached = None
            return None
        if not REDIS_URL:
            _redis_cached = None
            return None

        _redis_cached = aioredis.from_url(REDIS_URL, decode_responses=True)
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
    if not rpc_url:
        return 10_000, bool(FAIL_OPEN), "missing_rpc_url"

    payload = {"jsonrpc": "2.0", "id": 1, "method": "getHealth"}
    t0 = time.perf_counter()
    try:
        async with session.post(rpc_url, json=payload, timeout=RPC_TIMEOUT_S) as r:
            txt = await r.text()
            ms = int((time.perf_counter() - t0) * 1000)
            if r.status != 200:
                return ms, bool(FAIL_OPEN), f"http_{r.status}"
            # Parse minimal JSON to ensure not a JSON-RPC error
            ok = True
            try:
                j = json.loads(txt) if txt else {}
                if isinstance(j, dict) and j.get("error"):
                    ok = False
                if isinstance(j, dict) and j.get("result") not in (None, "ok"):
                    # some RPCs return "ok"; tolerate missing, but non-ok strings treated as non-ok
                    if j.get("result") != "ok":
                        ok = False
            except Exception:
                # If body is weird but HTTP ok, treat as non-ok unless fail-open
                ok = bool(FAIL_OPEN)

            return ms, bool(ok), "ok" if ok else "rpc_error"
    except Exception as e:
        if DEBUG_LOG:
            with contextlib.suppress(Exception):
                log.debug("RPC probe error %s: %s", rpc_url, e)
        return 10_000, bool(FAIL_OPEN), "exception"


async def _rpc_latency(session: aiohttp.ClientSession) -> tuple[int, bool, str, str]:
    """
    Probe across multiple RPC_URLS; return the best (lowest latency) successful one.
    Returns (lat_ms, ok, detail, chosen_url).
    """
    if not ENABLE_RPC_CHECK:
        return 0, True, "rpc_check_disabled", ""

    if not RPC_URLS:
        return 10_000, bool(FAIL_OPEN), "no_rpc_urls", ""

    # Probe a subset each tick to reduce load if list is huge
    urls = RPC_URLS[:]
    random.shuffle(urls)
    urls = urls[: min(3, len(urls))]

    results = await asyncio.gather(*[_probe_rpc(session, u) for u in urls], return_exceptions=True)

    best_ok = None
    best_ms = 10_000
    best_detail = "no_ok"
    best_url = urls[0] if urls else ""

    for i, res in enumerate(results):
        if isinstance(res, Exception):
            continue
        ms, ok, detail = res
        if ok:
            if best_ok is None or ms < best_ms:
                best_ok = True
                best_ms = ms
                best_detail = detail
                best_url = urls[i]
        else:
            # If nothing ok, keep the least-bad latency for context
            if best_ok is None and ms < best_ms:
                best_ms = ms
                best_detail = detail
                best_url = urls[i]

    if best_ok is True:
        return int(best_ms), True, best_detail, best_url
    return int(best_ms), bool(FAIL_OPEN), best_detail, best_url


async def _jup_latency(session: aiohttp.ClientSession) -> tuple[int, bool, str]:
    """
    Probe Jupiter quote API via /quote. Returns (latency_ms, ok, detail).
    """
    if not JUP_ENABLE:
        return 0, True, "jup_check_disabled"

    # Minimal quote
    params = {
        "inputMint": SOL_MINT,
        "outputMint": USDC_MINT,
        "amount": str(int(JUP_TEST_AMOUNT)),
        "slippageBps": "50",
    }

    t0 = time.perf_counter()
    try:
        async with session.get(f"{JUP_BASE}/quote", params=params, timeout=JUP_TIMEOUT_S) as r:
            txt = await r.text()
            ms = int((time.perf_counter() - t0) * 1000)
            if r.status != 200:
                return ms, bool(FAIL_OPEN), f"http_{r.status}"

            # Validate response shape lightly
            ok = True
            try:
                j = json.loads(txt) if txt else {}
                # v6 typically includes "data": [...]
                if isinstance(j, dict) and "data" in j and not j.get("data"):
                    ok = False
                if isinstance(j, dict) and j.get("error"):
                    ok = False
            except Exception:
                ok = bool(FAIL_OPEN)

            return ms, bool(ok), "ok" if ok else "bad_payload"
    except Exception as e:
        if DEBUG_LOG:
            with contextlib.suppress(Exception):
                log.debug("JUP probe error: %s", e)
        return 10_000, bool(FAIL_OPEN), "exception"


# =============================================================================
# Snapshot / state logic
# =============================================================================


@dataclass
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
    *,
    rpc_ms: int,
    rpc_ok: bool,
    jup_ms: int,
    jup_ok: bool,
) -> tuple[bool, bool, str]:
    """
    Returns:
      sample_bad (hard bad), sample_warn (soft warn), reason_fragment
    """
    reasons: list[str] = []

    # RPC
    rpc_hard = (not rpc_ok) or (rpc_ms > RPC_HALT_MS)
    rpc_warn = (rpc_ms > RPC_WARN_MS) and (not rpc_hard)
    if rpc_hard:
        reasons.append(f"RPC bad(ok={rpc_ok}, {rpc_ms}ms)")
    elif rpc_warn:
        reasons.append(f"RPC slow({rpc_ms}ms)")

    # JUP
    jup_hard = False
    jup_warn = False
    if JUP_ENABLE:
        jup_hard = (not jup_ok) or (jup_ms > JUP_HALT_MS)
        jup_warn = (jup_ms > JUP_WARN_MS) and (not jup_hard)
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


def _state_from_scores(
    *,
    bad_score: float,
    warn_score: float,
) -> str:
    if bad_score >= HALT_SCORE:
        return STATE_HALTED
    if warn_score >= DEGRADED_SCORE:
        return STATE_DEGRADED
    return STATE_HEALTHY


# =============================================================================
# Public read helpers
# =============================================================================


async def infra_snapshot(redis=None) -> dict[str, Any]:
    """
    Read last snapshot from Redis; returns a safe default if missing.
    """
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
            try:
                return json.loads(raw)
            except Exception:
                pass

        # fallback to individual keys
        st = await _redis_get(redis, KEY_LAST_STATE) or STATE_HEALTHY
        rsn = await _redis_get(redis, KEY_LAST_REASON) or "no snapshot"
        halted = (await _redis_get(redis, KEY_HALTED) or "0") == "1"
        degraded = (await _redis_get(redis, KEY_DEGRADED) or "0") == "1"
        return {
            "ts": _iso_utc(),
            "state": STATE_HALTED if halted else (STATE_DEGRADED if degraded else str(st)),
            "reason": str(rsn),
        }
    except Exception:
        return {"ts": _iso_utc(), "state": STATE_HEALTHY, "reason": "snapshot_failed_default"}


async def is_halted(redis=None) -> bool:
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


async def is_degraded(redis=None) -> bool:
    try:
        if redis is None:
            redis = await _get_redis()
        if not redis:
            return False
        v = await _redis_get(redis, KEY_DEGRADED)
        return str(v or "0") == "1"
    except Exception:
        return False


async def should_block_entries(redis=None) -> bool:
    """
    Canonical helper:
      - Always block when HALTED
      - Optionally block when DEGRADED
    """
    try:
        if await is_halted(redis):
            return True
        return bool(BLOCK_ON_DEGRADED and await is_degraded(redis))
    except Exception:
        return False


# =============================================================================
# Manual overrides
# =============================================================================


async def set_infra_halt(reason: str | None = None) -> None:
    """
    Persist manual halt override. No TTL.
    """
    try:
        redis = await _get_redis()
        if not redis:
            return

        await _redis_set(redis, KEY_MANUAL_HALT, "1")
        if reason:
            await _redis_set(redis, KEY_MANUAL_REASON, str(reason)[:300])

        # Keep canonical flags aligned (TTL so if guard dies, it still blocks for a bit)
        await _redis_setex(redis, KEY_HALTED, "1", KEY_TTL_SEC)
        await _redis_setex(redis, KEY_DEGRADED, "1", KEY_TTL_SEC)
        await _redis_set(redis, KEY_LAST_STATE, STATE_HALTED)
        await _redis_set(
            redis, KEY_LAST_REASON, f"manual_halt: {str(reason)[:200] if reason else ''}".strip()
        )

        snap = await infra_snapshot(redis)
        snap["state"] = STATE_HALTED
        if reason:
            snap["reason"] = f"manual_halt: {reason}"
        await _redis_setex(redis, KEY_LAST_SNAPSHOT, json.dumps(snap), SNAPSHOT_TTL_SEC)

        if TG_ALERTS:
            await _send_telegram("🚨 *MANUAL INFRA HALT SET*\n" + f"• reason: {reason or 'n/a'}")
    except Exception:
        return


async def clear_infra_halt() -> None:
    """
    Clear manual halt override. The next guard tick will re-classify.
    """
    try:
        redis = await _get_redis()
        if not redis:
            return

        await _redis_set(redis, KEY_MANUAL_HALT, "0")
        await _redis_set(redis, KEY_MANUAL_REASON, "cleared")

        # Do not force flags here; let the guard loop compute on next tick.
        await _redis_set(redis, KEY_LAST_REASON, "manual clear_infra_halt()")

        if TG_ALERTS:
            await _send_telegram("🟢 Manual infra halt cleared.")
    except Exception:
        return


# =============================================================================
# AI / FeatureBuilder helpers
# =============================================================================


async def infra_features(redis=None) -> dict[str, float]:
    """
    Numeric infra features (model-friendly).
    """
    snap = await infra_snapshot(redis)

    state = str(snap.get("state", STATE_HEALTHY))
    rpc_ms = float(snap.get("rpc_latency_ms", 0.0) or 0.0)
    jup_ms = float(snap.get("jup_latency_ms", 0.0) or 0.0)
    bad_score = float(snap.get("bad_score", 0.0) or 0.0)
    warn_score = float(snap.get("warn_score", 0.0) or 0.0)

    is_h = 1.0 if state == STATE_HEALTHY else 0.0
    is_d = 1.0 if state in (STATE_DEGRADED, STATE_HALTED) else 0.0
    is_x = 1.0 if state == STATE_HALTED else 0.0

    rpc_norm = _norm_between(rpc_ms, 0.0, float(max(1, RPC_HALT_MS)))
    jup_norm = _norm_between(jup_ms, 0.0, float(max(1, JUP_HALT_MS)))

    # Simple bounded health score
    # healthy=1, degraded~0.5, halted=0
    health_score = 1.0 * is_h + 0.5 * (1.0 if state == STATE_DEGRADED else 0.0) + 0.0 * is_x
    stability_score = 1.0 / (1.0 + warn_score)

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


async def attach_infra_features(meta: dict[str, Any], redis=None) -> None:
    """
    Mutates meta dict with infra_* numeric features.
    """
    try:
        feats = await infra_features(redis)
        meta.update(feats)
    except Exception:
        return


# =============================================================================
# One-shot check
# =============================================================================


async def quick_check() -> dict[str, Any]:
    """
    One-shot infra check without writing to Redis.
    """
    async with aiohttp.ClientSession() as session:
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
    redis = await _get_redis()

    bad_score: float = 0.0
    warn_score: float = 0.0

    # rolling latency windows (for p95 context)
    rpc_hist: list[float] = []
    jup_hist: list[float] = []
    hist_max = max(10, _envi("INFRA_HIST_MAX", 60))

    last_state: str | None = None
    last_alert_at: float = 0.0
    last_trip_state: str | None = None

    # Start log
    with contextlib.suppress(Exception):
        log.info(
            "INFRA_GUARD_START poll=%ss rpc_check=%s jup_check=%s fail_open=%s block_on_degraded=%s",
            POLL_SEC,
            ENABLE_RPC_CHECK,
            JUP_ENABLE,
            FAIL_OPEN,
            BLOCK_ON_DEGRADED,
        )

    if TG_ALERTS:
        await _send_telegram("🛡️ Infra guard started.")

    timeout = aiohttp.ClientTimeout(total=max(RPC_TIMEOUT_S, JUP_TIMEOUT_S) + 2.0)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        while True:
            if stop_evt is not None and stop_evt.is_set():
                with contextlib.suppress(Exception):
                    log.info("INFRA_GUARD_STOP stop_evt")
                if TG_ALERTS:
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

            # Probes concurrently (faster, same tick)
            rpc_task = _rpc_latency(session)
            jup_task = _jup_latency(session)
            rpc_ms, rpc_ok, rpc_detail, rpc_url = await rpc_task
            jup_ms, jup_ok, jup_detail = await jup_task

            # Update hist
            rpc_hist.append(float(rpc_ms))
            if len(rpc_hist) > hist_max:
                rpc_hist = rpc_hist[-hist_max:]
            if JUP_ENABLE:
                jup_hist.append(float(jup_ms))
                if len(jup_hist) > hist_max:
                    jup_hist = jup_hist[-hist_max:]

            # Record probe errors into circuit_breakers (optional)
            if cb is not None:
                try:
                    cb.record_error(
                        bool(rpc_ok), kind="rpc_probe", where=rpc_url, ms=rpc_ms, detail=rpc_detail
                    )
                    if JUP_ENABLE:
                        cb.record_error(
                            bool(jup_ok),
                            kind="jup_probe",
                            where=JUP_BASE,
                            ms=jup_ms,
                            detail=jup_detail,
                        )
                except Exception:
                    pass

            # Sample classification
            sample_bad, sample_warn, frag = _classify_sample(
                rpc_ms=rpc_ms, rpc_ok=rpc_ok, jup_ms=jup_ms, jup_ok=jup_ok
            )

            # Smooth scores (anti-flap)
            bad_score = bad_score * SCORE_DECAY + (1.0 if sample_bad else 0.0)
            warn_score = warn_score * SCORE_DECAY + (1.0 if sample_warn else 0.0)

            state = _state_from_scores(bad_score=bad_score, warn_score=warn_score)

            # Manual override dominates
            reason = frag
            if manual_active:
                state = STATE_HALTED
                reason = (
                    f"manual_halt: {manual_reason[:200]}; {frag}"
                    if manual_reason
                    else f"manual_halt; {frag}"
                )

            # p95 context
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

            # Persist Redis keys
            if redis:
                try:
                    halted_flag = "1" if state == STATE_HALTED else "0"
                    degraded_flag = "1" if state in (STATE_DEGRADED, STATE_HALTED) else "0"

                    await _redis_setex(redis, KEY_HALTED, halted_flag, KEY_TTL_SEC)
                    await _redis_setex(redis, KEY_DEGRADED, degraded_flag, KEY_TTL_SEC)
                    await _redis_set(redis, KEY_LAST_STATE, state)
                    await _redis_set(redis, KEY_LAST_REASON, reason[:300])
                    await _redis_setex(
                        redis, KEY_LAST_SNAPSHOT, json.dumps(snap.to_dict()), SNAPSHOT_TTL_SEC
                    )
                except Exception as e:
                    if DEBUG_LOG:
                        with contextlib.suppress(Exception):
                            log.debug("infra_guard redis write error: %s", e)

            # Publish degraded signal into circuit_breakers shared key (TTL refreshed) (optional)
            if (
                PUBLISH_DEGRADED_TO_CB
                and cb is not None
                and state in (STATE_DEGRADED, STATE_HALTED)
            ):
                with contextlib.suppress(Exception):
                    cb.set_degraded(
                        True, reason=f"infra_guard:{state}", ttl_sec=int(DEGRADED_TTL_SEC)
                    )

            # Trip breaker on transition into HALTED (optional)
            if (
                TRIP_BREAKER_ON_HALT
                and cb is not None
                and state == STATE_HALTED
                and last_trip_state != STATE_HALTED
            ):
                last_trip_state = STATE_HALTED
                with contextlib.suppress(Exception):
                    cb.trip_breaker(seconds=float(BREAKER_COOLDOWN_S), reason="infra_halted")
            if state != STATE_HALTED and last_trip_state == STATE_HALTED:
                last_trip_state = state

            # Transition-based alerting (rate-limited)
            if (not ALERT_ON_TRANSITION) or (state != last_state):
                now = _now()
                if (now - last_alert_at) >= float(ALERT_COOLDOWN_S):
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
                            JUP_ENABLE,
                            bad_score,
                            warn_score,
                            reason,
                        )

                    if TG_ALERTS:
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
                                f"• JUP: {jup_ms}ms (ok={jup_ok}, enabled={JUP_ENABLE})\n"
                                f"• JUP p95: {jup_p95}ms\n"
                                f"• scores: bad={bad_score:.2f} warn={warn_score:.2f}\n"
                                f"• manual: {manual_active}\n"
                                f"• reason: {reason[:280]}"
                            )

            # Sleep with jitter, responsive to stop_evt
            sleep_s = float(POLL_SEC) + (random.random() * float(JITTER_SEC))
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
    "infra_features",
    "infra_snapshot",
    "is_degraded",
    "is_halted",
    "quick_check",
    "run_infra_guard",
    "set_infra_halt",
    "should_block_entries",
]
