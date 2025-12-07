from __future__ import annotations

"""
utils_pkg/infra_guard.py

Infra guard (V3+++ / quant-grade)
=================================

High-level role
---------------
Central infra health brain for the whole stack:

  • Continuously probe:
       - Solana RPC health / latency
       - Jupiter quote API health / latency (optional & configurable)

  • Classify infra state:
       - HEALTHY    → infra:halt = 0, infra:degraded = 0
       - DEGRADED   → infra:halt = 0, infra:degraded = 1
       - HALTED     → infra:halt = 1, infra:degraded = 1

  • Persist infra snapshots to Redis:
       - infra:last_snapshot (JSON payload)
       - infra:halt        (0/1)
       - infra:degraded    (0/1)
       - infra:last_state  ("healthy" | "degraded" | "halted")
       - infra:last_reason (free text)
       - infra:manual_halt (0/1) + infra:manual_reason (when set)

  • Provide helpers for the rest of the system:
       - async infra_snapshot(redis=None)        -> dict
       - async infra_features(redis=None)        -> dict[feature_name,float]
       - async attach_infra_features(meta, ...)  -> None (mutates meta)
       - async is_halted(redis=None)             -> bool
       - async is_degraded(redis=None)           -> bool
       - async should_block_entries(redis=None)  -> bool
       - async set_infra_halt(reason: str | None = None) -> None
       - async clear_infra_halt() -> None

  • Integrate with risk_manager / Strategy:
       - risk_manager.should_allow_new_entry() reads infra:halt / infra:degraded.
       - AI / FeatureBuilder can consume infra_features().

Usage
-----
In main bootstrap:

    from utils_pkg.infra_guard import run_infra_guard

    stop_evt = asyncio.Event()
    asyncio.create_task(run_infra_guard(stop_evt), name="infra.guard")

CLI:

    python -m utils_pkg.infra_guard

"""

import asyncio
import json
import os
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple

import aiohttp

from logging_setup import get_logger
from utils_pkg.core_utils import send_telegram, redis as shared_redis

# Optional standalone redis client (fallback if shared_redis is None)
try:  # pragma: no cover
    import redis.asyncio as aioredis
except Exception:  # pragma: no cover
    aioredis = None  # type: ignore[assignment]

log = get_logger("sniper.infra_guard")


# =============================================================================
# ENV / helpers
# =============================================================================


def _env_bool(key: str, default: bool = False) -> bool:
    v = os.getenv(key)
    if v is None:
        return default
    return str(v).strip().lower() in ("1", "true", "yes", "y", "on")


def _env_int(key: str, default: int) -> int:
    try:
        return int(os.getenv(key, str(default)))
    except Exception:
        return default


def _env_float(key: str, default: float) -> float:
    try:
        return float(os.getenv(key, str(default)))
    except Exception:
        return default


def _clamp(x: float, lo: float, hi: float) -> float:
    if x < lo:
        return lo
    if x > hi:
        return hi
    return x


def _norm_between(x: float, lo: float, hi: float) -> float:
    if hi <= lo:
        return 0.5
    return _clamp((x - lo) / (hi - lo), 0.0, 1.0)


# Core endpoints
RPC_URL = os.getenv("RPC_URL")  # must be set; if missing we treat infra as bad
USDC_MINT = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
SOL_MINT = "So11111111111111111111111111111111111111112"

JUP_API = os.getenv("JUPITER_SWAP_API", "https://quote-api.jup.ag/v6")
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")

# Latency thresholds (ms)
INFRA_LATENCY_MAX_MS = _env_int("INFRA_LATENCY_MAX_MS", 1200)   # hard halt
INFRA_LATENCY_WARN_MS = _env_int("INFRA_LATENCY_WARN_MS", 800)  # degraded

INFRA_JUP_MAX_MS = _env_int("INFRA_JUP_MAX_MS", 1500)   # hard halt
INFRA_JUP_WARN_MS = _env_int("INFRA_JUP_WARN_MS", 1000)  # degraded

# Error streak configuration
INFRA_ERROR_BURST = _env_int("INFRA_ERROR_BURST", 5)          # before HALTED
INFRA_ERROR_DECAY = _env_float("INFRA_ERROR_DECAY", 0.5)      # soft decay

# Behavior toggles
INFRA_ENABLE_JUP_CHECK = _env_bool("INFRA_ENABLE_JUP_CHECK", True)
INFRA_FAIL_OPEN = _env_bool("INFRA_FAIL_OPEN", False)
INFRA_BLOCK_ON_DEGRADED = _env_bool("INFRA_BLOCK_ON_DEGRADED", False)
INFRA_TELEGRAM_ALERTS = _env_bool("INFRA_TELEGRAM_ALERTS", True)
INFRA_DEBUG_LOG = _env_bool("INFRA_DEBUG_LOG", False)

# Poll cadence
POLL_SEC = max(5, _env_int("INFRA_POLL_SEC", 20))

# Redis keys
KEY_HALTED = "infra:halt"                # "0"/"1"
KEY_DEGRADED = "infra:degraded"          # "0"/"1"
KEY_LAST_STATE = "infra:last_state"      # healthy/degraded/halted
KEY_LAST_REASON = "infra:last_reason"
KEY_LAST_SNAPSHOT = "infra:last_snapshot"

# Manual override keys
KEY_MANUAL_HALT = "infra:manual_halt"     # "0"/"1"
KEY_MANUAL_REASON = "infra:manual_reason"

# Internal state labels
STATE_HEALTHY = "healthy"
STATE_DEGRADED = "degraded"
STATE_HALTED = "halted"


# =============================================================================
# Redis helpers
# =============================================================================


async def _get_redis():
    """
    Prefer shared_redis from core_utils; otherwise build our own.
    """
    if shared_redis is not None:
        return shared_redis
    if aioredis is None:
        return None
    return aioredis.from_url(REDIS_URL, decode_responses=True)


# =============================================================================
# Probes
# =============================================================================


async def _rpc_latency(session: aiohttp.ClientSession) -> Tuple[int, bool]:
    """
    Measure RPC latency via getHealth. Returns (latency_ms, ok).
    """
    if not RPC_URL:
        # Without RPC_URL infra should be treated as broken unless fail-open.
        return 10_000, bool(INFRA_FAIL_OPEN)

    payload = {"jsonrpc": "2.0", "id": 1, "method": "getHealth"}
    t0 = time.time()
    try:
        async with session.post(RPC_URL, json=payload, timeout=10) as r:
            # We don't care about content; just latency + status.
            await r.text()
            ms = int((time.time() - t0) * 1000)
            ok = (r.status == 200)
            return ms, ok
    except Exception as e:  # pragma: no cover
        if INFRA_DEBUG_LOG:
            log.debug("RPC probe error: %s", e)
        return 10_000, bool(INFRA_FAIL_OPEN)


async def _jup_latency(session: aiohttp.ClientSession) -> Tuple[int, bool]:
    """
    Measure Jupiter quote latency. Returns (latency_ms, ok).

    Uses a tiny SOL→USDC quote to keep load negligible.
    """
    if not INFRA_ENABLE_JUP_CHECK:
        return 0, True

    params = {
        "inputMint": SOL_MINT,
        "outputMint": USDC_MINT,
        "amount": 1_000_000,  # 0.001 SOL in lamports
        "slippageBps": 50,
    }
    t0 = time.time()
    try:
        async with session.get(f"{JUP_API}/quote", params=params, timeout=10) as r:
            await r.text()
            ms = int((time.time() - t0) * 1000)
            ok = (r.status == 200)
            return ms, ok
    except Exception as e:  # pragma: no cover
        if INFRA_DEBUG_LOG:
            log.debug("JUP probe error: %s", e)
        return 10_000, bool(INFRA_FAIL_OPEN)


# =============================================================================
# Snapshot / state logic
# =============================================================================


@dataclass
class InfraSnapshot:
    ts: str
    rpc_latency_ms: int
    rpc_ok: bool
    jup_latency_ms: int
    jup_ok: bool
    error_streak: float
    state: str
    reason: str

    def to_dict(self) -> Dict[str, Any]:
        return {
            "ts": self.ts,
            "rpc_latency_ms": int(self.rpc_latency_ms),
            "rpc_ok": bool(self.rpc_ok),
            "jup_latency_ms": int(self.jup_latency_ms),
            "jup_ok": bool(self.jup_ok),
            "error_streak": float(self.error_streak),
            "state": self.state,
            "reason": self.reason,
        }


def _classify_state(
    *,
    rpc_ms: int,
    rpc_ok: bool,
    jup_ms: int,
    jup_ok: bool,
    error_streak: float,
) -> Tuple[str, str, bool, bool]:
    """
    Decide state (healthy/degraded/halted) and reason string.

    Returns:
      (state, reason, is_bad_sample, is_hard_halt_sample)

    is_bad_sample:
      - counts towards error_streak.
    is_hard_halt_sample:
      - currently same as is_bad_sample (can be refined).
    """
    # Hard halt conditions: huge latency or outright failures
    rpc_hard_bad = (not rpc_ok) or (rpc_ms > INFRA_LATENCY_MAX_MS)
    jup_hard_bad = INFRA_ENABLE_JUP_CHECK and (
        (not jup_ok) or (jup_ms > INFRA_JUP_MAX_MS)
    )

    # Degraded conditions: warnings but not catastrophic
    rpc_warn = rpc_ms > INFRA_LATENCY_WARN_MS
    jup_warn = INFRA_ENABLE_JUP_CHECK and (jup_ms > INFRA_JUP_WARN_MS)

    is_bad_sample = rpc_hard_bad or jup_hard_bad
    is_hard_halt_sample = is_bad_sample

    # Base classification ignoring streak
    if rpc_hard_bad or jup_hard_bad:
        base_state = STATE_DEGRADED  # may be escalated by streak
    elif rpc_warn or jup_warn:
        base_state = STATE_DEGRADED
    else:
        base_state = STATE_HEALTHY

    # Escalate to HALTED when we've seen a burst of bad samples
    if base_state != STATE_HEALTHY and error_streak >= INFRA_ERROR_BURST:
        state = STATE_HALTED
    else:
        state = base_state

    # Build human-readable reason
    reasons = []
    if rpc_hard_bad:
        reasons.append(f"RPC bad (ok={rpc_ok}, latency={rpc_ms}ms)")
    elif rpc_warn:
        reasons.append(f"RPC slow ({rpc_ms}ms)")

    if INFRA_ENABLE_JUP_CHECK:
        if jup_hard_bad:
            reasons.append(f"JUP bad (ok={jup_ok}, latency={jup_ms}ms)")
        elif jup_warn:
            reasons.append(f"JUP slow ({jup_ms}ms)")
    else:
        reasons.append("JUP check disabled")

    if not reasons:
        reasons.append("all checks within thresholds")

    reason = "; ".join(reasons)
    return state, reason, is_bad_sample, is_hard_halt_sample


# =============================================================================
# Public helpers (for other modules)
# =============================================================================


async def infra_snapshot(redis=None) -> Dict[str, Any]:
    """
    Read last snapshot from Redis; if missing, return a default healthy-ish view.
    """
    if redis is None:
        redis = await _get_redis()
    if not redis:
        # best-effort default
        snap = InfraSnapshot(
            ts=datetime.now(timezone.utc).isoformat(),
            rpc_latency_ms=0,
            rpc_ok=True,
            jup_latency_ms=0,
            jup_ok=True,
            error_streak=0.0,
            state=STATE_HEALTHY,
            reason="no redis; default healthy",
        )
        return snap.to_dict()

    try:
        raw = await redis.get(KEY_LAST_SNAPSHOT)
        if not raw:
            raise KeyError("no snapshot")
        return json.loads(raw)
    except Exception:
        snap = InfraSnapshot(
            ts=datetime.now(timezone.utc).isoformat(),
            rpc_latency_ms=0,
            rpc_ok=True,
            jup_latency_ms=0,
            jup_ok=True,
            error_streak=0.0,
            state=STATE_HEALTHY,
            reason="no snapshot; default healthy",
        )
        return snap.to_dict()


async def is_halted(redis=None) -> bool:
    """
    True when infra is in HALTED state OR manual halt is active.
    """
    if redis is None:
        redis = await _get_redis()
    if not redis:
        return False
    try:
        manual = await redis.get(KEY_MANUAL_HALT)
        if str(manual) == "1":
            return True
        v = await redis.get(KEY_HALTED)
        return str(v) == "1"
    except Exception:
        return False


async def is_degraded(redis=None) -> bool:
    """
    True when infra is degraded or halted.
    """
    if redis is None:
        redis = await _get_redis()
    if not redis:
        return False
    try:
        v = await redis.get(KEY_DEGRADED)
        return str(v) == "1"
    except Exception:
        return False


async def should_block_entries(redis=None) -> bool:
    """
    Canonical helper for Strategy / risk_manager:

      - Block entries when HALTED.
      - Optionally (INFRA_BLOCK_ON_DEGRADED=1) block on degraded too.
    """
    if await is_halted(redis):
        return True
    if INFRA_BLOCK_ON_DEGRADED and await is_degraded(redis):
        return True
    return False


async def set_infra_halt(reason: Optional[str] = None) -> None:
    """
    Manually force infra halt (e.g., invoked from admin CLI / Discord bot).

    This sets a persistent manual flag; the guard loop will respect it and
    always treat state as HALTED until clear_infra_halt() is called.
    """
    redis = await _get_redis()
    if not redis:
        return
    try:
        await redis.set(KEY_MANUAL_HALT, "1")
        if reason:
            await redis.set(KEY_MANUAL_REASON, reason)
        # Keep backward-compat infra:* keys aligned
        await redis.set(KEY_HALTED, "1")
        await redis.set(KEY_DEGRADED, "1")
        await redis.set(KEY_LAST_STATE, STATE_HALTED)
        if reason:
            await redis.set(KEY_LAST_REASON, reason)

        snap = await infra_snapshot(redis)
        snap["state"] = STATE_HALTED
        if reason:
            snap["reason"] = reason
        await redis.set(KEY_LAST_SNAPSHOT, json.dumps(snap))
        if INFRA_TELEGRAM_ALERTS:
            try:
                await send_telegram(
                    "🚨 *MANUAL INFRA HALT SET*\n"
                    f"• reason: {reason or 'no reason provided'}"
                )
            except Exception:
                pass
    except Exception:
        pass


async def clear_infra_halt() -> None:
    """
    Clear manual halt and mark infra as healthy. The guard loop will
    re-classify on next tick, but this is useful for manual override.
    """
    redis = await _get_redis()
    if not redis:
        return
    try:
        await redis.set(KEY_MANUAL_HALT, "0")
        await redis.set(KEY_MANUAL_REASON, "cleared")
        await redis.set(KEY_HALTED, "0")
        await redis.set(KEY_DEGRADED, "0")
        await redis.set(KEY_LAST_STATE, STATE_HEALTHY)
        await redis.set(KEY_LAST_REASON, "manual clear_infra_halt()")

        snap = await infra_snapshot(redis)
        snap["state"] = STATE_HEALTHY
        snap["reason"] = "manual clear_infra_halt()"
        await redis.set(KEY_LAST_SNAPSHOT, json.dumps(snap))

        if INFRA_TELEGRAM_ALERTS:
            try:
                await send_telegram("🟢 Manual infra halt cleared.")
            except Exception:
                pass
    except Exception:
        pass


# =============================================================================
# AI / FeatureBuilder helpers
# =============================================================================


async def infra_features(redis=None) -> Dict[str, float]:
    """
    Numeric infra feature snapshot for FeatureBuilder / AI modules.

    All values are numeric for model-friendliness.
    """
    snap = await infra_snapshot(redis)

    rpc_ms = int(snap.get("rpc_latency_ms", 0))
    jup_ms = int(snap.get("jup_latency_ms", 0))
    error_streak = float(snap.get("error_streak", 0.0))
    state = str(snap.get("state", STATE_HEALTHY))

    halted = 1.0 if state == STATE_HALTED else 0.0
    degraded = 1.0 if state in (STATE_DEGRADED, STATE_HALTED) else 0.0
    healthy = 1.0 if state == STATE_HEALTHY else 0.0

    # Latency normalization
    rpc_lat_norm = _norm_between(
        float(rpc_ms),
        0.0,
        float(max(1, INFRA_LATENCY_MAX_MS)),
    )
    jup_lat_norm = _norm_between(
        float(jup_ms),
        0.0,
        float(max(1, INFRA_JUP_MAX_MS)),
    )

    # Health score: 1 = fully healthy, 0 = halted
    health_score = (
        1.0 * healthy +
        0.4 * (1.0 if state == STATE_DEGRADED else 0.0) +
        0.0 * halted
    )

    stability_score = 1.0 / (1.0 + error_streak)  # decays as streak grows

    return {
        "infra_rpc_latency_ms": float(rpc_ms),
        "infra_jup_latency_ms": float(jup_ms),
        "infra_rpc_latency_norm": rpc_lat_norm,
        "infra_jup_latency_norm": jup_lat_norm,
        "infra_error_streak": float(error_streak),
        "infra_state_is_healthy": healthy,
        "infra_state_is_degraded": degraded,
        "infra_state_is_halted": halted,
        "infra_health_score": _clamp(health_score, 0.0, 1.0),
        "infra_stability_score": _clamp(stability_score, 0.0, 1.0),
    }


async def attach_infra_features(meta: Dict[str, Any], redis=None) -> None:
    """
    Convenience helper for FeatureBuilder:

        meta = {...}
        await attach_infra_features(meta)
    """
    feats = await infra_features(redis)
    meta.update(feats)


# =============================================================================
# Quick one-shot check (for scripts / health endpoints)
# =============================================================================


async def quick_check() -> Dict[str, Any]:
    """
    One-shot infra check without relying on the background loop.

    Useful for scripts / health endpoints; does NOT write to Redis.
    """
    async with aiohttp.ClientSession() as session:
        rpc_ms, rpc_ok = await _rpc_latency(session)
        jup_ms, jup_ok = await _jup_latency(session)
    state, reason, _, _ = _classify_state(
        rpc_ms=rpc_ms,
        rpc_ok=rpc_ok,
        jup_ms=jup_ms,
        jup_ok=jup_ok,
        error_streak=0.0,
    )
    snap = InfraSnapshot(
        ts=datetime.now(timezone.utc).isoformat(),
        rpc_latency_ms=rpc_ms,
        rpc_ok=rpc_ok,
        jup_latency_ms=jup_ms,
        jup_ok=jup_ok,
        error_streak=0.0,
        state=state,
        reason=reason,
    )
    return snap.to_dict()


# =============================================================================
# Main guard loop
# =============================================================================


async def run_infra_guard(stop_evt: Optional[asyncio.Event] = None) -> None:
    """
    Main infra guard loop.

    - Probes RPC & JUP every POLL_SEC.
    - Tracks error_streak with soft decay.
    - Writes infra:* keys to Redis.
    - Sends Telegram + log alerts on state transitions only.
    - Respects manual halt overrides.
    """
    redis = await _get_redis()
    last_state: Optional[str] = None
    error_streak: float = 0.0

    if hasattr(log, "event"):
        log.event(
            "INFRA_GUARD_START",
            poll_sec=POLL_SEC,
            jup_check=INFRA_ENABLE_JUP_CHECK,
            fail_open=INFRA_FAIL_OPEN,
        )
    else:  # pragma: no cover
        log.info(
            "INFRA_GUARD_START poll_sec=%s jup_check=%s fail_open=%s",
            POLL_SEC,
            INFRA_ENABLE_JUP_CHECK,
            INFRA_FAIL_OPEN,
        )

    # Fire-and-forget start notification
    if INFRA_TELEGRAM_ALERTS:
        try:
            await send_telegram("🛡️ Infra guard started.")
        except Exception:
            pass

    async with aiohttp.ClientSession() as session:
        while True:
            if stop_evt is not None and stop_evt.is_set():
                if INFRA_TELEGRAM_ALERTS:
                    try:
                        await send_telegram("🛡️ Infra guard stopping on stop_evt.")
                    except Exception:
                        pass
                if hasattr(log, "event"):
                    log.event("INFRA_GUARD_STOP", reason="stop_evt")
                else:
                    log.info("INFRA_GUARD_STOP stop_evt")
                break

            # ---- Probes ----
            rpc_ms, rpc_ok = await _rpc_latency(session)
            jup_ms, jup_ok = await _jup_latency(session)

            # ---- Classify (pre-streak) ----
            state, reason, is_bad_sample, _ = _classify_state(
                rpc_ms=rpc_ms,
                rpc_ok=rpc_ok,
                jup_ms=jup_ms,
                jup_ok=jup_ok,
                error_streak=error_streak,
            )

            # Update streak (with soft decay when good)
            if is_bad_sample:
                error_streak += 1.0
            else:
                error_streak *= INFRA_ERROR_DECAY
                if error_streak < 0.01:
                    error_streak = 0.0

            # Re-classify with updated streak (for HALTED escalation)
            state, reason, _, _ = _classify_state(
                rpc_ms=rpc_ms,
                rpc_ok=rpc_ok,
                jup_ms=jup_ms,
                jup_ok=jup_ok,
                error_streak=error_streak,
            )

            # Respect manual halt override
            manual_reason = None
            manual_active = False
            if redis:
                try:
                    manual_val = await redis.get(KEY_MANUAL_HALT)
                    manual_active = str(manual_val) == "1"
                    if manual_active:
                        manual_reason = await redis.get(KEY_MANUAL_REASON)
                except Exception:
                    manual_active = False

            if manual_active:
                # Manual halt dominates classification.
                state = STATE_HALTED
                if manual_reason:
                    reason = f"manual_halt: {manual_reason}; {reason}"
                else:
                    reason = f"manual_halt; {reason}"

            snap = InfraSnapshot(
                ts=datetime.now(timezone.utc).isoformat(),
                rpc_latency_ms=rpc_ms,
                rpc_ok=rpc_ok,
                jup_latency_ms=jup_ms,
                jup_ok=jup_ok,
                error_streak=error_streak,
                state=state,
                reason=reason,
            )

            # Persist to Redis
            if redis:
                try:
                    halted_flag = "1" if state == STATE_HALTED else "0"
                    degraded_flag = "1" if state in (STATE_DEGRADED, STATE_HALTED) else "0"

                    await redis.set(KEY_HALTED, halted_flag, ex=POLL_SEC * 2)
                    await redis.set(KEY_DEGRADED, degraded_flag, ex=POLL_SEC * 2)
                    await redis.set(KEY_LAST_STATE, state)
                    await redis.set(KEY_LAST_REASON, reason)
                    await redis.set(KEY_LAST_SNAPSHOT, json.dumps(snap.to_dict()))
                except Exception as e:  # pragma: no cover
                    if INFRA_DEBUG_LOG:
                        log.debug("Redis write error in infra_guard: %s", e)

            # State transition alerts / logs
            if state != last_state:
                if hasattr(log, "event"):
                    log.event(
                        "INFRA_STATE_CHANGE",
                        old=last_state,
                        new=state,
                        rpc_ms=rpc_ms,
                        rpc_ok=rpc_ok,
                        jup_ms=jup_ms,
                        jup_ok=jup_ok,
                        error_streak=error_streak,
                        manual=manual_active,
                    )
                else:  # pragma: no cover
                    log.info(
                        "INFRA_STATE_CHANGE old=%s new=%s rpc=%sms jup=%sms "
                        "err_streak=%.2f manual=%s",
                        last_state,
                        state,
                        rpc_ms,
                        jup_ms,
                        error_streak,
                        manual_active,
                    )

                last_state = state

                if INFRA_TELEGRAM_ALERTS:
                    try:
                        if state == STATE_HALTED:
                            await send_telegram(
                                "🚨 *INFRA HALTED*\n"
                                f"• RPC: {rpc_ms}ms (ok={rpc_ok})\n"
                                f"• JUP: {jup_ms}ms (ok={jup_ok}, enabled={INFRA_ENABLE_JUP_CHECK})\n"
                                f"• streak={error_streak:.1f}\n"
                                f"• manual={manual_active}\n"
                                f"• reason: {reason}"
                            )
                        elif state == STATE_DEGRADED:
                            await send_telegram(
                                "🟠 *INFRA DEGRADED*\n"
                                f"• RPC: {rpc_ms}ms (ok={rpc_ok})\n"
                                f"• JUP: {jup_ms}ms (ok={jup_ok}, enabled={INFRA_ENABLE_JUP_CHECK})\n"
                                f"• streak={error_streak:.1f}\n"
                                f"• manual={manual_active}\n"
                                f"• reason: {reason}"
                            )
                        else:
                            await send_telegram(
                                "🟢 *INFRA HEALTHY*\n"
                                f"• RPC: {rpc_ms}ms (ok={rpc_ok})\n"
                                f"• JUP: {jup_ms}ms (ok={jup_ok}, enabled={INFRA_ENABLE_JUP_CHECK})\n"
                                f"• streak={error_streak:.1f}\n"
                                f"• manual={manual_active}"
                            )
                    except Exception:
                        # Don't kill infra guard because Telegram failed
                        pass

            # Sleep, but be responsive to stop_evt
            remaining = float(POLL_SEC)
            while remaining > 0 and (stop_evt is None or not stop_evt.is_set()):
                step = min(remaining, 1.0)
                await asyncio.sleep(step)
                remaining -= step


# =============================================================================
# CLI / manual run
# =============================================================================

if __name__ == "__main__":
    # Standalone: python -m utils_pkg.infra_guard
    asyncio.run(run_infra_guard())
