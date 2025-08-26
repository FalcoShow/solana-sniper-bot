# utils_pkg/ai_sell_logic.py
# Decides HOLD vs SELL using fraud risk, price action, volume trend, and time-in-position.
# - Pulls live params/overrides from Redis (token-specific + global strategy).
# - Computes price change if not provided.
# - Fail-soft: on missing data returns HOLD.
# - Designed to be a conservative gate: only returns SELL when confidence is high.

from __future__ import annotations

import os
import logging
from datetime import datetime, timedelta, timezone
from typing import Optional, Dict, Any, Tuple

from .core_utils import (
    get_token_price,
    ai_fraud_score,
    get_volume_data,
    get_entry_price,
)

logger = logging.getLogger("ai_sell_logic")
logging.basicConfig(level=logging.INFO)

# ───────────────────────────────────────────────────────────────────────────────
# Defaults (env can override; Redis can override both)
# Note: HOLD threshold is in [0..1]. Higher = stricter (need safer token to HOLD).
# ───────────────────────────────────────────────────────────────────────────────
DEFAULT_HOLD_THRESHOLD = float(os.getenv("HOLD_IF_SCORE_ABOVE", "0.85"))
DEFAULT_MAX_HOLD_MIN = int(os.getenv("MAX_HOLD_TIME_MINUTES", "360"))
EXTREME_PUMP_HOLD_PCT = float(os.getenv("EXTREME_PUMP_HOLD_PCT", "300"))  # >300% up with uptrend? HOLD
FAST_FALL_SELL_PCT = float(os.getenv("FAST_FALL_SELL_PCT", "15"))          # drop >15% and vol down? SELL bias

ENABLE_OVERRIDE_HOLD = os.getenv("ENABLE_OVERRIDE_HOLD", "false").lower() == "true"
OVERRIDE_HOLD_THRESHOLD = float(os.getenv("OVERRIDE_HOLD_THRESHOLD", "0"))  # applied if > current


# ───────────────────────────────────────────────────────────────────────────────
# Helpers to fetch live parameters (safe with/without Redis)
# Keys we look for (in order of priority):
#   1) token override:   override:<name>:<token_mint>
#   2) strategy JSON:    strategy:live  -> { "<name>": value, ... }
#   3) env default (passed in or module defaults)
# ───────────────────────────────────────────────────────────────────────────────

async def _get_param(redis, name: str, token: Optional[str], default: float) -> float:
    val = None
    try:
        if redis:
            if token:
                raw = await redis.get(f"override:{name}:{token}")
                if raw is not None:
                    val = float(raw)
            if val is None:
                raw_cfg = await redis.get("strategy:live")
                if raw_cfg:
                    try:
                        import json
                        cfg = json.loads(raw_cfg)
                        if name in cfg:
                            val = float(cfg[name])
                    except Exception:
                        pass
    except Exception as e:
        logger.debug(f"_get_param error for {name}/{token}: {e}")
    return float(val) if val is not None else default


# ───────────────────────────────────────────────────────────────────────────────
# Core decision
# ───────────────────────────────────────────────────────────────────────────────

async def should_hold_token_with_reason(
    *,
    threshold_score: Optional[float] = None,   # [0..1], higher = stricter hold
    price_change_pct: Optional[float] = None,  # % since entry
    token_address: Optional[str] = None,
    buy_time: Optional[datetime] = None,
    redis=None,
) -> Tuple[bool, Dict[str, Any]]:
    """
    Compute a conservative HOLD/SELL decision.

    Returns:
        (hold: bool, reasons: dict)
        - hold=True means do NOT sell yet.
        - hold=False means OK to sell (AI gate opened).
    """
    reasons: Dict[str, Any] = {"token": token_address}

    try:
        if not token_address:
            reasons["note"] = "no token_address provided"
            return True, reasons  # hold by default

        # Ensure timezone-aware UTC
        if buy_time and buy_time.tzinfo is None:
            buy_time = buy_time.replace(tzinfo=timezone.utc)
        if not buy_time and redis:
            # Try to recover buy_time from Redis
            ts_str = await redis.get(f"buytime:{token_address}")
            if ts_str:
                try:
                    bt = datetime.fromisoformat(ts_str)
                except Exception:
                    # bytes or malformed -> try decode
                    try:
                        bt = datetime.fromisoformat(ts_str.decode("utf-8"))  # type: ignore
                    except Exception:
                        bt = None
                if bt:
                    if bt.tzinfo is None:
                        bt = bt.replace(tzinfo=timezone.utc)
                    buy_time = bt
                    reasons["buy_time_restored"] = True

        # Pull dynamic params (Redis > strategy:live > env/default)
        max_hold_min = int(await _get_param(redis, "MAX_HOLD_TIME_MINUTES", token_address, DEFAULT_MAX_HOLD_MIN))
        hold_threshold = float(await _get_param(redis, "HOLD_IF_SCORE_ABOVE", token_address, DEFAULT_HOLD_THRESHOLD))

        # Apply env override if enabled
        if ENABLE_OVERRIDE_HOLD and OVERRIDE_HOLD_THRESHOLD > hold_threshold:
            reasons["override_hold_threshold"] = OVERRIDE_HOLD_THRESHOLD
            hold_threshold = OVERRIDE_HOLD_THRESHOLD

        if threshold_score is not None and threshold_score > hold_threshold:
            # caller wants stricter than system; use stricter
            reasons["caller_threshold_used"] = threshold_score
            hold_threshold = threshold_score

        # Compute price change if needed
        if price_change_pct is None and redis and buy_time:
            entry_price = await get_entry_price(redis, token_address)
            if entry_price is None or entry_price <= 0:
                reasons["note"] = "missing entry price"
                return True, reasons  # hold
            cur = await get_token_price(token_address)
            if not cur or cur <= 0:
                reasons["note"] = "missing current price"
                return True, reasons  # hold
            price_change_pct = ((cur - entry_price) / entry_price) * 100.0
            reasons["derived_price_change_pct"] = round(price_change_pct, 4)

        # If we STILL lack data, be safe
        if buy_time is None or price_change_pct is None:
            # If only price change was provided, apply a simple guardrail
            if price_change_pct is not None:
                if price_change_pct > EXTREME_PUMP_HOLD_PCT:
                    reasons["guardrail"] = "extreme_pump_hold"
                    return True, reasons
                if price_change_pct < 0:
                    reasons["guardrail"] = "price_dropped_sell"
                    return False, reasons
            reasons["note"] = "insufficient context -> hold"
            return True, reasons

        # 1) Fraud risk gate
        fraud = await ai_fraud_score(token_address)
        reasons["fraud_score"] = round(fraud, 4)
        if fraud < hold_threshold:
            reasons["decision"] = "SELL_fraud_below_threshold"
            reasons["threshold"] = hold_threshold
            if redis:
                await redis.set(f"alert:{token_address}:reason", "Fraud score below threshold")
            return False, reasons

        # 2) Volume/Trend context
        vol = await get_volume_data(token_address)
        direction = vol.get("direction", "flat")
        reasons["trend"] = direction
        reasons["h1_change"] = vol.get("pct_change_h1", 0.0)

        # Trend helpers
        def last_n_green(candles, n=3) -> bool:
            if not candles or len(candles) < n:
                return False
            tail = candles[-n:]
            try:
                return all((c.get("close", 0) or 0) > (c.get("open", 0) or 0) for c in tail)
            except Exception:
                return False

        candles = vol.get("candles", [])
        if price_change_pct > EXTREME_PUMP_HOLD_PCT and direction == "up":
            reasons["decision"] = "HOLD_extreme_pump_with_uptrend"
            return True, reasons

        if last_n_green(candles, 3):
            reasons["decision"] = "HOLD_last_3_green"
            return True, reasons

        # 3) Fast-fall breaker (avoid round-tripping profits)
        if price_change_pct <= -FAST_FALL_SELL_PCT and direction != "up":
            reasons["decision"] = "SELL_fast_fall"
            reasons["fast_fall_threshold_pct"] = FAST_FALL_SELL_PCT
            if redis:
                await redis.set(f"alert:{token_address}:reason", "Fast fall detected")
            return False, reasons

        # 4) Max hold time
        hold_duration = datetime.now(timezone.utc) - buy_time
        if hold_duration > timedelta(minutes=max_hold_min):
            reasons["decision"] = "SELL_max_hold_exceeded"
            reasons["hold_minutes"] = int(hold_duration.total_seconds() // 60)
            reasons["max_hold_minutes"] = max_hold_min
            if redis:
                await redis.set(f"alert:{token_address}:reason", "Max hold time exceeded")
            return False, reasons

        # 5) Neutral default: sell if no bullish signal AND not deeply in profit
        #    We keep this conservative: prefer HOLD unless there's a clear SELL reason.
        #    If you want more aggression, flip the default.
        reasons["decision"] = "HOLD_default_conservative"
        return True, reasons

    except Exception as e:
        logger.error(f"[AI SELL LOGIC ERROR] {e}", exc_info=True)
        # fail-safe: HOLD on unexpected errors
        reasons["error"] = str(e)
        return True, reasons


async def should_hold_token(
    threshold_score: Optional[float] = None,
    price_change_pct: Optional[float] = None,
    token_address: Optional[str] = None,
    buy_time: Optional[datetime] = None,
    redis=None,
) -> bool:
    """
    Shim for legacy callers: returns only the boolean HOLD/SELL decision.
    """
    hold, _ = await should_hold_token_with_reason(
        threshold_score=threshold_score,
        price_change_pct=price_change_pct,
        token_address=token_address,
        buy_time=buy_time,
        redis=redis,
    )
    return hold


# Optional CLI test
if __name__ == "__main__":
    import asyncio, sys
    from datetime import timedelta
    from utils_pkg.core_utils import init_resources, close_resources, redis as _redis

    async def _test():
        await init_resources()
        token = sys.argv[1] if len(sys.argv) > 1 else "FakeToken1111111111111111111111111111111111111"
        bt = datetime.now(timezone.utc) - timedelta(minutes=120)
        hold, info = await should_hold_token_with_reason(
            threshold_score=0.85,
            token_address=token,
            buy_time=bt,
            redis=_redis,
        )
        print("HOLD?", hold, "| info:", info)
        await close_resources()

    asyncio.run(_test())
