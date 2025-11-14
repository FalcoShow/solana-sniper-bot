# utils_pkg/ai_sell_logic.py
# Decides HOLD vs SELL using fraud risk, price action, volume trend, and time-in-position.
# Also exposes hooks used by the exit engine:
#   - schedule_after_buy(ev)
#   - on_trade_tick(ev)
#   - schedule_force_exit(ev) / force_exit / request_exit / exit_now
#
# Design:
#   - HOLD/SELL decisions are conservative: only return SELL when confidence is high.
#   - Redis is used for dynamic parameters, buy timestamps, and simple exit requests.
#   - All Redis access is best-effort; failures only cause us to HOLD by default.

from __future__ import annotations

import json
import logging
import os
import time
from datetime import datetime, timedelta, timezone
from typing import Optional, Dict, Any, Tuple

from .core_utils import (
    get_token_price,
    ai_fraud_score,
    get_entry_price,
)

# NOTE: core_utils may or may not expose get_volume_data.
# We import it defensively and wrap it so the whole module still works if it's missing.
try:  # pragma: no cover - environment dependent
    from .core_utils import get_volume_data as _core_get_volume_data  # type: ignore
except Exception:  # noqa: BLE001
    _core_get_volume_data = None  # type: ignore[assignment]

logger = logging.getLogger("sniper.ai_sell_logic")
logging.basicConfig(level=logging.INFO)

# ───────────────────────────────────────────────────────────────────────────────
# Default thresholds (env can override; Redis can override both)
# Note: HOLD threshold is in [0..1]. Higher = stricter (need safer token to HOLD).
# ───────────────────────────────────────────────────────────────────────────────
DEFAULT_HOLD_THRESHOLD = float(os.getenv("HOLD_IF_SCORE_ABOVE", "0.85"))
DEFAULT_MAX_HOLD_MIN = int(os.getenv("MAX_HOLD_TIME_MINUTES", "360"))
EXTREME_PUMP_HOLD_PCT = float(os.getenv("EXTREME_PUMP_HOLD_PCT", "300"))  # >300% up + trend up => HOLD
FAST_FALL_SELL_PCT = float(os.getenv("FAST_FALL_SELL_PCT", "15"))          # drop >15% and vol down => SELL

ENABLE_OVERRIDE_HOLD = os.getenv("ENABLE_OVERRIDE_HOLD", "false").lower() == "true"
OVERRIDE_HOLD_THRESHOLD = float(os.getenv("OVERRIDE_HOLD_THRESHOLD", "0"))  # applied if > current


# ───────────────────────────────────────────────────────────────────────────────
# Lazy Redis helper
# (avoids import cycles and lets us run even if Redis isn't configured yet)
# ───────────────────────────────────────────────────────────────────────────────

def _get_redis():
    try:
        # Import lazily to avoid circular imports at module load time
        from utils_pkg.core_utils import redis as _redis  # type: ignore
        return _redis
    except Exception:  # noqa: BLE001
        return None


# ───────────────────────────────────────────────────────────────────────────────
# Helpers
# ───────────────────────────────────────────────────────────────────────────────

async def _get_param(redis, name: str, token: Optional[str], default: float) -> float:
    """
    Fetch a dynamic parameter from Redis (override:token / strategy:live) with a safe fallback.
    """
    val = None
    try:
        if redis:
            # 1) token-specific override
            if token:
                raw = await redis.get(f"override:{name}:{token}")
                if raw is not None:
                    val = float(raw)

            # 2) global strategy config
            if val is None:
                raw_cfg = await redis.get("strategy:live")
                if raw_cfg:
                    try:
                        if isinstance(raw_cfg, (bytes, bytearray)):
                            raw_cfg = raw_cfg.decode("utf-8")
                        cfg = json.loads(raw_cfg)
                        if name in cfg:
                            val = float(cfg[name])
                    except Exception as e:  # noqa: BLE001
                        logger.debug("strategy:live parse error for %s: %s", name, e)
    except Exception as e:  # noqa: BLE001
        logger.debug("_get_param error for %s/%s: %s", name, token, e)

    return float(val) if val is not None else default


async def _safe_get_volume_data(token_address: str) -> Dict[str, Any]:
    """
    Wrapper around optional core_utils.get_volume_data.
    Returns a neutral 'flat' structure if the underlying helper doesn't exist or fails.
    """
    base: Dict[str, Any] = {
        "direction": "flat",       # "up" / "down" / "flat"
        "pct_change_h1": 0.0,      # 1h price change %
        "candles": [],             # list of {open, close, ...}
    }

    if _core_get_volume_data is None:
        return base

    try:
        data = await _core_get_volume_data(token_address)  # type: ignore[misc]
        if isinstance(data, dict):
            base.update(data)
        return base
    except Exception as e:  # noqa: BLE001
        logger.debug("get_volume_data failed for %s: %s", token_address, e)
        return base


def _extract_price(ev: Dict[str, Any]) -> Optional[float]:
    """
    Try to pull a float price (in SOL terms or any consistent quote) from a trade tick.
    If nothing looks like a price, return None.
    """
    for key in (
        "priceUi",
        "price_ui",
        "price",
        "priceInSol",
        "price_in_sol",
        "ui_price",
    ):
        v = ev.get(key)
        if v is None:
            continue
        try:
            f = float(v)
            if f > 0:
                return f
        except Exception:
            continue
    return None


# ───────────────────────────────────────────────────────────────────────────────
# Core decision: HOLD vs SELL with reasoning
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
        - hold=True  => do NOT sell yet (keep position open).
        - hold=False => OK to sell (AI gate opened).
    """
    reasons: Dict[str, Any] = {"token": token_address}

    try:
        if not token_address:
            reasons["note"] = "no token_address provided"
            return True, reasons  # hold by default (fail-safe)

        # Resolve Redis if not provided
        if redis is None:
            redis = _get_redis()

        # Ensure timezone-aware UTC
        if buy_time and buy_time.tzinfo is None:
            buy_time = buy_time.replace(tzinfo=timezone.utc)
        if not buy_time and redis:
            # Try to recover buy_time from Redis
            ts_str = await redis.get(f"buytime:{token_address}")
            if ts_str:
                bt = None
                try:
                    if isinstance(ts_str, (bytes, bytearray)):
                        ts_str = ts_str.decode("utf-8")
                    bt = datetime.fromisoformat(ts_str)
                except Exception:  # noqa: BLE001
                    bt = None

                if bt:
                    if bt.tzinfo is None:
                        bt = bt.replace(tzinfo=timezone.utc)
                    buy_time = bt
                    reasons["buy_time_restored"] = True

        # Pull dynamic params (Redis > strategy:live > env/default)
        max_hold_min = int(
            await _get_param(redis, "MAX_HOLD_TIME_MINUTES", token_address, DEFAULT_MAX_HOLD_MIN)
        )
        hold_threshold = float(
            await _get_param(redis, "HOLD_IF_SCORE_ABOVE", token_address, DEFAULT_HOLD_THRESHOLD)
        )

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
        reasons["threshold"] = hold_threshold
        if fraud < hold_threshold:
            reasons["decision"] = "SELL_fraud_below_threshold"
            if redis:
                await redis.set(
                    f"alert:{token_address}:reason",
                    "Fraud score below threshold",
                )
            return False, reasons

        # 2) Volume/Trend context (safe wrapper)
        vol = await _safe_get_volume_data(token_address)
        direction = vol.get("direction", "flat")
        reasons["trend"] = direction
        reasons["h1_change"] = vol.get("pct_change_h1", 0.0)

        # Trend helpers
        def last_n_green(candles, n: int = 3) -> bool:
            if not candles or len(candles) < n:
                return False
            tail = candles[-n:]
            try:
                return all(
                    (c.get("close", 0) or 0) > (c.get("open", 0) or 0)
                    for c in tail
                )
            except Exception:  # noqa: BLE001
                return False

        candles = vol.get("candles", [])

        # Extreme pump with uptrend: let it ride
        if price_change_pct > EXTREME_PUMP_HOLD_PCT and direction == "up":
            reasons["decision"] = "HOLD_extreme_pump_with_uptrend"
            return True, reasons

        # Last 3 candles green: keep holding
        if last_n_green(candles, 3):
            reasons["decision"] = "HOLD_last_3_green"
            return True, reasons

        # 3) Fast-fall breaker (avoid round-tripping profits)
        if price_change_pct <= -FAST_FALL_SELL_PCT and direction != "up":
            reasons["decision"] = "SELL_fast_fall"
            reasons["fast_fall_threshold_pct"] = FAST_FALL_SELL_PCT
            if redis:
                await redis.set(
                    f"alert:{token_address}:reason",
                    "Fast fall detected",
                )
            return False, reasons

        # 4) Max hold time
        hold_duration = datetime.now(timezone.utc) - buy_time
        if hold_duration > timedelta(minutes=max_hold_min):
            reasons["decision"] = "SELL_max_hold_exceeded"
            reasons["hold_minutes"] = int(hold_duration.total_seconds() // 60)
            reasons["max_hold_minutes"] = max_hold_min
            if redis:
                await redis.set(
                    f"alert:{token_address}:reason",
                    "Max hold time exceeded",
                )
            return False, reasons

        # 5) Neutral default: SELL gate stays closed unless we saw a strong SELL reason.
        reasons["decision"] = "HOLD_default_conservative"
        return True, reasons

    except Exception as e:  # noqa: BLE001
        logger.error("[AI SELL LOGIC ERROR] %s", e, exc_info=True)
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


# ───────────────────────────────────────────────────────────────────────────────
# Hooks used by exit_engine.py
# ───────────────────────────────────────────────────────────────────────────────

async def schedule_after_buy(ev: Dict[str, Any]) -> None:
    """
    Called by exit_engine.on_buy_filled() when a BUY is confirmed.

    Responsibilities:
      - Persist basic entry metadata for later AI decisions (buy_time, maybe entry price).
      - Optionally stash a compact snapshot in Redis for observability.
    """
    mint = ev.get("mint")
    if not mint:
        logger.warning("schedule_after_buy: missing mint in event: keys=%s", list(ev.keys()))
        return

    redis = _get_redis()
    if not redis:
        logger.debug("schedule_after_buy: redis not available, skipping persistence")
        return

    try:
        ts = ev.get("ts") or time.time()
        if isinstance(ts, (int, float)):
            ts_dt = datetime.fromtimestamp(ts, tz=timezone.utc)
        else:
            # if already datetime or iso
            try:
                if isinstance(ts, str):
                    ts_dt = datetime.fromisoformat(ts)
                    if ts_dt.tzinfo is None:
                        ts_dt = ts_dt.replace(tzinfo=timezone.utc)
                elif isinstance(ts, datetime):
                    ts_dt = ts if ts.tzinfo else ts.replace(tzinfo=timezone.utc)
                else:
                    ts_dt = datetime.now(timezone.utc)
            except Exception:
                ts_dt = datetime.now(timezone.utc)

        iso_ts = ts_dt.isoformat()

        # Store buy time (this is what should_hold_token_with_reason reads from)
        await redis.set(f"buytime:{mint}", iso_ts)

        # Optional: store entry price if UI provided one
        entry_price = ev.get("price_entry_ui") or ev.get("entry_price_ui")
        if entry_price is not None:
            try:
                ep = float(entry_price)
                if ep > 0:
                    await redis.set(f"entry_price_ui:{mint}", str(ep))
            except Exception:
                pass

        # Compact snapshot for debugging / dashboards
        snap = {
            "mint": mint,
            "symbol": ev.get("symbol"),
            "pool": ev.get("pool"),
            "ts": iso_ts,
            "spend_sol": ev.get("spend_sol"),
            "amount_raw": ev.get("amount_raw"),
            "tx_sig": ev.get("signature") or ev.get("tx_sig"),
        }
        await redis.set(f"pos:{mint}:meta", json.dumps(snap))

        logger.info(
            "AI_SELL_SCHEDULE_AFTER_BUY",
            extra={"mint": mint, "symbol": ev.get("symbol"), "pool": ev.get("pool")},
        )
    except Exception as e:  # noqa: BLE001
        logger.warning("schedule_after_buy failed for %s: %s", mint, e)


async def on_trade_tick(ev: Dict[str, Any]) -> None:
    """
    Called on every trade tick by exit_engine.on_trade_tick().

    Responsibilities:
      - Optionally persist last tick price/ts for analytics or future logic.
      - Can be extended later to support more advanced pattern recognition.
    """
    mint = ev.get("mint") or ev.get("mintAddress") or ev.get("mint_address") or ev.get("id")
    if not mint:
        return

    redis = _get_redis()
    if not redis:
        return

    try:
        price = _extract_price(ev)
        ts = ev.get("ts") or time.time()
        if isinstance(ts, (int, float)):
            ts_dt = datetime.fromtimestamp(ts, tz=timezone.utc)
        else:
            try:
                if isinstance(ts, str):
                    ts_dt = datetime.fromisoformat(ts)
                    if ts_dt.tzinfo is None:
                        ts_dt = ts_dt.replace(tzinfo=timezone.utc)
                elif isinstance(ts, datetime):
                    ts_dt = ts if ts.tzinfo else ts.replace(tzinfo=timezone.utc)
                else:
                    ts_dt = datetime.now(timezone.utc)
            except Exception:
                ts_dt = datetime.now(timezone.utc)

        payload = {
            "ts": ts_dt.isoformat(),
            "price": price,
            "side": ev.get("side"),
            "size": ev.get("size"),
            "pool": ev.get("pool"),
        }
        await redis.set(f"last_tick:{mint}", json.dumps(payload))
    except Exception as e:  # noqa: BLE001
        logger.debug("on_trade_tick failed for %s: %s", mint, e)


async def schedule_force_exit(ev: Dict[str, Any]) -> None:
    """
    Called by exit_engine when the "no follow-through" rule fires.

    The exit_engine has already decided the coin failed follow-through.
    Here we:
      - Log an exit request into Redis for downstream exit workers.
      - Optionally flag an alert reason.
    """
    mint = ev.get("mint")
    if not mint:
        logger.warning("schedule_force_exit: missing mint in event: keys=%s", list(ev.keys()))
        return

    redis = _get_redis()
    if not redis:
        logger.debug("schedule_force_exit: redis not available, skipping")
        return

    try:
        now = datetime.now(timezone.utc).isoformat()
        payload = {
            "mint": mint,
            "symbol": ev.get("symbol"),
            "pool": ev.get("pool"),
            "reason": ev.get("reason") or "follow_through_fail",
            "age": ev.get("age"),
            "trades_after_entry": ev.get("trades_after_entry"),
            "up_pct": ev.get("up_pct"),
            "dd_pct": ev.get("dd_pct"),
            "ts": now,
        }
        await redis.set(f"exit_request:{mint}", json.dumps(payload))
        await redis.set(
            f"alert:{mint}:reason",
            f"Exit requested: {payload['reason']}",
        )

        logger.info(
            "AI_SELL_FORCE_EXIT_REQUESTED",
            extra={"mint": mint, "symbol": payload.get("symbol"), "reason": payload["reason"]},
        )
    except Exception as e:  # noqa: BLE001
        logger.warning("schedule_force_exit failed for %s: %s", mint, e)


# Aliases so exit_engine can call any of these names
async def force_exit(ev: Dict[str, Any]) -> None:       # pragma: no cover - simple delegate
    await schedule_force_exit(ev)


async def request_exit(ev: Dict[str, Any]) -> None:     # pragma: no cover - simple delegate
    await schedule_force_exit(ev)


async def exit_now(ev: Dict[str, Any]) -> None:         # pragma: no cover - simple delegate
    await schedule_force_exit(ev)


# ───────────────────────────────────────────────────────────────────────────────
# Optional CLI test
# ───────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":  # pragma: no cover
    import asyncio
    import sys
    from utils_pkg.core_utils import init_resources, close_resources, redis as _redis

    async def _test() -> None:
        await init_resources()
        token = (
            sys.argv[1]
            if len(sys.argv) > 1
            else "FakeToken1111111111111111111111111111111111111"
        )
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
