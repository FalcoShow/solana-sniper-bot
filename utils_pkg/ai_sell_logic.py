# utils_pkg/ai_sell_logic.py
# Decides HOLD vs SELL using fraud risk, price action, volume trend, and time-in-position.
#
# Exposed hooks (used by exit_engine.py):
#   - should_hold_token_with_reason(...)
#   - should_hold_token(...)
#   - schedule_after_buy(ev)
#   - on_trade_tick(ev)
#   - schedule_force_exit(ev) / force_exit(ev) / request_exit(ev) / exit_now(ev)
#
# Design:
#   - Decisions are conservative: we only unlock SELL when we see clear reasons.
#   - Redis is used for:
#       * dynamic strategy params (thresholds, max hold, etc.)
#       * buy timestamps and entry price
#       * simple exit requests + alert reasons
#   - All Redis access is best-effort: if Redis or data is missing, we default to HOLD.

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

# Optional: core_utils may expose get_volume_data; we guard it so this module
# still works even if that helper does not exist in a given deployment.
try:  # pragma: no cover - environment dependent
    from .core_utils import get_volume_data as _core_get_volume_data  # type: ignore
except Exception:  # noqa: BLE001
    _core_get_volume_data = None  # type: ignore[assignment]

logger = logging.getLogger("sniper.ai_sell_logic")
if not logger.handlers:
    # Avoid double-config in some environments
    logging.basicConfig(level=logging.INFO)

# ───────────────────────────────────────────────────────────────────────────────
# Default thresholds (env can override; Redis can override both)
#
# HOLD_IF_SCORE_ABOVE: fraud score in [0..1]. Higher = stricter (need safer token to HOLD).
# MAX_HOLD_TIME_MINUTES: hard cap on time-in-position before we force SELL.
# EXTREME_PUMP_HOLD_PCT: if profit exceeds this and trend is up, HOLD to let winners run.
# FAST_FALL_SELL_PCT: if drawdown exceeds this and trend is not up, SELL to avoid round-trip.
# ───────────────────────────────────────────────────────────────────────────────

DEFAULT_HOLD_THRESHOLD = float(os.getenv("HOLD_IF_SCORE_ABOVE", "0.85"))
DEFAULT_MAX_HOLD_MIN = int(os.getenv("MAX_HOLD_TIME_MINUTES", "360"))
EXTREME_PUMP_HOLD_PCT = float(os.getenv("EXTREME_PUMP_HOLD_PCT", "300"))  # % up
FAST_FALL_SELL_PCT = float(os.getenv("FAST_FALL_SELL_PCT", "15"))         # % down

ENABLE_OVERRIDE_HOLD = os.getenv("ENABLE_OVERRIDE_HOLD", "false").lower() == "true"
OVERRIDE_HOLD_THRESHOLD = float(os.getenv("OVERRIDE_HOLD_THRESHOLD", "0"))  # used if > current


# ───────────────────────────────────────────────────────────────────────────────
# Lazy Redis helper (avoid circular imports at module import time)
# ───────────────────────────────────────────────────────────────────────────────

def _get_redis():
    """
    Lazy import of core_utils.redis (or None if unavailable).
    """
    try:
        from utils_pkg.core_utils import redis as _redis  # type: ignore
        return _redis
    except Exception:  # noqa: BLE001
        return None


# ───────────────────────────────────────────────────────────────────────────────
# Helpers
# ───────────────────────────────────────────────────────────────────────────────

async def _get_param(redis, name: str, token: Optional[str], default: float) -> float:
    """
    Fetch a dynamic parameter from Redis, with safe fallback to `default`.

    Order of precedence:
      1) token-specific override in key: override:{name}:{token}
      2) global strategy config in key: strategy:live (JSON object)
      3) provided default (env / hard-coded)
    """
    val = None
    try:
        if redis:
            # 1) Token-specific override
            if token:
                raw = await redis.get(f"override:{name}:{token}")
                if raw is not None:
                    try:
                        if isinstance(raw, (bytes, bytearray)):
                            raw = raw.decode("utf-8")
                        val = float(raw)
                    except Exception as e:  # noqa: BLE001
                        logger.debug("override parse error for %s/%s: %s", name, token, e)

            # 2) Global config
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

    Shape:
      {
        "direction": "up" | "down" | "flat",
        "pct_change_h1": float,
        "candles": [ {open, close, ...}, ... ]
      }
    """
    base: Dict[str, Any] = {
        "direction": "flat",
        "pct_change_h1": 0.0,
        "candles": [],
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
    Attempt to extract a float price from a trade tick/event payload.
    We accept several common keys and return None if nothing looks valid.
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


def _normalize_ts(ts: Any) -> datetime:
    """
    Normalize various timestamp formats to timezone-aware UTC datetime.
    Accepts:
      - float/int seconds since epoch
      - ISO-8601 string
      - datetime (naive or tz-aware)
      - anything else → now(UTC)
    """
    try:
        if isinstance(ts, (int, float)):
            return datetime.fromtimestamp(ts, tz=timezone.utc)
        if isinstance(ts, str):
            dt = datetime.fromisoformat(ts)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt
        if isinstance(ts, datetime):
            return ts if ts.tzinfo else ts.replace(tzinfo=timezone.utc)
    except Exception:
        pass
    return datetime.now(timezone.utc)


# ───────────────────────────────────────────────────────────────────────────────
# Core decision: HOLD vs SELL with reasoning
# ───────────────────────────────────────────────────────────────────────────────

async def should_hold_token_with_reason(
    *,
    threshold_score: Optional[float] = None,   # fraud score threshold [0..1]; higher = stricter HOLD
    price_change_pct: Optional[float] = None,  # current PnL % since entry
    token_address: Optional[str] = None,
    buy_time: Optional[datetime] = None,
    redis=None,
) -> Tuple[bool, Dict[str, Any]]:
    """
    Compute a conservative HOLD/SELL decision.

    Returns:
        (hold: bool, reasons: dict)

        - hold = True  => do NOT sell yet (keep position open).
        - hold = False => OK to sell (AI exit gate is opened).

    High-level logic:
      1) If we lack critical data (token, buy_time, price), default to HOLD.
      2) Fraud score below threshold → SELL.
      3) Extreme pump + uptrend → HOLD (let winners run).
      4) Last N candles green → HOLD (momentum still strong).
      5) Fast-fall + weak trend → SELL.
      6) Max hold time exceeded → SELL.
      7) Otherwise → HOLD (default conservative).
    """
    reasons: Dict[str, Any] = {"token": token_address}

    try:
        if not token_address:
            reasons["note"] = "no token_address provided"
            return True, reasons  # fail-safe HOLD

        # Resolve Redis lazily if caller didn't pass one
        if redis is None:
            redis = _get_redis()

        # Normalize buy_time; if missing, try to restore from Redis (buytime:{mint})
        if buy_time is not None and buy_time.tzinfo is None:
            buy_time = buy_time.replace(tzinfo=timezone.utc)

        if buy_time is None and redis:
            ts_str = await redis.get(f"buytime:{token_address}")
            if ts_str:
                try:
                    if isinstance(ts_str, (bytes, bytearray)):
                        ts_str = ts_str.decode("utf-8")
                    bt = datetime.fromisoformat(ts_str)
                    if bt.tzinfo is None:
                        bt = bt.replace(tzinfo=timezone.utc)
                    buy_time = bt
                    reasons["buy_time_restored"] = True
                except Exception:  # noqa: BLE001
                    buy_time = None

        # Pull dynamic parameters: max hold + fraud hold threshold
        max_hold_min = int(
            await _get_param(
                redis,
                "MAX_HOLD_TIME_MINUTES",
                token_address,
                DEFAULT_MAX_HOLD_MIN,
            )
        )
        hold_threshold = float(
            await _get_param(
                redis,
                "HOLD_IF_SCORE_ABOVE",
                token_address,
                DEFAULT_HOLD_THRESHOLD,
            )
        )

        # Apply hard override from env if enabled and stricter
        if ENABLE_OVERRIDE_HOLD and OVERRIDE_HOLD_THRESHOLD > hold_threshold:
            reasons["override_hold_threshold"] = OVERRIDE_HOLD_THRESHOLD
            hold_threshold = OVERRIDE_HOLD_THRESHOLD

        # Caller-provided threshold can be stricter than system; respect the stricter one
        if threshold_score is not None and threshold_score > hold_threshold:
            reasons["caller_threshold_used"] = threshold_score
            hold_threshold = threshold_score

        # Compute price change if needed and possible
        if price_change_pct is None and redis and buy_time is not None:
            entry_price = await get_entry_price(redis, token_address)
            if entry_price is None or entry_price <= 0:
                reasons["note"] = "missing entry price"
                return True, reasons  # HOLD
            cur = await get_token_price(token_address)
            if not cur or cur <= 0:
                reasons["note"] = "missing current price"
                return True, reasons  # HOLD
            price_change_pct = ((cur - entry_price) / entry_price) * 100.0
            reasons["derived_price_change_pct"] = round(price_change_pct, 4)

        # If we still lack critical context, be safe and HOLD
        if buy_time is None or price_change_pct is None:
            # If we at least know PnL, apply a very simple guardrail:
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
            # Riskier than we're allowed to HOLD → SELL
            reasons["decision"] = "SELL_fraud_below_threshold"
            if redis:
                await redis.set(
                    f"alert:{token_address}:reason",
                    "Fraud score below threshold",
                )
            return False, reasons

        # 2) Volume / trend context
        vol = await _safe_get_volume_data(token_address)
        direction = vol.get("direction", "flat")
        candles = vol.get("candles", [])
        reasons["trend"] = direction
        reasons["h1_change"] = vol.get("pct_change_h1", 0.0)

        # Helper: last N candles green
        def last_n_green(c: Any, n: int = 3) -> bool:
            try:
                if not c or len(c) < n:
                    return False
                tail = c[-n:]
                return all(
                    (x.get("close", 0) or 0) > (x.get("open", 0) or 0)
                    for x in tail
                )
            except Exception:  # noqa: BLE001
                return False

        # 3) Extreme pump + uptrend: let it ride
        if price_change_pct > EXTREME_PUMP_HOLD_PCT and direction == "up":
            reasons["decision"] = "HOLD_extreme_pump_with_uptrend"
            return True, reasons

        # Strong short-term momentum: last 3 candles all green → HOLD
        if last_n_green(candles, 3):
            reasons["decision"] = "HOLD_last_3_green"
            return True, reasons

        # 4) Fast-fall breaker: big drawdown + trend not up → SELL
        if price_change_pct <= -FAST_FALL_SELL_PCT and direction != "up":
            reasons["decision"] = "SELL_fast_fall"
            reasons["fast_fall_threshold_pct"] = FAST_FALL_SELL_PCT
            if redis:
                await redis.set(
                    f"alert:{token_address}:reason",
                    "Fast fall detected",
                )
            return False, reasons

        # 5) Max hold time hard cap
        hold_duration = datetime.now(timezone.utc) - buy_time
        hold_minutes = int(hold_duration.total_seconds() // 60)
        reasons["hold_minutes"] = hold_minutes
        reasons["max_hold_minutes"] = max_hold_min

        if hold_duration > timedelta(minutes=max_hold_min):
            reasons["decision"] = "SELL_max_hold_exceeded"
            if redis:
                await redis.set(
                    f"alert:{token_address}:reason",
                    "Max hold time exceeded",
                )
            return False, reasons

        # 6) Neutral default: we HOLD unless we saw a strong SELL reason
        reasons["decision"] = "HOLD_default_conservative"
        return True, reasons

    except Exception as e:  # noqa: BLE001
        # Fail-safe: on any unexpected error, we HOLD and record the error
        logger.error("[AI SELL LOGIC ERROR] %s", e, exc_info=True)
        reasons["error"] = str(e)
        reasons["decision"] = "HOLD_error_fail_safe"
        return True, reasons


async def should_hold_token(
    threshold_score: Optional[float] = None,
    price_change_pct: Optional[float] = None,
    token_address: Optional[str] = None,
    buy_time: Optional[datetime] = None,
    redis=None,
) -> bool:
    """
    Shim for callers that only care about the boolean decision.

    Returns:
        hold: bool
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
    Called by exit_engine.on_buy_filled(...) when a BUY is confirmed.

    Responsibilities:
      - Persist buy_time (UTC ISO) in Redis for this mint.
      - Persist optional entry price if provided (for later PnL computation).
      - Store a compact snapshot for observability / dashboards.
    """
    mint = ev.get("mint")
    if not mint:
        logger.warning(
            "schedule_after_buy: missing mint in event: keys=%s", list(ev.keys())
        )
        return

    redis = _get_redis()
    if not redis:
        logger.debug("schedule_after_buy: redis not available, skipping persistence")
        return

    try:
        # Timestamp normalization
        ts = ev.get("ts") or time.time()
        ts_dt = _normalize_ts(ts)
        iso_ts = ts_dt.isoformat()

        # 1) Store buy time for this token (consumed by should_hold_token_with_reason)
        await redis.set(f"buytime:{mint}", iso_ts)

        # 2) Optional: store entry price if provided by UI/context
        entry_price = (
            ev.get("price_entry_ui")
            or ev.get("entry_price_ui")
            or ev.get("entry_price")
        )
        if entry_price is not None:
            try:
                ep = float(entry_price)
                if ep > 0:
                    await redis.set(f"entry_price_ui:{mint}", str(ep))
            except Exception:
                # We don't want entry-price parsing issues to break scheduling
                pass

        # 3) Compact snapshot for debugging / dashboards
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
            extra={
                "mint": mint,
                "symbol": ev.get("symbol"),
                "pool": ev.get("pool"),
            },
        )
    except Exception as e:  # noqa: BLE001
        logger.warning("schedule_after_buy failed for %s: %s", mint, e)


async def on_trade_tick(ev: Dict[str, Any]) -> None:
    """
    Called on every trade tick by exit_engine.on_trade_tick(ev).

    Responsibilities:
      - Persist the last-seen tick for each mint for analytics / future features.
      - This is intentionally light: we don't run heavy AI here.
    """
    mint = (
        ev.get("mint")
        or ev.get("mintAddress")
        or ev.get("mint_address")
        or ev.get("id")
    )
    if not mint:
        return

    redis = _get_redis()
    if not redis:
        return

    try:
        price = _extract_price(ev)
        ts = ev.get("ts") or time.time()
        ts_dt = _normalize_ts(ts)

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
    Called by exit_engine when the follow-through logic decides this coin should be exited.

    Responsibilities:
      - Write an exit_request:{mint} payload to Redis for exit workers.
      - Record an alert reason for downstream notifications.
    """
    mint = ev.get("mint")
    if not mint:
        logger.warning(
            "schedule_force_exit: missing mint in event: keys=%s", list(ev.keys())
        )
        return

    redis = _get_redis()
    if not redis:
        logger.debug("schedule_force_exit: redis not available, skipping")
        return

    try:
        now_iso = datetime.now(timezone.utc).isoformat()
        payload = {
            "mint": mint,
            "symbol": ev.get("symbol"),
            "pool": ev.get("pool"),
            "reason": ev.get("reason") or "follow_through_fail",
            "age": ev.get("age"),
            "trades_after_entry": ev.get("trades_after_entry"),
            "up_pct": ev.get("up_pct"),
            "dd_pct": ev.get("dd_pct"),
            "ts": now_iso,
        }
        await redis.set(f"exit_request:{mint}", json.dumps(payload))
        await redis.set(
            f"alert:{mint}:reason",
            f"Exit requested: {payload['reason']}",
        )

        logger.info(
            "AI_SELL_FORCE_EXIT_REQUESTED",
            extra={
                "mint": mint,
                "symbol": payload.get("symbol"),
                "reason": payload["reason"],
            },
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
    from utils_pkg.core_utils import (
        init_resources,
        close_resources,
        redis as _redis,
    )

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
