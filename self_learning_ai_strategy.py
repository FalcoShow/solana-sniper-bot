# self_learning_ai_strategy.py
"""
Self-learning strategy brain.

Responsibilities
---------------
• Persist and serve live strategy parameters (Redis with safe fallbacks).
• Record every closed trade and maintain rolling performance stats.
• Periodically auto-tune parameters with guardrails:
    - Rule-based adjustments (always on).
    - Optional LLM suggestions (OpenAI) merged safely into bounds.
• Announce changes via Telegram/Notion for transparency.

Public API
----------
get_strategy_params(redis) -> dict
set_strategy_params(redis, params: dict) -> None
process_trade_closure(token_address, buy_price, sell_price, amount, partial, timestamp) -> None
get_perf_snapshot(redis) -> dict
run_self_learning_loop(redis=None) -> background loop

Notes
-----
• Keys in Redis:
  - strategy:params         -> JSON blob of current params
  - strategy:trades         -> LIST (JSON strings), capped to MAX_TRADES_KEEP
  - strategy:stats          -> JSON blob of aggregated stats
  - strategy:last_tune_ts   -> float timestamp
  - strategy:last_llm_ts    -> float timestamp

• All numeric params have strict bounds to prevent runaway risk.
• HOLD_IF_SCORE_ABOVE uses 0..1 scale internally (0.85 == 85%).
"""

from __future__ import annotations

import os
import json
import math
import time
import asyncio
import logging
from typing import Any, Dict, List, Optional, Tuple
from statistics import mean

try:
    import redis.asyncio as aioredis
except Exception:
    aioredis = None

# Optional LLM tuner
_ENABLE_AI_TUNING = os.getenv("ENABLE_AI_TUNING", "true").lower() == "true"
_OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
try:
    from openai import AsyncOpenAI  # openai>=1.x
except Exception:  # pragma: no cover
    AsyncOpenAI = None

# Notifiers (safe no-ops if not configured)
from utils_pkg.core_utils import send_telegram, log_to_notion

logger = logging.getLogger("self_learning_ai")
logging.basicConfig(level=logging.INFO)

# ----------------------
# Redis & runtime config
# ----------------------
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")
PARAMS_KEY = "strategy:params"
TRADES_KEY = "strategy:trades"
STATS_KEY = "strategy:stats"
LAST_TUNE_TS_KEY = "strategy:last_tune_ts"
LAST_LLM_TS_KEY = "strategy:last_llm_ts"

LEARNING_INTERVAL_SECONDS = int(os.getenv("LEARNING_INTERVAL_SECONDS", "1800"))  # 30m
LLM_MIN_INTERVAL_SECONDS = int(os.getenv("LLM_MIN_INTERVAL_SECONDS", "3600"))    # 60m
MIN_TRADES_FOR_TUNING = int(os.getenv("MIN_TRADES_FOR_TUNING", "12"))
MAX_TRADES_KEEP = int(os.getenv("MAX_TRADES_KEEP", "2000"))

# ----------------------
# Parameter schema
# ----------------------
# Hard safety rails: min/max values to clamp any change
BOUNDS: Dict[str, Tuple[float, float]] = {
    "STOP_LOSS_PERCENT": (8.0, 35.0),
    "TARGET_PROFIT_PERCENT": (0.0, 500.0),  # 0 disables TP
    "MAX_HOLD_TIME_MINUTES": (30.0, 1440.0),
    "REINVEST_PERCENT": (10.0, 95.0),
    "SLIPPAGE": (0.002, 0.02),  # 0.2%..2.0%
    "HOLD_IF_SCORE_ABOVE": (0.60, 0.98),  # AI fraud score gate 0..1
    "PARTIAL_TP_CHUNKS": (2.0, 8.0),
    "MAX_RISK_PER_TRADE": (0.02, 0.10),
    "MIN_SOL_BALANCE": (0.1, 2.0),
    "MIN_PROFIT_THRESHOLD": (5.0, 25.0),
}

# Default baseline (merged with any Redis values)
DEFAULTS: Dict[str, Any] = {
    "STOP_LOSS_PERCENT": 20.0,
    "TARGET_PROFIT_PERCENT": 200.0,      # can be 0 to disable
    "MAX_HOLD_TIME_MINUTES": 360,
    "REINVEST_PERCENT": 80.0,
    "SLIPPAGE": 0.005,                   # 0.5%
    "TRAILING_STOP_ENABLED": True,
    "HOLD_IF_SCORE_ABOVE": 0.85,         # 0..1
    "PARTIAL_TP_CHUNKS": 4,
    "MAX_RISK_PER_TRADE": 0.05,
    "MIN_SOL_BALANCE": 0.2,
    "MIN_PROFIT_THRESHOLD": 10.0,        # % profit considered "win"
}

# In-memory fallbacks if Redis is absent
_mem_params = DEFAULTS.copy()
_mem_trades: List[Dict[str, Any]] = []
_mem_stats: Dict[str, Any] = {}
_mem_last_tune_ts: float = 0.0
_mem_last_llm_ts: float = 0.0


# -------------
# Redis helpers
# -------------
async def _get_redis(url: Optional[str] = None):
    if aioredis is None:
        return None
    return aioredis.from_url(url or REDIS_URL, decode_responses=True)


async def get_strategy_params(redis=None) -> Dict[str, Any]:
    """Return current parameters (Redis → merged onto DEFAULTS)."""
    try:
        if redis is None:
            redis = await _get_redis()
        if redis:
            raw = await redis.get(PARAMS_KEY)
            if raw:
                data = json.loads(raw)
                return _merge_defaults(data)
        return _mem_params.copy()
    except Exception as e:
        logger.debug(f"get_strategy_params fallback due to: {e}")
        return _mem_params.copy()


async def set_strategy_params(redis, params: Dict[str, Any]) -> None:
    """Write validated params to Redis (and memory)."""
    cleaned = _sanitize_params(_merge_defaults(params))
    try:
        if redis is None:
            redis = await _get_redis()
        if redis:
            await redis.set(PARAMS_KEY, json.dumps(cleaned))
        _mem_params.update(cleaned)
    except Exception as e:
        logger.warning(f"set_strategy_params failed; memory only. {e}")
        _mem_params.update(cleaned)


def _merge_defaults(override: Dict[str, Any]) -> Dict[str, Any]:
    merged = DEFAULTS.copy()
    merged.update(override or {})
    return merged


def _sanitize_params(params: Dict[str, Any]) -> Dict[str, Any]:
    """Clamp into BOUNDS; coerce booleans/ints appropriately."""
    out = params.copy()
    for k, (lo, hi) in BOUNDS.items():
        if k in out:
            try:
                v = float(out[k])
                out[k] = float(min(max(v, lo), hi))
            except Exception:
                out[k] = DEFAULTS[k]
    # booleans
    out["TRAILING_STOP_ENABLED"] = bool(out.get("TRAILING_STOP_ENABLED", True))
    # ints where it makes sense
    out["PARTIAL_TP_CHUNKS"] = int(out.get("PARTIAL_TP_CHUNKS", DEFAULTS["PARTIAL_TP_CHUNKS"]))
    out["MAX_HOLD_TIME_MINUTES"] = int(out.get("MAX_HOLD_TIME_MINUTES", DEFAULTS["MAX_HOLD_TIME_MINUTES"]))
    return out


# ----------------------
# Trade & stats handling
# ----------------------
async def process_trade_closure(
    token_address: str,
    buy_price: float,
    sell_price: float,
    amount: float,
    partial: bool,
    timestamp: str,
) -> None:
    """
    Record a (partial or full) trade closure.
    profit_pct is computed from buy/sell. Amount is used as weight.
    """
    try:
        profit_pct = ((sell_price - buy_price) / buy_price) * 100.0 if buy_price > 0 else 0.0
        rec = {
            "token": token_address,
            "profit_pct": float(profit_pct),
            "amount": float(amount),
            "partial": bool(partial),
            "timestamp": timestamp,
        }
        # Push to Redis list (cap length)
        redis = await _get_redis()
        if redis:
            await redis.lpush(TRADES_KEY, json.dumps(rec))
            await redis.ltrim(TRADES_KEY, 0, MAX_TRADES_KEEP - 1)
            # recompute stats
            await _recompute_and_store_stats(redis)
        else:
            _mem_trades.insert(0, rec)
            _mem_trades[:] = _mem_trades[:MAX_TRADES_KEEP]
            await _recompute_and_store_stats(None)
    except Exception as e:
        logger.warning(f"process_trade_closure failed: {e}")


async def get_perf_snapshot(redis=None) -> Dict[str, Any]:
    """Read aggregated stats."""
    try:
        if redis is None:
            redis = await _get_redis()
        if redis:
            raw = await redis.get(STATS_KEY)
            if raw:
                return json.loads(raw)
        return _mem_stats.copy()
    except Exception:
        return _mem_stats.copy()


async def _recompute_and_store_stats(redis) -> None:
    """Aggregate win rate, avg profit/loss, streaks, and regime hints."""
    try:
        trades = await _read_trades(redis, limit=MAX_TRADES_KEEP)
        if not trades:
            stats = {
                "trades": 0,
                "wins": 0,
                "losses": 0,
                "avg_profit": 0.0,
                "avg_loss": 0.0,
                "win_rate": 0.0,
                "pnl_mean": 0.0,
                "pnl_std": 0.0,
                "recent_win_rate": 0.0,
                "recent_avg_profit": 0.0,
                "recent_avg_loss": 0.0,
            }
            await _store_stats(redis, stats)
            return

        params = await get_strategy_params(redis)
        win_cut = float(params.get("MIN_PROFIT_THRESHOLD", DEFAULTS["MIN_PROFIT_THRESHOLD"]))

        pnl = [float(t["profit_pct"]) for t in trades]
        wins = [x for x in pnl if x >= win_cut]
        losses = [x for x in pnl if x < win_cut]

        def _safe_mean(xs: List[float]) -> float:
            return float(mean(xs)) if xs else 0.0

        # Simple std (population)
        mu = _safe_mean(pnl)
        var = _safe_mean([(x - mu) ** 2 for x in pnl])
        std = math.sqrt(var)

        recent = pnl[: min(30, len(pnl))]
        r_wins = [x for x in recent if x >= win_cut]
        r_losses = [x for x in recent if x < win_cut]

        stats = {
            "trades": len(pnl),
            "wins": len(wins),
            "losses": len(losses),
            "avg_profit": _safe_mean(wins),
            "avg_loss": _safe_mean(losses),
            "win_rate": (len(wins) / len(pnl)) if pnl else 0.0,
            "pnl_mean": mu,
            "pnl_std": std,
            "recent_win_rate": (len(r_wins) / len(recent)) if recent else 0.0,
            "recent_avg_profit": _safe_mean(r_wins),
            "recent_avg_loss": _safe_mean(r_losses),
        }
        await _store_stats(redis, stats)
    except Exception as e:
        logger.debug(f"_recompute_and_store_stats skipped: {e}")


async def _read_trades(redis, limit: int) -> List[Dict[str, Any]]:
    try:
        if redis:
            raw_list = await redis.lrange(TRADES_KEY, 0, limit - 1)
            return [json.loads(x) for x in raw_list]
        return _mem_trades[:limit]
    except Exception:
        return []


async def _store_stats(redis, stats: Dict[str, Any]) -> None:
    try:
        if redis:
            await redis.set(STATS_KEY, json.dumps(stats))
        _mem_stats.update(stats)
    except Exception as e:
        logger.debug(f"_store_stats failed: {e}")


# ----------------------
# Tuning (rules + LLM)
# ----------------------
def _rule_based_adjust(params: Dict[str, Any], stats: Dict[str, Any]) -> Dict[str, Any]:
    """Conservative rules that gently adapt risk."""
    out = params.copy()
    win_rate = float(stats.get("win_rate", 0.0))
    avg_profit = float(stats.get("avg_profit", 0.0))
    avg_loss = float(stats.get("avg_loss", 0.0))
    recent_wr = float(stats.get("recent_win_rate", 0.0))

    # If we're losing, tighten stop, reduce slippage, lower TP
    if win_rate < 0.45 or recent_wr < 0.40:
        out["STOP_LOSS_PERCENT"] = max(BOUNDS["STOP_LOSS_PERCENT"][0], out["STOP_LOSS_PERCENT"] - 1.0)
        out["SLIPPAGE"] = max(BOUNDS["SLIPPAGE"][0], out["SLIPPAGE"] - 0.0005)
        # Encourage earlier profit taking if TP is very high
        if out["TARGET_PROFIT_PERCENT"] > 100:
            out["TARGET_PROFIT_PERCENT"] = max(50.0, out["TARGET_PROFIT_PERCENT"] - 10.0)
        # Reduce risk per trade slightly
        out["MAX_RISK_PER_TRADE"] = max(BOUNDS["MAX_RISK_PER_TRADE"][0], out["MAX_RISK_PER_TRADE"] - 0.005)
        # Lower reinvest when regime is bad
        out["REINVEST_PERCENT"] = max(BOUNDS["REINVEST_PERCENT"][0], out["REINVEST_PERCENT"] - 5.0)

    # If we're crushing, loosen stop gradually, increase reinvest
    if win_rate > 0.65 and avg_profit > 15:
        out["STOP_LOSS_PERCENT"] = min(BOUNDS["STOP_LOSS_PERCENT"][1], out["STOP_LOSS_PERCENT"] + 1.0)
        out["REINVEST_PERCENT"] = min(BOUNDS["REINVEST_PERCENT"][1], out["REINVEST_PERCENT"] + 3.0)
        # allow slightly higher slippage to get fills in momentum
        out["SLIPPAGE"] = min(BOUNDS["SLIPPAGE"][1], out["SLIPPAGE"] + 0.0002)

    # Holding too long with mediocre outcomes → shrink max hold
    if stats.get("pnl_mean", 0.0) < 5.0 and out["MAX_HOLD_TIME_MINUTES"] > 60:
        out["MAX_HOLD_TIME_MINUTES"] = max(60, out["MAX_HOLD_TIME_MINUTES"] - 30)

    # Clamp to bounds finally
    out = _sanitize_params(out)
    return out


async def _maybe_llm_adjust(params: Dict[str, Any], stats: Dict[str, Any], redis) -> Dict[str, Any]:
    """
    Optionally ask an LLM for small tweaks. We accept only JSON with known keys
    and then clamp into bounds. Rate-limited via LAST_LLM_TS_KEY.
    """
    if not _ENABLE_AI_TUNING or AsyncOpenAI is None:
        return params

    try:
        now = time.time()
        last_ts = 0.0
        if redis:
            raw = await redis.get(LAST_LLM_TS_KEY)
            if raw:
                last_ts = float(raw)
        else:
            last_ts = _mem_last_llm_ts

        if now - last_ts < LLM_MIN_INTERVAL_SECONDS:
            return params

        # Enough data?
        if int(stats.get("trades", 0)) < max(MIN_TRADES_FOR_TUNING, 8):
            return params

        client = AsyncOpenAI()
        prompt = (
            "You are an expert crypto execution & risk optimizer for a Solana sniper bot.\n"
            "Given the current parameters and performance stats, propose SMALL adjustments "
            "as strict JSON with only keys that exist in 'params'. Keep changes subtle and "
            "inside sensible bounds. Goal: maximize risk-adjusted returns while limiting drawdowns.\n\n"
            f"params = {json.dumps(params, indent=2)}\n"
            f"stats = {json.dumps(stats, indent=2)}\n\n"
            "Reply ONLY with a JSON object, no commentary."
        )

        resp = await client.chat.completions.create(
            model=_OPENAI_MODEL,
            messages=[{"role": "user", "content": prompt}],
            temperature=0.2,
            max_tokens=300,
        )
        text = resp.choices[0].message.content.strip()
        # Extract JSON
        try:
            proposed = json.loads(text)
            # Only keep known keys
            filtered = {k: proposed[k] for k in proposed.keys() if k in DEFAULTS}
            if filtered:
                merged = params.copy()
                merged.update(filtered)
                merged = _sanitize_params(merged)
                # Save last LLM ts
                if redis:
                    await redis.set(LAST_LLM_TS_KEY, str(now))
                else:
                    global _mem_last_llm_ts
                    _mem_last_llm_ts = now
                return merged
        except Exception:
            # ignore bad LLM output
            pass
    except Exception as e:
        logger.debug(f"LLM tuning skipped: {e}")

    return params


async def _apply_and_announce(old: Dict[str, Any], new: Dict[str, Any], stats: Dict[str, Any], redis) -> None:
    """Persist, announce diffs to Telegram/Notion."""
    await set_strategy_params(redis, new)
    diffs = {k: (old.get(k), new.get(k)) for k in sorted(new.keys()) if old.get(k) != new.get(k)}
    if not diffs:
        return

    # Telegram
    try:
        lines = [f"🤖 Strategy auto-tuned ({len(diffs)} changes):"]
        for k, (a, b) in diffs.items():
            lines.append(f"• {k}: {a} → {b}")
        lines.append(f"Win Rate: {stats.get('win_rate', 0.0):.2%} | μ={stats.get('pnl_mean',0.0):+.2f}% σ={stats.get('pnl_std',0.0):.2f}")
        await send_telegram("\n".join(lines))
    except Exception:
        pass

    # Notion (optional)
    try:
        await log_to_notion(
            "AI Strategy Parameters",
            {
                "stop_loss_percent": new["STOP_LOSS_PERCENT"],
                "reinvestment_percent": new["REINVEST_PERCENT"],
                "max_hold_time_minutes": new["MAX_HOLD_TIME_MINUTES"],
                "min_profit_threshold": new["MIN_PROFIT_THRESHOLD"],
                "slippage": new["SLIPPAGE"],
                "hold_if_score_above": new["HOLD_IF_SCORE_ABOVE"],
                "win_rate": float(stats.get("win_rate", 0.0)),
                "avg_profit": float(stats.get("avg_profit", 0.0)),
                "avg_loss": float(stats.get("avg_loss", 0.0)),
            },
        )
    except Exception:
        pass


async def _tune_once(redis=None) -> None:
    """One pass: recompute stats, adjust params, persist."""
    if redis is None:
        redis = await _get_redis()

    # Guard: rate limit full tuning loop
    now = time.time()
    try:
        last_ts = float((await redis.get(LAST_TUNE_TS_KEY)) or 0) if redis else _mem_last_tune_ts
    except Exception:
        last_ts = _mem_last_tune_ts

    if now - last_ts < max(LEARNING_INTERVAL_SECONDS * 0.5, 300):
        return

    await _recompute_and_store_stats(redis)
    stats = await get_perf_snapshot(redis)
    cur = await get_strategy_params(redis)

    # Need enough trades to adjust meaningfully
    if int(stats.get("trades", 0)) < 4:
        return

    new_params = _rule_based_adjust(cur, stats)
    new_params = await _maybe_llm_adjust(new_params, stats, redis)  # optional

    await _apply_and_announce(cur, new_params, stats, redis)

    # Persist last tune ts
    try:
        if redis:
            await redis.set(LAST_TUNE_TS_KEY, str(now))
        else:
            global _mem_last_tune_ts
            _mem_last_tune_ts = now
    except Exception:
        pass


# ----------------------
# Background loop
# ----------------------
async def run_self_learning_loop(redis=None) -> None:
    """Forever loop to keep tuning parameters."""
    if redis is None:
        redis = await _get_redis()
    logger.info(f"🧠 Self-learning loop started (interval={LEARNING_INTERVAL_SECONDS}s)")
    while True:
        try:
            await _tune_once(redis)
        except Exception as e:
            logger.error(f"[SELF_LEARNING] Tuning error: {e}", exc_info=True)
        await asyncio.sleep(LEARNING_INTERVAL_SECONDS)
