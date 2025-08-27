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
• Announce changes via Telegram (and Notion in a harmless, best-effort way).

Public API
----------
get_strategy_params(redis) -> dict
set_strategy_params(redis, params: dict) -> None
process_trade_closure(token_address, buy_price, sell_price, amount, partial, timestamp) -> None
get_perf_snapshot(redis) -> dict
run_self_learning_loop(redis=None) -> background loop

Notes
-----
Redis keys:
  - strategy:params         -> JSON blob of current params
  - strategy:trades         -> LIST (JSON strings), capped to MAX_TRADES_KEEP
  - strategy:stats          -> JSON blob of aggregated stats
  - strategy:last_tune_ts   -> float timestamp
  - strategy:last_llm_ts    -> float timestamp

All numeric params have strict bounds to prevent runaway risk.
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

# ---------- optional deps ----------
try:
    import redis.asyncio as aioredis
except Exception:  # pragma: no cover
    aioredis = None  # falls back to in-memory

_ENABLE_AI_TUNING = os.getenv("ENABLE_AI_TUNING", "true").lower() in ("1", "true", "yes", "on")
_OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
try:
    from openai import AsyncOpenAI  # openai>=1.x
except Exception:  # pragma: no cover
    AsyncOpenAI = None

# Notifiers (safe no-ops if envs not set)
from utils_pkg.core_utils import send_telegram, log_to_notion

logger = logging.getLogger("self_learning_ai")
logging.basicConfig(level=logging.INFO)

# ---------- Redis & runtime config ----------
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

# ---------- parameter schema ----------
BOUNDS: Dict[str, Tuple[float, float]] = {
    "STOP_LOSS_PERCENT": (8.0, 35.0),
    "TARGET_PROFIT_PERCENT": (0.0, 500.0),      # 0 disables TP
    "MAX_HOLD_TIME_MINUTES": (30.0, 1440.0),
    "REINVEST_PERCENT": (10.0, 95.0),
    "SLIPPAGE": (0.002, 0.02),                  # 0.2%..2.0%
    "HOLD_IF_SCORE_ABOVE": (0.60, 0.98),        # 0..1
    "PARTIAL_TP_CHUNKS": (2.0, 8.0),
    "MAX_RISK_PER_TRADE": (0.02, 0.10),
    "MIN_SOL_BALANCE": (0.1, 2.0),
    "MIN_PROFIT_THRESHOLD": (5.0, 25.0),
}

DEFAULTS: Dict[str, Any] = {
    "STOP_LOSS_PERCENT": 20.0,
    "TARGET_PROFIT_PERCENT": 200.0,
    "MAX_HOLD_TIME_MINUTES": 360,
    "REINVEST_PERCENT": 80.0,
    "SLIPPAGE": 0.005,                   # 0.5%
    "TRAILING_STOP_ENABLED": True,
    "HOLD_IF_SCORE_ABOVE": 0.85,
    "PARTIAL_TP_CHUNKS": 4,
    "MAX_RISK_PER_TRADE": 0.05,
    "MIN_SOL_BALANCE": 0.2,
    "MIN_PROFIT_THRESHOLD": 10.0,        # % profit counted as a "win"
}

# ---------- in-memory fallbacks ----------
_mem_params = DEFAULTS.copy()
_mem_trades: List[Dict[str, Any]] = []
_mem_stats: Dict[str, Any] = {}
_mem_last_tune_ts: float = 0.0
_mem_last_llm_ts: float = 0.0


# ---------- Redis helpers ----------
async def _get_redis(url: Optional[str] = None):
    if aioredis is None:
        return None
    return aioredis.from_url(url or REDIS_URL, decode_responses=True)


# ---------- params I/O ----------
def _merge_defaults(override: Dict[str, Any]) -> Dict[str, Any]:
    merged = DEFAULTS.copy()
    merged.update(override or {})
    return merged


def _sanitize_params(params: Dict[str, Any]) -> Dict[str, Any]:
    out = params.copy()
    for k, (lo, hi) in BOUNDS.items():
        if k in out:
            try:
                v = float(out[k])
                out[k] = float(min(max(v, lo), hi))
            except Exception:
                out[k] = DEFAULTS[k]
    out["TRAILING_STOP_ENABLED"] = bool(out.get("TRAILING_STOP_ENABLED", True))
    out["PARTIAL_TP_CHUNKS"] = int(out.get("PARTIAL_TP_CHUNKS", DEFAULTS["PARTIAL_TP_CHUNKS"]))
    out["MAX_HOLD_TIME_MINUTES"] = int(out.get("MAX_HOLD_TIME_MINUTES", DEFAULTS["MAX_HOLD_TIME_MINUTES"]))
    return out


async def get_strategy_params(redis=None) -> Dict[str, Any]:
    try:
        if redis is None:
            redis = await _get_redis()
        if redis:
            raw = await redis.get(PARAMS_KEY)
            if raw:
                return _sanitize_params(_merge_defaults(json.loads(raw)))
        return _mem_params.copy()
    except Exception:
        return _mem_params.copy()


async def set_strategy_params(redis, params: Dict[str, Any]) -> None:
    cleaned = _sanitize_params(_merge_defaults(params))
    try:
        if redis is None:
            redis = await _get_redis()
        if redis:
            await redis.set(PARAMS_KEY, json.dumps(cleaned))
        _mem_params.update(cleaned)
    except Exception:
        _mem_params.update(cleaned)


# ---------- trade recording & stats ----------
async def process_trade_closure(
    token_address: str,
    buy_price: float,
    sell_price: float,
    amount: float,
    partial: bool,
    timestamp: str,
) -> None:
    """Record a (partial or full) trade closure and refresh stats."""
    try:
        profit_pct = ((sell_price - buy_price) / buy_price) * 100.0 if buy_price > 0 else 0.0
        rec = {
            "token": token_address,
            "profit_pct": float(profit_pct),
            "amount": float(amount),
            "partial": bool(partial),
            "timestamp": timestamp,
        }
        redis = await _get_redis()
        if redis:
            await redis.lpush(TRADES_KEY, json.dumps(rec))
            await redis.ltrim(TRADES_KEY, 0, MAX_TRADES_KEEP - 1)
            await _recompute_and_store_stats(redis)
        else:
            _mem_trades.insert(0, rec)
            del _mem_trades[MAX_TRADES_KEEP:]
            await _recompute_and_store_stats(None)
    except Exception as e:
        logger.warning(f"process_trade_closure failed: {e}")


async def _read_trades(redis, limit: int) -> List[Dict[str, Any]]:
    try:
        if redis:
            raw = await redis.lrange(TRADES_KEY, 0, limit - 1)
            return [json.loads(x) for x in raw]
        return _mem_trades[:limit]
    except Exception:
        return []


async def _store_stats(redis, stats: Dict[str, Any]) -> None:
    try:
        if redis:
            await redis.set(STATS_KEY, json.dumps(stats))
        _mem_stats.update(stats)
    except Exception:
        pass


async def _recompute_and_store_stats(redis) -> None:
    try:
        trades = await _read_trades(redis, MAX_TRADES_KEEP)
        if not trades:
            await _store_stats(redis, {
                "trades": 0, "wins": 0, "losses": 0,
                "avg_profit": 0.0, "avg_loss": 0.0, "win_rate": 0.0,
                "pnl_mean": 0.0, "pnl_std": 0.0,
                "recent_win_rate": 0.0, "recent_avg_profit": 0.0, "recent_avg_loss": 0.0,
            })
            return

        params = await get_strategy_params(redis)
        win_cut = float(params.get("MIN_PROFIT_THRESHOLD", DEFAULTS["MIN_PROFIT_THRESHOLD"]))

        pnl = [float(t["profit_pct"]) for t in trades]
        wins = [x for x in pnl if x >= win_cut]
        losses = [x for x in pnl if x < win_cut]

        def _safe_mean(xs: List[float]) -> float:
            return float(mean(xs)) if xs else 0.0

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


async def get_perf_snapshot(redis=None) -> Dict[str, Any]:
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


# ---------- tuning (rules + optional LLM) ----------
def _rule_based_adjust(params: Dict[str, Any], stats: Dict[str, Any]) -> Dict[str, Any]:
    out = params.copy()
    win_rate = float(stats.get("win_rate", 0.0))
    avg_profit = float(stats.get("avg_profit", 0.0))
    recent_wr = float(stats.get("recent_win_rate", 0.0))

    # Losing → tighten risk
    if win_rate < 0.45 or recent_wr < 0.40:
        out["STOP_LOSS_PERCENT"] = max(BOUNDS["STOP_LOSS_PERCENT"][0], out["STOP_LOSS_PERCENT"] - 1.0)
        out["SLIPPAGE"] = max(BOUNDS["SLIPPAGE"][0], out["SLIPPAGE"] - 0.0005)
        if out.get("TARGET_PROFIT_PERCENT", 0) > 100:
            out["TARGET_PROFIT_PERCENT"] = max(50.0, out["TARGET_PROFIT_PERCENT"] - 10.0)
        out["MAX_RISK_PER_TRADE"] = max(BOUNDS["MAX_RISK_PER_TRADE"][0], out["MAX_RISK_PER_TRADE"] - 0.005)
        out["REINVEST_PERCENT"] = max(BOUNDS["REINVEST_PERCENT"][0], out["REINVEST_PERCENT"] - 5.0)

    # Crushing → allow a little more
    if win_rate > 0.65 and avg_profit > 15:
        out["STOP_LOSS_PERCENT"] = min(BOUNDS["STOP_LOSS_PERCENT"][1], out["STOP_LOSS_PERCENT"] + 1.0)
        out["REINVEST_PERCENT"] = min(BOUNDS["REINVEST_PERCENT"][1], out["REINVEST_PERCENT"] + 3.0)
        out["SLIPPAGE"] = min(BOUNDS["SLIPPAGE"][1], out["SLIPPAGE"] + 0.0002)

    if stats.get("pnl_mean", 0.0) < 5.0 and out["MAX_HOLD_TIME_MINUTES"] > 60:
        out["MAX_HOLD_TIME_MINUTES"] = max(60, out["MAX_HOLD_TIME_MINUTES"] - 30)

    return _sanitize_params(out)


async def _maybe_llm_adjust(params: Dict[str, Any], stats: Dict[str, Any], redis) -> Dict[str, Any]:
    global _mem_last_llm_ts  # declare up-front (fixes “used prior to global declaration”)
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
        text = (resp.choices[0].message.content or "").strip()
        try:
            proposed = json.loads(text)
            filtered = {k: proposed[k] for k in proposed.keys() if k in DEFAULTS}
            if filtered:
                merged = _sanitize_params({**params, **filtered})
                if redis:
                    await redis.set(LAST_LLM_TS_KEY, str(now))
                else:
                    _mem_last_llm_ts = now
                return merged
        except Exception:
            pass
    except Exception as e:
        logger.debug(f"LLM tuning skipped: {e}")

    return params


async def _apply_and_announce(old: Dict[str, Any], new: Dict[str, Any], stats: Dict[str, Any], redis) -> None:
    await set_strategy_params(redis, new)
    diffs = {k: (old.get(k), new.get(k)) for k in sorted(new.keys()) if old.get(k) != new.get(k)}
    if not diffs:
        return

    # Telegram (safe best-effort)
    try:
        lines = [f"🤖 Strategy auto-tuned ({len(diffs)} changes):"]
        for k, (a, b) in diffs.items():
            lines.append(f"• {k}: {a} → {b}")
        lines.append(f"Win Rate: {stats.get('win_rate', 0.0):.2%} | μ={stats.get('pnl_mean',0.0):+.2f}% σ={stats.get('pnl_std',0.0):.2f}")
        await send_telegram("\n".join(lines))
    except Exception:
        pass

    # Notion (only if your log_to_notion has the trade-style signature)
    try:
        # Pack a compact “reason” string so it fits the existing helper signature.
        reason = (
            f"SL={new['STOP_LOSS_PERCENT']}% "
            f"TP={new['TARGET_PROFIT_PERCENT']}% "
            f"Hold={new['MAX_HOLD_TIME_MINUTES']}m "
            f"Reinv={new['REINVEST_PERCENT']}% "
            f"Slip={new['SLIPPAGE']}"
        )
        # token, symbol, balance, entry, current, profit, reason
        await log_to_notion(
            token="STRAT",
            symbol="Params Update",
            balance=float(stats.get("trades", 0)),
            entry=float(new["STOP_LOSS_PERCENT"]),
            current=float(new["REINVEST_PERCENT"]),
            profit=float(stats.get("win_rate", 0.0) * 100.0),
            reason=reason,
        )
    except Exception:
        # If your project doesn’t use Notion, this is silently skipped.
        pass


async def _tune_once(redis=None) -> None:
    global _mem_last_tune_ts  # declare up-front (fixes global usage)
    if redis is None:
        redis = await _get_redis()

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

    if int(stats.get("trades", 0)) < 4:
        if redis:
            await redis.set(LAST_TUNE_TS_KEY, str(now))
        else:
            _mem_last_tune_ts = now
        return

    new_params = _rule_based_adjust(cur, stats)
    new_params = await _maybe_llm_adjust(new_params, stats, redis)  # optional
    await _apply_and_announce(cur, new_params, stats, redis)

    try:
        if redis:
            await redis.set(LAST_TUNE_TS_KEY, str(now))
        else:
            _mem_last_tune_ts = now
    except Exception:
        pass


# ---------- background loop ----------
async def run_self_learning_loop(redis=None) -> None:
    if redis is None:
        redis = await _get_redis()
    logger.info(f"🧠 Self-learning loop started (interval={LEARNING_INTERVAL_SECONDS}s)")
    while True:
        try:
            await _tune_once(redis)
        except Exception as e:
            logger.error(f"[SELF_LEARNING] Tuning error: {e}", exc_info=True)
        await asyncio.sleep(LEARNING_INTERVAL_SECONDS)
