from __future__ import annotations

"""
utils_pkg/self_learning_ai_strategy.py

Self-learning strategy brain (V3+++ / quant-grade)
==================================================

High-level role
---------------
Central "brain" that auto-tunes strategy knobs based on:

  • Trade-level performance (per-trade P&L in %)
  • Recent / long-horizon stats (winrate, Sharpe-ish, downside dev)
  • Kelly-style risk suggestion (clipped & smoothed)
  • Regime & risk context from risk_manager (risk:ctx in Redis)
  • Equity-curve "plan" tracking (PLAN_* fields)
  • Optional OpenAI (LLM) refinements with HARD numeric safety bounds
  • Global health score (0..100) for dashboards / HARD_HALT gating
  • Hard “risk API contract” with risk_manager / reinvestor:
        this module will *never* emit params that exceed risk API
        ceilings (max_risk_per_trade, min_sol_balance, hard_halt).

Design goals
------------
  • Slow, mean-reverting updates (no per-trade jitter).
  • Tight numeric BOUNDS on all tunables.
  • Fully usable without Redis (falls back to in-memory).
  • Explainable core: rule-based logic + Kelly + plan; LLM is a tiny nudge.
  • First-class coupling with:
        - utils_pkg.risk_manager
            • MAX_RISK_PER_TRADE
            • MIN_SOL_BALANCE
            • HARD_HALT
            • PLAN_* (equity curve)
            • risk:ctx snapshot
        - utils_pkg.exit_engine
            • STOP_LOSS_PERCENT
            • TARGET_PROFIT_PERCENT
            • MAX_HOLD_TIME_MINUTES
            • SLIPPAGE
        - utils_pkg.ai_sell_logic
            • HOLD_IF_SCORE_ABOVE
            • MIN_PROFIT_THRESHOLD
            • PARTIAL_TP_CHUNKS
        - utils_pkg.ai_entry_logic
            • ENTRY_SCORE_GATE
            • ENTRY_MAX_PARALLEL (optional)
        - utils_pkg.reinvestor
            • REINVEST_PERCENT

Public API
----------
    async def get_strategy_params(redis=None) -> dict
    async def set_strategy_params(redis, params: dict) -> None

    async def process_trade_closure(
        token_address: str,
        buy_price: float,
        sell_price: float,
        amount: float,
        partial: bool,
        timestamp: str,
        *,
        entry_score: float | None = None,
        hold_minutes: float | None = None,
    ) -> None

    async def get_perf_snapshot(redis=None) -> dict

    async def tune_once(redis=None) -> None
    async def run_self_learning_loop(redis=None) -> None

    async def get_health_snapshot(redis=None) -> dict

    # Optional helpers for dashboards / tools:
    async def get_plan_snapshot(redis=None) -> dict
    async def get_param_history(redis=None, limit: int = 50) -> list[dict]
    async def get_strategy_with_risk(redis=None) -> dict
        # { "params": ..., "risk_ctx": ..., "health": ... }

Redis keys
----------
  strategy:params             -> JSON blob of current params
  strategy:trades             -> LIST of JSON, newest first
  strategy:stats              -> JSON blob with aggregated stats
  strategy:last_tune_ts       -> float timestamp (seconds)
  strategy:last_llm_ts        -> float timestamp (seconds)
  strategy:param_history      -> LIST of compact param diffs
  strategy:health             -> JSON health snapshot

This module is fully usable without Redis (falls back to in-memory).
"""

import asyncio
import json
import logging
import math
import os
import time
from statistics import mean
from typing import Any, Dict, List, Optional, Tuple

# =============================================================================
# Logging
# =============================================================================

logger = logging.getLogger("self_learning_ai")
if not logger.handlers:
    logging.basicConfig(level=logging.INFO)


# =============================================================================
# Optional deps
# =============================================================================

# Redis (optional; we still run without it)
try:
    import redis.asyncio as aioredis  # type: ignore[import-not-found]
except Exception:  # pragma: no cover
    aioredis = None  # type: ignore[assignment]

# OpenAI client (optional; pure rules+Kelly if missing or disabled)
_ENABLE_AI_TUNING = (
    os.getenv("ENABLE_AI_TUNING", "true").strip().lower()
    in ("1", "true", "yes", "on")
)
_OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini")

try:
    from openai import AsyncOpenAI  # type: ignore[import-not-found]
except Exception:  # pragma: no cover
    AsyncOpenAI = None  # type: ignore[assignment]

# Notifiers (best-effort, fall back to local logging)
try:
    from utils_pkg.core_utils import send_telegram, log_to_notion  # type: ignore[attr-defined]
except Exception:  # pragma: no cover

    async def send_telegram(msg: str) -> None:
        logger.info("[TG] %s", msg)

    async def log_to_notion(**_: Any) -> None:
        return

# Structured logger (optional)
try:
    from utils_pkg.data_logger import log_event  # type: ignore[attr-defined]
except Exception:  # pragma: no cover

    async def log_event(*_: Any, **__: Any) -> None:
        return


# =============================================================================
# Redis / runtime config
# =============================================================================

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")

PARAMS_KEY = "strategy:params"
TRADES_KEY = "strategy:trades"
STATS_KEY = "strategy:stats"
LAST_TUNE_TS_KEY = "strategy:last_tune_ts"
LAST_LLM_TS_KEY = "strategy:last_llm_ts"
PARAM_HISTORY_KEY = "strategy:param_history"
HEALTH_KEY = "strategy:health"

# Cadence and history
LEARNING_INTERVAL_SECONDS = int(
    os.getenv("LEARNING_INTERVAL_SECONDS", "1800")  # default ~30m
)
LLM_MIN_INTERVAL_SECONDS = int(
    os.getenv("LLM_MIN_INTERVAL_SECONDS", "3600")   # default ~60m
)
MIN_TRADES_FOR_TUNING = int(os.getenv("MIN_TRADES_FOR_TUNING", "12"))
MAX_TRADES_KEEP = int(os.getenv("MAX_TRADES_KEEP", "2000"))
MAX_PARAM_HISTORY_KEEP = int(os.getenv("MAX_PARAM_HISTORY_KEEP", "200"))

# How many most-recent trades to emphasize for "recent" stats
RECENT_WINDOW = int(os.getenv("STRAT_RECENT_WINDOW", "30"))

# Optional: Kelly-style risk nudge
ENABLE_KELLY = (
    os.getenv("STRAT_ENABLE_KELLY", "true").strip().lower()
    in ("1", "true", "yes", "on")
)

# Optional: extra defensive mode when drawdown is severe (P&L percent)
DEFENSIVE_DD_THRESHOLD = float(os.getenv("STRAT_DEFENSIVE_DD_THRESHOLD", "12.0"))

# Optional: enable aggressive boost when things are very good
AGGRESSIVE_MIN_WINRATE = float(os.getenv("STRAT_AGG_WINRATE", "0.68"))
AGGRESSIVE_MIN_MEAN = float(os.getenv("STRAT_AGG_MEAN", "18.0"))

# Health gating for super-defensive HARD_HALT
HEALTH_HARD_HALT_WR = float(os.getenv("STRAT_HEALTH_HARD_HALT_WR", "0.28"))
HEALTH_HARD_HALT_SHARPE = float(os.getenv("STRAT_HEALTH_HARD_HALT_SHARPE", "-0.4"))

# Optional: let LLM only nudge within small % of current values
LLM_MAX_REL_MOVE = float(os.getenv("STRAT_LLM_MAX_REL_MOVE", "0.20"))  # 20% band

# Optional: allow disabling equity plan (for sim / testing)
PLAN_ENABLED = (
    os.getenv("STRAT_PLAN_ENABLED", "true").strip().lower()
    in ("1", "true", "yes", "on")
)

# Respect explicit risk API ceilings published by risk_manager / reinvestor
RESPECT_RISK_API_LIMITS = (
    os.getenv("STRAT_RESPECT_RISK_API_LIMITS", "true").strip().lower()
    in ("1", "true", "yes", "on")
)

# Optional TG spam control for health alerts
HEALTH_ALERT_MIN_SCORE = float(os.getenv("STRAT_HEALTH_ALERT_MIN_SCORE", "25.0"))
HEALTH_ALERT_INTERVAL_SEC = int(os.getenv("STRAT_HEALTH_ALERT_INTERVAL_SEC", "3600"))


# =============================================================================
# Parameter schema (V3+++)
# =============================================================================

# Hard numeric bounds for *all* tunable parameters
BOUNDS: Dict[str, Tuple[float, float]] = {
    # Exit / risk knobs
    "STOP_LOSS_PERCENT": (6.0, 35.0),
    "TARGET_PROFIT_PERCENT": (0.0, 600.0),
    "MAX_HOLD_TIME_MINUTES": (20.0, 1440.0),
    "SLIPPAGE": (0.0015, 0.02),  # 0.15%..2.0%

    # Reinvestment / risk per trade
    "REINVEST_PERCENT": (5.0, 95.0),        # % of realized profits
    "MAX_RISK_PER_TRADE": (0.01, 0.12),     # fraction of equity
    "MIN_SOL_BALANCE": (0.05, 3.0),

    # AI / entry interaction (ai_sell_logic / ai_entry_logic)
    "HOLD_IF_SCORE_ABOVE": (0.60, 0.99),
    "MIN_PROFIT_THRESHOLD": (3.0, 30.0),
    "PARTIAL_TP_CHUNKS": (2.0, 8.0),
    "ENTRY_SCORE_GATE": (0.45, 0.90),

    # Extra entry meta knobs (optional bridge to ai_entry_logic)
    "ENTRY_MAX_PARALLEL": (1.0, 16.0),         # concurrency gate
    "ENTRY_RISK_BOOST_FACTOR": (0.4, 1.6),     # scales per-trade sizing in entry

    # Extra exit ladders (used by exit_engine / ai_sell_logic)
    "EXIT_TAKE_PROFIT_1": (8.0, 60.0),
    "EXIT_TAKE_PROFIT_2": (20.0, 200.0),
    "EXIT_TAKE_PROFIT_3": (40.0, 350.0),
    "EXIT_TP_1_SIZE": (0.05, 0.50),
    "EXIT_TP_2_SIZE": (0.05, 0.60),
    "EXIT_TP_3_SIZE": (0.05, 0.80),

    # Equity-plan tuning knobs
    "PLAN_TARGET_APR_PCT": (20.0, 400.0),          # desired APR
    "PLAN_START_EQUITY_USD": (0.0, 1_000_000_000.0),
    "PLAN_START_TS": (0.0, 4_000_000_000.0),       # unix seconds
}

# Default starting values (safe + reasonable for micro-caps)
DEFAULTS: Dict[str, Any] = {
    "STOP_LOSS_PERCENT": 18.0,
    "TARGET_PROFIT_PERCENT": 180.0,
    "MAX_HOLD_TIME_MINUTES": 360,
    "REINVEST_PERCENT": 75.0,
    "SLIPPAGE": 0.005,  # 0.5%
    "TRAILING_STOP_ENABLED": True,

    "HOLD_IF_SCORE_ABOVE": 0.86,
    "PARTIAL_TP_CHUNKS": 4,
    "MAX_RISK_PER_TRADE": 0.05,
    "MIN_SOL_BALANCE": 0.25,
    "MIN_PROFIT_THRESHOLD": 10.0,

    # Extra ladders and entry bridge to AI modules
    "ENTRY_SCORE_GATE": 0.58,
    "ENTRY_MAX_PARALLEL": 4,
    "ENTRY_RISK_BOOST_FACTOR": 1.0,

    "EXIT_TAKE_PROFIT_1": 15.0,
    "EXIT_TAKE_PROFIT_2": 60.0,
    "EXIT_TAKE_PROFIT_3": 150.0,
    "EXIT_TP_1_SIZE": 0.25,
    "EXIT_TP_2_SIZE": 0.35,
    "EXIT_TP_3_SIZE": 0.40,

    # Meta strategy regime (derived, not tuned directly by LLM)
    # 0 = DEFENSIVE, 1 = NEUTRAL, 2 = AGGRESSIVE
    "STRAT_MODE": 1,

    # Equity plan fields (read by risk_manager)
    "PLAN_TARGET_APR_PCT": 180.0,
    "PLAN_START_EQUITY_USD": 0.0,
    "PLAN_START_TS": 0.0,
    "PLAN_EQUITY_TODAY_USD": 0.0,
    "PLAN_GAP_PCT": 0.0,
    "PLAN_STATUS": "n/a",

    # Global emergency brake toggled by self-learning
    "HARD_HALT": False,
}

# In-memory fallbacks when Redis is absent
_mem_params: Dict[str, Any] = DEFAULTS.copy()
_mem_trades: List[Dict[str, Any]] = []
_mem_stats: Dict[str, Any] = {}
_mem_last_tune_ts: float = 0.0
_mem_last_llm_ts: float = 0.0
_mem_last_health_alert_ts: float = 0.0


# =============================================================================
# Redis helper
# =============================================================================

async def _get_redis(url: Optional[str] = None):
    """
    Best-effort Redis client factory. Returns None if unavailable.
    """
    if aioredis is None:
        return None
    try:
        return aioredis.from_url(url or REDIS_URL, decode_responses=True)
    except Exception:
        return None


# =============================================================================
# Param helpers
# =============================================================================

def _merge_defaults(override: Dict[str, Any]) -> Dict[str, Any]:
    merged = DEFAULTS.copy()
    merged.update(override or {})
    return merged


def _sanitize_params(params: Dict[str, Any]) -> Dict[str, Any]:
    """
    Clamp all numeric params into BOUNDS and coerce booleans / ints.

    STRAT_MODE is always kept in {0,1,2} and never directly modified by LLM.
    """
    out = dict(params)

    # Numerics with bounds
    for key, (lo, hi) in BOUNDS.items():
        if key in out:
            try:
                v = float(out[key])
            except Exception:
                v = float(DEFAULTS.get(key, lo))
            v = min(max(v, lo), hi)
            out[key] = float(v)

    # Booleans
    out["TRAILING_STOP_ENABLED"] = bool(
        out.get("TRAILING_STOP_ENABLED", DEFAULTS["TRAILING_STOP_ENABLED"])
    )
    out["HARD_HALT"] = bool(out.get("HARD_HALT", DEFAULTS["HARD_HALT"]))

    # Integers
    out["PARTIAL_TP_CHUNKS"] = int(
        out.get("PARTIAL_TP_CHUNKS", DEFAULTS["PARTIAL_TP_CHUNKS"])
    )
    out["MAX_HOLD_TIME_MINUTES"] = int(
        out.get("MAX_HOLD_TIME_MINUTES", DEFAULTS["MAX_HOLD_TIME_MINUTES"])
    )
    out["ENTRY_MAX_PARALLEL"] = int(
        out.get("ENTRY_MAX_PARALLEL", DEFAULTS["ENTRY_MAX_PARALLEL"])
    )

    # Strategy regime is a tiny integer flag, always 0/1/2
    try:
        mode = int(out.get("STRAT_MODE", DEFAULTS["STRAT_MODE"]))
        if mode < 0:
            mode = 0
        if mode > 2:
            mode = 2
        out["STRAT_MODE"] = mode
    except Exception:
        out["STRAT_MODE"] = DEFAULTS["STRAT_MODE"]

    return out


async def get_strategy_params(redis=None) -> Dict[str, Any]:
    """
    Load current params from Redis (if available) else from in-memory default.

    This is the main entry point used by risk_manager, exit_engine, reinvestor, etc.
    """
    try:
        if redis is None:
            redis = await _get_redis()
        if redis:
            raw = await redis.get(PARAMS_KEY)
            if raw:
                parsed = json.loads(raw)
                return _sanitize_params(_merge_defaults(parsed))
        return _sanitize_params(_mem_params.copy())
    except Exception as e:  # pragma: no cover
        logger.debug("get_strategy_params fallback: %s", e)
        return _sanitize_params(_mem_params.copy())


async def set_strategy_params(redis, params: Dict[str, Any]) -> None:
    """
    Persist strategy params (Redis + memory) with sanitation.
    """
    cleaned = _sanitize_params(_merge_defaults(params))
    try:
        if redis is None:
            redis = await _get_redis()
        if redis:
            await redis.set(PARAMS_KEY, json.dumps(cleaned))
        _mem_params.update(cleaned)
    except Exception:
        _mem_params.update(cleaned)


# =============================================================================
# Trade recording & stats
# =============================================================================

def _safe_mean(xs: List[float]) -> float:
    return float(mean(xs)) if xs else 0.0


def _downside_deviation(pnl: List[float], threshold: float = 0.0) -> float:
    """
    Downside deviation relative to threshold (percent space).
    """
    losses = [min(0.0, x - threshold) for x in pnl]
    if not losses:
        return 0.0
    sq = [x**2 for x in losses]
    return math.sqrt(_safe_mean(sq))


def _classify_trade(profit_pct: float, hold_minutes: Optional[float]) -> str:
    """
    Lightweight tag for analytics & prompts.

    Returns one of:
      'rug', 'big_win', 'scalp_win', 'scalp_loss', 'grinder', 'scratch'
    """
    h = hold_minutes or 0.0
    p = profit_pct

    if p <= -50:
        return "rug"
    if p >= 150:
        return "big_win"
    if h <= 20 and p >= 10:
        return "scalp_win"
    if h <= 20 and p < 0:
        return "scalp_loss"
    if -5 <= p <= 5:
        return "scratch"
    return "grinder"


async def _read_trades(redis, limit: int) -> List[Dict[str, Any]]:
    try:
        if redis:
            raw = await redis.lrange(TRADES_KEY, 0, limit - 1)
            return [json.loads(x) for x in raw]
        return _mem_trades[:limit]
    except Exception:  # pragma: no cover
        return []


async def _store_stats(redis, stats: Dict[str, Any]) -> None:
    try:
        if redis:
            await redis.set(STATS_KEY, json.dumps(stats))
        _mem_stats.update(stats)
    except Exception:  # pragma: no cover
        pass


def _sharpe_like(mu: float, std: float) -> float:
    """
    Simple Sharpe-like score in percent space.
    """
    if std <= 1e-9:
        return 0.0
    return mu / max(std, 1e-9)


def _kelly_confidence(
    n_trades: int,
    sharpe: float,
    recent_sharpe: float,
) -> float:
    """
    Heuristic confidence in Kelly suggestion: 0..1.
    """
    if n_trades <= 0:
        return 0.0

    # Sample-size factor: 0 at 0 trades, → 1 near ~400 trades
    f_n = max(0.0, min(1.0, n_trades / 400.0))

    # Sharpe factor: map [-0.5, 1.5+] → [0, 1]
    s = max(-0.5, min(1.5, sharpe))
    f_s = (s + 0.5) / 2.0  # -0.5 → 0, 1.5 → 1

    rs = max(-0.5, min(1.5, recent_sharpe))
    f_rs = (rs + 0.5) / 2.0

    # Combine: more weight to recent Sharpe
    base = 0.4 * f_s + 0.6 * f_rs

    return max(0.0, min(1.0, f_n * base))


def _equity_curve_metrics(pnl: List[float]) -> Dict[str, float]:
    """
    Build simple equity curve from per-trade percent PnL and compute:
      - cumulative return %
      - max_drawdown %
    Assumes sequential trades in order.
    """
    if not pnl:
        return {"equity_cum_return_pct": 0.0, "equity_max_dd_pct": 0.0}

    equity = 1.0
    peak = 1.0
    max_dd = 0.0

    for p in reversed(pnl):  # oldest→newest for curve
        equity *= (1.0 + p / 100.0)
        peak = max(peak, equity)
        dd = (equity - peak) / peak  # negative
        max_dd = min(max_dd, dd)

    cum_ret = (equity - 1.0) * 100.0
    max_dd_pct = max_dd * 100.0
    return {
        "equity_cum_return_pct": float(cum_ret),
        "equity_max_dd_pct": float(max_dd_pct),
    }


async def _recompute_and_store_stats(redis) -> None:
    """
    Aggregate stats from recent trades (quant-grade snapshot).
    """
    try:
        trades = await _read_trades(redis, MAX_TRADES_KEEP)
        if not trades:
            await _store_stats(
                redis,
                {
                    "trades": 0,
                    "wins": 0,
                    "losses": 0,
                    "avg_profit": 0.0,
                    "avg_loss": 0.0,
                    "win_rate": 0.0,
                    "pnl_mean": 0.0,
                    "pnl_median": 0.0,
                    "pnl_std": 0.0,
                    "pnl_downside_dev": 0.0,
                    "recent_win_rate": 0.0,
                    "recent_avg_profit": 0.0,
                    "recent_avg_loss": 0.0,
                    "kelly_suggestion": 0.02,
                    "kelly_confidence": 0.0,
                    "recent_kelly_suggestion": 0.02,
                    "tag_counts": {},
                    "recent_tag_counts": {},
                    "sharpe_like": 0.0,
                    "recent_sharpe_like": 0.0,
                    "equity_cum_return_pct": 0.0,
                    "equity_max_dd_pct": 0.0,
                },
            )
            return

        params = await get_strategy_params(redis)
        win_cut = float(
            params.get("MIN_PROFIT_THRESHOLD", DEFAULTS["MIN_PROFIT_THRESHOLD"])
        )

        pnl = [float(t.get("profit_pct", 0.0)) for t in trades]
        wins = [x for x in pnl if x >= win_cut]
        losses = [x for x in pnl if x < win_cut]

        mu = _safe_mean(pnl)

        # median (robust to outliers)
        sorted_pnl = sorted(pnl)
        if sorted_pnl:
            mid = len(sorted_pnl) // 2
            if len(sorted_pnl) % 2 == 1:
                median_val = float(sorted_pnl[mid])
            else:
                median_val = float((sorted_pnl[mid - 1] + sorted_pnl[mid]) / 2.0)
        else:
            median_val = 0.0

        var = _safe_mean([(x - mu) ** 2 for x in pnl])
        std = math.sqrt(var)
        ddev = _downside_deviation(pnl, threshold=0.0)
        sharpe = _sharpe_like(mu, std)

        recent = pnl[: min(RECENT_WINDOW, len(pnl))]
        r_wins = [x for x in recent if x >= win_cut]
        r_losses = [x for x in recent if x < win_cut]
        rmu = _safe_mean(recent)
        rvar = _safe_mean([(x - rmu) ** 2 for x in recent])
        rstd = math.sqrt(rvar)
        rsharpe = _sharpe_like(rmu, rstd)

        # Kelly-style suggestion (fraction of equity) on full sample
        if std > 1e-9 and ENABLE_KELLY:
            edge = mu / 100.0
            var_r = (std / 100.0) ** 2
            kelly_raw = max(0.0, min(edge / max(var_r, 1e-9), 0.25))
        else:
            kelly_raw = 0.02

        # Same idea but only on recent window (for sanity)
        if rstd > 1e-9 and ENABLE_KELLY and recent:
            redge = rmu / 100.0
            rvar_r = (rstd / 100.0) ** 2
            recent_kelly = max(0.0, min(redge / max(rvar_r, 1e-9), 0.25))
        else:
            recent_kelly = kelly_raw

        k_conf = _kelly_confidence(len(pnl), sharpe, rsharpe)

        # Tag distribution (for prompts & health)
        tag_counts: Dict[str, int] = {}
        recent_tag_counts: Dict[str, int] = {}
        for idx, t in enumerate(trades[: min(len(trades), 300)]):
            profit_pct = float(t.get("profit_pct", 0.0))
            hold_m = float(t.get("hold_minutes", 0.0)) if t.get("hold_minutes") else None
            tag = _classify_trade(profit_pct, hold_m)
            tag_counts[tag] = tag_counts.get(tag, 0) + 1
            if idx < RECENT_WINDOW:
                recent_tag_counts[tag] = recent_tag_counts.get(tag, 0) + 1

        eq_metrics = _equity_curve_metrics(pnl)

        stats = {
            "trades": len(pnl),
            "wins": len(wins),
            "losses": len(losses),
            "avg_profit": _safe_mean(wins),
            "avg_loss": _safe_mean(losses),
            "win_rate": (len(wins) / len(pnl)) if pnl else 0.0,
            "pnl_mean": mu,
            "pnl_median": median_val,
            "pnl_std": std,
            "pnl_downside_dev": ddev,
            "recent_win_rate": (len(r_wins) / len(recent)) if recent else 0.0,
            "recent_avg_profit": _safe_mean(r_wins),
            "recent_avg_loss": _safe_mean(r_losses),
            "kelly_suggestion": kelly_raw,
            "recent_kelly_suggestion": recent_kelly,
            "kelly_confidence": k_conf,
            "tag_counts": tag_counts,
            "recent_tag_counts": recent_tag_counts,
            "sharpe_like": sharpe,
            "recent_sharpe_like": rsharpe,
            "equity_cum_return_pct": eq_metrics["equity_cum_return_pct"],
            "equity_max_dd_pct": eq_metrics["equity_max_dd_pct"],
        }

        await _store_stats(redis, stats)

    except Exception as e:  # pragma: no cover
        logger.debug("_recompute_and_store_stats skipped: %s", e)


async def get_perf_snapshot(redis=None) -> Dict[str, Any]:
    """
    Lightweight snapshot for dashboards, diagnostics & prompts.
    """
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


async def process_trade_closure(
    token_address: str,
    buy_price: float,
    sell_price: float,
    amount: float,
    partial: bool,
    timestamp: str,
    *,
    entry_score: Optional[float] = None,
    hold_minutes: Optional[float] = None,
) -> None:
    """
    Record a trade closure (full or partial) and refresh stats.

    All P&L is tracked in *percentage* terms, to keep things model-agnostic.
    Extra metadata (entry_score, hold_minutes) is stored for smarter LLM prompts
    and future quant research.
    """
    try:
        if buy_price <= 0 or amount <= 0:
            return

        profit_pct = ((sell_price - buy_price) / buy_price) * 100.0
        rec: Dict[str, Any] = {
            "token": token_address,
            "profit_pct": float(profit_pct),
            "amount": float(amount),
            "partial": bool(partial),
            "timestamp": timestamp,
        }
        if entry_score is not None:
            rec["entry_score"] = float(entry_score)
        if hold_minutes is not None:
            rec["hold_minutes"] = float(hold_minutes)

        rec["tag"] = _classify_trade(profit_pct, hold_minutes)

        # Persist trade + recompute stats
        redis = await _get_redis()
        if redis:
            await redis.lpush(TRADES_KEY, json.dumps(rec))
            await redis.ltrim(TRADES_KEY, 0, MAX_TRADES_KEEP - 1)
            await _recompute_and_store_stats(redis)
        else:
            _mem_trades.insert(0, rec)
            del _mem_trades[MAX_TRADES_KEEP:]
            await _recompute_and_store_stats(None)

        # Optional structured log event
        try:
            await log_event(
                "trade_closed",
                {
                    "token": token_address,
                    "profit_pct": profit_pct,
                    "amount": amount,
                    "partial": partial,
                    "timestamp": timestamp,
                    "entry_score": entry_score,
                    "hold_minutes": hold_minutes,
                    "tag": rec["tag"],
                },
            )
        except Exception:
            pass

    except Exception as e:  # pragma: no cover
        logger.warning("process_trade_closure failed: %s", e)


# =============================================================================
# Risk context coupling (risk_manager → strategy)
# =============================================================================

async def _read_risk_ctx(redis) -> Dict[str, Any]:
    """
    Pull risk:ctx from risk_manager if present; otherwise best-effort defaults.

    EXPECTED UNITS (V3+++):
      • daily_realized_pnl_pct: +10.0 means +10% realized today
      • daily_drawdown_pct:    -8.0 means -8% from today's peak

    Expected fields (but we guard against missing ones):

      Core:
        risk_capacity          (0..1)
        risk_mode              (0..3)
        daily_realized_pnl_pct (percent)
        daily_drawdown_pct     (percent; negative == drawdown)
        open_positions
        max_positions
        equity_usd

      Extra fields (if risk_manager publishes them):
        risk_profile
        max_risk_per_trade
        min_sol_balance
        plan_equity_today_usd, plan_gap_pct, plan_status
        ai_capacity_delta
        hard_halt (bool-ish, if risk API decides everything must stop)

      Optional regime-aware extras (future use):
        regime_label_numeric, regime_stress_score,
        regime_signal_strength, regime_entry_gate_factor
    """
    base: Dict[str, Any] = {
        "risk_capacity": 1.0,
        "risk_mode": 1,
        "daily_realized_pnl_pct": 0.0,   # percent (+/-)
        "daily_drawdown_pct": 0.0,       # percent (negative == DD)
        "open_positions": 0,
        "max_positions": 0,
        "equity_usd": 0.0,
        "regime_label_numeric": 0.0,
        "regime_stress_score": 0.0,
        "regime_signal_strength": 0.0,
        "regime_entry_gate_factor": 1.0,
        "plan_equity_today_usd": 0.0,
        "plan_gap_pct": 0.0,
        "plan_status": "n/a",
        "ai_capacity_delta": 0.0,
        "risk_profile": "balanced",
        "max_risk_per_trade": 0.0,
        "min_sol_balance": 0.0,
        "hard_halt": False,
    }
    if not redis:
        return base
    try:
        h = await redis.hgetall("risk:ctx")
        if not h:
            return base

        def _f(name: str, default: float = 0.0) -> float:
            try:
                return float(h.get(name, default))
            except Exception:
                return default

        base["risk_capacity"] = _f("risk_capacity", 1.0)
        base["risk_mode"] = int(float(h.get("risk_mode", 1))) if "risk_mode" in h else 1
        base["open_positions"] = int(float(h.get("open_positions", 0))) if "open_positions" in h else 0
        base["max_positions"] = int(float(h.get("max_positions", 0))) if "max_positions" in h else 0
        base["equity_usd"] = _f("equity_usd", 0.0)

        # Daily PnL / DD already in percent space (V3+++ risk_manager)
        base["daily_realized_pnl_pct"] = _f("daily_realized_pnl_pct", 0.0)
        base["daily_drawdown_pct"] = _f("daily_drawdown_pct", 0.0)

        # Regime extras if risk_manager / regime_state choose to publish them
        base["regime_label_numeric"] = _f("regime_label_numeric", 0.0)
        base["regime_stress_score"] = _f("regime_stress_score", 0.0)
        base["regime_signal_strength"] = _f("regime_signal_strength", 0.0)
        base["regime_entry_gate_factor"] = _f("regime_entry_gate_factor", 1.0)

        # Plan / AI overlay
        base["plan_equity_today_usd"] = _f("plan_equity_today_usd", 0.0)
        base["plan_gap_pct"] = _f("plan_gap_pct", 0.0)
        base["ai_capacity_delta"] = _f("ai_capacity_delta", 0.0)
        base["plan_status"] = h.get("plan_status", "n/a")

        if "risk_profile" in h:
            base["risk_profile"] = str(h.get("risk_profile"))

        # Explicit risk API ceilings (if provided)
        base["max_risk_per_trade"] = _f("max_risk_per_trade", 0.0)
        base["min_sol_balance"] = _f("min_sol_balance", 0.0)

        # Hard kill-switch from risk API (e.g. if DB / infra is stressed)
        if "hard_halt" in h:
            try:
                hv = h.get("hard_halt")
                if isinstance(hv, str):
                    base["hard_halt"] = hv.strip().lower() in ("1", "true", "yes", "on")
                else:
                    base["hard_halt"] = bool(hv)
            except Exception:
                base["hard_halt"] = False

        return base
    except Exception:  # pragma: no cover
        return base


# =============================================================================
# Tuning logic (rules + equity plan + optional LLM)
# =============================================================================

def _rule_based_adjust(
    params: Dict[str, Any],
    stats: Dict[str, Any],
    risk_ctx: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Deterministic, bounded adjustments based on stats & risk context.

    Rough heuristics:
      - Low winrate / big downside → tighten stops, reduce TP, reduce reinvest.
      - Strong winrate & stable → loosen a little, increase reinvest.
      - If capacity or risk_mode from risk_manager is low → defensive tweaks.
      - Kelly suggestion nudges max risk per trade.
      - Regime input from risk_ctx further biases defensive/aggressive stance.
      - STRAT_MODE (0/1/2) summarizes the overall stance for downstream modules.
      - In extreme bad health, we may toggle HARD_HALT for risk_manager.
      - ENTRY_MAX_PARALLEL is scaled to risk_capacity & regime.
    """
    out = dict(params)

    win_rate = float(stats.get("win_rate", 0.0))
    recent_wr = float(stats.get("recent_win_rate", 0.0))
    avg_profit = float(stats.get("avg_profit", 0.0))
    ddev = float(stats.get("pnl_downside_dev", 0.0))
    kelly = float(stats.get("kelly_suggestion", 0.02))
    sharpe = float(stats.get("sharpe_like", 0.0))
    recent_sharpe = float(stats.get("recent_sharpe_like", 0.0))
    equity_max_dd = float(stats.get("equity_max_dd_pct", 0.0))

    risk_capacity = float(risk_ctx.get("risk_capacity", 1.0))
    risk_mode = int(risk_ctx.get("risk_mode", 1))
    daily_pnl_pct = float(risk_ctx.get("daily_realized_pnl_pct", 0.0))
    daily_dd_pct = float(risk_ctx.get("daily_drawdown_pct", 0.0))
    open_positions = int(risk_ctx.get("open_positions", 0))
    max_positions = int(risk_ctx.get("max_positions", 0))

    # Regime-aware signals (if available)
    regime_label = float(risk_ctx.get("regime_label_numeric", 0.0))      # ~[-1..+1]
    regime_stress = float(risk_ctx.get("regime_stress_score", 0.0))      # 0..1
    regime_signal = float(risk_ctx.get("regime_signal_strength", 0.0))   # 0..1
    regime_gate = float(risk_ctx.get("regime_entry_gate_factor", 1.0))   # ~0.2..1.5

    # Plan context & AI overlay
    plan_gap_pct = float(risk_ctx.get("plan_gap_pct", 0.0))
    ai_capacity_delta = float(risk_ctx.get("ai_capacity_delta", 0.0))

    # Base mode
    mode = out.get("STRAT_MODE", DEFAULTS["STRAT_MODE"])

    # Reset HARD_HALT by default; we re-assert it when conditions are awful
    out["HARD_HALT"] = bool(out.get("HARD_HALT", DEFAULTS["HARD_HALT"]))

    # --- 0) If risk API says HARD HALT, we respect it and exit early ---
    if bool(risk_ctx.get("hard_halt", False)) or risk_capacity <= 0.0:
        out["HARD_HALT"] = True
        out["STRAT_MODE"] = 0
        return _sanitize_params(out)

    # --- 1) Super-catastrophic safety: health-based HARD_HALT ---
    if (
        win_rate < HEALTH_HARD_HALT_WR
        and sharpe < HEALTH_HARD_HALT_SHARPE
        and risk_capacity < 0.40
    ):
        out["HARD_HALT"] = True
        mode = 0  # force DEFENSIVE

    # --- 2) Losing / unstable regimes → defensive ---
    losing_regime = (
        win_rate < 0.45
        or recent_wr < 0.40
        or ddev > 15.0
        or daily_dd_pct <= -8.0
        or recent_sharpe < 0.0
        or regime_stress > 0.7
        or regime_label < -0.4
        or equity_max_dd <= -25.0
    )
    if losing_regime:
        mode = 0  # DEFENSIVE

        # tighter SL
        out["STOP_LOSS_PERCENT"] = max(
            BOUNDS["STOP_LOSS_PERCENT"][0],
            out["STOP_LOSS_PERCENT"] - 1.5,
        )
        # slightly tighter slippage (cheaper fills)
        out["SLIPPAGE"] = max(
            BOUNDS["SLIPPAGE"][0],
            out["SLIPPAGE"] - 0.0005,
        )
        # bring TPs closer if they are huge
        if out.get("TARGET_PROFIT_PERCENT", 0.0) > 80:
            out["TARGET_PROFIT_PERCENT"] = max(
                60.0, out["TARGET_PROFIT_PERCENT"] - 15.0
            )
        # less risk per trade + lower reinvestment
        out["MAX_RISK_PER_TRADE"] = max(
            BOUNDS["MAX_RISK_PER_TRADE"][0],
            out["MAX_RISK_PER_TRADE"] - 0.005,
        )
        out["REINVEST_PERCENT"] = max(
            BOUNDS["REINVEST_PERCENT"][0],
            out["REINVEST_PERCENT"] - 5.0,
        )
        # ask entry logic to be pickier
        out["ENTRY_SCORE_GATE"] = min(
            BOUNDS["ENTRY_SCORE_GATE"][1],
            out["ENTRY_SCORE_GATE"] + 0.01,
        )
        # reduce concurrency
        out["ENTRY_MAX_PARALLEL"] = max(
            BOUNDS["ENTRY_MAX_PARALLEL"][0],
            out["ENTRY_MAX_PARALLEL"] - 1,
        )
        out["ENTRY_RISK_BOOST_FACTOR"] = max(
            BOUNDS["ENTRY_RISK_BOOST_FACTOR"][0],
            out["ENTRY_RISK_BOOST_FACTOR"] - 0.05,
        )

    # Hard defensive if deep drawdown / tiny capacity / awful regime
    extreme_bad = (
        daily_dd_pct <= -DEFENSIVE_DD_THRESHOLD
        or risk_capacity < 0.35
        or regime_label <= -0.7
        or sharpe < -0.3
        or equity_max_dd <= -35.0
        or daily_pnl_pct <= -15.0
    )
    if extreme_bad:
        mode = 0
        out["MAX_RISK_PER_TRADE"] = max(
            BOUNDS["MAX_RISK_PER_TRADE"][0],
            out["MAX_RISK_PER_TRADE"] - 0.01,
        )
        out["REINVEST_PERCENT"] = max(
            BOUNDS["REINVEST_PERCENT"][0],
            out["REINVEST_PERCENT"] - 8.0,
        )
        out["ENTRY_SCORE_GATE"] = min(
            BOUNDS["ENTRY_SCORE_GATE"][1],
            out["ENTRY_SCORE_GATE"] + 0.02,
        )
        out["ENTRY_MAX_PARALLEL"] = max(
            BOUNDS["ENTRY_MAX_PARALLEL"][0],
            out["ENTRY_MAX_PARALLEL"] - 2,
        )
        out["ENTRY_RISK_BOOST_FACTOR"] = max(
            BOUNDS["ENTRY_RISK_BOOST_FACTOR"][0],
            out["ENTRY_RISK_BOOST_FACTOR"] - 0.1,
        )
        # In really nasty conditions, flip HARD_HALT so risk_manager
        # hard-blocks new entries even if capacity isn't 0 yet.
        if (
            daily_dd_pct <= -2 * DEFENSIVE_DD_THRESHOLD
            or win_rate < 0.3
            or recent_wr < 0.25
        ):
            out["HARD_HALT"] = True

    # --- 3) Crushing it + good regime → gentle scaling ---
    strong_regime = (
        win_rate > AGGRESSIVE_MIN_WINRATE
        and avg_profit > AGGRESSIVE_MIN_MEAN
        and ddev < 12.0
        and daily_dd_pct > -5.0
        and sharpe > 0.3
        and regime_label > 0.3
        and regime_signal > 0.4
        and regime_stress < 0.5
    )
    # Plan context: if massively ahead of plan, bias slightly DOWN even if strong.
    if plan_gap_pct > 10.0:
        strong_regime = False

    if strong_regime and not losing_regime and risk_capacity > 0.75:
        mode = 2  # AGGRESSIVE
        out["STOP_LOSS_PERCENT"] = min(
            BOUNDS["STOP_LOSS_PERCENT"][1],
            out["STOP_LOSS_PERCENT"] + 0.5,
        )
        out["REINVEST_PERCENT"] = min(
            BOUNDS["REINVEST_PERCENT"][1],
            out["REINVEST_PERCENT"] + 3.0,
        )
        out["SLIPPAGE"] = min(
            BOUNDS["SLIPPAGE"][1],
            out["SLIPPAGE"] + 0.0002,
        )
        out["ENTRY_SCORE_GATE"] = max(
            BOUNDS["ENTRY_SCORE_GATE"][0],
            out["ENTRY_SCORE_GATE"] - 0.005,
        )
        out["ENTRY_MAX_PARALLEL"] = min(
            BOUNDS["ENTRY_MAX_PARALLEL"][1],
            out["ENTRY_MAX_PARALLEL"] + 1,
        )
        out["ENTRY_RISK_BOOST_FACTOR"] = min(
            BOUNDS["ENTRY_RISK_BOOST_FACTOR"][1],
            out["ENTRY_RISK_BOOST_FACTOR"] + 0.05,
        )
        # If conditions are great, make sure HARD_HALT is off
        out["HARD_HALT"] = False

    # --- 4) If mean pnl is weak, shorten max hold a bit ---
    if stats.get("pnl_mean", 0.0) < 5.0 and out["MAX_HOLD_TIME_MINUTES"] > 45:
        out["MAX_HOLD_TIME_MINUTES"] = max(
            45, out["MAX_HOLD_TIME_MINUTES"] - 30
        )

    # --- 5) Kelly + risk_capacity + regime → target risk per trade ---
    if ENABLE_KELLY:
        target_risk = max(0.01, min(kelly * 0.7, 0.10))  # 70% of Kelly, clipped

        # adjust for capacity and explicit risk_mode
        if risk_capacity < 0.4 or risk_mode in (0, 3) or daily_dd_pct <= -6.0:
            target_risk *= 0.6

        # regime-aware tweak: bad regime → lower, great regime → slightly higher
        if regime_label < -0.2 or regime_stress > 0.6:
            target_risk *= 0.7
        elif regime_label > 0.4 and regime_signal > 0.4 and regime_stress < 0.4:
            target_risk *= 1.1

        # if AI capacity delta is strongly negative, be a bit more cautious
        if ai_capacity_delta < -0.1:
            target_risk *= 0.8
        elif ai_capacity_delta > 0.1:
            target_risk *= 1.05

        # avoid crowding: if at max positions, do not increase risk
        if max_positions > 0 and open_positions >= max_positions:
            target_risk = min(target_risk, out["MAX_RISK_PER_TRADE"])

        # Move 10% of the way toward target to avoid jumps
        cur = out["MAX_RISK_PER_TRADE"]
        out["MAX_RISK_PER_TRADE"] = cur + 0.1 * (target_risk - cur)

    # --- 6) Slightly align ENTRY_SCORE_GATE with regime gate factor ---
    if 0.1 <= regime_gate <= 2.0:
        # Map gate >1 → slightly lower threshold, gate <1 → higher threshold
        desired_gate = out["ENTRY_SCORE_GATE"]
        if regime_gate > 1.05:
            desired_gate = max(
                BOUNDS["ENTRY_SCORE_GATE"][0],
                desired_gate - 0.01 * (regime_gate - 1.0),
            )
        elif regime_gate < 0.95:
            desired_gate = min(
                BOUNDS["ENTRY_SCORE_GATE"][1],
                desired_gate + 0.01 * (1.0 - regime_gate),
            )
        # Move 15% toward the desired gate
        cur_gate = out["ENTRY_SCORE_GATE"]
        out["ENTRY_SCORE_GATE"] = max(
            BOUNDS["ENTRY_SCORE_GATE"][0],
            min(
                BOUNDS["ENTRY_SCORE_GATE"][1],
                cur_gate + 0.15 * (desired_gate - cur_gate),
            ),
        )

    # --- 7) Gentle HARD_HALT auto-release when health recovers ---
    if out.get("HARD_HALT"):
        # If we were halted and things look materially better, allow re-enable.
        if (
            risk_capacity > 0.55
            and daily_dd_pct > -DEFENSIVE_DD_THRESHOLD / 2.0
            and recent_wr > 0.48
            and recent_sharpe > 0.0
        ):
            out["HARD_HALT"] = False

    # Update mode last so sanitize clamps it to {0,1,2}
    out["STRAT_MODE"] = mode

    return _sanitize_params(out)


def _enforce_risk_api_contract(params: Dict[str, Any], risk_ctx: Dict[str, Any]) -> Dict[str, Any]:
    """
    Final "sanity pass" against the risk API contract.

    Anything that comes out of this function is guaranteed to:
      • respect risk_manager ceilings (if risk:ctx publishes them)
      • never relax MIN_SOL_BALANCE below the risk API floor
      • optionally hard-halt if the risk API demands it

    This is intentionally conservative and runs *after* rule-based / LLM tuning.
    """
    if not RESPECT_RISK_API_LIMITS:
        return _sanitize_params(params)

    out = dict(params)

    # Respect explicit max_risk_per_trade if risk API publishes it
    try:
        api_max_risk = float(risk_ctx.get("max_risk_per_trade", 0.0))
    except Exception:
        api_max_risk = 0.0
    if api_max_risk > 0.0:
        lo, hi = BOUNDS["MAX_RISK_PER_TRADE"]
        # Clamp to both the strategy bound and the risk API ceiling
        out["MAX_RISK_PER_TRADE"] = max(
            lo,
            min(float(out.get("MAX_RISK_PER_TRADE", DEFAULTS["MAX_RISK_PER_TRADE"])), min(hi, api_max_risk)),
        )

    # Respect explicit min_sol_balance floor if risk API publishes it
    try:
        api_min_sol = float(risk_ctx.get("min_sol_balance", 0.0))
    except Exception:
        api_min_sol = 0.0
    if api_min_sol > 0.0:
        lo, hi = BOUNDS["MIN_SOL_BALANCE"]
        out["MIN_SOL_BALANCE"] = max(
            lo,
            min(max(float(out.get("MIN_SOL_BALANCE", DEFAULTS["MIN_SOL_BALANCE"])), api_min_sol), hi),
        )

    # If risk API advertises hard_halt, we *always* mirror it here
    if bool(risk_ctx.get("hard_halt", False)):
        out["HARD_HALT"] = True
        out["STRAT_MODE"] = 0

    return _sanitize_params(out)


def _update_equity_plan(
    params: Dict[str, Any],
    risk_ctx: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Maintain PLAN_* fields used by risk_manager for plan-aware risk capacity.

    We keep a simple continuous-compounding equity curve:

        target_equity(t) = PLAN_START_EQUITY_USD * (1 + APR) ** (days/365)

    and track today's PLAN_EQUITY_TODAY_USD, PLAN_GAP_PCT and PLAN_STATUS.
    """
    out = dict(params)
    if not PLAN_ENABLED:
        # Keep PLAN_* neutral if the plan system is disabled.
        out["PLAN_EQUITY_TODAY_USD"] = float(risk_ctx.get("equity_usd", 0.0))
        out["PLAN_GAP_PCT"] = 0.0
        out["PLAN_STATUS"] = "plan_disabled"
        return out

    equity = float(risk_ctx.get("equity_usd", 0.0))
    apr = float(out.get("PLAN_TARGET_APR_PCT", DEFAULTS["PLAN_TARGET_APR_PCT"]))
    start_eq = float(out.get("PLAN_START_EQUITY_USD", 0.0))
    start_ts = float(out.get("PLAN_START_TS", 0.0))

    now_ts = time.time()

    # Initialize plan on first run once we see a sane equity
    if equity > 0 and (start_eq <= 0 or start_ts <= 0):
        start_eq = equity
        start_ts = now_ts
        out["PLAN_START_EQUITY_USD"] = start_eq
        out["PLAN_START_TS"] = start_ts

    # If equity or plan start not meaningful, just mirror equity
    if equity <= 0 or start_eq <= 0 or start_ts <= 0:
        out["PLAN_EQUITY_TODAY_USD"] = equity
        out["PLAN_GAP_PCT"] = 0.0
        out["PLAN_STATUS"] = "unknown"
        return out

    days = max(0.0, (now_ts - start_ts) / 86400.0)
    growth = max(0.0, 1.0 + apr / 100.0)
    # "APR" as simple annual compounding
    target_equity = start_eq * (growth ** (days / 365.0))

    gap_pct = 0.0
    if target_equity > 1e-9:
        gap_pct = (equity - target_equity) / target_equity * 100.0

    if gap_pct > 3.0:
        status = "ahead"
    elif gap_pct < -3.0:
        status = "behind"
    else:
        status = "on_track"

    out["PLAN_EQUITY_TODAY_USD"] = float(equity)
    out["PLAN_GAP_PCT"] = float(gap_pct)
    out["PLAN_STATUS"] = status

    return out


async def _maybe_llm_adjust(
    params: Dict[str, Any],
    stats: Dict[str, Any],
    risk_ctx: Dict[str, Any],
    redis,
) -> Dict[str, Any]:
    """
    Optional OpenAI refinement. Must ALWAYS stay within BOUNDS.

    LLM is not allowed to touch:
      • STRAT_MODE
      • boolean flags (TRAILING_STOP_ENABLED, HARD_HALT, etc.)
      • unknown keys
      • plan identity fields (PLAN_START_*, PLAN_EQUITY_TODAY_USD, PLAN_GAP_PCT, PLAN_STATUS)
    """
    global _mem_last_llm_ts

    if not _ENABLE_AI_TUNING or AsyncOpenAI is None:
        return params

    try:
        now = time.time()
        if redis:
            raw = await redis.get(LAST_LLM_TS_KEY)
            last_ts = float(raw) if raw else 0.0
        else:
            last_ts = _mem_last_llm_ts

        if now - last_ts < LLM_MIN_INTERVAL_SECONDS:
            return params

        if int(stats.get("trades", 0)) < max(MIN_TRADES_FOR_TUNING, 8):
            return params

        client = AsyncOpenAI()
        prompt = (
            "You are an expert systematic crypto execution & risk optimizer for "
            "a Solana microcap sniper bot.\n"
            "Given the CURRENT strategy parameters, performance stats, and risk "
            "context (including macro regime hints), propose SMALL numeric "
            "adjustments to a subset of params.\n\n"
            "Requirements:\n"
            "  • Only touch keys that already exist in `params`.\n"
            "  • NEVER modify 'STRAT_MODE', boolean flags, or PLAN_* fields.\n"
            "  • Keep all values inside the following bounds (inclusive):\n"
            f"    {json.dumps(BOUNDS, indent=2)}\n"
            "  • Prefer very gradual changes; this runs periodically.\n"
            "  • Optimize for higher risk-adjusted return (Sharpe), not raw PnL.\n"
            "  • NEVER add new keys or return prose.\n\n"
            "Return ONLY a JSON object of {param_name: new_value} with NO prose.\n\n"
            f"params = {json.dumps(params, indent=2)}\n"
            f"stats = {json.dumps(stats, indent=2)}\n"
            f"risk_ctx = {json.dumps(risk_ctx, indent=2)}\n"
        )

        resp = await client.chat.completions.create(
            model=_OPENAI_MODEL,
            messages=[{"role": "user", "content": prompt}],
            temperature=0.15,
            max_tokens=350,
        )
        text = (resp.choices[0].message.content or "").strip()

        try:
            proposed = json.loads(text)
        except Exception:
            return params

        # Only keep known keys and clamp to bounds,
        # and optionally enforce relative move cap.
        filtered: Dict[str, Any] = {}
        for k, v in proposed.items():
            if k not in DEFAULTS:
                continue
            if k == "STRAT_MODE":
                continue  # reserved regime flag
            if k.startswith("PLAN_"):
                continue  # plan fields are derived, not LLM-written
            if isinstance(DEFAULTS[k], bool):
                continue  # do not let the LLM touch booleans
            try:
                v_f = float(v)
            except Exception:
                continue
            lo, hi = BOUNDS.get(k, (v_f, v_f))
            # cap relative move vs current params
            cur_val = float(params.get(k, DEFAULTS[k]))
            if cur_val != 0 and LLM_MAX_REL_MOVE > 0:
                max_up = cur_val * (1.0 + LLM_MAX_REL_MOVE)
                max_dn = cur_val * (1.0 - LLM_MAX_REL_MOVE)
                v_f = max(min(v_f, max_up), max_dn)
            filtered[k] = float(min(max(v_f, lo), hi))

        if not filtered:
            return params

        merged = _sanitize_params({**params, **filtered})

        if redis:
            await redis.set(LAST_LLM_TS_KEY, str(now))
        else:
            _mem_last_llm_ts = now

        return merged

    except Exception as e:  # pragma: no cover
        logger.debug("LLM tuning skipped: %s", e)
        return params


# =============================================================================
# Param history / health
# =============================================================================

async def _record_param_history(
    redis,
    diffs: Dict[str, Tuple[Any, Any]],
    stats: Dict[str, Any],
    risk_ctx: Dict[str, Any],
) -> None:
    """
    Store compact tuning snapshot for debugging / backtesting insights.
    """
    if not diffs:
        return

    entry = {
        "ts": time.time(),
        "diffs": diffs,
        "trades": int(stats.get("trades", 0)),
        "win_rate": float(stats.get("win_rate", 0.0)),
        "pnl_mean": float(stats.get("pnl_mean", 0.0)),
        "pnl_std": float(stats.get("pnl_std", 0.0)),
        "sharpe_like": float(stats.get("sharpe_like", 0.0)),
        "recent_sharpe_like": float(stats.get("recent_sharpe_like", 0.0)),
        "kelly_suggestion": float(stats.get("kelly_suggestion", 0.02)),
        "kelly_confidence": float(stats.get("kelly_confidence", 0.0)),
        "risk_capacity": float(risk_ctx.get("risk_capacity", 1.0)),
        "risk_mode": int(risk_ctx.get("risk_mode", 1)),
        "regime_label_numeric": float(risk_ctx.get("regime_label_numeric", 0.0)),
        "regime_stress_score": float(risk_ctx.get("regime_stress_score", 0.0)),
        "regime_signal_strength": float(risk_ctx.get("regime_signal_strength", 0.0)),
        "equity_usd": float(risk_ctx.get("equity_usd", 0.0)),
        "daily_realized_pnl_pct": float(
            risk_ctx.get("daily_realized_pnl_pct", 0.0)
        ),
        "daily_drawdown_pct": float(risk_ctx.get("daily_drawdown_pct", 0.0)),
    }

    try:
        if redis:
            await redis.lpush(PARAM_HISTORY_KEY, json.dumps(entry))
            await redis.ltrim(PARAM_HISTORY_KEY, 0, MAX_PARAM_HISTORY_KEEP - 1)
    except Exception:  # pragma: no cover
        pass


def _health_score(stats: Dict[str, Any], risk_ctx: Dict[str, Any]) -> float:
    """
    Compressed 0..100 health score for dashboards / sanity checks.
    """
    wr = float(stats.get("win_rate", 0.0))
    sharpe = float(stats.get("sharpe_like", 0.0))
    dd = float(risk_ctx.get("daily_drawdown_pct", 0.0))  # negative when bad
    capacity = float(risk_ctx.get("risk_capacity", 1.0))
    eq_max_dd = float(stats.get("equity_max_dd_pct", 0.0))

    # Map each piece to 0..100 then average
    # winrate: 30% → 0, 70% → 100
    wr_score = max(0.0, min(1.0, (wr - 0.30) / (0.70 - 0.30))) * 100.0
    # sharpe: -0.5 → 0, 1.5 → 100
    s_clamped = max(-0.5, min(1.5, sharpe))
    sharpe_score = (s_clamped + 0.5) / 2.0 * 100.0
    # drawdown: -30% → 0, 0% → 100
    dd_score = max(0.0, min(1.0, (30.0 + dd) / 30.0)) * 100.0
    # capacity directly 0..1 → 0..100
    cap_score = max(0.0, min(1.0, capacity)) * 100.0
    # equity max DD: -50% → 0, 0% → 100
    eq_dd_score = max(0.0, min(1.0, (50.0 + eq_max_dd) / 50.0)) * 100.0

    return float(
        0.30 * wr_score
        + 0.25 * sharpe_score
        + 0.20 * dd_score
        + 0.15 * cap_score
        + 0.10 * eq_dd_score
    )


async def _update_health(redis, stats: Dict[str, Any], risk_ctx: Dict[str, Any]) -> None:
    """
    High-level health snapshot; useful for dashboards.
    """
    global _mem_last_health_alert_ts

    health_score = _health_score(stats, risk_ctx)

    health: Dict[str, Any] = {
        "ts": time.time(),
        "trades": int(stats.get("trades", 0)),
        "win_rate": float(stats.get("win_rate", 0.0)),
        "recent_win_rate": float(stats.get("recent_win_rate", 0.0)),
        "pnl_mean": float(stats.get("pnl_mean", 0.0)),
        "pnl_median": float(stats.get("pnl_median", 0.0)),
        "pnl_std": float(stats.get("pnl_std", 0.0)),
        "pnl_downside_dev": float(stats.get("pnl_downside_dev", 0.0)),
        "sharpe_like": float(stats.get("sharpe_like", 0.0)),
        "recent_sharpe_like": float(stats.get("recent_sharpe_like", 0.0)),
        "kelly_suggestion": float(stats.get("kelly_suggestion", 0.02)),
        "recent_kelly_suggestion": float(stats.get("recent_kelly_suggestion", 0.02)),
        "kelly_confidence": float(stats.get("kelly_confidence", 0.0)),
        "equity_cum_return_pct": float(stats.get("equity_cum_return_pct", 0.0)),
        "equity_max_dd_pct": float(stats.get("equity_max_dd_pct", 0.0)),
        "risk_capacity": float(risk_ctx.get("risk_capacity", 1.0)),
        "risk_mode": int(risk_ctx.get("risk_mode", 1)),
        "daily_realized_pnl_pct": float(risk_ctx.get("daily_realized_pnl_pct", 0.0)),
        "daily_drawdown_pct": float(risk_ctx.get("daily_drawdown_pct", 0.0)),
        "equity_usd": float(risk_ctx.get("equity_usd", 0.0)),
        "regime_label_numeric": float(risk_ctx.get("regime_label_numeric", 0.0)),
        "regime_stress_score": float(risk_ctx.get("regime_stress_score", 0.0)),
        "regime_signal_strength": float(risk_ctx.get("regime_signal_strength", 0.0)),
        "regime_entry_gate_factor": float(
            risk_ctx.get("regime_entry_gate_factor", 1.0)
        ),
        "tag_counts": stats.get("tag_counts", {}),
        "recent_tag_counts": stats.get("recent_tag_counts", {}),
        "health_score": health_score,
    }

    # Also include latest PLAN_* from params if available
    try:
        params = await get_strategy_params(redis)
        health["plan_target_apr_pct"] = float(
            params.get("PLAN_TARGET_APR_PCT", DEFAULTS["PLAN_TARGET_APR_PCT"])
        )
        health["plan_equity_today_usd"] = float(
            params.get("PLAN_EQUITY_TODAY_USD", 0.0)
        )
        health["plan_gap_pct"] = float(params.get("PLAN_GAP_PCT", 0.0))
        health["plan_status"] = str(params.get("PLAN_STATUS", "n/a"))
        health["hard_halt"] = bool(params.get("HARD_HALT", False))
        health["strat_mode"] = int(params.get("STRAT_MODE", DEFAULTS["STRAT_MODE"]))
        health["entry_max_parallel"] = int(
            params.get("ENTRY_MAX_PARALLEL", DEFAULTS["ENTRY_MAX_PARALLEL"])
        )
        health["entry_score_gate"] = float(
            params.get("ENTRY_SCORE_GATE", DEFAULTS["ENTRY_SCORE_GATE"])
        )
    except Exception:
        pass

    try:
        if redis:
            await redis.set(HEALTH_KEY, json.dumps(health))
        # optional event for dashboards
        try:
            await log_event("strategy_health", health)
        except Exception:
            pass
    except Exception:  # pragma: no cover
        pass

    # Optional TG alert if health very low (without spamming)
    try:
        now = time.time()
        if (
            health_score <= HEALTH_ALERT_MIN_SCORE
            and now - _mem_last_health_alert_ts >= HEALTH_ALERT_INTERVAL_SEC
        ):
            _mem_last_health_alert_ts = now
            await send_telegram(
                "⚠️ Strategy health low: "
                f"score={health_score:.1f}  "
                f"WR={health['win_rate']:.1%}  "
                f"Sharpe≈{health['sharpe_like']:+.2f}  "
                f"DD={health['daily_drawdown_pct']:+.2f}%  "
                f"eqMaxDD={health['equity_max_dd_pct']:+.2f}%"
            )
    except Exception:
        pass


async def get_health_snapshot(redis=None) -> Dict[str, Any]:
    """
    Public helper so dashboards / diagnostics can read the last health snapshot.
    """
    try:
        if redis is None:
            redis = await _get_redis()
        if redis:
            raw = await redis.get(HEALTH_KEY)
            if raw:
                return json.loads(raw)
        return {}
    except Exception:
        return {}


async def get_plan_snapshot(redis=None) -> Dict[str, Any]:
    """
    Small helper focused just on PLAN_* + equity, for dashboards / tools.
    """
    params = await get_strategy_params(redis)
    return {
        "plan_target_apr_pct": float(
            params.get("PLAN_TARGET_APR_PCT", DEFAULTS["PLAN_TARGET_APR_PCT"])
        ),
        "plan_start_equity_usd": float(params.get("PLAN_START_EQUITY_USD", 0.0)),
        "plan_start_ts": float(params.get("PLAN_START_TS", 0.0)),
        "plan_equity_today_usd": float(params.get("PLAN_EQUITY_TODAY_USD", 0.0)),
        "plan_gap_pct": float(params.get("PLAN_GAP_PCT", 0.0)),
        "plan_status": str(params.get("PLAN_STATUS", "n/a")),
    }


async def get_param_history(redis=None, limit: int = 50) -> List[Dict[str, Any]]:
    """
    Read the most recent parameter-change history entries.
    """
    if limit <= 0:
        return []
    try:
        if redis is None:
            redis = await _get_redis()
        if not redis:
            return []
        raw = await redis.lrange(PARAM_HISTORY_KEY, 0, limit - 1)
        return [json.loads(x) for x in raw]
    except Exception:
        return []


async def get_strategy_with_risk(redis=None) -> Dict[str, Any]:
    """
    Convenience helper to give other AI modules a single call to get:
      • current strategy params (already sanitized)
      • current risk:ctx snapshot
      • last health snapshot (if any)

    This is intended for ai_entry_logic / ai_sell_logic / reinvestor so that
    every module "sees" the same risk API picture.
    """
    if redis is None:
        redis = await _get_redis()
    params = await get_strategy_params(redis)
    risk_ctx = await _read_risk_ctx(redis)
    health = await get_health_snapshot(redis)
    return {
        "params": params,
        "risk_ctx": risk_ctx,
        "health": health,
    }


# =============================================================================
# Apply & announce
# =============================================================================

async def _apply_and_announce(
    old: Dict[str, Any],
    new: Dict[str, Any],
    stats: Dict[str, Any],
    risk_ctx: Dict[str, Any],
    redis,
) -> None:
    """
    Persist params, write history + health, and announce via Telegram/Notion.
    """
    await set_strategy_params(redis, new)

    diffs = {
        k: (old.get(k), new.get(k))
        for k in sorted(new.keys())
        if old.get(k) != new.get(k)
    }
    if not diffs:
        # Still update health so dashboards stay fresh
        await _update_health(redis, stats, risk_ctx)
        return

    # --- history / health ---
    await _record_param_history(redis, diffs, stats, risk_ctx)
    await _update_health(redis, stats, risk_ctx)

    # Telegram summary
    try:
        mode = int(new.get("STRAT_MODE", 1))
        mode_label = {0: "DEFENSIVE", 1: "NEUTRAL", 2: "AGGRESSIVE"}.get(mode, "NEUTRAL")
        lines = [
            "🤖 Strategy auto-tuned (V3+++):",
            (
                f"Mode={mode_label}  Trades={stats.get('trades', 0)}  "
                f"WinRate={stats.get('win_rate', 0.0):.2%}  "
                f"μ={stats.get('pnl_mean', 0.0):+.2f}%  "
                f"σ={stats.get('pnl_std', 0.0):.2f}%  "
                f"Sharpe≈{stats.get('sharpe_like', 0.0):+.2f}"
            ),
            (
                f"Risk capacity={risk_ctx.get('risk_capacity', 1.0):.3f}  "
                f"mode={risk_ctx.get('risk_mode', 1)}  "
                f"dailyPnL={risk_ctx.get('daily_realized_pnl_pct', 0.0):+.2f}%  "
                f"DD={risk_ctx.get('daily_drawdown_pct', 0.0):+.2f}%"
            ),
            (
                f"Regime num={risk_ctx.get('regime_label_numeric', 0.0):+.2f}  "
                f"stress={risk_ctx.get('regime_stress_score', 0.0):.2f}  "
                f"signal={risk_ctx.get('regime_signal_strength', 0.0):.2f}"
            ),
            f"Kelly={stats.get('kelly_suggestion', 0.02):.3f}  "
            f"recent={stats.get('recent_kelly_suggestion', 0.02):.3f}  "
            f"conf={stats.get('kelly_confidence', 0.0):.2f}",
            "",
        ]
        for k, (a, b) in diffs.items():
            lines.append(f"• {k}: {a} → {b}")
        await send_telegram("\n".join(lines))
    except Exception:
        pass

    # Notion log (if wired that way)
    try:
        reason = "; ".join(f"{k}: {a}→{b}" for k, (a, b) in diffs.items())
        await log_to_notion(
            token="STRAT",
            symbol="Params Update",
            balance=float(stats.get("trades", 0)),
            entry=float(new.get("STOP_LOSS_PERCENT", 0.0)),
            current=float(new.get("REINVEST_PERCENT", 0.0)),
            profit=float(stats.get("win_rate", 0.0) * 100.0),
            reason=reason,
        )
    except Exception:
        pass

    # Structured log_event
    try:
        await log_event(
            "strategy_tuned",
            {
                "diffs": diffs,
                "stats": stats,
                "risk_ctx": risk_ctx,
            },
        )
    except Exception:
        pass


# =============================================================================
# Tuning entrypoints
# =============================================================================

async def tune_once(redis=None) -> None:
    """
    Single tuning step; safe to call from cron / loop.
    """
    global _mem_last_tune_ts

    if redis is None:
        redis = await _get_redis()

    now = time.time()
    try:
        last_ts = float(
            (await redis.get(LAST_TUNE_TS_KEY)) or 0
        ) if redis else _mem_last_tune_ts
    except Exception:
        last_ts = _mem_last_tune_ts

    # Guard: avoid hyper-frequency even if called often
    min_interval = max(LEARNING_INTERVAL_SECONDS * 0.5, 300)
    if now - last_ts < min_interval:
        return

    await _recompute_and_store_stats(redis)
    stats = await get_perf_snapshot(redis)
    cur = await get_strategy_params(redis)
    risk_ctx = await _read_risk_ctx(redis)

    # Need a minimal sample size to avoid noise-driven tuning
    if int(stats.get("trades", 0)) < 4:
        if redis:
            await redis.set(LAST_TUNE_TS_KEY, str(now))
        else:
            _mem_last_tune_ts = now
        await _update_health(redis, stats, risk_ctx)
        return

    try:
        await log_event("strategy_tune_attempt", {"stats": stats, "risk_ctx": risk_ctx})
    except Exception:
        pass

    # Rule-based core + optional LLM refinement
    new_params = _rule_based_adjust(cur, stats, risk_ctx)
    new_params = await _maybe_llm_adjust(new_params, stats, risk_ctx, redis)
    # Sanity-pass against the explicit risk API contract
    new_params = _enforce_risk_api_contract(new_params, risk_ctx)
    # Update equity plan fields from latest equity / APR
    new_params = _update_equity_plan(new_params, risk_ctx)

    await _apply_and_announce(cur, new_params, stats, risk_ctx, redis)

    try:
        if redis:
            await redis.set(LAST_TUNE_TS_KEY, str(now))
        else:
            _mem_last_tune_ts = now
    except Exception:
        pass


async def run_self_learning_loop(redis=None) -> None:
    """
    Long-running loop; safe to fire from scripts/main.py:

        from utils_pkg.self_learning_ai_strategy import run_self_learning_ai_strategy_loop
        ...
        asyncio.create_task(run_self_learning_loop(redis), name="self.learning")
    """
    if redis is None:
        redis = await _get_redis()
    logger.info(
        "🧠 Self-learning loop started (interval=%ss, LLM=%s)",
        LEARNING_INTERVAL_SECONDS,
        bool(_ENABLE_AI_TUNING and AsyncOpenAI is not None),
    )
    while True:
        try:
            await tune_once(redis)
        except Exception as e:  # pragma: no cover
            logger.error("[SELF_LEARNING] Tuning error: %s", e, exc_info=True)
        await asyncio.sleep(LEARNING_INTERVAL_SECONDS)


# =============================================================================
# CLI / manual test
# =============================================================================

if __name__ == "__main__":  # manual debugging
    async def _demo():
        r = await _get_redis()
        await tune_once(r)
        snap = await get_perf_snapshot(r)
        print("perf_snapshot:", json.dumps(snap, indent=2))

    asyncio.run(_demo())
