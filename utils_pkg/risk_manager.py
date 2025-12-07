from __future__ import annotations

"""
utils_pkg/risk_manager.py

Quant-grade portfolio risk manager (V3+++)
=========================================

High-level role
---------------
Central portfolio risk brain. Single source of truth for:

  • How big we trade
        - ticket size (USD)
        - per-position % of equity cap
        - max concurrent positions
        - daily spend cap (USD / % of equity)
        - optional global equity drawdown kill-switch
        - ticket min/max clamps for UX
        - Kelly-style safety clamp for per-trade risk
        - Equity utilization guardrail (ticket * concurrency vs equity)
        - Liquidity-aware ticket sizing (vs pool_depth)

  • When we are allowed to open new positions
        - risk_capacity ∈ [0,1]
        - risk_mode (0=OFF, 1=NORMAL, 2=AGGRESSIVE, 3=DEFENSIVE)
        - infra halt flags ("infra:halt")
        - daily profit / loss caps
        - daily USD spend budget
        - daily max trades cap
        - consecutive-loss cooldown windows
        - DD-based cooldown windows
        - SOL balance floor via self-learning MIN_SOL_BALANCE
        - strategy “plan awareness” (ahead/behind equity curve)
        - optional HARD_HALT from self-learning strategy
        - optional manual/ops-level block ("risk:manual_block")
        - regime_state hard risk-off flag
        - optional equity floor guardrail (RISK_MIN_EQUITY_USD)
        - optional sandbox mode (RISK_SANDBOX_MODE) for low-risk live testing
        - optional synthetic equity override (RISK_EQUITY_OVERRIDE_USD)
        - NEW: trading-window gating (UTC hours + allowed weekdays)

  • How we compress performance + regime + plan → capacity
        - winrate-20
        - rolling DD vs equity HWM
        - daily realized PnL vs daily target
        - regime_state risk multiplier (risk_off/normal/risk_on)
        - volatility / choppiness penalties
        - AI risk overlay (ai_risk_model)
        - regime_gate_factor from regime_state (extra gate)
        - optional equity kill-switch
        - Kelly-style sanity fraction for per-trade risk
        - plan bias (ahead-of-plan → shrink; behind → slight boost)

  • Publishing a lightweight risk:ctx hash for:
        - FeatureBuilder / ai_entry_logic / scanners
        - Strategy / dashboards / self-learning feedback
        - reinvestor / exit_engine / ai_sell_logic

  • Central trade result hook (note_trade_result) for:
        - metrics / DD / winrate
        - Notion logging
        - self-learning strategy updates
        - regime_state account update
        - consecutive-loss & DD-based cooldown management
        - daily trades counter (for RISK_MAX_DAILY_TRADES)

  • Optional legacy per-position exit guardrail (non-AI path):
        - trailing/stop-loss
        - target profit
        - max-hold
        - Jupiter sell + reinvest + self-learning closure hook


Public API
----------
  async current_limits(redis=None) -> dict
  async recommend_ticket_usd(redis=None) -> float

  async should_allow_new_entry(redis=None) -> bool
  async entry_gate_debug(redis=None) -> dict

  async risk_snapshot(redis=None) -> dict
  async risk_capacity_snapshot(redis=None) -> dict
  async risk_capacity_score(redis=None) -> float
  async read_risk_ctx_hash(redis=None) -> dict[str, float]

  async note_trade_result(...)
  async note_spend(redis, usd: float) -> None
  async risk_budget_summary(redis=None) -> dict
  async estimate_equity_usd() -> float

  # Pair-level helper:
  async pair_limits_for_depth(pool_depth_usd: Optional[float], redis=None) -> dict
  def liq_aware_ticket_usd(base_ticket_usd: float, pool_depth_usd: Optional[float]) -> float

  # High-level diagnostics:
  async risk_health(redis=None) -> dict   # aggregate risk_snapshot + entry_gate_debug

  # Extra helpers for ops / dashboards:
  async set_manual_block(on: bool = True, redis=None) -> None
  async clear_manual_block(redis=None) -> None
  async clear_risk_cooldown(redis=None) -> None
  async force_risk_mode(mode: int | None, redis=None) -> None
  async set_risk_profile_override(profile: str | None, redis=None) -> None

Background loops
----------------
  async monitor_positions_loop(...)
  async risk_manager_loop(stop_evt)

All other modules (Strategy, scanners, ai_entry_logic, self_learning_ai_strategy,
reinvestor, ai_sell_logic, exit_engine) should treat THIS as the canonical
risk brain.
"""

import asyncio
import logging
import math
import os
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional, Tuple, List

# Optional standalone redis client (fallback if core_utils.redis is None)
try:  # pragma: no cover – optional dependency
    import redis.asyncio as aioredis  # type: ignore[import-not-found]
except Exception:  # pragma: no cover
    aioredis = None  # type: ignore[assignment]

# Local positions / strategy helpers (JSON-based store / canonical positions API)
from positions import (
    load_positions,
    save_position,
    close_position as remove_position,
)

# Self-learning strategy brain (equity plan + risk caps)
from utils_pkg.self_learning_ai_strategy import (
    get_strategy_params,
    process_trade_closure,
)

# Core utils (shared Redis client + infra)
from utils_pkg.core_utils import (
    get_token_price,             # returns USD price from Dexscreener/Birdeye
    get_wallet_tokens,           # wallet SPL tokens
    swap_token_jupiter,          # Jupiter-based swap helper
    convert_sol_to_usd_amount,   # SOL amount -> USD
    send_telegram,
    log_to_notion,
    redis as shared_redis,
)

# Regime state (risk_on / normal / risk_off) from upgraded module
from utils_pkg.regime import regime_state

# Optional AI risk capacity overlay (offline-trained model)
try:  # pragma: no cover - optional
    from utils_pkg.ai_risk_model import (
        predict_capacity_adjustment as _predict_capacity_adjustment,
    )
except Exception:  # pragma: no cover
    _predict_capacity_adjustment = None  # type: ignore[assignment]

# Optional structured logger (linked to data_logger)
try:
    from utils_pkg.data_logger import log_event  # type: ignore
except Exception:  # pragma: no cover

    async def log_event(*_: Any, **__: Any) -> None:
        return


logger = logging.getLogger("risk_manager")
if not logger.handlers:
    logging.basicConfig(level=logging.INFO)

# Version tag for dashboards / debugging / AI
RISK_ENGINE_VERSION = "3+++"  # keep in sync with other V3+++ modules

__all__ = [
    # sizing / gating
    "current_limits",
    "should_allow_new_entry",
    "entry_gate_debug",
    "recommend_ticket_usd",
    "risk_snapshot",
    "risk_capacity_snapshot",
    "risk_capacity_score",
    "read_risk_ctx_hash",
    "note_trade_result",
    "note_spend",
    "risk_budget_summary",
    "estimate_equity_usd",
    "liq_aware_ticket_usd",
    "pair_limits_for_depth",
    # loops
    "monitor_positions_loop",
    "risk_manager_loop",
    "handle_position",
    # ops helpers
    "set_manual_block",
    "clear_manual_block",
    "clear_risk_cooldown",
    "force_risk_mode",
    "set_risk_profile_override",
    # aggregate health helper
    "risk_health",
]

# ============================================================================ #
# Constants & ENV helpers
# ============================================================================ #

SOL_MINT = "So11111111111111111111111111111111111111112"


def _env_bool(key: str, default: bool = False) -> bool:
    v = os.getenv(key)
    if v is None:
        return default
    return str(v).strip().lower() in ("1", "true", "yes", "y", "on")


def _env_float(key: str, default: float) -> float:
    try:
        return float(os.getenv(key, str(default)))
    except Exception:
        return default


def _env_int(key: str, default: int) -> int:
    try:
        return int(os.getenv(key, str(default)))
    except Exception:
        return default


def _env_str(key: str, default: str = "") -> str:
    return os.getenv(key, default).strip()


# ---- Legacy per-position exit knobs (kept for backward compat) ----

TRAILING_STOP_ENABLED = _env_bool("TRAILING_STOP_ENABLED", True)
DEFAULT_STOP_LOSS_PERCENT = _env_float("STOP_LOSS_PERCENT", 12.0)
DEFAULT_TARGET_PROFIT_PERCENT = _env_float("TARGET_PROFIT_PERCENT", 0.0)  # 0 = disabled
DEFAULT_MAX_HOLD_MINUTES = _env_int("MAX_HOLD_TIME_MINUTES", 240)
DEFAULT_SLIPPAGE = _env_float("JUPITER_MAX_SLIPPAGE", 0.005)  # fraction (0.5%)

# Position monitor loop cadence
POSITION_CHECK_INTERVAL = _env_int("POSITION_MONITOR_INTERVAL", 20)

# Backwards-compatible cooldown env:
#   prefer ALERT_COOLDOWN_SECONDS, fallback to older ALERT_COOLDOWNS_SECONDS
ALERT_COOLDOWN_SECONDS = _env_int(
    "ALERT_COOLDOWN_SECONDS",
    _env_int("ALERT_COOLDOWNS_SECONDS", 900),
)

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")
WALLET_PUBKEY = (
    os.getenv("WALLET_PUBKEY")
    or os.getenv("WALLET_PUBLIC_KEY")
    or os.getenv("WALLET_ADDRESS")
)

# ---- Day-level governance / base caps ----

STOP_AFTER_DAILY_PROFIT_PCT = _env_float(
    "STOP_AFTER_DAILY_PROFIT_PCT",
    4.0,
)  # % of equity where we strongly taper capacity

DAILY_LOSS_CAP_PCT = _env_float(
    "DAILY_LOSS_CAP_PCT",
    0.0,
)  # optional daily loss kill-switch (% of equity, 0=disabled)

DAILY_SPEND_CAP_PCT = _env_float("DAILY_SPEND_CAP_PCT", 0.15)  # % of equity
DAILY_SPEND_CAP_USD = _env_float("DAILY_SPEND_CAP_USD", 0.0)   # hard USD cap (0=off)

# Baseline caps (tiering can be tighter, but not looser)
RISK_MAX_PER_TRADE = _env_float("RISK_MAX_PER_TRADE", 0.045)
MAX_CONCURRENT_POS = _env_int("MAX_CONCURRENT_POS", 4)

# Ticket min/max clamps (for UX + sanity)
RISK_MIN_TICKET_USD = _env_float("RISK_MIN_TICKET_USD", 5.0)
RISK_MAX_TICKET_USD = _env_float("RISK_MAX_TICKET_USD", 5_000.0)

# Equity utilization guardrail:
# max fraction of equity we are willing to lock across concurrent positions
# (ticket_usd * max_concurrent <= equity * RISK_MAX_EQUITY_UTILIZATION_PCT/100)
RISK_MAX_EQUITY_UTILIZATION_PCT = _env_float(
    "RISK_MAX_EQUITY_UTILIZATION_PCT",
    25.0,  # default: at most 25% of equity notionally at work across open tickets
)

# ---- Adaptive throttle thresholds ----

THROTTLE_WINRATE_FLOOR = _env_float("THROTTLE_WINRATE_FLOOR", 0.45)
THROTTLE_DD_FLOOR = _env_float("THROTTLE_DD_FLOOR", 0.08)
THROTTLE_TICKET_CUT = _env_float("THROTTLE_TICKET_CUT", 0.40)  # 40% cut
THROTTLE_CONC_DEC = _env_int("THROTTLE_CONC_DEC", 1)           # minus one slot

# ---- Risk profile (global flavour: conservative / balanced / aggressive / auto) ----

RISK_PROFILE = _env_str("RISK_PROFILE", "balanced").lower()
RISK_PROFILE_OVERRIDE_KEY = "risk:profile_override"

# Soft multipliers applied on top of tier + regime + capacity.
RISK_PROFILE_CONFIG: Dict[str, Dict[str, float]] = {
    "conservative": {
        "ticket_mult": 0.70,
        "pos_pct_mult": 0.80,
        "max_conc_mult": 0.80,
        "daily_spend_mult": 0.80,
        "capacity_exp_mult": 1.20,  # more convex (shrinks faster)
    },
    "balanced": {
        "ticket_mult": 1.00,
        "pos_pct_mult": 1.00,
        "max_conc_mult": 1.00,
        "daily_spend_mult": 1.00,
        "capacity_exp_mult": 1.00,
    },
    "aggressive": {
        "ticket_mult": 1.20,
        "pos_pct_mult": 1.10,
        "max_conc_mult": 1.10,
        "daily_spend_mult": 1.20,
        "capacity_exp_mult": 0.85,  # more linear/forgiving
    },
}
RISK_PROFILE_DEFAULTS = RISK_PROFILE_CONFIG.get(
    RISK_PROFILE if RISK_PROFILE in RISK_PROFILE_CONFIG else "balanced",
    RISK_PROFILE_CONFIG["balanced"],
)

# Regime concurrency deltas (ticket scaling uses regime_state.risk_multiplier())
REGIME_MAX_CONC_DELTA: Dict[str, int] = {
    "risk_off": _env_int("REGIME_MAX_CONC_DELTA_RISK_OFF", -2),
    "normal": _env_int("REGIME_MAX_CONC_DELTA_NORMAL", 0),
    "risk_on": _env_int("REGIME_MAX_CONC_DELTA_RISK_ON", 1),
}

# Tier schedule (equity bands).
TIER_SCHEDULE: List[Dict[str, Any]] = [
    {"equity_to": 2_500, "ticket": 50, "pos_pct": 0.025, "max_conc": 3, "spend_pct": 0.10},
    {"equity_to": 5_000, "ticket": 100, "pos_pct": 0.030, "max_conc": 4, "spend_pct": 0.12},
    {"equity_to": 10_000, "ticket": 175, "pos_pct": 0.035, "max_conc": 5, "spend_pct": 0.14},
    {"equity_to": 25_000, "ticket": 325, "pos_pct": 0.040, "max_conc": 6, "spend_pct": 0.15},
    {"equity_to": 50_000, "ticket": 600, "pos_pct": 0.045, "max_conc": 7, "spend_pct": 0.16},
    {"equity_to": 100_000, "ticket": 1_000, "pos_pct": 0.045, "max_conc": 8, "spend_pct": 0.18},
    {"equity_to": 500_000, "ticket": 2_500, "pos_pct": 0.050, "max_conc": 10, "spend_pct": 0.20},
    {
        "equity_to": 10**12,
        "ticket": 0,  # depth-driven at high AUM
        "pos_pct": 0.050,
        "max_conc": 10,
        "spend_pct": 0.20,
    },
]

# Risk capacity model (how we compress winrate / DD / daily pnl → 0..1)
RISK_CAPACITY_DD_AT_ZERO = _env_float("RISK_CAPACITY_DD_AT_ZERO", 0.20)  # 20% DD → 0
RISK_CAPACITY_WR_LOW = _env_float("RISK_CAPACITY_WR_LOW", 0.40)
RISK_CAPACITY_WR_MID = _env_float("RISK_CAPACITY_WR_MID", 0.55)

# Extra volatility / choppiness penalties from regime_state (optional)
RISK_CAPACITY_VOL_PENALTY = _env_float("RISK_CAPACITY_VOL_PENALTY", 0.15)
RISK_CAPACITY_CHOP_PENALTY = _env_float("RISK_CAPACITY_CHOP_PENALTY", 0.15)

# Optional manual risk_mode override (ENV) and infra soft flags
RISK_MODE_OVERRIDE_ENV = os.getenv("RISK_MODE_OVERRIDE")  # "0", "1", "2", "3" or None

# Ticket scaling exponent based on capacity (1.0 = linear, <1 more aggressive, >1 more defensive)
RISK_CAPACITY_TICKET_EXP_BASE = _env_float("RISK_CAPACITY_TICKET_EXP", 0.9)

# Optional global equity drawdown kill-switch
EQUITY_KILL_SWITCH_DD_PCT = _env_float("EQUITY_KILL_SWITCH_DD_PCT", 0.0)  # 0 = disabled

# Whether legacy per-position selling from this file is enabled
RISK_LEGACY_SELL_ENABLED = _env_bool("RISK_LEGACY_SELL_ENABLED", True)

# Consecutive-loss cooldown: after N losses in a row, block new entries for X minutes
RISK_COOLDOWN_CONSEC_LOSSES = _env_int("RISK_COOLDOWN_CONSEC_LOSSES", 3)
RISK_COOLDOWN_MINUTES = _env_int("RISK_COOLDOWN_MINUTES", 15)

# DD-based cooldown
RISK_DD_COOLDOWN_FLOOR_PCT = _env_float("RISK_DD_COOLDOWN_FLOOR_PCT", 0.0)
RISK_DD_COOLDOWN_MINUTES = _env_int("RISK_DD_COOLDOWN_MINUTES", 0)

# Optional hard floor on risk_capacity for entries (extra guardrail on top of risk_mode)
RISK_MIN_CAPACITY_FOR_ENTRY = _env_float("RISK_MIN_CAPACITY_FOR_ENTRY", 0.05)

# Plan-aware hard stop for entries when far ahead of equity curve
RISK_PLAN_AHEAD_HARD_STOP_PCT = _env_float("RISK_PLAN_AHEAD_HARD_STOP_PCT", 0.0)

# Equity floor guardrail – if equity_usd < RISK_MIN_EQUITY_USD, no new entries.
RISK_MIN_EQUITY_USD = _env_float("RISK_MIN_EQUITY_USD", 0.0)

# Synthetic equity override: if > 0, treat as equity for risk calculations
RISK_EQUITY_OVERRIDE_USD = _env_float("RISK_EQUITY_OVERRIDE_USD", 0.0)

# Daily trade-count cap — hard upper bound on trades per UTC day (0 = disabled)
RISK_MAX_DAILY_TRADES = _env_int("RISK_MAX_DAILY_TRADES", 0)

# Manual block (ops override) key
RISK_MANUAL_BLOCK_KEY = "risk:manual_block"  # "1" to block all new entries

# Risk context hash (consumed by FeatureBuilder, scanners, etc.)
RISK_CTX_KEY = "risk:ctx"

# Optional halt reason key
RISK_HALT_REASON_KEY = "risk:halt_reason"
RISK_COOLDOWN_UNTIL_KEY = "risk:cooldown_until_ts"

# Metrics keys
METRICS_LAST20_KEY = "metrics:last20"           # list of 1/0 (most recent first)
METRICS_TRADES_KEY = "metrics:trades_total"
METRICS_WINS_KEY = "metrics:wins_total"
METRICS_WINRATE20 = "metrics:winrate20"
METRICS_ROLLING_DD = "metrics:rolling_dd_pct"   # fraction (0..1)
EQUITY_HWM_KEY = "metrics:equity_hwm_usd"

# Live override keys for live tuning (all optional)
OVERRIDE_TICKET_KEY = "risk:ticket_usd_override"
OVERRIDE_POS_PCT_KEY = "risk:pos_pct_cap_override"
OVERRIDE_MAX_CONC_KEY = "risk:max_concurrent_override"
OVERRIDE_DAILY_SPEND_KEY = "risk:daily_spend_usd_override"
OVERRIDE_MODE_KEY = "risk:mode_override"  # "0".."3"

# Liquidity-aware sizing helpers:
RISK_LIQ_AWARE_SIZING = _env_bool("RISK_LIQ_AWARE_SIZING", True)
RISK_LIQ_MAX_IMPACT_BPS = _env_float("RISK_LIQ_MAX_IMPACT_BPS", 75.0)  # 0.75% default

# Sandbox mode: clamp risk super low for live testing
RISK_SANDBOX_MODE = _env_bool("RISK_SANDBOX_MODE", False)

# Kelly-style per-trade risk clamp (safety, not hyper-precise Kelly)
RISK_KELLY_ENABLED = _env_bool("RISK_KELLY_ENABLED", True)
RISK_KELLY_DEFAULT_R = _env_float("RISK_KELLY_DEFAULT_R", 1.5)     # avg win/loss payoff
RISK_KELLY_MAX_FRACTION = _env_float("RISK_KELLY_MAX_FRACTION", 0.05)
RISK_KELLY_MIN_FRACTION = _env_float("RISK_KELLY_MIN_FRACTION", 0.005)

# NEW: Trading window controls (UTC-based)
RISK_TRADING_WINDOW_ENABLED = _env_bool("RISK_TRADING_WINDOW_ENABLED", False)
RISK_TRADING_START_HOUR_UTC = _env_int("RISK_TRADING_START_HOUR_UTC", 0)   # [0, 23]
RISK_TRADING_END_HOUR_UTC = _env_int("RISK_TRADING_END_HOUR_UTC", 24)      # [0, 24], end-exclusive
RISK_TRADING_DAYS = _env_str(
    "RISK_TRADING_DAYS",
    "",  # empty = all days; otherwise comma-separated: "mon,tue,wed,fri" or "0,1,2,3,4"
).lower()

# Local cooldowns for sell alerts per-mint (in-memory only)
_COOLDOWNS: Dict[str, float] = {}

# Track last broadcast risk_mode to avoid spammy alerts
_last_broadcast_mode: Optional[int] = None


# ============================================================================ #
# Redis / time helpers
# ============================================================================ #

def _key_daily(prefix: str) -> str:
    today = datetime.now(timezone.utc).strftime("%Y%m%d")
    return f"{prefix}:{today}"


def _pnl_key_today() -> str:
    return _key_daily("pnl:day:realized_usd")


async def _get_redis(url: Optional[str] = None):
    """
    Prefer the shared Redis client from core_utils; fallback to a local aioredis
    client if needed, or return None if Redis is unavailable.
    """
    if shared_redis is not None:
        return shared_redis
    if aioredis is None:
        logger.warning(
            "redis (async) not installed; running without Redis-backed budgets/metrics."
        )
        return None
    return aioredis.from_url(url or REDIS_URL, decode_responses=True)


def _now_ts() -> float:
    return datetime.now(timezone.utc).timestamp()


def _midnight_utc() -> datetime:
    now = datetime.now(timezone.utc)
    return (now + timedelta(days=1)).replace(
        hour=0,
        minute=0,
        second=0,
        microsecond=0,
    )


async def _set_expire_at_midnight(redis, key: str) -> None:
    if not redis:
        return
    try:
        ttl = int((_midnight_utc() - datetime.now(timezone.utc)).total_seconds())
        if ttl > 0:
            await redis.expire(key, ttl)
    except Exception:
        pass


def _parse_trading_days(raw: str) -> Optional[set[int]]:
    """
    Parse RISK_TRADING_DAYS into a set of weekday indices (0=Mon..6=Sun).

    Accepts:
      - "mon,tue,wed" / "monday,friday"
      - "0,1,2,3,4"
    Returns None if parsing fails or raw is empty → interpreted as "all days".
    """
    raw = (raw or "").strip().lower()
    if not raw:
        return None

    name_to_idx = {
        "mon": 0,
        "monday": 0,
        "tue": 1,
        "tues": 1,
        "tuesday": 1,
        "wed": 2,
        "weds": 2,
        "wednesday": 2,
        "thu": 3,
        "thur": 3,
        "thurs": 3,
        "thursday": 3,
        "fri": 4,
        "friday": 4,
        "sat": 5,
        "saturday": 5,
        "sun": 6,
        "sunday": 6,
    }
    out: set[int] = set()
    for part in raw.split(","):
        p = part.strip()
        if not p:
            continue
        if p.isdigit():
            try:
                idx = int(p)
                if 0 <= idx <= 6:
                    out.add(idx)
            except Exception:
                continue
        else:
            idx = name_to_idx.get(p)
            if idx is not None:
                out.add(idx)
    return out or None


def _trading_window_status() -> Tuple[bool, str]:
    """
    Evaluate if we are inside the configured trading window (UTC).

    Env controls:
      RISK_TRADING_WINDOW_ENABLED       -> bool
      RISK_TRADING_START_HOUR_UTC      -> int in [0,23]
      RISK_TRADING_END_HOUR_UTC        -> int in [0,24], end-exclusive
      RISK_TRADING_DAYS                -> "" or comma list of weekdays
                                          "mon,tue,wed,fri" or "0,1,2,3,4"

    Returns: (is_open, human_reason)
    """
    if not RISK_TRADING_WINDOW_ENABLED:
        return True, "trading_window_disabled"

    now = datetime.now(timezone.utc)
    hour = now.hour
    weekday = now.weekday()  # 0=Mon..6=Sun

    allowed_days = _parse_trading_days(RISK_TRADING_DAYS)
    if allowed_days is not None and weekday not in allowed_days:
        return False, f"weekday {weekday} not in allowed trading days"

    # Clamp hours to valid ranges
    start = max(0, min(23, int(RISK_TRADING_START_HOUR_UTC)))
    end = max(0, min(24, int(RISK_TRADING_END_HOUR_UTC)))

    # If start == end, treat as "all day"
    if start == end:
        return True, "24h_window"

    if start < end:
        in_window = start <= hour < end
        if not in_window:
            return False, f"hour {hour} outside [{start},{end}) UTC"
    else:
        # Window crosses midnight, e.g. 18 → 4
        in_window = (hour >= start) or (hour < end)
        if not in_window:
            return False, f"hour {hour} outside overnight window {start}→{end} UTC"

    return True, "within_trading_window"


# ============================================================================ #
# Equity & metrics
# ============================================================================ #

async def _wallet_equity_usd() -> float:
    """
    Estimate total equity in USD using wallet balances & token prices.

    Strategy:
      - Fetch wallet SPL tokens from core_utils.get_wallet_tokens().
      - For SOL itself, treat balance as SOL.
      - For other tokens:
            price_usd = get_token_price(mint)
            sol_usd   = price of 1 SOL in USD
            px_sol    = price_usd / sol_usd
            sol_equiv = balance * px_sol
      - Convert aggregate SOL to USD once via convert_sol_to_usd_amount.

    This is best-effort and conservative; failures return 0.0.
    """
    if not WALLET_PUBKEY:
        return 0.0
    try:
        tokens = await get_wallet_tokens(WALLET_PUBKEY)  # [{mint, balance}, ...]
        if not tokens:
            return 0.0

        sol_usd = await convert_sol_to_usd_amount(1.0)
        if sol_usd <= 0:
            return 0.0

        total_sol = 0.0
        for t in tokens:
            try:
                mint = t.get("mint")
                bal = float(t.get("balance", 0.0) or 0.0)
            except Exception:
                continue
            if not mint or bal <= 0:
                continue

            if mint == SOL_MINT:
                total_sol += bal
            else:
                try:
                    px_usd = await get_token_price(mint)
                    if px_usd > 0:
                        px_sol = px_usd / sol_usd
                        total_sol += bal * px_sol
                except Exception:
                    continue

        usd = await convert_sol_to_usd_amount(total_sol)
        return float(usd or 0.0)
    except Exception as e:
        logger.warning("[RISK] equity calc failed: %s", e)
        return 0.0


async def estimate_equity_usd() -> float:
    """
    Public convenience wrapper: expose wallet equity to other modules.
    Synthetic equity override (RISK_EQUITY_OVERRIDE_USD) is *not* applied here,
    so other modules can explicitly choose whether to use synthetic equity.
    """
    return await _wallet_equity_usd()


async def _wallet_sol_balance() -> float:
    """
    Lightweight SOL-balance helper used for MIN_SOL_BALANCE guardrails.
    """
    if not WALLET_PUBKEY:
        return 0.0
    try:
        tokens = await get_wallet_tokens(WALLET_PUBKEY)
        for t in tokens or []:
            if t.get("mint") == SOL_MINT:
                return float(t.get("balance", 0.0))
        return 0.0
    except Exception as e:
        logger.warning("[RISK] SOL balance calc failed: %s", e)
        return 0.0


async def _metrics(redis) -> Tuple[float, float]:
    """
    Return (winrate20, rolling_dd_pct) from Redis.

    Defaults to (0.50, 0.0) if metrics or Redis are unavailable.
    rolling_dd_pct is a fraction (0..1).
    """
    try:
        if redis is None:
            redis = await _get_redis()
        if not redis:
            return 0.50, 0.0
        wr, dd = await redis.mget(METRICS_WINRATE20, METRICS_ROLLING_DD)
        winrate = float(wr) if wr is not None else 0.50
        drawdown = float(dd) if dd is not None else 0.0
        return winrate, drawdown
    except Exception:
        return 0.50, 0.0


async def _equity_hwm(redis) -> float:
    """
    Helper: best-effort equity high-watermark in USD.
    """
    try:
        if redis is None:
            redis = await _get_redis()
        if not redis:
            return 0.0
        v = await redis.get(EQUITY_HWM_KEY)
        return float(v) if v else 0.0
    except Exception:
        return 0.0


async def _consecutive_losses(redis) -> int:
    """
    Approximate consecutive losses from METRICS_LAST20 (1=win, 0=loss).

    'Consecutive losses' = number of leading zeros from the most
    recent trade backwards until the first win is encountered.
    """
    try:
        if redis is None:
            redis = await _get_redis()
        if not redis:
            return 0
        seq = await redis.lrange(METRICS_LAST20_KEY, 0, 19)
        if not seq:
            return 0
        losses = 0
        for v in seq:
            try:
                is_win = int(v) == 1
            except Exception:
                is_win = False
            if is_win:
                break
            losses += 1
        return losses
    except Exception:
        return 0


def _select_tier(equity_usd: float) -> Dict[str, Any]:
    """
    Choose a tier from TIER_SCHEDULE based on equity.
    """
    for tier in TIER_SCHEDULE:
        if equity_usd <= tier["equity_to"]:
            return dict(tier)
    return dict(TIER_SCHEDULE[-1])


# ============================================================================ #
# Risk capacity model & Kelly helper
# ============================================================================ #

def _clamp(x: float, lo: float = 0.0, hi: float = 1.0) -> float:
    return max(lo, min(hi, float(x)))


def _risk_capacity_from_components(
    *,
    winrate20: float,
    rolling_dd: float,
    realized_pnl_usd: float,
    equity_usd: float,
    regime_mult: float,
    regime_volatility: float = 0.5,
    regime_choppy: float = 0.0,
) -> float:
    """
    Compress performance / drawdown / daily pnl & regime into a 0..1 risk capacity.

    Intuition:
      - High DD & low winrate → near 0 capacity.
      - Flat DD & decent winrate → 0.7–1.0 capacity.
      - Approaching daily profit target → capacity tapers down.
      - High volatility / choppiness modestly shrink capacity.
      - Regime multiplier scales capacity (risk_off < 1, risk_on > 1).
    """
    # Drawdown term: 0 at RISK_CAPACITY_DD_AT_ZERO or worse, 1 at 0 DD
    dd = max(0.0, rolling_dd)
    if RISK_CAPACITY_DD_AT_ZERO <= 0:
        dd_term = 1.0
    else:
        dd_term = _clamp(1.0 - dd / RISK_CAPACITY_DD_AT_ZERO)

    # Winrate term: 0 at WR_LOW or worse, 1 at WR_MID or better
    wr = _clamp(winrate20, 0.0, 1.0)
    if RISK_CAPACITY_WR_MID <= RISK_CAPACITY_WR_LOW:
        wr_term = wr
    else:
        wr_term = _clamp(
            (wr - RISK_CAPACITY_WR_LOW)
            / (RISK_CAPACITY_WR_MID - RISK_CAPACITY_WR_LOW)
        )

    # Daily PnL term: 1 at <=0, taper as we approach daily target
    if equity_usd <= 0 or STOP_AFTER_DAILY_PROFIT_PCT <= 0:
        pnl_term = 1.0
    else:
        target_usd = equity_usd * (STOP_AFTER_DAILY_PROFIT_PCT / 100.0)
        if target_usd <= 0:
            pnl_term = 1.0
        else:
            pct = max(0.0, realized_pnl_usd) / target_usd
            base = 1.0 - min(pct, 1.0) * 0.7  # taper to 0.3 as we approach target
            pnl_term = _clamp(base, 0.3, 1.0)

    # Volatility / choppiness penalty from regime_state (best-effort)
    vol_penalty = _clamp(regime_volatility, 0.0, 1.0) * RISK_CAPACITY_VOL_PENALTY
    chop_penalty = _clamp(regime_choppy, 0.0, 1.0) * RISK_CAPACITY_CHOP_PENALTY

    base = 0.40 * dd_term + 0.40 * wr_term + 0.20 * pnl_term
    base = base * (1.0 - vol_penalty) * (1.0 - chop_penalty)

    # Regime: risk_off < 1, risk_on > 1, but clamp to sane band.
    mult = _clamp(regime_mult, 0.6, 1.4)
    capacity = _clamp(base * mult, 0.0, 1.0)
    return capacity


def _risk_mode_from_capacity(capacity: float) -> int:
    """
    Discrete risk_mode that maps into:
       0 = OFF (halt, no new entries)
       1 = NORMAL
       2 = AGGRESSIVE
       3 = DEFENSIVE (still trade but shrunk sizing)
    """
    c = _clamp(capacity, 0.0, 1.0)
    if c <= 0.15:
        return 0
    if c <= 0.35:
        return 3
    if c >= 0.80:
        return 2
    return 1


def _kelly_fraction(winrate: float, payoff_r: float) -> float:
    """
    Simple Kelly fraction estimate:

        f* = p - (q / R)

    where:
      p = winrate
      q = 1 - p
      R = average win / average loss payoff ratio.

    We clamp aggressively; this is meant as a *safety* signal, not a precision Kelly.
    """
    p = _clamp(winrate, 0.0, 1.0)
    if payoff_r <= 0:
        return 0.0
    q = 1.0 - p
    f_star = p - (q / payoff_r)
    return _clamp(f_star, 0.0, 1.0)


def _kelly_pos_cap(winrate: float) -> Tuple[float, float]:
    """
    Return (kelly_raw, kelly_pos_pct_cap):

      kelly_raw          → raw Kelly fraction in [0,1]
      kelly_pos_pct_cap  → sanitized fraction in [min, max] for use as an
                           *upper bound* on per-trade risk.
    """
    if not RISK_KELLY_ENABLED:
        return 0.0, 0.0
    raw = _kelly_fraction(winrate, RISK_KELLY_DEFAULT_R)
    if raw <= 0:
        # still allow a tiny exploratory fraction
        frac = RISK_KELLY_MIN_FRACTION
    else:
        frac = raw
    frac = max(RISK_KELLY_MIN_FRACTION, min(RISK_KELLY_MAX_FRACTION, frac))
    return raw, frac


async def _risk_mode_with_override(
    redis,
    capacity: float,
    *,
    rolling_dd: float,
) -> Tuple[float, int, Optional[int], Optional[str]]:
    """
    Compute (capacity, risk_mode, override_mode, halt_reason):

      • capacity: possibly shrunk (for OFF/DEFENSIVE)
      • risk_mode: 0..3 derived from capacity or override
      • override_mode: if non-None, the explicit override used
      • halt_reason: optional human-readable reason for risk_mode=0
    """
    base_mode = _risk_mode_from_capacity(capacity)
    override_mode: Optional[int] = None
    halt_reason: Optional[str] = None

    # ENV override wins over Redis if present
    if RISK_MODE_OVERRIDE_ENV is not None:
        try:
            override_mode = int(RISK_MODE_OVERRIDE_ENV)
        except Exception:
            override_mode = None
    elif redis is not None:
        try:
            val = await redis.get(OVERRIDE_MODE_KEY)
            if val is not None:
                override_mode = int(val)
        except Exception:
            override_mode = None

    if override_mode is not None and override_mode in (0, 1, 2, 3):
        risk_mode = override_mode
    else:
        risk_mode = base_mode

    # Optional global equity DD kill switch (uses rolling DD fraction)
    if EQUITY_KILL_SWITCH_DD_PCT > 0 and rolling_dd >= EQUITY_KILL_SWITCH_DD_PCT:
        risk_mode = 0
        halt_reason = (
            f"equity_drawdown>=kill_switch "
            f"({rolling_dd:.3f} ≥ {EQUITY_KILL_SWITCH_DD_PCT:.3f})"
        )

    # Optionally shrink capacity further for defensive or off
    if risk_mode == 0:
        capacity_adj = 0.0
    elif risk_mode == 3:
        capacity_adj = min(capacity, 0.40)
    else:
        capacity_adj = capacity

    return capacity_adj, risk_mode, override_mode, halt_reason


# ============================================================================ #
# Live overrides
# ============================================================================ #

async def _effective_risk_profile(redis) -> tuple[str, Dict[str, float]]:
    """
    Determine the *active* risk profile and return:

      (profile_name, profile_config_dict)

    Where profile_name is one of:
      - 'conservative' / 'balanced' / 'aggressive'
      - or 'auto' resolved to one of the above via regime_state.risk_profile_hint().
    """
    profile_name = RISK_PROFILE

    # Redis override beats env if valid
    if redis:
        try:
            v = await redis.get(RISK_PROFILE_OVERRIDE_KEY)
            if v:
                v_lower = str(v).strip().lower()
                if v_lower in RISK_PROFILE_CONFIG or v_lower == "auto":
                    profile_name = v_lower
        except Exception:
            pass

    # If "auto", let regime_state hint the flavour
    if profile_name == "auto":
        try:
            hinted = regime_state.risk_profile_hint()
            profile_name = hinted if hinted in RISK_PROFILE_CONFIG else "balanced"
        except Exception:
            profile_name = "balanced"

    cfg = RISK_PROFILE_CONFIG.get(profile_name, RISK_PROFILE_CONFIG["balanced"])
    return profile_name, cfg


async def _apply_overrides(
    redis,
    *,
    ticket_usd: float,
    pos_pct_cap: float,
    max_concurrent: int,
    daily_spend_usd: float,
) -> Dict[str, Any]:
    """
    Allow live tuning from Redis without redeploys.

    Keys (all optional):
      risk:ticket_usd_override          -> float (USD)
      risk:pos_pct_cap_override         -> float (0..RISK_MAX_PER_TRADE)
      risk:max_concurrent_override      -> int
      risk:daily_spend_usd_override     -> float (USD)
    """
    base = {
        "ticket_usd": float(ticket_usd),
        "pos_pct_cap": float(pos_pct_cap),
        "max_concurrent": int(max_concurrent),
        "daily_spend_usd": float(daily_spend_usd),
    }
    if not redis:
        return base

    try:
        t_raw, p_raw, c_raw, s_raw = await redis.mget(
            OVERRIDE_TICKET_KEY,
            OVERRIDE_POS_PCT_KEY,
            OVERRIDE_MAX_CONC_KEY,
            OVERRIDE_DAILY_SPEND_KEY,
        )
    except Exception:
        return base

    # Ticket override
    if t_raw is not None:
        try:
            t = float(t_raw)
            if t > 0:
                base["ticket_usd"] = t
        except Exception:
            pass

    # Pos pct override
    if p_raw is not None:
        try:
            p = float(p_raw)
            if 0 < p <= RISK_MAX_PER_TRADE:
                base["pos_pct_cap"] = p
        except Exception:
            pass

    # Max conc override
    if c_raw is not None:
        try:
            c = int(c_raw)
            if c > 0:
                base["max_concurrent"] = c
        except Exception:
            pass

    # Daily spend override
    if s_raw is not None:
        try:
            s = float(s_raw)
            if s > 0:
                base["daily_spend_usd"] = s
        except Exception:
            pass

    return base


# ============================================================================ #
# Liquidity-aware ticket helper (used by ai_entry_logic / FeatureBuilder)
# ============================================================================ #

def liq_aware_ticket_usd(
    base_ticket_usd: float,
    pool_depth_usd: Optional[float],
) -> float:
    """
    Liquidity-aware clamp for ticket size.

    This is intentionally stateless and pure so scanners / ai_entry_logic can call it.

    Intuition:
      - If pool_depth_usd is None or RISK_LIQ_AWARE_SIZING is disabled, return base.
      - Otherwise:
            max_notional = pool_depth_usd * (RISK_LIQ_MAX_IMPACT_BPS / 10_000)
        and we clamp ticket at that bound.

    Example:
      pool_depth_usd = 50_000
      RISK_LIQ_MAX_IMPACT_BPS = 75 → 0.75% → max_notional ≈ 375 USD
    """
    base_ticket_usd = float(max(0.0, base_ticket_usd))
    if not RISK_LIQ_AWARE_SIZING:
        return base_ticket_usd
    if pool_depth_usd is None or pool_depth_usd <= 0:
        return base_ticket_usd
    if RISK_LIQ_MAX_IMPACT_BPS <= 0:
        return base_ticket_usd

    impact_frac = float(RISK_LIQ_MAX_IMPACT_BPS) / 10_000.0
    max_notional = float(pool_depth_usd) * impact_frac
    if max_notional <= 0:
        return base_ticket_usd

    return float(max(0.0, min(base_ticket_usd, max_notional)))


async def pair_limits_for_depth(
    pool_depth_usd: Optional[float],
    redis=None,
) -> Dict[str, Any]:
    """
    Convenience helper for scanners / ai_entry_logic / reinvestor:

    Returns the *global* risk limits plus a liquidity-aware ticket for a
    specific pool depth.

    Output contains all fields from current_limits(), plus:
      - ticket_usd_raw: ticket_usd before liq clamp
      - ticket_usd_liq: ticket_usd after liq_aware_ticket_usd()

    Typical usage:
      limits = await pair_limits_for_depth(pool_depth_usd, redis)
      ticket = limits["ticket_usd_liq"]
    """
    limits = await current_limits(redis)
    base_ticket = float(limits.get("ticket_usd", 0.0))
    ticket_liq = liq_aware_ticket_usd(base_ticket, pool_depth_usd)
    merged = dict(limits)
    merged["ticket_usd_raw"] = base_ticket
    merged["ticket_usd_liq"] = ticket_liq
    return merged


# ============================================================================ #
# Limits / sizing (core API)
# ============================================================================ #

async def _daily_realized_usd(redis) -> float:
    """
    Daily realized PnL (USD) from Redis, keyed per UTC day.
    """
    try:
        if redis is None:
            redis = await _get_redis()
        if not redis:
            return 0.0
        key = _pnl_key_today()
        val = await redis.get(key)
        return float(val) if val else 0.0
    except Exception:
        return 0.0


async def _daily_spent_usd(redis) -> float:
    """
    How much we've already spent (entry notional) today in USD.
    """
    if redis is None:
        redis = await _get_redis()
    if not redis:
        return 0.0
    try:
        budget_key = _key_daily("budget:sniper:spent_usd")
        val = await redis.get(budget_key)
        return float(val) if val else 0.0
    except Exception:
        return 0.0


async def _daily_trades(redis) -> int:
    """
    How many trades we've executed today (closed trades),
    used for RISK_MAX_DAILY_TRADES gating and diagnostics.
    """
    if redis is None:
        redis = await _get_redis()
    if not redis:
        return 0
    try:
        key = _key_daily("risk:trades_count")
        val = await redis.get(key)
        return int(val) if val else 0
    except Exception:
        return 0


async def _update_risk_context_hash(
    redis,
    *,
    limits: Dict[str, Any],
    realized_usd: float,
    equity_usd: float,
    winrate20: float,
    consec_losses: int,
    cooldown_until_ts: Optional[float],
    max_risk_per_trade: float,
    min_sol_balance: float,
    strat_mode: Optional[int],
    plan_equity_today_usd: float,
    plan_gap_pct: float,
    plan_status: Optional[str],
    ai_capacity_delta: float,
    halt_reason: Optional[str],
    kelly_raw: float,
    kelly_pos_pct_cap: float,
    active_profile_name: str,
    regime_gate_factor: float,
    equity_utilization_pct: float,
    trading_window_open: bool,
) -> None:
    """
    Publish risk:ctx for FeatureBuilder / scanners / self-learning:

      - open_positions
      - max_positions
      - ticket_usd
      - daily_spend_usd / daily_spend_remaining_usd
      - daily_realized_pnl_pct   (percent, e.g. +5.0 == +5%)
      - daily_drawdown_pct       (percent, 0..100)
      - risk_mode
      - risk_capacity
      - regime_risk_multiplier
      - regime_gate_factor
      - winrate20                (fraction 0..1)
      - equity_usd
      - consecutive_losses
      - cooldown_remaining_sec
      - risk_profile             (active profile name after auto/override)
      - sandbox_mode
      - max_risk_per_trade
      - min_sol_balance
      - strat_mode (optional)
      - plan_equity_today_usd / plan_gap_pct / plan_status (optional)
      - ai_capacity_delta (effect of AI overlay)
      - daily_trades_count / daily_trades_cap
      - kelly_raw / kelly_pos_pct_cap (diagnostic)
      - equity_utilization_pct  (ticket * max_conc / equity, in %)
      - risk_profile_override   (if set in Redis)
      - risk_profile_env        (env default)
      - equity_override_usd     (synthetic equity in use, if any)
      - trading_window_open     (0/1, UTC-based)
      - halt_reason (if any)
      - risk_engine_version
      - equity_hwm_usd
      - last_update_ts
    """
    if not redis:
        return

    try:
        positions = load_positions()
        open_positions = len(positions) if positions else 0
    except Exception:
        open_positions = 0

    daily_spend_usd = float(limits.get("daily_spend_usd", 0.0))
    spent_usd = await _daily_spent_usd(redis)
    trades_count = await _daily_trades(redis)
    daily_realized_pct = (
        (realized_usd / equity_usd) * 100.0 if equity_usd > 0 else 0.0
    )
    dd_fraction = float(limits.get("rolling_dd_pct", 0.0))  # 0..1
    dd_pct = dd_fraction * 100.0
    risk_mode = int(limits.get("risk_mode", 1))
    max_conc = int(limits.get("max_concurrent", 0))
    risk_capacity = float(limits.get("risk_capacity", 1.0))
    regime_mult = float(limits.get("regime_risk_multiplier", 1.0))

    now_ts = _now_ts()
    cooldown_rem = 0.0
    if cooldown_until_ts is not None and cooldown_until_ts > now_ts:
        cooldown_rem = cooldown_until_ts - now_ts

    remaining_spend = max(0.0, daily_spend_usd - spent_usd)
    spend_used_pct = (
        (spent_usd / daily_spend_usd) * 100.0 if daily_spend_usd > 0 else 0.0
    )

    # Expose risk_profile_override (if present) for transparency to AI modules
    risk_profile_override = ""
    try:
        v = await redis.get(RISK_PROFILE_OVERRIDE_KEY)
        if v:
            risk_profile_override = str(v).strip().lower()
    except Exception:
        risk_profile_override = ""

    # HWM for diagnostics
    equity_hwm = await _equity_hwm(redis)

    mapping: Dict[str, str] = {
        "open_positions": f"{float(open_positions):.4f}",
        "max_positions": f"{float(max_conc):.4f}",
        "ticket_usd": f"{float(limits.get('ticket_usd', 0.0)):.4f}",
        "daily_spend_usd": f"{daily_spend_usd:.4f}",
        "daily_spend_remaining_usd": f"{remaining_spend:.4f}",
        "daily_spend_used_pct": f"{spend_used_pct:.6f}",
        "daily_realized_pnl_pct": f"{float(daily_realized_pct):.6f}",
        "daily_drawdown_pct": f"{float(dd_pct):.6f}",
        "risk_mode": f"{float(risk_mode):.1f}",
        "risk_capacity": f"{float(risk_capacity):.6f}",
        "regime_risk_multiplier": f"{float(regime_mult):.6f}",
        "regime_gate_factor": f"{float(regime_gate_factor):.6f}",
        "winrate20": f"{float(winrate20):.6f}",
        "equity_usd": f"{float(equity_usd):.4f}",
        "consecutive_losses": f"{float(consec_losses):.1f}",
        "cooldown_remaining_sec": f"{float(cooldown_rem):.1f}",
        "risk_profile": active_profile_name,
        "risk_profile_env": RISK_PROFILE,
        "risk_profile_override": risk_profile_override,
        "sandbox_mode": "1" if RISK_SANDBOX_MODE else "0",
        "max_risk_per_trade": f"{float(max_risk_per_trade):.6f}",
        "min_sol_balance": f"{float(min_sol_balance):.6f}",
        "plan_equity_today_usd": f"{float(plan_equity_today_usd):.4f}",
        "plan_gap_pct": f"{float(plan_gap_pct):.6f}",
        "ai_capacity_delta": f"{float(ai_capacity_delta):.6f}",
        "daily_trades_count": f"{float(trades_count):.4f}",
        "daily_trades_cap": f"{float(RISK_MAX_DAILY_TRADES):.4f}",
        "kelly_raw": f"{float(kelly_raw):.6f}",
        "kelly_pos_pct_cap": f"{float(kelly_pos_pct_cap):.6f}",
        "equity_utilization_pct": f"{float(equity_utilization_pct):.6f}",
        "risk_engine_version": RISK_ENGINE_VERSION,
        "equity_hwm_usd": f"{float(equity_hwm):.4f}",
        "equity_override_usd": f"{float(RISK_EQUITY_OVERRIDE_USD):.4f}",
        "trading_window_open": "1" if trading_window_open else "0",
        "last_update_ts": datetime.now(timezone.utc).isoformat(),
    }
    if strat_mode is not None:
        mapping["strat_mode"] = str(int(strat_mode))
    if plan_status is not None:
        mapping["plan_status"] = str(plan_status)
    if halt_reason is not None:
        mapping["halt_reason"] = halt_reason

    try:
        await redis.hset(RISK_CTX_KEY, mapping=mapping)
    except Exception:
        # best-effort; no need to explode the pipeline
        pass

    # Structured log for dashboards / analytics (best-effort)
    try:
        await log_event(
            "risk_ctx_update",
            {
                "equity_usd": equity_usd,
                "equity_hwm_usd": equity_hwm,
                "risk_capacity": risk_capacity,
                "risk_mode": risk_mode,
                "winrate20": winrate20,
                "rolling_dd_pct": dd_fraction,
                "plan_gap_pct": plan_gap_pct,
                "ai_capacity_delta": ai_capacity_delta,
                "daily_spend_usd": daily_spend_usd,
                "daily_spent_usd": spent_usd,
                "daily_trades_count": trades_count,
                "daily_trades_cap": RISK_MAX_DAILY_TRADES,
                "kelly_raw": kelly_raw,
                "kelly_pos_pct_cap": kelly_pos_pct_cap,
                "sandbox_mode": RISK_SANDBOX_MODE,
                "risk_profile": active_profile_name,
                "risk_profile_override": risk_profile_override,
                "regime_gate_factor": regime_gate_factor,
                "equity_utilization_pct": equity_utilization_pct,
                "trading_window_open": trading_window_open,
                "risk_engine_version": RISK_ENGINE_VERSION,
            },
        )
    except Exception:
        pass


async def _broadcast_risk_mode_change(
    *,
    redis,
    new_mode: int,
    halt_reason: Optional[str],
    capacity: float,
    winrate20: float,
    rolling_dd: float,
    realized_usd: float,
    equity_usd: float,
) -> None:
    """
    Best-effort broadcast when risk_mode changes (e.g. to 0 / OFF).

    This is intentionally very light; if Telegram or Redis fail, we just move on.
    """
    global _last_broadcast_mode

    if new_mode == _last_broadcast_mode:
        return

    _last_broadcast_mode = new_mode

    # Persist halt reason if any
    if redis and halt_reason:
        try:
            await redis.set(RISK_HALT_REASON_KEY, halt_reason)
        except Exception:
            pass

    # Optional Telegram alert (only on transitions)
    try:
        if new_mode == 0:
            msg = (
                "🚨 *RISK MODE: OFF* — Global trading HALTED\n"
                f"• capacity: {capacity:.3f}\n"
                f"• winrate20: {winrate20:.3f}\n"
                f"• rolling_dd: {rolling_dd:.3f}\n"
                f"• daily_realized_usd: {realized_usd:.2f}\n"
                f"• equity_usd: {equity_usd:.2f}\n"
                f"• sandbox: {'ON' if RISK_SANDBOX_MODE else 'OFF'}\n"
                f"• engine: {RISK_ENGINE_VERSION}\n"
                f"• reason: {halt_reason or 'n/a'}"
            )
        elif new_mode == 3:
            msg = (
                "🟠 *RISK MODE: DEFENSIVE* — shrinking tickets & concurrency\n"
                f"• capacity: {capacity:.3f}  • winrate20: {winrate20:.3f}  "
                f"• rolling_dd: {rolling_dd:.3f}"
            )
        elif new_mode == 2:
            msg = (
                "🟢 *RISK MODE: AGGRESSIVE* — scaling tickets & opportunities\n"
                f"• capacity: {capacity:.3f}  • winrate20: {winrate20:.3f}"
            )
        else:
            msg = (
                "🔵 *RISK MODE: NORMAL*\n"
                f"• capacity: {capacity:.3f}  • winrate20: {winrate20:.3f}"
            )

        await send_telegram(msg)
    except Exception:
        # don't let alerts break risk manager
        pass


@dataclass
class RiskLimits:
    """
    Internal structure for risk limits. We still return dicts publicly
    for backward compatibility, but the dataclass helps keep logic clean.
    """

    equity_usd: float
    ticket_usd: float
    pos_pct_cap: float
    max_concurrent: int
    daily_spend_usd: float
    winrate20: float
    rolling_dd_pct: float
    regime: str
    risk_capacity: float
    risk_mode: int
    risk_mode_override: Optional[int]
    regime_risk_multiplier: float
    regime_gate_factor: float
    risk_profile: str
    ai_capacity_delta: float
    halt_reason: Optional[str] = None
    # Kelly diagnostics (optional)
    kelly_raw: float = 0.0
    kelly_pos_pct_cap: float = 0.0
    # NEW: equity utilization (ticket * max_conc / equity, as %)
    equity_utilization_pct: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "equity_usd": float(self.equity_usd),
            "ticket_usd": float(self.ticket_usd),
            "pos_pct_cap": float(self.pos_pct_cap),
            "max_concurrent": int(self.max_concurrent),
            "daily_spend_usd": float(self.daily_spend_usd),
            "winrate20": float(self.winrate20),
            "rolling_dd_pct": float(self.rolling_dd_pct),
            "regime": self.regime,
            "risk_capacity": float(self.risk_capacity),
            "risk_mode": int(self.risk_mode),
            "risk_mode_override": self.risk_mode_override,
            "regime_risk_multiplier": float(self.regime_risk_multiplier),
            "regime_gate_factor": float(self.regime_gate_factor),
            "risk_profile": self.risk_profile,
            "ai_capacity_delta": float(self.ai_capacity_delta),
            "halt_reason": self.halt_reason,
            "kelly_raw": float(self.kelly_raw),
            "kelly_pos_pct_cap": float(self.kelly_pos_pct_cap),
            "equity_utilization_pct": float(self.equity_utilization_pct),
        }


# ============================================================================ #
# Core limits / sizing
# ============================================================================ #

async def current_limits(redis=None) -> Dict[str, Any]:
    """
    Compute risk limits from equity + tier + regime + throttle + risk capacity
    + overrides + self-learning strategy constraints + plan-aware & AI overlays
    + Kelly-style safety clamp, synthetic equity override, regime_gate_factor,
    equity utilization guardrail, and trading-window status.

    Returns dict with:
      equity_usd, ticket_usd, pos_pct_cap, max_concurrent,
      daily_spend_usd, winrate20, rolling_dd_pct, regime,
      risk_capacity, risk_mode, risk_mode_override,
      regime_risk_multiplier, regime_gate_factor,
      risk_profile, ai_capacity_delta, halt_reason,
      kelly_raw, kelly_pos_pct_cap, equity_utilization_pct
    """
    if redis is None:
        redis = await _get_redis()

    # Equity: wallet → optional synthetic override
    equity_wallet = await _wallet_equity_usd()
    equity = float(equity_wallet)
    if RISK_EQUITY_OVERRIDE_USD > 0:
        equity = RISK_EQUITY_OVERRIDE_USD

    tier = _select_tier(equity)

    # Base tier values
    ticket = float(tier["ticket"]) if tier["ticket"] else max(
        50.0, equity * float(tier["pos_pct"]) * 0.5
    )
    pos_pct_cap = float(tier["pos_pct"])
    max_conc = int(tier["max_conc"])
    daily_spend_usd = equity * float(tier["spend_pct"])

    # Performance metrics
    winrate, dd = await _metrics(redis)              # dd is fraction 0..1
    realized_usd = await _daily_realized_usd(redis)

    # Kelly-style upper bound on per-trade risk (safety clamp)
    kelly_raw = 0.0
    kelly_pos_pct_cap = 0.0
    if RISK_KELLY_ENABLED:
        kelly_raw, kelly_pos_pct_cap = _kelly_pos_cap(winrate)
        if kelly_pos_pct_cap > 0:
            pos_pct_cap = min(pos_pct_cap, kelly_pos_pct_cap)

    # Regime-based scaling
    regime = regime_state.get()
    regime_mult = float(regime_state.risk_multiplier())
    regime_feats = await regime_state.features()
    regime_vol = float(regime_feats.get("regime_volatility", 0.5))
    regime_chop = float(regime_feats.get("regime_choppy_score", 0.0))
    # NEW: regime_gate_factor (extra gate 0..1 from regime_state, default=1)
    regime_gate_factor = float(
        regime_feats.get(
            "regime_gate_factor",
            regime_feats.get("gate_factor", 1.0),
        )
    )
    regime_gate_factor = _clamp(regime_gate_factor, 0.0, 1.0)
    regime_delta_conc = REGIME_MAX_CONC_DELTA.get(regime, 0)

    # Pull self-learning strategy params as *additional* caps
    try:
        strat_params = await get_strategy_params(redis)
    except Exception:
        strat_params = {}

    strat_max_risk = float(strat_params.get("MAX_RISK_PER_TRADE", RISK_MAX_PER_TRADE))
    strat_min_sol_balance = float(strat_params.get("MIN_SOL_BALANCE", 0.0))
    strat_mode = strat_params.get("STRAT_MODE")
    try:
        strat_mode_int: Optional[int] = (
            int(strat_mode) if strat_mode is not None else None
        )
    except Exception:
        strat_mode_int = None

    # Optional plan / equity-curve context (all best-effort)
    plan_equity_today = float(strat_params.get("PLAN_EQUITY_TODAY_USD", 0.0))
    plan_gap_pct = float(strat_params.get("PLAN_GAP_PCT", 0.0))
    plan_status = strat_params.get("PLAN_STATUS")
    try:
        plan_status_str: Optional[str] = (
            str(plan_status) if plan_status is not None else None
        )
    except Exception:
        plan_status_str = None

    # NEW: optional self-learning caps for concurrency & daily spend
    try:
        strat_max_conc_override_raw = strat_params.get("MAX_CONCURRENT_OVERRIDE")
        strat_max_conc_override: Optional[int] = (
            int(strat_max_conc_override_raw)
            if strat_max_conc_override_raw is not None
            else None
        )
        if strat_max_conc_override is not None and strat_max_conc_override <= 0:
            strat_max_conc_override = None
    except Exception:
        strat_max_conc_override = None

    strat_daily_spend_cap_pct = float(strat_params.get("DAILY_SPEND_CAP_PCT", 0.0))
    strat_daily_spend_cap_usd = float(strat_params.get("DAILY_SPEND_CAP_USD", 0.0))

    # Apply global risk profile multipliers (with potential Redis/auto override)
    profile_name, profile_cfg = await _effective_risk_profile(redis)
    ticket *= profile_cfg["ticket_mult"]
    pos_pct_cap *= profile_cfg["pos_pct_mult"]
    max_conc = int(round(max_conc * profile_cfg["max_conc_mult"]))

    daily_spend_usd *= profile_cfg["daily_spend_mult"]

    # Adaptive throttle from performance metrics (pre-capacity)
    if (winrate < THROTTLE_WINRATE_FLOOR) or (dd > THROTTLE_DD_FLOOR):
        ticket *= (1.0 - THROTTLE_TICKET_CUT)
        max_conc = max(1, max_conc - THROTTLE_CONC_DEC)

    # Regime scaling
    ticket *= regime_mult
    max_conc = max(1, max_conc + int(regime_delta_conc))

    # Respect env caps and strategy caps before capacity
    pos_pct_cap = min(pos_pct_cap, RISK_MAX_PER_TRADE, strat_max_risk)
    max_conc = min(max_conc, MAX_CONCURRENT_POS)
    if strat_max_conc_override is not None:
        max_conc = min(max_conc, strat_max_conc_override)

    # Daily spend caps: % of equity AND optional absolute USD
    daily_spend_usd = min(daily_spend_usd, equity * DAILY_SPEND_CAP_PCT)
    if DAILY_SPEND_CAP_USD > 0:
        daily_spend_usd = min(daily_spend_usd, DAILY_SPEND_CAP_USD)

    # NEW: self-learning daily spend caps (strongest clamp)
    if strat_daily_spend_cap_pct > 0:
        daily_spend_usd = min(daily_spend_usd, equity * strat_daily_spend_cap_pct)
    if strat_daily_spend_cap_usd > 0:
        daily_spend_usd = min(daily_spend_usd, strat_daily_spend_cap_usd)

    # Risk capacity (global portfolio slider 0..1)
    capacity_raw = _risk_capacity_from_components(
        winrate20=winrate,
        rolling_dd=dd,
        realized_pnl_usd=realized_usd,
        equity_usd=equity,
        regime_mult=regime_mult,
        regime_volatility=regime_vol,
        regime_choppy=regime_chop,
    )

    # Strategy plan bias (behind/ahead) → tilt capacity
    plan_bias = 0.0
    if plan_gap_pct != 0.0:
        if plan_gap_pct > 0:
            # ahead of plan → shrink risk, up to -0.3
            plan_bias = -min(abs(plan_gap_pct) / 100.0 * 0.3, 0.3)
        else:
            # behind plan → can gently boost risk, up to +0.2
            plan_bias = min(abs(plan_gap_pct) / 100.0 * 0.2, 0.2)
    capacity_raw = _clamp(capacity_raw + plan_bias, 0.0, 1.0)

    # Optional AI overlay for capacity (quant model learns how much to nudge)
    ai_capacity_delta = 0.0
    if _predict_capacity_adjustment is not None:
        try:
            features = {
                "equity_usd": float(equity),
                "winrate20": float(winrate),
                "rolling_dd": float(dd),
                "realized_pnl_usd": float(realized_usd),
                "regime_mult": float(regime_mult),
                "plan_gap_pct": float(plan_gap_pct),
                "risk_profile": profile_name,
            }
            ai_raw = _predict_capacity_adjustment(features)
            if asyncio.iscoroutine(ai_raw):
                ai_raw = await ai_raw
            ai_capacity_delta = float(ai_raw or 0.0)
        except Exception:
            ai_capacity_delta = 0.0

    capacity_raw = _clamp(capacity_raw + ai_capacity_delta, 0.0, 1.0)

    # NEW: apply regime_gate_factor as final pre-mode gate on capacity
    capacity_raw = _clamp(capacity_raw * regime_gate_factor, 0.0, 1.0)

    capacity, risk_mode, override_mode, halt_reason = await _risk_mode_with_override(
        redis,
        capacity_raw,
        rolling_dd=dd,
    )

    # Capacity exponent adjusted by effective risk profile
    capacity_exp = max(
        0.10,
        RISK_CAPACITY_TICKET_EXP_BASE * profile_cfg["capacity_exp_mult"],
    )

    if capacity <= 0:
        capacity_scale = 0.0
    else:
        capacity_scale = math.pow(capacity, capacity_exp)

    ticket *= capacity_scale
    daily_spend_usd *= capacity_scale

    # Concurrency reduction for very low capacity
    if risk_mode == 0:
        max_conc = 0
    elif risk_mode == 3:
        max_conc = max(0, int(round(max_conc * 0.5)))

    # Sandbox mode: hard clamp risk super low for testing
    if RISK_SANDBOX_MODE:
        ticket = min(ticket, 5.0)
        daily_spend_usd = min(daily_spend_usd, 20.0)
        max_conc = min(max_conc, 1)

    # --- Equity utilization guardrail (quant-style sanity) --- #
    # If equity is non-zero, ensure ticket * max_conc doesn’t exceed the
    # configured fraction of equity. This is a *soft clamp* which scales
    # ticket (and daily_spend) down if needed.
    equity_utilization_pct = 0.0
    if equity > 0 and max_conc > 0:
        potential_notional = ticket * max_conc
        max_notional = equity * (RISK_MAX_EQUITY_UTILIZATION_PCT / 100.0)
        if max_notional > 0 and potential_notional > max_notional:
            scale = max_notional / potential_notional
            ticket *= max(0.0, scale)
            # Also don’t let daily spend exceed that upper budget
            daily_spend_usd = min(daily_spend_usd, max_notional)
        equity_utilization_pct = min(
            100.0,
            (ticket * max_conc) / equity * 100.0 if equity > 0 else 0.0,
        )

    # Round ticket nicely and clamp min/max
    if ticket < 10:
        ticket = max(RISK_MIN_TICKET_USD, round(ticket, 2))
    elif ticket < 100:
        ticket = max(RISK_MIN_TICKET_USD, round(ticket / 5.0) * 5.0)
    else:
        ticket = max(RISK_MIN_TICKET_USD, round(ticket / 10.0) * 10.0)
    ticket = min(ticket, max(RISK_MIN_TICKET_USD, RISK_MAX_TICKET_USD))

    # Apply live overrides from Redis (if present)
    overrides = await _apply_overrides(
        redis,
        ticket_usd=ticket,
        pos_pct_cap=pos_pct_cap,
        max_concurrent=max_conc,
        daily_spend_usd=daily_spend_usd,
    )

    ticket = float(overrides["ticket_usd"])
    pos_pct_cap = float(overrides["pos_pct_cap"])
    max_conc = int(overrides["max_concurrent"])
    daily_spend_usd = float(overrides["daily_spend_usd"])

    # After overrides, recompute equity utilization for diagnostics
    if equity > 0 and max_conc > 0:
        equity_utilization_pct = min(
            100.0,
            (ticket * max_conc) / equity * 100.0,
        )
    else:
        equity_utilization_pct = 0.0

    limits_obj = RiskLimits(
        equity_usd=float(equity),
        ticket_usd=float(ticket),
        pos_pct_cap=float(pos_pct_cap),
        max_concurrent=int(max_conc),
        daily_spend_usd=float(daily_spend_usd),
        winrate20=float(winrate),
        rolling_dd_pct=float(dd),
        regime=regime,
        risk_capacity=float(capacity),
        risk_mode=int(risk_mode),
        risk_mode_override=override_mode,
        regime_risk_multiplier=float(regime_mult),
        regime_gate_factor=float(regime_gate_factor),
        risk_profile=profile_name,
        ai_capacity_delta=float(ai_capacity_delta),
        halt_reason=halt_reason,
        kelly_raw=float(kelly_raw),
        kelly_pos_pct_cap=float(kelly_pos_pct_cap),
        equity_utilization_pct=float(equity_utilization_pct),
    )
    limits = limits_obj.to_dict()

    # --- Push account state into regime_state (account-aware regime) --- #
    try:
        equity_safe = float(equity)
        realized_safe = float(realized_usd)
        daily_pnl_pct = (
            0.0 if equity_safe <= 0 else (realized_safe / equity_safe) * 100.0
        )
        daily_dd_pct = float(dd) * 100.0  # convert fraction -> percent
        consec_losses_for_regime = await _consecutive_losses(redis)

        await regime_state.update_from_account(
            daily_realized_pnl_pct=daily_pnl_pct,
            daily_drawdown_pct=daily_dd_pct,
            consecutive_losses=consec_losses_for_regime,
        )
    except Exception:
        # regime_state is best-effort; never break risk manager
        pass

    # Check if there is a cooldown in effect
    cooldown_until_ts: Optional[float] = None
    if redis:
        try:
            val = await redis.get(RISK_COOLDOWN_UNTIL_KEY)
            if val is not None:
                cooldown_until_ts = float(val)
        except Exception:
            cooldown_until_ts = None

    # Trading window open/closed status (UTC)
    trading_window_open, _tw_reason = _trading_window_status()

    # Publish risk:ctx for FeatureBuilder / ai_buy_scoring + self-learning
    try:
        consec_losses = await _consecutive_losses(redis)
        await _update_risk_context_hash(
            redis,
            limits=limits,
            realized_usd=realized_usd,
            equity_usd=equity,
            winrate20=winrate,
            consec_losses=consec_losses,
            cooldown_until_ts=cooldown_until_ts,
            max_risk_per_trade=strat_max_risk,
            min_sol_balance=strat_min_sol_balance,
            strat_mode=strat_mode_int,
            plan_equity_today_usd=plan_equity_today,
            plan_gap_pct=plan_gap_pct,
            plan_status=plan_status_str,
            ai_capacity_delta=ai_capacity_delta,
            halt_reason=halt_reason,
            kelly_raw=kelly_raw,
            kelly_pos_pct_cap=kelly_pos_pct_cap,
            active_profile_name=profile_name,
            regime_gate_factor=regime_gate_factor,
            equity_utilization_pct=equity_utilization_pct,
            trading_window_open=trading_window_open,
        )
    except Exception:
        pass

    # Broadcast mode changes (once per transition)
    try:
        await _broadcast_risk_mode_change(
            redis=redis,
            new_mode=limits["risk_mode"],
            halt_reason=halt_reason,
            capacity=limits["risk_capacity"],
            winrate20=limits["winrate20"],
            rolling_dd=limits["rolling_dd_pct"],
            realized_usd=realized_usd,
            equity_usd=limits["equity_usd"],
        )
    except Exception:
        pass

    return limits


async def recommend_ticket_usd(redis=None) -> float:
    """
    Convenience wrapper for Strategy, scanners, etc.
    """
    limits = await current_limits(redis)
    return float(limits["ticket_usd"])


# ============================================================================ #
# Risk capacity public APIs (for FeatureBuilder / ai_entry_logic / reinvestor)
# ============================================================================ #

async def risk_capacity_snapshot(redis=None) -> Dict[str, Any]:
    """
    Focused snapshot for FeatureBuilder / ai_buy_scoring / ai_entry_logic /
    reinvestor / regime-aware modules.

    Returns a rich diagnostic dict (see code for full schema).
    """
    if redis is None:
        redis = await _get_redis()

    # Active profile (takes override + auto + regime hint into account)
    profile_name, _profile_cfg = await _effective_risk_profile(redis)

    # For diagnostics, also expose raw override (if any)
    profile_override: Optional[str] = None
    if redis:
        try:
            v = await redis.get(RISK_PROFILE_OVERRIDE_KEY)
            if v:
                profile_override = str(v).strip().lower()
        except Exception:
            profile_override = None

    equity_wallet = await _wallet_equity_usd()
    equity = float(equity_wallet)
    if RISK_EQUITY_OVERRIDE_USD > 0:
        equity = RISK_EQUITY_OVERRIDE_USD

    winrate, dd_fraction = await _metrics(redis)
    realized = await _daily_realized_usd(redis)
    trades_today = await _daily_trades(redis)
    regime = regime_state.get()
    regime_mult = float(regime_state.risk_multiplier())
    consec_losses = await _consecutive_losses(redis)
    equity_hwm = await _equity_hwm(redis)

    # Strategy params for extra context
    try:
        strat_params = await get_strategy_params(redis)
    except Exception:
        strat_params = {}
    strat_max_risk = float(strat_params.get("MAX_RISK_PER_TRADE", RISK_MAX_PER_TRADE))
    strat_min_sol_balance = float(strat_params.get("MIN_SOL_BALANCE", 0.0))
    strat_mode = strat_params.get("STRAT_MODE")
    try:
        strat_mode_int: Optional[int] = (
            int(strat_mode) if strat_mode is not None else None
        )
    except Exception:
        strat_mode_int = None

    plan_equity_today = float(strat_params.get("PLAN_EQUITY_TODAY_USD", 0.0))
    plan_gap_pct = float(strat_params.get("PLAN_GAP_PCT", 0.0))
    plan_status = strat_params.get("PLAN_STATUS")
    try:
        plan_status_str: Optional[str] = (
            str(plan_status) if plan_status is not None else None
        )
    except Exception:
        plan_status_str = None

    # NEW: self-learning caps (for visibility)
    try:
        strat_max_conc_override_raw = strat_params.get("MAX_CONCURRENT_OVERRIDE")
        strat_max_conc_override: Optional[int] = (
            int(strat_max_conc_override_raw)
            if strat_max_conc_override_raw is not None
            else None
        )
        if strat_max_conc_override is not None and strat_max_conc_override <= 0:
            strat_max_conc_override = None
    except Exception:
        strat_max_conc_override = None

    strat_daily_spend_cap_pct = float(strat_params.get("DAILY_SPEND_CAP_PCT", 0.0))
    strat_daily_spend_cap_usd = float(strat_params.get("DAILY_SPEND_CAP_USD", 0.0))

    # Regime features (same basis as current_limits)
    regime_feats = await regime_state.features()
    regime_vol = float(regime_feats.get("regime_volatility", 0.5))
    regime_chop = float(regime_feats.get("regime_choppy_score", 0.0))
    regime_label_numeric = float(regime_feats.get("regime_label_numeric", 0.0))
    regime_stress_score = float(regime_feats.get("regime_stress_score", regime_vol))
    regime_signal_strength = float(
        regime_feats.get(
            "regime_signal_strength",
            regime_feats.get("regime_meme_heat", 0.5),
        )
    )
    regime_entry_gate_factor = float(
        regime_feats.get(
            "regime_entry_gate_factor",
            regime_feats.get("gate_factor", regime_mult),
        )
    )

    # Capacity, with same pipeline as current_limits (minus live overrides)
    capacity_raw = _risk_capacity_from_components(
        winrate20=winrate,
        rolling_dd=dd_fraction,
        realized_pnl_usd=realized,
        equity_usd=equity,
        regime_mult=regime_mult,
        regime_volatility=regime_vol,
        regime_choppy=regime_chop,
    )

    # Plan bias (same logic as current_limits)
    plan_bias = 0.0
    if plan_gap_pct != 0.0:
        if plan_gap_pct > 0:
            plan_bias = -min(abs(plan_gap_pct) / 100.0 * 0.3, 0.3)
        else:
            plan_bias = min(abs(plan_gap_pct) / 100.0 * 0.2, 0.2)
    capacity_raw = _clamp(capacity_raw + plan_bias, 0.0, 1.0)

    # Optional AI overlay
    ai_capacity_delta = 0.0
    if _predict_capacity_adjustment is not None:
        try:
            features = {
                "equity_usd": float(equity),
                "winrate20": float(winrate),
                "rolling_dd": float(dd_fraction),
                "realized_pnl_usd": float(realized),
                "regime_mult": float(regime_mult),
                "plan_gap_pct": float(plan_gap_pct),
                "risk_profile": profile_name,
            }
            ai_raw = _predict_capacity_adjustment(features)
            if asyncio.iscoroutine(ai_raw):
                ai_raw = await ai_raw
            ai_capacity_delta = float(ai_raw or 0.0)
        except Exception:
            ai_capacity_delta = 0.0

    capacity_raw = _clamp(capacity_raw + ai_capacity_delta, 0.0, 1.0)

    capacity, risk_mode, override_mode, halt_reason = await _risk_mode_with_override(
        redis,
        capacity_raw,
        rolling_dd=dd_fraction,
    )

    # Kelly diagnostics, *not* used to recompute equity here
    kelly_raw = 0.0
    kelly_pos_pct_cap = 0.0
    if RISK_KELLY_ENABLED:
        kelly_raw, kelly_pos_pct_cap = _kelly_pos_cap(winrate)

    pnl_pct_fraction = 0.0 if equity <= 0 else (realized / equity)
    pnl_pct = pnl_pct_fraction * 100.0
    dd_pct = dd_fraction * 100.0

    cooldown_active = False
    if redis:
        try:
            val = await redis.get(RISK_COOLDOWN_UNTIL_KEY)
            if val is not None:
                cooldown_active = float(val) > _now_ts()
        except Exception:
            cooldown_active = False

    trading_window_open, tw_reason = _trading_window_status()

    return {
        "risk_capacity": float(capacity),
        "risk_mode": int(risk_mode),
        "risk_mode_override": override_mode,
        "winrate20": float(winrate),
        "rolling_dd_pct": float(dd_fraction),
        "equity_usd": float(equity),
        "equity_hwm_usd": float(equity_hwm),
        "daily_realized_pnl_usd": float(realized),
        "daily_realized_pnl_pct": float(pnl_pct),
        "daily_drawdown_pct": float(dd_pct),
        "regime_label": regime,
        "regime_risk_multiplier": float(regime_mult),
        "regime_label_numeric": float(regime_label_numeric),
        "regime_stress_score": float(regime_stress_score),
        "regime_signal_strength": float(regime_signal_strength),
        "regime_entry_gate_factor": float(regime_entry_gate_factor),
        "consecutive_losses": int(consec_losses),
        "cooldown_active": bool(cooldown_active),
        "max_risk_per_trade": float(strat_max_risk),
        "min_sol_balance": float(strat_min_sol_balance),
        "strat_mode": strat_mode_int,
        "plan_equity_today_usd": float(plan_equity_today),
        "plan_gap_pct": float(plan_gap_pct),
        "plan_status": plan_status_str,
        "strat_daily_spend_cap_pct": float(strat_daily_spend_cap_pct),
        "strat_daily_spend_cap_usd": float(strat_daily_spend_cap_usd),
        "strat_max_concurrent_override": strat_max_conc_override,
        "ai_capacity_delta": float(ai_capacity_delta),
        "halt_reason": halt_reason,
        "daily_trades_count": int(trades_today),
        "daily_trades_cap": int(RISK_MAX_DAILY_TRADES),
        "sandbox_mode": bool(RISK_SANDBOX_MODE),
        "kelly_raw": float(kelly_raw),
        "kelly_pos_pct_cap": float(kelly_pos_pct_cap),
        "equity_override_usd": float(RISK_EQUITY_OVERRIDE_USD),
        "risk_profile": profile_name,
        "risk_profile_env": RISK_PROFILE,
        "risk_profile_override": profile_override,
        "risk_engine_version": RISK_ENGINE_VERSION,
        "trading_window_open": bool(trading_window_open),
        "trading_window_reason": tw_reason,
    }


async def risk_capacity_score(redis=None) -> float:
    """
    Scalar convenience wrapper used by any module that just wants [0,1].
    """
    snap = await risk_capacity_snapshot(redis)
    return float(snap["risk_capacity"])


async def read_risk_ctx_hash(redis=None) -> Dict[str, float]:
    """
    Optional helper: read the already-published risk:ctx hash.

    This lets FeatureBuilder / scanners pull risk context cheaply without
    recomputing limits if they don't need the full `current_limits()` output.

    Returns 0'd defaults if Redis / hash missing.
    """
    if redis is None:
        redis = await _get_redis()
    if not redis:
        return {
            "open_positions": 0.0,
            "max_positions": 0.0,
            "daily_realized_pnl_pct": 0.0,
            "daily_drawdown_pct": 0.0,
            "risk_mode": 1.0,
            "risk_capacity": 1.0,
        }

    try:
        h = await redis.hgetall(RISK_CTX_KEY)
    except Exception:
        h = {}

    def _f(name: str, default: float) -> float:
        try:
            return float(h.get(name, default))
        except Exception:
            return default

    return {
        "open_positions": _f("open_positions", 0.0),
        "max_positions": _f("max_positions", 0.0),
        "daily_realized_pnl_pct": _f("daily_realized_pnl_pct", 0.0),
        "daily_drawdown_pct": _f("daily_drawdown_pct", 0.0),
        "risk_mode": _f("risk_mode", 1.0),
        "risk_capacity": _f("risk_capacity", 1.0),
    }


# ============================================================================ #
# Trade metrics updates
# ============================================================================ #

async def _push_trade_metrics(redis, profit_usd: float, equity_after: float) -> None:
    """
    Update:
      - total trades/wins
      - rolling winrate-20
      - rolling drawdown vs equity HWM
    """
    if not redis:
        return
    try:
        await redis.incr(METRICS_TRADES_KEY)
        if profit_usd > 0:
            await redis.incr(METRICS_WINS_KEY)

        # rolling last-20 (1/0)
        await redis.lpush(METRICS_LAST20_KEY, 1 if profit_usd > 0 else 0)
        await redis.ltrim(METRICS_LAST20_KEY, 0, 19)
        last20 = await redis.lrange(METRICS_LAST20_KEY, 0, 19)
        wins20 = sum(int(x) for x in last20)
        winrate20 = wins20 / max(1, len(last20))
        await redis.set(METRICS_WINRATE20, f"{winrate20:.4f}")

        # rolling DD vs equity HWM (fraction 0..1)
        hwm_raw = await redis.get(EQUITY_HWM_KEY)
        hwm = float(hwm_raw) if hwm_raw else equity_after
        if equity_after > hwm:
            hwm = equity_after
        await redis.set(EQUITY_HWM_KEY, f"{hwm:.2f}")
        dd = 0.0 if hwm <= 0 else (hwm - equity_after) / hwm
        await redis.set(METRICS_ROLLING_DD, f"{dd:.4f}")
    except Exception:
        pass


async def note_trade_result(
    token_address: str,
    *,
    profit_usd: float,
    equity_after_usd: Optional[float] = None,
    meta: Optional[Dict[str, Any]] = None,
    redis=None,
) -> None:
    """
    Central hook for *any* exit path (ai_sell_logic, exit_engine, legacy) to
    update daily realized PnL and risk metrics and push account context
    into regime_state.

    NEW (V3+++):
      - If profit_usd < 0 and consecutive losses ≥ RISK_COOLDOWN_CONSEC_LOSSES,
        we start a cooldown window (RISK_COOLDOWN_MINUTES) where should_allow_new_entry()
        will block new entries.
      - Optionally, if rolling DD% ≥ RISK_DD_COOLDOWN_FLOOR_PCT, we start a
        DD-based cooldown window (RISK_DD_COOLDOWN_MINUTES).
      - We increment daily trade count for use with RISK_MAX_DAILY_TRADES.
    """
    try:
        if redis is None:
            redis = await _get_redis()

        if not redis:
            return

        # 1) update daily realized PnL
        pnl_key = _pnl_key_today()
        await redis.incrbyfloat(pnl_key, float(profit_usd))
        await _set_expire_at_midnight(redis, pnl_key)

        # 2) update HWM / DD / winrate metrics
        if equity_after_usd is None:
            equity_after_usd = await _wallet_equity_usd()
        equity_after_usd = float(equity_after_usd or 0.0)

        await _push_trade_metrics(redis, float(profit_usd), equity_after_usd)

        # 3) daily trades counter (for RISK_MAX_DAILY_TRADES)
        try:
            trades_key = _key_daily("risk:trades_count")
            await redis.incr(trades_key)
            await _set_expire_at_midnight(redis, trades_key)
        except Exception:
            pass

        # 4) consecutive-loss cooldown (if configured and this trade is a loser)
        if RISK_COOLDOWN_CONSEC_LOSSES > 0 and profit_usd < 0:
            try:
                consec_losses = await _consecutive_losses(redis)
                if consec_losses >= RISK_COOLDOWN_CONSEC_LOSSES:
                    cooldown_until = _now_ts() + RISK_COOLDOWN_MINUTES * 60.0
                    await redis.set(RISK_COOLDOWN_UNTIL_KEY, f"{cooldown_until:.1f}")
                    await send_telegram(
                        "⚠️ *RISK COOLDOWN* — consecutive losses threshold hit\n"
                        f"• consecutive_losses={consec_losses}  "
                        f"• cooldown={RISK_COOLDOWN_MINUTES} min"
                    )
            except Exception:
                pass

        # 4b) DD-based cooldown (optional, uses updated METRICS_ROLLING_DD)
        if (
            RISK_DD_COOLDOWN_FLOOR_PCT > 0
            and RISK_DD_COOLDOWN_MINUTES > 0
        ):
            try:
                _, dd_fraction = await _metrics(redis)  # 0..1
                dd_pct = dd_fraction * 100.0
                if dd_pct >= RISK_DD_COOLDOWN_FLOOR_PCT:
                    cooldown_until = _now_ts() + RISK_DD_COOLDOWN_MINUTES * 60.0
                    await redis.set(RISK_COOLDOWN_UNTIL_KEY, f"{cooldown_until:.1f}")
                    await send_telegram(
                        "⚠️ *RISK DD COOLDOWN* — drawdown threshold hit\n"
                        f"• rolling_dd={dd_pct:.2f}% ≥ floor={RISK_DD_COOLDOWN_FLOOR_PCT:.2f}%\n"
                        f"• cooldown={RISK_DD_COOLDOWN_MINUTES} min"
                    )
            except Exception:
                pass

        # 5) push account → regime_state (account-aware regime)
        try:
            winrate20, dd = await _metrics(redis)
            realized_usd = await _daily_realized_usd(redis)
            equity_now = await _wallet_equity_usd()
            equity_now = float(equity_now or 0.0)

            daily_pnl_pct = (
                0.0 if equity_now <= 0 else (realized_usd / equity_now) * 100.0
            )
            daily_dd_pct = float(dd) * 100.0  # fraction -> percent
            consec_losses = await _consecutive_losses(redis)

            await regime_state.update_from_account(
                daily_realized_pnl_pct=daily_pnl_pct,
                daily_drawdown_pct=daily_dd_pct,
                consecutive_losses=consec_losses,
            )
        except Exception:
            # Don't let regime coupling break risk pipeline
            pass

        # 6) optional lightweight log to Notion for analytics
        try:
            await log_to_notion(
                token=token_address,
                symbol=(meta or {}).get("symbol") or token_address[:6],
                balance=(meta or {}).get("amount", 0.0),
                entry=(meta or {}).get("entry", 0.0),
                current=(meta or {}).get("exit", 0.0),
                profit=profit_usd,
                reason=(meta or {}).get("reason", "risk_manager.note_trade_result"),
            )
        except Exception:
            # Don't let analytics break risk pipeline
            pass

        # 7) Structured log_event
        try:
            await log_event(
                "trade_result",
                {
                    "token": token_address,
                    "profit_usd": profit_usd,
                    "equity_after_usd": equity_after_usd,
                    "meta": meta or {},
                    "sandbox_mode": RISK_SANDBOX_MODE,
                },
            )
        except Exception:
            pass

    except Exception as e:
        logger.warning("[RISK] note_trade_result failed for %s: %s", token_address, e)


# ============================================================================ #
# Entry gating
# ============================================================================ #

async def should_allow_new_entry(redis=None) -> bool:
    """
    Gate new entries by:
      - manual block (risk:manual_block)
      - infra halt (from infra_guard via "infra:halt" Redis key)
      - global risk_mode (0=off → no entries)
      - risk_capacity floor (RISK_MIN_CAPACITY_FOR_ENTRY)
      - equity floor (RISK_MIN_EQUITY_USD, optional)
      - daily realized PnL ≥ STOP_AFTER_DAILY_PROFIT_PCT * equity
      - daily realized PnL ≤ -DAILY_LOSS_CAP_PCT * equity (if configured)
      - daily spend budget exhausted
      - daily trade-count cap (RISK_MAX_DAILY_TRADES)
      - consecutive-loss cooldown window (if active)
      - DD-based cooldown window (if active)
      - wallet SOL balance < MIN_SOL_BALANCE from self-learning strategy
      - strategy HARD_HALT flag
      - strategy plan hard-stop when far ahead of plan (optional)
      - regime_state hard risk_off flag
      - NEW: trading window closed (UTC hours + weekdays)

    NOTE: Regime / performance are already reflected in ticket size & concurrency
    via current_limits(), not here.
    """
    if redis is None:
        redis = await _get_redis()

    # Manual block (ops / safety switch)
    if redis:
        try:
            manual = await redis.get(RISK_MANUAL_BLOCK_KEY)
            if str(manual).strip() == "1":
                logger.info("[RISK] risk:manual_block=1 -> blocking new entries")
                return False
        except Exception:
            pass

    limits = await current_limits(redis)
    equity = limits["equity_usd"]
    daily_spend = limits["daily_spend_usd"]
    risk_mode = limits.get("risk_mode", 1)
    capacity = float(limits.get("risk_capacity", 1.0))

    # Also pull strategy for balance floor, plan, and HARD_HALT
    try:
        strat_params = await get_strategy_params(redis)
    except Exception:
        strat_params = {}
    strat_min_sol_balance = float(strat_params.get("MIN_SOL_BALANCE", 0.0))
    plan_gap_pct = float(strat_params.get("PLAN_GAP_PCT", 0.0))

    # Optional hard halt from strategy (self-learning emergency brake)
    if str(strat_params.get("HARD_HALT", "0")).strip().lower() in (
        "1",
        "true",
        "yes",
        "on",
    ):
        logger.info("[RISK] strategy HARD_HALT=1 → blocking new entries")
        return False

    # Plan-aware hard stop when ahead of target curve
    if (
        RISK_PLAN_AHEAD_HARD_STOP_PCT > 0.0
        and plan_gap_pct >= RISK_PLAN_AHEAD_HARD_STOP_PCT
    ):
        logger.info(
            "[RISK] plan_gap_pct %.2f >= hard-stop %.2f → blocking new entries",
            plan_gap_pct,
            RISK_PLAN_AHEAD_HARD_STOP_PCT,
        )
        return False

    # Infra-halt flag
    try:
        if redis:
            halted = await redis.get("infra:halt")
            if halted == "1":
                logger.info("[RISK] infra:halt=1 -> blocking new entries")
                return False
    except Exception:
        pass

    # Global risk_mode = off
    if risk_mode == 0:
        logger.info("[RISK] risk_mode=0 (OFF) -> blocking new entries")
        return False

    # Extra guard: if risk_capacity is below floor, block even if mode>0
    if capacity < RISK_MIN_CAPACITY_FOR_ENTRY:
        logger.info(
            "[RISK] risk_capacity %.3f < floor %.3f -> blocking new entries",
            capacity,
            RISK_MIN_CAPACITY_FOR_ENTRY,
        )
        return False

    # Hard risk_off regime (from regime_state) – extra guardrail
    if regime_state.is_hard_risk_off():
        logger.info("[RISK] regime_state.is_hard_risk_off() -> blocking new entries")
        return False

    # Consecutive-loss or DD-based cooldown
    if redis and (
        (RISK_COOLDOWN_CONSEC_LOSSES > 0 and RISK_COOLDOWN_MINUTES > 0)
        or (RISK_DD_COOLDOWN_FLOOR_PCT > 0 and RISK_DD_COOLDOWN_MINUTES > 0)
    ):
        try:
            val = await redis.get(RISK_COOLDOWN_UNTIL_KEY)
            if val is not None:
                cooldown_until = float(val)
                if cooldown_until > _now_ts():
                    logger.info(
                        "[RISK] cooldown active until %.0f -> blocking new entries",
                        cooldown_until,
                    )
                    return False
        except Exception:
            pass

    # Equity floor guardrail (if configured)
    if RISK_MIN_EQUITY_USD > 0.0 and equity < RISK_MIN_EQUITY_USD:
        logger.info(
            "[RISK] equity_usd %.2f < RISK_MIN_EQUITY_USD %.2f -> blocking new entries",
            equity,
            RISK_MIN_EQUITY_USD,
        )
        return False

    # Daily trades cap (if configured)
    if RISK_MAX_DAILY_TRADES > 0 and redis:
        try:
            trades_today = await _daily_trades(redis)
        except Exception:
            trades_today = 0
        if trades_today >= RISK_MAX_DAILY_TRADES:
            logger.info(
                "[RISK] daily trades cap hit: trades=%d cap=%d",
                trades_today,
                RISK_MAX_DAILY_TRADES,
            )
            return False

    # Trading window (UTC hours + weekdays)
    trading_window_open, reason = _trading_window_status()
    if not trading_window_open:
        logger.info("[RISK] trading window closed: %s", reason)
        return False

    # Stop-after-profit & daily loss cap (day-level)
    realized = await _daily_realized_usd(redis)
    profit_stop_usd = equity * (STOP_AFTER_DAILY_PROFIT_PCT / 100.0)
    loss_cap_usd = (
        equity * (DAILY_LOSS_CAP_PCT / 100.0) if DAILY_LOSS_CAP_PCT > 0 else 0.0
    )

    if realized >= profit_stop_usd > 0:
        logger.info(
            "[RISK] stop-after-profit hit: realized=%.2f, stop=%.2f",
            realized,
            profit_stop_usd,
        )
        return False

    if loss_cap_usd > 0 and realized <= -abs(loss_cap_usd):
        logger.info(
            "[RISK] daily loss cap hit: realized=%.2f, cap=-%.2f",
            realized,
            abs(loss_cap_usd),
        )
        return False

    # Daily spend budget
    if redis:
        budget_key = _key_daily("budget:sniper:spent_usd")
        try:
            spent = await redis.get(budget_key)
            spent_val = float(spent) if spent else 0.0
        except Exception:
            spent_val = 0.0
        if spent_val >= daily_spend > 0:
            logger.info(
                "[RISK] daily spend cap hit: spent=%.2f, cap=%.2f",
                spent_val,
                daily_spend,
            )
            return False

    # MIN_SOL_BALANCE guardrail from self-learning strategy
    if strat_min_sol_balance > 0:
        sol_bal = await _wallet_sol_balance()
        if sol_bal < strat_min_sol_balance:
            logger.info(
                "[RISK] SOL balance %.4f < MIN_SOL_BALANCE %.4f -> blocking new entries",
                sol_bal,
                strat_min_sol_balance,
            )
            return False

    return True


async def entry_gate_debug(redis=None) -> Dict[str, Any]:
    """
    Debug helper: run the same gating logic as should_allow_new_entry(), but
    return a structured breakdown of *why* entries are blocked or allowed.

    This is for:
      - scripts/diagnostics.py
      - healthcheck endpoints
      - "why am I seeing no buys?" Telegram debugging
    """
    if redis is None:
        redis = await _get_redis()

    reasons: List[str] = []
    manual_block = False
    infra_halt = False
    hard_halt = False
    hard_risk_off = False
    cooldown_active = False
    spent_usd = 0.0
    sol_balance = 0.0
    trades_today = 0

    # Active risk profile (for debug)
    profile_name, _profile_cfg = await _effective_risk_profile(redis)

    # Load limits first so we have equity / capacity context
    limits = await current_limits(redis)
    equity = float(limits["equity_usd"])
    daily_spend_cap = float(limits["daily_spend_usd"])
    risk_mode = int(limits.get("risk_mode", 1))
    capacity = float(limits.get("risk_capacity", 1.0))

    # Strategy params
    try:
        strat_params = await get_strategy_params(redis)
    except Exception:
        strat_params = {}
    strat_min_sol_balance = float(strat_params.get("MIN_SOL_BALANCE", 0.0))
    plan_gap_pct = float(strat_params.get("PLAN_GAP_PCT", 0.0))

    # HARD_HALT
    if str(strat_params.get("HARD_HALT", "0")).strip().lower() in (
        "1",
        "true",
        "yes",
        "on",
    ):
        hard_halt = True
        reasons.append("strategy HARD_HALT=1")

    # Plan hard-stop (optional)
    if (
        RISK_PLAN_AHEAD_HARD_STOP_PCT > 0.0
        and plan_gap_pct >= RISK_PLAN_AHEAD_HARD_STOP_PCT
    ):
        reasons.append(
            f"plan_gap_pct {plan_gap_pct:.2f}% >= hard-stop {RISK_PLAN_AHEAD_HARD_STOP_PCT:.2f}%"
        )

    # Manual block
    if redis:
        try:
            v = await redis.get(RISK_MANUAL_BLOCK_KEY)
            manual_block = str(v).strip() == "1"
            if manual_block:
                reasons.append("risk:manual_block=1")
        except Exception:
            manual_block = False

    # Infra halt
    if redis:
        try:
            halted = await redis.get("infra:halt")
            infra_halt = (halted == "1")
            if infra_halt:
                reasons.append("infra:halt=1")
        except Exception:
            infra_halt = False

    # risk_mode
    if risk_mode == 0:
        reasons.append("risk_mode=0 (OFF)")

    # capacity floor
    if capacity < RISK_MIN_CAPACITY_FOR_ENTRY:
        reasons.append(
            f"risk_capacity {capacity:.3f} < floor {RISK_MIN_CAPACITY_FOR_ENTRY:.3f}"
        )

    # regime hard risk-off
    if regime_state.is_hard_risk_off():
        hard_risk_off = True
        reasons.append("regime_state.is_hard_risk_off()")

    # cooldown
    if redis and (
        (RISK_COOLDOWN_CONSEC_LOSSES > 0 and RISK_COOLDOWN_MINUTES > 0)
        or (RISK_DD_COOLDOWN_FLOOR_PCT > 0 and RISK_DD_COOLDOWN_MINUTES > 0)
    ):
        try:
            v = await redis.get(RISK_COOLDOWN_UNTIL_KEY)
            if v is not None:
                ts = float(v)
                cooldown_active = ts > _now_ts()
                if cooldown_active:
                    reasons.append("loss/DD cooldown active")
        except Exception:
            cooldown_active = False

    # daily realized PnL vs caps
    realized = await _daily_realized_usd(redis)
    profit_stop_usd = equity * (STOP_AFTER_DAILY_PROFIT_PCT / 100.0)
    loss_cap_usd = (
        equity * (DAILY_LOSS_CAP_PCT / 100.0) if DAILY_LOSS_CAP_PCT > 0 else 0.0
    )
    if realized >= profit_stop_usd > 0:
        reasons.append(
            f"stop-after-profit hit ({realized:.2f} ≥ {profit_stop_usd:.2f})"
        )
    if loss_cap_usd > 0 and realized <= -abs(loss_cap_usd):
        reasons.append(
            f"daily loss cap hit ({realized:.2f} ≤ -{abs(loss_cap_usd):.2f})"
        )

    # daily spend vs cap
    if redis:
        budget_key = _key_daily("budget:sniper:spent_usd")
        try:
            spent_raw = await redis.get(budget_key)
            spent_usd = float(spent_raw) if spent_raw else 0.0
        except Exception:
            spent_usd = 0.0
        if spent_usd >= daily_spend_cap > 0:
            reasons.append(
                f"daily spend cap hit (spent {spent_usd:.2f} ≥ cap {daily_spend_cap:.2f})"
            )

    # SOL balance vs MIN_SOL_BALANCE
    if strat_min_sol_balance > 0:
        sol_balance = await _wallet_sol_balance()
        if sol_balance < strat_min_sol_balance:
            reasons.append(
                f"SOL balance {sol_balance:.4f} < MIN_SOL_BALANCE {strat_min_sol_balance:.4f}"
            )

    # equity floor
    if RISK_MIN_EQUITY_USD > 0.0 and equity < RISK_MIN_EQUITY_USD:
        reasons.append(
            f"equity_usd {equity:.2f} < RISK_MIN_EQUITY_USD {RISK_MIN_EQUITY_USD:.2f}"
        )

    # daily trades cap
    if RISK_MAX_DAILY_TRADES > 0 and redis:
        try:
            trades_today = await _daily_trades(redis)
        except Exception:
            trades_today = 0
        if trades_today >= RISK_MAX_DAILY_TRADES:
            reasons.append(
                f"daily trades cap hit ({trades_today} ≥ cap {RISK_MAX_DAILY_TRADES})"
            )

    # Trading window
    trading_window_open, tw_reason = _trading_window_status()
    if not trading_window_open:
        reasons.append(f"trading window closed ({tw_reason})")

    allowed = len(reasons) == 0

    # Regime features for debug (including gate factor)
    regime_feats = await regime_state.features()
    regime_entry_gate_factor = float(
        regime_feats.get(
            "regime_entry_gate_factor",
            regime_feats.get("gate_factor", regime_feats.get("regime_risk_multiplier", 1.0)),
        )
    )

    return {
        "allowed": bool(allowed),
        "reasons_blocking": reasons,
        "limits": limits,
        "daily_realized_usd": float(realized),
        "profit_stop_usd": float(profit_stop_usd),
        "loss_cap_usd": float(loss_cap_usd),
        "spent_usd": float(spent_usd),
        "sol_balance": float(sol_balance),
        "cooldown_active": bool(cooldown_active),
        "manual_block": bool(manual_block),
        "infra_halt": bool(infra_halt),
        "hard_halt": bool(hard_halt),
        "hard_risk_off": bool(hard_risk_off),
        "plan_gap_pct": float(plan_gap_pct),
        "plan_ahead_hard_stop": float(RISK_PLAN_AHEAD_HARD_STOP_PCT),
        "trade_count_today": int(trades_today),
        "trade_cap_today": int(RISK_MAX_DAILY_TRADES),
        "equity_floor_usd": float(RISK_MIN_EQUITY_USD),
        "sandbox_mode": bool(RISK_SANDBOX_MODE),
        "risk_profile": profile_name,
        "risk_profile_env": RISK_PROFILE,
        "regime_entry_gate_factor": float(regime_entry_gate_factor),
        "trading_window_open": bool(trading_window_open),
        "trading_window_reason": tw_reason,
        "risk_engine_version": RISK_ENGINE_VERSION,
    }


async def note_spend(redis, usd: float) -> None:
    """
    Increment daily spend; create the key with TTL to midnight if missing.
    """
    if redis is None:
        redis = await _get_redis()
    if not redis:
        return
    try:
        budget_key = _key_daily("budget:sniper:spent_usd")
        await redis.incrbyfloat(budget_key, float(usd))
        await _set_expire_at_midnight(redis, budget_key)
        try:
            await log_event(
                "risk_spend",
                {"usd": float(usd), "sandbox_mode": RISK_SANDBOX_MODE},
            )
        except Exception:
            pass
    except Exception:
        pass


async def risk_budget_summary(redis=None) -> Dict[str, Any]:
    """
    Lightweight risk budget summary for dashboards / diagnostics.

    Returns:
      {
        "equity_usd": float,
        "daily_spend_cap_usd": float,
        "daily_spent_usd": float,
        "daily_spend_remaining_usd": float,
        "daily_spend_used_pct": float,
        "sandbox_mode": bool,
        "trading_window_open": bool,
      }
    """
    if redis is None:
        redis = await _get_redis()
    limits = await current_limits(redis)
    equity = float(limits["equity_usd"])
    cap = float(limits["daily_spend_usd"])
    spent = await _daily_spent_usd(redis)
    remaining = max(0.0, cap - spent)
    used_pct = (spent / cap) * 100.0 if cap > 0 else 0.0
    trading_window_open, _tw_reason = _trading_window_status()
    return {
        "equity_usd": equity,
        "daily_spend_cap_usd": cap,
        "daily_spent_usd": spent,
        "daily_spend_remaining_usd": remaining,
        "daily_spend_used_pct": used_pct,
        "sandbox_mode": bool(RISK_SANDBOX_MODE),
        "trading_window_open": bool(trading_window_open),
    }


async def risk_snapshot(redis=None) -> Dict[str, Any]:
    """
    Diagnostic snapshot combining regime, limits and performance.

    Useful for scripts/diagnostics.py, /healthcheck endpoints, dashboards, etc.
    """
    if redis is None:
        redis = await _get_redis()
    limits = await current_limits(redis)
    realized = await _daily_realized_usd(redis)
    winrate20, dd_fraction = await _metrics(redis)
    regime_feats = await regime_state.features()
    equity_hwm = await _equity_hwm(redis)

    # Strategy params for debugging / dashboards
    try:
        strat_params = await get_strategy_params(redis)
    except Exception:
        strat_params = {}
    strat_max_risk = float(strat_params.get("MAX_RISK_PER_TRADE", RISK_MAX_PER_TRADE))
    strat_min_sol_balance = float(strat_params.get("MIN_SOL_BALANCE", 0.0))
    strat_mode = strat_params.get("STRAT_MODE")
    plan_equity_today = float(strat_params.get("PLAN_EQUITY_TODAY_USD", 0.0))
    plan_gap_pct = float(strat_params.get("PLAN_GAP_PCT", 0.0))
    plan_status = strat_params.get("PLAN_STATUS")

    # NEW: self-learning caps for visibility
    strat_daily_spend_cap_pct = float(strat_params.get("DAILY_SPEND_CAP_PCT", 0.0))
    strat_daily_spend_cap_usd = float(strat_params.get("DAILY_SPEND_CAP_USD", 0.0))
    try:
        strat_max_conc_override_raw = strat_params.get("MAX_CONCURRENT_OVERRIDE")
        strat_max_conc_override: Optional[int] = (
            int(strat_max_conc_override_raw)
            if strat_max_conc_override_raw is not None
            else None
        )
        if strat_max_conc_override is not None and strat_max_conc_override <= 0:
            strat_max_conc_override = None
    except Exception:
        strat_max_conc_override = None

    # Active risk profile + override visibility
    profile_name, _profile_cfg = await _effective_risk_profile(redis)
    profile_override: Optional[str] = None
    if redis:
        try:
            v = await redis.get(RISK_PROFILE_OVERRIDE_KEY)
            if v:
                profile_override = str(v).strip().lower()
        except Exception:
            profile_override = None

    # Pull AI capacity delta / cooldown / manual block from redis
    ai_capacity_delta = float(limits.get("ai_capacity_delta", 0.0))
    cooldown_remaining = 0.0
    manual_block = False
    spent_usd = 0.0
    halt_reason = limits.get("halt_reason")
    trades_today = 0
    kelly_raw = float(limits.get("kelly_raw", 0.0))
    kelly_pos_pct_cap = float(limits.get("kelly_pos_pct_cap", 0.0))
    equity_utilization_pct = float(limits.get("equity_utilization_pct", 0.0))
    if redis:
        try:
            if halt_reason is None:
                saved_reason = await redis.get(RISK_HALT_REASON_KEY)
                if saved_reason:
                    halt_reason = saved_reason
            manual_block = str(await redis.get(RISK_MANUAL_BLOCK_KEY) or "").strip() == "1"
            val = await redis.get(RISK_COOLDOWN_UNTIL_KEY)
            if val is not None:
                ts = float(val)
                if ts > _now_ts():
                    cooldown_remaining = ts - _now_ts()
            spent_usd = await _daily_spent_usd(redis)
            trades_today = await _daily_trades(redis)
        except Exception:
            cooldown_remaining = 0.0
            spent_usd = 0.0
            trades_today = 0

    cap_spend = float(limits["daily_spend_usd"])
    spend_remaining = max(0.0, cap_spend - spent_usd)
    spend_used_pct = (spent_usd / cap_spend) * 100.0 if cap_spend > 0 else 0.0

    # Regime entry gate factor (same as risk_capacity_snapshot)
    regime_entry_gate_factor = float(
        regime_feats.get(
            "regime_entry_gate_factor",
            regime_feats.get("gate_factor", regime_feats.get("regime_risk_multiplier", 1.0)),
        )
    )

    trading_window_open, tw_reason = _trading_window_status()

    snap: Dict[str, Any] = {
        "equity_usd": limits["equity_usd"],
        "equity_hwm_usd": equity_hwm,
        "ticket_usd": limits["ticket_usd"],
        "pos_pct_cap": limits["pos_pct_cap"],
        "max_concurrent": limits["max_concurrent"],
        "daily_spend_usd": limits["daily_spend_usd"],
        "daily_spent_usd": spent_usd,
        "daily_spend_remaining_usd": spend_remaining,
        "daily_spend_used_pct": spend_used_pct,
        "daily_realized_pnl_usd": realized,
        "winrate20": winrate20,
        "rolling_dd_pct": dd_fraction,
        "regime_label": limits["regime"],
        "regime_risk_mode": regime_feats.get("regime_risk_mode", 1.0),
        "regime_risk_multiplier": regime_feats.get("regime_risk_multiplier", 1.0),
        "regime_trend": regime_feats.get("regime_trend", 0.0),
        "regime_volatility": regime_feats.get("regime_volatility", 0.0),
        "regime_liquidity": regime_feats.get("regime_liquidity", 0.5),
        "regime_manipulation_risk": regime_feats.get("regime_manipulation_risk", 0.0),
        "regime_choppy_score": regime_feats.get("regime_choppy_score", 0.0),
        "regime_confidence": regime_feats.get("regime_confidence", 0.5),
        "regime_meme_heat": regime_feats.get("regime_meme_heat", 0.5),
        "regime_entry_gate_factor": regime_entry_gate_factor,
        "risk_capacity": limits.get("risk_capacity", 1.0),
        "risk_mode": limits.get("risk_mode", 1),
        "risk_mode_override": limits.get("risk_mode_override"),
        "risk_profile": profile_name,            # ACTIVE
        "risk_profile_env": RISK_PROFILE,        # ENV default
        "risk_profile_override": profile_override,
        "ai_capacity_delta": ai_capacity_delta,
        "strat_max_risk_per_trade": strat_max_risk,
        "strat_min_sol_balance": strat_min_sol_balance,
        "strat_mode": strat_mode,
        "plan_equity_today_usd": plan_equity_today,
        "plan_gap_pct": plan_gap_pct,
        "plan_status": plan_status,
        "manual_block": manual_block,
        "cooldown_remaining_sec": cooldown_remaining,
        "halt_reason": halt_reason,
        "plan_ahead_hard_stop_pct": RISK_PLAN_AHEAD_HARD_STOP_PCT,
        "dd_cooldown_floor_pct": RISK_DD_COOLDOWN_FLOOR_PCT,
        "dd_cooldown_minutes": RISK_DD_COOLDOWN_MINUTES,
        "strat_daily_spend_cap_pct": strat_daily_spend_cap_pct,
        "strat_daily_spend_cap_usd": strat_daily_spend_cap_usd,
        "strat_max_concurrent_override": strat_max_conc_override,
        "daily_trades_cap": RISK_MAX_DAILY_TRADES,
        "daily_trades_count": trades_today,
        "equity_floor_usd": RISK_MIN_EQUITY_USD,
        "sandbox_mode": bool(RISK_SANDBOX_MODE),
        "kelly_raw": kelly_raw,
        "kelly_pos_pct_cap": kelly_pos_pct_cap,
        "equity_override_usd": RISK_EQUITY_OVERRIDE_USD,
        "equity_utilization_pct": equity_utilization_pct,
        "risk_engine_version": RISK_ENGINE_VERSION,
        "trading_window_open": bool(trading_window_open),
        "trading_window_reason": tw_reason,
    }
    return snap


# ============================================================================ #
# Sell-side logic (per-position legacy guardrail)
# ============================================================================ #

def _cooldown_ok(mint: str) -> bool:
    last = _COOLDOWNS.get(mint, 0.0)
    now = _now_ts()
    if now - last >= ALERT_COOLDOWN_SECONDS:
        _COOLDOWNS[mint] = now
        return True
    return False


async def _find_wallet_token_balance(mint: str) -> float:
    if not WALLET_PUBKEY:
        logger.warning("WALLET_PUBKEY not set; cannot query wallet token balances.")
        return 0.0
    try:
        tokens = await get_wallet_tokens(WALLET_PUBKEY)
        for t in tokens or []:
            if t.get("mint") == mint:
                return float(t.get("balance", 0.0))
    except Exception as e:
        logger.warning("get_wallet_tokens failed: %s", e)
    return 0.0


def _parse_buy_time(position: Dict[str, Any]) -> Optional[datetime]:
    ts = position.get("timestamp")
    if not ts:
        return None
    try:
        ts_norm = str(ts).replace("Z", "+00:00")
        dt = datetime.fromisoformat(ts_norm)
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except Exception:
        return None


async def _token_price_in_sol(token_address: str) -> float:
    """
    Convert token USD price to SOL-based price, so we stay consistent with
    older positions that stored buy_price in SOL.

    token_price_sol = token_price_usd / sol_usd
    """
    try:
        px_usd = await get_token_price(token_address)
        if px_usd <= 0:
            return 0.0
        sol_usd = await convert_sol_to_usd_amount(1.0)
        if sol_usd <= 0:
            return 0.0
        return px_usd / sol_usd
    except Exception:
        return 0.0


async def _sell_all_token(
    mint: str,
    token_balance: float,
    slippage: float,
) -> Tuple[bool, str]:
    if token_balance <= 0:
        return False, ""
    try:
        ok, tx_sig = await swap_token_jupiter(
            mint,
            SOL_MINT,
            amount_float=token_balance,
            buy=False,
            slippage=slippage,
        )
        return ok, tx_sig
    except TypeError:
        # fallback to older signature
        ok, tx_sig = await swap_token_jupiter(
            mint,
            SOL_MINT,
            token_balance,
            False,
            slippage,
        )
        return ok, tx_sig


async def handle_position(
    token_address: str,
    position: Dict[str, Any],
    redis=None,
) -> None:
    """
    Core per-position risk logic (legacy, non-AI path):

      - trailing stop maintenance
      - stop-loss / target-profit / max-hold exits
      - sell via Jupiter
      - metrics + PnL logging + reinvest queue
      - self-learning strategy closure hook

    RISK_LEGACY_SELL_ENABLED can be set to 0/false to effectively turn this
    off and let utils_pkg.exit_engine + ai_sell_logic handle all exits.
    """
    if not RISK_LEGACY_SELL_ENABLED:
        return

    try:
        if redis is None:
            redis = await _get_redis()

        params = await get_strategy_params(redis)
        stop_loss_pct = float(
            params.get("STOP_LOSS_PERCENT", DEFAULT_STOP_LOSS_PERCENT)
        )
        target_profit_pct = float(
            params.get("TARGET_PROFIT_PERCENT", DEFAULT_TARGET_PROFIT_PERCENT)
        )
        max_hold_minutes = int(
            params.get("MAX_HOLD_TIME_MINUTES", DEFAULT_MAX_HOLD_MINUTES)
        )
        slippage = float(params.get("SLIPPAGE", DEFAULT_SLIPPAGE))
        trailing_on = bool(
            params.get("TRAILING_STOP_ENABLED", TRAILING_STOP_ENABLED)
        )

        buy_price = float(position.get("buy_price", 0.0))  # price in SOL-equivalent
        if buy_price <= 0:
            return

        # Price in SOL-equivalent using USD+SOL conversions
        current_price = await _token_price_in_sol(token_address)
        if current_price <= 0:
            return

        # Maintain trailing stop on new highs
        trailing_stop = float(
            position.get(
                "trailing_stop",
                buy_price * (1 - stop_loss_pct / 100.0),
            )
        )
        if trailing_on and current_price > buy_price:
            new_trailing = current_price * (1 - stop_loss_pct / 100.0)
            if new_trailing > trailing_stop:
                trailing_stop = new_trailing
                position["trailing_stop"] = trailing_stop
                save_position(token_address, position)
                logger.info(
                    "[TSL] %s raised trailing_stop -> %.10f",
                    token_address,
                    trailing_stop,
                )

        pct_change = ((current_price - buy_price) / buy_price) * 100.0
        buy_dt = _parse_buy_time(position)
        hold_minutes = 0.0
        if buy_dt:
            hold_minutes = (
                datetime.now(timezone.utc) - buy_dt
            ).total_seconds() / 60.0

        # Exit checks
        do_sell = False
        reason = ""

        if current_price <= trailing_stop:
            do_sell = True
            reason = f"Trailing/Stop-Loss hit (≤ {stop_loss_pct:.1f}%)"

        if (not do_sell) and target_profit_pct > 0 and pct_change >= target_profit_pct:
            do_sell = True
            reason = f"Target profit reached (≥ {target_profit_pct:.1f}%)"

        if (not do_sell) and max_hold_minutes > 0 and hold_minutes >= max_hold_minutes:
            do_sell = True
            reason = f"Max hold time exceeded ({hold_minutes:.0f}m)"

        # If no sell trigger or alert cooldown not satisfied, bail
        if not do_sell or not _cooldown_ok(token_address):
            return

        token_balance = await _find_wallet_token_balance(token_address)
        if token_balance <= 0:
            logger.info(
                "[SELL] %s: no wallet balance; assuming already sold.",
                token_address,
            )
            remove_position(token_address)
            return

        ok, tx_sig = await _sell_all_token(token_address, token_balance, slippage)
        if not ok:
            logger.warning("[SELL] Jupiter sell failed for %s", token_address)
            return

        # Price is in SOL-equivalent; convert everything to USD for metrics
        proceeds_sol = token_balance * current_price
        usd_proceeds = await convert_sol_to_usd_amount(proceeds_sol)
        sol_usd_1 = await convert_sol_to_usd_amount(1.0)
        profit_usd = (
            (current_price - buy_price)
            * token_balance
            * float(sol_usd_1 or 0.0)
        )

        # Alerts / Notion log
        await send_telegram(
            f"🔴 Sold {token_address[:6]}… — {reason}\n"
            f"• Δ {pct_change:+.2f}%  • Proceeds ≈ {proceeds_sol:.4f} SOL (${usd_proceeds:.2f})\n"
            f"• Entry {buy_price:.10f} → Exit {current_price:.10f}\n"
            f"• Tx: {tx_sig}"
        )
        try:
            await log_to_notion(
                token=token_address,
                symbol=token_address[:6],
                balance=token_balance,
                entry=buy_price,
                current=current_price,
                profit=(current_price - buy_price) * token_balance,
                reason=reason,
            )
        except Exception:
            pass

        # Self-learning strategy closure hook
        try:
            sell_time = datetime.now(timezone.utc)
            await process_trade_closure(
                token_address=token_address,
                buy_price=buy_price,
                sell_price=current_price,
                amount=token_balance,
                partial=False,
                timestamp=sell_time.isoformat(),
            )
        except Exception as e:
            logger.debug("process_trade_closure failed: %s", e)

        # Metrics & realized PnL via central hook
        try:
            await note_trade_result(
                token_address,
                profit_usd=float(profit_usd),
                equity_after_usd=None,  # let note_trade_result recompute
                meta={
                    "symbol": token_address[:6],
                    "amount": token_balance,
                    "entry": buy_price,
                    "exit": current_price,
                    "reason": reason,
                },
                redis=redis,
            )
        except Exception:
            pass

        remove_position(token_address)

        # Queue reinvestment (do local import to avoid circulars)
        try:
            from utils_pkg import reinvestor as _reinvestor  # type: ignore
            await _reinvestor.queue_reinvest(
                proceeds_sol,
                meta={
                    "from": "risk_manager",
                    "reason": reason,
                    "pct_change": pct_change,
                },
                redis=redis,
            )
        except Exception as e:
            logger.warning("Queue reinvest failed: %s", e)

    except Exception as e:
        logger.error("[RISK] Error handling %s: %s", token_address, e, exc_info=True)


# ============================================================================ #
# Loops
# ============================================================================ #

async def monitor_positions_loop(
    redis=None,
    interval: Optional[int] = None,
    stop_evt: Optional[asyncio.Event] = None,
) -> None:
    """
    Generic monitor loop (can be used standalone).

    If stop_evt is provided, exits when stop_evt.is_set().
    """
    if redis is None:
        redis = await _get_redis()
    sleep_s = int(interval or POSITION_CHECK_INTERVAL)
    logger.info("📊 Risk monitor started (interval=%ss, sandbox=%s, engine=%s)",
                sleep_s, RISK_SANDBOX_MODE, RISK_ENGINE_VERSION)
    while True:
        if stop_evt is not None and stop_evt.is_set():
            logger.info("📊 Risk monitor stopping on stop_evt.")
            break
        try:
            positions = load_positions()
            if positions and RISK_LEGACY_SELL_ENABLED:
                await asyncio.gather(
                    *(
                        handle_position(mint, pos, redis=redis)
                        for mint, pos in positions.items()
                    )
                )
        except Exception as e:
            logger.error("[RISK] monitor loop error: %s", e, exc_info=True)
        await asyncio.sleep(sleep_s)


async def risk_manager_loop(stop_evt: asyncio.Event) -> None:
    """
    Entry point used by scripts/main.py:

        from utils_pkg.risk_manager import risk_manager_loop
        ...
        asyncio.create_task(risk_manager_loop(stop_evt), name="risk.manager")

    Thin wrapper around monitor_positions_loop with its own poll interval knob.
    It also refreshes `current_limits` periodically so the risk:ctx hash stays
    hot even when there are no open positions.
    """
    poll = float(os.getenv("RISK_MANAGER_POLL_SEC", str(POSITION_CHECK_INTERVAL)))
    redis = await _get_redis()
    logger.info("📊 risk_manager_loop started (poll=%ss, sandbox=%s, engine=%s)",
                poll, RISK_SANDBOX_MODE, RISK_ENGINE_VERSION)
    while not stop_evt.is_set():
        try:
            # 1) keep risk context updated (even if no new entries)
            try:
                await current_limits(redis)
            except Exception:
                pass

            # 2) legacy per-position guardrail
            positions = load_positions()
            if positions and RISK_LEGACY_SELL_ENABLED:
                await asyncio.gather(
                    *(
                        handle_position(mint, pos, redis=redis)
                        for mint, pos in positions.items()
                    )
                )
        except Exception as e:
            logger.error("[RISK] risk_manager_loop error: %s", e, exc_info=True)

        # Sleep in small chunks so we respond promptly to stop_evt
        remaining = poll
        while remaining > 0 and not stop_evt.is_set():
            step = min(remaining, 1.0)
            await asyncio.sleep(step)
            remaining -= step
    logger.info("📊 risk_manager_loop exiting (stop_evt set, sandbox=%s, engine=%s).",
                RISK_SANDBOX_MODE, RISK_ENGINE_VERSION)


# ============================================================================ #
# Ops helpers (for dashboards / scripts)
# ============================================================================ #

async def set_manual_block(on: bool = True, redis=None) -> None:
    """
    Set or clear the global manual entry block (risk:manual_block).
    """
    if redis is None:
        redis = await _get_redis()
    if not redis:
        return
    try:
        if on:
            await redis.set(RISK_MANUAL_BLOCK_KEY, "1")
        else:
            await redis.delete(RISK_MANUAL_BLOCK_KEY)
    except Exception:
        pass


async def clear_manual_block(redis=None) -> None:
    """
    Convenience wrapper around set_manual_block(False).
    """
    await set_manual_block(False, redis=redis)


async def clear_risk_cooldown(redis=None) -> None:
    """
    Clear any active loss/DD cooldown (risk:cooldown_until_ts).
    """
    if redis is None:
        redis = await _get_redis()
    if not redis:
        return
    try:
        await redis.delete(RISK_COOLDOWN_UNTIL_KEY)
    except Exception:
        pass


async def force_risk_mode(mode: Optional[int], redis=None) -> None:
    """
    Force a specific risk_mode via Redis override (0..3),
    or clear override if mode is None.
    """
    if redis is None:
        redis = await _get_redis()
    if not redis:
        return
    try:
        if mode is None:
            await redis.delete(OVERRIDE_MODE_KEY)
        else:
            if mode not in (0, 1, 2, 3):
                raise ValueError("mode must be one of 0,1,2,3 or None")
            await redis.set(OVERRIDE_MODE_KEY, str(int(mode)))
    except Exception:
        pass


async def set_risk_profile_override(profile: Optional[str], redis=None) -> None:
    """
    Override RISK_PROFILE at runtime via Redis.

    profile:
      - 'conservative' / 'balanced' / 'aggressive' / 'auto'
      - None -> clear override and fall back to env RISK_PROFILE.
    """
    if redis is None:
        redis = await _get_redis()
    if not redis:
        return
    try:
        if profile is None:
            await redis.delete(RISK_PROFILE_OVERRIDE_KEY)
        else:
            p = str(profile).strip().lower()
            if p not in (*RISK_PROFILE_CONFIG.keys(), "auto"):
                raise ValueError(
                    f"invalid profile {profile!r}; expected one of "
                    f"{list(RISK_PROFILE_CONFIG.keys()) + ['auto']}"
                )
            await redis.set(RISK_PROFILE_OVERRIDE_KEY, p)
    except Exception:
        pass


# ============================================================================ #
# Aggregate health helper (NEW)
# ============================================================================ #

async def risk_health(redis=None) -> Dict[str, Any]:
    """
    High-level health view for dashboards / CLI / alerts.

    Combines:
      • risk_snapshot()  → regime + limits + performance
      • entry_gate_debug() → whether entries are allowed + reasons

    Schema:
      {
        "snapshot": { ... },       # output of risk_snapshot()
        "entry_gate": { ... },     # output of entry_gate_debug()
      }
    """
    if redis is None:
        redis = await _get_redis()
    snap = await risk_snapshot(redis)
    gate = await entry_gate_debug(redis)
    return {
        "snapshot": snap,
        "entry_gate": gate,
    }


# ============================================================================ #
# CLI / manual run
# ============================================================================ #

if __name__ == "__main__":
    # Standalone: python -m utils_pkg.risk_manager
    asyncio.run(monitor_positions_loop())
