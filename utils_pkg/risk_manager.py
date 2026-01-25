from __future__ import annotations

"""
utils_pkg/risk_manager.py

Quant-Grade Portfolio Risk Manager (V3+++ / God-Tier / Phase 0–7)
=================================================================

This module is the *canonical* portfolio governance layer for ALL entry paths.

Primary guarantees
------------------
1) **Instant deterministic kill-switch (fast path)**
   Checked on *every* entry gate call (no cache dependency):
     - redis key:  risk:halt == "1"
     - redis key:  risk:manual_block == "1"
     - redis key:  infra:halt == "1"
     - strategy param: HARD_HALT == true-ish

2) **Governance**
   - Daily profit stop (taper/stop)
   - Daily loss cap (hard stop)
   - Daily spend cap (hard stop + atomic reservation helpers)
   - Max concurrent positions
   - Optional max exposure (open notional cap)
   - Cooldowns: loss streak / drawdown / per-mint anti-chop cooldown

3) **Production wiring + observability**
   - Uses utils_pkg.core_utils shared redis/alerts if available
   - Publishes explainable risk context to redis hash (risk:ctx)
   - Entry gate returns explicit reasons (no silent “no buys”)
   - Import-safe fallbacks: if optional modules not present, still runs

Integration contract (entry path)
---------------------------------
Entry executor SHOULD:
  A) gate = await entry_gate_debug(mint=..., pool_depth_usd=..., projected_ticket_usd=...)
     if not gate["allowed"]:
         skip + log gate["reasons_blocking"]

  B) limits = gate["limits"]
     ticket_usd = gate["intended_ticket_usd_for_exposure"] or limits["ticket_usd"]

  C) Immediately BEFORE sending BUY (race-safe):
       ok = await reserve_daily_entries_slot(redis)
       ok = ok and await reserve_entry_budget(ticket_usd, redis)
       if not ok: skip
       await note_entry_attempt(mint=..., ticket_usd=..., ...)

     If BUY fails after reserving:
       await release_entry_budget(ticket_usd, redis)

On SELL / close_position:
  - Exit engine MUST call note_trade_result(..., trade_id=...) exactly once.
    trade_id enables idempotency (prevents double counting).

"""

import asyncio
import contextlib
import hashlib
import json
import logging
import math
import os
import time
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

# -----------------------------------------------------------------------------
# Logging
# -----------------------------------------------------------------------------
try:
    from logging_setup import get_logger  # type: ignore
except Exception:  # pragma: no cover
    get_logger = None  # type: ignore

log = get_logger(__name__) if get_logger else logging.getLogger("risk_manager")

RISK_ENGINE_VERSION = "v3+++_godtier_phase0_7"
SOL_MINT = "So11111111111111111111111111111111111111112"

# -----------------------------------------------------------------------------
# Optional redis asyncio
# -----------------------------------------------------------------------------
try:  # pragma: no cover
    import redis.asyncio as aioredis  # type: ignore[import-not-found]
except Exception:  # pragma: no cover
    aioredis = None  # type: ignore[assignment]

# -----------------------------------------------------------------------------
# Core deps
# -----------------------------------------------------------------------------
try:
    from utils_pkg import core_utils as cu  # type: ignore
except Exception as err:  # pragma: no cover
    raise RuntimeError(f"risk_manager requires utils_pkg.core_utils: {err}") from err

try:
    from utils_pkg.regime import regime_state  # type: ignore
except Exception as err:  # pragma: no cover
    raise RuntimeError(f"risk_manager requires utils_pkg.regime.regime_state: {err}") from err

# Self-learning strategy (best effort)
try:
    from utils_pkg.self_learning_ai_strategy import (  # type: ignore
        get_strategy_params,
        process_trade_closure,
    )
except Exception:  # pragma: no cover

    async def get_strategy_params(redis=None) -> dict[str, Any]:  # type: ignore
        return {}

    async def process_trade_closure(*_: Any, **__: Any) -> None:  # type: ignore
        return


# Optional AI overlay
try:  # pragma: no cover
    from utils_pkg.ai_risk_model import (
        predict_capacity_adjustment as _predict_capacity_adjustment,  # type: ignore
    )
except Exception:  # pragma: no cover
    _predict_capacity_adjustment = None  # type: ignore[assignment]

# Optional structured event logger
try:
    from utils_pkg.data_logger import log_event  # type: ignore
except Exception:  # pragma: no cover

    async def log_event(*_: Any, **__: Any) -> None:  # type: ignore
        return


__all__ = [
    "clear_manual_block",
    "clear_mint_cooldown",
    "clear_risk_cooldown",
    "ctx_digest",
    # limits/sizing/gating
    "current_limits",
    "entry_gate_debug",
    "equity_and_ticket",
    "estimate_equity_usd",
    "force_risk_mode",
    # ops helpers (halt/block/overrides)
    "halt_trading",
    "invalidate_limits_cache",
    "liq_aware_ticket_usd",
    # loops
    "monitor_positions_loop",
    "note_entry_attempt",
    "note_spend",
    # trade ingestion
    "note_trade_result",
    "pair_limits_for_depth",
    "read_risk_ctx_hash",
    "recommend_ticket_usd",
    "reinvestor_risk_view",
    "release_entry_budget",
    "reserve_daily_entries_slot",
    # reservations
    "reserve_entry_budget",
    "resume_trading",
    "risk_budget_summary",
    "risk_capacity_score",
    "risk_capacity_snapshot",
    # health
    "risk_health",
    "risk_manager_loop",
    "risk_snapshot",
    "set_manual_block",
    "set_mint_cooldown",
    "set_risk_profile_override",
    "should_allow_new_entry",
]


# =============================================================================
# ENV helpers
# =============================================================================
def _env_bool(key: str, default: bool = False) -> bool:
    v = os.getenv(key)
    if v is None:
        return default
    return str(v).strip().lower() in ("1", "true", "yes", "y", "on")


def _env_int(key: str, default: int) -> int:
    try:
        return int(str(os.getenv(key, str(default))).strip())
    except Exception:
        return default


def _env_float(key: str, default: float) -> float:
    try:
        return float(str(os.getenv(key, str(default))).strip())
    except Exception:
        return default


def _env_str(key: str, default: str = "") -> str:
    return str(os.getenv(key, default) or default).strip()


def _clamp(x: float, lo: float = 0.0, hi: float = 1.0) -> float:
    try:
        x = float(x)
    except Exception:
        x = 0.0
    return max(lo, min(hi, x))


def _s(x: Any) -> str:
    if x is None:
        return ""
    if isinstance(x, bytes | bytearray):
        return x.decode("utf-8", "ignore")
    return str(x)


async def _maybe_await(x: Any) -> Any:
    if asyncio.iscoroutine(x):
        return await x
    return x


async def _call(fn, *args, **kwargs):
    if fn is None:
        return None
    try:
        return await _maybe_await(fn(*args, **kwargs))
    except Exception:
        return None


# =============================================================================
# Config (ENV)
# =============================================================================
# Loop cadences
POSITION_CHECK_INTERVAL = _env_int("POSITION_MONITOR_INTERVAL", 20)
RISK_MANAGER_LOOP_INTERVAL = _env_int("RISK_MANAGER_LOOP_INTERVAL_SEC", 5)

# Alerts / output
ALERT_COOLDOWN_SECONDS = _env_int(
    "ALERT_COOLDOWN_SECONDS", _env_int("ALERT_COOLDOWNS_SECONDS", 900)
)
RISK_DISABLE_ALERTS = _env_bool("RISK_DISABLE_ALERTS", False)
RISK_DISABLE_NOTION = _env_bool("RISK_DISABLE_NOTION", False)
RISK_DISABLE_AI_OVERLAY = _env_bool("RISK_DISABLE_AI_OVERLAY", False)

# Redis
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")

# Wallet pubkey env aliases (equity calc best effort)
WALLET_PUBKEY = (
    os.getenv("WALLET_PUBKEY") or os.getenv("WALLET_PUBLIC_KEY") or os.getenv("WALLET_ADDRESS")
)

# Day-level governance
STOP_AFTER_DAILY_PROFIT_PCT = _env_float("STOP_AFTER_DAILY_PROFIT_PCT", 4.0)
DAILY_LOSS_CAP_PCT = _env_float("DAILY_LOSS_CAP_PCT", 0.0)  # 0 disables
DAILY_SPEND_CAP_PCT = _env_float("DAILY_SPEND_CAP_PCT", 0.15)  # percent of equity
DAILY_SPEND_CAP_USD = _env_float("DAILY_SPEND_CAP_USD", 0.0)  # absolute cap override if >0

# Baseline caps
RISK_MAX_PER_TRADE = _env_float("RISK_MAX_PER_TRADE", 0.045)
MAX_CONCURRENT_POS = _env_int("MAX_CONCURRENT_POS", 4)

RISK_MIN_TICKET_USD = _env_float("RISK_MIN_TICKET_USD", 5.0)
RISK_MAX_TICKET_USD = _env_float("RISK_MAX_TICKET_USD", 5_000.0)
RISK_MAX_EQUITY_UTILIZATION_PCT = _env_float(
    "RISK_MAX_EQUITY_UTILIZATION_PCT", 25.0
)  # cap for (ticket*max_conc)/equity

# Optional: exposure caps (open notional)
RISK_MAX_EXPOSURE_PCT = _env_float("RISK_MAX_EXPOSURE_PCT", 0.0)  # percent of equity (0 disables)
RISK_MAX_EXPOSURE_USD = _env_float("RISK_MAX_EXPOSURE_USD", 0.0)  # absolute (0 disables)

# Performance throttle
THROTTLE_WINRATE_FLOOR = _env_float("THROTTLE_WINRATE_FLOOR", 0.45)
THROTTLE_DD_FLOOR = _env_float("THROTTLE_DD_FLOOR", 0.08)
THROTTLE_TICKET_CUT = _env_float("THROTTLE_TICKET_CUT", 0.40)
THROTTLE_CONC_DEC = _env_int("THROTTLE_CONC_DEC", 1)

# Risk profile
RISK_PROFILE = _env_str("RISK_PROFILE", "balanced").lower()
RISK_PROFILE_OVERRIDE_KEY = "risk:profile_override"
RISK_PROFILE_CONFIG: dict[str, dict[str, float]] = {
    "conservative": {
        "ticket_mult": 0.70,
        "pos_pct_mult": 0.80,
        "max_conc_mult": 0.80,
        "daily_spend_mult": 0.80,
        "capacity_exp_mult": 1.20,
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
        "capacity_exp_mult": 0.85,
    },
}

# Regime conc deltas
REGIME_MAX_CONC_DELTA: dict[str, int] = {
    "risk_off": _env_int("REGIME_MAX_CONC_DELTA_RISK_OFF", -2),
    "normal": _env_int("REGIME_MAX_CONC_DELTA_NORMAL", 0),
    "risk_on": _env_int("REGIME_MAX_CONC_DELTA_RISK_ON", 1),
}

# Tier schedule (equity bands)
TIER_SCHEDULE: list[dict[str, Any]] = [
    {"equity_to": 2_500, "ticket": 50, "pos_pct": 0.025, "max_conc": 3, "spend_pct": 0.10},
    {"equity_to": 5_000, "ticket": 100, "pos_pct": 0.030, "max_conc": 4, "spend_pct": 0.12},
    {"equity_to": 10_000, "ticket": 175, "pos_pct": 0.035, "max_conc": 5, "spend_pct": 0.14},
    {"equity_to": 25_000, "ticket": 325, "pos_pct": 0.040, "max_conc": 6, "spend_pct": 0.15},
    {"equity_to": 50_000, "ticket": 600, "pos_pct": 0.045, "max_conc": 7, "spend_pct": 0.16},
    {"equity_to": 100_000, "ticket": 1_000, "pos_pct": 0.045, "max_conc": 8, "spend_pct": 0.18},
    {"equity_to": 500_000, "ticket": 2_500, "pos_pct": 0.050, "max_conc": 10, "spend_pct": 0.20},
    {"equity_to": 10**12, "ticket": 0, "pos_pct": 0.050, "max_conc": 10, "spend_pct": 0.20},
]

# Capacity model knobs
RISK_CAPACITY_DD_AT_ZERO = _env_float("RISK_CAPACITY_DD_AT_ZERO", 0.20)  # fraction
RISK_CAPACITY_WR_LOW = _env_float("RISK_CAPACITY_WR_LOW", 0.40)
RISK_CAPACITY_WR_MID = _env_float("RISK_CAPACITY_WR_MID", 0.55)
RISK_CAPACITY_VOL_PENALTY = _env_float("RISK_CAPACITY_VOL_PENALTY", 0.15)
RISK_CAPACITY_CHOP_PENALTY = _env_float("RISK_CAPACITY_CHOP_PENALTY", 0.15)
RISK_CAPACITY_TICKET_EXP_BASE = _env_float("RISK_CAPACITY_TICKET_EXP", 0.9)

# Optional risk mode override
RISK_MODE_OVERRIDE_ENV = os.getenv("RISK_MODE_OVERRIDE")  # optional "0..3"
EQUITY_KILL_SWITCH_DD_PCT = _env_float("EQUITY_KILL_SWITCH_DD_PCT", 0.0)  # fraction

# Cooldowns
RISK_COOLDOWN_CONSEC_LOSSES = _env_int("RISK_COOLDOWN_CONSEC_LOSSES", 3)
RISK_COOLDOWN_MINUTES = _env_int("RISK_COOLDOWN_MINUTES", 15)
RISK_DD_COOLDOWN_FLOOR_PCT = _env_float("RISK_DD_COOLDOWN_FLOOR_PCT", 0.0)  # percent
RISK_DD_COOLDOWN_MINUTES = _env_int("RISK_DD_COOLDOWN_MINUTES", 0)

# Entry floors
RISK_MIN_CAPACITY_FOR_ENTRY = _env_float("RISK_MIN_CAPACITY_FOR_ENTRY", 0.05)
RISK_PLAN_AHEAD_HARD_STOP_PCT = _env_float("RISK_PLAN_AHEAD_HARD_STOP_PCT", 0.0)  # percent
RISK_MIN_EQUITY_USD = _env_float("RISK_MIN_EQUITY_USD", 0.0)
RISK_EQUITY_OVERRIDE_USD = _env_float("RISK_EQUITY_OVERRIDE_USD", 0.0)

# Daily caps
RISK_MAX_DAILY_TRADES = _env_int("RISK_MAX_DAILY_TRADES", 0)
RISK_MAX_DAILY_ENTRIES = _env_int("RISK_MAX_DAILY_ENTRIES", 0)

# Liquidity-aware sizing
RISK_LIQ_AWARE_SIZING = _env_bool("RISK_LIQ_AWARE_SIZING", True)
RISK_LIQ_MAX_IMPACT_BPS = _env_float("RISK_LIQ_MAX_IMPACT_BPS", 75.0)

# Sandbox (tiny sizing)
RISK_SANDBOX_MODE = _env_bool("RISK_SANDBOX_MODE", False)

# Kelly clamp
RISK_KELLY_ENABLED = _env_bool("RISK_KELLY_ENABLED", True)
RISK_KELLY_DEFAULT_R = _env_float("RISK_KELLY_DEFAULT_R", 1.5)
RISK_KELLY_MAX_FRACTION = _env_float("RISK_KELLY_MAX_FRACTION", 0.05)
RISK_KELLY_MIN_FRACTION = _env_float("RISK_KELLY_MIN_FRACTION", 0.005)

# Trading window (UTC)
RISK_TRADING_WINDOW_ENABLED = _env_bool("RISK_TRADING_WINDOW_ENABLED", False)
RISK_TRADING_START_HOUR_UTC = _env_int("RISK_TRADING_START_HOUR_UTC", 0)
RISK_TRADING_END_HOUR_UTC = _env_int("RISK_TRADING_END_HOUR_UTC", 24)
RISK_TRADING_DAYS = _env_str("RISK_TRADING_DAYS", "").lower()

# Caching (hot path)
RISK_LIMITS_CACHE_TTL_MS = _env_int("RISK_LIMITS_CACHE_TTL_MS", 500)

# Per-mint cooldown default (anti-chop)
RISK_MINT_COOLDOWN_MINUTES_DEFAULT = _env_int("RISK_MINT_COOLDOWN_MINUTES_DEFAULT", 0)

# Redis keys
RISK_HALT_KEY = "risk:halt"
RISK_MANUAL_BLOCK_KEY = "risk:manual_block"
RISK_CTX_KEY = "risk:ctx"
RISK_CTX_DIGEST_KEY = "risk:ctx_digest"
RISK_HALT_REASON_KEY = "risk:halt_reason"
RISK_COOLDOWN_UNTIL_KEY = "risk:cooldown_until_ts"

METRICS_LAST20_KEY = "metrics:last20"
METRICS_TRADES_KEY = "metrics:trades_total"
METRICS_WINS_KEY = "metrics:wins_total"
METRICS_WINRATE20 = "metrics:winrate20"
METRICS_ROLLING_DD = "metrics:rolling_dd_frac"  # fraction [0..1]
EQUITY_HWM_KEY = "metrics:equity_hwm_usd"

OVERRIDE_TICKET_KEY = "risk:ticket_usd_override"
OVERRIDE_POS_PCT_KEY = "risk:pos_pct_cap_override"
OVERRIDE_MAX_CONC_KEY = "risk:max_concurrent_override"
OVERRIDE_DAILY_SPEND_KEY = "risk:daily_spend_usd_override"
OVERRIDE_MODE_KEY = "risk:mode_override"

# Local alert cooldowns (process-level)
_COOLDOWNS: dict[str, float] = {}
_last_broadcast_mode: int | None = None


# =============================================================================
# Daily keys + idempotency keys
# =============================================================================
def _key_daily(prefix: str) -> str:
    d = datetime.now(UTC).strftime("%Y%m%d")
    return f"{prefix}:{d}"


def _pnl_key_today() -> str:
    return _key_daily("pnl:day:realized_usd")


def _spent_key_today() -> str:
    return _key_daily("budget:sniper:spent_usd")


def _entries_key_today() -> str:
    return _key_daily("risk:entries_count")


def _trades_key_today() -> str:
    return _key_daily("risk:trades_count")


def _closed_trade_key(trade_id: str) -> str:
    return f"trade:closed:{trade_id}"


# =============================================================================
# Time + redis helpers
# =============================================================================
def _now_ts() -> float:
    return datetime.now(UTC).timestamp()


def _midnight_utc() -> datetime:
    now = datetime.now(UTC)
    return (now + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)


def _ttl_to_midnight_sec() -> int:
    ttl = int((_midnight_utc() - datetime.now(UTC)).total_seconds())
    return max(1, ttl)


async def _set_expire_at_midnight(redis, key: str) -> None:
    if not redis:
        return
    with contextlib.suppress(Exception):
        await redis.expire(key, _ttl_to_midnight_sec())


_redis_singleton = None


async def _get_redis(url: str | None = None):
    """
    Prefer core_utils shared redis (if present), otherwise use core_utils.get_redis_async(),
    otherwise fallback to aioredis singleton.
    """
    global _redis_singleton

    shared = getattr(cu, "redis", None)
    if shared is not None:
        return shared

    getter = getattr(cu, "get_redis_async", None)
    if getter is not None:
        with contextlib.suppress(Exception):
            r = await getter()
            if r is not None:
                return r

    if _redis_singleton is not None:
        return _redis_singleton

    if aioredis is None:
        return None

    try:
        _redis_singleton = aioredis.from_url(url or REDIS_URL, decode_responses=True)
        return _redis_singleton
    except Exception:
        _redis_singleton = None
        return None


def _alert_ok(token: str) -> bool:
    if RISK_DISABLE_ALERTS:
        return False
    if ALERT_COOLDOWN_SECONDS <= 0:
        return True
    now = _now_ts()
    until = _COOLDOWNS.get(token)
    if until is not None and until > now:
        return False
    _COOLDOWNS[token] = now + ALERT_COOLDOWN_SECONDS
    return True


def _parse_trading_days(raw: str) -> set[int] | None:
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
            with contextlib.suppress(Exception):
                v = int(p)
                if 0 <= v <= 6:
                    out.add(v)
        else:
            v = name_to_idx.get(p)
            if v is not None:
                out.add(v)
    return out or None


def _trading_window_status() -> tuple[bool, str]:
    if not RISK_TRADING_WINDOW_ENABLED:
        return True, "trading_window_disabled"

    now = datetime.now(UTC)
    hour = now.hour
    weekday = now.weekday()

    allowed_days = _parse_trading_days(RISK_TRADING_DAYS)
    if allowed_days is not None and weekday not in allowed_days:
        return False, f"weekday {weekday} not allowed"

    start = max(0, min(23, int(RISK_TRADING_START_HOUR_UTC)))
    end = max(0, min(24, int(RISK_TRADING_END_HOUR_UTC)))
    if start == end:
        return True, "24h_window"

    if start < end:
        ok = start <= hour < end
        return ok, (f"within [{start},{end})" if ok else f"hour {hour} outside [{start},{end})")

    ok = (hour >= start) or (hour < end)
    return ok, (
        f"within overnight {start}->{end}"
        if ok
        else f"hour {hour} outside overnight {start}->{end}"
    )


# =============================================================================
# Halt / stop-trading (instant deterministic)
# =============================================================================
async def _read_halt_reason(redis) -> str:
    if not redis:
        return ""
    with contextlib.suppress(Exception):
        return _s(await redis.get(RISK_HALT_REASON_KEY))
    return ""


async def _is_halted_fast(redis) -> tuple[bool, str]:
    """
    FAST path: do not depend on cached limits.
    Keys here must immediately stop entries.
    """
    if not redis:
        return False, ""

    with contextlib.suppress(Exception):
        if _s(await redis.get(RISK_HALT_KEY)).strip() == "1":
            reason = await _read_halt_reason(redis)
            return True, f"risk:halt=1{(' | ' + reason) if reason else ''}"

    with contextlib.suppress(Exception):
        if _s(await redis.get(RISK_MANUAL_BLOCK_KEY)).strip() == "1":
            reason = await _read_halt_reason(redis)
            return True, f"risk:manual_block=1{(' | ' + reason) if reason else ''}"

    with contextlib.suppress(Exception):
        if _s(await redis.get("infra:halt")).strip() == "1":
            return True, "infra:halt=1"

    # strategy HARD_HALT (best effort)
    with contextlib.suppress(Exception):
        strat = await get_strategy_params(redis)
        hh = str(strat.get("HARD_HALT", "0")).strip().lower() in ("1", "true", "yes", "on")
        if hh:
            return True, "strategy HARD_HALT=1"

    return False, ""


async def halt_trading(reason: str = "manual_halt", redis=None) -> None:
    """Hard global halt. Strongest stop; checked on every gate call."""
    if redis is None:
        redis = await _get_redis()
    if not redis:
        return
    with contextlib.suppress(Exception):
        await redis.set(RISK_HALT_KEY, "1")
        if reason:
            await redis.set(RISK_HALT_REASON_KEY, str(reason)[:300])
        await log_event("risk_halt_set", {"reason": reason, "engine": RISK_ENGINE_VERSION})
        invalidate_limits_cache()


async def resume_trading(redis=None) -> None:
    """Clears hard global halt. Does NOT clear manual block / infra halt."""
    if redis is None:
        redis = await _get_redis()
    if not redis:
        return
    with contextlib.suppress(Exception):
        await redis.delete(RISK_HALT_KEY)
        await redis.delete(RISK_HALT_REASON_KEY)
        await log_event("risk_halt_cleared", {"engine": RISK_ENGINE_VERSION})
        invalidate_limits_cache()


# =============================================================================
# Equity + positions exposure (best effort)
# =============================================================================
async def _smart_wallets_view() -> dict[str, Any] | None:
    """
    Optional: utils_pkg.smart_wallets.trading_equity_view()
    Import-safe (never raises if missing).
    """
    try:
        from utils_pkg.smart_wallets import trading_equity_view  # type: ignore
    except Exception:
        return None
    with contextlib.suppress(Exception):
        v = await _maybe_await(trading_equity_view())
        return v if isinstance(v, dict) else None
    return None


async def _wallet_equity_usd() -> float:
    """
    Best-effort total trading equity in USD.

    Priority:
      1) utils_pkg.smart_wallets.trading_equity_view (if available)
      2) fallback: WALLET_PUBKEY token balances + token prices (best effort)
    """
    view = await _smart_wallets_view()
    if isinstance(view, dict):
        raw_eq = (
            view.get("equity_usd") or view.get("equity") or view.get("trading_equity_usd") or 0.0
        )
        with contextlib.suppress(Exception):
            eq = float(raw_eq or 0.0)
            if eq > 0:
                return eq

    if not WALLET_PUBKEY:
        return 0.0

    get_wallet_tokens = getattr(cu, "get_wallet_tokens", None)
    get_token_price = getattr(cu, "get_token_price", None)
    convert_sol_to_usd_amount = getattr(cu, "convert_sol_to_usd_amount", None)

    tokens = await _call(get_wallet_tokens, WALLET_PUBKEY)
    tokens = tokens or []
    if not tokens:
        return 0.0

    sol_usd = await _call(convert_sol_to_usd_amount, 1.0)
    with contextlib.suppress(Exception):
        sol_usd = float(sol_usd or 0.0)
    if not sol_usd or sol_usd <= 0:
        return 0.0

    total_usd = 0.0
    for t in tokens:
        try:
            mint = (t or {}).get("mint")
            bal = float((t or {}).get("balance", 0.0) or 0.0)
        except Exception:
            continue
        if not mint or bal <= 0:
            continue

        if mint == SOL_MINT:
            total_usd += bal * sol_usd
            continue

        px = await _call(get_token_price, mint)
        with contextlib.suppress(Exception):
            px = float(px or 0.0)
        if not px or px <= 0:
            continue
        total_usd += bal * px

    return float(total_usd)


async def estimate_equity_usd() -> float:
    return await _wallet_equity_usd()


async def _wallet_sol_balance() -> float:
    view = await _smart_wallets_view()
    if isinstance(view, dict):
        raw = view.get("sol_balance") or view.get("sol") or view.get("trading_sol_balance") or 0.0
        with contextlib.suppress(Exception):
            bal = float(raw or 0.0)
            if bal > 0:
                return bal

    if not WALLET_PUBKEY:
        return 0.0

    get_wallet_tokens = getattr(cu, "get_wallet_tokens", None)
    tokens = await _call(get_wallet_tokens, WALLET_PUBKEY)
    for t in tokens or []:
        if (t or {}).get("mint") == SOL_MINT:
            with contextlib.suppress(Exception):
                return float((t or {}).get("balance", 0.0) or 0.0)
    return 0.0


def _select_tier(equity_usd: float) -> dict[str, Any]:
    for t in TIER_SCHEDULE:
        if equity_usd <= t["equity_to"]:
            return dict(t)
    return dict(TIER_SCHEDULE[-1])


def _load_open_positions_safe() -> list[dict[str, Any]]:
    """
    Best-effort open positions view.

    Priority:
      1) utils_pkg.position_store.list_open_positions() if present
      2) positions.load_positions() legacy fallback

    This must remain import-safe (never raises).
    """
    # 1) preferred future canonical source
    with contextlib.suppress(Exception):
        from utils_pkg.position_store import list_open_positions  # type: ignore

        out = list_open_positions()
        if asyncio.iscoroutine(out):
            # avoid awaiting in sync helper; caller uses count/exposure helpers that can stay sync
            # (position_store should ideally be sync for reads, but we keep safe)
            return []
        return list(out or [])

    # 2) legacy fallback
    with contextlib.suppress(Exception):
        from positions import load_positions  # type: ignore

        out = load_positions()
        return list(out or [])

    return []


def _positions_open_count() -> int:
    return len(_load_open_positions_safe())


def _positions_open_exposure_usd() -> float:
    """
    Best-effort open exposure (USD). Does NOT hit RPC; uses stored fields only.
    """
    total = 0.0
    for p in _load_open_positions_safe():
        if not isinstance(p, dict):
            continue
        for k in (
            "notional_usd",
            "exposure_usd",
            "entry_usd",
            "cost_usd",
            "spent_usd",
            "ticket_usd",
        ):
            v = p.get(k)
            if v is None:
                continue
            with contextlib.suppress(Exception):
                vv = float(v)
                if vv > 0:
                    total += vv
                    break
    return float(total)


# =============================================================================
# Metrics helpers
# =============================================================================
async def _metrics(redis) -> tuple[float, float]:
    """Return (winrate20, rolling_dd_frac) where rolling_dd_frac ∈ [0..1]."""
    if not redis:
        return 0.50, 0.0
    try:
        wr, dd = await redis.mget(METRICS_WINRATE20, METRICS_ROLLING_DD)
        winrate = float(wr) if wr is not None else 0.50
        drawdown = float(dd) if dd is not None else 0.0
        return _clamp(winrate, 0.0, 1.0), _clamp(drawdown, 0.0, 1.0)
    except Exception:
        return 0.50, 0.0


async def _equity_hwm(redis) -> float:
    if not redis:
        return 0.0
    with contextlib.suppress(Exception):
        v = await redis.get(EQUITY_HWM_KEY)
        return float(v) if v else 0.0
    return 0.0


async def _daily_realized_usd(redis) -> float:
    if not redis:
        return 0.0
    with contextlib.suppress(Exception):
        v = await redis.get(_pnl_key_today())
        return float(v) if v else 0.0
    return 0.0


async def _daily_spent_usd(redis) -> float:
    if not redis:
        return 0.0
    with contextlib.suppress(Exception):
        v = await redis.get(_spent_key_today())
        return float(v) if v else 0.0
    return 0.0


async def _daily_trades(redis) -> int:
    if not redis:
        return 0
    with contextlib.suppress(Exception):
        v = await redis.get(_trades_key_today())
        return int(float(v)) if v else 0
    return 0


async def _daily_entries(redis) -> int:
    if not redis:
        return 0
    with contextlib.suppress(Exception):
        v = await redis.get(_entries_key_today())
        return int(float(v)) if v else 0
    return 0


async def _consecutive_losses(redis) -> int:
    """Count leading losses in last20 list (most recent first). last20 values: 1=win, 0=loss"""
    if not redis:
        return 0
    try:
        seq = await redis.lrange(METRICS_LAST20_KEY, 0, 19)
        losses = 0
        for v in seq or []:
            is_win = False
            with contextlib.suppress(Exception):
                is_win = int(v) == 1
            if is_win:
                break
            losses += 1
        return losses
    except Exception:
        return 0


# =============================================================================
# Risk capacity + Kelly helpers
# =============================================================================
def _risk_capacity_from_components(
    *,
    winrate20: float,
    rolling_dd: float,  # fraction
    realized_pnl_usd: float,
    equity_usd: float,
    regime_mult: float,
    regime_volatility: float = 0.5,
    regime_choppy: float = 0.0,
) -> float:
    dd = max(0.0, float(rolling_dd))
    dd_term = 1.0 if RISK_CAPACITY_DD_AT_ZERO <= 0 else _clamp(1.0 - dd / RISK_CAPACITY_DD_AT_ZERO)

    wr = _clamp(winrate20, 0.0, 1.0)
    if RISK_CAPACITY_WR_MID <= RISK_CAPACITY_WR_LOW:
        wr_term = wr
    else:
        wr_term = _clamp(
            (wr - RISK_CAPACITY_WR_LOW) / (RISK_CAPACITY_WR_MID - RISK_CAPACITY_WR_LOW)
        )

    # Profit stop taper as we approach the daily profit target
    if equity_usd <= 0 or STOP_AFTER_DAILY_PROFIT_PCT <= 0:
        pnl_term = 1.0
    else:
        target = equity_usd * (STOP_AFTER_DAILY_PROFIT_PCT / 100.0)
        if target <= 0:
            pnl_term = 1.0
        else:
            pct = max(0.0, float(realized_pnl_usd)) / target
            pnl_term = _clamp(1.0 - min(pct, 1.0) * 0.7, 0.3, 1.0)

    vol_pen = _clamp(regime_volatility, 0.0, 1.0) * RISK_CAPACITY_VOL_PENALTY
    chop_pen = _clamp(regime_choppy, 0.0, 1.0) * RISK_CAPACITY_CHOP_PENALTY

    base = 0.40 * dd_term + 0.40 * wr_term + 0.20 * pnl_term
    base *= (1.0 - vol_pen) * (1.0 - chop_pen)

    mult = _clamp(regime_mult, 0.6, 1.4)
    return _clamp(base * mult, 0.0, 1.0)


def _risk_mode_from_capacity(capacity: float) -> int:
    """
    Modes:
      0 OFF
      1 NORMAL
      2 AGGRESSIVE
      3 DEFENSIVE
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
    p = _clamp(winrate, 0.0, 1.0)
    if payoff_r <= 0:
        return 0.0
    q = 1.0 - p
    f = p - (q / payoff_r)
    return _clamp(f, 0.0, 1.0)


def _kelly_pos_cap(winrate: float) -> tuple[float, float]:
    if not RISK_KELLY_ENABLED:
        return 0.0, 0.0
    raw = _kelly_fraction(winrate, RISK_KELLY_DEFAULT_R)
    frac = raw if raw > 0 else RISK_KELLY_MIN_FRACTION
    frac = max(RISK_KELLY_MIN_FRACTION, min(RISK_KELLY_MAX_FRACTION, frac))
    return raw, frac


async def _risk_mode_with_override(
    redis, capacity: float, *, rolling_dd: float
) -> tuple[float, int, int | None, str | None]:
    base_mode = _risk_mode_from_capacity(capacity)
    override_mode: int | None = None
    halt_reason: str | None = None

    if RISK_MODE_OVERRIDE_ENV is not None:
        with contextlib.suppress(Exception):
            override_mode = int(str(RISK_MODE_OVERRIDE_ENV).strip())
    elif redis:
        with contextlib.suppress(Exception):
            v = await redis.get(OVERRIDE_MODE_KEY)
            if v is not None:
                override_mode = int(float(v))

    mode = override_mode if (override_mode in (0, 1, 2, 3)) else base_mode

    if EQUITY_KILL_SWITCH_DD_PCT > 0 and float(rolling_dd) >= float(EQUITY_KILL_SWITCH_DD_PCT):
        mode = 0
        halt_reason = (
            f"equity_drawdown>=kill_switch ({rolling_dd:.3f} ≥ {EQUITY_KILL_SWITCH_DD_PCT:.3f})"
        )

    if mode == 0:
        cap_adj = 0.0
    elif mode == 3:
        cap_adj = min(float(capacity), 0.40)
    else:
        cap_adj = float(capacity)

    return _clamp(cap_adj, 0.0, 1.0), int(mode), override_mode, halt_reason


# =============================================================================
# Profiles + live overrides
# =============================================================================
async def _effective_risk_profile(redis) -> tuple[str, dict[str, float]]:
    profile = RISK_PROFILE

    if redis:
        with contextlib.suppress(Exception):
            v = await redis.get(RISK_PROFILE_OVERRIDE_KEY)
            if v:
                vv = _s(v).strip().lower()
                if vv in RISK_PROFILE_CONFIG or vv == "auto":
                    profile = vv

    if profile == "auto":
        with contextlib.suppress(Exception):
            hinted = regime_state.risk_profile_hint()
            profile = hinted if hinted in RISK_PROFILE_CONFIG else "balanced"

    cfg = RISK_PROFILE_CONFIG.get(profile, RISK_PROFILE_CONFIG["balanced"])
    return profile, cfg


async def _apply_overrides(
    redis, *, ticket_usd: float, pos_pct_cap: float, max_concurrent: int, daily_spend_usd: float
) -> dict[str, Any]:
    out = {
        "ticket_usd": float(ticket_usd),
        "pos_pct_cap": float(pos_pct_cap),
        "max_concurrent": int(max_concurrent),
        "daily_spend_usd": float(daily_spend_usd),
    }
    if not redis:
        return out

    try:
        t_raw, p_raw, c_raw, s_raw = await redis.mget(
            OVERRIDE_TICKET_KEY,
            OVERRIDE_POS_PCT_KEY,
            OVERRIDE_MAX_CONC_KEY,
            OVERRIDE_DAILY_SPEND_KEY,
        )
    except Exception:
        return out

    if t_raw is not None:
        with contextlib.suppress(Exception):
            t = float(t_raw)
            if t > 0:
                out["ticket_usd"] = t

    if p_raw is not None:
        with contextlib.suppress(Exception):
            p = float(p_raw)
            if 0 < p <= max(RISK_MAX_PER_TRADE, 1e-9):
                out["pos_pct_cap"] = p

    if c_raw is not None:
        with contextlib.suppress(Exception):
            c = int(float(c_raw))
            if c >= 0:
                out["max_concurrent"] = c

    if s_raw is not None:
        with contextlib.suppress(Exception):
            s = float(s_raw)
            if s > 0:
                out["daily_spend_usd"] = s

    return out


# =============================================================================
# Liquidity-aware sizing
# =============================================================================
def liq_aware_ticket_usd(base_ticket_usd: float, pool_depth_usd: float | None) -> float:
    base = max(0.0, float(base_ticket_usd))
    if not RISK_LIQ_AWARE_SIZING:
        return base
    if pool_depth_usd is None or pool_depth_usd <= 0:
        return base
    if RISK_LIQ_MAX_IMPACT_BPS <= 0:
        return base

    impact_frac = float(RISK_LIQ_MAX_IMPACT_BPS) / 10_000.0
    max_notional = float(pool_depth_usd) * impact_frac
    if max_notional <= 0:
        return base
    return max(0.0, min(base, max_notional))


async def pair_limits_for_depth(pool_depth_usd: float | None, redis=None) -> dict[str, Any]:
    limits = await current_limits(redis)
    base_ticket = float(limits.get("ticket_usd", 0.0))
    ticket_liq = liq_aware_ticket_usd(base_ticket, pool_depth_usd)
    out = dict(limits)
    out["ticket_usd_raw"] = base_ticket
    out["ticket_usd_liq"] = ticket_liq
    return out


# =============================================================================
# ctx digest (observability & reproducibility)
# =============================================================================
def ctx_digest(d: dict[str, Any]) -> str:
    try:
        s = json.dumps(d, sort_keys=True, separators=(",", ":"), default=str)
        return hashlib.sha1(s.encode("utf-8")).hexdigest()[:16]
    except Exception:
        return "digest_err"


async def _read_ctx_digest(redis=None) -> str:
    if redis is None:
        redis = await _get_redis()
    if not redis:
        return "no_redis"
    with contextlib.suppress(Exception):
        v = await redis.get(RISK_CTX_DIGEST_KEY)
        return _s(v) if v else "missing"
    return "err"


# =============================================================================
# Risk context publishing + alerts
# =============================================================================
async def _update_risk_ctx(
    redis,
    *,
    limits: dict[str, Any],
    realized_usd: float,
    winrate20: float,
    consec_losses: int,
    cooldown_until_ts: float | None,
    strat_params: dict[str, Any],
    profile_name: str,
    ai_capacity_delta: float,
    halt_reason: str | None,
    kelly_raw: float,
    kelly_pos_pct_cap: float,
    regime_gate_factor: float,
    equity_utilization_pct: float,
    open_exposure_usd: float,
    max_exposure_usd: float,
    trading_window_open: bool,
    trading_window_reason: str,
) -> None:
    if not redis:
        return

    open_positions = _positions_open_count()
    equity_usd = float(limits.get("equity_usd", 0.0))
    max_conc = int(limits.get("max_concurrent", 0))
    daily_spend_usd = float(limits.get("daily_spend_usd", 0.0))
    spent_usd = await _daily_spent_usd(redis)
    trades_today = await _daily_trades(redis)
    entries_today = await _daily_entries(redis)
    equity_hwm = await _equity_hwm(redis)

    daily_realized_pct = (realized_usd / equity_usd) * 100.0 if equity_usd > 0 else 0.0
    dd_fraction = float(limits.get("rolling_dd_frac", 0.0))
    dd_pct = dd_fraction * 100.0

    now_ts = _now_ts()
    cooldown_rem = 0.0
    if cooldown_until_ts is not None and cooldown_until_ts > now_ts:
        cooldown_rem = cooldown_until_ts - now_ts

    remaining_spend = max(0.0, daily_spend_usd - spent_usd)
    spend_used_pct = (spent_usd / daily_spend_usd) * 100.0 if daily_spend_usd > 0 else 0.0

    profile_override = ""
    with contextlib.suppress(Exception):
        v = await redis.get(RISK_PROFILE_OVERRIDE_KEY)
        if v:
            profile_override = _s(v).strip().lower()

    strat_mode = strat_params.get("STRAT_MODE")
    plan_equity_today = float(strat_params.get("PLAN_EQUITY_TODAY_USD", 0.0))
    plan_gap_pct = float(strat_params.get("PLAN_GAP_PCT", 0.0))
    plan_status = strat_params.get("PLAN_STATUS")

    mapping: dict[str, str] = {
        "open_positions": str(int(open_positions)),
        "max_positions": str(int(max_conc)),
        "open_exposure_usd": f"{float(open_exposure_usd):.4f}",
        "max_exposure_usd": f"{float(max_exposure_usd):.4f}",
        "ticket_usd": f"{float(limits.get('ticket_usd', 0.0)):.4f}",
        "daily_spend_usd": f"{daily_spend_usd:.4f}",
        "daily_spend_remaining_usd": f"{remaining_spend:.4f}",
        "daily_spend_used_pct": f"{spend_used_pct:.6f}",
        "daily_realized_pnl_pct": f"{float(daily_realized_pct):.6f}",
        "daily_drawdown_pct": f"{float(dd_pct):.6f}",
        "risk_mode": str(int(limits.get("risk_mode", 1))),
        "risk_capacity": f"{float(limits.get('risk_capacity', 1.0)):.6f}",
        "regime_risk_multiplier": f"{float(limits.get('regime_risk_multiplier', 1.0)):.6f}",
        "regime_gate_factor": f"{float(regime_gate_factor):.6f}",
        "winrate20": f"{float(winrate20):.6f}",
        "equity_usd": f"{float(equity_usd):.4f}",
        "equity_hwm_usd": f"{float(equity_hwm):.4f}",
        "consecutive_losses": str(int(consec_losses)),
        "cooldown_remaining_sec": f"{float(cooldown_rem):.1f}",
        "risk_profile": profile_name,
        "risk_profile_env": RISK_PROFILE,
        "risk_profile_override": profile_override,
        "sandbox_mode": "1" if RISK_SANDBOX_MODE else "0",
        "max_risk_per_trade": f"{float(strat_params.get('MAX_RISK_PER_TRADE', RISK_MAX_PER_TRADE)):.6f}",
        "min_sol_balance": f"{float(strat_params.get('MIN_SOL_BALANCE', 0.0)):.6f}",
        "plan_equity_today_usd": f"{plan_equity_today:.4f}",
        "plan_gap_pct": f"{plan_gap_pct:.6f}",
        "ai_capacity_delta": f"{float(ai_capacity_delta):.6f}",
        "daily_trades_count": str(int(trades_today)),
        "daily_trades_cap": str(int(RISK_MAX_DAILY_TRADES)),
        "daily_entries_count": str(int(entries_today)),
        "daily_entries_cap": str(int(RISK_MAX_DAILY_ENTRIES)),
        "kelly_raw": f"{float(kelly_raw):.6f}",
        "kelly_pos_pct_cap": f"{float(kelly_pos_pct_cap):.6f}",
        "equity_utilization_pct": f"{float(equity_utilization_pct):.6f}",
        "equity_override_usd": f"{float(RISK_EQUITY_OVERRIDE_USD):.4f}",
        "trading_window_open": "1" if trading_window_open else "0",
        "trading_window_reason": trading_window_reason,
        "risk_engine_version": RISK_ENGINE_VERSION,
        "last_update_ts": datetime.now(UTC).isoformat(),
    }
    if strat_mode is not None:
        mapping["strat_mode"] = str(strat_mode)
    if plan_status is not None:
        mapping["plan_status"] = str(plan_status)
    if halt_reason is not None:
        mapping["halt_reason"] = str(halt_reason)

    digest_payload = {
        "equity_usd": equity_usd,
        "ticket_usd": float(limits.get("ticket_usd", 0.0)),
        "pos_pct_cap": float(limits.get("pos_pct_cap", 0.0)),
        "max_concurrent": max_conc,
        "daily_spend_usd": daily_spend_usd,
        "winrate20": winrate20,
        "rolling_dd_frac": dd_fraction,
        "risk_capacity": float(limits.get("risk_capacity", 1.0)),
        "risk_mode": int(limits.get("risk_mode", 1)),
        "regime": str(limits.get("regime", "")),
        "regime_gate_factor": float(regime_gate_factor),
        "profile": profile_name,
        "sandbox": bool(RISK_SANDBOX_MODE),
        "open_exposure_usd": float(open_exposure_usd),
        "max_exposure_usd": float(max_exposure_usd),
    }
    digest = ctx_digest(digest_payload)

    with contextlib.suppress(Exception):
        await redis.hset(RISK_CTX_KEY, mapping=mapping)
        await redis.set(RISK_CTX_DIGEST_KEY, digest)
        await _set_expire_at_midnight(redis, RISK_CTX_DIGEST_KEY)

    with contextlib.suppress(Exception):
        await log_event(
            "risk_ctx_update",
            {
                "ctx_digest": digest,
                "equity_usd": equity_usd,
                "equity_hwm_usd": equity_hwm,
                "risk_capacity": float(limits.get("risk_capacity", 1.0)),
                "risk_mode": int(limits.get("risk_mode", 1)),
                "winrate20": winrate20,
                "rolling_dd_frac": dd_fraction,
                "plan_gap_pct": plan_gap_pct,
                "ai_capacity_delta": ai_capacity_delta,
                "daily_spend_usd": daily_spend_usd,
                "daily_spent_usd": spent_usd,
                "daily_trades_count": trades_today,
                "daily_entries_count": entries_today,
                "kelly_raw": kelly_raw,
                "kelly_pos_pct_cap": kelly_pos_pct_cap,
                "sandbox_mode": bool(RISK_SANDBOX_MODE),
                "risk_profile": profile_name,
                "risk_profile_override": profile_override,
                "regime_gate_factor": regime_gate_factor,
                "equity_utilization_pct": equity_utilization_pct,
                "open_exposure_usd": float(open_exposure_usd),
                "max_exposure_usd": float(max_exposure_usd),
                "trading_window_open": trading_window_open,
                "trading_window_reason": trading_window_reason,
                "risk_engine_version": RISK_ENGINE_VERSION,
            },
        )


async def _broadcast_risk_mode_change(
    *,
    redis,
    new_mode: int,
    halt_reason: str | None,
    capacity: float,
    winrate20: float,
    rolling_dd: float,
    realized_usd: float,
    equity_usd: float,
) -> None:
    global _last_broadcast_mode
    if new_mode == _last_broadcast_mode:
        return
    _last_broadcast_mode = new_mode

    if redis and halt_reason:
        with contextlib.suppress(Exception):
            await redis.set(RISK_HALT_REASON_KEY, str(halt_reason)[:300])

    if RISK_DISABLE_ALERTS:
        return
    if not _alert_ok(f"risk_mode:{new_mode}"):
        return

    send_telegram = getattr(cu, "send_telegram", None)
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
            msg = "🟠 *RISK MODE: DEFENSIVE* — shrinking tickets & concurrency"
        elif new_mode == 2:
            msg = "🟢 *RISK MODE: AGGRESSIVE* — scaling tickets & opportunities"
        else:
            msg = "🔵 *RISK MODE: NORMAL*"
        await _call(send_telegram, msg)
    except Exception:
        pass


# =============================================================================
# Limits dataclass + engine (single-flight + caching)
# =============================================================================
@dataclass
class RiskLimits:
    equity_usd: float
    ticket_usd: float
    pos_pct_cap: float
    max_concurrent: int
    daily_spend_usd: float
    winrate20: float
    rolling_dd_frac: float
    regime: str
    risk_capacity: float
    risk_mode: int
    risk_mode_override: int | None
    regime_risk_multiplier: float
    regime_gate_factor: float
    risk_profile: str
    ai_capacity_delta: float
    halt_reason: str | None
    kelly_raw: float
    kelly_pos_pct_cap: float
    equity_utilization_pct: float
    open_positions: int
    open_exposure_usd: float
    max_exposure_usd: float
    daily_spent_usd: float
    daily_realized_usd: float
    trading_window_open: bool
    trading_window_reason: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "equity_usd": float(self.equity_usd),
            "ticket_usd": float(self.ticket_usd),
            "pos_pct_cap": float(self.pos_pct_cap),
            "max_concurrent": int(self.max_concurrent),
            "daily_spend_usd": float(self.daily_spend_usd),
            "winrate20": float(self.winrate20),
            "rolling_dd_frac": float(self.rolling_dd_frac),
            "regime": str(self.regime),
            "risk_capacity": float(self.risk_capacity),
            "risk_mode": int(self.risk_mode),
            "risk_mode_override": self.risk_mode_override,
            "regime_risk_multiplier": float(self.regime_risk_multiplier),
            "regime_gate_factor": float(self.regime_gate_factor),
            "risk_profile": str(self.risk_profile),
            "ai_capacity_delta": float(self.ai_capacity_delta),
            "halt_reason": self.halt_reason,
            "kelly_raw": float(self.kelly_raw),
            "kelly_pos_pct_cap": float(self.kelly_pos_pct_cap),
            "equity_utilization_pct": float(self.equity_utilization_pct),
            "open_positions": int(self.open_positions),
            "open_exposure_usd": float(self.open_exposure_usd),
            "max_exposure_usd": float(self.max_exposure_usd),
            "daily_spent_usd": float(self.daily_spent_usd),
            "daily_realized_usd": float(self.daily_realized_usd),
            "trading_window_open": bool(self.trading_window_open),
            "trading_window_reason": str(self.trading_window_reason),
            "risk_engine_version": RISK_ENGINE_VERSION,
        }


class _RiskEngine:
    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._cached: dict[str, Any] | None = None
        self._cached_at_monotonic: float = 0.0

    def invalidate(self) -> None:
        self._cached = None
        self._cached_at_monotonic = 0.0

    def _cache_valid(self) -> bool:
        ttl = max(0, int(RISK_LIMITS_CACHE_TTL_MS))
        if ttl <= 0:
            return False
        age_ms = (time.monotonic() - self._cached_at_monotonic) * 1000.0
        return (self._cached is not None) and (age_ms <= ttl)

    async def current_limits(self, redis=None) -> dict[str, Any]:
        if self._cache_valid():
            return dict(self._cached or {})

        async with self._lock:
            if self._cache_valid():
                return dict(self._cached or {})

            if redis is None:
                redis = await _get_redis()

            # Equity sensing
            equity_wallet = await _wallet_equity_usd()
            equity = float(equity_wallet)
            if RISK_EQUITY_OVERRIDE_USD > 0:
                equity = float(RISK_EQUITY_OVERRIDE_USD)

            tier = _select_tier(equity)
            ticket = (
                float(tier["ticket"])
                if tier.get("ticket")
                else max(50.0, equity * float(tier["pos_pct"]) * 0.5)
            )
            pos_pct_cap = float(tier["pos_pct"])
            max_conc = int(tier["max_conc"])
            daily_spend = equity * float(tier["spend_pct"])

            # Metrics
            winrate, dd_frac = await _metrics(redis)
            realized_usd = await _daily_realized_usd(redis)
            spent_usd = await _daily_spent_usd(redis)

            # Kelly clamp
            kelly_raw, kelly_pos_cap = (0.0, 0.0)
            if RISK_KELLY_ENABLED:
                kelly_raw, kelly_pos_cap = _kelly_pos_cap(winrate)
                if kelly_pos_cap > 0:
                    pos_pct_cap = min(pos_pct_cap, kelly_pos_cap)

            # Regime scaling
            regime = regime_state.get()
            regime_mult = float(regime_state.risk_multiplier())
            regime_feats = await regime_state.features()
            regime_vol = float(regime_feats.get("regime_volatility", 0.5))
            regime_chop = float(regime_feats.get("regime_choppy_score", 0.0))
            regime_gate_factor = float(
                regime_feats.get("regime_gate_factor", regime_feats.get("gate_factor", 1.0))
            )
            regime_gate_factor = _clamp(regime_gate_factor, 0.0, 1.0)
            max_conc += int(REGIME_MAX_CONC_DELTA.get(regime, 0))

            # Strategy caps (self-learning)
            try:
                strat_params = await get_strategy_params(redis)
            except Exception:
                strat_params = {}

            strat_max_risk = float(strat_params.get("MAX_RISK_PER_TRADE", RISK_MAX_PER_TRADE))
            strat_max_conc_override: int | None = None
            with contextlib.suppress(Exception):
                v = strat_params.get("MAX_CONCURRENT_OVERRIDE")
                if v is not None:
                    vv = int(v)
                    if vv > 0:
                        strat_max_conc_override = vv

            strat_daily_spend_cap_pct = float(strat_params.get("DAILY_SPEND_CAP_PCT", 0.0))
            strat_daily_spend_cap_usd = float(strat_params.get("DAILY_SPEND_CAP_USD", 0.0))
            plan_gap_pct = float(strat_params.get("PLAN_GAP_PCT", 0.0))

            # Profile multipliers
            profile_name, profile_cfg = await _effective_risk_profile(redis)
            ticket *= profile_cfg["ticket_mult"]
            pos_pct_cap *= profile_cfg["pos_pct_mult"]
            max_conc = int(round(max_conc * profile_cfg["max_conc_mult"]))
            daily_spend *= profile_cfg["daily_spend_mult"]

            # Adaptive throttle (pre-capacity)
            if (winrate < THROTTLE_WINRATE_FLOOR) or (dd_frac > THROTTLE_DD_FLOOR):
                ticket *= 1.0 - THROTTLE_TICKET_CUT
                max_conc = max(1, max_conc - THROTTLE_CONC_DEC)

            # Regime scaling on ticket
            ticket *= regime_mult

            # Respect caps (env + strategy)
            pos_pct_cap = min(pos_pct_cap, RISK_MAX_PER_TRADE, strat_max_risk)
            max_conc = max(0, min(max_conc, MAX_CONCURRENT_POS))
            if strat_max_conc_override is not None:
                max_conc = min(max_conc, strat_max_conc_override)

            # Daily spend caps
            daily_spend = min(daily_spend, equity * DAILY_SPEND_CAP_PCT)
            if DAILY_SPEND_CAP_USD > 0:
                daily_spend = min(daily_spend, DAILY_SPEND_CAP_USD)
            if strat_daily_spend_cap_pct > 0:
                daily_spend = min(daily_spend, equity * strat_daily_spend_cap_pct)
            if strat_daily_spend_cap_usd > 0:
                daily_spend = min(daily_spend, strat_daily_spend_cap_usd)

            # Capacity model
            capacity_raw = _risk_capacity_from_components(
                winrate20=winrate,
                rolling_dd=dd_frac,
                realized_pnl_usd=realized_usd,
                equity_usd=equity,
                regime_mult=regime_mult,
                regime_volatility=regime_vol,
                regime_choppy=regime_chop,
            )

            # Plan bias
            if plan_gap_pct != 0.0:
                if plan_gap_pct > 0:
                    capacity_raw = _clamp(
                        capacity_raw - min(abs(plan_gap_pct) / 100.0 * 0.3, 0.3), 0.0, 1.0
                    )
                else:
                    capacity_raw = _clamp(
                        capacity_raw + min(abs(plan_gap_pct) / 100.0 * 0.2, 0.2), 0.0, 1.0
                    )

            # AI overlay nudge
            ai_capacity_delta = 0.0
            if _predict_capacity_adjustment is not None and not RISK_DISABLE_AI_OVERLAY:
                with contextlib.suppress(Exception):
                    feats = {
                        "equity_usd": float(equity),
                        "winrate20": float(winrate),
                        "rolling_dd": float(dd_frac),
                        "realized_pnl_usd": float(realized_usd),
                        "regime_mult": float(regime_mult),
                        "plan_gap_pct": float(plan_gap_pct),
                        "risk_profile": profile_name,
                    }
                    out = _predict_capacity_adjustment(feats)
                    out = await _maybe_await(out)
                    ai_capacity_delta = float(out or 0.0)

            capacity_raw = _clamp(capacity_raw + ai_capacity_delta, 0.0, 1.0)

            # Apply regime gate factor (hard external throttle)
            capacity_raw = _clamp(capacity_raw * regime_gate_factor, 0.0, 1.0)

            # Derive risk mode
            capacity, risk_mode, override_mode, halt_reason = await _risk_mode_with_override(
                redis, capacity_raw, rolling_dd=dd_frac
            )

            # Capacity exponent (profile-aware)
            capacity_exp = max(
                0.10, float(RISK_CAPACITY_TICKET_EXP_BASE) * float(profile_cfg["capacity_exp_mult"])
            )
            capacity_scale = 0.0 if capacity <= 0 else math.pow(float(capacity), capacity_exp)

            ticket *= capacity_scale
            daily_spend *= capacity_scale

            # Risk-mode concurrency adjustments
            if risk_mode == 0:
                max_conc = 0
            elif risk_mode == 3:
                max_conc = max(0, int(round(max_conc * 0.5)))

            # Sandbox clamp
            if RISK_SANDBOX_MODE:
                ticket = min(ticket, 5.0)
                daily_spend = min(daily_spend, 20.0)
                max_conc = min(max_conc, 1)

            # Equity utilization guardrail
            equity_utilization_pct = 0.0
            if equity > 0 and max_conc > 0:
                potential = ticket * max_conc
                max_notional = equity * (RISK_MAX_EQUITY_UTILIZATION_PCT / 100.0)
                if max_notional > 0 and potential > max_notional:
                    scale = max_notional / max(1e-9, potential)
                    ticket *= max(0.0, scale)
                    daily_spend = min(daily_spend, max_notional)
                equity_utilization_pct = min(100.0, (ticket * max_conc) / equity * 100.0)

            # Round ticket & clamp min/max
            if ticket < 10:
                ticket = max(RISK_MIN_TICKET_USD, round(ticket, 2))
            elif ticket < 100:
                ticket = max(RISK_MIN_TICKET_USD, round(ticket / 5.0) * 5.0)
            else:
                ticket = max(RISK_MIN_TICKET_USD, round(ticket / 10.0) * 10.0)
            ticket = min(ticket, max(RISK_MIN_TICKET_USD, RISK_MAX_TICKET_USD))

            # Live overrides
            overrides = await _apply_overrides(
                redis,
                ticket_usd=ticket,
                pos_pct_cap=pos_pct_cap,
                max_concurrent=max_conc,
                daily_spend_usd=daily_spend,
            )
            ticket = float(overrides["ticket_usd"])
            pos_pct_cap = float(overrides["pos_pct_cap"])
            max_conc = int(overrides["max_concurrent"])
            daily_spend = float(overrides["daily_spend_usd"])

            # Final per-trade clamp from pos_pct_cap
            if equity > 0 and pos_pct_cap > 0:
                ticket = min(ticket, equity * pos_pct_cap)

            open_positions = _positions_open_count()
            open_exposure_usd = _positions_open_exposure_usd()

            # Exposure cap (optional)
            max_exposure_usd = 0.0
            if RISK_MAX_EXPOSURE_USD > 0:
                max_exposure_usd = float(RISK_MAX_EXPOSURE_USD)
            elif RISK_MAX_EXPOSURE_PCT > 0 and equity > 0:
                max_exposure_usd = float(equity) * (float(RISK_MAX_EXPOSURE_PCT) / 100.0)

            # Trading window
            tw_open, tw_reason = _trading_window_status()

            limits_obj = RiskLimits(
                equity_usd=float(equity),
                ticket_usd=float(ticket),
                pos_pct_cap=float(pos_pct_cap),
                max_concurrent=int(max_conc),
                daily_spend_usd=float(daily_spend),
                winrate20=float(winrate),
                rolling_dd_frac=float(dd_frac),
                regime=str(regime),
                risk_capacity=float(capacity),
                risk_mode=int(risk_mode),
                risk_mode_override=override_mode,
                regime_risk_multiplier=float(regime_mult),
                regime_gate_factor=float(regime_gate_factor),
                risk_profile=str(profile_name),
                ai_capacity_delta=float(ai_capacity_delta),
                halt_reason=halt_reason,
                kelly_raw=float(kelly_raw),
                kelly_pos_pct_cap=float(kelly_pos_cap),
                equity_utilization_pct=float(equity_utilization_pct),
                open_positions=int(open_positions),
                open_exposure_usd=float(open_exposure_usd),
                max_exposure_usd=float(max_exposure_usd),
                daily_spent_usd=float(spent_usd),
                daily_realized_usd=float(realized_usd),
                trading_window_open=bool(tw_open),
                trading_window_reason=str(tw_reason),
            )
            limits = limits_obj.to_dict()

            # Cooldown status
            cooldown_until: float | None = None
            if redis:
                with contextlib.suppress(Exception):
                    v = await redis.get(RISK_COOLDOWN_UNTIL_KEY)
                    if v is not None:
                        cooldown_until = float(v)

            # Publish risk:ctx
            with contextlib.suppress(Exception):
                consec_losses = await _consecutive_losses(redis)
                await _update_risk_ctx(
                    redis,
                    limits=limits,
                    realized_usd=float(realized_usd),
                    winrate20=float(winrate),
                    consec_losses=int(consec_losses),
                    cooldown_until_ts=cooldown_until,
                    strat_params=strat_params,
                    profile_name=profile_name,
                    ai_capacity_delta=float(ai_capacity_delta),
                    halt_reason=halt_reason,
                    kelly_raw=float(kelly_raw),
                    kelly_pos_pct_cap=float(kelly_pos_cap),
                    regime_gate_factor=float(regime_gate_factor),
                    equity_utilization_pct=float(equity_utilization_pct),
                    open_exposure_usd=float(open_exposure_usd),
                    max_exposure_usd=float(max_exposure_usd),
                    trading_window_open=bool(tw_open),
                    trading_window_reason=str(tw_reason),
                )

            with contextlib.suppress(Exception):
                await _broadcast_risk_mode_change(
                    redis=redis,
                    new_mode=int(limits["risk_mode"]),
                    halt_reason=halt_reason,
                    capacity=float(limits["risk_capacity"]),
                    winrate20=float(limits["winrate20"]),
                    rolling_dd=float(limits["rolling_dd_frac"]),
                    realized_usd=float(realized_usd),
                    equity_usd=float(limits["equity_usd"]),
                )

            self._cached = dict(limits)
            self._cached_at_monotonic = time.monotonic()
            return dict(limits)


_ENGINE = _RiskEngine()


def invalidate_limits_cache() -> None:
    _ENGINE.invalidate()


# =============================================================================
# Public API: limits/sizing
# =============================================================================
async def current_limits(redis=None) -> dict[str, Any]:
    return await _ENGINE.current_limits(redis)


async def recommend_ticket_usd(redis=None) -> float:
    return float((await current_limits(redis)).get("ticket_usd", 0.0))


async def equity_and_ticket(redis=None) -> dict[str, Any]:
    limits = await current_limits(redis)
    return {
        "equity_usd": float(limits.get("equity_usd", 0.0)),
        "ticket_usd": float(limits.get("ticket_usd", 0.0)),
        "risk_capacity": float(limits.get("risk_capacity", 1.0)),
        "risk_mode": int(limits.get("risk_mode", 1)),
        "ctx_digest": await _read_ctx_digest(redis),
    }


# =============================================================================
# Budget reservation (race-safe entry path)
# =============================================================================
_LUA_RESERVE_CAP = r"""
local key = KEYS[1]
local cap = tonumber(ARGV[1])
local inc = tonumber(ARGV[2])
local ttl = tonumber(ARGV[3])
local cur = tonumber(redis.call('GET', key) or '0')
if cap > 0 and (cur + inc) > cap then
  return 0
end
redis.call('INCRBYFLOAT', key, inc)
if ttl and ttl > 0 then
  redis.call('EXPIRE', key, ttl)
end
return 1
"""

_LUA_RELEASE_FLOOR0 = r"""
local key = KEYS[1]
local dec = tonumber(ARGV[1])
local ttl = tonumber(ARGV[2])
local cur = tonumber(redis.call('GET', key) or '0')
local nxt = cur - dec
if nxt < 0 then nxt = 0 end
redis.call('SET', key, tostring(nxt))
if ttl and ttl > 0 then
  redis.call('EXPIRE', key, ttl)
end
return 1
"""

_LUA_RESERVE_INCR = r"""
local key = KEYS[1]
local cap = tonumber(ARGV[1])
local ttl = tonumber(ARGV[2])
local cur = tonumber(redis.call('GET', key) or '0')
if cap > 0 and (cur + 1) > cap then
  return 0
end
redis.call('INCR', key)
if ttl and ttl > 0 then
  redis.call('EXPIRE', key, ttl)
end
return 1
"""


async def reserve_entry_budget(usd: float, redis=None) -> bool:
    """
    Atomically reserve today's spend budget for an entry.
    Also refuses if a hard halt is active (instant deterministic).
    """
    usd = float(usd or 0.0)
    if usd <= 0:
        return True
    if redis is None:
        redis = await _get_redis()
    if not redis:
        return True  # no redis -> allow (other caps still apply)

    halted, _ = await _is_halted_fast(redis)
    if halted:
        return False

    limits = await current_limits(redis)
    cap = float(limits.get("daily_spend_usd", 0.0))
    ttl = _ttl_to_midnight_sec()
    key = _spent_key_today()

    try:
        ok = await redis.eval(_LUA_RESERVE_CAP, 1, key, cap, usd, ttl)
        ok = int(ok) == 1
        with contextlib.suppress(Exception):
            await log_event(
                "risk_reserve_spend_ok" if ok else "risk_reserve_spend_block",
                {"usd": usd, "cap": cap, "sandbox_mode": bool(RISK_SANDBOX_MODE)},
            )
        return ok
    except Exception:
        # fallback non-atomic
        try:
            spent = await _daily_spent_usd(redis)
            if cap > 0 and (spent + usd) > cap:
                return False
            await redis.incrbyfloat(key, usd)
            await _set_expire_at_midnight(redis, key)
            return True
        except Exception:
            return True


async def release_entry_budget(usd: float, redis=None) -> None:
    """If you reserved spend but BUY never happened, refund budget (floors at 0)."""
    usd = float(usd or 0.0)
    if usd <= 0:
        return
    if redis is None:
        redis = await _get_redis()
    if not redis:
        return
    ttl = _ttl_to_midnight_sec()
    key = _spent_key_today()
    with contextlib.suppress(Exception):
        await redis.eval(_LUA_RELEASE_FLOOR0, 1, key, usd, ttl)
        await log_event("risk_release_spend", {"usd": usd, "sandbox_mode": bool(RISK_SANDBOX_MODE)})


async def reserve_daily_entries_slot(redis=None) -> bool:
    """If you enforce RISK_MAX_DAILY_ENTRIES, call this before entry."""
    if RISK_MAX_DAILY_ENTRIES <= 0:
        return True
    if redis is None:
        redis = await _get_redis()
    if not redis:
        return True

    halted, _ = await _is_halted_fast(redis)
    if halted:
        return False

    ttl = _ttl_to_midnight_sec()
    key = _entries_key_today()
    try:
        ok = await redis.eval(_LUA_RESERVE_INCR, 1, key, int(RISK_MAX_DAILY_ENTRIES), ttl)
        return int(ok) == 1
    except Exception:
        with contextlib.suppress(Exception):
            cur = await _daily_entries(redis)
            if cur + 1 > int(RISK_MAX_DAILY_ENTRIES):
                return False
            await redis.incr(key)
            await _set_expire_at_midnight(redis, key)
            return True
        return True


async def note_entry_attempt(
    *,
    mint: str,
    ticket_usd: float,
    symbol: str | None = None,
    meta: dict[str, Any] | None = None,
    redis=None,
) -> None:
    """Best-effort entry telemetry (NOT a budget reservation)."""
    if redis is None:
        redis = await _get_redis()
    with contextlib.suppress(Exception):
        await log_event(
            "entry_attempt",
            {
                "mint": mint,
                "symbol": symbol or "",
                "ticket_usd": float(ticket_usd),
                "meta": meta or {},
                "sandbox_mode": bool(RISK_SANDBOX_MODE),
            },
        )


# =============================================================================
# Trade metrics ingestion
# =============================================================================
async def _push_trade_metrics(redis, profit_usd: float, equity_after: float) -> None:
    if not redis:
        return
    try:
        await redis.incr(METRICS_TRADES_KEY)
        if profit_usd > 0:
            await redis.incr(METRICS_WINS_KEY)

        await redis.lpush(METRICS_LAST20_KEY, 1 if profit_usd > 0 else 0)
        await redis.ltrim(METRICS_LAST20_KEY, 0, 19)
        last20 = await redis.lrange(METRICS_LAST20_KEY, 0, 19)

        wins20 = 0
        for x in last20 or []:
            with contextlib.suppress(Exception):
                wins20 += int(x)
        winrate20 = wins20 / max(1, len(last20 or []))
        await redis.set(METRICS_WINRATE20, f"{winrate20:.4f}")

        hwm_raw = await redis.get(EQUITY_HWM_KEY)
        hwm = float(hwm_raw) if hwm_raw else equity_after
        if equity_after > hwm:
            hwm = equity_after
        await redis.set(EQUITY_HWM_KEY, f"{hwm:.2f}")

        dd = 0.0 if hwm <= 0 else (hwm - equity_after) / hwm
        await redis.set(METRICS_ROLLING_DD, f"{dd:.4f}")
    except Exception:
        pass


def _mint_cooldown_key(mint: str) -> str:
    return f"risk:mint_cooldown_until:{mint}"


async def set_mint_cooldown(mint: str, *, minutes: int, redis=None, reason: str = "") -> None:
    if minutes <= 0:
        return
    if redis is None:
        redis = await _get_redis()
    if not redis:
        return
    try:
        until = _now_ts() + float(minutes) * 60.0
        await redis.set(_mint_cooldown_key(mint), f"{until:.1f}", ex=int(minutes * 60))
        await log_event(
            "risk_mint_cooldown_set", {"mint": mint, "minutes": minutes, "reason": reason}
        )
    except Exception:
        pass


async def clear_mint_cooldown(mint: str, redis=None) -> None:
    if redis is None:
        redis = await _get_redis()
    if not redis:
        return
    try:
        await redis.delete(_mint_cooldown_key(mint))
        await log_event("risk_mint_cooldown_cleared", {"mint": mint})
    except Exception:
        pass


async def _mint_cooldown_active(mint: str | None, redis) -> tuple[bool, float]:
    if not mint or not redis:
        return False, 0.0
    try:
        v = await redis.get(_mint_cooldown_key(mint))
        if v is None:
            return False, 0.0
        until = float(v)
        return (until > _now_ts()), until
    except Exception:
        return False, 0.0


async def note_trade_result(
    token_address: str,
    *,
    profit_usd: float,
    equity_after_usd: float | None = None,
    meta: dict[str, Any] | None = None,
    trade_id: str | None = None,
    redis=None,
) -> None:
    """
    Canonical hook called by ANY exit path to update:
      - daily realized pnl
      - rolling metrics (winrate20, DD)
      - daily trade counter (completed trades)
      - cooldown windows (loss streak / DD)
      - regime_state account context
      - self-learning trade closure hook
      - Notion + structured logs (best effort)
      - optional per-mint cooldown
      - idempotency via trade_id
    """
    try:
        if redis is None:
            redis = await _get_redis()
        if not redis:
            return

        # Idempotency
        if trade_id:
            k = _closed_trade_key(str(trade_id).strip())
            if k.strip():
                already = False
                with contextlib.suppress(Exception):
                    already = bool(await redis.get(k))
                if already:
                    return
                with contextlib.suppress(Exception):
                    await redis.set(k, "1", ex=int(7 * 24 * 3600))  # 7d

        pnl_key = _pnl_key_today()
        await redis.incrbyfloat(pnl_key, float(profit_usd))
        await _set_expire_at_midnight(redis, pnl_key)

        if equity_after_usd is None:
            equity_after_usd = await _wallet_equity_usd()
        equity_after_usd = float(equity_after_usd or 0.0)

        await _push_trade_metrics(redis, float(profit_usd), equity_after_usd)

        # Daily completed trades counter
        with contextlib.suppress(Exception):
            k = _trades_key_today()
            await redis.incr(k)
            await _set_expire_at_midnight(redis, k)

        # Consecutive-loss cooldown
        if RISK_COOLDOWN_CONSEC_LOSSES > 0 and RISK_COOLDOWN_MINUTES > 0 and profit_usd < 0:
            with contextlib.suppress(Exception):
                consec = await _consecutive_losses(redis)
                if consec >= RISK_COOLDOWN_CONSEC_LOSSES:
                    until = _now_ts() + float(RISK_COOLDOWN_MINUTES) * 60.0
                    await redis.set(RISK_COOLDOWN_UNTIL_KEY, f"{until:.1f}")
                    if not RISK_DISABLE_ALERTS:
                        send_telegram = getattr(cu, "send_telegram", None)
                        await _call(
                            send_telegram,
                            "⚠️ *RISK COOLDOWN* — consecutive losses threshold hit\n"
                            f"• consecutive_losses={consec}\n"
                            f"• cooldown={RISK_COOLDOWN_MINUTES} min",
                        )

        # DD cooldown (optional)
        if RISK_DD_COOLDOWN_FLOOR_PCT > 0 and RISK_DD_COOLDOWN_MINUTES > 0:
            with contextlib.suppress(Exception):
                _, dd_frac = await _metrics(redis)
                dd_pct = dd_frac * 100.0
                if dd_pct >= float(RISK_DD_COOLDOWN_FLOOR_PCT):
                    until = _now_ts() + float(RISK_DD_COOLDOWN_MINUTES) * 60.0
                    await redis.set(RISK_COOLDOWN_UNTIL_KEY, f"{until:.1f}")
                    if not RISK_DISABLE_ALERTS:
                        send_telegram = getattr(cu, "send_telegram", None)
                        await _call(
                            send_telegram,
                            "⚠️ *RISK DD COOLDOWN* — drawdown threshold hit\n"
                            f"• rolling_dd={dd_pct:.2f}% ≥ floor={RISK_DD_COOLDOWN_FLOOR_PCT:.2f}%\n"
                            f"• cooldown={RISK_DD_COOLDOWN_MINUTES} min",
                        )

        # Optional per-mint cooldown (anti-chop)
        with contextlib.suppress(Exception):
            mint_cd = int(
                (meta or {}).get("mint_cooldown_min") or RISK_MINT_COOLDOWN_MINUTES_DEFAULT
            )
            if mint_cd > 0:
                await set_mint_cooldown(
                    token_address,
                    minutes=mint_cd,
                    redis=redis,
                    reason=f"trade_close_{profit_usd:.2f}",
                )

        # Push account state into regime
        with contextlib.suppress(Exception):
            winrate20, dd_frac = await _metrics(redis)
            realized = await _daily_realized_usd(redis)
            equity_now = await _wallet_equity_usd()
            equity_now = float(equity_now or 0.0)
            daily_pnl_pct = 0.0 if equity_now <= 0 else (realized / equity_now) * 100.0
            daily_dd_pct = float(dd_frac) * 100.0
            consec = await _consecutive_losses(redis)
            await regime_state.update_from_account(
                daily_realized_pnl_pct=float(daily_pnl_pct),
                daily_drawdown_pct=float(daily_dd_pct),
                consecutive_losses=int(consec),
            )

        # Notion (best effort)
        if not RISK_DISABLE_NOTION:
            log_to_notion = getattr(cu, "log_to_notion", None)
            with contextlib.suppress(Exception):
                await _call(
                    log_to_notion,
                    token=token_address,
                    symbol=(meta or {}).get("symbol") or token_address[:6],
                    balance=(meta or {}).get("amount", 0.0),
                    entry=(meta or {}).get("entry", 0.0),
                    current=(meta or {}).get("exit", 0.0),
                    profit=float(profit_usd),
                    reason=(meta or {}).get("reason", "risk_manager.note_trade_result"),
                )

        # Structured log
        with contextlib.suppress(Exception):
            await log_event(
                "trade_result",
                {
                    "token": token_address,
                    "profit_usd": float(profit_usd),
                    "equity_after_usd": float(equity_after_usd),
                    "trade_id": trade_id,
                    "meta": meta or {},
                    "sandbox_mode": bool(RISK_SANDBOX_MODE),
                },
            )

        # Self-learning hook
        with contextlib.suppress(Exception):
            await process_trade_closure(
                token_address=token_address,
                profit_usd=float(profit_usd),
                equity_after_usd=float(equity_after_usd),
                meta=meta or {},
                redis=redis,
            )

        # After any trade close, invalidate cached limits
        invalidate_limits_cache()

    except Exception as e:
        with contextlib.suppress(Exception):
            log.warning("[RISK] note_trade_result failed for %s: %s", token_address, e)


async def note_spend(*args, **kwargs) -> None:
    """
    Backwards compatible spend increment.

    Preferred: reserve_entry_budget() (atomic).
    Accepts both:
      - note_spend(redis, usd)
      - note_spend(usd, redis=redis)
    """
    redis = None
    usd = 0.0

    if len(args) == 2:
        redis, usd = args[0], args[1]
    elif len(args) == 1:
        usd = args[0]
        redis = kwargs.get("redis")
    else:
        usd = kwargs.get("usd", 0.0)
        redis = kwargs.get("redis")

    with contextlib.suppress(Exception):
        usd = float(usd or 0.0)

    if redis is None:
        redis = await _get_redis()
    if not redis or usd <= 0:
        return

    try:
        k = _spent_key_today()
        await redis.incrbyfloat(k, float(usd))
        await _set_expire_at_midnight(redis, k)
        with contextlib.suppress(Exception):
            await log_event(
                "risk_spend", {"usd": float(usd), "sandbox_mode": bool(RISK_SANDBOX_MODE)}
            )
        invalidate_limits_cache()
    except Exception:
        pass


# =============================================================================
# Entry gating (explainable, no silent blocks)
# =============================================================================
async def should_allow_new_entry(redis=None) -> bool:
    info = await entry_gate_debug(redis=redis)
    return bool(info.get("allowed", True))


async def entry_gate_debug(
    redis=None,
    *,
    mint: str | None = None,
    pool_depth_usd: float | None = None,
    projected_ticket_usd: float | None = None,
) -> dict[str, Any]:
    """
    Full explainable gate with concrete reasons.

    Instant deterministic stop:
      - checks halt keys FIRST (does not depend on cached limits)

    Extra args:
      - pool_depth_usd: enables liq-adjusted ticket visibility
      - projected_ticket_usd: if caller already decided a ticket, use it for exposure check
    """
    if redis is None:
        redis = await _get_redis()

    reasons: list[str] = []

    # 0) Hard halt / manual / infra / HARD_HALT (instant)
    halted, halt_reason = await _is_halted_fast(redis)
    if halted:
        reasons.append(halt_reason or "halted")

    # 1) trading window
    tw_open, tw_reason = _trading_window_status()
    if not tw_open:
        reasons.append(f"trading window closed ({tw_reason})")

    # 2) limits (cached)
    limits = await current_limits(redis)
    equity = float(limits.get("equity_usd", 0.0))
    risk_mode = int(limits.get("risk_mode", 1))
    capacity = float(limits.get("risk_capacity", 1.0))
    max_conc = int(limits.get("max_concurrent", 0))
    daily_spend_cap = float(limits.get("daily_spend_usd", 0.0))

    # 3) open positions / concurrency
    open_positions = _positions_open_count()
    if max_conc > 0 and open_positions >= max_conc:
        reasons.append(f"open_positions {open_positions} ≥ max_concurrent {max_conc}")

    # 4) per-mint cooldown
    mint_cd_active = False
    mint_cd_until = 0.0
    if mint and redis:
        mint_cd_active, mint_cd_until = await _mint_cooldown_active(mint, redis)
        if mint_cd_active:
            rem = max(0.0, mint_cd_until - _now_ts())
            reasons.append(f"mint cooldown active ({rem:.0f}s remaining)")

    # 5) strategy params (best effort)
    try:
        strat_params = await get_strategy_params(redis)
    except Exception:
        strat_params = {}

    hard_halt = str(strat_params.get("HARD_HALT", "0")).strip().lower() in (
        "1",
        "true",
        "yes",
        "on",
    )
    if hard_halt and ("strategy HARD_HALT=1" not in reasons):
        reasons.append("strategy HARD_HALT=1")

    plan_gap_pct = float(strat_params.get("PLAN_GAP_PCT", 0.0))
    if RISK_PLAN_AHEAD_HARD_STOP_PCT > 0 and plan_gap_pct >= float(RISK_PLAN_AHEAD_HARD_STOP_PCT):
        reasons.append(
            f"plan_gap_pct {plan_gap_pct:.2f}% ≥ hard-stop {RISK_PLAN_AHEAD_HARD_STOP_PCT:.2f}%"
        )

    # 6) risk mode + capacity floor
    if risk_mode == 0:
        reasons.append("risk_mode=0 (OFF)")
    if capacity < float(RISK_MIN_CAPACITY_FOR_ENTRY):
        reasons.append(f"risk_capacity {capacity:.3f} < floor {RISK_MIN_CAPACITY_FOR_ENTRY:.3f}")

    # 7) regime hard risk-off
    with contextlib.suppress(Exception):
        if regime_state.is_hard_risk_off():
            reasons.append("regime_state.is_hard_risk_off()")

    # 8) cooldown active
    cooldown_active = False
    if redis:
        with contextlib.suppress(Exception):
            v = await redis.get(RISK_COOLDOWN_UNTIL_KEY)
            if v is not None and float(v) > _now_ts():
                cooldown_active = True
                reasons.append("loss/DD cooldown active")

    # 9) equity floor
    if RISK_MIN_EQUITY_USD > 0 and equity < float(RISK_MIN_EQUITY_USD):
        reasons.append(f"equity_usd {equity:.2f} < RISK_MIN_EQUITY_USD {RISK_MIN_EQUITY_USD:.2f}")

    # 10) profit stop + loss cap
    realized = await _daily_realized_usd(redis)
    profit_stop_usd = equity * (STOP_AFTER_DAILY_PROFIT_PCT / 100.0)
    loss_cap_usd = equity * (DAILY_LOSS_CAP_PCT / 100.0) if DAILY_LOSS_CAP_PCT > 0 else 0.0
    if profit_stop_usd > 0 and realized >= profit_stop_usd:
        reasons.append(f"stop-after-profit hit ({realized:.2f} ≥ {profit_stop_usd:.2f})")
    if loss_cap_usd > 0 and realized <= -abs(loss_cap_usd):
        reasons.append(f"daily loss cap hit ({realized:.2f} ≤ -{abs(loss_cap_usd):.2f})")

    # 11) spend budget
    spent_usd = await _daily_spent_usd(redis)
    if daily_spend_cap > 0 and spent_usd >= daily_spend_cap:
        reasons.append(f"daily spend cap hit (spent {spent_usd:.2f} ≥ cap {daily_spend_cap:.2f})")

    # 12) daily caps
    trades_today = await _daily_trades(redis)
    if RISK_MAX_DAILY_TRADES > 0 and trades_today >= int(RISK_MAX_DAILY_TRADES):
        reasons.append(f"daily trades cap hit ({trades_today} ≥ cap {RISK_MAX_DAILY_TRADES})")

    entries_today = await _daily_entries(redis)
    if RISK_MAX_DAILY_ENTRIES > 0 and entries_today >= int(RISK_MAX_DAILY_ENTRIES):
        reasons.append(f"daily entries cap hit ({entries_today} ≥ cap {RISK_MAX_DAILY_ENTRIES})")

    # 13) MIN_SOL_BALANCE from strategy
    strat_min_sol_balance = float(strat_params.get("MIN_SOL_BALANCE", 0.0))
    sol_bal = 0.0
    if strat_min_sol_balance > 0:
        sol_bal = await _wallet_sol_balance()
        if sol_bal < strat_min_sol_balance:
            reasons.append(
                f"SOL balance {sol_bal:.4f} < MIN_SOL_BALANCE {strat_min_sol_balance:.4f}"
            )

    # 14) exposure cap (optional)
    open_exposure = _positions_open_exposure_usd()
    max_exposure = 0.0
    if RISK_MAX_EXPOSURE_USD > 0:
        max_exposure = float(RISK_MAX_EXPOSURE_USD)
    elif RISK_MAX_EXPOSURE_PCT > 0 and equity > 0:
        max_exposure = float(equity) * (float(RISK_MAX_EXPOSURE_PCT) / 100.0)

    intended_ticket = (
        float(projected_ticket_usd)
        if projected_ticket_usd is not None
        else float(limits.get("ticket_usd", 0.0))
    )
    if pool_depth_usd is not None:
        intended_ticket = liq_aware_ticket_usd(intended_ticket, float(pool_depth_usd))

    if max_exposure > 0 and (open_exposure + max(0.0, intended_ticket)) > max_exposure:
        reasons.append(
            f"max exposure hit (open {open_exposure:.2f} + ticket {intended_ticket:.2f} > cap {max_exposure:.2f})"
        )

    profile_name, _ = await _effective_risk_profile(redis)
    allowed = len(reasons) == 0

    regime_feats = await regime_state.features()
    regime_entry_gate_factor = float(
        regime_feats.get(
            "regime_entry_gate_factor",
            regime_feats.get("gate_factor", regime_feats.get("regime_risk_multiplier", 1.0)),
        )
    )

    ticket_usd_liq = None
    if pool_depth_usd is not None:
        ticket_usd_liq = liq_aware_ticket_usd(
            float(limits.get("ticket_usd", 0.0)), float(pool_depth_usd)
        )

    return {
        "allowed": bool(allowed),
        "reasons_blocking": reasons,
        "limits": limits,
        "daily_realized_usd": float(realized),
        "profit_stop_usd": float(profit_stop_usd),
        "loss_cap_usd": float(loss_cap_usd),
        "spent_usd": float(spent_usd),
        "sol_balance": float(sol_bal),
        "cooldown_active": bool(cooldown_active),
        "hard_halt": bool(hard_halt),
        "plan_gap_pct": float(plan_gap_pct),
        "plan_ahead_hard_stop": float(RISK_PLAN_AHEAD_HARD_STOP_PCT),
        "trade_count_today": int(trades_today),
        "trade_cap_today": int(RISK_MAX_DAILY_TRADES),
        "entries_count_today": int(entries_today),
        "entries_cap_today": int(RISK_MAX_DAILY_ENTRIES),
        "equity_floor_usd": float(RISK_MIN_EQUITY_USD),
        "sandbox_mode": bool(RISK_SANDBOX_MODE),
        "risk_profile": profile_name,
        "risk_profile_env": RISK_PROFILE,
        "regime_entry_gate_factor": float(regime_entry_gate_factor),
        "trading_window_open": bool(tw_open),
        "trading_window_reason": str(tw_reason),
        "risk_engine_version": RISK_ENGINE_VERSION,
        "open_positions": int(open_positions),
        "max_concurrent": int(max_conc),
        "positions_open_ge_max": bool(max_conc > 0 and open_positions >= max_conc),
        "mint": mint,
        "mint_cooldown_active": bool(mint_cd_active),
        "mint_cooldown_until_ts": float(mint_cd_until),
        "ticket_usd_liq": ticket_usd_liq,
        "open_exposure_usd": float(open_exposure),
        "max_exposure_usd": float(max_exposure),
        "intended_ticket_usd_for_exposure": float(intended_ticket),
        "ctx_digest": await _read_ctx_digest(redis),
    }


# =============================================================================
# Snapshots / health
# =============================================================================
async def read_risk_ctx_hash(redis=None) -> dict[str, float]:
    """Backwards compatible: small numeric hash-like subset."""
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
        with contextlib.suppress(Exception):
            return float(h.get(name, default))
        return default

    return {
        "open_positions": _f("open_positions", 0.0),
        "max_positions": _f("max_positions", 0.0),
        "daily_realized_pnl_pct": _f("daily_realized_pnl_pct", 0.0),
        "daily_drawdown_pct": _f("daily_drawdown_pct", 0.0),
        "risk_mode": _f("risk_mode", 1.0),
        "risk_capacity": _f("risk_capacity", 1.0),
    }


async def risk_capacity_snapshot(redis=None) -> dict[str, Any]:
    """
    Capacity snapshot (God-Tier / quant-grade, import-safe, test-safe).

    Goals:
      - In contract/no-IO mode, return a stable schema WITHOUT touching redis/network.
      - In normal mode, compute a full snapshot best-effort (never crash callers).
      - Keep backwards compatibility with legacy keys (halted/degraded/reason/ticket_usd/etc.).
    """
    # -------------------------------------------------------------------------
    # Contract / CI / no-network mode: return deterministic minimal schema
    # -------------------------------------------------------------------------
    if _contract_no_io():
        now = time.time()
        prof = (
            (os.getenv("RISK_PROFILE") or os.getenv("RISK_PROFILE_DEFAULT") or "balanced")
            .strip()
            .lower()
        )
        equity_override = (
            float(RISK_EQUITY_OVERRIDE_USD) if float(RISK_EQUITY_OVERRIDE_USD or 0) > 0 else 0.0
        )

        return {
            # --- required meta
            "ts": now,
            "mode": "contract_no_io",
            "profile": prof,
            "profile_name": prof,
            "risk_profile": prof,
            "risk_profile_env": RISK_PROFILE,
            "risk_profile_override": None,
            "risk_engine_version": RISK_ENGINE_VERSION,
            # --- capacity (safe defaults)
            "risk_capacity": 1.0,
            "risk_mode": 1,
            "risk_mode_override": None,
            "ai_capacity_delta": 0.0,
            # --- legacy compatibility flags/fields
            "halted": False,
            "degraded": False,
            "reason": "contract_no_io",
            "halt_reason": None,
            "cooldown_until": 0.0,
            "ticket_usd": 0.0,
            "max_ticket_usd": 0.0,
            # --- metrics placeholders
            "winrate20": 0.50,
            "rolling_dd_frac": 0.0,
            "equity_usd": equity_override,
            "equity_hwm_usd": 0.0,
            "daily_realized_pnl_usd": 0.0,
            "daily_realized_pnl_pct": 0.0,
            "daily_drawdown_pct": 0.0,
            "consecutive_losses": 0,
            # --- regime placeholders
            "regime_label": "contract_no_io",
            "regime_risk_multiplier": 1.0,
            "regime_label_numeric": 0.0,
            "regime_stress_score": 0.0,
            "regime_signal_strength": 0.0,
            "regime_entry_gate_factor": 1.0,
            # --- limits placeholders
            "daily_trades_count": 0,
            "daily_trades_cap": int(RISK_MAX_DAILY_TRADES),
            "daily_entries_count": 0,
            "daily_entries_cap": int(RISK_MAX_DAILY_ENTRIES),
            # --- ops state
            "cooldown_active": False,
            "sandbox_mode": bool(RISK_SANDBOX_MODE),
            "kelly_raw": 0.0,
            "kelly_pos_pct_cap": 0.0,
            "equity_override_usd": float(RISK_EQUITY_OVERRIDE_USD),
            # --- trading window
            "trading_window_open": True,
            "trading_window_reason": "contract_no_io",
            # --- fast halt channel
            "halted_fast": False,
            "halt_reason_fast": "",
            # --- ctx cache
            "ctx_digest": "contract_no_io",
        }

    # -------------------------------------------------------------------------
    # Normal mode: compute best-effort snapshot (never crash callers)
    # -------------------------------------------------------------------------
    degraded = False
    now = time.time()

    if redis is None:
        try:
            redis = await _get_redis()
        except Exception:
            redis = None
            degraded = True

    # profile
    profile_name = (
        (os.getenv("RISK_PROFILE") or os.getenv("RISK_PROFILE_DEFAULT") or "balanced")
        .strip()
        .lower()
    )
    profile_override: str | None = None
    try:
        pn, _ = await _effective_risk_profile(redis)
        if pn:
            profile_name = str(pn).strip().lower()
    except Exception:
        degraded = True

    if redis:
        with contextlib.suppress(Exception):
            v = await redis.get(RISK_PROFILE_OVERRIDE_KEY)
            if v:
                profile_override = _s(v).strip().lower()

    # equity
    equity = 0.0
    with contextlib.suppress(Exception):
        equity = float(await _wallet_equity_usd())
    if float(RISK_EQUITY_OVERRIDE_USD or 0) > 0:
        equity = float(RISK_EQUITY_OVERRIDE_USD)

    # metrics
    winrate = 0.50
    dd_frac = 0.0
    realized = 0.0
    trades_today = 0
    entries_today = 0
    consec_losses = 0
    equity_hwm = 0.0

    with contextlib.suppress(Exception):
        winrate, dd_frac = await _metrics(redis)
    with contextlib.suppress(Exception):
        realized = await _daily_realized_usd(redis)
    if redis:
        with contextlib.suppress(Exception):
            trades_today = await _daily_trades(redis)
        with contextlib.suppress(Exception):
            entries_today = await _daily_entries(redis)
        with contextlib.suppress(Exception):
            consec_losses = await _consecutive_losses(redis)
        with contextlib.suppress(Exception):
            equity_hwm = await _equity_hwm(redis)

    # strategy plan knobs
    plan_equity_today = 0.0
    plan_gap_pct = 0.0
    plan_status = None
    try:
        strat_params = await get_strategy_params(redis)
        plan_equity_today = float(strat_params.get("PLAN_EQUITY_TODAY_USD", 0.0))
        plan_gap_pct = float(strat_params.get("PLAN_GAP_PCT", 0.0))
        plan_status = strat_params.get("PLAN_STATUS")
    except Exception:
        degraded = True

    # regime
    regime = "unknown"
    regime_mult = 1.0
    regime_label_numeric = 0.0
    regime_stress_score = 0.0
    regime_signal_strength = 0.0
    regime_entry_gate_factor = 1.0
    regime_vol = 0.5
    regime_chop = 0.0

    with contextlib.suppress(Exception):
        regime = regime_state.get()
    with contextlib.suppress(Exception):
        regime_mult = float(regime_state.risk_multiplier())
    try:
        regime_feats = await regime_state.features()
        regime_vol = float(regime_feats.get("regime_volatility", 0.5))
        regime_chop = float(regime_feats.get("regime_choppy_score", 0.0))
        regime_label_numeric = float(regime_feats.get("regime_label_numeric", 0.0))
        regime_stress_score = float(regime_feats.get("regime_stress_score", regime_vol))
        regime_signal_strength = float(
            regime_feats.get("regime_signal_strength", regime_feats.get("regime_meme_heat", 0.5))
        )
        regime_entry_gate_factor = float(
            regime_feats.get(
                "regime_entry_gate_factor", regime_feats.get("gate_factor", regime_mult)
            )
        )
    except Exception:
        degraded = True

    # capacity baseline
    cap_raw = 1.0
    try:
        cap_raw = _risk_capacity_from_components(
            winrate20=float(winrate),
            rolling_dd=float(dd_frac),
            realized_pnl_usd=float(realized),
            equity_usd=float(equity),
            regime_mult=float(regime_mult),
            regime_volatility=float(regime_vol),
            regime_choppy=float(regime_chop),
        )
    except Exception:
        degraded = True
        cap_raw = 1.0

    # plan gap adjustment
    if plan_gap_pct != 0.0:
        if plan_gap_pct > 0:
            cap_raw = _clamp(cap_raw - min(abs(plan_gap_pct) / 100.0 * 0.3, 0.3), 0.0, 1.0)
        else:
            cap_raw = _clamp(cap_raw + min(abs(plan_gap_pct) / 100.0 * 0.2, 0.2), 0.0, 1.0)

    # AI overlay
    ai_capacity_delta = 0.0
    if _predict_capacity_adjustment is not None and not RISK_DISABLE_AI_OVERLAY:
        with contextlib.suppress(Exception):
            feats = {
                "equity_usd": float(equity),
                "winrate20": float(winrate),
                "rolling_dd": float(dd_frac),
                "realized_pnl_usd": float(realized),
                "regime_mult": float(regime_mult),
                "plan_gap_pct": float(plan_gap_pct),
                "risk_profile": str(profile_name),
            }
            out = _predict_capacity_adjustment(feats)
            out = await _maybe_await(out)
            ai_capacity_delta = float(out or 0.0)

    cap_raw = _clamp(float(cap_raw) + float(ai_capacity_delta), 0.0, 1.0)

    # mode selection (override-aware)
    cap = float(cap_raw)
    risk_mode = 1
    override_mode = None
    halt_reason = None
    try:
        cap, risk_mode, override_mode, halt_reason = await _risk_mode_with_override(
            redis, cap_raw, rolling_dd=float(dd_frac)
        )
    except Exception:
        degraded = True
        cap, risk_mode, override_mode, halt_reason = float(cap_raw), 1, None, None

    # kelly
    kelly_raw = 0.0
    kelly_pos_cap = 0.0
    if RISK_KELLY_ENABLED:
        with contextlib.suppress(Exception):
            kelly_raw, kelly_pos_cap = _kelly_pos_cap(float(winrate))

    pnl_pct = 0.0 if float(equity) <= 0 else (float(realized) / float(equity)) * 100.0
    dd_pct = float(dd_frac) * 100.0

    # cooldown
    cooldown_active = False
    cooldown_until = 0.0
    if redis:
        with contextlib.suppress(Exception):
            v = await redis.get(RISK_COOLDOWN_UNTIL_KEY)
            if v is not None:
                cooldown_until = float(v)
                cooldown_active = cooldown_until > _now_ts()

    # trading window
    tw_open, tw_reason = _trading_window_status()

    # fast halt
    halted_fast = False
    halt_reason_fast = ""
    if redis:
        with contextlib.suppress(Exception):
            halted_fast, halt_reason_fast = await _is_halted_fast(redis)

    # hard-halt behavior
    if halted_fast:
        halt_reason = halt_reason or halt_reason_fast
        risk_mode = 0
        cap = 0.0

    # legacy fields (kept)
    halted = bool(risk_mode == 0 or halted_fast)

    # ctx digest
    ctx_digest = "no_redis"
    if redis:
        with contextlib.suppress(Exception):
            ctx_digest = await _read_ctx_digest(redis)

    return {
        "ts": now,
        "mode": "normal" if not degraded else "degraded",
        "profile": str(profile_name),
        "profile_name": str(profile_name),
        "risk_capacity": float(cap),
        "risk_mode": int(risk_mode),
        "risk_mode_override": override_mode,
        "winrate20": float(winrate),
        "rolling_dd_frac": float(dd_frac),
        "equity_usd": float(equity),
        "equity_hwm_usd": float(equity_hwm),
        "daily_realized_pnl_usd": float(realized),
        "daily_realized_pnl_pct": float(pnl_pct),
        "daily_drawdown_pct": float(dd_pct),
        "regime_label": str(regime),
        "regime_risk_multiplier": float(regime_mult),
        "regime_label_numeric": float(regime_label_numeric),
        "regime_stress_score": float(regime_stress_score),
        "regime_signal_strength": float(regime_signal_strength),
        "regime_entry_gate_factor": float(regime_entry_gate_factor),
        "consecutive_losses": int(consec_losses),
        "cooldown_active": bool(cooldown_active),
        "cooldown_until": float(cooldown_until),
        "plan_equity_today_usd": float(plan_equity_today),
        "plan_gap_pct": float(plan_gap_pct),
        "plan_status": plan_status,
        "ai_capacity_delta": float(ai_capacity_delta),
        "halt_reason": halt_reason,
        "daily_trades_count": int(trades_today),
        "daily_trades_cap": int(RISK_MAX_DAILY_TRADES),
        "daily_entries_count": int(entries_today),
        "daily_entries_cap": int(RISK_MAX_DAILY_ENTRIES),
        "sandbox_mode": bool(RISK_SANDBOX_MODE),
        "kelly_raw": float(kelly_raw),
        "kelly_pos_pct_cap": float(kelly_pos_cap),
        "equity_override_usd": float(RISK_EQUITY_OVERRIDE_USD),
        "risk_profile": str(profile_name),
        "risk_profile_env": RISK_PROFILE,
        "risk_profile_override": profile_override,
        "risk_engine_version": RISK_ENGINE_VERSION,
        "trading_window_open": bool(tw_open),
        "trading_window_reason": str(tw_reason),
        # --- legacy compatibility keys
        "halted": bool(halted),
        "degraded": bool(degraded),
        "reason": "halted" if halted else ("degraded" if degraded else "ok"),
        "ticket_usd": 0.0,
        "max_ticket_usd": 0.0,
        "halted_fast": bool(halted_fast),
        "halt_reason_fast": halt_reason_fast,
        "ctx_digest": ctx_digest,
    }


def _contract_no_io() -> bool:
    """
    True => absolutely NO network/redis IO (used for tests/contract runs/import safety).
    """
    v = os.getenv("NETWORK", "").strip().lower()
    if v in ("off", "0", "false", "no"):
        return True
    if os.getenv("SNIPER_TEST_CONTRACT", "").strip() == "1":
        return True
    if os.getenv("CONTRACT_TEST", "").strip() == "1":
        return True
    if os.getenv("PYTEST_MODE", "").strip().lower() in ("1", "true", "yes", "on"):
        return True
    if os.getenv("NO_NETWORK", "").strip().lower() in ("1", "true", "yes", "on"):
        return True
    if os.getenv("DISABLE_NETWORK", "").strip().lower() in ("1", "true", "yes", "on"):
        return True
    if os.getenv("DISABLE_REDIS", "").strip().lower() in ("1", "true", "yes", "on"):
        return True
    return os.getenv("REDIS_ENABLED", "").strip().lower() in ("0", "false", "off", "no")


async def risk_capacity_score(redis=None) -> float:
    snap = await risk_capacity_snapshot(redis)
    return float(snap.get("risk_capacity", 0.0))


async def risk_budget_summary(redis=None) -> dict[str, Any]:
    """
    Budget view:
      - daily_spend_cap_usd, spent, remaining, used_pct
    Safe in contract/no-IO mode.
    """
    if _contract_no_io():
        return {
            "equity_usd": 0.0,
            "daily_spend_cap_usd": 0.0,
            "daily_spent_usd": 0.0,
            "daily_spend_remaining_usd": 0.0,
            "daily_spend_used_pct": 0.0,
            "sandbox_mode": bool(RISK_SANDBOX_MODE),
            "trading_window_open": True,
            "ctx_digest": "contract_no_io",
        }

    if redis is None:
        try:
            redis = await _get_redis()
        except Exception:
            redis = None

    # best-effort limits
    limits: dict[str, Any] = {}
    with contextlib.suppress(Exception):
        limits = await current_limits(redis)

    cap = float(limits.get("daily_spend_usd", 0.0))
    spent = 0.0
    with contextlib.suppress(Exception):
        spent = float(await _daily_spent_usd(redis))

    remaining = max(0.0, cap - spent)
    used_pct = (spent / cap) * 100.0 if cap > 0 else 0.0
    tw_open, _ = _trading_window_status()

    ctx_digest = "no_redis"
    if redis:
        with contextlib.suppress(Exception):
            ctx_digest = await _read_ctx_digest(redis)

    return {
        "equity_usd": float(limits.get("equity_usd", 0.0)),
        "daily_spend_cap_usd": float(cap),
        "daily_spent_usd": float(spent),
        "daily_spend_remaining_usd": float(remaining),
        "daily_spend_used_pct": float(used_pct),
        "sandbox_mode": bool(RISK_SANDBOX_MODE),
        "trading_window_open": bool(tw_open),
        "ctx_digest": ctx_digest,
    }


async def risk_snapshot(redis=None) -> dict[str, Any]:
    """
    Unified risk snapshot for dashboards/ops. Safe in contract/no-IO mode.
    """
    if _contract_no_io():
        prof = (
            (os.getenv("RISK_PROFILE") or os.getenv("RISK_PROFILE_DEFAULT") or "balanced")
            .strip()
            .lower()
        )
        tw_open, tw_reason = _trading_window_status()
        return {
            "equity_usd": 0.0,
            "equity_hwm_usd": 0.0,
            "ticket_usd": 0.0,
            "pos_pct_cap": 0.0,
            "max_concurrent": 0,
            "daily_spend_usd": 0.0,
            "daily_spent_usd": 0.0,
            "daily_spend_remaining_usd": 0.0,
            "daily_spend_used_pct": 0.0,
            "daily_realized_pnl_usd": 0.0,
            "winrate20": 0.50,
            "rolling_dd_frac": 0.0,
            "regime_label": "contract_no_io",
            "regime_features": {},
            "regime_entry_gate_factor": 1.0,
            "risk_capacity": 1.0,
            "risk_mode": 1,
            "risk_mode_override": None,
            "risk_profile": prof,
            "risk_profile_env": RISK_PROFILE,
            "risk_profile_override": None,
            "ai_capacity_delta": 0.0,
            "plan_equity_today_usd": 0.0,
            "plan_gap_pct": 0.0,
            "plan_status": None,
            "manual_block": False,
            "cooldown_remaining_sec": 0.0,
            "halt_reason": "",
            "halted_fast": False,
            "halt_reason_fast": "",
            "daily_trades_cap": int(RISK_MAX_DAILY_TRADES),
            "daily_trades_count": 0,
            "daily_entries_cap": int(RISK_MAX_DAILY_ENTRIES),
            "daily_entries_count": 0,
            "sandbox_mode": bool(RISK_SANDBOX_MODE),
            "kelly_raw": 0.0,
            "kelly_pos_pct_cap": 0.0,
            "equity_override_usd": float(RISK_EQUITY_OVERRIDE_USD),
            "equity_utilization_pct": 0.0,
            "open_exposure_usd": 0.0,
            "max_exposure_usd": 0.0,
            "risk_engine_version": RISK_ENGINE_VERSION,
            "trading_window_open": bool(tw_open),
            "trading_window_reason": str(tw_reason),
            "ctx_digest": "contract_no_io",
        }

    if redis is None:
        try:
            redis = await _get_redis()
        except Exception:
            redis = None

    limits: dict[str, Any] = {}
    with contextlib.suppress(Exception):
        limits = await current_limits(redis)

    realized = 0.0
    with contextlib.suppress(Exception):
        realized = float(await _daily_realized_usd(redis))

    winrate20, dd_frac = 0.50, 0.0
    with contextlib.suppress(Exception):
        winrate20, dd_frac = await _metrics(redis)

    regime_feats: dict[str, Any] = {}
    with contextlib.suppress(Exception):
        regime_feats = await regime_state.features()

    equity_hwm = 0.0
    with contextlib.suppress(Exception):
        equity_hwm = float(await _equity_hwm(redis))

    try:
        strat_params = await get_strategy_params(redis)
    except Exception:
        strat_params = {}
    plan_equity_today = float(strat_params.get("PLAN_EQUITY_TODAY_USD", 0.0))
    plan_gap_pct = float(strat_params.get("PLAN_GAP_PCT", 0.0))
    plan_status = strat_params.get("PLAN_STATUS")

    profile_name = (
        (os.getenv("RISK_PROFILE") or os.getenv("RISK_PROFILE_DEFAULT") or "balanced")
        .strip()
        .lower()
    )
    with contextlib.suppress(Exception):
        pn, _ = await _effective_risk_profile(redis)
        if pn:
            profile_name = str(pn).strip().lower()

    profile_override: str | None = None
    if redis:
        with contextlib.suppress(Exception):
            v = await redis.get(RISK_PROFILE_OVERRIDE_KEY)
            if v:
                profile_override = _s(v).strip().lower()

    cooldown_remaining = 0.0
    manual_block = False
    spent_usd = 0.0
    trades_today = 0
    entries_today = 0
    halt_reason = limits.get("halt_reason")

    if redis:
        with contextlib.suppress(Exception):
            if halt_reason is None:
                saved = await redis.get(RISK_HALT_REASON_KEY)
                if saved:
                    halt_reason = saved
            manual_block = _s(await redis.get(RISK_MANUAL_BLOCK_KEY) or "").strip() == "1"
            v = await redis.get(RISK_COOLDOWN_UNTIL_KEY)
            if v is not None:
                ts = float(v)
                if ts > _now_ts():
                    cooldown_remaining = ts - _now_ts()
            spent_usd = float(await _daily_spent_usd(redis))
            trades_today = int(await _daily_trades(redis))
            entries_today = int(await _daily_entries(redis))

    cap_spend = float(limits.get("daily_spend_usd", 0.0))
    spend_remaining = max(0.0, cap_spend - float(spent_usd))
    spend_used_pct = ((float(spent_usd) / cap_spend) * 100.0) if cap_spend > 0 else 0.0

    regime_entry_gate_factor = float(
        regime_feats.get(
            "regime_entry_gate_factor",
            regime_feats.get("gate_factor", regime_feats.get("regime_risk_multiplier", 1.0)),
        )
    )

    tw_open, tw_reason = _trading_window_status()

    halted, halt_reason2 = False, ""
    if redis:
        with contextlib.suppress(Exception):
            halted, halt_reason2 = await _is_halted_fast(redis)

    ctx_digest = "no_redis"
    if redis:
        with contextlib.suppress(Exception):
            ctx_digest = await _read_ctx_digest(redis)

    return {
        "equity_usd": float(limits.get("equity_usd", 0.0)),
        "equity_hwm_usd": float(equity_hwm),
        "ticket_usd": float(limits.get("ticket_usd", 0.0)),
        "pos_pct_cap": float(limits.get("pos_pct_cap", 0.0)),
        "max_concurrent": int(limits.get("max_concurrent", 0)),
        "daily_spend_usd": float(limits.get("daily_spend_usd", 0.0)),
        "daily_spent_usd": float(spent_usd),
        "daily_spend_remaining_usd": float(spend_remaining),
        "daily_spend_used_pct": float(spend_used_pct),
        "daily_realized_pnl_usd": float(realized),
        "winrate20": float(winrate20),
        "rolling_dd_frac": float(dd_frac),
        "regime_label": str(limits.get("regime", "")),
        "regime_features": regime_feats,
        "regime_entry_gate_factor": float(regime_entry_gate_factor),
        "risk_capacity": float(limits.get("risk_capacity", 1.0)),
        "risk_mode": int(limits.get("risk_mode", 1)),
        "risk_mode_override": limits.get("risk_mode_override"),
        "risk_profile": profile_name,
        "risk_profile_env": RISK_PROFILE,
        "risk_profile_override": profile_override,
        "ai_capacity_delta": float(limits.get("ai_capacity_delta", 0.0)),
        "plan_equity_today_usd": float(plan_equity_today),
        "plan_gap_pct": float(plan_gap_pct),
        "plan_status": plan_status,
        "manual_block": bool(manual_block),
        "cooldown_remaining_sec": float(cooldown_remaining),
        "halt_reason": _s(halt_reason),
        "halted_fast": bool(halted),
        "halt_reason_fast": halt_reason2,
        "daily_trades_cap": int(RISK_MAX_DAILY_TRADES),
        "daily_trades_count": int(trades_today),
        "daily_entries_cap": int(RISK_MAX_DAILY_ENTRIES),
        "daily_entries_count": int(entries_today),
        "sandbox_mode": bool(RISK_SANDBOX_MODE),
        "kelly_raw": float(limits.get("kelly_raw", 0.0)),
        "kelly_pos_pct_cap": float(limits.get("kelly_pos_pct_cap", 0.0)),
        "equity_override_usd": float(RISK_EQUITY_OVERRIDE_USD),
        "equity_utilization_pct": float(limits.get("equity_utilization_pct", 0.0)),
        "open_exposure_usd": float(limits.get("open_exposure_usd", 0.0)),
        "max_exposure_usd": float(limits.get("max_exposure_usd", 0.0)),
        "risk_engine_version": RISK_ENGINE_VERSION,
        "trading_window_open": bool(tw_open),
        "trading_window_reason": str(tw_reason),
        "ctx_digest": ctx_digest,
    }


async def risk_health(redis=None) -> dict[str, Any]:
    """
    High-level health envelope for ops.
    Safe in contract/no-IO mode.
    """
    if _contract_no_io():
        return {
            "ok": True,
            "ts": datetime.now(UTC).isoformat(),
            "snapshot": await risk_snapshot(redis=None),
            "entry_gate": {"allowed": True, "reason": "contract_no_io"},
            "capacity": await risk_capacity_snapshot(redis=None),
        }

    if redis is None:
        try:
            redis = await _get_redis()
        except Exception:
            redis = None

    snapshot = await risk_snapshot(redis)
    gate: dict[str, Any] = {"allowed": True}
    with contextlib.suppress(Exception):
        gate = await entry_gate_debug(redis=redis)
    capacity = await risk_capacity_snapshot(redis)
    ok = bool(gate.get("allowed", True))
    return {
        "ok": ok,
        "ts": datetime.now(UTC).isoformat(),
        "snapshot": snapshot,
        "entry_gate": gate,
        "capacity": capacity,
    }


async def reinvestor_risk_view(redis=None) -> dict[str, Any]:
    """
    Lightweight reinvestor-focused view:
      - budget_utilization_pct (0..100)
      - scale_hint (0..~2) recommended multiplicative aggressiveness
    Safe in contract/no-IO mode.
    """
    if _contract_no_io():
        return {
            "equity_usd": 0.0,
            "ticket_usd": 0.0,
            "risk_capacity": 1.0,
            "risk_mode": 1,
            "sandbox_mode": bool(RISK_SANDBOX_MODE),
            "daily_spend_cap_usd": 0.0,
            "daily_spent_usd": 0.0,
            "daily_spend_remaining_usd": 0.0,
            "budget_utilization_pct": 0.0,
            "trading_window_open": True,
            "scale_hint": 1.0,
            "ctx_digest": "contract_no_io",
            "risk_engine_version": RISK_ENGINE_VERSION,
        }

    if redis is None:
        try:
            redis = await _get_redis()
        except Exception:
            redis = None

    limits: dict[str, Any] = {}
    with contextlib.suppress(Exception):
        limits = await current_limits(redis)

    budget = await risk_budget_summary(redis)
    cap_snap = await risk_capacity_snapshot(redis)

    equity_usd = float(limits.get("equity_usd", 0.0))
    ticket_usd = float(limits.get("ticket_usd", 0.0))

    risk_capacity = float(cap_snap.get("risk_capacity", limits.get("risk_capacity", 1.0)))
    risk_mode = int(cap_snap.get("risk_mode", limits.get("risk_mode", 1)))

    daily_cap = float(budget.get("daily_spend_cap_usd", limits.get("daily_spend_usd", 0.0)))
    daily_spent = float(budget.get("daily_spent_usd", 0.0))
    daily_remaining = float(budget.get("daily_spend_remaining_usd", 0.0))
    tw_open = bool(budget.get("trading_window_open", cap_snap.get("trading_window_open", True)))

    budget_utilization_pct = ((daily_spent / daily_cap) * 100.0) if daily_cap > 0 else 0.0

    scale_hint = float(risk_capacity)
    if not tw_open or risk_mode == 0:
        scale_hint = 0.0
    elif risk_mode == 3:
        scale_hint = min(scale_hint, 0.75)
    elif risk_mode == 2:
        scale_hint = min(max(scale_hint * 1.5, 0.8), 2.0)
    else:
        scale_hint = min(max(scale_hint * 1.1, 0.5), 1.25)

    ctx_digest = "no_redis"
    if redis:
        with contextlib.suppress(Exception):
            ctx_digest = await _read_ctx_digest(redis)

    return {
        "equity_usd": float(equity_usd),
        "ticket_usd": float(ticket_usd),
        "risk_capacity": float(risk_capacity),
        "risk_mode": int(risk_mode),
        "sandbox_mode": bool(RISK_SANDBOX_MODE),
        "daily_spend_cap_usd": float(daily_cap),
        "daily_spent_usd": float(daily_spent),
        "daily_spend_remaining_usd": float(daily_remaining),
        "budget_utilization_pct": float(budget_utilization_pct),
        "trading_window_open": bool(tw_open),
        "scale_hint": float(scale_hint),
        "ctx_digest": ctx_digest,
        "risk_engine_version": RISK_ENGINE_VERSION,
    }


# =============================================================================
# Loops
# =============================================================================
async def monitor_positions_loop(
    *, redis=None, stop_evt: asyncio.Event | None = None, interval_sec: int | None = None
) -> None:
    """
    Kept for backwards compatibility; exits should be driven by ai_sell_logic/exit_engine.
    Safe in contract/no-IO mode.
    """
    if interval_sec is None:
        interval_sec = POSITION_CHECK_INTERVAL

    if _contract_no_io():
        while not (stop_evt and stop_evt.is_set()):
            await asyncio.sleep(max(1, int(interval_sec)))
        return

    if redis is None:
        with contextlib.suppress(Exception):
            redis = await _get_redis()

    while not (stop_evt and stop_evt.is_set()):
        await asyncio.sleep(max(1, int(interval_sec)))


async def risk_manager_loop(stop_evt: asyncio.Event) -> None:
    """Periodically refresh current_limits() so risk:ctx stays hot for scanners/AI."""
    if _contract_no_io():
        while not stop_evt.is_set():
            await asyncio.sleep(max(1, int(RISK_MANAGER_LOOP_INTERVAL)))
        return

    redis = await _get_redis()
    interval = max(1, int(RISK_MANAGER_LOOP_INTERVAL))

    while not stop_evt.is_set():
        with contextlib.suppress(Exception):
            await current_limits(redis)
        await asyncio.sleep(interval)


# =============================================================================
# Ops helpers
# =============================================================================
async def set_manual_block(on: bool = True, redis=None, reason: str = "manual_block") -> None:
    """Operator manual block (distinct from risk:halt, but blocks entries immediately)."""
    if _contract_no_io():
        return
    if redis is None:
        redis = await _get_redis()
    if not redis:
        return
    with contextlib.suppress(Exception):
        if on:
            await redis.set(RISK_MANUAL_BLOCK_KEY, "1")
            await redis.set(RISK_HALT_REASON_KEY, str(reason)[:300])
        else:
            await redis.delete(RISK_MANUAL_BLOCK_KEY)
        await log_event("risk_manual_block", {"on": bool(on), "reason": reason})
        invalidate_limits_cache()


async def clear_manual_block(redis=None) -> None:
    await set_manual_block(on=False, redis=redis, reason="clear_manual_block")


async def clear_risk_cooldown(redis=None) -> None:
    if _contract_no_io():
        return
    if redis is None:
        redis = await _get_redis()
    if not redis:
        return
    with contextlib.suppress(Exception):
        await redis.delete(RISK_COOLDOWN_UNTIL_KEY)
        await log_event("risk_cooldown_cleared", {})
        invalidate_limits_cache()


async def force_risk_mode(mode: int | None, redis=None) -> None:
    if _contract_no_io():
        return
    if redis is None:
        redis = await _get_redis()
    if not redis:
        return
    with contextlib.suppress(Exception):
        if mode is None:
            await redis.delete(OVERRIDE_MODE_KEY)
        else:
            await redis.set(OVERRIDE_MODE_KEY, str(int(mode)))
        await log_event("risk_mode_override", {"mode": mode})
        invalidate_limits_cache()


async def set_risk_profile_override(profile: str | None, redis=None) -> None:
    if _contract_no_io():
        return
    if redis is None:
        redis = await _get_redis()
    if not redis:
        return
    with contextlib.suppress(Exception):
        if profile is None:
            await redis.delete(RISK_PROFILE_OVERRIDE_KEY)
        else:
            await redis.set(RISK_PROFILE_OVERRIDE_KEY, str(profile).strip().lower())
        await log_event("risk_profile_override", {"profile": profile})
        invalidate_limits_cache()


# =============================================================================
# CLI
# =============================================================================
if __name__ == "__main__":

    async def _main() -> None:
        r = None
        with contextlib.suppress(Exception):
            r = await _get_redis()
        health = await risk_health(r)
        print(json.dumps(health, indent=2, default=str))

    asyncio.run(_main())
