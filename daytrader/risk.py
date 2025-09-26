# daytrader/risk.py
from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Tuple, Optional


# -------- env helpers ---------------------------------------------------------
def _get_float(name: str, default: float) -> float:
    try:
        v = os.getenv(name)
        return float(v) if v not in (None, "", "None") else float(default)
    except Exception:
        return float(default)


def _get_int(name: str, default: int) -> int:
    try:
        v = os.getenv(name)
        return int(v) if v not in (None, "", "None") else int(default)
    except Exception:
        return int(default)


def _get_bool(name: str, default: bool) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return str(v).strip().lower() in ("1", "true", "yes", "y", "on")


# -------- tunables (wire them in .env later) ---------------------------------
# Core per-trade risk
RISK_PCT: float = _get_float("DT_RISK_PCT", 0.005)           # risk 0.5% of equity per trade
MIN_TRADE_SOL: float = _get_float("DT_MIN_TRADE_SOL", 0.02)  # skip trades smaller than this (SOL terms)
SL_ATR_MULT: float = _get_float("DT_SL_ATR_MULT", 1.5)       # stop = entry - k * ATR(absolute)
TP_ATR_MULT: float = _get_float("DT_TP_ATR_MULT", 3.0)       # take-profit = entry + m * ATR(absolute)

# Dynamic trailing
TRAIL_ATR_MULT: float = _get_float("DT_TRAIL_ATR_MULT", 2.0) # trail below highest by k * ATR abs

# Session/portfolio guards
MAX_CONCURRENT_POS: int = _get_int("DT_MAX_CONCURRENT_POS", 4)
DAILY_MAX_DD_PCT: float = _get_float("DT_DAILY_MAX_DD_PCT", 0.03)  # stop trading for the day after -3%
ENABLE_CIRCUIT_BREAKER: bool = _get_bool("DT_ENABLE_CIRCUIT_BREAKER", True)

# Optional overall leverage cap (not used for orders; informational guard)
MAX_GROSS_EXPOSURE_PCT: float = _get_float("DT_MAX_GROSS_EXPOSURE_PCT", 1.25)  # 125% of equity


# -------- data models ---------------------------------------------------------
@dataclass
class OpenContext:
    """
    Inputs the trading loop can provide to decide if we are allowed to open a new position.
    """
    equity_sol: float
    equity_peak_sol: float
    gross_exposure_sol: float = 0.0
    open_positions: int = 0
    day_realized_pnl_sol: float = 0.0


@dataclass
class RiskDecision:
    ok: bool
    reason: str = ""


# -------- helpers kept for backward compatibility -----------------------------
def atr_to_price(atr_pct: float, price_sol: float) -> float:
    """
    Convert ATR expressed as a fraction of price (e.g. 0.015) to absolute price (in SOL).
    """
    return max(1e-12, atr_pct * price_sol)


def size_from_risk(balance_sol: float, entry_price_sol: float, atr_price: float) -> float:
    """
    Dollar-volatility position sizing: risk fixed % of equity per trade.
    risk (SOL)      = RISK_PCT * balance_sol
    stop distance   = SL_ATR_MULT * ATR_price
    qty_token       = risk / stop_distance
    trade in SOL    = qty_token * entry_price_sol

    Returns the SOL notional you should deploy on this trade (buy size if SOL->TOKEN; for sells
    you typically size by current token exposure elsewhere).
    """
    if atr_price <= 0 or entry_price_sol <= 0 or balance_sol <= 0:
        return 0.0
    risk_sol = max(0.0, balance_sol * RISK_PCT)
    stop_dist = SL_ATR_MULT * atr_price
    if stop_dist <= 0:
        return 0.0
    qty_token = risk_sol / stop_dist
    trade_sol = qty_token * entry_price_sol
    return max(0.0, trade_sol)


def stops_from_atr(entry_price_sol: float, atr_price: float) -> Tuple[float, float]:
    """
    Compute static stop-loss and take-profit from ATR.
    """
    stop = max(1e-12, entry_price_sol - SL_ATR_MULT * atr_price)
    tp   = max(1e-12, entry_price_sol + TP_ATR_MULT * atr_price)
    return stop, tp


# -------- extended API for the daytrader engine -------------------------------
def compute_trade_size(balance_sol: float,
                       entry_price_sol: float,
                       atr_pct: float) -> float:
    """
    Convenience sizing using ATR% directly.
    Applies MIN_TRADE_SOL floor.
    """
    atr_abs = atr_to_price(atr_pct, entry_price_sol)
    size = size_from_risk(balance_sol, entry_price_sol, atr_abs)
    if size < MIN_TRADE_SOL:
        return 0.0
    return size


def trailing_stop_from_high(highest_price_sol: float,
                            atr_price: float) -> float:
    """
    A volatility-aware trailing stop that hugs price by k*ATR.
    """
    if highest_price_sol <= 0:
        return 0.0
    return max(1e-12, highest_price_sol - TRAIL_ATR_MULT * max(1e-12, atr_price))


def update_trailing_stop(current_trail_stop: float,
                         highest_price_sol: float,
                         atr_price: float) -> float:
    """
    Only moves the stop upward (never down). Returns the updated trailing stop price.
    """
    new_trail = trailing_stop_from_high(highest_price_sol, atr_price)
    if current_trail_stop <= 0:
        return new_trail
    return max(current_trail_stop, new_trail)


def can_open_position(ctx: OpenContext) -> RiskDecision:
    """
    Portfolio-level gate before opening a new trade.
    - Enforces daily max drawdown (relative to equity peak OR via realized PnL).
    - Caps number of concurrent positions.
    - Caps gross exposure (optional).
    """
    # Daily drawdown guard (either realized pnl or equity vs peak)
    if ENABLE_CIRCUIT_BREAKER:
        if ctx.equity_peak_sol > 0:
            dd_frac = max(0.0, (ctx.equity_peak_sol - ctx.equity_sol) / ctx.equity_peak_sol)
            if dd_frac >= DAILY_MAX_DD_PCT:
                return RiskDecision(False, f"daily drawdown {dd_frac:.2%} >= {DAILY_MAX_DD_PCT:.2%}")
        # Also check realized PnL based guard (optional redundancy)
        if ctx.day_realized_pnl_sol < 0:
            realized_dd = abs(ctx.day_realized_pnl_sol) / max(1e-12, ctx.equity_peak_sol or ctx.equity_sol)
            if realized_dd >= DAILY_MAX_DD_PCT:
                return RiskDecision(False, f"realized daily loss {realized_dd:.2%} >= {DAILY_MAX_DD_PCT:.2%}")

    # Max concurrent positions
    if ctx.open_positions >= MAX_CONCURRENT_POS:
        return RiskDecision(False, f"max positions {MAX_CONCURRENT_POS} reached")

    # Gross exposure cap (informational soft guard; block if exceeded)
    max_exposure = MAX_GROSS_EXPOSURE_PCT * max(0.0, ctx.equity_sol)
    if max_exposure > 0 and ctx.gross_exposure_sol > max_exposure:
        return RiskDecision(False, f"gross exposure {ctx.gross_exposure_sol:.4f} > cap {max_exposure:.4f}")

    return RiskDecision(True, "ok")


def should_move_to_break_even(entry_price_sol: float,
                              last_price_sol: float,
                              atr_price: float,
                              be_atr_mult: float = 1.0) -> bool:
    """
    Optional tactic: once price moves +k*ATR in your favor, raise stop to breakeven.
    """
    if entry_price_sol <= 0 or atr_price <= 0:
        return False
    return (last_price_sol - entry_price_sol) >= be_atr_mult * atr_price


def normalize_stop(entry_price_sol: float,
                   raw_stop: float,
                   min_tick: Optional[float] = None) -> float:
    """
    Clamp and round a stop to practical tick size if needed.
    """
    stop = max(1e-12, min(entry_price_sol, raw_stop))
    if min_tick and min_tick > 0:
        # Round down to the nearest tick
        steps = int(stop / min_tick)
        stop = max(1e-12, steps * min_tick)
    return stop


def risk_summary() -> str:
    """
    Human-friendly summary for logs/healthcheck.
    """
    return (
        f"[risk] RISK_PCT={RISK_PCT:.3%} MIN_TRADE_SOL={MIN_TRADE_SOL:.5f} "
        f"SLxATR={SL_ATR_MULT:.2f} TPxATR={TP_ATR_MULT:.2f} TRAILxATR={TRAIL_ATR_MULT:.2f} "
        f"MAX_POS={MAX_CONCURRENT_POS} DAILY_MAX_DD={DAILY_MAX_DD_PCT:.2%} "
        f"CIRCUIT={'on' if ENABLE_CIRCUIT_BREAKER else 'off'} "
        f"GROSS_CAP={MAX_GROSS_EXPOSURE_PCT:.0%}"
    )


__all__ = [
    # Tunables
    "RISK_PCT", "MIN_TRADE_SOL", "SL_ATR_MULT", "TP_ATR_MULT",
    "TRAIL_ATR_MULT", "MAX_CONCURRENT_POS", "DAILY_MAX_DD_PCT",
    "ENABLE_CIRCUIT_BREAKER", "MAX_GROSS_EXPOSURE_PCT",
    # Models
    "OpenContext", "RiskDecision",
    # Legacy API (backward compatible)
    "atr_to_price", "size_from_risk", "stops_from_atr",
    # Extended API
    "compute_trade_size", "trailing_stop_from_high", "update_trailing_stop",
    "can_open_position", "should_move_to_break_even", "normalize_stop",
    "risk_summary",
]
