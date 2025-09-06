# execution/exits.py — v2.1 (exit engine + logging)
from __future__ import annotations

import os
import time
import asyncio
from dataclasses import dataclass, field
from typing import Optional, Tuple, Dict, Any, Callable

# ---- DB logging (compatible with your new logger) -----------------------------
try:
    from db.logger import log_trade_event, record_legacy_trade
except Exception:
    # Fallbacks if logger import fails (do nothing but don't crash)
    async def log_trade_event(*_a, **_k): ...
    async def record_legacy_trade(*_a, **_k): ...

# ------------------------------------------------------------------------------
# Config helpers
# ------------------------------------------------------------------------------

def _env_float(name: str, default: float) -> float:
    try:
        v = os.getenv(name)
        return float(v) if v is not None and v != "" else float(default)
    except Exception:
        return float(default)

def _normalize_stop_pct(v: float) -> float:
    """
    We accept either -30 (old style) or 30 (new style) for a 30% hard stop.
    Always return a negative number for comparison against change %.
    """
    if v > 0:
        return -abs(v)
    return v

# ----------------------- Tunables (env-driven) ---------------------------------
# Hard stop (drop from entry)
HARD_STOP_PCT = _normalize_stop_pct(
    _env_float("EXIT_HARD_STOP_PCT", _env_float("HARD_STOP_PCT", -30.0))
)

# Take profit 1
TP1_AT_X       = _env_float("TP1_AT_X", 2.0)        # e.g. 2.0 = 2x entry
TP1_TAKE_PCT   = _env_float("TP1_TAKE_PCT", 40.0)   # percent of position

# Optional Take profit 2
TP2_AT_X       = _env_float("TP2_AT_X", 3.0)        # 3x by default
TP2_TAKE_PCT   = _env_float("TP2_TAKE_PCT", 30.0)   # 30% by default (set 0 to disable)

# Trailing exit (drawdown from *peak* once armed)
TRAIL_FROM_PEAK_PCT = _env_float("TRAIL_FROM_PEAK_PCT", 20.0)  # exit if -20% off peak
TRAIL_ARM_AFTER_X   = _env_float("TRAIL_ARM_AFTER_X", 2.0)     # only trail after >=2x or TP1 hit

# Optional break-even (or small loss) exit once position is green
BREAKEVEN_ARM_AFTER_X = _env_float("BREAKEVEN_ARM_AFTER_X", 1.4)  # arm BE after >=1.4x
BREAKEVEN_STOP_PCT    = _normalize_stop_pct(_env_float("BREAKEVEN_STOP_PCT", -2.0))
# ^ if set to 0.0, this becomes "never let it go red after armed"

# Timing knobs
MIN_HOLD_SEC   = int(_env_float("EXIT_MIN_HOLD_SEC", 30.0))   # minimal time in position before any exit
COOL_OFF_SEC   = int(_env_float("EXIT_COOL_OFF_SEC", 15.0))   # minimal gap between consecutive sells

# ------------------------------------------------------------------------------
# Position & decisions
# ------------------------------------------------------------------------------

@dataclass
class PositionState:
    mint: str
    entry_px: float            # in quote (SOL/token or USD/token, same unit as last_px)
    size_usd: float            # *remaining* capital allocated in USD terms
    symbol: Optional[str] = None
    entry_ts: float = field(default_factory=lambda: time.time())
    peak_px: float = None
    tp1_done: bool = False
    tp2_done: bool = False
    last_action_ts: float = field(default_factory=lambda: 0.0)

    def __post_init__(self):
        if self.peak_px is None:
            self.peak_px = float(self.entry_px)

@dataclass
class ExitDecision:
    action: str                    # 'HOLD' | 'SELL_PARTIAL' | 'SELL_ALL'
    sell_pct: float = 0.0          # percent of *remaining* position to sell (for partials)
    reason: str = ""
    take_usd: float = 0.0          # computed $ chunk to sell (for convenience)
    pnl_usd: float = 0.0           # estimated pnl for the sold chunk

# ------------------------------------------------------------------------------
# Core math helpers
# ------------------------------------------------------------------------------

def _pct_change(curr: float, ref: float) -> float:
    if ref <= 0:
        return 0.0
    return (curr / ref - 1.0) * 100.0

def _dd_from_peak_pct(curr: float, peak: float) -> float:
    if peak <= 0:
        return 0.0
    return (curr / peak - 1.0) * 100.0   # negative when below peak

# ------------------------------------------------------------------------------
# Pure decision engine (no side effects)
# ------------------------------------------------------------------------------

def decide_exit(pos: PositionState, last_px: float, now_ts: Optional[float] = None) -> ExitDecision:
    """
    Returns an ExitDecision. Does not mutate pos, does not log.
    """
    now = now_ts or time.time()
    last_px = float(last_px)

    # time guards
    if now - pos.entry_ts < MIN_HOLD_SEC:
        return ExitDecision(action="HOLD", reason="min_hold")

    if pos.last_action_ts and (now - pos.last_action_ts) < COOL_OFF_SEC:
        return ExitDecision(action="HOLD", reason="cooloff")

    # update "virtual" peak for decision purposes
    peak_px = max(pos.peak_px, last_px)
    chg_pct = _pct_change(last_px, pos.entry_px)
    dd_pct  = _dd_from_peak_pct(last_px, peak_px)

    # 1) Hard stop
    if chg_pct <= HARD_STOP_PCT:
        return ExitDecision(action="SELL_ALL", reason="hard_stop")

    # 2) TP1
    if (not pos.tp1_done) and (last_px >= pos.entry_px * TP1_AT_X):
        sell_pct = max(0.0, min(100.0, TP1_TAKE_PCT))
        take_usd = pos.size_usd * (sell_pct / 100.0)
        pnl_usd  = (last_px / pos.entry_px - 1.0) * take_usd
        return ExitDecision(
            action="SELL_PARTIAL",
            sell_pct=sell_pct,
            reason="tp1",
            take_usd=take_usd,
            pnl_usd=pnl_usd,
        )

    # 3) TP2 (optional)
    if (TP2_TAKE_PCT > 0) and (not pos.tp2_done) and (last_px >= pos.entry_px * TP2_AT_X):
        sell_pct = max(0.0, min(100.0, TP2_TAKE_PCT))
        take_usd = pos.size_usd * (sell_pct / 100.0)
        pnl_usd  = (last_px / pos.entry_px - 1.0) * take_usd
        return ExitDecision(
            action="SELL_PARTIAL",
            sell_pct=sell_pct,
            reason="tp2",
            take_usd=take_usd,
            pnl_usd=pnl_usd,
        )

    # When is trailing/breakeven armed?
    armed_by_tp  = pos.tp1_done or pos.tp2_done
    armed_by_x   = (last_px >= pos.entry_px * TRAIL_ARM_AFTER_X)
    armed_by_be  = (last_px >= pos.entry_px * BREAKEVEN_ARM_AFTER_X)

    # 4) Trailing from peak (only when armed)
    if (armed_by_tp or armed_by_x) and dd_pct <= -abs(TRAIL_FROM_PEAK_PCT):
        return ExitDecision(action="SELL_ALL", reason="trailing_peak")

    # 5) Breakeven style stop once armed (optional)
    if BREAKEVEN_STOP_PCT < 0 and armed_by_be and chg_pct <= BREAKEVEN_STOP_PCT:
        return ExitDecision(action="SELL_ALL", reason="breakeven_stop")

    return ExitDecision(action="HOLD", reason="none")

# ------------------------------------------------------------------------------
# Side-effectful executor
# ------------------------------------------------------------------------------

# Signature that your caller should provide to actually sell via Jupiter etc.
# It receives USD chunk to sell (based on your remaining position) and should
# handle the mint/amount conversion internally.
SellFn = Callable[[str, float, str], "asyncio.Future[Tuple[bool, Optional[str], Dict[str, Any]]]"]
# -> returns (success, signature, meta)

async def apply_exit(
    pos: PositionState,
    last_px: float,
    sell_fn: SellFn,
    now_ts: Optional[float] = None,
) -> ExitDecision:
    """
    Compute a decision, execute it with `sell_fn`, update `pos`, and log it.
    - `sell_fn(mint, usd_to_sell, reason)` must perform the swap and return (ok, sig, meta).
    - Logs both the structured trade_event and a legacy row for quick PnL diffs.
    """
    now = now_ts or time.time()
    dec = decide_exit(pos, last_px, now)

    if dec.action == "HOLD":
        return dec

    # Compute take size for partials / full
    usd_to_sell = dec.take_usd
    sell_pct = dec.sell_pct

    if dec.action == "SELL_ALL":
        usd_to_sell = pos.size_usd
        sell_pct = 100.0

    if usd_to_sell <= 0.0:
        # nothing to do (may happen if size_usd is already 0 due to prior sells)
        return ExitDecision(action="HOLD", reason="empty_position")

    # Execute the sale
    ok, signature, meta = await sell_fn(pos.mint, float(usd_to_sell), dec.reason)

    # Estimate PnL for the chunk (for logs)
    pnl_usd = (last_px / pos.entry_px - 1.0) * usd_to_sell

    # Structured event log
    try:
        await log_trade_event(
            type="sell" if ok else "sell_fail",
            mint=pos.mint,
            symbol=pos.symbol,
            entry_sol_per_token=pos.entry_px,  # unit: same as last_px input
            sol_spent=0.0,                      # not meaningful on sell path
            usd_spent=usd_to_sell * -1.0,      # negative means we *reduced* risk
            signature=signature,
            cu_price=meta.get("cu_price") if isinstance(meta, dict) else None,
            jito_tip=meta.get("jito_tip") if isinstance(meta, dict) else None,
            data={
                "reason": dec.reason,
                "sell_pct": sell_pct,
                "last_px": last_px,
                "pnl_usd_est": pnl_usd,
            },
        )
    except Exception:
        pass

    # Legacy row for quick accounting views
    try:
        await record_legacy_trade(
            mint=pos.mint,
            entry_px=pos.entry_px,
            exit_px=last_px,
            size_usd=usd_to_sell,
            pnl_usd=pnl_usd,
            reason=dec.reason if ok else f"{dec.reason}_fail",
        )
    except Exception:
        pass

    # Update position state on success
    if ok:
        pos.size_usd = max(0.0, pos.size_usd - usd_to_sell)
        pos.last_action_ts = now
        # update peak after execution window
        pos.peak_px = max(pos.peak_px, last_px)
        if dec.reason == "tp1":
            pos.tp1_done = True
        if dec.reason == "tp2":
            pos.tp2_done = True

    # If SELL_ALL succeeded (or size fully sold), caller can drop the position.
    return ExitDecision(
        action="HOLD" if not ok else ("HOLD" if pos.size_usd > 0 else "SELL_ALL"),
        sell_pct=sell_pct,
        reason=dec.reason if ok else f"{dec.reason}_fail",
        take_usd=usd_to_sell,
        pnl_usd=pnl_usd,
    )

# ------------------------------------------------------------------------------
# Convenience: single-tick helper (sync decision only)
# ------------------------------------------------------------------------------

def on_tick(pos: PositionState, last_px: float) -> str:
    """
    Backward-compatible helper: returns "HOLD" / "EXIT_ALL".
    (No partial returns here—use decide_exit/apply_exit in live code.)
    """
    d = decide_exit(pos, last_px)
    if d.action == "SELL_ALL":
        return "EXIT_ALL"
    return "HOLD"

# ------------------------------------------------------------------------------
# Pretty-print config (optional)
# ------------------------------------------------------------------------------

def exits_config_summary() -> Dict[str, Any]:
    return {
        "HARD_STOP_PCT": HARD_STOP_PCT,
        "TP1_AT_X": TP1_AT_X,
        "TP1_TAKE_PCT": TP1_TAKE_PCT,
        "TP2_AT_X": TP2_AT_X,
        "TP2_TAKE_PCT": TP2_TAKE_PCT,
        "TRAIL_FROM_PEAK_PCT": TRAIL_FROM_PEAK_PCT,
        "TRAIL_ARM_AFTER_X": TRAIL_ARM_AFTER_X,
        "BREAKEVEN_ARM_AFTER_X": BREAKEVEN_ARM_AFTER_X,
        "BREAKEVEN_STOP_PCT": BREAKEVEN_STOP_PCT,
        "MIN_HOLD_SEC": MIN_HOLD_SEC,
        "COOL_OFF_SEC": COOL_OFF_SEC,
    }
