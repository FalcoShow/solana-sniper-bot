# daytrader/risk.py
from __future__ import annotations

"""
daytrader.risk
--------------
Risk sizing + portfolio gates for the Daytrader stack (NOT the sniper stack).

Design goals (God-Tier Overhaul / “no competing engines”)
- Daytrader-only: MUST NOT silently compete with canonical sniper risk logic.
- Deterministic, env-driven, production-safe.
- Never raise to caller (best-effort persistence).
- Fail-closed for NEW opens when critical inputs are missing, but allow SELL/risk-off.

Provides
- Portfolio gate: can we open a NEW position now? (max DD, max positions, exposure cap)
- Volatility-aware sizing: fixed % risk per trade using stop distance (ATR-based).
- Trailing stop helpers + optional break-even move.
- RiskManager.refine_intent(...) to refine TradeIntent from daytrader/features.py.

Units
- Quote-currency agnostic. Env names may still say *_SOL for legacy, but treat as QUOTE.

Env (recommended)
- DT_RISK_PCT                float  default 0.005    (0.5% of equity risk per trade)
- DT_MIN_TRADE_QUOTE         float  default ""       (overrides DT_MIN_TRADE_SOL if set)
- DT_MIN_TRADE_SOL           float  default 0.02     (legacy name; quote units)
- DT_MAX_TRADE_QUOTE         float  default 0        (0 = no cap)
- DT_SL_ATR_MULT             float  default 1.5
- DT_TP_ATR_MULT             float  default 3.0
- DT_TRAIL_ATR_MULT          float  default 2.0
- DT_MAX_CONCURRENT_POS      int    default 4
- DT_DAILY_MAX_DD_PCT        float  default 0.03     (stop opens after -3% from peak)
- DT_ENABLE_CIRCUIT_BREAKER  bool   default true
- DT_MAX_GROSS_EXPOSURE_PCT  float  default 1.25     (125% of equity)
- DT_MAX_ATR_PCT             float  default 0.30     (hard block opens if ATR% too high)
- DT_BE_ATR_MULT             float  default 1.0      (move stop to breakeven after +k*ATR)
- DT_SIZE_SAFETY_FRACTION    float  default 0.98     (leave dust for fees)

Optional (state)
- DT_STATE_DIR               path   default ./state
- DT_RISK_STATE_FILE         name   default daytrader_risk_state.json
"""

import json
import math
import os
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Final

# =============================================================================
# Env helpers (safe, bounded)
# =============================================================================


def _is_finite(x: float) -> bool:
    return math.isfinite(x)


def _safe_float(x: Any, default: float = 0.0) -> float:
    try:
        val = float(x)
        return float(val) if _is_finite(val) else float(default)
    except Exception:
        return float(default)


def _safe_int(x: Any, default: int = 0) -> int:
    try:
        return int(x)
    except Exception:
        return int(default)


def _env_str(name: str, default: str = "") -> str:
    raw = os.getenv(name)
    return default if raw is None else str(raw).strip()


def _env_float(
    name: str,
    default: float,
    *,
    min_value: float | None = None,
    max_value: float | None = None,
) -> float:
    raw = _env_str(name, "")
    val = _safe_float(raw, default) if raw else float(default)
    if min_value is not None:
        val = max(float(min_value), val)
    if max_value is not None:
        val = min(float(max_value), val)
    return float(val)


def _env_int(
    name: str,
    default: int,
    *,
    min_value: int | None = None,
    max_value: int | None = None,
) -> int:
    raw = _env_str(name, "")
    val = _safe_int(raw, default) if raw else int(default)
    if min_value is not None:
        val = max(int(min_value), val)
    if max_value is not None:
        val = min(int(max_value), val)
    return int(val)


def _env_bool(name: str, default: bool) -> bool:
    raw = _env_str(name, "")
    if raw == "":
        return bool(default)
    s = raw.lower()
    if s in ("1", "true", "yes", "y", "on", "t"):
        return True
    if s in ("0", "false", "no", "n", "off", "f"):
        return False
    return bool(default)


def _clamp(x: float, lo: float, hi: float) -> float:
    if x < lo:
        return lo
    if x > hi:
        return hi
    return x


def _now_iso() -> str:
    return datetime.now(UTC).isoformat()


def _today_key_utc() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%d")


def _json_dumps(obj: Any) -> str:
    # Deterministic JSON (stable diffs)
    return json.dumps(obj, indent=2, sort_keys=True)


# =============================================================================
# Tunables (module-level, deterministic)
# =============================================================================

RISK_PCT: Final[float] = _clamp(
    _env_float("DT_RISK_PCT", 0.005, min_value=0.0, max_value=0.25), 0.0, 0.25
)

# Back-compat: DT_MIN_TRADE_SOL is quote-units floor; prefer DT_MIN_TRADE_QUOTE.
_legacy_min_trade_sol: Final[float] = _env_float(
    "DT_MIN_TRADE_SOL", 0.02, min_value=0.0, max_value=1e12
)
_raw_min_trade_quote = _env_str("DT_MIN_TRADE_QUOTE", "")

MIN_TRADE_SOL: Final[float] = _legacy_min_trade_sol
MIN_TRADE_QUOTE: Final[float] = (
    _env_float("DT_MIN_TRADE_QUOTE", _legacy_min_trade_sol, min_value=0.0, max_value=1e12)
    if _raw_min_trade_quote != ""
    else _legacy_min_trade_sol
)

MAX_TRADE_QUOTE: Final[float] = _env_float("DT_MAX_TRADE_QUOTE", 0.0, min_value=0.0, max_value=1e15)

SL_ATR_MULT: Final[float] = _env_float("DT_SL_ATR_MULT", 1.5, min_value=0.0, max_value=100.0)
TP_ATR_MULT: Final[float] = _env_float("DT_TP_ATR_MULT", 3.0, min_value=0.0, max_value=100.0)
TRAIL_ATR_MULT: Final[float] = _env_float("DT_TRAIL_ATR_MULT", 2.0, min_value=0.0, max_value=100.0)

MAX_CONCURRENT_POS: Final[int] = _env_int("DT_MAX_CONCURRENT_POS", 4, min_value=0, max_value=10_000)
DAILY_MAX_DD_PCT: Final[float] = _env_float(
    "DT_DAILY_MAX_DD_PCT", 0.03, min_value=0.0, max_value=1.0
)
ENABLE_CIRCUIT_BREAKER: Final[bool] = _env_bool("DT_ENABLE_CIRCUIT_BREAKER", True)

MAX_GROSS_EXPOSURE_PCT: Final[float] = _env_float(
    "DT_MAX_GROSS_EXPOSURE_PCT", 1.25, min_value=0.0, max_value=50.0
)
MAX_ATR_PCT: Final[float] = _env_float("DT_MAX_ATR_PCT", 0.30, min_value=0.0, max_value=10.0)

BE_ATR_MULT: Final[float] = _env_float("DT_BE_ATR_MULT", 1.0, min_value=0.0, max_value=100.0)
SIZE_SAFETY_FRACTION: Final[float] = _env_float(
    "DT_SIZE_SAFETY_FRACTION", 0.98, min_value=0.1, max_value=1.0
)

DT_STATE_DIR = Path(_env_str("DT_STATE_DIR", "./state"))
DT_RISK_STATE_FILE = _env_str("DT_RISK_STATE_FILE", "daytrader_risk_state.json")
_STATE_PATH = DT_STATE_DIR / DT_RISK_STATE_FILE

try:
    DT_STATE_DIR.mkdir(parents=True, exist_ok=True)
except Exception:
    pass


# =============================================================================
# Data models
# =============================================================================


@dataclass
class OpenContext:
    """
    Inputs for deciding whether we can open a NEW position.
    All amounts are in quote currency units (SOL/USDC/etc).
    """

    equity_quote: float
    equity_peak_quote: float
    gross_exposure_quote: float = 0.0
    open_positions: int = 0
    day_realized_pnl_quote: float = 0.0
    atr_pct: float | None = None


@dataclass
class RiskDecision:
    ok: bool
    reason: str = ""


@dataclass
class RiskState:
    """
    Tiny persistent day/session state.
    - day_key_utc: YYYY-MM-DD
    - equity_peak_quote: day peak used for DD checks
    - opens_blocked: latch to stop new opens for the day
    """

    day_key_utc: str = ""
    equity_peak_quote: float = 0.0
    opens_blocked: bool = False
    blocked_reason: str = ""
    updated_at: str = ""

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @staticmethod
    def from_dict(obj: dict[str, Any]) -> "RiskState":
        try:
            return RiskState(
                day_key_utc=str(obj.get("day_key_utc") or ""),
                equity_peak_quote=_safe_float(obj.get("equity_peak_quote"), 0.0),
                opens_blocked=bool(obj.get("opens_blocked") or False),
                blocked_reason=str(obj.get("blocked_reason") or ""),
                updated_at=str(obj.get("updated_at") or ""),
            )
        except Exception:
            return RiskState()


# =============================================================================
# Persistence (best-effort, never raises)
# =============================================================================


def _load_state() -> RiskState:
    st: RiskState
    try:
        if not _STATE_PATH.exists():
            st = RiskState(day_key_utc=_today_key_utc(), updated_at=_now_iso())
        else:
            txt = _STATE_PATH.read_text(encoding="utf-8", errors="replace").strip()
            if not txt:
                st = RiskState(day_key_utc=_today_key_utc(), updated_at=_now_iso())
            else:
                data = json.loads(txt)
                st = RiskState.from_dict(data) if isinstance(data, dict) else RiskState()
    except Exception:
        st = RiskState()

    today = _today_key_utc()
    if st.day_key_utc != today:
        st = RiskState(
            day_key_utc=today,
            equity_peak_quote=0.0,
            opens_blocked=False,
            blocked_reason="",
            updated_at=_now_iso(),
        )
    return st


def _save_state(st: RiskState) -> None:
    try:
        DT_STATE_DIR.mkdir(parents=True, exist_ok=True)
        st.updated_at = _now_iso()
        tmp_path = _STATE_PATH.with_suffix(_STATE_PATH.suffix + ".tmp")
        tmp_path.write_text(_json_dumps(st.to_dict()), encoding="utf-8")
        os.replace(str(tmp_path), str(_STATE_PATH))
    except Exception:
        pass


# =============================================================================
# Bar math (ATR)
# =============================================================================


def _atr_abs_from_bars(bars: list[dict[str, Any]], period: int = 14) -> float | None:
    p = int(period)
    if p <= 1 or len(bars) < p + 1:
        return None

    true_ranges: list[float] = []
    for idx in range(1, len(bars)):
        prev_close = _safe_float(bars[idx - 1].get("c"), 0.0)
        high_px = _safe_float(bars[idx].get("h"), 0.0)
        low_px = _safe_float(bars[idx].get("l"), 0.0)  # key is "l"; variable is NOT `l`

        if prev_close <= 0.0 or high_px <= 0.0 or low_px <= 0.0:
            continue

        tr_val = max(high_px - low_px, abs(high_px - prev_close), abs(low_px - prev_close))
        if _is_finite(tr_val) and tr_val >= 0.0:
            true_ranges.append(float(tr_val))

    if len(true_ranges) < p:
        return None
    return sum(true_ranges[-p:]) / float(p)


def _atr_pct_from_bars(bars: list[dict[str, Any]], period: int = 14) -> float | None:
    atr_abs = _atr_abs_from_bars(bars, period=period)
    if atr_abs is None:
        return None
    last_close = _safe_float(bars[-1].get("c"), 0.0)
    if last_close <= 0.0:
        return None
    return float(atr_abs) / float(last_close)


# =============================================================================
# Public helpers (kept stable)
# =============================================================================


def atr_to_price(atr_pct: float, price_quote: float) -> float:
    ap = _safe_float(atr_pct, 0.0)
    px = _safe_float(price_quote, 0.0)
    if ap <= 0.0 or px <= 0.0:
        return 0.0
    return max(1e-12, ap * px)


def size_from_risk(balance_quote: float, entry_price_quote: float, atr_price: float) -> float:
    bal = _safe_float(balance_quote, 0.0)
    entry_px = _safe_float(entry_price_quote, 0.0)
    atr_abs = _safe_float(atr_price, 0.0)
    if bal <= 0.0 or entry_px <= 0.0 or atr_abs <= 0.0:
        return 0.0

    risk_amt = max(0.0, bal * max(0.0, RISK_PCT))
    stop_dist = max(1e-12, SL_ATR_MULT * atr_abs)
    qty_token = risk_amt / stop_dist
    notional = qty_token * entry_px
    if notional <= 0.0:
        return 0.0

    notional *= SIZE_SAFETY_FRACTION
    if MAX_TRADE_QUOTE > 0.0:
        notional = min(notional, MAX_TRADE_QUOTE)
    return max(0.0, notional)


def stops_from_atr(
    entry_price_quote: float, atr_price: float, side: str = "buy"
) -> tuple[float, float]:
    entry_px = _safe_float(entry_price_quote, 0.0)
    atr_abs = _safe_float(atr_price, 0.0)
    if entry_px <= 0.0 or atr_abs <= 0.0:
        return 0.0, 0.0

    s = (side or "buy").lower().strip()
    if s == "sell":
        stop_px = max(1e-12, entry_px + SL_ATR_MULT * atr_abs)
        tp_px = max(1e-12, entry_px - TP_ATR_MULT * atr_abs)
    else:
        stop_px = max(1e-12, entry_px - SL_ATR_MULT * atr_abs)
        tp_px = max(1e-12, entry_px + TP_ATR_MULT * atr_abs)
    return stop_px, tp_px


def compute_trade_size(balance_quote: float, entry_price_quote: float, atr_pct: float) -> float:
    entry_px = _safe_float(entry_price_quote, 0.0)
    if entry_px <= 0.0:
        return 0.0
    atr_abs = atr_to_price(_safe_float(atr_pct, 0.0), entry_px)
    notional = size_from_risk(balance_quote, entry_px, atr_abs)
    return notional if notional >= MIN_TRADE_QUOTE else 0.0


def trailing_stop_from_high(
    highest_price_quote: float, atr_price: float, side: str = "buy"
) -> float:
    anchor_px = _safe_float(highest_price_quote, 0.0)
    atr_abs = max(1e-12, _safe_float(atr_price, 0.0))
    if anchor_px <= 0.0:
        return 0.0
    s = (side or "buy").lower().strip()
    if s == "sell":
        return max(1e-12, anchor_px + TRAIL_ATR_MULT * atr_abs)
    return max(1e-12, anchor_px - TRAIL_ATR_MULT * atr_abs)


def update_trailing_stop(
    current_trail_stop: float, anchor_price_quote: float, atr_price: float, side: str = "buy"
) -> float:
    cur_stop = _safe_float(current_trail_stop, 0.0)
    new_stop = trailing_stop_from_high(anchor_price_quote, atr_price, side=side)
    if cur_stop <= 0.0:
        return new_stop
    s = (side or "buy").lower().strip()
    return min(cur_stop, new_stop) if s == "sell" else max(cur_stop, new_stop)


def should_move_to_break_even(
    entry_price_quote: float,
    last_price_quote: float,
    atr_price: float,
    be_atr_mult: float = BE_ATR_MULT,
) -> bool:
    entry_px = _safe_float(entry_price_quote, 0.0)
    last_px = _safe_float(last_price_quote, 0.0)
    atr_abs = _safe_float(atr_price, 0.0)
    mult = _safe_float(be_atr_mult, 1.0)
    if entry_px <= 0.0 or last_px <= 0.0 or atr_abs <= 0.0 or mult <= 0.0:
        return False
    return (last_px - entry_px) >= mult * atr_abs


def normalize_stop(
    entry_price_quote: float, raw_stop: float, min_tick: float | None = None
) -> float:
    entry_px = _safe_float(entry_price_quote, 0.0)
    stop_px = _safe_float(raw_stop, 0.0)
    if stop_px <= 0.0:
        return 0.0

    if entry_px > 0.0:
        stop_px = max(1e-12, min(entry_px, stop_px))
    else:
        stop_px = max(1e-12, stop_px)

    if min_tick is not None and min_tick > 0.0:
        steps = int(stop_px / float(min_tick))
        stop_px = max(1e-12, steps * float(min_tick))
    return stop_px


# =============================================================================
# Portfolio gate
# =============================================================================


def can_open_position(ctx: OpenContext) -> RiskDecision:
    eq = _safe_float(ctx.equity_quote, 0.0)
    peak = _safe_float(ctx.equity_peak_quote, 0.0)

    if eq <= 0.0:
        return RiskDecision(False, "equity<=0")

    if ENABLE_CIRCUIT_BREAKER and peak > 0.0:
        dd_frac = max(0.0, (peak - eq) / peak)
        if dd_frac >= DAILY_MAX_DD_PCT:
            return RiskDecision(False, f"daily drawdown {dd_frac:.2%} >= {DAILY_MAX_DD_PCT:.2%}")

    if ENABLE_CIRCUIT_BREAKER:
        realized_pnl = _safe_float(ctx.day_realized_pnl_quote, 0.0)
        if realized_pnl < 0.0:
            base = peak if peak > 0.0 else eq
            realized_dd = abs(realized_pnl) / max(1e-12, base)
            if realized_dd >= DAILY_MAX_DD_PCT:
                return RiskDecision(
                    False, f"realized daily loss {realized_dd:.2%} >= {DAILY_MAX_DD_PCT:.2%}"
                )

    if int(ctx.open_positions) >= MAX_CONCURRENT_POS:
        return RiskDecision(False, f"max positions {MAX_CONCURRENT_POS} reached")

    gross = _safe_float(ctx.gross_exposure_quote, 0.0)
    max_exposure = MAX_GROSS_EXPOSURE_PCT * eq
    if max_exposure > 0.0 and gross > max_exposure:
        return RiskDecision(False, f"gross exposure {gross:.6f} > cap {max_exposure:.6f}")

    if ctx.atr_pct is not None:
        ap = _safe_float(ctx.atr_pct, 0.0)
        if ap > 0.0 and ap >= MAX_ATR_PCT:
            return RiskDecision(False, f"atr% too high {ap:.3f} >= {MAX_ATR_PCT:.3f}")

    return RiskDecision(True, "ok")


def risk_summary() -> str:
    return (
        f"[dt_risk] RISK_PCT={RISK_PCT:.3%} MIN_TRADE_Q={MIN_TRADE_QUOTE:.6f} "
        f"MAX_TRADE_Q={MAX_TRADE_QUOTE:.6f} SLxATR={SL_ATR_MULT:.2f} "
        f"TPxATR={TP_ATR_MULT:.2f} TRAILxATR={TRAIL_ATR_MULT:.2f} "
        f"MAX_POS={MAX_CONCURRENT_POS} DAILY_MAX_DD={DAILY_MAX_DD_PCT:.2%} "
        f"CIRCUIT={'on' if ENABLE_CIRCUIT_BREAKER else 'off'} "
        f"GROSS_CAP={MAX_GROSS_EXPOSURE_PCT:.0%} MAX_ATR%={MAX_ATR_PCT:.2f}"
    )


# =============================================================================
# RiskManager (preferred API)
# =============================================================================


class RiskManager:
    """
    Refines TradeIntent objects produced by daytrader/features.py.

    - BUY: gates opens, ensures stop/tp, computes risk-based sizing (size_frac).
    - SELL: passes through (risk-off) with basic sanity.
    - Persistence: per-day peak + optional latch block. Best-effort IO.
    """

    def __init__(self) -> None:
        self._st = _load_state()

    def reset_day(self) -> None:
        self._st = RiskState(
            day_key_utc=_today_key_utc(),
            equity_peak_quote=0.0,
            opens_blocked=False,
            blocked_reason="",
            updated_at=_now_iso(),
        )
        _save_state(self._st)

    def update_equity_peak(self, equity_quote: float) -> float:
        eq = _safe_float(equity_quote, 0.0)
        if eq > 0.0:
            if self._st.equity_peak_quote <= 0.0:
                self._st.equity_peak_quote = eq
                _save_state(self._st)
            elif eq > self._st.equity_peak_quote:
                self._st.equity_peak_quote = eq
                _save_state(self._st)
        return float(self._st.equity_peak_quote)

    def latch_block(self, reason: str) -> None:
        self._st.opens_blocked = True
        self._st.blocked_reason = str(reason or "blocked")
        _save_state(self._st)

    def can_open(self, ctx: OpenContext) -> RiskDecision:
        today = _today_key_utc()
        if self._st.day_key_utc != today:
            self.reset_day()

        peak_in = _safe_float(ctx.equity_peak_quote, 0.0)
        if peak_in <= 0.0:
            peak_val = self.update_equity_peak(ctx.equity_quote)
            ctx = OpenContext(
                equity_quote=ctx.equity_quote,
                equity_peak_quote=peak_val,
                gross_exposure_quote=ctx.gross_exposure_quote,
                open_positions=ctx.open_positions,
                day_realized_pnl_quote=ctx.day_realized_pnl_quote,
                atr_pct=ctx.atr_pct,
            )

        if self._st.opens_blocked:
            return RiskDecision(False, f"latched: {self._st.blocked_reason}")

        dec = can_open_position(ctx)

        if not dec.ok and ENABLE_CIRCUIT_BREAKER:
            if "daily drawdown" in dec.reason or "realized daily loss" in dec.reason:
                self.latch_block(dec.reason)

        return dec

    def refine_intent(
        self,
        symbol: str,
        bars: list[dict[str, Any]],
        intent: Any,
        *,
        equity_quote: float | None = None,
        equity_peak_quote: float | None = None,
        open_positions: int = 0,
        gross_exposure_quote: float = 0.0,
        day_realized_pnl_quote: float = 0.0,
    ) -> Any:
        """
        Refine and validate intent. Returns modified intent, or None to reject.
        Never raises.

        Expected intent shape:
          - intent.side in {"buy","sell","hold"}
          - intent.size_frac in [0..1]
          - intent.stop / intent.take_profit are prices (quote per token)
        """
        try:
            if intent is None:
                return None

            side = str(getattr(intent, "side", "")).lower().strip()
            if side not in ("buy", "sell"):
                return None

            last_px = _safe_float(bars[-1].get("c"), 0.0) if bars else 0.0
            if last_px <= 0.0:
                return None

            atr_abs = _atr_abs_from_bars(bars, period=14) or 0.0
            atr_pct = _atr_pct_from_bars(bars, period=14)

            if side == "buy":
                eq = _safe_float(equity_quote, 0.0)
                if eq <= 0.0:
                    return None  # fail-closed for opens without equity context

                peak = _safe_float(equity_peak_quote, 0.0)
                ctx = OpenContext(
                    equity_quote=eq,
                    equity_peak_quote=peak,
                    gross_exposure_quote=_safe_float(gross_exposure_quote, 0.0),
                    open_positions=int(open_positions),
                    day_realized_pnl_quote=_safe_float(day_realized_pnl_quote, 0.0),
                    atr_pct=atr_pct,
                )
                dec = self.can_open(ctx)
                if not dec.ok:
                    return None

            stop_px = _safe_float(getattr(intent, "stop", 0.0), 0.0)
            tp_px = _safe_float(getattr(intent, "take_profit", 0.0), 0.0)

            if atr_abs > 0.0 and (stop_px <= 0.0 or tp_px <= 0.0):
                derived_stop, derived_tp = stops_from_atr(last_px, atr_abs, side=side)
                if stop_px <= 0.0:
                    stop_px = derived_stop
                if tp_px <= 0.0:
                    tp_px = derived_tp

            if side == "buy":
                if stop_px <= 0.0 or stop_px >= last_px:
                    if atr_abs > 0.0:
                        stop_px = max(1e-12, last_px - SL_ATR_MULT * atr_abs)
                    else:
                        return None
                if tp_px <= 0.0 or tp_px <= last_px:
                    if atr_abs > 0.0:
                        tp_px = max(1e-12, last_px + TP_ATR_MULT * atr_abs)
                    else:
                        return None
            else:
                if atr_abs > 0.0:
                    if stop_px <= 0.0 or stop_px <= last_px:
                        stop_px = max(1e-12, last_px + SL_ATR_MULT * atr_abs)
                    if tp_px <= 0.0 or tp_px >= last_px:
                        tp_px = max(1e-12, last_px - TP_ATR_MULT * atr_abs)

            if side == "buy":
                eq = _safe_float(equity_quote, 0.0)
                if eq <= 0.0:
                    return None

                stop_dist = abs(last_px - stop_px)
                if stop_dist <= 0.0 and atr_abs > 0.0:
                    stop_dist = SL_ATR_MULT * atr_abs
                if stop_dist <= 0.0:
                    return None

                risk_amt = max(0.0, eq * max(0.0, RISK_PCT))
                qty_token = risk_amt / max(1e-12, stop_dist)
                notional = qty_token * last_px

                notional *= SIZE_SAFETY_FRACTION
                if MAX_TRADE_QUOTE > 0.0:
                    notional = min(notional, MAX_TRADE_QUOTE)

                if notional < MIN_TRADE_QUOTE:
                    return None

                intent.size_frac = float(_clamp(notional / eq, 0.0, 1.0))

            intent.stop = float(stop_px)
            intent.take_profit = float(tp_px)
            return intent

        except Exception:
            return None


__all__ = [
    "BE_ATR_MULT",
    "DAILY_MAX_DD_PCT",
    "ENABLE_CIRCUIT_BREAKER",
    "MAX_ATR_PCT",
    "MAX_CONCURRENT_POS",
    "MAX_GROSS_EXPOSURE_PCT",
    "MAX_TRADE_QUOTE",
    "MIN_TRADE_QUOTE",
    "MIN_TRADE_SOL",
    "RISK_PCT",
    "SIZE_SAFETY_FRACTION",
    "SL_ATR_MULT",
    "TP_ATR_MULT",
    "TRAIL_ATR_MULT",
    "OpenContext",
    "RiskDecision",
    "RiskManager",
    "RiskState",
    "atr_to_price",
    "can_open_position",
    "compute_trade_size",
    "normalize_stop",
    "risk_summary",
    "should_move_to_break_even",
    "size_from_risk",
    "stops_from_atr",
    "trailing_stop_from_high",
    "update_trailing_stop",
]
