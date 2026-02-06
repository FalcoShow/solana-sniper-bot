# daytrader/risk.py
from __future__ import annotations

"""
daytrader.risk — V3+++ (God-Tier Overhaul, Phase P5) — CANONICAL / LOCK-IN
============================================================================

Risk sizing + portfolio gates for the Daytrader stack (NOT the sniper stack).

Design goals
------------
- Daytrader-only: MUST NOT silently compete with canonical sniper risk logic.
- Deterministic, env-driven, production-safe.
- Never raise to caller (best-effort persistence).
- Fail-closed for NEW opens when critical inputs are missing, but allow SELL/risk-off.

Provides
--------
- Portfolio gate: can we open a NEW position now? (max DD, max positions, exposure cap)
- Volatility-aware sizing: fixed % risk per trade using stop distance (ATR-based).
- Trailing stop helpers + optional break-even move.
- Optional TP ladder generation (ATR-multiple ladder) when enabled.
- Optional cooldowns after losses/blocks to prevent churn.
- RiskManager.refine_intent(...) to refine TradeIntent from daytrader/features.py.

Units
-----
- Quote-currency agnostic. Env names may still say *_SOL for legacy, but treat as QUOTE.

Env (recommended)
-----------------
Core sizing:
- DT_RISK_PCT                float  default 0.005    (0.5% of equity risk per trade)
- DT_MIN_TRADE_QUOTE         float  default ""       (overrides DT_MIN_TRADE_SOL if set)
- DT_MIN_TRADE_SOL           float  default 0.02     (legacy name; quote units)
- DT_MAX_TRADE_QUOTE         float  default 0        (0 = no cap)
- DT_SIZE_SAFETY_FRACTION    float  default 0.98     (leave dust for fees)

Stops/targets:
- DT_SL_ATR_MULT             float  default 1.5
- DT_TP_ATR_MULT             float  default 3.0
- DT_TRAIL_ATR_MULT          float  default 2.0
- DT_BE_ATR_MULT             float  default 1.0      (move stop to breakeven after +k*ATR)

Portfolio gates:
- DT_MAX_CONCURRENT_POS      int    default 4
- DT_DAILY_MAX_DD_PCT        float  default 0.03     (stop opens after -3% from peak)
- DT_ENABLE_CIRCUIT_BREAKER  bool   default true
- DT_MAX_GROSS_EXPOSURE_PCT  float  default 1.25     (125% of equity)
- DT_MAX_ATR_PCT             float  default 0.30     (hard block opens if ATR% too high)

Hard-stop guards (additional P5 safety):
- DT_MAX_LOSS_PER_TRADE_PCT  float  default 0.02     (hard cap; blocks/reshapes opens if risk>cap)

Optional cooldowns (anti-churn):
- DT_COOLDOWN_AFTER_LOSS_SEC int    default 0        (0 disables)
- DT_COOLDOWN_AFTER_BLOCK_SEC int   default 0        (0 disables)
- DT_SYMBOL_COOLDOWN_MAX     int    default 256      (cap persisted cooldown map size)

Optional TP ladder (ATR-based take-profit ladder):
- DT_ENABLE_TP_LADDER        bool   default false
- DT_TP_LADDER_ATR_MULTS     csv    default "1.5,3,6"
- DT_TP_LADDER_SPLITS        csv    default "0.4,0.3,0.3"

Optional (state)
- DT_STATE_DIR               path   default ./state
- DT_RISK_STATE_FILE         name   default daytrader_risk_state.json
"""

import json
import math
import os
import time
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
    return json.dumps(obj, indent=2, sort_keys=True)


def _parse_csv_floats(raw: str) -> list[float]:
    out: list[float] = []
    compact = (raw or "").replace(" ", "")
    for part in compact.split(","):
        if not part:
            continue
        try:
            val = float(part)
        except Exception:
            continue
        if _is_finite(val):
            out.append(float(val))
    return out


def _cap_dict_size(d: dict[str, Any], max_items: int) -> None:
    if max_items <= 0:
        d.clear()
        return
    if len(d) <= max_items:
        return
    # Drop oldest by timestamp if values are numeric timestamps; else arbitrary pop
    items = list(d.items())
    try:
        items.sort(key=lambda kv: float(kv[1]))
        for k, _ in items[: max(0, len(items) - max_items)]:
            d.pop(k, None)
    except Exception:
        while len(d) > max_items:
            d.pop(next(iter(d)), None)


# =============================================================================
# Tunables (module-level, deterministic)
# =============================================================================
RISK_PCT: Final[float] = _clamp(
    _env_float("DT_RISK_PCT", 0.005, min_value=0.0, max_value=0.25), 0.0, 0.25
)

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

# P5 hard guard: cap per-trade loss
MAX_LOSS_PER_TRADE_PCT: Final[float] = _env_float(
    "DT_MAX_LOSS_PER_TRADE_PCT", 0.02, min_value=0.0, max_value=1.0
)

# Optional cooldowns
COOLDOWN_AFTER_LOSS_SEC: Final[int] = _env_int(
    "DT_COOLDOWN_AFTER_LOSS_SEC", 0, min_value=0, max_value=86_400
)
COOLDOWN_AFTER_BLOCK_SEC: Final[int] = _env_int(
    "DT_COOLDOWN_AFTER_BLOCK_SEC", 0, min_value=0, max_value=86_400
)
SYMBOL_COOLDOWN_MAX: Final[int] = _env_int(
    "DT_SYMBOL_COOLDOWN_MAX", 256, min_value=0, max_value=50_000
)

# Optional TP ladder
ENABLE_TP_LADDER: Final[bool] = _env_bool("DT_ENABLE_TP_LADDER", False)
TP_LADDER_ATR_MULTS: Final[list[float]] = _parse_csv_floats(
    _env_str("DT_TP_LADDER_ATR_MULTS", "1.5,3,6")
) or [1.5, 3.0, 6.0]
TP_LADDER_SPLITS: Final[list[float]] = _parse_csv_floats(
    _env_str("DT_TP_LADDER_SPLITS", "0.4,0.3,0.3")
) or [0.4, 0.3, 0.3]

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
    - symbol_cooldown_until_ts: unix ts per symbol (optional anti-churn)
    """

    day_key_utc: str = ""
    equity_peak_quote: float = 0.0
    opens_blocked: bool = False
    blocked_reason: str = ""
    updated_at: str = ""
    symbol_cooldown_until_ts: dict[str, float] | None = None

    def to_dict(self) -> dict[str, Any]:
        d = asdict(self)
        if d.get("symbol_cooldown_until_ts") is None:
            d["symbol_cooldown_until_ts"] = {}
        return d

    @staticmethod
    def from_dict(obj: dict[str, Any]) -> "RiskState":
        try:
            cooldown_obj = obj.get("symbol_cooldown_until_ts")
            cooldown_map: dict[str, float] = {}
            if isinstance(cooldown_obj, dict):
                for k, v in cooldown_obj.items():
                    if not k:
                        continue
                    cooldown_map[str(k)] = _safe_float(v, 0.0)

            return RiskState(
                day_key_utc=str(obj.get("day_key_utc") or ""),
                equity_peak_quote=_safe_float(obj.get("equity_peak_quote"), 0.0),
                opens_blocked=bool(obj.get("opens_blocked") or False),
                blocked_reason=str(obj.get("blocked_reason") or ""),
                updated_at=str(obj.get("updated_at") or ""),
                symbol_cooldown_until_ts=cooldown_map,
            )
        except Exception:
            return RiskState(symbol_cooldown_until_ts={})


# =============================================================================
# Persistence (best-effort, never raises)
# =============================================================================
def _load_state() -> RiskState:
    st: RiskState
    try:
        if not _STATE_PATH.exists():
            st = RiskState(
                day_key_utc=_today_key_utc(), updated_at=_now_iso(), symbol_cooldown_until_ts={}
            )
        else:
            txt = _STATE_PATH.read_text(encoding="utf-8", errors="replace").strip()
            if not txt:
                st = RiskState(
                    day_key_utc=_today_key_utc(), updated_at=_now_iso(), symbol_cooldown_until_ts={}
                )
            else:
                data = json.loads(txt)
                st = (
                    RiskState.from_dict(data)
                    if isinstance(data, dict)
                    else RiskState(symbol_cooldown_until_ts={})
                )
    except Exception:
        st = RiskState(symbol_cooldown_until_ts={})

    if st.symbol_cooldown_until_ts is None:
        st.symbol_cooldown_until_ts = {}

    today = _today_key_utc()
    if st.day_key_utc != today:
        st = RiskState(
            day_key_utc=today,
            equity_peak_quote=0.0,
            opens_blocked=False,
            blocked_reason="",
            updated_at=_now_iso(),
            symbol_cooldown_until_ts={},
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
def _bar_get(b: dict[str, Any], *keys: str) -> float:
    for k in keys:
        if k in b:
            return _safe_float(b.get(k), 0.0)
    return 0.0


def _atr_abs_from_bars(bars: list[dict[str, Any]], period: int = 14) -> float | None:
    p = int(period)
    if p <= 1 or len(bars) < p + 1:
        return None

    true_ranges: list[float] = []
    for idx in range(1, len(bars)):
        prev_close = _bar_get(bars[idx - 1], "c", "close")
        high_px = _bar_get(bars[idx], "h", "high")
        low_px = _bar_get(bars[idx], "l", "low")  # key is "l"; variable is NOT ambiguous

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
    last_close = _bar_get(bars[-1], "c", "close")
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

    # nominal risk per trade
    risk_amt = max(0.0, bal * max(0.0, RISK_PCT))

    # hard cap risk (P5)
    if MAX_LOSS_PER_TRADE_PCT > 0.0:
        risk_amt = min(risk_amt, bal * MAX_LOSS_PER_TRADE_PCT)

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
    entry_price_quote: float,
    raw_stop: float,
    min_tick: float | None = None,
    side: str = "buy",
) -> float:
    """
    Backward-compatible helper with added `side` (default buy).
    Ensures stops are on the correct side of entry.
    """
    entry_px = _safe_float(entry_price_quote, 0.0)
    stop_px = _safe_float(raw_stop, 0.0)
    if stop_px <= 0.0:
        return 0.0

    s = (side or "buy").lower().strip()
    if entry_px > 0.0:
        if s == "sell":
            stop_px = max(1e-12, max(entry_px, stop_px))
        else:
            stop_px = max(1e-12, min(entry_px, stop_px))
    else:
        stop_px = max(1e-12, stop_px)

    if min_tick is not None and min_tick > 0.0:
        steps = int(stop_px / float(min_tick))
        stop_px = max(1e-12, steps * float(min_tick))
    return stop_px


# =============================================================================
# TP ladder (optional)
# =============================================================================
def tp_ladder_from_atr(
    entry_price_quote: float,
    atr_price: float,
    *,
    side: str = "buy",
    atr_mults: list[float] | None = None,
    splits: list[float] | None = None,
) -> list[dict[str, float]]:
    """
    Build a TP ladder as a list of {price, fraction} steps.
    Only used when DT_ENABLE_TP_LADDER is enabled.
    """
    entry_px = _safe_float(entry_price_quote, 0.0)
    atr_abs = _safe_float(atr_price, 0.0)
    if entry_px <= 0.0 or atr_abs <= 0.0:
        return []

    s = (side or "buy").lower().strip()
    mults = atr_mults or TP_LADDER_ATR_MULTS
    fracs = splits or TP_LADDER_SPLITS

    m2 = [float(x) for x in mults if _is_finite(float(x)) and float(x) > 0.0]
    f2 = [float(x) for x in fracs if _is_finite(float(x)) and float(x) > 0.0]
    if not m2 or not f2:
        return []

    # normalize fractions to sum 1.0
    ssum = sum(f2)
    if ssum <= 0.0:
        return []
    f2 = [x / ssum for x in f2]

    # align lengths: repeat last fraction if needed
    if len(f2) < len(m2):
        f2 = f2 + [f2[-1]] * (len(m2) - len(f2))
    if len(m2) < len(f2):
        f2 = f2[: len(m2)]

    out: list[dict[str, float]] = []
    for mult, frac in zip(m2, f2, strict=False):
        if s == "sell":
            tp_px = max(1e-12, entry_px - (mult * atr_abs))
        else:
            tp_px = max(1e-12, entry_px + (mult * atr_abs))
        out.append({"price": float(tp_px), "fraction": float(_clamp(frac, 0.0, 1.0))})

    return out


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
        f"[dt_risk] RISK_PCT={RISK_PCT:.3%} MAX_LOSS_TRADE={MAX_LOSS_PER_TRADE_PCT:.2%} "
        f"MIN_TRADE_Q={MIN_TRADE_QUOTE:.6f} MAX_TRADE_Q={MAX_TRADE_QUOTE:.6f} "
        f"SLxATR={SL_ATR_MULT:.2f} TPxATR={TP_ATR_MULT:.2f} TRAILxATR={TRAIL_ATR_MULT:.2f} "
        f"MAX_POS={MAX_CONCURRENT_POS} DAILY_MAX_DD={DAILY_MAX_DD_PCT:.2%} "
        f"CIRCUIT={'on' if ENABLE_CIRCUIT_BREAKER else 'off'} "
        f"GROSS_CAP={MAX_GROSS_EXPOSURE_PCT:.0%} MAX_ATR%={MAX_ATR_PCT:.2f} "
        f"TP_LADDER={'on' if ENABLE_TP_LADDER else 'off'}"
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
    - Optional symbol cooldowns (anti-churn).
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
            symbol_cooldown_until_ts={},
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

    def _cooldown_until(self, symbol: str) -> float:
        m = self._st.symbol_cooldown_until_ts or {}
        return _safe_float(m.get(symbol, 0.0), 0.0)

    def _set_cooldown(self, symbol: str, seconds: int, reason: str) -> None:
        if seconds <= 0 or not symbol:
            return
        m = self._st.symbol_cooldown_until_ts
        if m is None:
            m = {}
            self._st.symbol_cooldown_until_ts = m
        until_ts = float(time.time()) + float(seconds)
        m[str(symbol)] = until_ts
        _cap_dict_size(m, SYMBOL_COOLDOWN_MAX)
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

        Expected intent shape (best-effort):
          - intent.side in {"buy","sell","hold"}
          - intent.size_frac in [0..1]
          - intent.stop / intent.take_profit are prices (quote per token)
        """
        try:
            if intent is None:
                return None

            sym = str(symbol or "").strip()
            side = str(getattr(intent, "side", "")).lower().strip()
            if side not in ("buy", "sell"):
                return None

            if sym and self._cooldown_until(sym) > float(time.time()):
                return None

            last_px = _bar_get(bars[-1], "c", "close") if bars else 0.0
            if last_px <= 0.0:
                return None

            atr_abs = _atr_abs_from_bars(bars, period=14) or 0.0
            atr_pct = _atr_pct_from_bars(bars, period=14)

            # BUY gate: fail-closed without equity context
            if side == "buy":
                eq = _safe_float(equity_quote, 0.0)
                if eq <= 0.0:
                    return None

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
                    if sym and COOLDOWN_AFTER_BLOCK_SEC > 0:
                        self._set_cooldown(sym, COOLDOWN_AFTER_BLOCK_SEC, reason=dec.reason)
                    return None

            stop_px = _safe_float(getattr(intent, "stop", 0.0), 0.0)
            tp_px = _safe_float(getattr(intent, "take_profit", 0.0), 0.0)

            # Derive stop/tp if missing and ATR available
            if atr_abs > 0.0 and (stop_px <= 0.0 or tp_px <= 0.0):
                derived_stop, derived_tp = stops_from_atr(last_px, atr_abs, side=side)
                if stop_px <= 0.0:
                    stop_px = derived_stop
                if tp_px <= 0.0:
                    tp_px = derived_tp

            # Sanity correct stop/tp
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
                # sells are risk-off; keep reasonable if ATR available
                if atr_abs > 0.0:
                    if stop_px <= 0.0 or stop_px <= last_px:
                        stop_px = max(1e-12, last_px + SL_ATR_MULT * atr_abs)
                    if tp_px <= 0.0 or tp_px >= last_px:
                        tp_px = max(1e-12, last_px - TP_ATR_MULT * atr_abs)

            # BUY sizing (risk based)
            if side == "buy":
                eq = _safe_float(equity_quote, 0.0)
                if eq <= 0.0:
                    return None

                stop_dist = abs(last_px - stop_px)
                if stop_dist <= 0.0 and atr_abs > 0.0:
                    stop_dist = SL_ATR_MULT * atr_abs
                if stop_dist <= 0.0:
                    return None

                # risk amount with hard cap (P5)
                risk_amt = max(0.0, eq * max(0.0, RISK_PCT))
                if MAX_LOSS_PER_TRADE_PCT > 0.0:
                    risk_amt = min(risk_amt, eq * MAX_LOSS_PER_TRADE_PCT)

                qty_token = risk_amt / max(1e-12, stop_dist)
                notional = qty_token * last_px

                notional *= SIZE_SAFETY_FRACTION
                if MAX_TRADE_QUOTE > 0.0:
                    notional = min(notional, MAX_TRADE_QUOTE)

                if notional < MIN_TRADE_QUOTE:
                    return None

                intent.size_frac = float(_clamp(notional / eq, 0.0, 1.0))

            # Attach stop/tp
            intent.stop = float(stop_px)
            intent.take_profit = float(tp_px)

            # Optional TP ladder (non-breaking: set only if attribute is writable)
            if ENABLE_TP_LADDER and atr_abs > 0.0:
                ladder = tp_ladder_from_atr(last_px, atr_abs, side=side)
                if ladder:
                    # Try multiple conventional attribute names without breaking slots
                    for attr in ("tp_ladder", "take_profit_ladder", "tp_steps"):
                        try:
                            setattr(intent, attr, ladder)
                            break
                        except Exception:
                            continue

            return intent

        except Exception:
            # Optional: cooldown after loss is enforced by the caller when loss realized.
            return None


__all__ = [
    "BE_ATR_MULT",
    "COOLDOWN_AFTER_BLOCK_SEC",
    "COOLDOWN_AFTER_LOSS_SEC",
    "DAILY_MAX_DD_PCT",
    "ENABLE_CIRCUIT_BREAKER",
    "ENABLE_TP_LADDER",
    "MAX_ATR_PCT",
    "MAX_CONCURRENT_POS",
    "MAX_GROSS_EXPOSURE_PCT",
    "MAX_LOSS_PER_TRADE_PCT",
    "MAX_TRADE_QUOTE",
    "MIN_TRADE_QUOTE",
    "MIN_TRADE_SOL",
    "RISK_PCT",
    "SIZE_SAFETY_FRACTION",
    "SL_ATR_MULT",
    "SYMBOL_COOLDOWN_MAX",
    "TP_ATR_MULT",
    "TP_LADDER_ATR_MULTS",
    "TP_LADDER_SPLITS",
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
    "tp_ladder_from_atr",
    "trailing_stop_from_high",
    "update_trailing_stop",
]
