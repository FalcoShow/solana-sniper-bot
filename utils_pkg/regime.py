from __future__ import annotations

"""
utils_pkg/regime.py

Global market regime engine (V3+++ / quant-grade / God-Tier)
===========================================================

Role
----
Turns macro + meme market + smart-wallets + account + infra context into:

  Discrete regime:
    - "risk_off" / "normal" / "risk_on"

  Continuous regime features (numeric-only, for AI / FeatureBuilder):
    - trend_score                 ∈ [-1, +1]
    - volatility_score            ∈ [0, 1]
    - liquidity_score             ∈ [0, 1]
    - manipulation_risk           ∈ [0, 1]
    - regime_choppy_score         ∈ [0, 1]
    - regime_confidence           ∈ [0, 1]
    - regime_meme_heat            ∈ [0, 1]
    - macro_risk_score            ∈ [-1, +1]
    - regime_stress_score         ∈ [0, 1]
    - regime_signal_strength      ∈ [0, 1]
    - tradeability_score          ∈ [0, 1]
    - account_risk_bias           ∈ [-1, +1]
    - infra_degraded_score        ∈ [0, 1]

  Regime-aware risk knobs:
    - regime_risk_mode            ∈ {0,1,2}
    - regime_risk_multiplier      ≈ [0.4, 1.6]
    - regime_entry_tighten        ≈ [0.4, 1.5]
    - regime_entry_gate_factor    ≈ [0.2, 1.5]
    - regime_slippage_hint_bps    ≈ [MIN_BPS, MAX_BPS]
    - risk_profile_hint           ∈ {"conservative","balanced","aggressive"}
    - regime_should_throttle_entries ∈ {0,1}
    - regime_should_pause_new_entries ∈ {0,1}
    - regime_is_warmup            ∈ {0,1}

  Meta-safety / health:
    - regime_last_change_sec_ago
    - regime_last_eval_sec_ago
    - regime_is_stale
    - regime_hard_stop
    - infra_hard_block

Design principles (God-tier):
-----------------------------
- Deterministic + stable: smooth scores (EMA) and use hysteresis.
- PnL-positive safety: warmup + stale clamps always reduce risk.
- Backwards-compatible outputs: singleton + helper functions.
- Async-safe: one lock; minimal work under lock (compute-heavy outside).
- “Strict inputs, graceful degradation”: tolerate missing scanner stats; normalize DD sign.

Integration points:
- Scanners call update_from_scanner_batch(...)
- smart_wallets calls update_from_smart_wallets_global(...)
- risk_manager/accounting calls update_from_account(...)
- infra_guard/router/health calls update_from_infra(...)
- scripts/main.py should call regime_state.ensure_background_task() on startup
"""

import asyncio
import os
import statistics
import time
from bisect import bisect_left
from dataclasses import dataclass
from typing import Any, Dict, List, Literal, Optional, Tuple

from logging_setup import get_logger
from utils_pkg.core_utils import get_sol_usd

# ──────────────────────────────────────────────────────────────────────────────
# Logger (robust to get_logger signature differences)
# ──────────────────────────────────────────────────────────────────────────────
def _get_log():
    try:
        return get_logger("sniper.regime")  # type: ignore[arg-type]
    except TypeError:
        try:
            lg = get_logger()
            if hasattr(lg, "getChild"):
                return lg.getChild("regime")
            return lg
        except Exception:
            import logging
            return logging.getLogger("sniper.regime")


log = _get_log()

Regime = Literal["risk_on", "normal", "risk_off"]
REGIME_VERSION = "v3+++_godtier_2025-12-22"

# ──────────────────────────────────────────────────────────────────────────────
# Env helpers
# ──────────────────────────────────────────────────────────────────────────────
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


def _env_bool(key: str, default: bool = False) -> bool:
    v = os.getenv(key)
    if v is None:
        return default
    return str(v).strip().lower() in ("1", "true", "yes", "y", "on")


def _clamp(x: float, lo: float, hi: float) -> float:
    try:
        xf = float(x)
    except Exception:
        return lo
    if xf != xf:  # NaN
        return lo
    if xf < lo:
        return lo
    if xf > hi:
        return hi
    return xf


def _safe_float(x: Any, default: float = 0.0) -> float:
    try:
        if x is None or isinstance(x, bool):
            return default
        return float(x)
    except Exception:
        return default


def _norm_between(x: float, lo: float, hi: float) -> float:
    """Normalize x from [lo, hi] → [0,1], clipped."""
    if hi <= lo:
        return 0.5
    return _clamp((float(x) - lo) / (hi - lo), 0.0, 1.0)


def _ema(prev: float, new: float, alpha: float) -> float:
    a = _clamp(alpha, 0.0, 1.0)
    return (1.0 - a) * float(prev) + a * float(new)


def _pct_ret(new: float, old: float) -> float:
    try:
        o = float(old)
        n = float(new)
        if o <= 0.0:
            return 0.0
        return (n / o - 1.0) * 100.0
    except Exception:
        return 0.0


def _utc_hour(ts: Optional[float] = None) -> int:
    if ts is None:
        ts = time.time()
    return int(time.gmtime(ts).tm_hour)


def _utc_weekday(ts: Optional[float] = None) -> int:
    """Monday=0 ... Sunday=6 (accurate via gmtime)."""
    if ts is None:
        ts = time.time()
    return int(time.gmtime(ts).tm_wday)


def _session_of_day(ts: Optional[float] = None) -> str:
    """Lightweight UTC-session detector: 'asia' / 'eu' / 'us'."""
    h = _utc_hour(ts)
    if 0 <= h < 8:
        return "asia"
    if 8 <= h < 16:
        return "eu"
    return "us"


def _weekend_factor(ts: Optional[float] = None) -> float:
    """1.0 weekdays; ~0.7 on weekend (Sat/Sun) when enabled."""
    wd = _utc_weekday(ts)
    if wd in (5, 6):  # Sat/Sun
        return 0.7
    return 1.0


async def _call_maybe_async(fn, *args, **kwargs) -> Any:
    res = fn(*args, **kwargs)
    if asyncio.iscoroutine(res):
        return await res
    return res


# ──────────────────────────────────────────────────────────────────────────────
# Core toggles / thresholds
# ──────────────────────────────────────────────────────────────────────────────
REGIME_ENABLED = _env_bool("REGIME_ENABLED", True)

# Poll / history
REGIME_POLL_SEC = max(5, _env_int("REGIME_POLL_SEC", 60))
REGIME_HISTORY_HORIZON_SEC = _env_int("REGIME_HISTORY_HORIZON_SEC", 7200)  # 2h
REGIME_MIN_OBS_SEC = _env_int("REGIME_MIN_OBS_SEC", 600)  # 10m warmup
REGIME_HISTORY_MAX_POINTS = _env_int("REGIME_HISTORY_MAX_POINTS", 900)

# SOL tape thresholds (%)
SOL_RISK_ON_RET_1H = _env_float("REGIME_SOL_RISK_ON_RET_1H", 3.0)
SOL_RISK_ON_RET_5M = _env_float("REGIME_SOL_RISK_ON_RET_5M", 1.0)
SOL_RISK_OFF_RET_1H = _env_float("REGIME_SOL_RISK_OFF_RET_1H", -3.0)
SOL_RISK_OFF_RET_5M = _env_float("REGIME_SOL_RISK_OFF_RET_5M", -1.5)

# Volatility band (%)
REGIME_VOL_5M_HIGH_PCT = _env_float("REGIME_VOL_5M_HIGH_PCT", 2.5)

# Hysteresis / smoothing
REGIME_SMOOTH_SEC = _env_int("REGIME_SMOOTH_SEC", 300)  # min time between flips
REGIME_SCORE_EMA_ALPHA = _env_float("REGIME_SCORE_EMA_ALPHA", 0.25)
REGIME_SWITCH_MIN_CONF = _env_float("REGIME_SWITCH_MIN_CONF", 0.55)
REGIME_SWITCH_MIN_SIG = _env_float("REGIME_SWITCH_MIN_SIG", 0.45)

# Stale detection
REGIME_STALE_SEC = _env_int("REGIME_STALE_SEC", REGIME_POLL_SEC * 4)

# SOL fetch timeout / failure behavior
REGIME_SOL_FETCH_TIMEOUT_S = _env_float("REGIME_SOL_FETCH_TIMEOUT_S", 3.0)
REGIME_SOL_MAX_CONSEC_FAIL = _env_int("REGIME_SOL_MAX_CONSEC_FAIL", 5)

# Scanner-based normalization (meme market)
MEME_HIGH_LIQ_USD = _env_float("REGIME_MEME_HIGH_LIQ_USD", 200_000.0)
MEME_LOW_LIQ_USD = _env_float("REGIME_MEME_LOW_LIQ_USD", 30_000.0)
MEME_PUMP_DENSITY_HI = _env_float("REGIME_MEME_PUMP_DENSITY_HI", 0.35)
MEME_RUG_RATE_HI = _env_float("REGIME_MEME_RUG_RATE_HI", 0.20)
MEME_BREADTH_BULLISH = _env_float("REGIME_MEME_BREADTH_BULLISH", 0.60)
MEME_BREADTH_BEARISH = _env_float("REGIME_MEME_BREADTH_BEARISH", 0.40)

# Risk multipliers
RISK_MULT_RISK_OFF = _env_float("REGIME_RISK_MULT_RISK_OFF", 0.55)
RISK_MULT_NORMAL = _env_float("REGIME_RISK_MULT_NORMAL", 1.0)
RISK_MULT_RISK_ON = _env_float("REGIME_RISK_MULT_RISK_ON", 1.25)

RISK_MULT_MIN_CAP = _env_float("REGIME_RISK_MULT_MIN_CAP", 0.4)
RISK_MULT_MAX_CAP = _env_float("REGIME_RISK_MULT_MAX_CAP", 1.6)
REGIME_DYNAMIC_MULT_SPAN = _env_float("REGIME_DYNAMIC_MULT_SPAN", 0.25)

# Manipulation-driven forced risk_off
REGIME_MANIP_RISK_OFF_THRESH = _env_float("REGIME_MANIP_RISK_OFF_THRESH", 0.75)

# Account-level (%)
ACCOUNT_DD_RISK_OFF = _env_float("REGIME_ACCOUNT_DD_RISK_OFF", 8.0)
ACCOUNT_DD_HARD_STOP = _env_float("REGIME_ACCOUNT_DD_HARD_STOP", 15.0)
ACCOUNT_PNL_RISK_ON = _env_float("REGIME_ACCOUNT_PNL_RISK_ON", 5.0)
ACCOUNT_CONSEC_LOSS_RISK_OFF = _env_int("REGIME_ACCOUNT_CONSEC_LOSS_RISK_OFF", 4)

# Entry-tightening controls
REGIME_ENTRY_TIGHTEN_MAX = _env_float("REGIME_ENTRY_TIGHTEN_MAX", 0.5)
REGIME_ENTRY_LOOSEN_MAX = _env_float("REGIME_ENTRY_LOOSEN_MAX", 0.2)

# Scanner smoothing
REGIME_SCANNER_EMA_ALPHA = _clamp(_env_float("REGIME_SCANNER_EMA_ALPHA", 0.30), 0.0, 1.0)

# Optional override / debug
REGIME_FORCE_LABEL_RAW = os.getenv("REGIME_FORCE_LABEL", "").strip().lower()
REGIME_FORCE_LABEL = (
    REGIME_FORCE_LABEL_RAW if REGIME_FORCE_LABEL_RAW in ("", "risk_on", "normal", "risk_off") else ""
)
REGIME_DEBUG_LOG = _env_bool("REGIME_DEBUG_LOG", False)
REGIME_DEBUG_EVERY_SEC = max(5, _env_int("REGIME_DEBUG_EVERY_SEC", 180))

# Session / weekend extras
REGIME_SESSION_BIAS_ENABLED = _env_bool("REGIME_SESSION_BIAS_ENABLED", True)
REGIME_WEAK_WEEKEND_MODE = _env_bool("REGIME_WEAK_WEEKEND_MODE", True)

# Influence weights (0..1)
REGIME_ACCOUNT_INFLUENCE = _clamp(_env_float("REGIME_ACCOUNT_INFLUENCE", 0.8), 0.0, 1.0)
REGIME_INFRA_INFLUENCE = _clamp(_env_float("REGIME_INFRA_INFLUENCE", 0.6), 0.0, 1.0)

# Tradeability gating
REGIME_TRADEABILITY_HARD_MIN = _env_float("REGIME_TRADEABILITY_HARD_MIN", 0.15)
REGIME_TRADEABILITY_SOFT_MIN = _env_float("REGIME_TRADEABILITY_SOFT_MIN", 0.30)

# Infra kill-switch participation
REGIME_PAUSE_ON_INFRA_HARD = _env_bool("REGIME_PAUSE_ON_INFRA_HARD", True)

# Slippage hints (bps)
REGIME_SLIPPAGE_HINT_MIN_BPS = max(1, _env_int("REGIME_SLIPPAGE_HINT_MIN_BPS", 40))
REGIME_SLIPPAGE_HINT_MAX_BPS = max(REGIME_SLIPPAGE_HINT_MIN_BPS, _env_int("REGIME_SLIPPAGE_HINT_MAX_BPS", 200))

# Warmup & stale clamps
REGIME_WARMUP_RISK_CLAMP = _clamp(_env_float("REGIME_WARMUP_RISK_CLAMP", 0.75), 0.1, 1.0)
REGIME_STALE_RISK_CLAMP = _clamp(_env_float("REGIME_STALE_RISK_CLAMP", 0.70), 0.1, 1.0)
REGIME_WARMUP_ENTRY_TIGHTEN_CAP = _clamp(_env_float("REGIME_WARMUP_ENTRY_TIGHTEN_CAP", 0.90), 0.6, 1.0)

# Vol computation window controls
REGIME_VOL_WINDOW_SEC = max(60, _env_int("REGIME_VOL_WINDOW_SEC", 5 * 60))          # default "vol_5m"
REGIME_VOL_FALLBACK_SEC = max(REGIME_VOL_WINDOW_SEC, _env_int("REGIME_VOL_FALLBACK_SEC", 15 * 60))
REGIME_VOL_MAX_GAP_MULT = _clamp(_env_float("REGIME_VOL_MAX_GAP_MULT", 4.0), 1.0, 20.0)


# ──────────────────────────────────────────────────────────────────────────────
# History helpers
# ──────────────────────────────────────────────────────────────────────────────
def _rolling_return(history: List[Tuple[float, float]], horizon_sec: float) -> float:
    """% return over horizon using the first obs with ts >= now - horizon."""
    if len(history) < 2:
        return 0.0
    now_ts = history[-1][0]
    newest = history[-1][1]
    target_ts = now_ts - float(horizon_sec)

    ts_list = [t for (t, _) in history]
    idx = bisect_left(ts_list, target_ts)
    ref_px = history[0][1] if idx <= 0 else history[min(idx, len(history) - 1)][1]
    return _pct_ret(newest, ref_px)


def _subset_history(history: List[Tuple[float, float]], horizon_sec: float) -> List[Tuple[float, float]]:
    if len(history) < 2:
        return history
    now_ts = history[-1][0]
    cutoff = now_ts - float(horizon_sec)
    ts_list = [t for (t, _) in history]
    idx = bisect_left(ts_list, cutoff)
    return history[max(0, idx - 1) :]


def _step_returns(history: List[Tuple[float, float]], max_gap_sec: float) -> List[float]:
    """Stepwise returns for realized-vol estimates. Skips huge gaps."""
    if len(history) < 2:
        return []
    out: List[float] = []
    prev_ts, prev_px = history[0]
    for ts, px in history[1:]:
        if ts - prev_ts <= max_gap_sec and prev_px > 0:
            out.append(_pct_ret(px, prev_px))
        prev_ts, prev_px = ts, px
    return out


def _calc_sol_stats(history: List[Tuple[float, float]]) -> Tuple[float, float, float]:
    """Return (ret_5m, ret_1h, vol_window) from copied history."""
    if len(history) < 2:
        return 0.0, 0.0, 0.0

    ret_5m = _rolling_return(history, 5 * 60)
    ret_1h = _rolling_return(history, 60 * 60)

    max_gap = max(15.0, float(REGIME_POLL_SEC) * float(REGIME_VOL_MAX_GAP_MULT))

    # Vol: prefer last REGIME_VOL_WINDOW_SEC; fallback to REGIME_VOL_FALLBACK_SEC; then 1h
    h1 = _subset_history(history, float(REGIME_VOL_WINDOW_SEC))
    step1 = _step_returns(h1, max_gap_sec=max_gap)

    if step1:
        vol = float(statistics.pstdev(step1))
    else:
        h2 = _subset_history(history, float(REGIME_VOL_FALLBACK_SEC))
        step2 = _step_returns(h2, max_gap_sec=max_gap)
        if step2:
            vol = float(statistics.pstdev(step2))
        else:
            h3 = _subset_history(history, 60 * 60)
            step3 = _step_returns(h3, max_gap_sec=max_gap)
            vol = float(statistics.pstdev(step3)) if step3 else 0.0

    return float(ret_5m), float(ret_1h), float(vol)


# ──────────────────────────────────────────────────────────────────────────────
# Snapshot (safe read without holding lock too long)
# ──────────────────────────────────────────────────────────────────────────────
@dataclass(frozen=True)
class _RegimeSnap:
    regime: Regime
    last_change_ts: float
    last_eval_ts: float
    last_px: float
    history: Tuple[Tuple[float, float], ...]

    consec_sol_fail: int

    # Scores (smoothed)
    trend_score: float
    volatility_score: float
    liquidity_score: float
    manipulation_risk: float
    choppy_score: float
    confidence_score: float
    meme_heat: float

    # Scanner aggregates
    meme_breadth_up_ratio: float
    meme_liq_score: float
    meme_pump_density: float
    meme_rug_rate: float

    # Smart/whale aggregates
    smart_accum: float
    smart_panic: float
    whale_dom: float
    smart_conflict: float
    smart_risk: float

    # Account
    account_daily_pnl_pct: float
    account_daily_dd_pct: float
    account_consec_losses: int
    account_risk_bias: float
    account_equity_slope: float
    account_intraday_vol: float
    account_kelly_hint: float

    # Infra
    infra_degraded_score: float
    infra_hard_block: bool

    # External overrides
    external_macro_bias: float
    external_risk_off_flag: bool


# ──────────────────────────────────────────────────────────────────────────────
# Regime engine
# ──────────────────────────────────────────────────────────────────────────────
class RegimeState:
    """
    Global macro + memecoin + infra regime engine.
    """

    def __init__(self) -> None:
        # Discrete regime
        self._regime: Regime = "normal"
        self._last_change_ts: float = time.time()
        self._last_eval_ts: float = 0.0

        # SOL price history
        self._history: List[Tuple[float, float]] = []
        self._last_px: float = 0.0
        self._consec_sol_fail: int = 0

        # Continuous scores (smoothed)
        self.trend_score: float = 0.0
        self.volatility_score: float = 0.0
        self.liquidity_score: float = 0.5
        self.manipulation_risk: float = 0.0
        self.choppy_score: float = 0.0
        self.confidence_score: float = 0.5
        self.meme_heat: float = 0.5

        # Scanner aggregates (EMA)
        self._meme_liq_score: float = 0.5
        self._meme_breadth_up_ratio: float = 0.5
        self._meme_pump_density: float = 0.0
        self._meme_rug_rate: float = 0.0

        # Smart/whale aggregates (EMA)
        self._scanner_smart_accum: float = 0.0
        self._scanner_smart_panic: float = 0.0
        self._scanner_whale_dom: float = 0.0
        self._scanner_smart_conflict: float = 0.0
        self._global_smart_risk: float = 0.0

        # Account state (normalized)
        self.account_daily_pnl_pct: float = 0.0
        self.account_daily_dd_pct: float = 0.0  # negative drawdown (e.g., -3.2)
        self.account_consec_losses: int = 0
        self.account_risk_bias: float = 0.0
        self.account_equity_slope: float = 0.0
        self.account_intraday_vol: float = 0.0
        self.account_kelly_hint: float = 0.0

        # Infra
        self.infra_degraded_score: float = 0.0
        self.infra_hard_block: bool = False

        # Combined
        self.macro_risk_score: float = 0.0
        self.regime_stress_score: float = 0.0
        self.regime_signal_strength: float = 0.0
        self.tradeability_score: float = 0.0

        # External overrides
        self._external_macro_bias: float = 0.0
        self._external_risk_off_flag: bool = False

        # Internal
        self._lock = asyncio.Lock()
        self._running_task: Optional[asyncio.Task] = None
        self._force_label_logged: bool = False
        self._last_debug_ts: float = 0.0

    # ────────────────────────────────────────────────────────────────────
    # Sync accessors (cheap)
    # ────────────────────────────────────────────────────────────────────
    def get(self) -> Regime:
        return self._regime

    def label(self) -> str:
        return self._regime

    def as_numeric(self) -> float:
        return {"risk_off": -1.0, "normal": 0.0, "risk_on": 1.0}[self._regime]

    def risk_mode(self) -> int:
        return {"risk_off": 0, "normal": 1, "risk_on": 2}[self._regime]

    def is_risk_on(self) -> bool:
        return self._regime == "risk_on"

    def is_risk_off(self) -> bool:
        return self._regime == "risk_off"

    def is_infra_blocked(self) -> bool:
        return bool(self.infra_hard_block)

    def is_warmup(self) -> bool:
        if not REGIME_ENABLED:
            return False
        if len(self._history) < 2:
            return True
        try:
            span = self._history[-1][0] - self._history[0][0]
        except Exception:
            return True
        return span < float(REGIME_MIN_OBS_SEC)

    def is_stale(self) -> bool:
        if not REGIME_ENABLED:
            return False
        if self._last_eval_ts <= 0:
            return False
        return (time.time() - self._last_eval_ts) >= float(max(10, REGIME_STALE_SEC))

    def is_hard_stop(self) -> bool:
        # account dd_pct stored negative
        if self.account_daily_dd_pct <= -abs(ACCOUNT_DD_HARD_STOP):
            return True
        if REGIME_PAUSE_ON_INFRA_HARD and self.infra_hard_block:
            return True
        return False

    def is_hard_risk_off(self) -> bool:
        if self._external_risk_off_flag:
            return True
        return self._regime == "risk_off" and self.regime_stress_score >= 0.6

    # ────────────────────────────────────────────────────────────────────
    # Risk knobs
    # ────────────────────────────────────────────────────────────────────
    def _base_risk_mult(self) -> float:
        if self._regime == "risk_off":
            return max(0.0, RISK_MULT_RISK_OFF)
        if self._regime == "risk_on":
            return max(0.0, RISK_MULT_RISK_ON)
        return max(0.0, RISK_MULT_NORMAL)

    def risk_multiplier(self) -> float:
        base = self._base_risk_mult()

        liq = _clamp(self.liquidity_score, 0.0, 1.0)
        manip = _clamp(self.manipulation_risk, 0.0, 1.0)
        macro_signal = liq - manip  # [-1, +1]-ish

        span = max(0.0, REGIME_DYNAMIC_MULT_SPAN)
        dynamic_mod = 1.0 + span * macro_signal

        account_bias = _clamp(self.account_risk_bias, -1.0, 1.0)
        account_mod = 1.0 + 0.30 * account_bias

        ext_bias = _clamp(self._external_macro_bias, -1.0, 1.0)
        external_mod = 1.0 + 0.20 * ext_bias

        raw = base * dynamic_mod * account_mod * external_mod

        # Session/weekend soft bias
        if REGIME_SESSION_BIAS_ENABLED or REGIME_WEAK_WEEKEND_MODE:
            now = time.time()
            if REGIME_SESSION_BIAS_ENABLED:
                session = _session_of_day(now)
                if session == "asia":
                    raw *= 0.90
                elif session == "us":
                    raw *= 1.05
            if REGIME_WEAK_WEEKEND_MODE:
                raw *= _weekend_factor(now)

        # Warmup/stale clamps (PnL-positive safety)
        if self.is_warmup():
            raw *= REGIME_WARMUP_RISK_CLAMP
        if self.is_stale():
            raw *= REGIME_STALE_RISK_CLAMP

        return _clamp(raw, RISK_MULT_MIN_CAP, RISK_MULT_MAX_CAP)

    def entry_tighten(self) -> float:
        stress = _clamp(self.regime_stress_score, 0.0, 1.0)
        signal = _clamp(self.regime_signal_strength, 0.0, 1.0)

        if self._external_risk_off_flag:
            val = 0.40
        elif self._regime == "risk_off":
            tighten_span = max(0.0, REGIME_ENTRY_TIGHTEN_MAX)
            val = _clamp(1.0 - tighten_span * stress, 0.4, 1.0)
        elif self._regime == "risk_on":
            loosen_span = max(0.0, REGIME_ENTRY_LOOSEN_MAX)
            val = _clamp(1.0 + loosen_span * signal, 1.0, 1.5)
        else:
            tighten_span = max(0.0, REGIME_ENTRY_TIGHTEN_MAX * 0.5)
            loosen_span = max(0.0, REGIME_ENTRY_LOOSEN_MAX * 0.5)
            val = _clamp(1.0 - tighten_span * stress + loosen_span * signal, 0.7, 1.2)

        # Warmup safety: never loosen too much early
        if self.is_warmup() and val > REGIME_WARMUP_ENTRY_TIGHTEN_CAP:
            val = REGIME_WARMUP_ENTRY_TIGHTEN_CAP

        return float(val)

    def entry_gate_factor(self) -> float:
        rm = float(self.risk_multiplier())
        tighten = float(self.entry_tighten())
        stress = _clamp(self.regime_stress_score, 0.0, 1.0)

        stress_penalty = 1.0 - 0.40 * stress
        raw = rm * tighten * stress_penalty
        return float(_clamp(raw, 0.2, 1.5))

    def slippage_hint_bps(self) -> int:
        vol = _clamp(self.volatility_score, 0.0, 1.0)
        manip = _clamp(self.manipulation_risk, 0.0, 1.0)
        liq = _clamp(self.liquidity_score, 0.0, 1.0)
        stress = _clamp(self.regime_stress_score, 0.0, 1.0)

        dirtiness = _clamp(
            0.35 * vol + 0.30 * manip + 0.20 * stress + 0.15 * (1.0 - liq),
            0.0,
            1.0,
        )
        lo = float(REGIME_SLIPPAGE_HINT_MIN_BPS)
        hi = float(REGIME_SLIPPAGE_HINT_MAX_BPS)
        return int(round(lo + (hi - lo) * dirtiness))

    def should_throttle_entries(self) -> bool:
        if self.is_hard_risk_off():
            return True
        if self.regime_stress_score >= 0.75:
            return True
        if self.macro_risk_score <= -0.5 and self.regime_signal_strength <= 0.4:
            return True
        if self.tradeability_score <= REGIME_TRADEABILITY_SOFT_MIN:
            return True
        if self.is_stale() or self.is_warmup():
            return True
        return False

    def should_pause_all_new_entries(self) -> bool:
        if self.is_hard_stop():
            return True
        if REGIME_PAUSE_ON_INFRA_HARD and self.infra_hard_block:
            return True
        if self.tradeability_score <= REGIME_TRADEABILITY_HARD_MIN:
            return True
        return False

    def risk_profile_hint(self) -> str:
        if self.regime_stress_score >= 0.6 or self.macro_risk_score <= -0.4:
            return "conservative"
        if self.macro_risk_score >= 0.6 and self.regime_stress_score <= 0.35:
            return "aggressive"
        return "balanced"

    # ────────────────────────────────────────────────────────────────────
    # Export ctx (backwards compatible)
    # ────────────────────────────────────────────────────────────────────
    def to_risk_ctx(self) -> Dict[str, Any]:
        rm = float(self.risk_multiplier())
        eg = float(self.entry_gate_factor())
        return {
            "regime_version": REGIME_VERSION,
            "regime_label": self.label(),
            "regime_label_numeric": self.as_numeric(),
            "regime_risk_mode": self.risk_mode(),
            "regime_risk_multiplier": rm,
            "regime_mult": rm,  # legacy alias
            "regime_entry_tighten": float(self.entry_tighten()),
            "regime_entry_gate_factor": eg,
            "regime_gate": eg,  # legacy alias
            "regime_slippage_hint_bps": float(self.slippage_hint_bps()),
            "regime_trend": float(self.trend_score),
            "regime_volatility": float(self.volatility_score),
            "regime_liquidity": float(self.liquidity_score),
            "regime_manipulation_risk": float(self.manipulation_risk),
            "manipulation_risk": float(self.manipulation_risk),  # important for ai_risk_model
            "regime_meme_heat": float(self.meme_heat),
            "regime_stress_score": float(self.regime_stress_score),
            "regime_signal_strength": float(self.regime_signal_strength),
            "tradeability_score": float(self.tradeability_score),
            "macro_risk_score": float(self.macro_risk_score),
            "infra_degraded_score": float(self.infra_degraded_score),
            "account_risk_bias": float(self.account_risk_bias),
            "account_daily_dd_pct": float(self.account_daily_dd_pct),
            "account_daily_pnl_pct": float(self.account_daily_pnl_pct),
            "account_equity_slope": float(self.account_equity_slope),
            "account_intraday_vol": float(self.account_intraday_vol),
            "account_kelly_hint": float(self.account_kelly_hint),
            "risk_profile_hint": self.risk_profile_hint(),
            "regime_should_throttle_entries": 1.0 if self.should_throttle_entries() else 0.0,
            "regime_should_pause_new_entries": 1.0 if self.should_pause_all_new_entries() else 0.0,
            "regime_is_risk_on": 1.0 if self.is_risk_on() else 0.0,
            "regime_is_risk_off": 1.0 if self.is_risk_off() else 0.0,
            "regime_is_stale": 1.0 if self.is_stale() else 0.0,
            "regime_is_warmup": 1.0 if self.is_warmup() else 0.0,
            "regime_hard_stop": 1.0 if self.is_hard_stop() else 0.0,
            "infra_hard_block": 1.0 if self.infra_hard_block else 0.0,
        }

    def to_entry_ctx(self) -> Dict[str, float]:
        rm = float(self.risk_multiplier())
        eg = float(self.entry_gate_factor())
        return {
            "regime_label_numeric": float(self.as_numeric()),
            "regime_risk_mode": float(self.risk_mode()),
            "regime_risk_multiplier": rm,
            "regime_mult": rm,  # legacy alias
            "regime_entry_tighten": float(self.entry_tighten()),
            "regime_entry_gate_factor": eg,
            "regime_stress_score": float(self.regime_stress_score),
            "regime_signal_strength": float(self.regime_signal_strength),
            "tradeability_score": float(self.tradeability_score),
            "macro_risk_score": float(self.macro_risk_score),
            "manipulation_risk": float(self.manipulation_risk),
            "regime_should_throttle_entries": 1.0 if self.should_throttle_entries() else 0.0,
            "regime_should_pause_new_entries": 1.0 if self.should_pause_all_new_entries() else 0.0,
            "regime_is_stale": 1.0 if self.is_stale() else 0.0,
            "regime_is_warmup": 1.0 if self.is_warmup() else 0.0,
            "infra_degraded_score": float(self.infra_degraded_score),
            "infra_hard_block": 1.0 if self.infra_hard_block else 0.0,
        }

    # ────────────────────────────────────────────────────────────────────
    # Internal: snapshot + compute (min lock time)
    # ────────────────────────────────────────────────────────────────────
    def _snapshot_unlocked(self) -> _RegimeSnap:
        return _RegimeSnap(
            regime=self._regime,
            last_change_ts=self._last_change_ts,
            last_eval_ts=self._last_eval_ts,
            last_px=self._last_px,
            history=tuple(self._history),
            consec_sol_fail=self._consec_sol_fail,
            trend_score=float(self.trend_score),
            volatility_score=float(self.volatility_score),
            liquidity_score=float(self.liquidity_score),
            manipulation_risk=float(self.manipulation_risk),
            choppy_score=float(self.choppy_score),
            confidence_score=float(self.confidence_score),
            meme_heat=float(self.meme_heat),
            meme_breadth_up_ratio=float(self._meme_breadth_up_ratio),
            meme_liq_score=float(self._meme_liq_score),
            meme_pump_density=float(self._meme_pump_density),
            meme_rug_rate=float(self._meme_rug_rate),
            smart_accum=float(self._scanner_smart_accum),
            smart_panic=float(self._scanner_smart_panic),
            whale_dom=float(self._scanner_whale_dom),
            smart_conflict=float(self._scanner_smart_conflict),
            smart_risk=float(self._global_smart_risk),
            account_daily_pnl_pct=float(self.account_daily_pnl_pct),
            account_daily_dd_pct=float(self.account_daily_dd_pct),
            account_consec_losses=int(self.account_consec_losses),
            account_risk_bias=float(self.account_risk_bias),
            account_equity_slope=float(self.account_equity_slope),
            account_intraday_vol=float(self.account_intraday_vol),
            account_kelly_hint=float(self.account_kelly_hint),
            infra_degraded_score=float(self.infra_degraded_score),
            infra_hard_block=bool(self.infra_hard_block),
            external_macro_bias=float(self._external_macro_bias),
            external_risk_off_flag=bool(self._external_risk_off_flag),
        )

    def _decide_regime(self, *, ret_5m: float, ret_1h: float, vol_5m: float) -> Regime:
        # Manual override
        if REGIME_FORCE_LABEL:
            if not self._force_label_logged:
                self._force_label_logged = True
                if hasattr(log, "event"):
                    log.event("REGIME_FORCE_LABEL", label=REGIME_FORCE_LABEL, version=REGIME_VERSION)
                else:
                    log.warning("REGIME_FORCE_LABEL=%s (override)", REGIME_FORCE_LABEL)
            return REGIME_FORCE_LABEL  # type: ignore[return-value]

        # External hard risk_off
        if self._external_risk_off_flag:
            return "risk_off"

        # Infra hard block
        if REGIME_PAUSE_ON_INFRA_HARD and self.infra_hard_block:
            return "risk_off"

        # Account hard stop
        if self.account_daily_dd_pct <= -abs(ACCOUNT_DD_HARD_STOP):
            return "risk_off"

        # Hard macro dump
        if ret_1h <= SOL_RISK_OFF_RET_1H or ret_5m <= SOL_RISK_OFF_RET_5M:
            return "risk_off"

        # Manipulation
        if self.manipulation_risk >= REGIME_MANIP_RISK_OFF_THRESH:
            return "risk_off"

        # High vol + negative trend
        if ret_1h < 0.0 and vol_5m >= REGIME_VOL_5M_HIGH_PCT:
            return "risk_off"

        # Softer DD / streak
        if (self.account_daily_dd_pct <= -abs(ACCOUNT_DD_RISK_OFF)) or (
            self.account_consec_losses >= ACCOUNT_CONSEC_LOSS_RISK_OFF
        ):
            return "risk_off"

        # risk_on: aligned upside + ok vol + non-awful meme tape + not in DD
        breadth = self._meme_breadth_up_ratio
        liq_score = self._meme_liq_score
        acc_ok = self.account_daily_dd_pct > -abs(ACCOUNT_DD_RISK_OFF)

        if (
            ret_1h >= SOL_RISK_ON_RET_1H
            and ret_5m >= SOL_RISK_ON_RET_5M
            and vol_5m <= REGIME_VOL_5M_HIGH_PCT * 1.5
            and breadth >= MEME_BREADTH_BEARISH
            and liq_score >= 0.30
            and acc_ok
        ):
            return "risk_on"

        return "normal"

    def _is_strong_switch(self, *, current: Regime, target: Regime, ret_5m: float, ret_1h: float) -> bool:
        # Always allow into risk_off if nuking
        if target == "risk_off":
            return (
                ret_1h <= SOL_RISK_OFF_RET_1H * 1.5
                or ret_5m <= SOL_RISK_OFF_RET_5M * 2.0
                or self.account_daily_dd_pct <= -abs(ACCOUNT_DD_HARD_STOP)
                or (REGIME_PAUSE_ON_INFRA_HARD and self.infra_hard_block)
            )

        if target == "risk_on" and current != "risk_on":
            return (
                ret_1h >= SOL_RISK_ON_RET_1H * 1.5
                and ret_5m >= SOL_RISK_ON_RET_5M * 1.5
                and self.account_daily_dd_pct > -abs(ACCOUNT_DD_RISK_OFF)
            )

        return False

    def _recompute_scores_unlocked(self, *, ret_5m: float, ret_1h: float, vol_5m: float) -> None:
        """With lock held: recompute continuous regime scores and smooth them."""

        # Trend (-1..+1): scale by target bands
        pos_5m = max(SOL_RISK_ON_RET_5M, 1e-6)
        pos_1h = max(SOL_RISK_ON_RET_1H, 1e-6)
        neg_5m = min(SOL_RISK_OFF_RET_5M, -1e-6)
        neg_1h = min(SOL_RISK_OFF_RET_1H, -1e-6)

        if ret_5m >= 0:
            t5 = _clamp(ret_5m / pos_5m, 0.0, 1.5)
        else:
            t5 = -_clamp(ret_5m / neg_5m, 0.0, 1.5)

        if ret_1h >= 0:
            t1 = _clamp(ret_1h / pos_1h, 0.0, 1.5)
        else:
            t1 = -_clamp(ret_1h / neg_1h, 0.0, 1.5)

        trend_raw = _clamp((t5 + t1) / 2.0, -1.0, 1.0)
        self.trend_score = _ema(self.trend_score, trend_raw, REGIME_SCORE_EMA_ALPHA)

        # Volatility (0..1)
        vol_raw = 0.0 if vol_5m <= 0 else _norm_between(vol_5m, 0.0, REGIME_VOL_5M_HIGH_PCT * 2.0)
        self.volatility_score = _ema(self.volatility_score, vol_raw, REGIME_SCORE_EMA_ALPHA)

        # Liquidity (0..1): scanner liq plus slight trend boost
        base_liq = _clamp(self._meme_liq_score, 0.0, 1.0)
        liq_raw = _clamp(base_liq + 0.2 * self.trend_score, 0.0, 1.0)
        self.liquidity_score = _ema(self.liquidity_score, liq_raw, REGIME_SCORE_EMA_ALPHA)

        # Choppy: high vol with neutral trend
        neutral_trend = 1.0 - abs(self.trend_score)
        choppy_raw = _clamp(self.volatility_score * neutral_trend, 0.0, 1.0)
        self.choppy_score = _ema(self.choppy_score, choppy_raw, REGIME_SCORE_EMA_ALPHA)

        # Manipulation risk (0..1)
        pump_term = _clamp(self._meme_pump_density / max(MEME_PUMP_DENSITY_HI, 1e-6), 0.0, 2.0)
        rug_term = _clamp(self._meme_rug_rate / max(MEME_RUG_RATE_HI, 1e-6), 0.0, 2.0)
        vol_term = _clamp(self.volatility_score * neutral_trend, 0.0, 1.0)

        smart_conflict_term = _clamp(self._scanner_smart_conflict, 0.0, 1.0)
        whale_dom_term = _clamp(self._scanner_whale_dom, 0.0, 1.0)

        manip_raw = _clamp(
            0.30 * vol_term
            + 0.25 * pump_term
            + 0.20 * rug_term
            + 0.15 * smart_conflict_term
            + 0.10 * whale_dom_term,
            0.0,
            1.0,
        )
        self.manipulation_risk = _ema(self.manipulation_risk, manip_raw, REGIME_SCORE_EMA_ALPHA)

        # Meme heat (0..1)
        breadth = _clamp(self._meme_breadth_up_ratio, 0.0, 1.0)
        breadth_pos = _norm_between(breadth, 0.50, MEME_BREADTH_BULLISH)
        rug_penalty = 1.0 - _clamp(self._meme_rug_rate / max(MEME_RUG_RATE_HI, 1e-6), 0.0, 1.5)

        smart_acc = _clamp(self._scanner_smart_accum, 0.0, 1.0)
        smart_panic = _clamp(self._scanner_smart_panic, 0.0, 1.0)

        meme_heat_raw = _clamp(
            0.40 * breadth_pos
            + 0.30 * self.liquidity_score
            + 0.15 * (1.0 - smart_panic)
            + 0.10 * smart_acc
            + 0.05 * rug_penalty,
            0.0,
            1.0,
        )
        self.meme_heat = _ema(self.meme_heat, meme_heat_raw, REGIME_SCORE_EMA_ALPHA)

        # Confidence (0..1): clear trend + low choppy + low manipulation
        conf_raw = _clamp(
            0.40 * (1.0 - self.choppy_score)
            + 0.35 * abs(self.trend_score)
            + 0.25 * (1.0 - self.manipulation_risk),
            0.0,
            1.0,
        )
        self.confidence_score = _ema(self.confidence_score, conf_raw, REGIME_SCORE_EMA_ALPHA)

        # Account risk bias (-1..+1)
        dd = float(self.account_daily_dd_pct)  # negative
        pnl = float(self.account_daily_pnl_pct)
        losses = int(self.account_consec_losses)
        eq_slope = float(self.account_equity_slope)
        eq_vol = float(self.account_intraday_vol)
        kelly_hint = _clamp(float(self.account_kelly_hint), 0.0, 1.0)

        dd_bias = 0.0
        if dd < 0.0:
            dd_bias = -_clamp(abs(dd) / max(abs(ACCOUNT_DD_HARD_STOP), 1e-6), 0.0, 1.0)

        pnl_bias = 0.0
        if pnl > 0.0:
            pnl_bias = _clamp(pnl / max(ACCOUNT_PNL_RISK_ON, 1e-6), 0.0, 1.0)

        loss_bias = 0.0
        if losses >= ACCOUNT_CONSEC_LOSS_RISK_OFF:
            loss_bias = -_clamp((losses - ACCOUNT_CONSEC_LOSS_RISK_OFF + 1) / 3.0, 0.0, 1.0)

        slope_bias = 0.0
        if eq_slope > 0.0:
            slope_bias = _clamp(eq_slope / 4.0, 0.0, 1.0)
        elif eq_slope < 0.0:
            slope_bias = -_clamp(abs(eq_slope) / 4.0, 0.0, 1.0)

        vol_penalty = 0.0
        if eq_vol > 0.0:
            vol_penalty = _clamp(eq_vol / 5.0, 0.0, 1.0)

        acc_bias_raw = _clamp(
            0.45 * dd_bias + 0.20 * loss_bias + 0.20 * pnl_bias + 0.10 * slope_bias - 0.05 * vol_penalty,
            -1.0,
            1.0,
        )
        self.account_risk_bias = _ema(self.account_risk_bias, acc_bias_raw, REGIME_SCORE_EMA_ALPHA)

        # Macro risk (-1..+1)
        macro_core = (
            0.35 * self.trend_score
            - 0.20 * self.volatility_score
            - 0.20 * self.manipulation_risk
            + 0.15 * (self.meme_heat - 0.5) * 2.0
        )

        # External + Kelly hint (gentle)
        macro_core += 0.20 * self._external_macro_bias
        macro_core += 0.05 * ((kelly_hint - 0.5) * 2.0)

        # Smart-risk: exposed for AI; rule-based impact kept mild
        macro_core += 0.03 * ((smart_acc - smart_panic) * 2.0)
        macro_core -= 0.02 * self._global_smart_risk

        macro_acc = 0.40 * self.account_risk_bias
        macro_raw = (1.0 - REGIME_ACCOUNT_INFLUENCE) * macro_core + REGIME_ACCOUNT_INFLUENCE * (macro_core + macro_acc)
        self.macro_risk_score = _ema(self.macro_risk_score, _clamp(macro_raw, -1.0, 1.0), REGIME_SCORE_EMA_ALPHA)

        # Stress (0..1)
        stress_dd = _clamp(abs(dd) / max(abs(ACCOUNT_DD_HARD_STOP), 1e-6), 0.0, 1.0)
        stress_losses = _clamp(losses / max(ACCOUNT_CONSEC_LOSS_RISK_OFF, 1), 0.0, 1.0)

        infra = _clamp(self.infra_degraded_score, 0.0, 1.0)
        infra_term = REGIME_INFRA_INFLUENCE * infra

        stress_raw = _clamp(
            0.30 * self.volatility_score
            + 0.25 * self.manipulation_risk
            + 0.20 * stress_dd
            + 0.10 * stress_losses
            + 0.15 * infra_term,
            0.0,
            1.0,
        )
        self.regime_stress_score = _ema(self.regime_stress_score, stress_raw, REGIME_SCORE_EMA_ALPHA)

        # Signal strength (0..1)
        sig_raw = _clamp(
            0.40 * abs(self.trend_score) + 0.40 * self.confidence_score + 0.20 * (1.0 - self.choppy_score),
            0.0,
            1.0,
        )
        self.regime_signal_strength = _ema(self.regime_signal_strength, sig_raw, REGIME_SCORE_EMA_ALPHA)

        # Tradeability (0..1)
        trade_raw = _clamp(
            0.40 * self.regime_signal_strength
            + 0.25 * self.meme_heat
            + 0.20 * (1.0 - self.regime_stress_score)
            + 0.15 * max(0.0, self.account_risk_bias),
            0.0,
            1.0,
        )
        self.tradeability_score = _ema(self.tradeability_score, trade_raw, REGIME_SCORE_EMA_ALPHA)

    def _apply_regime_unlocked(self, new_regime: Regime, ret_5m: float, ret_1h: float, vol_5m: float) -> None:
        self._recompute_scores_unlocked(ret_5m=ret_5m, ret_1h=ret_1h, vol_5m=vol_5m)

        if new_regime == self._regime:
            return

        now = time.time()

        # Confidence gates: require min confidence/signal unless strong switch
        if not self._is_strong_switch(current=self._regime, target=new_regime, ret_5m=ret_5m, ret_1h=ret_1h):
            if self.confidence_score < REGIME_SWITCH_MIN_CONF or self.regime_signal_strength < REGIME_SWITCH_MIN_SIG:
                return

        # Hysteresis time gate
        if (now - self._last_change_ts) < float(REGIME_SMOOTH_SEC):
            if not self._is_strong_switch(current=self._regime, target=new_regime, ret_5m=ret_5m, ret_1h=ret_1h):
                return

        old = self._regime
        self._regime = new_regime
        self._last_change_ts = now

        if hasattr(log, "event"):
            log.event(
                "REGIME_SWITCH",
                version=REGIME_VERSION,
                old=old,
                new=new_regime,
                ret_5m=ret_5m,
                ret_1h=ret_1h,
                vol_5m=vol_5m,
                confidence=self.confidence_score,
                sig_strength=self.regime_signal_strength,
                account_dd=self.account_daily_dd_pct,
                account_pnl=self.account_daily_pnl_pct,
                account_bias=self.account_risk_bias,
                infra_deg=self.infra_degraded_score,
                infra_block=self.infra_hard_block,
            )
        else:
            log.info(
                "REGIME_SWITCH v=%s old=%s new=%s ret_5m=%.3f ret_1h=%.3f vol=%.3f conf=%.2f sig=%.2f",
                REGIME_VERSION, old, new_regime, ret_5m, ret_1h, vol_5m, self.confidence_score, self.regime_signal_strength
            )

    # ────────────────────────────────────────────────────────────────────
    # Public: feature payloads (async, safe)
    # ────────────────────────────────────────────────────────────────────
    async def features(self) -> Dict[str, float]:
        async with self._lock:
            snap = self._snapshot_unlocked()

        now = time.time()
        last_change_ago = max(0.0, now - snap.last_change_ts)
        last_eval_ago = max(0.0, now - snap.last_eval_ts) if snap.last_eval_ts > 0 else 1e9

        numeric = {"risk_off": -1.0, "normal": 0.0, "risk_on": 1.0}[snap.regime]
        session = _session_of_day(now)
        week_factor = _weekend_factor(now)

        hist = list(snap.history)
        ret_5m, ret_1h, vol_5m = _calc_sol_stats(hist)

        payload: Dict[str, float] = {
            # Keep numeric-only
            "regime_label_numeric": float(numeric),
            "regime_numeric": float(numeric),
            "regime_risk_mode": float({"risk_off": 0, "normal": 1, "risk_on": 2}[snap.regime]),
            "regime_risk_multiplier": float(self.risk_multiplier()),
            "regime_mult": float(self.risk_multiplier()),  # legacy
            "regime_entry_tighten": float(self.entry_tighten()),
            "regime_entry_gate_factor": float(self.entry_gate_factor()),
            "regime_slippage_hint_bps": float(self.slippage_hint_bps()),
            "regime_is_risk_on": 1.0 if snap.regime == "risk_on" else 0.0,
            "regime_is_risk_off": 1.0 if snap.regime == "risk_off" else 0.0,
            "regime_last_change_sec_ago": float(last_change_ago),
            "regime_last_eval_sec_ago": float(last_eval_ago),
            "regime_is_stale": 1.0 if self.is_stale() else 0.0,
            "regime_is_warmup": 1.0 if self.is_warmup() else 0.0,
            "regime_hard_stop": 1.0 if self.is_hard_stop() else 0.0,
            "trend_score": float(snap.trend_score),
            "volatility_score": float(snap.volatility_score),
            "liquidity_score": float(snap.liquidity_score),
            "manipulation_risk": float(snap.manipulation_risk),
            "regime_choppy_score": float(snap.choppy_score),
            "regime_confidence": float(snap.confidence_score),
            "regime_meme_heat": float(snap.meme_heat),
            "macro_risk_score": float(self.macro_risk_score),
            "regime_stress_score": float(self.regime_stress_score),
            "regime_signal_strength": float(self.regime_signal_strength),
            "tradeability_score": float(self.tradeability_score),
            "meme_breadth_up_ratio": float(snap.meme_breadth_up_ratio),
            "meme_liq_score": float(snap.meme_liq_score),
            "meme_pump_density": float(snap.meme_pump_density),
            "meme_rug_rate": float(snap.meme_rug_rate),
            "regime_smart_accum_avg": float(snap.smart_accum),
            "regime_smart_panic_avg": float(snap.smart_panic),
            "regime_whale_dom_avg": float(snap.whale_dom),
            "regime_smart_conflict_avg": float(snap.smart_conflict),
            "regime_smart_risk_avg": float(snap.smart_risk),
            "account_daily_pnl_pct": float(snap.account_daily_pnl_pct),
            "account_daily_dd_pct": float(snap.account_daily_dd_pct),
            "account_consec_losses": float(snap.account_consec_losses),
            "account_risk_bias": float(snap.account_risk_bias),
            "account_equity_slope": float(snap.account_equity_slope),
            "account_intraday_vol": float(snap.account_intraday_vol),
            "account_kelly_hint": float(snap.account_kelly_hint),
            "regime_session_is_asia": 1.0 if session == "asia" else 0.0,
            "regime_session_is_eu": 1.0 if session == "eu" else 0.0,
            "regime_session_is_us": 1.0 if session == "us" else 0.0,
            "regime_weekend_factor": float(week_factor),
            "regime_external_macro_bias": float(snap.external_macro_bias),
            "regime_external_risk_off_flag": 1.0 if snap.external_risk_off_flag else 0.0,
            "infra_degraded_score": float(snap.infra_degraded_score),
            "infra_hard_block": 1.0 if snap.infra_hard_block else 0.0,
            "regime_is_choppy": 1.0 if snap.choppy_score >= 0.6 else 0.0,
            "regime_is_high_manip": 1.0 if snap.manipulation_risk >= 0.6 else 0.0,
            "regime_is_high_vol": 1.0 if snap.volatility_score >= 0.7 else 0.0,
            "regime_is_hot_meme": 1.0 if snap.meme_heat >= 0.65 else 0.0,
            "regime_should_throttle_entries": 1.0 if self.should_throttle_entries() else 0.0,
            "regime_should_pause_new_entries": 1.0 if self.should_pause_all_new_entries() else 0.0,
            "sol_last_px": float(snap.last_px),
            "sol_ret_5m_pct": float(ret_5m),
            "sol_ret_1h_pct": float(ret_1h),
            "sol_vol_win_pct": float(vol_5m),
            "regime_consec_sol_fail": float(snap.consec_sol_fail),
        }
        return payload

    async def snapshot_features(self) -> Dict[str, float]:
        return await self.features()

    async def attach_features_to_meta(self, meta: Dict[str, Any]) -> None:
        meta.update(await self.features())

    # ────────────────────────────────────────────────────────────────────
    # External / AI hints
    # ────────────────────────────────────────────────────────────────────
    async def apply_external_macro_hint(
        self,
        *,
        macro_bias: Optional[float] = None,
        force_risk_off: Optional[bool] = None,
    ) -> None:
        async with self._lock:
            if macro_bias is not None:
                self._external_macro_bias = _clamp(float(macro_bias), -1.0, 1.0)
            if force_risk_off is not None:
                self._external_risk_off_flag = bool(force_risk_off)

    async def apply_ai_macro_hint(
        self,
        *,
        macro_bias: Optional[float] = None,
        force_risk_off: Optional[bool] = None,
    ) -> None:
        await self.apply_external_macro_hint(macro_bias=macro_bias, force_risk_off=force_risk_off)

    # ────────────────────────────────────────────────────────────────────
    # Macro loop (compute-heavy outside lock)
    # ────────────────────────────────────────────────────────────────────
    async def refresh_once(self) -> None:
        if not REGIME_ENABLED:
            return

        px: float = 0.0
        try:
            if REGIME_SOL_FETCH_TIMEOUT_S > 0:
                px = float(await asyncio.wait_for(_call_maybe_async(get_sol_usd), timeout=REGIME_SOL_FETCH_TIMEOUT_S))
            else:
                px = float(await _call_maybe_async(get_sol_usd))
        except Exception:
            px = 0.0

        now = time.time()
        if px <= 0.0:
            async with self._lock:
                self._consec_sol_fail += 1
                # Let stale clamp handle risk; optionally log
                if self._consec_sol_fail >= REGIME_SOL_MAX_CONSEC_FAIL and REGIME_DEBUG_LOG:
                    if hasattr(log, "event"):
                        log.event("REGIME_SOL_FETCH_FAIL", consec=self._consec_sol_fail)
                    else:
                        log.warning("REGIME_SOL_FETCH_FAIL consec=%s (px=0)", self._consec_sol_fail)
            return

        # Update history under lock; copy history for compute outside lock
        async with self._lock:
            self._consec_sol_fail = 0
            self._last_px = px
            self._history.append((now, px))

            cutoff = now - float(REGIME_HISTORY_HORIZON_SEC)
            while len(self._history) > 2 and self._history[0][0] < cutoff:
                self._history.pop(0)
            if REGIME_HISTORY_MAX_POINTS > 0 and len(self._history) > REGIME_HISTORY_MAX_POINTS:
                self._history = self._history[-REGIME_HISTORY_MAX_POINTS :]

            hist_copy = list(self._history)

        # Compute stats outside lock
        ret_5m, ret_1h, vol_5m = _calc_sol_stats(hist_copy)

        # Apply regime + scores under lock
        async with self._lock:
            # Always recompute scores (even during warmup); safety clamps still apply in risk_multiplier/entry_tighten
            new_regime = self._decide_regime(ret_5m=ret_5m, ret_1h=ret_1h, vol_5m=vol_5m)
            self._apply_regime_unlocked(new_regime, ret_5m, ret_1h, vol_5m)
            self._last_eval_ts = now

            if REGIME_DEBUG_LOG and (now - self._last_debug_ts) >= float(REGIME_DEBUG_EVERY_SEC):
                self._last_debug_ts = now
                if hasattr(log, "event"):
                    log.event(
                        "REGIME_STEP",
                        px=px,
                        ret_5m=ret_5m,
                        ret_1h=ret_1h,
                        vol=vol_5m,
                        trend=self.trend_score,
                        vol_score=self.volatility_score,
                        liq=self.liquidity_score,
                        manip=self.manipulation_risk,
                        conf=self.confidence_score,
                        label=self._regime,
                        tradeable=self.tradeability_score,
                        warmup=self.is_warmup(),
                        stale=self.is_stale(),
                    )
                else:
                    log.debug(
                        "REGIME_STEP px=%.4f ret_5m=%.3f ret_1h=%.3f vol=%.3f trend=%.2f volS=%.2f "
                        "liq=%.2f manip=%.2f conf=%.2f label=%s tradeable=%.2f warmup=%s stale=%s",
                        px, ret_5m, ret_1h, vol_5m,
                        self.trend_score, self.volatility_score, self.liquidity_score,
                        self.manipulation_risk, self.confidence_score, self._regime, self.tradeability_score,
                        int(self.is_warmup()), int(self.is_stale())
                    )

    async def refresh_loop(self) -> None:
        if not REGIME_ENABLED:
            if hasattr(log, "event"):
                log.event("REGIME_LOOP_DISABLED")
            else:
                log.info("REGIME_LOOP_DISABLED")
            return

        if hasattr(log, "event"):
            log.event("REGIME_LOOP_START", poll_sec=REGIME_POLL_SEC, hist_sec=REGIME_HISTORY_HORIZON_SEC, version=REGIME_VERSION)
        else:
            log.info("REGIME_LOOP_START poll_sec=%s hist_sec=%s v=%s", REGIME_POLL_SEC, REGIME_HISTORY_HORIZON_SEC, REGIME_VERSION)

        while True:
            try:
                await self.refresh_once()
            except Exception as e:
                if hasattr(log, "event"):
                    log.event("REGIME_LOOP_ERROR", error=str(e)[:200])
                else:
                    log.exception("REGIME_LOOP_ERROR: %s", e)
            await asyncio.sleep(max(5, REGIME_POLL_SEC))

    def ensure_background_task(self) -> None:
        """Call once during startup (inside a running event loop)."""
        if not REGIME_ENABLED:
            return
        if self._running_task is None or self._running_task.done():
            try:
                loop = asyncio.get_running_loop()
            except RuntimeError:
                return
            self._running_task = loop.create_task(self.refresh_loop())

    # ────────────────────────────────────────────────────────────────────
    # Scanner integration
    # ────────────────────────────────────────────────────────────────────
    async def update_from_scanner_batch(
        self,
        *,
        source: str,
        total_tokens: int,
        up_tokens: int,
        down_tokens: int,
        avg_liq_usd: float,
        pump_density: float = 0.0,
        rug_rate: float = 0.0,
        smart_accum_avg: float = 0.0,
        smart_panic_avg: float = 0.0,
        whale_dom_avg: float = 0.0,
        smart_conflict_avg: float = 0.0,
    ) -> None:
        if int(total_tokens) <= 0:
            return

        total = max(1, int(total_tokens))
        up_ratio = _clamp(int(up_tokens) / float(total), 0.0, 1.0)
        liq_score = _norm_between(_safe_float(avg_liq_usd, 0.0), MEME_LOW_LIQ_USD, MEME_HIGH_LIQ_USD)

        pump_d = _clamp(_safe_float(pump_density), 0.0, 1.0)
        rug_r = _clamp(_safe_float(rug_rate), 0.0, 1.0)

        s_acc = _clamp(_safe_float(smart_accum_avg), 0.0, 1.0)
        s_panic = _clamp(_safe_float(smart_panic_avg), 0.0, 1.0)
        w_dom = _clamp(_safe_float(whale_dom_avg), 0.0, 1.0)
        s_conf = _clamp(_safe_float(smart_conflict_avg), 0.0, 1.0)

        async with self._lock:
            a = REGIME_SCANNER_EMA_ALPHA
            self._meme_breadth_up_ratio = _ema(self._meme_breadth_up_ratio, up_ratio, a)
            self._meme_liq_score = _ema(self._meme_liq_score, liq_score, a)
            self._meme_pump_density = _ema(self._meme_pump_density, pump_d, a)
            self._meme_rug_rate = _ema(self._meme_rug_rate, rug_r, a)

            self._scanner_smart_accum = _ema(self._scanner_smart_accum, s_acc, a)
            self._scanner_smart_panic = _ema(self._scanner_smart_panic, s_panic, a)
            self._scanner_whale_dom = _ema(self._scanner_whale_dom, w_dom, a)
            self._scanner_smart_conflict = _ema(self._scanner_smart_conflict, s_conf, a)

            hist_copy = list(self._history)

        ret_5m, ret_1h, vol_5m = _calc_sol_stats(hist_copy)

        async with self._lock:
            self._recompute_scores_unlocked(ret_5m=ret_5m, ret_1h=ret_1h, vol_5m=vol_5m)
            self._last_eval_ts = time.time()

        if REGIME_DEBUG_LOG and hasattr(log, "event"):
            log.event(
                "REGIME_SCANNER_UPDATE",
                source=source,
                up_ratio=float(up_ratio),
                liq_score=float(liq_score),
                pump=float(pump_d),
                rug=float(rug_r),
                smart_acc=float(s_acc),
                smart_panic=float(s_panic),
                whale_dom=float(w_dom),
                smart_conflict=float(s_conf),
            )

    async def update_from_smart_wallets_global(
        self,
        *,
        smart_heat_global: float,
        whale_heat_global: float,
        smart_side_global: float,
        whale_side_global: float,
        smart_panic_global: float,
        smart_conflict_global: float,
        smart_risk_global: float,
    ) -> None:
        a = REGIME_SCANNER_EMA_ALPHA
        accum = max(0.0, _safe_float(smart_side_global)) * max(0.0, _safe_float(smart_heat_global))
        panic = _clamp(_safe_float(smart_panic_global), 0.0, 1.0)
        w_dom = _clamp(_safe_float(whale_heat_global), 0.0, 1.0)
        conflict = _clamp(_safe_float(smart_conflict_global), 0.0, 1.0)
        s_risk = _clamp(_safe_float(smart_risk_global), 0.0, 1.0)

        async with self._lock:
            self._scanner_smart_accum = _ema(self._scanner_smart_accum, accum, a)
            self._scanner_smart_panic = _ema(self._scanner_smart_panic, panic, a)
            self._scanner_whale_dom = _ema(self._scanner_whale_dom, w_dom, a)
            self._scanner_smart_conflict = _ema(self._scanner_smart_conflict, conflict, a)
            self._global_smart_risk = _ema(self._global_smart_risk, s_risk, a)
            hist_copy = list(self._history)

        ret_5m, ret_1h, vol_5m = _calc_sol_stats(hist_copy)

        async with self._lock:
            self._recompute_scores_unlocked(ret_5m=ret_5m, ret_1h=ret_1h, vol_5m=vol_5m)
            self._last_eval_ts = time.time()

    # ────────────────────────────────────────────────────────────────────
    # Infra integration
    # ────────────────────────────────────────────────────────────────────
    async def update_from_infra(self, *, infra_degraded_score: float, infra_hard_block: bool) -> None:
        async with self._lock:
            self.infra_degraded_score = _clamp(float(infra_degraded_score), 0.0, 1.0)
            self.infra_hard_block = bool(infra_hard_block)
            hist_copy = list(self._history)

        ret_5m, ret_1h, vol_5m = _calc_sol_stats(hist_copy)

        async with self._lock:
            self._recompute_scores_unlocked(ret_5m=ret_5m, ret_1h=ret_1h, vol_5m=vol_5m)
            self._last_eval_ts = time.time()

    # ────────────────────────────────────────────────────────────────────
    # Account integration
    # ────────────────────────────────────────────────────────────────────
    async def update_from_account(
        self,
        *,
        daily_realized_pnl_pct: float,
        daily_drawdown_pct: float,
        consecutive_losses: int,
        equity_curve_slope: Optional[float] = None,
        equity_intraday_vol: Optional[float] = None,
        kelly_fraction_hint: Optional[float] = None,
    ) -> None:
        """
        IMPORTANT: daily_drawdown_pct is normalized to NEGATIVE internally.
        If caller passes +3.2 meaning "3.2% drawdown", we convert to -3.2.
        """
        pnl = float(daily_realized_pnl_pct)
        dd_in = float(daily_drawdown_pct)
        dd = -abs(dd_in) if dd_in > 0 else float(dd_in)

        async with self._lock:
            self.account_daily_pnl_pct = pnl
            self.account_daily_dd_pct = dd
            self.account_consec_losses = max(0, int(consecutive_losses))

            if equity_curve_slope is not None:
                self.account_equity_slope = float(equity_curve_slope)
            if equity_intraday_vol is not None:
                self.account_intraday_vol = float(equity_intraday_vol)
            if kelly_fraction_hint is not None:
                self.account_kelly_hint = _clamp(float(kelly_fraction_hint), 0.0, 1.0)

            hist_copy = list(self._history)

        ret_5m, ret_1h, vol_5m = _calc_sol_stats(hist_copy)

        async with self._lock:
            self._recompute_scores_unlocked(ret_5m=ret_5m, ret_1h=ret_1h, vol_5m=vol_5m)
            self._last_eval_ts = time.time()

    # ────────────────────────────────────────────────────────────────────
    # Debug summary
    # ────────────────────────────────────────────────────────────────────
    def debug_summary(self) -> str:
        return (
            f"v={REGIME_VERSION} "
            f"regime={self._regime} "
            f"trend={self.trend_score:+.2f} "
            f"volS={self.volatility_score:.2f} "
            f"liq={self.liquidity_score:.2f} "
            f"manip={self.manipulation_risk:.2f} "
            f"meme_heat={self.meme_heat:.2f} "
            f"macro={self.macro_risk_score:+.2f} "
            f"stress={self.regime_stress_score:.2f} "
            f"sig={self.regime_signal_strength:.2f} "
            f"tradeable={self.tradeability_score:.2f} "
            f"acc_dd={self.account_daily_dd_pct:+.2f} "
            f"acc_pnl={self.account_daily_pnl_pct:+.2f} "
            f"infra_deg={self.infra_degraded_score:.2f} "
            f"risk_mult={self.risk_multiplier():.2f} "
            f"gate={self.entry_gate_factor():.2f} "
            f"slip_bps={self.slippage_hint_bps()} "
            f"throttle={int(self.should_throttle_entries())} "
            f"pause_new={int(self.should_pause_all_new_entries())} "
            f"warmup={int(self.is_warmup())} "
            f"stale={int(self.is_stale())} "
            f"hard_stop={int(self.is_hard_stop())}"
        )


# Global singleton used throughout the bot
regime_state = RegimeState()


# Convenience helpers (backwards-compatible)
def regime_risk_ctx() -> Dict[str, Any]:
    return regime_state.to_risk_ctx()


def regime_entry_ctx() -> Dict[str, float]:
    return regime_state.to_entry_ctx()


async def regime_features() -> Dict[str, float]:
    return await regime_state.features()


__all__ = [
    "Regime",
    "RegimeState",
    "regime_state",
    "regime_risk_ctx",
    "regime_entry_ctx",
    "regime_features",
    "REGIME_VERSION",
]
