# perps/alpha.py
"""
perps.alpha
===========

Perps *alpha* (signal) module — **NO execution**.

Overhaul contract:
- This module NEVER places orders.
- This module NEVER talks to exchanges/connectors directly.
- It only converts (features + funding) -> (signal + confidence + reasons).
- Must be safe to import anywhere (no side effects).

Backwards compatibility:
- Keeps `should_long(features, funding8h_sum)` and `should_short(...)`.

Feature contract (flexible):
- Expects a dict[str, float] with *some* of these keys (best-effort):
    ema12, ema26, ema50     (or ema_fast/ema_mid/ema_slow)
    rsi14                  (or rsi)
    atr_pct                (fraction, e.g. 0.015 = 1.5%)
    ret_1m, ret_5m, ret_15m (fraction returns)
    vol_burst              (>=1 means volume pickup)
    trend_regime           (0..1, >=0.5 trending)
    price                  (optional)
- Missing keys degrade gracefully.

Funding contract:
- funding8h_sum is a signed float (e.g. +0.01 = +1% over 8h).
  Positive funding penalizes LONGs; negative funding penalizes SHORTs.
"""

from __future__ import annotations

import logging
import math
import os
from dataclasses import dataclass
from enum import Enum


# -----------------------------------------------------------------------------
# Logging (safe fallback)
# -----------------------------------------------------------------------------
def _get_logger() -> logging.Logger:
    try:
        from utils_pkg.core_utils import logger as _lg  # type: ignore

        return _lg
    except Exception:
        lg = logging.getLogger("perps.alpha")
        if not lg.handlers:
            logging.basicConfig(level=logging.INFO)
        return lg


logger = _get_logger()


# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------
def _f(x: object, default: float = 0.0) -> float:
    try:
        v = float(x)  # type: ignore[arg-type]
        if math.isnan(v) or math.isinf(v):
            return float(default)
        return v
    except Exception:
        return float(default)


def _env_float(name: str, default: float) -> float:
    try:
        v = os.getenv(name, "")
        return float(v) if v not in ("", "None", None) else float(default)
    except Exception:
        return float(default)


def _env_int(name: str, default: int) -> int:
    try:
        v = os.getenv(name, "")
        return int(v) if v not in ("", "None", None) else int(default)
    except Exception:
        return int(default)


def _clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


def _sigmoid(x: float) -> float:
    # stable-ish sigmoid for confidence mapping
    if x >= 0:
        z = math.exp(-x)
        return 1.0 / (1.0 + z)
    z = math.exp(x)
    return z / (1.0 + z)


def _pick_ema(features: dict[str, float]) -> tuple[float, float, float]:
    """
    Prefer ema12/ema26/ema50, fallback ema_fast/ema_mid/ema_slow.
    """
    e1 = _f(features.get("ema12", features.get("ema_fast", 0.0)), 0.0)
    e2 = _f(features.get("ema26", features.get("ema_mid", 0.0)), 0.0)
    e3 = _f(features.get("ema50", features.get("ema_slow", 0.0)), 0.0)
    return e1, e2, e3


def _safe_div(a: float, b: float, eps: float = 1e-12) -> float:
    return a / (b if abs(b) > eps else (eps if b >= 0 else -eps))


# -----------------------------------------------------------------------------
# Public types
# -----------------------------------------------------------------------------
class Signal(str, Enum):
    LONG = "LONG"
    SHORT = "SHORT"
    FLAT = "FLAT"


@dataclass(frozen=True)
class PerpsAlphaConfig:
    """
    Default values are intentionally conservative.
    Tune via env vars prefixed with PERPS_.
    """

    # EMA trend structure
    ema_fast_key: str = "ema12"
    ema_mid_key: str = "ema26"
    ema_slow_key: str = "ema50"
    ema_stack_margin_bps: float = 5.0  # require ~0.05% separation to avoid noise

    # RSI gates
    rsi_key: str = "rsi14"
    rsi_long_min: float = 55.0
    rsi_short_max: float = 45.0

    # Funding guards
    funding_long_max: float = 0.03  # block longs if funding too positive
    funding_short_min: float = -0.03  # block shorts if funding too negative
    funding_penalty_scale: float = 12.0  # how strongly funding influences score

    # Volatility sanity
    atr_pct_key: str = "atr_pct"  # fraction (0.015 = 1.5%)
    atr_pct_min: float = 0.002  # too dead -> skip
    atr_pct_max: float = 0.08  # too hot -> skip

    # Momentum confirmation (optional)
    ret_1m_key: str = "ret_1m"
    ret_5m_key: str = "ret_5m"
    ret_15m_key: str = "ret_15m"
    momo_confirm_min: float = 0.0  # require >=0 for longs / <=0 for shorts (soft)

    # Micro confirmation (optional)
    vol_burst_key: str = "vol_burst"
    vol_burst_min: float = 1.0  # require at least neutral
    trend_regime_key: str = "trend_regime"
    trend_regime_min: float = 0.5

    # Decision thresholds (score in [-1, +1])
    long_score_thr: float = 0.35
    short_score_thr: float = -0.35
    min_confidence: float = 0.58  # below -> FLAT

    @staticmethod
    def from_env() -> PerpsAlphaConfig:
        """
        Load config from env with PERPS_ prefix.
        """
        return PerpsAlphaConfig(
            ema_stack_margin_bps=_env_float("PERPS_EMA_STACK_MARGIN_BPS", 5.0),
            rsi_long_min=_env_float("PERPS_RSI_LONG_MIN", 55.0),
            rsi_short_max=_env_float("PERPS_RSI_SHORT_MAX", 45.0),
            funding_long_max=_env_float("PERPS_FUNDING_LONG_MAX", 0.03),
            funding_short_min=_env_float("PERPS_FUNDING_SHORT_MIN", -0.03),
            funding_penalty_scale=_env_float("PERPS_FUNDING_PENALTY_SCALE", 12.0),
            atr_pct_min=_env_float("PERPS_ATR_PCT_MIN", 0.002),
            atr_pct_max=_env_float("PERPS_ATR_PCT_MAX", 0.08),
            momo_confirm_min=_env_float("PERPS_MOMO_CONFIRM_MIN", 0.0),
            vol_burst_min=_env_float("PERPS_VOL_BURST_MIN", 1.0),
            trend_regime_min=_env_float("PERPS_TREND_REGIME_MIN", 0.5),
            long_score_thr=_env_float("PERPS_LONG_SCORE_THR", 0.35),
            short_score_thr=_env_float("PERPS_SHORT_SCORE_THR", -0.35),
            min_confidence=_env_float("PERPS_MIN_CONFIDENCE", 0.58),
        )


@dataclass
class AlphaResult:
    signal: Signal
    score: float  # [-1, +1]
    confidence: float  # [0, 1]
    reasons: list[str]


# -----------------------------------------------------------------------------
# Core alpha logic
# -----------------------------------------------------------------------------
def score_alpha(
    features: dict[str, float],
    funding8h_sum: float,
    cfg: PerpsAlphaConfig | None = None,
) -> tuple[float, list[str]]:
    """
    Return (score in [-1,+1], reasons[]). Positive = long bias, negative = short bias.

    This is intentionally simple + deterministic:
    - Trend stack (EMA fast/mid/slow)
    - RSI gate & shaping
    - Funding penalty shaping
    - Optional momentum confirmations (ret_1m/5m/15m)
    - Volatility + liquidity-ish sanity (atr_pct + vol_burst)
    """
    cfg = cfg or PerpsAlphaConfig.from_env()
    reasons: list[str] = []

    # Extract core features
    ema_fast, ema_mid, ema_slow = _pick_ema(features)
    rsi = _f(features.get(cfg.rsi_key, features.get("rsi", 50.0)), 50.0)
    atr_pct = _f(features.get(cfg.atr_pct_key, 0.0), 0.0)
    vol_burst = _f(features.get(cfg.vol_burst_key, 1.0), 1.0)
    trend_regime = _f(features.get(cfg.trend_regime_key, 0.5), 0.5)

    # Sanity gates (fail-closed -> score 0)
    if atr_pct > 0:
        if atr_pct < cfg.atr_pct_min:
            return 0.0, [f"atr_pct too low {atr_pct:.4f} < {cfg.atr_pct_min:.4f}"]
        if atr_pct > cfg.atr_pct_max:
            return 0.0, [f"atr_pct too high {atr_pct:.4f} > {cfg.atr_pct_max:.4f}"]

    if vol_burst < cfg.vol_burst_min:
        return 0.0, [f"vol_burst {vol_burst:.2f} < {cfg.vol_burst_min:.2f}"]

    if trend_regime < cfg.trend_regime_min:
        return 0.0, [f"trend_regime {trend_regime:.2f} < {cfg.trend_regime_min:.2f}"]

    # Trend stack: require separation (avoid flat EMAs)
    # margin in bps relative to mid EMA
    if ema_mid <= 0 or ema_slow <= 0 or ema_fast <= 0:
        return 0.0, ["missing ema stack"]

    margin = cfg.ema_stack_margin_bps / 10_000.0
    long_stack = (ema_fast > ema_mid * (1 + margin)) and (ema_mid > ema_slow * (1 + margin))
    short_stack = (ema_fast < ema_mid * (1 - margin)) and (ema_mid < ema_slow * (1 - margin))

    # Base trend score
    score = 0.0
    if long_stack:
        score += 0.55
        reasons.append("ema stack bullish")
    elif short_stack:
        score -= 0.55
        reasons.append("ema stack bearish")
    else:
        # If no clear stack, don't force trades
        return 0.0, ["no clear ema stack"]

    # RSI shaping (soft, but directional)
    # Map RSI to [-1, +1] around 50 with saturation
    rsi_centered = (rsi - 50.0) / 25.0
    rsi_term = _clamp(rsi_centered, -1.0, 1.0) * 0.25
    score += rsi_term

    # Hard RSI gates (keep compatibility with your original intention)
    if score > 0 and rsi < cfg.rsi_long_min:
        return 0.0, [f"rsi {rsi:.1f} < long_min {cfg.rsi_long_min:.1f}"]
    if score < 0 and rsi > cfg.rsi_short_max:
        return 0.0, [f"rsi {rsi:.1f} > short_max {cfg.rsi_short_max:.1f}"]

    # Momentum confirmations (optional)
    ret_1m = _f(features.get(cfg.ret_1m_key, 0.0), 0.0)
    ret_5m = _f(features.get(cfg.ret_5m_key, 0.0), 0.0)
    ret_15m = _f(features.get(cfg.ret_15m_key, 0.0), 0.0)

    momo = 0.0
    momo += _clamp(ret_1m * 50.0, -0.2, 0.2)
    momo += _clamp(ret_5m * 30.0, -0.2, 0.2)
    momo += _clamp(ret_15m * 20.0, -0.2, 0.2)
    score += momo

    if score > 0 and (ret_1m < cfg.momo_confirm_min and ret_5m < cfg.momo_confirm_min):
        # Soft penalty (don’t force-accept longs in a micro dump)
        score -= 0.15
        reasons.append("weak momo vs long")
    if score < 0 and (ret_1m > -cfg.momo_confirm_min and ret_5m > -cfg.momo_confirm_min):
        score += 0.15
        reasons.append("weak momo vs short")

    # Funding shaping + guards (your original: funding not too positive for longs, etc.)
    f8 = _f(funding8h_sum, 0.0)

    if score > 0 and f8 >= cfg.funding_long_max:
        return 0.0, [f"funding too positive for long {f8:.4f} >= {cfg.funding_long_max:.4f}"]
    if score < 0 and f8 <= cfg.funding_short_min:
        return 0.0, [f"funding too negative for short {f8:.4f} <= {cfg.funding_short_min:.4f}"]

    # Penalize score in the "wrong" direction:
    # - positive funding penalizes longs
    # - negative funding penalizes shorts
    # Use tanh for bounded effect.
    penalty = math.tanh(abs(f8) * cfg.funding_penalty_scale) * 0.20
    if score > 0 and f8 > 0:
        score -= penalty
        reasons.append(f"funding penalty long ({f8:.4f})")
    if score < 0 and f8 < 0:
        score += penalty
        reasons.append(f"funding penalty short ({f8:.4f})")

    # Clamp
    score = _clamp(score, -1.0, 1.0)
    return score, reasons


def decide_alpha(
    features: dict[str, float],
    funding8h_sum: float,
    cfg: PerpsAlphaConfig | None = None,
) -> AlphaResult:
    """
    Convert alpha score into a discrete signal with confidence.
    """
    cfg = cfg or PerpsAlphaConfig.from_env()
    score, reasons = score_alpha(features, funding8h_sum, cfg=cfg)

    # Confidence: map |score| to [0.5..1] via sigmoid-ish curve
    conf = _sigmoid((abs(score) - 0.20) * 6.0)  # tuned for ~0.6 confidence at ~0.35 score
    conf = _clamp(conf, 0.0, 1.0)

    sig = Signal.FLAT
    if score >= cfg.long_score_thr and conf >= cfg.min_confidence:
        sig = Signal.LONG
    elif score <= cfg.short_score_thr and conf >= cfg.min_confidence:
        sig = Signal.SHORT

    if sig is Signal.FLAT and score != 0.0:
        reasons.append(f"below threshold/conf (score={score:.2f}, conf={conf:.2f})")

    return AlphaResult(signal=sig, score=score, confidence=conf, reasons=reasons)


# -----------------------------------------------------------------------------
# Backwards-compatible API (your original functions)
# -----------------------------------------------------------------------------
def should_long(features: dict[str, float], funding8h_sum: float) -> bool:
    """
    Legacy boolean long decision.
    """
    res = decide_alpha(features, funding8h_sum)
    return res.signal == Signal.LONG


def should_short(features: dict[str, float], funding8h_sum: float) -> bool:
    """
    Legacy boolean short decision.
    """
    res = decide_alpha(features, funding8h_sum)
    return res.signal == Signal.SHORT


__all__ = [
    "AlphaResult",
    "PerpsAlphaConfig",
    "Signal",
    "decide_alpha",
    "score_alpha",
    "should_long",
    "should_short",
]
