# allocator/regime.py
from __future__ import annotations

"""
allocator/regime.py

Regime classifier (P2 / non-competing subsystem)
================================================

Goal:
- Classify recent equity returns into a small, stable set of regime labels
  used by allocator/capital_allocator.py to bias sleeve weights.

Compatibility:
- Returns one of:
    "low_vol_trend", "low_vol_sideways", "high_vol_trend", "high_vol_chop", "neutral"

Design principles:
- Deterministic + robust: handles NaN/inf, small samples, outliers.
- Numpy-only (no scipy).
- Uses relative volatility (recent vs baseline) + trend strength + "chop" score.

If you later want richer regimes, use `classify_regime_detail()` and keep
`classify_regime()` returning the canonical labels so the rest of the system
doesn't drift.
"""

import os
from collections.abc import Iterable
from dataclasses import dataclass

import numpy as np


# -----------------------------
# Public types
# -----------------------------
@dataclass(frozen=True)
class RegimeResult:
    label: str
    metrics: dict[str, float]


# -----------------------------
# Helpers (robust stats)
# -----------------------------
def _env_int(name: str, default: int) -> int:
    try:
        v = int(str(os.getenv(name, "")).strip() or default)
        return v
    except Exception:
        return default


def _env_float(name: str, default: float) -> float:
    try:
        v = float(str(os.getenv(name, "")).strip() or default)
        return v
    except Exception:
        return default


def _to_clean_array(x: Iterable[float] | np.ndarray) -> np.ndarray:
    arr = np.asarray(list(x) if not isinstance(x, np.ndarray) else x, dtype=float).reshape(-1)
    if arr.size == 0:
        return arr
    # drop nan/inf
    arr = arr[np.isfinite(arr)]
    return arr


def _winsorize(arr: np.ndarray, p: float) -> np.ndarray:
    """
    Winsorize at p (two-sided). p=0.01 clamps to [1st, 99th] percentile.
    """
    if arr.size < 8 or p <= 0:
        return arr
    lo = np.nanpercentile(arr, 100.0 * p)
    hi = np.nanpercentile(arr, 100.0 * (1.0 - p))
    return np.clip(arr, lo, hi)


def _rolling_slice(arr: np.ndarray, n: int) -> np.ndarray:
    if n <= 0 or arr.size == 0:
        return arr
    return arr[-min(n, arr.size) :]


def _std(arr: np.ndarray) -> float:
    if arr.size < 2:
        return 0.0
    return float(np.std(arr, ddof=1))


def _mean(arr: np.ndarray) -> float:
    if arr.size == 0:
        return 0.0
    return float(np.mean(arr))


def _trend_z(returns: np.ndarray, trend_window: int, eps: float) -> float:
    """
    Trend strength as z-ish score:
      slope of cumulative curve / recent_vol * sqrt(n)
    """
    r = _rolling_slice(returns, trend_window)
    if r.size < max(12, trend_window // 3):
        return 0.0

    c = np.cumsum(r)
    # simple OLS slope with centered x
    x = np.arange(c.size, dtype=float)
    x -= x.mean()
    denom = float(np.dot(x, x)) + eps
    slope = float(np.dot(x, c - c.mean()) / denom)  # per-step slope in cumulative units

    vol = _std(r) + eps
    z = (slope / vol) * np.sqrt(float(r.size))
    return float(z)


def _chop_score(returns: np.ndarray, chop_window: int, eps: float) -> float:
    """
    Chop score: higher means more whipsaw / reversal-y.
    Uses:
      - sign flip rate
      - negative autocorrelation
    """
    r = _rolling_slice(returns, chop_window)
    if r.size < max(16, chop_window // 2):
        return 0.0

    s = np.sign(r)
    # ignore zeros by carrying forward sign where possible
    for i in range(1, s.size):
        if s[i] == 0.0:
            s[i] = s[i - 1]
    # flip rate
    flips = float(np.mean(s[1:] * s[:-1] < 0.0))  # 0..1
    # lag-1 autocorr (bounded)
    r0 = r - r.mean()
    num = float(np.dot(r0[1:], r0[:-1]))
    den = float(np.dot(r0, r0)) + eps
    ac1 = num / den  # ~[-1,1]
    # turn into chop: more flips + more negative autocorr -> higher chop
    chop = flips + max(0.0, -ac1)
    return float(chop)


def _max_drawdown(returns: np.ndarray, dd_window: int, eps: float) -> float:
    """
    Approx max drawdown on cumulative equity curve from returns.
    Returns fraction in [0, 1+] (as a positive number).
    """
    r = _rolling_slice(returns, dd_window)
    if r.size < 8:
        return 0.0
    eq = np.cumsum(r)  # log-ish ok for small returns
    peak = np.maximum.accumulate(eq)
    dd = peak - eq
    mdd = float(np.max(dd)) if dd.size else 0.0
    # scale by typical move to be stable; clamp to reasonable
    scale = (_std(r) * np.sqrt(float(r.size))) + eps
    frac = mdd / scale
    return float(max(0.0, min(frac, 10.0)))


# -----------------------------
# Main API
# -----------------------------
def classify_regime_detail(
    returns: Iterable[float] | np.ndarray,
    *,
    vol_window: int | None = None,
    baseline_window: int | None = None,
    trend_window: int | None = None,
    chop_window: int | None = None,
    dd_window: int | None = None,
    winsor_p: float | None = None,
    low_vol_ratio: float | None = None,
    high_vol_ratio: float | None = None,
    trend_z_cut: float | None = None,
    chop_cut: float | None = None,
    dd_panic_cut: float | None = None,
) -> RegimeResult:
    """
    Detailed classifier. Use this if you want metrics for debugging/telemetry.
    """

    # Defaults (overridable by env)
    eps = 1e-12
    vol_window = vol_window or _env_int("ALLOC_REGIME_VOL_WINDOW", 64)
    baseline_window = baseline_window or _env_int("ALLOC_REGIME_BASELINE_WINDOW", 256)
    trend_window = trend_window or _env_int("ALLOC_REGIME_TREND_WINDOW", 64)
    chop_window = chop_window or _env_int("ALLOC_REGIME_CHOP_WINDOW", 64)
    dd_window = dd_window or _env_int("ALLOC_REGIME_DD_WINDOW", 96)

    winsor_p = winsor_p if winsor_p is not None else _env_float("ALLOC_REGIME_WINSOR_P", 0.01)

    low_vol_ratio = (
        low_vol_ratio
        if low_vol_ratio is not None
        else _env_float("ALLOC_REGIME_LOW_VOL_RATIO", 0.75)
    )
    high_vol_ratio = (
        high_vol_ratio
        if high_vol_ratio is not None
        else _env_float("ALLOC_REGIME_HIGH_VOL_RATIO", 1.25)
    )

    trend_z_cut = (
        trend_z_cut if trend_z_cut is not None else _env_float("ALLOC_REGIME_TREND_Z_CUT", 0.55)
    )
    chop_cut = chop_cut if chop_cut is not None else _env_float("ALLOC_REGIME_CHOP_CUT", 0.85)
    dd_panic_cut = (
        dd_panic_cut if dd_panic_cut is not None else _env_float("ALLOC_REGIME_DD_PANIC_CUT", 2.25)
    )

    arr0 = _to_clean_array(returns)
    if arr0.size < max(32, min(baseline_window, 128)):
        return RegimeResult("neutral", {"n": float(arr0.size)})

    arr = _winsorize(arr0, winsor_p)

    recent = _rolling_slice(arr, vol_window)
    base = _rolling_slice(arr, baseline_window)

    recent_vol = _std(recent)
    base_vol = _std(base) + eps

    vol_ratio = (recent_vol + eps) / base_vol

    tz = _trend_z(arr, trend_window=trend_window, eps=eps)
    chop = _chop_score(arr, chop_window=chop_window, eps=eps)
    dd = _max_drawdown(arr, dd_window=dd_window, eps=eps)

    # Determine vol bucket
    if vol_ratio <= low_vol_ratio:
        vol_bucket = "low"
    elif vol_ratio >= high_vol_ratio:
        vol_bucket = "high"
    else:
        vol_bucket = "mid"

    # Determine trend / sideways
    is_trend = abs(tz) >= trend_z_cut
    trend_sign = 1.0 if tz > 0 else (-1.0 if tz < 0 else 0.0)

    # Panic-ish guard: if drawdown is very severe, treat as "chop"
    # (we keep labels compatible; allocator can de-risk in chop regimes)
    panic = dd >= dd_panic_cut

    label: str
    if vol_bucket == "low":
        label = "low_vol_trend" if (is_trend and trend_sign > 0) else "low_vol_sideways"
    elif vol_bucket == "high":
        # High vol + trend => trend. High vol + no trend OR very choppy OR panic => chop.
        if (is_trend and trend_sign > 0) and (chop < chop_cut) and not panic:
            label = "high_vol_trend"
        else:
            label = "high_vol_chop"
    else:
        # mid vol: neutral unless strong trend and low chop; still keep neutral label for stability
        if (is_trend and trend_sign > 0) and (chop < chop_cut) and not panic:
            # keep neutral to avoid reallocating too aggressively in mid-vol
            label = "neutral"
        else:
            label = "neutral"

    metrics = {
        "n": float(arr.size),
        "recent_vol": float(recent_vol),
        "base_vol": float(base_vol),
        "vol_ratio": float(vol_ratio),
        "trend_z": float(tz),
        "chop": float(chop),
        "dd_scaled": float(dd),
        "panic": float(1.0 if panic else 0.0),
    }
    return RegimeResult(label=label, metrics=metrics)


def classify_regime(
    returns: Iterable[float] | np.ndarray,
    vol_window: int = 64,
) -> str:
    """
    Backward-compatible API used by allocator/capital_allocator.py

    NOTE:
    We keep this signature simple, but it still uses env overrides internally
    via classify_regime_detail(). `vol_window` here is a hint and becomes the
    default vol window unless env overrides are set.
    """
    res = classify_regime_detail(returns, vol_window=vol_window)
    return res.label


def classify_regime_with_metrics(
    returns: Iterable[float] | np.ndarray,
    *,
    vol_window: int = 64,
) -> tuple[str, dict[str, float]]:
    """
    Convenience: returns (label, metrics) so callers can log or export telemetry.
    """
    res = classify_regime_detail(returns, vol_window=vol_window)
    return res.label, dict(res.metrics)


__all__ = [
    "RegimeResult",
    "classify_regime",
    "classify_regime_detail",
    "classify_regime_with_metrics",
]
