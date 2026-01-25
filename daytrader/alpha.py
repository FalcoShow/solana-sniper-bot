# daytrader/alpha.py
from __future__ import annotations

"""
daytrader.alpha
---------------

Ensemble alpha signal generator for the OPTIONAL "daytrader" sleeve (P2 cleanup area).
This module MUST NOT silently compete with the canonical sniper path.

Role
----
Given OHLCV bars, output a single actionable signal:

    (action, score_0_100, features_dict, reasons_list)

Actions:
    "BUY" | "SELL" | "HOLD"

Design goals
------------
- Deterministic and safe: handles NaN/inf, missing keys, small sample sizes.
- No network I/O, no wallet usage, no side effects.
- Optional gates (AI / whale) are OFF by default and must be explicitly enabled.
- Robust internal feature fallbacks even if daytrader.features changes.

Bars format
----------
bars: list[dict] chronological order, each dict has keys:
    {"t","o","h","l","c","v"}  (t optional)

Env tuning (safe defaults)
--------------------------
Core guards:
  DT_MIN_BARS=60
  DT_MIN_LIQ_USD=500
  DT_MAX_ATR_PCT=0.30                # 0.30 = 30% (very hot)
  DT_EXTREME_ATR_PCT=0.55            # above this, we refuse BUY

Signal thresholds:
  DT_SIGNAL_BUY_THR=65
  DT_SIGNAL_SELL_THR=65
  DT_RAW_BUY_MIN=0.30                # raw score in [-1..1]
  DT_RAW_SELL_MIN=0.30

Feature windows:
  DT_EMA_FAST=12
  DT_EMA_SLOW=48
  DT_RSI_N=14
  DT_ATR_N=14
  DT_DONCHIAN_N=20
  DT_VWAP_N=20
  DT_VOL_BURST_N=20

Regime:
  DT_REGIME_TREND_THR=0.55           # 0..1 score: >= means trending

Weights (regime-aware base):
  DT_W_TREND=0.45
  DT_W_BREAKOUT=0.35
  DT_W_MEANREV=0.10
  DT_W_VWAP=0.10

In ranging regime, weights are swapped toward mean-reversion:
  DT_W_TREND_R=0.20
  DT_W_BREAKOUT_R=0.20
  DT_W_MEANREV_R=0.45
  DT_W_VWAP_R=0.15

Optional gates (OFF by default):
  DT_AI_GATE_ENABLED=0|1
  DT_AI_GATE_STRICT=0|1              # if 1, blocks BUY hard; if 0, just penalizes
  DT_AI_GATE_MIN_SCORE=0.55          # if strict: must be >= this to allow BUY
"""

import math
import os
from dataclasses import dataclass
from typing import Any

Signal = str  # "BUY" | "SELL" | "HOLD"


_Bar = dict[str, float]


# -----------------------------------------------------------------------------
# Env helpers (safe parsing)
# -----------------------------------------------------------------------------
def _env_int(name: str, default: int) -> int:
    raw = str(os.getenv(name, "")).strip()
    if not raw:
        return int(default)
    try:
        return int(raw)
    except Exception:
        return int(default)


def _env_float(name: str, default: float) -> float:
    raw = str(os.getenv(name, "")).strip()
    if not raw:
        return float(default)
    try:
        return float(raw)
    except Exception:
        return float(default)


def _env_bool(name: str, default: bool = False) -> bool:
    raw = str(os.getenv(name, "")).strip()
    if raw == "":
        return bool(default)
    return raw.lower() in {"1", "true", "yes", "y", "on", "t"}


def _clamp(x: float, lo: float, hi: float) -> float:
    if x < lo:
        return lo
    if x > hi:
        return hi
    return x


def _finite_float(val: Any) -> float | None:
    try:
        x = float(val)
    except Exception:
        return None
    return x if math.isfinite(x) else None


def _first_float(bar: dict[str, Any], *keys: str) -> float | None:
    for key in keys:
        if key in bar:
            x = _finite_float(bar.get(key))
            if x is not None:
                return x
    return None


def _dedupe_reasons(reasons: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for r in reasons:
        if not r:
            continue
        if r in seen:
            continue
        seen.add(r)
        out.append(r)
    return out


# -----------------------------------------------------------------------------
# Config (single place for all knobs)
# -----------------------------------------------------------------------------
@dataclass(frozen=True)
class AlphaConfig:
    min_bars: int = 60
    min_liq_usd: float = 500.0
    max_atr_pct: float = 0.30
    extreme_atr_pct: float = 0.55

    signal_buy_thr: float = 65.0
    signal_sell_thr: float = 65.0
    raw_buy_min: float = 0.30
    raw_sell_min: float = 0.30

    ema_fast_n: int = 12
    ema_slow_n: int = 48
    rsi_n: int = 14
    atr_n: int = 14
    donchian_n: int = 20
    vwap_n: int = 20
    vol_burst_n: int = 20

    regime_trend_thr: float = 0.55

    # weights (trending)
    w_trend: float = 0.45
    w_breakout: float = 0.35
    w_meanrev: float = 0.10
    w_vwap: float = 0.10

    # weights (ranging)
    w_trend_r: float = 0.20
    w_breakout_r: float = 0.20
    w_meanrev_r: float = 0.45
    w_vwap_r: float = 0.15

    # AI gate
    ai_gate_enabled: bool = False
    ai_gate_strict: bool = False
    ai_gate_min_score: float = 0.55

    @staticmethod
    def from_env(min_liq_override: float | None = None) -> "AlphaConfig":
        min_bars = max(10, _env_int("DT_MIN_BARS", 60))

        ema_fast_n = max(2, _env_int("DT_EMA_FAST", 12))
        ema_slow_n = max(3, _env_int("DT_EMA_SLOW", 48))
        rsi_n = max(2, _env_int("DT_RSI_N", 14))
        atr_n = max(2, _env_int("DT_ATR_N", 14))
        don_n = max(2, _env_int("DT_DONCHIAN_N", 20))
        vwap_n = max(2, _env_int("DT_VWAP_N", 20))
        vb_n = max(3, _env_int("DT_VOL_BURST_N", 20))

        min_liq = (
            float(min_liq_override)
            if min_liq_override is not None
            else _env_float("DT_MIN_LIQ_USD", 500.0)
        )
        min_liq = max(0.0, float(min_liq))

        max_atr = max(0.0, _env_float("DT_MAX_ATR_PCT", 0.30))
        extreme_atr = max(0.0, _env_float("DT_EXTREME_ATR_PCT", 0.55))

        buy_thr = _clamp(_env_float("DT_SIGNAL_BUY_THR", 65.0), 0.0, 100.0)
        sell_thr = _clamp(_env_float("DT_SIGNAL_SELL_THR", 65.0), 0.0, 100.0)

        raw_buy_min = _clamp(_env_float("DT_RAW_BUY_MIN", 0.30), 0.0, 1.0)
        raw_sell_min = _clamp(_env_float("DT_RAW_SELL_MIN", 0.30), 0.0, 1.0)

        regime_thr = _clamp(_env_float("DT_REGIME_TREND_THR", 0.55), 0.0, 1.0)

        w_trend = float(_env_float("DT_W_TREND", 0.45))
        w_break = float(_env_float("DT_W_BREAKOUT", 0.35))
        w_mr = float(_env_float("DT_W_MEANREV", 0.10))
        w_vwap = float(_env_float("DT_W_VWAP", 0.10))

        w_trend_r = float(_env_float("DT_W_TREND_R", 0.20))
        w_break_r = float(_env_float("DT_W_BREAKOUT_R", 0.20))
        w_mr_r = float(_env_float("DT_W_MEANREV_R", 0.45))
        w_vwap_r = float(_env_float("DT_W_VWAP_R", 0.15))

        ai_enabled = _env_bool("DT_AI_GATE_ENABLED", False)
        ai_strict = _env_bool("DT_AI_GATE_STRICT", False)
        ai_min = _clamp(_env_float("DT_AI_GATE_MIN_SCORE", 0.55), 0.0, 1.0)

        return AlphaConfig(
            min_bars=min_bars,
            min_liq_usd=min_liq,
            max_atr_pct=max_atr,
            extreme_atr_pct=extreme_atr,
            signal_buy_thr=buy_thr,
            signal_sell_thr=sell_thr,
            raw_buy_min=raw_buy_min,
            raw_sell_min=raw_sell_min,
            ema_fast_n=ema_fast_n,
            ema_slow_n=ema_slow_n,
            rsi_n=rsi_n,
            atr_n=atr_n,
            donchian_n=don_n,
            vwap_n=vwap_n,
            vol_burst_n=vb_n,
            regime_trend_thr=regime_thr,
            w_trend=w_trend,
            w_breakout=w_break,
            w_meanrev=w_mr,
            w_vwap=w_vwap,
            w_trend_r=w_trend_r,
            w_breakout_r=w_break_r,
            w_meanrev_r=w_mr_r,
            w_vwap_r=w_vwap_r,
            ai_gate_enabled=ai_enabled,
            ai_gate_strict=ai_strict,
            ai_gate_min_score=ai_min,
        )


# -----------------------------------------------------------------------------
# Sanitization / extraction
# -----------------------------------------------------------------------------
def _sanitize_bars(bars: list[dict[str, Any]]) -> list[_Bar]:
    """
    Convert to list of float dicts, drop invalid rows,
    enforce high>=max(open,close,low), low<=min(open,close,high).
    """
    out: list[_Bar] = []
    if not bars:
        return out

    for bar in bars:
        if not isinstance(bar, dict):
            continue

        open_px = _first_float(bar, "o", "open")
        high_px = _first_float(bar, "h", "high")
        low_px = _first_float(bar, "l", "low")  # canonical key is "l" (ell)
        close_px = _first_float(bar, "c", "close")
        vol_px = _first_float(bar, "v", "volume")

        if (
            open_px is None
            or high_px is None
            or low_px is None
            or close_px is None
            or vol_px is None
        ):
            continue

        high_norm = max(high_px, open_px, close_px, low_px)
        low_norm = min(low_px, open_px, close_px, high_px)

        if high_norm <= 0.0 or low_norm <= 0.0 or close_px <= 0.0:
            continue

        vol_norm = 0.0 if vol_px < 0.0 else float(vol_px)

        out.append(
            {
                "o": float(open_px),
                "h": float(high_norm),
                "l": float(low_norm),
                "c": float(close_px),
                "v": float(vol_norm),
            }
        )

    return out


# -----------------------------------------------------------------------------
# Lightweight internal indicators (no deps, deterministic)
# -----------------------------------------------------------------------------
def _ema_last(values: list[float], n: int) -> float:
    if n <= 1 or len(values) < n:
        return float("nan")
    alpha = 2.0 / (n + 1.0)
    ema_val = values[0]
    for x in values[1:]:
        ema_val = alpha * x + (1.0 - alpha) * ema_val
    return float(ema_val)


def _rsi_last(closes: list[float], n: int) -> float:
    if n <= 1 or len(closes) < n + 1:
        return float("nan")

    gains = 0.0
    losses = 0.0
    for i in range(-n, 0):
        delta = closes[i] - closes[i - 1]
        if delta >= 0:
            gains += delta
        else:
            losses -= delta

    if gains == 0.0 and losses == 0.0:
        return 50.0

    rs = gains / (losses + 1e-12)
    return float(100.0 - (100.0 / (1.0 + rs)))


def _atr_pct(bars: list[_Bar], n: int) -> float:
    if n <= 1 or len(bars) < n + 1:
        return float("nan")
    trs: list[float] = []
    for i in range(-n, 0):
        high = bars[i]["h"]
        low = bars[i]["l"]
        prev_close = bars[i - 1]["c"]
        tr = max(high - low, abs(high - prev_close), abs(low - prev_close))
        trs.append(tr)
    atr = sum(trs) / max(1, len(trs))
    last_close = bars[-1]["c"]
    return float(atr / max(last_close, 1e-12))


def _donchian(bars: list[_Bar], n: int) -> tuple[float, float]:
    if n <= 1 or len(bars) < n:
        return float("nan"), float("nan")
    window = bars[-n:]
    return float(max(b["h"] for b in window)), float(min(b["l"] for b in window))


def _vwap(bars: list[_Bar], n: int) -> float:
    if n <= 1 or len(bars) < n:
        return float("nan")
    window = bars[-n:]
    pv = 0.0
    vv = 0.0
    for b in window:
        typical = (b["h"] + b["l"] + b["c"]) / 3.0
        vol = max(0.0, b["v"])
        pv += typical * vol
        vv += vol
    if vv <= 0.0:
        return float("nan")
    return float(pv / vv)


def _volume_burst(bars: list[_Bar], n: int) -> float:
    """Burst = current volume / mean(prev n-1 volumes)."""
    if n <= 2 or len(bars) < n:
        return float("nan")
    window = bars[-n:]
    v_last = window[-1]["v"]
    base = [b["v"] for b in window[:-1]]
    denom = (sum(base) / max(1, len(base))) + 1e-12
    return float(v_last / denom)


def _regime_score(closes: list[float], n: int) -> float:
    """
    0..1 score: higher => trending.
    Uses normalized slope vs noise on recent closes.
    """
    if n <= 10 or len(closes) < n:
        return 0.0

    w = closes[-n:]
    xm = (n - 1) / 2.0
    ym = sum(w) / n

    num = 0.0
    den = 0.0
    for i, y in enumerate(w):
        dx = i - xm
        num += dx * (y - ym)
        den += dx * dx
    slope = num / (den + 1e-12)

    diffs = [abs(w[i] - w[i - 1]) for i in range(1, n)]
    noise = (sum(diffs) / max(1, len(diffs))) + 1e-12

    strength = abs(slope) / noise
    return float(_clamp(strength / (strength + 1.0), 0.0, 1.0))


def _median(vals: list[float]) -> float:
    if not vals:
        return 0.0
    s = sorted(vals)
    return float(s[len(s) // 2])


# -----------------------------------------------------------------------------
# Optional AI gate (OFF by default)
# -----------------------------------------------------------------------------
def _maybe_ai_gate(
    *,
    enabled: bool,
    strict: bool,
    min_score: float,
    mint: str,
    symbol: str,
    liq_usd: float,
    atr_pct: float,
    feats: dict[str, float],
    reasons: list[str],
    raw: float,
) -> float:
    """
    Applies AI gate for BUY direction only (raw>0).
    Returns possibly adjusted raw score.
    """
    if not enabled or raw <= 0.0:
        return raw

    try:
        # Lazy import to keep daytrader sleeve optional and side-effect-free.
        from utils_pkg.ai_buy_scoring import Candidate, pass_gate  # type: ignore
    except Exception:
        reasons.append("ai_gate unavailable (import)")
        return raw

    try:
        cand_kwargs: dict[str, Any] = {
            "mint": mint,
            "symbol": symbol,
            "liq_usd": float(liq_usd),
            "vol5m_pct": float(atr_pct * 100.0) if math.isfinite(atr_pct) else 0.0,
            "impact_1k_bps": 0.0,
            "top10_pct": 0.0,
            "honeypot_flag": False,
            "whale_touches_1h": 0,
            "pool_age_min": 0.0,
        }

        try:
            cand = Candidate(**cand_kwargs)  # type: ignore
        except TypeError:
            # Fallback if Candidate has a smaller signature in your repo.
            cand = Candidate(mint=mint, symbol=symbol)  # type: ignore

        ok, gate_score, why = pass_gate(cand)  # type: ignore

        score_f = float(gate_score) if gate_score is not None else 0.0
        min_score_f = float(min_score)

        feats["ai_gate_ok"] = 1.0 if ok else 0.0
        feats["ai_gate_score"] = score_f

        if strict:
            if (not ok) or (score_f < min_score_f):
                reasons.append(f"ai_gate blocked buy: {why}")
                return 0.0
            reasons.append(f"ai_gate ok: {why}")
            return raw

        if (not ok) or (score_f < min_score_f):
            reasons.append(f"ai_gate penalized: {why}")
            return raw * 0.35

        reasons.append(f"ai_gate ok: {why}")
        return raw

    except Exception as exc:
        reasons.append(f"ai_gate error: {type(exc).__name__}: {exc}")
        return raw


# -----------------------------------------------------------------------------
# Main API
# -----------------------------------------------------------------------------
def generate_signal(
    mint: str,
    symbol: str,
    bars: list[dict[str, Any]],
    min_liq_usd: float | None = None,
) -> tuple[Signal, float, dict[str, float], list[str]]:
    cfg = AlphaConfig.from_env(min_liq_override=min_liq_usd)
    reasons: list[str] = []
    feats: dict[str, float] = {}

    sbars = _sanitize_bars(bars)
    if len(sbars) < cfg.min_bars:
        return "HOLD", 0.0, {}, [f"insufficient bars ({len(sbars)}<{cfg.min_bars})"]

    closes = [b["c"] for b in sbars]
    last_close = closes[-1]

    # Core features
    ema_fast = _ema_last(closes[-max(cfg.ema_fast_n * 3, cfg.ema_fast_n + 5) :], cfg.ema_fast_n)
    ema_slow = _ema_last(closes[-max(cfg.ema_slow_n * 3, cfg.ema_slow_n + 5) :], cfg.ema_slow_n)
    rsi_val = _rsi_last(closes, cfg.rsi_n)
    atrp = _atr_pct(sbars, cfg.atr_n)
    don_high, don_low = _donchian(sbars, cfg.donchian_n)
    vwap_val = _vwap(sbars, cfg.vwap_n)
    vol_burst = _volume_burst(sbars, cfg.vol_burst_n)
    regime_score = _regime_score(closes, max(cfg.ema_slow_n, 60))

    # Liquidity proxy: median of last N volumes * last close
    liq_win = min(10, len(sbars))
    recent_vols = [sbars[-i]["v"] for i in range(1, liq_win + 1)]
    liq_usd = float(_median(recent_vols) * last_close)

    feats.update(
        {
            "last_c": float(last_close),
            "ema_fast": float(ema_fast) if math.isfinite(ema_fast) else float("nan"),
            "ema_slow": float(ema_slow) if math.isfinite(ema_slow) else float("nan"),
            "rsi": float(rsi_val) if math.isfinite(rsi_val) else float("nan"),
            "atr_pct": float(atrp) if math.isfinite(atrp) else float("nan"),
            "don_hi": float(don_high) if math.isfinite(don_high) else float("nan"),
            "don_lo": float(don_low) if math.isfinite(don_low) else float("nan"),
            "vwap": float(vwap_val) if math.isfinite(vwap_val) else float("nan"),
            "vol_burst": float(vol_burst) if math.isfinite(vol_burst) else float("nan"),
            "regime": float(regime_score),
            "liq_usd_proxy": float(liq_usd),
        }
    )

    # Hard liquidity guard
    if liq_usd < cfg.min_liq_usd:
        return "HOLD", 0.0, feats, [f"low liquidity {liq_usd:.2f} < {cfg.min_liq_usd:.2f}"]

    # Volatility notes/guard
    if math.isfinite(atrp) and atrp > cfg.max_atr_pct:
        reasons.append(f"atr% high {atrp:.3f} > {cfg.max_atr_pct:.3f}")
    if math.isfinite(atrp) and atrp > cfg.extreme_atr_pct:
        reasons.append(f"atr% extreme {atrp:.3f} > {cfg.extreme_atr_pct:.3f}")

    regime = "trending" if regime_score >= cfg.regime_trend_thr else "ranging"
    feats["regime_is_trending"] = 1.0 if regime == "trending" else 0.0

    # -------------------------------------------------------------------------
    # Sub-signals in [-1..1]
    # -------------------------------------------------------------------------
    s_trend = 0.0
    if math.isfinite(ema_fast) and math.isfinite(ema_slow) and ema_slow > 0.0:
        if ema_fast > ema_slow * 1.001:
            s_trend = +0.80
            reasons.append("trend up (ema_fast>ema_slow)")
        elif ema_fast < ema_slow * 0.999:
            s_trend = -0.80
            reasons.append("trend down (ema_fast<ema_slow)")

    s_breakout = 0.0
    if math.isfinite(don_high) and math.isfinite(don_low) and math.isfinite(vol_burst):
        vb_thr = 2.0
        if last_close > don_high and vol_burst >= vb_thr:
            s_breakout = +1.00
            reasons.append("donchian breakout + vol burst")
        elif last_close < don_low and vol_burst >= vb_thr:
            s_breakout = -1.00
            reasons.append("donchian breakdown + vol burst")

    s_meanrev = 0.0
    if math.isfinite(rsi_val) and regime == "ranging":
        if rsi_val < 30.0:
            s_meanrev = +0.55
            reasons.append("rsi<30 mean-rev buy bias")
        elif rsi_val > 70.0:
            s_meanrev = -0.55
            reasons.append("rsi>70 mean-rev sell bias")

    s_vwap = 0.0
    if math.isfinite(vwap_val) and vwap_val > 0.0 and regime == "trending":
        if last_close > vwap_val:
            s_vwap = +0.30
        elif last_close < vwap_val:
            s_vwap = -0.30

    if regime == "trending":
        weights = (cfg.w_trend, cfg.w_breakout, cfg.w_meanrev, cfg.w_vwap)
    else:
        weights = (cfg.w_trend_r, cfg.w_breakout_r, cfg.w_meanrev_r, cfg.w_vwap_r)

    raw = (
        weights[0] * s_trend
        + weights[1] * s_breakout
        + weights[2] * s_meanrev
        + weights[3] * s_vwap
    )

    feats.update(
        {
            "raw_trend": float(s_trend),
            "raw_breakout": float(s_breakout),
            "raw_meanrev": float(s_meanrev),
            "raw_vwap": float(s_vwap),
            "raw_pre_ai": float(raw),
        }
    )

    # Optional AI gate (BUY only)
    raw = _maybe_ai_gate(
        enabled=cfg.ai_gate_enabled,
        strict=cfg.ai_gate_strict,
        min_score=cfg.ai_gate_min_score,
        mint=mint,
        symbol=symbol,
        liq_usd=liq_usd,
        atr_pct=atrp if math.isfinite(atrp) else 0.0,
        feats=feats,
        reasons=reasons,
        raw=raw,
    )

    # Risk tempering (preserves prior behavior)
    if math.isfinite(atrp):
        if atrp > 0.10:
            raw *= 0.70
        if atrp > 0.20:
            raw *= 0.50
        if atrp > cfg.extreme_atr_pct and raw > 0.0:
            raw = 0.0  # refuse BUY in extreme vol

    if liq_usd < (cfg.min_liq_usd * 2.0):
        raw *= 0.85

    raw = _clamp(raw, -1.0, 1.0)
    feats["raw_final"] = float(raw)

    score = _clamp((raw + 1.0) * 50.0, 0.0, 100.0)

    action: Signal = "HOLD"
    if raw >= cfg.raw_buy_min and score >= cfg.signal_buy_thr:
        action = "BUY"
    elif raw <= -cfg.raw_sell_min and score >= cfg.signal_sell_thr:
        action = "SELL"

    reasons_out = _dedupe_reasons(reasons)
    if not reasons_out:
        reasons_out = ["no strong edge"]

    return action, float(score), feats, reasons_out


__all__ = ["generate_signal"]
