# daytrader/alpha.py
"""
daytrader.alpha
---------------
Ensemble alpha that blends trend, breakout, mean-reversion, volume burst,
and optional AI/whale gating into a single actionable signal.

Inputs
------
- mint:  token mint (str)
- symbol: human-readable symbol (str)
- bars: list of dicts with OHLCV keys: {"t","o","h","l","c","v"} in chronological order
- min_liq_usd: optional minimum liquidity override (float)

Returns
-------
(action, score_0_100, features_dict, reasons_list)

Env tuning (optional)
---------------------
DT_MIN_LIQ_USD       default 500.0
DT_MAX_ATR_PCT       default 0.30
DT_SIGNAL_BUY_THR    default 65
DT_SIGNAL_SELL_THR   default 65
"""

from __future__ import annotations
from typing import List, Dict, Tuple, Optional
import math
import os

from .features import (
    ema, rsi, atr_pct, donchian, vwap, volume_burst, regime_trend_vs_range,
    liquidity_usd_proxy, pack_features
)

# Optional AI/whale gate (blocks buys that fail the gate)
_AI_AVAILABLE = False
try:
    from utils_pkg.ai_buy_scoring import Candidate, pass_gate  # type: ignore
    _AI_AVAILABLE = True
except Exception:
    _AI_AVAILABLE = False

Signal = str  # 'BUY' | 'SELL' | 'HOLD'


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, default))
    except Exception:
        return default


def generate_signal(
    mint: str,
    symbol: str,
    bars: List[Dict[str, float]],
    min_liq_usd: Optional[float] = None,
) -> Tuple[Signal, float, Dict[str, float], List[str]]:
    """
    Compute ensemble signal from recent bars.

    Returns (action, score0-100, feature_dict, reasons[]).
    """
    reasons: List[str] = []
    if not bars or len(bars) < 50:
        return "HOLD", 0.0, {}, ["insufficient bars"]

    # Compute and collect features (helper builds many basics in one pass)
    feats = pack_features(bars)

    last_c = float(bars[-1]["c"])
    regime = "trending" if feats.get("regime", 0.0) >= 0.5 else "ranging"
    atrp = feats.get("atr_pct", float("nan"))
    vol_burst = feats.get("vol_burst", float("nan"))
    ema_fast = feats.get("ema_fast", float("nan"))
    ema_slow = feats.get("ema_slow", float("nan"))
    rsi14 = feats.get("rsi14", float("nan"))
    u, l, _ = donchian(bars, 20)
    vwap20 = feats.get("vwap20", float("nan"))
    liq = feats.get("liq_usd_10", 0.0)

    # Liquidity guard
    min_liq = min_liq_usd if min_liq_usd is not None else _env_float("DT_MIN_LIQ_USD", 500.0)
    if liq < min_liq:
        return "HOLD", 0.0, feats, [f"low liquidity {liq:.0f} < {min_liq:.0f}"]

    # Volatility guard (penalize later if too hot)
    max_atr_pct = _env_float("DT_MAX_ATR_PCT", 0.30)  # e.g., 30% of price
    if not math.isnan(atrp) and atrp > max_atr_pct:
        reasons.append(f"atr% too high {atrp:.2f} > {max_atr_pct:.2f}")

    # ----- Sub-signals in [-1, 1] -----

    # 1) Trend (EMA12 vs EMA48)
    s_trend = 0.0
    if not (math.isnan(ema_fast) or math.isnan(ema_slow)):
        if ema_fast > ema_slow * 1.001:
            s_trend = 0.8
            reasons.append("trend up (ema12>ema48)")
        elif ema_fast < ema_slow * 0.999:
            s_trend = -0.8
            reasons.append("trend down (ema12<ema48)")

    # 2) Breakout (Donchian + volume burst confirmation)
    s_breakout = 0.0
    if not any(math.isnan(x) for x in (u, l, last_c)):
        if last_c > u and (not math.isnan(vol_burst) and vol_burst >= 2.0):
            s_breakout = 1.0
            reasons.append("donchian breakout + vol burst")
        elif last_c < l and (not math.isnan(vol_burst) and vol_burst >= 2.0):
            s_breakout = -1.0
            reasons.append("donchian breakdown + vol burst")

    # 3) Mean reversion (RSI in ranging regimes)
    s_meanrev = 0.0
    if not math.isnan(rsi14):
        if regime == "ranging":
            if rsi14 < 30.0:
                s_meanrev = +0.5
                reasons.append("rsi<30 mean-rev buy bias")
            elif rsi14 > 70.0:
                s_meanrev = -0.5
                reasons.append("rsi>70 mean-rev sell bias")

    # 4) VWAP location (trend confirmation)
    s_vwap = 0.0
    if not math.isnan(vwap20):
        if last_c > vwap20 and regime == "trending":
            s_vwap = +0.3
        elif last_c < vwap20 and regime == "trending":
            s_vwap = -0.3

    # ----- Regime-aware weights -----
    if regime == "trending":
        weights = (0.45, 0.35, 0.10, 0.10)  # trend, breakout, meanrev, vwap
    else:
        weights = (0.20, 0.20, 0.45, 0.15)

    components = (s_trend, s_breakout, s_meanrev, s_vwap)
    raw = sum(w * s for w, s in zip(weights, components))

    # ----- Optional AI/whale gate (blocks risky buys) -----
    if _AI_AVAILABLE:
        try:
            from utils_pkg.ai_buy_scoring import Candidate, pass_gate  # lazy import in case hot-reload
            cand = Candidate(
                mint=mint,
                symbol=symbol,
                pool_age_min=0.0,          # fill actual pool age if available
                liq_usd=liq,
                impact_1k_bps=0.0,         # set from your impact calc if available
                vol5m_pct=(atrp or 0.0) * 100.0,
                top10_pct=0.0,             # set if you compute holder concentration
                honeypot_flag=False,       # from honeypot checks if you run them here
                whale_touches_1h=0,        # from whale watcher if available
            )
            ok, score_gate, why = pass_gate(cand)
            feats["ai_gate_score"] = float(score_gate)
            if not ok and raw > 0:
                reasons.append(f"ai_gate blocked: {why}")
                raw = 0.0  # block buy, allow sells
        except Exception as e:
            reasons.append(f"ai_gate error: {e}")

    # ----- Risk tempering -----
    if not math.isnan(atrp):
        if atrp > 0.10:  # very hot
            raw *= 0.7
        if atrp > 0.20:  # extremely hot
            raw *= 0.5

    # Map [-1,1] → [0,100]
    score = max(0.0, min(100.0, (raw + 1.0) * 50.0))

    # Thresholds → Action
    try:
        buy_thr = float(os.getenv("DT_SIGNAL_BUY_THR", 65))
        sell_thr = float(os.getenv("DT_SIGNAL_SELL_THR", 65))
    except Exception:
        buy_thr, sell_thr = 65.0, 65.0

    if raw >= 0.3 and score >= buy_thr:
        action: Signal = "BUY"
    elif raw <= -0.3 and score >= sell_thr:
        action = "SELL"
    else:
        action = "HOLD"

    return action, score, feats, reasons
