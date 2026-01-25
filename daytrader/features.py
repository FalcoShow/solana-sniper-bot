# daytrader/features.py
from __future__ import annotations

"""
daytrader.features
------------------
Canonical indicator + feature toolbox for the Daytrader stack.

Hard contract
-------------
- Deterministic, safe, and never self-import / circular import.
- Tolerate imperfect bar dicts and return NaN (not exceptions) when data is insufficient.
- No numpy/pandas. Pure Python.

Expected bar schema
-------------------
bars: list[dict] oldest -> newest
keys: 't','o','h','l','c','v'
(we also accept alternates: open/high/low/close/volume, etc.)
values: numeric-ish (we coerce safely)

Exports
-------
Indicators/features used by alpha and downstream:
  sma, ema, zscore,
  atr, atr_pct, rsi,
  donchian, vwap, volume_burst,
  regime_trend_vs_range, liquidity_usd_proxy,
  pack_features

Optional legacy compatibility:
  StrategyConfig, TradeIntent, RulesEngineStrategy
"""

import math
import os
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import Any

# ----------------------------- Types / constants ------------------------------

Bar = dict[str, float]
Features = dict[str, float]
NAN = float("nan")

# canonical keys -> alternates (deterministic, no I/O)
_KEY_ALTS: dict[str, tuple[str, ...]] = {
    "t": ("t", "time", "ts", "timestamp"),
    "o": ("o", "open"),
    "h": ("h", "high"),
    # IMPORTANT: dict key is "l"; we never use a local variable named `l` (E741).
    "l": ("l", "low"),
    "c": ("c", "close"),
    "v": ("v", "volume", "vol"),
}

# ----------------------------- Safe env parsing -------------------------------


def _env_str(name: str, default: str = "") -> str:
    raw = os.getenv(name)
    return default if raw is None else str(raw).strip()


def _env_int(
    name: str,
    default: int,
    *,
    min_value: int | None = None,
    max_value: int | None = None,
) -> int:
    raw = _env_str(name, "")
    try:
        val = int(raw) if raw else int(default)
    except Exception:
        val = int(default)
    if min_value is not None:
        val = max(int(min_value), val)
    if max_value is not None:
        val = min(int(max_value), val)
    return val


def _env_float(
    name: str,
    default: float,
    *,
    min_value: float | None = None,
    max_value: float | None = None,
) -> float:
    raw = _env_str(name, "")
    try:
        val = float(raw) if raw else float(default)
    except Exception:
        val = float(default)
    if min_value is not None:
        val = max(float(min_value), val)
    if max_value is not None:
        val = min(float(max_value), val)
    return val


# ----------------------------- Coercion helpers -------------------------------


def _finite_float(x: Any, default: float = 0.0) -> float:
    """Convert to finite float; otherwise return default."""
    try:
        v = float(x)
        return v if math.isfinite(v) else float(default)
    except Exception:
        return float(default)


def _bar_get(bar: dict[str, Any], key: str, default: float = 0.0) -> float:
    """Deterministic lookup with alternates. Returns finite float."""
    alts = _KEY_ALTS.get(key, (key,))
    for k in alts:
        if k in bar:
            return _finite_float(bar.get(k), default)
    return _finite_float(default, default)


def _closes(bars: Sequence[dict[str, Any]]) -> list[float]:
    return [_bar_get(b, "c", 0.0) for b in bars]


def _vols(bars: Sequence[dict[str, Any]]) -> list[float]:
    return [_bar_get(b, "v", 0.0) for b in bars]


def _last_valid_price(closes: Sequence[float]) -> float:
    for idx in range(len(closes) - 1, -1, -1):
        px = float(closes[idx])
        if px > 0.0 and math.isfinite(px):
            return px
    return 0.0


def _count_positive_finite(xs: Sequence[float]) -> int:
    n = 0
    for x in xs:
        if math.isfinite(x) and x > 0.0:
            n += 1
    return n


# ----------------------------- Basics -----------------------------------------


def sma(values: Sequence[float], period: int) -> float:
    p = int(period)
    if p <= 0 or len(values) < p:
        return NAN
    window = values[-p:]
    s = 0.0
    for x in window:
        xf = float(x)
        if not math.isfinite(xf):
            return NAN
        s += xf
    return s / float(p)


def ema(values: Sequence[float], period: int) -> float:
    """
    EMA of a *value series* (NOT bars). Returns last EMA.
    Deterministic and pure.
    """
    p = int(period)
    if p <= 0 or len(values) < max(2, p):
        return NAN

    # Find a stable starting point (first finite)
    start_idx = None
    for i, x in enumerate(values):
        xf = float(x)
        if math.isfinite(xf):
            start_idx = i
            break
    if start_idx is None:
        return NAN

    alpha = 2.0 / (p + 1.0)
    e = float(values[start_idx])
    for x in values[start_idx + 1 :]:
        xf = float(x)
        if not math.isfinite(xf):
            return NAN
        e = alpha * xf + (1.0 - alpha) * e
    return float(e)


def zscore(values: Sequence[float], window: int = 50) -> float:
    w = int(window)
    if w <= 1 or len(values) < w:
        return NAN
    xs = [float(v) for v in values[-w:]]
    if any(not math.isfinite(v) for v in xs):
        return NAN

    mu = sum(xs) / float(w)
    var = 0.0
    for x in xs:
        var += (x - mu) ** 2
    var /= float(w)

    sd = math.sqrt(max(0.0, var))
    if sd <= 1e-18:
        return 0.0
    return float((xs[-1] - mu) / sd)


# ----------------------------- Candles / Vol ---------------------------------


def atr(bars: Sequence[dict[str, Any]], period: int = 14) -> float:
    """
    ATR in price units (same as close).
    Returns NaN if insufficient valid bars.
    """
    p = int(period)
    if p <= 1 or len(bars) < p + 1:
        return NAN

    trs: list[float] = []
    prev_close = _bar_get(bars[0], "c", 0.0)

    for idx in range(1, len(bars)):
        high_px = _bar_get(bars[idx], "h", 0.0)
        low_px = _bar_get(bars[idx], "l", 0.0)
        close_px = _bar_get(bars[idx], "c", 0.0)

        # Skip broken bars deterministically (no raise)
        if prev_close <= 0.0 or close_px <= 0.0:
            prev_close = close_px
            continue

        tr = max(high_px - low_px, abs(high_px - prev_close), abs(low_px - prev_close))
        if math.isfinite(tr) and tr >= 0.0:
            trs.append(float(tr))
        prev_close = close_px

    if len(trs) < p:
        return NAN
    return float(sum(trs[-p:]) / float(p))


def atr_pct(bars: Sequence[dict[str, Any]], period: int = 14) -> float:
    """ATR as fraction of last close. Example: 0.02 => ~2%."""
    a = atr(bars, period=period)
    if not math.isfinite(a):
        return NAN
    last_close = _bar_get(bars[-1], "c", 0.0) if bars else 0.0
    if last_close <= 0.0:
        return NAN
    return float(a) / float(last_close)


def rsi(bars: Sequence[dict[str, Any]], period: int = 14) -> float:
    """
    RSI(0..100) on close series with Wilder smoothing.
    Returns NaN if insufficient positive closes.
    """
    p = int(period)
    if p <= 1 or len(bars) < p + 1:
        return NAN

    closes = _closes(bars)
    if _count_positive_finite(closes[-(p + 1) :]) < (p + 1):
        return NAN

    gains = 0.0
    losses = 0.0

    for idx in range(1, p + 1):
        d = closes[idx] - closes[idx - 1]
        if d >= 0.0:
            gains += d
        else:
            losses -= d

    avg_gain = gains / float(p)
    avg_loss = losses / float(p)

    for idx in range(p + 1, len(closes)):
        d = closes[idx] - closes[idx - 1]
        gain = d if d > 0.0 else 0.0
        loss = -d if d < 0.0 else 0.0
        avg_gain = (avg_gain * (p - 1) + gain) / float(p)
        avg_loss = (avg_loss * (p - 1) + loss) / float(p)

    if avg_loss <= 1e-18:
        return 100.0
    rs = avg_gain / avg_loss
    return float(100.0 - (100.0 / (1.0 + rs)))


def donchian(bars: Sequence[dict[str, Any]], period: int = 20) -> tuple[float, float, float]:
    """Donchian channel on highs/lows. Returns (upper, lower, mid)."""
    p = int(period)
    if p <= 1 or len(bars) < p:
        return NAN, NAN, NAN
    sub = bars[-p:]
    upper = max(_bar_get(b, "h", 0.0) for b in sub)
    lower = min(_bar_get(b, "l", 0.0) for b in sub)
    mid = 0.5 * (upper + lower)
    return float(upper), float(lower), float(mid)


def vwap(bars: Sequence[dict[str, Any]], period: int = 20) -> float:
    """
    VWAP over last N bars using typical price * volume / sum(volume).
    If volume is absent/zero, degrade to SMA(close).
    """
    p = int(period)
    if p <= 1 or len(bars) < p:
        return NAN

    sub = bars[-p:]
    pv_sum = 0.0
    v_sum = 0.0

    for b in sub:
        high_px = _bar_get(b, "h", 0.0)
        low_px = _bar_get(b, "l", 0.0)
        close_px = _bar_get(b, "c", 0.0)
        vol_px = _bar_get(b, "v", 0.0)

        if close_px <= 0.0:
            continue
        typical = (high_px + low_px + close_px) / 3.0
        if vol_px > 0.0 and typical > 0.0:
            pv_sum += typical * vol_px
            v_sum += vol_px

    if v_sum <= 1e-18:
        closes = _closes(sub)
        return sma(closes, len(closes)) if closes else NAN

    return float(pv_sum / v_sum)


def volume_burst(
    bars: Sequence[dict[str, Any]],
    fast_window: int = 30,
    slow_window: int = 20,
) -> float:
    """
    Ratio of avg volume in last fast_window to avg volume in prior slow_window.
    """
    fw = int(fast_window)
    sw = int(slow_window)
    if fw <= 1 or sw <= 1 or len(bars) < fw + sw:
        return NAN

    vols = _vols(bars)
    fast = vols[-fw:]
    slow = vols[-(fw + sw) : -fw]

    fast_avg = sum(fast) / float(fw)
    slow_avg = sum(slow) / float(sw)
    if slow_avg <= 1e-18:
        return NAN
    return float(fast_avg / slow_avg)


def liquidity_usd_proxy(bars: Sequence[dict[str, Any]], window: int = 10) -> float:
    """
    Simple notional liquidity proxy: avg(close * volume) over last window bars.
    (If close isn't USD, it's still a relative proxy.)
    """
    w = int(window)
    if w <= 1 or len(bars) < w:
        return 0.0

    sub = bars[-w:]
    acc = 0.0
    for b in sub:
        close_px = _bar_get(b, "c", 0.0)
        vol_px = _bar_get(b, "v", 0.0)
        if close_px > 0.0 and vol_px > 0.0:
            acc += close_px * vol_px
    return float(acc / float(w))


def _linear_slope(xs: Sequence[float]) -> float:
    """OLS slope of xs vs index(0..n-1). Returns 0 if degenerate."""
    n = len(xs)
    if n < 3:
        return 0.0

    xbar = (n - 1) * 0.5
    ybar = sum(xs) / float(n)

    num = 0.0
    den = 0.0
    for idx, y in enumerate(xs):
        dx = idx - xbar
        num += dx * (y - ybar)
        den += dx * dx

    if den <= 1e-18:
        return 0.0
    return float(num / den)


def regime_trend_vs_range(bars: Sequence[dict[str, Any]], window: int = 48) -> float:
    """
    Returns [0..1] trendingness score.
    Heuristic: |slope| relative to realized volatility of price diffs.
    """
    w = int(window)
    if w <= 10 or len(bars) < w:
        return 0.0

    closes = _closes(bars[-w:])
    last = _last_valid_price(closes)
    if last <= 0.0:
        return 0.0

    slope = _linear_slope(closes)

    diffs: list[float] = []
    for idx in range(1, len(closes)):
        diffs.append(closes[idx] - closes[idx - 1])

    if len(diffs) < 5:
        return 0.0

    mu = sum(diffs) / float(len(diffs))
    var = 0.0
    for d in diffs:
        var += (d - mu) ** 2
    var /= float(len(diffs))
    sd = math.sqrt(max(0.0, var))

    if sd <= 1e-18:
        return 0.0

    ratio = abs(slope) / (sd + 1e-12)
    return float(max(0.0, min(1.0, ratio / 3.0)))


# ----------------------------- Feature pack -----------------------------------


def pack_features(bars: Sequence[dict[str, Any]]) -> Features:
    """
    Canonical feature dict for daytrader models (alpha.py) and any light ML.

    Stable numeric keys (NaN for unavailable):
      - last
      - ema_fast, ema_slow
      - rsi14
      - atr_pct
      - vwap20
      - vol_burst
      - liq_usd_10
      - regime  (0..1 trendingness)
      - mom_5, mom_15, mom_30  (pct returns)
      - z_close_50
    """
    feats: Features = {}
    if not bars:
        return feats

    closes = _closes(bars)
    last = _last_valid_price(closes)
    feats["last"] = float(last) if last > 0.0 else NAN

    feats["ema_fast"] = ema(closes, 12)
    feats["ema_slow"] = ema(closes, 48)
    feats["rsi14"] = rsi(bars, 14)
    feats["atr_pct"] = atr_pct(bars, 14)
    feats["vwap20"] = vwap(bars, 20)
    feats["vol_burst"] = volume_burst(bars, 30, 20)
    feats["liq_usd_10"] = liquidity_usd_proxy(bars, 10)
    feats["regime"] = regime_trend_vs_range(bars, 48)

    def _mom(n: int) -> float:
        n_int = int(n)
        if len(closes) < n_int + 1:
            return NAN
        p0 = float(closes[-(n_int + 1)])
        p1 = float(closes[-1])
        if p0 <= 0.0 or p1 <= 0.0:
            return NAN
        return float((p1 / p0 - 1.0) * 100.0)

    feats["mom_5"] = _mom(5)
    feats["mom_15"] = _mom(15)
    feats["mom_30"] = _mom(30)
    feats["z_close_50"] = zscore(closes, 50)
    return feats


# =============================================================================
# Optional legacy decision helpers (kept to avoid breaking imports)
# =============================================================================


@dataclass
class StrategyConfig:
    min_liquidity_usd: float = _env_float("DT_MIN_LIQ_USD", 500.0, min_value=0.0)
    max_atr_pct: float = _env_float("DT_MAX_ATR_PCT", 0.30, min_value=0.0, max_value=10.0)
    min_vol_burst: float = _env_float("DT_MIN_VOL_BURST", 1.25, min_value=0.0)

    rsi_buy: float = _env_float("DT_RSI_BUY", 42.0, min_value=0.0, max_value=100.0)
    rsi_sell: float = _env_float("DT_RSI_SELL", 65.0, min_value=0.0, max_value=100.0)

    ema_fast: int = _env_int("DT_EMA_FAST", 12, min_value=2, max_value=500)
    ema_slow: int = _env_int("DT_EMA_SLOW", 48, min_value=2, max_value=2000)
    donchian_period: int = _env_int("DT_DONCHIAN", 20, min_value=2, max_value=2000)

    base_size_frac: float = _env_float("DT_BASE_SIZE_FRAC", 0.15, min_value=0.0, max_value=1.0)
    max_size_frac: float = _env_float("DT_MAX_SIZE_FRAC", 0.35, min_value=0.0, max_value=1.0)

    tp_atr_mult: float = _env_float("DT_TP_ATR_MULT", 1.5, min_value=0.0, max_value=100.0)
    sl_atr_mult: float = _env_float("DT_SL_ATR_MULT", 1.0, min_value=0.0, max_value=100.0)

    alpha_weight: float = _env_float("DT_ALPHA_WEIGHT", 0.55, min_value=0.0, max_value=1.0)
    min_alpha_score: float = _env_float("DT_MIN_ALPHA", 0.55, min_value=0.0, max_value=1.0)
    min_rule_score: float = _env_float("DT_MIN_RULE", 0.10, min_value=0.0, max_value=1.0)

    min_minutes_between_trades: int = _env_int(
        "DT_MIN_MINS_BETWEEN_TRADES", 5, min_value=0, max_value=10_000
    )


@dataclass
class TradeIntent:
    side: str  # "buy" | "sell" | "hold"
    size_frac: float  # 0..1
    stop: float | None
    take_profit: float | None
    reason: str
    confidence: float  # 0..1


class RulesEngineStrategy:
    """
    Lightweight rule+alpha blend producing TradeIntent.
    alpha_score can be:
      - float in [0,1]
      - callable(features)->float
    """

    def __init__(self, cfg: StrategyConfig | None = None):
        self.cfg = cfg or StrategyConfig()
        self._last_trade_ts: dict[str, float] = {}

    def _cooldown_ok(self, symbol: str) -> bool:
        last_ts = self._last_trade_ts.get(symbol, 0.0)
        return (time.time() - last_ts) >= self.cfg.min_minutes_between_trades * 60

    def rule_score(self, bars: Sequence[dict[str, Any]]) -> tuple[float, Features, str]:
        """
        Returns (score in [-1,1], features, why_string)
        """
        if len(bars) < 50:
            return 0.0, {}, "insufficient_bars"

        feats = pack_features(bars)

        liq = feats.get("liq_usd_10", 0.0)
        a_pct = feats.get("atr_pct", NAN)
        vb = feats.get("vol_burst", NAN)
        ef = feats.get("ema_fast", NAN)
        es = feats.get("ema_slow", NAN)
        r = feats.get("rsi14", NAN)
        last = feats.get("last", NAN)

        if liq < self.cfg.min_liquidity_usd:
            return 0.0, feats, f"low_liquidity:{liq:.0f}"
        if math.isfinite(a_pct) and a_pct > self.cfg.max_atr_pct:
            return 0.0, feats, f"atr_pct_high:{a_pct:.3f}"
        if math.isfinite(vb) and vb < self.cfg.min_vol_burst:
            return 0.0, feats, f"no_interest:{vb:.2f}"

        score = 0.0

        if math.isfinite(ef) and math.isfinite(es):
            score += 0.45 if ef > es else -0.45

        upper, lower, _mid = donchian(bars, self.cfg.donchian_period)
        if math.isfinite(last) and math.isfinite(upper) and math.isfinite(lower):
            if last > upper:
                score += 0.35
            elif last < lower:
                score -= 0.35

        if math.isfinite(r):
            if score > 0.0 and r <= self.cfg.rsi_buy:
                score += 0.20
            elif score < 0.0 and r >= self.cfg.rsi_sell:
                score -= 0.20

        score = max(-1.0, min(1.0, score))
        return float(score), feats, "ok"

    def decide(
        self,
        symbol: str,
        bars: Sequence[dict[str, Any]],
        alpha_score: float | Callable[[Features], float] | None = None,
    ) -> TradeIntent | None:
        rs, feats, why = self.rule_score(bars)
        if why != "ok":
            return None
        if not self._cooldown_ok(symbol):
            return None

        a = 0.5
        try:
            if callable(alpha_score):
                a = float(alpha_score(feats))
            elif alpha_score is not None:
                a = float(alpha_score)
        except Exception:
            a = 0.5
        a = max(0.0, min(1.0, a))

        ens = (1.0 - self.cfg.alpha_weight) * rs + self.cfg.alpha_weight * (2.0 * a - 1.0)
        conf = min(1.0, abs(ens))

        side = "hold"
        if ens >= 0.25:
            side = "buy"
        elif ens <= -0.25:
            side = "sell"
        else:
            return None

        if side == "buy":
            if a < self.cfg.min_alpha_score:
                return None
            if rs < self.cfg.min_rule_score:
                return None

        last_close = _bar_get(bars[-1], "c", 0.0)
        a_price = atr(bars, 14)
        if not math.isfinite(a_price) or last_close <= 0.0:
            return None

        stop = (
            (last_close - self.cfg.sl_atr_mult * a_price)
            if side == "buy"
            else (last_close + self.cfg.sl_atr_mult * a_price)
        )
        tp = (
            (last_close + self.cfg.tp_atr_mult * a_price)
            if side == "buy"
            else (last_close - self.cfg.tp_atr_mult * a_price)
        )

        size = self.cfg.base_size_frac + (self.cfg.max_size_frac - self.cfg.base_size_frac) * conf
        size = max(0.0, min(self.cfg.max_size_frac, size))

        reason = (
            f"ens={ens:.2f} rule={rs:.2f} alpha={a:.2f} "
            f"atr%={feats.get('atr_pct', NAN):.3f} rsi={feats.get('rsi14', NAN):.1f}"
        )

        self._last_trade_ts[symbol] = time.time()
        return TradeIntent(
            side=side,
            size_frac=float(size),
            stop=float(stop),
            take_profit=float(tp),
            reason=reason,
            confidence=float(conf),
        )


__all__ = [
    "RulesEngineStrategy",
    "StrategyConfig",
    "TradeIntent",
    "atr",
    "atr_pct",
    "donchian",
    "ema",
    "liquidity_usd_proxy",
    "pack_features",
    "regime_trend_vs_range",
    "rsi",
    "sma",
    "volume_burst",
    "vwap",
    "zscore",
]
