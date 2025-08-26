# daytrader/strategy.py
"""
Decision logic for intraday trading:
- Blends pattern/rule signals with an ML-style AlphaModel score
- Enforces basic guardrails (min liquidity, min volume, ATR sanity)
- Produces actionable intents: {side, size_frac, stop, take_profit, reason}
- Delegates exact share sizing and order type to RiskManager / Broker

Dependencies (internal):
  - daytrader.features: simple TA functions (ema/sma/atr/rsi/vwap/etc)
  - daytrader.alpha.AlphaModel: predict_proba or score on a compact feature dict
  - daytrader.broker.Broker: to query balances or symbol meta as needed
  - (optional) daytrader.risk.RiskManager: for position sizing & exit targets

Bars format: list[dict] with keys: 't','o','h','l','c','v' (floats)
"""

from __future__ import annotations
from typing import List, Dict, Optional, Tuple
from dataclasses import dataclass
import math
import statistics
import time

from .features import (
    ema, sma, atr, atr_pct, rsi, vwap,
    donchian, volume_burst, pack_features, liquidity_usd_proxy, zscore,
)
from .alpha import AlphaModel

# -----------------------------------------------------------------------------
# Types
# -----------------------------------------------------------------------------

Bar = Dict[str, float]

@dataclass
class StrategyConfig:
    # Liquidity & sanity
    min_liquidity_usd: float = 5_000.0           # min avg notional over 10 bars
    max_spread_bps: float = 80.0                 # if you have L2, enforce here
    max_atr_pct: float = 0.25                    # avoid hyper-volatility regimes
    min_vol_burst: float = 1.25                  # require some interest pickup

    # Momentum/mean-rev thresholds
    rsi_buy: float = 42.0                        # buy dip in trend
    rsi_sell: float = 65.0                       # take profits into strength
    ema_fast: int = 12
    ema_slow: int = 48
    donchian_period: int = 20

    # Pattern break filters (simple)
    bull_break_z: float = 0.8                    # z-score above mean for break
    bear_break_z: float = -0.8

    # Sizing guidelines (RiskManager will refine)
    base_size_frac: float = 0.15                 # 15% of capital on base conviction
    max_size_frac: float = 0.35
    scale_in_steps: int = 2

    # Profit targets & stops (RiskManager may override)
    tp_atr_mult: float = 1.5
    sl_atr_mult: float = 1.0

    # Alpha/ensemble blend
    alpha_weight: float = 0.55                   # blend between AI and rules
    min_alpha_score: float = 0.55                # require AI score to pass for buys
    min_rule_score: float = 0.5                  # require rules to be net positive

    # Cooldowns & churn control
    min_minutes_between_trades: int = 5

@dataclass
class TradeIntent:
    side: str                              # "buy" or "sell" (or "flat")
    size_frac: float                       # fraction of equity to deploy (0..1)
    stop: Optional[float]                  # suggested stop price
    take_profit: Optional[float]           # suggested TP price
    reason: str                            # brief reason string
    confidence: float                      # 0..1

# -----------------------------------------------------------------------------
# Utility: Simple pattern detectors (compact, not exhaustive)
# -----------------------------------------------------------------------------

def _is_bull_flag(bars: List[Bar], lookback: int = 30) -> bool:
    """Detects a crude bull flag: impulse up then mild down/sideways consolidation."""
    if len(bars) < lookback:
        return False
    sub = bars[-lookback:]
    closes = [b["c"] for b in sub]
    # impulse check: last close > ema of earlier chunk by a margin
    early = closes[: lookback // 2]
    late = closes[lookback // 2 :]
    if not early or not late:
        return False
    early_ema = ema(early, max(3, len(early)//2))
    late_ema = ema(late, max(3, len(late)//2))
    # rising structure
    if late_ema <= early_ema:
        return False
    # consolidation tightness: stdev of late segment smaller than early
    sd_early = statistics.pstdev(early) if len(early) > 1 else 0.0
    sd_late = statistics.pstdev(late) if len(late) > 1 else 0.0
    return sd_late < sd_early

def _is_bear_flag(bars: List[Bar], lookback: int = 30) -> bool:
    """Inverse of bull flag."""
    if len(bars) < lookback:
        return False
    sub = bars[-lookback:]
    closes = [b["c"] for b in sub]
    early = closes[: lookback // 2]
    late = closes[lookback // 2 :]
    if not early or not late:
        return False
    early_ema = ema(early, max(3, len(early)//2))
    late_ema = ema(late, max(3, len(late)//2))
    if late_ema >= early_ema:
        return False
    sd_early = statistics.pstdev(early) if len(early) > 1 else 0.0
    sd_late = statistics.pstdev(late) if len(late) > 1 else 0.0
    return sd_late < sd_early

def _head_and_shoulders(bars: List[Bar], lookback: int = 50) -> bool:
    """Very rough H&S via local maxima pattern."""
    if len(bars) < lookback:
        return False
    sub = bars[-lookback:]
    highs = [b["h"] for b in sub]
    # find three local maxima
    locs = []
    for i in range(2, len(highs) - 2):
        if highs[i] > highs[i-1] and highs[i] > highs[i-2] and highs[i] > highs[i+1] and highs[i] > highs[i+2]:
            locs.append(i)
    if len(locs) < 3:
        return False
    # pick first three peaks
    a, b, c = locs[0], locs[1], locs[2]
    # shoulder-head-shoulder geometry: head highest
    return highs[b] > highs[a] * 1.02 and highs[b] > highs[c] * 1.02 and abs(highs[a] - highs[c]) / highs[b] < 0.1

def _cup_and_handle(bars: List[Bar], lookback: int = 60) -> bool:
    """Crude cup & handle: rounded bottom then small pullback."""
    if len(bars) < lookback:
        return False
    sub = bars[-lookback:]
    closes = [b["c"] for b in sub]
    low_idx = min(range(len(closes)), key=lambda i: closes[i])
    # require low around the middle
    if low_idx < lookback * 0.25 or low_idx > lookback * 0.75:
        return False
    # left and right lips within ~5%
    if abs(closes[0] - closes[-1]) / max(1e-9, closes[-1]) > 0.05:
        return False
    # handle: last few bars slightly down but shallow
    handle = closes[-5:]
    return len(handle) >= 3 and handle[-1] >= min(handle) * 0.98

# -----------------------------------------------------------------------------
# Strategy
# -----------------------------------------------------------------------------

class RulesEngineStrategy:
    """
    Combines:
      - Trend bias (EMA fast/slow, Donchian breakout)
      - Volatility & liquidity gates (ATR%, vBurst, notional liquidity)
      - Pattern confirmations (flags, H&S, cup/handle)
      - RSI overlays for entry/exit timing
    Produces a continuous score in [-1, +1] and an intent if thresholds pass.
    """

    def __init__(self, cfg: StrategyConfig):
        self.cfg = cfg
        self._last_trade_ts: Dict[str, float] = {}  # symbol -> epoch seconds

    # --------------------------- scoring -------------------------------------

    def rule_score(self, bars: List[Bar]) -> Tuple[float, Dict[str, float]]:
        if len(bars) < 50:
            return 0.0, {"why": "insufficient_bars"}

        u,l,_ = donchian(bars, self.cfg.donchian_period)
        closes = [b["c"] for b in bars]
        last = closes[-1]
        ef = ema(closes, self.cfg.ema_fast)
        es = ema(closes, self.cfg.ema_slow)
        a_pct = atr_pct(bars, 14)
        rb = volume_burst(bars, 30, 20)
        liq = liquidity_usd_proxy(bars, 10)
        r = rsi(bars, 14)

        # gates
        if liq < self.cfg.min_liquidity_usd:
            return 0.0, {"why": f"low_liquidity:{liq:.0f}"}
        if a_pct > self.cfg.max_atr_pct:
            return 0.0, {"why": f"atr_pct_high:{a_pct:.2f}"}
        if not math.isnan(rb) and rb < self.cfg.min_vol_burst:
            return 0.0, {"why": f"no_interest:{rb:.2f}"}

        score = 0.0
        notes = {}

        # Trend bias
        trend = 1.0 if ef > es else -1.0
        score += 0.4 * trend
        notes["trend"] = trend

        # Breakout bias via Donchian
        if last > u:
            score += 0.3
            notes["donchian_break"] = 1.0
        elif last < l:
            score -= 0.3
            notes["donchian_break"] = -1.0
        else:
            notes["donchian_break"] = 0.0

        # RSI timing
        if trend > 0 and r <= self.cfg.rsi_buy:
            score += 0.15
            notes["rsi_timing"] = +1.0
        elif trend < 0 and r >= self.cfg.rsi_sell:
            score -= 0.15
            notes["rsi_timing"] = -1.0
        else:
            notes["rsi_timing"] = 0.0

        # Patterns
        if _is_bull_flag(bars):
            score += 0.15
            notes["bull_flag"] = 1.0
        if _cup_and_handle(bars):
            score += 0.1
            notes["cup_handle"] = 1.0
        if _is_bear_flag(bars):
            score -= 0.15
            notes["bear_flag"] = -1.0
        if _head_and_shoulders(bars):
            score -= 0.15
            notes["head_shoulders"] = -1.0

        # Normalize to [-1,1]
        score = max(-1.0, min(1.0, score))
        notes["liq_usd_10"] = liq
        notes["atr_pct"] = a_pct
        notes["rsi"] = r
        notes["last"] = last
        return score, notes

    # --------------------------- decision ------------------------------------

    def _cooldown_ok(self, symbol: str) -> bool:
        tnow = time.time()
        last = self._last_trade_ts.get(symbol, 0.0)
        if tnow - last < self.cfg.min_minutes_between_trades * 60:
            return False
        return True

    def decide(self, symbol: str, bars: List[Bar], alpha: Optional[AlphaModel]=None) -> Optional[TradeIntent]:
        """
        Returns a TradeIntent or None if no action.
        """
        rule_s, notes = self.rule_score(bars)

        # AI alpha (0..1 buy probability); fallback to 0.5 neutral
        feat = pack_features(bars)
        alpha_s = (alpha.predict_proba(feat) if alpha else 0.5)

        # Ensemble score in [-1,1]
        # map alpha_s in [0,1] to [-1,1] by (2p-1)
        ens = (1.0 - self.cfg.alpha_weight)*rule_s + self.cfg.alpha_weight*(2.0*alpha_s - 1.0)

        # Direction
        side = "buy" if ens > 0 else "sell" if ens < 0 else "flat"
        conf = abs(ens)

        # Basic thresholds
        if side == "buy":
            if alpha_s < self.cfg.min_alpha_score or rule_s < self.cfg.min_rule_score:
                return None
        elif side == "sell":
            # Allow lighter threshold for sells (risk-off)
            if rule_s > -self.cfg.min_rule_score and alpha_s > 0.45:
                return None
        else:
            return None

        if not self._cooldown_ok(symbol):
            return None

        # Stops/TPs from ATR
        last = bars[-1]["c"]
        a = atr(bars, 14)
        sl = (last - self.cfg.sl_atr_mult * a) if side == "buy" else (last + self.cfg.sl_atr_mult * a)
        tp = (last + self.cfg.tp_atr_mult * a) if side == "buy" else (last - self.cfg.tp_atr_mult * a)

        # Size: scale with confidence, bounded
        size = self.cfg.base_size_frac + (self.cfg.max_size_frac - self.cfg.base_size_frac) * conf
        size = max(0.0, min(self.cfg.max_size_frac, size))

        reason = f"ens={ens:.2f} rule={rule_s:.2f} alpha={alpha_s:.2f} rsi={notes.get('rsi'):.1f} atr%={notes.get('atr_pct'):.3f}"
        self._last_trade_ts[symbol] = time.time()
        return TradeIntent(side=side, size_frac=size, stop=sl, take_profit=tp, reason=reason, confidence=conf)

# -----------------------------------------------------------------------------
# Director: orchestrate a full decision pass and optional order emission
# -----------------------------------------------------------------------------

class StrategyDirector:
    """
    Thin wrapper that:
      - pulls recent bars
      - runs RulesEngineStrategy + AlphaModel
      - optionally asks RiskManager for final sizing/validation
      - returns an actionable intent

    Example:
        director = StrategyDirector(broker, risk_mgr, cfg)
        intent = director.evaluate("SOL-USD", feed.bars("SOL-USD", "1m"))
        if intent: broker.place(intent, symbol)
    """
    def __init__(self, broker, risk_manager=None, cfg: Optional[StrategyConfig]=None, alpha: Optional[AlphaModel]=None):
        self.broker = broker
        self.risk_manager = risk_manager
        self.cfg = cfg or StrategyConfig()
        self.alpha = alpha or AlphaModel()  # default neutral model

        self.rules = RulesEngineStrategy(self.cfg)

    def evaluate(self, symbol: str, bars: List[Bar]) -> Optional[TradeIntent]:
        if not bars or len(bars) < 50:
            return None
        intent = self.rules.decide(symbol, bars, self.alpha)
        if not intent:
            return None

        # Let RiskManager adjust (position sizing, hard risk checks)
        if self.risk_manager:
            intent = self.risk_manager.refine_intent(symbol, bars, intent)
        return intent

    def act(self, symbol: str, bars: List[Bar]) -> Optional[str]:
        """
        Complete decision + execution loop.
        Returns broker order id or tx signature if placed, else None.
        """
        intent = self.evaluate(symbol, bars)
        if not intent:
            return None
        try:
            order_id = self.broker.place_market(
                symbol=symbol,
                side=intent.side,
                size_frac=intent.size_frac,
                stop=intent.stop,
                take_profit=intent.take_profit,
                client_tag=f"dt_{intent.reason[:40]}"
            )
            return order_id
        except Exception as e:
            # Fail closed — do not place partials on error
            print(f"[strategy] place error: {e}")
            return None
