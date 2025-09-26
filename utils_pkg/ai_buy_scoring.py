# utils_pkg/ai_buy_scoring.py
# -*- coding: utf-8 -*-
"""
AI Buy Scoring — v2
-------------------
Backwards-compatible scorer used by sniper.py / reinvestor.py:
    from utils_pkg.ai_buy_scoring import Candidate, pass_gate
    ok, score, why = pass_gate(Candidate(...))

This module keeps your existing fields and adds optional, richer market
signals (order flow, orderbook, verification, momentum). All thresholds
and weights may be tuned via environment variables (see bottom of file
for a quick env reference).

Key ideas
- Hard filters first (fast reject): pool age, liquidity, impact, top10
  concentration, 5m volatility, honeypot/dev risks, etc.
- Soft score next (0..1): normalized mix of liquidity, depth, trend,
  orderflow, whales, token verification, etc.
- Gate: accept when score >= AI_SCORE_GATE (default 0.58).

This file avoids external deps (only stdlib).
"""
from __future__ import annotations

import os
import math
from dataclasses import dataclass, field, asdict
from typing import Any, Dict, Optional, Tuple, List

__all__ = [
    "Candidate",
    "pass_gate",
    "score_only",
    "from_pool_snapshot",
]

# -------------------------------
# Utilities
# -------------------------------

def _env_float(name: str, default: float) -> float:
    v = os.getenv(name)
    if v is None or v == "":
        return default
    try:
        return float(v)
    except Exception:
        return default

def _env_int(name: str, default: int) -> int:
    v = os.getenv(name)
    if v is None or v == "":
        return default
    try:
        return int(v)
    except Exception:
        return default

def _env_bool(name: str, default: bool) -> bool:
    v = os.getenv(name)
    if v is None or v == "":
        return default
    v = v.strip().lower()
    if v in ("1", "true", "yes", "y", "on"):
        return True
    if v in ("0", "false", "no", "n", "off"):
        return False
    return default

def _clamp(x: float, lo: float = 0.0, hi: float = 1.0) -> float:
    return max(lo, min(hi, x))

def _safe_div(n: float, d: float, fallback: float = 0.0) -> float:
    try:
        if d == 0:
            return fallback
        return n / d
    except Exception:
        return fallback

def _nz(x: Optional[float], fallback: float = 0.0) -> float:
    try:
        return float(x) if x is not None else fallback
    except Exception:
        return fallback

# -------------------------------
# Candidate schema
# -------------------------------

@dataclass
class Candidate:
    # Required / commonly supplied
    mint: str
    symbol: str

    pool_age_min: float                      # minutes since pool creation
    liq_usd: float                           # current pool liquidity (USD)
    impact_1k_bps: float                     # price impact (bps) for $1k swap
    vol5m_pct: float                         # 5m realized volatility (%) or 5m % change magnitude
    top10_pct: float                         # % of supply held by top 10 holders (0..100)
    honeypot_flag: bool                      # detected honeypot / denial risks
    whale_touches_1h: int                    # # of unique whale wallets touched in last hour

    # Optional enrichments (all default-safe)
    verified: Optional[bool] = None          # aggregator token verification
    liq_locked: Optional[bool] = None        # lp locked flag
    mint_renounced: Optional[bool] = None    # renounced mint authority
    freeze_auth_disabled: Optional[bool] = None  # freeze authority disabled

    dev_wallet_pct: Optional[float] = None   # % supply in developer cluster

    # Momentum / returns (positive small drift is preferred)
    ret5m_pct: Optional[float] = None        # 5m price return (%)
    ret15m_pct: Optional[float] = None       # 15m price return (%)

    # Order flow (last 1m)
    orderflow_buys_1m: Optional[int] = None
    orderflow_sells_1m: Optional[int] = None
    orderflow_buy_usd_1m: Optional[float] = None
    orderflow_sell_usd_1m: Optional[float] = None

    # Orderbook depth snapshot (USD)
    orderbook_bid_usd: Optional[float] = None
    orderbook_ask_usd: Optional[float] = None

    # External safety signals
    rugpull_reports: Optional[int] = None    # # of community rug reports in 24h

    # For future extensibility
    meta: Dict[str, Any] = field(default_factory=dict)


# -------------------------------
# Environment-driven thresholds & weights (sane defaults)
# -------------------------------

# Hard filters
AI_MIN_POOL_AGE_MIN       = _env_float("AI_MIN_POOL_AGE_MIN", 5.0)     # too fresh => fail
AI_MIN_LIQ_USD            = _env_float("AI_MIN_LIQ_USD", 15000.0)
AI_MAX_IMPACT_1K_BPS      = _env_float("AI_MAX_IMPACT_1K_BPS", 125.0)  # slippage guard
AI_MAX_TOP10_PCT          = _env_float("AI_MAX_TOP10_PCT", 70.0)
AI_MIN_VOL5M_PCT          = _env_float("AI_MIN_VOL5M_PCT", 1.0)        # must be moving a bit
AI_MAX_DEV_WALLET_PCT     = _env_float("AI_MAX_DEV_WALLET_PCT", 15.0)  # >15% dev control => fail
AI_MIN_WHALE_TOUCHES_1H   = _env_int("AI_MIN_WHALE_TOUCHES_1H", 0)     # optional

AI_REQUIRE_LIQ_LOCK       = _env_bool("AI_REQUIRE_LIQ_LOCK", False)
AI_REQUIRE_MINT_RENOUNCED = _env_bool("AI_REQUIRE_MINT_RENOUNCED", False)
AI_REQUIRE_NO_FREEZE      = _env_bool("AI_REQUIRE_NO_FREEZE", False)

# Soft score gating
AI_SCORE_GATE             = _env_float("AI_SCORE_GATE", 0.58)

# Weights (sum doesn't have to be 1; will be clamped 0..1 at the end)
W_POOL_AGE    = _env_float("AI_W_POOL_AGE", 0.08)
W_LIQ         = _env_float("AI_W_LIQ", 0.18)
W_IMPACT      = _env_float("AI_W_IMPACT", 0.12)
W_VOL5M       = _env_float("AI_W_VOL5M", 0.08)
W_TOP10       = _env_float("AI_W_TOP10", 0.10)
W_WHALES      = _env_float("AI_W_WHALES", 0.10)
W_TREND       = _env_float("AI_W_TREND", 0.12)
W_FLOW        = _env_float("AI_W_FLOW", 0.10)
W_BOOK        = _env_float("AI_W_BOOK", 0.07)
W_SAFETY      = _env_float("AI_W_SAFETY", 0.05)

# Bonuses / penalties
BONUS_VERIFIED         = _env_float("AI_BONUS_VERIFIED", 0.03)
PENALTY_UNVERIFIED     = _env_float("AI_PENALTY_UNVERIFIED", -0.05)
BONUS_LIQ_LOCK         = _env_float("AI_BONUS_LIQ_LOCK", 0.03)
BONUS_RENOUNCED        = _env_float("AI_BONUS_RENOUNCED", 0.02)
BONUS_NO_FREEZE        = _env_float("AI_BONUS_NO_FREEZE", 0.02)
PENALTY_RUG_REPORT     = _env_float("AI_PENALTY_RUG_REPORT", -0.02)  # per report, capped

WHALE_TOUCH_BONUS      = _env_float("AI_WHALE_TOUCH_BONUS", 0.03)     # per touch (small), capped
WHALE_TOUCH_BONUS_CAP  = _env_float("AI_WHALE_TOUCH_BONUS_CAP", 0.15)

RUG_REPORT_CAP         = _env_int("AI_RUG_REPORT_CAP", 5)

# Normalization anchors
# (saturate by these levels; tweak to your market regime)
ANCHOR_POOL_AGE   = _env_float("AI_ANCHOR_POOL_AGE", 120.0)      # 2 hours saturates
ANCHOR_LIQ        = _env_float("AI_ANCHOR_LIQ", 100_000.0)       # $100k saturates
ANCHOR_IMPACT_BPS = _env_float("AI_ANCHOR_IMPACT_BPS", 150.0)    # 150 bps = score 0
ANCHOR_VOL5M      = _env_float("AI_ANCHOR_VOL5M", 5.0)           # 5% vol saturates
ANCHOR_TOP10_LO   = _env_float("AI_ANCHOR_TOP10_GOOD", 20.0)     # <=20% is excellent
ANCHOR_TOP10_HI   = _env_float("AI_ANCHOR_TOP10_BAD", 90.0)      # >=90% is terrible

ANCHOR_FLOW_IMB   = _env_float("AI_ANCHOR_FLOW_IMB", 0.65)       # 65% buys ideal-ish
ANCHOR_BOOK_IMB   = _env_float("AI_ANCHOR_BOOK_IMB", 0.60)       # 60% bid share

# Momentum clamps to avoid parabolic chase/knife catching
RET5M_POS_CEIL    = _env_float("AI_RET5M_POS_CEIL", 9.0)         # >+9% in 5m => overheat
RET5M_NEG_FLOOR   = _env_float("AI_RET5M_NEG_FLOOR", -9.0)       # <-9% => catching knife
RET15M_POS_CEIL   = _env_float("AI_RET15M_POS_CEIL", 18.0)       # >+18% in 15m => overheat
RET15M_NEG_FLOOR  = _env_float("AI_RET15M_NEG_FLOOR", -18.0)

# -------------------------------
# Hard filters
# -------------------------------

def _hard_filters(c: Candidate) -> Tuple[bool, List[str]]:
    reasons: List[str] = []

    if c.pool_age_min < AI_MIN_POOL_AGE_MIN:
        reasons.append(f"pool too new: {c.pool_age_min:.1f}m < {AI_MIN_POOL_AGE_MIN:.1f}m")

    if c.liq_usd < AI_MIN_LIQ_USD:
        reasons.append(f"liq too low: ${c.liq_usd:,.0f} < ${AI_MIN_LIQ_USD:,.0f}")

    if c.impact_1k_bps > AI_MAX_IMPACT_1K_BPS:
        reasons.append(f"impact high: {c.impact_1k_bps:.0f}bps > {AI_MAX_IMPACT_1K_BPS:.0f}bps")

    if c.top10_pct > AI_MAX_TOP10_PCT:
        reasons.append(f"top10 too concentrated: {c.top10_pct:.1f}% > {AI_MAX_TOP10_PCT:.1f}%")

    if c.vol5m_pct < AI_MIN_VOL5M_PCT:
        reasons.append(f"too quiet: vol5m {c.vol5m_pct:.2f}% < {AI_MIN_VOL5M_PCT:.2f}%")

    if c.honeypot_flag:
        reasons.append("honeypot risk")

    if c.dev_wallet_pct is not None and c.dev_wallet_pct > AI_MAX_DEV_WALLET_PCT:
        reasons.append(f"dev wallet too big: {c.dev_wallet_pct:.1f}% > {AI_MAX_DEV_WALLET_PCT:.1f}%")

    if AI_MIN_WHALE_TOUCHES_1H > 0 and c.whale_touches_1h < AI_MIN_WHALE_TOUCHES_1H:
        reasons.append(f"not enough whale touches: {c.whale_touches_1h} < {AI_MIN_WHALE_TOUCHES_1H}")

    if AI_REQUIRE_LIQ_LOCK and not bool(c.liq_locked):
        reasons.append("liquidity not locked")

    if AI_REQUIRE_MINT_RENOUNCED and not bool(c.mint_renounced):
        reasons.append("mint not renounced")

    if AI_REQUIRE_NO_FREEZE and not bool(c.freeze_auth_disabled):
        reasons.append("freeze authority enabled")

    return (len(reasons) == 0, reasons)

# -------------------------------
# Soft scoring
# -------------------------------

def _score_pool_age(mins: float) -> float:
    return _clamp(mins / ANCHOR_POOL_AGE)

def _score_liq(usd: float) -> float:
    return _clamp(usd / ANCHOR_LIQ)

def _score_impact(bps: float) -> float:
    # 0 bps => 1.0, ANCHOR_IMPACT_BPS => 0.0
    s = 1.0 - _clamp(bps / ANCHOR_IMPACT_BPS)
    return _clamp(s, 0.0, 1.0)

def _score_vol5m(vol_pct: float) -> float:
    # More vol (to a point) is better for day-trading entry opportunities
    return _clamp(vol_pct / ANCHOR_VOL5M)

def _score_top10(concentration_pct: float) -> float:
    # 20% => 1.0, 90% => 0.0 (linear between anchors)
    x = concentration_pct
    if x <= ANCHOR_TOP10_LO:
        return 1.0
    if x >= ANCHOR_TOP10_HI:
        return 0.0
    return _clamp(1.0 - (x - ANCHOR_TOP10_LO) / (ANCHOR_TOP10_HI - ANCHOR_TOP10_LO))

def _score_whales(touches: int) -> float:
    # Base whale component as weight; extra additive bonus handled separately
    return _clamp(touches / 5.0)  # 5+ touches => saturated

def _trend_component(ret5m: Optional[float], ret15m: Optional[float]) -> float:
    r5 = _nz(ret5m)
    r15 = _nz(ret15m)
    # Penalize extremes to avoid chasing or knives
    if r5 >= RET5M_POS_CEIL or r15 >= RET15M_POS_CEIL:
        return 0.2
    if r5 <= RET5M_NEG_FLOOR or r15 <= RET15M_NEG_FLOOR:
        return 0.2
    # Prefer mild positive drift
    # Map -5..+5% to 0..1 softly
    def softstep(r: float, width: float = 5.0) -> float:
        return _clamp((r + width) / (2 * width))
    s5 = softstep(r5, 6.0)
    s15 = softstep(r15, 10.0)
    return _clamp(0.6 * s5 + 0.4 * s15)

def _flow_component(buys: Optional[int], sells: Optional[int],
                    buy_usd: Optional[float], sell_usd: Optional[float]) -> float:
    b = float(buys or 0)
    s = float(sells or 0)
    bu = _nz(buy_usd)
    su = _nz(sell_usd)
    # Mix count and notional imbalance
    cnt_total = b + s
    not_total = bu + su
    cnt_imb = _safe_div(b, cnt_total, 0.5)   # share of buys
    not_imb = _safe_div(bu, not_total, 0.5)  # share of buy notional
    # Blend and center around ANCHOR_FLOW_IMB
    share = 0.5 * cnt_imb + 0.5 * not_imb
    # Map 0..1 -> 0..1 with peak near ANCHOR_FLOW_IMB
    # simple tent function with top at anchor (prefer 0.6-0.7)
    diff = abs(share - ANCHOR_FLOW_IMB)
    return _clamp(1.0 - diff / max(ANCHOR_FLOW_IMB, 1 - ANCHOR_FLOW_IMB))

def _book_component(bid_usd: Optional[float], ask_usd: Optional[float]) -> float:
    bid = _nz(bid_usd)
    ask = _nz(ask_usd)
    tot = bid + ask
    if tot <= 0:
        return 0.5
    bid_share = bid / tot
    # prefer >= 0.6 bid share
    diff = abs(bid_share - ANCHOR_BOOK_IMB)
    return _clamp(1.0 - diff / max(ANCHOR_BOOK_IMB, 1 - ANCHOR_BOOK_IMB))

def _safety_component(verified: Optional[bool],
                      liq_locked: Optional[bool],
                      mint_renounced: Optional[bool],
                      freeze_disabled: Optional[bool],
                      rug_reports: Optional[int]) -> Tuple[float, float]:
    """Return (base_safety_score 0..1, additive_bonus in [-, +])"""
    base = 0.5
    bonus = 0.0

    if verified is True:
        bonus += BONUS_VERIFIED
        base += 0.1
    elif verified is False:
        bonus += PENALTY_UNVERIFIED
        base -= 0.1

    if liq_locked is True:
        bonus += BONUS_LIQ_LOCK
        base += 0.1

    if mint_renounced is True:
        bonus += BONUS_RENOUNCED
        base += 0.05

    if freeze_disabled is True:
        bonus += BONUS_NO_FREEZE
        base += 0.05

    rr = int(rug_reports or 0)
    if rr > 0:
        bonus += max(-RUG_REPORT_CAP * abs(PENALTY_RUG_REPORT), rr * PENALTY_RUG_REPORT)
        base -= min(0.2, 0.03 * rr)

    return _clamp(base), bonus

# -------------------------------
# Public API
# -------------------------------

def score_only(c: Candidate) -> Dict[str, Any]:
    """Return detailed scoring breakdown WITHOUT applying the final gate."""
    hard_ok, hard_reasons = _hard_filters(c)

    parts: Dict[str, float] = {}
    parts["pool_age"] = _score_pool_age(c.pool_age_min)
    parts["liq"]      = _score_liq(c.liq_usd)
    parts["impact"]   = _score_impact(c.impact_1k_bps)
    parts["vol5m"]    = _score_vol5m(c.vol5m_pct)
    parts["top10"]    = _score_top10(c.top10_pct)
    parts["whales"]   = _score_whales(c.whale_touches_1h)
    parts["trend"]    = _trend_component(c.ret5m_pct, c.ret15m_pct)
    parts["flow"]     = _flow_component(c.orderflow_buys_1m, c.orderflow_sells_1m,
                                        c.orderflow_buy_usd_1m, c.orderflow_sell_usd_1m)
    parts["book"]     = _book_component(c.orderbook_bid_usd, c.orderbook_ask_usd)

    safety_base, safety_bonus = _safety_component(
        c.verified, c.liq_locked, c.mint_renounced, c.freeze_auth_disabled, c.rugpull_reports
    )
    parts["safety"] = safety_base

    # Weighted sum
    score = (
        W_POOL_AGE * parts["pool_age"] +
        W_LIQ      * parts["liq"] +
        W_IMPACT   * parts["impact"] +
        W_VOL5M    * parts["vol5m"] +
        W_TOP10    * parts["top10"] +
        W_WHALES   * parts["whales"] +
        W_TREND    * parts["trend"] +
        W_FLOW     * parts["flow"] +
        W_BOOK     * parts["book"] +
        W_SAFETY   * parts["safety"]
    )

    # Add whale-touch micro-bonus & safety bonus
    whale_bonus = min(WHALE_TOUCH_BONUS_CAP, c.whale_touches_1h * WHALE_TOUCH_BONUS)
    score += whale_bonus + safety_bonus

    score = _clamp(score, 0.0, 1.0)

    return {
        "ok_hard": hard_ok,
        "hard_reasons": hard_reasons,
        "parts": parts,
        "whale_bonus": whale_bonus,
        "safety_bonus": safety_bonus,
        "score": score,
    }

def pass_gate(c: Candidate) -> Tuple[bool, float, str]:
    """
    Returns (ok, score, reason).
    ok: bool — True if candidate passes both hard filters and score >= gate.
    score: float — 0..1 total score.
    reason: human-readable reason (why rejected or brief summary when accepted).
    """
    detail = score_only(c)

    if not detail["ok_hard"]:
        return (False, detail["score"], "; ".join(detail["hard_reasons"]))

    score = detail["score"]
    if score >= AI_SCORE_GATE:
        reason = (
            f"pass score={score:.3f} "
            f"(liq={detail['parts']['liq']:.2f}, impact={detail['parts']['impact']:.2f}, "
            f"trend={detail['parts']['trend']:.2f}, flow={detail['parts']['flow']:.2f}, "
            f"book={detail['parts']['book']:.2f}, whales+bonus={detail['parts']['whales']:.2f}+{detail['whale_bonus']:.2f})"
        )
        return (True, score, reason)
    else:
        return (False, score, f"below gate: score {score:.3f} < {AI_SCORE_GATE:.3f}")

# -------------------------------
# Convenience: build from raw dict
# -------------------------------

def from_pool_snapshot(d: Dict[str, Any]) -> Candidate:
    """
    Best-effort builder from a mixed dict (e.g., aggregator feed).
    Keys are optional — missing values fall back gracefully.
    """
    return Candidate(
        mint = d.get("mint") or d.get("token") or d.get("address") or "UNKNOWN",
        symbol = d.get("symbol") or d.get("ticker") or "UNK",

        pool_age_min = _nz(d.get("pool_age_min") or d.get("age_min") or d.get("age_minutes"), 0.0),
        liq_usd = _nz(d.get("liq_usd") or d.get("liquidity_usd") or d.get("liquidity"), 0.0),
        impact_1k_bps = _nz(d.get("impact_1k_bps") or d.get("price_impact_bps") or d.get("impact_bps"), 999.0),
        vol5m_pct = _nz(d.get("vol5m_pct") or d.get("vol_5m_pct") or d.get("volatility_5m_pct"), 0.0),
        top10_pct = _nz(d.get("top10_pct") or d.get("top_10_pct") or d.get("holder_top10_pct"), 100.0),
        honeypot_flag = bool(d.get("honeypot_flag") or d.get("rug_risk") or d.get("hp_flag") or False),
        whale_touches_1h = int(d.get("whale_touches_1h") or d.get("whale_hits_1h") or 0),

        verified = d.get("verified"),
        liq_locked = d.get("liq_locked"),
        mint_renounced = d.get("mint_renounced"),
        freeze_auth_disabled = d.get("freeze_auth_disabled"),

        dev_wallet_pct = _nz(d.get("dev_wallet_pct"), None),

        ret5m_pct = _nz(d.get("ret5m_pct"), None),
        ret15m_pct = _nz(d.get("ret15m_pct"), None),

        orderflow_buys_1m = d.get("orderflow_buys_1m"),
        orderflow_sells_1m = d.get("orderflow_sells_1m"),
        orderflow_buy_usd_1m = _nz(d.get("orderflow_buy_usd_1m"), None),
        orderflow_sell_usd_1m = _nz(d.get("orderflow_sell_usd_1m"), None),

        orderbook_bid_usd = _nz(d.get("orderbook_bid_usd"), None),
        orderbook_ask_usd = _nz(d.get("orderbook_ask_usd"), None),

        rugpull_reports = d.get("rugpull_reports"),

        meta = d.get("meta") or {},
    )

# -------------------------------
# ENV quick reference (optional)
# -------------------------------
# Hard filters:
#   AI_MIN_POOL_AGE_MIN=5
#   AI_MIN_LIQ_USD=15000
#   AI_MAX_IMPACT_1K_BPS=125
#   AI_MAX_TOP10_PCT=70
#   AI_MIN_VOL5M_PCT=1.0
#   AI_MAX_DEV_WALLET_PCT=15
#   AI_MIN_WHALE_TOUCHES_1H=0
#   AI_REQUIRE_LIQ_LOCK=false
#   AI_REQUIRE_MINT_RENOUNCED=false
#   AI_REQUIRE_NO_FREEZE=false
#
# Gate:
#   AI_SCORE_GATE=0.58
#
# Weights:
#   AI_W_POOL_AGE=0.08
#   AI_W_LIQ=0.18
#   AI_W_IMPACT=0.12
#   AI_W_VOL5M=0.08
#   AI_W_TOP10=0.10
#   AI_W_WHALES=0.10
#   AI_W_TREND=0.12
#   AI_W_FLOW=0.10
#   AI_W_BOOK=0.07
#   AI_W_SAFETY=0.05
#
# Bonuses/Penalties:
#   AI_BONUS_VERIFIED=0.03
#   AI_PENALTY_UNVERIFIED=-0.05
#   AI_BONUS_LIQ_LOCK=0.03
#   AI_BONUS_RENOUNCED=0.02
#   AI_BONUS_NO_FREEZE=0.02
#   AI_PENALTY_RUG_REPORT=-0.02
#   AI_WHALE_TOUCH_BONUS=0.03
#   AI_WHALE_TOUCH_BONUS_CAP=0.15
#   AI_RUG_REPORT_CAP=5
#
# Anchors:
#   AI_ANCHOR_POOL_AGE=120
#   AI_ANCHOR_LIQ=100000
#   AI_ANCHOR_IMPACT_BPS=150
#   AI_ANCHOR_VOL5M=5
#   AI_ANCHOR_TOP10_GOOD=20
#   AI_ANCHOR_TOP10_BAD=90
#   AI_ANCHOR_FLOW_IMB=0.65
#   AI_ANCHOR_BOOK_IMB=0.60
#
# Momentum clamps:
#   AI_RET5M_POS_CEIL=9
#   AI_RET5M_NEG_FLOOR=-9
#   AI_RET15M_POS_CEIL=18
#   AI_RET15M_NEG_FLOOR=-18
