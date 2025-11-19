# utils_pkg/pair_filters.py
from __future__ import annotations

"""
Shared pair / token filter logic.

This module is the single source of truth for:
  - Hard viability filters (liquidity, mcap, age, volume, etc.)
  - Momentum / "trending" score
  - Final score that combines momentum with AI rug-risk

All scanners (Raydium, Pump.fun, Dexscreener, etc.) should:
  1. Build a `meta` dict with the best info they have.
  2. Call `is_viable_pair(meta)` to decide whether to skip.
  3. Optionally call `compute_momentum_score(meta)` / `compute_final_score(meta)`
     to include on the candidate object / alert.

Expected `meta` keys (all optional, but the more you provide the smarter it gets):

  # core
  - "mint": str
  - "symbol": str
  - "dex": str              # e.g. "raydium", "pumpfun", "dexscreener"

  # price / size
  - "price_usd": float
  - "liq_usd": float        # current pool liquidity
  - "mcap_usd": float       # current market cap
  - "fdv_usd": float        # fully diluted valuation (optional)
  - "token_age_min": float  # minutes since creation / listing

  # volume / trades
  - "vol_5m_usd": float
  - "vol_1h_usd": float
  - "buys_5m": int
  - "sells_5m": int
  - "unique_buyers_5m": int

  # whales
  - "whales_5m": int                # count of whale wallets in last 5m
  - "whale_vol_5m_usd": float       # total USD bought by whales 5m
  - "largest_buy_5m_sol": float     # size of largest buy in SOL 5m

  # price change (percentage, e.g. +120.0 for +120%)
  - "price_change_5m_pct": float
  - "price_change_15m_pct": float
  - "price_change_1h_pct": float

  # holder dynamics
  - "holders": int
  - "holders_5m_change": int        # new holders in last 5m

  # security / AI
  - "is_honeypot": bool
  - "trading_paused": bool
  - "blacklisted": bool
  - "ai_rug_score": float           # 0.0 (safest) … 1.0 (obvious rug)

  - "top10_hold_pct": float         # 0–100
  - "dev_hold_pct": float           # 0–100
  - "mint_authority_disabled": bool
  - "freeze_authority_disabled": bool
"""

from typing import Any, Dict, Tuple
import math

from logging_setup import get_logger

log = get_logger()

# -----------------------------------------------------------------------------
# Config helpers
# -----------------------------------------------------------------------------

try:
    import config as C  # type: ignore
except Exception:  # pragma: no cover
    class _C:  # ultra-minimal fallback; real values live in config.py
        PF_SCAN_MIN_LIQ_USD = 3_000.0
        PF_SCAN_MAX_LIQ_USD = 250_000.0

        PF_SCAN_MIN_MCAP_USD = 3_000.0
        PF_SCAN_MAX_MCAP_USD = 750_000.0

        PF_SCAN_MAX_TOKEN_AGE_MIN = 240.0  # 4 hours

        PF_SCAN_MIN_5M_VOL_USD = 1_000.0
        PF_SCAN_MIN_5M_BUYS = 8
        PF_SCAN_MIN_BUY_TO_SELL_RATIO = 1.2

        PF_SCAN_MIN_WHALE_SIZE_SOL = 3.0
        PF_SCAN_MIN_WHALE_COUNT_5M = 1

        PF_MAX_RUG_RISK_SCORE = 0.65  # > this is too risky

        PF_MIN_MOMENTUM_SCORE = 0.25
        PF_MIN_FINAL_SCORE = 0.20

        PF_RISK_PENALTY_WEIGHT = 0.7  # how strongly AI rug score reduces final score

        # Soft guardrails for centralization (used as hints, not hard env)
        PF_SCAN_MAX_TOP10_HOLD_PCT = 90.0
        PF_SCAN_MAX_DEV_HOLD_PCT = 40.0

    C = _C()  # type: ignore


# -----------------------------------------------------------------------------
# Small numeric helpers
# -----------------------------------------------------------------------------

Meta = Dict[str, Any]


def _to_float(meta: Meta, key: str, default: float = 0.0) -> float:
    v = meta.get(key, default)
    try:
        f = float(v)
        if math.isnan(f) or math.isinf(f):  # defensive
            return default
        return f
    except (TypeError, ValueError):
        return default


def _to_int(meta: Meta, key: str, default: int = 0) -> int:
    v = meta.get(key, default)
    try:
        return int(v)
    except (TypeError, ValueError):
        return default


def _to_bool(meta: Meta, key: str, default: bool = False) -> bool:
    v = meta.get(key, default)
    if isinstance(v, bool):
        return v
    if isinstance(v, str):
        return v.strip().lower() in {"1", "true", "yes", "y", "on"}
    return bool(v)


def _soft_scale(x: float, lo: float, hi: float) -> float:
    """
    Map x -> [0, 1] between lo and hi (clamped).
      - <= lo  -> 0
      - >= hi  -> 1
    """
    if hi <= lo:
        return 0.0
    if x <= lo:
        return 0.0
    if x >= hi:
        return 1.0
    return (x - lo) / (hi - lo)


def _sigmoid(x: float, k: float = 1.0) -> float:
    """
    Standard logistic squashing into (0,1). k controls steepness.
    Currently unused but kept for future shaping.
    """
    try:
        return 1.0 / (1.0 + math.exp(-k * x))
    except OverflowError:  # pragma: no cover
        return 0.0 if x < 0 else 1.0


# -----------------------------------------------------------------------------
# Core scoring
# -----------------------------------------------------------------------------

def _compute_scores(meta: Meta) -> Tuple[float, float, float]:
    """
    Internal: compute (momentum_score, final_score, ai_rug_score).

    Returns:
        momentum_score: 0.0–1.0 (higher = trendier / more active)
        final_score:    0.0–1.0 (momentum adjusted by AI risk)
        ai_rug_score:   0.0–1.0 (0 safe, 1 obvious rug)
    """
    # Price action
    p5 = _to_float(meta, "price_change_5m_pct", 0.0)
    p15 = _to_float(meta, "price_change_15m_pct", 0.0)
    p1h = _to_float(meta, "price_change_1h_pct", 0.0)

    # Volume / trades
    vol_5m = _to_float(meta, "vol_5m_usd", 0.0)
    vol_1h = _to_float(meta, "vol_1h_usd", 0.0)
    buys_5m = _to_int(meta, "buys_5m", 0)
    sells_5m = _to_int(meta, "sells_5m", 0)

    # Buy pressure
    total_trades_5m = max(1, buys_5m + sells_5m)
    buy_ratio = buys_5m / float(total_trades_5m)

    # Whales
    whales_5m = _to_int(meta, "whales_5m", 0)
    whale_vol_5m = _to_float(meta, "whale_vol_5m_usd", 0.0)  # not directly used yet
    largest_buy_5m_sol = _to_float(meta, "largest_buy_5m_sol", 0.0)

    # Age
    age_min = _to_float(meta, "token_age_min", 9999.0)

    # Holders
    holders_change_5m = _to_int(meta, "holders_5m_change", 0)

    # Security / rug risk
    ai_rug_score = _to_float(meta, "ai_rug_score", 0.0)
    ai_rug_score = max(0.0, min(1.0, ai_rug_score))

    # ------------------------------------------------------------------
    # Individual components mapped to 0–1
    # ------------------------------------------------------------------

    # We like strong but not completely insane 5m move
    comp_price_5m = _soft_scale(p5, 10.0, 120.0)  # 0 below +10%, 1 at +120%+

    # 15m can be flat or moderately up; small negative is ok early
    comp_price_15m = _soft_scale(p15 + 20.0, 0.0, 180.0)  # allow some red startup

    # 1h we mostly care when it's already trending
    comp_price_1h = _soft_scale(p1h, 0.0, 250.0)

    # Volume: stronger if 5m is big relative to 1h (breakout)
    min_5m = getattr(C, "PF_SCAN_MIN_5M_VOL_USD", 1_000.0)
    comp_vol_5m_abs = _soft_scale(vol_5m, min_5m, min_5m * 5)
    vol_ratio = vol_5m / max(vol_1h, 1.0)
    comp_vol_ratio = _soft_scale(vol_ratio, 0.1, 0.7)  # >70% of 1h in last 5m is strong
    comp_volume = 0.6 * comp_vol_5m_abs + 0.4 * comp_vol_ratio

    # Buy pressure
    comp_buy_pressure = _soft_scale(buy_ratio, 0.5, 0.8)  # >80% buys very bullish
    comp_trades_count = _soft_scale(total_trades_5m, 10.0, 80.0)

    # Whales: more whales + larger buys
    min_whale_size_sol = getattr(C, "PF_SCAN_MIN_WHALE_SIZE_SOL", 3.0)
    comp_whale_count = _soft_scale(float(whales_5m), 0.0, 3.0)
    comp_whale_size = _soft_scale(
        largest_buy_5m_sol,
        min_whale_size_sol,
        min_whale_size_sol * 4,
    )
    comp_whales = 0.5 * comp_whale_count + 0.5 * comp_whale_size

    # Age: newer is better; beyond PF_SCAN_MAX_TOKEN_AGE_MIN we fade to 0
    max_age_min = getattr(C, "PF_SCAN_MAX_TOKEN_AGE_MIN", 240.0)
    # Map age 0–max_age_min -> 1–0
    comp_age = 1.0 - _soft_scale(age_min, 5.0, max_age_min)

    # Holder growth
    comp_holders_growth = _soft_scale(float(holders_change_5m), 2.0, 40.0)

    # Combine into a single momentum score
    momentum_score = (
        0.25 * comp_price_5m +
        0.10 * comp_price_15m +
        0.05 * comp_price_1h +
        0.20 * comp_volume +
        0.15 * comp_buy_pressure +
        0.05 * comp_trades_count +
        0.10 * comp_whales +
        0.05 * comp_age +
        0.05 * comp_holders_growth
    )

    # Squash and clamp
    momentum_score = max(0.0, min(1.0, momentum_score))

    # Risk penalty from AI rug score
    risk_weight = getattr(C, "PF_RISK_PENALTY_WEIGHT", 0.7)
    risk_penalty = max(0.0, 1.0 - risk_weight * ai_rug_score)

    final_score = momentum_score * risk_penalty
    final_score = max(0.0, min(1.0, final_score))

    return momentum_score, final_score, ai_rug_score


# -----------------------------------------------------------------------------
# Public APIs
# -----------------------------------------------------------------------------

def compute_momentum_score(meta: Meta) -> float:
    """
    Compute a 0.0–1.0 "trending / momentum" score based on the recent
    price action, volume, buy pressure, whale activity, age, and holder growth.
    """
    momentum, _, _ = _compute_scores(meta)
    return momentum


def compute_final_score(meta: Meta) -> float:
    """
    Compute a 0.0–1.0 final score that penalizes high AI rug-risk.
    Use this if you want a single number to rank opportunities.
    """
    _, final, _ = _compute_scores(meta)
    return final


def is_viable_pair(meta: Meta) -> bool:
    """
    Hard filter for "is this pair worth even considering?".

    This uses:
      - Liquidity & market cap ranges
      - Age
      - Minimum recent volume & trades
      - Buy/sell balance
      - Whale presence
      - Honeypot / paused / blacklisted flags
      - AI rug risk
      - Minimum momentum & final scores

    Returns:
        bool: True if the pair passes all filters, False otherwise.
    """
    liq = _to_float(meta, "liq_usd", 0.0)
    mcap = _to_float(meta, "mcap_usd", 0.0)
    age_min = _to_float(meta, "token_age_min", 9999.0)

    vol_5m = _to_float(meta, "vol_5m_usd", 0.0)
    buys_5m = _to_int(meta, "buys_5m", 0)
    sells_5m = _to_int(meta, "sells_5m", 0)

    whales_5m = _to_int(meta, "whales_5m", 0)
    largest_buy_5m_sol = _to_float(meta, "largest_buy_5m_sol", 0.0)

    is_honeypot = _to_bool(meta, "is_honeypot", False)
    trading_paused = _to_bool(meta, "trading_paused", False)
    blacklisted = _to_bool(meta, "blacklisted", False)

    # Security / centralization
    top10_hold_pct = _to_float(meta, "top10_hold_pct", 0.0)
    dev_hold_pct = _to_float(meta, "dev_hold_pct", 0.0)
    mint_auth_disabled = _to_bool(meta, "mint_authority_disabled", True)
    freeze_auth_disabled = _to_bool(meta, "freeze_authority_disabled", True)

    min_liq = getattr(C, "PF_SCAN_MIN_LIQ_USD", 3_000.0)
    max_liq = getattr(C, "PF_SCAN_MAX_LIQ_USD", 250_000.0)
    min_mcap = getattr(C, "PF_SCAN_MIN_MCAP_USD", 3_000.0)
    max_mcap = getattr(C, "PF_SCAN_MAX_MCAP_USD", 750_000.0)
    max_age_min = getattr(C, "PF_SCAN_MAX_TOKEN_AGE_MIN", 240.0)

    min_5m_vol = getattr(C, "PF_SCAN_MIN_5M_VOL_USD", 1_000.0)
    min_5m_buys = getattr(C, "PF_SCAN_MIN_5M_BUYS", 8)
    min_buy_sell_ratio = getattr(C, "PF_SCAN_MIN_BUY_TO_SELL_RATIO", 1.2)

    min_whale_size_sol = getattr(C, "PF_SCAN_MIN_WHALE_SIZE_SOL", 3.0)
    min_whale_count_5m = getattr(C, "PF_SCAN_MIN_WHALE_COUNT_5M", 1)

    max_rug_score = getattr(C, "PF_MAX_RUG_RISK_SCORE", 0.65)
    min_momentum = getattr(C, "PF_MIN_MOMENTUM_SCORE", 0.25)
    min_final = getattr(C, "PF_MIN_FINAL_SCORE", 0.20)

    max_top10 = getattr(C, "PF_SCAN_MAX_TOP10_HOLD_PCT", 90.0)
    max_dev_hold = getattr(C, "PF_SCAN_MAX_DEV_HOLD_PCT", 40.0)

    # ------------------------------------------------------------------
    # Hard filters
    # ------------------------------------------------------------------
    reasons = []

    # Liquidity / mcap window
    if liq < min_liq:
        reasons.append(f"liq {liq:.0f} < min {min_liq:.0f}")
    if max_liq > 0 and liq > max_liq:
        reasons.append(f"liq {liq:.0f} > max {max_liq:.0f}")

    if mcap < min_mcap:
        reasons.append(f"mcap {mcap:.0f} < min {min_mcap:.0f}")
    if max_mcap > 0 and mcap > max_mcap:
        reasons.append(f"mcap {mcap:.0f} > max {max_mcap:.0f}")

    # Age
    if age_min > max_age_min:
        reasons.append(f"age {age_min:.1f}m > max {max_age_min:.1f}m")

    # Obvious safety flags
    if is_honeypot:
        reasons.append("honeypot flagged")
    if trading_paused:
        reasons.append("trading paused")
    if blacklisted:
        reasons.append("token blacklisted")

    # Basic decentralization / safety (soft caps, but treated as hard rejects here)
    if top10_hold_pct > max_top10:
        reasons.append(f"top10 too concentrated ({top10_hold_pct:.1f}%)")
    if dev_hold_pct > max_dev_hold:
        reasons.append(f"dev holds too much ({dev_hold_pct:.1f}%)")
    if not mint_auth_disabled:
        reasons.append("mint authority still enabled")
    if not freeze_auth_disabled:
        reasons.append("freeze authority still enabled")

    # Volume / activity
    if vol_5m < min_5m_vol:
        reasons.append(f"5m volume {vol_5m:.0f} < min {min_5m_vol:.0f}")
    if buys_5m < min_5m_buys:
        reasons.append(f"buys_5m {buys_5m} < min {min_5m_buys}")

    total_trades_5m = max(1, buys_5m + sells_5m)
    buy_sell_ratio = buys_5m / float(max(1, sells_5m))
    buy_share = buys_5m / float(total_trades_5m)
    if sells_5m > 0 and buy_sell_ratio < min_buy_sell_ratio:
        reasons.append(f"buy/sell ratio {buy_sell_ratio:.2f} < {min_buy_sell_ratio:.2f}")
    if buy_share < 0.5:
        reasons.append(f"buy share {buy_share:.2f} < 0.5")

    # Whales
    if whales_5m < min_whale_count_5m and largest_buy_5m_sol < min_whale_size_sol:
        reasons.append("no meaningful whales in last 5m")

    # AI / momentum scores
    momentum_score, final_score, ai_rug_score = _compute_scores(meta)

    if ai_rug_score > max_rug_score:
        reasons.append(f"AI rug score {ai_rug_score:.2f} > max {max_rug_score:.2f}")
    if momentum_score < min_momentum:
        reasons.append(f"momentum {momentum_score:.2f} < min {min_momentum:.2f}")
    if final_score < min_final:
        reasons.append(f"final score {final_score:.2f} < min {min_final:.2f}")

    ok = len(reasons) == 0

    # Attach decision for downstream / debugging
    meta.setdefault("_filter_debug", {})
    meta["_filter_debug"].update(
        {
            "ok": ok,
            "reasons": reasons,
            "momentum_score": momentum_score,
            "final_score": final_score,
            "ai_rug_score": ai_rug_score,
            "liq_usd": liq,
            "mcap_usd": mcap,
            "age_min": age_min,
            "vol_5m_usd": vol_5m,
            "buys_5m": buys_5m,
            "sells_5m": sells_5m,
            "whales_5m": whales_5m,
            "largest_buy_5m_sol": largest_buy_5m_sol,
        }
    )

    if not ok:
        # Use .every if available to avoid log spam
        msg = (
            f"[FILTER-REJECT] mint={meta.get('mint')} symbol={meta.get('symbol')} "
            f"dex={meta.get('dex')} reasons={'; '.join(reasons)} "
            f"momentum={momentum_score:.2f} final={final_score:.2f}"
        )
        key = f"pair_filters.reject.{meta.get('mint', 'unknown')}"
        if hasattr(log, "every"):
            # type: ignore[attr-defined]
            log.every(key, 60, msg)
        else:
            log.debug(msg)

    return ok
