from __future__ import annotations

"""
utils_pkg/pair_filters.py

Shared pair / token filter logic (V3+++ / quant-grade).
=======================================================

Single source of truth for:

  • Hard viability filters (liq / mcap / age / volume / whales / safety)
  • Momentum / "trending" score:      pf_momentum_score
  • Final risk-adjusted score:        pf_final_score
  • Extra risk-adjusted final:        pf_risk_adjusted_final
  • Normalized AI rug score:          ai_rug_score_01
  • Rich debugging / profiling for AI & FeatureBuilder (meta["_filter_debug"],
    meta["_score_debug"], meta["pf_profile"])

All scanners (Raydium, Pump.fun, Dexscreener, on-chain, etc.) should:

  1. Build a `meta` dict with as many fields as possible.
  2. Call:

         ok = is_viable_pair(meta)

     or for richer info:

         decision = evaluate_pair(meta)  # see evaluate_pair() below

  3. Pass the enriched `meta` onward into FeatureBuilder / ai_entry_logic /
     ai_buy_scoring / logging.

Expected `meta` keys (all optional, but the more you provide the smarter it gets):

  # identity / routing
  - "mint": str
  - "symbol": str
  - "dex": str              # e.g. "raydium", "pumpfun", "dexscreener"
  - "source": str           # e.g. "raydium", "pumpfun", "dexscreener"
  - "regime": str           # optional override of global regime, e.g. "risk_on"

  # price / size
  - "price_usd": float
  - "liq_usd": float        # current pool liquidity
  - "mcap_usd": float       # current market cap
  - "fdv_usd": float        # fully diluted valuation (optional, for valuation risk)
  - "token_age_min": float  # minutes since creation / listing

  # volume / trades
  - "vol_5m_usd": float
  - "vol_1h_usd": float
  - "vol_24h_usd": float
  - "buys_5m": int
  - "sells_5m": int
  - "buys_1h": int
  - "sells_1h": int
  - "buys_24h": int
  - "sells_24h": int

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

  # security / AI risk
  - "is_honeypot": bool
  - "trading_paused": bool
  - "blacklisted": bool
  - "ai_rug_score": float           # 0.0 (safest) … 1.0 (obvious rug)
  - "ai_rug_score_01": float        # explicit normalized 0..1 if provided
  - "ai_fraud_score": float         # optional alt name (0..1 or 0..100)

  - "top10_hold_pct": float         # 0–100
  - "dev_hold_pct": float           # 0–100
  - "mint_authority_disabled": bool
  - "freeze_authority_disabled": bool

  # optional microstructure / alpha
  - "impact_1k_bps": float          # price impact for 1k USD trade (basis points)
  - "smart_wallet_score": float     # 0–1, higher = more "smart money" support
"""

from typing import Any, Dict, Tuple
import math

from logging_setup import get_logger

log = get_logger()

# =============================================================================
# Config + optional regime integration
# =============================================================================

try:
    import config as C  # type: ignore
except Exception:  # pragma: no cover
    # Very small, safe defaults for tests / notebooks
    class _C:
        PF_SCAN_MIN_LIQ_USD = 150.0
        PF_SCAN_MAX_LIQ_USD = 0.0          # 0 = no max cap
        PF_SCAN_MIN_MCAP_USD = 500.0
        PF_SCAN_MAX_MCAP_USD = 0.0         # 0 = no max cap
        PF_SCAN_MAX_TOKEN_AGE_MIN = 720.0  # 12h default

        PF_SCAN_MIN_5M_VOL_USD = 30.0
        PF_SCAN_MIN_5M_BUYS = 1
        PF_SCAN_MIN_BUY_TO_SELL_RATIO = 1.05

        PF_SCAN_MIN_WHALE_SIZE_SOL = 1.5
        PF_SCAN_MIN_WHALE_COUNT_5M = 0

        PF_MAX_RUG_RISK_SCORE = 0.80  # > this is too risky

        PF_MIN_MOMENTUM_SCORE = 0.14
        PF_MIN_FINAL_SCORE = 0.10

        # How strongly AI rug score reduces final score
        PF_RISK_PENALTY_WEIGHT = 0.6

        # Soft guardrails for centralization (used as hard rejects)
        PF_SCAN_MAX_TOP10_HOLD_PCT = 95.0
        PF_SCAN_MAX_DEV_HOLD_PCT = 60.0

        # Microstructure / slippage (impact in basis points)
        PF_SCAN_MAX_IMPACT_1K_BPS = 800.0   # >8% impact for 1k → too illiquid
        PF_IMPACT_PENALTY_WEIGHT = 0.25     # how much impact hurts risk factor

        # Smart-wallet alpha (if available in meta)
        PF_MIN_SMART_SCORE = 0.0            # 0 = no hard min
        PF_SMART_RISK_BOOST = 0.25          # boosts risk factor for high smart score

        # Valuation stretch (FDV vs liquidity) penalty
        PF_VALUATION_PENALTY_WEIGHT = 0.15  # how much over-dilution hurts risk factor

    C = _C()  # type: ignore

# Optional regime helper (for dynamic thresholds)
try:
    from utils_pkg import regime as _regime  # type: ignore
except Exception:  # pragma: no cover
    _regime = None  # type: ignore

Meta = Dict[str, Any]

# =============================================================================
# Small numeric helpers
# =============================================================================


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
    Currently unused but kept for future shaping / experimentation.
    """
    try:
        return 1.0 / (1.0 + math.exp(-k * x))
    except OverflowError:  # pragma: no cover
        return 0.0 if x < 0 else 1.0


# =============================================================================
# Regime helpers (dynamic thresholds / risk weight)
# =============================================================================


def _get_regime(meta: Meta) -> str:
    """
    Resolve an effective regime name:
      1) Explicit `meta["regime"]` if provided.
      2) utils_pkg.regime.regime_for_src(source) if available.
      3) utils_pkg.regime.current_regime() if available.
      4) Fallback "live".
    """
    r = meta.get("regime")
    if isinstance(r, str) and r:
        return r

    src = (meta.get("source") or meta.get("dex") or "generic").lower()

    if _regime is not None:
        # Prefer per-source regime if available
        if hasattr(_regime, "regime_for_src"):
            try:
                rr = _regime.regime_for_src(src, default="live")  # type: ignore[arg-type]
                if isinstance(rr, str) and rr:
                    return rr
            except Exception:
                pass
        # Fallback to global regime
        if hasattr(_regime, "current_regime"):
            try:
                rr = _regime.current_regime()  # type: ignore[call-arg]
                if isinstance(rr, str) and rr:
                    return rr
            except Exception:
                pass

    return "live"


def _regime_profile(name: str) -> Dict[str, float]:
    """
    Map regime → tuning knobs.

    Returns a dict with:
      - risk_weight: multiplier for AI rug penalty (higher = more risk-averse)
      - min_momentum_mult: multiplier for PF_MIN_MOMENTUM_SCORE
      - min_final_mult: multiplier for PF_MIN_FINAL_SCORE
      - volume_mult: multiplier for PF_SCAN_MIN_5M_VOL_USD (risk-off needs more flow)
    """
    n = (name or "").lower()

    # Aggressive / bull / momentum regimes
    if n in {"bull", "risk_on", "momentum", "pump", "up_only"}:
        return {
            "risk_weight": 0.55,     # slightly softer on rug penalty
            "min_momentum_mult": 0.85,
            "min_final_mult": 0.80,
            "volume_mult": 0.90,
        }

    # Defensive / chop / crab
    if n in {"crab", "chop", "range", "sideways"}:
        return {
            "risk_weight": 0.65,
            "min_momentum_mult": 1.00,
            "min_final_mult": 1.00,
            "volume_mult": 1.00,
        }

    # Bearish / risk-off
    if n in {"bear", "risk_off", "defensive"}:
        return {
            "risk_weight": 0.80,     # harsher on rug penalty
            "min_momentum_mult": 1.10,
            "min_final_mult": 1.20,
            "volume_mult": 1.10,
        }

    # Default live regime
    return {
        "risk_weight": getattr(C, "PF_RISK_PENALTY_WEIGHT", 0.6),
        "min_momentum_mult": 1.00,
        "min_final_mult": 1.00,
        "volume_mult": 1.00,
    }


# =============================================================================
# Core scoring
# =============================================================================


def _resolve_ai_rug_score(meta: Meta) -> float:
    """
    Normalize any of the rug/fraud fields into a clean 0..1 value:
      - ai_rug_score_01
      - ai_rug_score
      - ai_fraud_score (can be 0..1 or 0..100)
    """
    raw = None

    if "ai_rug_score_01" in meta:
        raw = _to_float(meta, "ai_rug_score_01", 0.0)
    elif "ai_rug_score" in meta:
        raw = _to_float(meta, "ai_rug_score", 0.0)
    elif "ai_fraud_score" in meta:
        raw = _to_float(meta, "ai_fraud_score", 0.0)

    if raw is None:
        return 0.0

    # If someone fed 0..100, squash.
    if raw > 1.5:
        raw = raw / 100.0
    return max(0.0, min(1.0, raw))


def _compute_scores(meta: Meta) -> Tuple[float, float, float]:
    """
    Internal: compute (momentum_score, final_score, ai_rug_score).

    Returns:
        momentum_score:      0.0–1.0 (higher = trendier / more active)
        final_score:         0.0–1.0 (momentum adjusted by AI risk, slippage, valuation, smart flow)
        ai_rug_score:        0.0–1.0 (0 safe, 1 obvious rug)
    """
    regime_name = _get_regime(meta)
    rprof = _regime_profile(regime_name)

    # Price action
    p5 = _to_float(meta, "price_change_5m_pct", 0.0)
    p15 = _to_float(meta, "price_change_15m_pct", 0.0)
    p1h = _to_float(meta, "price_change_1h_pct", 0.0)

    # Volume / trades
    vol_5m = _to_float(meta, "vol_5m_usd", 0.0)
    vol_1h = _to_float(meta, "vol_1h_usd", 0.0)
    vol_24h = _to_float(meta, "vol_24h_usd", 0.0)
    buys_5m = _to_int(meta, "buys_5m", 0)
    sells_5m = _to_int(meta, "sells_5m", 0)

    # Buy pressure
    total_trades_5m = max(1, buys_5m + sells_5m)
    buy_ratio = buys_5m / float(total_trades_5m)

    # Whales
    whales_5m = _to_int(meta, "whales_5m", 0)
    whale_vol_5m = _to_float(meta, "whale_vol_5m_usd", 0.0)
    largest_buy_5m_sol = _to_float(meta, "largest_buy_5m_sol", 0.0)

    # Age
    age_min = _to_float(meta, "token_age_min", 9999.0)

    # Holders
    holders_change_5m = _to_int(meta, "holders_5m_change", 0)

    # Security / rug risk
    ai_rug_score = _resolve_ai_rug_score(meta)

    # Slippage / microstructure
    impact_bps = _to_float(meta, "impact_1k_bps", 0.0)

    # Liquidity / valuation
    liq_usd = _to_float(meta, "liq_usd", 0.0)
    fdv_usd = _to_float(meta, "fdv_usd", 0.0)
    fdv_to_liq = 0.0
    if liq_usd > 0 and fdv_usd > 0:
        fdv_to_liq = fdv_usd / liq_usd

    # Smart-wallet alpha (0–1)
    smart_score = _to_float(meta, "smart_wallet_score", 0.0)
    smart_score = max(0.0, min(1.0, smart_score))

    # ------------------------------------------------------------------
    # Individual components mapped to 0–1
    # ------------------------------------------------------------------

    # We like decent 5m move, but not necessarily only face-melters.
    comp_price_5m = _soft_scale(p5, 5.0, 90.0)   # 0 below +5%, 1 at +90%+

    # 15m trend — avoids overfitting to a single spike.
    comp_price_15m = _soft_scale(p15 + 20.0, 0.0, 180.0)

    # 1h trend — mostly useful once already moving.
    comp_price_1h = _soft_scale(p1h, -20.0, 250.0)

    # Volume: absolute + concentration in last 5m vs 1h/24h
    base_min_5m = getattr(C, "PF_SCAN_MIN_5M_VOL_USD", 50.0)
    min_5m = base_min_5m * rprof.get("volume_mult", 1.0)

    comp_vol_5m_abs = _soft_scale(vol_5m, min_5m, min_5m * 6)
    vol_ratio_5m_1h = vol_5m / max(vol_1h, 1.0)
    comp_vol_ratio_5m_1h = _soft_scale(vol_ratio_5m_1h, 0.1, 0.7)  # >70% of 1h in last 5m is strong

    # If only 24h volume exists, interpret big 24h spike as momentum too.
    vol_ratio_5m_24h = vol_5m / max(vol_24h, 1.0)
    comp_vol_ratio_5m_24h = _soft_scale(vol_ratio_5m_24h, 0.02, 0.40)

    comp_volume = (
        0.55 * comp_vol_5m_abs +
        0.30 * comp_vol_ratio_5m_1h +
        0.15 * comp_vol_ratio_5m_24h
    )

    # Buy pressure
    comp_buy_pressure = _soft_scale(buy_ratio, 0.45, 0.8)  # allow more chop
    comp_trades_count = _soft_scale(total_trades_5m, 6.0, 60.0)

    # Whales: more whales + larger buys; bonus if whale share is big.
    min_whale_size_sol = getattr(C, "PF_SCAN_MIN_WHALE_SIZE_SOL", 1.5)
    comp_whale_count = _soft_scale(float(whales_5m), 0.0, 3.0)
    comp_whale_size = _soft_scale(
        largest_buy_5m_sol,
        min_whale_size_sol,
        min_whale_size_sol * 4,
    )
    # Whale volume share vs overall 5m
    whale_share = whale_vol_5m / max(vol_5m, 1.0)
    comp_whale_share = _soft_scale(whale_share, 0.05, 0.50)

    comp_whales = (
        0.45 * comp_whale_count +
        0.35 * comp_whale_size +
        0.20 * comp_whale_share
    )

    # Age: newer is better; beyond PF_SCAN_MAX_TOKEN_AGE_MIN we fade to 0.
    max_age_min = getattr(C, "PF_SCAN_MAX_TOKEN_AGE_MIN", 720.0)
    # Map age 0–max_age_min -> 1–0
    comp_age = 1.0 - _soft_scale(age_min, 5.0, max_age_min)

    # Holder growth → demand
    comp_holders_growth = _soft_scale(float(holders_change_5m), 1.0, 30.0)

    # Smart wallets: treat as a positive alpha component
    comp_smart = smart_score

    # ------------------------------------------------------------------
    # Combine into momentum score
    # ------------------------------------------------------------------
    momentum_score = (
        0.23 * comp_price_5m +
        0.10 * comp_price_15m +
        0.05 * comp_price_1h +
        0.20 * comp_volume +
        0.14 * comp_buy_pressure +
        0.06 * comp_trades_count +
        0.10 * comp_whales +
        0.07 * comp_age +
        0.03 * comp_holders_growth +
        0.02 * comp_smart
    )

    momentum_score = max(0.0, min(1.0, momentum_score))

    # ------------------------------------------------------------------
    # Risk / quality adjustments (AI rug, slippage, valuation, smart wallets)
    # ------------------------------------------------------------------
    risk_weight_ai = rprof.get("risk_weight", getattr(C, "PF_RISK_PENALTY_WEIGHT", 0.6))
    impact_weight = getattr(C, "PF_IMPACT_PENALTY_WEIGHT", 0.25)
    smart_risk_boost = getattr(C, "PF_SMART_RISK_BOOST", 0.25)
    val_weight = getattr(C, "PF_VALUATION_PENALTY_WEIGHT", 0.15)

    # AI rug: 0 → no penalty, 1 → full penalty, scaled by risk_weight_ai
    ai_penalty = risk_weight_ai * ai_rug_score
    ai_factor = max(0.0, 1.0 - ai_penalty)

    # Slippage: more impact → more penalty
    max_impact_bps = getattr(C, "PF_SCAN_MAX_IMPACT_1K_BPS", 800.0)
    # Map impact 0 → 0, max_impact_bps → 1
    impact_component = _soft_scale(impact_bps, 50.0, max_impact_bps)
    slip_factor = max(0.0, 1.0 - impact_weight * impact_component)

    # Valuation stretch (FDV vs liquidity): over-diluted tokens get penalized
    # Typical healthy FDV/liquidity bands might be 5–20; 40+ starts to look
    # very "air-y".
    val_component = _soft_scale(fdv_to_liq, 10.0, 40.0) if fdv_to_liq > 0 else 0.0
    val_factor = max(0.0, 1.0 - val_weight * val_component)

    # Smart wallets: high smart_score amplifies risk factor (lower effective risk)
    smart_factor = 1.0 + smart_risk_boost * smart_score

    risk_factor = ai_factor * slip_factor * val_factor * smart_factor
    # Clamp risk_factor to a sane band (allow slight >1 boost for smart wallets)
    risk_factor = max(0.0, min(1.3, risk_factor))

    final_score = momentum_score * min(1.0, risk_factor)
    final_score = max(0.0, min(1.0, final_score))

    # Risk-adjusted final for downstream AI (used by ai_entry_logic, etc.)
    risk_adjusted_final = final_score * (1.0 - ai_rug_score)
    risk_adjusted_final = max(0.0, min(1.0, risk_adjusted_final))

    # ------------------------------------------------------------------
    # Expose pf_* fields on meta so FeatureBuilder / ai_entry_logic can use them
    # ------------------------------------------------------------------
    meta["pf_momentum_score"] = momentum_score
    meta["pf_final_score"] = final_score
    meta["pf_risk_adjusted_final"] = risk_adjusted_final
    meta["ai_rug_score_01"] = ai_rug_score  # normalized 0..1
    meta["pf_fdv_to_liq"] = fdv_to_liq

    # Optional: stash some internal comps on meta for downstream introspection
    dbg = meta.setdefault("_score_debug", {})
    dbg.update(
        {
            "regime": regime_name,
            "momentum": momentum_score,
            "final": final_score,
            "risk_adjusted_final": risk_adjusted_final,
            "ai_rug_score": ai_rug_score,
            "impact_1k_bps": impact_bps,
            "smart_wallet_score": smart_score,
            "fdv_to_liq": fdv_to_liq,
            "components": {
                "price_5m": comp_price_5m,
                "price_15m": comp_price_15m,
                "price_1h": comp_price_1h,
                "volume": comp_volume,
                "buy_pressure": comp_buy_pressure,
                "trades": comp_trades_count,
                "whales": comp_whales,
                "age": comp_age,
                "holders_growth": comp_holders_growth,
                "smart": comp_smart,
                "ai_factor": ai_factor,
                "slip_factor": slip_factor,
                "val_factor": val_factor,
                "smart_factor": smart_factor,
            },
        }
    )

    return momentum_score, final_score, ai_rug_score


# =============================================================================
# Public scoring helpers
# =============================================================================


def compute_momentum_score(meta: Meta) -> float:
    """Return 0.0–1.0 'trending / momentum' score (also writes pf_momentum_score)."""
    momentum, _, _ = _compute_scores(meta)
    return momentum


def compute_final_score(meta: Meta) -> float:
    """
    Return 0.0–1.0 final score (momentum adjusted by AI risk, slippage, valuation, smart wallets).

    Also writes:
      - pf_final_score
      - pf_risk_adjusted_final
      - ai_rug_score_01
    into `meta`.
    """
    _, final, _ = _compute_scores(meta)
    return final


# =============================================================================
# Profiling helpers (for FeatureBuilder / AI)
# =============================================================================


def _build_profile(meta: Meta) -> Dict[str, Any]:
    """
    Build a light-weight "profile" for the pair to help downstream AI / analytics.

    Profile fields (all optional / best-effort):
      - risk_bucket: "green" | "yellow" | "red"
      - size_bucket: "micro" | "small" | "mid" | "large"
      - style: e.g. "fresh_microcap", "whale_chase", "midcap_trend", ...
    """
    liq = _to_float(meta, "liq_usd", 0.0)
    mcap = _to_float(meta, "mcap_usd", 0.0)
    age_min = _to_float(meta, "token_age_min", 9999.0)

    momentum = float(meta.get("pf_momentum_score") or 0.0)
    final_score = float(meta.get("pf_final_score") or 0.0)
    ai_rug = _resolve_ai_rug_score(meta)
    smart_score = _to_float(meta, "smart_wallet_score", 0.0)
    whales_5m = _to_int(meta, "whales_5m", 0)
    fdv_to_liq = _to_float(meta, "pf_fdv_to_liq", 0.0)

    # --- risk bucket ---
    if ai_rug >= 0.85 or final_score < 0.12:
        risk_bucket = "red"
    elif ai_rug >= 0.60 or final_score < 0.22:
        risk_bucket = "yellow"
    else:
        risk_bucket = "green"

    # --- size bucket (by liq + mcap) ---
    if liq < 50_000 and mcap < 200_000:
        size_bucket = "micro"
    elif liq < 200_000 and mcap < 1_000_000:
        size_bucket = "small"
    elif liq < 1_000_000 and mcap < 5_000_000:
        size_bucket = "mid"
    else:
        size_bucket = "large"

    # --- style heuristic ---
    style = "unknown"
    if age_min < 30 and size_bucket in {"micro", "small"}:
        style = "fresh_microcap"
    if whales_5m > 0 and momentum > 0.35:
        style = "whale_chase"
    if size_bucket in {"mid", "large"} and momentum > 0.30 and ai_rug < 0.4:
        style = "midcap_trend"
    if fdv_to_liq > 30 and size_bucket in {"micro", "small"}:
        style = "overdiluted_degen"

    # If extremely risky, override style.
    if risk_bucket == "red":
        style = "high_risk"

    prof = {
        "risk_bucket": risk_bucket,
        "size_bucket": size_bucket,
        "style": style,
    }

    meta["pf_profile"] = prof
    return prof


# =============================================================================
# Hard filter
# =============================================================================


def is_viable_pair(meta: Meta) -> bool:
    """
    Hard filter for "is this pair worth even considering?".

    This uses:
      - Liquidity & market cap ranges
      - Age
      - Minimum recent volume & trades
      - Buy/sell balance (looser than before)
      - Whale presence (configurable)
      - Honeypot / paused / blacklisted flags
      - AI rug risk
      - Slippage limits (impact_1k_bps)
      - Optional smart-wallet minimum score
      - Minimum *final* score (regime-aware)
      - Light valuation sanity (FDV vs liquidity via final_score penalty)

    Side effects:
      - Populates pf_momentum_score, pf_final_score, pf_risk_adjusted_final.
      - Populates ai_rug_score_01, pf_fdv_to_liq.
      - Populates meta["_filter_debug"] with reasons and key stats.
      - Populates meta["pf_profile"] with a small risk/size/style profile.

    Returns:
        bool: True if the pair passes all filters, False otherwise.
    """
    regime_name = _get_regime(meta)
    rprof = _regime_profile(regime_name)

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

    # Security / centralization.
    top10_hold_pct = _to_float(meta, "top10_hold_pct", 0.0)
    dev_hold_pct = _to_float(meta, "dev_hold_pct", 0.0)
    mint_auth_disabled = _to_bool(meta, "mint_authority_disabled", True)
    freeze_auth_disabled = _to_bool(meta, "freeze_authority_disabled", True)

    # Microstructure / slippage.
    impact_bps = _to_float(meta, "impact_1k_bps", 0.0)

    # Smart-wallet alpha.
    smart_score = _to_float(meta, "smart_wallet_score", 0.0)
    smart_score = max(0.0, min(1.0, smart_score))

    min_liq = getattr(C, "PF_SCAN_MIN_LIQ_USD", 150.0)
    max_liq = getattr(C, "PF_SCAN_MAX_LIQ_USD", 0.0)  # 0 => no max
    min_mcap = getattr(C, "PF_SCAN_MIN_MCAP_USD", 500.0)
    max_mcap = getattr(C, "PF_SCAN_MAX_MCAP_USD", 0.0)  # 0 => no max
    max_age_min = getattr(C, "PF_SCAN_MAX_TOKEN_AGE_MIN", 720.0)

    base_min_5m_vol = getattr(C, "PF_SCAN_MIN_5M_VOL_USD", 50.0)
    min_5m_vol = base_min_5m_vol * rprof.get("volume_mult", 1.0)
    min_5m_buys = getattr(C, "PF_SCAN_MIN_5M_BUYS", 1)
    min_buy_sell_ratio = getattr(C, "PF_SCAN_MIN_BUY_TO_SELL_RATIO", 1.05)

    min_whale_size_sol = getattr(C, "PF_SCAN_MIN_WHALE_SIZE_SOL", 1.5)
    min_whale_count_5m = getattr(C, "PF_SCAN_MIN_WHALE_COUNT_5M", 0)

    max_rug_score = getattr(C, "PF_MAX_RUG_RISK_SCORE", 0.80)
    base_min_momentum = getattr(C, "PF_MIN_MOMENTUM_SCORE", 0.14)
    base_min_final = getattr(C, "PF_MIN_FINAL_SCORE", 0.10)

    max_top10 = getattr(C, "PF_SCAN_MAX_TOP10_HOLD_PCT", 95.0)
    max_dev_hold = getattr(C, "PF_SCAN_MAX_DEV_HOLD_PCT", 60.0)

    max_impact_bps = getattr(C, "PF_SCAN_MAX_IMPACT_1K_BPS", 800.0)
    min_smart_score = getattr(C, "PF_MIN_SMART_SCORE", 0.0)

    # Regime-aware dynamic thresholds
    min_momentum = base_min_momentum * rprof.get("min_momentum_mult", 1.0)
    min_final = base_min_final * rprof.get("min_final_mult", 1.0)

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

    # Basic decentralization / safety (hard rejects).
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
    # Avoid divide-by-zero for sell-heavy coins.
    buy_sell_ratio = buys_5m / float(max(1, sells_5m))
    buy_share = buys_5m / float(total_trades_5m)

    # Only enforce buy pressure when there is at least a bit of trade flow.
    if sells_5m > 0 and total_trades_5m >= 3:
        if buy_sell_ratio < min_buy_sell_ratio:
            reasons.append(
                f"buy/sell ratio {buy_sell_ratio:.2f} < {min_buy_sell_ratio:.2f}"
            )
        # Looser than 0.5: we allow some chop but avoid fully sell-dominated coins.
        if buy_share < 0.35:
            reasons.append(f"buy share {buy_share:.2f} < 0.35")

    # Whales: only require if config actually asks for whales.
    if min_whale_count_5m > 0 or min_whale_size_sol > 0:
        if whales_5m < min_whale_count_5m and largest_buy_5m_sol < min_whale_size_sol:
            reasons.append("no meaningful whales in last 5m")

    # Slippage / microstructure
    if max_impact_bps > 0 and impact_bps > max_impact_bps:
        reasons.append(
            f"impact1k {impact_bps:.0f} bps > max {max_impact_bps:.0f} bps"
        )

    # Smart-wallet minimum (if enabled)
    if min_smart_score > 0.0 and smart_score < min_smart_score:
        reasons.append(
            f"smart_wallet_score {smart_score:.2f} < min {min_smart_score:.2f}"
        )

    # AI / momentum scores (also populates pf_* on meta)
    momentum_score, final_score, ai_rug_score = _compute_scores(meta)

    if ai_rug_score > max_rug_score:
        reasons.append(f"AI rug score {ai_rug_score:.2f} > max {max_rug_score:.2f}")

    # We require final score (regime-aware) to clear the bar.
    if final_score < min_final:
        reasons.append(
            f"final score {final_score:.2f} < min {min_final:.2f} (regime={regime_name})"
        )
    # If momentum is *way* below the bar (e.g. completely dead), still flag.
    elif momentum_score < (min_momentum * 0.6):
        reasons.append(
            f"momentum {momentum_score:.2f} << min {min_momentum:.2f} (dead trend, regime={regime_name})"
        )

    ok = len(reasons) == 0

    # Attach decision + profile for downstream / debugging
    profile = _build_profile(meta)

    dbg = meta.setdefault("_filter_debug", {})
    dbg.update(
        {
            "ok": ok,
            "reasons": reasons,
            "regime": regime_name,
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
            "impact_1k_bps": impact_bps,
            "smart_wallet_score": smart_score,
            "profile": profile,
        }
    )

    if not ok:
        msg = (
            f"[FILTER-REJECT] mint={meta.get('mint')} symbol={meta.get('symbol')} "
            f"dex={meta.get('dex') or meta.get('source')} regime={regime_name} "
            f"reasons={'; '.join(reasons)} "
            f"momentum={momentum_score:.2f} final={final_score:.2f}"
        )
        key = f"pair_filters.reject.{meta.get('mint', 'unknown')}"
        if hasattr(log, "every"):
            # type: ignore[attr-defined]
            log.every(key, 60, msg)
        else:
            log.debug(msg)

    return ok


# =============================================================================
# High-level evaluate helper (for AI / FeatureBuilder / logs)
# =============================================================================


def evaluate_pair(meta: Meta) -> Dict[str, Any]:
    """
    Rich wrapper around is_viable_pair().

    Returns a dict like:

        {
            "ok": bool,
            "momentum": float,
            "final": float,
            "risk_adjusted_final": float,
            "ai_rug_score": float,
            "profile": { ... },
            "reasons": [ ... ],
            "regime": "risk_on" | ...,
        }

    Side effects:
      - Calls is_viable_pair(meta) (so meta is fully enriched).
    """
    ok = is_viable_pair(meta)

    momentum = float(meta.get("pf_momentum_score") or 0.0)
    final_score = float(meta.get("pf_final_score") or 0.0)
    risk_adjusted = float(meta.get("pf_risk_adjusted_final") or 0.0)
    ai_rug = _resolve_ai_rug_score(meta)
    regime_name = _get_regime(meta)

    filt_dbg = meta.get("_filter_debug") or {}
    profile = meta.get("pf_profile") or filt_dbg.get("profile") or {}

    return {
        "ok": ok,
        "momentum": momentum,
        "final": final_score,
        "risk_adjusted_final": risk_adjusted,
        "ai_rug_score": ai_rug,
        "regime": regime_name,
        "reasons": list(filt_dbg.get("reasons") or []),
        "profile": profile,
    }
