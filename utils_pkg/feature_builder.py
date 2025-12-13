from __future__ import annotations

"""
utils_pkg/feature_builder.py

Canonical feature builder for AI models
(God-Tier Overhaul – V3+++ / quant-grade / Pump-aware / Phase 0–7).
===================================================================

Central place to turn a raw scanner / meta snapshot into a dense, numeric
feature dict that is consumed by:

  - utils_pkg.data_logger.log_entry_candidate(...)
  - utils_pkg.ai_entry_logic._heuristic_score(...)
  - utils_pkg.ai_buy_scoring.from_feature_vector(...)
  - utils_pkg.self_learning_ai_strategy (policy updates)
  - utils_pkg.risk_manager (capacity / Kelly / daily controls)
  - scripts/backtest.py & fine-tuning pipelines

Design goals
------------
- Pure + deterministic (no network / Redis / DB here).
- Robust to missing fields (defaults, fallbacks, clamping).
- Rich derived feature set:
    * liquidity / mcap / fdv (levels + logs + normalized/buckets)
    * age bands (very new / newish / mid / old / super-early)
    * volumes across horizons + ratios & intensity
    * orderflow (buy ratios, skew, net buys, vol per tx, buy pressure)
    * holders & whales (growth, whale share, whales per holder)
    * smart-wallet & whale alignment (from smart_wallets / scanner)
    * smart accumulation / panic dump / whale footprint / flow conflict
    * regime alignment (trend, liquidity, manipulation, volatility)
    * rug / impact normalization (AI fraud score, impact_1k_bps)
    * PF scores + risk-adjusted composites + micro-alpha score
    * microstructure from short-term history (returns, vol, trend flags)
    * risk context (open positions, DD, capacity, RR/SL/TP, Kelly hints)
    * high-level composites:
        - entry_quality_score
        - entry_quality_risk_adjusted
        - entry_quality_kelly_adjusted
        - entry_risk_score
        - entry_grade_bucket
        - entry_quality_vs_degen
        - position_size_scalar
        - position_size_raw_scalar
        - position_size_capacity_scalar
        - position_size_kelly_scalar
        - risk_capacity_score / risk_kelly_fraction_hint
        - entry_block_flag / entry_soft_block_flag
        - gate_*_ok flags (mirrors ai_buy_scoring pre-gates)
        - entry_tier_bucket / position_size_bucket
        - entry_score_v1 / size_scalar_v1 (stable aliases)
        - risk_daily_state & labels (recovery / neutral / heat)
    * scanner metadata (raydium / dexscreener / pumpfun / momo)
    * AI hooks (ai_buy_score, ai_entry_score, ai_confidence, etc.)
    * liquidity_regime_score + micro_risk_on_score/flag
    * Pump.fun 60s flow & stage features for dead-coin avoidance:
        - pump_stage_01 / pump_stage
        - pump_buyers_60s / pump_buys_60s / pump_sol_60s
        - pump_idle_sec / pump_is_idle_heavy
        - pump_velocity_score

Backwards-compat
----------------
Public API remains:

    build_features(snapshot: Mapping[str, Any]) -> Dict[str, float]

    fb = FeatureBuilder()
    feats = fb.build(
        symbol="ABC",
        mint=mint_str,
        snapshot=last_snapshot_dict,
        history=recent_snapshots_list,   # optional
        regime=regime_state,             # optional (RegimeState)
        wallet_signals=wallet_dict,      # optional (from smart_wallets)
        ai_signals=ai_dict,              # optional (AI fraud/entry outputs)
        scanner_source="raydium",        # optional
        risk_context=risk_ctx_dict,      # optional (open positions, DD, etc.),
    )

Anything that already depended on the old feature names keeps working.
We only add new fields; we do not rename or remove existing ones.
"""

import math
from typing import Any, Dict, Mapping, Optional, Sequence

from logging_setup import get_logger

log = get_logger(__name__)

# Optional: regime integration (we only need as_numeric()/get())
try:  # pragma: no cover
    from utils_pkg.regime import RegimeState  # type: ignore
except Exception:  # pragma: no cover
    RegimeState = None  # type: ignore


# Bump when adding / changing composite logic (NOT on every tiny tweak).
# This is the canonical feature-vector version the rest of the stack uses.
FEATURE_VERSION: float = 7.00  # God-Tier V3+++ / quant-grade (Phase 0–7 final)
FEATURE_SCHEMA_NAME: str = "god_tier_v3_quant_phase0_7"
FEATURE_PHASE: float = 7.0  # numeric tag for Phase 0–7 stack


# ============================================================================ #
# Low-level helpers
# ============================================================================ #


def _f(src: Mapping[str, Any], key: str, default: float = 0.0) -> float:
    """Float getter with fallback."""
    try:
        v = src.get(key, default)
        return float(v)
    except Exception:
        return float(default)


def _i(src: Mapping[str, Any], key: str, default: int = 0) -> int:
    """Int getter with fallback."""
    try:
        v = src.get(key, default)
        return int(v)
    except Exception:
        return int(default)


def _b(src: Mapping[str, Any], key: str, default: bool = False) -> float:
    """Bool getter mapped to 0.0/1.0 with fallback."""
    try:
        v = src.get(key, default)
        return 1.0 if bool(v) else 0.0
    except Exception:
        return 1.0 if default else 0.0


def _clip(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, float(x)))


def _clip01(x: float) -> float:
    return _clip(x, 0.0, 1.0)


def _log1p_pos(x: float) -> float:
    """Safe log1p; negative / zero -> 0.0."""
    try:
        if x <= 0:
            return 0.0
        return math.log1p(float(x))
    except Exception:
        return 0.0


def _safe_ratio(num: float, den: float, default: float = 0.0) -> float:
    """Safe ratio num/den with default on zero / bad values."""
    try:
        d = float(den)
        if d == 0:
            return default
        return float(num) / d
    except Exception:
        return default


def _nan_zero(v: Any) -> float:
    """Normalize NaN/inf/garbage to 0.0."""
    try:
        f = float(v)
        if math.isnan(f) or math.isinf(f):
            return 0.0
        return f
    except Exception:
        return 0.0


def _norm_log_scale(x: float, ref: float) -> float:
    """
    Map a positive value into ~0..1 using log1p(x) / log1p(ref).
    If ref is small or zero, fall back to 0.
    """
    try:
        if ref <= 0:
            return 0.0
        num = _log1p_pos(x)
        den = _log1p_pos(ref)
        if den <= 0:
            return 0.0
        return _clip01(num / den)
    except Exception:
        return 0.0


def _net_minus_one_to_one(pos: float, neg: float) -> float:
    """
    Convert (positive_count, negative_count) → net in [-1, 1].
      net = (pos - neg) / (pos + neg + eps)
    """
    p = max(0.0, pos)
    n = max(0.0, neg)
    denom = p + n
    if denom <= 0:
        return 0.0
    return float(max(-1.0, min(1.0, (p - n) / denom)))


# ============================================================================ #
# Core snapshot → feature map (used by logger + AI entry/buy logic)
# ============================================================================ #


def build_features(snapshot: Mapping[str, Any]) -> Dict[str, float]:
    """
    Convert a raw PF/snapshot dict into a dense numeric feature map.

    Expected snapshot fields (best-effort, all optional):

      liq_usd, mcap_usd, fdv_usd, token_age_min/age_min/ageMinutes/age
      vol_1m_usd, vol_5m_usd/vol5, vol_15m_usd, vol_1h_usd/vol1h, vol_24h_usd/vol24h
      buys_1m, sells_1m, buys_5m/buys5, sells_5m/sells5, txns_5m
      price_usd/px_usd/price/p
      price_change_1m_pct, price_change_5m_pct/chg5,
          price_change_15m_pct/chg15, price_change_1h_pct/chg1h
      holders, holders_5m_change/holders_delta_5m
      whales_5m, whale_vol_5m_usd, largest_buy_5m_sol,
      whale_share_5m, whale_buy_score, whale_sell_score,
      smart_wallet_score, smart_flow_raw, smart_wallet_count_5m,
      smart_buy_score_5m, smart_sell_score_5m,
      smart_last_buy_sec_ago, smart_last_sell_sec_ago,
      smart_last_cluster_is_alpha, unique_buyers_5m
      smart_net_side_5m, whale_net_side_5m, smart_alpha_bias
      smart_whale_alignment, smart_whale_alignment_abs,
      whale_domination_5m, smart_vs_whale_conflict,
      smart_accumulation_score, smart_panic_dump_score,
      whale_liquidity_footprint

      impact_1k_bps/impact_1000usd_bps
      ai_rug_score/ai_rug_score_01/ai_fraud_score

      regime_score / regime_numeric / regime_trend / regime_liquidity
      regime_manipulation_risk

      pf_momentum_score, pf_final_score, pf_risk_adjusted_final

      Pump.fun-specific (optional, used by pump_filters & AI):
      -------------------------------------------------------
      pump_stage, pump_stage_01, pf_stage, pf_curve_progress
      pump_buyers_60s, buyers_60s
      pump_buys_60s, buys_60s
      pump_sol_60s, vol_60s_sol, vol_60s_base
      pump_idle_sec, idle_sec, secs_since_last_trade

    We tolerate missing fields and derive as many stable features as possible.
    """
    raw = dict(snapshot)  # shallow copy for safety
    feats: Dict[str, Any] = {}

    # Meta / versioning – keep version stable, extend vector only
    feats["feature_version"] = FEATURE_VERSION
    feats["feature_schema"] = FEATURE_SCHEMA_NAME
    feats["feature_phase"] = FEATURE_PHASE

    # ------------------------------------------------------------------ #
    # Core size / age
    # ------------------------------------------------------------------ #
    liq_usd = _f(raw, "liq_usd", _f(raw, "liq", _f(raw, "liquidity", 0.0)))
    mcap_usd = _f(raw, "mcap_usd", _f(raw, "mcap", 0.0))
    fdv_usd = _f(raw, "fdv_usd", _f(raw, "fdv", 0.0))
    token_age_min = _f(
        raw,
        "token_age_min",
        _f(raw, "age_min", _f(raw, "ageMinutes", _f(raw, "age", 0.0))),
    )
    if token_age_min < 0:
        token_age_min = 0.0
    token_age_hr = token_age_min / 60.0 if token_age_min > 0 else 0.0
    token_age_sec = token_age_min * 60.0 if token_age_min > 0 else 0.0

    feats["liq_usd"] = liq_usd
    feats["mcap_usd"] = mcap_usd
    feats["fdv_usd"] = fdv_usd
    feats["token_age_min"] = token_age_min
    feats["token_age_hr"] = token_age_hr
    feats["token_age_sec"] = token_age_sec

    feats["log_liq_usd"] = _log1p_pos(liq_usd)
    feats["log_mcap_usd"] = _log1p_pos(mcap_usd)
    feats["log_fdv_usd"] = _log1p_pos(fdv_usd)

    # Normalized size (rough reference scales tuned for Solana meme land)
    feats["liq_norm"] = _norm_log_scale(liq_usd, 1_000_000.0)        # ref 1M liq
    feats["mcap_norm"] = _norm_log_scale(mcap_usd, 10_000_000.0)     # ref 10M mcap
    feats["fdv_norm"] = _norm_log_scale(fdv_usd, 20_000_000.0)       # ref 20M fdv

    # Age buckets
    is_very_new = 1.0 if token_age_min <= 30 else 0.0
    is_newish = 1.0 if (30 < token_age_min <= 180) else 0.0
    is_mid = 1.0 if (180 < token_age_min <= 720) else 0.0
    is_old = 1.0 if token_age_min > 240 else 0.0  # note: overlaps mid for 240..720

    feats["is_very_new"] = is_very_new
    feats["is_newish"] = is_newish
    feats["is_mid_age"] = is_mid
    feats["is_old"] = is_old
    feats["is_super_early"] = 1.0 if token_age_min <= 10 else 0.0

    # ------------------------------------------------------------------ #
    # Volumes
    # ------------------------------------------------------------------ #
    vol_1m = _f(raw, "vol_1m_usd", raw.get("volume_1m_usd", 0.0))
    vol_5m = _f(
        raw,
        "vol_5m_usd",
        raw.get("vol5", raw.get("volume_5m_usd", raw.get("vol_5m", 0.0))),
    )
    vol_15m = _f(raw, "vol_15m_usd", raw.get("volume_15m_usd", 0.0))
    vol_1h = _f(
        raw,
        "vol_1h_usd",
        raw.get("vol1h", raw.get("volume_1h_usd", 0.0)),
    )
    vol_24h = _f(
        raw,
        "vol_24h_usd",
        raw.get("vol24h", raw.get("volume_24h_usd", 0.0)),
    )

    feats["vol_1m_usd"] = vol_1m
    feats["vol_5m_usd"] = vol_5m
    feats["vol_15m_usd"] = vol_15m
    feats["vol_1h_usd"] = vol_1h
    feats["vol_24h_usd"] = vol_24h

    feats["log_vol_5m_usd"] = _log1p_pos(vol_5m)
    feats["log_vol_24h_usd"] = _log1p_pos(vol_24h)

    # Ratios (short-term vs higher TF)
    feats["vol_ratio_5m_1h"] = _safe_ratio(vol_5m, max(vol_1h, 1.0))
    feats["vol_ratio_5m_24h"] = _safe_ratio(vol_5m, max(vol_24h, 1.0))
    feats["vol_ratio_1m_5m"] = _safe_ratio(vol_1m, max(vol_5m, 1.0))

    # Volume intensity vs size
    feats["vol_5m_per_mcap"] = _safe_ratio(vol_5m, max(mcap_usd, 1.0))
    feats["vol_5m_per_liq"] = _safe_ratio(vol_5m, max(liq_usd, 1.0))

    # Intraday “heat” proxy: how loud is 5m vs 24h?
    feats["volume_heat_intraday"] = _clip01(
        _norm_log_scale(vol_5m, max(vol_24h, 1.0)) * 1.5
    )

    # ------------------------------------------------------------------ #
    # Trades / orderflow
    # ------------------------------------------------------------------ #
    buys_1m = _i(raw, "buys_1m", 0)
    sells_1m = _i(raw, "sells_1m", 0)
    buys_5m = _i(raw, "buys_5m", _i(raw, "buys5", 0))
    sells_5m = _i(raw, "sells_5m", _i(raw, "sells5", 0))
    txns_5m = _i(raw, "txns_5m", buys_5m + sells_5m)

    feats["buys_1m"] = buys_1m
    feats["sells_1m"] = sells_1m
    feats["buys_5m"] = buys_5m
    feats["sells_5m"] = sells_5m
    feats["txns_5m"] = txns_5m

    buy_ratio_5m = _safe_ratio(buys_5m + 1.0, txns_5m + 2.0, 0.5)  # smoothed
    buy_ratio_1m = _safe_ratio(
        buys_1m + 1.0,
        buys_1m + sells_1m + 2.0,
        0.5,
    )
    net_buys_5m = float(buys_5m - sells_5m)
    orderflow_skew_5m = _safe_ratio(buys_5m + 1.0, sells_5m + 1.0, 1.0) - 1.0
    orderflow_net_norm_5m = _net_minus_one_to_one(float(buys_5m), float(sells_5m))

    feats["buy_ratio_5m"] = _clip01(buy_ratio_5m)
    feats["buy_ratio_1m"] = _clip01(buy_ratio_1m)
    feats["net_buys_5m"] = net_buys_5m
    feats["orderflow_skew_5m"] = orderflow_skew_5m
    feats["orderflow_net_5m_norm"] = orderflow_net_norm_5m

    vol_per_tx_5m = vol_5m / max(txns_5m, 1.0)
    feats["vol_per_tx_5m_usd"] = vol_per_tx_5m
    feats["liq_per_trade_5m_usd"] = _safe_ratio(liq_usd, max(txns_5m, 1.0))

    # Base buy pressure (before smart/whale overlay)
    base_buy_press = (
        _clip(orderflow_skew_5m, -2.0, 2.0) / 2.0
        + _clip(vol_5m / max(mcap_usd, 1.0), 0.0, 0.5)
    )
    feats["buy_pressure_score"] = _clip(base_buy_press, -1.0, 2.0)

    # Trade intensity metrics
    feats["trade_intensity_5m"] = _safe_ratio(txns_5m, 5.0, 0.0)  # txns / minute
    feats["buy_intensity_5m"] = _safe_ratio(buys_5m, 5.0, 0.0)
    feats["sell_intensity_5m"] = _safe_ratio(sells_5m, 5.0, 0.0)

    # ------------------------------------------------------------------ #
    # Price + momentum
    # ------------------------------------------------------------------ #
    price_usd = _f(
        raw,
        "price_usd",
        raw.get("px_usd", raw.get("price", raw.get("p", 0.0))),
    )
    chg_1m = _f(raw, "price_change_1m_pct", raw.get("chg1", 0.0))
    chg_5m = _f(
        raw,
        "price_change_5m_pct",
        raw.get("chg5", raw.get("price_chg_5m_pct", 0.0)),
    )
    chg_15m = _f(raw, "price_change_15m_pct", raw.get("chg15", 0.0))
    chg_1h = _f(raw, "price_change_1h_pct", raw.get("chg1h", 0.0))

    # Clamp extremes (protect models from outliers / bad ticks)
    chg_1m_c = _clip(chg_1m, -60.0, 80.0)
    chg_5m_c = _clip(chg_5m, -60.0, 120.0)
    chg_15m_c = _clip(chg_15m, -80.0, 180.0)
    chg_1h_c = _clip(chg_1h, -90.0, 250.0)

    feats["price_usd"] = price_usd
    feats["price_change_1m_pct"] = chg_1m_c
    feats["price_change_5m_pct"] = chg_5m_c
    feats["price_change_15m_pct"] = chg_15m_c
    feats["price_change_1h_pct"] = chg_1h_c

    # Simple momentum composite signals
    short_mom = max(0.0, chg_1m_c) * 0.4 + max(0.0, chg_5m_c) * 0.6
    mid_mom = max(0.0, chg_15m_c) * 0.6 + max(0.0, chg_1h_c) * 0.4

    feats["momentum_short_pct"] = short_mom
    feats["momentum_mid_pct"] = mid_mom

    # Aggressive downside / upside flags
    feats["has_fast_dump_5m"] = 1.0 if chg_5m_c <= -20.0 else 0.0
    feats["has_fast_pump_5m"] = 1.0 if chg_5m_c >= 30.0 else 0.0

    # Pump-chasing risk flag for ai_entry_logic heuristics
    pump_chase = 1.0 if chg_1m_c > 10.0 and chg_5m_c > 25.0 else 0.0
    feats["is_pump_chasing_risk"] = pump_chase

    # Coarse momentum regime flags (for AI routing)
    feats["momentum_regime_bear"] = 1.0 if chg_5m_c <= -10.0 else 0.0
    feats["momentum_regime_flat"] = 1.0 if -5.0 < chg_5m_c < 5.0 else 0.0
    feats["momentum_regime_bull"] = 1.0 if chg_5m_c >= 10.0 else 0.0

    # ------------------------------------------------------------------ #
    # Holders / whales / smart wallets
    # ------------------------------------------------------------------ #
    holders = _i(raw, "holders", _i(raw, "holders_total", 0))
    holders_5m_change = _i(
        raw,
        "holders_5m_change",
        _i(raw, "holders_delta_5m", 0),
    )

    feats["holders"] = holders
    feats["holders_5m_change"] = holders_5m_change

    # Approx relative growth in the last 5m (bounded)
    holders_growth_5m = _safe_ratio(holders_5m_change, max(holders, 1), 0.0)
    feats["holders_growth_5m"] = _clip(holders_growth_5m, -0.5, 1.0)  # -50%..+100%

    whales_5m = _i(raw, "whales_5m", _i(raw, "whale_buys_5m", 0))
    whale_vol_5m = _f(
        raw,
        "whale_vol_5m_usd",
        raw.get("whale_volume_5m_usd", 0.0),
    )
    largest_buy_5m_sol = _f(
        raw,
        "largest_buy_5m_sol",
        raw.get("largest_buy_5m", 0.0),
    )

    feats["whales_5m"] = whales_5m
    feats["whale_vol_5m_usd"] = whale_vol_5m
    feats["largest_buy_5m_sol"] = largest_buy_5m_sol

    whale_share_5m_raw = _f(
        raw,
        "whale_share_5m",
        _safe_ratio(whale_vol_5m, max(vol_5m, 1.0), 0.0),
    )
    feats["whale_share_5m"] = _clip01(whale_share_5m_raw)

    feats["whales_per_holder"] = _safe_ratio(float(whales_5m), max(holders, 1.0))

    # smart-wallet signals (from smart_wallets.build_features or snapshot)
    smart_wallet_score = _f(raw, "smart_wallet_score", 0.0)
    smart_flow_raw = _f(raw, "smart_flow_raw", 0.0)
    smart_wallet_count_5m = _f(raw, "smart_wallet_count_5m", 0.0)
    smart_buy_score_5m = _f(raw, "smart_buy_score_5m", 0.0)
    smart_sell_score_5m = _f(raw, "smart_sell_score_5m", 0.0)
    smart_last_buy_sec_ago = _f(raw, "smart_last_buy_sec_ago", 1e9)
    smart_last_sell_sec_ago = _f(raw, "smart_last_sell_sec_ago", 1e9)
    smart_last_cluster_is_alpha = _f(raw, "smart_last_cluster_is_alpha", 0.0)
    unique_buyers_5m = _f(raw, "unique_buyers_5m", 0.0)
    whale_buy_score = _f(raw, "whale_buy_score", 0.0)
    whale_sell_score = _f(raw, "whale_sell_score", 0.0)

    # V3+++ wallet extras (optional, directly from smart_wallets tracker)
    smart_net_side_5m = _f(raw, "smart_net_side_5m", 0.0)
    whale_net_side_5m = _f(raw, "whale_net_side_5m", 0.0)
    smart_alpha_bias = _f(raw, "smart_alpha_bias", 0.0)
    smart_whale_alignment_v3 = _f(raw, "smart_whale_alignment", 0.0)
    smart_whale_alignment_abs = _f(raw, "smart_whale_alignment_abs", 0.0)
    whale_domination_5m = _f(raw, "whale_domination_5m", 0.0)
    smart_vs_whale_conflict = _f(raw, "smart_vs_whale_conflict", 0.0)
    smart_accumulation_score = _f(raw, "smart_accumulation_score", 0.0)
    smart_panic_dump_score = _f(raw, "smart_panic_dump_score", 0.0)
    whale_liquidity_footprint_raw = _f(raw, "whale_liquidity_footprint", 0.0)

    # Approximate net sides if missing but we have buy/sell scores
    if smart_net_side_5m == 0.0 and (smart_buy_score_5m or smart_sell_score_5m):
        smart_net_side_5m = _net_minus_one_to_one(
            smart_buy_score_5m, smart_sell_score_5m
        )
    if whale_net_side_5m == 0.0 and (whale_buy_score or whale_sell_score):
        whale_net_side_5m = _net_minus_one_to_one(whale_buy_score, whale_sell_score)
    # If smart_alpha_bias missing but cluster + events present
    if (
        smart_alpha_bias == 0.0
        and smart_last_cluster_is_alpha > 0.0
        and smart_wallet_count_5m > 0.0
    ):
        smart_alpha_bias = _clip01(smart_wallet_count_5m / 5.0)

    feats["smart_wallet_score"] = smart_wallet_score
    feats["smart_flow_raw"] = smart_flow_raw
    feats["smart_wallet_count_5m"] = smart_wallet_count_5m
    feats["smart_buy_score_5m"] = smart_buy_score_5m
    feats["smart_sell_score_5m"] = smart_sell_score_5m
    feats["smart_last_buy_sec_ago"] = smart_last_buy_sec_ago
    feats["smart_last_sell_sec_ago"] = smart_last_sell_sec_ago
    feats["smart_last_cluster_is_alpha"] = smart_last_cluster_is_alpha
    feats["unique_buyers_5m"] = unique_buyers_5m
    feats["whale_buy_score"] = whale_buy_score
    feats["whale_sell_score"] = whale_sell_score
    feats["smart_net_side_5m"] = smart_net_side_5m
    feats["whale_net_side_5m"] = whale_net_side_5m
    feats["smart_alpha_bias"] = smart_alpha_bias
    feats["smart_whale_alignment_v3"] = smart_whale_alignment_v3
    feats["smart_whale_alignment_abs"] = smart_whale_alignment_abs
    feats["whale_domination_5m"] = whale_domination_5m
    feats["smart_vs_whale_conflict"] = smart_vs_whale_conflict
    feats["smart_accumulation_score"] = smart_accumulation_score
    feats["smart_panic_dump_score"] = smart_panic_dump_score
    feats["whale_liquidity_footprint"] = _clip01(whale_liquidity_footprint_raw)

    # Derived: smart/whale alignment & participation (legacy-style composite)
    smart_bias_01 = 0.5 * (max(-1.0, min(1.0, smart_flow_raw)) + 1.0)
    smart_recent_buy = 1.0 if smart_last_buy_sec_ago < 300 else 0.0  # <5m
    smart_recent_sell = 1.0 if smart_last_sell_sec_ago < 300 else 0.0

    feats["smart_flow_bias_01"] = smart_bias_01
    feats["smart_recent_buy_flag"] = smart_recent_buy
    feats["smart_recent_sell_flag"] = smart_recent_sell

    feats["unique_buyers_5m_norm"] = _norm_log_scale(unique_buyers_5m, 50.0)
    feats["whale_to_unique_ratio"] = _safe_ratio(
        whales_5m, max(unique_buyers_5m, 1.0)
    )

    smart_buy_term = _clip01(smart_buy_score_5m)
    smart_sell_term = _clip01(smart_sell_score_5m)
    smart_net_bias = _clip(smart_net_side_5m, -1.0, 1.0)
    whale_net_bias = _clip(whale_net_side_5m, -1.0, 1.0)

    smart_whale_alignment_legacy = (
        0.20 * smart_wallet_score
        + 0.15 * smart_buy_term
        - 0.10 * smart_sell_term
        + 0.20 * whale_buy_score
        - 0.10 * whale_sell_score
        + 0.15 * _clip01(buy_ratio_5m)
        + 0.10 * (0.5 * (smart_net_bias + 1.0))  # map [-1,1]→[0,1]
        + 0.10 * (0.5 * (whale_net_bias + 1.0))
    )
    feats["smart_whale_alignment"] = _clip(smart_whale_alignment_legacy, 0.0, 1.5)

    # Consolidated alignment: prefer explicit V3 value if present
    if smart_whale_alignment_v3 != 0.0:
        consolidated_align = smart_whale_alignment_v3
    else:
        consolidated_align = _clip01(smart_whale_alignment_legacy)
    feats["smart_whale_alignment_consolidated"] = _clip01(consolidated_align)

    # Alpha-cluster boost (for "follow the killers" behavior)
    alpha_flow_boost = smart_alpha_bias * _clip01(smart_wallet_score)
    feats["smart_alpha_flow_boost"] = _clip(alpha_flow_boost, 0.0, 1.0)

    # Flow regime flags: accumulation vs dump vs conflict (for heuristics / AI)
    feats["flow_accumulation_score"] = _clip01(
        smart_accumulation_score
        if smart_accumulation_score > 0.0
        else max(0.0, smart_net_bias) * max(0.0, whale_net_bias)
    )
    feats["flow_panic_dump_score"] = _clip01(
        smart_panic_dump_score
        if smart_panic_dump_score > 0.0
        else max(0.0, -smart_net_bias) * max(0.0, -whale_net_bias)
    )
    feats["flow_conflict_score"] = _clip01(smart_vs_whale_conflict)

    # Binary flags from flow scores
    feats["flow_is_strong_accum"] = (
        1.0 if feats["flow_accumulation_score"] >= 0.6 else 0.0
    )
    feats["flow_is_panic_dump"] = (
        1.0 if feats["flow_panic_dump_score"] >= 0.6 else 0.0
    )
    feats["flow_is_conflicting"] = (
        1.0 if feats["flow_conflict_score"] >= 0.6 else 0.0
    )

    # ------------------------------------------------------------------ #
    # Rug / impact normalization
    # ------------------------------------------------------------------ #
    ai_rug_raw = _f(
        raw,
        "ai_rug_score_01",
        raw.get("ai_rug_score", raw.get("ai_fraud_score", 0.0)),
    )
    if ai_rug_raw > 1.5:  # treat as 0..100 if big
        ai_rug_01 = _clip01(ai_rug_raw / 100.0)
    else:
        ai_rug_01 = _clip01(ai_rug_raw)

    feats["ai_rug_score_01"] = ai_rug_01
    # Alias used by data_logger / AI modules as a generic fraud metric
    feats["ai_fraud_score"] = ai_rug_01

    impact_1k_bps = _f(
        raw,
        "impact_1k_bps",
        raw.get("impact_1000usd_bps", 0.0),
    )
    feats["impact_1k_bps"] = impact_1k_bps
    feats["impact_1k_pct"] = impact_1k_bps / 100.0

    # ------------------------------------------------------------------ #
    # PF scores (momentum / final / risk-adjusted)
    # ------------------------------------------------------------------ #
    pf_mom = _f(raw, "pf_momentum_score", raw.get("momentum_score", 0.0))
    pf_final = _f(raw, "pf_final_score", raw.get("final_score", 0.0))
    pf_risk_adj = _f(raw, "pf_risk_adjusted_final", 0.0)

    if pf_risk_adj == 0.0 and pf_final > 0.0:
        pf_risk_adj = max(0.0, pf_final * (1.0 - ai_rug_01))

    feats["pf_momentum_score"] = _clip01(pf_mom)
    feats["pf_final_score"] = _clip01(pf_final)
    feats["pf_risk_adjusted_final"] = _clip01(pf_risk_adj)

    feats["pf_momo_risk_adj"] = _clip01(
        feats["pf_momentum_score"] * (1.0 - ai_rug_01)
    )

    # ------------------------------------------------------------------ #
    # Pump.fun dynamic flow / idle extras (for pump_filters & AI)
    # ------------------------------------------------------------------ #
    # Stage may come as 0..1, 0..100, or similar; normalize to 0..1.
    pump_stage_raw = _f(
        raw,
        "pump_stage",
        _f(raw, "pump_stage_01", _f(raw, "pf_stage", _f(raw, "pf_curve_progress", 0.0))),
    )
    if pump_stage_raw > 1.0:
        if pump_stage_raw <= 100.0:
            pump_stage_01 = pump_stage_raw / 100.0
        else:
            pump_stage_01 = pump_stage_raw / 1000.0
    else:
        pump_stage_01 = pump_stage_raw
    pump_stage_01 = _clip01(pump_stage_01)

    pump_buyers_60s = _i(raw, "pump_buyers_60s", _i(raw, "buyers_60s", 0))
    pump_buys_60s = _i(raw, "pump_buys_60s", _i(raw, "buys_60s", 0))
    pump_sol_60s = _f(
        raw,
        "pump_sol_60s",
        _f(raw, "vol_60s_sol", _f(raw, "vol_60s_base", 0.0)),
    )
    pump_idle_sec = _f(
        raw,
        "pump_idle_sec",
        _f(raw, "idle_sec", _f(raw, "secs_since_last_trade", 0.0)),
    )

    feats["pump_stage_01"] = pump_stage_01
    feats["pump_stage"] = pump_stage_01 * 100.0
    feats["pump_buyers_60s"] = pump_buyers_60s
    feats["pump_buys_60s"] = pump_buys_60s
    feats["pump_sol_60s"] = pump_sol_60s
    feats["pump_idle_sec"] = pump_idle_sec
    feats["pump_is_idle_heavy"] = 1.0 if pump_idle_sec >= 120.0 else 0.0

    # Velocity: how "alive" is the pump in the last 60s.
    vel = 0.0
    vel += _safe_ratio(pump_buys_60s, 10.0, 0.0)    # 10+ buys/min is strong
    vel += _safe_ratio(pump_buyers_60s, 5.0, 0.0)   # buyer diversity
    vel += _safe_ratio(pump_sol_60s, 5.0, 0.0)      # raw SOL flow

    # Early-stage boost, late-stage penalty.
    early_factor = 1.0 + (1.0 - pump_stage_01)
    # Idle penalty (heavy idle kills velocity).
    if pump_idle_sec > 0.0:
        idle_pen = _clip01(pump_idle_sec / 120.0)  # 2m idle → ~1
        early_factor *= (1.0 - 0.5 * idle_pen)

    vel *= early_factor
    pump_velocity_score = _clip(vel, 0.0, 20.0)
    feats["pump_velocity_score"] = pump_velocity_score

    # ------------------------------------------------------------------ #
    # Entry micro-alpha: local momentum + orderflow + smart/whale alignment
    #                    + alpha flow + (optional) pump velocity.
    # ------------------------------------------------------------------ #
    pump_vel_norm = _clip01(_safe_ratio(pump_velocity_score, 10.0, 0.0))

    entry_micro = (
        0.22 * _clip01(short_mom / 50.0)
        + 0.20 * _clip01(mid_mom / 80.0)
        + 0.18 * _clip01(base_buy_press)
        + 0.18 * _clip01(consolidated_align)
        + 0.12 * _clip01(alpha_flow_boost)
        + 0.10 * pump_vel_norm
    )
    feats["entry_micro_alpha"] = _clip(entry_micro, 0.0, 1.5)
    # Alias for AI models / clarity
    feats["alpha_micro_score"] = feats["entry_micro_alpha"]

    # ------------------------------------------------------------------ #
    # Regime alignment (if present in snapshot)
    # ------------------------------------------------------------------ #
    regime_score = _f(raw, "regime_score", raw.get("regime_numeric", 0.0))
    regime_score = _clip(regime_score, -1.0, 1.0)
    feats["regime_score"] = regime_score
    feats["regime_numeric"] = regime_score
    feats["regime_is_risk_on"] = 1.0 if regime_score >= 0.5 else 0.0
    feats["regime_is_risk_off"] = 1.0 if regime_score <= -0.5 else 0.0

    regime_trend = _f(raw, "regime_trend", regime_score)
    regime_liquidity = _f(raw, "regime_liquidity", 0.5)
    regime_manip = _f(raw, "regime_manipulation_risk", 0.0)

    feats["regime_trend"] = _clip(regime_trend, -1.0, 1.0)
    feats["regime_liquidity"] = _clip01(regime_liquidity)
    feats["regime_manipulation_risk"] = _clip01(regime_manip)

    # Regime “volatility” proxy from volume + price swings
    vol_norm = _norm_log_scale(vol_5m, max(vol_24h, 1.0))
    mom_norm = _clip01(abs(chg_5m_c) / 80.0)
    feats["regime_volatility"] = _clip01(0.5 * vol_norm + 0.5 * mom_norm)

    # ------------------------------------------------------------------ #
    # Liquidity buckets (soft one-hot style)
    # ------------------------------------------------------------------ #
    # Buckets tuned for Solana memecoins:
    #   ultra_low   < 8k
    #   low         8k - 20k
    #   sweet_spot  20k - 120k
    #   mid         120k - 500k
    #   high        > 500k
    liq_ultra_low = 1.0 if liq_usd < 8_000 else 0.0
    liq_low = 1.0 if 8_000 <= liq_usd < 20_000 else 0.0
    liq_sweet = 1.0 if 20_000 <= liq_usd < 120_000 else 0.0
    liq_mid = 1.0 if 120_000 <= liq_usd < 500_000 else 0.0
    liq_high = 1.0 if liq_usd >= 500_000 else 0.0

    feats["liq_bucket_ultra_low"] = liq_ultra_low
    feats["liq_bucket_low"] = liq_low
    feats["liq_bucket_sweet"] = liq_sweet
    feats["liq_bucket_mid"] = liq_mid
    feats["liq_bucket_high"] = liq_high

    # mcap “early vs late” buckets
    mcap_early = 1.0 if 150_000 <= mcap_usd <= 6_000_000 else 0.0
    mcap_very_late = 1.0 if mcap_usd > 25_000_000 else 0.0

    feats["mcap_bucket_early"] = mcap_early
    feats["mcap_bucket_very_late"] = mcap_very_late

    # Structural quality score: combines liquidity, mcap, age, rug risk
    structural_quality = (
        feats["liq_norm"] * 0.4
        + (1.0 - feats["ai_rug_score_01"]) * 0.3
        + (1.0 - feats["is_very_new"]) * 0.1
        + feats["liq_bucket_sweet"] * 0.2
    )
    feats["structural_quality_score"] = _clip(structural_quality, 0.0, 1.5)

    # Dead-coin heuristic: low liq, low 5m volume, negative or flat momentum
    dead_like = (
        (1.0 if liq_usd < 15_000 else 0.0)
        + (1.0 if vol_5m < 1_000 else 0.0)
        + (1.0 if chg_5m_c <= 0.0 else 0.0)
    )
    feats["is_dead_like"] = 1.0 if dead_like >= 2.0 else 0.0

    # ------------------------------------------------------------------ #
    # Pump.fun / bonding-curve extras (optional; only if present)
    # ------------------------------------------------------------------ #
    pf_curve_progress = _f(
        raw,
        "pf_curve_progress",
        raw.get("pf_curve_progress_pct", 0.0),
    )
    pf_minted_pct = _f(raw, "pf_minted_pct", 0.0)
    pf_creator_hold_pct = _f(raw, "pf_creator_hold_pct", 0.0)
    pf_bot_score = _f(raw, "pf_bot_score", 0.0)
    pf_sniper_density = _f(raw, "pf_sniper_density", 0.0)

    feats["pf_curve_progress"] = pf_curve_progress
    feats["pf_minted_pct"] = pf_minted_pct
    feats["pf_creator_hold_pct"] = pf_creator_hold_pct
    feats["pf_bot_score"] = pf_bot_score
    feats["pf_sniper_density"] = pf_sniper_density

    # Simple “bonding curve danger” heuristic
    pf_curve_danger = 0.0
    if pf_curve_progress > 75.0:
        pf_curve_danger += 0.4
    if pf_creator_hold_pct > 25.0:
        pf_curve_danger += 0.3
    if pf_bot_score > 0.5:
        pf_curve_danger += 0.3
    feats["pf_curve_danger_score"] = _clip01(pf_curve_danger)

    # ------------------------------------------------------------------ #
    # Misc optional passthroughs (kept numeric where possible)
    # ------------------------------------------------------------------ #
    if "src_id" in raw:
        feats["src_id"] = _nan_zero(raw["src_id"])
    if "scanner_source_id" in raw:
        feats["scanner_source_id"] = _nan_zero(raw["scanner_source_id"])

    # Optional: scanner-side ranking info (for backtests / AI)
    if "scanner_rank" in raw:
        feats["scanner_rank"] = _nan_zero(raw["scanner_rank"])
    if "scanner_total" in raw:
        feats["scanner_total"] = _nan_zero(raw["scanner_total"])
        feats["scanner_rank_frac"] = _safe_ratio(
            feats.get("scanner_rank", 0.0), max(feats["scanner_total"], 1.0), 0.0
        )

    # ------------------------------------------------------------------ #
    # Extra composites for V3+++ (liquidity-regime + micro risk-on)
    # ------------------------------------------------------------------ #
    feats["liquidity_regime_score"] = _clip01(
        0.5 * feats.get("liq_norm", 0.0)
        + 0.3 * feats.get("volume_heat_intraday", 0.0)
        + 0.2 * feats.get("regime_liquidity", 0.0)
    )

    micro_risk = (
        0.4 * _clip01(chg_5m_c / 40.0)
        + 0.2 * feats.get("buy_pressure_score", 0.0)
        + 0.2 * feats.get("smart_whale_alignment_consolidated", 0.0)
        + 0.2 * max(0.0, regime_score)
    )
    feats["micro_risk_on_score"] = _clip01(micro_risk)
    feats["micro_risk_on_flag"] = 1.0 if feats["micro_risk_on_score"] >= 0.6 else 0.0

    # Final pass: ensure numeric types everywhere
    return {k: _nan_zero(v) for k, v in feats.items()}


# ============================================================================ #
# Backwards-compatible FeatureBuilder class
# ============================================================================ #


class FeatureBuilder:
    """
    Central feature factory for the sniper bot.

    Usage:

        fb = FeatureBuilder()
        feats = fb.build(
            symbol="ABC",
            mint=mint_str,
            snapshot=last_snapshot_dict,
            history=recent_snapshots_list,   # optional
            regime=regime_state,             # optional (RegimeState)
            wallet_signals=wallet_dict,      # optional (from smart_wallets)
            ai_signals=ai_dict,              # optional (AI fraud/entry outputs)
            scanner_source="raydium",        # optional
            risk_context=risk_ctx_dict,      # optional (open positions, DD, etc.),
        )
    """

    def __init__(
        self,
        *xargs: Any,
        new_age_threshold_min: float = 60.0,  # kept for compatibility (currently unused)
        **xkwargs: Any,
    ) -> None:
        # xargs/xkwargs are accepted only for backward compatibility in case some
        # older caller passed unexpected parameters.
        self.new_age_threshold_min = float(new_age_threshold_min)

    # Public entrypoint
    def build(
        self,
        *,
        symbol: Optional[str],
        mint: Optional[str],
        snapshot: Mapping[str, Any],
        history: Sequence[Mapping[str, Any]] | None = None,
        regime: "RegimeState | None" = None,
        wallet_signals: Mapping[str, Any] | None = None,
        ai_signals: Mapping[str, Any] | None = None,
        scanner_source: str | None = None,
        risk_context: Mapping[str, Any] | None = None,
    ) -> Dict[str, float]:
        """
        High-level entry for feature vector creation.

        - Starts from core snapshot-only features.
        - Enriches with regime, wallet, AI, risk-context, scanner metadata.
        - Adds short-term microstructure + quant-grade composites for entry/sizing.
        """
        try:
            feats = build_features(snapshot)

            # Attach symbol / mint as numeric markers (real strings live in raw JSON/logs)
            if symbol is not None:
                # keep as numeric stub for ML frameworks that expect purely numeric arrays
                feats["symbol"] = 0.0
            if mint is not None:
                feats["mint"] = 0.0

            # Regime enrichment from RegimeState instance (overrides snapshot regime_* if present)
            if regime is not None:
                try:
                    if hasattr(regime, "as_numeric"):
                        rs = float(regime.as_numeric())
                    elif hasattr(regime, "get"):
                        name = regime.get()
                        rs = {
                            "risk_off": -1.0,
                            "normal": 0.0,
                            "risk_on": 1.0,
                            "aggressive": 0.7,
                        }.get(str(name), 0.0)
                    else:
                        rs = 0.0
                    rs = _clip(rs, -1.0, 1.0)
                    feats["regime_score"] = rs
                    feats["regime_numeric"] = rs
                    feats["regime_is_risk_on"] = 1.0 if rs >= 0.5 else 0.0
                    feats["regime_is_risk_off"] = 1.0 if rs <= -0.5 else 0.0
                    feats["regime_trend"] = rs
                except Exception:
                    log.debug("FeatureBuilder: regime enrichment failed", exc_info=True)

            # Enrich with wallet_signals (if provided separately from snapshot)
            if wallet_signals:
                for k in (
                    "smart_wallet_score",
                    "smart_flow_raw",
                    "smart_wallet_count_5m",
                    "smart_buy_score_5m",
                    "smart_sell_score_5m",
                    "smart_last_buy_sec_ago",
                    "smart_last_sell_sec_ago",
                    "smart_last_cluster_is_alpha",
                    "whales_5m",
                    "whale_vol_5m_usd",
                    "whale_buy_score",
                    "whale_sell_score",
                    "unique_buyers_5m",
                    "whale_share_5m",
                    "holders",
                    "holders_5m_change",
                    "holders_growth_5m",
                    "smart_net_side_5m",
                    "whale_net_side_5m",
                    "smart_alpha_bias",
                    "smart_whale_alignment",
                    "smart_whale_alignment_abs",
                    "whale_domination_5m",
                    "smart_vs_whale_conflict",
                    "smart_accumulation_score",
                    "smart_panic_dump_score",
                    "whale_liquidity_footprint",
                    # Pump extras if wallet tracker knows about them:
                    "pump_buyers_60s",
                    "pump_buys_60s",
                    "pump_sol_60s",
                    "pump_idle_sec",
                    "pump_stage_01",
                    "pump_velocity_score",
                ):
                    if k in wallet_signals:
                        feats[k] = _nan_zero(wallet_signals[k])

            # Enrich with ai_signals (if provided separately)
            if ai_signals:
                # fraud / rug score
                if "ai_rug_score_01" in ai_signals or "ai_rug_score" in ai_signals:
                    raw_rug = _f(
                        ai_signals,
                        "ai_rug_score_01",
                        ai_signals.get(
                            "ai_rug_score",
                            ai_signals.get("ai_fraud_score", 0.0),
                        ),
                    )
                    if raw_rug > 1.5:
                        rug01 = _clip01(raw_rug / 100.0)
                    else:
                        rug01 = _clip01(raw_rug)
                    feats["ai_rug_score_01"] = rug01
                    feats["ai_fraud_score"] = rug01

                # AI buy scoring hook (for prior / gating in ai_entry_logic/ai_buy_scoring)
                if "ai_buy_score" in ai_signals:
                    feats["ai_buy_score"] = _nan_zero(ai_signals["ai_buy_score"])
                if "ai_buy_ok_hard" in ai_signals:
                    feats["ai_buy_ok_hard"] = (
                        1.0 if bool(ai_signals["ai_buy_ok_hard"]) else 0.0
                    )
                if "ai_buy_pass_gate" in ai_signals:
                    feats["ai_buy_pass_gate"] = (
                        1.0 if bool(ai_signals["ai_buy_pass_gate"]) else 0.0
                    )

                # generic extra AI meta (entry/exit risk, confidence, rank, etc.)
                for k in (
                    "ai_entry_score",
                    "ai_entry_prob",
                    "ai_exit_risk",
                    "ai_rank_bucket",
                    "ai_confidence",
                    "ai_reinvest_score",
                    "ai_rebuy_score",
                    "ai_expected_rr",
                ):
                    if k in ai_signals:
                        feats[k] = _nan_zero(ai_signals[k])

                # Optional AI-provided “segment/head” for model routing:
                if "risk_profile" in ai_signals:
                    rp = str(ai_signals["risk_profile"]).lower()
                    feats["risk_profile_conservative"] = (
                        1.0 if rp == "conservative" else 0.0
                    )
                    feats["risk_profile_balanced"] = (
                        1.0 if rp == "balanced" else 0.0
                    )
                    feats["risk_profile_aggressive"] = (
                        1.0 if rp == "aggressive" else 0.0
                    )

            # Risk context enrichment (from risk_manager / core_utils.load_position_context)
            if risk_context:
                open_pos = _f(risk_context, "open_positions", 0.0)
                max_pos = _f(risk_context, "max_positions", 0.0)
                dd_pct = _f(risk_context, "daily_drawdown_pct", 0.0)
                pnl_pct = _f(risk_context, "daily_realized_pnl_pct", 0.0)
                mode = _f(risk_context, "risk_mode", 0.0)
                eq_usd = _f(risk_context, "account_equity_usd", 0.0)
                max_dd_pct = _f(risk_context, "max_daily_dd_pct", 30.0)
                exposure_usd = _f(risk_context, "total_exposure_usd", 0.0)
                win_rate = _f(risk_context, "rolling_win_rate_pct", 0.0)
                streak = _f(risk_context, "recent_loss_streak", 0.0)

                # Per-trade RR / SL / TP context for quant-grade sizing
                rr_target = _f(risk_context, "expected_rr_ratio", 0.0)
                stop_loss_pct = _f(risk_context, "default_stop_loss_pct", 0.0)
                take_profit_pct = _f(risk_context, "default_take_profit_pct", 0.0)

                # Optional daily trades context
                trades_count = _f(risk_context, "daily_trades_count", 0.0)
                trades_limit = _f(risk_context, "daily_trades_limit", 0.0)

                frac_open = _safe_ratio(open_pos, max(max_pos, 1.0), 0.0)
                exposure_frac = _safe_ratio(exposure_usd, max(eq_usd, 1.0), 0.0)
                trades_frac = _safe_ratio(trades_count, max(trades_limit, 1.0), 0.0)

                # risk_* naming for dashboards / logging
                feats["risk_open_positions"] = open_pos
                feats["risk_max_positions"] = max_pos
                feats["risk_open_fraction"] = _clip01(frac_open)
                feats["risk_daily_dd_pct"] = dd_pct
                feats["risk_daily_pnl_pct"] = pnl_pct
                feats["risk_equity_usd"] = eq_usd
                feats["risk_exposure_usd"] = exposure_usd
                feats["risk_exposure_frac"] = _clip01(exposure_frac)
                feats["risk_win_rate_pct"] = win_rate
                feats["risk_loss_streak"] = streak
                feats["risk_expected_rr_ratio"] = rr_target
                feats["risk_stop_loss_pct"] = stop_loss_pct
                feats["risk_take_profit_pct"] = take_profit_pct
                feats["risk_daily_trades_count"] = trades_count
                feats["risk_daily_trades_limit"] = trades_limit
                feats["risk_daily_trades_frac"] = _clip01(trades_frac)

                # Optional high-level risk profile tag (0/1 one-hots)
                rp = str(risk_context.get("risk_profile", "")).lower()
                feats["risk_profile_conservative"] = max(
                    feats.get("risk_profile_conservative", 0.0),
                    1.0 if rp == "conservative" else 0.0,
                )
                feats["risk_profile_balanced"] = max(
                    feats.get("risk_profile_balanced", 0.0),
                    1.0 if rp == "balanced" else 0.0,
                )
                feats["risk_profile_aggressive"] = max(
                    feats.get("risk_profile_aggressive", 0.0),
                    1.0 if rp == "aggressive" else 0.0,
                )

                # ctx_* naming for ai_entry_logic / ai_buy_scoring
                feats["ctx_open_positions"] = open_pos
                feats["ctx_max_positions"] = max_pos
                feats["ctx_daily_drawdown_pct"] = dd_pct
                feats["ctx_daily_realized_pnl_pct"] = pnl_pct
                feats["ctx_risk_mode"] = mode
                feats["ctx_account_equity_usd"] = eq_usd
                feats["ctx_exposure_usd"] = exposure_usd
                feats["ctx_exposure_frac"] = _clip01(exposure_frac)
                feats["ctx_expected_rr_ratio"] = rr_target
                feats["ctx_stop_loss_pct"] = stop_loss_pct
                feats["ctx_take_profit_pct"] = take_profit_pct
                feats["ctx_daily_trades_count"] = trades_count
                feats["ctx_daily_trades_limit"] = trades_limit
                feats["ctx_daily_trades_frac"] = _clip01(trades_frac)

                # Mode one-hots: 0=off,1=normal,2=aggressive,3=defensive (convention)
                feats["risk_mode_raw"] = mode
                feats["risk_mode_off"] = 1.0 if mode == 0 else 0.0
                feats["risk_mode_normal"] = 1.0 if mode == 1 else 0.0
                feats["risk_mode_aggressive"] = 1.0 if mode == 2 else 0.0
                feats["risk_mode_defensive"] = 1.0 if mode == 3 else 0.0

                # Capacity scalar: how much room we have to add risk today
                base_capacity = (1.0 - _clip01(frac_open)) * (
                    1.0 - _clip01(dd_pct / max_dd_pct)
                )
                mode_scalar = 1.15 if mode == 2 else 1.0  # aggressive a bit more
                if mode == 3:  # defensive
                    mode_scalar = 0.6

                feats["risk_capacity_base"] = _clip01(base_capacity)
                feats["risk_capacity_mode_scalar"] = _clip(mode_scalar, 0.3, 1.5)

                capacity = base_capacity * mode_scalar
                feats["risk_capacity_score"] = _clip(capacity, 0.0, 1.0)

                # Reward-to-risk attractiveness scalar
                rr_norm = _clip01(rr_target / 3.0)  # 3R+ is very good
                sl_norm = _clip01(stop_loss_pct / 20.0)  # big SL → riskier
                feats["risk_rr_attractiveness"] = _clip01(
                    rr_norm * (1.0 - 0.4 * sl_norm)
                )

                # Kelly-style fraction hint (for self_learning_ai_strategy / risk_manager)
                p = _clip01(win_rate / 100.0)
                b = rr_target if rr_target > 0.0 else 0.0
                if b > 0.0:
                    q = 1.0 - p
                    kelly = (b * p - q) / b
                    kelly = _clip01(kelly)
                else:
                    kelly = 0.0
                feats["risk_kelly_fraction_hint"] = kelly

                # Optional flags for daily controls (for policy / self-learning)
                feats["risk_hit_daily_dd_limit"] = _b(
                    risk_context, "hit_daily_dd_limit", False
                )
                feats["risk_hit_daily_trades_limit"] = _b(
                    risk_context, "hit_daily_trades_limit", False
                )

                # PnL bias hint: used by AI layers to lean risk-on/risk-off per day
                # Negative PnL → tilt toward defensive; positive → allow more aggression.
                pnl_norm = _clip(
                    pnl_pct / max(max_dd_pct, 1.0), -2.0, 2.0
                )  # -200%..+200% of daily DD
                feats["pnl_bias_hint"] = pnl_norm  # keep signed; downstream can map

                # Map daily PnL into a compact regime-style state for self-learning:
                #  -1 = recovery / drawdown mode
                #   0 = neutral
                #  +1 = green-day / heat mode
                if pnl_norm <= -0.5:
                    daily_state = -1.0
                elif pnl_norm >= 0.5:
                    daily_state = 1.0
                else:
                    daily_state = 0.0
                feats["risk_daily_state"] = daily_state
                feats["risk_daily_state_defensive"] = 1.0 if daily_state < 0.0 else 0.0
                feats["risk_daily_state_neutral"] = 1.0 if daily_state == 0.0 else 0.0
                feats["risk_daily_state_aggressive"] = 1.0 if daily_state > 0.0 else 0.0

            # Scanner metadata (numeric id + momentum flag)
            self._enrich_scanner_metadata(feats, snapshot, scanner_source)

            # Short-term microstructure from history (optional)
            if history:
                self._enrich_short_term_history(feats, history)

            # Quant-grade composites that rely on enriched features
            self._enrich_entry_quality_and_sizing(feats)

            return {k: _nan_zero(v) for k, v in feats.items()}

        except Exception:
            log.exception(
                "FeatureBuilder.build failed for symbol=%s mint=%s", symbol, mint
            )
            # Fail-safe: return minimal features from snapshot only
            return build_features(snapshot)

    # ------------------------------------------------------------------ #
    # Enrichment helpers
    # ------------------------------------------------------------------ #

    @staticmethod
    def _enrich_scanner_metadata(
        feats: Dict[str, Any],
        snapshot: Mapping[str, Any],
        scanner_source: str | None,
    ) -> None:
        src = (scanner_source or "").lower().strip()
        mapping = {
            "": 0.0,
            "raydium": 1.0,
            "dexscreener": 2.0,
            "pumpfun": 3.0,
            "momo": 4.0,
        }
        code = mapping.get(src, 0.0)
        feats["scanner_source_id"] = code

        # One-hots for routing / stats
        feats["scanner_is_raydium"] = 1.0 if src == "raydium" else 0.0
        feats["scanner_is_dexscreener"] = 1.0 if src == "dexscreener" else 0.0
        feats["scanner_is_pumpfun"] = 1.0 if src == "pumpfun" else 0.0
        feats["scanner_is_momo"] = 1.0 if src == "momo" else 0.0

        mom_ok = (
            snapshot.get("scanner_momentum_ok")
            or snapshot.get("momentum_ok")
            or snapshot.get("MOMENTUM_OK")
        )
        if isinstance(mom_ok, bool):
            feats["scanner_momentum_ok"] = 1.0 if mom_ok else 0.0
        elif isinstance(mom_ok, (int, float)):
            feats["scanner_momentum_ok"] = 1.0 if mom_ok else 0.0
        else:
            feats.setdefault("scanner_momentum_ok", 0.0)

        # Optional: scanner / segment hints (numeric only, for routing / stats)
        segment = str(snapshot.get("segment", "")).lower()
        feats["segment_pumpfun"] = 1.0 if segment == "pumpfun" else 0.0
        feats["segment_raydium"] = 1.0 if segment == "raydium" else 0.0
        feats["segment_momo"] = 1.0 if segment == "momo" else 0.0
        feats["segment_dexscreener"] = 1.0 if segment == "dexscreener" else 0.0

    @staticmethod
    def _enrich_short_term_history(
        feats: Dict[str, Any],
        history: Sequence[Mapping[str, Any]],
    ) -> None:
        """
        Optional microstructure features from recent history snapshots.

        We assume history is in chronological order (oldest -> newest).
        Only numeric operations here; no I/O.
        """
        if len(history) < 2:
            return

        prices: list[float] = []
        for snap in history:
            p = snap.get("price") or snap.get("p") or snap.get("price_usd")
            try:
                if p is not None:
                    prices.append(float(p))
            except Exception:
                continue

        if len(prices) < 2:
            return

        n = len(prices)

        def _pct(new: float, old: float) -> float:
            try:
                if old <= 0:
                    return 0.0
                return (new / old - 1.0) * 100.0
            except Exception:
                return 0.0

        def _log_ret(new: float, old: float) -> float:
            try:
                if new <= 0 or old <= 0:
                    return 0.0
                return math.log(new / old)
            except Exception:
                return 0.0

        def _window(idx_back: int):
            if n <= idx_back:
                return None
            return prices[-1], prices[-idx_back - 1]

        # Approximate offsets in terms of number of snapshots
        offsets = {
            "5s": 2,
            "15s": 5,
            "60s": 20,
            "300s": 100,
        }

        for label, off in offsets.items():
            win = _window(off)
            if not win:
                continue
            new, old = win
            lr = _log_ret(new, old)
            pct = _pct(new, old)
            feats[f"ret_{label}"] = lr
            feats[f"mom_{label}_pct"] = pct

        # Volatility: std of log-returns over short windows
        log_rets: list[float] = []
        for i in range(1, len(prices)):
            lr = _log_ret(prices[i], prices[i - 1])
            if lr != 0.0:
                log_rets.append(lr)

        if len(log_rets) >= 2:
            short = log_rets[-5:] if len(log_rets) >= 5 else log_rets
            med = log_rets[-20:] if len(log_rets) >= 20 else log_rets
            long = log_rets[-100:] if len(log_rets) >= 100 else log_rets

            def _std(xs: Sequence[float]) -> float:
                if len(xs) < 2:
                    return 0.0
                m = sum(xs) / len(xs)
                return math.sqrt(sum((x - m) ** 2 for x in xs) / len(xs))

            vol_15s = _std(short)
            vol_60s = _std(med)
            vol_300s = _std(long)

            feats["vol_15s"] = vol_15s
            feats["vol_60s"] = vol_60s
            feats["vol_300s"] = vol_300s

            # Volatility buckets (good for AI + heuristics)
            feats["vol_low"] = 1.0 if vol_60s < 0.01 else 0.0
            feats["vol_mid"] = 1.0 if 0.01 <= vol_60s < 0.05 else 0.0
            feats["vol_high"] = 1.0 if vol_60s >= 0.05 else 0.0

            # Micro trend consistency vs chop (Sharpe-like directionality)
            signs = [1.0 if r > 0 else -1.0 for r in log_rets]
            if signs:
                trend_consistency = abs(sum(signs)) / float(len(signs))
                feats["micro_trend_consistency"] = _clip01(trend_consistency)
                feats["micro_chop_score"] = _clip01(1.0 - trend_consistency)

            # Distance from intraday low/high for “late pump” awareness
            try:
                last = prices[-1]
                p_min = min(prices)
                p_max = max(prices)
                feats["price_from_intraday_min_pct"] = (
                    _pct(last, p_min) if p_min > 0 else 0.0
                )
                feats["price_from_intraday_max_pct"] = (
                    _pct(last, p_max) if p_max > 0 else 0.0
                )
            except Exception:
                pass

        # Short-term trend flags from latest price
        try:
            last = prices[-1]
            prev = prices[-2]
            last_ret_pct = _pct(last, prev)
            feats["last_ret_pct"] = last_ret_pct
            feats["short_trend_up"] = 1.0 if last_ret_pct > 0.0 else 0.0
            feats["short_trend_down"] = 1.0 if last_ret_pct < 0.0 else 0.0

            # “Heat” of last move: how aggressive is the last tick?
            feats["last_move_heat"] = _clip01(abs(last_ret_pct) / 20.0)
        except Exception:
            pass

    @staticmethod
    def _enrich_entry_quality_and_sizing(feats: Dict[str, Any]) -> None:
        """
        Build high-level composites for:
          - entry_quality_score (0..1.5)
          - position_size_scalar (0..1)
          - position_size_raw_scalar (0..1)
          - position_size_capacity_scalar (0..1)
          - position_size_kelly_scalar (0..1)  [Kelly-aware sizing hint]
          - entry_block_flag (0 or 1)
          - entry_soft_block_flag (0 or 1)
          - entry_risk_score (0..1)
          - entry_tier_bucket (0,1,2,3)
          - position_size_bucket (0,1,2,3)
          - entry_score_v1 / size_scalar_v1
          - God-tier extras:
              * entry_quality_risk_adjusted
              * entry_quality_kelly_adjusted
              * slippage_risk_score
              * entry_grade_bucket
              * entry_quality_vs_degen
              * gate_*_ok flags (soft pre-gates mirrored by ai_buy_scoring)
              * risk_daily_state-aware labels (recovery / neutral / heat)
        """
        structural_quality = _nan_zero(feats.get("structural_quality_score", 0.0))
        entry_micro = _nan_zero(feats.get("entry_micro_alpha", 0.0))
        pf_risk_adj = _nan_zero(feats.get("pf_risk_adjusted_final", 0.0))
        regime_score = _clip(_nan_zero(feats.get("regime_score", 0.0)), -1.0, 1.0)
        ai_rug = _clip01(_nan_zero(feats.get("ai_rug_score_01", 0.0)))
        is_dead_like = _nan_zero(feats.get("is_dead_like", 0.0))
        liq_ultra_low = _nan_zero(feats.get("liq_bucket_ultra_low", 0.0))
        liq_sweet = _nan_zero(feats.get("liq_bucket_sweet", 0.0))

        # Pump / curve risk extras
        pf_curve_danger = _clip01(
            _nan_zero(feats.get("pf_curve_danger_score", 0.0))
        )
        pump_stage_01 = _clip01(_nan_zero(feats.get("pump_stage_01", 0.0)))
        pump_idle_sec = _nan_zero(feats.get("pump_idle_sec", 0.0))
        pump_idle_long = 1.0 if pump_idle_sec >= 120.0 else 0.0
        pump_vel_norm = _clip01(
            _safe_ratio(_nan_zero(feats.get("pump_velocity_score", 0.0)), 10.0, 0.0)
        )

        # Optional smart/whale extras for finer sizing
        smart_alpha_boost = _clip01(_nan_zero(feats.get("smart_alpha_flow_boost", 0.0)))
        whale_share = _clip01(_nan_zero(feats.get("whale_share_5m", 0.0)))
        smart_whale_align = _clip01(
            _nan_zero(feats.get("smart_whale_alignment_consolidated", 0.0))
        )
        flow_accum = _clip01(_nan_zero(feats.get("flow_accumulation_score", 0.0)))
        flow_dump = _clip01(_nan_zero(feats.get("flow_panic_dump_score", 0.0)))
        whale_liq_fp = _clip01(_nan_zero(feats.get("whale_liquidity_footprint", 0.0)))

        # Optional risk capacity from context
        risk_capacity = _clip01(_nan_zero(feats.get("risk_capacity_score", 1.0)))
        rr_attract = _clip01(_nan_zero(feats.get("risk_rr_attractiveness", 0.5)))

        # Kelly hint (if risk context provided)
        kelly_hint = _clip01(_nan_zero(feats.get("risk_kelly_fraction_hint", 0.0)))

        # Entry quality (higher = better candidate)
        base_quality = (
            0.35 * _clip(structural_quality, 0.0, 1.5)
            + 0.25 * _clip01(pf_risk_adj)
            + 0.25 * _clip(entry_micro, 0.0, 1.5)
            + 0.10 * _clip01(smart_alpha_boost)
            + 0.05 * rr_attract
        )
        regime_boost = 1.0 + 0.20 * max(regime_score, 0.0)  # extra when risk-on
        rug_penalty = 1.0 - 0.7 * ai_rug  # strong penalty if ruggy

        # Flow-aware tweaks: boost on accumulation, penalize on dumps
        flow_boost = 1.0 + 0.15 * flow_accum - 0.15 * flow_dump

        # Pump stage: prefer early-stage Pump.fun; penalize late curve/late stage
        stage_penalty = 1.0 - 0.3 * pump_stage_01
        stage_penalty = _clip(stage_penalty, 0.4, 1.1)

        entry_quality = (
            base_quality * regime_boost * rug_penalty * flow_boost * stage_penalty
        )
        entry_quality = _clip(entry_quality, 0.0, 1.5)
        feats["entry_quality_score"] = entry_quality

        # -------------------------------------------------------------- #
        # Soft pre-gates — mirrored by ai_buy_scoring gate layout
        # (kept as features so AI / dashboards can see them directly).
        # -------------------------------------------------------------- #
        gate_structural = 1.0 if (structural_quality >= 0.35 and liq_ultra_low == 0.0) else 0.0
        gate_flow = 1.0 if flow_dump <= 0.7 else 0.0
        gate_rug = 1.0 if ai_rug <= 0.75 else 0.0
        gate_pump = 1.0 if (pf_curve_danger <= 0.8 and pump_stage_01 <= 0.97) else 0.0
        gate_context = 1.0 if risk_capacity >= 0.2 else 0.0

        feats["gate_structural_ok"] = gate_structural
        feats["gate_flow_ok"] = gate_flow
        feats["gate_rug_ok"] = gate_rug
        feats["gate_pump_ok"] = gate_pump
        feats["gate_context_ok"] = gate_context
        feats["gate_all_soft_ok"] = (
            1.0
            if (gate_structural and gate_flow and gate_rug and gate_pump and gate_context)
            else 0.0
        )

        # Hard block conditions: don't even consider entries here
        block_hard = False
        if ai_rug >= 0.85:
            block_hard = True
        if is_dead_like >= 1.0:
            block_hard = True
        if liq_ultra_low >= 1.0 and liq_sweet == 0.0:
            block_hard = True
        if flow_dump >= 0.9:  # strong panic dump
            block_hard = True
        # Very dangerous pump situations:
        if pf_curve_danger >= 0.9:
            block_hard = True
        # Very late stage + low current velocity + idle → likely dead pump
        if pump_stage_01 >= 0.98 and pump_vel_norm <= 0.2 and pump_idle_long >= 0.5:
            block_hard = True

        feats["entry_block_flag"] = 1.0 if block_hard else 0.0

        # Position sizing scalar (0..1)
        raw_size = (
            _clip(entry_quality, 0.0, 1.5) / 1.5
            * (1.0 - 0.6 * ai_rug)
            * (1.0 - 0.5 * is_dead_like)
        )

        # Smart/whale positive tilt only (never increase on bearish flow)
        tilt = (
            max(0.0, smart_whale_align - 0.5) * 0.3
            + smart_alpha_boost * 0.2
            + flow_accum * 0.2
        )
        tilt = _clip01(tilt)

        # Pump-specific tilt: only boost if stage is early and velocity is good
        if pump_stage_01 <= 0.7 and pump_vel_norm >= 0.3:
            tilt += 0.15 * pump_vel_norm
        tilt = _clip01(tilt)

        raw_size = raw_size * (1.0 + 0.3 * tilt)

        # Whale footprint: if whales are dominating, but not dumpy, allow slightly larger size
        if whale_liq_fp > 0.5 and flow_dump < 0.4:
            raw_size *= 1.0 + 0.1 * (whale_liq_fp - 0.5)

        # Late-stage pump penalty on size even if we didn't hard-block it
        late_penalty = 1.0 - 0.5 * pump_stage_01
        late_penalty = _clip(late_penalty, 0.3, 1.0)
        raw_size = raw_size * late_penalty

        raw_size = _clip01(raw_size)
        pos_size = raw_size * risk_capacity

        if block_hard:
            pos_size = 0.0

        feats["position_size_scalar"] = _clip01(pos_size)
        feats["position_size_raw_scalar"] = _clip01(raw_size)
        feats["position_size_capacity_scalar"] = _clip01(risk_capacity)

        # Kelly-aware suggested size scalar (for self-learning / dashboards)
        # Use Kelly as a soft modifier on the base scalar (never totally zero).
        kelly_floor = 0.2  # ensure non-zero guidance even with low Kelly
        kelly_mix = kelly_floor + (1.0 - kelly_floor) * kelly_hint
        pos_size_kelly = _clip01(pos_size * kelly_mix)
        if block_hard:
            pos_size_kelly = 0.0
        feats["position_size_kelly_scalar"] = pos_size_kelly

        # Optional: entry_risk_score (0..1) for dashboards / AI models
        entry_risk = (
            0.45 * ai_rug
            + 0.18 * _clip01(is_dead_like)
            + 0.10 * _clip01(liq_ultra_low)
            + 0.07 * _clip01(whale_share * 1.5)
            + 0.07 * flow_dump
            + 0.08 * pf_curve_danger
            + 0.05 * pump_stage_01
        )
        # Extra bump for very idle pumps
        entry_risk += 0.05 * pump_idle_long
        feats["entry_risk_score"] = _clip01(entry_risk)

        # Discrete tiers for downstream routing / analysis
        #   entry_tier_bucket: 0=ignore,1=small,2=medium,3=aggressive
        q = entry_quality  # 0..1.5
        if q < 0.35 or pos_size <= 0.0 or block_hard:
            tier = 0.0
        elif q < 0.7:
            tier = 1.0
        elif q < 1.1:
            tier = 2.0
        else:
            tier = 3.0
        feats["entry_tier_bucket"] = tier

        # Position size tier (0..3) based on scalar
        s = pos_size
        if s <= 0.0:
            s_tier = 0.0
        elif s < 0.33:
            s_tier = 1.0
        elif s < 0.66:
            s_tier = 2.0
        else:
            s_tier = 3.0
        feats["position_size_bucket"] = s_tier

        # Stable aliases for ML / AI layers (so we don't depend on internal names):
        feats["entry_score_v1"] = feats["entry_quality_score"]
        feats["size_scalar_v1"] = feats["position_size_scalar"]
        feats["entry_soft_block_flag"] = (
            1.0 if (feats["entry_risk_score"] >= 0.85) else 0.0
        )

        # ------------------------------------------------------------------ #
        # God-tier extra composites: risk-adjusted quality, slippage risk,
        # PnL-friendly grade, and quality-vs-degen slider.
        # ------------------------------------------------------------------ #
        eq = feats["entry_quality_score"]
        risk = feats["entry_risk_score"]

        # Risk-adjusted entry quality (still ~0..1.5)
        eq_risk_adj = eq * (1.0 - 0.6 * risk)
        feats["entry_quality_risk_adjusted"] = _clip(eq_risk_adj, 0.0, 1.5)

        # Kelly-adjusted view of risk-adjusted quality
        eq_kelly_adj = eq_risk_adj * (0.4 + 0.6 * kelly_hint)
        feats["entry_quality_kelly_adjusted"] = _clip(eq_kelly_adj, 0.0, 1.5)

        # Slippage risk from impact + liquidity
        impact_pct = _clip01(_nan_zero(feats.get("impact_1k_pct", 0.0)) / 5.0)  # 5%+ bad
        liq_norm = _clip01(_nan_zero(feats.get("liq_norm", 0.0)))
        slippage_risk = _clip01(0.7 * impact_pct + 0.3 * (1.0 - liq_norm))
        feats["slippage_risk_score"] = slippage_risk

        # Grade bucket 0..3: 0=skip, 1=ok/degen edge, 2=good, 3=elite
        if block_hard or eq < 0.3 or risk >= 0.9:
            grade = 0.0
        elif eq < 0.7 or risk >= 0.7:
            grade = 1.0
        elif eq < 1.1 or risk >= 0.5:
            grade = 2.0
        else:
            grade = 3.0
        feats["entry_grade_bucket"] = grade

        # Quality vs degen slider: -1 = pure degen, +1 = pristine quality
        # We want high quality, low rug, reasonable risk.
        quality_term = _clip01(eq / 1.5)
        safety_term = 1.0 - ai_rug
        qv = quality_term * safety_term * (1.0 - 0.5 * risk)
        # map [0,1] → [-1,1] so models can see direction
        feats["entry_quality_vs_degen"] = _clip(2.0 * qv - 1.0, -1.0, 1.0)

        # PnL-aware trade labels (for self-learning / analytics only).
        daily_state = _nan_zero(feats.get("risk_daily_state", 0.0))
        is_recovery = 1.0 if (daily_state < 0.0 and eq > 0.0 and pos_size > 0.0) else 0.0
        is_heat = 1.0 if (daily_state > 0.0 and pos_size > 0.0) else 0.0
        is_neutral = 1.0 if (daily_state == 0.0 and pos_size > 0.0) else 0.0

        feats["entry_label_recovery_trade"] = is_recovery
        feats["entry_label_heat_trade"] = is_heat
        feats["entry_label_neutral_trade"] = is_neutral


__all__ = [
    "build_features",
    "FeatureBuilder",
    "FEATURE_VERSION",
    "FEATURE_SCHEMA_NAME",
    "FEATURE_PHASE",
]
