# utils_pkg/feature_builder.py
from __future__ import annotations

"""
utils_pkg/feature_builder.py

Canonical feature builder (God-Tier Overhaul – V3+++ / quant-grade / Phase 0–7)
=============================================================================

Non-negotiables (production):
- Canonical: this is the *only* place that defines the feature schema.
- Deterministic + pure: NO network/Redis/DB calls. No env reads. No global mutable state.
- Numeric-only output: Dict[str, float] only. (No strings, no bools, no None.)
- No missing keys: output ALWAYS contains the full canonical key set.
- Stable naming: canonical keys are enforced; common aliases are emitted for backward-compat.
- Bounded outputs:
    - Derived/composite/normalized features are clamped to known ranges.
    - Raw magnitudes are hard-capped to prevent outliers/NaN/inf poisoning models.

Public API
----------
- build_features(snapshot: Mapping[str, Any]) -> Dict[str, float]
    Snapshot-only (scanner output → full canonical feature map).

- FeatureBuilder().build(...)-> Dict[str, float]
    Snapshot + optional enrichments (history/regime/wallet_signals/ai_signals/risk_context).

This file is intentionally verbose: the schema is the contract between scanners/sniper/AI/risk/exits.
"""

import math
from dataclasses import dataclass
from typing import Any, Dict, Iterable, Mapping, Optional, Sequence, Tuple

from logging_setup import get_logger

log = get_logger(__name__)

# Optional: Regime integration (we only read as_numeric/get)
try:  # pragma: no cover
    from utils_pkg.regime import RegimeState  # type: ignore
except Exception:  # pragma: no cover
    RegimeState = None  # type: ignore


# =============================================================================
# Schema tags (numeric-only, included in output)
# =============================================================================

FEATURE_VERSION: float = 7.10
FEATURE_SCHEMA_NAME: str = "god_tier_v3_quant_phase0_7_canonical"
FEATURE_PHASE: float = 7.0


def _stable_schema_id(name: str) -> float:
    # deterministic FNV-ish hash -> [0..999999]
    h = 2166136261
    for ch in name.encode("utf-8", errors="ignore"):
        h ^= ch
        h = (h * 16777619) & 0xFFFFFFFF
    return float(h % 1_000_000)


FEATURE_SCHEMA_ID: float = _stable_schema_id(FEATURE_SCHEMA_NAME)


# =============================================================================
# Numeric safety / clamps
# =============================================================================

_HARD_ABS_MAX = 1.0e12  # hard cap for raw magnitudes (USD, counts, etc.)
_TINY = 1.0e-12


def _is_finite(x: float) -> bool:
    return not (math.isnan(x) or math.isinf(x))


def _nan0(v: Any) -> float:
    try:
        f = float(v)
    except Exception:
        return 0.0
    if not _is_finite(f):
        return 0.0
    if f > _HARD_ABS_MAX:
        return _HARD_ABS_MAX
    if f < -_HARD_ABS_MAX:
        return -_HARD_ABS_MAX
    return float(f)


def _clip(x: Any, lo: float, hi: float) -> float:
    f = _nan0(x)
    if f < lo:
        return float(lo)
    if f > hi:
        return float(hi)
    return float(f)


def _clip01(x: Any) -> float:
    return _clip(x, 0.0, 1.0)


def _bool01(v: Any, default: bool = False) -> float:
    if v is None:
        return 1.0 if default else 0.0
    if isinstance(v, bool):
        return 1.0 if v else 0.0
    s = str(v).strip().lower()
    if s in ("1", "true", "t", "yes", "y", "on"):
        return 1.0
    if s in ("0", "false", "f", "no", "n", "off"):
        return 0.0
    return 1.0 if bool(v) else 0.0


def _log1p_pos(x: Any) -> float:
    f = _nan0(x)
    if f <= 0.0:
        return 0.0
    return float(math.log1p(f))


def _safe_ratio(num: Any, den: Any, default: float = 0.0) -> float:
    n = _nan0(num)
    d = _nan0(den)
    if abs(d) <= _TINY:
        return float(default)
    return float(n / d)


def _net_minus_one_to_one(pos: Any, neg: Any) -> float:
    p = max(0.0, _nan0(pos))
    n = max(0.0, _nan0(neg))
    d = p + n
    if d <= _TINY:
        return 0.0
    return _clip((p - n) / d, -1.0, 1.0)


def _norm_log_scale(x: Any, ref: float) -> float:
    # log1p(x)/log1p(ref) -> [0,1]
    r = max(float(ref), 1.0)
    return _clip01(_safe_ratio(_log1p_pos(x), _log1p_pos(r), 0.0))


# =============================================================================
# Snapshot extraction helpers (aliases + dotted paths)
# =============================================================================

def _get_path(src: Mapping[str, Any], key: str) -> Any:
    if "." not in key:
        return src.get(key)
    cur: Any = src
    for part in key.split("."):
        if not isinstance(cur, Mapping):
            return None
        cur = cur.get(part)
    return cur


def _first_present(src: Mapping[str, Any], keys: Sequence[str], default: Any = None) -> Any:
    for k in keys:
        try:
            v = _get_path(src, k)
            if v is not None:
                return v
        except Exception:
            continue
    return default


def _f_any(src: Mapping[str, Any], keys: Sequence[str], default: float = 0.0) -> float:
    v = _first_present(src, keys, None)
    if v is None:
        return float(default)
    if isinstance(v, bool):
        return float(default)
    return _nan0(v)


def _i_any(src: Mapping[str, Any], keys: Sequence[str], default: int = 0) -> int:
    v = _first_present(src, keys, None)
    if v is None:
        return int(default)
    try:
        return int(float(v))
    except Exception:
        return int(default)


# =============================================================================
# Canonical schema (NO missing keys)
# =============================================================================
# Keep this list stable. If you add fields, add them here (append near related section).

CANONICAL_KEYS: Tuple[str, ...] = (
    # schema tags
    "feature_version",
    "feature_phase",
    "feature_schema_id",
    # core size/age
    "liq_usd",
    "mcap_usd",
    "fdv_usd",
    "age_min",
    "token_age_min",
    "token_age_hr",
    "token_age_sec",
    "log_liq_usd",
    "log_mcap_usd",
    "log_fdv_usd",
    "liq_norm",
    "mcap_norm",
    "fdv_norm",
    "mcap_to_liq",
    "fdv_to_liq",
    "fdv_to_mcap",
    "is_super_early",
    "is_very_new",
    "is_newish",
    "is_mid_age",
    "is_old",
    "liq_bucket_ultra_low",
    "liq_bucket_low",
    "liq_bucket_sweet",
    "liq_bucket_mid",
    "liq_bucket_high",
    "mcap_bucket_early",
    "mcap_bucket_very_late",
    # volume
    "vol_1m_usd",
    "vol_5m_usd",
    "vol_15m_usd",
    "vol_1h_usd",
    "vol_24h_usd",
    "log_vol_5m_usd",
    "log_vol_24h_usd",
    "vol_ratio_1m_5m",
    "vol_ratio_5m_1h",
    "vol_ratio_5m_24h",
    "vol_5m_per_mcap",
    "vol_5m_per_liq",
    "volume_heat_intraday",
    # trades/orderflow
    "buys_1m",
    "sells_1m",
    "buys_5m",
    "sells_5m",
    "txns_5m",
    "buy_ratio_1m",
    "buy_ratio_5m",
    "net_buys_5m",
    "orderflow_net_5m_norm",
    "orderflow_skew_5m",
    "vol_per_tx_5m_usd",
    "liq_per_trade_5m_usd",
    "trade_intensity_5m",
    "buy_intensity_5m",
    "sell_intensity_5m",
    "buy_pressure_score",          # (-1..2) clipped downstream where used
    # price/momentum
    "price_usd",
    "price_change_1m_pct",
    "price_change_5m_pct",
    "price_change_15m_pct",
    "price_change_1h_pct",
    "momentum_short_pct",
    "momentum_mid_pct",
    "has_fast_dump_5m",
    "has_fast_pump_5m",
    "is_pump_chasing_risk",
    "momentum_regime_bear",
    "momentum_regime_flat",
    "momentum_regime_bull",
    # holders/concentration
    "holders",
    "holders_5m_change",
    "holders_growth_5m",
    "top1_holder_frac",
    "top10_holder_frac",
    # whales/smart-wallet
    "whales_5m",
    "whale_vol_5m_usd",
    "largest_buy_5m_sol",
    "whale_buy_score",
    "whale_sell_score",
    "whale_share_5m",
    "whales_per_holder",
    "unique_buyers_5m",
    "unique_buyers_5m_norm",
    "whale_to_unique_ratio",
    "smart_wallet_score",
    "smart_flow_raw",
    "smart_flow_bias_01",
    "smart_wallet_count_5m",
    "smart_buy_score_5m",
    "smart_sell_score_5m",
    "smart_last_buy_sec_ago",
    "smart_last_sell_sec_ago",
    "smart_recent_buy_flag",
    "smart_recent_sell_flag",
    "smart_last_cluster_is_alpha",
    "smart_net_side_5m",
    "whale_net_side_5m",
    "smart_alpha_bias",
    "smart_whale_alignment_v3",
    "smart_whale_alignment_abs",
    "whale_domination_5m",
    "smart_vs_whale_conflict",
    "smart_accumulation_score",
    "smart_panic_dump_score",
    "whale_liquidity_footprint",
    "smart_whale_alignment",
    "smart_whale_alignment_consolidated",
    "smart_alpha_flow_boost",
    "flow_accumulation_score",
    "flow_panic_dump_score",
    "flow_conflict_score",
    "flow_is_strong_accum",
    "flow_is_panic_dump",
    "flow_is_conflicting",
    # rug/authority/impact
    "ai_rug_score_01",
    "ai_fraud_score",
    "impact_1k_bps",
    "impact_1k_pct",
    "authority_risk",
    # PF scores & pumpfun
    "pf_momentum_score",
    "pf_final_score",
    "pf_risk_adjusted_final",
    "pf_momo_risk_adj",
    "pump_stage_01",
    "pump_stage",
    "pump_buyers_60s",
    "pump_buys_60s",
    "pump_sol_60s",
    "pump_idle_sec",
    "pump_is_idle_heavy",
    "pump_velocity_score",
    "pf_curve_progress",
    "pf_minted_pct",
    "pf_creator_hold_pct",
    "pf_bot_score",
    "pf_sniper_density",
    "pf_curve_danger_score",
    # regime
    "regime_score",
    "regime_numeric",
    "regime_is_risk_on",
    "regime_is_risk_off",
    "regime_trend",
    "regime_liquidity",
    "regime_manipulation_risk",
    "regime_volatility",
    # structure + alpha
    "structural_quality_score",
    "is_dead_like",
    "entry_micro_alpha",
    "alpha_micro_score",
    "liquidity_regime_score",
    "micro_risk_on_score",
    "micro_risk_on_flag",
    # risk context (optional overlay)
    "ai_capacity_delta",
    "ai_capacity_scalar",
    "risk_regime_entry_gate_factor",
    "risk_block_new_entries",
    "risk_recommend_max_pos_mult",
    "risk_mode_label_risk_off",
    "risk_mode_label_normal",
    "risk_mode_label_risk_on",
    "risk_open_positions",
    "risk_max_positions",
    "risk_open_fraction",
    "risk_daily_dd_pct",
    "risk_daily_pnl_pct",
    "risk_equity_usd",
    "risk_exposure_usd",
    "risk_exposure_frac",
    "risk_win_rate_pct",
    "risk_loss_streak",
    "risk_expected_rr_ratio",
    "risk_stop_loss_pct",
    "risk_take_profit_pct",
    "risk_daily_trades_count",
    "risk_daily_trades_limit",
    "risk_daily_trades_frac",
    "ctx_open_positions",
    "ctx_max_positions",
    "ctx_daily_drawdown_pct",
    "ctx_daily_realized_pnl_pct",
    "ctx_risk_mode",
    "ctx_account_equity_usd",
    "ctx_exposure_usd",
    "ctx_exposure_frac",
    "ctx_expected_rr_ratio",
    "ctx_stop_loss_pct",
    "ctx_take_profit_pct",
    "ctx_daily_trades_count",
    "ctx_daily_trades_limit",
    "ctx_daily_trades_frac",
    "risk_mode_raw",
    "risk_mode_off",
    "risk_mode_normal",
    "risk_mode_aggressive",
    "risk_mode_defensive",
    "risk_capacity_base",
    "risk_capacity_mode_scalar",
    "risk_capacity_score",
    "risk_rr_attractiveness",
    "risk_kelly_fraction_hint",
    "risk_hit_daily_dd_limit",
    "risk_hit_daily_trades_limit",
    "pnl_bias_hint",
    "risk_daily_state",
    "risk_daily_state_defensive",
    "risk_daily_state_neutral",
    "risk_daily_state_aggressive",
    "risk_daily_heat_score",
    "risk_daily_cooldown_score",
    # scanner metadata
    "scanner_source_id",
    "scanner_is_raydium",
    "scanner_is_dexscreener",
    "scanner_is_pumpfun",
    "scanner_is_momo",
    "scanner_momentum_ok",
    "segment_pumpfun",
    "segment_raydium",
    "segment_momo",
    "segment_dexscreener",
    # history microstructure (optional, still always present keys)
    "ret_5s",
    "ret_15s",
    "ret_60s",
    "ret_300s",
    "mom_5s_pct",
    "mom_15s_pct",
    "mom_60s_pct",
    "mom_300s_pct",
    "vol_15s",
    "vol_60s",
    "vol_300s",
    "vol_low",
    "vol_mid",
    "vol_high",
    "micro_trend_consistency",
    "micro_chop_score",
    "price_from_intraday_min_pct",
    "price_from_intraday_max_pct",
    "last_ret_pct",
    "short_trend_up",
    "short_trend_down",
    "last_move_heat",
    # composites / gating / sizing (core contract with entry logic)
    "entry_quality_score",
    "entry_quality_risk_adjusted",
    "entry_quality_kelly_adjusted",
    "entry_risk_score",
    "entry_grade_bucket",
    "entry_tier_bucket",
    "position_size_scalar",
    "position_size_raw_scalar",
    "position_size_capacity_scalar",
    "position_size_kelly_scalar",
    "position_size_bucket",
    "entry_block_flag",
    "entry_soft_block_flag",
    "gate_structural_ok",
    "gate_flow_ok",
    "gate_rug_ok",
    "gate_pump_ok",
    "gate_context_ok",
    "gate_regime_ok",
    "gate_daily_limits_ok",
    "gate_all_soft_ok",
    "entry_score_v1",
    "size_scalar_v1",
    "entry_score",
    "size_scalar",
    "position_scalar",
    "entry_block_hard",
    "slippage_risk_score",
    "entry_quality_vs_degen",
    "entry_label_recovery_trade",
    "entry_label_heat_trade",
    "entry_label_neutral_trade",
    # backward-compat aliases (same numeric value as canon)
    "vol5",
    "buys5",
    "sells5",
    "chg5",
    "liq",
    "mcap",
)

# =============================================================================
# Snapshot-only core builder (pure)
# =============================================================================

def build_features(snapshot: Mapping[str, Any]) -> Dict[str, float]:
    raw = dict(snapshot or {})
    feats: Dict[str, Any] = {}

    # schema tags
    feats["feature_version"] = FEATURE_VERSION
    feats["feature_phase"] = FEATURE_PHASE
    feats["feature_schema_id"] = FEATURE_SCHEMA_ID

    # ---- core: liq/mcap/fdv/age ----
    liq_usd = _f_any(raw, ("liq_usd", "liq", "liquidity_usd", "liquidity", "pool_liquidity_usd"), 0.0)
    mcap_usd = _f_any(raw, ("mcap_usd", "mcap", "market_cap_usd", "marketCapUsd"), 0.0)
    fdv_usd = _f_any(raw, ("fdv_usd", "fdv", "fully_diluted_valuation_usd", "fdvUsd"), 0.0)
    age_min = max(0.0, _f_any(raw, ("token_age_min", "age_min", "ageMinutes", "pool_age_min", "age"), 0.0))

    feats["liq_usd"] = liq_usd
    feats["mcap_usd"] = mcap_usd
    feats["fdv_usd"] = fdv_usd
    feats["age_min"] = age_min
    feats["token_age_min"] = age_min
    feats["token_age_hr"] = age_min / 60.0 if age_min > 0 else 0.0
    feats["token_age_sec"] = age_min * 60.0 if age_min > 0 else 0.0

    feats["log_liq_usd"] = _log1p_pos(liq_usd)
    feats["log_mcap_usd"] = _log1p_pos(mcap_usd)
    feats["log_fdv_usd"] = _log1p_pos(fdv_usd)

    feats["liq_norm"] = _norm_log_scale(liq_usd, 1_000_000.0)
    feats["mcap_norm"] = _norm_log_scale(mcap_usd, 10_000_000.0)
    feats["fdv_norm"] = _norm_log_scale(fdv_usd, 20_000_000.0)

    feats["mcap_to_liq"] = _safe_ratio(mcap_usd, max(liq_usd, 1.0), 0.0)
    feats["fdv_to_liq"] = _safe_ratio(fdv_usd, max(liq_usd, 1.0), 0.0)
    feats["fdv_to_mcap"] = _safe_ratio(fdv_usd, max(mcap_usd, 1.0), 0.0)

    feats["is_super_early"] = 1.0 if age_min <= 10.0 else 0.0
    feats["is_very_new"] = 1.0 if age_min <= 30.0 else 0.0
    feats["is_newish"] = 1.0 if (30.0 < age_min <= 180.0) else 0.0
    feats["is_mid_age"] = 1.0 if (180.0 < age_min <= 720.0) else 0.0
    feats["is_old"] = 1.0 if age_min > 240.0 else 0.0

    feats["liq_bucket_ultra_low"] = 1.0 if liq_usd < 8_000.0 else 0.0
    feats["liq_bucket_low"] = 1.0 if 8_000.0 <= liq_usd < 20_000.0 else 0.0
    feats["liq_bucket_sweet"] = 1.0 if 20_000.0 <= liq_usd < 120_000.0 else 0.0
    feats["liq_bucket_mid"] = 1.0 if 120_000.0 <= liq_usd < 500_000.0 else 0.0
    feats["liq_bucket_high"] = 1.0 if liq_usd >= 500_000.0 else 0.0

    feats["mcap_bucket_early"] = 1.0 if 150_000.0 <= mcap_usd <= 6_000_000.0 else 0.0
    feats["mcap_bucket_very_late"] = 1.0 if mcap_usd > 25_000_000.0 else 0.0

    # ---- volumes ----
    vol_1m = _f_any(raw, ("vol_1m_usd", "volume_1m_usd", "vol1m", "vol_1m"), 0.0)
    vol_5m = _f_any(raw, ("vol_5m_usd", "volume_5m_usd", "vol5", "vol_5m"), 0.0)
    vol_15m = _f_any(raw, ("vol_15m_usd", "volume_15m_usd", "vol15", "vol_15m"), 0.0)
    vol_1h = _f_any(raw, ("vol_1h_usd", "volume_1h_usd", "vol1h", "vol_1h"), 0.0)
    vol_24h = _f_any(raw, ("vol_24h_usd", "volume_24h_usd", "vol24h", "vol_24h"), 0.0)

    feats["vol_1m_usd"] = vol_1m
    feats["vol_5m_usd"] = vol_5m
    feats["vol_15m_usd"] = vol_15m
    feats["vol_1h_usd"] = vol_1h
    feats["vol_24h_usd"] = vol_24h

    feats["log_vol_5m_usd"] = _log1p_pos(vol_5m)
    feats["log_vol_24h_usd"] = _log1p_pos(vol_24h)

    feats["vol_ratio_1m_5m"] = _safe_ratio(vol_1m, max(vol_5m, 1.0), 0.0)
    feats["vol_ratio_5m_1h"] = _safe_ratio(vol_5m, max(vol_1h, 1.0), 0.0)
    feats["vol_ratio_5m_24h"] = _safe_ratio(vol_5m, max(vol_24h, 1.0), 0.0)
    feats["vol_5m_per_mcap"] = _safe_ratio(vol_5m, max(mcap_usd, 1.0), 0.0)
    feats["vol_5m_per_liq"] = _safe_ratio(vol_5m, max(liq_usd, 1.0), 0.0)
    feats["volume_heat_intraday"] = _clip01(_norm_log_scale(vol_5m, max(vol_24h, 1.0)) * 1.5)

    # ---- trades / orderflow ----
    buys_1m = _i_any(raw, ("buys_1m", "buys1", "txns.buys_1m"), 0)
    sells_1m = _i_any(raw, ("sells_1m", "sells1", "txns.sells_1m"), 0)
    buys_5m = _i_any(raw, ("buys_5m", "buys5", "txns.buys_5m"), 0)
    sells_5m = _i_any(raw, ("sells_5m", "sells5", "txns.sells_5m"), 0)
    txns_5m = _i_any(raw, ("txns_5m", "txns5", "txns.total_5m"), buys_5m + sells_5m)

    feats["buys_1m"] = float(buys_1m)
    feats["sells_1m"] = float(sells_1m)
    feats["buys_5m"] = float(buys_5m)
    feats["sells_5m"] = float(sells_5m)
    feats["txns_5m"] = float(txns_5m)

    buy_ratio_1m = _safe_ratio(buys_1m + 1.0, buys_1m + sells_1m + 2.0, 0.5)
    buy_ratio_5m = _safe_ratio(buys_5m + 1.0, txns_5m + 2.0, 0.5)
    feats["buy_ratio_1m"] = _clip01(buy_ratio_1m)
    feats["buy_ratio_5m"] = _clip01(buy_ratio_5m)

    feats["net_buys_5m"] = float(buys_5m - sells_5m)
    feats["orderflow_net_5m_norm"] = _net_minus_one_to_one(buys_5m, sells_5m)

    skew = _safe_ratio(buys_5m + 1.0, sells_5m + 1.0, 1.0) - 1.0
    feats["orderflow_skew_5m"] = _clip(skew, -3.0, 3.0)

    feats["vol_per_tx_5m_usd"] = _safe_ratio(vol_5m, max(txns_5m, 1), 0.0)
    feats["liq_per_trade_5m_usd"] = _safe_ratio(liq_usd, max(txns_5m, 1), 0.0)

    feats["trade_intensity_5m"] = _safe_ratio(txns_5m, 5.0, 0.0)  # tx/min
    feats["buy_intensity_5m"] = _safe_ratio(buys_5m, 5.0, 0.0)
    feats["sell_intensity_5m"] = _safe_ratio(sells_5m, 5.0, 0.0)

    turnover_term = _clip(_safe_ratio(vol_5m, max(mcap_usd, 1.0), 0.0), 0.0, 0.5)
    feats["buy_pressure_score"] = _clip((_clip(skew, -2.0, 2.0) / 2.0) + turnover_term, -1.0, 2.0)

    # ---- price / momentum ----
    price_usd = _f_any(raw, ("price_usd", "px_usd", "price", "p", "usdPrice"), 0.0)
    chg_1m = _f_any(raw, ("price_change_1m_pct", "chg1", "chg_1m", "change_1m_pct"), 0.0)
    chg_5m = _f_any(raw, ("price_change_5m_pct", "chg5", "chg_5m", "change_5m_pct"), 0.0)
    chg_15m = _f_any(raw, ("price_change_15m_pct", "chg15", "chg_15m", "change_15m_pct"), 0.0)
    chg_1h = _f_any(raw, ("price_change_1h_pct", "chg1h", "chg_1h", "change_1h_pct"), 0.0)

    chg_1m_c = _clip(chg_1m, -60.0, 80.0)
    chg_5m_c = _clip(chg_5m, -60.0, 120.0)
    chg_15m_c = _clip(chg_15m, -80.0, 180.0)
    chg_1h_c = _clip(chg_1h, -90.0, 250.0)

    feats["price_usd"] = price_usd
    feats["price_change_1m_pct"] = chg_1m_c
    feats["price_change_5m_pct"] = chg_5m_c
    feats["price_change_15m_pct"] = chg_15m_c
    feats["price_change_1h_pct"] = chg_1h_c

    short_mom = max(0.0, chg_1m_c) * 0.4 + max(0.0, chg_5m_c) * 0.6
    mid_mom = max(0.0, chg_15m_c) * 0.6 + max(0.0, chg_1h_c) * 0.4
    feats["momentum_short_pct"] = float(short_mom)
    feats["momentum_mid_pct"] = float(mid_mom)

    feats["has_fast_dump_5m"] = 1.0 if chg_5m_c <= -20.0 else 0.0
    feats["has_fast_pump_5m"] = 1.0 if chg_5m_c >= 30.0 else 0.0
    feats["is_pump_chasing_risk"] = 1.0 if (chg_1m_c > 10.0 and chg_5m_c > 25.0) else 0.0

    feats["momentum_regime_bear"] = 1.0 if chg_5m_c <= -10.0 else 0.0
    feats["momentum_regime_flat"] = 1.0 if (-5.0 < chg_5m_c < 5.0) else 0.0
    feats["momentum_regime_bull"] = 1.0 if chg_5m_c >= 10.0 else 0.0

    # ---- holders / concentration ----
    holders = _i_any(raw, ("holders", "holders_total", "holdersCount"), 0)
    holders_5m_change = _i_any(raw, ("holders_5m_change", "holders_delta_5m", "holdersChange5m"), 0)
    feats["holders"] = float(holders)
    feats["holders_5m_change"] = float(holders_5m_change)
    feats["holders_growth_5m"] = _clip(_safe_ratio(holders_5m_change, max(holders, 1), 0.0), -0.5, 1.0)

    top1 = _f_any(raw, ("top_holder_pct", "top1_holder_pct", "top1_pct"), 0.0)
    top10 = _f_any(raw, ("top10_holder_pct", "top10_pct", "holder_concentration"), 0.0)
    if 1.0 < top1 <= 100.0:
        top1 /= 100.0
    if 1.0 < top10 <= 100.0:
        top10 /= 100.0
    feats["top1_holder_frac"] = _clip01(top1)
    feats["top10_holder_frac"] = _clip01(top10)

    # ---- whale/smart ----
    whales_5m = _i_any(raw, ("whales_5m", "whale_buys_5m", "whaleTrades5m"), 0)
    whale_vol_5m = _f_any(raw, ("whale_vol_5m_usd", "whale_volume_5m_usd"), 0.0)
    largest_buy_5m_sol = _f_any(raw, ("largest_buy_5m_sol", "largest_buy_5m"), 0.0)
    whale_buy_score = _clip01(_f_any(raw, ("whale_buy_score", "whale_buy_score_5m"), 0.0))
    whale_sell_score = _clip01(_f_any(raw, ("whale_sell_score", "whale_sell_score_5m"), 0.0))

    feats["whales_5m"] = float(whales_5m)
    feats["whale_vol_5m_usd"] = whale_vol_5m
    feats["largest_buy_5m_sol"] = largest_buy_5m_sol
    feats["whale_buy_score"] = whale_buy_score
    feats["whale_sell_score"] = whale_sell_score

    whale_share_5m = _f_any(raw, ("whale_share_5m",), _safe_ratio(whale_vol_5m, max(vol_5m, 1.0), 0.0))
    feats["whale_share_5m"] = _clip01(whale_share_5m)
    feats["whales_per_holder"] = _safe_ratio(float(whales_5m), max(float(holders), 1.0), 0.0)

    unique_buyers_5m = _f_any(raw, ("unique_buyers_5m",), 0.0)
    feats["unique_buyers_5m"] = float(unique_buyers_5m)
    feats["unique_buyers_5m_norm"] = _norm_log_scale(unique_buyers_5m, 50.0)
    feats["whale_to_unique_ratio"] = _safe_ratio(whales_5m, max(unique_buyers_5m, 1.0), 0.0)

    smart_wallet_score = _clip01(_f_any(raw, ("smart_wallet_score",), 0.0))
    smart_flow_raw = _clip(_f_any(raw, ("smart_flow_raw",), 0.0), -1.5, 1.5)
    smart_wallet_count_5m = _f_any(raw, ("smart_wallet_count_5m",), 0.0)
    smart_buy_score_5m = _clip01(_f_any(raw, ("smart_buy_score_5m",), 0.0))
    smart_sell_score_5m = _clip01(_f_any(raw, ("smart_sell_score_5m",), 0.0))
    smart_last_buy_sec_ago = _f_any(raw, ("smart_last_buy_sec_ago",), 1e9)
    smart_last_sell_sec_ago = _f_any(raw, ("smart_last_sell_sec_ago",), 1e9)
    smart_last_cluster_is_alpha = _clip01(_f_any(raw, ("smart_last_cluster_is_alpha",), 0.0))

    smart_net_side_5m = _f_any(raw, ("smart_net_side_5m",), 0.0)
    whale_net_side_5m = _f_any(raw, ("whale_net_side_5m",), 0.0)

    # derive net sides if absent but buy/sell present
    if abs(smart_net_side_5m) < 1e-9 and (smart_buy_score_5m > 0 or smart_sell_score_5m > 0):
        smart_net_side_5m = _net_minus_one_to_one(smart_buy_score_5m, smart_sell_score_5m)
    if abs(whale_net_side_5m) < 1e-9 and (whale_buy_score > 0 or whale_sell_score > 0):
        whale_net_side_5m = _net_minus_one_to_one(whale_buy_score, whale_sell_score)

    smart_alpha_bias = _f_any(raw, ("smart_alpha_bias",), 0.0)
    if abs(smart_alpha_bias) < 1e-9 and smart_last_cluster_is_alpha > 0.0 and smart_wallet_count_5m > 0.0:
        smart_alpha_bias = _clip01(smart_wallet_count_5m / 5.0)

    feats["smart_wallet_score"] = smart_wallet_score
    feats["smart_flow_raw"] = smart_flow_raw
    feats["smart_flow_bias_01"] = _clip01(0.5 * (_clip(smart_flow_raw, -1.0, 1.0) + 1.0))
    feats["smart_wallet_count_5m"] = float(smart_wallet_count_5m)
    feats["smart_buy_score_5m"] = smart_buy_score_5m
    feats["smart_sell_score_5m"] = smart_sell_score_5m
    feats["smart_last_buy_sec_ago"] = float(smart_last_buy_sec_ago)
    feats["smart_last_sell_sec_ago"] = float(smart_last_sell_sec_ago)
    feats["smart_recent_buy_flag"] = 1.0 if smart_last_buy_sec_ago < 300.0 else 0.0
    feats["smart_recent_sell_flag"] = 1.0 if smart_last_sell_sec_ago < 300.0 else 0.0
    feats["smart_last_cluster_is_alpha"] = smart_last_cluster_is_alpha

    feats["smart_net_side_5m"] = _clip(smart_net_side_5m, -1.0, 1.0)
    feats["whale_net_side_5m"] = _clip(whale_net_side_5m, -1.0, 1.0)
    feats["smart_alpha_bias"] = _clip01(smart_alpha_bias)

    feats["smart_whale_alignment_v3"] = _clip(_f_any(raw, ("smart_whale_alignment",), 0.0), 0.0, 2.0)
    feats["smart_whale_alignment_abs"] = _clip01(_f_any(raw, ("smart_whale_alignment_abs",), 0.0))
    feats["whale_domination_5m"] = _clip01(_f_any(raw, ("whale_domination_5m",), 0.0))
    feats["smart_vs_whale_conflict"] = _clip01(_f_any(raw, ("smart_vs_whale_conflict",), 0.0))
    feats["smart_accumulation_score"] = _clip01(_f_any(raw, ("smart_accumulation_score",), 0.0))
    feats["smart_panic_dump_score"] = _clip01(_f_any(raw, ("smart_panic_dump_score",), 0.0))
    feats["whale_liquidity_footprint"] = _clip01(_f_any(raw, ("whale_liquidity_footprint",), 0.0))

    smart_net_bias_01 = 0.5 * (feats["smart_net_side_5m"] + 1.0)
    whale_net_bias_01 = 0.5 * (feats["whale_net_side_5m"] + 1.0)

    align_legacy = (
        0.20 * feats["smart_wallet_score"]
        + 0.15 * feats["smart_buy_score_5m"]
        - 0.10 * feats["smart_sell_score_5m"]
        + 0.20 * feats["whale_buy_score"]
        - 0.10 * feats["whale_sell_score"]
        + 0.15 * feats["buy_ratio_5m"]
        + 0.10 * smart_net_bias_01
        + 0.10 * whale_net_bias_01
    )
    feats["smart_whale_alignment"] = _clip(align_legacy, 0.0, 1.5)

    consolidated = feats["smart_whale_alignment_v3"] if feats["smart_whale_alignment_v3"] > 0.0 else _clip01(align_legacy)
    feats["smart_whale_alignment_consolidated"] = _clip01(consolidated)

    feats["smart_alpha_flow_boost"] = _clip01(feats["smart_alpha_bias"] * feats["smart_wallet_score"])

    smart_net = feats["smart_net_side_5m"]
    whale_net = feats["whale_net_side_5m"]
    feats["flow_accumulation_score"] = _clip01(
        feats["smart_accumulation_score"] if feats["smart_accumulation_score"] > 0.0 else max(0.0, smart_net) * max(0.0, whale_net)
    )
    feats["flow_panic_dump_score"] = _clip01(
        feats["smart_panic_dump_score"] if feats["smart_panic_dump_score"] > 0.0 else max(0.0, -smart_net) * max(0.0, -whale_net)
    )
    feats["flow_conflict_score"] = _clip01(feats["smart_vs_whale_conflict"])
    feats["flow_is_strong_accum"] = 1.0 if feats["flow_accumulation_score"] >= 0.6 else 0.0
    feats["flow_is_panic_dump"] = 1.0 if feats["flow_panic_dump_score"] >= 0.6 else 0.0
    feats["flow_is_conflicting"] = 1.0 if feats["flow_conflict_score"] >= 0.6 else 0.0

    # ---- rug/impact/authority ----
    ai_rug_raw = _f_any(raw, ("ai_rug_score_01", "ai_rug_score", "ai_fraud_score", "fraud_score", "rug_score"), 0.0)
    ai_rug_01 = ai_rug_raw / 100.0 if ai_rug_raw > 1.5 else ai_rug_raw
    feats["ai_rug_score_01"] = _clip01(ai_rug_01)
    feats["ai_fraud_score"] = feats["ai_rug_score_01"]

    impact_1k_bps = _f_any(raw, ("impact_1k_bps", "impact_1000usd_bps", "priceImpact1kBps", "price_impact_bps"), 0.0)
    feats["impact_1k_bps"] = impact_1k_bps
    feats["impact_1k_pct"] = impact_1k_bps / 100.0

    mint_auth_disabled = raw.get("mint_authority_disabled")
    freeze_auth_disabled = raw.get("freeze_authority_disabled")
    mint_r = 0.5 if mint_auth_disabled is None else (0.0 if bool(mint_auth_disabled) else 0.8)
    freeze_r = 0.5 if freeze_auth_disabled is None else (0.0 if bool(freeze_auth_disabled) else 0.8)
    feats["authority_risk"] = _clip01(0.5 * mint_r + 0.5 * freeze_r)

    # ---- PF scores ----
    pf_mom = _clip01(_f_any(raw, ("pf_momentum_score", "momentum_score"), 0.0))
    pf_final = _clip01(_f_any(raw, ("pf_final_score", "final_score"), 0.0))
    pf_risk_adj = _f_any(raw, ("pf_risk_adjusted_final",), 0.0)
    if pf_risk_adj <= 0.0 and pf_final > 0.0:
        pf_risk_adj = max(0.0, pf_final * (1.0 - feats["ai_rug_score_01"]))
    feats["pf_momentum_score"] = pf_mom
    feats["pf_final_score"] = pf_final
    feats["pf_risk_adjusted_final"] = _clip01(pf_risk_adj)
    feats["pf_momo_risk_adj"] = _clip01(feats["pf_momentum_score"] * (1.0 - feats["ai_rug_score_01"]))

    # ---- pumpfun ----
    pump_stage_raw = _f_any(raw, ("pump_stage", "pump_stage_01", "pf_stage", "pf_curve_progress", "pf_curve_progress_pct"), 0.0)
    if pump_stage_raw > 1.0:
        pump_stage_01 = pump_stage_raw / 100.0 if pump_stage_raw <= 100.0 else pump_stage_raw / 1000.0
    else:
        pump_stage_01 = pump_stage_raw
    pump_stage_01 = _clip01(pump_stage_01)

    pump_buyers_60s = _i_any(raw, ("pump_buyers_60s", "buyers_60s"), 0)
    pump_buys_60s = _i_any(raw, ("pump_buys_60s", "buys_60s"), 0)
    pump_sol_60s = _f_any(raw, ("pump_sol_60s", "vol_60s_sol", "vol_60s_base"), 0.0)
    pump_idle_sec = _f_any(raw, ("pump_idle_sec", "idle_sec", "secs_since_last_trade"), 0.0)

    feats["pump_stage_01"] = pump_stage_01
    feats["pump_stage"] = pump_stage_01 * 100.0
    feats["pump_buyers_60s"] = float(pump_buyers_60s)
    feats["pump_buys_60s"] = float(pump_buys_60s)
    feats["pump_sol_60s"] = pump_sol_60s
    feats["pump_idle_sec"] = pump_idle_sec
    feats["pump_is_idle_heavy"] = 1.0 if pump_idle_sec >= 120.0 else 0.0

    vel = 0.0
    vel += _safe_ratio(pump_buys_60s, 10.0, 0.0)
    vel += _safe_ratio(pump_buyers_60s, 5.0, 0.0)
    vel += _safe_ratio(pump_sol_60s, 5.0, 0.0)

    early_factor = 1.0 + (1.0 - pump_stage_01)
    if pump_idle_sec > 0.0:
        idle_pen = _clip01(pump_idle_sec / 120.0)
        early_factor *= (1.0 - 0.5 * idle_pen)
    feats["pump_velocity_score"] = _clip(vel * early_factor, 0.0, 20.0)

    feats["pf_curve_progress"] = _f_any(raw, ("pf_curve_progress", "pf_curve_progress_pct"), 0.0)
    feats["pf_minted_pct"] = _f_any(raw, ("pf_minted_pct",), 0.0)
    feats["pf_creator_hold_pct"] = _f_any(raw, ("pf_creator_hold_pct", "creator_hold_pct", "deployer_hold_pct"), 0.0)
    feats["pf_bot_score"] = _clip01(_f_any(raw, ("pf_bot_score",), 0.0))
    feats["pf_sniper_density"] = _clip01(_f_any(raw, ("pf_sniper_density",), 0.0))

    pf_curve_danger = 0.0
    if feats["pf_curve_progress"] > 75.0:
        pf_curve_danger += 0.4
    if feats["pf_creator_hold_pct"] > 25.0:
        pf_curve_danger += 0.3
    if feats["pf_bot_score"] > 0.5:
        pf_curve_danger += 0.3
    feats["pf_curve_danger_score"] = _clip01(pf_curve_danger)

    # ---- regime (snapshot only; can be overridden by FeatureBuilder with a RegimeState) ----
    regime_score = _clip(_f_any(raw, ("regime_score", "regime_numeric", "regime_label_numeric"), 0.0), -1.0, 1.0)
    feats["regime_score"] = regime_score
    feats["regime_numeric"] = regime_score
    feats["regime_is_risk_on"] = 1.0 if regime_score >= 0.5 else 0.0
    feats["regime_is_risk_off"] = 1.0 if regime_score <= -0.5 else 0.0
    feats["regime_trend"] = _clip(_f_any(raw, ("regime_trend",), regime_score), -1.0, 1.0)
    feats["regime_liquidity"] = _clip01(_f_any(raw, ("regime_liquidity",), 0.5))
    feats["regime_manipulation_risk"] = _clip01(_f_any(raw, ("regime_manipulation_risk", "manipulation_risk"), 0.0))
    vol_norm = _norm_log_scale(vol_5m, max(vol_24h, 1.0))
    mom_norm = _clip01(abs(chg_5m_c) / 80.0)
    feats["regime_volatility"] = _clip01(0.5 * vol_norm + 0.5 * mom_norm)

    # ---- structure + dead-like ----
    structural_quality = (
        0.40 * feats["liq_norm"]
        + 0.30 * (1.0 - feats["ai_rug_score_01"])
        + 0.10 * (1.0 - feats["is_very_new"])
        + 0.20 * feats["liq_bucket_sweet"]
    )
    feats["structural_quality_score"] = _clip(structural_quality, 0.0, 1.5)

    dead_like = (1.0 if liq_usd < 15_000.0 else 0.0) + (1.0 if vol_5m < 1_000.0 else 0.0) + (1.0 if chg_5m_c <= 0.0 else 0.0)
    feats["is_dead_like"] = 1.0 if dead_like >= 2.0 else 0.0

    # ---- entry micro alpha ----
    pump_vel_norm = _clip01(_safe_ratio(feats["pump_velocity_score"], 10.0, 0.0))
    entry_micro = (
        0.22 * _clip01(short_mom / 50.0)
        + 0.20 * _clip01(mid_mom / 80.0)
        + 0.18 * _clip01((feats["buy_pressure_score"] + 1.0) / 3.0)  # map [-1..2] -> [0..1]
        + 0.18 * feats["smart_whale_alignment_consolidated"]
        + 0.12 * feats["smart_alpha_flow_boost"]
        + 0.10 * pump_vel_norm
    )
    feats["entry_micro_alpha"] = _clip(entry_micro, 0.0, 1.5)
    feats["alpha_micro_score"] = feats["entry_micro_alpha"]

    feats["liquidity_regime_score"] = _clip01(0.5 * feats["liq_norm"] + 0.3 * feats["volume_heat_intraday"] + 0.2 * feats["regime_liquidity"])

    micro_risk_on = (
        0.4 * _clip01(chg_5m_c / 40.0)
        + 0.2 * _clip01((feats["buy_pressure_score"] + 1.0) / 3.0)
        + 0.2 * feats["smart_whale_alignment_consolidated"]
        + 0.2 * max(0.0, feats["regime_score"])
    )
    feats["micro_risk_on_score"] = _clip01(micro_risk_on)
    feats["micro_risk_on_flag"] = 1.0 if feats["micro_risk_on_score"] >= 0.6 else 0.0

    # ---- defaults for optional overlays (risk, scanner meta, history, composites filled later) ----
    _seed_defaults(feats)

    # ---- compute composites/gates/sizing ----
    FeatureBuilder._enrich_entry_quality_and_sizing(feats)

    # ---- backward-compat aliases ----
    feats["vol5"] = feats["vol_5m_usd"]
    feats["buys5"] = feats["buys_5m"]
    feats["sells5"] = feats["sells_5m"]
    feats["chg5"] = feats["price_change_5m_pct"]
    feats["liq"] = feats["liq_usd"]
    feats["mcap"] = feats["mcap_usd"]

    return _finalize(feats)


def _seed_defaults(feats: Dict[str, Any]) -> None:
    # risk context defaults
    for k in (
        "ai_capacity_delta", "ai_capacity_scalar", "risk_regime_entry_gate_factor", "risk_block_new_entries",
        "risk_recommend_max_pos_mult", "risk_mode_label_risk_off", "risk_mode_label_normal", "risk_mode_label_risk_on",
        "risk_open_positions", "risk_max_positions", "risk_open_fraction", "risk_daily_dd_pct", "risk_daily_pnl_pct",
        "risk_equity_usd", "risk_exposure_usd", "risk_exposure_frac", "risk_win_rate_pct", "risk_loss_streak",
        "risk_expected_rr_ratio", "risk_stop_loss_pct", "risk_take_profit_pct", "risk_daily_trades_count",
        "risk_daily_trades_limit", "risk_daily_trades_frac",
        "ctx_open_positions", "ctx_max_positions", "ctx_daily_drawdown_pct", "ctx_daily_realized_pnl_pct",
        "ctx_risk_mode", "ctx_account_equity_usd", "ctx_exposure_usd", "ctx_exposure_frac", "ctx_expected_rr_ratio",
        "ctx_stop_loss_pct", "ctx_take_profit_pct", "ctx_daily_trades_count", "ctx_daily_trades_limit", "ctx_daily_trades_frac",
        "risk_mode_raw", "risk_mode_off", "risk_mode_normal", "risk_mode_aggressive", "risk_mode_defensive",
        "risk_capacity_base", "risk_capacity_mode_scalar", "risk_capacity_score", "risk_rr_attractiveness",
        "risk_kelly_fraction_hint", "risk_hit_daily_dd_limit", "risk_hit_daily_trades_limit", "pnl_bias_hint",
        "risk_daily_state", "risk_daily_state_defensive", "risk_daily_state_neutral", "risk_daily_state_aggressive",
        "risk_daily_heat_score", "risk_daily_cooldown_score",
    ):
        feats.setdefault(k, 0.0)

    # scanner meta defaults
    for k in (
        "scanner_source_id", "scanner_is_raydium", "scanner_is_dexscreener", "scanner_is_pumpfun", "scanner_is_momo",
        "scanner_momentum_ok", "segment_pumpfun", "segment_raydium", "segment_momo", "segment_dexscreener",
    ):
        feats.setdefault(k, 0.0)

    # history defaults
    for k in (
        "ret_5s", "ret_15s", "ret_60s", "ret_300s",
        "mom_5s_pct", "mom_15s_pct", "mom_60s_pct", "mom_300s_pct",
        "vol_15s", "vol_60s", "vol_300s",
        "vol_low", "vol_mid", "vol_high",
        "micro_trend_consistency", "micro_chop_score",
        "price_from_intraday_min_pct", "price_from_intraday_max_pct",
        "last_ret_pct", "short_trend_up", "short_trend_down", "last_move_heat",
    ):
        feats.setdefault(k, 0.0)

    # composites placeholders (will be overwritten by _enrich_entry_quality_and_sizing)
    for k in (
        "entry_quality_score", "entry_quality_risk_adjusted", "entry_quality_kelly_adjusted",
        "entry_risk_score", "entry_grade_bucket", "entry_tier_bucket",
        "position_size_scalar", "position_size_raw_scalar", "position_size_capacity_scalar", "position_size_kelly_scalar",
        "position_size_bucket", "entry_block_flag", "entry_soft_block_flag",
        "gate_structural_ok", "gate_flow_ok", "gate_rug_ok", "gate_pump_ok", "gate_context_ok",
        "gate_regime_ok", "gate_daily_limits_ok", "gate_all_soft_ok",
        "entry_score_v1", "size_scalar_v1", "entry_score", "size_scalar", "position_scalar", "entry_block_hard",
        "slippage_risk_score", "entry_quality_vs_degen",
        "entry_label_recovery_trade", "entry_label_heat_trade", "entry_label_neutral_trade",
    ):
        feats.setdefault(k, 0.0)


def _finalize(feats: Dict[str, Any]) -> Dict[str, float]:
    # enforce no missing keys
    out: Dict[str, float] = {}
    for k in CANONICAL_KEYS:
        out[k] = _nan0(feats.get(k, 0.0))

    # hard-enforce numeric-only + finite, plus gentle clamps for “should-be-bounded” keys
    # (raw keys already hard-capped by _nan0)
    bounded_01 = (
        "liq_norm", "mcap_norm", "fdv_norm",
        "buy_ratio_1m", "buy_ratio_5m",
        "volume_heat_intraday",
        "whale_buy_score", "whale_sell_score", "whale_share_5m",
        "smart_wallet_score", "smart_buy_score_5m", "smart_sell_score_5m",
        "smart_flow_bias_01",
        "smart_alpha_bias", "smart_whale_alignment_consolidated", "smart_alpha_flow_boost",
        "flow_accumulation_score", "flow_panic_dump_score", "flow_conflict_score",
        "ai_rug_score_01", "ai_fraud_score",
        "authority_risk",
        "pf_momentum_score", "pf_final_score", "pf_risk_adjusted_final", "pf_momo_risk_adj",
        "pump_stage_01", "pump_is_idle_heavy",
        "pf_bot_score", "pf_sniper_density", "pf_curve_danger_score",
        "regime_liquidity", "regime_manipulation_risk", "regime_volatility",
        "liquidity_regime_score", "micro_risk_on_score",
        "risk_open_fraction", "risk_exposure_frac", "risk_daily_trades_frac",
        "risk_capacity_base", "risk_capacity_score", "risk_rr_attractiveness", "risk_kelly_fraction_hint",
        "risk_daily_heat_score", "risk_daily_cooldown_score",
        "position_size_scalar", "position_size_raw_scalar", "position_size_capacity_scalar", "position_size_kelly_scalar",
        "slippage_risk_score",
        "entry_label_recovery_trade", "entry_label_heat_trade", "entry_label_neutral_trade",
    )
    for k in bounded_01:
        if k in out:
            out[k] = _clip01(out[k])

    # clamp known range keys
    clamp_m11 = ("orderflow_net_5m_norm", "smart_net_side_5m", "whale_net_side_5m", "regime_score", "regime_numeric", "regime_trend", "entry_quality_vs_degen")
    for k in clamp_m11:
        if k in out:
            out[k] = _clip(out[k], -1.0, 1.0)

    # grade/tier/buckets should be in small discrete ranges
    for k, hi in (("entry_grade_bucket", 3.0), ("entry_tier_bucket", 3.0), ("position_size_bucket", 3.0)):
        out[k] = _clip(out.get(k, 0.0), 0.0, hi)

    # flags should be 0/1
    flags = (
        "is_super_early", "is_very_new", "is_newish", "is_mid_age", "is_old",
        "liq_bucket_ultra_low", "liq_bucket_low", "liq_bucket_sweet", "liq_bucket_mid", "liq_bucket_high",
        "mcap_bucket_early", "mcap_bucket_very_late",
        "has_fast_dump_5m", "has_fast_pump_5m", "is_pump_chasing_risk",
        "momentum_regime_bear", "momentum_regime_flat", "momentum_regime_bull",
        "smart_recent_buy_flag", "smart_recent_sell_flag",
        "flow_is_strong_accum", "flow_is_panic_dump", "flow_is_conflicting",
        "pump_is_idle_heavy",
        "regime_is_risk_on", "regime_is_risk_off",
        "micro_risk_on_flag",
        "risk_block_new_entries",
        "risk_mode_label_risk_off", "risk_mode_label_normal", "risk_mode_label_risk_on",
        "risk_mode_off", "risk_mode_normal", "risk_mode_aggressive", "risk_mode_defensive",
        "risk_hit_daily_dd_limit", "risk_hit_daily_trades_limit",
        "entry_block_flag", "entry_soft_block_flag",
        "gate_structural_ok", "gate_flow_ok", "gate_rug_ok", "gate_pump_ok", "gate_context_ok",
        "gate_regime_ok", "gate_daily_limits_ok", "gate_all_soft_ok",
        "entry_block_hard",
        "scanner_is_raydium", "scanner_is_dexscreener", "scanner_is_pumpfun", "scanner_is_momo",
        "scanner_momentum_ok",
        "segment_pumpfun", "segment_raydium", "segment_momo", "segment_dexscreener",
        "vol_low", "vol_mid", "vol_high",
        "short_trend_up", "short_trend_down",
        "is_dead_like",
    )
    for k in flags:
        out[k] = 1.0 if out.get(k, 0.0) >= 0.5 else 0.0

    # ensure required alias keys match canonical values
    out["vol5"] = out["vol_5m_usd"]
    out["buys5"] = out["buys_5m"]
    out["sells5"] = out["sells_5m"]
    out["chg5"] = out["price_change_5m_pct"]
    out["liq"] = out["liq_usd"]
    out["mcap"] = out["mcap_usd"]

    return out


# =============================================================================
# FeatureBuilder: adds enrichment (still pure: accepts data, does not fetch data)
# =============================================================================

@dataclass
class FeatureBuilder:
    """
    High-level builder that merges optional enrichments without IO.
    """
    new_age_threshold_min: float = 60.0  # kept for compat; not used for logic

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
        try:
            feats: Dict[str, Any] = dict(build_features(snapshot))

            # Regime object override
            if regime is not None:
                rs = 0.0
                try:
                    if hasattr(regime, "as_numeric"):
                        rs = float(regime.as_numeric())
                    elif hasattr(regime, "get"):
                        name = str(regime.get()).lower()
                        rs = {"risk_off": -1.0, "normal": 0.0, "risk_on": 1.0, "aggressive": 0.7}.get(name, 0.0)
                except Exception:
                    rs = 0.0
                rs = _clip(rs, -1.0, 1.0)
                feats["regime_score"] = rs
                feats["regime_numeric"] = rs
                feats["regime_is_risk_on"] = 1.0 if rs >= 0.5 else 0.0
                feats["regime_is_risk_off"] = 1.0 if rs <= -0.5 else 0.0
                feats["regime_trend"] = rs

            # wallet_signals overlay (only keys we know)
            if wallet_signals:
                for k in (
                    "smart_wallet_score", "smart_flow_raw", "smart_wallet_count_5m",
                    "smart_buy_score_5m", "smart_sell_score_5m",
                    "smart_last_buy_sec_ago", "smart_last_sell_sec_ago",
                    "smart_last_cluster_is_alpha",
                    "whales_5m", "whale_vol_5m_usd", "whale_buy_score", "whale_sell_score",
                    "whale_share_5m", "unique_buyers_5m",
                    "smart_net_side_5m", "whale_net_side_5m",
                    "smart_alpha_bias", "smart_whale_alignment", "smart_whale_alignment_abs",
                    "whale_domination_5m", "smart_vs_whale_conflict",
                    "smart_accumulation_score", "smart_panic_dump_score", "whale_liquidity_footprint",
                    "pump_buyers_60s", "pump_buys_60s", "pump_sol_60s", "pump_idle_sec", "pump_stage_01",
                ):
                    if k in wallet_signals:
                        feats[k] = _nan0(wallet_signals[k])

            # AI signals overlay
            if ai_signals:
                raw_rug = _f_any(ai_signals, ("ai_rug_score_01", "ai_rug_score", "ai_fraud_score"), 0.0)
                rug01 = raw_rug / 100.0 if raw_rug > 1.5 else raw_rug
                feats["ai_rug_score_01"] = _clip01(rug01)
                feats["ai_fraud_score"] = feats["ai_rug_score_01"]

            # risk context overlay (bounded + copy into ctx_* aliases)
            if risk_context:
                open_pos = _nan0(risk_context.get("open_positions", 0.0))
                max_pos = _nan0(risk_context.get("max_positions", 0.0))
                dd_pct = _nan0(risk_context.get("daily_drawdown_pct", 0.0))
                pnl_pct = _nan0(risk_context.get("daily_realized_pnl_pct", 0.0))
                mode = _nan0(risk_context.get("risk_mode", 0.0))
                eq_usd = _nan0(risk_context.get("account_equity_usd", 0.0))
                max_dd_pct = _nan0(risk_context.get("max_daily_dd_pct", 30.0))
                exposure_usd = _nan0(risk_context.get("total_exposure_usd", 0.0))
                win_rate = _nan0(risk_context.get("rolling_win_rate_pct", 0.0))
                streak = _nan0(risk_context.get("recent_loss_streak", 0.0))

                ai_capacity_delta = _nan0(risk_context.get("ai_capacity_delta", 0.0))
                regime_gate = _nan0(risk_context.get("regime_entry_gate_factor", 1.0))
                block_new_entries = bool(risk_context.get("block_new_entries", False))
                max_pos_mult = _nan0(risk_context.get("recommend_max_pos_mult", 1.0))
                risk_mode_label = str(risk_context.get("risk_mode_label", "")).lower()

                rr_target = _nan0(risk_context.get("expected_rr_ratio", 0.0))
                stop_loss_pct = _nan0(risk_context.get("default_stop_loss_pct", 0.0))
                take_profit_pct = _nan0(risk_context.get("default_take_profit_pct", 0.0))

                trades_count = _nan0(risk_context.get("daily_trades_count", 0.0))
                trades_limit = _nan0(risk_context.get("daily_trades_limit", 0.0))

                frac_open = _safe_ratio(open_pos, max(max_pos, 1.0), 0.0)
                exposure_frac = _safe_ratio(exposure_usd, max(eq_usd, 1.0), 0.0)
                trades_frac = _safe_ratio(trades_count, max(trades_limit, 1.0), 0.0)

                feats["ai_capacity_delta"] = ai_capacity_delta
                feats["ai_capacity_scalar"] = _clip01((ai_capacity_delta + 0.6) / 1.2)
                feats["risk_regime_entry_gate_factor"] = regime_gate
                feats["risk_block_new_entries"] = 1.0 if block_new_entries else 0.0
                feats["risk_recommend_max_pos_mult"] = max_pos_mult
                feats["risk_mode_label_risk_off"] = 1.0 if risk_mode_label == "risk_off" else 0.0
                feats["risk_mode_label_normal"] = 1.0 if risk_mode_label == "normal" else 0.0
                feats["risk_mode_label_risk_on"] = 1.0 if risk_mode_label == "risk_on" else 0.0

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

                feats["risk_mode_raw"] = mode
                feats["risk_mode_off"] = 1.0 if mode == 0 else 0.0
                feats["risk_mode_normal"] = 1.0 if mode == 1 else 0.0
                feats["risk_mode_aggressive"] = 1.0 if mode == 2 else 0.0
                feats["risk_mode_defensive"] = 1.0 if mode == 3 else 0.0

                base_capacity = (1.0 - _clip01(frac_open)) * (1.0 - _clip01(dd_pct / max(max_dd_pct, 1.0)))
                mode_scalar = 1.15 if mode == 2 else (0.6 if mode == 3 else 1.0)
                feats["risk_capacity_base"] = _clip01(base_capacity)
                feats["risk_capacity_mode_scalar"] = _clip(mode_scalar, 0.3, 1.5)
                feats["risk_capacity_score"] = _clip01(base_capacity * mode_scalar)

                rr_norm = _clip01(rr_target / 3.0)
                sl_norm = _clip01(stop_loss_pct / 20.0)
                feats["risk_rr_attractiveness"] = _clip01(rr_norm * (1.0 - 0.4 * sl_norm))

                p = _clip01(win_rate / 100.0)
                b = rr_target if rr_target > 0.0 else 0.0
                if b > 0.0:
                    q = 1.0 - p
                    kelly = (b * p - q) / b
                    feats["risk_kelly_fraction_hint"] = _clip01(kelly)
                else:
                    feats["risk_kelly_fraction_hint"] = 0.0

                feats["risk_hit_daily_dd_limit"] = _bool01(risk_context.get("hit_daily_dd_limit", False))
                feats["risk_hit_daily_trades_limit"] = _bool01(risk_context.get("hit_daily_trades_limit", False))

                pnl_norm = _clip(pnl_pct / max(max_dd_pct, 1.0), -2.0, 2.0)
                feats["pnl_bias_hint"] = pnl_norm
                feats["risk_daily_state"] = -1.0 if pnl_norm <= -0.5 else (1.0 if pnl_norm >= 0.5 else 0.0)
                feats["risk_daily_state_defensive"] = 1.0 if feats["risk_daily_state"] < 0 else 0.0
                feats["risk_daily_state_neutral"] = 1.0 if feats["risk_daily_state"] == 0 else 0.0
                feats["risk_daily_state_aggressive"] = 1.0 if feats["risk_daily_state"] > 0 else 0.0
                feats["risk_daily_heat_score"] = _clip01(max(0.0, pnl_norm))
                feats["risk_daily_cooldown_score"] = _clip01(max(0.0, -pnl_norm))

            # scanner source enrichment (pure)
            self._enrich_scanner_metadata(feats, snapshot, scanner_source)

            # history enrichment (pure)
            if history:
                self._enrich_short_term_history(feats, history)

            # recompute composites after all overlays
            self._enrich_entry_quality_and_sizing(feats)

            # keep aliases consistent
            feats["vol5"] = feats["vol_5m_usd"]
            feats["buys5"] = feats["buys_5m"]
            feats["sells5"] = feats["sells_5m"]
            feats["chg5"] = feats["price_change_5m_pct"]
            feats["liq"] = feats["liq_usd"]
            feats["mcap"] = feats["mcap_usd"]

            return _finalize(feats)

        except Exception:
            log.exception("FeatureBuilder.build failed")
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
        mapping = {"": 0.0, "raydium": 1.0, "dexscreener": 2.0, "pumpfun": 3.0, "momo": 4.0}
        feats["scanner_source_id"] = mapping.get(src, 0.0)
        feats["scanner_is_raydium"] = 1.0 if src == "raydium" else 0.0
        feats["scanner_is_dexscreener"] = 1.0 if src == "dexscreener" else 0.0
        feats["scanner_is_pumpfun"] = 1.0 if src == "pumpfun" else 0.0
        feats["scanner_is_momo"] = 1.0 if src == "momo" else 0.0

        mom_ok = snapshot.get("scanner_momentum_ok", snapshot.get("momentum_ok", snapshot.get("MOMENTUM_OK", None)))
        feats["scanner_momentum_ok"] = _bool01(mom_ok, default=False)

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
        if not history or len(history) < 2:
            return

        series: list[tuple[float, float]] = []
        for snap in history:
            t = _f_any(snap, ("ts", "time", "t"), 0.0)
            p = _f_any(snap, ("price_usd", "price", "p", "px_usd"), 0.0)
            if p > 0:
                series.append((t, p))

        if len(series) < 2:
            prices = []
            for snap in history:
                p = _f_any(snap, ("price_usd", "price", "p", "px_usd"), 0.0)
                if p > 0:
                    prices.append(p)
            FeatureBuilder._enrich_history_from_prices(feats, prices)
            return

        series.sort(key=lambda x: x[0])
        if abs(series[-1][0]) < 1e-9:
            FeatureBuilder._enrich_history_from_prices(feats, [p for _, p in series])
            return

        last_t, last_p = series[-1]

        def price_at_or_before(target_t: float) -> Optional[float]:
            for i in range(len(series) - 1, -1, -1):
                t, p = series[i]
                if t <= target_t:
                    return p
            return None

        windows = {"5s": 5.0, "15s": 15.0, "60s": 60.0, "300s": 300.0}
        for label, sec in windows.items():
            old_p = price_at_or_before(last_t - sec)
            if old_p is None or old_p <= 0:
                continue
            feats[f"ret_{label}"] = _clip(math.log(last_p / old_p), -2.0, 2.0)
            feats[f"mom_{label}_pct"] = _clip((last_p / old_p - 1.0) * 100.0, -200.0, 300.0)

        log_rets: list[float] = []
        for i in range(1, len(series)):
            p0 = series[i - 1][1]
            p1 = series[i][1]
            if p0 > 0 and p1 > 0:
                log_rets.append(math.log(p1 / p0))

        def std(xs: Sequence[float]) -> float:
            if len(xs) < 2:
                return 0.0
            m = sum(xs) / len(xs)
            return float(math.sqrt(sum((x - m) ** 2 for x in xs) / len(xs)))

        if len(log_rets) >= 2:
            short = log_rets[-5:] if len(log_rets) >= 5 else log_rets
            med = log_rets[-20:] if len(log_rets) >= 20 else log_rets
            long = log_rets[-100:] if len(log_rets) >= 100 else log_rets
            feats["vol_15s"] = _clip(std(short), 0.0, 2.0)
            feats["vol_60s"] = _clip(std(med), 0.0, 2.0)
            feats["vol_300s"] = _clip(std(long), 0.0, 2.0)

            v60 = float(feats["vol_60s"])
            feats["vol_low"] = 1.0 if v60 < 0.01 else 0.0
            feats["vol_mid"] = 1.0 if 0.01 <= v60 < 0.05 else 0.0
            feats["vol_high"] = 1.0 if v60 >= 0.05 else 0.0

            signs = [1.0 if r > 0 else -1.0 for r in log_rets]
            tc = abs(sum(signs)) / float(len(signs)) if signs else 0.0
            feats["micro_trend_consistency"] = _clip01(tc)
            feats["micro_chop_score"] = _clip01(1.0 - tc)

        try:
            ps = [p for _, p in series]
            p_min, p_max = min(ps), max(ps)
            feats["price_from_intraday_min_pct"] = _clip((last_p / p_min - 1.0) * 100.0, -200.0, 1000.0) if p_min > 0 else 0.0
            feats["price_from_intraday_max_pct"] = _clip((last_p / p_max - 1.0) * 100.0, -200.0, 1000.0) if p_max > 0 else 0.0
        except Exception:
            pass

        if len(series) >= 2:
            prev_p = series[-2][1]
            if prev_p > 0:
                last_ret_pct = (last_p / prev_p - 1.0) * 100.0
                feats["last_ret_pct"] = _clip(last_ret_pct, -200.0, 300.0)
                feats["short_trend_up"] = 1.0 if last_ret_pct > 0 else 0.0
                feats["short_trend_down"] = 1.0 if last_ret_pct < 0 else 0.0
                feats["last_move_heat"] = _clip01(abs(last_ret_pct) / 20.0)

    @staticmethod
    def _enrich_history_from_prices(feats: Dict[str, Any], prices: Sequence[float]) -> None:
        if not prices or len(prices) < 2:
            return

        def log_ret(new: float, old: float) -> float:
            if new <= 0 or old <= 0:
                return 0.0
            return float(math.log(new / old))

        def pct(new: float, old: float) -> float:
            if old <= 0:
                return 0.0
            return float((new / old - 1.0) * 100.0)

        n = len(prices)
        last = prices[-1]
        offsets = {"5s": 2, "15s": 5, "60s": 20, "300s": 100}
        for label, off in offsets.items():
            if n <= off:
                continue
            old = prices[-off - 1]
            feats[f"ret_{label}"] = _clip(log_ret(last, old), -2.0, 2.0)
            feats[f"mom_{label}_pct"] = _clip(pct(last, old), -200.0, 300.0)

    @staticmethod
    def _enrich_entry_quality_and_sizing(feats: Dict[str, Any]) -> None:
        # Pull core signals
        structural_quality = _clip(feats.get("structural_quality_score", 0.0), 0.0, 1.5)
        entry_micro = _clip(feats.get("entry_micro_alpha", 0.0), 0.0, 1.5)
        pf_risk_adj = _clip01(feats.get("pf_risk_adjusted_final", 0.0))
        regime_score = _clip(feats.get("regime_score", 0.0), -1.0, 1.0)
        ai_rug = _clip01(feats.get("ai_rug_score_01", 0.0))
        is_dead_like = _clip01(feats.get("is_dead_like", 0.0))
        liq_ultra_low = _clip01(feats.get("liq_bucket_ultra_low", 0.0))
        liq_sweet = _clip01(feats.get("liq_bucket_sweet", 0.0))

        pf_curve_danger = _clip01(feats.get("pf_curve_danger_score", 0.0))
        pump_stage_01 = _clip01(feats.get("pump_stage_01", 0.0))
        pump_idle_sec = _nan0(feats.get("pump_idle_sec", 0.0))
        pump_idle_long = 1.0 if pump_idle_sec >= 120.0 else 0.0
        pump_vel_norm = _clip01(_safe_ratio(feats.get("pump_velocity_score", 0.0), 10.0, 0.0))

        smart_alpha_boost = _clip01(feats.get("smart_alpha_flow_boost", 0.0))
        smart_whale_align = _clip01(feats.get("smart_whale_alignment_consolidated", 0.0))
        flow_accum = _clip01(feats.get("flow_accumulation_score", 0.0))
        flow_dump = _clip01(feats.get("flow_panic_dump_score", 0.0))
        whale_share = _clip01(feats.get("whale_share_5m", 0.0))
        whale_liq_fp = _clip01(feats.get("whale_liquidity_footprint", 0.0))

        risk_capacity = _clip01(feats.get("risk_capacity_score", 1.0))
        rr_attract = _clip01(feats.get("risk_rr_attractiveness", 0.5))
        kelly_hint = _clip01(feats.get("risk_kelly_fraction_hint", 0.0))

        # Base quality (0..1.5)
        base_quality = (
            0.35 * structural_quality
            + 0.25 * pf_risk_adj
            + 0.25 * entry_micro
            + 0.10 * smart_alpha_boost
            + 0.05 * rr_attract
        )
        regime_boost = 1.0 + 0.20 * max(regime_score, 0.0)
        rug_penalty = 1.0 - 0.7 * ai_rug
        flow_boost = 1.0 + 0.15 * flow_accum - 0.15 * flow_dump
        stage_penalty = _clip(1.0 - 0.3 * pump_stage_01, 0.4, 1.1)

        entry_quality = _clip(base_quality * regime_boost * rug_penalty * flow_boost * stage_penalty, 0.0, 1.5)
        feats["entry_quality_score"] = entry_quality

        # Soft gates
        gate_structural = 1.0 if (structural_quality >= 0.35 and liq_ultra_low < 0.5) else 0.0
        gate_flow = 1.0 if flow_dump <= 0.7 else 0.0
        gate_rug = 1.0 if ai_rug <= 0.75 else 0.0
        gate_pump = 1.0 if (pf_curve_danger <= 0.8 and pump_stage_01 <= 0.97) else 0.0
        gate_context = 1.0 if risk_capacity >= 0.2 else 0.0

        hit_dd = _clip01(feats.get("risk_hit_daily_dd_limit", 0.0))
        hit_trades = _clip01(feats.get("risk_hit_daily_trades_limit", 0.0))
        gate_regime = 1.0 if regime_score > -0.75 else 0.0
        gate_daily = 1.0 if (hit_dd < 0.5 and hit_trades < 0.5) else 0.0

        feats["gate_structural_ok"] = gate_structural
        feats["gate_flow_ok"] = gate_flow
        feats["gate_rug_ok"] = gate_rug
        feats["gate_pump_ok"] = gate_pump
        feats["gate_context_ok"] = gate_context
        feats["gate_regime_ok"] = gate_regime
        feats["gate_daily_limits_ok"] = gate_daily
        feats["gate_all_soft_ok"] = 1.0 if (gate_structural and gate_flow and gate_rug and gate_pump and gate_context and gate_regime and gate_daily) else 0.0

        # Hard blocks
        block_hard = False
        if ai_rug >= 0.85:
            block_hard = True
        if is_dead_like >= 0.5:
            block_hard = True
        if liq_ultra_low >= 0.5 and liq_sweet < 0.5:
            block_hard = True
        if flow_dump >= 0.9:
            block_hard = True
        if pf_curve_danger >= 0.9:
            block_hard = True
        if pump_stage_01 >= 0.98 and pump_vel_norm <= 0.2 and pump_idle_long >= 0.5:
            block_hard = True
        feats["entry_block_flag"] = 1.0 if block_hard else 0.0
        feats["entry_block_hard"] = feats["entry_block_flag"]

        # Sizing
        raw_size = (_clip(entry_quality, 0.0, 1.5) / 1.5) * (1.0 - 0.6 * ai_rug) * (1.0 - 0.5 * is_dead_like)
        tilt = (
            max(0.0, smart_whale_align - 0.5) * 0.3
            + smart_alpha_boost * 0.2
            + flow_accum * 0.2
        )
        tilt = _clip01(tilt)
        if pump_stage_01 <= 0.7 and pump_vel_norm >= 0.3:
            tilt = _clip01(tilt + 0.15 * pump_vel_norm)
        raw_size = _clip01(raw_size * (1.0 + 0.3 * tilt))

        if whale_liq_fp > 0.5 and flow_dump < 0.4:
            raw_size *= 1.0 + 0.1 * (whale_liq_fp - 0.5)

        late_penalty = _clip(1.0 - 0.5 * pump_stage_01, 0.3, 1.0)
        raw_size = _clip01(raw_size * late_penalty)

        pos_size = raw_size * risk_capacity
        if block_hard:
            pos_size = 0.0

        feats["position_size_scalar"] = _clip01(pos_size)
        feats["position_size_raw_scalar"] = _clip01(raw_size)
        feats["position_size_capacity_scalar"] = _clip01(risk_capacity)

        kelly_floor = 0.2
        kelly_mix = kelly_floor + (1.0 - kelly_floor) * kelly_hint
        feats["position_size_kelly_scalar"] = _clip01(pos_size * kelly_mix) if not block_hard else 0.0

        # Entry risk score (0..1)
        entry_risk = (
            0.45 * ai_rug
            + 0.18 * is_dead_like
            + 0.10 * liq_ultra_low
            + 0.07 * _clip01(whale_share * 1.5)
            + 0.07 * flow_dump
            + 0.08 * pf_curve_danger
            + 0.05 * pump_stage_01
            + 0.05 * pump_idle_long
        )
        feats["entry_risk_score"] = _clip01(entry_risk)

        # Soft block (not hard) when extremely risky
        feats["entry_soft_block_flag"] = 1.0 if feats["entry_risk_score"] >= 0.85 else 0.0

        # buckets/tiers
        q = entry_quality
        s = feats["position_size_scalar"]
        if q < 0.35 or s <= 0.0 or block_hard:
            feats["entry_tier_bucket"] = 0.0
        elif q < 0.7:
            feats["entry_tier_bucket"] = 1.0
        elif q < 1.1:
            feats["entry_tier_bucket"] = 2.0
        else:
            feats["entry_tier_bucket"] = 3.0

        if s <= 0.0:
            feats["position_size_bucket"] = 0.0
        elif s < 0.33:
            feats["position_size_bucket"] = 1.0
        elif s < 0.66:
            feats["position_size_bucket"] = 2.0
        else:
            feats["position_size_bucket"] = 3.0

        # stable aliases
        feats["entry_score_v1"] = feats["entry_quality_score"]
        feats["size_scalar_v1"] = feats["position_size_scalar"]
        feats["entry_score"] = feats["entry_quality_score"]
        feats["size_scalar"] = feats["position_size_scalar"]
        feats["position_scalar"] = feats["position_size_scalar"]

        # risk-adjusted quality
        eq = feats["entry_quality_score"]
        risk = feats["entry_risk_score"]
        eq_risk_adj = _clip(eq * (1.0 - 0.6 * risk), 0.0, 1.5)
        feats["entry_quality_risk_adjusted"] = eq_risk_adj
        feats["entry_quality_kelly_adjusted"] = _clip(eq_risk_adj * (0.4 + 0.6 * kelly_hint), 0.0, 1.5)

        # slippage risk
        impact_pct = _clip01(_nan0(feats.get("impact_1k_pct", 0.0)) / 5.0)
        liq_norm = _clip01(_nan0(feats.get("liq_norm", 0.0)))
        feats["slippage_risk_score"] = _clip01(0.7 * impact_pct + 0.3 * (1.0 - liq_norm))

        # grade bucket
        if block_hard or eq < 0.3 or risk >= 0.9:
            feats["entry_grade_bucket"] = 0.0
        elif eq < 0.7 or risk >= 0.7:
            feats["entry_grade_bucket"] = 1.0
        elif eq < 1.1 or risk >= 0.5:
            feats["entry_grade_bucket"] = 2.0
        else:
            feats["entry_grade_bucket"] = 3.0

        # quality-vs-degen slider [-1, +1]
        quality_term = _clip01(eq / 1.5)
        safety_term = 1.0 - ai_rug
        qv = quality_term * safety_term * (1.0 - 0.5 * risk)
        feats["entry_quality_vs_degen"] = _clip(2.0 * qv - 1.0, -1.0, 1.0)

        # labels for analytics
        daily_state = _nan0(feats.get("risk_daily_state", 0.0))
        feats["entry_label_recovery_trade"] = 1.0 if (daily_state < 0.0 and s > 0.0) else 0.0
        feats["entry_label_heat_trade"] = 1.0 if (daily_state > 0.0 and s > 0.0) else 0.0
        feats["entry_label_neutral_trade"] = 1.0 if (daily_state == 0.0 and s > 0.0) else 0.0


__all__ = [
    "FEATURE_VERSION",
    "FEATURE_SCHEMA_NAME",
    "FEATURE_SCHEMA_ID",
    "FEATURE_PHASE",
    "CANONICAL_KEYS",
    "build_features",
    "FeatureBuilder",
]
