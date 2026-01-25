from __future__ import annotations

"""
utils_pkg/pair_filters.py

Shared pair / token filter logic (V3+++ / quant-grade).
=======================================================

Role
----
Canonical viability + scoring layer for *all* scanners (Raydium / Pump.fun /
Dexscreener / on-chain / future feeds). This module:

  1) Normalizes meta fields (best-effort; accepts common aliases).
  2) Computes a momentum score and a risk-adjusted final score (0..1).
  3) Applies hard filters + regime-aware thresholds.
  4) Enriches meta with:
        - pf_momentum_score
        - pf_final_score
        - pf_risk_adjusted_final
        - ai_rug_score_01
        - pf_fdv_to_liq
        - pf_profile
        - _score_debug
        - _filter_debug
        - pf_decision  (rich decision payload)

Public API (stable)
-------------------
- is_viable_pair(meta) -> bool
- evaluate_pair(meta)  -> dict (rich info; calls is_viable_pair)
- compute_momentum_score(meta) -> float (also writes pf_momentum_score)
- compute_final_score(meta)    -> float (also writes pf_* fields)
- enrich_pair_meta(meta)       -> dict (scores + profile + debug; no hard filters)

Notes
-----
- No external deps; safe for Render and local.
- Does NOT execute trades; just scoring + filtering.
- Designed to be deterministic and robust to missing/dirty fields.

Expected meta keys (all optional; more keys => smarter):
  Identity:
    mint, symbol, dex, source, chain, regime

  Size / valuation:
    price_usd, liq_usd, mcap_usd, fdv_usd, token_age_min

  Flow:
    vol_5m_usd, vol_1h_usd, vol_24h_usd, buys_5m, sells_5m

  Whales:
    whales_5m, whale_vol_5m_usd, largest_buy_5m_sol

  Price change %:
    price_change_5m_pct, price_change_15m_pct, price_change_1h_pct

  Holders:
    holders, holders_5m_change

  Security / risk:
    is_honeypot, trading_paused, blacklisted
    ai_rug_score_01, ai_rug_score, ai_fraud_score
    top10_hold_pct, dev_hold_pct, mint_authority_disabled, freeze_authority_disabled
    lp_locked_pct, renounced, burned_pct  (optional extras if you have them)

  Microstructure:
    impact_1k_bps, spread_bps, smart_wallet_score

"""

import math
import time
from collections.abc import Mapping, MutableMapping
from dataclasses import dataclass
from typing import Any

from logging_setup import get_logger

log = get_logger()

# =============================================================================
# Config (safe defaults if config import fails)
# =============================================================================

try:
    import config as C  # type: ignore
except Exception:  # pragma: no cover

    class _C:
        # Hard windows
        PF_SCAN_MIN_LIQ_USD = 150.0
        PF_SCAN_MAX_LIQ_USD = 0.0  # 0 = no max
        PF_SCAN_MIN_MCAP_USD = 500.0
        PF_SCAN_MAX_MCAP_USD = 0.0  # 0 = no max
        PF_SCAN_MAX_TOKEN_AGE_MIN = 720.0  # 12h

        # Activity thresholds
        PF_SCAN_MIN_5M_VOL_USD = 30.0
        PF_SCAN_MIN_5M_BUYS = 1
        PF_SCAN_MIN_BUY_TO_SELL_RATIO = 1.05
        PF_SCAN_MIN_BUY_SHARE = 0.35

        # Whales
        PF_SCAN_MIN_WHALE_SIZE_SOL = 1.5
        PF_SCAN_MIN_WHALE_COUNT_5M = 0

        # Risk caps
        PF_MAX_RUG_RISK_SCORE = 0.80

        # Score gates (regime multiplies these)
        PF_MIN_MOMENTUM_SCORE = 0.14
        PF_MIN_FINAL_SCORE = 0.10

        # Penalty weights
        PF_RISK_PENALTY_WEIGHT = 0.60  # AI rug penalty strength
        PF_IMPACT_PENALTY_WEIGHT = 0.25
        PF_SMART_RISK_BOOST = 0.25
        PF_VALUATION_PENALTY_WEIGHT = 0.15

        # Concentration / authority
        PF_SCAN_MAX_TOP10_HOLD_PCT = 95.0
        PF_SCAN_MAX_DEV_HOLD_PCT = 60.0
        PF_REQUIRE_MINT_AUTH_DISABLED = 1
        PF_REQUIRE_FREEZE_AUTH_DISABLED = 1

        # Slippage
        PF_SCAN_MAX_IMPACT_1K_BPS = 800.0

        # Optional smart min
        PF_MIN_SMART_SCORE = 0.0

        # Score weights (sum ~ 1.0; only used if present)
        PF_W_PRICE_5M = 0.23
        PF_W_PRICE_15M = 0.10
        PF_W_PRICE_1H = 0.05
        PF_W_VOLUME = 0.20
        PF_W_BUY_PRESSURE = 0.14
        PF_W_TRADES = 0.06
        PF_W_WHALES = 0.10
        PF_W_AGE = 0.07
        PF_W_HOLDERS_GROWTH = 0.03
        PF_W_SMART = 0.02

        # Anti-manipulation (optional)
        PF_PUMP_DUMP_SELL_PRESSURE_PENALTY = 0.18  # 0..1
        PF_SPIKE_LOW_VOL_PENALTY = 0.12  # 0..1

        # Logging throttle (seconds)
        PF_REJECT_LOG_THROTTLE_S = 60

    C = _C()  # type: ignore


# Optional regime integration
try:
    from utils_pkg import regime as _regime  # type: ignore
except Exception:  # pragma: no cover
    _regime = None  # type: ignore


Meta = MutableMapping[str, Any]

# =============================================================================
# Local throttling fallback (if logger doesn't support log.every)
# =============================================================================

_THROTTLE: dict[str, float] = {}


def _log_throttled(key: str, secs: float, level: str, msg: str) -> None:
    """
    Throttle log lines without depending on custom logger features.
    Prefer logger.every if present; otherwise use in-process throttling.
    """
    if hasattr(log, "every"):
        try:
            # type: ignore[attr-defined]
            log.every(key, int(secs), msg)
            return
        except Exception:
            pass

    now = time.time()
    last = _THROTTLE.get(key, 0.0)
    if now - last < secs:
        return
    _THROTTLE[key] = now

    fn = getattr(log, level, None)
    if callable(fn):
        fn(msg)
    else:
        log.info(msg)


# =============================================================================
# Numeric helpers
# =============================================================================


def _finite(x: float) -> bool:
    return not (math.isnan(x) or math.isinf(x))


def _to_float(v: Any, default: float = 0.0) -> float:
    try:
        f = float(v)
        if not _finite(f):
            return default
        return f
    except Exception:
        return default


def _to_int(v: Any, default: int = 0) -> int:
    try:
        return int(v)
    except Exception:
        return default


def _to_bool(v: Any, default: bool = False) -> bool:
    if isinstance(v, bool):
        return v
    if isinstance(v, int | float):
        return bool(v)
    if isinstance(v, str):
        return v.strip().lower() in {"1", "true", "yes", "y", "on"}
    return default


def _clamp(x: float, lo: float, hi: float) -> float:
    if x < lo:
        return lo
    if x > hi:
        return hi
    return x


def _soft_scale(x: float, lo: float, hi: float) -> float:
    """
    Map x -> [0,1] between lo and hi (clamped).
    """
    if hi <= lo:
        return 0.0
    if x <= lo:
        return 0.0
    if x >= hi:
        return 1.0
    return (x - lo) / (hi - lo)


def _first_present(meta: Mapping[str, Any], keys: list[str]) -> Any | None:
    for k in keys:
        if k in meta and meta.get(k) is not None:
            return meta.get(k)
    return None


def _get_float(meta: Mapping[str, Any], keys: list[str], default: float = 0.0) -> float:
    v = _first_present(meta, keys)
    return _to_float(v, default)


def _get_int(meta: Mapping[str, Any], keys: list[str], default: int = 0) -> int:
    v = _first_present(meta, keys)
    return _to_int(v, default)


def _get_bool(meta: Mapping[str, Any], keys: list[str], default: bool = False) -> bool:
    v = _first_present(meta, keys)
    return _to_bool(v, default)


def _norm_pct(x: float, default: float = 0.0) -> float:
    """
    Percent sanity clamp; scanners sometimes emit crazy values.
    """
    if not _finite(x):
        return default
    return _clamp(x, -10_000.0, 100_000.0)


# =============================================================================
# Regime
# =============================================================================


def _get_regime(meta: Mapping[str, Any]) -> str:
    r = meta.get("regime")
    if isinstance(r, str) and r.strip():
        return r.strip()

    src = meta.get("source") or meta.get("dex") or "generic"
    src = str(src).lower()

    if _regime is not None:
        if hasattr(_regime, "regime_for_src"):
            try:
                rr = _regime.regime_for_src(src, default="live")  # type: ignore[arg-type]
                if isinstance(rr, str) and rr.strip():
                    return rr.strip()
            except Exception:
                pass
        if hasattr(_regime, "current_regime"):
            try:
                rr = _regime.current_regime()  # type: ignore[call-arg]
                if isinstance(rr, str) and rr.strip():
                    return rr.strip()
            except Exception:
                pass

    return "live"


def _regime_profile(name: str) -> dict[str, float]:
    """
    Regime multipliers:
      - risk_weight: stronger means more risk-averse (bigger AI rug penalty)
      - min_momentum_mult / min_final_mult: threshold multipliers
      - volume_mult: more required flow when risk-off
    """
    n = (name or "").lower()

    if n in {"bull", "risk_on", "momentum", "pump", "up_only"}:
        return {
            "risk_weight": 0.55,
            "min_momentum_mult": 0.85,
            "min_final_mult": 0.80,
            "volume_mult": 0.90,
        }
    if n in {"crab", "chop", "range", "sideways"}:
        return {
            "risk_weight": 0.65,
            "min_momentum_mult": 1.00,
            "min_final_mult": 1.00,
            "volume_mult": 1.00,
        }
    if n in {"bear", "risk_off", "defensive"}:
        return {
            "risk_weight": 0.80,
            "min_momentum_mult": 1.10,
            "min_final_mult": 1.20,
            "volume_mult": 1.10,
        }

    # default live
    return {
        "risk_weight": float(getattr(C, "PF_RISK_PENALTY_WEIGHT", 0.60)),
        "min_momentum_mult": 1.00,
        "min_final_mult": 1.00,
        "volume_mult": 1.00,
    }


# =============================================================================
# Decision dataclasses (internal)
# =============================================================================


@dataclass
class _Scores:
    regime: str
    momentum: float
    final: float
    risk_adjusted_final: float
    ai_rug: float
    impact_bps: float
    smart_score: float
    fdv_to_liq: float
    components: dict[str, float]
    risk_factors: dict[str, float]


# =============================================================================
# AI rug / fraud normalization
# =============================================================================


def _resolve_ai_rug_score(meta: Mapping[str, Any]) -> float:
    """
    Normalize any rug/fraud fields into 0..1:
      - ai_rug_score_01 (preferred)
      - ai_rug_score
      - ai_fraud_score (0..1 or 0..100)
      - rug_score, fraud_score (common aliases)
    """
    raw = _first_present(
        meta, ["ai_rug_score_01", "ai_rug_score", "ai_fraud_score", "rug_score", "fraud_score"]
    )
    if raw is None:
        return 0.0

    f = _to_float(raw, 0.0)
    if f > 1.5:  # treat as 0..100
        f = f / 100.0
    return _clamp(f, 0.0, 1.0)


# =============================================================================
# Normalization / aliases
# =============================================================================


def _normalize_inplace(meta: Meta) -> None:
    """
    Best-effort normalization so scanners can use different field names.
    We only fill missing canonical keys; we do not overwrite existing ones.
    """
    # Liquidity
    if "liq_usd" not in meta:
        meta["liq_usd"] = _get_float(
            meta, ["liq_usd", "liq", "liquidity_usd", "liquidity", "pool_liquidity_usd"], 0.0
        )

    # Market cap / FDV
    if "mcap_usd" not in meta:
        meta["mcap_usd"] = _get_float(
            meta, ["mcap_usd", "market_cap", "market_cap_usd", "mc_usd"], 0.0
        )
    if "fdv_usd" not in meta:
        meta["fdv_usd"] = _get_float(
            meta, ["fdv_usd", "fdv", "fully_diluted_valuation", "fully_diluted_valuation_usd"], 0.0
        )

    # Age
    if "token_age_min" not in meta:
        meta["token_age_min"] = _get_float(
            meta, ["token_age_min", "age_min", "age_minutes", "minutes_since_created"], 9999.0
        )

    # Volume
    if "vol_5m_usd" not in meta:
        meta["vol_5m_usd"] = _get_float(
            meta, ["vol_5m_usd", "vol5", "volume_5m_usd", "volume5m"], 0.0
        )
    if "vol_1h_usd" not in meta:
        meta["vol_1h_usd"] = _get_float(
            meta, ["vol_1h_usd", "vol1h", "volume_1h_usd", "volume1h"], 0.0
        )
    if "vol_24h_usd" not in meta:
        meta["vol_24h_usd"] = _get_float(
            meta, ["vol_24h_usd", "vol24h", "volume_24h_usd", "volume24h"], 0.0
        )

    # Trades
    if "buys_5m" not in meta:
        meta["buys_5m"] = _get_int(meta, ["buys_5m", "buys5", "buy_tx_5m", "buy_count_5m"], 0)
    if "sells_5m" not in meta:
        meta["sells_5m"] = _get_int(meta, ["sells_5m", "sells5", "sell_tx_5m", "sell_count_5m"], 0)

    # Price change pct
    if "price_change_5m_pct" not in meta:
        meta["price_change_5m_pct"] = _get_float(
            meta, ["price_change_5m_pct", "chg5", "change_5m_pct", "priceChange5m"], 0.0
        )
    if "price_change_15m_pct" not in meta:
        meta["price_change_15m_pct"] = _get_float(
            meta, ["price_change_15m_pct", "chg15", "change_15m_pct", "priceChange15m"], 0.0
        )
    if "price_change_1h_pct" not in meta:
        meta["price_change_1h_pct"] = _get_float(
            meta, ["price_change_1h_pct", "chg1h", "change_1h_pct", "priceChange1h"], 0.0
        )

    # Whales
    if "whales_5m" not in meta:
        meta["whales_5m"] = _get_int(meta, ["whales_5m", "whale_count_5m", "whaleCount5m"], 0)
    if "whale_vol_5m_usd" not in meta:
        meta["whale_vol_5m_usd"] = _get_float(
            meta, ["whale_vol_5m_usd", "whale_volume_5m_usd"], 0.0
        )
    if "largest_buy_5m_sol" not in meta:
        meta["largest_buy_5m_sol"] = _get_float(
            meta, ["largest_buy_5m_sol", "largest_buy_sol_5m"], 0.0
        )

    # Holders
    if "holders_5m_change" not in meta:
        meta["holders_5m_change"] = _get_int(
            meta, ["holders_5m_change", "new_holders_5m", "holdersChange5m"], 0
        )

    # Microstructure
    if "impact_1k_bps" not in meta:
        meta["impact_1k_bps"] = _get_float(
            meta, ["impact_1k_bps", "impactBps1k", "impact_bps_1k"], 0.0
        )
    if "smart_wallet_score" not in meta:
        meta["smart_wallet_score"] = _get_float(
            meta, ["smart_wallet_score", "smartMoneyScore", "smart_score"], 0.0
        )

    # Canonicalize dex/source
    if "source" not in meta and "dex" in meta:
        meta["source"] = meta.get("dex")


# =============================================================================
# Scoring core
# =============================================================================


def _compute_scores(meta: Meta) -> _Scores:
    _normalize_inplace(meta)

    regime = _get_regime(meta)
    rprof = _regime_profile(regime)

    # price action (pct)
    p5 = _norm_pct(_to_float(meta.get("price_change_5m_pct"), 0.0))
    p15 = _norm_pct(_to_float(meta.get("price_change_15m_pct"), 0.0))
    p1h = _norm_pct(_to_float(meta.get("price_change_1h_pct"), 0.0))

    # volume / trades
    vol_5m = max(0.0, _to_float(meta.get("vol_5m_usd"), 0.0))
    vol_1h = max(0.0, _to_float(meta.get("vol_1h_usd"), 0.0))
    vol_24h = max(0.0, _to_float(meta.get("vol_24h_usd"), 0.0))
    buys_5m = max(0, _to_int(meta.get("buys_5m"), 0))
    sells_5m = max(0, _to_int(meta.get("sells_5m"), 0))
    trades_5m = max(1, buys_5m + sells_5m)

    buy_ratio = buys_5m / float(trades_5m)
    sell_ratio = sells_5m / float(trades_5m)

    # whales
    whales_5m = max(0, _to_int(meta.get("whales_5m"), 0))
    whale_vol_5m = max(0.0, _to_float(meta.get("whale_vol_5m_usd"), 0.0))
    largest_buy_5m_sol = max(0.0, _to_float(meta.get("largest_buy_5m_sol"), 0.0))

    # age
    age_min = max(0.0, _to_float(meta.get("token_age_min"), 9999.0))

    # holders growth
    holders_change_5m = _to_int(meta.get("holders_5m_change"), 0)

    # risk
    ai_rug = _resolve_ai_rug_score(meta)

    # microstructure
    impact_bps = max(0.0, _to_float(meta.get("impact_1k_bps"), 0.0))

    # valuation
    liq_usd = max(0.0, _to_float(meta.get("liq_usd"), 0.0))
    fdv_usd = max(0.0, _to_float(meta.get("fdv_usd"), 0.0))
    fdv_to_liq = (fdv_usd / liq_usd) if (liq_usd > 0 and fdv_usd > 0) else 0.0

    # smart wallets (0..1)
    smart_score = _clamp(_to_float(meta.get("smart_wallet_score"), 0.0), 0.0, 1.0)

    # ------------------------------------------------------------------
    # Components 0..1
    # ------------------------------------------------------------------

    # Price components
    comp_price_5m = _soft_scale(p5, 5.0, 90.0)
    comp_price_15m = _soft_scale(p15 + 20.0, 0.0, 180.0)
    comp_price_1h = _soft_scale(p1h, -20.0, 250.0)

    # Volume components (regime-aware)
    base_min_5m = float(getattr(C, "PF_SCAN_MIN_5M_VOL_USD", 50.0))
    min_5m = base_min_5m * float(rprof.get("volume_mult", 1.0))

    comp_vol_5m_abs = _soft_scale(vol_5m, min_5m, min_5m * 6.0)
    vol_ratio_5m_1h = vol_5m / max(vol_1h, 1.0)
    comp_vol_ratio_5m_1h = _soft_scale(vol_ratio_5m_1h, 0.10, 0.70)

    vol_ratio_5m_24h = vol_5m / max(vol_24h, 1.0)
    comp_vol_ratio_5m_24h = _soft_scale(vol_ratio_5m_24h, 0.02, 0.40)

    comp_volume = (
        0.55 * comp_vol_5m_abs + 0.30 * comp_vol_ratio_5m_1h + 0.15 * comp_vol_ratio_5m_24h
    )

    # Flow / pressure
    comp_buy_pressure = _soft_scale(buy_ratio, 0.45, 0.80)
    comp_trades = _soft_scale(float(trades_5m), 6.0, 60.0)

    # Whales
    min_whale_size_sol = float(getattr(C, "PF_SCAN_MIN_WHALE_SIZE_SOL", 1.5))
    comp_whale_count = _soft_scale(float(whales_5m), 0.0, 3.0)
    comp_whale_size = _soft_scale(largest_buy_5m_sol, min_whale_size_sol, min_whale_size_sol * 4.0)
    whale_share = whale_vol_5m / max(vol_5m, 1.0)
    comp_whale_share = _soft_scale(whale_share, 0.05, 0.50)
    comp_whales = 0.45 * comp_whale_count + 0.35 * comp_whale_size + 0.20 * comp_whale_share

    # Age: newer better; fade out by max age
    max_age_min = float(getattr(C, "PF_SCAN_MAX_TOKEN_AGE_MIN", 720.0))
    comp_age = 1.0 - _soft_scale(age_min, 5.0, max_age_min)

    # Holder growth
    comp_holders_growth = _soft_scale(float(holders_change_5m), 1.0, 30.0)

    # Smart alpha
    comp_smart = smart_score

    # ------------------------------------------------------------------
    # Combine to momentum
    # ------------------------------------------------------------------
    w_price_5m = float(getattr(C, "PF_W_PRICE_5M", 0.23))
    w_price_15m = float(getattr(C, "PF_W_PRICE_15M", 0.10))
    w_price_1h = float(getattr(C, "PF_W_PRICE_1H", 0.05))
    w_volume = float(getattr(C, "PF_W_VOLUME", 0.20))
    w_buy_pressure = float(getattr(C, "PF_W_BUY_PRESSURE", 0.14))
    w_trades = float(getattr(C, "PF_W_TRADES", 0.06))
    w_whales = float(getattr(C, "PF_W_WHALES", 0.10))
    w_age = float(getattr(C, "PF_W_AGE", 0.07))
    w_holders = float(getattr(C, "PF_W_HOLDERS_GROWTH", 0.03))
    w_smart = float(getattr(C, "PF_W_SMART", 0.02))

    momentum = (
        w_price_5m * comp_price_5m
        + w_price_15m * comp_price_15m
        + w_price_1h * comp_price_1h
        + w_volume * comp_volume
        + w_buy_pressure * comp_buy_pressure
        + w_trades * comp_trades
        + w_whales * comp_whales
        + w_age * comp_age
        + w_holders * comp_holders_growth
        + w_smart * comp_smart
    )
    momentum = _clamp(momentum, 0.0, 1.0)

    # ------------------------------------------------------------------
    # Risk / quality factors
    # ------------------------------------------------------------------
    risk_weight_ai = float(
        rprof.get("risk_weight", float(getattr(C, "PF_RISK_PENALTY_WEIGHT", 0.60)))
    )
    impact_weight = float(getattr(C, "PF_IMPACT_PENALTY_WEIGHT", 0.25))
    smart_risk_boost = float(getattr(C, "PF_SMART_RISK_BOOST", 0.25))
    val_weight = float(getattr(C, "PF_VALUATION_PENALTY_WEIGHT", 0.15))

    # AI rug factor
    ai_penalty = risk_weight_ai * ai_rug
    ai_factor = max(0.0, 1.0 - ai_penalty)

    # Slippage factor (impact)
    max_impact_bps = float(getattr(C, "PF_SCAN_MAX_IMPACT_1K_BPS", 800.0))
    impact_component = _soft_scale(impact_bps, 50.0, max_impact_bps) if max_impact_bps > 0 else 0.0
    slip_factor = max(0.0, 1.0 - impact_weight * impact_component)

    # Valuation stretch factor
    val_component = _soft_scale(fdv_to_liq, 10.0, 40.0) if fdv_to_liq > 0 else 0.0
    val_factor = max(0.0, 1.0 - val_weight * val_component)

    # Smart factor can boost slightly above 1 (bounded)
    smart_factor = 1.0 + smart_risk_boost * smart_score

    # Anti-manipulation penalties (soft; do not dominate)
    pump_dump_pen = float(getattr(C, "PF_PUMP_DUMP_SELL_PRESSURE_PENALTY", 0.18))
    spike_low_vol_pen = float(getattr(C, "PF_SPIKE_LOW_VOL_PENALTY", 0.12))

    # If sell pressure is high while price is spiking, reduce quality.
    sell_pressure_component = _soft_scale(sell_ratio, 0.45, 0.75)
    spike_component = _soft_scale(p5, 60.0, 250.0)
    pump_dump_factor = max(0.0, 1.0 - pump_dump_pen * (sell_pressure_component * spike_component))

    # If huge spike but low volume, likely manipulation.
    low_vol_component = 1.0 - comp_vol_5m_abs
    spike_low_vol_factor = max(0.0, 1.0 - spike_low_vol_pen * (spike_component * low_vol_component))

    risk_factor = (
        ai_factor
        * slip_factor
        * val_factor
        * smart_factor
        * pump_dump_factor
        * spike_low_vol_factor
    )
    risk_factor = _clamp(risk_factor, 0.0, 1.3)

    final = momentum * min(1.0, risk_factor)
    final = _clamp(final, 0.0, 1.0)

    risk_adjusted_final = final * (1.0 - ai_rug)
    risk_adjusted_final = _clamp(risk_adjusted_final, 0.0, 1.0)

    # ------------------------------------------------------------------
    # Side effects: write pf_* and debug
    # ------------------------------------------------------------------
    meta["pf_momentum_score"] = momentum
    meta["pf_final_score"] = final
    meta["pf_risk_adjusted_final"] = risk_adjusted_final
    meta["ai_rug_score_01"] = ai_rug
    meta["pf_fdv_to_liq"] = fdv_to_liq

    comps = {
        "price_5m": comp_price_5m,
        "price_15m": comp_price_15m,
        "price_1h": comp_price_1h,
        "volume": comp_volume,
        "buy_pressure": comp_buy_pressure,
        "trades": comp_trades,
        "whales": comp_whales,
        "age": comp_age,
        "holders_growth": comp_holders_growth,
        "smart": comp_smart,
        "sell_pressure": sell_pressure_component,
        "spike": spike_component,
        "low_vol": low_vol_component,
    }
    rf = {
        "ai_factor": ai_factor,
        "slip_factor": slip_factor,
        "val_factor": val_factor,
        "smart_factor": smart_factor,
        "pump_dump_factor": pump_dump_factor,
        "spike_low_vol_factor": spike_low_vol_factor,
        "risk_factor": risk_factor,
    }

    meta.setdefault("_score_debug", {})
    meta["_score_debug"].update(
        {
            "regime": regime,
            "momentum": momentum,
            "final": final,
            "risk_adjusted_final": risk_adjusted_final,
            "ai_rug_score": ai_rug,
            "impact_1k_bps": impact_bps,
            "smart_wallet_score": smart_score,
            "fdv_to_liq": fdv_to_liq,
            "components": comps,
            "risk_factors": rf,
            "raw": {
                "p5": p5,
                "p15": p15,
                "p1h": p1h,
                "vol_5m": vol_5m,
                "vol_1h": vol_1h,
                "vol_24h": vol_24h,
                "buys_5m": buys_5m,
                "sells_5m": sells_5m,
                "whales_5m": whales_5m,
                "largest_buy_5m_sol": largest_buy_5m_sol,
                "age_min": age_min,
            },
        }
    )

    return _Scores(
        regime=regime,
        momentum=momentum,
        final=final,
        risk_adjusted_final=risk_adjusted_final,
        ai_rug=ai_rug,
        impact_bps=impact_bps,
        smart_score=smart_score,
        fdv_to_liq=fdv_to_liq,
        components=comps,
        risk_factors=rf,
    )


# =============================================================================
# Profile (small descriptive buckets for downstream AI/logging)
# =============================================================================


def _build_profile(meta: Meta) -> dict[str, Any]:
    liq = max(0.0, _to_float(meta.get("liq_usd"), 0.0))
    mcap = max(0.0, _to_float(meta.get("mcap_usd"), 0.0))
    age_min = max(0.0, _to_float(meta.get("token_age_min"), 9999.0))

    momentum = float(meta.get("pf_momentum_score") or 0.0)
    final_score = float(meta.get("pf_final_score") or 0.0)
    ai_rug = _resolve_ai_rug_score(meta)
    smart_score = _clamp(_to_float(meta.get("smart_wallet_score"), 0.0), 0.0, 1.0)
    whales_5m = max(0, _to_int(meta.get("whales_5m"), 0))
    fdv_to_liq = max(0.0, _to_float(meta.get("pf_fdv_to_liq"), 0.0))

    # risk bucket
    if ai_rug >= 0.85 or final_score < 0.12:
        risk_bucket = "red"
    elif ai_rug >= 0.60 or final_score < 0.22:
        risk_bucket = "yellow"
    else:
        risk_bucket = "green"

    # size bucket
    if liq < 50_000 and mcap < 200_000:
        size_bucket = "micro"
    elif liq < 200_000 and mcap < 1_000_000:
        size_bucket = "small"
    elif liq < 1_000_000 and mcap < 5_000_000:
        size_bucket = "mid"
    else:
        size_bucket = "large"

    # style heuristic
    style = "unknown"
    if age_min < 30 and size_bucket in {"micro", "small"}:
        style = "fresh_microcap"
    if whales_5m > 0 and momentum > 0.35:
        style = "whale_chase"
    if size_bucket in {"mid", "large"} and momentum > 0.30 and ai_rug < 0.40:
        style = "midcap_trend"
    if fdv_to_liq > 30 and size_bucket in {"micro", "small"}:
        style = "overdiluted_degen"
    if smart_score > 0.70 and momentum > 0.25 and ai_rug < 0.50:
        style = "smart_money_push"

    if risk_bucket == "red":
        style = "high_risk"

    prof = {"risk_bucket": risk_bucket, "size_bucket": size_bucket, "style": style}
    meta["pf_profile"] = prof
    return prof


# =============================================================================
# Public helpers
# =============================================================================


def enrich_pair_meta(meta: Meta) -> dict[str, Any]:
    """
    Compute scores + profile + debug, but do NOT apply hard filters.
    Useful for FeatureBuilder or logging pipelines that want pf_* fields always.
    """
    s = _compute_scores(meta)
    prof = _build_profile(meta)
    return {
        "regime": s.regime,
        "momentum": s.momentum,
        "final": s.final,
        "risk_adjusted_final": s.risk_adjusted_final,
        "ai_rug_score": s.ai_rug,
        "profile": prof,
    }


def compute_momentum_score(meta: Meta) -> float:
    """Return 0..1 momentum score and write pf_momentum_score into meta."""
    return float(_compute_scores(meta).momentum)


def compute_final_score(meta: Meta) -> float:
    """
    Return 0..1 final score and write:
      pf_momentum_score, pf_final_score, pf_risk_adjusted_final,
      ai_rug_score_01, pf_fdv_to_liq
    """
    return float(_compute_scores(meta).final)


# =============================================================================
# Hard filter
# =============================================================================


def is_viable_pair(meta: Meta) -> bool:
    """
    Hard viability filter (scanner gate). Also enriches meta with pf_* and debug.

    Returns:
      True if passes, False otherwise.

    Side effects:
      - Always computes and writes pf_* fields.
      - Always writes pf_profile, _score_debug, _filter_debug, pf_decision.
    """
    _normalize_inplace(meta)
    s = _compute_scores(meta)
    prof = _build_profile(meta)

    # Pull canonical values
    liq = max(0.0, _to_float(meta.get("liq_usd"), 0.0))
    mcap = max(0.0, _to_float(meta.get("mcap_usd"), 0.0))
    age_min = max(0.0, _to_float(meta.get("token_age_min"), 9999.0))
    vol_5m = max(0.0, _to_float(meta.get("vol_5m_usd"), 0.0))
    buys_5m = max(0, _to_int(meta.get("buys_5m"), 0))
    sells_5m = max(0, _to_int(meta.get("sells_5m"), 0))
    trades_5m = max(1, buys_5m + sells_5m)

    whales_5m = max(0, _to_int(meta.get("whales_5m"), 0))
    largest_buy_5m_sol = max(0.0, _to_float(meta.get("largest_buy_5m_sol"), 0.0))

    is_honeypot = _to_bool(meta.get("is_honeypot"), False)
    trading_paused = _to_bool(meta.get("trading_paused"), False)
    blacklisted = _to_bool(meta.get("blacklisted"), False)

    # Concentration / authorities
    top10_hold_pct = max(0.0, _to_float(meta.get("top10_hold_pct"), 0.0))
    dev_hold_pct = max(0.0, _to_float(meta.get("dev_hold_pct"), 0.0))

    mint_auth_disabled = _to_bool(meta.get("mint_authority_disabled"), True)
    freeze_auth_disabled = _to_bool(meta.get("freeze_authority_disabled"), True)

    # microstructure
    impact_bps = max(0.0, _to_float(meta.get("impact_1k_bps"), 0.0))

    # smart alpha
    smart_score = _clamp(_to_float(meta.get("smart_wallet_score"), 0.0), 0.0, 1.0)

    # regime-aware threshold multipliers
    rprof = _regime_profile(s.regime)

    # Config thresholds
    min_liq = float(getattr(C, "PF_SCAN_MIN_LIQ_USD", 150.0))
    max_liq = float(getattr(C, "PF_SCAN_MAX_LIQ_USD", 0.0))
    min_mcap = float(getattr(C, "PF_SCAN_MIN_MCAP_USD", 500.0))
    max_mcap = float(getattr(C, "PF_SCAN_MAX_MCAP_USD", 0.0))
    max_age = float(getattr(C, "PF_SCAN_MAX_TOKEN_AGE_MIN", 720.0))

    base_min_5m_vol = float(getattr(C, "PF_SCAN_MIN_5M_VOL_USD", 50.0))
    min_5m_vol = base_min_5m_vol * float(rprof.get("volume_mult", 1.0))
    min_5m_buys = int(getattr(C, "PF_SCAN_MIN_5M_BUYS", 1))
    min_buy_sell_ratio = float(getattr(C, "PF_SCAN_MIN_BUY_TO_SELL_RATIO", 1.05))
    min_buy_share = float(getattr(C, "PF_SCAN_MIN_BUY_SHARE", 0.35))

    min_whale_size_sol = float(getattr(C, "PF_SCAN_MIN_WHALE_SIZE_SOL", 1.5))
    min_whale_count_5m = int(getattr(C, "PF_SCAN_MIN_WHALE_COUNT_5M", 0))

    max_rug = float(getattr(C, "PF_MAX_RUG_RISK_SCORE", 0.80))
    base_min_momentum = float(getattr(C, "PF_MIN_MOMENTUM_SCORE", 0.14))
    base_min_final = float(getattr(C, "PF_MIN_FINAL_SCORE", 0.10))

    max_top10 = float(getattr(C, "PF_SCAN_MAX_TOP10_HOLD_PCT", 95.0))
    max_dev = float(getattr(C, "PF_SCAN_MAX_DEV_HOLD_PCT", 60.0))

    require_mint_disabled = bool(int(getattr(C, "PF_REQUIRE_MINT_AUTH_DISABLED", 1)))
    require_freeze_disabled = bool(int(getattr(C, "PF_REQUIRE_FREEZE_AUTH_DISABLED", 1)))

    max_impact = float(getattr(C, "PF_SCAN_MAX_IMPACT_1K_BPS", 800.0))
    min_smart = float(getattr(C, "PF_MIN_SMART_SCORE", 0.0))

    # regime-aware gates
    min_momentum = base_min_momentum * float(rprof.get("min_momentum_mult", 1.0))
    min_final = base_min_final * float(rprof.get("min_final_mult", 1.0))

    # Derived flow stats
    buy_sell_ratio = buys_5m / float(max(1, sells_5m))
    buy_share = buys_5m / float(trades_5m)

    reasons: list[str] = []

    # Size window
    if liq < min_liq:
        reasons.append(f"liq {liq:.0f} < {min_liq:.0f}")
    if max_liq > 0 and liq > max_liq:
        reasons.append(f"liq {liq:.0f} > {max_liq:.0f}")

    if mcap < min_mcap:
        reasons.append(f"mcap {mcap:.0f} < {min_mcap:.0f}")
    if max_mcap > 0 and mcap > max_mcap:
        reasons.append(f"mcap {mcap:.0f} > {max_mcap:.0f}")

    # Age
    if age_min > max_age:
        reasons.append(f"age {age_min:.1f}m > {max_age:.1f}m")

    # Safety flags
    if is_honeypot:
        reasons.append("honeypot")
    if trading_paused:
        reasons.append("paused")
    if blacklisted:
        reasons.append("blacklisted")

    # Centralization / authority guards (hard)
    if top10_hold_pct > max_top10:
        reasons.append(f"top10 {top10_hold_pct:.1f}% > {max_top10:.1f}%")
    if dev_hold_pct > max_dev:
        reasons.append(f"dev {dev_hold_pct:.1f}% > {max_dev:.1f}%")
    if require_mint_disabled and (not mint_auth_disabled):
        reasons.append("mint_auth_enabled")
    if require_freeze_disabled and (not freeze_auth_disabled):
        reasons.append("freeze_auth_enabled")

    # Activity
    if vol_5m < min_5m_vol:
        reasons.append(f"vol5 {vol_5m:.0f} < {min_5m_vol:.0f}")
    if buys_5m < min_5m_buys:
        reasons.append(f"buys5 {buys_5m} < {min_5m_buys}")

    # Only enforce buy/sell quality if there is at least some trading
    if trades_5m >= 3 and sells_5m > 0:
        if buy_sell_ratio < min_buy_sell_ratio:
            reasons.append(f"bsr {buy_sell_ratio:.2f} < {min_buy_sell_ratio:.2f}")
        if buy_share < min_buy_share:
            reasons.append(f"buy_share {buy_share:.2f} < {min_buy_share:.2f}")

    # Whale requirement (only if configured)
    if (min_whale_count_5m > 0) or (min_whale_size_sol > 0):
        if whales_5m < min_whale_count_5m and largest_buy_5m_sol < min_whale_size_sol:
            reasons.append("no_whales")

    # Microstructure
    if max_impact > 0 and impact_bps > max_impact:
        reasons.append(f"impact {impact_bps:.0f}bps > {max_impact:.0f}bps")

    # Smart min (optional)
    if min_smart > 0.0 and smart_score < min_smart:
        reasons.append(f"smart {smart_score:.2f} < {min_smart:.2f}")

    # AI rug cap
    if s.ai_rug > max_rug:
        reasons.append(f"ai_rug {s.ai_rug:.2f} > {max_rug:.2f}")

    # Score gates
    if s.final < min_final:
        reasons.append(f"final {s.final:.2f} < {min_final:.2f}")
    elif s.momentum < (min_momentum * 0.60):
        reasons.append(f"dead_momo {s.momentum:.2f} << {min_momentum:.2f}")

    ok = len(reasons) == 0

    # Debug payload
    filt_dbg = meta.setdefault("_filter_debug", {})
    filt_dbg.update(
        {
            "ok": ok,
            "reasons": reasons,
            "regime": s.regime,
            "momentum_score": s.momentum,
            "final_score": s.final,
            "risk_adjusted_final": s.risk_adjusted_final,
            "ai_rug_score": s.ai_rug,
            "liq_usd": liq,
            "mcap_usd": mcap,
            "age_min": age_min,
            "vol_5m_usd": vol_5m,
            "buys_5m": buys_5m,
            "sells_5m": sells_5m,
            "buy_sell_ratio": buy_sell_ratio,
            "buy_share": buy_share,
            "whales_5m": whales_5m,
            "largest_buy_5m_sol": largest_buy_5m_sol,
            "impact_1k_bps": impact_bps,
            "smart_wallet_score": smart_score,
            "profile": prof,
            "thresholds": {
                "min_liq": min_liq,
                "max_liq": max_liq,
                "min_mcap": min_mcap,
                "max_mcap": max_mcap,
                "max_age_min": max_age,
                "min_5m_vol": min_5m_vol,
                "min_5m_buys": min_5m_buys,
                "min_buy_sell_ratio": min_buy_sell_ratio,
                "min_buy_share": min_buy_share,
                "min_final": min_final,
                "min_momentum": min_momentum,
                "max_rug": max_rug,
                "max_impact_bps": max_impact,
            },
        }
    )

    # Canonical "decision" blob (useful everywhere)
    meta["pf_decision"] = {
        "ok": ok,
        "reasons": reasons,
        "regime": s.regime,
        "momentum": s.momentum,
        "final": s.final,
        "risk_adjusted_final": s.risk_adjusted_final,
        "ai_rug_score": s.ai_rug,
        "profile": prof,
    }

    if not ok:
        mint = meta.get("mint")
        sym = meta.get("symbol")
        dex = meta.get("dex") or meta.get("source")
        msg = (
            f"[FILTER-REJECT] mint={mint} sym={sym} dex={dex} regime={s.regime} "
            f"reasons={';'.join(reasons)} momo={s.momentum:.2f} final={s.final:.2f}"
        )
        throttle_s = float(getattr(C, "PF_REJECT_LOG_THROTTLE_S", 60))
        _log_throttled(f"pair_filters.reject.{mint or sym or 'unknown'}", throttle_s, "debug", msg)

    return ok


def evaluate_pair(meta: Meta) -> dict[str, Any]:
    """
    Rich wrapper around is_viable_pair(). Always enriches meta fully.
    """
    ok = is_viable_pair(meta)
    filt_dbg = meta.get("_filter_debug") or {}
    profile = meta.get("pf_profile") or filt_dbg.get("profile") or {}
    return {
        "ok": ok,
        "momentum": float(meta.get("pf_momentum_score") or 0.0),
        "final": float(meta.get("pf_final_score") or 0.0),
        "risk_adjusted_final": float(meta.get("pf_risk_adjusted_final") or 0.0),
        "ai_rug_score": float(meta.get("ai_rug_score_01") or 0.0),
        "regime": _get_regime(meta),
        "reasons": list(filt_dbg.get("reasons") or []),
        "profile": profile,
    }
