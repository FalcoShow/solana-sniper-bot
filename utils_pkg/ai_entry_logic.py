from __future__ import annotations

"""
utils_pkg/ai_entry_logic.py

Entry IQ engine (V3+++ / quant-grade).
======================================

Purpose
-------
Given a *feature dict* (FeatureBuilder output + PairFilter meta + session context),
this module decides:

    • A continuous entry score in [0, 1]          → score_entry()
    • Whether we should actually enter            → should_enter()
    • A short human-readable reason               → for logs / debugging

Design
------
1) Deterministic heuristic baseline using:
     - Multi-horizon momentum   (5s/15s/60s/300s + 1m/5m/15m/1h)
     - Volatility regime        (dead vs sweet vs chaotic)
     - Orderflow / microstruct  (buys, imbalance, intensity, vol/liq)
     - Liquidity & mcap         (buckets + smooth log scales)
     - Lifecycle / age          (very-new / newish / late / ancient)
     - Smart wallets & whales   (alignment, breadth, crowd vs whale share)
     - Global regime            (trend/liquidity/manipulation risk)
     - AI fraud / rug / impact  (max penalties + hard veto)
     - PairFilter scores        (momentum/final/risk-adjusted)
     - Optional ai_buy_scoring  (ai_buy_score / ai_buy_ok_hard)
     - FeatureBuilder composites:
         * entry_quality_score
         * position_size_scalar
         * entry_micro_alpha
         * structural_quality_score
         * is_dead_like
         * entry_risk_score
         * risk_capacity_score
         * entry_tier_bucket
         * position_size_bucket
     - Session / RiskManager context:
         * open_positions / max_positions
         * daily_drawdown_pct / realized_pnl_pct
         * rolling_win_rate_pct / recent_loss_streak
         * risk_mode / risk_capacity
         * hard_halt / plan_gap_pct / plan_status (via risk API)

2) Optional fine-tuned OpenAI model:
     - Produces a 0..1 probability of “good trade”
     - Blended with heuristic using ENTRY_AI_WEIGHT
     - Multi-model routing via ENTRY_MODEL_ROUTING_JSON and hints:
           features.entry_head / segment / regime_label
     - Payload auto-trimmed to ENTRY_MAX_FEATURE_BYTES with a curated key set

3) Per-source & regime-aware gating:
     - Global floor (ENTRY_MIN_SCORE) plus per-source overrides
     - Dynamic floor auto-tuning via ai_maintenance_loop()
     - Strategy-driven floor via risk API (ctx_entry_score_gate)
     - Regime tightening in hard risk-off / high-manip regimes
     - Optional regime_risk_mode_lock (0=risk_off,1=normal,2=risk_on)
     - Session drawdown / exposure / capacity locks
     - Risk-profile aware tweaks (conservative / balanced / aggressive)
     - Hard vetoes from fraud / entry_block_flag / hard_halt

4) Rich meta attachment (if ENTRY_ATTACH_META=1):
     - features["entry_meta"] = {
           heuristic_score, ai_score, blended_score,
           buy_score, effective_threshold, base_threshold,
           risk_profile, regime_risk_mode, reason, pass,
           flags (risk_off_tightening, manip_tightening,
                  dd_lock, target_lock, fraud_lock, pos_full,
                  streak_lock, ctx_lock, dynamic_floor,
                  hard_halt, capacity_zero, ...)
           breakdown: {
               pf, buy, mom, vol, flow, liq, age, wallet,
               structure, quality, regime, impact_penalty,
               rug_penalty, ctx, scanner_hint
           }
       }

Public API
----------
  async score_entry(features) -> float
  async should_enter(features) -> (bool ok, float score, str reason)
  score_entry_sync(features) -> float    (for scripts/tests)

Internal API used by backtests
------------------------------
  _heuristic_score(features: dict) -> float
  _ai_score(features: dict) -> Optional[float]
  ai_maintenance_loop(stop_evt: asyncio.Event) -> None

Diagnostics (for test_stack AI_ENTRY_LAYER)
-------------------------------------------
  async diag_snapshot() -> dict
      – returns static config + dynamic floor + optional risk/regime snapshots
"""

import asyncio
import json
import math
import os
from typing import Any, Dict, Optional, Tuple

from logging_setup import get_logger

log = get_logger(__name__)


# ============================================================================ #
# Env helpers
# ============================================================================ #

def _env_bool(key: str, default: bool = False) -> bool:
    v = os.getenv(key)
    if v is None:
        return default
    return str(v).strip().lower() in ("1", "true", "yes", "y", "on")


def _env_float(key: str, default: float) -> float:
    raw = os.getenv(key)
    if raw is None or raw == "":
        return float(default)
    try:
        return float(raw)
    except Exception:
        return float(default)


def _env_int(key: str, default: int) -> int:
    raw = os.getenv(key)
    if raw is None or raw == "":
        return int(default)
    try:
        return int(raw)
    except Exception:
        return int(default)


def _env_json(key: str) -> Optional[dict]:
    raw = os.getenv(key)
    if not raw:
        return None
    try:
        return json.loads(raw)
    except Exception:
        log.warning("ai_entry_logic: could not parse JSON from %s", key)
        return None


def _clip01(x: float) -> float:
    return max(0.0, min(1.0, float(x)))


def _log1p(x: float) -> float:
    if x <= 0:
        return 0.0
    return math.log1p(x)


# ============================================================================ #
# Core config (entry-level knobs)
# ============================================================================ #

# Fine-tuned entry model (if any)
ENTRY_MODEL = (
    os.getenv("ENTRY_MODEL_NAME", "").strip()
    or os.getenv("ENTRY_MODEL", "").strip()
)

# Optional multi-model routing:
#   ENTRY_MODEL_ROUTING_JSON = '{"default": "ft:main", "pumpfun": "ft:pump"}'
ENTRY_MODEL_ROUTING = _env_json("ENTRY_MODEL_ROUTING_JSON") or {}
ENTRY_DEFAULT_HEAD = os.getenv("ENTRY_DEFAULT_HEAD", "default").strip() or "default"

# Global AI toggle: if False we never hit OpenAI (heuristic only)
ENTRY_AI_ENABLED = _env_bool("ENTRY_AI_ENABLED", True)

# Blend weight for AI vs heuristic (0..1)
ENTRY_AI_WEIGHT = max(0.0, min(1.0, _env_float("ENTRY_AI_WEIGHT", 0.7)))

# Minimum blended score required to enter (global floor; per-source can override upwards)
ENTRY_MIN_SCORE = max(0.0, min(1.0, _env_float("ENTRY_MIN_SCORE", 0.55)))

# Dynamic floor / auto-calibration
ENTRY_DYNAMIC_MIN_ENABLED = _env_bool("ENTRY_DYNAMIC_MIN_ENABLED", True)
ENTRY_DYNAMIC_MIN_LO = max(0.0, min(1.0, _env_float("ENTRY_DYNAMIC_MIN_LO", 0.48)))
ENTRY_DYNAMIC_MIN_HI = max(0.0, min(1.0, _env_float("ENTRY_DYNAMIC_MIN_HI", 0.72)))
ENTRY_DYNAMIC_STEP = max(0.001, min(0.05, _env_float("ENTRY_DYNAMIC_STEP", 0.01)))

# Optional minimum AI-only score (0 disables)
ENTRY_MIN_AI_SCORE = max(0.0, min(1.0, _env_float("ENTRY_MIN_AI_SCORE", 0.0)))

# If True, raise on OpenAI failure. If False, fail-soft to heuristic.
ENTRY_AI_STRICT = _env_bool("ENTRY_AI_STRICT", False)

# Optional max token age in minutes for entries (0 = disabled here)
ENTRY_MAX_AGE_MIN = max(0.0, _env_float("ENTRY_MAX_AGE_MIN", 0.0))

# Optional min liquidity (USD) to consider entries at all
ENTRY_MIN_LIQ_USD = max(0.0, _env_float("ENTRY_MIN_LIQ_USD", 10_000.0))

# Hard fraud/rug-score cutoff (0..1)
ENTRY_MAX_RUG_SCORE = max(0.0, min(1.0, _env_float("ENTRY_MAX_RUG_SCORE", 0.80)))

# Use PairFilter scores as priors?
ENTRY_USE_PF_SCORES = _env_bool("ENTRY_USE_PF_SCORES", True)

# PF minimums, only applied if ENTRY_USE_PF_SCORES and scores present
ENTRY_MIN_PF_FINAL = max(0.0, min(1.0, _env_float("ENTRY_MIN_PF_FINAL", 0.10)))
ENTRY_MIN_PF_RISK_ADJ = max(0.0, min(1.0, _env_float("ENTRY_MIN_PF_RISK_ADJ", 0.08)))

# Extra “dead coin” guards on top of PF
ENTRY_MIN_5M_BUYS = max(0, _env_int("ENTRY_MIN_5M_BUYS", 0))
ENTRY_MIN_5M_VOL_USD = max(0.0, _env_float("ENTRY_MIN_5M_VOL_USD", 0.0))

# Regime tightening knobs
ENTRY_REGIME_TIGHTEN_ENABLED = _env_bool("ENTRY_REGIME_TIGHTEN_ENABLED", True)
ENTRY_RISK_OFF_EXTRA_MARGIN = max(
    0.0, min(0.3, _env_float("ENTRY_RISK_OFF_EXTRA_MARGIN", 0.10))
)
ENTRY_MANIP_HIGH = max(0.0, min(1.0, _env_float("ENTRY_MANIP_HIGH", 0.75)))

# Smart-wallet & entry-micro usage
ENTRY_SMART_WALLET_BOOST = _env_bool("ENTRY_SMART_WALLET_BOOST", True)
ENTRY_MIN_ENTRY_MICRO = max(0.0, min(1.5, _env_float("ENTRY_MIN_ENTRY_MICRO", 0.0)))
ENTRY_MIN_STRUCTURAL_QUALITY = max(
    0.0, min(1.5, _env_float("ENTRY_MIN_STRUCTURAL_QUALITY", 0.0))
)
ENTRY_FORBID_DEADLIKE = _env_bool("ENTRY_FORBID_DEADLIKE", False)

# Pump-chasing penalty scaling
ENTRY_PUMP_CHASE_MAX_PENALTY = max(
    0.0, min(0.7, _env_float("ENTRY_PUMP_CHASE_MAX_PENALTY", 0.35))
)

# Session / RiskManager integration
ENTRY_CTX_MAX_DRAWDOWN_PCT = abs(_env_float("ENTRY_CTX_MAX_DRAWDOWN_PCT", 12.0))
ENTRY_CTX_MAX_OPEN_POS_RATIO = max(
    0.1, min(2.0, _env_float("ENTRY_CTX_MAX_OPEN_POS_RATIO", 1.0))
)
ENTRY_CTX_LOCK_AFTER_TARGET_PNL = _env_float(
    "ENTRY_CTX_LOCK_AFTER_TARGET_PNL", 0.0
)  # e.g. 5.0 = +5% day lock

# Require some risk capacity (from FeatureBuilder or RiskManager) to enter?
ENTRY_REQUIRE_RISK_CAPACITY = _env_bool("ENTRY_REQUIRE_RISK_CAPACITY", False)
ENTRY_MIN_RISK_CAPACITY = max(
    0.0, min(1.0, _env_float("ENTRY_MIN_RISK_CAPACITY", 0.20))
)

# Use FeatureBuilder entry_quality_score + position_size_scalar?
ENTRY_USE_ENTRY_QUALITY = _env_bool("ENTRY_USE_ENTRY_QUALITY", True)
ENTRY_MIN_ENTRY_QUALITY = max(
    0.0, min(1.5, _env_float("ENTRY_MIN_ENTRY_QUALITY", 0.0))
)
ENTRY_REQUIRE_POS_SIZE = _env_bool("ENTRY_REQUIRE_POS_SIZE", False)
ENTRY_MIN_POS_SIZE = max(0.0, min(1.0, _env_float("ENTRY_MIN_POS_SIZE", 0.0)))

# Optional prior from ai_buy_scoring
ENTRY_USE_BUY_SCORE = _env_bool("ENTRY_USE_BUY_SCORE", True)
ENTRY_MIN_BUY_SCORE = max(0.0, min(1.0, _env_float("ENTRY_MIN_BUY_SCORE", 0.0)))
ENTRY_BUY_SCORE_WEIGHT = max(0.0, min(1.0, _env_float("ENTRY_BUY_SCORE_WEIGHT", 0.20)))
ENTRY_BUY_HARD_FAIL = _env_bool("ENTRY_BUY_HARD_FAIL", False)

# Per-source threshold & score nudges (e.g. pumpfun tighter, momo looser)
# Example:
#   ENTRY_SOURCE_SCORE_OVERRIDES_JSON='{"pumpfun":0.62,"raydium":0.55,"momo":0.50}'
#   ENTRY_SOURCE_SCORE_BONUS_JSON='{"momo":0.02}'
ENTRY_SOURCE_SCORE_OVERRIDES = _env_json("ENTRY_SOURCE_SCORE_OVERRIDES_JSON") or {}
ENTRY_SOURCE_SCORE_BONUS = _env_json("ENTRY_SOURCE_SCORE_BONUS_JSON") or {}

# AI call control
ENTRY_AI_TIMEOUT_SEC = max(1.0, _env_float("ENTRY_AI_TIMEOUT_SEC", 8.0))
ENTRY_MAX_FEATURE_BYTES = _env_int("ENTRY_MAX_FEATURE_BYTES", 50_000)

# Extra debugging / meta
ENTRY_DEBUG_LOG = _env_bool("ENTRY_DEBUG_LOG", False)
ENTRY_DEBUG_LOG_FAIL = _env_bool("ENTRY_DEBUG_LOG_FAIL", True)
ENTRY_ATTACH_META = _env_bool("ENTRY_ATTACH_META", True)

# Risk profile (higher-level style): conservative / balanced / aggressive
RISK_PROFILE = (
    os.getenv("ENTRY_RISK_PROFILE", "")
    or os.getenv("RISK_PROFILE", "")
).strip().lower() or "balanced"

# Optional regime risk-mode lock from env
ENTRY_REGIME_RISK_MODE_LOCK = _env_int("ENTRY_REGIME_RISK_MODE_LOCK", -1)

# Dynamic min-score runtime value (updated by ai_maintenance_loop)
_ENTRY_DYNAMIC_MIN_SCORE: Optional[float] = None


# ============================================================================ #
# OpenAI client shim (new responses API + legacy ChatCompletion)
# ============================================================================ #

_CLIENT: Any = None
_USE_RESPONSES_API = False


def _ensure_client() -> Optional[Any]:
    """
    Try to import either the new `OpenAI` client or legacy `openai` module.
    Returns a client object or None if unavailable.
    """
    global _CLIENT, _USE_RESPONSES_API

    if _CLIENT is not None:
        return _CLIENT

    # New-style client
    try:
        from openai import OpenAI  # type: ignore

        _CLIENT = OpenAI()
        _USE_RESPONSES_API = True
        log.info("ai_entry_logic: using OpenAI responses client")
        return _CLIENT
    except Exception:
        pass

    # Legacy client
    try:
        import openai  # type: ignore

        if not getattr(openai, "api_key", None):
            openai.api_key = os.getenv("OPENAI_API_KEY", "")
        _CLIENT = openai
        _USE_RESPONSES_API = False
        log.info("ai_entry_logic: using legacy openai.ChatCompletion client")
        return _CLIENT
    except Exception:
        log.warning("ai_entry_logic: no OpenAI client available; AI disabled")
        _CLIENT = None
        return None


def _pick_entry_model(features: Dict[str, Any]) -> Optional[str]:
    """
    Decide which fine-tuned model to use.

    Priority:
      1) features["entry_model_name"]
      2) routing map ENTRY_MODEL_ROUTING_JSON using:
           - features["entry_head"]
           - features["segment"]
           - features["regime_label"]
      3) global ENTRY_MODEL
    """
    # Explicit override from features
    explicit = str(features.get("entry_model_name") or "").strip()
    if explicit:
        return explicit

    # If no routing configured, use global default
    if not ENTRY_MODEL_ROUTING:
        return ENTRY_MODEL or None

    head = (
        str(features.get("entry_head") or "").strip()
        or str(features.get("segment") or "").strip()
        or str(features.get("regime_label") or "").strip()
        or ENTRY_DEFAULT_HEAD
    )

    if head in ENTRY_MODEL_ROUTING:
        return str(ENTRY_MODEL_ROUTING[head])

    # Fallback to "default" if defined
    if "default" in ENTRY_MODEL_ROUTING:
        return str(ENTRY_MODEL_ROUTING["default"])

    # Last resort: global model
    return ENTRY_MODEL or None


# ============================================================================ #
# Feature helpers
# ============================================================================ #

def _f(features: Dict[str, Any], key: str, default: float = 0.0) -> float:
    try:
        v = features.get(key, default)
        return float(v)
    except Exception:
        return float(default)


def _i(features: Dict[str, Any], key: str, default: int = 0) -> int:
    try:
        v = features.get(key, default)
        return int(v)
    except Exception:
        return int(default)


def _source_key(features: Dict[str, Any]) -> str:
    """
    Normalize the "source" of this candidate:
      - scanner_source (string) > source (string) > segment (string) > scanner_source_id (numeric)
    """
    src = (
        features.get("scanner_source")
        or features.get("source")
        or features.get("segment")
        or ""
    )
    if isinstance(src, (int, float)):
        return f"id_{int(src)}"
    return str(src).strip().lower()


def _global_min_threshold() -> float:
    """
    Global min-score floor, possibly dynamically tuned by ai_maintenance_loop().
    """
    if not ENTRY_DYNAMIC_MIN_ENABLED:
        return ENTRY_MIN_SCORE
    if _ENTRY_DYNAMIC_MIN_SCORE is None:
        return ENTRY_MIN_SCORE
    return float(max(0.0, min(1.0, _ENTRY_DYNAMIC_MIN_SCORE)))


def _source_min_threshold(features: Dict[str, Any]) -> float:
    """
    Per-source min score threshold, falling back to global dynamic floor.
    """
    base = _global_min_threshold()
    key = _source_key(features)
    if not key:
        return base
    try:
        if key in ENTRY_SOURCE_SCORE_OVERRIDES:
            return max(
                base,
                min(1.0, float(ENTRY_SOURCE_SCORE_OVERRIDES[key])),
            )
    except Exception:
        pass
    return base


def _source_score_bonus(features: Dict[str, Any]) -> float:
    """
    Optional per-source additive score nudge (tiny).
    """
    key = _source_key(features)
    if not key:
        return 0.0
    try:
        v = ENTRY_SOURCE_SCORE_BONUS.get(key)
        return float(v) if v is not None else 0.0
    except Exception:
        return 0.0


def _effective_risk_profile(features: Dict[str, Any]) -> str:
    """
    Decide the effective risk profile for this decision.

    Order of precedence:
      1) features["risk_profile"]      ← can be injected by risk API
      2) global env RISK_PROFILE / ENTRY_RISK_PROFILE
    """
    rp = str(features.get("risk_profile") or "").strip().lower()
    if rp in ("conservative", "balanced", "aggressive"):
        return rp
    return RISK_PROFILE


def _risk_profile_threshold_adjust(base: float, features: Dict[str, Any]) -> float:
    """
    Adjust threshold up/down for conservative vs aggressive profiles.
    """
    rp = _effective_risk_profile(features)
    adj = 0.0
    if rp == "conservative":
        adj = 0.03
    elif rp == "aggressive":
        adj = -0.03
    return _clip01(base + adj)


def _effective_regime_mode(features: Dict[str, Any]) -> int:
    """
    Regime risk mode lock:

      0 = risk_off  (tightest)
      1 = normal
      2 = risk_on   (loosest)

    Priority:
      1) features["regime_risk_mode_lock"]
      2) env ENTRY_REGIME_RISK_MODE_LOCK
      3) inferred from ctx_risk_mode if present (>=2 → risk_on, <=0 → risk_off)
      4) -1 for "not locked" (caller uses raw regime signals)
    """
    # explicit per-candidate override
    mode_feat = features.get("regime_risk_mode_lock")
    try:
        if mode_feat is not None:
            m = int(mode_feat)
            if m in (-1, 0, 1, 2):
                return m
    except Exception:
        pass

    # env-driven
    if ENTRY_REGIME_RISK_MODE_LOCK in (0, 1, 2):
        return ENTRY_REGIME_RISK_MODE_LOCK

    # infer from ctx_risk_mode if present
    try:
        ctx_rm = _f(features, "ctx_risk_mode", _f(features, "risk_mode_raw", 1.0))
        if ctx_rm <= 0.0:
            return 0
        if ctx_rm >= 2.0:
            return 2
    except Exception:
        pass

    return -1


# ============================================================================ #
# Heuristic scoring (deterministic)
# ============================================================================ #

def _heuristic_score(features: Dict[str, Any]) -> float:
    """
    Deterministic baseline tuned to FeatureBuilder V3+++ FeatureVector.

    Uses:
      - PairFilter scores
      - FeatureBuilder composites:
           * entry_quality_score
           * position_size_scalar
           * entry_micro_alpha
           * structural_quality_score
           * entry_block_flag / is_dead_like
           * entry_risk_score
           * risk_capacity_score
           * entry_tier_bucket / position_size_bucket
      - Multi-horizon momentum
      - Volatility
      - Orderflow & microstructure
      - Liquidity & mcap
      - Smart wallets & whales
      - Global regime (trend/liquidity/manip)
      - AI fraud / rug / impact
      - Session context (open positions, DD, risk mode, capacity, win rate/streak)
      - Optional prior from ai_buy_scoring
    """

    # ------------------------------------------------------------------ #
    # PairFilter scores (if present) – strong prior
    # ------------------------------------------------------------------ #
    pf_mom = _f(features, "pf_momentum_score", 0.0)
    pf_final = _f(features, "pf_final_score", 0.0)
    pf_risk_adj = _f(features, "pf_risk_adjusted_final", pf_final)

    pf_component = 0.0
    if ENTRY_USE_PF_SCORES:
        pf_component = 0.55 * pf_risk_adj + 0.25 * pf_final + 0.15 * pf_mom

    # ------------------------------------------------------------------ #
    # Optional ai_buy_scoring prior
    # ------------------------------------------------------------------ #
    buy_component = 0.0
    if ENTRY_USE_BUY_SCORE:
        buy_score = _f(features, "ai_buy_score", 0.0)
        buy_ok_hard = bool(
            features.get("ai_buy_ok_hard")
            or features.get("ai_buy_pass_gate")
        )
        if buy_score > 0.0 and (not ENTRY_MIN_BUY_SCORE or buy_score >= ENTRY_MIN_BUY_SCORE):
            buy_component = ENTRY_BUY_SCORE_WEIGHT * buy_score
        elif ENTRY_BUY_HARD_FAIL and buy_score > 0.0 and not buy_ok_hard:
            buy_component -= 1.2  # strong negative prior

    # ------------------------------------------------------------------ #
    # Momentum: multi-horizon + legacy fallback
    # ------------------------------------------------------------------ #
    mom_5s = _f(features, "mom_5s_pct", 0.0)
    mom_15s = _f(features, "mom_15s_pct", 0.0)
    mom_60s = _f(features, "mom_60s_pct", 0.0)
    mom_300s = _f(features, "mom_300s_pct", 0.0)

    if mom_300s == 0.0:
        mom_300s = _f(
            features,
            "price_change_5m_pct",
            _f(features, "chg5", 0.0),
        )

    chg_1m = _f(features, "price_change_1m_pct", 0.0)
    chg_5m = _f(features, "price_change_5m_pct", chg_1m)
    chg_15m = _f(features, "price_change_15m_pct", 0.0)
    chg_1h = _f(features, "price_change_1h_pct", 0.0)

    # Clamp extremes
    mom_5s = max(-40.0, min(80.0, mom_5s))
    mom_15s = max(-60.0, min(120.0, mom_15s))
    mom_60s = max(-80.0, min(180.0, mom_60s))
    mom_300s = max(-80.0, min(220.0, mom_300s))

    chg_1m = max(-60.0, min(80.0, chg_1m))
    chg_5m = max(-60.0, min(120.0, chg_5m))
    chg_15m = max(-80.0, min(180.0, chg_15m))
    chg_1h = max(-90.0, min(250.0, chg_1h))

    mom_score = 0.0
    # Micro / short-term
    mom_score += max(0.0, mom_5s) * 0.010
    mom_score += max(0.0, mom_15s) * 0.006
    mom_score += max(0.0, mom_60s) * 0.003
    mom_score += max(0.0, mom_300s) * 0.002
    # TF stacking
    mom_score += max(0.0, chg_1m) * 0.006
    mom_score += max(0.0, chg_5m) * 0.004
    mom_score += max(0.0, chg_15m) * 0.003
    mom_score += max(0.0, chg_1h) * 0.001

    # Mean-reversion after big pump
    if chg_15m < 0.0 and chg_5m > 25.0:
        mom_score -= 0.18
    if mom_300s > 40.0 and mom_15s < 0.0:
        mom_score -= 0.15

    # Explicit pump-chasing flag from FeatureBuilder
    pump_chase = _f(features, "is_pump_chasing_risk", 0.0)
    if pump_chase > 0.0:
        mom_score -= ENTRY_PUMP_CHASE_MAX_PENALTY * pump_chase

    # ------------------------------------------------------------------ #
    # Volatility windows
    # ------------------------------------------------------------------ #
    vol_15s = _f(features, "vol_15s", 0.0)
    vol_60s = _f(features, "vol_60s", 0.0)
    vol_300s = _f(features, "vol_300s", 0.0)

    vol15_pct = vol_15s * 100.0
    vol60_pct = vol_60s * 100.0
    vol300_pct = vol_300s * 100.0

    vol_score = 0.0
    for v in (vol15_pct, vol60_pct, vol300_pct):
        if v <= 0.2:
            vol_score -= 0.12  # dead
        elif 0.3 <= v <= 3.0:
            vol_score += 0.12  # sweet
        elif 3.0 < v <= 8.0:
            vol_score += 0.04  # spicy but okay
        elif v > 12.0:
            vol_score -= 0.18  # chaos

    # ------------------------------------------------------------------ #
    # Orderflow & microstructure
    # ------------------------------------------------------------------ #
    buys_5m = _i(features, "buys_5m", 0)
    sells_5m = _i(features, "sells_5m", 0)
    trades_5m = _f(features, "txns_5m", buys_5m + sells_5m)
    imbalance_5m = _f(features, "orderflow_skew_5m", 0.0)  # -1..1
    buy_ratio_5m = _f(features, "buy_ratio_5m", 0.5)      # 0..1
    buy_ratio_1m = _f(features, "buy_ratio_1m", 0.5)

    buy_intensity_5m = _f(features, "buy_intensity_5m", 0.0)
    sell_intensity_5m = _f(features, "sell_intensity_5m", 0.0)

    vol_5m_usd = _f(features, "vol_5m_usd", 0.0)
    vol_ratio_5m_24h = _f(features, "vol_ratio_5m_24h", 0.0)
    vol_per_tx_5m = _f(features, "vol_per_tx_5m_usd", 0.0)

    flow_score = 0.0
    flow_score += min(buys_5m, 60) * 0.007
    flow_score += min(trades_5m, 120) * 0.003
    flow_score += max(0.0, min(imbalance_5m, 1.0)) * 0.25
    flow_score += (buy_ratio_5m - 0.5) * 0.6
    flow_score += (buy_ratio_1m - 0.5) * 0.3

    flow_score += min(buy_intensity_5m * 300.0, 3.0) * 0.08
    flow_score -= min(sell_intensity_5m * 300.0, 3.0) * 0.05

    flow_score += min(vol_5m_usd / 5_000.0, 5.0) * 0.015
    flow_score += min(vol_per_tx_5m / 500.0, 4.0) * 0.02
    flow_score += min(vol_ratio_5m_24h, 0.4) * 0.25

    # ------------------------------------------------------------------ #
    # Liquidity / Market cap / buckets
    # ------------------------------------------------------------------ #
    liq = _f(features, "liq_usd", 0.0)
    log_liq = _log1p(liq)
    mcap = _f(features, "mcap_usd", 0.0)
    log_mcap = _log1p(mcap)

    liq_score = 0.0
    # Bucket nudges
    liq_score += _f(features, "liq_bucket_ultra_low", 0.0) * -0.4
    liq_score += _f(features, "liq_bucket_low", 0.0) * -0.06
    liq_score += _f(features, "liq_bucket_sweet", 0.0) * 0.22
    liq_score += _f(features, "liq_bucket_mid", 0.0) * 0.16
    liq_score += _f(features, "liq_bucket_high", 0.0) * 0.08

    # Fallback if buckets missing
    if (
        _f(features, "liq_bucket_ultra_low", -1.0) < 0
        and _f(features, "liq_bucket_low", -1.0) < 0
    ):
        if liq < 8_000:
            liq_score -= 0.4
        elif liq < 20_000:
            liq_score -= 0.05
        elif liq <= 60_000:
            liq_score += 0.12
        elif liq <= 400_000:
            liq_score += 0.18
        else:
            liq_score += 0.10

    # mcap early/late buckets
    liq_score += _f(features, "mcap_bucket_early", 0.0) * 0.12
    liq_score += _f(features, "mcap_bucket_very_late", 0.0) * -0.08

    # Mild contribution from log scales
    liq_score += (log_liq / 20.0) * 0.05
    liq_score += (log_mcap / 20.0) * 0.03

    # ------------------------------------------------------------------ #
    # Age / lifecycle
    # ------------------------------------------------------------------ #
    age_min = _f(features, "age_min", _f(features, "token_age_min", 0.0))
    is_very_new = _f(features, "is_very_new", 1.0 if age_min <= 30.0 else 0.0)
    is_newish = _f(features, "is_newish", 1.0 if 30.0 < age_min <= 180.0 else 0.0)
    is_old = _f(features, "is_old", 1.0 if age_min > 240.0 else 0.0)

    age_score = 0.0
    age_score += is_very_new * 0.20
    age_score += is_newish * 0.10
    age_score -= is_old * 0.18
    if age_min > 24 * 60:
        age_score -= 0.35

    # ------------------------------------------------------------------ #
    # Smart wallets / whales / breadth
    # ------------------------------------------------------------------ #
    smart_wallet_score = _f(features, "smart_wallet_score", 0.0)  # 0..1-ish
    whales_5m = _f(features, "whales_5m", 0.0)
    whale_share_5m = _f(features, "whale_share_5m", 0.0)         # 0..1
    holders_growth_5m = _f(features, "holders_growth_5m", 0.0)

    unique_buyers_5m = _f(features, "unique_buyers_5m", 0.0)
    smart_wallet_count_5m = _f(features, "smart_wallet_count_5m", 0.0)
    smart_whale_alignment = _f(features, "smart_whale_alignment", 0.0)

    wallet_score = 0.0
    if ENTRY_SMART_WALLET_BOOST:
        wallet_score += smart_wallet_score * 0.7
        wallet_score += min(whales_5m, 8.0) * 0.03
        # punish extreme whale domination
        if whale_share_5m > 0.7:
            wallet_score -= (whale_share_5m - 0.7) * 0.8
        else:
            wallet_score += whale_share_5m * 0.25

        wallet_score += min(unique_buyers_5m / 20.0, 1.5) * 0.10
        wallet_score += min(smart_wallet_count_5m / 5.0, 1.5) * 0.12
        wallet_score += max(-0.2, min(holders_growth_5m, 0.5)) * 0.4
        wallet_score += smart_whale_alignment * 0.4

    # ------------------------------------------------------------------ #
    # Structural quality & entry micro-alpha & entry_quality composite
    # ------------------------------------------------------------------ #
    structural_quality = _f(features, "structural_quality_score", 0.0)
    is_dead_like = _f(features, "is_dead_like", 0.0)
    entry_micro = _f(features, "entry_micro_alpha", 0.0)
    risk_capacity = _f(features, "risk_capacity_score", 1.0)
    entry_quality = _f(features, "entry_quality_score", 0.0)
    pos_size_scalar = _f(features, "position_size_scalar", 1.0)
    entry_risk_score = _f(features, "entry_risk_score", 0.0)
    entry_tier_bucket = _f(features, "entry_tier_bucket", 0.0)
    pos_size_bucket = _f(features, "position_size_bucket", 0.0)

    structure_score = 0.0
    structure_score += structural_quality * 0.30
    structure_score -= is_dead_like * 0.60
    structure_score += entry_micro * 0.60
    structure_score += (risk_capacity - 0.5) * 0.20

    # Slight boost for higher discrete tiers (already derived from quality/size)
    structure_score += max(0.0, entry_tier_bucket - 1.0) * 0.06
    structure_score += max(0.0, pos_size_bucket - 1.0) * 0.04

    quality_term = 0.0
    if ENTRY_USE_ENTRY_QUALITY and entry_quality > 0.0:
        norm_q = min(entry_quality / 1.2, 1.5)  # scale into ~0..1.5
        quality_term += (norm_q - 0.6) * 0.40   # centered around 0.6

    quality_term += (pos_size_scalar - 0.5) * 0.25
    quality_term -= min(max(entry_risk_score, 0.0), 1.0) * 0.20

    # ------------------------------------------------------------------ #
    # Regime alignment (macro)
    # ------------------------------------------------------------------ #
    regime_trend = _f(
        features,
        "regime_trend",
        _f(features, "regime_score", 0.0),
    )  # -1..1
    regime_vol = _f(features, "regime_volatility", 0.0)           # 0..1
    regime_liq = _f(features, "regime_liquidity", 0.5)            # 0..1
    regime_manip = _f(features, "regime_manipulation_risk", 0.0)  # 0..1

    regime_is_risk_on = _f(
        features, "regime_is_risk_on", 1.0 if regime_trend > 0.3 else 0.0
    )
    regime_is_risk_off = _f(
        features, "regime_is_risk_off", 1.0 if regime_trend < -0.3 else 0.0
    )

    regime_component = 0.0
    regime_component += regime_trend * 0.25
    regime_component += (regime_liq - 0.5) * 0.25
    regime_component -= max(0.0, regime_manip - 0.5) * 0.6

    regime_component += regime_is_risk_on * 0.05
    regime_component -= regime_is_risk_off * 0.05

    if regime_vol > 0.7:
        regime_component -= 0.08

    # ------------------------------------------------------------------ #
    # Fraud / rug / impact penalties
    # ------------------------------------------------------------------ #
    ai_fraud_raw = _f(
        features,
        "ai_fraud_score",
        _f(features, "ai_rug_score_01", _f(features, "ai_rug_score", 0.0)),
    )
    if ai_fraud_raw > 1.5:
        ai_fraud = max(0.0, min(1.0, ai_fraud_raw / 100.0))
    else:
        ai_fraud = max(0.0, min(1.0, ai_fraud_raw))

    impact_1k_bps = _f(features, "impact_1k_bps", 0.0)
    impact_penalty = 0.0
    if impact_1k_bps > 400:
        impact_penalty -= 0.25
    elif impact_1k_bps > 200:
        impact_penalty -= 0.12

    rug_penalty = -ai_fraud * 0.7

    # ------------------------------------------------------------------ #
    # Scanner hints (momentum flags etc.)
    # ------------------------------------------------------------------ #
    scanner_momo_ok = _f(features, "scanner_momentum_ok", 0.0)
    scanner_source_id = _f(features, "scanner_source_id", 0.0)
    scanner_hint = 0.10 * scanner_momo_ok
    if scanner_source_id == 4.0:  # e.g. "momo"
        scanner_hint += 0.04

    # ------------------------------------------------------------------ #
    # Session / risk context from RiskManager / Strategy
    # ------------------------------------------------------------------ #
    ctx_open = _f(
        features,
        "ctx_open_positions",
        _f(features, "risk_open_positions", 0.0),
    )
    ctx_max = _f(
        features,
        "ctx_max_positions",
        _f(features, "risk_max_positions", 0.0),
    )
    ctx_dd = _f(
        features,
        "ctx_daily_drawdown_pct",
        _f(features, "risk_daily_dd_pct", 0.0),
    )  # negative = loss
    ctx_pnl = _f(
        features,
        "ctx_daily_realized_pnl_pct",
        _f(features, "risk_daily_pnl_pct", 0.0),
    )
    ctx_risk_mode = _f(
        features,
        "ctx_risk_mode",
        _f(features, "risk_mode_raw", 1.0),
    )
    risk_capacity = _f(features, "risk_capacity_score", 1.0)

    # New: win-rate & loss-streak nudges
    win_rate_pct = _f(features, "risk_win_rate_pct", 0.0)
    loss_streak = _f(features, "risk_loss_streak", 0.0)

    ctx_score = 0.0
    if ctx_max > 0:
        ratio = ctx_open / ctx_max
        if ratio > 0.6:
            ctx_score -= (ratio - 0.6) * 0.40

    if ctx_dd < 0:
        ctx_score -= min(abs(ctx_dd) / 10.0, 1.5) * 0.10
    if 0.0 < ctx_pnl < 5.0:
        ctx_score += min(ctx_pnl / 5.0, 1.0) * 0.05

    if ctx_risk_mode <= 0.0:
        ctx_score -= 0.4
    elif ctx_risk_mode >= 2.0:
        ctx_score += 0.08

    if risk_capacity < 0.2:
        ctx_score -= 0.15

    # Win-rate & streak:
    if win_rate_pct > 55.0:
        ctx_score += min((win_rate_pct - 55.0) / 25.0, 1.0) * 0.06
    if loss_streak >= 3:
        ctx_score -= min((loss_streak - 2.0) / 3.0, 1.0) * 0.14

    # ------------------------------------------------------------------ #
    # Combine into raw score
    # ------------------------------------------------------------------ #
    raw = (
        pf_component
        + buy_component
        + mom_score
        + vol_score
        + flow_score
        + liq_score
        + age_score
        + wallet_score
        + structure_score
        + quality_term
        + regime_component
        + impact_penalty
        + rug_penalty
        + scanner_hint
        + ctx_score
    )

    # Hard crush for extreme fraud risk before logistic squash
    if ai_fraud >= 0.95:
        raw -= 4.0

    try:
        score = 1.0 / (1.0 + math.exp(-raw))
    except OverflowError:
        score = 1.0 if raw > 0 else 0.0

    score = _clip01(score)

    if ENTRY_DEBUG_LOG:
        sym = features.get("symbol") or features.get("mint") or "?"
        log.debug(
            "ENTRY_HEURISTIC sym=%s raw=%.3f score=%.3f "
            "pf=%.3f buy=%.3f mom=%.3f vol=%.3f flow=%.3f liq=%.3f age=%.3f "
            "wallet=%.3f struct=%.3f qual=%.3f regime=%.3f impact=%.3f "
            "rug=%.3f ctx=%.3f",
            sym,
            raw,
            score,
            pf_component,
            buy_component,
            mom_score,
            vol_score,
            flow_score,
            liq_score,
            age_score,
            wallet_score,
            structure_score,
            quality_term,
            regime_component,
            impact_penalty,
            rug_penalty,
            ctx_score,
        )

    # Attach per-component breakdown into meta (for later analysis / backtests)
    if ENTRY_ATTACH_META:
        meta = features.get("entry_meta") or {}
        breakdown = meta.get("breakdown") or {}
        breakdown.update(
            {
                "pf": float(pf_component),
                "buy": float(buy_component),
                "mom": float(mom_score),
                "vol": float(vol_score),
                "flow": float(flow_score),
                "liq": float(liq_score),
                "age": float(age_score),
                "wallet": float(wallet_score),
                "structure": float(structure_score),
                "quality": float(quality_term),
                "regime": float(regime_component),
                "impact_penalty": float(impact_penalty),
                "rug_penalty": float(rug_penalty),
                "scanner_hint": float(scanner_hint),
                "ctx": float(ctx_score),
            }
        )
        meta["breakdown"] = breakdown
        features["entry_meta"] = meta

    return score


# ============================================================================ #
# RiskManager enrichment helper (risk API coupling)
# ============================================================================ #

async def _maybe_enrich_risk_context(features: Dict[str, Any]) -> None:
    """
    If FeatureBuilder didn't already inject risk context, pull a snapshot
    from utils_pkg.risk_manager and patch into the feature dict.

    This lets scanners that only know token-level features still be aware of
    current portfolio risk / capacity and strategy-level risk API fields.

    Expected optional fields from risk_manager.risk_capacity_snapshot():
      - risk_capacity, risk_mode, open_positions, max_positions
      - daily_realized_pnl_pct, daily_drawdown_pct, max_dd_pct
      - rolling_win_rate_pct, recent_loss_streak
      - hard_halt, risk_profile
      - plan_gap_pct, plan_status, plan_equity_today_usd
      - entry_score_gate (strategy-tuned entry floor)
    """
    needs_capacity = "risk_capacity_score" not in features
    needs_ctx = any(
        key not in features
        for key in (
            "ctx_open_positions",
            "ctx_max_positions",
            "ctx_daily_drawdown_pct",
            "ctx_daily_realized_pnl_pct",
            "ctx_risk_mode",
        )
    )

    # Even if we don't strictly "need" capacity/ctx, we still want to sync
    # hard_halt / risk_profile / plan fields / entry_score_gate if they exist.
    need_soft_extras = any(
        key not in features
        for key in (
            "risk_profile",
            "ctx_hard_halt",
            "ctx_plan_gap_pct",
            "ctx_plan_status",
            "ctx_entry_score_gate",
        )
    )

    if not (needs_capacity or needs_ctx or need_soft_extras):
        return

    try:
        from utils_pkg import risk_manager as _rm  # local import to avoid cycles

        snap = await _rm.risk_capacity_snapshot()
        if not isinstance(snap, dict):
            return

        # risk_capacity_score
        if needs_capacity:
            features.setdefault("risk_capacity_score", float(snap.get("risk_capacity", 1.0)))

        # ctx_* fields
        if needs_ctx:
            features.setdefault(
                "ctx_open_positions",
                float(snap.get("open_positions", 0.0)),
            )
            features.setdefault(
                "ctx_max_positions",
                float(snap.get("max_positions", 0.0)),
            )
            features.setdefault(
                "ctx_daily_drawdown_pct",
                float(snap.get("daily_drawdown_pct", snap.get("max_dd_pct", 0.0))),
            )
            features.setdefault(
                "ctx_daily_realized_pnl_pct",
                float(snap.get("daily_realized_pnl_pct", 0.0)),
            )
            features.setdefault("ctx_risk_mode", float(snap.get("risk_mode", 1.0)))

            # Optional: rolling win-rate & loss streak
            if "rolling_win_rate_pct" in snap:
                features.setdefault(
                    "risk_win_rate_pct",
                    float(snap.get("rolling_win_rate_pct", 0.0)),
                )
            if "recent_loss_streak" in snap:
                features.setdefault(
                    "risk_loss_streak",
                    float(snap.get("recent_loss_streak", 0.0)),
                )

        # --- Soft extras from risk API (strategy alignment) -----------------
        # risk_profile string
        if "risk_profile" in snap and "risk_profile" not in features:
            features["risk_profile"] = str(snap.get("risk_profile") or "balanced")

        # hard_halt from risk/strategy (we expose as ctx_hard_halt + alias)
        if "hard_halt" in snap or "HARD_HALT" in snap:
            hh = bool(snap.get("hard_halt", snap.get("HARD_HALT", False)))
            features.setdefault("ctx_hard_halt", hh)
            features.setdefault("hard_halt", hh)

        # Equity plan context
        if "plan_gap_pct" in snap and "ctx_plan_gap_pct" not in features:
            features["ctx_plan_gap_pct"] = float(snap.get("plan_gap_pct", 0.0))
        if "plan_status" in snap and "ctx_plan_status" not in features:
            features["ctx_plan_status"] = str(snap.get("plan_status", "n/a"))
        if "plan_equity_today_usd" in snap and "ctx_plan_equity_today_usd" not in features:
            features["ctx_plan_equity_today_usd"] = float(
                snap.get("plan_equity_today_usd", 0.0)
            )

        # Strategy-driven entry score gate (fed from self_learning_ai_strategy)
        # We keep it separate as ctx_entry_score_gate so we can clamp thresholds
        # upward without losing dynamic floor behavior.
        if "entry_score_gate" in snap and "ctx_entry_score_gate" not in features:
            try:
                features["ctx_entry_score_gate"] = float(snap.get("entry_score_gate", 0.0))
            except Exception:
                pass

    except Exception as e:
        if ENTRY_DEBUG_LOG:
            log.debug("ai_entry_logic: risk context enrichment failed: %s", e)


# ============================================================================ #
# OpenAI-based scoring
# ============================================================================ #

async def _ai_score(features: Dict[str, Any]) -> Optional[float]:
    """
    Call the fine-tuned OpenAI model (if configured) to get a 0..1 score.
    On any failure, returns None so callers can fall back to heuristic.

    NOTE: This function is also used by backtests via utils_pkg.backtest_engine.
    Keep signature stable.
    """
    if not ENTRY_AI_ENABLED:
        return None
    if not os.getenv("OPENAI_API_KEY"):
        if ENTRY_DEBUG_LOG:
            log.debug("ai_entry_logic: no OPENAI_API_KEY – skipping AI call")
        return None

    model = _pick_entry_model(features)
    if not model:
        return None

    client = _ensure_client()
    if client is None:
        return None

    # Possibly shrink features for payload size
    feat = dict(features)
    try:
        blob = json.dumps(feat, separators=(",", ":"), ensure_ascii=False)
        if len(blob.encode("utf-8")) > ENTRY_MAX_FEATURE_BYTES:
            # crude pruning: keep only key composites / scores / risk context
            keep_keys = {
                "symbol",
                "mint",
                "pf_final_score",
                "pf_risk_adjusted_final",
                "pf_momentum_score",
                "entry_quality_score",
                "entry_micro_alpha",
                "structural_quality_score",
                "entry_tier_bucket",
                "position_size_bucket",
                "position_size_scalar",
                "is_dead_like",
                "entry_risk_score",
                "liq_usd",
                "mcap_usd",
                "age_min",
                "token_age_min",
                "vol_5m_usd",
                "buys_5m",
                "sells_5m",
                "smart_wallet_score",
                "smart_wallet_count_5m",
                "smart_whale_alignment",
                "unique_buyers_5m",
                "ai_fraud_score",
                "ai_rug_score",
                "ai_rug_score_01",
                "ai_buy_score",
                "regime_trend",
                "regime_liquidity",
                "regime_manipulation_risk",
                "regime_volatility",
                "risk_capacity_score",
                "ctx_open_positions",
                "ctx_max_positions",
                "ctx_daily_drawdown_pct",
                "ctx_daily_realized_pnl_pct",
                "ctx_risk_mode",
                "risk_win_rate_pct",
                "risk_loss_streak",
                "scanner_source_id",
                "scanner_momentum_ok",
                "ctx_plan_gap_pct",
                "ctx_plan_status",
                "ctx_entry_score_gate",
                "ctx_hard_halt",
            }
            feat = {k: v for k, v in feat.items() if k in keep_keys}
            blob = json.dumps(feat, separators=(",", ":"), ensure_ascii=False)
    except Exception:
        blob = json.dumps({"features": "encode_error"})

    payload = {
        "features": feat,
        "task": "score_entry_probability_0_to_1",
    }
    prompt = (
        "You are a trading classifier. Given the following JSON features for a token, "
        "return ONLY a JSON object of the form {\"score\": <number between 0 and 1>} "
        "representing the probability that entering NOW will be profitable.\n\n"
        f"{json.dumps(payload, separators=(',', ':'), ensure_ascii=False)}"
    )

    try:
        if _USE_RESPONSES_API:

            def _do_req() -> Any:
                return client.responses.create(
                    model=model,
                    input=prompt,
                    response_format={"type": "json_object"},
                )

            resp = await asyncio.wait_for(
                asyncio.to_thread(_do_req),
                timeout=ENTRY_AI_TIMEOUT_SEC,
            )
            content_str = str(resp)
            try:
                out = resp.output[0].content[0].text  # type: ignore[attr-defined,index]
                content_str = getattr(out, "value", str(out))
            except Exception:
                pass
        else:

            def _do_req_legacy() -> Any:
                return client.ChatCompletion.create(
                    model=model,
                    messages=[
                        {"role": "system", "content": "You output JSON only."},
                        {"role": "user", "content": prompt},
                    ],
                    temperature=0.0,
                )

            resp = await asyncio.wait_for(
                asyncio.to_thread(_do_req_legacy),
                timeout=ENTRY_AI_TIMEOUT_SEC,
            )
            content_str = resp["choices"][0]["message"]["content"]  # type: ignore[index]

        try:
            obj = json.loads(content_str)
            score = float(obj.get("score", 0.0))
            score = _clip01(score)
        except Exception:
            # Fallback: try to parse a raw float
            try:
                score = float(str(content_str).strip())
                score = _clip01(score)
            except Exception:
                if ENTRY_DEBUG_LOG_FAIL:
                    log.warning(
                        "ai_entry_logic: could not parse AI score content=%r",
                        str(content_str)[:200],
                    )
                return None

        # Optional hard floor on AI score
        if ENTRY_MIN_AI_SCORE > 0 and score < ENTRY_MIN_AI_SCORE:
            return None

        return score

    except Exception as e:
        if ENTRY_DEBUG_LOG_FAIL:
            log.warning("ai_entry_logic: OpenAI call failed: %s", str(e)[:200])
        if ENTRY_AI_STRICT:
            raise
        return None


# ============================================================================ #
# Public API
# ============================================================================ #

async def score_entry(features: Dict[str, Any]) -> float:
    """
    Return a score in [0,1] = probability this is a 'good' trade.

    Pipeline:
      1) (Optional) enrich features with risk_capacity_score and ctx_* from
         RiskManager if FeatureBuilder didn't inject them (risk API link).
      2) Compute heuristic baseline over PF scores + FeatureVector fields +
         optional ai_buy_scoring prior.
      3) If AI model configured, blend AI score with heuristic using ENTRY_AI_WEIGHT.
      4) Apply per-source score bonus.
      5) Attach entry_meta into features (if enabled) and return final score.
    """
    # Optional risk capacity + context enrichment from utils_pkg.risk_manager
    await _maybe_enrich_risk_context(features)

    heuristic = _heuristic_score(features)
    ai_score = await _ai_score(features)

    if ai_score is None:
        blended = heuristic
    else:
        blended = ENTRY_AI_WEIGHT * ai_score + (1.0 - ENTRY_AI_WEIGHT) * heuristic

    blended = _clip01(blended + _source_score_bonus(features))

    if ENTRY_ATTACH_META:
        meta = features.get("entry_meta") or {}
        meta.update(
            {
                "heuristic_score": float(heuristic),
                "ai_score": float(ai_score) if ai_score is not None else None,
                "blended_score": float(blended),
                "buy_score": float(_f(features, "ai_buy_score", 0.0)),
                "source_key": _source_key(features),
                "risk_profile": _effective_risk_profile(features),
                "regime_risk_mode": _effective_regime_mode(features),
            }
        )
        features["entry_meta"] = meta

    return blended


async def should_enter(features: Dict[str, Any]) -> Tuple[bool, float, str]:
    """
    Convenience helper for Strategy:

        ok, score, reason = await should_enter(feats)
        if ok: ... proceed to buy ...

    Returns:
      - ok:     True if score >= per-source+regime+profile threshold and hard guards pass
      - score:  final blended score in [0,1]
      - reason: short human-readable explanation string
    """
    score = await score_entry(features)

    liq = _f(features, "liq_usd", 0.0)
    age_min = _f(features, "age_min", _f(features, "token_age_min", 0.0))

    # Regime
    regime_trend = _f(
        features,
        "regime_trend",
        _f(features, "regime_score", 0.0),
    )
    regime_manip = _f(features, "regime_manipulation_risk", 0.0)

    # Structural
    structural_quality = _f(features, "structural_quality_score", 0.0)
    is_dead_like = _f(features, "is_dead_like", 0.0)
    entry_micro = _f(features, "entry_micro_alpha", 0.0)

    # FeatureBuilder composites
    entry_quality = _f(features, "entry_quality_score", 0.0)
    pos_size_scalar = _f(features, "position_size_scalar", 1.0)
    entry_block_flag = _f(features, "entry_block_flag", 0.0)
    entry_tier_bucket = _f(features, "entry_tier_bucket", 0.0)
    pos_size_bucket = _f(features, "position_size_bucket", 0.0)

    # Fraud / rug
    ai_fraud_raw = _f(
        features,
        "ai_fraud_score",
        _f(features, "ai_rug_score_01", _f(features, "ai_rug_score", 0.0)),
    )
    if ai_fraud_raw > 1.5:
        ai_fraud = max(0.0, min(1.0, ai_fraud_raw / 100.0))
    else:
        ai_fraud = max(0.0, min(1.0, ai_fraud_raw))

    pf_final = _f(features, "pf_final_score", 0.0)
    pf_risk_adj = _f(
        features,
        "pf_risk_adjusted_final",
        pf_final * (1.0 - ai_fraud) if pf_final > 0.0 else 0.0,
    )

    buys_5m = _i(features, "buys_5m", 0)
    vol_5m_usd = _f(features, "vol_5m_usd", 0.0)

    # Session context (support both ctx_* and risk_* naming)
    ctx_open = _f(
        features,
        "ctx_open_positions",
        _f(features, "risk_open_positions", 0.0),
    )
    ctx_max = _f(
        features,
        "ctx_max_positions",
        _f(features, "risk_max_positions", 0.0),
    )
    ctx_dd = _f(
        features,
        "ctx_daily_drawdown_pct",
        _f(features, "risk_daily_dd_pct", 0.0),
    )
    ctx_pnl = _f(
        features,
        "ctx_daily_realized_pnl_pct",
        _f(features, "risk_daily_pnl_pct", 0.0),
    )
    ctx_risk_mode = _f(
        features,
        "ctx_risk_mode",
        _f(features, "risk_mode_raw", 1.0),
    )
    risk_capacity = _f(features, "risk_capacity_score", 1.0)

    # Win-rate / streak for hard-ish gating in conservative modes
    win_rate_pct = _f(features, "risk_win_rate_pct", 0.0)
    loss_streak = _f(features, "risk_loss_streak", 0.0)

    # Risk-API extras
    hard_halt = bool(
        features.get("ctx_hard_halt")
        or features.get("hard_halt")
        or features.get("strategy_hard_halt")
    )
    plan_gap_pct = _f(features, "ctx_plan_gap_pct", 0.0)
    plan_status = str(features.get("ctx_plan_status", "n/a"))

    # Regime risk-mode lock
    regime_mode_lock = _effective_regime_mode(features)

    # Buy score hard guard (optional)
    if ENTRY_USE_BUY_SCORE and ENTRY_BUY_HARD_FAIL:
        buy_score = _f(features, "ai_buy_score", 0.0)
        buy_ok_hard = bool(
            features.get("ai_buy_ok_hard")
            or features.get("ai_buy_pass_gate")
        )
        if buy_score > 0.0 and not buy_ok_hard:
            reason = "ai_buy_scoring_hard_fail"
            if ENTRY_ATTACH_META:
                features.setdefault("entry_meta", {}).update(
                    {"reason": reason, "pass": False, "effective_threshold": None}
                )
            return False, score, reason

    # ----------------------- Hard guards -------------------------------- #

    # Risk API: global hard halt from risk_manager / strategy
    if hard_halt:
        reason = "risk_api_hard_halt"
        if ENTRY_ATTACH_META:
            features.setdefault("entry_meta", {}).update(
                {
                    "reason": reason,
                    "pass": False,
                    "hard_halt": True,
                    "ctx_lock": True,
                }
            )
        return False, score, reason

    # Risk API: zero capacity
    if risk_capacity <= 0.0:
        reason = "risk_capacity_zero"
        if ENTRY_ATTACH_META:
            features.setdefault("entry_meta", {}).update(
                {
                    "reason": reason,
                    "pass": False,
                    "capacity_zero": True,
                    "risk_capacity": float(risk_capacity),
                    "ctx_lock": True,
                }
            )
        return False, score, reason

    # FeatureBuilder hard veto
    if entry_block_flag >= 0.5:
        reason = "entry_block_flag"
        if ENTRY_ATTACH_META:
            features.setdefault("entry_meta", {}).update(
                {"reason": reason, "pass": False}
            )
        return False, score, reason

    # Liquidity
    if liq < ENTRY_MIN_LIQ_USD:
        reason = f"low_liq({liq:.0f} < {ENTRY_MIN_LIQ_USD:.0f})"
        if ENTRY_ATTACH_META:
            features.setdefault("entry_meta", {}).update(
                {"reason": reason, "pass": False}
            )
        return False, score, reason

    # Session drawdown
    if ENTRY_CTX_MAX_DRAWDOWN_PCT > 0 and ctx_dd <= -ENTRY_CTX_MAX_DRAWDOWN_PCT:
        reason = (
            f"session_drawdown_limit(ctx_dd={ctx_dd:.2f}% <= "
            f"-{ENTRY_CTX_MAX_DRAWDOWN_PCT:.1f}%)"
        )
        if ENTRY_ATTACH_META:
            features.setdefault("entry_meta", {}).update(
                {"reason": reason, "pass": False, "dd_lock": True}
            )
        return False, score, reason

    # Lock out once daily target hit unless in explicitly aggressive mode
    if (
        ENTRY_CTX_LOCK_AFTER_TARGET_PNL > 0
        and ctx_pnl >= ENTRY_CTX_LOCK_AFTER_TARGET_PNL
        and ctx_risk_mode <= 1.0
    ):
        reason = (
            f"session_target_hit(ctx_pnl={ctx_pnl:.2f}% >= "
            f"{ENTRY_CTX_LOCK_AFTER_TARGET_PNL:.2f}%)"
        )
        if ENTRY_ATTACH_META:
            features.setdefault("entry_meta", {}).update(
                {"reason": reason, "pass": False, "target_lock": True}
            )
        return False, score, reason

    # Exposure relative to max positions
    if ctx_max > 0 and ENTRY_CTX_MAX_OPEN_POS_RATIO > 0:
        ratio = ctx_open / ctx_max
        limit = ENTRY_CTX_MAX_OPEN_POS_RATIO
        if ratio >= limit:
            reason = (
                f"too_many_positions(open={ctx_open:.0f}/{ctx_max:.0f}, "
                f"ratio={ratio:.2f}>=limit={limit:.2f})"
            )
            if ENTRY_ATTACH_META:
                features.setdefault("entry_meta", {}).update(
                    {"reason": reason, "pass": False, "pos_full": True}
                )
            return False, score, reason

    # Fraud / rug
    if ai_fraud >= ENTRY_MAX_RUG_SCORE:
        reason = f"high_fraud_score({ai_fraud:.2f} >= {ENTRY_MAX_RUG_SCORE:.2f})"
        if ENTRY_ATTACH_META:
            features.setdefault("entry_meta", {}).update(
                {"reason": reason, "pass": False, "fraud_lock": True}
            )
        return False, score, reason

    # Age guard (optional, allow some buffer)
    if ENTRY_MAX_AGE_MIN > 0 and age_min > ENTRY_MAX_AGE_MIN * 2:
        reason = f"too_old(age_min={age_min:.1f} > {ENTRY_MAX_AGE_MIN*2:.1f})"
        if ENTRY_ATTACH_META:
            features.setdefault("entry_meta", {}).update(
                {"reason": reason, "pass": False}
            )
        return False, score, reason

    # Dead-like coins
    if ENTRY_FORBID_DEADLIKE and is_dead_like >= 0.5:
        reason = "dead_like_heuristic"
        if ENTRY_ATTACH_META:
            features.setdefault("entry_meta", {}).update(
                {"reason": reason, "pass": False}
            )
        return False, score, reason

    # Structural quality gate
    if ENTRY_MIN_STRUCTURAL_QUALITY > 0 and structural_quality < ENTRY_MIN_STRUCTURAL_QUALITY:
        reason = (
            f"low_structural_quality({structural_quality:.2f} "
            f"< {ENTRY_MIN_STRUCTURAL_QUALITY:.2f})"
        )
        if ENTRY_ATTACH_META:
            features.setdefault("entry_meta", {}).update(
                {"reason": reason, "pass": False}
            )
        return False, score, reason

    # Entry micro-alpha gate
    if ENTRY_MIN_ENTRY_MICRO > 0 and entry_micro < ENTRY_MIN_ENTRY_MICRO:
        reason = (
            f"low_entry_micro_alpha({entry_micro:.2f} < {ENTRY_MIN_ENTRY_MICRO:.2f})"
        )
        if ENTRY_ATTACH_META:
            features.setdefault("entry_meta", {}).update(
                {"reason": reason, "pass": False}
            )
        return False, score, reason

    # Entry quality gate
    if ENTRY_USE_ENTRY_QUALITY and ENTRY_MIN_ENTRY_QUALITY > 0 and entry_quality < ENTRY_MIN_ENTRY_QUALITY:
        reason = (
            f"low_entry_quality({entry_quality:.2f} < {ENTRY_MIN_ENTRY_QUALITY:.2f})"
        )
        if ENTRY_ATTACH_META:
            features.setdefault("entry_meta", {}).update(
                {"reason": reason, "pass": False}
            )
        return False, score, reason

    # Position size scalar gate (optional)
    if ENTRY_REQUIRE_POS_SIZE and pos_size_scalar < ENTRY_MIN_POS_SIZE:
        reason = (
            f"small_position_size_scalar({pos_size_scalar:.2f} < "
            f"{ENTRY_MIN_POS_SIZE:.2f})"
        )
        if ENTRY_ATTACH_META:
            features.setdefault("entry_meta", {}).update(
                {"reason": reason, "pass": False}
            )
        return False, score, reason

    # Soft extra guards vs dead coins
    if ENTRY_MIN_5M_BUYS > 0 and buys_5m < ENTRY_MIN_5M_BUYS:
        reason = f"low_recent_flow(buys_5m={buys_5m} < {ENTRY_MIN_5M_BUYS})"
        if ENTRY_ATTACH_META:
            features.setdefault("entry_meta", {}).update(
                {"reason": reason, "pass": False}
            )
        return False, score, reason

    if ENTRY_MIN_5M_VOL_USD > 0 and vol_5m_usd < ENTRY_MIN_5M_VOL_USD:
        reason = (
            f"low_recent_volume(vol_5m={vol_5m_usd:.0f} < {ENTRY_MIN_5M_VOL_USD:.0f})"
        )
        if ENTRY_ATTACH_META:
            features.setdefault("entry_meta", {}).update(
                {"reason": reason, "pass": False}
            )
        return False, score, reason

    # PF floors (if enabled + present)
    if ENTRY_USE_PF_SCORES:
        if pf_final > 0.0 and pf_final < ENTRY_MIN_PF_FINAL:
            reason = (
                f"pf_final_below_min({pf_final:.3f} < {ENTRY_MIN_PF_FINAL:.3f})"
            )
            if ENTRY_ATTACH_META:
                features.setdefault("entry_meta", {}).update(
                    {"reason": reason, "pass": False}
                )
            return False, score, reason
        if pf_risk_adj > 0.0 and pf_risk_adj < ENTRY_MIN_PF_RISK_ADJ:
            reason = (
                f"pf_risk_adj_below_min({pf_risk_adj:.3f} < {ENTRY_MIN_PF_RISK_ADJ:.3f})"
            )
            if ENTRY_ATTACH_META:
                features.setdefault("entry_meta", {}).update(
                    {"reason": reason, "pass": False}
                )
            return False, score, reason

    # Risk capacity requirement (soft vs ENTRY_MIN_RISK_CAPACITY)
    if ENTRY_REQUIRE_RISK_CAPACITY and risk_capacity < ENTRY_MIN_RISK_CAPACITY:
        reason = (
            f"low_risk_capacity({risk_capacity:.2f} < {ENTRY_MIN_RISK_CAPACITY:.2f})"
        )
        if ENTRY_ATTACH_META:
            features.setdefault("entry_meta", {}).update(
                {"reason": reason, "pass": False}
            )
        return False, score, reason

    # Extra discipline for bad streaks in conservative / normal modes
    if loss_streak >= 4 and _effective_risk_profile(features) != "aggressive":
        reason = f"loss_streak_guard(streak={loss_streak:.0f} >= 4)"
        if ENTRY_ATTACH_META:
            features.setdefault("entry_meta", {}).update(
                {"reason": reason, "pass": False, "streak_lock": True}
            )
        return False, score, reason

    # Slight extra caution if win-rate is horrible AND we're conservative
    if win_rate_pct > 0.0 and win_rate_pct < 40.0 and _effective_risk_profile(features) == "conservative":
        # we don't hard fail, but nudge threshold via dynamic floor anyway
        pass

    # ------------------------------------------------------------------ #
    # Regime tightening + risk-profile thresholding
    # ------------------------------------------------------------------ #
    base_threshold = _source_min_threshold(features)

    # Strategy / risk API entry gate floor from risk_manager/self_learning
    ctx_entry_gate = _f(features, "ctx_entry_score_gate", 0.0)
    if ctx_entry_gate > 0.0:
        base_threshold = max(base_threshold, _clip01(ctx_entry_gate))

    tightened = base_threshold
    risk_off_flag = False
    manip_flag = False

    # Apply regime lock interpretation
    if regime_mode_lock == 0:
        # hard risk-off
        regime_trend = min(regime_trend, -0.8)
    elif regime_mode_lock == 2:
        # risk-on
        regime_trend = max(regime_trend, 0.8)

    if ENTRY_REGIME_TIGHTEN_ENABLED:
        if regime_trend <= -0.7:
            tightened = max(tightened + ENTRY_RISK_OFF_EXTRA_MARGIN, 0.80)
            risk_off_flag = True
        if regime_manip >= ENTRY_MANIP_HIGH:
            tightened = max(tightened + 0.08, tightened)
            manip_flag = True

    # Risk-profile tweak
    prof_threshold = _risk_profile_threshold_adjust(tightened, features)

    # Threshold nudges from tiers (never below base)
    if entry_tier_bucket >= 3.0 and pos_size_bucket >= 3.0:
        prof_threshold = max(base_threshold, prof_threshold - 0.01)
    elif entry_tier_bucket <= 1.0 and pos_size_bucket <= 1.0:
        prof_threshold = min(1.0, prof_threshold + 0.01)

    # Final threshold check
    if score < prof_threshold:
        reason = (
            f"score_below_threshold(score={score:.3f} < threshold={prof_threshold:.3f})"
        )
        if ENTRY_ATTACH_META:
            features.setdefault("entry_meta", {}).update(
                {
                    "reason": reason,
                    "pass": False,
                    "effective_threshold": float(prof_threshold),
                    "base_threshold": float(base_threshold),
                    "risk_off_tightening": risk_off_flag,
                    "manip_tightening": manip_flag,
                    "risk_profile": _effective_risk_profile(features),
                    "regime_risk_mode": regime_mode_lock,
                    "dynamic_floor": _global_min_threshold(),
                    "ctx_entry_score_gate": float(ctx_entry_gate) if ctx_entry_gate > 0 else None,
                    "ctx_plan_gap_pct": float(plan_gap_pct),
                    "ctx_plan_status": plan_status,
                }
            )
        return False, score, reason

    # If we got here, everything passes
    reason = "ok"
    if ENTRY_ATTACH_META:
        features.setdefault("entry_meta", {}).update(
            {
                "reason": reason,
                "pass": True,
                "effective_threshold": float(prof_threshold),
                "base_threshold": float(base_threshold),
                "risk_off_tightening": risk_off_flag,
                "manip_tightening": manip_flag,
                "risk_profile": _effective_risk_profile(features),
                "regime_risk_mode": regime_mode_lock,
                "dynamic_floor": _global_min_threshold(),
                "ctx_entry_score_gate": float(ctx_entry_gate) if ctx_entry_gate > 0 else None,
                "ctx_plan_gap_pct": float(plan_gap_pct),
                "ctx_plan_status": plan_status,
            }
        )

    return True, score, reason


def score_entry_sync(features: Dict[str, Any]) -> float:
    """
    Synchronous wrapper over score_entry() for scripts / quick tests.

    - If there is no running event loop, we create one and run the coroutine.
    - If there is a running loop (async context), we fall back to heuristic-only
      to avoid cross-loop issues.
    """
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = None

    if loop and loop.is_running():
        if ENTRY_DEBUG_LOG:
            log.debug(
                "score_entry_sync called from async context; returning heuristic-only score"
            )
        return _heuristic_score(features)

    return asyncio.run(score_entry(features))


# ============================================================================ #
# AI maintenance / calibration loop
# ============================================================================ #

async def ai_maintenance_loop(stop_evt: asyncio.Event) -> None:
    """
    Background loop to:
      - Observe recent performance (win-rate / DD / realized PnL) via risk_manager
        or data_logger summary.
      - Gently auto-tune the global dynamic min-score floor inside
        [ENTRY_DYNAMIC_MIN_LO, ENTRY_DYNAMIC_MIN_HI].
      - Optionally log calibration decisions for debugging.

    This does NOT touch any env vars; it only updates the in-process
    _ENTRY_DYNAMIC_MIN_SCORE, which is then used by _global_min_threshold().

    This loop is implicitly "sanity-passed" against the risk API via
    risk_manager.risk_capacity_snapshot(), so floors respond to the same
    DD / PnL / streak signals the rest of the stack uses.
    """
    global _ENTRY_DYNAMIC_MIN_SCORE

    if not ENTRY_DYNAMIC_MIN_ENABLED:
        log.event("AI_MAINT_DISABLED", reason="ENTRY_DYNAMIC_MIN_ENABLED=0")
        return

    interval_sec = max(30.0, _env_float("ENTRY_AI_MAINT_INTERVAL_SEC", 180.0))
    backoff = interval_sec

    # Initialize to base floor if not set
    if _ENTRY_DYNAMIC_MIN_SCORE is None:
        _ENTRY_DYNAMIC_MIN_SCORE = ENTRY_MIN_SCORE

    log.event(
        "AI_MAINT_START",
        interval_sec=interval_sec,
        base_min=ENTRY_MIN_SCORE,
        lo=ENTRY_DYNAMIC_MIN_LO,
        hi=ENTRY_DYNAMIC_MIN_HI,
    )

    while not stop_evt.is_set():
        try:
            # --- Gather risk snapshot from risk_manager (primary) ---
            snap: Dict[str, Any] = {}
            try:
                from utils_pkg import risk_manager as _rm  # type: ignore

                snap = await _rm.risk_capacity_snapshot()
            except Exception as e:
                if ENTRY_DEBUG_LOG:
                    log.debug("ai_maintenance: risk_capacity_snapshot failed: %s", e)

            win_rate = float(snap.get("rolling_win_rate_pct", 0.0))
            daily_pnl_pct = float(snap.get("daily_realized_pnl_pct", 0.0))
            daily_dd_pct = float(
                snap.get("daily_drawdown_pct", snap.get("max_dd_pct", 0.0))
            )
            loss_streak = float(snap.get("recent_loss_streak", 0.0))

            # Optional: refine with data_logger summary if exposed
            try:
                from utils_pkg.data_logger import summarize_trading_day  # type: ignore[attr-defined]

                summary = await summarize_trading_day()
                if isinstance(summary, dict):
                    win_rate = float(summary.get("win_rate_pct", win_rate))
                    daily_pnl_pct = float(summary.get("realized_pnl_pct", daily_pnl_pct))
            except Exception:
                pass

            prev_floor = _global_min_threshold()
            new_floor = prev_floor

            # --- Simple adaptive policy --------------------------------
            #   - If DD is large or win-rate is poor -> tighten (raise floor)
            #   - If DD is small and win-rate is good -> loosen (lower floor)
            #   - Changes are small and clamped in [LO, HI]
            dd_bad = daily_dd_pct <= -ENTRY_CTX_MAX_DRAWDOWN_PCT * 0.5
            win_good = win_rate >= 55.0
            win_excellent = win_rate >= 65.0
            pnl_good = daily_pnl_pct >= 4.0
            pnl_bad = daily_pnl_pct <= -3.0
            streak_bad = loss_streak >= 4.0

            # Tighten conditions
            if dd_bad or pnl_bad or streak_bad:
                new_floor = min(
                    ENTRY_DYNAMIC_MIN_HI,
                    prev_floor + ENTRY_DYNAMIC_STEP,
                )
            # Loosen conditions
            elif (win_excellent and daily_pnl_pct >= 6.0) or (win_good and pnl_good):
                new_floor = max(
                    ENTRY_DYNAMIC_MIN_LO,
                    prev_floor - ENTRY_DYNAMIC_STEP,
                )

            # Clamp and assign
            new_floor = max(ENTRY_DYNAMIC_MIN_LO, min(ENTRY_DYNAMIC_MIN_HI, new_floor))
            _ENTRY_DYNAMIC_MIN_SCORE = new_floor

            if ENTRY_DEBUG_LOG:
                log.debug(
                    "ai_maintenance: floor prev=%.3f new=%.3f win=%.1f pnl=%.2f dd=%.2f streak=%.1f",
                    prev_floor,
                    new_floor,
                    win_rate,
                    daily_pnl_pct,
                    daily_dd_pct,
                    loss_streak,
                )

            log.event(
                "AI_MAINT_STEP",
                prev_floor=float(prev_floor),
                new_floor=float(new_floor),
                win_rate=win_rate,
                daily_pnl_pct=daily_pnl_pct,
                daily_dd_pct=daily_dd_pct,
                loss_streak=loss_streak,
            )

            backoff = interval_sec

        except asyncio.CancelledError:
            raise
        except Exception as e:
            log.event("AI_MAINT_ERROR", error=str(e))
            # small backoff on error
            backoff = min(backoff * 1.5, max(600.0, interval_sec * 4))

        # Wait with cancellation-awareness
        remaining = backoff
        while remaining > 0 and not stop_evt.is_set():
            sl = min(5.0, remaining)
            await asyncio.sleep(sl)
            remaining -= sl

    log.event("AI_MAINT_STOP")


# ============================================================================ #
# Diagnostics snapshot (for scripts/test_stack AI_ENTRY_LAYER)
# ============================================================================ #

async def diag_snapshot() -> Dict[str, Any]:
    """
    Lightweight async diagnostics snapshot for the AI entry layer.

    Used by scripts/test_stack (AI_ENTRY_LAYER) to verify that:

      • Env-driven config is wired (toggles, thresholds, dynamic floor)
      • RiskManager & Regime modules are reachable (best-effort)
      • Dynamic floor is inside expected bounds

    This does NOT mutate any state and is safe to call even with no traffic.
    """
    cfg = {
        "ENTRY_AI_ENABLED": ENTRY_AI_ENABLED,
        "ENTRY_AI_WEIGHT": ENTRY_AI_WEIGHT,
        "ENTRY_MIN_SCORE": ENTRY_MIN_SCORE,
        "ENTRY_DYNAMIC_MIN_ENABLED": ENTRY_DYNAMIC_MIN_ENABLED,
        "ENTRY_DYNAMIC_MIN_LO": ENTRY_DYNAMIC_MIN_LO,
        "ENTRY_DYNAMIC_MIN_HI": ENTRY_DYNAMIC_MIN_HI,
        "ENTRY_DYNAMIC_STEP": ENTRY_DYNAMIC_STEP,
        "ENTRY_MIN_AI_SCORE": ENTRY_MIN_AI_SCORE,
        "ENTRY_MIN_LIQ_USD": ENTRY_MIN_LIQ_USD,
        "ENTRY_MAX_RUG_SCORE": ENTRY_MAX_RUG_SCORE,
        "ENTRY_USE_PF_SCORES": ENTRY_USE_PF_SCORES,
        "ENTRY_MIN_PF_FINAL": ENTRY_MIN_PF_FINAL,
        "ENTRY_MIN_PF_RISK_ADJ": ENTRY_MIN_PF_RISK_ADJ,
        "ENTRY_MIN_5M_BUYS": ENTRY_MIN_5M_BUYS,
        "ENTRY_MIN_5M_VOL_USD": ENTRY_MIN_5M_VOL_USD,
        "ENTRY_REGIME_TIGHTEN_ENABLED": ENTRY_REGIME_TIGHTEN_ENABLED,
        "ENTRY_RISK_OFF_EXTRA_MARGIN": ENTRY_RISK_OFF_EXTRA_MARGIN,
        "ENTRY_MANIP_HIGH": ENTRY_MANIP_HIGH,
        "ENTRY_SMART_WALLET_BOOST": ENTRY_SMART_WALLET_BOOST,
        "ENTRY_MIN_ENTRY_MICRO": ENTRY_MIN_ENTRY_MICRO,
        "ENTRY_MIN_STRUCTURAL_QUALITY": ENTRY_MIN_STRUCTURAL_QUALITY,
        "ENTRY_FORBID_DEADLIKE": ENTRY_FORBID_DEADLIKE,
        "ENTRY_CTX_MAX_DRAWDOWN_PCT": ENTRY_CTX_MAX_DRAWDOWN_PCT,
        "ENTRY_CTX_MAX_OPEN_POS_RATIO": ENTRY_CTX_MAX_OPEN_POS_RATIO,
        "ENTRY_CTX_LOCK_AFTER_TARGET_PNL": ENTRY_CTX_LOCK_AFTER_TARGET_PNL,
        "ENTRY_REQUIRE_RISK_CAPACITY": ENTRY_REQUIRE_RISK_CAPACITY,
        "ENTRY_MIN_RISK_CAPACITY": ENTRY_MIN_RISK_CAPACITY,
        "ENTRY_USE_ENTRY_QUALITY": ENTRY_USE_ENTRY_QUALITY,
        "ENTRY_MIN_ENTRY_QUALITY": ENTRY_MIN_ENTRY_QUALITY,
        "ENTRY_REQUIRE_POS_SIZE": ENTRY_REQUIRE_POS_SIZE,
        "ENTRY_MIN_POS_SIZE": ENTRY_MIN_POS_SIZE,
        "ENTRY_USE_BUY_SCORE": ENTRY_USE_BUY_SCORE,
        "ENTRY_MIN_BUY_SCORE": ENTRY_MIN_BUY_SCORE,
        "ENTRY_BUY_SCORE_WEIGHT": ENTRY_BUY_SCORE_WEIGHT,
        "ENTRY_BUY_HARD_FAIL": ENTRY_BUY_HARD_FAIL,
        "ENTRY_AI_TIMEOUT_SEC": ENTRY_AI_TIMEOUT_SEC,
        "ENTRY_MAX_FEATURE_BYTES": ENTRY_MAX_FEATURE_BYTES,
        "ENTRY_DEBUG_LOG": ENTRY_DEBUG_LOG,
        "ENTRY_DEBUG_LOG_FAIL": ENTRY_DEBUG_LOG_FAIL,
        "ENTRY_ATTACH_META": ENTRY_ATTACH_META,
        "RISK_PROFILE": RISK_PROFILE,
        "ENTRY_REGIME_RISK_MODE_LOCK": ENTRY_REGIME_RISK_MODE_LOCK,
    }

    out: Dict[str, Any] = {
        "config": cfg,
        "dynamic_floor": _global_min_threshold(),
        "dynamic_min_score_raw": _ENTRY_DYNAMIC_MIN_SCORE,
        "source_score_overrides": ENTRY_SOURCE_SCORE_OVERRIDES,
        "source_score_bonus": ENTRY_SOURCE_SCORE_BONUS,
    }

    # Best-effort risk snapshot
    try:
        from utils_pkg import risk_manager as _rm  # type: ignore

        snap = await _rm.risk_capacity_snapshot()
        if isinstance(snap, dict):
            # keep a small subset to avoid huge dumps
            out["risk_capacity_snapshot"] = {
                "risk_capacity": snap.get("risk_capacity"),
                "risk_mode": snap.get("risk_mode"),
                "open_positions": snap.get("open_positions"),
                "max_positions": snap.get("max_positions"),
                "daily_realized_pnl_pct": snap.get("daily_realized_pnl_pct"),
                "daily_drawdown_pct": snap.get("daily_drawdown_pct", snap.get("max_dd_pct")),
                "rolling_win_rate_pct": snap.get("rolling_win_rate_pct"),
                "recent_loss_streak": snap.get("recent_loss_streak"),
                "hard_halt": snap.get("hard_halt"),
                "entry_score_gate": snap.get("entry_score_gate"),
            }
    except Exception as e:
        out["risk_error"] = f"risk_manager_snapshot_failed: {e}"

    # Best-effort regime snapshot
    try:
        from utils_pkg.regime import regime_state  # type: ignore

        feats = await regime_state.features()
        if isinstance(feats, dict):
            out["regime_features"] = {
                "regime_label": feats.get("regime_label"),
                "regime_trend": feats.get("regime_trend"),
                "regime_volatility": feats.get("regime_volatility"),
                "regime_liquidity": feats.get("regime_liquidity"),
                "regime_manipulation_risk": feats.get("regime_manipulation_risk"),
                "regime_risk_multiplier": feats.get("regime_risk_multiplier"),
            }
    except Exception as e:
        out["regime_error"] = f"regime_state_features_failed: {e}"

    return out
