# utils_pkg/ai_buy_scoring.py
# -*- coding: utf-8 -*-
from __future__ import annotations

"""
AI Buy Scoring — V3+++ (quant-grade)
------------------------------------

Backwards-compatible scorer used by sniper / scanners / reinvestor:

    from utils_pkg.ai_buy_scoring import Candidate, pass_gate, score_only
    ok, score, why = pass_gate(Candidate(...))

High-level role
---------------
This module runs just *ahead* of the actual entry engine and acts as a
"pre-gate" on raw pool candidates:

  • Hard filters  (fast reject, regime / risk / AI fraud aware)
  • Soft score    (0..1, quant-style composite)
  • Gate          (score >= AI_SCORE_GATE, plus optional extra guards)

It is the deterministic “bouncer at the door”. The smarter AI brain
(`ai_entry_logic.py` + models + self_learning_ai_strategy) sits behind
this and uses the outputs:

    - ai_buy_score
    - ai_buy_pass_gate
    - ai_buy_size_scalar
    - ai_buy_reason
    - ai_buy_gate_threshold

Design
------
- 100% deterministic & cheap (no OpenAI here, no Redis, no HTTP).
- Aware of:
      * Pool age, liquidity, impact, top10 concentration
      * 5m volatility & returns (trend sanity: no manic chase / knives)
      * Orderflow & orderbook microstructure
      * Whales & holder breadth / share & whale liquidity footprint
      * Smart-wallet & whale alignment (FeatureBuilder / smart_wallets)
      * PF scores (momentum / final / risk-adjusted)
      * AI fraud / rug risk scores (0..1 or 0..100)
      * Regime / session context (risk-off, drawdown, exposure, loss streak)
      * Risk profile flags (conservative / balanced / aggressive)
      * Risk capacity score & ai_risk_model overlays (capacity / regime gate)
      * High-level entry_quality_score / position_size_scalar / entry_block_flag
      * Market cap sweet-spot (micro/mid buckets)
      * Optional AI entry model probability / score
      * Tier buckets (entry_tier_bucket / position_size_bucket)
      * Flow regime (accumulation vs panic dump) & buy-pressure score
      * Structural quality & entry_risk_score as final sanity guards
      * Optional self-learning overrides via Candidate.meta
      * Infra flags (e.g. pump.fun liquidity `infra_no_account`),
        handled as a **soft** liquidity guard with a small penalty.

Extra outputs (for logs / dashboards / backtests)
-------------------------------------------------
`score_only(c)` returns a dict with:

    {
      "ok_hard": bool,
      "hard_reasons": [...],
      "parts": {component -> 0..1},
      "whale_bonus": float,
      "safety_bonus": float,
      "infra_penalty": float,
      "raw_score": float,
      "score": float,
      "entry_quality": float,          # 0..1.5
      "size_scalar": float,            # 0..1
      "entry_block_flag": float,       # 0 or 1
      "risk_capacity": float,          # 0..1
      "gate_threshold": float,
      "entry_tier": float,             # 0..3
      "position_size_tier": float,     # 0..3
      "source": str | None,
      "infra_no_account": bool,
    }

`gate_feature_vector(feats)` writes the key outputs back into the feature dict:

    feats["ai_buy_score"]
    feats["ai_buy_ok_hard"]
    feats["ai_buy_pass_gate"]
    feats["ai_buy_reason"]
    feats["ai_buy_gate_threshold"]
    feats["ai_buy_size_scalar"]

V3+++ additions vs older generations
------------------------------------
- Smart-wallet + whale-alignment composite (`smart_whale_align_component`).
- Micro-cap sweet-spot term (`mcap` component).
- Explicit `entry_quality` and `size_scalar` aligned with FeatureBuilder.
- Richer regime/session context (exposure frac, win rate, loss streak).
- FeatureBuilder tier awareness (entry_tier_bucket / position_size_bucket).
- Flow regime & whale footprint awareness (accum / panic-dump / footprint).
- Buy-pressure component aligned with FeatureBuilder’s `buy_pressure_score`.
- Structural quality & entry_risk_score hard guards (env-driven).
- Risk capacity & ai_risk_model overlay (`ai_capacity_delta`,
  `regime_entry_gate_factor`) to bias size & gate threshold.
- Optional self-learning nudges via Candidate.meta
  (`sl_entry_boost`, `sl_entry_gate_offset`, `sl_entry_block`, etc.).
- **Infra-aware pump.fun handling**:
  - `infra_no_account` propagated into Candidate (from features / snapshots).
  - For Pump-only, low-liquidity hard filter is softened when `infra_no_account`
    is true (no hard fail, just softer trust).
  - A small AI penalty is applied when `infra_no_account` is set (Pump-only).

V3+++ test-mode addition
------------------------
- Env flag `AI_PRE_BYPASS_FOR_TEST`:
    * When =1 and hard filters pass, we **bypass the score gate** and always
      return ok=True so we can verify:
          scanner → buy → position tracking → exit engine → Telegram
    * Hard filters (honeypot, extreme fraud, etc.) still apply.
"""

import json
import math
import os
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

from logging_setup import get_logger

log = get_logger(__name__)

__all__ = [
    "Candidate",
    "pass_gate",
    "score_only",
    "from_pool_snapshot",
    "from_feature_vector",
    "gate_feature_vector",
]


# =============================================================================
# Env helpers / utils
# =============================================================================


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
    if v is None:
        return default
    v = v.strip().lower()
    if v in ("1", "true", "yes", "y", "on"):
        return True
    if v in ("0", "false", "no", "n", "off"):
        return False
    return default


def _env_json(name: str) -> Optional[dict]:
    raw = os.getenv(name)
    if not raw:
        return None
    try:
        return json.loads(raw)
    except Exception:
        log.warning("ai_buy_scoring: could not parse JSON from %s", name)
        return None


def _clamp(x: float, lo: float = 0.0, hi: float = 1.0) -> float:
    return max(lo, min(hi, float(x)))


def _safe_div(n: float, d: float, fallback: float = 0.0) -> float:
    try:
        if d == 0:
            return fallback
        return float(n) / float(d)
    except Exception:
        return fallback


def _nz(x: Optional[float], fallback: Optional[float] = 0.0) -> float:
    if x is None:
        return float(fallback or 0.0)
    try:
        return float(x)
    except Exception:
        return float(fallback or 0.0)


def _sigmoid(x: float) -> float:
    try:
        return 1.0 / (1.0 + math.exp(-x))
    except OverflowError:
        return 1.0 if x > 0 else 0.0


def _is_pump_source(source: Optional[str]) -> bool:
    """
    Helper to detect Pump / pump.fun style sources.
    We keep this loose so it works for "pump", "pumpfun", "pf_pump", etc.
    """
    if not source:
        return False
    key = str(source).lower()
    return "pump" in key


# =============================================================================
# Candidate schema (V3+++)
# =============================================================================


@dataclass
class Candidate:
    # Core fields (required)
    mint: str
    symbol: str

    pool_age_min: float                  # minutes since pool creation
    liq_usd: float                       # pool liquidity (USD)
    impact_1k_bps: float                 # price impact (bps) for $1k swap
    vol5m_pct: float                     # 5m realized vol (%) or 5m % change magnitude
    top10_pct: float                     # % supply held by top 10 holders (0..100)
    honeypot_flag: bool                  # detected honeypot / denial risks
    whale_touches_1h: int                # # of unique whale wallets in last hour

    # --- Optional enrichments (all default-safe) ----------------------- #
    # Basic structure / safety
    verified: Optional[bool] = None
    liq_locked: Optional[bool] = None
    mint_renounced: Optional[bool] = None
    freeze_auth_disabled: Optional[bool] = None
    dev_wallet_pct: Optional[float] = None

    # Momentum / returns
    ret5m_pct: Optional[float] = None
    ret15m_pct: Optional[float] = None

    # Order flow (last 1m)
    orderflow_buys_1m: Optional[int] = None
    orderflow_sells_1m: Optional[int] = None
    orderflow_buy_usd_1m: Optional[float] = None
    orderflow_sell_usd_1m: Optional[float] = None

    # Orderbook depth snapshot (USD)
    orderbook_bid_usd: Optional[float] = None
    orderbook_ask_usd: Optional[float] = None

    # External safety
    rugpull_reports: Optional[int] = None

    # Extended structure / flow
    mcap_usd: Optional[float] = None
    vol_5m_usd: Optional[float] = None
    buys_5m: Optional[int] = None
    sells_5m: Optional[int] = None

    # AI / PF scores (from PF / AI fraud module)
    ai_fraud_score: Optional[float] = None              # 0..1 or 0..100 (rug/fraud)
    pf_momentum_score: Optional[float] = None           # 0..1
    pf_final_score: Optional[float] = None              # 0..1
    pf_risk_adjusted_final: Optional[float] = None      # 0..1

    # Whale / holders microstructure
    whale_share_1h: Optional[float] = None              # 0..1
    unique_buyers_1h: Optional[int] = None

    # Smart-wallet / whale alignment (from FeatureBuilder / smart_wallets)
    smart_wallet_score: Optional[float] = None          # 0..1
    smart_flow_raw: Optional[float] = None              # signed [-1,1]-ish
    smart_whale_alignment: Optional[float] = None       # 0..1.5
    unique_buyers_5m: Optional[float] = None            # decayed breadth
    whale_share_5m: Optional[float] = None              # 0..1
    whales_5m: Optional[float] = None

    # Flow regime & buy-pressure (from FeatureBuilder)
    buy_pressure_score: Optional[float] = None          # 0..1.5
    flow_accumulation_score: Optional[float] = None     # 0..1
    flow_panic_dump_score: Optional[float] = None       # 0..1
    flow_conflict_score: Optional[float] = None         # 0..1
    whale_liquidity_footprint: Optional[float] = None   # 0..1

    # High-level entry features (from FeatureBuilder)
    entry_quality_score: Optional[float] = None         # 0..1.5
    position_size_scalar: Optional[float] = None        # 0..1
    entry_block_flag: Optional[float] = None            # 0 or 1
    entry_micro_alpha: Optional[float] = None           # 0..1.5
    structural_quality_score: Optional[float] = None    # 0..1.5
    is_dead_like: Optional[float] = None                # 0 or 1
    risk_capacity_score: Optional[float] = None         # 0..1
    entry_risk_score: Optional[float] = None            # 0..1
    entry_tier_bucket: Optional[float] = None           # 0..3
    position_size_bucket: Optional[float] = None        # 0..3

    # AI entry model outputs (optional, from ai_entry_logic / external model)
    ai_entry_prob: Optional[float] = None               # 0..1
    ai_entry_score: Optional[float] = None              # 0..1

    # Regime & session context
    regime_trend: Optional[float] = None                # -1..1
    regime_liquidity: Optional[float] = None            # 0..1
    regime_manipulation_risk: Optional[float] = None    # 0..1
    regime_score: Optional[float] = None                # -1..1 (from FeatureBuilder)
    regime_volatility: Optional[float] = None           # 0..1 (from FeatureBuilder)

    ctx_open_positions: Optional[int] = None
    ctx_max_positions: Optional[int] = None
    ctx_daily_drawdown_pct: Optional[float] = None      # negative = loss
    ctx_daily_pnl_pct: Optional[float] = None
    ctx_risk_mode: Optional[float] = None               # 0=off,1=normal,2=agg,3=defensive

    # Extra risk context (aligned with risk_manager / FeatureBuilder)
    ctx_exposure_frac: Optional[float] = None           # 0..1 (current exposure / equity)
    ctx_exposure_usd: Optional[float] = None            # absolute exposure
    ctx_win_rate_pct: Optional[float] = None            # rolling win rate (%)
    ctx_recent_loss_streak: Optional[int] = None        # consecutive loss count

    # Optional risk-profile tags from FeatureBuilder / AI
    risk_profile_conservative: Optional[float] = None   # 0 or 1
    risk_profile_balanced: Optional[float] = None       # 0 or 1
    risk_profile_aggressive: Optional[float] = None     # 0 or 1

    # ai_risk_model overlays (optional)
    ai_capacity_delta: Optional[float] = None           # -0.6..+0.6
    regime_entry_gate_factor: Optional[float] = None    # 0.2..1.6
    ai_risk_profile_label: Optional[str] = None         # "defensive" / "aggressive" / ...

    # Self-learning overrides (optional; from self_learning_ai_strategy)
    self_learn_entry_boost: Optional[float] = None      # -1..+1 (score adjustment)
    self_learn_entry_gate_offset: Optional[float] = None  # additive gate offset
    self_learn_entry_block: Optional[float] = None      # 0 or 1 (hard block hint)

    # Source hint (e.g. "raydium", "pumpfun", "momo", etc.)
    source: Optional[str] = None

    # Misc / future extensions (e.g. extra self-learning data)
    meta: Dict[str, Any] = field(default_factory=dict)

    # Infra flags (e.g. liquidity infra "no_account", mostly Pump / pump.fun)
    infra_no_account: Optional[bool] = None


# =============================================================================
# Env-driven thresholds & weights
# =============================================================================

# --- Hard filters ----------------------------------------------------- #
AI_MIN_POOL_AGE_MIN = _env_float("AI_MIN_POOL_AGE_MIN", 3.0)         # too fresh ⇒ fail
AI_MIN_LIQ_USD = _env_float("AI_MIN_LIQ_USD", 15_000.0)
AI_MAX_IMPACT_1K_BPS = _env_float("AI_MAX_IMPACT_1K_BPS", 125.0)
AI_MAX_TOP10_PCT = _env_float("AI_MAX_TOP10_PCT", 70.0)
AI_MIN_VOL5M_PCT = _env_float("AI_MIN_VOL5M_PCT", 0.8)
AI_MAX_DEV_WALLET_PCT = _env_float("AI_MAX_DEV_WALLET_PCT", 20.0)
AI_MIN_WHALE_TOUCHES_1H = _env_int("AI_MIN_WHALE_TOUCHES_1H", 0)

AI_REQUIRE_LIQ_LOCK = _env_bool("AI_REQUIRE_LIQ_LOCK", False)
AI_REQUIRE_MINT_RENOUNCED = _env_bool("AI_REQUIRE_MINT_RENOUNCED", False)
AI_REQUIRE_NO_FREEZE = _env_bool("AI_REQUIRE_NO_FREEZE", False)

# AI / PF / fraud / AI-entry guards
AI_MAX_RUG_SCORE = _env_float("AI_MAX_RUG_SCORE", 0.85)        # 0..1 or 0..100
AI_MIN_PF_FINAL = _env_float("AI_MIN_PF_FINAL", 0.08)
AI_MIN_PF_RISK_ADJ = _env_float("AI_MIN_PF_RISK_ADJ", 0.06)
AI_MIN_AI_ENTRY_PROB = _env_float("AI_MIN_AI_ENTRY_PROB", 0.0)  # 0 disables

# Entry-quality / risk guards synced with FeatureBuilder
AI_MIN_ENTRY_QUALITY = _env_float("AI_MIN_ENTRY_QUALITY", 0.0)      # 0 to disable
AI_MIN_STRUCTURAL_QUALITY = _env_float("AI_MIN_STRUCTURAL_QUALITY", 0.0)
AI_MAX_ENTRY_RISK_SCORE = _env_float("AI_MAX_ENTRY_RISK_SCORE", 1.0)  # 1.0 = no cap

# Regime & session tightening (aligned with ai_entry_logic / ai_risk_model)
AI_REGIME_TIGHTEN_ENABLED = _env_bool("AI_REGIME_TIGHTEN_ENABLED", True)
AI_RISK_OFF_EXTRA_MARGIN = _env_float("AI_RISK_OFF_EXTRA_MARGIN", 0.08)
AI_MANIP_HIGH = _env_float("AI_MANIP_HIGH", 0.75)

AI_CTX_MAX_DRAWDOWN_PCT = abs(_env_float("AI_CTX_MAX_DRAWDOWN_PCT", 15.0))
AI_CTX_MAX_OPEN_POS_RATIO = _env_float("AI_CTX_MAX_OPEN_POS_RATIO", 1.0)
AI_CTX_LOCK_AFTER_TARGET_PNL = _env_float("AI_CTX_LOCK_AFTER_TARGET_PNL", 0.0)

# FeatureBuilder-aligned guards
AI_USE_ENTRY_BLOCK_FLAG = _env_bool("AI_USE_ENTRY_BLOCK_FLAG", True)
AI_USE_RISK_CAPACITY_GUARD = _env_bool("AI_USE_RISK_CAPACITY_GUARD", True)
AI_MIN_RISK_CAPACITY = _env_float("AI_MIN_RISK_CAPACITY", 0.02)     # minimal room
AI_MIN_ENTRY_TIER = _env_float("AI_MIN_ENTRY_TIER", 0.0)            # 0..3 (0 disables)

# Self-learning overrides
AI_BUY_SELF_LEARN_ENABLED = _env_bool("AI_BUY_SELF_LEARN_ENABLED", True)
AI_BUY_SELF_LEARN_MAX_ABS_BOOST = _env_float(
    "AI_BUY_SELF_LEARN_MAX_ABS_BOOST", 0.12
)  # max |score boost|
AI_BUY_SELF_LEARN_MAX_ABS_GATE = _env_float(
    "AI_BUY_SELF_LEARN_MAX_ABS_GATE", 0.10
)  # max |gate +/-|
AI_BUY_SELF_LEARN_GATE_WEIGHT = _env_float(
    "AI_BUY_SELF_LEARN_GATE_WEIGHT", 1.0
)  # 1.0 = full; 0.5 = half

# Regime gate factor usage (from ai_risk_model)
AI_USE_REGIME_GATE_FACTOR = _env_bool("AI_USE_REGIME_GATE_FACTOR", True)
AI_REGIME_GATE_FACTOR_STRENGTH = _env_float("AI_REGIME_GATE_FACTOR_STRENGTH", 0.30)

# Infra / Pump-specific controls
AI_INFRA_NO_ACCOUNT_SOFT_LIQ = _env_bool(
    "AI_INFRA_NO_ACCOUNT_SOFT_LIQ", True
)  # if True: for Pump+infra_no_account, do NOT hard-fail low_liq
AI_INFRA_NO_ACCOUNT_PENALTY = _env_float(
    "AI_INFRA_NO_ACCOUNT_PENALTY", -0.03
)  # small global score penalty when infra_no_account (Pump-only)

# --- Soft score gate -------------------------------------------------- #
AI_SCORE_GATE = _env_float("AI_SCORE_GATE", 0.58)

# Optional per-source gate overrides:
#   AI_SCORE_GATE_OVERRIDES_JSON='{"pumpfun":0.62,"raydium":0.55,"momo":0.52}'
AI_SCORE_GATE_OVERRIDES = _env_json("AI_SCORE_GATE_OVERRIDES_JSON") or {}

# Optional per-source *additive* score nudges:
#   AI_SCORE_SOURCE_BONUS_JSON='{"pumpfun":-0.02,"raydium":0.01}'
AI_SCORE_SOURCE_BONUS = _env_json("AI_SCORE_SOURCE_BONUS_JSON") or {}

# --- Component weights (not required to sum to 1) --------------------- #
W_POOL_AGE = _env_float("AI_W_POOL_AGE", 0.07)
W_LIQ = _env_float("AI_W_LIQ", 0.16)
W_IMPACT = _env_float("AI_W_IMPACT", 0.10)
W_VOL5M = _env_float("AI_W_VOL5M", 0.07)
W_TOP10 = _env_float("AI_W_TOP10", 0.08)
W_WHALES = _env_float("AI_W_WHALES", 0.08)
W_TREND = _env_float("AI_W_TREND", 0.10)
W_FLOW = _env_float("AI_W_FLOW", 0.09)
W_BOOK = _env_float("AI_W_BOOK", 0.05)
W_SAFETY = _env_float("AI_W_SAFETY", 0.05)
W_PF = _env_float("AI_W_PF", 0.09)
W_FRAUD = _env_float("AI_W_FRAUD", 0.11)
W_REGIME_CTX = _env_float("AI_W_REGIME_CTX", 0.07)

# New V3+++ components
W_ENTRY_QUALITY = _env_float("AI_W_ENTRY_QUALITY", 0.12)
W_SMART_WHALE_ALIGN = _env_float("AI_W_SMART_WHALE_ALIGN", 0.08)
W_MCAP = _env_float("AI_W_MCAP", 0.06)            # mcap sweet-spot term
W_AI_ENTRY = _env_float("AI_W_AI_ENTRY", 0.08)    # AI entry prob/score
W_TIER = _env_float("AI_W_TIER", 0.05)            # tier-based quality
W_BUY_PRESS = _env_float("AI_W_BUY_PRESS", 0.06)  # buy-pressure component

# Normalization so we get a stable 0..1-ish score range even if weights change.
_WEIGHT_SUM = (
    W_POOL_AGE
    + W_LIQ
    + W_IMPACT
    + W_VOL5M
    + W_TOP10
    + W_WHALES
    + W_TREND
    + W_FLOW
    + W_BOOK
    + W_SAFETY
    + W_PF
    + W_FRAUD
    + W_REGIME_CTX
    + W_ENTRY_QUALITY
    + W_SMART_WHALE_ALIGN
    + W_MCAP
    + W_AI_ENTRY
    + W_TIER
    + W_BUY_PRESS
)
AI_WEIGHT_NORM = _env_float("AI_WEIGHT_NORM", max(_WEIGHT_SUM, 1.0))

# --- Bonuses / penalties ---------------------------------------------- #
BONUS_VERIFIED = _env_float("AI_BONUS_VERIFIED", 0.03)
PENALTY_UNVERIFIED = _env_float("AI_PENALTY_UNVERIFIED", -0.05)
BONUS_LIQ_LOCK = _env_float("AI_BONUS_LIQ_LOCK", 0.03)
BONUS_RENOUNCED = _env_float("AI_BONUS_RENOUNCED", 0.02)
BONUS_NO_FREEZE = _env_float("AI_BONUS_NO_FREEZE", 0.02)
PENALTY_RUG_REPORT = _env_float("AI_PENALTY_RUG_REPORT", -0.02)  # per report, capped

WHALE_TOUCH_BONUS = _env_float("AI_WHALE_TOUCH_BONUS", 0.03)
WHALE_TOUCH_BONUS_CAP = _env_float("AI_WHALE_TOUCH_BONUS_CAP", 0.15)

RUG_REPORT_CAP = _env_int("AI_RUG_REPORT_CAP", 5)

# --- Normalization anchors ------------------------------------------- #
ANCHOR_POOL_AGE = _env_float("AI_ANCHOR_POOL_AGE", 120.0)        # 2h saturates
ANCHOR_LIQ = _env_float("AI_ANCHOR_LIQ", 100_000.0)              # $100k saturates
ANCHOR_IMPACT_BPS = _env_float("AI_ANCHOR_IMPACT_BPS", 150.0)   # 150bps → 0
ANCHOR_VOL5M = _env_float("AI_ANCHOR_VOL5M", 5.0)               # 5% vol saturates
ANCHOR_TOP10_LO = _env_float("AI_ANCHOR_TOP10_GOOD", 20.0)
ANCHOR_TOP10_HI = _env_float("AI_ANCHOR_TOP10_BAD", 90.0)
ANCHOR_FLOW_IMB = _env_float("AI_ANCHOR_FLOW_IMB", 0.65)        # ideal buy share
ANCHOR_BOOK_IMB = _env_float("AI_ANCHOR_BOOK_IMB", 0.60)
ANCHOR_MCAP = _env_float("AI_ANCHOR_MCAP", 250_000.0)           # $250k mid-micro

# Momentum clamps (avoid manic chasing / catching knives)
RET5M_POS_CEIL = _env_float("AI_RET5M_POS_CEIL", 9.0)
RET5M_NEG_FLOOR = _env_float("AI_RET5M_NEG_FLOOR", -9.0)
RET15M_POS_CEIL = _env_float("AI_RET15M_POS_CEIL", 18.0)
RET15M_NEG_FLOOR = _env_float("AI_RET15M_NEG_FLOOR", -18.0)

# Position sizing transform
AI_SIZE_SCALAR_EXP = _env_float("AI_SIZE_SCALAR_EXP", 1.0)  # 1.0 = linear

AI_BUY_DEBUG_LOG = _env_bool("AI_BUY_DEBUG_LOG", False)
AI_BUY_ATTACH_PARTS = _env_bool("AI_BUY_ATTACH_PARTS", False)

# --- Test-mode soft-gate bypass -------------------------------------- #
# If set to 1 and hard filters pass, pass_gate() *always* returns ok=True.
# This is intended ONLY for DRYRUN / paper-testing end-to-end wiring.
AI_PRE_BYPASS_FOR_TEST = _env_bool("AI_PRE_BYPASS_FOR_TEST", False)


# =============================================================================
# Hard filters
# =============================================================================


def _hard_filters(c: Candidate) -> Tuple[bool, List[str]]:
    reasons: List[str] = []

    # Basic structural
    if c.pool_age_min < AI_MIN_POOL_AGE_MIN:
        reasons.append(
            f"pool_too_new({c.pool_age_min:.1f}m < {AI_MIN_POOL_AGE_MIN:.1f}m)"
        )

    # Liquidity guard with Pump + infra_no_account softening
    if c.liq_usd < AI_MIN_LIQ_USD:
        if (
            AI_INFRA_NO_ACCOUNT_SOFT_LIQ
            and bool(c.infra_no_account)
            and _is_pump_source(c.source)
        ):
            # Infra "no_account" for Pump: treat as soft warning, not hard fail.
            # The candidate will still get a small global penalty in the score,
            # but we do NOT reject purely on low_liq when infra says "no_account".
            # (We intentionally do NOT append a hard reason here.)
            pass
        else:
            reasons.append(
                f"low_liq(${c.liq_usd:,.0f} < ${AI_MIN_LIQ_USD:,.0f})"
            )

    if c.impact_1k_bps > AI_MAX_IMPACT_1K_BPS:
        reasons.append(
            f"high_impact({c.impact_1k_bps:.0f}bps > {AI_MAX_IMPACT_1K_BPS:.0f}bps)"
        )

    if c.top10_pct > AI_MAX_TOP10_PCT:
        reasons.append(
            f"top10_concentration({c.top10_pct:.1f}% > {AI_MAX_TOP10_PCT:.1f}%)"
        )

    if c.vol5m_pct < AI_MIN_VOL5M_PCT:
        reasons.append(
            f"too_quiet(vol5m={c.vol5m_pct:.2f}% < {AI_MIN_VOL5M_PCT:.2f}%)"
        )

    if c.honeypot_flag:
        reasons.append("honeypot_flag")

    if c.dev_wallet_pct is not None and c.dev_wallet_pct > AI_MAX_DEV_WALLET_PCT:
        reasons.append(
            f"dev_wallet_too_big({c.dev_wallet_pct:.1f}% > {AI_MAX_DEV_WALLET_PCT:.1f}%)"
        )

    if AI_MIN_WHALE_TOUCHES_1H > 0 and c.whale_touches_1h < AI_MIN_WHALE_TOUCHES_1H:
        reasons.append(
            f"few_whale_touches({c.whale_touches_1h} < {AI_MIN_WHALE_TOUCHES_1H})"
        )

    if AI_REQUIRE_LIQ_LOCK and not bool(c.liq_locked):
        reasons.append("liq_not_locked")

    if AI_REQUIRE_MINT_RENOUNCED and not bool(c.mint_renounced):
        reasons.append("mint_not_renounced")

    if AI_REQUIRE_NO_FREEZE and not bool(c.freeze_auth_disabled):
        reasons.append("freeze_authority_enabled")

    # Fraud / rug cutoff
    if c.ai_fraud_score is not None:
        fraud = _nz(c.ai_fraud_score)
        if fraud > 1.5:  # allow 0..100 inputs
            fraud = fraud / 100.0
        fraud = _clamp(fraud)
        if fraud >= AI_MAX_RUG_SCORE:
            reasons.append(f"high_fraud_score({fraud:.2f}>={AI_MAX_RUG_SCORE:.2f})")

    # PF floors as hard reject (only if scores present)
    if c.pf_final_score is not None and c.pf_final_score < AI_MIN_PF_FINAL:
        reasons.append(
            f"pf_final_below_min({c.pf_final_score:.3f} < {AI_MIN_PF_FINAL:.3f})"
        )

    if (
        c.pf_risk_adjusted_final is not None
        and c.pf_risk_adjusted_final < AI_MIN_PF_RISK_ADJ
    ):
        reasons.append(
            "pf_risk_adj_below_min("
            f"{c.pf_risk_adjusted_final:.3f} < {AI_MIN_PF_RISK_ADJ:.3f})"
        )

    # AI-entry model minimum probability (optional)
    if AI_MIN_AI_ENTRY_PROB > 0.0 and c.ai_entry_prob is not None:
        if _nz(c.ai_entry_prob, 0.0) < AI_MIN_AI_ENTRY_PROB:
            reasons.append(
                f"ai_entry_prob_low({c.ai_entry_prob:.3f}"
                f"<{AI_MIN_AI_ENTRY_PROB:.3f})"
            )

    # FeatureBuilder dead-coin & entry block
    if AI_USE_ENTRY_BLOCK_FLAG:
        if _nz(c.entry_block_flag, 0.0) >= 0.5:
            reasons.append("entry_block_flag_from_features")

    if _nz(c.is_dead_like, 0.0) >= 1.0:
        reasons.append("dead_like_heuristic")

    # FeatureBuilder entry tier guard (optional)
    if AI_MIN_ENTRY_TIER > 0.0 and c.entry_tier_bucket is not None:
        if _nz(c.entry_tier_bucket, 0.0) < AI_MIN_ENTRY_TIER:
            reasons.append(
                f"entry_tier_low({c.entry_tier_bucket:.1f} < {AI_MIN_ENTRY_TIER:.1f})"
            )

    # Structural quality floor (optional)
    if AI_MIN_STRUCTURAL_QUALITY > 0.0 and c.structural_quality_score is not None:
        if _nz(c.structural_quality_score, 0.0) < AI_MIN_STRUCTURAL_QUALITY:
            reasons.append(
                "low_structural_quality("
                f"{c.structural_quality_score:.3f}<"
                f"{AI_MIN_STRUCTURAL_QUALITY:.3f})"
            )

    # Entry risk cap (optional)
    if AI_MAX_ENTRY_RISK_SCORE < 1.0 and c.entry_risk_score is not None:
        if _nz(c.entry_risk_score, 0.0) > AI_MAX_ENTRY_RISK_SCORE:
            reasons.append(
                "entry_risk_too_high("
                f"{c.entry_risk_score:.3f}>"
                f"{AI_MAX_ENTRY_RISK_SCORE:.3f})"
            )

    # Session / book risk
    if AI_CTX_MAX_DRAWDOWN_PCT > 0 and c.ctx_daily_drawdown_pct is not None:
        if c.ctx_daily_drawdown_pct <= -AI_CTX_MAX_DRAWDOWN_PCT:
            reasons.append(
                f"session_drawdown({c.ctx_daily_drawdown_pct:.2f}% <= "
                f"-{AI_CTX_MAX_DRAWDOWN_PCT:.1f}%)"
            )

    if (
        AI_CTX_LOCK_AFTER_TARGET_PNL > 0
        and c.ctx_daily_pnl_pct is not None
        and c.ctx_risk_mode is not None
    ):
        if c.ctx_daily_pnl_pct >= AI_CTX_LOCK_AFTER_TARGET_PNL and c.ctx_risk_mode <= 1.0:
            reasons.append(
                f"session_target_hit({c.ctx_daily_pnl_pct:.2f}% >= "
                f"{AI_CTX_LOCK_AFTER_TARGET_PNL:.2f}%)"
            )

    if (
        AI_CTX_MAX_OPEN_POS_RATIO > 0
        and c.ctx_open_positions is not None
        and c.ctx_max_positions is not None
        and c.ctx_max_positions > 0
    ):
        ratio = c.ctx_open_positions / c.ctx_max_positions
        if ratio >= AI_CTX_MAX_OPEN_POS_RATIO:
            reasons.append(
                "too_many_positions("
                f"{c.ctx_open_positions}/{c.ctx_max_positions},"
                f" ratio={ratio:.2f}>={AI_CTX_MAX_OPEN_POS_RATIO:.2f})"
            )

    # Explicit risk capacity guard (from FeatureBuilder / risk_manager)
    if AI_USE_RISK_CAPACITY_GUARD and c.risk_capacity_score is not None:
        if _nz(c.risk_capacity_score, 0.0) < AI_MIN_RISK_CAPACITY:
            reasons.append(
                f"no_risk_capacity({c.risk_capacity_score:.3f}<"
                f"{AI_MIN_RISK_CAPACITY:.3f})"
            )

    # Entry quality floor (optional, separate from final gate)
    if AI_MIN_ENTRY_QUALITY > 0.0 and c.entry_quality_score is not None:
        if _nz(c.entry_quality_score, 0.0) < AI_MIN_ENTRY_QUALITY:
            reasons.append(
                f"low_entry_quality({c.entry_quality_score:.3f}"
                f"<{AI_MIN_ENTRY_QUALITY:.3f})"
            )

    # Self-learning explicit block (if provided)
    if AI_BUY_SELF_LEARN_ENABLED:
        sl_block = _nz(c.self_learn_entry_block, 0.0)
        if sl_block >= 0.5:
            reasons.append("self_learning_block")

        # meta-based block
        slm = c.meta or {}
        meta_block = _nz(slm.get("sl_entry_block"), 0.0)
        if meta_block >= 0.5 and "self_learning_block" not in reasons:
            reasons.append("self_learning_block_meta")

    return len(reasons) == 0, reasons


# =============================================================================
# Soft scoring components
# =============================================================================


def _score_pool_age(mins: float) -> float:
    return _clamp(_nz(mins) / max(ANCHOR_POOL_AGE, 1e-6))


def _score_liq(usd: float) -> float:
    return _clamp(_nz(usd) / max(ANCHOR_LIQ, 1e-6))


def _score_impact(bps: float) -> float:
    s = 1.0 - _clamp(_nz(bps) / max(ANCHOR_IMPACT_BPS, 1e-6))
    return _clamp(s)


def _score_vol5m(vol_pct: float) -> float:
    return _clamp(_nz(vol_pct) / max(ANCHOR_VOL5M, 1e-6))


def _score_top10(concentration_pct: float) -> float:
    x = _nz(concentration_pct)
    if x <= ANCHOR_TOP10_LO:
        return 1.0
    if x >= ANCHOR_TOP10_HI:
        return 0.0
    return _clamp(
        1.0 - (x - ANCHOR_TOP10_LO) / max(ANCHOR_TOP10_HI - ANCHOR_TOP10_LO, 1e-6)
    )


def _score_whales(touches: int, whale_share: Optional[float]) -> float:
    base = _clamp(_nz(touches) / 5.0)  # 5+ touches saturates
    share = _clamp(_nz(whale_share, 0.3))
    # punish extreme domination, reward healthy presence
    if share > 0.75:
        base -= (share - 0.75) * 1.0
    else:
        base += share * 0.3
    return _clamp(base)


def _score_mcap(mcap_usd: Optional[float]) -> float:
    """
    Simple micro-cap sweet-spot scoring:
      - Very tiny mcap => low (rugland)
      - Mid micro (~ANCHOR_MCAP) => best
      - Very large => slightly decays (late stage)
    """
    m = _nz(mcap_usd, 0.0)
    if m <= 0:
        return 0.3
    # Normalize around anchor, then turn into bell-ish curve in [0,1]
    x = m / max(ANCHOR_MCAP, 1e-6)
    # x in (0, inf) -> z in (-inf, inf)
    z = math.log(max(x, 1e-6))
    # Squash: peak at z=0, width ~1.2
    s = math.exp(-0.5 * (z / 1.2) ** 2)
    return _clamp(s)


def _tier_component(entry_tier: Optional[float], size_tier: Optional[float]) -> float:
    """
    Tier component based on FeatureBuilder's discrete buckets:
      - entry_tier_bucket:   0=ignore,1=small,2=medium,3=aggressive
      - position_size_bucket: same scale
    We treat them as soft hints; they should be consistent with the
    underlying entry_quality and size_scalar, but add stability.
    """
    e = _nz(entry_tier, 0.0)
    s = _nz(size_tier, 0.0)

    # Normalize 0..3 → 0..1
    e_norm = _clamp(e / 3.0, 0.0, 1.0)
    s_norm = _clamp(s / 3.0, 0.0, 1.0)

    # Slightly prefer when both tiers agree
    align = 0.5 * (e_norm + s_norm)
    if e > 0 and s > 0:
        align += 0.15 * min(e_norm, s_norm)

    return _clamp(align)


def _trend_component(ret5m: Optional[float], ret15m: Optional[float]) -> float:
    r5 = _nz(ret5m)
    r15 = _nz(ret15m)

    # avoid manic chase / knife catches
    if r5 >= RET5M_POS_CEIL or r15 >= RET15M_POS_CEIL:
        return 0.25
    if r5 <= RET5M_NEG_FLOOR or r15 <= RET15M_NEG_FLOOR:
        return 0.20

    def softstep(r: float, width: float) -> float:
        # map [-width, +width] → [0,1]
        return _clamp((r + width) / (2.0 * width))

    s5 = softstep(r5, 6.0)
    s15 = softstep(r15, 10.0)
    return _clamp(0.6 * s5 + 0.4 * s15)


def _flow_component(
    buys: Optional[int],
    sells: Optional[int],
    buy_usd: Optional[float],
    sell_usd: Optional[float],
    vol_5m_usd: Optional[float],
    flow_accum: Optional[float],
    flow_panic: Optional[float],
) -> float:
    """
    Orderflow component blended with flow regime (accum vs panic dump).
    """
    b = _nz(buys)
    s = _nz(sells)
    bu = _nz(buy_usd)
    su = _nz(sell_usd)
    v5 = _nz(vol_5m_usd)

    cnt_total = b + s
    notional_total = bu + su

    cnt_imb = _safe_div(b, cnt_total, 0.5)
    not_imb = _safe_div(bu, notional_total, 0.5)
    share = 0.5 * cnt_imb + 0.5 * not_imb

    # volume energy (encourage some but not crazy)
    vol_energy = _clamp(v5 / 8_000.0)  # $8k/5m saturates

    diff = abs(share - ANCHOR_FLOW_IMB)
    base = _clamp(1.0 - diff / max(ANCHOR_FLOW_IMB, 1 - ANCHOR_FLOW_IMB))

    # Flow regime shading from FeatureBuilder
    fa = _clamp(_nz(flow_accum, 0.0))
    fd = _clamp(_nz(flow_panic, 0.0))

    base = _clamp(base + 0.15 * fa - 0.20 * fd)

    return _clamp(0.7 * base + 0.3 * vol_energy)


def _book_component(bid_usd: Optional[float], ask_usd: Optional[float]) -> float:
    bid = _nz(bid_usd)
    ask = _nz(ask_usd)
    tot = bid + ask
    if tot <= 0:
        return 0.5
    bid_share = bid / tot
    diff = abs(bid_share - ANCHOR_BOOK_IMB)
    return _clamp(1.0 - diff / max(ANCHOR_BOOK_IMB, 1 - ANCHOR_BOOK_IMB))


def _safety_component(
    verified: Optional[bool],
    liq_locked: Optional[bool],
    mint_renounced: Optional[bool],
    freeze_disabled: Optional[bool],
    rug_reports: Optional[int],
) -> Tuple[float, float]:
    """
    Returns: (base_safety_score 0..1, additive_bonus in [-, +])
    """
    base = 0.5
    bonus = 0.0

    if verified is True:
        bonus += BONUS_VERIFIED
        base += 0.12
    elif verified is False:
        bonus += PENALTY_UNVERIFIED
        base -= 0.10

    if liq_locked is True:
        bonus += BONUS_LIQ_LOCK
        base += 0.08

    if mint_renounced is True:
        bonus += BONUS_RENOUNCED
        base += 0.05

    if freeze_disabled is True:
        bonus += BONUS_NO_FREEZE
        base += 0.05

    rr = int(rug_reports or 0)
    if rr > 0:
        bonus += max(-RUG_REPORT_CAP * abs(PENALTY_RUG_REPORT), rr * PENALTY_RUG_REPORT)
        base -= min(0.25, 0.04 * rr)

    return _clamp(base), bonus


def _pf_component(
    pf_mom: Optional[float],
    pf_final: Optional[float],
    pf_risk_adj: Optional[float],
) -> float:
    m = _clamp(_nz(pf_mom))
    f = _clamp(_nz(pf_final))
    r = _clamp(_nz(pf_risk_adj, f))
    if f == 0 and r == 0 and m == 0:
        return 0.0
    # treat as logit-style prior
    raw = 1.2 * r + 0.6 * f + 0.4 * m
    return _clamp(raw)


def _fraud_component(ai_fraud_score: Optional[float]) -> float:
    if ai_fraud_score is None:
        return 0.5
    f = _nz(ai_fraud_score)
    if f > 1.5:  # 0..100 input
        f = f / 100.0
    f = _clamp(f)
    # convert to [0,1] safety score
    return _clamp(1.0 - f)


def _regime_ctx_component(c: Candidate) -> float:
    """
    Rich context component that blends:
      - market regime (trend / liquidity / manipulation / volatility)
      - session state (drawdown, PnL, risk mode)
      - book state (exposure fraction, win rate, loss streak)
      - risk profile tags (conservative/balanced/aggressive)
      - ai_risk_model overlays (capacity / regime gate factor)
    """
    # Prefer explicit regime_score if present, then trend
    trend = _nz(c.regime_score, _nz(c.regime_trend, 0.0))   # -1..1
    trend = _clamp(trend, -1.0, 1.0)
    liq = _clamp(_nz(c.regime_liquidity, 0.5))              # 0..1
    manip = _clamp(_nz(c.regime_manipulation_risk, 0.0))    # 0..1
    vol = _clamp(_nz(c.regime_volatility, 0.5))             # 0..1

    ctx_dd = _nz(c.ctx_daily_drawdown_pct, 0.0)
    ctx_pnl = _nz(c.ctx_daily_pnl_pct, 0.0)
    ctx_risk = _nz(c.ctx_risk_mode, 1.0)

    exposure_frac = _clamp(_nz(c.ctx_exposure_frac, 0.0), 0.0, 1.0)
    win_rate = _clamp(_nz(c.ctx_win_rate_pct, 50.0) / 100.0, 0.0, 1.0)
    loss_streak = _nz(c.ctx_recent_loss_streak, 0.0)

    rp_cons = _clamp(_nz(c.risk_profile_conservative, 0.0), 0.0, 1.0)
    rp_bal = _clamp(_nz(c.risk_profile_balanced, 0.0), 0.0, 1.0)
    rp_agg = _clamp(_nz(c.risk_profile_aggressive, 0.0), 0.0, 1.0)

    cap_delta = _nz(c.ai_capacity_delta, 0.0)  # -0.6..+0.6
    reg_gate = _clamp(_nz(c.regime_entry_gate_factor, 1.0), 0.2, 1.6)

    raw = 0.0
    raw += trend * 0.32
    raw += (liq - 0.5) * 0.20
    raw -= max(0.0, manip - 0.4) * 0.7
    raw += (vol - 0.5) * 0.16  # moderate help in “alive” regimes, hurt in too dead

    # mild drag in drawdown, mild boost in hot streak (capped)
    if ctx_dd < 0:
        raw -= min(abs(ctx_dd) / 10.0, 1.5) * 0.18
    if 0.0 < ctx_pnl < 6.0:
        raw += min(ctx_pnl / 6.0, 1.0) * 0.10

    # risk mode
    if ctx_risk <= 0.0:
        raw -= 0.6           # "off" mode – strong drag
    elif ctx_risk == 3.0:
        raw -= 0.2           # defensive mode
    elif ctx_risk >= 2.0:
        raw += 0.10          # aggressive – minor boost

    # exposure: high exposure reduces context attractiveness
    raw -= exposure_frac * 0.25

    # win rate: persistent good performance gives a small boost
    raw += (win_rate - 0.5) * 0.20

    # loss streak: clamp negative impact
    if loss_streak > 0:
        raw -= min(loss_streak / 4.0, 1.0) * 0.22

    # risk-profile tags: conservative → slight drag, aggressive → slight boost
    raw -= 0.10 * rp_cons
    raw += 0.05 * rp_bal
    raw += 0.10 * rp_agg

    # capacity tilt: if capacity crushed, shade context negative; if expanded, mildly positive
    if cap_delta < 0.0:
        raw -= min(abs(cap_delta) / 0.6, 1.0) * 0.20
    elif cap_delta > 0.0:
        raw += min(cap_delta / 0.6, 1.0) * 0.10

    # regime gate factor: <1 means risk-off tilt; >1 means more permissive
    raw += (reg_gate - 1.0) * 0.15

    # squash to [0,1]
    return _sigmoid(raw)


def _entry_quality_component(c: Candidate) -> float:
    """
    Use FeatureBuilder's entry_quality_score when available.
    Fallback: reconstruct from structural_quality_score, pf_risk_adj, entry_micro.
    """
    if c.entry_quality_score is not None:
        return _clamp(_nz(c.entry_quality_score), 0.0, 1.5)

    structural = _clamp(_nz(c.structural_quality_score, 0.0), 0.0, 1.5)
    pf_risk_adj = _clamp(
        _nz(c.pf_risk_adjusted_final, _nz(c.pf_final_score, 0.0)), 0.0, 1.0
    )
    entry_micro = _clamp(_nz(c.entry_micro_alpha, 0.0), 0.0, 1.5)

    base_quality = (
        0.45 * structural +
        0.30 * pf_risk_adj +
        0.25 * entry_micro
    )
    return _clamp(base_quality, 0.0, 1.5)


def _smart_whale_align_component(c: Candidate) -> float:
    """
    Composite that prefers tokens where smart wallets + whales + breadth
    are aligned in a healthy way.
    """
    if c.smart_whale_alignment is not None:
        base_align = _clamp(_nz(c.smart_whale_alignment), 0.0, 1.5)
    else:
        smart = _clamp(_nz(c.smart_wallet_score, 0.5), 0.0, 1.0)
        flow_raw = _nz(c.smart_flow_raw, 0.0)
        # map [-1,1] -> [0,1]
        smart_bias = 0.5 * (_clamp(flow_raw, -1.0, 1.0) + 1.0)

        whales = _clamp(_nz(c.whales_5m, _nz(c.whale_touches_1h, 0.0)) / 5.0, 0.0, 1.0)
        whale_share = _clamp(_nz(c.whale_share_5m, _nz(c.whale_share_1h, 0.3)), 0.0, 1.0)
        buyers = _clamp(_nz(c.unique_buyers_5m, _nz(c.unique_buyers_1h, 0.0)) / 50.0, 0.0, 1.0)

        # Penalize hyper-concentrated whales
        if whale_share > 0.8:
            whale_term = max(0.0, whales * (1.1 - (whale_share - 0.8) * 2.0))
        else:
            whale_term = whales * (0.7 + 0.3 * whale_share)

        base_align = (
            0.40 * smart +
            0.25 * smart_bias +
            0.20 * whale_term +
            0.15 * buyers
        )

    base_align = _clamp(base_align, 0.0, 1.5)

    # Whale liquidity footprint: prefer controlled-but-liquid footprints
    wlf = _clamp(_nz(c.whale_liquidity_footprint, 0.0), 0.0, 1.0)
    if wlf > 0.0:
        base_align *= (0.9 + 0.2 * wlf)  # mild boost when whales are supporting liq

    return _clamp(base_align, 0.0, 1.5)


def _ai_entry_component(prob: Optional[float], score: Optional[float]) -> float:
    """
    Component for AI entry model. If probability is present, prefer it.
    Otherwise fall back to an AI score head. Both are assumed 0..1.
    """
    if prob is not None:
        return _clamp(_nz(prob), 0.0, 1.0)
    if score is not None:
        return _clamp(_nz(score), 0.0, 1.0)
    return 0.0


def _buy_pressure_component(buy_press: Optional[float]) -> float:
    """
    Turn FeatureBuilder's buy_pressure_score (0..1.5-ish) into 0..1.
    """
    return _clamp(_nz(buy_press, 0.0) / 1.5, 0.0, 1.0)


def _size_scalar_from_candidate(c: Candidate, entry_quality: float) -> float:
    """
    Position size scalar (0..1) derived from:
      - entry_quality (0..1.5)
      - risk_capacity_score (0..1)
      - ai_capacity_delta (ai_risk_model overlay)
      - regime (risk-on vs off)
      - risk mode / exposure / loss streak
      - FeatureBuilder-provided position_size_scalar (if present)
      - entry_tier_bucket / position_size_bucket as a sanity cross-check
      - self-learning hints (via meta) for small nudges

    This is advisory only; risk_manager can still override.
    """
    # Start from FeatureBuilder's suggestion if present
    if c.position_size_scalar is not None:
        base = _clamp(_nz(c.position_size_scalar), 0.0, 1.0)
    else:
        base = _clamp(entry_quality / 1.5, 0.0, 1.0)

    # Apply tiers as a soft guide (if present)
    if c.position_size_bucket is not None:
        tier_norm = _clamp(_nz(c.position_size_bucket, 0.0) / 3.0, 0.0, 1.0)
        # Blend: 80% from base, 20% from tier suggestion
        base = _clamp(0.8 * base + 0.2 * tier_norm, 0.0, 1.0)

    # Apply risk capacity if available
    cap = _clamp(_nz(c.risk_capacity_score, 1.0), 0.0, 1.0)
    size = base * cap

    # ai_capacity_delta: if capacity crushed, shrink; if positive, mild boost
    cap_delta = _nz(c.ai_capacity_delta, 0.0)  # -0.6..+0.6
    if cap_delta < 0:
        size *= (1.0 + max(cap_delta / 0.6, -1.0))  # shrink up to full if extremely negative
    elif cap_delta > 0:
        size *= (1.0 + min(cap_delta / 0.6, 0.25))  # at most +25%

    # Regime adjustment: risk-off shrinks, strong risk-on mildly boosts
    reg = _clamp(_nz(c.regime_score, _nz(c.regime_trend, 0.0)), -1.0, 1.0)
    if reg <= -0.7:
        size *= 0.4
    elif reg <= -0.3:
        size *= 0.6
    elif reg >= 0.7:
        size *= 1.15
    elif reg >= 0.3:
        size *= 1.05

    # Risk mode: 0 = off, 1 = normal, 2 = aggressive, 3 = defensive
    mode = _nz(c.ctx_risk_mode, 1.0)
    if mode <= 0.0:     # off
        size *= 0.0
    elif mode == 3.0:   # defensive
        size *= 0.6
    elif mode >= 2.0:   # aggressive
        size *= 1.1

    # Exposure: near-full exposure clamps size hard
    exposure_frac = _clamp(_nz(c.ctx_exposure_frac, 0.0), 0.0, 1.0)
    if exposure_frac > 0.7:
        # smoothly reduce from 1.0 at 0.7 to 0.4 at 1.0
        shrink = 1.0 - 0.6 * ((exposure_frac - 0.7) / 0.3)
        size *= max(0.4, shrink)

    # Loss streak: cut risk when getting slapped
    loss_streak = _nz(c.ctx_recent_loss_streak, 0.0)
    if loss_streak >= 3:
        size *= 0.5
    elif loss_streak == 2:
        size *= 0.7

    # Hard adjustments from dead-like / rugginess
    dead = _nz(c.is_dead_like, 0.0)
    fraud = 1.0 - _fraud_component(c.ai_fraud_score)  # 0..1 rugginess
    if dead >= 1.0:
        size *= 0.0
    size *= (1.0 - 0.6 * fraud)

    # Self-learning micro nudges on size (via meta)
    if AI_BUY_SELF_LEARN_ENABLED and c.meta:
        try:
            size_boost = float(c.meta.get("sl_size_scalar_boost", 0.0) or 0.0)
        except Exception:
            size_boost = 0.0
        if size_boost != 0.0:
            # clamp to small ± range
            size_boost = max(-0.4, min(0.4, size_boost))
            size *= (1.0 + size_boost)

    size = _clamp(size, 0.0, 1.0)
    # Apply configurable exponent to skew small vs big sizes
    if AI_SIZE_SCALAR_EXP != 1.0 and size > 0.0:
        size = _clamp(size ** AI_SIZE_SCALAR_EXP, 0.0, 1.0)

    return size


def _source_gate_threshold(source: Optional[str]) -> float:
    """
    Return the gate threshold adjusted for candidate source.
    Example:
        pumpfun: slightly higher threshold
        raydium: slightly looser
        momo: maybe lower because it's pre-filtered by momentum engine
    """
    if not source:
        return AI_SCORE_GATE
    key = str(source).lower()
    if key in AI_SCORE_GATE_OVERRIDES:
        try:
            return float(AI_SCORE_GATE_OVERRIDES[key])
        except Exception:
            return AI_SCORE_GATE
    return AI_SCORE_GATE


def _source_score_bonus(source: Optional[str]) -> float:
    if not source:
        return 0.0
    key = str(source).lower()
    val = AI_SCORE_SOURCE_BONUS.get(key)
    try:
        return float(val) if val is not None else 0.0
    except Exception:
        return 0.0


def _apply_self_learning_overrides(
    score: float,
    threshold: float,
    c: Candidate,
) -> Tuple[float, float, str]:
    """
    Optional micro-overrides from self_learning_ai_strategy via Candidate fields
    and/or meta. All effects are bounded and reversible (just nudges).
    """
    if not AI_BUY_SELF_LEARN_ENABLED:
        return score, threshold, ""

    reason_bits: List[str] = []
    sl_meta = c.meta or {}

    # Score boost / penalty
    raw_boost = 0.0
    if c.self_learn_entry_boost is not None:
        raw_boost += _nz(c.self_learn_entry_boost, 0.0)
    try:
        raw_boost += float(sl_meta.get("sl_entry_boost", 0.0) or 0.0)
    except Exception:
        pass

    if raw_boost != 0.0:
        max_b = AI_BUY_SELF_LEARN_MAX_ABS_BOOST
        raw_boost = max(-max_b, min(max_b, raw_boost))
        score = _clamp(score + raw_boost, 0.0, 1.0)
        reason_bits.append(f"sl_boost={raw_boost:+.3f}")

    # Gate offset
    raw_gate_off = 0.0
    if c.self_learn_entry_gate_offset is not None:
        raw_gate_off += _nz(c.self_learn_entry_gate_offset, 0.0)
    try:
        raw_gate_off += float(sl_meta.get("sl_entry_gate_offset", 0.0) or 0.0)
    except Exception:
        pass

    if raw_gate_off != 0.0:
        max_g = AI_BUY_SELF_LEARN_MAX_ABS_GATE
        raw_gate_off = max(-max_g, min(max_g, raw_gate_off))
        raw_gate_off *= AI_BUY_SELF_LEARN_GATE_WEIGHT
        threshold = _clamp(threshold + raw_gate_off, 0.0, 0.99)
        reason_bits.append(f"sl_gate={raw_gate_off:+.3f}")

    # Force-pass / soft-pass hints can be honored at ai_entry_logic layer; here
    # we just attach hints in the reason string if present.
    try:
        if sl_meta.get("sl_force_entry_flag"):
            reason_bits.append("sl_force_entry_hint")
    except Exception:
        pass

    return score, threshold, ",".join(reason_bits)


# =============================================================================
# Public scoring API
# =============================================================================


def score_only(c: Candidate) -> Dict[str, Any]:
    """
    Return detailed scoring breakdown WITHOUT applying the final gate.
    """
    hard_ok, hard_reasons = _hard_filters(c)

    parts: Dict[str, float] = {}

    parts["pool_age"] = _score_pool_age(c.pool_age_min)
    parts["liq"] = _score_liq(c.liq_usd)
    parts["impact"] = _score_impact(c.impact_1k_bps)
    parts["vol5m"] = _score_vol5m(c.vol5m_pct)
    parts["top10"] = _score_top10(c.top10_pct)
    parts["whales"] = _score_whales(c.whale_touches_1h, c.whale_share_1h)
    parts["trend"] = _trend_component(c.ret5m_pct, c.ret15m_pct)
    parts["flow"] = _flow_component(
        c.orderflow_buys_1m,
        c.orderflow_sells_1m,
        c.orderflow_buy_usd_1m,
        c.orderflow_sell_usd_1m,
        c.vol_5m_usd,
        c.flow_accumulation_score,
        c.flow_panic_dump_score,
    )
    parts["book"] = _book_component(c.orderbook_bid_usd, c.orderbook_ask_usd)

    safety_base, safety_bonus = _safety_component(
        c.verified,
        c.liq_locked,
        c.mint_renounced,
        c.freeze_auth_disabled,
        c.rugpull_reports,
    )
    parts["safety"] = safety_base

    parts["pf"] = _pf_component(
        c.pf_momentum_score, c.pf_final_score, c.pf_risk_adjusted_final
    )
    parts["fraud_safety"] = _fraud_component(c.ai_fraud_score)
    parts["regime_ctx"] = _regime_ctx_component(c)

    # New V3+++ components
    entry_quality = _entry_quality_component(c)       # 0..1.5
    parts["entry_quality"] = _clamp(entry_quality / 1.5, 0.0, 1.0)

    smart_whale_align = _smart_whale_align_component(c)  # 0..1.5
    parts["smart_whale_align"] = _clamp(smart_whale_align / 1.5, 0.0, 1.0)

    parts["mcap"] = _score_mcap(c.mcap_usd)

    # Tier / bucket component
    tier_comp = _tier_component(c.entry_tier_bucket, c.position_size_bucket)
    parts["tier"] = tier_comp

    # AI entry head
    ai_entry_comp = _ai_entry_component(c.ai_entry_prob, c.ai_entry_score)
    parts["ai_entry"] = ai_entry_comp

    # Buy pressure from FeatureBuilder
    parts["buy_pressure"] = _buy_pressure_component(c.buy_pressure_score)

    # Weighted linear combo (then normalized / clamped)
    raw_score = (
        W_POOL_AGE * parts["pool_age"]
        + W_LIQ * parts["liq"]
        + W_IMPACT * parts["impact"]
        + W_VOL5M * parts["vol5m"]
        + W_TOP10 * parts["top10"]
        + W_WHALES * parts["whales"]
        + W_TREND * parts["trend"]
        + W_FLOW * parts["flow"]
        + W_BOOK * parts["book"]
        + W_SAFETY * parts["safety"]
        + W_PF * parts["pf"]
        + W_FRAUD * parts["fraud_safety"]
        + W_REGIME_CTX * parts["regime_ctx"]
        + W_ENTRY_QUALITY * parts["entry_quality"]
        + W_SMART_WHALE_ALIGN * parts["smart_whale_align"]
        + W_MCAP * parts["mcap"]
        + W_AI_ENTRY * parts["ai_entry"]
        + W_TIER * parts["tier"]
        + W_BUY_PRESS * parts["buy_pressure"]
    )

    whale_bonus = min(
        WHALE_TOUCH_BONUS_CAP, c.whale_touches_1h * WHALE_TOUCH_BONUS
    )
    raw_score += whale_bonus + safety_bonus

    # Infra penalty: only applies when infra_no_account is true AND this is Pump.
    infra_penalty = 0.0
    if bool(c.infra_no_account) and _is_pump_source(c.source):
        infra_penalty = AI_INFRA_NO_ACCOUNT_PENALTY
        raw_score += infra_penalty

    # Normalize by weight sum to keep in a sane 0..1 band even if weights change
    score = _clamp(raw_score / max(AI_WEIGHT_NORM, 1e-6), 0.0, 1.0)

    # Per-source additive bonus (tiny shifts for pumpfun / raydium / momo)
    score += _source_score_bonus(c.source)
    score = _clamp(score, 0.0, 1.0)

    # Advisory sizing & risk capacity info
    risk_capacity = _clamp(_nz(c.risk_capacity_score, 1.0), 0.0, 1.0)
    size_scalar = _size_scalar_from_candidate(c, entry_quality)

    if AI_BUY_DEBUG_LOG:
        try:
            log.debug(
                "BUY_SCORING sym=%s mint=%s src=%s score=%.3f size=%.3f gate=%.3f "
                "infra_no_account=%s parts=%s hard_ok=%s reasons=%s",
                c.symbol,
                c.mint,
                c.source,
                score,
                size_scalar,
                _source_gate_threshold(c.source),
                bool(c.infra_no_account),
                {k: round(v, 3) for k, v in parts.items()},
                hard_ok,
                "; ".join(hard_reasons),
            )
        except Exception:
            log.debug(
                "BUY_SCORING sym=%s mint=%s src=%s score=%.3f size=%.3f gate=%.3f "
                "infra_no_account=%s hard_ok=%s reasons=%s",
                c.symbol,
                c.mint,
                c.source,
                score,
                size_scalar,
                _source_gate_threshold(c.source),
                bool(c.infra_no_account),
                hard_ok,
                "; ".join(hard_reasons),
            )

    detail: Dict[str, Any] = {
        "ok_hard": hard_ok,
        "hard_reasons": hard_reasons,
        "parts": parts,
        "whale_bonus": whale_bonus,
        "safety_bonus": safety_bonus,
        "infra_penalty": infra_penalty,
        "raw_score": raw_score,
        "score": score,
        "entry_quality": entry_quality,
        "size_scalar": size_scalar,
        "entry_block_flag": _nz(c.entry_block_flag, 0.0),
        "risk_capacity": risk_capacity,
        "gate_threshold": _source_gate_threshold(c.source),
        "entry_tier": _nz(c.entry_tier_bucket, 0.0),
        "position_size_tier": _nz(c.position_size_bucket, 0.0),
        "source": c.source,
        "infra_no_account": bool(c.infra_no_account),
    }

    if AI_BUY_ATTACH_PARTS:
        # Shallow copy to avoid bloating the live feature dict unless explicitly requested.
        try:
            detail["parts_json"] = json.dumps(
                {k: round(float(v), 4) for k, v in parts.items()},
                separators=(",", ":"),
            )
        except Exception:
            pass

    return detail


def pass_gate(c: Candidate) -> Tuple[bool, float, str]:
    """
    Returns (ok, score, reason).

    ok:     True if candidate passes hard filters + score >= (possibly tightened) gate
            OR if AI_PRE_BYPASS_FOR_TEST=1 and hard filters pass.
    score:  final [0,1] score
    reason: human summary (reject reason(s) or short “pass” summary)
    """
    detail = score_only(c)

    # Hard filters first (structural / risk / AI fraud / dead / entry_block_flag etc.)
    if not detail["ok_hard"]:
        reason = "; ".join(detail["hard_reasons"])
        # If we softened liquidity due to Pump+infra_no_account, hint that in reason.
        if bool(c.infra_no_account) and _is_pump_source(c.source):
            reason += " [infra_no_account_soft_liq]"
        return (
            False,
            detail["score"],
            reason,
        )

    score = detail["score"]
    threshold = float(detail["gate_threshold"])
    parts = detail["parts"]

    # ──────────────────────────────────────────────────────────────
    # TEST MODE: bypass soft gate but still respect hard filters
    # ──────────────────────────────────────────────────────────────
    if AI_PRE_BYPASS_FOR_TEST:
        eq = detail["entry_quality"]
        size_scalar = detail["size_scalar"]
        reason = (
            f"pass TEST_BYPASS score={score:.3f} thr={threshold:.3f} "
            f"size={size_scalar:.2f} eq={eq:.2f} "
            f"(liq={parts['liq']:.2f}, impact={parts['impact']:.2f}, "
            f"trend={parts['trend']:.2f}, flow={parts['flow']:.2f}, "
            f"book={parts['book']:.2f}, pf={parts['pf']:.2f}, "
            f"fraud_safety={parts['fraud_safety']:.2f}, "
            f"regime_ctx={parts['regime_ctx']:.2f}, "
            f"entry_quality_norm={parts['entry_quality']:.2f}, "
            f"smart_whale_align={parts['smart_whale_align']:.2f}, "
            f"mcap={parts['mcap']:.2f}, "
            f"tier={parts['tier']:.2f}, "
            f"ai_entry={parts['ai_entry']:.2f}, "
            f"buy_pressure={parts['buy_pressure']:.2f})"
        )
        if bool(c.infra_no_account) and _is_pump_source(c.source):
            reason += " [infra_no_account_soft_liq]"
        reason += " [AI_PRE_BYPASS_FOR_TEST=1]"
        return True, score, reason
    # ──────────────────────────────────────────────────────────────

    # Regime-aware tightening (must align with ai_entry_logic + ai_risk_model)
    if AI_REGIME_TIGHTEN_ENABLED:
        trend = _clamp(_nz(c.regime_score, _nz(c.regime_trend, 0.0)), -1.0, 1.0)
        manip = _clamp(_nz(c.regime_manipulation_risk, 0.0))

        # Deep risk-off → require much higher score
        if trend <= -0.7:
            threshold = max(threshold + AI_RISK_OFF_EXTRA_MARGIN, 0.80)

        # High manipulation regime → bump gate a bit
        if manip >= AI_MANIP_HIGH:
            threshold = max(threshold + 0.08, threshold)

    # ai_risk_model regime gate factor → tighten/loosen gate multiplicatively
    if AI_USE_REGIME_GATE_FACTOR and c.regime_entry_gate_factor is not None:
        rg = _clamp(_nz(c.regime_entry_gate_factor, 1.0), 0.2, 1.6)
        # center at 1.0: <1 ⇒ tougher gate, >1 ⇒ easier gate
        delta = (1.0 - rg) * AI_REGIME_GATE_FACTOR_STRENGTH
        threshold = _clamp(threshold * (1.0 + delta), 0.0, 0.99)

    # Self-learning overrides (bounded adjustments)
    score, threshold, sl_note = _apply_self_learning_overrides(score, threshold, c)

    # Final decision
    if score >= threshold:
        eq = detail["entry_quality"]
        size_scalar = detail["size_scalar"]
        reason = (
            f"pass score={score:.3f} thr={threshold:.3f} "
            f"size={size_scalar:.2f} eq={eq:.2f} "
            f"(liq={parts['liq']:.2f}, impact={parts['impact']:.2f}, "
            f"trend={parts['trend']:.2f}, flow={parts['flow']:.2f}, "
            f"book={parts['book']:.2f}, pf={parts['pf']:.2f}, "
            f"fraud_safety={parts['fraud_safety']:.2f}, "
            f"regime_ctx={parts['regime_ctx']:.2f}, "
            f"entry_quality_norm={parts['entry_quality']:.2f}, "
            f"smart_whale_align={parts['smart_whale_align']:.2f}, "
            f"mcap={parts['mcap']:.2f}, "
            f"tier={parts['tier']:.2f}, "
            f"ai_entry={parts['ai_entry']:.2f}, "
            f"buy_pressure={parts['buy_pressure']:.2f})"
        )
        if bool(c.infra_no_account) and _is_pump_source(c.source):
            reason += " [infra_no_account_soft_liq]"
        if sl_note:
            reason += f" [self_learning:{sl_note}]"
        return True, score, reason

    # Reject path
    rej_reason = f"below_gate(score={score:.3f} < {threshold:.3f})"
    if bool(c.infra_no_account) and _is_pump_source(c.source):
        rej_reason += " [infra_no_account_soft_liq]"
    if sl_note:
        rej_reason += f" [self_learning:{sl_note}]"

    return False, score, rej_reason


# =============================================================================
# Convenience builders
# =============================================================================


def from_pool_snapshot(d: Dict[str, Any]) -> Candidate:
    """
    Best-effort builder from a mixed dict (e.g. Raydium / Dexscreener snapshot).
    Keys are optional — missing values fall back gracefully.

    This path is mostly used when we don't yet have the full FeatureBuilder
    feature vector available. When possible, prefer `from_feature_vector(...)`
    so all smart-wallet / regime / risk context fields are populated.
    """
    meta = d.get("meta") or {}
    return Candidate(
        mint=d.get("mint")
        or d.get("token")
        or d.get("address")
        or d.get("baseMint")
        or "UNKNOWN",
        symbol=d.get("symbol") or d.get("ticker") or d.get("sym") or "UNK",
        pool_age_min=_nz(
            d.get("pool_age_min")
            or d.get("token_age_min")
            or d.get("age_min")
            or d.get("age_minutes")
            or d.get("ageMinutes"),
            0.0,
        ),
        liq_usd=_nz(
            d.get("liq_usd")
            or d.get("liquidity_usd")
            or d.get("liquidity")
            or d.get("liq"),
            0.0,
        ),
        impact_1k_bps=_nz(
            d.get("impact_1k_bps")
            or d.get("impact_1000usd_bps")
            or d.get("price_impact_bps")
            or d.get("impact_bps"),
            999.0,
        ),
        vol5m_pct=_nz(
            d.get("vol5m_pct")
            or d.get("vol_5m_pct")
            or d.get("volatility_5m_pct")
            or d.get("chg5")
            or d.get("price_change_5m_pct"),
            0.0,
        ),
        top10_pct=_nz(
            d.get("top10_pct")
            or d.get("top_10_pct")
            or d.get("holder_top10_pct")
            or d.get("top10"),
            100.0,
        ),
        honeypot_flag=bool(
            d.get("honeypot_flag")
            or d.get("rug_risk")
            or d.get("hp_flag")
            or d.get("is_honeypot")
            or False
        ),
        whale_touches_1h=int(
            d.get("whale_touches_1h") or d.get("whale_hits_1h") or 0
        ),
        verified=d.get("verified"),
        liq_locked=d.get("liq_locked"),
        mint_renounced=d.get("mint_renounced"),
        freeze_auth_disabled=d.get("freeze_auth_disabled"),
        dev_wallet_pct=d.get("dev_wallet_pct"),
        ret5m_pct=d.get("ret5m_pct") or d.get("price_change_5m_pct"),
        ret15m_pct=d.get("ret15m_pct") or d.get("price_change_15m_pct"),
        orderflow_buys_1m=d.get("orderflow_buys_1m"),
        orderflow_sells_1m=d.get("orderflow_sells_1m"),
        orderflow_buy_usd_1m=d.get("orderflow_buy_usd_1m"),
        orderflow_sell_usd_1m=d.get("orderflow_sell_usd_1m"),
        orderbook_bid_usd=d.get("orderbook_bid_usd"),
        orderbook_ask_usd=d.get("orderbook_ask_usd"),
        rugpull_reports=d.get("rugpull_reports"),
        mcap_usd=d.get("mcap_usd") or d.get("fdv_usd") or d.get("fdv"),
        vol_5m_usd=d.get("vol_5m_usd") or d.get("vol5") or d.get("vol5m_usd"),
        buys_5m=d.get("buys_5m") or d.get("buys5"),
        sells_5m=d.get("sells_5m") or d.get("sells5"),
        ai_fraud_score=d.get("ai_fraud_score")
        or d.get("ai_rug_score")
        or d.get("ai_rug_score_01"),
        pf_momentum_score=d.get("pf_momentum_score")
        or d.get("momentum_score"),
        pf_final_score=d.get("pf_final_score") or d.get("final_score"),
        pf_risk_adjusted_final=d.get("pf_risk_adjusted_final"),
        whale_share_1h=d.get("whale_share_1h"),
        unique_buyers_1h=d.get("unique_buyers_1h"),
        smart_wallet_score=d.get("smart_wallet_score"),
        smart_flow_raw=d.get("smart_flow_raw"),
        smart_whale_alignment=d.get("smart_whale_alignment"),
        unique_buyers_5m=d.get("unique_buyers_5m"),
        whale_share_5m=d.get("whale_share_5m"),
        whales_5m=d.get("whales_5m"),
        buy_pressure_score=d.get("buy_pressure_score") or d.get("buy_pressure"),
        flow_accumulation_score=d.get("flow_accumulation_score"),
        flow_panic_dump_score=d.get("flow_panic_dump_score"),
        flow_conflict_score=d.get("flow_conflict_score"),
        whale_liquidity_footprint=d.get("whale_liquidity_footprint"),
        entry_quality_score=d.get("entry_quality_score"),
        position_size_scalar=d.get("position_size_scalar"),
        entry_block_flag=d.get("entry_block_flag"),
        entry_micro_alpha=d.get("entry_micro_alpha"),
        structural_quality_score=d.get("structural_quality_score"),
        is_dead_like=d.get("is_dead_like"),
        risk_capacity_score=d.get("risk_capacity_score"),
        entry_risk_score=d.get("entry_risk_score"),
        entry_tier_bucket=d.get("entry_tier_bucket"),
        position_size_bucket=d.get("position_size_bucket"),
        ai_entry_prob=d.get("ai_entry_prob"),
        ai_entry_score=d.get("ai_entry_score"),
        regime_trend=d.get("regime_trend"),
        regime_liquidity=d.get("regime_liquidity"),
        regime_manipulation_risk=d.get("regime_manipulation_risk"),
        regime_score=d.get("regime_score") or d.get("regime_numeric"),
        regime_volatility=d.get("regime_volatility"),
        ctx_open_positions=d.get("ctx_open_positions"),
        ctx_max_positions=d.get("ctx_max_positions"),
        ctx_daily_drawdown_pct=d.get("ctx_daily_drawdown_pct"),
        ctx_daily_pnl_pct=d.get("ctx_daily_pnl_pct"),
        ctx_risk_mode=d.get("ctx_risk_mode"),
        ctx_exposure_frac=d.get("ctx_exposure_frac"),
        ctx_exposure_usd=d.get("ctx_exposure_usd"),
        ctx_win_rate_pct=d.get("ctx_win_rate_pct"),
        ctx_recent_loss_streak=d.get("ctx_recent_loss_streak"),
        risk_profile_conservative=d.get("risk_profile_conservative"),
        risk_profile_balanced=d.get("risk_profile_balanced"),
        risk_profile_aggressive=d.get("risk_profile_aggressive"),
        ai_capacity_delta=d.get("ai_capacity_delta"),
        regime_entry_gate_factor=d.get("regime_entry_gate_factor"),
        ai_risk_profile_label=d.get("ai_risk_profile_label"),
        self_learn_entry_boost=d.get("self_learn_entry_boost"),
        self_learn_entry_gate_offset=d.get("self_learn_entry_gate_offset"),
        self_learn_entry_block=d.get("self_learn_entry_block"),
        source=d.get("source"),
        meta=meta,
        infra_no_account=d.get("infra_no_account"),
    )


def from_feature_vector(feats: Dict[str, Any]) -> Candidate:
    """
    Builder from FeatureBuilder / PF feature dict so scanners can do:

        cand = from_feature_vector(features)
        ok, score, reason = pass_gate(cand)

    This is the preferred path for V3+++ because it carries all the enriched
    smart-wallet / regime / risk context / entry_quality fields.
    """
    meta = feats.get("meta") or {}
    return Candidate(
        mint=feats.get("mint") or feats.get("token") or "UNKNOWN",
        symbol=feats.get("symbol") or feats.get("sym") or "UNK",
        pool_age_min=_nz(
            feats.get("token_age_min")
            or feats.get("age_min")
            or feats.get("ageMinutes"),
            0.0,
        ),
        liq_usd=_nz(
            feats.get("liq_usd")
            or feats.get("liquidity_usd")
            or feats.get("liq"),
            0.0,
        ),
        impact_1k_bps=_nz(
            feats.get("impact_1k_bps")
            or feats.get("impact_1000usd_bps"),
            999.0,
        ),
        vol5m_pct=_nz(
            feats.get("vol5m_pct")
            or feats.get("vol_5m_pct")
            or feats.get("price_change_5m_pct")
            or feats.get("chg5"),
            0.0,
        ),
        top10_pct=_nz(feats.get("top10_pct"), 100.0),
        honeypot_flag=bool(
            feats.get("honeypot_flag")
            or feats.get("rug_risk")
            or feats.get("is_honeypot")
        ),
        whale_touches_1h=int(
            feats.get("whale_touches_1h")
            or feats.get("whales_5m")
            or 0
        ),
        verified=feats.get("verified"),
        liq_locked=feats.get("liq_locked"),
        mint_renounced=feats.get("mint_renounced"),
        freeze_auth_disabled=feats.get("freeze_auth_disabled"),
        dev_wallet_pct=feats.get("dev_wallet_pct"),
        ret5m_pct=feats.get("price_change_5m_pct"),
        ret15m_pct=feats.get("price_change_15m_pct"),
        orderflow_buys_1m=feats.get("buys_1m"),
        orderflow_sells_1m=feats.get("sells_1m"),
        orderflow_buy_usd_1m=feats.get("buy_usd_1m"),
        orderflow_sell_usd_1m=feats.get("sell_usd_1m"),
        orderbook_bid_usd=feats.get("orderbook_bid_usd"),
        orderbook_ask_usd=feats.get("orderbook_ask_usd"),
        rugpull_reports=feats.get("rugpull_reports"),
        mcap_usd=feats.get("mcap_usd"),
        vol_5m_usd=feats.get("vol_5m_usd"),
        buys_5m=feats.get("buys_5m"),
        sells_5m=feats.get("sells_5m"),
        ai_fraud_score=feats.get("ai_fraud_score")
        or feats.get("ai_rug_score")
        or feats.get("ai_rug_score_01"),
        pf_momentum_score=feats.get("pf_momentum_score")
        or feats.get("momentum_score"),
        pf_final_score=feats.get("pf_final_score")
        or feats.get("final_score"),
        pf_risk_adjusted_final=feats.get("pf_risk_adjusted_final"),
        whale_share_1h=feats.get("whale_share_1h"),
        unique_buyers_1h=feats.get("unique_buyers_1h"),
        smart_wallet_score=feats.get("smart_wallet_score"),
        smart_flow_raw=feats.get("smart_flow_raw"),
        smart_whale_alignment=feats.get("smart_whale_alignment_consolidated")
        or feats.get("smart_whale_alignment"),
        unique_buyers_5m=feats.get("unique_buyers_5m"),
        whale_share_5m=feats.get("whale_share_5m"),
        whales_5m=feats.get("whales_5m"),
        buy_pressure_score=feats.get("buy_pressure_score")
        or feats.get("buy_pressure"),
        flow_accumulation_score=feats.get("flow_accumulation_score"),
        flow_panic_dump_score=feats.get("flow_panic_dump_score"),
        flow_conflict_score=feats.get("flow_conflict_score"),
        whale_liquidity_footprint=feats.get("whale_liquidity_footprint"),
        entry_quality_score=feats.get("entry_quality_score"),
        position_size_scalar=feats.get("position_size_scalar"),
        entry_block_flag=feats.get("entry_block_flag"),
        entry_micro_alpha=feats.get("entry_micro_alpha"),
        structural_quality_score=feats.get("structural_quality_score"),
        is_dead_like=feats.get("is_dead_like"),
        risk_capacity_score=feats.get("risk_capacity_score"),
        entry_risk_score=feats.get("entry_risk_score"),
        entry_tier_bucket=feats.get("entry_tier_bucket"),
        position_size_bucket=feats.get("position_size_bucket"),
        ai_entry_prob=feats.get("ai_entry_prob"),
        ai_entry_score=feats.get("ai_entry_score"),
        regime_trend=feats.get("regime_trend"),
        regime_liquidity=feats.get("regime_liquidity"),
        regime_manipulation_risk=feats.get("regime_manipulation_risk"),
        regime_score=feats.get("regime_score") or feats.get("regime_numeric"),
        regime_volatility=feats.get("regime_volatility"),
        ctx_open_positions=feats.get("ctx_open_positions"),
        ctx_max_positions=feats.get("ctx_max_positions"),
        ctx_daily_drawdown_pct=feats.get("ctx_daily_drawdown_pct"),
        ctx_daily_pnl_pct=feats.get("ctx_daily_realized_pnl_pct")
        or feats.get("ctx_daily_pnl_pct"),
        ctx_risk_mode=feats.get("ctx_risk_mode"),
        ctx_exposure_frac=feats.get("ctx_exposure_frac"),
        ctx_exposure_usd=feats.get("ctx_exposure_usd"),
        ctx_win_rate_pct=feats.get("risk_win_rate_pct")
        or feats.get("ctx_win_rate_pct"),
        ctx_recent_loss_streak=feats.get("risk_loss_streak")
        or feats.get("ctx_recent_loss_streak"),
        risk_profile_conservative=feats.get("risk_profile_conservative"),
        risk_profile_balanced=feats.get("risk_profile_balanced"),
        risk_profile_aggressive=feats.get("risk_profile_aggressive"),
        ai_capacity_delta=feats.get("ai_capacity_delta"),
        regime_entry_gate_factor=feats.get("regime_entry_gate_factor"),
        ai_risk_profile_label=feats.get("ai_risk_profile_label"),
        self_learn_entry_boost=feats.get("self_learn_entry_boost"),
        self_learn_entry_gate_offset=feats.get("self_learn_entry_gate_offset"),
        self_learn_entry_block=feats.get("self_learn_entry_block"),
        source=feats.get("scanner_source") or feats.get("source"),
        meta=meta,
        infra_no_account=feats.get("infra_no_account"),
    )


# =============================================================================
# FeatureVector helper (bridge into ai_entry_logic / scanners)
# =============================================================================


def gate_feature_vector(feats: Dict[str, Any]) -> Tuple[bool, float, str]:
    """
    Convenience bridge for scanners / Strategy:

        ok, score, reason = gate_feature_vector(features)

    - Builds a Candidate from FeatureBuilder / PairFilter features.
    - Runs full V3+++ hard filters + soft scoring + regime tightening +
      ai_risk_model overlays + optional self-learning nudges.
    - Writes the core outputs back into the feature dict:

        feats["ai_buy_score"]          = score
        feats["ai_buy_ok_hard"]        = ok_hard
        feats["ai_buy_pass_gate"]      = ok
        feats["ai_buy_reason"]         = reason
        feats["ai_buy_gate_threshold"] = dynamic gate applied (float)
        feats["ai_buy_size_scalar"]    = advisory position size scalar (0..1)

    Returns:
        ok, score, reason
    """
    cand = from_feature_vector(feats)
    ok, score, reason = pass_gate(cand)

    # Recompute detail to get gate_threshold and size_scalar (cheap & deterministic)
    detail = score_only(cand)

    feats["ai_buy_score"] = float(score)
    feats["ai_buy_ok_hard"] = bool(detail["ok_hard"])
    feats["ai_buy_pass_gate"] = bool(ok)
    feats["ai_buy_reason"] = str(reason)
    feats["ai_buy_gate_threshold"] = float(detail["gate_threshold"])
    feats["ai_buy_size_scalar"] = float(detail["size_scalar"])

    # Optional: attach compact parts JSON for downstream analytics
    if AI_BUY_ATTACH_PARTS and "parts" in detail:
        try:
            feats["ai_buy_parts_json"] = detail.get("parts_json") or json.dumps(
                {k: round(float(v), 4) for k, v in detail["parts"].items()},
                separators=(",", ":"),
            )
        except Exception:
            pass

    return ok, score, reason
