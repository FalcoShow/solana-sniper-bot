# -*- coding: utf-8 -*-
from __future__ import annotations

"""
utils_pkg/ai_buy_scoring.py

AI Buy Scoring — V3+++ (God-Tier / quant-grade)
-----------------------------------------------

Backwards-compatible scorer used by sniper / scanners / reinvestor:

    from utils_pkg.ai_buy_scoring import Candidate, pass_gate, score_only
    ok, score, why = pass_gate(Candidate(...))

Role
----
Deterministic “bouncer at the door” that runs *just ahead* of the entry engine:

  • Hard filters: fast reject, strict reasons (cheap + deterministic)
  • Soft score:   composite 0..1 from many components
  • Gate:         score >= dynamic threshold (source/regime/risk overlays)
  • Output wiring: helpers that write ai_buy_* fields back into feature dicts

Guarantees / design constraints
-------------------------------
- 100% deterministic & cheap (no OpenAI, no Redis, no HTTP).
- Safe defaults: missing fields degrade gracefully.
- Strict reject reasons are stable, bucket-friendly strings (good for aggregation).
- Regime/risk overlays are compatible with ai_entry_logic + ai_risk_model patterns.
- Optional test bypass: AI_PRE_BYPASS_FOR_TEST=1 bypasses *soft* gate only.

NOTE:
This module does NOT execute trades; it just decides whether a candidate is
allowed to proceed to quote/build/send in sniper/entry executor.
"""

import json
import math
import os
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

from logging_setup import get_logger

# ──────────────────────────────────────────────────────────────────────────────
# Logger (compatible with get_logger() or get_logger(name))
# ──────────────────────────────────────────────────────────────────────────────
try:
    log = get_logger(__name__)
except TypeError:  # older signature
    log = get_logger()

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
    vv = v.strip().lower()
    if vv in ("1", "true", "yes", "y", "on"):
        return True
    if vv in ("0", "false", "no", "n", "off"):
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
    try:
        xx = float(x)
    except Exception:
        xx = lo
    return max(lo, min(hi, xx))


def _safe_div(n: float, d: float, fallback: float = 0.0) -> float:
    try:
        dd = float(d)
        if dd == 0.0:
            return fallback
        return float(n) / dd
    except Exception:
        return fallback


def _nz(x: Any, fallback: float = 0.0) -> float:
    if x is None:
        return float(fallback)
    try:
        return float(x)
    except Exception:
        return float(fallback)


def _sigmoid(x: float) -> float:
    try:
        return 1.0 / (1.0 + math.exp(-float(x)))
    except OverflowError:
        return 1.0 if x > 0 else 0.0
    except Exception:
        return 0.5


def _normalize_source_key(source: Optional[str]) -> Optional[str]:
    if not source:
        return None
    s = str(source).strip().lower()
    # normalize common variants
    if "pump" in s:
        return "pumpfun"
    if "rayd" in s:
        return "raydium"
    if "dex" in s:
        return "dexscreener"
    if "momo" in s or "momentum" in s:
        return "momo"
    return s


def _is_pump_source(source: Optional[str]) -> bool:
    sk = _normalize_source_key(source) or ""
    return "pump" in sk


def _fraud_to_01(ai_fraud_score: Optional[float]) -> Optional[float]:
    if ai_fraud_score is None:
        return None
    f = _nz(ai_fraud_score, 0.0)
    # allow 0..100 inputs
    if f > 1.5:
        f = f / 100.0
    return _clamp(f, 0.0, 1.0)


# =============================================================================
# Candidate schema (V3+++ God-Tier)
# =============================================================================


@dataclass
class Candidate:
    # Core required fields
    mint: str
    symbol: str

    pool_age_min: float          # minutes since pool creation
    liq_usd: float               # pool liquidity USD
    impact_1k_bps: float         # impact bps for ~$1k swap
    vol5m_pct: float             # "alive-ness" magnitude, % (or chg5 proxy)
    top10_pct: float             # top10 concentration, 0..100
    honeypot_flag: bool          # hard deny
    whale_touches_1h: int        # whale touches count

    # Optional structure / safety
    verified: Optional[bool] = None
    liq_locked: Optional[bool] = None
    mint_renounced: Optional[bool] = None
    freeze_auth_disabled: Optional[bool] = None
    dev_wallet_pct: Optional[float] = None

    # Momentum / returns
    ret5m_pct: Optional[float] = None
    ret15m_pct: Optional[float] = None

    # Orderflow (last 1m)
    orderflow_buys_1m: Optional[int] = None
    orderflow_sells_1m: Optional[int] = None
    orderflow_buy_usd_1m: Optional[float] = None
    orderflow_sell_usd_1m: Optional[float] = None

    # Orderbook snapshot
    orderbook_bid_usd: Optional[float] = None
    orderbook_ask_usd: Optional[float] = None

    # External safety
    rugpull_reports: Optional[int] = None

    # Extended structure / flow
    mcap_usd: Optional[float] = None
    vol_5m_usd: Optional[float] = None
    buys_5m: Optional[int] = None
    sells_5m: Optional[int] = None

    # AI / PF scores
    ai_fraud_score: Optional[float] = None         # 0..1 or 0..100
    pf_momentum_score: Optional[float] = None      # 0..1
    pf_final_score: Optional[float] = None         # 0..1
    pf_risk_adjusted_final: Optional[float] = None # 0..1

    # Whale / holders microstructure
    whale_share_1h: Optional[float] = None         # 0..1
    unique_buyers_1h: Optional[int] = None

    # Smart-wallet / whale alignment
    smart_wallet_score: Optional[float] = None     # 0..1
    smart_flow_raw: Optional[float] = None         # [-1,1] approx
    smart_whale_alignment: Optional[float] = None  # 0..1.5
    unique_buyers_5m: Optional[float] = None
    whale_share_5m: Optional[float] = None         # 0..1
    whales_5m: Optional[float] = None

    # Flow regime & buy-pressure
    buy_pressure_score: Optional[float] = None     # 0..1.5-ish
    flow_accumulation_score: Optional[float] = None
    flow_panic_dump_score: Optional[float] = None
    flow_conflict_score: Optional[float] = None
    whale_liquidity_footprint: Optional[float] = None

    # High-level entry features
    entry_quality_score: Optional[float] = None    # 0..1.5
    position_size_scalar: Optional[float] = None   # 0..1
    entry_block_flag: Optional[float] = None       # 0/1
    entry_micro_alpha: Optional[float] = None      # 0..1.5
    structural_quality_score: Optional[float] = None  # 0..1.5
    is_dead_like: Optional[float] = None           # 0/1
    risk_capacity_score: Optional[float] = None    # 0..1
    entry_risk_score: Optional[float] = None       # 0..1
    entry_tier_bucket: Optional[float] = None      # 0..3
    position_size_bucket: Optional[float] = None   # 0..3

    # AI entry model head (optional)
    ai_entry_prob: Optional[float] = None          # 0..1
    ai_entry_score: Optional[float] = None         # 0..1

    # Regime & context
    regime_trend: Optional[float] = None           # -1..1
    regime_liquidity: Optional[float] = None       # 0..1
    regime_manipulation_risk: Optional[float] = None # 0..1
    regime_score: Optional[float] = None           # -1..1
    regime_volatility: Optional[float] = None      # 0..1

    ctx_open_positions: Optional[int] = None
    ctx_max_positions: Optional[int] = None
    ctx_daily_drawdown_pct: Optional[float] = None # negative = loss
    ctx_daily_pnl_pct: Optional[float] = None
    ctx_risk_mode: Optional[float] = None          # 0 off, 1 normal, 2 agg, 3 defensive

    ctx_exposure_frac: Optional[float] = None      # 0..1
    ctx_exposure_usd: Optional[float] = None
    ctx_win_rate_pct: Optional[float] = None       # 0..100
    ctx_recent_loss_streak: Optional[int] = None

    # Risk manager overlays / bucket capacity
    ctx_global_block_flag: Optional[float] = None  # 0/1
    risk_bucket_utilization: Optional[float] = None    # 0..1
    risk_bucket_capacity_score: Optional[float] = None # 0..1

    # Risk profile tags
    risk_profile_conservative: Optional[float] = None
    risk_profile_balanced: Optional[float] = None
    risk_profile_aggressive: Optional[float] = None

    # ai_risk_model overlays
    ai_capacity_delta: Optional[float] = None
    regime_entry_gate_factor: Optional[float] = None
    ai_risk_profile_label: Optional[str] = None

    # Self-learning hints
    self_learn_entry_boost: Optional[float] = None
    self_learn_entry_gate_offset: Optional[float] = None
    self_learn_entry_block: Optional[float] = None

    # Historical edge overlays
    hist_edge_score: Optional[float] = None
    hist_win_rate: Optional[float] = None          # 0..1 or 0..100
    hist_rr_ratio: Optional[float] = None
    exploration_score: Optional[float] = None      # 0..1

    # Source hint
    source: Optional[str] = None

    # Misc
    meta: Dict[str, Any] = field(default_factory=dict)

    # Infra flags (Pump)
    infra_no_account: Optional[bool] = None

    def __post_init__(self) -> None:
        # Normalize a few core types to reduce downstream surprises
        self.mint = str(self.mint or "UNKNOWN")
        self.symbol = str(self.symbol or "UNK")
        self.pool_age_min = _nz(self.pool_age_min, 0.0)
        self.liq_usd = _nz(self.liq_usd, 0.0)
        self.impact_1k_bps = _nz(self.impact_1k_bps, 999.0)
        self.vol5m_pct = _nz(self.vol5m_pct, 0.0)
        self.top10_pct = _nz(self.top10_pct, 100.0)
        self.honeypot_flag = bool(self.honeypot_flag)
        try:
            self.whale_touches_1h = int(self.whale_touches_1h or 0)
        except Exception:
            self.whale_touches_1h = 0


# =============================================================================
# Env-driven thresholds & weights
# =============================================================================

# --- Hard filters
AI_MIN_POOL_AGE_MIN = _env_float("AI_MIN_POOL_AGE_MIN", 3.0)
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
AI_MAX_RUG_SCORE = _env_float("AI_MAX_RUG_SCORE", 0.85)
AI_MIN_PF_FINAL = _env_float("AI_MIN_PF_FINAL", 0.08)
AI_MIN_PF_RISK_ADJ = _env_float("AI_MIN_PF_RISK_ADJ", 0.06)
AI_MIN_AI_ENTRY_PROB = _env_float("AI_MIN_AI_ENTRY_PROB", 0.0)  # 0 disables

# Entry quality / risk guards
AI_MIN_ENTRY_QUALITY = _env_float("AI_MIN_ENTRY_QUALITY", 0.0)
AI_MIN_STRUCTURAL_QUALITY = _env_float("AI_MIN_STRUCTURAL_QUALITY", 0.0)
AI_MAX_ENTRY_RISK_SCORE = _env_float("AI_MAX_ENTRY_RISK_SCORE", 1.0)

# Regime/session tightening
AI_REGIME_TIGHTEN_ENABLED = _env_bool("AI_REGIME_TIGHTEN_ENABLED", True)
AI_RISK_OFF_EXTRA_MARGIN = _env_float("AI_RISK_OFF_EXTRA_MARGIN", 0.08)
AI_MANIP_HIGH = _env_float("AI_MANIP_HIGH", 0.75)

AI_CTX_MAX_DRAWDOWN_PCT = abs(_env_float("AI_CTX_MAX_DRAWDOWN_PCT", 15.0))
AI_CTX_MAX_OPEN_POS_RATIO = _env_float("AI_CTX_MAX_OPEN_POS_RATIO", 1.0)
AI_CTX_LOCK_AFTER_TARGET_PNL = _env_float("AI_CTX_LOCK_AFTER_TARGET_PNL", 0.0)

# FeatureBuilder-aligned guards
AI_USE_ENTRY_BLOCK_FLAG = _env_bool("AI_USE_ENTRY_BLOCK_FLAG", True)
AI_USE_RISK_CAPACITY_GUARD = _env_bool("AI_USE_RISK_CAPACITY_GUARD", True)
AI_MIN_RISK_CAPACITY = _env_float("AI_MIN_RISK_CAPACITY", 0.02)
AI_MIN_ENTRY_TIER = _env_float("AI_MIN_ENTRY_TIER", 0.0)

# Per-bucket capacity guards
AI_USE_BUCKET_CAPACITY_GUARD = _env_bool("AI_USE_BUCKET_CAPACITY_GUARD", True)
AI_MIN_BUCKET_CAPACITY = _env_float("AI_MIN_BUCKET_CAPACITY", 0.05)

# Historical edge floor
AI_MIN_HIST_EDGE = _env_float("AI_MIN_HIST_EDGE", 0.0)

# Microstructure floors (normalized 0..1)
AI_MIN_BUY_PRESSURE = _env_float("AI_MIN_BUY_PRESSURE", 0.0)
AI_MIN_SMART_ALIGN = _env_float("AI_MIN_SMART_ALIGN", 0.0)

# Optional market-cap band (0 disables)
AI_MIN_MCAP_USD = _env_float("AI_MIN_MCAP_USD", 0.0)
AI_MAX_MCAP_USD = _env_float("AI_MAX_MCAP_USD", 0.0)

# Self-learning overrides
AI_BUY_SELF_LEARN_ENABLED = _env_bool("AI_BUY_SELF_LEARN_ENABLED", True)
AI_BUY_SELF_LEARN_MAX_ABS_BOOST = _env_float("AI_BUY_SELF_LEARN_MAX_ABS_BOOST", 0.12)
AI_BUY_SELF_LEARN_MAX_ABS_GATE = _env_float("AI_BUY_SELF_LEARN_MAX_ABS_GATE", 0.10)
AI_BUY_SELF_LEARN_GATE_WEIGHT = _env_float("AI_BUY_SELF_LEARN_GATE_WEIGHT", 1.0)

# Regime gate factor usage (from ai_risk_model)
AI_USE_REGIME_GATE_FACTOR = _env_bool("AI_USE_REGIME_GATE_FACTOR", True)
AI_REGIME_GATE_FACTOR_STRENGTH = _env_float("AI_REGIME_GATE_FACTOR_STRENGTH", 0.30)

# Infra / Pump-specific controls
AI_INFRA_NO_ACCOUNT_SOFT_LIQ = _env_bool("AI_INFRA_NO_ACCOUNT_SOFT_LIQ", True)
AI_INFRA_NO_ACCOUNT_PENALTY = _env_float("AI_INFRA_NO_ACCOUNT_PENALTY", -0.03)

# --- Soft score gate
AI_SCORE_GATE = _env_float("AI_SCORE_GATE", 0.58)
AI_SCORE_GATE_OVERRIDES = _env_json("AI_SCORE_GATE_OVERRIDES_JSON") or {}
AI_SCORE_SOURCE_BONUS = _env_json("AI_SCORE_SOURCE_BONUS_JSON") or {}

# --- Component weights
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

W_ENTRY_QUALITY = _env_float("AI_W_ENTRY_QUALITY", 0.12)
W_SMART_WHALE_ALIGN = _env_float("AI_W_SMART_WHALE_ALIGN", 0.08)
W_MCAP = _env_float("AI_W_MCAP", 0.06)
W_AI_ENTRY = _env_float("AI_W_AI_ENTRY", 0.08)
W_TIER = _env_float("AI_W_TIER", 0.05)
W_BUY_PRESS = _env_float("AI_W_BUY_PRESS", 0.06)
W_HIST_EDGE = _env_float("AI_W_HIST_EDGE", 0.05)

_WEIGHT_SUM = (
    W_POOL_AGE + W_LIQ + W_IMPACT + W_VOL5M + W_TOP10 + W_WHALES + W_TREND
    + W_FLOW + W_BOOK + W_SAFETY + W_PF + W_FRAUD + W_REGIME_CTX
    + W_ENTRY_QUALITY + W_SMART_WHALE_ALIGN + W_MCAP + W_AI_ENTRY
    + W_TIER + W_BUY_PRESS + W_HIST_EDGE
)
AI_WEIGHT_NORM = _env_float("AI_WEIGHT_NORM", max(_WEIGHT_SUM, 1.0))

# --- Bonuses / penalties
BONUS_VERIFIED = _env_float("AI_BONUS_VERIFIED", 0.03)
PENALTY_UNVERIFIED = _env_float("AI_PENALTY_UNVERIFIED", -0.05)
BONUS_LIQ_LOCK = _env_float("AI_BONUS_LIQ_LOCK", 0.03)
BONUS_RENOUNCED = _env_float("AI_BONUS_RENOUNCED", 0.02)
BONUS_NO_FREEZE = _env_float("AI_BONUS_NO_FREEZE", 0.02)

PENALTY_RUG_REPORT = _env_float("AI_PENALTY_RUG_REPORT", -0.02)  # per report
RUG_REPORT_CAP = _env_int("AI_RUG_REPORT_CAP", 5)

WHALE_TOUCH_BONUS = _env_float("AI_WHALE_TOUCH_BONUS", 0.03)
WHALE_TOUCH_BONUS_CAP = _env_float("AI_WHALE_TOUCH_BONUS_CAP", 0.15)

# --- Normalization anchors
ANCHOR_POOL_AGE = _env_float("AI_ANCHOR_POOL_AGE", 120.0)
ANCHOR_LIQ = _env_float("AI_ANCHOR_LIQ", 100_000.0)
ANCHOR_IMPACT_BPS = _env_float("AI_ANCHOR_IMPACT_BPS", 150.0)
ANCHOR_VOL5M = _env_float("AI_ANCHOR_VOL5M", 5.0)

ANCHOR_TOP10_LO = _env_float("AI_ANCHOR_TOP10_GOOD", 20.0)
ANCHOR_TOP10_HI = _env_float("AI_ANCHOR_TOP10_BAD", 90.0)

ANCHOR_FLOW_IMB = _env_float("AI_ANCHOR_FLOW_IMB", 0.65)
ANCHOR_BOOK_IMB = _env_float("AI_ANCHOR_BOOK_IMB", 0.60)
ANCHOR_MCAP = _env_float("AI_ANCHOR_MCAP", 250_000.0)

# Momentum clamps
RET5M_POS_CEIL = _env_float("AI_RET5M_POS_CEIL", 9.0)
RET5M_NEG_FLOOR = _env_float("AI_RET5M_NEG_FLOOR", -9.0)
RET15M_POS_CEIL = _env_float("AI_RET15M_POS_CEIL", 18.0)
RET15M_NEG_FLOOR = _env_float("AI_RET15M_NEG_FLOOR", -18.0)

# Position sizing transform
AI_SIZE_SCALAR_EXP = _env_float("AI_SIZE_SCALAR_EXP", 1.0)
AI_SIZE_HIST_EDGE_STRENGTH = _env_float("AI_SIZE_HIST_EDGE_STRENGTH", 0.35)
AI_SIZE_MIN_IF_OK = _env_float("AI_SIZE_MIN_IF_OK", 0.0)

AI_BUY_DEBUG_LOG = _env_bool("AI_BUY_DEBUG_LOG", False)
AI_BUY_ATTACH_PARTS = _env_bool("AI_BUY_ATTACH_PARTS", False)

# Test-mode bypass (soft gate only)
AI_PRE_BYPASS_FOR_TEST = _env_bool("AI_PRE_BYPASS_FOR_TEST", False)


# =============================================================================
# Hard filters (strict reject reasons)
# =============================================================================


def _hard_filters(c: Candidate) -> Tuple[bool, List[str]]:
    r: List[str] = []

    # pool age
    if c.pool_age_min < AI_MIN_POOL_AGE_MIN:
        r.append(f"pool_too_new({c.pool_age_min:.1f}m<{AI_MIN_POOL_AGE_MIN:.1f}m)")

    # liquidity (Pump infra softening)
    if c.liq_usd < AI_MIN_LIQ_USD:
        if AI_INFRA_NO_ACCOUNT_SOFT_LIQ and bool(c.infra_no_account) and _is_pump_source(c.source):
            # soft warning only; scoring will include infra penalty
            pass
        else:
            r.append(f"low_liq(${c.liq_usd:,.0f}<${AI_MIN_LIQ_USD:,.0f})")

    # impact
    if c.impact_1k_bps > AI_MAX_IMPACT_1K_BPS:
        r.append(f"high_impact({c.impact_1k_bps:.0f}bps>{AI_MAX_IMPACT_1K_BPS:.0f}bps)")

    # top10 concentration
    if c.top10_pct > AI_MAX_TOP10_PCT:
        r.append(f"top10_concentration({c.top10_pct:.1f}%>{AI_MAX_TOP10_PCT:.1f}%)")

    # alive-ness
    if c.vol5m_pct < AI_MIN_VOL5M_PCT:
        r.append(f"too_quiet(vol5m={c.vol5m_pct:.2f}%<{AI_MIN_VOL5M_PCT:.2f}%)")

    # honeypot
    if c.honeypot_flag:
        r.append("honeypot_flag")

    # dev wallet
    if c.dev_wallet_pct is not None and _nz(c.dev_wallet_pct) > AI_MAX_DEV_WALLET_PCT:
        r.append(f"dev_wallet_too_big({c.dev_wallet_pct:.1f}%>{AI_MAX_DEV_WALLET_PCT:.1f}%)")

    # whale touches (optional)
    if AI_MIN_WHALE_TOUCHES_1H > 0 and c.whale_touches_1h < AI_MIN_WHALE_TOUCHES_1H:
        r.append(f"few_whale_touches({c.whale_touches_1h}<{AI_MIN_WHALE_TOUCHES_1H})")

    # structural requirements
    if AI_REQUIRE_LIQ_LOCK and not bool(c.liq_locked):
        r.append("liq_not_locked")
    if AI_REQUIRE_MINT_RENOUNCED and not bool(c.mint_renounced):
        r.append("mint_not_renounced")
    if AI_REQUIRE_NO_FREEZE and not bool(c.freeze_auth_disabled):
        r.append("freeze_authority_enabled")

    # fraud cutoff
    f01 = _fraud_to_01(c.ai_fraud_score)
    if f01 is not None and f01 >= AI_MAX_RUG_SCORE:
        r.append(f"high_fraud_score({f01:.2f}>={AI_MAX_RUG_SCORE:.2f})")

    # PF floors (only if present)
    if c.pf_final_score is not None and _nz(c.pf_final_score) < AI_MIN_PF_FINAL:
        r.append(f"pf_final_below_min({_nz(c.pf_final_score):.3f}<{AI_MIN_PF_FINAL:.3f})")
    if c.pf_risk_adjusted_final is not None and _nz(c.pf_risk_adjusted_final) < AI_MIN_PF_RISK_ADJ:
        r.append(f"pf_risk_adj_below_min({_nz(c.pf_risk_adjusted_final):.3f}<{AI_MIN_PF_RISK_ADJ:.3f})")

    # AI entry minimum (optional)
    if AI_MIN_AI_ENTRY_PROB > 0.0 and c.ai_entry_prob is not None and _nz(c.ai_entry_prob) < AI_MIN_AI_ENTRY_PROB:
        r.append(f"ai_entry_prob_low({_nz(c.ai_entry_prob):.3f}<{AI_MIN_AI_ENTRY_PROB:.3f})")

    # FeatureBuilder hard blocks
    if AI_USE_ENTRY_BLOCK_FLAG and _nz(c.entry_block_flag, 0.0) >= 0.5:
        r.append("entry_block_flag_from_features")
    if _nz(c.is_dead_like, 0.0) >= 1.0:
        r.append("dead_like_heuristic")

    # tier guard (optional)
    if AI_MIN_ENTRY_TIER > 0.0 and c.entry_tier_bucket is not None and _nz(c.entry_tier_bucket) < AI_MIN_ENTRY_TIER:
        r.append(f"entry_tier_low({_nz(c.entry_tier_bucket):.1f}<{AI_MIN_ENTRY_TIER:.1f})")

    # structural quality floor (optional)
    if AI_MIN_STRUCTURAL_QUALITY > 0.0 and c.structural_quality_score is not None and _nz(c.structural_quality_score) < AI_MIN_STRUCTURAL_QUALITY:
        r.append(f"low_structural_quality({_nz(c.structural_quality_score):.3f}<{AI_MIN_STRUCTURAL_QUALITY:.3f})")

    # entry risk cap (optional)
    if AI_MAX_ENTRY_RISK_SCORE < 1.0 and c.entry_risk_score is not None and _nz(c.entry_risk_score) > AI_MAX_ENTRY_RISK_SCORE:
        r.append(f"entry_risk_too_high({_nz(c.entry_risk_score):.3f}>{AI_MAX_ENTRY_RISK_SCORE:.3f})")

    # market-cap band (optional)
    if AI_MIN_MCAP_USD > 0.0 or AI_MAX_MCAP_USD > 0.0:
        mcap = _nz(c.mcap_usd, 0.0)
        if AI_MIN_MCAP_USD > 0.0 and mcap > 0.0 and mcap < AI_MIN_MCAP_USD:
            r.append(f"mcap_too_small(${mcap:,.0f}<${AI_MIN_MCAP_USD:,.0f})")
        if AI_MAX_MCAP_USD > 0.0 and mcap > AI_MAX_MCAP_USD:
            r.append(f"mcap_too_large(${mcap:,.0f}>${AI_MAX_MCAP_USD:,.0f})")

    # session drawdown
    if AI_CTX_MAX_DRAWDOWN_PCT > 0 and c.ctx_daily_drawdown_pct is not None and _nz(c.ctx_daily_drawdown_pct) <= -AI_CTX_MAX_DRAWDOWN_PCT:
        r.append(f"session_drawdown({_nz(c.ctx_daily_drawdown_pct):.2f}%<=-{AI_CTX_MAX_DRAWDOWN_PCT:.1f}%)")

    # lock after target pnl
    if AI_CTX_LOCK_AFTER_TARGET_PNL > 0 and c.ctx_daily_pnl_pct is not None and c.ctx_risk_mode is not None:
        if _nz(c.ctx_daily_pnl_pct) >= AI_CTX_LOCK_AFTER_TARGET_PNL and _nz(c.ctx_risk_mode) <= 1.0:
            r.append(f"session_target_hit({_nz(c.ctx_daily_pnl_pct):.2f}%>={AI_CTX_LOCK_AFTER_TARGET_PNL:.2f}%)")

    # open positions ratio
    if AI_CTX_MAX_OPEN_POS_RATIO > 0 and c.ctx_open_positions is not None and c.ctx_max_positions is not None and _nz(c.ctx_max_positions) > 0:
        ratio = _safe_div(_nz(c.ctx_open_positions), _nz(c.ctx_max_positions), 0.0)
        if ratio >= AI_CTX_MAX_OPEN_POS_RATIO:
            r.append(f"too_many_positions({int(_nz(c.ctx_open_positions))}/{int(_nz(c.ctx_max_positions))},ratio={ratio:.2f}>={AI_CTX_MAX_OPEN_POS_RATIO:.2f})")

    # risk capacity (FeatureBuilder / risk_manager)
    if AI_USE_RISK_CAPACITY_GUARD and c.risk_capacity_score is not None and _nz(c.risk_capacity_score) < AI_MIN_RISK_CAPACITY:
        r.append(f"no_risk_capacity({_nz(c.risk_capacity_score):.3f}<{AI_MIN_RISK_CAPACITY:.3f})")

    # per-bucket capacity
    if AI_USE_BUCKET_CAPACITY_GUARD:
        util = _nz(c.risk_bucket_utilization, 0.0)
        cap = _nz(c.risk_bucket_capacity_score, 1.0)
        if util >= 1.0:
            r.append("risk_bucket_full")
        elif cap < AI_MIN_BUCKET_CAPACITY:
            r.append(f"risk_bucket_no_capacity({cap:.3f}<{AI_MIN_BUCKET_CAPACITY:.3f})")

    # global kill-switch
    if _nz(c.ctx_global_block_flag, 0.0) >= 0.5:
        r.append("global_risk_block")

    # entry quality (optional)
    if AI_MIN_ENTRY_QUALITY > 0.0 and c.entry_quality_score is not None and _nz(c.entry_quality_score) < AI_MIN_ENTRY_QUALITY:
        r.append(f"low_entry_quality({_nz(c.entry_quality_score):.3f}<{AI_MIN_ENTRY_QUALITY:.3f})")

    # buy-pressure (optional; normalized 0..1)
    if AI_MIN_BUY_PRESSURE > 0.0 and c.buy_pressure_score is not None:
        bp = _buy_pressure_component(c.buy_pressure_score)
        if bp < AI_MIN_BUY_PRESSURE:
            r.append(f"buy_pressure_low({bp:.3f}<{AI_MIN_BUY_PRESSURE:.3f})")

    # smart-whale alignment floor (optional; normalized 0..1)
    if AI_MIN_SMART_ALIGN > 0.0:
        swa = _clamp(_smart_whale_align_component(c) / 1.5, 0.0, 1.0)
        if swa < AI_MIN_SMART_ALIGN:
            r.append(f"smart_whale_align_low({swa:.3f}<{AI_MIN_SMART_ALIGN:.3f})")

    # historical edge floor (optional)
    if AI_MIN_HIST_EDGE > 0.0:
        he = _historical_edge_component(c)
        if he < AI_MIN_HIST_EDGE:
            r.append(f"hist_edge_low({he:.3f}<{AI_MIN_HIST_EDGE:.3f})")

    # self-learning explicit blocks
    if AI_BUY_SELF_LEARN_ENABLED:
        if _nz(c.self_learn_entry_block, 0.0) >= 0.5:
            r.append("self_learning_block")
        if c.meta:
            if _nz(c.meta.get("sl_entry_block"), 0.0) >= 0.5 and "self_learning_block" not in r:
                r.append("self_learning_block_meta")

    return (len(r) == 0), r


# =============================================================================
# Soft scoring components
# =============================================================================


def _score_pool_age(mins: float) -> float:
    return _clamp(_nz(mins) / max(ANCHOR_POOL_AGE, 1e-9))


def _score_liq(usd: float) -> float:
    return _clamp(_nz(usd) / max(ANCHOR_LIQ, 1e-9))


def _score_impact(bps: float) -> float:
    return _clamp(1.0 - _clamp(_nz(bps) / max(ANCHOR_IMPACT_BPS, 1e-9)))


def _score_vol5m(vol_pct: float) -> float:
    return _clamp(abs(_nz(vol_pct)) / max(ANCHOR_VOL5M, 1e-9))


def _score_top10(conc_pct: float) -> float:
    x = _nz(conc_pct)
    if x <= ANCHOR_TOP10_LO:
        return 1.0
    if x >= ANCHOR_TOP10_HI:
        return 0.0
    return _clamp(1.0 - (x - ANCHOR_TOP10_LO) / max(ANCHOR_TOP10_HI - ANCHOR_TOP10_LO, 1e-9))


def _score_whales(touches: int, whale_share: Optional[float]) -> float:
    base = _clamp(_nz(touches) / 5.0)  # saturate at ~5 touches
    share = _clamp(_nz(whale_share, 0.3), 0.0, 1.0)
    if share > 0.75:
        base -= (share - 0.75) * 1.0
    else:
        base += share * 0.3
    return _clamp(base)


def _score_mcap(mcap_usd: Optional[float]) -> float:
    m = _nz(mcap_usd, 0.0)
    if m <= 0.0:
        return 0.3
    x = m / max(ANCHOR_MCAP, 1e-9)
    z = math.log(max(x, 1e-9))
    s = math.exp(-0.5 * (z / 1.2) ** 2)
    return _clamp(s)


def _tier_component(entry_tier: Optional[float], size_tier: Optional[float]) -> float:
    e = _clamp(_nz(entry_tier, 0.0) / 3.0, 0.0, 1.0)
    s = _clamp(_nz(size_tier, 0.0) / 3.0, 0.0, 1.0)
    align = 0.5 * (e + s)
    if e > 0.0 and s > 0.0:
        align += 0.15 * min(e, s)
    return _clamp(align)


def _trend_component(ret5m: Optional[float], ret15m: Optional[float]) -> float:
    r5 = _nz(ret5m, 0.0)
    r15 = _nz(ret15m, 0.0)

    if r5 >= RET5M_POS_CEIL or r15 >= RET15M_POS_CEIL:
        return 0.25
    if r5 <= RET5M_NEG_FLOOR or r15 <= RET15M_NEG_FLOOR:
        return 0.20

    def softstep(r: float, w: float) -> float:
        return _clamp((r + w) / (2.0 * w))

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
    b = _nz(buys, 0.0)
    s = _nz(sells, 0.0)
    bu = _nz(buy_usd, 0.0)
    su = _nz(sell_usd, 0.0)
    v5 = _nz(vol_5m_usd, 0.0)

    cnt_total = b + s
    not_total = bu + su

    cnt_imb = _safe_div(b, cnt_total, 0.5)
    not_imb = _safe_div(bu, not_total, 0.5)
    share = 0.5 * cnt_imb + 0.5 * not_imb

    vol_energy = _clamp(v5 / 8_000.0)  # saturate around $8k/5m

    diff = abs(share - ANCHOR_FLOW_IMB)
    base = _clamp(1.0 - diff / max(ANCHOR_FLOW_IMB, 1.0 - ANCHOR_FLOW_IMB))

    fa = _clamp(_nz(flow_accum, 0.0))
    fp = _clamp(_nz(flow_panic, 0.0))
    base = _clamp(base + 0.15 * fa - 0.20 * fp)

    return _clamp(0.7 * base + 0.3 * vol_energy)


def _book_component(bid_usd: Optional[float], ask_usd: Optional[float]) -> float:
    bid = _nz(bid_usd, 0.0)
    ask = _nz(ask_usd, 0.0)
    tot = bid + ask
    if tot <= 0.0:
        return 0.5
    bid_share = bid / tot
    diff = abs(bid_share - ANCHOR_BOOK_IMB)
    return _clamp(1.0 - diff / max(ANCHOR_BOOK_IMB, 1.0 - ANCHOR_BOOK_IMB))


def _safety_component(
    verified: Optional[bool],
    liq_locked: Optional[bool],
    mint_renounced: Optional[bool],
    freeze_disabled: Optional[bool],
    rug_reports: Optional[int],
) -> Tuple[float, float]:
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
        rr = min(rr, RUG_REPORT_CAP)
        bonus += rr * PENALTY_RUG_REPORT
        base -= min(0.25, 0.04 * rr)

    return _clamp(base), float(bonus)


def _pf_component(pf_mom: Optional[float], pf_final: Optional[float], pf_risk_adj: Optional[float]) -> float:
    m = _clamp(_nz(pf_mom, 0.0))
    f = _clamp(_nz(pf_final, 0.0))
    r = _clamp(_nz(pf_risk_adj, f))
    if f == 0.0 and r == 0.0 and m == 0.0:
        return 0.0
    return _clamp(1.2 * r + 0.6 * f + 0.4 * m)


def _fraud_component(ai_fraud_score: Optional[float]) -> float:
    f01 = _fraud_to_01(ai_fraud_score)
    if f01 is None:
        return 0.5
    return _clamp(1.0 - f01)


def _historical_edge_component(c: Candidate) -> float:
    if c.hist_edge_score is not None:
        h = _nz(c.hist_edge_score, 0.5)
    else:
        win = _nz(c.hist_win_rate, 0.5)
        if win > 1.5:
            win = win / 100.0
        win = _clamp(win, 0.0, 1.0)

        rr = _nz(c.hist_rr_ratio, 1.0)
        rr_norm = _clamp((rr - 0.8) / 2.0, 0.0, 1.0)
        h = 0.65 * win + 0.35 * rr_norm

    expl = _clamp(_nz(c.exploration_score, 0.5), 0.0, 1.0)
    return _clamp(0.9 * _clamp(h, 0.0, 1.0) + 0.1 * expl, 0.0, 1.0)


def _regime_ctx_component(c: Candidate) -> float:
    trend = _clamp(_nz(c.regime_score, _nz(c.regime_trend, 0.0)), -1.0, 1.0)
    liq = _clamp(_nz(c.regime_liquidity, 0.5))
    manip = _clamp(_nz(c.regime_manipulation_risk, 0.0))
    vol = _clamp(_nz(c.regime_volatility, 0.5))

    ctx_dd = _nz(c.ctx_daily_drawdown_pct, 0.0)
    ctx_pnl = _nz(c.ctx_daily_pnl_pct, 0.0)
    ctx_risk = _nz(c.ctx_risk_mode, 1.0)

    exposure_frac = _clamp(_nz(c.ctx_exposure_frac, 0.0), 0.0, 1.0)
    win_rate = _clamp(_nz(c.ctx_win_rate_pct, 50.0) / 100.0, 0.0, 1.0)
    loss_streak = _nz(c.ctx_recent_loss_streak, 0.0)

    rp_cons = _clamp(_nz(c.risk_profile_conservative, 0.0), 0.0, 1.0)
    rp_bal = _clamp(_nz(c.risk_profile_balanced, 0.0), 0.0, 1.0)
    rp_agg = _clamp(_nz(c.risk_profile_aggressive, 0.0), 0.0, 1.0)

    cap_delta = _nz(c.ai_capacity_delta, 0.0)
    reg_gate = _clamp(_nz(c.regime_entry_gate_factor, 1.0), 0.2, 1.6)

    raw = 0.0
    raw += trend * 0.32
    raw += (liq - 0.5) * 0.20
    raw -= max(0.0, manip - 0.4) * 0.7
    raw += (vol - 0.5) * 0.16

    if ctx_dd < 0:
        raw -= min(abs(ctx_dd) / 10.0, 1.5) * 0.18
    if 0.0 < ctx_pnl < 6.0:
        raw += min(ctx_pnl / 6.0, 1.0) * 0.10

    if ctx_risk <= 0.0:
        raw -= 0.6
    elif ctx_risk == 3.0:
        raw -= 0.2
    elif ctx_risk >= 2.0:
        raw += 0.10

    raw -= exposure_frac * 0.25
    raw += (win_rate - 0.5) * 0.20

    if loss_streak > 0:
        raw -= min(loss_streak / 4.0, 1.0) * 0.22

    raw -= 0.10 * rp_cons
    raw += 0.05 * rp_bal
    raw += 0.10 * rp_agg

    if cap_delta < 0.0:
        raw -= min(abs(cap_delta) / 0.6, 1.0) * 0.20
    elif cap_delta > 0.0:
        raw += min(cap_delta / 0.6, 1.0) * 0.10

    raw += (reg_gate - 1.0) * 0.15
    return _sigmoid(raw)


def _entry_quality_component(c: Candidate) -> float:
    if c.entry_quality_score is not None:
        return _clamp(_nz(c.entry_quality_score), 0.0, 1.5)

    structural = _clamp(_nz(c.structural_quality_score, 0.0), 0.0, 1.5)
    pf_risk_adj = _clamp(_nz(c.pf_risk_adjusted_final, _nz(c.pf_final_score, 0.0)), 0.0, 1.0)
    entry_micro = _clamp(_nz(c.entry_micro_alpha, 0.0), 0.0, 1.5)

    return _clamp(0.45 * structural + 0.30 * pf_risk_adj + 0.25 * entry_micro, 0.0, 1.5)


def _smart_whale_align_component(c: Candidate) -> float:
    if c.smart_whale_alignment is not None:
        base = _clamp(_nz(c.smart_whale_alignment), 0.0, 1.5)
    else:
        smart = _clamp(_nz(c.smart_wallet_score, 0.5), 0.0, 1.0)
        flow_raw = _clamp(_nz(c.smart_flow_raw, 0.0), -1.0, 1.0)
        smart_bias = 0.5 * (flow_raw + 1.0)

        whales = _clamp(_nz(c.whales_5m, _nz(c.whale_touches_1h, 0.0)) / 5.0, 0.0, 1.0)
        whale_share = _clamp(_nz(c.whale_share_5m, _nz(c.whale_share_1h, 0.3)), 0.0, 1.0)
        buyers = _clamp(_nz(c.unique_buyers_5m, _nz(c.unique_buyers_1h, 0.0)) / 50.0, 0.0, 1.0)

        if whale_share > 0.8:
            whale_term = max(0.0, whales * (1.1 - (whale_share - 0.8) * 2.0))
        else:
            whale_term = whales * (0.7 + 0.3 * whale_share)

        base = 0.40 * smart + 0.25 * smart_bias + 0.20 * whale_term + 0.15 * buyers

    base = _clamp(base, 0.0, 1.5)

    wlf = _clamp(_nz(c.whale_liquidity_footprint, 0.0), 0.0, 1.0)
    if wlf > 0.0:
        base *= (0.9 + 0.2 * wlf)

    return _clamp(base, 0.0, 1.5)


def _ai_entry_component(prob: Optional[float], score: Optional[float]) -> float:
    if prob is not None:
        return _clamp(_nz(prob), 0.0, 1.0)
    if score is not None:
        return _clamp(_nz(score), 0.0, 1.0)
    return 0.0


def _buy_pressure_component(buy_press: Optional[float]) -> float:
    return _clamp(_nz(buy_press, 0.0) / 1.5, 0.0, 1.0)


def _size_scalar_from_candidate(c: Candidate, entry_quality: float) -> float:
    if c.position_size_scalar is not None:
        base = _clamp(_nz(c.position_size_scalar), 0.0, 1.0)
    else:
        base = _clamp(entry_quality / 1.5, 0.0, 1.0)

    if c.position_size_bucket is not None:
        tier_norm = _clamp(_nz(c.position_size_bucket, 0.0) / 3.0, 0.0, 1.0)
        base = _clamp(0.8 * base + 0.2 * tier_norm, 0.0, 1.0)

    cap = _clamp(_nz(c.risk_capacity_score, 1.0), 0.0, 1.0)
    size = base * cap

    cap_delta = _nz(c.ai_capacity_delta, 0.0)
    if cap_delta < 0:
        size *= (1.0 + max(cap_delta / 0.6, -1.0))
    elif cap_delta > 0:
        size *= (1.0 + min(cap_delta / 0.6, 0.25))

    util = _clamp(_nz(c.risk_bucket_utilization, 0.0), 0.0, 1.5)
    bucket_cap = _clamp(_nz(c.risk_bucket_capacity_score, 1.0), 0.0, 1.0)
    if util > 0.7:
        shrink = 1.0 - 0.6 * min((util - 0.7) / 0.3, 1.0)
        size *= max(0.4, shrink)
    size *= (0.7 + 0.3 * bucket_cap)

    reg = _clamp(_nz(c.regime_score, _nz(c.regime_trend, 0.0)), -1.0, 1.0)
    if reg <= -0.7:
        size *= 0.4
    elif reg <= -0.3:
        size *= 0.6
    elif reg >= 0.7:
        size *= 1.15
    elif reg >= 0.3:
        size *= 1.05

    mode = _nz(c.ctx_risk_mode, 1.0)
    if mode <= 0.0:
        size *= 0.0
    elif mode == 3.0:
        size *= 0.6
    elif mode >= 2.0:
        size *= 1.1

    exposure = _clamp(_nz(c.ctx_exposure_frac, 0.0), 0.0, 1.0)
    if exposure > 0.7:
        shrink = 1.0 - 0.6 * ((exposure - 0.7) / 0.3)
        size *= max(0.4, shrink)

    loss_streak = _nz(c.ctx_recent_loss_streak, 0.0)
    if loss_streak >= 3:
        size *= 0.5
    elif loss_streak == 2:
        size *= 0.7

    dead = _nz(c.is_dead_like, 0.0)
    if dead >= 1.0:
        size *= 0.0

    rugginess = 1.0 - _fraud_component(c.ai_fraud_score)
    size *= (1.0 - 0.6 * rugginess)

    if AI_BUY_SELF_LEARN_ENABLED and c.meta:
        try:
            sb = float(c.meta.get("sl_size_scalar_boost", 0.0) or 0.0)
        except Exception:
            sb = 0.0
        if sb != 0.0:
            sb = max(-0.4, min(0.4, sb))
            size *= (1.0 + sb)

    if AI_SIZE_HIST_EDGE_STRENGTH > 0.0:
        try:
            he = _historical_edge_component(c)
            factor = _clamp(1.0 + AI_SIZE_HIST_EDGE_STRENGTH * (he - 0.5), 0.5, 1.5)
            size *= factor
        except Exception:
            pass

    size = _clamp(size, 0.0, 1.0)

    if AI_SIZE_MIN_IF_OK > 0.0 and size > 0.0:
        size = max(size, AI_SIZE_MIN_IF_OK)

    if AI_SIZE_SCALAR_EXP != 1.0 and size > 0.0:
        size = _clamp(size ** AI_SIZE_SCALAR_EXP, 0.0, 1.0)

    return size


def _source_gate_threshold(source: Optional[str]) -> float:
    sk = _normalize_source_key(source)
    if not sk:
        return AI_SCORE_GATE
    if sk in AI_SCORE_GATE_OVERRIDES:
        try:
            return float(AI_SCORE_GATE_OVERRIDES[sk])
        except Exception:
            return AI_SCORE_GATE
    return AI_SCORE_GATE


def _source_score_bonus(source: Optional[str]) -> float:
    sk = _normalize_source_key(source)
    if not sk:
        return 0.0
    v = AI_SCORE_SOURCE_BONUS.get(sk)
    try:
        return float(v) if v is not None else 0.0
    except Exception:
        return 0.0


def _apply_self_learning_overrides(score: float, threshold: float, c: Candidate) -> Tuple[float, float, str]:
    if not AI_BUY_SELF_LEARN_ENABLED:
        return score, threshold, ""

    bits: List[str] = []
    meta = c.meta or {}

    raw_boost = 0.0
    if c.self_learn_entry_boost is not None:
        raw_boost += _nz(c.self_learn_entry_boost, 0.0)
    try:
        raw_boost += float(meta.get("sl_entry_boost", 0.0) or 0.0)
    except Exception:
        pass

    if raw_boost != 0.0:
        mb = AI_BUY_SELF_LEARN_MAX_ABS_BOOST
        raw_boost = max(-mb, min(mb, raw_boost))
        score = _clamp(score + raw_boost, 0.0, 1.0)
        bits.append(f"sl_boost={raw_boost:+.3f}")

    raw_gate = 0.0
    if c.self_learn_entry_gate_offset is not None:
        raw_gate += _nz(c.self_learn_entry_gate_offset, 0.0)
    try:
        raw_gate += float(meta.get("sl_entry_gate_offset", 0.0) or 0.0)
    except Exception:
        pass

    if raw_gate != 0.0:
        mg = AI_BUY_SELF_LEARN_MAX_ABS_GATE
        raw_gate = max(-mg, min(mg, raw_gate))
        raw_gate *= AI_BUY_SELF_LEARN_GATE_WEIGHT
        threshold = _clamp(threshold + raw_gate, 0.0, 0.99)
        bits.append(f"sl_gate={raw_gate:+.3f}")

    try:
        if meta.get("sl_force_entry_flag"):
            bits.append("sl_force_entry_hint")
    except Exception:
        pass

    return score, threshold, ",".join(bits)


# =============================================================================
# Public API
# =============================================================================


def score_only(c: Candidate) -> Dict[str, Any]:
    """
    Full scoring breakdown (hard filters evaluated but not final-gated).
    Returns a dict suitable for logging/analytics and gate_feature_vector().
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
        c.orderflow_buys_1m, c.orderflow_sells_1m,
        c.orderflow_buy_usd_1m, c.orderflow_sell_usd_1m,
        c.vol_5m_usd, c.flow_accumulation_score, c.flow_panic_dump_score,
    )
    parts["book"] = _book_component(c.orderbook_bid_usd, c.orderbook_ask_usd)

    safety_base, safety_bonus = _safety_component(
        c.verified, c.liq_locked, c.mint_renounced, c.freeze_auth_disabled, c.rugpull_reports
    )
    parts["safety"] = safety_base

    parts["pf"] = _pf_component(c.pf_momentum_score, c.pf_final_score, c.pf_risk_adjusted_final)
    parts["fraud_safety"] = _fraud_component(c.ai_fraud_score)
    parts["regime_ctx"] = _regime_ctx_component(c)

    entry_quality = _entry_quality_component(c)  # 0..1.5
    parts["entry_quality"] = _clamp(entry_quality / 1.5, 0.0, 1.0)

    smart_align = _smart_whale_align_component(c)  # 0..1.5
    parts["smart_whale_align"] = _clamp(smart_align / 1.5, 0.0, 1.0)

    parts["mcap"] = _score_mcap(c.mcap_usd)
    parts["tier"] = _tier_component(c.entry_tier_bucket, c.position_size_bucket)
    parts["ai_entry"] = _ai_entry_component(c.ai_entry_prob, c.ai_entry_score)
    parts["buy_pressure"] = _buy_pressure_component(c.buy_pressure_score)
    parts["hist_edge"] = _historical_edge_component(c)

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
        + W_HIST_EDGE * parts["hist_edge"]
    )

    whale_bonus = min(WHALE_TOUCH_BONUS_CAP, float(c.whale_touches_1h) * WHALE_TOUCH_BONUS)
    raw_score += whale_bonus + float(safety_bonus)

    infra_penalty = 0.0
    if bool(c.infra_no_account) and _is_pump_source(c.source):
        infra_penalty = AI_INFRA_NO_ACCOUNT_PENALTY
        raw_score += infra_penalty

    score = _clamp(raw_score / max(AI_WEIGHT_NORM, 1e-9), 0.0, 1.0)
    score = _clamp(score + _source_score_bonus(c.source), 0.0, 1.0)

    size_scalar = _size_scalar_from_candidate(c, entry_quality)

    if AI_BUY_DEBUG_LOG:
        try:
            log.debug(
                "BUY_SCORING sym=%s mint=%s src=%s score=%.3f size=%.3f gate=%.3f hard_ok=%s reasons=%s parts=%s",
                c.symbol, c.mint, _normalize_source_key(c.source), score, size_scalar,
                _source_gate_threshold(c.source), hard_ok, ";".join(hard_reasons),
                {k: round(v, 3) for k, v in parts.items()},
            )
        except Exception:
            pass

    detail: Dict[str, Any] = {
        "ok_hard": hard_ok,
        "hard_reasons": hard_reasons,
        "parts": parts,
        "whale_bonus": whale_bonus,
        "safety_bonus": float(safety_bonus),
        "infra_penalty": infra_penalty,
        "raw_score": raw_score,
        "score": score,
        "entry_quality": entry_quality,
        "size_scalar": size_scalar,
        "entry_block_flag": _nz(c.entry_block_flag, 0.0),
        "risk_capacity": _clamp(_nz(c.risk_capacity_score, 1.0), 0.0, 1.0),
        "risk_bucket_capacity": _clamp(_nz(c.risk_bucket_capacity_score, 1.0), 0.0, 1.0),
        "risk_bucket_utilization": _clamp(_nz(c.risk_bucket_utilization, 0.0), 0.0, 2.0),
        "ctx_global_block_flag": _nz(c.ctx_global_block_flag, 0.0),
        "gate_threshold": _source_gate_threshold(c.source),
        "entry_tier": _nz(c.entry_tier_bucket, 0.0),
        "position_size_tier": _nz(c.position_size_bucket, 0.0),
        "source": _normalize_source_key(c.source),
        "infra_no_account": bool(c.infra_no_account),
    }

    if AI_BUY_ATTACH_PARTS:
        try:
            detail["parts_json"] = json.dumps({k: round(float(v), 4) for k, v in parts.items()}, separators=(",", ":"))
        except Exception:
            pass

    return detail


def pass_gate(c: Candidate) -> Tuple[bool, float, str]:
    """
    Returns (ok, score, reason).

    ok=True when:
      - hard filters pass AND (score >= dynamic threshold), OR
      - AI_PRE_BYPASS_FOR_TEST=1 AND hard filters pass

    The *final* dynamic threshold used is stored into:
        c.meta["_ai_buy_last_gate_threshold"]
    """
    detail = score_only(c)

    if not detail["ok_hard"]:
        reason = "; ".join(detail["hard_reasons"])
        if bool(c.infra_no_account) and _is_pump_source(c.source):
            reason += " [infra_no_account_soft_liq]"
        return False, float(detail["score"]), reason

    score = float(detail["score"])
    threshold = float(detail["gate_threshold"])
    parts = detail["parts"]

    # TEST MODE: bypass soft gate (still respects hard filters)
    if AI_PRE_BYPASS_FOR_TEST:
        try:
            c.meta["_ai_buy_last_gate_threshold"] = float(threshold)
        except Exception:
            pass
        reason = (
            f"pass TEST_BYPASS score={score:.3f} thr={threshold:.3f} "
            f"size={detail['size_scalar']:.2f} eq={detail['entry_quality']:.2f} "
            f"(liq={parts['liq']:.2f},impact={parts['impact']:.2f},trend={parts['trend']:.2f},"
            f"flow={parts['flow']:.2f},book={parts['book']:.2f},pf={parts['pf']:.2f},"
            f"fraud_safety={parts['fraud_safety']:.2f},regime_ctx={parts['regime_ctx']:.2f},"
            f"entry_quality_norm={parts['entry_quality']:.2f},smart_whale_align={parts['smart_whale_align']:.2f},"
            f"mcap={parts['mcap']:.2f},tier={parts['tier']:.2f},ai_entry={parts['ai_entry']:.2f},"
            f"buy_pressure={parts['buy_pressure']:.2f},hist_edge={parts['hist_edge']:.2f})"
        )
        if bool(c.infra_no_account) and _is_pump_source(c.source):
            reason += " [infra_no_account_soft_liq]"
        reason += " [AI_PRE_BYPASS_FOR_TEST=1]"
        return True, score, reason

    # Regime-aware tightening
    if AI_REGIME_TIGHTEN_ENABLED:
        trend = _clamp(_nz(c.regime_score, _nz(c.regime_trend, 0.0)), -1.0, 1.0)
        manip = _clamp(_nz(c.regime_manipulation_risk, 0.0), 0.0, 1.0)
        if trend <= -0.7:
            threshold = max(threshold + AI_RISK_OFF_EXTRA_MARGIN, 0.80)
        if manip >= AI_MANIP_HIGH:
            threshold = max(threshold + 0.08, threshold)

    # ai_risk_model gate factor (multiplicative)
    if AI_USE_REGIME_GATE_FACTOR and c.regime_entry_gate_factor is not None:
        rg = _clamp(_nz(c.regime_entry_gate_factor, 1.0), 0.2, 1.6)
        delta = (1.0 - rg) * AI_REGIME_GATE_FACTOR_STRENGTH
        threshold = _clamp(threshold * (1.0 + delta), 0.0, 0.99)

    # bounded self-learning overrides
    score, threshold, sl_note = _apply_self_learning_overrides(score, threshold, c)

    try:
        c.meta["_ai_buy_last_gate_threshold"] = float(threshold)
    except Exception:
        pass

    if score >= threshold:
        reason = (
            f"pass score={score:.3f} thr={threshold:.3f} "
            f"size={detail['size_scalar']:.2f} eq={detail['entry_quality']:.2f} "
            f"(liq={parts['liq']:.2f},impact={parts['impact']:.2f},trend={parts['trend']:.2f},"
            f"flow={parts['flow']:.2f},book={parts['book']:.2f},pf={parts['pf']:.2f},"
            f"fraud_safety={parts['fraud_safety']:.2f},regime_ctx={parts['regime_ctx']:.2f},"
            f"entry_quality_norm={parts['entry_quality']:.2f},smart_whale_align={parts['smart_whale_align']:.2f},"
            f"mcap={parts['mcap']:.2f},tier={parts['tier']:.2f},ai_entry={parts['ai_entry']:.2f},"
            f"buy_pressure={parts['buy_pressure']:.2f},hist_edge={parts['hist_edge']:.2f})"
        )
        if bool(c.infra_no_account) and _is_pump_source(c.source):
            reason += " [infra_no_account_soft_liq]"
        if sl_note:
            reason += f" [self_learning:{sl_note}]"
        return True, score, reason

    rej = f"below_gate(score={score:.3f}<{threshold:.3f})"
    if bool(c.infra_no_account) and _is_pump_source(c.source):
        rej += " [infra_no_account_soft_liq]"
    if sl_note:
        rej += f" [self_learning:{sl_note}]"
    return False, score, rej


# =============================================================================
# Builders (snapshot → Candidate)
# =============================================================================


def from_pool_snapshot(d: Dict[str, Any]) -> Candidate:
    meta = d.get("meta") or {}
    return Candidate(
        mint=(d.get("mint") or d.get("token") or d.get("address") or d.get("baseMint") or "UNKNOWN"),
        symbol=(d.get("symbol") or d.get("ticker") or d.get("sym") or "UNK"),
        pool_age_min=_nz(d.get("pool_age_min") or d.get("token_age_min") or d.get("age_min") or d.get("age_minutes") or d.get("ageMinutes"), 0.0),
        liq_usd=_nz(d.get("liq_usd") or d.get("liquidity_usd") or d.get("liquidity") or d.get("liq"), 0.0),
        impact_1k_bps=_nz(d.get("impact_1k_bps") or d.get("impact_1000usd_bps") or d.get("price_impact_bps") or d.get("impact_bps"), 999.0),
        vol5m_pct=_nz(d.get("vol5m_pct") or d.get("vol_5m_pct") or d.get("volatility_5m_pct") or d.get("chg5") or d.get("price_change_5m_pct"), 0.0),
        top10_pct=_nz(d.get("top10_pct") or d.get("top_10_pct") or d.get("holder_top10_pct") or d.get("top10"), 100.0),
        honeypot_flag=bool(d.get("honeypot_flag") or d.get("rug_risk") or d.get("hp_flag") or d.get("is_honeypot") or False),
        whale_touches_1h=int(d.get("whale_touches_1h") or d.get("whale_hits_1h") or 0),
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
        ai_fraud_score=(d.get("ai_fraud_score") or d.get("ai_rug_score") or d.get("ai_rug_score_01")),
        pf_momentum_score=d.get("pf_momentum_score") or d.get("momentum_score"),
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
        ctx_global_block_flag=(d.get("ctx_global_block_flag") or d.get("global_entry_block_flag")),
        risk_bucket_utilization=d.get("risk_bucket_utilization"),
        risk_bucket_capacity_score=d.get("risk_bucket_capacity_score"),
        risk_profile_conservative=d.get("risk_profile_conservative"),
        risk_profile_balanced=d.get("risk_profile_balanced"),
        risk_profile_aggressive=d.get("risk_profile_aggressive"),
        ai_capacity_delta=d.get("ai_capacity_delta"),
        regime_entry_gate_factor=d.get("regime_entry_gate_factor"),
        ai_risk_profile_label=d.get("ai_risk_profile_label"),
        self_learn_entry_boost=d.get("self_learn_entry_boost"),
        self_learn_entry_gate_offset=d.get("self_learn_entry_gate_offset"),
        self_learn_entry_block=d.get("self_learn_entry_block"),
        hist_edge_score=d.get("hist_edge_score") or d.get("sl_hist_edge_score"),
        hist_win_rate=d.get("hist_win_rate") or d.get("sl_hist_win_rate"),
        hist_rr_ratio=d.get("hist_rr_ratio") or d.get("hist_rr"),
        exploration_score=d.get("exploration_score") or d.get("explore_score") or d.get("sl_exploration_score"),
        source=d.get("source"),
        meta=meta,
        infra_no_account=d.get("infra_no_account"),
    )


def from_feature_vector(feats: Dict[str, Any]) -> Candidate:
    meta = feats.get("meta") or {}
    return Candidate(
        mint=feats.get("mint") or feats.get("token") or "UNKNOWN",
        symbol=feats.get("symbol") or feats.get("sym") or "UNK",
        pool_age_min=_nz(feats.get("token_age_min") or feats.get("age_min") or feats.get("ageMinutes"), 0.0),
        liq_usd=_nz(feats.get("liq_usd") or feats.get("liquidity_usd") or feats.get("liq"), 0.0),
        impact_1k_bps=_nz(feats.get("impact_1k_bps") or feats.get("impact_1000usd_bps"), 999.0),
        vol5m_pct=_nz(feats.get("vol5m_pct") or feats.get("vol_5m_pct") or feats.get("price_change_5m_pct") or feats.get("chg5"), 0.0),
        top10_pct=_nz(feats.get("top10_pct"), 100.0),
        honeypot_flag=bool(feats.get("honeypot_flag") or feats.get("rug_risk") or feats.get("is_honeypot")),
        whale_touches_1h=int(feats.get("whale_touches_1h") or feats.get("whales_5m") or 0),
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
        ai_fraud_score=(feats.get("ai_fraud_score") or feats.get("ai_rug_score") or feats.get("ai_rug_score_01")),
        pf_momentum_score=feats.get("pf_momentum_score") or feats.get("momentum_score"),
        pf_final_score=feats.get("pf_final_score") or feats.get("final_score"),
        pf_risk_adjusted_final=feats.get("pf_risk_adjusted_final"),
        whale_share_1h=feats.get("whale_share_1h"),
        unique_buyers_1h=feats.get("unique_buyers_1h"),
        smart_wallet_score=feats.get("smart_wallet_score"),
        smart_flow_raw=feats.get("smart_flow_raw"),
        smart_whale_alignment=feats.get("smart_whale_alignment_consolidated") or feats.get("smart_whale_alignment"),
        unique_buyers_5m=feats.get("unique_buyers_5m"),
        whale_share_5m=feats.get("whale_share_5m"),
        whales_5m=feats.get("whales_5m"),
        buy_pressure_score=feats.get("buy_pressure_score") or feats.get("buy_pressure"),
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
        ctx_daily_pnl_pct=feats.get("ctx_daily_realized_pnl_pct") or feats.get("ctx_daily_pnl_pct"),
        ctx_risk_mode=feats.get("ctx_risk_mode"),
        ctx_exposure_frac=feats.get("ctx_exposure_frac"),
        ctx_exposure_usd=feats.get("ctx_exposure_usd"),
        ctx_win_rate_pct=feats.get("risk_win_rate_pct") or feats.get("ctx_win_rate_pct"),
        ctx_recent_loss_streak=feats.get("risk_loss_streak") or feats.get("ctx_recent_loss_streak"),
        ctx_global_block_flag=feats.get("ctx_global_block_flag") or feats.get("global_entry_block_flag"),
        risk_bucket_utilization=feats.get("risk_bucket_utilization"),
        risk_bucket_capacity_score=feats.get("risk_bucket_capacity_score"),
        risk_profile_conservative=feats.get("risk_profile_conservative"),
        risk_profile_balanced=feats.get("risk_profile_balanced"),
        risk_profile_aggressive=feats.get("risk_profile_aggressive"),
        ai_capacity_delta=feats.get("ai_capacity_delta"),
        regime_entry_gate_factor=feats.get("regime_entry_gate_factor"),
        ai_risk_profile_label=feats.get("ai_risk_profile_label"),
        self_learn_entry_boost=feats.get("self_learn_entry_boost"),
        self_learn_entry_gate_offset=feats.get("self_learn_entry_gate_offset"),
        self_learn_entry_block=feats.get("self_learn_entry_block"),
        hist_edge_score=feats.get("hist_edge_score"),
        hist_win_rate=feats.get("hist_win_rate") or feats.get("sl_hist_win_rate"),
        hist_rr_ratio=feats.get("hist_rr_ratio") or feats.get("sl_hist_rr"),
        exploration_score=feats.get("exploration_score") or feats.get("sl_exploration_score"),
        source=feats.get("scanner_source") or feats.get("source"),
        meta=meta,
        infra_no_account=feats.get("infra_no_account"),
    )


# =============================================================================
# Feature vector bridge (writes ai_buy_* into feats)
# =============================================================================


def gate_feature_vector(feats: Dict[str, Any]) -> Tuple[bool, float, str]:
    """
    Convenience bridge for scanners / Strategy:

        ok, score, reason = gate_feature_vector(features)

    Writes:
        feats["ai_buy_score"]
        feats["ai_buy_ok_hard"]
        feats["ai_buy_pass_gate"]
        feats["ai_buy_reason"]
        feats["ai_buy_gate_threshold"]   (dynamic threshold used)
        feats["ai_buy_size_scalar"]
        feats["ai_buy_parts_json"]       (optional)
    """
    cand = from_feature_vector(feats)
    ok, score, reason = pass_gate(cand)

    detail = score_only(cand)
    try:
        gate_thr = float(getattr(cand, "meta", {}).get("_ai_buy_last_gate_threshold", detail["gate_threshold"]))
    except Exception:
        gate_thr = float(detail["gate_threshold"])

    feats["ai_buy_score"] = float(score)
    feats["ai_buy_ok_hard"] = bool(detail["ok_hard"])
    feats["ai_buy_pass_gate"] = bool(ok)
    feats["ai_buy_reason"] = str(reason)
    feats["ai_buy_gate_threshold"] = float(gate_thr)
    feats["ai_buy_size_scalar"] = float(detail["size_scalar"])

    if AI_BUY_ATTACH_PARTS and "parts" in detail:
        try:
            feats["ai_buy_parts_json"] = detail.get("parts_json") or json.dumps(
                {k: round(float(v), 4) for k, v in detail["parts"].items()},
                separators=(",", ":"),
            )
        except Exception:
            pass

    return ok, float(score), str(reason)
