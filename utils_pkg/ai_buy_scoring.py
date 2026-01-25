from __future__ import annotations

"""
utils_pkg/ai_buy_scoring.py

AI Buy Scoring — V3+++ (God-Tier / quant-grade)
==============================================

Backwards-compatible scorer used by sniper / scanners / reinvestor:

    from utils_pkg.ai_buy_scoring import Candidate, pass_gate, score_only
    ok, score, why = pass_gate(Candidate(...))

Role
----
Deterministic “bouncer at the door” that runs just ahead of the entry engine:

  • Hard filters: fast reject, STABLE reason codes (cheap + deterministic)
  • Soft score:   composite 0..1 from many components (weights env-tunable)
  • Gate:         score >= dynamic threshold (source/regime/risk overlays)
  • Feature bridge: gate_feature_vector(feats) writes ai_buy_* keys into feats

Design constraints
------------------
- 100% deterministic & cheap (no OpenAI, no Redis, no HTTP).
- Safe defaults: missing fields degrade gracefully.
- Hard reject reasons default to stable codes (good for aggregation).
- Optional debug detail is available via explain() / score_only().

This module does NOT execute trades; it only gates candidates before quote/build/send.

Public API
----------
- score_only(c) -> dict breakdown (incl. hard filter info; score; size scalar; parts)
- pass_gate(c)  -> (ok, score, reason_str)
- explain(c)    -> dict: same as score_only + soft explanations + final threshold
- from_pool_snapshot(d) -> Candidate
- from_feature_vector(feats) -> Candidate
- gate_feature_vector(feats) -> (ok, score, reason_str) + writes ai_buy_* keys
"""

import json
import math
import os
from dataclasses import dataclass, field
from typing import Any

from logging_setup import get_logger

try:
    log = get_logger(__name__)
except TypeError:
    log = get_logger()

__all__ = [
    "Candidate",
    "explain",
    "from_feature_vector",
    "from_pool_snapshot",
    "gate_feature_vector",
    "pass_gate",
    "score_only",
]


# =============================================================================
# Helpers (deterministic + NaN/inf safe)
# =============================================================================


def _as_float(x: Any, fallback: float = 0.0) -> float:
    if x is None:
        return float(fallback)
    try:
        v = float(x)
        if math.isnan(v) or math.isinf(v):
            return float(fallback)
        return v
    except Exception:
        return float(fallback)


def _as_int(x: Any, fallback: int = 0) -> int:
    if x is None:
        return fallback
    try:
        return int(x)
    except Exception:
        return fallback


def _clamp(x: Any, lo: float = 0.0, hi: float = 1.0) -> float:
    v = _as_float(x, lo)
    return max(lo, min(hi, v))


def _safe_div(n: Any, d: Any, fallback: float = 0.0) -> float:
    nn = _as_float(n, 0.0)
    dd = _as_float(d, 0.0)
    if dd == 0.0:
        return float(fallback)
    return nn / dd


def _sigmoid(x: Any) -> float:
    v = _as_float(x, 0.0)
    try:
        return 1.0 / (1.0 + math.exp(-v))
    except OverflowError:
        return 1.0 if v > 0 else 0.0
    except Exception:
        return 0.5


def _env_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None or raw == "":
        return float(default)
    try:
        return float(raw)
    except Exception:
        return float(default)


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or raw == "":
        return int(default)
    try:
        return int(raw)
    except Exception:
        return int(default)


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return bool(default)
    s = str(raw).strip().lower()
    if s in ("1", "true", "yes", "y", "on"):
        return True
    if s in ("0", "false", "no", "n", "off"):
        return False
    return bool(default)


def _env_json(name: str) -> dict | None:
    raw = os.getenv(name)
    if not raw:
        return None
    try:
        obj = json.loads(raw)
        return obj if isinstance(obj, dict) else None
    except Exception:
        log.warning("ai_buy_scoring: could not parse JSON from %s", name)
        return None


def _json_compact(d: dict[str, Any]) -> str:
    # stable ordering for deterministic logs/feature payload
    return json.dumps(d, separators=(",", ":"), sort_keys=True)


def _normalize_source_key(source: str | None) -> str | None:
    if not source:
        return None
    s = str(source).strip().lower()
    if "pump" in s:
        return "pumpfun"
    if "rayd" in s:
        return "raydium"
    if "dex" in s:
        return "dexscreener"
    if "momo" in s or "momentum" in s:
        return "momo"
    return s


def _is_pump_source(source: str | None) -> bool:
    sk = _normalize_source_key(source) or ""
    return "pump" in sk


def _fraud_to_01(ai_fraud_score: float | None) -> float | None:
    if ai_fraud_score is None:
        return None
    f = _as_float(ai_fraud_score, 0.0)
    # accept 0..100 inputs
    if f > 1.5:
        f = f / 100.0
    return _clamp(f, 0.0, 1.0)


def _pick(d: dict[str, Any], *keys: str, default: Any = None) -> Any:
    for k in keys:
        if k in d and d.get(k) is not None:
            return d.get(k)
    return default


# =============================================================================
# Candidate schema
# =============================================================================


@dataclass
class Candidate:
    # Core identity
    mint: str
    symbol: str

    # Minimal hard-filter fields
    pool_age_min: float
    liq_usd: float
    impact_1k_bps: float
    vol5m_pct: float
    top10_pct: float
    honeypot_flag: bool
    whale_touches_1h: int

    # Optional safety structure
    verified: bool | None = None
    liq_locked: bool | None = None
    mint_renounced: bool | None = None
    freeze_auth_disabled: bool | None = None
    dev_wallet_pct: float | None = None

    # Momentum/returns
    ret5m_pct: float | None = None
    ret15m_pct: float | None = None

    # Orderflow (last 1m)
    orderflow_buys_1m: int | None = None
    orderflow_sells_1m: int | None = None
    orderflow_buy_usd_1m: float | None = None
    orderflow_sell_usd_1m: float | None = None

    # Depth proxy
    orderbook_bid_usd: float | None = None
    orderbook_ask_usd: float | None = None

    # External safety
    rugpull_reports: int | None = None

    # Liquidity/volume/buys (5m)
    mcap_usd: float | None = None
    vol_5m_usd: float | None = None
    buys_5m: int | None = None
    sells_5m: int | None = None

    # AI / PF overlays
    ai_fraud_score: float | None = None  # 0..1 or 0..100
    pf_momentum_score: float | None = None  # 0..1
    pf_final_score: float | None = None  # 0..1
    pf_risk_adjusted_final: float | None = None  # 0..1

    # Whale/holders
    whale_share_1h: float | None = None  # 0..1
    unique_buyers_1h: int | None = None

    # Smart money
    smart_wallet_score: float | None = None  # 0..1
    smart_flow_raw: float | None = None  # [-1,1]
    smart_whale_alignment: float | None = None  # 0..1.5
    unique_buyers_5m: float | None = None
    whale_share_5m: float | None = None
    whales_5m: float | None = None

    # Flow regime features
    buy_pressure_score: float | None = None
    flow_accumulation_score: float | None = None
    flow_panic_dump_score: float | None = None
    flow_conflict_score: float | None = None
    whale_liquidity_footprint: float | None = None

    # FeatureBuilder / entry overlays
    entry_quality_score: float | None = None  # 0..1.5
    position_size_scalar: float | None = None  # 0..1
    entry_block_flag: float | None = None  # 0/1
    entry_micro_alpha: float | None = None  # 0..1.5
    structural_quality_score: float | None = None  # 0..1.5
    is_dead_like: float | None = None  # 0/1
    risk_capacity_score: float | None = None  # 0..1
    entry_risk_score: float | None = None  # 0..1
    entry_tier_bucket: float | None = None  # 0..3
    position_size_bucket: float | None = None  # 0..3

    # Optional AI entry model head
    ai_entry_prob: float | None = None
    ai_entry_score: float | None = None

    # Regime/context
    regime_trend: float | None = None
    regime_liquidity: float | None = None
    regime_manipulation_risk: float | None = None
    regime_score: float | None = None
    regime_volatility: float | None = None

    ctx_open_positions: int | None = None
    ctx_max_positions: int | None = None
    ctx_daily_drawdown_pct: float | None = None
    ctx_daily_pnl_pct: float | None = None
    ctx_risk_mode: float | None = None  # 0 off, 1 normal, 2 agg, 3 defensive

    ctx_exposure_frac: float | None = None
    ctx_exposure_usd: float | None = None
    ctx_win_rate_pct: float | None = None
    ctx_recent_loss_streak: int | None = None

    ctx_global_block_flag: float | None = None
    risk_bucket_utilization: float | None = None
    risk_bucket_capacity_score: float | None = None

    # Risk profiles
    risk_profile_conservative: float | None = None
    risk_profile_balanced: float | None = None
    risk_profile_aggressive: float | None = None

    # ai_risk_model overlays
    ai_capacity_delta: float | None = None
    regime_entry_gate_factor: float | None = None
    ai_risk_profile_label: str | None = None

    # Self-learning hints
    self_learn_entry_boost: float | None = None
    self_learn_entry_gate_offset: float | None = None
    self_learn_entry_block: float | None = None

    # Historical edge overlays
    hist_edge_score: float | None = None
    hist_win_rate: float | None = None
    hist_rr_ratio: float | None = None
    exploration_score: float | None = None

    # Source + misc
    source: str | None = None
    meta: dict[str, Any] = field(default_factory=dict)

    # Pump infra flag
    infra_no_account: bool | None = None

    def __post_init__(self) -> None:
        self.mint = str(self.mint or "UNKNOWN")
        self.symbol = str(self.symbol or "UNK")
        self.pool_age_min = _as_float(self.pool_age_min, 0.0)
        self.liq_usd = _as_float(self.liq_usd, 0.0)
        self.impact_1k_bps = _as_float(self.impact_1k_bps, 999.0)
        self.vol5m_pct = _as_float(self.vol5m_pct, 0.0)
        self.top10_pct = _as_float(self.top10_pct, 100.0)
        self.honeypot_flag = bool(self.honeypot_flag)
        self.whale_touches_1h = _as_int(self.whale_touches_1h, 0)
        if self.meta is None:
            self.meta = {}


# =============================================================================
# Env thresholds / weights (module-level for compatibility)
# =============================================================================

# Hard filters
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

AI_MAX_RUG_SCORE = _env_float("AI_MAX_RUG_SCORE", 0.85)
AI_MIN_PF_FINAL = _env_float("AI_MIN_PF_FINAL", 0.08)
AI_MIN_PF_RISK_ADJ = _env_float("AI_MIN_PF_RISK_ADJ", 0.06)
AI_MIN_AI_ENTRY_PROB = _env_float("AI_MIN_AI_ENTRY_PROB", 0.0)

AI_MIN_ENTRY_QUALITY = _env_float("AI_MIN_ENTRY_QUALITY", 0.0)
AI_MIN_STRUCTURAL_QUALITY = _env_float("AI_MIN_STRUCTURAL_QUALITY", 0.0)
AI_MAX_ENTRY_RISK_SCORE = _env_float("AI_MAX_ENTRY_RISK_SCORE", 1.0)

AI_USE_ENTRY_BLOCK_FLAG = _env_bool("AI_USE_ENTRY_BLOCK_FLAG", True)
AI_USE_RISK_CAPACITY_GUARD = _env_bool("AI_USE_RISK_CAPACITY_GUARD", True)
AI_MIN_RISK_CAPACITY = _env_float("AI_MIN_RISK_CAPACITY", 0.02)

AI_MIN_ENTRY_TIER = _env_float("AI_MIN_ENTRY_TIER", 0.0)

AI_USE_BUCKET_CAPACITY_GUARD = _env_bool("AI_USE_BUCKET_CAPACITY_GUARD", True)
AI_MIN_BUCKET_CAPACITY = _env_float("AI_MIN_BUCKET_CAPACITY", 0.05)

AI_MIN_HIST_EDGE = _env_float("AI_MIN_HIST_EDGE", 0.0)
AI_MIN_BUY_PRESSURE = _env_float("AI_MIN_BUY_PRESSURE", 0.0)
AI_MIN_SMART_ALIGN = _env_float("AI_MIN_SMART_ALIGN", 0.0)

AI_MIN_MCAP_USD = _env_float("AI_MIN_MCAP_USD", 0.0)
AI_MAX_MCAP_USD = _env_float("AI_MAX_MCAP_USD", 0.0)

# Session/risk tightening
AI_REGIME_TIGHTEN_ENABLED = _env_bool("AI_REGIME_TIGHTEN_ENABLED", True)
AI_RISK_OFF_EXTRA_MARGIN = _env_float("AI_RISK_OFF_EXTRA_MARGIN", 0.08)
AI_MANIP_HIGH = _env_float("AI_MANIP_HIGH", 0.75)

AI_CTX_MAX_DRAWDOWN_PCT = abs(_env_float("AI_CTX_MAX_DRAWDOWN_PCT", 15.0))
AI_CTX_MAX_OPEN_POS_RATIO = _env_float("AI_CTX_MAX_OPEN_POS_RATIO", 1.0)
AI_CTX_LOCK_AFTER_TARGET_PNL = _env_float("AI_CTX_LOCK_AFTER_TARGET_PNL", 0.0)

# Regime gate factor usage (from ai_risk_model)
AI_USE_REGIME_GATE_FACTOR = _env_bool("AI_USE_REGIME_GATE_FACTOR", True)
AI_REGIME_GATE_FACTOR_STRENGTH = _env_float("AI_REGIME_GATE_FACTOR_STRENGTH", 0.30)

# Self-learning overlays
AI_BUY_SELF_LEARN_ENABLED = _env_bool("AI_BUY_SELF_LEARN_ENABLED", True)
AI_BUY_SELF_LEARN_MAX_ABS_BOOST = _env_float("AI_BUY_SELF_LEARN_MAX_ABS_BOOST", 0.12)
AI_BUY_SELF_LEARN_MAX_ABS_GATE = _env_float("AI_BUY_SELF_LEARN_MAX_ABS_GATE", 0.10)
AI_BUY_SELF_LEARN_GATE_WEIGHT = _env_float("AI_BUY_SELF_LEARN_GATE_WEIGHT", 1.0)

# Pump infra
AI_INFRA_NO_ACCOUNT_SOFT_LIQ = _env_bool("AI_INFRA_NO_ACCOUNT_SOFT_LIQ", True)
AI_INFRA_NO_ACCOUNT_PENALTY = _env_float("AI_INFRA_NO_ACCOUNT_PENALTY", -0.03)

# Soft gate
AI_SCORE_GATE = _env_float("AI_SCORE_GATE", 0.58)
AI_SCORE_GATE_OVERRIDES = _env_json("AI_SCORE_GATE_OVERRIDES_JSON") or {}
AI_SCORE_SOURCE_BONUS = _env_json("AI_SCORE_SOURCE_BONUS_JSON") or {}

# Reason formatting
AI_REASON_INCLUDE_DETAILS = _env_bool("AI_REASON_INCLUDE_DETAILS", False)

# Weights
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
W_SMART_ALIGN = _env_float("AI_W_SMART_WHALE_ALIGN", 0.08)
W_MCAP = _env_float("AI_W_MCAP", 0.06)
W_AI_ENTRY = _env_float("AI_W_AI_ENTRY", 0.08)
W_TIER = _env_float("AI_W_TIER", 0.05)
W_BUY_PRESS = _env_float("AI_W_BUY_PRESS", 0.06)
W_HIST_EDGE = _env_float("AI_W_HIST_EDGE", 0.05)

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
    + W_SMART_ALIGN
    + W_MCAP
    + W_AI_ENTRY
    + W_TIER
    + W_BUY_PRESS
    + W_HIST_EDGE
)
AI_WEIGHT_NORM = _env_float("AI_WEIGHT_NORM", max(_WEIGHT_SUM, 1.0))

# Bonuses/penalties
BONUS_VERIFIED = _env_float("AI_BONUS_VERIFIED", 0.03)
PENALTY_UNVERIFIED = _env_float("AI_PENALTY_UNVERIFIED", -0.05)
BONUS_LIQ_LOCK = _env_float("AI_BONUS_LIQ_LOCK", 0.03)
BONUS_RENOUNCED = _env_float("AI_BONUS_RENOUNCED", 0.02)
BONUS_NO_FREEZE = _env_float("AI_BONUS_NO_FREEZE", 0.02)

PENALTY_RUG_REPORT = _env_float("AI_PENALTY_RUG_REPORT", -0.02)
RUG_REPORT_CAP = _env_int("AI_RUG_REPORT_CAP", 5)

WHALE_TOUCH_BONUS = _env_float("AI_WHALE_TOUCH_BONUS", 0.03)
WHALE_TOUCH_BONUS_CAP = _env_float("AI_WHALE_TOUCH_BONUS_CAP", 0.15)

# Anchors
ANCHOR_POOL_AGE = _env_float("AI_ANCHOR_POOL_AGE", 120.0)
ANCHOR_LIQ = _env_float("AI_ANCHOR_LIQ", 100_000.0)
ANCHOR_IMPACT_BPS = _env_float("AI_ANCHOR_IMPACT_BPS", 150.0)
ANCHOR_VOL5M = _env_float("AI_ANCHOR_VOL5M", 5.0)

ANCHOR_TOP10_GOOD = _env_float("AI_ANCHOR_TOP10_GOOD", 20.0)
ANCHOR_TOP10_BAD = _env_float("AI_ANCHOR_TOP10_BAD", 90.0)

ANCHOR_FLOW_IMB = _env_float("AI_ANCHOR_FLOW_IMB", 0.65)
ANCHOR_BOOK_IMB = _env_float("AI_ANCHOR_BOOK_IMB", 0.60)
ANCHOR_MCAP = _env_float("AI_ANCHOR_MCAP", 250_000.0)

RET5M_POS_CEIL = _env_float("AI_RET5M_POS_CEIL", 9.0)
RET5M_NEG_FLOOR = _env_float("AI_RET5M_NEG_FLOOR", -9.0)
RET15M_POS_CEIL = _env_float("AI_RET15M_POS_CEIL", 18.0)
RET15M_NEG_FLOOR = _env_float("AI_RET15M_NEG_FLOOR", -18.0)

# Size logic
AI_SIZE_SCALAR_EXP = _env_float("AI_SIZE_SCALAR_EXP", 1.0)
AI_SIZE_HIST_EDGE_STRENGTH = _env_float("AI_SIZE_HIST_EDGE_STRENGTH", 0.35)
AI_SIZE_MIN_IF_OK = _env_float("AI_SIZE_MIN_IF_OK", 0.0)

AI_BUY_DEBUG_LOG = _env_bool("AI_BUY_DEBUG_LOG", False)
AI_BUY_ATTACH_PARTS = _env_bool("AI_BUY_ATTACH_PARTS", False)

# Extra explain payload written into features (optional)
AI_BUY_ATTACH_EXPLAIN = _env_bool("AI_BUY_ATTACH_EXPLAIN", False)
AI_BUY_EXPLAIN_TOPK = _env_int("AI_BUY_EXPLAIN_TOPK", 4)

# Test-mode bypass (soft gate only)
AI_PRE_BYPASS_FOR_TEST = _env_bool("AI_PRE_BYPASS_FOR_TEST", False)


# =============================================================================
# Reason codes (stable)
# =============================================================================

HARD_REASON_CODES = {
    "POOL_TOO_NEW",
    "LOW_LIQ",
    "HIGH_IMPACT",
    "TOP10_TOO_HIGH",
    "TOO_QUIET",
    "HONEYPOT_FLAG",
    "DEV_WALLET_TOO_BIG",
    "FEW_WHALE_TOUCHES",
    "LIQ_NOT_LOCKED",
    "MINT_NOT_RENOUNCED",
    "FREEZE_AUTH_ENABLED",
    "FRAUD_TOO_HIGH",
    "PF_FINAL_TOO_LOW",
    "PF_RISK_ADJ_TOO_LOW",
    "AI_ENTRY_PROB_TOO_LOW",
    "ENTRY_BLOCK_FLAG",
    "DEAD_LIKE",
    "ENTRY_TIER_TOO_LOW",
    "STRUCTURAL_QUALITY_TOO_LOW",
    "ENTRY_RISK_TOO_HIGH",
    "MCAP_TOO_SMALL",
    "MCAP_TOO_LARGE",
    "SESSION_DRAWDOWN_MAX",
    "SESSION_TARGET_HIT",
    "TOO_MANY_POSITIONS",
    "NO_RISK_CAPACITY",
    "RISK_BUCKET_FULL",
    "RISK_BUCKET_NO_CAPACITY",
    "GLOBAL_RISK_BLOCK",
    "BUY_PRESSURE_TOO_LOW",
    "SMART_ALIGN_TOO_LOW",
    "HIST_EDGE_TOO_LOW",
    "SELF_LEARNING_BLOCK",
}

SOFT_REASON_CODES = {
    "PASS",
    "PASS_TEST_BYPASS",
    "BELOW_GATE",
    "HARD_REJECT",
    "RISK_MODE_OFF",
}


def _source_gate_threshold(source: str | None) -> float:
    sk = _normalize_source_key(source)
    if not sk:
        return AI_SCORE_GATE
    if sk in AI_SCORE_GATE_OVERRIDES:
        try:
            return float(AI_SCORE_GATE_OVERRIDES[sk])
        except Exception:
            return AI_SCORE_GATE
    return AI_SCORE_GATE


def _source_score_bonus(source: str | None) -> float:
    sk = _normalize_source_key(source)
    if not sk:
        return 0.0
    v = AI_SCORE_SOURCE_BONUS.get(sk)
    try:
        return float(v) if v is not None else 0.0
    except Exception:
        return 0.0


def _fmt_reason(code: str, detail: str | None) -> str:
    # Keep stable codes by default; optionally include details for debugging.
    if not AI_REASON_INCLUDE_DETAILS or not detail:
        return code
    return f"{code}({detail})"


# =============================================================================
# Hard filters (stable codes + optional details)
# =============================================================================


def _hard_filters(c: Candidate) -> tuple[bool, list[str], dict[str, str]]:
    reasons: list[str] = []
    details: dict[str, str] = {}

    def add(code: str, detail: str | None = None) -> None:
        reasons.append(_fmt_reason(code, detail))
        if detail:
            details[code] = detail

    # Pool age
    if c.pool_age_min < AI_MIN_POOL_AGE_MIN:
        add("POOL_TOO_NEW", f"{c.pool_age_min:.1f}m<{AI_MIN_POOL_AGE_MIN:.1f}m")

    # Liquidity (pump infra softening)
    if c.liq_usd < AI_MIN_LIQ_USD:
        if AI_INFRA_NO_ACCOUNT_SOFT_LIQ and bool(c.infra_no_account) and _is_pump_source(c.source):
            # do not hard reject; we'll penalize softly (and keep a detail marker)
            details["LOW_LIQ_SOFT"] = f"${c.liq_usd:,.0f}<${AI_MIN_LIQ_USD:,.0f}"
        else:
            add("LOW_LIQ", f"${c.liq_usd:,.0f}<${AI_MIN_LIQ_USD:,.0f}")

    # Impact
    if c.impact_1k_bps > AI_MAX_IMPACT_1K_BPS:
        add("HIGH_IMPACT", f"{c.impact_1k_bps:.0f}bps>{AI_MAX_IMPACT_1K_BPS:.0f}bps")

    # Holder concentration
    if c.top10_pct > AI_MAX_TOP10_PCT:
        add("TOP10_TOO_HIGH", f"{c.top10_pct:.1f}%>{AI_MAX_TOP10_PCT:.1f}%")

    # Alive-ness / movement proxy
    if c.vol5m_pct < AI_MIN_VOL5M_PCT:
        add("TOO_QUIET", f"vol5m={c.vol5m_pct:.2f}%<{AI_MIN_VOL5M_PCT:.2f}%")

    # Honeypot
    if c.honeypot_flag:
        add("HONEYPOT_FLAG")

    # Dev wallet
    if c.dev_wallet_pct is not None and _as_float(c.dev_wallet_pct, 0.0) > AI_MAX_DEV_WALLET_PCT:
        add(
            "DEV_WALLET_TOO_BIG",
            f"{_as_float(c.dev_wallet_pct, 0.0):.1f}%>{AI_MAX_DEV_WALLET_PCT:.1f}%",
        )

    # Whale touches (optional)
    if AI_MIN_WHALE_TOUCHES_1H > 0 and c.whale_touches_1h < AI_MIN_WHALE_TOUCHES_1H:
        add("FEW_WHALE_TOUCHES", f"{c.whale_touches_1h}<{AI_MIN_WHALE_TOUCHES_1H}")

    # Structural requirements
    if AI_REQUIRE_LIQ_LOCK and not bool(c.liq_locked):
        add("LIQ_NOT_LOCKED")
    if AI_REQUIRE_MINT_RENOUNCED and not bool(c.mint_renounced):
        add("MINT_NOT_RENOUNCED")
    if AI_REQUIRE_NO_FREEZE and not bool(c.freeze_auth_disabled):
        add("FREEZE_AUTH_ENABLED")

    # Fraud cutoff
    f01 = _fraud_to_01(c.ai_fraud_score)
    if f01 is not None and f01 >= AI_MAX_RUG_SCORE:
        add("FRAUD_TOO_HIGH", f"{f01:.2f}>={AI_MAX_RUG_SCORE:.2f}")

    # PF floors if present
    if c.pf_final_score is not None and _as_float(c.pf_final_score, 0.0) < AI_MIN_PF_FINAL:
        add("PF_FINAL_TOO_LOW", f"{_as_float(c.pf_final_score, 0.0):.3f}<{AI_MIN_PF_FINAL:.3f}")
    if (
        c.pf_risk_adjusted_final is not None
        and _as_float(c.pf_risk_adjusted_final, 0.0) < AI_MIN_PF_RISK_ADJ
    ):
        add(
            "PF_RISK_ADJ_TOO_LOW",
            f"{_as_float(c.pf_risk_adjusted_final, 0.0):.3f}<{AI_MIN_PF_RISK_ADJ:.3f}",
        )

    # AI entry minimum (optional)
    if (
        AI_MIN_AI_ENTRY_PROB > 0.0
        and c.ai_entry_prob is not None
        and _as_float(c.ai_entry_prob, 0.0) < AI_MIN_AI_ENTRY_PROB
    ):
        add(
            "AI_ENTRY_PROB_TOO_LOW",
            f"{_as_float(c.ai_entry_prob, 0.0):.3f}<{AI_MIN_AI_ENTRY_PROB:.3f}",
        )

    # FeatureBuilder blocks
    if AI_USE_ENTRY_BLOCK_FLAG and _as_float(c.entry_block_flag, 0.0) >= 0.5:
        add("ENTRY_BLOCK_FLAG")
    if _as_float(c.is_dead_like, 0.0) >= 1.0:
        add("DEAD_LIKE")

    # Tier guard
    if (
        AI_MIN_ENTRY_TIER > 0.0
        and c.entry_tier_bucket is not None
        and _as_float(c.entry_tier_bucket, 0.0) < AI_MIN_ENTRY_TIER
    ):
        add(
            "ENTRY_TIER_TOO_LOW",
            f"{_as_float(c.entry_tier_bucket, 0.0):.1f}<{AI_MIN_ENTRY_TIER:.1f}",
        )

    # Structural quality floor
    if (
        AI_MIN_STRUCTURAL_QUALITY > 0.0
        and c.structural_quality_score is not None
        and _as_float(c.structural_quality_score, 0.0) < AI_MIN_STRUCTURAL_QUALITY
    ):
        add(
            "STRUCTURAL_QUALITY_TOO_LOW",
            f"{_as_float(c.structural_quality_score, 0.0):.3f}<{AI_MIN_STRUCTURAL_QUALITY:.3f}",
        )

    # Entry risk cap
    if (
        AI_MAX_ENTRY_RISK_SCORE < 1.0
        and c.entry_risk_score is not None
        and _as_float(c.entry_risk_score, 0.0) > AI_MAX_ENTRY_RISK_SCORE
    ):
        add(
            "ENTRY_RISK_TOO_HIGH",
            f"{_as_float(c.entry_risk_score, 0.0):.3f}>{AI_MAX_ENTRY_RISK_SCORE:.3f}",
        )

    # Market cap band (optional)
    if AI_MIN_MCAP_USD > 0.0 or AI_MAX_MCAP_USD > 0.0:
        mcap = _as_float(c.mcap_usd, 0.0)
        if AI_MIN_MCAP_USD > 0.0 and mcap > 0.0 and mcap < AI_MIN_MCAP_USD:
            add("MCAP_TOO_SMALL", f"${mcap:,.0f}<${AI_MIN_MCAP_USD:,.0f}")
        if AI_MAX_MCAP_USD > 0.0 and mcap > AI_MAX_MCAP_USD:
            add("MCAP_TOO_LARGE", f"${mcap:,.0f}>${AI_MAX_MCAP_USD:,.0f}")

    # Session drawdown
    if (
        AI_CTX_MAX_DRAWDOWN_PCT > 0
        and c.ctx_daily_drawdown_pct is not None
        and _as_float(c.ctx_daily_drawdown_pct, 0.0) <= -AI_CTX_MAX_DRAWDOWN_PCT
    ):
        add(
            "SESSION_DRAWDOWN_MAX",
            f"{_as_float(c.ctx_daily_drawdown_pct, 0.0):.2f}%<=-{AI_CTX_MAX_DRAWDOWN_PCT:.1f}%",
        )

    # Lock after target pnl
    if (
        AI_CTX_LOCK_AFTER_TARGET_PNL > 0
        and c.ctx_daily_pnl_pct is not None
        and c.ctx_risk_mode is not None
    ) and (
        _as_float(c.ctx_daily_pnl_pct, 0.0) >= AI_CTX_LOCK_AFTER_TARGET_PNL
        and _as_float(c.ctx_risk_mode, 1.0) <= 1.0
    ):
        add(
            "SESSION_TARGET_HIT",
            f"{_as_float(c.ctx_daily_pnl_pct, 0.0):.2f}%>={AI_CTX_LOCK_AFTER_TARGET_PNL:.2f}%",
        )

    # Open positions ratio
    if (
        AI_CTX_MAX_OPEN_POS_RATIO > 0
        and c.ctx_open_positions is not None
        and c.ctx_max_positions is not None
        and _as_float(c.ctx_max_positions, 0.0) > 0
    ):
        ratio = _safe_div(
            _as_float(c.ctx_open_positions, 0.0), _as_float(c.ctx_max_positions, 0.0), 0.0
        )
        if ratio >= AI_CTX_MAX_OPEN_POS_RATIO:
            add(
                "TOO_MANY_POSITIONS",
                f"{int(_as_float(c.ctx_open_positions, 0.0))}/{int(_as_float(c.ctx_max_positions, 0.0))} ratio={ratio:.2f}",
            )

    # Risk capacity
    if (
        AI_USE_RISK_CAPACITY_GUARD
        and c.risk_capacity_score is not None
        and _as_float(c.risk_capacity_score, 1.0) < AI_MIN_RISK_CAPACITY
    ):
        add(
            "NO_RISK_CAPACITY",
            f"{_as_float(c.risk_capacity_score, 1.0):.3f}<{AI_MIN_RISK_CAPACITY:.3f}",
        )

    # Bucket capacity
    if AI_USE_BUCKET_CAPACITY_GUARD:
        util = _as_float(c.risk_bucket_utilization, 0.0)
        cap = _as_float(c.risk_bucket_capacity_score, 1.0)
        if util >= 1.0:
            add("RISK_BUCKET_FULL")
        elif cap < AI_MIN_BUCKET_CAPACITY:
            add("RISK_BUCKET_NO_CAPACITY", f"{cap:.3f}<{AI_MIN_BUCKET_CAPACITY:.3f}")

    # Global kill-switch
    if _as_float(c.ctx_global_block_flag, 0.0) >= 0.5:
        add("GLOBAL_RISK_BLOCK")

    # Buy-pressure floor
    if AI_MIN_BUY_PRESSURE > 0.0 and c.buy_pressure_score is not None:
        bp = _clamp(_as_float(c.buy_pressure_score, 0.0) / 1.5, 0.0, 1.0)
        if bp < AI_MIN_BUY_PRESSURE:
            add("BUY_PRESSURE_TOO_LOW", f"{bp:.3f}<{AI_MIN_BUY_PRESSURE:.3f}")

    # Smart-align floor
    if AI_MIN_SMART_ALIGN > 0.0:
        swa = _clamp(_smart_whale_align_component(c) / 1.5, 0.0, 1.0)
        if swa < AI_MIN_SMART_ALIGN:
            add("SMART_ALIGN_TOO_LOW", f"{swa:.3f}<{AI_MIN_SMART_ALIGN:.3f}")

    # Hist-edge floor
    if AI_MIN_HIST_EDGE > 0.0:
        he = _historical_edge_component(c)
        if he < AI_MIN_HIST_EDGE:
            add("HIST_EDGE_TOO_LOW", f"{he:.3f}<{AI_MIN_HIST_EDGE:.3f}")

    # Self-learning explicit block
    if AI_BUY_SELF_LEARN_ENABLED and _as_float(c.self_learn_entry_block, 0.0) >= 0.5:
        add("SELF_LEARNING_BLOCK")

    return (len(reasons) == 0), reasons, details


# =============================================================================
# Scoring components (0..1 unless stated)
# =============================================================================


def _score_pool_age(mins: float) -> float:
    return _clamp(_as_float(mins, 0.0) / max(ANCHOR_POOL_AGE, 1e-9))


def _score_liq(usd: float) -> float:
    return _clamp(_as_float(usd, 0.0) / max(ANCHOR_LIQ, 1e-9))


def _score_impact(bps: float) -> float:
    return _clamp(1.0 - _clamp(_as_float(bps, ANCHOR_IMPACT_BPS) / max(ANCHOR_IMPACT_BPS, 1e-9)))


def _score_vol5m(vol_pct: float) -> float:
    return _clamp(abs(_as_float(vol_pct, 0.0)) / max(ANCHOR_VOL5M, 1e-9))


def _score_top10(conc_pct: float) -> float:
    x = _as_float(conc_pct, 100.0)
    if x <= ANCHOR_TOP10_GOOD:
        return 1.0
    if x >= ANCHOR_TOP10_BAD:
        return 0.0
    return _clamp(1.0 - (x - ANCHOR_TOP10_GOOD) / max(ANCHOR_TOP10_BAD - ANCHOR_TOP10_GOOD, 1e-9))


def _score_whales(touches: int, whale_share: float | None) -> float:
    base = _clamp(_as_float(touches, 0.0) / 5.0)  # saturate ~5
    share = _clamp(_as_float(whale_share, 0.3), 0.0, 1.0)
    if share > 0.75:
        base -= (share - 0.75) * 1.0
    else:
        base += share * 0.3
    return _clamp(base)


def _score_mcap(mcap_usd: float | None) -> float:
    m = _as_float(mcap_usd, 0.0)
    if m <= 0.0:
        return 0.3
    x = m / max(ANCHOR_MCAP, 1e-9)
    z = math.log(max(x, 1e-9))
    s = math.exp(-0.5 * (z / 1.2) ** 2)  # log-normal-ish preference
    return _clamp(s)


def _tier_component(entry_tier: float | None, size_tier: float | None) -> float:
    e = _clamp(_as_float(entry_tier, 0.0) / 3.0, 0.0, 1.0)
    s = _clamp(_as_float(size_tier, 0.0) / 3.0, 0.0, 1.0)
    align = 0.5 * (e + s)
    if e > 0.0 and s > 0.0:
        align += 0.15 * min(e, s)
    return _clamp(align)


def _trend_component(ret5m: float | None, ret15m: float | None) -> float:
    r5 = _as_float(ret5m, 0.0)
    r15 = _as_float(ret15m, 0.0)

    # avoid chasing nukes/pumps
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
    buys: int | None,
    sells: int | None,
    buy_usd: float | None,
    sell_usd: float | None,
    vol_5m_usd: float | None,
    flow_accum: float | None,
    flow_panic: float | None,
) -> float:
    b = _as_float(buys, 0.0)
    s = _as_float(sells, 0.0)
    bu = _as_float(buy_usd, 0.0)
    su = _as_float(sell_usd, 0.0)
    v5 = _as_float(vol_5m_usd, 0.0)

    cnt_total = b + s
    not_total = bu + su

    cnt_imb = _safe_div(b, cnt_total, 0.5)
    not_imb = _safe_div(bu, not_total, 0.5)
    share = 0.5 * cnt_imb + 0.5 * not_imb

    vol_energy = _clamp(v5 / 8_000.0)  # saturate ~$8k/5m
    diff = abs(share - ANCHOR_FLOW_IMB)
    base = _clamp(1.0 - diff / max(ANCHOR_FLOW_IMB, 1.0 - ANCHOR_FLOW_IMB))

    fa = _clamp(_as_float(flow_accum, 0.0))
    fp = _clamp(_as_float(flow_panic, 0.0))
    base = _clamp(base + 0.15 * fa - 0.20 * fp)

    return _clamp(0.7 * base + 0.3 * vol_energy)


def _book_component(bid_usd: float | None, ask_usd: float | None) -> float:
    bid = _as_float(bid_usd, 0.0)
    ask = _as_float(ask_usd, 0.0)
    tot = bid + ask
    if tot <= 0.0:
        return 0.5
    bid_share = bid / tot
    diff = abs(bid_share - ANCHOR_BOOK_IMB)
    return _clamp(1.0 - diff / max(ANCHOR_BOOK_IMB, 1.0 - ANCHOR_BOOK_IMB))


def _safety_component(
    verified: bool | None,
    liq_locked: bool | None,
    mint_renounced: bool | None,
    freeze_disabled: bool | None,
    rug_reports: int | None,
) -> tuple[float, float]:
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


def _pf_component(pf_mom: float | None, pf_final: float | None, pf_risk_adj: float | None) -> float:
    m = _clamp(_as_float(pf_mom, 0.0))
    f = _clamp(_as_float(pf_final, 0.0))
    r = _clamp(_as_float(pf_risk_adj, f))
    if f == 0.0 and r == 0.0 and m == 0.0:
        return 0.0
    return _clamp(1.2 * r + 0.6 * f + 0.4 * m)


def _fraud_component(ai_fraud_score: float | None) -> float:
    f01 = _fraud_to_01(ai_fraud_score)
    if f01 is None:
        return 0.5
    return _clamp(1.0 - f01)


def _historical_edge_component(c: Candidate) -> float:
    if c.hist_edge_score is not None:
        h = _as_float(c.hist_edge_score, 0.5)
    else:
        win = _as_float(c.hist_win_rate, 0.5)
        if win > 1.5:
            win = win / 100.0
        win = _clamp(win, 0.0, 1.0)

        rr = _as_float(c.hist_rr_ratio, 1.0)
        rr_norm = _clamp((rr - 0.8) / 2.0, 0.0, 1.0)
        h = 0.65 * win + 0.35 * rr_norm

    expl = _clamp(_as_float(c.exploration_score, 0.5), 0.0, 1.0)
    return _clamp(0.9 * _clamp(h, 0.0, 1.0) + 0.1 * expl, 0.0, 1.0)


def _regime_ctx_component(c: Candidate) -> float:
    trend = _clamp(
        _as_float(_pick(vars(c), "regime_score", "regime_trend", default=0.0), 0.0), -1.0, 1.0
    )
    liq = _clamp(_as_float(c.regime_liquidity, 0.5))
    manip = _clamp(_as_float(c.regime_manipulation_risk, 0.0))
    vol = _clamp(_as_float(c.regime_volatility, 0.5))

    ctx_dd = _as_float(c.ctx_daily_drawdown_pct, 0.0)
    ctx_pnl = _as_float(c.ctx_daily_pnl_pct, 0.0)
    ctx_risk = _as_float(c.ctx_risk_mode, 1.0)

    exposure_frac = _clamp(_as_float(c.ctx_exposure_frac, 0.0), 0.0, 1.0)
    win_rate = _clamp(_as_float(c.ctx_win_rate_pct, 50.0) / 100.0, 0.0, 1.0)
    loss_streak = _as_float(c.ctx_recent_loss_streak, 0.0)

    rp_cons = _clamp(_as_float(c.risk_profile_conservative, 0.0), 0.0, 1.0)
    rp_bal = _clamp(_as_float(c.risk_profile_balanced, 0.0), 0.0, 1.0)
    rp_agg = _clamp(_as_float(c.risk_profile_aggressive, 0.0), 0.0, 1.0)

    cap_delta = _as_float(c.ai_capacity_delta, 0.0)
    reg_gate = _clamp(_as_float(c.regime_entry_gate_factor, 1.0), 0.2, 1.6)

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
        return _clamp(_as_float(c.entry_quality_score, 0.0), 0.0, 1.5)
    structural = _clamp(_as_float(c.structural_quality_score, 0.0), 0.0, 1.5)
    pf_risk_adj = _clamp(
        _as_float(c.pf_risk_adjusted_final, _as_float(c.pf_final_score, 0.0)), 0.0, 1.0
    )
    entry_micro = _clamp(_as_float(c.entry_micro_alpha, 0.0), 0.0, 1.5)
    return _clamp(0.45 * structural + 0.30 * pf_risk_adj + 0.25 * entry_micro, 0.0, 1.5)


def _smart_whale_align_component(c: Candidate) -> float:
    if c.smart_whale_alignment is not None:
        base = _clamp(_as_float(c.smart_whale_alignment, 0.0), 0.0, 1.5)
    else:
        smart = _clamp(_as_float(c.smart_wallet_score, 0.5), 0.0, 1.0)
        flow_raw = _clamp(_as_float(c.smart_flow_raw, 0.0), -1.0, 1.0)
        smart_bias = 0.5 * (flow_raw + 1.0)

        whales = _clamp(
            _as_float(_pick(vars(c), "whales_5m", "whale_touches_1h", default=0.0), 0.0) / 5.0,
            0.0,
            1.0,
        )
        whale_share = _clamp(
            _as_float(_pick(vars(c), "whale_share_5m", "whale_share_1h", default=0.3), 0.3),
            0.0,
            1.0,
        )
        buyers = _clamp(
            _as_float(_pick(vars(c), "unique_buyers_5m", "unique_buyers_1h", default=0.0), 0.0)
            / 50.0,
            0.0,
            1.0,
        )

        if whale_share > 0.8:
            whale_term = max(0.0, whales * (1.1 - (whale_share - 0.8) * 2.0))
        else:
            whale_term = whales * (0.7 + 0.3 * whale_share)

        base = 0.40 * smart + 0.25 * smart_bias + 0.20 * whale_term + 0.15 * buyers

    base = _clamp(base, 0.0, 1.5)
    wlf = _clamp(_as_float(c.whale_liquidity_footprint, 0.0), 0.0, 1.0)
    if wlf > 0.0:
        base *= 0.9 + 0.2 * wlf
    return _clamp(base, 0.0, 1.5)


def _ai_entry_component(prob: float | None, score: float | None) -> float:
    if prob is not None:
        return _clamp(_as_float(prob, 0.0), 0.0, 1.0)
    if score is not None:
        return _clamp(_as_float(score, 0.0), 0.0, 1.0)
    return 0.0


def _buy_pressure_component(bp: float | None) -> float:
    return _clamp(_as_float(bp, 0.0) / 1.5, 0.0, 1.0)


# =============================================================================
# Size scalar (0..1)
# =============================================================================


def _size_scalar_from_candidate(c: Candidate, entry_quality: float) -> float:
    if c.position_size_scalar is not None:
        base = _clamp(_as_float(c.position_size_scalar, 0.0), 0.0, 1.0)
    else:
        base = _clamp(entry_quality / 1.5, 0.0, 1.0)

    if c.position_size_bucket is not None:
        tier_norm = _clamp(_as_float(c.position_size_bucket, 0.0) / 3.0, 0.0, 1.0)
        base = _clamp(0.8 * base + 0.2 * tier_norm, 0.0, 1.0)

    cap = _clamp(_as_float(c.risk_capacity_score, 1.0), 0.0, 1.0)
    size = base * cap

    cap_delta = _as_float(c.ai_capacity_delta, 0.0)
    if cap_delta < 0:
        size *= 1.0 + max(cap_delta / 0.6, -1.0)
    elif cap_delta > 0:
        size *= 1.0 + min(cap_delta / 0.6, 0.25)

    util = _clamp(_as_float(c.risk_bucket_utilization, 0.0), 0.0, 1.5)
    bucket_cap = _clamp(_as_float(c.risk_bucket_capacity_score, 1.0), 0.0, 1.0)
    if util > 0.7:
        shrink = 1.0 - 0.6 * min((util - 0.7) / 0.3, 1.0)
        size *= max(0.4, shrink)
    size *= 0.7 + 0.3 * bucket_cap

    reg = _clamp(
        _as_float(_pick(vars(c), "regime_score", "regime_trend", default=0.0), 0.0), -1.0, 1.0
    )
    if reg <= -0.7:
        size *= 0.4
    elif reg <= -0.3:
        size *= 0.6
    elif reg >= 0.7:
        size *= 1.15
    elif reg >= 0.3:
        size *= 1.05

    mode = _as_float(c.ctx_risk_mode, 1.0)
    if mode <= 0.0:
        size *= 0.0
    elif mode == 3.0:
        size *= 0.6
    elif mode >= 2.0:
        size *= 1.1

    exposure = _clamp(_as_float(c.ctx_exposure_frac, 0.0), 0.0, 1.0)
    if exposure > 0.7:
        shrink = 1.0 - 0.6 * ((exposure - 0.7) / 0.3)
        size *= max(0.4, shrink)

    loss_streak = _as_float(c.ctx_recent_loss_streak, 0.0)
    if loss_streak >= 3:
        size *= 0.5
    elif loss_streak == 2:
        size *= 0.7

    if _as_float(c.is_dead_like, 0.0) >= 1.0:
        size *= 0.0

    # Fraud "rugginess" shrink
    rugginess = 1.0 - _fraud_component(c.ai_fraud_score)
    size *= 1.0 - 0.6 * rugginess

    if AI_SIZE_HIST_EDGE_STRENGTH > 0.0:
        he = _historical_edge_component(c)
        factor = _clamp(1.0 + AI_SIZE_HIST_EDGE_STRENGTH * (he - 0.5), 0.5, 1.5)
        size *= factor

    size = _clamp(size, 0.0, 1.0)
    if AI_SIZE_MIN_IF_OK > 0.0 and size > 0.0:
        size = max(size, AI_SIZE_MIN_IF_OK)
    if AI_SIZE_SCALAR_EXP != 1.0 and size > 0.0:
        size = _clamp(size**AI_SIZE_SCALAR_EXP, 0.0, 1.0)
    return size


# =============================================================================
# Self-learning bounded overlays
# =============================================================================


def _apply_self_learning_overrides(
    score: float, threshold: float, c: Candidate
) -> tuple[float, float, str]:
    if not AI_BUY_SELF_LEARN_ENABLED:
        return score, threshold, ""

    meta = c.meta or {}
    notes: list[str] = []

    raw_boost = 0.0
    if c.self_learn_entry_boost is not None:
        raw_boost += _as_float(c.self_learn_entry_boost, 0.0)
    raw_boost += _as_float(meta.get("sl_entry_boost", 0.0), 0.0)

    if raw_boost != 0.0:
        mb = abs(_as_float(AI_BUY_SELF_LEARN_MAX_ABS_BOOST, 0.12))
        raw_boost = max(-mb, min(mb, raw_boost))
        score = _clamp(score + raw_boost, 0.0, 1.0)
        notes.append(f"boost={raw_boost:+.3f}")

    raw_gate = 0.0
    if c.self_learn_entry_gate_offset is not None:
        raw_gate += _as_float(c.self_learn_entry_gate_offset, 0.0)
    raw_gate += _as_float(meta.get("sl_entry_gate_offset", 0.0), 0.0)

    if raw_gate != 0.0:
        mg = abs(_as_float(AI_BUY_SELF_LEARN_MAX_ABS_GATE, 0.10))
        raw_gate = max(-mg, min(mg, raw_gate))
        raw_gate *= _as_float(AI_BUY_SELF_LEARN_GATE_WEIGHT, 1.0)
        threshold = _clamp(threshold + raw_gate, 0.0, 0.99)
        notes.append(f"gate={raw_gate:+.3f}")

    return score, threshold, ",".join(notes)


# =============================================================================
# Core scoring (single-pass)
# =============================================================================


def _compute_score_detail(c: Candidate) -> dict[str, Any]:
    hard_ok, hard_reasons, hard_details = _hard_filters(c)

    parts: dict[str, float] = {}
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
        + W_SMART_ALIGN * parts["smart_whale_align"]
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

    base_threshold = float(_source_gate_threshold(c.source))
    size_scalar = _size_scalar_from_candidate(c, entry_quality)

    out: dict[str, Any] = {
        "ok_hard": hard_ok,
        "hard_reasons": hard_reasons,
        "hard_details": hard_details,
        "parts": parts,
        "whale_bonus": whale_bonus,
        "safety_bonus": float(safety_bonus),
        "infra_penalty": infra_penalty,
        "raw_score": raw_score,
        "score": float(score),
        "entry_quality": float(entry_quality),
        "size_scalar": float(size_scalar),
        "entry_block_flag": _as_float(c.entry_block_flag, 0.0),
        "risk_capacity": _clamp(_as_float(c.risk_capacity_score, 1.0), 0.0, 1.0),
        "risk_bucket_capacity": _clamp(_as_float(c.risk_bucket_capacity_score, 1.0), 0.0, 1.0),
        "risk_bucket_utilization": _clamp(_as_float(c.risk_bucket_utilization, 0.0), 0.0, 2.0),
        "ctx_global_block_flag": _as_float(c.ctx_global_block_flag, 0.0),
        "base_gate_threshold": float(base_threshold),
        "gate_threshold": float(base_threshold),  # may be updated in gate phase
        "entry_tier": _as_float(c.entry_tier_bucket, 0.0),
        "position_size_tier": _as_float(c.position_size_bucket, 0.0),
        "source": _normalize_source_key(c.source),
        "infra_no_account": bool(c.infra_no_account),
    }
    return out


def _apply_dynamic_threshold(
    c: Candidate, score: float, base_threshold: float
) -> tuple[float, list[str]]:
    """
    Returns (threshold, notes). Notes are short machine-friendly tags.
    """
    thr = float(base_threshold)
    notes: list[str] = []

    # Regime tightening (additive)
    if AI_REGIME_TIGHTEN_ENABLED:
        trend = _clamp(
            _as_float(_pick(vars(c), "regime_score", "regime_trend", default=0.0), 0.0), -1.0, 1.0
        )
        manip = _clamp(_as_float(c.regime_manipulation_risk, 0.0), 0.0, 1.0)
        if trend <= -0.7:
            thr = max(thr + AI_RISK_OFF_EXTRA_MARGIN, 0.80)
            notes.append("REGIME_RISK_OFF")
        if manip >= AI_MANIP_HIGH:
            thr = max(thr + 0.08, thr)
            notes.append("MANIP_HIGH")

    # ai_risk_model gate factor (multiplicative; bounded)
    if AI_USE_REGIME_GATE_FACTOR and c.regime_entry_gate_factor is not None:
        rg = _clamp(_as_float(c.regime_entry_gate_factor, 1.0), 0.2, 1.6)
        delta = (1.0 - rg) * AI_REGIME_GATE_FACTOR_STRENGTH
        thr = _clamp(thr * (1.0 + delta), 0.0, 0.99)
        notes.append(f"RGF={rg:.2f}")

    # If risk_mode is explicitly off, we don't want accidental trading.
    mode = _as_float(c.ctx_risk_mode, 1.0)
    if c.ctx_risk_mode is not None and mode <= 0.0:
        thr = 0.99
        notes.append("RISK_MODE_OFF")

    # Self-learning bounded overlays
    score2, thr2, sl_note = _apply_self_learning_overrides(float(score), float(thr), c)
    if sl_note:
        notes.append(f"SL:{sl_note}")
    return float(thr2), notes


def _soft_explain(parts: dict[str, float], topk: int) -> dict[str, Any]:
    """
    Explain score drivers without leaking weights into reason strings.
    Returns top positive + top negative parts (by distance from 0.5).
    """
    items = [(k, float(v)) for k, v in (parts or {}).items() if isinstance(v, int | float)]
    if not items:
        return {"top_pos": [], "top_neg": []}

    # positive: high values, negative: low values
    top_pos = sorted(items, key=lambda kv: kv[1], reverse=True)[: max(1, topk)]
    top_neg = sorted(items, key=lambda kv: kv[1])[: max(1, topk)]
    return {
        "top_pos": [{"k": k, "v": round(v, 4)} for k, v in top_pos],
        "top_neg": [{"k": k, "v": round(v, 4)} for k, v in top_neg],
    }


# =============================================================================
# Public API
# =============================================================================


def score_only(c: Candidate) -> dict[str, Any]:
    """
    Full scoring breakdown (hard filters evaluated but not final-gated).
    Returns dict suitable for logging and for gate_feature_vector().
    """
    detail = _compute_score_detail(c)

    if AI_BUY_ATTACH_PARTS:
        try:
            parts = detail.get("parts") or {}
            detail["parts_json"] = _json_compact({k: round(float(v), 4) for k, v in parts.items()})
        except Exception:
            pass

    if AI_BUY_DEBUG_LOG:
        try:
            parts = detail.get("parts") or {}
            log.debug(
                "BUY_SCORING sym=%s mint=%s src=%s score=%.3f size=%.3f thr=%.3f hard_ok=%s reasons=%s parts=%s",
                c.symbol,
                c.mint,
                _normalize_source_key(c.source),
                float(detail["score"]),
                float(detail["size_scalar"]),
                float(detail["gate_threshold"]),
                bool(detail["ok_hard"]),
                ";".join(detail.get("hard_reasons") or []),
                {k: round(float(v), 3) for k, v in parts.items()},
            )
        except Exception:
            pass

    return detail


def explain(c: Candidate) -> dict[str, Any]:
    """
    Like score_only(), but also computes the final dynamic threshold and provides
    concise soft explanations (top components).
    """
    detail = _compute_score_detail(c)

    score = float(detail["score"])
    base_thr = float(detail["base_gate_threshold"])
    final_thr, notes = _apply_dynamic_threshold(c, score, base_thr)

    detail["gate_threshold"] = float(final_thr)
    detail["gate_notes"] = list(notes)

    # soft explain: top drivers (not weights, just component values)
    try:
        parts = detail.get("parts") or {}
        detail["soft_explain"] = _soft_explain(parts, topk=max(1, AI_BUY_EXPLAIN_TOPK))
    except Exception:
        detail["soft_explain"] = {"top_pos": [], "top_neg": []}

    # add a compact machine-friendly delta field
    detail["gate_margin"] = round(score - float(final_thr), 6)

    if AI_BUY_ATTACH_PARTS and "parts_json" not in detail:
        try:
            parts = detail.get("parts") or {}
            detail["parts_json"] = _json_compact({k: round(float(v), 4) for k, v in parts.items()})
        except Exception:
            pass

    return detail


def pass_gate(c: Candidate) -> tuple[bool, float, str]:
    """
    Returns (ok, score, reason_str).

    ok=True when:
      - hard filters pass AND score >= dynamic threshold, OR
      - AI_PRE_BYPASS_FOR_TEST=1 AND hard filters pass

    Writes:
        c.meta["_ai_buy_last_gate_threshold"]
        c.meta["_ai_buy_last_gate_notes"]
    """
    d = _compute_score_detail(c)

    # Hard reject
    if not bool(d["ok_hard"]):
        reason = ";".join(d.get("hard_reasons") or []) or "HARD_REJECT"
        if bool(c.infra_no_account) and _is_pump_source(c.source):
            reason += " [INFRA_NO_ACCOUNT_SOFT_LIQ]"
        return False, float(d["score"]), reason

    score = float(d["score"])
    base_thr = float(d["base_gate_threshold"])

    # TEST MODE: bypass soft gate
    if AI_PRE_BYPASS_FOR_TEST:
        try:
            c.meta["_ai_buy_last_gate_threshold"] = float(base_thr)
            c.meta["_ai_buy_last_gate_notes"] = ["TEST_BYPASS"]
        except Exception:
            pass
        return True, score, f"PASS_TEST_BYPASS score={score:.3f} thr={base_thr:.3f}"

    thr, notes = _apply_dynamic_threshold(c, score, base_thr)

    try:
        c.meta["_ai_buy_last_gate_threshold"] = float(thr)
        c.meta["_ai_buy_last_gate_notes"] = list(notes)
    except Exception:
        pass

    # If risk_mode explicitly off, emit a clear stable reason
    if c.ctx_risk_mode is not None and _as_float(c.ctx_risk_mode, 1.0) <= 0.0:
        msg = f"RISK_MODE_OFF score={score:.3f} thr={thr:.3f}"
        return False, score, msg

    if score >= thr:
        msg = f"PASS score={score:.3f} thr={thr:.3f} size={float(d['size_scalar']):.2f}"
        if bool(c.infra_no_account) and _is_pump_source(c.source):
            msg += " [INFRA_NO_ACCOUNT_SOFT_LIQ]"
        if notes:
            msg += " [" + ",".join(notes) + "]"
        return True, score, msg

    msg = f"BELOW_GATE score={score:.3f}<{thr:.3f}"
    if bool(c.infra_no_account) and _is_pump_source(c.source):
        msg += " [INFRA_NO_ACCOUNT_SOFT_LIQ]"
    if notes:
        msg += " [" + ",".join(notes) + "]"
    return False, score, msg


# =============================================================================
# Builders: snapshot/feature_vector → Candidate
# =============================================================================


def from_pool_snapshot(d: dict[str, Any]) -> Candidate:
    meta = d.get("meta") or {}
    return Candidate(
        mint=str(_pick(d, "mint", "token", "address", "baseMint", default="UNKNOWN")),
        symbol=str(_pick(d, "symbol", "ticker", "sym", default="UNK")),
        pool_age_min=_as_float(
            _pick(
                d,
                "pool_age_min",
                "token_age_min",
                "age_min",
                "age_minutes",
                "ageMinutes",
                default=0.0,
            ),
            0.0,
        ),
        liq_usd=_as_float(
            _pick(d, "liq_usd", "liquidity_usd", "liquidity", "liq", default=0.0), 0.0
        ),
        impact_1k_bps=_as_float(
            _pick(
                d,
                "impact_1k_bps",
                "impact_1000usd_bps",
                "price_impact_bps",
                "impact_bps",
                default=999.0,
            ),
            999.0,
        ),
        vol5m_pct=_as_float(
            _pick(
                d,
                "vol5m_pct",
                "vol_5m_pct",
                "volatility_5m_pct",
                "chg5",
                "price_change_5m_pct",
                default=0.0,
            ),
            0.0,
        ),
        top10_pct=_as_float(
            _pick(d, "top10_pct", "top_10_pct", "holder_top10_pct", "top10", default=100.0), 100.0
        ),
        honeypot_flag=bool(
            _pick(d, "honeypot_flag", "rug_risk", "hp_flag", "is_honeypot", default=False)
        ),
        whale_touches_1h=_as_int(_pick(d, "whale_touches_1h", "whale_hits_1h", default=0), 0),
        verified=d.get("verified"),
        liq_locked=d.get("liq_locked"),
        mint_renounced=d.get("mint_renounced"),
        freeze_auth_disabled=d.get("freeze_auth_disabled"),
        dev_wallet_pct=d.get("dev_wallet_pct"),
        ret5m_pct=_pick(d, "ret5m_pct", "price_change_5m_pct"),
        ret15m_pct=_pick(d, "ret15m_pct", "price_change_15m_pct"),
        orderflow_buys_1m=d.get("orderflow_buys_1m"),
        orderflow_sells_1m=d.get("orderflow_sells_1m"),
        orderflow_buy_usd_1m=d.get("orderflow_buy_usd_1m"),
        orderflow_sell_usd_1m=d.get("orderflow_sell_usd_1m"),
        orderbook_bid_usd=d.get("orderbook_bid_usd"),
        orderbook_ask_usd=d.get("orderbook_ask_usd"),
        rugpull_reports=d.get("rugpull_reports"),
        mcap_usd=_pick(d, "mcap_usd", "fdv_usd", "fdv"),
        vol_5m_usd=_pick(d, "vol_5m_usd", "vol5", "vol5m_usd"),
        buys_5m=_pick(d, "buys_5m", "buys5"),
        sells_5m=_pick(d, "sells_5m", "sells5"),
        ai_fraud_score=_pick(d, "ai_fraud_score", "ai_rug_score", "ai_rug_score_01"),
        pf_momentum_score=_pick(d, "pf_momentum_score", "momentum_score"),
        pf_final_score=_pick(d, "pf_final_score", "final_score"),
        pf_risk_adjusted_final=d.get("pf_risk_adjusted_final"),
        whale_share_1h=d.get("whale_share_1h"),
        unique_buyers_1h=d.get("unique_buyers_1h"),
        smart_wallet_score=d.get("smart_wallet_score"),
        smart_flow_raw=d.get("smart_flow_raw"),
        smart_whale_alignment=d.get("smart_whale_alignment"),
        unique_buyers_5m=d.get("unique_buyers_5m"),
        whale_share_5m=d.get("whale_share_5m"),
        whales_5m=d.get("whales_5m"),
        buy_pressure_score=_pick(d, "buy_pressure_score", "buy_pressure"),
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
        regime_score=_pick(d, "regime_score", "regime_numeric"),
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
        ctx_global_block_flag=_pick(d, "ctx_global_block_flag", "global_entry_block_flag"),
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
        hist_edge_score=_pick(d, "hist_edge_score", "sl_hist_edge_score"),
        hist_win_rate=_pick(d, "hist_win_rate", "sl_hist_win_rate"),
        hist_rr_ratio=_pick(d, "hist_rr_ratio", "hist_rr"),
        exploration_score=_pick(d, "exploration_score", "explore_score", "sl_exploration_score"),
        source=d.get("source"),
        meta=meta,
        infra_no_account=d.get("infra_no_account"),
    )


def from_feature_vector(feats: dict[str, Any]) -> Candidate:
    meta = feats.get("meta") or {}
    return Candidate(
        mint=str(_pick(feats, "mint", "token", default="UNKNOWN")),
        symbol=str(_pick(feats, "symbol", "sym", default="UNK")),
        pool_age_min=_as_float(
            _pick(feats, "token_age_min", "age_min", "ageMinutes", default=0.0), 0.0
        ),
        liq_usd=_as_float(_pick(feats, "liq_usd", "liquidity_usd", "liq", default=0.0), 0.0),
        impact_1k_bps=_as_float(
            _pick(feats, "impact_1k_bps", "impact_1000usd_bps", default=999.0), 999.0
        ),
        vol5m_pct=_as_float(
            _pick(feats, "vol5m_pct", "vol_5m_pct", "price_change_5m_pct", "chg5", default=0.0), 0.0
        ),
        top10_pct=_as_float(_pick(feats, "top10_pct", default=100.0), 100.0),
        honeypot_flag=bool(_pick(feats, "honeypot_flag", "rug_risk", "is_honeypot", default=False)),
        whale_touches_1h=_as_int(_pick(feats, "whale_touches_1h", "whales_5m", default=0), 0),
        verified=feats.get("verified"),
        liq_locked=feats.get("liq_locked"),
        mint_renounced=feats.get("mint_renounced"),
        freeze_auth_disabled=feats.get("freeze_auth_disabled"),
        dev_wallet_pct=feats.get("dev_wallet_pct"),
        ret5m_pct=feats.get("price_change_5m_pct"),
        ret15m_pct=feats.get("price_change_15m_pct"),
        orderflow_buys_1m=_pick(feats, "orderflow_buys_1m", "buys_1m"),
        orderflow_sells_1m=_pick(feats, "orderflow_sells_1m", "sells_1m"),
        orderflow_buy_usd_1m=_pick(feats, "orderflow_buy_usd_1m", "buy_usd_1m"),
        orderflow_sell_usd_1m=_pick(feats, "orderflow_sell_usd_1m", "sell_usd_1m"),
        orderbook_bid_usd=feats.get("orderbook_bid_usd"),
        orderbook_ask_usd=feats.get("orderbook_ask_usd"),
        rugpull_reports=feats.get("rugpull_reports"),
        mcap_usd=feats.get("mcap_usd"),
        vol_5m_usd=feats.get("vol_5m_usd"),
        buys_5m=feats.get("buys_5m"),
        sells_5m=feats.get("sells_5m"),
        ai_fraud_score=_pick(feats, "ai_fraud_score", "ai_rug_score", "ai_rug_score_01"),
        pf_momentum_score=_pick(feats, "pf_momentum_score", "momentum_score"),
        pf_final_score=_pick(feats, "pf_final_score", "final_score"),
        pf_risk_adjusted_final=feats.get("pf_risk_adjusted_final"),
        whale_share_1h=feats.get("whale_share_1h"),
        unique_buyers_1h=feats.get("unique_buyers_1h"),
        smart_wallet_score=feats.get("smart_wallet_score"),
        smart_flow_raw=feats.get("smart_flow_raw"),
        smart_whale_alignment=_pick(
            feats, "smart_whale_alignment_consolidated", "smart_whale_alignment"
        ),
        unique_buyers_5m=feats.get("unique_buyers_5m"),
        whale_share_5m=feats.get("whale_share_5m"),
        whales_5m=feats.get("whales_5m"),
        buy_pressure_score=_pick(feats, "buy_pressure_score", "buy_pressure"),
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
        regime_score=_pick(feats, "regime_score", "regime_numeric"),
        regime_volatility=feats.get("regime_volatility"),
        ctx_open_positions=feats.get("ctx_open_positions"),
        ctx_max_positions=feats.get("ctx_max_positions"),
        ctx_daily_drawdown_pct=feats.get("ctx_daily_drawdown_pct"),
        ctx_daily_pnl_pct=_pick(feats, "ctx_daily_realized_pnl_pct", "ctx_daily_pnl_pct"),
        ctx_risk_mode=feats.get("ctx_risk_mode"),
        ctx_exposure_frac=feats.get("ctx_exposure_frac"),
        ctx_exposure_usd=feats.get("ctx_exposure_usd"),
        ctx_win_rate_pct=_pick(feats, "risk_win_rate_pct", "ctx_win_rate_pct"),
        ctx_recent_loss_streak=_pick(feats, "risk_loss_streak", "ctx_recent_loss_streak"),
        ctx_global_block_flag=_pick(feats, "ctx_global_block_flag", "global_entry_block_flag"),
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
        hist_win_rate=_pick(feats, "hist_win_rate", "sl_hist_win_rate"),
        hist_rr_ratio=_pick(feats, "hist_rr_ratio", "sl_hist_rr"),
        exploration_score=_pick(feats, "exploration_score", "sl_exploration_score"),
        source=_pick(feats, "scanner_source", "source"),
        meta=meta,
        infra_no_account=feats.get("infra_no_account"),
    )


# =============================================================================
# Feature vector bridge
# =============================================================================


def gate_feature_vector(feats: dict[str, Any]) -> tuple[bool, float, str]:
    """
    Convenience bridge:

        ok, score, reason = gate_feature_vector(features)

    Writes:
        feats["ai_buy_score"]
        feats["ai_buy_ok_hard"]
        feats["ai_buy_pass_gate"]
        feats["ai_buy_reason"]
        feats["ai_buy_gate_threshold"]
        feats["ai_buy_size_scalar"]
        feats["ai_buy_parts_json"] (optional)
        feats["ai_buy_explain_json"] (optional)
    """
    cand = from_feature_vector(feats)

    # one-pass scoring detail
    d = _compute_score_detail(cand)
    ok_hard = bool(d["ok_hard"])
    score = float(d["score"])
    base_thr = float(d["base_gate_threshold"])

    if not ok_hard:
        ok = False
        reason = ";".join(d.get("hard_reasons") or []) or "HARD_REJECT"
        gate_thr = base_thr
    else:
        if AI_PRE_BYPASS_FOR_TEST:
            ok = True
            gate_thr = base_thr
            reason = f"PASS_TEST_BYPASS score={score:.3f} thr={gate_thr:.3f}"
        else:
            gate_thr, notes = _apply_dynamic_threshold(cand, score, base_thr)
            if cand.ctx_risk_mode is not None and _as_float(cand.ctx_risk_mode, 1.0) <= 0.0:
                ok = False
                reason = f"RISK_MODE_OFF score={score:.3f} thr={gate_thr:.3f}"
            else:
                ok = bool(score >= gate_thr)
                if ok:
                    reason = f"PASS score={score:.3f} thr={gate_thr:.3f} size={float(d['size_scalar']):.2f}"
                    if bool(cand.infra_no_account) and _is_pump_source(cand.source):
                        reason += " [INFRA_NO_ACCOUNT_SOFT_LIQ]"
                    if notes:
                        reason += " [" + ",".join(notes) + "]"
                else:
                    reason = f"BELOW_GATE score={score:.3f}<{gate_thr:.3f}"
                    if bool(cand.infra_no_account) and _is_pump_source(cand.source):
                        reason += " [INFRA_NO_ACCOUNT_SOFT_LIQ]"
                    if notes:
                        reason += " [" + ",".join(notes) + "]"

    # write feature fields
    feats["ai_buy_score"] = float(score)
    feats["ai_buy_ok_hard"] = bool(ok_hard)
    feats["ai_buy_pass_gate"] = bool(ok)
    feats["ai_buy_reason"] = str(reason)
    feats["ai_buy_gate_threshold"] = float(gate_thr)
    feats["ai_buy_size_scalar"] = float(d["size_scalar"])

    if AI_BUY_ATTACH_PARTS:
        try:
            parts = d.get("parts") or {}
            feats["ai_buy_parts_json"] = d.get("parts_json") or _json_compact(
                {k: round(float(v), 4) for k, v in parts.items()}
            )
        except Exception:
            pass

    if AI_BUY_ATTACH_EXPLAIN:
        try:
            parts = d.get("parts") or {}
            soft = _soft_explain(parts, topk=max(1, AI_BUY_EXPLAIN_TOPK))
            feats["ai_buy_explain_json"] = _json_compact(
                {"top_pos": soft.get("top_pos", []), "top_neg": soft.get("top_neg", [])}
            )
        except Exception:
            pass

    return bool(ok), float(score), str(reason)
