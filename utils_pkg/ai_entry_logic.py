from __future__ import annotations

"""
utils_pkg/ai_entry_logic.py

Entry IQ engine (V3+++ / quant-grade, God-Tier Overhaul).
=========================================================

Purpose
-------
Given a *feature dict* (FeatureBuilder V3+++ output + PairFilter meta +
risk/strategy context), this module decides:

    • A continuous entry score in [0, 1]          → score_entry()
    • Whether we should actually enter            → should_enter()
    • A short human-readable reason               → for logs / debugging

Key requirements
----------------
- Deterministic baseline (heuristic) always available.
- Optional OpenAI overlay (fail-soft by default).
- Explainability: "why entered / why rejected" loggable in *one line*.
- Production-safe: no hard dependency cycles; no logger method assumptions.

Public API
----------
  async score_entry(features) -> float
  async should_enter(features) -> (bool ok, float score, str reason)
  score_entry_sync(features) -> float

Diagnostics
-----------
  async diag_snapshot() -> dict

Notes
-----
- This module expects FeatureBuilder to supply most features, but will
  best-effort enrich risk context via utils_pkg.risk_manager if missing.
"""

import asyncio
import json
import math
import os
import time
from typing import Any, Dict, Optional, Tuple

from logging_setup import get_logger

log = get_logger(__name__)

# ──────────────────────────────────────────────────────────────────────────────
# Logger safety shims (so .event/.every never explode)
# ──────────────────────────────────────────────────────────────────────────────
if not hasattr(log, "every"):

    def _every(_key: str, n: int = 200) -> bool:
        # simple stable sampler: roughly 1/n based on time bucket
        try:
            return (hash(_key) ^ int(time.time() // 5)) % max(1, int(n)) == 0
        except Exception:
            return False

    setattr(log, "every", _every)

if not hasattr(log, "event"):

    def _event(name: str, **fields: Any) -> None:
        # fall back to structured-ish info log
        try:
            log.info("%s %s", name, json.dumps(fields, default=str)[:2000])
        except Exception:
            log.info("%s", name)

    setattr(log, "event", _event)


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

ENTRY_MODEL = (os.getenv("ENTRY_MODEL_NAME", "").strip() or os.getenv("ENTRY_MODEL", "").strip())

ENTRY_MODEL_ROUTING = _env_json("ENTRY_MODEL_ROUTING_JSON") or {}
ENTRY_DEFAULT_HEAD = os.getenv("ENTRY_DEFAULT_HEAD", "default").strip() or "default"

ENTRY_AI_ENABLED = _env_bool("ENTRY_AI_ENABLED", True)
ENTRY_AI_WEIGHT = max(0.0, min(1.0, _env_float("ENTRY_AI_WEIGHT", 0.70)))

ENTRY_MIN_SCORE = max(0.0, min(1.0, _env_float("ENTRY_MIN_SCORE", 0.55)))

ENTRY_DYNAMIC_MIN_ENABLED = _env_bool("ENTRY_DYNAMIC_MIN_ENABLED", True)
ENTRY_DYNAMIC_MIN_LO = max(0.0, min(1.0, _env_float("ENTRY_DYNAMIC_MIN_LO", 0.48)))
ENTRY_DYNAMIC_MIN_HI = max(0.0, min(1.0, _env_float("ENTRY_DYNAMIC_MIN_HI", 0.72)))
ENTRY_DYNAMIC_STEP = max(0.001, min(0.05, _env_float("ENTRY_DYNAMIC_STEP", 0.01)))

ENTRY_MIN_AI_SCORE = max(0.0, min(1.0, _env_float("ENTRY_MIN_AI_SCORE", 0.0)))
ENTRY_AI_STRICT = _env_bool("ENTRY_AI_STRICT", False)

ENTRY_MAX_AGE_MIN = max(0.0, _env_float("ENTRY_MAX_AGE_MIN", 0.0))
ENTRY_MIN_LIQ_USD = max(0.0, _env_float("ENTRY_MIN_LIQ_USD", 10_000.0))
ENTRY_MAX_RUG_SCORE = max(0.0, min(1.0, _env_float("ENTRY_MAX_RUG_SCORE", 0.80)))

ENTRY_USE_PF_SCORES = _env_bool("ENTRY_USE_PF_SCORES", True)
ENTRY_MIN_PF_FINAL = max(0.0, min(1.0, _env_float("ENTRY_MIN_PF_FINAL", 0.10)))
ENTRY_MIN_PF_RISK_ADJ = max(0.0, min(1.0, _env_float("ENTRY_MIN_PF_RISK_ADJ", 0.08)))

ENTRY_MIN_5M_BUYS = max(0, _env_int("ENTRY_MIN_5M_BUYS", 0))
ENTRY_MIN_5M_VOL_USD = max(0.0, _env_float("ENTRY_MIN_5M_VOL_USD", 0.0))

ENTRY_REGIME_TIGHTEN_ENABLED = _env_bool("ENTRY_REGIME_TIGHTEN_ENABLED", True)
ENTRY_RISK_OFF_EXTRA_MARGIN = max(0.0, min(0.3, _env_float("ENTRY_RISK_OFF_EXTRA_MARGIN", 0.10)))
ENTRY_MANIP_HIGH = max(0.0, min(1.0, _env_float("ENTRY_MANIP_HIGH", 0.75)))

ENTRY_SMART_WALLET_BOOST = _env_bool("ENTRY_SMART_WALLET_BOOST", True)
ENTRY_MIN_ENTRY_MICRO = max(0.0, min(1.5, _env_float("ENTRY_MIN_ENTRY_MICRO", 0.0)))
ENTRY_MIN_STRUCTURAL_QUALITY = max(0.0, min(1.5, _env_float("ENTRY_MIN_STRUCTURAL_QUALITY", 0.0)))
ENTRY_FORBID_DEADLIKE = _env_bool("ENTRY_FORBID_DEADLIKE", False)

ENTRY_PUMP_CHASE_MAX_PENALTY = max(0.0, min(0.7, _env_float("ENTRY_PUMP_CHASE_MAX_PENALTY", 0.35)))

ENTRY_CTX_MAX_DRAWDOWN_PCT = abs(_env_float("ENTRY_CTX_MAX_DRAWDOWN_PCT", 12.0))
ENTRY_CTX_MAX_OPEN_POS_RATIO = max(0.1, min(2.0, _env_float("ENTRY_CTX_MAX_OPEN_POS_RATIO", 1.0)))
ENTRY_CTX_LOCK_AFTER_TARGET_PNL = _env_float("ENTRY_CTX_LOCK_AFTER_TARGET_PNL", 0.0)

ENTRY_REQUIRE_RISK_CAPACITY = _env_bool("ENTRY_REQUIRE_RISK_CAPACITY", False)
ENTRY_MIN_RISK_CAPACITY = max(0.0, min(1.0, _env_float("ENTRY_MIN_RISK_CAPACITY", 0.20)))

ENTRY_USE_ENTRY_QUALITY = _env_bool("ENTRY_USE_ENTRY_QUALITY", True)
ENTRY_MIN_ENTRY_QUALITY = max(0.0, min(1.5, _env_float("ENTRY_MIN_ENTRY_QUALITY", 0.0)))

ENTRY_REQUIRE_POS_SIZE = _env_bool("ENTRY_REQUIRE_POS_SIZE", False)
ENTRY_MIN_POS_SIZE = max(0.0, min(1.0, _env_float("ENTRY_MIN_POS_SIZE", 0.0)))

ENTRY_USE_EDGE_SCORE = _env_bool("ENTRY_USE_EDGE_SCORE", True)
ENTRY_MIN_EDGE_SCORE = max(0.0, min(1.0, _env_float("ENTRY_MIN_EDGE_SCORE", 0.0)))

ENTRY_USE_KELLY = _env_bool("ENTRY_USE_KELLY", False)
ENTRY_MIN_KELLY = max(0.0, min(1.0, _env_float("ENTRY_MIN_KELLY", 0.0)))
ENTRY_MAX_KELLY = max(0.0, min(1.0, _env_float("ENTRY_MAX_KELLY", 0.25)))

ENTRY_USE_BUY_SCORE = _env_bool("ENTRY_USE_BUY_SCORE", True)
ENTRY_MIN_BUY_SCORE = max(0.0, min(1.0, _env_float("ENTRY_MIN_BUY_SCORE", 0.0)))
ENTRY_BUY_SCORE_WEIGHT = max(0.0, min(1.0, _env_float("ENTRY_BUY_SCORE_WEIGHT", 0.20)))
ENTRY_BUY_HARD_FAIL = _env_bool("ENTRY_BUY_HARD_FAIL", False)

ENTRY_USE_HINT_SCORE = _env_bool("ENTRY_USE_HINT_SCORE", True)
ENTRY_MIN_HINT_SCORE = max(0.0, min(1.0, _env_float("ENTRY_MIN_HINT_SCORE", 0.0)))
ENTRY_HINT_SCORE_WEIGHT = max(0.0, min(1.0, _env_float("ENTRY_HINT_SCORE_WEIGHT", 0.10)))
ENTRY_HINT_HARD_FAIL = _env_bool("ENTRY_HINT_HARD_FAIL", False)

ENTRY_USE_PLAN_GAP_TILT = _env_bool("ENTRY_USE_PLAN_GAP_TILT", True)
ENTRY_PLAN_GAP_TILT = max(0.0, min(0.2, _env_float("ENTRY_PLAN_GAP_TILT", 0.03)))
ENTRY_PLAN_GAP_MAX_ABS = max(1.0, _env_float("ENTRY_PLAN_GAP_MAX_ABS", 25.0))

ENTRY_PLAN_STATUS_BLOCK_ENABLED = _env_bool("ENTRY_PLAN_STATUS_BLOCK_ENABLED", True)
ENTRY_PLAN_STATUS_BLOCK_KEYWORDS = os.getenv("ENTRY_PLAN_STATUS_BLOCK_KEYWORDS", "halt,no_trade").strip().lower()

ENTRY_SOURCE_SCORE_OVERRIDES = _env_json("ENTRY_SOURCE_SCORE_OVERRIDES_JSON") or {}
ENTRY_SOURCE_SCORE_BONUS = _env_json("ENTRY_SOURCE_SCORE_BONUS_JSON") or {}

ENTRY_PLAYBOOK_SCORE_OVERRIDES = _env_json("ENTRY_PLAYBOOK_SCORE_OVERRIDES_JSON") or {}
ENTRY_PLAYBOOK_SCORE_BONUS = _env_json("ENTRY_PLAYBOOK_SCORE_BONUS_JSON") or {}

ENTRY_AI_TIMEOUT_SEC = max(1.0, _env_float("ENTRY_AI_TIMEOUT_SEC", 8.0))
ENTRY_MAX_FEATURE_BYTES = _env_int("ENTRY_MAX_FEATURE_BYTES", 50_000)

ENTRY_DEBUG_LOG = _env_bool("ENTRY_DEBUG_LOG", False)
ENTRY_DEBUG_LOG_FAIL = _env_bool("ENTRY_DEBUG_LOG_FAIL", True)
ENTRY_ATTACH_META = _env_bool("ENTRY_ATTACH_META", True)

RISK_PROFILE = (os.getenv("ENTRY_RISK_PROFILE", "") or os.getenv("RISK_PROFILE", "")).strip().lower() or "balanced"
ENTRY_REGIME_RISK_MODE_LOCK = _env_int("ENTRY_REGIME_RISK_MODE_LOCK", -1)

_ENTRY_DYNAMIC_MIN_SCORE: Optional[float] = None
_DYNAMIC_LOCK = asyncio.Lock()

# Lightweight AI call throttle (per mint) to prevent spam/cost blowups
_AI_COOLDOWN_SEC = max(0.0, _env_float("ENTRY_AI_COOLDOWN_SEC", 0.0))
_AI_LAST_CALL: Dict[str, float] = {}


# ============================================================================ #
# OpenAI client shim (new SDK chat.completions + legacy ChatCompletion)
# ============================================================================ #

_CLIENT: Any = None
_CLIENT_KIND: str = "none"  # "sdk" | "legacy" | "none"


def _ensure_client() -> Optional[Any]:
    global _CLIENT, _CLIENT_KIND
    if _CLIENT is not None:
        return _CLIENT

    # New-style SDK (openai>=1.x)
    try:
        from openai import OpenAI  # type: ignore

        _CLIENT = OpenAI()
        _CLIENT_KIND = "sdk"
        log.info("ai_entry_logic: using OpenAI SDK client")
        return _CLIENT
    except Exception:
        pass

    # Legacy (openai<=0.28)
    try:
        import openai  # type: ignore

        if not getattr(openai, "api_key", None):
            openai.api_key = os.getenv("OPENAI_API_KEY", "")
        _CLIENT = openai
        _CLIENT_KIND = "legacy"
        log.info("ai_entry_logic: using legacy openai client")
        return _CLIENT
    except Exception:
        log.warning("ai_entry_logic: no OpenAI client available; AI disabled")
        _CLIENT = None
        _CLIENT_KIND = "none"
        return None


def _pick_entry_model(features: Dict[str, Any]) -> Optional[str]:
    explicit = str(features.get("entry_model_name") or "").strip()
    if explicit:
        return explicit

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

    if "default" in ENTRY_MODEL_ROUTING:
        return str(ENTRY_MODEL_ROUTING["default"])

    return ENTRY_MODEL or None


# ============================================================================ #
# Feature helpers
# ============================================================================ #


def _f(features: Dict[str, Any], key: str, default: float = 0.0) -> float:
    try:
        return float(features.get(key, default))
    except Exception:
        return float(default)


def _i(features: Dict[str, Any], key: str, default: int = 0) -> int:
    try:
        return int(features.get(key, default))
    except Exception:
        return int(default)


def _source_key(features: Dict[str, Any]) -> str:
    src = features.get("scanner_source") or features.get("source") or features.get("segment") or ""
    if isinstance(src, (int, float)):
        return f"id_{int(src)}"
    return str(src).strip().lower()


def _playbook_key(features: Dict[str, Any]) -> str:
    pb = (
        features.get("entry_playbook")
        or features.get("playbook_id")
        or features.get("playbook_name")
        or features.get("strategy_playbook")
        or ""
    )
    return str(pb).strip().lower()


def _global_min_threshold() -> float:
    if not ENTRY_DYNAMIC_MIN_ENABLED:
        return ENTRY_MIN_SCORE
    if _ENTRY_DYNAMIC_MIN_SCORE is None:
        return ENTRY_MIN_SCORE
    return float(max(0.0, min(1.0, _ENTRY_DYNAMIC_MIN_SCORE)))


def _source_min_threshold(features: Dict[str, Any]) -> float:
    base = _global_min_threshold()
    key = _source_key(features)
    if not key:
        return base
    try:
        if key in ENTRY_SOURCE_SCORE_OVERRIDES:
            return max(base, min(1.0, float(ENTRY_SOURCE_SCORE_OVERRIDES[key])))
    except Exception:
        pass
    return base


def _source_score_bonus(features: Dict[str, Any]) -> float:
    key = _source_key(features)
    if not key:
        return 0.0
    try:
        v = ENTRY_SOURCE_SCORE_BONUS.get(key)
        return float(v) if v is not None else 0.0
    except Exception:
        return 0.0


def _playbook_min_threshold(base: float, features: Dict[str, Any]) -> float:
    key = _playbook_key(features)
    if not key:
        return base
    try:
        v = ENTRY_PLAYBOOK_SCORE_OVERRIDES.get(key)
        if v is None:
            return base
        return max(base, min(1.0, float(v)))
    except Exception:
        return base


def _playbook_score_bonus(features: Dict[str, Any]) -> float:
    key = _playbook_key(features)
    if not key:
        return 0.0
    try:
        v = ENTRY_PLAYBOOK_SCORE_BONUS.get(key)
        return float(v) if v is not None else 0.0
    except Exception:
        return 0.0


def _effective_risk_profile(features: Dict[str, Any]) -> str:
    rp = str(features.get("risk_profile") or "").strip().lower()
    if rp in ("conservative", "balanced", "aggressive"):
        return rp
    return RISK_PROFILE


def _risk_profile_threshold_adjust(base: float, features: Dict[str, Any]) -> float:
    rp = _effective_risk_profile(features)
    if rp == "conservative":
        return _clip01(base + 0.03)
    if rp == "aggressive":
        return _clip01(base - 0.03)
    return _clip01(base)


def _effective_regime_mode(features: Dict[str, Any]) -> int:
    mode_feat = features.get("regime_risk_mode_lock")
    try:
        if mode_feat is not None:
            m = int(mode_feat)
            if m in (-1, 0, 1, 2):
                return m
    except Exception:
        pass

    if ENTRY_REGIME_RISK_MODE_LOCK in (0, 1, 2):
        return ENTRY_REGIME_RISK_MODE_LOCK

    try:
        ctx_rm = _f(features, "ctx_risk_mode", _f(features, "risk_mode_raw", 1.0))
        if ctx_rm <= 0.0:
            return 0
        if ctx_rm >= 2.0:
            return 2
    except Exception:
        pass

    return -1


def _one_line_explain(features: Dict[str, Any], prefix: str = "") -> str:
    """
    Produce a stable one-line explanation using entry_meta breakdown.
    Keeps it short and loggable.
    """
    try:
        meta = features.get("entry_meta") or {}
        b = meta.get("breakdown") or {}
        if not isinstance(b, dict) or not b:
            return prefix.strip()

        items = []
        for k, v in b.items():
            try:
                fv = float(v)
            except Exception:
                continue
            if abs(fv) < 0.06:
                continue
            items.append((k, fv))

        if not items:
            return prefix.strip()

        pos = sorted([it for it in items if it[1] > 0], key=lambda x: -x[1])[:2]
        neg = sorted([it for it in items if it[1] < 0], key=lambda x: x[1])[:2]

        parts = []
        if pos:
            parts.append("top+=" + ",".join([f"{k}:{v:+.2f}" for k, v in pos]))
        if neg:
            parts.append("top-=" + ",".join([f"{k}:{v:+.2f}" for k, v in neg]))

        expl = " ".join(parts)
        if prefix:
            return f"{prefix} | {expl}"
        return expl
    except Exception:
        return prefix.strip()


# ============================================================================ #
# Heuristic scoring (deterministic)
# ============================================================================ #


def _heuristic_score(features: Dict[str, Any]) -> float:
    # PairFilter priors
    pf_mom = _f(features, "pf_momentum_score", 0.0)
    pf_final = _f(features, "pf_final_score", 0.0)
    pf_risk_adj = _f(features, "pf_risk_adjusted_final", pf_final)

    pf_component = 0.0
    if ENTRY_USE_PF_SCORES:
        pf_component = 0.55 * pf_risk_adj + 0.25 * pf_final + 0.15 * pf_mom

    # ai_buy_scoring prior
    buy_component = 0.0
    if ENTRY_USE_BUY_SCORE:
        buy_score = _f(features, "ai_buy_score", 0.0)
        buy_ok_hard = bool(features.get("ai_buy_ok_hard") or features.get("ai_buy_pass_gate"))
        if buy_score > 0.0 and (ENTRY_MIN_BUY_SCORE <= 0.0 or buy_score >= ENTRY_MIN_BUY_SCORE):
            buy_component = ENTRY_BUY_SCORE_WEIGHT * buy_score
        elif ENTRY_BUY_HARD_FAIL and buy_score > 0.0 and not buy_ok_hard:
            buy_component -= 1.2

    # strategy hint prior
    hint_component = 0.0
    if ENTRY_USE_HINT_SCORE:
        hint_score = _f(features, "ai_entry_hint_score", _f(features, "entry_hint_score", 0.0))
        if hint_score > 0.0 and (ENTRY_MIN_HINT_SCORE <= 0.0 or hint_score >= ENTRY_MIN_HINT_SCORE):
            hint_component = ENTRY_HINT_SCORE_WEIGHT * hint_score

    # momentum
    mom_5s = max(-40.0, min(80.0, _f(features, "mom_5s_pct", 0.0)))
    mom_15s = max(-60.0, min(120.0, _f(features, "mom_15s_pct", 0.0)))
    mom_60s = max(-80.0, min(180.0, _f(features, "mom_60s_pct", 0.0)))
    mom_300s = _f(features, "mom_300s_pct", 0.0)
    if mom_300s == 0.0:
        mom_300s = _f(features, "price_change_5m_pct", _f(features, "chg5", 0.0))
    mom_300s = max(-80.0, min(220.0, mom_300s))

    chg_1m = max(-60.0, min(80.0, _f(features, "price_change_1m_pct", 0.0)))
    chg_5m = max(-60.0, min(120.0, _f(features, "price_change_5m_pct", chg_1m)))
    chg_15m = max(-80.0, min(180.0, _f(features, "price_change_15m_pct", 0.0)))
    chg_1h = max(-90.0, min(250.0, _f(features, "price_change_1h_pct", 0.0)))

    mom_score = 0.0
    mom_score += max(0.0, mom_5s) * 0.010
    mom_score += max(0.0, mom_15s) * 0.006
    mom_score += max(0.0, mom_60s) * 0.003
    mom_score += max(0.0, mom_300s) * 0.002
    mom_score += max(0.0, chg_1m) * 0.006
    mom_score += max(0.0, chg_5m) * 0.004
    mom_score += max(0.0, chg_15m) * 0.003
    mom_score += max(0.0, chg_1h) * 0.001

    if chg_15m < 0.0 and chg_5m > 25.0:
        mom_score -= 0.18
    if mom_300s > 40.0 and mom_15s < 0.0:
        mom_score -= 0.15

    pump_chase = _f(features, "is_pump_chasing_risk", 0.0)
    if pump_chase > 0.0:
        mom_score -= ENTRY_PUMP_CHASE_MAX_PENALTY * pump_chase

    # volatility
    vol15_pct = _f(features, "vol_15s", 0.0) * 100.0
    vol60_pct = _f(features, "vol_60s", 0.0) * 100.0
    vol300_pct = _f(features, "vol_300s", 0.0) * 100.0

    vol_score = 0.0
    for v in (vol15_pct, vol60_pct, vol300_pct):
        if v <= 0.2:
            vol_score -= 0.12
        elif 0.3 <= v <= 3.0:
            vol_score += 0.12
        elif 3.0 < v <= 8.0:
            vol_score += 0.04
        elif v > 12.0:
            vol_score -= 0.18

    # orderflow
    buys_5m = _i(features, "buys_5m", 0)
    sells_5m = _i(features, "sells_5m", 0)
    trades_5m = _f(features, "txns_5m", buys_5m + sells_5m)
    imbalance_5m = _f(features, "orderflow_skew_5m", 0.0)  # -1..1
    buy_ratio_5m = _f(features, "buy_ratio_5m", 0.5)
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

    # liquidity / mcap
    liq = _f(features, "liq_usd", 0.0)
    mcap = _f(features, "mcap_usd", 0.0)
    log_liq = _log1p(liq)
    log_mcap = _log1p(mcap)

    liq_score = 0.0
    liq_score += _f(features, "liq_bucket_ultra_low", 0.0) * -0.4
    liq_score += _f(features, "liq_bucket_low", 0.0) * -0.06
    liq_score += _f(features, "liq_bucket_sweet", 0.0) * 0.22
    liq_score += _f(features, "liq_bucket_mid", 0.0) * 0.16
    liq_score += _f(features, "liq_bucket_high", 0.0) * 0.08

    # fallback
    if _f(features, "liq_bucket_ultra_low", -1.0) < 0 and _f(features, "liq_bucket_low", -1.0) < 0:
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

    liq_score += _f(features, "mcap_bucket_early", 0.0) * 0.12
    liq_score += _f(features, "mcap_bucket_very_late", 0.0) * -0.08
    liq_score += (log_liq / 20.0) * 0.05
    liq_score += (log_mcap / 20.0) * 0.03

    # age
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

    # wallets
    smart_wallet_score = _f(features, "smart_wallet_score", 0.0)
    whales_5m = _f(features, "whales_5m", 0.0)
    whale_share_5m = _f(features, "whale_share_5m", 0.0)
    holders_growth_5m = _f(features, "holders_growth_5m", 0.0)
    unique_buyers_5m = _f(features, "unique_buyers_5m", 0.0)
    smart_wallet_count_5m = _f(features, "smart_wallet_count_5m", 0.0)
    smart_whale_alignment = _f(features, "smart_whale_alignment", 0.0)

    wallet_score = 0.0
    if ENTRY_SMART_WALLET_BOOST:
        wallet_score += smart_wallet_score * 0.7
        wallet_score += min(whales_5m, 8.0) * 0.03
        if whale_share_5m > 0.7:
            wallet_score -= (whale_share_5m - 0.7) * 0.8
        else:
            wallet_score += whale_share_5m * 0.25
        wallet_score += min(unique_buyers_5m / 20.0, 1.5) * 0.10
        wallet_score += min(smart_wallet_count_5m / 5.0, 1.5) * 0.12
        wallet_score += max(-0.2, min(holders_growth_5m, 0.5)) * 0.4
        wallet_score += smart_whale_alignment * 0.4

    # structure / quality
    structural_quality = _f(features, "structural_quality_score", 0.0)
    is_dead_like = _f(features, "is_dead_like", 0.0)
    entry_micro = _f(features, "entry_micro_alpha", 0.0)
    risk_capacity = _f(features, "risk_capacity_score", 1.0)
    entry_quality = _f(features, "entry_quality_score", 0.0)
    pos_size_scalar = _f(features, "position_size_scalar", 1.0)
    entry_risk_score = _f(features, "entry_risk_score", 0.0)
    entry_tier_bucket = _f(features, "entry_tier_bucket", 0.0)
    pos_size_bucket = _f(features, "position_size_bucket", 0.0)

    edge_score = _f(features, "entry_edge_score", _f(features, "edge_score", 0.0))
    kelly_frac = _f(features, "entry_kelly_frac", _f(features, "kelly_fraction", 0.0))

    structure_score = 0.0
    structure_score += structural_quality * 0.30
    structure_score -= is_dead_like * 0.60
    structure_score += entry_micro * 0.60
    structure_score += (risk_capacity - 0.5) * 0.20
    structure_score += max(0.0, entry_tier_bucket - 1.0) * 0.06
    structure_score += max(0.0, pos_size_bucket - 1.0) * 0.04

    quality_term = 0.0
    if ENTRY_USE_ENTRY_QUALITY and entry_quality > 0.0:
        norm_q = min(entry_quality / 1.2, 1.5)
        quality_term += (norm_q - 0.6) * 0.40

    quality_term += (pos_size_scalar - 0.5) * 0.25
    quality_term -= min(max(entry_risk_score, 0.0), 1.0) * 0.20

    if ENTRY_USE_EDGE_SCORE and edge_score != 0.0:
        e = max(-0.3, min(edge_score, 0.8))
        quality_term += e * 0.40

    if ENTRY_USE_KELLY and kelly_frac != 0.0:
        k = max(-0.5, min(kelly_frac, 0.5))
        quality_term += k * 0.25

    # regime
    regime_trend = _f(features, "regime_trend", _f(features, "regime_score", 0.0))
    regime_vol = _f(features, "regime_volatility", 0.0)
    regime_liq = _f(features, "regime_liquidity", 0.5)
    regime_manip = _f(features, "regime_manipulation_risk", 0.0)

    regime_is_risk_on = _f(features, "regime_is_risk_on", 1.0 if regime_trend > 0.3 else 0.0)
    regime_is_risk_off = _f(features, "regime_is_risk_off", 1.0 if regime_trend < -0.3 else 0.0)

    regime_component = 0.0
    regime_component += regime_trend * 0.25
    regime_component += (regime_liq - 0.5) * 0.25
    regime_component -= max(0.0, regime_manip - 0.5) * 0.6
    regime_component += regime_is_risk_on * 0.05
    regime_component -= regime_is_risk_off * 0.05
    if regime_vol > 0.7:
        regime_component -= 0.08

    # fraud / impact
    ai_fraud_raw = _f(features, "ai_fraud_score", _f(features, "ai_rug_score_01", _f(features, "ai_rug_score", 0.0)))
    ai_fraud = max(0.0, min(1.0, ai_fraud_raw / 100.0)) if ai_fraud_raw > 1.5 else max(0.0, min(1.0, ai_fraud_raw))

    impact_1k_bps = _f(features, "impact_1k_bps", 0.0)
    impact_penalty = 0.0
    if impact_1k_bps > 400:
        impact_penalty -= 0.25
    elif impact_1k_bps > 200:
        impact_penalty -= 0.12

    rug_penalty = -ai_fraud * 0.7

    # scanner hints
    scanner_momo_ok = _f(features, "scanner_momentum_ok", 0.0)
    scanner_source_id = _f(features, "scanner_source_id", 0.0)
    scanner_hint = 0.10 * scanner_momo_ok + (0.04 if scanner_source_id == 4.0 else 0.0)

    # session / ctx
    ctx_open = _f(features, "ctx_open_positions", _f(features, "risk_open_positions", 0.0))
    ctx_max = _f(features, "ctx_max_positions", _f(features, "risk_max_positions", 0.0))
    ctx_dd = _f(features, "ctx_daily_drawdown_pct", _f(features, "risk_daily_dd_pct", 0.0))
    ctx_pnl = _f(features, "ctx_daily_realized_pnl_pct", _f(features, "risk_daily_pnl_pct", 0.0))
    ctx_risk_mode = _f(features, "ctx_risk_mode", _f(features, "risk_mode_raw", 1.0))
    risk_capacity = _f(features, "risk_capacity_score", 1.0)

    win_rate_pct = _f(features, "risk_win_rate_pct", 0.0)
    loss_streak = _f(features, "risk_loss_streak", 0.0)
    plan_gap_pct = _f(features, "ctx_plan_gap_pct", 0.0)

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

    if win_rate_pct > 55.0:
        ctx_score += min((win_rate_pct - 55.0) / 25.0, 1.0) * 0.06
    if loss_streak >= 3:
        ctx_score -= min((loss_streak - 2.0) / 3.0, 1.0) * 0.14

    if ENTRY_USE_PLAN_GAP_TILT and plan_gap_pct != 0.0:
        gap = max(-ENTRY_PLAN_GAP_MAX_ABS, min(ENTRY_PLAN_GAP_MAX_ABS, plan_gap_pct))
        if gap > 0.0 and ctx_dd > -ENTRY_CTX_MAX_DRAWDOWN_PCT * 0.5:
            ctx_score += (gap / ENTRY_PLAN_GAP_MAX_ABS) * (ENTRY_PLAN_GAP_TILT * 0.6)
        elif gap < 0.0:
            ctx_score -= (abs(gap) / ENTRY_PLAN_GAP_MAX_ABS) * (ENTRY_PLAN_GAP_TILT * 0.4)

    raw = (
        pf_component
        + buy_component
        + hint_component
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

    if ai_fraud >= 0.95:
        raw -= 4.0

    try:
        score = 1.0 / (1.0 + math.exp(-raw))
    except OverflowError:
        score = 1.0 if raw > 0 else 0.0

    score = _clip01(score)

    if ENTRY_ATTACH_META:
        meta = features.get("entry_meta") or {}
        breakdown = meta.get("breakdown") or {}
        breakdown.update(
            {
                "pf": float(pf_component),
                "buy": float(buy_component),
                "hint": float(hint_component),
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

    if ENTRY_DEBUG_LOG and log.every("ENTRY_HEURISTIC", 200):
        sym = features.get("symbol") or features.get("mint") or "?"
        log.debug("ENTRY_HEURISTIC sym=%s raw=%.3f score=%.3f", sym, raw, score)

    return score


# ============================================================================ #
# RiskManager enrichment helper (risk API coupling)
# ============================================================================ #


async def _maybe_enrich_risk_context(features: Dict[str, Any]) -> None:
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

        if needs_capacity:
            features.setdefault("risk_capacity_score", float(snap.get("risk_capacity", 1.0)))

        if needs_ctx:
            features.setdefault("ctx_open_positions", float(snap.get("open_positions", 0.0)))
            features.setdefault("ctx_max_positions", float(snap.get("max_positions", 0.0)))
            features.setdefault(
                "ctx_daily_drawdown_pct",
                float(snap.get("daily_drawdown_pct", snap.get("max_dd_pct", 0.0))),
            )
            features.setdefault("ctx_daily_realized_pnl_pct", float(snap.get("daily_realized_pnl_pct", 0.0)))
            features.setdefault("ctx_risk_mode", float(snap.get("risk_mode", 1.0)))

            if "rolling_win_rate_pct" in snap:
                features.setdefault("risk_win_rate_pct", float(snap.get("rolling_win_rate_pct", 0.0)))
            if "recent_loss_streak" in snap:
                features.setdefault("risk_loss_streak", float(snap.get("recent_loss_streak", 0.0)))

        if "risk_profile" in snap and "risk_profile" not in features:
            features["risk_profile"] = str(snap.get("risk_profile") or "balanced")

        if "hard_halt" in snap or "HARD_HALT" in snap:
            hh = bool(snap.get("hard_halt", snap.get("HARD_HALT", False)))
            features.setdefault("ctx_hard_halt", hh)
            features.setdefault("hard_halt", hh)

        if "plan_gap_pct" in snap and "ctx_plan_gap_pct" not in features:
            features["ctx_plan_gap_pct"] = float(snap.get("plan_gap_pct", 0.0))
        if "plan_status" in snap and "ctx_plan_status" not in features:
            features["ctx_plan_status"] = str(snap.get("plan_status", "n/a"))

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
    if not ENTRY_AI_ENABLED:
        return None
    if not os.getenv("OPENAI_API_KEY"):
        return None

    model = _pick_entry_model(features)
    if not model:
        return None

    # throttle (per mint)
    if _AI_COOLDOWN_SEC > 0:
        key = str(features.get("mint") or features.get("symbol") or "")
        if key:
            now = time.time()
            last = _AI_LAST_CALL.get(key, 0.0)
            if now - last < _AI_COOLDOWN_SEC:
                return None
            _AI_LAST_CALL[key] = now

    client = _ensure_client()
    if client is None:
        return None

    feat = dict(features)
    try:
        blob = json.dumps(feat, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
        if len(blob) > ENTRY_MAX_FEATURE_BYTES:
            keep_keys = {
                "symbol",
                "mint",
                "pf_final_score",
                "pf_risk_adjusted_final",
                "pf_momentum_score",
                "entry_quality_score",
                "entry_edge_score",
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
                "ai_entry_hint_score",
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
                "ctx_plan_gap_pct",
                "ctx_plan_status",
                "ctx_entry_score_gate",
                "ctx_hard_halt",
                "entry_playbook",
                "playbook_id",
                "playbook_name",
            }
            feat = {k: v for k, v in feat.items() if k in keep_keys}
    except Exception:
        feat = {"features": "encode_error"}

    payload = {"features": feat, "task": "score_entry_probability_0_to_1"}

    prompt = (
        "Return ONLY JSON: {\"score\": <number between 0 and 1>}.\n"
        "Score is probability that entering NOW will be profitable.\n\n"
        f"{json.dumps(payload, separators=(',', ':'), ensure_ascii=False)}"
    )

    try:
        if _CLIENT_KIND == "sdk":

            def _do_req_sdk() -> Any:
                try:
                    return client.chat.completions.create(
                        model=model,
                        messages=[
                            {"role": "system", "content": "You output JSON only."},
                            {"role": "user", "content": prompt},
                        ],
                        temperature=0.0,
                        response_format={"type": "json_object"},
                    )
                except TypeError:
                    return client.chat.completions.create(
                        model=model,
                        messages=[
                            {"role": "system", "content": "You output JSON only."},
                            {"role": "user", "content": prompt},
                        ],
                        temperature=0.0,
                    )

            resp = await asyncio.wait_for(asyncio.to_thread(_do_req_sdk), timeout=ENTRY_AI_TIMEOUT_SEC)
            content_str = getattr(resp.choices[0].message, "content", None)  # type: ignore[attr-defined]
            if not content_str:
                return None

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

            resp = await asyncio.wait_for(asyncio.to_thread(_do_req_legacy), timeout=ENTRY_AI_TIMEOUT_SEC)
            content_str = resp["choices"][0]["message"]["content"]  # type: ignore[index]

        try:
            obj = json.loads(content_str)
            score = _clip01(float(obj.get("score", 0.0)))
        except Exception:
            try:
                score = _clip01(float(str(content_str).strip()))
            except Exception:
                if ENTRY_DEBUG_LOG_FAIL:
                    log.warning("ai_entry_logic: could not parse AI score content=%r", str(content_str)[:200])
                return None

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
    await _maybe_enrich_risk_context(features)

    heuristic = _heuristic_score(features)
    ai_score = await _ai_score(features)

    if ai_score is None:
        blended = heuristic
    else:
        blended = ENTRY_AI_WEIGHT * ai_score + (1.0 - ENTRY_AI_WEIGHT) * heuristic

    blended = _clip01(blended + _source_score_bonus(features) + _playbook_score_bonus(features))

    if ENTRY_ATTACH_META:
        meta = features.get("entry_meta") or {}
        meta.update(
            {
                "heuristic_score": float(heuristic),
                "ai_score": float(ai_score) if ai_score is not None else None,
                "blended_score": float(blended),
                "buy_score": float(_f(features, "ai_buy_score", 0.0)),
                "hint_score": float(_f(features, "ai_entry_hint_score", _f(features, "entry_hint_score", 0.0))),
                "source_key": _source_key(features),
                "playbook_key": _playbook_key(features),
                "risk_profile": _effective_risk_profile(features),
                "regime_risk_mode": _effective_regime_mode(features),
            }
        )
        features["entry_meta"] = meta

    return blended


async def should_enter(features: Dict[str, Any]) -> Tuple[bool, float, str]:
    score = await score_entry(features)

    liq = _f(features, "liq_usd", 0.0)
    age_min = _f(features, "age_min", _f(features, "token_age_min", 0.0))

    regime_trend = _f(features, "regime_trend", _f(features, "regime_score", 0.0))
    regime_manip = _f(features, "regime_manipulation_risk", 0.0)

    structural_quality = _f(features, "structural_quality_score", 0.0)
    is_dead_like = _f(features, "is_dead_like", 0.0)
    entry_micro = _f(features, "entry_micro_alpha", 0.0)

    entry_quality = _f(features, "entry_quality_score", 0.0)
    pos_size_scalar = _f(features, "position_size_scalar", 1.0)
    entry_block_flag = _f(features, "entry_block_flag", 0.0)
    entry_tier_bucket = _f(features, "entry_tier_bucket", 0.0)
    pos_size_bucket = _f(features, "position_size_bucket", 0.0)

    edge_score = _f(features, "entry_edge_score", _f(features, "edge_score", 0.0))
    kelly_frac = _f(features, "entry_kelly_frac", _f(features, "kelly_fraction", 0.0))
    has_edge = ("entry_edge_score" in features) or ("edge_score" in features)
    has_kelly = ("entry_kelly_frac" in features) or ("kelly_fraction" in features)

    ai_fraud_raw = _f(features, "ai_fraud_score", _f(features, "ai_rug_score_01", _f(features, "ai_rug_score", 0.0)))
    ai_fraud = max(0.0, min(1.0, ai_fraud_raw / 100.0)) if ai_fraud_raw > 1.5 else max(0.0, min(1.0, ai_fraud_raw))

    pf_final = _f(features, "pf_final_score", 0.0)
    pf_risk_adj = _f(features, "pf_risk_adjusted_final", pf_final * (1.0 - ai_fraud) if pf_final > 0.0 else 0.0)

    buys_5m = _i(features, "buys_5m", 0)
    vol_5m_usd = _f(features, "vol_5m_usd", 0.0)

    ctx_open = _f(features, "ctx_open_positions", _f(features, "risk_open_positions", 0.0))
    ctx_max = _f(features, "ctx_max_positions", _f(features, "risk_max_positions", 0.0))
    ctx_dd = _f(features, "ctx_daily_drawdown_pct", _f(features, "risk_daily_dd_pct", 0.0))
    ctx_pnl = _f(features, "ctx_daily_realized_pnl_pct", _f(features, "risk_daily_pnl_pct", 0.0))
    ctx_risk_mode = _f(features, "ctx_risk_mode", _f(features, "risk_mode_raw", 1.0))
    risk_capacity = _f(features, "risk_capacity_score", 1.0)

    win_rate_pct = _f(features, "risk_win_rate_pct", 0.0)
    loss_streak = _f(features, "risk_loss_streak", 0.0)

    hard_halt = bool(features.get("ctx_hard_halt") or features.get("hard_halt") or features.get("strategy_hard_halt"))
    plan_gap_pct = _f(features, "ctx_plan_gap_pct", 0.0)
    plan_status = str(features.get("ctx_plan_status", "n/a"))

    regime_mode_lock = _effective_regime_mode(features)

    hint_score = _f(features, "ai_entry_hint_score", _f(features, "entry_hint_score", 0.0))
    has_hint = ("ai_entry_hint_score" in features) or ("entry_hint_score" in features)
    hint_ok = bool(features.get("ai_entry_hint_ok") or features.get("entry_hint_ok"))

    # ------------------------------------------------------------------ #
    # Optional hard guards from priors
    # ------------------------------------------------------------------ #
    if ENTRY_USE_BUY_SCORE and ENTRY_BUY_HARD_FAIL:
        buy_score = _f(features, "ai_buy_score", 0.0)
        buy_ok_hard = bool(features.get("ai_buy_ok_hard") or features.get("ai_buy_pass_gate"))
        if buy_score > 0.0 and not buy_ok_hard:
            reason = "ai_buy_scoring_hard_fail"
            if ENTRY_ATTACH_META:
                features.setdefault("entry_meta", {}).update({"reason": reason, "pass": False})
            return False, score, _one_line_explain(features, reason)

    if ENTRY_USE_HINT_SCORE and ENTRY_HINT_HARD_FAIL and has_hint:
        if ENTRY_MIN_HINT_SCORE > 0 and hint_score < ENTRY_MIN_HINT_SCORE:
            reason = f"entry_hint_below_min({hint_score:.3f}<{ENTRY_MIN_HINT_SCORE:.3f})"
            if ENTRY_ATTACH_META:
                features.setdefault("entry_meta", {}).update({"reason": reason, "pass": False, "hint_score": float(hint_score)})
            return False, score, _one_line_explain(features, reason)
        if not hint_ok and hint_score > 0.0:
            reason = "entry_hint_hard_fail"
            if ENTRY_ATTACH_META:
                features.setdefault("entry_meta", {}).update({"reason": reason, "pass": False, "hint_score": float(hint_score)})
            return False, score, _one_line_explain(features, reason)

    if ENTRY_USE_EDGE_SCORE and ENTRY_MIN_EDGE_SCORE > 0 and has_edge and edge_score < ENTRY_MIN_EDGE_SCORE:
        reason = f"low_entry_edge_score({edge_score:.3f}<{ENTRY_MIN_EDGE_SCORE:.3f})"
        if ENTRY_ATTACH_META:
            features.setdefault("entry_meta", {}).update({"reason": reason, "pass": False, "edge_score": float(edge_score)})
        return False, score, _one_line_explain(features, reason)

    if ENTRY_USE_KELLY and has_kelly:
        if kelly_frac <= 0.0:
            reason = f"non_positive_kelly({kelly_frac:.3f}<=0)"
            if ENTRY_ATTACH_META:
                features.setdefault("entry_meta", {}).update({"reason": reason, "pass": False, "kelly_frac": float(kelly_frac)})
            return False, score, _one_line_explain(features, reason)
        if ENTRY_MIN_KELLY > 0 and kelly_frac < ENTRY_MIN_KELLY:
            reason = f"kelly_below_min({kelly_frac:.3f}<{ENTRY_MIN_KELLY:.3f})"
            if ENTRY_ATTACH_META:
                features.setdefault("entry_meta", {}).update({"reason": reason, "pass": False, "kelly_frac": float(kelly_frac)})
            return False, score, _one_line_explain(features, reason)

    # ------------------------------------------------------------------ #
    # Hard guards (capital protection)
    # ------------------------------------------------------------------ #
    if hard_halt:
        reason = "risk_api_hard_halt"
        if ENTRY_ATTACH_META:
            features.setdefault("entry_meta", {}).update({"reason": reason, "pass": False, "hard_halt": True})
        return False, score, reason

    if ENTRY_PLAN_STATUS_BLOCK_ENABLED and plan_status:
        ps_lc = plan_status.lower()
        for kw in ENTRY_PLAN_STATUS_BLOCK_KEYWORDS.split(","):
            kw = kw.strip()
            if kw and kw in ps_lc:
                reason = f"plan_status_block({ps_lc})"
                if ENTRY_ATTACH_META:
                    features.setdefault("entry_meta", {}).update({"reason": reason, "pass": False, "plan_status": plan_status})
                return False, score, reason

    if risk_capacity <= 0.0:
        reason = "risk_capacity_zero"
        if ENTRY_ATTACH_META:
            features.setdefault("entry_meta", {}).update({"reason": reason, "pass": False, "risk_capacity": float(risk_capacity)})
        return False, score, reason

    if entry_block_flag >= 0.5:
        reason = "entry_block_flag"
        if ENTRY_ATTACH_META:
            features.setdefault("entry_meta", {}).update({"reason": reason, "pass": False})
        return False, score, reason

    if liq < ENTRY_MIN_LIQ_USD:
        reason = f"low_liq({liq:.0f}<{ENTRY_MIN_LIQ_USD:.0f})"
        if ENTRY_ATTACH_META:
            features.setdefault("entry_meta", {}).update({"reason": reason, "pass": False})
        return False, score, reason

    if ENTRY_CTX_MAX_DRAWDOWN_PCT > 0 and ctx_dd <= -ENTRY_CTX_MAX_DRAWDOWN_PCT:
        reason = f"session_drawdown_limit({ctx_dd:.2f}<=-{ENTRY_CTX_MAX_DRAWDOWN_PCT:.1f})"
        if ENTRY_ATTACH_META:
            features.setdefault("entry_meta", {}).update({"reason": reason, "pass": False, "dd_lock": True})
        return False, score, reason

    if ENTRY_CTX_LOCK_AFTER_TARGET_PNL > 0 and ctx_pnl >= ENTRY_CTX_LOCK_AFTER_TARGET_PNL and ctx_risk_mode <= 1.0:
        reason = f"session_target_hit({ctx_pnl:.2f}>={ENTRY_CTX_LOCK_AFTER_TARGET_PNL:.2f})"
        if ENTRY_ATTACH_META:
            features.setdefault("entry_meta", {}).update({"reason": reason, "pass": False, "target_lock": True})
        return False, score, reason

    if ctx_max > 0 and ENTRY_CTX_MAX_OPEN_POS_RATIO > 0:
        ratio = ctx_open / ctx_max
        if ratio >= ENTRY_CTX_MAX_OPEN_POS_RATIO:
            reason = f"too_many_positions({ctx_open:.0f}/{ctx_max:.0f},ratio={ratio:.2f})"
            if ENTRY_ATTACH_META:
                features.setdefault("entry_meta", {}).update({"reason": reason, "pass": False, "pos_full": True})
            return False, score, reason

    if ai_fraud >= ENTRY_MAX_RUG_SCORE:
        reason = f"high_fraud_score({ai_fraud:.2f}>={ENTRY_MAX_RUG_SCORE:.2f})"
        if ENTRY_ATTACH_META:
            features.setdefault("entry_meta", {}).update({"reason": reason, "pass": False, "fraud_lock": True})
        return False, score, reason

    if ENTRY_MAX_AGE_MIN > 0 and age_min > ENTRY_MAX_AGE_MIN * 2:
        reason = f"too_old({age_min:.1f}>{ENTRY_MAX_AGE_MIN*2:.1f})"
        if ENTRY_ATTACH_META:
            features.setdefault("entry_meta", {}).update({"reason": reason, "pass": False})
        return False, score, reason

    if ENTRY_FORBID_DEADLIKE and is_dead_like >= 0.5:
        reason = "dead_like_heuristic"
        if ENTRY_ATTACH_META:
            features.setdefault("entry_meta", {}).update({"reason": reason, "pass": False})
        return False, score, reason

    if ENTRY_MIN_STRUCTURAL_QUALITY > 0 and structural_quality < ENTRY_MIN_STRUCTURAL_QUALITY:
        reason = f"low_structural_quality({structural_quality:.2f}<{ENTRY_MIN_STRUCTURAL_QUALITY:.2f})"
        if ENTRY_ATTACH_META:
            features.setdefault("entry_meta", {}).update({"reason": reason, "pass": False})
        return False, score, reason

    if ENTRY_MIN_ENTRY_MICRO > 0 and entry_micro < ENTRY_MIN_ENTRY_MICRO:
        reason = f"low_entry_micro_alpha({entry_micro:.2f}<{ENTRY_MIN_ENTRY_MICRO:.2f})"
        if ENTRY_ATTACH_META:
            features.setdefault("entry_meta", {}).update({"reason": reason, "pass": False})
        return False, score, reason

    if ENTRY_USE_ENTRY_QUALITY and ENTRY_MIN_ENTRY_QUALITY > 0 and entry_quality < ENTRY_MIN_ENTRY_QUALITY:
        reason = f"low_entry_quality({entry_quality:.2f}<{ENTRY_MIN_ENTRY_QUALITY:.2f})"
        if ENTRY_ATTACH_META:
            features.setdefault("entry_meta", {}).update({"reason": reason, "pass": False})
        return False, score, reason

    if ENTRY_REQUIRE_POS_SIZE and pos_size_scalar < ENTRY_MIN_POS_SIZE:
        reason = f"small_position_size_scalar({pos_size_scalar:.2f}<{ENTRY_MIN_POS_SIZE:.2f})"
        if ENTRY_ATTACH_META:
            features.setdefault("entry_meta", {}).update({"reason": reason, "pass": False})
        return False, score, reason

    if ENTRY_MIN_5M_BUYS > 0 and buys_5m < ENTRY_MIN_5M_BUYS:
        reason = f"low_recent_flow(buys_5m={buys_5m}<{ENTRY_MIN_5M_BUYS})"
        if ENTRY_ATTACH_META:
            features.setdefault("entry_meta", {}).update({"reason": reason, "pass": False})
        return False, score, reason

    if ENTRY_MIN_5M_VOL_USD > 0 and vol_5m_usd < ENTRY_MIN_5M_VOL_USD:
        reason = f"low_recent_volume(vol_5m={vol_5m_usd:.0f}<{ENTRY_MIN_5M_VOL_USD:.0f})"
        if ENTRY_ATTACH_META:
            features.setdefault("entry_meta", {}).update({"reason": reason, "pass": False})
        return False, score, reason

    if ENTRY_USE_PF_SCORES:
        if pf_final > 0.0 and pf_final < ENTRY_MIN_PF_FINAL:
            reason = f"pf_final_below_min({pf_final:.3f}<{ENTRY_MIN_PF_FINAL:.3f})"
            if ENTRY_ATTACH_META:
                features.setdefault("entry_meta", {}).update({"reason": reason, "pass": False})
            return False, score, reason
        if pf_risk_adj > 0.0 and pf_risk_adj < ENTRY_MIN_PF_RISK_ADJ:
            reason = f"pf_risk_adj_below_min({pf_risk_adj:.3f}<{ENTRY_MIN_PF_RISK_ADJ:.3f})"
            if ENTRY_ATTACH_META:
                features.setdefault("entry_meta", {}).update({"reason": reason, "pass": False})
            return False, score, reason

    if ENTRY_REQUIRE_RISK_CAPACITY and risk_capacity < ENTRY_MIN_RISK_CAPACITY:
        reason = f"low_risk_capacity({risk_capacity:.2f}<{ENTRY_MIN_RISK_CAPACITY:.2f})"
        if ENTRY_ATTACH_META:
            features.setdefault("entry_meta", {}).update({"reason": reason, "pass": False})
        return False, score, reason

    if loss_streak >= 4 and _effective_risk_profile(features) != "aggressive":
        reason = f"loss_streak_guard({loss_streak:.0f}>=4)"
        if ENTRY_ATTACH_META:
            features.setdefault("entry_meta", {}).update({"reason": reason, "pass": False, "streak_lock": True})
        return False, score, reason

    # ------------------------------------------------------------------ #
    # Thresholding: regime + profile + plan tilt
    # ------------------------------------------------------------------ #
    base_threshold = _source_min_threshold(features)
    base_threshold = _playbook_min_threshold(base_threshold, features)

    ctx_entry_gate = _f(features, "ctx_entry_score_gate", 0.0)
    if ctx_entry_gate > 0.0:
        base_threshold = max(base_threshold, _clip01(ctx_entry_gate))

    tightened = base_threshold
    risk_off_flag = False
    manip_flag = False

    if regime_mode_lock == 0:
        regime_trend = min(regime_trend, -0.8)
    elif regime_mode_lock == 2:
        regime_trend = max(regime_trend, 0.8)

    if ENTRY_REGIME_TIGHTEN_ENABLED:
        if regime_trend <= -0.7:
            tightened = max(tightened + ENTRY_RISK_OFF_EXTRA_MARGIN, 0.80)
            risk_off_flag = True
        if regime_manip >= ENTRY_MANIP_HIGH:
            tightened = max(tightened + 0.08, tightened)
            manip_flag = True

    prof_threshold = _risk_profile_threshold_adjust(tightened, features)

    if entry_tier_bucket >= 3.0 and pos_size_bucket >= 3.0:
        prof_threshold = max(base_threshold, prof_threshold - 0.01)
    elif entry_tier_bucket <= 1.0 and pos_size_bucket <= 1.0:
        prof_threshold = min(1.0, prof_threshold + 0.01)

    plan_delta = 0.0
    if ENTRY_USE_PLAN_GAP_TILT and plan_gap_pct != 0.0:
        gap = max(-ENTRY_PLAN_GAP_MAX_ABS, min(ENTRY_PLAN_GAP_MAX_ABS, plan_gap_pct))
        before = prof_threshold
        if gap > 0.0 and ctx_dd > -ENTRY_CTX_MAX_DRAWDOWN_PCT * 0.5:
            prof_threshold = max(
                ENTRY_DYNAMIC_MIN_LO,
                prof_threshold - (gap / ENTRY_PLAN_GAP_MAX_ABS) * ENTRY_PLAN_GAP_TILT,
            )
        elif gap < 0.0:
            prof_threshold = min(
                1.0,
                prof_threshold + (abs(gap) / ENTRY_PLAN_GAP_MAX_ABS) * (ENTRY_PLAN_GAP_TILT * 0.5),
            )
        prof_threshold = max(base_threshold, prof_threshold)
        plan_delta = prof_threshold - before

    if score < prof_threshold:
        reason = f"score_below_threshold({score:.3f}<{prof_threshold:.3f})"
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
                    "plan_gap_threshold_delta": float(plan_delta),
                    "source_key": _source_key(features),
                    "playbook_key": _playbook_key(features),
                }
            )
        return False, score, _one_line_explain(features, reason)

    # pass
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
                "plan_gap_threshold_delta": float(plan_delta),
                "source_key": _source_key(features),
                "playbook_key": _playbook_key(features),
            }
        )

    return True, score, _one_line_explain(features, reason)


def score_entry_sync(features: Dict[str, Any]) -> float:
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = None

    if loop and loop.is_running():
        if ENTRY_DEBUG_LOG:
            log.debug("score_entry_sync called in async context; heuristic-only")
        return _heuristic_score(features)

    return asyncio.run(score_entry(features))


# ============================================================================ #
# AI maintenance / calibration loop
# ============================================================================ #


async def ai_maintenance_loop(stop_evt: asyncio.Event) -> None:
    global _ENTRY_DYNAMIC_MIN_SCORE

    if not ENTRY_DYNAMIC_MIN_ENABLED:
        log.event("AI_MAINT_DISABLED", reason="ENTRY_DYNAMIC_MIN_ENABLED=0")
        return

    interval_sec = max(30.0, _env_float("ENTRY_AI_MAINT_INTERVAL_SEC", 180.0))
    backoff = interval_sec

    async with _DYNAMIC_LOCK:
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
            snap: Dict[str, Any] = {}
            try:
                from utils_pkg import risk_manager as _rm  # type: ignore

                snap = await _rm.risk_capacity_snapshot()
            except Exception as e:
                if ENTRY_DEBUG_LOG:
                    log.debug("ai_maintenance: risk_capacity_snapshot failed: %s", e)

            win_rate = float(snap.get("rolling_win_rate_pct", 0.0))
            daily_pnl_pct = float(snap.get("daily_realized_pnl_pct", 0.0))
            daily_dd_pct = float(snap.get("daily_drawdown_pct", snap.get("max_dd_pct", 0.0)))
            loss_streak = float(snap.get("recent_loss_streak", 0.0))
            plan_gap_pct = float(snap.get("plan_gap_pct", 0.0))

            prev_floor = _global_min_threshold()
            new_floor = prev_floor

            dd_bad = daily_dd_pct <= -ENTRY_CTX_MAX_DRAWDOWN_PCT * 0.5
            win_good = win_rate >= 55.0
            win_excellent = win_rate >= 65.0
            pnl_good = daily_pnl_pct >= 4.0
            pnl_bad = daily_pnl_pct <= -3.0
            streak_bad = loss_streak >= 4.0

            if dd_bad or pnl_bad or streak_bad:
                new_floor = min(ENTRY_DYNAMIC_MIN_HI, prev_floor + ENTRY_DYNAMIC_STEP)
            elif (win_excellent and daily_pnl_pct >= 6.0) or (win_good and pnl_good):
                new_floor = max(ENTRY_DYNAMIC_MIN_LO, prev_floor - ENTRY_DYNAMIC_STEP)

            if ENTRY_USE_PLAN_GAP_TILT and new_floor == prev_floor and plan_gap_pct != 0.0:
                gap = max(-ENTRY_PLAN_GAP_MAX_ABS, min(ENTRY_PLAN_GAP_MAX_ABS, plan_gap_pct))
                if gap > 0.0 and daily_dd_pct > -ENTRY_CTX_MAX_DRAWDOWN_PCT * 0.3:
                    new_floor = max(ENTRY_DYNAMIC_MIN_LO, prev_floor - ENTRY_DYNAMIC_STEP * 0.5)
                elif gap < 0.0 and daily_pnl_pct >= 0.0:
                    new_floor = min(ENTRY_DYNAMIC_MIN_HI, prev_floor + ENTRY_DYNAMIC_STEP * 0.5)

            new_floor = max(ENTRY_DYNAMIC_MIN_LO, min(ENTRY_DYNAMIC_MIN_HI, new_floor))

            async with _DYNAMIC_LOCK:
                _ENTRY_DYNAMIC_MIN_SCORE = new_floor

            log.event(
                "AI_MAINT_STEP",
                prev_floor=float(prev_floor),
                new_floor=float(new_floor),
                win_rate=win_rate,
                daily_pnl_pct=daily_pnl_pct,
                daily_dd_pct=daily_dd_pct,
                loss_streak=loss_streak,
                plan_gap_pct=plan_gap_pct,
            )

            backoff = interval_sec

        except asyncio.CancelledError:
            raise
        except Exception as e:
            log.event("AI_MAINT_ERROR", error=str(e))
            backoff = min(backoff * 1.5, max(600.0, interval_sec * 4))

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
        "ENTRY_USE_EDGE_SCORE": ENTRY_USE_EDGE_SCORE,
        "ENTRY_MIN_EDGE_SCORE": ENTRY_MIN_EDGE_SCORE,
        "ENTRY_USE_KELLY": ENTRY_USE_KELLY,
        "ENTRY_MIN_KELLY": ENTRY_MIN_KELLY,
        "ENTRY_MAX_KELLY": ENTRY_MAX_KELLY,
        "ENTRY_USE_BUY_SCORE": ENTRY_USE_BUY_SCORE,
        "ENTRY_MIN_BUY_SCORE": ENTRY_MIN_BUY_SCORE,
        "ENTRY_BUY_SCORE_WEIGHT": ENTRY_BUY_SCORE_WEIGHT,
        "ENTRY_BUY_HARD_FAIL": ENTRY_BUY_HARD_FAIL,
        "ENTRY_USE_HINT_SCORE": ENTRY_USE_HINT_SCORE,
        "ENTRY_MIN_HINT_SCORE": ENTRY_MIN_HINT_SCORE,
        "ENTRY_HINT_SCORE_WEIGHT": ENTRY_HINT_SCORE_WEIGHT,
        "ENTRY_HINT_HARD_FAIL": ENTRY_HINT_HARD_FAIL,
        "ENTRY_USE_PLAN_GAP_TILT": ENTRY_USE_PLAN_GAP_TILT,
        "ENTRY_PLAN_GAP_TILT": ENTRY_PLAN_GAP_TILT,
        "ENTRY_PLAN_GAP_MAX_ABS": ENTRY_PLAN_GAP_MAX_ABS,
        "ENTRY_PLAN_STATUS_BLOCK_ENABLED": ENTRY_PLAN_STATUS_BLOCK_ENABLED,
        "ENTRY_PLAN_STATUS_BLOCK_KEYWORDS": ENTRY_PLAN_STATUS_BLOCK_KEYWORDS,
        "ENTRY_AI_TIMEOUT_SEC": ENTRY_AI_TIMEOUT_SEC,
        "ENTRY_MAX_FEATURE_BYTES": ENTRY_MAX_FEATURE_BYTES,
        "ENTRY_DEBUG_LOG": ENTRY_DEBUG_LOG,
        "ENTRY_DEBUG_LOG_FAIL": ENTRY_DEBUG_LOG_FAIL,
        "ENTRY_ATTACH_META": ENTRY_ATTACH_META,
        "RISK_PROFILE": RISK_PROFILE,
        "ENTRY_REGIME_RISK_MODE_LOCK": ENTRY_REGIME_RISK_MODE_LOCK,
        "ENTRY_SOURCE_SCORE_OVERRIDES": ENTRY_SOURCE_SCORE_OVERRIDES,
        "ENTRY_SOURCE_SCORE_BONUS": ENTRY_SOURCE_SCORE_BONUS,
        "ENTRY_PLAYBOOK_SCORE_OVERRIDES": ENTRY_PLAYBOOK_SCORE_OVERRIDES,
        "ENTRY_PLAYBOOK_SCORE_BONUS": ENTRY_PLAYBOOK_SCORE_BONUS,
        "ENTRY_AI_COOLDOWN_SEC": _AI_COOLDOWN_SEC,
    }

    out: Dict[str, Any] = {
        "config": cfg,
        "dynamic_floor": _global_min_threshold(),
        "dynamic_min_score_raw": _ENTRY_DYNAMIC_MIN_SCORE,
        "source_score_overrides": ENTRY_SOURCE_SCORE_OVERRIDES,
        "source_score_bonus": ENTRY_SOURCE_SCORE_BONUS,
        "playbook_score_overrides": ENTRY_PLAYBOOK_SCORE_OVERRIDES,
        "playbook_score_bonus": ENTRY_PLAYBOOK_SCORE_BONUS,
    }

    try:
        from utils_pkg import risk_manager as _rm  # type: ignore

        snap = await _rm.risk_capacity_snapshot()
        if isinstance(snap, dict):
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
                "plan_gap_pct": snap.get("plan_gap_pct"),
                "plan_status": snap.get("plan_status"),
            }
    except Exception as e:
        out["risk_error"] = f"risk_manager_snapshot_failed: {e}"

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


__all__ = [
    "score_entry",
    "should_enter",
    "score_entry_sync",
    "_heuristic_score",
    "_ai_score",
    "ai_maintenance_loop",
    "diag_snapshot",
]
