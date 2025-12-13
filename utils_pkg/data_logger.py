from __future__ import annotations

"""
utils_pkg/data_logger.py

God-Tier V3+++ data logger for AI training, quant analytics & self-learning.
===========================================================================

High-level responsibilities
---------------------------
Centralized logger for everything the stack *sees* and *does*:

  • Entry candidates
      - what we *considered* buying (PF meta + regime / wallet / AI context)
      - normalized into feature vectors via FeatureBuilder (V3+++)

  • Decisions
      - BUY / SKIP / TEST_BUY + scores, reasons, risk context, AI meta
      - optional late updates (async checks) via ai_db.update_entry_decision()

  • Outcomes
      - realized PnL / hold time / max DD / exit policy metadata
      - coarse label categories for ML / stats: "mega_win", "loss", etc.

  • Optional extras
      - Trade executions (fills)
      - Regime snapshots (market state over time)
      - AI events (risk halts, parameter tunes, self-learning steps)
      - Equity curve points (account-level PnL / equity tracking)
      - Risk-manager snapshots (risk mode, capacity, drawdown, halts)
      - Reinvestor snapshots (soft/hard stops, kelly/target profiles)

Plus ergonomic per-source wrappers:

  - log_raydium_candidate(...)
  - log_pumpfun_candidate(...)
  - log_dexscreener_candidate(...)
  - log_momo_candidate(...)

Design goals
------------
  • Single, stable API used by scanners, strategy, exit engine, RL brain.
  • Rich logging for:
        - ML training (ai_entry_logic, ai_sell_logic, ai_buy_scoring,
          self_learning_ai_strategy)
        - quant backtests (scripts/backtest.py, scripts/test_stack.py)
        - policy evaluation (perf-by-policy-version / experiment-tag)
  • Safe + configurable:
        - PG_ENABLED / AI_LOG_* flags control behavior
        - DB errors swallowed in non-strict mode (AI_LOG_STRICT=0)
        - optional auto-schema init via ai_db.ensure_ai_tables()
  • Backwards compatible with existing call sites.
  • Extra helpers are "soft-hooks":
        - They no-op cleanly if ai_db does not expose corresponding functions.

This module is the Phase-0/1 backbone for the "God-Tier Overhaul" roadmap:

  Phase 0  – DB & schema bootstrap (ai_db.ensure_ai_tables)
  Phase 1  – Rich candidate/outcome logging (this module + FeatureBuilder)
  Phase 2  – AI entry/sell logic training data (ai_entry_logic/ai_sell_logic)
  Phase 3  – Risk manager & reinvestor analytics
  Phase 4  – Self-learning policy search (self_learning_ai_strategy)
  Phase 5  – Regime-aware analytics and dashboards
  Phase 6  – Quant-grade backtesting & policy evaluation
  Phase 7  – Continuous online learning / live A/B experiments
"""

import json
import os
from typing import Any, Dict, Mapping, Optional, Sequence

from logging_setup import get_logger
from utils_pkg import ai_db
from utils_pkg import feature_builder

log = get_logger(__name__)

__all__ = [
    # core candidate logging
    "log_entry_candidate",
    "log_buy_candidate",
    "log_skip_candidate",
    "log_entry_decision_update",
    "log_entry_outcome",
    # per-source convenience wrappers
    "log_raydium_candidate",
    "log_pumpfun_candidate",
    "log_dexscreener_candidate",
    "log_momo_candidate",
    # optional advanced helpers (no-op if ai_db doesn’t support them)
    "log_trade_execution",
    "log_regime_snapshot",
    "log_ai_event",
    "log_equity_curve_point",
    "log_risk_manager_snapshot",
    "log_reinvestor_snapshot",
]

# Global, shared FeatureBuilder instance (quant-grade, V3+++)
try:
    _FB = feature_builder.FeatureBuilder()
except Exception:  # pragma: no cover - defensive
    log.exception("data_logger: failed to initialize FeatureBuilder()")
    _FB = None  # type: ignore


# ============================================================================ #
# Env helpers / global flags
# ============================================================================ #


def _env_bool(key: str, default: bool = False) -> bool:
    v = os.getenv(key)
    if v is None:
        return default
    return str(v).strip().lower() in ("1", "true", "yes", "y", "on")


def _env_int(key: str, default: int) -> int:
    try:
        return int(os.getenv(key, str(default)))
    except Exception:
        return default


def _env_float(key: str, default: float) -> float:
    try:
        return float(os.getenv(key, str(default)))
    except Exception:
        return default


# Postgres master toggle (mirrors/global PG_ENABLED)
PG_ENABLED = _env_bool("PG_ENABLED", True)

# Base feature version (bump when you change FeatureBuilder semantics)
# This is a *logging schema*/feature-vector version label.
FEATURE_VERSION = _env_int("AI_FEATURE_VERSION", 3)

# Optional per-src / per-regime overrides via JSON:
#   AI_FEATURE_VERSION_MAP='{"raydium_live": 3, "pumpfun_live": 3, "src:pumpfun": 3}'
_feature_version_map_raw = os.getenv("AI_FEATURE_VERSION_MAP", "")
_FEATURE_VERSION_MAP: Dict[str, int] = {}
if _feature_version_map_raw.strip():
    try:
        blob = json.loads(_feature_version_map_raw)
        if isinstance(blob, dict):
            for k, v in blob.items():
                try:
                    _FEATURE_VERSION_MAP[str(k)] = int(v)
                except Exception:
                    continue
    except Exception:
        log.warning("AI_FEATURE_VERSION_MAP is not valid JSON – ignoring")

# Global switches so you can turn logging on/off without code changes.
AI_LOG_CANDIDATES = _env_bool("AI_LOG_CANDIDATES", True)
AI_LOG_OUTCOMES = _env_bool("AI_LOG_OUTCOMES", True)

# Strict mode: if False, we swallow DB errors (after logging) and return -1.
AI_LOG_STRICT = _env_bool("AI_LOG_STRICT", True)

# Optional: only log certain sources / regimes if non-empty (comma-separated)
AI_LOG_SRC_FILTER = os.getenv("AI_LOG_SRC_FILTER", "").strip()
AI_LOG_REGIME_FILTER = os.getenv("AI_LOG_REGIME_FILTER", "").strip()

_SRC_ALLOW: Optional[set[str]] = None
_REGIME_ALLOW: Optional[set[str]] = None
if AI_LOG_SRC_FILTER:
    _SRC_ALLOW = {s.strip() for s in AI_LOG_SRC_FILTER.split(",") if s.strip()}
if AI_LOG_REGIME_FILTER:
    _REGIME_ALLOW = {s.strip() for s in AI_LOG_REGIME_FILTER.split(",") if s.strip()}

# Optional: sampling to avoid flooding DB on hyper-busy days
# 1.0 = log everything, 0.1 = log ~10% of candidates.
AI_LOG_SAMPLE_RATE = _env_float("AI_LOG_SAMPLE_RATE", 1.0)

# Optional: separate sampling for SKIPs (often the largest firehose).
# Defaults to AI_LOG_SAMPLE_RATE if not explicitly set.
AI_LOG_SAMPLE_RATE_SKIPS = _env_float("AI_LOG_SAMPLE_RATE_SKIPS", AI_LOG_SAMPLE_RATE)

# Optional: if false, SKIP decisions are *not* logged via log_skip_candidate.
AI_LOG_SKIPS = _env_bool("AI_LOG_SKIPS", True)

# Optional policy/version labels (for later ML: which policy produced this decision?)
AI_POLICY_VERSION = os.getenv("AI_POLICY_VERSION", "").strip()  # e.g. "entry_v3.1"

# Optional experimental mode label (for A/B testing etc.)
AI_EXPERIMENT_TAG = os.getenv("AI_EXPERIMENT_TAG", "").strip()  # e.g. "exp_regime_v2"

# Optional: mark which entry model was used (fine-tune name etc.)
AI_ENTRY_MODEL_NAME = (
    os.getenv("ENTRY_MODEL_NAME", "").strip()
    or os.getenv("ENTRY_MODEL", "").strip()
)

# Optional: code / deploy metadata for offline analysis (harmless if missing)
AI_CODE_VERSION = os.getenv("AI_CODE_VERSION", "").strip()  # e.g. git SHA
AI_STACK_PROFILE = os.getenv("AI_STACK_PROFILE", "").strip()  # e.g. "render_prod"

# Outcome-labeling strategy version (so we can change buckets later).
AI_LABELING_VERSION = os.getenv("AI_LABELING_VERSION", "").strip()  # e.g. "v1"

# Optional: log raw history sequence into `raw` payload for deeper offline work.
AI_LOG_HISTORY_IN_RAW = _env_bool("AI_LOG_HISTORY_IN_RAW", False)
AI_LOG_HISTORY_MAX_LEN = _env_int("AI_LOG_HISTORY_MAX_LEN", 120)  # candles/events

# Optional: capture full feature vector into raw payload for easier offline export.
AI_LOG_FEATURES_IN_RAW = _env_bool("AI_LOG_FEATURES_IN_RAW", False)

# Optional: log full ai_buy_scoring / gate detail into `raw` payload when present.
AI_LOG_GATE_DETAIL = _env_bool("AI_LOG_GATE_DETAIL", True)

# Optional: emit a very small, one-line JSON to stdout for each candidate (debug)
AI_LOG_STDOUT_BRIEF = _env_bool("AI_LOG_STDOUT_BRIEF", False)

# Optional: execution + regime snapshot logging (if ai_db exposes hooks)
AI_LOG_EXECUTIONS = _env_bool("AI_LOG_EXECUTIONS", True)
AI_LOG_REGIME_SNAPSHOTS = _env_bool("AI_LOG_REGIME_SNAPSHOTS", False)

# Optional: per-channel sampling for heavy streams
AI_LOG_EXEC_SAMPLE_RATE = _env_float("AI_LOG_EXEC_SAMPLE_RATE", 1.0)
AI_LOG_OUTCOME_SAMPLE_RATE = _env_float("AI_LOG_OUTCOME_SAMPLE_RATE", 1.0)
AI_LOG_EVENT_SAMPLE_RATE = _env_float("AI_LOG_EVENT_SAMPLE_RATE", 1.0)

# Optional: hard filters at logger-level to avoid logging pure dust.
AI_LOG_MIN_LIQ_USD = _env_float("AI_LOG_MIN_LIQ_USD", 0.0)
AI_LOG_MIN_VOL5_USD = _env_float("AI_LOG_MIN_VOL5_USD", 0.0)

# Optional: auto schema init on first DB touch (Phase 0 bootstrap).
AI_DB_AUTO_INIT = _env_bool("AI_DB_AUTO_INIT", True)

# Optional: equity / risk / reinvestor specific toggles (Phase 3/4/5 helpers)
AI_LOG_EQUITY = _env_bool("AI_LOG_EQUITY", True)
AI_LOG_RISK_SNAPSHOTS = _env_bool("AI_LOG_RISK_SNAPSHOTS", True)
AI_LOG_REINVESTOR_SNAPSHOTS = _env_bool("AI_LOG_REINVESTOR_SNAPSHOTS", True)

AI_LOG_EQUITY_SAMPLE_RATE = _env_float("AI_LOG_EQUITY_SAMPLE_RATE", 1.0)
AI_LOG_RISK_SAMPLE_RATE = _env_float("AI_LOG_RISK_SAMPLE_RATE", 1.0)
AI_LOG_REINVESTOR_SAMPLE_RATE = _env_float("AI_LOG_REINVESTOR_SAMPLE_RATE", 1.0)


# ============================================================================ #
# Internal helpers
# ============================================================================ #

_SCHEMA_INIT_DONE = False


def _ensure_ai_schema_once() -> None:
    """
    Best-effort call into ai_db.ensure_ai_tables() exactly once per process.

    This keeps app code clean: data_logger sits on the hot path and guarantees
    the AI tables exist as soon as we try to write anything, *if* the ai_db
    module exposes ensure_ai_tables().
    """
    global _SCHEMA_INIT_DONE

    if _SCHEMA_INIT_DONE or not PG_ENABLED or not AI_DB_AUTO_INIT:
        return

    fn = getattr(ai_db, "ensure_ai_tables", None)
    if not callable(fn):
        _SCHEMA_INIT_DONE = True
        return

    try:
        fn()
        log.debug("data_logger: ensure_ai_tables() completed")
    except Exception:
        log.exception("data_logger: ensure_ai_tables() failed during auto-init")
        if AI_LOG_STRICT:
            # Fail loudly in strict mode so we notice schema drift.
            raise
    finally:
        _SCHEMA_INIT_DONE = True


def _src_regime_allowed(src: str, regime: Optional[str]) -> bool:
    if _SRC_ALLOW is not None and src not in _SRC_ALLOW:
        return False
    if _REGIME_ALLOW is not None and regime is not None and regime not in _REGIME_ALLOW:
        return False
    return True


def _feature_version_for(src: str, regime: Optional[str]) -> int:
    """
    Choose a feature version based on regime / src overrides.

    Priority:
      1) exact regime match
      2) "src:<src>" match
      3) global FEATURE_VERSION
    """
    if regime and regime in _FEATURE_VERSION_MAP:
        return _FEATURE_VERSION_MAP[regime]
    key = f"src:{src}"
    if key in _FEATURE_VERSION_MAP:
        return _FEATURE_VERSION_MAP[key]
    return FEATURE_VERSION


def _stable_hash(s: str) -> float:
    """
    Deterministic hash → [0, 1) float.

    Used for sampling so that repeated runs keep/skip the same rows.
    """
    try:
        h = 0
        for ch in s:
            h = (h * 31 + ord(ch)) & 0xFFFFFFFF
        return (h % 10_000) / 10_000.0
    except Exception:
        # Fallback: always keep if we somehow fail
        return 0.0


def _should_sample(key: str, *, rate: float) -> bool:
    """
    Deterministic sampling based on key, so we always keep a stable slice
    of events when rate < 1.0.
    """
    if rate >= 0.999:
        return True
    if rate <= 0.0:
        return False
    return _stable_hash(key) < rate


def _normalize_rug(raw: Any) -> float:
    """
    Normalize rug/fraud score into 0..1 regardless of whether it's
    already 0..1 or 0..100.
    """
    try:
        v = float(raw)
    except Exception:
        return 0.0

    # If it's clearly 0..100, rescale
    if v > 1.5:
        v = max(0.0, min(1.0, v / 100.0))

    if v < 0.0:
        return 0.0
    if v > 1.0:
        return 1.0
    return v


# ============================================================================ #
# Snapshot pre-normalization (scanner meta → generic PF-style snapshot)
# ============================================================================ #


def _build_pf_snapshot_from_meta(meta: Mapping[str, Any]) -> Dict[str, Any]:
    """
    Generic snapshot builder for PF-style meta dicts (Raydium / Pump.fun /
    Dexscreener / MOMO).

    We pull stable numeric fields and attach the entire meta under "raw_meta" so
    future feature versions can do more offline work.

    IMPORTANT: this is just a *pre-normalization* snapshot; the real feature
    engineering happens in FeatureBuilder.build().
    """
    m = meta

    def f(key: str, default: float = 0.0) -> float:
        try:
            v = m.get(key, default)
            return float(v)
        except Exception:
            return float(default)

    def i(key: str, default: int = 0) -> int:
        try:
            v = m.get(key, default)
            return int(v)
        except Exception:
            return int(default)

    def f_any(keys: Sequence[str], default: float = 0.0) -> float:
        for k in keys:
            if k in m:
                return f(k, default)
        return float(default)

    def i_any(keys: Sequence[str], default: int = 0) -> int:
        for k in keys:
            if k in m:
                return i(k, default)
        return int(default)

    # Core size / age
    liq_usd = f_any(["liq_usd", "liq", "liqUsd", "liquidity_usd"])
    mcap_usd = f_any(["mcap_usd", "mcap", "market_cap_usd"])
    fdv_usd = f_any(["fdv_usd", "fdv", "fully_diluted_valuation_usd"])
    token_age_min = f_any(["token_age_min", "age_min", "age_minutes", "ageMinutes"])

    # Volumes
    vol_1m_usd = f_any(["vol_1m_usd", "volume_1m_usd"])
    vol_5m_usd = f_any(["vol_5m_usd", "vol5", "volume_5m_usd"])
    vol_15m_usd = f_any(["vol_15m_usd", "volume_15m_usd"])
    vol_1h_usd = f_any(["vol_1h_usd", "vol1h", "volume_1h_usd"])
    vol_24h_usd = f_any(["vol_24h_usd", "vol24h", "volume_24h_usd"])

    # Trades
    buys_1m = i_any(["buys_1m"])
    sells_1m = i_any(["sells_1m"])
    buys_5m = i_any(["buys_5m", "buys5"])
    sells_5m = i_any(["sells_5m", "sells5"])
    txns_5m = i_any(["txns_5m", "txns5"], default=buys_5m + sells_5m)

    # Price / change
    price_usd = f_any(["price_usd", "px_usd", "priceUsd", "price", "p"])
    price_change_1m_pct = f_any(["price_change_1m_pct", "chg1", "change_1m_pct"])
    price_change_5m_pct = f_any(["price_change_5m_pct", "chg5", "change_5m_pct"])
    price_change_15m_pct = f_any(["price_change_15m_pct", "chg15", "change_15m_pct"])
    price_change_1h_pct = f_any(["price_change_1h_pct", "chg1h", "change_1h_pct"])

    # Holders / whales / smart wallets
    holders = i_any(["holders", "holders_total"])
    holders_5m_change = i_any(["holders_5m_change", "holders_delta_5m"])
    whales_5m = i_any(["whales_5m", "whale_buys_5m"])
    whale_vol_5m_usd = f_any(["whale_vol_5m_usd", "whale_volume_5m_usd"])
    largest_buy_5m_sol = f_any(["largest_buy_5m_sol", "largest_buy_5m"])

    whale_share_5m = f_any(["whale_share_5m"])
    whale_share_1h = f_any(["whale_share_1h"])
    smart_wallet_score = f_any(["smart_wallet_score"])
    smart_flow_raw = f_any(["smart_flow_raw"])
    smart_wallet_count_5m = f_any(["smart_wallet_count_5m"])
    smart_buy_score_5m = f_any(["smart_buy_score_5m"])
    smart_sell_score_5m = f_any(["smart_sell_score_5m"])

    # Smart wallet flag (some feeds give a simple bool-ish indicator)
    smart_wallet_flag = m.get("smart_wallet_flag")

    # V3+++ smart/whale extras if scanners already provide them
    smart_net_side_5m = f_any(["smart_net_side_5m"])
    whale_net_side_5m = f_any(["whale_net_side_5m"])
    smart_alpha_bias = f_any(["smart_alpha_bias"])
    smart_whale_alignment = f_any(["smart_whale_alignment"])
    smart_whale_alignment_abs = f_any(["smart_whale_alignment_abs"])
    whale_domination_5m = f_any(["whale_domination_5m"])
    smart_vs_whale_conflict = f_any(["smart_vs_whale_conflict"])
    smart_accumulation_score = f_any(["smart_accumulation_score"])
    smart_panic_dump_score = f_any(["smart_panic_dump_score"])
    whale_liquidity_footprint = f_any(["whale_liquidity_footprint"])

    # Structural / safety (for ai_buy_scoring alignment)
    top10_pct = f_any(["top10_pct", "holder_top10_pct", "top10"], 100.0)
    dev_wallet_pct = f_any(["dev_wallet_pct"], 0.0)
    rugpull_reports = i_any(["rugpull_reports"], 0)
    honeypot_flag = bool(
        m.get("honeypot_flag") or m.get("rug_risk") or m.get("is_honeypot") or False
    )
    liq_locked = m.get("liq_locked")
    mint_renounced = m.get("mint_renounced")
    freeze_auth_disabled = m.get("freeze_auth_disabled")

    # Orderbook microstructure if scanners provide it
    orderbook_bid_usd = f_any(["orderbook_bid_usd", "bid_usd"])
    orderbook_ask_usd = f_any(["orderbook_ask_usd", "ask_usd"])

    # Risk / impact / rug
    impact_1k_bps = f_any(["impact_1k_bps", "impact_1000usd_bps"])
    ai_rug_score = f_any(["ai_rug_score", "ai_rug_score_01", "fraud_score"], 0.0)

    # PF scores (from pair_filters / scanners)
    pf_momentum_score = f_any(["pf_momentum_score", "momentum_score"], 0.0)
    pf_final_score = f_any(["pf_final_score", "final_score"], 0.0)
    pf_risk_adjusted_final = f_any(["pf_risk_adjusted_final"], 0.0)
    if pf_risk_adjusted_final == 0.0 and pf_final_score > 0.0:
        try:
            rug_norm = _normalize_rug(ai_rug_score)
            pf_risk_adjusted_final = max(0.0, pf_final_score * (1.0 - rug_norm))
        except Exception:
            pf_risk_adjusted_final = pf_final_score

    # Regime coarse score if scanners already provide one
    regime_score = f_any(["regime_score", "regime_numeric"], 0.0)

    # Scanner hints (if scanners set them)
    scanner_momentum_ok = f_any(["scanner_momentum_ok"], 0.0)
    scanner_source_id = f_any(["scanner_source_id"], 0.0)

    # More detailed regime if present (used by FeatureBuilder / ai_entry_logic)
    regime_trend = f_any(["regime_trend"], regime_score)
    regime_volatility = f_any(["regime_volatility"], 0.0)
    regime_liquidity = f_any(["regime_liquidity"], 0.5)
    regime_manipulation_risk = f_any(["regime_manipulation_risk"], 0.0)

    # Pump.fun / bonding-curve extras (if scanners already pass them)
    pf_curve_progress = f_any(["pf_curve_progress", "pf_curve_progress_pct"], 0.0)
    pf_minted_pct = f_any(["pf_minted_pct"], 0.0)
    pf_creator_hold_pct = f_any(["pf_creator_hold_pct"], 0.0)
    pf_bot_score = f_any(["pf_bot_score"], 0.0)
    pf_sniper_density = f_any(["pf_sniper_density"], 0.0)

    # Scanner meta for FeatureBuilder segment / rank routing
    scanner_rank = i_any(["scanner_rank"], 0)
    scanner_total = i_any(["scanner_total"], 0)
    segment = m.get("segment")  # e.g. "pumpfun", "raydium", "momo"

    snap: Dict[str, Any] = {
        "liq_usd": liq_usd,
        "mcap_usd": mcap_usd,
        "fdv_usd": fdv_usd,
        "token_age_min": token_age_min,
        "vol_1m_usd": vol_1m_usd,
        "vol_5m_usd": vol_5m_usd,
        "vol_15m_usd": vol_15m_usd,
        "vol_1h_usd": vol_1h_usd,
        "vol_24h_usd": vol_24h_usd,
        "buys_1m": buys_1m,
        "sells_1m": sells_1m,
        "buys_5m": buys_5m,
        "sells_5m": sells_5m,
        "txns_5m": txns_5m,
        "price_usd": price_usd,
        "price_change_1m_pct": price_change_1m_pct,
        "price_change_5m_pct": price_change_5m_pct,
        "price_change_15m_pct": price_change_15m_pct,
        "price_change_1h_pct": price_change_1h_pct,
        "holders": holders,
        "holders_5m_change": holders_5m_change,
        "whales_5m": whales_5m,
        "whale_vol_5m_usd": whale_vol_5m_usd,
        "largest_buy_5m_sol": largest_buy_5m_sol,
        "whale_share_5m": whale_share_5m,
        "whale_share_1h": whale_share_1h,
        "smart_wallet_score": smart_wallet_score,
        "smart_flow_raw": smart_flow_raw,
        "smart_wallet_count_5m": smart_wallet_count_5m,
        "smart_buy_score_5m": smart_buy_score_5m,
        "smart_sell_score_5m": smart_sell_score_5m,
        "smart_wallet_flag": smart_wallet_flag,
        "impact_1k_bps": impact_1k_bps,
        "ai_rug_score": ai_rug_score,
        "regime_score": regime_score,
        "regime_trend": regime_trend,
        "regime_volatility": regime_volatility,
        "regime_liquidity": regime_liquidity,
        "regime_manipulation_risk": regime_manipulation_risk,
        "pf_momentum_score": pf_momentum_score,
        "pf_final_score": pf_final_score,
        "pf_risk_adjusted_final": pf_risk_adjusted_final,
        "scanner_momentum_ok": scanner_momentum_ok,
        "scanner_source_id": scanner_source_id,
        # V3+++ wallet/flow extras (if present)
        "smart_net_side_5m": smart_net_side_5m,
        "whale_net_side_5m": whale_net_side_5m,
        "smart_alpha_bias": smart_alpha_bias,
        "smart_whale_alignment": smart_whale_alignment,
        "smart_whale_alignment_abs": smart_whale_alignment_abs,
        "whale_domination_5m": whale_domination_5m,
        "smart_vs_whale_conflict": smart_vs_whale_conflict,
        "smart_accumulation_score": smart_accumulation_score,
        "smart_panic_dump_score": smart_panic_dump_score,
        "whale_liquidity_footprint": whale_liquidity_footprint,
        # Structural / safety extras
        "top10_pct": top10_pct,
        "dev_wallet_pct": dev_wallet_pct,
        "rugpull_reports": rugpull_reports,
        "honeypot_flag": honeypot_flag,
        "liq_locked": liq_locked,
        "mint_renounced": mint_renounced,
        "freeze_auth_disabled": freeze_auth_disabled,
        # Orderbook
        "orderbook_bid_usd": orderbook_bid_usd,
        "orderbook_ask_usd": orderbook_ask_usd,
        # Pump.fun extras
        "pf_curve_progress": pf_curve_progress,
        "pf_minted_pct": pf_minted_pct,
        "pf_creator_hold_pct": pf_creator_hold_pct,
        "pf_bot_score": pf_bot_score,
        "pf_sniper_density": pf_sniper_density,
        # Scanner meta for routing / stats
        "scanner_rank": scanner_rank,
        "scanner_total": scanner_total,
        "segment": segment,
    }

    # Full meta for offline feature engineering
    snap["raw_meta"] = dict(meta)
    # Keep filter debug if present (for PF scores / AI rug)
    if "_filter_debug" in meta:
        snap["_filter_debug"] = meta["_filter_debug"]

    return snap


# ============================================================================ #
# Outcome label helper
# ============================================================================ #


def _safe_label_from_pnl(pnl_pct: float, got_stopped: bool) -> str:
    """
    Turn pnl + stop flag into a coarse label for training.
    """
    try:
        p = float(pnl_pct)
    except Exception:
        p = 0.0

    if got_stopped:
        return "stopped_loss" if p <= 0 else "stopped_win"
    if p >= 40:
        return "mega_win"
    if p >= 20:
        return "monster_win"
    if p >= 10:
        return "big_win"
    if p >= 0:
        return "win"
    if p <= -35:
        return "disaster"
    if p <= -20:
        return "big_loss"
    return "loss"


# ============================================================================ #
# Core candidate logger (generic)
# ============================================================================ #


def log_entry_candidate(
    *,
    src: str,
    mint: str,
    symbol: Optional[str],
    snapshot: Mapping[str, Any],
    regime: Optional[str] = None,
    wallet_tag: Optional[str] = None,
    # Advanced context hooks (optional, can be None):
    regime_state: Optional[Any] = None,
    wallet_signals: Optional[Mapping[str, Any]] = None,
    ai_signals: Optional[Mapping[str, Any]] = None,
    scanner_source: Optional[str] = None,
    history: Optional[Sequence[Mapping[str, Any]]] = None,
    # Broader risk / portfolio context (from risk_manager / strategy):
    position_context: Optional[Mapping[str, Any]] = None,
    # Decision metadata (only known if strategy already decided):
    decision: Optional[str] = None,          # e.g. "BUY", "SKIP", "TEST_BUY"
    decision_score: Optional[float] = None,  # final entry score used
    decision_reason: Optional[str] = None,   # short text
    # Self-learning / RL context (optional):
    episode_id: Optional[str] = None,
    step_idx: Optional[int] = None,
    strategy_profile: Optional[str] = None,  # e.g. "aggressive_v1"
    # Optional linking fields for outcomes / trades:
    trade_id: Optional[str] = None,
    session_id: Optional[str] = None,
    candidate_key: Optional[str] = None,     # e.g. f"{src}:{mint}:{ts_bucket}"
    # Optional AI gate detail (from ai_buy_scoring.gate_feature_vector / score_only)
    gate_detail: Optional[Mapping[str, Any]] = None,
    # Optional timestamp (seconds since epoch or ISO string).
    ts: Optional[Any] = None,
    # Raw extras (debug / extended context):
    extra_raw: Optional[Mapping[str, Any]] = None,
    # Advanced override: force a specific feature_version if needed.
    feature_version_override: Optional[int] = None,
) -> int:
    """
    High-level helper: take a raw scanner snapshot, build features, write to DB.

    Returns:
      int: row id, or 0 if disabled, or -1 if failed in non-strict mode.
    """
    if not PG_ENABLED:
        log.debug(
            "PG_ENABLED=0 – skipping candidate log for src=%s mint=%s",
            src,
            mint,
        )
        return 0

    if not AI_LOG_CANDIDATES:
        log.debug(
            "AI_LOG_CANDIDATES=0 – skipping candidate log for src=%s mint=%s",
            src,
            mint,
        )
        return 0

    if not _src_regime_allowed(src, regime):
        log.debug(
            "data_logger: src/regime filtered out (src=%s regime=%s) – skip",
            src,
            regime,
        )
        return 0

    # Ensure schema once before first write (Phase 0 bootstrap).
    _ensure_ai_schema_once()

    # Optional deterministic sampling (BUY vs SKIP can be downsampled separately)
    cand_hash = candidate_key or f"{src}:{regime or ''}:{mint}"

    # Hard filters on micro liquidity / micro volume if configured
    try:
        liq_usd = float(
            snapshot.get("liq_usd")
            or snapshot.get("liq")
            or snapshot.get("liqUsd")
            or 0.0
        )
    except Exception:
        liq_usd = 0.0

    try:
        vol5_usd = float(
            snapshot.get("vol_5m_usd")
            or snapshot.get("vol5")
            or snapshot.get("volume_5m_usd")
            or 0.0
        )
    except Exception:
        vol5_usd = 0.0

    if AI_LOG_MIN_LIQ_USD > 0.0 and liq_usd < AI_LOG_MIN_LIQ_USD:
        log.debug(
            "data_logger: liq_usd=%.2f < AI_LOG_MIN_LIQ_USD=%.2f – skip src=%s mint=%s",
            liq_usd,
            AI_LOG_MIN_LIQ_USD,
            src,
            mint,
        )
        return 0

    if AI_LOG_MIN_VOL5_USD > 0.0 and vol5_usd < AI_LOG_MIN_VOL5_USD:
        log.debug(
            "data_logger: vol_5m_usd=%.2f < AI_LOG_MIN_VOL5_USD=%.2f – skip src=%s mint=%s",
            vol5_usd,
            AI_LOG_MIN_VOL5_USD,
            src,
            mint,
        )
        return 0

    # Sampling: SKIPs can be downsampled more aggressively than BUYs.
    if str(decision or "").upper() == "SKIP":
        if not _should_sample(cand_hash, rate=AI_LOG_SAMPLE_RATE_SKIPS):
            log.debug("data_logger: sampled-out SKIP candidate src=%s mint=%s", src, mint)
            return 0
    else:
        if not _should_sample(cand_hash, rate=AI_LOG_SAMPLE_RATE):
            log.debug("data_logger: sampled-out candidate src=%s mint=%s", src, mint)
            return 0

    if _FB is None:
        log.warning("FeatureBuilder not available – skipping candidate log")
        return 0

    try:
        # ------------------------------------------------------------------ #
        # 1) Build normalized feature dict from the raw snapshot (+ context)
        # ------------------------------------------------------------------ #
        feats = _FB.build(
            symbol=symbol,
            mint=mint,
            snapshot=snapshot,
            history=history or None,
            regime=regime_state,
            wallet_signals=wallet_signals,
            ai_signals=ai_signals,
            scanner_source=scanner_source or src,
            risk_context=position_context,
        )

        # ------------------------------------------------------------------ #
        # 2) PF scores & AI rug / fraud risk from snapshot/meta (redundant)
        # ------------------------------------------------------------------ #
        pf_mom = snapshot.get("pf_momentum_score") or snapshot.get("momentum_score")
        pf_final = snapshot.get("pf_final_score") or snapshot.get("final_score")

        if "_filter_debug" in snapshot:
            dbg = snapshot.get("_filter_debug") or {}
            try:
                pf_mom = pf_mom or dbg.get("momentum_score")
                pf_final = pf_final or dbg.get("final_score")
                ai_rug_dbg = dbg.get("ai_rug_score")
                if ai_rug_dbg is not None and "ai_fraud_score" not in feats:
                    feats["ai_fraud_score"] = _normalize_rug(ai_rug_dbg)
            except Exception:
                pass

        # Normalize rug risk from snapshot if present
        rug01 = 0.0
        try:
            ai_rug = snapshot.get(
                "ai_rug_score",
                snapshot.get("ai_rug_score_01", feats.get("ai_fraud_score", 0.0)),
            )
            rug01 = _normalize_rug(ai_rug)
        except Exception:
            rug01 = 0.0

        feats.setdefault("ai_fraud_score", rug01)

        if pf_mom is not None:
            try:
                feats["pf_momentum_score"] = float(pf_mom)
            except Exception:
                pass
        if pf_final is not None:
            try:
                pf_final_f = float(pf_final)
                feats["pf_final_score"] = pf_final_f
                feats.setdefault(
                    "pf_risk_adjusted_final",
                    max(0.0, pf_final_f * (1.0 - rug01)),
                )
            except Exception:
                pass

        # Smart-wallet flag from snapshot (if not already encoded by FeatureBuilder)
        if "smart_wallet_flag" in snapshot and "smart_wallet_flag" not in feats:
            sw_flag = snapshot.get("smart_wallet_flag")
            feats["smart_wallet_flag"] = 1.0 if bool(sw_flag) else 0.0

        # ------------------------------------------------------------------ #
        # 3) Decision metadata & AI meta-features
        # ------------------------------------------------------------------ #
        if decision_score is not None:
            try:
                feats["decision_score"] = float(decision_score)
            except Exception:
                pass

        if ai_signals is not None:
            # Prefer explicit entry score / prob from ai_signals if present
            if "entry_score" in ai_signals:
                try:
                    feats["ai_entry_score"] = float(ai_signals["entry_score"])
                except Exception:
                    pass
            if "ai_entry_score" in ai_signals:
                try:
                    feats["ai_entry_score"] = float(ai_signals["ai_entry_score"])
                except Exception:
                    pass
            if "entry_prob" in ai_signals:
                try:
                    feats["ai_entry_prob"] = float(ai_signals["entry_prob"])
                except Exception:
                    pass
            if "value_estimate" in ai_signals:
                try:
                    feats["ai_value_estimate"] = float(ai_signals["value_estimate"])
                except Exception:
                    pass
            # Generic AI tags, e.g. ["risk_off", "high_confidence"]
            if "tags" in ai_signals and isinstance(ai_signals["tags"], (list, tuple)):
                feats["ai_tags_count"] = float(len(ai_signals["tags"]))

        # If no explicit AI entry score but we have a decision_score, reuse it
        if "ai_entry_score" not in feats and decision_score is not None:
            feats["ai_entry_score"] = float(decision_score)

        # Policy / experiment markers (numeric flags; interpreted downstream)
        if AI_POLICY_VERSION:
            feats["ai_policy_version_id"] = 1.0
        if AI_EXPERIMENT_TAG:
            feats["ai_experiment_flag"] = 1.0
        if AI_ENTRY_MODEL_NAME:
            feats["ai_entry_model_used"] = 1.0
        if AI_CODE_VERSION:
            feats["ai_code_version_marker"] = 1.0
        if AI_STACK_PROFILE:
            feats["ai_stack_profile_marker"] = 1.0

        # Decision flags (BUY / SKIP / TEST_BUY / OTHER)
        if decision is not None:
            d = str(decision).upper()
            feats["decision_flag_buy"] = 1.0 if d == "BUY" else 0.0
            feats["decision_flag_skip"] = 1.0 if d == "SKIP" else 0.0
            feats["decision_flag_test"] = 1.0 if d == "TEST_BUY" else 0.0
            feats["decision_flag_other"] = (
                1.0 if d not in ("BUY", "SKIP", "TEST_BUY") else 0.0
            )

        # ------------------------------------------------------------------ #
        # 4) Risk / portfolio context features (from risk_manager / strategy)
        # ------------------------------------------------------------------ #
        if position_context:
            ctx = position_context

            def _ctx_f(k: str, default: float = 0.0) -> float:
                try:
                    v = ctx.get(k, default)
                    return float(v)
                except Exception:
                    return float(default)

            # Mirror what FeatureBuilder may already set, but ensure ctx_* for AI
            feats.setdefault("ctx_open_positions", _ctx_f("open_positions", 0.0))
            feats.setdefault("ctx_max_positions", _ctx_f("max_positions", 0.0))
            feats.setdefault(
                "ctx_daily_realized_pnl_pct",
                _ctx_f("daily_realized_pnl_pct", 0.0),
            )
            feats.setdefault(
                "ctx_daily_drawdown_pct",
                _ctx_f("daily_drawdown_pct", 0.0),
            )
            feats.setdefault("ctx_risk_mode", _ctx_f("risk_mode", 0.0))

            # Optional capacity estimate (0..1) from risk_manager
            feats.setdefault(
                "risk_capacity_score",
                _ctx_f("risk_capacity_score", feats.get("risk_capacity_score", 1.0)),
            )

            # Bubble session_id through if present and not explicitly passed
            if "session_id" in ctx and session_id is None:
                session_id = str(ctx["session_id"])

        # ------------------------------------------------------------------ #
        # 5) Self-learning / RL context
        # ------------------------------------------------------------------ #
        if episode_id is not None:
            feats.setdefault("rl_episode_marker", 1.0)
        if step_idx is not None:
            try:
                feats["rl_step_idx"] = float(step_idx)
            except Exception:
                pass
        if strategy_profile is not None:
            feats.setdefault("strategy_profile_marker", 1.0)

        # ------------------------------------------------------------------ #
        # 6) AI gate detail integration (ai_buy_scoring / ai_entry_logic)
        # ------------------------------------------------------------------ #
        if gate_detail is not None:
            try:
                # Non-destructive: only fill if not already set by FeatureBuilder.
                if "ai_buy_score" not in feats and "score" in gate_detail:
                    feats["ai_buy_score"] = float(gate_detail["score"])
                if "ai_buy_ok_hard" not in feats and "ok_hard" in gate_detail:
                    feats["ai_buy_ok_hard"] = 1.0 if gate_detail["ok_hard"] else 0.0
                if "ai_buy_size_scalar" not in feats and "size_scalar" in gate_detail:
                    feats["ai_buy_size_scalar"] = float(gate_detail["size_scalar"])
                if (
                    "ai_buy_gate_threshold" not in feats
                    and "gate_threshold" in gate_detail
                ):
                    feats["ai_buy_gate_threshold"] = float(
                        gate_detail["gate_threshold"]
                    )
            except Exception:
                # Never break logging because of malformed gate detail.
                log.debug("data_logger: failed to fold gate_detail into features")

        # ------------------------------------------------------------------ #
        # 7) Feature version & linking identifiers
        # ------------------------------------------------------------------ #
        fv = feature_version_override or _feature_version_for(src, regime)

        if trade_id is not None:
            # Numeric marker so model knows row has a trade link
            feats.setdefault("trade_id_hash", 1.0)
        if session_id is not None:
            feats.setdefault("session_marker", 1.0)
        if candidate_key is not None:
            feats.setdefault("candidate_key_marker", 1.0)

        # ------------------------------------------------------------------ #
        # 8) Raw payload for debugging / offline work
        # ------------------------------------------------------------------ #
        raw_payload: Dict[str, Any] = dict(snapshot)
        if extra_raw:
            raw_payload.update(dict(extra_raw))

        decision_meta = {
            "decision": decision,
            "decision_score": decision_score,
            "decision_reason": decision_reason,
            "wallet_tag": wallet_tag,
            "regime_label": regime,
            "ai_policy_version": AI_POLICY_VERSION
            or snapshot.get("_ai_policy_version"),
            "ai_experiment_tag": AI_EXPERIMENT_TAG,
            "ai_entry_model_name": AI_ENTRY_MODEL_NAME,
            "ai_code_version": AI_CODE_VERSION,
            "ai_stack_profile": AI_STACK_PROFILE,
            "episode_id": episode_id,
            "step_idx": step_idx,
            "strategy_profile": strategy_profile,
        }
        raw_payload.setdefault("_decision_meta", decision_meta)
        raw_payload.setdefault("_ai_signals", dict(ai_signals or {}))
        raw_payload.setdefault("_wallet_signals", dict(wallet_signals or {}))
        if position_context:
            raw_payload.setdefault("_position_context", dict(position_context))
        if gate_detail is not None and AI_LOG_GATE_DETAIL:
            raw_payload.setdefault("_gate_detail", dict(gate_detail))
        if trade_id is not None:
            raw_payload.setdefault("_trade_id", trade_id)
        if session_id is not None:
            raw_payload.setdefault("_session_id", session_id)
        if candidate_key is not None:
            raw_payload.setdefault("_candidate_key", candidate_key)
        if ts is not None:
            raw_payload.setdefault("_ts", ts)
        if regime_state is not None:
            # allow downstream scripts to inspect the exact regime snapshot used
            raw_payload.setdefault("_regime_state", regime_state)

        # Optionally include short history slice for offline path-aware models
        if AI_LOG_HISTORY_IN_RAW and history:
            try:
                max_len = max(0, AI_LOG_HISTORY_MAX_LEN)
                if max_len > 0:
                    raw_payload.setdefault(
                        "_history",
                        list(history[-max_len:]),
                    )
            except Exception:
                log.debug("data_logger: failed to attach history to raw payload")

        # Optional: snapshot of the numeric feature vector for offline export
        if AI_LOG_FEATURES_IN_RAW:
            try:
                raw_payload.setdefault("_features_snapshot", dict(feats))
            except Exception:
                log.debug("data_logger: failed to attach features snapshot to raw")

        # ------------------------------------------------------------------ #
        # 9) Write to Postgres via ai_db helper
        # ------------------------------------------------------------------ #
        row_id = ai_db.insert_entry_candidate(
            src=src,
            mint=mint,
            symbol=symbol,
            regime=regime,
            wallet_tag=wallet_tag,
            feature_version=fv,
            features=feats,
            raw=raw_payload,
        )

        if AI_LOG_STDOUT_BRIEF:
            try:
                print(
                    json.dumps(
                        {
                            "event": "entry_candidate",
                            "id": row_id,
                            "src": src,
                            "mint": mint,
                            "symbol": symbol,
                            "regime": regime,
                            "fv": fv,
                            "decision": decision,
                            "pnl_ctx": position_context.get("daily_realized_pnl_pct")
                            if position_context
                            else None,
                        },
                        separators=(",", ":"),
                    )
                )
            except Exception:
                pass

        log.debug(
            "Logged entry_candidate id=%s src=%s mint=%s symbol=%s regime=%s "
            "wallet_tag=%s fv=%s decision=%s",
            row_id,
            src,
            mint,
            symbol,
            regime,
            wallet_tag,
            fv,
            decision,
        )
        return row_id
    except Exception:
        log.exception("Failed to log entry candidate for src=%s mint=%s", src, mint)
        if AI_LOG_STRICT:
            raise
        return -1


# Convenience alias when you explicitly know it was a BUY decision.
def log_buy_candidate(**kwargs: Any) -> int:
    """
    Wrapper that forces decision="BUY" for clarity in callers.
    """
    kwargs.setdefault("decision", "BUY")
    return log_entry_candidate(**kwargs)


# Convenience alias when you explicitly want to log a SKIP (for RL/AI training).
def log_skip_candidate(**kwargs: Any) -> int:
    """
    Wrapper that forces decision="SKIP". Obeys AI_LOG_SKIPS toggle.
    """
    if not AI_LOG_SKIPS:
        return 0
    kwargs.setdefault("decision", "SKIP")
    return log_entry_candidate(**kwargs)


# Optional: update decision metadata later if ai_db exposes a hook.
def log_entry_decision_update(
    *,
    candidate_id: int,
    decision: Optional[str] = None,
    decision_score: Optional[float] = None,
    decision_reason: Optional[str] = None,
    extra: Optional[Mapping[str, Any]] = None,
) -> int:
    """
    If ai_db.update_entry_decision(...) exists, use it to update decision
    fields after the fact. Otherwise this is a safe no-op returning 0.

    This is useful if you log candidates early (no decision yet) and only
    later decide BUY/SKIP after async checks.
    """
    if not PG_ENABLED:
        return 0

    _ensure_ai_schema_once()

    update_fn = getattr(ai_db, "update_entry_decision", None)
    if not callable(update_fn):
        # No DB support in current ai_db version; keep silent.
        return 0

    payload = {
        "decision": decision,
        "decision_score": float(decision_score) if decision_score is not None else None,
        "decision_reason": decision_reason,
    }
    if extra:
        payload.update(dict(extra))

    try:
        row_id = update_fn(candidate_id=candidate_id, **payload)
        log.debug(
            "Updated entry_candidate decision id=%s decision=%s score=%s",
            candidate_id,
            decision,
            decision_score,
        )
        return row_id
    except Exception:
        log.exception(
            "Failed to update entry decision for candidate_id=%s", candidate_id
        )
        if AI_LOG_STRICT:
            raise
        return -1


# ============================================================================ #
# Core outcome logger (generic)
# ============================================================================ #


def log_entry_outcome(
    *,
    candidate_id: int,
    pnl_pct: float,
    hold_seconds: float,
    got_stopped: bool,
    label: Optional[str] = None,
    notes: Optional[str] = None,
    extra: Optional[Mapping[str, Any]] = None,
) -> int:
    """
    Log the *result* of a candidate (e.g. trade outcome) into entry_outcomes.

    Typical call site: exit_engine / auto-sell logic when a position is closed.

    `extra` is the place to stuff quant-grade outcome details, e.g.:
      - "max_runup_pct", "max_dd_pct"
      - "exit_policy", "exit_trigger", "exit_engine_version"
      - "slippage_bps", "fees_pct", "final_r_multiple"
      - "reentry_count", "trail_hits", "reward_proxy", etc.
    """
    if not PG_ENABLED:
        log.debug(
            "PG_ENABLED=0 – skipping outcome log for candidate_id=%s",
            candidate_id,
        )
        return 0

    if not AI_LOG_OUTCOMES:
        log.debug(
            "AI_LOG_OUTCOMES=0 – skipping outcome log for candidate_id=%s",
            candidate_id,
        )
        return 0

    if candidate_id <= 0:
        # Nothing to link to; avoid noisy errors in prod.
        log.debug(
            "log_entry_outcome called with non-positive candidate_id=%s – skip",
            candidate_id,
        )
        return 0

    # Optional sampling for ultra-high-frequency backtests
    if not _should_sample(f"outcome:{candidate_id}", rate=AI_LOG_OUTCOME_SAMPLE_RATE):
        log.debug(
            "data_logger: sampled-out outcome for candidate_id=%s", candidate_id
        )
        return 0

    _ensure_ai_schema_once()

    try:
        effective_label = label or _safe_label_from_pnl(pnl_pct, got_stopped)
        merged_extra: Dict[str, Any] = {
            "got_stopped": bool(got_stopped),
            "ai_policy_version": AI_POLICY_VERSION or None,
            "ai_experiment_tag": AI_EXPERIMENT_TAG or None,
            "label_version": AI_LABELING_VERSION or None,
            # Keep exact hold seconds for RL even if DB stores int.
            "hold_sec_exact": float(hold_seconds),
        }
        if extra:
            merged_extra.update(dict(extra))

        row_id = ai_db.mark_outcome_for_candidate(
            candidate_id=candidate_id,
            label=effective_label,
            pnl_pct=float(pnl_pct),
            hold_sec=int(hold_seconds),
            notes=notes,
            extra=merged_extra,
        )
        log.debug(
            "Logged entry_outcome id=%s candidate_id=%s pnl_pct=%.4f hold_sec=%.2f "
            "stopped=%s label=%s",
            row_id,
            candidate_id,
            pnl_pct,
            hold_seconds,
            got_stopped,
            effective_label,
        )
        return row_id
    except Exception:
        log.exception(
            "Failed to log entry outcome for candidate_id=%s", candidate_id
        )
        if AI_LOG_STRICT:
            raise
        return -1


# ============================================================================ #
# Optional advanced helpers (executions, regime snapshots, AI events, equity,
# risk manager, reinvestor)
# ============================================================================ #


def log_trade_execution(
    *,
    trade_id: str,
    mint: str,
    side: str,  # "BUY" / "SELL"
    qty_raw: int,
    price_usd: float,
    slippage_bps: Optional[float] = None,
    fees_usd: Optional[float] = None,
    candidate_id: Optional[int] = None,
    session_id: Optional[str] = None,
    ts: Optional[Any] = None,
    extra: Optional[Mapping[str, Any]] = None,
) -> int:
    """
    Optional helper to log *per-fill* executions if ai_db exposes
    insert_trade_execution(...).

    This can be used by sniper / exit_engine to record:
      - realized prices vs quotes
      - raw size (lamports/tokens)
      - slippage, fees, latency, router version, etc.

    Returns:
      row id, or 0 if disabled / unsupported, or -1 on failure in non-strict mode.
    """
    if not PG_ENABLED or not AI_LOG_EXECUTIONS:
        return 0

    # Optional sampling for high-traffic streams
    if not _should_sample(f"exec:{trade_id}:{side}", rate=AI_LOG_EXEC_SAMPLE_RATE):
        log.debug("data_logger: sampled-out execution trade_id=%s", trade_id)
        return 0

    _ensure_ai_schema_once()

    fn = getattr(ai_db, "insert_trade_execution", None)
    if not callable(fn):
        # ai_db version doesn’t implement executions; safe no-op.
        return 0

    payload: Dict[str, Any] = {
        "trade_id": trade_id,
        "mint": mint,
        "side": side,
        "qty_raw": int(qty_raw),
        "price_usd": float(price_usd),
        "slippage_bps": float(slippage_bps) if slippage_bps is not None else None,
        "fees_usd": float(fees_usd) if fees_usd is not None else None,
        "candidate_id": candidate_id,
        "session_id": session_id,
        "ts": ts,
        "extra": dict(extra) if extra else {},
    }

    try:
        row_id = fn(**payload)
        log.debug(
            "Logged trade_execution id=%s trade_id=%s mint=%s side=%s price=%.6f",
            row_id,
            trade_id,
            mint,
            side,
            price_usd,
        )
        return row_id
    except Exception:
        log.exception("Failed to log trade execution trade_id=%s mint=%s", trade_id, mint)
        if AI_LOG_STRICT:
            raise
        return -1


def log_regime_snapshot(
    *,
    regime: str,
    state: Mapping[str, Any],
    ts: Optional[Any] = None,
    session_id: Optional[str] = None,
    extra: Optional[Mapping[str, Any]] = None,
) -> int:
    """
    Optional helper to log regime snapshots over time if ai_db exposes
    insert_regime_snapshot(...).

    Good for:
      - building regime-aware performance stats
      - training models that explicitly condition on macro regime series
    """
    if not PG_ENABLED or not AI_LOG_REGIME_SNAPSHOTS:
        return 0

    _ensure_ai_schema_once()

    fn = getattr(ai_db, "insert_regime_snapshot", None)
    if not callable(fn):
        fn = getattr(ai_db, "log_regime_snapshot", None)
    if not callable(fn):
        return 0

    payload: Dict[str, Any] = {
        "regime": regime,
        "state": dict(state),
        "session_id": session_id,
        "ts": ts,
        "extra": dict(extra) if extra else {},
    }

    try:
        row_id = fn(**payload)
        log.debug(
            "Logged regime_snapshot id=%s regime=%s session_id=%s",
            row_id,
            regime,
            session_id,
        )
        return row_id
    except Exception:
        log.exception("Failed to log regime snapshot regime=%s", regime)
        if AI_LOG_STRICT:
            raise
        return -1


def log_ai_event(
    *,
    event_type: str,
    policy_version: Optional[str] = None,
    experiment_tag: Optional[str] = None,
    src: Optional[str] = None,
    regime: Optional[str] = None,
    payload: Optional[Mapping[str, Any]] = None,
) -> int:
    """
    Thin wrapper around ai_db.log_ai_event (if present).

    Used by:
      - risk_manager (risk_halt_triggered / cleared, Kelly clamp changes)
      - self_learning_ai_strategy (parameter_tune, best_config_selected)
      - reinvestor (soft_stop / hard_stop)
      - monitoring / backtest drivers
    """
    if not PG_ENABLED:
        return 0

    # Optional sampling in case of very chatty self-learning loops.
    sample_key = f"event:{event_type}:{src or ''}:{regime or ''}"
    if not _should_sample(sample_key, rate=AI_LOG_EVENT_SAMPLE_RATE):
        log.debug("data_logger: sampled-out ai_event type=%s", event_type)
        return 0

    _ensure_ai_schema_once()

    fn = getattr(ai_db, "log_ai_event", None)
    if not callable(fn):
        return 0

    try:
        row_id = fn(
            event_type=event_type,
            policy_version=policy_version or AI_POLICY_VERSION or None,
            experiment_tag=experiment_tag or AI_EXPERIMENT_TAG or None,
            src=src,
            regime=regime,
            payload=payload or {},
        )
        log.debug(
            "Logged ai_event id=%s type=%s policy=%s exp=%s src=%s regime=%s",
            row_id,
            event_type,
            policy_version or AI_POLICY_VERSION,
            experiment_tag or AI_EXPERIMENT_TAG,
            src,
            regime,
        )
        return row_id
    except Exception:
        log.exception("Failed to log ai_event type=%s", event_type)
        if AI_LOG_STRICT:
            raise
        return -1


def log_equity_curve_point(
    *,
    ts: Optional[Any] = None,
    equity_usd: Optional[float] = None,
    balance_sol: Optional[float] = None,
    pnl_day_pct: Optional[float] = None,
    pnl_total_pct: Optional[float] = None,
    session_id: Optional[str] = None,
    extra: Optional[Mapping[str, Any]] = None,
) -> int:
    """
    Phase 3+ helper: log account-level equity curve points.

    Intended caller: reinvestor / risk_manager / periodic equity cron.

    Fields:
      - equity_usd: total account equity snapshot in USD
      - balance_sol: wallet SOL balance snapshot
      - pnl_day_pct: realized+unrealized PnL today (pct of equity or base)
      - pnl_total_pct: total PnL vs baseline (optional)
      - extra: any extra metrics (e.g. 'base_equity_usd', 'open_risk_usd')

    This function is a soft hook: if ai_db has no matching function, this
    returns 0 without error.
    """
    if not PG_ENABLED or not AI_LOG_EQUITY:
        return 0

    sample_key = f"equity:{session_id or ''}:{ts or ''}"
    if not _should_sample(sample_key, rate=AI_LOG_EQUITY_SAMPLE_RATE):
        log.debug("data_logger: sampled-out equity point session=%s", session_id)
        return 0

    _ensure_ai_schema_once()

    fn = getattr(ai_db, "insert_equity_curve_point", None)
    if not callable(fn):
        fn = getattr(ai_db, "log_equity_curve_point", None)
    if not callable(fn):
        return 0

    payload: Dict[str, Any] = {
        "ts": ts,
        "equity_usd": float(equity_usd) if equity_usd is not None else None,
        "balance_sol": float(balance_sol) if balance_sol is not None else None,
        "pnl_day_pct": float(pnl_day_pct) if pnl_day_pct is not None else None,
        "pnl_total_pct": float(pnl_total_pct) if pnl_total_pct is not None else None,
        "session_id": session_id,
        "extra": dict(extra) if extra else {},
    }

    try:
        row_id = fn(**payload)
        log.debug(
            "Logged equity_curve_point id=%s equity_usd=%s balance_sol=%s session_id=%s",
            row_id,
            equity_usd,
            balance_sol,
            session_id,
        )
        return row_id
    except Exception:
        log.exception("Failed to log equity_curve_point session_id=%s", session_id)
        if AI_LOG_STRICT:
            raise
        return -1


def log_risk_manager_snapshot(
    *,
    ts: Optional[Any] = None,
    regime: Optional[str] = None,
    risk_mode: Optional[str] = None,
    capacity_score: Optional[float] = None,
    daily_drawdown_pct: Optional[float] = None,
    daily_realized_pnl_pct: Optional[float] = None,
    max_positions: Optional[int] = None,
    open_positions: Optional[int] = None,
    hard_halt: Optional[bool] = None,
    soft_halt: Optional[bool] = None,
    session_id: Optional[str] = None,
    extra: Optional[Mapping[str, Any]] = None,
) -> int:
    """
    Phase 3/5 helper: snapshot of risk_manager state.

    Intended caller: risk_manager's periodic tick or when key events occur.

    Fields:
      - regime: label or key (e.g. "risk_on", "risk_off", "mixed")
      - risk_mode: string description of active mode / profile
      - capacity_score: 0..1 capacity / Kelly-style scalar
      - daily_drawdown_pct: today's drawdown vs equity high
      - daily_realized_pnl_pct: realized PnL for the day
      - max_positions, open_positions: current/max concurrency
      - hard_halt / soft_halt: flags if trading is stopped or clamped
    """
    if not PG_ENABLED or not AI_LOG_RISK_SNAPSHOTS:
        return 0

    sample_key = f"risk:{session_id or ''}:{regime or ''}"
    if not _should_sample(sample_key, rate=AI_LOG_RISK_SAMPLE_RATE):
        log.debug("data_logger: sampled-out risk snapshot session=%s", session_id)
        return 0

    _ensure_ai_schema_once()

    fn = getattr(ai_db, "insert_risk_snapshot", None)
    if not callable(fn):
        fn = getattr(ai_db, "log_risk_snapshot", None)
    if not callable(fn):
        return 0

    payload: Dict[str, Any] = {
        "ts": ts,
        "regime": regime,
        "risk_mode": risk_mode,
        "capacity_score": float(capacity_score) if capacity_score is not None else None,
        "daily_drawdown_pct": float(daily_drawdown_pct)
        if daily_drawdown_pct is not None
        else None,
        "daily_realized_pnl_pct": float(daily_realized_pnl_pct)
        if daily_realized_pnl_pct is not None
        else None,
        "max_positions": int(max_positions) if max_positions is not None else None,
        "open_positions": int(open_positions) if open_positions is not None else None,
        "hard_halt": bool(hard_halt) if hard_halt is not None else None,
        "soft_halt": bool(soft_halt) if soft_halt is not None else None,
        "session_id": session_id,
        "extra": dict(extra) if extra else {},
    }

    try:
        row_id = fn(**payload)
        log.debug(
            "Logged risk_manager_snapshot id=%s regime=%s risk_mode=%s session_id=%s",
            row_id,
            regime,
            risk_mode,
            session_id,
        )
        return row_id
    except Exception:
        log.exception("Failed to log risk_manager_snapshot session_id=%s", session_id)
        if AI_LOG_STRICT:
            raise
        return -1


def log_reinvestor_snapshot(
    *,
    ts: Optional[Any] = None,
    profile: Optional[str] = None,
    account_equity_usd: Optional[float] = None,
    base_capital_usd: Optional[float] = None,
    kelly_fraction: Optional[float] = None,
    risk_budget_pct: Optional[float] = None,
    soft_stop_active: Optional[bool] = None,
    hard_stop_active: Optional[bool] = None,
    session_id: Optional[str] = None,
    extra: Optional[Mapping[str, Any]] = None,
) -> int:
    """
    Phase 3/4 helper: snapshot of reinvestor state.

    Intended caller: reinvestor.py whenever it recalculates targets / clamps.

    Fields:
      - profile: string describing active reinvestor profile (e.g. "growth_v1")
      - account_equity_usd: current equity
      - base_capital_usd: baseline / starting capital for PnL reference
      - kelly_fraction: Kelly-style fraction used for target risk
      - risk_budget_pct: risk budget vs equity (e.g. max daily loss)
      - soft_stop_active / hard_stop_active: protective modes
    """
    if not PG_ENABLED or not AI_LOG_REINVESTOR_SNAPSHOTS:
        return 0

    sample_key = f"reinv:{session_id or ''}:{profile or ''}"
    if not _should_sample(sample_key, rate=AI_LOG_REINVESTOR_SAMPLE_RATE):
        log.debug("data_logger: sampled-out reinvestor snapshot session=%s", session_id)
        return 0

    _ensure_ai_schema_once()

    fn = getattr(ai_db, "insert_reinvestor_snapshot", None)
    if not callable(fn):
        fn = getattr(ai_db, "log_reinvestor_snapshot", None)
    if not callable(fn):
        return 0

    payload: Dict[str, Any] = {
        "ts": ts,
        "profile": profile,
        "account_equity_usd": float(account_equity_usd)
        if account_equity_usd is not None
        else None,
        "base_capital_usd": float(base_capital_usd)
        if base_capital_usd is not None
        else None,
        "kelly_fraction": float(kelly_fraction) if kelly_fraction is not None else None,
        "risk_budget_pct": float(risk_budget_pct)
        if risk_budget_pct is not None
        else None,
        "soft_stop_active": bool(soft_stop_active)
        if soft_stop_active is not None
        else None,
        "hard_stop_active": bool(hard_stop_active)
        if hard_stop_active is not None
        else None,
        "session_id": session_id,
        "extra": dict(extra) if extra else {},
    }

    try:
        row_id = fn(**payload)
        log.debug(
            "Logged reinvestor_snapshot id=%s profile=%s session_id=%s",
            row_id,
            profile,
            session_id,
        )
        return row_id
    except Exception:
        log.exception("Failed to log reinvestor_snapshot session_id=%s", session_id)
        if AI_LOG_STRICT:
            raise
        return -1


# ============================================================================ #
# Source-specific convenience wrappers
# ============================================================================ #


def log_raydium_candidate(
    *,
    mint: str,
    symbol: Optional[str],
    meta: Mapping[str, Any],
    regime: str = "raydium_live",
    wallet_tag: Optional[str] = None,
    regime_state: Optional[Any] = None,
    wallet_signals: Optional[Mapping[str, Any]] = None,
    ai_signals: Optional[Mapping[str, Any]] = None,
    history: Optional[Sequence[Mapping[str, Any]]] = None,
    position_context: Optional[Mapping[str, Any]] = None,
    # Optional linkage fields:
    trade_id: Optional[str] = None,
    session_id: Optional[str] = None,
    candidate_key: Optional[str] = None,
    ts: Optional[Any] = None,
    # Self-learning / gate detail hooks:
    episode_id: Optional[str] = None,
    step_idx: Optional[int] = None,
    strategy_profile: Optional[str] = None,
    gate_detail: Optional[Mapping[str, Any]] = None,
    # Advanced override:
    feature_version_override: Optional[int] = None,
) -> int:
    """
    Convenience wrapper for logging Raydium pool candidates.

    `meta` is the Raydium meta dict you already compute and log, with fields
    like liq, vol5, buys5, sells5, chg5, age_min, etc.
    """
    snapshot = _build_pf_snapshot_from_meta(meta)
    return log_entry_candidate(
        src="raydium",
        mint=mint,
        symbol=symbol,
        snapshot=snapshot,
        regime=regime,
        wallet_tag=wallet_tag,
        regime_state=regime_state,
        wallet_signals=wallet_signals,
        ai_signals=ai_signals,
        scanner_source="raydium",
        history=history,
        position_context=position_context,
        trade_id=trade_id,
        session_id=session_id,
        candidate_key=candidate_key,
        ts=ts,
        episode_id=episode_id,
        step_idx=step_idx,
        strategy_profile=strategy_profile,
        gate_detail=gate_detail,
        feature_version_override=feature_version_override,
    )


def log_pumpfun_candidate(
    *,
    mint: str,
    symbol: Optional[str],
    meta: Mapping[str, Any],
    regime: str = "pumpfun_live",
    wallet_tag: Optional[str] = None,
    regime_state: Optional[Any] = None,
    wallet_signals: Optional[Mapping[str, Any]] = None,
    ai_signals: Optional[Mapping[str, Any]] = None,
    history: Optional[Sequence[Mapping[str, Any]]] = None,
    position_context: Optional[Mapping[str, Any]] = None,
    trade_id: Optional[str] = None,
    session_id: Optional[str] = None,
    candidate_key: Optional[str] = None,
    ts: Optional[Any] = None,
    episode_id: Optional[str] = None,
    step_idx: Optional[int] = None,
    strategy_profile: Optional[str] = None,
    gate_detail: Optional[Mapping[str, Any]] = None,
    feature_version_override: Optional[int] = None,
) -> int:
    """
    Wrapper for Pump.fun PF meta dicts (bonding-curve / early-phase coins).
    """
    snapshot = _build_pf_snapshot_from_meta(meta)
    return log_entry_candidate(
        src="pumpfun",
        mint=mint,
        symbol=symbol,
        snapshot=snapshot,
        regime=regime,
        wallet_tag=wallet_tag,
        regime_state=regime_state,
        wallet_signals=wallet_signals,
        ai_signals=ai_signals,
        scanner_source="pumpfun",
        history=history,
        position_context=position_context,
        trade_id=trade_id,
        session_id=session_id,
        candidate_key=candidate_key,
        ts=ts,
        episode_id=episode_id,
        step_idx=step_idx,
        strategy_profile=strategy_profile,
        gate_detail=gate_detail,
        feature_version_override=feature_version_override,
    )


def log_dexscreener_candidate(
    *,
    mint: str,
    symbol: Optional[str],
    meta: Mapping[str, Any],
    regime: str = "dexscreener_live",
    wallet_tag: Optional[str] = None,
    regime_state: Optional[Any] = None,
    wallet_signals: Optional[Mapping[str, Any]] = None,
    ai_signals: Optional[Mapping[str, Any]] = None,
    history: Optional[Sequence[Mapping[str, Any]]] = None,
    position_context: Optional[Mapping[str, Any]] = None,
    trade_id: Optional[str] = None,
    session_id: Optional[str] = None,
    candidate_key: Optional[str] = None,
    ts: Optional[Any] = None,
    episode_id: Optional[str] = None,
    step_idx: Optional[int] = None,
    strategy_profile: Optional[str] = None,
    gate_detail: Optional[Mapping[str, Any]] = None,
    feature_version_override: Optional[int] = None,
) -> int:
    """
    Wrapper for Dexscreener PF meta dicts.
    """
    snapshot = _build_pf_snapshot_from_meta(meta)
    return log_entry_candidate(
        src="dexscreener",
        mint=mint,
        symbol=symbol,
        snapshot=snapshot,
        regime=regime,
        wallet_tag=wallet_tag,
        regime_state=regime_state,
        wallet_signals=wallet_signals,
        ai_signals=ai_signals,
        scanner_source="dexscreener",
        history=history,
        position_context=position_context,
        trade_id=trade_id,
        session_id=session_id,
        candidate_key=candidate_key,
        ts=ts,
        episode_id=episode_id,
        step_idx=step_idx,
        strategy_profile=strategy_profile,
        gate_detail=gate_detail,
        feature_version_override=feature_version_override,
    )


def log_momo_candidate(
    *,
    mint: str,
    symbol: Optional[str],
    meta: Mapping[str, Any],
    regime: str = "momo_live",
    wallet_tag: Optional[str] = None,
    regime_state: Optional[Any] = None,
    wallet_signals: Optional[Mapping[str, Any]] = None,
    ai_signals: Optional[Mapping[str, Any]] = None,
    history: Optional[Sequence[Mapping[str, Any]]] = None,
    position_context: Optional[Mapping[str, Any]] = None,
    trade_id: Optional[str] = None,
    session_id: Optional[str] = None,
    candidate_key: Optional[str] = None,
    ts: Optional[Any] = None,
    episode_id: Optional[str] = None,
    step_idx: Optional[int] = None,
    strategy_profile: Optional[str] = None,
    gate_detail: Optional[Mapping[str, Any]] = None,
    feature_version_override: Optional[int] = None,
) -> int:
    """
    Wrapper for MOMO / pure-momentum candidates (if you want a dedicated src).
    """
    snapshot = _build_pf_snapshot_from_meta(meta)
    return log_entry_candidate(
        src="momo",
        mint=mint,
        symbol=symbol,
        snapshot=snapshot,
        regime=regime,
        wallet_tag=wallet_tag,
        regime_state=regime_state,
        wallet_signals=wallet_signals,
        ai_signals=ai_signals,
        scanner_source="momo",
        history=history,
        position_context=position_context,
        trade_id=trade_id,
        session_id=session_id,
        candidate_key=candidate_key,
        ts=ts,
        episode_id=episode_id,
        step_idx=step_idx,
        strategy_profile=strategy_profile,
        gate_detail=gate_detail,
        feature_version_override=feature_version_override,
    )
