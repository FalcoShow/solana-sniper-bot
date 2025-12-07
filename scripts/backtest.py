from __future__ import annotations

"""
scripts/backtest.py

Quant-grade backtest runner for AI entry/exit + risk configs (V3+++).
=====================================================================

High-level:
    • Build a GRID of BacktestConfig points (score thresholds, risk knobs,
      AI knobs, feature modes, smart-wallet modes, regime modes, self-learning
      toggles, reinvestor toggles, AI risk overlay, etc.).
    • For each config:
          - Call utils_pkg.backtest_engine.run_backtest(...).
          - Let the engine talk to:
                ai_db / FeatureBuilder / ai_entry_logic / smart_wallets /
                regime / risk_manager / ai_risk_model /
                self_learning_ai_strategy / reinvestor / data_logger.
    • Optionally run in parallel with a concurrency cap.
    • Augment stats with derived quant metrics (edge_score, pnl_dd_ratio, calmar,
      win_loss_ratio, expectancy_pct, kelly_fraction, kelly_safe_fraction,
      trades_per_day, etc.).
    • Apply result-level filters (min trades, max DD, min Sharpe, min edge_score,
      min pnl_dd_ratio, min calmar_ratio, min win_loss_ratio, min expectancy_pct,
      min kelly_fraction, min_kelly_safe_fraction, min_trades_per_day, ...).
    • Pretty-print a comparison table.
    • Optionally:
          - export aggregated results JSON
          - export flattened CSV (config + stats)
          - export per-trade JSON
          - export best-config JSON (for piping into env or config files)
          - export best-config as .env-style file for quick prod wiring
          - print grouped summaries by config fields (e.g. risk_profile)
          - log the sweep into ai_db (if ai_db.log_backtest_sweep exists)

Everything is CLI + ENV driven – no code edits required for new experiments.

Usage examples
--------------

# Simple sweep with defaults over last 14 days
    python -m scripts.backtest

# Focus on pumpfun regime, require minimum win rate and Sharpe, export CSV
    BT_REGIME_WHITELIST=pumpfun_live \
    BT_MIN_WIN_RATE=0.55 \
    BT_MIN_SHARPE=1.2 \
      python -m scripts.backtest --export-csv backtests_pumpfun.csv

# Use parallel mode with limited concurrency
    BT_PARALLEL=1 BT_PARALLEL_LIMIT=4 python -m scripts.backtest

# Walk-forward or split mode controlling engine_mode / split_mode via base JSON:
    BT_BASE_CONFIG_JSON=config/backtest_base.json python -m scripts.backtest

# Example: random sample 30% of grid, shuffled (for quick scouting)
    BT_SAMPLE_CONFIG_FRACTION=0.3 BT_SHUFFLE_CONFIGS=1 python -m scripts.backtest

# Example: group summary by risk_profile and exit_policy_name
    BT_GROUP_SUMMARY_BY=risk_profile,exit_policy_name python -m scripts.backtest

# Example: also export best config to .env-style file
    BT_EXPORT_ENV_BEST=backtest_best.env python -m scripts.backtest
"""

import argparse
import asyncio
import csv
import json
import os
import random
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

from logging_setup import get_logger
from utils_pkg.backtest_engine import run_backtest
from utils_pkg import ai_db  # for ping / describe_backend / stats

log = get_logger(__name__)

# =============================================================================
# Env helpers
# =============================================================================


def _env_str(key: str, default: str = "") -> str:
    return os.getenv(key, default).strip()


def _env_int(key: str, default: int) -> int:
    raw = os.getenv(key)
    if raw is None or raw == "":
        return default
    try:
        return int(raw)
    except Exception:
        return default


def _env_float(key: str, default: float) -> float:
    raw = os.getenv(key)
    if raw is None or raw == "":
        return default
    try:
        return float(raw)
    except Exception:
        return default


def _env_bool(key: str, default: bool = False) -> bool:
    v = os.getenv(key)
    if v is None:
        return default
    return str(v).strip().lower() in {"1", "true", "yes", "y", "on"}


def _parse_csv_floats(s: str) -> List[float]:
    out: List[float] = []
    for part in s.replace(";", ",").split(","):
        part = part.strip()
        if not part:
            continue
        try:
            out.append(float(part))
        except Exception:
            continue
    return out


def _parse_csv_ints(s: str) -> List[int]:
    out: List[int] = []
    for part in s.replace(";", ",").split(","):
        part = part.strip()
        if not part:
            continue
        try:
            out.append(int(part))
        except Exception:
            continue
    return out


def _parse_csv_strs(s: str) -> List[str]:
    out: List[str] = []
    for part in s.replace(";", ",").split(","):
        p = part.strip()
        if p:
            out.append(p)
    return out


def _parse_date(s: str) -> Optional[datetime]:
    s = s.strip()
    if not s:
        return None
    try:
        # Allow YYYY-MM-DD or full ISO
        if "T" in s:
            return datetime.fromisoformat(s)
        return datetime.strptime(s, "%Y-%m-%d")
    except Exception:
        return None


# =============================================================================
# BacktestConfig dataclass
# =============================================================================


@dataclass
class BacktestConfig:
    """
    Full backtest configuration for a single grid point.

    Most fields are optional; to_run_kwargs() only passes non-None ones
    to the engine so unknown keys can be safely ignored.
    """

    # Core knobs for sweeps
    ENTRY_SCORE_MIN: float
    max_positions: int

    # Global backtest filters
    days_back: int
    regime_whitelist: Optional[List[str]] = None
    label_include: Optional[List[str]] = None
    label_exclude: Optional[List[str]] = None
    limit_candidates: Optional[int] = None

    # Optional date range (overrides days_back if both present)
    start_ts: Optional[datetime] = None
    end_ts: Optional[datetime] = None

    # Risk / regime knobs
    risk_mult_override: Optional[float] = None
    regime_risk_mode_lock: Optional[int] = None  # 0=risk_off,1=normal,2=risk_on

    # Strategy / AI knobs
    entry_min_score: Optional[float] = None          # ENTRY_MIN_SCORE override
    entry_ai_weight: Optional[float] = None          # ENTRY_AI_WEIGHT override
    entry_model_name: Optional[str] = None           # ENTRY_MODEL_NAME
    risk_profile: Optional[str] = None               # RISK_PROFILE
    exit_policy_name: Optional[str] = None           # EXIT_POLICY_NAME
    require_risk_capacity: Optional[bool] = None     # ENTRY_REQUIRE_RISK_CAPACITY

    # Label / sample weighting
    sample_weight_mode: Optional[str] = None         # BT_SAMPLE_WEIGHT_MODE
    label_weights_json: Optional[str] = None         # BT_LABEL_WEIGHTS_JSON

    # AI policy meta (for evolution / experiments)
    policy_version: Optional[str] = None             # ai_policy_version
    experiment_tag: Optional[str] = None             # ai_experiment_tag

    # Risk capacity gates + regime feature mode
    min_risk_capacity: Optional[float] = None        # BT_MIN_RISK_CAPACITY
    max_risk_capacity: Optional[float] = None        # BT_MAX_RISK_CAPACITY
    regime_feature_mode: Optional[str] = None        # BT_REGIME_FEATURE_MODE

    # Feature / smart-wallet / dedup wiring
    feature_mode: Optional[str] = None               # BT_FEATURE_MODE
    smart_wallet_mode: Optional[str] = None          # BT_SMART_WALLET_MODE
    dedup_mode: Optional[str] = None                 # BT_DEDUP_MODE
    dedup_window_sec: Optional[int] = None           # BT_DEDUP_WINDOW_SEC

    # Engine / split controls
    engine_mode: Optional[str] = None                # BT_ENGINE_MODE
    split_mode: Optional[str] = None                 # BT_SPLIT_MODE
    split_date: Optional[datetime] = None            # BT_SPLIT_DATE
    split_ratio: Optional[float] = None              # BT_SPLIT_RATIO
    result_segment: Optional[str] = None             # BT_RESULT_SEGMENT

    # Execution / logging controls
    max_trades: Optional[int] = None                 # BT_MAX_TRADES
    log_trades: Optional[bool] = None                # BT_LOG_TRADES
    log_features: Optional[bool] = None              # BT_LOG_FEATURES
    rand_seed: Optional[int] = None                  # BT_RAND_SEED
    tag_prefix: Optional[str] = None                 # BT_TAG_PREFIX
    use_live_risk_manager: Optional[bool] = None     # BT_USE_LIVE_RISK_MANAGER

    # Self-learning strategy knobs (V3+++)
    enable_self_learning: Optional[bool] = None      # BT_ENABLE_SELF_LEARNING
    self_learning_use_llm: Optional[bool] = None     # BT_SELF_LEARN_USE_LLM
    self_learning_interval_sec: Optional[int] = None # BT_SELF_LEARN_INTERVAL_SEC
    self_learning_min_trades: Optional[int] = None   # BT_SELF_LEARN_MIN_TRADES
    self_learning_max_tunes_per_day: Optional[int] = None  # BT_SELF_LEARN_MAX_TUNES_PER_DAY
    log_strategy_health: Optional[bool] = None       # BT_LOG_STRATEGY_HEALTH

    # Reinvestor simulation (V3+++)
    enable_reinvestor: Optional[bool] = None              # BT_ENABLE_REINVESTOR
    reinvest_percent_override: Optional[float] = None     # BT_REINVEST_PERCENT
    reinvest_min_profit_sol: Optional[float] = None       # BT_REINVEST_MIN_PROFIT_SOL
    reinvest_max_single_usd: Optional[float] = None       # BT_REINVEST_MAX_SINGLE_USD
    reinvest_dd_soft_stop_pct: Optional[float] = None     # BT_REINVEST_DD_SOFT_STOP_PCT
    reinvest_dd_hard_stop_pct: Optional[float] = None     # BT_REINVEST_DD_HARD_STOP_PCT

    # AI risk overlay (ai_risk_model) usage
    use_ai_risk_overlay: Optional[bool] = None            # BT_USE_AI_RISK_MODEL

    # Extra AI-DB slicing (quant-grade)
    src_filter: Optional[str] = None                 # BT_SRC
    min_feature_version: Optional[int] = None        # BT_MIN_FEATURE_VERSION
    wallet_tag_filter: Optional[str] = None          # BT_WALLET_TAG
    session_id_filter: Optional[str] = None          # BT_SESSION_ID

    # Execution microstructure / cost knobs (extra pass-through)
    trade_cost_bps: Optional[float] = None           # BT_TRADE_COST_BPS
    jito_tip_bps: Optional[float] = None             # BT_JITO_TIP_BPS
    base_size_sol: Optional[float] = None            # BT_BASE_SIZE_SOL
    max_hold_sec: Optional[int] = None               # BT_MAX_HOLD_SEC

    # Human-readable tag (used in tables / exports)
    tag: Optional[str] = None

    # Optional run-level meta (for notebooks / dashboards)
    group_id: Optional[str] = None                   # BT_GROUP_ID
    run_note: Optional[str] = None                   # BT_RUN_NOTE

    # Extra passthrough keys for future expansion
    extra: Dict[str, Any] | None = None

    def to_run_kwargs(self) -> Dict[str, Any]:
        """
        Flatten to the dict passed to utils_pkg.backtest_engine.run_backtest.

        Convention:
            • Boolean toggles → BT_* or ENTRY_* style names.
            • Unknown keys should be safely ignored by the engine.
        """
        cfg: Dict[str, Any] = {
            "ENTRY_SCORE_MIN": self.ENTRY_SCORE_MIN,
            "max_positions": self.max_positions,
            "days_back": self.days_back,
        }

        # Filters
        if self.regime_whitelist:
            cfg["regime_whitelist"] = list(self.regime_whitelist)
        if self.label_include:
            cfg["label_include"] = list(self.label_include)
        if self.label_exclude:
            cfg["label_exclude"] = list(self.label_exclude)
        if self.limit_candidates is not None:
            cfg["limit_candidates"] = int(self.limit_candidates)

        # Date range
        if self.start_ts is not None:
            cfg["start_ts"] = self.start_ts.isoformat()
        if self.end_ts is not None:
            cfg["end_ts"] = self.end_ts.isoformat()

        # Risk / regime
        if self.risk_mult_override is not None:
            cfg["risk_mult_override"] = float(self.risk_mult_override)
        if self.regime_risk_mode_lock is not None:
            cfg["regime_risk_mode_lock"] = int(self.regime_risk_mode_lock)

        # Strategy / AI overrides
        if self.entry_min_score is not None:
            cfg["ENTRY_MIN_SCORE"] = float(self.entry_min_score)
        if self.entry_ai_weight is not None:
            cfg["ENTRY_AI_WEIGHT"] = float(self.entry_ai_weight)
        if self.entry_model_name:
            cfg["ENTRY_MODEL_NAME"] = self.entry_model_name

        if self.risk_profile:
            cfg["RISK_PROFILE"] = self.risk_profile
        if self.exit_policy_name:
            cfg["EXIT_POLICY_NAME"] = self.exit_policy_name

        if self.require_risk_capacity is not None:
            cfg["ENTRY_REQUIRE_RISK_CAPACITY"] = bool(self.require_risk_capacity)

        # Label / sample weighting
        if self.sample_weight_mode:
            cfg["BT_SAMPLE_WEIGHT_MODE"] = self.sample_weight_mode
        if self.label_weights_json:
            cfg["BT_LABEL_WEIGHTS_JSON"] = self.label_weights_json

        # AI policy meta
        if self.policy_version:
            cfg["ai_policy_version"] = self.policy_version
        if self.experiment_tag:
            cfg["ai_experiment_tag"] = self.experiment_tag

        # Risk capacity / regime feature controls
        if self.min_risk_capacity is not None:
            cfg["BT_MIN_RISK_CAPACITY"] = float(self.min_risk_capacity)
        if self.max_risk_capacity is not None:
            cfg["BT_MAX_RISK_CAPACITY"] = float(self.max_risk_capacity)
        if self.regime_feature_mode:
            cfg["BT_REGIME_FEATURE_MODE"] = self.regime_feature_mode

        # Feature / smart-wallet / dedup
        if self.feature_mode:
            cfg["BT_FEATURE_MODE"] = self.feature_mode
        if self.smart_wallet_mode:
            cfg["BT_SMART_WALLET_MODE"] = self.smart_wallet_mode
        if self.dedup_mode:
            cfg["BT_DEDUP_MODE"] = self.dedup_mode
        if self.dedup_window_sec is not None:
            cfg["BT_DEDUP_WINDOW_SEC"] = int(self.dedup_window_sec)

        # Engine / split
        if self.engine_mode:
            cfg["BT_ENGINE_MODE"] = self.engine_mode
        if self.split_mode:
            cfg["BT_SPLIT_MODE"] = self.split_mode
        if self.split_date is not None:
            cfg["BT_SPLIT_DATE"] = self.split_date.isoformat()
        if self.split_ratio is not None:
            cfg["BT_SPLIT_RATIO"] = float(self.split_ratio)
        if self.result_segment:
            cfg["BT_RESULT_SEGMENT"] = self.result_segment

        # Execution / logging
        if self.max_trades is not None:
            cfg["BT_MAX_TRADES"] = int(self.max_trades)
        if self.log_trades is not None:
            cfg["BT_LOG_TRADES"] = bool(self.log_trades)
        if self.log_features is not None:
            cfg["BT_LOG_FEATURES"] = bool(self.log_features)
        if self.rand_seed is not None:
            cfg["BT_RAND_SEED"] = int(self.rand_seed)
        if self.tag_prefix:
            cfg["BT_TAG_PREFIX"] = self.tag_prefix
        if self.use_live_risk_manager is not None:
            cfg["BT_USE_LIVE_RISK_MANAGER"] = bool(self.use_live_risk_manager)

        # Self-learning strategy
        if self.enable_self_learning is not None:
            cfg["BT_ENABLE_SELF_LEARNING"] = bool(self.enable_self_learning)
        if self.self_learning_use_llm is not None:
            cfg["BT_SELF_LEARN_USE_LLM"] = bool(self.self_learning_use_llm)
        if self.self_learning_interval_sec is not None:
            cfg["BT_SELF_LEARN_INTERVAL_SEC"] = int(self.self_learning_interval_sec)
        if self.self_learning_min_trades is not None:
            cfg["BT_SELF_LEARN_MIN_TRADES"] = int(self.self_learning_min_trades)
        if self.self_learning_max_tunes_per_day is not None:
            cfg["BT_SELF_LEARN_MAX_TUNES_PER_DAY"] = int(
                self.self_learning_max_tunes_per_day
            )
        if self.log_strategy_health is not None:
            cfg["BT_LOG_STRATEGY_HEALTH"] = bool(self.log_strategy_health)

        # Reinvestor knobs (pass-through to engine/reinvestor if supported)
        if self.enable_reinvestor is not None:
            cfg["BT_ENABLE_REINVESTOR"] = bool(self.enable_reinvestor)
        if self.reinvest_percent_override is not None:
            cfg["BT_REINVEST_PERCENT"] = float(self.reinvest_percent_override)
        if self.reinvest_min_profit_sol is not None:
            cfg["BT_REINVEST_MIN_PROFIT_SOL"] = float(self.reinvest_min_profit_sol)
        if self.reinvest_max_single_usd is not None:
            cfg["BT_REINVEST_MAX_SINGLE_USD"] = float(self.reinvest_max_single_usd)
        if self.reinvest_dd_soft_stop_pct is not None:
            cfg["BT_REINVEST_DD_SOFT_STOP_PCT"] = float(self.reinvest_dd_soft_stop_pct)
        if self.reinvest_dd_hard_stop_pct is not None:
            cfg["BT_REINVEST_DD_HARD_STOP_PCT"] = float(self.reinvest_dd_hard_stop_pct)

        # AI risk overlay usage
        if self.use_ai_risk_overlay is not None:
            cfg["BT_USE_AI_RISK_MODEL"] = bool(self.use_ai_risk_overlay)

        # Extra AI-DB slicing meta
        if self.src_filter:
            cfg["BT_SRC"] = self.src_filter
        if self.min_feature_version is not None:
            cfg["BT_MIN_FEATURE_VERSION"] = int(self.min_feature_version)
        if self.wallet_tag_filter:
            cfg["BT_WALLET_TAG"] = self.wallet_tag_filter
        if self.session_id_filter:
            cfg["BT_SESSION_ID"] = self.session_id_filter

        # Execution microstructure / costs
        if self.trade_cost_bps is not None:
            cfg["BT_TRADE_COST_BPS"] = float(self.trade_cost_bps)
        if self.jito_tip_bps is not None:
            cfg["BT_JITO_TIP_BPS"] = float(self.jito_tip_bps)
        if self.base_size_sol is not None:
            cfg["BT_BASE_SIZE_SOL"] = float(self.base_size_sol)
        if self.max_hold_sec is not None:
            cfg["BT_MAX_HOLD_SEC"] = int(self.max_hold_sec)

        # Run meta
        if self.tag:
            cfg["config_tag"] = self.tag
        if self.group_id:
            cfg["BT_GROUP_ID"] = self.group_id
        if self.run_note:
            cfg["BT_RUN_NOTE"] = self.run_note

        if self.extra:
            cfg.update(self.extra)

        return cfg


# =============================================================================
# CLI builder
# =============================================================================


def build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Quant-grade backtest runner for AI entry/exit + risk configs."
    )

    # --- Core sweep knobs ----------------------------------------------------
    p.add_argument(
        "--score-min",
        default=_env_str("BT_SCORE_MIN_LIST", "0.55,0.60,0.65"),
        help="Comma-separated ENTRY_SCORE_MIN thresholds (default: 0.55,0.60,0.65 or BT_SCORE_MIN_LIST).",
    )
    p.add_argument(
        "--max-positions",
        default=_env_str("BT_MAX_POS_LIST", "1,2"),
        help="Comma-separated max open positions (default: 1,2 or BT_MAX_POS_LIST).",
    )

    # --- Risk / regime sweeps ------------------------------------------------
    p.add_argument(
        "--risk-mult",
        default=_env_str("BT_RISK_MULT_LIST", ""),
        help="Comma-separated risk_mult_override values (e.g. 0.75,1.0,1.25).",
    )
    p.add_argument(
        "--regime-risk-mode",
        default=_env_str("BT_REGIME_RISK_MODE_LIST", ""),
        help="Comma-separated regime_risk_mode_lock values (0=risk_off,1=normal,2=risk_on).",
    )

    # --- Strategy / AI sweeps ------------------------------------------------
    p.add_argument(
        "--entry-min-score",
        default=_env_str("BT_ENTRY_MIN_SCORE_LIST", ""),
        help="Comma-separated ENTRY_MIN_SCORE overrides (blended score gate).",
    )
    p.add_argument(
        "--entry-ai-weight",
        default=_env_str("BT_ENTRY_AI_WEIGHT_LIST", ""),
        help="Comma-separated ENTRY_AI_WEIGHT overrides (0..1 AI vs heuristic).",
    )
    p.add_argument(
        "--entry-model",
        default=_env_str("BT_ENTRY_MODEL_NAME", ""),
        help="Single ENTRY_MODEL_NAME override (e.g. ai-entry-v3).",
    )
    p.add_argument(
        "--risk-profile",
        default=_env_str("BT_RISK_PROFILE_LIST", ""),
        help='Comma-separated risk profiles ("conservative,balanced,aggressive").',
    )
    p.add_argument(
        "--exit-policy",
        default=_env_str("BT_EXIT_POLICY_LIST", ""),
        help='Comma-separated exit policy names (e.g. "dca,hard_stop,hybrid").',
    )
    p.add_argument(
        "--require-risk-capacity",
        action="store_true",
        default=_env_bool("BT_REQUIRE_RISK_CAPACITY", False),
        help="Toggle ENTRY_REQUIRE_RISK_CAPACITY for all configs.",
    )

    # --- Label / sample weighting -------------------------------------------
    p.add_argument(
        "--sample-weight-mode",
        default=_env_str("BT_SAMPLE_WEIGHT_MODE", ""),
        help='Sample weighting mode (e.g. "by_label","uniform","by_outcome_severity").',
    )
    p.add_argument(
        "--label-weights-json",
        default=_env_str("BT_LABEL_WEIGHTS_JSON", ""),
        help='JSON mapping label->weight (e.g. \'{"monster_win":1.5,"loss":0.7}\').',
    )

    # --- Global backtest filters --------------------------------------------
    p.add_argument(
        "--days-back",
        type=int,
        default=_env_int("BT_DAYS_BACK", 14),
        help="Days back for data (ignored if start+end dates are both set).",
    )
    p.add_argument(
        "--start-date",
        default=_env_str("BT_START_DATE", ""),
        help="Optional start date (YYYY-MM-DD or ISO).",
    )
    p.add_argument(
        "--end-date",
        default=_env_str("BT_END_DATE", ""),
        help="Optional end date (YYYY-MM-DD or ISO).",
    )
    p.add_argument(
        "--regime",
        default=_env_str("BT_REGIME_WHITELIST", ""),
        help="Comma-separated regime names (e.g. raydium_live,pumpfun_live).",
    )
    p.add_argument(
        "--label-include",
        default=_env_str("BT_LABEL_INCLUDE", ""),
        help="Comma-separated outcome labels to include (monster_win,win,loss...).",
    )
    p.add_argument(
        "--label-exclude",
        default=_env_str("BT_LABEL_EXCLUDE", ""),
        help="Comma-separated outcome labels to exclude.",
    )
    p.add_argument(
        "--limit-candidates",
        type=int,
        default=_env_int("BT_LIMIT_CANDIDATES", 0),
        help="Cap number of candidates (0 = no cap).",
    )

    # --- AI policy filters ---------------------------------------------------
    p.add_argument(
        "--policy-version",
        default=_env_str("BT_POLICY_VERSION", ""),
        help="Filter by ai_policy_version.",
    )
    p.add_argument(
        "--experiment-tag",
        default=_env_str("BT_EXPERIMENT_TAG", ""),
        help="Filter by ai_experiment_tag.",
    )

    # --- Risk capacity / regime feature controls ----------------------------
    p.add_argument(
        "--min-risk-capacity",
        type=float,
        default=_env_float("BT_MIN_RISK_CAPACITY", -1.0),
        help="Minimum risk_capacity gate (0..1). Negative = disabled.",
    )
    p.add_argument(
        "--max-risk-capacity",
        type=float,
        default=_env_float("BT_MAX_RISK_CAPACITY", -1.0),
        help="Maximum risk_capacity gate (0..1). Negative = disabled.",
    )
    p.add_argument(
        "--regime-feature-mode",
        default=_env_str("BT_REGIME_FEATURE_MODE", ""),
        help='Regime feature richness ("off","simple","full").',
    )

    # --- Feature / smart-wallet / dedup wiring ------------------------------
    p.add_argument(
        "--feature-mode",
        default=_env_str("BT_FEATURE_MODE", ""),
        help='FeatureBuilder mode ("baseline","extended","full").',
    )
    p.add_argument(
        "--smart-wallet-mode",
        default=_env_str("BT_SMART_WALLET_MODE", ""),
        help='SmartWallets mode ("off","alpha","whale","all").',
    )
    p.add_argument(
        "--dedup-mode",
        default=_env_str("BT_DEDUP_MODE", ""),
        help='Dedup mode ("off","mint","mint_window").',
    )
    p.add_argument(
        "--dedup-window-sec",
        type=int,
        default=_env_int("BT_DEDUP_WINDOW_SEC", 0),
        help="Dedup window in seconds (0 = engine default).",
    )

    # --- Engine / split controls --------------------------------------------
    p.add_argument(
        "--engine-mode",
        default=_env_str("BT_ENGINE_MODE", ""),
        help='Engine mode ("vanilla","split","walkforward").',
    )
    p.add_argument(
        "--split-mode",
        default=_env_str("BT_SPLIT_MODE", ""),
        help='Split mode when engine_mode="split" ("time","hash").',
    )
    p.add_argument(
        "--split-date",
        default=_env_str("BT_SPLIT_DATE", ""),
        help="Boundary date for split-mode='time' (YYYY-MM-DD or ISO).",
    )
    p.add_argument(
        "--split-ratio",
        type=float,
        default=_env_float("BT_SPLIT_RATIO", 0.0),
        help="Train/test ratio for split-mode='hash' (0..1, 0=engine default).",
    )
    p.add_argument(
        "--segment",
        default=_env_str("BT_RESULT_SEGMENT", ""),
        help='Result segment to focus on ("full","train","test").',
    )

    # --- Execution / output controls ----------------------------------------
    p.add_argument(
        "--max-trades",
        type=int,
        default=_env_int("BT_MAX_TRADES", 0),
        help="Hard cap on trades per config (0 = no cap).",
    )
    p.add_argument(
        "--log-trades",
        action="store_true",
        default=_env_bool("BT_LOG_TRADES", False),
        help="Ask engine to attach per-trade logs if supported.",
    )
    p.add_argument(
        "--log-features",
        action="store_true",
        default=_env_bool("BT_LOG_FEATURES", False),
        help="Ask engine to attach feature vectors if supported.",
    )
    p.add_argument(
        "--seed",
        type=int,
        default=_env_int("BT_RAND_SEED", 0),
        help="Random seed (0 = disabled).",
    )
    p.add_argument(
        "--tag-prefix",
        default=_env_str("BT_TAG_PREFIX", ""),
        help="Prefix applied to config tag (for grouping runs).",
    )
    p.add_argument(
        "--use-live-risk-manager",
        action="store_true",
        default=_env_bool("BT_USE_LIVE_RISK_MANAGER", False),
        help="Simulate live risk_manager sizing/halting logic (if engine supports).",
    )

    # --- Self-learning strategy simulation ----------------------------------
    p.add_argument(
        "--enable-self-learning",
        action="store_true",
        default=_env_bool("BT_ENABLE_SELF_LEARNING", False),
        help="Enable self_learning_ai_strategy inside backtest (if engine supports).",
    )
    p.add_argument(
        "--self-learn-use-llm",
        action="store_true",
        default=_env_bool("BT_SELF_LEARN_USE_LLM", False),
        help="Allow LLM adjustments during self-learning in backtest.",
    )
    p.add_argument(
        "--self-learn-interval-sec",
        type=int,
        default=_env_int("BT_SELF_LEARN_INTERVAL_SEC", 0),
        help="Tuning interval in seconds (0 = engine default).",
    )
    p.add_argument(
        "--self-learn-min-trades",
        type=int,
        default=_env_int("BT_SELF_LEARN_MIN_TRADES", 0),
        help="Minimum trades before self-learning starts (0 = engine default).",
    )
    p.add_argument(
        "--self-learn-max-per-day",
        type=int,
        default=_env_int("BT_SELF_LEARN_MAX_TUNES_PER_DAY", 0),
        help="Hard cap on param updates per day in backtest (0 = engine default).",
    )
    p.add_argument(
        "--log-strategy-health",
        action="store_true",
        default=_env_bool("BT_LOG_STRATEGY_HEALTH", False),
        help="Ask engine to log strategy health snapshots (for dashboards).",
    )

    # --- Reinvestor simulation ----------------------------------------------
    p.add_argument(
        "--enable-reinvestor",
        action="store_true",
        default=_env_bool("BT_ENABLE_REINVESTOR", False),
        help="Enable reinvestor simulation in backtest (if engine supports).",
    )
    p.add_argument(
        "--reinvest-percent",
        type=float,
        default=_env_float("BT_REINVEST_PERCENT", 0.0),
        help="Override base REINVEST_PERCENT for backtest (0 = use engine default).",
    )
    p.add_argument(
        "--reinvest-min-profit-sol",
        type=float,
        default=_env_float("BT_REINVEST_MIN_PROFIT_SOL", 0.0),
        help="Override REINVESTMENT_MIN_PROFIT_SOL for backtest (0 = engine default).",
    )
    p.add_argument(
        "--reinvest-max-single-usd",
        type=float,
        default=_env_float("BT_REINVEST_MAX_SINGLE_USD", 0.0),
        help="Override REINVEST_MAX_SINGLE_USD for backtest (0 = engine default).",
    )
    p.add_argument(
        "--reinvest-dd-soft",
        type=float,
        default=_env_float("BT_REINVEST_DD_SOFT_STOP_PCT", 0.0),
        help="Override REINVEST_DD_SOFT_STOP_PCT (0 = engine default).",
    )
    p.add_argument(
        "--reinvest-dd-hard",
        type=float,
        default=_env_float("BT_REINVEST_DD_HARD_STOP_PCT", 0.0),
        help="Override REINVEST_DD_HARD_STOP_PCT (0 = engine default).",
    )

    # --- AI risk overlay usage ----------------------------------------------
    p.add_argument(
        "--use-ai-risk-overlay",
        action="store_true",
        default=_env_bool("BT_USE_AI_RISK_MODEL", False),
        help="Enable ai_risk_model overlay inside risk_manager during backtest (if engine supports).",
    )

    # --- Extra AI-DB slicing (quant-grade) ----------------------------------
    p.add_argument(
        "--src",
        default=_env_str("BT_SRC", ""),
        help="Optional src filter for quick ai_db stats (e.g. raydium,pumpfun).",
    )
    p.add_argument(
        "--min-feature-version",
        type=int,
        default=_env_int("BT_MIN_FEATURE_VERSION", 0),
        help="Minimum feature_version for ai_db stats / engine, 0 = disabled.",
    )
    p.add_argument(
        "--wallet-tag",
        default=_env_str("BT_WALLET_TAG", ""),
        help="Optional wallet_tag filter (if engine/ai_db support it).",
    )
    p.add_argument(
        "--session-id",
        default=_env_str("BT_SESSION_ID", ""),
        help="Optional session_id filter (if engine/ai_db support it).",
    )

    # --- Microstructure / cost modelling ------------------------------------
    p.add_argument(
        "--trade-cost-bps",
        type=float,
        default=_env_float("BT_TRADE_COST_BPS", 0.0),
        help="Round-trip trade cost in bps (commissions + slippage model). 0 = engine default.",
    )
    p.add_argument(
        "--jito-tip-bps",
        type=float,
        default=_env_float("BT_JITO_TIP_BPS", 0.0),
        help="Optional Jito tip cost in bps for backtests.",
    )
    p.add_argument(
        "--base-size-sol",
        type=float,
        default=_env_float("BT_BASE_SIZE_SOL", 0.0),
        help="Base size per trade in SOL for the engine (0 = engine default).",
    )
    p.add_argument(
        "--max-hold-sec",
        type=int,
        default=_env_int("BT_MAX_HOLD_SEC", 0),
        help="Hard max holding period per trade in seconds (0 = engine default).",
    )

    # --- Parallelism / sampling / sorting -----------------------------------
    p.add_argument(
        "--parallel",
        action="store_true",
        default=_env_bool("BT_PARALLEL", False),
        help="Run configs in parallel with asyncio.",
    )
    p.add_argument(
        "--parallel-limit",
        type=int,
        default=_env_int("BT_PARALLEL_LIMIT", 8),
        help="Max concurrent backtests when --parallel is enabled.",
    )
    p.add_argument(
        "--sample-config-fraction",
        type=float,
        default=_env_float("BT_SAMPLE_CONFIG_FRACTION", 0.0),
        help="If >0 and <1, randomly sample this fraction of the built config grid.",
    )
    p.add_argument(
        "--shuffle-configs",
        action="store_true",
        default=_env_bool("BT_SHUFFLE_CONFIGS", False),
        help="Shuffle config execution order before running (purely research/QoL).",
    )
    p.add_argument(
        "--sort-key",
        default=_env_str("BT_SORT_KEY", ""),
        help="Metric name to sort by (e.g. pnl_total_pct, sharpe, edge_score).",
    )
    p.add_argument(
        "--sort-asc",
        action="store_true",
        default=_env_bool("BT_SORT_ASC", False),
        help="Sort ascending instead of descending (useful for drawdown metrics).",
    )

    # --- Research result filters --------------------------------------------
    p.add_argument(
        "--min-trades",
        type=int,
        default=_env_int("BT_MIN_TRADES", 0),
        help="Drop configs with fewer trades than this (0 = no min).",
    )
    p.add_argument(
        "--require-trades",
        action="store_true",
        default=_env_bool("BT_REQUIRE_TRADES", False),
        help="Drop configs with zero trades.",
    )
    p.add_argument(
        "--top-n",
        type=int,
        default=_env_int("BT_TOP_N", 0),
        help="Only show/export top N configs by sort key (0 = all).",
    )
    p.add_argument(
        "--max-configs",
        type=int,
        default=_env_int("BT_MAX_CONFIGS", 0),
        help="Hard cap on number of configs to evaluate (0 = no cap).",
    )

    # Result metric filters (V3+++ / quant-grade)
    p.add_argument(
        "--min-win-rate",
        type=float,
        default=_env_float("BT_MIN_WIN_RATE", -1.0),
        help="Minimum win_rate (0..1). Negative = disabled.",
    )
    p.add_argument(
        "--max-drawdown",
        type=float,
        default=_env_float("BT_MAX_DRAWDOWN_PCT", 0.0),
        help="Maximum |max_drawdown_pct| allowed (e.g. 30 for 30%%). 0 = disabled.",
    )
    p.add_argument(
        "--max-p95-drawdown-trade-pct",
        type=float,
        default=_env_float("BT_MAX_P95_DRAWDOWN_TRADE_PCT", 0.0),
        help="Maximum p95_drawdown_trade_pct allowed (0 = disabled).",
    )
    p.add_argument(
        "--min-sharpe",
        type=float,
        default=_env_float("BT_MIN_SHARPE", 0.0),
        help="Minimum Sharpe ratio (0 = disabled).",
    )
    p.add_argument(
        "--min-profit-factor",
        type=float,
        default=_env_float("BT_MIN_PROFIT_FACTOR", 0.0),
        help="Minimum profit_factor (0 = disabled).",
    )
    p.add_argument(
        "--min-avg-pnl-pct",
        type=float,
        default=_env_float("BT_MIN_AVG_PNL_PCT", 0.0),
        help="Minimum avg_pnl_pct per trade (0 = disabled).",
    )
    p.add_argument(
        "--min-sortino",
        type=float,
        default=_env_float("BT_MIN_SORTINO", 0.0),
        help="Minimum Sortino ratio (0 = disabled).",
    )
    p.add_argument(
        "--min-daily-pnl-pct",
        type=float,
        default=_env_float("BT_MIN_DAILY_PNL_PCT", 0.0),
        help="Minimum daily_pnl_pct (0 = disabled).",
    )
    p.add_argument(
        "--min-equity-final-pct",
        type=float,
        default=_env_float("BT_MIN_EQUITY_FINAL_PCT", 0.0),
        help="Minimum equity_final_pct (0 = disabled).",
    )
    # Backwards-compat alias (older scripts might still use this spelling)
    p.add_argument(
        "--min-equity-final-pcnt",
        type=float,
        default=_env_float("BT_MIN_EQUITY_FINAL_PCT", 0.0),
        help=argparse.SUPPRESS,
    )
    p.add_argument(
        "--min-net-pnl-pct",
        type=float,
        default=_env_float("BT_MIN_NET_PNL_PCT", 0.0),
        help="Minimum net_pnl_pct (0 = disabled).",
    )
    p.add_argument(
        "--min-reinvest-count",
        type=int,
        default=_env_int("BT_MIN_REINVEST_COUNT", 0),
        help="Minimum reinvest_count required (0 = disabled).",
    )
    p.add_argument(
        "--min-self-learn-tunes",
        type=int,
        default=_env_int("BT_MIN_SELF_LEARN_TUNE_COUNT", 0),
        help="Minimum self_learning_tune_count required (0 = disabled).",
    )
    p.add_argument(
        "--min-edge-score",
        type=float,
        default=_env_float("BT_MIN_EDGE_SCORE", 0.0),
        help="Minimum edge_score required (0 = disabled).",
    )
    p.add_argument(
        "--min-pnl-dd-ratio",
        type=float,
        default=_env_float("BT_MIN_PNL_DD_RATIO", 0.0),
        help="Minimum pnl_dd_ratio required (0 = disabled).",
    )
    p.add_argument(
        "--min-calmar",
        type=float,
        default=_env_float("BT_MIN_CALMAR", 0.0),
        help="Minimum calmar_ratio required (0 = disabled).",
    )
    p.add_argument(
        "--min-win-loss-ratio",
        type=float,
        default=_env_float("BT_MIN_WIN_LOSS_RATIO", 0.0),
        help="Minimum win_loss_ratio required (0 = disabled).",
    )
    p.add_argument(
        "--min-expectancy-pct",
        type=float,
        default=_env_float("BT_MIN_EXPECTANCY_PCT", 0.0),
        help="Minimum expectancy_pct required (0 = disabled).",
    )
    p.add_argument(
        "--min-kelly-fraction",
        type=float,
        default=_env_float("BT_MIN_KELLY_FRACTION", 0.0),
        help="Minimum Kelly fraction required (0 = disabled).",
    )
    p.add_argument(
        "--min-kelly-safe-fraction",
        type=float,
        default=_env_float("BT_MIN_KELLY_SAFE_FRACTION", 0.0),
        help="Minimum kelly_safe_fraction required (0 = disabled).",
    )
    p.add_argument(
        "--min-trades-per-day",
        type=float,
        default=_env_float("BT_MIN_TRADES_PER_DAY", 0.0),
        help="Minimum trades_per_day (derived from n_trades / days_span). 0 = disabled.",
    )

    # Custom metric columns
    p.add_argument(
        "--metrics",
        default=_env_str("BT_METRICS", ""),
        help='Comma-separated metric names for the table (e.g. "n_trades,win_rate,pnl_total_pct,sharpe,edge_score").',
    )

    # Export knobs
    p.add_argument(
        "--export-json",
        default=_env_str("BT_EXPORT_JSON", ""),
        help="Path to export full config+stats results as JSON.",
    )
    p.add_argument(
        "--export-csv",
        default=_env_str("BT_EXPORT_CSV", ""),
        help="Path to export flattened results as CSV.",
    )
    p.add_argument(
        "--export-trades-json",
        default=_env_str("BT_EXPORT_TRADES_JSON", ""),
        help="Path to export per-trade logs JSON (if engine returns 'trades').",
    )
    p.add_argument(
        "--export-env-best",
        default=_env_str("BT_EXPORT_ENV_BEST", ""),
        help="Path to export BEST config as .env-style key=value file for quick wiring into live env or config files.",
    )

    # Optional: print top-1 config as standalone JSON (for piping)
    p.add_argument(
        "--print-best-json",
        action="store_true",
        default=_env_bool("BT_PRINT_BEST_JSON", False),
        help="After filtering/sorting, print the best config+stats as JSON.",
    )

    p.add_argument(
        "--verbose",
        action="store_true",
        help="Dump full config+stats for each surviving config.",
    )

    # Small debug helper: just build + print configs, don't hit DB
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Build and print config grid, then exit (no DB/backtests).",
    )

    # Optional: baseline config file to merge into each grid point
    p.add_argument(
        "--base-config-json",
        default=_env_str("BT_BASE_CONFIG_JSON", ""),
        help="Optional JSON file with baseline engine config to merge into each grid point.",
    )

    # Run-level meta for grouping / notebooks
    p.add_argument(
        "--group-id",
        default=_env_str("BT_GROUP_ID", ""),
        help="Optional group id for this sweep (used in exports / dashboards).",
    )
    p.add_argument(
        "--run-note",
        default=_env_str("BT_RUN_NOTE", ""),
        help="Optional free-text note for this sweep (stored in config.extra).",
    )

    # Group summary + ai_db sweep logging (V3+++ sugar)
    p.add_argument(
        "--group-summary-by",
        default=_env_str("BT_GROUP_SUMMARY_BY", ""),
        help='Comma-separated BacktestConfig fields for grouped summaries '
             '(e.g. "risk_profile,exit_policy_name,max_positions").',
    )
    p.add_argument(
        "--save-sweep-to-ai-db",
        action="store_true",
        default=_env_bool("BT_SAVE_SWEEP_TO_AI_DB", False),
        help="If set and ai_db.log_backtest_sweep exists, log this sweep into ai_db.",
    )

    return p


# =============================================================================
# Config grid builder
# =============================================================================


def _resolve_date_range(
    args: argparse.Namespace,
) -> Tuple[Optional[datetime], Optional[datetime], int]:
    """
    Decide effective (start_ts, end_ts, days_back).

    If both start_date and end_date are valid, we use them and derive an
    effective days_back just for display. Otherwise fallback to days_back only.
    """
    start_ts = _parse_date(args.start_date)
    end_ts = _parse_date(args.end_date)

    if start_ts and end_ts:
        if end_ts.time() == datetime.min.time():
            end_ts = end_ts + timedelta(days=1)
        eff_days = max(1, (end_ts - start_ts).days)
        return start_ts, end_ts, eff_days

    return None, None, int(args.days_back)


def _load_base_extra(path: str) -> Dict[str, Any]:
    """
    Optional helper: load a baseline config JSON and treat it as 'extra'
    passthrough key/values for each BacktestConfig.

    This lets you fix engine-mode / feature-mode defaults in a file,
    then sweep just a few knobs via CLI/ENV.
    """
    if not path:
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, dict):
            return dict(data)
    except Exception as e:
        log.warning("Failed to load base config JSON (%s): %s", path, e)
    return {}


def build_config_grid(args: argparse.Namespace) -> List[BacktestConfig]:
    score_mins = _parse_csv_floats(args.score_min)
    if not score_mins:
        score_mins = [0.60]

    max_pos_list = _parse_csv_ints(args.max_positions)
    if not max_pos_list:
        max_pos_list = [1]

    risk_mults: List[Optional[float]] = _parse_csv_floats(args.risk_mult) or [None]

    regime_modes_raw = _parse_csv_ints(args.regime_risk_mode)
    if regime_modes_raw:
        regime_modes: List[Optional[int]] = [
            m for m in regime_modes_raw if m in (0, 1, 2)
        ] or [None]
    else:
        regime_modes = [None]

    # Strategy / AI sweeps
    entry_min_scores: List[Optional[float]] = _parse_csv_floats(
        args.entry_min_score
    ) or [None]
    entry_ai_weights: List[Optional[float]] = _parse_csv_floats(
        args.entry_ai_weight
    ) or [None]
    risk_profiles: List[Optional[str]] = _parse_csv_strs(args.risk_profile) or [None]
    exit_policies: List[Optional[str]] = _parse_csv_strs(args.exit_policy) or [None]

    # Filters
    regime_whitelist = _parse_csv_strs(args.regime) or None
    label_include = _parse_csv_strs(args.label_include) or None
    label_exclude = _parse_csv_strs(args.label_exclude) or None
    limit_candidates = args.limit_candidates if args.limit_candidates > 0 else None

    policy_version = args.policy_version.strip() or None
    experiment_tag = args.experiment_tag.strip() or None

    start_ts, end_ts, eff_days_back = _resolve_date_range(args)

    # Weighting
    sample_weight_mode = args.sample_weight_mode.strip() or None
    label_weights_json = args.label_weights_json.strip() or None

    entry_model_name = args.entry_model.strip() or None

    # Risk capacity / regime features
    min_risk_capacity: Optional[float] = None
    max_risk_capacity: Optional[float] = None
    if args.min_risk_capacity >= 0.0:
        min_risk_capacity = float(args.min_risk_capacity)
    if args.max_risk_capacity >= 0.0:
        max_risk_capacity = float(args.max_risk_capacity)
    regime_feature_mode = args.regime_feature_mode.strip() or None

    # Feature / smart-wallet / dedup
    feature_mode = args.feature_mode.strip() or None
    smart_wallet_mode = args.smart_wallet_mode.strip() or None
    dedup_mode = args.dedup_mode.strip() or None
    dedup_window_sec = args.dedup_window_sec if args.dedup_window_sec > 0 else None

    # Engine / split
    engine_mode = args.engine_mode.strip() or None
    split_mode = args.split_mode.strip() or None
    split_date = _parse_date(args.split_date) if args.split_date.strip() else None
    split_ratio = args.split_ratio if args.split_ratio > 0.0 else None
    result_segment = args.segment.strip() or None

    # Execution / logging
    max_trades = args.max_trades if args.max_trades > 0 else None
    log_trades = bool(args.log_trades)
    log_features = bool(args.log_features)
    rand_seed = args.seed if args.seed != 0 else None
    tag_prefix = args.tag_prefix.strip() or None
    use_live_risk_manager = bool(args.use_live_risk_manager)

    # Self-learning knobs
    enable_self_learning = bool(args.enable_self_learning)
    self_learning_use_llm = bool(args.self_learn_use_llm)
    self_learning_interval_sec = (
        args.self_learn_interval_sec if args.self_learn_interval_sec > 0 else None
    )
    self_learning_min_trades = (
        args.self_learn_min_trades if args.self_learn_min_trades > 0 else None
    )
    self_learning_max_tunes_per_day = (
        args.self_learn_max_per_day if args.self_learn_max_per_day > 0 else None
    )
    log_strategy_health = bool(args.log_strategy_health)

    # Reinvestor knobs
    enable_reinvestor = bool(args.enable_reinvestor)
    reinvest_percent_override = (
        args.reinvest_percent if args.reinvest_percent > 0.0 else None
    )
    reinvest_min_profit_sol = (
        args.reinvest_min_profit_sol if args.reinvest_min_profit_sol > 0.0 else None
    )
    reinvest_max_single_usd = (
        args.reinvest_max_single_usd if args.reinvest_max_single_usd > 0.0 else None
    )
    reinvest_dd_soft_stop_pct = (
        args.reinvest_dd_soft if args.reinvest_dd_soft > 0.0 else None
    )
    reinvest_dd_hard_stop_pct = (
        args.reinvest_dd_hard if args.reinvest_dd_hard > 0.0 else None
    )

    # AI risk overlay
    use_ai_risk_overlay = bool(args.use_ai_risk_overlay)

    # Extra AI-DB slicing
    src_filter = args.src.strip() or None
    min_feature_version = (
        args.min_feature_version if args.min_feature_version > 0 else None
    )
    wallet_tag_filter = args.wallet_tag.strip() or None
    session_id_filter = args.session_id.strip() or None

    # Microstructure / cost modelling
    trade_cost_bps = args.trade_cost_bps if args.trade_cost_bps > 0.0 else None
    jito_tip_bps = args.jito_tip_bps if args.jito_tip_bps > 0.0 else None
    base_size_sol = args.base_size_sol if args.base_size_sol > 0.0 else None
    max_hold_sec = args.max_hold_sec if args.max_hold_sec > 0 else None

    # Optional per-grid baseline config
    base_extra = _load_base_extra(args.base_config_json)

    # Run meta
    group_id = args.group_id.strip() or None
    run_note = args.run_note.strip() or None

    configs: List[BacktestConfig] = []

    for s in score_mins:
        for max_pos in max_pos_list:
            for risk_mult in risk_mults:
                for regime_mode in regime_modes:
                    for entry_min in entry_min_scores:
                        for ai_w in entry_ai_weights:
                            for rp in risk_profiles:
                                for ep in exit_policies:
                                    tag_parts = [
                                        f"s{round(s, 3):.3f}",
                                        f"mp{max_pos}",
                                    ]
                                    if risk_mult is not None:
                                        tag_parts.append(f"rm{risk_mult:.2f}")
                                    if regime_mode is not None:
                                        tag_parts.append(f"rg{regime_mode}")
                                    if entry_min is not None:
                                        tag_parts.append(f"emin{entry_min:.2f}")
                                    if ai_w is not None:
                                        tag_parts.append(f"aiw{ai_w:.2f}")
                                    if rp:
                                        tag_parts.append(f"rp{rp}")
                                    if ep:
                                        tag_parts.append(f"ep{ep}")
                                    if min_risk_capacity is not None:
                                        tag_parts.append(
                                            f"rcmin{min_risk_capacity:.2f}"
                                        )
                                    if max_risk_capacity is not None:
                                        tag_parts.append(
                                            f"rcmax{max_risk_capacity:.2f}"
                                        )
                                    if feature_mode:
                                        tag_parts.append(f"feat{feature_mode}")
                                    if smart_wallet_mode:
                                        tag_parts.append(f"sw{smart_wallet_mode}")
                                    if engine_mode:
                                        tag_parts.append(f"eng{engine_mode}")
                                    if result_segment:
                                        tag_parts.append(f"seg{result_segment}")
                                    if enable_self_learning:
                                        tag_parts.append("sl1")
                                    if self_learning_use_llm:
                                        tag_parts.append("slllm1")
                                    if enable_reinvestor:
                                        tag_parts.append("rv1")
                                    if use_ai_risk_overlay:
                                        tag_parts.append("airisk1")
                                    if src_filter:
                                        tag_parts.append(f"src{src_filter}")
                                    if min_feature_version is not None:
                                        tag_parts.append(f"fv{min_feature_version}")
                                    if wallet_tag_filter:
                                        tag_parts.append(f"wt{wallet_tag_filter}")
                                    if session_id_filter:
                                        tag_parts.append("sess")
                                    if trade_cost_bps is not None:
                                        tag_parts.append(f"tc{trade_cost_bps:.1f}")
                                    if jito_tip_bps is not None:
                                        tag_parts.append(f"jito{jito_tip_bps:.1f}")
                                    if group_id:
                                        tag_parts.append(f"gid{group_id}")

                                    tag = "_".join(tag_parts)
                                    if tag_prefix:
                                        tag = f"{tag_prefix}{tag}"

                                    extra = dict(base_extra)
                                    if run_note:
                                        extra.setdefault("_run_meta", {})
                                        extra["_run_meta"]["note"] = run_note

                                    cfg = BacktestConfig(
                                        ENTRY_SCORE_MIN=s,
                                        max_positions=max_pos,
                                        days_back=eff_days_back,
                                        regime_whitelist=regime_whitelist,
                                        label_include=label_include,
                                        label_exclude=label_exclude,
                                        limit_candidates=limit_candidates,
                                        start_ts=start_ts,
                                        end_ts=end_ts,
                                        risk_mult_override=(
                                            risk_mult if risk_mult is not None else None
                                        ),
                                        regime_risk_mode_lock=regime_mode,
                                        entry_min_score=entry_min,
                                        entry_ai_weight=ai_w,
                                        entry_model_name=entry_model_name,
                                        risk_profile=rp,
                                        exit_policy_name=ep,
                                        require_risk_capacity=args.require_risk_capacity,
                                        sample_weight_mode=sample_weight_mode,
                                        label_weights_json=label_weights_json,
                                        policy_version=policy_version,
                                        experiment_tag=experiment_tag,
                                        min_risk_capacity=min_risk_capacity,
                                        max_risk_capacity=max_risk_capacity,
                                        regime_feature_mode=regime_feature_mode,
                                        feature_mode=feature_mode,
                                        smart_wallet_mode=smart_wallet_mode,
                                        dedup_mode=dedup_mode,
                                        dedup_window_sec=dedup_window_sec,
                                        engine_mode=engine_mode,
                                        split_mode=split_mode,
                                        split_date=split_date,
                                        split_ratio=split_ratio,
                                        result_segment=result_segment,
                                        max_trades=max_trades,
                                        log_trades=log_trades,
                                        log_features=log_features,
                                        rand_seed=rand_seed,
                                        tag_prefix=tag_prefix,
                                        use_live_risk_manager=use_live_risk_manager,
                                        enable_self_learning=enable_self_learning,
                                        self_learning_use_llm=self_learning_use_llm,
                                        self_learning_interval_sec=(
                                            self_learning_interval_sec
                                        ),
                                        self_learning_min_trades=(
                                            self_learning_min_trades
                                        ),
                                        self_learning_max_tunes_per_day=(
                                            self_learning_max_tunes_per_day
                                        ),
                                        log_strategy_health=log_strategy_health,
                                        enable_reinvestor=enable_reinvestor,
                                        reinvest_percent_override=(
                                            reinvest_percent_override
                                        ),
                                        reinvest_min_profit_sol=(
                                            reinvest_min_profit_sol
                                        ),
                                        reinvest_max_single_usd=(
                                            reinvest_max_single_usd
                                        ),
                                        reinvest_dd_soft_stop_pct=(
                                            reinvest_dd_soft_stop_pct
                                        ),
                                        reinvest_dd_hard_stop_pct=(
                                            reinvest_dd_hard_stop_pct
                                        ),
                                        use_ai_risk_overlay=use_ai_risk_overlay,
                                        src_filter=src_filter,
                                        min_feature_version=min_feature_version,
                                        wallet_tag_filter=wallet_tag_filter,
                                        session_id_filter=session_id_filter,
                                        trade_cost_bps=trade_cost_bps,
                                        jito_tip_bps=jito_tip_bps,
                                        base_size_sol=base_size_sol,
                                        max_hold_sec=max_hold_sec,
                                        tag=tag,
                                        group_id=group_id,
                                        run_note=run_note,
                                        extra=extra,
                                    )
                                    configs.append(cfg)

    if args.max_configs and len(configs) > args.max_configs:
        log.info(
            "build_config_grid: limiting configs from %d to %d (BT_MAX_CONFIGS / --max-configs).",
            len(configs),
            args.max_configs,
        )
        configs = configs[: args.max_configs]

    return configs


# =============================================================================
# Pretty-print helpers / derived metrics
# =============================================================================


def _pick_sort_key_name(
    stats_list: Sequence[Mapping[str, Any]],
    override: Optional[str] = None,
) -> Optional[str]:
    if not stats_list:
        return None

    if override:
        override = override.strip()
        if override and override in stats_list[0]:
            return override

    # Preferred PnL / risk metrics
    preferred = [
        "edge_score",        # composite metric if present
        "pnl_total_pct",
        "net_pnl_pct",
        "equity_final_pct",
        "equity_final",
        "net_pnl",
        "sharpe",
        "profit_factor",
    ]
    keys = set(stats_list[0].keys())
    for k in preferred:
        if k in keys:
            return k

    # Fallback: first numeric key
    for k, v in stats_list[0].items():
        if isinstance(v, (int, float)):
            return k
    return None


def _parse_metrics_override(override_metrics: Optional[str]) -> List[str]:
    if not override_metrics:
        return []
    return _parse_csv_strs(override_metrics)


def _pick_metric_keys(
    stats_sample: Mapping[str, Any],
    override_metrics: Optional[str] = None,
) -> List[str]:
    requested = _parse_metrics_override(override_metrics)
    if requested:
        return [k for k in requested if k in stats_sample]

    preferred = [
        "n_trades",
        "n_wins",
        "n_losses",
        "win_rate",
        "avg_pnl_pct",
        "median_pnl_pct",
        "pnl_total_pct",
        "net_pnl_pct",
        "max_drawdown_pct",
        "p95_drawdown_trade_pct",
        "sharpe",
        "sortino",
        "profit_factor",
        "expectancy_pct",
        "pnl_per_trade_pct",
        "daily_pnl_pct",
        "trades_per_day",
        "edge_score",
        "pnl_dd_ratio",
        "calmar_ratio",
        "win_loss_ratio",
        "kelly_fraction",
        "kelly_safe_fraction",
        "risk_capacity_avg",
        "risk_capacity_p05",
        "risk_capacity_p95",
        "n_candidates",
        "avg_hold_sec",
        "median_hold_sec",
        "self_learning_tune_count",
        "reinvest_count",
        "reinvest_pnl_pct",
    ]
    out = [k for k in preferred if k in stats_sample]
    if not out:
        out = list(sorted(stats_sample.keys()))[:10]
    return out


def _fmt_val(v: Any) -> str:
    if isinstance(v, float):
        return f"{v: .3f}"
    if isinstance(v, int):
        return str(v)
    return str(v)


def _augment_stats(stats: Dict[str, Any]) -> None:
    """
    Add lightweight derived metrics for quant-grade analysis:
        - win_rate (if missing)
        - net_pnl_pct (alias if missing)
        - profit_factor (if missing)
        - pnl_per_trade_pct
        - expectancy_pct
        - daily_pnl_pct
        - trades_per_day
        - sortino (if downside dev present)
        - pnl_dd_ratio (PnL / |DD|)
        - win_loss_ratio (avg_win_pct / |avg_loss_pct|)
        - calmar_ratio: net_pnl_pct / |max_drawdown_pct|
        - kelly_fraction, kelly_safe_fraction (approx Kelly sizing on PnL%)
        - edge_score: composite PnL vs drawdown vs Sharpe metric
    """
    # win_rate
    try:
        if "win_rate" not in stats and "n_trades" in stats and "n_wins" in stats:
            n_trades = float(stats.get("n_trades", 0) or 0)
            n_wins = float(stats.get("n_wins", 0) or 0)
            if n_trades > 0:
                stats["win_rate"] = n_wins / n_trades
    except Exception:
        pass

    # net_pnl_pct alias
    try:
        if "net_pnl_pct" not in stats:
            for key in ("pnl_total_pct", "net_return_pct", "total_return_pct"):
                if key in stats:
                    stats["net_pnl_pct"] = float(stats[key])
                    break
    except Exception:
        pass

    # profit_factor (gross wins / gross losses)
    try:
        if "profit_factor" not in stats:
            gw = stats.get("gross_win") or stats.get("gross_win_pct")
            gl = stats.get("gross_loss") or stats.get("gross_loss_pct")
            if isinstance(gw, (int, float)) and isinstance(gl, (int, float)) and gl != 0:
                stats["profit_factor"] = float(gw) / abs(float(gl))
    except Exception:
        pass

    # pnl_per_trade_pct
    try:
        if (
            "pnl_per_trade_pct" not in stats
            and "pnl_total_pct" in stats
            and "n_trades" in stats
        ):
            n_trades = float(stats.get("n_trades", 0) or 0)
            if n_trades > 0:
                stats["pnl_per_trade_pct"] = float(stats["pnl_total_pct"]) / n_trades
    except Exception:
        pass

    # expectancy_pct ≈ avg_win_pct * win_rate + avg_loss_pct * (1 - win_rate)
    try:
        if "expectancy_pct" not in stats and "win_rate" in stats:
            wr = float(stats.get("win_rate", 0.0) or 0.0)
            avg_win = stats.get("avg_win_pct")
            avg_loss = stats.get("avg_loss_pct")
            if isinstance(avg_win, (int, float)) and isinstance(avg_loss, (int, float)):
                stats["expectancy_pct"] = (
                    wr * float(avg_win) + (1.0 - wr) * float(avg_loss)
                )
    except Exception:
        pass

    # daily_pnl_pct and trades_per_day if we have a span in days
    try:
        days = None
        for key in ("days_span", "days", "window_days"):
            if key in stats and isinstance(stats[key], (int, float)):
                days = float(stats[key])
                break

        if days and days > 0:
            if "daily_pnl_pct" not in stats and "pnl_total_pct" in stats:
                stats["daily_pnl_pct"] = float(stats["pnl_total_pct"]) / days
            if "trades_per_day" not in stats and "n_trades" in stats:
                stats["trades_per_day"] = float(stats.get("n_trades", 0) or 0.0) / days
    except Exception:
        pass

    # sortino if we have downside dev and avg_pnl_pct
    try:
        if "sortino" not in stats:
            avg = stats.get("avg_pnl_pct")
            dd = stats.get("downside_dev_pct") or stats.get("downside_dev")
            if isinstance(avg, (int, float)) and isinstance(dd, (int, float)) and dd > 0:
                stats["sortino"] = float(avg) / float(dd)
    except Exception:
        pass

    # Extra quant-grade helpers: pnl_dd_ratio, win_loss_ratio, calmar, Kelly, edge_score
    try:
        npct = float(stats.get("net_pnl_pct", stats.get("pnl_total_pct", 0.0)) or 0.0)
        dd = float(stats.get("max_drawdown_pct", 0.0) or 0.0)
        dd_abs = abs(dd)
        sh = float(stats.get("sharpe", 0.0) or 0.0)

        if dd_abs > 0 and "pnl_dd_ratio" not in stats:
            stats["pnl_dd_ratio"] = npct / dd_abs

        if dd_abs > 0 and "calmar_ratio" not in stats:
            stats["calmar_ratio"] = npct / dd_abs

        avg_win = stats.get("avg_win_pct")
        avg_loss = stats.get("avg_loss_pct")
        if (
            "win_loss_ratio" not in stats
            and isinstance(avg_win, (int, float))
            and isinstance(avg_loss, (int, float))
            and avg_loss != 0
        ):
            stats["win_loss_ratio"] = float(avg_win) / abs(float(avg_loss))

        # Kelly fraction from avg win/loss pct + win_rate (purely heuristic on PnL%)
        if (
            "kelly_fraction" not in stats
            and isinstance(avg_win, (int, float))
            and isinstance(avg_loss, (int, float))
            and stats.get("win_rate") is not None
        ):
            wr = float(stats.get("win_rate", 0.0) or 0.0)
            q = 1.0 - wr
            if avg_loss != 0:
                b = float(avg_win) / abs(float(avg_loss))
                if b > 0:
                    k = (b * wr - q) / b
                    stats["kelly_fraction"] = k
                    # conservative half-Kelly for live sizing decisions
                    stats["kelly_safe_fraction"] = max(0.0, k * 0.5)

        # edge_score: composite of pnl vs drawdown vs Sharpe
        if "edge_score" not in stats:
            denom = 1.0 + dd_abs
            base = npct / denom
            edge = base * (1.0 + 0.25 * sh)
            stats["edge_score"] = edge
    except Exception:
        pass


def print_results_table(
    configs: Sequence[BacktestConfig],
    stats_list: Sequence[Mapping[str, Any]],
    sort_key_override: Optional[str] = None,
    sort_ascending: bool = False,
    top_n: Optional[int] = None,
    metrics_override: Optional[str] = None,
) -> None:
    if not configs or not stats_list or len(configs) != len(stats_list):
        print("No results to display.")
        return

    sort_key = _pick_sort_key_name(stats_list, sort_key_override)
    if sort_key is not None:
        try:
            paired = sorted(
                zip(configs, stats_list),
                key=lambda cs: float(cs[1].get(sort_key, 0.0)),
                reverse=not sort_ascending,
            )
        except Exception:
            paired = list(zip(configs, stats_list))
    else:
        paired = list(zip(configs, stats_list))

    if top_n and top_n > 0 and len(paired) > top_n:
        paired = paired[:top_n]

    metric_keys = _pick_metric_keys(paired[0][1], metrics_override)
    if sort_key and sort_key not in metric_keys:
        metric_keys = [sort_key] + metric_keys

    headers = ["rank", "tag", "ENTRY_SCORE_MIN", "max_pos"] + metric_keys
    rows: List[List[str]] = []

    for rank, (cfg, stats) in enumerate(paired, start=1):
        row: List[str] = [
            str(rank),
            (cfg.tag or ""),
            f"{cfg.ENTRY_SCORE_MIN:.3f}",
            str(cfg.max_positions),
        ]
        for k in metric_keys:
            v = stats.get(k, "")
            row.append(_fmt_val(v))
        rows.append(row)

    col_widths = [len(h) for h in headers]
    for row in rows:
        for ix, cell in enumerate(row):
            col_widths[ix] = max(col_widths[ix], len(cell))

    def _join_line(cells: Sequence[str]) -> str:
        return " | ".join(
            cell.ljust(col_widths[i]) for i, cell in enumerate(cells)
        )

    total_width = sum(col_widths) + 3 * (len(col_widths) - 1)
    print("")
    print("Backtest results (best first):")
    print("-" * total_width)
    print(_join_line(headers))
    print("-" * total_width)
    for row in rows:
        print(_join_line(row))
    print("-" * total_width)
    print("")


# =============================================================================
# Group summary helpers (quant-grade sugar)
# =============================================================================


def _print_group_summary_for_field(
    field: str,
    configs: Sequence[BacktestConfig],
    stats_list: Sequence[Mapping[str, Any]],
) -> None:
    """
    Aggregate configs + stats by a BacktestConfig field (e.g. risk_profile)
    and print mean/sum of core PnL + risk metrics for quick coarse insights.
    """
    if not configs or not stats_list:
        return

    # Check field exists on BacktestConfig
    if not hasattr(configs[0], field):
        print(f"\n[group-summary] Skipping field '{field}': not on BacktestConfig.")
        return

    buckets: Dict[Any, List[Mapping[str, Any]]] = {}
    for cfg, stats in zip(configs, stats_list):
        key = getattr(cfg, field, None)
        # treat None as "UNSET" bucket
        if key is None:
            key = "UNSET"
        buckets.setdefault(key, []).append(stats)

    if not buckets:
        return

    metrics = [
        "n_trades",
        "win_rate",
        "pnl_total_pct",
        "net_pnl_pct",
        "max_drawdown_pct",
        "sharpe",
        "edge_score",
        "pnl_dd_ratio",
        "calmar_ratio",
        "trades_per_day",
    ]

    # Compute aggregates
    rows: List[List[str]] = []
    headers = ["group_value", "n_configs", "sum_n_trades", "avg_n_trades"] + [
        f"avg_{m}" for m in metrics if m != "n_trades"
    ]

    for key, bucket_stats in buckets.items():
        n_cfg = len(bucket_stats)
        sum_trades = 0.0
        for s in bucket_stats:
            try:
                sum_trades += float(s.get("n_trades", 0) or 0.0)
            except Exception:
                pass
        avg_trades = sum_trades / n_cfg if n_cfg > 0 else 0.0

        row: List[str] = [str(key), str(n_cfg), f"{sum_trades:.1f}", f"{avg_trades:.2f}"]



        for m in metrics:
            if m == "n_trades":
                continue
            vals: List[float] = []
            for s in bucket_stats:
                v = s.get(m)
                if isinstance(v, (int, float)):
                    vals.append(float(v))
            if vals:
                row.append(f"{(sum(vals) / len(vals)):.3f}")
            else:
                row.append("")

        rows.append(row)

    # Pretty-print table
    col_widths = [len(h) for h in headers]
    for row in rows:
        for ix, cell in enumerate(row):
            col_widths[ix] = max(col_widths[ix], len(cell))

    def _join_line(cells: Sequence[str]) -> str:
        return " | ".join(
            cell.ljust(col_widths[i]) for i, cell in enumerate(cells)
        )

    total_width = sum(col_widths) + 3 * (len(col_widths) - 1)
    print(f"\nGrouped summary by '{field}':")
    print("-" * total_width)
    print(_join_line(headers))
    print("-" * total_width)
    for row in rows:
        print(_join_line(row))
    print("-" * total_width)


def print_group_summaries(
    fields_csv: str,
    configs: Sequence[BacktestConfig],
    stats_list: Sequence[Mapping[str, Any]],
) -> None:
    fields = _parse_csv_strs(fields_csv)
    if not fields:
        return
    for f in fields:
        _print_group_summary_for_field(f, configs, stats_list)


# =============================================================================
# Export helpers
# =============================================================================


def export_json(
    path: str,
    configs: Sequence[BacktestConfig],
    stats_list: Sequence[Mapping[str, Any]],
) -> None:
    data = []
    for cfg, stats in zip(configs, stats_list):
        data.append(
            {
                "config": cfg.to_run_kwargs(),
                "stats": dict(stats),
            }
        )
    payload = {
        "meta": {
            "generated_at": datetime.utcnow().isoformat() + "Z",
            "command": " ".join(sys.argv),
            "count": len(data),
        },
        "results": data,
    }
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    print(f"Exported JSON results to {path}")


def export_csv(
    path: str,
    configs: Sequence[BacktestConfig],
    stats_list: Sequence[Mapping[str, Any]],
    sort_key_override: Optional[str] = None,
    sort_ascending: bool = False,
    top_n: Optional[int] = None,
) -> None:
    cfg_keys: List[str] = sorted(
        {k for cfg in configs for k in cfg.to_run_kwargs().keys()}
    )
    stat_keys: List[str] = sorted(
        {k for stats in stats_list for k in stats.keys()}
    )
    fieldnames = ["rank"] + cfg_keys + stat_keys

    sort_key = _pick_sort_key_name(stats_list, sort_key_override)
    if sort_key is not None:
        try:
            paired = sorted(
                zip(configs, stats_list),
                key=lambda cs: float(cs[1].get(sort_key, 0.0)),
                reverse=not sort_ascending,
            )
        except Exception:
            paired = list(zip(configs, stats_list))
    else:
        paired = list(zip(configs, stats_list))

    if top_n and top_n > 0 and len(paired) > top_n:
        paired = paired[:top_n]

    with open(path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for idx, (cfg, stats) in enumerate(paired, start=1):
            row: Dict[str, Any] = {"rank": idx}
            cfg_dict = cfg.to_run_kwargs()
            row.update(cfg_dict)
            for k in stat_keys:
                if k in stats:
                    row[k] = stats[k]
            writer.writerow(row)

    print(f"Exported CSV results to {path}")


def export_trades_json(
    path: str,
    configs: Sequence[BacktestConfig],
    stats_list: Sequence[Mapping[str, Any]],
) -> None:
    """
    Aggregate per-trade logs from all configs (if engine returned them), tagging
    trades with the config_tag so you can slice in notebooks.
    """
    all_trades: List[Dict[str, Any]] = []
    for cfg, stats in zip(configs, stats_list):
        trades = stats.get("trades")
        if not isinstance(trades, list):
            continue
        tag = cfg.tag or ""
        for t in trades:
            rec = dict(t)
            rec.setdefault("config_tag", tag)
            all_trades.append(rec)

    payload = {
        "meta": {
            "generated_at": datetime.utcnow().isoformat() + "Z",
            "command": " ".join(sys.argv),
            "count_trades": len(all_trades),
        },
        "trades": all_trades,
    }
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    print(f"Exported trades JSON to {path} ({len(all_trades)} trades)")


def _pick_best_pair(
    configs: Sequence[BacktestConfig],
    stats_list: Sequence[Mapping[str, Any]],
    sort_key_override: Optional[str],
    sort_ascending: bool,
) -> Optional[Tuple[BacktestConfig, Mapping[str, Any]]]:
    if not configs or not stats_list:
        return None

    sort_key = _pick_sort_key_name(stats_list, sort_key_override)
    if sort_key is not None:
        try:
            paired = sorted(
                zip(configs, stats_list),
                key=lambda cs: float(cs[1].get(sort_key, 0.0)),
                reverse=not sort_ascending,
            )
        except Exception:
            paired = list(zip(configs, stats_list))
    else:
        paired = list(zip(configs, stats_list))

    if not paired:
        return None

    return paired[0]


def print_best_json(
    configs: Sequence[BacktestConfig],
    stats_list: Sequence[Mapping[str, Any]],
    sort_key_override: Optional[str],
    sort_ascending: bool,
) -> None:
    """
    Print the best surviving config + stats as JSON (for direct reuse).
    """
    best = _pick_best_pair(configs, stats_list, sort_key_override, sort_ascending)
    if best is None:
        return
    best_cfg, best_stats = best
    payload = {
        "config": best_cfg.to_run_kwargs(),
        "stats": dict(best_stats),
    }
    print("\nBest config (JSON):")
    print(json.dumps(payload, indent=2, default=str))
    print("")


def export_env_best(
    path: str,
    configs: Sequence[BacktestConfig],
    stats_list: Sequence[Mapping[str, Any]],
    sort_key_override: Optional[str],
    sort_ascending: bool,
) -> None:
    """
    Export the best config as a .env-style key=value file for quick wiring
    into live env or config files.
    """
    best = _pick_best_pair(configs, stats_list, sort_key_override, sort_ascending)
    if best is None:
        print("No best config available for export_env_best.")
        return
    best_cfg, best_stats = best

    cfg_dict = best_cfg.to_run_kwargs()
    # Basic header with a few useful stats
    sort_key = _pick_sort_key_name(stats_list, sort_key_override)
    sort_val = None
    if sort_key:
        try:
            sort_val = float(best_stats.get(sort_key, 0.0) or 0.0)
        except Exception:
            sort_val = best_stats.get(sort_key)

    with open(path, "w", encoding="utf-8") as f:
        f.write("# Auto-generated by scripts/backtest.py\n")
        f.write(f"# generated_at={datetime.utcnow().isoformat()}Z\n")
        if sort_key is not None and sort_val is not None:
            f.write(f"# best_sort_key={sort_key} value={sort_val}\n")
        if best_cfg.tag:
            f.write(f"# config_tag={best_cfg.tag}\n")
        f.write("# --- Backtest best config env ---\n")
        for k in sorted(cfg_dict.keys()):
            v = cfg_dict[k]
            if isinstance(v, bool):
                v_str = "1" if v else "0"
            elif isinstance(v, (int, float)):
                v_str = str(v)
            else:
                v_str = str(v)
            f.write(f"{k}={v_str}\n")

    print(f"Exported best config as .env-style file to {path}")


def maybe_log_sweep_to_ai_db(
    args: argparse.Namespace,
    configs: Sequence[BacktestConfig],
    stats_list: Sequence[Mapping[str, Any]],
) -> None:
    """
    If ai_db.log_backtest_sweep exists and BT_SAVE_SWEEP_TO_AI_DB / --save-sweep-to-ai-db
    is set, log aggregate sweep info + per-config configs/stats into ai_db.

    This is fully optional and wrapped in try/except so it cannot break backtests.
    """
    log_fn = getattr(ai_db, "log_backtest_sweep", None)
    if not callable(log_fn):
        print(
            "NOTE: --save-sweep-to-ai-db set but ai_db.log_backtest_sweep() not found; "
            "skipping DB sweep logging."
        )
        return

    payload_meta = {
        "group_id": args.group_id or None,
        "run_note": args.run_note or None,
        "command": " ".join(sys.argv),
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "n_configs": len(configs),
        "bt_days_back": int(getattr(args, "days_back", 0) or 0),
        "src": _env_str("BT_SRC", ""),
        "regime_whitelist": _env_str("BT_REGIME_WHITELIST", ""),
        "min_feature_version": _env_int("BT_MIN_FEATURE_VERSION", 0),
    }

    try:
        log_fn(
            payload_meta,
            [cfg.to_run_kwargs() for cfg in configs],
            [dict(s) for s in stats_list],
        )
        print("Logged backtest sweep to ai_db.log_backtest_sweep().")
    except Exception as e:
        print(f"WARNING: ai_db.log_backtest_sweep() failed: {e}")


# =============================================================================
# Main backtest runner
# =============================================================================


async def _run_backtests(args: argparse.Namespace) -> None:
    """
    Orchestrate building the grid, sanity checking AI-DB, running
    backtests (optionally in parallel), post-filtering, and exports.
    """
    # Build configs
    configs = build_config_grid(args)
    if not configs:
        print("No configs built; check --score-min / --max-positions.")
        return

    # Optional: shuffle & sample (research QoL, useful for giant grids)
    if args.seed:
        random.seed(args.seed)

    if args.shuffle_configs:
        random.shuffle(configs)

    if 0.0 < args.sample_config_fraction < 1.0 and len(configs) > 1:
        k = max(1, int(len(configs) * args.sample_config_fraction))
        configs = configs[:k]
        print(
            f"Sampling config grid: using {k} configs "
            f"(fraction={args.sample_config_fraction:.3f})."
        )

    if args.dry_run:
        print(f"DRY RUN: built {len(configs)} config(s). Example:")
        example = configs[0]
        print(json.dumps(example.to_run_kwargs(), indent=2, default=str))
        return

    # Quick DB sanity check
    try:
        backend_desc = ai_db.describe_backend()
        print(f"DB backend: {backend_desc}")
    except Exception as e:
        print(f"WARNING: ai_db.describe_backend() failed: {e}")

    try:
        ok = ai_db.ping()
    except Exception as e:
        print(f"ERROR: ai_db.ping() raised exception: {e}")
        return

    if not ok:
        print("ERROR: ai_db.ping() failed – cannot reach Postgres.")
        return

    sample_cfg = configs[0]

    # Quant-grade: quick baseline stats from ai_db for context
    try:
        basic_stats = ai_db.fetch_basic_stats(
            days_back=sample_cfg.days_back,
            src=sample_cfg.src_filter,
            regime=(
                sample_cfg.regime_whitelist[0]
                if sample_cfg.regime_whitelist
                else None
            ),
            min_feature_version=sample_cfg.min_feature_version,
        )
        label_dist = ai_db.fetch_label_distribution(
            days_back=sample_cfg.days_back,
            src=sample_cfg.src_filter,
            regime=(
                sample_cfg.regime_whitelist[0]
                if sample_cfg.regime_whitelist
                else None
            ),
            min_feature_version=sample_cfg.min_feature_version,
        )
        perf_regime = ai_db.fetch_perf_by_regime(
            days_back=sample_cfg.days_back,
            min_feature_version=sample_cfg.min_feature_version,
            src=sample_cfg.src_filter,
        )
        perf_src = ai_db.fetch_perf_by_src(
            days_back=sample_cfg.days_back,
            min_feature_version=sample_cfg.min_feature_version,
            regime=(
                sample_cfg.regime_whitelist[0]
                if sample_cfg.regime_whitelist
                else None
            ),
        )
        perf_policy = ai_db.fetch_perf_by_policy_version(
            days_back=sample_cfg.days_back,
            min_feature_version=sample_cfg.min_feature_version,
            src=sample_cfg.src_filter,
            regime=(
                sample_cfg.regime_whitelist[0]
                if sample_cfg.regime_whitelist
                else None
            ),
        )
        perf_exp = ai_db.fetch_perf_by_experiment_tag(
            days_back=sample_cfg.days_back,
            min_feature_version=sample_cfg.min_feature_version,
            src=sample_cfg.src_filter,
            regime=(
                sample_cfg.regime_whitelist[0]
                if sample_cfg.regime_whitelist
                else None
            ),
        )
        coverage = ai_db.fetch_outcome_coverage(
            days_back=sample_cfg.days_back,
            min_feature_version=sample_cfg.min_feature_version,
            src=sample_cfg.src_filter,
            regime=(
                sample_cfg.regime_whitelist[0]
                if sample_cfg.regime_whitelist
                else None
            ),
        )
        print("\nAI-DB baseline stats (pre-backtest, first regime/src slice):")
        print("  basic              :", json.dumps(basic_stats, indent=2, default=str))
        print("  labels             :", json.dumps(label_dist, indent=2, default=str))
        print("  perf_by_regime     :", json.dumps(perf_regime, indent=2, default=str))
        print("  perf_by_src        :", json.dumps(perf_src, indent=2, default=str))
        print("  perf_by_policy_ver :", json.dumps(perf_policy, indent=2, default=str))
        print("  perf_by_experiment :", json.dumps(perf_exp, indent=2, default=str))
        print("  outcome_coverage   :", json.dumps(coverage, indent=2, default=str))
        print("")
    except Exception as e:
        print(f"WARNING: ai_db quick baseline stats failed: {e}")

    print(
        f"Running backtest on {len(configs)} config(s)...\n"
        f"  days_back         : {sample_cfg.days_back}\n"
        f"  parallel          : {bool(args.parallel)} (limit={args.parallel_limit})\n"
        f"  shuffle_configs   : {bool(args.shuffle_configs)}\n"
        f"  sample_fraction   : {args.sample_config_fraction}\n"
        f"  regime_whitelist  : {sample_cfg.regime_whitelist}\n"
        f"  label_include     : {sample_cfg.label_include}\n"
        f"  label_exclude     : {sample_cfg.label_exclude}\n"
        f"  risk_mult_override: {sample_cfg.risk_mult_override}\n"
        f"  regime_risk_mode  : {sample_cfg.regime_risk_mode_lock}\n"
        f"  min_risk_capacity : {sample_cfg.min_risk_capacity}\n"
        f"  max_risk_capacity : {sample_cfg.max_risk_capacity}\n"
        f"  feature_mode      : {sample_cfg.feature_mode}\n"
        f"  smart_wallet_mode : {sample_cfg.smart_wallet_mode}\n"
        f"  regime_feature_md : {sample_cfg.regime_feature_mode}\n"
        f"  dedup_mode        : {sample_cfg.dedup_mode}\n"
        f"  dedup_window_sec  : {sample_cfg.dedup_window_sec}\n"
        f"  engine_mode       : {sample_cfg.engine_mode}\n"
        f"  split_mode        : {sample_cfg.split_mode}\n"
        f"  result_segment    : {sample_cfg.result_segment}\n"
        f"  max_trades        : {sample_cfg.max_trades}\n"
        f"  min_trades        : {args.min_trades}\n"
        f"  require_trades    : {bool(args.require_trades)}\n"
        f"  entry_min_score   : {sample_cfg.entry_min_score}\n"
        f"  entry_ai_weight   : {sample_cfg.entry_ai_weight}\n"
        f"  risk_profile      : {sample_cfg.risk_profile}\n"
        f"  exit_policy_name  : {sample_cfg.exit_policy_name}\n"
        f"  require_risk_cap  : {sample_cfg.require_risk_capacity}\n"
        f"  use_live_risk_mgr : {sample_cfg.use_live_risk_manager}\n"
        f"  self_learning     : {sample_cfg.enable_self_learning}\n"
        f"  sl_use_llm        : {sample_cfg.self_learning_use_llm}\n"
        f"  sl_interval_sec   : {sample_cfg.self_learning_interval_sec}\n"
        f"  sl_min_trades     : {sample_cfg.self_learning_min_trades}\n"
        f"  sl_max_tunes_day  : {sample_cfg.self_learning_max_tunes_per_day}\n"
        f"  reinvestor        : {sample_cfg.enable_reinvestor}\n"
        f"  reinvest_pct_ovr  : {sample_cfg.reinvest_percent_override}\n"
        f"  reinvest_min_sol  : {sample_cfg.reinvest_min_profit_sol}\n"
        f"  reinvest_max_usd  : {sample_cfg.reinvest_max_single_usd}\n"
        f"  reinvest_dd_soft  : {sample_cfg.reinvest_dd_soft_stop_pct}\n"
        f"  reinvest_dd_hard  : {sample_cfg.reinvest_dd_hard_stop_pct}\n"
        f"  use_ai_risk_model : {sample_cfg.use_ai_risk_overlay}\n"
        f"  src_filter        : {sample_cfg.src_filter}\n"
        f"  wallet_tag_filter : {sample_cfg.wallet_tag_filter}\n"
        f"  session_id_filter : {sample_cfg.session_id_filter}\n"
        f"  min_feat_version  : {sample_cfg.min_feature_version}\n"
        f"  trade_cost_bps    : {sample_cfg.trade_cost_bps}\n"
        f"  jito_tip_bps      : {sample_cfg.jito_tip_bps}\n"
        f"  base_size_sol     : {sample_cfg.base_size_sol}\n"
        f"  max_hold_sec      : {sample_cfg.max_hold_sec}\n"
        f"  group_id          : {sample_cfg.group_id}\n"
        f"  base_extra        : {bool(args.base_config_json)}\n"
    )

    cfg_dicts = [cfg.to_run_kwargs() for cfg in configs]
    stats_list: List[Dict[str, Any]] = []

    async def _run_single(idx: int, cfg: BacktestConfig, cfg_kwargs: Dict[str, Any]) -> Dict[str, Any]:
        try:
            res = await run_backtest(cfg_kwargs)
            stats_dict: Dict[str, Any] = dict(res)
            _augment_stats(stats_dict)
            return stats_dict
        except Exception as e:
            log.exception("Backtest config failed (tag=%s)", cfg.tag)
            return {"error": str(e)}

    if args.parallel and len(cfg_dicts) > 1:
        parallel_limit = max(1, int(args.parallel_limit or 1))
        sem = asyncio.Semaphore(parallel_limit)

        async def _guarded_run(i: int, cfg: BacktestConfig, cfg_kwargs: Dict[str, Any]) -> Dict[str, Any]:
            async with sem:
                return await _run_single(i, cfg, cfg_kwargs)

        coros = [
            _guarded_run(i, cfg, cfg_kwargs)
            for i, (cfg, cfg_kwargs) in enumerate(zip(configs, cfg_dicts))
        ]
        try:
            results = await asyncio.gather(*coros, return_exceptions=False)
        except Exception as e:
            print(f"Backtest error (parallel gather): {e}")
            return

        stats_list = list(results)
    else:
        for i, (cfg, cfg_kwargs) in enumerate(zip(configs, cfg_dicts)):
            print(f"Running config {i+1}/{len(configs)} → {cfg.tag or ''}")
            stats_list.append(await _run_single(i, cfg, cfg_kwargs))

    # Drop configs that fully failed
    ok_configs: List[BacktestConfig] = []
    ok_stats: List[Mapping[str, Any]] = []
    for cfg, stats in zip(configs, stats_list):
        if "error" in stats and len(stats) == 1:
            print(f"Config {cfg.tag or ''} failed: {stats['error']}")
            continue
        ok_configs.append(cfg)
        ok_stats.append(stats)

    if not ok_configs:
        print("All backtests failed – nothing to display.")
        return

    # Apply result-level filters
    filtered_configs: List[BacktestConfig] = []
    filtered_stats: List[Mapping[str, Any]] = []

    min_trades = int(args.min_trades or 0)
    require_trades = bool(args.require_trades)

    min_win_rate = args.min_win_rate
    max_drawdown = args.max_drawdown
    max_p95_dd_trade = args.max_p95_drawdown_trade_pct
    min_sharpe = args.min_sharpe
    min_profit_factor = args.min_profit_factor
    min_avg_pnl_pct = args.min_avg_pnl_pct
    min_sortino = args.min_sortino
    min_daily_pnl_pct = args.min_daily_pnl_pct
    # defensive: support historical typo min_equity_final_pcnt as well
    min_equity_final_pct = (
        args.min_equity_final_pcnt
        if hasattr(args, "min_equity_final_pcnt")
        else args.min_equity_final_pct
    )
    min_net_pnl_pct = args.min_net_pnl_pct
    min_reinvest_count = int(args.min_reinvest_count or 0)
    min_self_learn_tunes = int(args.min_self_learn_tunes or 0)
    min_edge_score = args.min_edge_score
    min_pnl_dd_ratio = args.min_pnl_dd_ratio
    min_calmar = args.min_calmar
    min_win_loss_ratio = args.min_win_loss_ratio
    min_expectancy_pct = args.min_expectancy_pct
    min_kelly_fraction = args.min_kelly_fraction
    min_kelly_safe_fraction = args.min_kelly_safe_fraction
    min_trades_per_day = args.min_trades_per_day

    # Normalize disabled thresholds
    if min_win_rate < 0.0:
        min_win_rate = None
    if max_drawdown <= 0.0:
        max_drawdown = None
    if max_p95_dd_trade <= 0.0:
        max_p95_dd_trade = None
    if min_sharpe <= 0.0:
        min_sharpe = None
    if min_profit_factor <= 0.0:
        min_profit_factor = None
    if min_avg_pnl_pct <= 0.0:
        min_avg_pnl_pct = None
    if min_sortino <= 0.0:
        min_sortino = None
    if min_daily_pnl_pct <= 0.0:
        min_daily_pnl_pct = None
    if min_equity_final_pct <= 0.0:
        min_equity_final_pct = None
    if min_net_pnl_pct <= 0.0:
        min_net_pnl_pct = None
    if min_edge_score <= 0.0:
        min_edge_score = None
    if min_pnl_dd_ratio <= 0.0:
        min_pnl_dd_ratio = None
    if min_calmar <= 0.0:
        min_calmar = None
    if min_win_loss_ratio <= 0.0:
        min_win_loss_ratio = None
    if min_expectancy_pct <= 0.0:
        min_expectancy_pct = None
    if min_kelly_fraction <= 0.0:
        min_kelly_fraction = None
    if min_kelly_safe_fraction <= 0.0:
        min_kelly_safe_fraction = None
    if min_trades_per_day <= 0.0:
        min_trades_per_day = None

    for cfg, stats in zip(ok_configs, ok_stats):
        n_trades = int(stats.get("n_trades", 0) or 0)
        if require_trades and n_trades == 0:
            continue
        if min_trades > 0 and n_trades < min_trades:
            continue

        # win_rate filter
        if min_win_rate is not None:
            wr = stats.get("win_rate")
            if wr is None:
                try:
                    n_wins = float(stats.get("n_wins", 0) or 0)
                    wr = n_wins / float(n_trades) if n_trades > 0 else 0.0
                except Exception:
                    wr = 0.0
            if wr is None or wr < min_win_rate:
                continue

        # max_drawdown filter (compare absolute value)
        if max_drawdown is not None:
            dd = float(stats.get("max_drawdown_pct", 0.0) or 0.0)
            if abs(dd) > float(max_drawdown):
                continue

        # p95_drawdown_trade_pct filter
        if max_p95_dd_trade is not None:
            p95dd = float(stats.get("p95_drawdown_trade_pct", 0.0) or 0.0)
            if abs(p95dd) > float(max_p95_dd_trade):
                continue

        # Sharpe filter
        if min_sharpe is not None:
            sh = float(stats.get("sharpe", 0.0) or 0.0)
            if sh < float(min_sharpe):
                continue

        # Profit factor filter
        if min_profit_factor is not None:
            pf = float(stats.get("profit_factor", 0.0) or 0.0)
            if pf < float(min_profit_factor):
                continue

        # avg_pnl_pct filter
        if min_avg_pnl_pct is not None:
            avg_pnl = float(stats.get("avg_pnl_pct", 0.0) or 0.0)
            if avg_pnl < float(min_avg_pnl_pct):
                continue

        # Sortino filter
        if min_sortino is not None:
            srt = float(stats.get("sortino", 0.0) or 0.0)
            if srt < float(min_sortino):
                continue

        # daily_pnl_pct filter
        if min_daily_pnl_pct is not None:
            dp = float(stats.get("daily_pnl_pct", 0.0) or 0.0)
            if dp < float(min_daily_pnl_pct):
                continue

        # equity_final_pct filter
        if min_equity_final_pct is not None:
            ef = float(stats.get("equity_final_pct", 0.0) or 0.0)
            if ef < float(min_equity_final_pct):
                continue

        # net_pnl_pct filter
        if min_net_pnl_pct is not None:
            npct = float(stats.get("net_pnl_pct", 0.0) or 0.0)
            if npct < float(min_net_pnl_pct):
                continue

        # reinvest_count filter
        if min_reinvest_count > 0:
            reinv = int(stats.get("reinvest_count", 0) or 0)
            if reinv < min_reinvest_count:
                continue

        # self_learning_tune_count filter
        if min_self_learn_tunes > 0:
            tunes = int(stats.get("self_learning_tune_count", 0) or 0)
            if tunes < min_self_learn_tunes:
                continue

        # edge_score filter
        if min_edge_score is not None:
            es = float(stats.get("edge_score", 0.0) or 0.0)
            if es < float(min_edge_score):
                continue

        # pnl_dd_ratio filter
        if min_pnl_dd_ratio is not None:
            v = float(stats.get("pnl_dd_ratio", 0.0) or 0.0)
            if v < float(min_pnl_dd_ratio):
                continue

        # calmar_ratio filter
        if min_calmar is not None:
            v = float(stats.get("calmar_ratio", 0.0) or 0.0)
            if v < float(min_calmar):
                continue

        # win_loss_ratio filter
        if min_win_loss_ratio is not None:
            v = float(stats.get("win_loss_ratio", 0.0) or 0.0)
            if v < float(min_win_loss_ratio):
                continue

        # expectancy_pct filter
        if min_expectancy_pct is not None:
            v = float(stats.get("expectancy_pct", 0.0) or 0.0)
            if v < float(min_expectancy_pct):
                continue

        # Kelly fraction filter
        if min_kelly_fraction is not None:
            v = float(stats.get("kelly_fraction", 0.0) or 0.0)
            if v < float(min_kelly_fraction):
                continue

        # Kelly safe fraction filter (half-Kelly guardrail)
        if min_kelly_safe_fraction is not None:
            v = float(stats.get("kelly_safe_fraction", 0.0) or 0.0)
            if v < float(min_kelly_safe_fraction):
                continue

        # trades_per_day filter
        if min_trades_per_day is not None:
            tpd = stats.get("trades_per_day")
            if tpd is None:
                # compute from n_trades / days if we can
                days = None
                for key in ("days_span", "days", "window_days"):
                    if key in stats and isinstance(stats[key], (int, float)):
                        days = float(stats[key])
                        break
                if days and days > 0:
                    tpd = float(n_trades) / days
            if tpd is None or float(tpd) < float(min_trades_per_day):
                continue

        filtered_configs.append(cfg)
        filtered_stats.append(stats)

    if not filtered_configs:
        print("All configs filtered out by min-trades / result filters.")
        return

    sort_key_override = args.sort_key.strip() or None
    sort_asc = bool(args.sort_asc)
    top_n = args.top_n if args.top_n > 0 else None
    metrics_override = args.metrics.strip() or None

    print_results_table(
        filtered_configs,
        filtered_stats,
        sort_key_override=sort_key_override,
        sort_ascending=sort_asc,
        top_n=top_n,
        metrics_override=metrics_override,
    )

    # Group summaries (optional)
    if args.group_summary_by:
        print_group_summaries(args.group_summary_by, filtered_configs, filtered_stats)

    # Verbose dump
    if args.verbose:
        for cfg, stats in zip(filtered_configs, filtered_stats):
            print(f"--- {cfg.tag or ''} ---")
            print("config:", json.dumps(cfg.to_run_kwargs(), indent=2, default=str))
            print("stats :", json.dumps(stats, indent=2, default=str))
            print("")

    # Optional: print best config as JSON
    if args.print_best_json:
        print_best_json(
            filtered_configs,
            filtered_stats,
            sort_key_override=sort_key_override,
            sort_ascending=sort_asc,
        )

    # Exports
    if args.export_json:
        export_json(args.export_json, filtered_configs, filtered_stats)
    if args.export_csv:
        export_csv(
            args.export_csv,
            filtered_configs,
            filtered_stats,
            sort_key_override=sort_key_override,
            sort_ascending=sort_asc,
            top_n=top_n,
        )
    if args.export_trades_json:
        export_trades_json(args.export_trades_json, filtered_configs, filtered_stats)
    if args.export_env_best:
        export_env_best(
            args.export_env_best,
            filtered_configs,
            filtered_stats,
            sort_key_override=sort_key_override,
            sort_ascending=sort_asc,
        )

    # Optional: log sweep into ai_db if backend supports it
    if args.save_sweep_to_ai_db:
        maybe_log_sweep_to_ai_db(args, filtered_configs, filtered_stats)


def main() -> None:
    parser = build_arg_parser()
    args = parser.parse_args()
    try:
        asyncio.run(_run_backtests(args))
    except KeyboardInterrupt:
        print("Interrupted by user.", file=sys.stderr)


if __name__ == "__main__":
    main()
