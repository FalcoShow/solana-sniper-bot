from __future__ import annotations

"""
scripts/backtest.py

God-Tier Backtest Runner (V3+++ / Quant-Grade / Master Roadmap Phases 5–7)
=========================================================================

Role
----
Grid-sweep orchestrator for utils_pkg.backtest_engine.run_backtest(...).

Key capabilities
----------------
✅ Grid search on ENTRY_SCORE_MIN, risk multipliers, regime modes, profiles, exits, etc.
✅ Fully env-driven default lists (BT_*), override via CLI.
✅ Resume mode: --resume-json <prior_export.json> to skip completed tags.
✅ Per-config timeout: --timeout-sec to prevent hangs.
✅ Tag include/exclude regex: --include-tag / --exclude-tag.
✅ True random sampling: --sample-config-fraction uses random.sample (seeded).
✅ Boolean flags support both --flag and --no-flag via BooleanOptionalAction.
✅ ai_db calls are guarded + support sync OR async helpers (won’t crash sweep).
✅ Robust parallel runner (order preserved, progress, abort-on-max-failures).
✅ Cleaner filtering/sorting/table/export, robust numeric handling (NaN/inf safe).
✅ Reinvestor + self-learning + AI risk overlay fully wired into config.
✅ Deterministic + safe: never mutates live knobs; only reads env and writes to explicit export paths.
✅ Atomic exports: JSON/CSV/trades/env-best writes are crash-safe.

Notes
-----
- This runner does not implement the backtest itself; it *calls* run_backtest.
- run_backtest may accept:
    (a) run_backtest(cfg_dict)
    (b) run_backtest(**kwargs)
  and may be sync or async. We detect and adapt.

Recommended quick compile check (PowerShell):
  py -m py_compile scripts\\backtest.py
"""

import argparse
import asyncio
import csv
import hashlib
import inspect
import json
import math
import os
import platform
import random
import re
import sys
import tempfile
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

from logging_setup import get_logger
from utils_pkg.backtest_engine import run_backtest

# ai_db is optional (guarded everywhere)
try:
    from utils_pkg import ai_db  # type: ignore
except Exception:  # pragma: no cover
    ai_db = None  # type: ignore


# logger: keep compatible with get_logger() variants
try:
    log = get_logger(__name__)
except TypeError:  # pragma: no cover
    log = get_logger()


# =============================================================================
# Env helpers
# =============================================================================


def _env_present(key: str) -> bool:
    v = os.getenv(key)
    return v is not None and str(v).strip() != ""


def _env_str(key: str, default: str = "") -> str:
    return str(os.getenv(key, default) or "").strip()


def _env_int(key: str, default: int) -> int:
    raw = os.getenv(key)
    if raw is None or str(raw).strip() == "":
        return default
    try:
        return int(str(raw).strip())
    except Exception:
        return default


def _env_float(key: str, default: float) -> float:
    raw = os.getenv(key)
    if raw is None or str(raw).strip() == "":
        return default
    try:
        return float(str(raw).strip())
    except Exception:
        return default


def _env_bool(key: str, default: bool = False) -> bool:
    raw = os.getenv(key)
    if raw is None:
        return default
    return str(raw).strip().lower() in {"1", "true", "yes", "y", "on"}


# =============================================================================
# Parsing helpers (CSV + simple ranges)
# =============================================================================

_RANGE_RE = re.compile(
    r"""
    ^
    \s*
    (?P<a>-?\d+(?:\.\d+)?)
    \s*
    (?:\.\.|:)
    \s*
    (?P<b>-?\d+(?:\.\d+)?)
    (?:\s*:\s*(?P<step>-?\d+(?:\.\d+)?))?
    \s*
    $
    """,
    re.VERBOSE,
)


def _parse_csv_tokens(s: str) -> List[str]:
    out: List[str] = []
    for part in (s or "").replace(";", ",").split(","):
        p = part.strip()
        if p:
            out.append(p)
    return out


def _expand_range_token(token: str, *, as_int: bool) -> List[float]:
    """
    Accepts:
      - "0.55..0.70:0.05"
      - "0.55:0.70:0.05"
      - "1..5" (step inferred: 1 or 0.01 depending on int/float)
    Returns a list of floats (caller can cast to int if needed).
    """
    m = _RANGE_RE.match(token)
    if not m:
        return []
    a = float(m.group("a"))
    b = float(m.group("b"))
    step_raw = m.group("step")

    if step_raw is None or str(step_raw).strip() == "":
        step = 1.0 if as_int else 0.01
    else:
        step = float(step_raw)

    if step == 0:
        return []

    # normalize direction
    if a < b and step < 0:
        step = abs(step)
    if a > b and step > 0:
        step = -abs(step)

    # build inclusive range (bounded iterations)
    out: List[float] = []
    cur = a
    max_iter = 50_000

    def _done(x: float) -> bool:
        return x > b + 1e-12 if step > 0 else x < b - 1e-12

    it = 0
    while not _done(cur) and it < max_iter:
        out.append(cur)
        cur += step
        it += 1

    # ensure b included if very close
    if out:
        if abs(out[-1] - b) > 1e-9:
            out.append(b)
    else:
        out = [a, b]

    return out


def _parse_csv_floats(s: str) -> List[float]:
    out: List[float] = []
    for tok in _parse_csv_tokens(s):
        expanded = _expand_range_token(tok, as_int=False)
        if expanded:
            for v in expanded:
                if math.isfinite(v):
                    out.append(float(v))
            continue
        try:
            v = float(tok)
            if math.isfinite(v):
                out.append(v)
        except Exception:
            continue
    return out


def _parse_csv_ints(s: str) -> List[int]:
    out: List[int] = []
    for tok in _parse_csv_tokens(s):
        expanded = _expand_range_token(tok, as_int=True)
        if expanded:
            for v in expanded:
                if math.isfinite(v):
                    out.append(int(round(v)))
            continue
        try:
            out.append(int(tok))
        except Exception:
            continue
    return out


def _parse_csv_strs(s: str) -> List[str]:
    return _parse_csv_tokens(s)


def _parse_date(s: str) -> Optional[datetime]:
    s = (s or "").strip()
    if not s:
        return None
    try:
        if "T" in s:
            return datetime.fromisoformat(s)
        return datetime.strptime(s, "%Y-%m-%d")
    except Exception:
        return None


def _safe_float(x: Any, default: float = 0.0) -> float:
    try:
        if x is None:
            return default
        v = float(x)
        if not math.isfinite(v):
            return default
        return v
    except Exception:
        return default


def _safe_int(x: Any, default: int = 0) -> int:
    try:
        if x is None:
            return default
        return int(x)
    except Exception:
        return default


def _disabled_threshold(x: Optional[float]) -> bool:
    """
    Treat <=0, None, NaN, +/-inf as disabled.
    """
    if x is None:
        return True
    try:
        v = float(x)
    except Exception:
        return True
    if not math.isfinite(v):
        return True
    return v <= 0.0


# =============================================================================
# Atomic write helpers
# =============================================================================


def _atomic_write_text(path: str, text: str) -> None:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix=p.name + ".", suffix=".tmp", dir=str(p.parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="") as f:
            f.write(text)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)
    finally:
        try:
            if os.path.exists(tmp):
                os.remove(tmp)
        except Exception:
            pass


def _atomic_write_json(path: str, payload: Any) -> None:
    text = json.dumps(payload, ensure_ascii=False, indent=2, default=str)
    _atomic_write_text(path, text)


def _read_json(path: str) -> Optional[Dict[str, Any]]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else None
    except Exception:
        return None


# =============================================================================
# Argparse helpers (bool flags with tri-state support)
# =============================================================================


def _add_bool_flag(
    p: argparse.ArgumentParser,
    flag_name: str,
    env_key: str,
    default: Optional[bool],
    help_text: str,
) -> None:
    """
    Adds --<flag-name> and --no-<flag-name> via BooleanOptionalAction.

    Tri-state defaults:
      - If env_key is set: default becomes env value (True/False)
      - Else: default is `default` (can be None to mean "unspecified")
    """
    dest = flag_name.replace("-", "_")

    if _env_present(env_key):
        effective_default: Optional[bool] = _env_bool(env_key, False)
    else:
        effective_default = default

    p.add_argument(
        f"--{flag_name}",
        dest=dest,
        default=effective_default,
        action=argparse.BooleanOptionalAction,
        help=help_text,
    )


# =============================================================================
# BacktestConfig
# =============================================================================


@dataclass
class BacktestConfig:
    # Core knobs for sweeps
    ENTRY_SCORE_MIN: float
    max_positions: int

    # Global backtest filters
    days_back: int
    regime_whitelist: Optional[List[str]] = None
    label_include: Optional[List[str]] = None
    label_exclude: Optional[List[str]] = None
    limit_candidates: Optional[int] = None

    # Optional date range
    start_ts: Optional[datetime] = None
    end_ts: Optional[datetime] = None

    # Risk / regime
    risk_mult_override: Optional[float] = None
    regime_risk_mode_lock: Optional[int] = None  # 0=risk_off,1=normal,2=risk_on

    # Strategy / AI
    entry_min_score: Optional[float] = None
    entry_ai_weight: Optional[float] = None
    entry_model_name: Optional[str] = None
    risk_profile: Optional[str] = None
    exit_policy_name: Optional[str] = None
    require_risk_capacity: Optional[bool] = None

    # Weighting
    sample_weight_mode: Optional[str] = None
    label_weights_json: Optional[str] = None

    # AI policy meta
    policy_version: Optional[str] = None
    experiment_tag: Optional[str] = None

    # Risk capacity + regime feature mode
    min_risk_capacity: Optional[float] = None
    max_risk_capacity: Optional[float] = None
    regime_feature_mode: Optional[str] = None

    # Feature / smart-wallet / dedup
    feature_mode: Optional[str] = None
    smart_wallet_mode: Optional[str] = None
    dedup_mode: Optional[str] = None
    dedup_window_sec: Optional[int] = None

    # Engine / split
    engine_mode: Optional[str] = None
    split_mode: Optional[str] = None
    split_date: Optional[datetime] = None
    split_ratio: Optional[float] = None
    result_segment: Optional[str] = None

    # Execution / logging
    max_trades: Optional[int] = None
    log_trades: Optional[bool] = None
    log_features: Optional[bool] = None
    rand_seed: Optional[int] = None
    tag_prefix: Optional[str] = None
    use_live_risk_manager: Optional[bool] = None

    # Self-learning strategy
    enable_self_learning: Optional[bool] = None
    self_learning_use_llm: Optional[bool] = None
    self_learning_interval_sec: Optional[int] = None
    self_learning_min_trades: Optional[int] = None
    self_learning_max_tunes_per_day: Optional[int] = None
    log_strategy_health: Optional[bool] = None

    # Reinvestor
    enable_reinvestor: Optional[bool] = None
    reinvest_percent_override: Optional[float] = None
    reinvest_min_profit_sol: Optional[float] = None
    reinvest_max_single_usd: Optional[float] = None
    reinvest_dd_soft_stop_pct: Optional[float] = None
    reinvest_dd_hard_stop_pct: Optional[float] = None

    # AI risk overlay
    use_ai_risk_overlay: Optional[bool] = None

    # ai_db slicing
    src_filter: Optional[str] = None
    min_feature_version: Optional[int] = None
    wallet_tag_filter: Optional[str] = None
    session_id_filter: Optional[str] = None

    # Microstructure / costs
    trade_cost_bps: Optional[float] = None
    jito_tip_bps: Optional[float] = None
    base_size_sol: Optional[float] = None
    max_hold_sec: Optional[int] = None

    # Tag + run meta
    tag: Optional[str] = None
    group_id: Optional[str] = None
    run_note: Optional[str] = None

    # Passthrough for future expansion
    extra: Optional[Dict[str, Any]] = None

    def to_run_kwargs(self) -> Dict[str, Any]:
        cfg: Dict[str, Any] = {
            "ENTRY_SCORE_MIN": float(self.ENTRY_SCORE_MIN),
            "max_positions": int(self.max_positions),
            "days_back": int(self.days_back),
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

        # Strategy / AI
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

        # Weighting
        if self.sample_weight_mode:
            cfg["BT_SAMPLE_WEIGHT_MODE"] = self.sample_weight_mode
        if self.label_weights_json:
            cfg["BT_LABEL_WEIGHTS_JSON"] = self.label_weights_json

        # AI policy meta
        if self.policy_version:
            cfg["ai_policy_version"] = self.policy_version
        if self.experiment_tag:
            cfg["ai_experiment_tag"] = self.experiment_tag

        # Risk capacity / regime feature
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

        # Self-learning
        if self.enable_self_learning is not None:
            cfg["BT_ENABLE_SELF_LEARNING"] = bool(self.enable_self_learning)
        if self.self_learning_use_llm is not None:
            cfg["BT_SELF_LEARN_USE_LLM"] = bool(self.self_learning_use_llm)
        if self.self_learning_interval_sec is not None:
            cfg["BT_SELF_LEARN_INTERVAL_SEC"] = int(self.self_learning_interval_sec)
        if self.self_learning_min_trades is not None:
            cfg["BT_SELF_LEARN_MIN_TRADES"] = int(self.self_learning_min_trades)
        if self.self_learning_max_tunes_per_day is not None:
            cfg["BT_SELF_LEARN_MAX_TUNES_PER_DAY"] = int(self.self_learning_max_tunes_per_day)
        if self.log_strategy_health is not None:
            cfg["BT_LOG_STRATEGY_HEALTH"] = bool(self.log_strategy_health)

        # Reinvestor
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

        # AI risk overlay
        if self.use_ai_risk_overlay is not None:
            cfg["BT_USE_AI_RISK_MODEL"] = bool(self.use_ai_risk_overlay)

        # ai_db slicing
        if self.src_filter:
            cfg["BT_SRC"] = self.src_filter
        if self.min_feature_version is not None:
            cfg["BT_MIN_FEATURE_VERSION"] = int(self.min_feature_version)
        if self.wallet_tag_filter:
            cfg["BT_WALLET_TAG"] = self.wallet_tag_filter
        if self.session_id_filter:
            cfg["BT_SESSION_ID"] = self.session_id_filter

        # Costs / microstructure
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
# CLI
# =============================================================================


def build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Quant-grade backtest runner (grid sweep).")

    # Core sweeps
    p.add_argument("--score-min", default=_env_str("BT_SCORE_MIN_LIST", "0.55,0.60,0.65"))
    p.add_argument("--max-positions", default=_env_str("BT_MAX_POS_LIST", "1,2"))

    # Risk/regime sweeps
    p.add_argument("--risk-mult", default=_env_str("BT_RISK_MULT_LIST", ""))
    p.add_argument("--regime-risk-mode", default=_env_str("BT_REGIME_RISK_MODE_LIST", ""))

    # Strategy/AI sweeps
    p.add_argument("--entry-min-score", default=_env_str("BT_ENTRY_MIN_SCORE_LIST", ""))
    p.add_argument("--entry-ai-weight", default=_env_str("BT_ENTRY_AI_WEIGHT_LIST", ""))
    p.add_argument("--entry-model", default=_env_str("BT_ENTRY_MODEL_NAME", ""))
    p.add_argument("--risk-profile", default=_env_str("BT_RISK_PROFILE_LIST", ""))
    p.add_argument("--exit-policy", default=_env_str("BT_EXIT_POLICY_LIST", ""))

    # Date/data filters
    p.add_argument("--days-back", type=int, default=_env_int("BT_DAYS_BACK", 14))
    p.add_argument("--start-date", default=_env_str("BT_START_DATE", ""))
    p.add_argument("--end-date", default=_env_str("BT_END_DATE", ""))
    p.add_argument("--regime", default=_env_str("BT_REGIME_WHITELIST", ""))
    p.add_argument("--label-include", default=_env_str("BT_LABEL_INCLUDE", ""))
    p.add_argument("--label-exclude", default=_env_str("BT_LABEL_EXCLUDE", ""))
    p.add_argument("--limit-candidates", type=int, default=_env_int("BT_LIMIT_CANDIDATES", 0))

    # Weighting
    p.add_argument("--sample-weight-mode", default=_env_str("BT_SAMPLE_WEIGHT_MODE", ""))
    p.add_argument("--label-weights-json", default=_env_str("BT_LABEL_WEIGHTS_JSON", ""))

    # AI policy filters
    p.add_argument("--policy-version", default=_env_str("BT_POLICY_VERSION", ""))
    p.add_argument("--experiment-tag", default=_env_str("BT_EXPERIMENT_TAG", ""))

    # Risk capacity & regime feature richness
    p.add_argument("--min-risk-capacity", type=float, default=_env_float("BT_MIN_RISK_CAPACITY", -1.0))
    p.add_argument("--max-risk-capacity", type=float, default=_env_float("BT_MAX_RISK_CAPACITY", -1.0))
    p.add_argument("--regime-feature-mode", default=_env_str("BT_REGIME_FEATURE_MODE", ""))

    # Feature/smartwallet/dedup
    p.add_argument("--feature-mode", default=_env_str("BT_FEATURE_MODE", ""))
    p.add_argument("--smart-wallet-mode", default=_env_str("BT_SMART_WALLET_MODE", ""))
    p.add_argument("--dedup-mode", default=_env_str("BT_DEDUP_MODE", ""))
    p.add_argument("--dedup-window-sec", type=int, default=_env_int("BT_DEDUP_WINDOW_SEC", 0))

    # Engine/split
    p.add_argument("--engine-mode", default=_env_str("BT_ENGINE_MODE", ""))
    p.add_argument("--split-mode", default=_env_str("BT_SPLIT_MODE", ""))
    p.add_argument("--split-date", default=_env_str("BT_SPLIT_DATE", ""))
    p.add_argument("--split-ratio", type=float, default=_env_float("BT_SPLIT_RATIO", 0.0))
    p.add_argument("--segment", default=_env_str("BT_RESULT_SEGMENT", ""))

    # Execution/logging
    p.add_argument("--max-trades", type=int, default=_env_int("BT_MAX_TRADES", 0))
    p.add_argument("--seed", type=int, default=_env_int("BT_RAND_SEED", 0))
    p.add_argument("--tag-prefix", default=_env_str("BT_TAG_PREFIX", ""))

    _add_bool_flag(p, "log-trades", "BT_LOG_TRADES", None, "Ask engine to attach per-trade logs if supported")
    _add_bool_flag(p, "log-features", "BT_LOG_FEATURES", None, "Ask engine to attach feature vectors if supported")
    _add_bool_flag(p, "use-live-risk-manager", "BT_USE_LIVE_RISK_MANAGER", None, "Simulate live risk_manager behavior inside backtest")
    _add_bool_flag(p, "require-risk-capacity", "BT_REQUIRE_RISK_CAPACITY", None, "Set ENTRY_REQUIRE_RISK_CAPACITY (tri-state)")

    # Self-learning
    _add_bool_flag(p, "enable-self-learning", "BT_ENABLE_SELF_LEARNING", None, "Enable self-learning strategy updates during backtest")
    _add_bool_flag(p, "self-learn-use-llm", "BT_SELF_LEARN_USE_LLM", None, "Allow LLM-driven tuning during self-learning (simulated)")
    p.add_argument("--self-learn-interval-sec", type=int, default=_env_int("BT_SELF_LEARN_INTERVAL_SEC", 0))
    p.add_argument("--self-learn-min-trades", type=int, default=_env_int("BT_SELF_LEARN_MIN_TRADES", 0))
    p.add_argument("--self-learn-max-per-day", type=int, default=_env_int("BT_SELF_LEARN_MAX_TUNES_PER_DAY", 0))
    _add_bool_flag(p, "log-strategy-health", "BT_LOG_STRATEGY_HEALTH", None, "Ask engine to log strategy health snapshots")

    # Reinvestor
    _add_bool_flag(p, "enable-reinvestor", "BT_ENABLE_REINVESTOR", None, "Enable reinvestor simulation in backtest")
    p.add_argument("--reinvest-percent", type=float, default=_env_float("BT_REINVEST_PERCENT", 0.0))
    p.add_argument("--reinvest-min-profit-sol", type=float, default=_env_float("BT_REINVEST_MIN_PROFIT_SOL", 0.0))
    p.add_argument("--reinvest-max-single-usd", type=float, default=_env_float("BT_REINVEST_MAX_SINGLE_USD", 0.0))
    p.add_argument("--reinvest-dd-soft", type=float, default=_env_float("BT_REINVEST_DD_SOFT_STOP_PCT", 0.0))
    p.add_argument("--reinvest-dd-hard", type=float, default=_env_float("BT_REINVEST_DD_HARD_STOP_PCT", 0.0))

    # AI risk overlay
    _add_bool_flag(p, "use-ai-risk-overlay", "BT_USE_AI_RISK_MODEL", None, "Enable ai_risk_model overlay in backtest (if supported)")

    # ai_db slicing
    p.add_argument("--src", default=_env_str("BT_SRC", ""))
    p.add_argument("--min-feature-version", type=int, default=_env_int("BT_MIN_FEATURE_VERSION", 0))
    p.add_argument("--wallet-tag", default=_env_str("BT_WALLET_TAG", ""))
    p.add_argument("--session-id", default=_env_str("BT_SESSION_ID", ""))

    # Costs
    p.add_argument("--trade-cost-bps", type=float, default=_env_float("BT_TRADE_COST_BPS", 0.0))
    p.add_argument("--jito-tip-bps", type=float, default=_env_float("BT_JITO_TIP_BPS", 0.0))
    p.add_argument("--base-size-sol", type=float, default=_env_float("BT_BASE_SIZE_SOL", 0.0))
    p.add_argument("--max-hold-sec", type=int, default=_env_int("BT_MAX_HOLD_SEC", 0))

    # Parallel / sampling / ordering
    _add_bool_flag(p, "parallel", "BT_PARALLEL", None, "Run configs in parallel with asyncio")
    p.add_argument("--parallel-limit", type=int, default=_env_int("BT_PARALLEL_LIMIT", 8))
    p.add_argument("--sample-config-fraction", type=float, default=_env_float("BT_SAMPLE_CONFIG_FRACTION", 0.0))
    _add_bool_flag(p, "shuffle-configs", "BT_SHUFFLE_CONFIGS", None, "Shuffle config execution order")

    # Sorting / metrics
    p.add_argument("--sort-key", default=_env_str("BT_SORT_KEY", ""))
    _add_bool_flag(p, "sort-asc", "BT_SORT_ASC", None, "Sort ascending instead of descending")
    p.add_argument("--metrics", default=_env_str("BT_METRICS", ""))
    p.add_argument("--top-n", type=int, default=_env_int("BT_TOP_N", 0))
    p.add_argument("--max-configs", type=int, default=_env_int("BT_MAX_CONFIGS", 0))

    # Result filters
    p.add_argument("--min-trades", type=int, default=_env_int("BT_MIN_TRADES", 0))
    _add_bool_flag(p, "require-trades", "BT_REQUIRE_TRADES", None, "Drop configs with zero trades")

    p.add_argument("--min-win-rate", type=float, default=_env_float("BT_MIN_WIN_RATE", -1.0))
    p.add_argument("--max-drawdown", type=float, default=_env_float("BT_MAX_DRAWDOWN_PCT", 0.0))
    p.add_argument("--max-p95-drawdown-trade-pct", type=float, default=_env_float("BT_MAX_P95_DRAWDOWN_TRADE_PCT", 0.0))
    p.add_argument("--min-sharpe", type=float, default=_env_float("BT_MIN_SHARPE", 0.0))
    p.add_argument("--min-profit-factor", type=float, default=_env_float("BT_MIN_PROFIT_FACTOR", 0.0))
    p.add_argument("--min-avg-pnl-pct", type=float, default=_env_float("BT_MIN_AVG_PNL_PCT", 0.0))
    p.add_argument("--min-sortino", type=float, default=_env_float("BT_MIN_SORTINO", 0.0))
    p.add_argument("--min-daily-pnl-pct", type=float, default=_env_float("BT_MIN_DAILY_PNL_PCT", 0.0))
    p.add_argument("--min-monthly-pnl-pct", type=float, default=_env_float("BT_MIN_MONTHLY_PNL_PCT", 0.0))
    p.add_argument("--min-monthly-pnl-compound-pct", type=float, default=_env_float("BT_MIN_MONTHLY_PNL_COMPOUND_PCT", 0.0))
    p.add_argument("--min-equity-final-pct", type=float, default=_env_float("BT_MIN_EQUITY_FINAL_PCT", 0.0))
    p.add_argument("--min-cagr-annual-pct", type=float, default=_env_float("BT_MIN_CAGR_ANNUAL_PCT", 0.0))
    p.add_argument("--min-net-pnl-pct", type=float, default=_env_float("BT_MIN_NET_PNL_PCT", 0.0))
    p.add_argument("--min-reinvest-count", type=int, default=_env_int("BT_MIN_REINVEST_COUNT", 0))
    p.add_argument("--min-self-learn-tunes", type=int, default=_env_int("BT_MIN_SELF_LEARN_TUNE_COUNT", 0))
    p.add_argument("--min-edge-score", type=float, default=_env_float("BT_MIN_EDGE_SCORE", 0.0))
    p.add_argument("--min-pnl-dd-ratio", type=float, default=_env_float("BT_MIN_PNL_DD_RATIO", 0.0))
    p.add_argument("--min-calmar", type=float, default=_env_float("BT_MIN_CALMAR", 0.0))
    p.add_argument("--min-win-loss-ratio", type=float, default=_env_float("BT_MIN_WIN_LOSS_RATIO", 0.0))
    p.add_argument("--min-expectancy-pct", type=float, default=_env_float("BT_MIN_EXPECTANCY_PCT", 0.0))
    p.add_argument("--min-kelly-fraction", type=float, default=_env_float("BT_MIN_KELLY_FRACTION", 0.0))
    p.add_argument("--min-kelly-safe-fraction", type=float, default=_env_float("BT_MIN_KELLY_SAFE_FRACTION", 0.0))
    p.add_argument("--min-trades-per-day", type=float, default=_env_float("BT_MIN_TRADES_PER_DAY", 0.0))

    # Target monthly PnL commentary
    p.add_argument("--target-monthly-pnl-pct", type=float, default=_env_float("BT_TARGET_MONTHLY_PNL_PCT", 0.0))

    # Export
    p.add_argument("--export-json", default=_env_str("BT_EXPORT_JSON", ""))
    p.add_argument("--export-csv", default=_env_str("BT_EXPORT_CSV", ""))
    p.add_argument("--export-trades-json", default=_env_str("BT_EXPORT_TRADES_JSON", ""))
    p.add_argument("--export-env-best", default=_env_str("BT_EXPORT_ENV_BEST", ""))
    _add_bool_flag(p, "print-best-json", "BT_PRINT_BEST_JSON", None, "Print best config+stats JSON at end")

    # Resume / safety / tag filters
    p.add_argument("--resume-json", default=_env_str("BT_RESUME_JSON", ""), help="Load prior export-json and skip tags already present")
    p.add_argument("--timeout-sec", type=int, default=_env_int("BT_TIMEOUT_SEC", 0), help="Per-config timeout (0 = disabled)")
    p.add_argument("--max-failures", type=int, default=_env_int("BT_MAX_FAILURES", 0), help="Abort after N failed configs (0 = never)")
    p.add_argument("--include-tag", default=_env_str("BT_INCLUDE_TAG", ""), help="Regex: only run configs whose tag matches")
    p.add_argument("--exclude-tag", default=_env_str("BT_EXCLUDE_TAG", ""), help="Regex: skip configs whose tag matches")

    # Base config file merge
    p.add_argument("--base-config-json", default=_env_str("BT_BASE_CONFIG_JSON", ""))

    # Run meta / grouping / optional db logging
    p.add_argument("--group-id", default=_env_str("BT_GROUP_ID", ""))
    p.add_argument("--run-note", default=_env_str("BT_RUN_NOTE", ""))
    p.add_argument("--group-summary-by", default=_env_str("BT_GROUP_SUMMARY_BY", ""))
    _add_bool_flag(p, "save-sweep-to-ai-db", "BT_SAVE_SWEEP_TO_AI_DB", None, "If ai_db.log_backtest_sweep exists, log sweep")

    # Export safety / durability
    p.add_argument("--export-json-every", type=int, default=_env_int("BT_EXPORT_JSON_EVERY", 0), help="Checkpoint export-json every N completed configs (0=off)")
    _add_bool_flag(p, "append-export-json", "BT_APPEND_EXPORT_JSON", None, "If export-json exists, append results instead of overwriting")

    # Debug
    _add_bool_flag(p, "dry-run", "BT_DRY_RUN", False, "Build configs and print one example, then exit")
    _add_bool_flag(p, "verbose", "BT_VERBOSE", False, "Dump config+stats for each surviving config")

    return p


# =============================================================================
# Grid builder
# =============================================================================


def _resolve_date_range(args: argparse.Namespace) -> Tuple[Optional[datetime], Optional[datetime], int]:
    start_ts = _parse_date(args.start_date)
    end_ts = _parse_date(args.end_date)

    if start_ts and end_ts:
        # If user gave YYYY-MM-DD end date, include that day by bumping +1 day
        if end_ts.time() == datetime.min.time():
            end_ts = end_ts + timedelta(days=1)
        eff_days = max(1, int((end_ts - start_ts).days))
        return start_ts, end_ts, eff_days

    return None, None, int(args.days_back)


def _load_base_extra(path: str) -> Dict[str, Any]:
    if not path:
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return dict(data) if isinstance(data, dict) else {}
    except Exception as e:
        log.warning("Failed to load base config JSON (%s): %s", path, e)
        return {}


def _sanitize_tag(tag: str, max_len: int = 220) -> str:
    """
    Make tags filesystem- and JSON-friendly, and stable-shorten with hash suffix
    if they exceed max_len (uses stable md5).
    """
    tag = re.sub(r"[^A-Za-z0-9._-]+", "_", tag)
    tag = re.sub(r"_+", "_", tag).strip("_")
    if len(tag) <= max_len:
        return tag
    h = hashlib.md5(tag.encode("utf-8")).hexdigest()[:10]
    return tag[: max_len - 11] + "_" + h


def build_config_grid(args: argparse.Namespace) -> List[BacktestConfig]:
    score_mins = _parse_csv_floats(args.score_min) or [0.60]
    max_pos_list = _parse_csv_ints(args.max_positions) or [1]

    risk_mults: List[Optional[float]] = _parse_csv_floats(args.risk_mult) or [None]
    regime_modes_raw = _parse_csv_ints(args.regime_risk_mode)
    regime_modes: List[Optional[int]] = [None]
    if regime_modes_raw:
        cleaned = [m for m in regime_modes_raw if m in (0, 1, 2)]
        regime_modes = cleaned or [None]

    entry_min_scores: List[Optional[float]] = _parse_csv_floats(args.entry_min_score) or [None]
    entry_ai_weights: List[Optional[float]] = _parse_csv_floats(args.entry_ai_weight) or [None]
    risk_profiles: List[Optional[str]] = _parse_csv_strs(args.risk_profile) or [None]
    exit_policies: List[Optional[str]] = _parse_csv_strs(args.exit_policy) or [None]

    regime_whitelist = _parse_csv_strs(args.regime) or None
    label_include = _parse_csv_strs(args.label_include) or None
    label_exclude = _parse_csv_strs(args.label_exclude) or None
    limit_candidates = args.limit_candidates if args.limit_candidates and args.limit_candidates > 0 else None

    policy_version = (args.policy_version or "").strip() or None
    experiment_tag = (args.experiment_tag or "").strip() or None

    start_ts, end_ts, eff_days_back = _resolve_date_range(args)

    sample_weight_mode = (args.sample_weight_mode or "").strip() or None
    label_weights_json = (args.label_weights_json or "").strip() or None

    entry_model_name = (args.entry_model or "").strip() or None

    min_risk_capacity = float(args.min_risk_capacity) if args.min_risk_capacity is not None and args.min_risk_capacity >= 0.0 else None
    max_risk_capacity = float(args.max_risk_capacity) if args.max_risk_capacity is not None and args.max_risk_capacity >= 0.0 else None
    regime_feature_mode = (args.regime_feature_mode or "").strip() or None

    feature_mode = (args.feature_mode or "").strip() or None
    smart_wallet_mode = (args.smart_wallet_mode or "").strip() or None
    dedup_mode = (args.dedup_mode or "").strip() or None
    dedup_window_sec = args.dedup_window_sec if args.dedup_window_sec and args.dedup_window_sec > 0 else None

    engine_mode = (args.engine_mode or "").strip() or None
    split_mode = (args.split_mode or "").strip() or None
    split_date = _parse_date(args.split_date) if (args.split_date or "").strip() else None
    split_ratio = args.split_ratio if args.split_ratio and args.split_ratio > 0.0 else None
    result_segment = (args.segment or "").strip() or None

    max_trades = args.max_trades if args.max_trades and args.max_trades > 0 else None
    rand_seed = args.seed if args.seed and args.seed != 0 else None
    tag_prefix = (args.tag_prefix or "").strip() or None

    # tri-state bools
    require_risk_capacity_opt: Optional[bool] = args.require_risk_capacity
    log_trades_opt: Optional[bool] = args.log_trades
    log_features_opt: Optional[bool] = args.log_features
    use_live_risk_manager_opt: Optional[bool] = args.use_live_risk_manager

    enable_self_learning_opt: Optional[bool] = args.enable_self_learning
    self_learning_use_llm_opt: Optional[bool] = args.self_learn_use_llm
    self_learning_interval_sec = args.self_learn_interval_sec if args.self_learn_interval_sec and args.self_learn_interval_sec > 0 else None
    self_learning_min_trades = args.self_learn_min_trades if args.self_learn_min_trades and args.self_learn_min_trades > 0 else None
    self_learning_max_tunes_per_day = args.self_learn_max_per_day if args.self_learn_max_per_day and args.self_learn_max_per_day > 0 else None
    log_strategy_health_opt: Optional[bool] = args.log_strategy_health

    enable_reinvestor_opt: Optional[bool] = args.enable_reinvestor
    reinvest_percent_override = args.reinvest_percent if args.reinvest_percent and args.reinvest_percent > 0.0 else None
    reinvest_min_profit_sol = args.reinvest_min_profit_sol if args.reinvest_min_profit_sol and args.reinvest_min_profit_sol > 0.0 else None
    reinvest_max_single_usd = args.reinvest_max_single_usd if args.reinvest_max_single_usd and args.reinvest_max_single_usd > 0.0 else None
    reinvest_dd_soft_stop_pct = args.reinvest_dd_soft if args.reinvest_dd_soft and args.reinvest_dd_soft > 0.0 else None
    reinvest_dd_hard_stop_pct = args.reinvest_dd_hard if args.reinvest_dd_hard and args.reinvest_dd_hard > 0.0 else None

    use_ai_risk_overlay_opt: Optional[bool] = args.use_ai_risk_overlay

    src_filter = (args.src or "").strip() or None
    min_feature_version = args.min_feature_version if args.min_feature_version and args.min_feature_version > 0 else None
    wallet_tag_filter = (args.wallet_tag or "").strip() or None
    session_id_filter = (args.session_id or "").strip() or None

    trade_cost_bps = args.trade_cost_bps if args.trade_cost_bps and args.trade_cost_bps > 0.0 else None
    jito_tip_bps = args.jito_tip_bps if args.jito_tip_bps and args.jito_tip_bps > 0.0 else None
    base_size_sol = args.base_size_sol if args.base_size_sol and args.base_size_sol > 0.0 else None
    max_hold_sec = args.max_hold_sec if args.max_hold_sec and args.max_hold_sec > 0 else None

    base_extra = _load_base_extra(args.base_config_json)

    group_id = (args.group_id or "").strip() or None
    run_note = (args.run_note or "").strip() or None

    configs: List[BacktestConfig] = []

    for s in score_mins:
        s = max(0.0, min(1.0, float(s)))
        for max_pos in max_pos_list:
            max_pos = max(1, int(max_pos))
            for risk_mult in risk_mults:
                for regime_mode in regime_modes:
                    for entry_min in entry_min_scores:
                        for ai_w in entry_ai_weights:
                            for rp in risk_profiles:
                                for ep in exit_policies:
                                    tag_parts = [f"s{s:.3f}", f"mp{max_pos}"]
                                    if risk_mult is not None:
                                        tag_parts.append(f"rm{float(risk_mult):.2f}")
                                    if regime_mode is not None:
                                        tag_parts.append(f"rg{int(regime_mode)}")
                                    if entry_min is not None:
                                        tag_parts.append(f"emin{float(entry_min):.2f}")
                                    if ai_w is not None:
                                        tag_parts.append(f"aiw{float(ai_w):.2f}")
                                    if rp:
                                        tag_parts.append(f"rp{rp}")
                                    if ep:
                                        tag_parts.append(f"ep{ep}")
                                    if min_risk_capacity is not None:
                                        tag_parts.append(f"rcmin{min_risk_capacity:.2f}")
                                    if max_risk_capacity is not None:
                                        tag_parts.append(f"rcmax{max_risk_capacity:.2f}")
                                    if feature_mode:
                                        tag_parts.append(f"feat{feature_mode}")
                                    if smart_wallet_mode:
                                        tag_parts.append(f"sw{smart_wallet_mode}")
                                    if engine_mode:
                                        tag_parts.append(f"eng{engine_mode}")
                                    if result_segment:
                                        tag_parts.append(f"seg{result_segment}")
                                    if enable_self_learning_opt is True:
                                        tag_parts.append("sl1")
                                    if self_learning_use_llm_opt is True:
                                        tag_parts.append("slllm1")
                                    if enable_reinvestor_opt is True:
                                        tag_parts.append("rv1")
                                    if use_ai_risk_overlay_opt is True:
                                        tag_parts.append("airisk1")
                                    if src_filter:
                                        tag_parts.append(f"src{src_filter}")
                                    if min_feature_version is not None:
                                        tag_parts.append(f"fv{min_feature_version}")
                                    if wallet_tag_filter:
                                        tag_parts.append(f"wt{wallet_tag_filter}")
                                    if session_id_filter:
                                        tag_parts.append("sess1")
                                    if trade_cost_bps is not None:
                                        tag_parts.append(f"tc{trade_cost_bps:.1f}")
                                    if jito_tip_bps is not None:
                                        tag_parts.append(f"jito{jito_tip_bps:.1f}")
                                    if group_id:
                                        tag_parts.append(f"gid{group_id}")

                                    tag = "_".join(tag_parts)
                                    if tag_prefix:
                                        tag = f"{tag_prefix}{tag}"
                                    tag = _sanitize_tag(tag)

                                    extra = dict(base_extra)
                                    if run_note:
                                        extra.setdefault("_run_meta", {})
                                        extra["_run_meta"]["note"] = run_note

                                    cfg = BacktestConfig(
                                        ENTRY_SCORE_MIN=s,
                                        max_positions=max_pos,
                                        days_back=int(eff_days_back),
                                        regime_whitelist=regime_whitelist,
                                        label_include=label_include,
                                        label_exclude=label_exclude,
                                        limit_candidates=limit_candidates,
                                        start_ts=start_ts,
                                        end_ts=end_ts,
                                        risk_mult_override=float(risk_mult) if risk_mult is not None else None,
                                        regime_risk_mode_lock=regime_mode,
                                        entry_min_score=float(entry_min) if entry_min is not None else None,
                                        entry_ai_weight=float(ai_w) if ai_w is not None else None,
                                        entry_model_name=entry_model_name,
                                        risk_profile=rp,
                                        exit_policy_name=ep,
                                        require_risk_capacity=require_risk_capacity_opt,
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
                                        log_trades=log_trades_opt,
                                        log_features=log_features_opt,
                                        rand_seed=rand_seed,
                                        tag_prefix=tag_prefix,
                                        use_live_risk_manager=use_live_risk_manager_opt,
                                        enable_self_learning=enable_self_learning_opt,
                                        self_learning_use_llm=self_learning_use_llm_opt,
                                        self_learning_interval_sec=self_learning_interval_sec,
                                        self_learning_min_trades=self_learning_min_trades,
                                        self_learning_max_tunes_per_day=self_learning_max_tunes_per_day,
                                        log_strategy_health=log_strategy_health_opt,
                                        enable_reinvestor=enable_reinvestor_opt,
                                        reinvest_percent_override=reinvest_percent_override,
                                        reinvest_min_profit_sol=reinvest_min_profit_sol,
                                        reinvest_max_single_usd=reinvest_max_single_usd,
                                        reinvest_dd_soft_stop_pct=reinvest_dd_soft_stop_pct,
                                        reinvest_dd_hard_stop_pct=reinvest_dd_hard_stop_pct,
                                        use_ai_risk_overlay=use_ai_risk_overlay_opt,
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

    if args.max_configs and len(configs) > int(args.max_configs):
        log.info("Limiting configs from %d to %d (BT_MAX_CONFIGS / --max-configs).", len(configs), int(args.max_configs))
        configs = configs[: int(args.max_configs)]

    return configs


# =============================================================================
# Derived metrics
# =============================================================================


def _augment_stats(stats: Dict[str, Any]) -> None:
    # win_rate
    try:
        if "win_rate" not in stats and "n_trades" in stats and "n_wins" in stats:
            n_trades = _safe_float(stats.get("n_trades"), 0.0)
            n_wins = _safe_float(stats.get("n_wins"), 0.0)
            if n_trades > 0:
                stats["win_rate"] = n_wins / n_trades
    except Exception:
        pass

    # net_pnl_pct alias
    try:
        if "net_pnl_pct" not in stats:
            for k in ("pnl_total_pct", "net_return_pct", "total_return_pct"):
                if k in stats and isinstance(stats[k], (int, float)):
                    stats["net_pnl_pct"] = float(stats[k])
                    break
    except Exception:
        pass

    # pnl_per_trade_pct
    try:
        if "pnl_per_trade_pct" not in stats and "pnl_total_pct" in stats and "n_trades" in stats:
            n = _safe_float(stats.get("n_trades"), 0.0)
            if n > 0:
                stats["pnl_per_trade_pct"] = _safe_float(stats.get("pnl_total_pct"), 0.0) / n
    except Exception:
        pass

    # expectancy_pct
    try:
        if "expectancy_pct" not in stats and "win_rate" in stats:
            wr = _safe_float(stats.get("win_rate"), 0.0)
            avg_win = stats.get("avg_win_pct")
            avg_loss = stats.get("avg_loss_pct")
            if isinstance(avg_win, (int, float)) and isinstance(avg_loss, (int, float)):
                stats["expectancy_pct"] = wr * float(avg_win) + (1.0 - wr) * float(avg_loss)
    except Exception:
        pass

    # days span helpers
    days_val: Optional[float] = None
    for k in ("days_span", "days", "window_days"):
        v = stats.get(k)
        if isinstance(v, (int, float)) and float(v) > 0:
            days_val = float(v)
            break

    try:
        if days_val and "daily_pnl_pct" not in stats and "pnl_total_pct" in stats:
            stats["daily_pnl_pct"] = _safe_float(stats.get("pnl_total_pct"), 0.0) / days_val
        if days_val and "trades_per_day" not in stats and "n_trades" in stats:
            stats["trades_per_day"] = _safe_float(stats.get("n_trades"), 0.0) / days_val
    except Exception:
        pass

    try:
        if "monthly_pnl_pct" not in stats and "daily_pnl_pct" in stats:
            stats["monthly_pnl_pct"] = _safe_float(stats.get("daily_pnl_pct"), 0.0) * 30.0
        if "monthly_pnl_compound_pct" not in stats and "daily_pnl_pct" in stats:
            d = _safe_float(stats.get("daily_pnl_pct"), 0.0)
            gross_d = 1.0 + d / 100.0
            if gross_d > 0:
                stats["monthly_pnl_compound_pct"] = ((gross_d**30.0) - 1.0) * 100.0
    except Exception:
        pass

    # CAGR annual
    try:
        if days_val and days_val > 0:
            npct = _safe_float(stats.get("net_pnl_pct", stats.get("pnl_total_pct", 0.0)), 0.0)
            gross = 1.0 + npct / 100.0
            if gross > 0:
                years = max(1e-9, days_val / 365.0)
                if "cagr_annual_pct" not in stats:
                    stats["cagr_annual_pct"] = (gross ** (1.0 / years) - 1.0) * 100.0
    except Exception:
        pass

    # Sortino (if downside dev present)
    try:
        if "sortino" not in stats:
            avg = stats.get("avg_pnl_pct")
            dd = stats.get("downside_dev_pct") or stats.get("downside_dev")
            if isinstance(avg, (int, float)) and isinstance(dd, (int, float)) and float(dd) > 0:
                stats["sortino"] = float(avg) / float(dd)
    except Exception:
        pass

    # pnl_dd_ratio, calmar, win_loss_ratio, kelly, edge_score
    try:
        npct = _safe_float(stats.get("net_pnl_pct", stats.get("pnl_total_pct", 0.0)), 0.0)
        dd = _safe_float(stats.get("max_drawdown_pct", 0.0), 0.0)
        dd_abs = abs(dd)
        sh = _safe_float(stats.get("sharpe", 0.0), 0.0)

        if dd_abs > 0 and "pnl_dd_ratio" not in stats:
            stats["pnl_dd_ratio"] = npct / dd_abs
        if dd_abs > 0 and "calmar_ratio" not in stats:
            stats["calmar_ratio"] = npct / dd_abs

        avg_win = stats.get("avg_win_pct")
        avg_loss = stats.get("avg_loss_pct")
        if "win_loss_ratio" not in stats and isinstance(avg_win, (int, float)) and isinstance(avg_loss, (int, float)) and float(avg_loss) != 0:
            stats["win_loss_ratio"] = float(avg_win) / abs(float(avg_loss))

        if "kelly_fraction" not in stats and isinstance(avg_win, (int, float)) and isinstance(avg_loss, (int, float)) and stats.get("win_rate") is not None:
            wr = _safe_float(stats.get("win_rate"), 0.0)
            q = 1.0 - wr
            if float(avg_loss) != 0:
                b = float(avg_win) / abs(float(avg_loss))
                if b > 0:
                    k = (b * wr - q) / b
                    stats["kelly_fraction"] = k
                    stats["kelly_safe_fraction"] = max(0.0, k * 0.5)

        if "edge_score" not in stats:
            denom = 1.0 + dd_abs
            base = npct / denom
            stats["edge_score"] = base * (1.0 + 0.25 * sh)
    except Exception:
        pass


# =============================================================================
# Table / sorting
# =============================================================================


def _pick_sort_key_name(stats_list: Sequence[Mapping[str, Any]], override: Optional[str]) -> Optional[str]:
    if not stats_list:
        return None
    if override:
        o = override.strip()
        if o and o in stats_list[0]:
            return o
    preferred = ["edge_score", "net_pnl_pct", "pnl_total_pct", "equity_final_pct", "sharpe", "profit_factor"]
    keys = set(stats_list[0].keys())
    for k in preferred:
        if k in keys:
            return k
    for k, v in stats_list[0].items():
        if isinstance(v, (int, float)):
            return k
    return None


def _pick_metric_keys(sample: Mapping[str, Any], override_metrics: Optional[str]) -> List[str]:
    req = _parse_csv_strs(override_metrics or "")
    if req:
        return [k for k in req if k in sample]
    preferred = [
        "n_trades",
        "win_rate",
        "avg_pnl_pct",
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
        "monthly_pnl_pct",
        "monthly_pnl_compound_pct",
        "trades_per_day",
        "edge_score",
        "pnl_dd_ratio",
        "calmar_ratio",
        "kelly_fraction",
        "kelly_safe_fraction",
        "cagr_annual_pct",
        "self_learning_tune_count",
        "reinvest_count",
        "reinvest_pnl_pct",
    ]
    out = [k for k in preferred if k in sample]
    return out or list(sorted(sample.keys()))[:10]


def _fmt_val(v: Any) -> str:
    if isinstance(v, float):
        if not math.isfinite(v):
            return ""
        return f"{v:.3f}"
    if isinstance(v, int):
        return str(v)
    return str(v)


def print_results_table(
    configs: Sequence[BacktestConfig],
    stats_list: Sequence[Mapping[str, Any]],
    sort_key_override: Optional[str],
    sort_ascending: bool,
    top_n: Optional[int],
    metrics_override: Optional[str],
) -> Tuple[List[Tuple[BacktestConfig, Mapping[str, Any]]], Optional[str]]:
    if not configs or not stats_list or len(configs) != len(stats_list):
        print("No results to display.")
        return [], None

    sort_key = _pick_sort_key_name(stats_list, sort_key_override)
    paired = list(zip(configs, stats_list))

    if sort_key:

        def _key(cs: Tuple[BacktestConfig, Mapping[str, Any]]) -> float:
            return _safe_float(cs[1].get(sort_key), 0.0)

        paired.sort(key=_key, reverse=not sort_ascending)

    if top_n and top_n > 0:
        paired = paired[:top_n]

    metric_keys = _pick_metric_keys(paired[0][1], metrics_override)
    if sort_key and sort_key not in metric_keys:
        metric_keys = [sort_key] + [k for k in metric_keys if k != sort_key]

    headers = ["rank", "tag", "ENTRY_SCORE_MIN", "max_pos"] + metric_keys
    rows: List[List[str]] = []

    for rank, (cfg, stats) in enumerate(paired, start=1):
        row = [str(rank), (cfg.tag or ""), f"{cfg.ENTRY_SCORE_MIN:.3f}", str(cfg.max_positions)]
        for k in metric_keys:
            row.append(_fmt_val(stats.get(k, "")))
        rows.append(row)

    colw = [len(h) for h in headers]
    for r in rows:
        for i, c in enumerate(r):
            colw[i] = max(colw[i], len(c))

    def _line(cells: Sequence[str]) -> str:
        return " | ".join(cells[i].ljust(colw[i]) for i in range(len(cells)))

    total = sum(colw) + 3 * (len(colw) - 1)
    print("\nBacktest results (best first):")
    print("-" * total)
    print(_line(headers))
    print("-" * total)
    for r in rows:
        print(_line(r))
    print("-" * total)
    print("")
    return paired, sort_key


# =============================================================================
# Export
# =============================================================================


def export_json(path: str, paired: Sequence[Tuple[BacktestConfig, Mapping[str, Any]]], append: bool = False) -> None:
    new_rows = [{"config": cfg.to_run_kwargs(), "stats": dict(stats)} for cfg, stats in paired]

    if append and os.path.exists(path):
        old = _read_json(path) or {}
        old_results = old.get("results", [])
        if not isinstance(old_results, list):
            old_results = []
        payload = dict(old)
        payload["results"] = list(old_results) + new_rows
        payload["meta"] = dict(payload.get("meta") or {})
        payload["meta"]["updated_at"] = datetime.utcnow().isoformat() + "Z"
        payload["meta"]["count"] = len(payload["results"])
    else:
        payload = {
            "meta": {
                "generated_at": datetime.utcnow().isoformat() + "Z",
                "command": " ".join(sys.argv),
                "python": sys.version.split()[0],
                "platform": platform.platform(),
                "count": len(new_rows),
            },
            "results": new_rows,
        }

    _atomic_write_json(path, payload)
    print(f"Exported JSON results to {path} (count={payload.get('meta', {}).get('count')})")


def export_csv(path: str, paired: Sequence[Tuple[BacktestConfig, Mapping[str, Any]]]) -> None:
    cfg_keys = sorted({k for cfg, _ in paired for k in cfg.to_run_kwargs().keys()})
    stat_keys = sorted({k for _, st in paired for k in st.keys()})
    fieldnames = ["rank"] + cfg_keys + stat_keys

    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)

    fd, tmp = tempfile.mkstemp(prefix=p.name + ".", suffix=".tmp", dir=str(p.parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="") as f:
            w = csv.DictWriter(f, fieldnames=fieldnames)
            w.writeheader()
            for i, (cfg, st) in enumerate(paired, start=1):
                row: Dict[str, Any] = {"rank": i}
                row.update(cfg.to_run_kwargs())
                for k in stat_keys:
                    if k in st:
                        row[k] = st[k]
                w.writerow(row)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)
    finally:
        try:
            if os.path.exists(tmp):
                os.remove(tmp)
        except Exception:
            pass

    print(f"Exported CSV results to {path}")


def export_trades_json(path: str, paired: Sequence[Tuple[BacktestConfig, Mapping[str, Any]]]) -> None:
    trades_out: List[Dict[str, Any]] = []
    for cfg, st in paired:
        trades = st.get("trades")
        if not isinstance(trades, list):
            continue
        tag = cfg.tag or ""
        for t in trades:
            rec = dict(t) if isinstance(t, dict) else {"raw": t}
            rec.setdefault("config_tag", tag)
            trades_out.append(rec)

    payload = {
        "meta": {
            "generated_at": datetime.utcnow().isoformat() + "Z",
            "command": " ".join(sys.argv),
            "count_trades": len(trades_out),
        },
        "trades": trades_out,
    }
    _atomic_write_json(path, payload)
    print(f"Exported trades JSON to {path} ({len(trades_out)} trades)")


def export_env_best(path: str, best_cfg: BacktestConfig, best_stats: Mapping[str, Any], sort_key: Optional[str]) -> None:
    cfg_dict = best_cfg.to_run_kwargs()
    sort_val = best_stats.get(sort_key) if sort_key else None

    lines: List[str] = []
    lines.append("# Auto-generated by scripts/backtest.py")
    lines.append(f"# generated_at={datetime.utcnow().isoformat()}Z")
    if sort_key and sort_val is not None:
        lines.append(f"# best_sort_key={sort_key} value={sort_val}")
    if best_cfg.tag:
        lines.append(f"# config_tag={best_cfg.tag}")
    lines.append("# --- Backtest best config env ---")
    for k in sorted(cfg_dict.keys()):
        v = cfg_dict[k]
        v_str = "1" if isinstance(v, bool) and v else "0" if isinstance(v, bool) else str(v)
        lines.append(f"{k}={v_str}")
    _atomic_write_text(path, "\n".join(lines) + "\n")
    print(f"Exported best config as .env-style file to {path}")


def _load_resume_tags(path: str) -> set[str]:
    tags: set[str] = set()
    if not path or not os.path.exists(path):
        return tags
    try:
        payload = _read_json(path) or {}
        results = payload.get("results", [])
        if isinstance(results, list):
            for r in results:
                if not isinstance(r, dict):
                    continue
                cfg = r.get("config", {})
                if not isinstance(cfg, dict):
                    continue
                t = cfg.get("config_tag") or cfg.get("tag")
                if isinstance(t, str) and t:
                    tags.add(t)
    except Exception as e:
        print(f"WARNING: failed to load resume-json {path}: {e}")
    return tags


# =============================================================================
# ai_db optional helpers (sync OR async)
# =============================================================================


async def _ai_db_call(name: str, *args: Any, **kwargs: Any) -> Optional[Any]:
    if ai_db is None:
        return None
    fn = getattr(ai_db, name, None)
    if not callable(fn):
        return None
    try:
        out = fn(*args, **kwargs)
        if inspect.isawaitable(out):
            out = await out
        return out
    except Exception as e:
        print(f"WARNING: ai_db.{name} failed: {e}")
        return None


# =============================================================================
# Backtest engine call (supports run_backtest(cfg_dict) OR run_backtest(**kwargs))
# =============================================================================


async def _call_run_backtest(cfg_kwargs: Dict[str, Any]) -> Any:
    """
    Attempts to call run_backtest safely across signature styles.
    """
    sig = None
    try:
        sig = inspect.signature(run_backtest)
    except Exception:
        sig = None

    # Try kwargs if possible
    try:
        if sig is not None:
            params = list(sig.parameters.values())
            has_var_kw = any(p.kind == p.VAR_KEYWORD for p in params)
            if has_var_kw:
                out = run_backtest(**cfg_kwargs)
            elif len(params) == 1:
                out = run_backtest(cfg_kwargs)
            else:
                try:
                    out = run_backtest(**cfg_kwargs)
                except TypeError:
                    out = run_backtest(cfg_kwargs)
        else:
            out = run_backtest(cfg_kwargs)

        if inspect.isawaitable(out):
            out = await out
        return out
    except Exception:
        out2 = run_backtest(cfg_kwargs)
        if inspect.isawaitable(out2):
            out2 = await out2
        return out2


# =============================================================================
# Main runner
# =============================================================================


async def _run_backtests(args: argparse.Namespace) -> None:
    configs = build_config_grid(args)
    if not configs:
        print("No configs built.")
        return

    # Seed
    if args.seed and int(args.seed) != 0:
        random.seed(int(args.seed))

    # Tag include/exclude filters
    inc_pat = re.compile(args.include_tag) if (args.include_tag or "").strip() else None
    exc_pat = re.compile(args.exclude_tag) if (args.exclude_tag or "").strip() else None
    if inc_pat or exc_pat:
        kept: List[BacktestConfig] = []
        for c in configs:
            t = c.tag or ""
            if inc_pat and not inc_pat.search(t):
                continue
            if exc_pat and exc_pat.search(t):
                continue
            kept.append(c)
        configs = kept

    if not configs:
        print("All configs removed by include/exclude tag filters.")
        return

    # Shuffle
    if args.shuffle_configs is True:
        random.shuffle(configs)

    # Sampling
    if args.sample_config_fraction and 0.0 < float(args.sample_config_fraction) < 1.0 and len(configs) > 1:
        k = max(1, int(len(configs) * float(args.sample_config_fraction)))
        configs = random.sample(configs, k)
        print(f"Sampling config grid: using {k} configs (fraction={float(args.sample_config_fraction):.3f}).")

    # Resume
    resume_tags = _load_resume_tags(args.resume_json) if args.resume_json else set()
    if resume_tags:
        before = len(configs)
        configs = [c for c in configs if (c.tag or "") not in resume_tags]
        after = len(configs)
        print(f"Resume enabled: skipping {before - after} already-run config(s). Remaining: {after}.")

    if args.dry_run is True:
        print(f"DRY RUN: built {len(configs)} config(s). Example:")
        print(json.dumps(configs[0].to_run_kwargs(), indent=2, default=str))
        return

    # Optional: DB diagnostics (never hard-fail sweep if absent)
    desc = await _ai_db_call("describe_backend")
    if desc is not None:
        print(f"DB backend: {desc}")

    if ai_db is not None and callable(getattr(ai_db, "ping", None)):
        ok = await _ai_db_call("ping")
        if ok is False:
            print("ERROR: ai_db.ping() failed – cannot reach Postgres.")
            return
    else:
        print("NOTE: ai_db.ping() not found (or ai_db missing); skipping DB connectivity gate.")

    # Optional baseline slice stats (best-effort)
    sample_cfg = configs[0]
    basic = await _ai_db_call(
        "fetch_basic_stats",
        days_back=sample_cfg.days_back,
        src=sample_cfg.src_filter,
        regime=(sample_cfg.regime_whitelist[0] if sample_cfg.regime_whitelist else None),
        min_feature_version=sample_cfg.min_feature_version,
    )
    if basic is not None:
        print("\nAI-DB baseline stats (slice):")
        print(json.dumps(basic, indent=2, default=str))
        print("")

    n = len(configs)
    print(
        f"Running backtest on {n} config(s)...\n"
        f"  days_back        : {sample_cfg.days_back}\n"
        f"  parallel         : {args.parallel is True} (limit={args.parallel_limit})\n"
        f"  timeout_sec      : {args.timeout_sec}\n"
        f"  max_failures     : {args.max_failures}\n"
        f"  regime_whitelist : {sample_cfg.regime_whitelist}\n"
        f"  src_filter       : {sample_cfg.src_filter}\n"
        f"  min_feat_version : {sample_cfg.min_feature_version}\n"
    )

    cfg_dicts = [c.to_run_kwargs() for c in configs]
    results: List[Optional[Dict[str, Any]]] = [None] * n
    failures = 0
    completed = 0
    stop_event = asyncio.Event()

    async def _run_one(i: int, cfg: BacktestConfig, kwargs: Dict[str, Any]) -> Dict[str, Any]:
        try:
            coro = _call_run_backtest(kwargs)
            if args.timeout_sec and int(args.timeout_sec) > 0:
                res = await asyncio.wait_for(coro, timeout=float(args.timeout_sec))
            else:
                res = await coro
            st = dict(res) if isinstance(res, Mapping) else {"result": res}
            _augment_stats(st)
            return st
        except asyncio.TimeoutError:
            return {"error": f"timeout after {args.timeout_sec}s"}
        except asyncio.CancelledError:
            return {"error": "cancelled"}
        except Exception as e:
            log.exception("Backtest config failed (tag=%s)", cfg.tag)
            return {"error": str(e)}

    async def _store_result(i: int, st: Dict[str, Any]) -> None:
        nonlocal failures, completed
        results[i] = st
        completed += 1

        is_fail = ("error" in st) and (len(st) == 1)
        if is_fail:
            failures += 1
            print(f"[{completed}/{n}] FAIL tag={configs[i].tag or ''} err={st.get('error')}")
        else:
            if completed % 5 == 0 or completed == n:
                print(f"Progress: {completed}/{n} completed (failures={failures})")

        # checkpoint export
        if args.export_json and args.export_json_every and int(args.export_json_every) > 0:
            if completed % int(args.export_json_every) == 0:
                ok_pairs_ckpt: List[Tuple[BacktestConfig, Mapping[str, Any]]] = []
                for j in range(n):
                    stj = results[j]
                    if stj is None:
                        continue
                    if ("error" in stj) and (len(stj) == 1):
                        continue
                    ok_pairs_ckpt.append((configs[j], stj))
                if ok_pairs_ckpt:
                    export_json(args.export_json, ok_pairs_ckpt, append=bool(args.append_export_json))

        if args.max_failures and int(args.max_failures) > 0 and failures >= int(args.max_failures):
            stop_event.set()

    if args.parallel is True and n > 1:
        sem = asyncio.Semaphore(max(1, int(args.parallel_limit or 1)))

        async def _task(i: int, cfg: BacktestConfig, kwargs: Dict[str, Any]) -> None:
            if stop_event.is_set():
                results[i] = {"error": "skipped due to abort"}
                return
            async with sem:
                st = await _run_one(i, cfg, kwargs)
                await _store_result(i, st)

        tasks = [asyncio.create_task(_task(i, cfg, cfgd)) for i, (cfg, cfgd) in enumerate(zip(configs, cfg_dicts))]

        try:
            while True:
                done, pending = await asyncio.wait(tasks, timeout=0.25, return_when=asyncio.FIRST_COMPLETED)
                _ = done
                if stop_event.is_set():
                    for t in pending:
                        t.cancel()
                    await asyncio.gather(*pending, return_exceptions=True)
                    break
                if not pending:
                    break
        finally:
            await asyncio.gather(*tasks, return_exceptions=True)

    else:
        for i, (cfg, kwargs) in enumerate(zip(configs, cfg_dicts)):
            if stop_event.is_set():
                break
            print(f"Running config {i+1}/{n} → {cfg.tag or ''}")
            st = await _run_one(i, cfg, kwargs)
            await _store_result(i, st)

    # Pair configs to stats (drop None + drop pure failures)
    ok_pairs: List[Tuple[BacktestConfig, Mapping[str, Any]]] = []
    for i in range(n):
        st = results[i]
        if st is None:
            continue
        if ("error" in st) and (len(st) == 1):
            continue
        ok_pairs.append((configs[i], st))

    if not ok_pairs:
        print("All backtests failed – nothing to display.")
        return

    # Result-level filters (NaN/inf safe)
    min_trades = int(args.min_trades or 0)
    require_trades = args.require_trades is True

    min_win_rate = None if float(args.min_win_rate) < 0.0 else float(args.min_win_rate)
    max_drawdown = None if _disabled_threshold(float(args.max_drawdown)) else float(args.max_drawdown)
    max_p95_dd_trade = None if _disabled_threshold(float(args.max_p95_drawdown_trade_pct)) else float(args.max_p95_drawdown_trade_pct)
    min_sharpe = None if _disabled_threshold(float(args.min_sharpe)) else float(args.min_sharpe)
    min_profit_factor = None if _disabled_threshold(float(args.min_profit_factor)) else float(args.min_profit_factor)
    min_avg_pnl_pct = None if _disabled_threshold(float(args.min_avg_pnl_pct)) else float(args.min_avg_pnl_pct)
    min_sortino = None if _disabled_threshold(float(args.min_sortino)) else float(args.min_sortino)
    min_daily_pnl_pct = None if _disabled_threshold(float(args.min_daily_pnl_pct)) else float(args.min_daily_pnl_pct)
    min_monthly_pnl_pct = None if _disabled_threshold(float(args.min_monthly_pnl_pct)) else float(args.min_monthly_pnl_pct)
    min_monthly_pnl_compound_pct = None if _disabled_threshold(float(args.min_monthly_pnl_compound_pct)) else float(args.min_monthly_pnl_compound_pct)
    min_equity_final_pct = None if _disabled_threshold(float(args.min_equity_final_pct)) else float(args.min_equity_final_pct)
    min_cagr_annual_pct = None if _disabled_threshold(float(args.min_cagr_annual_pct)) else float(args.min_cagr_annual_pct)
    min_net_pnl_pct = None if _disabled_threshold(float(args.min_net_pnl_pct)) else float(args.min_net_pnl_pct)
    min_edge_score = None if _disabled_threshold(float(args.min_edge_score)) else float(args.min_edge_score)
    min_pnl_dd_ratio = None if _disabled_threshold(float(args.min_pnl_dd_ratio)) else float(args.min_pnl_dd_ratio)
    min_calmar = None if _disabled_threshold(float(args.min_calmar)) else float(args.min_calmar)
    min_win_loss_ratio = None if _disabled_threshold(float(args.min_win_loss_ratio)) else float(args.min_win_loss_ratio)
    min_expectancy_pct = None if _disabled_threshold(float(args.min_expectancy_pct)) else float(args.min_expectancy_pct)
    min_kelly_fraction = None if _disabled_threshold(float(args.min_kelly_fraction)) else float(args.min_kelly_fraction)
    min_kelly_safe_fraction = None if _disabled_threshold(float(args.min_kelly_safe_fraction)) else float(args.min_kelly_safe_fraction)
    min_trades_per_day = None if _disabled_threshold(float(args.min_trades_per_day)) else float(args.min_trades_per_day)

    min_reinvest_count = int(args.min_reinvest_count or 0)
    min_self_learn_tunes = int(args.min_self_learn_tunes or 0)

    filtered: List[Tuple[BacktestConfig, Mapping[str, Any]]] = []
    for cfg, st in ok_pairs:
        n_trades = _safe_int(st.get("n_trades"), 0)

        if require_trades and n_trades == 0:
            continue
        if min_trades > 0 and n_trades < min_trades:
            continue

        if min_win_rate is not None:
            wr = st.get("win_rate")
            if wr is None:
                n_wins = _safe_float(st.get("n_wins"), 0.0)
                wr = (n_wins / n_trades) if n_trades > 0 else 0.0
            if _safe_float(wr, 0.0) < min_win_rate:
                continue

        if max_drawdown is not None and abs(_safe_float(st.get("max_drawdown_pct"), 0.0)) > max_drawdown:
            continue
        if max_p95_dd_trade is not None and abs(_safe_float(st.get("p95_drawdown_trade_pct"), 0.0)) > max_p95_dd_trade:
            continue
        if min_sharpe is not None and _safe_float(st.get("sharpe"), 0.0) < min_sharpe:
            continue
        if min_profit_factor is not None and _safe_float(st.get("profit_factor"), 0.0) < min_profit_factor:
            continue
        if min_avg_pnl_pct is not None and _safe_float(st.get("avg_pnl_pct"), 0.0) < min_avg_pnl_pct:
            continue
        if min_sortino is not None and _safe_float(st.get("sortino"), 0.0) < min_sortino:
            continue
        if min_daily_pnl_pct is not None and _safe_float(st.get("daily_pnl_pct"), 0.0) < min_daily_pnl_pct:
            continue
        if min_monthly_pnl_pct is not None and _safe_float(st.get("monthly_pnl_pct"), 0.0) < min_monthly_pnl_pct:
            continue
        if min_monthly_pnl_compound_pct is not None and _safe_float(st.get("monthly_pnl_compound_pct"), 0.0) < min_monthly_pnl_compound_pct:
            continue
        if min_equity_final_pct is not None and _safe_float(st.get("equity_final_pct"), 0.0) < min_equity_final_pct:
            continue
        if min_cagr_annual_pct is not None and _safe_float(st.get("cagr_annual_pct"), 0.0) < min_cagr_annual_pct:
            continue
        if min_net_pnl_pct is not None and _safe_float(st.get("net_pnl_pct"), 0.0) < min_net_pnl_pct:
            continue

        if min_reinvest_count > 0 and _safe_int(st.get("reinvest_count"), 0) < min_reinvest_count:
            continue
        if min_self_learn_tunes > 0 and _safe_int(st.get("self_learning_tune_count"), 0) < min_self_learn_tunes:
            continue

        if min_edge_score is not None and _safe_float(st.get("edge_score"), 0.0) < min_edge_score:
            continue
        if min_pnl_dd_ratio is not None and _safe_float(st.get("pnl_dd_ratio"), 0.0) < min_pnl_dd_ratio:
            continue
        if min_calmar is not None and _safe_float(st.get("calmar_ratio"), 0.0) < min_calmar:
            continue
        if min_win_loss_ratio is not None and _safe_float(st.get("win_loss_ratio"), 0.0) < min_win_loss_ratio:
            continue
        if min_expectancy_pct is not None and _safe_float(st.get("expectancy_pct"), 0.0) < min_expectancy_pct:
            continue
        if min_kelly_fraction is not None and _safe_float(st.get("kelly_fraction"), 0.0) < min_kelly_fraction:
            continue
        if min_kelly_safe_fraction is not None and _safe_float(st.get("kelly_safe_fraction"), 0.0) < min_kelly_safe_fraction:
            continue
        if min_trades_per_day is not None and _safe_float(st.get("trades_per_day"), 0.0) < min_trades_per_day:
            continue

        filtered.append((cfg, st))

    if not filtered:
        print("All configs filtered out by result filters.")
        return

    sort_key_override = (args.sort_key or "").strip() or None
    sort_asc = args.sort_asc is True
    top_n = args.top_n if args.top_n and args.top_n > 0 else None
    metrics_override = (args.metrics or "").strip() or None

    ranked, resolved_sort_key = print_results_table(
        [c for c, _ in filtered],
        [s for _, s in filtered],
        sort_key_override=sort_key_override,
        sort_ascending=sort_asc,
        top_n=top_n,
        metrics_override=metrics_override,
    )
    if not ranked:
        return

    # Target monthly PnL commentary
    if args.target_monthly_pnl_pct and float(args.target_monthly_pnl_pct) > 0:
        best_cfg, best_st = ranked[0]
        best_monthly = _safe_float(best_st.get("monthly_pnl_pct", best_st.get("net_pnl_pct")), 0.0)
        best_dd = _safe_float(best_st.get("max_drawdown_pct"), 0.0)
        print(
            f"Target monthly_pnl_pct: {float(args.target_monthly_pnl_pct):.2f}% | "
            f"best config: {best_cfg.tag or ''} → monthly_pnl_pct≈{best_monthly:.2f}%, "
            f"max_dd≈{best_dd:.2f}%"
        )

    # Optional group summary
    if (args.group_summary_by or "").strip():
        key = args.group_summary_by.strip()

        def _get_group_value(cfg: BacktestConfig) -> str:
            v = getattr(cfg, key, None)
            return str(v) if v is not None and str(v).strip() else "None"

        groups: Dict[str, List[Tuple[BacktestConfig, Mapping[str, Any]]]] = {}
        for cfg, st in ranked:
            g = _get_group_value(cfg)
            groups.setdefault(g, []).append((cfg, st))

        print(f"\nGroup summary by '{key}':")
        for g, items in sorted(groups.items(), key=lambda kv: len(kv[1]), reverse=True):
            best = items[0][1]
            best_sort_val = _safe_float(best.get(resolved_sort_key), 0.0) if resolved_sort_key else 0.0
            best_net = _safe_float(best.get("net_pnl_pct", best.get("pnl_total_pct")), 0.0)
            print(f"  {g:>12} | n={len(items):<4} | best_{resolved_sort_key or 'score'}={best_sort_val:.3f} | best_net_pnl_pct={best_net:.2f}")
        print("")

    # Verbose dump
    if args.verbose is True:
        for cfg, st in ranked:
            print(f"--- {cfg.tag or ''} ---")
            print("config:", json.dumps(cfg.to_run_kwargs(), indent=2, default=str))
            print("stats :", json.dumps(dict(st), indent=2, default=str))
            print("")

    # Print best json
    if args.print_best_json is True:
        best_cfg, best_st = ranked[0]
        payload = {"config": best_cfg.to_run_kwargs(), "stats": dict(best_st)}
        print("\nBest config (JSON):")
        print(json.dumps(payload, indent=2, default=str))
        print("")

    # Exports
    if args.export_json:
        auto_append = bool(args.append_export_json)
        if args.resume_json and args.export_json and os.path.abspath(args.resume_json) == os.path.abspath(args.export_json):
            auto_append = True
        export_json(args.export_json, ranked, append=auto_append)

    if args.export_csv:
        export_csv(args.export_csv, ranked)
    if args.export_trades_json:
        export_trades_json(args.export_trades_json, ranked)
    if args.export_env_best:
        export_env_best(args.export_env_best, ranked[0][0], ranked[0][1], resolved_sort_key)

    # Optional: log sweep to ai_db
    if args.save_sweep_to_ai_db is True:
        if ai_db is None:
            print("NOTE: --save-sweep-to-ai-db set but ai_db import failed; skipping.")
        else:
            log_fn = getattr(ai_db, "log_backtest_sweep", None)
            if callable(log_fn):
                meta = {
                    "group_id": args.group_id or None,
                    "run_note": args.run_note or None,
                    "command": " ".join(sys.argv),
                    "generated_at": datetime.utcnow().isoformat() + "Z",
                    "n_configs_survived": len(ranked),
                    "sort_key": resolved_sort_key,
                }
                try:
                    out = log_fn(meta, [c.to_run_kwargs() for c, _ in ranked], [dict(s) for _, s in ranked])
                    if inspect.isawaitable(out):
                        await out
                    print("Logged backtest sweep to ai_db.log_backtest_sweep().")
                except Exception as e:
                    print(f"WARNING: ai_db.log_backtest_sweep() failed: {e}")
            else:
                print("NOTE: --save-sweep-to-ai-db set but ai_db.log_backtest_sweep() not found; skipping.")


def main() -> None:
    parser = build_arg_parser()
    args = parser.parse_args()
    try:
        asyncio.run(_run_backtests(args))
    except KeyboardInterrupt:
        print("Interrupted by user.", file=sys.stderr)


if __name__ == "__main__":
    main()
