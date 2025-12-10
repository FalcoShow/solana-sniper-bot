# utils_pkg/backtest_engine.py
from __future__ import annotations

"""
utils_pkg/backtest_engine.py

Quant-grade backtest engine for AI entry / risk / reinvestor configs (V3+++).
=============================================================================

Single public entrypoint:

    async run_backtest(config: dict) -> dict

Core responsibilities
---------------------
• Pull historical candidate rows from Postgres via utils_pkg.ai_db
• Use the SAME entry stack as live trading (configurable engine):
      - Default: utils_pkg.ai_entry_logic.score_entry(...)
      - Optional: utils_pkg.ai_buy_scoring.score_only(...)

• Apply config-driven gates:
      - ENTRY_SCORE_MIN                 (raw heuristic gate)
      - ENTRY_MIN_SCORE                 (blended gate)
      - max_positions                   (capacity / equity scaling)
      - risk_mult_override              (global risk multiplier)
      - RISK_PROFILE                    (conservative/balanced/aggressive)
      - regime_risk_mode_lock           (force risk_off/normal/risk_on)
      - ENTRY_REQUIRE_RISK_CAPACITY
      - BT_MIN_RISK_CAPACITY / BT_MAX_RISK_CAPACITY
      - BT_REQUIRE_POSITIVE_RC
      - BT_MAX_TRADES cap
      - limit_candidates / BT_LIMIT_CANDIDATES
      - label_include / label_exclude
      - BT_MIN_SMART_WALLET_SCORE / BT_REQUIRE_SMART_WALLET_FLAG
      - BT_DEDUP_MODE / BT_DEDUP_WINDOW_SEC (mint-level deduplication)
      - BT_SAMPLE_WEIGHT_MODE / BT_LABEL_WEIGHTS_JSON
      - BT_USE_LIVE_RISK_MANAGER (allocation from risk_capacity_score)
      - BT_RISK_EQUITY_HALT_DD (equity drawdown kill switch)
      - BT_ENABLE_SELF_LEARNING (+ BT_SELF_LEARN_* knobs)
      - BT_ENABLE_REINVESTOR (+ BT_REINVEST_* knobs)
      - BT_USE_AI_RISK_MODEL (+ BT_AI_RISK_* knobs, if ai_risk_model present)
      - BT_TRADE_COST_BPS / BT_JITO_TIP_BPS (microstructure cost model)
      - BT_TRADE_COST_MODE ("per_trade" vs "half_side")
      - BT_LOG_TRADES / BT_LOG_FEATURES
      - BT_LOG_EQUITY_SERIES (optional equity curve logging)
      - BT_RESULT_SEGMENT (optional: "train" / "test" segment filter)
      - BT_ALLOC_MODE (optional: "simple" | "risk_capacity")
      - BT_EQUITY_INIT           (initial equity, default 1.0)
      - BT_EQUITY_CAP_MULT       (cap equity at init * multiplier; 0/None = no cap)
      - BT_START_TS / BT_END_TS  (optional time-window filter on created_at)
      - BT_SCORE_ENGINE          ("entry_logic" | "ai_buy_scoring")
      - BT_USE_AI_BUY_SCORING    (bool flag to force ai_buy_scoring engine)

Extra V3+++ quant helpers
-------------------------
• Exposure metrics:
      - avg_exposure, p95_exposure (notional fraction-of-equity per trade)
• Microstructure cost analytics:
      - trade_cost_total_pct, trade_cost_avg_pct
• Risk multiplier diagnostics:
      - avg_risk_mult_effective (risk_mult_override * profile * regime * AI sizer)
• Risk capacity bucket performance:
      - rc_bucket_stats (PnL & win-rate per RC bucket)
• Equity-path risk:
      - ulcer_index_pct, ulcer_perf_ratio, equity_vol_pct
• Tail & run diagnostics:
      - pnl_p05_pct / pnl_p95_pct / pnl_cvar_p05_pct
      - max_consec_wins / max_consec_losses
      - skewness / kurtosis_excess
      - rolling_sharpe_50 / rolling_sharpe_100

Assumptions about ai_db
-----------------------
Backtest data source MUST provide rows via one of:

    def ai_db.iter_backtest_rows(config: dict) -> Iterator[Mapping]: ...
    def ai_db.iter_training_rows(config: dict) -> Iterator[Mapping]: ...

or async equivalents. We adapt both sync & async.

Each row is expected to be a mapping with at least:

    - "features"        : dict        (FeatureBuilder output)
      OR "features_json": str (JSON)

    - "pnl_pct"         : float       (realized PnL as % of notional, e.g. +12.5)

Optionally:

    - "label"           : str         (e.g. "monster_win","win","loss","chop")
    - "max_dd_pct"      : float       (max drawdown % during the trade)
    - "hold_sec"        : float       (seconds in trade)
    - "created_at"      : datetime    (candidate timestamp; used for ordering & dedup)
    - "mint"            : str         (token mint; used for dedup if available)
    - "ai_risk_score"   : float       (pre-computed AI risk score, 0..1 or 0..100)
    - "ai_risk_conf"    : float       (optional confidence 0..1)
    - "segment"         : str         ("train","test",...) if you want BT_RESULT_SEGMENT

If your column names differ, adapt `_normalize_row()` and `_extract_mint()` only.

Public API
----------
    async run_backtest(config: dict) -> dict
        Returns a flat metrics dict suitable for printing / CSV export.
"""

import asyncio
import json
import math
import statistics
from dataclasses import dataclass
from datetime import datetime
from typing import Any, AsyncIterator, Dict, List, Mapping, Optional, Tuple

from logging_setup import get_logger
from utils_pkg import ai_db
from utils_pkg import ai_entry_logic as _entry

# Optional modules (all guarded; backtest still works if missing)
try:  # ai_risk_model overlay (backtest + live share the same brain)
    from utils_pkg import ai_risk_model as _ai_risk_model  # type: ignore[attr-defined]
except Exception:  # pragma: no cover
    _ai_risk_model = None  # type: ignore[assignment]

try:  # optional self-learning brain for richer backtest hooks
    from utils_pkg import self_learning_ai_strategy as _sl_mod  # type: ignore[attr-defined]
except Exception:  # pragma: no cover
    _sl_mod = None  # type: ignore[assignment]

try:  # optional regime helper for risk multipliers
    from utils_pkg import regime as _regime_mod  # type: ignore[attr-defined]
except Exception:  # pragma: no cover
    _regime_mod = None  # type: ignore[assignment]

try:  # optional data logger for research audit trail
    from utils_pkg import data_logger as _data_logger  # type: ignore[attr-defined]
except Exception:  # pragma: no cover
    _data_logger = None  # type: ignore[assignment]

try:  # optional risk manager (to keep backtest & live risk in sync if desired)
    from utils_pkg import risk_manager as _rm  # type: ignore[attr-defined]
except Exception:  # pragma: no cover
    _rm = None  # type: ignore[assignment]

try:  # optional reinvestor module (for deeper reinvest simulation)
    from utils_pkg import reinvestor as _reinv  # type: ignore[attr-defined]
except Exception:  # pragma: no cover
    _reinv = None  # type: ignore[assignment]

try:  # optional V3+++ buy-scoring engine shared with live entry stack
    from utils_pkg import ai_buy_scoring as _buy_scorer  # type: ignore[attr-defined]
except Exception:  # pragma: no cover
    _buy_scorer = None  # type: ignore[assignment]

log = get_logger(__name__)


# =============================================================================
# Datamodels
# =============================================================================


@dataclass
class BacktestRow:
    """Normalized view of a single candidate+outcome row."""

    features: Dict[str, Any]
    pnl_pct: float
    label: Optional[str] = None
    max_dd_pct: Optional[float] = None
    hold_sec: Optional[float] = None
    created_at: Optional[datetime] = None
    ai_risk_score: Optional[float] = None
    ai_risk_conf: Optional[float] = None
    raw: Mapping[str, Any] | None = None  # original row for debugging


@dataclass
class BacktestTrade:
    """
    Normalized per-trade record for optional logging / analysis.
    Used when BT_LOG_TRADES and/or BT_LOG_EQUITY_SERIES is enabled.
    """

    pnl_pct: float
    pnl_pct_gross: float
    heur_score: float
    blended_score: float
    ai_score: Optional[float]
    label: Optional[str]
    risk_capacity_score: Optional[float]
    smart_wallet_score: Optional[float]
    smart_wallet_flag: bool
    created_at: Optional[datetime]
    regime_mult: float
    equity: float
    drawdown: float
    trade_cost_bps: float
    jito_tip_bps: float
    ai_risk_score: Optional[float]
    ai_risk_conf: Optional[float]
    ai_risk_mode: Optional[str]
    ai_risk_sizer_mult: float
    features: Optional[Dict[str, Any]] = None


# =============================================================================
# Helpers: feature / row normalization
# =============================================================================


def _ensure_dict(obj: Any) -> Dict[str, Any]:
    """Best-effort parse of arbitrary feature payload into a dict."""
    if obj is None:
        return {}
    if isinstance(obj, dict):
        return dict(obj)
    if isinstance(obj, str):
        try:
            return json.loads(obj)
        except Exception:
            return {}
    try:
        return dict(obj)
    except Exception:
        return {}


def _maybe_float(val: Any) -> Optional[float]:
    if val is None:
        return None
    try:
        f = float(val)
        if math.isnan(f) or math.isinf(f):
            return None
        return f
    except Exception:
        return None


def _normalize_row(row: Mapping[str, Any]) -> Optional[BacktestRow]:
    """
    Coerce an arbitrary DB row into BacktestRow.

    This is the ONLY place you should edit if your schema changes.
    """
    # Features
    feats_raw = (
        row.get("features")
        or row.get("feature_json")
        or row.get("features_json")
        or row.get("feature_vec")
    )
    features = _ensure_dict(feats_raw)

    # Outcome / label
    pnl_pct: Optional[float] = None
    for key in ("pnl_pct", "pnl_percent", "return_pct", "ret_pct"):
        if key in row and row[key] is not None:
            pnl_pct = _maybe_float(row[key])
            if pnl_pct is not None:
                break

    if pnl_pct is None:
        # Without PnL we cannot compute portfolio metrics; skip row.
        return None

    label: Optional[str] = None
    for key in ("label", "outcome_label", "y_label"):
        if key in row and row[key] is not None:
            label = str(row[key])
            break

    max_dd_pct: Optional[float] = None
    for key in ("max_dd_pct", "max_drawdown_pct", "dd_pct"):
        if key in row and row[key] is not None:
            max_dd_pct = _maybe_float(row[key])
            if max_dd_pct is not None:
                break

    hold_sec: Optional[float] = None
    for key in ("hold_sec", "hold_time_sec", "duration_sec"):
        if key in row and row[key] is not None:
            hold_sec = _maybe_float(row[key])
            if hold_sec is not None:
                break

    created_at: Optional[datetime] = None
    for key in ("created_at", "entry_ts", "timestamp"):
        if key in row and row[key] is not None:
            val = row[key]
            if isinstance(val, datetime):
                created_at = val
                break
            try:
                created_at = datetime.fromisoformat(str(val).replace("Z", "+00:00"))
                break
            except Exception:
                pass

    # AI risk signals (optional)
    ai_risk_score = None
    for key in ("ai_risk_score", "risk_score", "fraud_score"):
        if key in row and row[key] is not None:
            ai_risk_score = _maybe_float(row[key])
            if ai_risk_score is not None:
                break
    ai_risk_conf = None
    for key in ("ai_risk_conf", "risk_confidence", "fraud_conf"):
        if key in row and row[key] is not None:
            ai_risk_conf = _maybe_float(row[key])
            if ai_risk_conf is not None:
                break

    return BacktestRow(
        features=features,
        pnl_pct=float(pnl_pct),
        label=label,
        max_dd_pct=max_dd_pct,
        hold_sec=hold_sec,
        created_at=created_at,
        ai_risk_score=ai_risk_score,
        ai_risk_conf=ai_risk_conf,
        raw=row,
    )


def _extract_mint(row: BacktestRow) -> Optional[str]:
    """
    Best-effort mint extraction for dedup logic.

    Priority:
      1) features["mint"]
      2) features["mint_str"]
      3) raw["mint"]
      4) None if unavailable
    """
    f = row.features
    for key in ("mint", "mint_str"):
        if key in f and f[key]:
            return str(f[key])
    if row.raw is not None and "mint" in row.raw and row.raw["mint"]:
        return str(row.raw["mint"])
    return None


def _parse_ts_val(val: Any) -> Optional[datetime]:
    """
    Parse config timestamp-ish values into datetime.

    Accepts:
      - datetime
      - float/int epoch seconds
      - ISO8601-ish strings (with optional 'Z')
    """
    if val is None:
        return None
    if isinstance(val, datetime):
        return val
    s = str(val).strip()
    if not s:
        return None

    # Try numeric epoch seconds
    try:
        num = float(s)
        if math.isfinite(num) and num > 10_000_000:  # ~1970+
            return datetime.fromtimestamp(num)
    except Exception:
        pass

    # Try ISO8601
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception:
        return None


# =============================================================================
# Data source: ai_db
# =============================================================================


async def _iter_rows_from_ai_db(config: Mapping[str, Any]) -> AsyncIterator[BacktestRow]:
    """
    Adapter over ai_db.

    Preferred contract:

        def ai_db.iter_backtest_rows(config: dict) -> Iterator[Mapping]:

    Fallback:

        def ai_db.iter_training_rows(config: dict) -> Iterator[Mapping]:

    Async variants are also supported.

    Rows are normalized via _normalize_row; rows that cannot be normalized are
    silently skipped (but we log at debug level).
    """
    if hasattr(ai_db, "iter_backtest_rows"):
        it = ai_db.iter_backtest_rows(config)  # type: ignore[attr-defined]
    elif hasattr(ai_db, "iter_training_rows"):
        it = ai_db.iter_training_rows(config)  # type: ignore[attr-defined]
    else:
        raise RuntimeError(
            "ai_db must expose iter_backtest_rows(config) or iter_training_rows(config) "
            "to feed backtest_engine.run_backtest()."
        )

    # Async iterator path
    if hasattr(it, "__aiter__"):
        async for row in it:  # type: ignore[operator]
            br = _normalize_row(row)
            if br is None:
                log.debug("backtest_engine: skipping row without pnl_pct")
                continue
            yield br
        return

    # Sync iterator path – offload to a thread to avoid blocking the loop
    def _collect_sync_rows() -> List[BacktestRow]:
        out: List[BacktestRow] = []
        for r in it:  # type: ignore[operator]
            br = _normalize_row(r)
            if br is not None:
                out.append(br)
        return out

    rows = await asyncio.to_thread(_collect_sync_rows)
    for br in rows:
        yield br


# =============================================================================
# Scoring helpers: use ai_entry_logic V3+++ stack / ai_buy_scoring V3+++
# =============================================================================


def _clip01(x: float) -> float:
    return max(0.0, min(1.0, float(x)))


async def _compute_scores_entry_logic(
    features: Dict[str, Any],
    cfg: Mapping[str, Any],
) -> Tuple[float, Optional[float], float]:
    """
    Baseline scoring via utils_pkg.ai_entry_logic (default engine).

    Returns:
        (heur_score, ai_score_or_None, blended_score)
    """
    # Prefer the V3+++ public API if available
    if hasattr(_entry, "score_entry"):
        try:
            # Try with explicit ctx first
            res = _entry.score_entry(features, ctx={"mode": "backtest", "config": cfg})  # type: ignore[attr-defined]
        except TypeError:
            # Fallback: legacy signature score_entry(features)
            res = _entry.score_entry(features)  # type: ignore[attr-defined]

        if asyncio.iscoroutine(res):
            res = await res  # type: ignore[assignment]

        # Allow either dataclass-like or dict-like result
        heur = getattr(res, "heur_score", None)
        ai = getattr(res, "ai_score", None)
        blended = getattr(res, "blended_score", None)

        if heur is None and isinstance(res, dict):
            heur = res.get("heur_score")
            ai = res.get("ai_score")
            blended = res.get("blended_score")

        if heur is not None and blended is not None:
            heur_f = _clip01(float(heur))
            ai_f = _clip01(float(ai)) if ai is not None else None
            blended_f = _clip01(float(blended))
            return heur_f, ai_f, blended_f

        # If something weird happens, fall back to low-level functions below

    # Legacy / low-level path – uses the same primitives live code does
    heur_score = float(_entry._heuristic_score(features))  # type: ignore[attr-defined]

    # Decide blend weight
    w: Optional[float] = None
    for key in ("ENTRY_AI_WEIGHT", "entry_ai_weight"):
        if key in cfg and cfg[key] is not None:
            try:
                w = float(cfg[key])
                break
            except Exception:
                pass

    if w is None:
        # Fall back to module-level default if present, else 0.7
        w = getattr(_entry, "ENTRY_AI_WEIGHT", 0.7)
    w = _clip01(w)

    ai_raw = await _entry._ai_score(features)  # type: ignore[attr-defined]
    if ai_raw is None:
        blended = _clip01(heur_score)
        return _clip01(heur_score), None, blended

    ai_score = float(ai_raw)
    blended = _clip01(w * ai_score + (1.0 - w) * heur_score)
    return _clip01(heur_score), _clip01(ai_score), blended


async def _compute_scores_ai_buy_scoring(
    features: Dict[str, Any],
    cfg: Mapping[str, Any],
) -> Optional[Tuple[float, Optional[float], float]]:
    """
    Optional scoring via utils_pkg.ai_buy_scoring (V3+++ buy-scoring engine).

    This is resilient to schema / API changes:

      • If Candidate(...) exists, we try to build one with features+ctx.
      • Otherwise we pass features directly to score_only().
      • We accept:
            - dataclass-like with .heur_score/.ai_score/.blended_score
            - dict with same keys
            - tuple/list where [0] is blended_score

    Returns:
        (heur_score, ai_score_or_None, blended_score) OR None on failure.
    """
    if _buy_scorer is None:
        return None
    if not hasattr(_buy_scorer, "score_only"):
        return None

    try:
        candidate: Any
        # Try Candidate dataclass if present
        if hasattr(_buy_scorer, "Candidate"):
            Cand = getattr(_buy_scorer, "Candidate")
            try:
                candidate = Cand(features=features, ctx={"mode": "backtest", "config": cfg})
            except TypeError:
                # Fallback: some versions may only accept features
                try:
                    candidate = Cand(features=features)
                except Exception:
                    candidate = features
        else:
            candidate = features

        res = _buy_scorer.score_only(candidate)  # type: ignore[misc]
        if asyncio.iscoroutine(res):
            res = await res  # type: ignore[assignment]

        heur = getattr(res, "heur_score", None)
        ai = getattr(res, "ai_score", None)
        blended = getattr(res, "blended_score", None)

        if isinstance(res, dict):
            heur = heur if heur is not None else res.get("heur_score")
            ai = ai if ai is not None else res.get("ai_score")
            blended = blended if blended is not None else res.get("blended_score")

        # If only blended is provided (e.g. res is tuple/list)
        if blended is None and isinstance(res, (tuple, list)) and len(res) >= 1:
            blended = float(res[0])
            # heur/ai will be inferred as blended for safety
            heur = blended if heur is None else heur
            ai = ai  # leave None if not present

        # Final sanity
        if blended is None:
            return None

        heur_f = _clip01(float(heur) if heur is not None else float(blended))
        ai_f = _clip01(float(ai)) if ai is not None else None
        blended_f = _clip01(float(blended))

        return heur_f, ai_f, blended_f

    except Exception:
        log.debug("backtest_engine: ai_buy_scoring scoring failed; falling back to entry_logic.", exc_info=True)
        return None


async def _compute_scores(
    features: Dict[str, Any],
    cfg: Mapping[str, Any],
) -> Tuple[float, Optional[float], float]:
    """
    Unified scoring router.

    Engine selection (in priority order):
      1) BT_SCORE_ENGINE / score_engine:
           "ai_buy_scoring" / "buy" / "buy_scoring" → ai_buy_scoring
           "entry_logic" / default                  → ai_entry_logic

      2) BT_USE_AI_BUY_SCORING / use_ai_buy_scoring (bool flag)
         if true and ai_buy_scoring is available → ai_buy_scoring

      3) Fallback → ai_entry_logic
    """
    engine_raw = (
        str(cfg.get("BT_SCORE_ENGINE") or cfg.get("score_engine") or "")
        .strip()
        .lower()
    )

    use_ai_buy_flag = False
    for k in ("BT_USE_AI_BUY_SCORING", "use_ai_buy_scoring"):
        if k in cfg and cfg[k] is not None:
            v = str(cfg[k]).strip().lower()
            if v in ("1", "true", "yes", "y", "on"):
                use_ai_buy_flag = True
                break

    # Explicit engine selection via BT_SCORE_ENGINE
    use_buy_engine = engine_raw in (
        "ai_buy_scoring",
        "buy",
        "buy_scoring",
        "buy_engine",
    ) or use_ai_buy_flag

    if use_buy_engine:
        scored = await _compute_scores_ai_buy_scoring(features, cfg)
        if scored is not None:
            return scored

    # Default / fallback: entry_logic stack
    return await _compute_scores_entry_logic(features, cfg)


# =============================================================================
# Config helpers
# =============================================================================


def _get_cfg_float(cfg: Mapping[str, Any], *keys: str, default: float) -> float:
    for k in keys:
        if k in cfg and cfg[k] is not None:
            try:
                return float(cfg[k])
            except Exception:
                pass
    return float(default)


def _get_cfg_int(cfg: Mapping[str, Any], *keys: str, default: int) -> int:
    for k in keys:
        if k in cfg and cfg[k] is not None:
            try:
                return int(cfg[k])
            except Exception:
                pass
    return int(default)


def _get_cfg_bool(cfg: Mapping[str, Any], *keys: str, default: bool = False) -> bool:
    for k in keys:
        if k in cfg and cfg[k] is not None:
            v = str(cfg[k]).strip().lower()
            if v in ("1", "true", "yes", "y", "on"):
                return True
            if v in ("0", "false", "no", "n", "off"):
                return False
    return bool(default)


def _risk_profile_scale(profile: Optional[str]) -> float:
    if not profile:
        return 1.0
    p = profile.strip().lower()
    if p.startswith("cons"):  # conservative
        return 0.7
    if p.startswith("agg"):  # aggressive
        return 1.3
    return 1.0  # "balanced" / default


def _regime_mult_from_features(
    features: Mapping[str, Any],
    regime_mode_lock: Optional[int],
) -> float:
    """
    Derive a regime-aware multiplier from features + optional lock.

    Priority:
      1) If utils_pkg.regime exposes risk_mult_from_features, defer to it.
      2) regime_mode_lock:
            0 -> risk_off   (0.8)
            1 -> normal     (1.0)
            2 -> risk_on    (1.2)
      3) features["regime_risk_multiplier"]
      4) features["regime_label_numeric"] in [-1,1] → [0.8,1.2]
      5) fallback 1.0
    """
    # 1) delegate to regime module if available (keeps engine + live in sync)
    if _regime_mod is not None and hasattr(_regime_mod, "risk_mult_from_features"):
        try:
            return float(
                _regime_mod.risk_mult_from_features(features, regime_mode_lock)  # type: ignore[attr-defined]
            )
        except Exception:  # pragma: no cover
            log.debug("regime.risk_mult_from_features failed, falling back.", exc_info=True)

    # 2) explicit lock
    if regime_mode_lock in (0, 1, 2):
        if regime_mode_lock == 0:
            return 0.8
        if regime_mode_lock == 2:
            return 1.2
        return 1.0

    # 3) explicit feature multiplier
    if "regime_risk_multiplier" in features:
        try:
            v = float(features["regime_risk_multiplier"])
            if math.isfinite(v) and v > 0:
                return v
        except Exception:
            pass

    # 4) label_numeric → [0.8, 1.2]
    label_num = features.get("regime_label_numeric")
    if label_num is not None:
        try:
            ln = float(label_num)
            ln = max(-1.0, min(1.0, ln))
            return 1.0 + 0.2 * ln
        except Exception:
            pass

    # 5) default
    return 1.0


def _compute_sharpe(returns: List[float]) -> float:
    """
    Simple Sharpe-like metric on per-trade equity returns.
    Assumes each trade is a "period". No risk-free rate, no annualization.
    """
    if not returns:
        return 0.0
    if len(returns) == 1:
        return returns[0]
    avg = statistics.mean(returns)
    std = statistics.pstdev(returns)
    if std <= 1e-9:
        return 0.0
    return avg / std


def _compute_downside_dev(returns: List[float]) -> float:
    """
    Downside deviation of per-trade PnL % (only negative returns).
    Used for Sortino-style metric.
    """
    downsides = [r for r in returns if r < 0]
    if not downsides:
        return 0.0
    if len(downsides) == 1:
        return abs(downsides[0])
    var = statistics.pvariance(downsides, mu=0.0)
    return math.sqrt(var)


def _compute_skew_kurtosis(returns: List[float]) -> Tuple[float, float]:
    """
    Simple skewness and excess kurtosis on PnL %, for tail-shape diagnostics.
    Skewness >0 → right-tail heavy; kurtosis_excess >0 → fatter tails than normal.
    """
    n = len(returns)
    if n < 3:
        return 0.0, 0.0
    mean = statistics.mean(returns)
    std = statistics.pstdev(returns)
    if std <= 1e-9:
        return 0.0, 0.0
    m3 = sum((x - mean) ** 3 for x in returns) / n
    m4 = sum((x - mean) ** 4 for x in returns) / n
    skew = m3 / (std ** 3)
    kurt_excess = m4 / (std ** 4) - 3.0
    return skew, kurt_excess


def _compute_consecutive_runs(returns: List[float]) -> Tuple[int, int]:
    """
    Compute max consecutive wins / losses based on sign of PnL.
    IMPORTANT: expects returns in chronological order, not sorted.
    """
    max_wins = 0
    max_losses = 0
    cur_wins = 0
    cur_losses = 0

    for r in returns:
        if r > 0:
            cur_wins += 1
            cur_losses = 0
        elif r < 0:
            cur_losses += 1
            cur_wins = 0
        else:
            # flat trade resets both
            cur_wins = 0
            cur_losses = 0
        max_wins = max(max_wins, cur_wins)
        max_losses = max(max_losses, cur_losses)

    return max_wins, max_losses


def _safe_quantile(values: List[float], q: float) -> float:
    """
    Safe quantile helper in [0,1]. Uses simple index-based approximation.
    """
    if not values:
        return 0.0
    if len(values) == 1:
        return values[0]
    v_sorted = sorted(values)
    q = max(0.0, min(1.0, float(q)))
    idx = int(q * (len(v_sorted) - 1))
    return v_sorted[idx]


# =============================================================================
# Self-learning (backtest-only) helper
# =============================================================================


@dataclass
class _SelfLearningState:
    enabled: bool
    use_llm: bool
    interval_sec: Optional[int]
    min_trades: int
    max_tunes_per_day: Optional[int]

    tune_count: int = 0
    last_tune_ts: Optional[datetime] = None

    # Rolling window since last tune
    window_pnls: List[float] = None  # type: ignore[assignment]
    window_ts: List[Optional[datetime]] = None  # type: ignore[assignment]

    def __post_init__(self) -> None:
        if self.window_pnls is None:
            self.window_pnls = []
        if self.window_ts is None:
            self.window_ts = []

    def maybe_tune(
        self,
        created_at: Optional[datetime],
        pnl_pct: float,
        equity: float,  # reserved for future hooks (e.g. equity-based tuning)
        score_min: float,
        blended_gate: float,
        risk_mult_override: float,
    ) -> Tuple[float, float, float]:
        """
        Lightweight adaptive logic (backtest-only):

          - Keep a window of trades since last tune.
          - If enough trades + interval elapsed:
                • if win_rate < ~45% or window PnL < 0:
                      → tighten gates, reduce risk
                • if win_rate > ~60% and window PnL > 0:
                      → gently increase risk

        Returns potentially updated:
            (score_min, blended_gate, risk_mult_override)
        """
        if not self.enabled:
            return score_min, blended_gate, risk_mult_override

        self.window_pnls.append(float(pnl_pct))
        self.window_ts.append(created_at)

        n = len(self.window_pnls)
        if n < max(1, self.min_trades):
            return score_min, blended_gate, risk_mult_override

        # Time-based interval gate (if timestamps + interval given)
        if self.interval_sec is not None and self.interval_sec > 0:
            if created_at is not None and self.last_tune_ts is not None:
                delta = (created_at - self.last_tune_ts).total_seconds()
                if delta < self.interval_sec:
                    return score_min, blended_gate, risk_mult_override

        # Max tunes per day guard
        if self.max_tunes_per_day is not None and self.max_tunes_per_day > 0:
            if created_at is not None and self.window_ts and self.window_ts[0] is not None:
                days = max(
                    1.0,
                    (created_at - self.window_ts[0]).total_seconds() / 86400.0,
                )
                if self.tune_count >= self.max_tunes_per_day * days:
                    return score_min, blended_gate, risk_mult_override

        # Evaluate window performance
        wins = [p for p in self.window_pnls if p > 0]
        losses = [p for p in self.window_pnls if p < 0]
        win_rate = len(wins) / float(n) if n > 0 else 0.0
        window_pnl = sum(self.window_pnls)

        new_score_min = score_min
        new_blended_gate = blended_gate
        new_risk_mult = risk_mult_override

        defensive = (win_rate < 0.45) or (window_pnl < 0.0)
        offensive = (win_rate > 0.60) and (window_pnl > 0.0) and not defensive

        # Defensive: tighten gates and reduce risk
        if defensive:
            new_score_min = min(0.98, score_min + 0.02)
            if blended_gate > 0.0:
                new_blended_gate = min(0.98, blended_gate + 0.02)
            new_risk_mult = max(0.5, risk_mult_override * 0.9)

        # Offensive: gently scale risk up, but cap
        elif offensive:
            new_risk_mult = min(2.0, risk_mult_override * 1.10)

        # No change: exit early
        if (
            abs(new_score_min - score_min) < 1e-9
            and abs(new_blended_gate - blended_gate) < 1e-9
            and abs(new_risk_mult - risk_mult_override) < 1e-9
        ):
            return score_min, blended_gate, risk_mult_override

        # Commit tune
        self.tune_count += 1
        self.last_tune_ts = created_at or self.last_tune_ts

        log.info(
            "backtest_engine.self_learning: tune #%d (win_rate=%.3f, window_pnl=%.2f, "
            "score_min=%.3f→%.3f, blended_gate=%.3f→%.3f, risk_mult=%.3f→%.3f)",
            self.tune_count,
            win_rate,
            window_pnl,
            score_min,
            new_score_min,
            blended_gate,
            new_blended_gate,
            risk_mult_override,
            new_risk_mult,
        )

        # Optional: forward the tune to self_learning_ai_strategy for richer logging
        if _sl_mod is not None:
            try:
                if hasattr(_sl_mod, "record_backtest_tune"):
                    _sl_mod.record_backtest_tune(  # type: ignore[attr-defined]
                        win_rate=win_rate,
                        window_pnl_pct=window_pnl,
                        new_score_min=new_score_min,
                        new_blended_gate=new_blended_gate,
                        new_risk_mult=new_risk_mult,
                        created_at=created_at,
                    )
            except Exception:  # pragma: no cover
                log.debug("self_learning_ai_strategy.record_backtest_tune failed", exc_info=True)

        # Reset window post-tune
        self.window_pnls.clear()
        self.window_ts.clear()

        return new_score_min, new_blended_gate, new_risk_mult


def _build_self_learning_state(config: Mapping[str, Any]) -> _SelfLearningState:
    enabled = _get_cfg_bool(
        config,
        "BT_ENABLE_SELF_LEARNING",
        "enable_self_learning",
        default=False,
    )
    use_llm = _get_cfg_bool(
        config,
        "BT_SELF_LEARN_USE_LLM",
        "self_learning_use_llm",
        default=False,
    )

    interval_sec: Optional[int] = None
    raw_interval = config.get("BT_SELF_LEARN_INTERVAL_SEC") or config.get(
        "self_learning_interval_sec"
    )
    if raw_interval:
        try:
            interval_sec = int(raw_interval)
            if interval_sec <= 0:
                interval_sec = None
        except Exception:
            interval_sec = None

    min_trades = _get_cfg_int(
        config,
        "BT_SELF_LEARN_MIN_TRADES",
        "self_learning_min_trades",
        default=10,
    )

    max_tunes_per_day: Optional[int] = None
    raw_max = config.get("BT_SELF_LEARN_MAX_TUNES_PER_DAY") or config.get(
        "self_learning_max_tunes_per_day"
    )
    if raw_max:
        try:
            max_tunes_per_day = int(raw_max)
            if max_tunes_per_day <= 0:
                max_tunes_per_day = None
        except Exception:
            max_tunes_per_day = None

    return _SelfLearningState(
        enabled=enabled,
        use_llm=use_llm,
        interval_sec=interval_sec,
        min_trades=max(1, min_trades),
        max_tunes_per_day=max_tunes_per_day,
    )


# =============================================================================
# AI risk overlay helper
# =============================================================================


async def _maybe_ai_risk_score(
    row: BacktestRow,
    features: Dict[str, Any],
    cfg: Mapping[str, Any],
) -> Tuple[Optional[float], Optional[float]]:
    """
    Resolve an AI risk score / confidence for this candidate, if overlay is enabled.

    Priority:
      1) Pre-computed row.ai_risk_score / ai_risk_conf
      2) features["ai_risk_score"] / ["ai_risk_conf"]
      3) ai_risk_model module (if present):
            - async score_entry(features, cfg)
            - async score_candidate(features, cfg)
            - sync score_entry / score_candidate / score / evaluate
      4) None if all missing
    """
    # Pre-computed row-level
    score = row.ai_risk_score
    conf = row.ai_risk_conf

    # Feature-level override
    if score is None:
        score = _maybe_float(
            features.get("ai_risk_score")
            or features.get("risk_score")
            or features.get("fraud_score")
        )
    if conf is None:
        conf = _maybe_float(
            features.get("ai_risk_conf")
            or features.get("risk_confidence")
            or features.get("fraud_conf")
        )

    # ai_risk_model module (optional)
    if score is None and _ai_risk_model is not None:
        try:
            fn = None
            for name in ("score_entry", "score_candidate", "score", "evaluate"):
                if hasattr(_ai_risk_model, name):
                    fn = getattr(_ai_risk_model, name)
                    break

            if fn is not None:
                # Handle async vs sync seamlessly
                res = fn(features, cfg)  # type: ignore[misc]
                if asyncio.iscoroutine(res):
                    res = await res  # type: ignore[assignment]

                # Allow a few shapes:
                #   • float
                #   • {"score": float, "conf": float}
                #   • (score, conf)
                if isinstance(res, (int, float)):
                    score = float(res)
                elif isinstance(res, dict):
                    score = _maybe_float(res.get("score"))
                    conf = _maybe_float(res.get("conf") or res.get("confidence"))
                elif isinstance(res, (tuple, list)) and len(res) >= 1:
                    score = _maybe_float(res[0])
                    if len(res) >= 2:
                        conf = _maybe_float(res[1])
        except Exception:  # pragma: no cover
            log.debug("ai_risk_model scoring failed; ignoring overlay for this row.", exc_info=True)

    return score, conf


# =============================================================================
# Main backtest engine
# =============================================================================


async def run_backtest(config: Mapping[str, Any]) -> Dict[str, Any]:
    """
    Main entrypoint for scripts/backtest.py.

    Parameters (keys in `config`)
    ------------------------------
        ENTRY_SCORE_MIN          (raw heuristic gate)
        ENTRY_MIN_SCORE          (blended gate)
        ENTRY_AI_WEIGHT / entry_ai_weight
        BT_SCORE_ENGINE / score_engine
        BT_USE_AI_BUY_SCORING / use_ai_buy_scoring
        max_positions
        RISK_PROFILE / risk_profile
        risk_mult_override
        regime_risk_mode_lock
        ENTRY_REQUIRE_RISK_CAPACITY / require_risk_capacity
        BT_MIN_RISK_CAPACITY / min_risk_capacity
        BT_MAX_RISK_CAPACITY / max_risk_capacity
        BT_REQUIRE_POSITIVE_RC (optional strict guard on rc>0)
        BT_MAX_TRADES / max_trades
        BT_SAMPLE_WEIGHT_MODE / sample_weight_mode
        BT_LABEL_WEIGHTS_JSON / label_weights_json
        BT_LOG_TRADES / log_trades
        BT_LOG_FEATURES / log_features
        BT_LOG_EQUITY_SERIES / log_equity_series
        BT_MIN_SMART_WALLET_SCORE / min_smart_wallet_score
        BT_REQUIRE_SMART_WALLET_FLAG / require_smart_wallet_flag
        BT_DEDUP_MODE / dedup_mode
        BT_DEDUP_WINDOW_SEC / dedup_window_sec
        BT_USE_LIVE_RISK_MANAGER / use_live_risk_manager
        BT_ALLOC_MODE / alloc_mode ("simple" | "risk_capacity")
        BT_RISK_EQUITY_HALT_DD / risk_equity_halt_dd_pct
        BT_ENABLE_SELF_LEARNING / enable_self_learning
        BT_SELF_LEARN_*          (self-learning knobs)
        BT_ENABLE_REINVESTOR / enable_reinvestor
        BT_REINVEST_*            (reinvestor knobs – approx usage)
        BT_USE_AI_RISK_MODEL / use_ai_risk_overlay / use_ai_risk_model
        BT_AI_RISK_MAX_SCORE / ai_risk_max_score
        BT_AI_RISK_MIN_CONF / ai_risk_min_confidence
        BT_AI_RISK_MODE / ai_risk_mode ("filter" | "sizer"), default "filter"
        BT_TRADE_COST_BPS / trade_cost_bps (round-trip model, in bps)
        BT_JITO_TIP_BPS / jito_tip_bps    (extra cost, in bps)
        BT_TRADE_COST_MODE / trade_cost_mode ("per_trade" or "half_side")
        limit_candidates / BT_LIMIT_CANDIDATES
        label_include / label_exclude
        BT_RESULT_SEGMENT / result_segment
        BT_EQUITY_INIT / equity_init
        BT_EQUITY_CAP_MULT / equity_cap_mult
        BT_START_TS / BT_END_TS / start_ts / end_ts
        config_tag / BT_TAG_PREFIX

    All other keys are passed straight through to ai_db.iter_backtest_rows(...).

    Returns
    -------
    Flat dict of metrics suitable for scripts/backtest.py:
        - tag, n_candidates, n_trades, n_wins, n_losses, n_flats, win_rate
        - avg_pnl_pct, median_pnl_pct, pnl_total_pct, pnl_vol_pct, pnl_std_error_pct
        - net_pnl_pct, equity_final, max_drawdown_pct, max_runup_pct, sharpe, sortino
        - equity_vol_pct, avg_equity_return_per_trade_pct
        - gross_win_pct, gross_loss_pct, profit_factor
        - avg_dd_trade_pct, p95_drawdown_trade_pct
        - avg_hold_sec, median_hold_sec, p95_hold_sec
        - risk_capacity_avg, risk_capacity_p05, risk_capacity_p95
        - weighted_avg_pnl_pct
        - avg_entry_score, median_entry_score
        - avg_heur_score, avg_ai_score
        - avg_win_pct, avg_loss_pct, expectancy_pct, pnl_per_trade_pct
        - days_span, daily_pnl_pct, trade_density_per_day
        - label_counts (dict[str,int])
        - label_pnl_sum (dict[str,float])
        - label_avg_pnl_pct (dict[str,float])
        - label_win_rate (dict[str,float])
        - label_loss_rate (dict[str,float])
        - self_learning_tune_count, self_learning_use_llm
        - reinvest_count, reinvest_pnl_pct
        - equity_halt_triggered, equity_halt_dd_pct
        - ai_risk_score_avg, ai_risk_score_med, ai_risk_reject_rate
        - pnl_dd_ratio, calmar_ratio, win_loss_ratio
        - kelly_fraction_est, kelly_fraction_capped
        - pnl_p05_pct, pnl_p95_pct, pnl_cvar_p05_pct
        - pnl_skew, pnl_kurtosis_excess
        - max_consec_wins, max_consec_losses
        - rolling_sharpe_50, rolling_sharpe_100
        - ulcer_index_pct, ulcer_perf_ratio
        - avg_exposure, p95_exposure
        - avg_risk_mult_effective
        - trade_cost_total_pct, trade_cost_avg_pct
        - rc_bucket_stats (performance per RC bucket)
        - score_min_initial / score_min_final
        - entry_min_score_initial / entry_min_score_final
        - risk_profile, risk_profile_mult, risk_mult_override_final
        - equity_init, equity_cap_mult
        - trade_cost_bps_cfg, jito_tip_bps_cfg, trade_cost_mode_cfg
        - trades (optional, if BT_LOG_TRADES is enabled)
        - equity_series (optional, if BT_LOG_EQUITY_SERIES is enabled)
    """
    tag = str(config.get("config_tag") or config.get("BT_TAG_PREFIX") or "")

    # Optional time-window filters
    start_dt = _parse_ts_val(
        config.get("BT_START_TS")
        or config.get("bt_start_ts")
        or config.get("start_ts")
    )
    end_dt = _parse_ts_val(
        config.get("BT_END_TS")
        or config.get("bt_end_ts")
        or config.get("end_ts")
    )

    # -------------------------------------------------------------------------
    # Gates / knobs
    # -------------------------------------------------------------------------

    # Raw heuristic gate
    score_min = _get_cfg_float(config, "ENTRY_SCORE_MIN", "entry_score_min", default=0.6)

    # Blended gate (0 = disabled)
    entry_min_score = _get_cfg_float(
        config, "ENTRY_MIN_SCORE", "entry_min_score", default=0.0
    )
    blended_gate = max(0.0, entry_min_score)

    # Snapshot initial gates (for diagnostics vs self-learning)
    score_min_initial = float(score_min)
    blended_gate_initial = float(blended_gate)

    max_positions = max(1, _get_cfg_int(config, "max_positions", default=1))

    risk_mult_override = _get_cfg_float(config, "risk_mult_override", default=1.0)
    risk_profile = str(
        config.get("RISK_PROFILE") or config.get("risk_profile") or ""
    ).strip() or None
    risk_profile_mult = _risk_profile_scale(risk_profile)

    regime_mode_lock_raw = config.get("regime_risk_mode_lock")
    try:
        regime_mode_lock: Optional[int] = (
            int(regime_mode_lock_raw) if regime_mode_lock_raw is not None else None
        )
    except Exception:
        regime_mode_lock = None

    # Risk-capacity gating
    require_rc = _get_cfg_bool(
        config,
        "ENTRY_REQUIRE_RISK_CAPACITY",
        "require_risk_capacity",
        default=False,
    )
    require_positive_rc = _get_cfg_bool(
        config,
        "BT_REQUIRE_POSITIVE_RC",
        "require_positive_rc",
        default=False,
    )
    min_rc = config.get("BT_MIN_RISK_CAPACITY", config.get("min_risk_capacity"))
    max_rc = config.get("BT_MAX_RISK_CAPACITY", config.get("max_risk_capacity"))
    min_rc_val = float(min_rc) if min_rc is not None else None
    max_rc_val = float(max_rc) if max_rc is not None else None

    # Trade cap
    max_trades_cfg = config.get("BT_MAX_TRADES", config.get("max_trades"))
    max_trades = int(max_trades_cfg) if max_trades_cfg else None

    # Extra candidate cap (safety / speed)
    limit_candidates_cfg = config.get("limit_candidates", config.get("BT_LIMIT_CANDIDATES"))
    try:
        limit_candidates: Optional[int] = (
            int(limit_candidates_cfg) if limit_candidates_cfg else None
        )
        if limit_candidates is not None and limit_candidates <= 0:
            limit_candidates = None
    except Exception:
        limit_candidates = None

    # Smart-wallet gates (values typically produced by utils_pkg.smart_wallets)
    min_sw_score = _get_cfg_float(
        config, "BT_MIN_SMART_WALLET_SCORE", "min_smart_wallet_score", default=-1.0
    )
    if min_sw_score < 0.0:
        min_sw_score = None
    require_sw_flag = _get_cfg_bool(
        config,
        "BT_REQUIRE_SMART_WALLET_FLAG",
        "require_smart_wallet_flag",
        default=False,
    )

    # Mint-level dedup
    dedup_mode_raw = (
        str(config.get("BT_DEDUP_MODE") or config.get("dedup_mode") or "")
        .strip()
        .lower()
    )
    if dedup_mode_raw not in ("mint", "mint_window"):
        dedup_mode = "off"
    else:
        dedup_mode = dedup_mode_raw

    dedup_window_cfg = config.get("BT_DEDUP_WINDOW_SEC", config.get("dedup_window_sec"))
    try:
        dedup_window_sec: Optional[float] = (
            float(dedup_window_cfg) if dedup_window_cfg else None
        )
        if dedup_window_sec is not None and dedup_window_sec <= 0:
            dedup_window_sec = None
    except Exception:
        dedup_window_sec = None

    # Label filters (extra safety on top of DB-side filters)
    label_include = config.get("label_include")
    label_exclude = config.get("label_exclude")
    if isinstance(label_include, (list, tuple)):
        label_include = [str(x) for x in label_include]
    else:
        label_include = None
    if isinstance(label_exclude, (list, tuple)):
        label_exclude = [str(x) for x in label_exclude]
    else:
        label_exclude = None

    # Optional result segment filter (train/test/etc)
    result_segment_cfg = (
        config.get("BT_RESULT_SEGMENT") or config.get("result_segment") or ""
    )
    result_segment = str(result_segment_cfg).strip().lower() or None
    if result_segment in ("full", "all"):
        result_segment = None

    # Sample weighting
    label_weights: Dict[str, float] = {}
    lw_raw = config.get("BT_LABEL_WEIGHTS_JSON") or config.get("label_weights_json")
    if lw_raw:
        try:
            label_weights = {
                str(k): float(v) for k, v in json.loads(str(lw_raw)).items()
            }
        except Exception:
            log.warning("backtest_engine: could not parse label_weights_json; ignoring.")

    sample_weight_mode = str(
        config.get("BT_SAMPLE_WEIGHT_MODE") or config.get("sample_weight_mode") or ""
    ).strip() or None

    # Logging controls
    log_trades = _get_cfg_bool(config, "BT_LOG_TRADES", "log_trades", default=False)
    log_features = _get_cfg_bool(config, "BT_LOG_FEATURES", "log_features", default=False)
    log_equity_series = _get_cfg_bool(
        config, "BT_LOG_EQUITY_SERIES", "log_equity_series", default=False
    )
    trades_for_log: List[Dict[str, Any]] = []
    equity_series: List[Dict[str, Any]] = []

    # Live risk simulation + equity halt
    use_live_rm = _get_cfg_bool(
        config, "BT_USE_LIVE_RISK_MANAGER", "use_live_risk_manager", default=False
    )
    alloc_mode_raw = (
        str(config.get("BT_ALLOC_MODE") or config.get("alloc_mode") or "")
        .strip()
        .lower()
    )
    if alloc_mode_raw not in ("simple", "risk_capacity"):
        alloc_mode = "simple"
    else:
        alloc_mode = alloc_mode_raw

    eq_halt_dd_cfg = config.get("BT_RISK_EQUITY_HALT_DD", config.get("risk_equity_halt_dd_pct"))
    try:
        equity_halt_dd_pct: Optional[float] = (
            float(eq_halt_dd_cfg) if eq_halt_dd_cfg is not None else None
        )
    except Exception:
        equity_halt_dd_pct = None

    # Self-learning state (backtest-only adaptive tuning)
    sl_state = _build_self_learning_state(config)

    # Reinvestor knobs (approx backtest simulation)
    enable_reinvestor = _get_cfg_bool(
        config, "BT_ENABLE_REINVESTOR", "enable_reinvestor", default=False
    )
    reinvest_percent = _get_cfg_float(
        config, "BT_REINVEST_PERCENT", "reinvest_percent_override", default=0.0
    )
    reinvest_min_profit_sol = _get_cfg_float(
        config, "BT_REINVEST_MIN_PROFIT_SOL", "reinvest_min_profit_sol", default=0.0
    )
    reinvest_max_single_usd = _get_cfg_float(
        config, "BT_REINVEST_MAX_SINGLE_USD", "reinvest_max_single_usd", default=0.0
    )
    reinvest_dd_soft_stop_pct = _get_cfg_float(
        config, "BT_REINVEST_DD_SOFT_STOP_PCT", "reinvest_dd_soft_stop_pct", default=0.0
    )
    reinvest_dd_hard_stop_pct = _get_cfg_float(
        config, "BT_REINVEST_DD_HARD_STOP_PCT", "reinvest_dd_hard_stop_pct", default=0.0
    )

    # AI risk overlay knobs
    use_ai_risk_overlay = _get_cfg_bool(
        config,
        "BT_USE_AI_RISK_MODEL",
        "use_ai_risk_overlay",
        "use_ai_risk_model",
        default=False,
    )
    ai_risk_max_score = _get_cfg_float(
        config, "BT_AI_RISK_MAX_SCORE", "ai_risk_max_score", default=1.01
    )
    ai_risk_min_conf = _get_cfg_float(
        config, "BT_AI_RISK_MIN_CONF", "ai_risk_min_confidence", default=0.0
    )
    ai_risk_mode_raw = (
        str(config.get("BT_AI_RISK_MODE") or config.get("ai_risk_mode") or "")
        .strip()
        .lower()
    )
    if ai_risk_mode_raw not in ("filter", "sizer"):
        ai_risk_mode = "filter"
    else:
        ai_risk_mode = ai_risk_mode_raw

    # Microstructure / cost modelling
    trade_cost_bps = _get_cfg_float(
        config, "BT_TRADE_COST_BPS", "trade_cost_bps", default=0.0
    )
    jito_tip_bps = _get_cfg_float(
        config, "BT_JITO_TIP_BPS", "jito_tip_bps", default=0.0
    )
    trade_cost_mode = (
        str(config.get("BT_TRADE_COST_MODE") or config.get("trade_cost_mode") or "")
        .strip()
        .lower()
    )
    if trade_cost_mode not in ("per_trade", "half_side"):
        trade_cost_mode = "per_trade"  # simple, conservative model

    # Initial equity + optional equity cap
    equity_init_cfg = config.get("BT_EQUITY_INIT") or config.get("equity_init")
    try:
        equity_init = float(equity_init_cfg) if equity_init_cfg is not None else 1.0
    except Exception:
        equity_init = 1.0
    if not math.isfinite(equity_init) or equity_init <= 0.0:
        equity_init = 1.0

    equity_cap_mult_cfg = config.get("BT_EQUITY_CAP_MULT") or config.get("equity_cap_mult")
    try:
        equity_cap_mult: Optional[float] = (
            float(equity_cap_mult_cfg) if equity_cap_mult_cfg is not None else None
        )
        if equity_cap_mult is not None and equity_cap_mult <= 0.0:
            equity_cap_mult = None
    except Exception:
        equity_cap_mult = None

    # -------------------------------------------------------------------------
    # Iterate rows & simulate portfolio
    # -------------------------------------------------------------------------

    n_candidates = 0
    n_trades = 0
    n_wins = 0
    n_losses = 0

    pnl_list: List[float] = []  # chronological PnL%
    dd_list: List[float] = []
    hold_list: List[float] = []

    rc_exec_list: List[float] = []
    label_counts: Dict[str, int] = {}
    label_pnl_sum: Dict[str, float] = {}
    label_win_counts: Dict[str, int] = {}
    label_loss_counts: Dict[str, int] = {}

    # Equity path
    equity = float(equity_init)
    hwm = float(equity_init)
    max_dd = 0.0
    max_runup = 0.0
    equity_returns: List[float] = []  # r_alloc per trade
    equity_halt_triggered = False
    eq_drawdowns_pct: List[float] = []  # equity drawdown per trade in %

    # Score diagnostics
    blended_scores: List[float] = []
    heur_scores: List[float] = []
    ai_scores: List[float] = []

    # AI risk diagnostics
    ai_risk_scores_used: List[float] = []
    ai_risk_rejects = 0

    # Weighted research
    weighted_return_sum = 0.0
    weight_sum = 0.0

    # Profit-factor aggregates
    gross_win_pct = 0.0  # sum positive pnl_pct
    gross_loss_pct = 0.0  # sum negative pnl_pct (≤0)

    # Dedup state
    last_seen_by_mint: Dict[str, float] = {}  # mint -> last created_at epoch seconds

    # For days_span metric
    first_ts: Optional[datetime] = None
    last_ts: Optional[datetime] = None

    # Reinvestor diagnostics
    reinvest_count = 0
    reinvest_pnl_pct = 0.0

    # Exposure & risk diagnostics
    exposure_list: List[float] = []
    risk_mult_effective_sum = 0.0
    cost_sum_pct = 0.0

    # Risk capacity buckets
    rc_bucket_defs: List[Tuple[float, float, str]] = [
        (0.0, 0.25, "0.00-0.25"),
        (0.25, 0.50, "0.25-0.50"),
        (0.50, 0.75, "0.50-0.75"),
        (0.75, 1.01, "0.75-1.00"),
    ]
    rc_bucket_acc: Dict[str, Dict[str, Any]] = {}

    async for row in _iter_rows_from_ai_db(config):
        # Hard candidate cap (safety)
        if limit_candidates is not None and n_candidates >= limit_candidates:
            break

        n_candidates += 1

        # Time-window filter (if configured). Rows without created_at are skipped
        # when a window is set, to keep metrics consistent.
        if start_dt or end_dt:
            if row.created_at is None:
                continue
            if start_dt and row.created_at < start_dt:
                continue
            if end_dt and row.created_at > end_dt:
                continue

        # Segment filter (if DB has a "segment" column)
        if result_segment and row.raw is not None:
            seg = (
                str(
                    row.raw.get("segment")
                    or row.raw.get("split_segment")
                    or row.features.get("segment")
                    or ""
                )
                .strip()
                .lower()
            )
            if seg and seg != result_segment:
                continue

        # Label filters (if we have labels)
        if row.label is not None:
            if label_include and row.label not in label_include:
                continue
            if label_exclude and row.label in label_exclude:
                continue

        # Mint-based dedup (optional)
        if dedup_mode != "off":
            mint = _extract_mint(row)
            if mint:
                if dedup_mode == "mint":
                    if mint in last_seen_by_mint:
                        continue
                    last_seen_by_mint[mint] = (
                        row.created_at.timestamp() if row.created_at else 0.0
                    )
                elif dedup_mode == "mint_window":
                    ts = row.created_at.timestamp() if row.created_at else 0.0
                    last_ts_mint = last_seen_by_mint.get(mint)
                    if last_ts_mint is not None and dedup_window_sec is not None:
                        if (ts - last_ts_mint) <= dedup_window_sec:
                            continue
                    last_seen_by_mint[mint] = ts

        feats = row.features

        # Risk-capacity gating
        rc = feats.get("risk_capacity_score")
        if rc is not None:
            try:
                rc = float(rc)
            except Exception:
                rc = None

        if require_rc and rc is None:
            continue
        if rc is not None and min_rc_val is not None and rc < min_rc_val:
            continue
        if rc is not None and max_rc_val is not None and rc > max_rc_val:
            continue
        if require_positive_rc and rc is not None and rc <= 0.0:
            continue

        # Smart-wallet gating
        sw_score = None
        if "smart_wallet_score" in feats and feats["smart_wallet_score"] is not None:
            try:
                sw_score = float(feats["smart_wallet_score"])
            except Exception:
                sw_score = None

        sw_flag = feats.get("smart_wallet_flag")
        sw_flag_bool = bool(sw_flag) if sw_flag is not None else False

        if require_sw_flag and not sw_flag_bool:
            continue
        if min_sw_score is not None:
            if sw_score is None or sw_score < min_sw_score:
                continue

        # AI risk overlay gating (pre-entry)
        ai_risk_score = None
        ai_risk_conf = None
        ai_risk_sizer_mult = 1.0

        if use_ai_risk_overlay:
            ai_risk_score, ai_risk_conf = await _maybe_ai_risk_score(row, feats, config)
            if ai_risk_score is not None:
                # Normalize: if someone stores [0,100], scale to [0,1]
                if ai_risk_score > 1.0 and ai_risk_score <= 100.0:
                    ai_risk_score = ai_risk_score / 100.0

                conf_ok = (
                    ai_risk_conf is None
                    or ai_risk_conf >= ai_risk_min_conf
                    or ai_risk_min_conf <= 0.0
                )

                if conf_ok:
                    if ai_risk_mode == "filter":
                        if ai_risk_score > ai_risk_max_score:
                            ai_risk_rejects += 1
                            # Hard reject: we do not even compute entry scores
                            continue
                        else:
                            ai_risk_scores_used.append(ai_risk_score)
                    else:  # "sizer" mode: use score to scale risk instead of pure filter
                        ai_risk_scores_used.append(ai_risk_score)
                        # Simple sizer: higher risk_score → smaller allocation
                        #  score≈0 → ~1x, score≈1 → ~0x
                        ai_risk_sizer_mult = max(0.0, min(1.0, 1.0 - ai_risk_score))

        # Entry scoring (same stack as live, via unified scoring router)
        heur_score, ai_score, blended = await _compute_scores(feats, config)

        # Score-based gates:
        #   - raw heuristic must clear ENTRY_SCORE_MIN
        #   - blended must clear ENTRY_MIN_SCORE (if >0)
        if heur_score < score_min:
            continue
        if blended_gate > 0.0 and blended < blended_gate:
            continue

        # Passed all gates → we "take" this trade
        n_trades += 1

        # Use gross PnL then subtract microstructure costs
        gross_pnl_pct = float(row.pnl_pct)
        pnl_pct = gross_pnl_pct

        total_cost_bps = max(0.0, float(trade_cost_bps) + float(jito_tip_bps))
        cost_pct = 0.0
        if total_cost_bps > 0.0:
            # bps → percentage points; full round-trip cost
            cost_pct = total_cost_bps / 100.0
            # Cost always drags returns regardless of direction
            if trade_cost_mode == "half_side":
                # model roughly half the configured round-trip as effective drag
                effective_cost = cost_pct * 0.5
            else:  # "per_trade" (default)
                effective_cost = cost_pct
            pnl_pct -= effective_cost
            cost_sum_pct += effective_cost

        pnl_list.append(pnl_pct)

        if row.max_dd_pct is not None:
            dd_list.append(float(row.max_dd_pct))
        if row.hold_sec is not None:
            hold_list.append(float(row.hold_sec))

        if row.created_at is not None:
            if first_ts is None:
                first_ts = row.created_at
            last_ts = row.created_at

        if pnl_pct > 0:
            n_wins += 1
            gross_win_pct += pnl_pct
        elif pnl_pct < 0:
            n_losses += 1
            gross_loss_pct += pnl_pct  # negative

        if rc is not None:
            rc_val = float(rc)
            rc_exec_list.append(rc_val)
            # RC bucket stats
            for lo, hi, name in rc_bucket_defs:
                if (rc_val >= lo and rc_val < hi) or (name.endswith("1.00") and rc_val <= hi):
                    bucket = rc_bucket_acc.setdefault(
                        name,
                        {"count": 0, "pnl_sum": 0.0, "win_count": 0, "rc_sum": 0.0},
                    )
                    bucket["count"] += 1
                    bucket["pnl_sum"] += pnl_pct
                    bucket["rc_sum"] += rc_val
                    if pnl_pct > 0:
                        bucket["win_count"] += 1
                    break

        if row.label:
            label_counts[row.label] = label_counts.get(row.label, 0) + 1
            label_pnl_sum[row.label] = label_pnl_sum.get(row.label, 0.0) + pnl_pct
            if pnl_pct > 0:
                label_win_counts[row.label] = label_win_counts.get(row.label, 0) + 1
            elif pnl_pct < 0:
                label_loss_counts[row.label] = label_loss_counts.get(row.label, 0) + 1

        # Portfolio equity simulation
        r_raw = pnl_pct / 100.0
        regime_mult = _regime_mult_from_features(feats, regime_mode_lock)
        risk_mult_effective = (
            float(risk_mult_override)
            * float(risk_profile_mult)
            * float(regime_mult)
            * float(ai_risk_sizer_mult)
        )
        r_eff = r_raw * risk_mult_effective

        # Allocation factor: simple vs risk_capacity vs (optional) live risk_manager
        if use_live_rm and _rm is not None and hasattr(_rm, "backtest_allocation_factor"):
            try:
                alloc_factor = float(
                    _rm.backtest_allocation_factor(  # type: ignore[attr-defined]
                        equity=equity,
                        risk_capacity_score=float(rc) if rc is not None else None,
                        config=config,
                    )
                )
                alloc_factor = max(0.0, min(1.0, alloc_factor))
            except Exception:  # pragma: no cover
                alloc_factor = 1.0
        else:
            if alloc_mode == "risk_capacity" and rc is not None:
                alloc_factor = max(0.0, min(1.0, float(rc)))
            else:
                alloc_factor = 1.0

        # Spread allocation across max_positions (capacity-style)
        r_alloc = r_eff * alloc_factor / float(max_positions)
        equity_returns.append(r_alloc)

        # Exposure diagnostics: notional fraction-of-equity in this trade
        effective_exposure = alloc_factor / float(max_positions)
        exposure_list.append(effective_exposure)
        risk_mult_effective_sum += risk_mult_effective

        equity *= (1.0 + r_alloc)

        # Apply optional equity cap
        if equity_cap_mult is not None:
            cap_val = equity_init * equity_cap_mult
            if equity > cap_val:
                equity = cap_val

        if equity > hwm:
            hwm = equity
        # Max run-up relative to initial equity
        max_runup = max(max_runup, (hwm - equity_init) / equity_init)

        drawdown = 0.0 if hwm <= 0 else (hwm - equity) / hwm
        if drawdown > max_dd:
            max_dd = drawdown

        eq_drawdowns_pct.append(drawdown * 100.0)

        # Optional equity series logging (for plotting / research)
        if log_equity_series:
            equity_series.append(
                {
                    "trade_index": n_trades,
                    "equity": equity,
                    "drawdown": drawdown,
                    "created_at": row.created_at.isoformat() if row.created_at else None,
                }
            )

        # Equity-based kill switch
        if equity_halt_dd_pct is not None and (drawdown * 100.0) >= equity_halt_dd_pct:
            equity_halt_triggered = True
            log.info(
                "backtest_engine: equity halt triggered at DD=%.2f%% (threshold=%.2f%%)",
                drawdown * 100.0,
                equity_halt_dd_pct,
            )

        # Optional hook into risk_manager for trade-level diagnostics
        if use_live_rm and _rm is not None and hasattr(_rm, "backtest_on_trade"):
            try:
                _rm.backtest_on_trade(  # type: ignore[attr-defined]
                    pnl_pct=pnl_pct,
                    equity=equity,
                    drawdown_pct=drawdown * 100.0,
                    risk_capacity_score=float(rc) if rc is not None else None,
                    config=config,
                    created_at=row.created_at,
                )
            except Exception:  # pragma: no cover
                log.debug("risk_manager.backtest_on_trade failed", exc_info=True)

        # Reinvestor approx simulation
        if enable_reinvestor and pnl_pct > 0:
            qualifies = True
            if reinvest_dd_hard_stop_pct > 0 and (drawdown * 100.0) >= reinvest_dd_hard_stop_pct:
                qualifies = False
            if reinvest_dd_soft_stop_pct > 0 and (drawdown * 100.0) >= reinvest_dd_soft_stop_pct:
                qualifies = False

            if qualifies:
                reinvest_count += 1
                # Optional hook into real reinvestor module for richer behavior
                if _reinv is not None and hasattr(_reinv, "backtest_on_win"):
                    try:
                        extra_pnl = _reinv.backtest_on_win(  # type: ignore[attr-defined]
                            pnl_pct=pnl_pct,
                            equity=equity,
                            config=config,
                        )
                        reinvest_pnl_pct += float(extra_pnl)
                    except Exception:  # pragma: no cover
                        if reinvest_percent > 0:
                            reinvest_pnl_pct += pnl_pct * (reinvest_percent / 100.0)
                        else:
                            reinvest_pnl_pct += pnl_pct
                else:
                    if reinvest_percent > 0:
                        reinvest_pnl_pct += pnl_pct * (reinvest_percent / 100.0)
                    else:
                        reinvest_pnl_pct += pnl_pct

        # Weighted research metric
        if sample_weight_mode and row.label is not None:
            if sample_weight_mode == "by_label":
                w = label_weights.get(row.label, 1.0)
            elif sample_weight_mode == "by_outcome_severity":
                # heavier weight on larger losers; winners ~1
                w = 1.0 + max(0.0, -pnl_pct) / 100.0
            elif sample_weight_mode == "uniform":
                w = 1.0
            else:
                w = 1.0
            weighted_return_sum += w * pnl_pct
            weight_sum += w

        blended_scores.append(blended)
        heur_scores.append(heur_score)
        if ai_score is not None:
            ai_scores.append(ai_score)

        # Optional per-trade logging
        if log_trades:
            trade_rec = BacktestTrade(
                pnl_pct=pnl_pct,
                pnl_pct_gross=gross_pnl_pct,
                heur_score=heur_score,
                blended_score=blended,
                ai_score=ai_score,
                label=row.label,
                risk_capacity_score=float(rc) if rc is not None else None,
                smart_wallet_score=sw_score,
                smart_wallet_flag=sw_flag_bool,
                created_at=row.created_at,
                regime_mult=regime_mult,
                equity=equity,
                drawdown=drawdown,
                trade_cost_bps=trade_cost_bps,
                jito_tip_bps=jito_tip_bps,
                ai_risk_score=ai_risk_score if use_ai_risk_overlay else None,
                ai_risk_conf=ai_risk_conf if use_ai_risk_overlay else None,
                ai_risk_mode=ai_risk_mode if use_ai_risk_overlay else None,
                ai_risk_sizer_mult=ai_risk_sizer_mult if use_ai_risk_overlay else 1.0,
                features=feats if log_features else None,
            )
            # Store as dict for more JSON-friendly output
            trade_dict = {
                **{k: getattr(trade_rec, k) for k in trade_rec.__dataclass_fields__},  # type: ignore[attr-defined]
                "created_at": trade_rec.created_at.isoformat()
                if trade_rec.created_at
                else None,
            }
            trades_for_log.append(trade_dict)

            # Optional per-trade hooks into data_logger / self_learning_ai_strategy
            if _data_logger is not None:
                try:
                    if hasattr(_data_logger, "record_backtest_trade"):
                        _data_logger.record_backtest_trade(  # type: ignore[attr-defined]
                            config=dict(config),
                            trade=trade_dict,
                        )
                except Exception:  # pragma: no cover
                    log.debug("data_logger.record_backtest_trade failed", exc_info=True)

            if _sl_mod is not None:
                try:
                    if hasattr(_sl_mod, "record_backtest_trade"):
                        _sl_mod.record_backtest_trade(  # type: ignore[attr-defined]
                            config=dict(config),
                            trade=trade_dict,
                        )
                except Exception:  # pragma: no cover
                    log.debug("self_learning_ai_strategy.record_backtest_trade failed", exc_info=True)

        # Self-learning simulation (backtest-only adaptive tuning)
        (
            score_min,
            blended_gate,
            risk_mult_override,
        ) = sl_state.maybe_tune(
            created_at=row.created_at,
            pnl_pct=pnl_pct,
            equity=equity,
            score_min=score_min,
            blended_gate=blended_gate,
            risk_mult_override=risk_mult_override,
        )

        if equity_halt_triggered:
            break

        if max_trades is not None and n_trades >= max_trades:
            break

    # -------------------------------------------------------------------------
    # Aggregate stats
    # -------------------------------------------------------------------------

    stats: Dict[str, Any] = {}
    stats["tag"] = tag
    stats["n_candidates"] = n_candidates
    stats["n_trades"] = n_trades
    stats["n_wins"] = n_wins
    stats["n_losses"] = n_losses
    stats["n_flats"] = max(0, n_trades - n_wins - n_losses)
    stats["win_rate"] = n_wins / n_trades if n_trades > 0 else 0.0

    # Basic PnL aggregates
    if pnl_list:
        stats["avg_pnl_pct"] = statistics.mean(pnl_list)
        stats["median_pnl_pct"] = statistics.median(pnl_list)
        stats["pnl_total_pct"] = sum(pnl_list)
        if len(pnl_list) > 1:
            stats["pnl_vol_pct"] = statistics.pstdev(pnl_list)
            # standard error of mean (quant research helper)
            stats["pnl_std_error_pct"] = stats["pnl_vol_pct"] / math.sqrt(len(pnl_list))
        else:
            stats["pnl_vol_pct"] = 0.0
            stats["pnl_std_error_pct"] = 0.0
    else:
        stats["avg_pnl_pct"] = 0.0
        stats["median_pnl_pct"] = 0.0
        stats["pnl_total_pct"] = 0.0
        stats["pnl_vol_pct"] = 0.0
        stats["pnl_std_error_pct"] = 0.0

    # Equity / drawdown
    stats["net_pnl_pct"] = (equity - equity_init) / equity_init * 100.0
    stats["equity_final"] = equity
    stats["max_drawdown_pct"] = max_dd * 100.0
    stats["max_runup_pct"] = max_runup * 100.0
    stats["sharpe"] = _compute_sharpe(equity_returns)
    if equity_returns:
        stats["avg_equity_return_per_trade_pct"] = statistics.mean(equity_returns) * 100.0
        if len(equity_returns) > 1:
            stats["equity_vol_pct"] = statistics.pstdev(equity_returns) * 100.0
        else:
            stats["equity_vol_pct"] = 0.0
    else:
        stats["avg_equity_return_per_trade_pct"] = 0.0
        stats["equity_vol_pct"] = 0.0

    # Ulcer index (equity-path based risk metric)
    if eq_drawdowns_pct:
        mean_sq = statistics.mean([d * d for d in eq_drawdowns_pct])
        ulcer_index = math.sqrt(mean_sq)
    else:
        ulcer_index = 0.0
    stats["ulcer_index_pct"] = ulcer_index
    net_pnl_pct = float(stats["net_pnl_pct"])
    if ulcer_index > 0:
        stats["ulcer_perf_ratio"] = net_pnl_pct / ulcer_index
    else:
        stats["ulcer_perf_ratio"] = 0.0

    # Profit-factor helpers
    stats["gross_win_pct"] = gross_win_pct
    stats["gross_loss_pct"] = gross_loss_pct
    if gross_loss_pct < 0:
        stats["profit_factor"] = gross_win_pct / abs(gross_loss_pct)
    else:
        stats["profit_factor"] = 0.0

    # Per-trade drawdown (per-trade max_dd_pct if present in DB)
    if dd_list:
        stats["avg_dd_trade_pct"] = statistics.mean(dd_list)
        stats["p95_drawdown_trade_pct"] = _safe_quantile(dd_list, 0.95)
    else:
        stats["avg_dd_trade_pct"] = 0.0
        stats["p95_drawdown_trade_pct"] = 0.0

    # Hold-time stats
    if hold_list:
        stats["avg_hold_sec"] = statistics.mean(hold_list)
        stats["median_hold_sec"] = statistics.median(hold_list)
        stats["p95_hold_sec"] = _safe_quantile(hold_list, 0.95)
    else:
        stats["avg_hold_sec"] = 0.0
        stats["median_hold_sec"] = 0.0
        stats["p95_hold_sec"] = 0.0

    # Risk-capacity distribution
    if rc_exec_list:
        rc_sorted = sorted(rc_exec_list)
        stats["risk_capacity_avg"] = statistics.mean(rc_sorted)
        stats["risk_capacity_p05"] = _safe_quantile(rc_sorted, 0.05)
        stats["risk_capacity_p95"] = _safe_quantile(rc_sorted, 0.95)
    else:
        stats["risk_capacity_avg"] = 0.0
        stats["risk_capacity_p05"] = 0.0
        stats["risk_capacity_p95"] = 0.0

    # Risk capacity bucket performance
    rc_bucket_stats: Dict[str, Any] = {}
    for name, agg in rc_bucket_acc.items():
        cnt = agg["count"]
        if cnt <= 0:
            continue
        rc_bucket_stats[name] = {
            "n_trades": cnt,
            "avg_pnl_pct": agg["pnl_sum"] / float(cnt),
            "win_rate": agg["win_count"] / float(cnt),
            "avg_rc": agg["rc_sum"] / float(cnt),
        }
    stats["rc_bucket_stats"] = rc_bucket_stats

    # Exposure metrics
    if exposure_list:
        stats["avg_exposure"] = statistics.mean(exposure_list)
        stats["p95_exposure"] = _safe_quantile(exposure_list, 0.95)
    else:
        stats["avg_exposure"] = 0.0
        stats["p95_exposure"] = 0.0

    # Effective risk multiplier
    if n_trades > 0:
        stats["avg_risk_mult_effective"] = risk_mult_effective_sum / float(n_trades)
    else:
        stats["avg_risk_mult_effective"] = 1.0

    # Trade cost analytics
    stats["trade_cost_total_pct"] = cost_sum_pct
    stats["trade_cost_avg_pct"] = cost_sum_pct / float(n_trades) if n_trades > 0 else 0.0
    stats["trade_cost_bps_cfg"] = float(trade_cost_bps)
    stats["jito_tip_bps_cfg"] = float(jito_tip_bps)
    stats["trade_cost_mode_cfg"] = trade_cost_mode

    # Weighted research metric
    if weight_sum > 0:
        stats["weighted_avg_pnl_pct"] = weighted_return_sum / weight_sum
    else:
        stats["weighted_avg_pnl_pct"] = stats["avg_pnl_pct"]

    # Score diagnostics
    if blended_scores:
        stats["avg_entry_score"] = statistics.mean(blended_scores)
        stats["median_entry_score"] = statistics.median(blended_scores)
    else:
        stats["avg_entry_score"] = 0.0
        stats["median_entry_score"] = 0.0

    if heur_scores:
        stats["avg_heur_score"] = statistics.mean(heur_scores)
    else:
        stats["avg_heur_score"] = 0.0

    if ai_scores:
        stats["avg_ai_score"] = statistics.mean(ai_scores)
    else:
        stats["avg_ai_score"] = 0.0

    # Win / loss breakdown + expectancy
    pos_pnls = [p for p in pnl_list if p > 0]
    neg_pnls = [p for p in pnl_list if p < 0]

    if pos_pnls:
        stats["avg_win_pct"] = statistics.mean(pos_pnls)
    else:
        stats["avg_win_pct"] = 0.0

    if neg_pnls:
        stats["avg_loss_pct"] = statistics.mean(neg_pnls)
    else:
        stats["avg_loss_pct"] = 0.0

    if n_trades > 0:
        stats["pnl_per_trade_pct"] = stats["pnl_total_pct"] / float(n_trades)
    else:
        stats["pnl_per_trade_pct"] = 0.0

    if stats["avg_win_pct"] != 0.0 or stats["avg_loss_pct"] != 0.0:
        wr = stats["win_rate"]
        stats["expectancy_pct"] = (
            wr * stats["avg_win_pct"] + (1.0 - wr) * stats["avg_loss_pct"]
        )
    else:
        stats["expectancy_pct"] = 0.0

    # Span in days + daily PnL + trade density
    if first_ts is not None and last_ts is not None and last_ts > first_ts:
        span_days = (last_ts - first_ts).total_seconds() / 86400.0
        stats["days_span"] = max(span_days, 1e-6)
    else:
        stats["days_span"] = 0.0

    if stats["days_span"] > 0:
        stats["daily_pnl_pct"] = stats["pnl_total_pct"] / stats["days_span"]
        stats["trade_density_per_day"] = n_trades / stats["days_span"]
    else:
        stats["daily_pnl_pct"] = 0.0
        stats["trade_density_per_day"] = 0.0

    # Label distribution + per-label pnl sum + per-label avg + win/loss-rate
    stats["label_counts"] = dict(label_counts)
    stats["label_pnl_sum"] = dict(label_pnl_sum)
    label_avg_pnl: Dict[str, float] = {}
    label_win_rate: Dict[str, float] = {}
    label_loss_rate: Dict[str, float] = {}
    for lbl, cnt in label_counts.items():
        if cnt > 0:
            label_avg_pnl[lbl] = label_pnl_sum.get(lbl, 0.0) / float(cnt)
            wins_lbl = label_win_counts.get(lbl, 0)
            losses_lbl = label_loss_counts.get(lbl, 0)
            label_win_rate[lbl] = wins_lbl / float(cnt)
            label_loss_rate[lbl] = losses_lbl / float(cnt)
    stats["label_avg_pnl_pct"] = label_avg_pnl
    stats["label_win_rate"] = label_win_rate
    stats["label_loss_rate"] = label_loss_rate

    # Equity halt diagnostics
    stats["equity_halt_triggered"] = bool(equity_halt_triggered)
    stats["equity_halt_dd_pct"] = float(equity_halt_dd_pct or 0.0)

    # Self-learning diagnostics
    stats["self_learning_tune_count"] = int(sl_state.tune_count)
    stats["self_learning_use_llm"] = bool(sl_state.use_llm)

    # Sortino-style metric (using per-trade pnl_list as % returns)
    downside_dev = _compute_downside_dev(pnl_list)
    stats["downside_dev_pct"] = downside_dev
    if downside_dev > 0:
        stats["sortino"] = stats["avg_pnl_pct"] / downside_dev
    else:
        stats["sortino"] = 0.0

    # Reinvestor diagnostics (approx)
    stats["reinvest_count"] = reinvest_count
    stats["reinvest_pnl_pct"] = reinvest_pnl_pct
    stats["reinvest_enabled"] = bool(enable_reinvestor)
    stats["reinvest_percent"] = reinvest_percent
    stats["reinvest_min_profit_sol"] = reinvest_min_profit_sol
    stats["reinvest_max_single_usd"] = reinvest_max_single_usd
    stats["reinvest_dd_soft_stop_pct"] = reinvest_dd_soft_stop_pct
    stats["reinvest_dd_hard_stop_pct"] = reinvest_dd_hard_stop_pct

    # AI risk overlay diagnostics
    stats["ai_risk_overlay_enabled"] = bool(use_ai_risk_overlay)
    stats["ai_risk_max_score"] = ai_risk_max_score
    stats["ai_risk_min_conf"] = ai_risk_min_conf
    stats["ai_risk_mode"] = ai_risk_mode
    stats["ai_risk_reject_count"] = ai_risk_rejects
    stats["ai_risk_reject_rate"] = (
        ai_risk_rejects / float(n_candidates) if n_candidates > 0 else 0.0
    )
    if ai_risk_scores_used:
        stats["ai_risk_score_avg"] = statistics.mean(ai_risk_scores_used)
        stats["ai_risk_score_med"] = statistics.median(ai_risk_scores_used)
    else:
        stats["ai_risk_score_avg"] = 0.0
        stats["ai_risk_score_med"] = 0.0

    # Extra quant-grade helpers
    dd_abs = abs(float(stats.get("max_drawdown_pct", 0.0) or 0.0))
    if dd_abs > 0:
        stats["pnl_dd_ratio"] = net_pnl_pct / dd_abs
        stats["calmar_ratio"] = net_pnl_pct / dd_abs
    else:
        stats["pnl_dd_ratio"] = 0.0
        stats["calmar_ratio"] = 0.0

    if stats["avg_loss_pct"] != 0.0:
        stats["win_loss_ratio"] = stats["avg_win_pct"] / abs(stats["avg_loss_pct"])
    else:
        stats["win_loss_ratio"] = 0.0

    # Tail metrics on PnL distribution
    if pnl_list:
        sorted_pnl = sorted(pnl_list)
        stats["pnl_p05_pct"] = _safe_quantile(sorted_pnl, 0.05)
        stats["pnl_p95_pct"] = _safe_quantile(sorted_pnl, 0.95)
        # CVaR / expected shortfall at p05 (mean of worst 5% trades)
        tail_n = max(1, int(len(sorted_pnl) * 0.05))
        stats["pnl_cvar_p05_pct"] = statistics.mean(sorted_pnl[:tail_n])
        skew, kurt_excess = _compute_skew_kurtosis(sorted_pnl)
        stats["pnl_skew"] = skew
        stats["pnl_kurtosis_excess"] = kurt_excess
        # NOTE: consecutive runs must use chronological pnl_list, NOT sorted
        max_consec_wins, max_consec_losses = _compute_consecutive_runs(pnl_list)
        stats["max_consec_wins"] = max_consec_wins
        stats["max_consec_losses"] = max_consec_losses
    else:
        stats["pnl_p05_pct"] = 0.0
        stats["pnl_p95_pct"] = 0.0
        stats["pnl_cvar_p05_pct"] = 0.0
        stats["pnl_skew"] = 0.0
        stats["pnl_kurtosis_excess"] = 0.0
        stats["max_consec_wins"] = 0
        stats["max_consec_losses"] = 0

    # Kelly fraction estimate (based on avg win/loss and winrate)
    wr = stats["win_rate"]
    avg_win = stats["avg_win_pct"]
    avg_loss = stats["avg_loss_pct"]
    if wr > 0 and wr < 1 and avg_win > 0 and avg_loss < 0:
        b = avg_win / abs(avg_loss)  # payoff ratio
        q = 1.0 - wr
        kelly = (b * wr - q) / b
        kelly = max(0.0, kelly)  # no negative Kelly
        stats["kelly_fraction_est"] = kelly
        stats["kelly_fraction_capped"] = min(kelly, 0.5)
    else:
        stats["kelly_fraction_est"] = 0.0
        stats["kelly_fraction_capped"] = 0.0

    # Rolling Sharpe diagnostics (short-horizon quality checks)
    if len(equity_returns) >= 2:
        stats["rolling_sharpe_50"] = _compute_sharpe(equity_returns[-50:])
        stats["rolling_sharpe_100"] = _compute_sharpe(equity_returns[-100:])
    else:
        stats["rolling_sharpe_50"] = 0.0
        stats["rolling_sharpe_100"] = 0.0

    # Final gate diagnostics after self-learning
    stats["score_min_initial"] = score_min_initial
    stats["score_min_final"] = score_min
    stats["entry_min_score_initial"] = blended_gate_initial
    stats["entry_min_score_final"] = blended_gate

    # Risk / equity config echo
    stats["risk_profile"] = risk_profile
    stats["risk_profile_mult"] = risk_profile_mult
    stats["risk_mult_override_final"] = risk_mult_override
    stats["equity_init"] = equity_init
    stats["equity_cap_mult"] = equity_cap_mult
    stats["alloc_mode"] = alloc_mode
    stats["use_live_risk_manager"] = bool(use_live_rm)

    # Optional per-trade log
    if log_trades:
        stats["trades"] = trades_for_log

    # Optional equity series
    if log_equity_series:
        stats["equity_series"] = equity_series

    # Optional research logging hook into utils_pkg.data_logger
    if _data_logger is not None:
        try:
            if hasattr(_data_logger, "record_backtest_run"):
                _data_logger.record_backtest_run(  # type: ignore[attr-defined]
                    config=dict(config),
                    stats=stats,
                )
            elif hasattr(_data_logger, "log_backtest_run"):
                _data_logger.log_backtest_run(  # type: ignore[attr-defined]
                    config=dict(config),
                    stats=stats,
                )
        except Exception:  # pragma: no cover
            log.debug("data_logger backtest hook failed", exc_info=True)

    # Optional: let self_learning_ai_strategy see full result for meta-learning
    if _sl_mod is not None:
        try:
            if hasattr(_sl_mod, "record_backtest_summary"):
                _sl_mod.record_backtest_summary(  # type: ignore[attr-defined]
                    config=dict(config),
                    stats=stats,
                )
        except Exception:  # pragma: no cover
            log.debug("self_learning_ai_strategy.record_backtest_summary failed", exc_info=True)

    return stats
