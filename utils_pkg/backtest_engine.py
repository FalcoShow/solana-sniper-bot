from __future__ import annotations

"""
God-Tier Quant Backtest Engine (V3+++ / Phase 0–7, Final Overhaul)
==================================================================

Single public entrypoint:

    async run_backtest(config: Mapping[str, Any]) -> dict

This engine is designed to mirror *live* behavior as closely as possible while
remaining robust to API drift and optional modules.

What this engine guarantees
---------------------------
• Uses the *same entry scoring stack as live* (best-effort, resilient):

    - Default: utils_pkg.ai_entry_logic.score_entry(...)
    - Optional: utils_pkg.ai_buy_scoring.score_only(...) (V3+++ buy engine)

• Streaming-safe data ingestion from ai_db (sync or async):

    - ai_db.iter_backtest_rows(config)  -> iterator/aiter of row mappings
    - fallback: ai_db.iter_training_rows(config)

• Config-driven wiring:

    - Entry gates (heuristic + blended) + self-learning adaptive tuning
    - Risk profile + regime multiplier + (optional) AI-risk overlay
      (filter or sizer mode)
    - Risk-capacity gating + smart-wallet gating
    - Dedup by mint (off/mint/mint_window)
    - Cost model (bps): trade_cost_bps + jito_tip_bps
      + optional slippage/spread
    - Optional overlap/capacity simulation when timestamps + hold_sec exist
    - Optional equity kill-switch on drawdown
    - Optional integration hooks:
        • risk_manager (live-style allocation & equity halt behavior +
          optional backtest_on_summary)
        • reinvestor (compounding simulation + optional backtest_on_summary)
        • data_logger (DB logging)
        • self_learning_ai_strategy (backtest-driven strategy tuning)

• Quant-grade metrics:

    - Equity curve stats, max DD/run-up, time-in-DD, ulcer index,
      ulcer performance ratio
    - Sharpe/Sortino-like ratios (per-trade basis), skew/kurtosis
    - Profit factor, gain-to-pain, omega, CVaR(5%)
    - Kelly estimate (simple)
    - Buckets:
        • risk_capacity
        • score
        • regime
        • hour-of-day
    - Label-level stats (counts, avg pnl, win rate, loss rate)
    - Segment filters and echo

Assumptions about ai_db rows
----------------------------
Each row is expected to be a mapping with at least:

    - "features"        : dict        (FeatureBuilder output)
      OR "features_json": str (JSON)
    - "pnl_pct"         : float       (realized PnL as % of notional, e.g. +12.5)

Optionally:

    - "label"           : str
    - "max_dd_pct"      : float
    - "hold_sec"        : float
    - "created_at"      : datetime | ISO str | epoch seconds
    - "mint"            : str
    - "ai_risk_score"   : float       (0..1 or 0..100)
    - "ai_risk_conf"    : float       (0..1)
    - "segment"         : str         (e.g. "train","test",...)

If your column names differ, ONLY edit:
    - _normalize_row()
    - _extract_mint()
"""

import asyncio
import heapq
import json
import math
import statistics
import threading
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, AsyncIterator, Dict, Iterable, List, Mapping, Optional, Tuple

from logging_setup import get_logger
from utils_pkg import ai_db
from utils_pkg import ai_entry_logic as _entry

# -----------------------------------------------------------------------------
# Logger (robust to get_logger signature differences)
# -----------------------------------------------------------------------------
try:
    log = get_logger(__name__)
except TypeError:  # pragma: no cover
    log = get_logger()

# logging safety shim (so .every always exists)
if not hasattr(log, "every"):

    def _every(_key: str, n: int = 60) -> bool:  # pragma: no cover
        return True

    setattr(log, "every", _every)

ENGINE_NAME = "backtest_engine"
ENGINE_VERSION = "V3+++_Phase0_7_GodTier"

__all__ = ["run_backtest"]

# -----------------------------------------------------------------------------
# Optional modules (guarded; engine still works if missing)
# -----------------------------------------------------------------------------
try:
    from utils_pkg import ai_risk_model as _ai_risk_model  # type: ignore[attr-defined]
except Exception:  # pragma: no cover
    _ai_risk_model = None  # type: ignore[assignment]

try:
    from utils_pkg import self_learning_ai_strategy as _sl_mod  # type: ignore[attr-defined]
except Exception:  # pragma: no cover
    _sl_mod = None  # type: ignore[assignment]

try:
    from utils_pkg import regime as _regime_mod  # type: ignore[attr-defined]
except Exception:  # pragma: no cover
    _regime_mod = None  # type: ignore[assignment]

try:
    from utils_pkg import data_logger as _data_logger  # type: ignore[attr-defined]
except Exception:  # pragma: no cover
    _data_logger = None  # type: ignore[assignment]

try:
    from utils_pkg import risk_manager as _rm  # type: ignore[attr-defined]
except Exception:  # pragma: no cover
    _rm = None  # type: ignore[assignment]

try:
    from utils_pkg import ai_buy_scoring as _buy_scorer  # type: ignore[attr-defined]
except Exception:  # pragma: no cover
    _buy_scorer = None  # type: ignore[assignment]

try:
    from utils_pkg import reinvestor as _reinv  # type: ignore[attr-defined]
except Exception:  # pragma: no cover
    _reinv = None  # type: ignore[assignment]


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
    raw: Optional[Mapping[str, Any]] = None  # original row for debugging


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

    # costs (in pct points, per-trade net subtraction)
    cost_total_pct: float
    trade_cost_bps: float
    jito_tip_bps: float
    slippage_bps: float
    spread_bps: float
    cost_mode: str

    # AI risk overlay
    ai_risk_score: Optional[float]
    ai_risk_conf: Optional[float]
    ai_risk_mode: Optional[str]
    ai_risk_sizer_mult: float

    # sizing / exposure
    exposure: float
    risk_mult_effective: float

    features: Optional[Dict[str, Any]] = None


@dataclass
class ScoreTriple:
    heur: float
    ai: Optional[float]
    blended: float


# =============================================================================
# Helpers: parsing / normalization
# =============================================================================


def _clip01(x: float) -> float:
    return max(0.0, min(1.0, float(x)))


def _maybe_float(val: Any) -> Optional[float]:
    if val is None:
        return None
    try:
        f = float(val)
        if not math.isfinite(f):
            return None
        return f
    except Exception:
        return None


def _ensure_dict(obj: Any) -> Dict[str, Any]:
    """Best-effort parse of arbitrary feature payload into a dict."""
    if obj is None:
        return {}
    if isinstance(obj, dict):
        return dict(obj)
    if isinstance(obj, (bytes, bytearray)):
        try:
            obj = obj.decode("utf-8", errors="ignore")
        except Exception:
            return {}
    if isinstance(obj, str):
        s = obj.strip()
        if not s:
            return {}
        try:
            v = json.loads(s)
            return dict(v) if isinstance(v, dict) else {}
        except Exception:
            return {}
    try:
        return dict(obj)
    except Exception:
        return {}


def _to_utc_naive(dt: datetime) -> datetime:
    """
    Normalize datetime to *naive UTC* to avoid aware/naive comparison explosions.
    """
    if dt.tzinfo is None:
        return dt
    try:
        return dt.astimezone(timezone.utc).replace(tzinfo=None)
    except Exception:
        return dt.replace(tzinfo=None)


def _parse_ts_val(val: Any) -> Optional[datetime]:
    """
    Parse timestamp-ish values into datetime (naive UTC best-effort).

    Accepts:
      - datetime
      - float/int epoch seconds (or ms; auto-detect)
      - ISO8601-ish strings (with optional 'Z')
    """
    if val is None:
        return None
    if isinstance(val, datetime):
        return _to_utc_naive(val)

    s = str(val).strip()
    if not s:
        return None

    # numeric epoch seconds / ms
    try:
        num = float(s)
        if math.isfinite(num) and num > 10_000_000:  # ~1970+ seconds
            # epoch ms detection
            if num > 10_000_000_000:  # ~2286 in seconds; assume ms
                num = num / 1000.0
            return _to_utc_naive(datetime.fromtimestamp(num, tz=timezone.utc))
    except Exception:
        pass

    # ISO8601-ish
    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        return _to_utc_naive(dt)
    except Exception:
        return None


def _normalize_row(row: Mapping[str, Any]) -> Optional[BacktestRow]:
    """
    Coerce an arbitrary DB row into BacktestRow.

    This is the ONLY place you should edit if your schema changes.
    """
    feats_raw = (
        row.get("features")
        or row.get("feature_json")
        or row.get("features_json")
        or row.get("features_blob")
        or row.get("feature_vec")
    )
    features = _ensure_dict(feats_raw)

    pnl_pct: Optional[float] = None
    for key in ("pnl_pct", "pnl_percent", "return_pct", "ret_pct", "pnl"):
        if key in row and row[key] is not None:
            pnl_pct = _maybe_float(row[key])
            if pnl_pct is not None:
                break
    if pnl_pct is None:
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
    for key in ("created_at", "entry_ts", "timestamp", "ts"):
        if key in row and row[key] is not None:
            created_at = _parse_ts_val(row[key])
            if created_at is not None:
                break

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
      4) None
    """
    f = row.features
    for k in ("mint", "mint_str"):
        v = f.get(k)
        if v:
            return str(v)
    if row.raw is not None and row.raw.get("mint"):
        return str(row.raw["mint"])
    return None


# =============================================================================
# Data source: ai_db (sync+async adapter, streaming-safe)
# =============================================================================


async def _aiter_from_sync_iter(
    it: Iterable[Mapping[str, Any]],
    max_queue: int = 2048,
) -> AsyncIterator[Mapping[str, Any]]:
    """
    Stream a sync iterator into the async loop using a worker thread + asyncio.Queue.

    - Avoids materializing huge datasets.
    - Propagates exceptions.
    - Backpressure safe.
    """
    loop = asyncio.get_running_loop()
    q: asyncio.Queue[Any] = asyncio.Queue(maxsize=max_queue)
    stop = threading.Event()
    sentinel = object()

    def _worker() -> None:
        try:
            for item in it:
                if stop.is_set():
                    break
                fut = asyncio.run_coroutine_threadsafe(q.put(item), loop)
                fut.result()
        except Exception as e:
            try:
                fut = asyncio.run_coroutine_threadsafe(q.put(e), loop)
                fut.result()
            except Exception:
                pass
        finally:
            try:
                fut = asyncio.run_coroutine_threadsafe(q.put(sentinel), loop)
                fut.result()
            except Exception:
                pass

    t = threading.Thread(target=_worker, daemon=True)
    t.start()

    try:
        while True:
            item = await q.get()
            if item is sentinel:
                break
            if isinstance(item, Exception):
                raise item
            yield item
    finally:
        stop.set()


async def _iter_rows_from_ai_db(config: Mapping[str, Any]) -> AsyncIterator[BacktestRow]:
    """
    Adapter over ai_db.

    Preferred:
        ai_db.iter_backtest_rows(config) -> (sync or async) iterator of mappings
    Fallback:
        ai_db.iter_training_rows(config)

    Normalizes rows via _normalize_row and skips rows missing pnl_pct.
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
                if log.every("bt_skip_row_no_pnl", 2500):
                    log.debug("backtest_engine: skipping row without pnl_pct")
                continue
            yield br
        return

    # Sync iterator path (stream via worker thread)
    async for row in _aiter_from_sync_iter(it):  # type: ignore[arg-type]
        br = _normalize_row(row)
        if br is None:
            if log.every("bt_skip_row_no_pnl_sync", 2500):
                log.debug("backtest_engine: skipping row without pnl_pct")
            continue
        yield br


# =============================================================================
# Config helpers (strict, forgiving)
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
    return 1.0  # balanced/default


def _regime_mult_from_features(features: Mapping[str, Any], regime_mode_lock: Optional[int]) -> float:
    """
    Derive a regime-aware multiplier from features + optional lock.

    Priority:
      1) If utils_pkg.regime exposes risk_mult_from_features, defer to it
      2) regime_mode_lock:
            0 -> risk_off   (0.8)
            1 -> normal     (1.0)
            2 -> risk_on    (1.2)
      3) features["regime_risk_multiplier"]
      4) features["regime_label_numeric"] in [-1,1] -> [0.8,1.2]
      5) fallback 1.0
    """
    if _regime_mod is not None and hasattr(_regime_mod, "risk_mult_from_features"):
        try:
            v = float(_regime_mod.risk_mult_from_features(features, regime_mode_lock))  # type: ignore[attr-defined]
            return v if math.isfinite(v) and v > 0 else 1.0
        except Exception:  # pragma: no cover
            log.debug("regime.risk_mult_from_features failed; fallback.", exc_info=True)

    if regime_mode_lock in (0, 1, 2):
        return 0.8 if regime_mode_lock == 0 else (1.2 if regime_mode_lock == 2 else 1.0)

    if features.get("regime_risk_multiplier") is not None:
        try:
            v = float(features["regime_risk_multiplier"])
            if math.isfinite(v) and v > 0:
                return v
        except Exception:
            pass

    if features.get("regime_label_numeric") is not None:
        try:
            ln = float(features["regime_label_numeric"])
            ln = max(-1.0, min(1.0, ln))
            return 1.0 + 0.2 * ln
        except Exception:
            pass

    return 1.0


# =============================================================================
# Scoring helpers: ai_entry_logic stack / ai_buy_scoring stack
# =============================================================================


async def _compute_scores_entry_logic(features: Dict[str, Any], cfg: Mapping[str, Any]) -> ScoreTriple:
    """
    Baseline scoring via utils_pkg.ai_entry_logic.

    Returns ScoreTriple(heur, ai_or_None, blended).
    """
    # Preferred: V3+++ score_entry API
    if hasattr(_entry, "score_entry"):
        try:
            res = _entry.score_entry(features, ctx={"mode": "backtest", "config": cfg})  # type: ignore[attr-defined]
        except TypeError:
            res = _entry.score_entry(features)  # type: ignore[attr-defined]

        if asyncio.iscoroutine(res):
            res = await res  # type: ignore[assignment]

        heur = getattr(res, "heur_score", None)
        ai = getattr(res, "ai_score", None)
        blended = getattr(res, "blended_score", None)

        if isinstance(res, dict):
            heur = res.get("heur_score", heur)
            ai = res.get("ai_score", ai)
            blended = res.get("blended_score", blended)

        if blended is not None:
            heur_f = _clip01(float(heur) if heur is not None else float(blended))
            ai_f = _clip01(float(ai)) if ai is not None else None
            blended_f = _clip01(float(blended))
            return ScoreTriple(heur=heur_f, ai=ai_f, blended=blended_f)

    # Fallback heuristic score
    heur_score = None
    for name in ("_heuristic_score", "heuristic_score", "score_heuristic"):
        if hasattr(_entry, name):
            try:
                heur_score = float(getattr(_entry, name)(features))  # type: ignore[misc]
                break
            except Exception:
                pass
    if heur_score is None:
        heur_score = float(_maybe_float(features.get("heur_score") or features.get("entry_heur")) or 0.0)
    heur_score = _clip01(heur_score)

    # Blend weight
    w = _get_cfg_float(
        cfg,
        "ENTRY_AI_WEIGHT",
        "entry_ai_weight",
        default=float(getattr(_entry, "ENTRY_AI_WEIGHT", 0.7)),
    )
    w = _clip01(w)

    # AI score fallback
    ai_score = None
    for name in ("_ai_score", "ai_score", "score_ai"):
        if hasattr(_entry, name):
            try:
                r = getattr(_entry, name)(features)  # type: ignore[misc]
                if asyncio.iscoroutine(r):
                    r = await r
                ai_score = _maybe_float(r)
                break
            except Exception:
                pass

    if ai_score is None:
        return ScoreTriple(heur=heur_score, ai=None, blended=heur_score)

    blended = _clip01(w * float(ai_score) + (1.0 - w) * heur_score)
    return ScoreTriple(heur=heur_score, ai=_clip01(float(ai_score)), blended=blended)


async def _compute_scores_ai_buy_scoring(features: Dict[str, Any], cfg: Mapping[str, Any]) -> Optional[ScoreTriple]:
    """
    Optional scoring via utils_pkg.ai_buy_scoring.

    Returns ScoreTriple(...) or None if unavailable/fails.
    """
    if _buy_scorer is None or not hasattr(_buy_scorer, "score_only"):
        return None

    try:
        candidate: Any
        if hasattr(_buy_scorer, "Candidate"):
            Cand = getattr(_buy_scorer, "Candidate")
            try:
                candidate = Cand(features=features, ctx={"mode": "backtest", "config": cfg})
            except TypeError:
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
            heur = res.get("heur_score", heur)
            ai = res.get("ai_score", ai)
            blended = res.get("blended_score", blended)

        if blended is None and isinstance(res, (tuple, list)) and len(res) >= 1:
            blended = float(res[0])
            if heur is None:
                heur = blended

        if blended is None:
            return None

        heur_f = _clip01(float(heur) if heur is not None else float(blended))
        ai_f = _clip01(float(ai)) if ai is not None else None
        blended_f = _clip01(float(blended))
        return ScoreTriple(heur=heur_f, ai=ai_f, blended=blended_f)

    except Exception:
        log.debug("backtest_engine: ai_buy_scoring failed; falling back.", exc_info=True)
        return None


async def _compute_scores(features: Dict[str, Any], cfg: Mapping[str, Any]) -> ScoreTriple:
    """
    Unified scoring router.

    Engine selection:
      1) BT_SCORE_ENGINE / score_engine:
           "ai_buy_scoring" / "buy" / "buy_scoring" -> ai_buy_scoring
           "entry_logic" / default                  -> ai_entry_logic
      2) BT_USE_AI_BUY_SCORING / use_ai_buy_scoring (bool)
      3) fallback -> ai_entry_logic
    """
    engine_raw = str(cfg.get("BT_SCORE_ENGINE") or cfg.get("score_engine") or "").strip().lower()
    use_ai_buy_flag = _get_cfg_bool(cfg, "BT_USE_AI_BUY_SCORING", "use_ai_buy_scoring", default=False)

    use_buy_engine = engine_raw in ("ai_buy_scoring", "buy", "buy_scoring", "buy_engine") or use_ai_buy_flag
    if use_buy_engine:
        scored = await _compute_scores_ai_buy_scoring(features, cfg)
        if scored is not None:
            return scored

    return await _compute_scores_entry_logic(features, cfg)


# =============================================================================
# Quant metrics helpers
# =============================================================================


def _compute_sharpe(returns: List[float]) -> float:
    """Simple Sharpe-like metric on per-trade equity returns (no RF, no annualization)."""
    if not returns:
        return 0.0
    if len(returns) == 1:
        return returns[0]
    avg = statistics.mean(returns)
    std = statistics.pstdev(returns)
    if std <= 1e-12:
        return 0.0
    return avg / std


def _compute_downside_dev(returns_pct: List[float]) -> float:
    """Downside deviation in *pct units* (only negative trades, target=0)."""
    downsides = [r for r in returns_pct if r < 0]
    if not downsides:
        return 0.0
    if len(downsides) == 1:
        return abs(downsides[0])
    var = statistics.pvariance(downsides, mu=0.0)
    return math.sqrt(var)


def _compute_skew_kurtosis(values: List[float]) -> Tuple[float, float]:
    """Skewness and excess kurtosis (population moments)."""
    n = len(values)
    if n < 3:
        return 0.0, 0.0
    mean = statistics.mean(values)
    std = statistics.pstdev(values)
    if std <= 1e-12:
        return 0.0, 0.0
    m3 = sum((x - mean) ** 3 for x in values) / n
    m4 = sum((x - mean) ** 4 for x in values) / n
    skew = m3 / (std ** 3)
    kurt_excess = m4 / (std ** 4) - 3.0
    return skew, kurt_excess


def _compute_consecutive_runs(chron_returns: List[float]) -> Tuple[int, int]:
    """Max consecutive wins/losses using chronological sign."""
    max_w = max_l = 0
    cur_w = cur_l = 0
    for r in chron_returns:
        if r > 0:
            cur_w += 1
            cur_l = 0
        elif r < 0:
            cur_l += 1
            cur_w = 0
        else:
            cur_w = 0
            cur_l = 0
        max_w = max(max_w, cur_w)
        max_l = max(max_l, cur_l)
    return max_w, max_l


def _safe_quantile(values: List[float], q: float) -> float:
    """Safe index-based quantile approximation."""
    if not values:
        return 0.0
    if len(values) == 1:
        return float(values[0])
    v = sorted(values)
    q = max(0.0, min(1.0, float(q)))
    idx = int(q * (len(v) - 1))
    return float(v[idx])


def _gain_to_pain(returns_pct: List[float]) -> float:
    """Sum(positive)/abs(sum(negative))."""
    pos = sum(r for r in returns_pct if r > 0)
    neg = sum(r for r in returns_pct if r < 0)
    return (pos / abs(neg)) if neg < 0 else 0.0


def _omega_ratio(returns_pct: List[float], threshold_pct: float = 0.0) -> float:
    """
    Omega ratio at threshold: sum(max(r-th,0)) / sum(max(th-r,0)).
    """
    gains = 0.0
    losses = 0.0
    th = float(threshold_pct)
    for r in returns_pct:
        gains += max(0.0, r - th)
        losses += max(0.0, th - r)
    return (gains / losses) if losses > 1e-12 else 0.0


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
        equity: float,
        score_min: float,
        blended_gate: float,
        risk_mult_override: float,
    ) -> Tuple[float, float, float]:
        """
        Lightweight adaptive logic (backtest-only):

          - If win_rate low or window pnl < 0: tighten gates, reduce risk
          - If win_rate high and window pnl > 0: gently increase risk

        Returns updated (score_min, blended_gate, risk_mult_override).
        """
        if not self.enabled:
            return score_min, blended_gate, risk_mult_override

        self.window_pnls.append(float(pnl_pct))
        self.window_ts.append(created_at)

        n = len(self.window_pnls)
        if n < max(1, self.min_trades):
            return score_min, blended_gate, risk_mult_override

        if self.interval_sec is not None and self.interval_sec > 0:
            if created_at is not None and self.last_tune_ts is not None:
                if (created_at - self.last_tune_ts).total_seconds() < self.interval_sec:
                    return score_min, blended_gate, risk_mult_override

        if self.max_tunes_per_day is not None and self.max_tunes_per_day > 0:
            if created_at is not None and self.window_ts and self.window_ts[0] is not None:
                days = max(1.0, (created_at - self.window_ts[0]).total_seconds() / 86400.0)
                if self.tune_count >= self.max_tunes_per_day * days:
                    return score_min, blended_gate, risk_mult_override

        wins = [p for p in self.window_pnls if p > 0]
        win_rate = len(wins) / float(n) if n else 0.0
        window_pnl = sum(self.window_pnls)

        new_score_min = score_min
        new_blended_gate = blended_gate
        new_risk_mult = risk_mult_override

        defensive = (win_rate < 0.45) or (window_pnl < 0.0)
        offensive = (win_rate > 0.60) and (window_pnl > 0.0) and not defensive

        if defensive:
            new_score_min = min(0.98, score_min + 0.02)
            if blended_gate > 0.0:
                new_blended_gate = min(0.98, blended_gate + 0.02)
            new_risk_mult = max(0.5, risk_mult_override * 0.9)
        elif offensive:
            new_risk_mult = min(2.0, risk_mult_override * 1.10)

        if (
            abs(new_score_min - score_min) < 1e-12
            and abs(new_blended_gate - blended_gate) < 1e-12
            and abs(new_risk_mult - risk_mult_override) < 1e-12
        ):
            return score_min, blended_gate, risk_mult_override

        self.tune_count += 1
        self.last_tune_ts = created_at or self.last_tune_ts

        log.info(
            "backtest_engine.self_learning: tune #%d (win_rate=%.3f, window_pnl=%.2f, "
            "score_min=%.3f→%.3f, blended_gate=%.3f→%.3f, risk_mult=%.3f→%.3f, equity=%.4f)",
            self.tune_count,
            win_rate,
            window_pnl,
            score_min,
            new_score_min,
            blended_gate,
            new_blended_gate,
            risk_mult_override,
            new_risk_mult,
            equity,
        )

        if _sl_mod is not None and hasattr(_sl_mod, "record_backtest_tune"):
            try:
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

        self.window_pnls.clear()
        self.window_ts.clear()
        return new_score_min, new_blended_gate, new_risk_mult


def _build_self_learning_state(config: Mapping[str, Any]) -> _SelfLearningState:
    enabled = _get_cfg_bool(config, "BT_ENABLE_SELF_LEARNING", "enable_self_learning", default=False)
    use_llm = _get_cfg_bool(config, "BT_SELF_LEARN_USE_LLM", "self_learning_use_llm", default=False)

    interval_sec = None
    raw_interval = config.get("BT_SELF_LEARN_INTERVAL_SEC") or config.get("self_learning_interval_sec")
    if raw_interval:
        try:
            interval_sec = int(raw_interval)
            if interval_sec <= 0:
                interval_sec = None
        except Exception:
            interval_sec = None

    min_trades = _get_cfg_int(config, "BT_SELF_LEARN_MIN_TRADES", "self_learning_min_trades", default=10)

    max_tunes_per_day = None
    raw_max = config.get("BT_SELF_LEARN_MAX_TUNES_PER_DAY") or config.get("self_learning_max_tunes_per_day")
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
    Resolve AI risk score / confidence for this candidate if overlay is enabled.

    Priority:
      1) Row precomputed (row.ai_risk_score/conf)
      2) Feature-level (features["ai_risk_score"/...])
      3) utils_pkg.ai_risk_model if present (async or sync)
      4) None
    """
    score = row.ai_risk_score
    conf = row.ai_risk_conf

    if score is None:
        score = _maybe_float(features.get("ai_risk_score") or features.get("risk_score") or features.get("fraud_score"))
    if conf is None:
        conf = _maybe_float(features.get("ai_risk_conf") or features.get("risk_confidence") or features.get("fraud_conf"))

    if score is None and _ai_risk_model is not None:
        try:
            fn = None
            for name in ("score_entry", "score_candidate", "score", "evaluate"):
                if hasattr(_ai_risk_model, name):
                    fn = getattr(_ai_risk_model, name)
                    break
            if fn is not None:
                res = fn(features, cfg)  # type: ignore[misc]
                if asyncio.iscoroutine(res):
                    res = await res  # type: ignore[assignment]
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
            log.debug("ai_risk_model scoring failed; ignoring overlay.", exc_info=True)

    return score, conf


# =============================================================================
# Costs helper
# =============================================================================


def _calc_effective_cost_pct(
    *,
    trade_cost_bps: float,
    jito_tip_bps: float,
    slippage_bps: float,
    spread_bps: float,
    mode: str,
) -> float:
    """
    Convert cost bps -> % points and apply mode.

    mode:
      - "per_trade": full subtraction (treat costs as already round-trip)
      - "half_side": halve the bps sum (treat bps as per-side and we model one side)
    """
    total_bps = max(0.0, float(trade_cost_bps) + float(jito_tip_bps) + float(slippage_bps) + float(spread_bps))
    if total_bps <= 0:
        return 0.0
    cost_pct = total_bps / 100.0  # bps -> pct points
    if mode == "half_side":
        cost_pct *= 0.5
    return float(cost_pct)


# =============================================================================
# Main backtest engine
# =============================================================================


async def run_backtest(config: Mapping[str, Any]) -> Dict[str, Any]:
    """
    Main entrypoint for scripts/backtest.py.

    Returns a flat dict of metrics + optional trades/equity_series.

    NOTE: Any unknown keys in `config` are passed to ai_db.iter_backtest_rows(...).
    """
    tag = str(config.get("config_tag") or config.get("BT_TAG_PREFIX") or "")

    # Optional time window filters
    start_dt = _parse_ts_val(config.get("BT_START_TS") or config.get("bt_start_ts") or config.get("start_ts"))
    end_dt = _parse_ts_val(config.get("BT_END_TS") or config.get("bt_end_ts") or config.get("end_ts"))

    # Gates / knobs
    score_min = _get_cfg_float(config, "ENTRY_SCORE_MIN", "entry_score_min", default=0.6)
    entry_min_score = _get_cfg_float(config, "ENTRY_MIN_SCORE", "entry_min_score", default=0.0)
    blended_gate = max(0.0, float(entry_min_score))

    score_min_initial = float(score_min)
    blended_gate_initial = float(blended_gate)

    max_positions = max(1, _get_cfg_int(config, "max_positions", "BT_MAX_POSITIONS", default=1))

    # If you want explicit per-position fraction (instead of 1/max_positions)
    position_fraction = config.get("BT_POSITION_FRACTION")
    try:
        position_fraction_f = float(position_fraction) if position_fraction is not None else (1.0 / float(max_positions))
    except Exception:
        position_fraction_f = 1.0 / float(max_positions)
    position_fraction_f = max(0.0, min(1.0, position_fraction_f))

    risk_mult_override = _get_cfg_float(config, "risk_mult_override", "BT_RISK_MULT_OVERRIDE", default=1.0)
    risk_profile = str(config.get("RISK_PROFILE") or config.get("risk_profile") or "").strip() or None
    risk_profile_mult = _risk_profile_scale(risk_profile)

    # Regime lock
    regime_mode_lock_raw = config.get("regime_risk_mode_lock") or config.get("BT_REGIME_MODE_LOCK")
    try:
        regime_mode_lock: Optional[int] = int(regime_mode_lock_raw) if regime_mode_lock_raw is not None else None
    except Exception:
        regime_mode_lock = None

    # Risk-capacity gating
    require_rc = _get_cfg_bool(config, "ENTRY_REQUIRE_RISK_CAPACITY", "require_risk_capacity", default=False)
    require_positive_rc = _get_cfg_bool(config, "BT_REQUIRE_POSITIVE_RC", "require_positive_rc", default=False)
    min_rc = config.get("BT_MIN_RISK_CAPACITY", config.get("min_risk_capacity"))
    max_rc = config.get("BT_MAX_RISK_CAPACITY", config.get("max_risk_capacity"))
    try:
        min_rc_val = float(min_rc) if min_rc is not None else None
    except Exception:
        min_rc_val = None
    try:
        max_rc_val = float(max_rc) if max_rc is not None else None
    except Exception:
        max_rc_val = None

    # Trade caps
    max_trades_cfg = config.get("BT_MAX_TRADES", config.get("max_trades"))
    try:
        max_trades = int(max_trades_cfg) if max_trades_cfg else None
    except Exception:
        max_trades = None

    limit_candidates_cfg = config.get("limit_candidates", config.get("BT_LIMIT_CANDIDATES"))
    try:
        limit_candidates: Optional[int] = int(limit_candidates_cfg) if limit_candidates_cfg else None
        if limit_candidates is not None and limit_candidates <= 0:
            limit_candidates = None
    except Exception:
        limit_candidates = None

    # Smart-wallet gates
    min_sw_score = _get_cfg_float(config, "BT_MIN_SMART_WALLET_SCORE", "min_smart_wallet_score", default=-1.0)
    min_sw_score = None if min_sw_score < 0.0 else float(min_sw_score)
    require_sw_flag = _get_cfg_bool(config, "BT_REQUIRE_SMART_WALLET_FLAG", "require_smart_wallet_flag", default=False)

    # Dedup
    dedup_mode_raw = str(config.get("BT_DEDUP_MODE") or config.get("dedup_mode") or "").strip().lower()
    dedup_mode = dedup_mode_raw if dedup_mode_raw in ("mint", "mint_window") else "off"
    dedup_window_cfg = config.get("BT_DEDUP_WINDOW_SEC", config.get("dedup_window_sec"))
    try:
        dedup_window_sec: Optional[float] = float(dedup_window_cfg) if dedup_window_cfg else None
        if dedup_window_sec is not None and dedup_window_sec <= 0:
            dedup_window_sec = None
    except Exception:
        dedup_window_sec = None

    # Label filters
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

    # Segment filter
    result_segment_cfg = config.get("BT_RESULT_SEGMENT") or config.get("result_segment") or ""
    result_segment = str(result_segment_cfg).strip().lower() or None
    if result_segment in ("full", "all"):
        result_segment = None

    # Weighting (research)
    label_weights: Dict[str, float] = {}
    lw_raw = config.get("BT_LABEL_WEIGHTS_JSON") or config.get("label_weights_json")
    if lw_raw:
        try:
            label_weights = {str(k): float(v) for k, v in json.loads(str(lw_raw)).items()}
        except Exception:
            log.warning("backtest_engine: could not parse label_weights_json; ignoring.")

    sample_weight_mode = str(config.get("BT_SAMPLE_WEIGHT_MODE") or config.get("sample_weight_mode") or "").strip() or None

    # Logging controls (guard memory by caps)
    log_trades = _get_cfg_bool(config, "BT_LOG_TRADES", "log_trades", default=False)
    log_features = _get_cfg_bool(config, "BT_LOG_FEATURES", "log_features", default=False)
    log_equity_series = _get_cfg_bool(config, "BT_LOG_EQUITY_SERIES", "log_equity_series", default=False)
    max_log_trades = max(0, _get_cfg_int(config, "BT_MAX_LOG_TRADES", "max_log_trades", default=25000))
    max_log_equity = max(0, _get_cfg_int(config, "BT_MAX_LOG_EQUITY_POINTS", "max_log_equity_points", default=25000))
    trades_for_log: List[Dict[str, Any]] = []
    equity_series: List[Dict[str, Any]] = []

    # Live risk simulation + equity halt
    use_live_rm = _get_cfg_bool(config, "BT_USE_LIVE_RISK_MANAGER", "use_live_risk_manager", default=False)
    alloc_mode_raw = str(config.get("BT_ALLOC_MODE") or config.get("alloc_mode") or "").strip().lower()
    alloc_mode = alloc_mode_raw if alloc_mode_raw in ("simple", "risk_capacity") else "simple"

    eq_halt_dd_cfg = config.get("BT_RISK_EQUITY_HALT_DD", config.get("risk_equity_halt_dd_pct"))
    try:
        equity_halt_dd_pct: Optional[float] = float(eq_halt_dd_cfg) if eq_halt_dd_cfg is not None else None
    except Exception:
        equity_halt_dd_pct = None

    # Overlap simulation (more realistic capacity handling if created_at+hold_sec exist)
    simulate_overlap = _get_cfg_bool(config, "BT_SIMULATE_OVERLAP", "simulate_overlap", default=False)

    # Self-learning state (backtest-only adaptive tuning)
    sl_state = _build_self_learning_state(config)

    # Reinvestor knobs (approx)
    enable_reinvestor = _get_cfg_bool(config, "BT_ENABLE_REINVESTOR", "enable_reinvestor", default=False)
    reinvest_percent = _get_cfg_float(config, "BT_REINVEST_PERCENT", "reinvest_percent_override", default=0.0)
    reinvest_min_profit_sol = _get_cfg_float(config, "BT_REINVEST_MIN_PROFIT_SOL", "reinvest_min_profit_sol", default=0.0)
    reinvest_dd_soft_stop_pct = _get_cfg_float(config, "BT_REINVEST_DD_SOFT_STOP_PCT", "reinvest_dd_soft_stop_pct", default=0.0)
    reinvest_dd_hard_stop_pct = _get_cfg_float(config, "BT_REINVEST_DD_HARD_STOP_PCT", "reinvest_dd_hard_stop_pct", default=0.0)

    # AI risk overlay knobs
    use_ai_risk_overlay = _get_cfg_bool(
        config,
        "BT_USE_AI_RISK_MODEL",
        "use_ai_risk_overlay",
        "use_ai_risk_model",
        default=False,
    )
    ai_risk_max_score = _get_cfg_float(config, "BT_AI_RISK_MAX_SCORE", "ai_risk_max_score", default=1.01)
    ai_risk_min_conf = _get_cfg_float(config, "BT_AI_RISK_MIN_CONF", "ai_risk_min_confidence", default=0.0)
    ai_risk_mode_raw = str(config.get("BT_AI_RISK_MODE") or config.get("ai_risk_mode") or "").strip().lower()
    ai_risk_mode = ai_risk_mode_raw if ai_risk_mode_raw in ("filter", "sizer") else "filter"

    # Cost model (bps)
    trade_cost_bps = _get_cfg_float(config, "BT_TRADE_COST_BPS", "trade_cost_bps", default=0.0)
    jito_tip_bps = _get_cfg_float(config, "BT_JITO_TIP_BPS", "jito_tip_bps", default=0.0)
    # Optional realism knobs (still bps; default 0)
    slippage_bps = _get_cfg_float(config, "BT_SLIPPAGE_BPS", "slippage_bps", default=0.0)
    spread_bps = _get_cfg_float(config, "BT_SPREAD_BPS", "spread_bps", default=0.0)

    trade_cost_mode_raw = str(config.get("BT_TRADE_COST_MODE") or config.get("trade_cost_mode") or "").strip().lower()
    trade_cost_mode = trade_cost_mode_raw if trade_cost_mode_raw in ("per_trade", "half_side") else "per_trade"

    # Equity init + cap
    equity_init_cfg = config.get("BT_EQUITY_INIT") or config.get("equity_init")
    try:
        equity_init = float(equity_init_cfg) if equity_init_cfg is not None else 1.0
    except Exception:
        equity_init = 1.0
    if not math.isfinite(equity_init) or equity_init <= 0.0:
        equity_init = 1.0

    equity_cap_mult_cfg = config.get("BT_EQUITY_CAP_MULT") or config.get("equity_cap_mult")
    try:
        equity_cap_mult: Optional[float] = float(equity_cap_mult_cfg) if equity_cap_mult_cfg is not None else None
        if equity_cap_mult is not None and equity_cap_mult <= 0.0:
            equity_cap_mult = None
    except Exception:
        equity_cap_mult = None

    log.info(
        "backtest_engine: starting run (tag=%s, max_trades=%s, limit_candidates=%s, start=%s, end=%s, "
        "risk_profile=%s, overlap=%s, score_engine=%s)",
        tag or "<none>",
        max_trades,
        limit_candidates,
        start_dt,
        end_dt,
        risk_profile or "<default>",
        simulate_overlap,
        str(config.get("BT_SCORE_ENGINE") or config.get("score_engine") or "entry_logic"),
    )

    # -------------------------------------------------------------------------
    # Iterate rows & simulate portfolio
    # -------------------------------------------------------------------------
    n_candidates = 0
    n_trades = 0
    n_wins = 0
    n_losses = 0

    pnl_list: List[float] = []  # per-trade net pnl% (chron)
    dd_list: List[float] = []  # per-trade max_dd_pct if provided
    hold_list: List[float] = []  # hold_sec if provided

    # Equity path
    equity = float(equity_init)
    hwm = float(equity_init)
    max_dd = 0.0
    max_runup = 0.0
    equity_returns: List[float] = []  # r_alloc (decimal)
    equity_halt_triggered = False
    eq_drawdowns_pct: List[float] = []  # drawdown% per trade

    # Diagnostics
    blended_scores: List[float] = []
    heur_scores: List[float] = []
    ai_scores: List[float] = []

    ai_risk_scores_used: List[float] = []
    ai_risk_rejects = 0

    # Weighted research
    weighted_return_sum = 0.0
    weight_sum = 0.0

    # Profit-factor components
    gross_win_sum = 0.0
    gross_loss_sum = 0.0  # negative

    # Dedup state
    last_seen_by_mint: Dict[str, float] = {}  # mint -> last created_at epoch seconds

    # Span in days
    first_ts: Optional[datetime] = None
    last_ts: Optional[datetime] = None

    # Reinvestor diagnostics
    reinvest_count = 0
    reinvest_pnl_pct = 0.0

    # Exposure / risk diagnostics
    exposure_list: List[float] = []
    risk_mult_effective_sum = 0.0
    cost_sum_pct = 0.0

    # Buckets
    rc_bucket_defs: List[Tuple[float, float, str]] = [
        (0.0, 0.25, "0.00-0.25"),
        (0.25, 0.50, "0.25-0.50"),
        (0.50, 0.75, "0.50-0.75"),
        (0.75, 1.01, "0.75-1.00"),
    ]
    rc_bucket_acc: Dict[str, Dict[str, Any]] = {}

    score_bucket_defs: List[Tuple[float, float, str]] = [
        (0.0, 0.70, "0.00-0.70"),
        (0.70, 0.80, "0.70-0.80"),
        (0.80, 0.90, "0.80-0.90"),
        (0.90, 1.01, "0.90-1.00"),
    ]
    score_bucket_acc: Dict[str, Dict[str, Any]] = {}

    regime_bucket_acc: Dict[str, Dict[str, Any]] = {}
    hour_bucket_acc: Dict[int, Dict[str, Any]] = {}

    label_counts: Dict[str, int] = {}
    label_pnl_sum: Dict[str, float] = {}
    label_win_counts: Dict[str, int] = {}
    label_loss_counts: Dict[str, int] = {}

    # Overlap state: min-heap of end times for open positions
    open_pos_ends: List[datetime] = []

    async for row in _iter_rows_from_ai_db(config):
        if limit_candidates is not None and n_candidates >= limit_candidates:
            break
        n_candidates += 1

        # Time window filters
        if start_dt or end_dt:
            if row.created_at is None:
                continue
            if start_dt and row.created_at < start_dt:
                continue
            if end_dt and row.created_at > end_dt:
                continue

        # Segment filter (if present)
        if result_segment:
            seg = ""
            if row.raw is not None:
                seg = str(row.raw.get("segment") or row.raw.get("split_segment") or "").strip().lower()
            if not seg:
                seg = str(row.features.get("segment") or "").strip().lower()
            if seg and seg != result_segment:
                continue

        # Label filters
        if row.label is not None:
            if label_include and row.label not in label_include:
                continue
            if label_exclude and row.label in label_exclude:
                continue

        # Overlap capacity (optional)
        if simulate_overlap and row.created_at is not None:
            while open_pos_ends and open_pos_ends[0] <= row.created_at:
                heapq.heappop(open_pos_ends)
            if len(open_pos_ends) >= max_positions:
                continue

        feats = row.features

        # Risk capacity extraction
        rc = _maybe_float(feats.get("risk_capacity_score") or feats.get("risk_capacity") or feats.get("rc_score"))
        # Risk-capacity gating
        if require_rc and rc is None:
            continue
        if rc is not None and min_rc_val is not None and rc < min_rc_val:
            continue
        if rc is not None and max_rc_val is not None and rc > max_rc_val:
            continue
        if require_positive_rc and rc is not None and rc <= 0.0:
            continue

        # Smart-wallet gating
        sw_score = _maybe_float(feats.get("smart_wallet_score"))
        sw_flag_bool = bool(feats.get("smart_wallet_flag") or False)

        if require_sw_flag and not sw_flag_bool:
            continue
        if min_sw_score is not None:
            if sw_score is None or sw_score < min_sw_score:
                continue

        # Dedup
        if dedup_mode != "off":
            mint = _extract_mint(row)
            if mint:
                if dedup_mode == "mint":
                    if mint in last_seen_by_mint:
                        continue
                    last_seen_by_mint[mint] = row.created_at.timestamp() if row.created_at else 0.0
                elif dedup_mode == "mint_window":
                    ts = row.created_at.timestamp() if row.created_at else 0.0
                    last_ts_mint = last_seen_by_mint.get(mint)
                    if last_ts_mint is not None and dedup_window_sec is not None:
                        if (ts - last_ts_mint) <= dedup_window_sec:
                            continue
                    last_seen_by_mint[mint] = ts

        # AI risk overlay
        ai_risk_score = None
        ai_risk_conf = None
        ai_risk_sizer_mult = 1.0

        if use_ai_risk_overlay:
            ai_risk_score, ai_risk_conf = await _maybe_ai_risk_score(row, feats, config)
            if ai_risk_score is not None:
                # allow 0..100 inputs
                if ai_risk_score > 1.0 and ai_risk_score <= 100.0:
                    ai_risk_score = ai_risk_score / 100.0

                conf_ok = (ai_risk_conf is None) or (ai_risk_conf >= ai_risk_min_conf) or (ai_risk_min_conf <= 0.0)
                if conf_ok:
                    if ai_risk_mode == "filter":
                        if ai_risk_score > ai_risk_max_score:
                            ai_risk_rejects += 1
                            continue
                        ai_risk_scores_used.append(ai_risk_score)
                    else:  # sizer
                        ai_risk_scores_used.append(ai_risk_score)
                        ai_risk_sizer_mult = max(0.0, min(1.0, 1.0 - ai_risk_score))

        # Entry scoring (same stack as live)
        scores = await _compute_scores(feats, config)

        # Gates
        if scores.heur < score_min:
            continue
        if blended_gate > 0.0 and scores.blended < blended_gate:
            continue

        # Passed -> take trade
        n_trades += 1

        # If overlap simulation is enabled and we have created_at, register an "end"
        if simulate_overlap and row.created_at is not None:
            hs = float(row.hold_sec) if row.hold_sec is not None and row.hold_sec > 0 else 0.0
            heapq.heappush(open_pos_ends, row.created_at + timedelta(seconds=hs))

        # Gross pnl then subtract costs
        gross_pnl_pct = float(row.pnl_pct)
        pnl_pct = float(gross_pnl_pct)

        effective_cost_pct = _calc_effective_cost_pct(
            trade_cost_bps=trade_cost_bps,
            jito_tip_bps=jito_tip_bps,
            slippage_bps=slippage_bps,
            spread_bps=spread_bps,
            mode=trade_cost_mode,
        )
        if effective_cost_pct > 0.0:
            pnl_pct -= effective_cost_pct
            cost_sum_pct += effective_cost_pct

        pnl_list.append(pnl_pct)

        if row.max_dd_pct is not None:
            dd_list.append(float(row.max_dd_pct))
        if row.hold_sec is not None:
            hold_list.append(float(row.hold_sec))

        if row.created_at is not None:
            if first_ts is None:
                first_ts = row.created_at
            last_ts = row.created_at

        # Win/loss
        if pnl_pct > 0:
            n_wins += 1
            gross_win_sum += pnl_pct
        elif pnl_pct < 0:
            n_losses += 1
            gross_loss_sum += pnl_pct

        # Buckets: RC
        if rc is not None:
            rc_val = float(rc)
            for lo, hi, name in rc_bucket_defs:
                if (rc_val >= lo and rc_val < hi) or (name.endswith("1.00") and rc_val <= hi):
                    b = rc_bucket_acc.setdefault(name, {"count": 0, "pnl_sum": 0.0, "win_count": 0, "rc_sum": 0.0})
                    b["count"] += 1
                    b["pnl_sum"] += pnl_pct
                    b["rc_sum"] += rc_val
                    if pnl_pct > 0:
                        b["win_count"] += 1
                    break

        # Buckets: score
        for lo, hi, name in score_bucket_defs:
            if (scores.blended >= lo and scores.blended < hi) or (name.endswith("1.00") and scores.blended <= hi):
                sb = score_bucket_acc.setdefault(name, {"count": 0, "pnl_sum": 0.0, "win_count": 0})
                sb["count"] += 1
                sb["pnl_sum"] += pnl_pct
                if pnl_pct > 0:
                    sb["win_count"] += 1
                break

        # Label stats
        if row.label:
            label_counts[row.label] = label_counts.get(row.label, 0) + 1
            label_pnl_sum[row.label] = label_pnl_sum.get(row.label, 0.0) + pnl_pct
            if pnl_pct > 0:
                label_win_counts[row.label] = label_win_counts.get(row.label, 0) + 1
            elif pnl_pct < 0:
                label_loss_counts[row.label] = label_loss_counts.get(row.label, 0) + 1

        # Regime stats
        regime_key = None
        if feats.get("regime_label"):
            regime_key = str(feats["regime_label"]).strip().lower()
        elif feats.get("regime_label_numeric") is not None:
            rn = _maybe_float(feats.get("regime_label_numeric"))
            if rn is not None:
                regime_key = f"num_{rn:+.1f}"
        if not regime_key:
            regime_key = "unknown"

        reg = regime_bucket_acc.setdefault(
            regime_key,
            {"count": 0, "pnl_sum": 0.0, "win_count": 0, "loss_count": 0},
        )
        reg["count"] += 1
        reg["pnl_sum"] += pnl_pct
        if pnl_pct > 0:
            reg["win_count"] += 1
        elif pnl_pct < 0:
            reg["loss_count"] += 1

        # Hour-of-day stats
        if row.created_at is not None:
            hour = int(row.created_at.hour)
            hb = hour_bucket_acc.setdefault(hour, {"count": 0, "pnl_sum": 0.0, "win_count": 0, "loss_count": 0})
            hb["count"] += 1
            hb["pnl_sum"] += pnl_pct
            if pnl_pct > 0:
                hb["win_count"] += 1
            elif pnl_pct < 0:
                hb["loss_count"] += 1

        # Portfolio simulation
        r_raw = pnl_pct / 100.0  # % -> decimal
        regime_mult = _regime_mult_from_features(feats, regime_mode_lock)
        risk_mult_effective = float(risk_mult_override) * float(risk_profile_mult) * float(regime_mult) * float(ai_risk_sizer_mult)

        # Allocation factor: live RM vs simple vs risk_capacity
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

        # Per-position fraction (default: 1/max_positions, or BT_POSITION_FRACTION override)
        exposure = alloc_factor * position_fraction_f
        r_alloc = r_raw * risk_mult_effective * exposure

        equity_returns.append(r_alloc)
        exposure_list.append(exposure)
        risk_mult_effective_sum += risk_mult_effective

        equity *= (1.0 + r_alloc)

        # Equity cap
        if equity_cap_mult is not None:
            cap_val = equity_init * equity_cap_mult
            if equity > cap_val:
                equity = cap_val

        if equity > hwm:
            hwm = equity
        max_runup = max(max_runup, (hwm - equity_init) / equity_init)

        drawdown = 0.0 if hwm <= 0 else (hwm - equity) / hwm
        max_dd = max(max_dd, drawdown)
        eq_drawdowns_pct.append(drawdown * 100.0)

        if log_equity_series and (max_log_equity <= 0 or len(equity_series) < max_log_equity):
            equity_series.append(
                {
                    "trade_index": n_trades,
                    "equity": equity,
                    "drawdown": drawdown,
                    "created_at": row.created_at.isoformat() if row.created_at else None,
                }
            )

        # Equity kill switch
        if equity_halt_dd_pct is not None and (drawdown * 100.0) >= equity_halt_dd_pct:
            equity_halt_triggered = True
            log.info(
                "backtest_engine: equity halt triggered at DD=%.2f%% (threshold=%.2f%%)",
                drawdown * 100.0,
                equity_halt_dd_pct,
            )

        # Optional RM hook
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

        # Reinvestor approx
        if enable_reinvestor and pnl_pct > 0:
            qualifies = True
            if reinvest_dd_hard_stop_pct > 0 and (drawdown * 100.0) >= reinvest_dd_hard_stop_pct:
                qualifies = False
            if reinvest_dd_soft_stop_pct > 0 and (drawdown * 100.0) >= reinvest_dd_soft_stop_pct:
                qualifies = False

            if reinvest_min_profit_sol > 0:
                # DB rows don't carry SOL notional; treat as "must be a winner"
                qualifies = qualifies and pnl_pct > 0

            if qualifies:
                reinvest_count += 1
                if _reinv is not None and hasattr(_reinv, "backtest_on_win"):
                    try:
                        extra = _reinv.backtest_on_win(  # type: ignore[attr-defined]
                            pnl_pct=pnl_pct,
                            equity=equity,
                            config=config,
                        )
                        reinvest_pnl_pct += float(extra)
                    except Exception:  # pragma: no cover
                        reinvest_pnl_pct += pnl_pct * (reinvest_percent / 100.0) if reinvest_percent > 0 else pnl_pct
                else:
                    reinvest_pnl_pct += pnl_pct * (reinvest_percent / 100.0) if reinvest_percent > 0 else pnl_pct

        # Weighted metric
        if sample_weight_mode and row.label is not None:
            if sample_weight_mode == "by_label":
                w = label_weights.get(row.label, 1.0)
            elif sample_weight_mode == "by_outcome_severity":
                w = 1.0 + max(0.0, -pnl_pct) / 100.0
            elif sample_weight_mode == "uniform":
                w = 1.0
            else:
                w = 1.0
            weighted_return_sum += w * pnl_pct
            weight_sum += w

        # Score diagnostics
        blended_scores.append(scores.blended)
        heur_scores.append(scores.heur)
        if scores.ai is not None:
            ai_scores.append(scores.ai)

        # Optional per-trade logging
        if log_trades and (max_log_trades <= 0 or len(trades_for_log) < max_log_trades):
            trade = BacktestTrade(
                pnl_pct=pnl_pct,
                pnl_pct_gross=gross_pnl_pct,
                heur_score=scores.heur,
                blended_score=scores.blended,
                ai_score=scores.ai,
                label=row.label,
                risk_capacity_score=float(rc) if rc is not None else None,
                smart_wallet_score=sw_score,
                smart_wallet_flag=sw_flag_bool,
                created_at=row.created_at,
                regime_mult=regime_mult,
                equity=equity,
                drawdown=drawdown,
                cost_total_pct=effective_cost_pct,
                trade_cost_bps=trade_cost_bps,
                jito_tip_bps=jito_tip_bps,
                slippage_bps=slippage_bps,
                spread_bps=spread_bps,
                cost_mode=trade_cost_mode,
                ai_risk_score=ai_risk_score if use_ai_risk_overlay else None,
                ai_risk_conf=ai_risk_conf if use_ai_risk_overlay else None,
                ai_risk_mode=ai_risk_mode if use_ai_risk_overlay else None,
                ai_risk_sizer_mult=ai_risk_sizer_mult if use_ai_risk_overlay else 1.0,
                exposure=exposure,
                risk_mult_effective=risk_mult_effective,
                features=feats if log_features else None,
            )
            td = {k: getattr(trade, k) for k in trade.__dataclass_fields__}  # type: ignore[attr-defined]
            td["created_at"] = trade.created_at.isoformat() if trade.created_at else None
            trades_for_log.append(td)

            if _data_logger is not None and hasattr(_data_logger, "record_backtest_trade"):
                try:
                    _data_logger.record_backtest_trade(config=dict(config), trade=td)  # type: ignore[attr-defined]
                except Exception:  # pragma: no cover
                    log.debug("data_logger.record_backtest_trade failed", exc_info=True)

            if _sl_mod is not None and hasattr(_sl_mod, "record_backtest_trade"):
                try:
                    _sl_mod.record_backtest_trade(config=dict(config), trade=td)  # type: ignore[attr-defined]
                except Exception:  # pragma: no cover
                    log.debug("self_learning_ai_strategy.record_backtest_trade failed", exc_info=True)

        # Self-learning tuning
        score_min, blended_gate, risk_mult_override = sl_state.maybe_tune(
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
    stats["engine_name"] = ENGINE_NAME
    stats["engine_version"] = ENGINE_VERSION

    stats["n_candidates"] = n_candidates
    stats["n_trades"] = n_trades
    stats["n_wins"] = n_wins
    stats["n_losses"] = n_losses
    stats["n_flats"] = max(0, n_trades - n_wins - n_losses)
    stats["win_rate"] = (n_wins / n_trades) if n_trades > 0 else 0.0

    # PnL stats
    if pnl_list:
        stats["avg_pnl_pct"] = statistics.mean(pnl_list)
        stats["median_pnl_pct"] = statistics.median(pnl_list)
        stats["pnl_total_pct"] = sum(pnl_list)
        stats["pnl_vol_pct"] = statistics.pstdev(pnl_list) if len(pnl_list) > 1 else 0.0
        stats["pnl_std_error_pct"] = (stats["pnl_vol_pct"] / math.sqrt(len(pnl_list))) if len(pnl_list) > 1 else 0.0
        stats["best_trade_pct"] = max(pnl_list)
        stats["worst_trade_pct"] = min(pnl_list)
    else:
        stats["avg_pnl_pct"] = 0.0
        stats["median_pnl_pct"] = 0.0
        stats["pnl_total_pct"] = 0.0
        stats["pnl_vol_pct"] = 0.0
        stats["pnl_std_error_pct"] = 0.0
        stats["best_trade_pct"] = 0.0
        stats["worst_trade_pct"] = 0.0

    # Equity / drawdown
    stats["net_pnl_pct"] = ((equity - equity_init) / equity_init * 100.0) if equity_init > 0 else 0.0
    stats["equity_final"] = equity
    stats["equity_max"] = hwm
    stats["max_drawdown_pct"] = max_dd * 100.0
    stats["max_runup_pct"] = max_runup * 100.0

    # Equity-return metrics
    stats["sharpe"] = _compute_sharpe(equity_returns)
    stats["avg_equity_return_per_trade_pct"] = (statistics.mean(equity_returns) * 100.0) if equity_returns else 0.0
    stats["equity_vol_pct"] = (statistics.pstdev(equity_returns) * 100.0) if len(equity_returns) > 1 else 0.0

    # Ulcer index + time in DD
    if eq_drawdowns_pct:
        ulcer = math.sqrt(statistics.mean([d * d for d in eq_drawdowns_pct]))
        stats["ulcer_index_pct"] = ulcer
        stats["time_in_drawdown_frac"] = sum(1 for d in eq_drawdowns_pct if d > 0) / float(len(eq_drawdowns_pct))
    else:
        stats["ulcer_index_pct"] = 0.0
        stats["time_in_drawdown_frac"] = 0.0

    stats["ulcer_perf_ratio"] = (float(stats["net_pnl_pct"]) / float(stats["ulcer_index_pct"])) if stats["ulcer_index_pct"] > 0 else 0.0

    # Profit factor / gain-to-pain / omega
    stats["gross_win_pct"] = gross_win_sum
    stats["gross_loss_pct"] = gross_loss_sum
    stats["profit_factor"] = (gross_win_sum / abs(gross_loss_sum)) if gross_loss_sum < 0 else 0.0
    stats["gain_to_pain"] = _gain_to_pain(pnl_list)
    stats["omega_0"] = _omega_ratio(pnl_list, threshold_pct=0.0)

    # Per-trade DD + holds
    stats["avg_dd_trade_pct"] = statistics.mean(dd_list) if dd_list else 0.0
    stats["p95_drawdown_trade_pct"] = _safe_quantile(dd_list, 0.95) if dd_list else 0.0

    stats["avg_hold_sec"] = statistics.mean(hold_list) if hold_list else 0.0
    stats["median_hold_sec"] = statistics.median(hold_list) if hold_list else 0.0
    stats["p95_hold_sec"] = _safe_quantile(hold_list, 0.95) if hold_list else 0.0

    # Exposure / risk mult
    stats["avg_exposure"] = statistics.mean(exposure_list) if exposure_list else 0.0
    stats["p95_exposure"] = _safe_quantile(exposure_list, 0.95) if exposure_list else 0.0
    stats["avg_risk_mult_effective"] = (risk_mult_effective_sum / float(n_trades)) if n_trades > 0 else 1.0

    # Trade cost
    stats["trade_cost_total_pct"] = cost_sum_pct
    stats["trade_cost_avg_pct"] = (cost_sum_pct / float(n_trades)) if n_trades > 0 else 0.0
    stats["trade_cost_bps_cfg"] = float(trade_cost_bps)
    stats["jito_tip_bps_cfg"] = float(jito_tip_bps)
    stats["slippage_bps_cfg"] = float(slippage_bps)
    stats["spread_bps_cfg"] = float(spread_bps)
    stats["trade_cost_mode_cfg"] = trade_cost_mode

    # Weighted research metric
    stats["weighted_avg_pnl_pct"] = (weighted_return_sum / weight_sum) if weight_sum > 0 else float(stats["avg_pnl_pct"])

    # Score diagnostics
    stats["avg_entry_score"] = statistics.mean(blended_scores) if blended_scores else 0.0
    stats["median_entry_score"] = statistics.median(blended_scores) if blended_scores else 0.0
    stats["avg_heur_score"] = statistics.mean(heur_scores) if heur_scores else 0.0
    stats["avg_ai_score"] = statistics.mean(ai_scores) if ai_scores else 0.0

    # Win/loss averages + expectancy
    pos = [p for p in pnl_list if p > 0]
    neg = [p for p in pnl_list if p < 0]
    stats["avg_win_pct"] = statistics.mean(pos) if pos else 0.0
    stats["avg_loss_pct"] = statistics.mean(neg) if neg else 0.0
    stats["pnl_per_trade_pct"] = (float(stats["pnl_total_pct"]) / float(n_trades)) if n_trades > 0 else 0.0
    wr = float(stats["win_rate"])
    stats["expectancy_pct"] = (wr * stats["avg_win_pct"] + (1.0 - wr) * stats["avg_loss_pct"]) if n_trades > 0 else 0.0

    # Span / density
    if first_ts is not None and last_ts is not None and last_ts > first_ts:
        span_days = (last_ts - first_ts).total_seconds() / 86400.0
        stats["days_span"] = max(span_days, 1e-6)
    else:
        stats["days_span"] = 0.0
    if stats["days_span"] > 0:
        stats["daily_pnl_pct"] = float(stats["pnl_total_pct"]) / float(stats["days_span"])
        stats["trade_density_per_day"] = float(n_trades) / float(stats["days_span"])
    else:
        stats["daily_pnl_pct"] = 0.0
        stats["trade_density_per_day"] = 0.0

    # Geometric PnL from equity path (time-aware if we have timestamps)
    if stats["days_span"] > 0 and equity_init > 0 and equity > 0:
        growth = equity / equity_init
        daily_geo = growth ** (1.0 / float(stats["days_span"]))
        stats["daily_pnl_geo_pct"] = (daily_geo - 1.0) * 100.0
        stats["weekly_pnl_geo_pct"] = ((daily_geo ** 7.0) - 1.0) * 100.0
        stats["monthly_pnl_geo_pct"] = ((daily_geo ** 30.0) - 1.0) * 100.0
        stats["annual_pnl_geo_pct"] = ((daily_geo ** 365.0) - 1.0) * 100.0
    else:
        stats["daily_pnl_geo_pct"] = 0.0
        stats["weekly_pnl_geo_pct"] = 0.0
        stats["monthly_pnl_geo_pct"] = 0.0
        stats["annual_pnl_geo_pct"] = 0.0

    # Tail metrics
    if pnl_list:
        sorted_pnl = sorted(pnl_list)
        stats["pnl_p05_pct"] = _safe_quantile(sorted_pnl, 0.05)
        stats["pnl_p95_pct"] = _safe_quantile(sorted_pnl, 0.95)
        tail_n = max(1, int(len(sorted_pnl) * 0.05))
        stats["pnl_cvar_p05_pct"] = statistics.mean(sorted_pnl[:tail_n])
        skew, kurt = _compute_skew_kurtosis(sorted_pnl)
        stats["pnl_skew"] = skew
        stats["pnl_kurtosis_excess"] = kurt
        mw, ml = _compute_consecutive_runs(pnl_list)
        stats["max_consec_wins"] = mw
        stats["max_consec_losses"] = ml
    else:
        stats["pnl_p05_pct"] = 0.0
        stats["pnl_p95_pct"] = 0.0
        stats["pnl_cvar_p05_pct"] = 0.0
        stats["pnl_skew"] = 0.0
        stats["pnl_kurtosis_excess"] = 0.0
        stats["max_consec_wins"] = 0
        stats["max_consec_losses"] = 0

    # Sortino (pct basis)
    downside = _compute_downside_dev(pnl_list)
    stats["downside_dev_pct"] = downside
    stats["sortino"] = (float(stats["avg_pnl_pct"]) / downside) if downside > 1e-12 else 0.0

    # DD ratio / calmar-like
    dd_abs = abs(float(stats.get("max_drawdown_pct", 0.0) or 0.0))
    net_pnl_pct = float(stats["net_pnl_pct"])
    stats["pnl_dd_ratio"] = (net_pnl_pct / dd_abs) if dd_abs > 1e-12 else 0.0
    stats["calmar_ratio"] = stats["pnl_dd_ratio"]

    # Win/loss ratio
    stats["win_loss_ratio"] = (
        (float(stats["avg_win_pct"]) / abs(float(stats["avg_loss_pct"])))
        if float(stats["avg_loss_pct"]) < 0
        else 0.0
    )

    # Kelly estimate
    avg_win = float(stats["avg_win_pct"])
    avg_loss = float(stats["avg_loss_pct"])
    if 0.0 < wr < 1.0 and avg_win > 0 and avg_loss < 0:
        b = avg_win / abs(avg_loss)
        q = 1.0 - wr
        kelly = (b * wr - q) / b
        kelly = max(0.0, kelly)
        stats["kelly_fraction_est"] = kelly
        stats["kelly_fraction_capped"] = min(kelly, 0.5)
    else:
        stats["kelly_fraction_est"] = 0.0
        stats["kelly_fraction_capped"] = 0.0

    # Rolling sharpes
    stats["rolling_sharpe_50"] = _compute_sharpe(equity_returns[-50:]) if len(equity_returns) >= 2 else 0.0
    stats["rolling_sharpe_100"] = _compute_sharpe(equity_returns[-100:]) if len(equity_returns) >= 2 else 0.0

    # Label stats
    stats["label_counts"] = dict(label_counts)
    stats["label_pnl_sum"] = dict(label_pnl_sum)
    label_avg: Dict[str, float] = {}
    label_wr: Dict[str, float] = {}
    label_lr: Dict[str, float] = {}
    for lbl, cnt in label_counts.items():
        if cnt > 0:
            label_avg[lbl] = label_pnl_sum.get(lbl, 0.0) / float(cnt)
            label_wr[lbl] = label_win_counts.get(lbl, 0) / float(cnt)
            label_lr[lbl] = label_loss_counts.get(lbl, 0) / float(cnt)
    stats["label_avg_pnl_pct"] = label_avg
    stats["label_win_rate"] = label_wr
    stats["label_loss_rate"] = label_lr

    # Bucket stats outputs
    stats["rc_bucket_stats"] = {
        name: {
            "n_trades": agg["count"],
            "avg_pnl_pct": (agg["pnl_sum"] / float(agg["count"])) if agg["count"] > 0 else 0.0,
            "win_rate": (agg["win_count"] / float(agg["count"])) if agg["count"] > 0 else 0.0,
            "avg_rc": (agg["rc_sum"] / float(agg["count"])) if agg["count"] > 0 else 0.0,
        }
        for name, agg in rc_bucket_acc.items()
        if agg.get("count", 0) > 0
    }

    stats["score_bucket_stats"] = {
        name: {
            "n_trades": agg["count"],
            "avg_pnl_pct": (agg["pnl_sum"] / float(agg["count"])) if agg["count"] > 0 else 0.0,
            "win_rate": (agg["win_count"] / float(agg["count"])) if agg["count"] > 0 else 0.0,
        }
        for name, agg in score_bucket_acc.items()
        if agg.get("count", 0) > 0
    }

    stats["regime_stats"] = {
        key: {
            "n_trades": agg["count"],
            "avg_pnl_pct": (agg["pnl_sum"] / float(agg["count"])) if agg["count"] > 0 else 0.0,
            "win_rate": (agg["win_count"] / float(agg["count"])) if agg["count"] > 0 else 0.0,
            "loss_rate": (agg["loss_count"] / float(agg["count"])) if agg["count"] > 0 else 0.0,
        }
        for key, agg in regime_bucket_acc.items()
        if agg.get("count", 0) > 0
    }

    stats["hour_of_day_stats"] = {
        str(hour): {
            "n_trades": agg["count"],
            "avg_pnl_pct": (agg["pnl_sum"] / float(agg["count"])) if agg["count"] > 0 else 0.0,
            "win_rate": (agg["win_count"] / float(agg["count"])) if agg["count"] > 0 else 0.0,
            "loss_rate": (agg["loss_count"] / float(agg["count"])) if agg["count"] > 0 else 0.0,
        }
        for hour, agg in sorted(hour_bucket_acc.items())
        if agg.get("count", 0) > 0
    }

    # Self-learning
    stats["self_learning_tune_count"] = int(sl_state.tune_count)
    stats["self_learning_use_llm"] = bool(sl_state.use_llm)

    # Reinvestor
    stats["reinvest_count"] = int(reinvest_count)
    stats["reinvest_pnl_pct"] = float(reinvest_pnl_pct)
    stats["reinvest_enabled"] = bool(enable_reinvestor)
    stats["reinvest_percent"] = float(reinvest_percent)
    stats["reinvest_min_profit_sol"] = float(reinvest_min_profit_sol)
    stats["reinvest_dd_soft_stop_pct"] = float(reinvest_dd_soft_stop_pct)
    stats["reinvest_dd_hard_stop_pct"] = float(reinvest_dd_hard_stop_pct)
    stats["net_pnl_with_reinvest_pct"] = float(stats["net_pnl_pct"]) + float(reinvest_pnl_pct)

    # AI risk overlay diagnostics
    stats["ai_risk_overlay_enabled"] = bool(use_ai_risk_overlay)
    stats["ai_risk_max_score"] = float(ai_risk_max_score)
    stats["ai_risk_min_conf"] = float(ai_risk_min_conf)
    stats["ai_risk_mode"] = ai_risk_mode
    stats["ai_risk_reject_count"] = int(ai_risk_rejects)
    stats["ai_risk_reject_rate"] = (ai_risk_rejects / float(n_candidates)) if n_candidates > 0 else 0.0
    stats["ai_risk_score_avg"] = statistics.mean(ai_risk_scores_used) if ai_risk_scores_used else 0.0
    stats["ai_risk_score_med"] = statistics.median(ai_risk_scores_used) if ai_risk_scores_used else 0.0
    stats["ai_risk_scores_used_count"] = len(ai_risk_scores_used)

    # Final gates after self-learning
    stats["score_min_initial"] = float(score_min_initial)
    stats["score_min_final"] = float(score_min)
    stats["entry_min_score_initial"] = float(blended_gate_initial)
    stats["entry_min_score_final"] = float(blended_gate)

    # Config echo
    stats["risk_profile"] = risk_profile
    stats["risk_profile_mult"] = float(risk_profile_mult)
    stats["risk_mult_override_final"] = float(risk_mult_override)
    stats["equity_init"] = float(equity_init)
    stats["equity_cap_mult"] = equity_cap_mult
    stats["alloc_mode"] = alloc_mode
    stats["use_live_risk_manager"] = bool(use_live_rm)
    stats["position_fraction"] = float(position_fraction_f)
    stats["simulate_overlap"] = bool(simulate_overlap)
    stats["result_segment"] = result_segment
    stats["dedup_mode"] = dedup_mode
    stats["dedup_window_sec"] = dedup_window_sec

    # Target metric helper for sweeps
    target_metric_key_raw = config.get("BT_TARGET_METRIC") or config.get("target_metric") or ""
    target_metric_key = str(target_metric_key_raw).strip() or None
    if not target_metric_key:
        for cand in ("annual_pnl_geo_pct", "net_pnl_with_reinvest_pct", "net_pnl_pct", "sharpe"):
            if isinstance(stats.get(cand), (int, float)):
                target_metric_key = cand
                break
    if target_metric_key is not None and isinstance(stats.get(target_metric_key), (int, float)):
        stats["target_metric_key"] = target_metric_key
        stats["target_metric_value"] = float(stats[target_metric_key])
    else:
        stats["target_metric_key"] = target_metric_key
        stats["target_metric_value"] = 0.0

    if log_trades:
        stats["trades"] = trades_for_log
        stats["trades_logged"] = len(trades_for_log)
        stats["trades_log_capped"] = bool(max_log_trades > 0 and len(trades_for_log) >= max_log_trades)
    if log_equity_series:
        stats["equity_series"] = equity_series
        stats["equity_points_logged"] = len(equity_series)
        stats["equity_log_capped"] = bool(max_log_equity > 0 and len(equity_series) >= max_log_equity)

    # Optional research logging hook
    if _data_logger is not None:
        try:
            if hasattr(_data_logger, "record_backtest_run"):
                _data_logger.record_backtest_run(config=dict(config), stats=stats)  # type: ignore[attr-defined]
            elif hasattr(_data_logger, "log_backtest_run"):
                _data_logger.log_backtest_run(config=dict(config), stats=stats)  # type: ignore[attr-defined]
        except Exception:  # pragma: no cover
            log.debug("data_logger backtest hook failed", exc_info=True)

    if _sl_mod is not None and hasattr(_sl_mod, "record_backtest_summary"):
        try:
            _sl_mod.record_backtest_summary(config=dict(config), stats=stats)  # type: ignore[attr-defined]
        except Exception:  # pragma: no cover
            log.debug("self_learning_ai_strategy.record_backtest_summary failed", exc_info=True)

    # Optional summary hooks for risk_manager / reinvestor (whole-run awareness)
    if _rm is not None and hasattr(_rm, "backtest_on_summary"):
        try:
            _rm.backtest_on_summary(config=dict(config), stats=stats)  # type: ignore[attr-defined]
        except Exception:  # pragma: no cover
            log.debug("risk_manager.backtest_on_summary failed", exc_info=True)

    if _reinv is not None and hasattr(_reinv, "backtest_on_summary"):
        try:
            _reinv.backtest_on_summary(config=dict(config), stats=stats)  # type: ignore[attr-defined]
        except Exception:  # pragma: no cover
            log.debug("reinvestor.backtest_on_summary failed", exc_info=True)

    log.info(
        "backtest_engine: finished run (tag=%s, n_trades=%d, net_pnl=%.2f%%, max_dd=%.2f%%, sharpe=%.3f, "
        "kelly=%.3f, target=%s: %.3f)",
        tag or "<none>",
        n_trades,
        float(stats["net_pnl_pct"]),
        float(stats["max_drawdown_pct"]),
        float(stats["sharpe"]),
        float(stats["kelly_fraction_est"]),
        stats.get("target_metric_key") or "<none>",
        float(stats.get("target_metric_value") or 0.0),
    )

    return stats
