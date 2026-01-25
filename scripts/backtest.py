from __future__ import annotations

"""
scripts/backtest.py — God-Tier Backtest Runner (LOCK-IN FINAL)
==============================================================

Role
----
Grid-sweep orchestrator for utils_pkg.backtest_engine.run_backtest(...), plus optional
"fixtures replay" mode (tests/fixtures/*).

Hard requirements (guaranteed)
------------------------------
- Works whether run_backtest is sync or async.
- Works whether run_backtest accepts:
    (a) run_backtest(cfg_dict)
    (b) run_backtest(**kwargs)
- Never touches live trading state:
    * sets BACKTEST_MODE=1 (default)
    * sets LIVE_TRADING=0 (default)
    * does NOT write Redis / Postgres unless you explicitly opt-in via exports / ai_db flags

Quality guarantees
------------------
- Repo bootstrap (root + optional src/ added to sys.path)
- Deterministic tags + stable hashing
- Resume by prior export-json (skip existing tags)
- Parallel execution with concurrency cap
- Per-config timeout + total run timeout
- Atomic JSON/CSV exports + checkpointing
- Drift-tolerant stats normalization + derived metrics
- Optional ai_db integration (never hard-fails sweep unless --require-db)

Fixtures replay (deterministic pipeline replay)
-----------------------------------------------
- Discovers fixture files under tests/fixtures by default.
- Passes fixture info into cfg dict using several common keys so your engine can pick it up:
    * BT_FIXTURES_DIR
    * fixtures_dir
    * fixtures
    * fixture_paths
    * fixture_glob
    * bt_input_mode = "fixtures_replay"
- If your engine ignores these keys, nothing breaks (drift-tolerant).

Quick checks
------------
  py -m py_compile scripts\\backtest.py
  py scripts\\backtest.py --dry-run
  py scripts\\backtest.py --fixtures-mode on --dry-run
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
import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any


# =============================================================================
# Repo bootstrap (root + src/)
# =============================================================================
def _bootstrap_repo_root() -> str:
    """
    Make running from anywhere work:
      - ensure repo root is on sys.path
      - ensure repo root /src is on sys.path (if present)
      - set REPO_ROOT env var
      - set safe defaults for backtest isolation
    """
    try:
        here = Path(__file__).resolve()
    except Exception:
        return ""

    markers = ("pyproject.toml", "requirements.txt", "setup.cfg", ".git")
    root: Path | None = None

    for p in (here.parent, *tuple(here.parents)):
        try:
            if any((p / m).exists() for m in markers):
                root = p
                break
            if (p / "src").is_dir() and any(
                (p / "src" / x).exists() for x in ("utils_pkg", "scanner", "scripts")
            ):
                root = p
                break
        except Exception:
            continue

    if root is None:
        root = here.parents[1] if len(here.parents) > 1 else here.parent

    root_str = str(root)
    if root_str and root_str not in sys.path:
        sys.path.insert(0, root_str)

    src = root / "src"
    if src.is_dir():
        src_str = str(src)
        if src_str not in sys.path:
            sys.path.insert(0, src_str)

    os.environ.setdefault("REPO_ROOT", root_str)

    # Safety / isolation defaults (engine may ignore; harmless if unused)
    os.environ.setdefault("BACKTEST_MODE", "1")
    os.environ.setdefault("LIVE_TRADING", "0")
    os.environ.setdefault("DISABLE_LIVE_TRADING", "1")
    os.environ.setdefault("BT_NO_SIDE_EFFECTS", "1")
    return root_str


REPO_ROOT = _bootstrap_repo_root()


# =============================================================================
# Logging (fallback-safe)
# =============================================================================
def _get_logger():
    try:
        from logging_setup import get_logger  # type: ignore

        try:
            return get_logger(__name__)
        except TypeError:
            return get_logger()
    except Exception:
        import logging

        logging.basicConfig(
            level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
        )
        return logging.getLogger("backtest")


log = _get_logger()


# =============================================================================
# Imports (engine is required)
# =============================================================================
try:
    from utils_pkg.backtest_engine import run_backtest  # type: ignore
except Exception as e:  # pragma: no cover
    raise SystemExit(
        f"[backtest] Failed to import utils_pkg.backtest_engine.run_backtest: {e}"
    ) from e

# ai_db optional
try:
    from utils_pkg import ai_db  # type: ignore
except Exception:  # pragma: no cover
    ai_db = None  # type: ignore


# =============================================================================
# Env helpers
# =============================================================================
_TRUE = {"1", "true", "yes", "y", "on", "t"}
_FALSE = {"0", "false", "no", "n", "off", "f"}


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
    s = str(raw).strip().lower()
    if s in _TRUE:
        return True
    if s in _FALSE:
        return False
    return default


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


def _csv_tokens(s: str) -> list[str]:
    out: list[str] = []
    for part in (s or "").replace(";", ",").split(","):
        p = part.strip()
        if p:
            out.append(p)
    return out


def _expand_range_token(token: str, *, as_int: bool) -> list[float]:
    """
    Expand tokens of the form:
      - "a..b" or "a:b" optionally with ":step" => "a..b:step"
    Examples:
      - "0.5..0.8:0.05"
      - "1:5" (ints)
    """
    m = _RANGE_RE.match(token)
    if not m:
        return []

    a = float(m.group("a"))
    b = float(m.group("b"))
    step_raw = m.group("step")

    step = float(step_raw) if step_raw and str(step_raw).strip() else (1.0 if as_int else 0.01)
    if step == 0:
        return []

    # normalize direction
    if a < b and step < 0:
        step = abs(step)
    if a > b and step > 0:
        step = -abs(step)

    out: list[float] = []
    cur = a
    max_iter = 250_000

    def done(x: float) -> bool:
        return x > b + 1e-12 if step > 0 else x < b - 1e-12

    it = 0
    while not done(cur) and it < max_iter:
        out.append(cur)
        cur += step
        it += 1

    # include end exactly (best effort)
    if out and abs(out[-1] - b) > 1e-9:
        out.append(b)
    if not out:
        out = [a, b]
    return out


def _parse_floats(s: str) -> list[float]:
    out: list[float] = []
    for tok in _csv_tokens(s):
        expanded = _expand_range_token(tok, as_int=False)
        if expanded:
            out.extend([float(v) for v in expanded if math.isfinite(v)])
            continue
        try:
            v = float(tok)
            if math.isfinite(v):
                out.append(v)
        except Exception:
            continue
    return out


def _parse_ints(s: str) -> list[int]:
    out: list[int] = []
    for tok in _csv_tokens(s):
        expanded = _expand_range_token(tok, as_int=True)
        if expanded:
            out.extend([int(round(v)) for v in expanded if math.isfinite(v)])
            continue
        try:
            out.append(int(tok))
        except Exception:
            continue
    return out


def _parse_strs(s: str) -> list[str]:
    return _csv_tokens(s)


def _parse_date(s: str) -> datetime | None:
    s = (s or "").strip()
    if not s:
        return None
    try:
        if "T" in s:
            return datetime.fromisoformat(s)
        return datetime.strptime(s, "%Y-%m-%d")
    except Exception:
        return None


# =============================================================================
# Atomic IO
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
    _atomic_write_text(path, json.dumps(payload, ensure_ascii=False, indent=2, default=str) + "\n")


def _read_json(path: str) -> dict[str, Any] | None:
    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else None
    except Exception:
        return None


# =============================================================================
# CLI helpers
# =============================================================================
def _add_bool_flag(
    p: argparse.ArgumentParser, flag: str, env_key: str, default: bool | None, help_text: str
) -> None:
    """
    Tri-state convenience:
      - if env exists -> env value becomes default
      - else -> provided default
    Uses BooleanOptionalAction => supports --flag / --no-flag.
    """
    dest = flag.replace("-", "_")
    eff_default = _env_bool(env_key, False) if _env_present(env_key) else default
    p.add_argument(
        f"--{flag}",
        dest=dest,
        default=eff_default,
        action=argparse.BooleanOptionalAction,
        help=help_text,
    )


# =============================================================================
# BacktestConfig
# =============================================================================
@dataclass(frozen=True)
class BacktestConfig:
    # core sweep knobs
    ENTRY_SCORE_MIN: float
    max_positions: int
    days_back: int

    # time bounds
    start_ts: datetime | None = None
    end_ts: datetime | None = None

    # optional filters
    regime_whitelist: tuple[str, ...] | None = None
    label_include: tuple[str, ...] | None = None
    label_exclude: tuple[str, ...] | None = None
    limit_candidates: int | None = None

    # risk/regime
    risk_mult_override: float | None = None
    regime_risk_mode_lock: int | None = None  # 0/1/2

    # strategy/ai
    entry_min_score: float | None = None
    entry_ai_weight: float | None = None
    entry_model_name: str | None = None
    risk_profile: str | None = None
    exit_policy_name: str | None = None
    require_risk_capacity: bool | None = None  # tri-state

    # misc modes
    feature_mode: str | None = None
    smart_wallet_mode: str | None = None
    engine_mode: str | None = None
    result_segment: str | None = None

    # sim knobs
    trade_cost_bps: float | None = None
    jito_tip_bps: float | None = None
    base_size_sol: float | None = None
    max_hold_sec: int | None = None

    # experiment/meta filters
    policy_version: str | None = None
    experiment_tag: str | None = None
    src_filter: str | None = None
    min_feature_version: int | None = None
    wallet_tag_filter: str | None = None
    session_id_filter: str | None = None

    # toggles
    use_ai_risk_overlay: bool | None = None
    enable_self_learning: bool | None = None
    enable_reinvestor: bool | None = None

    # fixtures replay (optional)
    fixtures_mode: str | None = None  # "on" | "off"
    fixtures_dir: str | None = None
    fixture_glob: str | None = None
    fixture_paths: tuple[str, ...] | None = None
    fixture_limit: int | None = None

    # tag/meta
    tag: str = ""
    group_id: str | None = None
    run_note: str | None = None
    extra: dict[str, Any] | None = None

    def to_run_kwargs(self) -> dict[str, Any]:
        """
        Drift-tolerant config dict for backtest_engine.
        Unknown keys are OK; engine can ignore them.
        """
        cfg: dict[str, Any] = {
            "ENTRY_SCORE_MIN": float(self.ENTRY_SCORE_MIN),
            "max_positions": int(self.max_positions),
            "days_back": int(self.days_back),
            "config_tag": str(self.tag or ""),
            # safety knobs (engine may ignore)
            "BACKTEST_MODE": True,
            "LIVE_TRADING": False,
        }

        # time bounds
        if self.start_ts is not None:
            cfg["start_ts"] = self.start_ts.isoformat()
        if self.end_ts is not None:
            cfg["end_ts"] = self.end_ts.isoformat()

        # label/regime filters
        if self.regime_whitelist:
            cfg["regime_whitelist"] = list(self.regime_whitelist)
        if self.label_include:
            cfg["label_include"] = list(self.label_include)
        if self.label_exclude:
            cfg["label_exclude"] = list(self.label_exclude)
        if self.limit_candidates is not None:
            cfg["limit_candidates"] = int(self.limit_candidates)

        # risk/regime
        if self.risk_mult_override is not None:
            cfg["risk_mult_override"] = float(self.risk_mult_override)
        if self.regime_risk_mode_lock is not None:
            cfg["regime_risk_mode_lock"] = int(self.regime_risk_mode_lock)

        # strategy/ai
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

        # modes
        if self.feature_mode:
            cfg["BT_FEATURE_MODE"] = self.feature_mode
        if self.smart_wallet_mode:
            cfg["BT_SMART_WALLET_MODE"] = self.smart_wallet_mode
        if self.engine_mode:
            cfg["BT_ENGINE_MODE"] = self.engine_mode
        if self.result_segment:
            cfg["BT_RESULT_SEGMENT"] = self.result_segment

        # costs / sim knobs
        if self.trade_cost_bps is not None:
            cfg["BT_TRADE_COST_BPS"] = float(self.trade_cost_bps)
        if self.jito_tip_bps is not None:
            cfg["BT_JITO_TIP_BPS"] = float(self.jito_tip_bps)
        if self.base_size_sol is not None:
            cfg["BT_BASE_SIZE_SOL"] = float(self.base_size_sol)
        if self.max_hold_sec is not None:
            cfg["BT_MAX_HOLD_SEC"] = int(self.max_hold_sec)

        # experiment/meta filters
        if self.policy_version:
            cfg["ai_policy_version"] = self.policy_version
        if self.experiment_tag:
            cfg["ai_experiment_tag"] = self.experiment_tag
        if self.src_filter:
            cfg["BT_SRC"] = self.src_filter
        if self.min_feature_version is not None:
            cfg["BT_MIN_FEATURE_VERSION"] = int(self.min_feature_version)
        if self.wallet_tag_filter:
            cfg["BT_WALLET_TAG"] = self.wallet_tag_filter
        if self.session_id_filter:
            cfg["BT_SESSION_ID"] = self.session_id_filter

        # toggles
        if self.use_ai_risk_overlay is not None:
            cfg["BT_USE_AI_RISK_MODEL"] = bool(self.use_ai_risk_overlay)
        if self.enable_self_learning is not None:
            cfg["BT_ENABLE_SELF_LEARNING"] = bool(self.enable_self_learning)
        if self.enable_reinvestor is not None:
            cfg["BT_ENABLE_REINVESTOR"] = bool(self.enable_reinvestor)

        # fixtures replay keys (engine may ignore; safe)
        if self.fixtures_mode:
            cfg["bt_input_mode"] = "fixtures_replay" if self.fixtures_mode == "on" else "default"
        if self.fixtures_dir:
            cfg["fixtures_dir"] = self.fixtures_dir
            cfg["BT_FIXTURES_DIR"] = self.fixtures_dir
        if self.fixture_glob:
            cfg["fixture_glob"] = self.fixture_glob
        if self.fixture_paths:
            cfg["fixtures"] = list(self.fixture_paths)
            cfg["fixture_paths"] = list(self.fixture_paths)
        if self.fixture_limit is not None:
            cfg["fixture_limit"] = int(self.fixture_limit)

        if self.group_id:
            cfg["BT_GROUP_ID"] = self.group_id
        if self.run_note:
            cfg.setdefault("_run_meta", {})
            cfg["_run_meta"]["note"] = self.run_note

        if self.extra:
            cfg.update(self.extra)

        return cfg


# =============================================================================
# Tagging & fingerprinting
# =============================================================================
def _sanitize_tag(tag: str, max_len: int = 220) -> str:
    tag = re.sub(r"[^A-Za-z0-9._-]+", "_", tag)
    tag = re.sub(r"_+", "_", tag).strip("_")
    if len(tag) <= max_len:
        return tag
    h = hashlib.md5(tag.encode("utf-8")).hexdigest()[:10]
    return tag[: max_len - 11] + "_" + h


def _fingerprint(cfg_dict: Mapping[str, Any]) -> str:
    raw = json.dumps(cfg_dict, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]


# =============================================================================
# Sync/async invocation with signature drift tolerance
# =============================================================================
async def _invoke_any(fn: Any, *args: Any, **kwargs: Any) -> Any:
    """
    Invoke sync or async function. If sync, run in thread.
    If it returns an awaitable, await it.
    """
    if inspect.iscoroutinefunction(fn):
        return await fn(*args, **kwargs)
    out = await asyncio.to_thread(fn, *args, **kwargs)
    if inspect.isawaitable(out):
        return await out
    return out


def _filter_kwargs_for(fn: Any, kwargs: dict[str, Any]) -> dict[str, Any]:
    """
    If fn doesn't accept **kwargs, only pass allowed params.
    """
    try:
        sig = inspect.signature(fn)
        params = list(sig.parameters.values())
        if any(p.kind == inspect.Parameter.VAR_KEYWORD for p in params):
            return kwargs
        allowed = {p.name for p in params}
        return {k: v for k, v in kwargs.items() if k in allowed}
    except Exception:
        return kwargs


async def _call_run_backtest(cfg_kwargs: dict[str, Any]) -> Any:
    """
    Supports both:
      - run_backtest(cfg_dict)
      - run_backtest(**kwargs)
    and sync or async implementations.
    """
    fn = run_backtest
    try:
        sig = inspect.signature(fn)
        params = list(sig.parameters.values())
    except Exception:
        sig = None
        params = []

    if sig is not None:
        has_var_kw = any(p.kind == inspect.Parameter.VAR_KEYWORD for p in params)
        if has_var_kw:
            return await _invoke_any(fn, **cfg_kwargs)

        positional = [
            p
            for p in params
            if p.kind
            in (inspect.Parameter.POSITIONAL_ONLY, inspect.Parameter.POSITIONAL_OR_KEYWORD)
        ]
        if len(positional) == 1 and len(params) == 1:
            return await _invoke_any(fn, cfg_kwargs)

        filtered = _filter_kwargs_for(fn, dict(cfg_kwargs))
        try:
            return await _invoke_any(fn, **filtered)
        except TypeError:
            return await _invoke_any(fn, cfg_kwargs)

    try:
        return await _invoke_any(fn, **cfg_kwargs)
    except TypeError:
        return await _invoke_any(fn, cfg_kwargs)


# =============================================================================
# Stats normalization + derived metrics
# =============================================================================
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


def _augment_stats(stats: dict[str, Any]) -> None:
    """
    Add derived/alias metrics so sorting & exports remain stable across engine versions.
    """
    # win_rate
    if "win_rate" not in stats:
        n = _safe_float(stats.get("n_trades"), 0.0)
        w = _safe_float(stats.get("n_wins"), 0.0)
        if n > 0:
            stats["win_rate"] = w / n

    # net_pnl_pct alias
    if "net_pnl_pct" not in stats:
        for k in ("pnl_total_pct", "net_return_pct", "total_return_pct"):
            if isinstance(stats.get(k), int | float):
                stats["net_pnl_pct"] = float(stats[k])
                break

    # pnl_per_trade_pct
    if "pnl_per_trade_pct" not in stats and "pnl_total_pct" in stats and "n_trades" in stats:
        n = _safe_float(stats.get("n_trades"), 0.0)
        if n > 0:
            stats["pnl_per_trade_pct"] = _safe_float(stats.get("pnl_total_pct"), 0.0) / n

    # expectancy_pct (if avg_win/avg_loss present)
    if "expectancy_pct" not in stats:
        wr = _safe_float(stats.get("win_rate"), 0.0)
        aw = stats.get("avg_win_pct")
        al = stats.get("avg_loss_pct")
        if isinstance(aw, int | float) and isinstance(al, int | float):
            stats["expectancy_pct"] = wr * float(aw) + (1.0 - wr) * float(al)

    # daily/monthly projections
    days = None
    for k in ("days_span", "days", "window_days"):
        v = stats.get(k)
        if isinstance(v, int | float) and float(v) > 0:
            days = float(v)
            break

    if days:
        if "daily_pnl_pct" not in stats:
            stats["daily_pnl_pct"] = _safe_float(stats.get("pnl_total_pct"), 0.0) / days
        if "trades_per_day" not in stats:
            stats["trades_per_day"] = _safe_float(stats.get("n_trades"), 0.0) / days
        if "monthly_pnl_pct" not in stats and "daily_pnl_pct" in stats:
            stats["monthly_pnl_pct"] = _safe_float(stats.get("daily_pnl_pct"), 0.0) * 30.0
        if "monthly_pnl_compound_pct" not in stats and "daily_pnl_pct" in stats:
            d = _safe_float(stats.get("daily_pnl_pct"), 0.0)
            gross = 1.0 + d / 100.0
            if gross > 0:
                stats["monthly_pnl_compound_pct"] = ((gross**30.0) - 1.0) * 100.0

    # edge_score + pnl/dd + calmar + (safe) kelly
    npct = _safe_float(stats.get("net_pnl_pct", stats.get("pnl_total_pct", 0.0)), 0.0)
    dd = abs(_safe_float(stats.get("max_drawdown_pct"), 0.0))
    sh = _safe_float(stats.get("sharpe"), 0.0)

    if dd > 0 and "pnl_dd_ratio" not in stats:
        stats["pnl_dd_ratio"] = npct / dd
    if dd > 0 and "calmar_ratio" not in stats:
        stats["calmar_ratio"] = npct / dd
    if "edge_score" not in stats:
        stats["edge_score"] = (npct / (1.0 + dd)) * (1.0 + 0.25 * sh)

    if "kelly_fraction" not in stats:
        wr = _safe_float(stats.get("win_rate"), 0.0)
        aw = stats.get("avg_win_pct")
        al = stats.get("avg_loss_pct")
        if isinstance(aw, int | float) and isinstance(al, int | float) and float(al) != 0:
            b = float(aw) / abs(float(al))
            q = 1.0 - wr
            if b > 0:
                k = (b * wr - q) / b
                stats["kelly_fraction"] = k
                stats["kelly_safe_fraction"] = max(0.0, k * 0.5)


# =============================================================================
# Resume tags
# =============================================================================
def _load_resume_tags(path: str) -> set[str]:
    tags: set[str] = set()
    if not path or not os.path.exists(path):
        return tags
    payload = _read_json(path) or {}
    results = payload.get("results", [])
    if isinstance(results, list):
        for r in results:
            if not isinstance(r, dict):
                continue
            cfg = r.get("config", {})
            if isinstance(cfg, dict):
                t = cfg.get("config_tag") or cfg.get("tag")
                if isinstance(t, str) and t:
                    tags.add(t)
    return tags


# =============================================================================
# Fixtures discovery (tests/fixtures/*)
# =============================================================================
def _discover_fixtures(
    fixtures_dir: str,
    glob_expr: str,
    *,
    explicit: Sequence[str] | None = None,
    limit: int | None = None,
    shuffle: bool = False,
    seed: int | None = None,
) -> list[str]:
    """
    Deterministically list fixture file paths (sorted by path by default).
    """
    paths: list[Path] = []

    if explicit:
        for x in explicit:
            p = Path(x).expanduser()
            if not p.is_absolute():
                p = (Path(REPO_ROOT) / p).resolve()
            if p.exists() and p.is_file():
                paths.append(p)
        out = [str(p) for p in paths]
        if limit and limit > 0:
            out = out[:limit]
        return out

    d = Path(fixtures_dir).expanduser()
    if not d.is_absolute():
        d = (Path(REPO_ROOT) / d).resolve()
    if not d.exists() or not d.is_dir():
        return []

    # allow comma-separated globs
    globs = [g.strip() for g in (glob_expr or "").split(",") if g.strip()] or [
        "*.jsonl",
        "*.json",
        "*.csv",
    ]
    for g in globs:
        try:
            paths.extend(sorted(d.glob(g)))
        except Exception:
            continue

    # de-dupe + stable sort
    uniq = sorted({p.resolve() for p in paths if p.is_file()}, key=lambda p: str(p))
    out = [str(p) for p in uniq]

    if shuffle:
        rng = random.Random(seed if seed is not None else 0)
        rng.shuffle(out)

    if limit and limit > 0:
        out = out[:limit]
    return out


# =============================================================================
# CLI
# =============================================================================
def build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="God-Tier backtest runner (grid sweep).")

    # core sweeps
    p.add_argument("--score-min", default=_env_str("BT_SCORE_MIN_LIST", "0.55,0.60,0.65"))
    p.add_argument("--max-positions", default=_env_str("BT_MAX_POS_LIST", "1,2"))
    p.add_argument("--days-back", type=int, default=_env_int("BT_DAYS_BACK", 14))
    p.add_argument("--start-date", default=_env_str("BT_START_DATE", ""))
    p.add_argument("--end-date", default=_env_str("BT_END_DATE", ""))

    # risk/regime sweeps
    p.add_argument("--risk-mult", default=_env_str("BT_RISK_MULT_LIST", ""))
    p.add_argument("--regime-risk-mode", default=_env_str("BT_REGIME_RISK_MODE_LIST", ""))

    # strategy/ai sweeps
    p.add_argument("--entry-min-score", default=_env_str("BT_ENTRY_MIN_SCORE_LIST", ""))
    p.add_argument("--entry-ai-weight", default=_env_str("BT_ENTRY_AI_WEIGHT_LIST", ""))
    p.add_argument("--entry-model", default=_env_str("BT_ENTRY_MODEL_NAME", ""))
    p.add_argument("--risk-profile", default=_env_str("BT_RISK_PROFILE_LIST", ""))
    p.add_argument("--exit-policy", default=_env_str("BT_EXIT_POLICY_LIST", ""))

    # filters
    p.add_argument("--regime", default=_env_str("BT_REGIME_WHITELIST", ""))
    p.add_argument("--label-include", default=_env_str("BT_LABEL_INCLUDE", ""))
    p.add_argument("--label-exclude", default=_env_str("BT_LABEL_EXCLUDE", ""))
    p.add_argument("--limit-candidates", type=int, default=_env_int("BT_LIMIT_CANDIDATES", 0))

    # modes
    p.add_argument("--feature-mode", default=_env_str("BT_FEATURE_MODE", ""))
    p.add_argument("--smart-wallet-mode", default=_env_str("BT_SMART_WALLET_MODE", ""))
    p.add_argument("--engine-mode", default=_env_str("BT_ENGINE_MODE", ""))
    p.add_argument("--segment", default=_env_str("BT_RESULT_SEGMENT", ""))

    # meta / slicing
    p.add_argument("--policy-version", default=_env_str("BT_POLICY_VERSION", ""))
    p.add_argument("--experiment-tag", default=_env_str("BT_EXPERIMENT_TAG", ""))
    p.add_argument("--src", default=_env_str("BT_SRC", ""))
    p.add_argument("--min-feature-version", type=int, default=_env_int("BT_MIN_FEATURE_VERSION", 0))
    p.add_argument("--wallet-tag", default=_env_str("BT_WALLET_TAG", ""))
    p.add_argument("--session-id", default=_env_str("BT_SESSION_ID", ""))

    # costs
    p.add_argument("--trade-cost-bps", type=float, default=_env_float("BT_TRADE_COST_BPS", 0.0))
    p.add_argument("--jito-tip-bps", type=float, default=_env_float("BT_JITO_TIP_BPS", 0.0))
    p.add_argument("--base-size-sol", type=float, default=_env_float("BT_BASE_SIZE_SOL", 0.0))
    p.add_argument("--max-hold-sec", type=int, default=_env_int("BT_MAX_HOLD_SEC", 0))

    # toggles
    _add_bool_flag(
        p,
        "use-ai-risk-overlay",
        "BT_USE_AI_RISK_MODEL",
        None,
        "Enable ai_risk_model overlay (if supported)",
    )
    _add_bool_flag(
        p,
        "enable-self-learning",
        "BT_ENABLE_SELF_LEARNING",
        None,
        "Enable self-learning simulation (if supported)",
    )
    _add_bool_flag(
        p,
        "enable-reinvestor",
        "BT_ENABLE_REINVESTOR",
        None,
        "Enable reinvestor simulation (if supported)",
    )
    _add_bool_flag(
        p,
        "require-risk-capacity",
        "BT_REQUIRE_RISK_CAPACITY",
        None,
        "Set ENTRY_REQUIRE_RISK_CAPACITY (tri-state)",
    )

    # fixtures replay
    p.add_argument("--fixtures-mode", default=_env_str("BT_FIXTURES_MODE", "off"), help="off|on")
    p.add_argument("--fixtures-dir", default=_env_str("BT_FIXTURES_DIR", "tests/fixtures"))
    p.add_argument("--fixtures-glob", default=_env_str("BT_FIXTURES_GLOB", "*.jsonl,*.json,*.csv"))
    p.add_argument(
        "--fixture", action="append", default=None, help="Explicit fixture file path (repeatable)"
    )
    p.add_argument("--fixture-limit", type=int, default=_env_int("BT_FIXTURE_LIMIT", 0))
    _add_bool_flag(
        p,
        "fixture-shuffle",
        "BT_FIXTURE_SHUFFLE",
        None,
        "Shuffle fixture ordering deterministically with --seed",
    )

    # execution
    _add_bool_flag(p, "parallel", "BT_PARALLEL", None, "Run configs in parallel")
    p.add_argument("--parallel-limit", type=int, default=_env_int("BT_PARALLEL_LIMIT", 8))
    p.add_argument(
        "--timeout-sec",
        type=float,
        default=_env_float("BT_TIMEOUT_SEC", 0.0),
        help="Per-config timeout (0 disables)",
    )
    p.add_argument(
        "--total-timeout-sec",
        type=float,
        default=_env_float("BT_TOTAL_TIMEOUT_SEC", 0.0),
        help="Total run timeout (0 disables)",
    )
    p.add_argument("--max-failures", type=int, default=_env_int("BT_MAX_FAILURES", 0))
    p.add_argument("--seed", type=int, default=_env_int("BT_RAND_SEED", 0))
    _add_bool_flag(
        p, "shuffle-configs", "BT_SHUFFLE_CONFIGS", None, "Shuffle config execution order"
    )
    p.add_argument(
        "--sample-config-fraction", type=float, default=_env_float("BT_SAMPLE_CONFIG_FRACTION", 0.0)
    )

    # tag filters
    p.add_argument("--include-tag", default=_env_str("BT_INCLUDE_TAG", ""))
    p.add_argument("--exclude-tag", default=_env_str("BT_EXCLUDE_TAG", ""))

    # resume
    p.add_argument(
        "--resume-json",
        default=_env_str("BT_RESUME_JSON", ""),
        help="Prior export JSON to skip already-run tags",
    )

    # exports
    p.add_argument("--export-json", default=_env_str("BT_EXPORT_JSON", ""))
    p.add_argument("--export-csv", default=_env_str("BT_EXPORT_CSV", ""))
    p.add_argument("--export-trades-json", default=_env_str("BT_EXPORT_TRADES_JSON", ""))
    p.add_argument("--export-errors-json", default=_env_str("BT_EXPORT_ERRORS_JSON", ""))
    p.add_argument("--export-env-best", default=_env_str("BT_EXPORT_ENV_BEST", ""))
    p.add_argument("--export-json-every", type=int, default=_env_int("BT_EXPORT_JSON_EVERY", 0))
    _add_bool_flag(
        p, "append-export-json", "BT_APPEND_EXPORT_JSON", None, "Append to export-json if it exists"
    )

    # sorting/printing
    p.add_argument(
        "--sort-key",
        default=_env_str("BT_SORT_KEY", ""),
        help="Force sort key (otherwise auto-pick)",
    )
    p.add_argument(
        "--top", type=int, default=_env_int("BT_TOP", 25), help="Rows to print (0 prints all)"
    )

    # db
    _add_bool_flag(
        p,
        "save-sweep-to-ai-db",
        "BT_SAVE_SWEEP_TO_AI_DB",
        None,
        "Log sweep to ai_db.log_backtest_sweep if available",
    )
    _add_bool_flag(
        p, "require-db", "BT_REQUIRE_DB", False, "Hard-fail if ai_db connectivity checks fail"
    )

    # debug
    _add_bool_flag(p, "dry-run", "BT_DRY_RUN", False, "Build grid and print example, then exit")
    _add_bool_flag(p, "verbose", "BT_VERBOSE", False, "Print per-config stats JSON")
    _add_bool_flag(p, "print-best-json", "BT_PRINT_BEST_JSON", None, "Print best config+stats JSON")

    p.add_argument("--group-id", default=_env_str("BT_GROUP_ID", ""))
    p.add_argument("--run-note", default=_env_str("BT_RUN_NOTE", ""))

    return p


# =============================================================================
# Grid builder
# =============================================================================
def _resolve_date_range(args: argparse.Namespace) -> tuple[datetime | None, datetime | None, int]:
    start_ts = _parse_date(args.start_date)
    end_ts = _parse_date(args.end_date)
    if start_ts and end_ts:
        if end_ts.time() == datetime.min.time():
            end_ts = end_ts + timedelta(days=1)
        eff_days = max(1, int((end_ts - start_ts).days))
        return start_ts, end_ts, eff_days
    return None, None, int(args.days_back)


def _build_grid(args: argparse.Namespace, fixture_paths: Sequence[str]) -> list[BacktestConfig]:
    score_mins = _parse_floats(args.score_min) or [0.60]
    max_pos_list = _parse_ints(args.max_positions) or [1]

    risk_mults: list[float | None] = _parse_floats(args.risk_mult) or [None]
    regime_modes_raw = _parse_ints(args.regime_risk_mode)
    regime_modes: list[int | None] = (
        [None]
        if not regime_modes_raw
        else [m for m in regime_modes_raw if m in (0, 1, 2)] or [None]
    )

    entry_min_scores: list[float | None] = _parse_floats(args.entry_min_score) or [None]
    entry_ai_weights: list[float | None] = _parse_floats(args.entry_ai_weight) or [None]
    risk_profiles: list[str | None] = _parse_strs(args.risk_profile) or [None]
    exit_policies: list[str | None] = _parse_strs(args.exit_policy) or [None]

    start_ts, end_ts, eff_days = _resolve_date_range(args)

    regime = tuple(_parse_strs(args.regime)) or None
    lab_inc = tuple(_parse_strs(args.label_include)) or None
    lab_exc = tuple(_parse_strs(args.label_exclude)) or None
    limit_candidates = (
        int(args.limit_candidates)
        if args.limit_candidates and int(args.limit_candidates) > 0
        else None
    )

    entry_model = (args.entry_model or "").strip() or None
    feature_mode = (args.feature_mode or "").strip() or None
    sw_mode = (args.smart_wallet_mode or "").strip() or None
    engine_mode = (args.engine_mode or "").strip() or None
    segment = (args.segment or "").strip() or None

    policy_version = (args.policy_version or "").strip() or None
    experiment_tag = (args.experiment_tag or "").strip() or None

    src = (args.src or "").strip() or None
    min_fv = (
        int(args.min_feature_version)
        if args.min_feature_version and int(args.min_feature_version) > 0
        else None
    )
    wallet_tag = (args.wallet_tag or "").strip() or None
    session_id = (args.session_id or "").strip() or None

    trade_cost = (
        float(args.trade_cost_bps)
        if args.trade_cost_bps and float(args.trade_cost_bps) > 0
        else None
    )
    jito_tip = (
        float(args.jito_tip_bps) if args.jito_tip_bps and float(args.jito_tip_bps) > 0 else None
    )
    base_size = (
        float(args.base_size_sol) if args.base_size_sol and float(args.base_size_sol) > 0 else None
    )
    max_hold = int(args.max_hold_sec) if args.max_hold_sec and int(args.max_hold_sec) > 0 else None

    group_id = (args.group_id or "").strip() or None
    run_note = (args.run_note or "").strip() or None

    fixtures_mode = (args.fixtures_mode or "off").strip().lower()
    fixtures_mode = "on" if fixtures_mode in ("1", "true", "yes", "y", "on") else "off"
    fixtures_dir = (args.fixtures_dir or "").strip() or None
    fixture_glob = (args.fixtures_glob or "").strip() or None
    fixture_limit = (
        int(args.fixture_limit) if args.fixture_limit and int(args.fixture_limit) > 0 else None
    )
    fixture_paths_t = tuple(fixture_paths) if fixture_paths else None

    configs: list[BacktestConfig] = []

    for s in score_mins:
        s = max(0.0, min(1.0, float(s)))
        for mp in max_pos_list:
            mp = max(1, int(mp))
            for rm in risk_mults:
                for rg in regime_modes:
                    for emin in entry_min_scores:
                        for aiw in entry_ai_weights:
                            for rp in risk_profiles:
                                for ep in exit_policies:
                                    parts = [f"s{s:.3f}", f"mp{mp}"]
                                    if rm is not None:
                                        parts.append(f"rm{float(rm):.2f}")
                                    if rg is not None:
                                        parts.append(f"rg{int(rg)}")
                                    if emin is not None:
                                        parts.append(f"emin{float(emin):.2f}")
                                    if aiw is not None:
                                        parts.append(f"aiw{float(aiw):.2f}")
                                    if rp:
                                        parts.append(f"rp{rp}")
                                    if ep:
                                        parts.append(f"ep{ep}")
                                    if feature_mode:
                                        parts.append(f"feat{feature_mode}")
                                    if sw_mode:
                                        parts.append(f"sw{sw_mode}")
                                    if engine_mode:
                                        parts.append(f"eng{engine_mode}")
                                    if segment:
                                        parts.append(f"seg{segment}")
                                    if src:
                                        parts.append(f"src{src}")
                                    if min_fv is not None:
                                        parts.append(f"fv{min_fv}")
                                    if wallet_tag:
                                        parts.append(f"wt{wallet_tag}")
                                    if session_id:
                                        parts.append("sess1")
                                    if trade_cost is not None:
                                        parts.append(f"tc{trade_cost:.1f}")
                                    if jito_tip is not None:
                                        parts.append(f"jito{jito_tip:.1f}")
                                    if group_id:
                                        parts.append(f"gid{group_id}")
                                    if args.use_ai_risk_overlay is True:
                                        parts.append("airisk1")
                                    if args.enable_self_learning is True:
                                        parts.append("sl1")
                                    if args.enable_reinvestor is True:
                                        parts.append("rv1")
                                    if fixtures_mode == "on":
                                        parts.append(f"fx{len(fixture_paths)}")

                                    tag = _sanitize_tag("_".join(parts))

                                    configs.append(
                                        BacktestConfig(
                                            ENTRY_SCORE_MIN=s,
                                            max_positions=mp,
                                            days_back=int(eff_days),
                                            start_ts=start_ts,
                                            end_ts=end_ts,
                                            regime_whitelist=regime,
                                            label_include=lab_inc,
                                            label_exclude=lab_exc,
                                            limit_candidates=limit_candidates,
                                            risk_mult_override=float(rm)
                                            if rm is not None
                                            else None,
                                            regime_risk_mode_lock=rg,
                                            entry_min_score=float(emin)
                                            if emin is not None
                                            else None,
                                            entry_ai_weight=float(aiw) if aiw is not None else None,
                                            entry_model_name=entry_model,
                                            risk_profile=rp,
                                            exit_policy_name=ep,
                                            require_risk_capacity=args.require_risk_capacity,
                                            feature_mode=feature_mode,
                                            smart_wallet_mode=sw_mode,
                                            engine_mode=engine_mode,
                                            result_segment=segment,
                                            trade_cost_bps=trade_cost,
                                            jito_tip_bps=jito_tip,
                                            base_size_sol=base_size,
                                            max_hold_sec=max_hold,
                                            policy_version=policy_version,
                                            experiment_tag=experiment_tag,
                                            src_filter=src,
                                            min_feature_version=min_fv,
                                            wallet_tag_filter=wallet_tag,
                                            session_id_filter=session_id,
                                            use_ai_risk_overlay=args.use_ai_risk_overlay,
                                            enable_self_learning=args.enable_self_learning,
                                            enable_reinvestor=args.enable_reinvestor,
                                            fixtures_mode=fixtures_mode,
                                            fixtures_dir=fixtures_dir,
                                            fixture_glob=fixture_glob,
                                            fixture_paths=fixture_paths_t,
                                            fixture_limit=fixture_limit,
                                            tag=tag,
                                            group_id=group_id,
                                            run_note=run_note,
                                            extra=None,
                                        )
                                    )

    return configs


# =============================================================================
# Exports
# =============================================================================
def _export_json(
    path: str, pairs: Sequence[tuple[BacktestConfig, Mapping[str, Any]]], append: bool
) -> None:
    rows = [{"config": cfg.to_run_kwargs(), "stats": dict(st)} for cfg, st in pairs]
    meta = {
        "generated_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "command": " ".join(sys.argv),
        "python": sys.version.split()[0],
        "platform": platform.platform(),
        "repo_root": REPO_ROOT,
        "count": len(rows),
    }

    if append and os.path.exists(path):
        old = _read_json(path) or {}
        old_results = old.get("results", [])
        if not isinstance(old_results, list):
            old_results = []
        payload = dict(old)
        payload["meta"] = dict(payload.get("meta") or {})
        payload["meta"].update(meta)
        payload["meta"]["updated_at"] = datetime.now(UTC).isoformat().replace("+00:00", "Z")
        payload["results"] = list(old_results) + rows
        payload["meta"]["count"] = len(payload["results"])
    else:
        payload = {"meta": meta, "results": rows}

    _atomic_write_json(path, payload)
    print(f"Exported JSON → {path} (count={payload['meta']['count']})")


def _export_csv(path: str, pairs: Sequence[tuple[BacktestConfig, Mapping[str, Any]]]) -> None:
    cfg_keys = sorted({k for cfg, _ in pairs for k in cfg.to_run_kwargs()})
    st_keys = sorted({k for _, st in pairs for k in st})
    fieldnames = ["rank", *cfg_keys, *st_keys]

    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix=p.name + ".", suffix=".tmp", dir=str(p.parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="") as f:
            w = csv.DictWriter(f, fieldnames=fieldnames)
            w.writeheader()
            for i, (cfg, st) in enumerate(pairs, start=1):
                row: dict[str, Any] = {"rank": i}
                row.update(cfg.to_run_kwargs())
                for k in st_keys:
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

    print(f"Exported CSV → {path}")


def _export_trades_json(
    path: str, pairs: Sequence[tuple[BacktestConfig, Mapping[str, Any]]]
) -> None:
    trades_out: list[dict[str, Any]] = []
    for cfg, st in pairs:
        trades = st.get("trades")
        if isinstance(trades, list):
            for t in trades:
                rec = dict(t) if isinstance(t, dict) else {"raw": t}
                rec.setdefault("config_tag", cfg.tag)
                trades_out.append(rec)

    payload = {
        "meta": {
            "generated_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
            "count_trades": len(trades_out),
        },
        "trades": trades_out,
    }
    _atomic_write_json(path, payload)
    print(f"Exported trades JSON → {path} (n={len(trades_out)})")


def _export_errors_json(path: str, errors: Sequence[Mapping[str, Any]]) -> None:
    payload = {
        "meta": {
            "generated_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
            "count_errors": len(errors),
        },
        "errors": list(errors),
    }
    _atomic_write_json(path, payload)
    print(f"Exported errors JSON → {path} (n={len(errors)})")


def _export_env_best(
    path: str, best_cfg: BacktestConfig, best_stats: Mapping[str, Any], sort_key: str
) -> None:
    cfgd = best_cfg.to_run_kwargs()
    sort_val = best_stats.get(sort_key)
    lines = [
        "# Auto-generated by scripts/backtest.py",
        f"# generated_at={datetime.now(UTC).isoformat().replace('+00:00', 'Z')}",
        f"# best_sort_key={sort_key} value={sort_val}",
        f"# config_tag={best_cfg.tag}",
        "",
    ]
    for k in sorted(cfgd.keys()):
        v = cfgd[k]
        if isinstance(v, bool):
            v = "1" if v else "0"
        lines.append(f"{k}={v}")
    _atomic_write_text(path, "\n".join(lines) + "\n")
    print(f"Exported best env → {path}")


# =============================================================================
# ai_db helpers (optional)
# =============================================================================
async def _ai_db_call(name: str, *args: Any, **kwargs: Any) -> Any | None:
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
        log.warning("ai_db.%s failed: %s", name, e)
        return None


# =============================================================================
# Formatting + printing
# =============================================================================
def _pick_sort_key(stats: Mapping[str, Any]) -> str:
    preferred = ["edge_score", "net_pnl_pct", "pnl_total_pct", "sharpe", "profit_factor"]
    for k in preferred:
        if k in stats:
            return k
    for k, v in stats.items():
        if isinstance(v, int | float):
            return k
    return "edge_score"


def _fmt(v: Any) -> str:
    if isinstance(v, float):
        if not math.isfinite(v):
            return ""
        return f"{v:.3f}"
    return "" if v is None else str(v)


def _print_table(
    pairs: Sequence[tuple[BacktestConfig, Mapping[str, Any]]], sort_key: str, top_n: int
) -> None:
    top = list(pairs[:top_n]) if top_n and top_n > 0 else list(pairs)

    headers = [
        "rank",
        "tag",
        "score",
        "mp",
        sort_key,
        "n_trades",
        "win_rate",
        "max_dd",
        "monthly",
        "edge",
    ]
    rows: list[list[str]] = []
    for i, (cfg, st) in enumerate(top, start=1):
        rows.append(
            [
                str(i),
                cfg.tag,
                f"{cfg.ENTRY_SCORE_MIN:.3f}",
                str(cfg.max_positions),
                _fmt(st.get(sort_key, "")),
                _fmt(st.get("n_trades", "")),
                _fmt(st.get("win_rate", "")),
                _fmt(st.get("max_drawdown_pct", "")),
                _fmt(st.get("monthly_pnl_pct", "")),
                _fmt(st.get("edge_score", "")),
            ]
        )

    colw = [len(h) for h in headers]
    for r in rows:
        for j, c in enumerate(r):
            colw[j] = max(colw[j], len(c))

    def line(cells: Sequence[str]) -> str:
        return " | ".join(cells[i].ljust(colw[i]) for i in range(len(cells)))

    sep = "-" * (sum(colw) + 3 * (len(colw) - 1))
    print("\nBacktest results:")
    print(sep)
    print(line(headers))
    print(sep)
    for r in rows:
        print(line(r))
    print(sep)
    print("")


# =============================================================================
# Execution
# =============================================================================
async def _run_one(cfg: BacktestConfig, per_timeout: float) -> dict[str, Any]:
    kwargs = cfg.to_run_kwargs()
    kwargs["_fingerprint"] = _fingerprint(kwargs)
    t0 = time.time()
    try:
        coro = _call_run_backtest(kwargs)
        res = await asyncio.wait_for(coro, timeout=per_timeout) if per_timeout > 0 else await coro
        st = dict(res) if isinstance(res, Mapping) else {"result": res}
        _augment_stats(st)
        st.setdefault("elapsed_s", round(time.time() - t0, 3))
        st.setdefault("config_tag", cfg.tag)
        st.setdefault("_fingerprint", kwargs["_fingerprint"])
        return st
    except TimeoutError:
        return {
            "error": f"timeout after {per_timeout}s",
            "config_tag": cfg.tag,
            "_fingerprint": kwargs["_fingerprint"],
        }
    except Exception as e:
        log.exception("Backtest failed tag=%s", cfg.tag)
        return {"error": str(e), "config_tag": cfg.tag, "_fingerprint": kwargs["_fingerprint"]}


async def _main_async(args: argparse.Namespace) -> int:
    # Seed for deterministic shuffles/sampling
    seed = int(args.seed or 0)
    if seed != 0:
        random.seed(seed)

    # fixtures discovery
    fixtures_mode = (args.fixtures_mode or "off").strip().lower()
    fixtures_mode = "on" if fixtures_mode in ("1", "true", "yes", "y", "on") else "off"
    fixture_limit = (
        int(args.fixture_limit) if args.fixture_limit and int(args.fixture_limit) > 0 else None
    )
    fixture_shuffle = bool(args.fixture_shuffle is True)

    fixture_paths = (
        _discover_fixtures(
            args.fixtures_dir,
            args.fixtures_glob,
            explicit=args.fixture,
            limit=fixture_limit,
            shuffle=fixture_shuffle,
            seed=seed if seed != 0 else 0,
        )
        if fixtures_mode == "on"
        else []
    )

    configs = _build_grid(args, fixture_paths)
    if not configs:
        print("No configs built.")
        return 2

    # include/exclude tag filters
    inc = re.compile(args.include_tag) if (args.include_tag or "").strip() else None
    exc = re.compile(args.exclude_tag) if (args.exclude_tag or "").strip() else None
    if inc or exc:
        configs = [
            c
            for c in configs
            if (not inc or inc.search(c.tag)) and (not exc or not exc.search(c.tag))
        ]
        if not configs:
            print("All configs removed by include/exclude tag filters.")
            return 2

    # shuffle configs (deterministic given seed)
    if args.shuffle_configs is True:
        random.shuffle(configs)

    # sample fraction (deterministic given seed)
    frac = float(args.sample_config_fraction or 0.0)
    if 0.0 < frac < 1.0 and len(configs) > 1:
        k = max(1, int(len(configs) * frac))
        total_before = len(configs)
        configs = random.sample(configs, k)
        print(f"Sampling configs: {k}/{total_before} (fraction={frac:.3f})")

    # resume skip
    if args.resume_json:
        skip = _load_resume_tags(args.resume_json)
        if skip:
            before = len(configs)
            configs = [c for c in configs if c.tag not in skip]
            print(
                f"Resume: skipped {before - len(configs)} already-run tags, remaining={len(configs)}"
            )
            if not configs:
                print("Nothing to run after resume-skip.")
                return 0

    # dry run
    if args.dry_run is True:
        print(f"DRY RUN: built {len(configs)} configs.")
        if fixtures_mode == "on":
            print(f"Fixtures replay: {len(fixture_paths)} fixture(s)")
            if fixture_paths:
                print("First fixture:", fixture_paths[0])
        print("Example cfg:")
        print(json.dumps(configs[0].to_run_kwargs(), indent=2, default=str))
        return 0

    # ai_db checks
    if ai_db is None:
        log.info("ai_db not available (import failed) — continuing without DB helpers.")
        if args.require_db is True:
            print("ERROR: --require-db set but ai_db import failed.")
            return 2
    else:
        ok = await _ai_db_call("ping")
        if ok is False and args.require_db is True:
            print("ERROR: ai_db.ping() failed and --require-db set.")
            return 2

    # run summary
    n = len(configs)
    per_timeout = float(args.timeout_sec or 0.0)
    total_timeout = float(args.total_timeout_sec or 0.0)
    parallel = bool(args.parallel is True)
    par_limit = max(1, int(args.parallel_limit or 1))

    print(
        f"Running {n} config(s)\n"
        f"  repo_root        : {REPO_ROOT}\n"
        f"  fixtures_mode    : {fixtures_mode} (count={len(fixture_paths)})\n"
        f"  parallel         : {parallel} (limit={par_limit})\n"
        f"  per_timeout_sec  : {per_timeout}\n"
        f"  total_timeout_sec: {total_timeout}\n"
    )

    results: list[dict[str, Any] | None] = [None] * n
    failures = 0
    completed = 0
    abort = asyncio.Event()

    async def store(i: int, st: dict[str, Any]) -> None:
        nonlocal failures, completed
        results[i] = st
        completed += 1
        is_fail = "error" in st
        if is_fail:
            failures += 1
            print(f"[{completed}/{n}] FAIL tag={configs[i].tag} err={st.get('error')}")
        else:
            if args.verbose is True:
                print(
                    f"[{completed}/{n}] OK tag={configs[i].tag} stats={json.dumps(st, default=str)[:1200]}"
                )
            elif completed % 5 == 0 or completed == n:
                print(f"Progress {completed}/{n} (failures={failures})")

        if args.max_failures and int(args.max_failures) > 0 and failures >= int(args.max_failures):
            abort.set()

        # checkpoint export
        if args.export_json and args.export_json_every and int(args.export_json_every) > 0:
            if completed % int(args.export_json_every) == 0:
                ok_pairs_ckpt: list[tuple[BacktestConfig, Mapping[str, Any]]] = []
                for j in range(n):
                    stj = results[j]
                    if not stj or "error" in stj:
                        continue
                    ok_pairs_ckpt.append((configs[j], stj))
                if ok_pairs_ckpt:
                    _export_json(
                        args.export_json, ok_pairs_ckpt, append=bool(args.append_export_json)
                    )

    async def run_serial() -> None:
        for i, cfg in enumerate(configs):
            if abort.is_set():
                break
            st = await _run_one(cfg, per_timeout)
            await store(i, st)

    async def run_parallel() -> None:
        sem = asyncio.Semaphore(par_limit)

        async def task(i: int, cfg: BacktestConfig) -> None:
            if abort.is_set():
                results[i] = {"error": "skipped due to abort", "config_tag": cfg.tag}
                return
            async with sem:
                st = await _run_one(cfg, per_timeout)
                await store(i, st)

        tasks = [asyncio.create_task(task(i, cfg)) for i, cfg in enumerate(configs)]
        try:
            while True:
                if abort.is_set():
                    for t in tasks:
                        if not t.done():
                            t.cancel()
                    await asyncio.gather(*tasks, return_exceptions=True)
                    break
                if all(t.done() for t in tasks):
                    break
                await asyncio.sleep(0.25)
        finally:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def run_all() -> None:
        if parallel and n > 1:
            await run_parallel()
        else:
            await run_serial()

    try:
        if total_timeout > 0:
            await asyncio.wait_for(run_all(), timeout=total_timeout)
        else:
            await run_all()
    except TimeoutError:
        print("ERROR: total timeout reached.")
        abort.set()

    ok_pairs: list[tuple[BacktestConfig, Mapping[str, Any]]] = []
    errors: list[Mapping[str, Any]] = []
    for i in range(n):
        st = results[i]
        if not st:
            continue
        if "error" in st:
            errors.append({"tag": configs[i].tag, **dict(st)})
            continue
        ok_pairs.append((configs[i], st))

    if not ok_pairs:
        print("All configs failed.")
        if args.export_errors_json and errors:
            _export_errors_json(args.export_errors_json, errors)
        return 2

    sort_key = (args.sort_key or "").strip() or _pick_sort_key(ok_pairs[0][1])
    ok_pairs.sort(key=lambda cs: _safe_float(cs[1].get(sort_key), 0.0), reverse=True)

    _print_table(ok_pairs, sort_key=sort_key, top_n=int(args.top))

    best_cfg, best_st = ok_pairs[0]
    if args.print_best_json is True:
        print(
            json.dumps(
                {"config": best_cfg.to_run_kwargs(), "stats": dict(best_st)}, indent=2, default=str
            )
        )

    if args.export_json:
        auto_append = bool(args.append_export_json)
        if args.resume_json and os.path.abspath(args.resume_json) == os.path.abspath(
            args.export_json
        ):
            auto_append = True
        _export_json(args.export_json, ok_pairs, append=auto_append)
    if args.export_csv:
        _export_csv(args.export_csv, ok_pairs)
    if args.export_trades_json:
        _export_trades_json(args.export_trades_json, ok_pairs)
    if args.export_errors_json and errors:
        _export_errors_json(args.export_errors_json, errors)
    if args.export_env_best:
        _export_env_best(args.export_env_best, best_cfg, best_st, sort_key)

    if args.save_sweep_to_ai_db is True and ai_db is not None:
        fn = getattr(ai_db, "log_backtest_sweep", None)
        if callable(fn):
            meta = {
                "group_id": (args.group_id or None),
                "run_note": (args.run_note or None),
                "command": " ".join(sys.argv),
                "generated_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
                "sort_key": sort_key,
                "count": len(ok_pairs),
                "fixtures_mode": fixtures_mode,
                "fixtures_count": len(fixture_paths),
            }
            try:
                out = fn(
                    meta, [c.to_run_kwargs() for c, _ in ok_pairs], [dict(s) for _, s in ok_pairs]
                )
                if inspect.isawaitable(out):
                    await out
                print("Logged sweep to ai_db.log_backtest_sweep().")
            except Exception as e:
                print(f"WARNING: ai_db.log_backtest_sweep failed: {e}")

    return 0


def main() -> None:
    args = build_arg_parser().parse_args()

    # Windows event loop safety
    try:
        if os.name == "nt" and hasattr(asyncio, "WindowsSelectorEventLoopPolicy"):
            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())  # type: ignore[attr-defined]
    except Exception:
        pass

    try:
        rc = asyncio.run(_main_async(args))
    except KeyboardInterrupt:
        print("Interrupted.")
        rc = 130
    raise SystemExit(rc)


if __name__ == "__main__":
    main()
