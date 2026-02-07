# -*- coding: utf-8 -*-
from __future__ import annotations

"""
utils_pkg/self_learning_ai_strategy.py

Self-learning Strategy Brain — God Tier V4+++ (quant-grade)
===========================================================

A bounded, safety-first strategy tuner that:
  • Records closed trades (full/partial) into a rolling store
  • Computes robust performance stats (trimmed, downside-risk aware, drawdown-aware)
  • Reads risk_manager context (risk:ctx) and obeys it
  • Publishes live knobs to strategy:params (single source for tunable params)
  • Emits health snapshots + param history
  • Optional (off by default) OpenAI micro-nudges that are STILL bounded + step-limited

Non-goals:
  • Does not execute trades
  • Does not invent params outside the contract
  • Does not tune outside hard bounds + per-step limits
  • LLM tuning is optional, off by default, and only suggests bounded micro nudges

Public API (stable)
-------------------
    async def get_strategy_params(redis=None) -> dict
    async def set_strategy_params(redis, params: dict) -> None

    async def process_trade_closure(
        token_address: str,
        buy_price: float,
        sell_price: float,
        amount: float,
        partial: bool,
        timestamp: str,
        *,
        entry_score: float | None = None,
        hold_minutes: float | None = None,
        meta: dict | None = None,
        # optional extra fields (safe, ignored if absent)
        profit_usd: float | None = None,
        equity_after_usd: float | None = None,
        redis: RedisLike | None = None,
        **_ignored: Any,
    ) -> None

    async def get_perf_snapshot(redis=None) -> dict
    async def tune_once(redis=None) -> None
    async def run_self_learning_loop(redis=None) -> None

    async def get_health_snapshot(redis=None) -> dict
    async def get_plan_snapshot(redis=None) -> dict
    async def get_param_history(redis=None, limit: int = 50) -> list[dict]

    # reinvest feedback hooks
    async def note_reinvest_decision(redis, payload: dict) -> None
    async def note_reinvest_result(redis, payload: dict) -> None
    async def note_reinvest_skipped(redis, payload: dict) -> None
    async def get_reinvest_history(redis=None, limit: int = 100) -> list[dict]

    async def get_strategy_with_risk(redis=None) -> dict
    async def get_full_debug_snapshot(redis=None) -> dict

Redis keys (contract)
---------------------
  strategy:params             -> JSON blob (current params)
  strategy:trades             -> LIST JSON (newest first)
  strategy:stats              -> JSON blob (aggregated stats)
  strategy:last_tune_ts       -> float timestamp seconds
  strategy:last_llm_ts        -> float timestamp seconds
  strategy:param_history      -> LIST JSON (tuning diffs, newest first)
  strategy:health             -> JSON blob (health snapshot)
  strategy:reinvest_history   -> LIST JSON (reinvest events)
  strategy:tune_lock          -> distributed lock key

Notes
-----
- Safe by construction: all modifications are clamped + step-limited.
- Mean-reverting: “aggressive” mode is earned and fades fast when regime degrades.
- risk_manager remains the ultimate authority via risk:ctx (hard_halt, caps, etc).
- Import is side-effect free (no network connections or background tasks at import).
"""

import asyncio
import contextlib
import inspect
import json
import logging
import math
import os
import time
import uuid
from collections.abc import Awaitable, Callable
from statistics import mean
from typing import TYPE_CHECKING, Any, Protocol, cast

# =============================================================================
# Logging (mypy-safe, avoids "Logger assigned to EventLogger" mismatches)
# =============================================================================

_stdlog = logging.getLogger(__name__)

if TYPE_CHECKING:
    from utils_pkg.log_compat import get_logger as _get_logger  # pragma: no cover
else:  # pragma: no cover
    try:
        from utils_pkg.log_compat import get_logger as _get_logger  # type: ignore
    except Exception:  # pragma: no cover

        def _get_logger(  # type: ignore[no-redef]
            name: str | None = None,
            *,
            service: str | None = None,
            version: str | None = None,
            env: str | None = None,
            level: str | int | None = None,
            json_logs: bool | None = None,
            use_utc: bool | None = None,
            include_src: bool | None = None,
            stream: str | None = None,
            force: bool = False,
        ) -> logging.Logger:
            lg = logging.getLogger(name or "self_learning_ai_strategy")
            if force or not lg.handlers:
                logging.basicConfig(level=logging.INFO)
            if level is not None:
                with contextlib.suppress(Exception):
                    lg.setLevel(level)  # type: ignore[arg-type]
            return lg


# Do NOT annotate this as a project-specific EventLogger; keep Any to avoid mypy mismatch.
log: Any = _get_logger(__name__)


def _emit_event(name: str, **fields: Any) -> None:
    """Best-effort structured event emission. Never raises."""
    try:
        ev = getattr(log, "event", None)
        if callable(ev):
            ev(name, **fields)  # type: ignore[misc]
            return
    except Exception:
        pass
    with contextlib.suppress(Exception):
        _stdlog.info("[event] %s %s", name, fields)


# Optional metric bump (best-effort).
class _BumpImpl(Protocol):
    def __call__(self, name: str, by: int = 1, **tags: Any) -> None: ...


_bump_impl: _BumpImpl | None
try:
    from logging_setup import bump as _bump_func  # type: ignore[import-not-found]
except Exception:  # pragma: no cover
    _bump_impl = None
else:
    _bump_impl = _bump_func


def _metric(name: str, value: int = 1) -> None:
    if _bump_impl is None:
        return
    with contextlib.suppress(Exception):
        _bump_impl(name, int(value))


async def _maybe_await(x: Any) -> Any:
    if inspect.isawaitable(x):
        return await x
    return x


def _now() -> float:
    return float(time.time())


# =============================================================================
# Optional dependencies (keep imports lightweight)
# =============================================================================

_redis_async: Any | None
try:
    import redis.asyncio as _redis_async  # type: ignore[import-not-found]
except Exception:  # pragma: no cover
    _redis_async = None


# =============================================================================
# Redis interface typing (kept intentionally SMALL + compatible)
# =============================================================================


class RedisLike(Protocol):
    async def get(self, key: str) -> Any: ...
    async def set(self, key: str, value: Any, *args: Any, **kwargs: Any) -> Any: ...
    async def lrange(self, key: str, start: int, end: int) -> Any: ...
    async def lpush(self, key: str, value: Any) -> Any: ...
    async def ltrim(self, key: str, start: int, end: int) -> Any: ...
    async def hgetall(self, key: str) -> Any: ...
    async def eval(self, script: str, numkeys: int, *args: Any) -> Any: ...


# Project redis factory (optional)
_GetRedisAsync = Callable[[], Awaitable["RedisLike | None"]]
_project_get_redis_async: _GetRedisAsync | None

try:
    from utils_pkg.core_utils import (
        get_redis_async as _project_get_redis_async_raw,  # type: ignore[attr-defined]
    )
except Exception:  # pragma: no cover
    _project_get_redis_async = None
else:
    _project_get_redis_async = cast(_GetRedisAsync, _project_get_redis_async_raw)


# ---- Telegram (optional, but mypy-safe) -------------------------------------

_SendTelegramImpl = Callable[..., Any]
_send_telegram_impl: _SendTelegramImpl | None

try:
    from utils_pkg.core_utils import (
        send_telegram as _send_telegram_func,  # type: ignore[attr-defined]
    )
except Exception:  # pragma: no cover
    _send_telegram_impl = None
else:
    _send_telegram_impl = cast(_SendTelegramImpl, _send_telegram_func)


# ---- Notion (optional, but mypy-safe) ---------------------------------------

_LogToNotionImpl = Callable[..., Any]
_log_to_notion_impl: _LogToNotionImpl | None

try:
    from utils_pkg.core_utils import (
        log_to_notion as _log_to_notion_func,  # type: ignore[attr-defined]
    )
except Exception:  # pragma: no cover
    _log_to_notion_impl = None
else:
    _log_to_notion_impl = _log_to_notion_func


# ---- Data logger (optional) -------------------------------------------------

_log_event_impl: Callable[..., Any] | None
try:
    from utils_pkg.data_logger import log_event as _log_event_func  # type: ignore[attr-defined]
except Exception:  # pragma: no cover
    _log_event_impl = None
else:
    _log_event_impl = cast(Callable[..., Any], _log_event_func)


def _to_str(x: Any) -> str:
    if x is None:
        return ""
    if isinstance(x, str):
        return x
    if isinstance(x, (bytes, bytearray)):
        return bytes(x).decode("utf-8", "ignore")
    return str(x)


def _to_dict_str_any(x: Any) -> dict[str, Any]:
    if not isinstance(x, dict):
        return {}
    out: dict[str, Any] = {}
    for k, v in x.items():
        out[_to_str(k)] = v
    return out


def _json_fallback(o: Any) -> str:
    # Last-resort for non-serializable objects; keeps module crash-proof.
    return str(o)


def _dumps(obj: Any) -> str:
    return json.dumps(
        obj,
        separators=(",", ":"),
        ensure_ascii=False,
        sort_keys=True,
        default=_json_fallback,
    )


def _loads_dict(raw: Any) -> dict[str, Any]:
    try:
        s = _to_str(raw).strip()
        if not s:
            return {}
        v = json.loads(s)
    except Exception:
        return {}
    return v if isinstance(v, dict) else {}


def _loads_obj(raw: Any) -> Any:
    try:
        s = _to_str(raw).strip()
        if not s:
            return None
        return json.loads(s)
    except Exception:
        return None


def _parse_bool(v: Any, default: bool = False) -> bool:
    if v is None:
        return default
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float)):
        return bool(v)
    if isinstance(v, str):
        return v.strip().lower() in ("1", "true", "yes", "on")
    return default


def _finite(x: float) -> float:
    if math.isfinite(x):
        return float(x)
    return 0.0


def _safe_float(x: Any, default: float | None = 0.0) -> float:
    try:
        return _finite(float(x))
    except Exception:
        return _finite(float(default if default is not None else 0.0))


def _safe_int(x: Any, default: int = 0) -> int:
    try:
        return int(round(float(x)))
    except Exception:
        return int(default)


def _clamp(v: float, lo: float, hi: float) -> float:
    vv = float(v)
    lo2 = float(lo)
    hi2 = float(hi)
    if hi2 < lo2:
        hi2 = lo2
    return float(min(max(vv, lo2), hi2))


def _safe_mean(xs: list[float]) -> float:
    return float(mean(xs)) if xs else 0.0


def _trimmed_mean(xs: list[float], trim: float = 0.10) -> float:
    if not xs:
        return 0.0
    ys = sorted(xs)
    n = len(ys)
    k = int(n * trim)
    if n - 2 * k <= 0:
        return _safe_mean(xs)
    return _safe_mean(ys[k : n - k])


def _std(xs: list[float]) -> float:
    if not xs:
        return 0.0
    mu = _safe_mean(xs)
    var = _safe_mean([(x - mu) ** 2 for x in xs])
    return float(math.sqrt(var))


def _downside_deviation(pnl: list[float], threshold: float = 0.0) -> float:
    downs = [min(0.0, x - threshold) for x in pnl]
    if not downs:
        return 0.0
    return float(math.sqrt(_safe_mean([d * d for d in downs])))


def _sharpe_like(mu: float, stdv: float) -> float:
    if stdv <= 1e-9:
        return 0.0
    return float(mu / stdv)


def _equity_curve_metrics(pnl_newest_first: list[float]) -> dict[str, float]:
    if not pnl_newest_first:
        return {"equity_cum_return_pct": 0.0, "equity_max_dd_pct": 0.0}

    equity = 1.0
    peak = 1.0
    max_dd = 0.0  # negative

    for p in reversed(pnl_newest_first):
        equity *= 1.0 + p / 100.0
        peak = max(peak, equity)
        dd = (equity - peak) / peak
        max_dd = min(max_dd, dd)

    return {
        "equity_cum_return_pct": float((equity - 1.0) * 100.0),
        "equity_max_dd_pct": float(max_dd * 100.0),
    }


def _current_streaks(pnl_newest_first: list[float], win_cut: float) -> tuple[int, int]:
    win_streak = 0
    loss_streak = 0
    for p in pnl_newest_first:
        if p >= win_cut:
            if loss_streak > 0:
                break
            win_streak += 1
        else:
            if win_streak > 0:
                break
            loss_streak += 1
    return win_streak, loss_streak


def _classify_trade(profit_pct: float, hold_minutes: float | None) -> str:
    h = float(hold_minutes or 0.0)
    p = float(profit_pct)
    if p <= -50.0:
        return "rug"
    if p >= 150.0:
        return "big_win"
    if h <= 20.0 and p >= 10.0:
        return "scalp_win"
    if h <= 20.0 and p < 0.0:
        return "scalp_loss"
    if -5.0 <= p <= 5.0:
        return "scratch"
    return "grinder"


async def send_telegram(
    text: str | None = None,
    *,
    message: str | None = None,
    msg: str | None = None,
    session: Any | None = None,
    chat_id: str | None = None,
    token: str | None = None,
    parse_mode: str | None = None,
    disable_preview: bool | None = None,
    timeout_s: float | None = None,
    sol_value: float | None = None,
    usd_value: float | None = None,
    **_ignored: Any,
) -> bool:
    """
    Stable wrapper matching the project's widest expected signature.
    Returns bool (best-effort). Never raises.
    """
    final = msg if msg is not None else message if message is not None else text
    if not final:
        return False

    impl = _send_telegram_impl
    if impl is None:
        with contextlib.suppress(Exception):
            _stdlog.info("[TG] %s", final)
        return False

    wide_kwargs: dict[str, Any] = {
        "session": session,
        "chat_id": chat_id,
        "token": token,
        "parse_mode": parse_mode,
        "timeout_s": timeout_s,
        "sol_value": sol_value,
        "usd_value": usd_value,
    }
    if disable_preview is not None:
        wide_kwargs["disable_preview"] = bool(disable_preview)
        wide_kwargs["disable_web_page_preview"] = bool(disable_preview)

    wide_kwargs = {k: v for k, v in wide_kwargs.items() if v is not None}

    try:
        sig = inspect.signature(impl)
        params = sig.parameters
        has_varkw = any(p.kind == inspect.Parameter.VAR_KEYWORD for p in params.values())

        use_positional = True
        call_kwargs = dict(wide_kwargs)

        if "msg" in params:
            call_kwargs["msg"] = final
            use_positional = False
        elif "message" in params:
            call_kwargs["message"] = final
            use_positional = False
        elif "text" in params:
            call_kwargs["text"] = final
            use_positional = False

        if not has_varkw:
            call_kwargs = {k: v for k, v in call_kwargs.items() if k in params}

        out = impl(final, **call_kwargs) if use_positional else impl(**call_kwargs)
        out2 = await _maybe_await(out)
        return bool(out2) if out2 is not None else True
    except TypeError:
        try:
            out = impl(text=final, **wide_kwargs)  # type: ignore[misc]
            out2 = await _maybe_await(out)
            return bool(out2) if out2 is not None else True
        except Exception:
            try:
                out = impl(final)  # type: ignore[misc]
                out2 = await _maybe_await(out)
                return bool(out2) if out2 is not None else True
            except Exception:
                return False
    except Exception:
        return False


async def log_to_notion(*args: Any, **kwargs: Any) -> Any:
    """Stable async wrapper; safely awaits if impl returns awaitable."""
    impl = _log_to_notion_impl
    if impl is None:
        return None
    try:
        out = impl(*args, **kwargs)
        return await _maybe_await(out)
    except Exception:
        return None


async def log_event(name: str, payload: dict[str, Any] | None = None, **fields: Any) -> None:
    """Stable async wrapper for telemetry/event logging."""
    impl = _log_event_impl
    if impl is None:
        return
    try:
        out = impl(name, payload, **fields)
        await _maybe_await(out)
    except Exception:
        return


# =============================================================================
# OpenAI optional (bounded micro-nudges only)
# =============================================================================

_ENABLE_AI_TUNING = os.getenv("ENABLE_AI_TUNING", "false").strip().lower() in (
    "1",
    "true",
    "yes",
    "on",
)
_OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini")

try:
    from openai import AsyncOpenAI  # type: ignore[import-not-found]
except Exception:  # pragma: no cover
    AsyncOpenAI = None


# =============================================================================
# Redis / keys / cadence
# =============================================================================

REDIS_URL = os.getenv("REDIS_URL", os.getenv("REDIS_TLS_URL", "redis://localhost:6379"))

PARAMS_KEY = "strategy:params"
TRADES_KEY = "strategy:trades"
STATS_KEY = "strategy:stats"
LAST_TUNE_TS_KEY = "strategy:last_tune_ts"
LAST_LLM_TS_KEY = "strategy:last_llm_ts"
PARAM_HISTORY_KEY = "strategy:param_history"
HEALTH_KEY = "strategy:health"
REINVEST_HISTORY_KEY = "strategy:reinvest_history"

TUNE_LOCK_KEY = "strategy:tune_lock"
TUNE_LOCK_TTL_SEC = int(os.getenv("STRAT_TUNE_LOCK_TTL_SEC", "30"))

LEARNING_INTERVAL_SECONDS = int(os.getenv("LEARNING_INTERVAL_SECONDS", "1800"))  # 30m
LLM_MIN_INTERVAL_SECONDS = int(os.getenv("LLM_MIN_INTERVAL_SECONDS", "3600"))  # 60m

MIN_TRADES_FOR_TUNING = int(os.getenv("MIN_TRADES_FOR_TUNING", "12"))
MIN_TRADES_FOR_LLM = int(os.getenv("STRAT_MIN_TRADES_FOR_LLM", "30"))

MAX_TRADES_KEEP = int(os.getenv("MAX_TRADES_KEEP", "2000"))
MAX_PARAM_HISTORY_KEEP = int(os.getenv("MAX_PARAM_HISTORY_KEEP", "200"))
MAX_REINVEST_HISTORY_KEEP = int(os.getenv("MAX_REINVEST_HISTORY_KEEP", "400"))

RECENT_WINDOW = int(os.getenv("STRAT_RECENT_WINDOW", "30"))

STRAT_REQUIRE_RUNTIME_STABLE = os.getenv(
    "STRAT_REQUIRE_RUNTIME_STABLE", "true"
).strip().lower() in (
    "1",
    "true",
    "yes",
    "on",
)

MAX_STEP_REL = float(os.getenv("STRAT_MAX_STEP_REL", "0.12"))  # 12% per tune
LLM_MAX_REL_MOVE = float(os.getenv("STRAT_LLM_MAX_REL_MOVE", "0.20"))  # ±20% (then step-limited)

PLAN_ENABLED = os.getenv("STRAT_PLAN_ENABLED", "true").strip().lower() in ("1", "true", "yes", "on")
RESPECT_RISK_API_LIMITS = os.getenv("STRAT_RESPECT_RISK_API_LIMITS", "true").strip().lower() in (
    "1",
    "true",
    "yes",
    "on",
)

LOSS_STREAK_SLOWDOWN = int(os.getenv("STRAT_LOSS_STREAK_SLOWDOWN", "3"))
DEFENSIVE_DD_THRESHOLD = float(os.getenv("STRAT_DEFENSIVE_DD_THRESHOLD", "12.0"))

AGGRESSIVE_MIN_WINRATE = float(os.getenv("STRAT_AGG_WINRATE", "0.68"))
AGGRESSIVE_MIN_MEAN = float(os.getenv("STRAT_AGG_MEAN", "18.0"))

HEALTH_HARD_HALT_WR = float(os.getenv("STRAT_HEALTH_HARD_HALT_WR", "0.28"))
HEALTH_HARD_HALT_SHARPE = float(os.getenv("STRAT_HEALTH_HARD_HALT_SHARPE", "-0.4"))

HEALTH_ALERT_MIN_SCORE = float(os.getenv("STRAT_HEALTH_ALERT_MIN_SCORE", "25.0"))
HEALTH_ALERT_INTERVAL_SEC = int(os.getenv("STRAT_HEALTH_ALERT_INTERVAL_SEC", "3600"))

HALT_COOLDOWN_SEC = int(os.getenv("STRAT_HALT_COOLDOWN_SEC", "1800"))  # 30m

SCHEMA_VERSION = 4

__all__ = [
    "RedisLike",
    "get_strategy_params",
    "set_strategy_params",
    "process_trade_closure",
    "get_perf_snapshot",
    "tune_once",
    "run_self_learning_loop",
    "get_health_snapshot",
    "get_plan_snapshot",
    "get_param_history",
    "note_reinvest_decision",
    "note_reinvest_result",
    "note_reinvest_skipped",
    "get_reinvest_history",
    "get_strategy_with_risk",
    "get_full_debug_snapshot",
]

# =============================================================================
# Param schema (hard bounds + defaults + abs-step caps)
# =============================================================================

BOUNDS: dict[str, tuple[float, float]] = {
    "STOP_LOSS_PERCENT": (6.0, 35.0),
    "TARGET_PROFIT_PERCENT": (0.0, 600.0),
    "MAX_HOLD_TIME_MINUTES": (20.0, 1440.0),
    "SLIPPAGE": (0.0015, 0.02),  # 0.15%..2%
    "REINVEST_PERCENT": (5.0, 95.0),
    "MAX_RISK_PER_TRADE": (0.01, 0.12),  # fraction of equity
    "MIN_SOL_BALANCE": (0.05, 3.0),
    "HOLD_IF_SCORE_ABOVE": (0.60, 0.99),
    "MIN_PROFIT_THRESHOLD": (3.0, 30.0),
    "PARTIAL_TP_CHUNKS": (2.0, 8.0),
    "ENTRY_SCORE_GATE": (0.45, 0.90),
    "ENTRY_MAX_PARALLEL": (1.0, 16.0),
    "ENTRY_RISK_BOOST_FACTOR": (0.4, 1.6),
    "EXIT_TAKE_PROFIT_1": (8.0, 60.0),
    "EXIT_TAKE_PROFIT_2": (20.0, 200.0),
    "EXIT_TAKE_PROFIT_3": (40.0, 350.0),
    "EXIT_TP_1_SIZE": (0.05, 0.50),
    "EXIT_TP_2_SIZE": (0.05, 0.60),
    "EXIT_TP_3_SIZE": (0.05, 0.80),
    "PLAN_TARGET_APR_PCT": (20.0, 400.0),
    "PLAN_START_EQUITY_USD": (0.0, 1_000_000_000.0),
    "PLAN_START_TS": (0.0, 4_000_000_000.0),
}

MAX_STEP_ABS: dict[str, float] = {
    "STOP_LOSS_PERCENT": 2.0,
    "TARGET_PROFIT_PERCENT": 35.0,
    "MAX_HOLD_TIME_MINUTES": 120.0,
    "SLIPPAGE": 0.002,
    "REINVEST_PERCENT": 10.0,
    "MAX_RISK_PER_TRADE": 0.015,
    "MIN_SOL_BALANCE": 0.30,
    "HOLD_IF_SCORE_ABOVE": 0.05,
    "MIN_PROFIT_THRESHOLD": 4.0,
    "ENTRY_SCORE_GATE": 0.03,
    "ENTRY_MAX_PARALLEL": 2.0,
    "ENTRY_RISK_BOOST_FACTOR": 0.15,
    "EXIT_TAKE_PROFIT_1": 8.0,
    "EXIT_TAKE_PROFIT_2": 20.0,
    "EXIT_TAKE_PROFIT_3": 35.0,
    "EXIT_TP_1_SIZE": 0.12,
    "EXIT_TP_2_SIZE": 0.12,
    "EXIT_TP_3_SIZE": 0.12,
    "PLAN_TARGET_APR_PCT": 25.0,
}

DEFAULTS: dict[str, Any] = {
    "SCHEMA_VERSION": SCHEMA_VERSION,
    "STOP_LOSS_PERCENT": 18.0,
    "TARGET_PROFIT_PERCENT": 180.0,
    "MAX_HOLD_TIME_MINUTES": 360,
    "SLIPPAGE": 0.005,  # 0.5%
    "REINVEST_PERCENT": 75.0,
    "MAX_RISK_PER_TRADE": 0.05,
    "MIN_SOL_BALANCE": 0.25,
    "TRAILING_STOP_ENABLED": True,
    "HARD_HALT": False,
    "HARD_HALT_TS": 0.0,
    "HOLD_IF_SCORE_ABOVE": 0.86,
    "MIN_PROFIT_THRESHOLD": 10.0,
    "PARTIAL_TP_CHUNKS": 4,
    "ENTRY_SCORE_GATE": 0.58,
    "ENTRY_MAX_PARALLEL": 4,
    "ENTRY_RISK_BOOST_FACTOR": 1.0,
    "EXIT_TAKE_PROFIT_1": 15.0,
    "EXIT_TAKE_PROFIT_2": 60.0,
    "EXIT_TAKE_PROFIT_3": 150.0,
    "EXIT_TP_1_SIZE": 0.25,
    "EXIT_TP_2_SIZE": 0.35,
    "EXIT_TP_3_SIZE": 0.40,
    # 0 DEFENSIVE, 1 NEUTRAL, 2 AGGRESSIVE
    "STRAT_MODE": 1,
    # Plan fields
    "PLAN_TARGET_APR_PCT": float(os.getenv("STRAT_PLAN_TARGET_APR_PCT", "180.0")),
    "PLAN_START_EQUITY_USD": 0.0,
    "PLAN_START_TS": 0.0,
    "PLAN_EQUITY_TODAY_USD": 0.0,
    "PLAN_GAP_PCT": 0.0,
    "PLAN_STATUS": "n/a",
}

# In-memory fallbacks (Redis optional)
_mem_params: dict[str, Any] = dict(DEFAULTS)
_mem_trades: list[dict[str, Any]] = []
_mem_stats: dict[str, Any] = {}
_mem_last_tune_ts: float = 0.0
_mem_last_llm_ts: float = 0.0
_mem_last_health_alert_ts: float = 0.0

# =============================================================================
# Redis client factory (lazy, import-safe)
# =============================================================================

_redis_singleton: RedisLike | None = None
_redis_singleton_lock: asyncio.Lock | None = None


async def _ensure_redis(redis: RedisLike | None) -> RedisLike | None:
    """
    Lazily resolve a Redis client.

    Priority:
      1) Use the passed-in `redis` if provided
      2) Use cached singleton if already created
      3) If project factory exists, try that
      4) Fall back to redis.asyncio.from_url(REDIS_URL)
    """
    global _redis_singleton, _redis_singleton_lock

    if redis is not None:
        return redis

    cached = _redis_singleton
    if cached is not None:
        return cached

    if _redis_singleton_lock is None:
        _redis_singleton_lock = asyncio.Lock()
    lock = _redis_singleton_lock
    assert lock is not None

    async with lock:
        cached2 = _redis_singleton
        if cached2 is not None:
            return cached2

        # Prefer project-level factory if available
        if _project_get_redis_async is not None:
            try:
                r = await _project_get_redis_async()
            except Exception:
                r = None
            if r is not None:
                _redis_singleton = r
                return r

        # Fallback to redis.asyncio if installed
        if _redis_async is None:
            return None

        try:
            r2 = _redis_async.from_url(REDIS_URL, decode_responses=True)
        except Exception:
            return None

        _redis_singleton = cast(RedisLike, r2)
        return _redis_singleton


async def _redis_set(redis: Any, key: str, value: Any, **kwargs: Any) -> Any:
    fn = getattr(redis, "set", None)
    if not callable(fn):
        return None
    out = fn(key, value, **kwargs)
    return await _maybe_await(out)


async def _redis_eval(redis: Any, script: str, numkeys: int, *args: Any) -> Any:
    fn = getattr(redis, "eval", None)
    if not callable(fn):
        return None
    out = fn(script, numkeys, *args)
    return await _maybe_await(out)


# =============================================================================
# Param sanitation + invariants + step limiting
# =============================================================================


def _merge_defaults(override: dict[str, Any]) -> dict[str, Any]:
    out = dict(DEFAULTS)
    out.update(override or {})
    out["SCHEMA_VERSION"] = SCHEMA_VERSION
    return out


def _normalize_tp_sizes(p: dict[str, Any]) -> None:
    s1 = _safe_float(p.get("EXIT_TP_1_SIZE", DEFAULTS["EXIT_TP_1_SIZE"]))
    s2 = _safe_float(p.get("EXIT_TP_2_SIZE", DEFAULTS["EXIT_TP_2_SIZE"]))
    s3 = _safe_float(p.get("EXIT_TP_3_SIZE", DEFAULTS["EXIT_TP_3_SIZE"]))
    total = max(0.0, s1) + max(0.0, s2) + max(0.0, s3)
    if total <= 1.0 + 1e-9 or total <= 1e-9:
        return
    p["EXIT_TP_1_SIZE"] = s1 / total
    p["EXIT_TP_2_SIZE"] = s2 / total
    p["EXIT_TP_3_SIZE"] = s3 / total


def _sanitize_params(params: dict[str, Any]) -> dict[str, Any]:
    p = dict(params or {})
    for k, dv in DEFAULTS.items():
        p.setdefault(k, dv)

    p["SCHEMA_VERSION"] = SCHEMA_VERSION

    for k, (lo, hi) in BOUNDS.items():
        p[k] = _clamp(_safe_float(p.get(k, DEFAULTS.get(k, lo))), lo, hi)

    p["PARTIAL_TP_CHUNKS"] = _safe_int(
        p.get("PARTIAL_TP_CHUNKS", DEFAULTS["PARTIAL_TP_CHUNKS"]),
        int(DEFAULTS["PARTIAL_TP_CHUNKS"]),
    )
    p["MAX_HOLD_TIME_MINUTES"] = _safe_int(
        p.get("MAX_HOLD_TIME_MINUTES", DEFAULTS["MAX_HOLD_TIME_MINUTES"]),
        int(DEFAULTS["MAX_HOLD_TIME_MINUTES"]),
    )
    p["ENTRY_MAX_PARALLEL"] = _safe_int(
        p.get("ENTRY_MAX_PARALLEL", DEFAULTS["ENTRY_MAX_PARALLEL"]),
        int(DEFAULTS["ENTRY_MAX_PARALLEL"]),
    )

    p["TRAILING_STOP_ENABLED"] = _parse_bool(p.get("TRAILING_STOP_ENABLED", True), True)
    p["HARD_HALT"] = _parse_bool(p.get("HARD_HALT", False), False)

    mode = _safe_int(p.get("STRAT_MODE", DEFAULTS["STRAT_MODE"]), int(DEFAULTS["STRAT_MODE"]))
    p["STRAT_MODE"] = 0 if mode < 0 else 2 if mode > 2 else mode
    p["HARD_HALT_TS"] = _safe_float(p.get("HARD_HALT_TS", 0.0), 0.0)

    _normalize_tp_sizes(p)
    for k in ("EXIT_TP_1_SIZE", "EXIT_TP_2_SIZE", "EXIT_TP_3_SIZE"):
        lo, hi = BOUNDS[k]
        p[k] = _clamp(_safe_float(p.get(k, DEFAULTS[k]), DEFAULTS[k]), lo, hi)

    return p


def _limit_step(old: dict[str, Any], new: dict[str, Any]) -> dict[str, Any]:
    out = dict(new)

    for k, (lo, hi) in BOUNDS.items():
        if k not in out:
            continue

        cur = _safe_float(old.get(k, DEFAULTS.get(k, lo)), DEFAULTS.get(k, lo))
        nxt = _safe_float(out.get(k, cur), cur)

        abs_cap = MAX_STEP_ABS.get(k)
        if abs_cap is not None:
            delta = nxt - cur
            if delta > abs_cap:
                nxt = cur + abs_cap
            elif delta < -abs_cap:
                nxt = cur - abs_cap

        if MAX_STEP_REL > 0 and abs(cur) > 1e-12:
            rel_cap = abs(cur) * MAX_STEP_REL
            delta = nxt - cur
            if delta > rel_cap:
                nxt = cur + rel_cap
            elif delta < -rel_cap:
                nxt = cur - rel_cap

        out[k] = _clamp(nxt, lo, hi)

    out["TRAILING_STOP_ENABLED"] = _parse_bool(
        out.get("TRAILING_STOP_ENABLED", old.get("TRAILING_STOP_ENABLED", True)), True
    )
    out["HARD_HALT"] = _parse_bool(out.get("HARD_HALT", old.get("HARD_HALT", False)), False)

    mode = _safe_int(out.get("STRAT_MODE", old.get("STRAT_MODE", 1)), int(old.get("STRAT_MODE", 1)))
    out["STRAT_MODE"] = 0 if mode < 0 else 2 if mode > 2 else mode

    out["HARD_HALT_TS"] = _safe_float(
        out.get("HARD_HALT_TS", old.get("HARD_HALT_TS", 0.0)),
        _safe_float(old.get("HARD_HALT_TS", 0.0)),
    )

    return _sanitize_params(out)


# =============================================================================
# Public params API
# =============================================================================


async def get_strategy_params(redis: RedisLike | None = None) -> dict[str, Any]:
    try:
        r = await _ensure_redis(redis)
        if r is not None:
            raw = await r.get(PARAMS_KEY)
            if raw:
                parsed = _loads_dict(raw)
                return _sanitize_params(_merge_defaults(parsed))
        return _sanitize_params(dict(_mem_params))
    except Exception:
        return _sanitize_params(dict(_mem_params))


async def set_strategy_params(redis: RedisLike | None, params: dict[str, Any]) -> None:
    cleaned = _sanitize_params(_merge_defaults(params or {}))
    _mem_params.update(cleaned)
    try:
        r = await _ensure_redis(redis)
        if r is not None:
            await r.set(PARAMS_KEY, _dumps(cleaned))
    except Exception:
        pass


# =============================================================================
# Trades & Stats
# =============================================================================


async def _read_trades(redis: RedisLike | None, limit: int) -> list[dict[str, Any]]:
    lim = max(0, int(limit))
    if lim <= 0:
        return []
    try:
        if redis is not None:
            raw = await redis.lrange(TRADES_KEY, 0, lim - 1)
            items: list[Any]
            if isinstance(raw, list):
                items = raw
            else:
                items = []
            out: list[dict[str, Any]] = []
            for x in items:
                v = _loads_obj(x)
                if isinstance(v, dict):
                    out.append(v)
            return out
        return list(_mem_trades[:lim])
    except Exception:
        return []


async def _store_stats(redis: RedisLike | None, stats: dict[str, Any]) -> None:
    _mem_stats.update(stats or {})
    try:
        if redis is not None:
            await redis.set(STATS_KEY, _dumps(stats))
    except Exception:
        pass


async def _recompute_and_store_stats(redis: RedisLike | None) -> None:
    trades = await _read_trades(redis, MAX_TRADES_KEEP)
    if not trades:
        await _store_stats(
            redis,
            {
                "trades": 0,
                "wins": 0,
                "losses": 0,
                "win_rate": 0.0,
                "avg_profit": 0.0,
                "avg_loss": 0.0,
                "pnl_mean": 0.0,
                "pnl_tmean": 0.0,
                "pnl_median": 0.0,
                "pnl_std": 0.0,
                "pnl_downside_dev": 0.0,
                "sharpe_like": 0.0,
                "recent_win_rate": 0.0,
                "recent_sharpe_like": 0.0,
                "profit_factor": 0.0,
                "recent_profit_factor": 0.0,
                "equity_cum_return_pct": 0.0,
                "equity_max_dd_pct": 0.0,
                "tag_counts": {},
                "recent_tag_counts": {},
                "current_win_streak": 0,
                "current_loss_streak": 0,
                "kelly_suggestion": 0.02,
                "recent_kelly_suggestion": 0.02,
                "kelly_confidence": 0.0,
            },
        )
        return

    params = await get_strategy_params(redis)
    win_cut = float(params.get("MIN_PROFIT_THRESHOLD", DEFAULTS["MIN_PROFIT_THRESHOLD"]))

    pnl = [float(t.get("profit_pct", 0.0)) for t in trades]
    sorted_pnl = sorted(pnl)

    mu = _safe_mean(pnl)
    tmu = _trimmed_mean(pnl, trim=0.10)
    s = _std(pnl)
    ddev = _downside_deviation(pnl, threshold=0.0)
    sharpe = _sharpe_like(tmu, s)

    if sorted_pnl:
        mid = len(sorted_pnl) // 2
        median_val = (
            float(sorted_pnl[mid])
            if (len(sorted_pnl) % 2)
            else float((sorted_pnl[mid - 1] + sorted_pnl[mid]) / 2.0)
        )
    else:
        median_val = 0.0

    wins = [x for x in pnl if x >= win_cut]
    losses = [x for x in pnl if x < win_cut]

    total_profit = sum(x for x in pnl if x > 0)
    total_loss = sum(x for x in pnl if x < 0)
    profit_factor = float(total_profit / abs(total_loss)) if total_loss < 0 else 0.0

    recent = pnl[: min(RECENT_WINDOW, len(pnl))]
    rmu = _trimmed_mean(recent, trim=0.10) if recent else 0.0
    rs = _std(recent) if recent else 0.0
    rsharpe = _sharpe_like(rmu, rs)

    recent_profit = sum(x for x in recent if x > 0)
    recent_loss = sum(x for x in recent if x < 0)
    recent_profit_factor = float(recent_profit / abs(recent_loss)) if recent_loss < 0 else 0.0

    def _kelly(m: float, stdv: float) -> float:
        if stdv <= 1e-9:
            return 0.02
        edge = m / 100.0
        var_r = (stdv / 100.0) ** 2
        raw = edge / max(var_r, 1e-9)
        return float(max(0.0, min(raw, 0.25)))

    kelly_all = _kelly(tmu, s)
    kelly_recent = _kelly(rmu, rs)

    n = len(pnl)
    f_n = max(0.0, min(1.0, n / 400.0))
    s_clamped = max(-0.5, min(1.5, sharpe))
    rs_clamped = max(-0.5, min(1.5, rsharpe))
    f_s = (s_clamped + 0.5) / 2.0
    f_rs = (rs_clamped + 0.5) / 2.0
    k_conf = float(max(0.0, min(1.0, f_n * (0.4 * f_s + 0.6 * f_rs))))

    tag_counts: dict[str, int] = {}
    recent_tag_counts: dict[str, int] = {}
    for idx, t in enumerate(trades[: min(len(trades), 300)]):
        p = float(t.get("profit_pct", 0.0))
        hm = t.get("hold_minutes")
        hold_m = float(hm) if hm is not None else None
        tag = _classify_trade(p, hold_m)
        tag_counts[tag] = tag_counts.get(tag, 0) + 1
        if idx < RECENT_WINDOW:
            recent_tag_counts[tag] = recent_tag_counts.get(tag, 0) + 1

    eq = _equity_curve_metrics(pnl)
    win_streak, loss_streak = _current_streaks(pnl, win_cut)

    stats = {
        "trades": n,
        "wins": len(wins),
        "losses": len(losses),
        "win_rate": (len(wins) / n) if n else 0.0,
        "avg_profit": _safe_mean(wins),
        "avg_loss": _safe_mean(losses),
        "pnl_mean": mu,
        "pnl_tmean": tmu,
        "pnl_median": median_val,
        "pnl_std": s,
        "pnl_downside_dev": ddev,
        "sharpe_like": sharpe,
        "recent_win_rate": (len([x for x in recent if x >= win_cut]) / len(recent))
        if recent
        else 0.0,
        "recent_sharpe_like": rsharpe,
        "profit_factor": profit_factor,
        "recent_profit_factor": recent_profit_factor,
        "equity_cum_return_pct": eq["equity_cum_return_pct"],
        "equity_max_dd_pct": eq["equity_max_dd_pct"],
        "tag_counts": tag_counts,
        "recent_tag_counts": recent_tag_counts,
        "current_win_streak": win_streak,
        "current_loss_streak": loss_streak,
        "kelly_suggestion": kelly_all,
        "recent_kelly_suggestion": kelly_recent,
        "kelly_confidence": k_conf,
    }
    await _store_stats(redis, stats)


async def get_perf_snapshot(redis: RedisLike | None = None) -> dict[str, Any]:
    try:
        r = await _ensure_redis(redis)
        if r is not None:
            raw = await r.get(STATS_KEY)
            if raw:
                v = _loads_dict(raw)
                return v
        return dict(_mem_stats)
    except Exception:
        return dict(_mem_stats)


async def process_trade_closure(
    token_address: str,
    buy_price: float,
    sell_price: float,
    amount: float,
    partial: bool,
    timestamp: str,
    *,
    entry_score: float | None = None,
    hold_minutes: float | None = None,
    meta: dict[str, Any] | None = None,
    profit_usd: float | None = None,
    equity_after_usd: float | None = None,
    redis: RedisLike | None = None,
    **_ignored: Any,
) -> None:
    """
    Record a trade closure (full or partial) and refresh stats.
    Accepts extra keyword fields for forward compatibility.
    """
    try:
        if buy_price <= 0 or amount <= 0:
            return

        profit_pct = ((sell_price - buy_price) / buy_price) * 100.0
        rec: dict[str, Any] = {
            "token": str(token_address),
            "profit_pct": float(profit_pct),
            "amount": float(amount),
            "partial": bool(partial),
            "timestamp": str(timestamp),
            "tag": _classify_trade(profit_pct, hold_minutes),
        }
        if entry_score is not None:
            rec["entry_score"] = float(entry_score)
        if hold_minutes is not None:
            rec["hold_minutes"] = float(hold_minutes)
        if meta:
            rec["meta"] = dict(meta)
        if profit_usd is not None:
            rec["profit_usd"] = float(profit_usd)
        if equity_after_usd is not None:
            rec["equity_after_usd"] = float(equity_after_usd)

        r = await _ensure_redis(redis)
        if r is not None:
            await r.lpush(TRADES_KEY, _dumps(rec))
            await r.ltrim(TRADES_KEY, 0, MAX_TRADES_KEEP - 1)
            await _recompute_and_store_stats(r)
        else:
            _mem_trades.insert(0, rec)
            del _mem_trades[MAX_TRADES_KEEP:]
            await _recompute_and_store_stats(None)

        _metric("selflearn.trade_closure", 1)
        with contextlib.suppress(Exception):
            await log_event("trade_closed", rec)
    except Exception as e:  # pragma: no cover
        with contextlib.suppress(Exception):
            _stdlog.warning("process_trade_closure failed: %s", e)


# =============================================================================
# Risk context coupling (risk_manager -> strategy)
# =============================================================================


async def _read_risk_ctx(redis: RedisLike | None) -> dict[str, Any]:
    base: dict[str, Any] = {
        "runtime_stable": True,
        "risk_capacity": 1.0,
        "risk_mode": 1,
        "daily_realized_pnl_pct": 0.0,
        "daily_drawdown_pct": 0.0,
        "open_positions": 0,
        "max_positions": 0,
        "equity_usd": 0.0,
        "regime_label_numeric": 0.0,
        "regime_stress_score": 0.0,
        "regime_signal_strength": 0.0,
        "regime_entry_gate_factor": 1.0,
        "plan_gap_pct": 0.0,
        "plan_status": "n/a",
        "risk_profile": "balanced",
        "max_risk_per_trade": 0.0,
        "min_sol_balance": 0.0,
        "hard_halt": False,
    }
    if redis is None:
        return base

    try:
        h_raw = await redis.hgetall("risk:ctx")
        h = _to_dict_str_any(h_raw)
        if not h:
            return base

        base["runtime_stable"] = _parse_bool(h.get("runtime_stable"), True)
        base["risk_capacity"] = _safe_float(h.get("risk_capacity", 1.0), 1.0)
        base["risk_mode"] = _safe_int(h.get("risk_mode", 1), 1)
        base["daily_realized_pnl_pct"] = _safe_float(h.get("daily_realized_pnl_pct", 0.0), 0.0)
        base["daily_drawdown_pct"] = _safe_float(h.get("daily_drawdown_pct", 0.0), 0.0)
        base["open_positions"] = _safe_int(h.get("open_positions", 0), 0)
        base["max_positions"] = _safe_int(h.get("max_positions", 0), 0)
        base["equity_usd"] = _safe_float(h.get("equity_usd", 0.0), 0.0)

        base["regime_label_numeric"] = _safe_float(h.get("regime_label_numeric", 0.0), 0.0)
        base["regime_stress_score"] = _safe_float(h.get("regime_stress_score", 0.0), 0.0)
        base["regime_signal_strength"] = _safe_float(h.get("regime_signal_strength", 0.0), 0.0)
        base["regime_entry_gate_factor"] = _safe_float(h.get("regime_entry_gate_factor", 1.0), 1.0)

        base["plan_gap_pct"] = _safe_float(h.get("plan_gap_pct", 0.0), 0.0)
        base["plan_status"] = str(h.get("plan_status", "n/a"))
        base["risk_profile"] = str(h.get("risk_profile", "balanced"))

        base["max_risk_per_trade"] = _safe_float(h.get("max_risk_per_trade", 0.0), 0.0)
        base["min_sol_balance"] = _safe_float(h.get("min_sol_balance", 0.0), 0.0)

        base["hard_halt"] = _parse_bool(h.get("hard_halt"), False)
        return base
    except Exception:
        return base


# =============================================================================
# Tuning core (rules + plan + optional LLM)
# =============================================================================


def _rule_based_adjust(
    params: dict[str, Any], stats: dict[str, Any], risk_ctx: dict[str, Any]
) -> tuple[dict[str, Any], str]:
    out = dict(params)
    reason_parts: list[str] = []

    win_rate = float(stats.get("win_rate", 0.0))
    recent_wr = float(stats.get("recent_win_rate", 0.0))
    tmean = float(stats.get("pnl_tmean", stats.get("pnl_mean", 0.0)))
    stdv = float(stats.get("pnl_std", 0.0))
    ddev = float(stats.get("pnl_downside_dev", 0.0))
    sharpe = float(stats.get("sharpe_like", 0.0))
    equity_max_dd = float(stats.get("equity_max_dd_pct", 0.0))
    profit_factor = float(stats.get("profit_factor", 0.0))
    recent_pf = float(stats.get("recent_profit_factor", 0.0))
    cur_win_streak = int(stats.get("current_win_streak", 0))
    cur_loss_streak = int(stats.get("current_loss_streak", 0))
    kelly = float(stats.get("kelly_suggestion", 0.02))
    k_conf = float(stats.get("kelly_confidence", 0.0))

    risk_capacity = float(risk_ctx.get("risk_capacity", 1.0))
    risk_mode = int(risk_ctx.get("risk_mode", 1))
    daily_pnl_pct = float(risk_ctx.get("daily_realized_pnl_pct", 0.0))
    daily_dd_pct = float(risk_ctx.get("daily_drawdown_pct", 0.0))
    open_positions = int(risk_ctx.get("open_positions", 0))
    max_positions = int(risk_ctx.get("max_positions", 0))

    regime_label = float(risk_ctx.get("regime_label_numeric", 0.0))
    regime_stress = float(risk_ctx.get("regime_stress_score", 0.0))
    regime_signal = float(risk_ctx.get("regime_signal_strength", 0.0))
    regime_gate = float(risk_ctx.get("regime_entry_gate_factor", 1.0))

    plan_gap_pct = float(risk_ctx.get("plan_gap_pct", 0.0))

    if bool(risk_ctx.get("hard_halt", False)) or risk_capacity <= 0.0:
        out["HARD_HALT"] = True
        out["STRAT_MODE"] = 0
        out["HARD_HALT_TS"] = _now()
        return _sanitize_params(out), "risk_hard_halt"

    now = _now()
    prev_halt = bool(out.get("HARD_HALT", False))
    last_halt_ts = float(out.get("HARD_HALT_TS", 0.0) or 0.0)
    if prev_halt and (now - last_halt_ts) < HALT_COOLDOWN_SEC:
        if not (
            risk_capacity > 0.60
            and recent_wr > 0.50
            and sharpe > 0.05
            and daily_dd_pct > -DEFENSIVE_DD_THRESHOLD / 2.0
        ):
            out["HARD_HALT"] = True
            out["STRAT_MODE"] = 0
            return _sanitize_params(out), "halt_cooldown"

    if win_rate < HEALTH_HARD_HALT_WR and sharpe < HEALTH_HARD_HALT_SHARPE and risk_capacity < 0.40:
        out["HARD_HALT"] = True
        out["STRAT_MODE"] = 0
        out["HARD_HALT_TS"] = now
        return _sanitize_params(out), "health_catastrophic_halt"

    losing_regime = (
        win_rate < 0.45
        or recent_wr < 0.40
        or ddev > 15.0
        or daily_dd_pct <= -8.0
        or sharpe < 0.0
        or regime_stress > 0.70
        or regime_label < -0.40
        or equity_max_dd <= -25.0
        or recent_pf < 1.0
        or risk_capacity < 0.55
        or risk_mode in (0, 3)
    )
    extreme_bad = (
        daily_dd_pct <= -DEFENSIVE_DD_THRESHOLD
        or risk_capacity < 0.35
        or regime_label <= -0.70
        or sharpe < -0.30
        or equity_max_dd <= -35.0
        or daily_pnl_pct <= -15.0
    )

    mode = int(out.get("STRAT_MODE", 1))

    if losing_regime:
        reason_parts.append("losing_regime")
        mode = 0

        out["STOP_LOSS_PERCENT"] = float(out["STOP_LOSS_PERCENT"]) - 1.2
        out["SLIPPAGE"] = float(out["SLIPPAGE"]) - 0.0004
        if float(out.get("TARGET_PROFIT_PERCENT", 0.0)) > 90.0:
            out["TARGET_PROFIT_PERCENT"] = float(out["TARGET_PROFIT_PERCENT"]) - 12.0

        out["MAX_RISK_PER_TRADE"] = float(out["MAX_RISK_PER_TRADE"]) - 0.006
        out["REINVEST_PERCENT"] = float(out["REINVEST_PERCENT"]) - 5.0

        out["ENTRY_SCORE_GATE"] = float(out["ENTRY_SCORE_GATE"]) + 0.010
        out["ENTRY_MAX_PARALLEL"] = float(out["ENTRY_MAX_PARALLEL"]) - 1.0
        out["ENTRY_RISK_BOOST_FACTOR"] = float(out["ENTRY_RISK_BOOST_FACTOR"]) - 0.06

        if cur_loss_streak >= LOSS_STREAK_SLOWDOWN:
            reason_parts.append(f"loss_streak={cur_loss_streak}")
            out["MAX_RISK_PER_TRADE"] = float(out["MAX_RISK_PER_TRADE"]) - 0.006
            out["REINVEST_PERCENT"] = float(out["REINVEST_PERCENT"]) - 3.0
            out["ENTRY_SCORE_GATE"] = float(out["ENTRY_SCORE_GATE"]) + 0.008

    if extreme_bad:
        reason_parts.append("extreme_bad")
        mode = 0
        out["MAX_RISK_PER_TRADE"] = float(out["MAX_RISK_PER_TRADE"]) - 0.010
        out["REINVEST_PERCENT"] = float(out["REINVEST_PERCENT"]) - 8.0
        out["ENTRY_SCORE_GATE"] = float(out["ENTRY_SCORE_GATE"]) + 0.020
        out["ENTRY_MAX_PARALLEL"] = float(out["ENTRY_MAX_PARALLEL"]) - 2.0
        out["ENTRY_RISK_BOOST_FACTOR"] = float(out["ENTRY_RISK_BOOST_FACTOR"]) - 0.10

        if daily_dd_pct <= -2 * DEFENSIVE_DD_THRESHOLD or recent_wr < 0.25:
            out["HARD_HALT"] = True
            out["HARD_HALT_TS"] = now
            reason_parts.append("hard_halt_triggered")

    strong_regime = (
        win_rate > AGGRESSIVE_MIN_WINRATE
        and tmean > AGGRESSIVE_MIN_MEAN
        and stdv < 35.0
        and ddev < 12.0
        and daily_dd_pct > -5.0
        and sharpe > 0.30
        and regime_label > 0.30
        and regime_signal > 0.40
        and regime_stress < 0.50
        and profit_factor > 1.40
        and risk_capacity > 0.75
        and not losing_regime
        and not extreme_bad
    )
    if plan_gap_pct > 10.0:
        strong_regime = False

    if strong_regime:
        reason_parts.append("strong_regime")
        mode = 2

        out["STOP_LOSS_PERCENT"] = float(out["STOP_LOSS_PERCENT"]) + 0.5
        out["REINVEST_PERCENT"] = float(out["REINVEST_PERCENT"]) + 3.0
        out["SLIPPAGE"] = float(out["SLIPPAGE"]) + 0.0002

        out["ENTRY_SCORE_GATE"] = float(out["ENTRY_SCORE_GATE"]) - 0.006
        out["ENTRY_MAX_PARALLEL"] = float(out["ENTRY_MAX_PARALLEL"]) + 1.0
        out["ENTRY_RISK_BOOST_FACTOR"] = float(out["ENTRY_RISK_BOOST_FACTOR"]) + 0.06

        out["HARD_HALT"] = False

        if cur_win_streak >= 3 and daily_dd_pct > -3.0:
            reason_parts.append(f"win_streak={cur_win_streak}")
            out["REINVEST_PERCENT"] = float(out["REINVEST_PERCENT"]) + 2.0
            out["ENTRY_SCORE_GATE"] = float(out["ENTRY_SCORE_GATE"]) - 0.004

    if (
        plan_gap_pct < -8.0
        and not losing_regime
        and risk_capacity > 0.70
        and sharpe > 0.0
        and daily_dd_pct > -DEFENSIVE_DD_THRESHOLD
        and regime_stress < 0.60
    ):
        reason_parts.append("plan_catchup_bias")
        mode = max(mode, 1)
        out["REINVEST_PERCENT"] = float(out["REINVEST_PERCENT"]) + 2.0
        out["ENTRY_MAX_PARALLEL"] = float(out["ENTRY_MAX_PARALLEL"]) + 1.0
        out["ENTRY_SCORE_GATE"] = float(out["ENTRY_SCORE_GATE"]) - 0.003

    if tmean < 5.0 and float(out["MAX_HOLD_TIME_MINUTES"]) > 60.0:
        reason_parts.append("weak_mean_reduce_hold")
        out["MAX_HOLD_TIME_MINUTES"] = float(out["MAX_HOLD_TIME_MINUTES"]) - 30.0

    if k_conf > 0.0:
        target = max(0.01, min(kelly * 0.70, 0.10))
        if risk_capacity < 0.50 or daily_dd_pct <= -6.0 or risk_mode in (0, 3):
            target *= 0.65
        if regime_label < -0.20 or regime_stress > 0.60:
            target *= 0.75
        elif regime_label > 0.40 and regime_signal > 0.40 and regime_stress < 0.40:
            target *= 1.08

        if max_positions > 0 and open_positions >= max_positions:
            target = min(target, float(out["MAX_RISK_PER_TRADE"]))

        cur = float(out["MAX_RISK_PER_TRADE"])
        out["MAX_RISK_PER_TRADE"] = cur + (0.08 * k_conf) * (target - cur)
        reason_parts.append("kelly_nudge")

    if 0.1 <= regime_gate <= 2.0:
        desired = float(out["ENTRY_SCORE_GATE"])
        if regime_gate > 1.05:
            desired = desired - 0.01 * (regime_gate - 1.0)
        elif regime_gate < 0.95:
            desired = desired + 0.01 * (1.0 - regime_gate)

        cur_gate = float(out["ENTRY_SCORE_GATE"])
        out["ENTRY_SCORE_GATE"] = cur_gate + 0.12 * (desired - cur_gate)
        reason_parts.append("regime_gate_align")

    out["STRAT_MODE"] = 0 if mode < 0 else 2 if mode > 2 else mode
    return _sanitize_params(out), (";".join(reason_parts) if reason_parts else "no_change_reason")


def _enforce_risk_api_contract(params: dict[str, Any], risk_ctx: dict[str, Any]) -> dict[str, Any]:
    if not RESPECT_RISK_API_LIMITS:
        return _sanitize_params(params)

    out = dict(params)

    api_max = _safe_float(risk_ctx.get("max_risk_per_trade", 0.0), 0.0)
    if api_max > 0.0:
        lo, hi = BOUNDS["MAX_RISK_PER_TRADE"]
        hi2 = max(lo, min(hi, api_max))
        out["MAX_RISK_PER_TRADE"] = _clamp(
            min(float(out.get("MAX_RISK_PER_TRADE", DEFAULTS["MAX_RISK_PER_TRADE"])), api_max),
            lo,
            hi2,
        )

    api_floor = _safe_float(risk_ctx.get("min_sol_balance", 0.0), 0.0)
    if api_floor > 0.0:
        lo, hi = BOUNDS["MIN_SOL_BALANCE"]
        out["MIN_SOL_BALANCE"] = _clamp(
            max(float(out.get("MIN_SOL_BALANCE", DEFAULTS["MIN_SOL_BALANCE"])), api_floor),
            lo,
            hi,
        )

    if bool(risk_ctx.get("hard_halt", False)):
        out["HARD_HALT"] = True
        out["STRAT_MODE"] = 0
        out["HARD_HALT_TS"] = _now()

    return _sanitize_params(out)


def _update_equity_plan(params: dict[str, Any], risk_ctx: dict[str, Any]) -> dict[str, Any]:
    out = dict(params)
    equity = float(risk_ctx.get("equity_usd", 0.0) or 0.0)

    if not PLAN_ENABLED:
        out["PLAN_EQUITY_TODAY_USD"] = equity
        out["PLAN_GAP_PCT"] = 0.0
        out["PLAN_STATUS"] = "plan_disabled"
        return out

    profile = str(risk_ctx.get("risk_profile", "balanced")).lower()
    apr = float(out.get("PLAN_TARGET_APR_PCT", DEFAULTS["PLAN_TARGET_APR_PCT"]))
    if profile in ("ultra_defensive", "defensive", "conservative"):
        apr *= 0.85
    elif profile in ("aggressive", "degenerate"):
        apr *= 1.10
    lo_apr, hi_apr = BOUNDS["PLAN_TARGET_APR_PCT"]
    apr = _clamp(apr, lo_apr, hi_apr)
    out["PLAN_TARGET_APR_PCT"] = apr

    now = _now()
    start_eq = float(out.get("PLAN_START_EQUITY_USD", 0.0) or 0.0)
    start_ts = float(out.get("PLAN_START_TS", 0.0) or 0.0)

    if equity > 0.0 and (start_eq <= 0.0 or start_ts <= 0.0):
        out["PLAN_START_EQUITY_USD"] = equity
        out["PLAN_START_TS"] = now
        start_eq, start_ts = equity, now

    if equity <= 0.0 or start_eq <= 0.0 or start_ts <= 0.0:
        out["PLAN_EQUITY_TODAY_USD"] = equity
        out["PLAN_GAP_PCT"] = 0.0
        out["PLAN_STATUS"] = "unknown"
        return out

    days = max(0.0, (now - start_ts) / 86400.0)
    growth = 1.0 + apr / 100.0
    target = start_eq * (growth ** (days / 365.0))
    gap = ((equity - target) / target * 100.0) if target > 1e-9 else 0.0

    out["PLAN_EQUITY_TODAY_USD"] = equity
    out["PLAN_GAP_PCT"] = float(gap)
    out["PLAN_STATUS"] = "ahead" if gap > 3.0 else "behind" if gap < -3.0 else "on_track"
    return out


async def _maybe_llm_adjust(
    params: dict[str, Any], stats: dict[str, Any], risk_ctx: dict[str, Any], redis: RedisLike | None
) -> tuple[dict[str, Any], str]:
    global _mem_last_llm_ts

    if not (_ENABLE_AI_TUNING and AsyncOpenAI is not None):
        return params, "llm_disabled"

    try:
        now = _now()
        if redis is not None:
            raw = await redis.get(LAST_LLM_TS_KEY)
            last_ts = _safe_float(raw, 0.0)
        else:
            last_ts = _mem_last_llm_ts

        if now - last_ts < LLM_MIN_INTERVAL_SECONDS:
            return params, "llm_cooldown"
        if int(stats.get("trades", 0)) < max(MIN_TRADES_FOR_LLM, MIN_TRADES_FOR_TUNING, 8):
            return params, "llm_not_enough_trades"

        prompt = (
            "You are a systematic risk optimizer for a Solana microcap sniper strategy.\n"
            "Propose SMALL numeric adjustments to current params.\n\n"
            "Rules:\n"
            " - Return ONLY JSON {param_name: new_value} (no prose).\n"
            " - Only touch keys that already exist in params.\n"
            " - NEVER touch STRAT_MODE, booleans, or any PLAN_* fields.\n"
            " - Values MUST stay within bounds:\n"
            f"{json.dumps(BOUNDS, indent=2)}\n"
            f" - AND within ±{LLM_MAX_REL_MOVE * 100:.0f}% of current value.\n"
            " - Optimize for risk-adjusted return (Sharpe-like), not raw PnL.\n\n"
            f"params={json.dumps(params, indent=2)}\n"
            f"stats={json.dumps(stats, indent=2)}\n"
            f"risk_ctx={json.dumps(risk_ctx, indent=2)}\n"
        )

        client = AsyncOpenAI()
        resp = await client.chat.completions.create(
            model=_OPENAI_MODEL,
            messages=[{"role": "user", "content": prompt}],
            temperature=0.15,
            max_tokens=350,
        )
        text = (resp.choices[0].message.content or "").strip()

        try:
            proposed = json.loads(text)
        except Exception:
            return params, "llm_bad_json"
        if not isinstance(proposed, dict):
            return params, "llm_not_dict"

        filtered: dict[str, Any] = {}
        for k, v in proposed.items():
            if not isinstance(k, str):
                continue
            if k not in params:
                continue
            if k == "STRAT_MODE" or k.startswith("PLAN_"):
                continue
            if isinstance(DEFAULTS.get(k), bool):
                continue
            if k not in BOUNDS:
                continue

            v_f = _safe_float(v, 0.0)
            cur = float(params.get(k, DEFAULTS.get(k, v_f)))
            if abs(cur) > 1e-12 and LLM_MAX_REL_MOVE > 0:
                v_f = max(cur * (1.0 - LLM_MAX_REL_MOVE), min(v_f, cur * (1.0 + LLM_MAX_REL_MOVE)))

            lo, hi = BOUNDS[k]
            filtered[k] = _clamp(v_f, lo, hi)

        if not filtered:
            return params, "llm_no_valid_suggestions"

        merged = _sanitize_params({**params, **filtered})

        if redis is not None:
            await redis.set(LAST_LLM_TS_KEY, str(now))
        else:
            _mem_last_llm_ts = now

        return merged, "llm_applied"
    except Exception:
        return params, "llm_exception"


# =============================================================================
# Health + history
# =============================================================================


def _health_score(stats: dict[str, Any], risk_ctx: dict[str, Any]) -> float:
    wr = float(stats.get("win_rate", 0.0))
    sharpe = float(stats.get("sharpe_like", 0.0))
    dd = float(risk_ctx.get("daily_drawdown_pct", 0.0))
    cap = float(risk_ctx.get("risk_capacity", 1.0))
    eq_dd = float(stats.get("equity_max_dd_pct", 0.0))
    plan_gap = float(risk_ctx.get("plan_gap_pct", 0.0))

    wr_score = max(0.0, min(1.0, (wr - 0.30) / 0.40)) * 100.0
    s_clamped = max(-0.5, min(1.5, sharpe))
    sharpe_score = (s_clamped + 0.5) / 2.0 * 100.0
    dd_score = max(0.0, min(1.0, (30.0 + dd) / 30.0)) * 100.0
    cap_score = max(0.0, min(1.0, cap)) * 100.0
    eq_dd_score = max(0.0, min(1.0, (50.0 + eq_dd) / 50.0)) * 100.0

    if abs(plan_gap) <= 5.0:
        plan_score = 100.0
    elif plan_gap < -5.0:
        plan_score = max(0.0, min(1.0, (50.0 + plan_gap) / 45.0)) * 80.0
    else:
        plan_score = max(0.0, min(1.0, (50.0 - plan_gap) / 45.0)) * 40.0 + 60.0

    return float(
        0.27 * wr_score
        + 0.23 * sharpe_score
        + 0.18 * dd_score
        + 0.14 * cap_score
        + 0.08 * eq_dd_score
        + 0.10 * plan_score
    )


async def _record_param_history(
    redis: RedisLike | None,
    diffs: dict[str, tuple[Any, Any]],
    stats: dict[str, Any],
    risk_ctx: dict[str, Any],
    reason: str,
) -> None:
    if not diffs:
        return

    entry = {
        "ts": _now(),
        "reason": reason,
        "diffs": diffs,
        "trades": int(stats.get("trades", 0)),
        "win_rate": float(stats.get("win_rate", 0.0)),
        "pnl_tmean": float(stats.get("pnl_tmean", stats.get("pnl_mean", 0.0))),
        "pnl_std": float(stats.get("pnl_std", 0.0)),
        "sharpe_like": float(stats.get("sharpe_like", 0.0)),
        "risk_capacity": float(risk_ctx.get("risk_capacity", 1.0)),
        "risk_mode": int(risk_ctx.get("risk_mode", 1)),
        "daily_realized_pnl_pct": float(risk_ctx.get("daily_realized_pnl_pct", 0.0)),
        "daily_drawdown_pct": float(risk_ctx.get("daily_drawdown_pct", 0.0)),
        "regime_label_numeric": float(risk_ctx.get("regime_label_numeric", 0.0)),
        "regime_stress_score": float(risk_ctx.get("regime_stress_score", 0.0)),
        "regime_signal_strength": float(risk_ctx.get("regime_signal_strength", 0.0)),
        "equity_usd": float(risk_ctx.get("equity_usd", 0.0)),
        "profit_factor": float(stats.get("profit_factor", 0.0)),
        "current_win_streak": int(stats.get("current_win_streak", 0)),
        "current_loss_streak": int(stats.get("current_loss_streak", 0)),
    }

    try:
        if redis is not None:
            await redis.lpush(PARAM_HISTORY_KEY, _dumps(entry))
            await redis.ltrim(PARAM_HISTORY_KEY, 0, MAX_PARAM_HISTORY_KEEP - 1)
    except Exception:
        pass


async def _update_health(
    redis: RedisLike | None, stats: dict[str, Any], risk_ctx: dict[str, Any]
) -> None:
    global _mem_last_health_alert_ts

    score = _health_score(stats, risk_ctx)
    params = await get_strategy_params(redis)

    health: dict[str, Any] = {
        "ts": _now(),
        "health_score": score,
        "hard_halt": bool(params.get("HARD_HALT", False)),
        "strat_mode": int(params.get("STRAT_MODE", 1)),
        "trades": int(stats.get("trades", 0)),
        "win_rate": float(stats.get("win_rate", 0.0)),
        "recent_win_rate": float(stats.get("recent_win_rate", 0.0)),
        "pnl_tmean": float(stats.get("pnl_tmean", stats.get("pnl_mean", 0.0))),
        "pnl_std": float(stats.get("pnl_std", 0.0)),
        "pnl_downside_dev": float(stats.get("pnl_downside_dev", 0.0)),
        "sharpe_like": float(stats.get("sharpe_like", 0.0)),
        "profit_factor": float(stats.get("profit_factor", 0.0)),
        "recent_profit_factor": float(stats.get("recent_profit_factor", 0.0)),
        "equity_cum_return_pct": float(stats.get("equity_cum_return_pct", 0.0)),
        "equity_max_dd_pct": float(stats.get("equity_max_dd_pct", 0.0)),
        "current_win_streak": int(stats.get("current_win_streak", 0)),
        "current_loss_streak": int(stats.get("current_loss_streak", 0)),
        "kelly_suggestion": float(stats.get("kelly_suggestion", 0.0)),
        "recent_kelly_suggestion": float(stats.get("recent_kelly_suggestion", 0.0)),
        "kelly_confidence": float(stats.get("kelly_confidence", 0.0)),
        "risk_capacity": float(risk_ctx.get("risk_capacity", 1.0)),
        "risk_mode": int(risk_ctx.get("risk_mode", 1)),
        "daily_realized_pnl_pct": float(risk_ctx.get("daily_realized_pnl_pct", 0.0)),
        "daily_drawdown_pct": float(risk_ctx.get("daily_drawdown_pct", 0.0)),
        "equity_usd": float(risk_ctx.get("equity_usd", 0.0)),
        "regime_label_numeric": float(risk_ctx.get("regime_label_numeric", 0.0)),
        "regime_stress_score": float(risk_ctx.get("regime_stress_score", 0.0)),
        "regime_signal_strength": float(risk_ctx.get("regime_signal_strength", 0.0)),
        "regime_entry_gate_factor": float(risk_ctx.get("regime_entry_gate_factor", 1.0)),
        "plan_gap_pct": float(risk_ctx.get("plan_gap_pct", 0.0)),
        "plan_status": str(risk_ctx.get("plan_status", "n/a")),
        "risk_profile": str(risk_ctx.get("risk_profile", "balanced")),
        "entry_score_gate": float(params.get("ENTRY_SCORE_GATE", DEFAULTS["ENTRY_SCORE_GATE"])),
        "entry_max_parallel": int(params.get("ENTRY_MAX_PARALLEL", DEFAULTS["ENTRY_MAX_PARALLEL"])),
        "max_risk_per_trade": float(
            params.get("MAX_RISK_PER_TRADE", DEFAULTS["MAX_RISK_PER_TRADE"])
        ),
        "reinvest_percent": float(params.get("REINVEST_PERCENT", DEFAULTS["REINVEST_PERCENT"])),
        "stop_loss_percent": float(params.get("STOP_LOSS_PERCENT", DEFAULTS["STOP_LOSS_PERCENT"])),
        "target_profit_percent": float(
            params.get("TARGET_PROFIT_PERCENT", DEFAULTS["TARGET_PROFIT_PERCENT"])
        ),
        "plan_target_apr_pct": float(
            params.get("PLAN_TARGET_APR_PCT", DEFAULTS["PLAN_TARGET_APR_PCT"])
        ),
        "plan_gap_pct_param": float(params.get("PLAN_GAP_PCT", 0.0)),
        "plan_status_param": str(params.get("PLAN_STATUS", "n/a")),
    }

    try:
        if redis is not None:
            await redis.set(HEALTH_KEY, _dumps(health))
        await log_event("strategy_health", health)
        _emit_event("strategy_health", health_score=score, trades=int(stats.get("trades", 0)))
    except Exception:
        pass

    try:
        now = _now()
        if (
            score <= HEALTH_ALERT_MIN_SCORE
            and (now - _mem_last_health_alert_ts) >= HEALTH_ALERT_INTERVAL_SEC
        ):
            _mem_last_health_alert_ts = now
            await send_telegram(
                "⚠️ Strategy health low "
                f"(score={score:.1f}, WR={health['win_rate']:.2%}, Sharpe≈{health['sharpe_like']:+.2f}, "
                f"DD={health['daily_drawdown_pct']:+.2f}%)"
            )
    except Exception:
        pass


async def get_health_snapshot(redis: RedisLike | None = None) -> dict[str, Any]:
    try:
        r = await _ensure_redis(redis)
        if r is not None:
            raw = await r.get(HEALTH_KEY)
            if raw:
                v = _loads_dict(raw)
                return v
        return {}
    except Exception:
        return {}


async def get_plan_snapshot(redis: RedisLike | None = None) -> dict[str, Any]:
    params = await get_strategy_params(redis)
    return {
        "plan_target_apr_pct": float(
            params.get("PLAN_TARGET_APR_PCT", DEFAULTS["PLAN_TARGET_APR_PCT"])
        ),
        "plan_start_equity_usd": float(params.get("PLAN_START_EQUITY_USD", 0.0)),
        "plan_start_ts": float(params.get("PLAN_START_TS", 0.0)),
        "plan_equity_today_usd": float(params.get("PLAN_EQUITY_TODAY_USD", 0.0)),
        "plan_gap_pct": float(params.get("PLAN_GAP_PCT", 0.0)),
        "plan_status": str(params.get("PLAN_STATUS", "n/a")),
    }


async def get_param_history(
    redis: RedisLike | None = None, limit: int = 50
) -> list[dict[str, Any]]:
    if limit <= 0:
        return []
    try:
        r = await _ensure_redis(redis)
        if r is None:
            return []
        raw = await r.lrange(PARAM_HISTORY_KEY, 0, limit - 1)
        items: list[Any] = raw if isinstance(raw, list) else []
        out: list[dict[str, Any]] = []
        for x in items:
            v = _loads_obj(x)
            if isinstance(v, dict):
                out.append(v)
        return out
    except Exception:
        return []


# =============================================================================
# Reinvest feedback hooks
# =============================================================================


async def _store_reinvest_event(redis: RedisLike | None, event: dict[str, Any]) -> None:
    try:
        r = await _ensure_redis(redis)
        e = dict(event or {})
        e.setdefault("ts", _now())
        if r is not None:
            await r.lpush(REINVEST_HISTORY_KEY, _dumps(e))
            await r.ltrim(REINVEST_HISTORY_KEY, 0, MAX_REINVEST_HISTORY_KEEP - 1)
    except Exception:
        pass
    with contextlib.suppress(Exception):
        await log_event("reinvest_event", event)


async def note_reinvest_decision(redis: RedisLike | None, payload: dict[str, Any]) -> None:
    e = {"kind": "decision"}
    e.update(payload or {})
    await _store_reinvest_event(redis, e)


async def note_reinvest_result(redis: RedisLike | None, payload: dict[str, Any]) -> None:
    e = {"kind": "result"}
    e.update(payload or {})
    await _store_reinvest_event(redis, e)


async def note_reinvest_skipped(redis: RedisLike | None, payload: dict[str, Any]) -> None:
    e = {"kind": "skipped"}
    e.update(payload or {})
    await _store_reinvest_event(redis, e)


async def get_reinvest_history(
    redis: RedisLike | None = None, limit: int = 100
) -> list[dict[str, Any]]:
    if limit <= 0:
        return []
    try:
        r = await _ensure_redis(redis)
        if r is None:
            return []
        raw = await r.lrange(REINVEST_HISTORY_KEY, 0, limit - 1)
        items: list[Any] = raw if isinstance(raw, list) else []
        out: list[dict[str, Any]] = []
        for x in items:
            v = _loads_obj(x)
            if isinstance(v, dict):
                out.append(v)
        return out
    except Exception:
        return []


# =============================================================================
# Snapshots
# =============================================================================


async def get_strategy_with_risk(redis: RedisLike | None = None) -> dict[str, Any]:
    r = await _ensure_redis(redis)
    params = await get_strategy_params(r)
    risk_ctx = await _read_risk_ctx(r)
    health = await get_health_snapshot(r)
    return {"params": params, "risk_ctx": risk_ctx, "health": health}


async def get_full_debug_snapshot(redis: RedisLike | None = None) -> dict[str, Any]:
    r = await _ensure_redis(redis)
    params = await get_strategy_params(r)
    stats = await get_perf_snapshot(r)
    risk_ctx = await _read_risk_ctx(r)
    health = await get_health_snapshot(r)
    return {"params": params, "stats": stats, "risk_ctx": risk_ctx, "health": health}


# =============================================================================
# Distributed lock (Render-safe)
# =============================================================================

_RELEASE_LOCK_LUA = """
if redis.call("get", KEYS[1]) == ARGV[1] then
  return redis.call("del", KEYS[1])
else
  return 0
end
"""


async def _acquire_tune_lock(redis: RedisLike | None) -> tuple[bool, str]:
    if redis is None:
        return True, "no_redis"
    token = uuid.uuid4().hex
    try:
        ok = await _redis_set(redis, TUNE_LOCK_KEY, token, nx=True, ex=TUNE_LOCK_TTL_SEC)
        return bool(ok), token
    except Exception:
        return True, "lock_fail_open"


async def _release_tune_lock(redis: RedisLike | None, token: str) -> None:
    if redis is None:
        return
    with contextlib.suppress(Exception):
        await _redis_eval(redis, _RELEASE_LOCK_LUA, 1, TUNE_LOCK_KEY, token)


# =============================================================================
# Apply + announce
# =============================================================================


async def _apply_and_announce(
    old: dict[str, Any],
    new: dict[str, Any],
    stats: dict[str, Any],
    risk_ctx: dict[str, Any],
    redis: RedisLike | None,
    *,
    reason: str,
    llm_reason: str,
) -> None:
    await set_strategy_params(redis, new)

    diffs = {k: (old.get(k), new.get(k)) for k in sorted(new.keys()) if old.get(k) != new.get(k)}
    await _record_param_history(redis, diffs, stats, risk_ctx, reason=f"{reason}|{llm_reason}")
    await _update_health(redis, stats, risk_ctx)

    if not diffs:
        return

    try:
        mode = int(new.get("STRAT_MODE", 1))
        mode_label = {0: "DEFENSIVE", 1: "NEUTRAL", 2: "AGGRESSIVE"}.get(mode, "NEUTRAL")
        lines = [
            "🤖 Strategy tuned (V4+++):",
            f"Mode={mode_label}  Trades={stats.get('trades', 0)}  WR={stats.get('win_rate', 0.0):.2%}  "
            f"Sharpe≈{stats.get('sharpe_like', 0.0):+.2f}  PF={stats.get('profit_factor', 0.0):.2f}",
            f"Capacity={risk_ctx.get('risk_capacity', 1.0):.3f}  "
            f"DailyPnL={risk_ctx.get('daily_realized_pnl_pct', 0.0):+.2f}%  "
            f"DD={risk_ctx.get('daily_drawdown_pct', 0.0):+.2f}%",
            f"Reason={reason}  LLM={llm_reason}",
        ]
        shown = 0
        for k, (a, b) in diffs.items():
            if k in ("PLAN_EQUITY_TODAY_USD", "PLAN_GAP_PCT", "PLAN_STATUS"):
                continue
            lines.append(f"• {k}: {a} → {b}")
            shown += 1
            if shown >= 12:
                lines.append(f"… +{max(0, len(diffs) - shown)} more")
                break
        await send_telegram("\n".join(lines))
    except Exception:
        pass

    try:
        reason_line = "; ".join(f"{k}:{a}→{b}" for k, (a, b) in diffs.items())
        await log_to_notion(
            token="STRAT",
            symbol="Params Update",
            balance=float(stats.get("trades", 0)),
            entry=float(new.get("STOP_LOSS_PERCENT", 0.0)),
            current=float(new.get("REINVEST_PERCENT", 0.0)),
            profit=float(stats.get("win_rate", 0.0) * 100.0),
            reason=(f"{reason}|{llm_reason} :: {reason_line}")[:1800],
        )
    except Exception:
        pass

    with contextlib.suppress(Exception):
        await log_event(
            "strategy_tuned",
            {
                "diffs": diffs,
                "reason": reason,
                "llm_reason": llm_reason,
                "stats": stats,
                "risk_ctx": risk_ctx,
            },
        )
    _metric("selflearn.tune_applied", 1)


# =============================================================================
# Tuning entrypoints
# =============================================================================


async def tune_once(redis: RedisLike | None = None) -> None:
    global _mem_last_tune_ts

    r = await _ensure_redis(redis)

    locked, token = await _acquire_tune_lock(r)
    if not locked:
        return

    try:
        now = _now()
        try:
            last_ts = _safe_float(
                (await r.get(LAST_TUNE_TS_KEY)) if r is not None else _mem_last_tune_ts, 0.0
            )
        except Exception:
            last_ts = _mem_last_tune_ts

        min_interval = max(int(LEARNING_INTERVAL_SECONDS * 0.50), 300)
        if (now - last_ts) < min_interval:
            return

        await _recompute_and_store_stats(r)
        stats = await get_perf_snapshot(r)
        cur = await get_strategy_params(r)
        risk_ctx = await _read_risk_ctx(r)

        if STRAT_REQUIRE_RUNTIME_STABLE and not bool(risk_ctx.get("runtime_stable", True)):
            await _update_health(r, stats, risk_ctx)
            if r is not None:
                await r.set(LAST_TUNE_TS_KEY, str(now))
            else:
                _mem_last_tune_ts = now
            with contextlib.suppress(Exception):
                await log_event("strategy_skip_not_stable", {"risk_ctx": risk_ctx})
            return

        if int(stats.get("trades", 0)) < MIN_TRADES_FOR_TUNING:
            await _update_health(r, stats, risk_ctx)
            if r is not None:
                await r.set(LAST_TUNE_TS_KEY, str(now))
            else:
                _mem_last_tune_ts = now
            return

        with contextlib.suppress(Exception):
            await log_event("strategy_tune_attempt", {"stats": stats, "risk_ctx": risk_ctx})

        proposed, reason = _rule_based_adjust(cur, stats, risk_ctx)
        proposed = _enforce_risk_api_contract(proposed, risk_ctx)
        proposed = _update_equity_plan(proposed, risk_ctx)

        proposed, llm_reason = await _maybe_llm_adjust(proposed, stats, risk_ctx, r)
        proposed = _enforce_risk_api_contract(proposed, risk_ctx)
        proposed = _update_equity_plan(proposed, risk_ctx)

        limited = _limit_step(cur, proposed)

        await _apply_and_announce(
            cur, limited, stats, risk_ctx, r, reason=reason, llm_reason=llm_reason
        )

        try:
            if r is not None:
                await r.set(LAST_TUNE_TS_KEY, str(now))
            else:
                _mem_last_tune_ts = now
        except Exception:
            pass

    finally:
        await _release_tune_lock(r, token)


async def run_self_learning_loop(redis: RedisLike | None = None) -> None:
    r = await _ensure_redis(redis)

    try:
        info_fn = getattr(log, "info", None)
        if callable(info_fn):
            info_fn(
                "[self_learning] started interval=%ss require_stable=%s ai_tuning=%s",
                LEARNING_INTERVAL_SECONDS,
                STRAT_REQUIRE_RUNTIME_STABLE,
                bool(_ENABLE_AI_TUNING and AsyncOpenAI is not None),
            )
        else:
            _stdlog.info(
                "[self_learning] started interval=%ss require_stable=%s ai_tuning=%s",
                LEARNING_INTERVAL_SECONDS,
                STRAT_REQUIRE_RUNTIME_STABLE,
                bool(_ENABLE_AI_TUNING and AsyncOpenAI is not None),
            )
    except Exception:
        _stdlog.info(
            "[self_learning] started interval=%ss require_stable=%s ai_tuning=%s",
            LEARNING_INTERVAL_SECONDS,
            STRAT_REQUIRE_RUNTIME_STABLE,
            bool(_ENABLE_AI_TUNING and AsyncOpenAI is not None),
        )

    while True:
        try:
            await tune_once(r)
        except Exception as e:  # pragma: no cover
            with contextlib.suppress(Exception):
                _stdlog.error("[self_learning] tune_once error: %s", e, exc_info=True)
        await asyncio.sleep(max(60, LEARNING_INTERVAL_SECONDS))


# =============================================================================
# CLI manual test
# =============================================================================

if __name__ == "__main__":  # pragma: no cover

    async def _demo() -> None:
        r = await _ensure_redis(None)
        await tune_once(r)
        print(json.dumps(await get_full_debug_snapshot(r), indent=2))

    asyncio.run(_demo())
