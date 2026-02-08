from __future__ import annotations

"""
Reinvestor — V3+++ / God-Tier LOCK-IN (P0→P1)
============================================

Purpose
-------
Turn realized proceeds into a compounding flywheel WITHOUT becoming a second execution engine.

This module:
  1) Builds decision context (risk/regime/perf/infra/budget)
  2) Applies hard gates (halt, drawdown, infra, budgets, idempotency, locks)
  3) Computes reinvest vs savings split (base_pct × scales, bounded)
  4) Selects a reinvest target (smart_wallets preferred; legacy fallback)
  5) Executes buy (Jupiter via core_utils.swap_token_jupiter)
  6) Persists a position (positions.save_position if present; fallback json)
  7) Emits events + Telegram + Notion + Redis introspection
  8) Supports queue worker w/ DLQ + stale job dropping

Public API
----------
    async def reinvest(proceeds_sol: float, meta: dict|None = None, redis=None) -> str|None
    async def queue_reinvest(proceeds_sol: float, meta: dict|None = None, redis=None) -> None
    async def run_reinvest_worker(redis=None, poll_interval: float = 2.0, stop_evt: asyncio.Event|None = None) -> None

    async def preview_reinvest_split(proceeds_sol: float, meta: dict|None = None, redis=None) -> dict|None
    async def explain_reinvest_decision(proceeds_sol: float, meta: dict|None = None, redis=None) -> dict|None
    async def dry_run_reinvest(proceeds_sol: float, meta: dict|None = None, redis=None) -> dict|None

    async def handle_sell_filled_event(ev: dict, redis=None) -> None

    async def get_reinvest_health(redis=None) -> dict|None
    async def get_last_reinvest_decision(redis=None) -> dict|None
    async def get_recent_reinvest_decisions(limit: int = 50, redis=None) -> list[dict]
    def reload_reinvestor_config() -> None

Design notes (mypy/CI)
----------------------
- No merge markers (this file must be clean).
- Optional integrations are loaded lazily via importlib at runtime (signature drift tolerant).
- Avoids widespread "# type: ignore" to play nice with warn_unused_ignores.

Env keys
--------
This rewrite preserves the existing env key names/usage patterns from your module.
Do not add new env keys here; contract tests depend on stable key usage.
"""

import asyncio
import contextlib
import dataclasses
import hashlib
import importlib
import inspect
import json
import logging
import os
import secrets
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime
from functools import lru_cache
from typing import Any, Callable, TypeAlias, cast

# ──────────────────────────────────────────────────────────────────────────────
# Types
# ──────────────────────────────────────────────────────────────────────────────
JSONLike: TypeAlias = dict[str, Any] | list[Any] | str | int | float | bool | None
Meta: TypeAlias = dict[str, Any]
RedisClient: TypeAlias = Any
SavePosFn: TypeAlias = Callable[[str, dict[str, Any]], Any]


# ──────────────────────────────────────────────────────────────────────────────
# Logger (no hard imports; keep import-time side effects minimal)
# ──────────────────────────────────────────────────────────────────────────────
def _get_logger() -> logging.Logger:
    with contextlib.suppress(Exception):
        mod = importlib.import_module("utils_pkg.logging_setup")
        get_logger = getattr(mod, "get_logger", None)
        if callable(get_logger):
            lg = get_logger("sniper.reinvestor")
            if isinstance(lg, logging.Logger):
                return lg

    lg2 = logging.getLogger("sniper.reinvestor")
    if not lg2.handlers:
        logging.basicConfig(level=logging.INFO)
    return lg2


log: logging.Logger = _get_logger()


# ──────────────────────────────────────────────────────────────────────────────
# Small utilities
# ──────────────────────────────────────────────────────────────────────────────
SOL_MINT: str = "So11111111111111111111111111111111111111112"


def _envs(k: str, d: str = "") -> str:
    v = os.getenv(k)
    return (v if v is not None else d).strip()


def _envb(k: str, d: bool = False) -> bool:
    v = _envs(k, "")
    if v == "":
        return d
    return v.lower() in {"1", "true", "yes", "y", "on", "t"}


def _envi(k: str, d: int) -> int:
    try:
        return int(_envs(k, str(d)))
    except Exception:
        return d


def _envf(k: str, d: float) -> float:
    try:
        return float(_envs(k, str(d)))
    except Exception:
        return d


def _now_utc() -> datetime:
    return datetime.now(UTC)


def _utc_ts() -> float:
    return _now_utc().timestamp()


def _today_ymd() -> str:
    return _now_utc().strftime("%Y%m%d")


def _today_key(prefix: str) -> str:
    return f"{prefix}:{_today_ymd()}"


def _is_weekend(dt: datetime | None = None) -> bool:
    dt2 = dt or _now_utc()
    return dt2.weekday() >= 5


def _safe_float(x: Any, d: float = 0.0) -> float:
    try:
        if x is None:
            return d
        return float(x)
    except Exception:
        return d


def _safe_int(x: Any, d: int = 0) -> int:
    try:
        if x is None:
            return d
        return int(float(x))
    except Exception:
        return d


def _maybe_decode(x: Any) -> Any:
    if isinstance(x, (bytes, bytearray)):
        with contextlib.suppress(Exception):
            return x.decode("utf-8", errors="ignore")
    return x


def _is_awaitable(x: Any) -> bool:
    with contextlib.suppress(Exception):
        return inspect.isawaitable(x)
    return False


async def _maybe_await(x: Any) -> Any:
    if _is_awaitable(x):
        return await x
    return x


async def _call_maybe_async(fn: Any, *args: Any, **kwargs: Any) -> Any:
    """
    Call a function that might be sync or async and might have signature drift.
    - Tries kwargs call; if TypeError, retries positional call.
    - Awaits if return is awaitable.
    """
    if not callable(fn):
        raise RuntimeError("call target is not callable")
    try:
        out = fn(*args, **kwargs)
    except TypeError:
        out = fn(*args)
    return await _maybe_await(out)


def _phase() -> int:
    try:
        p = int(_envs("MASTER_PHASE", _envs("STRATEGY_PHASE", "7")) or "7")
    except Exception:
        p = 7
    return max(0, min(7, p))


def _normalize_slippage(x: float) -> float:
    """
    Accepts slippage as:
      - fraction (0.005 = 0.5%)
      - percent  (0.5 = 0.5%)  [treated as percent if >= 0.1]
      - bps      (50 = 50 bps) [treated as bps if >= 5]
    Returns fraction in [0, 0.25].
    """
    try:
        v = float(x)
    except Exception:
        return 0.005
    if v <= 0:
        return 0.0
    if v >= 5.0:
        v = v / 10_000.0
    elif v >= 0.1:
        v = v / 100.0
    return max(0.0, min(0.25, v))


def _json_dumps(obj: Any, **kwargs: Any) -> str:
    def _default(o: Any) -> Any:
        if dataclasses.is_dataclass(o) and not isinstance(o, type):
            return dataclasses.asdict(o)
        if isinstance(o, datetime):
            return o.isoformat()
        return str(o)

    return json.dumps(obj, default=_default, ensure_ascii=False, **kwargs)


# ──────────────────────────────────────────────────────────────────────────────
# Lazy import helpers (avoid mypy stub/import errors + keep optional deps optional)
# ──────────────────────────────────────────────────────────────────────────────
@lru_cache(maxsize=256)
def _import_module(name: str) -> Any | None:
    with contextlib.suppress(Exception):
        return importlib.import_module(name)
    return None


def _import_attr(mod_name: str, attr: str) -> Any | None:
    mod = _import_module(mod_name)
    if mod is None:
        return None
    return getattr(mod, attr, None)


# Mint normalization (prefer canonical helper if present)
def normalize_mint(x: str) -> str:
    fn = _import_attr("utils_pkg.mints", "normalize_mint")
    if callable(fn):
        with contextlib.suppress(Exception):
            out = fn(x)
            return str(out).strip()
    return (x or "").strip()


@lru_cache(maxsize=1)
def _mint_guard_fn() -> Callable[[str], bool] | None:
    fn = _import_attr("utils_pkg.mint_guard", "is_mint_allowed")
    return cast(Callable[[str], bool], fn) if callable(fn) else None


# ──────────────────────────────────────────────────────────────────────────────
# core_utils (lazy)
# ──────────────────────────────────────────────────────────────────────────────
@lru_cache(maxsize=1)
def _core_mod() -> Any | None:
    # Canonical location in your repo
    return _import_module("utils_pkg.core_utils")


def _core_attr(name: str) -> Any:
    mod = _core_mod()
    if mod is None:
        raise RuntimeError(f"core_utils missing required function: {name}")
    fn = getattr(mod, name, None)
    if not callable(fn):
        raise RuntimeError(f"core_utils missing required function: {name}")
    return fn


def _core_opt(name: str) -> Any | None:
    mod = _core_mod()
    if mod is None:
        return None
    return getattr(mod, name, None)


def _shared_redis_from_core() -> RedisClient | None:
    mod = _core_mod()
    if mod is None:
        return None
    return getattr(mod, "redis", None)


# ──────────────────────────────────────────────────────────────────────────────
# Redis helpers
# ──────────────────────────────────────────────────────────────────────────────
async def _redis_call(redis: RedisClient, fn_name: str, *args: Any, **kwargs: Any) -> Any:
    fn = getattr(redis, fn_name, None)
    if not callable(fn):
        raise AttributeError(fn_name)
    try:
        out = fn(*args, **kwargs)
    except TypeError:
        out = fn(*args)
    return await _maybe_await(out)


async def _get_redis(url: str | None = None) -> RedisClient | None:
    """
    Return a Redis client if available.

    Priority:
      1) Reuse shared Redis from core_utils (if exposed there).
      2) Create a redis.asyncio client via REDIS_URL / provided url.

    This function is deliberately "no-throw": it returns None on any failure.
    """
    shared = _shared_redis_from_core()
    if shared is not None:
        return shared

    ru = (url or _envs("REDIS_URL", "redis://localhost:6379")).strip()
    if not ru:
        return None

    mod = _import_module("redis.asyncio")
    if mod is None:
        return None

    from_url = getattr(mod, "from_url", None)
    if not callable(from_url):
        return None

    with contextlib.suppress(Exception):
        return from_url(ru, decode_responses=True)

    return None


async def _get_key_float(redis: RedisClient | None, key: str) -> float | None:
    if not redis:
        return None
    try:
        raw = _maybe_decode(await _redis_call(redis, "get", key))
        return float(raw) if raw else None
    except Exception:
        return None


async def _set_key_float(redis: RedisClient | None, key: str, val: float) -> None:
    if not redis:
        return
    with contextlib.suppress(Exception):
        await _redis_call(redis, "set", key, str(float(val)))


# ──────────────────────────────────────────────────────────────────────────────
# Data logger (soft; may be sync or async)
# ──────────────────────────────────────────────────────────────────────────────
async def _log_event_safe(*args: Any, **kwargs: Any) -> None:
    fn = _import_attr("utils_pkg.data_logger", "log_event")
    if not callable(fn):
        return
    with contextlib.suppress(Exception):
        await _call_maybe_async(fn, *args, **kwargs)


# ──────────────────────────────────────────────────────────────────────────────
# Position persistence — prefer SoT store if present
# ──────────────────────────────────────────────────────────────────────────────
@lru_cache(maxsize=1)
def _load_save_position() -> SavePosFn | None:
    fn1 = _import_attr("utils_pkg.positions", "save_position")
    if callable(fn1):
        return cast(SavePosFn, fn1)
    fn2 = _import_attr("positions", "save_position")
    if callable(fn2):
        return cast(SavePosFn, fn2)
    return None


def _fallback_save_position(mint: str, data: dict[str, Any]) -> None:
    os.makedirs("data", exist_ok=True)
    path = os.path.join("data", "positions.json")

    cur: dict[str, Any] = {}
    with contextlib.suppress(Exception):
        if os.path.exists(path):
            with open(path, encoding="utf-8") as f:
                loaded = json.load(f)
            if isinstance(loaded, dict):
                cur = loaded

    cur[mint] = data
    with open(path, "w", encoding="utf-8") as f:
        json.dump(cur, f, indent=2, ensure_ascii=False)


async def _save_position(mint: str, data: dict[str, Any]) -> None:
    sp = _load_save_position()
    if callable(sp):
        await _maybe_await(sp(mint, data))
        return
    _fallback_save_position(mint, data)


# ──────────────────────────────────────────────────────────────────────────────
# Optional modules — always called via wrappers (signature drift tolerant)
# ──────────────────────────────────────────────────────────────────────────────
async def infra_features(redis: RedisClient | None = None) -> dict[str, float]:
    fn = _import_attr("utils_pkg.infra_guard", "infra_features")
    if not callable(fn):
        return {
            "infra_rpc_latency_ms": 0.0,
            "infra_jup_latency_ms": 0.0,
            "infra_error_streak": 0.0,
            "infra_state_is_healthy": 1.0,
            "infra_state_is_degraded": 0.0,
            "infra_state_is_halted": 0.0,
            "infra_health_score": 1.0,
            "infra_stability_score": 1.0,
        }
    try:
        out = await _call_maybe_async(fn, redis)
        return out if isinstance(out, dict) else {}
    except Exception:
        return {}


async def infra_should_block_entries(redis: RedisClient | None = None) -> bool:
    fn = _import_attr("utils_pkg.infra_guard", "should_block_entries")
    if not callable(fn):
        return False
    try:
        out = await _call_maybe_async(fn, redis)
        return bool(out)
    except Exception:
        return False


async def get_strategy_params(redis: RedisClient | None = None) -> dict[str, Any]:
    fn = _import_attr("utils_pkg.self_learning_ai_strategy", "get_strategy_params")
    if not callable(fn):
        return {
            "REINVEST_PERCENT": float(_envs("REINVEST_PERCENT", "75.0")),
            "STOP_LOSS_PERCENT": float(_envs("STOP_LOSS_PERCENT", "18.0")),
            "SLIPPAGE": float(_envs("SLIPPAGE", "0.005")),
            "MIN_SOL_BALANCE": float(_envs("MIN_SOL_BALANCE", "0.25")),
            "MAX_RISK_PER_TRADE": float(_envs("MAX_RISK_PER_TRADE", "0.05")),
            "STRAT_MODE": int(_envs("STRAT_MODE", "1")),
            "HARD_HALT": _envb("HARD_HALT", False),
            "PLAN_TARGET_APR_PCT": float(_envs("PLAN_TARGET_APR_PCT", "180.0")),
            "PLAN_GAP_PCT": 0.0,
            "PLAN_STATUS": "n/a",
        }
    out = await _call_maybe_async(fn, redis)
    return out if isinstance(out, dict) else {}


async def get_perf_snapshot(redis: RedisClient | None = None) -> dict[str, Any]:
    fn = _import_attr("utils_pkg.self_learning_ai_strategy", "get_perf_snapshot")
    if not callable(fn):
        return {}
    out = await _call_maybe_async(fn, redis)
    return out if isinstance(out, dict) else {}


async def note_reinvest_decision(*args: Any, **kwargs: Any) -> None:
    fn = _import_attr("utils_pkg.self_learning_ai_strategy", "note_reinvest_decision")
    if not callable(fn):
        return
    with contextlib.suppress(Exception):
        await _call_maybe_async(fn, *args, **kwargs)


async def note_reinvest_result(*args: Any, **kwargs: Any) -> None:
    fn = _import_attr("utils_pkg.self_learning_ai_strategy", "note_reinvest_result")
    if not callable(fn):
        return
    with contextlib.suppress(Exception):
        await _call_maybe_async(fn, *args, **kwargs)


async def note_reinvest_skipped(*args: Any, **kwargs: Any) -> None:
    fn = _import_attr("utils_pkg.self_learning_ai_strategy", "note_reinvest_skipped")
    if not callable(fn):
        return
    with contextlib.suppress(Exception):
        await _call_maybe_async(fn, *args, **kwargs)


async def risk_capacity_snapshot(redis: RedisClient | None = None, **kw: Any) -> dict[str, Any]:
    fn = _import_attr("utils_pkg.risk_manager", "risk_capacity_snapshot")
    if not callable(fn):
        return {
            "risk_capacity": 1.0,
            "risk_mode": 1,
            "daily_realized_pnl_pct": 0.0,
            "daily_drawdown_pct": 0.0,
            "equity_usd": 0.0,
            "daily_trades_count": 0,
            "daily_trades_cap": 0,
            "sandbox_mode": False,
            "regime_label": "neutral",
            "regime_label_numeric": 0.0,
            "regime_stress_score": 0.0,
            "regime_signal_strength": 0.5,
            "regime_entry_gate_factor": 1.0,
            "regime_risk_multiplier": 1.0,
            "max_risk_per_trade": 0.05,
            "min_sol_balance": 0.25,
            "strat_mode": 1,
            "plan_gap_pct": 0.0,
            "plan_status": "n/a",
            "ai_capacity_delta": 0.0,
            "halt_reason": None,
            "tail_risk_score": 0.0,
            "daily_spend_remaining_usd": 1e12,
            "spend_budget_block": False,
            "equity_floor_usd": 0.0,
            "equity_below_floor": False,
            "risk_tags": [],
            "kelly_raw": 0.02,
            "kelly_pos_pct_cap": 0.08,
            "kelly_confidence": 0.0,
        }
    try:
        out = await _call_maybe_async(fn, redis, **kw)
    except TypeError:
        out = await _call_maybe_async(fn, redis)
    return out if isinstance(out, dict) else {}


async def should_allow_new_entry(redis: RedisClient | None = None, **kw: Any) -> bool:
    fn = _import_attr("utils_pkg.risk_manager", "should_allow_new_entry")
    if not callable(fn):
        return True
    try:
        out = await _call_maybe_async(fn, redis, **kw)
    except TypeError:
        out = await _call_maybe_async(fn, redis)
    return bool(out)


async def note_spend(redis: RedisClient | None, usd: float, **kw: Any) -> None:
    fn = _import_attr("utils_pkg.risk_manager", "note_spend")
    if not callable(fn):
        return
    with contextlib.suppress(Exception):
        try:
            await _call_maybe_async(fn, redis, usd, **kw)
        except TypeError:
            await _call_maybe_async(fn, redis, usd)


def _pick_reinvestment_target_fn() -> Any | None:
    fn = _import_attr("utils_pkg.smart_wallets", "pick_reinvestment_target")
    return fn if callable(fn) else None


def _find_reinvestment_token_fn() -> Any | None:
    fn = _import_attr("utils_pkg.find_reinvestment_token", "find_reinvestment_token")
    if callable(fn):
        return fn
    fn2 = _import_attr("find_reinvestment_token", "find_reinvestment_token")
    return fn2 if callable(fn2) else None


async def find_reinvestment_token(
    min_volume: float, honeypot_filter: bool, **kw: Any
) -> str | None:
    fn = _find_reinvestment_token_fn()
    if not callable(fn):
        return None
    try:
        out = await _call_maybe_async(
            fn,
            min_volume=float(min_volume),
            honeypot_filter=bool(honeypot_filter),
            **kw,
        )
    except TypeError:
        out = await _call_maybe_async(fn, float(min_volume), bool(honeypot_filter))
    return out if isinstance(out, str) else None


def _sell_schedule_after_buy_fn() -> Any | None:
    fn = _import_attr("utils_pkg.ai_sell_logic", "schedule_after_buy")
    return fn if callable(fn) else None


def _exit_register_after_buy_fn() -> Any | None:
    fn = _import_attr("utils_pkg.exit_engine", "register_position_after_buy")
    return fn if callable(fn) else None


def _autoexit_register_after_buy_fn() -> Any | None:
    fn = _import_attr("utils_pkg.auto_exit_worker.exit_engine", "register_position_after_buy")
    return fn if callable(fn) else None


# ──────────────────────────────────────────────────────────────────────────────
# Redis keys (keep existing env keys only)
# ──────────────────────────────────────────────────────────────────────────────
REINVEST_QUEUE: str = _envs("REINVEST_QUEUE_KEY", "reinvest:queue")
REINVEST_DLQ: str = _envs("REINVEST_DLQ_KEY", "reinvest:dlq")

REINVEST_HEALTH_KEY: str = _envs("REINVEST_HEALTH_KEY", "reinvest:health")
REINVEST_DECISION_KEY: str = _envs("REINVEST_DECISION_KEY", "reinvest:last_decision")
REINVEST_DECISION_LIST_KEY: str = _envs("REINVEST_DECISION_LIST_KEY", "reinvest:decisions")

REINVEST_DAILY_COUNT_KEY: str = _envs("REINVEST_DAILY_COUNT_KEY", "reinvest:daily")
REINVEST_LAST_TS_KEY: str = _envs("REINVEST_LAST_TS_KEY", "reinvest:last_ts")
REINVEST_FAIL_TS_KEY: str = _envs("REINVEST_FAIL_TS_KEY", "reinvest:last_fail_ts")
REINVEST_TG_LAST_TS_KEY: str = _envs("REINVEST_TG_LAST_TS_KEY", "reinvest:tg:last_ts")

REINVEST_SEEN_PREFIX: str = _envs("REINVEST_SEEN_PREFIX", "reinvest:seen")
REINVEST_LOCK_PREFIX: str = _envs("REINVEST_LOCK_PREFIX", "reinvest:lock")


# ──────────────────────────────────────────────────────────────────────────────
# Config (keep env key usage stable)
# ──────────────────────────────────────────────────────────────────────────────
def _parse_mint_set(val: str) -> set[str]:
    out: set[str] = set()
    for part in (val or "").replace(";", ",").split(","):
        s = part.strip()
        if len(s) > 30:
            out.add(normalize_mint(s))
    return out


@dataclass(frozen=True)
class ReinvestCfg:
    enabled: bool
    global_dry_run: bool

    min_proceeds_sol: float
    min_proceeds_usd: float

    vol_min_usd: float
    honeypot_check: bool
    require_price: bool

    allow_mints: set[str]
    deny_mints: set[str]

    savings_wallet: str | None
    always_savings_pct: float
    savings_after_swap: bool

    block_weekends: bool
    min_capacity: float
    defensive_capacity: float
    min_equity_sol: float

    dd_soft_stop_pct: float
    dd_hard_stop_pct: float
    pnl_lock_pct: float

    health_min_winrate: float
    health_min_recent_winrate: float
    health_min_sharpe: float
    health_min_score: float

    infra_min_health: float
    block_if_infra_halted: bool
    block_if_infra_degraded: bool

    tail_soft: float
    tail_hard: float
    tail_gate: float

    min_eff_pct: float
    max_eff_pct: float
    max_single_usd: float
    max_budget_fraction: float

    max_reinvests_per_day: int
    min_minutes_between: float
    queue_max_age_s: float
    cooldown_on_fail_min: float

    worker_batch: int
    queue_maxlen: int
    worker_backoff_s: float

    idempotency_ttl_h: float
    lock_enabled: bool
    lock_ttl_s: int
    lock_scope: str

    tg_min_seconds: float
    decisions_keep: int

    phase: int

    @staticmethod
    def from_env() -> "ReinvestCfg":
        ph = _phase()
        default_batch = 5 if ph <= 2 else (10 if ph <= 5 else 14)

        allow = _parse_mint_set(_envs("REINVEST_ALLOW_MINTS", _envs("REINVEST_ALLOWLIST", "")))
        deny = _parse_mint_set(_envs("REINVEST_DENY_MINTS", _envs("REINVEST_BLOCKLIST", "")))

        scope = (_envs("REINVEST_LOCK_SCOPE", "job") or "job").strip().lower()
        if scope not in ("job", "global"):
            scope = "job"

        soft = max(0.0, min(1.0, _envf("REINVEST_TAIL_RISK_SOFT", 0.60)))
        hard = max(soft, min(1.0, _envf("REINVEST_TAIL_RISK_HARD", 0.85)))

        return ReinvestCfg(
            enabled=_envb("REINVESTOR_ENABLED", True),
            global_dry_run=_envb("REINVEST_GLOBAL_DRY_RUN", False),
            min_proceeds_sol=max(
                0.0,
                _envf("REINVESTMENT_MIN_PROCEEDS_SOL", _envf("REINVESTMENT_MIN_PROFIT_SOL", 0.01)),
            ),
            min_proceeds_usd=max(
                0.0,
                _envf("REINVESTMENT_MIN_PROCEEDS_USD", _envf("REINVESTMENT_MIN_PROFIT_USD", 0.0)),
            ),
            vol_min_usd=max(
                0.0, _envf("REINVESTMENT_VOLUME_MIN_USD", _envf("REINVESTMENT_VOLUME_MIN", 5000.0))
            ),
            honeypot_check=_envb("REINVESTMENT_HONEYPOT_CHECK", True),
            require_price=_envb("REINVEST_REQUIRE_PRICE", False),
            allow_mints=allow,
            deny_mints=deny,
            savings_wallet=_envs("SAVINGS_WALLET", _envs("SAVINGS_WALLET_ADDRESS", "")) or None,
            always_savings_pct=max(0.0, min(95.0, _envf("REINVEST_ALWAYS_SAVINGS_PCT", 0.0))),
            savings_after_swap=_envb("REINVEST_SAVINGS_AFTER_SWAP", True),
            block_weekends=_envb("REINVEST_BLOCK_WEEKENDS", False),
            min_capacity=max(0.0, min(1.0, _envf("REINVEST_MIN_CAPACITY", 0.15))),
            defensive_capacity=max(0.0, min(1.0, _envf("REINVEST_DEF_CAPACITY", 0.40))),
            min_equity_sol=max(0.0, _envf("REINVEST_MIN_EQUITY_SOL", 0.0)),
            dd_soft_stop_pct=max(0.0, _envf("REINVEST_DD_SOFT_STOP_PCT", 8.0)),
            dd_hard_stop_pct=max(0.0, _envf("REINVEST_DD_HARD_STOP_PCT", 15.0)),
            pnl_lock_pct=max(0.0, _envf("REINVEST_PNL_LOCK_PCT", 20.0)),
            health_min_winrate=max(0.0, min(1.0, _envf("REINVEST_HEALTH_MIN_WINRATE", 0.40))),
            health_min_recent_winrate=max(
                0.0, min(1.0, _envf("REINVEST_HEALTH_MIN_RECENT_WINRATE", 0.38))
            ),
            health_min_sharpe=_envf("REINVEST_HEALTH_MIN_SHARPE", -0.2),
            health_min_score=max(0.0, _envf("REINVEST_HEALTH_MIN_SCORE", 0.0)),
            infra_min_health=max(0.0, _envf("REINVEST_INFRA_MIN_HEALTH", 0.0)),
            block_if_infra_halted=_envb("REINVEST_BLOCK_IF_INFRA_HALTED", True),
            block_if_infra_degraded=_envb("REINVEST_BLOCK_IF_INFRA_DEGRADED", False),
            tail_soft=soft,
            tail_hard=hard,
            tail_gate=max(0.0, min(1.0, _envf("REINVEST_TAIL_RISK_GATE", 0.0))),
            min_eff_pct=max(0.0, _envf("REINVEST_MIN_EFF_PCT", 0.0)),
            max_eff_pct=max(0.0, min(95.0, _envf("REINVEST_MAX_EFF_PCT", 95.0))),
            max_single_usd=max(0.0, _envf("REINVEST_MAX_SINGLE_USD", 0.0)),
            max_budget_fraction=max(0.0, min(1.0, _envf("REINVEST_MAX_BUDGET_FRACTION", 0.0))),
            max_reinvests_per_day=max(0, _envi("REINVEST_MAX_REINVESTS_PER_DAY", 0)),
            min_minutes_between=max(0.0, _envf("REINVEST_MIN_MINUTES_BETWEEN_REINVESTS", 0.0)),
            queue_max_age_s=max(0.0, _envf("REINVEST_QUEUE_MAX_AGE_S", 0.0)),
            cooldown_on_fail_min=max(0.0, _envf("REINVEST_COOLDOWN_ON_FAIL_MIN", 0.0)),
            worker_batch=max(1, _envi("REINVEST_WORKER_BATCH", default_batch)),
            queue_maxlen=max(0, _envi("REINVEST_QUEUE_MAXLEN", 0)),
            worker_backoff_s=max(0.25, _envf("REINVEST_WORKER_BACKOFF_S", 1.0)),
            idempotency_ttl_h=max(1.0, _envf("REINVEST_IDEMPOTENCY_TTL_H", 48.0)),
            lock_enabled=_envb("REINVEST_LOCK_ENABLED", True),
            lock_ttl_s=max(5, _envi("REINVEST_LOCK_TTL_S", 45)),
            lock_scope=scope,
            tg_min_seconds=max(0.0, _envf("REINVEST_TG_MIN_SECONDS", 2.5)),
            decisions_keep=max(10, _envi("REINVEST_DECISIONS_KEEP", 200)),
            phase=ph,
        )


CFG: ReinvestCfg = ReinvestCfg.from_env()


def reload_reinvestor_config() -> None:
    global CFG
    CFG = ReinvestCfg.from_env()
    log.info(
        "REINVEST_CFG_RELOADED enabled=%s dry=%s phase=%s batch=%s lock_scope=%s",
        int(CFG.enabled),
        int(CFG.global_dry_run),
        CFG.phase,
        CFG.worker_batch,
        CFG.lock_scope,
    )


# ──────────────────────────────────────────────────────────────────────────────
# Counters / timestamps
# ──────────────────────────────────────────────────────────────────────────────
async def _get_last_reinvest_ts(redis: RedisClient | None) -> float | None:
    return await _get_key_float(redis, REINVEST_LAST_TS_KEY)


async def _set_last_reinvest_ts(redis: RedisClient | None, ts: float | None = None) -> None:
    await _set_key_float(redis, REINVEST_LAST_TS_KEY, float(ts if ts is not None else _utc_ts()))


async def _set_last_fail_ts(redis: RedisClient | None, ts: float | None = None) -> None:
    await _set_key_float(redis, REINVEST_FAIL_TS_KEY, float(ts if ts is not None else _utc_ts()))


async def _get_last_fail_ts(redis: RedisClient | None) -> float | None:
    return await _get_key_float(redis, REINVEST_FAIL_TS_KEY)


async def _bump_daily_reinvest_counter(redis: RedisClient | None) -> None:
    if not redis:
        return
    key = _today_key(REINVEST_DAILY_COUNT_KEY)
    try:
        pipe = getattr(redis, "pipeline", None)
        if callable(pipe):
            p = pipe()
            p.incr(key)
            p.expire(key, 60 * 60 * 36)
            await _maybe_await(p.execute())
            return
        await _redis_call(redis, "incr", key)
        await _redis_call(redis, "expire", key, 60 * 60 * 36)
    except Exception:
        return


async def _get_daily_reinvest_count(redis: RedisClient | None) -> int:
    if not redis:
        return 0
    key = _today_key(REINVEST_DAILY_COUNT_KEY)
    try:
        raw = _maybe_decode(await _redis_call(redis, "get", key))
        return int(raw or "0")
    except Exception:
        return 0


# ──────────────────────────────────────────────────────────────────────────────
# Idempotency + locks
# ──────────────────────────────────────────────────────────────────────────────
def _id_key_from_meta(meta: Meta) -> str | None:
    for k in ("job_id", "origin_trade_id", "origin_position_id", "cycle_id", "tx_sig", "txid"):
        v = meta.get(k)
        if isinstance(v, str) and v.strip():
            return v.strip()
    return None


def _ensure_job_id(meta: Meta, proceeds_sol: float) -> None:
    if _id_key_from_meta(meta):
        return
    base = _json_dumps(
        {"p": float(proceeds_sol), "m": meta}, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    h = hashlib.sha256(base).hexdigest()[:16]
    meta["job_id"] = f"reinv-{int(_utc_ts())}-{h}"


async def _idempotency_claim(redis: RedisClient | None, meta: Meta) -> bool:
    """
    True  => claimed (not seen before) OR best-effort (no redis / no id).
    False => already processed.
    """
    if not redis:
        return True
    jid = _id_key_from_meta(meta)
    if not jid:
        return True
    key = f"{REINVEST_SEEN_PREFIX}:{jid}"
    ttl_s = int(max(1.0, CFG.idempotency_ttl_h) * 3600)
    try:
        ok = await _redis_call(redis, "set", key, "1", nx=True, ex=ttl_s)
        return bool(ok)
    except Exception:
        return True


_RELEASE_LUA: str = "if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) else return 0 end"


def _lock_key(meta: Meta) -> str:
    if CFG.lock_scope == "global":
        return REINVEST_LOCK_PREFIX
    jid = _id_key_from_meta(meta) or "noid"
    return f"{REINVEST_LOCK_PREFIX}:{jid}"


async def _acquire_lock(redis: RedisClient | None, key: str, ttl_s: int) -> str | None:
    if (not redis) or (not CFG.lock_enabled):
        return None
    token = secrets.token_hex(12)
    try:
        ok = await _redis_call(redis, "set", key, token, nx=True, ex=max(1, int(ttl_s)))
        return token if ok else None
    except Exception:
        return None


async def _release_lock(redis: RedisClient | None, key: str, token: str | None) -> None:
    if not redis or not token:
        return
    try:
        ev = getattr(redis, "eval", None)
        if callable(ev):
            await _maybe_await(ev(_RELEASE_LUA, 1, key, token))
            return
    except Exception:
        pass
    try:
        cur = _maybe_decode(await _redis_call(redis, "get", key))
        if cur == token:
            await _redis_call(redis, "delete", key)
    except Exception:
        return


# ──────────────────────────────────────────────────────────────────────────────
# Pricing helpers
# ──────────────────────────────────────────────────────────────────────────────
async def _sol_to_usd(amount_sol: float) -> float:
    try:
        convert = _core_attr("convert_sol_to_usd_amount")
        usd = await _call_maybe_async(convert, float(amount_sol))
        return float(usd or 0.0)
    except Exception:
        return 0.0


async def _token_price_in_sol(mint: str) -> float:
    mint2 = normalize_mint(mint)
    try:
        get_price = _core_attr("get_token_price")
        px_usd = await _call_maybe_async(get_price, mint2)
        if px_usd is None or float(px_usd) <= 0:
            return 0.0
        convert = _core_attr("convert_sol_to_usd_amount")
        sol_usd = await _call_maybe_async(convert, 1.0)
        if sol_usd is None or float(sol_usd) <= 0:
            return 0.0
        return float(px_usd) / float(sol_usd)
    except Exception:
        return 0.0


# ──────────────────────────────────────────────────────────────────────────────
# Telegram (rate-limited)
# ──────────────────────────────────────────────────────────────────────────────
async def _safe_tg(msg: str, redis: RedisClient | None = None) -> None:
    if not msg:
        return
    if redis and CFG.tg_min_seconds > 0:
        try:
            raw = _maybe_decode(await _redis_call(redis, "get", REINVEST_TG_LAST_TS_KEY))
            last = float(raw) if raw else 0.0
            if last and (_utc_ts() - last) < float(CFG.tg_min_seconds):
                return
            await _redis_call(redis, "set", REINVEST_TG_LAST_TS_KEY, str(_utc_ts()))
        except Exception:
            pass

    with contextlib.suppress(Exception):
        send_telegram = _core_attr("send_telegram")
        await _call_maybe_async(send_telegram, msg)


# ──────────────────────────────────────────────────────────────────────────────
# Savings transfer (signature drift tolerant)
# ──────────────────────────────────────────────────────────────────────────────
async def _send_to_savings(amount_sol: float, redis: RedisClient | None = None) -> None:
    if amount_sol <= 0:
        return
    if not CFG.savings_wallet:
        log.info("[REINVEST] SAVINGS_WALLET not set; keeping %.6f SOL in main wallet.", amount_sol)
        return

    try:
        transfer = _core_attr("transfer_to_savings")
        try:
            await _call_maybe_async(transfer, float(amount_sol))
        except TypeError:
            await _call_maybe_async(transfer, float(amount_sol), wallet=CFG.savings_wallet)

        usd = await _sol_to_usd(amount_sol)
        await _safe_tg(
            f"🏦 Moved {amount_sol:.4f} SOL (~${usd:.2f}) to savings wallet.",
            redis=redis,
        )
        await _log_event_safe(
            "reinvest_savings_transfer",
            {"amount_sol": amount_sol, "amount_usd": usd, "ts": _now_utc().isoformat()},
        )
    except Exception as e:
        log.warning("[REINVEST] Savings transfer failed: %s", str(e)[:200])


# ──────────────────────────────────────────────────────────────────────────────
# Target selection
# ──────────────────────────────────────────────────────────────────────────────
@dataclass
class TargetPick:
    mint: str
    score: float = 0.0
    symbol: str | None = None
    raw: Any = None


def _mint_allowed(mint: str) -> bool:
    m = normalize_mint(mint)
    if not m or len(m) <= 30:
        return False
    if m == SOL_MINT:
        return False
    if m in CFG.deny_mints:
        return False
    if CFG.allow_mints and m not in CFG.allow_mints:
        return False
    guard = _mint_guard_fn()
    if callable(guard):
        with contextlib.suppress(Exception):
            if not bool(guard(m)):
                return False
    return True


async def _pick_target_mint(
    reinvest_sol: float, meta: Meta, redis: RedisClient | None = None
) -> TargetPick | None:
    forced = meta.get("force_token_mint") or meta.get("target_mint")
    if isinstance(forced, str) and _mint_allowed(forced):
        return TargetPick(
            mint=normalize_mint(forced),
            score=9e9,
            symbol=(str(meta.get("target_symbol")) if meta.get("target_symbol") else None),
            raw={"forced": True},
        )

    picker = _pick_reinvestment_target_fn()
    if callable(picker):
        out: Any = None
        try:
            out = await _call_maybe_async(
                picker,
                budget_sol=reinvest_sol,
                min_24h_volume_usd=CFG.vol_min_usd,
                redis=redis,
                meta=meta,
            )
        except TypeError:
            with contextlib.suppress(Exception):
                out = await _call_maybe_async(picker, reinvest_sol)

        if isinstance(out, str) and _mint_allowed(out):
            return TargetPick(mint=normalize_mint(out), score=1.0, raw=out)

        if isinstance(out, dict):
            mint = out.get("mint") or out.get("token") or out.get("address")
            if isinstance(mint, str) and _mint_allowed(mint):
                sym = out.get("symbol") or out.get("sym")
                sc = _safe_float(out.get("score"), 0.0)
                return TargetPick(
                    mint=normalize_mint(mint),
                    score=sc,
                    symbol=(str(sym) if sym else None),
                    raw=out,
                )

        if isinstance(out, (list, tuple)):
            best: TargetPick | None = None
            for cand in out:
                if not isinstance(cand, dict):
                    continue
                mint = cand.get("mint") or cand.get("token") or cand.get("address")
                if not isinstance(mint, str) or not _mint_allowed(mint):
                    continue
                sc = _safe_float(cand.get("score"), 0.0)
                sym = cand.get("symbol") or cand.get("sym")
                tp = TargetPick(
                    mint=normalize_mint(mint),
                    score=sc,
                    symbol=(str(sym) if sym else None),
                    raw=cand,
                )
                if best is None or tp.score > best.score:
                    best = tp
            if best is not None:
                return best

    # Legacy fallback
    try:
        mint2 = await find_reinvestment_token(
            min_volume=float(CFG.vol_min_usd),
            honeypot_filter=bool(CFG.honeypot_check),
        )
        if isinstance(mint2, str) and _mint_allowed(mint2):
            return TargetPick(mint=normalize_mint(mint2), score=0.0, raw={"legacy": True})
    except Exception as e:
        log.warning("[REINVEST] legacy find_reinvestment_token failed: %s", str(e)[:200])

    return None


# ──────────────────────────────────────────────────────────────────────────────
# Context
# ──────────────────────────────────────────────────────────────────────────────
@dataclass
class ReinvestContext:
    proceeds_sol: float
    proceeds_usd: float
    meta: Meta

    risk_capacity: float
    risk_mode: int
    risk_mode_override: int | None
    halt_reason: str | None

    daily_pnl_pct: float
    daily_dd_pct: float

    equity_usd: float
    equity_sol: float

    daily_trades_count: int
    daily_trades_cap: int
    sandbox_mode: bool

    regime_label: str
    regime_label_numeric: float
    regime_stress_score: float
    regime_signal_strength: float
    regime_entry_gate_factor: float
    regime_risk_multiplier: float

    base_reinvest_pct: float
    stop_loss_percent: float
    slippage: float
    min_sol_balance: float
    max_risk_per_trade: float
    strat_mode: int
    hard_halt: bool

    plan_target_apr_pct: float
    plan_gap_pct: float
    plan_status: str

    kelly_suggestion: float
    kelly_confidence: float
    kelly_pos_cap_pct: float

    win_rate: float
    recent_win_rate: float
    pnl_mean: float
    pnl_std: float
    sharpe_like: float
    recent_sharpe_like: float

    tail_risk_score: float
    spend_remaining_usd: float
    spend_budget_block: bool
    equity_floor_usd: float
    equity_below_floor: bool
    risk_tags: list[str] = field(default_factory=list)

    infra_health_score: float = 1.0
    infra_rpc_latency_ms: float = 0.0
    infra_jup_latency_ms: float = 0.0
    infra_state_is_healthy: float = 1.0
    infra_state_is_degraded: float = 0.0
    infra_state_is_halted: float = 0.0
    infra_stability_score: float = 1.0
    infra_raw: dict[str, Any] = field(default_factory=dict)

    risk_ctx_raw: dict[str, Any] = field(default_factory=dict)
    perf_raw: dict[str, Any] = field(default_factory=dict)


@dataclass
class ReinvestScales:
    base_pct: float
    cap_scale: float
    dd_scale: float
    kelly_scale: float
    plan_scale: float
    regime_scale: float
    ai_scale: float
    perf_scale: float
    profile_scale: float
    mode_scale: float
    infra_scale: float
    tail_scale: float

    @property
    def eff_pct(self) -> float:
        return float(
            self.base_pct
            * self.cap_scale
            * self.dd_scale
            * self.kelly_scale
            * self.plan_scale
            * self.regime_scale
            * self.ai_scale
            * self.perf_scale
            * self.profile_scale
            * self.mode_scale
            * self.infra_scale
            * self.tail_scale
        )

    def as_dict(self) -> dict[str, float]:
        d = {
            "base_pct": float(self.base_pct),
            "cap_scale": float(self.cap_scale),
            "dd_scale": float(self.dd_scale),
            "kelly_scale": float(self.kelly_scale),
            "plan_scale": float(self.plan_scale),
            "regime_scale": float(self.regime_scale),
            "ai_scale": float(self.ai_scale),
            "perf_scale": float(self.perf_scale),
            "profile_scale": float(self.profile_scale),
            "mode_scale": float(self.mode_scale),
            "infra_scale": float(self.infra_scale),
            "tail_scale": float(self.tail_scale),
        }
        d["eff_pct"] = float(self.eff_pct)
        return d


async def _build_context(
    proceeds_sol: float, meta: Meta, redis: RedisClient | None
) -> ReinvestContext | None:
    meta2: Meta = dict(meta or {})
    proceeds_sol2 = float(proceeds_sol or 0.0)
    proceeds_usd = await _sol_to_usd(proceeds_sol2)

    try:
        risk_ctx0 = await risk_capacity_snapshot(redis, source="reinvestor")
    except TypeError:
        risk_ctx0 = await risk_capacity_snapshot(redis)
    except Exception:
        risk_ctx0 = await risk_capacity_snapshot()

    risk_ctx_any: Any = risk_ctx0
    risk_ctx: dict[str, Any] = risk_ctx_any if isinstance(risk_ctx_any, dict) else {}

    risk_capacity = max(0.0, min(1.0, _safe_float(risk_ctx.get("risk_capacity"), 1.0)))
    risk_mode = _safe_int(risk_ctx.get("risk_mode"), 1)

    rmo = risk_ctx.get("risk_mode_override")
    risk_mode_override: int | None = None
    if rmo is not None:
        with contextlib.suppress(Exception):
            risk_mode_override = int(rmo)

    halt_reason_raw = risk_ctx.get("halt_reason")
    halt_reason = str(halt_reason_raw) if halt_reason_raw is not None else None

    daily_pnl_pct = _safe_float(
        risk_ctx.get("daily_realized_pnl_pct") or risk_ctx.get("pnl_day_pct"), 0.0
    )
    daily_dd_pct = _safe_float(
        risk_ctx.get("daily_drawdown_pct")
        or risk_ctx.get("daily_dd_pct")
        or risk_ctx.get("rolling_dd_pct"),
        0.0,
    )

    equity_usd = _safe_float(risk_ctx.get("equity_usd"), 0.0)
    sandbox_mode = bool(risk_ctx.get("sandbox_mode", False))
    daily_trades_count = _safe_int(risk_ctx.get("daily_trades_count"), 0)
    daily_trades_cap = _safe_int(risk_ctx.get("daily_trades_cap"), 0)

    regime_label = str(risk_ctx.get("regime_label", "neutral") or "neutral")
    regime_label_numeric = _safe_float(risk_ctx.get("regime_label_numeric"), 0.0)
    regime_stress_score = _safe_float(risk_ctx.get("regime_stress_score"), 0.0)
    regime_signal_strength = _safe_float(risk_ctx.get("regime_signal_strength"), 0.5)
    regime_entry_gate_factor = _safe_float(risk_ctx.get("regime_entry_gate_factor"), 1.0)
    regime_risk_multiplier = _safe_float(risk_ctx.get("regime_risk_multiplier"), 1.0)

    tail_risk_score = _safe_float(risk_ctx.get("tail_risk_score"), 0.0)
    spend_remaining_usd = _safe_float(risk_ctx.get("daily_spend_remaining_usd"), 1e12)
    spend_budget_block = bool(risk_ctx.get("spend_budget_block", False))
    equity_floor_usd = _safe_float(risk_ctx.get("equity_floor_usd"), 0.0)
    equity_below_floor = bool(risk_ctx.get("equity_below_floor", False))

    rt = risk_ctx.get("risk_tags", [])
    if isinstance(rt, str):
        risk_tags = [rt]
    elif isinstance(rt, (list, tuple, set)):
        risk_tags = [str(x) for x in rt]
    else:
        risk_tags = []

    # equity sol (derived)
    try:
        convert = _core_attr("convert_sol_to_usd_amount")
        sol_usd = await _call_maybe_async(convert, 1.0)
        equity_sol = equity_usd / float(sol_usd) if sol_usd and float(sol_usd) > 0 else 0.0
    except Exception:
        equity_sol = 0.0

    params_any: Any = await get_strategy_params(redis)
    params: dict[str, Any] = params_any if isinstance(params_any, dict) else {}

    perf_any: Any = await get_perf_snapshot(redis)
    perf: dict[str, Any] = perf_any if isinstance(perf_any, dict) else {}

    base_reinvest_pct = _safe_float(
        risk_ctx.get("reinvest_base_pct"),
        _safe_float(params.get("REINVEST_PERCENT"), 75.0),
    )
    stop_loss_percent = _safe_float(params.get("STOP_LOSS_PERCENT"), 18.0)
    slippage = _normalize_slippage(_safe_float(params.get("SLIPPAGE"), 0.005))

    min_sol_balance = _safe_float(
        params.get("MIN_SOL_BALANCE"),
        _safe_float(risk_ctx.get("min_sol_balance"), 0.25),
    )
    max_risk_per_trade = _safe_float(
        params.get("MAX_RISK_PER_TRADE"),
        _safe_float(risk_ctx.get("max_risk_per_trade"), 0.05),
    )
    strat_mode = _safe_int(
        params.get("STRAT_MODE"),
        _safe_int(risk_ctx.get("strat_mode"), 1) or 1,
    )
    hard_halt = bool(params.get("HARD_HALT", False))

    plan_target_apr_pct = _safe_float(params.get("PLAN_TARGET_APR_PCT"), 180.0)
    plan_gap_pct = _safe_float(
        params.get("PLAN_GAP_PCT"),
        _safe_float(risk_ctx.get("plan_gap_pct"), 0.0),
    )
    plan_status = str(params.get("PLAN_STATUS", risk_ctx.get("plan_status", "n/a")) or "n/a")

    kelly_from_perf = _safe_float(perf.get("kelly_suggestion"), 0.0)
    kelly_from_risk = _safe_float(risk_ctx.get("kelly_raw"), 0.0)
    kelly_suggestion = kelly_from_perf if kelly_from_perf != 0.0 else kelly_from_risk
    if kelly_suggestion == 0.0:
        kelly_suggestion = 0.02

    kelly_pos_cap_pct = _safe_float(risk_ctx.get("kelly_pos_pct_cap"), 0.0)
    kelly_confidence = _safe_float(
        perf.get("kelly_confidence", risk_ctx.get("kelly_confidence", 0.0)),
        0.0,
    )

    win_rate = _safe_float(perf.get("win_rate", risk_ctx.get("winrate20", 0.0)), 0.0)
    recent_win_rate = _safe_float(perf.get("recent_win_rate", win_rate), win_rate)

    pnl_mean = _safe_float(perf.get("pnl_mean"), 0.0)
    pnl_std = _safe_float(perf.get("pnl_std"), 0.0)
    sharpe_like = _safe_float(perf.get("sharpe_like"), 0.0)
    recent_sharpe_like = _safe_float(perf.get("recent_sharpe_like", sharpe_like), sharpe_like)

    infra = await infra_features(redis)
    infra_health_score = (
        _safe_float(infra.get("infra_health_score"), 1.0) if isinstance(infra, dict) else 1.0
    )
    infra_rpc_latency_ms = (
        _safe_float(infra.get("infra_rpc_latency_ms"), 0.0) if isinstance(infra, dict) else 0.0
    )
    infra_jup_latency_ms = (
        _safe_float(infra.get("infra_jup_latency_ms"), 0.0) if isinstance(infra, dict) else 0.0
    )
    infra_state_is_healthy = (
        _safe_float(infra.get("infra_state_is_healthy"), 1.0) if isinstance(infra, dict) else 1.0
    )
    infra_state_is_degraded = (
        _safe_float(infra.get("infra_state_is_degraded"), 0.0) if isinstance(infra, dict) else 0.0
    )
    infra_state_is_halted = (
        _safe_float(infra.get("infra_state_is_halted"), 0.0) if isinstance(infra, dict) else 0.0
    )
    infra_stability_score = (
        _safe_float(infra.get("infra_stability_score"), 1.0) if isinstance(infra, dict) else 1.0
    )

    return ReinvestContext(
        proceeds_sol=proceeds_sol2,
        proceeds_usd=proceeds_usd,
        meta=meta2,
        risk_capacity=risk_capacity,
        risk_mode=risk_mode,
        risk_mode_override=risk_mode_override,
        halt_reason=halt_reason,
        daily_pnl_pct=daily_pnl_pct,
        daily_dd_pct=daily_dd_pct,
        equity_usd=equity_usd,
        equity_sol=equity_sol,
        daily_trades_count=daily_trades_count,
        daily_trades_cap=daily_trades_cap,
        sandbox_mode=sandbox_mode,
        regime_label=regime_label,
        regime_label_numeric=regime_label_numeric,
        regime_stress_score=regime_stress_score,
        regime_signal_strength=regime_signal_strength,
        regime_entry_gate_factor=regime_entry_gate_factor,
        regime_risk_multiplier=regime_risk_multiplier,
        base_reinvest_pct=base_reinvest_pct,
        stop_loss_percent=stop_loss_percent,
        slippage=slippage,
        min_sol_balance=min_sol_balance,
        max_risk_per_trade=max_risk_per_trade,
        strat_mode=strat_mode,
        hard_halt=hard_halt,
        plan_target_apr_pct=plan_target_apr_pct,
        plan_gap_pct=plan_gap_pct,
        plan_status=plan_status,
        kelly_suggestion=kelly_suggestion,
        kelly_confidence=kelly_confidence,
        kelly_pos_cap_pct=kelly_pos_cap_pct,
        win_rate=win_rate,
        recent_win_rate=recent_win_rate,
        pnl_mean=pnl_mean,
        pnl_std=pnl_std,
        sharpe_like=sharpe_like,
        recent_sharpe_like=recent_sharpe_like,
        tail_risk_score=tail_risk_score,
        spend_remaining_usd=spend_remaining_usd,
        spend_budget_block=spend_budget_block,
        equity_floor_usd=equity_floor_usd,
        equity_below_floor=equity_below_floor,
        risk_tags=risk_tags,
        infra_health_score=infra_health_score,
        infra_rpc_latency_ms=infra_rpc_latency_ms,
        infra_jup_latency_ms=infra_jup_latency_ms,
        infra_state_is_healthy=infra_state_is_healthy,
        infra_state_is_degraded=infra_state_is_degraded,
        infra_state_is_halted=infra_state_is_halted,
        infra_stability_score=infra_stability_score,
        infra_raw=dict(infra) if isinstance(infra, dict) else {},
        risk_ctx_raw=dict(risk_ctx),
        perf_raw=dict(perf),
    )


# ──────────────────────────────────────────────────────────────────────────────
# Health scoring + persistence
# ──────────────────────────────────────────────────────────────────────────────
def _compute_health_score(ctx: ReinvestContext) -> float:
    with contextlib.suppress(Exception):
        pre = ctx.risk_ctx_raw.get("reinvest_health_score") or ctx.risk_ctx_raw.get(
            "global_health_score"
        )
        if isinstance(pre, (int, float)) and float(pre) >= 0:
            return float(pre)

    wr = max(0.0, min(1.0, ctx.win_rate))
    wr_score = max(0.0, min(1.0, (wr - 0.30) / (0.70 - 0.30))) * 100.0

    dd = max(0.0, ctx.daily_dd_pct)
    max_dd_for_score = max(CFG.dd_hard_stop_pct, CFG.dd_soft_stop_pct, 25.0)
    dd_norm = (
        max(0.0, 1.0 - min(dd, max_dd_for_score) / max_dd_for_score)
        if max_dd_for_score > 0
        else 1.0
    )
    dd_score = dd_norm * 100.0

    cap = max(0.0, min(1.0, ctx.risk_capacity))
    cap_score = cap * 100.0

    sharpe = ctx.recent_sharpe_like or ctx.sharpe_like or 0.0
    sharpe_clamped = max(-1.5, min(1.5, sharpe))
    sharpe_score = max(0.0, min(1.0, (sharpe_clamped + 1.0) / 2.0)) * 100.0

    infra_score = max(0.0, min(1.0, ctx.infra_health_score)) * 100.0

    t = max(0.0, min(1.0, float(ctx.tail_risk_score or 0.0)))
    tail_score = (1.0 - t) * 100.0

    return float(
        0.28 * wr_score
        + 0.20 * dd_score
        + 0.20 * cap_score
        + 0.10 * sharpe_score
        + 0.10 * infra_score
        + 0.12 * tail_score
    )


async def _update_reinvest_health(
    redis: RedisClient | None,
    ctx: ReinvestContext,
    eff_pct: float,
    reinvest_sol: float,
    savings_sol: float,
    reason: str = "ok",
) -> None:
    if not redis:
        return
    try:
        health_doc: dict[str, Any] = {
            "ts": _now_utc().isoformat(),
            "phase": int(CFG.phase),
            "reason": reason,
            "eff_pct": float(eff_pct),
            "proceeds_sol": float(ctx.proceeds_sol),
            "reinvest_sol": float(reinvest_sol),
            "savings_sol": float(savings_sol),
            "reinvest_usd": await _sol_to_usd(reinvest_sol),
            "savings_usd": await _sol_to_usd(savings_sol),
            "health_score": _compute_health_score(ctx),
            "risk_capacity": ctx.risk_capacity,
            "risk_mode": ctx.risk_mode,
            "risk_mode_override": ctx.risk_mode_override,
            "halt_reason": ctx.halt_reason,
            "daily_pnl_pct": ctx.daily_pnl_pct,
            "daily_dd_pct": ctx.daily_dd_pct,
            "win_rate": ctx.win_rate,
            "recent_win_rate": ctx.recent_win_rate,
            "sharpe_like": ctx.sharpe_like,
            "recent_sharpe_like": ctx.recent_sharpe_like,
            "plan_status": ctx.plan_status,
            "plan_gap_pct": ctx.plan_gap_pct,
            "regime_label": ctx.regime_label,
            "regime_label_numeric": ctx.regime_label_numeric,
            "regime_stress_score": ctx.regime_stress_score,
            "regime_signal_strength": ctx.regime_signal_strength,
            "regime_entry_gate_factor": ctx.regime_entry_gate_factor,
            "tail_risk_score": ctx.tail_risk_score,
            "spend_remaining_usd": ctx.spend_remaining_usd,
            "spend_budget_block": ctx.spend_budget_block,
            "equity_floor_usd": ctx.equity_floor_usd,
            "equity_below_floor": ctx.equity_below_floor,
            "daily_trades_count": ctx.daily_trades_count,
            "daily_trades_cap": ctx.daily_trades_cap,
            "sandbox_mode": ctx.sandbox_mode,
            "infra_health_score": ctx.infra_health_score,
            "infra_rpc_latency_ms": ctx.infra_rpc_latency_ms,
            "infra_jup_latency_ms": ctx.infra_jup_latency_ms,
            "infra_state_is_halted": ctx.infra_state_is_halted,
            "infra_state_is_degraded": ctx.infra_state_is_degraded,
            "infra_stability_score": ctx.infra_stability_score,
        }
        await _redis_call(redis, "set", REINVEST_HEALTH_KEY, _json_dumps(health_doc))
        await _log_event_safe("reinvest_health", health_doc)
    except Exception:
        return


async def get_reinvest_health(redis: RedisClient | None = None) -> dict[str, Any] | None:
    if redis is None:
        redis = await _get_redis()
    if not redis:
        return None
    try:
        raw = _maybe_decode(await _redis_call(redis, "get", REINVEST_HEALTH_KEY))
        return json.loads(raw) if raw else None
    except Exception:
        return None


async def get_last_reinvest_decision(redis: RedisClient | None = None) -> dict[str, Any] | None:
    if redis is None:
        redis = await _get_redis()
    if not redis:
        return None
    try:
        raw = _maybe_decode(await _redis_call(redis, "get", REINVEST_DECISION_KEY))
        return json.loads(raw) if raw else None
    except Exception:
        return None


async def get_recent_reinvest_decisions(
    limit: int = 50, redis: RedisClient | None = None
) -> list[dict[str, Any]]:
    if redis is None:
        redis = await _get_redis()
    if not redis:
        return []
    limit2 = max(1, min(int(limit or 1), 1000))
    try:
        raw_list = await _redis_call(redis, "lrange", REINVEST_DECISION_LIST_KEY, 0, limit2 - 1)
        out: list[dict[str, Any]] = []
        for item in raw_list or []:
            try:
                doc = json.loads(_maybe_decode(item))
                if isinstance(doc, dict):
                    out.append(doc)
            except Exception:
                continue
        return out
    except Exception:
        return []


async def _persist_decision(redis: RedisClient | None, decision_doc: dict[str, Any]) -> None:
    if not redis:
        return
    try:
        payload = _json_dumps(decision_doc, separators=(",", ":"))
        await _redis_call(redis, "set", REINVEST_DECISION_KEY, payload)
        await _redis_call(redis, "lpush", REINVEST_DECISION_LIST_KEY, payload)
        await _redis_call(
            redis, "ltrim", REINVEST_DECISION_LIST_KEY, 0, int(CFG.decisions_keep) - 1
        )
    except Exception:
        return


# ──────────────────────────────────────────────────────────────────────────────
# Scales / effective %
# ──────────────────────────────────────────────────────────────────────────────
def _kelly_scale(ctx: ReinvestContext) -> float:
    kelly = max(0.0, ctx.kelly_suggestion)
    target = ctx.kelly_pos_cap_pct if ctx.kelly_pos_cap_pct > 0 else 0.08
    target = max(0.01, min(float(target), 0.15))

    if kelly <= 0.005 or ctx.win_rate < 0.45 or ctx.pnl_mean < 0:
        base_scale = 0.5
    else:
        ratio = min(2.0, max(0.3, kelly / target))
        base_scale = 0.7 + 0.6 * ((ratio - 0.3) / (2.0 - 0.3))

    conf = float(ctx.kelly_confidence or 0.0)
    if 0.0 < conf < 1.0:
        return 1.0 + (base_scale - 1.0) * conf
    return base_scale


def _capacity_scale(ctx: ReinvestContext) -> float:
    cap = max(0.0, min(1.0, ctx.risk_capacity))
    scale = max(0.25, min(1.0, cap))
    if ctx.risk_mode == 3:
        scale *= 0.5
    elif ctx.risk_mode == 2:
        scale *= 1.1
    if ctx.strat_mode == 0:
        scale *= 0.7
    elif ctx.strat_mode == 2:
        scale *= 1.1
    return scale


def _drawdown_scale(ctx: ReinvestContext) -> float:
    dd = max(0.0, ctx.daily_dd_pct)
    if CFG.dd_soft_stop_pct <= 0 or CFG.dd_hard_stop_pct <= CFG.dd_soft_stop_pct:
        return 1.0
    if dd < CFG.dd_soft_stop_pct:
        return 1.0
    span = max(CFG.dd_hard_stop_pct - CFG.dd_soft_stop_pct, 1e-6)
    mag = min(CFG.dd_hard_stop_pct, dd)
    frac = max(0.0, min(1.0, (mag - CFG.dd_soft_stop_pct) / span))
    return max(0.2, 1.0 - 0.8 * frac)


def _plan_scale(ctx: ReinvestContext) -> float:
    status, gap, dd, vol, cap = (
        ctx.plan_status,
        ctx.plan_gap_pct,
        ctx.daily_dd_pct,
        ctx.pnl_std,
        ctx.risk_capacity,
    )
    scale = 1.0
    if status == "behind" and gap < -3.0:
        if cap > 0.6 and dd < CFG.dd_soft_stop_pct:
            scale *= 1.05
    elif status == "ahead" and gap > 3.0 and (dd >= CFG.dd_soft_stop_pct or vol > 20.0):
        scale *= 0.9
    return scale


def _regime_scale(ctx: ReinvestContext) -> float:
    lab = ctx.regime_label_numeric
    stress = ctx.regime_stress_score
    sig = ctx.regime_signal_strength
    gate = ctx.regime_entry_gate_factor

    scale = 1.0
    if lab < -0.3 or stress > 0.7:
        scale *= 0.7
    elif lab > 0.3 and sig > 0.4 and stress < 0.4:
        scale *= 1.08

    if 0.1 <= gate <= 2.0:
        if gate < 0.95:
            scale *= 0.9 + 0.1 * gate
        elif gate > 1.05:
            scale *= 1.0 + 0.05 * (gate - 1.0)
    return scale


def _ai_scale(ctx: ReinvestContext) -> float:
    delta = float(ctx.risk_ctx_raw.get("ai_capacity_delta", 0.0) or 0.0)
    if delta < -0.6:
        return 0.7
    if delta < -0.1:
        return 0.85
    if delta > 0.3:
        return 1.05
    if delta > 0.1:
        return 1.03
    return 1.0


def _perf_scale(ctx: ReinvestContext) -> float:
    rec_wr = ctx.recent_win_rate or ctx.win_rate
    sharpe = ctx.recent_sharpe_like or ctx.sharpe_like
    scale = 1.0
    if rec_wr < 0.38 and sharpe < -0.2:
        scale *= 0.7
    elif rec_wr > 0.55 and sharpe > 0.3:
        scale *= 1.07
    return scale


def _profile_scale(meta: Meta) -> float:
    raw = meta.get("profile") or meta.get("bucket") or meta.get("reinvest_profile")
    if raw is None:
        return 1.0
    try:
        p = str(raw).lower()
    except Exception:
        return 1.0
    if p in ("core", "base"):
        s = 1.0
    elif p in ("growth", "momentum", "trend"):
        s = 1.05
    elif p in ("explore", "experimental", "beta"):
        s = 1.10
    elif p in ("defensive", "income", "savings"):
        s = 0.75
    elif p in ("degen", "aggressive"):
        s = 1.15
    else:
        s = 1.0
    return max(0.5, min(1.2, s))


def _mode_scale(ctx: ReinvestContext) -> float:
    meta = ctx.meta or {}
    raw = (
        meta.get("mode")
        or meta.get("reinvest_mode")
        or ctx.risk_ctx_raw.get("reinvest_mode")
        or _envs("REINVEST_MODE", "balanced")
    )
    try:
        mode = str(raw).strip().lower()
    except Exception:
        mode = "balanced"

    if mode in ("off", "savings_only", "halt", "park"):
        s = 0.05
    elif mode in ("repair", "rebuild", "recover", "capital_preserve"):
        s = 0.4
    elif mode in ("harvest", "cashout", "skim"):
        s = 0.6
    elif mode in ("snowball", "growth", "hyper", "aggressive_compound"):
        s = 1.12
    else:
        s = 1.0
    return max(0.0, min(1.25, s))


def _infra_scale(ctx: ReinvestContext) -> float:
    halted = ctx.infra_state_is_halted > 0.5
    degraded = ctx.infra_state_is_degraded > 0.5
    healthy = ctx.infra_state_is_healthy > 0.5
    infra_h = max(0.0, min(1.0, ctx.infra_health_score))
    stability = max(0.0, min(1.0, ctx.infra_stability_score))
    if halted:
        return 0.3
    if degraded:
        return 0.8
    if healthy and infra_h > 0.7 and stability > 0.7:
        return 1.03
    return 1.0


def _tail_scale(ctx: ReinvestContext) -> float:
    t = max(0.0, min(1.0, float(ctx.tail_risk_score or 0.0)))
    soft = max(0.0, min(1.0, CFG.tail_soft))
    hard = max(soft, min(1.0, CFG.tail_hard))
    if t <= 0.2:
        return 1.05
    if t >= hard:
        return 0.5
    if t >= soft:
        span = max(hard - soft, 1e-6)
        frac = (t - soft) / span
        return max(0.7, 1.0 - 0.3 * frac)
    return 0.95


def _compute_scales(ctx: ReinvestContext) -> ReinvestScales:
    return ReinvestScales(
        base_pct=float(ctx.base_reinvest_pct),
        cap_scale=_capacity_scale(ctx),
        dd_scale=_drawdown_scale(ctx),
        kelly_scale=_kelly_scale(ctx),
        plan_scale=_plan_scale(ctx),
        regime_scale=_regime_scale(ctx),
        ai_scale=_ai_scale(ctx),
        perf_scale=_perf_scale(ctx),
        profile_scale=_profile_scale(ctx.meta),
        mode_scale=_mode_scale(ctx),
        infra_scale=_infra_scale(ctx),
        tail_scale=_tail_scale(ctx),
    )


def _apply_meta_eff_overrides(eff_pct: float, meta: Meta) -> float:
    if "force_eff_pct" in meta:
        with contextlib.suppress(Exception):
            forced = float(meta["force_eff_pct"])
            return max(CFG.min_eff_pct, min(CFG.max_eff_pct, forced))

    raw_min = meta.get("min_eff_pct")
    raw_max = meta.get("max_eff_pct")

    min_eff_v = float(CFG.min_eff_pct)
    max_eff_v = float(CFG.max_eff_pct)

    if raw_min is not None:
        with contextlib.suppress(Exception):
            min_eff_v = float(raw_min)
    if raw_max is not None:
        with contextlib.suppress(Exception):
            max_eff_v = float(raw_max)

    eff_pct2 = max(min_eff_v, eff_pct)
    eff_pct2 = min(float(CFG.max_eff_pct), max_eff_v, eff_pct2)
    return max(0.0, eff_pct2)


def _compute_eff_pct(ctx: ReinvestContext) -> tuple[float, ReinvestScales]:
    scales = _compute_scales(ctx)
    eff = float(scales.eff_pct)
    if CFG.always_savings_pct > 0:
        eff = max(0.0, eff - float(CFG.always_savings_pct))
    eff = _apply_meta_eff_overrides(eff, ctx.meta)
    return eff, scales


# ──────────────────────────────────────────────────────────────────────────────
# Gates + journaling
# ──────────────────────────────────────────────────────────────────────────────
async def _evaluate_gates(ctx: ReinvestContext, redis: RedisClient | None) -> tuple[bool, str]:
    meta = ctx.meta or {}
    force_anyway = bool(meta.get("force_reinvest_even_if_risk_blocked", False))

    if ctx.proceeds_sol <= 0:
        return False, "no_proceeds"
    if ctx.proceeds_sol < CFG.min_proceeds_sol:
        return False, "below_min_proceeds_sol"
    if CFG.min_proceeds_usd > 0 and ctx.proceeds_usd < CFG.min_proceeds_usd:
        return False, "below_min_proceeds_usd"
    if (not CFG.enabled) or meta.get("force_savings_only"):
        return False, "reinvestor_disabled_or_forced_savings"
    if CFG.block_weekends and _is_weekend() and not meta.get("ignore_time_gate"):
        return False, "weekend_guard"

    if redis and CFG.max_reinvests_per_day > 0:
        if (await _get_daily_reinvest_count(redis)) >= CFG.max_reinvests_per_day:
            return False, "daily_cap_reached"

    if ctx.sandbox_mode and not meta.get("dry_run"):
        meta["dry_run"] = True

    if ctx.equity_below_floor and not meta.get("ignore_health_gate"):
        return False, "equity_below_floor_flag"
    if CFG.min_equity_sol > 0 and ctx.equity_sol > 0 and ctx.equity_sol < CFG.min_equity_sol:
        return False, "equity_below_min_env"

    if redis and CFG.cooldown_on_fail_min > 0 and not meta.get("ignore_time_gate"):
        last_fail = await _get_last_fail_ts(redis)
        if last_fail and (_utc_ts() - float(last_fail)) < float(CFG.cooldown_on_fail_min) * 60.0:
            return False, "cooldown_on_fail"

    if CFG.min_minutes_between > 0 and redis and not meta.get("ignore_time_gate"):
        last_ts = await _get_last_reinvest_ts(redis)
        if last_ts:
            elapsed_min = (_utc_ts() - float(last_ts)) / 60.0
            if elapsed_min < CFG.min_minutes_between:
                return False, "min_gap_between_reinvests"

    if ctx.hard_halt and not force_anyway:
        return False, "hard_halt"

    if not force_anyway:
        if any(
            tag in ("halt_all", "reinvestor_off", "reinvestor_hard_off", "halt_reinvestor")
            for tag in ctx.risk_tags
        ):
            return False, "risk_tag_halt"
        if any(tag in ("reinvestor_soft_off", "cooldown_reinvestor") for tag in ctx.risk_tags):
            return False, "risk_tag_soft_halt"

    if (ctx.daily_trades_cap > 0 and ctx.daily_trades_count >= ctx.daily_trades_cap) and (
        not force_anyway
    ):
        return False, "daily_trades_cap_reached"

    if ctx.spend_budget_block and not force_anyway:
        return False, "spend_budget_block"
    if ctx.spend_remaining_usd <= 0.0 and not force_anyway:
        return False, "spend_budget_exhausted"

    try:
        allow_entry = await should_allow_new_entry(redis, source="reinvestor")
    except TypeError:
        allow_entry = await should_allow_new_entry(redis)
    except Exception:
        allow_entry = True

    if ((not allow_entry) or ctx.risk_capacity < CFG.min_capacity or ctx.risk_mode == 0) and (
        not force_anyway
    ):
        return False, "risk_blocked"
    if ctx.risk_mode == 3 and ctx.risk_capacity < CFG.defensive_capacity and not force_anyway:
        return False, "defensive_capacity_low"

    if CFG.dd_hard_stop_pct > 0 and ctx.daily_dd_pct >= CFG.dd_hard_stop_pct:
        return False, "deep_drawdown"

    if CFG.pnl_lock_pct > 0 and ctx.daily_pnl_pct >= CFG.pnl_lock_pct and not force_anyway:
        return False, "pnl_day_high"

    if (
        (ctx.win_rate < CFG.health_min_winrate)
        and (ctx.recent_win_rate < CFG.health_min_recent_winrate)
        and (ctx.sharpe_like < CFG.health_min_sharpe)
        and not meta.get("ignore_health_gate")
        and not force_anyway
    ):
        return False, "perf_too_weak"

    health = _compute_health_score(ctx)
    if (
        CFG.health_min_score > 0.0
        and not meta.get("ignore_health_gate")
        and health < CFG.health_min_score
        and not force_anyway
    ):
        return False, "health_score_low"

    if (
        CFG.tail_gate > 0.0
        and not meta.get("ignore_health_gate")
        and ctx.tail_risk_score >= CFG.tail_gate
        and not force_anyway
    ):
        return False, "tail_risk_high"

    if not meta.get("ignore_infra_gate") and not force_anyway:
        infra_block = await infra_should_block_entries(redis)
        infra_halted = ctx.infra_state_is_halted > 0.5
        infra_degraded = ctx.infra_state_is_degraded > 0.5

        if infra_block and CFG.block_if_infra_halted:
            return False, "infra_guard_block"
        if infra_halted and CFG.block_if_infra_halted:
            return False, "infra_halted"
        if infra_degraded and CFG.block_if_infra_degraded:
            return False, "infra_degraded"
        if CFG.infra_min_health > 0.0 and ctx.infra_health_score < CFG.infra_min_health:
            return False, "infra_health_low"

    if ctx.regime_entry_gate_factor < 0.2 and not force_anyway:
        return False, "regime_gate_low"
    if float(ctx.risk_ctx_raw.get("ai_capacity_delta", 0.0) or 0.0) < -0.8 and not force_anyway:
        return False, "ai_capacity_tight"

    return True, "ok"


async def _journal_skip(
    reason: str, ctx: ReinvestContext | None, meta: Meta, redis: RedisClient | None = None
) -> None:
    payload: dict[str, Any] = {
        "reason": reason,
        "proceeds_sol": float(ctx.proceeds_sol)
        if ctx
        else _safe_float(meta.get("proceeds_sol"), 0.0),
        "meta": dict(meta),
        "ts": _now_utc().isoformat(),
    }
    if ctx is not None:
        payload["ctx"] = {
            "risk_capacity": ctx.risk_capacity,
            "risk_mode": ctx.risk_mode,
            "risk_mode_override": ctx.risk_mode_override,
            "halt_reason": ctx.halt_reason,
            "daily_pnl_pct": ctx.daily_pnl_pct,
            "daily_dd_pct": ctx.daily_dd_pct,
            "equity_sol": ctx.equity_sol,
            "plan_status": ctx.plan_status,
            "plan_gap_pct": ctx.plan_gap_pct,
            "sandbox_mode": ctx.sandbox_mode,
            "daily_trades_count": ctx.daily_trades_count,
            "daily_trades_cap": ctx.daily_trades_cap,
            "infra_health_score": ctx.infra_health_score,
            "infra_state_is_halted": ctx.infra_state_is_halted,
            "infra_state_is_degraded": ctx.infra_state_is_degraded,
            "tail_risk_score": ctx.tail_risk_score,
            "spend_remaining_usd": ctx.spend_remaining_usd,
            "spend_budget_block": ctx.spend_budget_block,
            "risk_tags": ctx.risk_tags,
        }

    await note_reinvest_skipped(
        reason=reason,
        proceeds_sol=payload["proceeds_sol"],
        ctx=(payload.get("ctx") or {}),
        meta=meta,
        redis=redis,
    )
    await _log_event_safe("reinvest_skipped", payload)


# ──────────────────────────────────────────────────────────────────────────────
# Swap adapter (signature/return drift tolerant)
# ──────────────────────────────────────────────────────────────────────────────
async def _do_swap_buy(
    sol_in: float, out_mint: str, slippage: float
) -> tuple[bool, str | None, Any]:
    out: Any = None
    err: str | None = None

    out_mint2 = normalize_mint(out_mint)
    slip = _normalize_slippage(slippage)

    swap = _core_attr("swap_token_jupiter")
    try:
        out = await _call_maybe_async(
            swap,
            SOL_MINT,
            out_mint2,
            amount_float=sol_in,
            buy=True,
            slippage=slip,
        )
    except TypeError as e:
        err = str(e)
        with contextlib.suppress(Exception):
            out = await _call_maybe_async(swap, SOL_MINT, out_mint2, sol_in, True, slip)
        if out is None:
            with contextlib.suppress(Exception):
                out = await _call_maybe_async(swap, SOL_MINT, out_mint2, sol_in)
    except Exception as e:
        err = str(e)

    if out is None:
        return False, None, {"error": err or "swap_return_none"}

    if isinstance(out, tuple) and len(out) >= 2:
        ok = bool(out[0])
        sig = out[1]
        return ok, str(sig) if sig else None, out

    if isinstance(out, dict):
        ok = bool(out.get("ok", True))
        sig = out.get("tx_sig") or out.get("signature") or out.get("sig")
        return ok, str(sig) if sig else None, out

    if isinstance(out, str):
        return True, out, out

    return bool(out), None, out


# ──────────────────────────────────────────────────────────────────────────────
# Core reinvest
# ──────────────────────────────────────────────────────────────────────────────
async def reinvest(
    proceeds_sol: float, meta: dict[str, Any] | None = None, redis: RedisClient | None = None
) -> str | None:
    meta2: Meta = dict(meta or {})
    proceeds_sol2 = float(proceeds_sol or 0.0)
    if proceeds_sol2 <= 0:
        return None

    if CFG.global_dry_run:
        meta2["dry_run"] = True
    if meta2.get("sandbox") or meta2.get("sandbox_mode"):
        meta2["dry_run"] = True

    _ensure_job_id(meta2, proceeds_sol2)
    meta2.setdefault("proceeds_sol", proceeds_sol2)

    if redis is None:
        redis = await _get_redis()

    t0 = time.monotonic()

    if redis and not await _idempotency_claim(redis, meta2):
        log.info("[REINVEST] Idempotency: already processed. job_id=%s", meta2.get("job_id"))
        await _journal_skip("idempotency_already_processed", None, meta2, redis=redis)
        return None

    lock_token: str | None = None
    lk = _lock_key(meta2)
    if redis and CFG.lock_enabled:
        lock_token = await _acquire_lock(redis, lk, CFG.lock_ttl_s)
        if not lock_token:
            log.info("[REINVEST] Lock busy -> skip. key=%s", lk)
            await _journal_skip("lock_busy", None, meta2, redis=redis)
            return None

    try:
        ctx = await _build_context(proceeds_sol2, meta2, redis)
        if ctx is None:
            await _journal_skip("context_build_failed", None, meta2, redis=redis)
            await _send_to_savings(proceeds_sol2, redis=redis)
            return None

        allowed, reason = await _evaluate_gates(ctx, redis)
        meta2 = ctx.meta

        eff_pct, scales = _compute_eff_pct(ctx)
        eff_pct = max(CFG.min_eff_pct, min(CFG.max_eff_pct, eff_pct))

        reinvest_sol = proceeds_sol2 * (eff_pct / 100.0)
        savings_sol = max(0.0, proceeds_sol2 - reinvest_sol)

        if CFG.max_budget_fraction > 0 and ctx.spend_remaining_usd > 0:
            cap_usd = float(CFG.max_budget_fraction) * float(ctx.spend_remaining_usd)
            if cap_usd > 0:
                reinv_usd = await _sol_to_usd(reinvest_sol)
                if reinv_usd > cap_usd and reinv_usd > 0:
                    scale_down = cap_usd / max(reinv_usd, 1e-9)
                    reinvest_sol = max(0.0, reinvest_sol * scale_down)
                    savings_sol = max(0.0, proceeds_sol2 - reinvest_sol)

        health_score = _compute_health_score(ctx)

        decision_doc: dict[str, Any] = {
            "ts": _now_utc().isoformat(),
            "job_id": meta2.get("job_id"),
            "allowed": bool(allowed),
            "reason": reason,
            "proceeds_sol": proceeds_sol2,
            "proceeds_usd": ctx.proceeds_usd,
            "eff_pct": eff_pct,
            "reinvest_sol": reinvest_sol,
            "savings_sol": savings_sol,
            "health_score": health_score,
            "scales": scales.as_dict(),
            "risk_capacity": ctx.risk_capacity,
            "risk_mode": ctx.risk_mode,
            "plan_status": ctx.plan_status,
            "plan_gap_pct": ctx.plan_gap_pct,
            "infra_health_score": ctx.infra_health_score,
            "tail_risk_score": ctx.tail_risk_score,
            "meta": meta2,
        }
        if redis:
            await _persist_decision(redis, decision_doc)

        if not allowed:
            log.info("[REINVEST] Gate blocked (%s) -> savings.", reason)
            await _journal_skip(reason, ctx, meta2, redis=redis)
            await _send_to_savings(proceeds_sol2, redis=redis)
            await _update_reinvest_health(
                redis,
                ctx,
                eff_pct=0.0,
                reinvest_sol=0.0,
                savings_sol=proceeds_sol2,
                reason=reason,
            )
            return None

        await note_reinvest_decision(
            proceeds_sol=proceeds_sol2,
            eff_pct_raw=float(scales.eff_pct),
            eff_pct_final=float(eff_pct),
            scales=scales.as_dict(),
            ctx={
                "health_score": health_score,
                "risk_capacity": ctx.risk_capacity,
                "risk_mode": ctx.risk_mode,
                "risk_mode_override": ctx.risk_mode_override,
                "daily_pnl_pct": ctx.daily_pnl_pct,
                "daily_dd_pct": ctx.daily_dd_pct,
                "equity_sol": ctx.equity_sol,
                "plan_status": ctx.plan_status,
                "plan_gap_pct": ctx.plan_gap_pct,
                "kelly_suggestion": ctx.kelly_suggestion,
                "kelly_confidence": ctx.kelly_confidence,
                "regime_label_numeric": ctx.regime_label_numeric,
                "regime_stress_score": ctx.regime_stress_score,
                "regime_signal_strength": ctx.regime_signal_strength,
                "regime_entry_gate_factor": ctx.regime_entry_gate_factor,
                "ai_capacity_delta": float(ctx.risk_ctx_raw.get("ai_capacity_delta", 0.0) or 0.0),
                "infra_health_score": ctx.infra_health_score,
                "tail_risk_score": ctx.tail_risk_score,
                "spend_remaining_usd": ctx.spend_remaining_usd,
                "spend_budget_block": ctx.spend_budget_block,
            },
            meta=meta2,
            redis=redis,
        )

        if ctx.equity_sol > 0 and ctx.max_risk_per_trade > 0:
            cap_sol = float(ctx.max_risk_per_trade) * float(ctx.equity_sol)
            if reinvest_sol > cap_sol:
                reinvest_sol = cap_sol
                savings_sol = max(0.0, proceeds_sol2 - reinvest_sol)

        if CFG.max_single_usd > 0:
            usd_full = await _sol_to_usd(reinvest_sol)
            if usd_full > CFG.max_single_usd and usd_full > 0:
                scale_down = float(CFG.max_single_usd) / max(float(usd_full), 1e-9)
                reinvest_sol = max(0.0, reinvest_sol * scale_down)
                savings_sol = max(0.0, proceeds_sol2 - reinvest_sol)

        if reinvest_sol < CFG.min_proceeds_sol:
            await _journal_skip("reinvest_leg_too_small", ctx, meta2, redis=redis)
            await _send_to_savings(proceeds_sol2, redis=redis)
            await _update_reinvest_health(
                redis,
                ctx,
                eff_pct=0.0,
                reinvest_sol=0.0,
                savings_sol=proceeds_sol2,
                reason="too_small",
            )
            return None

        get_wallet_balance_sol = _core_opt("get_wallet_balance_sol")
        if callable(get_wallet_balance_sol) and not meta2.get("ignore_balance_gate"):
            with contextlib.suppress(Exception):
                bal = float(await _call_maybe_async(get_wallet_balance_sol))
                if (bal - proceeds_sol2) >= 0 and (bal - proceeds_sol2) < float(
                    ctx.min_sol_balance
                ):
                    await _journal_skip("min_sol_reserve_gate", ctx, meta2, redis=redis)
                    await _send_to_savings(proceeds_sol2, redis=redis)
                    await _update_reinvest_health(
                        redis,
                        ctx,
                        eff_pct=0.0,
                        reinvest_sol=0.0,
                        savings_sol=proceeds_sol2,
                        reason="min_reserve",
                    )
                    return None

        if meta2.get("dry_run"):
            await _update_reinvest_health(
                redis,
                ctx,
                eff_pct=eff_pct,
                reinvest_sol=reinvest_sol,
                savings_sol=savings_sol,
                reason="dry_run",
            )
            return None

        pick = await _pick_target_mint(reinvest_sol, meta=meta2, redis=redis)
        if not pick:
            await _safe_tg(
                "⚠️ Reinvestment skipped — no suitable target met filters right now.", redis=redis
            )
            await _journal_skip("no_target", ctx, meta2, redis=redis)
            await _update_reinvest_health(
                redis,
                ctx,
                eff_pct=0.0,
                reinvest_sol=0.0,
                savings_sol=proceeds_sol2,
                reason="no_target",
            )
            await note_reinvest_result(
                mint=None,
                proceeds_sol=proceeds_sol2,
                reinvest_sol=0.0,
                savings_sol=proceeds_sol2,
                eff_pct=0.0,
                success=False,
                tx_sig=None,
                ctx={"health_score": health_score, "tail_risk_score": ctx.tail_risk_score},
                redis=redis,
            )
            return None

        if CFG.require_price and not meta2.get("ignore_price_gate"):
            px_sol = await _token_price_in_sol(pick.mint)
            if px_sol <= 0:
                await _safe_tg(
                    "⚠️ Reinvestment skipped — target has no reliable price feed.", redis=redis
                )
                await _journal_skip("no_price_for_target", ctx, meta2, redis=redis)
                await _update_reinvest_health(
                    redis,
                    ctx,
                    eff_pct=0.0,
                    reinvest_sol=0.0,
                    savings_sol=proceeds_sol2,
                    reason="no_price",
                )
                return None

        if savings_sol > 0 and (not CFG.savings_after_swap):
            await _send_to_savings(savings_sol, redis=redis)

        ok, tx_sig, raw_swap = await _do_swap_buy(reinvest_sol, pick.mint, ctx.slippage)
        if not ok:
            await _safe_tg(f"❌ Reinvestment buy failed for `{pick.mint}`.", redis=redis)
            await _journal_skip("swap_failed", ctx, meta2, redis=redis)
            await _update_reinvest_health(
                redis,
                ctx,
                eff_pct=0.0,
                reinvest_sol=0.0,
                savings_sol=proceeds_sol2,
                reason="swap_failed",
            )
            if redis and CFG.cooldown_on_fail_min > 0:
                await _set_last_fail_ts(redis)
            await _send_to_savings(proceeds_sol2, redis=redis)
            await note_reinvest_result(
                mint=pick.mint,
                proceeds_sol=proceeds_sol2,
                reinvest_sol=0.0,
                savings_sol=proceeds_sol2,
                eff_pct=0.0,
                success=False,
                tx_sig=None,
                ctx={
                    "health_score": health_score,
                    "tail_risk_score": ctx.tail_risk_score,
                    "swap": raw_swap,
                },
                redis=redis,
            )
            return None

        if savings_sol > 0 and CFG.savings_after_swap:
            await _send_to_savings(savings_sol, redis=redis)

        buy_price_sol = await _token_price_in_sol(pick.mint)
        trailing_stop = (
            (buy_price_sol * (1.0 - (ctx.stop_loss_percent / 100.0))) if buy_price_sol > 0 else 0.0
        )
        now_iso = _now_utc().isoformat()

        infra_halted = bool(ctx.infra_state_is_halted > 0.5)
        infra_degraded = bool(ctx.infra_state_is_degraded > 0.5)
        ai_capacity_delta = float(ctx.risk_ctx_raw.get("ai_capacity_delta", 0.0) or 0.0)

        position_payload: dict[str, Any] = {
            "token_address": pick.mint,
            "buy_price": float(buy_price_sol),
            "amount_invested_sol": float(reinvest_sol),
            "timestamp": now_iso,
            "trailing_stop": float(trailing_stop),
            "source": meta2.get("tag") or "reinvestor",
            "origin_trade_id": meta2.get("origin_trade_id"),
            "origin_position_id": meta2.get("origin_position_id"),
            "meta": {
                **meta2,
                "job_id": meta2.get("job_id"),
                "reinvest_pct": float(eff_pct),
                "risk_capacity": ctx.risk_capacity,
                "risk_mode": ctx.risk_mode,
                "risk_mode_override": ctx.risk_mode_override,
                "strat_mode": ctx.strat_mode,
                "kelly_suggestion": ctx.kelly_suggestion,
                "kelly_confidence": ctx.kelly_confidence,
                "daily_realized_pnl_pct": ctx.daily_pnl_pct,
                "daily_drawdown_pct": ctx.daily_dd_pct,
                "plan_status": ctx.plan_status,
                "plan_gap_pct": ctx.plan_gap_pct,
                "regime_label": ctx.regime_label,
                "regime_label_numeric": ctx.regime_label_numeric,
                "regime_stress_score": ctx.regime_stress_score,
                "regime_signal_strength": ctx.regime_signal_strength,
                "regime_entry_gate_factor": ctx.regime_entry_gate_factor,
                "ai_capacity_delta": ai_capacity_delta,
                "sandbox_mode": ctx.sandbox_mode,
                "health_score": health_score,
                "daily_trades_count": ctx.daily_trades_count,
                "daily_trades_cap": ctx.daily_trades_cap,
                "infra_health_score": ctx.infra_health_score,
                "infra_state_is_halted": infra_halted,
                "infra_state_is_degraded": infra_degraded,
                "infra_rpc_latency_ms": ctx.infra_rpc_latency_ms,
                "infra_jup_latency_ms": ctx.infra_jup_latency_ms,
                "infra_stability_score": ctx.infra_stability_score,
                "tail_risk_score": ctx.tail_risk_score,
                "spend_remaining_usd": ctx.spend_remaining_usd,
                "spend_budget_block": ctx.spend_budget_block,
                "scales": scales.as_dict(),
                "target_score": pick.score,
                "target_symbol": pick.symbol,
                "tx_sig": tx_sig,
                "swap_raw": raw_swap if isinstance(raw_swap, (dict, str)) else str(raw_swap)[:500],
            },
        }
        await _save_position(pick.mint, position_payload)

        with contextlib.suppress(Exception):
            fn = _exit_register_after_buy_fn()
            if callable(fn):
                await _call_maybe_async(fn, position_payload)
        with contextlib.suppress(Exception):
            fn2 = _autoexit_register_after_buy_fn()
            if callable(fn2):
                await _call_maybe_async(fn2, position_payload)
        with contextlib.suppress(Exception):
            fn3 = _sell_schedule_after_buy_fn()
            if callable(fn3):
                await _call_maybe_async(fn3, pick.mint, position_payload)

        with contextlib.suppress(Exception):
            usd_spent = await _sol_to_usd(reinvest_sol)
            if usd_spent > 0:
                await note_spend(redis, usd_spent, source="reinvestor")

        await _bump_daily_reinvest_counter(redis)
        await _set_last_reinvest_ts(redis)

        usd = await _sol_to_usd(reinvest_sol)
        await _safe_tg(
            "🔁 *Reinvestment executed*\n"
            f"• Token: `{pick.mint}`\n"
            f"• Size: {reinvest_sol:.4f} SOL (~${usd:.2f})\n"
            f"• Entry(px SOL): {buy_price_sol:.10f}\n"
            f"• SL%: {ctx.stop_loss_percent:.1f}\n"
            f"• Risk cap: {ctx.risk_capacity:.3f} (risk_mode={ctx.risk_mode}, strat_mode={ctx.strat_mode})\n"
            f"• Kelly: {ctx.kelly_suggestion:.3f} (conf={ctx.kelly_confidence:.2f})  WR={ctx.win_rate:.2%}\n"
            f"• DailyPnL={ctx.daily_pnl_pct:+.2f}%  DD={ctx.daily_dd_pct:.2f}%  Plan={ctx.plan_status} (gap={ctx.plan_gap_pct:+.2f}%)\n"
            f"• Regime: {ctx.regime_label} num={ctx.regime_label_numeric:+.2f} stress={ctx.regime_stress_score:.2f} gate={ctx.regime_entry_gate_factor:.2f}\n"
            f"• Tail risk: {ctx.tail_risk_score:.2f} | Infra health={ctx.infra_health_score:.2f} (halted={infra_halted}, degraded={infra_degraded})\n"
            f"• Health score: {health_score:.1f}\n"
            f"• Tx: `{tx_sig or 'n/a'}`",
            redis=redis,
        )

        with contextlib.suppress(Exception):
            log_to_notion = _core_attr("log_to_notion")
            try:
                await _call_maybe_async(
                    log_to_notion,
                    token=pick.mint,
                    symbol=(pick.symbol or pick.mint[:6]),
                    balance=reinvest_sol,
                    entry=buy_price_sol,
                    current=buy_price_sol,
                    profit=0.0,
                    reason=(
                        f"Reinvest buy eff_pct={eff_pct:.1f}% "
                        f"health={health_score:.1f} tail={ctx.tail_risk_score:.2f} infra={ctx.infra_health_score:.2f}"
                    ),
                )
            except TypeError:
                await _call_maybe_async(
                    log_to_notion,
                    pick.mint,
                    pick.symbol or pick.mint[:6],
                    reinvest_sol,
                    buy_price_sol,
                    buy_price_sol,
                    0.0,
                )

        await _log_event_safe(
            "reinvest_executed",
            {
                "mint": pick.mint,
                "symbol": pick.symbol,
                "proceeds_sol": proceeds_sol2,
                "reinvest_sol": reinvest_sol,
                "savings_sol": savings_sol,
                "eff_pct": eff_pct,
                "health_score": health_score,
                "tail_risk_score": ctx.tail_risk_score,
                "infra": ctx.infra_raw,
                "scales": scales.as_dict(),
                "risk_ctx": ctx.risk_ctx_raw,
                "perf": ctx.perf_raw,
                "tx_sig": tx_sig,
                "meta": meta2,
                "ts": now_iso,
                "latency_s": max(0.0, time.monotonic() - t0),
            },
        )

        await _update_reinvest_health(
            redis,
            ctx,
            eff_pct=eff_pct,
            reinvest_sol=reinvest_sol,
            savings_sol=savings_sol,
            reason="executed",
        )

        await note_reinvest_result(
            mint=pick.mint,
            proceeds_sol=proceeds_sol2,
            reinvest_sol=reinvest_sol,
            savings_sol=savings_sol,
            eff_pct=eff_pct,
            success=True,
            tx_sig=tx_sig,
            ctx={
                "risk_capacity": ctx.risk_capacity,
                "risk_mode": ctx.risk_mode,
                "daily_pnl_pct": ctx.daily_pnl_pct,
                "daily_dd_pct": ctx.daily_dd_pct,
                "equity_sol": ctx.equity_sol,
                "sandbox_mode": ctx.sandbox_mode,
                "health_score": health_score,
                "infra_health_score": ctx.infra_health_score,
                "tail_risk_score": ctx.tail_risk_score,
                "spend_remaining_usd": ctx.spend_remaining_usd,
                "spend_budget_block": ctx.spend_budget_block,
            },
            redis=redis,
        )

        log.info(
            "[REINVEST] ✅ Completed into %s (tx=%s) latency=%.2fs",
            pick.mint,
            str(tx_sig)[:120],
            max(0.0, time.monotonic() - t0),
        )
        return pick.mint

    finally:
        if redis and lock_token:
            await _release_lock(redis, lk, lock_token)


# ──────────────────────────────────────────────────────────────────────────────
# Queue + worker (+ DLQ)
# ──────────────────────────────────────────────────────────────────────────────
async def queue_reinvest(
    proceeds_sol: float, meta: dict[str, Any] | None = None, redis: RedisClient | None = None
) -> None:
    proceeds_sol2 = float(proceeds_sol or 0.0)
    if proceeds_sol2 <= 0:
        return

    meta2: Meta = dict(meta or {})
    _ensure_job_id(meta2, proceeds_sol2)

    if redis is None:
        redis = await _get_redis()

    if not redis:
        await reinvest(proceeds_sol2, meta=meta2, redis=None)
        return

    job: dict[str, Any] = {
        "proceeds_sol": proceeds_sol2,
        "meta": meta2,
        "enqueued_ts": _utc_ts(),
        "enqueued_iso": _now_utc().isoformat(),
    }
    payload = _json_dumps(job, separators=(",", ":"))

    try:
        await _redis_call(redis, "rpush", REINVEST_QUEUE, payload)
        if CFG.queue_maxlen > 0:
            with contextlib.suppress(Exception):
                await _redis_call(redis, "ltrim", REINVEST_QUEUE, -int(CFG.queue_maxlen), -1)
    except Exception as e:
        log.warning(
            "[REINVEST] Queue push failed; falling back to direct reinvest. err=%s", str(e)[:200]
        )
        await reinvest(proceeds_sol2, meta=meta2, redis=redis)

    log.info("[REINVEST] Queued job proceeds=%.6f job_id=%s", proceeds_sol2, meta2.get("job_id"))


async def _push_dlq(redis: RedisClient | None, payload: str, err: str) -> None:
    if not redis:
        return
    doc = {"payload": payload, "err": err[:300], "ts": _now_utc().isoformat()}
    try:
        await _redis_call(redis, "rpush", REINVEST_DLQ, _json_dumps(doc, separators=(",", ":")))
        await _redis_call(redis, "ltrim", REINVEST_DLQ, -500, -1)
    except Exception:
        return


async def run_reinvest_worker(
    redis: RedisClient | None = None,
    poll_interval: float = 2.0,
    stop_evt: asyncio.Event | None = None,
) -> None:
    if redis is None:
        redis = await _get_redis()
    if not redis:
        log.warning("[REINVEST] Worker disabled: no Redis client.")
        return

    if CFG.phase <= 2:
        poll_interval = max(poll_interval, 2.0)
    elif CFG.phase <= 5:
        poll_interval = max(poll_interval, 1.5)
    else:
        poll_interval = max(poll_interval, 1.0)

    log.info(
        "[REINVEST] Worker started (poll=%.2fs batch=%d phase=%d)",
        poll_interval,
        CFG.worker_batch,
        CFG.phase,
    )
    backoff = float(CFG.worker_backoff_s)

    while True:
        if stop_evt is not None and stop_evt.is_set():
            log.info("[REINVEST] Worker stopping on stop_evt.")
            break

        processed = 0
        try:
            while processed < max(1, int(CFG.worker_batch)):
                raw = await _redis_call(
                    redis, "blpop", REINVEST_QUEUE, timeout=int(max(1, poll_interval))
                )
                if not raw:
                    break

                payload_any: Any
                if isinstance(raw, (list, tuple)) and len(raw) >= 2:
                    payload_any = raw[1]
                else:
                    payload_any = raw

                payload_decoded = _maybe_decode(payload_any)
                payload = (
                    payload_decoded
                    if isinstance(payload_decoded, str)
                    else str(payload_decoded or "")
                )

                try:
                    job_any = json.loads(payload)
                except Exception:
                    log.warning("[REINVEST] Invalid job payload: %s", str(payload)[:240])
                    await _push_dlq(redis, str(payload)[:2000], "invalid_json")
                    continue

                if not isinstance(job_any, dict):
                    await _push_dlq(redis, payload[:2000], "invalid_job_shape")
                    continue

                if CFG.queue_max_age_s > 0:
                    enq = _safe_float(job_any.get("enqueued_ts"), 0.0)
                    if enq and (_utc_ts() - enq) > float(CFG.queue_max_age_s):
                        await _push_dlq(redis, payload[:4000], "stale_job_drop")
                        continue

                amt = float(job_any.get("proceeds_sol", 0.0) or 0.0)
                meta_obj = job_any.get("meta") or {}
                meta2: Meta = meta_obj if isinstance(meta_obj, dict) else {"meta_raw": meta_obj}

                if amt > 0:
                    await reinvest(amt, meta=meta2, redis=redis)

                processed += 1

            if processed == 0:
                await asyncio.sleep(float(poll_interval))

        except asyncio.CancelledError:
            raise
        except Exception as e:
            log.error("[REINVEST] Worker error: %s", e, exc_info=True)
            await asyncio.sleep(backoff)


# ──────────────────────────────────────────────────────────────────────────────
# Preview / explain / dry-run
# ──────────────────────────────────────────────────────────────────────────────
async def preview_reinvest_split(
    proceeds_sol: float, meta: dict[str, Any] | None = None, redis: RedisClient | None = None
) -> dict[str, Any] | None:
    meta2: Meta = dict(meta or {})
    meta2.setdefault("sandbox", True)
    meta2["dry_run"] = True
    _ensure_job_id(meta2, float(proceeds_sol or 0.0))

    if redis is None:
        redis = await _get_redis()

    proceeds_sol2 = float(proceeds_sol or 0.0)
    ctx = await _build_context(proceeds_sol2, meta2, redis)
    if ctx is None:
        return {"outcome": "skip", "reason": "context_build_failed", "proceeds_sol": proceeds_sol2}

    allowed, reason = await _evaluate_gates(ctx, redis)
    health_score = _compute_health_score(ctx)
    eff_pct, scales = _compute_eff_pct(ctx)

    reinvest_sol = proceeds_sol2 * (eff_pct / 100.0)
    savings_sol = max(0.0, proceeds_sol2 - reinvest_sol)

    if not allowed:
        return {
            "outcome": "savings_only",
            "reason": reason,
            "proceeds_sol": proceeds_sol2,
            "proceeds_usd": ctx.proceeds_usd,
            "eff_pct_final": 0.0,
            "reinvest_sol": 0.0,
            "savings_sol": proceeds_sol2,
            "health_score": health_score,
            "infra_health_score": ctx.infra_health_score,
            "scales": scales.as_dict(),
            "ctx": {
                "risk_capacity": ctx.risk_capacity,
                "risk_mode": ctx.risk_mode,
                "daily_pnl_pct": ctx.daily_pnl_pct,
                "daily_dd_pct": ctx.daily_dd_pct,
                "equity_sol": ctx.equity_sol,
                "plan_status": ctx.plan_status,
                "plan_gap_pct": ctx.plan_gap_pct,
                "tail_risk_score": ctx.tail_risk_score,
                "spend_remaining_usd": ctx.spend_remaining_usd,
                "spend_budget_block": ctx.spend_budget_block,
                "infra_state_is_halted": ctx.infra_state_is_halted,
                "infra_state_is_degraded": ctx.infra_state_is_degraded,
            },
        }

    outcome = "reinvest" if reinvest_sol >= CFG.min_proceeds_sol else "savings_only"
    reason2 = None if outcome == "reinvest" else "reinvest_leg_too_small"

    return {
        "outcome": outcome,
        "reason": reason2,
        "proceeds_sol": proceeds_sol2,
        "proceeds_usd": ctx.proceeds_usd,
        "eff_pct_final": eff_pct,
        "reinvest_sol": reinvest_sol,
        "savings_sol": savings_sol,
        "health_score": health_score,
        "infra_health_score": ctx.infra_health_score,
        "scales": scales.as_dict(),
        "ctx": {
            "risk_capacity": ctx.risk_capacity,
            "risk_mode": ctx.risk_mode,
            "daily_pnl_pct": ctx.daily_pnl_pct,
            "daily_dd_pct": ctx.daily_dd_pct,
            "equity_sol": ctx.equity_sol,
            "plan_status": ctx.plan_status,
            "plan_gap_pct": ctx.plan_gap_pct,
            "tail_risk_score": ctx.tail_risk_score,
            "spend_remaining_usd": ctx.spend_remaining_usd,
            "spend_budget_block": ctx.spend_budget_block,
            "infra_state_is_halted": ctx.infra_state_is_halted,
            "infra_state_is_degraded": ctx.infra_state_is_degraded,
        },
    }


async def explain_reinvest_decision(
    proceeds_sol: float, meta: dict[str, Any] | None = None, redis: RedisClient | None = None
) -> dict[str, Any] | None:
    preview = await preview_reinvest_split(proceeds_sol, meta=meta, redis=redis)
    if not preview:
        return None

    scales = preview.get("scales") or {}
    ctx = preview.get("ctx") or {}

    deltas: dict[str, float] = {}
    for k in (
        "cap_scale",
        "dd_scale",
        "kelly_scale",
        "plan_scale",
        "regime_scale",
        "ai_scale",
        "perf_scale",
        "profile_scale",
        "mode_scale",
        "infra_scale",
        "tail_scale",
    ):
        try:
            deltas[k] = float(scales.get(k, 1.0)) - 1.0
        except Exception:
            deltas[k] = 0.0

    boost = sorted(
        [(k, v) for k, v in deltas.items() if v > 0], key=lambda kv: kv[1], reverse=True
    )[:2]
    suppress = sorted([(k, v) for k, v in deltas.items() if v < 0], key=lambda kv: kv[1])[:2]

    def _fmt(k: str) -> str:
        return k.replace("_scale", "")

    preview["explanation"] = (
        f"Outcome={preview.get('outcome')} reason={preview.get('reason') or 'n/a'} | "
        f"eff={float(preview.get('eff_pct_final', 0.0)):.1f}% | "
        f"health={float(preview.get('health_score', 0.0)):.1f} "
        f"infra={float(preview.get('infra_health_score', 0.0)):.2f} "
        f"tail={ctx.get('tail_risk_score', 'n/a')} | "
        f"boosts={', '.join(f'{_fmt(k)}({v:+.2f})' for k, v in boost) or 'none'} | "
        f"suppressors={', '.join(f'{_fmt(k)}({v:+.2f})' for k, v in suppress) or 'none'}"
    )
    preview["boosts"] = boost
    preview["suppressors"] = suppress
    return preview


async def dry_run_reinvest(
    proceeds_sol: float, meta: dict[str, Any] | None = None, redis: RedisClient | None = None
) -> dict[str, Any] | None:
    meta2: Meta = dict(meta or {})
    meta2.setdefault("sandbox", True)
    meta2["dry_run"] = True
    prev = await preview_reinvest_split(proceeds_sol, meta=meta2, redis=redis)
    if not prev or prev.get("outcome") != "reinvest":
        return prev

    reinvest_sol = float(prev.get("reinvest_sol", 0.0) or 0.0)
    if reinvest_sol <= 0:
        return prev

    if redis is None:
        redis = await _get_redis()

    _ensure_job_id(meta2, float(proceeds_sol or 0.0))
    pick = await _pick_target_mint(reinvest_sol, meta=dict(meta2), redis=redis)
    prev["target"] = (
        {"mint": pick.mint, "symbol": pick.symbol, "score": pick.score, "raw": pick.raw}
        if pick
        else None
    )
    return prev


# ──────────────────────────────────────────────────────────────────────────────
# Wiring helper: consume sell-filled events
# ──────────────────────────────────────────────────────────────────────────────
async def handle_sell_filled_event(ev: Any, redis: RedisClient | None = None) -> None:
    if not isinstance(ev, dict):
        return

    status_raw = ev.get("status")
    status = str(status_raw or "").strip().lower()
    if status and status not in ("filled", "fill", "success"):
        return

    meta0 = ev.get("meta")
    meta: Meta = dict(meta0) if isinstance(meta0, dict) else {"meta_raw": meta0}

    profit_only = bool(meta.get("reinvest_profit_only", False))

    realized_profit = (
        _safe_float(ev.get("realized_profit_sol"), 0.0)
        or _safe_float(ev.get("profit_sol"), 0.0)
        or _safe_float(ev.get("pnl_sol"), 0.0)
    )
    proceeds = (
        _safe_float(ev.get("proceeds_sol"), 0.0)
        or _safe_float(ev.get("received_sol"), 0.0)
        or _safe_float(ev.get("out_sol"), 0.0)
    )

    proceeds_sol = (
        realized_profit if profit_only else (proceeds if proceeds > 0 else realized_profit)
    )
    if proceeds_sol <= 0:
        return

    meta.setdefault("origin_trade_id", ev.get("trade_id") or ev.get("req_id") or ev.get("tx_sig"))
    meta.setdefault("origin_position_id", ev.get("position_id") or ev.get("pos_id"))
    meta.setdefault("tx_sig", ev.get("tx_sig") or ev.get("signature"))
    meta.setdefault("tag", "exit_engine")

    jid = meta.get("tx_sig") or meta.get("origin_trade_id") or str(int(time.time()))
    if not isinstance(jid, str):
        jid = str(jid)
    meta.setdefault("job_id", f"sellfill-{jid}")

    await queue_reinvest(float(proceeds_sol), meta=meta, redis=redis)


__all__: list[str] = [
    "dry_run_reinvest",
    "explain_reinvest_decision",
    "get_last_reinvest_decision",
    "get_recent_reinvest_decisions",
    "get_reinvest_health",
    "handle_sell_filled_event",
    "preview_reinvest_split",
    "queue_reinvest",
    "reinvest",
    "reload_reinvestor_config",
    "run_reinvest_worker",
]


if __name__ == "__main__":  # local sanity demo

    async def _demo() -> None:
        r = await _get_redis()
        await queue_reinvest(0.05, {"job_id": f"demo-{int(time.time())}", "dry_run": True}, redis=r)
        exp = await explain_reinvest_decision(
            0.05, {"job_id": "demo-explain", "sandbox": True}, redis=r
        )
        print(_json_dumps(exp, indent=2))
        stop = asyncio.Event()

        async def stop_later() -> None:
            await asyncio.sleep(5)
            stop.set()

        asyncio.create_task(stop_later())
        await run_reinvest_worker(r, poll_interval=1.0, stop_evt=stop)

    asyncio.run(_demo())
