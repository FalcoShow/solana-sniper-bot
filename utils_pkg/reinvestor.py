# utils_pkg/reinvestor.py
# -*- coding: utf-8 -*-
from __future__ import annotations

"""
Reinvestor — V3+++ / God-Tier LOCK-IN (Master Roadmap Phase 0–7)
================================================================

Goal
----
Turn realized proceeds into a compounding flywheel, safely + observably:

  1) Build context   (risk/regime/perf/infra/tail/budget/plan)
  2) Gate            (hard stops + health thresholds + idempotency + locks)
  3) Split           (reinvest vs savings)
  4) Select target   (smart_wallets preferred; legacy fallback)
  5) Execute buy     (Jupiter via core_utils.swap_token_jupiter; drift tolerant)
  6) Persist + hook  (positions + optional ai_sell_logic/exit_engine hooks)
  7) Queue + worker  (Redis BLPOP; DLQ; stale-job drop; TG rate-limit)

Public API
----------
    async def reinvest(proceeds_sol: float, meta: dict|None = None, redis=None) -> str|None
    async def queue_reinvest(proceeds_sol: float, meta: dict|None = None, redis=None) -> None
    async def run_reinvest_worker(redis=None, poll_interval: float = 2.0, stop_evt: asyncio.Event|None = None) -> None
    async def preview_reinvest_split(proceeds_sol: float, meta: dict|None = None, redis=None) -> dict|None
    async def explain_reinvest_decision(proceeds_sol: float, meta: dict|None = None, redis=None) -> dict|None
    async def handle_sell_filled_event(ev: dict, redis=None) -> None
    async def dry_run_reinvest(proceeds_sol: float, meta: dict|None = None, redis=None) -> dict|None

    # extra helpers (Step 1+2 quality-of-life):
    async def get_reinvest_health(redis=None) -> dict|None
    async def get_last_reinvest_decision(redis=None) -> dict|None
    async def get_recent_reinvest_decisions(limit: int = 50, redis=None) -> list[dict]

Meta conventions (optional)
---------------------------
    force_token_mint / target_mint      -> force reinvest into this mint (still allow/deny checked)
    dry_run                             -> simulate only; no swap; still decision + health snapshot
    sandbox / sandbox_mode              -> implies dry_run
    force_savings_only                  -> do not reinvest (route to savings)
    force_reinvest_even_if_risk_blocked -> bypass most gates (NOT drawdown hard stop)
    reinvest_profit_only                -> if consuming sell events, use realized profit only
    ignore_*_gate flags                 -> selectively bypass gates (time/infra/health/balance)

Notes
-----
- Engineering system. No profit guarantees.
- Designed to tolerate signature drift across your utils_pkg modules.
"""

import asyncio
import contextlib
import hashlib
import json
import logging
import os
import secrets
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple, List

# ──────────────────────────────────────────────────────────────────────────────
# Logger
# ──────────────────────────────────────────────────────────────────────────────
def _get_logger() -> logging.Logger:
    with contextlib.suppress(Exception):
        from logging_setup import get_logger  # type: ignore
        return get_logger("sniper.reinvestor")
    lg = logging.getLogger("sniper.reinvestor")
    if not lg.handlers:
        logging.basicConfig(level=logging.INFO)
    return lg


log = _get_logger()


def _log_every(key: str, n: int = 30) -> bool:
    """Simple in-process rate limiter for repetitive logs."""
    try:
        store = getattr(_log_every, "_store")  # type: ignore[attr-defined]
    except Exception:
        store = {}
        setattr(_log_every, "_store", store)  # type: ignore[attr-defined]
    c = int(store.get(key, 0)) + 1
    store[key] = c
    return c % max(1, int(n)) == 0


# ──────────────────────────────────────────────────────────────────────────────
# Small utils
# ──────────────────────────────────────────────────────────────────────────────
SOL_MINT = "So11111111111111111111111111111111111111112"


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
    return datetime.now(timezone.utc)


def _utc_ts() -> float:
    return _now_utc().timestamp()


def _today_ymd() -> str:
    return _now_utc().strftime("%Y%m%d")


def _today_key(prefix: str) -> str:
    return f"{prefix}:{_today_ymd()}"


def _is_weekend(dt: Optional[datetime] = None) -> bool:
    dt = dt or _now_utc()
    return dt.weekday() >= 5


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


async def _maybe_await(x: Any) -> Any:
    return await x if asyncio.iscoroutine(x) else x


async def _call_any(fn: Any, *args: Any, **kwargs: Any) -> Any:
    """
    Best-effort call adapter:
      - tries fn(*args, **kwargs)
      - if TypeError, retries without kwargs
    """
    if not callable(fn):
        raise TypeError("fn not callable")
    try:
        return await _maybe_await(fn(*args, **kwargs))
    except TypeError:
        return await _maybe_await(fn(*args))


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


# Optional mint normalization (single source if available)
try:
    from utils_pkg.mints import normalize_mint  # type: ignore
except Exception:  # pragma: no cover
    def normalize_mint(x: str) -> str:
        return (x or "").strip()


# ──────────────────────────────────────────────────────────────────────────────
# Optional redis.asyncio import
# ──────────────────────────────────────────────────────────────────────────────
try:
    import redis.asyncio as aioredis  # type: ignore[import-not-found]
except Exception:  # pragma: no cover
    aioredis = None  # type: ignore[assignment]


async def _redis_call(redis: Any, fn_name: str, *args: Any, **kwargs: Any) -> Any:
    fn = getattr(redis, fn_name, None)
    if not callable(fn):
        raise AttributeError(fn_name)
    try:
        out = fn(*args, **kwargs)
    except TypeError:
        out = fn(*args)
    return await _maybe_await(out)


# ──────────────────────────────────────────────────────────────────────────────
# Core helpers (single source of truth) — tolerate sync/async drift
# ──────────────────────────────────────────────────────────────────────────────
from utils_pkg.core_utils import (  # type: ignore
    convert_sol_to_usd_amount,
    get_token_price,
    log_to_notion,
    redis as shared_redis,
    send_telegram,
    swap_token_jupiter,
    transfer_to_savings,
)

try:
    from utils_pkg.core_utils import get_wallet_balance_sol  # type: ignore
except Exception:  # pragma: no cover
    get_wallet_balance_sol = None  # type: ignore[assignment]

try:
    from utils_pkg.data_logger import log_event  # type: ignore[attr-defined]
except Exception:  # pragma: no cover
    async def log_event(*_: Any, **__: Any) -> None:
        return None


# Position persistence — prefer shared positions.save_position, fallback to local file
try:
    from positions import save_position  # type: ignore
except Exception:  # pragma: no cover
    def save_position(mint: str, data: Dict[str, Any]) -> None:
        os.makedirs("data", exist_ok=True)
        path = os.path.join("data", "positions.json")
        try:
            with open(path, "r", encoding="utf-8") as f:
                cur = json.load(f)
        except Exception:
            cur = {}
        cur[mint] = data
        with open(path, "w", encoding="utf-8") as f:
            json.dump(cur, f, indent=2)


# ──────────────────────────────────────────────────────────────────────────────
# Optional modules (soft imports)
# ──────────────────────────────────────────────────────────────────────────────
try:
    from utils_pkg.infra_guard import (  # type: ignore
        infra_features,
        should_block_entries as infra_should_block_entries,
    )
except Exception:  # pragma: no cover
    async def infra_features(redis=None) -> Dict[str, float]:
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

    async def infra_should_block_entries(redis=None) -> bool:
        return False


try:
    from utils_pkg.self_learning_ai_strategy import (  # type: ignore
        get_perf_snapshot,
        get_strategy_params,
        note_reinvest_decision,
        note_reinvest_result,
        note_reinvest_skipped,
    )
except Exception:  # pragma: no cover
    async def get_strategy_params(_redis=None) -> Dict[str, Any]:
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

    async def get_perf_snapshot(_redis=None) -> Dict[str, Any]:
        return {}

    async def note_reinvest_decision(*_: Any, **__: Any) -> None:
        return None

    async def note_reinvest_result(*_: Any, **__: Any) -> None:
        return None

    async def note_reinvest_skipped(*_: Any, **__: Any) -> None:
        return None


try:
    from utils_pkg.risk_manager import (  # type: ignore
        note_spend,
        risk_capacity_snapshot,
        should_allow_new_entry,
    )
except Exception:  # pragma: no cover
    async def risk_capacity_snapshot(_redis=None, **_: Any) -> Dict[str, Any]:
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

    async def should_allow_new_entry(_redis=None, **_: Any) -> bool:
        return True

    async def note_spend(_redis, usd: float, **_: Any) -> None:
        return None


try:
    from utils_pkg.smart_wallets import pick_reinvestment_target  # type: ignore
except Exception:  # pragma: no cover
    pick_reinvestment_target = None  # type: ignore[assignment]


try:
    from find_reinvestment_token import find_reinvestment_token  # type: ignore
except Exception:  # pragma: no cover
    async def find_reinvestment_token(min_volume: float, honeypot_filter: bool, **_: Any) -> Optional[str]:
        return None


# Optional: post-buy hooks (avoid circular imports)
try:
    from utils_pkg.ai_sell_logic import schedule_after_buy as _sell_schedule_after_buy  # type: ignore
except Exception:  # pragma: no cover
    _sell_schedule_after_buy = None  # type: ignore[assignment]

try:
    from utils_pkg.exit_engine import register_position_after_buy as _exit_register_after_buy  # type: ignore
except Exception:  # pragma: no cover
    _exit_register_after_buy = None  # type: ignore[assignment]

# Optional: new auto-exit worker hook (soft import; safe if absent)
try:
    from utils_pkg.auto_exit_worker.exit_engine import register_position_after_buy as _autoexit_register_after_buy  # type: ignore
except Exception:  # pragma: no cover
    _autoexit_register_after_buy = None  # type: ignore[assignment]


# ──────────────────────────────────────────────────────────────────────────────
# Redis keys
# ──────────────────────────────────────────────────────────────────────────────
REINVEST_QUEUE = _envs("REINVEST_QUEUE_KEY", "reinvest:queue")
REINVEST_DLQ = _envs("REINVEST_DLQ_KEY", "reinvest:dlq")

REINVEST_HEALTH_KEY = _envs("REINVEST_HEALTH_KEY", "reinvest:health")
REINVEST_DECISION_KEY = _envs("REINVEST_DECISION_KEY", "reinvest:last_decision")
REINVEST_DECISION_LIST_KEY = _envs("REINVEST_DECISION_LIST_KEY", "reinvest:decisions")

REINVEST_DAILY_COUNT_KEY = _envs("REINVEST_DAILY_COUNT_KEY", "reinvest:daily")
REINVEST_LAST_TS_KEY = _envs("REINVEST_LAST_TS_KEY", "reinvest:last_ts")
REINVEST_FAIL_TS_KEY = _envs("REINVEST_FAIL_TS_KEY", "reinvest:last_fail_ts")
REINVEST_TG_LAST_TS_KEY = _envs("REINVEST_TG_LAST_TS_KEY", "reinvest:tg:last_ts")

REINVEST_SEEN_PREFIX = _envs("REINVEST_SEEN_PREFIX", "reinvest:seen")
REINVEST_LOCK_PREFIX = _envs("REINVEST_LOCK_PREFIX", "reinvest:lock")


# ──────────────────────────────────────────────────────────────────────────────
# Config
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

    allow_mints: set[str]
    deny_mints: set[str]

    savings_wallet: Optional[str]
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
    max_budget_fraction: float  # fraction of daily remaining spend budget

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
    lock_scope: str  # "job" or "global"

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

        return ReinvestCfg(
            enabled=_envb("REINVESTOR_ENABLED", True),
            global_dry_run=_envb("REINVEST_GLOBAL_DRY_RUN", False),

            min_proceeds_sol=max(0.0, _envf("REINVESTMENT_MIN_PROCEEDS_SOL", _envf("REINVESTMENT_MIN_PROFIT_SOL", 0.01))),
            min_proceeds_usd=max(0.0, _envf("REINVESTMENT_MIN_PROCEEDS_USD", _envf("REINVESTMENT_MIN_PROFIT_USD", 0.0))),

            vol_min_usd=max(0.0, _envf("REINVESTMENT_VOLUME_MIN_USD", _envf("REINVESTMENT_VOLUME_MIN", 5000.0))),
            honeypot_check=_envb("REINVESTMENT_HONEYPOT_CHECK", True),

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

            health_min_recent_winrate=max(0.0, min(1.0, _envf("REINVEST_HEALTH_MIN_RECENT_WINRATE", 0.38))),
            health_min_sharpe=_envf("REINVEST_HEALTH_MIN_SHARPE", -0.2),
            health_min_score=max(0.0, _envf("REINVEST_HEALTH_MIN_SCORE", 0.0)),

            infra_min_health=max(0.0, _envf("REINVEST_INFRA_MIN_HEALTH", 0.0)),
            block_if_infra_halted=_envb("REINVEST_BLOCK_IF_INFRA_HALTED", True),
            block_if_infra_degraded=_envb("REINVEST_BLOCK_IF_INFRA_DEGRADED", False),

            tail_soft=max(0.0, min(1.0, _envf("REINVEST_TAIL_RISK_SOFT", 0.60))),
            tail_hard=max(0.0, min(1.0, _envf("REINVEST_TAIL_RISK_HARD", 0.85))),
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
# Redis client + counters
# ──────────────────────────────────────────────────────────────────────────────
async def _get_redis(url: Optional[str] = None):
    if shared_redis is not None:
        return shared_redis
    if aioredis is None:
        return None
    ru = url or _envs("REDIS_URL", "redis://localhost:6379")
    try:
        return aioredis.from_url(ru, decode_responses=True)
    except Exception:
        return None


async def _get_key_float(redis, key: str) -> Optional[float]:
    if not redis:
        return None
    try:
        raw = _maybe_decode(await _redis_call(redis, "get", key))
        return float(raw) if raw else None
    except Exception:
        return None


async def _set_key_float(redis, key: str, val: float) -> None:
    if not redis:
        return
    try:
        await _redis_call(redis, "set", key, str(float(val)))
    except Exception:
        return


async def _get_last_reinvest_ts(redis) -> Optional[float]:
    return await _get_key_float(redis, REINVEST_LAST_TS_KEY)


async def _set_last_reinvest_ts(redis, ts: Optional[float] = None) -> None:
    await _set_key_float(redis, REINVEST_LAST_TS_KEY, float(ts if ts is not None else _utc_ts()))


async def _set_last_fail_ts(redis, ts: Optional[float] = None) -> None:
    await _set_key_float(redis, REINVEST_FAIL_TS_KEY, float(ts if ts is not None else _utc_ts()))


async def _get_last_fail_ts(redis) -> Optional[float]:
    return await _get_key_float(redis, REINVEST_FAIL_TS_KEY)


async def _bump_daily_reinvest_counter(redis) -> None:
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


async def _get_daily_reinvest_count(redis) -> int:
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
def _id_key_from_meta(meta: Dict[str, Any]) -> Optional[str]:
    for k in ("job_id", "origin_trade_id", "origin_position_id", "cycle_id", "tx_sig", "txid"):
        v = meta.get(k)
        if isinstance(v, str) and v.strip():
            return v.strip()
    return None


def _ensure_job_id(meta: Dict[str, Any], proceeds_sol: float) -> None:
    if _id_key_from_meta(meta):
        return
    base = json.dumps({"p": float(proceeds_sol), "m": meta}, sort_keys=True, default=str).encode("utf-8")
    h = hashlib.sha256(base).hexdigest()[:16]
    meta["job_id"] = f"reinv-{int(_utc_ts())}-{h}"


async def _idempotency_claim(redis, meta: Dict[str, Any]) -> bool:
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


_RELEASE_LUA = "if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) else return 0 end"


def _lock_key(meta: Dict[str, Any]) -> str:
    if CFG.lock_scope == "global":
        return REINVEST_LOCK_PREFIX
    jid = _id_key_from_meta(meta) or "noid"
    return f"{REINVEST_LOCK_PREFIX}:{jid}"


async def _acquire_lock(redis, key: str, ttl_s: int) -> Optional[str]:
    if (not redis) or (not CFG.lock_enabled):
        return None
    token = secrets.token_hex(12)
    try:
        ok = await _redis_call(redis, "set", key, token, nx=True, ex=max(1, int(ttl_s)))
        return token if ok else None
    except Exception:
        return None


async def _release_lock(redis, key: str, token: Optional[str]) -> None:
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
        usd = await _maybe_await(convert_sol_to_usd_amount(float(amount_sol)))
        return float(usd or 0.0)
    except Exception:
        return 0.0


async def _token_price_in_sol(mint: str) -> float:
    mint = normalize_mint(mint)
    try:
        px_usd = await _maybe_await(get_token_price(mint))
        if px_usd is None or float(px_usd) <= 0:
            return 0.0
        sol_usd = await _maybe_await(convert_sol_to_usd_amount(1.0))
        if sol_usd is None or float(sol_usd) <= 0:
            return 0.0
        return float(px_usd) / float(sol_usd)
    except Exception:
        return 0.0


# ──────────────────────────────────────────────────────────────────────────────
# Telegram helpers (rate-limited via redis when available)
# ──────────────────────────────────────────────────────────────────────────────
async def _safe_tg(msg: str, redis=None) -> None:
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
        await _maybe_await(send_telegram(msg))


# ──────────────────────────────────────────────────────────────────────────────
# Savings transfer (signature drift tolerant)
# ──────────────────────────────────────────────────────────────────────────────
async def _send_to_savings(amount_sol: float, redis=None) -> None:
    if amount_sol <= 0:
        return
    if not CFG.savings_wallet:
        if _log_every("savings_wallet_missing", n=20):
            log.info("[REINVEST] SAVINGS_WALLET not set; keeping %.6f SOL in main wallet.", amount_sol)
        return

    try:
        try:
            await _maybe_await(transfer_to_savings(float(amount_sol)))
        except TypeError:
            await _maybe_await(transfer_to_savings(float(amount_sol), wallet=CFG.savings_wallet))
        usd = await _sol_to_usd(amount_sol)
        await _safe_tg(f"🏦 Moved {amount_sol:.4f} SOL (~${usd:.2f}) to savings wallet.", redis=redis)
        with contextlib.suppress(Exception):
            await log_event("reinvest_savings_transfer", {"amount_sol": amount_sol, "amount_usd": usd, "ts": _now_utc().isoformat()})
    except Exception as e:
        log.warning("[REINVEST] Savings transfer failed: %s", str(e)[:200])


# ──────────────────────────────────────────────────────────────────────────────
# Target selection
# ──────────────────────────────────────────────────────────────────────────────
@dataclass
class TargetPick:
    mint: str
    score: float = 0.0
    symbol: Optional[str] = None
    raw: Any = None


def _mint_allowed(mint: str) -> bool:
    m = normalize_mint(mint)
    if not m or len(m) <= 30:
        return False
    if m in CFG.deny_mints:
        return False
    if CFG.allow_mints and m not in CFG.allow_mints:
        return False
    return True


async def _pick_target_mint(reinvest_sol: float, meta: Dict[str, Any], redis=None) -> Optional[TargetPick]:
    forced = meta.get("force_token_mint") or meta.get("target_mint")
    if isinstance(forced, str) and _mint_allowed(forced):
        return TargetPick(mint=normalize_mint(forced), score=9e9, symbol=meta.get("target_symbol"), raw={"forced": True})

    # Prefer smart_wallets
    if pick_reinvestment_target is not None:
        out = None
        try:
            out = await pick_reinvestment_target(
                budget_sol=reinvest_sol,
                min_24h_volume_usd=CFG.vol_min_usd,
                redis=redis,
                meta=meta,
            )
        except TypeError:
            with contextlib.suppress(Exception):
                out = await pick_reinvestment_target(reinvest_sol)  # type: ignore[misc]
        except Exception:
            out = None

        if isinstance(out, str) and _mint_allowed(out):
            return TargetPick(mint=normalize_mint(out), score=1.0, raw=out)

        if isinstance(out, dict):
            mint = out.get("mint") or out.get("token") or out.get("address")
            if isinstance(mint, str) and _mint_allowed(mint):
                sym = out.get("symbol") or out.get("sym")
                sc = _safe_float(out.get("score"), 0.0)
                return TargetPick(mint=normalize_mint(mint), score=sc, symbol=str(sym) if sym else None, raw=out)

        if isinstance(out, (list, tuple)):
            best: Optional[TargetPick] = None
            for cand in out:
                if not isinstance(cand, dict):
                    continue
                mint = cand.get("mint") or cand.get("token") or cand.get("address")
                if not isinstance(mint, str) or not _mint_allowed(mint):
                    continue
                sc = _safe_float(cand.get("score"), 0.0)
                sym = cand.get("symbol") or cand.get("sym")
                tp = TargetPick(mint=normalize_mint(mint), score=sc, symbol=str(sym) if sym else None, raw=cand)
                if best is None or tp.score > best.score:
                    best = tp
            if best is not None:
                return best

    # Legacy fallback
    try:
        mint = await find_reinvestment_token(min_volume=float(CFG.vol_min_usd), honeypot_filter=bool(CFG.honeypot_check))
        if isinstance(mint, str) and _mint_allowed(mint):
            return TargetPick(mint=normalize_mint(mint), score=0.0, raw={"legacy": True})
    except Exception as e:
        log.warning("[REINVEST] legacy find_reinvestment_token failed: %s", str(e)[:200])

    return None


# ──────────────────────────────────────────────────────────────────────────────
# Context + scoring models
# ──────────────────────────────────────────────────────────────────────────────
@dataclass
class ReinvestContext:
    proceeds_sol: float
    proceeds_usd: float
    meta: Dict[str, Any]

    risk_capacity: float
    risk_mode: int
    risk_mode_override: Optional[int]
    halt_reason: Optional[str]

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
    risk_tags: List[str] = field(default_factory=list)

    infra_health_score: float = 1.0
    infra_rpc_latency_ms: float = 0.0
    infra_jup_latency_ms: float = 0.0
    infra_state_is_healthy: float = 1.0
    infra_state_is_degraded: float = 0.0
    infra_state_is_halted: float = 0.0
    infra_stability_score: float = 1.0
    infra_raw: Dict[str, Any] = field(default_factory=dict)

    risk_ctx_raw: Dict[str, Any] = field(default_factory=dict)
    perf_raw: Dict[str, Any] = field(default_factory=dict)


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

    def as_dict(self) -> Dict[str, float]:
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


async def _build_context(proceeds_sol: float, meta: Dict[str, Any], redis) -> Optional[ReinvestContext]:
    meta = dict(meta or {})
    proceeds_sol = float(proceeds_sol or 0.0)
    proceeds_usd = await _sol_to_usd(proceeds_sol)

    # risk snapshot (tolerate signature drift)
    try:
        risk_ctx = await risk_capacity_snapshot(redis, source="reinvestor")  # type: ignore[arg-type]
    except TypeError:
        risk_ctx = await risk_capacity_snapshot(redis)
    except Exception:
        risk_ctx = await risk_capacity_snapshot()

    risk_capacity = float(risk_ctx.get("risk_capacity", 1.0))
    risk_mode = int(risk_ctx.get("risk_mode", 1))
    rmo = risk_ctx.get("risk_mode_override")
    risk_mode_override = int(rmo) if isinstance(rmo, (int, float, str)) and str(rmo).strip() != "" else None

    halt_reason = risk_ctx.get("halt_reason")
    halt_reason = str(halt_reason) if halt_reason is not None else None

    daily_pnl_pct = float(risk_ctx.get("daily_realized_pnl_pct") or risk_ctx.get("pnl_day_pct") or 0.0)
    daily_dd_pct = float(risk_ctx.get("daily_drawdown_pct") or risk_ctx.get("daily_dd_pct") or risk_ctx.get("rolling_dd_pct") or 0.0)

    equity_usd = float(risk_ctx.get("equity_usd", 0.0))
    sandbox_mode = bool(risk_ctx.get("sandbox_mode", False))
    daily_trades_count = int(risk_ctx.get("daily_trades_count", 0) or 0)
    daily_trades_cap = int(risk_ctx.get("daily_trades_cap", 0) or 0)

    regime_label = str(risk_ctx.get("regime_label", "neutral") or "neutral")
    regime_label_numeric = float(risk_ctx.get("regime_label_numeric", 0.0))
    regime_stress_score = float(risk_ctx.get("regime_stress_score", 0.0))
    regime_signal_strength = float(risk_ctx.get("regime_signal_strength", 0.5))
    regime_entry_gate_factor = float(risk_ctx.get("regime_entry_gate_factor", 1.0))
    regime_risk_multiplier = float(risk_ctx.get("regime_risk_multiplier", 1.0))

    tail_risk_score = float(risk_ctx.get("tail_risk_score", 0.0) or 0.0)
    spend_remaining_usd = float(risk_ctx.get("daily_spend_remaining_usd", 1e12) or 1e12)
    spend_budget_block = bool(risk_ctx.get("spend_budget_block", False))
    equity_floor_usd = float(risk_ctx.get("equity_floor_usd", 0.0) or 0.0)
    equity_below_floor = bool(risk_ctx.get("equity_below_floor", False))

    rt = risk_ctx.get("risk_tags", [])
    if isinstance(rt, str):
        risk_tags = [rt]
    elif isinstance(rt, (list, tuple, set)):
        risk_tags = [str(x) for x in rt]
    else:
        risk_tags = []

    # equity_sol (derive from equity_usd / sol_usd)
    try:
        sol_usd = await _maybe_await(convert_sol_to_usd_amount(1.0))
        equity_sol = equity_usd / float(sol_usd) if sol_usd and float(sol_usd) > 0 else 0.0
    except Exception:
        equity_sol = 0.0

    params = await get_strategy_params(redis)
    perf = await get_perf_snapshot(redis)

    base_reinvest_pct = float(risk_ctx.get("reinvest_base_pct") or params.get("REINVEST_PERCENT", 75.0))
    stop_loss_percent = float(params.get("STOP_LOSS_PERCENT", 18.0))
    slippage = _normalize_slippage(float(params.get("SLIPPAGE", 0.005)))

    min_sol_balance = float(params.get("MIN_SOL_BALANCE", risk_ctx.get("min_sol_balance", 0.25)))
    max_risk_per_trade = float(params.get("MAX_RISK_PER_TRADE", risk_ctx.get("max_risk_per_trade", 0.05)))

    strat_mode = int(params.get("STRAT_MODE", risk_ctx.get("strat_mode", 1) or 1))
    hard_halt = bool(params.get("HARD_HALT", False))

    plan_target_apr_pct = float(params.get("PLAN_TARGET_APR_PCT", 180.0))
    plan_gap_pct = float(params.get("PLAN_GAP_PCT", risk_ctx.get("plan_gap_pct", 0.0)))
    plan_status = str(params.get("PLAN_STATUS", risk_ctx.get("plan_status", "n/a")) or "n/a")

    kelly_from_perf = float(perf.get("kelly_suggestion", 0.0))
    kelly_from_risk = float(risk_ctx.get("kelly_raw", 0.0))
    kelly_suggestion = kelly_from_perf if kelly_from_perf != 0.0 else kelly_from_risk
    if kelly_suggestion == 0.0:
        kelly_suggestion = 0.02

    kelly_pos_cap_pct = float(risk_ctx.get("kelly_pos_pct_cap", 0.0) or 0.0)
    kelly_confidence = float(perf.get("kelly_confidence", risk_ctx.get("kelly_confidence", 0.0)) or 0.0)

    win_rate = float(perf.get("win_rate", risk_ctx.get("winrate20", 0.0)) or 0.0)
    recent_win_rate = float(perf.get("recent_win_rate", win_rate) or win_rate)

    pnl_mean = float(perf.get("pnl_mean", 0.0) or 0.0)
    pnl_std = float(perf.get("pnl_std", 0.0) or 0.0)
    sharpe_like = float(perf.get("sharpe_like", 0.0) or 0.0)
    recent_sharpe_like = float(perf.get("recent_sharpe_like", sharpe_like) or sharpe_like)

    infra = await infra_features(redis)
    infra_health_score = float(infra.get("infra_health_score", 1.0))
    infra_rpc_latency_ms = float(infra.get("infra_rpc_latency_ms", 0.0))
    infra_jup_latency_ms = float(infra.get("infra_jup_latency_ms", 0.0))
    infra_state_is_healthy = float(infra.get("infra_state_is_healthy", 1.0))
    infra_state_is_degraded = float(infra.get("infra_state_is_degraded", 0.0))
    infra_state_is_halted = float(infra.get("infra_state_is_halted", 0.0))
    infra_stability_score = float(infra.get("infra_stability_score", 1.0))

    return ReinvestContext(
        proceeds_sol=proceeds_sol,
        proceeds_usd=proceeds_usd,
        meta=meta,

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
        infra_raw=dict(infra),

        risk_ctx_raw=dict(risk_ctx),
        perf_raw=dict(perf),
    )


# ──────────────────────────────────────────────────────────────────────────────
# Health scoring + persistence
# ──────────────────────────────────────────────────────────────────────────────
def _compute_health_score(ctx: ReinvestContext) -> float:
    with contextlib.suppress(Exception):
        pre = ctx.risk_ctx_raw.get("reinvest_health_score") or ctx.risk_ctx_raw.get("global_health_score")
        if isinstance(pre, (int, float)) and float(pre) >= 0:
            return float(pre)

    wr = max(0.0, min(1.0, ctx.win_rate))
    wr_score = max(0.0, min(1.0, (wr - 0.30) / (0.70 - 0.30))) * 100.0

    dd = max(0.0, ctx.daily_dd_pct)
    max_dd_for_score = max(CFG.dd_hard_stop_pct, CFG.dd_soft_stop_pct, 25.0)
    dd_norm = max(0.0, 1.0 - min(dd, max_dd_for_score) / max_dd_for_score) if max_dd_for_score > 0 else 1.0
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


async def _update_reinvest_health(redis, ctx: ReinvestContext, eff_pct: float, reinvest_sol: float, savings_sol: float, reason: str = "ok") -> None:
    if not redis:
        return
    try:
        health_doc: Dict[str, Any] = {
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
        await _redis_call(redis, "set", REINVEST_HEALTH_KEY, json.dumps(health_doc))
        with contextlib.suppress(Exception):
            await log_event("reinvest_health", health_doc)
    except Exception:
        return


async def get_reinvest_health(redis=None) -> Optional[Dict[str, Any]]:
    if redis is None:
        redis = await _get_redis()
    if not redis:
        return None
    try:
        raw = _maybe_decode(await _redis_call(redis, "get", REINVEST_HEALTH_KEY))
        return json.loads(raw) if raw else None
    except Exception:
        return None


async def get_last_reinvest_decision(redis=None) -> Optional[Dict[str, Any]]:
    if redis is None:
        redis = await _get_redis()
    if not redis:
        return None
    try:
        raw = _maybe_decode(await _redis_call(redis, "get", REINVEST_DECISION_KEY))
        return json.loads(raw) if raw else None
    except Exception:
        return None


async def get_recent_reinvest_decisions(limit: int = 50, redis=None) -> List[Dict[str, Any]]:
    """
    Introspection helper:
    Return the last N reinvest decisions from Redis (most recent first).
    Best-effort; empty list if Redis unavailable.
    """
    if redis is None:
        redis = await _get_redis()
    if not redis:
        return []
    limit = max(1, min(int(limit or 1), 1000))
    try:
        raw_list = await _redis_call(redis, "lrange", REINVEST_DECISION_LIST_KEY, 0, limit - 1)
        out: List[Dict[str, Any]] = []
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


async def _persist_decision(redis, decision_doc: Dict[str, Any]) -> None:
    if not redis:
        return
    try:
        payload = json.dumps(decision_doc, default=str)
        await _redis_call(redis, "set", REINVEST_DECISION_KEY, payload)
        await _redis_call(redis, "lpush", REINVEST_DECISION_LIST_KEY, payload)
        await _redis_call(redis, "ltrim", REINVEST_DECISION_LIST_KEY, 0, int(CFG.decisions_keep) - 1)
    except Exception:
        return


# ──────────────────────────────────────────────────────────────────────────────
# Scales / effective % (stable logic)
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
    status, gap, dd, vol, cap = ctx.plan_status, ctx.plan_gap_pct, ctx.daily_dd_pct, ctx.pnl_std, ctx.risk_capacity
    scale = 1.0
    if status == "behind" and gap < -3.0:
        if cap > 0.6 and dd < CFG.dd_soft_stop_pct:
            scale *= 1.05
    elif status == "ahead" and gap > 3.0:
        if dd >= CFG.dd_soft_stop_pct or vol > 20.0:
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
            scale *= (0.9 + 0.1 * gate)
        elif gate > 1.05:
            scale *= (1.0 + 0.05 * (gate - 1.0))
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


def _profile_scale(meta: Dict[str, Any]) -> float:
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
    raw = meta.get("mode") or meta.get("reinvest_mode") or ctx.risk_ctx_raw.get("reinvest_mode") or _envs("REINVEST_MODE", "balanced")
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


def _apply_meta_eff_overrides(eff_pct: float, meta: Dict[str, Any]) -> float:
    if "force_eff_pct" in meta:
        with contextlib.suppress(Exception):
            forced = float(meta["force_eff_pct"])
            return max(CFG.min_eff_pct, min(CFG.max_eff_pct, forced))

    try:
        min_eff_v = float(meta.get("min_eff_pct")) if meta.get("min_eff_pct") is not None else float(CFG.min_eff_pct)
    except Exception:
        min_eff_v = float(CFG.min_eff_pct)

    try:
        max_eff_v = float(meta.get("max_eff_pct")) if meta.get("max_eff_pct") is not None else float(CFG.max_eff_pct)
    except Exception:
        max_eff_v = float(CFG.max_eff_pct)

    eff_pct = max(min_eff_v, eff_pct)
    eff_pct = min(CFG.max_eff_pct, max_eff_v, eff_pct)
    return max(0.0, eff_pct)


def _compute_eff_pct(ctx: ReinvestContext) -> Tuple[float, ReinvestScales]:
    scales = _compute_scales(ctx)
    eff = float(scales.eff_pct)
    if CFG.always_savings_pct > 0:
        eff = max(0.0, eff - float(CFG.always_savings_pct))
    eff = _apply_meta_eff_overrides(eff, ctx.meta)
    return eff, scales


# ──────────────────────────────────────────────────────────────────────────────
# Gates + journaling
# ──────────────────────────────────────────────────────────────────────────────
async def _evaluate_gates(ctx: ReinvestContext, redis) -> Tuple[bool, str]:
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

    # daily cap
    if redis and CFG.max_reinvests_per_day > 0:
        if (await _get_daily_reinvest_count(redis)) >= CFG.max_reinvests_per_day:
            return False, "daily_cap_reached"

    # IMPORTANT: propagate sandbox -> dry_run (ctx.meta is authoritative later)
    if ctx.sandbox_mode and not meta.get("dry_run"):
        meta["dry_run"] = True

    if ctx.equity_below_floor and not meta.get("ignore_health_gate"):
        return False, "equity_below_floor_flag"
    if CFG.min_equity_sol > 0 and ctx.equity_sol > 0 and ctx.equity_sol < CFG.min_equity_sol:
        return False, "equity_below_min_env"

    # cooldown on fail
    if redis and CFG.cooldown_on_fail_min > 0 and not meta.get("ignore_time_gate"):
        last_fail = await _get_last_fail_ts(redis)
        if last_fail and (_utc_ts() - float(last_fail)) < float(CFG.cooldown_on_fail_min) * 60.0:
            return False, "cooldown_on_fail"

    # min spacing between reinvests
    if CFG.min_minutes_between > 0 and redis and not meta.get("ignore_time_gate"):
        last_ts = await _get_last_reinvest_ts(redis)
        if last_ts:
            elapsed_min = (_utc_ts() - float(last_ts)) / 60.0
            if elapsed_min < CFG.min_minutes_between:
                return False, "min_gap_between_reinvests"

    if ctx.hard_halt and not force_anyway:
        return False, "hard_halt"

    if not force_anyway:
        if any(tag in ("halt_all", "reinvestor_off", "reinvestor_hard_off", "halt_reinvestor") for tag in ctx.risk_tags):
            return False, "risk_tag_halt"
        if any(tag in ("reinvestor_soft_off", "cooldown_reinvestor") for tag in ctx.risk_tags):
            return False, "risk_tag_soft_halt"

    if ctx.daily_trades_cap > 0 and ctx.daily_trades_count >= ctx.daily_trades_cap and not force_anyway:
        return False, "daily_trades_cap_reached"

    if ctx.spend_budget_block and not force_anyway:
        return False, "spend_budget_block"
    if ctx.spend_remaining_usd is not None and ctx.spend_remaining_usd <= 0.0 and not force_anyway:
        return False, "spend_budget_exhausted"

    # risk manager entry gate
    try:
        allow_entry = await should_allow_new_entry(redis, source="reinvestor")  # type: ignore[arg-type]
    except TypeError:
        allow_entry = await should_allow_new_entry(redis)
    except Exception:
        allow_entry = True

    if ((not allow_entry) or ctx.risk_capacity < CFG.min_capacity or ctx.risk_mode == 0) and not force_anyway:
        return False, "risk_blocked"
    if ctx.risk_mode == 3 and ctx.risk_capacity < CFG.defensive_capacity and not force_anyway:
        return False, "defensive_capacity_low"

    # hard stop drawdown always wins (even if force_anyway)
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
    if CFG.health_min_score > 0.0 and not meta.get("ignore_health_gate") and health < CFG.health_min_score and not force_anyway:
        return False, "health_score_low"

    if CFG.tail_gate > 0.0 and not meta.get("ignore_health_gate") and ctx.tail_risk_score >= CFG.tail_gate and not force_anyway:
        return False, "tail_risk_high"

    if not meta.get("ignore_infra_gate") and not force_anyway:
        try:
            infra_block = await infra_should_block_entries(redis)
        except Exception:
            infra_block = False

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


async def _journal_skip(reason: str, ctx: Optional[ReinvestContext], meta: Dict[str, Any], redis=None) -> None:
    payload: Dict[str, Any] = {
        "reason": reason,
        "proceeds_sol": float(ctx.proceeds_sol) if ctx else _safe_float(meta.get("proceeds_sol"), 0.0),
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

    with contextlib.suppress(Exception):
        await note_reinvest_skipped(reason=reason, proceeds_sol=payload["proceeds_sol"], ctx=(payload.get("ctx") or {}), meta=meta, redis=redis)
    with contextlib.suppress(Exception):
        await log_event("reinvest_skipped", payload)


# ──────────────────────────────────────────────────────────────────────────────
# Swap adapter (signature/return drift tolerant)
# ──────────────────────────────────────────────────────────────────────────────
async def _do_swap_buy(sol_in: float, out_mint: str, slippage: float) -> Tuple[bool, Optional[str], Any]:
    """
    Normalizes multiple possible swap_token_jupiter signatures / return shapes.
    Returns: (ok, tx_sig, raw_out)
    """
    out = None
    err: Optional[str] = None

    out_mint = normalize_mint(out_mint)
    slip = _normalize_slippage(slippage)

    try:
        out = await _maybe_await(swap_token_jupiter(SOL_MINT, out_mint, amount_float=sol_in, buy=True, slippage=slip))
    except TypeError as e:
        err = str(e)
        with contextlib.suppress(Exception):
            out = await _maybe_await(swap_token_jupiter(SOL_MINT, out_mint, sol_in, True, slip))
        if out is None:
            with contextlib.suppress(Exception):
                out = await _maybe_await(swap_token_jupiter(SOL_MINT, out_mint, sol_in))
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
async def reinvest(proceeds_sol: float, meta: Optional[Dict[str, Any]] = None, redis=None) -> Optional[str]:
    meta = dict(meta or {})
    proceeds_sol = float(proceeds_sol or 0.0)

    # global dry-run switches
    if CFG.global_dry_run:
        meta["dry_run"] = True
    if meta.get("sandbox") or meta.get("sandbox_mode"):
        meta["dry_run"] = True

    _ensure_job_id(meta, proceeds_sol)
    meta.setdefault("proceeds_sol", proceeds_sol)

    if redis is None:
        redis = await _get_redis()

    t0 = time.monotonic()

    # idempotency
    if redis and not await _idempotency_claim(redis, meta):
        log.info("[REINVEST] Idempotency: already processed. job_id=%s", meta.get("job_id"))
        await _journal_skip("idempotency_already_processed", None, meta, redis=redis)
        return None

    # lock
    lock_token: Optional[str] = None
    lk = _lock_key(meta)
    if redis and CFG.lock_enabled:
        lock_token = await _acquire_lock(redis, lk, CFG.lock_ttl_s)
        if not lock_token:
            log.info("[REINVEST] Lock busy -> skip. key=%s", lk)
            await _journal_skip("lock_busy", None, meta, redis=redis)
            return None

    try:
        ctx = await _build_context(proceeds_sol, meta, redis)
        if ctx is None:
            await _journal_skip("context_build_failed", None, meta, redis=redis)
            await _send_to_savings(proceeds_sol, redis=redis)
            return None

        allowed, reason = await _evaluate_gates(ctx, redis)

        # IMPORTANT: ctx.meta is authoritative (it may have been mutated by gates)
        meta = ctx.meta

        eff_pct, scales = _compute_eff_pct(ctx)
        eff_pct = max(CFG.min_eff_pct, min(CFG.max_eff_pct, eff_pct))

        reinvest_sol = proceeds_sol * (eff_pct / 100.0)
        savings_sol = max(0.0, proceeds_sol - reinvest_sol)

        # budget fraction cap (optional): reinvest <= max_budget_fraction * spend_remaining_usd
        if CFG.max_budget_fraction > 0 and ctx.spend_remaining_usd and ctx.spend_remaining_usd > 0:
            cap_usd = float(CFG.max_budget_fraction) * float(ctx.spend_remaining_usd)
            if cap_usd > 0:
                reinv_usd = await _sol_to_usd(reinvest_sol)
                if reinv_usd > cap_usd and reinv_usd > 0:
                    scale_down = cap_usd / max(reinv_usd, 1e-9)
                    reinvest_sol = max(0.0, reinvest_sol * scale_down)
                    savings_sol = max(0.0, proceeds_sol - reinvest_sol)

        health_score = _compute_health_score(ctx)

        # persist last decision (even on skip)
        decision_doc = {
            "ts": _now_utc().isoformat(),
            "job_id": meta.get("job_id"),
            "allowed": bool(allowed),
            "reason": reason,
            "proceeds_sol": proceeds_sol,
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
            "meta": meta,
        }
        if redis:
            await _persist_decision(redis, decision_doc)

        if not allowed:
            log.info("[REINVEST] Gate blocked (%s) -> savings.", reason)
            await _journal_skip(reason, ctx, meta, redis=redis)
            await _send_to_savings(proceeds_sol, redis=redis)
            await _update_reinvest_health(redis, ctx, eff_pct=0.0, reinvest_sol=0.0, savings_sol=proceeds_sol, reason=reason)
            return None

        with contextlib.suppress(Exception):
            await note_reinvest_decision(
                proceeds_sol=proceeds_sol,
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
                meta=meta,
                redis=redis,
            )

        # cap by per-trade risk vs equity
        if ctx.equity_sol > 0 and ctx.max_risk_per_trade > 0:
            cap_sol = float(ctx.max_risk_per_trade) * float(ctx.equity_sol)
            if reinvest_sol > cap_sol:
                reinvest_sol = cap_sol
                savings_sol = max(0.0, proceeds_sol - reinvest_sol)

        # USD hard cap
        if CFG.max_single_usd > 0:
            usd_full = await _sol_to_usd(reinvest_sol)
            if usd_full > CFG.max_single_usd and usd_full > 0:
                scale_down = float(CFG.max_single_usd) / max(float(usd_full), 1e-9)
                reinvest_sol = max(0.0, reinvest_sol * scale_down)
                savings_sol = max(0.0, proceeds_sol - reinvest_sol)

        # too small reinvest leg -> savings
        if reinvest_sol < CFG.min_proceeds_sol:
            await _journal_skip("reinvest_leg_too_small", ctx, meta, redis=redis)
            await _send_to_savings(proceeds_sol, redis=redis)
            await _update_reinvest_health(redis, ctx, eff_pct=0.0, reinvest_sol=0.0, savings_sol=proceeds_sol, reason="too_small")
            return None

        # optional min SOL reserve gate
        if get_wallet_balance_sol is not None and not meta.get("ignore_balance_gate"):
            with contextlib.suppress(Exception):
                bal = float(await _maybe_await(get_wallet_balance_sol()))  # type: ignore[misc]
                if (bal - proceeds_sol) >= 0 and (bal - proceeds_sol) < float(ctx.min_sol_balance):
                    await _journal_skip("min_sol_reserve_gate", ctx, meta, redis=redis)
                    await _send_to_savings(proceeds_sol, redis=redis)
                    await _update_reinvest_health(redis, ctx, eff_pct=0.0, reinvest_sol=0.0, savings_sol=proceeds_sol, reason="min_reserve")
                    return None

        # dry run exits here
        if meta.get("dry_run"):
            await _update_reinvest_health(redis, ctx, eff_pct=eff_pct, reinvest_sol=reinvest_sol, savings_sol=savings_sol, reason="dry_run")
            return None

        # pick target
        pick = await _pick_target_mint(reinvest_sol, meta=meta, redis=redis)
        if not pick:
            await _safe_tg("⚠️ Reinvestment skipped — no suitable target met filters right now.", redis=redis)
            await _journal_skip("no_target", ctx, meta, redis=redis)
            await _update_reinvest_health(redis, ctx, eff_pct=0.0, reinvest_sol=0.0, savings_sol=proceeds_sol, reason="no_target")
            with contextlib.suppress(Exception):
                await note_reinvest_result(
                    mint=None,
                    proceeds_sol=proceeds_sol,
                    reinvest_sol=0.0,
                    savings_sol=proceeds_sol,
                    eff_pct=0.0,
                    success=False,
                    tx_sig=None,
                    ctx={"health_score": health_score, "tail_risk_score": ctx.tail_risk_score},
                    redis=redis,
                )
            return None

        # savings before swap if configured
        if savings_sol > 0 and (not CFG.savings_after_swap):
            await _send_to_savings(savings_sol, redis=redis)

        ok, tx_sig, raw_swap = await _do_swap_buy(reinvest_sol, pick.mint, ctx.slippage)
        if not ok:
            await _safe_tg(f"❌ Reinvestment buy failed for `{pick.mint}`.", redis=redis)
            await _journal_skip("swap_failed", ctx, meta, redis=redis)
            await _update_reinvest_health(redis, ctx, eff_pct=0.0, reinvest_sol=0.0, savings_sol=proceeds_sol, reason="swap_failed")
            if redis and CFG.cooldown_on_fail_min > 0:
                await _set_last_fail_ts(redis)
            await _send_to_savings(proceeds_sol, redis=redis)
            with contextlib.suppress(Exception):
                await note_reinvest_result(
                    mint=pick.mint,
                    proceeds_sol=proceeds_sol,
                    reinvest_sol=0.0,
                    savings_sol=proceeds_sol,
                    eff_pct=0.0,
                    success=False,
                    tx_sig=None,
                    ctx={"health_score": health_score, "tail_risk_score": ctx.tail_risk_score, "swap": raw_swap},
                    redis=redis,
                )
            return None

        # savings after swap (default)
        if savings_sol > 0 and CFG.savings_after_swap:
            await _send_to_savings(savings_sol, redis=redis)

        # persist position (+ optional hooks)
        buy_price_sol = await _token_price_in_sol(pick.mint)
        trailing_stop = (buy_price_sol * (1.0 - (ctx.stop_loss_percent / 100.0))) if buy_price_sol > 0 else 0.0
        now_iso = _now_utc().isoformat()

        infra_halted = bool(ctx.infra_state_is_halted > 0.5)
        infra_degraded = bool(ctx.infra_state_is_degraded > 0.5)
        ai_capacity_delta = float(ctx.risk_ctx_raw.get("ai_capacity_delta", 0.0) or 0.0)

        position_payload: Dict[str, Any] = {
            "token_address": pick.mint,
            "buy_price": float(buy_price_sol),
            "amount_invested_sol": float(reinvest_sol),
            "timestamp": now_iso,
            "trailing_stop": float(trailing_stop),
            "source": meta.get("tag") or "reinvestor",
            "origin_trade_id": meta.get("origin_trade_id"),
            "origin_position_id": meta.get("origin_position_id"),
            "meta": {
                **meta,
                "job_id": meta.get("job_id"),
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
        await _maybe_await(save_position(pick.mint, position_payload))

        # optional: tell exit engine / auto-exit worker / AI sell logic about the new position
        with contextlib.suppress(Exception):
            if callable(_exit_register_after_buy):
                await _maybe_await(_exit_register_after_buy(position_payload))  # type: ignore[misc]
        with contextlib.suppress(Exception):
            if callable(_autoexit_register_after_buy):
                await _maybe_await(_autoexit_register_after_buy(position_payload))  # type: ignore[misc]
        with contextlib.suppress(Exception):
            if callable(_sell_schedule_after_buy):
                await _maybe_await(_sell_schedule_after_buy(pick.mint, position_payload))  # type: ignore[misc]

        # note spend
        with contextlib.suppress(Exception):
            usd_spent = await _sol_to_usd(reinvest_sol)
            if usd_spent > 0:
                try:
                    await note_spend(redis, usd_spent, source="reinvestor")  # type: ignore[arg-type]
                except TypeError:
                    await note_spend(redis, usd_spent)

        # counters
        await _bump_daily_reinvest_counter(redis)
        await _set_last_reinvest_ts(redis)

        # alerts
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

        # best-effort notion
        with contextlib.suppress(Exception):
            await _maybe_await(
                log_to_notion(
                    token=pick.mint,
                    symbol=(pick.symbol or pick.mint[:6]),
                    balance=reinvest_sol,
                    entry=buy_price_sol,
                    current=buy_price_sol,
                    profit=0.0,
                    reason=f"Reinvest buy eff_pct={eff_pct:.1f}% health={health_score:.1f} tail={ctx.tail_risk_score:.2f} infra={ctx.infra_health_score:.2f}",
                )
            )

        with contextlib.suppress(Exception):
            await log_event(
                "reinvest_executed",
                {
                    "mint": pick.mint,
                    "symbol": pick.symbol,
                    "proceeds_sol": proceeds_sol,
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
                    "meta": meta,
                    "ts": now_iso,
                    "latency_s": max(0.0, time.monotonic() - t0),
                },
            )

        await _update_reinvest_health(redis, ctx, eff_pct=eff_pct, reinvest_sol=reinvest_sol, savings_sol=savings_sol, reason="executed")

        with contextlib.suppress(Exception):
            await note_reinvest_result(
                mint=pick.mint,
                proceeds_sol=proceeds_sol,
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

        log.info("[REINVEST] ✅ Completed into %s (tx=%s)", pick.mint, str(tx_sig)[:120])
        return pick.mint

    finally:
        if redis and lock_token:
            await _release_lock(redis, lk, lock_token)


# ──────────────────────────────────────────────────────────────────────────────
# Queue + worker (+ DLQ)
# ──────────────────────────────────────────────────────────────────────────────
async def queue_reinvest(proceeds_sol: float, meta: Optional[Dict[str, Any]] = None, redis=None) -> None:
    proceeds_sol = float(proceeds_sol or 0.0)
    if proceeds_sol <= 0:
        return

    meta = dict(meta or {})
    _ensure_job_id(meta, proceeds_sol)

    if redis is None:
        redis = await _get_redis()

    # no redis -> run direct
    if not redis:
        await reinvest(proceeds_sol, meta=meta, redis=None)
        return

    job = {
        "proceeds_sol": proceeds_sol,
        "meta": meta,
        "enqueued_ts": _utc_ts(),
        "enqueued_iso": _now_utc().isoformat(),
    }
    payload = json.dumps(job, separators=(",", ":"), ensure_ascii=False)

    try:
        await _redis_call(redis, "rpush", REINVEST_QUEUE, payload)
        if CFG.queue_maxlen > 0:
            with contextlib.suppress(Exception):
                await _redis_call(redis, "ltrim", REINVEST_QUEUE, -int(CFG.queue_maxlen), -1)
    except Exception as e:
        log.warning("[REINVEST] Queue push failed; falling back to direct reinvest. err=%s", str(e)[:200])
        await reinvest(proceeds_sol, meta=meta, redis=redis)

    log.info("[REINVEST] Queued job proceeds=%.6f job_id=%s", proceeds_sol, meta.get("job_id"))


async def _push_dlq(redis, payload: str, err: str) -> None:
    if not redis:
        return
    doc = {"payload": payload, "err": err[:300], "ts": _now_utc().isoformat()}
    try:
        await _redis_call(redis, "rpush", REINVEST_DLQ, json.dumps(doc))
        await _redis_call(redis, "ltrim", REINVEST_DLQ, -500, -1)
    except Exception:
        return


async def run_reinvest_worker(redis=None, poll_interval: float = 2.0, stop_evt: Optional[asyncio.Event] = None) -> None:
    if redis is None:
        redis = await _get_redis()
    if not redis:
        log.warning("[REINVEST] Worker disabled: no Redis client.")
        return

    # phase-based sane polling floors
    if CFG.phase <= 2:
        poll_interval = max(poll_interval, 2.0)
    elif CFG.phase <= 5:
        poll_interval = max(poll_interval, 1.5)
    else:
        poll_interval = max(poll_interval, 1.0)

    log.info("[REINVEST] Worker started (poll=%.2fs batch=%d phase=%d)", poll_interval, CFG.worker_batch, CFG.phase)
    backoff = float(CFG.worker_backoff_s)

    while True:
        if stop_evt is not None and stop_evt.is_set():
            log.info("[REINVEST] Worker stopping on stop_evt.")
            break

        processed = 0
        try:
            while processed < max(1, int(CFG.worker_batch)):
                raw = await _redis_call(redis, "blpop", REINVEST_QUEUE, timeout=int(max(1, poll_interval)))
                if not raw:
                    break

                try:
                    _, payload = raw
                except Exception:
                    payload = raw

                payload = _maybe_decode(payload)

                try:
                    job = json.loads(payload)
                except Exception:
                    log.warning("[REINVEST] Invalid job payload: %s", str(payload)[:240])
                    await _push_dlq(redis, str(payload)[:2000], "invalid_json")
                    continue

                # stale job guard (optional)
                if CFG.queue_max_age_s > 0:
                    enq = _safe_float(job.get("enqueued_ts"), 0.0)
                    if enq and (_utc_ts() - enq) > float(CFG.queue_max_age_s):
                        await _push_dlq(redis, payload[:4000], "stale_job_drop")
                        continue

                amt = float(job.get("proceeds_sol", 0.0) or 0.0)
                meta = job.get("meta") or {}
                if not isinstance(meta, dict):
                    meta = {"meta_raw": meta}

                if amt > 0:
                    await reinvest(amt, meta=meta, redis=redis)

                processed += 1

            if processed == 0:
                await asyncio.sleep(float(poll_interval))

        except asyncio.CancelledError:
            raise
        except Exception as e:
            log.error("[REINVEST] Worker error: %s", e, exc_info=True)
            await asyncio.sleep(backoff)


# ──────────────────────────────────────────────────────────────────────────────
# Preview + explain (+ high-level dry-run helper)
# ──────────────────────────────────────────────────────────────────────────────
async def preview_reinvest_split(proceeds_sol: float, meta: Optional[Dict[str, Any]] = None, redis=None) -> Optional[Dict[str, Any]]:
    meta = dict(meta or {})
    meta.setdefault("sandbox", True)  # hard safety
    meta["dry_run"] = True            # enforce no-swap for preview
    _ensure_job_id(meta, float(proceeds_sol or 0.0))

    if redis is None:
        redis = await _get_redis()

    proceeds_sol = float(proceeds_sol or 0.0)
    ctx = await _build_context(proceeds_sol, meta, redis)
    if ctx is None:
        return {"outcome": "skip", "reason": "context_build_failed", "proceeds_sol": proceeds_sol}

    allowed, reason = await _evaluate_gates(ctx, redis)
    health_score = _compute_health_score(ctx)
    eff_pct, scales = _compute_eff_pct(ctx)

    reinvest_sol = proceeds_sol * (eff_pct / 100.0)
    savings_sol = max(0.0, proceeds_sol - reinvest_sol)

    if not allowed:
        return {
            "outcome": "savings_only",
            "reason": reason,
            "proceeds_sol": proceeds_sol,
            "proceeds_usd": ctx.proceeds_usd,
            "eff_pct_final": 0.0,
            "reinvest_sol": 0.0,
            "savings_sol": proceeds_sol,
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
        "proceeds_sol": proceeds_sol,
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


async def explain_reinvest_decision(proceeds_sol: float, meta: Optional[Dict[str, Any]] = None, redis=None) -> Optional[Dict[str, Any]]:
    preview = await preview_reinvest_split(proceeds_sol, meta=meta, redis=redis)
    if not preview:
        return None

    scales = preview.get("scales") or {}
    ctx = preview.get("ctx") or {}

    deltas: Dict[str, float] = {}
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

    boost = sorted([(k, v) for k, v in deltas.items() if v > 0], key=lambda kv: kv[1], reverse=True)[:2]
    suppress = sorted([(k, v) for k, v in deltas.items() if v < 0], key=lambda kv: kv[1])[:2]

    def _fmt(k: str) -> str:
        return k.replace("_scale", "")

    preview["explanation"] = (
        f"Outcome={preview.get('outcome')} reason={preview.get('reason') or 'n/a'} | "
        f"eff={float(preview.get('eff_pct_final', 0.0)):.1f}% | "
        f"health={float(preview.get('health_score', 0.0)):.1f} "
        f"infra={float(preview.get('infra_health_score', 0.0)):.2f} "
        f"tail={ctx.get('tail_risk_score','n/a')} | "
        f"boosts={', '.join(f'{_fmt(k)}({v:+.2f})' for k,v in boost) or 'none'} | "
        f"suppressors={', '.join(f'{_fmt(k)}({v:+.2f})' for k,v in suppress) or 'none'}"
    )
    preview["boosts"] = boost
    preview["suppressors"] = suppress
    return preview


async def dry_run_reinvest(proceeds_sol: float, meta: Optional[Dict[str, Any]] = None, redis=None) -> Optional[Dict[str, Any]]:
    """
    High-level "what would happen" helper.

    - Forces sandbox + dry_run
    - Uses full context/scales logic
    - Returns preview + (optional) best target candidate
    """
    meta = dict(meta or {})
    meta.setdefault("sandbox", True)
    meta["dry_run"] = True
    prev = await preview_reinvest_split(proceeds_sol, meta=meta, redis=redis)
    if not prev or prev.get("outcome") != "reinvest":
        return prev

    # optionally pick a target with the previewed reinvest_sol amount
    reinvest_sol = float(prev.get("reinvest_sol", 0.0) or 0.0)
    if reinvest_sol <= 0:
        return prev

    if redis is None:
        redis = await _get_redis()

    _ensure_job_id(meta, float(proceeds_sol or 0.0))
    ctx_target_meta = dict(meta)
    pick = await _pick_target_mint(reinvest_sol, meta=ctx_target_meta, redis=redis)
    if pick:
        prev["target"] = {
            "mint": pick.mint,
            "symbol": pick.symbol,
            "score": pick.score,
            "raw": pick.raw,
        }
    else:
        prev["target"] = None

    return prev


# ──────────────────────────────────────────────────────────────────────────────
# Wiring helper: consume sell-filled events (fixes "profit vs proceeds")
# ──────────────────────────────────────────────────────────────────────────────
async def handle_sell_filled_event(ev: Dict[str, Any], redis=None) -> None:
    """
    Adapter for exit-engine events.

    Prefers proceeds fields if present; falls back to profit fields.
    Supports:
        meta["reinvest_profit_only"]=1 -> use realized_profit_sol
    """
    if not isinstance(ev, dict):
        return

    status = str(ev.get("status") or "").lower()
    if status and status not in ("filled", "fill", "success"):
        return

    meta = dict(ev.get("meta") or {})
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

    proceeds_sol = realized_profit if profit_only else (proceeds if proceeds > 0 else realized_profit)
    if proceeds_sol <= 0:
        return

    meta.setdefault("origin_trade_id", ev.get("trade_id") or ev.get("req_id") or ev.get("tx_sig"))
    meta.setdefault("origin_position_id", ev.get("position_id") or ev.get("pos_id"))
    meta.setdefault("tx_sig", ev.get("tx_sig") or ev.get("signature"))
    meta.setdefault("tag", "exit_engine")

    jid = meta.get("tx_sig") or meta.get("origin_trade_id") or str(int(time.time()))
    meta.setdefault("job_id", f"sellfill-{jid}")

    await queue_reinvest(proceeds_sol, meta=meta, redis=redis)


__all__ = [
    "reinvest",
    "queue_reinvest",
    "run_reinvest_worker",
    "get_reinvest_health",
    "get_last_reinvest_decision",
    "get_recent_reinvest_decisions",
    "preview_reinvest_split",
    "explain_reinvest_decision",
    "dry_run_reinvest",
    "handle_sell_filled_event",
    "reload_reinvestor_config",
]


if __name__ == "__main__":  # local sanity demo
    async def _demo():
        r = await _get_redis()
        await queue_reinvest(0.05, {"job_id": f"demo-{int(time.time())}", "dry_run": True}, redis=r)
        exp = await explain_reinvest_decision(0.05, {"job_id": "demo-explain", "sandbox": True}, redis=r)
        print(json.dumps(exp, indent=2, default=str))
        stop = asyncio.Event()

        async def stop_later():
            await asyncio.sleep(5)
            stop.set()

        asyncio.create_task(stop_later())
        await run_reinvest_worker(r, poll_interval=1.0, stop_evt=stop)

    asyncio.run(_demo())
