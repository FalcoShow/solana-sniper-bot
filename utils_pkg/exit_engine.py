# utils_pkg/exit_engine.py
# -*- coding: utf-8 -*-
from __future__ import annotations

"""
Unified Exit Engine — V3+++ / quant-grade / God-Tier (LOCK-IN, PRODUCTION)
========================================================================

ROLE
----
Central exit coordinator that enforces *protective* exits deterministically:

  ✅ Hard stop-loss (never overridden by AI)
  ✅ Trailing-stop (arm + trail)
  ✅ Time-stop / stagnant coin exit
  ✅ Dead-follow-through exit (early "no momentum" protection)
  ✅ Liquidity-collapse exit (if liquidity data exists)
  ✅ Router sell-quote sanity (best-effort) before enqueue/direct fallback

It also:
  • Tracks open positions (in-memory + optional reconciliation with position_store).
  • Forwards ticks to AI sell brain (utils_pkg.ai_sell_logic) best-effort.
  • Delegates execution to: AI hook → worker enqueue → direct fallback (last resort).
  • Mirrors state into Redis for dashboards / feature_builder.
  • Logs exit events to utils_pkg.data_logger (best-effort).
  • Notifies risk_manager / guardrails / self_learning hooks (best-effort).
  • DRYRUN short-circuits sells but keeps signals/logs.

HARD COMPATIBILITY CONTRACTS (DO NOT BREAK)
------------------------------------------
Public API (stable):
    async def on_buy_filled(ev: Dict[str, Any]) -> None
    async def on_trade_tick(raw_event: Dict[str, Any]) -> None
    async def on_buy_filled_event(ev: Dict[str, Any]) -> None
    async def on_buy_filled_legacy(raw_id_or_mint: str, tx_sig: str) -> None

Optional / additive:
    async def on_sell_filled(ev: Dict[str, Any]) -> None
    async def request_exit(ev: Dict[str, Any]) -> bool
    def get_open_positions_snapshot() -> Dict[str, Dict[str, Any]]
    def reload_exit_engine_config() -> None

Worker entrypoint (needed by scripts/main.py):
    async def run_worker(stop_evt: asyncio.Event) -> None
    async def exit_engine_loop(stop_evt: asyncio.Event) -> None  # alias

SAFETY GUARANTEES
-----------------
  • Hard SL always wins (AI never overrides).
  • If AI missing → worker/fallback still protect.
  • If Redis misconfigured → no behavior change; only no mirrors.
  • Worker/direct fallback ONLY allowed when token amount_raw is known (never guess).
  • Optional wallet balance resolution for amount_raw (best-effort).
  • Quote sanity is best-effort; failures are recorded and can be phase-gated.

"""

import asyncio
import contextlib
import datetime as _dt
import importlib
import json
import os
import time
from dataclasses import dataclass
from typing import Any, Callable, Dict, Optional, Set, Tuple

EXIT_ENGINE_VERSION = "V3+++_LOCKIN_2025-12-22"
INSTANCE_ID = (os.getenv("INSTANCE_ID") or os.getenv("RENDER_INSTANCE_ID") or os.getenv("HOSTNAME") or "").strip() or "local"
_SOL_MINT = "So11111111111111111111111111111111111111112"

# =============================================================================
# Logger (soft compatible: .event + .every)
# =============================================================================
try:
    from logging_setup import get_logger  # type: ignore

    log = get_logger("sniper.exit_engine")  # type: ignore
except Exception:  # pragma: no cover
    import logging

    log = logging.getLogger("sniper.exit_engine")
    if not log.handlers:
        logging.basicConfig(level=logging.INFO)

if not hasattr(log, "event"):

    def _event(name: str, **fields: Any) -> None:  # pragma: no cover
        with contextlib.suppress(Exception):
            log.info("%s %s", name, fields)

    setattr(log, "event", _event)

if not hasattr(log, "every"):

    def _every(key: str, n: int = 60, **fields: Any) -> None:  # pragma: no cover
        now = time.time()
        store = getattr(_every, "_store", {})  # type: ignore[attr-defined]
        last = float(store.get(key, 0.0))
        if now - last >= float(n):
            store[key] = now
            setattr(_every, "_store", store)  # type: ignore[attr-defined]
            with contextlib.suppress(Exception):
                log.event(key, **fields)  # type: ignore[attr-defined]

    setattr(log, "every", _every)


def _log(name: str, level: str = "info", **fields: Any) -> None:
    with contextlib.suppress(Exception):
        log.event(name, **fields)  # type: ignore[attr-defined]
        return
    with contextlib.suppress(Exception):
        if level == "warning":
            log.warning("%s %s", name, fields)
        elif level == "error":
            log.error("%s %s", name, fields)
        else:
            log.info("%s %s", name, fields)


# =============================================================================
# Env helpers
# =============================================================================
_TRUE = {"1", "true", "t", "yes", "y", "on"}
_FALSE = {"0", "false", "f", "no", "n", "off"}


def _env_bool(key: str, default: bool = False) -> bool:
    v = os.getenv(key)
    if v is None:
        return default
    s = str(v).strip().lower()
    if s in _TRUE:
        return True
    if s in _FALSE:
        return False
    return default


def _env_int(key: str, default: int) -> int:
    try:
        return int(float(str(os.getenv(key, str(default))).strip()))
    except Exception:
        return default


def _env_float(key: str, default: float) -> float:
    try:
        return float(str(os.getenv(key, str(default))).strip())
    except Exception:
        return default


def _env_str(key: str, default: str = "") -> str:
    return (os.getenv(key, default) or default).strip()


def _env_csv_lower(key: str) -> Set[str]:
    raw = (os.getenv(key, "") or "").strip()
    if not raw:
        return set()
    return {p.strip().lower() for p in raw.split(",") if p.strip()}


def _clamp(x: float, lo: float, hi: float) -> float:
    return lo if x < lo else hi if x > hi else x


def _finite(x: Any, d: float = 0.0) -> float:
    try:
        v = float(x)
        if v != v or v in (float("inf"), float("-inf")):
            return d
        return v
    except Exception:
        return d


async def _maybe_await(x: Any) -> Any:
    if asyncio.iscoroutine(x):
        return await x
    return x


def _short_exc(e: BaseException, n: int = 260) -> str:
    s = f"{type(e).__name__}: {e}"
    return (s[:n] + "…") if len(s) > n else s


def _json_default(o: Any) -> str:
    try:
        return str(o)
    except Exception:
        return "<unserializable>"


def _json_dumps(payload: Any) -> str:
    return json.dumps(payload, separators=(",", ":"), ensure_ascii=False, default=_json_default)


# =============================================================================
# Lazy imports (avoid cycles)
# =============================================================================
_MOD_CACHE: Dict[str, Any] = {}


def _get_mod(name: str) -> Any:
    if name in _MOD_CACHE:
        return _MOD_CACHE[name]
    try:
        m = importlib.import_module(name)
    except Exception:
        m = None
    _MOD_CACHE[name] = m
    return m


# =============================================================================
# Mint normalization
# =============================================================================
def _norm_mint(m: str) -> str:
    s = (m or "").strip()
    if not s:
        return ""
    try:
        mm = _get_mod("utils_pkg.mints")
        fn = getattr(mm, "normalize_mint", None) if mm else None
        if callable(fn):
            return str(fn(s))
    except Exception:
        pass
    return s


def _pool_normalize(pool: Optional[str]) -> str:
    if not pool:
        return "unknown"
    return str(pool).strip().lower() or "unknown"


# =============================================================================
# Config
# =============================================================================
@dataclass(frozen=True)
class ExitCfg:
    master_phase: int
    dryrun: bool
    enabled: bool

    # tracking / reconciliation
    track_all_positions: bool
    track_max_age_sec: int
    scan_positions_every_sec: int
    reconcile_with_position_store: bool
    reconcile_every_sec: int

    # follow-through (dead-follow)
    follow_enabled: bool
    follow_window_sec: int
    follow_grace_sec: int
    follow_extend_low_trades_sec: int
    follow_min_trades: int
    follow_min_up_pct: float
    follow_max_dd_pct: float
    follow_min_pnl_pct: float
    follow_require_price: bool
    follow_force_exit_on_smart_risk: bool
    follow_debug: bool
    follow_early_catastrophic_dd_pct: float  # if >0 and dd <= -X%, force exit even during warmup

    # protective exits (core)
    stop_loss_pct: float
    trailing_arm_pct: float
    trailing_stop_pct: float
    time_stop_min: int
    time_stop_min_profit_pct: float
    stagnant_max_min: int
    stagnant_min_move_pct: float

    # liquidity collapse
    liq_collapse_pct: float
    min_liq_usd: float

    # quote sanity
    quote_sanity_enabled: bool
    quote_timeout_s: float
    quote_max_price_impact_pct: float
    quote_allow_fail_on_hard_stop: bool

    # scope filters
    follow_pools: Set[str]
    follow_min_spend_sol: float
    follow_mint_whitelist: Set[str]
    follow_mint_blacklist: Set[str]

    # throttling / cleanup
    tick_throttle_ms: int
    cleanup_every_sec: int

    # redis mirror + caps
    publish_redis: bool
    redis_ttl_sec: int
    publish_open_snapshot: bool
    open_snapshot_every_sec: int
    max_daily_exits: int
    daily_key_prefix: str
    follow_state_key_prefix: str
    open_snapshot_key: str

    # health reporting
    health_redis_key: str
    health_every_sec: int

    # anti-spam / circuit breakers
    exit_dedupe_sec: int
    burst_cap_per_min: int

    # delegation / last resort
    allow_worker_enqueue: bool
    allow_direct_fallback: bool
    fallback_sell_pct: float
    fallback_slippage_bps: int
    fallback_priority: int

    # optional amount resolution
    resolve_amount_on_force_exit: bool
    resolve_amount_timeout_s: float

    # worker loop housekeeping
    worker_heartbeat_sec: int
    reload_cfg_every_sec: int

    # extra safe defaults
    forward_ai_ticks_when_disabled: bool
    price_resolve_enabled: bool
    price_resolve_ttl_sec: int
    price_resolve_min_interval_sec: float
    price_resolve_timeout_s: float


def _load_cfg() -> ExitCfg:
    master = _env_int("MASTER_PHASE", _env_int("STRATEGY_PHASE", 7))
    master = max(0, min(7, master))

    dryrun = (
        _env_bool("DRYRUN", False)
        or _env_bool("DRY_RUN", False)
        or _env_bool("IS_DRYRUN", False)
        or _env_bool("PAPER_TRADING", False)
    )

    enabled_flag = _env_bool("EXIT_ENGINE_ENABLED", True)
    enabled = (master > 0) and _env_bool("EXIT_ENGINE", True) and enabled_flag and (not _env_bool("KILL_SWITCH", False))

    # Defaults by phase
    if master <= 2:
        default_follow = True
        default_publish = True
        default_worker = True
        default_direct = False
        default_force_smart = False
    else:
        default_follow = True
        default_publish = True
        default_worker = True
        default_direct = True
        default_force_smart = True

    follow_enabled = _env_bool("EXIT_FOLLOW_ENABLED", default_follow)
    publish_redis = _env_bool("EXIT_FOLLOW_PUBLISH_REDIS", default_publish)
    allow_worker = _env_bool("EXIT_ALLOW_WORKER_ENQUEUE", default_worker)
    allow_direct = _env_bool("EXIT_ALLOW_DIRECT_FALLBACK", default_direct)

    if master <= 0:
        allow_direct = False

    # Protective exits: pull from your common PF_/EXIT_ vars (whichever exists)
    stop_loss = _env_float("EXIT_STOP_LOSS_PCT", _env_float("PF_SL_PCT", _env_float("EXIT_STOPLOSS_PCT", 8.0)))
    stop_loss = max(0.1, float(stop_loss))

    arm = _env_float("EXIT_TSL_ARM_PCT", _env_float("PF_TSL_ARM_PCT", 10.0))
    arm = max(0.0, float(arm))

    tsl = _env_float("EXIT_TSL_PCT", _env_float("PF_TSL_PCT", 4.5))
    tsl = max(0.1, float(tsl))

    time_stop_min = _env_int("EXIT_TIME_STOP_MIN", _env_int("EXIT_MAX_HOLD_MIN", 0))
    time_stop_min = max(0, int(time_stop_min))
    time_stop_min_profit = _env_float("EXIT_TIME_STOP_MIN_PROFIT_PCT", 0.0)

    stagnant_max = _env_int("EXIT_STAGNANT_MAX_MIN", 0)
    stagnant_max = max(0, int(stagnant_max))
    stagnant_move = _env_float("EXIT_STAGNANT_MIN_MOVE_PCT", 0.0)

    liq_collapse = _env_float("EXIT_LIQ_COLLAPSE_PCT", 0.0)
    min_liq = _env_float("EXIT_MIN_LIQ_USD", 0.0)

    fallback_slip = _env_int("EXIT_FALLBACK_SLIPPAGE_BPS", _env_int("EXIT_FOLLOW_FALLBACK_SLIPPAGE_BPS", _env_int("PF_SLIPPAGE_BPS", 200)))
    fallback_slip = max(5, min(5000, int(fallback_slip)))

    sell_pct = _env_float("EXIT_FALLBACK_SELL_PCT", _env_float("EXIT_FOLLOW_SELL_PCT", 1.0))
    sell_pct = max(0.01, min(1.0, float(sell_pct)))

    catastrophic_dd = _env_float("EXIT_FOLLOW_EARLY_CATASTROPHIC_DD_PCT", 0.0)
    catastrophic_dd = max(0.0, float(catastrophic_dd))

    quote_enabled = _env_bool("EXIT_QUOTE_SANITY", True)
    quote_timeout = max(0.15, _env_float("EXIT_QUOTE_TIMEOUT_S", 0.65))
    quote_pi = _env_float("EXIT_QUOTE_MAX_PRICE_IMPACT_PCT", 85.0)
    quote_allow_hard = _env_bool("EXIT_QUOTE_ALLOW_FAIL_ON_HARD_STOP", True)

    return ExitCfg(
        master_phase=master,
        dryrun=dryrun,
        enabled=enabled,
        track_all_positions=_env_bool("EXIT_TRACK_ALL_POSITIONS", True),
        track_max_age_sec=max(60, _env_int("EXIT_TRACK_MAX_AGE_SEC", 6 * 60 * 60)),
        scan_positions_every_sec=max(0, _env_int("EXIT_SCAN_POSITIONS_EVERY_SEC", 5)),
        reconcile_with_position_store=_env_bool("EXIT_RECONCILE_WITH_POSITION_STORE", True),
        reconcile_every_sec=max(1, _env_int("EXIT_RECONCILE_EVERY_SEC", 10)),
        follow_enabled=follow_enabled,
        follow_window_sec=max(3, _env_int("EXIT_FOLLOW_WINDOW_SEC", 35)),
        follow_grace_sec=max(0, _env_int("EXIT_FOLLOW_GRACE_SEC", 5)),
        follow_extend_low_trades_sec=max(0, _env_int("EXIT_FOLLOW_EXTEND_LOW_TRADES_SEC", 15)),
        follow_min_trades=max(0, _env_int("EXIT_FOLLOW_MIN_TRADES", 5)),
        follow_min_up_pct=_env_float("EXIT_FOLLOW_MIN_UP_PCT", 6.0),
        follow_max_dd_pct=max(0.0, _env_float("EXIT_FOLLOW_MAX_DD_PCT", 5.0)),
        follow_min_pnl_pct=_env_float("EXIT_FOLLOW_MIN_PNL_PCT", 0.0),
        follow_require_price=_env_bool("EXIT_FOLLOW_REQUIRE_PRICE", True),
        follow_force_exit_on_smart_risk=_env_bool("EXIT_FOLLOW_FORCE_EXIT_ON_SMART_RISK", default_force_smart),
        follow_debug=_env_bool("EXIT_FOLLOW_DEBUG", False),
        follow_early_catastrophic_dd_pct=catastrophic_dd,
        stop_loss_pct=stop_loss,
        trailing_arm_pct=arm,
        trailing_stop_pct=tsl,
        time_stop_min=time_stop_min,
        time_stop_min_profit_pct=float(time_stop_min_profit),
        stagnant_max_min=stagnant_max,
        stagnant_min_move_pct=float(stagnant_move),
        liq_collapse_pct=max(0.0, float(liq_collapse)),
        min_liq_usd=max(0.0, float(min_liq)),
        quote_sanity_enabled=quote_enabled,
        quote_timeout_s=float(quote_timeout),
        quote_max_price_impact_pct=float(quote_pi),
        quote_allow_fail_on_hard_stop=bool(quote_allow_hard),
        follow_pools=_env_csv_lower("EXIT_FOLLOW_POOLS"),
        follow_min_spend_sol=max(0.0, _env_float("EXIT_FOLLOW_MIN_SPEND_SOL", 0.0)),
        follow_mint_whitelist=_env_csv_lower("EXIT_FOLLOW_MINT_WHITELIST"),
        follow_mint_blacklist=_env_csv_lower("EXIT_FOLLOW_MINT_BLACKLIST"),
        tick_throttle_ms=max(0, _env_int("EXIT_TICK_THROTTLE_MS", _env_int("EXIT_FOLLOW_TICK_THROTTLE_MS", 250))),
        cleanup_every_sec=max(1, _env_int("EXIT_CLEANUP_EVERY_SEC", _env_int("EXIT_FOLLOW_CLEANUP_EVERY_SEC", 30))),
        publish_redis=publish_redis,
        redis_ttl_sec=max(30, _env_int("EXIT_REDIS_TTL_SEC", _env_int("EXIT_FOLLOW_REDIS_TTL_SEC", 3600))),
        publish_open_snapshot=_env_bool("EXIT_PUBLISH_OPEN_SNAPSHOT", False),
        open_snapshot_every_sec=max(5, _env_int("EXIT_OPEN_SNAPSHOT_EVERY_SEC", 30)),
        max_daily_exits=max(0, _env_int("EXIT_MAX_DAILY_EXITS", _env_int("EXIT_FOLLOW_MAX_DAILY_EXITS", 0))),
        daily_key_prefix=_env_str("EXIT_DAILY_KEY_PREFIX", _env_str("EXIT_FOLLOW_DAILY_KEY_PREFIX", "exit_engine:daily")) or "exit_engine:daily",
        follow_state_key_prefix=_env_str("EXIT_STATE_KEY_PREFIX", _env_str("EXIT_FOLLOW_STATE_KEY_PREFIX", "exit_state")) or "exit_state",
        open_snapshot_key=_env_str("EXIT_OPEN_SNAPSHOT_KEY", "exit_engine:open_positions") or "exit_engine:open_positions",
        health_redis_key=_env_str("EXIT_HEALTH_REDIS_KEY", "exit_engine:health") or "exit_engine:health",
        health_every_sec=max(0, _env_int("EXIT_HEALTH_EVERY_SEC", 30)),
        exit_dedupe_sec=max(5, _env_int("EXIT_DEDUPE_SEC", 25)),
        burst_cap_per_min=max(0, _env_int("EXIT_BURST_CAP_PER_MIN", 0)),
        allow_worker_enqueue=allow_worker,
        allow_direct_fallback=allow_direct,
        fallback_sell_pct=float(sell_pct),
        fallback_slippage_bps=int(fallback_slip),
        fallback_priority=max(0, _env_int("EXIT_FALLBACK_PRIORITY", _env_int("EXIT_FOLLOW_FALLBACK_PRIORITY", 1))),
        resolve_amount_on_force_exit=_env_bool("EXIT_RESOLVE_AMOUNT_ON_FORCE_EXIT", True),
        resolve_amount_timeout_s=max(0.25, _env_float("EXIT_RESOLVE_AMOUNT_TIMEOUT_S", 2.0)),
        worker_heartbeat_sec=max(5, _env_int("EXIT_WORKER_HEARTBEAT_SEC", 30)),
        reload_cfg_every_sec=max(0, _env_int("EXIT_RELOAD_CFG_EVERY_SEC", 0)),
        forward_ai_ticks_when_disabled=_env_bool("EXIT_FORWARD_AI_TICKS_WHEN_DISABLED", False),
        price_resolve_enabled=_env_bool("EXIT_PRICE_RESOLVE_ENABLED", False),
        price_resolve_ttl_sec=max(2, _env_int("EXIT_PRICE_RESOLVE_TTL_SEC", 6)),
        price_resolve_min_interval_sec=max(0.25, _env_float("EXIT_PRICE_RESOLVE_MIN_INTERVAL_SEC", 1.0)),
        price_resolve_timeout_s=max(0.15, _env_float("EXIT_PRICE_RESOLVE_TIMEOUT_S", 0.8)),
    )


CFG: ExitCfg = _load_cfg()


def reload_exit_engine_config() -> None:
    global CFG
    CFG = _load_cfg()
    _log(
        "EXIT_CFG_RELOADED",
        version=EXIT_ENGINE_VERSION,
        instance=INSTANCE_ID,
        phase=int(CFG.master_phase),
        enabled=int(CFG.enabled),
        dryrun=int(CFG.dryrun),
        follow=int(CFG.follow_enabled),
        sl=float(CFG.stop_loss_pct),
        arm=float(CFG.trailing_arm_pct),
        tsl=float(CFG.trailing_stop_pct),
        time_stop_min=int(CFG.time_stop_min),
        stagnant_max=int(CFG.stagnant_max_min),
        liq_collapse=float(CFG.liq_collapse_pct),
        quote=int(CFG.quote_sanity_enabled),
        worker=int(CFG.allow_worker_enqueue),
        direct=int(CFG.allow_direct_fallback),
        track_all=int(CFG.track_all_positions),
        reconcile=int(CFG.reconcile_with_position_store),
    )


# =============================================================================
# Optional module hooks
# =============================================================================
def _sell_hook(name: str) -> Optional[Callable[..., Any]]:
    m = _get_mod("utils_pkg.ai_sell_logic")
    if not m:
        return None
    fn = getattr(m, name, None)
    return fn if callable(fn) else None


async def _call_optional_hook(mod_name: str, fn_candidates: Tuple[str, ...], event: Dict[str, Any]) -> None:
    m = _get_mod(mod_name)
    if not m:
        return
    for name in fn_candidates:
        fn = getattr(m, name, None)
        if not callable(fn):
            continue
        try:
            if asyncio.iscoroutinefunction(fn):
                await fn(event)  # type: ignore[misc]
            else:
                await asyncio.to_thread(fn, event)  # type: ignore[misc]
        except Exception:
            pass
        break


async def _log_exit_event_safe(event: Dict[str, Any]) -> None:
    m = _get_mod("utils_pkg.data_logger")
    fn = getattr(m, "log_exit_event", None) if m else None
    if callable(fn):
        with contextlib.suppress(Exception):
            if asyncio.iscoroutinefunction(fn):
                await fn(event)  # type: ignore[misc]
            else:
                await asyncio.to_thread(fn, event)  # type: ignore[misc]

    await _call_optional_hook("utils_pkg.self_learning_ai_strategy", ("on_exit_event", "on_forced_exit", "on_exit_signal"), event)
    await _call_optional_hook("utils_pkg.risk_manager", ("on_forced_exit", "on_exit_event"), event)


async def _guardrails_note_close(realized_pnl_sol: float, mint: str) -> None:
    m = _get_mod("utils_pkg.guardrails")
    fn = getattr(m, "note_close", None) if m else None
    if not callable(fn):
        return
    with contextlib.suppress(Exception):
        out = fn(float(realized_pnl_sol), mint=mint)  # type: ignore[misc]
        await _maybe_await(out)


# =============================================================================
# Redis helpers (best-effort)
# =============================================================================
def _get_redis() -> Any:
    cu = _get_mod("utils_pkg.core_utils")
    if not cu:
        return None
    r = getattr(cu, "redis", None)
    if r is not None:
        return r
    gr = getattr(cu, "get_redis", None)
    if callable(gr):
        return gr()
    gra = getattr(cu, "get_redis_async", None)
    if callable(gra):
        return gra()
    return None


def _today_key(prefix: str) -> str:
    now = _dt.datetime.now(_dt.timezone.utc)
    return f"{prefix}:{now.strftime('%Y%m%d')}"


async def _redis_get_daily(redis: Any) -> int:
    if not redis or CFG.max_daily_exits <= 0:
        return 0
    key = _today_key(CFG.daily_key_prefix)
    try:
        raw = await _maybe_await(redis.get(key))
        if raw is None:
            return 0
        if isinstance(raw, (bytes, bytearray)):
            raw = raw.decode(errors="ignore")
        return int(str(raw).strip() or "0")
    except Exception:
        return 0


async def _redis_incr_daily(redis: Any) -> None:
    if not redis or CFG.max_daily_exits <= 0:
        return
    key = _today_key(CFG.daily_key_prefix)
    try:
        pipe = redis.pipeline()
        pipe.incr(key)
        pipe.expire(key, 60 * 60 * 36)
        res = pipe.execute()
        await _maybe_await(res)
        return
    except Exception:
        pass
    with contextlib.suppress(Exception):
        await _maybe_await(redis.incr(key))
        await _maybe_await(redis.expire(key, 60 * 60 * 36))


async def _redis_set_json(redis: Any, key: str, payload: Dict[str, Any], ttl: int) -> None:
    try:
        val = _json_dumps(payload)
        res = redis.set(key, val, ex=int(ttl))
        await _maybe_await(res)
    except Exception:
        return


# =============================================================================
# Position state
# =============================================================================
class _Pos:
    __slots__ = (
        "mint",
        "symbol",
        "pool",
        "entry_ts",
        "entry_sig",
        "spend_sol",
        "amount_raw",
        "candidate_id",
        "opened_from",
        "entry_price",
        "max_price",
        "min_price",
        "last_price",
        "last_liq_usd",
        "entry_liq_usd",
        "trades_after_entry",
        "last_eval_ts",
        "last_feed_trade_counter",
        "last_price_resolve_ts",
        "closed",
        "follow_done",
        "follow_pass",
        "last_tick_ts",
    )

    def __init__(
        self,
        *,
        mint: str,
        symbol: str,
        pool: str,
        entry_sig: str,
        spend_sol: float,
        amount_raw: Optional[int],
        entry_price: Optional[float],
        candidate_id: Optional[Any],
        opened_from: str,
        entry_ts: Optional[float] = None,
        entry_liq_usd: Optional[float] = None,
    ) -> None:
        now = time.time()
        self.mint = mint
        self.symbol = symbol
        self.pool = pool
        self.entry_ts = float(entry_ts) if isinstance(entry_ts, (int, float)) and entry_ts > 0 else now
        self.entry_sig = entry_sig
        self.spend_sol = float(spend_sol or 0.0)
        self.amount_raw = int(amount_raw) if isinstance(amount_raw, int) and amount_raw > 0 else None
        self.candidate_id = candidate_id
        self.opened_from = opened_from

        self.entry_price = float(entry_price) if isinstance(entry_price, (int, float)) and entry_price > 0 else None
        self.max_price = self.entry_price
        self.min_price = self.entry_price
        self.last_price = self.entry_price

        el = float(entry_liq_usd) if isinstance(entry_liq_usd, (int, float)) and float(entry_liq_usd) > 0 else None
        self.entry_liq_usd = el
        self.last_liq_usd = el

        self.trades_after_entry = 0
        self.last_eval_ts = 0.0
        self.last_feed_trade_counter = None  # type: Optional[int]
        self.last_price_resolve_ts = 0.0
        self.closed = False

        self.follow_done = False
        self.follow_pass = False

        self.last_tick_ts = now


_OPEN: Dict[str, _Pos] = {}
_OPEN_LOCK = asyncio.Lock()

_LAST_OPEN_SNAPSHOT: Dict[str, Dict[str, Any]] = {}
_LAST_OPEN_CACHE_TS = 0.0
_LAST_OPEN_PUBLISH_TS = 0.0
_LAST_CLEANUP_TS = 0.0
_LAST_CFG_RELOAD_TS = 0.0
_LAST_HEALTH_TS = 0.0
_LAST_RECONCILE_TS = 0.0

_RECENT_EXIT_TS: Dict[str, float] = {}
_BURST_BUCKET: Dict[str, int] = {}

# price resolve cache (safe + tiny)
_PRICE_CACHE: Dict[str, Tuple[float, float]] = {}  # mint -> (ts, price)


# =============================================================================
# Event extraction (robust best-effort)
# =============================================================================
def _extract_mint(ev: Dict[str, Any]) -> Optional[str]:
    for key in ("mint", "mintAddress", "mint_address", "tokenMint", "baseMint", "id", "address", "token"):
        v = ev.get(key)
        if isinstance(v, str) and v:
            m = _norm_mint(v)
            return m if m else None
    with contextlib.suppress(Exception):
        for k in ("pair", "token", "base", "asset"):
            o = ev.get(k)
            if isinstance(o, dict):
                for kk in ("mint", "address", "id"):
                    v = o.get(kk)
                    if isinstance(v, str) and v:
                        m = _norm_mint(v)
                        return m if m else None
    return None


def _extract_signature(ev: Dict[str, Any]) -> str:
    for key in ("signature", "tx_sig", "txSig", "sig", "txid", "tx", "transaction", "id"):
        v = ev.get(key)
        if isinstance(v, str) and v.strip():
            return v.strip()
    return ""


def _extract_symbol(ev: Dict[str, Any], mint: str) -> str:
    for key in ("symbol", "sym", "ticker"):
        v = ev.get(key)
        if isinstance(v, str) and v.strip():
            return v.strip()
    return (mint[:4] if mint else "").strip()


def _extract_pool(ev: Dict[str, Any]) -> str:
    for key in ("pool", "source", "venue", "dex", "market"):
        v = ev.get(key)
        if isinstance(v, str) and v.strip():
            return _pool_normalize(v)
    return "unknown"


def _extract_price(ev: Dict[str, Any]) -> Optional[float]:
    for key in (
        "priceUi",
        "price_ui",
        "price",
        "priceInSol",
        "price_in_sol",
        "ui_price",
        "sol_per_token",
        "token_price_sol",
        "last_price",
        "lastPrice",
        "close",
        "mid",
        "mark",
        "p",
    ):
        v = ev.get(key)
        if v is None:
            continue
        with contextlib.suppress(Exception):
            f = float(v)
            if f > 0:
                return f
    return None


def _extract_amount_raw(ev: Dict[str, Any]) -> Optional[int]:
    for key in (
        "amount_raw",
        "amount_out",
        "base_amount_raw",
        "token_amount_raw",
        "out_amount_raw",
        "tokenAmountRaw",
        "amountRaw",
        "size_raw",
    ):
        v = ev.get(key)
        if v is None:
            continue
        with contextlib.suppress(Exception):
            iv = int(v)
            if iv > 0:
                return iv
    return None


def _extract_spend_sol(ev: Dict[str, Any]) -> float:
    for key in ("spend_sol", "buy_size_sol", "amount_sol", "entry_size_sol", "spent_sol", "in_sol", "basis_sol"):
        if ev.get(key) is None:
            continue
        with contextlib.suppress(Exception):
            return float(ev[key])
    return 0.0


def _extract_trade_counter(ev: Dict[str, Any]) -> Optional[int]:
    for k in ("trades", "trade_count", "txns", "transactions", "txCount", "txnCount"):
        v = ev.get(k)
        if v is None:
            continue
        with contextlib.suppress(Exception):
            iv = int(v)
            return iv if iv >= 0 else None
    return None


def _extract_liq_usd(ev: Dict[str, Any]) -> Optional[float]:
    for k in ("liq", "liquidity", "liquidityUsd", "liquidity_usd", "liq_usd", "liquidityUSD"):
        v = ev.get(k)
        if v is None:
            continue
        with contextlib.suppress(Exception):
            f = float(v)
            return f if f > 0 else None
    with contextlib.suppress(Exception):
        meta = ev.get("meta")
        if isinstance(meta, dict):
            for k in ("liq", "liquidity", "liquidityUsd", "liquidity_usd", "liq_usd"):
                v = meta.get(k)
                if v is None:
                    continue
                f = float(v)
                return f if f > 0 else None
    return None


# =============================================================================
# Position_store (best-effort)
# =============================================================================
def _position_store_get_position(mint: str) -> Optional[Dict[str, Any]]:
    ps = _get_mod("utils_pkg.position_store")
    if not ps:
        return None
    for nm in ("get_position", "get_open_position", "position", "get"):
        fn = getattr(ps, nm, None)
        if not callable(fn):
            continue
        try:
            out = fn(mint)  # may be sync
            if asyncio.iscoroutine(out):
                # caller should not await here; keep best-effort for sync only
                return None
            if isinstance(out, dict):
                return out
        except Exception:
            continue
    return None


async def _position_store_open_snapshot() -> Optional[Dict[str, Any]]:
    ps = _get_mod("utils_pkg.position_store")
    if not ps:
        return None
    for nm in ("get_open_positions_snapshot", "open_positions_snapshot", "snapshot_open", "get_open_positions"):
        fn = getattr(ps, nm, None)
        if not callable(fn):
            continue
        try:
            if asyncio.iscoroutinefunction(fn):
                out = await fn()  # type: ignore[misc]
            else:
                out = await asyncio.to_thread(fn)  # type: ignore[misc]
            if isinstance(out, dict):
                return out
        except Exception:
            continue
    return None


# =============================================================================
# Cleanup / snapshots / health
# =============================================================================
def _build_open_snapshot_nolock(now: float) -> Dict[str, Dict[str, Any]]:
    snap: Dict[str, Dict[str, Any]] = {}
    for mint, pos in _OPEN.items():
        if getattr(pos, "closed", False):
            continue
        snap[mint] = {
            "mint": pos.mint,
            "symbol": pos.symbol,
            "pool": pos.pool,
            "age_sec": now - pos.entry_ts,
            "spend_sol": pos.spend_sol,
            "amount_raw": pos.amount_raw,
            "trades_after_entry": pos.trades_after_entry,
            "entry_price": pos.entry_price,
            "max_price": pos.max_price,
            "min_price": pos.min_price,
            "last_price": pos.last_price,
            "entry_liq_usd": pos.entry_liq_usd,
            "last_liq_usd": pos.last_liq_usd,
            "candidate_id": pos.candidate_id,
            "opened_from": pos.opened_from,
            "follow_done": int(pos.follow_done),
            "follow_pass": int(pos.follow_pass),
            "last_tick_ts": pos.last_tick_ts,
            "version": EXIT_ENGINE_VERSION,
            "instance": INSTANCE_ID,
        }
    return snap


async def _refresh_snapshot_cache(now: float) -> None:
    global _LAST_OPEN_SNAPSHOT, _LAST_OPEN_CACHE_TS
    if now - _LAST_OPEN_CACHE_TS < 0.9:
        return
    async with _OPEN_LOCK:
        _LAST_OPEN_SNAPSHOT = _build_open_snapshot_nolock(now)
        _LAST_OPEN_CACHE_TS = now


async def _cleanup_if_needed(now: float) -> None:
    global _LAST_CLEANUP_TS
    if now - _LAST_CLEANUP_TS < float(max(1, CFG.cleanup_every_sec)):
        return
    _LAST_CLEANUP_TS = now

    dropped = 0
    async with _OPEN_LOCK:
        for mint, pos in list(_OPEN.items()):
            if pos.closed:
                _OPEN.pop(mint, None)
                dropped += 1
                continue
            if (now - pos.entry_ts) > float(CFG.track_max_age_sec):
                _OPEN.pop(mint, None)
                dropped += 1

    if CFG.follow_debug and dropped:
        _log("EXIT_CLEANUP", dropped=dropped, open_count=len(_OPEN), instance=INSTANCE_ID)


async def _publish_open_snapshot(now: float) -> None:
    global _LAST_OPEN_PUBLISH_TS
    if not (CFG.publish_redis and CFG.publish_open_snapshot):
        return
    if now - _LAST_OPEN_PUBLISH_TS < float(max(5, CFG.open_snapshot_every_sec)):
        return

    await _refresh_snapshot_cache(now)

    redis = await _maybe_await(_get_redis())
    if not redis:
        return

    with contextlib.suppress(Exception):
        await _redis_set_json(
            redis,
            CFG.open_snapshot_key,
            {"ts": now, "open": dict(_LAST_OPEN_SNAPSHOT), "version": EXIT_ENGINE_VERSION, "instance": INSTANCE_ID},
            int(CFG.redis_ttl_sec),
        )
        _LAST_OPEN_PUBLISH_TS = now


async def _publish_health(now: float) -> None:
    global _LAST_HEALTH_TS
    if not (CFG.publish_redis and CFG.health_every_sec > 0):
        return
    if now - _LAST_HEALTH_TS < float(max(5, CFG.health_every_sec)):
        return
    _LAST_HEALTH_TS = now

    redis = await _maybe_await(_get_redis())
    if not redis:
        return

    payload = {
        "ts": now,
        "version": EXIT_ENGINE_VERSION,
        "instance": INSTANCE_ID,
        "enabled": int(CFG.enabled),
        "dryrun": int(CFG.dryrun),
        "phase": int(CFG.master_phase),
        "open_count": int(len(_LAST_OPEN_SNAPSHOT)),
        "sl_pct": float(CFG.stop_loss_pct),
        "arm_pct": float(CFG.trailing_arm_pct),
        "tsl_pct": float(CFG.trailing_stop_pct),
        "time_stop_min": int(CFG.time_stop_min),
        "stagnant_max_min": int(CFG.stagnant_max_min),
        "liq_collapse_pct": float(CFG.liq_collapse_pct),
        "quote": int(CFG.quote_sanity_enabled),
        "dedupe_sec": int(CFG.exit_dedupe_sec),
        "burst_cap": int(CFG.burst_cap_per_min),
        "worker": int(CFG.allow_worker_enqueue),
        "direct": int(CFG.allow_direct_fallback),
    }
    await _redis_set_json(redis, CFG.health_redis_key, payload, int(max(30, CFG.redis_ttl_sec)))


async def _housekeeping(now: float) -> None:
    global _LAST_CFG_RELOAD_TS
    if CFG.reload_cfg_every_sec > 0 and (now - _LAST_CFG_RELOAD_TS) >= float(CFG.reload_cfg_every_sec):
        _LAST_CFG_RELOAD_TS = now
        with contextlib.suppress(Exception):
            reload_exit_engine_config()

    await _cleanup_if_needed(now)
    await _refresh_snapshot_cache(now)
    await _publish_open_snapshot(now)
    await _publish_health(now)


# =============================================================================
# Amount resolution (best-effort, never guess)
# =============================================================================
async def _resolve_amount_raw_from_wallet(mint: str) -> Optional[int]:
    if not CFG.resolve_amount_on_force_exit:
        return None
    mint = _norm_mint(mint)
    if not mint or mint == _SOL_MINT:
        return None

    candidates: list[Any] = []

    cu = _get_mod("utils_pkg.core_utils")
    if cu:
        for nm in ("get_token_balance_raw", "token_balance_raw", "get_wallet_token_balance_raw"):
            fn = getattr(cu, nm, None)
            if callable(fn):
                candidates.append(fn)

    sw = _get_mod("utils_pkg.solana_wallet")
    if sw:
        for nm in ("get_token_balance_raw", "token_balance_raw", "get_wallet_token_balance_raw"):
            fn = getattr(sw, nm, None)
            if callable(fn):
                candidates.append(fn)

    router = _get_mod("utils_pkg.router")
    if router:
        for nm in ("get_token_balance_raw", "token_balance_raw"):
            fn = getattr(router, nm, None)
            if callable(fn):
                candidates.append(fn)

    if not candidates:
        return None

    async def _call(fn: Any) -> Optional[int]:
        try:
            if asyncio.iscoroutinefunction(fn):
                out = await asyncio.wait_for(fn(mint), timeout=float(CFG.resolve_amount_timeout_s))
            else:
                out = await asyncio.wait_for(asyncio.to_thread(fn, mint), timeout=float(CFG.resolve_amount_timeout_s))
            iv = int(out)
            return iv if iv > 0 else None
        except Exception:
            return None

    for fn in candidates[:10]:
        amt = await _call(fn)
        if amt:
            return amt
    return None


# =============================================================================
# Price resolution (optional)
# =============================================================================
async def _resolve_price_best_effort(mint: str) -> Optional[float]:
    if not CFG.price_resolve_enabled:
        return None
    mint = _norm_mint(mint)
    if not mint or mint == _SOL_MINT:
        return None

    now = time.time()
    cached = _PRICE_CACHE.get(mint)
    if cached and (now - float(cached[0])) <= float(CFG.price_resolve_ttl_sec):
        p = float(cached[1])
        return p if p > 0 else None

    candidates: list[Callable[..., Any]] = []

    cu = _get_mod("utils_pkg.core_utils")
    if cu:
        for nm in ("get_price_sol", "get_token_price_sol", "token_price_sol", "price_in_sol"):
            fn = getattr(cu, nm, None)
            if callable(fn):
                candidates.append(fn)

    router = _get_mod("utils_pkg.router")
    if router:
        for nm in ("get_token_price_sol", "get_price_sol", "token_price_sol", "price_in_sol"):
            fn = getattr(router, nm, None)
            if callable(fn):
                candidates.append(fn)

    if not candidates:
        return None

    async def _call(fn: Any) -> Optional[float]:
        try:
            if asyncio.iscoroutinefunction(fn):
                out = await asyncio.wait_for(fn(mint), timeout=float(CFG.price_resolve_timeout_s))
            else:
                out = await asyncio.wait_for(asyncio.to_thread(fn, mint), timeout=float(CFG.price_resolve_timeout_s))
            p = float(out)
            return p if p > 0 else None
        except Exception:
            return None

    for fn in candidates[:6]:
        p = await _call(fn)
        if p:
            _PRICE_CACHE[mint] = (now, float(p))
            if len(_PRICE_CACHE) > 2500:
                cutoff = now - max(5.0, float(CFG.price_resolve_ttl_sec) * 3.0)
                for k, (ts, _) in list(_PRICE_CACHE.items()):
                    if ts < cutoff:
                        _PRICE_CACHE.pop(k, None)
            return float(p)
    return None


# =============================================================================
# Anti-spam / caps
# =============================================================================
def _minute_key(now: float) -> str:
    dt = _dt.datetime.fromtimestamp(now, tz=_dt.timezone.utc)
    return dt.strftime("%Y%m%d%H%M")


def _exit_dedupe_ok(mint: str, now: float) -> bool:
    if not mint:
        return False
    last = float(_RECENT_EXIT_TS.get(mint, 0.0))
    if now - last < float(CFG.exit_dedupe_sec):
        return False
    _RECENT_EXIT_TS[mint] = now
    if len(_RECENT_EXIT_TS) > 5000:
        cutoff = now - 3600.0
        for k, v in list(_RECENT_EXIT_TS.items()):
            if v < cutoff:
                _RECENT_EXIT_TS.pop(k, None)
    return True


def _burst_cap_ok(now: float) -> bool:
    cap = int(CFG.burst_cap_per_min)
    if cap <= 0:
        return True
    mk = _minute_key(now)
    _BURST_BUCKET[mk] = int(_BURST_BUCKET.get(mk, 0)) + 1
    if len(_BURST_BUCKET) > 10:
        for k in list(_BURST_BUCKET.keys()):
            if k != mk:
                _BURST_BUCKET.pop(k, None)
    return _BURST_BUCKET[mk] <= cap


# =============================================================================
# Quote sanity (router best-effort)
# =============================================================================
def _lamports_to_sol_best_effort(lamports: int) -> float:
    try:
        cu = _get_mod("utils_pkg.core_utils")
        fn = getattr(cu, "lamports_to_sol", None) if cu else None
        if callable(fn):
            return float(fn(int(lamports)))
    except Exception:
        pass
    return float(lamports) / 1_000_000_000.0


async def _sell_quote_sanity(mint: str, amount_raw: int, slippage_bps: int) -> Tuple[bool, Dict[str, Any]]:
    """
    Returns (ok, info). ok means we found a route and outAmount > 0 and priceImpact sane (if present).
    If router isn't available, returns ok=True with {"note":"no_router_quote"} (doesn't block).
    """
    info: Dict[str, Any] = {"mint": mint, "amount_raw": int(amount_raw), "slippage_bps": int(slippage_bps)}
    router = _get_mod("utils_pkg.router")
    if not router:
        info["note"] = "no_router_module"
        return True, info

    fn = getattr(router, "get_sell_quote", None) or getattr(router, "sell_quote", None)
    if not callable(fn):
        # fallback to generic quote
        fn = getattr(router, "get_quote", None) or getattr(router, "quote", None)
    if not callable(fn):
        info["note"] = "no_quote_fn"
        return True, info

    async def _call() -> Any:
        # try several signatures
        try:
            return fn(mint=mint, amount_raw=int(amount_raw), slippage_bps=int(slippage_bps))
        except TypeError:
            pass
        try:
            return fn(mint, int(amount_raw), int(slippage_bps))
        except TypeError:
            pass
        # generic quote style
        try:
            return fn(inputMint=mint, outputMint=_SOL_MINT, amount=int(amount_raw), slippageBps=int(slippage_bps))
        except TypeError:
            pass
        return fn(mint, _SOL_MINT, int(amount_raw), int(slippage_bps))

    try:
        q = await asyncio.wait_for(_maybe_await(_call()), timeout=float(CFG.quote_timeout_s))
    except Exception as e:
        info["note"] = "quote_error"
        info["err"] = _short_exc(e)
        return False, info

    # normalize q
    if isinstance(q, str):
        with contextlib.suppress(Exception):
            qj = json.loads(q)
            q = qj

    if isinstance(q, (list, tuple)) and q:
        # sometimes (ok, quote) or (quote,)
        if isinstance(q[0], dict) and len(q) == 1:
            q = q[0]
        elif len(q) >= 2 and isinstance(q[1], dict):
            info["tuple_ok"] = bool(q[0])
            q = q[1]

    if not isinstance(q, dict):
        info["note"] = "quote_non_dict"
        info["type"] = str(type(q).__name__)
        return False, info

    info["quote"] = q

    # out amount detection
    out_raw = None
    for k in ("outAmount", "out_amount", "outAmountRaw", "out_amount_raw", "outLamports", "out_lamports"):
        if q.get(k) is None:
            continue
        with contextlib.suppress(Exception):
            out_raw = int(q[k])
            break

    if out_raw is None:
        # sometimes nested
        with contextlib.suppress(Exception):
            route = q.get("routePlan")
            if isinstance(route, list) and route:
                # still no out amount, accept route existence
                info["note"] = "route_no_out_amount"
                return True, info

        info["note"] = "no_out_amount"
        return False, info

    info["out_amount_raw"] = int(out_raw)
    info["out_sol_est"] = float(_lamports_to_sol_best_effort(int(out_raw)))

    if int(out_raw) <= 0:
        info["note"] = "zero_out"
        return False, info

    # price impact check (if present)
    pi = None
    for k in ("priceImpactPct", "price_impact_pct", "priceImpact", "priceImpactBps"):
        if q.get(k) is None:
            continue
        with contextlib.suppress(Exception):
            pi = float(q[k])
            break
    if pi is not None:
        # if bps, normalize if huge
        if pi > 100.0 and pi <= 10_000.0:
            pi = pi / 100.0
        info["price_impact_pct"] = float(pi)
        if float(pi) > float(CFG.quote_max_price_impact_pct):
            info["note"] = "price_impact_too_high"
            return False, info

    return True, info


# =============================================================================
# Delegation (AI hooks -> worker -> direct fallback)
# =============================================================================
async def _direct_sell_best_effort(*, mint: str, amount_raw: int, slippage_bps: int) -> Optional[str]:
    # 1) lightning_impl.sell_via_lightning (preferred)
    with contextlib.suppress(Exception):
        LI = _get_mod("utils_pkg.lightning_impl")
        fn = getattr(LI, "sell_via_lightning", None) if LI else None
        if callable(fn):
            try:
                out = fn(mint=mint, token_program=None, amount_raw=int(amount_raw), slippage_bps=int(slippage_bps))
            except TypeError:
                try:
                    out = fn(mint=mint, amount_raw=int(amount_raw), slippage_bps=int(slippage_bps))
                except TypeError:
                    out = fn(mint, int(amount_raw), int(slippage_bps))
            sig = await _maybe_await(out)
            if isinstance(sig, str) and sig.strip():
                return sig.strip()

    # 2) router.sell / router.sell_token
    with contextlib.suppress(Exception):
        router = _get_mod("utils_pkg.router")
        fn = (getattr(router, "sell", None) or getattr(router, "sell_token", None)) if router else None
        if callable(fn):
            try:
                out = fn(mint=mint, amount_raw=int(amount_raw), slippage_bps=int(slippage_bps))
            except TypeError:
                out = fn(mint, int(amount_raw), int(slippage_bps))
            sig2 = await _maybe_await(out)
            if isinstance(sig2, str) and sig2.strip():
                return sig2.strip()

    return None


async def _enqueue_exit_request_best_effort(**kwargs: Any) -> bool:
    candidates = [
        ("utils_pkg.auto_exit_worker", "enqueue_exit_request"),
        ("utils_pkg.auto_exit_worker.worker", "enqueue_exit_request"),
        ("utils_pkg.exit_worker", "enqueue_exit_request"),
        ("utils_pkg.exit_worker.worker", "enqueue_exit_request"),
    ]
    for mod_name, fn_name in candidates:
        mod = _get_mod(mod_name)
        fn = getattr(mod, fn_name, None) if mod else None
        if not callable(fn):
            continue
        try:
            await _maybe_await(fn(**kwargs))
            return True
        except Exception:
            continue
    return False


def _scale_amount(amount_raw: int, sell_pct: float) -> int:
    if amount_raw <= 0:
        return 0
    pct = _clamp(float(sell_pct), 0.01, 1.0)
    amt = int(amount_raw * pct)
    return max(1, amt)


async def _delegate_force_exit(exit_ev: Dict[str, Any], *, token_amount_raw: Optional[int], hard: bool) -> bool:
    mint = _norm_mint(str(exit_ev.get("mint") or ""))
    exit_ev["mint"] = mint

    if not mint or mint == _SOL_MINT:
        _log("EXIT_REFUSE_BAD_MINT", level="warning", mint=mint, instance=INSTANCE_ID)
        return False

    now = time.time()
    if not _exit_dedupe_ok(mint, now):
        _log("EXIT_DEDUPE_SKIP", mint=mint, reason=str(exit_ev.get("reason") or ""), instance=INSTANCE_ID)
        return False

    if not _burst_cap_ok(now):
        _log("EXIT_BURST_CAP_SKIP", level="warning", mint=mint, cap=int(CFG.burst_cap_per_min), instance=INSTANCE_ID)
        return False

    amt = int(token_amount_raw) if isinstance(token_amount_raw, int) and token_amount_raw > 0 else None
    if amt is None:
        amt = await _resolve_amount_raw_from_wallet(mint)
        if amt:
            exit_ev["resolved_amount_raw"] = int(amt)

    exit_ev.setdefault("slippage_bps", int(CFG.fallback_slippage_bps))
    exit_ev.setdefault("priority", int(CFG.fallback_priority))
    exit_ev.setdefault("sell_pct", float(CFG.fallback_sell_pct))
    exit_ev.setdefault("version", EXIT_ENGINE_VERSION)
    exit_ev.setdefault("instance", INSTANCE_ID)
    exit_ev.setdefault("dryrun", int(bool(CFG.dryrun)))
    exit_ev.setdefault("ts", now)

    # quote sanity (best-effort) — if router missing, doesn't block.
    quote_ok = True
    quote_info: Dict[str, Any] = {"note": "no_amount_for_quote"}
    if CFG.quote_sanity_enabled and amt and amt > 0:
        sell_pct = _clamp(_finite(exit_ev.get("sell_pct"), float(CFG.fallback_sell_pct)), 0.01, 1.0)
        scaled_amt = _scale_amount(int(amt), sell_pct)
        quote_ok, quote_info = await _sell_quote_sanity(mint, int(scaled_amt), int(exit_ev.get("slippage_bps") or int(CFG.fallback_slippage_bps)))
        exit_ev["quote_sanity"] = {"ok": int(bool(quote_ok)), **quote_info}

        if not quote_ok and (not hard or not CFG.quote_allow_fail_on_hard_stop):
            _log("EXIT_QUOTE_SANITY_BLOCK", level="warning", mint=mint, hard=int(hard), note=str(quote_info.get("note") or ""), instance=INSTANCE_ID)
            await _log_exit_event_safe({**exit_ev, "label": "exit_block_quote"})
            return False

    # AI hook first (unless hard stop-loss: we still allow AI scheduling, but it cannot override)
    for hook_name in ("schedule_force_exit", "force_exit", "request_exit", "exit_now"):
        hook = _sell_hook(hook_name)
        if hook is None:
            continue
        try:
            if amt:
                exit_ev.setdefault("amount_raw", int(amt))
            exit_ev.setdefault("hard", int(bool(hard)))
            await _maybe_await(hook(exit_ev))
            _log("EXIT_DELEGATED_AI", mint=mint, hook=hook_name, hard=int(hard), instance=INSTANCE_ID)
            return True
        except Exception as e:
            _log("EXIT_AI_HOOK_FAIL", level="warning", mint=mint, hook=hook_name, err=_short_exc(e), instance=INSTANCE_ID)

    # From here: REQUIRE a known amount
    if not amt or amt <= 0:
        _log("EXIT_NO_AMOUNT_BLOCK", level="warning", mint=mint, reason=str(exit_ev.get("reason") or ""), instance=INSTANCE_ID)
        return False

    sell_pct = _clamp(_finite(exit_ev.get("sell_pct"), float(CFG.fallback_sell_pct)), 0.01, 1.0)
    scaled_amt = _scale_amount(int(amt), sell_pct)

    # Worker enqueue
    if CFG.allow_worker_enqueue:
        ok = await _enqueue_exit_request_best_effort(
            mint=mint,
            amount_raw=int(scaled_amt),
            reason=str(exit_ev.get("reason") or "forced_exit"),
            sell_pct=float(sell_pct),
            entry_spend_sol=float(exit_ev.get("spend_sol") or 0.0),
            follow_stats=dict(exit_ev.get("stats") or exit_ev.get("follow_stats") or {}),
            symbol=str(exit_ev.get("symbol") or ""),
            pool=str(exit_ev.get("pool") or ""),
            slippage_bps=int(exit_ev.get("slippage_bps") or int(CFG.fallback_slippage_bps)),
            priority=int(exit_ev.get("priority") or int(CFG.fallback_priority)),
            candidate_id=exit_ev.get("candidate_id"),
            exit_label=str(exit_ev.get("exit_label") or ""),
            source=str(exit_ev.get("source") or "exit_engine"),
            hard=int(bool(hard)),
            dryrun=int(bool(CFG.dryrun)),
            version=EXIT_ENGINE_VERSION,
            instance=INSTANCE_ID,
            quote_sanity=exit_ev.get("quote_sanity"),
        )
        if ok:
            _log("EXIT_DELEGATED_WORKER", mint=mint, amount_raw=int(scaled_amt), hard=int(hard), dryrun=int(CFG.dryrun), instance=INSTANCE_ID)
            return True
        _log("EXIT_WORKER_FAIL", level="warning", mint=mint, err="enqueue_exit_request_not_found_or_failed", instance=INSTANCE_ID)

    # Direct fallback
    if not CFG.allow_direct_fallback:
        return False

    if CFG.dryrun:
        _log("EXIT_DIRECT_FALLBACK_DRYRUN", mint=mint, amount_raw=int(scaled_amt), sell_pct=float(sell_pct), hard=int(hard), instance=INSTANCE_ID)
        return True

    if CFG.master_phase <= 2 and not _env_bool("EXIT_FORCE_DIRECT_IN_LOW_PHASE", False):
        _log("EXIT_DIRECT_BLOCK_LOW_PHASE", level="warning", mint=mint, phase=int(CFG.master_phase), instance=INSTANCE_ID)
        return False

    try:
        sig = await _direct_sell_best_effort(
            mint=mint,
            amount_raw=int(scaled_amt),
            slippage_bps=int(exit_ev.get("slippage_bps") or int(CFG.fallback_slippage_bps)),
        )
        if sig:
            _log("EXIT_DIRECT_FALLBACK_OK", mint=mint, amount_raw=int(scaled_amt), hard=int(hard), sig=str(sig)[:120], instance=INSTANCE_ID)
            return True
        _log("EXIT_DIRECT_NO_IMPL", level="warning", mint=mint, instance=INSTANCE_ID)
        return False
    except Exception as e:
        _log("EXIT_DIRECT_FALLBACK_FAIL", level="warning", mint=mint, err=_short_exc(e), instance=INSTANCE_ID)
        return False


# =============================================================================
# Rule evaluation
# =============================================================================
def _compute_metrics(entry: Optional[float], maxp: Optional[float], minp: Optional[float], last: Optional[float]) -> Dict[str, float]:
    out = {"pnl_pct": 0.0, "up_pct": 0.0, "dd_pct": 0.0, "peak_dd_pct": 0.0, "range_pct": 0.0}
    if entry and last:
        out["pnl_pct"] = (float(last) / float(entry) - 1.0) * 100.0
    if entry and maxp:
        out["up_pct"] = (float(maxp) / float(entry) - 1.0) * 100.0
    if entry and minp:
        out["dd_pct"] = (float(minp) / float(entry) - 1.0) * 100.0
    if maxp and last:
        out["peak_dd_pct"] = (float(last) / float(maxp) - 1.0) * 100.0
    if maxp and minp and minp > 0:
        out["range_pct"] = (float(maxp) / float(minp) - 1.0) * 100.0
    return out


def _is_hard_stop(reason: str) -> bool:
    return reason in {"stop_loss", "catastrophic_dd", "liq_collapse_hard", "smart_wallet_hard_risk"}


def _mint_allowed(mint: str, spend_sol: float, pool: str) -> bool:
    if not mint or mint == _SOL_MINT:
        return False
    ml = mint.lower()
    if CFG.follow_mint_whitelist and ml not in CFG.follow_mint_whitelist:
        return False
    if CFG.follow_mint_blacklist and ml in CFG.follow_mint_blacklist:
        return False
    if CFG.follow_pools and pool not in CFG.follow_pools:
        return False
    if spend_sol < CFG.follow_min_spend_sol:
        return False
    return True


# =============================================================================
# Public: request_exit (manual/programmatic)
# =============================================================================
async def request_exit(ev: Dict[str, Any]) -> bool:
    if not CFG.enabled:
        _log("EXIT_REQUEST_IGNORED_DISABLED", mint=ev.get("mint"), instance=INSTANCE_ID)
        return False

    mint = _extract_mint(ev) or _norm_mint(str(ev.get("mint") or ""))
    if not mint:
        _log("EXIT_REQUEST_BAD_EVENT", level="warning", keys=list(ev.keys())[:40], instance=INSTANCE_ID)
        return False

    amount_raw = _extract_amount_raw(ev)
    exit_ev = dict(ev)
    exit_ev.setdefault("mint", mint)
    exit_ev.setdefault("reason", "external_request")
    exit_ev.setdefault("exit_label", "manual_request")
    exit_ev.setdefault("source", "exit_engine.request_exit")
    exit_ev.setdefault("sell_pct", float(_clamp(_finite(exit_ev.get("sell_pct"), float(CFG.fallback_sell_pct)), 0.01, 1.0)))
    exit_ev.setdefault("slippage_bps", int(exit_ev.get("slippage_bps") or int(CFG.fallback_slippage_bps)))
    exit_ev.setdefault("priority", int(exit_ev.get("priority") or int(CFG.fallback_priority)))
    exit_ev.setdefault("ts", time.time())
    exit_ev.setdefault("version", EXIT_ENGINE_VERSION)
    exit_ev.setdefault("instance", INSTANCE_ID)
    exit_ev.setdefault("dryrun", int(bool(CFG.dryrun)))

    await _log_exit_event_safe({**exit_ev, "label": "exit_requested"})
    return await _delegate_force_exit(exit_ev, token_amount_raw=amount_raw, hard=False)


# =============================================================================
# BUY filled (primary)
# =============================================================================
async def on_buy_filled(ev: Dict[str, Any]) -> None:
    if not CFG.enabled:
        _log("EXIT_ENGINE_DISABLED", mint=ev.get("mint"), pool=ev.get("pool"), instance=INSTANCE_ID)
        return

    mint = _extract_mint(ev) or _norm_mint(str(ev.get("mint") or ""))
    tx_sig = _extract_signature(ev)
    if not mint or not tx_sig:
        _log("EXIT_HOOK_BAD_EVENT", level="warning", ev_keys=list(ev.keys())[:40], instance=INSTANCE_ID)
        return

    symbol = _extract_symbol(ev, mint)
    pool = _extract_pool(ev)
    spend_sol = float(_extract_spend_sol(ev) or 0.0)
    amount_raw = _extract_amount_raw(ev)

    entry_ts = None
    for k in ("entry_ts", "ts", "timestamp"):
        if ev.get(k) is None:
            continue
        with contextlib.suppress(Exception):
            entry_ts = float(ev[k])
            break

    entry_price: Optional[float] = None
    for key in ("price_entry_ui", "entry_price", "entryPrice", "priceUi", "price_ui", "price"):
        if ev.get(key) is None:
            continue
        with contextlib.suppress(Exception):
            f = float(ev[key])
            if f > 0:
                entry_price = f
                break

    liq_usd = _extract_liq_usd(ev)

    candidate_id = ev.get("candidate_id") or ev.get("entry_candidate_id") or ev.get("cand_id")

    _log(
        "EXIT_BUY_FILLED",
        mint=mint,
        symbol=symbol,
        pool=pool,
        spend_sol=float(spend_sol),
        amount_raw=amount_raw,
        tx_sig=tx_sig,
        dryrun=int(CFG.dryrun),
        candidate_id=candidate_id,
        phase=int(CFG.master_phase),
        version=EXIT_ENGINE_VERSION,
        instance=INSTANCE_ID,
    )

    # Track position (always if configured, otherwise only if follow enabled & in scope)
    if CFG.track_all_positions or (CFG.follow_enabled and _mint_allowed(mint, spend_sol, pool)):
        async with _OPEN_LOCK:
            prev = _OPEN.get(mint)
            if prev is None or getattr(prev, "entry_sig", "") != tx_sig:
                _OPEN[mint] = _Pos(
                    mint=mint,
                    symbol=symbol,
                    pool=pool,
                    entry_sig=tx_sig,
                    spend_sol=spend_sol,
                    amount_raw=amount_raw,
                    entry_price=entry_price,
                    candidate_id=candidate_id,
                    opened_from="on_buy_filled",
                    entry_ts=entry_ts,
                    entry_liq_usd=liq_usd,
                )

    # schedule AI sell brain (best-effort)
    schedule_fn = _sell_hook("schedule_after_buy")
    if schedule_fn is not None:
        try:
            payload = dict(ev)
            payload.update(
                {
                    "mint": mint,
                    "signature": tx_sig,
                    "ts": time.time(),
                    "pool": pool,
                    "symbol": symbol,
                    "spend_sol": spend_sol,
                    "amount_raw": amount_raw,
                    "candidate_id": candidate_id,
                    "dryrun": int(bool(CFG.dryrun)),
                    "exit_engine_version": EXIT_ENGINE_VERSION,
                    "instance": INSTANCE_ID,
                }
            )
            await _maybe_await(schedule_fn(payload))
            _log("EXIT_AI_SCHEDULED_AFTER_BUY", mint=mint, symbol=symbol, instance=INSTANCE_ID)
        except Exception as e:
            _log("EXIT_AI_SCHEDULE_AFTER_BUY_FAIL", level="warning", mint=mint, err=_short_exc(e), instance=INSTANCE_ID)


# =============================================================================
# Trade tick hook
# =============================================================================
async def on_trade_tick(raw_event: Dict[str, Any]) -> None:
    # If disabled, do nothing (optionally forward to AI for debug).
    if not CFG.enabled:
        if CFG.forward_ai_ticks_when_disabled:
            ai_tick = _sell_hook("on_trade_tick")
            if ai_tick is not None:
                with contextlib.suppress(Exception):
                    await _maybe_await(ai_tick(raw_event))
        return

    now = time.time()
    await _housekeeping(now)

    mint = _extract_mint(raw_event)

    # always forward to AI brain (best-effort)
    ai_tick = _sell_hook("on_trade_tick")
    if ai_tick is not None:
        with contextlib.suppress(Exception):
            await _maybe_await(ai_tick(raw_event))

    if not mint:
        return
    mint = _norm_mint(mint)
    if not mint or mint == _SOL_MINT:
        return

    price = _extract_price(raw_event)
    feed_counter = _extract_trade_counter(raw_event)
    liq_usd = _extract_liq_usd(raw_event)

    # best-effort: auto-track from position_store if not present
    async with _OPEN_LOCK:
        pos = _OPEN.get(mint)
    if pos is None and CFG.track_all_positions:
        ps = _position_store_get_position(mint)
        if isinstance(ps, dict):
            try:
                symbol = str(ps.get("symbol") or ps.get("sym") or mint[:4]).strip()
                pool = _pool_normalize(str(ps.get("pool") or ps.get("source") or "unknown"))
                spend_sol = _finite(ps.get("spend_sol") or ps.get("basis_sol") or 0.0, 0.0)
                amount_raw = ps.get("amount_raw")
                try:
                    amount_raw = int(amount_raw) if amount_raw is not None else None
                except Exception:
                    amount_raw = None
                entry_price = ps.get("entry_price") or ps.get("avg_entry_price") or ps.get("price_entry")
                try:
                    entry_price = float(entry_price) if entry_price is not None else None
                except Exception:
                    entry_price = None
                entry_ts = _finite(ps.get("entry_ts") or ps.get("opened_ts") or ps.get("ts") or 0.0, 0.0) or None
                async with _OPEN_LOCK:
                    if mint not in _OPEN:
                        _OPEN[mint] = _Pos(
                            mint=mint,
                            symbol=symbol,
                            pool=pool,
                            entry_sig=str(ps.get("entry_sig") or "store"),
                            spend_sol=float(spend_sol),
                            amount_raw=amount_raw,
                            entry_price=entry_price,
                            candidate_id=ps.get("candidate_id"),
                            opened_from="position_store",
                            entry_ts=entry_ts,
                            entry_liq_usd=ps.get("entry_liq_usd"),
                        )
            except Exception:
                pass

    # Update pos safely (NO awaits inside lock)
    needs_price_resolve = False
    snapshot: Optional[Dict[str, Any]] = None

    async with _OPEN_LOCK:
        pos = _OPEN.get(mint)
        if pos is None or pos.closed:
            return

        # throttle per mint
        if CFG.tick_throttle_ms > 0:
            if (now - pos.last_eval_ts) * 1000.0 < float(CFG.tick_throttle_ms):
                return
            pos.last_eval_ts = now

        pos.last_tick_ts = now

        age_sec = now - pos.entry_ts
        if age_sec > float(CFG.track_max_age_sec):
            _OPEN.pop(mint, None)
            return

        # trade tick count (delta-aware when counter exists)
        inc = 1
        if feed_counter is not None:
            if pos.last_feed_trade_counter is None:
                pos.last_feed_trade_counter = int(feed_counter)
                inc = 0
            else:
                delta = int(feed_counter) - int(pos.last_feed_trade_counter)
                pos.last_feed_trade_counter = int(feed_counter)
                inc = max(0, delta)

        pos.trades_after_entry += int(inc)

        if liq_usd is not None:
            pos.last_liq_usd = float(liq_usd)
            if pos.entry_liq_usd is None:
                pos.entry_liq_usd = float(liq_usd)

        if price is not None:
            p = float(price)
            pos.last_price = p
            if pos.entry_price is None:
                pos.entry_price = p
                pos.max_price = p
                pos.min_price = p
            else:
                if pos.max_price is None or p > pos.max_price:
                    pos.max_price = p
                if pos.min_price is None or p < pos.min_price:
                    pos.min_price = p
        else:
            if CFG.price_resolve_enabled:
                if (now - float(pos.last_price_resolve_ts)) >= float(CFG.price_resolve_min_interval_sec):
                    pos.last_price_resolve_ts = now
                    needs_price_resolve = True

        snapshot = {
            "mint": pos.mint,
            "symbol": pos.symbol,
            "pool": pos.pool,
            "entry_ts": pos.entry_ts,
            "entry_sig": pos.entry_sig,
            "spend_sol": float(pos.spend_sol),
            "amount_raw": pos.amount_raw,
            "candidate_id": pos.candidate_id,
            "opened_from": pos.opened_from,
            "entry_price": pos.entry_price,
            "max_price": pos.max_price,
            "min_price": pos.min_price,
            "last_price": pos.last_price,
            "entry_liq_usd": pos.entry_liq_usd,
            "last_liq_usd": pos.last_liq_usd,
            "trades_after_entry": int(pos.trades_after_entry),
            "age_sec": float(age_sec),
            "follow_done": bool(pos.follow_done),
            "follow_pass": bool(pos.follow_pass),
        }

    if snapshot is None:
        return

    # optional price resolve path (outside lock)
    if needs_price_resolve and (snapshot["entry_price"] is None or snapshot["last_price"] is None):
        rp = await _resolve_price_best_effort(mint)
        if rp:
            async with _OPEN_LOCK:
                pos2 = _OPEN.get(mint)
                if pos2 and not pos2.closed:
                    pos2.last_price = float(rp)
                    if pos2.entry_price is None:
                        pos2.entry_price = float(rp)
                        pos2.max_price = float(rp)
                        pos2.min_price = float(rp)
                    else:
                        if pos2.max_price is None or float(rp) > float(pos2.max_price):
                            pos2.max_price = float(rp)
                        if pos2.min_price is None or float(rp) < float(pos2.min_price):
                            pos2.min_price = float(rp)
                    snapshot["last_price"] = pos2.last_price
                    snapshot["entry_price"] = pos2.entry_price
                    snapshot["max_price"] = pos2.max_price
                    snapshot["min_price"] = pos2.min_price

    # If we still don't have price, we cannot enforce price-based exits safely.
    if CFG.follow_require_price and (snapshot["entry_price"] is None or snapshot["last_price"] is None):
        return

    entry = snapshot["entry_price"]
    lastp = snapshot["last_price"]
    maxp = snapshot["max_price"]
    minp = snapshot["min_price"]
    age_sec = float(snapshot["age_sec"])
    age_min = age_sec / 60.0
    trades = int(snapshot["trades_after_entry"])
    metrics = _compute_metrics(entry, maxp, minp, lastp)

    # ========== 1) HARD STOP-LOSS (always) ==========
    if entry and lastp and float(metrics["pnl_pct"]) <= -float(CFG.stop_loss_pct):
        reason = "stop_loss"
        await _force_exit_from_snapshot(snapshot, reason=reason, metrics=metrics, hard=True)
        return

    # ========== 2) Liquidity collapse (if liquidity data exists) ==========
    if float(CFG.min_liq_usd) > 0.0 and snapshot.get("last_liq_usd"):
        if float(snapshot["last_liq_usd"]) < float(CFG.min_liq_usd):
            await _force_exit_from_snapshot(snapshot, reason="liq_collapse_hard", metrics=metrics, hard=True)
            return

    if float(CFG.liq_collapse_pct) > 0.0 and snapshot.get("entry_liq_usd") and snapshot.get("last_liq_usd"):
        e = float(snapshot["entry_liq_usd"])
        c = float(snapshot["last_liq_usd"])
        if e > 0 and c > 0:
            drop_pct = (c / e - 1.0) * 100.0
            if drop_pct <= -float(CFG.liq_collapse_pct):
                await _force_exit_from_snapshot(snapshot, reason="liq_collapse", metrics={**metrics, "liq_drop_pct": drop_pct}, hard=False)
                return

    # ========== 3) Trailing stop (arm + trail) ==========
    if entry and maxp and lastp and float(CFG.trailing_arm_pct) > 0.0:
        max_gain = float(metrics["up_pct"])
        peak_dd = float(metrics["peak_dd_pct"])  # negative when below peak
        if max_gain >= float(CFG.trailing_arm_pct) and peak_dd <= -float(CFG.trailing_stop_pct):
            await _force_exit_from_snapshot(snapshot, reason="trailing_stop", metrics=metrics, hard=False)
            return

    # ========== 4) Time stop ==========
    if int(CFG.time_stop_min) > 0 and age_min >= float(CFG.time_stop_min):
        if float(metrics["pnl_pct"]) < float(CFG.time_stop_min_profit_pct):
            await _force_exit_from_snapshot(snapshot, reason="time_stop", metrics=metrics, hard=False)
            return

    # ========== 5) Stagnant exit ==========
    if int(CFG.stagnant_max_min) > 0 and age_min >= float(CFG.stagnant_max_min):
        if float(metrics["range_pct"]) < float(CFG.stagnant_min_move_pct):
            await _force_exit_from_snapshot(snapshot, reason="stagnant", metrics=metrics, hard=False)
            return

    # ========== 6) Dead follow-through (early) ==========
    if CFG.follow_enabled and (not snapshot.get("follow_done", False)):
        warmup_end = float(CFG.follow_window_sec + max(0, CFG.follow_grace_sec))
        if age_sec >= warmup_end:
            # catastrophic dump during warmup (optional)
            if float(CFG.follow_early_catastrophic_dd_pct) > 0.0 and float(metrics["dd_pct"]) <= -float(CFG.follow_early_catastrophic_dd_pct):
                await _force_exit_from_snapshot(snapshot, reason="catastrophic_dd", metrics=metrics, hard=True)
                return

            extend_end = warmup_end + float(max(0, CFG.follow_extend_low_trades_sec))
            if trades < int(CFG.follow_min_trades) and age_sec < extend_end:
                return

            trades_ok = trades >= int(CFG.follow_min_trades)
            up_ok = float(metrics["up_pct"]) >= float(CFG.follow_min_up_pct)
            dd_ok = float(metrics["dd_pct"]) >= -float(CFG.follow_max_dd_pct)
            pnl_ok = True
            if float(CFG.follow_min_pnl_pct) > 0.0:
                pnl_ok = float(metrics["pnl_pct"]) >= float(CFG.follow_min_pnl_pct)

            follow_pass = bool(trades_ok and up_ok and dd_ok and pnl_ok)

            async with _OPEN_LOCK:
                pos3 = _OPEN.get(mint)
                if pos3:
                    pos3.follow_done = True
                    pos3.follow_pass = bool(follow_pass)

            if follow_pass:
                await _log_exit_event_safe(
                    {
                        "label": "follow_ok",
                        "mint": snapshot["mint"],
                        "symbol": snapshot["symbol"],
                        "pool": snapshot["pool"],
                        "age_sec": age_sec,
                        "trades_after_entry": trades,
                        **metrics,
                        "spend_sol": float(snapshot.get("spend_sol") or 0.0),
                        "amount_raw": snapshot.get("amount_raw"),
                        "candidate_id": snapshot.get("candidate_id"),
                        "source": "exit_engine.follow_through",
                        "ts": time.time(),
                        "version": EXIT_ENGINE_VERSION,
                        "instance": INSTANCE_ID,
                        "dryrun": int(bool(CFG.dryrun)),
                    }
                )
                return

            await _force_exit_from_snapshot(snapshot, reason="follow_through_fail", metrics=metrics, hard=False)
            return


async def _force_exit_from_snapshot(snapshot: Dict[str, Any], *, reason: str, metrics: Dict[str, Any], hard: bool) -> None:
    mint = _norm_mint(str(snapshot.get("mint") or ""))
    if not mint:
        return

    # smart_wallets hard-risk (optional, can be triggered by follow logic elsewhere)
    smart_hint: Dict[str, Any] = {}
    if reason == "smart_wallet_hard_risk":
        smart_hint = dict(snapshot.get("smart_hint") or {})

    exit_ev: Dict[str, Any] = {
        "mint": mint,
        "symbol": str(snapshot.get("symbol") or mint[:4]),
        "pool": str(snapshot.get("pool") or "unknown"),
        "reason": reason,
        "exit_label": reason,
        "candidate_id": snapshot.get("candidate_id"),
        "source": "exit_engine.rules",
        "spend_sol": float(snapshot.get("spend_sol") or 0.0),
        "sell_pct": float(CFG.fallback_sell_pct),
        "slippage_bps": int(CFG.fallback_slippage_bps),
        "priority": int(CFG.fallback_priority),
        "stats": {
            "age_sec": float(snapshot.get("age_sec") or 0.0),
            "trades_after_entry": int(snapshot.get("trades_after_entry") or 0),
            **{k: _finite(v, 0.0) if isinstance(v, (int, float, str)) else v for k, v in dict(metrics).items()},
        },
        "smart_hint": smart_hint if smart_hint else None,
        "ts": time.time(),
        "version": EXIT_ENGINE_VERSION,
        "instance": INSTANCE_ID,
        "dryrun": int(bool(CFG.dryrun)),
    }

    await _log_exit_event_safe({**exit_ev, "label": f"exit_signal:{reason}", "hard": int(bool(hard))})

    # cap check (only for "auto exits")
    redis = await _maybe_await(_get_redis())
    if redis and int(CFG.max_daily_exits) > 0:
        daily = await _redis_get_daily(redis)
        if daily >= int(CFG.max_daily_exits):
            _log("EXIT_DAILY_CAP_REACHED", mint=mint, daily=daily, cap=int(CFG.max_daily_exits), reason=reason, instance=INSTANCE_ID)
            return

    accepted = await _delegate_force_exit(exit_ev, token_amount_raw=snapshot.get("amount_raw"), hard=bool(hard))
    if accepted and redis:
        await _redis_incr_daily(redis)

    # once we signal an exit, stop tracking to avoid double-sell
    async with _OPEN_LOCK:
        _OPEN.pop(mint, None)


# =============================================================================
# SELL filled hook (realized outcomes + reinvestor + guardrails)
# =============================================================================
async def on_sell_filled(ev: Dict[str, Any]) -> None:
    try:
        mint = _extract_mint(ev) or _norm_mint(str(ev.get("mint") or ""))
        symbol = str(ev.get("symbol") or (mint[:4] if mint else "")).strip()
        pool = _pool_normalize(str(ev.get("pool") or ev.get("source_pool") or "unknown"))

        # stop tracking if a sell happened externally
        if mint:
            async with _OPEN_LOCK:
                _OPEN.pop(mint, None)

        event = dict(ev)
        event.update(
            {
                "mint": mint,
                "symbol": symbol,
                "pool": pool,
                "ts": time.time(),
                "source": event.get("source") or "exit_engine.sell_filled",
                "version": EXIT_ENGINE_VERSION,
                "instance": INSTANCE_ID,
                "dryrun": int(bool(CFG.dryrun)),
            }
        )

        realized_pnl_sol: Optional[float] = None
        for k in ("realized_pnl_sol", "realizedPnLSol", "pnl_sol", "pnlSol"):
            if event.get(k) is None:
                continue
            with contextlib.suppress(Exception):
                realized_pnl_sol = float(event[k])
                break

        if realized_pnl_sol is None:
            received: Optional[float] = None
            for k in ("received_sol", "out_sol", "proceeds_sol", "sol_out", "amount_sol_out"):
                if event.get(k) is None:
                    continue
                with contextlib.suppress(Exception):
                    received = float(event[k])
                    break

            basis: Optional[float] = None
            for k in ("entry_spend_sol", "spend_sol", "spent_sol", "basis_sol"):
                if event.get(k) is None:
                    continue
                with contextlib.suppress(Exception):
                    basis = float(event[k])
                    break

            if received is not None and basis is not None:
                realized_pnl_sol = float(received - basis)

        if realized_pnl_sol is not None:
            event["realized_pnl_sol"] = float(realized_pnl_sol)

        await _log_exit_event_safe({**event, "label": event.get("label") or "sell_filled"})

        if realized_pnl_sol is not None and mint:
            await _guardrails_note_close(float(realized_pnl_sol), mint)

        if realized_pnl_sol is not None and realized_pnl_sol > 0:
            await _call_optional_hook(
                "utils_pkg.reinvestor",
                ("on_realized_profit", "on_realized_pnl", "on_sell_filled", "on_exit_filled", "handle_realized_profit"),
                {**event, "realized_pnl_sol": float(realized_pnl_sol)},
            )
    except Exception:
        return


# =============================================================================
# Legacy wrappers
# =============================================================================
async def on_buy_filled_event(ev: Dict[str, Any]) -> None:
    await on_buy_filled(ev)


async def on_buy_filled_legacy(raw_id_or_mint: str, tx_sig: str) -> None:
    ev: Dict[str, Any] = {"mint": raw_id_or_mint, "signature": tx_sig, "symbol": "", "spend_sol": 0.0, "pool": "legacy", "ts": time.time()}
    await on_buy_filled(ev)


# =============================================================================
# Diagnostics (SYNC SAFE)
# =============================================================================
def get_open_positions_snapshot() -> Dict[str, Dict[str, Any]]:
    return dict(_LAST_OPEN_SNAPSHOT)


# =============================================================================
# Reconciliation with position_store (remove closed positions, optional)
# =============================================================================
async def _reconcile_with_position_store(now: float) -> None:
    global _LAST_RECONCILE_TS
    if not (CFG.reconcile_with_position_store and CFG.track_all_positions):
        return
    if now - _LAST_RECONCILE_TS < float(max(1, CFG.reconcile_every_sec)):
        return
    _LAST_RECONCILE_TS = now

    snap = await _position_store_open_snapshot()
    if not isinstance(snap, dict):
        return

    # Accept either {"open": {...}} or direct mapping
    open_map = snap.get("open") if "open" in snap else snap
    if not isinstance(open_map, dict):
        return

    open_mints = {str(k) for k in open_map.keys()}
    if not open_mints:
        return

    async with _OPEN_LOCK:
        for mint in list(_OPEN.keys()):
            if mint not in open_mints:
                _OPEN.pop(mint, None)


# =============================================================================
# Worker loop (scripts/main.py supervisor compatibility)
# =============================================================================
async def run_worker(stop_evt: asyncio.Event) -> None:
    if _env_bool("EXIT_RELOAD_CFG_ON_START", True):
        with contextlib.suppress(Exception):
            reload_exit_engine_config()

    _log("EXIT_ENGINE_WORKER_START", version=EXIT_ENGINE_VERSION, phase=int(CFG.master_phase), enabled=int(CFG.enabled), instance=INSTANCE_ID)

    if not CFG.enabled:
        _log("EXIT_ENGINE_DISABLED_WAIT", phase=int(CFG.master_phase), kill=int(_env_bool("KILL_SWITCH", False)), instance=INSTANCE_ID)
        await stop_evt.wait()
        return

    try:
        while not stop_evt.is_set():
            now = time.time()
            await _housekeeping(now)
            await _reconcile_with_position_store(now)

            # periodic scan for time-based exits even if no fresh ticks
            if CFG.scan_positions_every_sec > 0:
                await _scan_positions_best_effort(now)

            log.every(  # type: ignore[attr-defined]
                "EXIT_ENGINE_HEARTBEAT",
                int(max(5, CFG.worker_heartbeat_sec)),
                enabled=int(CFG.enabled),
                dryrun=int(CFG.dryrun),
                phase=int(CFG.master_phase),
                open_count=len(_LAST_OPEN_SNAPSHOT),
                sl=float(CFG.stop_loss_pct),
                arm=float(CFG.trailing_arm_pct),
                tsl=float(CFG.trailing_stop_pct),
                time_stop_min=int(CFG.time_stop_min),
                stagnant_max_min=int(CFG.stagnant_max_min),
                liq_collapse=float(CFG.liq_collapse_pct),
                quote=int(CFG.quote_sanity_enabled),
                worker=int(CFG.allow_worker_enqueue),
                direct=int(CFG.allow_direct_fallback),
                version=EXIT_ENGINE_VERSION,
                instance=INSTANCE_ID,
            )

            try:
                await asyncio.wait_for(stop_evt.wait(), timeout=1.0)
            except asyncio.TimeoutError:
                pass

    except asyncio.CancelledError:
        raise
    finally:
        _log("EXIT_ENGINE_WORKER_STOP", version=EXIT_ENGINE_VERSION, instance=INSTANCE_ID)


async def _scan_positions_best_effort(now: float) -> None:
    """
    Safety net: evaluate time/stagnant rules even when tick stream is thin.
    We only act when we have prices. (We never guess.)
    """
    # throttle scan
    key = getattr(_scan_positions_best_effort, "_last", 0.0)  # type: ignore[attr-defined]
    if (now - float(key)) < float(CFG.scan_positions_every_sec):
        return
    setattr(_scan_positions_best_effort, "_last", now)  # type: ignore[attr-defined]

    await _refresh_snapshot_cache(now)
    for mint, snap in list(_LAST_OPEN_SNAPSHOT.items()):
        if not isinstance(snap, dict):
            continue
        if snap.get("entry_price") is None or snap.get("last_price") is None:
            continue
        # rebuild mini snapshot shape expected by _force_exit_from_snapshot callers
        snapshot = {
            "mint": mint,
            "symbol": snap.get("symbol"),
            "pool": snap.get("pool"),
            "entry_ts": now - float(snap.get("age_sec") or 0.0),
            "spend_sol": snap.get("spend_sol"),
            "amount_raw": snap.get("amount_raw"),
            "candidate_id": snap.get("candidate_id"),
            "opened_from": snap.get("opened_from"),
            "entry_price": snap.get("entry_price"),
            "max_price": snap.get("max_price"),
            "min_price": snap.get("min_price"),
            "last_price": snap.get("last_price"),
            "entry_liq_usd": snap.get("entry_liq_usd"),
            "last_liq_usd": snap.get("last_liq_usd"),
            "trades_after_entry": snap.get("trades_after_entry"),
            "age_sec": snap.get("age_sec"),
            "follow_done": bool(snap.get("follow_done")),
            "follow_pass": bool(snap.get("follow_pass")),
        }

        entry = snapshot["entry_price"]
        lastp = snapshot["last_price"]
        maxp = snapshot["max_price"]
        minp = snapshot["min_price"]
        metrics = _compute_metrics(entry, maxp, minp, lastp)

        age_sec = float(snapshot.get("age_sec") or 0.0)
        age_min = age_sec / 60.0

        # time stop / stagnant (scan-only; we do not run SL here because SL needs fresh)
        if int(CFG.time_stop_min) > 0 and age_min >= float(CFG.time_stop_min):
            if float(metrics["pnl_pct"]) < float(CFG.time_stop_min_profit_pct):
                await _force_exit_from_snapshot(snapshot, reason="time_stop", metrics=metrics, hard=False)
                continue

        if int(CFG.stagnant_max_min) > 0 and age_min >= float(CFG.stagnant_max_min):
            if float(metrics["range_pct"]) < float(CFG.stagnant_min_move_pct):
                await _force_exit_from_snapshot(snapshot, reason="stagnant", metrics=metrics, hard=False)
                continue


async def exit_engine_loop(stop_evt: asyncio.Event) -> None:
    await run_worker(stop_evt)


__all__ = [
    "on_buy_filled",
    "on_trade_tick",
    "on_buy_filled_event",
    "on_buy_filled_legacy",
    "on_sell_filled",
    "request_exit",
    "get_open_positions_snapshot",
    "reload_exit_engine_config",
    "run_worker",
    "exit_engine_loop",
]
