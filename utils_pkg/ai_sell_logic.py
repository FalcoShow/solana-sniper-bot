from __future__ import annotations

"""
utils_pkg/ai_sell_logic.py

V3+++ / QUANT-GRADE SELL INTELLIGENCE ENGINE (GOD-TIER LOCK-IN)
==============================================================

SELL brain: guardrail-first, best-effort, production-safe.

Called by:
  • utils_pkg.exit_engine.on_buy_filled  -> schedule_after_buy(payload)
  • utils_pkg.exit_engine.on_trade_tick  -> on_trade_tick(raw_event)
  • utils_pkg.exit_engine.follow-through -> schedule_force_exit(ev)
  • worker (optional) -> on_sell_filled(ev) / on_sell_failed(ev)

LOCKED SAFETY CONTRACT
----------------------
  • NEVER guess token amounts. If amount_raw unknown -> do not enqueue sells.
    - Allowed: best-effort resolve from wallet balance (optional, guarded, timed).
  • Hard exits ALWAYS win. AI can never override hard exits.
  • Best-effort only: never raise to callers.
  • Concurrency safe: per-mint locks + reservations prevent double-sells.
  • Router quote sanity is optional, OFF by default, and must never block forced exits
    unless explicitly enabled.

Public API (stable):
    compute_sell_decision(...)
    schedule_after_buy(ev)
    on_trade_tick(raw_event)
    schedule_force_exit(ev)
    force_exit(ev) / request_exit(ev) / exit_now(ev)
    reload_ai_sell_config()
    on_sell_filled(ev) / on_sell_failed(ev)
    register_position(...) / get_state(mint)

AI integration (LOCK-IN)
-----------------------
This module does NOT directly prompt OpenAI.
Instead it calls utils_pkg.ai_exit_model.predict_exit_score(...) (thin AI layer)
when enabled. If ai_exit_model is missing or disabled, logic falls back to rules.
"""

import asyncio
import contextlib
import hashlib
import inspect
import json
import math
import os
import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, cast


# =============================================================================
# Logging (logging_setup / log_compat compatible if present)
# =============================================================================
def _get_logger(name: str) -> Any:
    # Prefer your repo’s compat layer if it exists, then legacy logging_setup, then stdlib logging.
    with contextlib.suppress(Exception):
        from utils_pkg.log_compat import get_logger as _gl  # type: ignore

        return _gl(name)  # type: ignore[misc]

    with contextlib.suppress(Exception):
        from logging_setup import get_logger as _gl2  # type: ignore

        return _gl2(name)  # type: ignore[misc]

    import logging

    lg = logging.getLogger(name)
    if not lg.handlers:
        lg.addHandler(logging.NullHandler())
    return lg


log = _get_logger("sniper.ai_sell")


def _ensure_event_methods(logger: Any) -> None:
    if not hasattr(logger, "event"):

        def _event(name: str, **fields: Any) -> None:  # pragma: no cover
            with contextlib.suppress(Exception):
                logger.info("%s %s", name, fields)

        logger.event = _event  # type: ignore[attr-defined]

    if not hasattr(logger, "every"):

        def _every(key: str, n: int = 60, **fields: Any) -> None:  # pragma: no cover
            now = time.time()
            store = getattr(_every, "_store", {})  # type: ignore[attr-defined]
            last = float(store.get(key, 0.0))
            if now - last >= float(n):
                store[key] = now
                _every._store = store  # type: ignore[attr-defined]
                with contextlib.suppress(Exception):
                    logger.event(key, **fields)  # type: ignore[attr-defined]

        logger.every = _every  # type: ignore[attr-defined]


_ensure_event_methods(log)


def _log(name: str, level: str = "info", **fields: Any) -> None:
    """Structured log best-effort; never raises."""
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
# Lazy imports (avoid cycles)
# =============================================================================
_MOD_CACHE: dict[str, Any] = {}


def _get_mod(name: str) -> Any:
    if name in _MOD_CACHE:
        return _MOD_CACHE[name]
    try:
        import importlib

        m = importlib.import_module(name)
    except Exception:
        m = None
    _MOD_CACHE[name] = m
    return m


# =============================================================================
# Env + numeric safety helpers
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


def _clamp(x: float, lo: float, hi: float) -> float:
    return lo if x < lo else hi if x > hi else x


def _clip01(x: float) -> float:
    return _clamp(float(x), 0.0, 1.0)


def _smoothstep01(x: float) -> float:
    x = _clip01(x)
    return x * x * (3.0 - 2.0 * x)


def _finite(x: Any, d: float = 0.0) -> float:
    try:
        v = float(x)
        if not math.isfinite(v):
            return d
        return v
    except Exception:
        return d


def _safe_int(x: Any, d: int = 0) -> int:
    try:
        return int(x)
    except Exception:
        return d


def _safe_str(x: Any, d: str = "") -> str:
    try:
        return str(x).strip()
    except Exception:
        return d


async def _maybe_await(x: Any) -> Any:
    if inspect.isawaitable(x):
        return await x
    return x


def _short_exc(e: BaseException, n: int = 260) -> str:
    s = f"{type(e).__name__}: {e}"
    return (s[:n] + "…") if len(s) > n else s


# =============================================================================
# Version / instance / constants
# =============================================================================
AI_SELL_VERSION: str = "V3+++_LOCKIN_2026-01-06"
INSTANCE_ID: str = (
    os.getenv("INSTANCE_ID") or os.getenv("RENDER_INSTANCE_ID") or os.getenv("HOSTNAME") or ""
).strip() or "local"

_SOL_MINT = "So11111111111111111111111111111111111111112"


# =============================================================================
# Config (SellCfg)
# =============================================================================
@dataclass(frozen=True)
class SellCfg:
    # switches
    master_phase: int
    enabled: bool
    dryrun: bool
    debug: bool

    # pacing / memory
    state_ttl_sec: int
    eval_min_interval_ms: int
    tick_dedupe_window_ms: int
    tick_min_price_delta_pct: float
    exit_dedupe_sec: int
    max_mints_in_mem: int

    # sell score thresholds
    strong_score: float
    mid_score: float

    # guardrails
    min_hold_sec: float
    max_hold_sec: float
    hard_stop_loss_pct: float  # negative
    account_dd_protect_pct: float  # negative

    # take-profit tiers
    min_profit_pct: float
    tp1_pct: float
    tp2_pct: float
    tp3_pct: float
    tp1_sell_pct: float
    tp2_sell_pct: float
    tp3_sell_pct: float

    # giveback protection
    giveback_soft_pct: float
    giveback_hard_pct: float
    giveback_soft_sell_pct: float

    # stagnation exits
    stagnation_min: float
    stagnation_min_move_pct: float
    stagnation_sell_pct: float

    # risk exits
    fraud_exit_pct: float  # 0..100
    whale_exit_pressure: float  # 0..1
    liquidity_floor_usd: float

    # AI blend
    ai_enabled: bool
    ai_weight: float
    ai_weight_loss_mult: float
    ai_timeout_sec: float

    # quote sanity
    require_quote_sanity: bool
    quote_timeout_sec: float
    quote_max_price_impact_bps: int
    quote_min_out_sol: float
    require_quote_sanity_for_force_exit: bool

    # worker defaults
    default_slippage_bps: int
    default_priority: int

    # amount resolution
    resolve_amount_on_force_exit: bool
    resolve_amount_timeout_s: float
    resolve_amount_on_tick_when_unknown: bool

    # reservations / circuit breakers
    reserve_ttl_sec: int
    failure_freeze_sec: int
    max_enqueue_failures: int

    # telemetry
    publish_state_to_redis: bool
    state_key_prefix: str
    publish_state_shadow_key: bool


def _load_cfg() -> SellCfg:
    master = max(0, min(12, _env_int("MASTER_PHASE", _env_int("STRATEGY_PHASE", 7))))

    dryrun = (
        _env_bool("AI_SELL_DRYRUN", False)
        or _env_bool("DRYRUN", False)
        or _env_bool("DRY_RUN", False)
        or _env_bool("PAPER_TRADING", False)
    )

    enabled = _env_bool("AI_SELL_ENABLED", master >= 2) and (not _env_bool("KILL_SWITCH", False))

    min_profit = _env_float("EXIT_MIN_PROFIT_PCT", 2.0)
    tp1 = _env_float("EXIT_TP1_PCT", 4.0)
    tp2 = _env_float("EXIT_TP2_PCT", 8.0)
    tp3 = _env_float("EXIT_TP3_PCT", 15.0)

    sl_pct = abs(_env_float("EXIT_STOP_LOSS_PCT", 10.0))
    hard_sl = -abs(float(sl_pct))

    min_hold = _env_float("EXIT_MIN_HOLD_SEC", _env_float("EXIT_MIN_HOLD", 10.0))
    max_hold = _env_float("EXIT_MAX_HOLD_SEC", _env_float("EXIT_MAX_HOLD", 7200.0))

    gb_soft = _env_float("AI_SELL_MAX_GIVEBACK_SOFT", 35.0)
    gb_hard = _env_float("AI_SELL_MAX_GIVEBACK_HARD", 55.0)

    default_slip = _env_int("AI_SELL_SLIPPAGE_BPS", _env_int("PF_SLIPPAGE_BPS", 200))
    default_slip = max(5, min(5000, int(default_slip)))

    return SellCfg(
        master_phase=master,
        enabled=enabled,
        dryrun=dryrun,
        debug=_env_bool("AI_SELL_DEBUG", False),
        state_ttl_sec=max(60, _env_int("AI_SELL_STATE_TTL_SEC", 7200)),
        eval_min_interval_ms=max(0, _env_int("AI_SELL_EVAL_MIN_INTERVAL_MS", 450)),
        tick_dedupe_window_ms=max(0, _env_int("AI_SELL_TICK_DEDUPE_WINDOW_MS", 150)),
        tick_min_price_delta_pct=max(0.0, _env_float("AI_SELL_TICK_MIN_PRICE_DELTA_PCT", 0.0)),
        exit_dedupe_sec=max(1, _env_int("AI_SELL_EXIT_DEDUPE_SEC", 6)),
        max_mints_in_mem=max(200, _env_int("AI_SELL_MAX_MINTS_IN_MEM", 5000)),
        strong_score=_clip01(_env_float("AI_SELL_STRONG_SCORE", 0.75)),
        mid_score=_clip01(_env_float("AI_SELL_MID_SCORE", 0.55)),
        min_hold_sec=max(0.0, float(min_hold)),
        max_hold_sec=max(30.0, float(max_hold)),
        hard_stop_loss_pct=float(hard_sl),
        account_dd_protect_pct=-abs(_env_float("AI_SELL_ACCOUNT_DD_PROTECT_PCT", 20.0)),
        min_profit_pct=float(min_profit),
        tp1_pct=float(tp1),
        tp2_pct=float(tp2),
        tp3_pct=float(tp3),
        tp1_sell_pct=_clamp(_env_float("AI_SELL_TP1_SELL_PCT", 0.15), 0.01, 1.0),
        tp2_sell_pct=_clamp(_env_float("AI_SELL_TP2_SELL_PCT", 0.20), 0.01, 1.0),
        tp3_sell_pct=_clamp(_env_float("AI_SELL_TP3_SELL_PCT", 0.30), 0.01, 1.0),
        giveback_soft_pct=float(gb_soft),
        giveback_hard_pct=float(gb_hard),
        giveback_soft_sell_pct=_clamp(
            _env_float("AI_SELL_GIVEBACK_SOFT_SELL_PCT", 0.50), 0.01, 1.0
        ),
        stagnation_min=max(
            0.0, _env_float("EXIT_STAGNANT_MAX_MIN", _env_float("AI_SELL_STAGNATION_MIN", 8.0))
        ),
        stagnation_min_move_pct=max(
            0.0,
            _env_float(
                "EXIT_STAGNANT_MIN_MOVE_PCT", _env_float("AI_SELL_STAGNATION_MIN_MOVE_PCT", 5.0)
            ),
        ),
        stagnation_sell_pct=_clamp(_env_float("AI_SELL_STAGNATION_SELL_PCT", 1.0), 0.01, 1.0),
        fraud_exit_pct=_clamp(_env_float("AI_SELL_FRAUD_EXIT_PCT", 60.0), 0.0, 100.0),
        whale_exit_pressure=_clip01(_env_float("AI_SELL_WHALE_EXIT_PRESSURE", 0.80)),
        liquidity_floor_usd=max(0.0, _env_float("AI_SELL_LIQ_FLOOR_USD", 8000.0)),
        ai_enabled=_env_bool("AI_SELL_AI_ENABLED", True),
        ai_weight=_clip01(_env_float("AI_SELL_AI_WEIGHT", 0.60)),
        ai_weight_loss_mult=_clamp(_env_float("AI_SELL_AI_WEIGHT_LOSS_MULT", 0.35), 0.0, 1.0),
        ai_timeout_sec=max(
            0.5, _env_float("AI_SELL_AI_TIMEOUT_SEC", _env_float("AI_EXIT_MODEL_TIMEOUT_S", 1.8))
        ),
        require_quote_sanity=_env_bool("AI_SELL_REQUIRE_QUOTE_SANITY", False),
        quote_timeout_sec=max(0.2, _env_float("AI_SELL_QUOTE_TIMEOUT_SEC", 1.25)),
        quote_max_price_impact_bps=max(0, _env_int("AI_SELL_QUOTE_MAX_PRICE_IMPACT_BPS", 1800)),
        quote_min_out_sol=max(0.0, _env_float("AI_SELL_QUOTE_MIN_OUT_SOL", 0.0)),
        require_quote_sanity_for_force_exit=_env_bool(
            "AI_SELL_REQUIRE_QUOTE_SANITY_FOR_FORCE_EXIT", False
        ),
        default_slippage_bps=int(default_slip),
        default_priority=max(0, _env_int("AI_SELL_PRIORITY", 1)),
        resolve_amount_on_force_exit=_env_bool("AI_SELL_RESOLVE_AMOUNT_ON_FORCE_EXIT", True),
        resolve_amount_timeout_s=max(0.25, _env_float("AI_SELL_RESOLVE_AMOUNT_TIMEOUT_S", 2.0)),
        resolve_amount_on_tick_when_unknown=_env_bool(
            "AI_SELL_RESOLVE_AMOUNT_ON_TICK_WHEN_UNKNOWN", False
        ),
        reserve_ttl_sec=max(5, _env_int("AI_SELL_RESERVE_TTL_SEC", 180)),
        failure_freeze_sec=max(0, _env_int("AI_SELL_FAILURE_FREEZE_SEC", 30)),
        max_enqueue_failures=max(1, _env_int("AI_SELL_MAX_ENQUEUE_FAILURES", 6)),
        publish_state_to_redis=_env_bool("AI_SELL_PUBLISH_STATE_TO_REDIS", True),
        state_key_prefix=_env_str("AI_SELL_STATE_KEY_PREFIX", "ai_sell") or "ai_sell",
        publish_state_shadow_key=_env_bool("AI_SELL_PUBLISH_SHADOW_KEY", True),
    )


CFG: SellCfg = _load_cfg()


def reload_ai_sell_config() -> None:
    global CFG
    CFG = _load_cfg()
    _log(
        "AI_SELL_CFG_RELOADED",
        version=AI_SELL_VERSION,
        instance=INSTANCE_ID,
        phase=int(CFG.master_phase),
        enabled=int(CFG.enabled),
        dryrun=int(CFG.dryrun),
        ai=int(CFG.ai_enabled),
        quote=int(CFG.require_quote_sanity),
        eval_ms=int(CFG.eval_min_interval_ms),
        dedupe_sec=int(CFG.exit_dedupe_sec),
        slippage_bps=int(CFG.default_slippage_bps),
        priority=int(CFG.default_priority),
        reserve_ttl=int(CFG.reserve_ttl_sec),
    )


# =============================================================================
# State store: Redis preferred + memory fallback
# =============================================================================
_MEM: dict[str, dict[str, Any]] = {}
_MEM_LOCK = asyncio.Lock()

_MINT_LOCKS: dict[str, asyncio.Lock] = {}
_MINT_LOCKS_GUARD = asyncio.Lock()


def _state_key(mint: str) -> str:
    return f"{CFG.state_key_prefix}:{mint}"


def _shadow_key(mint: str) -> str:
    return f"{CFG.state_key_prefix}:state:{mint}"


async def _mint_lock(mint: str) -> asyncio.Lock:
    mint = (mint or "").strip()
    if not mint:
        return asyncio.Lock()
    async with _MINT_LOCKS_GUARD:
        lk = _MINT_LOCKS.get(mint)
        if lk is None:
            lk = asyncio.Lock()
            _MINT_LOCKS[mint] = lk
        # keep bounded
        if len(_MINT_LOCKS) > 8000:
            for k in list(_MINT_LOCKS.keys())[:1500]:
                if k != mint:
                    _MINT_LOCKS.pop(k, None)
        return lk


async def _get_redis() -> Any:
    """
    Best-effort redis getter:
      - utils_pkg.core_utils.get_redis_async()
      - utils_pkg.core_utils.get_redis()
      - utils_pkg.core_utils.redis
    Must return None if not available (never throw).
    """
    cu = _get_mod("utils_pkg.core_utils")
    if not cu:
        return None

    for nm in ("get_redis_async", "get_redis"):
        fn = getattr(cu, nm, None)
        if callable(fn):
            with contextlib.suppress(Exception):
                r = fn()
                return await _maybe_await(r)

    with contextlib.suppress(Exception):
        return getattr(cu, "redis", None)
    return None


async def _redis_get(r: Any, key: str) -> str | None:
    if r is None:
        return None
    try:
        raw = await _maybe_await(r.get(key))
    except Exception:
        return None
    if raw is None:
        return None
    if isinstance(raw, (bytes, bytearray)):
        with contextlib.suppress(Exception):
            return raw.decode("utf-8", "ignore")
        return None
    with contextlib.suppress(Exception):
        return str(raw)
    return None


async def _redis_del(r: Any, key: str) -> None:
    if r is None:
        return
    with contextlib.suppress(Exception):
        await _maybe_await(r.delete(key))


async def _redis_setex(r: Any, key: str, value: str, ttl: int) -> None:
    if r is None:
        return
    ttl = int(max(1, ttl))
    # Try common APIs; never throw.
    with contextlib.suppress(Exception):
        await _maybe_await(r.set(key, value, ex=ttl))
        return
    with contextlib.suppress(Exception):
        await _maybe_await(r.setex(key, ttl, value))
        return
    with contextlib.suppress(Exception):
        await _maybe_await(r.set(key, value))
        return


def _prune_reservations(st: dict[str, Any], now: float) -> None:
    res = st.get("reservations")
    if not isinstance(res, list) or not res:
        st["reservations"] = []
        st["reserved_raw"] = int(st.get("reserved_raw") or 0)
        return

    ttl = float(CFG.reserve_ttl_sec)
    keep: list[dict[str, Any]] = []
    for item in res:
        if not isinstance(item, dict):
            continue
        ts = _finite(item.get("ts"), 0.0)
        amt = _safe_int(item.get("amt"), 0)
        if amt <= 0 or ts <= 0:
            continue
        if (now - ts) > ttl:
            continue
        keep.append({"id": _safe_str(item.get("id"))[:32], "ts": float(ts), "amt": int(amt)})

    st["reservations"] = keep
    st["reserved_raw"] = int(sum(int(x.get("amt") or 0) for x in keep))


async def _load_state(mint: str) -> dict[str, Any]:
    mint = (mint or "").strip()
    if not mint:
        return {}

    r = await _get_redis()
    if r is not None:
        raw = await _redis_get(r, _state_key(mint))
        if raw:
            with contextlib.suppress(Exception):
                obj = json.loads(raw)
                if isinstance(obj, dict):
                    return obj

    async with _MEM_LOCK:
        st = _MEM.get(mint)
        if not st:
            return {}
        exp = float(st.get("_expires_ts", 0.0))
        if exp and time.time() > exp:
            _MEM.pop(mint, None)
            return {}
        return dict(st)


async def _save_state(mint: str, state: dict[str, Any]) -> None:
    mint = (mint or "").strip()
    if not mint:
        return

    now = time.time()
    st = dict(state)
    st["_updated_ts"] = now
    st.setdefault("_version", AI_SELL_VERSION)
    st.setdefault("_instance", INSTANCE_ID)

    _prune_reservations(st, now)

    async with _MEM_LOCK:
        if len(_MEM) > CFG.max_mints_in_mem:
            keys = list(_MEM.keys())
            keys.sort(key=lambda k: float(_MEM.get(k, {}).get("_updated_ts", 0.0)))
            for k in keys[: max(1, int(0.10 * len(keys)))]:
                _MEM.pop(k, None)
        st["_expires_ts"] = now + float(CFG.state_ttl_sec)
        _MEM[mint] = dict(st)

    r = await _get_redis()
    if r is not None and CFG.publish_state_to_redis:
        payload = json.dumps(st, separators=(",", ":"), ensure_ascii=False)
        await _redis_setex(r, _state_key(mint), payload, int(CFG.state_ttl_sec))
        if CFG.publish_state_shadow_key:
            await _redis_setex(r, _shadow_key(mint), payload, int(CFG.state_ttl_sec))


async def _delete_state(mint: str) -> None:
    mint = (mint or "").strip()
    if not mint:
        return
    async with _MEM_LOCK:
        _MEM.pop(mint, None)
    r = await _get_redis()
    if r is not None:
        await _redis_del(r, _state_key(mint))
        if CFG.publish_state_shadow_key:
            await _redis_del(r, _shadow_key(mint))


# =============================================================================
# Event extraction helpers
# =============================================================================
def _extract_mint(ev: dict[str, Any]) -> str | None:
    for k in (
        "mint",
        "mintAddress",
        "mint_address",
        "tokenMint",
        "baseMint",
        "id",
        "address",
        "token",
    ):
        v = ev.get(k)
        if isinstance(v, str) and v.strip():
            return v.strip()

    with contextlib.suppress(Exception):
        for kk in ("pair", "token", "base", "asset"):
            o = ev.get(kk)
            if isinstance(o, dict):
                for k2 in ("mint", "address", "id"):
                    v2 = o.get(k2)
                    if isinstance(v2, str) and v2.strip():
                        return v2.strip()
    return None


def _extract_symbol(ev: dict[str, Any], mint: str) -> str:
    for k in ("symbol", "sym", "ticker"):
        v = ev.get(k)
        if isinstance(v, str) and v.strip():
            return v.strip()
    return (mint[:4] if mint else "").strip()


def _extract_pool(ev: dict[str, Any]) -> str:
    for k in ("pool", "source", "venue", "dex", "market"):
        v = ev.get(k)
        if isinstance(v, str) and v.strip():
            return v.strip()
    return "unknown"


def _extract_price(ev: dict[str, Any]) -> float | None:
    for k in (
        "priceUi",
        "price_ui",
        "price",
        "priceInSol",
        "price_in_sol",
        "ui_price",
        "last_price",
        "lastPrice",
        "close",
        "mid",
        "mark",
    ):
        v = ev.get(k)
        if v is None:
            continue
        with contextlib.suppress(Exception):
            f = float(v)
            if f > 0:
                return f
    with contextlib.suppress(Exception):
        t = ev.get("tick")
        if isinstance(t, dict):
            return _extract_price(t)
    return None


def _extract_amount_raw(ev: dict[str, Any]) -> int | None:
    for k in (
        "amount_raw",
        "amount_out",
        "base_amount_raw",
        "token_amount_raw",
        "out_amount_raw",
        "position_raw",
        "size_raw",
        "amountRaw",
        "filled_amount_raw",
        "filled_raw",
        "amount_in_raw",
        "amountSoldRaw",
    ):
        v = ev.get(k)
        if v is None:
            continue
        with contextlib.suppress(Exception):
            i = int(v)
            if i > 0:
                return i
    return None


def _extract_entry_price(ev: dict[str, Any]) -> float | None:
    for k in (
        "entry_price",
        "entryPrice",
        "price_entry_ui",
        "entry_price_ui",
        "fill_price_ui",
        "avg_price_ui",
        "priceUi",
    ):
        v = ev.get(k)
        if v is None:
            continue
        with contextlib.suppress(Exception):
            f = float(v)
            if f > 0:
                return f
    return None


# =============================================================================
# Optional hooks: risk_manager / ai_risk_model / self-learning / logging sinks
# =============================================================================
async def _risk_ctx() -> dict[str, Any]:
    rm = _get_mod("utils_pkg.risk_manager")
    if not rm:
        return {}
    for nm in ("risk_manager", "RISK", "RISK_MANAGER"):
        obj = getattr(rm, nm, None)
        if obj is not None:
            rm = obj
            break
    for name in ("get_sell_ctx", "get_ai_ctx", "get_ctx"):
        fn = getattr(rm, name, None)
        if callable(fn):
            with contextlib.suppress(Exception):
                out = await _maybe_await(fn())
                return out if isinstance(out, dict) else {}
    return {}


async def _kill_switch_active() -> bool:
    if _env_bool("KILL_SWITCH", False):
        return True
    rm = _get_mod("utils_pkg.risk_manager")
    if not rm:
        return False
    for nm in ("risk_manager", "RISK", "RISK_MANAGER"):
        obj = getattr(rm, nm, None)
        if obj is not None:
            rm = obj
            break
    for fnn in ("should_stop_trading", "stop_trading", "is_killed", "is_halted"):
        fn = getattr(rm, fnn, None)
        if callable(fn):
            with contextlib.suppress(Exception):
                val = await _maybe_await(fn())
                if isinstance(val, bool):
                    return val
    return False


def _ai_risk_patch(features: dict[str, Any], trade: dict[str, Any]) -> dict[str, Any]:
    arm = _get_mod("utils_pkg.ai_risk_model")
    if not arm:
        return features
    for fnn in ("score_exit_risk", "score_risk", "score"):
        fn = getattr(arm, fnn, None)
        if callable(fn):
            with contextlib.suppress(Exception):
                out = fn({"features": features, "trade": trade, "mode": "sell"})
                if isinstance(out, dict):
                    merged = dict(features)
                    for k in (
                        "fraud_risk_pct",
                        "whale_exit_pressure",
                        "regime_exit_bias",
                        "exit_quality_score",
                        "rug_prob_pct",
                        "rug_prob",
                    ):
                        if k in out and out[k] is not None:
                            merged[k] = out[k]
                    return merged
    return features


def _self_learning_overrides(
    mint: str, base_cfg: dict[str, Any], state: dict[str, Any], features: dict[str, Any]
) -> dict[str, Any]:
    sl = _get_mod("utils_pkg.self_learning_ai_strategy")
    if not sl:
        return base_cfg
    for nm in ("get_live_sell_overrides", "get_live_overrides", "get_live_config"):
        fn = getattr(sl, nm, None)
        if callable(fn):
            with contextlib.suppress(Exception):
                out = fn({"mint": mint, "state": state, "features": features, "base_cfg": base_cfg})
                if isinstance(out, dict):
                    merged = dict(base_cfg)
                    merged.update(out)
                    return merged
    return base_cfg


def _self_learning_refine(
    mint: str,
    decision: dict[str, Any],
    *,
    state: dict[str, Any],
    features: dict[str, Any],
    trade: dict[str, Any],
    risk_ctx: dict[str, Any],
    strat_ctx: dict[str, Any],
    ai_score: float | None,
    ai_meta: dict | None,
) -> dict[str, Any]:
    sl = _get_mod("utils_pkg.self_learning_ai_strategy")
    if not sl:
        return decision
    fn = getattr(sl, "refine_sell_decision", None)
    if not callable(fn):
        return decision
    with contextlib.suppress(Exception):
        out = fn(
            {
                "mint": mint,
                "state": state,
                "features": features,
                "trade": trade,
                "risk_ctx": risk_ctx,
                "strat_ctx": strat_ctx,
                "decision": decision,
                "ai_score": ai_score,
                "ai_meta": ai_meta,
            }
        )
        return out if isinstance(out, dict) else decision
    return decision


async def _log_sell_signal(payload: dict[str, Any]) -> None:
    dl = _get_mod("utils_pkg.data_logger")
    if dl:
        for nm in ("log_sell_signal", "log_exit_signal", "log_signal", "log_event"):
            fn = getattr(dl, nm, None)
            if callable(fn):
                with contextlib.suppress(Exception):
                    await _maybe_await(fn(payload))
                break

    adb = _get_mod("utils_pkg.ai_db")
    if adb:
        for nm in ("log_event", "insert_event", "write_event", "log_sell_signal"):
            fn = getattr(adb, nm, None)
            if callable(fn):
                with contextlib.suppress(Exception):
                    await _maybe_await(fn({"type": "sell_signal", **payload}))
                break


async def _log_sell_enqueue(payload: dict[str, Any]) -> None:
    dl = _get_mod("utils_pkg.data_logger")
    if dl:
        for nm in ("log_sell_enqueue", "log_exit_enqueue", "log_event"):
            fn = getattr(dl, nm, None)
            if callable(fn):
                with contextlib.suppress(Exception):
                    await _maybe_await(fn(payload))
                break

    adb = _get_mod("utils_pkg.ai_db")
    if adb:
        for nm in ("log_event", "insert_event", "write_event"):
            fn = getattr(adb, nm, None)
            if callable(fn):
                with contextlib.suppress(Exception):
                    await _maybe_await(fn({"type": "sell_enqueue", **payload}))
                break


# =============================================================================
# Feature building (best-effort)
# =============================================================================
async def _build_features(
    mint: str, raw_event: dict[str, Any], state: dict[str, Any]
) -> dict[str, Any]:
    for k in ("features", "fb", "feat"):
        v = raw_event.get(k)
        if isinstance(v, dict) and v:
            out = dict(v)
            out.setdefault("mint", mint)
            return out

    fb = _get_mod("utils_pkg.feature_builder")
    if fb:
        for fn_name in ("build_features_for_sell", "build_features_for_mint", "build_features"):
            fn = getattr(fb, fn_name, None)
            if callable(fn):
                payload = {
                    "mint": mint,
                    "tick": raw_event,
                    "state": state,
                    "mode": "sell",
                    "ts": time.time(),
                }
                with contextlib.suppress(Exception):
                    res = await _maybe_await(fn(payload))
                    if isinstance(res, dict) and res:
                        out = dict(res)
                        out.setdefault("mint", mint)
                        return out

    out = dict(raw_event)
    out.setdefault("mint", mint)
    return out


# =============================================================================
# AI exit model call (preferred)
# =============================================================================
def _filter_kwargs(fn: Any, kwargs: dict[str, Any]) -> dict[str, Any]:
    try:
        sig = inspect.signature(fn)
        allowed = set(sig.parameters.keys())
        return {k: v for k, v in kwargs.items() if k in allowed}
    except Exception:
        return kwargs


async def _ai_exit_score_from_model(
    *, features: dict[str, Any], mint: str
) -> tuple[float | None, dict | None, str]:
    """
    Calls utils_pkg.ai_exit_model.predict_exit_score(...) if available.

    Returns:
      (score_0_1 or None, meta or None, reason_str)
    """
    if not CFG.ai_enabled:
        return None, None, "ai_disabled"

    m = _get_mod("utils_pkg.ai_exit_model")
    if not m:
        return None, None, "ai_exit_model_missing"

    fn = getattr(m, "predict_exit_score", None)
    if not callable(fn):
        return None, None, "ai_exit_fn_missing"

    try:
        # Prefer kwargs; tolerate signature drift.
        kwargs = _filter_kwargs(fn, {"features": features, "mint": mint})
        coro = fn(**kwargs)
        out = await asyncio.wait_for(_maybe_await(coro), timeout=float(CFG.ai_timeout_sec))

        if isinstance(out, tuple) and len(out) == 2:
            score, meta = out
            with contextlib.suppress(Exception):
                sc = _clip01(float(score))
                return sc, (meta if isinstance(meta, dict) else None), "ai_exit_model_ok"
            return None, (meta if isinstance(meta, dict) else None), "ai_exit_model_bad_score"

        return None, None, "ai_exit_model_bad_return"
    except Exception as e:
        if CFG.debug:
            _log("AI_SELL_AI_EXIT_FAIL", level="warning", mint=mint, err=_short_exc(e))
        return None, None, "ai_exit_model_exception"


# =============================================================================
# Router quote sanity (optional)
# =============================================================================
async def _quote_sanity_ok(*, mint: str, amount_raw: int, force: bool = False) -> tuple[bool, str]:
    """
    If disabled -> always OK.

    IMPORTANT:
      - Tick-driven exits: can be used as a gate if enabled.
      - Forced exits: MUST NOT block unless explicitly enabled.
    """
    if CFG.dryrun:
        return True, "quote_skipped_dryrun"

    if force:
        if not CFG.require_quote_sanity_for_force_exit:
            return True, "quote_force_disabled"
    else:
        if not CFG.require_quote_sanity:
            return True, "quote_disabled"

    if not mint or mint == _SOL_MINT or int(amount_raw) <= 0:
        return False, "bad_mint_or_amount"

    router = _get_mod("utils_pkg.router")
    if not router:
        return True, "router_missing_allow"

    fns: list[Callable[..., Any]] = []
    for nm in ("get_sell_quote", "sell_quote", "get_quote"):
        cand = getattr(router, nm, None)
        if callable(cand):
            fns.append(cast(Callable[..., Any], cand))

    if not fns:
        return True, "quote_fn_missing_allow"

    fn = fns[0]
    kwargs = _filter_kwargs(
        fn,
        {
            "mint": mint,
            "amount_raw": int(amount_raw),
            "slippage_bps": int(CFG.default_slippage_bps),
        },
    )

    try:
        out = fn(**kwargs)
        out = await asyncio.wait_for(_maybe_await(out), timeout=float(CFG.quote_timeout_sec))

        if isinstance(out, dict):
            pib = _safe_int(out.get("priceImpactBps") or out.get("price_impact_bps") or 0, 0)
            if int(CFG.quote_max_price_impact_bps) > 0 and pib > int(
                CFG.quote_max_price_impact_bps
            ):
                return False, f"price_impact_bps_too_high({pib})"

            out_sol = _finite(
                out.get("out_sol") or out.get("outAmountSol") or out.get("out_amount_sol"), 0.0
            )
            if (
                float(CFG.quote_min_out_sol) > 0
                and out_sol > 0
                and out_sol < float(CFG.quote_min_out_sol)
            ):
                return False, f"out_sol_too_low({out_sol:.6f})"

        return True, "quote_ok"
    except Exception as e:
        if CFG.debug:
            _log("AI_SELL_QUOTE_FAIL", level="warning", mint=mint, err=_short_exc(e))
        # Never block forced exits on quote failure; for tick exits, prefer allow-over-block too.
        return True, "quote_fail_allow"


# =============================================================================
# Worker enqueue
# =============================================================================
async def _enqueue_exit_request(**kwargs: Any) -> tuple[bool, str]:
    ew = _get_mod("utils_pkg.auto_exit_worker.exit_engine")
    if not ew:
        ew = _get_mod("utils_pkg.auto_exit_worker")
    fn = getattr(ew, "enqueue_exit_request", None) if ew else None
    if not callable(fn):
        return False, "enqueue_fn_missing"

    try:
        payload = _filter_kwargs(fn, dict(kwargs))
        await _maybe_await(fn(**payload))
        return True, "enqueued"
    except Exception as e:
        return False, _short_exc(e)


# =============================================================================
# Strategy cfg base (env-compatible)
# =============================================================================
def _base_sell_cfg_from_env() -> dict[str, Any]:
    # Intentionally small stable contract; self-learning can override.
    return {
        "MIN_PROFIT_PCT": _env_float("EXIT_MIN_PROFIT_PCT", CFG.min_profit_pct),
        "TP1_PCT": _env_float("EXIT_TP1_PCT", CFG.tp1_pct),
        "TP2_PCT": _env_float("EXIT_TP2_PCT", CFG.tp2_pct),
        "TP3_PCT": _env_float("EXIT_TP3_PCT", CFG.tp3_pct),
        "STOP_LOSS_PCT": _env_float("EXIT_STOP_LOSS_PCT", abs(CFG.hard_stop_loss_pct)),
        "MIN_HOLD_SEC": _env_float("EXIT_MIN_HOLD_SEC", CFG.min_hold_sec),
        "MAX_HOLD_SEC": _env_float("EXIT_MAX_HOLD_SEC", CFG.max_hold_sec),
        "MAX_GIVEBACK_SOFT": _env_float("AI_SELL_MAX_GIVEBACK_SOFT", CFG.giveback_soft_pct),
        "MAX_GIVEBACK_HARD": _env_float("AI_SELL_MAX_GIVEBACK_HARD", CFG.giveback_hard_pct),
        "STAGNATION_MIN": _env_float("EXIT_STAGNANT_MAX_MIN", CFG.stagnation_min),
        "STAGNATION_MIN_MOVE_PCT": _env_float(
            "EXIT_STAGNANT_MIN_MOVE_PCT", CFG.stagnation_min_move_pct
        ),
    }


# =============================================================================
# Amount resolution (best-effort, never guess)
# =============================================================================
async def _resolve_amount_raw_from_wallet(mint: str) -> int | None:
    """
    Best-effort attempt to resolve a token balance, with tight timeout.
    Never throws. Returns None if not provable.
    """
    if not CFG.resolve_amount_on_force_exit:
        return None

    mint = (mint or "").strip()
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

    async def _call(fn: Any) -> int | None:
        try:
            if asyncio.iscoroutinefunction(fn):
                out = await asyncio.wait_for(fn(mint), timeout=float(CFG.resolve_amount_timeout_s))
            else:
                out = await asyncio.wait_for(
                    asyncio.to_thread(fn, mint), timeout=float(CFG.resolve_amount_timeout_s)
                )
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
# Decision model (pure)
# =============================================================================
def compute_sell_decision(
    *,
    features: dict[str, Any],
    trade: dict[str, Any],
    risk_ctx: dict[str, Any],
    strat_ctx: dict[str, Any],
    ai_score: float | None = None,  # 0..1, higher = stronger EXIT pressure
    ai_meta: dict | None = None,
) -> dict[str, Any]:
    """
    Returns:
      {
        "action": "hold"|"partial"|"sell",
        "pct": float 0..1,
        "reason": str,
        "sell_score": float 0..1,
        "meta": dict
      }
    """
    pnl_pct = _finite(trade.get("pnl_pct"), 0.0)
    max_up_pct = _finite(trade.get("max_up_pct"), pnl_pct)
    hold_sec = max(0.0, _finite(trade.get("hold_sec"), 0.0))
    giveback_pct = max(0.0, max_up_pct - pnl_pct)

    sell_alpha = _finite(features.get("sell_alpha"), 0.0)  # 0..1
    micro_shift = _finite(features.get("micro_trend_shift"), 0.0)  # negative => deterioration
    liq_usd = max(
        _finite(features.get("liq_usd"), 0.0), _finite(features.get("liquidity_usd"), 0.0)
    )
    whale_exit = _finite(features.get("whale_exit_pressure"), 0.0)  # 0..1
    fraud_risk_pct = _finite(features.get("fraud_risk_pct"), 0.0)  # 0..100
    rug_prob = features.get("rug_prob_pct", features.get("rug_prob", 0.0))
    rug_prob_pct = _finite(rug_prob, 0.0)
    if 0.0 <= rug_prob_pct <= 1.0:
        rug_prob_pct *= 100.0
    rug_prob_pct = _clamp(rug_prob_pct, 0.0, 100.0)

    risk_profile = _safe_str(risk_ctx.get("risk_profile"), "balanced").lower() or "balanced"
    daily_dd_pct = _finite(risk_ctx.get("daily_drawdown_pct"), 0.0)
    regime_exit_bias = _finite(
        risk_ctx.get("regime_exit_bias"), _finite(features.get("regime_exit_bias"), 1.0)
    )

    min_profit = _finite(strat_ctx.get("MIN_PROFIT_PCT"), CFG.min_profit_pct)
    tp1 = _finite(strat_ctx.get("TP1_PCT"), CFG.tp1_pct)
    tp2 = _finite(strat_ctx.get("TP2_PCT"), CFG.tp2_pct)
    tp3 = _finite(strat_ctx.get("TP3_PCT"), CFG.tp3_pct)

    hard_sl = -abs(_finite(strat_ctx.get("STOP_LOSS_PCT"), abs(CFG.hard_stop_loss_pct)))
    min_hold = _finite(strat_ctx.get("MIN_HOLD_SEC"), CFG.min_hold_sec)
    max_hold = _finite(strat_ctx.get("MAX_HOLD_SEC"), CFG.max_hold_sec)

    gb_soft = _finite(strat_ctx.get("MAX_GIVEBACK_SOFT"), CFG.giveback_soft_pct)
    gb_hard = _finite(strat_ctx.get("MAX_GIVEBACK_HARD"), CFG.giveback_hard_pct)

    stagnation_min = _finite(strat_ctx.get("STAGNATION_MIN"), CFG.stagnation_min)
    stagnation_min_move = _finite(
        strat_ctx.get("STAGNATION_MIN_MOVE_PCT"), CFG.stagnation_min_move_pct
    )

    meta: dict[str, Any] = {
        "pnl_pct": float(pnl_pct),
        "max_up_pct": float(max_up_pct),
        "giveback_pct": float(giveback_pct),
        "hold_sec": float(hold_sec),
        "risk_profile": risk_profile,
    }

    # -------------------------
    # HARD exits (AI cannot override)
    # -------------------------
    if fraud_risk_pct >= float(CFG.fraud_exit_pct):
        meta.update({"fraud_risk_pct": float(fraud_risk_pct)})
        return {
            "action": "sell",
            "pct": 1.0,
            "reason": "HARD_FRAUD_RISK",
            "sell_score": 1.0,
            "meta": meta,
        }

    if whale_exit >= float(CFG.whale_exit_pressure):
        meta.update({"whale_exit_pressure": float(whale_exit)})
        return {
            "action": "sell",
            "pct": 1.0,
            "reason": "HARD_WHALE_EXIT",
            "sell_score": 1.0,
            "meta": meta,
        }

    if pnl_pct <= float(hard_sl):
        meta.update({"stop_loss_pct": float(hard_sl)})
        return {
            "action": "sell",
            "pct": 1.0,
            "reason": "HARD_STOP_LOSS",
            "sell_score": 1.0,
            "meta": meta,
        }

    if daily_dd_pct <= float(CFG.account_dd_protect_pct):
        meta.update(
            {
                "daily_dd_pct": float(daily_dd_pct),
                "account_dd_protect_pct": float(CFG.account_dd_protect_pct),
            }
        )
        return {
            "action": "sell",
            "pct": 1.0,
            "reason": "HARD_ACCOUNT_DD_PROTECT",
            "sell_score": 1.0,
            "meta": meta,
        }

    if hold_sec < float(min_hold):
        meta.update({"min_hold_sec": float(min_hold)})
        return {
            "action": "hold",
            "pct": 0.0,
            "reason": "GUARD_MIN_HOLD",
            "sell_score": 0.0,
            "meta": meta,
        }

    if hold_sec >= float(max_hold):
        meta.update({"max_hold_sec": float(max_hold)})
        return {
            "action": "sell",
            "pct": 1.0,
            "reason": "MAX_HOLD_EXIT",
            "sell_score": 0.95,
            "meta": meta,
        }

    # -------------------------
    # Profit-protecting exits
    # -------------------------
    in_profit = pnl_pct >= float(min_profit)

    if liq_usd > 0 and liq_usd <= float(CFG.liquidity_floor_usd) and in_profit:
        meta.update({"liq_usd": float(liq_usd)})
        return {
            "action": "partial",
            "pct": float(CFG.giveback_soft_sell_pct),
            "reason": "LIQ_FLOOR_PARTIAL",
            "sell_score": 0.72,
            "meta": meta,
        }

    if pnl_pct > float(min_profit) and max_up_pct > float(min_profit):
        if giveback_pct >= float(gb_hard):
            meta.update({"gb_hard": float(gb_hard)})
            return {
                "action": "sell",
                "pct": 1.0,
                "reason": "GIVEBACK_HARD",
                "sell_score": 1.0,
                "meta": meta,
            }
        if giveback_pct >= float(gb_soft):
            meta.update({"gb_soft": float(gb_soft)})
            return {
                "action": "partial",
                "pct": float(CFG.giveback_soft_sell_pct),
                "reason": "GIVEBACK_SOFT",
                "sell_score": 0.78,
                "meta": meta,
            }

    stagnation_seen_min = _finite(
        features.get("stagnation_min", features.get("stagnant_min", 0.0)), 0.0
    )
    last_move_pct = _finite(features.get("last_move_pct", features.get("move_pct", 0.0)), 0.0)
    if (
        in_profit
        and stagnation_seen_min >= float(stagnation_min)
        and abs(last_move_pct) < float(stagnation_min_move)
    ):
        meta.update(
            {"stagnation_min": float(stagnation_seen_min), "last_move_pct": float(last_move_pct)}
        )
        return {
            "action": "sell",
            "pct": float(CFG.stagnation_sell_pct),
            "reason": "STAGNATION_EXIT",
            "sell_score": 0.85,
            "meta": meta,
        }

    if pnl_pct >= float(tp3):
        meta.update({"tp": "tp3"})
        return {
            "action": "partial",
            "pct": float(CFG.tp3_sell_pct),
            "reason": "TP3",
            "sell_score": 0.70,
            "meta": meta,
        }
    if pnl_pct >= float(tp2):
        meta.update({"tp": "tp2"})
        return {
            "action": "partial",
            "pct": float(CFG.tp2_sell_pct),
            "reason": "TP2",
            "sell_score": 0.62,
            "meta": meta,
        }
    if pnl_pct >= float(tp1):
        meta.update({"tp": "tp1"})
        if sell_alpha > 0.55 or micro_shift < -0.40:
            return {
                "action": "partial",
                "pct": max(float(CFG.tp1_sell_pct), 0.30),
                "reason": "TP1_EXIT_PRESSURE",
                "sell_score": 0.62,
                "meta": meta,
            }
        return {
            "action": "partial",
            "pct": float(CFG.tp1_sell_pct),
            "reason": "TP1",
            "sell_score": 0.56,
            "meta": meta,
        }

    # -------------------------
    # Soft scoring baseline + AI blend
    # -------------------------
    profit_factor = 0.0 if pnl_pct < 0 else _smoothstep01(pnl_pct / 20.0)
    micro_factor = _clip01((-micro_shift) / 1.5)
    alpha_factor = _clip01(sell_alpha)

    fraud_soft = _clip01(fraud_risk_pct / 100.0)
    rug_soft = _clip01(rug_prob_pct / 100.0)
    whale_soft = _clip01(whale_exit)

    liq_term = (
        0.12
        if (liq_usd > 0 and liq_usd < 8_000)
        else (0.05 if (liq_usd > 0 and liq_usd < 20_000) else 0.0)
    )
    hold_factor = _clip01(hold_sec / max(max_hold, 1.0))

    base_score = (
        0.18 * profit_factor
        + 0.18 * alpha_factor
        + 0.16 * micro_factor
        + 0.08 * whale_soft
        + 0.06 * fraud_soft
        + 0.06 * rug_soft
        + 0.10 * hold_factor
        + 0.08 * _clip01(liq_term)
        + 0.04 * _clip01(max(0.0, (max_up_pct - pnl_pct) / 60.0))
    )

    prof_mult = {
        "ultra_defensive": 1.25,
        "defensive": 1.10,
        "balanced": 1.00,
        "aggressive": 0.90,
        "degenerate": 0.80,
    }.get(risk_profile, 1.00)

    regime_mult = _clamp(_finite(regime_exit_bias, 1.0), 0.50, 1.50)
    sell_score = _clip01(base_score * prof_mult * regime_mult)

    if ai_score is not None:
        w = float(CFG.ai_weight)
        if pnl_pct < 0:
            w = w * float(CFG.ai_weight_loss_mult)
        w = _clamp(w, 0.0, 0.90)
        sell_score = _clip01(w * _clip01(ai_score) + (1.0 - w) * sell_score)
        if isinstance(ai_meta, dict):
            meta["ai_meta"] = ai_meta

    meta["sell_score"] = float(sell_score)

    if sell_score >= float(CFG.strong_score):
        return {
            "action": "sell",
            "pct": 1.0,
            "reason": "SELL_SCORE_STRONG",
            "sell_score": float(sell_score),
            "meta": meta,
        }
    if sell_score >= float(CFG.mid_score):
        pct = 0.40 if pnl_pct >= 0 else 0.25
        return {
            "action": "partial",
            "pct": float(pct),
            "reason": "SELL_SCORE_MID",
            "sell_score": float(sell_score),
            "meta": meta,
        }

    return {
        "action": "hold",
        "pct": 0.0,
        "reason": "HOLD_SCORE",
        "sell_score": float(sell_score),
        "meta": meta,
    }


# =============================================================================
# Reservations (prevent double-sells)
# =============================================================================
def _decision_sig(action: str, pct: float, reason: str) -> str:
    base = f"{action}|{pct:.6f}|{reason}"
    return hashlib.sha256(base.encode("utf-8", "ignore")).hexdigest()[:24]


def _reserve_add(st: dict[str, Any], *, request_id: str, amt: int, now: float) -> None:
    res = st.get("reservations")
    if not isinstance(res, list):
        res = []
    res.append({"id": request_id[:32], "amt": int(max(0, amt)), "ts": float(now)})
    st["reservations"] = res
    _prune_reservations(st, now)


def _reserve_remove(
    st: dict[str, Any],
    *,
    request_id: str | None = None,
    amt: int | None = None,
    now: float | None = None,
) -> int:
    now_f = float(now if now is not None else time.time())
    res = st.get("reservations")
    if not isinstance(res, list) or not res:
        st["reservations"] = []
        st["reserved_raw"] = int(st.get("reserved_raw") or 0)
        return 0

    freed = 0
    keep: list[dict[str, Any]] = []
    rid = (request_id or "").strip()[:32]

    if rid:
        for item in res:
            if not isinstance(item, dict):
                continue
            if str(item.get("id") or "")[:32] == rid:
                freed += _safe_int(item.get("amt"), 0)
                continue
            keep.append(item)
    else:
        dec = int(amt or 0)
        if dec <= 0:
            keep = list(res)
        else:
            res2: list[dict[str, Any]] = [x for x in res if isinstance(x, dict)]
            res2.sort(key=lambda x: _finite(x.get("ts"), 0.0))
            for item in res2:
                a = _safe_int(item.get("amt"), 0)
                if a <= 0:
                    continue
                if dec <= 0:
                    keep.append(item)
                    continue
                take = min(a, dec)
                a2 = a - take
                dec -= take
                freed += take
                if a2 > 0:
                    keep.append({**item, "amt": int(a2)})

    st["reservations"] = keep
    _prune_reservations(st, now_f)
    return int(max(0, freed))


# =============================================================================
# Public helpers
# =============================================================================
async def get_state(mint: str) -> dict[str, Any]:
    return await _load_state(mint)


async def register_position(
    *,
    mint: str,
    symbol: str = "",
    pool: str = "unknown",
    entry_ts: float | None = None,
    entry_price: float | None = None,
    position_raw: int = 0,
    entry_spend_sol: float = 0.0,
    candidate_id: str | None = None,
    entry_sig: str | None = None,
) -> None:
    """
    Safe hydration hook for restarts.
    IMPORTANT: position_raw must be REAL (from fills / wallet proof).
    """
    mint = (mint or "").strip()
    if not mint or mint == _SOL_MINT:
        return

    now = time.time()
    st: dict[str, Any] = {
        "mint": mint,
        "symbol": (symbol.strip() or mint[:4]),
        "pool": (pool.strip() or "unknown"),
        "entry_ts": float(entry_ts if entry_ts else now),
        "entry_price": float(entry_price) if entry_price and entry_price > 0 else None,
        "entry_spend_sol": float(entry_spend_sol or 0.0),
        "position_raw": int(max(0, int(position_raw))),
        "reserved_raw": 0,
        "reservations": [],
        "max_up_pct": 0.0,
        "last_eval_ts": 0.0,
        "last_price": None,
        "last_tick_ts": 0.0,
        "last_enqueue_ts": 0.0,
        "last_decision_sig": "",
        "enqueue_failures": 0,
        "freeze_until_ts": 0.0,
        "candidate_id": candidate_id,
        "entry_sig": entry_sig,
    }
    await _save_state(mint, st)


# =============================================================================
# Public API
# =============================================================================
async def schedule_after_buy(ev: dict[str, Any]) -> None:
    if not CFG.enabled:
        return
    try:
        mint = _extract_mint(ev)
        if not mint:
            return
        mint = mint.strip()
        if mint == _SOL_MINT:
            return

        now = time.time()
        entry_ts = _finite(ev.get("ts") or ev.get("entry_ts") or now, now)
        amount_raw = _extract_amount_raw(ev)
        entry_price = _extract_entry_price(ev)

        spend_sol = 0.0
        for k in (
            "spend_sol",
            "buy_size_sol",
            "amount_sol",
            "entry_size_sol",
            "spent_sol",
            "entry_spend_sol",
        ):
            if ev.get(k) is None:
                continue
            spend_sol = _finite(ev.get(k), 0.0)
            if spend_sol:
                break

        state: dict[str, Any] = {
            "mint": mint,
            "symbol": _extract_symbol(ev, mint),
            "pool": str(ev.get("pool") or ev.get("source") or _extract_pool(ev)).strip()
            or "unknown",
            "entry_ts": float(entry_ts),
            "entry_price": float(entry_price) if entry_price else None,
            "entry_spend_sol": float(spend_sol),
            "position_raw": int(amount_raw)
            if isinstance(amount_raw, int) and amount_raw > 0
            else 0,
            "reserved_raw": 0,
            "reservations": [],
            "max_up_pct": 0.0,
            "last_eval_ts": 0.0,
            "last_price": None,
            "last_tick_ts": 0.0,
            "last_enqueue_ts": 0.0,
            "last_decision_sig": "",
            "enqueue_failures": 0,
            "freeze_until_ts": 0.0,
            "candidate_id": ev.get("candidate_id")
            or ev.get("entry_candidate_id")
            or ev.get("cand_id"),
            "entry_sig": ev.get("signature") or ev.get("tx_sig") or ev.get("tx"),
        }

        sl = _get_mod("utils_pkg.self_learning_ai_strategy")
        hook = getattr(sl, "on_buy_registered", None) if sl else None
        if callable(hook):
            with contextlib.suppress(Exception):
                out = hook(dict(state))
                if isinstance(out, dict):
                    state.update(out)

        await _save_state(mint, state)

        if CFG.debug:
            _log(
                "AI_SELL_AFTER_BUY",
                mint=mint,
                symbol=state.get("symbol"),
                pool=state.get("pool"),
                amount_raw=state.get("position_raw"),
                entry_price=state.get("entry_price"),
                spend_sol=state.get("entry_spend_sol"),
                version=AI_SELL_VERSION,
                instance=INSTANCE_ID,
            )
    except Exception:
        return


async def on_trade_tick(raw_event: dict[str, Any]) -> None:
    """
    Tick-driven exit evaluation. Safe-by-default:
      - No amount guessing
      - Hard exits first
      - Optional AI via ai_exit_model
      - Optional quote sanity
      - Reservation + dedupe to prevent double-sells
    """
    if not CFG.enabled:
        return

    try:
        mint = _extract_mint(raw_event)
        if not mint:
            return
        mint = mint.strip()
        if not mint or mint == _SOL_MINT:
            return

        lk = await _mint_lock(mint)
        async with lk:
            state = await _load_state(mint)
            if not state:
                return

            now = time.time()
            _prune_reservations(state, now)

            freeze_until = _finite(state.get("freeze_until_ts"), 0.0)
            if freeze_until and now < freeze_until:
                if CFG.debug:
                    _log("AI_SELL_FROZEN", mint=mint, until=freeze_until, instance=INSTANCE_ID)
                return

            last_eval = _finite(state.get("last_eval_ts"), 0.0)
            if CFG.eval_min_interval_ms > 0 and (now - last_eval) * 1000.0 < float(
                CFG.eval_min_interval_ms
            ):
                return
            state["last_eval_ts"] = now

            price = _extract_price(raw_event)
            if price is None:
                await _save_state(mint, state)
                return

            last_tick_ts = _finite(state.get("last_tick_ts"), 0.0)
            last_price = _finite(state.get("last_price"), 0.0)

            if last_tick_ts and CFG.tick_dedupe_window_ms > 0:
                if (now - last_tick_ts) * 1000.0 < float(CFG.tick_dedupe_window_ms):
                    if last_price > 0 and CFG.tick_min_price_delta_pct > 0:
                        dp = abs(float(price) - last_price) / last_price * 100.0
                        if dp < float(CFG.tick_min_price_delta_pct):
                            return
                    else:
                        return

            state["last_tick_ts"] = now
            state["last_price"] = float(price)

            entry_price_f = _finite(state.get("entry_price"), 0.0)
            entry_price = entry_price_f if entry_price_f > 0 else None

            pnl_pct = 0.0
            if entry_price and price:
                pnl_pct = (float(price) / float(entry_price) - 1.0) * 100.0

            max_up = _finite(state.get("max_up_pct"), pnl_pct)
            if pnl_pct > max_up:
                max_up = pnl_pct
            state["max_up_pct"] = float(max_up)

            entry_ts = _finite(state.get("entry_ts"), now)
            hold_sec = max(0.0, now - entry_ts)

            trade = {
                "entry_price": entry_price,
                "current_price": float(price),
                "pnl_pct": float(pnl_pct),
                "max_up_pct": float(max_up),
                "hold_sec": float(hold_sec),
            }

            features = await _build_features(mint, raw_event, state)
            features.setdefault("mint", mint)
            features.setdefault("symbol", state.get("symbol"))
            features.setdefault("pnl_pct", pnl_pct)
            features.setdefault("max_up_pct", max_up)
            features.setdefault("hold_sec", hold_sec)

            features = _ai_risk_patch(features, trade)
            risk_ctx = await _risk_ctx()
            strat_ctx = _self_learning_overrides(mint, _base_sell_cfg_from_env(), state, features)

            ai_score: float | None = None
            ai_meta: dict | None = None
            ai_reason = "ai_skipped"
            if CFG.ai_enabled and (not CFG.dryrun):
                ai_score, ai_meta, ai_reason = await _ai_exit_score_from_model(
                    features=features, mint=mint
                )

            decision = compute_sell_decision(
                features=features,
                trade=trade,
                risk_ctx=risk_ctx,
                strat_ctx=strat_ctx,
                ai_score=ai_score,
                ai_meta=ai_meta,
            )
            decision = _self_learning_refine(
                mint,
                decision,
                state=state,
                features=features,
                trade=trade,
                risk_ctx=risk_ctx,
                strat_ctx=strat_ctx,
                ai_score=ai_score,
                ai_meta=ai_meta,
            )

            await _log_sell_signal(
                {
                    "mint": mint,
                    "decision": decision,
                    "trade": trade,
                    "risk_ctx": risk_ctx,
                    "strat_ctx": strat_ctx,
                    "ai_score": ai_score,
                    "ai_reason": ai_reason,
                    "ai_meta": ai_meta,
                    "ts": now,
                    "version": AI_SELL_VERSION,
                    "instance": INSTANCE_ID,
                }
            )

            if CFG.dryrun:
                await _save_state(mint, state)
                if CFG.debug:
                    _log(
                        "AI_SELL_DRYRUN_DECISION",
                        mint=mint,
                        action=decision.get("action"),
                        pct=decision.get("pct"),
                        reason=decision.get("reason"),
                        sell_score=decision.get("sell_score"),
                        ai_reason=ai_reason,
                        version=AI_SELL_VERSION,
                        instance=INSTANCE_ID,
                    )
                return

            action = str(decision.get("action") or "hold").lower()
            pct = _clamp(_finite(decision.get("pct"), 0.0), 0.0, 1.0)
            reason = str(decision.get("reason") or "AI_SELL_DECISION")
            sell_score = _clip01(_finite(decision.get("sell_score"), 0.0))

            if action not in ("sell", "partial") or pct <= 0.0:
                await _save_state(mint, state)
                return

            # Default: allow exits even if killed (risk reduction).
            if await _kill_switch_active() and _env_bool(
                "AI_SELL_BLOCK_TICK_SELL_WHEN_KILLED", False
            ):
                if CFG.debug:
                    _log(
                        "AI_SELL_BLOCKED_KILL_SWITCH",
                        level="warning",
                        mint=mint,
                        reason=reason,
                        instance=INSTANCE_ID,
                    )
                await _save_state(mint, state)
                return

            sig = _decision_sig(action, pct, reason)
            last_sig = str(state.get("last_decision_sig") or "")
            last_enq = _finite(state.get("last_enqueue_ts"), 0.0)
            if last_sig == sig and (now - last_enq) < float(CFG.exit_dedupe_sec):
                await _save_state(mint, state)
                return

            # amount safety (NEVER GUESS)
            position_raw = _safe_int(state.get("position_raw"), 0)
            reserved_raw = _safe_int(state.get("reserved_raw"), 0)

            if position_raw <= 0 and CFG.resolve_amount_on_tick_when_unknown:
                resolved = await _resolve_amount_raw_from_wallet(mint)
                if resolved:
                    position_raw = int(resolved)
                    state["position_raw"] = int(resolved)

            available = max(0, position_raw - reserved_raw)
            if available <= 0:
                if CFG.debug:
                    _log(
                        "AI_SELL_NO_AVAILABLE",
                        mint=mint,
                        position_raw=position_raw,
                        reserved_raw=reserved_raw,
                        reason=reason,
                        instance=INSTANCE_ID,
                    )
                await _save_state(mint, state)
                return

            amount_raw = int(max(1, int(available * pct)))
            amount_raw = min(amount_raw, available)

            ok_quote, quote_reason = await _quote_sanity_ok(
                mint=mint, amount_raw=amount_raw, force=False
            )
            if not ok_quote:
                if CFG.debug:
                    _log(
                        "AI_SELL_QUOTE_BLOCK",
                        level="warning",
                        mint=mint,
                        reason=quote_reason,
                        instance=INSTANCE_ID,
                    )
                await _save_state(mint, state)
                return

            request_id = str(raw_event.get("request_id") or "") or uuid.uuid4().hex[:16]

            ok, enq_reason = await _enqueue_exit_request(
                mint=mint,
                amount_raw=int(amount_raw),
                reason=str(reason),
                sell_pct=float(pct),
                entry_spend_sol=float(state.get("entry_spend_sol") or 0.0),
                follow_stats={
                    "pnl_pct": float(pnl_pct),
                    "max_up_pct": float(max_up),
                    "hold_sec": float(hold_sec),
                    "sell_score": float(sell_score),
                    "action": action,
                    "reason": str(reason),
                    "quote_sanity": quote_reason,
                    "ai_reason": ai_reason,
                    "request_id": request_id,
                },
                candidate_id=state.get("candidate_id"),
                symbol=str(state.get("symbol") or ""),
                pool=str(state.get("pool") or ""),
                slippage_bps=int(raw_event.get("slippage_bps") or CFG.default_slippage_bps),
                priority=int(raw_event.get("priority") or CFG.default_priority),
                source="ai_sell_logic.on_trade_tick",
                version=AI_SELL_VERSION,
                instance=INSTANCE_ID,
                dryrun=int(bool(CFG.dryrun)),
                request_id=request_id,
            )

            await _log_sell_enqueue(
                {
                    "mint": mint,
                    "ok": int(ok),
                    "enqueue_reason": enq_reason,
                    "amount_raw": int(amount_raw),
                    "pct": float(pct),
                    "action": action,
                    "reason": str(reason),
                    "sell_score": float(sell_score),
                    "quote_sanity": quote_reason,
                    "ai_reason": ai_reason,
                    "request_id": request_id,
                    "ts": now,
                    "version": AI_SELL_VERSION,
                    "instance": INSTANCE_ID,
                }
            )

            if ok:
                _reserve_add(state, request_id=request_id, amt=int(amount_raw), now=now)
                state["last_enqueue_ts"] = now
                state["last_decision_sig"] = sig
                state["enqueue_failures"] = 0
                await _save_state(mint, state)

                _log(
                    "AI_SELL_ENQUEUED",
                    mint=mint,
                    amount_raw=int(amount_raw),
                    pct=float(pct),
                    action=action,
                    reason=str(reason),
                    sell_score=float(sell_score),
                    quote=quote_reason,
                    ai_reason=ai_reason,
                    request_id=request_id,
                    version=AI_SELL_VERSION,
                    instance=INSTANCE_ID,
                )
            else:
                fails = _safe_int(state.get("enqueue_failures"), 0) + 1
                state["enqueue_failures"] = fails
                if fails >= int(CFG.max_enqueue_failures) and int(CFG.failure_freeze_sec) > 0:
                    state["freeze_until_ts"] = now + float(CFG.failure_freeze_sec)
                    _log(
                        "AI_SELL_FREEZE_ON_FAILURES",
                        level="warning",
                        mint=mint,
                        failures=fails,
                        freeze_sec=int(CFG.failure_freeze_sec),
                        instance=INSTANCE_ID,
                    )
                await _save_state(mint, state)

    except Exception:
        return


async def schedule_force_exit(ev: dict[str, Any]) -> None:
    """
    Forced exits: used by exit_engine follow-through fails (and other hard signals).
    Enqueues ONLY if we can prove amount_raw (event -> state -> wallet resolve).

    NOTE: quote sanity MUST NOT block forced exits unless explicitly enabled.
    """
    if not CFG.enabled:
        return

    try:
        mint = _extract_mint(ev)
        if not mint:
            return
        mint = mint.strip()
        if not mint or mint == _SOL_MINT:
            return

        lk = await _mint_lock(mint)
        async with lk:
            state = await _load_state(mint)
            now = time.time()
            if state:
                _prune_reservations(state, now)

            sell_pct = _clamp(_finite(ev.get("sell_pct"), 1.0), 0.01, 1.0)
            reason = str(ev.get("reason") or "force_exit")

            amount_raw = _extract_amount_raw(ev)

            # state-based best-effort (still provable)
            if (not amount_raw or int(amount_raw) <= 0) and state:
                pos = _safe_int(state.get("position_raw"), 0)
                resv = _safe_int(state.get("reserved_raw"), 0)
                avail = max(0, pos - resv)
                if avail > 0:
                    amount_raw = max(1, int(avail * sell_pct))

            # wallet resolve (optional)
            if (not amount_raw or int(amount_raw) <= 0) and CFG.resolve_amount_on_force_exit:
                resolved = await _resolve_amount_raw_from_wallet(mint)
                if resolved:
                    amount_raw = int(max(1, int(resolved * sell_pct)))
                    if state is not None:
                        state.setdefault("position_raw", int(resolved))

            if not amount_raw or int(amount_raw) <= 0:
                _log(
                    "AI_SELL_FORCE_EXIT_NO_AMOUNT",
                    level="warning",
                    mint=mint,
                    reason=reason,
                    version=AI_SELL_VERSION,
                    instance=INSTANCE_ID,
                )
                return

            ok_quote, quote_reason = await _quote_sanity_ok(
                mint=mint, amount_raw=int(amount_raw), force=True
            )
            if not ok_quote:
                _log(
                    "AI_SELL_FORCE_EXIT_QUOTE_BLOCK",
                    level="warning",
                    mint=mint,
                    reason=quote_reason,
                    instance=INSTANCE_ID,
                )
                return

            spend_sol = _finite(
                ev.get("spend_sol"), _finite(state.get("entry_spend_sol") if state else 0.0, 0.0)
            )
            follow_stats = dict(ev.get("follow_stats") or {})
            follow_stats["quote_sanity"] = quote_reason

            request_id = str(ev.get("request_id") or "") or uuid.uuid4().hex[:16]

            ok, enq_reason = await _enqueue_exit_request(
                mint=mint,
                amount_raw=int(amount_raw),
                reason=reason,
                sell_pct=float(sell_pct),
                entry_spend_sol=float(spend_sol),
                follow_stats={**follow_stats, "request_id": request_id},
                candidate_id=ev.get("candidate_id")
                or (state.get("candidate_id") if state else None),
                symbol=str(ev.get("symbol") or (state.get("symbol") if state else "") or ""),
                pool=str(ev.get("pool") or (state.get("pool") if state else "") or ""),
                slippage_bps=int(ev.get("slippage_bps") or CFG.default_slippage_bps),
                priority=int(ev.get("priority") or CFG.default_priority),
                exit_label=str(ev.get("exit_label") or ""),
                source=str(ev.get("source") or "ai_sell_logic.force_exit"),
                version=AI_SELL_VERSION,
                instance=INSTANCE_ID,
                dryrun=int(bool(CFG.dryrun)),
                request_id=request_id,
            )

            if ok and state:
                _reserve_add(state, request_id=request_id, amt=int(amount_raw), now=time.time())
                state["last_enqueue_ts"] = time.time()
                state["enqueue_failures"] = 0
                await _save_state(mint, state)

            _log(
                "AI_SELL_FORCE_EXIT_ENQUEUED",
                mint=mint,
                amount_raw=int(amount_raw),
                sell_pct=float(sell_pct),
                ok=int(ok),
                enqueue_reason=enq_reason,
                request_id=request_id,
                reason=reason,
                version=AI_SELL_VERSION,
                instance=INSTANCE_ID,
            )

    except Exception:
        return


# Backwards-compatible aliases
async def force_exit(ev: dict[str, Any]) -> None:
    await schedule_force_exit(ev)


async def request_exit(ev: dict[str, Any]) -> None:
    await schedule_force_exit(ev)


async def exit_now(ev: dict[str, Any]) -> None:
    await schedule_force_exit(ev)


async def on_sell_filled(ev: dict[str, Any]) -> None:
    """
    Worker callback after an actual sell fill.
      - frees reservation (by request_id if present, else by amount)
      - reduces position_raw best-effort if fill amount is known
      - deletes state if position goes to zero
    """
    try:
        mint = _extract_mint(ev) or str(ev.get("mint") or "").strip()
        if not mint:
            return
        mint = mint.strip()

        amt = _extract_amount_raw(ev)
        request_id = str(ev.get("request_id") or ev.get("id") or "").strip()[:32]

        lk = await _mint_lock(mint)
        async with lk:
            st = await _load_state(mint)
            if not st:
                return

            now = time.time()
            _prune_reservations(st, now)

            freed = 0
            if request_id:
                freed = _reserve_remove(st, request_id=request_id, now=now)
            elif isinstance(amt, int) and amt > 0:
                freed = _reserve_remove(st, amt=int(amt), now=now)

            pos = _safe_int(st.get("position_raw"), 0)
            if isinstance(amt, int) and amt > 0 and pos > 0:
                pos = max(0, pos - int(amt))
                st["position_raw"] = int(pos)

            st["last_sell_fill_ts"] = now

            if _safe_int(st.get("position_raw"), 0) <= 0:
                await _delete_state(mint)
                _log(
                    "AI_SELL_STATE_CLOSED",
                    mint=mint,
                    freed_reserved=int(freed),
                    version=AI_SELL_VERSION,
                    instance=INSTANCE_ID,
                )
                return

            await _save_state(mint, st)

    except Exception:
        return


async def on_sell_failed(ev: dict[str, Any]) -> None:
    """
    Worker callback when a sell request fails / is rejected.
    Frees reservation so future exits aren't blocked.
    """
    try:
        mint = _extract_mint(ev) or str(ev.get("mint") or "").strip()
        if not mint:
            return
        mint = mint.strip()

        request_id = str(ev.get("request_id") or ev.get("id") or "").strip()[:32]
        amt = _extract_amount_raw(ev)

        lk = await _mint_lock(mint)
        async with lk:
            st = await _load_state(mint)
            if not st:
                return

            now = time.time()
            _prune_reservations(st, now)

            freed = 0
            if request_id:
                freed = _reserve_remove(st, request_id=request_id, now=now)
            elif isinstance(amt, int) and amt > 0:
                freed = _reserve_remove(st, amt=int(amt), now=now)

            st["last_sell_fail_ts"] = now
            await _save_state(mint, st)

            if CFG.debug:
                _log(
                    "AI_SELL_FREE_RESERVE_ON_FAIL",
                    mint=mint,
                    freed_reserved=int(freed),
                    request_id=request_id,
                    version=AI_SELL_VERSION,
                    instance=INSTANCE_ID,
                )

    except Exception:
        return


__all__ = [
    "compute_sell_decision",
    "exit_now",
    "force_exit",
    "get_state",
    "on_sell_failed",
    "on_sell_filled",
    "on_trade_tick",
    "register_position",
    "reload_ai_sell_config",
    "request_exit",
    "schedule_after_buy",
    "schedule_force_exit",
]
