from __future__ import annotations

"""
utils_pkg/ai_sell_logic.py

V3+++ / QUANT-GRADE SELL INTELLIGENCE ENGINE (GOD-TIER LOCK-IN)
==============================================================

This module is the SELL brain (AI-assisted, guardrail-first).

Called by:
  • utils_pkg.exit_engine.on_buy_filled  -> schedule_after_buy(payload)
  • utils_pkg.exit_engine.on_trade_tick  -> on_trade_tick(raw_event)
  • utils_pkg.exit_engine.follow-through -> schedule_force_exit / force_exit / request_exit / exit_now

Primary job:
  1) Maintain per-mint position state (entry, amount_raw, max_up, etc.)
  2) On each tick, evaluate exit logic (hard exits first, then soft/AI blend)
  3) Enqueue exit requests via utils_pkg.auto_exit_worker.exit_engine.enqueue_exit_request

SAFETY CONTRACTS (LOCKED)
------------------------
  • NEVER guess token amounts. If amount_raw unknown -> do not enqueue sells.
    - Allowed: best-effort resolve from wallet balance (optional, guarded, timed).
  • Hard exits (stop loss, fraud, kill/account drawdown protection) ALWAYS win.
    AI can *never* override hard exits.
  • Best-effort only: never raise to callers.
  • Concurrency safe: per-mint locks + in-flight reservations to prevent double-sells.
  • Optional quote sanity (router sell quote) is OFF by default (safe).

Public API (stable):
    compute_sell_decision(...)
    schedule_after_buy(ev)
    on_trade_tick(raw_event)
    schedule_force_exit(ev)
    force_exit(ev) / request_exit(ev) / exit_now(ev)

Optional / additive:
    reload_ai_sell_config()
    on_sell_filled(ev)   # can be called by worker after a sell fill to release reservations

"""

import asyncio
import contextlib
import dataclasses
import inspect
import json
import os
import time
from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple


# =============================================================================
# Logger (logging_setup compatible if present)
# =============================================================================
try:
    from logging_setup import get_logger  # type: ignore

    log = get_logger("sniper.ai_sell")  # type: ignore
except Exception:  # pragma: no cover
    import logging

    log = logging.getLogger("sniper.ai_sell")
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
# Lazy imports (avoid cycles)
# =============================================================================
_MOD_CACHE: Dict[str, Any] = {}


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
# Env helpers / clamps
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


def _env_json(key: str) -> Optional[dict]:
    raw = (os.getenv(key, "") or "").strip()
    if not raw:
        return None
    try:
        obj = json.loads(raw)
        return obj if isinstance(obj, dict) else None
    except Exception:
        _log("AI_SELL_BAD_ENV_JSON", level="warning", key=key)
        return None


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
        if v != v or v in (float("inf"), float("-inf")):
            return d
        return v
    except Exception:
        return d


def _safe_float(d: Dict[str, Any], key: str, default: float = 0.0) -> float:
    return _finite(d.get(key, default), default)


def _safe_int(d: Dict[str, Any], key: str, default: int = 0) -> int:
    try:
        return int(d.get(key, default))
    except Exception:
        return default


async def _maybe_await(x: Any) -> Any:
    if inspect.isawaitable(x):
        return await x
    return x


def _short_exc(e: BaseException, n: int = 260) -> str:
    s = f"{type(e).__name__}: {e}"
    return (s[:n] + "…") if len(s) > n else s


# =============================================================================
# Version / instance / phase
# =============================================================================
AI_SELL_VERSION: str = "V3+++_LOCKIN_2025-12-22"
INSTANCE_ID: str = (
    os.getenv("INSTANCE_ID")
    or os.getenv("RENDER_INSTANCE_ID")
    or os.getenv("HOSTNAME")
    or ""
).strip() or "local"

_SOL_MINT = "So11111111111111111111111111111111111111112"


# =============================================================================
# Config
# =============================================================================
@dataclass(frozen=True)
class SellCfg:
    master_phase: int
    enabled: bool
    dryrun: bool
    debug: bool

    state_ttl_sec: int
    eval_min_interval_ms: int
    exit_dedupe_sec: int

    # scoring thresholds
    strong_score: float
    mid_score: float

    # guardrails
    min_hold_sec: float
    max_hold_sec: float
    hard_stop_loss_pct: float  # negative, e.g. -10
    account_dd_protect_pct: float  # negative, e.g. -20

    # take profit tiers
    min_profit_pct: float
    tp1_pct: float
    tp2_pct: float
    tp3_pct: float
    tp1_sell_pct: float
    tp2_sell_pct: float
    tp3_sell_pct: float

    # giveback protection (from peak)
    giveback_soft_pct: float
    giveback_hard_pct: float
    giveback_soft_sell_pct: float

    # risk exits
    fraud_exit_pct: float  # 0..100
    whale_exit_pressure: float  # 0..1
    liquidity_floor_usd: float  # if <=, bias exit

    # AI blend
    ai_enabled: bool
    ai_weight: float
    ai_timeout_sec: float
    ai_max_feature_bytes: int
    ai_min_score: float
    ai_strict: bool
    ai_model: str
    ai_model_routing: Dict[str, Any]
    ai_default_head: str

    # quote sanity (router)
    require_quote_sanity: bool
    quote_timeout_sec: float
    quote_max_price_impact_bps: int
    quote_min_out_sol: float

    # worker enqueue defaults
    default_slippage_bps: int
    default_priority: int

    # force-exit amount resolution
    resolve_amount_on_force_exit: bool
    resolve_amount_timeout_s: float

    # telemetry
    publish_state_to_redis: bool
    state_key_prefix: str


def _load_cfg() -> SellCfg:
    master = _env_int("MASTER_PHASE", _env_int("STRATEGY_PHASE", 7))
    master = max(0, min(7, master))

    dryrun = (
        _env_bool("AI_SELL_DRYRUN", False)
        or _env_bool("DRYRUN", False)
        or _env_bool("DRY_RUN", False)
        or _env_bool("PAPER_TRADING", False)
    )

    enabled = _env_bool("AI_SELL_ENABLED", master >= 2) and (not _env_bool("KILL_SWITCH", False))

    # Tiers & guardrails follow EXIT_* naming to match your env patterns
    min_profit = _env_float("EXIT_MIN_PROFIT_PCT", 2.0)
    tp1 = _env_float("EXIT_TP1_PCT", 4.0)
    tp2 = _env_float("EXIT_TP2_PCT", 8.0)
    tp3 = _env_float("EXIT_TP3_PCT", 15.0)

    sl_pct = _env_float("EXIT_STOP_LOSS_PCT", 10.0)
    # accept either 10 or -10; normalize to negative
    hard_sl = -abs(float(sl_pct))

    min_hold = _env_float("EXIT_MIN_HOLD_SEC", 10.0)
    max_hold = _env_float("EXIT_MAX_HOLD_SEC", 7200.0)

    gb_soft = _env_float("AI_SELL_MAX_GIVEBACK_SOFT", 35.0)
    gb_hard = _env_float("AI_SELL_MAX_GIVEBACK_HARD", 55.0)

    model = _env_str("AI_SELL_MODEL_NAME", _env_str("AI_SELL_MODEL", ""))
    routing = _env_json("AI_SELL_MODEL_ROUTING_JSON") or {}
    default_head = _env_str("AI_SELL_DEFAULT_HEAD", "default") or "default"

    # quote sanity defaults OFF (safe)
    require_quote = _env_bool("AI_SELL_REQUIRE_QUOTE_SANITY", False)

    default_slip = _env_int("AI_SELL_SLIPPAGE_BPS", _env_int("PF_SLIPPAGE_BPS", 200))
    default_slip = max(5, min(5000, int(default_slip)))

    return SellCfg(
        master_phase=master,
        enabled=enabled,
        dryrun=dryrun,
        debug=_env_bool("AI_SELL_DEBUG", False),
        state_ttl_sec=max(60, _env_int("AI_SELL_STATE_TTL_SEC", 7200)),
        eval_min_interval_ms=max(0, _env_int("AI_SELL_EVAL_MIN_INTERVAL_MS", 450)),
        exit_dedupe_sec=max(1, _env_int("AI_SELL_EXIT_DEDUPE_SEC", 6)),
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
        giveback_soft_sell_pct=_clamp(_env_float("AI_SELL_GIVEBACK_SOFT_SELL_PCT", 0.50), 0.01, 1.0),
        fraud_exit_pct=_clamp(_env_float("AI_SELL_FRAUD_EXIT_PCT", 60.0), 0.0, 100.0),
        whale_exit_pressure=_clip01(_env_float("AI_SELL_WHALE_EXIT_PRESSURE", 0.80)),
        liquidity_floor_usd=max(0.0, _env_float("AI_SELL_LIQ_FLOOR_USD", 8000.0)),
        ai_enabled=_env_bool("AI_SELL_AI_ENABLED", True),
        ai_weight=_clip01(_env_float("AI_SELL_AI_WEIGHT", 0.60)),
        ai_timeout_sec=max(1.0, _env_float("AI_SELL_TIMEOUT_SEC", 8.0)),
        ai_max_feature_bytes=max(2000, _env_int("AI_SELL_MAX_FEATURE_BYTES", 50_000)),
        ai_min_score=_clip01(_env_float("AI_SELL_MIN_AI_SCORE", 0.0)),
        ai_strict=_env_bool("AI_SELL_STRICT", False),
        ai_model=model,
        ai_model_routing=routing,
        ai_default_head=default_head,
        require_quote_sanity=require_quote,
        quote_timeout_sec=max(0.2, _env_float("AI_SELL_QUOTE_TIMEOUT_SEC", 1.25)),
        quote_max_price_impact_bps=max(0, _env_int("AI_SELL_QUOTE_MAX_PRICE_IMPACT_BPS", 1800)),
        quote_min_out_sol=max(0.0, _env_float("AI_SELL_QUOTE_MIN_OUT_SOL", 0.0)),
        default_slippage_bps=int(default_slip),
        default_priority=max(0, _env_int("AI_SELL_PRIORITY", 1)),
        resolve_amount_on_force_exit=_env_bool("AI_SELL_RESOLVE_AMOUNT_ON_FORCE_EXIT", True),
        resolve_amount_timeout_s=max(0.25, _env_float("AI_SELL_RESOLVE_AMOUNT_TIMEOUT_S", 2.0)),
        publish_state_to_redis=_env_bool("AI_SELL_PUBLISH_STATE_TO_REDIS", True),
        state_key_prefix=_env_str("AI_SELL_STATE_KEY_PREFIX", "ai_sell") or "ai_sell",
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
    )


# =============================================================================
# Redis state store (preferred) + memory fallback
# =============================================================================
_MEM: Dict[str, Dict[str, Any]] = {}
_MEM_LOCK = asyncio.Lock()


def _state_key(mint: str) -> str:
    return f"{CFG.state_key_prefix}:{mint}"


async def _get_redis() -> Any:
    """
    Best-effort redis getter:
      - utils_pkg.core_utils.get_redis_async()
      - utils_pkg.core_utils.get_redis()
      - utils_pkg.core_utils.redis
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

    return getattr(cu, "redis", None)


async def _load_state(mint: str) -> Dict[str, Any]:
    mint = (mint or "").strip()
    if not mint:
        return {}

    r = await _get_redis()
    if r is not None:
        with contextlib.suppress(Exception):
            raw = await _maybe_await(r.get(_state_key(mint)))
            if raw:
                if isinstance(raw, (bytes, bytearray)):
                    raw = raw.decode("utf-8", "ignore")
                obj = json.loads(str(raw))
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


async def _save_state(mint: str, state: Dict[str, Any]) -> None:
    mint = (mint or "").strip()
    if not mint:
        return
    now = time.time()

    st = dict(state)
    st["_updated_ts"] = now
    st.setdefault("_version", AI_SELL_VERSION)
    st.setdefault("_instance", INSTANCE_ID)

    r = await _get_redis()
    if r is not None:
        with contextlib.suppress(Exception):
            payload = json.dumps(st, separators=(",", ":"), ensure_ascii=False)
            await _maybe_await(r.set(_state_key(mint), payload, ex=int(CFG.state_ttl_sec)))
            return

    async with _MEM_LOCK:
        st["_expires_ts"] = now + float(CFG.state_ttl_sec)
        _MEM[mint] = st


async def _delete_state(mint: str) -> None:
    mint = (mint or "").strip()
    if not mint:
        return
    r = await _get_redis()
    if r is not None:
        with contextlib.suppress(Exception):
            await _maybe_await(r.delete(_state_key(mint)))
    async with _MEM_LOCK:
        _MEM.pop(mint, None)


# =============================================================================
# Per-mint locks (concurrency safety)
# =============================================================================
_MINT_LOCKS: Dict[str, asyncio.Lock] = {}
_MINT_LOCKS_GUARD = asyncio.Lock()


async def _mint_lock(mint: str) -> asyncio.Lock:
    mint = (mint or "").strip()
    if not mint:
        return asyncio.Lock()
    async with _MINT_LOCKS_GUARD:
        lk = _MINT_LOCKS.get(mint)
        if lk is None:
            lk = asyncio.Lock()
            _MINT_LOCKS[mint] = lk
        # opportunistic pruning
        if len(_MINT_LOCKS) > 5000:
            for k in list(_MINT_LOCKS.keys())[:1000]:
                if k != mint:
                    _MINT_LOCKS.pop(k, None)
        return lk


# =============================================================================
# Event extraction helpers
# =============================================================================
def _extract_mint(ev: Dict[str, Any]) -> Optional[str]:
    for k in ("mint", "mintAddress", "mint_address", "tokenMint", "baseMint", "id", "address", "token"):
        v = ev.get(k)
        if isinstance(v, str) and v.strip():
            return v.strip()
    # nested
    with contextlib.suppress(Exception):
        for kk in ("pair", "token", "base", "asset"):
            o = ev.get(kk)
            if isinstance(o, dict):
                for k2 in ("mint", "address", "id"):
                    v = o.get(k2)
                    if isinstance(v, str) and v.strip():
                        return v.strip()
    return None


def _extract_price(ev: Dict[str, Any]) -> Optional[float]:
    for k in ("priceUi", "price_ui", "price", "priceInSol", "price_in_sol", "ui_price", "last_price", "lastPrice", "close", "mid", "mark"):
        v = ev.get(k)
        if v is None:
            continue
        with contextlib.suppress(Exception):
            f = float(v)
            if f > 0:
                return f
    return None


def _extract_amount_raw(ev: Dict[str, Any]) -> Optional[int]:
    for k in ("amount_raw", "amount_out", "base_amount_raw", "token_amount_raw", "out_amount_raw", "position_raw", "size_raw", "amountRaw"):
        v = ev.get(k)
        if v is None:
            continue
        with contextlib.suppress(Exception):
            i = int(v)
            if i > 0:
                return i
    return None


def _extract_entry_price(ev: Dict[str, Any]) -> Optional[float]:
    for k in ("entry_price", "entryPrice", "price_entry_ui", "entry_price_ui", "fill_price_ui", "avg_price_ui", "priceUi"):
        v = ev.get(k)
        if v is None:
            continue
        with contextlib.suppress(Exception):
            f = float(v)
            if f > 0:
                return f
    return None


def _extract_symbol(ev: Dict[str, Any], mint: str) -> str:
    for k in ("symbol", "sym", "ticker"):
        v = ev.get(k)
        if isinstance(v, str) and v.strip():
            return v.strip()
    return (mint[:4] if mint else "").strip()


def _extract_pool(ev: Dict[str, Any]) -> str:
    for k in ("pool", "source", "venue", "dex", "market"):
        v = ev.get(k)
        if isinstance(v, str) and v.strip():
            return v.strip()
    return "unknown"


# =============================================================================
# Optional hooks: risk_manager / self-learning / data_logger
# =============================================================================
def _risk_ctx() -> Dict[str, Any]:
    rm = _get_mod("utils_pkg.risk_manager")
    if not rm:
        return {}
    for nm in ("risk_manager", "RISK", "RISK_MANAGER"):
        obj = getattr(rm, nm, None)
        if obj is not None:
            rm = obj
            break
    for name in ("get_ai_ctx", "get_sell_ctx", "get_ctx"):
        fn = getattr(rm, name, None)
        if callable(fn):
            with contextlib.suppress(Exception):
                out = fn()
                return out if isinstance(out, dict) else {}
    return {}


def _kill_switch_active() -> bool:
    # hard global switch
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
                val = fn()
                if isinstance(val, bool):
                    return val
    return False


def _self_learning_overrides(mint: str, base_cfg: Dict[str, Any], state: Dict[str, Any], features: Dict[str, Any]) -> Dict[str, Any]:
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


def _self_learning_refine(mint: str, decision: Dict[str, Any], *, state: Dict[str, Any], features: Dict[str, Any], trade: Dict[str, Any], risk_ctx: Dict[str, Any], strat_ctx: Dict[str, Any], ai_score: Optional[float]) -> Dict[str, Any]:
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
            }
        )
        return out if isinstance(out, dict) else decision
    return decision


async def _log_sell_signal(payload: Dict[str, Any]) -> None:
    dl = _get_mod("utils_pkg.data_logger")
    if not dl:
        dl = _get_mod("utils_pkg.data_logger")  # some repos use same name; keep
    if not dl:
        return
    for nm in ("log_sell_signal", "log_exit_signal", "log_signal"):
        fn = getattr(dl, nm, None)
        if callable(fn):
            with contextlib.suppress(Exception):
                out = fn(payload)
                await _maybe_await(out)
            return


# =============================================================================
# Feature building (best-effort)
# =============================================================================
async def _build_features(mint: str, raw_event: Dict[str, Any], state: Dict[str, Any]) -> Dict[str, Any]:
    # 1) passthrough provided features
    for k in ("features", "fb", "feat"):
        v = raw_event.get(k)
        if isinstance(v, dict) and v:
            out = dict(v)
            out.setdefault("mint", mint)
            return out

    # 2) try FeatureBuilder
    fb = _get_mod("utils_pkg.feature_builder")
    if fb:
        for fn_name in ("build_features_for_sell", "build_features_for_mint", "build_features"):
            fn = getattr(fb, fn_name, None)
            if callable(fn):
                payload = {"mint": mint, "tick": raw_event, "state": state, "mode": "sell", "ts": time.time()}
                with contextlib.suppress(Exception):
                    res = fn(payload)
                    res = await _maybe_await(res)
                    if isinstance(res, dict) and res:
                        out = dict(res)
                        out.setdefault("mint", mint)
                        return out

    # 3) fallback: raw tick
    out = dict(raw_event)
    out.setdefault("mint", mint)
    return out


# =============================================================================
# Optional OpenAI scoring (blended, never overrides hard exits)
# =============================================================================
_CLIENT: Any = None


def _ensure_client() -> Optional[Any]:
    global _CLIENT
    if _CLIENT is not None:
        return _CLIENT
    if not (os.getenv("OPENAI_API_KEY") or ""):
        _CLIENT = None
        return None
    # Prefer OpenAI() (modern)
    with contextlib.suppress(Exception):
        from openai import OpenAI  # type: ignore

        _CLIENT = OpenAI()
        _log("AI_SELL_OPENAI_CLIENT", mode="OpenAI()")
        return _CLIENT
    # Fallback: openai module (legacy)
    with contextlib.suppress(Exception):
        import openai  # type: ignore

        if not getattr(openai, "api_key", None):
            openai.api_key = os.getenv("OPENAI_API_KEY", "")
        _CLIENT = openai
        _log("AI_SELL_OPENAI_CLIENT", mode="legacy_module")
        return _CLIENT
    _CLIENT = None
    return None


def _pick_sell_model(features: Dict[str, Any], trade: Dict[str, Any], risk_ctx: Dict[str, Any]) -> Optional[str]:
    explicit = str(features.get("sell_model_name") or "").strip()
    if explicit:
        return explicit
    if CFG.ai_model_routing:
        head = (
            str(features.get("exit_head") or "").strip()
            or str(features.get("segment") or "").strip()
            or str(risk_ctx.get("risk_profile") or "").strip()
            or CFG.ai_default_head
        )
        if head in CFG.ai_model_routing:
            return str(CFG.ai_model_routing[head])
        if "default" in CFG.ai_model_routing:
            return str(CFG.ai_model_routing["default"])
    m = (CFG.ai_model or "").strip()
    return m or None


async def _ai_sell_score(*, features: Dict[str, Any], trade: Dict[str, Any], risk_ctx: Dict[str, Any]) -> Optional[float]:
    if not CFG.ai_enabled:
        return None
    if not (os.getenv("OPENAI_API_KEY") or ""):
        return None
    model = _pick_sell_model(features, trade, risk_ctx)
    if not model:
        return None
    client = _ensure_client()
    if client is None:
        return None

    payload = {
        "features": features,
        "trade": trade,
        "risk_ctx": {
            k: risk_ctx.get(k)
            for k in (
                "risk_profile",
                "daily_drawdown_pct",
                "daily_realized_pnl_pct",
                "recent_loss_streak",
                "rolling_win_rate_pct",
                "regime_exit_bias",
                "regime_entry_gate_factor",
            )
            if k in risk_ctx
        },
        "task": "score_exit_probability_and_action",
    }

    try:
        blob = json.dumps(payload, separators=(",", ":"), ensure_ascii=False)
        if len(blob.encode("utf-8")) > CFG.ai_max_feature_bytes:
            keep = {
                "mint",
                "symbol",
                "pnl_pct",
                "max_up_pct",
                "sell_alpha",
                "buy_alpha",
                "micro_trend_shift",
                "liq_usd",
                "whale_exit_pressure",
                "fraud_risk_pct",
                "exit_quality_score",
                "vol_pct_5s",
                "vol_pct_30s",
            }
            compact = {
                "features": {k: v for k, v in features.items() if k in keep},
                "trade": trade,
                "risk_ctx": payload["risk_ctx"],
                "task": payload["task"],
            }
            blob = json.dumps(compact, separators=(",", ":"), ensure_ascii=False)
    except Exception:
        blob = "{}"

    prompt = (
        "You are a crypto exit classifier.\n"
        "Input is JSON for a single open trade.\n"
        "Output ONLY JSON:\n"
        '{"sell_score": <0-1>, "action": "sell|hold|partial", "sell_pct": <0-1>}.\n'
        "sell_score = probability that selling now is better than holding.\n"
        f"Input:\n{blob}"
    )

    try:
        # Modern client path: client.responses.create OR client.chat.completions.create
        if hasattr(client, "responses") and hasattr(client.responses, "create"):

            def _do() -> Any:
                return client.responses.create(  # type: ignore[call-arg]
                    model=model,
                    input=prompt,
                    response_format={"type": "json_object"},
                )

            resp = await asyncio.wait_for(asyncio.to_thread(_do), timeout=float(CFG.ai_timeout_sec))
            # Try to extract structured JSON
            content_str = None
            with contextlib.suppress(Exception):
                # Many SDKs return text in nested output
                content_str = resp.output[0].content[0].text  # type: ignore[attr-defined,index]
            if content_str is None:
                content_str = str(resp)

        elif hasattr(client, "chat") and hasattr(client.chat, "completions") and hasattr(client.chat.completions, "create"):

            def _do2() -> Any:
                return client.chat.completions.create(  # type: ignore[call-arg]
                    model=model,
                    messages=[
                        {"role": "system", "content": "You output JSON only."},
                        {"role": "user", "content": prompt},
                    ],
                    temperature=0.0,
                    response_format={"type": "json_object"},
                )

            resp = await asyncio.wait_for(asyncio.to_thread(_do2), timeout=float(CFG.ai_timeout_sec))
            content_str = resp.choices[0].message.content  # type: ignore[attr-defined,index]

        else:
            # Legacy module path (best effort)
            def _do3() -> Any:
                # type: ignore[attr-defined]
                return client.ChatCompletion.create(
                    model=model,
                    messages=[
                        {"role": "system", "content": "You output JSON only."},
                        {"role": "user", "content": prompt},
                    ],
                    temperature=0.0,
                )

            resp = await asyncio.wait_for(asyncio.to_thread(_do3), timeout=float(CFG.ai_timeout_sec))
            content_str = resp["choices"][0]["message"]["content"]  # type: ignore[index]

        obj = json.loads(content_str) if isinstance(content_str, str) else {}
        score = _clip01(_finite(obj.get("sell_score", obj.get("score", 0.0)), 0.0))
        if CFG.ai_min_score > 0 and score < CFG.ai_min_score:
            return None
        return score

    except Exception as e:
        if CFG.ai_strict:
            raise
        if CFG.debug:
            _log("AI_SELL_OPENAI_FAIL", level="warning", err=_short_exc(e))
        return None


# =============================================================================
# Router quote sanity (optional)
# =============================================================================
def _filter_kwargs(fn: Any, kwargs: Dict[str, Any]) -> Dict[str, Any]:
    try:
        sig = inspect.signature(fn)
        allowed = set(sig.parameters.keys())
        return {k: v for k, v in kwargs.items() if k in allowed}
    except Exception:
        return kwargs


async def _quote_sanity_ok(*, mint: str, amount_raw: int) -> Tuple[bool, str]:
    """
    Best-effort sanity check for SELL route (optional, OFF by default).
    Returns (ok, reason).
    """
    if not CFG.require_quote_sanity:
        return True, "quote_disabled"
    if not mint or mint == _SOL_MINT or amount_raw <= 0:
        return False, "bad_mint_or_amount"

    router = _get_mod("utils_pkg.router")
    if not router:
        return True, "router_missing"

    # Try a few common sell quote functions
    fns = [
        getattr(router, "get_sell_quote", None),
        getattr(router, "sell_quote", None),
        getattr(router, "get_quote", None),
    ]
    fns = [fn for fn in fns if callable(fn)]
    if not fns:
        return True, "quote_fn_missing"

    fn = fns[0]
    kwargs = {
        "mint": mint,
        "amount_raw": int(amount_raw),
        "slippage_bps": int(CFG.default_slippage_bps),
    }
    kwargs = _filter_kwargs(fn, kwargs)

    try:
        out = fn(**kwargs)
        out = await asyncio.wait_for(_maybe_await(out), timeout=float(CFG.quote_timeout_sec))
        # Accept dict-like quotes; otherwise assume OK (best-effort)
        if isinstance(out, dict):
            # Optional fields we can sanity-check if present
            pib = _safe_int(out, "priceImpactBps", _safe_int(out, "price_impact_bps", 0))
            if pib and int(CFG.quote_max_price_impact_bps) > 0 and pib > int(CFG.quote_max_price_impact_bps):
                return False, f"price_impact_bps_too_high({pib})"
            out_sol = _finite(out.get("out_sol") or out.get("outAmountSol") or out.get("out_amount_sol"), 0.0)
            if CFG.quote_min_out_sol > 0 and out_sol > 0 and out_sol < float(CFG.quote_min_out_sol):
                return False, f"out_sol_too_low({out_sol:.6f})"
        return True, "quote_ok"
    except Exception as e:
        # Never block exits just because quote failed; treat as "unknown but allowed"
        if CFG.debug:
            _log("AI_SELL_QUOTE_FAIL", level="warning", mint=mint, err=_short_exc(e))
        return True, "quote_fail_allow"


# =============================================================================
# Worker enqueue
# =============================================================================
async def _enqueue_exit_request(**kwargs: Any) -> bool:
    ew = _get_mod("utils_pkg.auto_exit_worker.exit_engine")
    if not ew:
        ew = _get_mod("utils_pkg.auto_exit_worker")  # sometimes exports
    fn = getattr(ew, "enqueue_exit_request", None) if ew else None
    if not callable(fn):
        return False
    try:
        payload = _filter_kwargs(fn, dict(kwargs))
        res = fn(**payload)
        await _maybe_await(res)
        return True
    except Exception as e:
        _log("AI_SELL_ENQUEUE_FAIL", level="warning", err=_short_exc(e), mint=str(kwargs.get("mint") or ""))
        return False


# =============================================================================
# Pure decision model
# =============================================================================
def compute_sell_decision(
    *,
    features: Dict[str, Any],
    trade: Dict[str, Any],
    risk_ctx: Dict[str, Any],
    strat_ctx: Dict[str, Any],
    ai_score: Optional[float] = None,
) -> Dict[str, Any]:
    """
    Returns dict:
      action: "sell"|"partial"|"hold"
      pct: 0..1
      reason: str  (reason bucket)
      sell_score: 0..1
      meta: dict
    """
    pnl_pct = _finite(trade.get("pnl_pct"), 0.0)
    max_up_pct = _finite(trade.get("max_up_pct"), pnl_pct)
    hold_sec = max(0.0, _finite(trade.get("hold_sec"), 0.0))
    giveback_pct = max(0.0, max_up_pct - pnl_pct)

    # features (best-effort)
    sell_alpha = _safe_float(features, "sell_alpha", 0.0)  # 0..1 (higher => exit pressure)
    buy_alpha = _safe_float(features, "buy_alpha", 0.0)  # 0..1
    micro_shift = _safe_float(features, "micro_trend_shift", 0.0)  # negative => deterioration
    liq_usd = _safe_float(features, "liq_usd", 0.0)
    whale_exit = _safe_float(features, "whale_exit_pressure", 0.0)  # 0..1
    fraud_risk_pct = _safe_float(features, "fraud_risk_pct", 0.0)  # 0..100
    vol_5s = _safe_float(features, "vol_pct_5s", 0.0)
    vol_30s = _safe_float(features, "vol_pct_30s", 0.0)

    # risk ctx
    risk_profile = str(risk_ctx.get("risk_profile", "balanced") or "balanced").lower()
    daily_dd_pct = _safe_float(risk_ctx, "daily_drawdown_pct", 0.0)
    regime_exit_bias = _safe_float(risk_ctx, "regime_exit_bias", 1.0)

    # strategy cfg
    min_profit = _finite(strat_ctx.get("MIN_PROFIT_PCT"), CFG.min_profit_pct)
    tp1 = _finite(strat_ctx.get("TP1_PCT"), CFG.tp1_pct)
    tp2 = _finite(strat_ctx.get("TP2_PCT"), CFG.tp2_pct)
    tp3 = _finite(strat_ctx.get("TP3_PCT"), CFG.tp3_pct)
    hard_sl = _finite(strat_ctx.get("STOP_LOSS_PCT"), CFG.hard_stop_loss_pct)
    # normalize (if someone passed +10)
    hard_sl = -abs(float(hard_sl))
    min_hold = _finite(strat_ctx.get("MIN_HOLD_SEC"), CFG.min_hold_sec)
    max_hold = _finite(strat_ctx.get("MAX_HOLD_SEC"), CFG.max_hold_sec)

    gb_soft = _finite(strat_ctx.get("MAX_GIVEBACK_SOFT"), CFG.giveback_soft_pct)
    gb_hard = _finite(strat_ctx.get("MAX_GIVEBACK_HARD"), CFG.giveback_hard_pct)

    vol_crush_thr = _finite(strat_ctx.get("VOL_CRUSH_THRESHOLD"), -35.0)
    vol_expand_thr = _finite(strat_ctx.get("VOL_EXPAND_THRESHOLD"), 50.0)

    # risk-profile adjustments (conservative -> tighter giveback)
    if risk_profile in ("ultra_defensive", "defensive"):
        gb_soft = max(10.0, gb_soft - 5.0)
        gb_hard = max(20.0, gb_hard - 10.0)
    elif risk_profile in ("aggressive", "degenerate"):
        gb_soft = gb_soft + 5.0
        gb_hard = gb_hard + 5.0

    # -------------------------
    # HARD exits (AI cannot override)
    # -------------------------
    if fraud_risk_pct >= CFG.fraud_exit_pct:
        return {"action": "sell", "pct": 1.0, "reason": "HARD_FRAUD_RISK", "sell_score": 1.0, "meta": {"fraud_risk_pct": fraud_risk_pct}}

    if whale_exit >= CFG.whale_exit_pressure:
        return {"action": "sell", "pct": 1.0, "reason": "HARD_WHALE_EXIT", "sell_score": 1.0, "meta": {"whale_exit_pressure": whale_exit}}

    if pnl_pct <= hard_sl:
        return {"action": "sell", "pct": 1.0, "reason": "HARD_STOP_LOSS", "sell_score": 1.0, "meta": {"pnl_pct": pnl_pct, "stop_loss_pct": hard_sl}}

    if daily_dd_pct <= float(CFG.account_dd_protect_pct):
        return {"action": "sell", "pct": 1.0, "reason": "HARD_ACCOUNT_DD_PROTECT", "sell_score": 1.0, "meta": {"daily_dd_pct": daily_dd_pct}}

    # MIN hold guard (unless already dangerous)
    if hold_sec < min_hold:
        return {"action": "hold", "pct": 0.0, "reason": "GUARD_MIN_HOLD", "sell_score": 0.0, "meta": {"hold_sec": hold_sec, "min_hold_sec": min_hold}}

    # Liquidity collapse bias (soft unless loss)
    if liq_usd > 0 and liq_usd <= float(CFG.liquidity_floor_usd) and pnl_pct >= 0.0:
        return {"action": "partial", "pct": float(CFG.giveback_soft_sell_pct), "reason": "LIQ_FLOOR_PARTIAL", "sell_score": 0.70, "meta": {"liq_usd": liq_usd}}

    # VOL crush exit (if not losing; losing handled by stoploss)
    if vol_30s <= vol_crush_thr and pnl_pct >= 0.0:
        return {"action": "sell", "pct": 1.0, "reason": "VOL_CRUSH_EXIT", "sell_score": 1.0, "meta": {"vol_30s": vol_30s}}

    # Trend continuation exception
    if vol_30s >= vol_expand_thr and buy_alpha > 0.55 and pnl_pct > min_profit:
        return {"action": "hold", "pct": 0.0, "reason": "TREND_CONTINUATION", "sell_score": 0.0, "meta": {"vol_30s": vol_30s, "buy_alpha": buy_alpha}}

    # Giveback protection (only if profit > min_profit)
    if pnl_pct > min_profit and max_up_pct > min_profit:
        if giveback_pct >= gb_hard:
            return {"action": "sell", "pct": 1.0, "reason": "GIVEBACK_HARD", "sell_score": 1.0, "meta": {"giveback_pct": giveback_pct}}
        if giveback_pct >= gb_soft:
            return {"action": "partial", "pct": float(CFG.giveback_soft_sell_pct), "reason": "GIVEBACK_SOFT", "sell_score": 0.75, "meta": {"giveback_pct": giveback_pct}}

    # Take profit tiers (progressive trims)
    if pnl_pct >= tp3:
        return {"action": "partial", "pct": float(CFG.tp3_sell_pct), "reason": "TP3", "sell_score": 0.70, "meta": {"pnl_pct": pnl_pct}}
    if pnl_pct >= tp2:
        return {"action": "partial", "pct": float(CFG.tp2_sell_pct), "reason": "TP2", "sell_score": 0.60, "meta": {"pnl_pct": pnl_pct}}
    if pnl_pct >= tp1:
        # If exit pressure is high at TP1, trim more
        if sell_alpha > 0.55 or micro_shift < -0.4:
            return {"action": "partial", "pct": max(float(CFG.tp1_sell_pct), 0.30), "reason": "TP1_EXIT_PRESSURE", "sell_score": 0.60, "meta": {"pnl_pct": pnl_pct}}
        return {"action": "partial", "pct": float(CFG.tp1_sell_pct), "reason": "TP1", "sell_score": 0.55, "meta": {"pnl_pct": pnl_pct}}

    # Dead coin exit (best-effort heuristic)
    dead_coin = (micro_shift < -0.6 and vol_5s < -20.0 and liq_usd > 0 and liq_usd < 15_000 and buy_alpha < 0.25)
    if dead_coin and pnl_pct >= 0.0:
        return {"action": "sell", "pct": 1.0, "reason": "DEAD_COIN_EXIT", "sell_score": 0.85, "meta": {"micro_shift": micro_shift, "vol_5s": vol_5s, "liq_usd": liq_usd}}

    # Time-based bias (max_hold)
    hold_factor = _clip01(hold_sec / max(max_hold, 1.0))

    # Score model
    profit_factor = 0.0 if pnl_pct < 0 else _smoothstep01(pnl_pct / 20.0)
    alpha_factor = _clip01(sell_alpha)
    micro_factor = _clip01((-micro_shift) / 1.5)
    liq_term = 0.12 if (liq_usd > 0 and liq_usd < 8_000) else (0.05 if (liq_usd > 0 and liq_usd < 20_000) else 0.0)

    base_score = (
        0.22 * profit_factor
        + 0.22 * alpha_factor
        + 0.18 * micro_factor
        + 0.10 * _clip01(whale_exit)
        + 0.08 * _clip01(fraud_risk_pct / 100.0)
        + 0.10 * hold_factor
        + 0.10 * _clip01(liq_term)
    )

    prof_mult = {
        "ultra_defensive": 1.35,
        "defensive": 1.15,
        "balanced": 1.00,
        "aggressive": 0.85,
        "degenerate": 0.70,
    }.get(risk_profile, 1.00)

    regime_mult = _clip01(regime_exit_bias) if regime_exit_bias > 0 else 1.0
    sell_score = _clip01(base_score * prof_mult * regime_mult)

    # Blend with AI score (soft only; hard exits already returned above)
    if ai_score is not None:
        sell_score = _clip01(float(CFG.ai_weight) * _clip01(ai_score) + (1.0 - float(CFG.ai_weight)) * sell_score)

    if sell_score >= float(CFG.strong_score):
        return {"action": "sell", "pct": 1.0, "reason": "SELL_SCORE_STRONG", "sell_score": sell_score, "meta": {}}
    if sell_score >= float(CFG.mid_score):
        return {"action": "partial", "pct": 0.40, "reason": "SELL_SCORE_MID", "sell_score": sell_score, "meta": {}}

    # Max-hold hard bias at the end
    if hold_sec >= max_hold:
        return {"action": "sell", "pct": 1.0, "reason": "MAX_HOLD_EXIT", "sell_score": 0.90, "meta": {"hold_sec": hold_sec, "max_hold_sec": max_hold}}

    return {"action": "hold", "pct": 0.0, "reason": "HOLD_SCORE", "sell_score": sell_score, "meta": {}}


# =============================================================================
# Strategy cfg base (env-compatible)
# =============================================================================
def _base_sell_cfg_from_env() -> Dict[str, Any]:
    return {
        "MIN_PROFIT_PCT": float(os.getenv("EXIT_MIN_PROFIT_PCT", str(CFG.min_profit_pct)) or CFG.min_profit_pct),
        "TP1_PCT": float(os.getenv("EXIT_TP1_PCT", str(CFG.tp1_pct)) or CFG.tp1_pct),
        "TP2_PCT": float(os.getenv("EXIT_TP2_PCT", str(CFG.tp2_pct)) or CFG.tp2_pct),
        "TP3_PCT": float(os.getenv("EXIT_TP3_PCT", str(CFG.tp3_pct)) or CFG.tp3_pct),
        # STOP_LOSS_PCT: accept + or -; compute_sell_decision normalizes
        "STOP_LOSS_PCT": float(os.getenv("EXIT_STOP_LOSS_PCT", str(abs(CFG.hard_stop_loss_pct))) or abs(CFG.hard_stop_loss_pct)),
        "MIN_HOLD_SEC": float(os.getenv("EXIT_MIN_HOLD_SEC", str(CFG.min_hold_sec)) or CFG.min_hold_sec),
        "MAX_HOLD_SEC": float(os.getenv("EXIT_MAX_HOLD_SEC", str(CFG.max_hold_sec)) or CFG.max_hold_sec),
        "VOL_CRUSH_THRESHOLD": float(os.getenv("EXIT_VOL_CRUSH_THRESHOLD", "-35.0") or -35.0),
        "VOL_EXPAND_THRESHOLD": float(os.getenv("EXIT_VOL_EXPAND_THRESHOLD", "50.0") or 50.0),
        "MAX_GIVEBACK_SOFT": float(os.getenv("AI_SELL_MAX_GIVEBACK_SOFT", str(CFG.giveback_soft_pct)) or CFG.giveback_soft_pct),
        "MAX_GIVEBACK_HARD": float(os.getenv("AI_SELL_MAX_GIVEBACK_HARD", str(CFG.giveback_hard_pct)) or CFG.giveback_hard_pct),
    }


# =============================================================================
# Force-exit amount resolution (best-effort, never guess)
# =============================================================================
async def _resolve_amount_raw_from_wallet(mint: str) -> Optional[int]:
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
# Public API
# =============================================================================
async def schedule_after_buy(ev: Dict[str, Any]) -> None:
    """
    Called after BUY fill. Stores per-mint state needed for later sells.
    """
    if not CFG.enabled:
        return
    try:
        mint = _extract_mint(ev)
        if not mint:
            return
        mint = mint.strip()
        if mint == _SOL_MINT:
            return

        entry_ts = _finite(ev.get("ts") or ev.get("entry_ts") or time.time(), time.time())
        amount_raw = _extract_amount_raw(ev)
        entry_price = _extract_entry_price(ev)

        spend_sol = 0.0
        for k in ("spend_sol", "buy_size_sol", "amount_sol", "entry_size_sol", "spent_sol", "entry_spend_sol"):
            if ev.get(k) is None:
                continue
            spend_sol = _finite(ev.get(k), 0.0)
            if spend_sol:
                break

        state: Dict[str, Any] = {
            "mint": mint,
            "symbol": _extract_symbol(ev, mint),
            "pool": str(ev.get("pool") or ev.get("source") or _extract_pool(ev)).strip() or "unknown",
            "entry_ts": float(entry_ts),
            "entry_price": float(entry_price) if entry_price else None,
            "entry_spend_sol": float(spend_sol),
            # position_raw is authoritative ONLY if provided by fill
            "position_raw": int(amount_raw) if isinstance(amount_raw, int) and amount_raw > 0 else 0,
            "reserved_raw": 0,
            "max_up_pct": 0.0,
            "last_eval_ts": 0.0,
            "last_price": None,
            "last_enqueue_ts": 0.0,
            "candidate_id": ev.get("candidate_id") or ev.get("entry_candidate_id") or ev.get("cand_id"),
            "entry_sig": ev.get("signature") or ev.get("tx_sig") or ev.get("tx"),
        }

        # self-learning can annotate initial state (best-effort)
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


async def on_trade_tick(raw_event: Dict[str, Any]) -> None:
    """
    Called on each trade tick (best-effort).
    Loads state, evaluates decision, enqueues exit if needed.
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

            # throttle evaluations
            now = time.time()
            last_eval = _finite(state.get("last_eval_ts"), 0.0)
            if CFG.eval_min_interval_ms > 0 and (now - last_eval) * 1000.0 < float(CFG.eval_min_interval_ms):
                return
            state["last_eval_ts"] = now

            # Update last_price + max_up tracking
            price = _extract_price(raw_event)
            if price is not None:
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
                "current_price": price,
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

            risk_ctx = _risk_ctx()
            strat_ctx = _self_learning_overrides(mint, _base_sell_cfg_from_env(), state, features)

            # Optional AI score
            ai_score: Optional[float] = None
            if CFG.ai_enabled:
                ai_score = await _ai_sell_score(features=features, trade=trade, risk_ctx=risk_ctx)

            decision = compute_sell_decision(
                features=features,
                trade=trade,
                risk_ctx=risk_ctx,
                strat_ctx=strat_ctx,
                ai_score=ai_score,
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
            )

            # Log signals (always)
            await _log_sell_signal(
                {
                    "mint": mint,
                    "decision": decision,
                    "trade": trade,
                    "risk_ctx": risk_ctx,
                    "strat_ctx": strat_ctx,
                    "ai_score": ai_score,
                    "state": state,
                    "ts": now,
                    "version": AI_SELL_VERSION,
                    "instance": INSTANCE_ID,
                }
            )

            # DRYRUN: no enqueue
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
                        version=AI_SELL_VERSION,
                        instance=INSTANCE_ID,
                    )
                return

            action = str(decision.get("action") or "hold").lower()
            pct = _clamp(_finite(decision.get("pct"), 0.0), 0.0, 1.0)
            reason = str(decision.get("reason") or "AI_SELL_DECISION")
            sell_score = _clip01(_finite(decision.get("sell_score"), 0.0))

            # HOLD path
            if action not in ("sell", "partial") or pct <= 0.0:
                await _save_state(mint, state)
                return

            # Kill switch: block tick-driven sells if halted, but allow safety sells via schedule_force_exit
            if _kill_switch_active() and not _env_bool("AI_SELL_ALLOW_TICK_SELL_WHEN_KILLED", False):
                if CFG.debug:
                    _log("AI_SELL_BLOCKED_KILL_SWITCH", level="warning", mint=mint, reason=reason, instance=INSTANCE_ID)
                await _save_state(mint, state)
                return

            # Enqueue dedupe (prevents rapid repeated sells)
            last_enq = _finite(state.get("last_enqueue_ts"), 0.0)
            if (now - last_enq) < float(CFG.exit_dedupe_sec):
                await _save_state(mint, state)
                return

            # Amount safety: must have known position_raw
            position_raw = int(state.get("position_raw") or 0)
            reserved_raw = int(state.get("reserved_raw") or 0)
            available = max(0, position_raw - reserved_raw)
            if available <= 0:
                if CFG.debug:
                    _log("AI_SELL_NO_AVAILABLE", mint=mint, position_raw=position_raw, reserved_raw=reserved_raw, reason=reason, instance=INSTANCE_ID)
                await _save_state(mint, state)
                return

            amount_raw = int(max(1, int(available * pct)))
            amount_raw = min(amount_raw, available)

            # Optional quote sanity
            ok_quote, quote_reason = await _quote_sanity_ok(mint=mint, amount_raw=amount_raw)
            if not ok_quote:
                if CFG.debug:
                    _log("AI_SELL_QUOTE_BLOCK", level="warning", mint=mint, reason=quote_reason, instance=INSTANCE_ID)
                await _save_state(mint, state)
                return

            # Enqueue
            ok = await _enqueue_exit_request(
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
            )

            if ok:
                # Reserve to prevent double-selling before fill confirmation
                state["reserved_raw"] = int(reserved_raw + amount_raw)
                state["last_enqueue_ts"] = now
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
                    version=AI_SELL_VERSION,
                    instance=INSTANCE_ID,
                )
            else:
                await _save_state(mint, state)

    except Exception:
        return


async def schedule_force_exit(ev: Dict[str, Any]) -> None:
    """
    Forced exits: used by exit_engine follow-through fails (and other hard signals).
    Enqueues ONLY if we can prove amount_raw (event -> state -> wallet resolve).
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

            sell_pct = _clamp(_finite(ev.get("sell_pct"), 1.0), 0.01, 1.0)
            reason = str(ev.get("reason") or "force_exit")

            # amount from event
            amount_raw = _extract_amount_raw(ev)

            # else from state (available)
            if not amount_raw and state:
                pos = int(state.get("position_raw") or 0)
                resv = int(state.get("reserved_raw") or 0)
                avail = max(0, pos - resv)
                if avail > 0:
                    amount_raw = max(1, int(avail * sell_pct))

            # else from wallet resolve (optional)
            if (not amount_raw or int(amount_raw) <= 0) and CFG.resolve_amount_on_force_exit:
                resolved = await _resolve_amount_raw_from_wallet(mint)
                if resolved:
                    amount_raw = int(max(1, int(resolved * sell_pct)))
                    if state:
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

            spend_sol = _finite(ev.get("spend_sol"), _finite(state.get("entry_spend_sol") if state else 0.0, 0.0))
            follow_stats = dict(ev.get("follow_stats") or {})
            if not follow_stats:
                for k in ("up_pct", "dd_pct", "pnl_pct", "age_sec", "trades_after_entry", "exit_label", "candidate_id", "smart_hint"):
                    if k in ev:
                        follow_stats[k] = ev.get(k)

            # Optional quote sanity for forced exit can be enabled separately
            if _env_bool("AI_SELL_REQUIRE_QUOTE_SANITY_FOR_FORCE_EXIT", False):
                ok_quote, quote_reason = await _quote_sanity_ok(mint=mint, amount_raw=int(amount_raw))
                follow_stats["quote_sanity"] = quote_reason
                if not ok_quote:
                    _log("AI_SELL_FORCE_EXIT_QUOTE_BLOCK", level="warning", mint=mint, reason=quote_reason, instance=INSTANCE_ID)
                    return

            ok = await _enqueue_exit_request(
                mint=mint,
                amount_raw=int(amount_raw),
                reason=reason,
                sell_pct=float(sell_pct),
                entry_spend_sol=float(spend_sol),
                follow_stats=follow_stats,
                candidate_id=ev.get("candidate_id") or (state.get("candidate_id") if state else None),
                symbol=str(ev.get("symbol") or (state.get("symbol") if state else "") or ""),
                pool=str(ev.get("pool") or (state.get("pool") if state else "") or ""),
                slippage_bps=int(ev.get("slippage_bps") or CFG.default_slippage_bps),
                priority=int(ev.get("priority") or CFG.default_priority),
                exit_label=str(ev.get("exit_label") or ""),
                source=str(ev.get("source") or "ai_sell_logic.force_exit"),
                version=AI_SELL_VERSION,
                instance=INSTANCE_ID,
                dryrun=int(bool(CFG.dryrun)),
            )

            if ok and state:
                state["reserved_raw"] = int(int(state.get("reserved_raw") or 0) + int(amount_raw))
                state["last_enqueue_ts"] = time.time()
                await _save_state(mint, state)

            _log(
                "AI_SELL_FORCE_EXIT_ENQUEUED",
                mint=mint,
                amount_raw=int(amount_raw),
                sell_pct=float(sell_pct),
                ok=int(ok),
                reason=reason,
                version=AI_SELL_VERSION,
                instance=INSTANCE_ID,
            )

    except Exception:
        return


# Backwards-compatible aliases
async def force_exit(ev: Dict[str, Any]) -> None:
    await schedule_force_exit(ev)


async def request_exit(ev: Dict[str, Any]) -> None:
    await schedule_force_exit(ev)


async def exit_now(ev: Dict[str, Any]) -> None:
    await schedule_force_exit(ev)


# Optional: called by worker after a sell fill to release reservations / close state
async def on_sell_filled(ev: Dict[str, Any]) -> None:
    """
    If your worker can call this after an actual sell fill, we:
      - reduce reserved_raw
      - reduce position_raw (best-effort if fill amount is known)
      - delete state if position goes to zero
    """
    try:
        mint = _extract_mint(ev) or str(ev.get("mint") or "").strip()
        if not mint:
            return
        mint = mint.strip()

        amt = _extract_amount_raw(ev)
        if amt is None:
            # common keys for filled size
            for k in ("filled_amount_raw", "filled_raw", "amount_in_raw", "amountSoldRaw"):
                if ev.get(k) is None:
                    continue
                with contextlib.suppress(Exception):
                    amt = int(ev[k])
                    break

        lk = await _mint_lock(mint)
        async with lk:
            st = await _load_state(mint)
            if not st:
                return

            reserved = int(st.get("reserved_raw") or 0)
            pos = int(st.get("position_raw") or 0)

            if isinstance(amt, int) and amt > 0:
                reserved = max(0, reserved - amt)
                pos = max(0, pos - amt)
                st["reserved_raw"] = int(reserved)
                st["position_raw"] = int(pos)
            else:
                # unknown fill size: at least clear reservations if caller says full exit
                if _env_bool("AI_SELL_CLEAR_RESERVED_ON_FILL_UNKNOWN", True):
                    st["reserved_raw"] = 0

            st["last_sell_fill_ts"] = time.time()

            if int(st.get("position_raw") or 0) <= 0:
                await _delete_state(mint)
                _log("AI_SELL_STATE_CLOSED", mint=mint, version=AI_SELL_VERSION, instance=INSTANCE_ID)
                return

            await _save_state(mint, st)

    except Exception:
        return


__all__ = [
    "compute_sell_decision",
    "reload_ai_sell_config",
    "schedule_after_buy",
    "on_trade_tick",
    "schedule_force_exit",
    "force_exit",
    "request_exit",
    "exit_now",
    "on_sell_filled",
]
