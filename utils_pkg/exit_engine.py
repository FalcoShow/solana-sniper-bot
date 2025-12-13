# utils_pkg/exit_engine.py
# -*- coding: utf-8 -*-
from __future__ import annotations

"""
utils_pkg/exit_engine.py

Unified exit engine (V3+++ / quant-grade)
========================================

High-level role
---------------
Central exit coordinator that:

  • Tracks fresh positions after BUY fills (in-memory follow-through state).
  • Streams trade ticks into the AI sell brain (utils_pkg.ai_sell_logic).
  • Detects "dead" / failed follow-through coins and triggers exits.
  • Delegates real SELL execution to:
        1) ai_sell_logic.* hooks (schedule_after_buy / on_trade_tick /
           schedule_force_exit / request_exit / exit_now)
        2) utils_pkg.auto_exit_worker.exit_engine (Redis-backed worker)
        3) LAST-RESORT: direct Jupiter path via utils_pkg.lightning_impl
           **ONLY when we know token amount_raw** (never SOL lamports mistake).

  • Mirrors follow-through state into Redis for dashboards / feature_builder.
  • Logs exit signals into utils_pkg.data_logger for AI training + analytics.
  • Notifies self-learning strategy & risk manager modules about exit events.
  • (Optional) Notifies reinvestor when realized profit > 0 via on_sell_filled().

Public API (stable)
-------------------
    async def on_buy_filled(ev: Dict[str, Any]) -> None
    async def on_trade_tick(raw_event: Dict[str, Any]) -> None
    async def on_buy_filled_event(ev: Dict[str, Any]) -> None
    async def on_buy_filled_legacy(raw_id_or_mint: str, tx_sig: str) -> None

New (non-breaking, optional)
----------------------------
    async def on_sell_filled(ev: Dict[str, Any]) -> None
        # call this from your worker / strategy when a SELL fill is confirmed
        # so reinvestor + learning brains get the realized outcome

Safety guarantees
-----------------
  • If ai_sell_logic is missing → follow-through + worker/fallback still protect.
  • If Redis / worker are misconfigured → we HOLD by default; no blind sells.
  • DRYRUN short-circuits actual sells but preserves all signals / logging.
"""

import asyncio
import json
import os
import time
from dataclasses import dataclass
from typing import Any, Callable, Dict, Optional, Set, Tuple

# ──────────────────────────────────────────────────────────────────────────────
# Logger (prefer logging_setup.get_logger if present)
# ──────────────────────────────────────────────────────────────────────────────
try:
    from logging_setup import get_logger  # type: ignore
    log = get_logger("sniper.exit_engine")  # type: ignore
except Exception:  # pragma: no cover
    import logging
    log = logging.getLogger("sniper.exit_engine")
    if not log.handlers:
        logging.basicConfig(level=logging.INFO)

# ──────────────────────────────────────────────────────────────────────────────
# Env helpers
# ──────────────────────────────────────────────────────────────────────────────
def _env_bool(key: str, default: bool = False) -> bool:
    v = os.getenv(key)
    if v is None:
        return default
    return str(v).strip().lower() in ("1", "true", "t", "yes", "y", "on")

def _env_int(key: str, default: int) -> int:
    try:
        return int(str(os.getenv(key, str(default))).strip())
    except Exception:
        return default

def _env_float(key: str, default: float) -> float:
    try:
        return float(str(os.getenv(key, str(default))).strip())
    except Exception:
        return default

def _env_csv(key: str) -> Set[str]:
    raw = (os.getenv(key, "") or "").strip()
    if not raw:
        return set()
    return {p.strip().lower() for p in raw.split(",") if p.strip()}

async def _maybe_await(x: Any) -> Any:
    if asyncio.iscoroutine(x):
        return await x
    return x

# ──────────────────────────────────────────────────────────────────────────────
# Config
# ──────────────────────────────────────────────────────────────────────────────
@dataclass(frozen=True)
class ExitCfg:
    # master flags
    dryrun: bool
    enabled: bool

    # follow-through window
    follow_enabled: bool
    follow_window_sec: int
    follow_min_trades: int
    follow_min_up_pct: float
    follow_max_dd_pct: float
    follow_min_pnl_pct: float
    follow_debug: bool

    # scope / filters
    follow_pools: Set[str]
    follow_min_spend_sol: float
    follow_max_age_sec: int
    follow_mint_whitelist: Set[str]
    follow_mint_blacklist: Set[str]

    # throttling / cleanup
    tick_throttle_ms: int
    cleanup_every_sec: int

    # redis mirror + caps
    publish_redis: bool
    redis_ttl_sec: int
    max_daily_exits: int
    daily_key_prefix: str

    # labeling / logging
    exit_label: str
    log_to_data_logger: bool
    notify_self_learning: bool
    notify_risk_manager: bool

    # worker / last resort
    allow_direct_fallback: bool  # still requires token amount
    fallback_fraction: float

def _load_cfg() -> ExitCfg:
    dryrun = _env_bool("DRYRUN", False) or _env_bool("DRY_RUN", False) or _env_bool("IS_DRYRUN", False)

    flag = _env_bool("EXIT_ENGINE", True)
    enabled = flag and _env_bool("EXIT_ENGINE_ENABLED", True)

    cfg = ExitCfg(
        dryrun=dryrun,
        enabled=enabled,

        follow_enabled=_env_bool("EXIT_FOLLOW_ENABLED", True),
        follow_window_sec=_env_int("EXIT_FOLLOW_WINDOW_SEC", 35),
        follow_min_trades=_env_int("EXIT_FOLLOW_MIN_TRADES", 5),
        follow_min_up_pct=_env_float("EXIT_FOLLOW_MIN_UP_PCT", 6.0),
        follow_max_dd_pct=_env_float("EXIT_FOLLOW_MAX_DD_PCT", 5.0),
        follow_min_pnl_pct=_env_float("EXIT_FOLLOW_MIN_PNL_PCT", 0.0),
        follow_debug=_env_bool("EXIT_FOLLOW_DEBUG", False),

        follow_pools=_env_csv("EXIT_FOLLOW_POOLS"),
        follow_min_spend_sol=_env_float("EXIT_FOLLOW_MIN_SPEND_SOL", 0.0),
        follow_max_age_sec=_env_int("EXIT_FOLLOW_MAX_AGE_SEC", 30 * 60),
        follow_mint_whitelist=_env_csv("EXIT_FOLLOW_MINT_WHITELIST"),
        follow_mint_blacklist=_env_csv("EXIT_FOLLOW_MINT_BLACKLIST"),

        tick_throttle_ms=_env_int("EXIT_FOLLOW_TICK_THROTTLE_MS", 250),
        cleanup_every_sec=_env_int("EXIT_FOLLOW_CLEANUP_EVERY_SEC", 30),

        publish_redis=_env_bool("EXIT_FOLLOW_PUBLISH_REDIS", True),
        redis_ttl_sec=_env_int("EXIT_FOLLOW_REDIS_TTL_SEC", 3600),
        max_daily_exits=_env_int("EXIT_FOLLOW_MAX_DAILY_EXITS", 0),
        daily_key_prefix=str(os.getenv("EXIT_FOLLOW_DAILY_KEY_PREFIX", "exit_follow:daily")).strip() or "exit_follow:daily",

        exit_label=str(os.getenv("EXIT_FOLLOW_EXIT_LABEL", "dead_follow")).strip() or "dead_follow",
        log_to_data_logger=_env_bool("EXIT_LOG_TO_DATA_LOGGER", True),
        notify_self_learning=_env_bool("EXIT_NOTIFY_SELF_LEARNING", True),
        notify_risk_manager=_env_bool("EXIT_NOTIFY_RISK_MANAGER", True),

        allow_direct_fallback=_env_bool("EXIT_FOLLOW_ALLOW_DIRECT_FALLBACK", True),
        fallback_fraction=_env_float("EXIT_FOLLOW_PROBE_FRACTION", 0.10),
    )
    return cfg

CFG = _load_cfg()

# ──────────────────────────────────────────────────────────────────────────────
# Optional modules (best-effort)
# ──────────────────────────────────────────────────────────────────────────────
try:
    from utils_pkg import ai_sell_logic as SELL  # type: ignore[attr-defined]
except Exception:  # pragma: no cover
    SELL = None  # type: ignore[assignment]

def _get_sell_hook(name: str) -> Optional[Callable[..., Any]]:
    if SELL is None:
        return None
    fn = getattr(SELL, name, None)
    return fn if callable(fn) else None

try:
    from utils_pkg import data_logger as _DL  # type: ignore[attr-defined]
except Exception:  # pragma: no cover
    _DL = None  # type: ignore[assignment]

try:
    from utils_pkg import self_learning_ai_strategy as _SLM  # type: ignore[attr-defined]
except Exception:  # pragma: no cover
    _SLM = None  # type: ignore[assignment]

try:
    from utils_pkg import risk_manager as _RM  # type: ignore[attr-defined]
except Exception:  # pragma: no cover
    _RM = None  # type: ignore[assignment]

try:
    from utils_pkg import reinvestor as _REINV  # type: ignore[attr-defined]
except Exception:  # pragma: no cover
    _REINV = None  # type: ignore[assignment]

# Optional: smart wallets signal (does NOT change behavior unless you wire it downstream)
try:
    from utils_pkg import smart_wallets as _SW  # type: ignore[attr-defined]
except Exception:  # pragma: no cover
    _SW = None  # type: ignore[assignment]

# ──────────────────────────────────────────────────────────────────────────────
# Redis helpers (supports redis.asyncio style)
# ──────────────────────────────────────────────────────────────────────────────
def _get_redis():
    try:
        from utils_pkg.core_utils import redis as _redis  # type: ignore
        return _redis
    except Exception:
        return None

def _today_key(prefix: str) -> str:
    import datetime as _dt
    now = _dt.datetime.now(_dt.timezone.utc)
    return f"{prefix}:{now.strftime('%Y%m%d')}"

async def _redis_incr_daily(redis) -> None:
    if not redis or CFG.max_daily_exits <= 0:
        return
    key = _today_key(CFG.daily_key_prefix)
    try:
        pipe = redis.pipeline()
        pipe.incr(key)
        pipe.expire(key, 60 * 60 * 36)  # ~1.5 days
        res = pipe.execute()
        await _maybe_await(res)
    except Exception:
        return

async def _redis_get_daily(redis) -> int:
    if not redis or CFG.max_daily_exits <= 0:
        return 0
    key = _today_key(CFG.daily_key_prefix)
    try:
        raw = await _maybe_await(redis.get(key))
        return int(raw or "0")
    except Exception:
        return 0

async def _redis_set_json(redis, key: str, payload: Dict[str, Any], ttl: int) -> None:
    try:
        val = json.dumps(payload, separators=(",", ":"), ensure_ascii=False)
        # redis-py async: set(name, value, ex=ttl)
        res = redis.set(key, val, ex=ttl)
        await _maybe_await(res)
    except Exception:
        return

# ──────────────────────────────────────────────────────────────────────────────
# Optional notifications/logging
# ──────────────────────────────────────────────────────────────────────────────
async def _call_optional_hook(mod: Any, fn_candidates: Tuple[str, ...], event: Dict[str, Any]) -> None:
    if mod is None:
        return
    for name in fn_candidates:
        fn = getattr(mod, name, None)
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
    # 1) data_logger
    if CFG.log_to_data_logger and _DL is not None:
        fn = getattr(_DL, "log_exit_event", None)
        if callable(fn):
            try:
                if asyncio.iscoroutinefunction(fn):
                    await fn(event)  # type: ignore[misc]
                else:
                    await asyncio.to_thread(fn, event)  # type: ignore[misc]
            except Exception:
                pass

    # 2) self-learning
    if CFG.notify_self_learning and _SLM is not None:
        await _call_optional_hook(_SLM, ("on_exit_event", "on_forced_exit", "on_exit_signal"), event)

    # 3) risk manager
    if CFG.notify_risk_manager and _RM is not None:
        await _call_optional_hook(_RM, ("on_forced_exit", "on_exit_event"), event)

# ──────────────────────────────────────────────────────────────────────────────
# Core position state
# ──────────────────────────────────────────────────────────────────────────────
class _Pos:
    __slots__ = (
        "mint",
        "symbol",
        "pool",
        "entry_ts",
        "entry_sig",
        "spend_sol",
        "amount_raw",         # token amount in base units (preferred)
        "entry_price",
        "max_price",
        "min_price",
        "last_price",
        "trades_after_entry",
        "candidate_id",
        "closed",
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
    ) -> None:
        now = time.time()
        self.mint = mint
        self.symbol = symbol
        self.pool = pool
        self.entry_ts = now
        self.entry_sig = entry_sig
        self.spend_sol = float(spend_sol or 0.0)
        self.amount_raw = int(amount_raw) if isinstance(amount_raw, int) and amount_raw > 0 else None

        self.entry_price = float(entry_price) if isinstance(entry_price, (int, float)) and entry_price > 0 else None
        self.max_price = self.entry_price
        self.min_price = self.entry_price
        self.last_price = self.entry_price

        self.trades_after_entry = 0
        self.candidate_id = candidate_id
        self.closed = False
        self.last_tick_ts = 0.0

_OPEN: Dict[str, _Pos] = {}
_OPEN_LOCK = asyncio.Lock()
_LAST_CLEANUP_TS = 0.0

# ──────────────────────────────────────────────────────────────────────────────
# Event extraction
# ──────────────────────────────────────────────────────────────────────────────
def _pool_normalize(pool: Optional[str]) -> str:
    if not pool:
        return "unknown"
    return str(pool).strip().lower()

def _extract_mint(ev: Dict[str, Any]) -> Optional[str]:
    for key in ("mint", "mintAddress", "mint_address", "tokenMint", "id"):
        v = ev.get(key)
        if isinstance(v, str) and v:
            return v
    return None

def _extract_price(ev: Dict[str, Any]) -> Optional[float]:
    # accept many possible keys
    for key in (
        "priceUi", "price_ui", "price",
        "priceInSol", "price_in_sol",
        "ui_price", "sol_per_token", "token_price_sol",
    ):
        v = ev.get(key)
        if v is None:
            continue
        try:
            f = float(v)
            if f > 0:
                return f
        except Exception:
            continue
    return None

def _mint_ok_for_follow(mint: str, spend_sol: float, pool: str) -> bool:
    if not CFG.follow_enabled:
        return False
    if CFG.follow_pools and pool not in CFG.follow_pools:
        return False
    ml = mint.lower()
    if CFG.follow_mint_whitelist and ml not in CFG.follow_mint_whitelist:
        return False
    if CFG.follow_mint_blacklist and ml in CFG.follow_mint_blacklist:
        return False
    if spend_sol < CFG.follow_min_spend_sol:
        return False
    return True

async def _cleanup_if_needed(now: float) -> None:
    global _LAST_CLEANUP_TS
    if now - _LAST_CLEANUP_TS < CFG.cleanup_every_sec:
        return
    _LAST_CLEANUP_TS = now

    if not CFG.follow_enabled:
        return

    to_drop: list[str] = []
    async with _OPEN_LOCK:
        for mint, pos in list(_OPEN.items()):
            if pos.closed:
                to_drop.append(mint)
                continue
            age = now - pos.entry_ts
            if age > CFG.follow_max_age_sec:
                pos.closed = True
                to_drop.append(mint)
        for mint in to_drop:
            _OPEN.pop(mint, None)

    if CFG.follow_debug and to_drop:
        log.info("EXIT_FOLLOW_CLEANUP", extra={"dropped": len(to_drop)})

# ──────────────────────────────────────────────────────────────────────────────
# Redis mirror
# ──────────────────────────────────────────────────────────────────────────────
async def _publish_follow_state(*, pos: _Pos, age_sec: float, up_pct: float, dd_pct: float, pnl_pct: float, follow_pass: bool) -> None:
    if not CFG.publish_redis:
        return
    redis = _get_redis()
    if not redis:
        return
    payload = {
        "mint": pos.mint,
        "symbol": pos.symbol,
        "pool": pos.pool,
        "age_sec": age_sec,
        "trades_after_entry": pos.trades_after_entry,
        "entry_price": pos.entry_price,
        "last_price": pos.last_price,
        "up_pct": up_pct,
        "dd_pct": dd_pct,
        "pnl_pct": pnl_pct,
        "spend_sol": pos.spend_sol,
        "amount_raw": pos.amount_raw,
        "follow_pass": bool(follow_pass),
        "ts": time.time(),
        "candidate_id": pos.candidate_id,
        "window_sec": CFG.follow_window_sec,
        "min_trades": CFG.follow_min_trades,
        "min_up_pct": CFG.follow_min_up_pct,
        "max_dd_pct": CFG.follow_max_dd_pct,
        "min_pnl_pct": CFG.follow_min_pnl_pct,
    }
    # optional smart-wallet hint for dashboards (best-effort)
    if _SW is not None:
        try:
            fn = getattr(_SW, "get_mint_signal", None) or getattr(_SW, "score_mint", None)
            if callable(fn):
                sig = fn(pos.mint)  # may return dict/float
                payload["smart_wallets"] = sig
        except Exception:
            pass

    await _redis_set_json(redis, f"follow_state:{pos.mint}", payload, CFG.redis_ttl_sec)

# ──────────────────────────────────────────────────────────────────────────────
# Selling / delegation (layered)
# ──────────────────────────────────────────────────────────────────────────────
async def _delegate_force_exit(exit_ev: Dict[str, Any], *, token_amount_raw: Optional[int]) -> bool:
    """
    Try (1) AI hooks -> (2) worker enqueue -> (3) direct lightning fallback (if allowed).
    Returns True if a downstream path accepted the request.
    """
    # 1) AI engine hooks
    for hook_name in ("schedule_force_exit", "force_exit", "request_exit", "exit_now"):
        hook = _get_sell_hook(hook_name)
        if hook is None:
            continue
        try:
            await hook(exit_ev)
            log.info("EXIT_DELEGATED_AI", extra={"mint": exit_ev.get("mint"), "hook": hook_name})
            return True
        except Exception as e:
            log.warning("EXIT_AI_HOOK_FAIL", extra={"mint": exit_ev.get("mint"), "hook": hook_name, "err": str(e)[:200]})

    # 2) Worker enqueue (Redis-backed exit worker)
    try:
        from utils_pkg.auto_exit_worker import exit_engine as WORKER_ENGINE  # type: ignore
        enqueue = getattr(WORKER_ENGINE, "enqueue_exit_request", None)
        if callable(enqueue) and token_amount_raw and token_amount_raw > 0:
            await enqueue(
                mint=str(exit_ev.get("mint")),
                amount_raw=int(token_amount_raw),
                reason=str(exit_ev.get("reason") or "forced_exit"),
                sell_pct=float(exit_ev.get("sell_pct") or 1.0),
                entry_spend_sol=float(exit_ev.get("spend_sol") or 0.0),
                follow_stats=dict(exit_ev.get("follow_stats") or {}),
            )
            log.info("EXIT_DELEGATED_WORKER", extra={"mint": exit_ev.get("mint"), "amount_raw": int(token_amount_raw)})
            return True
    except Exception as e:
        log.warning("EXIT_WORKER_FAIL", extra={"mint": exit_ev.get("mint"), "err": str(e)[:200]})

    # 3) Direct fallback (only if we know token amount_raw)
    if not CFG.allow_direct_fallback:
        return False
    if not token_amount_raw or token_amount_raw <= 0:
        # Safety: never guess amounts, never use SOL lamports here.
        return False
    try:
        from utils_pkg.lightning_impl import sell_via_lightning  # type: ignore
        # Sell a fraction (probe) or full based on config
        frac = max(0.01, min(1.0, float(CFG.fallback_fraction)))
        amt = int(max(1, int(token_amount_raw * frac)))
        sig = await sell_via_lightning(
            mint=str(exit_ev.get("mint")),
            token_program=None,
            amount_raw=amt,          # token amount (base units)
            slippage_bps=int(exit_ev.get("slippage_bps") or _env_int("PF_SLIPPAGE_BPS", 200)),
        )
        log.info("EXIT_DIRECT_FALLBACK_OK", extra={"mint": exit_ev.get("mint"), "amount_raw": amt, "sig": sig})
        return True
    except Exception as e:
        log.warning("EXIT_DIRECT_FALLBACK_FAIL", extra={"mint": exit_ev.get("mint"), "err": str(e)[:200]})
        return False

# ──────────────────────────────────────────────────────────────────────────────
# BUY filled hook (primary)
# ──────────────────────────────────────────────────────────────────────────────
async def on_buy_filled(ev: Dict[str, Any]) -> None:
    if not CFG.enabled:
        log.info("EXIT_ENGINE_DISABLED", extra={"mint": ev.get("mint"), "symbol": ev.get("symbol"), "pool": ev.get("pool")})
        return

    mint = _extract_mint(ev) or ""
    tx_sig = str(ev.get("signature") or ev.get("tx_sig") or ev.get("tx") or ev.get("id") or "").strip()
    if not mint or not tx_sig:
        log.warning("EXIT_HOOK_BAD_EVENT", extra={"ev_keys": list(ev.keys())[:20]})
        return

    symbol = str(ev.get("symbol") or mint[:4]).strip()
    pool = _pool_normalize(ev.get("pool") or ev.get("source"))

    # Spend in SOL (best-effort)
    spend_sol = 0.0
    for key in ("spend_sol", "buy_size_sol", "amount_sol", "entry_size_sol"):
        if ev.get(key) is None:
            continue
        try:
            spend_sol = float(ev[key])
            break
        except Exception:
            continue

    # Token amount in base units (preferred)
    amount_raw: Optional[int] = None
    for key in ("amount_raw", "amount_out", "base_amount_raw", "token_amount_raw"):
        v = ev.get(key)
        if v is None:
            continue
        try:
            iv = int(v)
            if iv > 0:
                amount_raw = iv
                break
        except Exception:
            continue

    # Entry price
    entry_price = None
    for key in ("price_entry_ui", "entry_price", "entryPrice", "priceUi", "price_ui", "price"):
        if ev.get(key) is None:
            continue
        try:
            f = float(ev[key])
            if f > 0:
                entry_price = f
                break
        except Exception:
            continue

    candidate_id = ev.get("candidate_id") or ev.get("entry_candidate_id")

    log.info(
        "EXIT_BUY_FILLED",
        extra={
            "mint": mint,
            "symbol": symbol,
            "pool": pool,
            "spend_sol": spend_sol,
            "amount_raw": amount_raw,
            "tx_sig": tx_sig,
            "dryrun": CFG.dryrun,
            "candidate_id": candidate_id,
        },
    )

    # Track follow-through (in-memory)
    if _mint_ok_for_follow(mint, spend_sol, pool):
        async with _OPEN_LOCK:
            _OPEN[mint] = _Pos(
                mint=mint,
                symbol=symbol,
                pool=pool,
                entry_sig=tx_sig,
                spend_sol=spend_sol,
                amount_raw=amount_raw,
                entry_price=entry_price,
                candidate_id=candidate_id,
            )
        if CFG.follow_debug:
            log.info("EXIT_FOLLOW_OPEN", extra={"mint": mint, "symbol": symbol, "pool": pool, "spend_sol": spend_sol})

    # Delegate to AI sell engine for its Redis state + scheduling
    schedule_fn = _get_sell_hook("schedule_after_buy")
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
                }
            )
            await schedule_fn(payload)
            log.info("EXIT_AI_SCHEDULED_AFTER_BUY", extra={"mint": mint, "symbol": symbol})
        except Exception as e:
            log.warning("EXIT_AI_SCHEDULE_AFTER_BUY_FAIL", extra={"mint": mint, "err": str(e)[:200]})

    # DRYRUN: nothing else to do.
    return

# ──────────────────────────────────────────────────────────────────────────────
# Trade tick hook
# ──────────────────────────────────────────────────────────────────────────────
async def on_trade_tick(raw_event: Dict[str, Any]) -> None:
    if not CFG.enabled:
        return

    now = time.time()
    await _cleanup_if_needed(now)

    mint = _extract_mint(raw_event)
    if not mint:
        return

    # 1) Always forward tick to AI engine first (best-effort)
    ai_tick = _get_sell_hook("on_trade_tick")
    if ai_tick is not None:
        try:
            await ai_tick(raw_event)
        except Exception as e:
            log.warning("EXIT_AI_TICK_FAIL", extra={"mint": mint, "err": str(e)[:200]})

    if not CFG.follow_enabled:
        return

    # 2) Lookup follow-through position
    async with _OPEN_LOCK:
        pos = _OPEN.get(mint)

    if pos is None or pos.closed:
        return

    # throttle per mint to avoid CPU churn on super-hot tick streams
    if CFG.tick_throttle_ms > 0:
        if (now - pos.last_tick_ts) * 1000.0 < CFG.tick_throttle_ms:
            return
        pos.last_tick_ts = now

    age = now - pos.entry_ts
    if age > CFG.follow_max_age_sec:
        async with _OPEN_LOCK:
            pos.closed = True
            _OPEN.pop(mint, None)
        if CFG.follow_debug:
            log.info("EXIT_FOLLOW_MAX_AGE_DROP", extra={"mint": pos.mint, "symbol": pos.symbol, "age": age})
        return

    price = _extract_price(raw_event)
    pos.trades_after_entry += 1

    if price is not None:
        pos.last_price = price
        if pos.entry_price is None:
            pos.entry_price = price
            pos.max_price = price
            pos.min_price = price
        else:
            if pos.max_price is None or price > pos.max_price:
                pos.max_price = price
            if pos.min_price is None or price < pos.min_price:
                pos.min_price = price

    # still warming up: publish telemetry only
    if age < CFG.follow_window_sec:
        if CFG.follow_debug:
            log.info("EXIT_FOLLOW_WARMUP", extra={"mint": pos.mint, "age": age, "trades": pos.trades_after_entry})
        return

    # evaluate follow-through
    trades_ok = pos.trades_after_entry >= CFG.follow_min_trades

    up_pct = 0.0
    dd_pct = 0.0
    pnl_pct = 0.0

    up_ok = True
    dd_ok = True
    pnl_ok = True

    if pos.entry_price and pos.max_price:
        up_pct = (pos.max_price / pos.entry_price - 1.0) * 100.0
        up_ok = up_pct >= CFG.follow_min_up_pct

    if pos.entry_price and pos.min_price:
        dd_pct = (pos.min_price / pos.entry_price - 1.0) * 100.0  # negative below entry
        dd_ok = dd_pct >= -CFG.follow_max_dd_pct

    if pos.entry_price and pos.last_price:
        pnl_pct = (pos.last_price / pos.entry_price - 1.0) * 100.0
        if CFG.follow_min_pnl_pct > 0.0:
            pnl_ok = pnl_pct >= CFG.follow_min_pnl_pct

    follow_pass = trades_ok and up_ok and dd_ok and pnl_ok

    await _publish_follow_state(
        pos=pos,
        age_sec=age,
        up_pct=up_pct,
        dd_pct=dd_pct,
        pnl_pct=pnl_pct,
        follow_pass=follow_pass,
    )

    if CFG.follow_debug:
        log.info(
            "EXIT_FOLLOW_EVAL",
            extra={
                "mint": pos.mint,
                "symbol": pos.symbol,
                "age": age,
                "trades": pos.trades_after_entry,
                "up_pct": up_pct,
                "dd_pct": dd_pct,
                "pnl_pct": pnl_pct,
                "follow_pass": int(follow_pass),
            },
        )

    # Passed follow-through → stop tracking
    if follow_pass:
        async with _OPEN_LOCK:
            pos.closed = True
            _OPEN.pop(mint, None)
        await _log_exit_event_safe(
            {
                "mint": pos.mint,
                "symbol": pos.symbol,
                "pool": pos.pool,
                "label": "follow_ok",
                "age_sec": age,
                "trades_after_entry": pos.trades_after_entry,
                "up_pct": up_pct,
                "dd_pct": dd_pct,
                "pnl_pct": pnl_pct,
                "spend_sol": pos.spend_sol,
                "amount_raw": pos.amount_raw,
                "candidate_id": pos.candidate_id,
                "source": "exit_engine.follow_through",
            }
        )
        return

    # Failed follow-through → consider forced exit (with daily cap + DRYRUN safety)
    async with _OPEN_LOCK:
        pos.closed = True
        _OPEN.pop(mint, None)

    # daily cap
    redis = _get_redis()
    if redis and CFG.max_daily_exits > 0:
        daily = await _redis_get_daily(redis)
        if daily >= CFG.max_daily_exits:
            log.info("EXIT_FOLLOW_DAILY_CAP_REACHED", extra={"mint": pos.mint, "daily": daily, "cap": CFG.max_daily_exits})
            await _log_exit_event_safe(
                {
                    "mint": pos.mint,
                    "symbol": pos.symbol,
                    "pool": pos.pool,
                    "label": "follow_fail_daily_cap",
                    "age_sec": age,
                    "trades_after_entry": pos.trades_after_entry,
                    "up_pct": up_pct,
                    "dd_pct": dd_pct,
                    "pnl_pct": pnl_pct,
                    "spend_sol": pos.spend_sol,
                    "amount_raw": pos.amount_raw,
                    "candidate_id": pos.candidate_id,
                    "source": "exit_engine.follow_through",
                }
            )
            return

    log.info(
        "EXIT_FOLLOW_TRIGGER",
        extra={
            "mint": pos.mint,
            "symbol": pos.symbol,
            "age": age,
            "trades": pos.trades_after_entry,
            "up_pct": up_pct,
            "dd_pct": dd_pct,
            "pnl_pct": pnl_pct,
            "dryrun": CFG.dryrun,
        },
    )

    # log event for training / tuning
    exit_ev: Dict[str, Any] = {
        "mint": pos.mint,
        "symbol": pos.symbol,
        "pool": pos.pool,
        "reason": "follow_through_fail",
        "age_sec": age,
        "trades_after_entry": pos.trades_after_entry,
        "up_pct": up_pct,
        "dd_pct": dd_pct,
        "pnl_pct": pnl_pct,
        "exit_label": CFG.exit_label,
        "candidate_id": pos.candidate_id,
        "source": "exit_engine.follow_through",
        "spend_sol": pos.spend_sol,
        "sell_pct": 1.0,
        "follow_stats": {
            "up_pct": up_pct,
            "dd_pct": dd_pct,
            "pnl_pct": pnl_pct,
            "age_sec": age,
            "trades_after_entry": pos.trades_after_entry,
            "exit_label": CFG.exit_label,
            "candidate_id": pos.candidate_id,
        },
    }

    await _log_exit_event_safe({**exit_ev, "label": "follow_fail"})

    if CFG.dryrun:
        log.info("EXIT_FOLLOW_DRYRUN", extra={"mint": pos.mint, "symbol": pos.symbol})
        return

    accepted = await _delegate_force_exit(exit_ev, token_amount_raw=pos.amount_raw)
    if accepted and redis:
        await _redis_incr_daily(redis)

# ──────────────────────────────────────────────────────────────────────────────
# Optional SELL filled hook (for realized outcomes + reinvestor)
# ──────────────────────────────────────────────────────────────────────────────
async def on_sell_filled(ev: Dict[str, Any]) -> None:
    """
    Call this when a sell is CONFIRMED (worker fill / signature confirmed).
    Intended to:
      - log realized outcomes
      - notify self-learning / risk manager
      - notify reinvestor if realized profit > 0
    """
    try:
        mint = _extract_mint(ev) or str(ev.get("mint") or "").strip()
        symbol = str(ev.get("symbol") or (mint[:4] if mint else "")).strip()
        realized_pnl_sol = None

        for k in ("realized_pnl_sol", "realizedPnLSol", "pnl_sol", "pnlSol"):
            if ev.get(k) is None:
                continue
            try:
                realized_pnl_sol = float(ev[k])
                break
            except Exception:
                continue

        event = dict(ev)
        event.update(
            {
                "mint": mint,
                "symbol": symbol,
                "source": event.get("source") or "exit_engine.sell_filled",
                "ts": time.time(),
            }
        )

        await _log_exit_event_safe({**event, "label": event.get("label") or "sell_filled"})

        # reinvestor hook ONLY when realized profit > 0
        if realized_pnl_sol is not None and realized_pnl_sol > 0 and _REINV is not None:
            await _call_optional_hook(_REINV, ("on_realized_profit", "on_realized_pnl", "on_sell_filled", "on_exit_filled"), event)
    except Exception:
        return

# ──────────────────────────────────────────────────────────────────────────────
# Legacy wrappers
# ──────────────────────────────────────────────────────────────────────────────
async def on_buy_filled_event(ev: Dict[str, Any]) -> None:
    await on_buy_filled(ev)

async def on_buy_filled_legacy(raw_id_or_mint: str, tx_sig: str) -> None:
    ev: Dict[str, Any] = {
        "mint": raw_id_or_mint,
        "signature": tx_sig,
        "symbol": "",
        "spend_sol": 0.0,
        "pool": "legacy",
        "ts": time.time(),
    }
    await on_buy_filled(ev)

# ──────────────────────────────────────────────────────────────────────────────
# Diagnostics
# ──────────────────────────────────────────────────────────────────────────────
def get_open_positions_snapshot() -> Dict[str, Dict[str, Any]]:
    snap: Dict[str, Dict[str, Any]] = {}
    now = time.time()
    for mint, pos in list(_OPEN.items()):
        if pos.closed:
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
            "candidate_id": pos.candidate_id,
        }
    return snap

__all__ = [
    "on_buy_filled",
    "on_trade_tick",
    "on_buy_filled_event",
    "on_buy_filled_legacy",
    "on_sell_filled",
    "get_open_positions_snapshot",
]
