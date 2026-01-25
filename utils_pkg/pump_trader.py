from __future__ import annotations

"""
utils_pkg/pump_trader.py — V3+++ (God-Tier Overhaul, Phase 0–7) — LOCK-IN
========================================================================

Role
----
Pump.fun (and early-pool) entry orchestrator consistent with the router contract.

Deterministic gate order (never crash caller):
  0) wallet sanity
  1) kill switch / phase gate / pool gate
  2) guardrails (daily caps + breaker + pauses)                           [HARD]
  3) cooldown + dedupe + per-mint lock + global concurrency               [PACE]
  4) pump filters (optional)                                              [QUALITY]
  5) AI entry gate (optional; signature-drift safe)                       [AI]
  6) momentum gate (optional)                                             [MOMO]
  7) sizing (fixed / override / autosize / wallet-% + scalars)            [RISK]
  8) risk_manager veto/cap (optional; signature-drift safe)               [RISK+]
  9) route preflight (routeguard preferred; internal fallback)            [ROUTE]
 10) execute:
        a) Jupiter via router.buy_via_lightning (preferred)
        b) PumpPortal remote/local fallback (signature OR tx b64)
 11) post-fill: guardrails/risk_manager bookkeeping, DB, exit engine, alerts
 12) hooks + telemetry best-effort

Public contract (stable)
------------------------
maybe_buy(...) -> Optional[str] signature (or None)
buy_pump_token(...) -> Dict[str, Any] receipt
buy_direct_lamports(...) -> Dict[str, Any]
resolve_true_mint(...) helpers
close_pp_http()

Hard guarantees
---------------
- Public entrypoints never raise (exception-suppressed at edges).
- Async-only networking; no blocking I/O.
- No network at import time (lazy clients only).
- Signature drift safe integration with guardrails/risk_manager/AI/router/exit-engine.

Notes
-----
This module is intentionally defensive: it will prefer "skip a trade" over
"crash the bot". It also emits structured log.event(...) breadcrumbs so
you can *always* know why it didn't buy.
"""

import asyncio
import contextlib
import inspect
import math
import os
import random
import time
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any, Optional

import httpx

# =============================================================================
# Logging shims (LOCK-IN: root logging_setup.py is canonical)
# =============================================================================
try:
    from logging_setup import bump as _bump, get_logger  # type: ignore

    log = get_logger(__name__)  # type: ignore
except Exception:  # pragma: no cover
    import logging

    log = logging.getLogger(__name__)  # type: ignore
    _bump = None  # type: ignore

if not hasattr(log, "event"):

    def _event(name: str, **kw: Any) -> None:  # pragma: no cover
        with contextlib.suppress(Exception):
            log.info("%s %s", name, kw)

    log.event = _event

if not hasattr(log, "every"):

    def _every(
        key: str, n: int = 30, msg: str | None = None, **fields: Any
    ) -> None:  # pragma: no cover
        try:
            now = time.monotonic()
            store = getattr(_every, "_store", {})  # type: ignore[attr-defined]
            last = float(store.get(key, 0.0))
            if now - last >= float(n):
                store[key] = now
                _every._store = store  # type: ignore[attr-defined]
                try:
                    log.event(msg or key, **fields)  # type: ignore[attr-defined]
                except Exception:
                    pass
        except Exception:
            return None

    log.every = _every


def bump(k: str) -> None:
    try:
        if _bump:
            _bump(k)
    except Exception:
        pass


# =============================================================================
# Soft imports (never break caller)
# =============================================================================
try:
    from utils_pkg import router as _router  # type: ignore
except Exception:
    _router = None  # type: ignore

try:
    from utils_pkg.routeguard import preflight as rg_preflight  # type: ignore
except Exception:
    rg_preflight = None  # type: ignore

try:
    from utils_pkg.mints import normalize_mint as _normalize_mint  # type: ignore
except Exception:
    _normalize_mint = None  # type: ignore

try:
    from utils_pkg.telegram_utils import notify_trade_fill  # type: ignore
except Exception:  # pragma: no cover

    async def notify_trade_fill(*_a: Any, **_k: Any) -> None:
        return None


_wallet_err: str | None = None
try:
    from utils_pkg.solana_wallet import (  # type: ignore
        confirm_signature as _confirm_signature,
        get_balance_sol as _get_balance_sol,
        get_pubkey_str,
        load_keypair,
        sign_and_send_b64_tx as _sign_and_send_b64_tx,
    )
except Exception as e:  # pragma: no cover
    _wallet_err = str(e)
    _confirm_signature = None  # type: ignore
    _get_balance_sol = None  # type: ignore
    get_pubkey_str = None  # type: ignore
    load_keypair = None  # type: ignore
    _sign_and_send_b64_tx = None  # type: ignore

try:
    from utils_pkg.sizer import suggest_trade_sol as _auto_size_sol  # type: ignore
except Exception:
    _auto_size_sol = None  # type: ignore

try:
    from utils_pkg.liquidity import (  # type: ignore
        looks_like_mint as _liq_looks_like_mint,
        screen_token,
    )
except Exception:  # pragma: no cover
    _liq_looks_like_mint = None  # type: ignore

    async def screen_token(_: str) -> tuple[bool, dict[str, Any]]:
        return True, {"ok": True}


try:
    from utils_pkg import guardrails as _gr  # type: ignore
except Exception:
    _gr = None  # type: ignore

try:
    from utils_pkg import risk_manager as _rm  # type: ignore
except Exception:
    _rm = None  # type: ignore

try:
    from utils_pkg.dedupe import claim_once  # type: ignore
except Exception:  # pragma: no cover

    def claim_once(_key: str, _ttl: int = 60, ttl: int | None = None) -> bool:
        return True


try:
    from utils_pkg.core_utils import pg_upsert_position  # type: ignore
except Exception:  # pragma: no cover

    async def pg_upsert_position(**_kwargs: Any) -> None:
        return None


try:
    from utils_pkg.pump_filters import (  # type: ignore
        PumpStats,
        check_pump_candidate,
        check_pump_candidate_async,
    )
except Exception:  # pragma: no cover
    PumpStats = None  # type: ignore
    check_pump_candidate = None  # type: ignore
    check_pump_candidate_async = None  # type: ignore

try:
    from utils_pkg.auto_exit_worker import exit_engine as _exit_engine  # type: ignore
except Exception:
    _exit_engine = None  # type: ignore

# Optional AI gate (signature drift safe)
_AI_GATE_ERR: str | None = None
try:
    from utils_pkg.ai_buy_scoring import (  # type: ignore
        Candidate as _AICandidate,
        pass_gate as _ai_pass_gate,
    )
except Exception as e:
    _AICandidate = None  # type: ignore
    _ai_pass_gate = None  # type: ignore
    _AI_GATE_ERR = str(e)

try:
    from utils_pkg import ai_entry_logic as _ai_entry_logic  # type: ignore
except Exception:
    _ai_entry_logic = None  # type: ignore

try:
    from utils_pkg import data_logger as _data_logger  # type: ignore
except Exception:
    _data_logger = None  # type: ignore


# =============================================================================
# Small helpers
# =============================================================================
_TRUE = {"1", "true", "yes", "y", "on", "t"}
_FALSE = {"0", "false", "no", "n", "off", "f"}

LAMPORTS_PER_SOL = 1_000_000_000
LAMPORTS_PER_SOL_F = float(LAMPORTS_PER_SOL)

_BASE58 = set("123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz")


def _envs(k: str, d: str = "") -> str:
    try:
        v = os.getenv(k)
        return (v if v is not None else d).strip()
    except Exception:
        return str(d).strip()


def _envb(k: str, d: bool = False) -> bool:
    v = _envs(k, "")
    if v == "":
        return bool(d)
    s = v.lower().strip()
    if s in _TRUE:
        return True
    if s in _FALSE:
        return False
    return bool(d)


def _envf(k: str, d: float) -> float:
    try:
        return float(_envs(k, str(d)))
    except Exception:
        return float(d)


def _envi(k: str, d: int) -> int:
    try:
        return int(float(_envs(k, str(d))))
    except Exception:
        return int(d)


def _mono() -> float:
    return time.monotonic()


def _finite(x: Any, d: float = 0.0) -> float:
    try:
        v = float(x)
        if not math.isfinite(v):
            return float(d)
        return float(v)
    except Exception:
        return float(d)


def _clamp(x: float, lo: float, hi: float) -> float:
    try:
        x = float(x)
    except Exception:
        return float(lo)
    return float(lo) if x < lo else float(hi) if x > hi else float(x)


def _short_exc(e: BaseException, n: int = 260) -> str:
    s = f"{type(e).__name__}: {e}"
    return (s[:n] + "…") if len(s) > n else s


async def _maybe_await(x: Any) -> Any:
    if inspect.isawaitable(x):
        return await x
    return x


def _new_trace_id() -> str:
    return uuid.uuid4().hex[:12]


def _phase() -> int:
    return max(0, min(7, _envi("MASTER_PHASE", _envi("STRATEGY_PHASE", 7))))


def _looks_base58(s: str) -> bool:
    return bool(s) and isinstance(s, str) and all(ch in _BASE58 for ch in s)


def _strip_pseudo_suffix(raw: str) -> tuple[str, str | None]:
    s = (raw or "").strip()
    for suf in ("pump", "bonk"):
        if s.endswith(suf):
            return s[: -len(suf)].strip(), suf
    return s, None


def _is_pseudo_id(s: str) -> bool:
    return (s or "").strip().endswith(("pump", "bonk"))


def _canon_mint(m: str) -> str:
    s = (m or "").strip()
    if not s:
        return ""
    if callable(_normalize_mint):
        with contextlib.suppress(Exception):
            return str(_normalize_mint(s))
    return s


def looks_like_mint(s: str) -> bool:
    if callable(_liq_looks_like_mint):
        with contextlib.suppress(Exception):
            return bool(_liq_looks_like_mint(s))  # type: ignore[misc]
    x = (s or "").strip()
    if not x or not _looks_base58(x):
        return False
    return 28 <= len(x) <= 50


def _round_step(val: float, step: float) -> float:
    if step <= 0:
        return float(val)
    return math.floor(float(val) / step) * step


# =============================================================================
# Config (cached)
# =============================================================================
@dataclass(frozen=True)
class _Cfg:
    # mode / safety
    dryrun: bool
    kill_switch: bool
    disable_buys: bool
    min_phase: int

    # wallet
    owner_from_env: str
    min_sol_balance: float
    reserve_sol: float

    # pools
    allowed_pools: tuple[str, ...]

    # pump filters
    pf_enabled: bool
    pf_required: bool
    pf_async_enrich: bool
    pf_log_details: bool

    # ai gate
    ai_gate_enabled: bool
    ai_gate_min_score: float
    ai_gate_require: bool

    # pacing
    buy_cooldown_sec: float
    buy_cooldown_jitter_sec: float
    attempt_pace_frac: float
    max_concurrent_buys: int
    noroute_cooldown_sec: int
    per_mint_lock: bool
    hard_timeout_sec: float

    # dedupe
    dedupe_enabled: bool
    dedupe_ttl_sec: int

    # confirm
    confirm_timeout_sec: int
    confirm_commitment: str

    # sizing
    buy_sol_per_trade: float
    risk_pct_wallet: float
    autosize_refresh_sec: float
    min_sol_per_trade: float
    max_sol_per_trade: float
    step_sol: float

    # risk manager
    risk_manager_enabled: bool

    # momentum
    momo_enabled: bool
    momo_min_up_pct: float
    momo_window_sec: float
    momo_price_tries: int
    momo_price_delay_s: float
    momo_allow_no_data_for_pump: bool
    price_ttl_sec: float

    # liquidity shaping
    liq_filter_enabled: bool
    entry_min_liq_usd: float
    entry_max_liq_usd: float
    entry_min_impact_bps: int
    entry_max_impact_bps: int

    # routing / pumpportal
    pp_api_key: str
    slippage_bps: int
    dynamic_slippage: bool
    slippage_min_bps: int
    slippage_max_bps: int
    priority_sol: float
    jito_tip_sol: float
    compute_units: int
    prefer_pp_for_pump_pool: bool
    use_local_fallback: bool
    skip_preflight: bool

    # route preflight policy
    routeguard_allow_no_route: bool
    routeguard_require_sell: bool
    jup_retries: int

    # pumpportal http knobs
    pp_timeout_sec: float
    pp_retry_max: int
    pp_retry_backoff: float
    pp_max_qps: int

    # pseudo resolve
    resolve_pseudo_enabled: bool
    resolve_pseudo_timeout_sec: float

    # phase-7 logging
    log_candidates: bool
    log_trades: bool

    # telegram
    tg_enabled: bool
    tg_notify_filled: bool

    # debug
    debug: bool
    mint_lock_max: int


_CFG_TTL_SEC = max(0.5, _envf("PF_TRADER_REFRESH_SEC", 2.0))
_cfg_at: float = 0.0
_cfg_cache: _Cfg | None = None


def _cfg() -> _Cfg:
    global _cfg_at, _cfg_cache
    now = _mono()
    if _cfg_cache is not None and (now - _cfg_at) < _CFG_TTL_SEC:
        return _cfg_cache

    phase = _phase()

    dryrun = _envb("DRYRUN", _envb("DRY_RUN", _envb("IS_DRYRUN", False)))
    kill_switch = _envb("KILL_SWITCH", False) or _envb("MASTER_KILL_SWITCH", False)
    disable_buys = _envb("DISABLE_BUYS", False)
    min_phase = _envi("PF_MIN_PHASE", 1)

    owner_from_env = _envs("PF_PP_ACCOUNT", "")

    allowed_pools = tuple(
        sorted({p.strip().lower() for p in _envs("PF_POOLS", "pump").split(",") if p.strip()})
    ) or ("pump",)

    pf_enabled = _envb("PUMP_FILTER_ENABLED", True) and (phase >= 3)
    pf_required = _envb("PUMP_FILTER_REQUIRED", False)
    pf_async_enrich = _envb("PUMP_FILTER_ASYNC_ENRICH", True)
    pf_log_details = _envb("PUMP_FILTER_LOG_DETAILS", False)

    ai_gate_enabled = _envb("AI_ENTRY_GATE_ENABLED", False) and (phase >= 2)
    ai_gate_min_score = _envf("AI_ENTRY_GATE_MIN_SCORE", 0.0)
    ai_gate_require = _envb("AI_ENTRY_GATE_REQUIRED", False)

    buy_cooldown_sec = float(_envi("BUY_COOLDOWN_SEC", _envi("PF_COOLDOWN_SEC", 120)))
    buy_cooldown_jitter_sec = float(_envf("BUY_COOLDOWN_JITTER_SEC", 3.0))
    attempt_pace_frac = float(_clamp(_envf("PF_ATTEMPT_PACE_FRAC", 0.50), 0.10, 1.00))
    max_concurrent_buys = max(1, _envi("MAX_CONCURRENT_BUYS", _envi("PF_MAX_CONCURRENT", 2)))
    noroute_cooldown_sec = _envi("NOROUTE_COOLDOWN_SEC", 600)
    per_mint_lock = _envb("PF_PER_MINT_LOCK", True)
    hard_timeout_sec = max(5.0, _envf("BUY_HARD_TIMEOUT_SEC", 45.0))

    dedupe_enabled = _envb("DEDUPE_ENABLED", True)
    dedupe_ttl_sec = _envi("DEDUPE_TTL_SEC", 600)

    confirm_timeout_sec = _envi("CONFIRM_TIMEOUT_SEC", 25)
    confirm_commitment = _envs("CONFIRM_COMMITMENT", "confirmed").lower().strip() or "confirmed"

    buy_sol_per_trade = _envf("BUY_SOL_PER_TRADE", 0.0)
    risk_pct_wallet = _envf("PF_TRADE_RISK_PCT", 0.05)
    autosize_refresh_sec = _envf("PF_TRADE_AUTOSIZE_REFRESH_SEC", 10.0)
    min_sol_per_trade = _envf("PF_TRADE_MIN_SOL", _envf("ENTRY_MIN_INITIAL_BUY_SOL", 0.005))
    max_sol_per_trade = _envf("PF_TRADE_MAX_SOL", _envf("ENTRY_MAX_BUY_SOL", 0.08))
    step_sol = _envf("PF_TRADE_STEP_SOL", 0.005)

    if min_sol_per_trade > max_sol_per_trade:
        min_sol_per_trade, max_sol_per_trade = max_sol_per_trade, min_sol_per_trade

    risk_manager_enabled = _envb("RISK_MANAGER_ENABLED", True) and (phase >= 1)

    momo_enabled = _envb("ENTRY_MOMENTUM_ENABLED", True) and (phase >= 2)
    momo_min_up_pct = _envf("ENTRY_MOMO_MIN_UP_PCT", 5.0)
    momo_window_sec = _envf("ENTRY_MOMO_WINDOW_SEC", 10.0)
    momo_price_tries = max(2, _envi("ENTRY_MOMO_PRICE_TRIES", 4))
    momo_price_delay_s = _envf("ENTRY_MOMO_PRICE_DELAY_S", 0.7)
    momo_allow_no_data_for_pump = _envb("ENTRY_MOMO_ALLOW_NO_DATA_FOR_PUMP", True)
    price_ttl_sec = _envf("PF_PRICE_TTL_SEC", 5.0)

    liq_filter_enabled = _envb("ENTRY_LIQ_FILTER", True) and (phase >= 4)
    entry_min_liq_usd = _envf("ENTRY_MIN_LIQ_USD", 10_000)
    entry_max_liq_usd = _envf("ENTRY_MAX_LIQ_USD", 200_000)
    entry_min_impact_bps = _envi("ENTRY_MIN_IMPACT_BPS", 250)
    entry_max_impact_bps = _envi("ENTRY_MAX_IMPACT_BPS", 800)

    pp_api_key = _envs("PF_PP_KEY", "")
    slippage_bps = _envi("PF_SLIPPAGE_BPS", 600)
    dynamic_slippage = _envb("PF_DYNAMIC_SLIPPAGE", False)
    slippage_min_bps = _envi("PF_SLIPPAGE_MIN_BPS", 50)
    slippage_max_bps = _envi("PF_SLIPPAGE_MAX_BPS", 1500)
    priority_sol = _envf("PF_PRIORITY_SOL", 0.002)
    jito_tip_sol = _envf("PF_JITO_TIP_SOL", _envf("PF_PRIORITY_SOL", 0.002))
    compute_units = _envi("PF_COMPUTE_UNITS", 0)
    prefer_pp_for_pump_pool = _envb("PF_PREFER_PP_FOR_PUMP_POOL", False)
    use_local_fallback = _envb("PF_USE_LOCAL_FALLBACK", False)
    skip_preflight = _envb("PF_SKIP_PREFLIGHT", True)

    routeguard_allow_no_route = _envb("ROUTEGUARD_ALLOW_NO_ROUTE", True)
    routeguard_require_sell = _envb("ROUTEGUARD_REQUIRE_SELL", True) and (phase >= 3)
    jup_retries = max(1, _envi("PF_JUP_RETRIES", 2))

    pp_timeout_sec = max(6.0, _envf("PF_PP_TIMEOUT_SEC", 18.0))
    pp_retry_max = max(0, _envi("PF_PP_RETRY_MAX", 1))
    pp_retry_backoff = max(0.1, _envf("PF_PP_RETRY_BACKOFF_SEC", 0.7))
    pp_max_qps = max(1, _envi("PF_PP_MAX_QPS", 2))

    resolve_pseudo_enabled = _envb("PF_RESOLVE_PSEUDO_TO_MINT", False)
    resolve_pseudo_timeout_sec = max(0.25, _envf("PF_RESOLVE_PSEUDO_TIMEOUT_SEC", 1.25))

    log_candidates = _envb("PF_LOG_CANDIDATES", True) and (phase >= 7)
    log_trades = _envb("PF_LOG_TRADES", True) and (phase >= 7)

    tg_enabled = _envb("TELEGRAM_ENABLED", True)
    tg_notify_filled = _envb("TELEGRAM_NOTIFY_FILLED", True)

    debug = _envb("PF_DEBUG", _envb("PF_DEBUG_TX", False))
    mint_lock_max = max(128, _envi("PF_MINT_LOCK_MAX", 2048))

    min_sol_balance = _envf("MIN_SOL_BALANCE", 0.20)
    reserve_sol = _envf("PF_WALLET_RESERVE_SOL", 0.08)

    _cfg_cache = _Cfg(
        dryrun=dryrun,
        kill_switch=kill_switch,
        disable_buys=disable_buys,
        min_phase=min_phase,
        owner_from_env=owner_from_env,
        min_sol_balance=min_sol_balance,
        reserve_sol=reserve_sol,
        allowed_pools=allowed_pools,
        pf_enabled=pf_enabled,
        pf_required=pf_required,
        pf_async_enrich=pf_async_enrich,
        pf_log_details=pf_log_details,
        ai_gate_enabled=ai_gate_enabled,
        ai_gate_min_score=ai_gate_min_score,
        ai_gate_require=ai_gate_require,
        buy_cooldown_sec=buy_cooldown_sec,
        buy_cooldown_jitter_sec=buy_cooldown_jitter_sec,
        attempt_pace_frac=attempt_pace_frac,
        max_concurrent_buys=max_concurrent_buys,
        noroute_cooldown_sec=noroute_cooldown_sec,
        per_mint_lock=per_mint_lock,
        hard_timeout_sec=hard_timeout_sec,
        dedupe_enabled=dedupe_enabled,
        dedupe_ttl_sec=dedupe_ttl_sec,
        confirm_timeout_sec=confirm_timeout_sec,
        confirm_commitment=confirm_commitment,
        buy_sol_per_trade=buy_sol_per_trade,
        risk_pct_wallet=risk_pct_wallet,
        autosize_refresh_sec=autosize_refresh_sec,
        min_sol_per_trade=min_sol_per_trade,
        max_sol_per_trade=max_sol_per_trade,
        step_sol=step_sol,
        risk_manager_enabled=risk_manager_enabled,
        momo_enabled=momo_enabled,
        momo_min_up_pct=momo_min_up_pct,
        momo_window_sec=momo_window_sec,
        momo_price_tries=momo_price_tries,
        momo_price_delay_s=momo_price_delay_s,
        momo_allow_no_data_for_pump=momo_allow_no_data_for_pump,
        price_ttl_sec=price_ttl_sec,
        liq_filter_enabled=liq_filter_enabled,
        entry_min_liq_usd=entry_min_liq_usd,
        entry_max_liq_usd=entry_max_liq_usd,
        entry_min_impact_bps=entry_min_impact_bps,
        entry_max_impact_bps=entry_max_impact_bps,
        pp_api_key=pp_api_key,
        slippage_bps=slippage_bps,
        dynamic_slippage=dynamic_slippage,
        slippage_min_bps=slippage_min_bps,
        slippage_max_bps=slippage_max_bps,
        priority_sol=priority_sol,
        jito_tip_sol=jito_tip_sol,
        compute_units=compute_units,
        prefer_pp_for_pump_pool=prefer_pp_for_pump_pool,
        use_local_fallback=use_local_fallback,
        skip_preflight=skip_preflight,
        routeguard_allow_no_route=routeguard_allow_no_route,
        routeguard_require_sell=routeguard_require_sell,
        jup_retries=jup_retries,
        pp_timeout_sec=pp_timeout_sec,
        pp_retry_max=pp_retry_max,
        pp_retry_backoff=pp_retry_backoff,
        pp_max_qps=pp_max_qps,
        resolve_pseudo_enabled=resolve_pseudo_enabled,
        resolve_pseudo_timeout_sec=resolve_pseudo_timeout_sec,
        log_candidates=log_candidates,
        log_trades=log_trades,
        tg_enabled=tg_enabled,
        tg_notify_filled=tg_notify_filled,
        debug=debug,
        mint_lock_max=mint_lock_max,
    )
    _cfg_at = now
    return _cfg_cache


# =============================================================================
# Dynamic concurrency limiter
# =============================================================================
class _DynLimiter:
    """Safe dynamic concurrency limiter (no semaphore internals)."""

    def __init__(self, n: int) -> None:
        self._max = max(1, int(n))
        self._inflight = 0
        self._cv = asyncio.Condition()

    async def resize(self, n: int) -> None:
        async with self._cv:
            self._max = max(1, int(n))
            self._cv.notify_all()

    async def sync_from_cfg(self) -> None:
        await self.resize(_cfg().max_concurrent_buys)

    async def acquire(self) -> None:
        async with self._cv:
            while self._inflight >= self._max:
                await self._cv.wait()
            self._inflight += 1

    async def release(self) -> None:
        async with self._cv:
            self._inflight = max(0, self._inflight - 1)
            self._cv.notify_all()

    async def __aenter__(self) -> _DynLimiter:
        await self.acquire()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.release()


# =============================================================================
# Internal state
# =============================================================================
_last_buy_mono_global: float = 0.0
_last_attempt_mono_global: float = 0.0
_last_no_route_mono: dict[str, float] = {}

_pace_lock = asyncio.Lock()
_inflight = _DynLimiter(_cfg().max_concurrent_buys)

_last_autosize_mono: float = 0.0
_cached_wallet_sol: float = 0.0

_mint_locks: dict[str, asyncio.Lock] = {}
_mint_lock_meta: dict[str, float] = {}

_kp_cached: Any = None
_owner_cached: str = ""

_pp_client: httpx.AsyncClient | None = None
_pp_client_timeout_s: float = 0.0
_pp_rate_lock = asyncio.Lock()
_pp_last_mono: float = 0.0

_price_cache: dict[str, tuple[float, float]] = {}  # mint -> (ts_mono, price_ui)


def _cooldown_ok_mono(key: str, ttl: int) -> bool:
    now = _mono()
    last = _last_no_route_mono.get(key, 0.0)
    return (now - last) >= float(ttl)


def _note_no_route_mono(key: str) -> None:
    _last_no_route_mono[key] = _mono()


def _mint_key_for_lock(mint: str, raw_hint: str) -> str:
    base, suf = _strip_pseudo_suffix(raw_hint or mint)
    if suf and _looks_base58(base):
        return base
    return (mint or base or raw_hint or "").strip()


def _cleanup_mint_locks(max_n: int) -> None:
    try:
        if len(_mint_locks) <= max_n:
            return
        items = sorted(_mint_lock_meta.items(), key=lambda kv: kv[1])
        drop = max(1, len(_mint_locks) - max_n)
        for k, _ts in items[:drop]:
            _mint_locks.pop(k, None)
            _mint_lock_meta.pop(k, None)
    except Exception:
        pass


def _mint_lock(mint_key: str) -> asyncio.Lock:
    if mint_key not in _mint_locks:
        _mint_locks[mint_key] = asyncio.Lock()
    _mint_lock_meta[mint_key] = _mono()
    _cleanup_mint_locks(_cfg().mint_lock_max)
    return _mint_locks[mint_key]


def _wallet_ready() -> bool:
    return (
        (_wallet_err is None)
        and callable(load_keypair)
        and callable(get_pubkey_str)
        and callable(_get_balance_sol)
    )


def _get_kp_owner() -> tuple[Any, str]:
    global _kp_cached, _owner_cached
    cfg = _cfg()

    if not _wallet_ready():
        return None, cfg.owner_from_env.strip()

    if _kp_cached is None:
        _kp_cached = load_keypair()  # type: ignore[misc]

    if not _owner_cached:
        owner_from_env = cfg.owner_from_env.strip()
        _owner_cached = owner_from_env or str(get_pubkey_str(_kp_cached))  # type: ignore[misc]

    return _kp_cached, _owner_cached


async def _get_sol_balance(owner: str) -> float:
    if not _wallet_ready() or not owner:
        return 0.0
    try:
        res = _get_balance_sol(owner)  # type: ignore[misc]
        return float(await _maybe_await(res))
    except Exception:
        return 0.0


async def _sign_and_send_tx_b64(tx_b64: str, kp: Any, *, skip_preflight: bool) -> str | None:
    if not callable(_sign_and_send_b64_tx):
        return None
    try:
        res = _sign_and_send_b64_tx(tx_b64, kp, skip_preflight=skip_preflight)  # type: ignore[misc]
        sig = await _maybe_await(res)
        return str(sig) if sig else None
    except TypeError:
        with contextlib.suppress(Exception):
            res = _sign_and_send_b64_tx(tx_b64, kp)  # type: ignore[misc]
            sig = await _maybe_await(res)
            return str(sig) if sig else None
        return None
    except Exception:
        return None


# =============================================================================
# Guardrails wrappers (signature drift safe)
# =============================================================================
def _gr_buys_allowed() -> bool:
    try:
        fn = getattr(_gr, "buys_allowed", None) if _gr is not None else None
        return bool(fn()) if callable(fn) else True
    except Exception:
        return True


def _gr_block_reason() -> str:
    try:
        fn = getattr(_gr, "buy_block_reason", None) if _gr is not None else None
        return str(fn()) if callable(fn) else "blocked"
    except Exception:
        return "blocked"


def _gr_record_open(notional_sol: float, mint: str | None = None) -> None:
    try:
        fn = getattr(_gr, "record_trade_open", None) if _gr is not None else None
        if not callable(fn):
            return
        try:
            fn(float(notional_sol), mint=mint)  # type: ignore[misc]
        except TypeError:
            fn(float(notional_sol))  # type: ignore[misc]
    except Exception:
        return


def _gr_record_buy_result(
    ok: bool, reason: str, sig: str | None = None, mint: str | None = None
) -> None:
    try:
        fn = getattr(_gr, "record_buy_result", None) if _gr is not None else None
        if not callable(fn):
            return
        shapes = (
            lambda: fn(ok=ok, reason=reason, sig=sig, mint=mint),  # type: ignore[misc]
            lambda: fn(ok=ok, reason=reason, sig=sig),  # type: ignore[misc]
            lambda: fn(ok, reason, sig),  # type: ignore[misc]
            lambda: fn(ok, reason),  # type: ignore[misc]
        )
        for call in shapes:
            try:
                call()
                return
            except TypeError:
                continue
            except Exception:
                return
    except Exception:
        return


# =============================================================================
# Risk-manager wrappers (signature drift safe)
# =============================================================================
async def _rm_pretrade(
    *,
    owner: str,
    mint: str,
    pool: str,
    spend_sol: float,
    ui: dict[str, Any],
    trace: str,
) -> tuple[bool, float, str, dict[str, Any]]:
    cfg = _cfg()
    if _rm is None or not cfg.risk_manager_enabled:
        return True, float(spend_sol), "rm_disabled", {}

    try:
        candidates = [
            getattr(_rm, "pretrade", None),
            getattr(_rm, "pre_trade", None),
            getattr(_rm, "pre_trade_check", None),
            getattr(_rm, "check_entry", None),
            getattr(_rm, "allow_entry", None),
            getattr(_rm, "allow_trade", None),
            getattr(_rm, "can_trade", None),
        ]
        fns = [fn for fn in candidates if callable(fn)]
        if not fns:
            return True, float(spend_sol), "rm_no_fn", {}

        payload = {
            "owner": owner,
            "wallet": owner,
            "mint": mint,
            "pool": pool,
            "spend_sol": float(spend_sol),
            "size_sol": float(spend_sol),
            "ui": ui,
            "trace": trace,
        }

        for fn in fns:
            try:
                res = fn(**payload)  # type: ignore[misc]
                res = await _maybe_await(res)
            except TypeError:
                with contextlib.suppress(Exception):
                    res = fn(owner, mint, float(spend_sol))  # type: ignore[misc]
                    res = await _maybe_await(res)
                if "res" not in locals():
                    continue
            except Exception:
                continue

            if isinstance(res, bool):
                return bool(res), float(spend_sol), ("rm_ok" if res else "rm_block"), {}
            if isinstance(res, dict):
                ok = bool(res.get("ok", True))
                adj = float(res.get("spend_sol", res.get("size_sol", spend_sol)) or spend_sol)
                reason = str(res.get("reason", "rm"))[:140]
                extra = {
                    k: v
                    for k, v in res.items()
                    if k not in ("ok", "spend_sol", "size_sol", "reason")
                }
                return ok, float(adj), reason, extra
            if isinstance(res, list | tuple):
                if len(res) == 2:
                    return bool(res[0]), float(spend_sol), str(res[1])[:140], {}
                if len(res) >= 3:
                    ok = bool(res[0])
                    adj = float(res[1]) if res[1] is not None else float(spend_sol)
                    return ok, float(adj), str(res[2])[:140], {}

        return True, float(spend_sol), "rm_no_match", {}
    except Exception as e:
        log.event("RISK_MGR_ERR", trace=trace, mint=mint, err=_short_exc(e))
        return True, float(spend_sol), "rm_error_allowed", {}


def _rm_record_open(spend_sol: float, *, owner: str, mint: str, pool: str, trace: str) -> None:
    try:
        cfg = _cfg()
        if _rm is None or not cfg.risk_manager_enabled:
            return
        fn = (
            getattr(_rm, "record_open", None)
            or getattr(_rm, "on_open", None)
            or getattr(_rm, "record_trade_open", None)
        )
        if not callable(fn):
            return
        try:
            fn(owner=owner, mint=mint, pool=pool, spend_sol=float(spend_sol), trace=trace)  # type: ignore[misc]
        except TypeError:
            with contextlib.suppress(Exception):
                fn(float(spend_sol), mint=mint)  # type: ignore[misc]
    except Exception:
        return


def _rm_record_buy_result(
    ok: bool,
    reason: str,
    *,
    owner: str,
    mint: str,
    pool: str,
    sig: str | None,
    trace: str,
) -> None:
    try:
        cfg = _cfg()
        if _rm is None or not cfg.risk_manager_enabled:
            return
        fn = (
            getattr(_rm, "record_buy_result", None)
            or getattr(_rm, "on_buy_result", None)
            or getattr(_rm, "record_trade_result", None)
        )
        if not callable(fn):
            return
        try:
            fn(ok=ok, reason=reason, sig=sig, owner=owner, mint=mint, pool=pool, trace=trace)  # type: ignore[misc]
            return
        except TypeError:
            pass
        with contextlib.suppress(Exception):
            fn(ok, reason, sig)  # type: ignore[misc]
    except Exception:
        return


# =============================================================================
# Router wrappers (signature drift safe)
# =============================================================================
async def _router_get_price_ui(mint: str) -> float:
    if not _router:
        return 0.0
    fn = getattr(_router, "get_price_ui", None)
    if not callable(fn):
        return 0.0
    try:
        res = fn(mint)
        return float(await _maybe_await(res))
    except Exception:
        return 0.0


async def _price_ui_cached(mint: str, ttl_sec: float) -> float:
    try:
        if ttl_sec <= 0:
            return await _router_get_price_ui(mint)
        now = _mono()
        ts, px = _price_cache.get(mint, (0.0, 0.0))
        if px > 0 and (now - ts) <= ttl_sec:
            return float(px)
        px2 = float(await _router_get_price_ui(mint))
        if px2 > 0:
            _price_cache[mint] = (now, px2)
        return px2
    except Exception:
        return 0.0


async def _router_get_buy_quote(mint: str, lamports_in: int) -> dict[str, Any] | None:
    if not _router:
        return None
    fn = getattr(_router, "get_buy_quote", None)
    if not callable(fn):
        return None
    try:
        return await fn(mint=mint, lamports_in=lamports_in)  # type: ignore[misc]
    except TypeError:
        with contextlib.suppress(Exception):
            return await fn(mint=mint, amount_raw=lamports_in)  # type: ignore[misc]
        with contextlib.suppress(Exception):
            return await fn(mint, lamports_in)  # type: ignore[misc]
        return None
    except Exception:
        return None


async def _router_get_sell_quote(mint: str, amount_raw: int) -> dict[str, Any] | None:
    if not _router:
        return None
    fn = getattr(_router, "get_sell_quote", None)
    if not callable(fn):
        return None
    try:
        return await fn(mint=mint, amount_raw=amount_raw)  # type: ignore[misc]
    except TypeError:
        with contextlib.suppress(Exception):
            return await fn(mint=mint, amount=amount_raw)  # type: ignore[misc]
        with contextlib.suppress(Exception):
            return await fn(mint, amount_raw)  # type: ignore[misc]
        return None
    except Exception:
        return None


async def _router_buy_via_lightning(
    *,
    mint: str,
    amount_sol: float,
    slippage_bps: int | None,
    priority_sol: float | None,
    compute_units: int | None,
    prequote: dict[str, Any] | None = None,
) -> tuple[bool, str | None, dict[str, Any] | None]:
    """Return (ok, sig, meta)."""
    if not _router:
        return False, None, None
    fn = getattr(_router, "buy_via_lightning", None)
    if not callable(fn):
        return False, None, None

    base: dict[str, Any] = {"mint": mint, "amount_sol": float(amount_sol)}
    if slippage_bps is not None:
        base["slippage_bps"] = int(slippage_bps)
    if priority_sol is not None:
        base["priority_sol"] = float(priority_sol)
    if compute_units is not None:
        base["compute_units"] = int(compute_units)
    if isinstance(prequote, dict) and prequote:
        base["prequote"] = prequote

    attempts: list[dict[str, Any]] = [
        {**base, "return_receipt": True},
        {**base, "return_meta": True},
        {**base},
    ]

    for kwargs in attempts:
        try:
            res = await fn(**kwargs)  # type: ignore[misc]
        except TypeError:
            # common drift: priority_sol renamed to jito_tip_sol
            try:
                kwargs2 = dict(kwargs)
                if "priority_sol" in kwargs2:
                    kwargs2["jito_tip_sol"] = kwargs2.pop("priority_sol")
                res = await fn(**kwargs2)  # type: ignore[misc]
            except Exception:
                continue
        except Exception:
            continue

        if isinstance(res, dict):
            sig = res.get("sig") or res.get("signature") or res.get("txid")
            sig_s = str(sig) if sig else None
            ok = bool(res.get("ok")) if "ok" in res else bool(sig_s)
            meta = res.get("meta") if isinstance(res.get("meta"), dict) else None
            if meta is None:
                meta = {}
                for k in (
                    "base_url",
                    "endpoint_mode",
                    "in_amount_raw",
                    "out_amount_raw",
                    "priceImpactPct",
                    "slippage_bps",
                    "routeSummary",
                    "route",
                ):
                    if k in res:
                        meta[k] = res.get(k)
            return ok, sig_s, meta

        if isinstance(res, list | tuple):
            ok = bool(res[0]) if len(res) >= 1 else False
            sig_s = str(res[1]) if len(res) >= 2 and res[1] else None
            meta = res[2] if len(res) >= 3 and isinstance(res[2], dict) else None
            return ok, sig_s, meta

        if isinstance(res, str) and res:
            return True, res, None

    return False, None, None


# =============================================================================
# AI gate (optional; signature drift safe)
# =============================================================================
async def _ai_gate(
    ui: dict[str, Any], *, mint: str, pool: str, trace: str
) -> tuple[bool, float, str]:
    cfg = _cfg()
    if not cfg.ai_gate_enabled:
        return True, _finite(ui.get("ai_gate_score"), -1.0), "ai_gate_disabled"

    # honor pre-filled decision if caller provided it
    if ui.get("ai_gate_ok") is not None:
        ok = bool(ui.get("ai_gate_ok"))
        score = _finite(ui.get("ai_gate_score"), -1.0)
        reason = str(ui.get("ai_gate_reason") or ("ai_ok" if ok else "ai_block"))[:160]
        if cfg.ai_gate_min_score and score >= 0:
            ok = ok and (score >= float(cfg.ai_gate_min_score))
            if not ok:
                reason = f"ai_score<{cfg.ai_gate_min_score}"
        return ok, score, reason

    # pseudo ids often lack data — allow unless strict
    if _is_pseudo_id(mint) and not cfg.ai_gate_require:
        return True, -1.0, "ai_skip_pseudo"

    # path 1: ai_buy_scoring
    if _ai_pass_gate is not None and _AICandidate is not None:
        try:
            payload = {
                "mint": mint,
                "pool": pool,
                "ui": ui,
                "meta": ui.get("meta") if isinstance(ui.get("meta"), dict) else None,
                "trace": trace,
            }
            cand = None
            for ctor in (
                lambda: _AICandidate(**payload),  # type: ignore[misc]
                lambda: _AICandidate(mint=mint, ui=ui),  # type: ignore[misc]
                lambda: _AICandidate(mint=mint),  # type: ignore[misc]
            ):
                try:
                    cand = ctor()
                    break
                except TypeError:
                    continue

            if cand is not None:
                ok, score, why = _ai_pass_gate(cand)  # type: ignore[misc]
                ok = bool(ok)
                score_f = _finite(score, -1.0)
                why_s = str(why)[:220]
                ui["ai_gate_ok"] = ok
                ui["ai_gate_score"] = score_f
                ui["ai_gate_reason"] = why_s
                if cfg.ai_gate_min_score and score_f >= 0:
                    ok = ok and (score_f >= float(cfg.ai_gate_min_score))
                    if not ok:
                        why_s = f"ai_score<{cfg.ai_gate_min_score}"
                log.event(
                    "AI_GATE",
                    trace=trace,
                    mint=mint,
                    pool=pool,
                    ok=int(ok),
                    score=score_f,
                    why=why_s,
                )
                return ok, score_f, why_s
        except Exception as e:
            log.event("AI_GATE_ERR", trace=trace, mint=mint, err=_short_exc(e))

    # path 2: ai_entry_logic
    if _ai_entry_logic is not None:
        try:
            fn_should = getattr(_ai_entry_logic, "should_enter", None)
            fn_score = getattr(_ai_entry_logic, "score_entry", None)
            features = ui.get("features") or ui.get("feature_dict") or ui.get("feature") or None
            if isinstance(features, dict) and (callable(fn_should) or callable(fn_score)):
                score = -1.0
                if callable(fn_score):
                    with contextlib.suppress(Exception):
                        score = float(await _maybe_await(fn_score(features)))  # type: ignore[misc]

                ok = True
                why = "ai_entry_logic"
                if callable(fn_should):
                    try:
                        res = fn_should(features)  # type: ignore[misc]
                        res = await _maybe_await(res)
                        if isinstance(res, dict):
                            ok = bool(res.get("ok", True))
                            why = str(res.get("reason", why))
                            score = _finite(res.get("score", score), score)
                        elif isinstance(res, list | tuple) and res:
                            ok = bool(res[0])
                            if len(res) >= 2:
                                why = str(res[1])
                            if len(res) >= 3:
                                score = _finite(res[2], score)
                        else:
                            ok = bool(res)
                    except Exception:
                        ok = True

                ui["ai_gate_ok"] = ok
                ui["ai_gate_score"] = score
                ui["ai_gate_reason"] = why[:220]

                if cfg.ai_gate_min_score and score >= 0:
                    ok = ok and (score >= float(cfg.ai_gate_min_score))
                    if not ok:
                        why = f"ai_score<{cfg.ai_gate_min_score}"

                log.event(
                    "AI_GATE",
                    trace=trace,
                    mint=mint,
                    pool=pool,
                    ok=int(ok),
                    score=score,
                    why=why[:220],
                )
                return ok, score, why[:220]
        except Exception as e:
            log.event("AI_GATE_ERR", trace=trace, mint=mint, err=_short_exc(e))

    if cfg.ai_gate_require:
        return False, -1.0, f"ai_gate_unavailable:{_AI_GATE_ERR or 'missing'}"
    return True, -1.0, "ai_gate_unavailable_allowed"


# =============================================================================
# Momentum gate
# =============================================================================
async def _momentum_ok(mint: str, *, pool: str, trace: str) -> bool:
    cfg = _cfg()
    if not cfg.momo_enabled:
        return True

    # if no real mint address, momentum is meaningless
    if (not looks_like_mint(mint)) or _is_pseudo_id(mint):
        return bool(pool == "pump" and cfg.momo_allow_no_data_for_pump)

    ttl = float(cfg.price_ttl_sec or 0.0)
    window = float(cfg.momo_window_sec or 10.0)
    tries = max(2, int(cfg.momo_price_tries or 4))
    delay = max(float(cfg.momo_price_delay_s or 0.7), ttl + 0.10)

    px0 = await _price_ui_cached(mint, ttl_sec=ttl)
    if px0 <= 0:
        return bool(pool == "pump" and cfg.momo_allow_no_data_for_pump)

    last_px = px0
    for _ in range(tries - 1):
        await asyncio.sleep(delay)
        last_px = await _price_ui_cached(mint, ttl_sec=ttl)
        if last_px <= 0:
            return bool(pool == "pump" and cfg.momo_allow_no_data_for_pump)

    pct_up = (last_px / px0 - 1.0) * 100.0
    ok = pct_up >= float(cfg.momo_min_up_pct or 0.0)

    # If ttl ~= window, signal is noisy -> allow to avoid false negatives
    if ttl >= window * 0.9:
        ok = True

    log.event(
        "ENTRY_MOMO",
        trace=trace,
        mint=mint,
        pool=pool,
        px0=px0,
        px1=last_px,
        pct_up=round(pct_up, 2),
        ok=int(ok),
        tries=tries,
        delay_s=round(delay, 2),
        ttl_s=round(ttl, 2),
        window_s=round(window, 2),
    )
    return ok


# =============================================================================
# Liquidity-aware size modulation (optional)
# =============================================================================
async def _liquidity_size_boost(mint: str, base_sol: float, trace: str) -> float:
    cfg = _cfg()
    if not cfg.liq_filter_enabled or not looks_like_mint(mint):
        return float(base_sol)

    try:
        ok, rep = await screen_token(mint)
    except Exception as e:
        log.event("LIQ_SCREEN_ERR", trace=trace, mint=mint, err=_short_exc(e))
        ok, rep = True, {"ok": True}

    min_sol = float(cfg.min_sol_per_trade or 0.0)
    max_sol = float(cfg.max_sol_per_trade or 0.0)

    if not ok:
        return max(min_sol, min(max_sol, float(base_sol) * 0.75))

    liq_usd = float(rep.get("liqUsd") or rep.get("liq_usd") or 0.0)
    imp_bps = int(rep.get("priceImpactBps") or rep.get("impactBps") or rep.get("impact_bps") or 0)

    boost = 1.0

    if liq_usd <= 0:
        boost *= 0.80
    elif liq_usd < float(cfg.entry_min_liq_usd or 0.0):
        boost *= 0.70
    elif liq_usd > float(cfg.entry_max_liq_usd or 0.0):
        boost *= 1.05

    if imp_bps > 0:
        if imp_bps < int(cfg.entry_min_impact_bps or 0):
            boost *= 1.15
        elif imp_bps > int(cfg.entry_max_impact_bps or 10_000):
            boost *= 0.70

    sized = float(base_sol) * boost
    return max(min_sol, min(max_sol, sized))


# =============================================================================
# Autosizer fallback (wallet % sizing)
# =============================================================================
async def _autosize_by_wallet(owner: str) -> float:
    global _last_autosize_mono, _cached_wallet_sol
    cfg = _cfg()
    now = _mono()
    if (now - _last_autosize_mono) >= float(
        cfg.autosize_refresh_sec or 10.0
    ) or _cached_wallet_sol <= 0:
        _cached_wallet_sol = await _get_sol_balance(owner)
        _last_autosize_mono = now

    min_sol = float(cfg.min_sol_per_trade or 0.0)
    max_sol = float(cfg.max_sol_per_trade or 0.0)
    step = float(cfg.step_sol or 0.0)

    base = float(_cached_wallet_sol) * _clamp(float(cfg.risk_pct_wallet or 0.0), 0.0, 1.0)
    base = min(max_sol, max(min_sol, base))
    return _round_step(base, step)


# =============================================================================
# Size derivation
# =============================================================================
async def _derive_spend_sol(
    *,
    owner: str,
    wallet_bal_sol: float,
    mint: str,
    pool: str,
    ui: dict[str, Any],
    trace: str,
) -> float:
    cfg = _cfg()
    min_sol = float(cfg.min_sol_per_trade or 0.0)
    max_sol = float(cfg.max_sol_per_trade or 0.0)
    step = float(cfg.step_sol or 0.0)

    base: float | None = None

    if float(cfg.buy_sol_per_trade or 0.0) > 0:
        base = float(cfg.buy_sol_per_trade)

    if base is None:
        for k in ("size_sol_override", "spend_sol_override", "size_sol", "spend_sol"):
            if ui.get(k) is not None:
                with contextlib.suppress(Exception):
                    base = float(ui[k])
                    break

    if base is None and _auto_size_sol is not None:
        with contextlib.suppress(Exception):
            base = float(_auto_size_sol())  # type: ignore[call-arg]

    if base is None:
        base = await _autosize_by_wallet(owner)

    scalars: list[float] = []
    for k in (
        "position_size_kelly_scalar",
        "position_size_scalar",
        "size_scalar",
        "wallet_risk_scalar",
        "risk_scalar",
    ):
        if ui.get(k) is None:
            continue
        scalars.append(_finite(ui.get(k), 1.0))

    if ui.get("ai_gate_score") is not None:
        s = _finite(ui.get("ai_gate_score"), 0.0)
        scalars.append(0.60 + 0.65 * _clamp(s, 0.0, 1.0))

    if scalars:
        base *= _clamp(max(scalars), 0.05, 3.0)

    pf_score = ui.get("pump_filter_score")
    if pf_score is not None:
        s = _finite(pf_score, 0.0)
        base *= 0.65 + 0.60 * _clamp(s / 100.0, 0.0, 1.0)

    reserve = float(cfg.reserve_sol or 0.0)
    max_affordable = max(0.0, float(wallet_bal_sol) - max(0.0, reserve))
    if max_affordable > 0:
        base = min(float(base), max_affordable)

    base = max(min_sol, min(max_sol, float(base)))
    base = await _liquidity_size_boost(mint, base, trace)
    base = max(min_sol, min(max_sol, float(base)))
    base = _round_step(base, step)
    base = max(min_sol, min(max_sol, float(base)))
    return float(base)


# =============================================================================
# PumpFilters integration
# =============================================================================
def _extract_pump_stats(ui: dict[str, Any], *, mint: str) -> Any | None:
    cfg = _cfg()
    if not cfg.pf_enabled:
        return None
    if PumpStats is None or check_pump_candidate is None:
        return None

    ps = ui.get("pump_stats")
    if ps is not None:
        if PumpStats and isinstance(ps, PumpStats):  # type: ignore[arg-type]
            with contextlib.suppress(Exception):
                if not getattr(ps, "mint", ""):
                    ps.mint = mint  # type: ignore[attr-defined]
            return ps
        if isinstance(ps, dict):
            d = ps
            return PumpStats(  # type: ignore[call-arg]
                float(d.get("stage", 0.0)),
                float(d.get("liq_usd", d.get("liqUsd", 0.0))),
                float(d.get("fdv_usd", d.get("fdvUsd", 0.0))),
                int(d.get("buyers_60s", d.get("buyers60s", 0))),
                int(d.get("buys_60s", d.get("buys60s", 0))),
                float(d.get("sol_60s", d.get("sol60s", 0.0))),
                float(d.get("idle_sec", d.get("idleSec", 0.0))),
                int(d.get("good_wallets", d.get("goodWallets", 0))),
                int(d.get("bad_wallets", d.get("badWallets", 0))),
                mint=mint,  # type: ignore[call-arg]
            )

    keys = (
        "stage",
        "liq_usd",
        "fdv_usd",
        "buyers_60s",
        "buys_60s",
        "sol_60s",
        "idle_sec",
        "good_wallets",
        "bad_wallets",
    )
    if any(k in ui for k in keys):
        return PumpStats(  # type: ignore[call-arg]
            float(ui.get("stage", 0.0)),
            float(ui.get("liq_usd", ui.get("liqUsd", 0.0))),
            float(ui.get("fdv_usd", ui.get("fdvUsd", 0.0))),
            int(ui.get("buyers_60s", ui.get("buyers60s", 0))),
            int(ui.get("buys_60s", ui.get("buys60s", 0))),
            float(ui.get("sol_60s", ui.get("sol60s", 0.0))),
            float(ui.get("idle_sec", ui.get("idleSec", 0.0))),
            int(ui.get("good_wallets", ui.get("goodWallets", 0))),
            int(ui.get("bad_wallets", ui.get("badWallets", 0))),
            mint=mint,  # type: ignore[call-arg]
        )
    return None


async def _pump_filters_ok(
    ui: dict[str, Any], *, mint: str, pool: str, trace: str
) -> tuple[bool, str]:
    cfg = _cfg()
    if not cfg.pf_enabled:
        return True, "pump_filter_disabled"
    if pool != "pump":
        return True, "not_pump_pool"
    if PumpStats is None or check_pump_candidate is None:
        return True, "pump_filter_unavailable"

    stats = _extract_pump_stats(ui, mint=mint)
    if stats is None:
        return (
            (False, "pump_stats_missing")
            if cfg.pf_required
            else (True, "pump_stats_missing_allowed")
        )

    try:
        if cfg.pf_async_enrich and check_pump_candidate_async is not None:
            dec = await check_pump_candidate_async(stats)  # type: ignore[misc]
        else:
            dec = check_pump_candidate(stats)  # type: ignore[misc]
    except Exception as e:
        log.event("PUMP_FILTER_ERROR", trace=trace, mint=mint, err=_short_exc(e))
        return (
            (False, "pump_filter_error") if cfg.pf_required else (True, "pump_filter_error_allowed")
        )

    with contextlib.suppress(Exception):
        ui["pump_filter_ok"] = bool(getattr(dec, "ok", False))
        ui["pump_filter_reason"] = str(getattr(dec, "reason", ""))
        ui["pump_filter_score"] = float(getattr(dec, "score", 0.0))
        ui["pump_filter_velocity"] = float(getattr(dec, "velocity_score", 0.0))
        ui["pump_filter_sw_score"] = float(getattr(dec, "smart_wallet_score", 0.0))
        ui["pump_filter_details"] = getattr(dec, "details", {}) if hasattr(dec, "details") else {}

    ok = bool(getattr(dec, "ok", False))
    reason = str(getattr(dec, "reason", "unknown"))

    details = ui.get("pump_filter_details") if cfg.pf_log_details else None
    log.event(
        "PUMP_FILTER",
        trace=trace,
        mint=mint,
        ok=int(ok),
        reason=reason,
        score=_finite(ui.get("pump_filter_score"), 0.0),
        vel=_finite(ui.get("pump_filter_velocity"), 0.0),
        sw=_finite(ui.get("pump_filter_sw_score"), 0.0),
        details=details,
    )
    return ok, reason


# =============================================================================
# Route preflight fallback
# =============================================================================
async def _internal_preflight(
    mint: str, spend_lamports: int, trace: str
) -> tuple[bool, dict[str, Any] | None]:
    cfg = _cfg()
    q_buy = await _router_get_buy_quote(mint, spend_lamports)
    if not q_buy:
        if not cfg.routeguard_allow_no_route:
            return False, None
        log.event("ROUTE_NO_BUY_PATH_ALLOWED", trace=trace, mint=mint, lamports_in=spend_lamports)
        return True, None

    try:
        out_raw = int(q_buy.get("outAmount", 0) or q_buy.get("out_amount_raw", 0) or 0)
        if out_raw <= 0:
            if not cfg.routeguard_allow_no_route:
                return False, None
            log.event(
                "ROUTE_BUY_OUT_ZERO_ALLOWED", trace=trace, mint=mint, lamports_in=spend_lamports
            )
            q_buy = None
    except Exception:
        pass

    if cfg.routeguard_require_sell:
        q_sell = await _router_get_sell_quote(mint, 1_000)  # existence check only
        if not q_sell:
            return False, q_buy
        try:
            lam = int(q_sell.get("outAmount", 0) or q_sell.get("lamports_out", 0) or 0)
            if lam <= 0:
                return False, q_buy
        except Exception:
            return False, q_buy

    return True, q_buy


# =============================================================================
# PumpPortal client + helpers
# =============================================================================
class TemporaryPumpPortal(Exception):
    pass


def _pp_url_lightning() -> str:
    key = str(_cfg().pp_api_key or "").strip()
    if not key:
        raise RuntimeError("PF_PP_KEY missing.")
    return f"https://pumpportal.fun/api/trade?api-key={key}"


def _pp_url_local() -> str:
    key = str(_cfg().pp_api_key or "").strip()
    if not key:
        raise RuntimeError("PF_PP_KEY missing.")
    return f"https://pumpportal.fun/api/trade-local?api-key={key}"


def _pp_body(
    *,
    action: str,
    pool: str,
    payer: str,
    amount_sol: float,
    slippage_bps: int,
    priority_sol: float,
    mint: str,
) -> dict[str, Any]:
    return {
        "action": action,
        "amount": float(amount_sol),
        "denominatedInSol": True,
        "slippage": float(slippage_bps) / 100.0,  # bps -> %
        "priorityFee": float(priority_sol),
        "pool": pool,
        "publicKey": payer,
        "mint": str(mint),
    }


def _pp_errors(resp: dict[str, Any]) -> list[str]:
    errs = resp.get("errors") or resp.get("error") or resp.get("message") or []
    if isinstance(errs, dict):
        return [str(errs)]
    if isinstance(errs, list):
        return [str(e) for e in errs if e]
    return [str(errs)] if errs else []


def _pp_candidates(raw_hint: str, mint: str) -> list[tuple[str, str]]:
    out: list[tuple[str, str]] = []
    s = (raw_hint or "").strip()
    base, suf = _strip_pseudo_suffix(s)
    if suf in ("pump", "bonk"):
        out.append((s, f"as-{suf}-id"))
        if _looks_base58(base):
            out.append((base, f"as-base58-from-{suf}"))
    else:
        if _looks_base58((mint or "").strip()):
            out.append((mint.strip(), "as-mint"))
        if _looks_base58(base):
            out.append((base, "as-base58"))
        if s and s != mint:
            out.append((s, "as-raw"))

    seen = set()
    uniq: list[tuple[str, str]] = []
    for a, b in out:
        if a in seen:
            continue
        seen.add(a)
        uniq.append((a, b))
    return uniq


def _pp_client_get() -> httpx.AsyncClient:
    global _pp_client, _pp_client_timeout_s
    cfg = _cfg()
    timeout_s = float(cfg.pp_timeout_sec or 18.0)

    if _pp_client is None or _pp_client.is_closed or abs(_pp_client_timeout_s - timeout_s) > 1e-6:
        try:
            if _pp_client is not None and not _pp_client.is_closed:
                asyncio.create_task(_pp_client.aclose())
        except Exception:
            pass
        limits = httpx.Limits(max_keepalive_connections=10, max_connections=30)
        _pp_client = httpx.AsyncClient(
            timeout=timeout_s,
            follow_redirects=False,
            limits=limits,
            headers={"accept": "application/json", "user-agent": "pnlsniper"},
        )
        _pp_client_timeout_s = timeout_s
    return _pp_client


async def close_pp_http() -> None:
    global _pp_client, _pp_client_timeout_s
    if _pp_client is not None and not _pp_client.is_closed:
        with contextlib.suppress(Exception):
            await _pp_client.aclose()
    _pp_client = None
    _pp_client_timeout_s = 0.0


async def _pp_qps_gate() -> None:
    global _pp_last_mono
    cfg = _cfg()
    max_qps = float(cfg.pp_max_qps or 1.0)
    min_interval = 1.0 / max(1.0, max_qps)
    async with _pp_rate_lock:
        now = _mono()
        wait_for = min_interval - (now - _pp_last_mono)
        if wait_for > 0:
            await asyncio.sleep(wait_for)
        _pp_last_mono = _mono()


async def _pp_post_json(url: str, body: dict[str, Any], trace: str) -> dict[str, Any]:
    await _pp_qps_gate()
    c = _pp_client_get()
    r = await c.post(url, json=body)
    ctype = (r.headers.get("content-type") or "").split(";", 1)[0].strip().lower()

    if r.status_code in (429, 500, 502, 503, 504):
        head = (r.text or "")[:200].replace("\n", " ")
        raise TemporaryPumpPortal(f"transient {r.status_code}: {head or 'temporary'}")

    if r.status_code == 200 and ctype != "application/json":
        head = (r.text or "")[:200].replace("\n", " ")
        raise TemporaryPumpPortal(f"non-JSON 200 ({ctype}): {head or 'Bad Request'}")

    try:
        js = r.json()
    except Exception as err:
        head = (r.text or "")[:200].replace("\n", " ")
        raise TemporaryPumpPortal(f"bad-json {r.status_code} ({ctype}): {head}") from err

    if r.status_code != 200:
        raise RuntimeError(f"{url} {r.status_code}: {js}")
    if not isinstance(js, dict):
        raise TemporaryPumpPortal(f"unexpected-json-type: {type(js)}")

    if _cfg().debug:
        log.event("PP_HTTP_OK", trace=trace, url=url, keys=list(js.keys())[:12])
    return js


async def _pp_try(url: str, body: dict[str, Any], trace: str) -> dict[str, Any]:
    cfg = _cfg()
    rmax = int(cfg.pp_retry_max or 0)
    backoff = float(cfg.pp_retry_backoff or 0.7)
    for i in range(rmax + 1):
        try:
            return await _pp_post_json(url, body, trace)
        except TemporaryPumpPortal:
            if i < rmax:
                await asyncio.sleep(backoff * (i + 1) + random.random() * 0.15)
                continue
            raise
    raise RuntimeError("pp_try_failed")


# =============================================================================
# Hooks
# =============================================================================
SubmittedHook = Optional[Callable[[dict[str, Any]], Awaitable[None]]]
FilledHook = Optional[Callable[[dict[str, Any]], Awaitable[None]]]
FailedHook = Optional[Callable[[dict[str, Any]], Awaitable[None]]]


async def _fire(
    h: Callable[[dict[str, Any]], Awaitable[None]] | None, payload: dict[str, Any], name: str
) -> None:
    if not h:
        return
    try:
        await h(payload)
    except Exception as e:
        with contextlib.suppress(Exception):
            log.warning("%s hook failed: %s", name, e)


# =============================================================================
# Confirm helper
# =============================================================================
async def _maybe_confirm(sig: str | None, trace: str) -> str | None:
    if not sig:
        return None
    cfg = _cfg()
    tout = int(cfg.confirm_timeout_sec or 0)
    if tout <= 0:
        return sig
    try:
        fn = _confirm_signature
        if not callable(fn):
            return sig
        commitment = str(cfg.confirm_commitment or "confirmed")
        try:
            ok = await fn(sig, timeout=tout, commitment=commitment)  # type: ignore[misc]
        except TypeError:
            try:
                ok = await fn(sig, timeout=tout)  # type: ignore[misc]
            except TypeError:
                ok = await fn(sig)  # type: ignore[misc]
        if ok:
            return sig
        log.event("CONFIRM_TIMEOUT", trace=trace, sig=sig, commitment=commitment)
        return sig
    except Exception as e:
        log.event("CONFIRM_ERROR", trace=trace, sig=sig, err=_short_exc(e))
        return sig


# =============================================================================
# Optional: resolve pseudo ids to true mint (Helius)
# =============================================================================
def _helius_key() -> str | None:
    key = _envs("HELIUS_API_KEY", "")
    if key:
        return key
    sol_rpc = _envs("SOL_RPC_URL", "")
    try:
        import urllib.parse as _u

        q = _u.urlparse(sol_rpc).query
        return _u.parse_qs(q).get("api-key", [None])[0]
    except Exception:
        return None


async def resolve_true_mint_from_pseudo_id(pid: str) -> str | None:
    key = _helius_key()
    base, suf = _strip_pseudo_suffix(pid)
    if not (key and suf and _looks_base58(base)):
        return None

    url = f"https://api.helius.xyz/v0/addresses/{base}/transactions"
    params = {"api-key": key, "limit": "25"}

    try:
        async with httpx.AsyncClient(timeout=8.0) as c:
            r = await c.get(url, params=params)
            data = r.json() if r.status_code == 200 else []
    except Exception:
        return None

    for tx in data or []:
        for t in tx.get("tokenTransfers") or []:
            m = (t.get("mint") or "").strip()
            if _looks_base58(m):
                return m
        evs = tx.get("events") or {}
        if isinstance(evs, dict):
            for ev in evs.values():
                if isinstance(ev, dict):
                    for k in ("mint", "mintAddress", "mint_address", "tokenMint", "token_mint"):
                        m = (ev.get(k) or "").strip()
                        if _looks_base58(m):
                            return m
    return None


async def resolve_true_mint_from_signature(sig: str) -> str | None:
    key = _helius_key()
    if not (key and sig):
        return None

    url = "https://api.helius.xyz/v0/transactions"
    payload = {"transactions": [sig]}
    params = {"api-key": key}

    try:
        async with httpx.AsyncClient(timeout=8.0) as c:
            r = await c.post(url, params=params, json=payload)
            arr = r.json() if r.status_code == 200 else []
    except Exception:
        return None

    for tx in arr or []:
        for t in tx.get("tokenTransfers") or []:
            m = (t.get("mint") or "").strip()
            if _looks_base58(m):
                return m
        evs = tx.get("events") or {}
        if isinstance(evs, dict):
            for ev in evs.values():
                if isinstance(ev, dict):
                    for k in ("mint", "mintAddress", "mint_address", "tokenMint", "token_mint"):
                        m = (ev.get(k) or "").strip()
                        if _looks_base58(m):
                            return m
    return None


async def resolve_true_mint(pump_or_mint: str, *, signature: str | None = None) -> str | None:
    m = await resolve_true_mint_from_signature(signature) if signature else None
    if m:
        return m
    m2 = await resolve_true_mint_from_pseudo_id(pump_or_mint)
    if m2:
        return m2
    base, suf = _strip_pseudo_suffix(pump_or_mint)
    if (suf is None) and _looks_base58(base):
        return base
    return None


async def _maybe_resolve_pseudo_to_mint(raw_hint: str, mint: str, trace: str) -> str:
    cfg = _cfg()
    if not cfg.resolve_pseudo_enabled:
        return mint
    if not _is_pseudo_id(raw_hint or mint):
        return mint
    try:
        m = await asyncio.wait_for(
            resolve_true_mint_from_pseudo_id(raw_hint or mint),
            timeout=float(cfg.resolve_pseudo_timeout_sec or 1.25),
        )
        if m and looks_like_mint(m):
            return m
    except Exception as e:
        log.event("PSEUDO_RESOLVE_ERR", trace=trace, raw=raw_hint, mint=mint, err=_short_exc(e))
    return mint


# =============================================================================
# Exit-engine notification (soft; signature drift safe)
# =============================================================================
async def _notify_exit_engine_after_buy(payload: dict[str, Any]) -> None:
    try:
        ee = _exit_engine
        if ee is None:
            return
        candidates = [
            getattr(ee, "on_buy_filled", None),
            getattr(ee, "on_buy_fill", None),
            getattr(ee, "schedule_after_buy", None),
            getattr(ee, "track_new_position", None),
            getattr(ee, "register_open_position", None),
            getattr(ee, "register_position_open", None),
        ]
        fns = [fn for fn in candidates if callable(fn)]
        if not fns:
            return

        for fn in fns:
            try:
                r = fn(payload)  # type: ignore[misc]
                await _maybe_await(r)
                return
            except TypeError:
                pass
            except Exception:
                return

            try:
                r = fn(**payload)  # type: ignore[misc]
                await _maybe_await(r)
                return
            except TypeError:
                continue
            except Exception:
                return
    except Exception:
        return


# =============================================================================
# Phase-7 logging (best-effort)
# =============================================================================
async def _log_candidate_best_effort(event: dict[str, Any]) -> None:
    cfg = _cfg()
    if not (cfg.log_candidates and _data_logger):
        return
    try:
        fn = (
            getattr(_data_logger, "log_entry_candidate", None)
            or getattr(_data_logger, "log_candidate", None)
            or getattr(_data_logger, "log_entry", None)
        )
        if not callable(fn):
            return
        r = fn(event)  # type: ignore[misc]
        await _maybe_await(r)
    except Exception:
        return


async def _log_trade_best_effort(event: dict[str, Any]) -> None:
    cfg = _cfg()
    if not (cfg.log_trades and _data_logger):
        return
    try:
        fn = getattr(_data_logger, "log_trade", None) or getattr(_data_logger, "log_fill", None)
        if not callable(fn):
            return
        r = fn(event)  # type: ignore[misc]
        await _maybe_await(r)
    except Exception:
        return


# =============================================================================
# Slippage decision (optional dynamic)
# =============================================================================
def _decide_slippage_bps(ui: dict[str, Any]) -> int | None:
    cfg = _cfg()
    base = int(cfg.slippage_bps or 0)
    if base <= 0:
        return None

    bps = int(_clamp(float(base), 5, 5000))

    if not cfg.dynamic_slippage:
        return bps

    min_bps = int(cfg.slippage_min_bps or 50)
    max_bps = int(cfg.slippage_max_bps or 1500)
    min_bps = max(5, min(min_bps, max_bps))

    impact = ui.get("impact_bps") or ui.get("priceImpactBps") or ui.get("impactBps")
    vel = ui.get("pump_filter_velocity")

    if impact is not None:
        ib = int(_clamp(_finite(impact, 0.0), 0.0, 5000.0))
        bps = int(bps + _clamp(ib * 0.35, 0.0, 600.0))

    if vel is not None:
        v = _clamp(_finite(vel, 0.0), 0.0, 2.0)
        if v >= 1.0:
            bps = int(bps * 1.15)

    return int(_clamp(float(bps), float(min_bps), float(max_bps)))


# =============================================================================
# Post-fill actions
# =============================================================================
async def _post_fill(
    *,
    owner: str,
    mint: str,
    pool: str,
    symbol: str,
    spend_sol: float,
    sig: str,
    route: str,
    wf: dict[str, Any],
    trace: str,
    meta: dict[str, Any] | None = None,
    on_buy_filled: FilledHook = None,
) -> None:
    _gr_record_open(spend_sol, mint=mint)
    _gr_record_buy_result(True, f"executed_{route}", sig=sig, mint=mint)

    _rm_record_open(spend_sol, owner=owner, mint=mint, pool=pool, trace=trace)
    _rm_record_buy_result(
        True, f"executed_{route}", owner=owner, mint=mint, pool=pool, sig=sig, trace=trace
    )

    bump("buys_ok")
    log.event(
        "BUY_EXEC_OK",
        trace=trace,
        mint=mint,
        pool=pool,
        spend_sol=spend_sol,
        sig=sig,
        route=route,
        symbol=symbol or "-",
        **wf,
    )

    await _log_trade_best_effort(
        {
            "trace": trace,
            "event": "buy_fill",
            "wallet": owner,
            "mint": mint,
            "pool": pool,
            "symbol": symbol or None,
            "spend_sol": float(spend_sol),
            "sig": sig,
            "route": route,
            "meta": meta or {},
            **wf,
        }
    )

    try:
        await pg_upsert_position(
            wallet=owner,
            mint=mint,
            pool=pool,
            symbol=symbol or None,
            size_raw=None,
            size_sol=spend_sol,
            entry_price=None,
            status="open",
        )
    except Exception as e:
        with contextlib.suppress(Exception):
            log.warning("pg_upsert_position failed: %s", e)

    cfg = _cfg()
    if cfg.tg_enabled and cfg.tg_notify_filled:
        with contextlib.suppress(Exception):
            await notify_trade_fill(
                symbol=symbol or "-", side="BUY", size_sol=spend_sol, price=0.0, tx=sig
            )

    with contextlib.suppress(Exception):
        await _notify_exit_engine_after_buy(
            {
                "wallet": owner,
                "owner": owner,
                "mint": mint,
                "pool": pool,
                "symbol": symbol or None,
                "spend_sol": float(spend_sol),
                "size_sol": float(spend_sol),
                "signature": sig,
                "sig": sig,
                "route": route,
                "meta": meta or {},
                "trace": trace,
            }
        )

    await _fire(
        on_buy_filled,
        {
            "mint": mint,
            "signature": sig,
            "symbol": symbol,
            "spend_sol": spend_sol,
            "pool": pool,
            "route": route,
            "meta": meta,
            "trace": trace,
            **wf,
        },
        "on_buy_filled",
    )


# =============================================================================
# Main: maybe_buy
# =============================================================================
async def maybe_buy(
    *,
    mint: str,
    ui: dict[str, Any] | None = None,
    on_buy_submitted: SubmittedHook = None,
    on_buy_filled: FilledHook = None,
    on_buy_failed: FailedHook = None,
    worker_id: int | None = None,
    source: str | None = None,
) -> str | None:
    """
    Entry orchestrator.

    ui supports:
      - pool ("pump" default), symbol/sym, rawMint/raw
      - PumpFilters fields (pump_stats dict or individual keys)
      - sizing overrides + scalars (see _derive_spend_sol)
      - AI hints (ai_gate_ok/ai_gate_score/ai_gate_reason or features dict)
      - impact hints, velocity hints, etc (optional)
    """

    async def _run() -> str | None:
        global _last_attempt_mono_global, _last_buy_mono_global

        await _inflight.sync_from_cfg()
        cfg = _cfg()

        trace = _new_trace_id()
        phase = _phase()
        u: dict[str, Any] = dict(ui or {})

        pool = str(u.get("pool") or "pump").strip().lower()
        symbol = str(u.get("symbol") or u.get("sym") or "").strip()
        raw_hint = str(u.get("rawMint") or u.get("raw") or mint or "").strip()

        mint_in = _canon_mint(mint)
        if not mint_in:
            bump("buy_skip_bad_mint")
            _gr_record_buy_result(False, "bad_mint", mint=None)
            return None

        wf = {"worker_id": worker_id, "source": source}
        wf = {k: v for k, v in wf.items() if v is not None}

        # 0) wallet sanity
        if not _wallet_ready():
            bump("buy_skip_wallet_missing")
            log.event(
                "BUY_SKIP_WALLET_MISSING",
                trace=trace,
                mint=mint_in,
                pool=pool,
                err=_wallet_err or "wallet_missing",
                **wf,
            )
            _gr_record_buy_result(False, "wallet_missing", mint=mint_in)
            await _log_candidate_best_effort(
                {
                    "trace": trace,
                    "event": "candidate",
                    "mint": mint_in,
                    "pool": pool,
                    "ok": False,
                    "reason": "wallet_missing",
                    **wf,
                }
            )
            return None

        # 1) kill switches
        if cfg.kill_switch or cfg.disable_buys:
            bump("buy_skip_kill_switch")
            log.every("BUY_SKIP_KILL_SWITCH", 10, trace=trace, mint=mint_in, pool=pool, **wf)
            _gr_record_buy_result(False, "kill_switch", mint=mint_in)
            await _log_candidate_best_effort(
                {
                    "trace": trace,
                    "event": "candidate",
                    "mint": mint_in,
                    "pool": pool,
                    "ok": False,
                    "reason": "kill_switch",
                    **wf,
                }
            )
            return None

        # 2) phase gate
        if phase < int(cfg.min_phase or 1):
            log.event("BUY_SKIP_PHASE_OFF", trace=trace, phase=phase, mint=mint_in, pool=pool, **wf)
            _gr_record_buy_result(False, "phase_off", mint=mint_in)
            await _log_candidate_best_effort(
                {
                    "trace": trace,
                    "event": "candidate",
                    "mint": mint_in,
                    "pool": pool,
                    "ok": False,
                    "reason": "phase_off",
                    "phase": phase,
                    **wf,
                }
            )
            return None

        # 3) pool gate
        if pool not in set(cfg.allowed_pools or ("pump",)):
            log.event(
                "BUY_SKIP_UNSUPPORTED_POOL",
                trace=trace,
                pool=pool,
                mint=mint_in,
                allowed=list(cfg.allowed_pools),
                **wf,
            )
            _gr_record_buy_result(False, "unsupported_pool", mint=mint_in)
            await _log_candidate_best_effort(
                {
                    "trace": trace,
                    "event": "candidate",
                    "mint": mint_in,
                    "pool": pool,
                    "ok": False,
                    "reason": "unsupported_pool",
                    **wf,
                }
            )
            return None

        # 4) guardrails gate
        if not _gr_buys_allowed():
            reason = _gr_block_reason()
            log.event(
                "BUY_BLOCKED_GUARDRAILS", trace=trace, mint=mint_in, pool=pool, reason=reason, **wf
            )
            _gr_record_buy_result(False, f"guardrails:{reason}", mint=mint_in)
            bump("buy_skip_guardrails")
            await _log_candidate_best_effort(
                {
                    "trace": trace,
                    "event": "candidate",
                    "mint": mint_in,
                    "pool": pool,
                    "ok": False,
                    "reason": f"guardrails:{reason}",
                    **wf,
                }
            )
            return None

        mint_key = _mint_key_for_lock(mint_in, raw_hint)

        # 5) global cooldown (attempt pacing + fill pacing)
        cooldown = float(cfg.buy_cooldown_sec or 0.0) + random.random() * max(
            0.0, float(cfg.buy_cooldown_jitter_sec or 0.0)
        )
        attempt_cd = max(2.0, cooldown * float(cfg.attempt_pace_frac or 0.5))

        async with _pace_lock:
            nowm = _mono()
            if (nowm - _last_attempt_mono_global) < attempt_cd:
                secs_left = int(max(0.0, attempt_cd - (nowm - _last_attempt_mono_global)))
                log.every(
                    "BUY_COOLDOWN_ATTEMPT",
                    5,
                    trace=trace,
                    mint=mint_in,
                    pool=pool,
                    secs_left=secs_left,
                    **wf,
                )
                _gr_record_buy_result(False, "attempt_cooldown", mint=mint_in)
                bump("buy_skip_cooldown")
                await _log_candidate_best_effort(
                    {
                        "trace": trace,
                        "event": "candidate",
                        "mint": mint_in,
                        "pool": pool,
                        "ok": False,
                        "reason": "attempt_cooldown",
                        "secs_left": secs_left,
                        **wf,
                    }
                )
                return None

            if (nowm - _last_buy_mono_global) < cooldown:
                secs_left = int(max(0.0, cooldown - (nowm - _last_buy_mono_global)))
                log.every(
                    "BUY_COOLDOWN_GLOBAL",
                    5,
                    trace=trace,
                    mint=mint_in,
                    pool=pool,
                    secs_left=secs_left,
                    **wf,
                )
                _gr_record_buy_result(False, "global_cooldown", mint=mint_in)
                bump("buy_skip_cooldown")
                await _log_candidate_best_effort(
                    {
                        "trace": trace,
                        "event": "candidate",
                        "mint": mint_in,
                        "pool": pool,
                        "ok": False,
                        "reason": "global_cooldown",
                        "secs_left": secs_left,
                        **wf,
                    }
                )
                return None

            _last_attempt_mono_global = nowm

        # 6) noroute cooldown per mint_key
        if mint_key in _last_no_route_mono and not _cooldown_ok_mono(
            mint_key, int(cfg.noroute_cooldown_sec or 600)
        ):
            log.every(
                "BUY_SKIP_NOROUTE_COOLDOWN",
                10,
                trace=trace,
                mint=mint_in,
                mint_key=mint_key,
                pool=pool,
                **wf,
            )
            _gr_record_buy_result(False, "noroute_cooldown", mint=mint_in)
            bump("buy_skip_noroute_cooldown")
            await _log_candidate_best_effort(
                {
                    "trace": trace,
                    "event": "candidate",
                    "mint": mint_in,
                    "pool": pool,
                    "ok": False,
                    "reason": "noroute_cooldown",
                    "mint_key": mint_key,
                    **wf,
                }
            )
            return None

        # 7) dedupe
        if cfg.dedupe_enabled:
            ok_claim = True
            try:
                ok_claim = bool(
                    claim_once(f"buy:{pool}:{mint_key}", ttl=int(cfg.dedupe_ttl_sec or 600))
                )  # type: ignore[misc]
            except TypeError:
                with contextlib.suppress(Exception):
                    ok_claim = bool(
                        claim_once(f"buy:{pool}:{mint_key}", _ttl=int(cfg.dedupe_ttl_sec or 600))
                    )  # type: ignore[misc]
            except Exception:
                ok_claim = True

            if not ok_claim:
                log.every(
                    "BUY_SKIP_DEDUPE",
                    10,
                    trace=trace,
                    mint=mint_in,
                    mint_key=mint_key,
                    pool=pool,
                    **wf,
                )
                _gr_record_buy_result(False, "dedupe", mint=mint_in)
                bump("buy_skip_dedupe")
                await _log_candidate_best_effort(
                    {
                        "trace": trace,
                        "event": "candidate",
                        "mint": mint_in,
                        "pool": pool,
                        "ok": False,
                        "reason": "dedupe",
                        "mint_key": mint_key,
                        **wf,
                    }
                )
                return None

        mint_lock = _mint_lock(mint_key) if cfg.per_mint_lock else None

        async def _core() -> str | None:
            nonlocal cfg
            cfg = _cfg()

            kp, owner = _get_kp_owner()
            if not owner:
                bump("buy_skip_owner_missing")
                log.event("BUY_SKIP_OWNER_MISSING", trace=trace, mint=mint_in, pool=pool, **wf)
                _gr_record_buy_result(False, "owner_missing", mint=mint_in)
                await _log_candidate_best_effort(
                    {
                        "trace": trace,
                        "event": "candidate",
                        "mint": mint_in,
                        "pool": pool,
                        "ok": False,
                        "reason": "owner_missing",
                        **wf,
                    }
                )
                return None

            bal = await _get_sol_balance(owner)
            if bal < float(cfg.min_sol_balance or 0.0):
                log.event(
                    "BUY_BLOCK_LOW_BAL",
                    trace=trace,
                    owner=owner,
                    bal_sol=bal,
                    min_sol=float(cfg.min_sol_balance or 0.0),
                    mint=mint_in,
                    pool=pool,
                    **wf,
                )
                _gr_record_buy_result(False, "low_balance", mint=mint_in)
                bump("buy_skip_low_balance")
                await _log_candidate_best_effort(
                    {
                        "trace": trace,
                        "event": "candidate",
                        "wallet": owner,
                        "mint": mint_in,
                        "pool": pool,
                        "ok": False,
                        "reason": "low_balance",
                        "bal_sol": bal,
                        **wf,
                    }
                )
                return None

            # pump filters
            ok_pf, pf_reason = await _pump_filters_ok(u, mint=mint_in, pool=pool, trace=trace)
            if not ok_pf:
                log.event(
                    "BUY_BLOCK_PUMP_FILTER",
                    trace=trace,
                    mint=mint_in,
                    pool=pool,
                    reason=pf_reason,
                    **wf,
                )
                _gr_record_buy_result(False, f"pump_filter:{pf_reason}", mint=mint_in)
                bump("buy_skip_pump_filter")
                await _log_candidate_best_effort(
                    {
                        "trace": trace,
                        "event": "candidate",
                        "wallet": owner,
                        "mint": mint_in,
                        "pool": pool,
                        "ok": False,
                        "reason": f"pump_filter:{pf_reason}",
                        **wf,
                    }
                )
                return None

            # resolve pseudo ids
            mint_for_routing = await _maybe_resolve_pseudo_to_mint(raw_hint, mint_in, trace)
            mint_for_routing = _canon_mint(mint_for_routing)
            if mint_for_routing and mint_for_routing != mint_in:
                log.event(
                    "PSEUDO_RESOLVED",
                    trace=trace,
                    raw=raw_hint,
                    mint=mint_in,
                    resolved=mint_for_routing,
                    **wf,
                )

            if not mint_for_routing:
                _gr_record_buy_result(False, "bad_mint_after_resolve", mint=mint_in)
                bump("buy_skip_bad_mint")
                await _log_candidate_best_effort(
                    {
                        "trace": trace,
                        "event": "candidate",
                        "wallet": owner,
                        "mint": mint_in,
                        "pool": pool,
                        "ok": False,
                        "reason": "bad_mint_after_resolve",
                        **wf,
                    }
                )
                return None

            # AI gate
            ok_ai, ai_score, ai_reason = await _ai_gate(
                u, mint=mint_for_routing, pool=pool, trace=trace
            )
            if not ok_ai:
                log.event(
                    "BUY_BLOCK_AI_GATE",
                    trace=trace,
                    mint=mint_for_routing,
                    pool=pool,
                    reason=ai_reason,
                    score=ai_score,
                    **wf,
                )
                _gr_record_buy_result(False, f"ai_gate:{ai_reason}", mint=mint_for_routing)
                bump("buy_skip_ai_gate")
                await _log_candidate_best_effort(
                    {
                        "trace": trace,
                        "event": "candidate",
                        "wallet": owner,
                        "mint": mint_for_routing,
                        "pool": pool,
                        "ok": False,
                        "reason": f"ai_gate:{ai_reason}",
                        "ai_score": ai_score,
                        **wf,
                    }
                )
                return None

            # momentum
            if not await _momentum_ok(mint_for_routing, pool=pool, trace=trace):
                log.event("BUY_BLOCK_MOMENTUM", trace=trace, mint=mint_for_routing, pool=pool, **wf)
                _gr_record_buy_result(False, "momo_reject", mint=mint_for_routing)
                bump("buy_skip_momo")
                await _log_candidate_best_effort(
                    {
                        "trace": trace,
                        "event": "candidate",
                        "wallet": owner,
                        "mint": mint_for_routing,
                        "pool": pool,
                        "ok": False,
                        "reason": "momo_reject",
                        **wf,
                    }
                )
                return None

            # size
            spend_sol = await _derive_spend_sol(
                owner=owner, wallet_bal_sol=bal, mint=mint_for_routing, pool=pool, ui=u, trace=trace
            )

            reserve = float(cfg.reserve_sol or 0.0)
            max_afford = max(0.0, float(bal) - max(0.0, reserve))
            if max_afford > 0:
                spend_sol = min(float(spend_sol), float(max_afford))

            spend_sol = min(
                float(cfg.max_sol_per_trade or spend_sol),
                max(float(cfg.min_sol_per_trade or spend_sol), float(spend_sol)),
            )
            if spend_sol <= 0:
                _gr_record_buy_result(False, "size_zero", mint=mint_for_routing)
                bump("buy_skip_size_zero")
                await _log_candidate_best_effort(
                    {
                        "trace": trace,
                        "event": "candidate",
                        "wallet": owner,
                        "mint": mint_for_routing,
                        "pool": pool,
                        "ok": False,
                        "reason": "size_zero",
                        **wf,
                    }
                )
                return None

            # risk-manager pretrade
            ok_rm, spend_adj, rm_reason, rm_extra = await _rm_pretrade(
                owner=owner,
                mint=mint_for_routing,
                pool=pool,
                spend_sol=float(spend_sol),
                ui=u,
                trace=trace,
            )
            if not ok_rm:
                log.event(
                    "BUY_BLOCK_RISK_MGR",
                    trace=trace,
                    mint=mint_for_routing,
                    pool=pool,
                    reason=rm_reason,
                    spend_sol=spend_sol,
                    **wf,
                )
                _gr_record_buy_result(False, f"risk_manager:{rm_reason}", mint=mint_for_routing)
                bump("buy_skip_risk_manager")
                await _log_candidate_best_effort(
                    {
                        "trace": trace,
                        "event": "candidate",
                        "wallet": owner,
                        "mint": mint_for_routing,
                        "pool": pool,
                        "ok": False,
                        "reason": f"risk_manager:{rm_reason}",
                        "spend_sol": spend_sol,
                        **wf,
                    }
                )
                return None

            if spend_adj > 0:
                spend_sol = float(spend_adj)

            spend_sol = min(
                float(cfg.max_sol_per_trade or spend_sol),
                max(float(cfg.min_sol_per_trade or spend_sol), float(spend_sol)),
            )
            lamports_in = int(spend_sol * LAMPORTS_PER_SOL)
            if lamports_in <= 0:
                _gr_record_buy_result(False, "lamports_zero", mint=mint_for_routing)
                bump("buy_skip_size_zero")
                await _log_candidate_best_effort(
                    {
                        "trace": trace,
                        "event": "candidate",
                        "wallet": owner,
                        "mint": mint_for_routing,
                        "pool": pool,
                        "ok": False,
                        "reason": "lamports_zero",
                        **wf,
                    }
                )
                return None

            # candidate log snapshot (pre-route)
            await _log_candidate_best_effort(
                {
                    "trace": trace,
                    "event": "candidate",
                    "wallet": owner,
                    "mint": mint_for_routing,
                    "mint_key": mint_key,
                    "pool": pool,
                    "symbol": symbol or None,
                    "ok": True,
                    "spend_sol": float(spend_sol),
                    "wallet_sol": float(bal),
                    "ai_gate_score": _finite(
                        u.get("ai_gate_score"), ai_score if ai_score >= 0 else -1.0
                    ),
                    "ai_gate_reason": str(u.get("ai_gate_reason") or ai_reason)[:160],
                    "pf_score": _finite(u.get("pump_filter_score"), -1.0),
                    "pf_reason": str(u.get("pump_filter_reason") or ""),
                    "rm_reason": rm_reason,
                    **(rm_extra or {}),
                    **wf,
                }
            )

            log.event(
                "BUY_SIZE_DECISION",
                trace=trace,
                mint=mint_for_routing,
                mint_key=mint_key,
                pool=pool,
                spend_sol=spend_sol,
                wallet_sol=bal,
                ai_gate_score=_finite(u.get("ai_gate_score"), ai_score if ai_score >= 0 else -1.0),
                ai_gate_reason=str(u.get("ai_gate_reason") or ai_reason)[:160],
                pf_score=_finite(u.get("pump_filter_score"), -1.0),
                pf_reason=str(u.get("pump_filter_reason") or ""),
                rm_reason=rm_reason,
                **(rm_extra or {}),
                **wf,
            )

            # DRYRUN
            if cfg.dryrun:
                await _fire(
                    on_buy_submitted,
                    {
                        "mint": mint_for_routing,
                        "mint_key": mint_key,
                        "pool": pool,
                        "symbol": symbol,
                        "spend_sol": spend_sol,
                        "route": "dryrun",
                        "trace": trace,
                        **wf,
                    },
                    "on_buy_submitted",
                )
                log.event(
                    "DRYRUN_BUY_EXEC",
                    trace=trace,
                    mint=mint_for_routing,
                    pool=pool,
                    spend_sol=spend_sol,
                    **wf,
                )
                bump("buys_dryrun")
                _gr_record_buy_result(True, "dryrun", mint=mint_for_routing)
                async with _pace_lock:
                    _last_buy_mono_global = _mono()
                return None

            # re-check guardrails right before execution
            if not _gr_buys_allowed():
                reason = _gr_block_reason()
                log.event(
                    "BUY_BLOCKED_GUARDRAILS_PRE_EXEC",
                    trace=trace,
                    mint=mint_for_routing,
                    pool=pool,
                    reason=reason,
                    **wf,
                )
                _gr_record_buy_result(False, f"guardrails:{reason}", mint=mint_for_routing)
                bump("buy_skip_guardrails")
                return None

            # choose preferred route
            skip_jupiter = (
                _is_pseudo_id(raw_hint or mint_in)
                or (not looks_like_mint(mint_for_routing))
                or (pool == "pump" and cfg.prefer_pp_for_pump_pool)
            )

            allow_buy = True
            q_buy: dict[str, Any] | None = None

            if not skip_jupiter:
                if rg_preflight:
                    try:
                        try:
                            allow_buy, q_buy = await rg_preflight(
                                mint=mint_for_routing, lamports_in=lamports_in
                            )  # type: ignore[misc]
                        except TypeError:
                            allow_buy, q_buy = await rg_preflight(mint_for_routing, lamports_in)  # type: ignore[misc]
                    except Exception as e:
                        log.event(
                            "ROUTEGUARD_ERROR",
                            trace=trace,
                            mint=mint_for_routing,
                            err=_short_exc(e),
                            **wf,
                        )
                        allow_buy, q_buy = True, None
                else:
                    allow_buy, q_buy = await _internal_preflight(
                        mint_for_routing, lamports_in, trace
                    )

            if not allow_buy:
                _note_no_route_mono(mint_key)
                _gr_record_buy_result(False, "routeguard_block", mint=mint_for_routing)
                bump("buy_skip_no_route")
                log.event(
                    "BUY_BLOCK_ROUTE",
                    trace=trace,
                    mint=mint_for_routing,
                    pool=pool,
                    reason="routeguard_block",
                    **wf,
                )
                await _fire(
                    on_buy_failed,
                    {
                        "mint": mint_for_routing,
                        "mint_key": mint_key,
                        "pool": pool,
                        "symbol": symbol,
                        "spend_sol": spend_sol,
                        "route": "preflight",
                        "reason": "routeguard_block",
                        "trace": trace,
                        **wf,
                    },
                    "on_buy_failed",
                )
                return None

            jupiter_path_allowed = (not skip_jupiter) and allow_buy and (q_buy is not None)

            async with _inflight:
                # ---------------------------------------------
                # Preferred: Jupiter buy
                # ---------------------------------------------
                if jupiter_path_allowed:
                    await _fire(
                        on_buy_submitted,
                        {
                            "mint": mint_for_routing,
                            "mint_key": mint_key,
                            "pool": pool,
                            "symbol": symbol,
                            "spend_sol": spend_sol,
                            "route": "jupiter",
                            "hint": None,
                            "ai_gate_score": _finite(
                                u.get("ai_gate_score"), ai_score if ai_score >= 0 else -1.0
                            ),
                            "ai_gate_reason": str(u.get("ai_gate_reason") or ai_reason)[:160],
                            "pf_score": _finite(u.get("pump_filter_score"), -1.0),
                            "pf_reason": str(u.get("pump_filter_reason") or ""),
                            "trace": trace,
                            **wf,
                        },
                        "on_buy_submitted",
                    )

                    slippage_bps = _decide_slippage_bps(u)
                    jito_tip = (
                        float(cfg.jito_tip_sol) if float(cfg.jito_tip_sol or 0.0) > 0 else None
                    )
                    compute_units = (
                        int(cfg.compute_units) if int(cfg.compute_units or 0) > 0 else None
                    )

                    sig: str | None = None
                    meta: dict[str, Any] | None = None

                    for i in range(int(cfg.jup_retries or 1)):
                        if not _gr_buys_allowed():
                            reason = _gr_block_reason()
                            _gr_record_buy_result(
                                False, f"guardrails:{reason}", mint=mint_for_routing
                            )
                            log.event(
                                "BUY_BLOCKED_GUARDRAILS_AT_EXEC",
                                trace=trace,
                                mint=mint_for_routing,
                                pool=pool,
                                reason=reason,
                                route="jupiter",
                                **wf,
                            )
                            bump("buy_skip_guardrails")
                            return None

                        ok2, sig, meta = await _router_buy_via_lightning(
                            mint=mint_for_routing,
                            amount_sol=spend_sol,
                            slippage_bps=slippage_bps,
                            priority_sol=jito_tip,
                            compute_units=compute_units,
                            prequote=q_buy,
                        )
                        if ok2 and sig:
                            break
                        await asyncio.sleep(0.35 * (i + 1) + random.random() * 0.15)

                    if sig:
                        sig = await _maybe_confirm(sig, trace)
                        async with _pace_lock:
                            _last_buy_mono_global = _mono()
                        await _post_fill(
                            owner=owner,
                            mint=mint_for_routing,
                            pool=pool,
                            symbol=symbol,
                            spend_sol=spend_sol,
                            sig=str(sig),
                            route="jupiter",
                            wf=wf,
                            trace=trace,
                            meta=meta,
                            on_buy_filled=on_buy_filled,
                        )
                        return str(sig)

                    log.event(
                        "BUY_SWAP_FAIL",
                        trace=trace,
                        mint=mint_for_routing,
                        pool=pool,
                        err="router_fail",
                        route="jupiter",
                        **wf,
                    )
                    _gr_record_buy_result(False, "router_fail", mint=mint_for_routing)
                    bump("buys_fail")
                    await _fire(
                        on_buy_failed,
                        {
                            "mint": mint_for_routing,
                            "mint_key": mint_key,
                            "pool": pool,
                            "symbol": symbol,
                            "spend_sol": spend_sol,
                            "route": "jupiter",
                            "reason": "router_fail",
                            "error": "router_fail",
                            "trace": trace,
                            **wf,
                        },
                        "on_buy_failed",
                    )

                # ---------------------------------------------
                # Fallback: PumpPortal
                # ---------------------------------------------
                if not str(cfg.pp_api_key or "").strip():
                    log.event("BUY_NO_PP_KEY", trace=trace, mint=mint_for_routing, pool=pool, **wf)
                    _gr_record_buy_result(False, "pp_key_missing", mint=mint_for_routing)
                    bump("buys_fail")
                    return None

                payer = owner
                candidates = _pp_candidates(raw_hint, mint_for_routing)
                last_err_short: str = "no_route"

                for send_mint, note in candidates:
                    await _fire(
                        on_buy_submitted,
                        {
                            "mint": mint_for_routing,
                            "mint_key": mint_key,
                            "pool": pool,
                            "symbol": symbol,
                            "spend_sol": spend_sol,
                            "route": "pp_remote",
                            "hint": note,
                            "trace": trace,
                            **wf,
                        },
                        "on_buy_submitted",
                    )

                    if not _gr_buys_allowed():
                        reason = _gr_block_reason()
                        _gr_record_buy_result(False, f"guardrails:{reason}", mint=mint_for_routing)
                        log.event(
                            "BUY_BLOCKED_GUARDRAILS_AT_EXEC",
                            trace=trace,
                            mint=mint_for_routing,
                            pool=pool,
                            reason=reason,
                            route="pp",
                            **wf,
                        )
                        bump("buy_skip_guardrails")
                        return None

                    body = _pp_body(
                        action="buy",
                        pool=pool,
                        payer=payer,
                        amount_sol=spend_sol,
                        slippage_bps=int(_clamp(float(cfg.slippage_bps or 600), 5, 5000)),
                        priority_sol=float(cfg.priority_sol or 0.0),
                        mint=send_mint,
                    )

                    try:
                        resp = await _pp_try(_pp_url_lightning(), body, trace)
                    except TemporaryPumpPortal as te:
                        last_err_short = "pp_temp"
                        log.event(
                            "PP_TEMP",
                            trace=trace,
                            mint=send_mint,
                            note=note,
                            err=_short_exc(te),
                            **wf,
                        )
                        continue
                    except Exception as e:
                        last_err_short = "pp_http_err"
                        log.event(
                            "PP_HTTP_ERR",
                            trace=trace,
                            mint=send_mint,
                            note=note,
                            err=_short_exc(e),
                            **wf,
                        )
                        continue

                    errs = _pp_errors(resp)
                    if errs:
                        last_err_short = "pp_errors"
                        log.event(
                            "PP_ERRORS",
                            trace=trace,
                            mint=send_mint,
                            note=note,
                            errors=";".join(errs)[:300],
                            **wf,
                        )

                        if not cfg.use_local_fallback:
                            continue

                        await _fire(
                            on_buy_submitted,
                            {
                                "mint": mint_for_routing,
                                "mint_key": mint_key,
                                "pool": pool,
                                "symbol": symbol,
                                "spend_sol": spend_sol,
                                "route": "pp_local",
                                "hint": note,
                                "trace": trace,
                                **wf,
                            },
                            "on_buy_submitted",
                        )
                        try:
                            resp2 = await _pp_try(_pp_url_local(), body, trace)
                            errs2 = _pp_errors(resp2)
                            if errs2:
                                last_err_short = "pp_local_errors"
                                log.event(
                                    "PP_LOCAL_ERRORS",
                                    trace=trace,
                                    mint=send_mint,
                                    note=note,
                                    errors=";".join(errs2)[:300],
                                    **wf,
                                )
                                continue

                            sig2 = resp2.get("signature") or resp2.get("txid")
                            if sig2:
                                sig2s = await _maybe_confirm(str(sig2), trace)
                                async with _pace_lock:
                                    _last_buy_mono_global = _mono()
                                await _post_fill(
                                    owner=owner,
                                    mint=mint_for_routing,
                                    pool=pool,
                                    symbol=symbol,
                                    spend_sol=spend_sol,
                                    sig=str(sig2s),
                                    route="pp_local",
                                    wf=wf,
                                    trace=trace,
                                    meta=None,
                                    on_buy_filled=on_buy_filled,
                                )
                                return str(sig2s)

                            tx_b64 = resp2.get("transaction") or resp2.get("tx")
                            if tx_b64 and kp is not None:
                                sig3 = await _sign_and_send_tx_b64(
                                    str(tx_b64), kp, skip_preflight=bool(cfg.skip_preflight)
                                )
                                sig3 = await _maybe_confirm(sig3, trace)
                                if sig3:
                                    async with _pace_lock:
                                        _last_buy_mono_global = _mono()
                                    await _post_fill(
                                        owner=owner,
                                        mint=mint_for_routing,
                                        pool=pool,
                                        symbol=symbol,
                                        spend_sol=spend_sol,
                                        sig=str(sig3),
                                        route="pp_local_sign",
                                        wf=wf,
                                        trace=trace,
                                        meta=None,
                                        on_buy_filled=on_buy_filled,
                                    )
                                    return str(sig3)

                        except Exception as e2:
                            last_err_short = "pp_local_err"
                            log.event(
                                "PP_LOCAL_ERR",
                                trace=trace,
                                mint=send_mint,
                                note=note,
                                err=_short_exc(e2),
                                **wf,
                            )
                        continue

                    sig = resp.get("signature") or resp.get("txid")
                    if sig:
                        sigs = await _maybe_confirm(str(sig), trace)
                        async with _pace_lock:
                            _last_buy_mono_global = _mono()
                        await _post_fill(
                            owner=owner,
                            mint=mint_for_routing,
                            pool=pool,
                            symbol=symbol,
                            spend_sol=spend_sol,
                            sig=str(sigs),
                            route="pp_remote",
                            wf=wf,
                            trace=trace,
                            meta=None,
                            on_buy_filled=on_buy_filled,
                        )
                        return str(sigs)

                    tx_b64 = resp.get("transaction") or resp.get("tx")
                    if tx_b64 and kp is not None:
                        sig2 = await _sign_and_send_tx_b64(
                            str(tx_b64), kp, skip_preflight=bool(cfg.skip_preflight)
                        )
                        sig2 = await _maybe_confirm(sig2, trace)
                        if sig2:
                            async with _pace_lock:
                                _last_buy_mono_global = _mono()
                            await _post_fill(
                                owner=owner,
                                mint=mint_for_routing,
                                pool=pool,
                                symbol=symbol,
                                spend_sol=spend_sol,
                                sig=str(sig2),
                                route="pp_remote_sign",
                                wf=wf,
                                trace=trace,
                                meta=None,
                                on_buy_filled=on_buy_filled,
                            )
                            return str(sig2)

                _note_no_route_mono(mint_key)
                bump("buys_fail")
                _gr_record_buy_result(False, f"no_route:{last_err_short}", mint=mint_for_routing)
                await _fire(
                    on_buy_failed,
                    {
                        "mint": mint_for_routing,
                        "mint_key": mint_key,
                        "pool": pool,
                        "symbol": symbol,
                        "spend_sol": spend_sol,
                        "route": "pp",
                        "hint": raw_hint,
                        "reason": "no_route",
                        "error": last_err_short,
                        "trace": trace,
                        **wf,
                    },
                    "on_buy_failed",
                )
                return None

        if mint_lock:
            async with mint_lock:
                return await _core()
        return await _core()

    try:
        return await asyncio.wait_for(_run(), timeout=float(_cfg().hard_timeout_sec or 45.0))
    except TimeoutError:
        bump("buy_timeout")
        _gr_record_buy_result(False, "timeout", mint=(mint or "").strip() or None)
        log.event("BUY_TIMEOUT", mint=mint, timeout_sec=float(_cfg().hard_timeout_sec or 45.0))
        return None
    except Exception as e:
        bump("buy_exception")
        _gr_record_buy_result(False, "exception", mint=(mint or "").strip() or None)
        log.event("BUY_EXCEPTION", mint=mint, err=_short_exc(e))
        return None


# =============================================================================
# Direct Pump.fun helpers (public)
# =============================================================================
async def buy_pump_token(
    *, mint: str, sol_amount: float, slippage_bps: int, pool: str = "pump"
) -> dict[str, Any]:
    trace = _new_trace_id()
    cfg = _cfg()
    m = _canon_mint(mint)
    if sol_amount <= 0:
        return {"ok": False, "error": "sol_amount must be > 0", "mint": m, "trace": trace}
    if not str(cfg.pp_api_key or "").strip():
        return {"ok": False, "error": "PF_PP_KEY missing", "mint": m, "trace": trace}
    if not _wallet_ready():
        return {
            "ok": False,
            "error": f"wallet_missing:{_wallet_err or 'wallet_missing'}",
            "mint": m,
            "trace": trace,
        }

    if not _gr_buys_allowed():
        reason = _gr_block_reason()
        _gr_record_buy_result(False, f"guardrails:{reason}", mint=m)
        return {"ok": False, "error": f"guardrails:{reason}", "mint": m, "trace": trace}

    kp, payer = _get_kp_owner()
    body = _pp_body(
        action="buy",
        pool=pool,
        payer=payer,
        amount_sol=float(sol_amount),
        slippage_bps=int(slippage_bps),
        priority_sol=float(cfg.priority_sol or 0.0),
        mint=m,
    )

    last_err: str | None = None

    try:
        resp = await _pp_try(_pp_url_lightning(), body, trace)
        errs = _pp_errors(resp)
        if errs:
            last_err = ";".join(errs)
        else:
            sig = resp.get("signature") or resp.get("txid")
            if sig:
                sigs = await _maybe_confirm(str(sig), trace)
                _gr_record_open(float(sol_amount), mint=m)
                _gr_record_buy_result(True, "executed_pp_remote", sig=str(sigs), mint=m)
                return {
                    "ok": True,
                    "txid": str(sigs),
                    "signature": str(sigs),
                    "mint": m,
                    "route": "pp_remote",
                    "trace": trace,
                }

            tx_b64 = resp.get("transaction") or resp.get("tx")
            if tx_b64 and kp is not None:
                sig2 = await _sign_and_send_tx_b64(
                    str(tx_b64), kp, skip_preflight=bool(cfg.skip_preflight)
                )
                sig2 = await _maybe_confirm(sig2, trace)
                if sig2:
                    _gr_record_open(float(sol_amount), mint=m)
                    _gr_record_buy_result(True, "executed_pp_remote_sign", sig=str(sig2), mint=m)
                    return {
                        "ok": True,
                        "txid": str(sig2),
                        "signature": str(sig2),
                        "mint": m,
                        "route": "pp_remote_sign",
                        "trace": trace,
                    }
    except Exception as e:
        last_err = str(e)

    if cfg.use_local_fallback:
        try:
            resp2 = await _pp_try(_pp_url_local(), body, trace)
            errs2 = _pp_errors(resp2)
            if errs2:
                last_err = ";".join(errs2)
            else:
                sig = resp2.get("signature") or resp2.get("txid")
                if sig:
                    sigs = await _maybe_confirm(str(sig), trace)
                    _gr_record_open(float(sol_amount), mint=m)
                    _gr_record_buy_result(True, "executed_pp_local", sig=str(sigs), mint=m)
                    return {
                        "ok": True,
                        "txid": str(sigs),
                        "signature": str(sigs),
                        "mint": m,
                        "route": "pp_local",
                        "trace": trace,
                    }

                tx_b64 = resp2.get("transaction") or resp2.get("tx")
                if tx_b64 and kp is not None:
                    sig2 = await _sign_and_send_tx_b64(
                        str(tx_b64), kp, skip_preflight=bool(cfg.skip_preflight)
                    )
                    sig2 = await _maybe_confirm(sig2, trace)
                    if sig2:
                        _gr_record_open(float(sol_amount), mint=m)
                        _gr_record_buy_result(True, "executed_pp_local_sign", sig=str(sig2), mint=m)
                        return {
                            "ok": True,
                            "txid": str(sig2),
                            "signature": str(sig2),
                            "mint": m,
                            "route": "pp_local_sign",
                            "trace": trace,
                        }
        except Exception as e2:
            last_err = str(e2)

    _gr_record_buy_result(False, "pump_direct_failed", mint=m)
    return {"ok": False, "error": last_err or "pump_direct_failed", "mint": m, "trace": trace}


async def buy_direct_lamports(
    *, mint: str, in_amount_lamports: int, slippage_bps: int, pool: str = "pump"
) -> dict[str, Any]:
    if in_amount_lamports <= 0:
        return {"ok": False, "error": "in_amount_lamports must be > 0", "mint": _canon_mint(mint)}
    sol_amount = float(in_amount_lamports) / LAMPORTS_PER_SOL_F
    return await buy_pump_token(
        mint=mint, sol_amount=sol_amount, slippage_bps=int(slippage_bps), pool=pool
    )


__all__ = [
    "buy_direct_lamports",
    "buy_pump_token",
    "close_pp_http",
    "maybe_buy",
    "resolve_true_mint",
    "resolve_true_mint_from_pseudo_id",
    "resolve_true_mint_from_signature",
]
