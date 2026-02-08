# -*- coding: utf-8 -*-
from __future__ import annotations

"""
utils_pkg/pump_trader.py — V3+++ (God-Tier Overhaul) — LOCK-IN
=============================================================

Pump.fun + early pool entry orchestrator with signature-drift-safe integrations.

Hard guarantees:
- Public entrypoints never raise (exception suppressed at edges).
- No network at import time (lazy clients only).
- Deterministic gate order:
    0) wallet sanity
    1) kill switch / phase gate / pool gate
    2) guardrails (HARD)
    3) cooldown + dedupe + per-mint lock + concurrency (PACE)
    4) pump filters (optional)
    5) AI entry gate (optional)
    6) momentum gate (optional)
    7) sizing + liquidity modulation
    8) risk_manager veto/cap (optional)
    9) route preflight (routeguard preferred; fallback quote-based)
   10) execute: Jupiter(router.buy_via_lightning) preferred; PumpPortal fallback
   11) post-fill: bookkeeping + DB + exit engine + alerts + hooks (best-effort)
"""

import asyncio
import contextlib
import inspect
import math
import os
import random
import time
import uuid
from collections.abc import Awaitable, Callable, Coroutine, Mapping
from typing import Any, TypeAlias, cast

import httpx

JsonDict: TypeAlias = dict[str, Any]
PubHook: TypeAlias = Callable[[JsonDict], Awaitable[None]]

LAMPORTS_PER_SOL = 1_000_000_000
LAMPORTS_PER_SOL_F = float(LAMPORTS_PER_SOL)
_BASE58 = set("123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz")


# =============================================================================
# Logging contract (Phase A): canonical logger ONLY
# =============================================================================
from utils_pkg.log_compat import EventLogger, bump, get_logger

# Single canonical EventLogger type across the repo (no local Protocol here).
log: EventLogger = get_logger(__name__, service="pump_trader")


# =============================================================================
# Parse helpers (safe fallbacks)
# =============================================================================
try:
    from utils_pkg.parse import clamp_float, get_bool, get_str, parse_float, parse_int
except Exception:  # pragma: no cover
    _TRUE = {"1", "true", "yes", "y", "on", "t"}
    _FALSE = {"0", "false", "no", "n", "off", "f"}

    def get_str(value: object, default: str = "") -> str:
        if value is None:
            return default
        try:
            s = (
                value.decode("utf-8", "replace").strip()
                if isinstance(value, bytes)
                else str(value).strip()
            )
        except Exception:
            return default
        return s if s else default

    def get_bool(value: object, default: bool = False) -> bool:
        if value is None:
            return bool(default)
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)):
            with contextlib.suppress(Exception):
                f = float(value)
                return f != 0.0 if math.isfinite(f) else bool(default)
            return bool(default)
        s = get_str(value, "").lower()
        if s in _TRUE:
            return True
        if s in _FALSE:
            return False
        return bool(default)

    def parse_float(value: object, default: float = 0.0) -> float:
        if value is None or isinstance(value, bool):
            return float(default)
        if isinstance(value, (int, float)):
            v = float(value)
            return v if math.isfinite(v) else float(default)
        s = get_str(value, "")
        if not s:
            return float(default)
        s2 = s.replace(",", "")
        is_pct = s2.endswith("%")
        if is_pct:
            s2 = s2[:-1].strip()
        with contextlib.suppress(Exception):
            v2 = float(s2)
            if math.isfinite(v2):
                return v2 / 100.0 if is_pct else v2
        return float(default)

    def parse_int(value: object, default: int = 0) -> int:
        if value is None or isinstance(value, bool):
            return int(default)
        if isinstance(value, int):
            return value
        if isinstance(value, float):
            return int(value) if math.isfinite(value) else int(default)
        s = get_str(value, "")
        if not s:
            return int(default)
        s2 = s.replace(",", "").replace("%", "")
        with contextlib.suppress(Exception):
            f = float(s2)
            return int(f) if math.isfinite(f) else int(default)
        return int(default)

    def clamp_float(value: object, lo: float, hi: float, default: float | None = None) -> float:
        dflt = 0.0 if default is None else float(default)
        v = parse_float(value, default=dflt)
        if lo > hi:
            lo, hi = hi, lo
        if v < lo:
            return float(lo)
        if v > hi:
            return float(hi)
        return float(v)


# =============================================================================
# Soft imports (never break caller) — typed as Any to reduce signature drift pain
# =============================================================================
_router: object | None
try:
    from utils_pkg import router as _router_mod  # type: ignore

    _router = _router_mod
except Exception:
    _router = None

rg_preflight: Callable[..., Awaitable[tuple[bool, JsonDict | None]]] | None
try:
    from utils_pkg.routeguard import preflight as _rg_preflight  # type: ignore

    rg_preflight = cast(Callable[..., Awaitable[tuple[bool, JsonDict | None]]], _rg_preflight)
except Exception:
    rg_preflight = None

try:
    from utils_pkg.telegram_utils import notify_trade_fill  # type: ignore
except Exception:  # pragma: no cover

    async def notify_trade_fill(
        symbol: str,
        side: str,
        size_sol: float,
        price: float,
        tx: str | None = None,
    ) -> None:
        return None


_wallet_err: str | None = None
_confirm_signature: Callable[..., Any] | None
_get_balance_sol: Callable[..., Any] | None
get_pubkey_str: Callable[[object], str] | None
load_keypair: Callable[[], object] | None
_sign_and_send_b64_tx: Callable[..., Any] | None
try:
    from utils_pkg.solana_wallet import (  # type: ignore
        confirm_signature as _cs,
        get_balance_sol as _gbs,
        get_pubkey_str as _gps,
        load_keypair as _lkp,
        sign_and_send_b64_tx as _ssb,
    )

    _confirm_signature = cast(Callable[..., Any], _cs)
    _get_balance_sol = cast(Callable[..., Any], _gbs)
    get_pubkey_str = cast(Callable[[object], str], _gps)
    load_keypair = cast(Callable[[], object], _lkp)
    _sign_and_send_b64_tx = cast(Callable[..., Any], _ssb)
except Exception as e:  # pragma: no cover
    _wallet_err = str(e)
    _confirm_signature = None
    _get_balance_sol = None
    get_pubkey_str = None
    load_keypair = None
    _sign_and_send_b64_tx = None

_auto_size_sol: Callable[[], float] | None
try:
    from utils_pkg.sizer import suggest_trade_sol as _ats  # type: ignore

    _auto_size_sol = _ats
except Exception:
    _auto_size_sol = None

_liq_looks_like_mint: Callable[[str], bool] | None
screen_token: Callable[[str], Awaitable[tuple[bool, JsonDict]]]
try:
    from utils_pkg.liquidity import looks_like_mint as _llm, screen_token as _st  # type: ignore

    _liq_looks_like_mint = cast(Callable[[str], bool], _llm)
    screen_token = cast(Callable[[str], Awaitable[tuple[bool, JsonDict]]], _st)
except Exception:  # pragma: no cover
    _liq_looks_like_mint = None

    async def screen_token(_: str) -> tuple[bool, JsonDict]:
        return True, {"ok": True}


# NOTE: Any (not object) to avoid mypy attr-defined when integrations drift.
_gr: Any | None
try:
    from utils_pkg import guardrails as _gr_mod  # type: ignore

    _gr = _gr_mod
except Exception:
    _gr = None

_rm: object | None
try:
    from utils_pkg import risk_manager as _rm_mod  # type: ignore

    _rm = _rm_mod
except Exception:
    _rm = None

claim_once: Callable[..., bool]
try:
    from utils_pkg.dedupe import claim_once as _co  # type: ignore

    claim_once = cast(Callable[..., bool], _co)
except Exception:  # pragma: no cover

    def claim_once(_key: str, _ttl: int = 60, ttl: int | None = None) -> bool:
        return True


pg_upsert_position: Callable[..., Awaitable[None]]
try:
    from utils_pkg.core_utils import pg_upsert_position as _pup  # type: ignore

    pg_upsert_position = cast(Callable[..., Awaitable[None]], _pup)
except Exception:  # pragma: no cover

    async def pg_upsert_position(**_kwargs: Any) -> None:
        return None


PumpStats: type[object] | None
check_pump_candidate: Callable[[object], object] | None
check_pump_candidate_async: Callable[[object], Awaitable[object]] | None
try:
    from utils_pkg.pump_filters import (  # type: ignore
        PumpStats as _PS,
        check_pump_candidate as _cpc,
        check_pump_candidate_async as _cpc_a,
    )

    PumpStats = cast(type[object], _PS)
    check_pump_candidate = cast(Callable[[object], object], _cpc)
    check_pump_candidate_async = cast(Callable[[object], Awaitable[object]], _cpc_a)
except Exception:
    PumpStats = None
    check_pump_candidate = None
    check_pump_candidate_async = None

_exit_engine: object | None
try:
    from utils_pkg.auto_exit_worker import exit_engine as _ee  # type: ignore

    _exit_engine = _ee
except Exception:
    _exit_engine = None

_AICandidate: type[Any] | None
_ai_pass_gate: Callable[..., Any] | None
_AI_GATE_ERR: str | None = None
try:
    from utils_pkg.ai_buy_scoring import Candidate as _Cand, pass_gate as _pg  # type: ignore

    _AICandidate = cast(type[Any], _Cand)
    _ai_pass_gate = cast(Callable[..., Any], _pg)
except Exception as e:
    _AICandidate = None
    _ai_pass_gate = None
    _AI_GATE_ERR = str(e)

_ai_entry_logic: object | None
try:
    from utils_pkg import ai_entry_logic as _ael  # type: ignore

    _ai_entry_logic = _ael
except Exception:
    _ai_entry_logic = None

_data_logger: object | None
try:
    from utils_pkg import data_logger as _dl  # type: ignore

    _data_logger = _dl
except Exception:
    _data_logger = None


# =============================================================================
# Small helpers
# =============================================================================
def _envs(k: str, d: str = "") -> str:
    with contextlib.suppress(Exception):
        v = os.getenv(k)
        return (v if v is not None else d).strip()
    return str(d).strip()


def _envb(k: str, d: bool = False) -> bool:
    return get_bool(_envs(k, ""), default=d)


def _envf(k: str, d: float) -> float:
    return parse_float(_envs(k, str(d)), default=d)


def _envi(k: str, d: int) -> int:
    return parse_int(_envs(k, str(d)), default=d)


def _mono() -> float:
    return time.monotonic()


def _short_exc(e: BaseException, n: int = 260) -> str:
    s = f"{type(e).__name__}: {e}"
    return (s[:n] + "…") if len(s) > n else s


async def _maybe_await(x: Any) -> Any:
    return await x if inspect.isawaitable(x) else x


def _new_trace_id() -> str:
    return uuid.uuid4().hex[:12]


def _phase() -> int:
    return max(0, min(7, _envi("MASTER_PHASE", _envi("STRATEGY_PHASE", 7))))


def _looks_base58(s: str) -> bool:
    return bool(s) and all(ch in _BASE58 for ch in s)


def looks_like_mint(s: str) -> bool:
    if callable(_liq_looks_like_mint):
        with contextlib.suppress(Exception):
            return bool(_liq_looks_like_mint(s))
    x = (s or "").strip()
    return bool(x) and _looks_base58(x) and 28 <= len(x) <= 50


def _strip_pseudo_suffix(raw: str) -> tuple[str, str | None]:
    s = (raw or "").strip()
    for suf in ("pump", "bonk"):
        if s.endswith(suf):
            return s[: -len(suf)].strip(), suf
    return s, None


def _is_pseudo_id(s: str) -> bool:
    return (s or "").strip().endswith(("pump", "bonk"))


def _round_step(val: float, step: float) -> float:
    if step <= 0:
        return float(val)
    return math.floor(float(val) / step) * step


def _as_coroutine(aw: Awaitable[Any]) -> Coroutine[Any, Any, Any]:
    async def _run() -> Any:
        return await aw

    return _run()


def _safe_create_task(coro: Awaitable[Any]) -> None:
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        return
    with contextlib.suppress(Exception):
        loop.create_task(_as_coroutine(coro))


# =============================================================================
# Config (cached dict for compactness; phase-aware)
# =============================================================================
_CFG_TTL_SEC = max(0.5, _envf("PF_TRADER_REFRESH_SEC", 2.0))
_cfg_at = 0.0
_cfg_cache: dict[str, Any] | None = None


def _cfg() -> dict[str, Any]:
    global _cfg_at, _cfg_cache
    now = _mono()
    if _cfg_cache is not None and (now - _cfg_at) < _CFG_TTL_SEC:
        return _cfg_cache

    phase = _phase()
    dryrun = _envb("DRYRUN", _envb("DRY_RUN", _envb("IS_DRYRUN", False)))
    kill_switch = _envb("KILL_SWITCH", False) or _envb("MASTER_KILL_SWITCH", False)

    allowed_pools = tuple(
        sorted({p.strip().lower() for p in _envs("PF_POOLS", "pump").split(",") if p.strip()})
    ) or ("pump",)

    c: dict[str, Any] = {
        "phase": phase,
        "dryrun": dryrun,
        "kill_switch": kill_switch,
        "disable_buys": _envb("DISABLE_BUYS", False),
        "min_phase": _envi("PF_MIN_PHASE", 1),
        "owner_from_env": _envs("PF_PP_ACCOUNT", "").strip(),
        "allowed_pools": allowed_pools,
        "min_sol_balance": float(_envf("MIN_SOL_BALANCE", 0.20)),
        "reserve_sol": float(_envf("PF_WALLET_RESERVE_SOL", 0.08)),
        "pf_enabled": _envb("PUMP_FILTER_ENABLED", True) and (phase >= 3),
        "pf_required": _envb("PUMP_FILTER_REQUIRED", False),
        "pf_async_enrich": _envb("PUMP_FILTER_ASYNC_ENRICH", True),
        "pf_log_details": _envb("PUMP_FILTER_LOG_DETAILS", False),
        "ai_gate_enabled": _envb("AI_ENTRY_GATE_ENABLED", False) and (phase >= 2),
        "ai_gate_min_score": float(_envf("AI_ENTRY_GATE_MIN_SCORE", 0.0)),
        "ai_gate_require": _envb("AI_ENTRY_GATE_REQUIRED", False),
        "buy_cooldown_sec": float(_envi("BUY_COOLDOWN_SEC", _envi("PF_COOLDOWN_SEC", 120))),
        "buy_cooldown_jitter_sec": float(_envf("BUY_COOLDOWN_JITTER_SEC", 3.0)),
        "attempt_pace_frac": float(
            clamp_float(_envf("PF_ATTEMPT_PACE_FRAC", 0.50), 0.10, 1.00, 0.50)
        ),
        "max_concurrent_buys": int(
            max(1, _envi("MAX_CONCURRENT_BUYS", _envi("PF_MAX_CONCURRENT", 2)))
        ),
        "noroute_cooldown_sec": int(_envi("NOROUTE_COOLDOWN_SEC", 600)),
        "per_mint_lock": _envb("PF_PER_MINT_LOCK", True),
        "hard_timeout_sec": float(max(5.0, _envf("BUY_HARD_TIMEOUT_SEC", 45.0))),
        "dedupe_enabled": _envb("DEDUPE_ENABLED", True),
        "dedupe_ttl_sec": int(_envi("DEDUPE_TTL_SEC", 600)),
        "confirm_timeout_sec": int(_envi("CONFIRM_TIMEOUT_SEC", 25)),
        "confirm_commitment": (
            _envs("CONFIRM_COMMITMENT", "confirmed").lower().strip() or "confirmed"
        ),
        "buy_sol_per_trade": float(_envf("BUY_SOL_PER_TRADE", 0.0)),
        "risk_pct_wallet": float(_envf("PF_TRADE_RISK_PCT", 0.05)),
        "autosize_refresh_sec": float(_envf("PF_TRADE_AUTOSIZE_REFRESH_SEC", 10.0)),
        "min_sol_per_trade": float(
            _envf("PF_TRADE_MIN_SOL", _envf("ENTRY_MIN_INITIAL_BUY_SOL", 0.005))
        ),
        "max_sol_per_trade": float(_envf("PF_TRADE_MAX_SOL", _envf("ENTRY_MAX_BUY_SOL", 0.08))),
        "step_sol": float(_envf("PF_TRADE_STEP_SOL", 0.005)),
        "risk_manager_enabled": _envb("RISK_MANAGER_ENABLED", True) and (phase >= 1),
        "momo_enabled": _envb("ENTRY_MOMENTUM_ENABLED", True) and (phase >= 2),
        "momo_min_up_pct": float(_envf("ENTRY_MOMO_MIN_UP_PCT", 5.0)),
        "momo_window_sec": float(_envf("ENTRY_MOMO_WINDOW_SEC", 10.0)),
        "momo_price_tries": int(max(2, _envi("ENTRY_MOMO_PRICE_TRIES", 4))),
        "momo_price_delay_s": float(_envf("ENTRY_MOMO_PRICE_DELAY_S", 0.7)),
        "momo_allow_no_data_for_pump": _envb("ENTRY_MOMO_ALLOW_NO_DATA_FOR_PUMP", True),
        "price_ttl_sec": float(_envf("PF_PRICE_TTL_SEC", 5.0)),
        "liq_filter_enabled": _envb("ENTRY_LIQ_FILTER", True) and (phase >= 4),
        "entry_min_liq_usd": float(_envf("ENTRY_MIN_LIQ_USD", 10_000)),
        "entry_max_liq_usd": float(_envf("ENTRY_MAX_LIQ_USD", 200_000)),
        "entry_min_impact_bps": int(_envi("ENTRY_MIN_IMPACT_BPS", 250)),
        "entry_max_impact_bps": int(_envi("ENTRY_MAX_IMPACT_BPS", 800)),
        "pp_api_key": _envs("PF_PP_KEY", "").strip(),
        "slippage_bps": int(_envi("PF_SLIPPAGE_BPS", 600)),
        "dynamic_slippage": _envb("PF_DYNAMIC_SLIPPAGE", False),
        "slippage_min_bps": int(_envi("PF_SLIPPAGE_MIN_BPS", 50)),
        "slippage_max_bps": int(_envi("PF_SLIPPAGE_MAX_BPS", 1500)),
        "priority_sol": float(_envf("PF_PRIORITY_SOL", 0.002)),
        "jito_tip_sol": float(_envf("PF_JITO_TIP_SOL", _envf("PF_PRIORITY_SOL", 0.002))),
        "compute_units": int(_envi("PF_COMPUTE_UNITS", 0)),
        "prefer_pp_for_pump_pool": _envb("PF_PREFER_PP_FOR_PUMP_POOL", False),
        "use_local_fallback": _envb("PF_USE_LOCAL_FALLBACK", False),
        "skip_preflight": _envb("PF_SKIP_PREFLIGHT", True),
        "routeguard_allow_no_route": _envb("ROUTEGUARD_ALLOW_NO_ROUTE", True),
        "routeguard_require_sell": _envb("ROUTEGUARD_REQUIRE_SELL", True) and (phase >= 3),
        "jup_retries": int(max(1, _envi("PF_JUP_RETRIES", 2))),
        "pp_timeout_sec": float(max(6.0, _envf("PF_PP_TIMEOUT_SEC", 18.0))),
        "pp_retry_max": int(max(0, _envi("PF_PP_RETRY_MAX", 1))),
        "pp_retry_backoff": float(max(0.1, _envf("PF_PP_RETRY_BACKOFF_SEC", 0.7))),
        "pp_max_qps": int(max(1, _envi("PF_PP_MAX_QPS", 2))),
        "resolve_pseudo_enabled": _envb("PF_RESOLVE_PSEUDO_TO_MINT", False),
        "resolve_pseudo_timeout_sec": float(
            max(0.25, _envf("PF_RESOLVE_PSEUDO_TIMEOUT_SEC", 1.25))
        ),
        "log_candidates": _envb("PF_LOG_CANDIDATES", True) and (phase >= 7),
        "log_trades": _envb("PF_LOG_TRADES", True) and (phase >= 7),
        "tg_enabled": _envb("TELEGRAM_ENABLED", True),
        "tg_notify_filled": _envb("TELEGRAM_NOTIFY_FILLED", True),
        "debug": _envb("PF_DEBUG", _envb("PF_DEBUG_TX", False)),
        "mint_lock_max": int(max(128, _envi("PF_MINT_LOCK_MAX", 2048))),
    }

    if c["min_sol_per_trade"] > c["max_sol_per_trade"]:
        c["min_sol_per_trade"], c["max_sol_per_trade"] = (
            c["max_sol_per_trade"],
            c["min_sol_per_trade"],
        )

    _cfg_cache = c
    _cfg_at = now
    return c


# =============================================================================
# Dynamic concurrency limiter
# =============================================================================
class _DynLimiter:
    def __init__(self, n: int) -> None:
        self._max = max(1, int(n))
        self._inflight = 0
        self._cv = asyncio.Condition()

    async def resize(self, n: int) -> None:
        async with self._cv:
            self._max = max(1, int(n))
            self._cv.notify_all()

    async def sync_from_cfg(self) -> None:
        await self.resize(int(_cfg().get("max_concurrent_buys", 1)))

    async def acquire(self) -> None:
        async with self._cv:
            while self._inflight >= self._max:
                await self._cv.wait()
            self._inflight += 1

    async def release(self) -> None:
        async with self._cv:
            self._inflight = max(0, self._inflight - 1)
            self._cv.notify_all()

    async def __aenter__(self) -> "_DynLimiter":
        await self.acquire()
        return self

    async def __aexit__(self, _exc_type: object, _exc: BaseException | None, _tb: object) -> None:
        await self.release()


# =============================================================================
# Internal state
# =============================================================================
_last_buy_mono_global = 0.0
_last_attempt_mono_global = 0.0
_last_no_route_mono: dict[str, float] = {}
_pace_lock = asyncio.Lock()
_inflight = _DynLimiter(int(_cfg().get("max_concurrent_buys", 1)))

_last_autosize_mono = 0.0
_cached_wallet_sol = 0.0

_mint_locks: dict[str, asyncio.Lock] = {}
_mint_lock_meta: dict[str, float] = {}

_kp_cached: object | None = None
_owner_cached = ""

_pp_client: httpx.AsyncClient | None = None
_pp_client_timeout_s = 0.0
_pp_rate_lock = asyncio.Lock()
_pp_last_mono = 0.0

_price_cache: dict[str, tuple[float, float]] = {}


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
        return


def _mint_lock(mint_key: str) -> asyncio.Lock:
    if mint_key not in _mint_locks:
        _mint_locks[mint_key] = asyncio.Lock()
    _mint_lock_meta[mint_key] = _mono()
    _cleanup_mint_locks(int(_cfg().get("mint_lock_max", 2048)))
    return _mint_locks[mint_key]


def _cooldown_ok_mono(key: str, ttl: int) -> bool:
    now = _mono()
    last = _last_no_route_mono.get(key, 0.0)
    return (now - last) >= float(ttl)


def _note_no_route_mono(key: str) -> None:
    _last_no_route_mono[key] = _mono()


def _wallet_ready() -> bool:
    return (
        (_wallet_err is None)
        and callable(load_keypair)
        and callable(get_pubkey_str)
        and callable(_get_balance_sol)
    )


def _get_kp_owner() -> tuple[object | None, str]:
    global _kp_cached, _owner_cached
    cfg = _cfg()
    if not _wallet_ready():
        return None, str(cfg.get("owner_from_env", "")).strip()

    if _kp_cached is None and callable(load_keypair):
        with contextlib.suppress(Exception):
            _kp_cached = load_keypair()

    if not _owner_cached:
        owner_from_env = str(cfg.get("owner_from_env", "")).strip()
        if owner_from_env:
            _owner_cached = owner_from_env
        elif _kp_cached is not None and callable(get_pubkey_str):
            with contextlib.suppress(Exception):
                _owner_cached = str(get_pubkey_str(_kp_cached))

    return _kp_cached, _owner_cached


async def _get_sol_balance(owner: str) -> float:
    if not _wallet_ready() or not owner or not callable(_get_balance_sol):
        return 0.0
    try:
        res = _get_balance_sol(owner)
        return float(await _maybe_await(res))
    except Exception:
        return 0.0


async def _sign_and_send_tx_b64(tx_b64: str, kp: object, *, skip_preflight: bool) -> str | None:
    if not callable(_sign_and_send_b64_tx):
        return None
    try:
        res = _sign_and_send_b64_tx(tx_b64, kp, skip_preflight=skip_preflight)
        sig = await _maybe_await(res)
        return str(sig) if sig else None
    except TypeError:
        with contextlib.suppress(Exception):
            res2 = _sign_and_send_b64_tx(tx_b64, kp)
            sig2 = await _maybe_await(res2)
            return str(sig2) if sig2 else None
        return None
    except Exception:
        return None


# =============================================================================
# Guardrails / RiskManager wrappers (signature drift safe)
# =============================================================================
def _gr_buys_allowed() -> bool:
    if _gr is None:
        return True
    try:
        fn = (
            getattr(_gr, "buys_allowed", None)
            or getattr(_gr, "allow_buys", None)
            or getattr(_gr, "allow_buy", None)
            or getattr(_gr, "can_buy", None)
        )
        if callable(fn):
            with contextlib.suppress(TypeError):
                return bool(fn())
            with contextlib.suppress(Exception):
                return bool(fn(True))
        val = getattr(_gr, "buys_allowed", None)
        if isinstance(val, bool):
            return bool(val)
    except Exception:
        return True
    return True


def _gr_block_reason() -> str:
    if _gr is None:
        return "blocked"
    try:
        fn = (
            getattr(_gr, "buy_block_reason", None)
            or getattr(_gr, "block_reason", None)
            or getattr(_gr, "why_blocked", None)
        )
        if callable(fn):
            with contextlib.suppress(Exception):
                r = fn()
                return str(r) if r else "blocked"
        for attr in (
            "buy_block_reason",
            "block_reason",
            "reason",
            "last_reason",
            "last_block_reason",
        ):
            with contextlib.suppress(Exception):
                v = getattr(_gr, attr, None)
                if v:
                    return str(v)
    except Exception:
        return "blocked"
    return "blocked"


def _gr_record_open(notional_sol: float, mint: str | None = None) -> None:
    try:
        if _gr is None:
            return
        fn = getattr(_gr, "record_trade_open", None)
        if not callable(fn):
            return
        try:
            fn(float(notional_sol), mint=mint)
        except TypeError:
            fn(float(notional_sol))
    except Exception:
        return


def _gr_record_buy_result(
    ok: bool, reason: str, sig: str | None = None, mint: str | None = None
) -> None:
    try:
        if _gr is None:
            return
        fn = getattr(_gr, "record_buy_result", None)
        if not callable(fn):
            return
        for call in (
            lambda: fn(ok=ok, reason=reason, sig=sig, mint=mint),
            lambda: fn(ok=ok, reason=reason, sig=sig),
            lambda: fn(ok, reason, sig),
            lambda: fn(ok, reason),
        ):
            try:
                call()
                return
            except TypeError:
                continue
            except Exception:
                return
    except Exception:
        return


async def _rm_pretrade(
    *, owner: str, mint: str, pool: str, spend_sol: float, ui: JsonDict, trace: str
) -> tuple[bool, float, str, JsonDict]:
    cfg = _cfg()
    if _rm is None or not bool(cfg.get("risk_manager_enabled", True)):
        return True, float(spend_sol), "rm_disabled", {}

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

    for name in (
        "pretrade",
        "pre_trade",
        "pre_trade_check",
        "check_entry",
        "allow_entry",
        "allow_trade",
        "can_trade",
    ):
        fn = getattr(_rm, name, None)
        if not callable(fn):
            continue
        try:
            res_obj = await _maybe_await(fn(**payload))
        except TypeError:
            with contextlib.suppress(Exception):
                res_obj = await _maybe_await(fn(owner, mint, float(spend_sol)))
            if "res_obj" not in locals():
                continue
        except Exception:
            continue

        if res_obj is None:
            continue
        if isinstance(res_obj, bool):
            return bool(res_obj), float(spend_sol), ("rm_ok" if res_obj else "rm_block"), {}
        if isinstance(res_obj, dict):
            ok = bool(res_obj.get("ok", True))
            adj = parse_float(
                res_obj.get("spend_sol", res_obj.get("size_sol", spend_sol)), default=spend_sol
            )
            reason = get_str(res_obj.get("reason", "rm"), default="rm")[:140]
            extra = {
                k: v
                for k, v in res_obj.items()
                if k not in ("ok", "spend_sol", "size_sol", "reason")
            }
            return ok, float(adj), reason, cast(JsonDict, extra)
        if isinstance(res_obj, (list, tuple)) and res_obj:
            ok = bool(res_obj[0])
            adj = float(spend_sol)
            if len(res_obj) >= 2 and res_obj[1] is not None:
                adj = parse_float(res_obj[1], default=spend_sol)
            reason = (
                get_str(res_obj[2], "rm")[:140]
                if len(res_obj) >= 3
                else ("rm_ok" if ok else "rm_block")
            )
            return ok, float(adj), reason, {}

    return True, float(spend_sol), "rm_no_match", {}


def _rm_record_open(spend_sol: float, *, owner: str, mint: str, pool: str, trace: str) -> None:
    try:
        cfg = _cfg()
        if _rm is None or not bool(cfg.get("risk_manager_enabled", True)):
            return
        fn = (
            getattr(_rm, "record_open", None)
            or getattr(_rm, "on_open", None)
            or getattr(_rm, "record_trade_open", None)
        )
        if not callable(fn):
            return
        try:
            fn(owner=owner, mint=mint, pool=pool, spend_sol=float(spend_sol), trace=trace)
        except TypeError:
            with contextlib.suppress(Exception):
                fn(float(spend_sol), mint=mint)
    except Exception:
        return


def _rm_record_buy_result(
    ok: bool, reason: str, *, owner: str, mint: str, pool: str, sig: str | None, trace: str
) -> None:
    try:
        cfg = _cfg()
        if _rm is None or not bool(cfg.get("risk_manager_enabled", True)):
            return
        fn = (
            getattr(_rm, "record_buy_result", None)
            or getattr(_rm, "on_buy_result", None)
            or getattr(_rm, "record_trade_result", None)
        )
        if not callable(fn):
            return
        try:
            fn(ok=ok, reason=reason, sig=sig, owner=owner, mint=mint, pool=pool, trace=trace)
        except TypeError:
            with contextlib.suppress(Exception):
                fn(ok, reason, sig)
    except Exception:
        return


# =============================================================================
# Router wrappers (signature drift safe)
# =============================================================================
async def _router_get_price_ui(mint: str) -> float:
    if _router is None:
        return 0.0
    fn = getattr(_router, "get_price_ui", None)
    if not callable(fn):
        return 0.0
    with contextlib.suppress(Exception):
        return float(await _maybe_await(fn(mint)))
    return 0.0


async def _price_ui_cached(mint: str, ttl_sec: float) -> float:
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


async def _router_get_buy_quote(mint: str, lamports_in: int) -> JsonDict | None:
    if _router is None:
        return None
    fn = getattr(_router, "get_buy_quote", None)
    if not callable(fn):
        return None
    try:
        return cast(JsonDict | None, await fn(mint=mint, lamports_in=lamports_in))
    except TypeError:
        with contextlib.suppress(Exception):
            return cast(JsonDict | None, await fn(mint=mint, amount_raw=lamports_in))
        with contextlib.suppress(Exception):
            return cast(JsonDict | None, await fn(mint, lamports_in))
    except Exception:
        return None
    return None


async def _router_get_sell_quote(mint: str, amount_raw: int) -> JsonDict | None:
    if _router is None:
        return None
    fn = getattr(_router, "get_sell_quote", None)
    if not callable(fn):
        return None
    try:
        return cast(JsonDict | None, await fn(mint=mint, amount_raw=amount_raw))
    except TypeError:
        with contextlib.suppress(Exception):
            return cast(JsonDict | None, await fn(mint=mint, amount=amount_raw))
        with contextlib.suppress(Exception):
            return cast(JsonDict | None, await fn(mint, amount_raw))
    except Exception:
        return None
    return None


async def _router_buy_via_lightning(
    *,
    mint: str,
    amount_sol: float,
    slippage_bps: int | None,
    priority_sol: float | None,
    compute_units: int | None,
    prequote: JsonDict | None = None,
) -> tuple[bool, str | None, JsonDict | None]:
    if _router is None:
        return False, None, None
    fn = getattr(_router, "buy_via_lightning", None)
    if not callable(fn):
        return False, None, None

    base: JsonDict = {"mint": mint, "amount_sol": float(amount_sol)}
    if slippage_bps is not None:
        base["slippage_bps"] = int(slippage_bps)
    if priority_sol is not None:
        base["priority_sol"] = float(priority_sol)
    if compute_units is not None:
        base["compute_units"] = int(compute_units)
    if isinstance(prequote, dict) and prequote:
        base["prequote"] = prequote

    attempts: list[JsonDict] = [
        {**base, "return_receipt": True},
        {**base, "return_meta": True},
        {**base},
    ]
    for kwargs in attempts:
        try:
            res = await _maybe_await(fn(**kwargs))
        except TypeError:
            try:
                k2 = dict(kwargs)
                if "priority_sol" in k2:
                    k2["jito_tip_sol"] = k2.pop("priority_sol")
                res = await _maybe_await(fn(**k2))
            except Exception:
                continue
        except Exception:
            continue

        if isinstance(res, dict):
            sig = res.get("sig") or res.get("signature") or res.get("txid")
            sig_s = str(sig) if sig else None
            ok = bool(res.get("ok")) if "ok" in res else bool(sig_s)
            meta = res.get("meta") if isinstance(res.get("meta"), dict) else {}
            meta_out: JsonDict = dict(meta) if isinstance(meta, dict) else {}
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
                if k in res and k not in meta_out:
                    meta_out[k] = res.get(k)
            return ok, sig_s, (meta_out if meta_out else None)

        if isinstance(res, (list, tuple)):
            ok = bool(res[0]) if len(res) >= 1 else False
            sig_s = str(res[1]) if len(res) >= 2 and res[1] else None
            meta_obj = (
                cast(JsonDict, res[2]) if len(res) >= 3 and isinstance(res[2], dict) else None
            )
            return ok, sig_s, meta_obj

        if isinstance(res, str) and res:
            return True, res, None

    return False, None, None


# =============================================================================
# Gates: PumpFilters / AI / Momentum
# =============================================================================
def _pumpstats_from_dict(d: Mapping[str, Any], *, mint: str) -> object | None:
    """
    Build a PumpStats instance from a mapping (dict-like). Never raises.
    """
    if PumpStats is None:
        return None

    # Most PumpStats implementations are either:
    #  - positional args + mint kw
    #  - accepts a dict/mapping + mint kw
    with contextlib.suppress(Exception):
        return PumpStats(  # type: ignore[call-arg]
            parse_float(d.get("stage"), 0.0),
            parse_float(d.get("liq_usd", d.get("liqUsd")), 0.0),
            parse_float(d.get("fdv_usd", d.get("fdvUsd")), 0.0),
            parse_int(d.get("buyers_60s", d.get("buyers60s")), 0),
            parse_int(d.get("buys_60s", d.get("buys60s")), 0),
            parse_float(d.get("sol_60s", d.get("sol60s")), 0.0),
            parse_float(d.get("idle_sec", d.get("idleSec")), 0.0),
            parse_int(d.get("good_wallets", d.get("goodWallets")), 0),
            parse_int(d.get("bad_wallets", d.get("badWallets")), 0),
            mint=mint,  # type: ignore[call-arg]
        )

    with contextlib.suppress(Exception):
        return PumpStats(d, mint=mint)  # type: ignore[call-arg]

    return None


def _extract_pump_stats(ui: JsonDict, *, mint: str) -> object | None:
    """
    Extract/normalize PumpStats from ui:
      - If ui["pump_stats"] is already a PumpStats instance, ensure mint is set (mypy-safe).
      - If ui["pump_stats"] is a dict/mapping, convert to PumpStats.
      - Else, if ui contains pump-stat keys, treat ui itself as the mapping.
    """
    cfg = _cfg()
    if not bool(cfg.get("pf_enabled", False)) or PumpStats is None or check_pump_candidate is None:
        return None

    ps = ui.get("pump_stats")
    if ps is not None:
        # Already-built PumpStats object
        if PumpStats is not None and isinstance(ps, cast(Any, PumpStats)):
            # mypy-safe: avoid direct attribute assignment on 'object'
            with contextlib.suppress(Exception):
                cur = getattr(ps, "mint", "")
                if not cur:
                    ps.mint = mint
            return ps

        # Dict-like payload -> PumpStats
        if isinstance(ps, Mapping):
            return _pumpstats_from_dict(cast(Mapping[str, Any], ps), mint=mint)

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
    return _pumpstats_from_dict(ui, mint=mint) if any(k in ui for k in keys) else None


async def _pump_filters_ok(ui: JsonDict, *, mint: str, pool: str, trace: str) -> tuple[bool, str]:
    cfg = _cfg()
    if not bool(cfg.get("pf_enabled", False)) or pool != "pump":
        return True, "pump_filter_disabled"
    if PumpStats is None or check_pump_candidate is None:
        return True, "pump_filter_unavailable"

    stats = _extract_pump_stats(ui, mint=mint)
    if stats is None:
        return (
            (False, "pump_stats_missing")
            if bool(cfg.get("pf_required", False))
            else (True, "pump_stats_missing_allowed")
        )

    try:
        if bool(cfg.get("pf_async_enrich", True)) and check_pump_candidate_async is not None:
            dec = await check_pump_candidate_async(stats)
        else:
            dec = check_pump_candidate(stats)
    except Exception as e:
        log.event("PUMP_FILTER_ERROR", trace=trace, mint=mint, err=_short_exc(e))
        return (
            (False, "pump_filter_error")
            if bool(cfg.get("pf_required", False))
            else (True, "pump_filter_error_allowed")
        )

    ok = bool(getattr(dec, "ok", False))
    reason = str(getattr(dec, "reason", "unknown"))
    score = float(parse_float(getattr(dec, "score", 0.0), 0.0))
    vel = float(parse_float(getattr(dec, "velocity_score", 0.0), 0.0))
    sw = float(parse_float(getattr(dec, "smart_wallet_score", 0.0), 0.0))
    details_obj = getattr(dec, "details", {}) if hasattr(dec, "details") else {}

    ui["pump_filter_ok"] = ok
    ui["pump_filter_reason"] = reason
    ui["pump_filter_score"] = score
    ui["pump_filter_velocity"] = vel
    ui["pump_filter_sw_score"] = sw
    ui["pump_filter_details"] = details_obj if isinstance(details_obj, dict) else {}

    details = ui.get("pump_filter_details") if bool(cfg.get("pf_log_details", False)) else None
    log.event(
        "PUMP_FILTER",
        trace=trace,
        mint=mint,
        ok=int(ok),
        reason=reason,
        score=score,
        vel=vel,
        sw=sw,
        details=details,
    )
    return ok, reason


async def _ai_gate(ui: JsonDict, *, mint: str, pool: str, trace: str) -> tuple[bool, float, str]:
    cfg = _cfg()
    if not bool(cfg.get("ai_gate_enabled", False)):
        return True, float(parse_float(ui.get("ai_gate_score"), -1.0)), "ai_gate_disabled"

    if ui.get("ai_gate_ok") is not None:
        ok = bool(ui.get("ai_gate_ok"))
        score = float(parse_float(ui.get("ai_gate_score"), -1.0))
        reason = get_str(ui.get("ai_gate_reason") or ("ai_ok" if ok else "ai_block"), "")[:160]
        min_score = float(cfg.get("ai_gate_min_score", 0.0) or 0.0)
        if min_score and score >= 0:
            ok = ok and (score >= min_score)
            if not ok:
                reason = f"ai_score<{min_score}"
        return ok, score, reason

    if _is_pseudo_id(mint) and not bool(cfg.get("ai_gate_require", False)):
        return True, -1.0, "ai_skip_pseudo"

    # Prefer ai_buy_scoring if available
    if _ai_pass_gate is not None and _AICandidate is not None:
        try:
            payload = {
                "mint": mint,
                "pool": pool,
                "ui": ui,
                "meta": ui.get("meta") if isinstance(ui.get("meta"), dict) else None,
                "trace": trace,
            }

            Cand = cast(Any, _AICandidate)  # <- key line (mypy-safe)
            cand: Any | None = None

            for ctor in (
                lambda: Cand(**payload),
                lambda: Cand(mint=mint, ui=ui),
                lambda: Cand(mint=mint),
            ):
                try:
                    cand = ctor()
                    break
                except TypeError:
                    continue
            if cand is not None:
                res = _ai_pass_gate(cand)  # type: ignore[misc]
                if isinstance(res, (list, tuple)) and len(res) >= 3:
                    ok_b, score_f, why_s = (
                        bool(res[0]),
                        float(parse_float(res[1], -1.0)),
                        get_str(res[2], "ai")[:220],
                    )
                elif isinstance(res, dict):
                    ok_b, score_f, why_s = (
                        bool(res.get("ok", True)),
                        float(parse_float(res.get("score"), -1.0)),
                        get_str(res.get("reason", "ai"), "ai")[:220],
                    )
                else:
                    ok_b, score_f, why_s = True, -1.0, "ai_gate_unknown"

                ui["ai_gate_ok"], ui["ai_gate_score"], ui["ai_gate_reason"] = ok_b, score_f, why_s
                min_score = float(cfg.get("ai_gate_min_score", 0.0) or 0.0)
                if min_score and score_f >= 0:
                    ok_b = ok_b and (score_f >= min_score)
                    if not ok_b:
                        why_s = f"ai_score<{min_score}"

                log.event(
                    "AI_GATE",
                    trace=trace,
                    mint=mint,
                    pool=pool,
                    ok=int(ok_b),
                    score=score_f,
                    why=why_s,
                )
                return ok_b, score_f, why_s
        except Exception as e:
            log.event("AI_GATE_ERR", trace=trace, mint=mint, err=_short_exc(e))

    # Fallback to ai_entry_logic module
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
                ok_b, why = True, "ai_entry_logic"
                if callable(fn_should):
                    with contextlib.suppress(Exception):
                        res2 = await _maybe_await(fn_should(features))  # type: ignore[misc]
                        if isinstance(res2, dict):
                            ok_b = bool(res2.get("ok", True))
                            why = get_str(res2.get("reason", why), why)
                            score = float(parse_float(res2.get("score", score), score))
                        elif isinstance(res2, (list, tuple)) and res2:
                            ok_b = bool(res2[0])
                            if len(res2) >= 2:
                                why = get_str(res2[1], why)
                            if len(res2) >= 3:
                                score = float(parse_float(res2[2], score))
                        else:
                            ok_b = bool(res2)

                ui["ai_gate_ok"], ui["ai_gate_score"], ui["ai_gate_reason"] = ok_b, score, why[:220]
                min_score = float(cfg.get("ai_gate_min_score", 0.0) or 0.0)
                if min_score and score >= 0:
                    ok_b = ok_b and (score >= min_score)
                    if not ok_b:
                        why = f"ai_score<{min_score}"

                log.event(
                    "AI_GATE",
                    trace=trace,
                    mint=mint,
                    pool=pool,
                    ok=int(ok_b),
                    score=score,
                    why=why[:220],
                )
                return ok_b, score, why[:220]
        except Exception as e:
            log.event("AI_GATE_ERR", trace=trace, mint=mint, err=_short_exc(e))

    if bool(cfg.get("ai_gate_require", False)):
        return False, -1.0, f"ai_gate_unavailable:{_AI_GATE_ERR or 'missing'}"
    return True, -1.0, "ai_gate_unavailable_allowed"


async def _momentum_ok(mint: str, *, pool: str, trace: str) -> bool:
    cfg = _cfg()
    if not bool(cfg.get("momo_enabled", False)):
        return True
    if (not looks_like_mint(mint)) or _is_pseudo_id(mint):
        return bool(pool == "pump" and bool(cfg.get("momo_allow_no_data_for_pump", True)))

    ttl = float(cfg.get("price_ttl_sec", 0.0) or 0.0)
    tries = int(cfg.get("momo_price_tries", 4) or 4)
    delay = max(float(cfg.get("momo_price_delay_s", 0.7) or 0.7), ttl + 0.10)

    px0 = await _price_ui_cached(mint, ttl_sec=ttl)
    if px0 <= 0:
        return bool(pool == "pump" and bool(cfg.get("momo_allow_no_data_for_pump", True)))

    last_px = px0
    for _ in range(max(2, tries) - 1):
        await asyncio.sleep(delay)
        last_px = await _price_ui_cached(mint, ttl_sec=ttl)
        if last_px <= 0:
            return bool(pool == "pump" and bool(cfg.get("momo_allow_no_data_for_pump", True)))

    pct_up = (last_px / px0 - 1.0) * 100.0
    ok = pct_up >= float(cfg.get("momo_min_up_pct", 0.0) or 0.0)
    if ttl >= float(cfg.get("momo_window_sec", 10.0) or 10.0) * 0.9:
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
    )
    return ok


# =============================================================================
# Sizing (wallet%, overrides, liquidity modulation)
# =============================================================================
async def _liquidity_size_boost(mint: str, base_sol: float, trace: str) -> float:
    cfg = _cfg()
    if not bool(cfg.get("liq_filter_enabled", False)) or not looks_like_mint(mint):
        return float(base_sol)

    try:
        ok, rep = await screen_token(mint)
    except Exception as e:
        log.event("LIQ_SCREEN_ERR", trace=trace, mint=mint, err=_short_exc(e))
        ok, rep = True, {"ok": True}

    min_sol = float(cfg.get("min_sol_per_trade", 0.0) or 0.0)
    max_sol = float(cfg.get("max_sol_per_trade", 0.0) or 0.0)

    if not ok:
        return max(min_sol, min(max_sol, float(base_sol) * 0.75))

    liq_usd = float(parse_float(rep.get("liqUsd") or rep.get("liq_usd") or 0.0, 0.0))
    imp_bps = int(
        parse_int(
            rep.get("priceImpactBps") or rep.get("impactBps") or rep.get("impact_bps") or 0, 0
        )
    )

    boost = 1.0
    if liq_usd <= 0:
        boost *= 0.80
    elif liq_usd < float(cfg.get("entry_min_liq_usd", 0.0) or 0.0):
        boost *= 0.70
    elif liq_usd > float(cfg.get("entry_max_liq_usd", 0.0) or 0.0):
        boost *= 1.05

    if imp_bps > 0:
        if imp_bps < int(cfg.get("entry_min_impact_bps", 0) or 0):
            boost *= 1.15
        elif imp_bps > int(cfg.get("entry_max_impact_bps", 10_000) or 10_000):
            boost *= 0.70

    sized = float(base_sol) * boost
    return max(min_sol, min(max_sol, sized))


async def _autosize_by_wallet(owner: str) -> float:
    global _last_autosize_mono, _cached_wallet_sol
    cfg = _cfg()
    now = _mono()
    refresh = float(cfg.get("autosize_refresh_sec", 10.0) or 10.0)
    if (now - _last_autosize_mono) >= refresh or _cached_wallet_sol <= 0:
        _cached_wallet_sol = await _get_sol_balance(owner)
        _last_autosize_mono = now

    min_sol = float(cfg.get("min_sol_per_trade", 0.0) or 0.0)
    max_sol = float(cfg.get("max_sol_per_trade", 0.0) or 0.0)
    step = float(cfg.get("step_sol", 0.0) or 0.0)

    base = float(_cached_wallet_sol) * float(
        clamp_float(cfg.get("risk_pct_wallet", 0.0), 0.0, 1.0, 0.0)
    )
    base = min(max_sol, max(min_sol, base))
    return _round_step(base, step)


async def _derive_spend_sol(
    *, owner: str, wallet_bal_sol: float, mint: str, ui: JsonDict, trace: str
) -> float:
    cfg = _cfg()
    min_sol = float(cfg.get("min_sol_per_trade", 0.0) or 0.0)
    max_sol = float(cfg.get("max_sol_per_trade", 0.0) or 0.0)
    step = float(cfg.get("step_sol", 0.0) or 0.0)
    reserve = float(cfg.get("reserve_sol", 0.0) or 0.0)

    base: float | None = None
    if float(cfg.get("buy_sol_per_trade", 0.0) or 0.0) > 0:
        base = float(cfg.get("buy_sol_per_trade", 0.0))

    if base is None:
        for k in ("size_sol_override", "spend_sol_override", "size_sol", "spend_sol"):
            if ui.get(k) is not None:
                base = float(parse_float(ui.get(k), 0.0))
                break

    if base is None and callable(_auto_size_sol):
        with contextlib.suppress(Exception):
            base = float(_auto_size_sol())

    if base is None:
        base = await _autosize_by_wallet(owner)

    # scalars
    scalars: list[float] = []
    for k in (
        "position_size_kelly_scalar",
        "position_size_scalar",
        "size_scalar",
        "wallet_risk_scalar",
        "risk_scalar",
    ):
        if ui.get(k) is not None:
            scalars.append(float(parse_float(ui.get(k), 1.0)))
    if ui.get("ai_gate_score") is not None:
        s = float(parse_float(ui.get("ai_gate_score"), 0.0))
        scalars.append(0.60 + 0.65 * float(clamp_float(s, 0.0, 1.0, 0.0)))
    if ui.get("pump_filter_score") is not None:
        s = float(parse_float(ui.get("pump_filter_score"), 0.0))
        scalars.append(0.65 + 0.60 * float(clamp_float(s / 100.0, 0.0, 1.0, 0.0)))
    if scalars:
        base *= float(clamp_float(max(scalars), 0.05, 3.0, 1.0))

    max_affordable = max(0.0, float(wallet_bal_sol) - max(0.0, reserve))
    if max_affordable > 0:
        base = min(float(base), max_affordable)

    base = max(min_sol, min(max_sol, float(base)))
    base = await _liquidity_size_boost(mint, base, trace)
    base = max(min_sol, min(max_sol, float(base)))
    base = _round_step(base, step)
    return max(min_sol, min(max_sol, float(base)))


# =============================================================================
# Route preflight fallback
# =============================================================================
async def _internal_preflight(
    mint: str, spend_lamports: int, trace: str
) -> tuple[bool, JsonDict | None]:
    cfg = _cfg()
    q_buy = await _router_get_buy_quote(mint, spend_lamports)
    if not q_buy:
        if not bool(cfg.get("routeguard_allow_no_route", True)):
            return False, None
        log.event("ROUTE_NO_BUY_PATH_ALLOWED", trace=trace, mint=mint, lamports_in=spend_lamports)
        return True, None

    out_raw = int(parse_int(q_buy.get("outAmount", q_buy.get("out_amount_raw", 0)), 0))
    if out_raw <= 0 and not bool(cfg.get("routeguard_allow_no_route", True)):
        return False, None

    if bool(cfg.get("routeguard_require_sell", False)):
        q_sell = await _router_get_sell_quote(mint, 1_000)
        if not q_sell:
            return False, q_buy
        lam = int(parse_int(q_sell.get("outAmount", q_sell.get("lamports_out", 0)), 0))
        if lam <= 0:
            return False, q_buy

    return True, q_buy


# =============================================================================
# PumpPortal client + helpers
# =============================================================================
class TemporaryPumpPortal(Exception):
    pass


def _pp_url_lightning() -> str | None:
    key = str(_cfg().get("pp_api_key", "") or "").strip()
    return f"https://pumpportal.fun/api/trade?api-key={key}" if key else None


def _pp_url_local() -> str | None:
    key = str(_cfg().get("pp_api_key", "") or "").strip()
    return f"https://pumpportal.fun/api/trade-local?api-key={key}" if key else None


def _pp_body(
    *,
    action: str,
    pool: str,
    payer: str,
    amount_sol: float,
    slippage_bps: int,
    priority_sol: float,
    mint: str,
) -> JsonDict:
    return {
        "action": action,
        "amount": float(amount_sol),
        "denominatedInSol": True,
        "slippage": float(slippage_bps) / 100.0,
        "priorityFee": float(priority_sol),
        "pool": pool,
        "publicKey": payer,
        "mint": str(mint),
    }


def _pp_errors(resp: JsonDict) -> list[str]:
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

    seen: set[str] = set()
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
    timeout_s = float(cfg.get("pp_timeout_sec", 18.0) or 18.0)

    if _pp_client is None or _pp_client.is_closed or abs(_pp_client_timeout_s - timeout_s) > 1e-6:
        try:
            if _pp_client is not None and not _pp_client.is_closed:
                _safe_create_task(_pp_client.aclose())
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
    try:
        if _pp_client is not None and not _pp_client.is_closed:
            with contextlib.suppress(Exception):
                await _pp_client.aclose()
    except Exception:
        pass
    _pp_client = None
    _pp_client_timeout_s = 0.0


async def _pp_qps_gate() -> None:
    global _pp_last_mono
    cfg = _cfg()
    max_qps = float(cfg.get("pp_max_qps", 1) or 1)
    min_interval = 1.0 / max(1.0, max_qps)
    async with _pp_rate_lock:
        now = _mono()
        wait_for = min_interval - (now - _pp_last_mono)
        if wait_for > 0:
            await asyncio.sleep(wait_for)
        _pp_last_mono = _mono()


async def _pp_post_json(url: str, body: JsonDict, trace: str) -> JsonDict:
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

    if bool(_cfg().get("debug", False)):
        log.event("PP_HTTP_OK", trace=trace, url=url, keys=list(js.keys())[:12])
    return cast(JsonDict, js)


async def _pp_try(url: str, body: JsonDict, trace: str) -> JsonDict:
    cfg = _cfg()
    rmax = int(cfg.get("pp_retry_max", 0) or 0)
    backoff = float(cfg.get("pp_retry_backoff", 0.7) or 0.7)
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
# Hooks / confirmation / exit engine / data logger (best-effort)
# =============================================================================
async def _fire(h: PubHook | None, payload: JsonDict, name: str) -> None:
    if h is None:
        return
    try:
        await h(payload)
    except Exception as e:
        with contextlib.suppress(Exception):
            log.warning("%s hook failed: %s", name, e)


async def _maybe_confirm(sig: str | None, trace: str) -> str | None:
    if not sig:
        return None
    cfg = _cfg()
    tout = int(cfg.get("confirm_timeout_sec", 0) or 0)
    if tout <= 0:
        return sig

    try:
        fn = _confirm_signature
        if not callable(fn):
            return sig
        commitment = str(cfg.get("confirm_commitment", "confirmed") or "confirmed")
        ok = False
        try:
            ok = bool(await _maybe_await(fn(sig, timeout=tout, commitment=commitment)))
        except TypeError:
            with contextlib.suppress(Exception):
                ok = bool(await _maybe_await(fn(sig, timeout=tout)))
        if ok:
            return sig
        log.event("CONFIRM_TIMEOUT", trace=trace, sig=sig, commitment=commitment)
        return sig
    except Exception as e:
        log.event("CONFIRM_ERROR", trace=trace, sig=sig, err=_short_exc(e))
        return sig


async def _notify_exit_engine_after_buy(payload: JsonDict) -> None:
    ee = _exit_engine
    if ee is None:
        return
    for name in (
        "on_buy_filled",
        "on_buy_fill",
        "schedule_after_buy",
        "track_new_position",
        "register_open_position",
    ):
        fn = getattr(ee, name, None)
        if not callable(fn):
            continue
        try:
            await _maybe_await(fn(payload))
            return
        except TypeError:
            with contextlib.suppress(Exception):
                await _maybe_await(fn(**payload))
                return
        except Exception:
            return


async def _log_candidate_best_effort(event: JsonDict) -> None:
    cfg = _cfg()
    if not (bool(cfg.get("log_candidates", False)) and _data_logger):
        return
    fn = (
        getattr(_data_logger, "log_entry_candidate", None)
        or getattr(_data_logger, "log_candidate", None)
        or getattr(_data_logger, "log_entry", None)
    )
    if not callable(fn):
        return
    with contextlib.suppress(Exception):
        await _maybe_await(fn(event))


async def _log_trade_best_effort(event: JsonDict) -> None:
    cfg = _cfg()
    if not (bool(cfg.get("log_trades", False)) and _data_logger):
        return
    fn = getattr(_data_logger, "log_trade", None) or getattr(_data_logger, "log_fill", None)
    if not callable(fn):
        return
    with contextlib.suppress(Exception):
        await _maybe_await(fn(event))


# =============================================================================
# Slippage decision (optional dynamic)
# =============================================================================
def _decide_slippage_bps(ui: JsonDict) -> int | None:
    cfg = _cfg()
    base = int(cfg.get("slippage_bps", 0) or 0)
    if base <= 0:
        return None
    bps = int(clamp_float(float(base), 5, 5000, float(base)))
    if not bool(cfg.get("dynamic_slippage", False)):
        return bps

    min_bps = int(cfg.get("slippage_min_bps", 50) or 50)
    max_bps = int(cfg.get("slippage_max_bps", 1500) or 1500)
    min_bps = max(5, min(min_bps, max_bps))

    impact = ui.get("impact_bps") or ui.get("priceImpactBps") or ui.get("impactBps")
    vel = ui.get("pump_filter_velocity")

    if impact is not None:
        ib = int(clamp_float(parse_float(impact, 0.0), 0.0, 5000.0, 0.0))
        bps = int(bps + clamp_float(ib * 0.35, 0.0, 600.0, 0.0))

    if vel is not None:
        v = float(clamp_float(parse_float(vel, 0.0), 0.0, 2.0, 0.0))
        if v >= 1.0:
            bps = int(bps * 1.15)

    return int(clamp_float(float(bps), float(min_bps), float(max_bps), float(bps)))


# =============================================================================
# Pseudo resolution (optional; Helius)
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
                        m2 = (ev.get(k) or "").strip()
                        if _looks_base58(m2):
                            return m2
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
                        m2 = (ev.get(k) or "").strip()
                        if _looks_base58(m2):
                            return m2
    return None


async def resolve_true_mint(pump_or_mint: str, *, signature: str | None = None) -> str | None:
    try:
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
    except Exception:
        return None


async def _maybe_resolve_pseudo_to_mint(raw_hint: str, mint: str, trace: str) -> str:
    cfg = _cfg()
    if not bool(cfg.get("resolve_pseudo_enabled", False)):
        return mint
    if not _is_pseudo_id(raw_hint or mint):
        return mint
    try:
        tout = float(cfg.get("resolve_pseudo_timeout_sec", 1.25) or 1.25)
        m = await asyncio.wait_for(resolve_true_mint_from_pseudo_id(raw_hint or mint), timeout=tout)
        if m and looks_like_mint(m):
            return m
    except Exception as e:
        log.event("PSEUDO_RESOLVE_ERR", trace=trace, raw=raw_hint, mint=mint, err=_short_exc(e))
    return mint


# =============================================================================
# Post-fill
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
    wf: JsonDict,
    trace: str,
    meta: JsonDict | None,
    on_buy_filled: PubHook | None,
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

    with contextlib.suppress(Exception):
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

    cfg = _cfg()
    if bool(cfg.get("tg_enabled", True)) and bool(cfg.get("tg_notify_filled", True)):
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
# Main: maybe_buy (public; never raises)
# =============================================================================
async def maybe_buy(
    *,
    mint: str,
    ui: JsonDict | None = None,
    on_buy_submitted: PubHook | None = None,
    on_buy_filled: PubHook | None = None,
    on_buy_failed: PubHook | None = None,
    worker_id: int | None = None,
    source: str | None = None,
) -> str | None:
    async def _run() -> str | None:
        global _last_attempt_mono_global, _last_buy_mono_global

        await _inflight.sync_from_cfg()
        cfg = _cfg()
        trace = _new_trace_id()
        phase = int(cfg.get("phase", _phase()))

        u: JsonDict = dict(ui or {})
        pool = get_str(u.get("pool"), "pump").strip().lower()
        symbol = get_str(u.get("symbol") or u.get("sym") or "", "").strip()
        raw_hint = get_str(u.get("rawMint") or u.get("raw") or mint or "", "").strip()

        mint_in = (mint or "").strip()
        if not mint_in:
            bump("buy_skip_bad_mint")
            _gr_record_buy_result(False, "bad_mint", mint=None)
            return None

        wf: JsonDict = {"worker_id": worker_id, "source": source}
        wf = {k: v for k, v in wf.items() if v is not None}

        # wallet / phase / switches
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

        if bool(cfg.get("kill_switch", False)) or bool(cfg.get("disable_buys", False)):
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

        if phase < int(cfg.get("min_phase", 1) or 1):
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

        if pool not in set(cfg.get("allowed_pools", ("pump",))):
            log.event(
                "BUY_SKIP_UNSUPPORTED_POOL",
                trace=trace,
                pool=pool,
                mint=mint_in,
                allowed=list(cfg.get("allowed_pools", ("pump",))),
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

        cooldown = float(cfg.get("buy_cooldown_sec", 0.0) or 0.0) + random.random() * max(
            0.0, float(cfg.get("buy_cooldown_jitter_sec", 0.0) or 0.0)
        )
        attempt_cd = max(2.0, cooldown * float(cfg.get("attempt_pace_frac", 0.5) or 0.5))

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

        if mint_key in _last_no_route_mono and not _cooldown_ok_mono(
            mint_key, int(cfg.get("noroute_cooldown_sec", 600) or 600)
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

        if bool(cfg.get("dedupe_enabled", True)):
            ok_claim = True
            try:
                ok_claim = bool(
                    claim_once(
                        f"buy:{pool}:{mint_key}", ttl=int(cfg.get("dedupe_ttl_sec", 600) or 600)
                    )
                )
            except TypeError:
                with contextlib.suppress(Exception):
                    ok_claim = bool(
                        claim_once(
                            f"buy:{pool}:{mint_key}",
                            _ttl=int(cfg.get("dedupe_ttl_sec", 600) or 600),
                        )
                    )
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

        mint_lock = _mint_lock(mint_key) if bool(cfg.get("per_mint_lock", True)) else None

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
            if bal < float(cfg.get("min_sol_balance", 0.0) or 0.0):
                log.event(
                    "BUY_BLOCK_LOW_BAL",
                    trace=trace,
                    owner=owner,
                    bal_sol=bal,
                    min_sol=float(cfg.get("min_sol_balance", 0.0) or 0.0),
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

            mint_for_routing = await _maybe_resolve_pseudo_to_mint(raw_hint, mint_in, trace)
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

            spend_sol = await _derive_spend_sol(
                owner=owner, wallet_bal_sol=bal, mint=mint_for_routing, ui=u, trace=trace
            )
            reserve = float(cfg.get("reserve_sol", 0.0) or 0.0)
            max_afford = max(0.0, float(bal) - max(0.0, reserve))
            if max_afford > 0:
                spend_sol = min(float(spend_sol), float(max_afford))

            spend_sol = min(
                float(cfg.get("max_sol_per_trade", spend_sol) or spend_sol),
                max(float(cfg.get("min_sol_per_trade", spend_sol) or spend_sol), float(spend_sol)),
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
                float(cfg.get("max_sol_per_trade", spend_sol) or spend_sol),
                max(float(cfg.get("min_sol_per_trade", spend_sol) or spend_sol), float(spend_sol)),
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
                    "ai_gate_score": float(
                        parse_float(u.get("ai_gate_score"), ai_score if ai_score >= 0 else -1.0)
                    ),
                    "ai_gate_reason": get_str(u.get("ai_gate_reason") or ai_reason, "")[:160],
                    "pf_score": float(parse_float(u.get("pump_filter_score"), -1.0)),
                    "pf_reason": get_str(u.get("pump_filter_reason") or "", "")[:160],
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
                rm_reason=rm_reason,
                **(rm_extra or {}),
                **wf,
            )

            if bool(cfg.get("dryrun", False)):
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

            skip_jupiter = (
                _is_pseudo_id(raw_hint or mint_in)
                or (not looks_like_mint(mint_for_routing))
                or (pool == "pump" and bool(cfg.get("prefer_pp_for_pump_pool", False)))
            )

            allow_buy, q_buy = True, None
            if not skip_jupiter:
                if rg_preflight is not None:
                    try:
                        try:
                            allow_buy, q_buy = await rg_preflight(
                                mint=mint_for_routing, lamports_in=lamports_in
                            )
                        except TypeError:
                            allow_buy, q_buy = await rg_preflight(mint_for_routing, lamports_in)
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

            jupiter_ok_path = (not skip_jupiter) and (q_buy is not None)

            async with _inflight:
                # Jupiter first
                if jupiter_ok_path:
                    await _fire(
                        on_buy_submitted,
                        {
                            "mint": mint_for_routing,
                            "mint_key": mint_key,
                            "pool": pool,
                            "symbol": symbol,
                            "spend_sol": spend_sol,
                            "route": "jupiter",
                            "trace": trace,
                            **wf,
                        },
                        "on_buy_submitted",
                    )
                    slippage_bps = _decide_slippage_bps(u)
                    jito_tip = (
                        float(cfg.get("jito_tip_sol", 0.0) or 0.0)
                        if float(cfg.get("jito_tip_sol", 0.0) or 0.0) > 0
                        else None
                    )
                    cu = (
                        int(cfg.get("compute_units", 0) or 0)
                        if int(cfg.get("compute_units", 0) or 0) > 0
                        else None
                    )

                    sig: str | None = None
                    meta: JsonDict | None = None
                    for i in range(int(cfg.get("jup_retries", 1) or 1)):
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
                            compute_units=cu,
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

                # PumpPortal fallback
                pp_url = _pp_url_lightning()
                if not pp_url:
                    log.event("BUY_NO_PP_KEY", trace=trace, mint=mint_for_routing, pool=pool, **wf)
                    _gr_record_buy_result(False, "pp_key_missing", mint=mint_for_routing)
                    bump("buys_fail")
                    return None

                candidates = _pp_candidates(raw_hint, mint_for_routing)
                last_err_short = "no_route"

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
                        payer=owner,
                        amount_sol=spend_sol,
                        slippage_bps=int(
                            clamp_float(float(cfg.get("slippage_bps", 600) or 600), 5, 5000, 600.0)
                        ),
                        priority_sol=float(cfg.get("priority_sol", 0.0) or 0.0),
                        mint=send_mint,
                    )

                    try:
                        resp = await _pp_try(pp_url, body, trace)
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

                        if not bool(cfg.get("use_local_fallback", False)):
                            continue

                        pp_local = _pp_url_local()
                        if not pp_local:
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
                            resp2 = await _pp_try(pp_local, body, trace)
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
                                    str(tx_b64),
                                    kp,
                                    skip_preflight=bool(cfg.get("skip_preflight", True)),
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
                            str(tx_b64), kp, skip_preflight=bool(cfg.get("skip_preflight", True))
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

        if mint_lock is not None:
            async with mint_lock:
                return await _core()
        return await _core()

    try:
        return await asyncio.wait_for(
            _run(), timeout=float(_cfg().get("hard_timeout_sec", 45.0) or 45.0)
        )
    except asyncio.TimeoutError:
        bump("buy_timeout")
        _gr_record_buy_result(False, "timeout", mint=(mint or "").strip() or None)
        log.event(
            "BUY_TIMEOUT",
            mint=mint,
            timeout_sec=float(_cfg().get("hard_timeout_sec", 45.0) or 45.0),
        )
        return None
    except Exception as e:
        bump("buy_exception")
        _gr_record_buy_result(False, "exception", mint=(mint or "").strip() or None)
        log.event("BUY_EXCEPTION", mint=mint, err=_short_exc(e))
        return None


# =============================================================================
# Direct Pump.fun helpers (public; never raise)
# =============================================================================
async def buy_pump_token(
    *, mint: str, sol_amount: float, slippage_bps: int, pool: str = "pump"
) -> JsonDict:
    trace = _new_trace_id()
    try:
        cfg = _cfg()
        m = (mint or "").strip()
        if sol_amount <= 0:
            return {"ok": False, "error": "sol_amount must be > 0", "mint": m, "trace": trace}

        pp_url = _pp_url_lightning()
        if not pp_url:
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
            priority_sol=float(cfg.get("priority_sol", 0.0) or 0.0),
            mint=m,
        )

        last_err: str | None = None
        try:
            resp = await _pp_try(pp_url, body, trace)
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
                        str(tx_b64), kp, skip_preflight=bool(cfg.get("skip_preflight", True))
                    )
                    sig2 = await _maybe_confirm(sig2, trace)
                    if sig2:
                        _gr_record_open(float(sol_amount), mint=m)
                        _gr_record_buy_result(
                            True, "executed_pp_remote_sign", sig=str(sig2), mint=m
                        )
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

        if bool(cfg.get("use_local_fallback", False)):
            pp_local = _pp_url_local()
            if pp_local:
                try:
                    resp2 = await _pp_try(pp_local, body, trace)
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
                                str(tx_b64),
                                kp,
                                skip_preflight=bool(cfg.get("skip_preflight", True)),
                            )
                            sig2 = await _maybe_confirm(sig2, trace)
                            if sig2:
                                _gr_record_open(float(sol_amount), mint=m)
                                _gr_record_buy_result(
                                    True, "executed_pp_local_sign", sig=str(sig2), mint=m
                                )
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
    except Exception as e:
        return {
            "ok": False,
            "error": f"exception:{_short_exc(e)}",
            "mint": (mint or "").strip(),
            "trace": trace,
        }


async def buy_direct_lamports(
    *, mint: str, in_amount_lamports: int, slippage_bps: int, pool: str = "pump"
) -> JsonDict:
    try:
        if in_amount_lamports <= 0:
            return {
                "ok": False,
                "error": "in_amount_lamports must be > 0",
                "mint": (mint or "").strip(),
            }
        sol_amount = float(in_amount_lamports) / LAMPORTS_PER_SOL_F
        return await buy_pump_token(
            mint=mint, sol_amount=sol_amount, slippage_bps=int(slippage_bps), pool=pool
        )
    except Exception as e:
        return {"ok": False, "error": f"exception:{_short_exc(e)}", "mint": (mint or "").strip()}


__all__ = [
    "buy_direct_lamports",
    "buy_pump_token",
    "close_pp_http",
    "looks_like_mint",
    "maybe_buy",
    "resolve_true_mint",
    "resolve_true_mint_from_pseudo_id",
    "resolve_true_mint_from_signature",
]
