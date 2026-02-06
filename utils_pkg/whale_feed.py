from __future__ import annotations

"""
utils_pkg/whale_feed.py (P0 — production)
=========================================

Purpose
-------
"Whale feed" watches high-liquidity / high-activity pairs and detects
clustered buy bursts + short-window momentum (a proxy for whale-style flow).

SAFE DEFAULT:
  - QUEUE mode: publish candidates to Redis for the canonical sniper/entry pipeline.

OPTIONAL:
  - TRADE mode: execute buys directly via Jupiter (only if WHALE_FEED_MODE=TRADE).

P0 Lock-in Principles
---------------------
- Safe-by-default (no trading unless explicitly enabled)
- No competing execution engines by default (QUEUE feeds canonical path)
- Guardrails:
    * allow-entry gate (risk_manager if available)
    * liquidity + estimated impact caps
    * buy-cluster gate (buys over a window)
    * optional momentum gates (vol/price change)
    * cooldown + distributed lock (Redis)
    * optional route safety checks (route_guard / routeguard)
- Observability: logs + optional Telegram
- Import must be light: no network at import time, no event loop creation.

Phase A Note
------------
This file is hardened against Logger ↔ EventLogger incompatibilities by:
- Avoiding incompatible Callable assignments for get_logger/setup_logging
- Using a thin adapter that guarantees .debug/.info/.warning/.error/.exception + .event/.every
  without monkey-patching methods onto logger objects.
"""

import asyncio
import contextlib
import importlib
import inspect
import json
import logging
import os
import random
import signal
import time
from dataclasses import asdict, dataclass
from typing import Any, Callable, Protocol, cast

JsonDict = dict[str, Any]

# =============================================================================
# Logging (no side-effects at import)
# =============================================================================


class LoggerLike(Protocol):
    def debug(self, msg: str, *args: Any, **kwargs: Any) -> None: ...
    def info(self, msg: str, *args: Any, **kwargs: Any) -> None: ...
    def warning(self, msg: str, *args: Any, **kwargs: Any) -> None: ...
    def error(self, msg: str, *args: Any, **kwargs: Any) -> None: ...
    def exception(self, msg: str, *args: Any, **kwargs: Any) -> None: ...

    # structured helpers (guaranteed by adapter)
    def event(self, name: str, **kw: Any) -> None: ...
    def every(self, key: str, n: int = 10, msg: str | None = None, **kw: Any) -> None: ...


class _LogAdapter:
    """
    Adapter that normalizes Logger/EventLogger differences without mutating the underlying object.
    Guarantees event/every exist.
    """

    __slots__ = ("_base", "_every_store")

    def __init__(self, base: Any) -> None:
        self._base = base
        self._every_store: dict[str, float] = {}

    def debug(self, msg: str, *args: Any, **kwargs: Any) -> None:
        fn = getattr(self._base, "debug", None)
        if callable(fn):
            fn(msg, *args, **kwargs)
            return
        logging.getLogger(__name__).debug(msg, *args, **kwargs)

    def info(self, msg: str, *args: Any, **kwargs: Any) -> None:
        fn = getattr(self._base, "info", None)
        if callable(fn):
            fn(msg, *args, **kwargs)
            return
        logging.getLogger(__name__).info(msg, *args, **kwargs)

    def warning(self, msg: str, *args: Any, **kwargs: Any) -> None:
        fn = getattr(self._base, "warning", None)
        if callable(fn):
            fn(msg, *args, **kwargs)
            return
        logging.getLogger(__name__).warning(msg, *args, **kwargs)

    def error(self, msg: str, *args: Any, **kwargs: Any) -> None:
        fn = getattr(self._base, "error", None)
        if callable(fn):
            fn(msg, *args, **kwargs)
            return
        logging.getLogger(__name__).error(msg, *args, **kwargs)

    def exception(self, msg: str, *args: Any, **kwargs: Any) -> None:
        fn = getattr(self._base, "exception", None)
        if callable(fn):
            fn(msg, *args, **kwargs)
            return
        logging.getLogger(__name__).exception(msg, *args, **kwargs)

    def event(self, name: str, **kw: Any) -> None:
        fn = getattr(self._base, "event", None)
        if callable(fn):
            with contextlib.suppress(Exception):
                fn(name, **kw)
                return
        # fallback: single-line structured-ish info
        try:
            self.info("%s %s", name, kw)
        except Exception:
            # never let logging crash worker
            return

    def every(self, key: str, n: int = 10, msg: str | None = None, **kw: Any) -> None:
        fn = getattr(self._base, "every", None)
        if callable(fn):
            with contextlib.suppress(Exception):
                fn(key, n=n, msg=msg, **kw)
                return

        # fallback: log at most once per `n` seconds per key
        try:
            now = time.monotonic()
            last = float(self._every_store.get(key, 0.0))
            interval = float(max(1, int(n)))
            if (now - last) >= interval:
                self._every_store[key] = now
                self.event(msg or key, **kw)
        except Exception:
            return


def _fallback_setup_logging(*_a: Any, **_k: Any) -> None:
    return


def _fallback_get_logger(name: str | None = None, *_a: Any, **_k: Any) -> logging.Logger:
    return logging.getLogger(name or "whale_feed")


# Import compat layer without mid-file import statements (keeps ruff/isort + mypy happy)
_compat_get_logger: Callable[..., Any] = _fallback_get_logger
_compat_setup_logging: Callable[..., Any] = _fallback_setup_logging

try:  # preferred: compat layer (safe import; no network / no side effects expected)
    _lc = importlib.import_module("utils_pkg.log_compat")
    _compat_get_logger = cast(Callable[..., Any], getattr(_lc, "get_logger", _fallback_get_logger))
    _compat_setup_logging = cast(
        Callable[..., Any], getattr(_lc, "setup_logging", _fallback_setup_logging)
    )
except Exception:  # pragma: no cover
    pass


def _get_log(name: str) -> LoggerLike:
    base = None
    with contextlib.suppress(Exception):
        base = _compat_get_logger(name)
    if base is None:
        base = _fallback_get_logger(name)
    return cast(LoggerLike, _LogAdapter(base))


log = _get_log("whale")

# Optional metrics hook (no-op if not present)
try:  # pragma: no cover
    from logging_setup import bump  # type: ignore
except Exception:  # pragma: no cover

    def bump(*_a: Any, **_k: Any) -> None:  # type: ignore
        return None


# =============================================================================
# Optional integrations (must never crash import)
# =============================================================================


def _opt_attr(module_name: str, attr: str) -> Any:
    try:
        mod = importlib.import_module(module_name)
        return getattr(mod, attr)
    except Exception:
        return None


# core utils (prefer utils_pkg.core_utils, fall back to root core_utils)
init_resources: Callable[..., Any] | None = _opt_attr("utils_pkg.core_utils", "init_resources")
if init_resources is None:
    init_resources = _opt_attr("core_utils", "init_resources")

close_resources: Callable[..., Any] | None = _opt_attr("utils_pkg.core_utils", "close_resources")
if close_resources is None:
    close_resources = _opt_attr("core_utils", "close_resources")

get_redis_async: Callable[..., Any] | None = _opt_attr("utils_pkg.core_utils", "get_redis_async")
if get_redis_async is None:
    get_redis_async = _opt_attr("core_utils", "get_redis_async")

send_telegram: Callable[..., Any] | None = _opt_attr("utils_pkg.core_utils", "send_telegram")
if send_telegram is None:
    send_telegram = _opt_attr("core_utils", "send_telegram")

convert_sol_to_usd: Callable[..., Any] | None = _opt_attr(
    "utils_pkg.core_utils", "convert_sol_to_usd"
)
if convert_sol_to_usd is None:
    convert_sol_to_usd = _opt_attr("core_utils", "convert_sol_to_usd")

swap_token_jupiter: Callable[..., Any] | None = _opt_attr(
    "utils_pkg.core_utils", "swap_token_jupiter"
)
if swap_token_jupiter is None:
    swap_token_jupiter = _opt_attr("core_utils", "swap_token_jupiter")

# risk manager (root-level)
current_limits: Callable[..., Any] | None = _opt_attr("risk_manager", "current_limits")
should_allow_new_entry: Callable[..., Any] | None = _opt_attr(
    "risk_manager", "should_allow_new_entry"
)
note_spend: Callable[..., Any] | None = _opt_attr("risk_manager", "note_spend")

# shadow utils (root-level)
shadow_take: Callable[..., Any] | None = _opt_attr("shadow_utils", "shadow_take")
shadow_log: Callable[..., Any] | None = _opt_attr("shadow_utils", "shadow_log")

# route guard: try both naming conventions and locations
precheck_buy: Callable[..., Any] | None = (
    _opt_attr("route_guard", "precheck_buy")
    or _opt_attr("utils_pkg.routeguard", "precheck_buy")
    or _opt_attr("utils_pkg.routeguard", "precheck")
)
posttrade_watchdog: Callable[..., Any] | None = (
    _opt_attr("route_guard", "posttrade_watchdog")
    or _opt_attr("utils_pkg.routeguard", "posttrade_watchdog")
    or _opt_attr("utils_pkg.routeguard", "watchdog")
)

# quote prewarm (root-level)
prewarm_quotes: Callable[..., Any] | None = _opt_attr("jup_quote_cache", "prewarm_quotes")

# =============================================================================
# Env helpers (no side effects)
# =============================================================================

_TRUE = {"1", "true", "t", "yes", "y", "on"}
_FALSE = {"0", "false", "f", "no", "n", "off"}


def _env_bool(name: str, default: str = "0") -> bool:
    v = os.getenv(name, default)
    s = (str(v).strip().lower()) if v is not None else default.strip().lower()
    if s in _TRUE:
        return True
    if s in _FALSE:
        return False
    return default.strip().lower() in _TRUE


def _env_int(name: str, default: int, *, lo: int | None = None, hi: int | None = None) -> int:
    raw = os.getenv(name, "")
    try:
        val = int(raw.strip()) if raw.strip() else int(default)
    except Exception:
        val = int(default)
    if lo is not None:
        val = max(lo, val)
    if hi is not None:
        val = min(hi, val)
    return val


def _env_float(
    name: str, default: float, *, lo: float | None = None, hi: float | None = None
) -> float:
    raw = os.getenv(name, "")
    try:
        val = float(raw.strip()) if raw.strip() else float(default)
    except Exception:
        val = float(default)
    if lo is not None:
        val = max(lo, val)
    if hi is not None:
        val = min(hi, val)
    return val


def _safe_float(x: Any, default: float = 0.0) -> float:
    try:
        return float(x)
    except Exception:
        return default


def _safe_int(x: Any, default: int = 0) -> int:
    try:
        return int(x)
    except Exception:
        return default


def _now_ts() -> float:
    return float(time.time())


# =============================================================================
# Constants / configuration (DO NOT introduce new env keys here)
# =============================================================================

SOL_MINT: str = "So11111111111111111111111111111111111111112"

DEX_SEARCH_API: str = os.getenv(
    "WHALE_DEX_SEARCH_API",
    "https://api.dexscreener.com/latest/dex/search?q=sol",
)

WHALE_FEED_ENABLED: bool = _env_bool("WHALE_FEED_ENABLED", "1")

# SAFE DEFAULT: publish to queue (no direct execution)
WHALE_FEED_MODE: str = os.getenv("WHALE_FEED_MODE", "QUEUE").strip().upper()  # QUEUE | TRADE
if WHALE_FEED_MODE not in ("QUEUE", "TRADE"):
    WHALE_FEED_MODE = "QUEUE"

POLL_SEC: int = _env_int("WHALE_POLL_SEC", 20, lo=3, hi=300)
JITTER_SEC: float = _env_float("WHALE_JITTER_SEC", 1.0, lo=0.0, hi=10.0)

COOLDOWN_SEC: int = _env_int("WHALE_ENTRY_COOLDOWN_SEC", 300, lo=0, hi=3600)
LOCK_TTL_SEC: int = _env_int("WHALE_LOCK_TTL_SEC", 120, lo=10, hi=600)

MAX_SCAN_PAIRS: int = _env_int("WHALE_MAX_SCAN_PAIRS", 60, lo=10, hi=500)
MAX_EMIT_PER_TICK: int = _env_int("WHALE_MAX_EMIT_PER_TICK", 20, lo=1, hi=250)  # queue spam control
MAX_TRADES_PER_TICK: int = _env_int("WHALE_MAX_TRADES_PER_TICK", 1, lo=0, hi=10)  # trade mode only

# Gates
MIN_LIQ_USD: float = _env_float("AI_MIN_LIQ_USD", 20_000.0, lo=0.0)
MAX_IMPACT_BPS: float = _env_float(
    "WHALE_MAX_IMPACT_BPS",
    _env_float("AI_MAX_IMPACT_BPS", 150.0, lo=1.0),
    lo=1.0,
)

# "Honeypot-ish" sell tax gate (Dexscreener info.sellTax best-effort)
HONEYPOT_MAX_SELL_TAX_BPS: float = _env_float("HONEYPOT_MAX_SELL_TAX_BPS", 800.0, lo=0.0)

# Whale cluster gate (buy burst)
CLUSTER_MIN_BUYS: int = _env_int("WHALE_CLUSTER_MIN_BUYS", 2, lo=1, hi=50)
CLUSTER_WINDOW_MIN: int = _env_int("WHALE_CLUSTER_WINDOW_MIN", 5, lo=1, hi=1440)

# Optional momentum filters (helps avoid dead coins)
WHALE_MIN_VOL_M5_USD: float = _env_float("WHALE_MIN_VOL_M5_USD", 1500.0, lo=0.0)
WHALE_MIN_PRICECHANGE_M5: float = _env_float(
    "WHALE_MIN_PRICECHANGE_M5", 0.5, lo=-100.0, hi=10_000.0
)

# Scoring (bounded 0..1)
WHALE_SCORE_MIN: float = _env_float("WHALE_SCORE_MIN", 0.60, lo=0.0, hi=1.0)

# Slippage (trade mode)
JUPITER_MAX_SLIPPAGE: float = _env_float(
    "JUPITER_MAX_SLIPPAGE",
    _env_float("JUPITER_SLIPPAGE", 0.005, lo=0.0001, hi=0.50),
    lo=0.0001,
    hi=0.50,
)
SLIPPAGE_BPS: int = max(1, int(JUPITER_MAX_SLIPPAGE * 10_000))

# Publishing (queue mode)
REDIS_QUEUE_KEY: str = os.getenv("WHALE_QUEUE_KEY", "entry_queue:whale")
REDIS_COOLDOWN_KEY_PREFIX: str = os.getenv("WHALE_COOLDOWN_KEY_PREFIX", "cd:whale:")
REDIS_LOCK_KEY_PREFIX: str = os.getenv("WHALE_LOCK_KEY_PREFIX", "lock:whale:")

# Quote prewarm
PREWARM_ENABLED: bool = _env_bool("WHALE_PREWARM_ENABLED", "1")
PREWARM_TOP_N: int = _env_int("WHALE_PREWARM_TOP_N", 8, lo=0, hi=50)

# Notifications
TELEGRAM_ON_START: bool = _env_bool("WHALE_TELEGRAM_ON_START", "1")
TELEGRAM_ON_CANDIDATE: bool = _env_bool("WHALE_TELEGRAM_ON_CANDIDATE", "0")
TELEGRAM_ON_TRADE: bool = _env_bool("WHALE_TELEGRAM_ON_TRADE", "1")

# Shadow
WHALE_SHADOW: bool = _env_bool("WHALE_SHADOW", "0")

# Local fallback cooldown when Redis is not available
_LOCAL_COOLDOWN: dict[str, int] = {}


# =============================================================================
# Data model
# =============================================================================
@dataclass(frozen=True, slots=True)
class WhaleCandidate:
    ts: float
    source: str
    mint: str
    symbol: str
    pair_url: str

    liquidity_usd: float
    vol_m5_usd: float
    price_change_m5: float

    buys_win: int
    sells_win: int
    window_min: int

    sell_tax_bps: float
    est_impact_bps: float
    score: float

    raw: JsonDict


# =============================================================================
# Generic helpers (async/sync compat)
# =============================================================================
async def _maybe_await(x: Any) -> Any:
    if inspect.isawaitable(x):
        return await x
    return x


async def _call_first_ok(
    fn: Callable[..., Any],
    variants: list[tuple[tuple[Any, ...], dict[str, Any]]],
) -> Any:
    """
    Try calling `fn` with argument variants until one doesn't TypeError.
    Other exceptions propagate to caller.
    """
    for args, kwargs in variants:
        try:
            return await _maybe_await(fn(*args, **kwargs))
        except TypeError:
            continue
    return await _maybe_await(fn())


async def _wait_or_stop(stop_event: asyncio.Event, seconds: float) -> None:
    """
    Sleep for `seconds`, but return early if stop_event is set.
    """
    timeout = max(0.2, float(seconds))
    try:
        await asyncio.wait_for(stop_event.wait(), timeout=timeout)
    except asyncio.TimeoutError:
        return


async def _tg(msg: str) -> None:
    if send_telegram is None:
        log.info("[telegram disabled] %s", msg)
        return
    with contextlib.suppress(Exception):
        await _maybe_await(send_telegram(msg))


async def _get_redis() -> Any:
    if get_redis_async is None:
        return None
    try:
        return await _maybe_await(get_redis_async())
    except Exception:
        log.debug("get_redis_async failed", exc_info=True)
        return None


# =============================================================================
# Risk gates (optional)
# =============================================================================
async def _allow_new_entry(redis_obj: Any) -> bool:
    if should_allow_new_entry is None:
        return True
    try:
        out = await _call_first_ok(
            should_allow_new_entry,
            variants=[((redis_obj,), {}), ((None,), {}), ((), {})],
        )
        return bool(out)
    except Exception:
        # safest operational stance: don't brick the worker on risk gate errors
        return True


async def _current_ticket_usd(redis_obj: Any) -> float:
    """
    Best-effort ticket sizing:
      - fallback env chain: WHALE_TICKET_USD -> BUY_TICKET_USD -> PF_TICKET_USD
      - optionally consult risk_manager.current_limits() if present.
    """
    fallback = _env_float(
        "WHALE_TICKET_USD",
        _env_float("BUY_TICKET_USD", _env_float("PF_TICKET_USD", 20.0)),
    )
    fallback = max(0.5, min(fallback, 500.0))

    if current_limits is None:
        return fallback

    try:
        lim = await _call_first_ok(
            current_limits, variants=[((redis_obj,), {}), ((None,), {}), ((), {})]
        )
        if not isinstance(lim, dict):
            return fallback
        t = _safe_float(lim.get("ticket_usd", fallback), fallback)
        return max(0.5, min(t, 500.0))
    except Exception:
        return fallback


def _impact_bps(liq_usd: float, ticket_usd: float) -> float:
    if liq_usd <= 0.0:
        return 9e9
    return (ticket_usd / liq_usd) * 10_000.0


def _sell_tax_to_bps(v: Any) -> float:
    """
    Dexscreener's info.sellTax can vary:
      - 0.08  (fraction)
      - 8     (percent)
      - 800   (bps-like)
    Normalize to bps conservatively.
    """
    x = _safe_float(v, 0.0)
    if x <= 0.0:
        return 0.0
    if 0.0 < x <= 1.0:
        return x * 10_000.0
    if 1.0 < x <= 100.0:
        return x * 100.0
    return x


def _pick_window_txns(pair: JsonDict, window_min: int) -> tuple[int, int, int]:
    """
    Best-effort mapping to Dexscreener txns windows:
      - m5
      - m15 (sometimes present)
      - h1
      - h24
    Choose the smallest available window >= requested; else the largest available.
    Returns (buys, sells, actual_window_min_used).
    """
    txns = pair.get("txns") or {}
    if not isinstance(txns, dict):
        return 0, 0, window_min

    candidates: list[tuple[int, str]] = [(5, "m5"), (15, "m15"), (60, "h1"), (1440, "h24")]
    available: list[tuple[int, str]] = [
        (mins, key) for mins, key in candidates if isinstance(txns.get(key), dict)
    ]
    if not available:
        return 0, 0, window_min

    bigger_or_equal = [t for t in available if t[0] >= window_min]
    mins, key = (
        min(bigger_or_equal, key=lambda t: t[0])
        if bigger_or_equal
        else max(available, key=lambda t: t[0])
    )

    obj = txns.get(key) or {}
    if not isinstance(obj, dict):
        return 0, 0, mins

    buys = _safe_int(obj.get("buys", 0))
    sells = _safe_int(obj.get("sells", 0))
    return buys, sells, mins


def _compute_score(
    liq_usd: float, vol_m5: float, pct_m5: float, buys_win: int, sell_tax_bps: float
) -> float:
    """
    Bounded composite score in [0..1], tuned for whale-ish bursts:
      - more buys in window
      - decent vol_m5
      - positive price change
      - lower sell tax
      - higher liquidity (soft)
    """
    buys_norm = (buys_win - CLUSTER_MIN_BUYS) / 20.0
    buys_norm = max(0.0, min(1.0, buys_norm))

    vol_norm = (vol_m5 - WHALE_MIN_VOL_M5_USD) / 50_000.0
    vol_norm = max(0.0, min(1.0, vol_norm))

    px_norm = (pct_m5 - WHALE_MIN_PRICECHANGE_M5) / 12.0
    px_norm = max(0.0, min(1.0, px_norm))

    tax_norm = sell_tax_bps / max(1.0, HONEYPOT_MAX_SELL_TAX_BPS)
    tax_norm = max(0.0, min(1.0, tax_norm))
    tax_bonus = 1.0 - tax_norm

    liq_norm = (liq_usd - MIN_LIQ_USD) / max(1.0, 250_000.0)
    liq_norm = max(0.0, min(1.0, liq_norm))

    score = 0.34 * buys_norm + 0.24 * vol_norm + 0.20 * px_norm + 0.14 * tax_bonus + 0.08 * liq_norm
    return max(0.0, min(1.0, score))


# =============================================================================
# Redis ops (async or sync clients)
# =============================================================================
async def _redis_call(r: Any, method: str, *args: Any, **kwargs: Any) -> Any:
    if r is None:
        return None
    fn = getattr(r, method, None)
    if fn is None:
        return None
    try:
        out = fn(*args, **kwargs)
        return await _maybe_await(out)
    except Exception:
        return None


async def _redis_set_nx_ex(r: Any, key: str, value: str, ex: int) -> bool:
    """
    Best-effort NX+EX set supporting multiple redis clients.
    Returns True if set applied, False otherwise.
    """
    if r is None:
        return True
    out = await _redis_call(r, "set", key, value, nx=True, ex=int(ex))
    if out is not None:
        return bool(out)
    out = await _redis_call(r, "set", key, value, ex=int(ex), nx=True)
    if out is not None:
        return bool(out)
    return True


def _prune_local_cooldown(now: int) -> None:
    """
    Keep local cooldown dict from growing unbounded when Redis is absent.
    """
    if len(_LOCAL_COOLDOWN) <= 5000:
        return
    cutoff = now - int(max(60, COOLDOWN_SEC * 2))
    stale = [k for k, v in _LOCAL_COOLDOWN.items() if int(v) < cutoff]
    for k in stale[:2000]:
        _LOCAL_COOLDOWN.pop(k, None)


async def _cooldown_ok(r: Any, mint: str) -> bool:
    now = int(time.time())
    if r is None:
        _prune_local_cooldown(now)
        last = _LOCAL_COOLDOWN.get(mint, 0)
        if now - last >= COOLDOWN_SEC:
            _LOCAL_COOLDOWN[mint] = now
            return True
        return False

    key = f"{REDIS_COOLDOWN_KEY_PREFIX}{mint}"
    try:
        return await _redis_set_nx_ex(r, key, str(now), ex=COOLDOWN_SEC)
    except Exception:
        log.debug("cooldown redis op failed", exc_info=True)
        return True


async def _try_lock(r: Any, mint: str) -> bool:
    if r is None:
        return True
    key = f"{REDIS_LOCK_KEY_PREFIX}{mint}"
    try:
        return await _redis_set_nx_ex(r, key, "1", ex=LOCK_TTL_SEC)
    except Exception:
        log.debug("lock redis op failed", exc_info=True)
        return True


async def _unlock(r: Any, mint: str) -> None:
    if r is None:
        return
    key = f"{REDIS_LOCK_KEY_PREFIX}{mint}"
    with contextlib.suppress(Exception):
        await _redis_call(r, "delete", key)


async def _publish(r: Any, c: WhaleCandidate) -> None:
    payload = json.dumps(asdict(c), separators=(",", ":"), ensure_ascii=False)
    if r is None:
        log.info("[queue-fallback] %s", payload)
        return
    try:
        out = await _redis_call(r, "rpush", REDIS_QUEUE_KEY, payload)
        if out is None:
            await _redis_call(r, "lpush", REDIS_QUEUE_KEY, payload)
    except Exception:
        log.exception("Failed to publish whale candidate to Redis queue")


# =============================================================================
# Quote helpers
# =============================================================================
async def _usd_per_sol() -> float:
    if convert_sol_to_usd is None:
        return 0.0
    try:
        out = await _call_first_ok(convert_sol_to_usd, variants=[((1.0,), {}), ((), {})])
        return float(out or 0.0)
    except Exception:
        return 0.0


async def _maybe_prewarm(pairs: list[JsonDict], ticket_usd: float) -> None:
    if not PREWARM_ENABLED or prewarm_quotes is None or PREWARM_TOP_N <= 0:
        return

    usd_per_sol = await _usd_per_sol()
    if usd_per_sol <= 0.0:
        return

    amount_sol = ticket_usd / usd_per_sol

    top = sorted(
        pairs,
        key=lambda x: _safe_float((x.get("liquidity") or {}).get("usd", 0.0), 0.0),
        reverse=True,
    )[:PREWARM_TOP_N]

    mints: list[str] = []
    for pair in top:
        base = pair.get("baseToken") or {}
        if isinstance(base, dict):
            addr = base.get("address")
            if addr:
                mints.append(str(addr))

    if not mints:
        return

    try:
        await _call_first_ok(
            prewarm_quotes,
            variants=[
                ((mints, float(amount_sol), int(SLIPPAGE_BPS)), {}),
                ((mints, float(amount_sol)), {"slippage_bps": int(SLIPPAGE_BPS)}),
                ((mints,), {}),
            ],
        )
        log.debug("Prewarmed %d quotes (amount_sol=%.6f)", len(mints), amount_sol)
    except Exception:
        log.debug("prewarm_quotes failed", exc_info=True)


# =============================================================================
# HTTP (lazy usage)
# =============================================================================
async def _fetch_pairs(http: Any) -> list[JsonDict]:
    try:
        async with http.get(DEX_SEARCH_API, timeout=15) as r:
            if getattr(r, "status", 0) != 200:
                txt = await r.text()
                log.warning(
                    "Dexscreener fetch [%s] %s: %s",
                    r.status,
                    DEX_SEARCH_API,
                    (txt or "")[:250],
                )
                bump("whale_fetch_non_200")
                return []
            js = await r.json()
            pairs = js.get("pairs", []) if isinstance(js, dict) else []
            bump("whale_fetch_ok")
            return pairs if isinstance(pairs, list) else []
    except asyncio.CancelledError:
        raise
    except Exception as e:
        log.warning("Dexscreener fetch error: %s", e)
        bump("whale_fetch_err")
        return []


def _build_candidates(pairs: list[JsonDict], ticket_usd: float) -> list[WhaleCandidate]:
    out: list[WhaleCandidate] = []
    scan = pairs[: min(len(pairs), max(10, MAX_SCAN_PAIRS))]

    for pair in scan:
        base = pair.get("baseToken") or {}
        if not isinstance(base, dict):
            continue

        mint = str(base.get("address") or "").strip()
        if not mint:
            continue

        symbol = str(base.get("symbol") or base.get("name") or "UNK")
        pair_url = str(pair.get("url") or "")

        liq = _safe_float((pair.get("liquidity") or {}).get("usd", 0.0), 0.0)
        if liq < MIN_LIQ_USD:
            continue

        vol_m5 = _safe_float((pair.get("volume") or {}).get("m5", 0.0), 0.0)
        if vol_m5 < WHALE_MIN_VOL_M5_USD:
            continue

        pct_m5 = _safe_float((pair.get("priceChange") or {}).get("m5", 0.0), 0.0)
        if pct_m5 < WHALE_MIN_PRICECHANGE_M5:
            continue

        buys_win, sells_win, win_used = _pick_window_txns(pair, CLUSTER_WINDOW_MIN)
        if buys_win < CLUSTER_MIN_BUYS:
            continue

        info = pair.get("info") or {}
        sell_tax_raw = info.get("sellTax", 0.0) if isinstance(info, dict) else 0.0
        sell_tax_bps = _sell_tax_to_bps(sell_tax_raw)
        if sell_tax_bps > HONEYPOT_MAX_SELL_TAX_BPS:
            continue

        imp = _impact_bps(liq, ticket_usd)
        if imp > MAX_IMPACT_BPS:
            continue

        score = _compute_score(liq, vol_m5, pct_m5, buys_win, sell_tax_bps)
        if score < WHALE_SCORE_MIN:
            continue

        out.append(
            WhaleCandidate(
                ts=_now_ts(),
                source="dexscreener_whale",
                mint=mint,
                symbol=symbol,
                pair_url=pair_url,
                liquidity_usd=float(liq),
                vol_m5_usd=float(vol_m5),
                price_change_m5=float(pct_m5),
                buys_win=int(buys_win),
                sells_win=int(sells_win),
                window_min=int(win_used),
                sell_tax_bps=float(sell_tax_bps),
                est_impact_bps=float(imp),
                score=float(score),
                raw={"pair": pair},
            )
        )

    out.sort(key=lambda c: (c.score, c.buys_win, c.liquidity_usd, c.vol_m5_usd), reverse=True)
    return out


# =============================================================================
# TRADE mode helpers
# =============================================================================
async def _route_precheck(mint: str, amount_sol: float) -> tuple[bool, str, JsonDict]:
    if precheck_buy is None:
        return True, "no_route_guard", {}
    try:
        out = await _call_first_ok(
            precheck_buy,
            variants=[
                ((mint, amount_sol, SLIPPAGE_BPS), {}),
                ((mint, amount_sol), {"slippage_bps": SLIPPAGE_BPS}),
                ((mint,), {}),
            ],
        )
        if isinstance(out, tuple) and len(out) >= 2:
            ok = bool(out[0])
            why = str(out[1])
            meta = out[2] if (len(out) >= 3 and isinstance(out[2], dict)) else {}
            return ok, why, cast(JsonDict, meta)
        return True, "route_guard_unexpected_return", {}
    except Exception:
        log.debug("route_guard precheck failed", exc_info=True)
        return True, "route_guard_error_ignored", {}


async def _posttrade_watchdog(
    mint: str, amount_sol: float, pre_meta: JsonDict, tx_sig: str
) -> None:
    if posttrade_watchdog is None:
        return
    try:
        with contextlib.suppress(TypeError):
            await _maybe_await(posttrade_watchdog(mint, amount_sol, SLIPPAGE_BPS, pre_meta))  # type: ignore[misc]
            return
        with contextlib.suppress(TypeError):
            await _maybe_await(
                posttrade_watchdog(mint=mint, tx_sig=tx_sig, reason="whale_feed")  # type: ignore[misc]
            )
            return
        await _maybe_await(posttrade_watchdog(mint))  # type: ignore[misc]
    except Exception:
        log.debug("posttrade_watchdog failed", exc_info=True)


async def _swap_buy(mint: str, amount_sol: float) -> tuple[bool, str]:
    if swap_token_jupiter is None:
        return False, ""
    try:
        with contextlib.suppress(TypeError):
            out = await _maybe_await(
                swap_token_jupiter(
                    SOL_MINT,
                    mint,
                    amount_float=float(amount_sol),
                    buy=True,
                    slippage=float(JUPITER_MAX_SLIPPAGE),
                )
            )
            if isinstance(out, tuple) and len(out) >= 2:
                return bool(out[0]), str(out[1] or "")
            return bool(out), ""

        out2 = await _maybe_await(
            swap_token_jupiter(SOL_MINT, mint, float(amount_sol), True, float(JUPITER_MAX_SLIPPAGE))
        )
        if isinstance(out2, tuple) and len(out2) >= 2:
            return bool(out2[0]), str(out2[1] or "")
        return bool(out2), ""
    except Exception:
        log.exception("swap_token_jupiter failed")
        return False, ""


# =============================================================================
# Shadow mode helpers
# =============================================================================
def _is_shadow() -> bool:
    if WHALE_SHADOW:
        return True
    if shadow_take is not None:
        with contextlib.suppress(Exception):
            return bool(shadow_take("WHALE"))
    return _env_bool("SHADOW_MODE", "0")


async def _shadow_log(evt: str, payload: JsonDict) -> None:
    if shadow_log is None:
        log.info("[shadow:%s] %s", evt, payload)
        return
    with contextlib.suppress(Exception):
        await _maybe_await(shadow_log("whale", evt, payload))


# =============================================================================
# Main loop
# =============================================================================
class _SkipTick(Exception):
    """Internal control flow: end current tick cleanly (still sleeps + unlocks)."""


async def run_whale_feed(stop_event: asyncio.Event) -> None:
    """
    Long-running worker loop. Respects stop_event for graceful shutdown.
    """
    if not WHALE_FEED_ENABLED:
        log.warning("WHALE_FEED_ENABLED=0; worker will idle.")
        while not stop_event.is_set():
            await _wait_or_stop(stop_event, 5.0)
        return

    r = await _get_redis()

    if TELEGRAM_ON_START:
        await _tg(
            "🐳 Whale feed started\n"
            f"• mode={WHALE_FEED_MODE}\n"
            f"• poll={POLL_SEC}s cooldown={COOLDOWN_SEC}s lock_ttl={LOCK_TTL_SEC}s\n"
            f"• gates: liq>={MIN_LIQ_USD:,.0f} vol5>={WHALE_MIN_VOL_M5_USD:,.0f} "
            f"+5m>={WHALE_MIN_PRICECHANGE_M5:.2f}% "
            f"cluster>={CLUSTER_MIN_BUYS} buys in ~{CLUSTER_WINDOW_MIN}m "
            f"tax<={HONEYPOT_MAX_SELL_TAX_BPS:.0f}bps impact<={MAX_IMPACT_BPS:.0f}bps score>={WHALE_SCORE_MIN:.2f}"
        )

    # Lazy import aiohttp (no import-time dependency / no network at import)
    try:
        import aiohttp  # type: ignore
    except Exception as e:
        log.error("aiohttp not available; whale_feed cannot run: %s", e)
        while not stop_event.is_set():
            await _wait_or_stop(stop_event, 5.0)
        return

    timeout = aiohttp.ClientTimeout(total=20)  # type: ignore[attr-defined]

    async with aiohttp.ClientSession(timeout=timeout) as http:  # type: ignore[attr-defined]
        while not stop_event.is_set():
            t0 = _now_ts()
            locked: list[str] = []
            emitted = 0
            traded = 0
            sleep_base: float = float(POLL_SEC)

            try:
                if not await _allow_new_entry(r):
                    bump("whale_blocked_allow_new_entry")
                    raise _SkipTick()

                ticket_usd = await _current_ticket_usd(r)

                pairs = await _fetch_pairs(http)
                if not pairs:
                    raise _SkipTick()

                await _maybe_prewarm(pairs, ticket_usd)

                cands = _build_candidates(pairs, ticket_usd)
                if not cands:
                    raise _SkipTick()

                ready: list[WhaleCandidate] = []
                for c in cands:
                    if not await _cooldown_ok(r, c.mint):
                        continue
                    if not await _try_lock(r, c.mint):
                        continue
                    locked.append(c.mint)
                    ready.append(c)

                if not ready:
                    raise _SkipTick()

                if _is_shadow():
                    topn = ready[: min(10, len(ready))]
                    await _shadow_log(
                        "candidates",
                        {
                            "mode": WHALE_FEED_MODE,
                            "ticket_usd": ticket_usd,
                            "top": [
                                {
                                    "sym": c.symbol,
                                    "mint": c.mint,
                                    "score": round(c.score, 3),
                                    "liq_usd": round(c.liquidity_usd, 2),
                                    "vol_m5": round(c.vol_m5_usd, 2),
                                    "pct_m5": round(c.price_change_m5, 2),
                                    "buys_win": c.buys_win,
                                    "win_min": c.window_min,
                                    "tax_bps": round(c.sell_tax_bps, 1),
                                    "impact_bps": round(c.est_impact_bps, 1),
                                }
                                for c in topn
                            ],
                        },
                    )
                    bump("whale_shadow_tick")
                    raise _SkipTick()

                if WHALE_FEED_MODE == "QUEUE":
                    for c in ready[:MAX_EMIT_PER_TICK]:
                        await _publish(r, c)
                        emitted += 1
                        bump("whale_emit")
                        log.info(
                            "[QUEUE] %s %s score=%.2f buys%sm=%s vol5=$%s +5m=%.2f%% liq=$%s tax=%.0fbps impact=%.0fbps",
                            c.symbol,
                            c.mint,
                            c.score,
                            c.window_min,
                            c.buys_win,
                            f"{c.vol_m5_usd:,.0f}",
                            c.price_change_m5,
                            f"{c.liquidity_usd:,.0f}",
                            c.sell_tax_bps,
                            c.est_impact_bps,
                        )
                        if TELEGRAM_ON_CANDIDATE:
                            await _tg(
                                "🐳 Whale candidate queued\n"
                                f"• {c.symbol} ({c.mint}) score={c.score:.2f}\n"
                                f"• buys≈{c.buys_win} in {c.window_min}m • vol5=${c.vol_m5_usd:,.0f} • +5m={c.price_change_m5:.2f}%\n"
                                f"• liq=${c.liquidity_usd:,.0f} • tax≈{c.sell_tax_bps:.0f}bps • impact≈{c.est_impact_bps:.0f}bps\n"
                                f"• {c.pair_url}"
                            )

                    dt = _now_ts() - t0
                    log.info(
                        "[tick] pairs=%s cands=%s emitted=%s dt=%.2fs mode=%s",
                        len(pairs),
                        len(cands),
                        emitted,
                        dt,
                        WHALE_FEED_MODE,
                    )
                    raise _SkipTick()

                # TRADE mode
                if MAX_TRADES_PER_TICK <= 0:
                    log.warning("TRADE mode enabled but WHALE_MAX_TRADES_PER_TICK=0; skipping.")
                    bump("whale_trade_disabled_by_cap")
                    raise _SkipTick()

                usd_per_sol = await _usd_per_sol()
                if usd_per_sol <= 0.0:
                    log.warning("Cannot resolve SOL/USD; skipping TRADE tick.")
                    bump("whale_trade_no_usd_per_sol")
                    raise _SkipTick()

                amount_sol = ticket_usd / usd_per_sol

                for c in ready:
                    if traded >= MAX_TRADES_PER_TICK:
                        break

                    ok_rg, why, pre_meta = await _route_precheck(c.mint, amount_sol)
                    if not ok_rg:
                        bump("whale_trade_blocked_routeguard")
                        await _tg(
                            "⛔ Whale RouteGuard blocked\n"
                            f"• {c.symbol} ({c.mint})\n"
                            f"• why={why}\n"
                            f"• impact≈{c.est_impact_bps:.0f}bps slippage_bps={SLIPPAGE_BPS}"
                        )
                        continue

                    ok, sig = await _swap_buy(c.mint, amount_sol)
                    if not ok:
                        bump("whale_trade_swap_fail")
                        continue

                    if note_spend is not None:
                        with contextlib.suppress(Exception):
                            await _call_first_ok(
                                note_spend,
                                variants=[
                                    ((r, float(ticket_usd)), {}),
                                    ((None, float(ticket_usd)), {}),
                                    ((float(ticket_usd),), {}),
                                ],
                            )

                    traded += 1
                    bump("whale_trade_ok")

                    if TELEGRAM_ON_TRADE:
                        await _tg(
                            "🐳 BUY (whale_feed)\n"
                            f"• {c.symbol} ({c.mint})\n"
                            f"• ticket=${ticket_usd:,.2f} amount≈{amount_sol:.6f} SOL\n"
                            f"• score={c.score:.2f} buys≈{c.buys_win}/{c.window_min}m vol5=${c.vol_m5_usd:,.0f} +5m={c.price_change_m5:.2f}%\n"
                            f"• liq=${c.liquidity_usd:,.0f} tax≈{c.sell_tax_bps:.0f}bps impact≈{c.est_impact_bps:.0f}bps\n"
                            f"• tx={sig}"
                        )

                    await _posttrade_watchdog(c.mint, amount_sol, pre_meta, sig)

                dt = _now_ts() - t0
                log.info(
                    "[tick] pairs=%s cands=%s traded=%s dt=%.2fs mode=%s",
                    len(pairs),
                    len(cands),
                    traded,
                    dt,
                    WHALE_FEED_MODE,
                )

            except _SkipTick:
                pass
            except asyncio.CancelledError:
                raise
            except Exception as e:
                bump("whale_loop_err")
                log.error("[whale_feed] loop error: %s", e, exc_info=True)
            finally:
                if r is not None and locked:
                    for m in locked:
                        await _unlock(r, m)

            # Always sleep between ticks (prevents tight loops in shadow/skip paths).
            sleep_for = float(sleep_base) + random.uniform(0.0, float(JITTER_SEC))
            await _wait_or_stop(stop_event, sleep_for)


async def main() -> None:
    """
    Standalone entrypoint (safe). Initializes shared resources if available.
    """
    with contextlib.suppress(Exception):
        _compat_setup_logging(service="whale_feed")

    lvl = os.getenv("LOG_LEVEL", "").strip().upper()
    if lvl:
        with contextlib.suppress(Exception):
            logging.getLogger().setLevel(lvl)

    if init_resources is not None:
        with contextlib.suppress(Exception):
            await _maybe_await(init_resources())

    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()

    def _stop(*_: Any) -> None:
        stop_event.set()

    for sig_name in ("SIGINT", "SIGTERM"):
        sig_obj = getattr(signal, sig_name, None)
        if sig_obj is None:
            continue
        with contextlib.suppress(NotImplementedError):
            loop.add_signal_handler(sig_obj, _stop)

    try:
        await run_whale_feed(stop_event)
    finally:
        if close_resources is not None:
            with contextlib.suppress(Exception):
                await _maybe_await(close_resources())


__all__ = ["WhaleCandidate", "run_whale_feed", "main"]
