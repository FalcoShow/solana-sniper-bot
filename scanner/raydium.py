from __future__ import annotations

"""
scanner/raydium.py — Raydium scanner compatibility shim (V3+++ / God-Tier LOCK-IN)
================================================================================

CANONICAL IMPLEMENTATION
------------------------
The real scanner lives in:
    scanner/raydium_scanner.py

This module exists ONLY to preserve legacy imports like:
    from scanner.raydium import scan_raydium
    import scanner.raydium as raydium

Hard rules
----------
- Zero behavior drift in LIVE mode: forward to scanner.raydium_scanner at call time
- No network I/O at import time (Render-safe)
- Lazy-load the canonical module (fast import; avoids side-effects)
- Provide OFFLINE contract mode for tests/CI: deterministic candidates, no Redis/RPC/HTTP
- Cancellation-safe: never swallow asyncio.CancelledError
- Safe degradation: if canonical impl fails to import or call, return [] (never crash the bot)

Offline mode triggers
---------------------
Any of the below will enable offline behavior:
- Explicit args: offline=True, dry=True, no_network=True, test_mode=True
- Common env toggles: SAFE_MODE, OFFLINE_MODE, NO_NETWORK, DRY_RUN, TEST_MODE

Env toggles
-----------
- RAYDIUM_SHIM_WARN=1   -> warn once on import + once on first use
- RAYDIUM_IMPL_MODULE   -> optional override dotted path for canonical impl
"""

import asyncio
import contextlib
import hashlib
import importlib
import inspect
import logging
import os
import time
from collections.abc import Callable
from functools import lru_cache
from types import ModuleType
from typing import Any, Protocol, TypeAlias, cast

JsonDict: TypeAlias = dict[str, Any]

_TRUE = {"1", "true", "t", "yes", "y", "on"}


def _truthy(x: Any) -> bool:
    if x is None:
        return False
    if isinstance(x, bool):
        return x
    return str(x).strip().lower() in _TRUE


# =============================================================================
# Phase A — Logging contract (stable EventLogger, no callable mismatch)
# =============================================================================


class EventLogger(Protocol):
    handlers: list[Any]

    def debug(self, msg: str, *args: Any, **kwargs: Any) -> None: ...
    def info(self, msg: str, *args: Any, **kwargs: Any) -> None: ...
    def warning(self, msg: str, *args: Any, **kwargs: Any) -> None: ...
    def error(self, msg: str, *args: Any, **kwargs: Any) -> None: ...
    def exception(self, msg: str, *args: Any, **kwargs: Any) -> None: ...
    def log(self, level: int, msg: str, *args: Any, **kwargs: Any) -> None: ...

    # optional helpers (log_compat may provide)
    def event(self, name: str, **kw: Any) -> None: ...
    def every(self, key: str, n: int = 10, msg: str | None = None, **kw: Any) -> None: ...


class _LogAdapter:
    """
    Adapter that always looks like an EventLogger:
      - `.handlers` exists
      - `.warning` exists (even if base uses `.warn`)
      - `.event` / `.every` exist (best-effort throttling)
    """

    def __init__(self, base: Any):
        self._base = base
        self.handlers: list[Any] = []
        with contextlib.suppress(Exception):
            h = getattr(base, "handlers", None)
            if isinstance(h, list):
                self.handlers = h
        self._every_store: dict[str, float] = {}

    def debug(self, msg: str, *args: Any, **kwargs: Any) -> None:
        cast(Any, self._base).debug(msg, *args, **kwargs)

    def info(self, msg: str, *args: Any, **kwargs: Any) -> None:
        cast(Any, self._base).info(msg, *args, **kwargs)

    def warning(self, msg: str, *args: Any, **kwargs: Any) -> None:
        fn = getattr(self._base, "warning", None) or getattr(self._base, "warn", None)
        if callable(fn):
            fn(msg, *args, **kwargs)
            return
        self.info(msg, *args, **kwargs)

    def error(self, msg: str, *args: Any, **kwargs: Any) -> None:
        cast(Any, self._base).error(msg, *args, **kwargs)

    def exception(self, msg: str, *args: Any, **kwargs: Any) -> None:
        fn = getattr(self._base, "exception", None)
        if callable(fn):
            fn(msg, *args, **kwargs)
            return
        self.error(msg, *args, **kwargs)

    def log(self, level: int, msg: str, *args: Any, **kwargs: Any) -> None:
        cast(Any, self._base).log(level, msg, *args, **kwargs)

    def event(self, name: str, **kw: Any) -> None:
        fn = getattr(self._base, "event", None)
        if callable(fn):
            fn(str(name), **kw)
            return
        self.info("%s %s", str(name), kw)

    def every(self, key: str, n: int = 10, msg: str | None = None, **kw: Any) -> None:
        # Historically, callers use `n` as seconds between emits.
        try:
            now = time.monotonic()
            gap = float(max(1, int(n)))
            last = float(self._every_store.get(key, 0.0))
            if (now - last) < gap:
                return
            self._every_store[key] = now
        except Exception:
            pass
        self.event(msg or key, **kw)


def setup_logging(service: str | None = None) -> None:
    """Best-effort logging setup; never hard-fail."""
    with contextlib.suppress(Exception):
        from utils_pkg.log_compat import setup_logging as _sl  # type: ignore

        _sl(service=service)
        return
    _ = service


def get_logger(name: str | None = None) -> EventLogger:
    nm = (name or "scanner.raydium").strip() or "scanner.raydium"

    with contextlib.suppress(Exception):
        from utils_pkg.log_compat import get_event_logger  # type: ignore

        return cast(EventLogger, _LogAdapter(get_event_logger(nm)))

    with contextlib.suppress(Exception):
        from utils_pkg.log_compat import get_logger as _gl  # type: ignore

        return cast(EventLogger, _LogAdapter(_gl(nm)))

    with contextlib.suppress(Exception):
        from logging_setup import get_logger as _gl2  # type: ignore[import-not-found]

        return cast(EventLogger, _LogAdapter(_gl2(nm)))

    return cast(EventLogger, _LogAdapter(logging.getLogger(nm)))


LOG: EventLogger = get_logger("scanner.raydium")

# =============================================================================
# Warn-once behavior (optional)
# =============================================================================

_WARN = str(os.getenv("RAYDIUM_SHIM_WARN", "")).strip().lower() in _TRUE
_warned_import = False
_warned_use = False


def _warn_once(kind: str, msg: str) -> None:
    """Warn once per kind ('import'|'use') when RAYDIUM_SHIM_WARN is enabled."""
    global _warned_import, _warned_use
    if not _WARN:
        return
    if kind == "import":
        if _warned_import:
            return
        _warned_import = True
    elif kind == "use":
        if _warned_use:
            return
        _warned_use = True
    else:
        return

    with contextlib.suppress(Exception):
        LOG.warning(msg)


# =============================================================================
# Canonical import (lazy) + safe degradation
# =============================================================================

_IMPL: ModuleType | None = None
_LAST_IMPORT_ERR_TS: float = 0.0

_IMPL_DOTTED = str(os.getenv("RAYDIUM_IMPL_MODULE", "scanner.raydium_scanner") or "").strip() or (
    "scanner.raydium_scanner"
)


def _env_float(name: str, default: float) -> float:
    v = os.getenv(name)
    if v is None or v == "":
        return default
    try:
        return float(v)
    except Exception:
        return default


_IMPORT_ERR_TTL: float = _env_float("RAYDIUM_IMPL_IMPORT_ERR_COOLDOWN_S", 20.0)


def _throttled_import_error(exc: BaseException) -> None:
    global _LAST_IMPORT_ERR_TS
    now = time.time()
    if (now - _LAST_IMPORT_ERR_TS) < _IMPORT_ERR_TTL:
        return
    _LAST_IMPORT_ERR_TS = now
    LOG.exception(
        "Raydium shim: failed to import canonical scanner (scanner.raydium_scanner). "
        "Degrading to no-op. impl=%s err=%s",
        _IMPL_DOTTED,
        f"{type(exc).__name__}: {exc}",
    )


def _import_candidates() -> tuple[str, ...]:
    # canonical first, then package-relative, then override
    return (
        "scanner.raydium_scanner",
        ".raydium_scanner",
        _IMPL_DOTTED,
    )


def _load_impl() -> ModuleType | None:
    global _IMPL
    if _IMPL is not None:
        return _IMPL

    last_exc: BaseException | None = None
    for modpath in _import_candidates():
        try:
            if modpath.startswith("."):
                if not __package__:
                    continue
                _IMPL = importlib.import_module(modpath, package=__package__)
            else:
                _IMPL = importlib.import_module(modpath)
            return _IMPL
        except BaseException as e:  # pragma: no cover
            last_exc = e

    if last_exc is not None:
        _throttled_import_error(last_exc)
    return None


@lru_cache(maxsize=1)
def _impl_cached() -> ModuleType | None:
    # Keep a cached view for __getattr__/__dir__ forwarding
    return _load_impl()


def _pick_callable(mod: ModuleType, names: tuple[str, ...]) -> Callable[..., Any] | None:
    for n in names:
        fn = getattr(mod, n, None)
        if callable(fn):
            return cast(Callable[..., Any], fn)
    return None


async def _call_maybe_async(fn: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
    out = fn(*args, **kwargs)
    if inspect.isawaitable(out):
        return await out
    return out


def _coerce_candidate_list(x: Any) -> list[JsonDict]:
    if not isinstance(x, list):
        return []
    out: list[JsonDict] = []
    for it in x:
        if isinstance(it, dict):
            out.append(cast(JsonDict, it))
    return out


def _strip_offline_keys(kwargs: dict[str, Any]) -> dict[str, Any]:
    drop = {"offline", "dry", "dry_run", "no_network", "test_mode", "limit"}
    return {k: v for (k, v) in kwargs.items() if k not in drop}


# =============================================================================
# Offline candidate generator (deterministic, schema-rich)
# =============================================================================

_B58_ALPH = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
_OFFLINE_TS = 1_700_000_000  # deterministic fixed timestamp


def _b58encode(data: bytes) -> str:
    """Tiny base58 encoder (no deps). Produces Solana-valid strings for 32-byte inputs."""
    if not data:
        return ""
    n = int.from_bytes(data, "big")
    out: list[str] = []
    while n > 0:
        n, r = divmod(n, 58)
        out.append(_B58_ALPH[r])

    pad = 0
    for b in data:
        if b == 0:
            pad += 1
        else:
            break

    return ("1" * pad) + "".join(reversed(out or ["1"]))


def _stable_pubkey(tag: str) -> str:
    h = hashlib.sha256(tag.encode("utf-8")).digest()  # 32 bytes
    return _b58encode(h)


def _offline_candidates(limit: int | None = None) -> list[JsonDict]:
    """
    Deterministic candidates that satisfy BOTH:
      - canonical-ish schema expectations
      - legacy aliases (sym/liq/vol5/buys5/sells5/chg5/age_min)
    """
    mint0 = _stable_pubkey("raydium-offline-mint-0")
    mint1 = _stable_pubkey("raydium-offline-mint-1")
    pool0 = _stable_pubkey("raydium-offline-pool-0")
    pool1 = _stable_pubkey("raydium-offline-pool-1")

    base0: JsonDict = {
        # identifiers
        "candidate_schema": "CandidateSnapshot.v1",
        "scanner": "raydium",
        "scanner_id": "raydium",
        "scanner_version": "v3+++_shim",
        "source": "raydium",
        "dex": "raydium",
        "chain": "solana",
        "scan_ts": float(_OFFLINE_TS),
        "ts": _OFFLINE_TS,
        "time": _OFFLINE_TS,
        # core identity
        "mint": mint0,
        "token_mint": mint0,
        "address": mint0,
        "pool": pool0,
        "pair_address": pool0,
        "symbol": "RAY_OFFLINE_A",
        "sym": "RAY_OFFLINE_A",
        "pair_url": f"https://dexscreener.com/solana/{pool0}",
        # metrics (canonical)
        "liq_usd": 250_000.0,
        "vol_5m_usd": 12_500.0,
        "buys_5m": 18,
        "sells_5m": 9,
        "txns_5m": 27,
        "token_age_min": 42.0,
        "price_usd": 0.001234,
        "price_change_5m_pct": 3.25,
        "fdv_usd": 1_250_000.0,
        "mcap_usd": 1_250_000.0,
        # scoring placeholders
        "rank_score": 0.50,
        "score_combined": 0.50,
        "score_label": "offline",
        # legacy aliases
        "liq": 250_000.0,
        "vol5": 12_500.0,
        "buys5": 18,
        "sells5": 9,
        "chg5": 3.25,
        "age_min": 42.0,
        # extras
        "flags": ["offline_contract_candidate"],
        "notes": ["raydium shim offline mode"],
    }

    base1: JsonDict = dict(base0)
    base1.update(
        {
            "mint": mint1,
            "token_mint": mint1,
            "address": mint1,
            "pool": pool1,
            "pair_address": pool1,
            "symbol": "RAY_OFFLINE_B",
            "sym": "RAY_OFFLINE_B",
            "pair_url": f"https://dexscreener.com/solana/{pool1}",
            "liq_usd": 410_000.0,
            "liq": 410_000.0,
            "vol_5m_usd": 31_000.0,
            "vol5": 31_000.0,
            "buys_5m": 33,
            "buys5": 33,
            "sells_5m": 12,
            "sells5": 12,
            "price_change_5m_pct": 4.10,
            "chg5": 4.10,
            "token_age_min": 9.0,
            "age_min": 9.0,
            "rank_score": 0.62,
            "score_combined": 0.62,
        }
    )

    items = [base0, base1]
    if limit is None:
        return items
    try:
        lim = int(limit)
    except Exception:
        return items
    if lim <= 0:
        return items
    return items[:lim]


# =============================================================================
# Offline detection
# =============================================================================

_OFFLINE_ENV_KEYS = ("SAFE_MODE", "OFFLINE_MODE", "NO_NETWORK", "DRY_RUN", "TEST_MODE")


def _offline_enabled(
    offline: bool,
    dry: bool | None,
    no_network: bool | None,
    test_mode: bool | None,
) -> bool:
    if offline or _truthy(dry) or _truthy(no_network) or _truthy(test_mode):
        return True
    for k in _OFFLINE_ENV_KEYS:
        if _truthy(os.getenv(k)):
            return True
    return False


def _pop_truthy_kw(kwargs: dict[str, Any], key: str) -> bool | None:
    if key not in kwargs:
        return None
    v = kwargs.pop(key)
    return _truthy(v)


def _pop_int_kw(kwargs: dict[str, Any], key: str) -> int | None:
    if key not in kwargs:
        return None
    v = kwargs.pop(key)
    try:
        return int(v)
    except Exception:
        return None


# =============================================================================
# Public API (legacy-compatible) + OFFLINE contract flags
# =============================================================================

_warn_once(
    "import",
    "Imported scanner.raydium shim; prefer scanner.raydium_scanner (set RAYDIUM_SHIM_WARN=0 to silence)",
)


async def scan_raydium(
    *args: Any,
    offline: bool = False,
    dry: bool | None = None,
    no_network: bool | None = None,
    test_mode: bool | None = None,
    limit: int | None = None,
    **kwargs: Any,
) -> list[JsonDict]:
    """
    Primary entrypoint.

    OFFLINE MODE:
      scan_raydium(offline=True) -> deterministic sample candidates (no I/O)

    LIVE MODE:
      forwards to canonical implementation if available; otherwise safe empty list.
    """
    _warn_once("use", "scanner.raydium is a compatibility shim; prefer scanner.raydium_scanner")

    # Accept offline flags via kwargs too (legacy callers)
    if not offline and "offline" in kwargs:
        offline = _truthy(kwargs.pop("offline"))
    if dry is None:
        dry = _pop_truthy_kw(kwargs, "dry_run")
        if dry is None:
            dry = _pop_truthy_kw(kwargs, "dry")
    if no_network is None:
        no_network = _pop_truthy_kw(kwargs, "no_network")
    if test_mode is None:
        test_mode = _pop_truthy_kw(kwargs, "test_mode")
    if limit is None:
        limit = _pop_int_kw(kwargs, "limit")

    if _offline_enabled(offline, dry, no_network, test_mode):
        return _offline_candidates(limit=limit)

    impl = _load_impl()
    if impl is None:
        return []

    fn = _pick_callable(impl, ("scan_raydium", "scan_once", "scan"))
    if fn is None:
        return []

    fwd_kwargs = _strip_offline_keys(kwargs)
    try:
        res = await _call_maybe_async(fn, *args, **fwd_kwargs)
        items = _coerce_candidate_list(res)
    except asyncio.CancelledError:
        raise
    except Exception as e:
        LOG.exception(
            "Raydium shim: scan_raydium crashed (degrading). err=%s",
            f"{type(e).__name__}: {e}",
        )
        return []

    if limit is None:
        return items
    try:
        lim = int(limit)
    except Exception:
        return items
    if lim <= 0:
        return items
    return items[:lim]


async def scan(
    *args: Any,
    offline: bool = False,
    dry: bool | None = None,
    no_network: bool | None = None,
    test_mode: bool | None = None,
    limit: int | None = None,
    **kwargs: Any,
) -> list[JsonDict]:
    """Common alias some harnesses look for."""
    return await scan_raydium(
        *args,
        offline=offline,
        dry=dry,
        no_network=no_network,
        test_mode=test_mode,
        limit=limit,
        **kwargs,
    )


async def scan_once(
    *args: Any,
    offline: bool = False,
    dry: bool | None = None,
    no_network: bool | None = None,
    test_mode: bool | None = None,
    limit: int | None = None,
    **kwargs: Any,
) -> list[JsonDict]:
    """Alias with same offline contract flags."""
    return await scan_raydium(
        *args,
        offline=offline,
        dry=dry,
        no_network=no_network,
        test_mode=test_mode,
        limit=limit,
        **kwargs,
    )


async def run_scanner(*args: Any, **kwargs: Any) -> None:
    """
    Legacy runner entrypoint expected by older launch scripts.
    This shim never crashes the process if the canonical impl is missing/broken.
    """
    _warn_once("use", "scanner.raydium is a compatibility shim; prefer scanner.raydium_scanner")

    impl = _load_impl()
    if impl is None:
        return

    fn = _pick_callable(impl, ("run_scanner", "run", "main", "scan_loop"))
    if fn is None:
        return

    try:
        with contextlib.suppress(Exception):
            setup_logging(service="scanner.raydium")
        await _call_maybe_async(fn, *args, **kwargs)
    except asyncio.CancelledError:
        raise
    except Exception as e:
        LOG.exception(
            "Raydium shim: run_scanner crashed (degrading). err=%s",
            f"{type(e).__name__}: {e}",
        )


async def run(*args: Any, **kwargs: Any) -> None:
    await run_scanner(*args, **kwargs)


async def scan_loop(*args: Any, **kwargs: Any) -> None:
    """Some older code calls scan_loop; map it to run_scanner safely."""
    await run_scanner(*args, **kwargs)


__all__ = [
    "EventLogger",
    "LOG",
    "get_logger",
    "main",
    "run",
    "run_scanner",
    "scan",
    "scan_loop",
    "scan_once",
    "scan_raydium",
    "setup_logging",
]


# Provide main alias (legacy)
async def main(*args: Any, **kwargs: Any) -> None:
    await run_scanner(*args, **kwargs)


# =============================================================================
# Compatibility: attribute forwarding
# =============================================================================


def __getattr__(name: str) -> Any:
    """
    Forward ANY missing attribute to scanner.raydium_scanner lazily.
    Preserves older code that imported helpers/constants from scanner.raydium.
    """
    mod = _impl_cached()
    if mod is None:
        raise AttributeError(name)
    return getattr(mod, name)


def __dir__() -> list[str]:
    out = set(globals().keys())
    mod = _impl_cached()
    if mod is not None:
        with contextlib.suppress(Exception):
            out.update(dir(mod))
    return sorted(out)
