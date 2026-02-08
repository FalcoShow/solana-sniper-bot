from __future__ import annotations

"""
scanner/axiom.py — Axiom scanner compatibility shim (V3+++ / God-Tier LOCK-IN)
=============================================================================

WHY THIS FILE EXISTS
--------------------
The canonical (heavy) implementation lives in:
    scanner/axiom_scanner.py

This module exists to:
- Preserve legacy imports / entrypoints:
      from scanner.axiom import scan_axiom
      import scanner.axiom as axiom
- Guarantee: NO heavy work / NO network at import time (Render-safe)
- Provide an OFFLINE contract path for tests/CI:
      - explicit offline flags in signature
      - common env toggles
      - deterministic sample candidate output
- Forward live behavior 1:1 to the canonical implementation at call-time.

Env toggles
-----------
- AXIOM_SHIM_WARN=1        -> warn once on import and once on first use
- AXIOM_IMPL_MODULE=...    -> override impl module (default scanner.axiom_scanner)

Offline triggers (any true enables offline mode)
------------------------------------------------
Signature flags:
  offline, dry, dry_run, no_network, sample, test_mode

Environment flags:
  SAFE_MODE, OFFLINE_MODE, NO_NETWORK, DRY_RUN, TEST_MODE,
  SCANNER_LIVE_TESTS=0  (forces offline contract mode)
"""

import asyncio
import contextlib
import hashlib
import importlib
import logging
import os
from collections.abc import Callable
from functools import lru_cache
from types import ModuleType
from typing import TYPE_CHECKING, Any, Protocol, TypeAlias, cast

# ──────────────────────────────────────────────────────────────────────────────
# Phase A — Logging contract (EventLogger + safe get_logger)
# ──────────────────────────────────────────────────────────────────────────────


class EventLogger(Protocol):
    handlers: list[Any]

    def debug(self, msg: str, *args: Any, **kwargs: Any) -> None: ...
    def info(self, msg: str, *args: Any, **kwargs: Any) -> None: ...
    def warning(self, msg: str, *args: Any, **kwargs: Any) -> None: ...
    def error(self, msg: str, *args: Any, **kwargs: Any) -> None: ...
    def exception(self, msg: str, *args: Any, **kwargs: Any) -> None: ...
    def log(self, level: int, msg: str, *args: Any, **kwargs: Any) -> None: ...


class _EventCapable(EventLogger, Protocol):
    def event(self, name: str, **fields: Any) -> None: ...


class _LoggerProxy:
    """
    Adapter that guarantees `.handlers` exists for typing compatibility,
    while forwarding standard logging methods to the underlying logger.
    """

    def __init__(self, base: Any):
        self._base = base
        self.handlers: list[Any] = []
        with contextlib.suppress(Exception):
            h = getattr(base, "handlers", None)
            if isinstance(h, list):
                self.handlers = h

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

    def event(self, name: str, **fields: Any) -> None:
        fn = getattr(self._base, "event", None)
        if callable(fn):
            fn(name, **fields)
            return
        self.info("%s %s", name, fields)


def get_logger(name: str | None = None) -> EventLogger:
    """
    Always returns an EventLogger-compatible object.

    Priority:
      1) utils_pkg.log_compat.get_event_logger / get_logger
      2) logging_setup.get_logger
      3) stdlib logging.getLogger (basicConfig if needed)
    """
    nm = (name or "scanner.axiom").strip() or "scanner.axiom"

    with contextlib.suppress(Exception):
        from utils_pkg.log_compat import get_event_logger  # type: ignore

        return cast(EventLogger, _LoggerProxy(get_event_logger(nm)))

    with contextlib.suppress(Exception):
        from utils_pkg.log_compat import get_logger as gl  # type: ignore

        return cast(EventLogger, _LoggerProxy(gl(nm)))

    with contextlib.suppress(Exception):
        from logging_setup import get_logger as gl2  # type: ignore

        return cast(EventLogger, _LoggerProxy(gl2(nm)))

    lg = logging.getLogger(nm)
    if not lg.handlers:
        logging.basicConfig(
            level=logging.INFO,
            format="[scanner.axiom] %(asctime)s %(levelname)s %(name)s %(message)s",
        )
    return cast(EventLogger, _LoggerProxy(lg))


LOG: EventLogger = get_logger("scanner.axiom")


def _event(name: str, **fields: Any) -> None:
    """Best-effort structured event. Never raises."""
    try:
        fn = getattr(LOG, "event", None)
        if callable(fn):
            cast(_EventCapable, LOG).event(str(name), **fields)
        else:
            LOG.info("%s %s", str(name), fields)
    except Exception:
        return


# ──────────────────────────────────────────────────────────────────────────────
# Shim config + warning controls
# ──────────────────────────────────────────────────────────────────────────────
_TRUE = {"1", "true", "t", "yes", "y", "on"}
_WARN = str(os.getenv("AXIOM_SHIM_WARN", "")).strip().lower() in _TRUE
_warned_import = False
_warned_use = False

_IMPL_DOTTED = (
    os.getenv("AXIOM_IMPL_MODULE", "scanner.axiom_scanner").strip() or "scanner.axiom_scanner"
)


def _truthy(x: Any) -> bool:
    if x is None:
        return False
    if isinstance(x, bool):
        return x
    return str(x).strip().lower() in _TRUE


def _warn_once(kind: str, msg: str) -> None:
    """Warn once per kind ('import'|'use') when AXIOM_SHIM_WARN=1."""
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


# ──────────────────────────────────────────────────────────────────────────────
# Canonical impl loader (lazy)
# ──────────────────────────────────────────────────────────────────────────────
@lru_cache(maxsize=1)
def _impl() -> ModuleType:
    """
    Lazy-load canonical implementation.
    IMPORTANT: axiom_scanner.py must not perform network I/O at import time.
    """
    if _IMPL_DOTTED in {"scanner.axiom_scanner", "axiom_scanner"}:
        if not __package__:
            return importlib.import_module("scanner.axiom_scanner")
        return importlib.import_module(".axiom_scanner", package=__package__)
    return importlib.import_module(_IMPL_DOTTED)


def _pick_callable(mod: ModuleType, names: tuple[str, ...]) -> Callable[..., Any] | None:
    for n in names:
        fn = getattr(mod, n, None)
        if callable(fn):
            return cast(Callable[..., Any], fn)
    return None


async def _call_impl(fn: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
    """
    Call canonical fn whether it's async or sync.
    Cancellation is respected.
    """
    if asyncio.iscoroutinefunction(fn):
        return await fn(*args, **kwargs)
    # If it returns a coroutine anyway, _maybe_await will handle it.
    return await asyncio.to_thread(fn, *args, **kwargs)


async def _maybe_await(x: Any) -> Any:
    if asyncio.iscoroutine(x):
        return await x
    if hasattr(x, "__await__"):
        return await x
    return x


# ──────────────────────────────────────────────────────────────────────────────
# Offline contract candidate (deterministic, no deps)
# ──────────────────────────────────────────────────────────────────────────────
Json: TypeAlias = dict[str, Any]
CandidateList: TypeAlias = list[Json]

_B58_ALPH = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"


def _b58encode(data: bytes) -> str:
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
    h = hashlib.sha256(tag.encode("utf-8")).digest()
    return _b58encode(h)


_OFFLINE_ENV_KEYS = ("SAFE_MODE", "OFFLINE_MODE", "NO_NETWORK", "DRY_RUN", "TEST_MODE")


def _offline_enabled(
    offline: bool,
    dry: bool,
    dry_run: bool,
    no_network: bool,
    sample: bool,
    test_mode: bool,
) -> bool:
    if offline or dry or dry_run or no_network or sample or test_mode:
        return True

    if "SCANNER_LIVE_TESTS" in os.environ and not _truthy(os.getenv("SCANNER_LIVE_TESTS")):
        return True

    for k in _OFFLINE_ENV_KEYS:
        if _truthy(os.getenv(k)):
            return True

    return False


def _offline_sample_candidate() -> Json:
    """
    Deterministic candidate with a superset schema:
    - canonical-ish fields used by newer code
    - legacy aliases used by older feature builders
    """
    ts = int(os.getenv("PYTEST_SAMPLE_TS", "1700000000") or 1700000000)

    mint = _stable_pubkey("axiom-offline-mint")
    pair = _stable_pubkey("axiom-offline-pair")

    liq_usd = 12_345.67
    vol_5m = 2_345.67
    chg_5m = 3.21

    return {
        "candidate_schema": "CandidateSnapshot.v1",
        "scanner": "axiom",
        "scanner_version": "v3+++_shim",
        "source": "axiom_offline",
        "chain": "solana",
        "dex": "axiom",
        "symbol": "AXIOM_TEST",
        "mint": mint,
        "token_mint": mint,
        "pair_address": pair,
        "pool": pair,
        "ts": ts,
        "time": ts,
        # Canonical-ish metrics
        "price_usd": 0.000123,
        "liq_usd": liq_usd,
        "vol_5m_usd": vol_5m,
        "vol_1h_usd": 45_678.90,
        "buys_5m": 12,
        "sells_5m": 5,
        "price_change_5m_pct": chg_5m,
        "token_age_min": 42.0,
        # Legacy aliases (kept intentionally)
        "liq": liq_usd,
        "vol5": vol_5m,
        "buys5": 12,
        "sells5": 5,
        "chg5": chg_5m,
        "chg_5m": chg_5m,
        "age_min": 42.0,
        # Scoring placeholders
        "score_combined": 0.73,
        "score_label": "OFFLINE_SAMPLE",
        "score_is_trade_viable": True,
        # URLs/meta
        "url": "https://axiom.example/offline",
        "flags": ["offline_contract_candidate"],
        "meta": {
            "offline": True,
            "sample": True,
            "impl_module": _IMPL_DOTTED,
        },
    }


def axiom_impl_health() -> Json:
    """
    Wrapper-level health snapshot (no network).
    """
    health: Json = {"impl_module": _IMPL_DOTTED, "impl_loaded": False}
    try:
        mod = _impl()
        health["impl_loaded"] = True
        fn = getattr(mod, "axiom_scanner_health", None)
        if callable(fn):
            with contextlib.suppress(Exception):
                out = fn()
                if isinstance(out, dict):
                    health["impl_health"] = out
    except Exception as e:
        health["impl_err"] = f"{type(e).__name__}: {e}"
    return health


# ──────────────────────────────────────────────────────────────────────────────
# Public API (preferred entrypoints)
# ──────────────────────────────────────────────────────────────────────────────
async def scan_axiom(
    *args: Any,
    offline: bool = False,
    dry: bool = False,
    dry_run: bool = False,
    no_network: bool = False,
    sample: bool = False,
    test_mode: bool = False,
    limit: int | None = None,
    **kwargs: Any,
) -> CandidateList:
    """
    Backwards-compatible scanner entrypoint.

    OFFLINE contract path:
      - If any offline-ish flag or env trigger is active, returns deterministic samples
        WITHOUT importing/calling the heavy implementation.
    """
    _warn_once(
        "use",
        "scanner.axiom is a compatibility shim; prefer scanner.axiom_scanner for canonical impl.",
    )

    # Accept older callers passing offline-ish flags through kwargs
    if not offline and "offline" in kwargs:
        offline = _truthy(kwargs.pop("offline"))
    if not dry and "dry" in kwargs:
        dry = _truthy(kwargs.pop("dry"))
    if not dry_run and "dry_run" in kwargs:
        dry_run = _truthy(kwargs.pop("dry_run"))
    if not no_network and "no_network" in kwargs:
        no_network = _truthy(kwargs.pop("no_network"))
    if not sample and "sample" in kwargs:
        sample = _truthy(kwargs.pop("sample"))
    if not test_mode and "test_mode" in kwargs:
        test_mode = _truthy(kwargs.pop("test_mode"))
    if limit is None and "limit" in kwargs:
        try:
            limit = int(kwargs.pop("limit"))
        except Exception:
            kwargs.pop("limit", None)

    if _offline_enabled(offline, dry, dry_run, no_network, sample, test_mode):
        items: CandidateList = [_offline_sample_candidate()]
        if limit is None:
            return items
        if limit <= 0:
            return items
        return items[:limit]

    # Strip offline-ish keys before forwarding to avoid signature mismatches
    for k in ("offline", "dry", "dry_run", "no_network", "sample", "test_mode", "limit"):
        kwargs.pop(k, None)

    mod = _impl()
    fn = _pick_callable(mod, ("scan_axiom", "scan"))
    if fn is None:
        raise RuntimeError("scanner.axiom_scanner.scan_axiom/scan is missing")

    try:
        out = await _call_impl(fn, *args, **kwargs)
        res = await _maybe_await(out)
    except asyncio.CancelledError:
        raise

    if res is None:
        return []
    if isinstance(res, list):
        return [x for x in res if isinstance(x, dict)]
    return []


async def scan(
    *args: Any,
    offline: bool = False,
    dry: bool = False,
    dry_run: bool = False,
    no_network: bool = False,
    sample: bool = False,
    test_mode: bool = False,
    limit: int | None = None,
    **kwargs: Any,
) -> CandidateList:
    """Generic alias some registries use."""
    return await scan_axiom(
        *args,
        offline=offline,
        dry=dry,
        dry_run=dry_run,
        no_network=no_network,
        sample=sample,
        test_mode=test_mode,
        limit=limit,
        **kwargs,
    )


async def scan_once(*args: Any, **kwargs: Any) -> CandidateList:
    """
    Optional alias: if canonical impl provides scan_once, use it; else fall back to scan_axiom.
    """
    _warn_once("use", "scanner.axiom is a compatibility shim; prefer scanner.axiom_scanner")
    try:
        mod = _impl()
        fn = _pick_callable(mod, ("scan_once",))
        if fn is None:
            return await scan_axiom(*args, **kwargs)
        out = await _call_impl(fn, *args, **kwargs)
        res = await _maybe_await(out)
    except asyncio.CancelledError:
        raise

    if isinstance(res, list):
        return [x for x in res if isinstance(x, dict)]
    return []


async def run_scanner(*args: Any, **kwargs: Any) -> None:
    _warn_once("use", "scanner.axiom is a compatibility shim; prefer scanner.axiom_scanner")
    mod = _impl()
    fn = _pick_callable(mod, ("run_scanner", "run", "main"))
    if fn is None:
        return
    try:
        out = await _call_impl(fn, *args, **kwargs)
        await _maybe_await(out)
    except asyncio.CancelledError:
        raise


async def run(*args: Any, **kwargs: Any) -> None:
    await run_scanner(*args, **kwargs)


async def main(*args: Any, **kwargs: Any) -> None:
    await run_scanner(*args, **kwargs)


async def scan_loop(*args: Any, **kwargs: Any) -> None:
    """
    Optional alias: forward to canonical scan_loop if present; else forward to run_scanner.
    """
    _warn_once("use", "scanner.axiom is a compatibility shim; prefer scanner.axiom_scanner")
    mod = _impl()
    fn = _pick_callable(mod, ("scan_loop", "run_scanner", "run", "main"))
    if fn is None:
        return
    try:
        out = await _call_impl(fn, *args, **kwargs)
        await _maybe_await(out)
    except asyncio.CancelledError:
        raise


# ──────────────────────────────────────────────────────────────────────────────
# Attribute forwarding (keeps old imports working)
# ──────────────────────────────────────────────────────────────────────────────
def __getattr__(name: str) -> Any:
    mod = _impl()
    try:
        return getattr(mod, name)
    except AttributeError as e:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}") from e


def __dir__() -> list[str]:
    base = set(globals().keys())
    with contextlib.suppress(Exception):
        base.update(dir(_impl()))
    return sorted(base)


__all__ = [
    "EventLogger",
    "axiom_impl_health",
    "get_logger",
    "main",
    "run",
    "run_scanner",
    "scan",
    "scan_axiom",
    "scan_once",
    "scan_loop",
]


_warn_once(
    "import",
    "Imported scanner.axiom shim; prefer scanner.axiom_scanner (set AXIOM_SHIM_WARN=0 to silence)",
)


if TYPE_CHECKING:  # pragma: no cover
    from . import axiom_scanner as _axiom_scanner  # noqa: F401


if __name__ == "__main__":  # pragma: no cover

    async def _m() -> None:
        r = await scan_axiom(offline=True)
        print("offline candidates:", len(r))
        if r:
            print(r[0].get("symbol"), r[0].get("mint"), r[0].get("score_combined"))

    asyncio.run(_m())
