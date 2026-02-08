from __future__ import annotations

"""
scanner/pumpfun.py

Pump.fun Scanner — Backwards-Compatible Wrapper (LOCK-IN)
========================================================

Canonical implementation lives in:
    scanner/pumpfun_scanner.py

This module exists ONLY to preserve old imports and entrypoints while ensuring:
- Zero network / heavy work at import time
- Render + local behave identically
- Safe degradation: if canonical impl fails to import, wrapper returns empty lists
  and logs a throttled error (never hard-crash the whole bot)

NEW (God-Tier contract hardening)
---------------------------------
Adds an explicit OFFLINE contract flag in the scan signature so the test suite
can validate candidate schema without Redis/RPC/HTTP:
    scan_pumpfun(offline=True) -> deterministic sample candidates

Public API (legacy-compatible)
------------------------------
- async scan_pumpfun(*args, **kwargs) -> list[dict[str, Any]]
- async scan_once(*args, **kwargs) -> list[dict[str, Any]]      (alias)
- async run_scanner(*args, **kwargs) -> None                    (runner)
- async run(*args, **kwargs) -> None                            (alias)
- async main(*args, **kwargs) -> None                           (alias)

Also exposes __getattr__/__dir__ to forward other attributes from canonical impl.

Phase A focus:
--------------
Fix Logger ↔ EventLogger typing fallout by returning a stable EventLogger-compatible logger
(no reliance on mismatched get_logger callables).
"""

import contextlib
import hashlib
import importlib
import inspect
import logging
import os
import time
from collections.abc import Callable
from types import ModuleType
from typing import Any, Protocol, TypeAlias, cast

JsonDict: TypeAlias = dict[str, Any]

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
      - `.event` / `.every` exist (best-effort throttle)
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
    """
    Best-effort; do not hard-fail if compat layer is missing.
    """
    with contextlib.suppress(Exception):
        from utils_pkg.log_compat import setup_logging as _sl  # type: ignore

        _sl(service=service)
        return
    _ = service


def get_logger(name: str | None = None) -> EventLogger:
    nm = (name or "scanner.pumpfun.wrapper").strip() or "scanner.pumpfun.wrapper"

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


LOG: EventLogger = get_logger("scanner.pumpfun.wrapper")

# =============================================================================
# Small helpers
# =============================================================================

_TRUE = {"1", "true", "t", "yes", "y", "on"}


def _truthy(x: Any) -> bool:
    if x is None:
        return False
    if isinstance(x, bool):
        return x
    return str(x).strip().lower() in _TRUE


def _offline_enabled(
    offline: bool,
    dry: bool | None,
    no_network: bool | None,
    test_mode: bool | None,
) -> bool:
    return bool(offline) or _truthy(dry) or _truthy(no_network) or _truthy(test_mode)


def _now() -> float:
    return time.time()


# =============================================================================
# Offline candidate generator (deterministic, schema-rich)
# =============================================================================

_B58_ALPH = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
_OFFLINE_TS = 1_700_000_000  # fixed deterministic timestamp


def _b58encode(data: bytes) -> str:
    """Tiny base58 encoder (no deps). Produces Solana-valid strings for 32-byte inputs."""
    if not data:
        return ""
    n = int.from_bytes(data, "big")
    out: list[str] = []
    while n > 0:
        n, r = divmod(n, 58)
        out.append(_B58_ALPH[r])

    # Preserve leading zeros as '1'
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
    Deterministic candidates that satisfy both canonical and legacy fields.
    Intentionally a superset to prevent schema drift across modules/tests.
    """
    mint0 = _stable_pubkey("pumpfun-offline-mint-0")
    mint1 = _stable_pubkey("pumpfun-offline-mint-1")
    pair0 = _stable_pubkey("pumpfun-offline-pair-0")
    pair1 = _stable_pubkey("pumpfun-offline-pair-1")

    base0: JsonDict = {
        # identifiers
        "source": "pumpfun",
        "scanner": "pumpfun",
        "dex": "pumpfun",
        "chain": "solana",
        "ts": _OFFLINE_TS,
        "time": _OFFLINE_TS,
        "mint": mint0,
        "token_mint": mint0,
        "pair_address": pair0,
        "pool": pair0,
        "address": mint0,
        # symbols
        "symbol": "PUMP_OFFLINE_A",
        "sym": "PUMP_OFFLINE_A",
        # liquidity / volume / activity
        "liq_usd": 95_000.0,
        "liquidity_usd": 95_000.0,
        "liq": 95_000.0,
        "vol_5m_usd": 7_250.0,
        "volume_5m_usd": 7_250.0,
        "vol5": 7_250.0,
        "buys_5m": 18,
        "sells_5m": 11,
        "buys5": 18,
        "sells5": 11,
        "chg_5m_pct": 3.25,
        "change_5m_pct": 3.25,
        "chg5": 3.25,
        "age_min": 8.0,
        "age_minutes": 8.0,
        # pump.fun-ish extras
        "pumpfun": True,
        "bonding_curve": True,
        "curve_progress": 0.42,
        "holders": 120,
        # extras commonly expected downstream
        "price_usd": 0.000041,
        "fdv_usd": 410_000.0,
        "mcap_usd": 410_000.0,
        "txns_5m": 29,
        "trades_5m": 29,
        # scoring placeholders
        "score": 0.55,
        "score_combined": 0.55,
        "score_label": "offline",
        "risk": "unknown",
        "flags": [],
        "notes": ["offline_contract_candidate"],
    }

    base1: JsonDict = dict(base0)
    base1.update(
        {
            "mint": mint1,
            "token_mint": mint1,
            "pair_address": pair1,
            "pool": pair1,
            "address": mint1,
            "symbol": "PUMP_OFFLINE_B",
            "sym": "PUMP_OFFLINE_B",
            "liq_usd": 210_000.0,
            "liquidity_usd": 210_000.0,
            "liq": 210_000.0,
            "vol_5m_usd": 22_500.0,
            "volume_5m_usd": 22_500.0,
            "vol5": 22_500.0,
            "buys_5m": 41,
            "sells_5m": 19,
            "buys5": 41,
            "sells5": 19,
            "chg_5m_pct": 6.10,
            "change_5m_pct": 6.10,
            "chg5": 6.10,
            "age_min": 4.0,
            "age_minutes": 4.0,
            "curve_progress": 0.67,
            "holders": 310,
            "price_usd": 0.000093,
            "fdv_usd": 1_200_000.0,
            "mcap_usd": 1_200_000.0,
            "txns_5m": 73,
            "trades_5m": 73,
            "score": 0.66,
            "score_combined": 0.66,
        }
    )

    items: list[JsonDict] = [base0, base1]
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
# Import + error throttle
# =============================================================================

_IMPL: ModuleType | None = None
_LAST_IMPORT_ERR_TS: float = 0.0

# Allow overriding impl module path (useful for quarantining experiments)
_IMPL_DOTTED = (
    os.getenv("PUMPFUN_IMPL_MODULE", "scanner.pumpfun_scanner").strip() or "scanner.pumpfun_scanner"
)


# Throttle window for noisy import failures (seconds)
def _env_float(name: str, default: float) -> float:
    v = os.getenv(name)
    if v is None or v == "":
        return default
    try:
        return float(v)
    except Exception:
        return default


_IMPORT_ERR_TTL: float = _env_float("PUMPFUN_IMPL_IMPORT_ERR_COOLDOWN_S", 15.0)


def _throttled_log_import_err(exc: BaseException) -> None:
    global _LAST_IMPORT_ERR_TS
    t = _now()
    if (t - _LAST_IMPORT_ERR_TS) < _IMPORT_ERR_TTL:
        return
    _LAST_IMPORT_ERR_TS = t
    LOG.exception(
        "Pumpfun wrapper: failed to import canonical scanner (scanner.pumpfun_scanner). "
        "Degrading to no-op. impl=%s err=%s",
        _IMPL_DOTTED,
        f"{type(exc).__name__}: {exc}",
    )


def _import_module_candidates() -> tuple[str, ...]:
    """
    Import paths that work whether 'scanner' is a package or the module is invoked
    in different ways (pytest, Render start, local scripts, etc).
    """
    # Keep stable canonical first, then package-relative, then legacy, then override.
    return (
        "scanner.pumpfun_scanner",
        ".pumpfun_scanner",
        "pumpfun_scanner",
        _IMPL_DOTTED,
    )


def _load_impl() -> ModuleType | None:
    """
    Load and cache the canonical implementation.
    Never raises: returns None on failure.
    """
    global _IMPL
    if _IMPL is not None:
        return _IMPL

    last_exc: BaseException | None = None
    for modpath in _import_module_candidates():
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
            continue

    if last_exc is not None:
        _throttled_log_import_err(last_exc)
    return None


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
    for item in x:
        if isinstance(item, dict):
            out.append(cast(JsonDict, item))
    return out


def _strip_offline_keys(kwargs: dict[str, Any]) -> dict[str, Any]:
    """
    Prevent signature mismatches when forwarding into canonical implementations.
    """
    drop = {
        "offline",
        "dry",
        "dry_run",
        "no_network",
        "sample",
        "test_mode",
        "limit",
    }
    return {k: v for (k, v) in kwargs.items() if k not in drop}


# =============================================================================
# Public API (legacy-compatible) + OFFLINE contract flag
# =============================================================================


def pumpfun_impl_health() -> dict[str, Any]:
    """
    Wrapper-level health snapshot (does not force network).
    """
    impl = _load_impl()
    health: dict[str, Any] = {
        "impl_module": _IMPL_DOTTED,
        "impl_loaded": bool(impl is not None),
    }
    if impl is None:
        return health

    fn = getattr(impl, "pumpfun_scanner_health", None)
    if callable(fn):
        with contextlib.suppress(Exception):
            h = fn()
            if isinstance(h, dict):
                health["impl_health"] = h
    return health


async def scan_pumpfun(
    *args: Any,
    offline: bool = False,
    dry: bool | None = None,
    dry_run: bool | None = None,
    no_network: bool | None = None,
    sample: bool | None = None,
    test_mode: bool | None = None,
    limit: int | None = None,
    **kwargs: Any,
) -> list[JsonDict]:
    """
    Backwards-compatible entrypoint.

    OFFLINE MODE (for tests/contract runs):
      scan_pumpfun(offline=True) -> deterministic sample candidates (no I/O)

    LIVE MODE:
      forwards to canonical implementation if available; otherwise safe empty list.
    """
    # Accept flags passed via kwargs (older harnesses / generic callers)
    if "offline" in kwargs and not offline:
        offline = _truthy(kwargs.pop("offline"))
    if "dry_run" in kwargs and dry_run is None:
        dry_run = _truthy(kwargs.pop("dry_run"))
    if "dry" in kwargs and dry is None:
        dry = _truthy(kwargs.pop("dry"))
    if "no_network" in kwargs and no_network is None:
        no_network = _truthy(kwargs.pop("no_network"))
    if "sample" in kwargs and sample is None:
        sample = _truthy(kwargs.pop("sample"))
    if "test_mode" in kwargs and test_mode is None:
        test_mode = _truthy(kwargs.pop("test_mode"))
    if "limit" in kwargs and limit is None:
        with contextlib.suppress(Exception):
            limit = int(kwargs.pop("limit"))

    # Sample should behave like offline (schema contract).
    dry_eff = dry if dry is not None else dry_run
    if _offline_enabled(offline, dry_eff, no_network, test_mode) or _truthy(sample):
        return _offline_candidates(limit=limit)

    impl = _load_impl()
    if impl is None:
        return []

    fn = _pick_callable(impl, ("scan_pumpfun", "scan_once", "scan"))
    if fn is None:
        return []

    fwd_kwargs = _strip_offline_keys(kwargs)
    try:
        res = await _call_maybe_async(fn, *args, **fwd_kwargs)
        items = _coerce_candidate_list(res)
    except Exception as e:
        LOG.exception(
            "Pumpfun wrapper: scan_pumpfun crashed (degrading). err=%s",
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


async def scan_once(
    *args: Any,
    offline: bool = False,
    dry: bool | None = None,
    dry_run: bool | None = None,
    no_network: bool | None = None,
    sample: bool | None = None,
    test_mode: bool | None = None,
    limit: int | None = None,
    **kwargs: Any,
) -> list[JsonDict]:
    """Alias; keeps signature compatible with offline contract flags."""
    return await scan_pumpfun(
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


async def run_scanner(*args: Any, **kwargs: Any) -> None:
    """
    Legacy runner entrypoint expected by older launch scripts.
    This wrapper never crashes the process if the canonical impl is missing/broken.
    """
    impl = _load_impl()
    if impl is None:
        return

    fn = _pick_callable(impl, ("run_scanner", "run", "main"))
    if fn is None:
        return

    try:
        with contextlib.suppress(Exception):
            setup_logging(service="scanner.pumpfun")
        await _call_maybe_async(fn, *args, **kwargs)
    except Exception as e:
        LOG.exception(
            "Pumpfun wrapper: run_scanner crashed (degrading). err=%s",
            f"{type(e).__name__}: {e}",
        )


async def run(*args: Any, **kwargs: Any) -> None:
    await run_scanner(*args, **kwargs)


async def main(*args: Any, **kwargs: Any) -> None:
    await run_scanner(*args, **kwargs)


async def shutdown_pumpfun_scanner() -> None:
    """
    Optional shutdown hook; safe to call multiple times.
    """
    impl = _load_impl()
    if impl is None:
        return

    fn = getattr(impl, "shutdown_pumpfun_scanner", None)
    if not callable(fn):
        return

    with contextlib.suppress(Exception):
        out = fn()
        if inspect.isawaitable(out):
            await out


# =============================================================================
# Attribute forwarding (keeps old imports working)
# =============================================================================


def __getattr__(name: str) -> Any:
    impl = _load_impl()
    if impl is None:
        raise AttributeError(name)
    return getattr(impl, name)


def __dir__() -> list[str]:
    base = set(globals().keys())
    impl = _load_impl()
    if impl is not None:
        base.update(dir(impl))
    return sorted(base)


__all__ = [
    "EventLogger",
    "LOG",
    "get_logger",
    "main",
    "pumpfun_impl_health",
    "run",
    "run_scanner",
    "scan_once",
    "scan_pumpfun",
    "setup_logging",
    "shutdown_pumpfun_scanner",
]
