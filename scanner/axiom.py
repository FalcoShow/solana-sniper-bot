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
- Provide an OFFLINE contract path for tests (SCANNER_LIVE_TESTS=0):
      - explicit offline flags in signature
      - deterministic sample candidate output
- Forward live behavior 1:1 to the canonical implementation at call-time.

Env toggles
-----------
- AXIOM_SHIM_WARN=1        -> warn once on import and once on first use
- AXIOM_IMPL_MODULE=...    -> override impl module (default scanner.axiom_scanner)
"""

import contextlib
import importlib
import os
from collections.abc import Callable
from functools import lru_cache
from types import ModuleType
from typing import TYPE_CHECKING, Any

# -----------------------------------------------------------------------------
# Logger (best-effort; never hard-fail)
# -----------------------------------------------------------------------------
try:
    from logging_setup import get_logger  # type: ignore

    try:
        _log = get_logger("scanner.axiom")  # type: ignore[misc]
    except TypeError:  # pragma: no cover
        _log = get_logger()  # type: ignore[misc]
except Exception:  # pragma: no cover
    import logging

    _log = logging.getLogger("scanner.axiom")


_TRUE = {"1", "true", "t", "yes", "y", "on"}
_WARN = str(os.getenv("AXIOM_SHIM_WARN", "")).strip().lower() in _TRUE
_warned_import = False
_warned_use = False


def _warn_once(kind: str, msg: str) -> None:
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
    with contextlib.suppress(Exception):
        _log.warning(msg)


# -----------------------------------------------------------------------------
# Canonical impl loader (lazy)
# -----------------------------------------------------------------------------
_IMPL_DOTTED = (
    os.getenv("AXIOM_IMPL_MODULE", "scanner.axiom_scanner").strip() or "scanner.axiom_scanner"
)


@lru_cache(maxsize=1)
def _impl() -> ModuleType:
    """
    Lazy-load canonical implementation.
    IMPORTANT: axiom_scanner.py must not perform network I/O at import time.
    """
    # Default: sibling import keeps package-safe behavior under pytest + Render.
    if _IMPL_DOTTED in {"scanner.axiom_scanner", "axiom_scanner"}:
        if not __package__:
            return importlib.import_module("scanner.axiom_scanner")
        return importlib.import_module(".axiom_scanner", package=__package__)
    return importlib.import_module(_IMPL_DOTTED)


def _pick_callable(mod: ModuleType, names: tuple[str, ...]) -> Callable[..., Any] | None:
    for n in names:
        fn = getattr(mod, n, None)
        if callable(fn):
            return fn
    return None


async def _maybe_await(x: Any) -> Any:
    if hasattr(x, "__await__"):
        return await x
    return x


# -----------------------------------------------------------------------------
# Offline contract candidate (deterministic)
# -----------------------------------------------------------------------------
def _offline_sample_candidate() -> dict[str, Any]:
    # Deterministic values (avoid flakiness)
    ts = int(os.getenv("PYTEST_SAMPLE_TS", "1700000000") or 1700000000)

    # Use a real-looking Solana mint (USDC) to satisfy any "address-like" checks
    mint = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"

    # "pair_address" just needs to be a stable string in most schemas
    pair = "9wFFmX7d2Gz7m2bqE8uXJxdtD4F7sKJkVd3QeK7y1m8X"

    return {
        "source": "axiom",
        "scanner": "axiom",
        "chain": "solana",
        "dex": "axiom",
        "symbol": "AXIOM_TEST",
        "mint": mint,
        "pair_address": pair,
        "ts": ts,
        "age_min": 42.0,
        "price_usd": 0.000123,
        "liq_usd": 12345.67,
        "vol_5m_usd": 2345.67,
        "vol_1h_usd": 45678.9,
        "buys_5m": 12,
        "sells_5m": 5,
        "chg_5m": 3.21,
        "score_combined": 0.73,
        "score_label": "OFFLINE_SAMPLE",
        "url": "https://axiom.example/offline",
        "meta": {
            "offline": True,
            "sample": True,
            "impl_module": _IMPL_DOTTED,
        },
    }


def axiom_impl_health() -> dict[str, Any]:
    """
    Wrapper-level health snapshot (no network).
    """
    health: dict[str, Any] = {
        "impl_module": _IMPL_DOTTED,
        "impl_loaded": False,
    }
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


# -----------------------------------------------------------------------------
# Public API (preferred entrypoints)
# -----------------------------------------------------------------------------
async def scan_axiom(
    *args: Any,
    offline: bool = False,
    dry: bool = False,
    dry_run: bool = False,
    no_network: bool = False,
    sample: bool = False,
    **kwargs: Any,
) -> list[dict[str, Any]]:
    """
    Backwards-compatible scanner entrypoint.

    Offline contract path:
      - If any offline-ish flag is True, returns deterministic sample candidates
        WITHOUT importing/calling the heavy implementation.
    """
    _warn_once(
        "use",
        "scanner.axiom is a compatibility shim; prefer scanner.axiom_scanner for canonical impl.",
    )

    offline_mode = bool(offline or dry or dry_run or no_network or sample)
    if offline_mode:
        return [_offline_sample_candidate()]

    mod = _impl()
    fn = _pick_callable(mod, ("scan_axiom", "scan"))
    if fn is None:
        raise RuntimeError("scanner.axiom_scanner.scan_axiom/scan is missing")
    out = fn(*args, **kwargs)
    res = await _maybe_await(out)
    return res if isinstance(res, list) else []


async def scan(
    *args: Any,
    offline: bool = False,
    dry: bool = False,
    dry_run: bool = False,
    no_network: bool = False,
    sample: bool = False,
    **kwargs: Any,
) -> list[dict[str, Any]]:
    """
    Generic alias some registries use.
    """
    return await scan_axiom(
        *args,
        offline=offline,
        dry=dry,
        dry_run=dry_run,
        no_network=no_network,
        sample=sample,
        **kwargs,
    )


async def run_scanner(*args: Any, **kwargs: Any) -> None:
    _warn_once("use", "scanner.axiom is a compatibility shim; prefer scanner.axiom_scanner")
    mod = _impl()
    fn = _pick_callable(mod, ("run_scanner", "run", "main"))
    if fn is None:
        return
    out = fn(*args, **kwargs)
    await _maybe_await(out)


async def run(*args: Any, **kwargs: Any) -> None:
    await run_scanner(*args, **kwargs)


async def main(*args: Any, **kwargs: Any) -> None:
    await run_scanner(*args, **kwargs)


# -----------------------------------------------------------------------------
# Attribute forwarding (keeps old imports working)
# -----------------------------------------------------------------------------
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
    "axiom_impl_health",
    "main",
    "run",
    "run_scanner",
    "scan",
    "scan_axiom",
]


_warn_once(
    "import",
    "Imported scanner.axiom shim; prefer scanner.axiom_scanner (set AXIOM_SHIM_WARN=0 to silence)",
)


if TYPE_CHECKING:  # pragma: no cover
    from . import axiom_scanner as _axiom_scanner  # noqa: F401


if __name__ == "__main__":  # pragma: no cover
    import asyncio

    async def _m() -> None:
        r = await scan_axiom(offline=True)
        print("offline candidates:", len(r))
        if r:
            print(r[0].get("symbol"), r[0].get("mint"), r[0].get("score_combined"))

    asyncio.run(_m())
