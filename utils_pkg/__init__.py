# utils_pkg/__init__.py
from __future__ import annotations


"""
utils_pkg package

CRITICAL: Keep this file ultra-lightweight.

- Do NOT import heavy deps here (aiohttp/httpx/requests/numpy/pandas/solana SDK, etc).
- Do NOT import runtime modules that may create sessions or do I/O at import time.
- Use lazy imports for convenience re-exports.

This is required for tests that assert importing `utils_pkg.contracts` remains lightweight,
because importing `utils_pkg.contracts` executes this __init__.py first.

Additionally:
- This file must NEVER throw during import, or pytest collection can break.
"""

from importlib import import_module
from typing import Any

# ---------------------------------------------------------------------------
# Safe, stdlib-only re-exports.
# ---------------------------------------------------------------------------
from . import contracts as contracts  # stdlib-only module by contract


# Backwards compatible names you already expose:
build = contracts.build
build_contracts_surface = contracts.build_contracts_surface

# Common contract types (stdlib-only) re-exported for convenience
Candidate = contracts.Candidate
Tick = contracts.Tick
EntrySignal = contracts.EntrySignal
RouteQuote = contracts.RouteQuote
OrderIntent = contracts.OrderIntent
Fill = contracts.Fill
Position = contracts.Position
ExitSignal = contracts.ExitSignal
RiskSnapshot = contracts.RiskSnapshot
EventEnvelope = contracts.EventEnvelope

# ---------------------------------------------------------------------------
# Lazy submodules (NO eager import here).
# IMPORTANT: router must resolve as a MODULE (not a singleton / callable).
# ---------------------------------------------------------------------------
_LAZY_SUBMODULES: set[str] = {
    # core plumbing
    "core_utils",
    "logging_setup",
    "contracts",
    "mints",
    "mint_guard",
    # trading / routing
    "router",
    "exit_engine",
    "honeypot",
    "price_feed",
    # AI / analytics
    "ai_db",
    "ai_entry_logic",
    "ai_buy_scoring",
    "feature_builder",
    "data_logger",
}

__all__ = [
    # stdlib-only surface
    "contracts",
    "build",
    "build_contracts_surface",
    "build_app",
    "Candidate",
    "Tick",
    "EntrySignal",
    "RouteQuote",
    "OrderIntent",
    "Fill",
    "Position",
    "ExitSignal",
    "RiskSnapshot",
    "EventEnvelope",
    # lazy modules
    *_LAZY_SUBMODULES,
]


def __getattr__(name: str) -> Any:
    """
    Lazy-load selected submodules on demand.

    IMPORTANT:
    - Cache into globals() so repeated attribute access is stable and fast.
    - Never import heavy modules unless explicitly requested.
    - `router` MUST be the *module* object (utils_pkg.router), not a callable/singleton.
    """
    if name in _LAZY_SUBMODULES:
        mod = import_module(f"{__name__}.{name}")
        globals()[name] = mod
        return mod
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def __dir__() -> list[str]:
    return sorted(set(globals().keys()) | set(_LAZY_SUBMODULES))


def build_app(*, safe: bool = True, **kwargs: Any) -> dict[str, Any]:
    """
    Contract startup builder: must be import-safe and must not touch network.
    In safe mode, return a lightweight surface map.

    This is intentionally minimal and uses only stdlib-only contracts helpers.
    """
    return {
        "safe": bool(safe),
        "contracts": contracts.build_contracts_surface(),
        "kwargs": dict(kwargs),
    }
