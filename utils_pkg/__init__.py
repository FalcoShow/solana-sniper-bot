# utils_pkg/__init__.py
from __future__ import annotations

"""
utils_pkg package

CRITICAL: Keep this file ultra-lightweight.

Rules
-----
- Do NOT import heavy deps here (aiohttp/httpx/requests/numpy/pandas/solana SDK, etc).
- Do NOT import runtime modules that may create sessions or do I/O at import time.
- Never touch network, wallet, Redis, or Postgres at import time.
- This file must NEVER throw during import (pytest collection depends on it).

Key contract
------------
- `from utils_pkg import router` MUST resolve to the *module* `utils_pkg.router`
  (NOT a singleton, NOT a function, NOT a proxy object).

To preserve historical convenience, we provide:
- `get_router()` -> returns utils_pkg.router.get_router() (router singleton)
- `router_obj()` -> same as get_router() (explicitly named to avoid shadowing)
"""

from importlib import import_module
from types import ModuleType
from typing import TYPE_CHECKING, Any, Final

# ---------------------------------------------------------------------------
# stdlib-only: contracts is required to remain stdlib-only by project contract.
# Importing it here is safe.
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

__version__: Final[str] = "v3+++_init_probe_safe_LOCKIN_2026-01-17"

# ---------------------------------------------------------------------------
# IMPORTANT: Prevent submodule shadowing.
#
# If some prior code (or an old init) set `utils_pkg.router = get_router()`
# (or anything non-module), pytest will import that object and tests fail
# because it won't have `build_swap_payload`, etc.
#
# We DO NOT import the router module here (stays lightweight); we simply
# un-shadow names so Python can import submodules normally.
# ---------------------------------------------------------------------------
_SHADOW_SUSPECTS: tuple[str, ...] = (
    "router",
    "core_utils",
    "exit_engine",
    "ai_db",
    "ai_entry_logic",
    "ai_buy_scoring",
    "feature_builder",
    "data_logger",
    "honeypot",
    "price_feed",
    "mints",
    "mint_guard",
    "logging_setup",
)

for _name in _SHADOW_SUSPECTS:
    _obj = globals().get(_name)
    # If it's present but not a module, it is shadowing the submodule import.
    if _obj is not None and not isinstance(_obj, ModuleType):
        # Keep a reference under a safe name so we don't "remove features".
        globals()[f"{_name}_shadowed_obj"] = _obj
        del globals()[_name]

# ---------------------------------------------------------------------------
# Lazy submodules (NO eager import here).
# NOTE: `from utils_pkg import router` should import the submodule normally
# even without __getattr__, but we keep a strict lazy path too.
# ---------------------------------------------------------------------------
_LAZY_SUBMODULES: frozenset[str] = frozenset(
    {
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
)

__all__ = [
    # stdlib-only surface
    "contracts",
    "build",
    "build_contracts_surface",
    "build_app",
    "__version__",
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
    # convenience accessors (safe)
    "get_router",
    "router_obj",
    # lazy modules
    *_LAZY_SUBMODULES,
]


def _load_submodule(name: str) -> ModuleType:
    mod = import_module(f"{__name__}.{name}")
    if not isinstance(mod, ModuleType):
        # Extremely defensive: should never happen, but protects the contract.
        raise TypeError(f"{__name__}.{name} did not resolve to a module")
    globals()[name] = mod
    return mod


def __getattr__(name: str) -> Any:
    """
    Lazy-load selected submodules on demand.

    Contract guarantees:
    - `router` returns the module object `utils_pkg.router`
    - never returns a singleton or callable under the name `router`
    """
    if name in _LAZY_SUBMODULES:
        return _load_submodule(name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def __dir__() -> list[str]:
    return sorted(set(globals().keys()) | set(_LAZY_SUBMODULES))


def get_router() -> Any:
    """
    Backwards-compatible convenience accessor.

    Returns the router singleton from the router module:
        utils_pkg.router.get_router()

    This avoids shadowing `utils_pkg.router` (which must remain the module).
    """
    r = _load_submodule("router")
    fn = getattr(r, "get_router", None)
    if not callable(fn):
        raise AttributeError("utils_pkg.router.get_router is missing")
    return fn()


def router_obj() -> Any:
    """
    Explicit alias for the router singleton accessor (same as get_router()).
    Named to avoid shadowing the `router` submodule.
    """
    return get_router()


def build_app(*, safe: bool = True, **kwargs: Any) -> dict[str, Any]:
    """
    Contract startup builder: import-safe and must not touch network.
    In safe mode, return a lightweight surface map.

    This is intentionally minimal and uses only stdlib-only contracts helpers.
    """
    return {
        "safe": bool(safe),
        "contracts": contracts.build_contracts_surface(),
        "kwargs": dict(kwargs),
    }


if TYPE_CHECKING:
    # Helps editors without importing heavy modules at runtime.
    from . import router as router  # noqa: F401
