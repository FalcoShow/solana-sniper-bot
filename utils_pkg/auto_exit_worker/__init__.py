# utils_pkg/auto_exit_worker/__init__.py
from __future__ import annotations

"""
utils_pkg.auto_exit_worker

God-Tier Auto Exit Worker package — V3+++ LOCK-IN
=================================================

DEDUP / CONTRACT
----------------
- utils_pkg/exit_engine.py is the *canonical exit brain* (decisions).
- utils_pkg/auto_exit_worker/exit_engine.py is the *canonical exit executor*
  (queue semantics + crash-safe SELL execution).

This package __init__ is intentionally THIN:
- No side effects (no network, no Redis, no asyncio run, no background tasks).
- Exposes a stable import surface for the rest of the codebase.
- Uses lazy attribute loading to avoid importing heavy modules on simple imports.

Canonical usage:
    from utils_pkg.auto_exit_worker import enqueue_exit_request, worker_loop

Render worker entry should call:
    await worker_loop()

Do NOT add competing exit logic here.
"""

from importlib import import_module
from typing import TYPE_CHECKING, Any

__version__ = "3.0.0"

# Type-checker friendly imports (no runtime side effects thanks to TYPE_CHECKING)
if TYPE_CHECKING:  # pragma: no cover
    from .exit_engine import (
        ExitRequest,
        enqueue_exit_request,
        process_one,
        prune_done,
        reclaim_stale_inflight,
        worker_loop,
    )

# Stable public surface (keep this list tight and intentional)
__all__ = [
    "ExitRequest",
    "__version__",
    "enqueue_exit_request",
    "process_one",
    "prune_done",
    "reclaim_stale_inflight",
    "worker_loop",
]

# Lazy re-exports (PEP 562)
_LAZY_EXPORTS = {
    "ExitRequest",
    "enqueue_exit_request",
    "process_one",
    "reclaim_stale_inflight",
    "prune_done",
    "worker_loop",
}


def __getattr__(name: str) -> Any:
    """
    Lazily resolve exports from .exit_engine to keep import side-effects minimal.
    """
    if name in _LAZY_EXPORTS:
        mod = import_module(".exit_engine", __name__)
        return getattr(mod, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def __dir__() -> list[str]:
    return sorted(list(set(globals().keys()) | set(_LAZY_EXPORTS)))
