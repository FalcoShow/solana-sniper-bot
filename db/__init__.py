# db/__init__.py
from __future__ import annotations

"""
db package (God-Tier Overhaul / contract-safe)

Contract
--------
- MUST be side-effect free on import:
    - no DB connections
    - no pool creation
    - no schema creation
- Expose a stable public API for schema + logging utilities.

Canonical modules:
- db.schema  -> ensure_schema()
- db.logger  -> async DB writer utilities

This __init__ is intentionally LAZY:
- Importing `db` does NOT import `db.logger` (prevents import-time side effects).
- `from db import log_trade_event` still works via PEP 562 __getattr__.
"""

import importlib
from typing import TYPE_CHECKING, Any

_LOGGER_EXPORTS: tuple[str, ...] = (
    "close_pool",
    "get_writer",
    "log_candidate",
    "log_candidate_async",
    "log_error_event",
    "log_pnl_snapshot",
    "log_scan_batch",
    "log_scan_row",
    "log_trade",
    "log_trade_async",
    "log_trade_event",
    "record_legacy_trade",
    "start_writer",
    "stop_writer",
    "tiny_row",
)

_SCHEMA_EXPORTS: tuple[str, ...] = ("ensure_schema",)

__all__ = [*_SCHEMA_EXPORTS, *_LOGGER_EXPORTS]


def _lazy_import_logger() -> Any:
    return importlib.import_module(".logger", __name__)


def _lazy_import_schema() -> Any:
    return importlib.import_module(".schema", __name__)


def __getattr__(name: str) -> Any:
    if name in _SCHEMA_EXPORTS:
        mod = _lazy_import_schema()
        try:
            return getattr(mod, name)
        except AttributeError as e:  # pragma: no cover
            raise AttributeError(f"db.schema missing {name}") from e

    if name in _LOGGER_EXPORTS:
        mod = _lazy_import_logger()
        try:
            return getattr(mod, name)
        except AttributeError as e:  # pragma: no cover
            raise AttributeError(f"db.logger missing {name}") from e

    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def __dir__() -> list[str]:
    return sorted(set(globals().keys()) | set(__all__))


if TYPE_CHECKING:
    from .logger import (  # noqa: F401
        close_pool,
        get_writer,
        log_candidate,
        log_candidate_async,
        log_error_event,
        log_pnl_snapshot,
        log_scan_batch,
        log_scan_row,
        log_trade,
        log_trade_async,
        log_trade_event,
        record_legacy_trade,
        start_writer,
        stop_writer,
        tiny_row,
    )
    from .schema import ensure_schema as ensure_schema  # noqa: F401
