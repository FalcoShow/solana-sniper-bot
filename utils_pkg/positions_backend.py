# utils_pkg/positions_backend.py
from __future__ import annotations

"""
utils_pkg/positions_backend.py

Positions Backend Interface (P0 / NO business logic)
====================================================

This module defines the *pluggable persistence interface* for “position mirrors”
(Postgres, legacy SQLite, or Noop).

LOCK-IN PRINCIPLES
------------------
- position_store.py is the **canonical** runtime source-of-truth (Redis-first).
- This module is ONLY about *optional persistence backends* (mirrors) used by:
    - reporting / analytics (PnL, daily_report)
    - audits / recovery helpers
    - “write-through” mirrors called by higher-level modules (best-effort)

What this module MUST NOT do:
- Must not implement trading logic, entry/exit logic, or “state machine” rules.
- Must not hide alternate position truth sources.
- Must not star-import different modules at import time.

Compatibility
-------------
Some legacy call sites may expect function-style helpers.
We provide thin async wrappers around the selected backend instance:

    await upsert_position(...)
    await close_position(...)
    rows = await fetch_open_positions(...)
    row  = await fetch_position(...)

We also provide aliases:
    pg_upsert_position / pg_close_position

Backend selection (safe, lazy)
------------------------------
- Primary switch: POSITIONS_BACKEND = auto|pg|sqlite|noop   (default: auto)
- Secondary: PG_ENABLED=true/false
- Optional: config.PG_ENABLED / config.POSITIONS_BACKEND if config imports cleanly
"""

import os
from dataclasses import dataclass
from typing import Any, Protocol, runtime_checkable

# -----------------------------------------------------------------------------
# Logger (safe fallback)
# -----------------------------------------------------------------------------
try:  # pragma: no cover
    from logging_setup import get_logger  # type: ignore
except Exception:  # pragma: no cover
    import logging

    def get_logger(name: str | None = None):  # type: ignore
        return logging.getLogger(name or "positions_backend")


log = get_logger(__name__)


# -----------------------------------------------------------------------------
# Types
# -----------------------------------------------------------------------------
@dataclass(frozen=True)
class PositionRow:
    """
    Minimal persistence row (mirror-friendly, backend-agnostic).

    NOTE: This is NOT the canonical runtime Position object.
    It’s a “wire shape” for DB/reporting usage.
    """

    wallet: str
    mint: str
    pool: str

    symbol: str | None = None
    status: str | None = None  # e.g. "open" | "closed" | "failed"
    size_raw: float | None = None
    size_sol: float | None = None
    entry_price: float | None = None

    opened_ts: int | None = None
    closed_ts: int | None = None
    updated_ts: int | None = None

    extra: dict[str, Any] | None = None


@runtime_checkable
class PositionsBackend(Protocol):
    """
    Backend interface. Implementations live in:
      - utils_pkg/positions_pg.py   (Postgres)
      - utils_pkg/positions.py      (Legacy SQLite, optional)
      - internal Noop backend here
    """

    name: str

    async def healthcheck(self) -> bool: ...

    async def upsert_position(
        self,
        *,
        wallet: str,
        mint: str,
        pool: str,
        symbol: str | None = None,
        size_raw: float | None = None,
        size_sol: float | None = None,
        entry_price: float | None = None,
        status: str = "open",
        opened_ts: int | None = None,
        updated_ts: int | None = None,
        extra: dict[str, Any] | None = None,
    ) -> None: ...

    async def close_position(
        self,
        *,
        wallet: str,
        mint: str,
        pool: str,
        status: str = "closed",
        closed_ts: int | None = None,
        updated_ts: int | None = None,
        extra: dict[str, Any] | None = None,
    ) -> None: ...

    async def fetch_position(
        self,
        *,
        wallet: str,
        mint: str,
        pool: str,
    ) -> PositionRow | None: ...

    async def fetch_open_positions(
        self,
        *,
        wallet: str | None = None,
        limit: int = 500,
    ) -> list[PositionRow]: ...


# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------
def _truthy(x: Any) -> bool:
    return str(x or "").strip().lower() in {"1", "true", "yes", "y", "on"}


def _get_config_attr(name: str) -> Any:
    """
    Best-effort read from config without making it a hard dependency.
    """
    try:
        import config as C  # type: ignore

        return getattr(C, name)
    except Exception:
        return None


def _get_setting(name: str, default: str = "") -> str:
    v = _get_config_attr(name)
    if v is None:
        v = os.getenv(name, default)
    return str(v or default).strip()


def _backend_choice() -> str:
    # priority: config.POSITIONS_BACKEND -> env POSITIONS_BACKEND -> auto
    choice = _get_setting("POSITIONS_BACKEND", "auto").lower()
    return choice or "auto"


def _pg_enabled() -> bool:
    v = _get_config_attr("PG_ENABLED")
    if v is None:
        v = os.getenv("PG_ENABLED", "false")
    return _truthy(v)


# -----------------------------------------------------------------------------
# Backends
# -----------------------------------------------------------------------------
class NoopPositionsBackend:
    """
    Safe default: does nothing, never throws.
    """

    name = "noop"

    async def healthcheck(self) -> bool:
        return True

    async def upsert_position(self, **kwargs: Any) -> None:
        return

    async def close_position(self, **kwargs: Any) -> None:
        return

    async def fetch_position(self, **kwargs: Any) -> PositionRow | None:
        return None

    async def fetch_open_positions(self, **kwargs: Any) -> list[PositionRow]:
        return []


# -----------------------------------------------------------------------------
# Lazy singleton selection
# -----------------------------------------------------------------------------
_BACKEND: PositionsBackend | None = None


def _try_make_pg_backend() -> PositionsBackend | None:
    try:
        # NOTE: we will rewrite utils_pkg/positions_pg.py next to provide this.
        from .positions_pg import PostgresPositionsBackend  # type: ignore

        return PostgresPositionsBackend()
    except Exception as e:
        log.warning("PG_BACKEND_UNAVAILABLE", extra={"err": str(e)[:200]})
        return None


def _try_make_sqlite_backend() -> PositionsBackend | None:
    try:
        # Legacy fallback only. We will later rewrite utils_pkg/positions.py to be a thin adapter.
        from .positions import SqlitePositionsBackend  # type: ignore

        return SqlitePositionsBackend()
    except Exception:
        return None


def get_backend() -> PositionsBackend:
    """
    Return the selected backend (singleton). Never raises.
    """
    global _BACKEND
    if _BACKEND is not None:
        return _BACKEND

    choice = _backend_choice()

    # Explicit selection
    if choice == "noop":
        _BACKEND = NoopPositionsBackend()
        return _BACKEND

    if choice == "pg":
        _BACKEND = _try_make_pg_backend() or NoopPositionsBackend()
        return _BACKEND

    if choice == "sqlite":
        _BACKEND = _try_make_sqlite_backend() or NoopPositionsBackend()
        return _BACKEND

    # AUTO selection
    if _pg_enabled():
        _BACKEND = _try_make_pg_backend() or NoopPositionsBackend()
        return _BACKEND

    # SQLite is legacy; only use if it exists, otherwise noop
    _BACKEND = _try_make_sqlite_backend() or NoopPositionsBackend()
    return _BACKEND


# -----------------------------------------------------------------------------
# Thin function wrappers (compat + convenience)
# -----------------------------------------------------------------------------
async def healthcheck() -> bool:
    return await get_backend().healthcheck()


async def upsert_position(
    *,
    wallet: str,
    mint: str,
    pool: str,
    symbol: str | None = None,
    size_raw: float | None = None,
    size_sol: float | None = None,
    entry_price: float | None = None,
    status: str = "open",
    opened_ts: int | None = None,
    updated_ts: int | None = None,
    extra: dict[str, Any] | None = None,
) -> None:
    await get_backend().upsert_position(
        wallet=wallet,
        mint=mint,
        pool=pool,
        symbol=symbol,
        size_raw=size_raw,
        size_sol=size_sol,
        entry_price=entry_price,
        status=status,
        opened_ts=opened_ts,
        updated_ts=updated_ts,
        extra=extra,
    )


async def close_position(
    *,
    wallet: str,
    mint: str,
    pool: str,
    status: str = "closed",
    closed_ts: int | None = None,
    updated_ts: int | None = None,
    extra: dict[str, Any] | None = None,
) -> None:
    await get_backend().close_position(
        wallet=wallet,
        mint=mint,
        pool=pool,
        status=status,
        closed_ts=closed_ts,
        updated_ts=updated_ts,
        extra=extra,
    )


async def fetch_position(*, wallet: str, mint: str, pool: str) -> PositionRow | None:
    return await get_backend().fetch_position(wallet=wallet, mint=mint, pool=pool)


async def fetch_open_positions(*, wallet: str | None = None, limit: int = 500) -> list[PositionRow]:
    return await get_backend().fetch_open_positions(wallet=wallet, limit=int(limit))


# Backwards-compat aliases (some modules call these names)
pg_upsert_position = upsert_position
pg_close_position = close_position


__all__ = [
    "NoopPositionsBackend",
    "PositionRow",
    "PositionsBackend",
    "close_position",
    "fetch_open_positions",
    "fetch_position",
    "get_backend",
    "healthcheck",
    "pg_close_position",
    "pg_upsert_position",
    "upsert_position",
]
