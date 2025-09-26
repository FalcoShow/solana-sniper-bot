# utils_pkg/positions.py
from __future__ import annotations

"""
Single source of truth for positions storage.

This module intentionally re-exports the Postgres-backed implementation
from `utils_pkg.positions_pg`. Keep it minimal to avoid duplicated logic
or accidental divergence between backends.

Required env (handled inside positions_pg):
- DATABASE_URL  (or PG_DSN_INTERNAL / PG_DSN_EXTERNAL)

Public API:
- all() -> list[dict]
- get(mint: str) -> dict | None
- remove(mint: str) -> None
- decrement_qty(mint: str, sold_raw: int) -> None
- update_after_sell(mint: str, sold_raw: int, tx_sig: str | None, closed: bool) -> None
- record_sell(mint: str, qty_raw: int, sig: str | None) -> None
- record_buy(...)
"""

from .positions_pg import (  # re-export
    all as all,
    get as get,
    remove as remove,
    decrement_qty as decrement_qty,
    update_after_sell as update_after_sell,
    record_sell as record_sell,
    record_buy as record_buy,
)

__all__ = [
    "all",
    "get",
    "remove",
    "decrement_qty",
    "update_after_sell",
    "record_sell",
    "record_buy",
]
