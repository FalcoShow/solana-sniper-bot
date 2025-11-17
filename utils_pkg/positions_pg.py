from __future__ import annotations

import os
import time
from typing import Any, Dict, List, Optional

from logging_setup import get_logger

# SQLAlchemy Core (sync)
from sqlalchemy import (
    BigInteger,
    Column,
    DateTime,
    Float,
    Integer,
    MetaData,
    String,
    Table,
    Index,
    create_engine,
    func,
    select,
    update as sa_update,
    insert as sa_insert,
    delete as sa_delete,
)
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

log = get_logger()

# ──────────────────────────────────────────────────────────────────────────────
# DSN / Engine helpers
# ──────────────────────────────────────────────────────────────────────────────


def _pick_dsn() -> str:
    """
    Pick a Postgres DSN from env.

    Priority:
      1. PG_DSN_INTERNAL
      2. PG_DSN_EXTERNAL
      3. DATABASE_URL

    If an async driver (postgresql+asyncpg) is provided, we swap it to the
    sync driver (postgresql) so SQLAlchemy Core can use it.
    """
    dsn = ""
    for key in ("PG_DSN_INTERNAL", "PG_DSN_EXTERNAL", "DATABASE_URL"):
        v = (os.getenv(key) or "").strip()
        if v:
            dsn = v
            break

    if not dsn:
        raise RuntimeError(
            "positions_pg: No Postgres DSN found. "
            "Set PG_DSN_INTERNAL / PG_DSN_EXTERNAL / DATABASE_URL."
        )

    if "+asyncpg" in dsn:
        dsn = dsn.replace("+asyncpg", "")

    return dsn


_ENGINE: Optional[Engine] = None
_META = MetaData()

# ──────────────────────────────────────────────────────────────────────────────
# Schema
# ──────────────────────────────────────────────────────────────────────────────

positions = Table(
    "positions",
    _META,
    Column("mint", String, primary_key=True),
    # raw on-chain token amount (integer, using mint decimals)
    Column("qty_raw", BigInteger, nullable=False),
    Column("decimals", Integer, nullable=False),
    Column("token_program", String, nullable=True),
    # average entry cost in SOL per token (UI-side)
    Column("avg_cost_sol", Float, nullable=False),
    # opened_at uses epoch seconds for simplicity
    Column("opened_at", Float, nullable=False),
    Column("owner_ata", String, nullable=True),
    Column("symbol", String, nullable=True),
    Column("pool", String, nullable=True),
    Column("last_sig", String, nullable=True),
    Column(
        "created_at",
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
    ),
    Column(
        "updated_at",
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
    ),
)

sells = Table(
    "sells",
    _META,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("mint", String, nullable=False),
    Column("qty_raw", BigInteger, nullable=False),
    Column("sig", String, nullable=True),
    Column(
        "created_at",
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
    ),
)

buys = Table(
    "buys",
    _META,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("mint", String, nullable=False),
    Column("qty_raw", BigInteger, nullable=False),
    # SOL per token (user-facing price)
    Column("price_per_token_sol", Float, nullable=False),
    Column("sig", String, nullable=True),
    Column(
        "created_at",
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
    ),
)

# Helpful lightweight indexes
Index("idx_positions_updated_at", positions.c.updated_at)
Index("idx_sells_mint_created", sells.c.mint, sells.c.created_at)
Index("idx_buys_mint_created", buys.c.mint, buys.c.created_at)

# ──────────────────────────────────────────────────────────────────────────────
# Engine init + schema ensure
# ──────────────────────────────────────────────────────────────────────────────


def _ensure_engine() -> Engine:
    """
    Lazily create the global sync Engine and ensure the tables exist.
    """
    global _ENGINE
    if _ENGINE is not None:
        return _ENGINE

    dsn = _pick_dsn()

    # Pool knobs; safe defaults
    pool_size = int(os.getenv("PG_POOL_SIZE", "5") or 5)
    max_overflow = int(os.getenv("PG_POOL_MAX_OVERFLOW", "10") or 10)

    _ENGINE = create_engine(
        dsn,
        pool_size=pool_size,
        max_overflow=max_overflow,
        pool_pre_ping=True,
        future=True,
    )

    try:
        _META.create_all(_ENGINE)
    except SQLAlchemyError as e:
        raise RuntimeError(f"positions_pg: failed to create tables: {e}") from e

    # Mask credentials in log (show host/db only)
    masked = dsn.split("@")[-1] if "@" in dsn else dsn
    log.event("PG_POS_ENGINE_READY", dsn=masked)
    return _ENGINE


# ──────────────────────────────────────────────────────────────────────────────
# Internal helpers
# ──────────────────────────────────────────────────────────────────────────────


def _qty_ui(qty_raw: int, decimals: int) -> float:
    """
    Convert a raw token amount into UI amount using decimals.
    """
    try:
        d = int(decimals)
        if d < 0:
            d = 0
        if d > 18:
            d = 18
        return float(qty_raw) / (10 ** d)
    except Exception:
        return 0.0


def _row_to_pos(row) -> Dict[str, Any]:
    """
    Convert a SQLAlchemy Row into a plain dict with qty_ui convenience field.
    """
    d = dict(row._mapping)  # RowMapping -> dict
    d["qty_ui"] = _qty_ui(
        int(d.get("qty_raw", 0) or 0),
        int(d.get("decimals", 0) or 0),
    )
    return d


# ──────────────────────────────────────────────────────────────────────────────
# Public API (sync) — drop-in replacements for utils_pkg.positions
# ──────────────────────────────────────────────────────────────────────────────


def all() -> List[Dict[str, Any]]:
    """
    Return all open positions as a list of dicts.

    Each dict contains the raw DB columns plus qty_ui.
    """
    eng = _ensure_engine()
    with eng.connect() as cx:
        res = cx.execute(select(positions))
        return [_row_to_pos(r) for r in res.fetchall()]


def get(mint: str) -> Optional[Dict[str, Any]]:
    """
    Get a single position by mint, or None if not present.
    """
    eng = _ensure_engine()
    with eng.connect() as cx:
        res = cx.execute(
            select(positions).where(positions.c.mint == mint)
        ).first()
        return _row_to_pos(res) if res else None


def remove(mint: str) -> None:
    """
    Hard delete a position by mint.
    """
    eng = _ensure_engine()
    with eng.begin() as cx:
        cx.execute(sa_delete(positions).where(positions.c.mint == mint))


def decrement_qty(mint: str, sold_raw: int) -> None:
    """
    Reduce the qty_raw for a mint by sold_raw.

    If the resulting qty_raw <= 0, the position is deleted.
    """
    if sold_raw <= 0:
        return

    eng = _ensure_engine()
    with eng.begin() as cx:
        res = cx.execute(
            select(positions.c.qty_raw).where(positions.c.mint == mint)
        ).first()
        if not res:
            return

        cur = int(res[0] or 0)
        newq = max(0, cur - int(sold_raw))

        if newq <= 0:
            cx.execute(sa_delete(positions).where(positions.c.mint == mint))
        else:
            cx.execute(
                sa_update(positions)
                .where(positions.c.mint == mint)
                .values(
                    qty_raw=newq,
                    updated_at=func.now(),
                )
            )


def update_after_sell(
    *,
    mint: str,
    sold_raw: int,
    tx_sig: Optional[str],
    closed: bool,
) -> None:
    """
    Record a sell in the sells table and update / close the position.

    Args:
        mint: token mint.
        sold_raw: raw token amount sold.
        tx_sig: optional transaction signature.
        closed: if True, position is forcibly closed regardless of remaining qty.
    """
    if sold_raw <= 0:
        return

    sold_raw = int(sold_raw)
    eng = _ensure_engine()

    with eng.begin() as cx:
        # Log the sell event
        cx.execute(
            sa_insert(sells).values(
                mint=mint,
                qty_raw=sold_raw,
                sig=(tx_sig or None),
            )
        )

        # Fetch current qty
        res = cx.execute(
            select(positions.c.qty_raw).where(positions.c.mint == mint)
        ).first()
        if not res:
            return

        cur = int(res[0] or 0)
        newq = max(0, cur - sold_raw)

        if closed or newq <= 0:
            cx.execute(sa_delete(positions).where(positions.c.mint == mint))
        else:
            cx.execute(
                sa_update(positions)
                .where(positions.c.mint == mint)
                .values(
                    qty_raw=newq,
                    last_sig=(tx_sig or None),
                    updated_at=func.now(),
                )
            )


def record_sell(*, mint: str, qty_raw: int, sig: Optional[str]) -> None:
    """
    Convenience wrapper for a partial sell that does NOT force-close the position.
    """
    update_after_sell(
        mint=mint,
        sold_raw=int(qty_raw),
        tx_sig=sig,
        closed=False,
    )


def record_buy(
    *,
    mint: str,
    qty_raw: int,
    price_per_token_sol: float,  # SOL per token (UI)
    decimals: int,
    token_program: Optional[str],
    owner_ata: str = "",
    symbol: Optional[str] = None,
    pool: Optional[str] = None,
    sig: Optional[str] = None,
) -> None:
    """
    Upsert a position with a weighted-average entry price and increased qty.
    Also logs into the buys table.

    This is the sync PG-backed version used by the sniper to persist entries.
    """
    if qty_raw <= 0 or decimals is None:
        return

    eng = _ensure_engine()
    qty_raw = int(qty_raw)
    decimals = int(decimals)

    # Normalize decimals range
    if decimals < 0:
        decimals = 0
    if decimals > 18:
        decimals = 18

    new_ui = _qty_ui(qty_raw, decimals)
    px = float(price_per_token_sol or 0.0)
    now = time.time()

    with eng.begin() as cx:
        cur = cx.execute(
            select(
                positions.c.qty_raw,
                positions.c.avg_cost_sol,
                positions.c.decimals,
            ).where(positions.c.mint == mint)
        ).first()

        if cur:
            old_qty_raw = int(cur[0] or 0)

            # Always compute UI using the *latest* decimals we believe
            # for this mint to avoid weirdness when decimals get corrected.
            old_ui = _qty_ui(old_qty_raw, decimals)
            total_ui = old_ui + new_ui

            if total_ui <= 0:
                new_avg = px
            else:
                old_avg = float(cur[1] or 0.0)
                new_avg = ((old_avg * old_ui) + (px * new_ui)) / total_ui

            cx.execute(
                sa_update(positions)
                .where(positions.c.mint == mint)
                .values(
                    qty_raw=old_qty_raw + qty_raw,
                    decimals=decimals,
                    token_program=token_program,
                    avg_cost_sol=new_avg,
                    owner_ata=(owner_ata or None),
                    symbol=(symbol or None),
                    pool=(pool or None),
                    last_sig=(sig or None),
                    updated_at=func.now(),
                )
            )
        else:
            # New position
            cx.execute(
                sa_insert(positions).values(
                    mint=mint,
                    qty_raw=qty_raw,
                    decimals=decimals,
                    token_program=token_program,
                    avg_cost_sol=float(px),
                    opened_at=float(now),
                    owner_ata=(owner_ata or None),
                    symbol=(symbol or None),
                    pool=(pool or None),
                    last_sig=(sig or None),
                )
            )

        # Log the buy
        cx.execute(
            sa_insert(buys).values(
                mint=mint,
                qty_raw=qty_raw,
                price_per_token_sol=float(px),
                sig=(sig or None),
            )
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
