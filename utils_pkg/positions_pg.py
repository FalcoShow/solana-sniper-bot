# utils_pkg/positions_pg.py
from __future__ import annotations

import os
import time
from typing import Any, Dict, List, Optional

from logging_setup import get_logger

# SQLAlchemy Core (sync) – safe to call from async code via threadpool if needed
from sqlalchemy import (
    BigInteger,
    Column,
    DateTime,
    Float,
    Integer,
    MetaData,
    String,
    Table,
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
# DSN / Engine
# ──────────────────────────────────────────────────────────────────────────────
def _pick_dsn() -> str:
    # Priority: explicit DSNs, then DATABASE_URL
    for k in ("PG_DSN_INTERNAL", "PG_DSN_EXTERNAL", "DATABASE_URL"):
        v = (os.getenv(k) or "").strip()
        if v:
            dsn = v
            break
    else:
        raise RuntimeError("No Postgres DSN found. Set PG_DSN_INTERNAL/EXTERNAL or DATABASE_URL.")

    # If async driver is provided, switch to sync (SQLAlchemy chooses default driver)
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
    Column("qty_raw", BigInteger, nullable=False),
    Column("decimals", Integer, nullable=False),
    Column("token_program", String, nullable=True),
    Column("avg_cost_sol", Float, nullable=False),         # SOL per token
    Column("opened_at", Float, nullable=False),            # epoch seconds
    Column("owner_ata", String, nullable=True),
    Column("symbol", String, nullable=True),
    Column("pool", String, nullable=True),
    Column("last_sig", String, nullable=True),
    Column("created_at", DateTime(timezone=True), server_default=func.now(), nullable=False),
    Column("updated_at", DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False),
)

sells = Table(
    "sells",
    _META,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("mint", String, nullable=False),
    Column("qty_raw", BigInteger, nullable=False),
    Column("sig", String, nullable=True),
    Column("created_at", DateTime(timezone=True), server_default=func.now(), nullable=False),
)

buys = Table(
    "buys",
    _META,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("mint", String, nullable=False),
    Column("qty_raw", BigInteger, nullable=False),
    Column("price_per_token_sol", Float, nullable=False),
    Column("sig", String, nullable=True),
    Column("created_at", DateTime(timezone=True), server_default=func.now(), nullable=False),
)

# ──────────────────────────────────────────────────────────────────────────────
# Engine init + schema ensure
# ──────────────────────────────────────────────────────────────────────────────
def _ensure_engine() -> Engine:
    global _ENGINE
    if _ENGINE is not None:
        return _ENGINE

    dsn = _pick_dsn()
    _ENGINE = create_engine(
        dsn,
        pool_size=int(os.getenv("PG_POOL_SIZE", "5") or 5),
        max_overflow=int(os.getenv("PG_POOL_MAX_OVERFLOW", "10") or 10),
        pool_pre_ping=True,
        future=True,
    )

    try:
        _META.create_all(_ENGINE)
    except SQLAlchemyError as e:
        raise RuntimeError(f"positions_pg: failed to create tables: {e}") from e

    # mask credentials in log
    masked = dsn.split("@")[-1]
    log.event("PG_POS_ENGINE_READY", dsn=masked)
    return _ENGINE


def _qty_ui(qty_raw: int, decimals: int) -> float:
    try:
        return float(qty_raw) / (10 ** int(decimals))
    except Exception:
        return 0.0


def _row_to_pos(row) -> Dict[str, Any]:
    d = dict(row._mapping)  # RowMapping -> dict
    d["qty_ui"] = _qty_ui(int(d.get("qty_raw", 0)), int(d.get("decimals", 0) or 0))
    return d

# ──────────────────────────────────────────────────────────────────────────────
# Public API (sync) — drop-in replacements for utils_pkg.positions
# ──────────────────────────────────────────────────────────────────────────────
def all() -> List[Dict[str, Any]]:
    eng = _ensure_engine()
    with eng.connect() as cx:
        res = cx.execute(select(positions))
        return [_row_to_pos(r) for r in res.fetchall()]


def get(mint: str) -> Optional[Dict[str, Any]]:
    eng = _ensure_engine()
    with eng.connect() as cx:
        res = cx.execute(select(positions).where(positions.c.mint == mint)).first()
        return _row_to_pos(res) if res else None


def remove(mint: str) -> None:
    eng = _ensure_engine()
    with eng.begin() as cx:
        cx.execute(sa_delete(positions).where(positions.c.mint == mint))


def decrement_qty(mint: str, sold_raw: int) -> None:
    if sold_raw <= 0:
        return
    eng = _ensure_engine()
    with eng.begin() as cx:
        res = cx.execute(select(positions.c.qty_raw).where(positions.c.mint == mint)).first()
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
                .values(qty_raw=newq, updated_at=func.now())
            )


def update_after_sell(*, mint: str, sold_raw: int, tx_sig: Optional[str], closed: bool) -> None:
    if sold_raw <= 0:
        return
    eng = _ensure_engine()
    with eng.begin() as cx:
        cx.execute(sa_insert(sells).values(mint=mint, qty_raw=int(sold_raw), sig=(tx_sig or None)))

        res = cx.execute(select(positions.c.qty_raw).where(positions.c.mint == mint)).first()
        if not res:
            return
        cur = int(res[0] or 0)
        newq = max(0, cur - int(sold_raw))
        if closed or newq <= 0:
            cx.execute(sa_delete(positions).where(positions.c.mint == mint))
        else:
            cx.execute(
                sa_update(positions)
                .where(positions.c.mint == mint)
                .values(qty_raw=newq, last_sig=(tx_sig or None), updated_at=func.now())
            )


def record_sell(*, mint: str, qty_raw: int, sig: Optional[str]) -> None:
    update_after_sell(mint=mint, sold_raw=int(qty_raw), tx_sig=sig, closed=False)


def record_buy(
    *,
    mint: str,
    qty_raw: int,
    price_per_token_sol: float,  # SOL per token
    decimals: int,
    token_program: Optional[str],
    owner_ata: str = "",
    symbol: Optional[str] = None,
    pool: Optional[str] = None,
    sig: Optional[str] = None,
) -> None:
    """
    Upsert a position with weighted-average entry price and increased qty.
    Also logs into buys table.
    """
    if qty_raw <= 0 or decimals is None:
        return

    eng = _ensure_engine()
    qty_raw = int(qty_raw)
    decimals = int(decimals)
    new_ui = _qty_ui(qty_raw, decimals)
    px = float(price_per_token_sol or 0.0)
    now = time.time()

    with eng.begin() as cx:
        cur = cx.execute(
            select(positions.c.qty_raw, positions.c.avg_cost_sol, positions.c.decimals)
            .where(positions.c.mint == mint)
        ).first()

        if cur:
            old_qty_raw = int(cur[0] or 0)
            old_ui = _qty_ui(old_qty_raw, decimals)  # trust latest decimals
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
