# utils_pkg/positions_pg.py
from __future__ import annotations

"""
utils_pkg/positions_pg.py

Postgres Positions Backend (P0 / production-grade / NO business logic)
=====================================================================

This module provides the Postgres implementation for the positions backend
interface defined in utils_pkg/positions_backend.py.

LOCK-IN RULES
-------------
- position_store.py (Redis-first) is the **canonical** runtime truth.
- This file is ONLY an *optional mirror* used for:
    - reporting / analytics
    - audits / recovery
    - health verification that persistence is working
- It MUST NOT become a second execution engine.

It implements:
    class PostgresPositionsBackend

and keeps a small set of DEPRECATED legacy helper functions for compatibility
with older call sites that still do:
    from utils_pkg.positions_pg import record_buy, update_after_sell, ...

Those legacy helpers are best-effort and are meant to be removed once
utils_pkg/trader.py + utils_pkg/ledger.py are rewritten to the new contracts.
"""

import asyncio
import os
import time
from typing import Any

# Safe logger import
try:  # pragma: no cover
    from logging_setup import get_logger  # type: ignore
except Exception:  # pragma: no cover
    import logging

    def get_logger(name: str | None = None):  # type: ignore
        return logging.getLogger(name or "positions_pg")


log = get_logger(__name__)

# SQLAlchemy Core (sync). We run DB ops in a thread via asyncio.to_thread.
from sqlalchemy import (
    BigInteger,
    Column,
    Float,
    Index,
    MetaData,
    String,
    Table,
    create_engine,
    delete as sa_delete,
    select,
    text as sa_text,
    update as sa_update,
)
from sqlalchemy.engine import Engine

try:
    from sqlalchemy.dialects.postgresql import (
        JSONB,  # type: ignore
        insert as pg_insert,  # type: ignore
    )
except Exception as e:  # pragma: no cover
    raise RuntimeError("positions_pg requires SQLAlchemy with postgres dialect support") from e


# -----------------------------------------------------------------------------
# Small helpers
# -----------------------------------------------------------------------------
def _truthy(x: Any) -> bool:
    return str(x or "").strip().lower() in {"1", "true", "yes", "y", "on"}


def _safe_int(x: Any, default: int = 0) -> int:
    try:
        return int(float(x))
    except Exception:
        return default


def _safe_float(x: Any, default: float = 0.0) -> float:
    try:
        v = float(x)
        if v != v:  # NaN
            return default
        return v
    except Exception:
        return default


def _now_ts() -> int:
    return int(time.time())


def _get_config_attr(name: str) -> Any:
    try:
        import config as C  # type: ignore

        return getattr(C, name)
    except Exception:
        return None


def _first_nonempty(*vals: Any) -> str:
    for v in vals:
        s = str(v or "").strip()
        if s:
            return s
    return ""


def _normalize_dsn(dsn: str) -> str:
    """
    Normalize DSN for SQLAlchemy sync engine.
    - postgres:// -> postgresql://
    - remove +asyncpg
    """
    dsn = (dsn or "").strip()
    if not dsn:
        return ""
    if dsn.startswith("postgres://"):
        dsn = "postgresql://" + dsn[len("postgres://") :]
    # driver normalization
    dsn = dsn.replace("+asyncpg", "")
    return dsn


def _pick_dsn() -> str:
    """
    DSN precedence (best effort):
      config.POSTGRES_DSN / PG_DSN / PG_DSN_INTERNAL / PG_DSN_EXTERNAL / DATABASE_URL
      env   POSTGRES_DSN / PG_DSN / PG_DSN_INTERNAL / PG_DSN_EXTERNAL / DATABASE_URL
    """
    cfg = _get_config_attr
    dsn = _first_nonempty(
        cfg("POSTGRES_DSN"),
        cfg("PG_DSN"),
        cfg("PG_DSN_INTERNAL"),
        cfg("PG_DSN_EXTERNAL"),
        cfg("DATABASE_URL"),
        os.getenv("POSTGRES_DSN"),
        os.getenv("PG_DSN"),
        os.getenv("PG_DSN_INTERNAL"),
        os.getenv("PG_DSN_EXTERNAL"),
        os.getenv("DATABASE_URL"),
    )
    return _normalize_dsn(dsn)


def _default_wallet() -> str:
    """
    Legacy helpers sometimes don’t pass wallet; fall back to env/config.
    """
    cfg = _get_config_attr
    return _first_nonempty(
        cfg("WALLET_ADDRESS"),
        cfg("WALLET_PUBKEY"),
        cfg("PUBLIC_KEY"),
        os.getenv("WALLET_ADDRESS"),
        os.getenv("WALLET_PUBKEY"),
        os.getenv("PUBLIC_KEY"),
        "unknown_wallet",
    )


def _log_event(name: str, **fields: Any) -> None:
    """
    Support both custom logger.event(...) and standard logger.info(...)
    """
    try:
        if hasattr(log, "event") and callable(log.event):
            log.event(name, **fields)  # type: ignore
        else:
            log.info(name, extra=fields)
    except Exception:
        # never crash from logging
        return


# -----------------------------------------------------------------------------
# Schema (mirror table; backend-agnostic shape)
# -----------------------------------------------------------------------------
_META = MetaData()

positions_mirror = Table(
    "positions_mirror",
    _META,
    Column("wallet", String(96), primary_key=True),
    Column("mint", String(96), primary_key=True),
    Column("pool", String(128), primary_key=True),
    Column("symbol", String(64), nullable=True),
    Column("status", String(16), nullable=False, default="open"),  # open|closed|failed
    Column("size_raw", BigInteger, nullable=True),  # raw token qty (legacy compat)
    Column("size_sol", Float, nullable=True),  # SOL spent/received if known
    Column("entry_price", Float, nullable=True),  # price (unit defined by caller)
    Column("opened_ts", BigInteger, nullable=True),
    Column("closed_ts", BigInteger, nullable=True),
    Column("updated_ts", BigInteger, nullable=False),
    Column("extra", JSONB, nullable=True),
)

Index("idx_posm_wallet_status", positions_mirror.c.wallet, positions_mirror.c.status)
Index("idx_posm_mint", positions_mirror.c.mint)
Index("idx_posm_updated_ts", positions_mirror.c.updated_ts)


# -----------------------------------------------------------------------------
# Engine (lazy singleton)
# -----------------------------------------------------------------------------
_ENGINE: Engine | None = None
_ENGINE_LOCK = asyncio.Lock()


def _engine_settings() -> dict[str, Any]:
    """
    Pool + timeout knobs (safe defaults for Render).
    """
    pool_size = _safe_int(os.getenv("PG_POOL_SIZE", "3"), 3)
    max_overflow = _safe_int(os.getenv("PG_POOL_MAX_OVERFLOW", "6"), 6)
    pool_timeout = _safe_int(os.getenv("PG_POOL_TIMEOUT_S", "10"), 10)
    pool_recycle = _safe_int(os.getenv("PG_POOL_RECYCLE_S", "180"), 180)
    stmt_timeout_ms = _safe_int(os.getenv("PG_STATEMENT_TIMEOUT_MS", "5000"), 5000)

    connect_args: dict[str, Any] = {}
    # libpq statement_timeout option
    if stmt_timeout_ms > 0:
        connect_args["options"] = f"-c statement_timeout={stmt_timeout_ms}"

    return dict(
        pool_size=max(1, pool_size),
        max_overflow=max(0, max_overflow),
        pool_timeout=max(1, pool_timeout),
        pool_recycle=max(30, pool_recycle),
        connect_args=connect_args,
    )


def _ensure_engine_sync() -> Engine:
    """
    Sync engine initializer. Raises on fatal DSN errors.
    (Called inside a thread via asyncio.to_thread)
    """
    global _ENGINE
    if _ENGINE is not None:
        return _ENGINE

    dsn = _pick_dsn()
    if not dsn:
        raise RuntimeError(
            "positions_pg: No Postgres DSN found. "
            "Set POSTGRES_DSN (or PG_DSN/PG_DSN_INTERNAL/PG_DSN_EXTERNAL/DATABASE_URL)."
        )

    s = _engine_settings()
    _ENGINE = create_engine(
        dsn,
        future=True,
        pool_pre_ping=True,
        pool_size=s["pool_size"],
        max_overflow=s["max_overflow"],
        pool_timeout=s["pool_timeout"],
        pool_recycle=s["pool_recycle"],
        connect_args=s["connect_args"],
    )

    # Ensure schema
    try:
        _META.create_all(_ENGINE)
    except Exception as e:
        raise RuntimeError(f"positions_pg: schema create_all failed: {e}") from e

    masked = dsn.split("@")[-1] if "@" in dsn else dsn
    _log_event(
        "PG_POSITIONS_ENGINE_READY",
        dsn=masked,
        pool_size=s["pool_size"],
        max_overflow=s["max_overflow"],
    )
    return _ENGINE


async def _ensure_engine() -> Engine:
    """
    Async wrapper: create engine once safely.
    """
    global _ENGINE
    if _ENGINE is not None:
        return _ENGINE
    async with _ENGINE_LOCK:
        if _ENGINE is not None:
            return _ENGINE
        return await asyncio.to_thread(_ensure_engine_sync)


# -----------------------------------------------------------------------------
# Backend implementation
# -----------------------------------------------------------------------------
class PostgresPositionsBackend:
    """
    Postgres implementation of utils_pkg.positions_backend.PositionsBackend.

    Important:
    - All methods are best-effort and should not crash the bot.
    - DB I/O runs in a worker thread (SQLAlchemy sync engine).
    """

    name = "pg"

    async def healthcheck(self) -> bool:
        try:
            eng = await _ensure_engine()

            def _ping() -> bool:
                with eng.connect() as cx:
                    cx.execute(sa_text("SELECT 1"))
                return True

            return await asyncio.to_thread(_ping)
        except Exception as e:
            log.warning("PG_POSITIONS_HEALTHCHECK_FAIL", extra={"err": str(e)[:200]})
            return False

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
    ) -> None:
        try:
            eng = await _ensure_engine()
            w = str(wallet or "").strip()[:96] or "unknown_wallet"
            m = str(mint or "").strip()[:96]
            p = str(pool or "").strip()[:128] or "unknown_pool"
            if not m:
                return

            now = int(updated_ts or _now_ts())
            opened = int(opened_ts or 0) or None

            # size_raw is typed as float in the interface, but we store as BIGINT when integer-like.
            sr: int | None = None
            if size_raw is not None:
                try:
                    sr = int(float(size_raw))
                except Exception:
                    sr = None

            def _do() -> None:
                ins = pg_insert(positions_mirror).values(
                    wallet=w,
                    mint=m,
                    pool=p,
                    symbol=(str(symbol).strip()[:64] if symbol else None),
                    status=str(status or "open")[:16],
                    size_raw=sr,
                    size_sol=(float(size_sol) if size_sol is not None else None),
                    entry_price=(float(entry_price) if entry_price is not None else None),
                    opened_ts=opened,
                    closed_ts=None,
                    updated_ts=now,
                    extra=(extra if isinstance(extra, dict) else None),
                )

                # Only overwrite optional cols if provided; otherwise keep existing.
                upd = {
                    "symbol": ins.excluded.symbol.coalesce(positions_mirror.c.symbol),
                    "status": ins.excluded.status,
                    "size_raw": ins.excluded.size_raw.coalesce(positions_mirror.c.size_raw),
                    "size_sol": ins.excluded.size_sol.coalesce(positions_mirror.c.size_sol),
                    "entry_price": ins.excluded.entry_price.coalesce(
                        positions_mirror.c.entry_price
                    ),
                    "opened_ts": ins.excluded.opened_ts.coalesce(positions_mirror.c.opened_ts),
                    "updated_ts": ins.excluded.updated_ts,
                    "extra": ins.excluded.extra.coalesce(positions_mirror.c.extra),
                    # do not touch closed_ts on upsert; close_position owns it
                }

                stmt = ins.on_conflict_do_update(
                    index_elements=[
                        positions_mirror.c.wallet,
                        positions_mirror.c.mint,
                        positions_mirror.c.pool,
                    ],
                    set_=upd,
                )

                with eng.begin() as cx:
                    cx.execute(stmt)

            await asyncio.to_thread(_do)
        except Exception as e:
            log.warning(
                "PG_POS_UPSERT_FAIL",
                extra={"err": str(e)[:220], "wallet": str(wallet)[:48], "mint": str(mint)[:48]},
            )
            return

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
    ) -> None:
        try:
            eng = await _ensure_engine()
            w = str(wallet or "").strip()[:96] or "unknown_wallet"
            m = str(mint or "").strip()[:96]
            p = str(pool or "").strip()[:128] or "unknown_pool"
            if not m:
                return

            now = int(updated_ts or _now_ts())
            cts = int(closed_ts or _now_ts())

            def _do() -> None:
                with eng.begin() as cx:
                    # merge extra best-effort (read current, merge in python)
                    cur_extra = None
                    if extra:
                        row = cx.execute(
                            select(positions_mirror.c.extra).where(
                                (positions_mirror.c.wallet == w)
                                & (positions_mirror.c.mint == m)
                                & (positions_mirror.c.pool == p)
                            )
                        ).first()
                        cur_extra = (row[0] if row else None) or {}
                        if not isinstance(cur_extra, dict):
                            cur_extra = {}
                        merged = dict(cur_extra)
                        merged.update(extra)
                    else:
                        merged = None

                    cx.execute(
                        sa_update(positions_mirror)
                        .where(
                            (positions_mirror.c.wallet == w)
                            & (positions_mirror.c.mint == m)
                            & (positions_mirror.c.pool == p)
                        )
                        .values(
                            status=str(status or "closed")[:16],
                            closed_ts=cts,
                            updated_ts=now,
                            extra=(merged if merged is not None else positions_mirror.c.extra),
                        )
                    )

            await asyncio.to_thread(_do)
        except Exception as e:
            log.warning(
                "PG_POS_CLOSE_FAIL",
                extra={"err": str(e)[:220], "wallet": str(wallet)[:48], "mint": str(mint)[:48]},
            )
            return

    async def fetch_position(self, *, wallet: str, mint: str, pool: str):
        from .positions_backend import PositionRow  # local import to avoid cycles at import-time

        try:
            eng = await _ensure_engine()
            w = str(wallet or "").strip()[:96] or "unknown_wallet"
            m = str(mint or "").strip()[:96]
            p = str(pool or "").strip()[:128] or "unknown_pool"
            if not m:
                return None

            def _do() -> PositionRow | None:
                with eng.connect() as cx:
                    row = cx.execute(
                        select(positions_mirror).where(
                            (positions_mirror.c.wallet == w)
                            & (positions_mirror.c.mint == m)
                            & (positions_mirror.c.pool == p)
                        )
                    ).first()
                    if not row:
                        return None
                    d = dict(row._mapping)
                    return PositionRow(
                        wallet=d.get("wallet") or w,
                        mint=d.get("mint") or m,
                        pool=d.get("pool") or p,
                        symbol=d.get("symbol"),
                        status=d.get("status"),
                        size_raw=float(d["size_raw"]) if d.get("size_raw") is not None else None,
                        size_sol=float(d["size_sol"]) if d.get("size_sol") is not None else None,
                        entry_price=float(d["entry_price"])
                        if d.get("entry_price") is not None
                        else None,
                        opened_ts=int(d["opened_ts"]) if d.get("opened_ts") is not None else None,
                        closed_ts=int(d["closed_ts"]) if d.get("closed_ts") is not None else None,
                        updated_ts=int(d["updated_ts"])
                        if d.get("updated_ts") is not None
                        else None,
                        extra=d.get("extra") if isinstance(d.get("extra"), dict) else None,
                    )

            return await asyncio.to_thread(_do)
        except Exception as e:
            log.warning("PG_POS_FETCH_FAIL", extra={"err": str(e)[:200]})
            return None

    async def fetch_open_positions(self, *, wallet: str | None = None, limit: int = 500):
        from .positions_backend import PositionRow  # local import

        try:
            eng = await _ensure_engine()
            lim = max(1, min(int(limit or 500), 5000))

            w = str(wallet or "").strip()[:96] if wallet else None

            def _do() -> list[PositionRow]:
                with eng.connect() as cx:
                    q = select(positions_mirror).where(positions_mirror.c.status == "open")
                    if w:
                        q = q.where(positions_mirror.c.wallet == w)
                    q = q.order_by(positions_mirror.c.updated_ts.desc()).limit(lim)
                    res = cx.execute(q).fetchall()

                out: list[PositionRow] = []
                for row in res:
                    d = dict(row._mapping)
                    out.append(
                        PositionRow(
                            wallet=d.get("wallet") or (w or "unknown_wallet"),
                            mint=d.get("mint") or "",
                            pool=d.get("pool") or "",
                            symbol=d.get("symbol"),
                            status=d.get("status"),
                            size_raw=float(d["size_raw"])
                            if d.get("size_raw") is not None
                            else None,
                            size_sol=float(d["size_sol"])
                            if d.get("size_sol") is not None
                            else None,
                            entry_price=float(d["entry_price"])
                            if d.get("entry_price") is not None
                            else None,
                            opened_ts=int(d["opened_ts"])
                            if d.get("opened_ts") is not None
                            else None,
                            closed_ts=int(d["closed_ts"])
                            if d.get("closed_ts") is not None
                            else None,
                            updated_ts=int(d["updated_ts"])
                            if d.get("updated_ts") is not None
                            else None,
                            extra=d.get("extra") if isinstance(d.get("extra"), dict) else None,
                        )
                    )
                return out

            return await asyncio.to_thread(_do)
        except Exception as e:
            log.warning("PG_POS_FETCH_OPEN_FAIL", extra={"err": str(e)[:200]})
            return []


# -----------------------------------------------------------------------------
# DEPRECATED legacy helpers (compat only; remove after trader/ledger rewrite)
# -----------------------------------------------------------------------------
# These helpers map older "mint-only" calls onto positions_mirror using a default wallet.
# They are intentionally minimal and best-effort.
_PG_BACKEND_SINGLETON: PostgresPositionsBackend | None = None


def _backend() -> PostgresPositionsBackend:
    global _PG_BACKEND_SINGLETON
    if _PG_BACKEND_SINGLETON is None:
        _PG_BACKEND_SINGLETON = PostgresPositionsBackend()
    return _PG_BACKEND_SINGLETON


def _run(coro):
    """
    Run a coroutine from sync legacy calls.
    If already in an event loop, schedule + wait is unsafe; so we no-op safely.
    """
    try:
        loop = asyncio.get_running_loop()
        # If we are already inside asyncio (bot runtime), legacy sync calls should not be used.
        # Best-effort: schedule and return immediately (don’t block the loop).
        loop.create_task(coro)  # type: ignore
        return None
    except RuntimeError:
        # no running loop -> ok to run
        return asyncio.run(coro)


def all() -> list[dict[str, Any]]:
    """
    Legacy: return all open mirrored rows (across wallets). Best-effort.
    """
    out: list[dict[str, Any]] = []

    async def _c():
        rows = await _backend().fetch_open_positions(wallet=None, limit=2000)
        return [r.__dict__ for r in rows]  # PositionRow is dataclass-like

    res = _run(_c())
    if isinstance(res, list):
        out = res
    return out


def get(mint: str) -> dict[str, Any] | None:
    """
    Legacy: return the newest open row for this mint (prefers default wallet).
    """
    m = str(mint or "").strip()
    if not m:
        return None

    async def _c():
        w = _default_wallet()
        # try default wallet first
        rows = await _backend().fetch_open_positions(wallet=w, limit=500)
        for r in rows:
            if r.mint == m:
                return r.__dict__
        # fallback: any wallet
        rows2 = await _backend().fetch_open_positions(wallet=None, limit=2000)
        for r in rows2:
            if r.mint == m:
                return r.__dict__
        return None

    res = _run(_c())
    return res if isinstance(res, dict) else None


def remove(mint: str) -> None:
    """
    Legacy: hard-delete ALL mirror rows for this mint (admin only).
    """
    m = str(mint or "").strip()
    if not m:
        return

    async def _c():
        eng = await _ensure_engine()

        def _do():
            with eng.begin() as cx:
                cx.execute(sa_delete(positions_mirror).where(positions_mirror.c.mint == m))

        await asyncio.to_thread(_do)

    _run(_c())


def decrement_qty(mint: str, sold_raw: int) -> None:
    """
    Legacy: decrement size_raw for default wallet rows of mint.
    If size_raw <= 0, close the row.
    """
    m = str(mint or "").strip()
    sr = _safe_int(sold_raw, 0)
    if not m or sr <= 0:
        return

    async def _c():
        eng = await _ensure_engine()
        w = _default_wallet()

        def _do():
            with eng.begin() as cx:
                rows = cx.execute(
                    select(positions_mirror).where(
                        (positions_mirror.c.wallet == w)
                        & (positions_mirror.c.mint == m)
                        & (positions_mirror.c.status == "open")
                    )
                ).fetchall()

                for row in rows:
                    d = dict(row._mapping)
                    key_pool = d.get("pool") or "unknown_pool"
                    cur = int(d.get("size_raw") or 0)
                    newq = max(0, cur - sr)
                    if newq <= 0:
                        cx.execute(
                            sa_update(positions_mirror)
                            .where(
                                (positions_mirror.c.wallet == w)
                                & (positions_mirror.c.mint == m)
                                & (positions_mirror.c.pool == key_pool)
                            )
                            .values(
                                status="closed",
                                closed_ts=_now_ts(),
                                updated_ts=_now_ts(),
                                size_raw=0,
                            )
                        )
                    else:
                        cx.execute(
                            sa_update(positions_mirror)
                            .where(
                                (positions_mirror.c.wallet == w)
                                & (positions_mirror.c.mint == m)
                                & (positions_mirror.c.pool == key_pool)
                            )
                            .values(size_raw=newq, updated_ts=_now_ts())
                        )

        await asyncio.to_thread(_do)

    _run(_c())


def update_after_sell(*, mint: str, sold_raw: int, tx_sig: str | None, closed: bool) -> None:
    """
    Legacy: decrement size_raw and optionally force-close.
    """
    m = str(mint or "").strip()
    sr = _safe_int(sold_raw, 0)
    if not m or sr <= 0:
        return

    async def _c():
        eng = await _ensure_engine()
        w = _default_wallet()

        def _do():
            with eng.begin() as cx:
                rows = cx.execute(
                    select(positions_mirror).where(
                        (positions_mirror.c.wallet == w)
                        & (positions_mirror.c.mint == m)
                        & (positions_mirror.c.status == "open")
                    )
                ).fetchall()

                for row in rows:
                    d = dict(row._mapping)
                    key_pool = d.get("pool") or "unknown_pool"
                    cur = int(d.get("size_raw") or 0)
                    newq = max(0, cur - sr)
                    force = bool(closed) or (newq <= 0)
                    cx.execute(
                        sa_update(positions_mirror)
                        .where(
                            (positions_mirror.c.wallet == w)
                            & (positions_mirror.c.mint == m)
                            & (positions_mirror.c.pool == key_pool)
                        )
                        .values(
                            size_raw=(0 if force else newq),
                            status=("closed" if force else "open"),
                            closed_ts=(_now_ts() if force else positions_mirror.c.closed_ts),
                            updated_ts=_now_ts(),
                            extra=positions_mirror.c.extra,  # do not mutate here
                        )
                    )

        await asyncio.to_thread(_do)

    _run(_c())


def record_sell(*, mint: str, qty_raw: int, sig: str | None) -> None:
    """
    Legacy: a sell record is just a decrement.
    """
    update_after_sell(mint=mint, sold_raw=int(qty_raw or 0), tx_sig=sig, closed=False)


def record_buy(
    *,
    mint: str,
    qty_raw: int,
    price_per_token_sol: float,
    decimals: int,
    token_program: str | None,
    owner_ata: str = "",
    symbol: str | None = None,
    pool: str | None = None,
    sig: str | None = None,
) -> None:
    """
    Legacy: best-effort map onto positions_mirror (default wallet).

    We store:
      - size_raw := qty_raw
      - entry_price := price_per_token_sol (unit: sol_per_token) in extra
      - extra includes decimals/token_program/owner_ata/last_sig
    """
    m = str(mint or "").strip()
    q = _safe_int(qty_raw, 0)
    if not m or q <= 0:
        return

    async def _c():
        w = _default_wallet()
        p = (pool or "legacy")[:128]
        extra = {
            "price_unit": "sol_per_token",
            "decimals": int(decimals or 0),
            "token_program": token_program,
            "owner_ata": owner_ata or None,
            "last_sig": sig or None,
        }
        await _backend().upsert_position(
            wallet=w,
            mint=m,
            pool=p,
            symbol=symbol,
            size_raw=float(q),
            size_sol=None,
            entry_price=float(price_per_token_sol or 0.0),
            status="open",
            opened_ts=_now_ts(),
            updated_ts=_now_ts(),
            extra=extra,
        )

    _run(_c())


__all__ = [
    # Backend
    "PostgresPositionsBackend",
    # Legacy compat
    "all",
    "decrement_qty",
    "get",
    "record_buy",
    "record_sell",
    "remove",
    "update_after_sell",
]
