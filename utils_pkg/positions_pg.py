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
- utils_pkg.position_store (Redis-first) is the **canonical** runtime truth.
- This file is ONLY an *optional mirror* used for:
    - reporting / analytics
    - audits / recovery
    - health verification that persistence is working
- It MUST NOT become a second execution engine.

Design goals
------------
- Import-time safety: importing this module must not touch the network or DB.
- Best-effort: failure to reach Postgres must never crash the bot.
- Render-friendly pool defaults + statement timeouts.
- Minimal dependencies at import-time (SQLAlchemy is imported lazily).

Implements:
    class PostgresPositionsBackend

Also keeps a small set of DEPRECATED legacy helper functions for compatibility
with older call sites that still do:
    from utils_pkg.positions_pg import record_buy, update_after_sell, ...

Those legacy helpers are best-effort and are meant to be removed once
trader/ledger move to the new contracts.

Phase A Note
------------
This file hardens the Logger ↔ EventLogger typing contract by:
- Using a small adapter that guarantees .debug/.info/.warning/.error/.exception
  and provides .event/.every without mutating the underlying logger object.
- Avoiding mypy-incompatible Callable assignments and avoiding mid-file imports.
"""

import asyncio
import contextlib
import importlib
import logging
import os
import time
from functools import lru_cache
from typing import Any, Callable, Coroutine, Protocol, cast

JsonDict = dict[str, Any]

# ──────────────────────────────────────────────────────────────────────────────
# Logging (safe, mypy-stable)
# ──────────────────────────────────────────────────────────────────────────────


class LoggerLike(Protocol):
    def debug(self, msg: str, *args: Any, **kwargs: Any) -> None: ...
    def info(self, msg: str, *args: Any, **kwargs: Any) -> None: ...
    def warning(self, msg: str, *args: Any, **kwargs: Any) -> None: ...
    def error(self, msg: str, *args: Any, **kwargs: Any) -> None: ...
    def exception(self, msg: str, *args: Any, **kwargs: Any) -> None: ...

    # structured helpers (guaranteed by adapter)
    def event(self, name: str, **kw: Any) -> None: ...
    def every(self, key: str, n: int = 10, msg: str | None = None, **kw: Any) -> None: ...


class _LogAdapter:
    """
    Adapter that normalizes Logger/EventLogger differences without mutating the base object.
    - All standard logging methods exist, and tolerate signature differences.
    - event/every always exist.
    """

    __slots__ = ("_base", "_every_store", "_fallback")

    def __init__(self, base: Any) -> None:
        self._base = base
        self._every_store: dict[str, float] = {}
        self._fallback = logging.getLogger("positions_pg")

    def _call(self, method: str, msg: str, *args: Any, **kwargs: Any) -> None:
        fn = getattr(self._base, method, None)
        if callable(fn):
            # Some custom loggers don't accept kwargs like `extra=...`.
            with contextlib.suppress(Exception):
                try:
                    fn(msg, *args, **kwargs)
                    return
                except TypeError:
                    fn(msg, *args)
                    return

        # Fallback to std logging
        fb = getattr(self._fallback, method, None)
        if callable(fb):
            with contextlib.suppress(Exception):
                fb(msg, *args, **kwargs)

    def debug(self, msg: str, *args: Any, **kwargs: Any) -> None:
        self._call("debug", msg, *args, **kwargs)

    def info(self, msg: str, *args: Any, **kwargs: Any) -> None:
        self._call("info", msg, *args, **kwargs)

    def warning(self, msg: str, *args: Any, **kwargs: Any) -> None:
        self._call("warning", msg, *args, **kwargs)

    def error(self, msg: str, *args: Any, **kwargs: Any) -> None:
        self._call("error", msg, *args, **kwargs)

    def exception(self, msg: str, *args: Any, **kwargs: Any) -> None:
        self._call("exception", msg, *args, **kwargs)

    def event(self, name: str, **kw: Any) -> None:
        fn = getattr(self._base, "event", None)
        if callable(fn):
            with contextlib.suppress(Exception):
                fn(name, **kw)
                return
        # Fallback: log as a single structured line
        try:
            self.info("%s %s", name, kw)
        except Exception:
            return

    def every(self, key: str, n: int = 10, msg: str | None = None, **kw: Any) -> None:
        fn = getattr(self._base, "every", None)
        if callable(fn):
            with contextlib.suppress(Exception):
                fn(key, n=n, msg=msg, **kw)
                return

        # Fallback: at most once per n seconds per key
        try:
            now = time.monotonic()
            last = float(self._every_store.get(key, 0.0))
            interval = float(max(1, int(n)))
            if (now - last) >= interval:
                self._every_store[key] = now
                self.event(msg or key, **kw)
        except Exception:
            return


def _fallback_get_logger(name: str | None = None, *_a: Any, **_k: Any) -> logging.Logger:
    return logging.getLogger(name or "positions_pg")


def _resolve_get_logger() -> Callable[..., Any]:
    """
    Resolve get_logger without mid-file imports and without strict signature assumptions.
    Precedence:
      - utils_pkg.log_compat.get_logger
      - logging_setup.get_logger
      - fallback std logging
    """
    # utils_pkg.log_compat
    with contextlib.suppress(Exception):
        mod = importlib.import_module("utils_pkg.log_compat")
        fn = getattr(mod, "get_logger", None)
        if callable(fn):
            return cast(Callable[..., Any], fn)

    # logging_setup
    with contextlib.suppress(Exception):
        mod2 = importlib.import_module("logging_setup")
        fn2 = getattr(mod2, "get_logger", None)
        if callable(fn2):
            return cast(Callable[..., Any], fn2)

    return _fallback_get_logger


_GET_LOGGER = _resolve_get_logger()


def _get_log(name: str) -> LoggerLike:
    base: Any = None
    with contextlib.suppress(Exception):
        base = _GET_LOGGER(name)
    if base is None:
        base = _fallback_get_logger(name)
    return cast(LoggerLike, _LogAdapter(base))


log = _get_log(__name__)


def _log_event(name: str, **fields: Any) -> None:
    """
    Structured event helper. Always safe.
    """
    try:
        log.event(name, **fields)
    except Exception:
        return


# ──────────────────────────────────────────────────────────────────────────────
# Small helpers
# ──────────────────────────────────────────────────────────────────────────────
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
    # Best-effort; avoid hard import-time coupling.
    with contextlib.suppress(Exception):
        import config as C  # type: ignore

        return getattr(C, name)
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
    - remove +asyncpg (we use sync engine in a thread)
    """
    dsn = (dsn or "").strip()
    if not dsn:
        return ""
    if dsn.startswith("postgres://"):
        dsn = "postgresql://" + dsn[len("postgres://") :]
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


def _mask_dsn(dsn: str) -> str:
    """
    Best-effort DSN masking for logs.
    """
    s = (dsn or "").strip()
    if not s:
        return ""
    # Prefer SQLAlchemy URL parser if available; otherwise basic masking.
    with contextlib.suppress(Exception):
        from sqlalchemy.engine.url import make_url  # type: ignore

        u = make_url(s)
        if getattr(u, "password", None):
            u = u.set(password="***")  # type: ignore[attr-defined]
        return str(u)
    if "@" in s:
        return "***@" + s.split("@", 1)[1]
    return s


# ──────────────────────────────────────────────────────────────────────────────
# Lazy SQLAlchemy import + schema build (keeps module import safe)
# ──────────────────────────────────────────────────────────────────────────────
@lru_cache(maxsize=1)
def _sa() -> dict[str, Any]:
    """
    Import SQLAlchemy lazily.
    Returns a dict of the few symbols we use, so callers don't import at module scope.
    """
    import sqlalchemy as sa  # type: ignore
    from sqlalchemy import (  # type: ignore
        BigInteger,
        Column,
        Float,
        Index,
        MetaData,
        String,
        Table,
        create_engine,
        delete as sa_delete,
        func,
        insert as sa_insert,
        select,
        text as sa_text,
        update as sa_update,
    )

    try:
        from sqlalchemy.dialects.postgresql import JSONB, insert as pg_insert  # type: ignore
    except Exception:
        JSONB = sa.JSON  # type: ignore[attr-defined]
        pg_insert = None

    return {
        "sa": sa,
        "BigInteger": BigInteger,
        "Column": Column,
        "Float": Float,
        "Index": Index,
        "MetaData": MetaData,
        "String": String,
        "Table": Table,
        "create_engine": create_engine,
        "sa_delete": sa_delete,
        "sa_insert": sa_insert,
        "sa_text": sa_text,
        "sa_update": sa_update,
        "select": select,
        "func": func,
        "JSONB": JSONB,
        "pg_insert": pg_insert,
    }


_SCHEMA_READY = False
_META: Any = None
positions_mirror: Any = None  # intentionally Any to avoid stub friction


def _ensure_schema_sync() -> tuple[Any, Any]:
    """
    Define metadata + table lazily (first engine creation).
    """
    global _SCHEMA_READY, _META, positions_mirror
    if _SCHEMA_READY and _META is not None and positions_mirror is not None:
        return _META, positions_mirror

    s = _sa()
    MetaData = s["MetaData"]
    Table = s["Table"]
    Column = s["Column"]
    String = s["String"]
    Float = s["Float"]
    BigInteger = s["BigInteger"]
    Index = s["Index"]
    JSONB = s["JSONB"]

    _META = MetaData()
    positions_mirror = Table(
        "positions_mirror",
        _META,
        Column("wallet", String(96), primary_key=True),
        Column("mint", String(96), primary_key=True),
        Column("pool", String(128), primary_key=True),
        Column("symbol", String(64), nullable=True),
        Column("status", String(16), nullable=False, default="open"),  # open|closed|failed
        Column("size_raw", BigInteger, nullable=True),
        Column("size_sol", Float, nullable=True),
        Column("entry_price", Float, nullable=True),
        Column("opened_ts", BigInteger, nullable=True),
        Column("closed_ts", BigInteger, nullable=True),
        Column("updated_ts", BigInteger, nullable=False),
        Column("extra", JSONB, nullable=True),
    )

    Index("idx_posm_wallet_status", positions_mirror.c.wallet, positions_mirror.c.status)
    Index("idx_posm_mint", positions_mirror.c.mint)
    Index("idx_posm_updated_ts", positions_mirror.c.updated_ts)

    _SCHEMA_READY = True
    return _META, positions_mirror


# ──────────────────────────────────────────────────────────────────────────────
# Engine (lazy singleton)
# ──────────────────────────────────────────────────────────────────────────────
_ENGINE: Any | None = None
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
    if stmt_timeout_ms > 0:
        # libpq statement_timeout option
        connect_args["options"] = f"-c statement_timeout={stmt_timeout_ms}"

    return {
        "pool_size": max(1, pool_size),
        "max_overflow": max(0, max_overflow),
        "pool_timeout": max(1, pool_timeout),
        "pool_recycle": max(30, pool_recycle),
        "connect_args": connect_args,
    }


def _ensure_engine_sync() -> Any:
    """
    Sync engine initializer. Called inside a worker thread.
    Raises only inside this function; callers wrap safely.
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

    s = _sa()
    create_engine = s["create_engine"]

    meta, _tbl = _ensure_schema_sync()
    settings = _engine_settings()

    eng = create_engine(
        dsn,
        pool_pre_ping=True,
        pool_size=settings["pool_size"],
        max_overflow=settings["max_overflow"],
        pool_timeout=settings["pool_timeout"],
        pool_recycle=settings["pool_recycle"],
        connect_args=settings["connect_args"],
    )

    try:
        meta.create_all(eng)
    except Exception as e:
        raise RuntimeError(f"positions_pg: schema create_all failed: {e}") from e

    _ENGINE = eng
    _log_event(
        "PG_POSITIONS_ENGINE_READY",
        dsn=_mask_dsn(dsn),
        pool_size=settings["pool_size"],
        max_overflow=settings["max_overflow"],
    )
    return _ENGINE


async def _ensure_engine() -> Any | None:
    """
    Async wrapper: create engine once safely.
    Returns None if engine cannot be created.
    """
    global _ENGINE

    eng = _ENGINE
    if eng is not None:
        return eng

    async with _ENGINE_LOCK:
        eng = _ENGINE
        if eng is not None:
            return eng

        try:
            eng = await asyncio.to_thread(_ensure_engine_sync)
        except Exception as err:
            # No hard crash: mirror is optional
            log.warning("PG_POSITIONS_ENGINE_INIT_FAIL: %s", str(err)[:220])
            _log_event("PG_POSITIONS_ENGINE_INIT_FAIL", err=str(err)[:500])
            return None

        _ENGINE = eng
        return eng


# ──────────────────────────────────────────────────────────────────────────────
# Backend implementation
# ──────────────────────────────────────────────────────────────────────────────
class PostgresPositionsBackend:
    """
    Postgres implementation of utils_pkg.positions_backend.PositionsBackend.

    Important:
    - All methods are best-effort and should not crash the bot.
    - DB I/O runs in a worker thread (SQLAlchemy sync engine).
    """

    name = "pg"

    async def healthcheck(self) -> bool:
        eng = await _ensure_engine()
        if eng is None:
            return False

        s = _sa()
        sa_text = s["sa_text"]

        def _ping() -> bool:
            with eng.connect() as cx:
                cx.execute(sa_text("SELECT 1"))
            return True

        try:
            return await asyncio.to_thread(_ping)
        except Exception as e:
            log.warning("PG_POSITIONS_HEALTHCHECK_FAIL: %s", str(e)[:200])
            _log_event("PG_POSITIONS_HEALTHCHECK_FAIL", err=str(e)[:500])
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
        eng = await _ensure_engine()
        if eng is None:
            return

        _meta, tbl = _ensure_schema_sync()
        s = _sa()
        func = s["func"]
        pg_insert = s["pg_insert"]
        sa_insert = s["sa_insert"]
        sa_update = s["sa_update"]
        select = s["select"]

        w = str(wallet or "").strip()[:96] or "unknown_wallet"
        m = str(mint or "").strip()[:96]
        p = str(pool or "").strip()[:128] or "unknown_pool"
        if not m:
            return

        now = int(updated_ts or _now_ts())
        opened = int(opened_ts or 0) or None

        sr: int | None = None
        if size_raw is not None:
            with contextlib.suppress(Exception):
                sr = int(float(size_raw))

        sym = str(symbol).strip()[:64] if symbol else None
        st = str(status or "open")[:16]
        ex = extra if isinstance(extra, dict) else None

        def _do() -> None:
            # If dialect insert is available, use true upsert.
            if pg_insert is not None:
                ins = pg_insert(tbl).values(
                    wallet=w,
                    mint=m,
                    pool=p,
                    symbol=sym,
                    status=st,
                    size_raw=sr,
                    size_sol=(float(size_sol) if size_sol is not None else None),
                    entry_price=(float(entry_price) if entry_price is not None else None),
                    opened_ts=opened,
                    closed_ts=None,
                    updated_ts=now,
                    extra=ex,
                )

                upd = {
                    "symbol": func.coalesce(ins.excluded.symbol, tbl.c.symbol),
                    "status": ins.excluded.status,
                    "size_raw": func.coalesce(ins.excluded.size_raw, tbl.c.size_raw),
                    "size_sol": func.coalesce(ins.excluded.size_sol, tbl.c.size_sol),
                    "entry_price": func.coalesce(ins.excluded.entry_price, tbl.c.entry_price),
                    "opened_ts": func.coalesce(ins.excluded.opened_ts, tbl.c.opened_ts),
                    "updated_ts": ins.excluded.updated_ts,
                    "extra": func.coalesce(ins.excluded.extra, tbl.c.extra),
                }

                stmt = ins.on_conflict_do_update(
                    index_elements=[tbl.c.wallet, tbl.c.mint, tbl.c.pool],
                    set_=upd,
                )

                with eng.begin() as cx:
                    cx.execute(stmt)
                return

            # Fallback path without postgres dialect: UPDATE then INSERT if missing.
            with eng.begin() as cx:
                res = cx.execute(
                    sa_update(tbl)
                    .where((tbl.c.wallet == w) & (tbl.c.mint == m) & (tbl.c.pool == p))
                    .values(
                        symbol=(sym if sym is not None else tbl.c.symbol),
                        status=st,
                        size_raw=(sr if sr is not None else tbl.c.size_raw),
                        size_sol=(float(size_sol) if size_sol is not None else tbl.c.size_sol),
                        entry_price=(
                            float(entry_price) if entry_price is not None else tbl.c.entry_price
                        ),
                        opened_ts=(opened if opened is not None else tbl.c.opened_ts),
                        updated_ts=now,
                        extra=(ex if ex is not None else tbl.c.extra),
                    )
                )

                # If updated nothing, insert a new row (best-effort mirror).
                rowcount = getattr(res, "rowcount", 0) or 0
                if int(rowcount) > 0:
                    return

                # Confirm doesn't exist (avoid duplicate if rowcount unavailable)
                exists = cx.execute(
                    select(tbl.c.wallet).where(
                        (tbl.c.wallet == w) & (tbl.c.mint == m) & (tbl.c.pool == p)
                    )
                ).first()
                if exists:
                    return

                cx.execute(
                    sa_insert(tbl).values(
                        wallet=w,
                        mint=m,
                        pool=p,
                        symbol=sym,
                        status=st,
                        size_raw=sr,
                        size_sol=(float(size_sol) if size_sol is not None else None),
                        entry_price=(float(entry_price) if entry_price is not None else None),
                        opened_ts=opened,
                        closed_ts=None,
                        updated_ts=now,
                        extra=ex,
                    )
                )

        try:
            await asyncio.to_thread(_do)
        except Exception as e:
            log.warning("PG_POS_UPSERT_FAIL: %s", str(e)[:220])
            _log_event("PG_POS_UPSERT_FAIL", err=str(e)[:500], wallet=w[:48], mint=m[:48])

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
        eng = await _ensure_engine()
        if eng is None:
            return

        _meta, tbl = _ensure_schema_sync()
        s = _sa()
        select = s["select"]
        sa_update = s["sa_update"]

        w = str(wallet or "").strip()[:96] or "unknown_wallet"
        m = str(mint or "").strip()[:96]
        p = str(pool or "").strip()[:128] or "unknown_pool"
        if not m:
            return

        now = int(updated_ts or _now_ts())
        cts = int(closed_ts or _now_ts())
        st = str(status or "closed")[:16]
        ex = extra if isinstance(extra, dict) else None

        def _do() -> None:
            with eng.begin() as cx:
                merged_extra = None
                if ex:
                    row = cx.execute(
                        select(tbl.c.extra).where(
                            (tbl.c.wallet == w) & (tbl.c.mint == m) & (tbl.c.pool == p)
                        )
                    ).first()
                    cur_extra = (row[0] if row else None) or {}
                    if not isinstance(cur_extra, dict):
                        cur_extra = {}
                    merged_extra = dict(cur_extra)
                    merged_extra.update(ex)

                cx.execute(
                    sa_update(tbl)
                    .where((tbl.c.wallet == w) & (tbl.c.mint == m) & (tbl.c.pool == p))
                    .values(
                        status=st,
                        closed_ts=cts,
                        updated_ts=now,
                        extra=(merged_extra if merged_extra is not None else tbl.c.extra),
                    )
                )

        try:
            await asyncio.to_thread(_do)
        except Exception as e:
            log.warning("PG_POS_CLOSE_FAIL: %s", str(e)[:220])
            _log_event("PG_POS_CLOSE_FAIL", err=str(e)[:500], wallet=w[:48], mint=m[:48])

    async def fetch_position(self, *, wallet: str, mint: str, pool: str) -> Any:
        # local import to avoid cycles at import-time
        from .positions_backend import PositionRow

        eng = await _ensure_engine()
        if eng is None:
            return None

        _meta, tbl = _ensure_schema_sync()
        s = _sa()
        select = s["select"]

        w = str(wallet or "").strip()[:96] or "unknown_wallet"
        m = str(mint or "").strip()[:96]
        p = str(pool or "").strip()[:128] or "unknown_pool"
        if not m:
            return None

        def _do() -> Any:
            with eng.connect() as cx:
                row = cx.execute(
                    select(tbl).where((tbl.c.wallet == w) & (tbl.c.mint == m) & (tbl.c.pool == p))
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
                    updated_ts=int(d["updated_ts"]) if d.get("updated_ts") is not None else None,
                    extra=d.get("extra") if isinstance(d.get("extra"), dict) else None,
                )

        try:
            return await asyncio.to_thread(_do)
        except Exception as e:
            log.warning("PG_POS_FETCH_FAIL: %s", str(e)[:200])
            _log_event("PG_POS_FETCH_FAIL", err=str(e)[:500], wallet=w[:48], mint=m[:48])
            return None

    async def fetch_open_positions(
        self, *, wallet: str | None = None, limit: int = 500
    ) -> list[Any]:
        # local import to avoid cycles at import-time
        from .positions_backend import PositionRow

        eng = await _ensure_engine()
        if eng is None:
            return []

        _meta, tbl = _ensure_schema_sync()
        s = _sa()
        select = s["select"]

        lim = max(1, min(int(limit or 500), 5000))
        w = str(wallet or "").strip()[:96] if wallet else None

        def _do() -> list[Any]:
            with eng.connect() as cx:
                q = select(tbl).where(tbl.c.status == "open")
                if w:
                    q = q.where(tbl.c.wallet == w)
                q = q.order_by(tbl.c.updated_ts.desc()).limit(lim)
                rows = cx.execute(q).fetchall()

            out: list[Any] = []
            for row in rows:
                d = dict(row._mapping)
                out.append(
                    PositionRow(
                        wallet=d.get("wallet") or (w or "unknown_wallet"),
                        mint=d.get("mint") or "",
                        pool=d.get("pool") or "",
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
                )
            return out

        try:
            return await asyncio.to_thread(_do)
        except Exception as e:
            log.warning("PG_POS_FETCH_OPEN_FAIL: %s", str(e)[:200])
            _log_event("PG_POS_FETCH_OPEN_FAIL", err=str(e)[:500], wallet=(w or "")[:48])
            return []


# ──────────────────────────────────────────────────────────────────────────────
# DEPRECATED legacy helpers (compat only; remove after trader/ledger rewrite)
# ──────────────────────────────────────────────────────────────────────────────
_PG_BACKEND_SINGLETON: PostgresPositionsBackend | None = None


def _backend() -> PostgresPositionsBackend:
    global _PG_BACKEND_SINGLETON
    if _PG_BACKEND_SINGLETON is None:
        _PG_BACKEND_SINGLETON = PostgresPositionsBackend()
    return _PG_BACKEND_SINGLETON


def _run(coro: Coroutine[Any, Any, Any]) -> Any:
    """
    Run a coroutine from sync legacy calls.
    If already in an event loop, schedule and return immediately (safe no-block).
    """
    try:
        loop = asyncio.get_running_loop()
        loop.create_task(coro)
        return None
    except RuntimeError:
        return asyncio.run(coro)


def all() -> list[dict[str, Any]]:
    """
    Legacy: return all open mirrored rows (across wallets). Best-effort.
    """

    async def _c() -> list[dict[str, Any]]:
        rows = await _backend().fetch_open_positions(wallet=None, limit=2000)
        return [r.__dict__ for r in rows]

    res = _run(_c())
    return res if isinstance(res, list) else []


def get(mint: str) -> dict[str, Any] | None:
    """
    Legacy: return the newest open row for this mint (prefers default wallet).
    """
    m = str(mint or "").strip()
    if not m:
        return None

    async def _c() -> dict[str, Any] | None:
        w = _default_wallet()
        rows = await _backend().fetch_open_positions(wallet=w, limit=500)
        for r in rows:
            if getattr(r, "mint", None) == m:
                return r.__dict__
        rows2 = await _backend().fetch_open_positions(wallet=None, limit=2000)
        for r in rows2:
            if getattr(r, "mint", None) == m:
                return r.__dict__
        return None

    res = _run(_c())
    return res if isinstance(res, dict) else None


def remove(mint: str) -> None:
    """
    Legacy: hard-delete ALL mirror rows for this mint (admin only). Best-effort.
    """
    m = str(mint or "").strip()
    if not m:
        return

    async def _c() -> None:
        eng = await _ensure_engine()
        if eng is None:
            return
        _meta, tbl = _ensure_schema_sync()
        sa_delete = _sa()["sa_delete"]

        def _do() -> None:
            with eng.begin() as cx:
                cx.execute(sa_delete(tbl).where(tbl.c.mint == m))

        await asyncio.to_thread(_do)

    _run(_c())


def decrement_qty(mint: str, sold_raw: int) -> None:
    """
    Legacy: decrement size_raw for default wallet rows of mint.
    If size_raw <= 0, close the row. Best-effort.
    """
    m = str(mint or "").strip()
    sr = _safe_int(sold_raw, 0)
    if not m or sr <= 0:
        return

    async def _c() -> None:
        eng = await _ensure_engine()
        if eng is None:
            return
        _meta, tbl = _ensure_schema_sync()
        s = _sa()
        select = s["select"]
        sa_update = s["sa_update"]

        w = _default_wallet()

        def _do() -> None:
            with eng.begin() as cx:
                rows = cx.execute(
                    select(tbl).where(
                        (tbl.c.wallet == w) & (tbl.c.mint == m) & (tbl.c.status == "open")
                    )
                ).fetchall()

                for row in rows:
                    d = dict(row._mapping)
                    key_pool = d.get("pool") or "unknown_pool"
                    cur = int(d.get("size_raw") or 0)
                    newq = max(0, cur - sr)
                    if newq <= 0:
                        cx.execute(
                            sa_update(tbl)
                            .where(
                                (tbl.c.wallet == w) & (tbl.c.mint == m) & (tbl.c.pool == key_pool)
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
                            sa_update(tbl)
                            .where(
                                (tbl.c.wallet == w) & (tbl.c.mint == m) & (tbl.c.pool == key_pool)
                            )
                            .values(size_raw=newq, updated_ts=_now_ts())
                        )

        await asyncio.to_thread(_do)

    _run(_c())


def update_after_sell(*, mint: str, sold_raw: int, tx_sig: str | None, closed: bool) -> None:
    """
    Legacy: decrement size_raw and optionally force-close. Best-effort.
    tx_sig is accepted for backward compatibility (not required in mirror).
    """
    _ = tx_sig  # compat placeholder (legacy callers pass it)
    m = str(mint or "").strip()
    sr = _safe_int(sold_raw, 0)
    if not m or sr <= 0:
        return

    async def _c() -> None:
        eng = await _ensure_engine()
        if eng is None:
            return
        _meta, tbl = _ensure_schema_sync()
        s = _sa()
        select = s["select"]
        sa_update = s["sa_update"]

        w = _default_wallet()

        def _do() -> None:
            with eng.begin() as cx:
                rows = cx.execute(
                    select(tbl).where(
                        (tbl.c.wallet == w) & (tbl.c.mint == m) & (tbl.c.status == "open")
                    )
                ).fetchall()

                for row in rows:
                    d = dict(row._mapping)
                    key_pool = d.get("pool") or "unknown_pool"
                    cur = int(d.get("size_raw") or 0)
                    newq = max(0, cur - sr)
                    force = bool(closed) or (newq <= 0)

                    cx.execute(
                        sa_update(tbl)
                        .where((tbl.c.wallet == w) & (tbl.c.mint == m) & (tbl.c.pool == key_pool))
                        .values(
                            size_raw=(0 if force else newq),
                            status=("closed" if force else "open"),
                            closed_ts=(_now_ts() if force else tbl.c.closed_ts),
                            updated_ts=_now_ts(),
                            extra=tbl.c.extra,  # do not mutate mirror-extra here
                        )
                    )

        await asyncio.to_thread(_do)

    _run(_c())


def record_sell(*, mint: str, qty_raw: int, sig: str | None) -> None:
    """
    Legacy: a sell record is just a decrement (best-effort mirror).
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
      - entry_price := price_per_token_sol (unit: sol_per_token)
      - extra includes decimals/token_program/owner_ata/last_sig
    """
    m = str(mint or "").strip()
    q = _safe_int(qty_raw, 0)
    if not m or q <= 0:
        return

    async def _c() -> None:
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
