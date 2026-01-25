from __future__ import annotations

"""
utils_pkg/ledger.py — Canonical Ledger + Realized PnL (V3+++ / God-Tier Overhaul) — LOCK-IN
=========================================================================================

Role
----
This module is the ONE canonical trade ledger for fills + realized PnL, designed to be:

- Postgres-first (Render + local identical)
- Deterministic & idempotent (dedupe_key; safe replays)
- Async-friendly (no blocking I/O on hot path)
- Import-safe (no network at import time; lazy init)
- Backwards-compatible (best-effort sync wrappers)
- Healthcheck-ready (structured ping; never randomly throws by default)

Contract
--------
Primary storage: Postgres (JSONB meta support; proper lot matching)
Legacy: SQLite allowed ONLY if LEDGER_ALLOW_SQLITE=1 (dev-only, not recommended)

Core tables (PG)
---------------
ledger_fills:
  - raw fills (buy/sell), deduped via (wallet, dedupe_key)
ledger_lots:
  - buy lots with remaining_qty_raw for FIFO matching
ledger_matches:
  - FIFO match rows that store realized pnl per matched qty (SOL)

Public API
----------
Async (preferred):
  - await record_fill_async(...)
  - await realized_pnl_range_async(...)
  - await realized_pnl_today_async(...)
  - await healthcheck(), await ping()

Sync wrappers (best-effort):
  - record_fill(...)
  - realized_pnl_today(...)

Env
---
Behavior:
  LEDGER_ENABLED=1                 (default on)
  LEDGER_STRICT=0                  (raise exceptions if 1)
  LEDGER_BACKEND=pg|sqlite         (default pg)
  LEDGER_ALLOW_SQLITE=0/1          (default 0)

Pool:
  LEDGER_PG_POOL_MIN=1
  LEDGER_PG_POOL_MAX=5
  LEDGER_PG_COMMAND_TIMEOUT=30
  LEDGER_PG_MAX_INACTIVE_CONN_LIFETIME=300
  LEDGER_PG_POOL_CREATE_TIMEOUT_S=10
  LEDGER_PG_POOL_FAIL_COOLDOWN_S=30

Legacy sqlite:
  PF_LEDGER_DB=state/ledger.db

Notes
-----
- Price unit: price_sol is SOL per token (UI token)
- qty_raw/decimals used to compute qty_ui
- FIFO matching uses remaining_qty_raw in lots
"""

import asyncio
import contextlib
import hashlib
import inspect
import os
import time
from dataclasses import dataclass
from decimal import Decimal, getcontext
from typing import Any

from utils_pkg.db_dsn import mask_dsn, pick_asyncpg_dsn

# Higher precision to reduce drift in PnL math
getcontext().prec = 40

_TRUE = {"1", "true", "yes", "y", "on", "t"}


# -----------------------------------------------------------------------------
# Logging shim (never hard-fail)
# -----------------------------------------------------------------------------
try:
    from logging_setup import get_logger  # type: ignore

    log = get_logger(__name__)  # type: ignore
except Exception:  # pragma: no cover
    import logging

    log = logging.getLogger(__name__)  # type: ignore

if not hasattr(log, "event"):

    def _event(name: str, **kw: Any) -> None:  # pragma: no cover
        with contextlib.suppress(Exception):
            log.info("%s %s", name, kw)

    log.event = _event


def _short_exc(e: BaseException, n: int = 240) -> str:
    s = f"{type(e).__name__}: {e}"
    return (s[:n] + "…") if len(s) > n else s


# -----------------------------------------------------------------------------
# Soft wallet helpers (for default wallet)
# -----------------------------------------------------------------------------
_wallet_err: str | None = None
try:
    from utils_pkg.solana_wallet import get_pubkey_str, load_keypair  # type: ignore
except Exception as e:  # pragma: no cover
    _wallet_err = str(e)
    get_pubkey_str = None  # type: ignore
    load_keypair = None  # type: ignore


def _envs(k: str, d: str = "") -> str:
    v = os.getenv(k)
    return (v if v is not None else d).strip()


def _envb(k: str, d: bool = False) -> bool:
    v = _envs(k, "")
    if not v:
        return bool(d)
    return v.strip().lower() in _TRUE


def _envi(k: str, d: int) -> int:
    try:
        return int(float(_envs(k, str(d))))
    except Exception:
        return int(d)


def _envf(k: str, d: float) -> float:
    try:
        return float(_envs(k, str(d)))
    except Exception:
        return float(d)


def _ledger_enabled() -> bool:
    return _envb("LEDGER_ENABLED", True)


def _strict_mode() -> bool:
    return _envb("LEDGER_STRICT", False)


def _default_wallet_from_env() -> str:
    for k in ("WALLET_PUBKEY", "PUBLIC_KEY", "PF_PP_ACCOUNT", "OWNER_PUBKEY"):
        v = (os.getenv(k) or "").strip()
        if v:
            return v
    return ""


_kp_cache: Any = None
_owner_cache: str = ""


def _default_wallet() -> str:
    global _kp_cache, _owner_cache
    if _owner_cache:
        return _owner_cache
    envv = _default_wallet_from_env()
    if envv:
        _owner_cache = envv
        return _owner_cache
    if callable(load_keypair) and callable(get_pubkey_str):
        try:
            if _kp_cache is None:
                _kp_cache = load_keypair()  # type: ignore[misc]
            _owner_cache = str(get_pubkey_str(_kp_cache))  # type: ignore[misc]
            return _owner_cache
        except Exception:
            return ""
    return ""


# -----------------------------------------------------------------------------
# Backend selection
# -----------------------------------------------------------------------------
def _backend() -> str:
    b = _envs("LEDGER_BACKEND", "pg").lower().strip()
    if b in {"postgres", "postgresql"}:
        b = "pg"
    if b not in {"pg", "sqlite"}:
        b = "pg"
    if b == "sqlite" and not _envb("LEDGER_ALLOW_SQLITE", False):
        # hard contract: sqlite is dev-only unless explicitly allowed
        b = "pg"
    return b


# -----------------------------------------------------------------------------
# Optional asyncpg (import-safe) + broken-wheel self-check
# -----------------------------------------------------------------------------
_ASYNCPG_OK = True
_ASYNCPG_IMPORT_ERR = ""
_ASYNCPG_SELFTEST_DONE = False

try:  # pragma: no cover
    import asyncpg  # type: ignore
except Exception as err:  # pragma: no cover
    asyncpg = None  # type: ignore
    _ASYNCPG_OK = False
    _ASYNCPG_IMPORT_ERR = str(err)


def _asyncpg_selfcheck() -> tuple[bool, str]:
    """
    Detect "broken asyncpg" installs that import but fail at runtime due to
    missing compiled protocol modules or partial wheels.

    This does NOT open any network connections.
    """
    global _ASYNCPG_SELFTEST_DONE, _ASYNCPG_OK, _ASYNCPG_IMPORT_ERR
    if _ASYNCPG_SELFTEST_DONE:
        return _ASYNCPG_OK, _ASYNCPG_IMPORT_ERR

    _ASYNCPG_SELFTEST_DONE = True
    if asyncpg is None:
        _ASYNCPG_OK = False
        _ASYNCPG_IMPORT_ERR = _ASYNCPG_IMPORT_ERR or "asyncpg not installed"
        return False, _ASYNCPG_IMPORT_ERR

    try:
        if not callable(getattr(asyncpg, "create_pool", None)):
            raise RuntimeError("asyncpg.create_pool missing")
        if not callable(getattr(asyncpg, "connect", None)):
            raise RuntimeError("asyncpg.connect missing")
        __import__("asyncpg.protocol.protocol")
        _ASYNCPG_OK = True
        _ASYNCPG_IMPORT_ERR = ""
        return True, ""
    except Exception as err:  # pragma: no cover
        _ASYNCPG_OK = False
        _ASYNCPG_IMPORT_ERR = f"asyncpg selfcheck failed: {err}"
        return False, _ASYNCPG_IMPORT_ERR


def _can_use_asyncpg() -> bool:
    ok, _ = _asyncpg_selfcheck()
    return bool(ok)


# -----------------------------------------------------------------------------
# Decimal helpers
# -----------------------------------------------------------------------------
def _d(x: Any, default: str = "0") -> Decimal:
    try:
        if isinstance(x, Decimal):
            return x
        if x is None:
            return Decimal(default)
        return Decimal(str(x))
    except Exception:
        return Decimal(default)


def _qty_ui(qty_raw: int, decimals: int) -> Decimal:
    if decimals <= 0:
        return _d(qty_raw)
    return _d(qty_raw) / (Decimal(10) ** Decimal(decimals))


def _hash_dedupe(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8", "ignore")).hexdigest()[:24]


def _compute_dedupe_key(
    *,
    wallet: str,
    side: str,
    mint: str,
    qty_raw: int,
    decimals: int,
    price_sol: float | Decimal,
    ts: int,
    sig: str | None,
    hint: str | None = None,
) -> str:
    if sig:
        return f"sig:{sig}"
    base = f"{wallet}|{side}|{mint}|{qty_raw}|{decimals}|{price_sol}|{ts}|{hint or ''}"
    return "h:" + _hash_dedupe(base)


# -----------------------------------------------------------------------------
# PG schema (idempotent)
# -----------------------------------------------------------------------------
PG_SCHEMA = r"""
CREATE TABLE IF NOT EXISTS ledger_fills (
  id              BIGSERIAL PRIMARY KEY,
  ts              BIGINT NOT NULL,
  ts_utc          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  wallet          TEXT NOT NULL,
  side            TEXT NOT NULL CHECK (side IN ('buy','sell')),
  mint            TEXT NOT NULL,
  decimals        INT  NOT NULL DEFAULT 0,
  qty_raw         BIGINT NOT NULL,
  qty_ui          NUMERIC(40, 18) NOT NULL,
  price_sol       NUMERIC(40, 18) NOT NULL,
  notional_sol    NUMERIC(40, 18) NOT NULL,
  sig             TEXT,
  dedupe_key      TEXT NOT NULL,
  pool            TEXT,
  route           TEXT,
  status          TEXT DEFAULT 'filled',
  meta            JSONB DEFAULT '{}'::jsonb
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_ledger_fills_wallet_dedupe
  ON ledger_fills(wallet, dedupe_key);

CREATE INDEX IF NOT EXISTS ix_ledger_fills_ts ON ledger_fills(ts);
CREATE INDEX IF NOT EXISTS ix_ledger_fills_wallet_ts ON ledger_fills(wallet, ts);
CREATE INDEX IF NOT EXISTS ix_ledger_fills_wallet_mint_ts ON ledger_fills(wallet, mint, ts);

-- Buy lots (FIFO)
CREATE TABLE IF NOT EXISTS ledger_lots (
  id                BIGSERIAL PRIMARY KEY,
  wallet            TEXT NOT NULL,
  mint              TEXT NOT NULL,
  buy_fill_id       BIGINT NOT NULL REFERENCES ledger_fills(id) ON DELETE CASCADE,
  buy_ts            BIGINT NOT NULL,
  decimals          INT NOT NULL DEFAULT 0,
  buy_qty_raw       BIGINT NOT NULL,
  remaining_qty_raw BIGINT NOT NULL,
  buy_price_sol     NUMERIC(40, 18) NOT NULL,
  buy_notional_sol  NUMERIC(40, 18) NOT NULL,
  meta              JSONB DEFAULT '{}'::jsonb
);

CREATE INDEX IF NOT EXISTS ix_ledger_lots_wallet_mint_rem
  ON ledger_lots(wallet, mint, remaining_qty_raw);

-- Match rows store realized PnL in SOL
CREATE TABLE IF NOT EXISTS ledger_matches (
  id                BIGSERIAL PRIMARY KEY,
  wallet            TEXT NOT NULL,
  mint              TEXT NOT NULL,
  sell_fill_id      BIGINT NOT NULL REFERENCES ledger_fills(id) ON DELETE CASCADE,
  buy_lot_id        BIGINT NOT NULL REFERENCES ledger_lots(id) ON DELETE CASCADE,
  sell_ts           BIGINT NOT NULL,
  matched_qty_raw   BIGINT NOT NULL,
  decimals          INT NOT NULL DEFAULT 0,
  buy_price_sol     NUMERIC(40, 18) NOT NULL,
  sell_price_sol    NUMERIC(40, 18) NOT NULL,
  pnl_sol           NUMERIC(40, 18) NOT NULL
);

CREATE INDEX IF NOT EXISTS ix_ledger_matches_wallet_ts ON ledger_matches(wallet, sell_ts);
CREATE INDEX IF NOT EXISTS ix_ledger_matches_wallet_mint_ts ON ledger_matches(wallet, mint, sell_ts);
"""


# -----------------------------------------------------------------------------
# DSN resolver (single source of truth): pick_asyncpg_dsn()
# -----------------------------------------------------------------------------
def _pick_dsn_flex(*, timeout_s: float | None, app_name: str | None) -> tuple[str, dict[str, Any]]:
    """
    Call pick_asyncpg_dsn in a signature-flex way so we never break if the helper evolves.
    """
    try:
        sig = inspect.signature(pick_asyncpg_dsn)
        allowed = set(sig.parameters.keys())
        kw: dict[str, Any] = {}
        if "timeout_s" in allowed:
            kw["timeout_s"] = timeout_s
        if "app_name" in allowed:
            kw["app_name"] = app_name
        out = pick_asyncpg_dsn(**kw)  # type: ignore[arg-type]
    except Exception:
        out = pick_asyncpg_dsn()  # type: ignore[call-arg]

    if isinstance(out, tuple) and len(out) >= 1:
        dsn = str(out[0] or "")
        kwargs = out[1] if (len(out) >= 2 and isinstance(out[1], dict)) else {}
        return dsn, dict(kwargs)
    return "", {}


def _sanitize_connect_kwargs(connect_kwargs: dict[str, Any]) -> dict[str, Any]:
    """
    Keep only asyncpg-safe kwargs; drop libpq-only / unknown keys to avoid TypeError.
    """
    if not connect_kwargs:
        return {}

    allowed = {
        "host",
        "port",
        "user",
        "password",
        "database",
        "ssl",
        "timeout",
        "statement_cache_size",
        "max_cached_statement_lifetime",
        "max_cacheable_statement_size",
        "command_timeout",
        "server_settings",
        "application_name",
    }

    cleaned: dict[str, Any] = {}
    for k, v in connect_kwargs.items():
        if v is None:
            continue
        if k in allowed:
            cleaned[k] = v

    app = cleaned.pop("application_name", None)
    if app:
        ss = cleaned.get("server_settings")
        if not isinstance(ss, dict):
            ss = {}
        ss.setdefault("application_name", str(app))
        cleaned["server_settings"] = ss

    return cleaned


# -----------------------------------------------------------------------------
# Postgres pool management (asyncpg) + cooldown + soft reset
# -----------------------------------------------------------------------------
_POOL: Any = None
_POOL_LOCK = asyncio.Lock()
_POOL_FAIL_UNTIL = 0.0
_POOL_LAST_ERR: str = ""
_POOL_LAST_OK_AT = 0.0

_SCHEMA_READY = False
_SCHEMA_LOCK = asyncio.Lock()


def _pg_pool_min() -> int:
    return max(1, min(32, _envi("LEDGER_PG_POOL_MIN", 1)))


def _pg_pool_max() -> int:
    return max(_pg_pool_min(), min(64, _envi("LEDGER_PG_POOL_MAX", 5)))


def _pg_cmd_timeout() -> float:
    return max(1.0, _envf("LEDGER_PG_COMMAND_TIMEOUT", 30.0))


def _pg_idle_sec() -> float:
    return max(0.0, _envf("LEDGER_PG_MAX_INACTIVE_CONN_LIFETIME", 300.0))


def _pg_create_timeout() -> float:
    return max(1.0, _envf("LEDGER_PG_POOL_CREATE_TIMEOUT_S", 10.0))


def _pg_fail_cooldown() -> float:
    return max(1.0, float(_envi("LEDGER_PG_POOL_FAIL_COOLDOWN_S", 30)))


def _sqlstate(e: BaseException) -> str:
    return str(getattr(e, "sqlstate", "") or "")


def _is_transient_err(e: BaseException) -> bool:
    st = _sqlstate(e)
    if st.startswith("08") or st in {"53300", "57P03"}:
        return True
    if _can_use_asyncpg():
        transient = (
            asyncpg.CannotConnectNowError,  # type: ignore[attr-defined]
            asyncpg.ConnectionDoesNotExistError,  # type: ignore[attr-defined]
            asyncpg.TooManyConnectionsError,  # type: ignore[attr-defined]
            asyncpg.ConnectionFailureError,  # type: ignore[attr-defined]
            asyncpg.ConnectionRejectionError,  # type: ignore[attr-defined]
            asyncpg.InterfaceError,  # type: ignore[attr-defined]
            asyncpg.PostgresConnectionError,  # type: ignore[attr-defined]
        )
        return isinstance(e, transient)
    return isinstance(e, OSError)


async def _reset_pool_soft(reason: str) -> None:
    global _POOL_FAIL_UNTIL, _POOL_LAST_ERR
    with contextlib.suppress(Exception):
        await close_pg_pool()
    _POOL_FAIL_UNTIL = max(_POOL_FAIL_UNTIL, time.time() + _pg_fail_cooldown())
    _POOL_LAST_ERR = reason


async def _get_pg_pool(
    *, timeout_s: float | None = None, app_name: str | None = None
) -> Any | None:
    """
    Return pool or None in soft mode when unavailable.
    """
    global _POOL, _POOL_FAIL_UNTIL, _POOL_LAST_ERR, _POOL_LAST_OK_AT

    if not _ledger_enabled():
        return None

    if not _can_use_asyncpg():
        msg = f"asyncpg unavailable: {_ASYNCPG_IMPORT_ERR or 'not installed'}"
        if _strict_mode():
            raise RuntimeError(msg)
        if int(time.time()) % 60 == 0:
            log.event("LEDGER_ASYNCPG_DISABLED", err=msg)
        return None

    now = time.time()
    if _POOL is None and now < _POOL_FAIL_UNTIL and not _strict_mode():
        return None

    if _POOL is not None:
        return _POOL

    async with _POOL_LOCK:
        if _POOL is not None:
            return _POOL

        dsn, connect_kwargs = _pick_dsn_flex(timeout_s=timeout_s, app_name=app_name)
        if not dsn:
            if _strict_mode():
                raise RuntimeError("Ledger PG enabled but no DSN env found (via pick_asyncpg_dsn).")
            return None

        kwargs = _sanitize_connect_kwargs(connect_kwargs or {})
        kwargs.setdefault("command_timeout", float(_pg_cmd_timeout()))
        if timeout_s is not None and "timeout" not in kwargs:
            kwargs["timeout"] = float(timeout_s)

        min_size = _pg_pool_min()
        max_size = _pg_pool_max()
        idle = float(_pg_idle_sec())

        try:
            log.event(
                "LEDGER_PG_POOL_CREATE",
                dsn=mask_dsn(dsn),
                min=min_size,
                max=max_size,
                cmd_timeout=float(kwargs.get("command_timeout", 0)),
                idle=idle,
            )
            coro = asyncpg.create_pool(  # type: ignore[union-attr]
                dsn=dsn,
                min_size=min_size,
                max_size=max_size,
                max_inactive_connection_lifetime=idle,
                **kwargs,
            )
            _POOL = await asyncio.wait_for(coro, timeout=_pg_create_timeout())
            _POOL_LAST_ERR = ""
            _POOL_FAIL_UNTIL = 0.0
            _POOL_LAST_OK_AT = time.time()
            return _POOL
        except Exception as err:
            _POOL = None
            _POOL_LAST_ERR = f"create_pool failed: {err}"
            _POOL_FAIL_UNTIL = time.time() + _pg_fail_cooldown()
            if _strict_mode():
                raise RuntimeError(
                    f"Failed to create ledger asyncpg pool dsn={mask_dsn(dsn)}"
                ) from err
            log.event("LEDGER_PG_POOL_CREATE_FAIL", dsn=mask_dsn(dsn), err=_short_exc(err))
            return None


async def _ensure_schema_pg() -> None:
    global _SCHEMA_READY
    if _SCHEMA_READY:
        return
    async with _SCHEMA_LOCK:
        if _SCHEMA_READY:
            return
        pool = await _get_pg_pool(timeout_s=5.0, app_name="ledger.ensure_schema")
        if pool is None:
            if _strict_mode():
                raise RuntimeError("Ledger schema ensure failed: pool unavailable")
            return
        async with pool.acquire() as con:
            await con.execute(PG_SCHEMA)
        _SCHEMA_READY = True
        log.event("LEDGER_SCHEMA_READY", backend="pg")


async def close_pg_pool() -> None:
    global _POOL, _SCHEMA_READY
    async with _POOL_LOCK:
        if _POOL is not None:
            with contextlib.suppress(Exception):
                await _POOL.close()
        _POOL = None
    _SCHEMA_READY = False


# -----------------------------------------------------------------------------
# SQLite legacy backend (disabled by default)
# -----------------------------------------------------------------------------
SQLITE_DB = _envs("PF_LEDGER_DB", "state/ledger.db")
SQLITE_SCHEMA = """
CREATE TABLE IF NOT EXISTS fills (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  ts INTEGER NOT NULL,
  wallet TEXT NOT NULL,
  side TEXT NOT NULL,
  mint TEXT NOT NULL,
  qty_raw INTEGER NOT NULL,
  decimals INTEGER NOT NULL,
  price_sol REAL NOT NULL,
  notional_sol REAL NOT NULL,
  sig TEXT,
  dedupe_key TEXT NOT NULL,
  meta TEXT
);
CREATE UNIQUE INDEX IF NOT EXISTS ux_fills_wallet_dedupe ON fills(wallet, dedupe_key);
CREATE INDEX IF NOT EXISTS ix_fills_ts ON fills(ts);
CREATE INDEX IF NOT EXISTS ix_fills_wallet_ts ON fills(wallet, ts);
"""


def _sqlite_allowed() -> bool:
    return _envb("LEDGER_ALLOW_SQLITE", False)


def _sqlite_conn():  # pragma: no cover (dev-only)
    import sqlite3

    os.makedirs(os.path.dirname(SQLITE_DB), exist_ok=True)
    con = sqlite3.connect(SQLITE_DB)
    con.execute("PRAGMA journal_mode=WAL;")
    con.executescript(SQLITE_SCHEMA)
    return con


# -----------------------------------------------------------------------------
# Healthcheck contract
# -----------------------------------------------------------------------------
async def healthcheck(
    *,
    timeout_s: float = 2.0,
    raise_on_fail: bool = False,
    app_name: str | None = "utils_pkg.ledger.healthcheck",
) -> dict[str, Any]:
    """
    Structured health result. Never raises unless raise_on_fail=True.
    """
    backend = _backend()
    enabled = _ledger_enabled()
    strict = _strict_mode()
    asyncpg_ok = _can_use_asyncpg()

    result: dict[str, Any] = {
        "ok": False,
        "backend": backend,
        "enabled": bool(enabled),
        "strict": bool(strict),
        "asyncpg_ok": bool(asyncpg_ok),
        "asyncpg_err": _ASYNCPG_IMPORT_ERR if not asyncpg_ok else "",
        "dsn_set": False,
        "pool": {"ok": False, "last_err": _POOL_LAST_ERR, "last_ok_at": _POOL_LAST_OK_AT},
        "query": {"ok": False, "latency_ms": None, "err": ""},
        "ts": int(time.time()),
    }

    if not enabled:
        result["ok"] = True
        return result

    if backend == "sqlite":
        if not _sqlite_allowed():
            if raise_on_fail:
                raise RuntimeError("Ledger sqlite backend selected but LEDGER_ALLOW_SQLITE=0")
            return result
        try:
            with _sqlite_conn():
                pass
            result["ok"] = True
            result["query"]["ok"] = True
            return result
        except Exception as err:  # pragma: no cover
            result["query"]["err"] = str(err)
            if raise_on_fail:
                raise
            return result

    # PG backend
    dsn, _ = _pick_dsn_flex(timeout_s=float(timeout_s), app_name=app_name)
    result["dsn_set"] = bool(dsn)

    if not asyncpg_ok:
        if raise_on_fail:
            raise RuntimeError(result["asyncpg_err"] or "asyncpg unavailable")
        return result

    try:
        pool = await _get_pg_pool(timeout_s=float(timeout_s), app_name=app_name)
    except Exception as err:
        result["pool"]["last_err"] = str(err)
        if raise_on_fail:
            raise
        return result

    if pool is None:
        if raise_on_fail:
            raise RuntimeError("Ledger pool unavailable")
        return result

    result["pool"]["ok"] = True

    t0 = time.perf_counter()
    try:
        async with pool.acquire() as con:
            await asyncio.wait_for(con.fetchval("SELECT 1"), timeout=float(timeout_s))
        ms = int((time.perf_counter() - t0) * 1000.0)
        result["query"]["ok"] = True
        result["query"]["latency_ms"] = ms
        result["ok"] = True
        return result
    except Exception as err:
        ms = int((time.perf_counter() - t0) * 1000.0)
        result["query"]["latency_ms"] = ms
        result["query"]["err"] = str(err)
        if _is_transient_err(err) and not strict:
            await _reset_pool_soft(reason=f"healthcheck transient: {err}")
        if raise_on_fail:
            raise RuntimeError(f"Ledger DB healthcheck failed: {err}") from err
        return result


async def ping(*, timeout_s: float = 2.0) -> bool:
    r = await healthcheck(timeout_s=timeout_s, raise_on_fail=False)
    return bool(r.get("ok", False))


# -----------------------------------------------------------------------------
# Core record logic (PG)
# -----------------------------------------------------------------------------
@dataclass
class RecordResult:
    ok: bool
    backend: str
    inserted: bool
    wallet: str
    side: str
    mint: str
    fill_id: int | None
    dedupe_key: str
    realized_pnl_sol: Decimal
    reason: str = ""


async def _record_fill_pg(
    *,
    wallet: str,
    side: str,
    mint: str,
    qty_raw: int,
    decimals: int,
    price_sol: float | Decimal,
    ts: int | None,
    sig: str | None,
    pool: str | None,
    route: str | None,
    status: str | None,
    meta: dict[str, Any] | None,
    dedupe_key: str | None,
) -> RecordResult:
    await _ensure_schema_pg()

    w = (wallet or "").strip()
    if not w:
        return RecordResult(
            False, "pg", False, wallet, side, mint, None, "", Decimal("0"), "wallet_missing"
        )

    s = (side or "").strip().lower()
    if s not in ("buy", "sell"):
        return RecordResult(False, "pg", False, w, side, mint, None, "", Decimal("0"), "bad_side")

    m = (mint or "").strip()
    if not m:
        return RecordResult(False, "pg", False, w, s, mint, None, "", Decimal("0"), "mint_missing")

    qraw = int(qty_raw)
    dec = int(decimals or 0)
    if qraw <= 0:
        return RecordResult(False, "pg", False, w, s, m, None, "", Decimal("0"), "qty_raw<=0")

    px = _d(price_sol)
    if px <= 0:
        return RecordResult(False, "pg", False, w, s, m, None, "", Decimal("0"), "price_sol<=0")

    t = int(ts if ts is not None else time.time())
    qtyui = _qty_ui(qraw, dec)
    notion = qtyui * px

    dk = (dedupe_key or "").strip()
    if not dk:
        dk = _compute_dedupe_key(
            wallet=w, side=s, mint=m, qty_raw=qraw, decimals=dec, price_sol=px, ts=t, sig=sig
        )

    poolv = pool or None
    routev = route or None
    statusv = status or "filled"
    metav = meta or {}

    dbpool = await _get_pg_pool(timeout_s=10.0, app_name="ledger.record_fill")
    if dbpool is None:
        if _strict_mode():
            raise RuntimeError("Ledger PG pool unavailable")
        return RecordResult(False, "pg", False, w, s, m, None, dk, Decimal("0"), "pool_unavailable")

    realized = Decimal("0")

    try:
        async with dbpool.acquire() as con:
            async with con.transaction():
                row = await con.fetchrow(
                    """
                    INSERT INTO ledger_fills
                      (ts, wallet, side, mint, decimals, qty_raw, qty_ui, price_sol, notional_sol, sig, dedupe_key, pool, route, status, meta)
                    VALUES
                      ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15::jsonb)
                    ON CONFLICT (wallet, dedupe_key) DO NOTHING
                    RETURNING id
                    """,
                    t,
                    w,
                    s,
                    m,
                    dec,
                    qraw,
                    str(qtyui),
                    str(px),
                    str(notion),
                    sig,
                    dk,
                    poolv,
                    routev,
                    statusv,
                    metav,
                )
                if not row:
                    return RecordResult(
                        True, "pg", False, w, s, m, None, dk, Decimal("0"), "deduped"
                    )

                fill_id = int(row["id"])

                if s == "buy":
                    await con.execute(
                        """
                        INSERT INTO ledger_lots
                          (wallet, mint, buy_fill_id, buy_ts, decimals, buy_qty_raw, remaining_qty_raw, buy_price_sol, buy_notional_sol, meta)
                        VALUES
                          ($1,$2,$3,$4,$5,$6,$6,$7,$8,$9::jsonb)
                        """,
                        w,
                        m,
                        fill_id,
                        t,
                        dec,
                        qraw,
                        str(px),
                        str(notion),
                        metav,
                    )
                    return RecordResult(
                        True, "pg", True, w, s, m, fill_id, dk, Decimal("0"), "inserted_buy"
                    )

                # SELL: FIFO match to open lots
                sell_qty_left = qraw
                sell_px = px

                lots = await con.fetch(
                    """
                    SELECT id, remaining_qty_raw, decimals, buy_price_sol
                    FROM ledger_lots
                    WHERE wallet=$1 AND mint=$2 AND remaining_qty_raw > 0
                    ORDER BY buy_ts ASC, id ASC
                    """,
                    w,
                    m,
                )

                for lot in lots:
                    if sell_qty_left <= 0:
                        break
                    lot_id = int(lot["id"])
                    rem = int(lot["remaining_qty_raw"])
                    ldec = int(lot["decimals"] or 0)
                    buy_px = _d(lot["buy_price_sol"])

                    use = min(rem, sell_qty_left)
                    if use <= 0:
                        continue

                    qtyui_m = _qty_ui(use, ldec)
                    pnl = qtyui_m * (sell_px - buy_px)

                    await con.execute(
                        "UPDATE ledger_lots SET remaining_qty_raw = remaining_qty_raw - $2 WHERE id = $1",
                        lot_id,
                        use,
                    )
                    await con.execute(
                        """
                        INSERT INTO ledger_matches
                          (wallet, mint, sell_fill_id, buy_lot_id, sell_ts, matched_qty_raw, decimals, buy_price_sol, sell_price_sol, pnl_sol)
                        VALUES
                          ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
                        """,
                        w,
                        m,
                        fill_id,
                        lot_id,
                        t,
                        use,
                        ldec,
                        str(buy_px),
                        str(sell_px),
                        str(pnl),
                    )

                    realized += pnl
                    sell_qty_left -= use

                if sell_qty_left > 0:
                    log.event(
                        "LEDGER_SELL_UNMATCHED_QTY",
                        wallet=w,
                        mint=m,
                        sell_fill_id=fill_id,
                        unmatched_qty_raw=sell_qty_left,
                    )

                return RecordResult(
                    True, "pg", True, w, s, m, fill_id, dk, realized, "inserted_sell"
                )
    except Exception as err:
        if _is_transient_err(err) and not _strict_mode():
            await _reset_pool_soft(reason=f"record transient: {err}")
        if _strict_mode():
            raise
        log.event("LEDGER_RECORD_PG_ERR", err=_short_exc(err), wallet=w, side=s, mint=m)
        return RecordResult(
            False, "pg", False, w, s, m, None, dk, Decimal("0"), f"pg_error:{_short_exc(err)}"
        )


# -----------------------------------------------------------------------------
# Core record logic (SQLite legacy)
# -----------------------------------------------------------------------------
async def _record_fill_sqlite(  # pragma: no cover (dev-only)
    *,
    wallet: str,
    side: str,
    mint: str,
    qty_raw: int,
    decimals: int,
    price_sol: float | Decimal,
    ts: int | None,
    sig: str | None,
    meta: dict[str, Any] | None,
    dedupe_key: str | None,
) -> RecordResult:
    if not _sqlite_allowed():
        return RecordResult(
            False, "sqlite", False, wallet, side, mint, None, "", Decimal("0"), "sqlite_disabled"
        )

    w = (wallet or "").strip() or _default_wallet()
    s = (side or "").strip().lower()
    m = (mint or "").strip()
    qraw = int(qty_raw)
    dec = int(decimals or 0)
    px = float(price_sol)
    t = int(ts if ts is not None else time.time())

    if not w:
        return RecordResult(
            False, "sqlite", False, wallet, side, mint, None, "", Decimal("0"), "wallet_missing"
        )
    if s not in ("buy", "sell"):
        return RecordResult(
            False, "sqlite", False, w, side, mint, None, "", Decimal("0"), "bad_side"
        )
    if not m:
        return RecordResult(
            False, "sqlite", False, w, s, mint, None, "", Decimal("0"), "mint_missing"
        )
    if qraw <= 0 or px <= 0:
        return RecordResult(
            False, "sqlite", False, w, s, m, None, "", Decimal("0"), "bad_qty_or_price"
        )

    dk = (dedupe_key or "").strip()
    if not dk:
        dk = _compute_dedupe_key(
            wallet=w, side=s, mint=m, qty_raw=qraw, decimals=dec, price_sol=px, ts=t, sig=sig
        )

    qtyui = float(_qty_ui(qraw, dec))
    notion = float(qtyui * px)

    import json

    with _sqlite_conn() as con:
        try:
            con.execute(
                "INSERT INTO fills(ts, wallet, side, mint, qty_raw, decimals, price_sol, notional_sol, sig, dedupe_key, meta) "
                "VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                (t, w, s, m, qraw, dec, float(px), float(notion), sig, dk, json.dumps(meta or {})),
            )
            con.commit()
            return RecordResult(True, "sqlite", True, w, s, m, None, dk, Decimal("0"), "inserted")
        except Exception:
            return RecordResult(
                True, "sqlite", False, w, s, m, None, dk, Decimal("0"), "deduped_or_error"
            )


# -----------------------------------------------------------------------------
# Public async API
# -----------------------------------------------------------------------------
async def record_fill_async(
    *,
    side: str,
    mint: str,
    qty_raw: int,
    decimals: int,
    price_sol: float | Decimal,
    wallet: str | None = None,
    ts: int | None = None,
    sig: str | None = None,
    pool: str | None = None,
    route: str | None = None,
    status: str | None = None,
    meta: dict[str, Any] | None = None,
    dedupe_key: str | None = None,
) -> dict[str, Any]:
    """
    Record a fill and (for sells) compute realized PnL via FIFO matching.

    Returns:
      { ok, backend, inserted, fill_id, dedupe_key, realized_pnl_sol, reason }
    """
    backend = _backend()

    if not _ledger_enabled():
        return {
            "ok": True,
            "backend": backend,
            "inserted": False,
            "fill_id": None,
            "dedupe_key": (dedupe_key or ""),
            "realized_pnl_sol": 0.0,
            "reason": "disabled",
        }

    w = (wallet or "").strip() or _default_wallet()
    if not w:
        return {
            "ok": False,
            "backend": backend,
            "inserted": False,
            "fill_id": None,
            "dedupe_key": "",
            "realized_pnl_sol": 0.0,
            "reason": f"wallet_missing:{_wallet_err or 'no_wallet'}",
        }

    if backend == "sqlite":
        res = await _record_fill_sqlite(
            wallet=w,
            side=side,
            mint=mint,
            qty_raw=qty_raw,
            decimals=decimals,
            price_sol=price_sol,
            ts=ts,
            sig=sig,
            meta=meta,
            dedupe_key=dedupe_key,
        )
        return {
            "ok": res.ok,
            "backend": res.backend,
            "inserted": res.inserted,
            "fill_id": res.fill_id,
            "dedupe_key": res.dedupe_key,
            "realized_pnl_sol": float(res.realized_pnl_sol),
            "reason": res.reason,
        }

    # PG default
    try:
        res = await _record_fill_pg(
            wallet=w,
            side=side,
            mint=mint,
            qty_raw=qty_raw,
            decimals=decimals,
            price_sol=price_sol,
            ts=ts,
            sig=sig,
            pool=pool,
            route=route,
            status=status,
            meta=meta,
            dedupe_key=dedupe_key,
        )
        return {
            "ok": res.ok,
            "backend": res.backend,
            "inserted": res.inserted,
            "fill_id": res.fill_id,
            "dedupe_key": res.dedupe_key,
            "realized_pnl_sol": float(res.realized_pnl_sol),
            "reason": res.reason,
        }
    except Exception as e:
        if _strict_mode():
            raise
        log.event(
            "LEDGER_RECORD_ERR",
            backend="pg",
            err=_short_exc(e),
            mint=(mint or "").strip(),
            side=(side or "").strip(),
        )
        return {
            "ok": False,
            "backend": "pg",
            "inserted": False,
            "fill_id": None,
            "dedupe_key": (dedupe_key or ""),
            "realized_pnl_sol": 0.0,
            "reason": f"pg_error:{_short_exc(e)}",
        }


# -----------------------------------------------------------------------------
# Best-effort sync wrappers (do not crash the bot)
# -----------------------------------------------------------------------------
def record_fill(
    *,
    side: str,
    mint: str,
    qty_raw: int,
    decimals: int,
    price_sol: float | Decimal,
    wallet: str | None = None,
    ts: int | None = None,
    sig: str | None = None,
    pool: str | None = None,
    route: str | None = None,
    status: str | None = None,
    meta: dict[str, Any] | None = None,
    dedupe_key: str | None = None,
) -> None:
    """
    Backwards-compatible wrapper:
    - If we're already in an event loop: fire-and-forget task (best-effort).
    - Otherwise: run the async method to completion.

    If you need deterministic persistence, call `await record_fill_async(...)`.
    """
    try:
        loop = asyncio.get_running_loop()
        loop.create_task(
            record_fill_async(
                side=side,
                mint=mint,
                qty_raw=qty_raw,
                decimals=decimals,
                price_sol=price_sol,
                wallet=wallet,
                ts=ts,
                sig=sig,
                pool=pool,
                route=route,
                status=status,
                meta=meta,
                dedupe_key=dedupe_key,
            )
        )
    except RuntimeError:
        asyncio.run(
            record_fill_async(
                side=side,
                mint=mint,
                qty_raw=qty_raw,
                decimals=decimals,
                price_sol=price_sol,
                wallet=wallet,
                ts=ts,
                sig=sig,
                pool=pool,
                route=route,
                status=status,
                meta=meta,
                dedupe_key=dedupe_key,
            )
        )
    except Exception as e:
        log.event("LEDGER_RECORD_SYNC_ERR", err=_short_exc(e))


# -----------------------------------------------------------------------------
# Realized PnL queries (PG-first)
# -----------------------------------------------------------------------------
async def realized_pnl_range_async(
    *,
    start_ts: int,
    end_ts: int,
    wallet: str | None = None,
    mint: str | None = None,
) -> float:
    """
    Sum realized PnL (SOL) for sells whose timestamp is within [start_ts, end_ts].
    """
    backend = _backend()
    if backend == "sqlite":
        if not _sqlite_allowed():
            return 0.0
        try:  # pragma: no cover
            w = (wallet or "").strip() or _default_wallet()
            with _sqlite_conn() as con:
                cur = con.execute(
                    "SELECT side, SUM(notional_sol) FROM fills WHERE wallet=? AND ts BETWEEN ? AND ? GROUP BY side",
                    (w, int(start_ts), int(end_ts)),
                )
                d = dict(cur.fetchall())
            return float(d.get("sell", 0.0)) - float(d.get("buy", 0.0))
        except Exception:
            return 0.0

    try:
        await _ensure_schema_pg()
        w = (wallet or "").strip() or _default_wallet()
        if not w:
            return 0.0

        pool = await _get_pg_pool(timeout_s=5.0, app_name="ledger.pnl_range")
        if pool is None:
            return 0.0

        async with pool.acquire() as con:
            if mint:
                row = await con.fetchrow(
                    """
                    SELECT COALESCE(SUM(pnl_sol), 0) AS pnl
                    FROM ledger_matches
                    WHERE wallet=$1 AND mint=$2 AND sell_ts BETWEEN $3 AND $4
                    """,
                    w,
                    (mint or "").strip(),
                    int(start_ts),
                    int(end_ts),
                )
            else:
                row = await con.fetchrow(
                    """
                    SELECT COALESCE(SUM(pnl_sol), 0) AS pnl
                    FROM ledger_matches
                    WHERE wallet=$1 AND sell_ts BETWEEN $2 AND $3
                    """,
                    w,
                    int(start_ts),
                    int(end_ts),
                )
        return float(_d(row["pnl"] if row else 0))
    except Exception as e:
        if _strict_mode():
            raise
        log.event("LEDGER_PNL_RANGE_ERR", err=_short_exc(e))
        return 0.0


async def realized_pnl_today_async(
    *, wallet: str | None = None, tz: str = "UTC", mint: str | None = None
) -> float:
    """
    Realized PnL for today (UTC by default). Uses ledger_matches (FIFO PnL).
    tz is reserved for future expansion; today is deterministic UTC boundaries.
    """
    import datetime as dt

    d = dt.datetime.utcnow().date()
    start = int(dt.datetime(d.year, d.month, d.day, 0, 0, 0, tzinfo=dt.UTC).timestamp())
    end = int(dt.datetime(d.year, d.month, d.day, 23, 59, 59, tzinfo=dt.UTC).timestamp())
    return await realized_pnl_range_async(start_ts=start, end_ts=end, wallet=wallet, mint=mint)


def realized_pnl_today(*, wallet: str | None = None, mint: str | None = None) -> float:
    """
    Sync wrapper for realized_pnl_today_async().
    """
    try:
        asyncio.get_running_loop()
        log.event(
            "LEDGER_SYNC_PNL_CALLED_IN_LOOP", wallet=(wallet or "").strip() or _default_wallet()
        )
        return 0.0
    except RuntimeError:
        try:
            return float(asyncio.run(realized_pnl_today_async(wallet=wallet, mint=mint)))
        except Exception:
            return 0.0


__all__ = [
    "close_pg_pool",
    "healthcheck",
    "ping",
    "realized_pnl_range_async",
    "realized_pnl_today",
    "realized_pnl_today_async",
    "record_fill",
    "record_fill_async",
]
