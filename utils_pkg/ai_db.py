# -*- coding: utf-8 -*-
from __future__ import annotations

"""
utils_pkg/ai_db.py

God-Tier AI DB brain (Production / Quant-grade)
==============================================

Central Postgres utility for ALL persistent AI / quant data.

Hard guarantees
---------------
- Import-safe: NO network connects on import.
- DSN resolution + normalization (Render internal/external + local).
- Correct TLS behavior for asyncpg via explicit SSLContext when needed.
- Idempotent migrations (won't brick deploys).
- Soft-fail reads/writes (configurable) so trading never stops due to DB.
- Narrow public helpers: no ad-hoc SQL scattered elsewhere.
- Optional asyncpg pool (off by default).
- Schema supports entry/exit training + execution logs + dashboards.

Env (key)
---------
Enable/disable:
  AI_DB_ENABLED=1
  AI_DB_AUTODISABLE_IF_NO_DSN=1
  AI_DB_AUTODISABLE_IF_NO_DRIVER=1
  AI_DB_READ_ONLY=0

Soft-fail:
  AI_DB_SOFT_FAIL_WRITES=1
  AI_DB_SOFT_FAIL_READS=1

Connection:
  AI_DB_APP_NAME=sniper_ai_db
  AI_DB_CONNECT_TIMEOUT_S=10
  AI_DB_CONNECT_RETRIES=2
  AI_DB_CONNECT_RETRY_BASE_S=0.35
  AI_DB_CONNECT_RETRY_MAX_S=2.5

Session timeouts:
  AI_DB_STATEMENT_TIMEOUT_MS=0
  AI_DB_IDLE_IN_TX_TIMEOUT_MS=0
  AI_DB_LOCK_TIMEOUT_MS=0

SSL/DSN normalization:
  AI_DB_FORCE_SSLMODE=require|prefer|disable|allow|verify-ca|verify-full
  AI_DB_RENDER_SSLMODE=require          (default for external Render hosts)
  AI_DB_INTERNAL_SSLMODE=prefer         (default for internal Render dpg-* hosts)
  AI_DB_SSL_INSECURE=0                  (debug only, disables cert validation)
  PG_SSL_INSECURE=0                     (alias)
  AI_DB_SSL_CA_FILE=                    (optional CA bundle path)

DSN resolution priority:
  SNIPER_DB_DSN (recommended)
  PNL_DSN
  SNIPER_DB_URL
  DATABASE_URL
  POSTGRES_DSN
  POSTGRES_URL
  PG_DSN
  PG_DSN_INTERNAL
  PG_DSN_EXTERNAL
  RENDER_INTERNAL_DATABASE_URL
  RENDER_DATABASE_URL
  SNIPER_DB_DSN_FALLBACK (last resort)

Migrations:
  AI_DB_MIGRATIONS_ENABLED=1
  AI_DB_USE_MIGRATION_LOCK=1
  AI_DB_MIGRATION_LOCK_ID=907331
  AI_DB_MIGRATION_LOCK_TIMEOUT_S=20

Async pool:
  AI_DB_USE_ASYNCPG_POOL=0
  AI_DB_ASYNCPG_POOL_MIN_SIZE=1
  AI_DB_ASYNCPG_POOL_MAX_SIZE=4
  AI_DB_ASYNCPG_POOL_MAX_INACTIVE_S=60

Defaults (dashboard scoping):
  AI_DB_DEFAULT_SRC=
  AI_DB_DEFAULT_REGIME=
  AI_DB_DEFAULT_WALLET_TAG=
  AI_DB_MIN_FEATURE_VERSION=
  AI_DB_WIN_LABELS=good,win,green,profit
"""

import asyncio
import copy
import dataclasses
import hashlib
import json
import os
import random
import re
import shlex
import ssl
import time
from collections import defaultdict
from collections.abc import Callable, Iterator, Mapping, Sequence
from contextlib import contextmanager, suppress
from dataclasses import dataclass
from datetime import datetime, timezone
from functools import wraps
from typing import Any, ParamSpec, Protocol, TypeVar, cast

# -----------------------------------------------------------------------------
# Logging (robust import; never fails import)
# -----------------------------------------------------------------------------
try:
    from utils_pkg.logging_setup import get_logger
except Exception:  # pragma: no cover
    try:
        from logging_setup import get_logger  # type: ignore
    except Exception:  # pragma: no cover
        import logging

        def get_logger() -> Any:  # type: ignore
            return logging.getLogger("ai_db")


class _LoggerBase(Protocol):
    def debug(self, msg: str, *args: object, **kwargs: object) -> None: ...
    def info(self, msg: str, *args: object, **kwargs: object) -> None: ...
    def warning(self, msg: str, *args: object, **kwargs: object) -> None: ...
    def error(self, msg: str, *args: object, **kwargs: object) -> None: ...
    def exception(self, msg: str, *args: object, **kwargs: object) -> None: ...


class _LoggerLike(_LoggerBase, Protocol):
    # repo-wide throttle helper
    def every(self, key: str, n: int = 1000) -> bool: ...


class _LoggerShim:
    """
    Wrap any logger-like object and guarantee `.every(key, n)` exists,
    WITHOUT monkeypatching methods (keeps mypy happy).

    If the underlying logger already implements `.every`, we delegate to it.
    Otherwise we provide a deterministic throttle:
      - returns True on the 1st call for a key
      - then True every n calls (n<=0 => always True)
    """

    __slots__ = ("_base", "_base_every", "_counts")

    def __init__(self, base: _LoggerBase) -> None:
        self._base = base
        maybe = getattr(base, "every", None)
        self._base_every: Callable[[str, int], bool] | None = (
            cast(Callable[[str, int], bool], maybe) if callable(maybe) else None
        )
        self._counts: dict[str, int] = defaultdict(int)

    # Delegate standard logger methods
    def debug(self, msg: str, *args: object, **kwargs: object) -> None:
        self._base.debug(msg, *args, **kwargs)

    def info(self, msg: str, *args: object, **kwargs: object) -> None:
        self._base.info(msg, *args, **kwargs)

    def warning(self, msg: str, *args: object, **kwargs: object) -> None:
        self._base.warning(msg, *args, **kwargs)

    def error(self, msg: str, *args: object, **kwargs: object) -> None:
        self._base.error(msg, *args, **kwargs)

    def exception(self, msg: str, *args: object, **kwargs: object) -> None:
        self._base.exception(msg, *args, **kwargs)

    def every(self, key: str, n: int = 1000) -> bool:
        if self._base_every is not None:
            try:
                return bool(self._base_every(key, n))
            except Exception:
                # fall through to shim behavior
                pass

        if n <= 0:
            return True
        k = str(key)
        self._counts[k] += 1
        c = self._counts[k]
        return c == 1 or (c % int(n) == 1)


log = cast(_LoggerLike, _LoggerShim(cast(_LoggerBase, get_logger())))


# -----------------------------------------------------------------------------
# DB protocol surface (mypy-stable across psycopg2/psycopg3)
# -----------------------------------------------------------------------------
class _CursorLike(Protocol):
    rowcount: int
    itersize: int

    def execute(self, query: Any, params: Any = ...) -> Any: ...
    def executemany(self, query: Any, params: Any) -> Any: ...
    def fetchone(self) -> Any: ...
    def fetchall(self) -> Any: ...
    def fetchmany(self, size: int) -> Any: ...
    def close(self) -> Any: ...
    def __iter__(self) -> Iterator[Any]: ...


class _ConnLike(Protocol):
    def cursor(self, *args: Any, **kwargs: Any) -> _CursorLike: ...
    def commit(self) -> Any: ...
    def rollback(self) -> Any: ...
    def close(self) -> Any: ...


# -----------------------------------------------------------------------------
# Contract DB schema surface (tests may look for these)
# -----------------------------------------------------------------------------
LEGACY_SCHEMA_VERSION: int = 1
SCHEMA_VERSION: int = 4  # canonical current schema version


@dataclass(frozen=True)
class Migration:
    from_version: int | None
    to_version: int
    name: str
    sql: str | Sequence[str] = ()


def _migration_sql(m: Migration) -> str:
    if isinstance(m.sql, str):
        return m.sql
    return "\n".join([s for s in m.sql if isinstance(s, str) and s.strip()])


# Keep rich objects internally
MIGRATION_PLAN: list[Migration] = [
    Migration(
        from_version=None,
        to_version=LEGACY_SCHEMA_VERSION,
        name="init_core_tables",
        sql=(
            """
            CREATE TABLE IF NOT EXISTS ai_candidates (
                id BIGSERIAL PRIMARY KEY,
                ts BIGINT NOT NULL,
                src TEXT DEFAULT '',
                mint TEXT DEFAULT '',
                sym TEXT DEFAULT '',
                features JSONB DEFAULT '{}'::jsonb
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS ai_trades (
                id BIGSERIAL PRIMARY KEY,
                ts BIGINT NOT NULL,
                mint TEXT DEFAULT '',
                side TEXT DEFAULT '',
                qty_raw NUMERIC DEFAULT 0,
                price_sol NUMERIC DEFAULT 0,
                tx TEXT DEFAULT ''
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS ai_positions (
                id BIGSERIAL PRIMARY KEY,
                ts BIGINT NOT NULL,
                mint TEXT DEFAULT '',
                qty_raw NUMERIC DEFAULT 0,
                cost_sol NUMERIC DEFAULT 0,
                extra JSONB DEFAULT '{}'::jsonb
            );
            """,
            "CREATE INDEX IF NOT EXISTS idx_ai_candidates_ts ON ai_candidates(ts);",
            "CREATE INDEX IF NOT EXISTS idx_ai_trades_ts ON ai_trades(ts);",
            "CREATE INDEX IF NOT EXISTS idx_ai_positions_ts ON ai_positions(ts);",
        ),
    ),
    Migration(
        from_version=LEGACY_SCHEMA_VERSION,
        to_version=SCHEMA_VERSION,
        name="init_quant_schema_v4",
        sql=(
            r"""
        CREATE TABLE IF NOT EXISTS ai_schema_meta (
            k  TEXT PRIMARY KEY,
            v  TEXT,
            ts TIMESTAMPTZ NOT NULL DEFAULT now()
        );

        INSERT INTO ai_schema_meta (k, v)
        VALUES ('schema_version', '4')
        ON CONFLICT (k) DO UPDATE SET v = EXCLUDED.v, ts = now();

        CREATE TABLE IF NOT EXISTS entry_candidates (
            id              BIGSERIAL PRIMARY KEY,
            ts              TIMESTAMPTZ NOT NULL DEFAULT now(),
            src             TEXT NOT NULL,
            mint            TEXT NOT NULL,
            symbol          TEXT,
            regime          TEXT,
            wallet_tag      TEXT,
            session_id      TEXT,
            feature_version INT NOT NULL,
            features        JSONB NOT NULL DEFAULT '{}'::jsonb,
            raw             JSONB NOT NULL DEFAULT '{}'::jsonb
        );

        CREATE TABLE IF NOT EXISTS entry_outcomes (
            id           BIGSERIAL PRIMARY KEY,
            candidate_id BIGINT UNIQUE REFERENCES entry_candidates(id) ON DELETE CASCADE,
            label        TEXT NOT NULL,
            pnl_pct      DOUBLE PRECISION,
            hold_sec     INTEGER,
            notes        TEXT,
            extra        JSONB NOT NULL DEFAULT '{}'::jsonb,
            ts           TIMESTAMPTZ NOT NULL DEFAULT now()
        );

        CREATE TABLE IF NOT EXISTS trade_executions (
            id           BIGSERIAL PRIMARY KEY,
            ts           TIMESTAMPTZ NOT NULL DEFAULT now(),
            trade_id     TEXT NOT NULL,
            mint         TEXT NOT NULL,
            side         TEXT NOT NULL,
            qty_raw      NUMERIC,
            price_usd    DOUBLE PRECISION,
            slippage_bps DOUBLE PRECISION,
            fees_usd     DOUBLE PRECISION,
            tx_sig       TEXT,
            candidate_id BIGINT REFERENCES entry_candidates(id) ON DELETE SET NULL,
            session_id   TEXT,
            extra        JSONB NOT NULL DEFAULT '{}'::jsonb
        );

        CREATE TABLE IF NOT EXISTS exit_events (
            id           BIGSERIAL PRIMARY KEY,
            ts           TIMESTAMPTZ NOT NULL DEFAULT now(),
            event_key    TEXT UNIQUE,
            trade_id     TEXT,
            mint         TEXT NOT NULL,
            symbol       TEXT,
            action       TEXT,
            reason       TEXT,
            pnl_pct      DOUBLE PRECISION,
            pnl_usd      DOUBLE PRECISION,
            qty_raw      NUMERIC,
            price_usd    DOUBLE PRECISION,
            slippage_bps DOUBLE PRECISION,
            fees_usd     DOUBLE PRECISION,
            tx_sig       TEXT,
            candidate_id BIGINT REFERENCES entry_candidates(id) ON DELETE SET NULL,
            session_id   TEXT,
            extra        JSONB NOT NULL DEFAULT '{}'::jsonb
        );

        CREATE TABLE IF NOT EXISTS positions (
            id                 BIGSERIAL PRIMARY KEY,
            ts                 TIMESTAMPTZ NOT NULL DEFAULT now(),
            position_id        TEXT,
            mint               TEXT NOT NULL,
            symbol             TEXT,
            status             TEXT NOT NULL DEFAULT 'OPEN',
            side               TEXT NOT NULL DEFAULT 'LONG',
            qty_raw            NUMERIC,
            entry_price_usd    DOUBLE PRECISION,
            entry_trade_id     TEXT,
            entry_tx_sig       TEXT,
            entry_candidate_id BIGINT REFERENCES entry_candidates(id) ON DELETE SET NULL,
            entry_ts           TIMESTAMPTZ,
            exit_price_usd     DOUBLE PRECISION,
            exit_trade_id      TEXT,
            exit_tx_sig        TEXT,
            exit_ts            TIMESTAMPTZ,
            pnl_pct            DOUBLE PRECISION,
            pnl_usd            DOUBLE PRECISION,
            session_id         TEXT,
            wallet_tag         TEXT,
            extra              JSONB NOT NULL DEFAULT '{}'::jsonb
        );

        CREATE TABLE IF NOT EXISTS position_events (
            id          BIGSERIAL PRIMARY KEY,
            ts          TIMESTAMPTZ NOT NULL DEFAULT now(),
            position_id TEXT,
            mint        TEXT NOT NULL,
            event_type  TEXT NOT NULL,
            payload     JSONB NOT NULL DEFAULT '{}'::jsonb,
            session_id  TEXT,
            extra       JSONB NOT NULL DEFAULT '{}'::jsonb
        );

        CREATE TABLE IF NOT EXISTS regime_snapshots (
            id         BIGSERIAL PRIMARY KEY,
            ts         TIMESTAMPTZ NOT NULL DEFAULT now(),
            regime     TEXT NOT NULL,
            state      JSONB NOT NULL DEFAULT '{}'::jsonb,
            session_id TEXT,
            extra      JSONB NOT NULL DEFAULT '{}'::jsonb
        );

        CREATE TABLE IF NOT EXISTS ai_events (
            id             BIGSERIAL PRIMARY KEY,
            ts             TIMESTAMPTZ NOT NULL DEFAULT now(),
            event_type     TEXT NOT NULL,
            policy_version TEXT,
            experiment_tag TEXT,
            src            TEXT,
            regime         TEXT,
            payload        JSONB NOT NULL DEFAULT '{}'::jsonb
        );

        CREATE TABLE IF NOT EXISTS strategy_params (
            id         BIGSERIAL PRIMARY KEY,
            ts         TIMESTAMPTZ NOT NULL DEFAULT now(),
            key        TEXT NOT NULL,
            value      JSONB NOT NULL DEFAULT '{}'::jsonb,
            src        TEXT,
            regime     TEXT,
            session_id TEXT,
            extra      JSONB NOT NULL DEFAULT '{}'::jsonb
        );

        CREATE TABLE IF NOT EXISTS risk_assessments (
            id         BIGSERIAL PRIMARY KEY,
            ts         TIMESTAMPTZ NOT NULL DEFAULT now(),
            mint       TEXT NOT NULL,
            symbol     TEXT,
            score      DOUBLE PRECISION,
            verdict    TEXT,
            details    JSONB NOT NULL DEFAULT '{}'::jsonb,
            model_tag  TEXT,
            session_id TEXT,
            extra      JSONB NOT NULL DEFAULT '{}'::jsonb
        );

        CREATE UNIQUE INDEX IF NOT EXISTS uq_trade_executions_trade_side_mint
        ON trade_executions (trade_id, side, mint);

        CREATE UNIQUE INDEX IF NOT EXISTS uq_exit_events_event_key
        ON exit_events (event_key);

        CREATE UNIQUE INDEX IF NOT EXISTS uq_positions_position_id
        ON positions (position_id);

        CREATE INDEX IF NOT EXISTS idx_entry_candidates_ts ON entry_candidates (ts DESC);
        CREATE INDEX IF NOT EXISTS idx_entry_candidates_mint_ts ON entry_candidates (mint, ts DESC);
        CREATE INDEX IF NOT EXISTS idx_entry_outcomes_ts ON entry_outcomes (ts DESC);
        CREATE INDEX IF NOT EXISTS idx_trade_executions_ts ON trade_executions (ts DESC);
        CREATE INDEX IF NOT EXISTS idx_exit_events_ts ON exit_events (ts DESC);
        CREATE INDEX IF NOT EXISTS idx_positions_ts ON positions (ts DESC);

        -- Safe numeric expression indexes (no cast errors)
        CREATE INDEX IF NOT EXISTS idx_entry_candidates_decision_score_safe
        ON entry_candidates (
            (CASE
                WHEN (raw->'_decision_meta'->>'decision_score') ~ '^-?[0-9]+([.][0-9]+)?$'
                THEN (raw->'_decision_meta'->>'decision_score')::double precision
                ELSE NULL
            END)
        );

        CREATE INDEX IF NOT EXISTS idx_entry_outcomes_max_dd_pct_safe
        ON entry_outcomes (
            (CASE
                WHEN (extra->>'max_dd_pct') ~ '^-?[0-9]+([.][0-9]+)?$'
                THEN (extra->>'max_dd_pct')::double precision
                ELSE NULL
            END)
        );
        """,
        ),
    ),
]

# Contract tests often want: list[str] SQL blobs
MIGRATIONS: list[str] = []
for _m in MIGRATION_PLAN:
    _sql_blob = _migration_sql(_m)
    if _sql_blob:
        MIGRATIONS.append(_sql_blob)

Row = dict[str, Any]

# =============================================================================
# Optional imports (graceful) — psycopg2 OR psycopg3 OR asyncpg
# =============================================================================
_PG_DRIVER: str = "none"
_PSYCOPG2_OK = False
_PSYCOPG3_OK = False

psycopg2: Any | None = None
RealDictCursor: Any | None = None
_execute_values: Callable[..., Any] | None = None
register_default_json: Callable[..., Any] | None = None
register_default_jsonb: Callable[..., Any] | None = None

psycopg: Any | None = None
dict_row: Any | None = None

try:
    import psycopg2  # type: ignore
    from psycopg2.extras import (  # type: ignore
        RealDictCursor,
        execute_values as _execute_values,
        register_default_json,
        register_default_jsonb,
    )

    _PSYCOPG2_OK = True
    _PG_DRIVER = "psycopg2"
except Exception:  # pragma: no cover
    _PSYCOPG2_OK = False

try:
    import psycopg  # type: ignore
    from psycopg.rows import dict_row  # type: ignore

    _PSYCOPG3_OK = True
    if _PG_DRIVER == "none":
        _PG_DRIVER = "psycopg3"
except Exception:  # pragma: no cover
    _PSYCOPG3_OK = False

_ASYNCPG_OK = True
try:
    import asyncpg  # type: ignore
except Exception:  # pragma: no cover
    _ASYNCPG_OK = False
    asyncpg = None  # type: ignore


def _sync_pg_ok() -> bool:
    return bool(_PSYCOPG2_OK or _PSYCOPG3_OK)


# =============================================================================
# Env helpers (never raise)
# =============================================================================
def _env_str(key: str, default: str = "") -> str:
    v = os.getenv(key)
    return default if v is None else str(v)


def _env_int(key: str, default: int) -> int:
    raw = os.getenv(key)
    if raw is None or not str(raw).strip():
        return int(default)
    try:
        return int(str(raw).strip())
    except Exception:
        return int(default)


def _env_float(key: str, default: float) -> float:
    raw = os.getenv(key)
    if raw is None or not str(raw).strip():
        return float(default)
    try:
        return float(str(raw).strip())
    except Exception:
        return float(default)


def _env_bool(key: str, default: bool = False) -> bool:
    raw = os.getenv(key)
    if raw is None:
        return bool(default)
    s = str(raw).strip().lower()
    if s in ("1", "true", "yes", "y", "on"):
        return True
    if s in ("0", "false", "no", "n", "off"):
        return False
    return bool(default)


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _json_dumps(o: Any) -> str:
    return json.dumps(o, separators=(",", ":"), ensure_ascii=False, default=str)


def _ensure_obj(v: Any) -> Any:
    """Normalize json/jsonb-ish values to dict/list when possible. Never raises."""
    if v is None:
        return {}
    if isinstance(v, (dict, list)):
        return v
    if isinstance(v, (bytes, bytearray, memoryview)):
        try:
            v = bytes(v).decode("utf-8", "ignore")
        except Exception:
            return {}
    if isinstance(v, str):
        s = v.strip()
        if not s:
            return {}
        try:
            return json.loads(s)
        except Exception:
            return {}
    return v


def _as_json_object(v: Any, *, default: Mapping[str, Any] | None = None) -> Mapping[str, Any]:
    """Best-effort coercion to Mapping[str, Any] for jsonb inserts. Never raises."""
    if default is None:
        default = {}
    if v is None:
        return default
    if isinstance(v, Mapping):
        return v
    if isinstance(v, (bytes, bytearray, memoryview)):
        try:
            s = bytes(v).decode("utf-8", "ignore").strip()
            if not s:
                return default
            parsed = json.loads(s)
            return cast(Mapping[str, Any], parsed) if isinstance(parsed, Mapping) else default
        except Exception:
            return default
    if isinstance(v, str):
        s = v.strip()
        if not s:
            return default
        try:
            parsed = json.loads(s)
            return cast(Mapping[str, Any], parsed) if isinstance(parsed, Mapping) else default
        except Exception:
            return default
    return default


def _as_json_map(val: Any) -> dict[str, Any]:
    """Coerce unknown input into a JSON-serializable dict."""
    if val is None:
        return {}
    if isinstance(val, Mapping):
        return dict(_as_json_object(val))
    return {}


# =============================================================================
# Config
# =============================================================================
@dataclass(frozen=True)
class _DBCfg:
    enabled: bool
    autodisable_if_no_dsn: bool
    autodisable_if_no_driver: bool
    read_only: bool
    soft_fail_writes: bool
    soft_fail_reads: bool

    app_name: str
    connect_timeout_s: int
    connect_retries: int
    connect_retry_base_s: float
    connect_retry_max_s: float

    statement_timeout_ms: int
    idle_in_tx_timeout_ms: int
    lock_timeout_ms: int

    render_sslmode: str
    internal_sslmode: str
    force_sslmode: str | None

    migrations_enabled: bool
    use_migration_lock: bool
    migration_lock_id: int
    migration_lock_timeout_s: int

    default_src: str | None
    default_regime: str | None
    default_wallet_tag: str | None
    min_feature_version: int | None
    win_labels: list[str]

    ssl_insecure: bool
    ssl_ca_file: str | None

    use_asyncpg_pool: bool
    asyncpg_pool_min_size: int
    asyncpg_pool_max_size: int
    asyncpg_pool_max_inactive_s: int


_DSN_ENV_PRIMARY = "SNIPER_DB_DSN"
_DSN_ENV_CANDIDATES: list[str] = [
    _DSN_ENV_PRIMARY,
    "PNL_DSN",
    "SNIPER_DB_URL",
    "DATABASE_URL",
    "POSTGRES_DSN",
    "POSTGRES_URL",
    "PG_DSN",
    "PG_DSN_INTERNAL",
    "PG_DSN_EXTERNAL",
    "RENDER_INTERNAL_DATABASE_URL",
    "RENDER_DATABASE_URL",
]
DB_URL_FALLBACK = _env_str("SNIPER_DB_DSN_FALLBACK", "").strip()


def _has_any_dsn_env() -> bool:
    for k in _DSN_ENV_CANDIDATES:
        v = os.environ.get(k)
        if v and str(v).strip():
            return True
    return bool(DB_URL_FALLBACK)


def _load_cfg() -> _DBCfg:
    enabled = _env_bool("AI_DB_ENABLED", True)
    autodisable_if_no_dsn = _env_bool("AI_DB_AUTODISABLE_IF_NO_DSN", True)
    autodisable_if_no_driver = _env_bool("AI_DB_AUTODISABLE_IF_NO_DRIVER", True)
    read_only = _env_bool("AI_DB_READ_ONLY", False)

    soft_fail_writes = _env_bool("AI_DB_SOFT_FAIL_WRITES", True)
    soft_fail_reads = _env_bool("AI_DB_SOFT_FAIL_READS", True)

    app_name = _env_str("AI_DB_APP_NAME", "sniper_ai_db").strip() or "sniper_ai_db"
    connect_timeout_s = _env_int("AI_DB_CONNECT_TIMEOUT_S", 10)

    connect_retries = max(0, min(_env_int("AI_DB_CONNECT_RETRIES", 2), 10))
    connect_retry_base_s = max(0.05, min(_env_float("AI_DB_CONNECT_RETRY_BASE_S", 0.35), 10.0))
    connect_retry_max_s = max(
        connect_retry_base_s, min(_env_float("AI_DB_CONNECT_RETRY_MAX_S", 2.5), 30.0)
    )

    statement_timeout_ms = _env_int("AI_DB_STATEMENT_TIMEOUT_MS", 0)
    idle_in_tx_timeout_ms = _env_int("AI_DB_IDLE_IN_TX_TIMEOUT_MS", 0)
    lock_timeout_ms = _env_int("AI_DB_LOCK_TIMEOUT_MS", 0)

    render_sslmode = _env_str("AI_DB_RENDER_SSLMODE", "require").strip().lower() or "require"
    internal_sslmode = _env_str("AI_DB_INTERNAL_SSLMODE", "prefer").strip().lower() or "prefer"
    force_sslmode = _env_str("AI_DB_FORCE_SSLMODE", "").strip().lower() or None

    migrations_enabled = _env_bool("AI_DB_MIGRATIONS_ENABLED", True)
    use_migration_lock = _env_bool("AI_DB_USE_MIGRATION_LOCK", True)
    migration_lock_id = int(_env_str("AI_DB_MIGRATION_LOCK_ID", "907331").strip() or "907331")
    migration_lock_timeout_s = _env_int("AI_DB_MIGRATION_LOCK_TIMEOUT_S", 20)

    default_src = _env_str("AI_DB_DEFAULT_SRC", "").strip() or None
    default_regime = _env_str("AI_DB_DEFAULT_REGIME", "").strip() or None
    default_wallet_tag = _env_str("AI_DB_DEFAULT_WALLET_TAG", "").strip() or None

    min_feat_raw = _env_str("AI_DB_MIN_FEATURE_VERSION", "").strip()
    if min_feat_raw:
        try:
            min_feature_version: int | None = int(min_feat_raw)
        except Exception:
            log.warning("AI_DB_MIN_FEATURE_VERSION not valid int: %r", min_feat_raw)
            min_feature_version = None
    else:
        min_feature_version = None

    win_env = _env_str("AI_DB_WIN_LABELS", "").strip()
    if win_env:
        win_labels = [s.strip().lower() for s in win_env.split(",") if s.strip()]
    else:
        win_labels = ["good", "win", "green", "profit"]

    ssl_insecure = _env_bool("AI_DB_SSL_INSECURE", False) or _env_bool("PG_SSL_INSECURE", False)
    ssl_ca_file = _env_str("AI_DB_SSL_CA_FILE", "").strip() or None

    use_asyncpg_pool = _env_bool("AI_DB_USE_ASYNCPG_POOL", False)
    asyncpg_pool_min_size = max(1, min(_env_int("AI_DB_ASYNCPG_POOL_MIN_SIZE", 1), 32))
    asyncpg_pool_max_size = max(
        asyncpg_pool_min_size, min(_env_int("AI_DB_ASYNCPG_POOL_MAX_SIZE", 4), 64)
    )
    asyncpg_pool_max_inactive_s = max(
        5, min(_env_int("AI_DB_ASYNCPG_POOL_MAX_INACTIVE_S", 60), 3600)
    )

    if enabled and autodisable_if_no_dsn and not _has_any_dsn_env():
        if log.every("ai_db_no_dsn_autodisable", 1_000_000):
            log.warning(
                "ai_db: enabled but no DSN env found; auto-disabling "
                "(set AI_DB_AUTODISABLE_IF_NO_DSN=0 to override)"
            )
        enabled = False

    if enabled and autodisable_if_no_driver and (not _sync_pg_ok()) and (not _ASYNCPG_OK):
        if log.every("ai_db_no_driver_autodisable", 1_000_000):
            log.warning(
                "ai_db: enabled but no sync driver (psycopg2/psycopg) and asyncpg missing; "
                "auto-disabling (install psycopg[binary] and/or asyncpg, or set "
                "AI_DB_AUTODISABLE_IF_NO_DRIVER=0)"
            )
        enabled = False
    elif enabled and autodisable_if_no_driver and (not _sync_pg_ok()) and _ASYNCPG_OK:
        if log.every("ai_db_no_sync_driver_warn", 1_000_000):
            log.warning(
                "ai_db: no sync Postgres driver (psycopg2/psycopg). Sync helpers may soft-fail. "
                "Install psycopg[binary] or psycopg2-binary."
            )

    return _DBCfg(
        enabled=enabled,
        autodisable_if_no_dsn=autodisable_if_no_dsn,
        autodisable_if_no_driver=autodisable_if_no_driver,
        read_only=read_only,
        soft_fail_writes=soft_fail_writes,
        soft_fail_reads=soft_fail_reads,
        app_name=app_name,
        connect_timeout_s=connect_timeout_s,
        connect_retries=connect_retries,
        connect_retry_base_s=connect_retry_base_s,
        connect_retry_max_s=connect_retry_max_s,
        statement_timeout_ms=statement_timeout_ms,
        idle_in_tx_timeout_ms=idle_in_tx_timeout_ms,
        lock_timeout_ms=lock_timeout_ms,
        render_sslmode=render_sslmode,
        internal_sslmode=internal_sslmode,
        force_sslmode=force_sslmode,
        migrations_enabled=migrations_enabled,
        use_migration_lock=use_migration_lock,
        migration_lock_id=migration_lock_id,
        migration_lock_timeout_s=migration_lock_timeout_s,
        default_src=default_src,
        default_regime=default_regime,
        default_wallet_tag=default_wallet_tag,
        min_feature_version=min_feature_version,
        win_labels=win_labels,
        ssl_insecure=ssl_insecure,
        ssl_ca_file=ssl_ca_file,
        use_asyncpg_pool=use_asyncpg_pool,
        asyncpg_pool_min_size=asyncpg_pool_min_size,
        asyncpg_pool_max_size=asyncpg_pool_max_size,
        asyncpg_pool_max_inactive_s=asyncpg_pool_max_inactive_s,
    )


CFG = _load_cfg()

AI_DB_ENABLED = CFG.enabled
AI_DB_READ_ONLY = CFG.read_only
AI_DB_SOFT_FAIL_WRITES = CFG.soft_fail_writes
AI_DB_SOFT_FAIL_READS = CFG.soft_fail_reads

AI_DB_APP_NAME = CFG.app_name
AI_DB_CONNECT_TIMEOUT_S = CFG.connect_timeout_s
AI_DB_CONNECT_RETRIES = CFG.connect_retries
AI_DB_CONNECT_RETRY_BASE_S = CFG.connect_retry_base_s
AI_DB_CONNECT_RETRY_MAX_S = CFG.connect_retry_max_s

AI_DB_STATEMENT_TIMEOUT_MS = CFG.statement_timeout_ms
AI_DB_IDLE_IN_TX_TIMEOUT_MS = CFG.idle_in_tx_timeout_ms
AI_DB_LOCK_TIMEOUT_MS = CFG.lock_timeout_ms

AI_DB_RENDER_SSLMODE = CFG.render_sslmode
AI_DB_INTERNAL_SSLMODE = CFG.internal_sslmode
AI_DB_FORCE_SSLMODE = CFG.force_sslmode

AI_DB_MIGRATIONS_ENABLED = CFG.migrations_enabled
AI_DB_USE_MIGRATION_LOCK = CFG.use_migration_lock
AI_DB_MIGRATION_LOCK_ID = CFG.migration_lock_id
AI_DB_MIGRATION_LOCK_TIMEOUT_S = CFG.migration_lock_timeout_s

AI_DB_DEFAULT_SRC = CFG.default_src
AI_DB_DEFAULT_REGIME = CFG.default_regime
AI_DB_DEFAULT_WALLET_TAG = CFG.default_wallet_tag
AI_DB_MIN_FEATURE_VERSION = CFG.min_feature_version
WIN_LABELS: list[str] = list(CFG.win_labels)

AI_DB_SSL_INSECURE = CFG.ssl_insecure
AI_DB_SSL_CA_FILE = CFG.ssl_ca_file

AI_DB_USE_ASYNCPG_POOL = CFG.use_asyncpg_pool
AI_DB_ASYNCPG_POOL_MIN_SIZE = CFG.asyncpg_pool_min_size
AI_DB_ASYNCPG_POOL_MAX_SIZE = CFG.asyncpg_pool_max_size
AI_DB_ASYNCPG_POOL_MAX_INACTIVE_S = CFG.asyncpg_pool_max_inactive_s


# =============================================================================
# DSN resolution / normalization + SSL inference
# =============================================================================
@dataclass(frozen=True)
class DSNInfo:
    source: str
    raw: str
    normalized_libpq: str
    normalized_asyncpg: str
    normalized_sqlalchemy_async: str
    host: str | None
    db: str | None
    sslmode: str | None
    is_render_external: bool
    is_render_internal: bool


_NORMALIZED: DSNInfo | None = None


def reset_dsn_cache() -> None:
    global _NORMALIZED
    _NORMALIZED = None


def _effective_min_feature_version(explicit: int | None) -> int | None:
    return explicit if explicit is not None else AI_DB_MIN_FEATURE_VERSION


def _looks_like_internal_render_host(host: str | None) -> bool:
    if not host:
        return False
    h = host.lower()
    return (h.startswith("dpg-") and "." not in h) or (".internal" in h) or h.endswith(".internal")


def _is_render_external_host(host: str | None) -> bool:
    if not host:
        return False
    h = host.lower()
    return ("postgres.render.com" in h) or h.endswith(".render.com") or ("render.com" in h)


def _drop_query_key(q: str, key: str) -> str:
    q2 = re.sub(rf"(^|&){re.escape(key)}=[^&]*", "", q).strip("&")
    q2 = re.sub(r"&&+", "&", q2).strip("&")
    return q2


def _parse_kv_dsn(raw: str) -> dict[str, str]:
    s = (raw or "").strip()
    if not s:
        return {}
    parts = shlex.split(s)
    out: dict[str, str] = {}
    for token in parts:
        if "=" not in token:
            continue
        k, v = token.split("=", 1)
        k = k.strip()
        v = v.strip()
        if k:
            out[k] = v
    return out


def _mask_dsn(raw: str) -> str:
    raw = (raw or "").strip()
    if not raw:
        return "<empty>"
    if "://" not in raw:
        try:
            kv = _parse_kv_dsn(raw)
            host = kv.get("host") or "<host>"
            port = f":{kv.get('port')}" if kv.get("port") else ""
            db = kv.get("dbname") or kv.get("database") or "<db>"
            return f"{host}{port}/{db}"
        except Exception:
            return "<kv-dsn>"
    try:
        import urllib.parse as urlparse

        p = urlparse.urlparse(raw)
        host = p.hostname or "<host>"
        port = f":{p.port}" if p.port else ""
        db = (p.path or "").lstrip("/") or "<db>"
        return f"{host}{port}/{db}"
    except Exception:
        return "<dsn>"


def _kv_to_url(kv: Mapping[str, str]) -> str:
    import urllib.parse as urlparse

    host = kv.get("host") or kv.get("hostname") or ""
    db = kv.get("dbname") or kv.get("database") or ""
    user = kv.get("user") or ""
    password = kv.get("password") or ""
    port = kv.get("port") or ""

    if not host or not db:
        raise RuntimeError("kv DSN missing host/dbname; cannot convert to URL")

    userinfo = ""
    if user:
        userinfo = urlparse.quote(user, safe="")
        if password:
            userinfo += ":" + urlparse.quote(password, safe="")
        userinfo += "@"

    netloc = f"{userinfo}{host}"
    if port:
        netloc += f":{port}"

    q: list[str] = []
    for k in ("sslmode", "application_name", "connect_timeout"):
        if kv.get(k):
            q.append(f"{k}={urlparse.quote(str(kv[k]), safe='')}")
    query = "&".join(q)
    return f"postgresql://{netloc}/{urlparse.quote(db, safe='')}" + (f"?{query}" if query else "")


def _normalize_kv_dsn(raw: str) -> tuple[str, str, str | None, str | None, str | None, bool, bool]:
    s = (raw or "").strip()
    if not s:
        raise RuntimeError("Empty DB DSN")

    kv = _parse_kv_dsn(s)
    host = kv.get("host") or kv.get("hostname")
    db = kv.get("dbname") or kv.get("database")

    is_ext = _is_render_external_host(host)
    is_int = _looks_like_internal_render_host(host)

    sslmode = kv.get("sslmode")
    if AI_DB_FORCE_SSLMODE:
        kv["sslmode"] = AI_DB_FORCE_SSLMODE
        sslmode = AI_DB_FORCE_SSLMODE
    else:
        if not sslmode:
            if is_ext:
                kv["sslmode"] = AI_DB_RENDER_SSLMODE
                sslmode = AI_DB_RENDER_SSLMODE
            elif is_int:
                kv["sslmode"] = AI_DB_INTERNAL_SSLMODE
                sslmode = AI_DB_INTERNAL_SSLMODE

    if not kv.get("application_name"):
        kv["application_name"] = AI_DB_APP_NAME
    if not kv.get("connect_timeout"):
        kv["connect_timeout"] = str(int(AI_DB_CONNECT_TIMEOUT_S))

    ordered_keys = [
        "host",
        "port",
        "dbname",
        "user",
        "password",
        "sslmode",
        "application_name",
        "connect_timeout",
    ]
    seen = set(ordered_keys)
    tail = [k for k in kv if k not in seen]
    keys = ordered_keys + sorted(tail)

    def _q(v: str) -> str:
        if re.search(r"\s", v):
            return "'" + v.replace("'", "\\'") + "'"
        return v

    libpq = " ".join([f"{k}={_q(str(kv[k]))}" for k in keys if k in kv and str(kv[k]).strip()])
    url = _kv_to_url(kv)
    return libpq, url, host, db, sslmode, is_ext, is_int


def _normalize_url_dsn(raw: str) -> tuple[str, str | None, str | None, str | None, bool, bool]:
    import urllib.parse as urlparse

    raw = (raw or "").strip()
    if not raw:
        raise RuntimeError("Empty DB DSN")

    parsed = urlparse.urlparse(raw)

    scheme = parsed.scheme or "postgresql"
    if "+" in scheme:
        scheme = scheme.split("+", 1)[0]
    if scheme == "postgres":
        scheme = "postgresql"
    if not scheme.startswith("postgres"):
        scheme = "postgresql"

    host = getattr(parsed, "hostname", None)
    db = (parsed.path or "").lstrip("/") or None
    is_ext = _is_render_external_host(host)
    is_int = _looks_like_internal_render_host(host)

    query = parsed.query or ""
    sslmode: str | None = None
    m = re.search(r"(^|&)sslmode=([^&]+)", query)
    if m:
        sslmode = (m.group(2) or "").strip() or None

    if AI_DB_FORCE_SSLMODE:
        query = _drop_query_key(query, "sslmode")
        query = (query + "&" if query else "") + f"sslmode={AI_DB_FORCE_SSLMODE}"
        sslmode = AI_DB_FORCE_SSLMODE
    else:
        if sslmode is None:
            if is_ext:
                sslmode = AI_DB_RENDER_SSLMODE
                query = (query + "&" if query else "") + f"sslmode={AI_DB_RENDER_SSLMODE}"
            elif is_int:
                sslmode = AI_DB_INTERNAL_SSLMODE
                query = (query + "&" if query else "") + f"sslmode={AI_DB_INTERNAL_SSLMODE}"

    if re.search(r"(^|&)application_name=", query) is None:
        query = (query + "&" if query else "") + f"application_name={AI_DB_APP_NAME}"
    if re.search(r"(^|&)connect_timeout=", query) is None:
        query = (query + "&" if query else "") + f"connect_timeout={int(AI_DB_CONNECT_TIMEOUT_S)}"

    normalized = urlparse.urlunparse((scheme, parsed.netloc, parsed.path, "", query, ""))
    return normalized, host, db, sslmode, is_ext, is_int


def _normalize_dsn(raw: str) -> DSNInfo:
    raw = (raw or "").strip()
    if not raw:
        raise RuntimeError("Empty DB DSN")

    if "://" not in raw:
        libpq, url, host, db, sslmode, is_ext, is_int = _normalize_kv_dsn(raw)
        normalized_sqlalchemy = url.replace("postgresql://", "postgresql+asyncpg://", 1)
        return DSNInfo(
            source="<kv>",
            raw=raw,
            normalized_libpq=libpq,
            normalized_asyncpg=url,
            normalized_sqlalchemy_async=normalized_sqlalchemy,
            host=host,
            db=db,
            sslmode=sslmode,
            is_render_external=is_ext,
            is_render_internal=is_int,
        )

    libpq_url, host, db, sslmode, is_ext, is_int = _normalize_url_dsn(raw)
    normalized_sqlalchemy = libpq_url.replace("postgresql://", "postgresql+asyncpg://", 1)
    return DSNInfo(
        source="<url>",
        raw=raw,
        normalized_libpq=libpq_url,
        normalized_asyncpg=libpq_url,
        normalized_sqlalchemy_async=normalized_sqlalchemy,
        host=host,
        db=db,
        sslmode=sslmode,
        is_render_external=is_ext,
        is_render_internal=is_int,
    )


def _is_render_runtime() -> bool:
    v = os.getenv("RENDER")
    if v and str(v).strip().lower() in ("1", "true", "yes", "y", "on"):
        return True
    for k in (
        "RENDER_SERVICE_ID",
        "RENDER_INSTANCE_ID",
        "RENDER_EXTERNAL_URL",
        "RENDER_INTERNAL_HOSTNAME",
        "RENDER_GIT_COMMIT",
    ):
        if os.getenv(k):
            return True
    return False


def _peek_host_from_dsn(raw: str) -> str | None:
    raw = (raw or "").strip()
    if not raw:
        return None
    if "://" in raw:
        try:
            import urllib.parse as urlparse

            return urlparse.urlparse(raw).hostname
        except Exception:
            return None
    try:
        kv = _parse_kv_dsn(raw)
        return kv.get("host") or kv.get("hostname")
    except Exception:
        return None


def _resolve_raw_dsn() -> tuple[str, str]:
    candidates: list[tuple[str, str]] = []
    for key in _DSN_ENV_CANDIDATES:
        v = os.environ.get(key)
        if v and str(v).strip():
            candidates.append((key, str(v).strip()))
    if DB_URL_FALLBACK:
        candidates.append(("SNIPER_DB_DSN_FALLBACK", DB_URL_FALLBACK))

    if not candidates:
        raise RuntimeError(
            "No Postgres DSN found. Set SNIPER_DB_DSN (recommended) or one of: "
            f"{_DSN_ENV_CANDIDATES[1:]} or SNIPER_DB_DSN_FALLBACK."
        )

    on_render = _is_render_runtime()

    if on_render:
        for key, raw in candidates:
            host = _peek_host_from_dsn(raw)
            if _looks_like_internal_render_host(host):
                return raw, key
        return candidates[0][1], candidates[0][0]

    first_internal: tuple[str, str] | None = None
    for key, raw in candidates:
        host = _peek_host_from_dsn(raw)
        if _looks_like_internal_render_host(host):
            if first_internal is None:
                first_internal = (key, raw)
            continue
        return raw, key

    if first_internal is not None:
        return first_internal[1], first_internal[0]

    return candidates[0][1], candidates[0][0]


def resolve_dsn_info() -> DSNInfo:
    global _NORMALIZED
    if _NORMALIZED is not None:
        return _NORMALIZED

    raw, source = _resolve_raw_dsn()
    info = _normalize_dsn(raw)
    info = dataclasses.replace(info, source=source)
    _NORMALIZED = info

    if (not _is_render_runtime()) and info.is_render_internal:
        log.warning(
            "ai_db: selected DSN host looks like Render INTERNAL (%s). "
            "That hostname will not resolve outside Render. "
            "Set an EXTERNAL DSN in SNIPER_DB_DSN (preferred) or PG_DSN_EXTERNAL / RENDER_DATABASE_URL.",
            info.host,
        )

    log.info("ai_db: using DSN from %s (masked): %s", source, _mask_dsn(info.normalized_libpq))
    return info


def get_normalized_dsn_for_debug() -> str:
    try:
        return _mask_dsn(resolve_dsn_info().normalized_libpq)
    except Exception as e:
        return f"<unresolved: {e}>"


def describe_dsn_sources() -> dict[str, Any]:
    candidates: dict[str, str] = {}
    keys = _DSN_ENV_CANDIDATES + (["SNIPER_DB_DSN_FALLBACK"] if DB_URL_FALLBACK else [])
    for key in keys:
        raw = os.environ.get(key) if key != "SNIPER_DB_DSN_FALLBACK" else DB_URL_FALLBACK
        if raw and str(raw).strip():
            candidates[key] = _mask_dsn(str(raw).strip())

    selected = "<unresolved>"
    selected_source: str | None = None
    sslmode: str | None = None
    host: str | None = None
    try:
        info = resolve_dsn_info()
        selected = _mask_dsn(info.normalized_libpq)
        selected_source = info.source
        sslmode = info.sslmode
        host = info.host
    except Exception:
        pass

    return {
        "enabled": bool(AI_DB_ENABLED),
        "read_only": bool(AI_DB_READ_ONLY),
        "selected_source": selected_source,
        "selected": selected,
        "host": host,
        "sslmode": sslmode,
        "candidates": candidates,
        "migrations_enabled": bool(AI_DB_MIGRATIONS_ENABLED),
        "psycopg2_ok": bool(_PSYCOPG2_OK),
        "psycopg3_ok": bool(_PSYCOPG3_OK),
        "asyncpg_ok": bool(_ASYNCPG_OK),
        "use_asyncpg_pool": bool(AI_DB_USE_ASYNCPG_POOL),
    }


# =============================================================================
# SSL helpers for asyncpg
# =============================================================================
def dsn_wants_ssl(dsn: str | None = None) -> bool:
    try:
        info = resolve_dsn_info() if not dsn else _normalize_dsn(dsn)
        sm = (info.sslmode or "").lower().strip()
        if sm == "disable":
            return False
        if sm in {"require", "verify-ca", "verify-full"}:
            return True
        if info.is_render_external or info.is_render_internal:
            return True
    except Exception:
        pass
    return False


def ssl_context_for_dsn(dsn: str | None = None) -> ssl.SSLContext | None:
    if not dsn_wants_ssl(dsn):
        return None

    ctx = ssl.create_default_context()

    if AI_DB_SSL_CA_FILE:
        try:
            ctx.load_verify_locations(cafile=AI_DB_SSL_CA_FILE)
        except Exception as e:
            log.warning("ai_db: failed to load AI_DB_SSL_CA_FILE=%r: %s", AI_DB_SSL_CA_FILE, e)

    if AI_DB_SSL_INSECURE:
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE

    return ctx


def _asyncpg_server_settings(statement_timeout_ms: int | None = None) -> dict[str, str]:
    stm = statement_timeout_ms if statement_timeout_ms is not None else AI_DB_STATEMENT_TIMEOUT_MS
    ss: dict[str, str] = {"application_name": AI_DB_APP_NAME, "timezone": "UTC"}
    if stm and int(stm) > 0:
        ss["statement_timeout"] = str(int(stm))
    if AI_DB_IDLE_IN_TX_TIMEOUT_MS and int(AI_DB_IDLE_IN_TX_TIMEOUT_MS) > 0:
        ss["idle_in_transaction_session_timeout"] = str(int(AI_DB_IDLE_IN_TX_TIMEOUT_MS))
    if AI_DB_LOCK_TIMEOUT_MS and int(AI_DB_LOCK_TIMEOUT_MS) > 0:
        ss["lock_timeout"] = str(int(AI_DB_LOCK_TIMEOUT_MS))
    return ss


async def asyncpg_connect(
    dsn: str | None = None,
    *,
    timeout: float | None = None,
    statement_timeout_ms: int | None = None,
) -> Any:
    """Central asyncpg connector with correct TLS behavior for Render."""
    if not _ASYNCPG_OK:
        raise RuntimeError("asyncpg not installed. pip install asyncpg")
    if not AI_DB_ENABLED:
        raise RuntimeError("AI DB disabled (AI_DB_ENABLED=0)")

    info = resolve_dsn_info() if dsn is None else _normalize_dsn(dsn)
    connect_timeout = float(timeout if timeout is not None else AI_DB_CONNECT_TIMEOUT_S)

    ssl_ctx = ssl_context_for_dsn(info.normalized_asyncpg)
    server_settings = _asyncpg_server_settings(statement_timeout_ms)

    last_err: BaseException | None = None
    attempts = 1 + int(AI_DB_CONNECT_RETRIES)

    for i in range(1, attempts + 1):
        try:
            conn = await asyncpg.connect(  # type: ignore[union-attr]
                info.normalized_asyncpg,
                timeout=connect_timeout,
                ssl=ssl_ctx,
                server_settings=server_settings,
            )
            with suppress(Exception):
                await conn.execute("SELECT 1")
            return conn
        except Exception as e:
            last_err = e
            if i >= attempts:
                break
            if log.every("ai_db_asyncpg_connect_retry", 10):
                log.warning("ai_db: asyncpg connect failed (attempt %s/%s): %s", i, attempts, e)
            base = float(AI_DB_CONNECT_RETRY_BASE_S)
            cap = float(AI_DB_CONNECT_RETRY_MAX_S)
            delay = min(cap, base * (2.0 ** max(0, i - 1)))
            delay *= 0.9 + 0.2 * random.random()
            await asyncio.sleep(delay)

    assert last_err is not None
    raise last_err


_ASYNCPG_POOL: Any = None


async def get_asyncpg_pool() -> Any:
    """Optional shared asyncpg pool (disabled by default)."""
    global _ASYNCPG_POOL
    if _ASYNCPG_POOL is not None:
        return _ASYNCPG_POOL

    if not _ASYNCPG_OK:
        raise RuntimeError("asyncpg not installed. pip install asyncpg")
    if not AI_DB_ENABLED:
        raise RuntimeError("AI DB disabled (AI_DB_ENABLED=0)")
    if not AI_DB_USE_ASYNCPG_POOL:
        raise RuntimeError("AI_DB_USE_ASYNCPG_POOL=0 (pool disabled)")

    info = resolve_dsn_info()
    ssl_ctx = ssl_context_for_dsn(info.normalized_asyncpg)
    server_settings = _asyncpg_server_settings(None)

    _ASYNCPG_POOL = await asyncpg.create_pool(  # type: ignore[union-attr]
        dsn=info.normalized_asyncpg,
        min_size=int(AI_DB_ASYNCPG_POOL_MIN_SIZE),
        max_size=int(AI_DB_ASYNCPG_POOL_MAX_SIZE),
        max_inactive_connection_lifetime=float(AI_DB_ASYNCPG_POOL_MAX_INACTIVE_S),
        timeout=float(AI_DB_CONNECT_TIMEOUT_S),
        ssl=ssl_ctx,
        server_settings=server_settings,
    )
    return _ASYNCPG_POOL


async def close_asyncpg_pool() -> None:
    global _ASYNCPG_POOL
    if _ASYNCPG_POOL is not None:
        with suppress(Exception):
            await _ASYNCPG_POOL.close()
        _ASYNCPG_POOL = None


# =============================================================================
# Sync driver connect helpers (psycopg2 OR psycopg3)
# =============================================================================
def _require_sync_pg() -> None:
    if _sync_pg_ok():
        return
    raise RuntimeError(
        "No sync Postgres driver is available. Install psycopg[binary] (recommended) "
        "or psycopg2-binary, or set AI_DB_ENABLED=0 to disable DB features."
    )


def _sync_connect(conninfo: str) -> _ConnLike:
    _require_sync_pg()
    if _PG_DRIVER == "psycopg2":
        return cast(_ConnLike, psycopg2.connect(conninfo))  # type: ignore[union-attr]
    if _PG_DRIVER == "psycopg3":
        return cast(_ConnLike, psycopg.connect(conninfo))  # type: ignore[union-attr]
    raise RuntimeError("No sync Postgres driver configured")


def _apply_session_settings(conn: _ConnLike) -> None:
    cur: _CursorLike | None = None
    try:
        cur = conn.cursor()
        cur.execute("SET application_name = %s", (AI_DB_APP_NAME,))
        cur.execute("SET TIME ZONE 'UTC'")
        if AI_DB_STATEMENT_TIMEOUT_MS > 0:
            cur.execute("SET statement_timeout = %s", (int(AI_DB_STATEMENT_TIMEOUT_MS),))
        if AI_DB_IDLE_IN_TX_TIMEOUT_MS > 0:
            cur.execute(
                "SET idle_in_transaction_session_timeout = %s",
                (int(AI_DB_IDLE_IN_TX_TIMEOUT_MS),),
            )
        if AI_DB_LOCK_TIMEOUT_MS > 0:
            cur.execute("SET lock_timeout = %s", (int(AI_DB_LOCK_TIMEOUT_MS),))
    except Exception:
        log.exception("ai_db: failed to apply session settings")
    finally:
        with suppress(Exception):
            if cur is not None:
                cur.close()


def _sleep_backoff(attempt: int) -> None:
    base = float(AI_DB_CONNECT_RETRY_BASE_S)
    cap = float(AI_DB_CONNECT_RETRY_MAX_S)
    delay = min(cap, base * (2.0 ** max(0, attempt - 1)))
    delay *= 0.9 + 0.2 * random.random()
    time.sleep(delay)


def _connect_once() -> _ConnLike:
    if not AI_DB_ENABLED:
        raise RuntimeError("AI DB disabled (AI_DB_ENABLED=0)")
    _require_sync_pg()

    info = resolve_dsn_info()
    conn = _sync_connect(info.normalized_libpq)

    if _PG_DRIVER == "psycopg2":
        try:
            if register_default_json is not None:
                register_default_json(conn, loads=json.loads)  # type: ignore[misc]
            if register_default_jsonb is not None:
                register_default_jsonb(conn, loads=json.loads)  # type: ignore[misc]
        except Exception:
            pass

    _apply_session_settings(conn)
    return conn


def _connect() -> _ConnLike:
    last_err: BaseException | None = None
    attempts = 1 + int(AI_DB_CONNECT_RETRIES)
    for i in range(1, attempts + 1):
        try:
            return _connect_once()
        except Exception as e:
            last_err = e
            if i >= attempts:
                break
            if log.every("ai_db_connect_retry", 10):
                log.warning("ai_db: connect failed (attempt %s/%s): %s", i, attempts, e)
            _sleep_backoff(i)
    assert last_err is not None
    raise last_err


@contextmanager
def conn_cursor() -> Iterator[tuple[_ConnLike, _CursorLike]]:
    conn = _connect()
    cur: _CursorLike | None = None
    try:
        cur = conn.cursor()
        yield conn, cur
        conn.commit()
    except Exception:
        with suppress(Exception):
            conn.rollback()
        log.exception("ai_db: DB error")
        raise
    finally:
        with suppress(Exception):
            if cur is not None:
                cur.close()
        with suppress(Exception):
            conn.close()


@contextmanager
def conn_dict_cursor() -> Iterator[tuple[_ConnLike, Any]]:
    conn = _connect()
    cur: Any = None
    try:
        if _PG_DRIVER == "psycopg2" and RealDictCursor is not None:
            cur = conn.cursor(cursor_factory=RealDictCursor)  # type: ignore[arg-type]
        elif _PG_DRIVER == "psycopg3" and dict_row is not None:
            cur = conn.cursor(row_factory=dict_row)  # type: ignore[misc]
        else:
            cur = conn.cursor()
        yield conn, cur
        conn.commit()
    except Exception:
        with suppress(Exception):
            conn.rollback()
        log.exception("ai_db: DB error")
        raise
    finally:
        with suppress(Exception):
            if cur is not None:
                cur.close()
        with suppress(Exception):
            conn.close()


@contextmanager
def cursor_only() -> Iterator[_CursorLike]:
    with conn_cursor() as (_c, cur):
        yield cur


@contextmanager
def dict_cursor_only() -> Iterator[Any]:
    with conn_dict_cursor() as (_c, cur):
        yield cur


# =============================================================================
# Soft-fail decorators
# =============================================================================
P = ParamSpec("P")
R = TypeVar("R")


def _default_copy(v: object) -> object:
    """Deepcopy if possible, otherwise return the same object. Never raises."""
    try:
        return copy.deepcopy(v)
    except Exception:
        return v


def _softfail_write(default: object) -> Callable[[Callable[P, R]], Callable[P, R]]:
    def deco(fn: Callable[P, R]) -> Callable[P, R]:
        @wraps(fn)
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
            if (not AI_DB_ENABLED) or AI_DB_READ_ONLY:
                return cast(R, _default_copy(default))
            try:
                return fn(*args, **kwargs)
            except Exception:
                if AI_DB_SOFT_FAIL_WRITES:
                    if log.every("ai_db_write_fail", 50):
                        log.exception("ai_db: write failed in %s (soft-fail)", fn.__name__)
                    return cast(R, _default_copy(default))
                raise

        return wrapper

    return deco


def _softfail_read(default: object) -> Callable[[Callable[P, R]], Callable[P, R]]:
    def deco(fn: Callable[P, R]) -> Callable[P, R]:
        @wraps(fn)
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
            if not AI_DB_ENABLED:
                return cast(R, _default_copy(default))
            try:
                return fn(*args, **kwargs)
            except Exception:
                if AI_DB_SOFT_FAIL_READS:
                    if log.every("ai_db_read_fail", 50):
                        log.exception("ai_db: read failed in %s (soft-fail)", fn.__name__)
                    return cast(R, _default_copy(default))
                raise

        return wrapper

    return deco


# =============================================================================
# Migration lock + safe SQL helpers
# =============================================================================
@contextmanager
def _migration_lock(cur: _CursorLike) -> Iterator[None]:
    if not AI_DB_USE_MIGRATION_LOCK:
        yield
        return

    lock_id = int(AI_DB_MIGRATION_LOCK_ID)
    deadline = time.time() + max(1, int(AI_DB_MIGRATION_LOCK_TIMEOUT_S))

    got = False
    try:
        while time.time() < deadline:
            cur.execute("SELECT pg_try_advisory_lock(%s)", (lock_id,))
            row = cur.fetchone()
            try:
                got = bool(row and bool(row[0]))
            except Exception:
                got = False
            if got:
                break
            time.sleep(0.25)

        if not got:
            log.warning("ai_db: migration lock timeout; proceeding without lock (idempotent)")

        yield
    finally:
        if got:
            with suppress(Exception):
                cur.execute("SELECT pg_advisory_unlock(%s)", (lock_id,))


_NUMERIC_RE_SQL = r"^-?[0-9]+([.][0-9]+)?$"


def _sql_safe_float(text_expr: str) -> str:
    return (
        f"(CASE WHEN ({text_expr}) ~ '{_NUMERIC_RE_SQL}' "
        f"THEN ({text_expr})::double precision ELSE NULL END)"
    )


# =============================================================================
# Phase 0: Health / simple utilities
# =============================================================================
def ping() -> bool:
    if not AI_DB_ENABLED or not _sync_pg_ok():
        return False
    try:
        with cursor_only() as cur:
            cur.execute("SELECT 1")
            row = cur.fetchone()
            return bool(row and int(row[0]) == 1)
    except Exception:
        log.exception("ai_db.ping() failed")
        return False


def describe_backend() -> str:
    dsn_mask = get_normalized_dsn_for_debug()
    if not AI_DB_ENABLED:
        return f"dsn={dsn_mask}, server=<disabled>, ping=False"
    if not _sync_pg_ok():
        return f"dsn={dsn_mask}, server=<no-sync-driver>, ping=False"

    version = "unknown"
    ok = False
    try:
        with cursor_only() as cur:
            cur.execute("SHOW server_version")
            row = cur.fetchone()
            if row and row[0]:
                version = str(row[0])
            cur.execute("SELECT 1")
            row2 = cur.fetchone()
            ok = bool(row2 and int(row2[0]) == 1)
    except Exception as e:
        log.warning("describe_backend: failed: %s", e)
        ok = False

    return f"dsn={dsn_mask}, server={version}, ping={ok}"


# =============================================================================
# Phase 1: Schema / migrations
# =============================================================================
AI_DB_SCHEMA_VERSION: int = SCHEMA_VERSION


def _exec_sql_blob(cur: Any, sql_blob: str) -> None:
    s = (sql_blob or "").strip()
    if not s:
        return
    cur.execute(s)


def ensure_ai_tables() -> None:
    """
    Create/upgrade schema idempotently.
    Safe to call repeatedly. Skips when disabled, read-only, or migrations disabled.
    """
    if not AI_DB_ENABLED or AI_DB_READ_ONLY or not AI_DB_MIGRATIONS_ENABLED:
        return
    if not _sync_pg_ok():
        log.warning(
            "ai_db.ensure_ai_tables: no sync driver; skipping migrations "
            "(install psycopg[binary] or psycopg2-binary)."
        )
        return

    with conn_cursor() as (_conn, cur):
        with _migration_lock(cur):
            for m in MIGRATION_PLAN:
                _exec_sql_blob(cur, _migration_sql(m))

            # Defensive add-if-missing (legacy upgrades)
            cur.execute("ALTER TABLE entry_candidates ADD COLUMN IF NOT EXISTS session_id TEXT;")
            cur.execute("ALTER TABLE entry_candidates ADD COLUMN IF NOT EXISTS wallet_tag TEXT;")

            cur.execute("ALTER TABLE trade_executions ADD COLUMN IF NOT EXISTS tx_sig TEXT;")
            cur.execute("ALTER TABLE trade_executions ADD COLUMN IF NOT EXISTS session_id TEXT;")

            cur.execute("ALTER TABLE exit_events ADD COLUMN IF NOT EXISTS event_key TEXT;")

            cur.execute("ALTER TABLE positions ADD COLUMN IF NOT EXISTS position_id TEXT;")
            cur.execute("ALTER TABLE positions ADD COLUMN IF NOT EXISTS status TEXT;")
            cur.execute("ALTER TABLE positions ADD COLUMN IF NOT EXISTS side TEXT;")
            cur.execute("ALTER TABLE positions ADD COLUMN IF NOT EXISTS entry_candidate_id BIGINT;")
            cur.execute("ALTER TABLE positions ADD COLUMN IF NOT EXISTS session_id TEXT;")
            cur.execute("ALTER TABLE positions ADD COLUMN IF NOT EXISTS wallet_tag TEXT;")
            cur.execute(
                "ALTER TABLE positions ADD COLUMN IF NOT EXISTS extra JSONB NOT NULL DEFAULT '{}'::jsonb;"
            )

            cur.execute(
                "ALTER TABLE position_events ADD COLUMN IF NOT EXISTS payload JSONB NOT NULL DEFAULT '{}'::jsonb;"
            )
            cur.execute("ALTER TABLE position_events ADD COLUMN IF NOT EXISTS session_id TEXT;")
            cur.execute(
                "ALTER TABLE position_events ADD COLUMN IF NOT EXISTS extra JSONB NOT NULL DEFAULT '{}'::jsonb;"
            )

            decision_score_expr = _sql_safe_float("(raw->'_decision_meta'->>'decision_score')")
            max_dd_expr = _sql_safe_float("(extra->>'max_dd_pct')")

            cur.execute(
                "CREATE INDEX IF NOT EXISTS idx_entry_candidates_src_regime_ts "
                "ON entry_candidates (src, regime, ts DESC);"
            )
            cur.execute(
                "CREATE INDEX IF NOT EXISTS idx_entry_candidates_wallet_tag_ts "
                "ON entry_candidates (wallet_tag, ts DESC);"
            )
            cur.execute(
                "CREATE INDEX IF NOT EXISTS idx_entry_candidates_session_ts "
                "ON entry_candidates (session_id, ts DESC);"
            )
            cur.execute(
                "CREATE INDEX IF NOT EXISTS idx_entry_candidates_feature_version "
                "ON entry_candidates (feature_version);"
            )
            cur.execute(
                "CREATE INDEX IF NOT EXISTS idx_entry_outcomes_candidate_id "
                "ON entry_outcomes (candidate_id);"
            )
            cur.execute(
                "CREATE INDEX IF NOT EXISTS idx_entry_outcomes_label_ts "
                "ON entry_outcomes (label, ts DESC);"
            )
            cur.execute(
                "CREATE INDEX IF NOT EXISTS idx_trade_executions_trade_id "
                "ON trade_executions (trade_id);"
            )
            cur.execute(
                "CREATE INDEX IF NOT EXISTS idx_trade_executions_candidate_id "
                "ON trade_executions (candidate_id);"
            )
            cur.execute(
                "CREATE INDEX IF NOT EXISTS idx_exit_events_mint_ts ON exit_events (mint, ts DESC);"
            )
            cur.execute(
                "CREATE INDEX IF NOT EXISTS idx_exit_events_trade_id ON exit_events (trade_id);"
            )
            cur.execute(
                "CREATE INDEX IF NOT EXISTS idx_exit_events_candidate_id "
                "ON exit_events (candidate_id);"
            )
            cur.execute(
                "CREATE INDEX IF NOT EXISTS idx_exit_events_reason_ts "
                "ON exit_events (reason, ts DESC);"
            )
            cur.execute(
                "CREATE INDEX IF NOT EXISTS idx_positions_mint_status_ts "
                "ON positions (mint, status, ts DESC);"
            )
            cur.execute(
                "CREATE INDEX IF NOT EXISTS idx_position_events_mint_ts "
                "ON position_events (mint, ts DESC);"
            )
            cur.execute(
                "CREATE INDEX IF NOT EXISTS idx_regime_snapshots_regime_ts "
                "ON regime_snapshots (regime, ts DESC);"
            )
            cur.execute(
                "CREATE INDEX IF NOT EXISTS idx_ai_events_type_ts "
                "ON ai_events (event_type, ts DESC);"
            )
            cur.execute(
                "CREATE INDEX IF NOT EXISTS idx_entry_candidates_policy_version "
                "ON entry_candidates ((raw->'_decision_meta'->>'ai_policy_version'));"
            )
            cur.execute(
                "CREATE INDEX IF NOT EXISTS idx_entry_candidates_experiment_tag "
                "ON entry_candidates ((raw->'_decision_meta'->>'ai_experiment_tag'));"
            )
            cur.execute(
                "CREATE INDEX IF NOT EXISTS idx_entry_candidates_decision "
                "ON entry_candidates ((raw->'_decision_meta'->>'decision'));"
            )
            cur.execute(
                f"CREATE INDEX IF NOT EXISTS idx_entry_candidates_decision_score_safe "
                f"ON entry_candidates (({decision_score_expr}));"
            )
            cur.execute(
                f"CREATE INDEX IF NOT EXISTS idx_entry_outcomes_max_dd_pct_safe "
                f"ON entry_outcomes (({max_dd_expr}));"
            )

    log.info("ai_db.ensure_ai_tables: schema ensured")


def bootstrap_ai_db() -> bool:
    """
    Startup bootstrap:
      - returns False if disabled or not reachable
      - ensures schema if reachable (unless migrations disabled/read-only)
    """
    if not AI_DB_ENABLED:
        return False
    try:
        if AI_DB_MIGRATIONS_ENABLED and not AI_DB_READ_ONLY:
            ensure_ai_tables()
            return True
        return ping()
    except Exception:
        if log.every("ai_db_bootstrap_fail", 10):
            log.exception("ai_db.bootstrap_ai_db failed")
        return False


# =============================================================================
# Phase 2: entry_candidates (training inputs)
# =============================================================================
def _clamp_int(val: Any, lo: int, hi: int) -> int:
    try:
        x = int(val)
    except Exception:
        x = lo
    return max(lo, min(x, hi))


def _mk_where_sql(clauses: Sequence[str]) -> str:
    return ("WHERE " + " AND ".join(clauses)) if clauses else ""


@_softfail_write(0)
def insert_entry_candidate(
    *,
    src: str,
    mint: str,
    symbol: str | None,
    regime: str | None,
    wallet_tag: str | None,
    session_id: str | None = None,
    feature_version: int = 0,
    features: Mapping[str, Any] | None = None,
    raw: Mapping[str, Any] | None = None,
) -> int:
    with conn_cursor() as (_conn, cur):
        cur.execute(
            """
            INSERT INTO entry_candidates
                (src, mint, symbol, regime, wallet_tag, session_id, feature_version, features, raw)
            VALUES
                (%s,  %s,   %s,     %s,     %s,        %s,         %s,             %s::jsonb, %s::jsonb)
            RETURNING id
            """,
            (
                str(src),
                str(mint),
                symbol,
                regime,
                wallet_tag,
                session_id,
                int(feature_version),
                _json_dumps(_as_json_map(features)),
                _json_dumps(_as_json_map(raw)),
            ),
        )
        row = cur.fetchone()
        return int(row[0]) if row else 0


@_softfail_write(0)
def insert_entry_candidates_bulk(rows: Sequence[Mapping[str, Any]]) -> int:
    """
    Bulk insert candidates.
    Skips rows with missing/blank src or mint.
    Returns number of rows attempted (post-skip).
    """
    if not rows:
        return 0

    values: list[tuple[Any, ...]] = []
    for r in rows:
        src_val = str(r.get("src") or "").strip()
        mint_val = str(r.get("mint") or "").strip()
        if not src_val or not mint_val:
            continue

        values.append(
            (
                src_val,
                mint_val,
                r.get("symbol"),
                r.get("regime"),
                r.get("wallet_tag"),
                r.get("session_id"),
                int(r.get("feature_version") or 0),
                _json_dumps(_as_json_map(r.get("features"))),
                _json_dumps(_as_json_map(r.get("raw"))),
            )
        )

    if not values:
        return 0

    if _execute_values is not None and _PG_DRIVER == "psycopg2":
        with conn_cursor() as (_conn, cur):
            _execute_values(
                cur,
                """
                INSERT INTO entry_candidates
                    (src, mint, symbol, regime, wallet_tag, session_id, feature_version, features, raw)
                VALUES %s
                """,
                values,
                template="(%s,%s,%s,%s,%s,%s,%s,%s::jsonb,%s::jsonb)",
                page_size=500,
            )
        return len(values)

    with conn_cursor() as (_conn, cur):
        cur.executemany(
            """
            INSERT INTO entry_candidates
                (src, mint, symbol, regime, wallet_tag, session_id, feature_version, features, raw)
            VALUES
                (%s,%s,%s,%s,%s,%s,%s,%s::jsonb,%s::jsonb)
            """,
            values,
        )
    return len(values)


@_softfail_read([])
def fetch_latest_candidates(
    *,
    limit: int = 50,
    src: str | None = None,
    regime: str | None = None,
    wallet_tag: str | None = None,
    session_id: str | None = None,
    min_feature_version: int | None = None,
) -> list[Row]:
    limit_i = _clamp_int(limit, 1, 500)
    min_feature_version = _effective_min_feature_version(min_feature_version)

    clauses: list[str] = []
    params: list[Any] = []

    if src:
        clauses.append("src = %s")
        params.append(src)
    if regime:
        clauses.append("regime = %s")
        params.append(regime)
    if wallet_tag:
        clauses.append("wallet_tag = %s")
        params.append(wallet_tag)
    if session_id:
        clauses.append("session_id = %s")
        params.append(session_id)
    if min_feature_version is not None:
        clauses.append("feature_version >= %s")
        params.append(int(min_feature_version))

    where_sql = _mk_where_sql(clauses)

    sql = (
        """
        SELECT id, ts, src, mint, symbol, regime, wallet_tag, session_id, feature_version, features, raw
        FROM entry_candidates
        """
        + (where_sql + "\n" if where_sql else "")
        + """
        ORDER BY ts DESC, id DESC
        LIMIT %s
        """
    )
    params.append(limit_i)

    with dict_cursor_only() as cur:
        cur.execute(sql, params)
        rows = cur.fetchall() or []

    out: list[Row] = []
    for r in rows:
        rr = dict(r)
        rr["id"] = int(rr.get("id") or 0)
        rr["feature_version"] = int(rr.get("feature_version") or 0)
        rr["features"] = _ensure_obj(rr.get("features"))
        rr["raw"] = _ensure_obj(rr.get("raw"))
        out.append(rr)
    return out


@_softfail_read(None)
def fetch_candidate_by_id(candidate_id: int) -> Row | None:
    with dict_cursor_only() as cur:
        cur.execute(
            """
            SELECT id, ts, src, mint, symbol, regime, wallet_tag, session_id, feature_version, features, raw
            FROM entry_candidates
            WHERE id = %s
            """,
            (int(candidate_id),),
        )
        row = cur.fetchone()

    if not row:
        return None

    r = dict(row)
    r["id"] = int(r.get("id") or 0)
    r["feature_version"] = int(r.get("feature_version") or 0)
    r["features"] = _ensure_obj(r.get("features"))
    r["raw"] = _ensure_obj(r.get("raw"))
    return r


@_softfail_write(0)
def update_entry_decision(
    *,
    candidate_id: int,
    decision: str | None = None,
    decision_score: float | None = None,
    decision_reason: str | None = None,
    **extra_fields: Any,
) -> int:
    patch_meta: dict[str, Any] = {
        "decision": decision,
        "decision_score": decision_score,
        "decision_reason": decision_reason,
        "ts": _utcnow().isoformat(),
    }
    if extra_fields:
        patch_meta.update(extra_fields)

    patch_json = _json_dumps({"_decision_meta": dict(_as_json_object(patch_meta))})

    with conn_cursor() as (_conn, cur):
        cur.execute(
            """
            UPDATE entry_candidates
            SET raw = COALESCE(raw, '{}'::jsonb) || %s::jsonb
            WHERE id = %s
            RETURNING id
            """,
            (patch_json, int(candidate_id)),
        )
        row = cur.fetchone()
        return int(row[0]) if row else 0


# =============================================================================
# Phase 3: entry_outcomes (training labels)
# =============================================================================
@_softfail_write(0)
def insert_entry_outcome(
    *,
    candidate_id: int,
    label: str,
    pnl_pct: float | None = None,
    hold_sec: int | None = None,
    notes: str | None = None,
    extra: Mapping[str, Any] | None = None,
) -> int:
    with conn_cursor() as (_conn, cur):
        cur.execute(
            """
            INSERT INTO entry_outcomes
                (candidate_id, label, pnl_pct, hold_sec, notes, extra)
            VALUES
                (%s,           %s,    %s,       %s,       %s,    %s::jsonb)
            RETURNING id
            """,
            (
                int(candidate_id),
                str(label),
                pnl_pct,
                hold_sec,
                notes,
                _json_dumps(_as_json_map(extra)),
            ),
        )
        row = cur.fetchone()
        return int(row[0]) if row else 0


@_softfail_write(0)
def mark_outcome_for_candidate(
    candidate_id: int,
    *,
    label: str,
    pnl_pct: float | None = None,
    hold_sec: int | None = None,
    notes: str | None = None,
    extra: Mapping[str, Any] | None = None,
) -> int:
    with conn_cursor() as (_conn, cur):
        cur.execute(
            """
            INSERT INTO entry_outcomes
                (candidate_id, label, pnl_pct, hold_sec, notes, extra)
            VALUES
                (%s,           %s,    %s,       %s,       %s,    %s::jsonb)
            ON CONFLICT (candidate_id)
            DO UPDATE SET
                label    = EXCLUDED.label,
                pnl_pct  = EXCLUDED.pnl_pct,
                hold_sec = EXCLUDED.hold_sec,
                notes    = EXCLUDED.notes,
                extra    = EXCLUDED.extra,
                ts       = now()
            RETURNING id
            """,
            (
                int(candidate_id),
                str(label),
                pnl_pct,
                hold_sec,
                notes,
                _json_dumps(_as_json_map(extra)),
            ),
        )
        row = cur.fetchone()
        return int(row[0]) if row else 0


@_softfail_read([])
def fetch_recent_outcomes(*, limit: int = 100, since_candidate_id: int | None = None) -> list[Row]:
    limit_i = _clamp_int(limit, 1, 1000)

    clauses: list[str] = []
    params: list[Any] = []
    if since_candidate_id is not None:
        clauses.append("candidate_id > %s")
        params.append(int(since_candidate_id))

    where_sql = _mk_where_sql(clauses)

    sql = (
        """
        SELECT id, candidate_id, label, pnl_pct, hold_sec, notes, extra, ts
        FROM entry_outcomes
        """
        + (where_sql + "\n" if where_sql else "")
        + """
        ORDER BY ts DESC, id DESC
        LIMIT %s
        """
    )
    params.append(limit_i)

    with dict_cursor_only() as cur:
        cur.execute(sql, params)
        rows = cur.fetchall() or []

    out: list[Row] = []
    for r in rows:
        rr = dict(r)
        rr["id"] = int(rr.get("id") or 0)
        rr["candidate_id"] = int(rr.get("candidate_id") or 0)
        rr["extra"] = _ensure_obj(rr.get("extra"))
        out.append(rr)
    return out


@_softfail_read([])
def fetch_unlabeled_candidates(
    *,
    limit: int = 500,
    min_feature_version: int | None = None,
    src: str | None = None,
    regime: str | None = None,
    min_age_seconds: int | None = None,
    wallet_tag: str | None = None,
    session_id: str | None = None,
) -> list[Row]:
    limit_i = _clamp_int(limit, 1, 5000)
    min_feature_version = _effective_min_feature_version(min_feature_version)

    clauses: list[str] = []
    params: list[Any] = []

    if src:
        clauses.append("c.src = %s")
        params.append(src)
    if regime:
        clauses.append("c.regime = %s")
        params.append(regime)
    if wallet_tag:
        clauses.append("c.wallet_tag = %s")
        params.append(wallet_tag)
    if session_id:
        clauses.append("c.session_id = %s")
        params.append(session_id)
    if min_feature_version is not None:
        clauses.append("c.feature_version >= %s")
        params.append(int(min_feature_version))
    if min_age_seconds is not None:
        try:
            age_i = int(min_age_seconds)
        except Exception:
            age_i = 0
        if age_i > 0:
            clauses.append("c.ts <= now() - (%s::interval)")
            params.append(f"{age_i} seconds")

    clauses.append("o.candidate_id IS NULL")
    where_sql = _mk_where_sql(clauses)

    sql = (
        """
        SELECT
            c.id, c.ts, c.src, c.mint, c.symbol, c.regime, c.wallet_tag, c.session_id,
            c.feature_version, c.features, c.raw
        FROM entry_candidates c
        LEFT JOIN entry_outcomes o
          ON o.candidate_id = c.id
        """
        + (where_sql + "\n" if where_sql else "")
        + """
        ORDER BY c.ts DESC, c.id DESC
        LIMIT %s
        """
    )
    params.append(limit_i)

    with dict_cursor_only() as cur:
        cur.execute(sql, params)
        rows = cur.fetchall() or []

    out: list[Row] = []
    for r in rows:
        rr = dict(r)
        rr["id"] = int(rr.get("id") or 0)
        rr["feature_version"] = int(rr.get("feature_version") or 0)
        rr["features"] = _ensure_obj(rr.get("features"))
        rr["raw"] = _ensure_obj(rr.get("raw"))
        out.append(rr)
    return out


# =============================================================================
# Phase 3b: Executions, exit events, regime snapshots, ai events
# =============================================================================
@_softfail_write(0)
def insert_trade_execution(
    *,
    trade_id: str,
    mint: str,
    side: str,
    qty_raw: int,
    price_usd: float,
    slippage_bps: float | None = None,
    fees_usd: float | None = None,
    tx_sig: str | None = None,
    candidate_id: int | None = None,
    session_id: str | None = None,
    ts: Any | None = None,
    extra: Mapping[str, Any] | None = None,
) -> int:
    with conn_cursor() as (_conn, cur):
        cur.execute(
            """
            INSERT INTO trade_executions
                (ts, trade_id, mint, side, qty_raw, price_usd,
                 slippage_bps, fees_usd, tx_sig, candidate_id, session_id, extra)
            VALUES
                (COALESCE(%s::timestamptz, now()), %s, %s, %s, %s, %s,
                 %s, %s, %s, %s, %s, %s::jsonb)
            ON CONFLICT (trade_id, side, mint) DO NOTHING
            RETURNING id
            """,
            (
                ts,
                str(trade_id),
                str(mint),
                str(side).upper(),
                int(qty_raw),
                float(price_usd),
                slippage_bps,
                fees_usd,
                tx_sig,
                int(candidate_id) if candidate_id is not None else None,
                session_id,
                _json_dumps(_as_json_map(extra)),
            ),
        )
        row = cur.fetchone()
        return int(row[0]) if row else 0


def _mk_exit_event_key(
    *,
    trade_id: str | None,
    mint: str,
    action: str | None,
    reason: str | None,
    tx_sig: str | None,
    candidate_id: int | None,
    session_id: str | None,
    ts: Any | None,
) -> str:
    base = "|".join(
        [
            str(trade_id or ""),
            str(mint or ""),
            str((action or "").upper()),
            str(reason or ""),
            str(tx_sig or ""),
            str(candidate_id or ""),
            str(session_id or ""),
            str(ts or ""),
        ]
    )
    return hashlib.sha256(base.encode("utf-8", "ignore")).hexdigest()


@_softfail_write(0)
def insert_exit_event(
    *,
    mint: str,
    symbol: str | None = None,
    trade_id: str | None = None,
    action: str | None = None,
    reason: str | None = None,
    pnl_pct: float | None = None,
    pnl_usd: float | None = None,
    qty_raw: int | None = None,
    price_usd: float | None = None,
    slippage_bps: float | None = None,
    fees_usd: float | None = None,
    tx_sig: str | None = None,
    candidate_id: int | None = None,
    session_id: str | None = None,
    ts: Any | None = None,
    extra: Mapping[str, Any] | None = None,
    event_key: str | None = None,
) -> int:
    ek = event_key or _mk_exit_event_key(
        trade_id=trade_id,
        mint=mint,
        action=action,
        reason=reason,
        tx_sig=tx_sig,
        candidate_id=candidate_id,
        session_id=session_id,
        ts=ts,
    )

    with conn_cursor() as (_conn, cur):
        cur.execute(
            """
            INSERT INTO exit_events
                (ts, event_key, trade_id, mint, symbol, action, reason,
                 pnl_pct, pnl_usd, qty_raw, price_usd, slippage_bps, fees_usd,
                 tx_sig, candidate_id, session_id, extra)
            VALUES
                (COALESCE(%s::timestamptz, now()), %s, %s, %s, %s, %s, %s,
                 %s, %s, %s, %s, %s, %s,
                 %s, %s, %s, %s::jsonb)
            ON CONFLICT (event_key) DO NOTHING
            RETURNING id
            """,
            (
                ts,
                str(ek),
                trade_id,
                str(mint),
                symbol,
                (action or None),
                (reason or None),
                pnl_pct,
                pnl_usd,
                int(qty_raw) if qty_raw is not None else None,
                float(price_usd) if price_usd is not None else None,
                slippage_bps,
                fees_usd,
                tx_sig,
                int(candidate_id) if candidate_id is not None else None,
                session_id,
                _json_dumps(_as_json_map(extra)),
            ),
        )
        row = cur.fetchone()
        return int(row[0]) if row else 0


@_softfail_read([])
def fetch_recent_exit_events(
    *,
    limit: int = 200,
    mint: str | None = None,
    trade_id: str | None = None,
    candidate_id: int | None = None,
) -> list[Row]:
    limit_i = _clamp_int(limit, 1, 2000)

    clauses: list[str] = []
    params: list[Any] = []

    if mint:
        clauses.append("mint = %s")
        params.append(mint)
    if trade_id:
        clauses.append("trade_id = %s")
        params.append(trade_id)
    if candidate_id is not None:
        clauses.append("candidate_id = %s")
        params.append(int(candidate_id))

    where_sql = _mk_where_sql(clauses)

    sql = (
        """
        SELECT id, ts, event_key, trade_id, mint, symbol, action, reason,
               pnl_pct, pnl_usd, qty_raw, price_usd, slippage_bps, fees_usd, tx_sig,
               candidate_id, session_id, extra
        FROM exit_events
        """
        + (where_sql + "\n" if where_sql else "")
        + """
        ORDER BY ts DESC, id DESC
        LIMIT %s
        """
    )
    params.append(limit_i)

    with dict_cursor_only() as cur:
        cur.execute(sql, params)
        rows = cur.fetchall() or []

    out: list[Row] = []
    for r in rows:
        rr = dict(r)
        rr["id"] = int(rr.get("id") or 0)
        rr["extra"] = _ensure_obj(rr.get("extra"))
        out.append(rr)
    return out


@_softfail_read([])
def fetch_recent_trade_executions(
    *,
    limit: int = 200,
    trade_id: str | None = None,
    candidate_id: int | None = None,
) -> list[Row]:
    limit_i = _clamp_int(limit, 1, 2000)

    clauses: list[str] = []
    params: list[Any] = []

    if trade_id:
        clauses.append("trade_id = %s")
        params.append(trade_id)
    if candidate_id is not None:
        clauses.append("candidate_id = %s")
        params.append(int(candidate_id))

    where_sql = _mk_where_sql(clauses)

    sql = (
        """
        SELECT id, ts, trade_id, mint, side, qty_raw, price_usd,
               slippage_bps, fees_usd, tx_sig, candidate_id, session_id, extra
        FROM trade_executions
        """
        + (where_sql + "\n" if where_sql else "")
        + """
        ORDER BY ts DESC, id DESC
        LIMIT %s
        """
    )
    params.append(limit_i)

    with dict_cursor_only() as cur:
        cur.execute(sql, params)
        rows = cur.fetchall() or []

    out: list[Row] = []
    for r in rows:
        rr = dict(r)
        rr["id"] = int(rr.get("id") or 0)
        rr["extra"] = _ensure_obj(rr.get("extra"))
        out.append(rr)
    return out


@_softfail_write(0)
def insert_regime_snapshot(
    *,
    regime: str,
    state: Mapping[str, Any],
    ts: Any | None = None,
    session_id: str | None = None,
    extra: Mapping[str, Any] | None = None,
) -> int:
    with conn_cursor() as (_conn, cur):
        cur.execute(
            """
            INSERT INTO regime_snapshots
                (ts, regime, state, session_id, extra)
            VALUES
                (COALESCE(%s::timestamptz, now()), %s, %s::jsonb, %s, %s::jsonb)
            RETURNING id
            """,
            (
                ts,
                str(regime),
                _json_dumps(_as_json_map(state)),
                session_id,
                _json_dumps(_as_json_map(extra)),
            ),
        )
        row = cur.fetchone()
        return int(row[0]) if row else 0


@_softfail_read([])
def fetch_recent_regime_snapshots(*, limit: int = 200, regime: str | None = None) -> list[Row]:
    limit_i = _clamp_int(limit, 1, 2000)

    clauses: list[str] = []
    params: list[Any] = []
    if regime:
        clauses.append("regime = %s")
        params.append(regime)

    where_sql = _mk_where_sql(clauses)

    sql = (
        """
        SELECT id, ts, regime, state, session_id, extra
        FROM regime_snapshots
        """
        + (where_sql + "\n" if where_sql else "")
        + """
        ORDER BY ts DESC, id DESC
        LIMIT %s
        """
    )
    params.append(limit_i)

    with dict_cursor_only() as cur:
        cur.execute(sql, params)
        rows = cur.fetchall() or []

    out: list[Row] = []
    for r in rows:
        rr = dict(r)
        rr["id"] = int(rr.get("id") or 0)
        rr["state"] = _ensure_obj(rr.get("state"))
        rr["extra"] = _ensure_obj(rr.get("extra"))
        out.append(rr)
    return out


@_softfail_write(0)
def log_ai_event(
    *,
    event_type: str,
    policy_version: str | None = None,
    experiment_tag: str | None = None,
    src: str | None = None,
    regime: str | None = None,
    payload: Mapping[str, Any] | None = None,
) -> int:
    with conn_cursor() as (_conn, cur):
        cur.execute(
            """
            INSERT INTO ai_events
                (event_type, policy_version, experiment_tag, src, regime, payload)
            VALUES
                (%s,         %s,             %s,             %s,  %s,     %s::jsonb)
            RETURNING id
            """,
            (
                str(event_type),
                policy_version,
                experiment_tag,
                src,
                regime,
                _json_dumps(_as_json_map(payload)),
            ),
        )
        row = cur.fetchone()
        return int(row[0]) if row else 0


@_softfail_read([])
def fetch_ai_event_summary(
    *,
    days_back: int = 30,
    event_types: Sequence[str] | None = None,
) -> list[dict[str, Any]]:
    days_i = _clamp_int(days_back, 1, 365)

    clauses: list[str] = ["ts >= now() - (%s::interval)"]
    params: list[Any] = [f"{days_i} days"]
    if event_types:
        clauses.append("event_type = ANY(%s)")
        params.append(list(event_types))

    where_sql = _mk_where_sql(clauses)

    sql = (
        """
        SELECT event_type, count(*) AS n_events, min(ts) AS first_ts, max(ts) AS last_ts
        FROM ai_events
        """
        + (where_sql + "\n" if where_sql else "")
        + """
        GROUP BY event_type
        ORDER BY event_type
        """
    )

    out: list[dict[str, Any]] = []
    with cursor_only() as cur:
        cur.execute(sql, params)
        rows = cur.fetchall() or []

    for event_type, n_events, first_ts, last_ts in rows:
        out.append(
            {
                "event_type": event_type,
                "n_events": int(n_events or 0),
                "first_ts": first_ts.isoformat()
                if isinstance(first_ts, datetime)
                else str(first_ts),
                "last_ts": last_ts.isoformat() if isinstance(last_ts, datetime) else str(last_ts),
            }
        )
    return out


# =============================================================================
# Strategy params + risk assessments
# =============================================================================
@_softfail_write(0)
def insert_strategy_param(
    *,
    key: str,
    value: Mapping[str, Any],
    src: str | None = None,
    regime: str | None = None,
    session_id: str | None = None,
    extra: Mapping[str, Any] | None = None,
) -> int:
    with conn_cursor() as (_conn, cur):
        cur.execute(
            """
            INSERT INTO strategy_params (key, value, src, regime, session_id, extra)
            VALUES (%s, %s::jsonb, %s, %s, %s, %s::jsonb)
            RETURNING id
            """,
            (
                str(key),
                _json_dumps(_as_json_map(value)),
                src,
                regime,
                session_id,
                _json_dumps(_as_json_map(extra)),
            ),
        )
        row = cur.fetchone()
        return int(row[0]) if row else 0


@_softfail_read([])
def fetch_latest_strategy_params(*, key: str | None = None, limit: int = 50) -> list[Row]:
    limit_i = _clamp_int(limit, 1, 500)

    clauses: list[str] = []
    params: list[Any] = []
    if key:
        clauses.append("key = %s")
        params.append(str(key))

    where_sql = _mk_where_sql(clauses)

    sql = (
        """
        SELECT id, ts, key, value, src, regime, session_id, extra
        FROM strategy_params
        """
        + (where_sql + "\n" if where_sql else "")
        + """
        ORDER BY ts DESC, id DESC
        LIMIT %s
        """
    )
    params.append(limit_i)

    with dict_cursor_only() as cur:
        cur.execute(sql, params)
        rows = cur.fetchall() or []

    out: list[Row] = []
    for r in rows:
        rr = dict(r)
        rr["id"] = int(rr.get("id") or 0)
        rr["value"] = _ensure_obj(rr.get("value"))
        rr["extra"] = _ensure_obj(rr.get("extra"))
        out.append(rr)
    return out


@_softfail_write(0)
def insert_risk_assessment(
    *,
    mint: str,
    symbol: str | None = None,
    score: float | None = None,
    verdict: str | None = None,
    details: Mapping[str, Any] | None = None,
    model_tag: str | None = None,
    session_id: str | None = None,
    extra: Mapping[str, Any] | None = None,
) -> int:
    with conn_cursor() as (_conn, cur):
        cur.execute(
            """
            INSERT INTO risk_assessments (mint, symbol, score, verdict, details, model_tag, session_id, extra)
            VALUES (%s, %s, %s, %s, %s::jsonb, %s, %s, %s::jsonb)
            RETURNING id
            """,
            (
                str(mint),
                symbol,
                score,
                verdict,
                _json_dumps(_as_json_map(details)),
                model_tag,
                session_id,
                _json_dumps(_as_json_map(extra)),
            ),
        )
        row = cur.fetchone()
        return int(row[0]) if row else 0


@_softfail_read([])
def fetch_latest_risk_assessments(*, mint: str | None = None, limit: int = 100) -> list[Row]:
    limit_i = _clamp_int(limit, 1, 1000)

    clauses: list[str] = []
    params: list[Any] = []
    if mint:
        clauses.append("mint = %s")
        params.append(str(mint))

    where_sql = _mk_where_sql(clauses)

    sql = (
        """
        SELECT id, ts, mint, symbol, score, verdict, details, model_tag, session_id, extra
        FROM risk_assessments
        """
        + (where_sql + "\n" if where_sql else "")
        + """
        ORDER BY ts DESC, id DESC
        LIMIT %s
        """
    )
    params.append(limit_i)

    with dict_cursor_only() as cur:
        cur.execute(sql, params)
        rows = cur.fetchall() or []

    out: list[Row] = []
    for r in rows:
        rr = dict(r)
        rr["id"] = int(rr.get("id") or 0)
        rr["details"] = _ensure_obj(rr.get("details"))
        rr["extra"] = _ensure_obj(rr.get("extra"))
        out.append(rr)
    return out


# =============================================================================
# Composite helpers
# =============================================================================
@_softfail_write({"execution_id": 0, "outcome_id": 0})
def record_trade_and_outcome(
    *,
    trade_id: str,
    mint: str,
    side: str,
    qty_raw: int,
    price_usd: float,
    candidate_id: int | None = None,
    label: str | None = None,
    pnl_pct: float | None = None,
    hold_sec: int | None = None,
    notes: str | None = None,
    slippage_bps: float | None = None,
    fees_usd: float | None = None,
    tx_sig: str | None = None,
    session_id: str | None = None,
    trade_extra: Mapping[str, Any] | None = None,
    outcome_extra: Mapping[str, Any] | None = None,
) -> dict[str, int]:
    exec_id = insert_trade_execution(
        trade_id=trade_id,
        mint=mint,
        side=side,
        qty_raw=qty_raw,
        price_usd=price_usd,
        slippage_bps=slippage_bps,
        fees_usd=fees_usd,
        tx_sig=tx_sig,
        candidate_id=candidate_id,
        session_id=session_id,
        extra=trade_extra or {},
    )

    out_id = 0
    if candidate_id is not None and label is not None:
        out_id = mark_outcome_for_candidate(
            candidate_id,
            label=label,
            pnl_pct=pnl_pct,
            hold_sec=hold_sec,
            notes=notes,
            extra=(outcome_extra or {}),
        )

    return {"execution_id": int(exec_id or 0), "outcome_id": int(out_id or 0)}


@_softfail_write({"exit_event_id": 0, "outcome_id": 0})
def record_exit_event_and_outcome(
    *,
    mint: str,
    symbol: str | None = None,
    trade_id: str | None = None,
    action: str | None = None,
    reason: str | None = None,
    pnl_pct: float | None = None,
    pnl_usd: float | None = None,
    qty_raw: int | None = None,
    price_usd: float | None = None,
    slippage_bps: float | None = None,
    fees_usd: float | None = None,
    tx_sig: str | None = None,
    candidate_id: int | None = None,
    session_id: str | None = None,
    ts: Any | None = None,
    exit_extra: Mapping[str, Any] | None = None,
    label: str | None = None,
    hold_sec: int | None = None,
    notes: str | None = None,
    outcome_extra: Mapping[str, Any] | None = None,
    event_key: str | None = None,
) -> dict[str, int]:
    exit_id = insert_exit_event(
        mint=mint,
        symbol=symbol,
        trade_id=trade_id,
        action=action,
        reason=reason,
        pnl_pct=pnl_pct,
        pnl_usd=pnl_usd,
        qty_raw=qty_raw,
        price_usd=price_usd,
        slippage_bps=slippage_bps,
        fees_usd=fees_usd,
        tx_sig=tx_sig,
        candidate_id=candidate_id,
        session_id=session_id,
        ts=ts,
        extra=exit_extra or {},
        event_key=event_key,
    )

    out_id = 0
    if candidate_id is not None and label is not None:
        out_id = mark_outcome_for_candidate(
            candidate_id,
            label=label,
            pnl_pct=pnl_pct,
            hold_sec=hold_sec,
            notes=notes,
            extra=(outcome_extra or {}),
        )

    return {"exit_event_id": int(exit_id or 0), "outcome_id": int(out_id or 0)}


# =============================================================================
# Training export / backtest streaming
# =============================================================================
@_softfail_read([])
def fetch_training_rows(
    *,
    limit: int = 2000,
    require_outcome: bool = True,
    min_feature_version: int | None = None,
    src: str | None = None,
    regime: str | None = None,
    policy_version: str | None = None,
    experiment_tag: str | None = None,
    label_in: Sequence[str] | None = None,
    wallet_tag: str | None = None,
    session_id: str | None = None,
) -> list[Row]:
    limit_i = _clamp_int(limit, 1, 10_000)
    min_feature_version = _effective_min_feature_version(min_feature_version)

    join_type = "JOIN" if require_outcome else "LEFT JOIN"
    clauses: list[str] = []
    params: list[Any] = []

    if src:
        clauses.append("c.src = %s")
        params.append(src)
    if regime:
        clauses.append("c.regime = %s")
        params.append(regime)
    if wallet_tag:
        clauses.append("c.wallet_tag = %s")
        params.append(wallet_tag)
    if session_id:
        clauses.append("c.session_id = %s")
        params.append(session_id)
    if min_feature_version is not None:
        clauses.append("c.feature_version >= %s")
        params.append(int(min_feature_version))
    if policy_version:
        clauses.append("c.raw->'_decision_meta'->>'ai_policy_version' = %s")
        params.append(policy_version)
    if experiment_tag:
        clauses.append("c.raw->'_decision_meta'->>'ai_experiment_tag' = %s")
        params.append(experiment_tag)
    if label_in:
        clauses.append("o.label = ANY(%s)")
        params.append(list(label_in))

    where_sql = _mk_where_sql(clauses)
    max_dd_expr = _sql_safe_float("(o.extra->>'max_dd_pct')")

    sql = (
        f"""
        SELECT
            c.id AS id,
            c.ts AS ts,
            c.src AS src,
            c.mint AS mint,
            c.symbol AS symbol,
            c.regime AS regime,
            c.wallet_tag AS wallet_tag,
            c.session_id AS session_id,
            c.feature_version AS feature_version,
            c.features AS features,
            c.raw AS raw,
            o.label AS label,
            o.pnl_pct AS pnl_pct,
            o.hold_sec AS hold_sec,
            {max_dd_expr} AS max_dd_pct
        FROM entry_candidates c
        {join_type} entry_outcomes o
          ON o.candidate_id = c.id
        """
        + (where_sql + "\n" if where_sql else "")
        + """
        ORDER BY c.ts DESC, c.id DESC
        LIMIT %s
        """
    )
    params.append(limit_i)

    with dict_cursor_only() as cur:
        cur.execute(sql, params)
        rows = cur.fetchall() or []

    out: list[Row] = []
    for r in rows:
        rr = dict(r)
        rr["id"] = int(rr.get("id") or 0)
        rr["feature_version"] = int(rr.get("feature_version") or 0)
        rr["features"] = _ensure_obj(rr.get("features"))
        rr["raw"] = _ensure_obj(rr.get("raw"))
        rr["has_outcome"] = rr.get("label") is not None
        out.append(rr)
    return out


def _parse_iso_dt(val: Any) -> datetime | None:
    if val is None:
        return None
    if isinstance(val, datetime):
        return val
    s = str(val)
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception:
        return None


def iter_backtest_rows(config: Mapping[str, Any]) -> Iterator[Mapping[str, Any]]:
    if not AI_DB_ENABLED or not _sync_pg_ok():
        return iter(())

    limit = int(config.get("BT_MAX_ROWS") or config.get("limit") or 50_000)
    limit = max(1, min(limit, 200_000))

    days_back = config.get("days_back")
    start_ts_raw = config.get("start_ts")
    end_ts_raw = config.get("end_ts")

    min_feature_version = config.get("BT_MIN_FEATURE_VERSION", config.get("min_feature_version"))
    try:
        min_feature_version = int(min_feature_version) if min_feature_version is not None else None
    except Exception:
        min_feature_version = None
    min_feature_version = _effective_min_feature_version(min_feature_version)

    src = config.get("src")
    regime = config.get("regime")
    wallet_tag = config.get("wallet_tag")
    session_id = config.get("session_id")
    policy_version = config.get("policy_version")
    experiment_tag = config.get("experiment_tag")
    label_in = config.get("label_in")
    label_not_in = config.get("label_not_in")
    min_pnl_pct = config.get("min_pnl_pct")
    max_pnl_pct = config.get("max_pnl_pct")

    require_outcome = bool(config.get("BT_REQUIRE_OUTCOME", config.get("require_outcome", True)))
    join_type = "JOIN" if require_outcome else "LEFT JOIN"

    clauses: list[str] = []
    params: list[Any] = []

    if days_back is not None:
        try:
            db_int = max(1, min(int(days_back), 365))
            clauses.append("c.ts >= now() - (%s::interval)")
            params.append(f"{db_int} days")
        except Exception:
            pass

    if start_ts_raw:
        start_dt = _parse_iso_dt(start_ts_raw)
        if start_dt is not None:
            clauses.append("c.ts >= %s")
            params.append(start_dt)
    if end_ts_raw:
        end_dt = _parse_iso_dt(end_ts_raw)
        if end_dt is not None:
            clauses.append("c.ts <= %s")
            params.append(end_dt)

    if src:
        clauses.append("c.src = %s")
        params.append(src)
    if regime:
        clauses.append("c.regime = %s")
        params.append(regime)
    if wallet_tag:
        clauses.append("c.wallet_tag = %s")
        params.append(wallet_tag)
    if session_id:
        clauses.append("c.session_id = %s")
        params.append(session_id)
    if min_feature_version is not None:
        clauses.append("c.feature_version >= %s")
        params.append(int(min_feature_version))
    if policy_version:
        clauses.append("c.raw->'_decision_meta'->>'ai_policy_version' = %s")
        params.append(policy_version)
    if experiment_tag:
        clauses.append("c.raw->'_decision_meta'->>'ai_experiment_tag' = %s")
        params.append(experiment_tag)

    if label_in:
        clauses.append("o.label = ANY(%s)")
        params.append(list(label_in))
    if label_not_in:
        clauses.append("NOT (o.label = ANY(%s))")
        params.append(list(label_not_in))

    if min_pnl_pct is not None:
        try:
            clauses.append("o.pnl_pct >= %s")
            params.append(float(min_pnl_pct))
        except Exception:
            pass
    if max_pnl_pct is not None:
        try:
            clauses.append("o.pnl_pct <= %s")
            params.append(float(max_pnl_pct))
        except Exception:
            pass

    where_sql = _mk_where_sql(clauses)
    max_dd_expr = _sql_safe_float("(o.extra->>'max_dd_pct')")

    sql = (
        f"""
        SELECT
            c.id,
            c.ts,
            c.src,
            c.mint,
            c.symbol,
            c.regime,
            c.wallet_tag,
            c.session_id,
            c.feature_version,
            c.features,
            c.raw,
            o.label,
            o.pnl_pct,
            o.hold_sec,
            {max_dd_expr} AS max_dd_pct
        FROM entry_candidates c
        {join_type} entry_outcomes o
          ON o.candidate_id = c.id
        """
        + (where_sql + "\n" if where_sql else "")
        + """
        ORDER BY c.ts ASC, c.id ASC
        LIMIT %s
        """
    )
    params2 = [*list(params), limit]

    def _gen() -> Iterator[Mapping[str, Any]]:
        conn: _ConnLike | None = None
        cur: Any | None = None
        try:
            conn = _connect()

            iterator: Iterator[Any]

            if _PG_DRIVER == "psycopg2":
                cur_name = f"ai_db_backtest_cursor_{os.getpid()}"
                cur = conn.cursor(name=cur_name)
                try:
                    cur.itersize = 2000
                except Exception:
                    pass
                cur.execute(sql, params2)
                iterator = iter(cur)
            else:
                cur = conn.cursor()
                cur.execute(sql, params2)

                def _iter_fetchmany() -> Iterator[Any]:
                    while True:
                        batch = cur.fetchmany(2000)
                        if not batch:
                            break
                        for item in batch:
                            yield item

                iterator = _iter_fetchmany()

            for r in iterator:
                (
                    cid,
                    ts_val,
                    src_val,
                    mint_val,
                    symbol_val,
                    regime_val,
                    wallet_tag_val,
                    session_id_val,
                    feat_ver,
                    features_json,
                    raw_json,
                    label,
                    pnl_pct,
                    hold_sec,
                    max_dd_pct,
                ) = r
                yield {
                    "id": int(cid),
                    "created_at": ts_val,
                    "ts": ts_val,
                    "src": src_val,
                    "mint": mint_val,
                    "symbol": symbol_val,
                    "regime": regime_val,
                    "wallet_tag": wallet_tag_val,
                    "session_id": session_id_val,
                    "feature_version": int(feat_ver),
                    "features": _ensure_obj(features_json),
                    "raw": _ensure_obj(raw_json),
                    "label": label,
                    "pnl_pct": float(pnl_pct) if pnl_pct is not None else None,
                    "hold_sec": int(hold_sec) if hold_sec is not None else None,
                    "max_dd_pct": float(max_dd_pct) if max_dd_pct is not None else None,
                }
        except Exception:
            log.exception("iter_backtest_rows failed")
            return
        finally:
            with suppress(Exception):
                if cur is not None:
                    cur.close()
            with suppress(Exception):
                if conn is not None:
                    conn.close()

    return _gen()


def iter_training_rows(config: Mapping[str, Any]) -> Iterator[Row]:
    rows: list[Row] = fetch_training_rows(
        limit=int(config.get("limit") or 2000),
        require_outcome=bool(config.get("require_outcome", True)),
        min_feature_version=config.get("min_feature_version"),
        src=config.get("src"),
        regime=config.get("regime"),
        wallet_tag=config.get("wallet_tag"),
        session_id=config.get("session_id"),
        policy_version=config.get("policy_version"),
        experiment_tag=config.get("experiment_tag"),
        label_in=config.get("label_in"),
    )
    yield from rows


def iter_training_jsonl(config: Mapping[str, Any]) -> Iterator[str]:
    for row in iter_training_rows(config):
        obj = dict(row)
        ts_val = obj.get("ts")
        if isinstance(ts_val, datetime):
            obj["ts"] = ts_val.isoformat()
        created_at = obj.get("created_at")
        if isinstance(created_at, datetime):
            obj["created_at"] = created_at.isoformat()
        yield json.dumps(obj, separators=(",", ":"), ensure_ascii=False, default=str)


def dump_training_jsonl_to_file(path: str, config: Mapping[str, Any]) -> int:
    n = 0
    with open(path, "w", encoding="utf-8") as f:
        for line in iter_training_jsonl(config):
            f.write(line + "\n")
            n += 1
    log.info("dump_training_jsonl_to_file: wrote %s rows to %s", n, path)
    return n


# =============================================================================
# Quant stats & dashboards
# =============================================================================
@_softfail_read(
    {
        "n_trades": 0,
        "n_labeled": 0,
        "n_wins": 0,
        "win_rate": 0.0,
        "avg_pnl_pct": None,
        "median_pnl_pct": None,
        "avg_hold_sec": None,
        "win_labels": [],
    }
)
def fetch_basic_stats(
    *,
    days_back: int = 7,
    src: str | None = None,
    regime: str | None = None,
    min_feature_version: int | None = None,
    wallet_tag: str | None = None,
    session_id: str | None = None,
) -> dict[str, Any]:
    days_i = _clamp_int(days_back, 1, 365)
    min_feature_version = _effective_min_feature_version(min_feature_version)

    clauses: list[str] = ["o.ts >= now() - (%s::interval)"]
    params: list[Any] = [f"{days_i} days"]

    if src:
        clauses.append("c.src = %s")
        params.append(src)
    if regime:
        clauses.append("c.regime = %s")
        params.append(regime)
    if wallet_tag:
        clauses.append("c.wallet_tag = %s")
        params.append(wallet_tag)
    if session_id:
        clauses.append("c.session_id = %s")
        params.append(session_id)
    if min_feature_version is not None:
        clauses.append("c.feature_version >= %s")
        params.append(int(min_feature_version))

    where_sql = _mk_where_sql(clauses)
    win_labels = [str(x).lower() for x in WIN_LABELS]

    sql = """
        SELECT
            count(*) AS n_total,
            count(*) FILTER (WHERE o.label IS NOT NULL) AS n_labeled,
            count(*) FILTER (WHERE lower(o.label) = ANY(%s)) AS n_wins,
            avg(o.pnl_pct) AS avg_pnl_pct,
            percentile_disc(0.5) WITHIN GROUP (ORDER BY o.pnl_pct) AS median_pnl_pct,
            avg(o.hold_sec) AS avg_hold_sec
        FROM entry_outcomes o
        JOIN entry_candidates c ON c.id = o.candidate_id
        """ + (where_sql + "\n" if where_sql else "")

    with cursor_only() as cur:
        cur.execute(sql, [win_labels, *params])
        row = cur.fetchone()

    if not row:
        raise RuntimeError("fetch_basic_stats: no row")

    n_total, n_labeled, n_wins, avg_pnl_pct, median_pnl_pct, avg_hold_sec = row
    n_total_i = int(n_total or 0)
    n_labeled_i = int(n_labeled or 0)
    n_wins_i = int(n_wins or 0)
    win_rate = float(n_wins_i) / float(n_labeled_i) if n_labeled_i else 0.0

    return {
        "n_trades": n_total_i,
        "n_labeled": n_labeled_i,
        "win_labels": win_labels,
        "n_wins": n_wins_i,
        "win_rate": win_rate,
        "avg_pnl_pct": float(avg_pnl_pct) if avg_pnl_pct is not None else None,
        "median_pnl_pct": float(median_pnl_pct) if median_pnl_pct is not None else None,
        "avg_hold_sec": float(avg_hold_sec) if avg_hold_sec is not None else None,
    }


@_softfail_read({})
def fetch_label_distribution(
    *,
    days_back: int = 30,
    src: str | None = None,
    regime: str | None = None,
    min_feature_version: int | None = None,
    wallet_tag: str | None = None,
    session_id: str | None = None,
) -> dict[str, int]:
    days_i = _clamp_int(days_back, 1, 365)
    min_feature_version = _effective_min_feature_version(min_feature_version)

    clauses: list[str] = ["o.ts >= now() - (%s::interval)"]
    params: list[Any] = [f"{days_i} days"]

    if src:
        clauses.append("c.src = %s")
        params.append(src)
    if regime:
        clauses.append("c.regime = %s")
        params.append(regime)
    if wallet_tag:
        clauses.append("c.wallet_tag = %s")
        params.append(wallet_tag)
    if session_id:
        clauses.append("c.session_id = %s")
        params.append(session_id)
    if min_feature_version is not None:
        clauses.append("c.feature_version >= %s")
        params.append(int(min_feature_version))

    where_sql = _mk_where_sql(clauses)

    sql = (
        """
        SELECT o.label, count(*) AS n
        FROM entry_outcomes o
        JOIN entry_candidates c ON c.id = o.candidate_id
        """
        + (where_sql + "\n" if where_sql else "")
        + """
        GROUP BY o.label
        """
    )

    out: dict[str, int] = {}
    with cursor_only() as cur:
        cur.execute(sql, params)
        rows = cur.fetchall() or []
    for label, n in rows:
        out[str(label)] = int(n)
    return out


@_softfail_read({"days_back": 7, "n_candidates": 0, "n_labeled": 0, "coverage": 0.0})
def fetch_outcome_coverage(
    *,
    days_back: int = 7,
    min_feature_version: int | None = None,
    src: str | None = None,
    regime: str | None = None,
    wallet_tag: str | None = None,
    session_id: str | None = None,
) -> dict[str, Any]:
    days_i = _clamp_int(days_back, 1, 365)
    min_feature_version = _effective_min_feature_version(min_feature_version)

    clauses: list[str] = ["c.ts >= now() - (%s::interval)"]
    params: list[Any] = [f"{days_i} days"]

    if src:
        clauses.append("c.src = %s")
        params.append(src)
    if regime:
        clauses.append("c.regime = %s")
        params.append(regime)
    if wallet_tag:
        clauses.append("c.wallet_tag = %s")
        params.append(wallet_tag)
    if session_id:
        clauses.append("c.session_id = %s")
        params.append(session_id)
    if min_feature_version is not None:
        clauses.append("c.feature_version >= %s")
        params.append(int(min_feature_version))

    where_sql = _mk_where_sql(clauses)

    sql = """
        SELECT count(*) AS n_candidates, count(o.id) AS n_labeled
        FROM entry_candidates c
        LEFT JOIN entry_outcomes o ON o.candidate_id = c.id
        """ + (where_sql + "\n" if where_sql else "")

    with cursor_only() as cur:
        cur.execute(sql, params)
        row = cur.fetchone()
    if not row:
        raise RuntimeError("fetch_outcome_coverage: no row")

    n_candidates, n_labeled = row
    n_candidates_i = int(n_candidates or 0)
    n_labeled_i = int(n_labeled or 0)
    coverage = float(n_labeled_i) / float(n_candidates_i) if n_candidates_i else 0.0
    return {
        "days_back": days_i,
        "n_candidates": n_candidates_i,
        "n_labeled": n_labeled_i,
        "coverage": coverage,
    }


@_softfail_read({"days_back": 30, "n_exit_events": 0, "sell_like": 0, "stop_like": 0})
def fetch_exit_event_stats(*, days_back: int = 30) -> dict[str, Any]:
    days_i = _clamp_int(days_back, 1, 365)
    sql = """
        SELECT
            count(*) AS n,
            count(*) FILTER (WHERE lower(action) LIKE 'sell%%') AS sell_like,
            count(*) FILTER (
                WHERE lower(reason) LIKE 'stop%%' OR lower(reason) LIKE 'sl%%'
            ) AS stop_like
        FROM exit_events
        WHERE ts >= now() - (%s::interval)
    """
    with cursor_only() as cur:
        cur.execute(sql, (f"{days_i} days",))
        row = cur.fetchone()
    if not row:
        raise RuntimeError("fetch_exit_event_stats: no row")
    n, sell_like, stop_like = row
    return {
        "days_back": days_i,
        "n_exit_events": int(n or 0),
        "sell_like": int(sell_like or 0),
        "stop_like": int(stop_like or 0),
    }


def fetch_quant_dashboard(
    *,
    src: str | None = None,
    regime: str | None = None,
    wallet_tag: str | None = None,
    session_id: str | None = None,
    min_feature_version: int | None = None,
    days_back_core: int = 7,
    days_back_coverage: int = 7,
    days_back_label_dist: int = 30,
    days_back_events: int = 30,
    days_back_exits: int = 30,
) -> dict[str, Any]:
    min_feature_version = _effective_min_feature_version(min_feature_version)
    return {
        "core": fetch_basic_stats(
            days_back=days_back_core,
            src=src,
            regime=regime,
            wallet_tag=wallet_tag,
            session_id=session_id,
            min_feature_version=min_feature_version,
        ),
        "coverage": fetch_outcome_coverage(
            days_back=days_back_coverage,
            src=src,
            regime=regime,
            wallet_tag=wallet_tag,
            session_id=session_id,
            min_feature_version=min_feature_version,
        ),
        "label_distribution": fetch_label_distribution(
            days_back=days_back_label_dist,
            src=src,
            regime=regime,
            wallet_tag=wallet_tag,
            session_id=session_id,
            min_feature_version=min_feature_version,
        ),
        "exit_event_stats": fetch_exit_event_stats(days_back=days_back_exits),
        "ai_events_summary": fetch_ai_event_summary(days_back=days_back_events),
    }


def get_env_scoped_dashboard() -> dict[str, Any]:
    return fetch_quant_dashboard(
        src=AI_DB_DEFAULT_SRC or None,
        regime=AI_DB_DEFAULT_REGIME or None,
        wallet_tag=AI_DB_DEFAULT_WALLET_TAG or None,
        session_id=None,
        min_feature_version=AI_DB_MIN_FEATURE_VERSION,
    )


# =============================================================================
# Housekeeping
# =============================================================================
@_softfail_write(0)
def prune_old_ai_data(*, max_age_days: int = 365) -> int:
    max_age_days_i = max(1, int(max_age_days))
    sql = """
        WITH deleted AS (
            DELETE FROM entry_candidates
            WHERE ts < now() - (%s::interval)
            RETURNING 1
        )
        SELECT count(*) FROM deleted
    """
    with cursor_only() as cur:
        cur.execute(sql, (f"{max_age_days_i} days",))
        row = cur.fetchone()
    return int(row[0] or 0) if row else 0


_PRUNE_TABLES: tuple[str, ...] = (
    "ai_events",
    "exit_events",
    "position_events",
    "regime_snapshots",
    "trade_executions",
    "strategy_params",
    "risk_assessments",
)


def _delete_older_than(cur: Any, table: str, age_days: int) -> int:
    """Delete from a whitelisted table name; uses identifier-safe composition when available."""
    if table not in _PRUNE_TABLES:
        return 0

    interval_val = f"{max(1, int(age_days))} days"

    try:
        from psycopg2 import sql as _sql  # type: ignore

        stmt = _sql.SQL("DELETE FROM {} WHERE ts < now() - (%s::interval)").format(
            _sql.Identifier(table)
        )
        cur.execute(stmt, (interval_val,))
        return int(cur.rowcount or 0)
    except Exception:
        pass

    try:
        from psycopg import sql as _sql3  # type: ignore

        stmt = _sql3.SQL("DELETE FROM {} WHERE ts < now() - (%s::interval)").format(
            _sql3.Identifier(table)
        )
        cur.execute(stmt, (interval_val,))
        return int(cur.rowcount or 0)
    except Exception:
        pass

    cur.execute(  # noqa: S608
        "DELETE FROM " + table + " WHERE ts < now() - (%s::interval)",
        (interval_val,),
    )
    return int(cur.rowcount or 0)


@_softfail_write(0)
def prune_old_events(*, max_age_days: int = 365) -> int:
    max_age_days_i = max(1, int(max_age_days))
    total = 0
    with conn_cursor() as (_conn, cur):
        for tbl in _PRUNE_TABLES:
            with suppress(Exception):
                total += _delete_older_than(cur, tbl, max_age_days_i)
    return int(total)


def ai_db_status() -> dict[str, Any]:
    return {
        "enabled": bool(AI_DB_ENABLED),
        "read_only": bool(AI_DB_READ_ONLY),
        "dsn": describe_dsn_sources(),
        "backend": describe_backend(),
    }


# =============================================================================
# Async wrappers (so asyncio bot won't block)
# =============================================================================
async def abootstrap_ai_db() -> bool:
    return await asyncio.to_thread(bootstrap_ai_db)


async def ainsert_entry_candidate(**kwargs: Any) -> int:
    return await asyncio.to_thread(insert_entry_candidate, **kwargs)


async def amark_outcome_for_candidate(candidate_id: int, **kwargs: Any) -> int:
    return await asyncio.to_thread(mark_outcome_for_candidate, candidate_id, **kwargs)


async def ainsert_trade_execution(**kwargs: Any) -> int:
    return await asyncio.to_thread(insert_trade_execution, **kwargs)


async def arecord_trade_and_outcome(**kwargs: Any) -> dict[str, int]:
    return await asyncio.to_thread(record_trade_and_outcome, **kwargs)


async def ainsert_exit_event(**kwargs: Any) -> int:
    return await asyncio.to_thread(insert_exit_event, **kwargs)


async def arecord_exit_event_and_outcome(**kwargs: Any) -> dict[str, int]:
    return await asyncio.to_thread(record_exit_event_and_outcome, **kwargs)


async def alog_ai_event(**kwargs: Any) -> int:
    return await asyncio.to_thread(log_ai_event, **kwargs)


async def afetch_quant_dashboard(**kwargs: Any) -> dict[str, Any]:
    return await asyncio.to_thread(fetch_quant_dashboard, **kwargs)


async def afetch_training_rows(**kwargs: Any) -> list[Row]:
    return await asyncio.to_thread(fetch_training_rows, **kwargs)


async def aget_env_scoped_dashboard() -> dict[str, Any]:
    return await asyncio.to_thread(get_env_scoped_dashboard)


async def aai_db_status() -> dict[str, Any]:
    return await asyncio.to_thread(ai_db_status)


async def ainsert_strategy_param(**kwargs: Any) -> int:
    return await asyncio.to_thread(insert_strategy_param, **kwargs)


async def ainsert_risk_assessment(**kwargs: Any) -> int:
    return await asyncio.to_thread(insert_risk_assessment, **kwargs)


__all__ = [
    # flags
    "SCHEMA_VERSION",
    "MIGRATIONS",
    "MIGRATION_PLAN",
    "AI_DB_ENABLED",
    "AI_DB_MIN_FEATURE_VERSION",
    "AI_DB_READ_ONLY",
    "WIN_LABELS",
    # dsn / ssl helpers
    "DSNInfo",
    "aai_db_status",
    # async wrappers
    "abootstrap_ai_db",
    "afetch_quant_dashboard",
    "afetch_training_rows",
    "aget_env_scoped_dashboard",
    "ai_db_status",
    "ainsert_entry_candidate",
    "ainsert_exit_event",
    "ainsert_risk_assessment",
    "ainsert_strategy_param",
    "ainsert_trade_execution",
    "alog_ai_event",
    "amark_outcome_for_candidate",
    "arecord_exit_event_and_outcome",
    "arecord_trade_and_outcome",
    "asyncpg_connect",
    "bootstrap_ai_db",
    "close_asyncpg_pool",
    "describe_backend",
    "describe_dsn_sources",
    "dsn_wants_ssl",
    "dump_training_jsonl_to_file",
    "ensure_ai_tables",
    "fetch_ai_event_summary",
    "fetch_basic_stats",
    "fetch_candidate_by_id",
    "fetch_exit_event_stats",
    "fetch_label_distribution",
    "fetch_latest_candidates",
    "fetch_latest_risk_assessments",
    "fetch_latest_strategy_params",
    "fetch_outcome_coverage",
    "fetch_quant_dashboard",
    "fetch_recent_exit_events",
    "fetch_recent_outcomes",
    "fetch_recent_regime_snapshots",
    "fetch_recent_trade_executions",
    "fetch_training_rows",
    "fetch_unlabeled_candidates",
    "get_asyncpg_pool",
    "get_env_scoped_dashboard",
    "get_normalized_dsn_for_debug",
    # writes/reads
    "insert_entry_candidate",
    "insert_entry_candidates_bulk",
    "insert_entry_outcome",
    "insert_exit_event",
    "insert_regime_snapshot",
    "insert_risk_assessment",
    # strategy/risk
    "insert_strategy_param",
    "insert_trade_execution",
    "iter_backtest_rows",
    "iter_training_jsonl",
    "iter_training_rows",
    "log_ai_event",
    "mark_outcome_for_candidate",
    # health / boot
    "ping",
    "prune_old_ai_data",
    "prune_old_events",
    "record_exit_event_and_outcome",
    "record_trade_and_outcome",
    "reset_dsn_cache",
    "resolve_dsn_info",
    "ssl_context_for_dsn",
    "update_entry_decision",
]
