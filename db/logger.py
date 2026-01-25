from __future__ import annotations

"""
db/logger.py

Canonical DB Writer Utilities (LOCK-IN / PROD)
=============================================

Role
----
This module is the single safe way to write operational data to Postgres:

  - scan_batches, scan_rows
  - trades
  - errors
  - pnl_snapshots

Hard rules
----------
1) DSN must be resolved via utils_pkg.db_dsn.pick_asyncpg_dsn()
   (query params stripped; ssl mapped correctly; Render/local identical)
2) DB logging must NEVER break the trading loop by default.
   (DB_LOGGER_STRICT=1 turns failures into exceptions)
3) Writes should be non-blocking whenever possible:
   - background queue is used for most inserts
   - "need a RETURNING id" ops run inline (scan batch insert)

Import-safety
-------------
- This module MUST be importable even if asyncpg is missing/broken (common on Windows).
- DB logging becomes a no-op unless DB_LOGGER_STRICT=1.

Env knobs
---------
Pool:
  DB_POOL_MIN=1
  DB_POOL_MAX=5
  DB_POOL_MAX_INACTIVE_CONN_LIFETIME=300
  DB_POOL_COMMAND_TIMEOUT=30
  DB_POOL_CREATE_TIMEOUT_S=10         (pool creation wrapper timeout)
  DB_POOL_FAIL_COOLDOWN_S=30          (cooldown after pool creation failure)

Writer queue:
  DB_LOGGER_QUEUE=1                 (enable queue; default on)
  DB_LOGGER_QUEUE_MAX=2000
  DB_LOGGER_FLUSH_INTERVAL_MS=20    (wake interval)
  DB_LOGGER_BATCH_MAX=64            (max jobs per one acquired connection)
  DB_LOGGER_RETRIES=2
  DB_LOGGER_BACKOFF_MS=80
  DB_LOGGER_STOP_FLUSH_SEC=1        (best-effort drain on stop)

Behavior:
  DB_LOGGER_ENABLED=1               (default on)
  DB_LOGGER_STRICT=0                (raise exceptions if 1)
  DB_LOGGER_ENSURE_SCHEMA=1         (attempt ensure_schema on missing tables)
  DB_LOGGER_ENSURE_SCHEMA_TTL=60    (seconds, rate-limit)

Schema:
  PG_SCHEMA=public                  (db/schema.py controls schema/search_path;
                                     logger uses unqualified table names)

Health
------
- healthcheck() / ping() return a structured dict and do not raise by default.
- Set DB_LOGGER_STRICT=1 if you want failures to hard-fail other code paths.
- healthcheck(raise_on_fail=True) will raise on failure.

Public API (stable)
-------------------
- start_writer(), stop_writer()
- close_pool()
- get_writer()

- log_scan_batch(...)
- log_scan_row(...)
- log_trade_event(...)
- log_error_event(...)
- log_pnl_snapshot(...)

Back-compat shims:
- log_candidate_async / log_candidate
- log_trade_async / log_trade
- record_legacy_trade
"""

import asyncio
import contextlib
import hashlib
import json
import logging
import os
import time
from collections.abc import Awaitable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from utils_pkg.db_dsn import mask_dsn, pick_asyncpg_dsn

log = logging.getLogger("db.logger")

_TRUE = {"1", "true", "t", "yes", "y", "on"}
_FALSE = {"0", "false", "f", "no", "n", "off"}


# -----------------------------------------------------------------------------
# Optional asyncpg (import-safe) + "broken build" self-check
# -----------------------------------------------------------------------------
_ASYNCPG_OK = True
_ASYNCPG_IMPORT_ERR = ""
_ASYNCPG_SELFTEST_DONE = False

try:  # asyncpg is Linux/Render-first; may be missing/broken on Windows
    import asyncpg  # type: ignore
except Exception as err:  # pragma: no cover
    asyncpg = None  # type: ignore
    _ASYNCPG_OK = False
    _ASYNCPG_IMPORT_ERR = str(err)

if TYPE_CHECKING:  # pragma: no cover
    import asyncpg as _asyncpg

    PoolT = _asyncpg.Pool
    ConnT = _asyncpg.Connection
else:
    PoolT = Any
    ConnT = Any


def _env_int(k: str, default: int) -> int:
    try:
        return int(float((os.getenv(k, "") or "").strip() or default))
    except Exception:
        return default


def _env_float(k: str, default: float) -> float:
    try:
        return float((os.getenv(k, "") or "").strip() or default)
    except Exception:
        return default


def _env_bool(k: str, default: bool = False) -> bool:
    v = os.getenv(k)
    if v is None:
        return default
    s = str(v).strip().lower()
    if s in _TRUE:
        return True
    if s in _FALSE:
        return False
    return default


def _logger_enabled() -> bool:
    return _env_bool("DB_LOGGER_ENABLED", True)


def _strict_mode() -> bool:
    return _env_bool("DB_LOGGER_STRICT", False)


def _now_utc() -> datetime:
    return datetime.now(UTC)


def _as_ts(dt_or_iso: Any | None) -> datetime:
    if isinstance(dt_or_iso, datetime):
        return dt_or_iso if dt_or_iso.tzinfo else dt_or_iso.replace(tzinfo=UTC)
    if isinstance(dt_or_iso, str):
        s = dt_or_iso.strip()
        if s:
            try:
                ts = datetime.fromisoformat(s.replace("Z", "+00:00"))
                return ts if ts.tzinfo else ts.replace(tzinfo=UTC)
            except Exception:
                pass
    return _now_utc()


def _json_default(o: Any) -> Any:
    try:
        return str(o)
    except Exception:
        return None


def _j(o: Any) -> Any:
    """
    asyncpg JSONB accepts dict/list directly. This function normalizes input:
      - dict/list -> as-is
      - str -> json loads if possible, else {"_raw": "..."}
      - other -> json roundtrip with fallback
    """
    if o is None:
        return None
    if isinstance(o, (dict, list)):
        return o
    if isinstance(o, str):
        s = o.strip()
        if not s:
            return None
        try:
            return json.loads(s)
        except Exception:
            return {"_raw": s}
    try:
        return json.loads(json.dumps(o, default=_json_default))
    except Exception:
        return {"_raw": str(o)}


def _f(x: Any) -> float | None:
    if x is None or x == "":
        return None
    try:
        return float(x)
    except Exception:
        return None


def _i(x: Any) -> int | None:
    if x is None or x == "":
        return None
    try:
        return int(x)
    except Exception:
        try:
            return int(float(x))
        except Exception:
            return None


def _fingerprint(where: str, err: str) -> str:
    s = (where or "")[:256] + "|" + (err or "")[:2048]
    return hashlib.sha256(s.encode("utf-8", "ignore")).hexdigest()[:16]


def _schedule(coro: Awaitable[Any]) -> None:
    """
    Safe task scheduling:
    - If no running loop, do nothing (unless strict -> raise).
    """
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError as err:
        if _strict_mode():
            raise RuntimeError("No running event loop for db logger scheduling") from err
        return
    loop.create_task(coro)


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
        # basic surface
        if not callable(getattr(asyncpg, "create_pool", None)):
            raise RuntimeError("asyncpg.create_pool missing")
        if not callable(getattr(asyncpg, "connect", None)):
            raise RuntimeError("asyncpg.connect missing")

        # critical compiled module import (common broken-wheel failure point)
        with contextlib.suppress(Exception):
            __import__("asyncpg.protocol.protocol")

        # If the above import actually failed, it would have thrown; but we suppressed.
        # So, do a stricter import attempt:
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
# Pool (singleton) - created only on demand + failure cooldown
# -----------------------------------------------------------------------------
_POOL: PoolT | None = None
_POOL_LOCK = asyncio.Lock()

_POOL_FAIL_UNTIL = 0.0
_POOL_LAST_ERR: str = ""
_POOL_LAST_OK_AT = 0.0

_LAST_SCHEMA_ENSURE_AT = 0.0
_SCHEMA_ENSURE_LOCK = asyncio.Lock()


def _sqlstate(e: BaseException) -> str:
    return str(getattr(e, "sqlstate", "") or "")


def _is_missing_table_err(e: BaseException) -> bool:
    st = _sqlstate(e)
    if st in {"42P01", "42703", "42883"}:  # undefined_table/column/function
        return True
    if _can_use_asyncpg():
        return isinstance(
            e,
            (
                asyncpg.UndefinedTableError,  # type: ignore[attr-defined]
                asyncpg.UndefinedColumnError,  # type: ignore[attr-defined]
                asyncpg.UndefinedFunctionError,  # type: ignore[attr-defined]
            ),
        )
    return False


def _is_transient_err(e: BaseException) -> bool:
    st = _sqlstate(e)
    if st.startswith("08") or st in {"53300", "57P03"}:  # connection_exception-ish
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


def _pick_dsn_flex(*, timeout_s: float | None, app_name: str | None) -> tuple[str, dict[str, Any]]:
    """
    Call pick_asyncpg_dsn in a signature-flex way so we never break if the helper evolves.
    """
    try:
        import inspect

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
    Ensure we only pass asyncpg-safe kwargs. Also strips None values.

    We intentionally keep an allowlist that matches common asyncpg.connect args.
    Unknown keys are dropped to avoid TypeError and "libpq-only" leakage.
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
    }

    # Some versions accept these; keep if present (harmless if ignored in pick_dsn).
    # If asyncpg rejects them, we'd rather drop them here than crash pool creation.
    maybe_allowed = {"application_name", "prepared_statement_cache_size"}
    allowed |= maybe_allowed

    cleaned: dict[str, Any] = {}
    for k, v in connect_kwargs.items():
        if v is None:
            continue
        if k in allowed:
            cleaned[k] = v

    # Normalize application name into server_settings if present.
    app_name = cleaned.pop("application_name", None)
    if app_name:
        ss = cleaned.get("server_settings")
        if not isinstance(ss, dict):
            ss = {}
        ss.setdefault("application_name", str(app_name))
        cleaned["server_settings"] = ss

    return cleaned


async def _reset_pool_soft(reason: str) -> None:
    """
    Soft pool reset: close and clear the pool. Never raises unless strict.
    """
    global _POOL, _POOL_FAIL_UNTIL, _POOL_LAST_ERR
    try:
        await close_pool()
    except Exception as err:
        if _strict_mode():
            raise
        log.debug("db.pool: reset failed reason=%s err=%s", reason, str(err))
    cooldown = float(_env_int("DB_POOL_FAIL_COOLDOWN_S", 30))
    _POOL_FAIL_UNTIL = max(_POOL_FAIL_UNTIL, time.time() + cooldown)
    _POOL_LAST_ERR = reason


async def _get_pool(*, timeout_s: float | None = None, app_name: str | None = None) -> PoolT | None:
    """
    Return the asyncpg pool or None if DB logging is disabled/unavailable and not strict.
    """
    global _POOL, _POOL_FAIL_UNTIL, _POOL_LAST_ERR, _POOL_LAST_OK_AT

    if not _logger_enabled():
        return None

    if not _can_use_asyncpg():
        msg = f"asyncpg unavailable: {_ASYNCPG_IMPORT_ERR or 'not installed'}"
        if _strict_mode():
            raise RuntimeError(msg)
        # log once-ish without spamming
        if int(time.time()) % 60 == 0:
            log.warning("db.logger disabled (%s)", msg)
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
                raise RuntimeError("Postgres DSN not set (expected DATABASE_URL / PG_DSN_* / etc.)")
            return None

        min_size = _env_int("DB_POOL_MIN", 1)
        max_size = _env_int("DB_POOL_MAX", 5)
        cmd_timeout = _env_float("DB_POOL_COMMAND_TIMEOUT", 30.0)
        idle = _env_float("DB_POOL_MAX_INACTIVE_CONN_LIFETIME", 300.0)

        kwargs = _sanitize_connect_kwargs(dict(connect_kwargs or {}))
        kwargs.setdefault("command_timeout", float(cmd_timeout))

        # Ensure there is a connection timeout during pool creation if the helper didn't provide one.
        if timeout_s is not None and "timeout" not in kwargs:
            kwargs["timeout"] = float(timeout_s)

        create_timeout = float(_env_float("DB_POOL_CREATE_TIMEOUT_S", 10.0))
        cooldown = float(_env_int("DB_POOL_FAIL_COOLDOWN_S", 30))

        log.info(
            "db.pool: create_pool dsn=%s min=%s max=%s cmd_timeout=%ss idle=%ss",
            mask_dsn(dsn),
            min_size,
            max_size,
            cmd_timeout,
            idle,
        )

        try:
            coro = asyncpg.create_pool(  # type: ignore[union-attr]
                dsn=dsn,
                min_size=min_size,
                max_size=max_size,
                max_inactive_connection_lifetime=float(idle),
                **kwargs,
            )
            _POOL = await asyncio.wait_for(coro, timeout=create_timeout)
            _POOL_LAST_ERR = ""
            _POOL_FAIL_UNTIL = 0.0
            _POOL_LAST_OK_AT = time.time()
        except Exception as err:
            _POOL = None
            _POOL_LAST_ERR = f"create_pool failed: {err}"
            _POOL_FAIL_UNTIL = time.time() + cooldown
            if _strict_mode():
                raise RuntimeError(f"Failed to create asyncpg pool dsn={mask_dsn(dsn)}") from err
            log.warning("db.pool: create_pool failed dsn=%s err=%s", mask_dsn(dsn), str(err))
            return None

        return _POOL


async def close_pool() -> None:
    global _POOL
    async with _POOL_LOCK:
        if _POOL is not None:
            try:
                with contextlib.suppress(Exception):
                    await _POOL.close()  # type: ignore[union-attr]
            finally:
                _POOL = None


# -----------------------------------------------------------------------------
# Schema ensure (rate-limited)
# -----------------------------------------------------------------------------
async def _maybe_ensure_schema(reason: str) -> None:
    if not _env_bool("DB_LOGGER_ENSURE_SCHEMA", True):
        return

    ttl = float(_env_int("DB_LOGGER_ENSURE_SCHEMA_TTL", 60))
    now = time.time()
    global _LAST_SCHEMA_ENSURE_AT

    if now - _LAST_SCHEMA_ENSURE_AT < ttl:
        return

    async with _SCHEMA_ENSURE_LOCK:
        now2 = time.time()
        if now2 - _LAST_SCHEMA_ENSURE_AT < ttl:
            return
        _LAST_SCHEMA_ENSURE_AT = now2

        try:
            from db.schema import ensure_schema  # lazy import (avoid circulars)

            log.warning("db.logger: ensure_schema() triggered reason=%s", reason)
            await ensure_schema()
        except Exception as err:
            log.warning("db.logger: ensure_schema failed err=%s", str(err))


# -----------------------------------------------------------------------------
# Healthcheck contract (structured, non-throwing by default)
# -----------------------------------------------------------------------------
async def healthcheck(
    *,
    timeout_s: float = 2.0,
    raise_on_fail: bool = False,
    app_name: str | None = "db.logger.healthcheck",
) -> dict[str, Any]:
    """
    Structured health result. Never raises unless raise_on_fail=True.
    """
    enabled = _logger_enabled()
    strict = _strict_mode()
    asyncpg_ok = _can_use_asyncpg()
    result: dict[str, Any] = {
        "ok": False,
        "enabled": bool(enabled),
        "strict": bool(strict),
        "asyncpg_ok": bool(asyncpg_ok),
        "asyncpg_err": _ASYNCPG_IMPORT_ERR if not asyncpg_ok else "",
        "dsn_set": False,
        "pool": {"ok": False, "last_err": _POOL_LAST_ERR, "last_ok_at": _POOL_LAST_OK_AT},
        "query": {"ok": False, "latency_ms": None, "err": ""},
        "ts": _now_utc().isoformat(),
    }

    if not enabled:
        # If disabled, treat as "ok" for system health unless caller wants strictness.
        result["ok"] = True
        return result

    if not asyncpg_ok:
        if raise_on_fail:
            raise RuntimeError(result["asyncpg_err"] or "asyncpg unavailable")
        return result

    # Try to get pool (non-throwing even under strict unless raise_on_fail)
    try:
        pool = await _get_pool(timeout_s=float(timeout_s), app_name=app_name)
    except Exception as err:
        result["pool"]["ok"] = False
        result["pool"]["last_err"] = str(err)
        if raise_on_fail:
            raise
        return result

    if pool is None:
        # DSN missing or in cooldown; reflect best guess
        # Try to determine DSN presence without exposing secrets:
        dsn, _ = _pick_dsn_flex(timeout_s=float(timeout_s), app_name=app_name)
        result["dsn_set"] = bool(dsn)
        if not dsn:
            # If DSN is not set, treat as ok=false (caller decides).
            if raise_on_fail:
                raise RuntimeError("DSN not set")
            return result
        if raise_on_fail:
            raise RuntimeError("Pool unavailable")
        return result

    result["pool"]["ok"] = True
    dsn, _ = _pick_dsn_flex(timeout_s=float(timeout_s), app_name=app_name)
    result["dsn_set"] = bool(dsn)

    # SELECT 1 probe with timeout
    t0 = time.perf_counter()
    try:
        async with pool.acquire() as conn:
            await asyncio.wait_for(conn.fetchval("SELECT 1"), timeout=float(timeout_s))
        ms = int((time.perf_counter() - t0) * 1000.0)
        result["query"]["ok"] = True
        result["query"]["latency_ms"] = ms
        result["ok"] = True
        return result
    except Exception as err:
        ms = int((time.perf_counter() - t0) * 1000.0)
        result["query"]["ok"] = False
        result["query"]["latency_ms"] = ms
        result["query"]["err"] = str(err)

        # If it looks transient, reset pool softly (soft mode only).
        if _is_transient_err(err) and not strict:
            await _reset_pool_soft(reason=f"healthcheck transient: {err}")

        if raise_on_fail:
            raise RuntimeError(f"DB healthcheck failed: {err}") from err
        return result


async def ping(*, timeout_s: float = 2.0) -> bool:
    """
    Simple bool wrapper around healthcheck().
    """
    r = await healthcheck(timeout_s=timeout_s, raise_on_fail=False)
    return bool(r.get("ok", False))


# -----------------------------------------------------------------------------
# Writer queue
# -----------------------------------------------------------------------------
@dataclass(frozen=True)
class _Job:
    sql: str
    args: tuple[Any, ...]
    tag: str


class DBWriter:
    """
    Queue-backed writer. No DB work happens until you call start_writer()/get_writer().
    """

    def __init__(self) -> None:
        self._queue_enabled = _env_bool("DB_LOGGER_QUEUE", True)
        self._qmax = _env_int("DB_LOGGER_QUEUE_MAX", 2000)
        self._flush_ms = max(1, _env_int("DB_LOGGER_FLUSH_INTERVAL_MS", 20))
        self._batch_max = max(1, _env_int("DB_LOGGER_BATCH_MAX", 64))
        self._retries = max(0, _env_int("DB_LOGGER_RETRIES", 2))
        self._backoff_ms = max(1, _env_int("DB_LOGGER_BACKOFF_MS", 80))
        self._stop_flush_sec = max(0.0, _env_float("DB_LOGGER_STOP_FLUSH_SEC", 1.0))
        self._strict = _strict_mode()

        self._q: asyncio.Queue[_Job] = asyncio.Queue(maxsize=self._qmax)
        self._task: asyncio.Task[None] | None = None
        self._stop = asyncio.Event()

        self.enqueued = 0
        self.dropped = 0
        self.written = 0
        self.failed = 0

    async def start(self) -> None:
        if self._task is not None and not self._task.done():
            return
        self._stop.clear()
        if self._queue_enabled:
            self._task = asyncio.create_task(self._loop(), name="db-writer")
            log.info(
                "db.writer: started queue_enabled=1 qmax=%s flush_ms=%s batch_max=%s retries=%s",
                self._qmax,
                self._flush_ms,
                self._batch_max,
                self._retries,
            )
        else:
            log.info("db.writer: started queue_enabled=0 (inline scheduling)")

    async def stop(self) -> None:
        self._stop.set()
        # best-effort drain
        if self._queue_enabled and self._stop_flush_sec > 0:
            deadline = time.monotonic() + float(self._stop_flush_sec)
            while (time.monotonic() < deadline) and (not self._q.empty()):
                await asyncio.sleep(0.01)

        if self._task is not None:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await self._task
            self._task = None

    def submit(self, sql: str, *args: Any, tag: str = "db") -> None:
        """
        Fire-and-forget write:
          - queued if enabled
          - inline scheduled if queue disabled
        Never raises unless DB_LOGGER_STRICT=1 and scheduling/queueing fails.
        """
        if not self._queue_enabled:
            _schedule(self._exec(sql, tuple(args), tag=tag))
            return

        try:
            self._q.put_nowait(_Job(sql=sql, args=tuple(args), tag=tag))
            self.enqueued += 1
        except asyncio.QueueFull as err:
            self.dropped += 1
            if self._strict:
                raise RuntimeError("db.writer queue full") from err
            if self.dropped % 250 == 1:
                log.warning("db.writer: queue_full dropped=%s qmax=%s", self.dropped, self._qmax)

    async def exec_inline(self, sql: str, *args: Any, tag: str = "db") -> None:
        await self._exec(sql, tuple(args), tag=tag)

    async def fetchval_inline(self, sql: str, *args: Any, tag: str = "db") -> Any:
        """
        Inline operation that needs a return value (e.g. scan_batches id).
        """
        pool = await _get_pool(timeout_s=float(_env_float("DB_POOL_CREATE_TIMEOUT_S", 10.0)))
        if pool is None:
            return None

        try:
            async with pool.acquire() as conn:
                return await conn.fetchval(sql, *args)
        except Exception as err:
            if _is_missing_table_err(err):
                await _maybe_ensure_schema(reason=f"{tag}:missing_table")
                try:
                    async with pool.acquire() as conn:
                        return await conn.fetchval(sql, *args)
                except Exception as err2:
                    if _is_transient_err(err2) and not self._strict:
                        await _reset_pool_soft(reason=f"fetchval transient: {err2}")
                    if self._strict:
                        raise RuntimeError(f"db.fetchval failed tag={tag}") from err2
                    log.debug("db.fetchval failed tag=%s err=%s", tag, str(err2))
                    return None
            if _is_transient_err(err) and not self._strict:
                await _reset_pool_soft(reason=f"fetchval transient: {err}")
            if self._strict:
                raise RuntimeError(f"db.fetchval failed tag={tag}") from err
            log.debug("db.fetchval failed tag=%s err=%s", tag, str(err))
            return None

    async def _loop(self) -> None:
        while not self._stop.is_set():
            try:
                first = await asyncio.wait_for(self._q.get(), timeout=self._flush_ms / 1000.0)
            except asyncio.TimeoutError:
                continue
            except Exception:
                continue

            jobs: list[_Job] = [first]
            # drain up to batch_max quickly
            for _ in range(self._batch_max - 1):
                try:
                    jobs.append(self._q.get_nowait())
                except asyncio.QueueEmpty:
                    break
                except Exception:
                    break

            try:
                await self._exec_batch(jobs)
                self.written += len(jobs)
            except Exception as err:
                self.failed += len(jobs)
                if self._strict:
                    raise RuntimeError("db.writer batch failed") from err
                log.debug("db.writer: batch failed err=%s", str(err))
            finally:
                for _ in jobs:
                    with contextlib.suppress(Exception):
                        self._q.task_done()

    async def _exec_batch(self, jobs: list[_Job]) -> None:
        pool = await _get_pool(timeout_s=float(_env_float("DB_POOL_CREATE_TIMEOUT_S", 10.0)))
        if pool is None:
            return

        # One acquire per batch (cuts overhead).
        async with pool.acquire() as conn:
            for job in jobs:
                await self._exec_on_conn(conn, job.sql, job.args, tag=job.tag)

    async def _exec(self, sql: str, args: tuple[Any, ...], *, tag: str) -> None:
        pool = await _get_pool(timeout_s=float(_env_float("DB_POOL_CREATE_TIMEOUT_S", 10.0)))
        if pool is None:
            return

        async with pool.acquire() as conn:
            await self._exec_on_conn(conn, sql, args, tag=tag)

    async def _exec_on_conn(
        self, conn: ConnT, sql: str, args: tuple[Any, ...], *, tag: str
    ) -> None:
        attempt = 0
        while True:
            try:
                await conn.execute(sql, *args)
                return
            except Exception as err:
                if _is_missing_table_err(err):
                    await _maybe_ensure_schema(reason=f"{tag}:missing_table")
                    attempt += 1
                    if attempt <= 1:
                        continue

                if _is_transient_err(err) and attempt < self._retries:
                    attempt += 1
                    await asyncio.sleep((self._backoff_ms * attempt) / 1000.0)
                    continue

                if _is_transient_err(err) and not self._strict:
                    await _reset_pool_soft(reason=f"exec transient: {err}")

                if self._strict:
                    raise RuntimeError(f"db.exec failed tag={tag} attempt={attempt}") from err
                log.debug("db.exec failed tag=%s attempt=%s err=%s", tag, attempt, str(err))
                return


_WRITER: DBWriter | None = None
_WRITER_LOCK = asyncio.Lock()


async def get_writer() -> DBWriter:
    global _WRITER
    if _WRITER is not None:
        return _WRITER
    async with _WRITER_LOCK:
        if _WRITER is None:
            _WRITER = DBWriter()
            await _WRITER.start()
        return _WRITER


async def start_writer() -> None:
    w = await get_writer()
    await w.start()


async def stop_writer() -> None:
    global _WRITER
    async with _WRITER_LOCK:
        if _WRITER is not None:
            await _WRITER.stop()
            _WRITER = None


# -----------------------------------------------------------------------------
# Canonical inserts (aligned to db/schema.py)
# -----------------------------------------------------------------------------
async def log_scan_batch(
    when_iso: Any,
    total: int,
    kept: int,
    reason_top: str,
    *,
    source: str | None = None,
    scanner_version: str | None = None,
    dropped: int | None = None,
    elapsed_ms: int | None = None,
    reason_counts: dict[str, Any] | None = None,
    meta: dict[str, Any] | None = None,
) -> int | None:
    """
    Insert scan iteration summary. Returns batch_id (or None on failure).
    """
    ts = _as_ts(when_iso)
    total_i = int(total or 0)
    kept_i = int(kept or 0)
    dropped_i = int(dropped if dropped is not None else max(0, total_i - kept_i))

    q = """
    INSERT INTO scan_batches
      (ts, source, scanner_version, total, kept, dropped, elapsed_ms, reason_top, reason_counts, meta)
    VALUES
      ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
    RETURNING id
    """

    w = await get_writer()
    bid = await w.fetchval_inline(
        q,
        ts,
        source,
        scanner_version,
        total_i,
        kept_i,
        dropped_i,
        _i(elapsed_ms),
        str(reason_top or ""),
        _j(reason_counts),
        _j(meta),
        tag="scan_batch",
    )
    return int(bid) if bid is not None else None


async def log_scan_row(row: dict[str, Any], *, batch_id: int | None = None) -> None:
    """
    Insert one candidate row into scan_rows (normalized + raw JSONB).
    """
    ts = _now_utc()

    mint = row.get("mint") or row.get("token_address") or row.get("address")
    symbol = row.get("symbol") or row.get("sym")
    source = row.get("source") or row.get("scanner") or row.get("origin")

    age_min = _f(row.get("age_min") or row.get("pool_age_min") or row.get("ageMin"))
    liq_usd = _f(
        row.get("liq_usd") or row.get("liquidity_usd") or row.get("liquidityUSD") or row.get("liq")
    )
    vol_5m = _f(
        row.get("vol_5m_usd") or row.get("vol5") or row.get("vol5m") or row.get("volume_5m_usd")
    )
    vol_1h = _f(row.get("vol_1h_usd") or row.get("vol1h") or row.get("volume_1h_usd"))
    buys_5m = _i(row.get("buys_5m") or row.get("buys5") or row.get("buys5m"))
    sells_5m = _i(row.get("sells_5m") or row.get("sells5") or row.get("sells5m"))
    chg_5m = _f(row.get("chg_5m_pct") or row.get("chg5") or row.get("change_5m_pct"))
    impact_1k_bps = _f(row.get("impact_1k_bps") or row.get("priceImpact1kBps"))
    score = _f(row.get("score"))

    route_hint = row.get("route_hint") or row.get("route") or row.get("router_hint")

    q = """
    INSERT INTO scan_rows
      (ts, batch_id, source, mint, symbol, score, liq_usd, vol_5m_usd, vol_1h_usd,
       buys_5m, sells_5m, chg_5m_pct, age_min, impact_1k_bps, route_hint, raw)
    VALUES
      ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16)
    """

    w = await get_writer()
    w.submit(
        q,
        ts,
        _i(batch_id),
        source,
        mint,
        symbol,
        score,
        liq_usd,
        vol_5m,
        vol_1h,
        buys_5m,
        sells_5m,
        chg_5m,
        age_min,
        impact_1k_bps,
        str(route_hint) if route_hint is not None else None,
        _j(row),
        tag="scan_row",
    )


async def log_error_event(where: str, err: str, *, context: dict[str, Any] | None = None) -> None:
    """
    Insert an error row into errors (never raises by default).
    """
    ts = _now_utc()
    fp = _fingerprint(where or "", err or "")

    q = """
    INSERT INTO errors (ts, where_, err, fingerprint, context)
    VALUES ($1,$2,$3,$4,$5)
    """

    w = await get_writer()
    w.submit(q, ts, str(where or ""), str(err or ""), fp, _j(context), tag="error")


async def log_pnl_snapshot(
    when_iso: Any,
    equity_usd: Any,
    open_risk_usd: Any,
    *,
    realized_pnl_usd: Any = None,
    unrealized_pnl_usd: Any = None,
    positions_count: Any = None,
    meta: dict[str, Any] | None = None,
) -> None:
    ts = _as_ts(when_iso)

    q = """
    INSERT INTO pnl_snapshots
      (ts, equity_usd, open_risk_usd, realized_pnl_usd, unrealized_pnl_usd, positions_count, meta)
    VALUES
      ($1,$2,$3,$4,$5,$6,$7)
    """

    w = await get_writer()
    w.submit(
        q,
        ts,
        _f(equity_usd),
        _f(open_risk_usd),
        _f(realized_pnl_usd),
        _f(unrealized_pnl_usd),
        _i(positions_count),
        _j(meta),
        tag="pnl",
    )


# -----------------------------------------------------------------------------
# Trades (canonical)
# -----------------------------------------------------------------------------
def _trade_upsert_sql() -> str:
    # Relies on partial unique index ux_trades_signature_not_null (created in db/schema.py).
    return """
    INSERT INTO trades
      (ts, type, status, mint, symbol, qty_token, sol_spent, sol_received, usd_value,
       entry_sol_per_token, slippage_bps, signature, route, cu_price, jito_tip, err, meta)
    VALUES
      ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17)
    ON CONFLICT (signature) WHERE signature IS NOT NULL
    DO UPDATE SET
      ts = EXCLUDED.ts,
      type = EXCLUDED.type,
      status = COALESCE(EXCLUDED.status, trades.status),
      mint = COALESCE(EXCLUDED.mint, trades.mint),
      symbol = COALESCE(EXCLUDED.symbol, trades.symbol),
      qty_token = COALESCE(EXCLUDED.qty_token, trades.qty_token),
      sol_spent = COALESCE(EXCLUDED.sol_spent, trades.sol_spent),
      sol_received = COALESCE(EXCLUDED.sol_received, trades.sol_received),
      usd_value = COALESCE(EXCLUDED.usd_value, trades.usd_value),
      entry_sol_per_token = COALESCE(EXCLUDED.entry_sol_per_token, trades.entry_sol_per_token),
      slippage_bps = COALESCE(EXCLUDED.slippage_bps, trades.slippage_bps),
      route = COALESCE(EXCLUDED.route, trades.route),
      cu_price = COALESCE(EXCLUDED.cu_price, trades.cu_price),
      jito_tip = COALESCE(EXCLUDED.jito_tip, trades.jito_tip),
      err = COALESCE(EXCLUDED.err, trades.err),
      meta = CASE
        WHEN trades.meta IS NULL THEN EXCLUDED.meta
        WHEN EXCLUDED.meta IS NULL THEN trades.meta
        ELSE (trades.meta || EXCLUDED.meta)
      END
    """


async def log_trade_event(evt: dict[str, Any] | None = None, /, **kwargs: Any) -> None:
    """
    Unified trade/event logger.

    Accepts:
      - log_trade_event({...})
      - log_trade_event(type="BUY", mint="...", ...)

    Writes to canonical `trades` table. The full payload is preserved in `meta`.
    """
    payload = dict(evt) if evt is not None else dict(kwargs)
    ts = _now_utc()

    ttype = str(payload.get("type") or "").strip() or "EVENT"
    status = payload.get("status")
    mint = payload.get("mint") or payload.get("token_address")
    symbol = payload.get("symbol")
    signature = payload.get("signature")

    qty_token = _f(payload.get("qty_token") or payload.get("qty") or payload.get("amount_token"))
    sol_spent = _f(payload.get("sol_spent"))
    sol_received = _f(payload.get("sol_received"))
    usd_value = _f(
        payload.get("usd_value") or payload.get("usd_spent") or payload.get("usd_received")
    )
    entry_sol_per_token = _f(payload.get("entry_sol_per_token"))
    slippage_bps = _i(payload.get("slippage_bps"))
    route = payload.get("route")
    cu_price = _i(payload.get("cu_price"))
    jito_tip = _i(payload.get("jito_tip"))
    err = payload.get("err") or payload.get("error")

    meta = _j(payload.get("meta")) if "meta" in payload else {}
    if not isinstance(meta, dict):
        meta = {"meta": meta}
    meta.setdefault("_payload", _j(payload))

    if signature:
        q = _trade_upsert_sql()
    else:
        q = """
        INSERT INTO trades
          (ts, type, status, mint, symbol, qty_token, sol_spent, sol_received, usd_value,
           entry_sol_per_token, slippage_bps, signature, route, cu_price, jito_tip, err, meta)
        VALUES
          ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17)
        """

    w = await get_writer()
    w.submit(
        q,
        ts,
        ttype,
        str(status) if status is not None else None,
        mint,
        symbol,
        qty_token,
        sol_spent,
        sol_received,
        usd_value,
        entry_sol_per_token,
        slippage_bps,
        str(signature) if signature is not None else None,
        str(route) if route is not None else None,
        cu_price,
        jito_tip,
        str(err) if err is not None else None,
        meta,
        tag="trade",
    )


# -----------------------------------------------------------------------------
# Back-compat / legacy helpers
# -----------------------------------------------------------------------------
async def log_candidate_async(c: dict[str, Any]) -> None:
    await log_scan_row(dict(c))


def log_candidate(c: dict[str, Any]) -> None:
    _schedule(log_candidate_async(c))


async def log_trade_async(mint, entry_px, exit_px, size_usd, pnl_usd, reason) -> None:
    payload = {
        "type": "LEGACY_TRADE",
        "mint": mint,
        "usd_value": _f(size_usd),
        "status": "DONE",
        "meta": {
            "entry_px": _f(entry_px),
            "exit_px": _f(exit_px),
            "pnl_usd": _f(pnl_usd),
            "reason": reason,
        },
    }
    await log_trade_event(payload)


def log_trade(*args: Any, **kwargs: Any) -> None:
    _schedule(log_trade_async(*args, **kwargs))


async def record_legacy_trade(
    mint: str,
    entry_px: float,
    exit_px: float,
    size_usd: float,
    pnl_usd: float,
    reason: str,
) -> None:
    await log_trade_event(
        {
            "type": "EXIT_LEGACY",
            "mint": mint,
            "usd_value": _f(size_usd),
            "status": "DONE",
            "meta": {
                "entry_px": _f(entry_px),
                "exit_px": _f(exit_px),
                "pnl_usd": _f(pnl_usd),
                "reason": reason,
            },
        }
    )


# -----------------------------------------------------------------------------
# Tiny console row (kept)
# -----------------------------------------------------------------------------
def tiny_row(**cols: Any) -> None:
    try:
        line = " | ".join(f"{k}={v}" for k, v in cols.items())
        print(f"[tiny] {line}")
    except Exception:
        pass


__all__ = [
    "close_pool",
    "get_writer",
    "healthcheck",
    "log_candidate",
    "log_candidate_async",
    "log_error_event",
    "log_pnl_snapshot",
    "log_scan_batch",
    "log_scan_row",
    "log_trade",
    "log_trade_async",
    "log_trade_event",
    "ping",
    "record_legacy_trade",
    "start_writer",
    "stop_writer",
    "tiny_row",
]
