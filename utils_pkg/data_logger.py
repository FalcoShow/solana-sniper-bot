# -*- coding: utf-8 -*-
from __future__ import annotations

"""
utils_pkg/data_logger.py

God-Tier V3+++ Data Logger (Phase 0–7 LOCK-IN)
=============================================

Canonical persistence + observability layer for:
  - entry_candidates (every candidate seen, bought or skipped)
  - entry_outcomes   (labels/outcomes + PnL truth)
  - trades, positions, exit_state
  - risk_events, health_heartbeats (regime / equity / risk manager / reinvestor)

Design goals
------------
- Never takes the bot down unless AI_LOG_STRICT=1
- Bounded async writer queue (prevents task storms)
- Schema self-healing (CREATE TABLE IF NOT EXISTS + additive ALTERs)
- DSN handling: prefer utils_pkg.ai_db; fallback to asyncpg pool
- Payload safety: redaction + size caps + feature key caps
- Sampling + src/regime allow-lists to control volume

Important usage contract
------------------------
- In async code: prefer the async APIs (a_log_*) to receive candidate_id immediately.
- Sync wrappers (log_*) are fire-and-forget when called from inside a running loop.

Notes
-----
- Import must be safe: no DB connections, no network I/O.
- DB failures trip a circuit breaker to avoid cascading outages.
"""

import asyncio
import contextlib
import inspect
import json
import os
import socket
import threading
import time
import uuid
from collections.abc import Awaitable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from logging_setup import get_logger

log = get_logger(__name__)

__all__ = [
    # async public
    "a_enqueue_paper_label",
    "a_log_entry_candidate",
    "a_log_entry_decision_update",
    "a_log_entry_outcome",
    "a_log_trade_execution",
    # lifecycle
    "ensure_schema",
    "close_logger",
    # sync public
    "enqueue_paper_label",
    "log_entry_candidate",
    "log_buy_candidate",
    "log_skip_candidate",
    "log_entry_decision_update",
    "log_entry_outcome",
    "log_trade_execution",
    "log_position_upsert",
    "log_exit_state_upsert",
    "log_ai_event",
    "log_regime_snapshot",
    "log_equity_curve_point",
    "log_risk_manager_snapshot",
    "log_reinvestor_snapshot",
]

# ──────────────────────────────────────────────────────────────────────────────
# Env helpers
# ──────────────────────────────────────────────────────────────────────────────
_TRUE = {"1", "true", "yes", "y", "on", "t"}
_FALSE = {"0", "false", "no", "n", "off", "f"}


def _envs(k: str, d: str = "") -> str:
    v = os.getenv(k)
    return (v if v is not None else d).strip()


def _envb(k: str, d: bool = False) -> bool:
    v = _envs(k, "")
    if v == "":
        return bool(d)
    s = v.lower()
    if s in _TRUE:
        return True
    if s in _FALSE:
        return False
    return bool(d)


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


# Master toggles
PG_ENABLED = _envb("PG_ENABLED", True)
AI_LOG_STRICT = _envb("AI_LOG_STRICT", False)

AI_LOG_CANDIDATES = _envb("AI_LOG_CANDIDATES", True)
AI_LOG_SKIPS = _envb("AI_LOG_SKIPS", True)
AI_LOG_OUTCOMES = _envb("AI_LOG_OUTCOMES", True)
AI_LOG_EXECUTIONS = _envb("AI_LOG_EXECUTIONS", True)
AI_LOG_EVENTS = _envb("AI_LOG_EVENTS", True)
AI_LOG_SNAPSHOTS = _envb("AI_LOG_SNAPSHOTS", True)

# Sampling
AI_LOG_SAMPLE_RATE = _envf("AI_LOG_SAMPLE_RATE", 1.0)
AI_LOG_SAMPLE_RATE_SKIPS = _envf("AI_LOG_SAMPLE_RATE_SKIPS", AI_LOG_SAMPLE_RATE)
AI_LOG_EXEC_SAMPLE_RATE = _envf("AI_LOG_EXEC_SAMPLE_RATE", 1.0)
AI_LOG_OUTCOME_SAMPLE_RATE = _envf("AI_LOG_OUTCOME_SAMPLE_RATE", 1.0)
AI_LOG_EVENT_SAMPLE_RATE = _envf("AI_LOG_EVENT_SAMPLE_RATE", 1.0)
AI_LOG_SNAPSHOT_SAMPLE_RATE = _envf("AI_LOG_SNAPSHOT_SAMPLE_RATE", 1.0)

# Filters
AI_LOG_SRC_FILTER = _envs("AI_LOG_SRC_FILTER", "")
AI_LOG_REGIME_FILTER = _envs("AI_LOG_REGIME_FILTER", "")

_SRC_ALLOW: set[str] | None = None
_REGIME_ALLOW: set[str] | None = None
if AI_LOG_SRC_FILTER:
    _SRC_ALLOW = {s.strip() for s in AI_LOG_SRC_FILTER.split(",") if s.strip()}
if AI_LOG_REGIME_FILTER:
    _REGIME_ALLOW = {s.strip() for s in AI_LOG_REGIME_FILTER.split(",") if s.strip()}

# Payload caps
AI_LOG_MAX_UI_BYTES = _envi("AI_LOG_MAX_UI_BYTES", 60_000)
AI_LOG_MAX_FEATURE_KEYS = _envi("AI_LOG_MAX_FEATURE_KEYS", 450)

# Circuit breaker
AI_LOG_DISABLE_AFTER_FAILS = _envi("AI_LOG_DISABLE_AFTER_FAILS", 25)

# Async writer queue
AI_LOG_QUEUE_ENABLED = _envb("AI_LOG_QUEUE_ENABLED", True)
AI_LOG_QUEUE_MAX = _envi("AI_LOG_QUEUE_MAX", 2500)
AI_LOG_QUEUE_DROP_NEW = _envb("AI_LOG_QUEUE_DROP_NEW", True)

# Schema ensure
AI_LOG_ENSURE_SCHEMA = _envb("AI_LOG_ENSURE_SCHEMA", True)
AI_LOG_SCHEMA_BACKFILL_TS = _envb("AI_LOG_SCHEMA_BACKFILL_TS", True)

# Paper-label enqueue hook
PAPER_LABEL_ENQUEUE_ON_SKIP = _envb("PAPER_LABEL_ENQUEUE_ON_SKIP", False)
PAPER_LABEL_INBOX_KEY = _envs("PAPER_LABEL_INBOX_KEY", "paper_label:inbox")

SERVICE_NAME = _envs("SERVICE_NAME", "sniper")
SERVICE_VERSION = _envs("SERVICE_VERSION", _envs("VERSION", "v1"))

# ──────────────────────────────────────────────────────────────────────────────
# Logger throttling helper (no monkey patching logger methods)
# ──────────────────────────────────────────────────────────────────────────────
_EVERY_LAST: dict[str, float] = {}
_EVERY_LOCK = threading.Lock()


def _log_every(key: str, n: int = 60) -> bool:
    """
    Best-effort throttling.
    If your logger supports `.every(key, n=...)` we use it.
    Otherwise we implement a small monotonic time-based throttle.
    """
    fn = getattr(log, "every", None)
    if callable(fn):
        try:
            return bool(fn(key, n=n))
        except TypeError:
            try:
                return bool(fn(key, n))
            except Exception:
                return True
        except Exception:
            return True

    now = time.monotonic()
    with _EVERY_LOCK:
        last = _EVERY_LAST.get(key, 0.0)
        if (now - last) >= float(max(1, int(n))):
            _EVERY_LAST[key] = now
            return True
        return False


# ──────────────────────────────────────────────────────────────────────────────
# Optional imports (lazy; must not crash import)
# ──────────────────────────────────────────────────────────────────────────────
_ai_db: Any | None = None
_ai_db_loaded = False

_fb_obj: Any | None = None
_fb_attempted = False
_fb_lock = threading.Lock()


def _get_ai_db() -> Any | None:
    """
    Lazy import of utils_pkg.ai_db.
    Must not perform I/O on import; ai_db should remain lazy too.
    """
    global _ai_db, _ai_db_loaded
    if _ai_db_loaded:
        return _ai_db
    _ai_db_loaded = True
    try:
        from utils_pkg import ai_db as mod  # type: ignore
    except Exception:
        _ai_db = None
    else:
        _ai_db = mod
    return _ai_db


def _get_feature_builder() -> Any | None:
    """
    Lazy FeatureBuilder instantiation (keeps import lightweight).

    NOTE:
    This is written with a single return at the end to avoid mypy
    incorrectly flagging a reachable return as unreachable when using
    globals + lock + early returns.
    """
    global _fb_obj, _fb_attempted

    # Fast path (no lock)
    if _fb_attempted:
        return _fb_obj

    with _fb_lock:
        # Re-check under lock
        if not _fb_attempted:
            _fb_attempted = True

            try:
                from utils_pkg import feature_builder as mod  # type: ignore
            except Exception:
                _fb_obj = None
            else:
                try:
                    _fb_obj = mod.FeatureBuilder()
                except Exception:
                    if _log_every("data_logger_feature_builder_init_fail", 60):
                        log.exception(
                            "data_logger: FeatureBuilder init failed; continuing without it"
                        )
                    _fb_obj = None

    return _fb_obj


async def _get_redis_async() -> Any | None:
    """
    Lazy Redis accessor (keeps import safe).
    """
    try:
        from utils_pkg import core_utils as cu  # type: ignore
    except Exception:
        return None
    fn = getattr(cu, "get_redis_async", None)
    if not callable(fn):
        return None
    try:
        return await fn()
    except Exception:
        return None


# ──────────────────────────────────────────────────────────────────────────────
# Small utilities
# ──────────────────────────────────────────────────────────────────────────────
def _ts() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def _stable_hash01(s: str) -> float:
    # deterministic, stable, fast (no PYTHONHASHSEED dependence)
    h = 0
    for ch in s:
        h = (h * 31 + ord(ch)) & 0xFFFFFFFF
    return (h % 10_000) / 10_000.0


def _should_sample(key: str, rate: float) -> bool:
    if rate >= 0.999:
        return True
    if rate <= 0.0:
        return False
    return _stable_hash01(key) < float(rate)


def _src_regime_allowed(src: str, regime: str | None) -> bool:
    if _SRC_ALLOW is not None and src not in _SRC_ALLOW:
        return False
    if _REGIME_ALLOW is not None and regime is not None and regime not in _REGIME_ALLOW:
        return False
    return True


def _base_ctx() -> dict[str, Any]:
    return {
        "service": SERVICE_NAME,
        "version": SERVICE_VERSION,
        "host": socket.gethostname(),
        "pid": os.getpid(),
    }


def _normalize_01(raw: Any) -> float:
    try:
        v = float(raw)
    except Exception:
        return 0.0
    if v > 1.5:  # accept 0..100 style scores too
        v = v / 100.0
    return max(0.0, min(1.0, v))


# ──────────────────────────────────────────────────────────────────────────────
# DB backend (prefer ai_db; fallback asyncpg)
# ──────────────────────────────────────────────────────────────────────────────
_POOL: Any | None = None
_POOL_LOCK = asyncio.Lock()

_DB_FAILS = 0
_DB_DISABLED = False

_SCHEMA_READY = False
_SCHEMA_LOCK = asyncio.Lock()


def _db_disabled() -> bool:
    return (not PG_ENABLED) or _DB_DISABLED


def _bump_db_fail(e: Exception) -> None:
    global _DB_FAILS, _DB_DISABLED
    _DB_FAILS += 1
    if _log_every("data_logger_db_fail", 30):
        log.warning("data_logger: db error (fails=%s): %s", _DB_FAILS, repr(e))
    if max(1, int(AI_LOG_DISABLE_AFTER_FAILS)) <= _DB_FAILS:
        _DB_DISABLED = True
        log.error("data_logger: disabling DB logging after too many failures (%s)", _DB_FAILS)


def _call_with_accepted_kwargs(fn: Any, **kwargs: Any) -> Any:
    sig = inspect.signature(fn)
    params = sig.parameters
    if any(p.kind == p.VAR_KEYWORD for p in params.values()):
        return fn(**kwargs)
    filtered = {k: v for k, v in kwargs.items() if k in params}
    return fn(**filtered)


async def _ai_db_execute_raw(sql: str, *args: Any) -> Any:
    ai_db = _get_ai_db()
    if ai_db is None:
        raise RuntimeError("ai_db not available")
    fn = getattr(ai_db, "execute", None)
    if not callable(fn):
        raise RuntimeError("ai_db.execute not available")
    try:
        return await fn(sql, *args)
    except TypeError:
        return await _call_with_accepted_kwargs(fn, sql=sql, args=args)


async def _ai_db_fetchrow_raw(sql: str, *args: Any) -> Any:
    ai_db = _get_ai_db()
    if ai_db is None:
        raise RuntimeError("ai_db not available")
    fn = getattr(ai_db, "fetchrow", None)
    if not callable(fn):
        raise RuntimeError("ai_db.fetchrow not available")
    try:
        return await fn(sql, *args)
    except TypeError:
        return await _call_with_accepted_kwargs(fn, sql=sql, args=args)


def _candidate_dsns() -> list[tuple[str, str]]:
    keys = [
        "PNL_DSN",
        "POSTGRES_DSN",
        "PG_DSN",
        "DATABASE_URL",
        "PG_DSN_INTERNAL",
        "PG_DSN_EXTERNAL",
        "PG_DSN_PUBLIC",
    ]
    out: list[tuple[str, str]] = []
    for k in keys:
        v = _envs(k, "")
        if v:
            out.append((k, v))
    return out


def _normalize_asyncpg_dsn(raw: str) -> str:
    v = (raw or "").strip()
    if v.startswith("postgresql+asyncpg://"):
        v = "postgresql://" + v[len("postgresql+asyncpg://") :]
    if v.startswith("postgres://"):
        v = "postgresql://" + v[len("postgres://") :]
    # Render often requires SSL; safe to append (asyncpg accepts sslmode)
    if "sslmode=" not in v:
        v = v + ("&sslmode=require" if "?" in v else "?sslmode=require")
    return v


def _pick_best_dsn() -> tuple[str, str]:
    cands = _candidate_dsns()
    if not cands:
        return "", ""
    label, raw = cands[0]
    return label, _normalize_asyncpg_dsn(raw)


def _import_asyncpg() -> Any:
    try:
        import asyncpg  # type: ignore
    except Exception as e:  # pragma: no cover
        raise RuntimeError("asyncpg not installed (required for data_logger DB writes)") from e
    return asyncpg


async def _fallback_get_pool() -> Any:
    global _POOL

    # Fast path
    if _POOL is not None:
        return _POOL

    async with _POOL_LOCK:
        # Re-check under lock
        if _POOL is None:
            label, dsn = _pick_best_dsn()
            if not dsn:
                raise RuntimeError("No DSN found (set POSTGRES_DSN/PNL_DSN/PG_DSN/DATABASE_URL)")

            if _log_every("data_logger_pool_create", 60):
                log.debug("data_logger: creating asyncpg pool dsn_source=%s", label)

            asyncpg = _import_asyncpg()
            _POOL = await asyncpg.create_pool(dsn=dsn, min_size=1, max_size=3)

        # Help mypy: after the guarded init, _POOL must exist
        assert _POOL is not None
        return _POOL


@dataclass(frozen=True)
class _DB:
    """
    DB shim that prefers ai_db when available, else asyncpg pool.
    ai_db is accessed lazily to keep module import safe.
    """

    async def execute_raw(self, sql: str, *args: Any) -> Any:
        ai_db = _get_ai_db()
        if ai_db is not None:
            return await _ai_db_execute_raw(sql, *args)
        p = await _fallback_get_pool()
        async with p.acquire() as conn:
            return await conn.execute(sql, *args)

    async def fetchrow_raw(self, sql: str, *args: Any) -> Any:
        ai_db = _get_ai_db()
        if ai_db is not None:
            return await _ai_db_fetchrow_raw(sql, *args)
        p = await _fallback_get_pool()
        async with p.acquire() as conn:
            return await conn.fetchrow(sql, *args)

    async def execute(self, sql: str, *args: Any) -> Any:
        await _ensure_schema_once(self)
        return await self.execute_raw(sql, *args)

    async def fetchrow(self, sql: str, *args: Any) -> Any:
        await _ensure_schema_once(self)
        return await self.fetchrow_raw(sql, *args)


_DBX = _DB()

# ──────────────────────────────────────────────────────────────────────────────
# Schema (aligned to psql sanity checks: entry_candidates.ts, entry_outcomes.outcome_label)
# ──────────────────────────────────────────────────────────────────────────────
_SCHEMA_SQL: list[str] = [
    # entry_candidates
    """
    CREATE TABLE IF NOT EXISTS entry_candidates (
      id BIGSERIAL PRIMARY KEY,
      created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
      ts TIMESTAMPTZ,
      mint TEXT NOT NULL,
      symbol TEXT NOT NULL DEFAULT '',
      source TEXT NOT NULL,
      regime TEXT NOT NULL DEFAULT '',
      pool TEXT NOT NULL DEFAULT '',
      decision TEXT NOT NULL DEFAULT '',
      ai_ok BOOLEAN,
      ai_score DOUBLE PRECISION,
      outcome_label TEXT,
      outcome_pnl_pct DOUBLE PRECISION,
      outcome_ts TIMESTAMPTZ,
      ui JSONB
    );
    """,
    "ALTER TABLE entry_candidates ADD COLUMN IF NOT EXISTS ts TIMESTAMPTZ;",
    "ALTER TABLE entry_candidates ADD COLUMN IF NOT EXISTS outcome_label TEXT;",
    "ALTER TABLE entry_candidates ADD COLUMN IF NOT EXISTS outcome_pnl_pct DOUBLE PRECISION;",
    "ALTER TABLE entry_candidates ADD COLUMN IF NOT EXISTS outcome_ts TIMESTAMPTZ;",
    "CREATE INDEX IF NOT EXISTS idx_entry_candidates_ts ON entry_candidates (COALESCE(ts, created_at) DESC);",
    "CREATE INDEX IF NOT EXISTS idx_entry_candidates_mint_ts ON entry_candidates (mint, COALESCE(ts, created_at) DESC);",
    "CREATE INDEX IF NOT EXISTS idx_entry_candidates_decision_ts ON entry_candidates (decision, COALESCE(ts, created_at) DESC);",
    # entry_outcomes
    """
    CREATE TABLE IF NOT EXISTS entry_outcomes (
      id BIGSERIAL PRIMARY KEY,
      created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
      ts TIMESTAMPTZ,
      candidate_id BIGINT,
      mint TEXT NOT NULL DEFAULT '',
      outcome_label TEXT NOT NULL DEFAULT '',
      pnl_pct DOUBLE PRECISION,
      hold_sec DOUBLE PRECISION,
      got_stopped BOOLEAN,
      notes TEXT NOT NULL DEFAULT '',
      extra JSONB
    );
    """,
    "ALTER TABLE entry_outcomes ADD COLUMN IF NOT EXISTS ts TIMESTAMPTZ;",
    "ALTER TABLE entry_outcomes ADD COLUMN IF NOT EXISTS outcome_label TEXT;",
    "CREATE UNIQUE INDEX IF NOT EXISTS ux_entry_outcomes_candidate_id ON entry_outcomes (candidate_id) WHERE candidate_id IS NOT NULL;",
    "CREATE INDEX IF NOT EXISTS idx_entry_outcomes_label_ts ON entry_outcomes (outcome_label, COALESCE(ts, created_at) DESC);",
    # trades
    """
    CREATE TABLE IF NOT EXISTS trades (
      id BIGSERIAL PRIMARY KEY,
      created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
      mint TEXT NOT NULL,
      side TEXT NOT NULL,
      sig TEXT NOT NULL DEFAULT '',
      venue TEXT NOT NULL DEFAULT '',
      status TEXT NOT NULL DEFAULT 'unknown',
      amount_sol DOUBLE PRECISION,
      amount_raw BIGINT,
      out_amount_sol DOUBLE PRECISION,
      out_amount_raw BIGINT,
      price_sol DOUBLE PRECISION,
      fee_sol DOUBLE PRECISION,
      reason TEXT NOT NULL DEFAULT '',
      meta JSONB
    );
    """,
    "CREATE INDEX IF NOT EXISTS idx_trades_mint_created ON trades (mint, created_at DESC);",
    "CREATE UNIQUE INDEX IF NOT EXISTS ux_trades_sig_nonempty ON trades(sig) WHERE sig <> '';",
    # positions
    """
    CREATE TABLE IF NOT EXISTS positions (
      mint TEXT PRIMARY KEY,
      status TEXT NOT NULL DEFAULT 'open',
      qty_raw BIGINT,
      avg_price_sol DOUBLE PRECISION,
      cost_sol DOUBLE PRECISION,
      realized_pnl_sol DOUBLE PRECISION,
      last_price_sol DOUBLE PRECISION,
      meta JSONB,
      updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
    );
    """,
    # exit_state
    """
    CREATE TABLE IF NOT EXISTS exit_state (
      mint TEXT PRIMARY KEY,
      state JSONB,
      updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
    );
    """,
    # risk_events
    """
    CREATE TABLE IF NOT EXISTS risk_events (
      id BIGSERIAL PRIMARY KEY,
      created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
      level TEXT NOT NULL,
      code TEXT NOT NULL,
      msg TEXT NOT NULL DEFAULT '',
      meta JSONB
    );
    """,
    "CREATE INDEX IF NOT EXISTS idx_risk_events_code_created ON risk_events (code, created_at DESC);",
    # heartbeats / snapshots
    """
    CREATE TABLE IF NOT EXISTS health_heartbeats (
      id BIGSERIAL PRIMARY KEY,
      created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
      component TEXT NOT NULL,
      payload JSONB
    );
    """,
    "CREATE INDEX IF NOT EXISTS idx_health_heartbeats_component_created ON health_heartbeats (component, created_at DESC);",
]


async def _ensure_schema_once(db: _DB) -> None:
    global _SCHEMA_READY
    if _SCHEMA_READY or (not AI_LOG_ENSURE_SCHEMA) or _db_disabled():
        return

    async with _SCHEMA_LOCK:
        if _SCHEMA_READY or (not AI_LOG_ENSURE_SCHEMA) or _db_disabled():
            return
        try:
            for stmt in _SCHEMA_SQL:
                try:
                    await db.execute_raw(stmt)
                except Exception:
                    # tolerate older schemas / permissions / transient errors
                    pass

            if AI_LOG_SCHEMA_BACKFILL_TS:
                try:
                    await db.execute_raw(
                        "UPDATE entry_candidates SET ts = COALESCE(ts, created_at) WHERE ts IS NULL;"
                    )
                    await db.execute_raw(
                        "UPDATE entry_outcomes SET ts = COALESCE(ts, created_at) WHERE ts IS NULL;"
                    )
                except Exception:
                    pass

            _SCHEMA_READY = True
        except Exception as e:
            _bump_db_fail(e)
            if _log_every("data_logger_schema_fail", 60):
                log.warning("data_logger: schema ensure failed (continuing): %s", repr(e))


async def ensure_schema() -> None:
    await _ensure_schema_once(_DBX)


# ──────────────────────────────────────────────────────────────────────────────
# Writer queue (bounded)
# ──────────────────────────────────────────────────────────────────────────────
_WRITER_Q: asyncio.Queue[Awaitable[Any] | None] | None = None
_WRITER_TASK: asyncio.Task[None] | None = None
_WRITER_LOCK = asyncio.Lock()
_WRITER_DROPPED = 0
_WRITER_CLOSED = False


async def _writer_worker() -> None:
    assert _WRITER_Q is not None
    while True:
        item = await _WRITER_Q.get()
        try:
            if item is None:
                return
            await item
        except asyncio.CancelledError:
            raise
        except Exception as e:
            _bump_db_fail(e)
            if AI_LOG_STRICT and _log_every("data_logger_strict_write_fail", 10):
                log.exception("data_logger: strict async write failed")
        finally:
            _WRITER_Q.task_done()


async def _ensure_writer() -> None:
    global _WRITER_Q, _WRITER_TASK
    if not AI_LOG_QUEUE_ENABLED:
        return
    if _WRITER_Q is not None and _WRITER_TASK is not None and not _WRITER_TASK.done():
        return
    async with _WRITER_LOCK:
        if _WRITER_Q is None:
            _WRITER_Q = asyncio.Queue(maxsize=max(1, int(AI_LOG_QUEUE_MAX)))
        if _WRITER_TASK is None or _WRITER_TASK.done():
            _WRITER_TASK = asyncio.get_running_loop().create_task(_writer_worker())


def _close_awaitable_safely(a: Awaitable[Any]) -> None:
    try:
        close = getattr(a, "close", None)
        if callable(close):
            close()
    except Exception:
        pass


def _submit(coro: Awaitable[Any]) -> None:
    """
    Fire-and-forget submission with:
      - circuit breaker
      - optional bounded queue
      - safe fallback when no loop is running

    Accepts Awaitable[Any] so callers may submit coroutines returning ints (we ignore results).
    """

    async def _runner() -> None:
        if _db_disabled():
            return
        try:
            await coro
        except Exception as e:
            _bump_db_fail(e)
            if AI_LOG_STRICT:
                raise

    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        # no running loop: execute synchronously
        try:
            asyncio.run(_runner())
        except Exception as e:
            _bump_db_fail(e)
            if AI_LOG_STRICT:
                raise
        return

    if AI_LOG_QUEUE_ENABLED:

        async def _enqueue() -> None:
            global _WRITER_DROPPED
            if _WRITER_CLOSED:
                return
            await _ensure_writer()
            assert _WRITER_Q is not None
            a = _runner()
            try:
                _WRITER_Q.put_nowait(a)
            except asyncio.QueueFull:
                _WRITER_DROPPED += 1
                if not AI_LOG_QUEUE_DROP_NEW:
                    # drop oldest to accept newest
                    try:
                        old = _WRITER_Q.get_nowait()
                        if old is not None:
                            _close_awaitable_safely(old)
                        _WRITER_Q.task_done()
                        _WRITER_Q.put_nowait(a)
                    except Exception:
                        _close_awaitable_safely(a)
                else:
                    _close_awaitable_safely(a)

                if _log_every("data_logger_queue_full", 30):
                    log.warning(
                        "data_logger: writer queue full (dropped=%s max=%s drop_new=%s)",
                        _WRITER_DROPPED,
                        AI_LOG_QUEUE_MAX,
                        AI_LOG_QUEUE_DROP_NEW,
                    )

        loop.create_task(_enqueue())
    else:
        loop.create_task(_runner())


async def close_logger() -> None:
    """
    Graceful shutdown:
      - stop accepting new items
      - flush queue best-effort
      - close pool
    """
    global _POOL, _WRITER_CLOSED, _WRITER_TASK, _WRITER_Q

    _WRITER_CLOSED = True
    q = _WRITER_Q
    t = _WRITER_TASK

    if q is not None:
        with contextlib.suppress(Exception):
            q.put_nowait(None)

    if t is not None:
        with contextlib.suppress(Exception):
            await asyncio.wait_for(t, timeout=2.0)

    # best-effort drain
    if q is not None:
        try:
            while not q.empty():
                item = q.get_nowait()
                if item is not None:
                    _close_awaitable_safely(item)
                q.task_done()
        except Exception:
            pass

    _WRITER_TASK = None
    _WRITER_Q = None

    p = _POOL
    _POOL = None
    if p is not None:
        with contextlib.suppress(Exception):
            await p.close()


# ──────────────────────────────────────────────────────────────────────────────
# Payload safety: redaction + caps
# ──────────────────────────────────────────────────────────────────────────────
_SENSITIVE_TOKENS = {
    "WALLET_PRIVATE_KEY",
    "WALLET_JSON",
    "PRIVATE_KEY",
    "SECRET",
    "SEED",
    "MNEMONIC",
    "KEYPAIR",
    "API_KEY",
    "JUP_API_KEY",
    "OPENAI_API_KEY",
    "DISCORD_TOKEN",
    "TELEGRAM_BOT_TOKEN",
    "NOTION_TOKEN",
}


def _redact_obj(o: Any) -> Any:
    if isinstance(o, dict):
        out: dict[str, Any] = {}
        for k, v in o.items():
            ks = str(k)
            if any(tok in ks.upper() for tok in _SENSITIVE_TOKENS):
                out[ks] = "***REDACTED***"
            else:
                out[ks] = _redact_obj(v)
        return out
    if isinstance(o, (list, tuple)):
        return [_redact_obj(x) for x in o[:500]]
    return o


def _json_bytes(o: Any) -> int:
    try:
        return len(json.dumps(o, ensure_ascii=False, separators=(",", ":")).encode("utf-8"))
    except Exception:
        return 10**9


def _cap_dict_keys(d: dict[str, Any], max_keys: int) -> dict[str, Any]:
    if len(d) <= max_keys:
        return d
    keys = sorted(d.keys())
    keep = keys[:max_keys]
    return {k: d[k] for k in keep}


def _cap_ui(ui: dict[str, Any]) -> dict[str, Any]:
    ui2 = _redact_obj(ui)
    if not isinstance(ui2, dict):
        return {}

    feats = ui2.get("features")
    if isinstance(feats, dict):
        ui2["features"] = _cap_dict_keys(feats, AI_LOG_MAX_FEATURE_KEYS)

    if _json_bytes(ui2) <= AI_LOG_MAX_UI_BYTES:
        return ui2

    # progressively drop heavy sections
    for heavy in (
        "history",
        "raw",
        "wallet_signals",
        "ai_signals",
        "position_context",
        "gate_detail",
        "extra_raw",
        "debug",
    ):
        if heavy in ui2:
            ui2.pop(heavy, None)
            if _json_bytes(ui2) <= AI_LOG_MAX_UI_BYTES:
                return ui2

    # final minimal shape
    return {
        "ts": ui2.get("ts"),
        "event_uuid": ui2.get("event_uuid"),
        "src": ui2.get("src"),
        "mint": ui2.get("mint"),
        "symbol": ui2.get("symbol"),
        "regime": ui2.get("regime"),
        "decision": ui2.get("decision"),
        "score": ui2.get("score"),
        "reason": ui2.get("reason"),
    }


def _safe_label_from_pnl(pnl_pct: float, got_stopped: bool) -> str:
    try:
        p = float(pnl_pct)
    except Exception:
        p = 0.0
    if got_stopped:
        return "stopped_loss" if p <= 0 else "stopped_win"
    if p >= 40:
        return "mega_win"
    if p >= 20:
        return "monster_win"
    if p >= 10:
        return "big_win"
    if p >= 0:
        return "win"
    if p <= -35:
        return "disaster"
    if p <= -20:
        return "big_loss"
    return "loss"


# ──────────────────────────────────────────────────────────────────────────────
# FeatureBuilder hook
# ──────────────────────────────────────────────────────────────────────────────
def _build_features_safe(
    *,
    mint: str,
    symbol: str | None,
    snapshot: Mapping[str, Any],
    history: Sequence[Mapping[str, Any]] | None,
    regime: str | None,
    regime_state: Any | None,
    wallet_signals: Mapping[str, Any] | None,
    ai_signals: Mapping[str, Any] | None,
    scanner_source: str | None,
    position_context: Mapping[str, Any] | None,
) -> dict[str, Any]:
    feats: dict[str, Any] = {}

    def f(key: str, default: float = 0.0) -> float:
        try:
            return float(snapshot.get(key, default) or default)
        except Exception:
            return float(default)

    # minimal canonical fields (fast analytics)
    feats["liq_usd"] = f("liq_usd", f("liq", 0.0))
    feats["vol_5m_usd"] = f("vol_5m_usd", f("vol5", 0.0))
    feats["buys_5m"] = f("buys_5m", f("buys5", 0.0))
    feats["sells_5m"] = f("sells_5m", f("sells5", 0.0))
    feats["chg5_pct"] = f("price_change_5m_pct", f("chg5", 0.0))
    feats["age_min"] = f("token_age_min", f("age_min", 0.0))
    feats["price_usd"] = f("price_usd", 0.0)
    feats["ai_fraud_score"] = _normalize_01(
        snapshot.get("ai_rug_score", snapshot.get("fraud_score", 0.0))
    )

    fb = _get_feature_builder()
    if fb is None:
        feats["features_mode"] = "minimal"
        return feats

    try:
        built = _call_with_accepted_kwargs(
            fb.build,
            symbol=symbol,
            mint=mint,
            snapshot=dict(snapshot),
            history=list(history) if history else None,
            regime=regime_state,
            wallet_signals=dict(wallet_signals) if wallet_signals else None,
            ai_signals=dict(ai_signals) if ai_signals else None,
            scanner_source=scanner_source or "",
            risk_context=dict(position_context) if position_context else None,
            regime_label=regime,
        )
        if isinstance(built, dict):
            feats.update(built)
            feats["features_mode"] = "feature_builder"
        else:
            feats["features_mode"] = "minimal_fb_non_dict"
    except Exception:
        if _log_every("feature_builder_fail", 60):
            log.debug("data_logger: FeatureBuilder.build failed (using minimal)", exc_info=True)
        feats["features_mode"] = "minimal_fb_failed"

    return feats


# ──────────────────────────────────────────────────────────────────────────────
# Redis enqueue (paper label)
# ──────────────────────────────────────────────────────────────────────────────
async def a_enqueue_paper_label(candidate_id: int) -> int:
    if candidate_id <= 0:
        return 0
    r = await _get_redis_async()
    if r is None:
        return 0
    try:
        await r.lpush(PAPER_LABEL_INBOX_KEY, int(candidate_id))
        return 1
    except Exception:
        if _log_every("paper_label_enqueue_fail", 30):
            log.debug("data_logger: failed to enqueue paper label", exc_info=True)
        return 0


def enqueue_paper_label(candidate_id: int) -> int:
    if candidate_id <= 0:
        return 0
    _submit(a_enqueue_paper_label(int(candidate_id)))
    return 1


# ──────────────────────────────────────────────────────────────────────────────
# SQL helpers
# ──────────────────────────────────────────────────────────────────────────────
class _Now:
    pass


_NOW = _Now()


async def _a_insert_entry_candidate(
    *,
    mint: str,
    symbol: str,
    source: str,
    regime: str,
    pool: str,
    decision: str,
    ai_ok: bool | None,
    ai_score: float | None,
    ui: dict[str, Any],
) -> int:
    if _db_disabled():
        return 0
    ui_s = json.dumps(_cap_ui(ui), ensure_ascii=False)

    row = await _DBX.fetchrow(
        """
        INSERT INTO entry_candidates (ts, mint, symbol, source, regime, pool, decision, ai_ok, ai_score, ui)
        VALUES (now(), $1,$2,$3,$4,$5,$6,$7,$8,$9::jsonb)
        RETURNING id
        """,
        mint,
        symbol or "",
        source,
        regime or "",
        pool or "",
        decision or "",
        ai_ok,
        ai_score,
        ui_s,
    )
    try:
        return int(row["id"]) if row is not None and "id" in row else 0
    except Exception:
        return 0


async def _a_patch_candidate(
    candidate_id: int, patch_cols: dict[str, Any], ui_patch: dict[str, Any] | None = None
) -> int:
    if _db_disabled() or candidate_id <= 0:
        return 0

    sets: list[str] = []
    args: list[Any] = []
    i = 1

    for k, v in patch_cols.items():
        if v is _NOW:
            sets.append(f"{k} = now()")
        else:
            sets.append(f"{k} = ${i}")
            args.append(v)
            i += 1

    if ui_patch is not None:
        ui_s = json.dumps(_cap_ui(ui_patch), ensure_ascii=False)
        sets.append(f"ui = COALESCE(ui, '{{}}'::jsonb) || ${i}::jsonb")
        args.append(ui_s)
        i += 1

    if not sets:
        return int(candidate_id)

    args.append(int(candidate_id))
    where_idx = i
    await _DBX.execute(
        f"UPDATE entry_candidates SET {', '.join(sets)} WHERE id = ${where_idx}", *args
    )
    return int(candidate_id)


async def _a_insert_or_upsert_outcome(
    *,
    candidate_id: int,
    mint: str,
    outcome_label: str,
    pnl_pct: float,
    hold_sec: float,
    got_stopped: bool,
    notes: str,
    extra: dict[str, Any],
) -> int:
    if _db_disabled():
        return 0
    extra_s = json.dumps(_cap_ui(extra), ensure_ascii=False)
    await _DBX.execute(
        """
        INSERT INTO entry_outcomes (ts, candidate_id, mint, outcome_label, pnl_pct, hold_sec, got_stopped, notes, extra)
        VALUES (now(), $1,$2,$3,$4,$5,$6,$7,$8::jsonb)
        ON CONFLICT (candidate_id) DO UPDATE SET
          ts = now(),
          mint = EXCLUDED.mint,
          outcome_label = EXCLUDED.outcome_label,
          pnl_pct = EXCLUDED.pnl_pct,
          hold_sec = EXCLUDED.hold_sec,
          got_stopped = EXCLUDED.got_stopped,
          notes = EXCLUDED.notes,
          extra = COALESCE(entry_outcomes.extra, '{}'::jsonb) || EXCLUDED.extra
        """,
        int(candidate_id),
        mint or "",
        outcome_label or "",
        float(pnl_pct),
        float(hold_sec),
        bool(got_stopped),
        notes or "",
        extra_s,
    )
    return int(candidate_id)


async def _a_insert_trade(**kwargs: Any) -> int:
    if _db_disabled():
        return 0
    meta_s = json.dumps(_cap_ui(dict(kwargs.get("meta") or {})), ensure_ascii=False)
    row = await _DBX.fetchrow(
        """
        INSERT INTO trades
          (mint, side, sig, venue, status, amount_sol, amount_raw, out_amount_sol, out_amount_raw,
           price_sol, fee_sol, reason, meta)
        VALUES
          ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13::jsonb)
        ON CONFLICT DO NOTHING
        RETURNING id
        """,
        kwargs["mint"],
        kwargs["side"],
        kwargs.get("sig", ""),
        kwargs.get("venue", ""),
        kwargs.get("status", "unknown"),
        kwargs.get("amount_sol"),
        kwargs.get("amount_raw"),
        kwargs.get("out_amount_sol"),
        kwargs.get("out_amount_raw"),
        kwargs.get("price_sol"),
        kwargs.get("fee_sol"),
        kwargs.get("reason", ""),
        meta_s,
    )
    try:
        return int(row["id"]) if row is not None and "id" in row else 0
    except Exception:
        return 0


async def _a_upsert_position(**kwargs: Any) -> int:
    if _db_disabled():
        return 0
    meta_s = json.dumps(_cap_ui(dict(kwargs.get("meta") or {})), ensure_ascii=False)
    await _DBX.execute(
        """
        INSERT INTO positions
          (mint, status, qty_raw, avg_price_sol, cost_sol, realized_pnl_sol, last_price_sol, meta, updated_at)
        VALUES
          ($1,$2,$3,$4,$5,$6,$7,$8::jsonb, now())
        ON CONFLICT (mint) DO UPDATE SET
          status = EXCLUDED.status,
          qty_raw = COALESCE(EXCLUDED.qty_raw, positions.qty_raw),
          avg_price_sol = COALESCE(EXCLUDED.avg_price_sol, positions.avg_price_sol),
          cost_sol = COALESCE(EXCLUDED.cost_sol, positions.cost_sol),
          realized_pnl_sol = COALESCE(EXCLUDED.realized_pnl_sol, positions.realized_pnl_sol),
          last_price_sol = COALESCE(EXCLUDED.last_price_sol, positions.last_price_sol),
          meta = COALESCE(positions.meta, '{}'::jsonb) || EXCLUDED.meta,
          updated_at = now()
        """,
        kwargs["mint"],
        kwargs.get("status", "open"),
        kwargs.get("qty_raw"),
        kwargs.get("avg_price_sol"),
        kwargs.get("cost_sol"),
        kwargs.get("realized_pnl_sol"),
        kwargs.get("last_price_sol"),
        meta_s,
    )
    return 1


async def _a_upsert_exit_state(mint: str, state: dict[str, Any]) -> int:
    if _db_disabled():
        return 0
    state_s = json.dumps(_cap_ui(state), ensure_ascii=False)
    await _DBX.execute(
        """
        INSERT INTO exit_state (mint, state, updated_at)
        VALUES ($1, $2::jsonb, now())
        ON CONFLICT (mint) DO UPDATE SET
          state = COALESCE(exit_state.state, '{}'::jsonb) || EXCLUDED.state,
          updated_at = now()
        """,
        mint,
        state_s,
    )
    return 1


async def _a_insert_risk_event(level: str, code: str, msg: str, meta: dict[str, Any]) -> int:
    if _db_disabled():
        return 0
    meta_s = json.dumps(_cap_ui(meta), ensure_ascii=False)
    await _DBX.execute(
        "INSERT INTO risk_events (level, code, msg, meta) VALUES ($1,$2,$3,$4::jsonb)",
        level,
        code,
        msg,
        meta_s,
    )
    return 1


async def _a_insert_heartbeat(component: str, payload: dict[str, Any]) -> int:
    if _db_disabled():
        return 0
    payload_s = json.dumps(_cap_ui(payload), ensure_ascii=False)
    await _DBX.execute(
        "INSERT INTO health_heartbeats (component, payload) VALUES ($1,$2::jsonb)",
        component,
        payload_s,
    )
    return 1


# ──────────────────────────────────────────────────────────────────────────────
# Public API: candidates
# ──────────────────────────────────────────────────────────────────────────────
async def a_log_entry_candidate(
    *,
    src: str,
    mint: str,
    symbol: str | None,
    snapshot: Mapping[str, Any],
    regime: str | None = None,
    wallet_tag: str | None = None,
    regime_state: Any | None = None,
    wallet_signals: Mapping[str, Any] | None = None,
    ai_signals: Mapping[str, Any] | None = None,
    scanner_source: str | None = None,
    history: Sequence[Mapping[str, Any]] | None = None,
    position_context: Mapping[str, Any] | None = None,
    decision: str | None = None,
    decision_score: float | None = None,
    decision_reason: str | None = None,
    episode_id: str | None = None,
    step_idx: int | None = None,
    strategy_profile: str | None = None,
    trade_id: str | None = None,
    session_id: str | None = None,
    candidate_key: str | None = None,
    gate_detail: Mapping[str, Any] | None = None,
    pool: str = "",
    ts: Any | None = None,
    extra_raw: Mapping[str, Any] | None = None,
    event_uuid: str | None = None,
) -> int:
    if _db_disabled() or (not AI_LOG_CANDIDATES):
        return 0
    if not _src_regime_allowed(src, regime):
        return 0

    ck = candidate_key or f"{src}:{regime or ''}:{mint}"
    d_upper = (decision or "").upper()

    if d_upper == "SKIP":
        if not AI_LOG_SKIPS:
            return 0
        if not _should_sample(ck, AI_LOG_SAMPLE_RATE_SKIPS):
            return 0
    else:
        if not _should_sample(ck, AI_LOG_SAMPLE_RATE):
            return 0

    feats = _build_features_safe(
        mint=mint,
        symbol=symbol,
        snapshot=snapshot,
        history=history,
        regime=regime,
        regime_state=regime_state,
        wallet_signals=wallet_signals,
        ai_signals=ai_signals,
        scanner_source=scanner_source or src,
        position_context=position_context,
    )

    score: float | None = float(decision_score) if decision_score is not None else None
    ai_ok: bool | None = None
    if decision is not None:
        ai_ok = d_upper != "SKIP"

    e_uuid = event_uuid or str(uuid.uuid4())
    ui: dict[str, Any] = {
        "ts": ts if ts is not None else _ts(),
        "event_uuid": e_uuid,
        "src": src,
        "mint": mint,
        "symbol": symbol,
        "regime": regime,
        "wallet_tag": wallet_tag,
        "decision": decision,
        "score": score,
        "reason": decision_reason,
        "trade_id": trade_id,
        "session_id": session_id,
        "candidate_key": candidate_key,
        "episode_id": episode_id,
        "step_idx": step_idx,
        "strategy_profile": strategy_profile,
        "features": feats,
        "raw": dict(snapshot),
        "ctx": _base_ctx(),
    }
    if extra_raw:
        ui["extra_raw"] = dict(extra_raw)
    if wallet_signals:
        ui["wallet_signals"] = dict(wallet_signals)
    if ai_signals:
        ui["ai_signals"] = dict(ai_signals)
    if position_context:
        ui["position_context"] = dict(position_context)
    if gate_detail:
        ui["gate_detail"] = dict(gate_detail)

    try:
        cid = await _a_insert_entry_candidate(
            mint=mint,
            symbol=symbol or "",
            source=src,
            regime=regime or "",
            pool=pool or "",
            decision=d_upper,
            ai_ok=ai_ok,
            ai_score=score,
            ui=ui,
        )

        if PAPER_LABEL_ENQUEUE_ON_SKIP and d_upper == "SKIP" and cid > 0:
            await a_enqueue_paper_label(cid)

        return cid
    except Exception as e:
        _bump_db_fail(e)
        if AI_LOG_STRICT:
            raise
        return 0


def log_entry_candidate(**kwargs: Any) -> int:
    """
    Sync wrapper.
    - If no loop is running: performs the write and returns candidate_id.
    - If a loop is running: schedules the write and returns 0 (use a_log_* in async paths).
    """
    if _db_disabled() or (not AI_LOG_CANDIDATES):
        return 0

    async def _write() -> int:
        return await a_log_entry_candidate(**kwargs)

    try:
        asyncio.get_running_loop()
        _submit(_write())
        return 0
    except RuntimeError:
        try:
            return asyncio.run(_write())
        except Exception:
            if AI_LOG_STRICT:
                raise
            return 0


def log_buy_candidate(**kwargs: Any) -> int:
    kwargs.setdefault("decision", "BUY")
    return log_entry_candidate(**kwargs)


def log_skip_candidate(**kwargs: Any) -> int:
    kwargs.setdefault("decision", "SKIP")
    return log_entry_candidate(**kwargs)


async def a_log_entry_decision_update(
    *,
    candidate_id: int,
    decision: str | None = None,
    decision_score: float | None = None,
    decision_reason: str | None = None,
    extra: Mapping[str, Any] | None = None,
) -> int:
    if _db_disabled() or candidate_id <= 0:
        return 0
    d = (decision or "").upper()

    patch_cols: dict[str, Any] = {}
    if decision is not None:
        patch_cols["decision"] = d
        patch_cols["ai_ok"] = d != "SKIP"
    if decision_score is not None:
        patch_cols["ai_score"] = float(decision_score)

    ui_patch: dict[str, Any] = {
        "decision": decision,
        "score": float(decision_score) if decision_score is not None else None,
        "reason": decision_reason,
        "decision_updated_at": _ts(),
    }
    if extra:
        ui_patch["decision_extra"] = dict(extra)

    try:
        return await _a_patch_candidate(candidate_id, patch_cols, ui_patch)
    except Exception as e:
        _bump_db_fail(e)
        if AI_LOG_STRICT:
            raise
        return int(candidate_id)


def log_entry_decision_update(
    *,
    candidate_id: int,
    decision: str | None = None,
    decision_score: float | None = None,
    decision_reason: str | None = None,
    extra: Mapping[str, Any] | None = None,
) -> int:
    _submit(
        a_log_entry_decision_update(
            candidate_id=candidate_id,
            decision=decision,
            decision_score=decision_score,
            decision_reason=decision_reason,
            extra=extra,
        )
    )
    return int(candidate_id)


async def a_log_entry_outcome(
    *,
    candidate_id: int,
    pnl_pct: float,
    hold_seconds: float,
    got_stopped: bool,
    label: str | None = None,
    notes: str | None = None,
    mint: str | None = None,
    extra: Mapping[str, Any] | None = None,
) -> int:
    if _db_disabled() or (not AI_LOG_OUTCOMES) or candidate_id <= 0:
        return 0
    if not _should_sample(f"outcome:{candidate_id}", AI_LOG_OUTCOME_SAMPLE_RATE):
        return 0

    outcome_label = label or _safe_label_from_pnl(pnl_pct, got_stopped)
    mint_final = mint or (str((extra or {}).get("mint", "")) if isinstance(extra, dict) else "")

    outcome = {
        "label": outcome_label,
        "pnl_pct": float(pnl_pct),
        "hold_sec": float(hold_seconds),
        "got_stopped": bool(got_stopped),
        "notes": notes,
        "ts": _ts(),
        "extra": dict(extra) if extra else {},
    }

    try:
        await _a_insert_or_upsert_outcome(
            candidate_id=int(candidate_id),
            mint=mint_final or "",
            outcome_label=outcome_label,
            pnl_pct=float(pnl_pct),
            hold_sec=float(hold_seconds),
            got_stopped=bool(got_stopped),
            notes=notes or "",
            extra={"outcome": outcome, "ctx": _base_ctx()},
        )

        await _a_patch_candidate(
            int(candidate_id),
            {
                "outcome_label": outcome_label,
                "outcome_pnl_pct": float(pnl_pct),
                "outcome_ts": _NOW,
            },
            {"outcome": outcome},
        )

        await _a_insert_risk_event(
            level="info",
            code="outcome",
            msg=f"entry_outcome label={outcome_label} pnl_pct={float(pnl_pct):.2f}",
            meta={"candidate_id": int(candidate_id), "outcome": outcome, "ctx": _base_ctx()},
        )
        return int(candidate_id)
    except Exception as e:
        _bump_db_fail(e)
        if AI_LOG_STRICT:
            raise
        return int(candidate_id)


def log_entry_outcome(
    *,
    candidate_id: int,
    pnl_pct: float,
    hold_seconds: float,
    got_stopped: bool,
    label: str | None = None,
    notes: str | None = None,
    mint: str | None = None,
    extra: Mapping[str, Any] | None = None,
) -> int:
    _submit(
        a_log_entry_outcome(
            candidate_id=candidate_id,
            pnl_pct=pnl_pct,
            hold_seconds=hold_seconds,
            got_stopped=got_stopped,
            label=label,
            notes=notes,
            mint=mint,
            extra=extra,
        )
    )
    return int(candidate_id)


# ──────────────────────────────────────────────────────────────────────────────
# Executions / positions / exit_state
# ──────────────────────────────────────────────────────────────────────────────
async def a_log_trade_execution(
    *,
    mint: str,
    side: str,
    sig: str = "",
    venue: str = "",
    status: str = "ok",
    amount_sol: float | None = None,
    amount_raw: int | None = None,
    out_amount_sol: float | None = None,
    out_amount_raw: int | None = None,
    price_sol: float | None = None,
    fee_sol: float | None = None,
    reason: str = "",
    meta: Mapping[str, Any] | None = None,
) -> int:
    if _db_disabled() or (not AI_LOG_EXECUTIONS):
        return 0
    if not _should_sample(f"exec:{mint}:{side}:{sig}", AI_LOG_EXEC_SAMPLE_RATE):
        return 0

    s = str(side).lower().strip()
    if s in ("b", "buy"):
        s = "buy"
    elif s in ("s", "sell"):
        s = "sell"
    else:
        s = s[:12] or "unknown"

    meta_d: dict[str, Any] = dict(meta) if meta else {}
    meta_d.setdefault("ctx", _base_ctx())

    try:
        return await _a_insert_trade(
            mint=mint,
            side=s,
            sig=sig or "",
            venue=venue or "",
            status=status or "unknown",
            amount_sol=float(amount_sol) if amount_sol is not None else None,
            amount_raw=int(amount_raw) if amount_raw is not None else None,
            out_amount_sol=float(out_amount_sol) if out_amount_sol is not None else None,
            out_amount_raw=int(out_amount_raw) if out_amount_raw is not None else None,
            price_sol=float(price_sol) if price_sol is not None else None,
            fee_sol=float(fee_sol) if fee_sol is not None else None,
            reason=reason or "",
            meta=meta_d,
        )
    except Exception as e:
        _bump_db_fail(e)
        if AI_LOG_STRICT:
            raise
        return 0


def log_trade_execution(**kwargs: Any) -> int:
    """
    Sync wrapper.
    - If no loop is running: performs the write and returns trade row id.
    - If a loop is running: schedules the write and returns 0 (use a_log_* in async paths).
    """

    async def _write() -> int:
        return await a_log_trade_execution(**kwargs)

    try:
        asyncio.get_running_loop()
        _submit(_write())
        return 0
    except RuntimeError:
        try:
            return asyncio.run(_write())
        except Exception:
            if AI_LOG_STRICT:
                raise
            return 0


def log_position_upsert(
    *,
    mint: str,
    status: str = "open",
    qty_raw: int | None = None,
    avg_price_sol: float | None = None,
    cost_sol: float | None = None,
    realized_pnl_sol: float | None = None,
    last_price_sol: float | None = None,
    meta: Mapping[str, Any] | None = None,
) -> int:
    meta_d = dict(meta) if meta else {}
    meta_d.setdefault("ctx", _base_ctx())

    async def _write() -> None:
        try:
            await _a_upsert_position(
                mint=mint,
                status=status or "open",
                qty_raw=int(qty_raw) if qty_raw is not None else None,
                avg_price_sol=float(avg_price_sol) if avg_price_sol is not None else None,
                cost_sol=float(cost_sol) if cost_sol is not None else None,
                realized_pnl_sol=float(realized_pnl_sol) if realized_pnl_sol is not None else None,
                last_price_sol=float(last_price_sol) if last_price_sol is not None else None,
                meta=meta_d,
            )
        except Exception as e:
            _bump_db_fail(e)
            if AI_LOG_STRICT:
                raise

    _submit(_write())
    return 1


def log_exit_state_upsert(*, mint: str, state: Mapping[str, Any]) -> int:
    async def _write() -> None:
        try:
            st = dict(state)
            st.setdefault("ctx", _base_ctx())
            await _a_upsert_exit_state(mint, st)
        except Exception as e:
            _bump_db_fail(e)
            if AI_LOG_STRICT:
                raise

    _submit(_write())
    return 1


# ──────────────────────────────────────────────────────────────────────────────
# Events / snapshots
# ──────────────────────────────────────────────────────────────────────────────
def log_ai_event(
    *,
    event_type: str,
    level: str = "info",
    msg: str = "",
    src: str | None = None,
    regime: str | None = None,
    payload: Mapping[str, Any] | None = None,
) -> int:
    if _db_disabled() or (not AI_LOG_EVENTS):
        return 0
    if not _should_sample(f"evt:{event_type}:{src or ''}:{regime or ''}", AI_LOG_EVENT_SAMPLE_RATE):
        return 0

    meta: dict[str, Any] = {
        "event_type": event_type,
        "src": src,
        "regime": regime,
        "ts": _ts(),
        "payload": dict(payload) if payload else {},
        "ctx": _base_ctx(),
    }
    _submit(_a_insert_risk_event(level=level or "info", code=event_type, msg=msg or "", meta=meta))
    return 1


def log_regime_snapshot(
    *,
    regime: str,
    state: Mapping[str, Any],
    extra: Mapping[str, Any] | None = None,
) -> int:
    if _db_disabled() or (not AI_LOG_SNAPSHOTS):
        return 0
    if not _should_sample(f"snap:regime:{regime}", AI_LOG_SNAPSHOT_SAMPLE_RATE):
        return 0

    payload: dict[str, Any] = {
        "ts": _ts(),
        "regime": regime,
        "state": dict(state),
        "ctx": _base_ctx(),
    }
    if extra:
        payload["extra"] = dict(extra)

    _submit(_a_insert_heartbeat("regime", payload))
    return 1


def log_equity_curve_point(
    *,
    equity_usd: float | None = None,
    balance_sol: float | None = None,
    pnl_day_pct: float | None = None,
    pnl_total_pct: float | None = None,
    extra: Mapping[str, Any] | None = None,
) -> int:
    if _db_disabled() or (not AI_LOG_SNAPSHOTS):
        return 0
    if not _should_sample("snap:equity", AI_LOG_SNAPSHOT_SAMPLE_RATE):
        return 0

    payload: dict[str, Any] = {
        "ts": _ts(),
        "equity_usd": float(equity_usd) if equity_usd is not None else None,
        "balance_sol": float(balance_sol) if balance_sol is not None else None,
        "pnl_day_pct": float(pnl_day_pct) if pnl_day_pct is not None else None,
        "pnl_total_pct": float(pnl_total_pct) if pnl_total_pct is not None else None,
        "ctx": _base_ctx(),
    }
    if extra:
        payload["extra"] = dict(extra)

    _submit(_a_insert_heartbeat("equity", payload))
    return 1


def log_risk_manager_snapshot(*, snapshot: Mapping[str, Any]) -> int:
    if _db_disabled() or (not AI_LOG_SNAPSHOTS):
        return 0
    if not _should_sample("snap:risk_manager", AI_LOG_SNAPSHOT_SAMPLE_RATE):
        return 0

    payload = {"ts": _ts(), "snapshot": dict(snapshot), "ctx": _base_ctx()}
    _submit(_a_insert_heartbeat("risk_manager", payload))
    return 1


def log_reinvestor_snapshot(*, snapshot: Mapping[str, Any]) -> int:
    if _db_disabled() or (not AI_LOG_SNAPSHOTS):
        return 0
    if not _should_sample("snap:reinvestor", AI_LOG_SNAPSHOT_SAMPLE_RATE):
        return 0

    payload = {"ts": _ts(), "snapshot": dict(snapshot), "ctx": _base_ctx()}
    _submit(_a_insert_heartbeat("reinvestor", payload))
    return 1
