from __future__ import annotations

"""
utils_pkg/health.py

God-Tier Health Snapshot (P0) — deterministic, bounded, never-hangs
==================================================================

Goals
-----
- ONE canonical async health_snapshot() used by:
  - Render health endpoint
  - auto_exit_worker
  - CLI health scripts/tests
- Health must never hang.
- Health must never raise (returns ok=False with error details instead).
- Uses strict timeouts and a short cache TTL to avoid flapping/connection storms.
- Import-safe: NO network connects on import.
- Pytest-safe: DO NOT reconfigure logging or mutate handler streams at import time.

Design
------
- health_snapshot(): async → dict payload (JSON-serializable)
- health_snapshot_sync(): sync helper for scripts/tests
- Per-check timeouts + total timeout guard
- Optional DB check that uses direct asyncpg connect (strict timeout); SSL inference via utils_pkg.ai_db when available
- Cache TTL + per-loop lock to avoid thundering herds

Env keys used (must exist in canonical contract + .env.example)
--------------------------------------------------------------
HEALTH_CACHE_TTL_S
HEALTH_TOTAL_TIMEOUT_S
HEALTH_CHECK_TIMEOUT_S
HEALTH_DB_TIMEOUT_S
HEALTH_DB_STATEMENT_TIMEOUT_MS
HEALTH_REQUIRE_DB_IF_ENABLED
HEALTH_INCLUDE_SYSTEM
"""

import asyncio
import logging
import os
import platform
import re
import sys
import time
import weakref
from contextlib import suppress
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Awaitable, Callable

log = logging.getLogger("health")

# -----------------------------------------------------------------------------
# Small env helpers (strict + deterministic)
# -----------------------------------------------------------------------------


def _env_str(key: str, default: str = "") -> str:
    v = os.getenv(key)
    return default if v is None else str(v)


def _env_int(key: str, default: int) -> int:
    raw = os.getenv(key)
    if raw is None:
        return int(default)
    s = str(raw).strip()
    if not s:
        return int(default)
    try:
        return int(s)
    except Exception:
        return int(default)


def _env_float(key: str, default: float) -> float:
    raw = os.getenv(key)
    if raw is None:
        return float(default)
    s = str(raw).strip()
    if not s:
        return float(default)
    try:
        return float(s)
    except Exception:
        return float(default)


def _env_bool(key: str, default: bool = False) -> bool:
    raw = os.getenv(key)
    if raw is None:
        return bool(default)
    s = str(raw).strip().lower()
    if s in ("1", "true", "yes", "y", "on", "t"):
        return True
    if s in ("0", "false", "no", "n", "off", "f"):
        return False
    return bool(default)


def _utcnow_iso() -> str:
    return datetime.now(UTC).isoformat()


def _ms_since(t0: float) -> int:
    return int(max(0.0, (time.monotonic() - t0) * 1000.0))


def _clamp(v: float, lo: float, hi: float) -> float:
    try:
        x = float(v)
    except Exception:
        return float(lo)
    return float(max(lo, min(hi, x)))


# -----------------------------------------------------------------------------
# Config (loaded per-call so tests can tweak env without reimport)
# -----------------------------------------------------------------------------


@dataclass(frozen=True)
class HealthConfig:
    cache_ttl_s: float
    total_timeout_s: float
    check_timeout_s: float
    db_timeout_s: float
    db_statement_timeout_ms: int
    require_db_if_enabled: bool
    include_system_default: bool


def _load_config() -> HealthConfig:
    cache_ttl_s = _clamp(_env_float("HEALTH_CACHE_TTL_S", 5.0), 0.0, 60.0)
    total_timeout_s = _clamp(_env_float("HEALTH_TOTAL_TIMEOUT_S", 4.0), 0.2, 30.0)
    check_timeout_s = _clamp(_env_float("HEALTH_CHECK_TIMEOUT_S", 2.0), 0.1, 15.0)
    db_timeout_s = _clamp(_env_float("HEALTH_DB_TIMEOUT_S", 1.8), 0.1, 15.0)

    stmt_ms_raw = _env_int("HEALTH_DB_STATEMENT_TIMEOUT_MS", 1200)
    stmt_ms = int(max(50, min(stmt_ms_raw, 30_000)))

    require_db = _env_bool("HEALTH_REQUIRE_DB_IF_ENABLED", True)
    include_system = _env_bool("HEALTH_INCLUDE_SYSTEM", True)

    return HealthConfig(
        cache_ttl_s=cache_ttl_s,
        total_timeout_s=total_timeout_s,
        check_timeout_s=check_timeout_s,
        db_timeout_s=db_timeout_s,
        db_statement_timeout_ms=stmt_ms,
        require_db_if_enabled=require_db,
        include_system_default=include_system,
    )


# -----------------------------------------------------------------------------
# Snapshot cache + concurrency gate (avoids storms)
# -----------------------------------------------------------------------------

_LAST_SNAPSHOT: dict[str, Any] | None = None
_LAST_SNAPSHOT_AT_MONO: float = 0.0

# Per-loop lock registry (asyncio.run creates new loops in tests)
_LOCKS_BY_LOOP_ID: dict[int, asyncio.Lock] = {}
_LOOP_REFS: dict[int, weakref.ref[Any]] = {}


def _get_loop_lock() -> asyncio.Lock:
    """
    Return a lock scoped to the running event loop to avoid cross-loop reuse issues.
    Uses weakref cleanup when possible.
    """
    loop = asyncio.get_running_loop()
    lid = id(loop)

    lock = _LOCKS_BY_LOOP_ID.get(lid)
    if lock is not None:
        return lock

    lock = asyncio.Lock()
    _LOCKS_BY_LOOP_ID[lid] = lock

    # Best-effort cleanup when loop is GC'd
    try:

        def _cleanup(_ref: Any, _lid: int = lid) -> None:
            _LOCKS_BY_LOOP_ID.pop(_lid, None)
            _LOOP_REFS.pop(_lid, None)

        _LOOP_REFS[lid] = weakref.ref(loop, _cleanup)
    except Exception:
        # If loop isn't weakrefable, keep the lock; it’s still correct.
        pass

    return lock


def _cache_get_if_fresh(cfg: HealthConfig) -> dict[str, Any] | None:
    global _LAST_SNAPSHOT, _LAST_SNAPSHOT_AT_MONO
    if _LAST_SNAPSHOT is None:
        return None
    if cfg.cache_ttl_s <= 0:
        return None
    age = time.monotonic() - _LAST_SNAPSHOT_AT_MONO
    if age <= cfg.cache_ttl_s:
        # Return a shallow copy so callers don't mutate cached dict accidentally.
        return dict(_LAST_SNAPSHOT)
    return None


def _cache_set(snapshot: dict[str, Any]) -> None:
    global _LAST_SNAPSHOT, _LAST_SNAPSHOT_AT_MONO
    _LAST_SNAPSHOT = snapshot
    _LAST_SNAPSHOT_AT_MONO = time.monotonic()


# -----------------------------------------------------------------------------
# Check runner (never raises)
# -----------------------------------------------------------------------------


async def _run_check(
    name: str,
    fn: Callable[[], Awaitable[dict[str, Any]]],
    *,
    timeout_s: float,
    required: bool,
) -> dict[str, Any]:
    t0 = time.monotonic()
    check: dict[str, Any] = {"name": name, "required": bool(required), "ok": False, "ms": 0}
    try:
        res = await asyncio.wait_for(fn(), timeout=float(timeout_s))
        if isinstance(res, dict):
            # Do not allow caller to stomp identity fields
            res = dict(res)
            res.pop("name", None)
            res.pop("required", None)
            check.update(res)
        check["ok"] = bool(check.get("ok", False))
    except asyncio.TimeoutError:
        check["ok"] = False
        check["err"] = "timeout"
    except Exception as e:
        check["ok"] = False
        check["err"] = f"{type(e).__name__}: {e}"
    finally:
        check["ms"] = _ms_since(t0)
    return check


# -----------------------------------------------------------------------------
# Individual checks (import-safe; no network on import)
# -----------------------------------------------------------------------------


async def _check_runtime() -> dict[str, Any]:
    return {"ok": True, "detail": "runtime_ok", "ts": _utcnow_iso()}


_DSN_PASS_RE = re.compile(r"(?P<prefix>://[^:/?#]+:)(?P<pw>[^@/?#]+)(?P<suffix>@)", re.IGNORECASE)


def _redact_dsn(dsn: str) -> str:
    if not dsn:
        return ""
    return _DSN_PASS_RE.sub(r"\g<prefix>***\g<suffix>", dsn)


def _ai_db_enabled_fallback() -> bool:
    # Conservative fallback if ai_db does not expose AI_DB_ENABLED
    return _env_bool("AI_DB_ENABLED", False) or _env_bool("AIDB_ENABLED", False)


async def _check_ai_db(cfg: HealthConfig) -> dict[str, Any]:
    """
    Deterministic DB check:
    - If AI_DB disabled → ok=True (detail=disabled)
    - If enabled → one asyncpg connect (strict timeout) + SELECT 1
    Never raises.
    """
    try:
        from utils_pkg import ai_db  # local import; must be import-safe by design
    except Exception as e:
        # If we can't even import ai_db, treat as unhealthy only if enabled is implied by env.
        if _ai_db_enabled_fallback():
            return {"ok": False, "detail": "ai_db_import_failed", "err": f"{type(e).__name__}: {e}"}
        return {"ok": True, "detail": "ai_db_disabled"}

    enabled = bool(getattr(ai_db, "AI_DB_ENABLED", False))
    if not enabled:
        # Some builds may rely on env fallback
        enabled = _ai_db_enabled_fallback()

    if not enabled:
        return {"ok": True, "detail": "ai_db_disabled"}

    try:
        import asyncpg  # type: ignore
    except Exception as e:
        return {"ok": False, "detail": "asyncpg_missing", "err": f"{type(e).__name__}: {e}"}

    dsn: str = ""
    ssl_ctx: Any = None
    sslmode: Any = None
    host: Any = None

    # Prefer ai_db helpers if present
    try:
        if hasattr(ai_db, "resolve_dsn_info"):
            info = ai_db.resolve_dsn_info()
            dsn = getattr(info, "normalized_asyncpg", "") or ""
            sslmode = getattr(info, "sslmode", None)
            host = getattr(info, "host", None)
            if hasattr(ai_db, "ssl_context_for_dsn"):
                ssl_ctx = ai_db.ssl_context_for_dsn(dsn)
        elif hasattr(ai_db, "get_normalized_asyncpg_dsn"):
            dsn = str(ai_db.get_normalized_asyncpg_dsn())
            if hasattr(ai_db, "ssl_context_for_dsn"):
                ssl_ctx = ai_db.ssl_context_for_dsn(dsn)
        elif hasattr(ai_db, "get_normalized_dsn"):
            dsn = str(ai_db.get_normalized_dsn())
            if hasattr(ai_db, "ssl_context_for_dsn"):
                ssl_ctx = ai_db.ssl_context_for_dsn(dsn)
        else:
            # Last resort: try DATABASE_URL / POSTGRES_DSN envs without breaking determinism
            dsn = (
                _env_str("DATABASE_URL", "")
                or _env_str("POSTGRES_DSN", "")
                or _env_str("PG_DSN", "")
            )
            dsn = dsn.strip()
    except Exception as e:
        return {"ok": False, "detail": "dsn_unresolved", "err": f"{type(e).__name__}: {e}"}

    if not dsn:
        return {"ok": False, "detail": "dsn_empty"}

    server_settings: dict[str, str] = {
        "application_name": getattr(ai_db, "AI_DB_APP_NAME", "health"),
        "timezone": "UTC",
    }
    if cfg.db_statement_timeout_ms > 0:
        server_settings["statement_timeout"] = str(int(cfg.db_statement_timeout_ms))

    # Safe debug DSN (should never contain password)
    try:
        if hasattr(ai_db, "get_normalized_dsn_for_debug"):
            dsn_dbg = str(ai_db.get_normalized_dsn_for_debug())
        else:
            dsn_dbg = _redact_dsn(dsn)
    except Exception:
        dsn_dbg = _redact_dsn(dsn)

    conn = None
    try:
        conn = await asyncpg.connect(
            dsn,
            timeout=float(cfg.db_timeout_s),
            ssl=ssl_ctx,
            server_settings=server_settings,
        )
        val = await asyncio.wait_for(conn.fetchval("SELECT 1"), timeout=float(cfg.db_timeout_s))
        ok = bool(val == 1)
        return {
            "ok": ok,
            "detail": "select_1_ok" if ok else "select_1_bad",
            "dsn": dsn_dbg,
            "sslmode": sslmode,
            "host": host,
        }
    except asyncio.TimeoutError:
        return {"ok": False, "detail": "db_timeout", "dsn": dsn_dbg}
    except Exception as e:
        return {
            "ok": False,
            "detail": "db_error",
            "err": f"{type(e).__name__}: {e}",
            "dsn": dsn_dbg,
        }
    finally:
        if conn is not None:
            with suppress(Exception):
                await asyncio.wait_for(conn.close(), timeout=0.75)


# -----------------------------------------------------------------------------
# Public API
# -----------------------------------------------------------------------------


async def health_snapshot(
    *, deep: bool = False, include_system: bool | None = None
) -> dict[str, Any]:
    """
    Returns a deterministic JSON-serializable dict.
    Never raises. Never hangs (bounded by HEALTH_TOTAL_TIMEOUT_S).
    """
    cfg = _load_config()
    if include_system is None:
        include_system = cfg.include_system_default

    if not deep:
        cached = _cache_get_if_fresh(cfg)
        if cached is not None:
            return cached

    async def _build() -> dict[str, Any]:
        started = time.monotonic()
        lock = _get_loop_lock()

        async with lock:
            if not deep:
                cached2 = _cache_get_if_fresh(cfg)
                if cached2 is not None:
                    return cached2

            # Bound per-check timeouts to the total timeout to prevent runaway.
            per_check = float(min(cfg.check_timeout_s, cfg.total_timeout_s))

            checks: list[dict[str, Any]] = []
            checks.append(
                await _run_check("runtime", _check_runtime, timeout_s=per_check, required=True)
            )

            # Decide whether DB is "required" for overall ok.
            db_required = False
            try:
                from utils_pkg import ai_db  # type: ignore

                enabled = bool(getattr(ai_db, "AI_DB_ENABLED", False)) or _ai_db_enabled_fallback()
                if enabled and cfg.require_db_if_enabled:
                    db_required = True
            except Exception:
                # If we can't import ai_db, only treat as required in deep mode w/ require flag.
                db_required = bool(deep and cfg.require_db_if_enabled)

            checks.append(
                await _run_check(
                    "ai_db",
                    lambda: _check_ai_db(cfg),
                    timeout_s=float(min(cfg.db_timeout_s, cfg.total_timeout_s)),
                    required=db_required,
                )
            )

            required_failed = [c for c in checks if c.get("required") and not c.get("ok")]
            ok = len(required_failed) == 0

            payload: dict[str, Any] = {
                "ts": _utcnow_iso(),
                "ok": bool(ok),
                "ms": _ms_since(started),
                "checks": checks,
            }

            if include_system:
                # Keep lightweight; no psutil, no disk scans, no network.
                payload["system"] = {
                    "python": sys.version.split()[0],
                    "platform": platform.platform(),
                    "pid": os.getpid(),
                    "app_env": _env_str("APP_ENV", ""),
                }

            if not deep:
                _cache_set(payload)

            return payload

    try:
        return await asyncio.wait_for(_build(), timeout=float(cfg.total_timeout_s))
    except asyncio.TimeoutError:
        return {
            "ts": _utcnow_iso(),
            "ok": False,
            "ms": int(float(cfg.total_timeout_s) * 1000),
            "checks": [],
            "err": "health_snapshot_total_timeout",
        }
    except Exception as e:
        # Never raise from health endpoint; keep it deterministic.
        with suppress(Exception):
            log.debug("health_snapshot failed", exc_info=True)
        return {
            "ts": _utcnow_iso(),
            "ok": False,
            "ms": 0,
            "checks": [],
            "err": f"{type(e).__name__}: {e}",
        }


def health_snapshot_sync(
    *, deep: bool = False, include_system: bool | None = None
) -> dict[str, Any]:
    """
    Sync wrapper for scripts/tests.
    Never raises.

    Note: If called from within an already-running event loop,
    return a safe error payload (caller should `await health_snapshot()` instead).
    """
    try:
        asyncio.get_running_loop()
        return {
            "ts": _utcnow_iso(),
            "ok": False,
            "ms": 0,
            "checks": [],
            "err": "health_snapshot_sync_called_inside_event_loop_use_await",
        }
    except RuntimeError:
        pass

    try:
        return asyncio.run(health_snapshot(deep=deep, include_system=include_system))
    except Exception as e:
        return {
            "ts": _utcnow_iso(),
            "ok": False,
            "ms": 0,
            "checks": [],
            "err": f"{type(e).__name__}: {e}",
        }


__all__ = ["health_snapshot", "health_snapshot_sync"]
