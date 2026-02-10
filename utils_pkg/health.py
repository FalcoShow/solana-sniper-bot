from __future__ import annotations

"""
utils_pkg/health.py

God-Tier Health Snapshot (P0) — deterministic, bounded, never-hangs
==================================================================

Canonical health model
----------------------
This module defines the ONE canonical health model used everywhere:
- health_snapshot(): async → dict payload (JSON-serializable)
- health_snapshot_sync(): sync wrapper for scripts/tests

Tri-state status with two booleans
---------------------------------
- status="ok"        → ok=True,  ready=True
- status="degraded"  → ok=True,  ready=False   (optional dependency issues)
- status="fail"      → ok=False, ready=False   (required check(s) failed / total timeout)

Rules:
- Any REQUIRED check failing => status="fail"
- Otherwise, any OPTIONAL check failing => status="degraded"
- Otherwise => status="ok"

Hard requirements
-----------------
- Never hangs: bounded by HEALTH_TOTAL_TIMEOUT_S (and per-check timeouts).
- Never raises: returns ok/ready with error details.
- Import-safe: NO network connects on import.
- Pytest-safe: no logging reconfiguration / handler mutation on import.
- Short cache TTL to avoid flapping / storms.
- Partial results preserved even near timeout (deadline budgeting).

Env keys used (must exist in canonical contract + .env.example)
--------------------------------------------------------------
HEALTH_CACHE_TTL_S
HEALTH_TOTAL_TIMEOUT_S
HEALTH_CHECK_TIMEOUT_S
HEALTH_DB_TIMEOUT_S
HEALTH_DB_STATEMENT_TIMEOUT_MS
HEALTH_REQUIRE_DB_IF_ENABLED
HEALTH_INCLUDE_SYSTEM

Also reads (debug/system only):
APP_ENV

Optional DB posture (IMPORTANT)
------------------------------
- If AI DB is enabled but unavailable:
  - If HEALTH_REQUIRE_DB_IF_ENABLED is true => REQUIRED => status="fail"
  - Otherwise OPTIONAL => status="degraded" (ok=True, ready=False)
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
from typing import Any, Awaitable, Callable, Literal, cast

log = logging.getLogger("health")

HealthStatus = Literal["ok", "degraded", "fail"]


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


def _json_safe(v: Any) -> Any:
    # Keep payload JSON-serializable without raising.
    if v is None or isinstance(v, (bool, int, float, str)):
        return v
    if isinstance(v, (list, tuple)):
        return [_json_safe(x) for x in v]
    if isinstance(v, dict):
        return {str(k): _json_safe(val) for k, val in v.items()}
    return str(v)


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

    # Optional DB posture by default; require only when explicitly enabled by env.
    require_db = _env_bool("HEALTH_REQUIRE_DB_IF_ENABLED", False)
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

_LOCKS_BY_LOOP_ID: dict[int, asyncio.Lock] = {}
_LOOP_REFS: dict[int, weakref.ref[Any]] = {}


def _get_loop_lock() -> asyncio.Lock:
    loop = asyncio.get_running_loop()
    lid = id(loop)
    lock = _LOCKS_BY_LOOP_ID.get(lid)
    if lock is not None:
        return lock

    lock = asyncio.Lock()
    _LOCKS_BY_LOOP_ID[lid] = lock

    try:

        def _cleanup(_ref: Any, _lid: int = lid) -> None:
            _LOCKS_BY_LOOP_ID.pop(_lid, None)
            _LOOP_REFS.pop(_lid, None)

        _LOOP_REFS[lid] = weakref.ref(loop, _cleanup)
    except Exception:
        pass

    return lock


def _cache_get_if_fresh(cfg: HealthConfig) -> dict[str, Any] | None:
    global _LAST_SNAPSHOT, _LAST_SNAPSHOT_AT_MONO
    if _LAST_SNAPSHOT is None or cfg.cache_ttl_s <= 0:
        return None
    age = time.monotonic() - _LAST_SNAPSHOT_AT_MONO
    if age <= cfg.cache_ttl_s:
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
    check: dict[str, Any] = {
        "name": str(name),
        "required": bool(required),
        "ok": False,
        "ms": 0,
    }
    try:
        res = await asyncio.wait_for(fn(), timeout=float(timeout_s))
        if isinstance(res, dict):
            res2: dict[str, Any] = {}
            for k, v in res.items():
                if k in ("name", "required"):
                    continue
                res2[str(k)] = _json_safe(v)
            check.update(res2)
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


def _compute_status(checks: list[dict[str, Any]]) -> HealthStatus:
    required_failed = [c for c in checks if c.get("required") and not c.get("ok")]
    if required_failed:
        return "fail"
    optional_failed = [c for c in checks if not c.get("required") and not c.get("ok")]
    if optional_failed:
        return "degraded"
    return "ok"


# -----------------------------------------------------------------------------
# Individual checks (import-safe; no network on import)
# -----------------------------------------------------------------------------


async def _check_runtime() -> dict[str, Any]:
    return {"ok": True, "detail": "runtime_ok", "ts": _utcnow_iso()}


_DSN_PASS_RE = re.compile(
    r"(?P<prefix>://[^:/?#]+:)(?P<pw>[^@/?#]+)(?P<suffix>@)",
    re.IGNORECASE,
)


def _redact_dsn(dsn: str) -> str:
    if not dsn:
        return ""
    return _DSN_PASS_RE.sub(r"\g<prefix>***\g<suffix>", dsn)


def _ai_db_enabled_fallback() -> bool:
    # Conservative fallback (do not break if older env name used).
    return _env_bool("AI_DB_ENABLED", False) or _env_bool("AIDB_ENABLED", False)


async def _check_ai_db(cfg: HealthConfig) -> dict[str, Any]:
    """
    Deterministic DB check:
    - If AI_DB disabled → ok=True (detail=disabled)
    - If enabled → bounded connect + SELECT 1 (or ai_db-provided probe if present)
    Never raises.
    """
    try:
        from utils_pkg import ai_db  # type: ignore
    except Exception as e:
        if _ai_db_enabled_fallback():
            return {"ok": False, "detail": "ai_db_import_failed", "err": f"{type(e).__name__}: {e}"}
        return {"ok": True, "detail": "ai_db_disabled"}

    enabled = bool(getattr(ai_db, "AI_DB_ENABLED", False)) or _ai_db_enabled_fallback()
    if not enabled:
        return {"ok": True, "detail": "ai_db_disabled"}

    # If ai_db exposes a minimal probe, prefer it (no duplicated logic).
    # Signature expected: await ai_db.health_probe(timeout_s=..., statement_timeout_ms=...)
    probe = getattr(ai_db, "health_probe", None)
    if callable(probe):
        try:
            probe_fn = cast(Callable[..., Awaitable[Any]], probe)
            res_any = await asyncio.wait_for(
                probe_fn(
                    timeout_s=float(cfg.db_timeout_s),
                    statement_timeout_ms=int(cfg.db_statement_timeout_ms),
                ),
                timeout=float(cfg.db_timeout_s),
            )
            if isinstance(res_any, dict):
                return {str(k): _json_safe(v) for k, v in res_any.items()}
            return {
                "ok": False,
                "detail": "health_probe_bad_return",
                "type": type(res_any).__name__,
            }
        except asyncio.TimeoutError:
            return {"ok": False, "detail": "db_timeout"}
        except Exception as e:
            return {"ok": False, "detail": "db_error", "err": f"{type(e).__name__}: {e}"}

    # Fallback: direct asyncpg connect + SELECT 1
    try:
        import asyncpg  # type: ignore
    except Exception as e:
        return {"ok": False, "detail": "asyncpg_missing", "err": f"{type(e).__name__}: {e}"}

    dsn: str = ""
    ssl_ctx: Any = None
    sslmode: Any = None
    host: Any = None

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
            dsn = (
                _env_str("DATABASE_URL", "")
                or _env_str("POSTGRES_DSN", "")
                or _env_str("PG_DSN", "")
            ).strip()
    except Exception as e:
        return {"ok": False, "detail": "dsn_unresolved", "err": f"{type(e).__name__}: {e}"}

    if not dsn:
        return {"ok": False, "detail": "dsn_empty"}

    server_settings: dict[str, str] = {
        "application_name": str(getattr(ai_db, "AI_DB_APP_NAME", "health")),
        "timezone": "UTC",
    }
    if cfg.db_statement_timeout_ms > 0:
        server_settings["statement_timeout"] = str(int(cfg.db_statement_timeout_ms))

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
        val = await asyncio.wait_for(
            conn.fetchval("SELECT 1"),
            timeout=float(cfg.db_timeout_s),
        )
        ok = bool(val == 1)
        return {
            "ok": ok,
            "detail": "select_1_ok" if ok else "select_1_bad",
            "dsn": dsn_dbg,
            "sslmode": str(sslmode) if sslmode is not None else "",
            "host": str(host) if host is not None else "",
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

    started = time.monotonic()
    deadline = started + float(cfg.total_timeout_s)

    def _time_left() -> float:
        return max(0.0, deadline - time.monotonic())

    lock = _get_loop_lock()
    warnings: list[str] = []
    fix_hints: list[str] = []

    try:
        async with lock:
            if not deep:
                cached2 = _cache_get_if_fresh(cfg)
                if cached2 is not None:
                    return cached2

            checks: list[dict[str, Any]] = []

            # --- runtime (required)
            tl = _time_left()
            if tl <= 0.0:
                return {
                    "ts": _utcnow_iso(),
                    "status": "fail",
                    "ok": False,
                    "ready": False,
                    "ms": _ms_since(started),
                    "checks": checks,
                    "warnings": warnings,
                    "fix_hints": fix_hints,
                    "err": "health_snapshot_total_timeout_before_checks",
                }

            runtime_timeout = min(float(cfg.check_timeout_s), tl)
            checks.append(
                await _run_check(
                    "runtime", _check_runtime, timeout_s=runtime_timeout, required=True
                )
            )

            # Decide whether DB is REQUIRED for overall readiness.
            db_required = False
            db_enabled = False
            try:
                from utils_pkg import ai_db  # type: ignore

                db_enabled = (
                    bool(getattr(ai_db, "AI_DB_ENABLED", False)) or _ai_db_enabled_fallback()
                )
            except Exception:
                db_enabled = _ai_db_enabled_fallback()

            if db_enabled and cfg.require_db_if_enabled:
                db_required = True

            # --- ai_db (optional by default unless require_db_if_enabled=true)
            tl = _time_left()
            if tl > 0.0:
                db_timeout = min(float(cfg.db_timeout_s), tl)
                checks.append(
                    await _run_check(
                        "ai_db",
                        lambda: _check_ai_db(cfg),
                        timeout_s=db_timeout,
                        required=db_required,
                    )
                )
            else:
                # Preserve partial results; mark DB check skipped due to deadline.
                checks.append(
                    {
                        "name": "ai_db",
                        "required": bool(db_required),
                        "ok": False,
                        "ms": 0,
                        "err": "skipped_due_to_deadline",
                    }
                )

            status: HealthStatus = _compute_status(checks)

            # ok=True means "service can run" (fail only if required checks fail)
            ok = bool(status != "fail")
            ready = bool(status == "ok")

            payload: dict[str, Any] = {
                "ts": _utcnow_iso(),
                "status": status,
                "ok": ok,
                "ready": ready,
                "ms": _ms_since(started),
                "checks": checks,
                "warnings": warnings,
                "fix_hints": fix_hints,
            }

            if include_system:
                payload["system"] = {
                    "python": sys.version.split()[0],
                    "platform": platform.platform(),
                    "pid": os.getpid(),
                    "app_env": _env_str("APP_ENV", ""),
                }

            if not deep:
                _cache_set(payload)

            return payload

    except Exception as e:
        with suppress(Exception):
            log.debug("health_snapshot failed", exc_info=True)
        return {
            "ts": _utcnow_iso(),
            "status": "fail",
            "ok": False,
            "ready": False,
            "ms": _ms_since(started),
            "checks": [],
            "warnings": warnings,
            "fix_hints": fix_hints,
            "err": f"{type(e).__name__}: {e}",
        }


def health_snapshot_sync(
    *, deep: bool = False, include_system: bool | None = None
) -> dict[str, Any]:
    """
    Sync wrapper for scripts/tests.
    Never raises.

    If called from within an already-running event loop,
    return a safe error payload (caller should `await health_snapshot()` instead).
    """
    try:
        asyncio.get_running_loop()
        return {
            "ts": _utcnow_iso(),
            "status": "fail",
            "ok": False,
            "ready": False,
            "ms": 0,
            "checks": [],
            "warnings": [],
            "fix_hints": [],
            "err": "health_snapshot_sync_called_inside_event_loop_use_await",
        }
    except RuntimeError:
        pass

    try:
        return asyncio.run(health_snapshot(deep=deep, include_system=include_system))
    except Exception as e:
        return {
            "ts": _utcnow_iso(),
            "status": "fail",
            "ok": False,
            "ready": False,
            "ms": 0,
            "checks": [],
            "warnings": [],
            "fix_hints": [],
            "err": f"{type(e).__name__}: {e}",
        }


__all__ = ["health_snapshot", "health_snapshot_sync"]
