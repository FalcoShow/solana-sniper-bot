# scripts/health_http.py
from __future__ import annotations

"""
scripts/health_http.py — God-Tier Health HTTP (P0 LOCK-IN)
==========================================================

Purpose
-------
Tiny FastAPI service suitable for Render health checks + human debugging:

  GET /livez   -> process is up (always 200 if server running)
  GET /readyz  -> dependency readiness (required checks only) else 503
  GET /healthz -> alias of /readyz
  GET /        -> redirect to /healthz

Key behaviors (IMPORTANT)
------------------------
- Checks are cached with TTL (default 5s) to avoid hammering dependencies.
- Dependency checks are optional by default:
    - Optional failures DO NOT make readiness fail unless:
        (a) HEALTH_FAIL_ON_OPTIONAL=1, OR
        (b) the check is marked required via HEALTH_REQUIRE_*=1
- Backwards compatible skip envs:
    - HEALTHCHECK_SKIP_DB=1 disables PG check
    - HEALTHCHECK_SKIP_REDIS=1 disables Redis check
    - HEALTHCHECK_SKIP_RPC=1 disables RPC check
- Secret-safe output:
    - never prints credentials; URLs are masked; PG DSN presence is boolean only.

Env
---
Server:
  PORT=8000 (or HEALTH_PORT)
  HEALTH_HOST=0.0.0.0
  HEALTH_LOG_LEVEL=info
  HEALTH_PATH_PREFIX=""          (e.g. "/api" to serve /api/healthz)
  HEALTH_DISABLE_DOCS=1          (disable /docs + /redoc)

Caching / behavior:
  HEALTH_CACHE_TTL_SEC=5
  HEALTH_VERBOSE=0               (1 includes extra details)
  HEALTH_MAX_PARALLEL=6
  HEALTH_DEP_TIMEOUT_S=6.5       (per-dependency check timeout)
  HEALTH_FAIL_ON_OPTIONAL=0      (if 1, any enabled failing check makes readiness fail)

Dependency toggles:
  HEALTH_CHECK_REDIS=1
  HEALTH_CHECK_PG=1
  HEALTH_CHECK_RPC=0
  HEALTH_CHECK_ROUTER=0

Require toggles (readiness fails if required check fails):
  HEALTH_REQUIRE_REDIS=0
  HEALTH_REQUIRE_PG=0
  HEALTH_REQUIRE_RPC=0

Redis:
  REDIS_URL / REDIS_TLS_URL / REDIS

Postgres (any one):
  SNIPER_DB_DSN / PG_DSN_EXTERNAL / POSTGRES_DSN / PG_DSN / PG_DSN_INTERNAL / DATABASE_URL

Solana RPC:
  SOLANA_RPC_URL / RPC_URL / SOLANA_RPC_HTTP / SOL_RPC_URL / RPC_URLS / SOL_RPC_URLS

Notes
-----
- This file is the server.
- scripts/healthcheck.py is the client (deploy runner) that calls /readyz with retries.
"""

import asyncio
import contextlib
import os
import platform
import re
import time
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from typing import Any, Awaitable, Callable, cast

from fastapi import FastAPI, Query, Response
from fastapi.responses import JSONResponse, RedirectResponse

# ------------------------------------------------------------------------------
# Logging (prefer canonical logging_setup.get_logger; fallback safe)
# ------------------------------------------------------------------------------
try:
    from logging_setup import get_logger  # type: ignore
except Exception:  # pragma: no cover
    import logging

    def get_logger(name: str | None = None):  # type: ignore
        logging.basicConfig(level=logging.INFO)
        return logging.getLogger(name or "health_http")


log = get_logger("health_http")

# ------------------------------------------------------------------------------
# Optional deps (best-effort)
# ------------------------------------------------------------------------------
try:
    import httpx  # type: ignore
except Exception:  # pragma: no cover
    httpx = None

try:
    import redis.asyncio as aioredis  # type: ignore
except Exception:  # pragma: no cover
    aioredis = None

try:
    import asyncpg  # type: ignore
except Exception:  # pragma: no cover
    asyncpg = None


# ------------------------------------------------------------------------------
# Config helpers
# ------------------------------------------------------------------------------
_TRUE = {"1", "true", "yes", "y", "on", "t"}
_FALSE = {"0", "false", "no", "n", "off", "f"}

_URL_CRED_RE = re.compile(r"(://)([^/@\s]+)@")


def _envs(name: str, default: str = "") -> str:
    v = os.getenv(name)
    s = v if v is not None else default
    s = str(s).strip()
    # strip surrounding quotes (common in .env)
    if (s.startswith('"') and s.endswith('"')) or (s.startswith("'") and s.endswith("'")):
        s = s[1:-1].strip()
    return s


def _envb(name: str, default: bool = False) -> bool:
    v = _envs(name, "")
    if v == "":
        return bool(default)
    s = v.lower().strip()
    if s in _TRUE:
        return True
    if s in _FALSE:
        return False
    return bool(default)


def _envi(name: str, default: int) -> int:
    try:
        return int(float(_envs(name, str(default))))
    except Exception:
        return int(default)


def _envf(name: str, default: float) -> float:
    try:
        return float(_envs(name, str(default)))
    except Exception:
        return float(default)


def _now_ms() -> int:
    return int(time.time() * 1000)


def _render_detected() -> bool:
    return bool(
        _envs("RENDER_SERVICE_ID", "")
        or _envs("RENDER_INSTANCE_ID", "")
        or _envs("RENDER", "").lower() in _TRUE
    )


def _mask_url(u: str) -> str:
    if not u:
        return ""
    out = _URL_CRED_RE.sub(r"\1***@", str(u))
    if "?" in out:
        out = out.split("?", 1)[0] + "?..."
    return out


# Back-compat skip flags you’re using already in PowerShell
def _skip_flag(name: str) -> bool:
    # Treat missing as False; treat 1/true as True
    return _envb(name, False)


def _resolve_pg_dsn() -> str | None:
    # Include your preferred keys first
    for k in (
        "SNIPER_DB_DSN",
        "PG_DSN_EXTERNAL",
        "POSTGRES_DSN",
        "PG_DSN",
        "PG_DSN_INTERNAL",
        "DATABASE_URL",
    ):
        v = _envs(k, "")
        if v:
            return v
    return None


def _resolve_redis_url() -> str:
    return _envs("REDIS_URL", "") or _envs("REDIS_TLS_URL", "") or _envs("REDIS", "")


def _resolve_rpc_url() -> str | None:
    for k in (
        "SOLANA_RPC_URL",
        "RPC_URL",
        "SOLANA_RPC_HTTP",
        "SOL_RPC_URL",
        "SOL_RPC_URLS",
        "RPC_URLS",
    ):
        v = _envs(k, "")
        if v:
            if "," in v or ";" in v:
                cand = [p.strip() for p in v.replace(";", ",").split(",") if p.strip()]
                return cand[0] if cand else None
            return v
    return None


# ------------------------------------------------------------------------------
# Models
# ------------------------------------------------------------------------------
@dataclass
class CheckResult:
    ok: bool
    status: str  # "ok" | "fail" | "skip"
    detail: str = ""
    latency_ms: int = 0
    ts_ms: int = 0

    def compact(self) -> dict[str, Any]:
        return {
            "ok": bool(self.ok),
            "status": str(self.status),
            "latency_ms": int(self.latency_ms),
            "ts_ms": int(self.ts_ms),
        }

    def full(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class HealthConfig:
    cache_ttl_sec: float
    verbose: bool
    max_parallel: int
    dep_timeout_s: float

    check_redis: bool
    check_pg: bool
    check_rpc: bool
    check_router: bool

    require_redis: bool
    require_pg: bool
    require_rpc: bool

    fail_on_optional: bool


def load_health_config() -> HealthConfig:
    # Back-compat skip vars take precedence
    skip_db = _skip_flag("HEALTHCHECK_SKIP_DB") or _skip_flag("HEALTHCHECK_SKIP_PG")
    skip_redis = _skip_flag("HEALTHCHECK_SKIP_REDIS")
    skip_rpc = _skip_flag("HEALTHCHECK_SKIP_RPC")

    check_redis = _envb("HEALTH_CHECK_REDIS", True) and (not skip_redis)
    check_pg = _envb("HEALTH_CHECK_PG", True) and (not skip_db)
    check_rpc = _envb("HEALTH_CHECK_RPC", False) and (not skip_rpc)
    check_router = _envb("HEALTH_CHECK_ROUTER", False)

    # If a thing is skipped, it cannot be required
    require_redis = _envb("HEALTH_REQUIRE_REDIS", False) and check_redis
    require_pg = _envb("HEALTH_REQUIRE_PG", False) and check_pg
    require_rpc = _envb("HEALTH_REQUIRE_RPC", False) and check_rpc

    return HealthConfig(
        cache_ttl_sec=max(1.0, _envf("HEALTH_CACHE_TTL_SEC", 5.0)),
        verbose=_envb("HEALTH_VERBOSE", False),
        max_parallel=max(1, _envi("HEALTH_MAX_PARALLEL", 6)),
        dep_timeout_s=max(1.0, _envf("HEALTH_DEP_TIMEOUT_S", 6.5)),
        check_redis=check_redis,
        check_pg=check_pg,
        check_rpc=check_rpc,
        check_router=check_router,
        require_redis=require_redis,
        require_pg=require_pg,
        require_rpc=require_rpc,
        fail_on_optional=_envb("HEALTH_FAIL_ON_OPTIONAL", False),
    )


# ------------------------------------------------------------------------------
# State + caching
# ------------------------------------------------------------------------------
class HealthState:
    def __init__(self) -> None:
        self.start_ts_ms = _now_ms()
        self._lock = asyncio.Lock()
        self._cache_ts_ms: int = 0
        self._cache_cfg_sig: str = ""
        self._cache_checks: dict[str, CheckResult] = {}
        self._cache_ready: bool = False
        self._cache_why: str = "init"
        self.http_client: Any = None  # httpx.AsyncClient if available


STATE = HealthState()


def _cfg_signature(cfg: HealthConfig) -> str:
    return "|".join(
        [
            f"ttl={cfg.cache_ttl_sec}",
            f"verb={int(cfg.verbose)}",
            f"mp={cfg.max_parallel}",
            f"to={cfg.dep_timeout_s}",
            f"failopt={int(cfg.fail_on_optional)}",
            f"cr={int(cfg.check_redis)}:{int(cfg.require_redis)}",
            f"pg={int(cfg.check_pg)}:{int(cfg.require_pg)}",
            f"rp={int(cfg.check_rpc)}:{int(cfg.require_rpc)}",
            f"rt={int(cfg.check_router)}",
        ]
    )


async def _call_maybe_async(
    fn: Callable[..., Any],
    timeout_s: float,
    *args: Any,
    **kwargs: Any,
) -> Any:
    """
    If fn is async -> await it.
    If fn is sync -> run in a thread (so we never block the event loop).
    """
    import inspect

    if inspect.iscoroutinefunction(fn):
        return await asyncio.wait_for(
            cast(Awaitable[Any], fn(*args, **kwargs)), timeout=max(1.0, timeout_s)
        )

    # sync path
    return await asyncio.wait_for(
        asyncio.to_thread(fn, *args, **kwargs), timeout=max(1.0, timeout_s)
    )


# ------------------------------------------------------------------------------
# Dependency checks
# ------------------------------------------------------------------------------
async def _check_redis(cfg: HealthConfig) -> CheckResult:
    t0 = _now_ms()
    if not cfg.check_redis:
        return CheckResult(
            ok=True, status="skip", detail="disabled", latency_ms=_now_ms() - t0, ts_ms=_now_ms()
        )

    # Prefer repo canonical helper if present
    try:
        from utils_pkg.core_utils import get_redis_async as core_get_redis_async  # type: ignore

        r = await asyncio.wait_for(core_get_redis_async(), timeout=max(1.0, cfg.dep_timeout_s))
        pong = await asyncio.wait_for(r.ping(), timeout=max(1.0, cfg.dep_timeout_s))
        ok = bool(pong)
        return CheckResult(
            ok=ok,
            status="ok" if ok else "fail",
            detail="ping ok" if ok else "ping failed",
            latency_ms=_now_ms() - t0,
            ts_ms=_now_ms(),
        )
    except Exception:
        # fall through to direct client
        pass

    if aioredis is None:
        ok = not cfg.require_redis
        return CheckResult(
            ok=ok,
            status="skip" if ok else "fail",
            detail="redis client not installed",
            latency_ms=_now_ms() - t0,
            ts_ms=_now_ms(),
        )

    url = _resolve_redis_url()
    if not url:
        ok = not cfg.require_redis
        return CheckResult(
            ok=ok,
            status="skip" if ok else "fail",
            detail="REDIS_URL missing",
            latency_ms=_now_ms() - t0,
            ts_ms=_now_ms(),
        )

    try:
        r2 = aioredis.from_url(url, decode_responses=True)
        pong = await asyncio.wait_for(r2.ping(), timeout=max(1.0, cfg.dep_timeout_s))
        with contextlib.suppress(Exception):
            await r2.close()
        ok = bool(pong)
        return CheckResult(
            ok=ok,
            status="ok" if ok else "fail",
            detail="ping ok" if ok else "ping failed",
            latency_ms=_now_ms() - t0,
            ts_ms=_now_ms(),
        )
    except Exception as e:
        return CheckResult(
            ok=False,
            status="fail",
            detail=f"{type(e).__name__}: {e}",
            latency_ms=_now_ms() - t0,
            ts_ms=_now_ms(),
        )


async def _check_pg(cfg: HealthConfig) -> CheckResult:
    t0 = _now_ms()
    if not cfg.check_pg:
        return CheckResult(
            ok=True, status="skip", detail="disabled", latency_ms=_now_ms() - t0, ts_ms=_now_ms()
        )

    # If no DSN is present at all, treat as skip unless required
    if not _resolve_pg_dsn():
        ok = not cfg.require_pg
        return CheckResult(
            ok=ok,
            status="skip" if ok else "fail",
            detail="no pg dsn set",
            latency_ms=_now_ms() - t0,
            ts_ms=_now_ms(),
        )

    # Prefer canonical ai_db ping (keeps DSN normalization + SSL logic consistent)
    try:
        import inspect

        from utils_pkg import ai_db  # type: ignore

        # Pick the best ping-like function available
        candidates = ["ping_async", "ping", "healthcheck", "check"]
        fn = None
        fn_name = ""
        for nm in candidates:
            cand = getattr(ai_db, nm, None)
            if callable(cand):
                fn = cand
                fn_name = nm
                break

        if fn is None:
            raise RuntimeError("ai_db has no ping function")

        # only pass supported timeout args
        kw = {"timeout_s": cfg.dep_timeout_s, "timeout": cfg.dep_timeout_s}
        try:
            sig = inspect.signature(fn)
            allowed = {p.name for p in sig.parameters.values()}
            if not any(p.kind == inspect.Parameter.VAR_KEYWORD for p in sig.parameters.values()):
                kw = {k: v for k, v in kw.items() if k in allowed}
        except Exception:
            kw = {}

        v = await _call_maybe_async(fn, cfg.dep_timeout_s, **kw)  # type: ignore[arg-type]
        if isinstance(v, bool) and not v:
            raise RuntimeError(f"ai_db.{fn_name} returned False")

        return CheckResult(
            ok=True,
            status="ok",
            detail=f"ai_db.{fn_name}",
            latency_ms=_now_ms() - t0,
            ts_ms=_now_ms(),
        )
    except Exception as e:
        return CheckResult(
            ok=False,
            status="fail",
            detail=f"{type(e).__name__}: {e}",
            latency_ms=_now_ms() - t0,
            ts_ms=_now_ms(),
        )


async def _rpc_post_json(url: str, payload: dict[str, Any], timeout_s: float) -> dict[str, Any]:
    if httpx is not None and STATE.http_client is not None:
        r = await STATE.http_client.post(url, json=payload)
        if int(r.status_code) != 200:
            raise RuntimeError(f"http {r.status_code}")
        return cast(dict[str, Any], r.json())

    if httpx is not None:
        async with httpx.AsyncClient(timeout=timeout_s) as c:
            r = await c.post(url, json=payload)
            if int(r.status_code) != 200:
                raise RuntimeError(f"http {r.status_code}")
            return cast(dict[str, Any], r.json())

    # urllib fallback
    import json as _json
    import urllib.request

    def _do() -> dict[str, Any]:
        data = _json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(
            url, data=data, headers={"content-type": "application/json"}, method="POST"
        )
        with urllib.request.urlopen(req, timeout=timeout_s) as resp:
            body = resp.read().decode("utf-8", "ignore")
            return cast(dict[str, Any], _json.loads(body))

    return await asyncio.to_thread(_do)


async def _check_rpc(cfg: HealthConfig) -> CheckResult:
    t0 = _now_ms()
    if not cfg.check_rpc:
        return CheckResult(
            ok=True, status="skip", detail="disabled", latency_ms=_now_ms() - t0, ts_ms=_now_ms()
        )

    url = _resolve_rpc_url()
    if not url:
        ok = not cfg.require_rpc
        return CheckResult(
            ok=ok,
            status="skip" if ok else "fail",
            detail="no rpc url set",
            latency_ms=_now_ms() - t0,
            ts_ms=_now_ms(),
        )

    try:
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getLatestBlockhash",
            "params": [{"commitment": "processed"}],
        }
        data = await asyncio.wait_for(
            _rpc_post_json(url, payload, timeout_s=cfg.dep_timeout_s),
            timeout=max(1.0, cfg.dep_timeout_s),
        )
        bh = cast(dict[str, Any], data.get("result", {})).get("value", {}).get("blockhash")
        ok = bool(bh)
        return CheckResult(
            ok=ok,
            status="ok" if ok else "fail",
            detail="blockhash ok" if ok else "missing blockhash",
            latency_ms=_now_ms() - t0,
            ts_ms=_now_ms(),
        )
    except Exception as e:
        return CheckResult(
            ok=False,
            status="fail",
            detail=f"{type(e).__name__}: {e}",
            latency_ms=_now_ms() - t0,
            ts_ms=_now_ms(),
        )


async def _check_router(cfg: HealthConfig) -> CheckResult:
    t0 = _now_ms()
    if not cfg.check_router:
        return CheckResult(
            ok=True, status="skip", detail="disabled", latency_ms=_now_ms() - t0, ts_ms=_now_ms()
        )

    try:
        from utils_pkg import router as jrouter  # type: ignore

        for nm in ("get_quote", "quote", "get_buy_quote"):
            if callable(getattr(jrouter, nm, None)):
                return CheckResult(
                    ok=True,
                    status="ok",
                    detail=f"import ok ({nm})",
                    latency_ms=_now_ms() - t0,
                    ts_ms=_now_ms(),
                )

        fn = getattr(jrouter, "healthcheck", None)
        if callable(fn):
            v = await _call_maybe_async(fn, cfg.dep_timeout_s)
            ok = bool(v)
            return CheckResult(
                ok=ok,
                status="ok" if ok else "fail",
                detail="router.healthcheck",
                latency_ms=_now_ms() - t0,
                ts_ms=_now_ms(),
            )

        return CheckResult(
            ok=False,
            status="fail",
            detail="router missing quote fn",
            latency_ms=_now_ms() - t0,
            ts_ms=_now_ms(),
        )
    except Exception as e:
        return CheckResult(
            ok=False,
            status="fail",
            detail=f"{type(e).__name__}: {e}",
            latency_ms=_now_ms() - t0,
            ts_ms=_now_ms(),
        )


def _ready_from_checks(cfg: HealthConfig, checks: dict[str, CheckResult]) -> tuple[bool, str]:
    """
    Readiness rules:
      - If any REQUIRED check fails -> NOT ready.
      - Else if HEALTH_FAIL_ON_OPTIONAL=1 and any enabled check fails -> NOT ready.
      - Else ready (possibly degraded).
    """
    failed_required: list[str] = []
    if cfg.require_redis and not checks.get("redis", CheckResult(True, "skip")).ok:
        failed_required.append("redis")
    if cfg.require_pg and not checks.get("pg", CheckResult(True, "skip")).ok:
        failed_required.append("pg")
    if cfg.require_rpc and not checks.get("rpc", CheckResult(True, "skip")).ok:
        failed_required.append("rpc")

    if failed_required:
        return False, "missing: " + ",".join(failed_required)

    failed_enabled = [k for k, v in checks.items() if v.status == "fail"]
    if failed_enabled and cfg.fail_on_optional:
        return False, "failing: " + ",".join(failed_enabled)

    if failed_enabled:
        # Still READY, but degraded
        return True, "degraded: " + ",".join(failed_enabled)

    return True, "ok"


async def _collect_checks_cached(
    force_refresh: bool = False,
) -> tuple[HealthConfig, dict[str, CheckResult], bool, str]:
    cfg = load_health_config()
    sig = _cfg_signature(cfg)
    now = _now_ms()
    ttl_ms = int(cfg.cache_ttl_sec * 1000)

    async with STATE._lock:
        cache_valid = (
            (not force_refresh)
            and (STATE._cache_cfg_sig == sig)
            and bool(STATE._cache_checks)
            and (now - STATE._cache_ts_ms) < ttl_ms
        )
        if cache_valid:
            return cfg, STATE._cache_checks, STATE._cache_ready, STATE._cache_why

        sem = asyncio.Semaphore(cfg.max_parallel)

        async def _run(name: str, coro: Awaitable[CheckResult]) -> tuple[str, CheckResult]:
            async with sem:
                try:
                    res = await asyncio.wait_for(coro, timeout=max(1.0, cfg.dep_timeout_s))
                    return name, res
                except TimeoutError:
                    return name, CheckResult(
                        ok=False,
                        status="fail",
                        detail=f"timeout>{cfg.dep_timeout_s}s",
                        latency_ms=0,
                        ts_ms=_now_ms(),
                    )
                except Exception as e:
                    return name, CheckResult(
                        ok=False,
                        status="fail",
                        detail=f"{type(e).__name__}: {e}",
                        latency_ms=0,
                        ts_ms=_now_ms(),
                    )

        jobs: list[asyncio.Task[tuple[str, CheckResult]]] = []
        jobs.append(asyncio.create_task(_run("redis", _check_redis(cfg))))
        jobs.append(asyncio.create_task(_run("pg", _check_pg(cfg))))
        jobs.append(asyncio.create_task(_run("rpc", _check_rpc(cfg))))
        jobs.append(asyncio.create_task(_run("router", _check_router(cfg))))

        pairs = await asyncio.gather(*jobs)
        checks = {k: v for k, v in pairs}
        ready, why = _ready_from_checks(cfg, checks)

        STATE._cache_cfg_sig = sig
        STATE._cache_checks = checks
        STATE._cache_ready = bool(ready)
        STATE._cache_why = str(why)
        STATE._cache_ts_ms = _now_ms()

        return cfg, checks, bool(ready), str(why)


def _env_flags() -> dict[str, Any]:
    keys = [
        "DRYRUN",
        "PAUSE_TRADING",
        "SNIPER_ENABLED",
        "SNIPER_SHADOW_MODE",
        "DAYTRADER_ENABLED",
        "SCALPER_SHADOW_MODE",
        "WHALE_FEED_ENABLED",
        "WHALE_SHADOW_MODE",
        "VELOCITY_ENABLED",
        "VELOCITY_SHADOW_MODE",
        "HOTLIST_ENABLED",
        "HOTLIST_SHADOW_MODE",
        # health-related
        "HEALTHCHECK_SKIP_DB",
        "HEALTHCHECK_SKIP_REDIS",
        "HEALTHCHECK_SKIP_RPC",
        "HEALTH_FAIL_ON_OPTIONAL",
        "HEALTH_REQUIRE_PG",
        "HEALTH_REQUIRE_REDIS",
        "HEALTH_REQUIRE_RPC",
    ]
    out: dict[str, Any] = {}
    for k in keys:
        v = os.getenv(k)
        if v is not None:
            out[k] = v
    return out


def _build_meta() -> dict[str, Any]:
    return {
        "service": _envs("SERVICE_NAME", "sniper"),
        "version": _envs("APP_VERSION", _envs("GIT_SHA", "")) or "",
        "host": platform.node(),
        "pid": os.getpid(),
        "render": _render_detected(),
        "uptime_sec": int((_now_ms() - STATE.start_ts_ms) / 1000),
        "ts_utc": datetime.now(UTC).isoformat(),
    }


# ------------------------------------------------------------------------------
# FastAPI app
# ------------------------------------------------------------------------------
PREFIX = (_envs("HEALTH_PATH_PREFIX", "")).rstrip("/")
DISABLE_DOCS = _envb("HEALTH_DISABLE_DOCS", True)


def _path(p: str) -> str:
    return f"{PREFIX}{p}" if PREFIX else p


async def _lifespan(app_: FastAPI):
    # Shared http client
    if httpx is not None:
        with contextlib.suppress(Exception):
            timeout = float(load_health_config().dep_timeout_s)
            STATE.http_client = httpx.AsyncClient(timeout=timeout)
    log.info(
        "health_http startup (prefix=%s ttl=%s)", PREFIX or "/", load_health_config().cache_ttl_sec
    )
    try:
        yield
    finally:
        if STATE.http_client is not None:
            with contextlib.suppress(Exception):
                await STATE.http_client.aclose()
        STATE.http_client = None
        log.info("health_http shutdown")


app = FastAPI(
    title="sniper-health",
    version="1.0.0",
    docs_url=None if DISABLE_DOCS else "/docs",
    redoc_url=None if DISABLE_DOCS else "/redoc",
    lifespan=_lifespan,
)


@app.get(_path("/"))
async def root() -> RedirectResponse:
    return RedirectResponse(url=_path("/healthz"))


@app.get(_path("/livez"))
async def livez() -> JSONResponse:
    payload: dict[str, Any] = {"ok": True, "live": True, **_build_meta()}
    return JSONResponse(payload, status_code=200)


@app.get(_path("/readyz"))
async def readyz(
    response: Response,
    refresh: bool = Query(default=False, description="Bypass cache and recompute checks"),
) -> JSONResponse:
    cfg, checks, ready, why = await _collect_checks_cached(force_refresh=bool(refresh))

    payload: dict[str, Any] = {
        "ok": bool(ready),
        "ready": bool(ready),
        "why": str(why),
        **_build_meta(),
        "checks": {},
    }

    if cfg.verbose:
        payload["env"] = _env_flags()
        payload["resolved"] = {
            "rpc": _mask_url(_resolve_rpc_url() or ""),
            "redis": _mask_url(_resolve_redis_url() or ""),
            "pg": "set" if _resolve_pg_dsn() else "",
        }
        payload["checks"] = {k: v.full() for k, v in checks.items()}
    else:
        payload["checks"] = {k: v.compact() for k, v in checks.items()}

    response.status_code = 200 if ready else 503
    return JSONResponse(payload, status_code=response.status_code)


@app.get(_path("/healthz"))
async def healthz(response: Response, refresh: bool = Query(default=False)) -> JSONResponse:
    return await readyz(response=response, refresh=refresh)


# ------------------------------------------------------------------------------
# Entrypoint (uvicorn)
# ------------------------------------------------------------------------------
def main() -> None:
    import uvicorn

    host = _envs("HEALTH_HOST", "0.0.0.0")
    port = _envi("HEALTH_PORT", _envi("PORT", 8000))
    log_level = (_envs("HEALTH_LOG_LEVEL", "info") or "info").lower()

    uvicorn.run(
        "scripts.health_http:app",
        host=host,
        port=port,
        log_level=log_level,
        reload=False,
        access_log=True,
    )


if __name__ == "__main__":
    main()
