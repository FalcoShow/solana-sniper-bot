# scripts/health_http.py
from __future__ import annotations

"""
scripts/health_http.py — God-Tier Health HTTP (P0 LOCK-IN)
==========================================================

Purpose
-------
A tiny FastAPI service suitable for Render health checks + human debugging:

  GET /livez   -> process is up (always 200 if server running)
  GET /readyz  -> dependencies OK (redis/pg/rpc optional) else 503
  GET /healthz -> alias of /readyz
  GET /        -> redirect to /healthz

Design goals
-----------
- Never blocks the event loop on every request (checks are cached with TTL).
- Optional dependency checks (enable/require via env toggles).
- Robust imports: if optional libs are missing, check is "skipped" unless required.
- Secret-safe output: no DSNs/tokens/keys emitted; URLs are masked.

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

Dependency toggles:
  HEALTH_CHECK_REDIS=1
  HEALTH_CHECK_PG=1
  HEALTH_CHECK_RPC=0
  HEALTH_CHECK_ROUTER=0          (best-effort import utils_pkg.router)

Require toggles (fail readiness if required check fails):
  HEALTH_REQUIRE_REDIS=0
  HEALTH_REQUIRE_PG=0
  HEALTH_REQUIRE_RPC=0

Redis:
  REDIS_URL / REDIS_TLS_URL

Postgres (any one):
  POSTGRES_DSN / PG_DSN / PG_DSN_INTERNAL / PG_DSN_EXTERNAL / DATABASE_URL

Solana RPC:
  SOLANA_RPC_URL / RPC_URL / SOLANA_RPC_HTTP / SOL_RPC_URL

Notes
-----
- This file is the *server*.
- scripts/healthcheck.py is the *client* (deploy runner) that calls /readyz with retries.
"""

import asyncio
import contextlib
import os
import platform
import re
import time
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from typing import Any

from fastapi import FastAPI, Query, Response
from fastapi.responses import JSONResponse, RedirectResponse

# ------------------------------------------------------------------------------
# Logging (prefer canonical root logging_setup.get_logger)
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

# NOTE: Prefer ai_db ping instead of direct asyncpg in prod;
# we keep asyncpg as *optional* fallback only.
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


def _resolve_pg_dsn() -> str | None:
    for k in ("POSTGRES_DSN", "PG_DSN", "PG_DSN_INTERNAL", "PG_DSN_EXTERNAL", "DATABASE_URL"):
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
            # if it's a csv list, take first
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


def load_health_config() -> HealthConfig:
    return HealthConfig(
        cache_ttl_sec=max(1.0, _envf("HEALTH_CACHE_TTL_SEC", 5.0)),
        verbose=_envb("HEALTH_VERBOSE", False),
        max_parallel=max(1, _envi("HEALTH_MAX_PARALLEL", 6)),
        dep_timeout_s=max(1.0, _envf("HEALTH_DEP_TIMEOUT_S", 6.5)),
        check_redis=_envb("HEALTH_CHECK_REDIS", True),
        check_pg=_envb("HEALTH_CHECK_PG", True),
        check_rpc=_envb("HEALTH_CHECK_RPC", False),
        check_router=_envb("HEALTH_CHECK_ROUTER", False),
        require_redis=_envb("HEALTH_REQUIRE_REDIS", False),
        require_pg=_envb("HEALTH_REQUIRE_PG", False),
        require_rpc=_envb("HEALTH_REQUIRE_RPC", False),
    )


# ------------------------------------------------------------------------------
# State + caching (single-flight)
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
    # Minimal signature so cache invalidates if behavior toggles change.
    return "|".join(
        [
            f"ttl={cfg.cache_ttl_sec}",
            f"verb={int(cfg.verbose)}",
            f"mp={cfg.max_parallel}",
            f"to={cfg.dep_timeout_s}",
            f"cr={int(cfg.check_redis)}:{int(cfg.require_redis)}",
            f"pg={int(cfg.check_pg)}:{int(cfg.require_pg)}",
            f"rp={int(cfg.check_rpc)}:{int(cfg.require_rpc)}",
            f"rt={int(cfg.check_router)}",
        ]
    )


# ------------------------------------------------------------------------------
# Dependency checks
# ------------------------------------------------------------------------------
async def _check_redis(cfg: HealthConfig) -> CheckResult:
    t0 = _now_ms()
    if not cfg.check_redis:
        return CheckResult(ok=True, status="skip", detail="disabled", latency_ms=0, ts_ms=_now_ms())

    url = _resolve_redis_url()

    # Prefer core_utils (may already manage connection + TLS nuances)
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
        # fall through to direct redis if available
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
        return CheckResult(ok=True, status="skip", detail="disabled", latency_ms=0, ts_ms=_now_ms())

    # Prefer canonical ai_db ping (keeps DSN normalization + SSL logic consistent)
    try:
        import inspect

        from utils_pkg import ai_db  # type: ignore

        for nm in ("ping_async", "ping", "healthcheck", "check"):
            fn = getattr(ai_db, nm, None)
            if not callable(fn):
                continue
            # signature-tolerant timeout args
            kw = {"timeout_s": cfg.dep_timeout_s, "timeout": cfg.dep_timeout_s}
            try:
                sig = inspect.signature(fn)
                allowed = {p.name for p in sig.parameters.values()}
                if not any(
                    p.kind == inspect.Parameter.VAR_KEYWORD for p in sig.parameters.values()
                ):
                    kw = {k: v for k, v in kw.items() if k in allowed}
            except Exception:
                pass

            v = await asyncio.wait_for(fn(**kw), timeout=max(1.0, cfg.dep_timeout_s))  # type: ignore
            if isinstance(v, bool) and not v:
                raise RuntimeError(f"{nm} returned False")
            return CheckResult(
                ok=True,
                status="ok",
                detail=f"ai_db.{nm}",
                latency_ms=_now_ms() - t0,
                ts_ms=_now_ms(),
            )
    except Exception as e:
        # If ai_db exists but failed, count as failure (this is the canonical baselayer).
        return CheckResult(
            ok=False,
            status="fail",
            detail=f"{type(e).__name__}: {e}",
            latency_ms=_now_ms() - t0,
            ts_ms=_now_ms(),
        )

    # If ai_db import is missing (should not happen in P0), fallback only if asyncpg exists.
    dsn = _resolve_pg_dsn()
    if not dsn:
        ok = not cfg.require_pg
        return CheckResult(
            ok=ok,
            status="skip" if ok else "fail",
            detail="no pg dsn set",
            latency_ms=_now_ms() - t0,
            ts_ms=_now_ms(),
        )

    if asyncpg is None:
        ok = not cfg.require_pg
        return CheckResult(
            ok=ok,
            status="skip" if ok else "fail",
            detail="asyncpg not installed",
            latency_ms=_now_ms() - t0,
            ts_ms=_now_ms(),
        )

    conn = None
    try:
        try:
            conn = await asyncio.wait_for(
                asyncpg.connect(dsn, ssl="require", timeout=cfg.dep_timeout_s),
                timeout=max(1.0, cfg.dep_timeout_s),
            )
        except Exception:
            conn = await asyncio.wait_for(
                asyncpg.connect(dsn, timeout=cfg.dep_timeout_s), timeout=max(1.0, cfg.dep_timeout_s)
            )
        v = await asyncio.wait_for(conn.fetchval("SELECT 1;"), timeout=max(1.0, cfg.dep_timeout_s))
        ok = v == 1
        return CheckResult(
            ok=ok,
            status="ok" if ok else "fail",
            detail="select 1 ok" if ok else "select 1 bad",
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
    finally:
        with contextlib.suppress(Exception):
            if conn is not None:
                await conn.close()


async def _rpc_post_json(url: str, payload: dict[str, Any], timeout_s: float) -> dict[str, Any]:
    if httpx is not None and STATE.http_client is not None:
        r = await STATE.http_client.post(url, json=payload)
        if int(r.status_code) != 200:
            raise RuntimeError(f"http {r.status_code}")
        return r.json()

    if httpx is not None:
        async with httpx.AsyncClient(timeout=timeout_s) as c:
            r = await c.post(url, json=payload)
            if int(r.status_code) != 200:
                raise RuntimeError(f"http {r.status_code}")
            return r.json()

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
            return _json.loads(body)

    return await asyncio.to_thread(_do)


async def _check_rpc(cfg: HealthConfig) -> CheckResult:
    t0 = _now_ms()
    if not cfg.check_rpc:
        return CheckResult(ok=True, status="skip", detail="disabled", latency_ms=0, ts_ms=_now_ms())

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
        # Prefer getLatestBlockhash (more universally supported) with processed commitment.
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
        bh = data.get("result", {}).get("value", {}).get("blockhash")
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
        return CheckResult(ok=True, status="skip", detail="disabled", latency_ms=0, ts_ms=_now_ms())

    try:
        from utils_pkg import router as jrouter  # type: ignore

        # Extremely light: just verify import + expected quote function exists
        for nm in ("get_quote", "quote", "get_buy_quote"):
            if callable(getattr(jrouter, nm, None)):
                return CheckResult(
                    ok=True,
                    status="ok",
                    detail=f"import ok ({nm})",
                    latency_ms=_now_ms() - t0,
                    ts_ms=_now_ms(),
                )

        # If router exposes a cheap healthcheck, accept it (but don't require it)
        fn = getattr(jrouter, "healthcheck", None)
        if callable(fn):
            v = await asyncio.wait_for(fn(), timeout=max(1.0, cfg.dep_timeout_s))  # type: ignore
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
    # Required checks first
    missing: list[str] = []
    if cfg.require_redis and not checks.get("redis", CheckResult(True, "skip")).ok:
        missing.append("redis")
    if cfg.require_pg and not checks.get("pg", CheckResult(True, "skip")).ok:
        missing.append("pg")
    if cfg.require_rpc and not checks.get("rpc", CheckResult(True, "skip")).ok:
        missing.append("rpc")

    if missing:
        return False, "missing: " + ",".join(missing)

    # If nothing required, we still consider it not-ready if any enabled check is failing.
    enabled_fails = [k for k, v in checks.items() if v.status == "fail"]
    if enabled_fails:
        return False, "failing: " + ",".join(enabled_fails)

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

        # Build task list based on enabled checks
        sem = asyncio.Semaphore(cfg.max_parallel)

        async def _run(name: str, coro) -> tuple[str, CheckResult]:
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

        jobs = []
        if cfg.check_redis:
            jobs.append(asyncio.create_task(_run("redis", _check_redis(cfg))))
        else:
            jobs.append(asyncio.create_task(_run("redis", _check_redis(cfg))))  # returns skip

        if cfg.check_pg:
            jobs.append(asyncio.create_task(_run("pg", _check_pg(cfg))))
        else:
            jobs.append(asyncio.create_task(_run("pg", _check_pg(cfg))))  # returns skip

        if cfg.check_rpc:
            jobs.append(asyncio.create_task(_run("rpc", _check_rpc(cfg))))
        else:
            jobs.append(asyncio.create_task(_run("rpc", _check_rpc(cfg))))  # returns skip

        if cfg.check_router:
            jobs.append(asyncio.create_task(_run("router", _check_router(cfg))))
        else:
            jobs.append(asyncio.create_task(_run("router", _check_router(cfg))))  # returns skip

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
    # Small & non-sensitive flags that explain “why not trading”
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

app = FastAPI(
    title="sniper-health",
    version="1.0.0",
    docs_url=None if DISABLE_DOCS else "/docs",
    redoc_url=None if DISABLE_DOCS else "/redoc",
)


def _path(p: str) -> str:
    return f"{PREFIX}{p}" if PREFIX else p


@app.on_event("startup")
async def _startup() -> None:
    # Create a shared HTTP client if httpx exists (reduces overhead)
    if httpx is not None:
        with contextlib.suppress(Exception):
            timeout = float(load_health_config().dep_timeout_s)
            STATE.http_client = httpx.AsyncClient(timeout=timeout)
    log.info(
        "health_http startup (prefix=%s ttl=%s)", PREFIX or "/", load_health_config().cache_ttl_sec
    )


@app.on_event("shutdown")
async def _shutdown() -> None:
    if STATE.http_client is not None:
        with contextlib.suppress(Exception):
            await STATE.http_client.aclose()
    STATE.http_client = None
    log.info("health_http shutdown")


@app.get(_path("/"))
async def root() -> RedirectResponse:
    return RedirectResponse(url=_path("/healthz"))


@app.get(_path("/livez"))
async def livez() -> JSONResponse:
    # Liveness: the process is alive if it can answer HTTP.
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
            "pg": "***" if _resolve_pg_dsn() else "",
        }
        payload["checks"] = {k: v.full() for k, v in checks.items()}
    else:
        payload["checks"] = {k: v.compact() for k, v in checks.items()}

    response.status_code = 200 if ready else 503
    return JSONResponse(payload, status_code=response.status_code)


@app.get(_path("/healthz"))
async def healthz(response: Response, refresh: bool = Query(default=False)) -> JSONResponse:
    # Healthz mirrors readiness (most platforms use this)
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
