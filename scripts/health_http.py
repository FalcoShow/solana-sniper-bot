from __future__ import annotations

"""
scripts/health_http.py — God-Tier Health HTTP (P0 LOCK-IN, stdlib-only runtime)
==============================================================================

Purpose
-------
A tiny HTTP server suitable for Render health checks + human debugging:

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
- Render single-port contract:
    - PORT is authoritative and MUST WIN.
- Import-safe:
    - no third-party imports required; server only runs under __main__.

Extras (safe, optional)
-----------------------
- Per-request verbose override for debugging:
    - /readyz?verbose=1  (does NOT change default unless asked)
- Per-request refresh to bypass cache:
    - /readyz?refresh=1

Env (same contract as your previous version)
--------------------------------------------
Server:
  PORT is authoritative on Render (single-port contract).
  Optional overrides: HEALTH_HTTP_PORT, then HEALTH_PORT (legacy/local).
  Default: 8080 if Render-like, otherwise 8000.
  HEALTH_HOST=0.0.0.0
  HEALTH_LOG_LEVEL=info
  HEALTH_PATH_PREFIX=""          (e.g. "/api" to serve /api/healthz)
  HEALTH_DISABLE_DOCS=1          (kept for compatibility; stdlib server has no docs)

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

Policy:
  NO_NETWORK=1 => health checks return fast "policy_skip:no_network_or_dryrun"
                 (unless required toggles force them)

Redis:
  REDIS_URL / REDIS_TLS_URL / REDIS

Postgres (any one):
  SNIPER_DB_DSN / PG_DSN_EXTERNAL / POSTGRES_DSN / PG_DSN / PG_DSN_INTERNAL / DATABASE_URL

Solana RPC:
  SOLANA_RPC_URL / RPC_URL / SOLANA_RPC_HTTP / SOL_RPC_URL / RPC_URLS / SOL_RPC_URLS
"""

import argparse
import asyncio
import contextlib
import json
import logging
import os
import platform
import re
import threading
import time
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Awaitable, Callable, Mapping, cast
from urllib.parse import parse_qs, urlparse

# ------------------------------------------------------------------------------
# Logging (prefer canonical logging_setup.get_logger; fallback safe)
# ------------------------------------------------------------------------------
try:
    from logging_setup import get_logger  # type: ignore
except Exception:  # pragma: no cover

    def get_logger(name: str | None = None):  # type: ignore
        return logging.getLogger(name or "health_http")


# Keep logger type flexible (some repos use custom EventLogger types).
log: Any = get_logger("health_http")

# ------------------------------------------------------------------------------
# Optional deps (best-effort). Zero are REQUIRED.
# ------------------------------------------------------------------------------
try:
    import httpx  # type: ignore
except Exception:  # pragma: no cover
    httpx = None  # type: ignore

try:
    import redis.asyncio as aioredis  # type: ignore
except Exception:  # pragma: no cover
    aioredis = None  # type: ignore

# ------------------------------------------------------------------------------
# Config helpers
# ------------------------------------------------------------------------------
_TRUE = {"1", "true", "yes", "y", "on", "t"}
_FALSE = {"0", "false", "no", "n", "off", "f"}

_URL_CRED_RE = re.compile(r"(://)([^/@\s]+)@")


# Wall time (for timestamps) and monotonic time (for TTL/latency).
def _wall_ms() -> int:
    return int(time.time() * 1000)


def _mono_ms() -> int:
    return int(time.monotonic() * 1000)


def _envs(name: str, default: str = "") -> str:
    v = os.getenv(name)
    s = v if v is not None else default
    s = str(s).strip()
    if (s.startswith('"') and s.endswith('"')) or (s.startswith("'") and s.endswith("'")):
        s = s[1:-1].strip()
    return s


def _envb(name: str, default: bool = False) -> bool:
    raw = _envs(name, "")
    if raw == "":
        return bool(default)
    s = raw.lower().strip()
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


def _render_detected() -> bool:
    return bool(
        _envs("RENDER_SERVICE_ID", "")
        or _envs("RENDER_INSTANCE_ID", "")
        or _envs("RENDER", "").lower() in _TRUE
    )


def _no_network() -> bool:
    # Canonical toggles used elsewhere in your system
    return _envb("NO_NETWORK", False) or _envb("NO_NET", False) or _envb("DRYRUN", False)


def _resolve_health_port() -> int:
    """
    Port resolution order (Render-safe):
      1) PORT (Render single-port contract)  <-- MUST WIN
      2) HEALTH_HTTP_PORT (explicit override for this service)
      3) HEALTH_PORT (legacy/local override)
      4) default: 8080 if Render-like else 8000
    """
    default_port = 8080 if _render_detected() else 8000
    for key in ("PORT", "HEALTH_HTTP_PORT", "HEALTH_PORT"):
        raw = _envs(key, "")
        if raw:
            try:
                p = int(float(raw))
                if 1 <= p <= 65535:
                    return p
            except Exception:
                pass
    return default_port


def _mask_url(u: str) -> str:
    if not u:
        return ""
    out = _URL_CRED_RE.sub(r"\1***@", str(u))
    if "?" in out:
        out = out.split("?", 1)[0] + "?..."
    return out


def _skip_flag(name: str) -> bool:
    return _envb(name, False)


def _resolve_pg_dsn() -> str | None:
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

    # If a thing is skipped/disabled, it cannot be required
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
# Runtime meta + env flags
# ------------------------------------------------------------------------------
_START_WALL_MS = _wall_ms()
_START_MONO_MS = _mono_ms()


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
        "NO_NETWORK",
        "HEALTHCHECK_SKIP_DB",
        "HEALTHCHECK_SKIP_REDIS",
        "HEALTHCHECK_SKIP_RPC",
        "HEALTH_FAIL_ON_OPTIONAL",
        "HEALTH_REQUIRE_PG",
        "HEALTH_REQUIRE_REDIS",
        "HEALTH_REQUIRE_RPC",
        "HEALTH_CHECK_PG",
        "HEALTH_CHECK_REDIS",
        "HEALTH_CHECK_RPC",
        "HEALTH_CHECK_ROUTER",
        "HEALTH_PATH_PREFIX",
        "PORT",
        "HEALTH_HTTP_PORT",
        "HEALTH_PORT",
    ]
    out: dict[str, Any] = {}
    for k in keys:
        v = os.getenv(k)
        if v is not None:
            out[k] = v
    return out


def _build_meta() -> dict[str, Any]:
    uptime_sec = int((_mono_ms() - _START_MONO_MS) / 1000)
    return {
        "service": _envs("SERVICE_NAME", "sniper"),
        "version": _envs("APP_VERSION", _envs("GIT_SHA", "")) or "",
        "host": platform.node(),
        "pid": os.getpid(),
        "render": _render_detected(),
        "uptime_sec": max(0, uptime_sec),
        "ts_utc": datetime.now(UTC).isoformat(),
    }


# ------------------------------------------------------------------------------
# Async helpers + checks
# ------------------------------------------------------------------------------
async def _call_maybe_async(
    fn: Callable[..., Any],
    timeout_s: float,
    *args: Any,
    **kwargs: Any,
) -> Any:
    import inspect

    if inspect.iscoroutinefunction(fn):
        return await asyncio.wait_for(
            cast(Awaitable[Any], fn(*args, **kwargs)), timeout=max(1.0, timeout_s)
        )
    return await asyncio.wait_for(
        asyncio.to_thread(fn, *args, **kwargs), timeout=max(1.0, timeout_s)
    )


def _mk_res(ok: bool, status: str, detail: str, t0_mono_ms: int) -> CheckResult:
    return CheckResult(
        ok=bool(ok),
        status=str(status),
        detail=str(detail),
        latency_ms=max(0, _mono_ms() - t0_mono_ms),
        ts_ms=_wall_ms(),
    )


async def _check_redis(cfg: HealthConfig) -> CheckResult:
    t0 = _mono_ms()

    if not cfg.check_redis:
        return _mk_res(True, "skip", "disabled", t0)

    if _no_network() and not cfg.require_redis:
        return _mk_res(True, "skip", "policy_skip:no_network_or_dryrun", t0)

    # Prefer canonical helper if present
    try:
        from utils_pkg.core_utils import get_redis_async as core_get_redis_async  # type: ignore

        r = await asyncio.wait_for(core_get_redis_async(), timeout=max(1.0, cfg.dep_timeout_s))
        pong = await asyncio.wait_for(r.ping(), timeout=max(1.0, cfg.dep_timeout_s))
        ok = bool(pong)
        return _mk_res(ok, "ok" if ok else "fail", "ping ok" if ok else "ping failed", t0)
    except Exception:
        pass

    if aioredis is None:
        ok = not cfg.require_redis
        return _mk_res(ok, "skip" if ok else "fail", "redis client not installed", t0)

    url = _resolve_redis_url()
    if not url:
        ok = not cfg.require_redis
        return _mk_res(ok, "skip" if ok else "fail", "REDIS_URL missing", t0)

    try:
        r2 = aioredis.from_url(url, decode_responses=True)  # type: ignore[union-attr]
        pong2 = await asyncio.wait_for(r2.ping(), timeout=max(1.0, cfg.dep_timeout_s))
        with contextlib.suppress(Exception):
            await r2.close()
        ok = bool(pong2)
        return _mk_res(ok, "ok" if ok else "fail", "ping ok" if ok else "ping failed", t0)
    except Exception as e:
        if _no_network() and not cfg.require_redis:
            return _mk_res(True, "skip", "policy_skip:no_network_or_dryrun", t0)
        return _mk_res(False, "fail", f"{type(e).__name__}: {e}", t0)


async def _check_pg(cfg: HealthConfig) -> CheckResult:
    t0 = _mono_ms()

    if not cfg.check_pg:
        return _mk_res(True, "skip", "disabled", t0)

    if _no_network() and not cfg.require_pg:
        return _mk_res(True, "skip", "policy_skip:no_network_or_dryrun", t0)

    if not _resolve_pg_dsn():
        ok = not cfg.require_pg
        return _mk_res(ok, "skip" if ok else "fail", "no pg dsn set", t0)

    # Prefer canonical ai_db ping (keeps DSN normalization + SSL logic consistent)
    try:
        import inspect

        from utils_pkg import ai_db  # type: ignore

        candidates = ("ping_async", "ping", "healthcheck", "check")
        fn: Callable[..., Any] | None = None
        fn_name = ""
        for nm in candidates:
            cand = getattr(ai_db, nm, None)
            if callable(cand):
                fn = cast(Callable[..., Any], cand)
                fn_name = nm
                break
        if fn is None:
            raise RuntimeError("ai_db has no ping function")

        kw: dict[str, Any] = {"timeout_s": cfg.dep_timeout_s, "timeout": cfg.dep_timeout_s}
        try:
            sig = inspect.signature(fn)
            allowed = {p.name for p in sig.parameters.values()}
            if not any(p.kind == inspect.Parameter.VAR_KEYWORD for p in sig.parameters.values()):
                kw = {k: v for k, v in kw.items() if k in allowed}
        except Exception:
            kw = {}

        v = await _call_maybe_async(fn, cfg.dep_timeout_s, **kw)
        if isinstance(v, bool) and not v:
            raise RuntimeError(f"ai_db.{fn_name} returned False")

        return _mk_res(True, "ok", f"ai_db.{fn_name}", t0)
    except Exception as e:
        if _no_network() and not cfg.require_pg:
            return _mk_res(True, "skip", "policy_skip:no_network_or_dryrun", t0)
        return _mk_res(False, "fail", f"{type(e).__name__}: {e}", t0)


def _rpc_post_json_sync(url: str, payload: Mapping[str, Any], timeout_s: float) -> dict[str, Any]:
    # httpx fast path if installed
    if httpx is not None:
        r = httpx.post(url, json=dict(payload), timeout=timeout_s)  # type: ignore[union-attr]
        if int(getattr(r, "status_code", 0)) != 200:
            raise RuntimeError(f"http {getattr(r, 'status_code', 0)}")
        out = r.json()
        if not isinstance(out, dict):
            raise RuntimeError("rpc_non_object_json")
        return cast(dict[str, Any], out)

    # urllib fallback (stdlib-only)
    import urllib.request

    data = json.dumps(dict(payload)).encode("utf-8")
    req = urllib.request.Request(
        url, data=data, headers={"content-type": "application/json"}, method="POST"
    )
    with urllib.request.urlopen(req, timeout=timeout_s) as resp:
        body = resp.read().decode("utf-8", "ignore")
        out2 = json.loads(body)
        if not isinstance(out2, dict):
            raise RuntimeError("rpc_non_object_json")
        return cast(dict[str, Any], out2)


async def _check_rpc(cfg: HealthConfig) -> CheckResult:
    t0 = _mono_ms()

    if not cfg.check_rpc:
        return _mk_res(True, "skip", "disabled", t0)

    if _no_network() and not cfg.require_rpc:
        return _mk_res(True, "skip", "policy_skip:no_network_or_dryrun", t0)

    url = _resolve_rpc_url()
    if not url:
        ok = not cfg.require_rpc
        return _mk_res(ok, "skip" if ok else "fail", "no rpc url set", t0)

    try:
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getLatestBlockhash",
            "params": [{"commitment": "processed"}],
        }
        data = await asyncio.wait_for(
            asyncio.to_thread(_rpc_post_json_sync, url, payload, float(cfg.dep_timeout_s)),
            timeout=max(1.0, cfg.dep_timeout_s),
        )
        bh = cast(dict[str, Any], data.get("result", {})).get("value", {}).get("blockhash")
        ok = bool(bh)
        return _mk_res(
            ok, "ok" if ok else "fail", "blockhash ok" if ok else "missing blockhash", t0
        )
    except Exception as e:
        if _no_network() and not cfg.require_rpc:
            return _mk_res(True, "skip", "policy_skip:no_network_or_dryrun", t0)
        return _mk_res(False, "fail", f"{type(e).__name__}: {e}", t0)


async def _check_router(cfg: HealthConfig) -> CheckResult:
    t0 = _mono_ms()

    if not cfg.check_router:
        return _mk_res(True, "skip", "disabled", t0)

    # Router check is import-only by design; no network required here.
    try:
        from utils_pkg import router as jrouter  # type: ignore

        for nm in ("get_quote", "quote", "get_buy_quote"):
            if callable(getattr(jrouter, nm, None)):
                return _mk_res(True, "ok", f"import ok ({nm})", t0)

        fn = getattr(jrouter, "healthcheck", None)
        if callable(fn):
            v = await _call_maybe_async(cast(Callable[..., Any], fn), cfg.dep_timeout_s)
            ok = bool(v)
            return _mk_res(ok, "ok" if ok else "fail", "router.healthcheck", t0)

        return _mk_res(False, "fail", "router missing quote fn", t0)
    except Exception as e:
        return _mk_res(False, "fail", f"{type(e).__name__}: {e}", t0)


def _ready_from_checks(cfg: HealthConfig, checks: Mapping[str, CheckResult]) -> tuple[bool, str]:
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
        return True, "degraded: " + ",".join(failed_enabled)

    return True, "ok"


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


async def _collect_checks(cfg: HealthConfig) -> dict[str, CheckResult]:
    sem = asyncio.Semaphore(cfg.max_parallel)

    async def _run(name: str, coro: Awaitable[CheckResult]) -> tuple[str, CheckResult]:
        async with sem:
            try:
                res = await asyncio.wait_for(coro, timeout=max(1.0, cfg.dep_timeout_s))
                return name, res
            except TimeoutError:
                return name, CheckResult(
                    False, "fail", f"timeout>{cfg.dep_timeout_s}s", 0, _wall_ms()
                )
            except Exception as e:
                return name, CheckResult(False, "fail", f"{type(e).__name__}: {e}", 0, _wall_ms())

    jobs: list[asyncio.Task[tuple[str, CheckResult]]] = [
        asyncio.create_task(_run("redis", _check_redis(cfg))),
        asyncio.create_task(_run("pg", _check_pg(cfg))),
        asyncio.create_task(_run("rpc", _check_rpc(cfg))),
        asyncio.create_task(_run("router", _check_router(cfg))),
    ]
    pairs = await asyncio.gather(*jobs)
    return {k: v for k, v in pairs}


def _run_async(coro: Awaitable[Any]) -> Any:
    """
    Run coroutine in a safe, isolated way.
    - In normal usage (no running loop in this thread), asyncio.run is perfect.
    - If a loop is already running in *this* thread (rare for ThreadingHTTPServer),
      fall back to a fresh loop to avoid crashing readiness.
    """
    try:
        return asyncio.run(coro)
    except RuntimeError:
        loop = asyncio.new_event_loop()
        try:
            asyncio.set_event_loop(loop)
            return loop.run_until_complete(coro)
        finally:
            with contextlib.suppress(Exception):
                loop.close()
            asyncio.set_event_loop(None)


class _HealthCache:
    """
    Single-flight cached readiness results.
    Uses monotonic time for TTL correctness even if wall clock shifts.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._cond = threading.Condition(self._lock)

        self._computing = False

        self._cache_mono_ms = 0
        self._cache_cfg_sig = ""
        self._cache_checks: dict[str, CheckResult] = {}
        self._cache_ready = False
        self._cache_why = "init"

    def get(self, *, force_refresh: bool) -> tuple[HealthConfig, dict[str, CheckResult], bool, str]:
        cfg = load_health_config()
        sig = _cfg_signature(cfg)
        ttl_ms = int(cfg.cache_ttl_sec * 1000)
        now_m = _mono_ms()

        with self._cond:
            cache_valid = (
                (not force_refresh)
                and (self._cache_cfg_sig == sig)
                and bool(self._cache_checks)
                and (now_m - self._cache_mono_ms) < ttl_ms
            )
            if cache_valid:
                return cfg, self._cache_checks, self._cache_ready, self._cache_why

            # Single-flight: only one thread computes; others wait.
            while self._computing:
                self._cond.wait(timeout=1.0)

                now_m2 = _mono_ms()
                cache_valid2 = (
                    (not force_refresh)
                    and (self._cache_cfg_sig == sig)
                    and bool(self._cache_checks)
                    and (now_m2 - self._cache_mono_ms) < ttl_ms
                )
                if cache_valid2:
                    return cfg, self._cache_checks, self._cache_ready, self._cache_why

            self._computing = True

        # Compute outside lock
        checks = cast(dict[str, CheckResult], _run_async(_collect_checks(cfg)))
        ready, why = _ready_from_checks(cfg, checks)

        with self._cond:
            self._cache_cfg_sig = sig
            self._cache_checks = checks
            self._cache_ready = bool(ready)
            self._cache_why = str(why)
            self._cache_mono_ms = _mono_ms()
            self._computing = False
            self._cond.notify_all()

        return cfg, checks, bool(ready), str(why)


CACHE = _HealthCache()

# ------------------------------------------------------------------------------
# HTTP server
# ------------------------------------------------------------------------------
PREFIX = (_envs("HEALTH_PATH_PREFIX", "")).rstrip("/")
DISABLE_DOCS = _envb("HEALTH_DISABLE_DOCS", True)  # kept for compatibility


def _path(p: str) -> str:
    return f"{PREFIX}{p}" if PREFIX else p


def _bool_q(qs: Mapping[str, list[str]], key: str, default: bool = False) -> bool:
    if key not in qs:
        return default
    raw = (qs.get(key) or [""])[0].strip().lower()
    if raw == "":
        return True
    if raw in _TRUE:
        return True
    if raw in _FALSE:
        return False
    return default


def _json_body(obj: Mapping[str, Any]) -> bytes:
    return json.dumps(obj, ensure_ascii=False, separators=(",", ":")).encode("utf-8")


class HealthHandler(BaseHTTPRequestHandler):
    server_version = "health_http/stdlib"
    protocol_version = "HTTP/1.1"

    def log_message(self, fmt: str, *args: object) -> None:
        # Quiet by default; enable with HEALTH_HTTP_LOG=1
        if _envb("HEALTH_HTTP_LOG", False):
            with contextlib.suppress(Exception):
                log.info(fmt, *args)

    def _send(
        self, status: int, body: bytes, content_type: str = "application/json; charset=utf-8"
    ) -> None:
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        if self.command != "HEAD":
            with contextlib.suppress(BrokenPipeError, ConnectionResetError):
                self.wfile.write(body)

    def _redirect(self, location: str) -> None:
        self.send_response(307)
        self.send_header("Location", location)
        self.send_header("Content-Length", "0")
        self.end_headers()

    def do_HEAD(self) -> None:  # noqa: N802
        self._handle()

    def do_GET(self) -> None:  # noqa: N802
        self._handle()

    def _handle(self) -> None:
        parsed = urlparse(self.path)
        path = parsed.path or "/"
        qs = parse_qs(parsed.query or "")

        livez_p = _path("/livez")
        readyz_p = _path("/readyz")
        healthz_p = _path("/healthz")
        root_p = _path("/")

        # Root redirect (also accept plain "/" even when prefix is set)
        if path in {"/", root_p}:
            self._redirect(healthz_p)
            return

        if path == livez_p:
            payload: dict[str, Any] = {"ok": True, "live": True, **_build_meta()}
            self._send(200, _json_body(payload))
            return

        if path in {readyz_p, healthz_p}:
            refresh = _bool_q(qs, "refresh", False)
            cfg, checks, ready, why = CACHE.get(force_refresh=bool(refresh))

            req_verbose = (
                bool(cfg.verbose) or _bool_q(qs, "verbose", False) or _bool_q(qs, "v", False)
            )

            payload2: dict[str, Any] = {
                "ok": bool(ready),
                "ready": bool(ready),
                "why": str(why),
                **_build_meta(),
                "checks": {},
            }

            if req_verbose:
                payload2["env"] = _env_flags()
                payload2["resolved"] = {
                    "rpc": _mask_url(_resolve_rpc_url() or ""),
                    "redis": _mask_url(_resolve_redis_url() or ""),
                    "pg": "set" if _resolve_pg_dsn() else "",
                    "no_network": bool(_no_network()),
                    "docs_disabled": bool(DISABLE_DOCS),
                }
                payload2["checks"] = {k: v.full() for k, v in checks.items()}
            else:
                payload2["checks"] = {k: v.compact() for k, v in checks.items()}

            self._send(200 if ready else 503, _json_body(payload2))
            return

        # Not found
        self._send(
            404, _json_body({"ok": False, "error": "not_found", "path": path, **_build_meta()})
        )


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(add_help=True)
    p.add_argument("--host", default=_envs("HEALTH_HOST", "0.0.0.0"))
    p.add_argument("--port", type=int, default=_resolve_health_port())
    return p.parse_args()


def main() -> None:
    args = _parse_args()

    host = str(args.host)
    port = int(args.port)

    # If nothing configured logging yet, set a sane baseline.
    if not logging.getLogger().handlers:
        logging.basicConfig(level=logging.INFO)

    log_level = (_envs("HEALTH_LOG_LEVEL", "info") or "info").lower()
    with contextlib.suppress(Exception):
        if hasattr(log, "setLevel"):
            level = getattr(logging, log_level.upper(), logging.INFO)
            log.setLevel(level)

    cfg = load_health_config()
    with contextlib.suppress(Exception):
        log.info(
            "health_http bind host=%s port=%s prefix=%s ttl=%ss verbose=%s no_network=%s",
            host,
            port,
            PREFIX or "/",
            cfg.cache_ttl_sec,
            int(cfg.verbose),
            int(_no_network()),
        )

    ThreadingHTTPServer.allow_reuse_address = True
    httpd = ThreadingHTTPServer((host, port), HealthHandler)
    try:
        httpd.serve_forever(poll_interval=0.5)
    except KeyboardInterrupt:
        pass
    finally:
        with contextlib.suppress(Exception):
            httpd.server_close()


if __name__ == "__main__":
    main()
