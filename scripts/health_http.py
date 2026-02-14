# scripts/health_http.py
from __future__ import annotations

"""
scripts/health_http.py — God-Tier Health HTTP (P0 LOCK-IN, stdlib-only)
======================================================================

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

Env
---
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

Notes
-----
- This file is the server.
- scripts/healthcheck.py is the client (deploy runner) that calls /readyz with retries.
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
from typing import Any, Awaitable, Callable, cast
from urllib.parse import parse_qs, urlparse

# ------------------------------------------------------------------------------
# Logging (prefer canonical logging_setup.get_logger; fallback safe)
# ------------------------------------------------------------------------------
try:
    from logging_setup import get_logger  # type: ignore
except Exception:  # pragma: no cover

    def get_logger(name: str | None = None):  # type: ignore
        logging.basicConfig(level=logging.INFO)
        return logging.getLogger(name or "health_http")


log = get_logger("health_http")


# ------------------------------------------------------------------------------
# Optional deps (best-effort). Zero are REQUIRED.
# ------------------------------------------------------------------------------
try:
    import httpx  # type: ignore
except Exception:  # pragma: no cover
    httpx = None

try:
    import redis.asyncio as aioredis  # type: ignore
except Exception:  # pragma: no cover
    aioredis = None


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


def _no_network() -> bool:
    # Canonical toggles used elsewhere in your system
    return _envb("NO_NETWORK", False) or _envb("NO_NET", False)


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
_START_TS_MS = _now_ms()


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
    return {
        "service": _envs("SERVICE_NAME", "sniper"),
        "version": _envs("APP_VERSION", _envs("GIT_SHA", "")) or "",
        "host": platform.node(),
        "pid": os.getpid(),
        "render": _render_detected(),
        "uptime_sec": int((_now_ms() - _START_TS_MS) / 1000),
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


async def _check_redis(cfg: HealthConfig) -> CheckResult:
    t0 = _now_ms()
    if not cfg.check_redis:
        return CheckResult(True, "skip", "disabled", _now_ms() - t0, _now_ms())

    if _no_network() and not cfg.require_redis:
        return CheckResult(
            True, "skip", "policy_skip:no_network_or_dryrun", _now_ms() - t0, _now_ms()
        )

    # Prefer canonical helper if present
    try:
        from utils_pkg.core_utils import get_redis_async as core_get_redis_async  # type: ignore

        r = await asyncio.wait_for(core_get_redis_async(), timeout=max(1.0, cfg.dep_timeout_s))
        pong = await asyncio.wait_for(r.ping(), timeout=max(1.0, cfg.dep_timeout_s))
        ok = bool(pong)
        return CheckResult(
            ok,
            "ok" if ok else "fail",
            "ping ok" if ok else "ping failed",
            _now_ms() - t0,
            _now_ms(),
        )
    except Exception:
        pass

    if aioredis is None:
        ok = not cfg.require_redis
        return CheckResult(
            ok, "skip" if ok else "fail", "redis client not installed", _now_ms() - t0, _now_ms()
        )

    url = _resolve_redis_url()
    if not url:
        ok = not cfg.require_redis
        return CheckResult(
            ok, "skip" if ok else "fail", "REDIS_URL missing", _now_ms() - t0, _now_ms()
        )

    try:
        r2 = aioredis.from_url(url, decode_responses=True)
        pong = await asyncio.wait_for(r2.ping(), timeout=max(1.0, cfg.dep_timeout_s))
        with contextlib.suppress(Exception):
            await r2.close()
        ok = bool(pong)
        return CheckResult(
            ok,
            "ok" if ok else "fail",
            "ping ok" if ok else "ping failed",
            _now_ms() - t0,
            _now_ms(),
        )
    except Exception as e:
        if _no_network() and not cfg.require_redis:
            return CheckResult(
                True, "skip", "policy_skip:no_network_or_dryrun", _now_ms() - t0, _now_ms()
            )
        return CheckResult(False, "fail", f"{type(e).__name__}: {e}", _now_ms() - t0, _now_ms())


async def _check_pg(cfg: HealthConfig) -> CheckResult:
    t0 = _now_ms()
    if not cfg.check_pg:
        return CheckResult(True, "skip", "disabled", _now_ms() - t0, _now_ms())

    if _no_network() and not cfg.require_pg:
        return CheckResult(
            True, "skip", "policy_skip:no_network_or_dryrun", _now_ms() - t0, _now_ms()
        )

    if not _resolve_pg_dsn():
        ok = not cfg.require_pg
        return CheckResult(ok, "skip" if ok else "fail", "no pg dsn set", _now_ms() - t0, _now_ms())

    # Prefer canonical ai_db ping (keeps DSN normalization + SSL logic consistent)
    try:
        import inspect

        from utils_pkg import ai_db  # type: ignore

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

        kw: dict[str, Any] = {"timeout_s": cfg.dep_timeout_s, "timeout": cfg.dep_timeout_s}
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

        return CheckResult(True, "ok", f"ai_db.{fn_name}", _now_ms() - t0, _now_ms())
    except Exception as e:
        if _no_network() and not cfg.require_pg:
            return CheckResult(
                True, "skip", "policy_skip:no_network_or_dryrun", _now_ms() - t0, _now_ms()
            )
        return CheckResult(False, "fail", f"{type(e).__name__}: {e}", _now_ms() - t0, _now_ms())


def _rpc_post_json_sync(url: str, payload: dict[str, Any], timeout_s: float) -> dict[str, Any]:
    if httpx is not None:
        r = httpx.post(url, json=payload, timeout=timeout_s)
        if int(r.status_code) != 200:
            raise RuntimeError(f"http {r.status_code}")
        out = r.json()
        if not isinstance(out, dict):
            raise RuntimeError("rpc_non_object_json")
        return cast(dict[str, Any], out)

    # urllib fallback
    import urllib.request

    data = json.dumps(payload).encode("utf-8")
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
    t0 = _now_ms()
    if not cfg.check_rpc:
        return CheckResult(True, "skip", "disabled", _now_ms() - t0, _now_ms())

    if _no_network() and not cfg.require_rpc:
        return CheckResult(
            True, "skip", "policy_skip:no_network_or_dryrun", _now_ms() - t0, _now_ms()
        )

    url = _resolve_rpc_url()
    if not url:
        ok = not cfg.require_rpc
        return CheckResult(
            ok, "skip" if ok else "fail", "no rpc url set", _now_ms() - t0, _now_ms()
        )

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
        return CheckResult(
            ok,
            "ok" if ok else "fail",
            "blockhash ok" if ok else "missing blockhash",
            _now_ms() - t0,
            _now_ms(),
        )
    except Exception as e:
        if _no_network() and not cfg.require_rpc:
            return CheckResult(
                True, "skip", "policy_skip:no_network_or_dryrun", _now_ms() - t0, _now_ms()
            )
        return CheckResult(False, "fail", f"{type(e).__name__}: {e}", _now_ms() - t0, _now_ms())


async def _check_router(cfg: HealthConfig) -> CheckResult:
    t0 = _now_ms()
    if not cfg.check_router:
        return CheckResult(True, "skip", "disabled", _now_ms() - t0, _now_ms())

    # Router check is import-only by design; no network required here.
    try:
        from utils_pkg import router as jrouter  # type: ignore

        for nm in ("get_quote", "quote", "get_buy_quote"):
            if callable(getattr(jrouter, nm, None)):
                return CheckResult(True, "ok", f"import ok ({nm})", _now_ms() - t0, _now_ms())

        fn = getattr(jrouter, "healthcheck", None)
        if callable(fn):
            v = await _call_maybe_async(fn, cfg.dep_timeout_s)
            ok = bool(v)
            return CheckResult(
                ok, "ok" if ok else "fail", "router.healthcheck", _now_ms() - t0, _now_ms()
            )

        return CheckResult(False, "fail", "router missing quote fn", _now_ms() - t0, _now_ms())
    except Exception as e:
        return CheckResult(False, "fail", f"{type(e).__name__}: {e}", _now_ms() - t0, _now_ms())


def _ready_from_checks(cfg: HealthConfig, checks: dict[str, CheckResult]) -> tuple[bool, str]:
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
                    False, "fail", f"timeout>{cfg.dep_timeout_s}s", 0, _now_ms()
                )
            except Exception as e:
                return name, CheckResult(False, "fail", f"{type(e).__name__}: {e}", 0, _now_ms())

    jobs: list[asyncio.Task[tuple[str, CheckResult]]] = [
        asyncio.create_task(_run("redis", _check_redis(cfg))),
        asyncio.create_task(_run("pg", _check_pg(cfg))),
        asyncio.create_task(_run("rpc", _check_rpc(cfg))),
        asyncio.create_task(_run("router", _check_router(cfg))),
    ]
    pairs = await asyncio.gather(*jobs)
    return {k: v for k, v in pairs}


def _run_async(coro: Awaitable[Any]) -> Any:
    try:
        return asyncio.run(coro)
    except RuntimeError:
        # Safety fallback (rare): create isolated loop
        loop = asyncio.new_event_loop()
        try:
            asyncio.set_event_loop(loop)
            return loop.run_until_complete(coro)
        finally:
            with contextlib.suppress(Exception):
                loop.close()
            asyncio.set_event_loop(None)


class _HealthCache:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._cond = threading.Condition(self._lock)

        self._computing = False

        self._cache_ts_ms = 0
        self._cache_cfg_sig = ""
        self._cache_checks: dict[str, CheckResult] = {}
        self._cache_ready = False
        self._cache_why = "init"

    def get(self, *, force_refresh: bool) -> tuple[HealthConfig, dict[str, CheckResult], bool, str]:
        cfg = load_health_config()
        sig = _cfg_signature(cfg)
        ttl_ms = int(cfg.cache_ttl_sec * 1000)
        now = _now_ms()

        with self._cond:
            cache_valid = (
                (not force_refresh)
                and (self._cache_cfg_sig == sig)
                and bool(self._cache_checks)
                and (now - self._cache_ts_ms) < ttl_ms
            )
            if cache_valid:
                return cfg, self._cache_checks, self._cache_ready, self._cache_why

            # Single-flight: only one thread computes; others wait.
            while self._computing:
                self._cond.wait(timeout=1.0)

                cache_valid2 = (
                    (not force_refresh)
                    and (self._cache_cfg_sig == sig)
                    and bool(self._cache_checks)
                    and (now - self._cache_ts_ms) < ttl_ms
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
            self._cache_ts_ms = _now_ms()
            self._computing = False
            self._cond.notify_all()

        return cfg, checks, bool(ready), str(why)


CACHE = _HealthCache()


# ------------------------------------------------------------------------------
# HTTP server
# ------------------------------------------------------------------------------
PREFIX = (_envs("HEALTH_PATH_PREFIX", "")).rstrip("/")
DISABLE_DOCS = _envb("HEALTH_DISABLE_DOCS", True)  # kept for compatibility (stdlib has no docs)


def _path(p: str) -> str:
    return f"{PREFIX}{p}" if PREFIX else p


def _bool_q(qs: dict[str, list[str]], key: str, default: bool = False) -> bool:
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


def _json_body(obj: dict[str, Any]) -> bytes:
    return json.dumps(obj, ensure_ascii=False, separators=(",", ":")).encode("utf-8")


class HealthHandler(BaseHTTPRequestHandler):
    server_version = "health_http/stdlib"
    protocol_version = "HTTP/1.1"

    def log_message(self, fmt: str, *args: object) -> None:
        # Quiet by default; enable with HEALTH_HTTP_LOG=1
        if _envb("HEALTH_HTTP_LOG", False):
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

            payload2: dict[str, Any] = {
                "ok": bool(ready),
                "ready": bool(ready),
                "why": str(why),
                **_build_meta(),
                "checks": {},
            }

            if cfg.verbose:
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

    log_level = (_envs("HEALTH_LOG_LEVEL", "info") or "info").lower()
    # best-effort set level if using std logging fallback
    with contextlib.suppress(Exception):
        if hasattr(log, "setLevel"):
            level = getattr(logging, log_level.upper(), logging.INFO)
            log.setLevel(level)

    log.info(
        "health_http bind host=%s port=%s prefix=%s ttl=%ss verbose=%s no_network=%s",
        host,
        port,
        PREFIX or "/",
        load_health_config().cache_ttl_sec,
        int(load_health_config().verbose),
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
