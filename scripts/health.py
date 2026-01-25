# scripts/health.py
from __future__ import annotations

"""
scripts/health.py — Canonical Health Server (P0 LOCK-IN)
========================================================

Single HTTP health surface for Render + local.

Endpoints
---------
GET /healthz   -> liveness (FAST; always 200 if process is alive)
GET /livez     -> alias for liveness
GET /readyz    -> readiness (cached probes via scripts.healthcheck local-mode)
GET /stats     -> lightweight runtime snapshot (NO secrets)
GET /version   -> version/build info (safe)
GET /metrics   -> minimal Prometheus-ish text (no deps)

Readiness design
----------------
- /readyz runs scripts.healthcheck.run_local_healthcheck(...) when available.
- Results are cached in-memory (per "request profile") for a TTL.
- Profiles prevent false negatives in dev:
    profile=core (default): env + rpc_select + core_import + router
    profile=full: let healthcheck decide defaults (includes solana/redis/db when enabled)
- Strictness is controllable:
    strict=1 forces fail-fast semantics as implemented by healthcheck.

Framework
---------
Auto-select:
  - FastAPI + uvicorn if available (preferred)
  - else aiohttp.web fallback

Env (server)
------------
HOST=0.0.0.0
PORT=8080
HEALTH_SERVER_FRAMEWORK=auto|fastapi|aiohttp
UVICORN_LOG_LEVEL=info
UVICORN_ACCESS_LOG=1

Env (readiness)
---------------
HEALTH_SERVER_READY_ENABLED=1
HEALTH_SERVER_READY_CACHE_TTL_S=2.0
HEALTH_SERVER_READY_STRICT=0
HEALTH_SERVER_READY_TIMEOUT_S=14
HEALTH_SERVER_READY_PROBE_TIMEOUT_S=6
HEALTH_SERVER_READY_MAX_PARALLEL=6
HEALTH_SERVER_READY_PROFILE=core|full     # default core
HEALTH_SERVER_READY_PROBES=               # csv keys to run ONLY (overrides profile)
HEALTH_SERVER_READY_SKIP=                 # csv keys to skip (applies after probes/profile)

Safety / Redaction
------------------
- Secret values are never returned.
- Only an allow-list of env flags is surfaced.
- Payloads are JSON-serializable; non-serializable values are stringified.

Run
---
python -m scripts.health
# or (FastAPI only):
# uvicorn scripts.health:app --host 0.0.0.0 --port 8080
"""

import asyncio
import contextlib
import json
import os
import platform
import re
import time
from dataclasses import dataclass
from typing import Any, Tuple


# =============================================================================
# logger (canonical: logging_setup.get_logger)
# =============================================================================
def _get_logger():
    with contextlib.suppress(Exception):
        from logging_setup import get_logger  # type: ignore

        return get_logger()
    import logging

    logging.basicConfig(level=logging.INFO)
    return logging.getLogger("health")


log = _get_logger()

# =============================================================================
# env helpers / safety
# =============================================================================
_TRUE = {"1", "true", "yes", "y", "on", "t"}
_FALSE = {"0", "false", "no", "n", "off", "f"}

_URL_CRED_RE = re.compile(r"(://)([^/@\s]+)@")
_LONG_TOKENISH_RE = re.compile(r"\b[A-Za-z0-9_\-]{28,}\b")


def _envs(k: str, d: str = "") -> str:
    v = os.getenv(k)
    s = v if v is not None else d
    s = str(s).strip()
    if (s.startswith('"') and s.endswith('"')) or (s.startswith("'") and s.endswith("'")):
        s = s[1:-1].strip()
    return s


def _envb(k: str, d: bool = False) -> bool:
    v = _envs(k, "")
    if v == "":
        return bool(d)
    s = v.lower().strip()
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


def _parse_csv(s: str) -> list[str]:
    if not s:
        return []
    parts = [p.strip() for p in s.replace(";", ",").split(",")]
    return [p for p in parts if p]


def _render_detected() -> bool:
    return bool(
        _envs("RENDER_SERVICE_ID", "")
        or _envs("RENDER_INSTANCE_ID", "")
        or (_envs("RENDER", "").lower() in _TRUE)
    )


def _mask(s: str) -> str:
    if not s:
        return s
    out = _URL_CRED_RE.sub(r"\1***@", s)
    if _envb("HEALTH_MASK_LONG_TOKENS", True):
        out = _LONG_TOKENISH_RE.sub("***", out)
    return out


def _safe_jsonable(x: Any) -> Any:
    try:
        json.dumps(x, default=str)
        return x
    except Exception:
        return _mask(str(x))


# =============================================================================
# runtime state
# =============================================================================
_START_WALL = time.time()
_START_MONO = time.monotonic()


def _uptime_sec() -> int:
    return int(max(0.0, time.monotonic() - _START_MONO))


def _version_info() -> dict[str, Any]:
    return {
        "service": _envs("SERVICE_NAME", _envs("APP_NAME", "sniper")),
        "version": _envs("VERSION", _envs("APP_VERSION", "v1")),
        "git_sha": (_envs("GIT_SHA", "") or "")[:16],
        "python": platform.python_version(),
        "platform": platform.platform(),
        "render": int(bool(_render_detected())),
    }


def _safe_flags_env() -> dict[str, Any]:
    """
    SAFE allow-list only (no DSNs, no keys, no wallet info).
    """
    return {
        "DRY_RUN": _envs("DRY_RUN", _envs("DRYRUN", "0")),
        "KILL_SWITCH": _envs("KILL_SWITCH", "0"),
        "PAUSE_TRADING": _envs("PAUSE_TRADING", "0"),
        "DISABLE_SCANNERS": _envs("DISABLE_SCANNERS", "0"),
        "SKIP_DSN_REQUIRED": _envs("SKIP_DSN_REQUIRED", "0"),
        "HEALTH_SERVER_READY_PROFILE": _envs("HEALTH_SERVER_READY_PROFILE", "core"),
        "HEALTH_SERVER_READY_STRICT": _envs("HEALTH_SERVER_READY_STRICT", "0"),
    }


def _guardrails_snapshot() -> dict[str, Any]:
    """
    Optional: if you have a canonical guardrails module that can snapshot safe state.
    Must never leak secrets.
    """
    with contextlib.suppress(Exception):
        import utils_pkg.guardrails as gr  # type: ignore

        snap = gr.snapshot()
        if isinstance(snap, dict):
            return _safe_jsonable(snap)
        return {"value": _safe_jsonable(snap)}
    return {}


# =============================================================================
# readiness probing (cached per profile/strict/probes/skip)
# =============================================================================
@dataclass
class _ReadyEntry:
    ts_mono: float
    ok: bool
    payload: dict[str, Any]
    compute_ms: int


_ready_lock = asyncio.Lock()
_ready_cache: dict[str, _ReadyEntry] = {}


def _ready_cache_key(
    *, strict: bool, profile: str, probes: str, skip: str, timeout_s: float, probe_timeout_s: float
) -> str:
    # Small, deterministic cache key (avoid storing huge structures)
    return "|".join(
        [
            f"s={int(strict)}",
            f"p={profile}",
            f"probes={probes}",
            f"skip={skip}",
            f"to={round(float(timeout_s), 2)}",
            f"pto={round(float(probe_timeout_s), 2)}",
        ]
    )


def _profile_to_probes(profile: str) -> set[str]:
    """
    Healthcheck default set can be expensive / flaky in dev (db/redis/solana).
    profile=core is the stable readiness contract for "process ready to start".
    """
    p = (profile or "").strip().lower()
    if p in ("core", "minimal", "min"):
        return {"env", "rpc_select", "core_import", "router"}
    if p in ("full", "all"):
        return set()  # empty means "let healthcheck choose defaults"
    # unknown -> core
    return {"env", "rpc_select", "core_import", "router"}


async def _compute_ready_payload(
    *,
    strict: bool,
    profile: str,
    probes_csv: str,
    skip_csv: str,
    timeout_s: float,
    probe_timeout_s: float,
    max_parallel: int,
) -> Tuple[bool, dict[str, Any], int]:
    t0 = time.time()

    if not _envb("HEALTH_SERVER_READY_ENABLED", True):
        payload = {
            "ok": True,
            "ready": True,
            "disabled": True,
            "why": "HEALTH_SERVER_READY_ENABLED=0",
        }
        return True, payload, int((time.time() - t0) * 1000)

    # Determine probes/skip
    probes_only: set[str]
    if probes_csv.strip():
        probes_only = set(_parse_csv(probes_csv))
    else:
        probes_only = _profile_to_probes(profile)

    probes_skip = set(_parse_csv(skip_csv))

    # Call canonical healthcheck local runner (preferred)
    try:
        from scripts import healthcheck as hc  # type: ignore

        run_fn = getattr(hc, "run_local_healthcheck", None)
        if not callable(run_fn):
            raise RuntimeError("scripts.healthcheck.run_local_healthcheck missing")

        local_res = await run_fn(
            strict=bool(strict),
            overall_timeout_s=float(timeout_s),
            probe_timeout_s=float(probe_timeout_s),
            max_parallel=int(max_parallel),
            probes_only=set(probes_only),
            probes_skip=set(probes_skip),
            json_only=True,
        )

        ok = bool(isinstance(local_res, dict) and local_res.get("ok"))
        payload = {
            "ok": ok,
            "ready": ok,
            "source": "scripts.healthcheck.local",
            "profile": profile,
            "strict": int(bool(strict)),
            "probes_only": sorted(list(probes_only)) if probes_only else [],
            "probes_skip": sorted(list(probes_skip)) if probes_skip else [],
            "result": _safe_jsonable(local_res),
        }
        return ok, payload, int((time.time() - t0) * 1000)

    except Exception as e:
        # Degrade gracefully:
        # - non-strict: ready=true but warn that probe engine is missing
        # - strict: ready=false
        ok2 = bool(not strict)
        payload2 = {
            "ok": ok2,
            "ready": ok2,
            "source": "fallback",
            "profile": profile,
            "strict": int(bool(strict)),
            "error": _mask(repr(e)),
            "hint": "Install/import scripts.healthcheck or disable readiness probes with HEALTH_SERVER_READY_ENABLED=0.",
        }
        return ok2, payload2, int((time.time() - t0) * 1000)


async def _get_ready_payload(
    *,
    strict: bool,
    profile: str,
    probes_csv: str,
    skip_csv: str,
    timeout_s: float,
    probe_timeout_s: float,
    max_parallel: int,
    ttl_s: float,
) -> Tuple[bool, dict[str, Any], dict[str, Any]]:
    ttl_s = max(0.0, float(ttl_s))
    now = time.monotonic()

    key = _ready_cache_key(
        strict=strict,
        profile=profile,
        probes=probes_csv.strip(),
        skip=skip_csv.strip(),
        timeout_s=timeout_s,
        probe_timeout_s=probe_timeout_s,
    )

    async with _ready_lock:
        ent = _ready_cache.get(key)
        if ent is not None and (now - ent.ts_mono) <= ttl_s:
            meta = {
                "cached": 1,
                "age_s": round(float(now - ent.ts_mono), 3),
                "ttl_s": float(ttl_s),
                "compute_ms": int(ent.compute_ms),
                "key": key,
            }
            return bool(ent.ok), dict(ent.payload), meta

        ok, payload, ms = await _compute_ready_payload(
            strict=strict,
            profile=profile,
            probes_csv=probes_csv,
            skip_csv=skip_csv,
            timeout_s=timeout_s,
            probe_timeout_s=probe_timeout_s,
            max_parallel=max_parallel,
        )
        _ready_cache[key] = _ReadyEntry(
            ts_mono=now, ok=bool(ok), payload=dict(payload), compute_ms=int(ms)
        )

        meta2 = {
            "cached": 0,
            "age_s": 0.0,
            "ttl_s": float(ttl_s),
            "compute_ms": int(ms),
            "key": key,
        }
        return bool(ok), dict(payload), meta2


# =============================================================================
# FastAPI implementation (preferred)
# =============================================================================
def _build_fastapi_app():
    from fastapi import FastAPI, Response
    from fastapi.responses import JSONResponse, PlainTextResponse

    app_ = FastAPI(title="sniper-health", version="1.0")

    @app_.get("/")
    async def root():
        return {
            "ok": True,
            "endpoints": ["/healthz", "/livez", "/readyz", "/stats", "/version", "/metrics"],
        }

    @app_.get("/healthz")
    async def healthz():
        return PlainTextResponse("ok", status_code=200)

    @app_.get("/livez")
    async def livez():
        return PlainTextResponse("ok", status_code=200)

    @app_.get("/readyz")
    async def readyz(
        strict: int = 0,
        profile: str = "",
        probes: str = "",
        skip: str = "",
        ttl: float = 0.0,
        timeout_s: float = 0.0,
        probe_timeout_s: float = 0.0,
        max_parallel: int = 0,
    ):
        strict_default = _envb("HEALTH_SERVER_READY_STRICT", False)
        is_strict = bool(strict) or bool(strict_default)

        prof = (profile or _envs("HEALTH_SERVER_READY_PROFILE", "core")).strip().lower() or "core"
        probes_csv = probes or _envs("HEALTH_SERVER_READY_PROBES", "")
        skip_csv = skip or _envs("HEALTH_SERVER_READY_SKIP", "")

        ttl_s = (
            float(ttl) if ttl and ttl > 0 else float(_envf("HEALTH_SERVER_READY_CACHE_TTL_S", 2.0))
        )
        to_s = (
            float(timeout_s)
            if timeout_s and timeout_s > 0
            else float(_envf("HEALTH_SERVER_READY_TIMEOUT_S", 14.0))
        )
        pto_s = (
            float(probe_timeout_s)
            if probe_timeout_s and probe_timeout_s > 0
            else float(_envf("HEALTH_SERVER_READY_PROBE_TIMEOUT_S", 6.0))
        )
        mp = (
            int(max_parallel)
            if max_parallel and max_parallel > 0
            else int(_envi("HEALTH_SERVER_READY_MAX_PARALLEL", 6))
        )

        ok, payload, cache_meta = await _get_ready_payload(
            strict=is_strict,
            profile=prof,
            probes_csv=probes_csv,
            skip_csv=skip_csv,
            timeout_s=to_s,
            probe_timeout_s=pto_s,
            max_parallel=mp,
            ttl_s=ttl_s,
        )
        payload_out = dict(payload or {})
        payload_out["cache"] = cache_meta

        status = 200 if ok else 503
        return JSONResponse(content=_safe_jsonable(payload_out), status_code=status)

    @app_.get("/stats")
    async def stats():
        data = {
            "ok": True,
            "uptime_sec": _uptime_sec(),
            "started_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(_START_WALL)),
            "version": _version_info(),
            "guardrails": _guardrails_snapshot(),
            "env_flags": _safe_flags_env(),
        }
        return JSONResponse(content=_safe_jsonable(data), status_code=200)

    @app_.get("/version")
    async def version():
        return JSONResponse(content=_safe_jsonable(_version_info()), status_code=200)

    @app_.get("/metrics")
    async def metrics():
        lines = [
            "# HELP service_up Service liveness (always 1 if process responds).",
            "# TYPE service_up gauge",
            "service_up 1",
            "# HELP service_uptime_seconds Process uptime seconds.",
            "# TYPE service_uptime_seconds gauge",
            f"service_uptime_seconds {_uptime_sec()}",
            "# HELP service_render Detected Render environment (1/0).",
            "# TYPE service_render gauge",
            f"service_render {1 if _render_detected() else 0}",
        ]
        return Response(content="\n".join(lines) + "\n", media_type="text/plain; version=0.0.4")

    return app_


# =============================================================================
# aiohttp fallback
# =============================================================================
def _build_aiohttp_app():
    from aiohttp import web  # type: ignore

    async def root(_request):
        return web.json_response(
            {
                "ok": True,
                "endpoints": ["/healthz", "/livez", "/readyz", "/stats", "/version", "/metrics"],
            },
            status=200,
        )

    async def healthz(_request):
        return web.Response(text="ok", status=200)

    async def livez(_request):
        return web.Response(text="ok", status=200)

    async def readyz(request):
        q = request.query

        strict_default = _envb("HEALTH_SERVER_READY_STRICT", False)
        strict_q = (q.get("strict", "0") or "0").strip().lower()
        is_strict = (strict_q in _TRUE) or bool(strict_default)

        prof = (
            q.get("profile", "") or _envs("HEALTH_SERVER_READY_PROFILE", "core")
        ).strip().lower() or "core"
        probes_csv = q.get("probes", "") or _envs("HEALTH_SERVER_READY_PROBES", "")
        skip_csv = q.get("skip", "") or _envs("HEALTH_SERVER_READY_SKIP", "")

        ttl_s = float(q.get("ttl", "0") or 0.0) or float(
            _envf("HEALTH_SERVER_READY_CACHE_TTL_S", 2.0)
        )
        to_s = float(q.get("timeout_s", "0") or 0.0) or float(
            _envf("HEALTH_SERVER_READY_TIMEOUT_S", 14.0)
        )
        pto_s = float(q.get("probe_timeout_s", "0") or 0.0) or float(
            _envf("HEALTH_SERVER_READY_PROBE_TIMEOUT_S", 6.0)
        )
        mp = int(q.get("max_parallel", "0") or 0) or int(
            _envi("HEALTH_SERVER_READY_MAX_PARALLEL", 6)
        )

        ok, payload, cache_meta = await _get_ready_payload(
            strict=is_strict,
            profile=prof,
            probes_csv=probes_csv,
            skip_csv=skip_csv,
            timeout_s=to_s,
            probe_timeout_s=pto_s,
            max_parallel=mp,
            ttl_s=ttl_s,
        )
        payload_out = dict(payload or {})
        payload_out["cache"] = cache_meta
        return web.json_response(_safe_jsonable(payload_out), status=(200 if ok else 503))

    async def stats(_request):
        data = {
            "ok": True,
            "uptime_sec": _uptime_sec(),
            "started_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(_START_WALL)),
            "version": _version_info(),
            "guardrails": _guardrails_snapshot(),
            "env_flags": _safe_flags_env(),
        }
        return web.json_response(_safe_jsonable(data), status=200)

    async def version(_request):
        return web.json_response(_safe_jsonable(_version_info()), status=200)

    async def metrics(_request):
        lines = [
            "# HELP service_up Service liveness (always 1 if process responds).",
            "# TYPE service_up gauge",
            "service_up 1",
            "# HELP service_uptime_seconds Process uptime seconds.",
            "# TYPE service_uptime_seconds gauge",
            f"service_uptime_seconds {_uptime_sec()}",
            "# HELP service_render Detected Render environment (1/0).",
            "# TYPE service_render gauge",
            f"service_render {1 if _render_detected() else 0}",
        ]
        return web.Response(text="\n".join(lines) + "\n", content_type="text/plain")

    app_ = web.Application()
    app_.router.add_get("/", root)
    app_.router.add_get("/healthz", healthz)
    app_.router.add_get("/livez", livez)
    app_.router.add_get("/readyz", readyz)
    app_.router.add_get("/stats", stats)
    app_.router.add_get("/version", version)
    app_.router.add_get("/metrics", metrics)
    return app_


# =============================================================================
# public app export + runner
# =============================================================================
def create_app():
    """
    Returns either a FastAPI app or an aiohttp.web.Application depending on env and availability.
    """
    fw = _envs("HEALTH_SERVER_FRAMEWORK", "auto").lower().strip()
    if fw not in ("auto", "fastapi", "aiohttp"):
        fw = "auto"

    if fw in ("auto", "fastapi"):
        with contextlib.suppress(Exception):
            return _build_fastapi_app()

    return _build_aiohttp_app()


# If someone runs: uvicorn scripts.health:app
# We expose a valid ASGI app here (FastAPI). If FastAPI isn't installed, app remains None.
app = None
with contextlib.suppress(Exception):
    app = _build_fastapi_app()  # type: ignore


def run() -> None:
    host = _envs("HOST", "0.0.0.0")
    port = int(_envi("PORT", 8080))
    fw = _envs("HEALTH_SERVER_FRAMEWORK", "auto").lower().strip()
    if fw not in ("auto", "fastapi", "aiohttp"):
        fw = "auto"

    # FastAPI path
    if fw in ("auto", "fastapi"):
        with contextlib.suppress(Exception):
            import uvicorn  # type: ignore

            fa = _build_fastapi_app()
            log.info("health server starting (fastapi) host=%s port=%s", host, port)
            uvicorn.run(
                fa,
                host=host,
                port=port,
                log_level=_envs("UVICORN_LOG_LEVEL", "info"),
                access_log=_envb("UVICORN_ACCESS_LOG", True),
            )
            return

    # aiohttp fallback
    from aiohttp import web  # type: ignore

    a = _build_aiohttp_app()
    log.info("health server starting (aiohttp) host=%s port=%s", host, port)
    web.run_app(a, host=host, port=port)


def main() -> None:
    try:
        run()
    except KeyboardInterrupt:
        raise
    except Exception as e:
        log.exception("health server failed: %s", _mask(repr(e)))
        raise


if __name__ == "__main__":
    main()
