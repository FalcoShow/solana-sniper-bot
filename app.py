# app.py
from __future__ import annotations

"""
app.py — Health / readiness / metrics + lightweight scoring service
==================================================================

This file provides a small HTTP surface used for:
  - /live, /ready, /health : runbook-grade service checks
  - /metrics               : lightweight JSON metrics (Prometheus optional later)
  - /score                 : simple trust/fraud score for a mint (Dexscreener-based)

Core integration
----------------
core.py may start this module as a support loop via:
  - async amain(stop_evt=...) -> runs uvicorn server and stops cleanly

Design goals
------------
- No secrets in responses/logs
- Uses shared aiohttp session from utils_pkg.core_utils when available
- Avoids per-request ClientSession creation (keeps one session if needed)
- Defensive: if external APIs fail, returns safe defaults + reasons
"""

import asyncio
import contextlib
import json
import logging
import os
import time
from dataclasses import dataclass
from typing import Any

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse, PlainTextResponse
from pydantic import BaseModel, Field

log = logging.getLogger("app")


# -----------------------------------------------------------------------------
# Config (env-first; config.py optional)
# -----------------------------------------------------------------------------


def _truthy(v: str | None) -> bool:
    if v is None:
        return False
    return v.strip().lower() in {"1", "true", "yes", "y", "on"}


def _env_str(name: str, default: str = "") -> str:
    v = os.getenv(name)
    return default if v is None else str(v)


def _env_int(name: str, default: int) -> int:
    try:
        return int(_env_str(name, str(default)).strip())
    except Exception:
        return int(default)


def _env_float(name: str, default: float) -> float:
    try:
        return float(_env_str(name, str(default)).strip())
    except Exception:
        return float(default)


# Public HTTP server knobs
APP_HOST = _env_str("APP_HOST", "0.0.0.0")
APP_PORT = _env_int("APP_PORT", 8000)

# Auth for /score
FRAUD_API_KEY = _env_str("FRAUD_API_KEY", "").strip()
FRAUD_AUTH_REQUIRED = _truthy(
    _env_str("FRAUD_AUTH_REQUIRED", "1")
)  # if key is set, enforce; if key empty, allow unless required

# External source
DEX_URL = _env_str("DEX_SCORE_URL", "https://api.dexscreener.com/latest/dex/tokens/{mint}")

# Caching / timeouts
CACHE_TTL_SEC = _env_float("FRAUD_CACHE_TTL_SEC", 45.0)
HTTP_TIMEOUT_SEC = _env_float("FRAUD_HTTP_TIMEOUT_SEC", 10.0)
HTTP_RETRIES = _env_int("FRAUD_HTTP_RETRIES", 2)
HTTP_RETRY_BASE_MS = _env_int("FRAUD_HTTP_RETRY_BASE_MS", 250)

# Simple rate limiting (optional)
RATE_LIMIT_RPM = _env_int("FRAUD_RATE_LIMIT_RPM", 0)  # 0 disables


# -----------------------------------------------------------------------------
# Minimal metrics
# -----------------------------------------------------------------------------

_METRICS: dict[str, float] = {
    "requests_total": 0.0,
    "requests_4xx": 0.0,
    "requests_5xx": 0.0,
    "score_requests": 0.0,
    "score_cache_hit": 0.0,
    "score_cache_miss": 0.0,
    "score_upstream_ok": 0.0,
    "score_upstream_fail": 0.0,
    "score_latency_ms_sum": 0.0,
    "score_latency_ms_count": 0.0,
}


def _m_inc(name: str, amt: float = 1.0) -> None:
    _METRICS[name] = float(_METRICS.get(name, 0.0)) + float(amt)


def _m_obs_ms(name_prefix: str, ms: float) -> None:
    _m_inc(f"{name_prefix}_sum", ms)
    _m_inc(f"{name_prefix}_count", 1.0)


# -----------------------------------------------------------------------------
# In-memory cache + stampede locks
# -----------------------------------------------------------------------------


@dataclass
class CacheEntry:
    ts: float
    value: dict


_SCORE_CACHE: dict[str, CacheEntry] = {}
_LOCKS: dict[str, asyncio.Lock] = {}


def _cache_get(mint: str) -> dict | None:
    ent = _SCORE_CACHE.get(mint)
    if not ent:
        return None
    if (time.time() - ent.ts) > CACHE_TTL_SEC:
        return None
    return ent.value


def _cache_set(mint: str, value: dict) -> None:
    _SCORE_CACHE[mint] = CacheEntry(ts=time.time(), value=value)


def _lock_for(mint: str) -> asyncio.Lock:
    lock = _LOCKS.get(mint)
    if lock is None:
        lock = asyncio.Lock()
        _LOCKS[mint] = lock
    return lock


# -----------------------------------------------------------------------------
# FastAPI app
# -----------------------------------------------------------------------------

app = FastAPI(
    title="PnL Sniper App",
    version=_env_str("VERSION", _env_str("GIT_SHA", "dev")),
)


# -----------------------------------------------------------------------------
# Optional shared resources (aiohttp session, redis, db) via core_utils
# -----------------------------------------------------------------------------


async def _get_core_utils() -> Any:
    try:
        from utils_pkg import core_utils as cu  # type: ignore

        return cu
    except Exception:
        return None


async def _get_shared_session() -> Any:
    cu = await _get_core_utils()
    if cu is None:
        return None
    fn = getattr(cu, "get_http_session", None) or getattr(cu, "get_session", None)
    if callable(fn):
        try:
            out = fn()
            if asyncio.iscoroutine(out):
                out = await out
            return out
        except Exception:
            return None
    return None


async def _get_shared_redis() -> Any:
    cu = await _get_core_utils()
    if cu is None:
        return None
    fn = getattr(cu, "get_redis", None) or getattr(cu, "get_redis_async", None)
    if callable(fn):
        try:
            out = fn()
            if asyncio.iscoroutine(out):
                out = await out
            return out
        except Exception:
            return None
    return None


# fallback session owned by this module (only used if core_utils session is unavailable)
_AIOHTTP_SESSION: Any = None


@app.on_event("startup")
async def _startup() -> None:
    global _AIOHTTP_SESSION
    # Create a fallback aiohttp session only if needed
    s = await _get_shared_session()
    if s is None:
        try:
            import aiohttp

            timeout = aiohttp.ClientTimeout(total=HTTP_TIMEOUT_SEC)
            _AIOHTTP_SESSION = aiohttp.ClientSession(timeout=timeout)
            log.info("app: created fallback aiohttp session (shared session not available)")
        except Exception as e:
            _AIOHTTP_SESSION = None
            log.warning("app: could not create fallback aiohttp session: %s", e)


@app.on_event("shutdown")
async def _shutdown() -> None:
    global _AIOHTTP_SESSION
    if _AIOHTTP_SESSION is not None:
        with contextlib.suppress(Exception):
            await _AIOHTTP_SESSION.close()
        _AIOHTTP_SESSION = None


def _client_ip(req: Request) -> str:
    # best-effort; do not trust for security beyond simple throttles
    xff = req.headers.get("x-forwarded-for")
    if xff:
        return xff.split(",")[0].strip()
    return req.client.host if req.client else "unknown"  # type: ignore


# -----------------------------------------------------------------------------
# Simple in-memory rate limiter (optional)
# -----------------------------------------------------------------------------

_RL_BUCKET: dict[str, tuple[float, int]] = {}  # ip -> (window_start, count)


def _rate_limit_check(req: Request) -> None:
    if RATE_LIMIT_RPM <= 0:
        return
    ip = _client_ip(req)
    now = time.time()
    window = 60.0
    start, count = _RL_BUCKET.get(ip, (now, 0))
    if (now - start) >= window:
        start, count = now, 0
    count += 1
    _RL_BUCKET[ip] = (start, count)
    if count > RATE_LIMIT_RPM:
        raise HTTPException(status_code=429, detail="rate_limited")


# -----------------------------------------------------------------------------
# Models
# -----------------------------------------------------------------------------


class ScoreResponse(BaseModel):
    fraud_score: float = Field(
        ...,
        ge=0.0,
        le=1.0,
        description="0..1 where higher means more trustworthy / lower fraud risk",
    )
    source: str = Field("dexscreener", description="upstream source name")
    mint: str
    details: dict[str, Any] = Field(default_factory=dict)


# -----------------------------------------------------------------------------
# Auth
# -----------------------------------------------------------------------------


def _require_bearer(req: Request) -> None:
    if not FRAUD_API_KEY:
        if FRAUD_AUTH_REQUIRED:
            raise HTTPException(status_code=401, detail="unauthorized")
        return

    auth = req.headers.get("authorization", "")
    if not auth.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="unauthorized")
    token = auth.split(" ", 1)[1].strip()
    if token != FRAUD_API_KEY:
        raise HTTPException(status_code=401, detail="unauthorized")


# -----------------------------------------------------------------------------
# Scoring helpers
# -----------------------------------------------------------------------------


def _cap01(x: float) -> float:
    return max(0.0, min(1.0, float(x)))


def score_from_liq(liq_usd: float) -> float:
    # 0..1 cap; 0 at $0 liq, ~1 by $250k+
    return _cap01((liq_usd or 0.0) / 250_000.0)


def score_from_age_minutes(age_min: float) -> float:
    # brand new pools score lower; >= 2h ~ 1.0
    return _cap01((age_min or 0.0) / 120.0)


def _epoch_age_minutes(created_ts: float | None) -> float:
    if not created_ts:
        return 0.0
    ts = float(created_ts)
    # Dex may return ms
    if ts > 10_000_000_000:
        ts /= 1000.0
    # if ts looks like monotonic, treat as unknown
    if ts < 1_000_000_000:
        return 0.0
    return max(0.0, (time.time() - ts) / 60.0)


def _extract_num(d: Any, *path: str, default: float = 0.0) -> float:
    cur = d
    for p in path:
        if not isinstance(cur, dict):
            return float(default)
        cur = cur.get(p)
    try:
        return float(cur)
    except Exception:
        return float(default)


async def _http_get_json(url: str) -> dict:
    """
    Uses utils_pkg.proxy_client if available (for unified proxy/retry policy),
    otherwise uses aiohttp session (shared or fallback).
    """
    # Preferred: proxy_client for consistent proxy/timeouts
    try:
        from utils_pkg.proxy_client import http_get, make_session  # type: ignore

        # Retry loop around http_get
        last_err: str | None = None
        for i in range(max(0, HTTP_RETRIES) + 1):
            try:
                async with make_session(timeout=HTTP_TIMEOUT_SEC) as s:  # type: ignore
                    status, body = await http_get(s, url)  # type: ignore
                if int(status) == 200 and body:
                    return json.loads(body)
                last_err = f"status={status}"
            except Exception as e:
                last_err = str(e)

            if i < HTTP_RETRIES:
                await asyncio.sleep((HTTP_RETRY_BASE_MS / 1000.0) * (2**i))
        raise RuntimeError(last_err or "http_get_failed")
    except Exception:
        pass

    # Fallback: aiohttp with shared/fallback session
    try:
        import aiohttp

        s = await _get_shared_session()
        if s is None:
            s = _AIOHTTP_SESSION
        if s is None:
            timeout = aiohttp.ClientTimeout(total=HTTP_TIMEOUT_SEC)
            async with aiohttp.ClientSession(timeout=timeout) as tmp, tmp.get(url) as r:
                if int(r.status) != 200:
                    raise RuntimeError(f"status={r.status}")
                return await r.json(content_type=None)

        # Retry loop using existing session
        last_err = None
        for i in range(max(0, HTTP_RETRIES) + 1):
            try:
                async with s.get(url) as r:  # type: ignore
                    if int(r.status) != 200:
                        last_err = f"status={r.status}"
                    else:
                        return await r.json(content_type=None)
            except Exception as e:
                last_err = str(e)

            if i < HTTP_RETRIES:
                await asyncio.sleep((HTTP_RETRY_BASE_MS / 1000.0) * (2**i))
        raise RuntimeError(last_err or "aiohttp_failed")
    except Exception as e:
        raise RuntimeError(str(e)) from e


async def score_mint(mint: str) -> dict:
    """
    Returns dict with keys:
      - fraud_score (0..1)  higher = more trustworthy / lower fraud risk
      - details (signals)
    """
    mint = (mint or "").strip()
    if not mint:
        return {"fraud_score": 0.0, "details": {"reason": "empty_mint"}}

    # cache
    cached = _cache_get(mint)
    if cached is not None:
        _m_inc("score_cache_hit")
        return cached

    _m_inc("score_cache_miss")

    # stampede lock per mint
    lock = _lock_for(mint)
    async with lock:
        cached2 = _cache_get(mint)
        if cached2 is not None:
            _m_inc("score_cache_hit")
            return cached2

        url = DEX_URL.format(mint=mint)
        t0 = time.time()

        # Safe defaults if upstream fails
        out = {
            "fraud_score": 0.55,  # neutral-ish
            "source": "dexscreener",
            "mint": mint,
            "details": {"upstream": "unknown"},
        }

        try:
            data = await _http_get_json(url)
            pairs = data.get("pairs") or []
            if not pairs:
                out["fraud_score"] = 0.45
                out["details"] = {"upstream": "ok", "reason": "no_pairs"}
                _m_inc("score_upstream_ok")
                _m_obs_ms("score_latency_ms", (time.time() - t0) * 1000)
                _cache_set(mint, out)
                return out

            # Choose the most “relevant” pair: max liquidity.usd
            def _liq_key(p: Any) -> float:
                return _extract_num(p, "liquidity", "usd", default=0.0)

            top = max(pairs, key=_liq_key)

            liq_usd = _extract_num(top, "liquidity", "usd", default=0.0)
            created = top.get("pairCreatedAt") or top.get("createdAt") or top.get("pairCreatedAtMs")
            age_min = _epoch_age_minutes(float(created) if created is not None else None)

            vol_h24 = _extract_num(top, "volume", "h24", default=0.0)
            vol_h1 = _extract_num(top, "volume", "h1", default=0.0)
            chg_h1 = _extract_num(top, "priceChange", "h1", default=0.0)
            chg_h24 = _extract_num(top, "priceChange", "h24", default=0.0)
            buys_h1 = _extract_num(top, "txns", "h1", "buys", default=0.0)
            sells_h1 = _extract_num(top, "txns", "h1", "sells", default=0.0)
            fdv = _extract_num(top, "fdv", default=0.0)

            # Base “trust” score
            s_liq = score_from_liq(liq_usd)
            s_age = score_from_age_minutes(age_min)

            # Micro penalties (bounded)
            # - extremely high FDV relative to liquidity is suspicious
            fdv_to_liq = (fdv / max(1.0, liq_usd)) if liq_usd > 0 else 999.0
            p_fdv = _cap01((fdv_to_liq - 5.0) / 20.0)  # starts penalizing after ~5x, max by ~25x

            # - heavy sell pressure in last hour
            total_trades = max(1.0, buys_h1 + sells_h1)
            sell_ratio = sells_h1 / total_trades
            p_sell = _cap01((sell_ratio - 0.55) / 0.35)  # penalize if sells dominate

            # - violent dump in last hour/day
            p_dump = (
                _cap01(max(0.0, -chg_h1) / 40.0) * 0.7 + _cap01(max(0.0, -chg_h24) / 70.0) * 0.3
            )

            # - no volume at all is suspicious
            p_novol = 1.0 if (vol_h24 <= 0 and vol_h1 <= 0) else 0.0

            # Weighted combine: trust = base - penalties
            trust = (
                0.62 * s_liq
                + 0.25 * s_age
                + 0.13 * _cap01(vol_h24 / 250_000.0)  # small boost if meaningful activity
            )

            penalties = 0.22 * p_fdv + 0.18 * p_sell + 0.22 * p_dump + 0.10 * p_novol

            fraud_score = _cap01(trust - penalties)

            out = {
                "fraud_score": round(float(fraud_score), 4),
                "source": "dexscreener",
                "mint": mint,
                "details": {
                    "upstream": "ok",
                    "liq_usd": round(liq_usd, 2),
                    "age_min": round(age_min, 2),
                    "vol_h24": round(vol_h24, 2),
                    "vol_h1": round(vol_h1, 2),
                    "chg_h1": round(chg_h1, 3),
                    "chg_h24": round(chg_h24, 3),
                    "buys_h1": int(buys_h1),
                    "sells_h1": int(sells_h1),
                    "fdv": round(fdv, 2),
                    "fdv_to_liq": round(fdv_to_liq, 3) if fdv_to_liq < 1e9 else None,
                    "components": {
                        "s_liq": round(s_liq, 4),
                        "s_age": round(s_age, 4),
                        "p_fdv": round(p_fdv, 4),
                        "p_sell": round(p_sell, 4),
                        "p_dump": round(p_dump, 4),
                        "p_novol": round(p_novol, 4),
                        "trust_raw": round(trust, 4),
                        "penalties_raw": round(penalties, 4),
                    },
                },
            }

            _m_inc("score_upstream_ok")
            _m_obs_ms("score_latency_ms", (time.time() - t0) * 1000)
            _cache_set(mint, out)
            return out

        except Exception as e:
            _m_inc("score_upstream_fail")
            _m_obs_ms("score_latency_ms", (time.time() - t0) * 1000)
            out["details"] = {"upstream": "fail", "err": str(e)[:240]}
            _cache_set(mint, out)
            return out


# -----------------------------------------------------------------------------
# Middleware: request counting + error buckets
# -----------------------------------------------------------------------------


@app.middleware("http")
async def _metrics_middleware(request: Request, call_next):
    _m_inc("requests_total")
    try:
        resp = await call_next(request)
        sc = int(getattr(resp, "status_code", 200))
        if 400 <= sc < 500:
            _m_inc("requests_4xx")
        if sc >= 500:
            _m_inc("requests_5xx")
        return resp
    except HTTPException as he:
        sc = int(getattr(he, "status_code", 500))
        if 400 <= sc < 500:
            _m_inc("requests_4xx")
        if sc >= 500:
            _m_inc("requests_5xx")
        raise
    except Exception:
        _m_inc("requests_5xx")
        raise


# -----------------------------------------------------------------------------
# Endpoints
# -----------------------------------------------------------------------------


@app.get("/live")
async def live() -> dict:
    return {"ok": True, "service": "app", "ts": int(time.time())}


@app.get("/ready")
async def ready() -> dict:
    """
    Readiness = dependencies are reachable if configured.
    We keep this strict enough for ops, but not a secret-leaking endpoint.
    """
    checks: dict[str, Any] = {"ok": True}

    # Redis ping if available
    try:
        r = await _get_shared_redis()
        if r is not None:
            pong = r.ping()
            if asyncio.iscoroutine(pong):
                pong = await pong
            checks["redis"] = {"ok": bool(pong)}
        else:
            checks["redis"] = {"ok": None}  # not configured / not present
    except Exception as e:
        checks["ok"] = False
        checks["redis"] = {"ok": False, "err": str(e)[:160]}

    # Optional: DB check (future: use utils_pkg.ai_db/db_dsn). For now, keep soft.
    checks["db"] = {"ok": None}

    return checks


@app.get("/health")
async def health() -> dict:
    """
    Health = live + ready summary (fast).
    """
    r = await ready()
    return {"ok": bool(r.get("ok")), "ready": r, "ts": int(time.time())}


@app.get("/version")
async def version() -> dict:
    return {
        "version": app.version,
        "git_sha": _env_str("GIT_SHA", ""),
        "env": _env_str("ENV", ""),
        "ts": int(time.time()),
    }


@app.get("/metrics")
async def metrics() -> JSONResponse:
    """
    Lightweight JSON metrics. (Prometheus exporter lives elsewhere in your plan.)
    """
    # avoid unbounded growth in output
    snap = dict(_METRICS)
    # derived average latency
    c = float(snap.get("score_latency_ms_count", 0.0))
    s = float(snap.get("score_latency_ms_sum", 0.0))
    snap["score_latency_ms_avg"] = (s / c) if c > 0 else 0.0
    snap["cache_size"] = len(_SCORE_CACHE)
    return JSONResponse(snap)


@app.get("/score", response_model=ScoreResponse)
async def score(token: str, request: Request) -> Any:
    """
    Returns fraud_score in [0..1] (higher = more trustworthy / lower fraud risk).

    Auth:
      - If FRAUD_API_KEY is set, requires Authorization: Bearer <key>
      - If not set, allowed unless FRAUD_AUTH_REQUIRED=1
    """
    _rate_limit_check(request)
    _require_bearer(request)
    _m_inc("score_requests")

    data = await score_mint(token)
    # Keep response shape stable
    return ScoreResponse(
        fraud_score=float(data.get("fraud_score", 0.5)),
        source=str(data.get("source", "dexscreener")),
        mint=str(data.get("mint", token)),
        details=dict(data.get("details") or {}),
    )


@app.get("/")
async def root() -> PlainTextResponse:
    return PlainTextResponse("ok")


# -----------------------------------------------------------------------------
# Uvicorn runner (used by core.py support loop)
# -----------------------------------------------------------------------------


async def amain(
    stop_evt: asyncio.Event | None = None, host: str = APP_HOST, port: int = APP_PORT
) -> int:
    """
    Runs uvicorn server until stop_evt is set (or CTRL+C).
    """
    stop_evt = stop_evt or asyncio.Event()

    try:
        import uvicorn
    except Exception as e:
        raise RuntimeError(f"uvicorn not installed: {e}") from e

    cfg = uvicorn.Config(
        app,
        host=host,
        port=int(port),
        log_level=_env_str("UVICORN_LOG_LEVEL", _env_str("LOG_LEVEL", "info")).lower(),
        access_log=_truthy(_env_str("UVICORN_ACCESS_LOG", "0")),
        loop="asyncio",
        timeout_keep_alive=int(_env_int("UVICORN_KEEPALIVE", 10)),
    )
    server = uvicorn.Server(cfg)

    async def _stopper():
        await stop_evt.wait()
        server.should_exit = True

    stopper_task = asyncio.create_task(_stopper(), name="app.stopper")
    try:
        PB = None
        try:
            import playbook_logger as _pl  # type: ignore

            PB = _pl
        except Exception:
            PB = None
        if PB and callable(getattr(PB, "log_event", None)):
            PB.log_event(kind="app", msg="http server starting", host=host, port=port)

        await server.serve()
        return 0
    finally:
        stopper_task.cancel()
        with contextlib.suppress(Exception):
            await stopper_task


def main() -> None:
    try:
        rc = asyncio.run(amain())
    except KeyboardInterrupt:
        rc = 130
    raise SystemExit(int(rc))


if __name__ == "__main__":
    main()
