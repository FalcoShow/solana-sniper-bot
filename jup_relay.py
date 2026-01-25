#!/usr/bin/env python3
from __future__ import annotations

"""
jup_relay.py — Jupiter relay (IPv4-friendly + host pin + fallbacks)
==================================================================

Purpose
-------
A small HTTP reverse-proxy you can run *locally* or *on Render* to:
- force IPv4 (some environments have flaky IPv6 / DNS)
- optionally pin upstream hosts to specific IPs (curl --resolve style)
- attach x-api-key automatically
- provide upstream fallback list
- rewrite legacy paths to modern /swap/v1/* paths

This is NOT your canonical router. Your canonical router remains:
  utils_pkg/router.py

This relay is an infra tool to make upstream access more reliable.

Routes
------
Health:
  GET /ping
  GET /__debug

Proxy:
  /v6/*              -> /swap/v1/*
  /quote             -> /swap/v1/quote
  /swap              -> /swap/v1/swap
  /swap-instructions -> /swap/v1/swap-instructions
  /swap/v1/*         -> passthrough
  everything else    -> passthrough (no rewrite unless it matches above)

Env
---
Bind:
  RELAY_BIND=127.0.0.1
  RELAY_PORT=8081

Upstream base (any one):
  RELAY_UPSTREAM=https://api.jup.ag           (will normalize to /swap/v1)
  JUP_BASE_URL=https://api.jup.ag             (fallback if RELAY_UPSTREAM missing)

Fallbacks (comma-separated):
  JUP_ALT_BASE_URLS=https://api.jup.ag,https://quote-api.jup.ag/v6

API key (any one; injected as x-api-key if caller didn’t provide):
  JUP_API_KEY=...
  JUP_PRO_KEY=...
  JUPITER_API_KEY=...

Networking:
  RELAY_FORCE_IPV4=1
  RELAY_HOST_MAP="api.jup.ag=1.2.3.4,quote-api.jup.ag=5.6.7.8"
  RELAY_TIMEOUT_S=12
  RELAY_CONNECT_TIMEOUT_S=4
  RELAY_MAX_BODY_MB=2
  RELAY_MAX_UPSTREAM_BYTES=0   (0 = unlimited; else hard cap)

Logging:
  LOG_LEVEL=INFO

Notes
-----
- Never forwards hop-by-hop headers.
- Streams upstream responses to client (does not buffer large responses).
"""

import asyncio
import os
import socket
import time
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any

from aiohttp import ClientSession, ClientTimeout, TCPConnector, web
from aiohttp.abc import AbstractResolver
from aiohttp.client_exceptions import ClientConnectorError

# ──────────────────────────────────────────────────────────────────────────────
# Logging
# ──────────────────────────────────────────────────────────────────────────────
try:
    from logging_setup import get_logger  # type: ignore

    log = get_logger("jup_relay")  # type: ignore
except Exception:  # pragma: no cover
    import logging

    logging.basicConfig(level=logging.INFO, format="[relay] %(asctime)s %(levelname)s %(message)s")
    log = logging.getLogger("jup_relay")


_TRUE = {"1", "true", "yes", "y", "on", "t"}
_FALSE = {"0", "false", "no", "n", "off", "f"}


def _envs(k: str, d: str = "") -> str:
    v = os.getenv(k)
    return (v if v is not None else d).strip()


def _envb(k: str, d: bool = False) -> bool:
    v = _envs(k, "")
    if not v:
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


def _first_env(keys: Iterable[str], default: str = "") -> str:
    for k in keys:
        v = _envs(k, "")
        if v:
            return v
    return default


# ──────────────────────────────────────────────────────────────────────────────
# Config
# ──────────────────────────────────────────────────────────────────────────────
@dataclass(frozen=True)
class RelayConfig:
    bind: str
    port: int
    upstream_raw: str
    upstream_primary: str
    upstream_fallbacks: list[str]
    api_key: str
    force_ipv4: bool
    hostmap: dict[str, str]
    timeout_s: float
    connect_timeout_s: float
    max_body_bytes: int
    max_upstream_bytes: int  # 0 = unlimited


def _ensure_swap_v1(base: str) -> str:
    base = (base or "").strip().rstrip("/")
    if not base:
        return ""
    if base.endswith("/swap/v1"):
        return base
    # Some users provide /v6 as base (quote-api), normalize to that style but still serve /quote via rewrite.
    if base.endswith("/v6"):
        return base  # we will still rewrite paths; keep as provided
    return f"{base}/swap/v1"


def _parse_hostmap(raw: str) -> dict[str, str]:
    out: dict[str, str] = {}
    raw = (raw or "").strip()
    if not raw:
        return out
    for item in raw.split(","):
        item = item.strip()
        if not item or "=" not in item:
            continue
        host, ip = item.split("=", 1)
        host = host.strip().lower()
        ip = ip.strip()
        if host and ip:
            out[host] = ip
    return out


def _alts(raw: str) -> list[str]:
    out: list[str] = []
    for p in (raw or "").split(","):
        s = p.strip()
        if s:
            out.append(s.rstrip("/"))
    return out


def load_config() -> RelayConfig:
    bind = _envs("RELAY_BIND", "127.0.0.1")
    port = _envi("RELAY_PORT", 8081)

    upstream_raw = _first_env(["RELAY_UPSTREAM", "JUP_BASE_URL"], "https://api.jup.ag").rstrip("/")
    primary = _ensure_swap_v1(upstream_raw)

    fallbacks_raw = _envs("JUP_ALT_BASE_URLS", "")
    fallbacks = [_ensure_swap_v1(u) for u in _alts(fallbacks_raw) if _ensure_swap_v1(u)]

    # API key: accept several names; inject as x-api-key
    api_key = _first_env(["JUP_API_KEY", "JUP_PRO_KEY", "JUPITER_API_KEY"], "")

    force_ipv4 = _envb("RELAY_FORCE_IPV4", True)
    hostmap = _parse_hostmap(_envs("RELAY_HOST_MAP", ""))

    timeout_s = _envf("RELAY_TIMEOUT_S", 12.0)
    connect_timeout_s = _envf("RELAY_CONNECT_TIMEOUT_S", 4.0)

    max_body_mb = _envf("RELAY_MAX_BODY_MB", 2.0)
    max_body_bytes = int(max(64 * 1024, max_body_mb * 1024 * 1024))

    max_upstream_bytes = _envi("RELAY_MAX_UPSTREAM_BYTES", 0)  # 0 = unlimited

    # Dedup upstreams
    seen = set()
    dedup_fallbacks: list[str] = []
    for u in [primary, *fallbacks]:
        if u and u not in seen:
            seen.add(u)
            dedup_fallbacks.append(u)

    # first item is primary
    upstream_primary = dedup_fallbacks[0] if dedup_fallbacks else primary
    upstream_fallbacks = dedup_fallbacks[1:] if len(dedup_fallbacks) > 1 else []

    return RelayConfig(
        bind=bind,
        port=port,
        upstream_raw=upstream_raw,
        upstream_primary=upstream_primary,
        upstream_fallbacks=upstream_fallbacks,
        api_key=api_key,
        force_ipv4=force_ipv4,
        hostmap=hostmap,
        timeout_s=float(max(3.0, timeout_s)),
        connect_timeout_s=float(max(1.0, connect_timeout_s)),
        max_body_bytes=max_body_bytes,
        max_upstream_bytes=max_upstream_bytes,
    )


CFG = load_config()


# ──────────────────────────────────────────────────────────────────────────────
# Resolver (host pin)
# ──────────────────────────────────────────────────────────────────────────────
class HostMapResolver(AbstractResolver):
    """
    Static host->IP resolver (curl --resolve equivalent), with fallback to OS DNS.
    """

    def __init__(self, mapping: dict[str, str]):
        self._map = {k.lower(): v for k, v in (mapping or {}).items() if k and v}

    async def resolve(
        self, host: str, port: int = 0, family: int = socket.AF_UNSPEC
    ) -> list[dict[str, Any]]:
        ip = self._map.get((host or "").lower())
        if ip:
            fam = socket.AF_INET6 if ":" in ip else socket.AF_INET
            return [
                {
                    "hostname": host,
                    "host": ip,
                    "port": port,
                    "family": fam,
                    "proto": 0,
                    "flags": 0,
                }
            ]

        loop = asyncio.get_event_loop()
        infos = await loop.getaddrinfo(host, port, family=family, type=socket.SOCK_STREAM)
        out: list[dict[str, Any]] = []
        for a in infos:
            out.append(
                {
                    "hostname": host,
                    "host": a[4][0],
                    "port": a[4][1],
                    "family": a[0],
                    "proto": a[2],
                    "flags": 0,
                }
            )
        return out

    async def close(self) -> None:
        return None


# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────
_HOP_BY_HOP = {
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailers",
    "transfer-encoding",
    "upgrade",
}


def _sanitize_request_headers(h: dict[str, str]) -> dict[str, str]:
    out = {}
    for k, v in (h or {}).items():
        kl = k.lower()
        if kl in _HOP_BY_HOP:
            continue
        if kl == "host":
            continue
        # aiohttp will handle content-length automatically for bytes/data
        if kl == "content-length":
            continue
        out[k] = v
    return out


def _sanitize_response_headers(h: Any) -> dict[str, str]:
    out: dict[str, str] = {}
    for k, v in (h or {}).items():
        kl = k.lower()
        if kl in _HOP_BY_HOP:
            continue
        if kl == "content-length":
            continue
        out[k] = v
    return out


def _rewrite_path(path: str) -> str:
    p = path or "/"
    # direct modern
    if p.startswith("/swap/v1/"):
        return p
    # v6 legacy style
    if p.startswith("/v6/"):
        return "/swap/v1/" + p[len("/v6/") :]
    # short forms
    if p == "/quote":
        return "/swap/v1/quote"
    if p == "/swap":
        return "/swap/v1/swap"
    if p == "/swap-instructions":
        return "/swap/v1/swap-instructions"
    return p


def _build_try_list() -> list[str]:
    out = [CFG.upstream_primary, *CFG.upstream_fallbacks]
    seen = set()
    dedup: list[str] = []
    for u in out:
        if u and u not in seen:
            seen.add(u)
            dedup.append(u)
    return dedup


def _make_url(base: str, path: str, query: str) -> str:
    base = base.rstrip("/")
    p = (path or "/").lstrip("/")
    url = f"{base}/{p}"
    if query:
        url = f"{url}?{query}"
    return url


# ──────────────────────────────────────────────────────────────────────────────
# Runtime state / counters
# ──────────────────────────────────────────────────────────────────────────────
class Counters:
    def __init__(self) -> None:
        self.started = time.time()
        self.total = 0
        self.ok = 0
        self.fail = 0
        self.by_status: dict[str, int] = {}
        self.fallback_used = 0

    def bump(self, status: int | None, ok: bool, used_fallback: bool) -> None:
        self.total += 1
        if ok:
            self.ok += 1
        else:
            self.fail += 1
        if status is not None:
            s = str(int(status))
            self.by_status[s] = self.by_status.get(s, 0) + 1
        if used_fallback:
            self.fallback_used += 1


CTR = Counters()


# ──────────────────────────────────────────────────────────────────────────────
# Handlers
# ──────────────────────────────────────────────────────────────────────────────
async def ping(_req: web.Request) -> web.Response:
    return web.json_response({"ok": True, "relay": "up"})


async def debug(_req: web.Request) -> web.Response:
    return web.json_response(
        {
            "ok": True,
            "bind": f"{CFG.bind}:{CFG.port}",
            "upstream_raw": CFG.upstream_raw,
            "primary_norm": CFG.upstream_primary,
            "fallbacks_norm": CFG.upstream_fallbacks,
            "api_key_present": bool(CFG.api_key),
            "force_ipv4": CFG.force_ipv4,
            "hostmap": CFG.hostmap,
            "timeouts": {"connect_s": CFG.connect_timeout_s, "total_s": CFG.timeout_s},
            "limits": {
                "max_body_bytes": CFG.max_body_bytes,
                "max_upstream_bytes": CFG.max_upstream_bytes,
            },
            "counters": {
                "uptime_s": int(time.time() - CTR.started),
                "total": CTR.total,
                "ok": CTR.ok,
                "fail": CTR.fail,
                "fallback_used": CTR.fallback_used,
                "by_status": CTR.by_status,
            },
        }
    )


async def _read_body_limited(req: web.Request) -> bytes | None:
    if not req.can_read_body:
        return b""
    total = 0
    chunks: list[bytes] = []
    async for chunk in req.content.iter_chunked(64 * 1024):
        total += len(chunk)
        if total > CFG.max_body_bytes:
            return None
        chunks.append(chunk)
    return b"".join(chunks)


async def _proxy_once(
    *,
    session: ClientSession,
    method: str,
    url: str,
    headers: dict[str, str],
    body: bytes | None,
) -> tuple[int, dict[str, str], Any]:
    """
    Returns (status, headers, upstream_response_object).
    upstream_response_object is a ClientResponse for streaming.
    """
    timeout = ClientTimeout(total=CFG.timeout_s, connect=CFG.connect_timeout_s)
    resp = await session.request(
        method=method,
        url=url,
        headers=headers,
        data=body,
        allow_redirects=False,
        timeout=timeout,
    )
    return int(resp.status), _sanitize_response_headers(resp.headers), resp


async def passthrough(req: web.Request) -> web.StreamResponse:
    # protect health endpoints
    if req.path in ("/ping", "/__debug"):
        return web.Response(status=404, text="not found")

    method = req.method.upper()
    rewritten = _rewrite_path(req.path)

    # request headers + key injection
    headers = _sanitize_request_headers(dict(req.headers))
    if CFG.api_key and not any(k.lower() == "x-api-key" for k in headers):
        headers["x-api-key"] = CFG.api_key

    # request body limit
    body = await _read_body_limited(req)
    if body is None:
        return web.json_response({"ok": False, "error": "request body too large"}, status=413)

    # try primary then fallbacks
    tries = _build_try_list()
    session: ClientSession = req.app["session"]

    last_err: str | None = None
    used_fallback = False

    for idx, base in enumerate(tries):
        candidate = _make_url(base, rewritten, req.query_string)
        used_fallback = idx > 0
        try:
            st, resp_headers, upstream = await _proxy_once(
                session=session,
                method=method,
                url=candidate,
                headers=headers,
                body=body,
            )

            # If auth/not found and we have fallbacks, try next
            if st in (401, 403, 404) and idx < len(tries) - 1:
                last_err = f"status:{st}"
                await upstream.release()
                continue

            # Stream response back
            out = web.StreamResponse(status=st, headers=resp_headers)
            await out.prepare(req)

            total_out = 0
            async for chunk in upstream.content.iter_chunked(128 * 1024):
                if chunk:
                    total_out += len(chunk)
                    if CFG.max_upstream_bytes > 0 and total_out > CFG.max_upstream_bytes:
                        # stop streaming
                        CTR.bump(status=502, ok=False, used_fallback=used_fallback)
                        await upstream.release()
                        await out.write_eof()
                        return out
                    await out.write(chunk)

            await upstream.release()
            await out.write_eof()

            CTR.bump(status=st, ok=(200 <= st < 400), used_fallback=used_fallback)
            return out

        except TimeoutError:
            last_err = "timeout"
            continue
        except ClientConnectorError as e:
            # include DNS hint if gaierror
            cause = getattr(e, "__cause__", None)
            if isinstance(cause, socket.gaierror):
                last_err = "dns_resolution_failed"
            else:
                last_err = "connect_error"
            continue
        except Exception as e:
            last_err = f"upstream_error:{type(e).__name__}"
            break

    CTR.bump(status=502, ok=False, used_fallback=bool(used_fallback))
    return web.json_response(
        {"ok": False, "error": "failed to contact upstreams", "detail": last_err, "path": req.path},
        status=502,
    )


# ──────────────────────────────────────────────────────────────────────────────
# App factory
# ──────────────────────────────────────────────────────────────────────────────
def make_app() -> web.Application:
    app = web.Application()

    app.add_routes(
        [
            web.get("/ping", ping),
            web.get("/__debug", debug),
            # legacy short forms
            web.route("*", "/quote", passthrough),
            web.route("*", "/swap", passthrough),
            web.route("*", "/swap-instructions", passthrough),
            # v6 + v1 paths
            web.route("*", "/v6/{tail:.*}", passthrough),
            web.route("*", "/swap/v1/{tail:.*}", passthrough),
            # catch-all
            web.route("*", "/{tail:.*}", passthrough),
        ]
    )

    family = socket.AF_INET if CFG.force_ipv4 else 0
    resolver = HostMapResolver(CFG.hostmap) if CFG.hostmap else None

    async def on_startup(_app: web.Application) -> None:
        log.info(
            "relay start bind=%s:%s upstream_raw=%s primary=%s fallbacks=%s ipv4=%s hostmap=%s",
            CFG.bind,
            CFG.port,
            CFG.upstream_raw,
            CFG.upstream_primary,
            CFG.upstream_fallbacks,
            CFG.force_ipv4,
            bool(CFG.hostmap),
        )
        _app["session"] = ClientSession(
            connector=TCPConnector(family=family, resolver=resolver, ttl_dns_cache=60),
            headers={"user-agent": "pnlsniper-relay"},
        )

    async def on_cleanup(_app: web.Application) -> None:
        sess: ClientSession = _app.get("session")
        if sess:
            await sess.close()

    app.on_startup.append(on_startup)
    app.on_cleanup.append(on_cleanup)
    return app


if __name__ == "__main__":
    web.run_app(make_app(), host=CFG.bind, port=CFG.port)
