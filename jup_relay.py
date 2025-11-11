#!/usr/bin/env python3
"""
jup_relay.py — IPv4-friendly relay for Jupiter (Ultra/paid endpoints)

- Listens on RELAY_BIND:RELAY_PORT (defaults 127.0.0.1:8081)
- RELAY_UPSTREAM may be a bare base (https://api.jup.ag) or already /swap/v1
- Rewrites local legacy paths:
    /v6/*                -> /swap/v1/*
    /quote               -> /swap/v1/quote
    /swap                -> /swap/v1/swap
    /swap-instructions   -> /swap/v1/swap-instructions
  Also accepts direct /swap/v1/* and passes through unchanged.
- Adds x-api-key if JUP_API_KEY is set (unless client already sent it)
- Health: /__debug and /ping
- Optional DNS bypass via RELAY_HOST_MAP ("host=ip,host2=ip2")
- Optional fallbacks via JUP_ALT_BASE_URLS (comma-separated)
"""

from __future__ import annotations

import os
import socket
import asyncio
import logging
from typing import List, Optional, Dict
from urllib.parse import urljoin

from aiohttp import web, ClientSession, TCPConnector, ClientTimeout
from aiohttp.client_exceptions import ClientConnectorError
from aiohttp.abc import AbstractResolver

# ────────────────────────── logging / config ──────────────────────────
LOG = logging.getLogger("jup_relay")
logging.basicConfig(level=logging.INFO, format="[relay] %(asctime)s %(levelname)s %(message)s")

RELAY_BIND = os.environ.get("RELAY_BIND", "127.0.0.1")
RELAY_PORT = int(os.environ.get("RELAY_PORT", "8081"))

UPSTREAM = os.environ.get("RELAY_UPSTREAM", "https://api.jup.ag").rstrip("/")
JUP_API_KEY = os.environ.get("JUP_API_KEY", "")
FORCE_IPV4 = os.environ.get("RELAY_FORCE_IPV4", "1") == "1"

# Optional comma-separated additional base URLs
JUP_ALT_BASE_URLS = os.environ.get("JUP_ALT_BASE_URLS", "").strip()

# RELAY_HOST_MAP: "host=ip,host2=ip2" (curl --resolve style)
RELAY_HOST_MAP = os.environ.get("RELAY_HOST_MAP", "").strip()

# Relaxed validation: allow either bare base or already /swap/v1
if not (UPSTREAM.endswith("/swap/v1") or UPSTREAM.endswith("api.jup.ag")):
    print(f"WARNING: unusual RELAY_UPSTREAM: {UPSTREAM}")

# ────────────────────────── helpers / parsing ──────────────────────────
def _alt_list() -> List[str]:
    out: List[str] = []
    if JUP_ALT_BASE_URLS:
        for p in JUP_ALT_BASE_URLS.split(","):
            s = p.strip()
            if s:
                out.append(s.rstrip("/"))
    return out

ALT_UPSTREAMS_RAW = _alt_list()

def _ensure_swap_v1(base: str) -> str:
    """Normalize a base so it ends with /swap/v1 (no trailing slash)."""
    base = base.rstrip("/")
    return base if base.endswith("/swap/v1") else f"{base}/swap/v1"

# The normalized list we actually try against:
PRIMARY_NORM = _ensure_swap_v1(UPSTREAM)
ALT_UPSTREAMS = [_ensure_swap_v1(u) for u in ALT_UPSTREAMS_RAW]

def _parse_hostmap(raw: str) -> Dict[str, str]:
    hostmap: Dict[str, str] = {}
    if not raw:
        return hostmap
    for item in raw.split(","):
        if "=" in item:
            h, ip = item.split("=", 1)
            h, ip = h.strip().lower(), ip.strip()
            if h and ip:
                hostmap[h] = ip
    return hostmap

HOSTMAP = _parse_hostmap(RELAY_HOST_MAP)

# ────────────────────────── resolver (host pin) ──────────────────────────
class HostMapResolver(AbstractResolver):
    """Static host->IP resolver (curl --resolve equivalent).
    When a host is present in HOSTMAP, connect to that IP but keep SNI as the original host.
    """
    def __init__(self, mapping: Dict[str, str]):
        self._map = {k.lower(): v for k, v in mapping.items() if k and v}

    async def resolve(self, host: str, port: int = 0, family: int = socket.AF_UNSPEC):
        ip = self._map.get(host.lower())
        if not ip:
            loop = asyncio.get_event_loop()
            infos = await loop.getaddrinfo(host, port, family=family, type=socket.SOCK_STREAM)
            return [
                {"hostname": host, "host": a[4][0], "port": a[4][1], "family": a[0], "proto": a[2], "flags": 0}
                for a in infos
            ]
        # Force IPv4 for mapped hosts
        return [{
            "hostname": host,
            "host": ip,
            "port": port,
            "family": socket.AF_INET,
            "proto": 0,
            "flags": 0,
        }]

    async def close(self):
        return

# ────────────────────────── request shaping ──────────────────────────
def _rewrite_path_for_ultra(path: str) -> str:
    """Return a path appropriate for the Ultra endpoints."""
    p = path or "/"
    if p.startswith("/swap/v1/"):
        return p  # already correct
    if p.startswith("/v6/"):
        return "/swap/v1/" + p[len("/v6/"):]
    # short legacy forms
    if p == "/quote":
        return "/swap/v1/quote"
    if p == "/swap":
        return "/swap/v1/swap"
    if p == "/swap-instructions":
        return "/swap/v1/swap-instructions"
    # otherwise pass as-is (client might call newer paths in future)
    return p

async def _json_error(status: int, message: str):
    return web.json_response({"ok": False, "error": message}, status=status)

def _sanitize_request_headers(headers: dict) -> dict:
    h = dict(headers)
    h.pop("Host", None)
    h.pop("host", None)
    for k in list(h.keys()):
        if k.lower() in ("content-length", "transfer-encoding", "connection"):
            h.pop(k, None)
    return h

def _safe_passthrough_headers(resp_headers: dict) -> List[tuple]:
    return [
        (k, v)
        for k, v in resp_headers.items()
        if k.lower() not in ("content-length", "content-encoding", "transfer-encoding", "connection")
    ]

def _build_try_list() -> List[str]:
    out = [PRIMARY_NORM]
    out.extend(ALT_UPSTREAMS)
    seen = set()
    dedup: List[str] = []
    for u in out:
        if u not in seen:
            seen.add(u)
            dedup.append(u)
    return dedup

# ────────────────────────── handlers ──────────────────────────
async def ping(_req: web.Request):
    return web.json_response({"ok": True, "relay": "up"})

async def debug(_req: web.Request):
    return web.json_response({
        "bind": RELAY_BIND,
        "port": RELAY_PORT,
        "upstream_raw": UPSTREAM,
        "primary_norm": PRIMARY_NORM,
        "alt_upstreams_raw": ALT_UPSTREAMS_RAW,
        "alt_upstreams_norm": ALT_UPSTREAMS,
        "api_key_present": bool(JUP_API_KEY),
        "force_ipv4": FORCE_IPV4,
        "hostmap": HOSTMAP,
    })

async def _do_upstream_request(
    session: ClientSession,
    method: str,
    url: str,
    headers: dict,
    data: Optional[bytes],
    timeout_seconds: int = 12,
):
    timeout = ClientTimeout(total=timeout_seconds)
    async with session.request(
        method=method, url=url, headers=headers, data=data,
        allow_redirects=False, timeout=timeout
    ) as resp:
        body = await resp.read()
        passthrough_headers = _safe_passthrough_headers(resp.headers)
        return resp.status, passthrough_headers, body

async def passthrough(request: web.Request):
    path = _rewrite_path_for_ultra(request.path)
    try_list = _build_try_list()

    # Build candidate URLs
    url_candidates = [urljoin(base + "/", path.lstrip("/")) for base in try_list]
    if request.query_string:
        url_candidates = [f"{u}?{request.query_string}" for u in url_candidates]

    headers = _sanitize_request_headers(dict(request.headers))
    if JUP_API_KEY and not any(k.lower() == "x-api-key" for k in headers.keys()):
        headers["x-api-key"] = JUP_API_KEY

    data = await request.read() if request.can_read_body else None
    session: ClientSession = request.app["session"]

    last_exc: Optional[Exception] = None
    for idx, candidate in enumerate(url_candidates):
        LOG.info("proxy -> %s", candidate)
        try:
            status, resp_headers, body = await _do_upstream_request(
                session, request.method, candidate, headers, data
            )

            # fall through on 401/404 to next upstream if available
            if status in (401, 404) and idx < len(url_candidates) - 1:
                LOG.warning("got %s from %s, trying next fallback", status, candidate)
                continue

            return web.Response(status=status, headers=dict(resp_headers), body=body)

        except ClientConnectorError as e:
            cause = getattr(e, "__cause__", None)
            is_gai = isinstance(cause, socket.gaierror)
            LOG.warning("connect error to %s: %s (gai=%s) — trying next", candidate, e, is_gai)
            last_exc = e
            continue
        except asyncio.TimeoutError as e:
            LOG.warning("timeout contacting %s: %s", candidate, e)
            last_exc = e
            continue
        except Exception as e:
            LOG.exception("unexpected upstream error for %s", candidate)
            return await _json_error(502, f"upstream error: {e}")

    # All attempts failed
    if last_exc is None:
        return await _json_error(502, "no upstream candidates available")
    if isinstance(last_exc, ClientConnectorError) and isinstance(getattr(last_exc, "__cause__", None), socket.gaierror):
        return await _json_error(502, f"DNS resolution failed for upstream candidates; last: {last_exc}")
    return await _json_error(502, f"failed to contact upstreams; last: {last_exc}")

# ────────────────────────── app factory ──────────────────────────
def make_app():
    app = web.Application()

    # Health
    app.add_routes([
        web.get("/__debug", debug),
        web.get("/ping", ping),
    ])

    # Legacy short forms + legacy v6 + modern swap/v1 + catch-all
    app.add_routes([
        web.route("*", "/quote", passthrough),
        web.route("*", "/swap", passthrough),
        web.route("*", "/swap-instructions", passthrough),
        web.route("*", "/v6/{tail:.*}", passthrough),
        web.route("*", "/swap/v1/{tail:.*}", passthrough),
        web.route("*", "/{tail:.*}", passthrough),
    ])

    family = socket.AF_INET if FORCE_IPV4 else 0
    resolver = HostMapResolver(HOSTMAP) if HOSTMAP else None

    async def on_startup(_app: web.Application):
        LOG.info(
            "starting relay; bind=%s:%s upstream=%s primary_norm=%s force_ipv4=%s alts=%s hostmap=%s",
            RELAY_BIND, RELAY_PORT, UPSTREAM, PRIMARY_NORM, FORCE_IPV4, ALT_UPSTREAMS, HOSTMAP
        )
        _app["session"] = ClientSession(
            connector=TCPConnector(family=family, resolver=resolver)
        )

    async def on_cleanup(_app: web.Application):
        LOG.info("shutting down session")
        await _app["session"].close()

    app.on_startup.append(on_startup)
    app.on_cleanup.append(on_cleanup)
    return app

# ────────────────────────── run ──────────────────────────
if __name__ == "__main__":
    web.run_app(make_app(), host=RELAY_BIND, port=RELAY_PORT)
