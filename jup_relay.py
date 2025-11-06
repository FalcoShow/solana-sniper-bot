#!/usr/bin/env python3
# jup_relay.py — robust IPv4-friendly relay for Jupiter v6
# - Listens on RELAY_BIND:RELAY_PORT (defaults 127.0.0.1:8081)
# - RELAY_UPSTREAM must end with /v6 (e.g. https://quote-api.jup.ag/v6)
# - Accepts local paths like /quote and forwards to {UPSTREAM}/quote
# - Adds x-api-key if JUP_API_KEY is set
# - Has /__debug and /ping health endpoints
# - DNS fallback: if primary host fails to resolve we'll retry with fallbacks

from __future__ import annotations

import os
import socket
import asyncio
import logging
from typing import List, Optional
from urllib.parse import urljoin, urlparse

from aiohttp import web, ClientSession, TCPConnector, ClientTimeout
from aiohttp.client_exceptions import ClientConnectorError

# ---------- config ----------
LOG = logging.getLogger("jup_relay")
logging.basicConfig(level=logging.INFO, format="[relay] %(asctime)s %(levelname)s %(message)s")

RELAY_BIND = os.environ.get("RELAY_BIND", "127.0.0.1")
RELAY_PORT = int(os.environ.get("RELAY_PORT", "8081"))
UPSTREAM = os.environ.get("RELAY_UPSTREAM", "https://quote-api.jup.ag/v6").rstrip("/")
JUP_API_KEY = os.environ.get("JUP_API_KEY", "")
FORCE_IPV4 = os.environ.get("RELAY_FORCE_IPV4", "1") == "1"
# optional comma separated additional base urls to try if primary fails
JUP_ALT_BASE_URLS = os.environ.get("JUP_ALT_BASE_URLS", "").strip()

# ---------- validation ----------
if not UPSTREAM.endswith("/v6"):
    raise SystemExit(f"RELAY_UPSTREAM must end with /v6, got: {UPSTREAM}")

def _alt_list() -> List[str]:
    out = []
    if JUP_ALT_BASE_URLS:
        for p in JUP_ALT_BASE_URLS.split(","):
            s = p.strip()
            if s:
                out.append(s.rstrip("/"))
    # add a canonical fallback for quote-api -> api host if not present already
    parsed = urlparse(UPSTREAM)
    if "quote-api.jup.ag" in parsed.netloc and "https://api.jup.ag/v6" not in out:
        out.append("https://api.jup.ag/v6")
    return out

ALT_UPSTREAMS = _alt_list()

# ---------- helpers ----------
async def _json_error(status: int, message: str):
    return web.json_response({"ok": False, "error": message}, status=status)

def _sanitize_request_headers(headers: dict) -> dict:
    h = dict(headers)
    # remove hop-by-hop and host
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

def _build_try_list(primary: str) -> List[str]:
    out = [primary.rstrip("/")]
    out.extend(ALT_UPSTREAMS)
    # dedupe while preserving order
    seen = set()
    dedup = []
    for u in out:
        if u not in seen:
            seen.add(u)
            dedup.append(u)
    return dedup

# ---------- handlers ----------
async def ping(_req: web.Request):
    return web.json_response({"ok": True, "relay": "up"})

async def debug(_req: web.Request):
    return web.json_response({
        "bind": RELAY_BIND,
        "port": RELAY_PORT,
        "upstream": UPSTREAM,
        "alt_upstreams": ALT_UPSTREAMS,
        "api_key_present": bool(JUP_API_KEY),
        "force_ipv4": FORCE_IPV4,
    })

async def _do_upstream_request(session: ClientSession, method: str, url: str, headers: dict, data: Optional[bytes], timeout_seconds: int = 12):
    timeout = ClientTimeout(total=timeout_seconds)
    async with session.request(method=method, url=url, headers=headers, data=data, allow_redirects=False, timeout=timeout) as resp:
        body = await resp.read()
        passthrough_headers = _safe_passthrough_headers(resp.headers)
        return resp.status, passthrough_headers, body

async def passthrough(request: web.Request):
    # Map "/quote" -> "{UPSTREAM}/quote", etc.
    tail = request.path.lstrip("/")  # e.g. "quote" or "v6/quote" if client includes it
    # try the incoming path appended to the base upstream
    try_list = _build_try_list(UPSTREAM)

    # If the client accidentally sent a full path like /v6/quote, we shouldn't end up with .../v6/v6/quote;
    # keep the simple behavior of appending the tail to the upstream base.
    url_candidates = []
    for base in try_list:
        url_candidates.append(urljoin(base + "/", tail))

    # preserve original query string
    if request.query_string:
        url_candidates = [f"{u}?{request.query_string}" for u in url_candidates]

    headers = _sanitize_request_headers(dict(request.headers))
    # inject api key if not present
    if JUP_API_KEY and not any(h.lower() == "x-api-key" for h in headers.keys()):
        headers["x-api-key"] = JUP_API_KEY

    # read body
    data = await request.read() if request.can_read_body else None

    session: ClientSession = request.app["session"]

    last_exc = None
    for candidate in url_candidates:
        LOG.info("proxy -> %s", candidate)
        try:
            status, resp_headers, body = await _do_upstream_request(session, request.method, candidate, headers, data)
            return web.Response(status=status, headers=dict(resp_headers), body=body)
        except ClientConnectorError as e:
            # If underlying cause is a DNS getaddrinfo failure, try next fallback
            cause = getattr(e, "__cause__", None)
            is_gai = isinstance(cause, socket.gaierror)
            LOG.warning("connect error to upstream %s: %s (gai=%s) — trying next fallback if available", candidate, e, is_gai)
            last_exc = e
            # continue to next candidate
            continue
        except asyncio.TimeoutError as e:
            LOG.warning("timeout contacting upstream %s: %s", candidate, e)
            last_exc = e
            continue
        except Exception as e:  # unexpected, bubble as 502
            LOG.exception("unexpected error when contacting upstream %s", candidate)
            return await _json_error(502, f"upstream error: {e}")

    # all attempts failed
    if last_exc is None:
        return await _json_error(502, "no upstream candidates available")
    # If last_exc was DNS related, include that hint for faster debugging
    if isinstance(last_exc, ClientConnectorError) and isinstance(getattr(last_exc, "__cause__", None), socket.gaierror):
        return await _json_error(502, f"DNS resolution failed for upstream candidates; last: {last_exc}")
    return await _json_error(502, f"failed to contact upstreams; last: {last_exc}")

# ---------- app factory ----------
def make_app():
    app = web.Application()

    # health endpoints
    app.add_routes([
        web.get("/__debug", debug),
        web.get("/ping", ping),
    ])

    # v6 endpoints we care about; include catch-all -> passthrough too
    app.add_routes([
        web.route("*", "/quote", passthrough),
        web.route("*", "/swap", passthrough),
        web.route("*", "/swap-instructions", passthrough),
        web.route("*", "/price", passthrough),
        web.route("*", "/tokens", passthrough),
        web.route("*", "/routes", passthrough),
        web.route("*", "/{tail:.*}", passthrough),  # catch-all
    ])

    # create/close HTTP client inside event loop
    family = socket.AF_INET if FORCE_IPV4 else 0

    async def on_startup(_app: web.Application):
        LOG.info("starting relay; bind=%s:%s upstream=%s force_ipv4=%s alts=%s", RELAY_BIND, RELAY_PORT, UPSTREAM, FORCE_IPV4, ALT_UPSTREAMS)
        _app["session"] = ClientSession(connector=TCPConnector(family=family))

    async def on_cleanup(_app: web.Application):
        LOG.info("shutting down session")
        await _app["session"].close()

    app.on_startup.append(on_startup)
    app.on_cleanup.append(on_cleanup)
    return app

# ---------- run ----------
if __name__ == "__main__":
    web.run_app(make_app(), host=RELAY_BIND, port=RELAY_PORT)
