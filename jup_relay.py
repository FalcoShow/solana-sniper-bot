# jup_relay.py — lean IPv4-only relay for Jupiter v6
# - Listens on RELAY_BIND:RELAY_PORT (defaults 127.0.0.1:8081)
# - RELAY_UPSTREAM must end with /v6 (e.g. https://quote-api.jup.ag/v6)
# - Accepts local paths like /quote and forwards to {UPSTREAM}/quote
# - Adds x-api-key if JUP_API_KEY is set
# - Has /__debug and /ping health endpoints

import os
import socket
from urllib.parse import urljoin, urlencode

from aiohttp import web, ClientSession, TCPConnector, ClientTimeout

RELAY_BIND = os.environ.get("RELAY_BIND", "127.0.0.1")
RELAY_PORT = int(os.environ.get("RELAY_PORT", "8081"))
UPSTREAM   = os.environ.get("RELAY_UPSTREAM", "https://quote-api.jup.ag/v6").rstrip("/")
JUP_API_KEY = os.environ.get("JUP_API_KEY", "")
FORCE_IPV4  = os.environ.get("RELAY_FORCE_IPV4", "1") == "1"

if not UPSTREAM.endswith("/v6"):
    # Be strict so we never hit /quote without the v6 prefix upstream
    raise SystemExit(f"RELAY_UPSTREAM must end with /v6, got: {UPSTREAM}")

async def ping(_):
    return web.json_response({"ok": True, "relay": "up"})

async def debug(_):
    return web.json_response({
        "bind": RELAY_BIND,
        "port": RELAY_PORT,
        "upstream": UPSTREAM,
        "api_key_present": bool(JUP_API_KEY),
        "force_ipv4": FORCE_IPV4,
    })

async def passthrough(request: web.Request):
    # Map "/quote" -> "{UPSTREAM}/quote", etc.
    tail = request.path.lstrip("/")       # e.g. "quote"
    url  = urljoin(UPSTREAM + "/", tail)  # e.g. https://.../v6/quote

    # Keep original query string
    if request.query_string:
        url = f"{url}?{request.query_string}"

    # Copy incoming headers; scrub hop-by-hop & host
    headers = dict(request.headers)
    headers.pop("host", None)
    for k in list(headers.keys()):
        if k.lower() in ("content-length", "transfer-encoding", "connection"):
            headers.pop(k, None)

    # Inject API key if provided and not already present
    if JUP_API_KEY and "x-api-key" not in (h.lower() for h in headers.keys()):
        headers["x-api-key"] = JUP_API_KEY

    # Read body if present
    data = await request.read() if request.can_read_body else None

    timeout = ClientTimeout(total=12)
    async with request.app["session"].request(
        method=request.method,
        url=url,
        headers=headers,
        data=data,
        allow_redirects=False,
        timeout=timeout,
    ) as resp:
        body = await resp.read()
        # Pass through safe headers (let aiohttp set content-length)
        passthrough_headers = [
            (k, v) for k, v in resp.headers.items()
            if k.lower() not in ("content-length", "content-encoding",
                                 "transfer-encoding", "connection")
        ]
        return web.Response(status=resp.status, headers=passthrough_headers, body=body)

def make_app():
    app = web.Application()

    # health
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

    # Properly create/close the HTTP client inside the loop
    family = socket.AF_INET if FORCE_IPV4 else 0

    async def on_startup(_app):
        _app["session"] = ClientSession(
            connector=TCPConnector(family=family, ssl=False)
        )

    async def on_cleanup(_app):
        await _app["session"].close()

    app.on_startup.append(on_startup)
    app.on_cleanup.append(on_cleanup)
    return app

if __name__ == "__main__":
    web.run_app(make_app(), host=RELAY_BIND, port=RELAY_PORT)
