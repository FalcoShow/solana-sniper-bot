# jup_relay.py — lean IPv4-only relay for Jupiter v6
# - Listens on RELAY_BIND:RELAY_PORT (defaults 127.0.0.1:8081)
# - RELAY_UPSTREAM must be a base that ENDS WITH /v6, e.g. https://quote-api.jup.ag/v6
# - Accepts local paths like /quote and translates to {UPSTREAM}/quote
# - Adds x-api-key if JUP_API_KEY is set
# - Has /__debug and /ping local endpoints for health

import os, asyncio, socket
from urllib.parse import urljoin, urlencode
from aiohttp import web, ClientSession, TCPConnector

RELAY_BIND   = os.environ.get("RELAY_BIND", "127.0.0.1")
RELAY_PORT   = int(os.environ.get("RELAY_PORT", "8081"))
UPSTREAM     = os.environ.get("RELAY_UPSTREAM", "https://quote-api.jup.ag/v6").rstrip("/")
JUP_API_KEY  = os.environ.get("JUP_API_KEY", "")
FORCE_IPV4   = os.environ.get("RELAY_FORCE_IPV4", "1") == "1"

if not UPSTREAM.endswith("/v6"):
    # Be strict so we never hit /quote without the /v6 prefix upstream
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
    # map /quote -> {UPSTREAM}/quote, etc.
    tail = request.path.lstrip("/")         # e.g. "quote"
    url  = urljoin(UPSTREAM + "/", tail)    # e.g. https://.../v6/quote

    # rebuild query string
    if request.query:
        url = f"{url}?{urlencode(list(request.query.items()))}"

    headers = dict(request.headers)
    # ensure we forward API key
    if JUP_API_KEY:
        headers["x-api-key"] = JUP_API_KEY

    # avoid host leakage
    headers.pop("host", None)

    data = await request.read() if request.can_read_body else None

    async with request.app["session"].request(
        method=request.method,
        url=url,
        headers=headers,
        data=data,
        allow_redirects=False,
        timeout=10,
    ) as resp:
        body = await resp.read()
        # Pass through status + JSON/text body from Jupiter.
        h = [(k, v) for k, v in resp.headers.items()
             if k.lower() not in {"content-length", "content-encoding", "transfer-encoding", "connection"}]
        return web.Response(status=resp.status, headers=h, body=body)

def make_app():
    app = web.Application()
    # health
    app.add_routes([
        web.get("/__debug", debug),
        web.get("/ping", ping),
    ])
    # v6 entrypoints we care about; default catch-all → passthrough too
    app.add_routes([
        web.route("*", "/quote", passthrough),
        web.route("*", "/swap", passthrough),
        web.route("*", "/swap-instructions", passthrough),
        web.route("*", "/price", passthrough),
        web.route("*", "/tokens", passthrough),
        web.route("*", "/routes", passthrough),
        web.route("*", "/{tail:.*}", passthrough),
    ])
    # force IPv4 if requested
    family = socket.AF_INET if FORCE_IPV4 else 0
    app["session"] = ClientSession(connector=TCPConnector(family=family, ssl=False))
    async def cleanup(_):
        await app["session"].close()
    app.on_cleanup.append(cleanup)
    return app

if __name__ == "__main__":
    web.run_app(make_app(), host=RELAY_BIND, port=RELAY_PORT)
