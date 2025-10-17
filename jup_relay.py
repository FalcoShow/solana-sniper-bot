#!/usr/bin/env python3
import os
from aiohttp import web, ClientSession, TCPConnector, ClientTimeout

BIND = os.getenv("RELAY_BIND", "127.0.0.1")
PORT = int(os.getenv("RELAY_PORT", "18080"))

# Where we forward traffic. Default to public Jupiter edge (stable DNS),
# you can set JUP_UPSTREAM to override (e.g. https://quote-api.jup.ag)
UPSTREAM = os.getenv("JUP_UPSTREAM", os.getenv("JUP_BASE_URL", "https://public.jupiterapi.com")).rstrip("/")
HOST_OVERRIDE = os.getenv("JUP_HOST_HEADER", "").strip()  # e.g. quote-api.jup.ag to force Host header

HOP_BY_HOP = {
    "connection", "keep-alive", "proxy-authenticate", "proxy-authorization",
    "te", "trailer", "transfer-encoding", "upgrade"
}

async def make_session() -> ClientSession:
    # If you need to pin IPs/SNI tricks, do it with a custom connector here.
    connector = TCPConnector(limit=100, ttl_dns_cache=300)
    timeout = ClientTimeout(total=15)
    return ClientSession(connector=connector, timeout=timeout)

async def on_start(app: web.Application):
    app["session"] = await make_session()

async def on_cleanup(app: web.Application):
    await app["session"].close()

async def debug_handler(_: web.Request):
    return web.json_response({
        "ok": True, "upstream": UPSTREAM, "host_override": HOST_OVERRIDE
    })

async def proxy_handler(request: web.Request):
    tail = request.match_info["tail"]
    url = f"{UPSTREAM}/{tail}"
    # Copy headers and optionally force Host
    fwd_headers = {k: v for k, v in request.headers.items() if k.lower() not in HOP_BY_HOP}
    if HOST_OVERRIDE:
        fwd_headers["Host"] = HOST_OVERRIDE

    body = await request.read()
    async with request.app["session"].request(
        method=request.method,
        url=url,
        params=request.rel_url.query,
        headers=fwd_headers,
        data=body,
    ) as resp:
        out_headers = [(k, v) for k, v in resp.headers.items() if k.lower() not in HOP_BY_HOP]
        out_body = await resp.read()
        return web.Response(status=resp.status, headers=out_headers, body=out_body)

def build_app() -> web.Application:
    app = web.Application()
    app.on_startup.append(on_start)
    app.on_cleanup.append(on_cleanup)
    app.add_routes([
        web.get("/__debug", debug_handler),
        web.route("*", "/{tail:.*}", proxy_handler),
    ])
    return app

def main():
    app = build_app()
    web.run_app(app, host=BIND, port=PORT)

if __name__ == "__main__":
    main()
