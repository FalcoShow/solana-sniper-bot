#!/usr/bin/env python3
import os
from aiohttp import web, ClientSession, TCPConnector, ClientTimeout, ClientError

# ── Bind/port for the local relay ──────────────────────────────────────────────
BIND = os.getenv("RELAY_BIND", "127.0.0.1")
PORT = int(os.getenv("RELAY_PORT", "18080"))

# ── Upstream Jupiter endpoint (default to canonical v6 host) ──────────────────
# You can override with:
#   JUP_UPSTREAM=https://quote-api.jup.ag               (recommended)
#   JUP_UPSTREAM=https://public.jupiterapi.com          (alt edge)
UPSTREAM = os.getenv("JUP_UPSTREAM", os.getenv("JUP_BASE_URL", "https://quote-api.jup.ag")).rstrip("/")

# If you *must* pin an IP upstream (not recommended), set this so TLS Host header is correct:
#   JUP_HOST_HEADER=quote-api.jup.ag
HOST_OVERRIDE = os.getenv("JUP_HOST_HEADER", "").strip()

# Optional Jupiter key, forwarded if provided
API_KEY = os.getenv("JUP_API_KEY", "").strip()

# Headers we never forward hop-by-hop
HOP_BY_HOP = {
    "connection", "keep-alive", "proxy-authenticate", "proxy-authorization",
    "te", "trailer", "transfer-encoding", "upgrade"
}

# Friendly defaults to avoid Cloudflare/WAF bot blocks
DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
}

async def make_session() -> ClientSession:
    # DNS cached; sane connection limits; modest overall timeout.
    connector = TCPConnector(limit=128, ttl_dns_cache=300)
    timeout = ClientTimeout(total=15)
    return ClientSession(connector=connector, timeout=timeout)

async def on_start(app: web.Application):
    app["session"] = await make_session()

async def on_cleanup(app: web.Application):
    # Close if created
    sess = app.get("session")
    if sess is not None and not sess.closed:
        await sess.close()

async def debug_handler(_: web.Request):
    return web.json_response({
        "ok": True,
        "upstream": UPSTREAM,
        "host_override": HOST_OVERRIDE or None,
        "has_api_key": bool(API_KEY),
        "note": "relay healthy",
    })

def _forward_headers(req: web.Request) -> dict:
    # Copy incoming headers minus hop-by-hop ones
    fwd = {k: v for k, v in req.headers.items() if k.lower() not in HOP_BY_HOP}

    # Ensure browsery defaults to dodge WAF 403s if caller didn't set them
    for k, v in DEFAULT_HEADERS.items():
        fwd.setdefault(k, v)

    # Host override (only needed when upstream is a raw IP)
    if HOST_OVERRIDE:
        fwd["Host"] = HOST_OVERRIDE

    # Optional Jupiter API key
    if API_KEY and "x-api-key" not in {k.lower(): v for k, v in fwd.items()}:
        fwd["x-api-key"] = API_KEY

    # Strip any Proxy-* headers that might cause trouble
    keys_to_strip = [k for k in fwd if k.lower().startswith("proxy-")]
    for k in keys_to_strip:
        fwd.pop(k, None)

    return fwd

async def proxy_handler(request: web.Request):
    tail = request.match_info["tail"]
    url = f"{UPSTREAM}/{tail}"
    method = request.method
    params = request.rel_url.query
    headers = _forward_headers(request)
    body = await request.read()

    try:
        async with request.app["session"].request(
            method=method,
            url=url,
            params=params,
            headers=headers,
            data=body,
        ) as resp:
            # Pass most headers through, minus hop-by-hop
            out_headers = [(k, v) for k, v in resp.headers.items()
                           if k.lower() not in HOP_BY_HOP]
            out_body = await resp.read()
            return web.Response(status=resp.status, headers=out_headers, body=out_body)

    except ClientError as e:
        # Network/protocol error to upstream
        return web.json_response(
            {"ok": False, "error": "upstream_unreachable", "detail": str(e)},
            status=502,
        )
    except Exception as e:
        # Anything else: 500
        return web.json_response(
            {"ok": False, "error": "relay_exception", "detail": str(e)},
            status=500,
        )

def build_app() -> web.Application:
    app = web.Application()
    app.on_startup.append(on_start)
    app.on_cleanup.append(on_cleanup)
    app.router.add_get("/__debug", debug_handler)
    app.router.add_route("*", "/{tail:.*}", proxy_handler)
    return app

def main():
    app = build_app()
    web.run_app(app, host=BIND, port=PORT)

if __name__ == "__main__":
    main()
