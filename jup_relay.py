#!/usr/bin/env python3
import os
from aiohttp import web, ClientSession, TCPConnector, ClientTimeout, ClientError
from aiohttp.abc import AbstractResolver

# ── Bind/port ──────────────────────────────────────────────────────────────────
BIND = os.getenv("RELAY_BIND", "127.0.0.1")
PORT = int(os.getenv("RELAY_PORT", "18080"))

# ── Upstream (use canonical host; we’ll pin IPs via a custom resolver) ─────────
UPSTREAM = os.getenv("JUP_UPSTREAM", os.getenv("JUP_BASE_URL", "https://quote-api.jup.ag")).rstrip("/")

# Optional: provide a comma-separated list of IPs to resolve quote-api.jup.ag to
# e.g. "104.18.24.229,104.18.25.229"
RESOLVE_IPS = [ip.strip() for ip in os.getenv("JUP_RESOLVE_IPS", "").split(",") if ip.strip()]

# Optional Jupiter key
API_KEY = os.getenv("JUP_API_KEY", "").strip()

HOP_BY_HOP = {
    "connection", "keep-alive", "proxy-authenticate", "proxy-authorization",
    "te", "trailer", "transfer-encoding", "upgrade"
}

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br, zstd",
    "Origin": "https://quote-api.jup.ag",
    "Referer": "https://quote-api.jup.ag/",
}

class StaticResolver(AbstractResolver):
    """Resolve quote-api.jup.ag to fixed IPs but keep SNI/Host as the hostname."""
    def __init__(self, host: str, addrs: list[str]):
        self.host = host
        self.addrs = addrs

    async def resolve(self, host, port=0, family=0):
        if host == self.host and self.addrs:
            return [{"hostname": host, "host": ip, "port": port, "family": family, "proto": 0, "flags": 0}
                    for ip in self.addrs]
        # Fallback to system DNS for everything else
        # Using default resolver by returning empty list signals connector to system resolve
        return []

    async def close(self):  # no resources
        return

async def make_session() -> ClientSession:
    # If RESOLVE_IPS provided, pin DNS for quote-api.jup.ag
    if RESOLVE_IPS:
        resolver = StaticResolver("quote-api.jup.ag", RESOLVE_IPS)
        connector = TCPConnector(limit=128, ttl_dns_cache=300, resolver=resolver)
    else:
        connector = TCPConnector(limit=128, ttl_dns_cache=300)
    timeout = ClientTimeout(total=15)
    return ClientSession(connector=connector, timeout=timeout)

async def on_start(app: web.Application):
    app["session"] = await make_session()

async def on_cleanup(app: web.Application):
    sess = app.get("session")
    if sess is not None and not sess.closed:
        await sess.close()

async def debug_handler(_: web.Request):
    return web.json_response({
        "ok": True,
        "upstream": UPSTREAM,
        "resolve_ips": RESOLVE_IPS or None,
        "has_api_key": bool(API_KEY),
        "note": "relay healthy",
    })

def _forward_headers(req: web.Request) -> dict:
    fwd = {k: v for k, v in req.headers.items() if k.lower() not in HOP_BY_HOP}
    for k, v in DEFAULT_HEADERS.items():
        fwd.setdefault(k, v)
    if API_KEY and "x-api-key" not in {k.lower(): v for k, v in fwd.items()}:
        fwd["x-api-key"] = API_KEY
    # Strip Proxy-* just in case
    for k in list(fwd.keys()):
        if k.lower().startswith("proxy-"):
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
            method=method, url=url, params=params, headers=headers, data=body
        ) as resp:
            out_headers = [(k, v) for k, v in resp.headers.items()
                           if k.lower() not in HOP_BY_HOP]
            out_body = await resp.read()
            return web.Response(status=resp.status, headers=out_headers, body=out_body)
    except ClientError as e:
        return web.json_response(
            {"ok": False, "error": "upstream_unreachable", "detail": str(e)}, status=502
        )
    except Exception as e:
        return web.json_response(
            {"ok": False, "error": "relay_exception", "detail": str(e)}, status=500
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
