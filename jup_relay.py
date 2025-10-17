#!/usr/bin/env python3
import os, socket, ssl
from urllib.parse import urlparse
from aiohttp import web, ClientSession, TCPConnector, ClientTimeout
from aiohttp.resolver import ThreadedResolver

BIND = os.getenv("RELAY_BIND", "127.0.0.1")
PORT = int(os.getenv("RELAY_PORT", "18080"))

UPSTREAM = os.getenv("JUP_UPSTREAM", os.getenv("JUP_BASE_URL", "https://public.jupiterapi.com")).rstrip("/")
API_KEY = os.getenv("JUP_API_KEY", "").strip()
RESOLVE_IPS = [ip.strip() for ip in os.getenv("JUP_RESOLVE_IPS", "").split(",") if ip.strip()]

HOP_BY_HOP = {
    "connection","keep-alive","proxy-authenticate","proxy-authorization",
    "te","trailer","transfer-encoding","upgrade"
}

DEFAULT_HEADERS = {
    "User-Agent": ("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                   "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br, zstd",
    "Origin": "https://quote-api.jup.ag",
    "Referer": "https://quote-api.jup.ag/",
}

class PinnedResolver(ThreadedResolver):
    """Pin hostname to provided IPs without extra deps."""
    def __init__(self, hostname: str, ips: list[str]):
        super().__init__()
        self.hostname = hostname
        self.ips = ips

    async def resolve(self, host, port=0, family=socket.AF_INET):
        if self.ips and host == self.hostname:
            return [{"hostname": host, "host": ip, "port": port,
                     "family": family, "proto": 0, "flags": 0}
                    for ip in self.ips]
        return await super().resolve(host, port, family)

async def make_session(upstream: str) -> ClientSession:
    parsed = urlparse(upstream)
    hostname = parsed.hostname or "quote-api.jup.ag"

    resolver = PinnedResolver(hostname, RESOLVE_IPS) if RESOLVE_IPS else ThreadedResolver()

    # Keep SNI as the hostname; aiohttp uses the URL's host for SNI
    ssl_ctx = ssl.create_default_context()
    connector = TCPConnector(limit=100, ttl_dns_cache=300, resolver=resolver, ssl=ssl_ctx)
    timeout = ClientTimeout(total=20)
    return ClientSession(connector=connector, timeout=timeout)

async def on_start(app: web.Application):
    app["session"] = await make_session(UPSTREAM)

async def on_cleanup(app: web.Application):
    await app["session"].close()

async def debug_handler(_: web.Request):
    return web.json_response({
        "ok": True,
        "upstream": UPSTREAM,
        "resolve_ips": RESOLVE_IPS or None,
        "has_api_key": bool(API_KEY),
        "note": "relay healthy"
    })

def _forward_headers(req: web.Request):
    fwd = {k: v for k, v in req.headers.items() if k.lower() not in HOP_BY_HOP}
    for k, v in DEFAULT_HEADERS.items():
        fwd.setdefault(k, v)

    host = urlparse(UPSTREAM).hostname or "quote-api.jup.ag"
    fwd["Host"] = host

    if API_KEY and "x-api-key" not in {k.lower(): v for k, v in fwd.items()}:
        fwd["x-api-key"] = API_KEY
    return fwd

async def proxy_handler(request: web.Request):
    tail = request.match_info["tail"]
    url = f"{UPSTREAM}/{tail}"
    body = await request.read()
    headers = _forward_headers(request)

    async with request.app["session"].request(
        method=request.method,
        url=url,
        params=request.rel_url.query,
        headers=headers,
        data=body
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
