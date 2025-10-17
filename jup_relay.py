#!/usr/bin/env python3
import os, socket, ssl, traceback
from urllib.parse import urlparse
from aiohttp import web, ClientSession, TCPConnector, ClientTimeout
from aiohttp.resolver import ThreadedResolver

BIND = os.getenv("RELAY_BIND", "127.0.0.1")
PORT = int(os.getenv("RELAY_PORT", "18080"))

UPSTREAM = os.getenv("JUP_UPSTREAM", os.getenv("JUP_BASE_URL", "https://public.jupiterapi.com")).rstrip("/")
API_KEY = os.getenv("JUP_API_KEY", "").strip()
RESOLVE_IPS = [ip.strip() for ip in os.getenv("JUP_RESOLVE_IPS", "").split(",") if ip.strip()]
DEBUG = os.getenv("RELAY_DEBUG", "0") == "1"

HOP_BY_HOP = {"connection","keep-alive","proxy-authenticate","proxy-authorization","te","trailer","transfer-encoding","upgrade"}

DEFAULT_HEADERS = {
    "User-Agent": ("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                   "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br, zstd",
    "Origin": "https://quote-api.jup.ag",
    "Referer": "https://quote-api.jup.ag/",
}

def log(msg: str):
    with open("/tmp/jup_relay.log", "a", encoding="utf-8") as f:
        f.write(msg.rstrip() + "\n")

class PinnedResolver(ThreadedResolver):
    """Pin hostname to provided IPs without aiodns."""
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
    ssl_ctx = ssl.create_default_context()
    connector = TCPConnector(limit=100, ttl_dns_cache=300, resolver=resolver, ssl=ssl_ctx)
    timeout = ClientTimeout(total=20)
    return ClientSession(connector=connector, timeout=timeout)

async def on_start(app: web.Application):
    app["session"] = await make_session(UPSTREAM)
    log(f"[relay] start ok upstream={UPSTREAM} ips={RESOLVE_IPS} api_key={'yes' if API_KEY else 'no'}")

async def on_cleanup(app: web.Application):
    await app["session"].close()
    log("[relay] cleanup")

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
    try:
        body = await request.read()
        headers = _forward_headers(request)

        if DEBUG:
            log(f"[proxy] -> {url} q={dict(request.rel_url.query)}")

        async with request.app["session"].request(
            method=request.method,
            url=url,
            params=request.rel_url.query,
            headers=headers,
            data=body,
            allow_redirects=False,
        ) as resp:
            out_headers = [(k, v) for k, v in resp.headers.items() if k.lower() not in HOP_BY_HOP]
            out_body = await resp.read()

            if DEBUG or resp.status >= 400:
                log(f"[proxy] <- {resp.status} len={len(out_body)} via={url}")

            return web.Response(status=resp.status, headers=out_headers, body=out_body)

    except Exception as e:
        tb = traceback.format_exc()
        log(f"[proxy][ERR] {e}\n{tb}")
        return web.json_response(
            {"ok": False, "error": str(e), "upstream": UPSTREAM},
            status=502
        )

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
