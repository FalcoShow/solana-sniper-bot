#!/usr/bin/env python3
import os, asyncio
from aiohttp import web, ClientSession, TCPConnector
from aiohttp.abc import AbstractResolver

JUP_HOST = os.environ.get("JUP_HOST", "quote-api.jup.ag")
# Comma-separated list of Cloudflare IPs for quote-api.jup.ag
JUP_IPS = [ip.strip() for ip in os.environ.get("JUP_IPS", "104.18.24.229,104.18.25.229").split(",") if ip.strip()]
BIND = os.environ.get("RELAY_BIND", "127.0.0.1")
PORT = int(os.environ.get("RELAY_PORT", "18080"))

class StaticResolver(AbstractResolver):
    def __init__(self, mapping):
        self._mapping = mapping
    async def resolve(self, host, port=0, family=0):
        # Always return our pinned IPs for the target host
        if host == JUP_HOST:
            return [{"hostname": host, "host": ip, "port": port, "family": family, "proto": 0, "flags": 0} for ip in JUP_IPS]
        # default: let the system resolver handle others
        return [{"hostname": host, "host": host, "port": port, "family": family, "proto": 0, "flags": 0}]
    async def close(self): pass

async def make_session():
    # Using a custom resolver returns the IPs, but aiohttp still sends SNI=hostname
    connector = TCPConnector(resolver=StaticResolver({}), ssl=True, limit=100)
    return ClientSession(connector=connector, timeout=web.ClientTimeout(total=10))

async def proxy(request: web.Request):
    tail = request.match_info["tail"]
    qs = request.query_string
    url = f"https://{JUP_HOST}/{tail}"
    if qs:
        url += f"?{qs}"
    async with request.app["session"].get(url, headers=request.headers) as resp:
        data = await resp.read()
        return web.Response(body=data, status=resp.status, headers=resp.headers)

async def debug(request: web.Request):
    return web.json_response({"host": JUP_HOST, "ips": JUP_IPS})

async def on_start(app):
    app["session"] = await make_session()

async def on_cleanup(app):
    await app["session"].close()

def main():
    app = web.Application()
    app.on_startup.append(on_start)
    app.on_cleanup.append(on_cleanup)
    app.router.add_get("/__debug", debug)
    app.router.add_route("*", "/{tail:.*}", proxy)
    web.run_app(app, host=BIND, port=PORT)

if __name__ == "__main__":
    main()
