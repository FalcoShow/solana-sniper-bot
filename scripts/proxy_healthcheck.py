# scripts/proxy_healthcheck.py
import asyncio, aiohttp
from utils_pkg.proxy_client import proxy_cfg

URL = "https://api.ipify.org?format=json"

async def main():
    proxy, proxy_auth, insecure = proxy_cfg()  # now matches the new signature
    if not proxy:
        raise SystemExit("Proxy env vars missing (PROXY_HOSTPORT / PROXY_USER / PROXY_PASS)")

    timeout = aiohttp.ClientTimeout(total=20)
    connector = aiohttp.TCPConnector(ssl=False) if insecure else None

    async with aiohttp.ClientSession(timeout=timeout, connector=connector) as s:
        async with s.get(URL, proxy=proxy, proxy_auth=proxy_auth) as r:
            print(r.status, await r.text())

if __name__ == "__main__":
    asyncio.run(main())
