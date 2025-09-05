import asyncio, os
from playwright.async_api import async_playwright

async def main():
    print("apex via proxy?", os.getenv("PUMPFUN_APEX_VIA_PROXY"))
    proxy = os.getenv("PLAYWRIGHT_PROXY")
    async with async_playwright() as p:
        # Apex: should launch WITHOUT proxy
        b1 = await p.chromium.launch(headless=True, proxy=None)
        c1 = await b1.new_context()
        p1 = await c1.new_page()
        await p1.goto("https://pump.fun/board", timeout=120000, wait_until="domcontentloaded")
        print("apex title:", await p1.title())
        await b1.close()

        # API: should launch WITH proxy
        if proxy:
            b2 = await p.chromium.launch(headless=True, proxy={"server": proxy})
            c2 = await b2.new_context()
            r = await c2.request.get("https://frontend-api.pump.fun/health", timeout=30000)
            print("api /health:", r.status)
            await b2.close()

asyncio.run(main())
