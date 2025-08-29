# utils_pkg/pumpfun_cf.py
import os, asyncio, logging
from playwright.async_api import async_playwright

log = logging.getLogger("pumpfun_cf")
SCAN_URL = "https://pump.fun/advanced/scan"

async def _fetch_cookie_once() -> str:
    ua = os.getenv("PUMPFUN_UA", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126 Safari/537.36")
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, args=[
            "--no-sandbox", "--disable-dev-shm-usage", "--disable-gpu"
        ])
        ctx = await browser.new_context(user_agent=ua)
        page = await ctx.new_page()
        # Hit the normal UI—Cloudflare will challenge once and set cf_clearance
        await page.goto(SCAN_URL, wait_until="domcontentloaded", timeout=60000)
        # give CF a moment to complete any JS checks
        await page.wait_for_timeout(4000)
        cookies = await ctx.cookies()
        await browser.close()

    jar = {c["name"]: c["value"] for c in cookies if c["domain"].endswith(".pump.fun")}
    cf = jar.get("cf_clearance")
    bm = jar.get("__cf_bm")
    if not cf:
        raise RuntimeError("Could not obtain cf_clearance from Cloudflare")
    return f"cf_clearance={cf}" + (f"; __cf_bm={bm}" if bm else "")

async def refresh_loop(redis, interval_min: int | None = None):
    if interval_min is None:
        interval_min = int(os.getenv("PUMPFUN_REFRESH_MIN", "60"))
    while True:
        try:
            cookie = await _fetch_cookie_once()
            # cache for 8 hours; refresh every interval_min anyway
            try:
                await redis.set("pumpfun:cookie", cookie, ex=60*60*8)
            except Exception:
                pass
            os.environ["PUMPFUN_COOKIE"] = cookie  # hot-path for current process
            log.info("pumpfun_cf: refreshed Cloudflare cookie (len=%d)", len(cookie))
        except Exception as e:
            log.warning("pumpfun_cf: refresh failed: %s", e)
        await asyncio.sleep(interval_min * 60)
