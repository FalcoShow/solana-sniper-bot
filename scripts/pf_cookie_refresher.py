# scripts/pf_cookie_refresher.py
import asyncio, os, sys
from playwright.async_api import async_playwright
from utils_pkg.pf_cookie_store import save_pf_cookies

PF_START_URL = os.getenv("PF_START_URL", "https://pump.fun/board")
UA = os.getenv("PUMPFUN_UA", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")

# How long we wait for Cloudflare to issue cf_clearance (ms total)
CF_WAIT_MS = int(os.getenv("PF_CF_WAIT_MS", "120000"))  # 2 minutes

async def main():
    async with async_playwright() as pw:
        # HEADFUL + a real UA = fewer CF challenges than headless
        browser = await pw.chromium.launch(headless=False, args=["--disable-blink-features=AutomationControlled"])
        ctx = await browser.new_context(
            user_agent=UA,
            viewport={"width": 1280, "height": 800},
            locale="en-US",
        )
        page = await ctx.new_page()

        # Don't wait for "networkidle" (PF streams a lot); use DOMContentLoaded and then poll cookies.
        await page.goto(PF_START_URL, wait_until="domcontentloaded", timeout=120_000)

        # Give CF time. If it shows a verification, just wait; many are auto-solved.
        elapsed = 0
        step = 2000
        needed = {"__cf_bm", "_cfuvid", "cf_clearance"}
        jar = {}

        while elapsed < CF_WAIT_MS:
            cookies = await ctx.cookies()
            jar = {c["name"]: c["value"] for c in cookies if c["name"] in needed or c["name"] == "auth_token"}
            if "cf_clearance" in jar:
                break
            await page.wait_for_timeout(step)
            elapsed += step

        await browser.close()

    if "cf_clearance" not in jar:
        print("Cloudflare clearance not obtained yet. Open the browser window, wait until the site finishes verifying, and run again.", file=sys.stderr)
        sys.exit(2)

    save_pf_cookies(jar)
    print("cookie refreshed:", {k: "***" for k in jar})

if __name__ == "__main__":
    asyncio.run(main())
