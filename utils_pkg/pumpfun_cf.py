# pumpfun_cf.py
import asyncio, os, time, logging
from typing import Optional
from playwright.async_api import async_playwright

LOG = logging.getLogger("pumpfun.cf")

_UA = os.getenv("PUMPFUN_UA") or (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/126 Safari/537.36"
)

_COOKIE: Optional[str] = None     # "cf_clearance=...; __cf_bm=..."
_EXP: float = 0                   # epoch seconds when cookie should be refreshed
_TASK: Optional[asyncio.Task] = None


def _cookie_from(playwright_cookies) -> Optional[str]:
    jar = {c["name"]: c["value"] for c in playwright_cookies}
    parts = []
    for k in ("cf_clearance", "__cf_bm"):
        if jar.get(k):
            parts.append(f"{k}={jar[k]}")
    return "; ".join(parts) if parts else None


async def _refresh_once() -> None:
    """Open pump.fun to get/renew Cloudflare cookies, and prime the API subdomain."""
    global _COOKIE, _EXP
    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            ctx = await browser.new_context(user_agent=_UA)
            page = await ctx.new_page()

            # Hit a real page to satisfy CF challenge and set cookies
            await page.goto("https://pump.fun/advanced/scan", wait_until="networkidle", timeout=60_000)
            # Also touch the API host so cookies (or challenge) apply there as well.
            # If this 1st request is blocked, the navigation itself will complete the challenge.
            try:
                await page.goto("https://frontend-api.pump.fun/health", wait_until="domcontentloaded", timeout=20_000)
            except Exception:
                pass

            ck = _cookie_from(await ctx.cookies())
            await browser.close()

        if ck:
            _COOKIE = ck
            os.environ["PUMPFUN_COOKIE"] = ck  # optional: helps your current scanner reuse it
            _EXP = time.time() + 20 * 60       # refresh every ~20 minutes
            LOG.info("pumpfun: cf cookie refreshed")
        else:
            LOG.warning("pumpfun: no cf cookies found after refresh")
    except Exception as e:
        LOG.warning("pumpfun: cookie refresh failed: %s", e)


async def _loop() -> None:
    """Background refresher."""
    # First refresh immediately
    await _refresh_once()
    while True:
        if time.time() >= _EXP:
            await _refresh_once()
        await asyncio.sleep(300)  # check every 5 minutes


def ensure_cookie_refresher() -> None:
    """Call this from INSIDE a running asyncio loop (e.g., your async main)."""
    global _TASK
    if _TASK and not _TASK.done():
        return
    loop = asyncio.get_running_loop()
    _TASK = loop.create_task(_loop())
    LOG.info("pumpfun: cookie refresher started")


def get_cookie_header() -> str:
    """Return the current cookie string: 'cf_clearance=...; __cf_bm=...' (may be empty)."""
    return _COOKIE or os.getenv("PUMPFUN_COOKIE", "")


def flag_cookie_bad() -> None:
    """Force a refresh sooner (e.g., called when aiohttp receives 530)."""
    global _EXP
    _EXP = 0


# Browser-based JSON fetch (last-resort fallback)
async def browser_fetch_json(url: str):
    """Fetch JSON via Playwright using the current cookies (works even if aiohttp gets 530)."""
    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            ctx = await browser.new_context(user_agent=_UA)
            page = await ctx.new_page()
            # Prime cookies
            await page.goto("https://pump.fun/advanced/scan", wait_until="networkidle", timeout=60_000)
            # Now request the API JSON through the context
            r = await ctx.request.get(url, timeout=30_000)
            if r.ok:
                data = await r.json()
            else:
                data = None
            await browser.close()
            return data
    except Exception as e:
        LOG.warning("pumpfun: browser_fetch_json failed: %s", e)
        return None
