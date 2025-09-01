# utils_pkg/pumpfun_cf.py
"""
Periodic Cloudflare-cookie refresher for Pump.fun, using Playwright (Chromium).

Public API:
- refresh_loop():        awaitable infinite loop; schedule with asyncio.create_task(...)
- ensure_cookie_refresher(): start the loop once per process if an event loop is running
- get_cookie_header():   returns current cookie string "cf_clearance=...; __cf_bm=..."
- flag_cookie_bad():     force a refresh ASAP (e.g., after HTTP 530)
- browser_fetch_json():  last-resort JSON fetch using Playwright context

Env knobs:
- PUMPFUN_UA                       : User-Agent override
- PUMPFUN_CF_URL                   : page we open to satisfy CF (default: /advanced/scan)
- PUMPFUN_CF_REFRESH_SEC           : refresh cadence seconds (default: 900)
"""

from __future__ import annotations

import asyncio
import logging
import os
import time
from typing import Optional, Dict, Any

from playwright.async_api import async_playwright

log = logging.getLogger("pumpfun.cf")

# -------------------- config --------------------

UA = os.getenv(
    "PUMPFUN_UA",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/126 Safari/537.36",
)

TARGET_URL = os.getenv("PUMPFUN_CF_URL", "https://pump.fun/advanced/scan")
REFRESH_SEC = int(os.getenv("PUMPFUN_CF_REFRESH_SEC", "900"))  # default 15 min

# -------------------- state ---------------------

_COOKIE: Optional[str] = None  # "cf_clearance=...; __cf_bm=..."
_EXP: float = 0.0              # epoch seconds when cookie should be refreshed
_TASK: Optional[asyncio.Task] = None


# -------------------- helpers -------------------

def _cookie_from(playwright_cookies: list[Dict[str, Any]]) -> Optional[str]:
    jar = {c.get("name"): c.get("value") for c in playwright_cookies or []}
    parts = []
    for k in ("cf_clearance", "__cf_bm"):
        v = jar.get(k)
        if v:
            parts.append(f"{k}={v}")
    return "; ".join(parts) if parts else None


async def _refresh_once() -> None:
    """
    Open Pump.fun with Playwright to satisfy Cloudflare, collect cookies,
    and prime the API host. Stores cookie into process env for scanners.
    """
    global _COOKIE, _EXP
    try:
        async with async_playwright() as p:
            # container-friendly flags
            browser = await p.chromium.launch(
                headless=True,
                args=["--no-sandbox", "--disable-dev-shm-usage"]
            )
            try:
                ctx = await browser.new_context(user_agent=UA)
                page = await ctx.new_page()

                # Hit the real page so CF sets/validates cookies
                await page.goto(TARGET_URL, wait_until="networkidle", timeout=60_000)

                # Touch the API host; if first call is blocked, the previous nav
                # normally completes the challenge and cookies will work next call.
                try:
                    await page.goto(
                        "https://frontend-api.pump.fun/health",
                        wait_until="domcontentloaded",
                        timeout=20_000,
                    )
                except Exception:
                    # health endpoint sometimes 5xx/blocked — not fatal
                    pass

                ck = _cookie_from(await ctx.cookies())
            finally:
                await browser.close()

        if ck:
            _COOKIE = ck
            os.environ["PUMPFUN_COOKIE"] = ck  # scanners can read this directly
            _EXP = time.time() + max(60.0, float(REFRESH_SEC) * 0.9)  # refresh a bit early
            redacted = "cf_clearance=***" + ("" if "__cf_bm=" not in ck else "; __cf_bm=***")
            log.info("CF refresh: got cookies (%s), next refresh in ~%ds", redacted, REFRESH_SEC)
        else:
            # No cookie means CF didn’t set it (rare), try again soon
            _COOKIE = None
            _EXP = time.time() + 120.0
            log.warning("CF refresh: no cookies found; will retry sooner")
    except Exception as e:
        # Back off a bit on errors
        _EXP = time.time() + 120.0
        log.warning("CF refresh failed: %s", e)


# -------------------- public API ----------------

async def refresh_loop() -> None:
    """
    Background refresher; await or schedule via `asyncio.create_task(refresh_loop())`.
    """
    # Do an immediate refresh on start
    await _refresh_once()
    while True:
        # If someone marked cookie bad, _EXP will be <= now
        wait_for = max(1.0, _EXP - time.time())
        await asyncio.sleep(min(wait_for, 300.0))  # wake up at least every 5 minutes
        if time.time() >= _EXP:
            await _refresh_once()


def ensure_cookie_refresher() -> None:
    """
    Idempotently start the refresher loop once per process.
    Call this from INSIDE an already running asyncio loop.
    """
    global _TASK
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        log.warning("ensure_cookie_refresher() called without a running event loop")
        return

    if _TASK and not _TASK.done():
        return
    _TASK = loop.create_task(refresh_loop())
    log.info("CF refresher started")


# Backwards/alternate name in case other modules import a different symbol
run_refresh_loop = refresh_loop


def get_cookie_header() -> str:
    """Return 'cf_clearance=...; __cf_bm=...' or env fallback if not yet refreshed."""
    return _COOKIE or os.getenv("PUMPFUN_COOKIE", "")


def flag_cookie_bad() -> None:
    """Force an early refresh (e.g., when the HTTP client sees a 530 page)."""
    global _EXP
    _EXP = 0.0


async def browser_fetch_json(url: str) -> Optional[Any]:
    """
    Last-resort JSON fetch via Playwright’s request API inside a context
    that already solved CF. Useful when aiohttp gets HTML/530.
    """
    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(
                headless=True, args=["--no-sandbox", "--disable-dev-shm-usage"]
            )
            try:
                ctx = await browser.new_context(user_agent=UA)
                page = await ctx.new_page()
                # Prime cookies by visiting Pump.fun once
                await page.goto(TARGET_URL, wait_until="networkidle", timeout=60_000)
                # Fetch JSON using the same context/cookies
                r = await ctx.request.get(url, timeout=30_000)
                if r.ok:
                    return await r.json()
                log.warning("browser_fetch_json: %s -> HTTP %s", url, r.status)
                return None
            finally:
                await browser.close()
    except Exception as e:
        log.warning("browser_fetch_json failed: %s", e)
        return None


__all__ = [
    "refresh_loop",
    "run_refresh_loop",
    "ensure_cookie_refresher",
    "get_cookie_header",
    "flag_cookie_bad",
    "browser_fetch_json",
]
