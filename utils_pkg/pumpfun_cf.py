# utils_pkg/pumpfun_cf.py
"""
Cloudflare cookie manager for Pump.fun.

- Solves CF for BOTH hosts: pump.fun and frontend-api.pump.fun
- Keeps per-host cookie strings (cf_clearance, __cf_bm)
- Can apply cookies to an aiohttp session
- Provides a last-resort JSON fetch via Playwright if HTTP client hits 530/HTML
"""
from __future__ import annotations

import asyncio
import logging
import os
import time
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

from playwright.async_api import async_playwright

log = logging.getLogger("pumpfun.cf")

# ---------- Config ----------
UA = os.getenv(
    "PUMPFUN_UA",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/126 Safari/537.36",
)

# Page that reliably triggers CF for the apex site
PUMP_CF_PAGE = os.getenv(
    "PUMPFUN_CF_URL",
    "https://pump.fun/advanced/scan",
)

# A page-like URL that forces CF on the API host as well.
API_CF_PAGE = os.getenv(
    "PUMPFUN_API_CF_URL",
    "https://frontend-api.pump.fun/coins/trending?offset=0&limit=1",
)

REFRESH_SEC = int(os.getenv("PUMPFUN_CF_REFRESH_SEC", "900"))  # ~15 minutes

HOSTS = ("pump.fun", "frontend-api.pump.fun")

# ---------- State ----------
_COOKIES: Dict[str, str] = {}      # host -> "cf_clearance=...; __cf_bm=..."
_EXP: float = 0.0                  # when to refresh
_TASK: Optional[asyncio.Task] = None


# ---------- Helpers ----------
def _domain_matches_host(cookie_domain: str, host: str) -> bool:
    """
    Return True if a cookie scoped to `cookie_domain` will be sent to `host`.
    Cookie rules: a cookie with Domain=.pump.fun matches pump.fun and ANY subdomain.
    """
    d = (cookie_domain or "").lstrip(".").lower()
    h = (host or "").lower()
    if not d or not h:
        return False
    return h == d or h.endswith("." + d)

def _cookie_for_host(all_cookies: List[Dict[str, Any]], host: str) -> Optional[str]:
    """
    Extract CF cookies for the given host. Accept both host-scoped and parent-domain cookies.
    (e.g., a cookie with Domain=.pump.fun is valid for frontend-api.pump.fun)
    """
    got: Dict[str, str] = {}
    for c in all_cookies or []:
        name = c.get("name")
        domain = c.get("domain") or ""
        if name in ("cf_clearance", "__cf_bm") and _domain_matches_host(domain, host):
            val = c.get("value")
            if val:
                got[name] = val

    if not got:
        return None
    parts = []
    if "cf_clearance" in got:
        parts.append(f"cf_clearance={got['cf_clearance']}")
    if "__cf_bm" in got:
        parts.append(f"__cf_bm={got['__cf_bm']}")
    return "; ".join(parts)


async def _refresh_once() -> None:
    """Solve CF on both hosts and capture host-specific/properly-scoped cookies."""
    global _COOKIES, _EXP
    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(
                headless=True,
                args=["--no-sandbox", "--disable-dev-shm-usage"],
            )
            try:
                ctx = await browser.new_context(user_agent=UA)
                page = await ctx.new_page()

                # 1) Apex site first
                await page.goto(PUMP_CF_PAGE, wait_until="networkidle", timeout=60_000)

                # 2) API host
                try:
                    await page.goto(API_CF_PAGE, wait_until="networkidle", timeout=60_000)
                except Exception:
                    # CF may bounce; apex typically primes it anyway.
                    pass

                all_cookies = await ctx.cookies()
            finally:
                await browser.close()

        new_map: Dict[str, str] = {}
        for host in HOSTS:
            ck = _cookie_for_host(all_cookies, host)
            if ck:
                new_map[host] = ck

        _COOKIES = new_map
        # Optional exports so other code can read them
        os.environ["PUMPFUN_COOKIE"] = new_map.get("pump.fun", "")
        os.environ["PUMPFUN_COOKIE_API"] = new_map.get("frontend-api.pump.fun", "")

        if _COOKIES:
            _EXP = time.time() + max(60.0, REFRESH_SEC * 0.9)
            log.info(
                "CF refresh: cookies => pump=%s, api=%s (next ~%ds)",
                "***" if "pump.fun" in _COOKIES else "-",
                "***" if "frontend-api.pump.fun" in _COOKIES else "-",
                REFRESH_SEC,
            )
        else:
            _EXP = time.time() + 120.0
            log.warning("CF refresh: no cookies captured; retrying sooner.")
    except Exception as e:
        _EXP = time.time() + 120.0
        log.warning("CF refresh failed: %s", e)


# ---------- Public ----------
async def refresh_loop() -> None:
    await _refresh_once()
    while True:
        await asyncio.sleep(min(max(1.0, _EXP - time.time()), 300.0))
        if time.time() >= _EXP:
            await _refresh_once()

def ensure_cookie_refresher() -> None:
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

def get_cookie_for(url_or_host: str) -> str:
    """Return the cookie string for a given URL or bare host."""
    host = (urlparse(url_or_host).hostname or url_or_host or "").lower()
    # prefer exact host; if absent but apex cookie exists, that still applies
    return _COOKIES.get(host) or _COOKIES.get("pump.fun", "")

def flag_cookie_bad() -> None:
    """Force an early refresh (e.g., when the HTTP client sees a 530 page)."""
    global _EXP
    _EXP = 0.0

def _parse_cookie_string(s: str) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for part in (s or "").split(";"):
        if "=" in part:
            k, v = part.split("=", 1)
            out[k.strip()] = v.strip()
    return out

def apply_cookies_to_session(session) -> None:
    """Attach per-host cookies to an existing aiohttp.ClientSession."""
    try:
        from aiohttp import client_reqrep  # lazy import
        for host in HOSTS:
            ck = get_cookie_for(host)
            if not ck:
                continue
            session.cookie_jar.update_cookies(
                _parse_cookie_string(ck),
                response_url=client_reqrep.URL(f"https://{host}/"),
            )
    except Exception as e:
        log.warning("apply_cookies_to_session failed: %s", e)

async def browser_fetch_json(url: str) -> Optional[Any]:
    """
    Last-resort JSON GET via Playwright.
    Ensures CF is solved in this context before the request.
    """
    host = (urlparse(url).hostname or "").lower()
    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(
                headless=True,
                args=["--no-sandbox", "--disable-dev-shm-usage"],
            )
            try:
                ctx = await browser.new_context(user_agent=UA)
                page = await ctx.new_page()

                # Apex first
                try:
                    await page.goto(PUMP_CF_PAGE, wait_until="networkidle", timeout=60_000)
                except Exception:
                    pass

                # If target is the API host, nudge that host too
                if host.endswith("frontend-api.pump.fun"):
                    try:
                        await page.goto(API_CF_PAGE, wait_until="networkidle", timeout=60_000)
                    except Exception:
                        pass

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
    "ensure_cookie_refresher",
    "get_cookie_for",
    "apply_cookies_to_session",
    "flag_cookie_bad",
    "browser_fetch_json",
    "refresh_loop",
]
