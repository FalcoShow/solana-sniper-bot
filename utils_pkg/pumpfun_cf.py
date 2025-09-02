# utils_pkg/pumpfun_cf.py
"""
Cloudflare cookie manager for Pump.fun with self-healing Playwright install
and correct subdomain cookie matching.

- Solves CF for BOTH hosts: pump.fun and frontend-api.pump.fun
- Keeps per-host cookie strings (cf_clearance, __cf_bm)
- Can apply cookies to an aiohttp session
- Provides a last-resort JSON fetch via Playwright if HTTP client hits 530/HTML
- If Chromium is missing at runtime, auto-installs it once and retries
"""
from __future__ import annotations

import asyncio
import logging
import os
import time
import subprocess
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

# Ensure browsers go to a writeable, persistent-ish path on Render
if not os.getenv("PLAYWRIGHT_BROWSERS_PATH"):
    os.environ["PLAYWRIGHT_BROWSERS_PATH"] = "/opt/render/.cache/ms-playwright"

PUMP_HOST = "pump.fun"
API_HOST = "frontend-api.pump.fun"
HOSTS = (PUMP_HOST, API_HOST)

# Page that reliably triggers CF for the apex site
PUMP_CF_PAGE = os.getenv("PUMPFUN_CF_URL", f"https://{PUMP_HOST}/advanced/scan")
# Hit the API root (HTML) to trip/solve CF there
API_ROOT = f"https://{API_HOST}/"
# Optionally touch a JSON-ish path once solved
API_CF_TOUCH = "https://frontend-api.pump.fun/coins/trending?offset=0&limit=1"

REFRESH_SEC = int(os.getenv("PUMPFUN_CF_REFRESH_SEC", "900"))  # ~15 minutes

# ---------- State ----------
_COOKIES: Dict[str, str] = {}      # host -> "cf_clearance=...; __cf_bm=..."
_EXP: float = 0.0                  # when to refresh
_TASK: Optional[asyncio.Task] = None
_PW_INSTALL_DONE: bool = False     # guard we only try auto-install once


# ---------- Helpers ----------
def _domain_matches_host(cookie_domain: str, host: str) -> bool:
    """
    CF often sets Domain=.pump.fun which is valid for all subdomains.
    We consider that a match for both pump.fun and frontend-api.pump.fun.
    """
    d = (cookie_domain or "").lstrip(".")
    h = (host or "").strip()
    return h == d or h.endswith("." + d)

def _cookie_string_for_host(all_cookies: List[Dict[str, Any]], host: str) -> Optional[str]:
    want: Dict[str, str] = {}
    for c in all_cookies or []:
        name = c.get("name")
        domain = c.get("domain") or ""
        if name in ("cf_clearance", "__cf_bm") and _domain_matches_host(domain, host):
            val = c.get("value")
            if val:
                want[name] = val
    if not want:
        return None
    parts = []
    if "cf_clearance" in want:
        parts.append(f"cf_clearance={want['cf_clearance']}")
    if "__cf_bm" in want:
        parts.append(f"__cf_bm={want['__cf_bm']}")
    return "; ".join(parts)

def _looks_like_missing_browser(err: Exception) -> bool:
    s = str(err)
    return "BrowserType.launch: Executable doesn't exist" in s or "playwright install" in s

def _ensure_chromium_once() -> None:
    """Run `python -m playwright install chromium` once if we detect missing browser."""
    global _PW_INSTALL_DONE
    if _PW_INSTALL_DONE:
        return
    _PW_INSTALL_DONE = True
    try:
        log.info("Playwright: chromium not found, attempting one-time install...")
        subprocess.run(
            ["python", "-m", "playwright", "install", "chromium"],
            check=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        )
    except Exception as e:
        log.warning("Playwright auto-install attempt failed: %s", e)

async def _open_context():
    """Create a browser context. If Chromium is missing, auto-install then retry once."""
    try:
        p = await async_playwright().start()
        browser = await p.chromium.launch(headless=True, args=["--no-sandbox", "--disable-dev-shm-usage"])
        ctx = await browser.new_context(user_agent=UA)
        return p, browser, ctx
    except Exception as e:
        if _looks_like_missing_browser(e):
            _ensure_chromium_once()
            p = await async_playwright().start()
            browser = await p.chromium.launch(headless=True, args=["--no-sandbox", "--disable-dev-shm-usage"])
            ctx = await browser.new_context(user_agent=UA)
            return p, browser, ctx
        raise

# ---------- Refresh ----------
async def _refresh_once() -> None:
    """Solve CF on both hosts and capture host-specific cookies."""
    global _COOKIES, _EXP
    try:
        p, browser, ctx = await _open_context()
        try:
            page = await ctx.new_page()

            # 1) Apex site
            await page.goto(PUMP_CF_PAGE, wait_until="networkidle", timeout=60_000)

            # 2) API host — go to HTML root first so CF can render its challenge
            try:
                await page.goto(API_ROOT, wait_until="domcontentloaded", timeout=60_000)
                # Give CF a moment to complete JS challenge if present
                await page.wait_for_timeout(3000)
                # Light poke to the API path (non-fatal)
                try:
                    await page.goto(API_CF_TOUCH, wait_until="domcontentloaded", timeout=20_000)
                except Exception:
                    pass
            except Exception:
                pass

            all_cookies = await ctx.cookies()
        finally:
            await browser.close()
            await p.stop()

        new_map: Dict[str, str] = {}
        for host in HOSTS:
            ck = _cookie_string_for_host(all_cookies, host)
            if ck:
                new_map[host] = ck

        _COOKIES = new_map
        os.environ["PUMPFUN_COOKIE"] = new_map.get(PUMP_HOST, "")
        os.environ["PUMPFUN_COOKIE_API"] = new_map.get(API_HOST, "")

        pump_ok = "yes" if PUMP_HOST in _COOKIES else "no"
        api_ok = "yes" if API_HOST in _COOKIES else "no"
        if _COOKIES:
            _EXP = time.time() + max(60.0, REFRESH_SEC * 0.9)
            log.info("CF refresh: got cookies (pump=%s, api=%s), next in ~%ds", pump_ok, api_ok, REFRESH_SEC)
        else:
            _EXP = time.time() + 120.0
            log.warning("CF refresh: no cookies captured; will retry sooner.")
    except Exception as e:
        _EXP = time.time() + 120.0
        log.warning("CF refresh failed: %s", e)

# ---------- Public loop ----------
async def refresh_loop() -> None:
    await _refresh_once()
    while True:
        sleep_s = max(1.0, _EXP - time.time())
        await asyncio.sleep(min(sleep_s, 300.0))
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

# ---------- API ----------
def get_cookie_for(url_or_host: str) -> str:
    """Return the cookie string for a given URL or bare host."""
    host = urlparse(url_or_host).hostname or url_or_host
    return _COOKIES.get(host, "")

def flag_cookie_bad() -> None:
    """Force a quicker refresh (e.g., on 530)."""
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
    """
    Attach per-host cookies to an existing aiohttp.ClientSession.
    We set cookies for BOTH hosts explicitly.
    """
    try:
        from aiohttp import client_reqrep  # lazy import inside function
        for host in HOSTS:
            ck = _COOKIES.get(host)
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
    Last-resort JSON GET via Playwright. Ensures CF is solved for the
    target host in this context before the request; retries once on 530.
    """
    host = urlparse(url).hostname or ""
    try:
        p, browser, ctx = await _open_context()
        try:
            page = await ctx.new_page()

            # Always prime apex first – it often makes API host challenge easier
            try:
                await page.goto(PUMP_CF_PAGE, wait_until="networkidle", timeout=60_000)
            except Exception:
                pass

            if host.endswith(API_HOST):
                try:
                    await page.goto(API_ROOT, wait_until="domcontentloaded", timeout=60_000)
                    await page.wait_for_timeout(2500)
                except Exception:
                    pass

            # Try fetch, retry once on 530 after another brief wait
            for attempt in (1, 2):
                r = await ctx.request.get(url, timeout=30_000)
                if r.ok:
                    return await r.json()
                if r.status == 530 and attempt == 1:
                    await page.wait_for_timeout(2000)
                    continue
                log.warning("browser_fetch_json: %s -> HTTP %s", url, r.status)
                return None
        finally:
            await browser.close()
            await p.stop()
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
