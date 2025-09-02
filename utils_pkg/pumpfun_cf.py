# utils_pkg/pumpfun_cf.py
"""
Cloudflare cookie manager for Pump.fun.

- Solves CF for BOTH hosts: pump.fun and frontend-api.pump.fun
- Keeps per-host cookie strings (cf_clearance, __cf_bm)
- Can apply cookies to an aiohttp session
- Provides a last-resort JSON fetch via Playwright if HTTP client hits 530/HTML

Public:
    ensure_cookie_refresher()
    get_cookie_for(url_or_host) -> str
    apply_cookies_to_session(session) -> None
    flag_cookie_bad() -> None
    browser_fetch_json(url) -> dict|list|None
"""
from __future__ import annotations

import asyncio
import logging
import os
import time
import subprocess
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse
from glob import glob

from playwright.async_api import async_playwright

log = logging.getLogger("pumpfun.cf")

# ---------- Config ----------
UA = os.getenv(
    "PUMPFUN_UA",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/126 Safari/537.36",
)

# Page that reliably triggers CF for the apex site
PUMP_CF_PAGE = os.getenv("PUMPFUN_CF_URL", "https://pump.fun/advanced/scan")

# “Page-like” URL to force CF on the API host as well
API_CF_PAGE = os.getenv(
    "PUMPFUN_API_CF_URL",
    "https://frontend-api.pump.fun/coins/trending?offset=0&limit=1"
)

REFRESH_SEC = int(os.getenv("PUMPFUN_CF_REFRESH_SEC", "900"))  # ~15 minutes
HOSTS = ("pump.fun", "frontend-api.pump.fun")
BROWSERS_PATH = os.getenv("PLAYWRIGHT_BROWSERS_PATH", "/opt/render/.cache/ms-playwright")

# ---------- State ----------
_COOKIES: Dict[str, str] = {}      # host -> "cf_clearance=...; __cf_bm=..."
_EXP: float = 0.0
_TASK: Optional[asyncio.Task] = None
_BROWSERS_OK: bool = False
_BROWSERS_LOCK = asyncio.Lock()


# ---------- Browser bootstrap ----------
def _have_chromium_payloads() -> bool:
    """Detect both chromium and chromium-headless-shell payloads."""
    try:
        chromes = glob(os.path.join(BROWSERS_PATH, "chromium-*", "chrome-linux", "chrome"))
        shells  = glob(os.path.join(BROWSERS_PATH, "chromium_headless_shell-*", "chrome-linux", "headless_shell"))
        return bool(chromes) and bool(shells)
    except Exception:
        return False


async def _ensure_browsers() -> None:
    """Install missing Playwright browser payloads once (runtime-safe)."""
    global _BROWSERS_OK
    if _BROWSERS_OK and _have_chromium_payloads():
        return

    async with _BROWSERS_LOCK:
        if _BROWSERS_OK and _have_chromium_payloads():
            return
        env = dict(os.environ)
        env.setdefault("PLAYWRIGHT_BROWSERS_PATH", BROWSERS_PATH)

        # Install both chromium + chromium-headless-shell (no --with-deps on Render)
        try:
            log.info("playwright: ensuring browsers in %s ...", BROWSERS_PATH)
            subprocess.run(
                ["python", "-m", "playwright", "install", "chromium", "chromium-headless-shell"],
                check=True, env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT
            )
            _BROWSERS_OK = _have_chromium_payloads()
            if _BROWSERS_OK:
                log.info("playwright: chromium installed (path=%s)", BROWSERS_PATH)
            else:
                log.warning("playwright: install finished but binaries not found; will retry later")
        except Exception as e:
            log.warning("playwright: install failed (will retry when used): %s", e)


# ---------- Helpers ----------
def _cookie_for_host(all_cookies: List[Dict[str, Any]], host: str) -> Optional[str]:
    """Extract the cf cookies for a specific host (host-only cookies!)."""
    want = {}
    for c in all_cookies or []:
        name = c.get("name")
        domain = (c.get("domain") or "").lstrip(".")
        if domain.endswith(host) and name in ("cf_clearance", "__cf_bm"):
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


async def _refresh_once() -> None:
    """Solve CF on both hosts and capture host-specific cookies."""
    global _COOKIES, _EXP
    try:
        await _ensure_browsers()

        async with async_playwright() as p:
            browser = await p.chromium.launch(
                headless=True,
                args=["--no-sandbox", "--disable-dev-shm-usage"],
            )
            try:
                ctx = await browser.new_context(user_agent=UA)
                page = await ctx.new_page()

                # 1) Apex site (sets cf cookies)
                await page.goto(PUMP_CF_PAGE, wait_until="networkidle", timeout=60_000)

                # 2) API host (trip CF there too)
                try:
                    await page.goto(API_CF_PAGE, wait_until="networkidle", timeout=60_000)
                except Exception:
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
        os.environ["PUMPFUN_COOKIE"] = new_map.get("pump.fun", "")
        os.environ["PUMPFUN_COOKIE_API"] = new_map.get("frontend-api.pump.fun", "")

        if _COOKIES:
            _EXP = time.time() + max(60.0, REFRESH_SEC * 0.9)
            red_pump = "***" if "pump.fun" in _COOKIES else "-"
            red_api = "***" if "frontend-api.pump.fun" in _COOKIES else "-"
            log.info("CF refresh: got cookies (pump=%s, api=%s), next in ~%ds",
                     red_pump, red_api, REFRESH_SEC)
        else:
            _EXP = time.time() + 120.0
            log.warning("CF refresh: no cookies captured; will retry sooner.")

    except Exception as e:
        _EXP = time.time() + 120.0
        log.warning("CF refresh failed: %s", e)


# ---------- Public ----------
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
    """Attach per-host cookies to an existing aiohttp.ClientSession."""
    try:
        from aiohttp import client_reqrep  # lazy import
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
    """Last-resort JSON GET via Playwright with CF solved in-context."""
    await _ensure_browsers()
    host = urlparse(url).hostname or ""
    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(
                headless=True,
                args=["--no-sandbox", "--disable-dev-shm-usage"],
            )
            try:
                ctx = await browser.new_context(user_agent=UA)
                page = await ctx.new_page()

                # Prime apex
                try:
                    await page.goto(PUMP_CF_PAGE, wait_until="networkidle", timeout=60_000)
                except Exception:
                    pass

                # Prime API host if needed
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
