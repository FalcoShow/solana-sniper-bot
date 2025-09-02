# utils_pkg/pumpfun_cf.py
"""
Cloudflare cookie manager for Pump.fun.

What this does
--------------
- Solves Cloudflare for BOTH hosts:  pump.fun  and  frontend-api.pump.fun
- Keeps per-host cookie strings (cf_clearance, __cf_bm)
- Exposes helpers to:
    * start a background refresher loop
    * get the right cookie header for a URL or host
    * apply cookies into an existing aiohttp session
    * mark cookies "bad" (force early refresh)
    * last-resort JSON fetch via Playwright if your HTTP client gets 530/HTML
- Self-heals at runtime if the Chromium binary is missing by running
  `python -m playwright install chromium` once (no sudo, no --with-deps).

Env knobs
---------
PUMPFUN_UA                     : override User-Agent string
PUMPFUN_CF_URL                 : page on pump.fun that reliably trips CF (default /advanced/scan)
PUMPFUN_API_CF_URL             : page/endpoint to trip CF on the API host (default /coins/trending?limit=1)
PUMPFUN_CF_REFRESH_SEC         : refresh cadence in seconds (default 900)
PLAYWRIGHT_BROWSERS_PATH       : where Playwright keeps browsers; honored by installer & runtime checks
DISABLE_RUNTIME_PLAYWRIGHT_INSTALL : set to "1" to skip runtime self-heal (useful if build stage guarantees it)
"""

from __future__ import annotations

import asyncio
import logging
import os
import time
import subprocess
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse
from pathlib import Path

from playwright.async_api import async_playwright

log = logging.getLogger("pumpfun.cf")

# ---------- Config ----------
UA = os.getenv(
    "PUMPFUN_UA",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/126 Safari/537.36",
)

PUMP_CF_PAGE = os.getenv("PUMPFUN_CF_URL", "https://pump.fun/advanced/scan")
API_CF_PAGE = os.getenv(
    "PUMPFUN_API_CF_URL",
    "https://frontend-api.pump.fun/coins/trending?offset=0&limit=1",
)

REFRESH_SEC = int(os.getenv("PUMPFUN_CF_REFRESH_SEC", "900"))  # ~15 minutes

HOST_PUMP = "pump.fun"
HOST_API = "frontend-api.pump.fun"
HOSTS = (HOST_PUMP, HOST_API)

# ---------- State ----------
_COOKIES: Dict[str, str] = {}      # host -> "cf_clearance=...; __cf_bm=..."
_EXP: float = 0.0                  # when to refresh
_TASK: Optional[asyncio.Task] = None
_INSTALL_TRIED: bool = False       # avoid spamming runtime installs


# ---------- Runtime browser availability (self-heal) ----------
def _playwright_base() -> Path:
    base = os.getenv("PLAYWRIGHT_BROWSERS_PATH")
    if base:
        return Path(base).expanduser()
    # default used by Playwright when env var is not set
    return Path.home() / ".cache" / "ms-playwright"


def _chromium_is_available() -> bool:
    base = _playwright_base()
    try:
        for d in base.glob("chromium*"):
            headless = d / "chrome-linux" / "headless_shell"
            chrome = d / "chrome-linux" / "chrome"
            if headless.exists() or chrome.exists():
                return True
    except Exception:
        pass
    return False


def _ensure_chromium_sync() -> None:
    """Best-effort one-time install of Chromium if Playwright can’t find it."""
    global _INSTALL_TRIED
    if _chromium_is_available():
        return
    if _INSTALL_TRIED or os.getenv("DISABLE_RUNTIME_PLAYWRIGHT_INSTALL") == "1":
        return
    _INSTALL_TRIED = True
    try:
        env = dict(os.environ)
        # No --with-deps (Render/containers usually don't allow apt/sudo here)
        out = subprocess.run(
            ["python", "-m", "playwright", "install", "chromium"],
            check=True,
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )
        log.info("playwright: chromium installed (path=%s)", _playwright_base())
        if out.stdout:
            # Trim noisy banner but keep a breadcrumb
            log.debug("playwright install output (tail): %s", out.stdout[-400:])
    except Exception as e:
        log.warning("playwright: runtime install failed: %s", e)


# ---------- Cookie helpers ----------
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


# ---------- Refresh logic ----------
async def _refresh_once() -> None:
    """Solve CF on both hosts and capture host-specific cookies."""
    global _COOKIES, _EXP
    try:
        _ensure_chromium_sync()  # make sure the browser exists
        async with async_playwright() as p:
            browser = await p.chromium.launch(
                headless=True,
                args=["--no-sandbox", "--disable-dev-shm-usage"],
            )
            try:
                ctx = await browser.new_context(user_agent=UA)
                page = await ctx.new_page()

                # 1) Apex site
                await page.goto(PUMP_CF_PAGE, wait_until="networkidle", timeout=60_000)

                # 2) API host
                try:
                    await page.goto(API_CF_PAGE, wait_until="networkidle", timeout=60_000)
                except Exception:
                    # CF sometimes bounces; the apex step usually primes the challenge anyway.
                    pass

                all_cookies = await ctx.cookies()
            finally:
                await browser.close()

        new_map: Dict[str, str] = {}
        for host in HOSTS:
            ck = _cookie_for_host(all_cookies, host)
            if ck:
                new_map[host] = ck

        # store + env exports (optional for other components)
        _COOKIES = new_map
        os.environ["PUMPFUN_COOKIE"] = new_map.get(HOST_PUMP, "")
        os.environ["PUMPFUN_COOKIE_API"] = new_map.get(HOST_API, "")

        if _COOKIES:
            _EXP = time.time() + max(60.0, REFRESH_SEC * 0.9)
            red_pump = "***" if HOST_PUMP in _COOKIES else "-"
            red_api = "***" if HOST_API in _COOKIES else "-"
            log.info("CF refresh: got cookies (pump=%s, api=%s), next in ~%ds",
                     red_pump, red_api, REFRESH_SEC)
        else:
            _EXP = time.time() + 120.0
            log.warning("CF refresh: no cookies captured; will retry sooner.")

    except Exception as e:
        _EXP = time.time() + 120.0
        log.warning("CF refresh failed: %s", e)


# ---------- Public API ----------
async def refresh_loop() -> None:
    """Background refresher; schedule with `asyncio.create_task(refresh_loop())`."""
    await _refresh_once()
    while True:
        sleep_s = max(1.0, _EXP - time.time())
        await asyncio.sleep(min(sleep_s, 300.0))
        if time.time() >= _EXP:
            await _refresh_once()


def ensure_cookie_refresher() -> None:
    """Idempotently start the refresher loop once per process.

    Call this from INSIDE an already running asyncio event loop.
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


def get_cookie_for(url_or_host: str) -> str:
    """Return the cookie string for a given URL or bare host."""
    host = urlparse(url_or_host).hostname or url_or_host
    return _COOKIES.get(host, "")


def get_cookie_header(url_or_host: Optional[str] = None) -> str:
    """Compatibility helper: if no host given, prefer API host cookie."""
    if url_or_host:
        return get_cookie_for(url_or_host)
    # default to API host (most JSON calls hit this)
    return _COOKIES.get(HOST_API, "") or _COOKIES.get(HOST_PUMP, "")


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

    Example:
        apply_cookies_to_session(session)
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
    target host in this context before the request.
    """
    host = (urlparse(url).hostname or "").lower()
    try:
        _ensure_chromium_sync()
        async with async_playwright() as p:
            browser = await p.chromium.launch(
                headless=True,
                args=["--no-sandbox", "--disable-dev-shm-usage"],
            )
            try:
                ctx = await browser.new_context(user_agent=UA)
                page = await ctx.new_page()

                # Always prime apex first – it often makes API host challenge easier
                try:
                    await page.goto(PUMP_CF_PAGE, wait_until="networkidle", timeout=60_000)
                except Exception:
                    pass

                # If target is the API host, try to trip/solve CF there too
                if host.endswith(HOST_API):
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
    "get_cookie_header",
    "apply_cookies_to_session",
    "flag_cookie_bad",
    "browser_fetch_json",
    "refresh_loop",
]
