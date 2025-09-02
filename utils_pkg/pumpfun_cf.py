# utils_pkg/pumpfun_cf.py
"""
Cloudflare cookie manager for Pump.fun.

- Solves CF on BOTH hosts: pump.fun and frontend-api.pump.fun
- Keeps per-host cookie strings (cf_clearance, __cf_bm)
- Can apply cookies to an aiohttp session
- Provides a last-resort JSON fetch via Playwright if HTTP client hits 530/HTML

Public:
    ensure_cookie_refresher()
    refresh_loop()
    get_cookie_for(url_or_host) -> str
    apply_cookies_to_session(session) -> None
    flag_cookie_bad() -> None
    browser_fetch_json(url) -> dict|list|None
"""
from __future__ import annotations

import asyncio
import json
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
PUMP_CF_PAGE = os.getenv("PUMPFUN_CF_URL", "https://pump.fun/advanced/scan")

# A URL that causes the API host to set/validate CF cookies
API_CF_PAGE = os.getenv(
    "PUMPFUN_API_CF_URL",
    "https://frontend-api.pump.fun/coins/trending?offset=0&limit=1",
)

REFRESH_SEC = int(os.getenv("PUMPFUN_CF_REFRESH_SEC", "900"))  # ~15 minutes
HOSTS = ("pump.fun", "frontend-api.pump.fun")

# If Render’s env var isn’t set, still prefer a writable path
os.environ.setdefault("PLAYWRIGHT_BROWSERS_PATH", "/opt/render/.cache/ms-playwright")

# ---------- State ----------
_COOKIES: Dict[str, str] = {}  # host -> "cf_clearance=...; __cf_bm=..."
_EXP: float = 0.0
_TASK: Optional[asyncio.Task] = None

# ---------- Helpers ----------
async def _ensure_chromium_available() -> None:
    """
    Render sometimes misses browser binaries even if Playwright is present.
    Try a one-time runtime install of Chromium.
    """
    try:
        # Quick probe: try to launch and immediately close – if it explodes,
        # do a one-time install below.
        async with async_playwright() as p:
            b = await p.chromium.launch(headless=True, args=["--no-sandbox"])
            await b.close()
        return
    except Exception:
        pass

    try:
        log.info("Playwright: chromium not found, attempting one-time install...")
        # Avoid blocking the loop forever; keep it simple.
        proc = await asyncio.create_subprocess_exec(
            "python", "-m", "playwright", "install", "chromium",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
        )
        try:
            await asyncio.wait_for(proc.communicate(), timeout=180)
        except asyncio.TimeoutError:
            proc.kill()
            log.warning("Playwright install timed out; continuing anyway.")
    except Exception as e:
        log.warning("Playwright one-time install failed: %s", e)


def _cookie_for_host(all_cookies: List[Dict[str, Any]], host: str) -> Optional[str]:
    want: Dict[str, str] = {}
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


async def _prime_context_and_cookies(ctx) -> Dict[str, str]:
    """
    Navigate both hosts so CF sets cookies; return host->cookie string.
    """
    page = await ctx.new_page()
    # 1) apex – this often primes the challenge
    try:
        await page.goto(PUMP_CF_PAGE, wait_until="networkidle", timeout=60_000)
    except Exception:
        # Not fatal; CF might still be satisfied
        pass

    # 2) API host – try to trip/solve CF here as well
    try:
        await page.goto(API_CF_PAGE, wait_until="networkidle", timeout=60_000)
    except Exception:
        pass

    # tiny breath – sometimes cookies land just after network idle
    await page.wait_for_timeout(300)
    all_cookies = await ctx.cookies()

    new_map: Dict[str, str] = {}
    for host in HOSTS:
        ck = _cookie_for_host(all_cookies, host)
        if ck:
            new_map[host] = ck
    await page.close()
    return new_map


async def _refresh_once() -> None:
    global _COOKIES, _EXP
    try:
        await _ensure_chromium_available()
        async with async_playwright() as p:
            browser = await p.chromium.launch(
                headless=True,
                args=["--no-sandbox", "--disable-dev-shm-usage", "--disable-setuid-sandbox"],
            )
            try:
                ctx = await browser.new_context(user_agent=UA)
                new_map = await _prime_context_and_cookies(ctx)
            finally:
                await browser.close()

        _COOKIES = new_map
        os.environ["PUMPFUN_COOKIE"] = new_map.get("pump.fun", "")
        os.environ["PUMPFUN_COOKIE_API"] = new_map.get("frontend-api.pump.fun", "")

        if _COOKIES:
            _EXP = time.time() + max(60.0, REFRESH_SEC * 0.9)
            log.info(
                "CF refresh: got cookies (pump=%s, api=%s), next in ~%ds",
                "yes" if "pump.fun" in _COOKIES else "no",
                "yes" if "frontend-api.pump.fun" in _COOKIES else "no",
                REFRESH_SEC,
            )
        else:
            _EXP = time.time() + 120.0
            log.warning("CF refresh: no cookies captured; will retry sooner.")
    except Exception as e:
        _EXP = time.time() + 120.0
        log.warning("CF refresh failed: %s", e)


# ---------- Public: refresher ----------
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


# ---------- Public: helpers ----------
def get_cookie_for(url_or_host: str) -> str:
    host = urlparse(url_or_host).hostname or url_or_host
    return _COOKIES.get(host, "")


def flag_cookie_bad() -> None:
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
    """
    try:
        from aiohttp import client_reqrep
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


# ---------- Public: last-resort fetch ----------
async def browser_fetch_json(url: str) -> Optional[Any]:
    """
    JSON GET via Playwright after priming cookies in the *same* context.
    """
    try:
        await _ensure_chromium_available()
        async with async_playwright() as p:
            browser = await p.chromium.launch(
                headless=True,
                args=["--no-sandbox", "--disable-dev-shm-usage", "--disable-setuid-sandbox"],
            )
            try:
                ctx = await browser.new_context(user_agent=UA)
                # Make sure this context has solved CF
                await _prime_context_and_cookies(ctx)

                # Fetch JSON using the same context/cookies
                headers = {
                    "User-Agent": UA,
                    "Accept": "application/json, text/plain, */*",
                    "Referer": "https://pump.fun/trending",
                    "Origin": "https://pump.fun",
                    "Cache-Control": "no-cache",
                    "Pragma": "no-cache",
                }
                r = await ctx.request.get(url, headers=headers, timeout=30_000)
                if r.ok:
                    # Try JSON first; some endpoints return lists, some objects.
                    try:
                        return await r.json()
                    except Exception:
                        text = await r.text()
                        try:
                            return json.loads(text)
                        except Exception:
                            return None
                else:
                    status = r.status
                    if status == 530:
                        flag_cookie_bad()
                    log.warning("browser_fetch_json: %s -> HTTP %s", url, status)
                    return None
            finally:
                await browser.close()
    except Exception as e:
        log.warning("browser_fetch_json failed: %s", e)
        return None


__all__ = [
    "ensure_cookie_refresher",
    "refresh_loop",
    "get_cookie_for",
    "apply_cookies_to_session",
    "flag_cookie_bad",
    "browser_fetch_json",
]
