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

from playwright.async_api import async_playwright, Response

log = logging.getLogger("pumpfun.cf")

# ---------- Config ----------
UA = os.getenv(
    "PUMPFUN_UA",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/126 Safari/537.36",
)

# A real Pump.fun page that triggers CF for the apex host
PUMP_CF_PAGE = os.getenv("PUMPFUN_CF_URL", "https://pump.fun/advanced/scan")

# Path on the API host we can navigate to (a JSON endpoint works; CF will serve a challenge page)
API_PRIME_PATH = os.getenv("PUMPFUN_API_PRIME_PATH", "/coins/trending?offset=0&limit=1")

REFRESH_SEC = int(os.getenv("PUMPFUN_CF_REFRESH_SEC", "900"))  # ~15 minutes
HOSTS = ("pump.fun", "frontend-api.pump.fun")

# Keep browser binaries between deploys (Render-friendly)
os.environ.setdefault("PLAYWRIGHT_BROWSERS_PATH", "/opt/render/.cache/ms-playwright")

# ---------- State ----------
_COOKIES: Dict[str, str] = {}  # host -> "cf_clearance=...; __cf_bm=..."
_EXP: float = 0.0
_TASK: Optional[asyncio.Task] = None


# ---------- Utility ----------
async def _ensure_chromium_available() -> None:
    """Install Chromium once if not present."""
    try:
        async with async_playwright() as p:
            b = await p.chromium.launch(headless=True, args=["--no-sandbox"])
            await b.close()
        return
    except Exception:
        pass

    try:
        log.info("Playwright: chromium not found, attempting one-time install...")
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


async def _prime_api_host_via_navigation(page) -> None:
    """
    Navigate directly to the API JSON URL. Cloudflare will serve a challenge page
    under that host, execute, and then redirect back—setting host-scoped cookies.
    """
    targets = [
        f"https://frontend-api.pump.fun{API_PRIME_PATH}",
        "https://frontend-api.pump.fun/health",
        "https://frontend-api.pump.fun/",  # last resort just to establish origin
    ]
    for url in targets:
        try:
            # 'networkidle' lets CF challenge finish; some pages never reach that,
            # so we cap the timeout and continue.
            resp: Optional[Response] = await page.goto(url, wait_until="networkidle", timeout=60_000)
            await page.wait_for_timeout(600)  # give CF a moment to set cookies
            # If we actually got JSON 2xx back, great—cookies should be set now.
            if resp and resp.ok:
                ct = (resp.headers.get("content-type") or "").lower()
                if "json" in ct:
                    break
        except Exception:
            continue

    # As a final nudge, do a same-origin fetch to a tiny endpoint with credentials
    try:
        await page.evaluate(
            f"""() => fetch('{API_PRIME_PATH}', {{ credentials: 'include' }}).catch(() => {{}})"""
        )
        await page.wait_for_timeout(600)
    except Exception:
        pass


async def _prime_context_and_cookies(ctx) -> Dict[str, str]:
    """
    Solve CF for apex and API hosts within the same browser context, then return cookies.
    """
    page = await ctx.new_page()

    # 1) Apex host (pump.fun)
    try:
        await page.goto(PUMP_CF_PAGE, wait_until="networkidle", timeout=60_000)
    except Exception:
        pass

    # 2) API host (frontend-api.pump.fun) — MUST use navigation on that host
    try:
        await _prime_api_host_via_navigation(page)
    except Exception:
        pass

    # Collect cookies for both hosts
    all_cookies = await ctx.cookies()
    await page.close()

    new_map: Dict[str, str] = {}
    for host in HOSTS:
        ck = _cookie_for_host(all_cookies, host)
        if ck:
            new_map[host] = ck
    return new_map


# ---------- Refresher ----------
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


# ---------- Helpers ----------
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
    """Attach per-host cookies to an existing aiohttp.ClientSession."""
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


# ---------- Last-resort fetch ----------
async def browser_fetch_json(url: str) -> Optional[Any]:
    """
    Use the same browser-context that already solved CF. For API host,
    try a real navigation first (so the challenge page runs if needed),
    then fall back to same-origin page.fetch.
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
                # Ensure cookies exist in this context
                await _prime_context_and_cookies(ctx)

                host = urlparse(url).hostname or ""
                page = await ctx.new_page()

                if host.endswith("frontend-api.pump.fun"):
                    # 1) Try navigation directly to the JSON URL
                    try:
                        resp: Optional[Response] = await page.goto(url, wait_until="networkidle", timeout=45_000)
                        if resp and resp.ok:
                            text = await resp.text()
                            try:
                                return json.loads(text)
                            except Exception:
                                pass
                        status = (resp.status if resp else 0)
                        if status in (403, 429, 530):
                            flag_cookie_bad()
                        log.warning("browser_fetch_json: %s -> HTTP %s (page.goto)", url, status or 0)
                    except Exception:
                        # 2) Fallback to same-origin fetch via the page
                        pass

                    try:
                        js = """
                            async (u) => {
                              try {
                                const r = await fetch(u, { credentials: 'include' });
                                if (!r.ok) return { ok:false, status:r.status };
                                const ct = r.headers.get('content-type') || '';
                                if (ct.includes('application/json')) return { ok:true, status:r.status, data: await r.json() };
                                const t = await r.text();
                                try { return { ok:true, status:r.status, data: JSON.parse(t) }; }
                                catch { return { ok:false, status:r.status }; }
                              } catch (e) { return { ok:false, status:0 }; }
                            }
                        """
                        res = await page.evaluate(js, url)
                        if res and res.get("ok"):
                            return res.get("data")
                        status = res.get("status", 0) if isinstance(res, dict) else 0
                        if status in (403, 429, 530):
                            flag_cookie_bad()
                        log.warning("browser_fetch_json: %s -> HTTP %s (page.fetch)", url, status)
                        return None
                    finally:
                        await page.close()

                # Non-API hosts: simple request client
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
                    try:
                        return await r.json()
                    except Exception:
                        text = await r.text()
                        try:
                            return json.loads(text)
                        except Exception:
                            return None
                status = r.status
                if status in (403, 429, 530):
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
