# utils_pkg/pumpfun_cf.py
"""
Cloudflare cookie manager for Pump.fun.

- Solves CF on BOTH hosts: pump.fun and frontend-api.pump.fun
- Keeps per-host cookie strings (cf_clearance, __cf_bm)
- Can apply cookies to an aiohttp session
- Provides a last-resort JSON fetch via Playwright when HTTP client hits 530/HTML
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

# ---------------- Config ----------------
UA = os.getenv(
    "PUMPFUN_UA",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
)

PUMP_CF_PAGE = os.getenv("PUMPFUN_CF_URL", "https://pump.fun/advanced/scan")
API_PRIME_PATH = os.getenv("PUMPFUN_API_PRIME_PATH", "/coins/trending?offset=0&limit=1")
REFRESH_SEC = int(os.getenv("PUMPFUN_CF_REFRESH_SEC", "900"))
HOSTS = ("pump.fun", "frontend-api.pump.fun")

# Persist Chromium between deploys (Render-friendly)
os.environ.setdefault("PLAYWRIGHT_BROWSERS_PATH", "/opt/render/.cache/ms-playwright")

# ---------------- State -----------------
_COOKIES: Dict[str, str] = {}  # host -> cookie string
_EXP: float = 0.0
_TASK: Optional[asyncio.Task] = None

# ---------------- Stealth-lite ----------------
STEALTH_JS = r"""
// Hide webdriver
Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
// languages
Object.defineProperty(navigator, 'languages', { get: () => ['en-US','en'] });
// plugins
Object.defineProperty(navigator, 'plugins', { get: () => [1,2,3,4,5] });
// chrome object
window.chrome = window.chrome || { runtime: {} };
// permissions noise
const originalQuery = window.navigator.permissions && window.navigator.permissions.query;
if (originalQuery) {
  window.navigator.permissions.query = (parameters) => (
    parameters && parameters.name === 'notifications'
      ? Promise.resolve({ state: Notification.permission })
      : originalQuery(parameters)
  );
}
"""

EXTRA_HEADERS_HTML = {
    "User-Agent": UA,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Upgrade-Insecure-Requests": "1",
    "sec-ch-ua": '"Chromium";v="126", "Not)A;Brand";v="24", "Google Chrome";v="126"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Windows"',
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-User": "?1",
    "Sec-Fetch-Dest": "document",
}

EXTRA_HEADERS_JSON = {
    "User-Agent": UA,
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "sec-ch-ua": '"Chromium";v="126", "Not)A;Brand";v="24", "Google Chrome";v="126"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Windows"',
    "Sec-Fetch-Site": "same-origin",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Dest": "empty",
    "Origin": "https://frontend-api.pump.fun",
    "Referer": "https://pump.fun/trending",
}

# ---------------- Internals ----------------
async def _ensure_chromium_available() -> None:
    """Install chromium once if missing."""
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
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT
        )
        try:
            await asyncio.wait_for(proc.communicate(), timeout=180)
        except asyncio.TimeoutError:
            proc.kill()
            log.warning("Playwright install timed out; continuing.")
    except Exception as e:
        log.warning("Playwright one-time install failed: %s", e)

def _cookie_for_host(all_cookies: List[Dict[str, Any]], host: str) -> Optional[str]:
    jar: Dict[str, str] = {}
    for c in all_cookies or []:
        name = c.get("name")
        domain = (c.get("domain") or "").lstrip(".")
        if domain.endswith(host) and name in ("cf_clearance", "__cf_bm"):
            val = c.get("value")
            if val:
                jar[name] = val
    if not jar:
        return None
    parts = []
    if "cf_clearance" in jar:
        parts.append(f"cf_clearance={jar['cf_clearance']}")
    if "__cf_bm" in jar:
        parts.append(f"__cf_bm={jar['__cf_bm']}")
    return "; ".join(parts)

async def _prime_api_host(page) -> None:
    """
    Stronger priming for the API host:
      1) Navigate to root HTML of https://frontend-api.pump.fun/ (challenge runs)
      2) Reload once
      3) Same-origin fetch to a small JSON path with credentials
    """
    roots = [
        "https://frontend-api.pump.fun/",
        "https://frontend-api.pump.fun/health",
    ]
    for root in roots:
        try:
            await page.set_extra_http_headers(EXTRA_HEADERS_HTML)
            resp: Optional[Response] = await page.goto(root, wait_until="domcontentloaded", timeout=60_000)
            await page.wait_for_timeout(1000)
            try:
                await page.reload(wait_until="domcontentloaded", timeout=20_000)
            except Exception:
                pass
            await page.wait_for_timeout(800)
            # If HTML loaded, CF likely ran; proceed to same-origin fetch
            break
        except Exception:
            continue

    # same-origin fetch to JSON path (forces cookies to be used/issued on the API host)
    try:
        await page.set_extra_http_headers(EXTRA_HEADERS_JSON)
        res = await page.evaluate(
            """async (path) => {
                try {
                  const r = await fetch(path, { credentials: 'include' });
                  return { ok: r.ok, status: r.status, ct: r.headers.get('content-type') || '' };
                } catch (e) { return { ok:false, status:0, ct:'' }; }
            }""",
            API_PRIME_PATH,
        )
        await page.wait_for_timeout(600)
        # If still hard-blocked, do a gentle location change to same path
        if not (res and res.get("ok")):
            try:
                await page.goto(f"https://frontend-api.pump.fun{API_PRIME_PATH}", wait_until="networkidle", timeout=30_000)
                await page.wait_for_timeout(800)
            except Exception:
                pass
    except Exception:
        pass

async def _prime_context_and_collect(ctx) -> Dict[str, str]:
    page = await ctx.new_page()
    await page.add_init_script(STEALTH_JS)

    # 1) Apex site
    try:
        await page.set_extra_http_headers(EXTRA_HEADERS_HTML)
        await page.goto(PUMP_CF_PAGE, wait_until="networkidle", timeout=60_000)
        await page.wait_for_timeout(600)
    except Exception:
        pass

    # 2) API host (must be primed via navigation + same-origin fetch)
    try:
        await _prime_api_host(page)
    except Exception:
        pass

    all_cookies = await ctx.cookies()
    await page.close()

    out: Dict[str, str] = {}
    for host in HOSTS:
        ck = _cookie_for_host(all_cookies, host)
        if ck:
            out[host] = ck
    return out

# ---------------- Refresher ----------------
async def _refresh_once() -> None:
    global _COOKIES, _EXP
    try:
        await _ensure_chromium_available()
        async with async_playwright() as p:
            browser = await p.chromium.launch(
                headless=True,
                args=["--no-sandbox", "--disable-dev-shm-usage", "--disable-setuid-sandbox",
                      "--disable-blink-features=AutomationControlled"],
            )
            try:
                ctx = await browser.new_context(
                    user_agent=UA,
                    locale="en-US",
                    timezone_id="America/New_York",
                    viewport={"width": 1366, "height": 768},
                    color_scheme="light",
                )
                new_map = await _prime_context_and_collect(ctx)

                # If API cookie still missing, retry once within same context
                if "frontend-api.pump.fun" not in new_map:
                    page = await ctx.new_page()
                    await page.add_init_script(STEALTH_JS)
                    try:
                        await _prime_api_host(page)
                    except Exception:
                        pass
                    await page.close()
                    # collect again
                    all_cookies = await ctx.cookies()
                    ck_api = _cookie_for_host(all_cookies, "frontend-api.pump.fun")
                    if ck_api:
                        new_map["frontend-api.pump.fun"] = ck_api
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

# ---------------- Public helpers ----------------
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

# ---------------- Last-resort JSON fetch ----------------
async def browser_fetch_json(url: str) -> Optional[Any]:
    try:
        await _ensure_chromium_available()
        async with async_playwright() as p:
            browser = await p.chromium.launch(
                headless=True,
                args=["--no-sandbox", "--disable-dev-shm-usage", "--disable-setuid-sandbox",
                      "--disable-blink-features=AutomationControlled"],
            )
            try:
                ctx = await browser.new_context(
                    user_agent=UA,
                    locale="en-US",
                    timezone_id="America/New_York",
                    viewport={"width": 1366, "height": 768},
                    color_scheme="light",
                )
                page = await ctx.new_page()
                await page.add_init_script(STEALTH_JS)

                host = urlparse(url).hostname or ""
                if host.endswith("frontend-api.pump.fun"):
                    # Prime this context for the API host
                    await _prime_api_host(page)

                    # Try navigation directly to the JSON URL first
                    try:
                        await page.set_extra_http_headers(EXTRA_HEADERS_JSON)
                        resp: Optional[Response] = await page.goto(url, wait_until="networkidle", timeout=45_000)
                        if resp and resp.ok:
                            text = await resp.text()
                            try:
                                return json.loads(text)
                            except Exception:
                                pass
                        status = resp.status if resp else 0
                        log.warning("browser_fetch_json: %s -> HTTP %s (page.goto)", url, status or 0)
                        if status in (403, 429, 530):
                            flag_cookie_bad()
                    except Exception:
                        pass

                    # Fallback: same-origin fetch from inside the page
                    try:
                        await page.set_extra_http_headers(EXTRA_HEADERS_JSON)
                        res = await page.evaluate(
                            """async (u) => {
                                try {
                                  const r = await fetch(u, { credentials: 'include' });
                                  if (!r.ok) return { ok:false, status:r.status };
                                  const ct = r.headers.get('content-type') || '';
                                  if (ct.includes('application/json')) return { ok:true, status:r.status, data: await r.json() };
                                  const t = await r.text();
                                  try { return { ok:true, status:r.status, data: JSON.parse(t) }; }
                                  catch { return { ok:false, status:r.status }; }
                                } catch (e) { return { ok:false, status:0 }; }
                            }""",
                            url,
                        )
                        if res and res.get("ok"):
                            return res.get("data")
                        status = res.get("status", 0) if isinstance(res, dict) else 0
                        log.warning("browser_fetch_json: %s -> HTTP %s (page.fetch)", url, status)
                        if status in (403, 429, 530):
                            flag_cookie_bad()
                        return None
                    finally:
                        await page.close()

                # Non-API host: use request client
                r = await ctx.request.get(url, headers=EXTRA_HEADERS_JSON, timeout=30_000)
                if r.ok:
                    try:
                        return await r.json()
                    except Exception:
                        t = await r.text()
                        try:
                            return json.loads(t)
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
