# utils_pkg/pumpfun_cf.py
"""
Cloudflare cookie manager for Pump.fun.

What this module provides:
- CF cookie refresher for BOTH hosts: `pump.fun` and `frontend-api.pump.fun`
- Per-host cookie strings kept in-memory (cf_clearance, __cf_bm)
- Ability to apply cookies to an aiohttp session
- Last-resort JSON fetch via Playwright when the plain HTTP client fails
- **NEW:** DOM fallback that loads the apex Scan page and reads its Next.js
  data payload (works even when the API subdomain is 1016/530)

Env vars you can set:
- PUMPFUN_UA                  -> user agent to use (desktop UA is best)
- PUMPFUN_CF_URL              -> page used to prime CF on apex (default: /advanced/scan)
- PUMPFUN_API_PRIME_PATH      -> small JSON path used to prime the API host
- PUMPFUN_CF_REFRESH_SEC      -> refresh cadence (default 900)
- PUMPFUN_COOKIE              -> optional seed: "cf_clearance=...; __cf_bm=..." for apex
- PUMPFUN_COOKIE_API          -> optional seed for API host
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

# =============================== Config ===============================

UA = os.getenv(
    "PUMPFUN_UA",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
)

# Apex page we can always visit in a browser
PUMP_CF_PAGE = os.getenv("PUMPFUN_CF_URL", "https://pump.fun/advanced/scan")

# Small JSON on API host for priming (used only inside Playwright context)
API_PRIME_PATH = os.getenv("PUMPFUN_API_PRIME_PATH", "/coins/trending?offset=0&limit=1")

# Cookie refresh cadence
REFRESH_SEC = int(os.getenv("PUMPFUN_CF_REFRESH_SEC", "900"))

# The two hosts we care about
HOSTS = ("pump.fun", "frontend-api.pump.fun")

# Persist Playwright browser binaries in a writable location on Render
os.environ.setdefault("PLAYWRIGHT_BROWSERS_PATH", "/opt/render/.cache/ms-playwright")

# ========================= In-Memory State ===========================

_COOKIES: Dict[str, str] = {}
_EXP: float = 0.0
_TASK: Optional[asyncio.Task] = None


def _seed_from_env() -> None:
    """Seed cookie jar from env so we can work even if CF priming is blocked."""
    seeded = []
    apex = os.getenv("PUMPFUN_COOKIE", "").strip()
    api = os.getenv("PUMPFUN_COOKIE_API", "").strip()
    if apex:
        _COOKIES["pump.fun"] = apex
        seeded.append("pump")
    if api:
        _COOKIES["frontend-api.pump.fun"] = api
        seeded.append("api")
    if seeded:
        log.info("CF seed: loaded cookies from env (%s)", ",".join(seeded))


_seed_from_env()

# =========================== Stealth Lite ============================

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

# =========================== Internals ==============================


async def _ensure_chromium_available() -> None:
    """Install chromium once if missing (Render occasionally builds cold)."""
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
            "python",
            "-m",
            "playwright",
            "install",
            "chromium",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
        )
        try:
            await asyncio.wait_for(proc.communicate(), timeout=180)
        except asyncio.TimeoutError:
            proc.kill()
            log.warning("Playwright install timed out; continuing.")
    except Exception as e:
        log.warning("Playwright one-time install failed: %s", e)


def _cookie_for_host(all_cookies: List[Dict[str, Any]], host: str) -> Optional[str]:
    """Build 'cf_clearance=...; __cf_bm=...' for the given host, if present."""
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
      1) Navigate to root HTML of https://frontend-api.pump.fun/ (CF challenge runs)
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
            await page.goto(root, wait_until="domcontentloaded", timeout=60_000)
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
        if not (res and res.get("ok")):
            try:
                await page.goto(
                    f"https://frontend-api.pump.fun{API_PRIME_PATH}",
                    wait_until="networkidle",
                    timeout=30_000,
                )
                await page.wait_for_timeout(800)
            except Exception:
                pass
    except Exception:
        pass


async def _prime_context_and_collect(ctx) -> Dict[str, str]:
    """Prime both sites in a new context and collect cookies for the two hosts."""
    page = await ctx.new_page()
    await page.add_init_script(STEALTH_JS)

    # 1) Apex site
    try:
        await page.set_extra_http_headers(EXTRA_HEADERS_HTML)
        await page.goto(PUMP_CF_PAGE, wait_until="networkidle", timeout=60_000)
        await page.wait_for_timeout(600)
    except Exception:
        pass

    # 2) API host
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


# ============================ Refresher ==============================


async def _refresh_once() -> None:
    """Run one refresh cycle and schedule the next."""
    global _COOKIES, _EXP
    try:
        await _ensure_chromium_available()
        async with async_playwright() as p:
            browser = await p.chromium.launch(
                headless=True,
                args=[
                    "--no-sandbox",
                    "--disable-dev-shm-usage",
                    "--disable-setuid-sandbox",
                    "--disable-blink-features=AutomationControlled",
                ],
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

                # If API cookie still missing, retry once in same context
                if "frontend-api.pump.fun" not in new_map:
                    page = await ctx.new_page()
                    await page.add_init_script(STEALTH_JS)
                    try:
                        await _prime_api_host(page)
                    except Exception:
                        pass
                    await page.close()
                    all_cookies = await ctx.cookies()
                    ck_api = _cookie_for_host(all_cookies, "frontend-api.pump.fun")
                    if ck_api:
                        new_map["frontend-api.pump.fun"] = ck_api
            finally:
                await browser.close()

        _COOKIES = new_map
        # also reflect in os.environ so other places can read them if desired
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
    """Start the background refresher (call once at app start)."""
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


# ========================= Public Helpers ============================


def get_cookie_for(url_or_host: str) -> str:
    """Return the cookie string for this host (or empty string if absent)."""
    host = urlparse(url_or_host).hostname or url_or_host
    return _COOKIES.get(host, "")


def flag_cookie_bad() -> None:
    """Signal that cookies are stale; refresher will run ASAP."""
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


# ================= Last-resort JSON & DOM fetchers ===================


async def browser_fetch_json(url: str) -> Optional[Any]:
    """
    Try to fetch JSON with Playwright. Useful when the plain client gets 530/HTML.
    If the host is the API subdomain, we first try to prime that host in the same
    context, then attempt both page.goto and a same-origin fetch().
    """
    try:
        await _ensure_chromium_available()
        async with async_playwright() as p:
            browser = await p.chromium.launch(
                headless=True,
                args=[
                    "--no-sandbox",
                    "--disable-dev-shm-usage",
                    "--disable-setuid-sandbox",
                    "--disable-blink-features=AutomationControlled",
                ],
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

                host = (urlparse(url).hostname or "").lower()
                if host.endswith("frontend-api.pump.fun"):
                    # Prime this context for the API host
                    await _prime_api_host(page)

                    # Attempt navigation to the JSON URL (some CF configs allow it)
                    try:
                        await page.set_extra_http_headers(EXTRA_HEADERS_JSON)
                        resp: Optional[Response] = await page.goto(
                            url, wait_until="networkidle", timeout=45_000
                        )
                        if resp and resp.ok:
                            text = await resp.text()
                            try:
                                return json.loads(text)
                            except Exception:
                                pass
                        status = resp.status if resp else 0
                        log.warning(
                            "browser_fetch_json: %s -> HTTP %s (page.goto)", url, status or 0
                        )
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
                        log.warning(
                            "browser_fetch_json: %s -> HTTP %s (page.fetch)", url, status
                        )
                        if status in (403, 429, 530):
                            flag_cookie_bad()
                        return None
                    finally:
                        await page.close()

                # Non-API host: use request client directly
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


async def browser_fetch_scan_next_data(limit: int = 50) -> Optional[List[Dict[str, Any]]]:
    """
    Load the Pump.fun page in a real browser and pull the Next.js __NEXT_DATA__.
    Works even when the API subdomain is blocked. Returns a list of raw coin dicts.
    """
    targets = [
        os.getenv("PUMPFUN_CF_URL", "https://pump.fun/advanced/scan"),
        "https://pump.fun/trending",
    ]

    def _extract_from_next_data(payload: Any) -> List[Dict[str, Any]]:
        if not isinstance(payload, dict):
            return []
        page = payload.get("props", {}).get("pageProps", {})
        for k in ("trending", "coins", "items", "projects", "data"):
            v = page.get(k)
            if isinstance(v, list) and v:
                return v[:limit]
        return []

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

                for url in targets:
                    try:
                        await page.set_extra_http_headers(EXTRA_HEADERS_HTML)
                        # Be lenient: don't wait for networkidle; it often never happens with CF
                        await page.goto(url, wait_until="domcontentloaded", timeout=60_000)
                        # Wait (best-effort) for the Next.js data to appear
                        try:
                            await page.wait_for_selector('script#__NEXT_DATA__', timeout=45_000)
                        except Exception:
                            await page.wait_for_timeout(1500)

                        # Prefer reading window.__NEXT_DATA__ directly
                        payload = await page.evaluate("window.__NEXT_DATA__ || null")
                        if not payload:
                            # Fallback: parse the HTML for the script tag
                            html = await page.content()
                            import re, json as _json
                            m = re.search(r'id="__NEXT_DATA__"[^>]*>\s*({.*?})\s*</script>', html, re.S)
                            if m:
                                try:
                                    payload = _json.loads(m.group(1))
                                except Exception:
                                    payload = None

                        items = _extract_from_next_data(payload)
                        if items:
                            log.info("DOM-scrape: got %d items from %s", len(items), url)
                            return items
                        else:
                            log.warning("DOM-scrape: no items found at %s", url)
                    except Exception as e:
                        log.warning("browser_fetch_scan_next_data: %s failed: %s", url, e)

                return None
            finally:
                await browser.close()
    except Exception as e:
        log.warning("browser_fetch_scan_next_data failed: %s", e)
        return None

# === NEXT.JS DATA HELPERS (apex-only, works when API host is blocked) ===

async def _browser_get_build_id(page) -> Optional[str]:
    """Return Next.js buildId from window.__NEXT_DATA__ on the current page."""
    try:
        bid = await page.evaluate("window.__NEXT_DATA__ && window.__NEXT_DATA__.buildId || null")
        if bid and isinstance(bid, str):
            return bid
    except Exception:
        pass
    # Fallback: parse HTML for __NEXT_DATA__
    try:
        html = await page.content()
        import re, json as _json
        m = re.search(r'id="__NEXT_DATA__"[^>]*>\s*({.*?})\s*</script>', html, re.S)
        if m:
            j = _json.loads(m.group(1))
            bid = j.get("buildId")
            if bid and isinstance(bid, str):
                return bid
    except Exception:
        pass
    return None


async def browser_fetch_next_json(page_path: str) -> Optional[dict]:
    """
    Fetch a Next.js page's JSON via /_next/data/<buildId>/<path>.json on apex host.
    Example: page_path="/trending" -> /_next/data/<buildId>/trending.json
    """
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

                # 1) Load the page on apex (no API host involved)
                base = "https://pump.fun"
                path = page_path if page_path.startswith("/") else f"/{page_path}"
                await page.set_extra_http_headers(EXTRA_HEADERS_HTML)
                await page.goto(base + path, wait_until="domcontentloaded", timeout=60_000)
                try:
                    await page.wait_for_selector('script#__NEXT_DATA__', timeout=30_000)
                except Exception:
                    await page.wait_for_timeout(1500)

                # 2) Discover buildId, then fetch Next.js data JSON
                build_id = await _browser_get_build_id(page)
                if not build_id:
                    log.warning("next-data: buildId not found for %s", path)
                    return None

                # Construct the JSON URL
                # e.g. /_next/data/<buildId>/trending.json or /advanced/scan.json
                tail = path.lstrip("/")
                if not tail.endswith(".json"):
                    if not tail.endswith("/"):
                        tail = tail + ".json"
                    else:
                        tail = tail[:-1] + ".json"

                data_url = f"{base}/_next/data/{build_id}/{tail}"
                r = await ctx.request.get(data_url, headers=EXTRA_HEADERS_JSON, timeout=30_000)
                if r.ok:
                    try:
                        return await r.json()
                    except Exception:
                        t = await r.text()
                        import json as _json
                        try:
                            return _json.loads(t)
                        except Exception:
                            return None
                log.warning("next-data: fetch %s -> HTTP %s", data_url, r.status)
                return None
            finally:
                await browser.close()
    except Exception as e:
        log.warning("browser_fetch_next_json failed: %s", e)
        return None


async def browser_fetch_trending_via_next_data(limit: int = 50) -> Optional[List[Dict[str, Any]]]:
    """
    Get trending items via apex Next.js data JSON: first try /trending, then /advanced/scan.
    Returns a list of raw item dicts (not yet normalized).
    """
    candidates = ["/trending", "/advanced/scan"]
    for path in candidates:
        data = await browser_fetch_next_json(path)
        if not isinstance(data, dict):
            continue

        # Try common Next.js data shapes
        page = data.get("pageProps", {}) if isinstance(data, dict) else {}
        for k in ("trending", "coins", "projects", "items", "data"):
            v = page.get(k)
            if isinstance(v, list) and v:
                out = v[:limit]
                log.info("next-data: got %d items from %s", len(out), path)
                return out

        # Some apps nest under pageProps.data.<key>
        if isinstance(page.get("data"), dict):
            for k, v in page["data"].items():
                if isinstance(v, list) and v:
                    out = v[:limit]
                    log.info("next-data: got %d items from %s (data.%s)", len(out), path, k)
                    return out

    log.warning("next-data: no items found on apex JSON for /trending or /advanced/scan")
    return None

async def browser_scrape_trending_dom(limit: int = 50) -> Optional[List[Dict[str, Any]]]:
    """
    Load apex pages and pull mints without touching the API host.
    Tries DOM selectors first; if none found, falls back to regex over raw HTML.
    Returns [{'mint': <base58>}, ...]
    """
    from playwright.async_api import async_playwright
    from urllib.parse import urlparse

    PAGES = ["/trending", "/advanced/scan", "/"]  # try a few apex routes

    def _uniq(seq: List[str]) -> List[str]:
        s, out = set(), []
        for x in seq:
            if x and x not in s:
                s.add(x); out.append(x)
        return out

    # small helpers
    def _parse_cookie_string(s: str) -> Dict[str, str]:
        out: Dict[str, str] = {}
        for part in (s or "").split(";"):
            if "=" in part:
                k, v = part.split("=", 1)
                out[k.strip()] = v.strip()
        return out

    def _pair_to_cookies(cjar: Dict[str, str]) -> List[Dict[str, Any]]:
        cookies = []
        for name in ("cf_clearance", "__cf_bm"):
            if name in cjar:
                cookies.append({
                    "name": name,
                    "value": cjar[name],
                    "domain": "pump.fun",
                    "path": "/",
                    "httpOnly": False,
                    "secure": True,
                    "sameSite": "Lax",
                })
        return cookies

    BASE58 = r"[1-9A-HJ-NP-Za-km-z]{32,64}"

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

                # seed apex cookies into this context so CF doesn't bounce us
                apex_cookie_str = os.getenv("PUMPFUN_COOKIE", "").strip()
                if apex_cookie_str:
                    jar = _parse_cookie_string(apex_cookie_str)
                    seed = _pair_to_cookies(jar)
                    if seed:
                        await ctx.add_cookies(seed)

                page = await ctx.new_page()
                await page.add_init_script(STEALTH_JS)

                found: List[str] = []

                for path in PAGES:
                    url = f"https://pump.fun{path}"
                    try:
                        await page.set_extra_http_headers(EXTRA_HEADERS_HTML)
                        await page.goto(url, wait_until="domcontentloaded", timeout=60_000)
                    except Exception:
                        continue

                    # Give hydration a chance (but don’t depend on it)
                    try:
                        await page.wait_for_timeout(800)
                        # Try to find anchors to coin pages or elements with data-mint
                        anchors = await page.locator('a[href*="/coin/"], a[href*="/coins/"], [data-mint]').all()
                        if anchors:
                            # Try to scroll to reveal lazy items
                            for _ in range(4):
                                await page.mouse.wheel(0, 1200)
                                await page.wait_for_timeout(250)
                        # Collect mints from DOM
                        mints: List[str] = []
                        for el in anchors[: max(200, limit*3) ]:
                            href = await el.get_attribute("href")
                            dm = await el.get_attribute("data-mint")
                            if dm:
                                mints.append(dm)
                            if href:
                                # normalize relative
                                if href.startswith("/"):
                                    href = f"https://pump.fun{href}"
                                # match /coin/<mint> or /coins/<mint> or ?mint=<mint>
                                m = re.search(rf"/coins?/(?P<m>{BASE58})", href)
                                if not m:
                                    m = re.search(rf"[?&]mint=(?P<m>{BASE58})", href)
                                if m:
                                    mints.append(m.group("m"))

                        mints = _uniq([m for m in mints if m])
                        if mints:
                            log.info("DOM-scrape: %d mints via selectors on %s", len(mints), path)
                            found.extend(mints)
                            if len(_uniq(found)) >= limit:
                                break

                        # Fallback: regex over raw HTML (no JS required)
                        html = await page.content()
                        extra = []
                        for pat in [
                            rf'href="(?:https://pump\.fun)?/coin/({BASE58})"',
                            rf'href="(?:https://pump\.fun)?/coins/({BASE58})"',
                            rf'data-mint="({BASE58})"',
                            rf'"mintAddress"\s*:\s*"({BASE58})"',
                            rf'"mint"\s*:\s*"({BASE58})"',
                        ]:
                            extra += re.findall(pat, html)
                        extra = _uniq(extra)
                        if extra:
                            log.info("DOM-scrape: +%d mints via HTML regex on %s", len(extra), path)
                            found.extend(extra)
                            if len(_uniq(found)) >= limit:
                                break
                    except Exception:
                        continue

                uniq = _uniq(found)[:limit]
                await page.close()

                log.info("DOM-scrape total: %d unique mints", len(uniq))
                return [{"mint": m} for m in uniq]
            finally:
                await browser.close()
    except Exception as e:
        log.warning("browser_scrape_trending_dom failed: %s", e)
        return None

# ============================ Exports ================================

__all__ = [
    "ensure_cookie_refresher",
    "refresh_loop",
    "get_cookie_for",
    "apply_cookies_to_session",
    "flag_cookie_bad",
    "browser_fetch_json",
    "browser_fetch_scan_next_data",
    "browser_fetch_next_json", 
    "browser_fetch_trending_via_next_data",  
    "browser_scrape_trending_dom",
]
