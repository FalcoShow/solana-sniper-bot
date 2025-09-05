# utils_pkg/pumpfun_cf.py — v2.1
"""
Cloudflare cookie manager for Pump.fun.

What this module provides:
- CF cookie refresher for BOTH hosts: `pump.fun` (apex) and `frontend-api.pump.fun` (API)
- Per-host cookie strings kept in-memory (cf_clearance, __cf_bm)
- Apply cookies to aiohttp sessions
- Last-resort JSON via Playwright; DOM and Next.js data helpers
- Full outbound proxy support for Playwright
- Per-host proxy selection (apex without proxy by default, API with proxy by default)
- Resilient refresh: apex and API are primed in SEPARATE browsers so one cannot stall the other
- Compatible with Render (no root needed, cached Chromium)
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse, unquote

from playwright.async_api import async_playwright

log = logging.getLogger("pumpfun.cf")

# =============================== Config ===============================

def _env_true(x: Optional[str]) -> bool:
    # Positive-list only so "0", "", random text => False
    s = str(x or "").strip().lower()
    return s in ("1", "true", "yes", "on")

UA = os.getenv(
    "PUMPFUN_UA",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
)

# Which apex page to hit to solve CF
PUMP_CF_PAGE = os.getenv("PUMPFUN_CF_URL", "https://pump.fun/advanced/scan")

# Small JSON on API host for priming inside Playwright
API_PRIME_PATH = os.getenv("PUMPFUN_API_PRIME_PATH", "/coins/trending?offset=0&limit=1")

# Cookie refresh cadence
REFRESH_SEC = int(os.getenv("PUMPFUN_CF_REFRESH_SEC", "900"))

# The two hosts we care about
HOSTS = ("pump.fun", "frontend-api.pump.fun")

# Per-host proxy toggles (defaults: apex=no proxy, API=proxy)
USE_PROXY_FOR_APEX = _env_true(os.getenv("PUMPFUN_APEX_VIA_PROXY", "0"))
USE_PROXY_FOR_API = _env_true(os.getenv("PUMPFUN_API_VIA_PROXY", "1"))

# DOM pages to try on apex (comma separated)
DOM_PAGES = [
    p.strip()
    for p in os.getenv("PUMPFUN_DOM_PAGES", "/board,/trending,/advanced/scan,/").split(",")
    if p.strip()
] or ["/trending", "/advanced/scan", "/"]

# Persist Playwright browser binaries somewhere writable on Render
os.environ.setdefault("PLAYWRIGHT_BROWSERS_PATH", "/opt/render/project/src/.cache/ms-playwright")

# ========================= In-Memory State ===========================

_COOKIES: Dict[str, str] = {}
_EXP: float = 0.0
_TASK: Optional[asyncio.Task] = None

# ============================== Proxy ================================

_PROXY_LOGGED: Dict[str, bool] = {"api": False, "apex": False}

def _resolve_proxy_url() -> Optional[str]:
    """
    Resolve a proxy URL from env (priority order):
      1) PLAYWRIGHT_PROXY
      2) PUMPFUN_PROXY
      3) HTTPS_PROXY
      4) HTTP_PROXY
    """
    return (
        os.getenv("PLAYWRIGHT_PROXY")
        or os.getenv("PUMPFUN_PROXY")
        or os.getenv("HTTPS_PROXY")
        or os.getenv("HTTP_PROXY")
    )

def _build_proxy_dict(raw: Optional[str]) -> Optional[dict]:
    """
    Build a Playwright proxy dict.

    Accepted formats:
      A) Full URL:  http://user:pass@host:port
      B) Colon form: host:port:user:pass
    """
    if not raw:
        return None

    if "://" not in raw and raw.count(":") >= 3:
        host, port, user, pwd = raw.split(":", 3)
        return {
            "server": f"http://{host}:{port}",
            "username": unquote(user),
            "password": unquote(pwd),
        }

    u = urlparse(raw)
    if u.scheme and u.hostname and u.port:
        proxy = {"server": f"{u.scheme}://{u.hostname}:{u.port}"}
        if u.username:
            proxy["username"] = unquote(u.username)
        if u.password:
            proxy["password"] = unquote(u.password)
        return proxy

    return None

def _mask_proxy_for_log(url: Optional[str]) -> str:
    try:
        u = urlparse(url or "")
        if not u.scheme:
            return url or ""
        hostport = f"{u.hostname or ''}:{u.port or ''}".rstrip(":")
        return f"{u.scheme}://{hostport}"
    except Exception:
        return url or ""

def _proxy_logged(kind: str) -> bool:
    return bool(_PROXY_LOGGED.get(kind, False))

def _set_proxy_logged(kind: str) -> None:
    _PROXY_LOGGED[kind] = True

async def _launch_browser(p, kind: str) -> Any:
    """
    Launch a Chromium browser with or without proxy depending on `kind`:
      - kind="api"  -> USE_PROXY_FOR_API
      - kind="apex" -> USE_PROXY_FOR_APEX
    """
    use_proxy = USE_PROXY_FOR_API if kind == "api" else USE_PROXY_FOR_APEX
    proxy_cfg = _build_proxy_dict(_resolve_proxy_url()) if use_proxy else None

    if proxy_cfg and not _proxy_logged(kind):
        log.info("Playwright proxy enabled (%s): %s", kind, _mask_proxy_for_log(_resolve_proxy_url()))
        _set_proxy_logged(kind)

    return await p.chromium.launch(
        headless=True,
        args=[
            "--no-sandbox",
            "--disable-dev-shm-usage",
            "--disable-setuid-sandbox",
            "--disable-blink-features=AutomationControlled",
        ],
        proxy=proxy_cfg,
    )

# =========================== Seeding from ENV ========================

def _seed_from_env() -> None:
    """Seed cookie jar from env so we can start even if CF priming is blocked."""
    seeded = []
    apex = (os.getenv("PUMPFUN_COOKIE") or "").strip()
    api = (os.getenv("PUMPFUN_COOKIE_API") or "").strip()
    if apex:
        _COOKIES["pump.fun"] = apex
        seeded.append("pump")
    if api:
        _COOKIES["frontend-api.pump.fun"] = api
        seeded.append("api")
    if seeded:
        log.info("CF seed: loaded cookies from env (%s)", ",".join(seeded))

_seed_from_env()

# Startup sanity line
log.info(
    "config: apex_via_proxy=%s api_via_proxy=%s proxy=%s refresh=%ss dom_pages=%s",
    USE_PROXY_FOR_APEX,
    USE_PROXY_FOR_API,
    _mask_proxy_for_log(_resolve_proxy_url()),
    REFRESH_SEC,
    ",".join(DOM_PAGES),
)

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
            b = await _launch_browser(p, "apex")
            await b.close()
        return
    except Exception:
        pass
    try:
        log.info("Playwright: chromium not found, attempting one-time install...")
        proc = await asyncio.create_subprocess_exec(
            "python", "-m", "playwright", "install", "chromium",
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT,
        )
        try:
            await asyncio.wait_for(proc.communicate(), timeout=180)
        except asyncio.TimeoutError:
            proc.kill()
            log.warning("Playwright install timed out; continuing.")
    except Exception as e:
        log.warning("Playwright one-time install failed: %s", e)

def _cookie_for_host(all_cookies: List[Dict[str, Any]], host: str) -> Optional[str]:
    """
    Build 'cf_clearance=...; __cf_bm=...' for the given host, if present.
    Accept apex cookie for API host when CF sets it at parent domain.
    """
    want = (host or "").lower()

    def _match(dom: str) -> bool:
        d = (dom or "").lstrip(".").lower()
        if d == want or d.endswith(want):
            return True
        # allow parent apex cookie for API subdomain
        if want.endswith(".pump.fun") and d == "pump.fun":
            return True
        return False

    jar: Dict[str, str] = {}
    for c in all_cookies or []:
        name = c.get("name")
        domain = c.get("domain") or ""
        if name in ("cf_clearance", "__cf_bm") and _match(domain):
            val = c.get("value")
            if val:
                jar[name] = val
    if not jar:
        return None
    parts: List[str] = []
    if jar.get("cf_clearance"):
        parts.append(f"cf_clearance={jar['cf_clearance']}")
    if jar.get("__cf_bm"):
        parts.append(f"__cf_bm={jar['__cf_bm']}")
    return "; ".join(parts) if parts else None

async def _prime_api_host(page) -> None:
    roots = ["https://frontend-api.pump.fun/", "https://frontend-api.pump.fun/health"]
    for root in roots:
        try:
            await page.set_extra_http_headers(EXTRA_HEADERS_HTML)
            await page.goto(root, wait_until="domcontentloaded", timeout=60_000)
            await page.wait_for_timeout(1200)
            try:
                await page.reload(wait_until="domcontentloaded", timeout=20_000)
            except Exception:
                pass
            await page.wait_for_timeout(1000)
            break
        except Exception:
            continue

    # Ensure we actually try to touch the JSON endpoint once
    try:
        await page.set_extra_http_headers(EXTRA_HEADERS_JSON)
        await page.evaluate(
            """async (path) => {
                try {
                  const r = await fetch(path, { credentials: 'include' });
                  return { ok: r.ok, status: r.status, ct: r.headers.get('content-type') || '' };
                } catch (e) { return { ok:false, status:0, ct:'' }; }
            }""",
            API_PRIME_PATH,
        )
        await page.wait_for_timeout(800)
    except Exception:
        pass

async def _prime_apex_and_collect() -> Dict[str, str]:
    """Prime apex in its OWN browser (no proxy by default) and collect cookies."""
    out: Dict[str, str] = {}
    async with async_playwright() as p:
        browser = await _launch_browser(p, "apex")
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
            try:
                await page.set_extra_http_headers(EXTRA_HEADERS_HTML)
                # longer timeout for fresh cf_clearance
                await page.goto(PUMP_CF_PAGE, wait_until="domcontentloaded", timeout=120_000)
                await page.wait_for_timeout(800)
            except Exception:
                pass

            all_cookies = await ctx.cookies()
            ck_apex = _cookie_for_host(all_cookies, "pump.fun")
            if ck_apex:
                out["pump.fun"] = ck_apex
        finally:
            await browser.close()
    return out

async def _prime_api_and_collect() -> Dict[str, str]:
    """Prime API in its OWN browser (proxy by default) and collect cookies."""
    out: Dict[str, str] = {}
    async with async_playwright() as p:
        browser = await _launch_browser(p, "api")
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
            try:
                await _prime_api_host(page)
            except Exception:
                pass

            all_cookies = await ctx.cookies()
            ck_api = _cookie_for_host(all_cookies, "frontend-api.pump.fun")
            if ck_api:
                out["frontend-api.pump.fun"] = ck_api
            # If CF set only apex cookie but it covers the API subdomain, accept it
            if not ck_api:
                ck_apex = _cookie_for_host(all_cookies, "pump.fun")
                if ck_apex:
                    out["frontend-api.pump.fun"] = ck_apex
        finally:
            await browser.close()
    return out

# ============================ Refresher ==============================

async def _refresh_once() -> None:
    """Refresh cookies; apex and API are primed independently to avoid proxy stalls."""
    global _COOKIES, _EXP
    prev = dict(_COOKIES)
    try:
        await _ensure_chromium_available()

        # Run apex and API prime concurrently (best effort)
        apex_task = asyncio.create_task(_prime_apex_and_collect())
        api_task = asyncio.create_task(_prime_api_and_collect())
        new_apex, new_api = await asyncio.gather(apex_task, api_task, return_exceptions=True)

        new_map: Dict[str, str] = {}
        if isinstance(new_apex, dict):
            new_map.update(new_apex)
        if isinstance(new_api, dict):
            new_map.update(new_api)

        # Merge: keep previous entries if not refreshed this round
        merged = dict(prev)
        merged.update(new_map)
        _COOKIES = merged

        # Reflect to env without clobbering if missing
        if "pump.fun" in new_map:
            os.environ["PUMPFUN_COOKIE"] = new_map["pump.fun"]
        elif prev.get("pump.fun"):
            os.environ.setdefault("PUMPFUN_COOKIE", prev["pump.fun"])

        if "frontend-api.pump.fun" in new_map:
            os.environ["PUMPFUN_COOKIE_API"] = new_map["frontend-api.pump.fun"]
        elif prev.get("frontend-api.pump.fun"):
            os.environ.setdefault("PUMPFUN_COOKIE_API", prev["frontend-api.pump.fun"])

        has_pump = "yes" if "pump.fun" in _COOKIES else "no"
        has_api = "yes" if "frontend-api.pump.fun" in _COOKIES else "no"

        # schedule next
        _EXP = time.time() + max(60.0, REFRESH_SEC * 0.9)
        log.info("CF refresh: got cookies (pump=%s, api=%s), next in ~%ds", has_pump, has_api, REFRESH_SEC)

    except Exception as e:
        # retry sooner
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

# ========================= Public Helpers ============================

def get_cookie_for(url_or_host: str) -> str:
    from urllib.parse import urlparse as _urlparse
    host = (_urlparse(url_or_host).hostname or url_or_host).lower()
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
    Try to fetch JSON with Playwright. For API host we use the API browser (proxy by default).
    For other hosts we use the apex browser (no proxy by default).
    """
    host = (urlparse(url).hostname or "").lower()
    kind = "api" if host.endswith("frontend-api.pump.fun") else "apex"
    try:
        await _ensure_chromium_available()
        async with async_playwright() as p:
            browser = await _launch_browser(p, kind)
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

                if host.endswith("frontend-api.pump.fun"):
                    # Prefer page-side fetch (goes through browser proxy)
                    try:
                        await page.set_extra_http_headers(EXTRA_HEADERS_JSON)
                        res = await page.evaluate(
                            """async (u) => {
                                try {
                                  const r = await fetch(u, { credentials: 'include' });
                                  const ct = r.headers.get('content-type') || '';
                                  const text = await r.text();
                                  if (!r.ok) return { ok:false, status:r.status, text };
                                  if (ct.includes('application/json')) {
                                    try { return { ok:true, status:r.status, data: JSON.parse(text) }; }
                                    catch { return { ok:false, status:r.status, text }; }
                                  }
                                  try { return { ok:true, status:r.status, data: JSON.parse(text) }; }
                                  catch { return { ok:false, status:r.status, text }; }
                                } catch (e) { return { ok:false, status:0, text:'' }; }
                            }""",
                            url,
                        )
                        if res and res.get("ok"):
                            return res["data"]
                        if isinstance(res, dict) and res.get("status") in (403, 407, 429, 530):
                            flag_cookie_bad()
                    except Exception:
                        pass

                    # Fallback: dedicated request context with explicit proxy creds
                    try:
                        proxy_cfg = _build_proxy_dict(_resolve_proxy_url()) if USE_PROXY_FOR_API else None
                        req = await p.request.new_context(
                            proxy=proxy_cfg,
                            extra_http_headers=EXTRA_HEADERS_JSON,
                        )
                        r = await req.get(url, timeout=30_000)
                        if r.ok:
                            try:
                                return await r.json()
                            except Exception:
                                return json.loads(await r.text())
                        if r.status in (403, 407, 429, 530):
                            flag_cookie_bad()
                        log.warning("browser_fetch_json: %s -> HTTP %s (reqctx)", url, r.status)
                        return None
                    finally:
                        await ctx.close()
                else:
                    # Non-API host: use request client directly
                    r = await ctx.request.get(url, headers=EXTRA_HEADERS_JSON, timeout=35_000)
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
                    if status in (403, 407, 429, 530):
                        flag_cookie_bad()
                    log.warning("browser_fetch_json: %s -> HTTP %s", url, status)
                    return None
            finally:
                await browser.close()
    except Exception as e:
        log.warning("browser_fetch_json failed: %s", e)
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

def _dom_pages() -> List[str]:
    raw = os.getenv("PUMPFUN_DOM_PAGES", "/board,/trending,/advanced/scan,/")
    return [p.strip() for p in raw.split(",") if p.strip()]

async def browser_fetch_next_json(page_path: Optional[str] = None) -> Optional[dict]:
    # Don’t try until we have an apex cookie
    if not _COOKIES.get("pump.fun"):
        log.debug("pumpfun: apex cookie not ready yet; skipping next-data fetch")
        return None

    pages = _dom_pages() if page_path is None else [page_path]
    base = "https://pump.fun"

    try:
        await _ensure_chromium_available()
        async with async_playwright() as p:
            browser = await _launch_browser(p, "apex")  # launch WITHOUT proxy
            try:
                ctx = await browser.new_context(
                    user_agent=UA, locale="en-US", timezone_id="America/New_York",
                    viewport={"width": 1366, "height": 768}, color_scheme="light",
                )
                page = await ctx.new_page()
                await page.add_init_script(STEALTH_JS)

                for path in pages:
                    try:
                        await page.set_extra_http_headers(EXTRA_HEADERS_HTML)
                        await page.goto(base + (path if path.startswith("/") else f"/{path}"),
                                        wait_until="domcontentloaded", timeout=60_000)
                        try:
                            await page.wait_for_selector('script#__NEXT_DATA__', timeout=20_000)
                        except Exception:
                            await page.wait_for_timeout(1200)

                        build_id = await _browser_get_build_id(page)
                        if not build_id:
                            log.debug("next-data: buildId not found for %s", path)
                            continue

                        tail = path.lstrip("/")
                        if not tail.endswith(".json"):
                            tail = (tail.rstrip("/") + ".json") if tail else "index.json"

                        data_url = f"{base}/_next/data/{build_id}/{tail}"
                        r = await ctx.request.get(data_url, headers=EXTRA_HEADERS_JSON, timeout=30_000)
                        if r.ok:
                            try:
                                return await r.json()
                            except Exception:
                                return json.loads(await r.text())
                    except Exception as e:
                        log.debug("next-data: %s failed: %s", path, e)
                log.debug("next-data: no items found on apex JSON for %s", pages)
                return None
            finally:
                await browser.close()
    except Exception as e:
        log.warning("browser_fetch_next_json failed: %s", e)
        return None

async def browser_fetch_trending_via_next_data(*, limit: int = 50) -> Optional[List[Dict[str, Any]]]:
    # unchanged signature so callers can pass limit=
    candidates = ["/trending", "/advanced/scan"]
    for path in candidates:
        data = await browser_fetch_next_json(path)
        if not isinstance(data, dict):
            continue
        page = data.get("pageProps", {}) if isinstance(data, dict) else {}
        for k in ("trending", "coins", "projects", "items", "data"):
            v = page.get(k)
            if isinstance(v, list) and v:
                out = v[:limit]
                log.info("next-data: got %d items from %s", len(out), path)
                return out
        if isinstance(page.get("data"), dict):
            for k, v in page["data"].items():
                if isinstance(v, list) and v:
                    out = v[:limit]
                    log.info("next-data: got %d items from %s (data.%s)", len(out), path, k)
                    return out
    log.debug("next-data: no items found on apex JSON for /trending or /advanced/scan")
    return None

async def browser_scrape_trending_dom(*, limit: int = 50, pages: Optional[List[str]] = None) -> Optional[List[Dict[str, Any]]]:
    # Don’t try until we have an apex cookie
    if not _COOKIES.get("pump.fun"):
        log.debug("pumpfun: apex cookie not ready yet; skipping DOM scrape")
        return None

    import re
    PAGES = pages or ["/board", "/trending", "/advanced/scan", "/"]

    def _uniq(seq: List[str]) -> List[str]:
        s, out = set(), []
        for x in seq:
            if x and x not in s:
                s.add(x)
                out.append(x)
        return out

    def _pair_to_cookies(cjar: Dict[str, str]) -> List[Dict[str, Any]]:
        cookies = []
        for name in ("cf_clearance", "__cf_bm"):
            if name in cjar:
                cookies.append(
                    {"name": name, "value": cjar[name], "domain": "pump.fun", "path": "/", "httpOnly": False, "secure": True, "sameSite": "Lax"}
                )
        return cookies

    BASE58 = r"[1-9A-HJ-NP-Za-km-z]{32,64}"

    try:
        await _ensure_chromium_available()
        async with async_playwright() as p:
            browser = await _launch_browser(p, "apex")
            try:
                ctx = await browser.new_context(
                    user_agent=UA, locale="en-US", timezone_id="America/New_York",
                    viewport={"width": 1366, "height": 768}, color_scheme="light",
                )

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

                    try:
                        await page.wait_for_timeout(800)
                        anchors = await page.locator('a[href*="/coin/"], a[href*="/coins/"], [data-mint]').all()
                        if anchors:
                            for _ in range(4):
                                await page.mouse.wheel(0, 1200)
                                await page.wait_for_timeout(250)

                        mints: List[str] = []
                        for el in anchors[: max(200, limit * 3)]:
                            href = await el.get_attribute("href")
                            dm = await el.get_attribute("data-mint")
                            if dm:
                                mints.append(dm)
                            if href:
                                h = href
                                if h.startswith("/"):
                                    h = f"https://pump.fun{h}"
                                m = re.search(rf"/coins?/(?P<m>{BASE58})", h)
                                if not m:
                                    m = re.search(rf"[?&]mint=(?P<m>{BASE58})", h)
                                if m:
                                    mints.append(m.group("m"))

                        mints = _uniq([m for m in mints if m])
                        if mints:
                            log.info("DOM-scrape: %d mints via selectors on %s", len(mints), path)
                            found.extend(mints)
                            if len(_uniq(found)) >= limit:
                                break

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
                if not uniq:
                    log.debug("DOM-scrape: no items found on %s", ",".join(PAGES))
                else:
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
    "browser_fetch_next_json",
    "browser_fetch_trending_via_next_data",
    "browser_scrape_trending_dom",
]
