# utils_pkg/pumpfun_cf.py — v2.2 (syntax fix + sturdier DOM waits)
"""
Cloudflare cookie manager for Pump.fun.

- CF cookie refresher for both hosts: pump.fun (apex) and frontend-api.pump.fun (API)
- Per-host cookie strings kept in-memory (cf_clearance, __cf_bm)
- Apply cookies to aiohttp sessions
- JSON fallback via Playwright; DOM & Next.js helpers
- Full outbound proxy support (URL or colon form)
- Per-host proxy selection: apex(no proxy by default) vs API(proxy by default)
- Separate browsers for apex and API so one cannot stall the other
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
    return str(x or "").strip().lower() not in ("", "0", "false", "no", "off")

UA = os.getenv(
    "PUMPFUN_UA",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
)

# Page we hit to get CF cookies on apex (you set PUMPFUN_CF_URL=https://pump.fun/trending)
PUMP_CF_PAGE = os.getenv("PUMPFUN_CF_URL", "https://pump.fun/advanced/scan")
API_PRIME_PATH = os.getenv("PUMPFUN_API_PRIME_PATH", "/coins/trending?offset=0&limit=1")
REFRESH_SEC = int(os.getenv("PUMPFUN_CF_REFRESH_SEC", "900"))

HOSTS = ("pump.fun", "frontend-api.pump.fun")

USE_PROXY_FOR_APEX = _env_true(os.getenv("PUMPFUN_APEX_VIA_PROXY", "0"))
USE_PROXY_FOR_API = _env_true(os.getenv("PUMPFUN_API_VIA_PROXY", "1"))

# DOM pages list (allow empty env to fallback to defaults)
_dom_env = os.getenv("PUMPFUN_DOM_PAGES", "/board,/trending,/advanced/scan,/")
DOM_PAGES = [p.strip() for p in _dom_env.split(",") if p.strip()] or ["/trending", "/advanced/scan", "/"]

# Persist Playwright browser binaries somewhere writable on Render
os.environ.setdefault("PLAYWRIGHT_BROWSERS_PATH", "/opt/render/project/src/.cache/ms-playwright")

# ========================= In-Memory State ===========================

_COOKIES: Dict[str, str] = {}
_EXP: float = 0.0
_TASK: Optional[asyncio.Task] = None

# ============================== Proxy ================================

_PROXY_LOGGED: Dict[str, bool] = {"api": False, "apex": False}

def _resolve_proxy_url() -> Optional[str]:
    """Resolve a proxy string (URL or colon) from env variables in priority order."""
    return (
        os.getenv("PLAYWRIGHT_PROXY")
        or os.getenv("PUMPFUN_PROXY")
        or os.getenv("HTTPS_PROXY")
        or os.getenv("HTTP_PROXY")
    )

def _build_proxy_dict(raw: Optional[str]) -> Optional[dict]:
    """
    Build a Playwright proxy dict from:
      A) URL   -> http[s]://user:pass@host:port
      B) Colon -> host:port:user:pass
    """
    if not raw:
        return None

    # Colon form: host:port:user:pass
    if "://" not in raw and raw.count(":") >= 3:
        host, port, user, pwd = raw.split(":", 3)
        return {
            "server": f"http://{host}:{port}",
            "username": unquote(user),
            "password": unquote(pwd),
        }

    # URL form
    u = urlparse(raw)
    if u.scheme and u.hostname and u.port:
        proxy = {"server": f"{u.scheme}://{u.hostname}:{u.port}"}
        if u.username:
            proxy["username"] = unquote(u.username)
        if u.password:
            proxy["password"] = unquote(u.password)
        return proxy

    return None

def _mask_proxy_for_log(url: str) -> str:
    try:
        u = urlparse(url or "")
        if not u.scheme:
            if url.count(":") >= 3:
                host, port, *_ = url.split(":", 3)
                return f"http://{host}:{port}"
            return url
        hostport = f"{u.hostname or ''}:{u.port or ''}".rstrip(":")
        return f"{u.scheme}://{hostport}"
    except Exception:
        return url or ""

async def _launch_browser(p, kind: str):
    """Launch Chromium with/without proxy depending on `kind` ('api' or 'apex')."""
    use_proxy = USE_PROXY_FOR_API if kind == "api" else USE_PROXY_FOR_APEX
    proxy_cfg = _build_proxy_dict(_resolve_proxy_url()) if use_proxy else None

    if proxy_cfg and not _PROXY_LOGGED.get(kind):
        raw = _resolve_proxy_url() or ""
        log.info("Playwright proxy enabled (%s): %s", kind, _mask_proxy_for_log(raw))
        _PROXY_LOGGED[kind] = True

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

# =========================== Stealth Lite ============================

STEALTH_JS = r"""
Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
Object.defineProperty(navigator, 'languages', { get: () => ['en-US','en'] });
Object.defineProperty(navigator, 'plugins', { get: () => [1,2,3,4,5] });
window.chrome = window.chrome || { runtime: {} };
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
    """Build 'cf_clearance=...; __cf_bm=...' string for the given host if present."""
    want = (host or "").lower()

    def _match(dom: str) -> bool:
        d = (dom or "").lstrip(".").lower()
        if d == want or d.endswith(want):
            return True
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
    """Light-touch visit to API origin so CF sets cookies at subdomain or parent."""
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

    # Try a JSON fetch inside the page context; ignore failures
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
        await page.wait_for_timeout(800)
        if not (res and res.get("ok")):
            try:
                # Just hit the same path again to warm cookies; ignore result
                await page.goto("https://frontend-api.pump.fun" + API_PRIME_PATH, wait_until="networkidle", timeout=30_000)
            except Exception:
                pass
    except Exception:
        pass

async def _prime_apex_and_collect() -> Dict[str, str]:
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
                await page.goto(PUMP_CF_PAGE, wait_until="domcontentloaded", timeout=90_000)
                # wait for any visible coin anchors
                try:
                    await page.wait_for_selector('a[href*="/coin/"], a[href*="/coins/"], [data-mint]', timeout=10_000)
                except Exception:
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
            if not ck_api:
                ck_apex = _cookie_for_host(all_cookies, "pump.fun")
                if ck_apex:
                    out["frontend-api.pump.fun"] = ck_apex
        finally:
            await browser.close()
    return out

# ============================ Refresher ==============================

async def _refresh_once() -> None:
    global _COOKIES, _EXP
    prev = dict(_COOKIES)
    try:
        await _ensure_chromium_available()

        apex_task = asyncio.create_task(_prime_apex_and_collect())
        api_task = asyncio.create_task(_prime_api_and_collect())
        new_apex, new_api = await asyncio.gather(apex_task, api_task, return_exceptions=True)

        new_map: Dict[str, str] = {}
        if isinstance(new_apex, dict):
            new_map.update(new_apex)
        if isinstance(new_api, dict):
            new_map.update(new_api)

        merged = dict(prev)
        merged.update(new_map)
        _COOKIES = merged

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

        _EXP = time.time() + max(60.0, REFRESH_SEC * 0.9)
        log.info("CF refresh: got cookies (pump=%s, api=%s), next in ~%ds", has_pump, has_api, REFRESH_SEC)

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
    raw_px = _resolve_proxy_url() or ""
    log.info(
        "config: apex_via_proxy=%s api_via_proxy=%s proxy=%s refresh=%ss dom_pages=%s",
        str(USE_PROXY_FOR_APEX), str(USE_PROXY_FOR_API), _mask_proxy_for_log(raw_px), REFRESH_SEC, ",".join(DOM_PAGES)
    )
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
    Fetch JSON with Playwright.
    - API host uses API browser (proxy by default)
    - Others use apex browser (no proxy by default)
    Seeds cookies for API host if present in env.
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

                # Seed API cookies when available
                if kind == "api":
                    seed = (os.getenv("PUMPFUN_COOKIE_API") or os.getenv("PUMPFUN_COOKIE") or "").strip()
                    if seed:
                        jar = _parse_cookie_string(seed)
                        cookies = []
                        for name in ("cf_clearance", "__cf_bm"):
                            if name in jar:
                                cookies.append({
                                    "name": name, "value": jar[name],
                                    "domain": "frontend-api.pump.fun", "path": "/",
                                    "httpOnly": False, "secure": True, "sameSite": "Lax"
                                })
                        if cookies:
                            await ctx.add_cookies(cookies)

                page = await ctx.new_page()
                await page.add_init_script(STEALTH_JS)

                # 1) page.goto
                try:
                    await page.set_extra_http_headers(EXTRA_HEADERS_JSON)
                    resp = await page.goto(url, wait_until="networkidle", timeout=30_000)
                    if resp and resp.ok:
                        try:
                            return await resp.json()
                        except Exception:
                            text = await resp.text()
                            try:
                                return json.loads(text)
                            except Exception:
                                return None
                    if resp:
                        status = resp.status
                        if status in (403, 407, 429, 530):
                            flag_cookie_bad()
                        log.warning("browser_fetch_json: %s -> HTTP %s (page.goto)", url, status)
                except Exception:
                    pass

                # 2) page.evaluate fetch
                try:
                    result = await page.evaluate(
                        """async (u) => {
                            try {
                              const r = await fetch(u, { credentials: 'include' });
                              const ct = r.headers.get('content-type') || '';
                              if (!r.ok) return { ok:false, status:r.status };
                              if (ct.includes('application/json')) return { ok:true, data: await r.json(), status:r.status };
                              const t = await r.text();
                              try { return { ok:true, data: JSON.parse(t), status:r.status }; }
                              catch { return { ok:false, status:r.status }; }
                            } catch (e) { return { ok:false, status:0 }; }
                        }""",
                        url,
                    )
                    if isinstance(result, dict) and result.get("ok"):
                        return result.get("data")
                    st = int(result.get("status") or 0) if isinstance(result, dict) else 0
                    if st in (403, 407, 429, 530):
                        flag_cookie_bad()
                    if st:
                        log.warning("browser_fetch_json: %s -> HTTP %s (page.fetch)", url, st)
                except Exception:
                    pass

                # 3) explicit request context
                try:
                    raw = _resolve_proxy_url()
                    req = await p.request.new_context(
                        proxy=_build_proxy_dict(raw) if kind == "api" else None,
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
                except Exception:
                    pass
            finally:
                await browser.close()
    except Exception as e:
        log.warning("browser_fetch_json failed: %s", e)
    return None

# === NEXT.JS DATA HELPERS (apex-only) ===

async def _browser_get_build_id(page) -> Optional[str]:
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

async def browser_fetch_next_json(page_path: Optional[str] = None) -> Optional[dict]:
    pages = DOM_PAGES if page_path is None else [page_path]
    base = "https://pump.fun"

    try:
        await _ensure_chromium_available()
        async with async_playwright() as p:
            browser = await _launch_browser(p, "apex")  # apex: no proxy by default
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
            finally:
                await browser.close()
    except Exception as e:
        log.debug("browser_fetch_next_json failed: %s", e)
    return None

async def browser_fetch_trending_via_next_data(*, limit: int = 50) -> Optional[List[Dict[str, Any]]]:
    for path in ("/trending", "/advanced/scan"):
        data = await browser_fetch_next_json(path)
        if not isinstance(data, dict):
            continue
        page = data.get("pageProps", {}) if isinstance(data, dict) else {}
        for k in ("trending", "coins", "projects", "items", "data"):
            v = page.get(k)
            if isinstance(v, list) and v:
                return v[:limit]
        if isinstance(page.get("data"), dict):
            for _, v in page["data"].items():
                if isinstance(v, list) and v:
                    return v[:limit]
    log.debug("next-data: no items found on apex JSON for /trending or /advanced/scan")
    return None

async def browser_scrape_trending_dom(*, limit: int = 50, pages: Optional[List[str]] = None) -> Optional[List[Dict[str, Any]]]:
    import re
    PAGES = pages or ["/board", "/trending", "/advanced/scan", "/"]

    def _uniq(seq: List[str]) -> List[str]:
        s, out = set(), []
        for x in seq:
            if x and x not in s:
                s.add(x)
                out.append(x)
        return out

    try:
        await _ensure_chromium_available()
        async with async_playwright() as p:
            browser = await _launch_browser(p, "apex")
            try:
                ctx = await browser.new_context(
                    user_agent=UA, locale="en-US", timezone_id="America/New_York",
                    viewport={"width": 1366, "height": 768}, color_scheme="light",
                )

                # seed apex cookie if provided
                apex_cookie_str = (os.getenv("PUMPFUN_COOKIE") or "").strip()
                if apex_cookie_str:
                    jar = _parse_cookie_string(apex_cookie_str)
                    cookies = []
                    for name in ("cf_clearance", "__cf_bm"):
                        if name in jar:
                            cookies.append(
                                {"name": name, "value": jar[name], "domain": "pump.fun", "path": "/", "httpOnly": False, "secure": True, "sameSite": "Lax"}
                            )
                    if cookies:
                        await ctx.add_cookies(cookies)

                page = await ctx.new_page()
                await page.add_init_script(STEALTH_JS)

                found: List[str] = []
                BASE58 = r"[1-9A-HJ-NP-Za-km-z]{32,44}"

                for path in PAGES:
                    url = f"https://pump.fun{path}"
                    try:
                        await page.set_extra_http_headers(EXTRA_HEADERS_HTML)
                        await page.goto(url, wait_until="domcontentloaded", timeout=60_000)
                    except Exception:
                        continue

                    try:
                        # scroll a bit to let lazy content populate
                        anchors = await page.locator('a[href*="/coin/"], a[href*="/coins/"], [data-mint]').all()
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
                                if href.startswith("/"):
                                    href = f"https://pump.fun{href}"
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
