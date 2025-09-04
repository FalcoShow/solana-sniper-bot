# utils_pkg/pumpfun_cf.py — v1.4
"""
Cloudflare helper for Pump.fun

- Cookie refresher for hosts: pump.fun and frontend-api.pump.fun
- Per-host cookie strings in-memory (cf_clearance, __cf_bm)
- Apply cookies to aiohttp sessions
- Playwright JSON fallback; DOM + Next.js helpers
- Full outbound proxy support (Chromium + request client)
- No top-level await; accepts limit= kwarg (scanner compat)
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
PUMP_CF_PAGE = os.getenv("PUMPFUN_CF_URL", "https://pump.fun/advanced/scan")
API_PRIME_PATH = os.getenv("PUMPFUN_API_PRIME_PATH", "/coins/trending?offset=0&limit=1")
REFRESH_SEC = int(os.getenv("PUMPFUN_CF_REFRESH_SEC", "900"))
HOSTS = ("pump.fun", "frontend-api.pump.fun")

# Persist PW binaries somewhere writable on Render
os.environ.setdefault("PLAYWRIGHT_BROWSERS_PATH", "/opt/render/project/src/.cache/ms-playwright")

# ========================= In-Memory State ===========================

_COOKIES: Dict[str, str] = {}
_EXP: float = 0.0
_TASK: Optional[asyncio.Task] = None

# ============================== Proxy ================================

_PROXY_LOGGED = False


def _resolve_proxy_url() -> Optional[str]:
    return (
        os.getenv("PLAYWRIGHT_PROXY")
        or os.getenv("PUMPFUN_PROXY")
        or os.getenv("HTTPS_PROXY")
        or os.getenv("HTTP_PROXY")
    )


def _normalize_proxy(raw: str) -> Optional[dict]:
    """
    Accept:
      - http://user:pass@host:port
      - host:port:user:pass  (normalize to http://user:pass@host:port)
    """
    if not raw:
        return None
    if "://" not in raw and raw.count(":") >= 3:
        host, port, user, pwd = raw.split(":", 4)[:4]
        return {"server": f"http://{host}:{port}", "username": user, "password": pwd}

    u = urlparse(raw)
    if not (u.scheme and u.hostname and u.port):
        return None
    p = {"server": f"{u.scheme}://{u.hostname}:{u.port}"}
    if u.username:
        p["username"] = u.username
    if u.password:
        p["password"] = u.password
    return p


def _mask_proxy_for_log(url: str) -> str:
    try:
        if "://" not in url and url.count(":") >= 1:
            host, port = url.split(":", 2)[:2]
            return f"http://{host}:{port}"
        u = urlparse(url)
        return f"{u.scheme}://{u.hostname}:{u.port}"
    except Exception:
        return url or ""


async def _launch_browser(p):
    """Launch Chromium with proxy + hardened args."""
    global _PROXY_LOGGED
    proxy_cfg = _normalize_proxy(_resolve_proxy_url() or "")
    if proxy_cfg and not _PROXY_LOGGED:
        log.info("Playwright proxy enabled: %s", _mask_proxy_for_log(_resolve_proxy_url() or ""))
        _PROXY_LOGGED = True

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


async def _new_request_context(p):
    """
    Create a Playwright request context that ALSO uses the proxy with credentials.
    Do NOT rely on ctx.request; it may not inherit proxy auth.
    """
    proxy_cfg = _normalize_proxy(_resolve_proxy_url() or "")
    return await p.request.new_context(proxy=proxy_cfg, user_agent=UA)


def _seed_from_env() -> None:
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

# ======================= Playwright bootstrap ========================


async def _ensure_chromium_available() -> None:
    try:
        async with async_playwright() as p:
            b = await _launch_browser(p)
            await b.close()
        return
    except Exception:
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

# ============================ Cookie utils ===========================


def _cookie_for_host(all_cookies: List[Dict[str, Any]], want_host: str) -> Optional[str]:
    want = (want_host or "").lower()

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
    parts = []
    if "cf_clearance" in jar:
        parts.append(f"cf_clearance={jar['cf_clearance']}")
    if "__cf_bm" in jar:
        parts.append(f"__cf_bm={jar['__cf_bm']}")
    return "; ".join(parts)


async def _prime_api_host(page) -> None:
    roots = ["https://frontend-api.pump.fun/", "https://frontend-api.pump.fun/health"]
    for root in roots:
        try:
            await page.set_extra_http_headers(EXTRA_HEADERS_HTML)
            await page.goto(root, wait_until="domcontentloaded", timeout=60_000)
            await page.wait_for_timeout(900)
            try:
                await page.reload(wait_until="domcontentloaded", timeout=20_000)
            except Exception:
                pass
            await page.wait_for_timeout(600)
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
                await page.goto(f"https://frontend-api.pump.fun{API_PRIME_PATH}",
                                wait_until="networkidle", timeout=30_000)
                await page.wait_for_timeout(600)
            except Exception:
                pass
    except Exception:
        pass


async def _prime_context_and_collect(ctx) -> Dict[str, str]:
    page = await ctx.new_page()
    await page.add_init_script(STEALTH_JS)

    # 1) apex
    try:
        await page.set_extra_http_headers(EXTRA_HEADERS_HTML)
        await page.goto(PUMP_CF_PAGE, wait_until="networkidle", timeout=60_000)
        await page.wait_for_timeout(600)
    except Exception:
        pass

    # 2) api
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
    global _COOKIES, _EXP
    prev = dict(_COOKIES)
    try:
        await _ensure_chromium_available()
        async with async_playwright() as p:
            browser = await _launch_browser(p)
            try:
                ctx = await browser.new_context(
                    user_agent=UA,
                    locale="en-US",
                    timezone_id="America/New_York",
                    viewport={"width": 1366, "height": 768},
                    color_scheme="light",
                )
                new_map = await _prime_context_and_collect(ctx)

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

        merged = dict(prev)
        merged.update(new_map)
        _COOKIES = merged

        if "pump.fun" in new_map:
            os.environ["PUMPFUN_COOKIE"] = new_map["pump.fun"]
        if "frontend-api.pump.fun" in new_map:
            os.environ["PUMPFUN_COOKIE_API"] = new_map["frontend-api.pump.fun"]

        _EXP = time.time() + max(60.0, REFRESH_SEC * 0.9)
        log.info(
            "CF refresh: got cookies (pump=%s, api=%s), next in ~%ds",
            "yes" if "pump.fun" in _COOKIES else "no",
            "yes" if "frontend-api.pump.fun" in _COOKIES else "no",
            REFRESH_SEC,
        )
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

# ========================= Public Helpers ============================


def get_cookie_for(url_or_host: str) -> str:
    host = (urlparse(url_or_host).hostname or url_or_host).lower()
    return _COOKIES.get(host, "")


def flag_cookie_bad() -> None:
    global _EXP
    _EXP = 0.0


def _parse_cookie_string(s: str) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for part in (s or "").split(";"):
        part = part.strip()
        if not part or "=" not in part:
            continue
        k, v = part.split("=", 1)
        out[k.strip()] = v.strip()
    return out


def apply_cookies_to_session(session) -> None:
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
    Robust JSON fetch in a real browser context.

    API host:
      1) prime CF
      2) try page.goto
      3) try window.fetch
      4) try playwright.request context (with proxy credentials)

    Non-API:
      - use playwright.request context (with proxy credentials)
    """
    try:
        await _ensure_chromium_available()
        async with async_playwright() as p:
            req = await _new_request_context(p)
            browser = await _launch_browser(p)
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
                    # prime
                    await _prime_api_host(page)

                    # 1) page.goto
                    try:
                        await page.set_extra_http_headers(EXTRA_HEADERS_JSON)
                        resp: Optional[Response] = await page.goto(
                            url, wait_until="networkidle", timeout=45_000
                        )
                        if resp and resp.ok:
                            t = await resp.text()
                            try:
                                return json.loads(t)
                            except Exception:
                                pass
                        st = resp.status if resp else 0
                        if st in (403, 429, 530):
                            flag_cookie_bad()
                        if st:
                            log.warning("browser_fetch_json: %s -> HTTP %s (page.goto)", url, st)
                    except Exception as e:
                        log.warning("browser_fetch_json: page.goto error: %s", e)

                    # 2) window.fetch
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
                                } catch { return { ok:false, status:0 }; }
                            }""",
                            url,
                        )
                        if res and res.get("ok"):
                            return res.get("data")
                        st = res.get("status", 0) if isinstance(res, dict) else 0
                        if st in (403, 429, 530):
                            flag_cookie_bad()
                        if st:
                            log.warning("browser_fetch_json: %s -> HTTP %s (page.fetch)", url, st)
                    except Exception as e:
                        log.warning("browser_fetch_json: page.fetch error: %s", e)

                    # 3) playwright.request (with proxy creds)
                    try:
                        r = await req.get(url, headers=EXTRA_HEADERS_JSON, timeout=30_000)
                        if r.ok:
                            try:
                                return await r.json()
                            except Exception:
                                t = await r.text()
                                try:
                                    return json.loads(t)
                                except Exception:
                                    return None
                        if r.status in (403, 407, 429, 530):
                            flag_cookie_bad()
                        log.warning("browser_fetch_json: %s -> HTTP %s (req)", url, r.status)
                    except Exception as e:
                        log.warning("browser_fetch_json: req error: %s", e)

                    await page.close()
                    return None

                # Non-API host: use playwright.request
                try:
                    r = await req.get(url, headers=EXTRA_HEADERS_JSON, timeout=30_000)
                    if r.ok:
                        try:
                            return await r.json()
                        except Exception:
                            t = await r.text()
                            try:
                                return json.loads(t)
                            except Exception:
                                return None
                    if r.status in (403, 407, 429, 530):
                        flag_cookie_bad()
                    log.warning("browser_fetch_json: %s -> HTTP %s", url, r.status)
                    return None
                finally:
                    await page.close()
            finally:
                await browser.close()
                await req.dispose()
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
        import re as _re, json as _json
        m = _re.search(r'id="__NEXT_DATA__"[^>]*>\s*({.*?})\s*</script>', html, _re.S)
        if m:
            j = _json.loads(m.group(1))
            bid = j.get("buildId")
            if bid and isinstance(bid, str):
                return bid
    except Exception:
        pass
    return None


async def browser_fetch_next_json(page_path: str) -> Optional[dict]:
    try:
        await _ensure_chromium_available()
        async with async_playwright() as p:
            req = await _new_request_context(p)
            browser = await _launch_browser(p)
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

                base = "https://pump.fun"
                path = page_path if page_path.startswith("/") else f"/{page_path}"

                await page.set_extra_http_headers(EXTRA_HEADERS_HTML)
                await page.goto(base + path, wait_until="domcontentloaded", timeout=60_000)
                try:
                    await page.wait_for_selector('script#__NEXT_DATA__', timeout=30_000)
                except Exception:
                    await page.wait_for_timeout(1200)

                build_id = await _browser_get_build_id(page)
                if not build_id:
                    log.warning("next-data: buildId not found for %s", path)
                    await page.close()
                    return None

                tail = path.lstrip("/")
                if tail.endswith("/"):
                    tail = tail[:-1]
                if not tail.endswith(".json"):
                    tail = f"{tail}.json"

                data_url = f"{base}/_next/data/{build_id}/{tail}"
                r = await req.get(data_url, headers=EXTRA_HEADERS_JSON, timeout=30_000)
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
                await page.close()
                return None
            finally:
                await browser.close()
                await req.dispose()
    except Exception as e:
        log.warning("browser_fetch_next_json failed: %s", e)
        return None


async def browser_fetch_trending_via_next_data(limit: int = 50) -> Optional[List[Dict[str, Any]]]:
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
    log.warning("next-data: no items found on apex JSON for /trending or /advanced/scan")
    return None


async def browser_scrape_trending_dom(limit: int = 50) -> Optional[List[Dict[str, Any]]]:
    """
    Load apex pages and pull mints without touching the API host.
    Returns [{'mint': <base58>}, ...]
    """
    import re

    env_pages = os.getenv("PUMPFUN_DOM_PAGES", "/trending,/advanced/scan,/")
    PAGES = [p.strip() for p in env_pages.split(",") if p.strip()] or ["/trending", "/advanced/scan", "/"]

    def _uniq(seq: List[str]) -> List[str]:
        s, out = set(), []
        for x in seq:
            if x and x not in s:
                s.add(x)
                out.append(x)
        return out

    def _parse_cookie_string_local(s: str) -> Dict[str, str]:
        out: Dict[str, str] = {}
        for part in (s or "").split(";"):
            part = part.strip()
            if not part or "=" not in part:
                continue
            k, v = part.split("=", 1)
            out[k.strip()] = v.strip()
        return out

    def _pair_to_cookies(cjar: Dict[str, str]) -> List[Dict[str, Any]]:
        cookies = []
        for name in ("cf_clearance", "__cf_bm"):
            if name in cjar and cjar[name]:
                cookies.append(
                    {
                        "name": name,
                        "value": cjar[name],
                        "domain": "pump.fun",
                        "path": "/",
                        "httpOnly": False,
                        "secure": True,
                        "sameSite": "Lax",
                    }
                )
        return cookies

    BASE58 = r"[1-9A-HJ-NP-Za-km-z]{32,64}"

    try:
        await _ensure_chromium_available()
        async with async_playwright() as p:
            browser = await _launch_browser(p)
            try:
                ctx = await browser.new_context(
                    user_agent=UA,
                    locale="en-US",
                    timezone_id="America/New_York",
                    viewport={"width": 1366, "height": 768},
                    color_scheme="light",
                )

                apex_cookie_str = os.getenv("PUMPFUN_COOKIE", "").strip()
                if apex_cookie_str:
                    jar = _parse_cookie_string_local(apex_cookie_str)
                    seed = _pair_to_cookies(jar)
                    if seed:
                        try:
                            await ctx.add_cookies(seed)
                        except Exception:
                            pass

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
                        await page.wait_for_timeout(600)
                        anchors = await page.locator('a[href*="/coin/"], a[href*="/coins/"], [data-mint]').all()
                        if anchors:
                            for _ in range(4):
                                await page.mouse.wheel(0, 1400)
                                await page.wait_for_timeout(250)

                        mints: List[str] = []
                        for el in anchors[: max(200, limit * 3)]:
                            href = await el.get_attribute("href")
                            dm = await el.get_attribute("data-mint")
                            if dm:
                                mints.append(dm)
                            if href:
                                link = href if not href.startswith("/") else f"https://pump.fun{href}"
                                m = re.search(rf"/coins?/(?P<m>{BASE58})", link)
                                if not m:
                                    m = re.search(rf"[?&]mint=(?P<m>{BASE58})", link)
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
    "browser_fetch_next_json",
    "browser_fetch_trending_via_next_data",
    "browser_scrape_trending_dom",
]
