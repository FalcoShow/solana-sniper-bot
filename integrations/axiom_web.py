# integrations/axiom_web.py — v1.1
# Axiom web connector: discover, trackers, portfolio, rewards, (optional) UI trading.
# Proxy-aware (AXIOM_PROXY / PLAYWRIGHT_PROXY), cookie-aware (AXIOM_COOKIE or AXIOM_TOKEN_*),
# persistent user profile, optional Phantom bootstrap, and network-throttled scraping.

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

from playwright.async_api import async_playwright, TimeoutError as PwTimeout

LOG = logging.getLogger("axiom.web")

# =============================== Config ===============================

AXIOM_BASE = os.getenv("AXIOM_BASE", "https://axiom.trade").rstrip("/")
AXIOM_DISCOVER_PATH = os.getenv("AXIOM_DISCOVER_PATH", "/discover")
AXIOM_USER_DATA_DIR = os.getenv("AXIOM_USER_DATA_DIR", "/opt/render/.cache/axiom-user")
AXIOM_TIMEOUT_MS = int(os.getenv("AXIOM_TIMEOUT_MS", "90000"))  # default 90s for heavy pages
AXIOM_NAV_WAIT = os.getenv("AXIOM_NAV_WAIT", "networkidle")     # "networkidle" or "domcontentloaded"
AXIOM_HEADLESS = os.getenv("AXIOM_HEADLESS", "true").lower() != "false"

# Cookies (either a full cookie string or the two token cookies)
AXIOM_COOKIE = os.getenv("AXIOM_COOKIE", "").strip()
AXIOM_TOKEN_ACCESS = os.getenv("AXIOM_TOKEN_ACCESS", "").strip()
AXIOM_TOKEN_REFRESH = os.getenv("AXIOM_TOKEN_REFRESH", "").strip()

# Optional wallet automation (only used if configured)
AXIOM_WALLET = os.getenv("AXIOM_WALLET", "").lower()  # "phantom" to enable
PHANTOM_EXT_PATH = os.getenv("PHANTOM_EXT_PATH", "")
PHANTOM_SEED = os.getenv("PHANTOM_SEED", "")
PHANTOM_PASSWORD = os.getenv("PHANTOM_PASSWORD", "ChangeMe123!")

# Discover sections & per-section cap
AXIOM_SECTIONS = [
    s.strip() for s in os.getenv("AXIOM_SECTIONS", "Trending,Surge,DEX Screener,Pump Live").split(",") if s.strip()
]
AXIOM_MAX_PER_SECTION = int(os.getenv("AXIOM_MAX_PER_SECTION", "25"))

UA = os.getenv(
    "AXIOM_UA",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
)

# ============================== Proxy ================================

def _resolve_proxy_url() -> Optional[str]:
    """
    Prefer a dedicated Axiom proxy, otherwise fall back to PLAYWRIGHT_PROXY.
    Do not inherit HTTP(S)_PROXY to avoid impacting aiohttp/requests.
    """
    raw = os.getenv("AXIOM_PROXY") or os.getenv("PLAYWRIGHT_PROXY") or ""
    raw = raw.strip()
    if not raw:
        return None
    # allow bare "host:port"
    if "://" not in raw:
        raw = f"http://{raw}"
    return raw

def _playwright_proxy() -> Optional[dict]:
    raw = _resolve_proxy_url()
    if not raw:
        return None
    u = urlparse(raw)
    if not (u.scheme and u.hostname and u.port):
        return None
    proxy = {"server": f"{u.scheme}://{u.hostname}:{u.port}"}
    if u.username:
        proxy["username"] = u.username
    if u.password:
        proxy["password"] = u.password
    return proxy

def _mask_proxy_for_log(url: Optional[str]) -> str:
    try:
        if not url:
            return ""
        if "://" not in url:
            url = f"http://{url}"
        u = urlparse(url)
        return f"{u.scheme}://{u.hostname}:{u.port}".rstrip(":")
    except Exception:
        return url or ""

# ============================= Headers/JS ============================

EXTRA_HTML = {
    "User-Agent": UA,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Upgrade-Insecure-Requests": "1",
}
EXTRA_JSON = {
    "User-Agent": UA,
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
}

STEALTH_JS = r"""
Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
Object.defineProperty(navigator, 'languages', { get: () => ['en-US','en'] });
Object.defineProperty(navigator, 'plugins', { get: () => [1,2,3,4,5] });
window.chrome = window.chrome || { runtime: {} };
"""

BASE58 = r"[1-9A-HJ-NP-Za-km-z]{32,64}"

# ============================== Utilities =============================

def _parse_cookie_string(s: str) -> Dict[str, str]:
    jar: Dict[str, str] = {}
    for part in (s or "").split(";"):
        part = part.strip()
        if not part or "=" not in part:
            continue
        k, v = part.split("=", 1)
        jar[k.strip()] = v.strip()
    return jar

def _clean_html(s: str) -> str:
    s = re.sub(r"<[^>]+>", "", s or "")
    return re.sub(r"\s+", " ", s).strip()

def _to_int(s: Any) -> Optional[int]:
    try:
        return int(str(s).replace(",", "").strip())
    except Exception:
        return None

def _to_float(s: Any) -> Optional[float]:
    try:
        return float(str(s).replace(",", "").strip())
    except Exception:
        return None

def _first_int(arr: List[str]) -> Optional[int]:
    for x in arr:
        v = _to_int(x)
        if v is not None:
            return v
    return None

def _first_float(arr: List[str]) -> Optional[float]:
    for x in arr:
        v = _to_float(x)
        if v is not None:
            return v
    return None

async def _seed_axiom_cookies(ctx) -> bool:
    """Inject cookies from AXIOM_COOKIE or AXIOM_TOKEN_* envs. Returns True if any applied."""
    # 1) Whole cookie string
    if AXIOM_COOKIE:
        jar = _parse_cookie_string(AXIOM_COOKIE)
        cookies = [{
            "name": k, "value": v, "domain": "axiom.trade", "path": "/",
            "httpOnly": True, "secure": True, "sameSite": "Lax",
        } for k, v in jar.items() if k and v]
        if cookies:
            await ctx.add_cookies(cookies)
            LOG.info("Axiom: session cookies injected (%d)", len(cookies))
            return True

    # 2) Separate tokens
    cookies = []
    if AXIOM_TOKEN_ACCESS:
        cookies.append({
            "name": "auth-access-token", "value": AXIOM_TOKEN_ACCESS,
            "domain": "axiom.trade", "path": "/", "httpOnly": True, "secure": True, "sameSite": "Lax",
        })
    if AXIOM_TOKEN_REFRESH:
        cookies.append({
            "name": "auth-refresh-token", "value": AXIOM_TOKEN_REFRESH,
            "domain": "axiom.trade", "path": "/", "httpOnly": True, "secure": True, "sameSite": "Lax",
        })
    if cookies:
        await ctx.add_cookies(cookies)
        LOG.info("Axiom: token cookies injected (%d)", len(cookies))
        return True

    LOG.info("Axiom: no AXIOM_COOKIE / AXIOM_TOKEN_* in env — relying on persisted profile.")
    return False

# ============================== Client ================================

class AxiomWeb:
    def __init__(self, headless: bool = AXIOM_HEADLESS):
        self.headless = headless
        self._pw = None
        self.ctx = None
        self.page = None
        self._proxy_logged = False

    async def __aenter__(self):
        await self.launch()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.close()

    # ---------- launch / teardown ----------

    async def _install_throttle(self):
        """
        Abort heavy / non-essential resources to speed up slow networks.
        Keep XHR/fetch/doc/script/css; drop images/fonts/media/trackers/analytics.
        """
        async def handler(route):
            req = route.request
            rt = req.resource_type
            url = req.url.lower()
            if rt in {"image", "media", "font"} or any(
                bad in url for bad in (
                    "googletagmanager", "google-analytics", "g.doubleclick",
                    "hotjar", "segment.io", "fullstory", "intercom", "klarna",
                )
            ):
                await route.abort()
            else:
                await route.continue_()
        await self.ctx.route("**/*", handler)

    async def launch(self):
        if self.ctx:
            return

        proxy_cfg = _playwright_proxy()
        if proxy_cfg and not self._proxy_logged:
            LOG.info("Axiom Playwright proxy: %s", _mask_proxy_for_log(_resolve_proxy_url()))
            self._proxy_logged = True

        # Wallet extension needs headed Chromium
        ext_args: List[str] = []
        headless = self.headless
        if AXIOM_WALLET == "phantom" and PHANTOM_EXT_PATH and Path(PHANTOM_EXT_PATH).exists():
            ext_args = [
                f"--disable-extensions-except={PHANTOM_EXT_PATH}",
                f"--load-extension={PHANTOM_EXT_PATH}",
            ]
            headless = False
            LOG.info("Axiom: Phantom extension enabled (%s)", PHANTOM_EXT_PATH)

        self._pw = await async_playwright().start()
        self.ctx = await self._pw.chromium.launch_persistent_context(
            user_data_dir=AXIOM_USER_DATA_DIR,
            headless=headless,
            proxy=proxy_cfg,
            viewport={"width": 1366, "height": 768},
            locale="en-US",
            user_agent=UA,
            args=[
                "--no-sandbox",
                "--disable-dev-shm-usage",
                "--disable-blink-features=AutomationControlled",
                *ext_args,
            ],
        )
        await self._install_throttle()

        self.page = await self.ctx.new_page()
        await self.page.add_init_script(STEALTH_JS)

        # Seed cookies (idempotent)
        try:
            await _seed_axiom_cookies(self.ctx)
        except Exception as e:
            LOG.warning("Axiom: cookie seed failed: %s", e)

        # Prime the session: frontdoor + reload to refresh access using refresh cookie
        try:
            await self.goto("/", wait=AXIOM_NAV_WAIT)
            await self.page.wait_for_timeout(600)
            try:
                await self.page.reload(wait_until=AXIOM_NAV_WAIT, timeout=min(15000, AXIOM_TIMEOUT_MS))
            except Exception:
                pass
            await self.page.wait_for_timeout(400)
            LOG.info("Axiom: session primed")
        except Exception as e:
            LOG.warning("Axiom: prime failed: %s", e)

        # Optional Phantom bootstrap
        if AXIOM_WALLET == "phantom" and PHANTOM_EXT_PATH and Path(PHANTOM_EXT_PATH).exists() and PHANTOM_SEED:
            try:
                await self._phantom_bootstrap()
            except Exception as e:
                LOG.warning("Axiom: Phantom bootstrap failed: %s", e)

    async def close(self):
        try:
            if self.page:
                await self.page.close()
        except Exception:
            pass
        try:
            if self.ctx:
                await self.ctx.close()
        except Exception:
            pass
        try:
            if self._pw:
                await self._pw.stop()
        except Exception:
            pass
        self.page = self.ctx = self._pw = None

    # ---------- navigation & helpers ----------

    async def goto(self, path_or_url: str, wait: str = AXIOM_NAV_WAIT):
        url = path_or_url if path_or_url.startswith("http") else f"{AXIOM_BASE}{path_or_url if path_or_url.startswith('/') else '/' + path_or_url}"
        await self.page.set_extra_http_headers(EXTRA_HTML)
        return await self.page.goto(url, wait_until=wait, timeout=AXIOM_TIMEOUT_MS)

    async def _scroll(self, steps: int = 12, px: int = 1400, delay_ms: int = 180):
        for _ in range(max(1, steps)):
            await self.page.mouse.wheel(0, px)
            await self.page.wait_for_timeout(delay_ms)

    async def _pull_embedded_json(self) -> List[dict]:
        """Collect hydration JSON blobs on the current page (best-effort)."""
        blobs: List[dict] = []
        try:
            payload = await self.page.evaluate("window.__NEXT_DATA__ || null")
            if payload and isinstance(payload, dict):
                blobs.append(payload)
        except Exception:
            pass
        try:
            html = await self.page.content()
            for m in re.finditer(r'<script[^>]+type="application/json"[^>]*>(\{.*?\})</script>', html, re.S | re.I):
                try:
                    blobs.append(json.loads(m.group(1)))
                except Exception:
                    continue
        except Exception:
            pass
        return blobs

    def _slice_near_heading(self, html: str, heading: str, span: int = 20000) -> str:
        m = re.search(rf">{re.escape(heading)}<", html, re.I)
        if not m:
            return ""
        start = max(0, m.start() - span // 4)
        end = min(len(html), m.end() + span)
        return html[start:end]

    def _find_mints(self, text: str) -> List[str]:
        raw: List[str] = []
        raw += re.findall(rf"/coins?/({BASE58})", text)
        raw += re.findall(rf"/asset/({BASE58})", text)
        raw += re.findall(rf"[?&]mint=({BASE58})", text)
        raw += re.findall(rf'"mint"\s*:\s*"({BASE58})"', text)
        raw += re.findall(rf'"mintAddress"\s*:\s*"({BASE58})"', text)
        # normalize / uniq
        seen, out = set(), []
        for x in raw:
            if x and x not in seen:
                seen.add(x)
                out.append(x)
        return out

    # ======================== Public: Discover ========================

    async def discover_sections(self, limit_per_section: int = AXIOM_MAX_PER_SECTION) -> Dict[str, List[Dict[str, Any]]]:
        """
        Scrape Discover buckets:
          - Trending
          - Surge
          - DEX Screener
          - Pump Live
        Returns: { section_name: [ {mint, source:'axiom'}, ... ] }
        """
        try:
            await self.goto(AXIOM_DISCOVER_PATH, wait=AXIOM_NAV_WAIT)
        except PwTimeout:
            LOG.warning("Axiom discover: initial nav timeout (%sms)", AXIOM_TIMEOUT_MS)
        except Exception as e:
            LOG.warning("Axiom discover nav error: %s", e)
        await self.page.wait_for_timeout(1200)

        # Try two scroll passes (content is lazy-loaded)
        await self._scroll(steps=12)
        await self.page.wait_for_timeout(600)
        await self._scroll(steps=8)

        sections: Dict[str, List[Dict[str, Any]]] = {name: [] for name in AXIOM_SECTIONS}
        html = await self.page.content()

        for title in sections.keys():
            try:
                # Prefer ARIA heading; fallback to text
                heading = self.page.get_by_role("heading", name=title).first
                if await heading.count() == 0:
                    heading = self.page.get_by_text(title, exact=False).first

                if await heading.count() > 0:
                    container = heading.locator("xpath=ancestor::*[self::section or self::div][1]")
                    nodes = await container.locator(
                        "a[href*='/asset/'], a[href*='/token/'], a[href*='/coins/'], [data-mint], [data-testid*='card']"
                    ).all()
                    found: List[str] = []
                    for el in nodes:
                        try:
                            href = await el.get_attribute("href")
                            dm = await el.get_attribute("data-mint")
                            text = (await el.inner_text()).strip()
                            blob = " ".join([href or "", dm or "", text or ""])
                            m = re.search(BASE58, blob)
                            if m:
                                found.append(m.group(0))
                        except Exception:
                            continue
                    # uniq + cap
                    seen, uniq = set(), []
                    for m in found:
                        if m and m not in seen:
                            seen.add(m)
                            uniq.append(m)
                    sections[title] = [{"mint": m, "source": "axiom"} for m in uniq[:limit_per_section]]
                    continue

                # If no visible heading, slice raw HTML near the title
                chunk = self._slice_near_heading(html, title)
                mints = self._find_mints(chunk)
                sections[title] = [{"mint": m, "source": "axiom"} for m in mints[:limit_per_section]]

            except Exception:
                # Last resort: HTML slice
                chunk = self._slice_near_heading(html, title)
                mints = self._find_mints(chunk)
                sections[title] = [{"mint": m, "source": "axiom"} for m in mints[:limit_per_section]]

        # If absolutely empty, try hydration blobs
        if all(len(v) == 0 for v in sections.values()):
            for blob in await self._pull_embedded_json():
                text = json.dumps(blob, ensure_ascii=False)
                mints = self._find_mints(text)
                if mints:
                    uniq, seen = [], set()
                    for m in mints:
                        if m not in seen:
                            seen.add(m); uniq.append(m)
                    sections[AXIOM_SECTIONS[0]] = [{"mint": m, "source": "axiom"} for m in uniq[:limit_per_section]]
                    break

        LOG.info("Axiom discover sections: %s", {k: len(v) for k, v in sections.items()})
        return sections

    # ======================== Public: Trackers ========================

    async def get_trackers(self, limit: int = 100) -> List[Dict[str, Any]]:
        """
        Scrape "Trackers" page (names, counts, link/mints if present).
        """
        out: List[Dict[str, Any]] = []
        for path in ("/trackers", "/discover?tab=trackers"):
            try:
                await self.goto(path, wait=AXIOM_NAV_WAIT)
                await self.page.wait_for_timeout(1200)
                await self._scroll(steps=10)
                html = await self.page.content()
                # naive extraction: title + follower counts + href
                for m in re.finditer(
                    r'<a[^>]+href="(/tracker/[^"]+)"[^>]*>(.*?)</a>(?:.|\n){0,200}?(\d[\d,]*)\s*(?:Followers|Subs|Following)',
                    html, re.S | re.I
                ):
                    href, title, count = m.groups()
                    out.append({"title": _clean_html(title), "followers": _to_int(count), "href": AXIOM_BASE + href})
                if out:
                    break
            except Exception:
                continue
        LOG.info("Axiom trackers: %d", len(out[:limit]))
        return out[:limit]

    # ======================== Public: Portfolio =======================

    async def get_portfolio(self) -> Dict[str, Any]:
        """
        Snapshot of balances/positions from portfolio page if logged in.
        """
        try:
            await self.goto("/portfolio", wait=AXIOM_NAV_WAIT)
            await self.page.wait_for_timeout(1200)
            await self._scroll(steps=8)
            html = await self.page.content()
        except Exception as e:
            LOG.warning("Axiom portfolio nav failed: %s", e)
            html = ""

        balances: List[Dict[str, Any]] = []
        # Try to parse token rows: symbol + balance + value
        for m in re.finditer(
            r'>([A-Z0-9\.\-]{2,12})<(?:.|\n){0,200}?([\d\.,]+)\s*(?:\$|USD)?(?:.|\n){0,120}?\$([\d\.,]+)',
            html, re.I
        ):
            sym, amt, usd = m.groups()
            balances.append({"symbol": sym, "amount": _to_float(amt), "usd": _to_float(usd)})

        # Fallback to hydration JSON
        if not balances:
            for blob in await self._pull_embedded_json():
                text = json.dumps(blob, ensure_ascii=False)
                for row in re.finditer(r'"symbol"\s*:\s*"([A-Z0-9\.\-]{2,12})".{0,200}?"amount"\s*:\s*"?(?P<amt>[\d\.,]+)".{0,200}?"usd"\s*:\s*"?(?P<usd>[\d\.,]+)"?', text, re.S):
                    sym = row.group(1)
                    balances.append({"symbol": sym, "amount": _to_float(row.group("amt")), "usd": _to_float(row.group("usd"))})
                if balances:
                    break

        LOG.info("Axiom portfolio balances: %d", len(balances))
        return {"balances": balances}

    # ======================== Public: Rewards =========================

    async def get_rewards(self) -> Dict[str, Any]:
        """
        Pull rewards/points summary (best-effort).
        """
        try:
            await self.goto("/rewards", wait=AXIOM_NAV_WAIT)
            await self.page.wait_for_timeout(800)
            html = await self.page.content()
        except Exception as e:
            LOG.warning("Axiom rewards nav failed: %s", e)
            html = ""

        points = _first_float(re.findall(r'(?:Points|Score|XP)\D+([\d\.,]+)', html))
        rank = _first_int(re.findall(r'(?:Rank|Position)\D+([\d,]+)', html))

        # Fallback hydration JSON
        if points is None or rank is None:
            for blob in await self._pull_embedded_json():
                text = json.dumps(blob, ensure_ascii=False)
                if points is None:
                    m = re.search(r'"points"\s*:\s*"?(?P<val>[\d\.,]+)"?', text)
                    if m: points = _to_float(m.group("val"))
                if rank is None:
                    m = re.search(r'"rank"\s*:\s*"?(?P<val>[\d,]+)"?', text)
                    if m: rank = _to_int(m.group("val"))
                if points is not None and rank is not None:
                    break

        return {"points": points, "rank": rank}

    # ======================== Trading (UI) ============================

    async def place_market_order(self, mint: str, side: str, size: float) -> bool:
        """
        Drives the UI to attempt a market order. Requires a connected wallet.
        Experimental — keep disabled in prod unless Phantom bootstrap is working.
        """
        side = side.lower().strip()
        assert side in ("buy", "sell")
        try:
            await self.goto(AXIOM_DISCOVER_PATH, wait=AXIOM_NAV_WAIT)
            await self.page.wait_for_timeout(600)

            # Try common search boxes
            for ph in ("Search", "Search by token", "Search token"):
                try:
                    box = self.page.get_by_placeholder(re.compile(ph, re.I))
                    await box.click(timeout=1500)
                    await box.fill(mint)
                    break
                except Exception:
                    continue
            await self.page.wait_for_timeout(600)

            # Click first row that mentions the mint
            await self.page.get_by_text(re.compile(mint[:8], re.I)).first.click(timeout=AXIOM_TIMEOUT_MS)

            # Open trade/swap
            for name in ("Trade", "Instant Trade", "Swap"):
                try:
                    await self.page.get_by_role("button", name=re.compile(name, re.I)).click(timeout=1500)
                    break
                except Exception:
                    continue
            await self.page.wait_for_timeout(400)

            # Side
            try:
                await self.page.get_by_role("button", name=re.compile(side, re.I)).click(timeout=800)
            except Exception:
                pass

            # Amount input
            for ph in ("Amount", "You pay", "You receive", "Size"):
                try:
                    inp = self.page.get_by_placeholder(re.compile(ph, re.I))
                    await inp.fill(str(size))
                    break
                except Exception:
                    continue

            # Submit
            for lab in ("Buy", "Sell", "Swap", "Confirm", "Place Order"):
                try:
                    await self.page.get_by_role("button", name=re.compile(lab, re.I)).click(timeout=1500)
                    break
                except Exception:
                    continue

            # Heuristic success
            await self.page.wait_for_timeout(1500)
            for ok in ("Submitted", "Success", "Order placed", "Swapped"):
                if await self.page.get_by_text(re.compile(ok, re.I)).count() > 0:
                    LOG.info("Axiom %s %s OK", side, mint)
                    return True

            LOG.warning("Axiom trade likely waiting on wallet signature. Enable Phantom to automate.")
            return False

        except Exception as e:
            LOG.warning("Axiom market %s %s failed: %s", side, mint, e)
            return False

    # ===================== Phantom bootstrap (opt) ====================

    async def _phantom_bootstrap(self):
        """
        Minimal Phantom onboarding: opens the extension UI, imports seed, sets password.
        Runs once per user-data-dir. Highly version-dependent; best-effort.
        """
        if not PHANTOM_SEED:
            return
        # Try to find any extension page that looks like Phantom’s UI:
        pages = self.ctx.background_pages + self.ctx.pages
        ext = None
        for p in pages:
            if p.url.startswith("chrome-extension://") and any(x in p.url for x in ("onboarding", "welcome", "popup", "home")):
                ext = p
                break
        if not ext:
            LOG.info("Phantom: extension page not visible; skipping bootstrap.")
            return

        try:
            await ext.wait_for_timeout(1200)
            # Import flow (labels vary by version)
            for lab in ("Import", "I already have a wallet", "Use secret recovery phrase"):
                try:
                    await ext.get_by_role("button", name=re.compile(lab, re.I)).click(timeout=1000)
                    break
                except Exception:
                    continue
            # Seed phrase (single textarea path)
            try:
                await ext.get_by_role("textbox").fill(PHANTOM_SEED, timeout=1000)
            except Exception:
                pass
            # Password fields
            for ph in ("Password", "New password"):
                try:
                    await ext.get_by_placeholder(re.compile(ph, re.I)).fill(PHANTOM_PASSWORD, timeout=1000)
                except Exception:
                    continue
            for ph in ("Confirm", "Confirm password"):
                try:
                    await ext.get_by_placeholder(re.compile(ph, re.I)).fill(PHANTOM_PASSWORD, timeout=1000)
                except Exception:
                    continue
            # Continue through
            for lab in ("Continue", "Next", "Finish", "Done"):
                try:
                    await ext.get_by_role("button", name=re.compile(lab, re.I)).click(timeout=1200)
                    await ext.wait_for_timeout(400)
                except Exception:
                    continue
            LOG.info("Phantom: bootstrap flow attempted (check persisted profile).")
        except Exception as e:
            LOG.warning("Phantom bootstrap flow error: %s", e)

# ------------------------- Convenience runner -------------------------

async def axiom_discover_once(limit: int = 200) -> List[Dict[str, Any]]:
    async with AxiomWeb(headless=AXIOM_HEADLESS) as ax:
        sections = await ax.discover_sections(limit_per_section=limit)
        # flatten and uniq by mint
        out: List[Dict[str, Any]] = []
        for lst in sections.values():
            out.extend(lst)
        seen, uniq = set(), []
        for d in out:
            m = d.get("mint")
            if m and m not in seen:
                seen.add(m)
                uniq.append(d)
        return uniq

# ------------------------------ CLI test ------------------------------

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s:%(name)s: %(message)s")
    async def _t():
        async with AxiomWeb(headless=True) as ax:
            print("Sections:", await ax.discover_sections(25))
            print("Trackers:", await ax.get_trackers(25))
            print("Portfolio:", await ax.get_portfolio())
            print("Rewards:", await ax.get_rewards())
    asyncio.run(_t())
