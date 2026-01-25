# integrations/axiom_web.py
from __future__ import annotations

"""
integrations/axiom_web.py — Axiom Web Connector (Discovery + Signals) — LOCK-IN
=============================================================================

Role
----
Production-safe Playwright connector used by scanners to *discover candidate mints*
and optionally scrape lightweight account pages (trackers/portfolio/rewards).

This module is intentionally **NOT** a trading/execution engine.
- No wallet-seed automation (never handle seed phrases in code/env).
- No “click-to-trade” UI automation in prod by default.

Design goals
------------
✅ No network at import time.
✅ Render-friendly persistent profile (AXIOM_USER_DATA_DIR).
✅ Proxy-aware (AXIOM_PROXY / PLAYWRIGHT_PROXY), with safe logging/redaction.
✅ Cookie-aware (AXIOM_COOKIE / AXIOM_TOKEN_ACCESS / AXIOM_TOKEN_REFRESH).
✅ Fast mode: block images/fonts/media + common trackers.
✅ Hydration-first parsing (Next.js __NEXT_DATA__), fallback to DOM/HTML scan.
✅ Deterministic outputs: normalized dict rows (mint/source/section/url/ts).
✅ Robust timeouts + retries on navigation.

Env Contract (minimal)
----------------------
AXIOM_BASE=https://axiom.trade
AXIOM_DISCOVER_PATH=/discover
AXIOM_SECTIONS=Trending,Surge,DEX Screener,Pump Live
AXIOM_MAX_PER_SECTION=25
AXIOM_USER_DATA_DIR=/opt/render/.cache/axiom-user
AXIOM_TIMEOUT_MS=90000
AXIOM_NAV_WAIT=networkidle|domcontentloaded
AXIOM_HEADLESS=true|false

AXIOM_PROXY=host:port OR http://user:pass@host:port  (optional)
PLAYWRIGHT_PROXY=...                                 (fallback)

AXIOM_COOKIE="k=v; k2=v2; ..."                       (optional)
AXIOM_TOKEN_ACCESS=...                               (optional)
AXIOM_TOKEN_REFRESH=...                              (optional)

Security note
-------------
Do NOT put wallet seed phrases / private keys into env vars for browser automation.
If you ever add “UI trading” later, isolate it in a separate dev-only module and
require explicit opt-in flags + hardware wallet / manual signature.
"""

import asyncio
import contextlib
import json
import logging
import os
import re
import time
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from playwright.async_api import TimeoutError as PwTimeout, async_playwright

# ──────────────────────────────────────────────────────────────────────────────
# Logging shim (structured logger if available)
# ──────────────────────────────────────────────────────────────────────────────
try:
    from logging_setup import get_logger  # type: ignore

    log = get_logger(__name__)  # type: ignore
except Exception:  # pragma: no cover
    log = logging.getLogger("axiom.web")


def _event(name: str, **kw: Any) -> None:
    try:
        fn = getattr(log, "event", None)
        if callable(fn):
            fn(name, **kw)
        else:
            log.info("%s %s", name, kw)
    except Exception:
        pass


# ──────────────────────────────────────────────────────────────────────────────
# Env helpers
# ──────────────────────────────────────────────────────────────────────────────
_TRUE = {"1", "true", "t", "yes", "y", "on"}
_FALSE = {"0", "false", "f", "no", "n", "off"}


def _env_str(name: str, default: str = "") -> str:
    v = os.getenv(name)
    return default if v is None else str(v).strip()


def _env_int(name: str, default: int) -> int:
    try:
        return int(float(_env_str(name, str(default))))
    except Exception:
        return int(default)


def _env_bool(name: str, default: bool = False) -> bool:
    v = _env_str(name, "")
    if not v:
        return default
    s = v.lower().strip()
    if s in _TRUE:
        return True
    if s in _FALSE:
        return False
    return default


def _now_ts() -> float:
    return float(time.time())


# ──────────────────────────────────────────────────────────────────────────────
# Config
# ──────────────────────────────────────────────────────────────────────────────
AXIOM_BASE = _env_str("AXIOM_BASE", "https://axiom.trade").rstrip("/")
AXIOM_DISCOVER_PATH = _env_str("AXIOM_DISCOVER_PATH", "/discover")
AXIOM_USER_DATA_DIR = _env_str("AXIOM_USER_DATA_DIR", "/opt/render/.cache/axiom-user")
AXIOM_TIMEOUT_MS = max(10_000, _env_int("AXIOM_TIMEOUT_MS", 90_000))
AXIOM_NAV_WAIT = _env_str(
    "AXIOM_NAV_WAIT", "networkidle"
).strip()  # networkidle|domcontentloaded|load
AXIOM_HEADLESS = _env_bool("AXIOM_HEADLESS", True)

AXIOM_COOKIE = _env_str("AXIOM_COOKIE", "")
AXIOM_TOKEN_ACCESS = _env_str("AXIOM_TOKEN_ACCESS", "")
AXIOM_TOKEN_REFRESH = _env_str("AXIOM_TOKEN_REFRESH", "")

AXIOM_SECTIONS = [
    s.strip()
    for s in _env_str("AXIOM_SECTIONS", "Trending,Surge,DEX Screener,Pump Live").split(",")
    if s.strip()
]
AXIOM_MAX_PER_SECTION = max(1, _env_int("AXIOM_MAX_PER_SECTION", 25))

UA = _env_str(
    "AXIOM_UA",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
)

# Retries
AXIOM_GOTO_RETRIES = max(0, _env_int("AXIOM_GOTO_RETRIES", 2))
AXIOM_GOTO_BACKOFF_MS = max(150, _env_int("AXIOM_GOTO_BACKOFF_MS", 500))

# Resource throttle
AXIOM_BLOCK_TRACKERS = _env_bool("AXIOM_BLOCK_TRACKERS", True)
AXIOM_BLOCK_IMAGES = _env_bool("AXIOM_BLOCK_IMAGES", True)
AXIOM_BLOCK_FONTS = _env_bool("AXIOM_BLOCK_FONTS", True)
AXIOM_BLOCK_MEDIA = _env_bool("AXIOM_BLOCK_MEDIA", True)

# Safety caps
AXIOM_MAX_HTML_BYTES = max(200_000, _env_int("AXIOM_MAX_HTML_BYTES", 2_000_000))
AXIOM_MAX_HYDRATION_BYTES = max(200_000, _env_int("AXIOM_MAX_HYDRATION_BYTES", 4_000_000))


# ──────────────────────────────────────────────────────────────────────────────
# Proxy helpers
# ──────────────────────────────────────────────────────────────────────────────
def _resolve_proxy_url() -> str | None:
    """
    Prefer AXIOM_PROXY, else PLAYWRIGHT_PROXY.
    Accept:
      - host:port
      - http://user:pass@host:port
      - socks5://host:port
    """
    raw = (_env_str("AXIOM_PROXY", "") or _env_str("PLAYWRIGHT_PROXY", "")).strip()
    if not raw:
        return None
    if "://" not in raw:
        raw = f"http://{raw}"
    return raw


def _mask_proxy(url: str | None) -> str:
    try:
        if not url:
            return ""
        u = urlparse(url)
        if not u.hostname:
            return ""
        scheme = u.scheme or "http"
        port = f":{u.port}" if u.port else ""
        return f"{scheme}://{u.hostname}{port}"
    except Exception:
        return ""


def _playwright_proxy() -> dict | None:
    raw = _resolve_proxy_url()
    if not raw:
        return None
    u = urlparse(raw)
    if not (u.scheme and u.hostname and u.port):
        return None
    proxy: dict = {"server": f"{u.scheme}://{u.hostname}:{u.port}"}
    if u.username:
        proxy["username"] = u.username
    if u.password:
        proxy["password"] = u.password
    return proxy


# ──────────────────────────────────────────────────────────────────────────────
# Parsing utilities
# ──────────────────────────────────────────────────────────────────────────────
BASE58 = r"[1-9A-HJ-NP-Za-km-z]{32,64}"


def _clean_html(s: str) -> str:
    s = re.sub(r"<[^>]+>", "", s or "")
    return re.sub(r"\s+", " ", s).strip()


def _parse_cookie_string(s: str) -> dict[str, str]:
    jar: dict[str, str] = {}
    for part in (s or "").split(";"):
        part = part.strip()
        if not part or "=" not in part:
            continue
        k, v = part.split("=", 1)
        k = k.strip()
        v = v.strip()
        if k and v:
            jar[k] = v
    return jar


def _dedupe_keep_order(items: Iterable[str]) -> list[str]:
    seen: set = set()
    out: list[str] = []
    for x in items:
        if x and x not in seen:
            seen.add(x)
            out.append(x)
    return out


def _find_mints(text: str) -> list[str]:
    """
    Extract candidate mint addresses from text/html/json.
    """
    if not text:
        return []
    # common URL patterns + json keys
    raw: list[str] = []
    raw += re.findall(rf"/coins?/({BASE58})", text)
    raw += re.findall(rf"/asset/({BASE58})", text)
    raw += re.findall(rf"/token/({BASE58})", text)
    raw += re.findall(rf"[?&]mint=({BASE58})", text)
    raw += re.findall(rf'"mint"\s*:\s*"({BASE58})"', text)
    raw += re.findall(rf'"mintAddress"\s*:\s*"({BASE58})"', text)
    raw += re.findall(rf'"address"\s*:\s*"({BASE58})"', text)
    return _dedupe_keep_order(raw)


def _safe_slice(s: str, max_bytes: int) -> str:
    if not s:
        return ""
    b = s.encode("utf-8", errors="ignore")
    if len(b) <= max_bytes:
        return s
    return b[:max_bytes].decode("utf-8", errors="ignore")


# ──────────────────────────────────────────────────────────────────────────────
# Output schema (simple dict rows)
# ──────────────────────────────────────────────────────────────────────────────
@dataclass(frozen=True)
class AxiomRow:
    mint: str
    section: str
    url: str
    source: str = "axiom"
    ts: float = 0.0

    def asdict(self) -> dict[str, Any]:
        return {
            "mint": self.mint,
            "section": self.section,
            "url": self.url,
            "source": self.source,
            "ts": self.ts or _now_ts(),
        }


# ──────────────────────────────────────────────────────────────────────────────
# Main client
# ──────────────────────────────────────────────────────────────────────────────
class AxiomWeb:
    """
    Async context manager. Use:

        async with AxiomWeb() as ax:
            rows = await ax.discover_mints()

    """

    def __init__(self, *, headless: bool = AXIOM_HEADLESS):
        self.headless = bool(headless)
        self._pw = None
        self.ctx = None
        self.page = None
        self._launch_lock = asyncio.Lock()
        self._closed = False
        self._proxy_logged = False

    async def __aenter__(self) -> AxiomWeb:
        await self.launch()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.close()

    # ------------------------ lifecycle ------------------------

    async def launch(self) -> None:
        if self.page or self.ctx:
            return
        async with self._launch_lock:
            if self.page or self.ctx:
                return

            proxy_cfg = _playwright_proxy()
            if proxy_cfg and not self._proxy_logged:
                _event("AXIOM_PROXY", proxy=_mask_proxy(_resolve_proxy_url()))
                self._proxy_logged = True

            # Ensure persistent dir exists
            with contextlib.suppress(Exception):
                Path(AXIOM_USER_DATA_DIR).mkdir(parents=True, exist_ok=True)

            self._pw = await async_playwright().start()
            self.ctx = await self._pw.chromium.launch_persistent_context(
                user_data_dir=AXIOM_USER_DATA_DIR,
                headless=self.headless,
                proxy=proxy_cfg,
                viewport={"width": 1366, "height": 768},
                locale="en-US",
                user_agent=UA,
                args=[
                    "--no-sandbox",
                    "--disable-dev-shm-usage",
                    "--disable-blink-features=AutomationControlled",
                ],
            )

            await self._install_throttle()
            self.page = await self.ctx.new_page()

            # Minimal stealth (keep it light; do not fight anti-bot aggressively)
            await self.page.add_init_script(
                """
                Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
                Object.defineProperty(navigator, 'languages', { get: () => ['en-US','en'] });
                Object.defineProperty(navigator, 'plugins', { get: () => [1,2,3] });
                window.chrome = window.chrome || { runtime: {} };
                """
            )

            # Seed cookies if provided (optional)
            try:
                applied = await self._seed_cookies()
                _event("AXIOM_COOKIE_SEED", applied=applied)
            except Exception as e:
                _event("AXIOM_COOKIE_SEED_FAIL", err=str(e))

            # Prime session (best-effort)
            try:
                await self.goto("/", wait=AXIOM_NAV_WAIT)
                await self.page.wait_for_timeout(400)
                with _suppress():
                    await self.page.reload(
                        wait_until=AXIOM_NAV_WAIT, timeout=min(15_000, AXIOM_TIMEOUT_MS)
                    )
                await self.page.wait_for_timeout(250)
                _event("AXIOM_PRIMED")
            except Exception as e:
                _event("AXIOM_PRIME_FAIL", err=str(e))

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        with _suppress():
            if self.page:
                await self.page.close()
        with _suppress():
            if self.ctx:
                await self.ctx.close()
        with _suppress():
            if self._pw:
                await self._pw.stop()
        self.page = None
        self.ctx = None
        self._pw = None

    # ------------------------ internals ------------------------

    async def _seed_cookies(self) -> bool:
        """
        Inject cookies from AXIOM_COOKIE or AXIOM_TOKEN_* envs.
        Returns True if any cookies were applied.
        """
        if not self.ctx:
            return False

        applied = False

        if AXIOM_COOKIE:
            jar = _parse_cookie_string(AXIOM_COOKIE)
            cookies = []
            for k, v in jar.items():
                cookies.append(
                    {
                        "name": k,
                        "value": v,
                        "domain": "axiom.trade",
                        "path": "/",
                        "httpOnly": True,
                        "secure": True,
                        "sameSite": "Lax",
                    }
                )
            if cookies:
                await self.ctx.add_cookies(cookies)
                applied = True

        # Token cookies (if your session uses these)
        token_cookies = []
        if AXIOM_TOKEN_ACCESS:
            token_cookies.append(
                {
                    "name": "auth-access-token",
                    "value": AXIOM_TOKEN_ACCESS,
                    "domain": "axiom.trade",
                    "path": "/",
                    "httpOnly": True,
                    "secure": True,
                    "sameSite": "Lax",
                }
            )
        if AXIOM_TOKEN_REFRESH:
            token_cookies.append(
                {
                    "name": "auth-refresh-token",
                    "value": AXIOM_TOKEN_REFRESH,
                    "domain": "axiom.trade",
                    "path": "/",
                    "httpOnly": True,
                    "secure": True,
                    "sameSite": "Lax",
                }
            )
        if token_cookies:
            await self.ctx.add_cookies(token_cookies)
            applied = True

        return applied

    async def _install_throttle(self) -> None:
        """
        Abort heavy / non-essential resources to speed up slow networks.
        Keep: document, script, stylesheet, xhr/fetch.
        Drop: images/fonts/media + common trackers/analytics (optional).
        """
        if not self.ctx:
            return

        tracker_needles = (
            "googletagmanager",
            "google-analytics",
            "g.doubleclick",
            "hotjar",
            "segment.io",
            "fullstory",
            "intercom",
            "clarity.ms",
            "sentry.io",
            "mixpanel",
        )

        async def handler(route):
            req = route.request
            rt = (req.resource_type or "").lower()
            url = (req.url or "").lower()

            if AXIOM_BLOCK_IMAGES and rt == "image":
                return await route.abort()
            if AXIOM_BLOCK_FONTS and rt == "font":
                return await route.abort()
            if AXIOM_BLOCK_MEDIA and rt in {"media"}:
                return await route.abort()

            if AXIOM_BLOCK_TRACKERS and any(bad in url for bad in tracker_needles):
                return await route.abort()

            return await route.continue_()

        await self.ctx.route("**/*", handler)

    async def goto(self, path_or_url: str, *, wait: str = AXIOM_NAV_WAIT) -> None:
        """
        Navigation with retries + backoff.
        """
        if not self.page:
            raise RuntimeError("AxiomWeb not launched")

        url = (
            path_or_url
            if path_or_url.startswith("http")
            else f"{AXIOM_BASE}{path_or_url if path_or_url.startswith('/') else '/' + path_or_url}"
        )

        last_err: BaseException | None = None
        for attempt in range(AXIOM_GOTO_RETRIES + 1):
            try:
                await self.page.set_extra_http_headers(
                    {
                        "User-Agent": UA,
                        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                        "Accept-Language": "en-US,en;q=0.9",
                        "Upgrade-Insecure-Requests": "1",
                    }
                )
                await self.page.goto(url, wait_until=wait, timeout=AXIOM_TIMEOUT_MS)
                return
            except (PwTimeout, Exception) as e:
                last_err = e
                if attempt >= AXIOM_GOTO_RETRIES:
                    break
                await self.page.wait_for_timeout(AXIOM_GOTO_BACKOFF_MS * (attempt + 1))
        raise RuntimeError(f"axiom_goto_failed: {url} ({last_err})")

    async def _scroll(self, *, steps: int = 8, px: int = 1300, delay_ms: int = 140) -> None:
        if not self.page:
            return
        for _ in range(max(1, int(steps))):
            with _suppress():
                await self.page.mouse.wheel(0, int(px))
            await self.page.wait_for_timeout(int(delay_ms))

    async def _pull_hydration_json(self) -> list[dict]:
        """
        Best-effort: read Next.js hydration blob from window.__NEXT_DATA__.
        """
        if not self.page:
            return []
        blobs: list[dict] = []
        try:
            payload = await self.page.evaluate("window.__NEXT_DATA__ || null")
            if payload and isinstance(payload, dict):
                blobs.append(payload)
        except Exception:
            pass
        return blobs

    async def _page_html(self) -> str:
        if not self.page:
            return ""
        try:
            html = await self.page.content()
            return _safe_slice(html, AXIOM_MAX_HTML_BYTES)
        except Exception:
            return ""

    # ------------------------ public: discovery ------------------------

    async def discover_sections(
        self, *, limit_per_section: int = AXIOM_MAX_PER_SECTION
    ) -> dict[str, list[dict[str, Any]]]:
        """
        Return: {section_name: [ {mint, section, url, source, ts}, ... ], ...}
        """
        if limit_per_section <= 0:
            limit_per_section = AXIOM_MAX_PER_SECTION

        await self.goto(AXIOM_DISCOVER_PATH, wait=AXIOM_NAV_WAIT)
        await self.page.wait_for_timeout(450)
        await self._scroll(steps=6)

        sections = {name: [] for name in (AXIOM_SECTIONS or ["Trending"])}

        # 1) Hydration-first
        blobs = await self._pull_hydration_json()
        if blobs:
            try:
                raw = json.dumps(blobs[0], ensure_ascii=False)
                raw = _safe_slice(raw, AXIOM_MAX_HYDRATION_BYTES)
                mints = _find_mints(raw)
                # If hydration contains many mints but no section mapping, put into first section.
                if mints:
                    first = next(iter(sections.keys()))
                    out = []
                    for m in mints[:limit_per_section]:
                        out.append(
                            AxiomRow(
                                mint=m, section=first, url=f"{AXIOM_BASE}/asset/{m}", ts=_now_ts()
                            ).asdict()
                        )
                    sections[first] = out
            except Exception:
                pass

        # 2) DOM/HTML fallback per section (best-effort)
        if all(len(v) == 0 for v in sections.values()):
            html = await self._page_html()

            # If headings are present, try to slice around each heading to reduce noise.
            for sec in sections:
                chunk = _slice_near_heading(html, sec, span=40_000)
                mints = _find_mints(chunk) if chunk else []
                if not mints:
                    # fallback: full-page scan (still capped)
                    mints = _find_mints(html)
                mints = mints[:limit_per_section]
                sections[sec] = [
                    AxiomRow(
                        mint=m, section=sec, url=f"{AXIOM_BASE}/asset/{m}", ts=_now_ts()
                    ).asdict()
                    for m in mints
                ]

        _event("AXIOM_DISCOVER", counts={k: len(v) for k, v in sections.items()})
        return sections

    async def discover_mints(
        self,
        *,
        limit_total: int = 200,
        limit_per_section: int = AXIOM_MAX_PER_SECTION,
        sections: Sequence[str] | None = None,
    ) -> list[dict[str, Any]]:
        """
        Flattened unique rows across sections.
        """
        if sections is not None:
            # temporarily override configured sections for this call
            list(AXIOM_SECTIONS)
            try:
                object.__setattr__(globals(), "AXIOM_SECTIONS", list(sections))  # type: ignore[misc]
            except Exception:
                pass

        sec = await self.discover_sections(limit_per_section=limit_per_section)
        flat: list[dict[str, Any]] = []
        for sname, rows in sec.items():
            for r in rows:
                r["section"] = sname
                flat.append(r)

        # uniq by mint
        seen = set()
        uniq: list[dict[str, Any]] = []
        for r in flat:
            m = r.get("mint")
            if m and m not in seen:
                seen.add(m)
                uniq.append(r)
            if len(uniq) >= int(limit_total):
                break

        return uniq[: int(limit_total)]

    # ------------------------ public: trackers ------------------------

    async def get_trackers(self, *, limit: int = 100) -> list[dict[str, Any]]:
        """
        Scrape trackers page (best-effort). Output fields are not guaranteed stable.
        """
        out: list[dict[str, Any]] = []
        for path in ("/trackers", "/discover?tab=trackers"):
            try:
                await self.goto(path, wait=AXIOM_NAV_WAIT)
                await self.page.wait_for_timeout(600)
                await self._scroll(steps=8)
                html = await self._page_html()

                # heuristic: links that look like tracker pages
                for m in re.finditer(
                    r'<a[^>]+href="(?P<href>/tracker/[^"]+)"[^>]*>(?P<title>.*?)</a>',
                    html,
                    re.S | re.I,
                ):
                    href = m.group("href")
                    title = _clean_html(m.group("title"))
                    if not title:
                        continue
                    out.append({"title": title, "href": AXIOM_BASE + href})

                if out:
                    break
            except Exception as e:
                _event("AXIOM_TRACKERS_FAIL", path=path, err=str(e))
                continue

        return out[: max(0, int(limit))]

    # ------------------------ public: portfolio/rewards ------------------------

    async def get_portfolio(self) -> dict[str, Any]:
        """
        Best-effort portfolio snapshot (requires logged session).
        Returns: {"balances":[{"symbol","amount","usd"}...]}
        """
        balances: list[dict[str, Any]] = []
        try:
            await self.goto("/portfolio", wait=AXIOM_NAV_WAIT)
            await self.page.wait_for_timeout(700)
            await self._scroll(steps=6)
            html = await self._page_html()

            # very heuristic extraction; keep conservative
            for m in re.finditer(
                r">(?P<sym>[A-Z0-9\.\-]{2,12})<.*?(?P<amt>[\d\.,]+).*?\$(?P<usd>[\d\.,]+)",
                html,
                re.I | re.S,
            ):
                sym = m.group("sym")
                amt = _to_float(m.group("amt"))
                usd = _to_float(m.group("usd"))
                if sym and (amt is not None or usd is not None):
                    balances.append({"symbol": sym, "amount": amt, "usd": usd})
        except Exception as e:
            _event("AXIOM_PORTFOLIO_FAIL", err=str(e))

        if not balances:
            # hydration fallback
            try:
                blobs = await self._pull_hydration_json()
                if blobs:
                    text = _safe_slice(
                        json.dumps(blobs[0], ensure_ascii=False), AXIOM_MAX_HYDRATION_BYTES
                    )
                    for row in re.finditer(
                        r'"symbol"\s*:\s*"(?P<sym>[A-Z0-9\.\-]{2,12})".{0,220}?"amount"\s*:\s*"?(?P<amt>[\d\.,]+)".{0,220}?"usd"\s*:\s*"?(?P<usd>[\d\.,]+)"?',
                        text,
                        re.S,
                    ):
                        balances.append(
                            {
                                "symbol": row.group("sym"),
                                "amount": _to_float(row.group("amt")),
                                "usd": _to_float(row.group("usd")),
                            }
                        )
            except Exception:
                pass

        return {"balances": balances}

    async def get_rewards(self) -> dict[str, Any]:
        """
        Best-effort rewards snapshot (requires logged session).
        Returns: {"points": float|None, "rank": int|None}
        """
        points: float | None = None
        rank: int | None = None

        try:
            await self.goto("/rewards", wait=AXIOM_NAV_WAIT)
            await self.page.wait_for_timeout(500)
            html = await self._page_html()

            points = _first_float(re.findall(r"(?:Points|Score|XP)\D+([\d\.,]+)", html))
            rank = _first_int(re.findall(r"(?:Rank|Position)\D+([\d,]+)", html))
        except Exception as e:
            _event("AXIOM_REWARDS_FAIL", err=str(e))

        if points is None or rank is None:
            try:
                blobs = await self._pull_hydration_json()
                if blobs:
                    text = _safe_slice(
                        json.dumps(blobs[0], ensure_ascii=False), AXIOM_MAX_HYDRATION_BYTES
                    )
                    if points is None:
                        m = re.search(r'"points"\s*:\s*"?(?P<val>[\d\.,]+)"?', text)
                        if m:
                            points = _to_float(m.group("val"))
                    if rank is None:
                        m = re.search(r'"rank"\s*:\s*"?(?P<val>[\d,]+)"?', text)
                        if m:
                            rank = _to_int(m.group("val"))
            except Exception:
                pass

        return {"points": points, "rank": rank}


# ──────────────────────────────────────────────────────────────────────────────
# Convenience helpers
# ──────────────────────────────────────────────────────────────────────────────
class _suppress:
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return True


def _to_int(x: Any) -> int | None:
    try:
        return int(str(x).replace(",", "").strip())
    except Exception:
        return None


def _to_float(x: Any) -> float | None:
    try:
        return float(str(x).replace(",", "").strip())
    except Exception:
        return None


def _first_int(arr: Sequence[str]) -> int | None:
    for s in arr:
        v = _to_int(s)
        if v is not None:
            return v
    return None


def _first_float(arr: Sequence[str]) -> float | None:
    for s in arr:
        v = _to_float(s)
        if v is not None:
            return v
    return None


def _slice_near_heading(html: str, heading: str, *, span: int = 40_000) -> str:
    if not html or not heading:
        return ""
    m = re.search(rf">{re.escape(heading)}<", html, re.I)
    if not m:
        return ""
    start = max(0, m.start() - span // 4)
    end = min(len(html), m.end() + span)
    return html[start:end]


async def axiom_discover_once(
    *, limit_total: int = 200, limit_per_section: int = AXIOM_MAX_PER_SECTION
) -> list[dict[str, Any]]:
    async with AxiomWeb(headless=AXIOM_HEADLESS) as ax:
        return await ax.discover_mints(limit_total=limit_total, limit_per_section=limit_per_section)


# ──────────────────────────────────────────────────────────────────────────────
# CLI smoke
# ──────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s:%(name)s: %(message)s"
    )

    async def _t():
        async with AxiomWeb(headless=True) as ax:
            rows = await ax.discover_mints(limit_total=120, limit_per_section=25)
            print("discover_mints:", len(rows), "first:", rows[:3])
            print("trackers:", (await ax.get_trackers(limit=10))[:3])
            print("portfolio:", await ax.get_portfolio())
            print("rewards:", await ax.get_rewards())

    asyncio.run(_t())


__all__ = [
    "AxiomWeb",
    "axiom_discover_once",
]
