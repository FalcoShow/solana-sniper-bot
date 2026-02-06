# -*- coding: utf-8 -*-
from __future__ import annotations

"""
integrations/axiom_web.py — Axiom Web Connector (Discovery + Signals) — LOCK-IN
=============================================================================

Role
----
Production-safe Playwright connector used by scanners to discover candidate mints
and optionally scrape lightweight pages (trackers/portfolio/rewards).

This module is intentionally NOT a trading/execution engine.
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
AXIOM_NAV_WAIT=networkidle|domcontentloaded|load
AXIOM_HEADLESS=true|false

AXIOM_PROXY=host:port OR http://user:pass@host:port (optional)
PLAYWRIGHT_PROXY=...                                (fallback)

AXIOM_COOKIE="k=v; k2=v2; ..."                      (optional)
AXIOM_TOKEN_ACCESS=...                              (optional)
AXIOM_TOKEN_REFRESH=...                             (optional)

Security note
-------------
Do NOT put wallet seed phrases / private keys into env vars for browser automation.
If you ever add “UI trading” later, isolate it in a separate dev-only module and
require explicit opt-in flags + hardware wallet / manual signature.
"""

import asyncio
import contextlib
import hashlib
import json
import logging
import os
import re
import time
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol, cast
from urllib.parse import urlparse

# ──────────────────────────────────────────────────────────────────────────────
# Optional Playwright import (keeps imports safe in minimal CI / non-Playwright envs)
# ──────────────────────────────────────────────────────────────────────────────
try:  # pragma: no cover
    from playwright.async_api import (
        TimeoutError as PwTimeout,  # type: ignore
        async_playwright,  # type: ignore
    )
except Exception:  # pragma: no cover
    PwTimeout = TimeoutError  # type: ignore[assignment]
    async_playwright = None  # type: ignore[assignment]

# ──────────────────────────────────────────────────────────────────────────────
# Optional mint normalizer (best-effort, no hard dependency)
# ──────────────────────────────────────────────────────────────────────────────
try:  # pragma: no cover
    from utils_pkg.mints import normalize_mint as _normalize_mint  # type: ignore
except Exception:  # pragma: no cover
    _normalize_mint = None  # type: ignore[assignment]

# =============================================================================
# Phase A Logging contract: EventLogger Protocol + safe get_logger
# =============================================================================


class EventLogger(Protocol):
    def debug(self, msg: str, *args: Any, **kwargs: Any) -> None: ...
    def info(self, msg: str, *args: Any, **kwargs: Any) -> None: ...
    def warning(self, msg: str, *args: Any, **kwargs: Any) -> None: ...
    def error(self, msg: str, *args: Any, **kwargs: Any) -> None: ...
    def exception(self, msg: str, *args: Any, **kwargs: Any) -> None: ...
    def log(self, level: int, msg: str, *args: Any, **kwargs: Any) -> None: ...


class _EventCapable(EventLogger, Protocol):
    def event(self, name: str, **fields: Any) -> None: ...


def get_logger(name: str | None = None) -> EventLogger:
    """
    Single signature. Always returns an EventLogger-compatible object.

    Priority:
      1) utils_pkg.log_compat.get_event_logger / get_logger
      2) logging_setup.get_logger
      3) stdlib logging.getLogger (basicConfig if needed)
    """
    nm = (name or "integrations.axiom_web").strip() or "integrations.axiom_web"

    with contextlib.suppress(Exception):
        from utils_pkg.log_compat import get_event_logger  # type: ignore

        return cast(EventLogger, get_event_logger(nm))

    with contextlib.suppress(Exception):
        from utils_pkg.log_compat import get_logger as gl  # type: ignore

        return cast(EventLogger, gl(nm))

    with contextlib.suppress(Exception):
        from logging_setup import get_logger as gl2  # type: ignore

        return cast(EventLogger, gl2(nm))

    lg = logging.getLogger(nm)
    # Only configure stdlib logging when using stdlib logger.
    if isinstance(lg, logging.Logger) and not lg.handlers:
        logging.basicConfig(level=logging.INFO)
    return cast(EventLogger, lg)


log: EventLogger = get_logger(__name__)


def _event(name: str, **kw: Any) -> None:
    """Best-effort structured log event; never raises."""
    try:
        fn = getattr(log, "event", None)
        if callable(fn):
            cast(_EventCapable, log).event(str(name), **kw)
        else:
            log.info("%s %s", str(name), kw)
    except Exception:
        return


# ──────────────────────────────────────────────────────────────────────────────
# Env helpers (NO network)
# ──────────────────────────────────────────────────────────────────────────────
_TRUE = {"1", "true", "t", "yes", "y", "on"}
_FALSE = {"0", "false", "f", "no", "n", "off"}


def _env_str(name: str, default: str = "") -> str:
    v = os.getenv(name)
    return default if v is None else str(v).strip()


def _env_int(name: str, default: int) -> int:
    raw = _env_str(name, str(default))
    try:
        return int(float(raw))
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


def _redact(s: str) -> str:
    """
    Redact obvious sensitive strings from error messages/logs:
    - token values
    - cookie header contents
    - proxy creds in URLs
    """
    if not s:
        return s
    out = str(s)

    # redact proxy basic-auth: scheme://user:pass@host:port -> scheme://***:***@host:port
    out = re.sub(r"(https?|socks5)://([^:@/]+):([^@/]+)@", r"\1://***:***@", out, flags=re.I)

    # redact env tokens/cookies if present in exception messages
    for secret in (AXIOM_TOKEN_ACCESS, AXIOM_TOKEN_REFRESH, AXIOM_COOKIE):
        if secret and secret in out:
            out = out.replace(secret, "***REDACTED***")

    return out


# ──────────────────────────────────────────────────────────────────────────────
# Config (read-only at import time; NO network)
# ──────────────────────────────────────────────────────────────────────────────
AXIOM_BASE = _env_str("AXIOM_BASE", "https://axiom.trade").rstrip("/")
AXIOM_DISCOVER_PATH = _env_str("AXIOM_DISCOVER_PATH", "/discover")
AXIOM_USER_DATA_DIR = _env_str("AXIOM_USER_DATA_DIR", "/opt/render/.cache/axiom-user")

AXIOM_TIMEOUT_MS = max(10_000, _env_int("AXIOM_TIMEOUT_MS", 90_000))
AXIOM_NAV_WAIT = _env_str("AXIOM_NAV_WAIT", "networkidle").strip() or "networkidle"
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

_AX_DOMAIN = urlparse(AXIOM_BASE).hostname or "axiom.trade"


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
    """Redact user/pass, keep scheme://host:port only."""
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


def _playwright_proxy() -> dict[str, str] | None:
    raw = _resolve_proxy_url()
    if not raw:
        return None
    u = urlparse(raw)
    if not (u.scheme and u.hostname and u.port):
        return None
    proxy: dict[str, str] = {"server": f"{u.scheme}://{u.hostname}:{u.port}"}
    if u.username:
        proxy["username"] = u.username
    if u.password:
        proxy["password"] = u.password
    return proxy


# ──────────────────────────────────────────────────────────────────────────────
# Parsing utilities
# ──────────────────────────────────────────────────────────────────────────────
BASE58_RE = re.compile(r"[1-9A-HJ-NP-Za-km-z]{32,64}")


def _clean_html(s: str) -> str:
    s2 = re.sub(r"<[^>]+>", "", s or "")
    return re.sub(r"\s+", " ", s2).strip()


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
    seen: set[str] = set()
    out: list[str] = []
    for x in items:
        if x and x not in seen:
            seen.add(x)
            out.append(x)
    return out


def _normalize_candidate_mint(m: str) -> str:
    mm = (m or "").strip()
    if not mm:
        return ""
    if _normalize_mint is not None:
        with contextlib.suppress(Exception):
            out = _normalize_mint(mm)
            if isinstance(out, str) and out:
                mm = out
    mo = BASE58_RE.search(mm)
    return mo.group(0) if mo else ""


def _find_mints(text: str) -> list[str]:
    """
    Extract candidate mint addresses from text/html/json.
    We intentionally search multiple shapes because Axiom pages can change.
    """
    if not text:
        return []

    raw: list[str] = []

    # common URL shapes
    raw += re.findall(r"/asset/(" + BASE58_RE.pattern + r")", text)
    raw += re.findall(r"/token/(" + BASE58_RE.pattern + r")", text)
    raw += re.findall(r"/coins?/(" + BASE58_RE.pattern + r")", text)
    raw += re.findall(r"[?&]mint=(" + BASE58_RE.pattern + r")", text)

    # json-ish keys
    raw += re.findall(r'"mint"\s*:\s*"(' + BASE58_RE.pattern + r')"', text)
    raw += re.findall(r'"mintAddress"\s*:\s*"(' + BASE58_RE.pattern + r')"', text)
    raw += re.findall(r'"token_mint"\s*:\s*"(' + BASE58_RE.pattern + r')"', text)
    raw += re.findall(r'"address"\s*:\s*"(' + BASE58_RE.pattern + r')"', text)

    deduped = _dedupe_keep_order(raw)
    return [nm for nm in (_normalize_candidate_mint(x) for x in deduped) if nm]


def _safe_slice(s: str, max_bytes: int) -> str:
    if not s:
        return ""
    b = s.encode("utf-8", errors="ignore")
    if len(b) <= max_bytes:
        return s
    return b[:max_bytes].decode("utf-8", errors="ignore")


def _slice_near_heading(html: str, heading: str, *, span: int = 40_000) -> str:
    if not html or not heading:
        return ""
    m = re.search(rf">{re.escape(heading)}<", html, re.I)
    if not m:
        return ""
    start = max(0, m.start() - span // 4)
    end = min(len(html), m.end() + span)
    return html[start:end]


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


def _ensure_playwright_available() -> None:
    if async_playwright is None:
        raise RuntimeError(
            "Playwright is not available. Install it in your environment "
            "(pip install playwright && python -m playwright install chromium) "
            "or call this integration with offline/no_network flags."
        )


# ──────────────────────────────────────────────────────────────────────────────
# Offline deterministic sample rows (no deps)
# ──────────────────────────────────────────────────────────────────────────────
_B58_ALPH = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"


def _b58encode(data: bytes) -> str:
    if not data:
        return ""
    n = int.from_bytes(data, "big")
    out: list[str] = []
    while n > 0:
        n, r = divmod(n, 58)
        out.append(_B58_ALPH[r])
    pad = 0
    for b in data:
        if b == 0:
            pad += 1
        else:
            break
    return ("1" * pad) + "".join(reversed(out or ["1"]))


def _stable_pubkey(tag: str) -> str:
    h = hashlib.sha256(tag.encode("utf-8")).digest()
    return _b58encode(h)


def _offline_rows(
    *,
    sections: Sequence[str],
    limit_per_section: int,
    limit_total: int,
) -> list[dict[str, Any]]:
    now = _now_ts()
    out: list[dict[str, Any]] = []
    seen: set[str] = set()

    for sec in sections:
        for i in range(max(1, limit_per_section)):
            mint = _stable_pubkey(f"axiom-offline-{sec}-{i}")
            if mint in seen:
                continue
            seen.add(mint)
            out.append(
                {
                    "mint": mint,
                    "section": sec,
                    "url": f"{AXIOM_BASE}/asset/{mint}",
                    "source": "axiom",
                    "ts": now,
                }
            )
            if len(out) >= limit_total:
                return out
    return out


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

    This class does NOT do any network or browser launch until launch()/__aenter__.
    """

    def __init__(
        self,
        *,
        headless: bool = AXIOM_HEADLESS,
        offline: bool = False,
        no_network: bool = False,
        dry_run: bool = False,
        sample: bool = False,
    ):
        self.headless = bool(headless)
        self.offline_mode = bool(offline or no_network or dry_run or sample)

        self._pw: Any | None = None
        self.ctx: Any | None = None
        self.page: Any | None = None
        self._launch_lock = asyncio.Lock()
        self._closed = False
        self._proxy_logged = False

    async def __aenter__(self) -> "AxiomWeb":
        await self.launch()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.close()

    # ------------------------ lifecycle ------------------------

    async def launch(self) -> None:
        if self.page or self.ctx or self.offline_mode:
            return

        async with self._launch_lock:
            if self.page or self.ctx or self.offline_mode:
                return
            if self._closed:
                raise RuntimeError("AxiomWeb already closed")

            _ensure_playwright_available()

            proxy_cfg = _playwright_proxy()
            if proxy_cfg and not self._proxy_logged:
                _event("AXIOM_PROXY", proxy=_mask_proxy(_resolve_proxy_url()))
                self._proxy_logged = True

            with contextlib.suppress(Exception):
                Path(AXIOM_USER_DATA_DIR).mkdir(parents=True, exist_ok=True)

            # async_playwright is Any when available; keep call guarded by availability check above.
            self._pw = await async_playwright().start()  # type: ignore[misc]
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

            # lightweight stealth (don’t try to “fight” anti-bot systems aggressively)
            with contextlib.suppress(Exception):
                await self.page.add_init_script(
                    """
                    Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
                    Object.defineProperty(navigator, 'languages', { get: () => ['en-US','en'] });
                    Object.defineProperty(navigator, 'plugins', { get: () => [1,2,3] });
                    window.chrome = window.chrome || { runtime: {} };
                    """
                )

            # seed cookies if provided (optional)
            try:
                applied = await self._seed_cookies()
                _event("AXIOM_COOKIE_SEED", applied=applied)
            except Exception as e:
                _event("AXIOM_COOKIE_SEED_FAIL", err=_redact(str(e)))

            # prime session (best-effort)
            try:
                await self.goto("/", wait=AXIOM_NAV_WAIT)
                with contextlib.suppress(Exception):
                    await self.page.wait_for_timeout(400)
                with contextlib.suppress(Exception):
                    await self.page.reload(
                        wait_until=AXIOM_NAV_WAIT, timeout=min(15_000, AXIOM_TIMEOUT_MS)
                    )
                with contextlib.suppress(Exception):
                    await self.page.wait_for_timeout(250)
                _event("AXIOM_PRIMED")
            except Exception as e:
                _event("AXIOM_PRIME_FAIL", err=_redact(str(e)))

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True

        with contextlib.suppress(Exception):
            if self.page is not None:
                await self.page.close()
        with contextlib.suppress(Exception):
            if self.ctx is not None:
                await self.ctx.close()
        with contextlib.suppress(Exception):
            if self._pw is not None:
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
            cookies: list[dict[str, Any]] = []
            for k, v in jar.items():
                cookies.append(
                    {
                        "name": k,
                        "value": v,
                        "domain": _AX_DOMAIN,
                        "path": "/",
                        "httpOnly": True,
                        "secure": True,
                        "sameSite": "Lax",
                    }
                )
            if cookies:
                await self.ctx.add_cookies(cookies)
                applied = True

        token_cookies: list[dict[str, Any]] = []
        if AXIOM_TOKEN_ACCESS:
            token_cookies.append(
                {
                    "name": "auth-access-token",
                    "value": AXIOM_TOKEN_ACCESS,
                    "domain": _AX_DOMAIN,
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
                    "domain": _AX_DOMAIN,
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

        async def handler(route: Any) -> Any:
            req = getattr(route, "request", None)
            rt = str(getattr(req, "resource_type", "") or "").lower()
            url = str(getattr(req, "url", "") or "").lower()

            if AXIOM_BLOCK_IMAGES and rt == "image":
                return await route.abort()
            if AXIOM_BLOCK_FONTS and rt == "font":
                return await route.abort()
            if AXIOM_BLOCK_MEDIA and rt == "media":
                return await route.abort()

            if AXIOM_BLOCK_TRACKERS and any(bad in url for bad in tracker_needles):
                return await route.abort()

            return await route.continue_()

        await self.ctx.route("**/*", handler)

    def _require_page(self) -> Any:
        if not self.page:
            raise RuntimeError("AxiomWeb not launched")
        return self.page

    async def goto(self, path_or_url: str, *, wait: str = AXIOM_NAV_WAIT) -> None:
        """
        Navigation with retries + backoff.
        """
        if self.offline_mode:
            return

        page = self._require_page()

        if str(path_or_url).startswith("http"):
            url = str(path_or_url)
        else:
            path = str(path_or_url)
            if not path.startswith("/"):
                path = "/" + path
            url = f"{AXIOM_BASE}{path}"

        last_err: BaseException | None = None
        for attempt in range(AXIOM_GOTO_RETRIES + 1):
            try:
                with contextlib.suppress(Exception):
                    await page.set_extra_http_headers(
                        {
                            "User-Agent": UA,
                            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                            "Accept-Language": "en-US,en;q=0.9",
                            "Upgrade-Insecure-Requests": "1",
                        }
                    )
                await page.goto(url, wait_until=wait, timeout=AXIOM_TIMEOUT_MS)
                return
            except PwTimeout as e:
                last_err = e
            except Exception as e:
                last_err = e

            if attempt >= AXIOM_GOTO_RETRIES:
                break
            with contextlib.suppress(Exception):
                await page.wait_for_timeout(AXIOM_GOTO_BACKOFF_MS * (attempt + 1))

        raise RuntimeError(f"axiom_goto_failed: {url} ({_redact(str(last_err))})")

    async def _scroll(self, *, steps: int = 8, px: int = 1300, delay_ms: int = 140) -> None:
        if self.offline_mode:
            return
        page = self._require_page()
        for _ in range(max(1, int(steps))):
            with contextlib.suppress(Exception):
                await page.mouse.wheel(0, int(px))
            with contextlib.suppress(Exception):
                await page.wait_for_timeout(int(delay_ms))

    async def _pull_hydration_json(self) -> list[dict[str, Any]]:
        """
        Best-effort: read Next.js hydration blob from window.__NEXT_DATA__.
        """
        if self.offline_mode:
            return []
        page = self._require_page()
        blobs: list[dict[str, Any]] = []
        with contextlib.suppress(Exception):
            payload = await page.evaluate("window.__NEXT_DATA__ || null")
            if payload and isinstance(payload, dict):
                blobs.append(payload)
        return blobs

    async def _page_html(self) -> str:
        if self.offline_mode:
            return ""
        page = self._require_page()
        try:
            html = await page.content()
            return _safe_slice(html, AXIOM_MAX_HTML_BYTES)
        except Exception:
            return ""

    # ------------------------ public: discovery ------------------------

    async def discover_sections(
        self,
        *,
        limit_per_section: int = AXIOM_MAX_PER_SECTION,
        sections: Sequence[str] | None = None,
    ) -> dict[str, list[dict[str, Any]]]:
        """
        Return: {section_name: [ {mint, section, url, source, ts}, ... ], ...}
        """
        limit_per_section_i = max(1, int(limit_per_section))
        section_names = [
            s.strip() for s in (sections or AXIOM_SECTIONS or ["Trending"]) if s.strip()
        ]
        if not section_names:
            section_names = ["Trending"]

        if self.offline_mode:
            out: dict[str, list[dict[str, Any]]] = {}
            for sec in section_names:
                rows = _offline_rows(
                    sections=[sec],
                    limit_per_section=limit_per_section_i,
                    limit_total=limit_per_section_i,
                )
                out[sec] = rows
            return out

        await self.goto(AXIOM_DISCOVER_PATH, wait=AXIOM_NAV_WAIT)
        page = self._require_page()
        with contextlib.suppress(Exception):
            await page.wait_for_timeout(450)
        await self._scroll(steps=6)

        out_sections: dict[str, list[dict[str, Any]]] = {name: [] for name in section_names}

        # 1) Hydration-first
        blobs = await self._pull_hydration_json()
        if blobs:
            with contextlib.suppress(Exception):
                raw = _safe_slice(
                    json.dumps(blobs[0], ensure_ascii=False), AXIOM_MAX_HYDRATION_BYTES
                )
                mints = _find_mints(raw)
                if mints:
                    first = section_names[0]
                    out_sections[first] = [
                        AxiomRow(
                            mint=m,
                            section=first,
                            url=f"{AXIOM_BASE}/asset/{m}",
                            ts=_now_ts(),
                        ).asdict()
                        for m in mints[:limit_per_section_i]
                    ]

        # 2) HTML fallback per section
        if all(len(v) == 0 for v in out_sections.values()):
            html = await self._page_html()
            for sec in section_names:
                chunk = _slice_near_heading(html, sec, span=40_000)
                mints = _find_mints(chunk) if chunk else []
                if not mints:
                    mints = _find_mints(html)

                out_sections[sec] = [
                    AxiomRow(
                        mint=m,
                        section=sec,
                        url=f"{AXIOM_BASE}/asset/{m}",
                        ts=_now_ts(),
                    ).asdict()
                    for m in mints[:limit_per_section_i]
                ]

        _event("AXIOM_DISCOVER", counts={k: len(v) for k, v in out_sections.items()})
        return out_sections

    async def discover_mints(
        self,
        *,
        limit_total: int = 200,
        limit_per_section: int = AXIOM_MAX_PER_SECTION,
        sections: Sequence[str] | None = None,
    ) -> list[dict[str, Any]]:
        """Flattened unique rows across sections (deterministic, keep-first)."""
        limit_total_i = max(0, int(limit_total))
        limit_per_section_i = max(1, int(limit_per_section))

        section_names = [
            s.strip() for s in (sections or AXIOM_SECTIONS or ["Trending"]) if s.strip()
        ]
        if not section_names:
            section_names = ["Trending"]

        if self.offline_mode:
            return _offline_rows(
                sections=section_names,
                limit_per_section=limit_per_section_i,
                limit_total=limit_total_i,
            )

        sec = await self.discover_sections(
            limit_per_section=limit_per_section_i, sections=section_names
        )

        flat: list[dict[str, Any]] = []
        for sname, rows in sec.items():
            for r in rows:
                rr = dict(r)
                rr["section"] = sname
                flat.append(rr)

        # uniq by mint (keep first seen)
        seen: set[str] = set()
        uniq: list[dict[str, Any]] = []
        for r in flat:
            m = r.get("mint")
            if isinstance(m, str) and m and m not in seen:
                seen.add(m)
                uniq.append(r)
            if len(uniq) >= limit_total_i:
                break

        return uniq[:limit_total_i]

    # ------------------------ public: trackers ------------------------

    async def get_trackers(self, *, limit: int = 100) -> list[dict[str, Any]]:
        """
        Scrape trackers page (best-effort). Output fields are not guaranteed stable.
        """
        if self.offline_mode:
            return []

        out: list[dict[str, Any]] = []
        for path in ("/trackers", "/discover?tab=trackers"):
            try:
                await self.goto(path, wait=AXIOM_NAV_WAIT)
                page = self._require_page()
                with contextlib.suppress(Exception):
                    await page.wait_for_timeout(600)
                await self._scroll(steps=8)
                html = await self._page_html()

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
                _event("AXIOM_TRACKERS_FAIL", path=path, err=_redact(str(e)))
                continue

        return out[: max(0, int(limit))]

    # ------------------------ public: portfolio/rewards ------------------------

    async def get_portfolio(self) -> dict[str, Any]:
        """
        Best-effort portfolio snapshot (requires logged session).
        Returns: {"balances":[{"symbol","amount","usd"}...]}
        """
        if self.offline_mode:
            return {"balances": []}

        balances: list[dict[str, Any]] = []
        try:
            await self.goto("/portfolio", wait=AXIOM_NAV_WAIT)
            page = self._require_page()
            with contextlib.suppress(Exception):
                await page.wait_for_timeout(700)
            await self._scroll(steps=6)
            html = await self._page_html()

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
            _event("AXIOM_PORTFOLIO_FAIL", err=_redact(str(e)))

        if not balances:
            with contextlib.suppress(Exception):
                blobs = await self._pull_hydration_json()
                if blobs:
                    text = _safe_slice(
                        json.dumps(blobs[0], ensure_ascii=False), AXIOM_MAX_HYDRATION_BYTES
                    )
                    for row in re.finditer(
                        r'"symbol"\s*:\s*"(?P<sym>[A-Z0-9\.\-]{2,12})".{0,260}?"amount"\s*:\s*"?(?P<amt>[\d\.,]+)".{0,260}?"usd"\s*:\s*"?(?P<usd>[\d\.,]+)"?',
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

        return {"balances": balances}

    async def get_rewards(self) -> dict[str, Any]:
        """
        Best-effort rewards snapshot (requires logged session).
        Returns: {"points": float|None, "rank": int|None}
        """
        if self.offline_mode:
            return {"points": None, "rank": None}

        points: float | None = None
        rank: int | None = None

        try:
            await self.goto("/rewards", wait=AXIOM_NAV_WAIT)
            page = self._require_page()
            with contextlib.suppress(Exception):
                await page.wait_for_timeout(500)
            html = await self._page_html()

            points = _first_float(re.findall(r"(?:Points|Score|XP)\D+([\d\.,]+)", html))
            rank = _first_int(re.findall(r"(?:Rank|Position)\D+([\d,]+)", html))
        except Exception as e:
            _event("AXIOM_REWARDS_FAIL", err=_redact(str(e)))

        if points is None or rank is None:
            with contextlib.suppress(Exception):
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

        return {"points": points, "rank": rank}


# ──────────────────────────────────────────────────────────────────────────────
# Convenience helper
# ──────────────────────────────────────────────────────────────────────────────
async def axiom_discover_once(
    *,
    limit_total: int = 200,
    limit_per_section: int = AXIOM_MAX_PER_SECTION,
    sections: Sequence[str] | None = None,
    headless: bool = AXIOM_HEADLESS,
    offline: bool = False,
    no_network: bool = False,
    dry_run: bool = False,
    sample: bool = False,
) -> list[dict[str, Any]]:
    async with AxiomWeb(
        headless=headless,
        offline=offline,
        no_network=no_network,
        dry_run=dry_run,
        sample=sample,
    ) as ax:
        return await ax.discover_mints(
            limit_total=limit_total,
            limit_per_section=limit_per_section,
            sections=sections,
        )


# ──────────────────────────────────────────────────────────────────────────────
# CLI smoke
# ──────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s:%(name)s: %(message)s"
    )

    async def _t() -> None:
        async with AxiomWeb(headless=True) as ax:
            rows = await ax.discover_mints(limit_total=120, limit_per_section=25)
            print("discover_mints:", len(rows), "first:", rows[:3])
            print("trackers:", (await ax.get_trackers(limit=10))[:3])
            print("portfolio:", await ax.get_portfolio())
            print("rewards:", await ax.get_rewards())

    asyncio.run(_t())


__all__ = [
    "EventLogger",
    "get_logger",
    "AxiomRow",
    "AxiomWeb",
    "axiom_discover_once",
]
