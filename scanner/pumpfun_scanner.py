# scanner/pumpfun_scanner.py — v9
# Apex-first (next-data → DOM → HTML) with primary-API fallback; proxy/cookie aware.

from __future__ import annotations

import os
import re
import ssl
import json
import time
import logging
from typing import Any, Dict, List, Optional, Tuple

import aiohttp
import certifi

LOG = logging.getLogger("scanner.pumpfun")

# ---------------------------------------------------------------------------
# Cloudflare / Playwright helpers
# ---------------------------------------------------------------------------
try:
    from utils_pkg.pumpfun_cf import (
        apply_cookies_to_session,
        get_cookie_for,
        flag_cookie_bad,
        browser_fetch_json,
        browser_fetch_trending_via_next_data,
        browser_scrape_trending_dom,
    )
except Exception:  # pragma: no cover
    def apply_cookies_to_session(_session):  # type: ignore
        pass
    def get_cookie_for(_url_or_host: str) -> str:  # type: ignore
        return os.getenv("PUMPFUN_COOKIE_API", "") or os.getenv("PUMPFUN_COOKIE", "")
    def flag_cookie_bad() -> None:  # type: ignore
        pass
    async def browser_fetch_json(_url: str):  # type: ignore
        return None
    async def browser_fetch_trending_via_next_data(_limit: int = 50):  # type: ignore
        return None
    async def browser_scrape_trending_dom(_limit: int = 50):  # type: ignore
        return None

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
UA = os.getenv("PUMPFUN_UA") or (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/126 Safari/537.36"
)

# Optional override *full URL* (for testing a single endpoint)
OVERRIDE_URL = os.getenv("PUMPFUN_URL", "").strip()

REQ_TIMEOUT_S = int(os.getenv("PUMPFUN_HTTP_TIMEOUT_S", "15"))
SESSION_LIMIT = int(os.getenv("PUMPFUN_HTTP_CONN_LIMIT", "30"))

def _api_bases() -> List[str]:
    """
    Order of API bases to try. Env override first; then primary only.
    (No hardcoded mirror — we removed it due to DNS issues in your environment.)
    """
    bases: List[str] = []
    env_base = os.getenv("PUMPFUN_API_BASE", "").strip().rstrip("/")
    if env_base:
        bases.append(env_base)
    bases.append("https://frontend-api.pump.fun")
    # de-dup while preserving order
    seen, out = set(), []
    for b in bases:
        if b and b not in seen:
            seen.add(b)
            out.append(b)
    return out

# ---------------------------------------------------------------------------
# Header builder
# ---------------------------------------------------------------------------
def _host_of(url: str) -> str:
    try:
        from urllib.parse import urlparse
        return (urlparse(url).hostname or "").lower()
    except Exception:
        return ""

def _hdrs(url: str) -> Dict[str, str]:
    host = _host_of(url)
    origin = "https://frontend-api.pump.fun" if host.endswith("frontend-api.pump.fun") else "https://pump.fun"
    h = {
        "User-Agent": UA,
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://pump.fun/trending",
        "Origin": origin,
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
    }
    ck = get_cookie_for(url)
    if ck:
        h["Cookie"] = ck
    return h

# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------
def _now() -> float:
    return time.time()

def _age_min(created_ts: Optional[float]) -> Optional[float]:
    try:
        if created_ts is None:
            return None
        # accept seconds or milliseconds
        if float(created_ts) > 10_000_000_000:
            created_ts = float(created_ts) / 1000.0
        return max(0.0, (_now() - float(created_ts)) / 60.0)
    except Exception:
        return None

def _map_coin(obj: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Normalize multiple Pump.fun response shapes into a common candidate dict.
    """
    try:
        mint = (
            obj.get("mintAddress")
            or obj.get("mint")
            or obj.get("baseMint")
            or obj.get("tokenMint")
            or obj.get("mint_id")
        )
        if not mint:
            return None

        symbol = obj.get("symbol") or obj.get("tokenSymbol") or obj.get("name") or mint[:6]

        liq_usd = None
        for k in ("liquidity", "liquidityUsd", "liqUsd", "lpUsd", "liquidity_usd"):
            v = obj.get(k)
            if v is not None:
                try:
                    liq_usd = float(v)
                    break
                except Exception:
                    pass

        impact_bps = None
        for k in ("priceImpact1kBps", "impact1kBps", "impact_bps", "impact_1k_bps"):
            v = obj.get(k)
            if v is not None:
                try:
                    impact_bps = float(v)
                    break
                except Exception:
                    pass

        created = (
            obj.get("createdAt")
            or obj.get("createdAtTs")
            or obj.get("created")
            or obj.get("createdOn")
        )
        pool_age_min = _age_min(float(created)) if created is not None else None

        return {
            "source": "pumpfun",
            "mint": mint,
            "symbol": symbol,
            "liq_usd": liq_usd,
            "pool_age_min": pool_age_min,
            "impact_1k_bps": impact_bps,
        }
    except Exception:
        return None

def _extract_items_from_payload(payload: Any) -> List[Dict[str, Any]]:
    """
    Accept a server payload (list or dict) and return a list of mapped coin dicts.
    """
    items: List[Dict[str, Any]] = []

    if isinstance(payload, list):
        src = payload
    elif isinstance(payload, dict):
        src: List[Dict[str, Any]] | None = None
        for k in ("trending", "coins", "items", "projects", "data"):
            if k in payload and isinstance(payload[k], list):
                src = payload[k]
                break
            if "data" in payload and isinstance(payload["data"], dict):
                dd = payload["data"]
                for kk in ("trending", "coins", "items", "projects", "data"):
                    if kk in dd and isinstance(dd[kk], list):
                        src = dd[kk]
                        break
                if src:
                    break
        src = src or []
    else:
        src = []

    for it in src:
        if isinstance(it, dict):
            m = _map_coin(it)
            if m:
                items.append(m)
    return items

# ---------------------------------------------------------------------------
# HTTP helpers (JSON first)
# ---------------------------------------------------------------------------
async def _fetch_json(session: aiohttp.ClientSession, url: str) -> Tuple[Optional[Any], int, str]:
    """
    Primary fetch: try aiohttp. If HTML/403/429/530, mark cookies bad and
    attempt a browser-based JSON fetch for the same URL.
    """
    try:
        async with session.get(
            url, headers=_hdrs(url), timeout=aiohttp.ClientTimeout(total=REQ_TIMEOUT_S)
        ) as r:
            status = r.status
            text = await r.text()
            first = text[:120].replace("\n", " ")
            ctype = (r.headers.get("content-type") or "").lower()

            # Cloudflare / blocked patterns
            if status in (403, 429, 530) or "text/html" in ctype or text.lstrip().startswith("<!DOCTYPE html"):
                if status in (403, 429, 530):
                    flag_cookie_bad()
                try:
                    data = await browser_fetch_json(url)
                    if data is not None:
                        return (data, 200, first)
                except Exception:
                    pass
                return (None, status, first)

            # Normal JSON path
            try:
                return (json.loads(text), status, first)
            except Exception:
                try:
                    return (await r.json(content_type=None), status, first)
                except Exception:
                    return (None, status, first)

    except Exception as e:
        LOG.warning("Pump.fun request failed: %s %s", url, e)
        return (None, 0, "")

def _join_url(base: str, path: str) -> str:
    base = base.rstrip("/")
    path = path.lstrip("/")
    return f"{base}/{path}"

async def _fetch_api_json_with_fallback(
    session: aiohttp.ClientSession,
    path: str,
    tried_out: Optional[List[str]] = None,
) -> Optional[List[Dict[str, Any]]]:
    """
    Try each API base until one returns JSON with items.
    path example: "/coins/trending?offset=0&limit=50"
    """
    # Allow a one-off full URL override (debug/testing)
    if OVERRIDE_URL.lower().startswith("http"):
        payload, status, first = await _fetch_json(session, OVERRIDE_URL)
        if payload:
            items = _extract_items_from_payload(payload)
            if items:
                LOG.info("scanner.pumpfun: API JSON (override) → %s items", len(items))
                return items
        if tried_out is not None:
            tried_out.append(f"{OVERRIDE_URL} status={status}")
        # fall through to bases

    for base in _api_bases():
        url = _join_url(base, path)
        payload, status, first = await _fetch_json(session, url)
        if tried_out is not None:
            tried_out.append(f"{url} status={status}")
        if not payload:
            if status in (404, 403, 429, 530):
                LOG.warning("Pump.fun: [%s] HTTP %s; first 120: %s", url, status, first)
            continue
        items = _extract_items_from_payload(payload)
        if items:
            LOG.info("scanner.pumpfun: API JSON → %s items (base=%s)", len(items), base)
            return items

    return None

# ---------------------------------------------------------------------------
# HTML helpers: scrape coin links directly from apex HTML (no JS)
# ---------------------------------------------------------------------------
_BASE58 = r"[1-9A-HJ-NP-Za-km-z]{32,64}"

def _uniq(seq: List[str]) -> List[str]:
    seen = set()
    out = []
    for x in seq:
        if x not in seen:
            seen.add(x)
            out.append(x)
    return out

async def _html_scrape_links(session: aiohttp.ClientSession, path: str, limit: int = 50) -> List[Dict[str, Any]]:
    """
    Fetch the raw HTML for an apex page and regex out coin links.
    Works even if JS doesn't hydrate or Next.js JSON is absent.
    """
    url = f"https://pump.fun{path}"
    try:
        async with session.get(url, headers=_hdrs(url), timeout=aiohttp.ClientTimeout(total=REQ_TIMEOUT_S)) as r:
            html = await r.text()
    except Exception as e:
        LOG.warning("Pump.fun HTML link scrape failed (%s): %s", path, e)
        return []

    # Look for /coin/<mint>, /coins/<mint>, /token/<mint>, ?mint=<mint>, data-mint="<mint>"
    mints: List[str] = []
    for pattern in [
        rf'href="(?:https://pump\.fun)?/coin/({_BASE58})"',
        rf'href="(?:https://pump\.fun)?/coins/({_BASE58})"',
        rf'href="(?:https://pump\.fun)?/token/({_BASE58})"',
        rf'[?&]mint=({_BASE58})',
        rf'data-mint="({_BASE58})"',
    ]:
        mints += re.findall(pattern, html)

    mints = _uniq(mints)[:limit]
    return [{"mint": m} for m in mints]

# ---------------------------------------------------------------------------
# HTML __NEXT_DATA__ fallback (legacy, may be absent)
# ---------------------------------------------------------------------------
async def _html_next_data(session: aiohttp.ClientSession) -> List[Dict[str, Any]]:
    url = "https://pump.fun/trending"
    try:
        async with session.get(
            url, headers=_hdrs(url), timeout=aiohttp.ClientTimeout(total=REQ_TIMEOUT_S)
        ) as r:
            html = await r.text()
    except Exception as e:
        LOG.warning("Pump.fun HTML fallback failed: %s", e)
        return []

    m = re.search(r'id="__NEXT_DATA__"[^>]*>\s*({.*?})\s*</script>', html, re.S)
    if not m:
        return []

    try:
        data = json.loads(m.group(1))
    except Exception:
        return []

    page = data.get("props", {}).get("pageProps", {}) if isinstance(data, dict) else {}
    arr: Optional[List[Dict[str, Any]]] = None
    for k in ("trending", "coins", "projects", "items", "data"):
        v = page.get(k)
        if isinstance(v, list) and v:
            arr = v
            break

    items: List[Dict[str, Any]] = []
    if isinstance(arr, list):
        for it in arr:
            if isinstance(it, dict):
                mobj = _map_coin(it)
                if mobj:
                    items.append(mobj)
    return items

# ---------------------------------------------------------------------------
# Scanner core
# ---------------------------------------------------------------------------
async def _scan_inner(session: aiohttp.ClientSession) -> List[Dict[str, Any]]:
    # Ensure the session has current cookies for both hosts.
    apply_cookies_to_session(session)

    # 0) Apex Next.js data (/_next/data) via Playwright (cheap; may be absent)
    try:
        raw = await browser_fetch_trending_via_next_data(limit=50)
        if isinstance(raw, list) and raw:
            items: List[Dict[str, Any]] = []
            for it in raw:
                if isinstance(it, dict):
                    m = _map_coin(it)
                    if m:
                        items.append(m)
            if items:
                LOG.info("scanner.pumpfun: next-data apex → %s items", len(items))
                return items
    except Exception as e:
        LOG.warning("Pump.fun next-data apex fetch error: %s", e)

    # 1) Apex DOM scrape via Playwright (no API host)
    try:
        dom = await browser_scrape_trending_dom(limit=50)
        if isinstance(dom, list) and dom:
            items = []
            for it in dom:
                if isinstance(it, dict):
                    m = _map_coin(it)
                    if m:
                        items.append(m)
            if items:
                LOG.info("scanner.pumpfun: DOM apex → %s items", len(items))
                return items
    except Exception as e:
        LOG.warning("Pump.fun apex DOM scrape error: %s", e)

    tried: List[str] = []
    endpoints: List[str] = []

    if OVERRIDE_URL:
        endpoints.append(OVERRIDE_URL)

    # 2) Pure HTML (regex) from apex pages (no JS, no Playwright)
    html_items: List[Dict[str, Any]] = []
    for path in ("/", "/trending", "/advanced/scan"):
        try:
            links = await _html_scrape_links(session, path, limit=50)
            if links:
                html_items.extend(links)
        except Exception:
            pass
    if html_items:
        # Dedup by mint
        seen, uniq = set(), []
        for d in html_items:
            m = d.get("mint")
            if isinstance(m, str) and m not in seen:
                seen.add(m)
                uniq.append(d)
        items = []
        for it in uniq:
            m = _map_coin(it)
            if m:
                items.append(m)
        if items:
            LOG.info("scanner.pumpfun: HTML apex → %s items", len(items))
            return items

    # 3) API JSON using primary/override fallback
    items = await _fetch_api_json_with_fallback(
        session, "/coins/trending?offset=0&limit=50", tried_out=tried
    )
    if items:
        return items

    items = await _fetch_api_json_with_fallback(
        session, "/coins/new?offset=0&limit=50", tried_out=tried
    )
    if items:
        return items

    # 4) Legacy endpoint (often 404) — try once
    legacy = "https://pump.fun/api/trending"
    payload, status, first = await _fetch_json(session, legacy)
    tried.append(f"{legacy} status={status}")
    if payload:
        items = _extract_items_from_payload(payload)
        if items:
            LOG.info("scanner.pumpfun: legacy API JSON → %s items", len(items))
            return items

    # 5) HTML __NEXT_DATA__ fallback (legacy)
    items = await _html_next_data(session)
    if items:
        LOG.info("scanner.pumpfun: HTML __NEXT_DATA__ → %s items", len(items))
        return items

    # 6) Last resort: Playwright fetch to first available base
    try:
        first_base = _api_bases()[0]
    except Exception:
        first_base = "https://frontend-api.pump.fun"
    fallback_url = _join_url(first_base, "/coins/trending?offset=0&limit=50")
    payload = await browser_fetch_json(fallback_url)
    items = _extract_items_from_payload(payload)
    if items:
        LOG.info("scanner.pumpfun: Playwright API → %s items", len(items))
        return items

    LOG.warning("Pump.fun all endpoints failed: tried=%s", "; ".join(tried))
    return []

# ---------------------------------------------------------------------------
# Public entrypoint
# ---------------------------------------------------------------------------
async def scan_pumpfun(*args):
    """
    Flexible entrypoint:
      - scan_pumpfun()
      - scan_pumpfun(redis)
      - scan_pumpfun(redis, session)
      - scan_pumpfun(session)
    Only the aiohttp.ClientSession (if provided) is used.
    """
    sess: Optional[aiohttp.ClientSession] = None
    if len(args) >= 2 and isinstance(args[1], aiohttp.ClientSession):
        sess = args[1]
    elif len(args) == 1 and isinstance(args[0], aiohttp.ClientSession):
        sess = args[0]

    if sess is not None:
        try:
            items = await _scan_inner(sess)
            LOG.info("scanner.pumpfun: %s candidates", len(items))
            return items
        except Exception as e:
            LOG.warning("Pump.fun scan error (session): %s", e)
            return []

    # Create a local session if none was supplied (proxy/env aware via trust_env)
    sslctx = ssl.create_default_context(cafile=certifi.where())
    timeout = aiohttp.ClientTimeout(total=REQ_TIMEOUT_S + 5)
    conn = aiohttp.TCPConnector(ssl=sslctx, limit=SESSION_LIMIT)
    async with aiohttp.ClientSession(connector=conn, timeout=timeout, trust_env=True) as local:
        try:
            items = await _scan_inner(local)
            LOG.info("scanner.pumpfun: %s candidates", len(items))
            return items
        except Exception as e:
            LOG.warning("Pump.fun scan error: %s", e)
            return []

__all__ = ["scan_pumpfun"]
