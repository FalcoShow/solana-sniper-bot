# scanner/pumpfun_scanner.py — v6 (Apex-first: next-data → DOM → API)
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
        apply_cookies_to_session,              # attach per-host cookies to aiohttp session
        get_cookie_for,                        # cookie string for a given URL/host
        flag_cookie_bad,                       # mark cookie stale (e.g., on 530/403)
        browser_fetch_json,                    # last-resort JSON via Playwright (API host)
        browser_fetch_trending_via_next_data,  # Next.js JSON from apex (/_next/data)
        browser_scrape_trending_dom,           # NEW: pure DOM scrape from apex
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

OVERRIDE_URL = os.getenv("PUMPFUN_URL", "").strip()

REQ_TIMEOUT_S = int(os.getenv("PUMPFUN_HTTP_TIMEOUT_S", "15"))
SESSION_LIMIT = int(os.getenv("PUMPFUN_HTTP_CONN_LIMIT", "30"))

# ---------------------------------------------------------------------------
# Header builder (per-host cookies, reasonable browser-y headers)
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
        if float(created_ts) > 10_000_000_000:
            created_ts = float(created_ts) / 1000.0
        return max(0.0, (_now() - float(created_ts)) / 60.0)
    except Exception:
        return None

def _map_coin(obj: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    try:
        mint = (
            obj.get("mintAddress")
            or obj.get("mint")
            or obj.get("baseMint")
            or obj.get("tokenMint")
            or obj.get("mint_id")  # in case DOM scrape returns this key
        )
        if not mint:
            # Also accept a plain {"mint": "..."} from DOM scrape
            if "mint" in obj and isinstance(obj["mint"], str):
                mint = obj["mint"]
            else:
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
    items: List[Dict[str, Any]] = []

    if isinstance(payload, list):
        src = payload
    elif isinstance(payload, dict):
        src: List[Dict[str, Any]] | None = None
        for k in ("trending", "coins", "items", "projects", "data"):
            if k in payload and isinstance(payload[k], list):
                src = payload[k]
                break
            if (
                "data" in payload
                and isinstance(payload["data"], dict)
                and k in payload["data"]
                and isinstance(payload["data"][k], list)
            ):
                src = payload["data"][k]
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
# HTTP helpers
# ---------------------------------------------------------------------------
async def _fetch_json(session: aiohttp.ClientSession, url: str) -> Tuple[Optional[Any], int, str]:
    try:
        async with session.get(
            url, headers=_hdrs(url), timeout=aiohttp.ClientTimeout(total=REQ_TIMEOUT_S)
        ) as r:
            status = r.status
            text = await r.text()
            first = text[:120].replace("\n", " ")
            ctype = (r.headers.get("content-type") or "").lower()

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

# ---------------------------------------------------------------------------
# HTML __NEXT_DATA__ fallback (raw GET, no Playwright)
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
        LOG.warning("Pump.fun HTML fallback: __NEXT_DATA__ not found.")
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
    apply_cookies_to_session(session)

    # 0) Apex Next.js data (/_next/data) via Playwright
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
                return items
    except Exception as e:
        LOG.warning("Pump.fun next-data apex fetch error: %s", e)

    # 1) Apex DOM scrape via Playwright (no API host)
    try:
        dom = await browser_scrape_trending_dom(limit=50)
        if isinstance(dom, list) and dom:
            items: List[Dict[str, Any]] = []
            for it in dom:
                if isinstance(it, dict):
                    m = _map_coin(it)
                    if m:
                        items.append(m)
            if items:
                return items
    except Exception as e:
        LOG.warning("Pump.fun apex DOM scrape error: %s", e)

    tried: List[str] = []
    endpoints: List[str] = []

    if OVERRIDE_URL:
        endpoints.append(OVERRIDE_URL)

    endpoints += [
        "https://frontend-api.pump.fun/coins/trending?offset=0&limit=50",
        "https://frontend-api.pump.fun/coins/new?offset=0&limit=50",
        "https://pump.fun/api/trending",
    ]

    # 2) Direct JSON endpoints (CF-aware)
    for url in endpoints:
        payload, status, first = await _fetch_json(session, url)
        tried.append(f"{url} status={status}")
        if not payload:
            if status in (404, 403, 429, 530):
                LOG.warning("Pump.fun: [%s] HTTP %s; first 120: %s", url, status, first)
            continue
        items = _extract_items_from_payload(payload)
        if items:
            return items

    # 3) HTML __NEXT_DATA__ fallback (raw GET)
    items = await _html_next_data(session)
    if items:
        return items

    # 4) Last-resort: browser fetch on API trending (then normalize)
    fallback_url = "https://frontend-api.pump.fun/coins/trending?offset=0&limit=50"
    payload = await browser_fetch_json(fallback_url)
    items = _extract_items_from_payload(payload)
    if items:
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

    sslctx = ssl.create_default_context(cafile=certifi.where())
    timeout = aiohttp.ClientTimeout(total=REQ_TIMEOUT_S + 5)
    conn = aiohttp.TCPConnector(ssl=sslctx, limit=SESSION_LIMIT)
    async with aiohttp.ClientSession(connector=conn, timeout=timeout) as local:
        try:
            items = await _scan_inner(local)
            LOG.info("scanner.pumpfun: %s candidates", len(items))
            return items
        except Exception as e:
            LOG.warning("Pump.fun scan error: %s", e)
            return []

__all__ = ["scan_pumpfun"]
