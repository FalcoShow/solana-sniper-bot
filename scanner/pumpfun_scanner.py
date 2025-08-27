
import os
import re
import ssl
import json
import time
import certifi
import logging
from typing import Any, Dict, List, Optional, Tuple

import aiohttp

# This module is designed to be signature-flexible:
#   scan_pumpfun(), scan_pumpfun(redis), scan_pumpfun(redis, session)
# It returns a List[Dict[str, Any]] of candidates with common keys:
#   mint, symbol, liq_usd, pool_age_min, impact_1k_bps (best effort)

LOG = logging.getLogger("scanner.pumpfun")

# default UA
DEF_UA = (
    os.getenv("PUMPFUN_UA")
    or "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
       "Chrome/126 Safari/537.36"
)

# optional Cloudflare cookie(s). Accept either pump.fun cookies or frontend-api cookies.
RAW_COOKIE = os.getenv("PUMPFUN_COOKIE", "").strip()

# optional override
OVERRIDE_URL = os.getenv("PUMPFUN_URL", "").strip()


def _hdrs() -> Dict[str, str]:
    return {
        "User-Agent": DEF_UA,
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://pump.fun/trending",
        "Origin": "https://pump.fun",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
    }


def _attach_cookie(session: aiohttp.ClientSession) -> None:
    """Attach cookie string to both pump.fun and frontend-api subdomain (best-effort)."""
    if not RAW_COOKIE:
        return
    try:
        cookie_pairs = {}
        for part in RAW_COOKIE.split(";"):
            if "=" in part:
                k, v = part.split("=", 1)
                cookie_pairs[k.strip()] = v.strip()
        if not cookie_pairs:
            return
        session.cookie_jar.update_cookies(cookie_pairs, response_url=aiohttp.client_reqrep.URL("https://pump.fun/"))
        session.cookie_jar.update_cookies(cookie_pairs, response_url=aiohttp.client_reqrep.URL("https://frontend-api.pump.fun/"))
    except Exception as e:
        LOG.warning("Pump.fun cookie attach failed: %s", e)


def _now() -> float:
    return time.time()


def _age_min(created_ts: Optional[float]) -> Optional[float]:
    try:
        if created_ts is None:
            return None
        # accept seconds or ms
        if created_ts > 10_000_000_000:  # ms
            created_ts = created_ts / 1000.0
        return max(0.0, (_now() - float(created_ts)) / 60.0)
    except Exception:
        return None


def _map_coin(obj: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Map a Pump.fun coin JSON shape into a common candidate dict."""
    try:
        mint = obj.get("mintAddress") or obj.get("mint") or obj.get("baseMint") or obj.get("tokenMint")
        if not mint:
            return None
        symbol = obj.get("symbol") or obj.get("tokenSymbol") or (mint[:6] if mint else "")
        liq_usd = None
        for k in ("liquidity", "liquidityUsd", "liqUsd", "lpUsd", "liquidity_usd"):
            if k in obj and obj[k] is not None:
                try:
                    liq_usd = float(obj[k])
                    break
                except Exception:
                    pass
        impact_bps = None
        for k in ("priceImpact1kBps", "impact1kBps", "impact_bps", "impact_1k_bps"):
            if k in obj and obj[k] is not None:
                try:
                    impact_bps = float(obj[k])
                    break
                except Exception:
                    pass
        created = obj.get("createdAt") or obj.get("createdAtTs") or obj.get("created") or obj.get("createdOn")
        pool_age_min = _age_min(float(created)) if created is not None else None
        out = {
            "source": "pumpfun",
            "mint": mint,
            "symbol": symbol,
            "liq_usd": liq_usd,
            "pool_age_min": pool_age_min,
            "impact_1k_bps": impact_bps,
        }
        return out
    except Exception:
        return None


async def _fetch_json(session: aiohttp.ClientSession, url: str) -> Tuple[Optional[Any], int, str]:
    """GET URL and try to JSON-decode. Returns (data_or_None, status, first120Text)."""
    try:
        async with session.get(url, headers=_hdrs(), timeout=aiohttp.ClientTimeout(total=12)) as r:
            status = r.status
            text = await r.text()
            first = text[:120].replace("\n", " ")
            if "text/html" in (r.headers.get("content-type") or "") or text.lstrip().startswith("<!DOCTYPE html"):
                return (None, status, first)
            try:
                data = json.loads(text)
                return (data, status, first)
            except Exception:
                # Sometimes it *is* JSON but with wrong Content-Type
                try:
                    data = await r.json(content_type=None)
                    return (data, status, first)
                except Exception:
                    return (None, status, first)
    except Exception as e:
        LOG.warning("Pump.fun request failed: %s %s", url, e)
        return (None, 0, "")


async def _html_and_nextdata(session: aiohttp.ClientSession) -> List[Dict[str, Any]]:
    """Fallback: fetch /trending HTML, try to discover Next.js build id, then fetch data JSON."""
    # 1) GET the main page
    try:
        async with session.get("https://pump.fun/trending", headers=_hdrs(), timeout=aiohttp.ClientTimeout(total=12)) as r:
            status = r.status
            html = await r.text()
            if status >= 400:
                LOG.warning("Pump.fun HTML fallback: status=%s", status)
                return []
    except Exception as e:
        LOG.warning("Pump.fun HTML fallback failed: %s", e)
        return []

    # 2) Try direct __NEXT_DATA__ first
    m = re.search(r'id="__NEXT_DATA__"[^>]*>\s*({.*?})\s*</script>', html, re.S)
    if m:
        try:
            data = json.loads(m.group(1))
            coins = _extract_from_next_data(data)
            if coins:
                return coins
        except Exception:
            pass

    # 3) Try to extract buildId from /_next/static/<buildId>/_buildManifest.js
    m2 = re.search(r"/_next/static/([^/]+)/_buildManifest\.js", html)
    if not m2:
        LOG.warning("Pump.fun HTML fallback: __NEXT_DATA__ not found.")
        return []
    build_id = m2.group(1)
    data_url = f"https://pump.fun/_next/data/{build_id}/trending.json"
    data, status, _ = await _fetch_json(session, data_url)
    if not data:
        LOG.warning("Pump.fun HTML fallback: build json fetch failed (status=%s).", status)
        return []
    coins = _extract_from_next_data(data)
    return coins


def _extract_from_next_data(data: Any) -> List[Dict[str, Any]]:
    """Best-effort extraction from Next.js pageProps."""
    try:
        # Look for any array under data['pageProps']
        page = data.get("pageProps", {}) if isinstance(data, dict) else {}
        # Common keys seen across builds
        for key in ("trending", "coins", "projects", "items", "data"):
            arr = page.get(key)
            if isinstance(arr, list) and arr:
                out: List[Dict[str, Any]] = []
                for it in arr:
                    mapped = _map_coin(it) if isinstance(it, dict) else None
                    if mapped:
                        out.append(mapped)
                return out
        # Recursively search lists inside pageProps
        out: List[Dict[str, Any]] = []
        def walk(x):
            if isinstance(x, dict):
                for v in x.values():
                    walk(v)
            elif isinstance(x, list):
                for v in x:
                    if isinstance(v, dict):
                        m = _map_coin(v)
                        if m:
                            out.append(m)
                    else:
                        walk(v)
        walk(page)
        return out
    except Exception:
        return []


async def _scan_inner(session: aiohttp.ClientSession) -> List[Dict[str, Any]]:
    """Try multiple endpoints and fallbacks."""
    _attach_cookie(session)

    tried: List[str] = []
    endpoints = []
    if OVERRIDE_URL:
        endpoints.append(OVERRIDE_URL)
    # Current public endpoints seen in the wild
    endpoints += [
        "https://frontend-api.pump.fun/coins/trending?offset=0&limit=50",
        "https://frontend-api.pump.fun/coins/new?offset=0&limit=50",
        # Legacy (may 404)
        "https://pump.fun/api/trending",
    ]

    # 1) Direct JSON endpoints
    for url in endpoints:
        data, status, first = await _fetch_json(session, url)
        tried.append(f"{url} status={status}")
        if status in (404, 403, 429, 530) and not data:
            # noisy but helpful once per deploy
            LOG.warning("Pump.fun [%s] HTTP %s observed; first 120 chars: %s", url, status, first)
            continue
        if not data:
            continue
        # Normalize shapes
        items: List[Dict[str, Any]] = []
        if isinstance(data, list):
            for it in data:
                if isinstance(it, dict):
                    m = _map_coin(it)
                    if m:
                        items.append(m)
        elif isinstance(data, dict):
            # Common shapes
            arr = None
            for k in ("trending", "coins", "items", "projects", "data"):
                if k in data and isinstance(data[k], list):
                    arr = data[k]; break
                if "data" in data and isinstance(data["data"], dict) and k in data["data"] and isinstance(data["data"][k], list):
                    arr = data["data"][k]; break
            if arr:
                for it in arr:
                    if isinstance(it, dict):
                        m = _map_coin(it)
                        if m:
                            items.append(m)
        if items:
            return items

    # 2) HTML + Next.js fallback
    coins = await _html_and_nextdata(session)
    if coins:
        return coins

    LOG.warning("Pump.fun all endpoints failed: tried=%s", "; ".join(tried))
    return []


async def scan_pumpfun(*args):
    """Flexible entrypoint.

    Supports:
      - scan_pumpfun()
      - scan_pumpfun(redis)
      - scan_pumpfun(redis, session)

    Only the 'session' (aiohttp.ClientSession) is used if provided.
    """
    # Accept (redis, session) pattern from rest of the app
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

    # If no external session is provided, create a local one
    sslctx = ssl.create_default_context(cafile=certifi.where())
    timeout = aiohttp.ClientTimeout(total=15)
    conn = aiohttp.TCPConnector(ssl=sslctx, limit=30)
    async with aiohttp.ClientSession(connector=conn, timeout=timeout) as local:
        try:
            items = await _scan_inner(local)
            LOG.info("scanner.pumpfun: %s candidates", len(items))
            return items
        except Exception as e:
            LOG.warning("Pump.fun scan error: %s", e)
            return []
