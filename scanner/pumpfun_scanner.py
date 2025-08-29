# scanner/pumpfun_scanner.py
from __future__ import annotations

import os, re, ssl, json, time, logging
from typing import Any, Dict, List, Optional, Tuple

import aiohttp
import certifi

LOG = logging.getLogger("scanner.pumpfun")

# ---------- Config & constants ----------

DEFAULT_UA = os.getenv(
    "PUMPFUN_UA",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/126 Safari/537.36",
)

# If set, used first
OVERRIDE_URL = (os.getenv("PUMPFUN_URL") or "").strip()

# Where the refresher stores the cookie (utils_pkg/pumpfun_cf.refresh_loop)
REDIS_COOKIE_KEY = "pumpfun:cookie"

TREND_URL = "https://frontend-api.pump.fun/coins/trending?offset=0&limit=50"
NEW_URL   = "https://frontend-api.pump.fun/coins/new?offset=0&limit=50"

REQ_TIMEOUT = aiohttp.ClientTimeout(total=20)

# ---------- Small helpers ----------

def _now() -> float:
    return time.time()

def _age_min(created_ts: Optional[float]) -> Optional[float]:
    """Return age in minutes from a unix ts (sec or ms)."""
    try:
        if created_ts is None:
            return None
        ts = float(created_ts)
        if ts > 10_000_000_000:  # milliseconds
            ts /= 1000.0
        return max(0.0, (_now() - ts) / 60.0)
    except Exception:
        return None

def _headers(cookie: str | None) -> Dict[str, str]:
    h = {
        "User-Agent": DEFAULT_UA,
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "Origin": "https://pump.fun",
        "Referer": "https://pump.fun/advanced/scan",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
    }
    if cookie:
        h["Cookie"] = cookie
    return h

async def _get_cookie(redis) -> str:
    """
    Priority:
      1) PUMPFUN_COOKIE env (useful for local debug)
      2) redis key "pumpfun:cookie" (minted by utils_pkg/pumpfun_cf.refresh_loop)
      3) empty string (request will likely 530 without cookie)
    """
    env = (os.getenv("PUMPFUN_COOKIE") or "").strip()
    if env:
        return env
    if redis:
        try:
            v = await redis.get(REDIS_COOKIE_KEY)
            if v:
                return v.decode() if isinstance(v, (bytes, bytearray)) else v
        except Exception as e:
            LOG.debug("pumpfun: redis cookie fetch failed: %s", e)
    return ""

# ---------- Mapping / normalization ----------

def _map_coin(obj: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Normalize Pump.fun-ish objects to:
    { source, mint, symbol, liq_usd, pool_age_min, impact_1k_bps }
    """
    try:
        mint = (
            obj.get("mintAddress")
            or obj.get("mint")
            or obj.get("baseMint")
            or obj.get("tokenMint")
        )
        if not mint:
            return None

        symbol = obj.get("symbol") or obj.get("tokenSymbol") or mint[:6]

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
        pool_age_min = _age_min(created) if created is not None else None

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

# ---------- HTTP helpers ----------

async def _fetch_json(session: aiohttp.ClientSession, url: str, headers: Dict[str, str]) -> Tuple[Optional[Any], int, str]:
    """
    GET url; try to decode JSON; return (data_or_None, status, first120text)
    """
    try:
        async with session.get(url, headers=headers, timeout=REQ_TIMEOUT) as r:
            status = r.status
            text = await r.text()
            first = text[:120].replace("\n", " ")
            # If it's clearly HTML, do not attempt to json decode
            ctype = (r.headers.get("content-type") or "").lower()
            if "text/html" in ctype or text.lstrip().startswith("<!DOCTYPE html"):
                return (None, status, first)
            try:
                return (json.loads(text), status, first)
            except Exception:
                try:
                    data = await r.json(content_type=None)
                    return (data, status, first)
                except Exception:
                    return (None, status, first)
    except Exception as e:
        LOG.warning("pumpfun: request failed %s (%s)", url, e)
        return (None, 0, "")

async def _html_and_nextdata(session: aiohttp.ClientSession, headers: Dict[str, str]) -> List[Dict[str, Any]]:
    """
    Fallback: fetch /advanced/scan (or /trending) HTML, try __NEXT_DATA__ or buildId-driven JSON.
    This may still be blocked by CF if cookie is invalid; we only try once.
    """
    page_url = "https://pump.fun/advanced/scan"
    try:
        async with session.get(page_url, headers=headers, timeout=REQ_TIMEOUT) as r:
            status = r.status
            html = await r.text()
            if status >= 400:
                LOG.warning("pumpfun: HTML fallback status=%s", status)
                return []
    except Exception as e:
        LOG.warning("pumpfun: HTML fallback failed: %s", e)
        return []

    # Try inline __NEXT_DATA__
    m = re.search(r'id="__NEXT_DATA__"[^>]*>\s*({.*?})\s*</script>', html, re.S)
    if m:
        try:
            data = json.loads(m.group(1))
            coins = _extract_from_next_data(data)
            if coins:
                return coins
        except Exception:
            pass

    # Try buildId → /_next/data/<id>/advanced/scan.json or trending.json
    m2 = re.search(r"/_next/static/([^/]+)/_buildManifest\.js", html)
    if not m2:
        LOG.warning("pumpfun: HTML fallback — __NEXT_DATA__ not found.")
        return []
    build_id = m2.group(1)

    for path in ("advanced/scan", "trending"):
        data_url = f"https://pump.fun/_next/data/{build_id}/{path}.json"
        data, status, _ = await _fetch_json(session, data_url, headers)
        if data:
            coins = _extract_from_next_data(data)
            if coins:
                return coins

    LOG.warning("pumpfun: HTML fallback build json fetch failed")
    return []

def _extract_from_next_data(data: Any) -> List[Dict[str, Any]]:
    """Best-effort mining from Next.js pageProps."""
    try:
        page = data.get("pageProps", {}) if isinstance(data, dict) else {}
        # Typical top-level arrays
        for key in ("trending", "coins", "projects", "items", "data"):
            arr = page.get(key)
            if isinstance(arr, list) and arr:
                out: List[Dict[str, Any]] = []
                for it in arr:
                    if isinstance(it, dict):
                        m = _map_coin(it)
                        if m:
                            out.append(m)
                if out:
                    return out

        # recursive scan
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

# ---------- Core scan ----------

async def _scan_inner(session: aiohttp.ClientSession, cookie: str) -> List[Dict[str, Any]]:
    """
    Try JSON API endpoints first, then HTML fallback.
    """
    headers = _headers(cookie)
    tried: List[str] = []

    endpoints: List[str] = []
    if OVERRIDE_URL:
        endpoints.append(OVERRIDE_URL)
    endpoints += [TREND_URL, NEW_URL]  # main API

    # 1) JSON endpoints
    items: List[Dict[str, Any]] = []
    for url in endpoints:
        data, status, first = await _fetch_json(session, url, headers)
        tried.append(f"{url} status={status}")
        if not data:
            # log noisy statuses once
            if status in (403, 404, 429, 530):
                LOG.warning("pumpfun: [%s] HTTP %s; first 120: %s", url, status, first)
            continue

        # Normalize
        if isinstance(data, list):
            for it in data:
                if isinstance(it, dict):
                    m = _map_coin(it)
                    if m:
                        items.append(m)
        elif isinstance(data, dict):
            arr = None
            for k in ("trending", "coins", "items", "projects", "data"):
                v = data.get(k)
                if isinstance(v, list):
                    arr = v
                    break
                dv = data.get("data")
                if isinstance(dv, dict):
                    v2 = dv.get(k)
                    if isinstance(v2, list):
                        arr = v2
                        break
            if arr:
                for it in arr:
                    if isinstance(it, dict):
                        m = _map_coin(it)
                        if m:
                            items.append(m)

        if items:
            return items

    # 2) HTML + buildId fallback
    coins = await _html_and_nextdata(session, headers)
    if coins:
        return coins

    LOG.warning("pumpfun: all endpoints failed: %s", "; ".join(tried))
    return []

# ---------- Public entrypoint (signature-flexible) ----------

async def scan_pumpfun(*args):
    """
    Flexible entry:
        scan_pumpfun()
        scan_pumpfun(redis)
        scan_pumpfun(redis, session)

    - We use 'redis' only to read the Cloudflare cookie minted by utils_pkg/pumpfun_cf.
    - If a ClientSession is passed, we reuse it; otherwise we create a short-lived one.
    """
    # Parse args
    redis = None
    session: Optional[aiohttp.ClientSession] = None

    if len(args) == 1:
        if isinstance(args[0], aiohttp.ClientSession):
            session = args[0]
        else:
            redis = args[0]
    elif len(args) >= 2:
        redis = args[0]
        if isinstance(args[1], aiohttp.ClientSession):
            session = args[1]

    cookie = await _get_cookie(redis)

    if session is not None:
        try:
            items = await _scan_inner(session, cookie)
            LOG.info("scanner.pumpfun: %d candidates", len(items))
            return items
        except Exception as e:
            LOG.warning("pumpfun: scan error (session): %s", e)
            return []

    # No external session → make a one-off
    sslctx = ssl.create_default_context(cafile=certifi.where())
    conn = aiohttp.TCPConnector(ssl=sslctx, limit=30)
    async with aiohttp.ClientSession(connector=conn, timeout=REQ_TIMEOUT) as sess:
        try:
            items = await _scan_inner(sess, cookie)
            LOG.info("scanner.pumpfun: %d candidates", len(items))
            return items
        except Exception as e:
            LOG.warning("pumpfun: scan error: %s", e)
            return []
