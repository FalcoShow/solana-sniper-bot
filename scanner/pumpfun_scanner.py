
# scanner/pumpfun_scanner.py
# Pump.fun scanner (defensive). Tries official/undocumented endpoints first,
# then falls back to parsing the public Trending page. Works with or without
# explicit (redis, session) params and will noop gracefully if blocked.
from __future__ import annotations

import os, re, json, asyncio, logging, ssl
from typing import Any, Dict, List, Optional, Tuple

try:
    import certifi
except Exception:
    certifi = None

try:
    from utils_pkg.core_utils import get_session as _get_session  # optional accessor
except Exception:
    _get_session = None

logger = logging.getLogger("scanner.pumpfun")

# --- config
_UA = os.getenv("PUMPFUN_UA") or os.getenv("USER_AGENT") or "sniper/1.0 (+render)"
_COOKIE = os.getenv("PUMPFUN_COOKIE", "")
# You can override the first endpoint with PUMPFUN_URL if needed
_PRIMARY_JSON = os.getenv("PUMPFUN_URL", "").strip() or "https://frontend-api.pump.fun/coins/trending?offset=0&limit=50"
# Known alternates we've seen in the wild
_ALT_JSONS = [
    "https://frontend-api.pump.fun/coins/trending?offset=0&limit=100",
    "https://frontend-api.pump.fun/coins/new?offset=0&limit=50",
    # Legacy guesses that sometimes pop up. We'll still try them, but expect 404.
    "https://pump.fun/api/trending",
]
_HTML_TRENDING = "https://pump.fun/trending"

async def _ensure_session(session):
    if session is not None:
        return session
    if _get_session:
        try:
            s = _get_session()
            if s:
                return s
        except Exception:
            pass
    # Lazy local session, last resort (aiohttp imported only here)
    import aiohttp
    ssl_ctx = ssl.create_default_context(cafile=(certifi.where() if certifi else None))
    return aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=ssl_ctx))

def _headers_json() -> Dict[str, str]:
    h = {"accept": "application/json", "user-agent": _UA}
    if _COOKIE:
        h["cookie"] = _COOKIE
    return h

def _headers_html() -> Dict[str, str]:
    h = {"accept": "text/html,application/xhtml+xml", "user-agent": _UA}
    if _COOKIE:
        h["cookie"] = _COOKIE
    return h

def _mk_candidate(coin: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    # We try to coerce a consistent shape. Different endpoints may name fields differently.
    mint = coin.get("mint") or coin.get("mintAddress") or coin.get("address") or coin.get("publicKey")
    if not mint or not isinstance(mint, str):
        return None
    symbol = coin.get("symbol") or coin.get("ticker") or coin.get("name") or mint[:6]
    liq_usd = coin.get("liquidityUsd") or coin.get("liq_usd") or coin.get("liquidityUSD") or 0.0
    age_min = coin.get("pool_age_min") or coin.get("ageMin") or coin.get("createdAtMinutesAgo")
    impact1k = coin.get("impact_1k_bps") or coin.get("priceImpact1kBps")
    return {
        "mint": mint,
        "symbol": symbol,
        "liq_usd": float(liq_usd) if liq_usd else 0.0,
        "pool_age_min": float(age_min) if age_min else None,
        "impact_1k_bps": float(impact1k) if impact1k else None,
        "source": "pumpfun",
    }

async def _fetch_json(session, url: str) -> Optional[Dict[str, Any]]:
    try:
        async with session.get(url, headers=_headers_json(), timeout=10) as r:
            if r.status != 200:
                # Attach first ~120 chars of body for debugging in logs (mirrors sniper logs)
                try:
                    txt = await r.text()
                    logger.warning("Pump.fun [%s] HTTP %s observed; first 120 chars: %s",
                                   url, r.status, txt[:120])
                except Exception:
                    logger.warning("Pump.fun [%s] HTTP %s observed; <no body>", url, r.status)
                return None
            ct = (r.headers.get("content-type") or "").lower()
            if "application/json" not in ct:
                txt = await r.text()
                logger.warning("Pump.fun [%s] non-JSON content-type; first 120 chars: %s", url, txt[:120])
                return None
            return await r.json()
    except Exception as e:
        logger.warning("Pump.fun JSON fetch error on %s: %s", url, e)
        return None

_NEXT_DATA_RE = re.compile(r"__NEXT_DATA__.*?(\{.*?\})</script>", re.S)

def _find_next_data(html: str) -> Optional[Dict[str, Any]]:
    # Try id-based first
    m = re.search(r'<script id="__NEXT_DATA__" type="application/json">(.*?)</script>', html, re.S)
    blob = None
    if m:
        blob = m.group(1)
    else:
        m2 = _NEXT_DATA_RE.search(html)
        if m2:
            blob = m2.group(1)
    if not blob:
        return None
    try:
        return json.loads(blob)
    except Exception:
        # Sometimes HTML encodes characters; try unescape minimal
        blob2 = blob.replace("&quot;", '"').replace("&#34;", '"')
        try:
            return json.loads(blob2)
        except Exception:
            return None

def _coins_from_next(next_json: Dict[str, Any]) -> List[Dict[str, Any]]:
    # Heuristic walk: look for a list-like field containing dicts with 'mint'/'symbol'
    out: List[Dict[str, Any]] = []
    def walk(node):
        if isinstance(node, dict):
            # If this dict looks like a "coin" record, collect it
            if any(k in node for k in ("mint", "mintAddress", "publicKey")):
                cand = _mk_candidate(node)
                if cand:
                    out.append(cand)
            for v in node.values():
                walk(v)
        elif isinstance(node, list):
            for v in node:
                walk(v)
    walk(next_json)
    return out

async def _fetch_html_trending(session) -> List[Dict[str, Any]]:
    try:
        async with session.get(_HTML_TRENDING, headers=_headers_html(), timeout=12) as r:
            if r.status != 200:
                try:
                    txt = await r.text()
                    logger.warning("Pump.fun HTML fallback: status=%s", r.status)
                    # The main process already logs the first 120 chars; no need to double log here.
                except Exception:
                    logger.warning("Pump.fun HTML fallback: status=%s <no body>", r.status)
                return []
            html = await r.text()
    except Exception as e:
        logger.warning("Pump.fun trending HTML fetch error: %s", e)
        return []
    next_json = _find_next_data(html)
    if not next_json:
        logger.warning("Pump.fun HTML fallback: __NEXT_DATA__ not found.")
        return []
    coins = _coins_from_next(next_json)
    return coins

async def scan_pumpfun(*args) -> List[Dict[str, Any]]:
    """Signature-tolerant entrypoint: scan_pumpfun(redis?, session?) -> List[Dict]."""
    # Resolve session from args if provided
    redis = None
    session = None
    if len(args) == 2:
        redis, session = args
    elif len(args) == 1:
        # If only one arg, it's likely redis (we ignore)
        redis = args[0]

    created_local_session = False
    if session is None:
        session = await _ensure_session(None)
        created_local_session = True

    try:
        # 1) Primary JSON
        data = await _fetch_json(session, _PRIMARY_JSON)
        coins: List[Dict[str, Any]] = []
        if data:
            # Try common shapes
            candidates = None
            for key in ("coins", "items", "data", "trendingCoins", "result"):
                if isinstance(data, dict) and key in data and isinstance(data[key], list):
                    candidates = data[key]
                    break
            if candidates is None and isinstance(data, list):
                candidates = data
            if candidates:
                for rec in candidates:
                    cand = _mk_candidate(rec if isinstance(rec, dict) else {})
                    if cand:
                        coins.append(cand)
                if coins:
                    return coins

        # 2) Alternate JSON endpoints
        for url in _ALT_JSONS:
            data = await _fetch_json(session, url)
            if not data:
                continue
            candidates = None
            for key in ("coins", "items", "data", "trendingCoins", "result"):
                if isinstance(data, dict) and key in data and isinstance(data[key], list):
                    candidates = data[key]
                    break
            if candidates is None and isinstance(data, list):
                candidates = data
            if candidates:
                for rec in candidates:
                    cand = _mk_candidate(rec if isinstance(rec, dict) else {})
                    if cand:
                        coins.append(cand)
                if coins:
                    return coins

        # 3) HTML fallback
        coins = await _fetch_html_trending(session)
        return coins or []
    finally:
        if created_local_session:
            try:
                await session.close()
            except Exception:
                pass
