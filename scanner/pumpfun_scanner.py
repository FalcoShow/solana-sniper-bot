# Write an improved Pump.fun scanner with multiple fallbacks and tolerant signature.
from textwrap import dedent
from pathlib import Path

code = dedent('''
# scanner/pumpfun_scanner.py
# Robust Pump.fun scanner with JSON API attempts + HTML fallback.
# Signature tolerant: scan_pumpfun(redis=None, session=None) or scan_pumpfun(redis) or scan_pumpfun().
from __future__ import annotations

import os, re, json, ssl, asyncio, logging
from typing import Any, Dict, List, Optional
import certifi

try:
    import aiohttp
except Exception:  # pragma: no cover
    aiohttp = None  # type: ignore

# Light coupling on core_utils only for session getter if available
try:
    from utils_pkg.core_utils import get_session  # type: ignore
except Exception:  # pragma: no cover
    def get_session():
        return None

log = logging.getLogger("scanner.pumpfun")

USER_AGENT = os.getenv("PUMPFUN_UA", "").strip() or "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126 Safari/537.36"
COOKIE = os.getenv("PUMPFUN_COOKIE", "").strip()
TIMEOUT = int(os.getenv("PUMPFUN_TIMEOUT_SEC", "10"))
MAX_ITEMS = int(os.getenv("PUMPFUN_MAX_ITEMS", "50"))

JSON_ENDPOINTS = [
    # Public FE API (often works; still CF-protected). We try both "trending" and "last".
    "https://frontend-api.pump.fun/coins/trending?limit=60&offset=0",
    "https://frontend-api.pump.fun/coins/last?limit=100&offset=0",
]
HTML_PAGES = [
    "https://pump.fun/trending",
    "https://pump.fun/",
]

def _headers(json_pref: bool = True) -> Dict[str, str]:
    h = {
        "user-agent": USER_AGENT,
        "accept-language": "en-US,en;q=0.9",
        "referer": "https://pump.fun/",
        "origin": "https://pump.fun",
    }
    if json_pref:
        h["accept"] = "application/json, text/plain, */*"
    else:
        h["accept"] = "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
    if COOKIE:
        h["cookie"] = COOKIE
    return h

def _age_minutes(ts: Optional[str]) -> Optional[float]:
    if not ts:
        return None
    try:
        # Many Pump.fun APIs return ISO timestamps; parse lightly
        from datetime import datetime, timezone
        t = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        return max(0.0, (datetime.now(timezone.utc) - t).total_seconds() / 60.0)
    except Exception:
        return None

def _map_coin(obj: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Map a Pump.fun coin-like dict to our candidate schema.
    We only require "mint" to consider it valid.
    """
    mint = obj.get("mint") or obj.get("mintAddress") or obj.get("mint_address")
    if not mint:
        return None
    sym = obj.get("symbol") or obj.get("ticker") or (obj.get("name") or "")[:8]
    liq = obj.get("liquidityUSD") or obj.get("liq_usd") or obj.get("usdLiquidity") or obj.get("liquidity_usd")
    if liq is not None:
        try:
            liq = float(liq)
        except Exception:
            liq = None
    age = _age_minutes(obj.get("launchedAt") or obj.get("createdAt") or obj.get("launch_at") or obj.get("created_at"))
    imp = obj.get("impact_1k_bps") or obj.get("priceImpact1kBps")
    try:
        imp = float(imp) if imp is not None else None
    except Exception:
        imp = None
    return {
        "source": "pumpfun",
        "mint": mint,
        "symbol": sym or mint[:6],
        "liq_usd": liq,
        "pool_age_min": age,
        "impact_1k_bps": imp,
    }

async def _ensure_session(session: Optional[aiohttp.ClientSession]) -> aiohttp.ClientSession:
    if session and not session.closed:
        return session
    if aiohttp is None:
        raise RuntimeError("aiohttp not available")
    sslctx = ssl.create_default_context(cafile=certifi.where())
    return aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=sslctx))

async def _fetch_json(session: aiohttp.ClientSession, url: str) -> Optional[Any]:
    try:
        async with session.get(url, headers=_headers(True), timeout=TIMEOUT) as r:
            if r.status == 200:
                return await r.json(content_type=None)
            txt = await r.text()
            log.warning("Pump.fun [%s] HTTP %s observed; first 120 chars: %s", url, r.status, txt[:120].replace("\\n"," "))
            return None
    except Exception as e:
        log.warning("Pump.fun JSON fetch error (%s): %s", url, e)
        return None

async def _fetch_html(session: aiohttp.ClientSession, url: str) -> Optional[str]:
    try:
        async with session.get(url, headers=_headers(False), timeout=TIMEOUT) as r:
            txt = await r.text()
            if r.status != 200:
                log.warning("Pump.fun HTML [%s] HTTP %s observed; first 120 chars: %s", url, r.status, txt[:120].replace("\\n"," "))
            return txt
    except Exception as e:
        log.warning("Pump.fun HTML fetch error (%s): %s", url, e)
        return None

def _extract_next_data(html: str) -> Optional[Dict[str, Any]]:
    if not html:
        return None
    # Next.js usually exposes a script tag with id="__NEXT_DATA__"
    m = re.search(r'<script[^>]+id="__NEXT_DATA__"[^>]*>(.*?)</script>', html, flags=re.S|re.I)
    if m:
        try:
            return json.loads(m.group(1))
        except Exception:
            pass
    # Some setups set window.__NEXT_DATA__ =
    m2 = re.search(r'__NEXT_DATA__\\s*=\\s*({.*?})\\s*;</script>', html, flags=re.S)
    if m2:
        try:
            return json.loads(m2.group(1))
        except Exception:
            pass
    return None

def _walk_collect_mints(obj: Any, acc: List[Dict[str, Any]]) -> None:
    try:
        if isinstance(obj, dict):
            if ("mint" in obj) or ("mintAddress" in obj) or ("mint_address" in obj):
                mapped = _map_coin(obj)
                if mapped:
                    acc.append(mapped)
            for v in obj.values():
                _walk_collect_mints(v, acc)
        elif isinstance(obj, list):
            for v in obj:
                _walk_collect_mints(v, acc)
    except Exception:
        pass

async def _from_json_endpoints(session: aiohttp.ClientSession) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for url in JSON_ENDPOINTS:
        data = await _fetch_json(session, url)
        if not data:
            continue
        # Accept { "coins": [...] } OR list
        coins = []
        if isinstance(data, dict):
            for key in ("coins", "data", "items", "result"):
                if key in data and isinstance(data[key], list):
                    coins = data[key]
                    break
        elif isinstance(data, list):
            coins = data
        # Map
        for c in coins:
            if not isinstance(c, dict):
                continue
            m = _map_coin(c)
            if m:
                out.append(m)
        if out:
            break  # first successful endpoint is enough
    return out[:MAX_ITEMS]

async def _from_html_fallback(session: aiohttp.ClientSession) -> List[Dict[str, Any]]:
    for url in HTML_PAGES:
        html = await _fetch_html(session, url)
        if not html:
            continue
        data = _extract_next_data(html)
        if not data:
            log.warning("Pump.fun HTML fallback: __NEXT_DATA__ not found.")
            continue
        acc: List[Dict[str, Any]] = []
        _walk_collect_mints(data, acc)
        if acc:
            return acc[:MAX_ITEMS]
    return []

async def scan_pumpfun(*args) -> List[Dict[str, Any]]:
    """
    Returns a list of candidate dicts.
    Tolerant signature:
      - scan_pumpfun(redis, session)
      - scan_pumpfun(redis)
      - scan_pumpfun()
    """
    # args unpack
    session_arg = None
    if len(args) >= 2:
        session_arg = args[1]

    # Prefer shared session from core_utils if available
    session = session_arg or get_session()
    if aiohttp is None or session is None:
        log.warning("Pump.fun scanner: no HTTP session available")
        return []

    # Try JSON endpoints first
    try:
        items = await _from_json_endpoints(session)
        if items:
            log.info("Pump.fun: %d candidates from JSON API", len(items))
            return items
    except Exception as e:
        log.warning("Pump.fun JSON stage error: %s", e)

    # HTML fallback
    try:
        items = await _from_html_fallback(session)
        if items:
            log.info("Pump.fun: %d candidates from HTML fallback", len(items))
            return items
    except Exception as e:
        log.warning("Pump.fun HTML stage error: %s", e)

    return []
''')

out_path = Path('/mnt/data/pumpfun_scanner.py')
out_path.write_text(code, encoding='utf-8')
out_path, out_path.stat().st_size
