# scanner/pumpfun_scanner.py — v3 (CF-aware, fallback-capable)
import os, ssl, json, logging, time
from typing import Any, Dict, List, Optional, Tuple

import aiohttp, certifi

LOG = logging.getLogger("scanner.pumpfun")

try:
    from utils_pkg.pumpfun_cf import get_cookie_header, flag_cookie_bad, browser_fetch_json
except Exception:
    def get_cookie_header() -> str: return os.getenv("PUMPFUN_COOKIE", "")
    def flag_cookie_bad() -> None: pass
    async def browser_fetch_json(_url: str): return None

UA = os.getenv("PUMPFUN_UA") or (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/126 Safari/537.36"
)

OVERRIDE_URL = os.getenv("PUMPFUN_URL", "").strip()

def _hdrs() -> Dict[str, str]:
    h = {
        "User-Agent": UA,
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://pump.fun/trending",
        "Origin": "https://pump.fun",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
    }
    ck = get_cookie_header()
    if ck:
        h["Cookie"] = ck
    return h

def _now() -> float: return time.time()

def _age_min(created_ts: Optional[float]) -> Optional[float]:
    try:
        if created_ts is None: return None
        if created_ts > 10_000_000_000: created_ts = created_ts / 1000.0
        return max(0.0, (_now() - float(created_ts)) / 60.0)
    except Exception:
        return None

def _map_coin(obj: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    try:
        mint = obj.get("mintAddress") or obj.get("mint") or obj.get("baseMint") or obj.get("tokenMint")
        if not mint: return None
        symbol = obj.get("symbol") or obj.get("tokenSymbol") or mint[:6]
        liq_usd = None
        for k in ("liquidity", "liquidityUsd", "liqUsd", "lpUsd", "liquidity_usd"):
            v = obj.get(k)
            if v is not None:
                try: liq_usd = float(v); break
                except Exception: pass
        impact_bps = None
        for k in ("priceImpact1kBps", "impact1kBps", "impact_bps", "impact_1k_bps"):
            v = obj.get(k)
            if v is not None:
                try: impact_bps = float(v); break
                except Exception: pass
        created = obj.get("createdAt") or obj.get("createdAtTs") or obj.get("created") or obj.get("createdOn")
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

async def _fetch_json(session: aiohttp.ClientSession, url: str) -> Tuple[Optional[Any], int, str]:
    try:
        async with session.get(url, headers=_hdrs(), timeout=aiohttp.ClientTimeout(total=15)) as r:
            status = r.status
            text = await r.text()
            first = text[:120].replace("\n", " ")
            if "text/html" in (r.headers.get("content-type") or "") or text.lstrip().startswith("<!DOCTYPE html"):
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

async def _scan_inner(session: aiohttp.ClientSession) -> List[Dict[str, Any]]:
    tried: List[str] = []
    endpoints = [OVERRIDE_URL] if OVERRIDE_URL else []
    endpoints += [
        "https://frontend-api.pump.fun/coins/trending?offset=0&limit=50",
        "https://frontend-api.pump.fun/coins/new?offset=0&limit=50",
        "https://pump.fun/api/trending",  # legacy
    ]

    # 1) direct JSON
    for url in endpoints:
        data, status, first = await _fetch_json(session, url)
        tried.append(f"{url} status={status}")
        if status in (404, 403, 429, 530) and not data:
            if status == 530:
                flag_cookie_bad()
            LOG.warning("Pump.fun: [%s] HTTP %s; first 120: %s", url, status, first)
            continue
        if not data:  # other non-JSON
            continue

        items: List[Dict[str, Any]] = []
        if isinstance(data, list):
            src = data
        elif isinstance(data, dict):
            src = None
            for k in ("trending", "coins", "items", "projects", "data"):
                if k in data and isinstance(data[k], list):
                    src = data[k]; break
                if "data" in data and isinstance(data["data"], dict) and k in data["data"] and isinstance(data["data"][k], list):
                    src = data["data"][k]; break
            src = src or []
        else:
            src = []

        for it in src:
            if isinstance(it, dict):
                m = _map_coin(it)
                if m: items.append(m)

        if items:
            return items

    # 2) one-shot browser fallback (works even if aiohttp gets 530)
    fallback_url = "https://frontend-api.pump.fun/coins/trending?offset=0&limit=50"
    data = await browser_fetch_json(fallback_url)
    if isinstance(data, dict):
        arr = None
        for k in ("trending", "coins", "items", "projects", "data"):
            if k in data and isinstance(data[k], list):
                arr = data[k]; break
            if "data" in data and isinstance(data["data"], dict) and k in data["data"] and isinstance(data["data"][k], list):
                arr = data["data"][k]; break
        data = arr or data

    if isinstance(data, list) and data:
        out: List[Dict[str, Any]] = []
        for it in data:
            if isinstance(it, dict):
                m = _map_coin(it)
                if m: out.append(m)
        if out:
            return out

    LOG.warning("Pump.fun all endpoints failed: tried=%s", "; ".join(tried))
    return []

async def scan_pumpfun(*args):
    # (redis, session) | (redis) | ()
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
    timeout = aiohttp.ClientTimeout(total=20)
    conn = aiohttp.TCPConnector(ssl=sslctx, limit=30)
    async with aiohttp.ClientSession(connector=conn, timeout=timeout) as local:
        try:
            items = await _scan_inner(local)
            LOG.info("scanner.pumpfun: %s candidates", len(items))
            return items
        except Exception as e:
            LOG.warning("Pump.fun scan error: %s", e)
            return []
