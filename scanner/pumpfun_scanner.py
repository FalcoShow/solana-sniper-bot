# scanner/pumpfun_scanner.py
# Robust Pump.fun scanner:
# - Accepts signatures: () | (redis,) | (redis, session)
# - Uses optional Cloudflare cookie + custom UA from env (PUMPFUN_COOKIE, PUMPFUN_UA)
# - Tries JSON endpoint first (if it exists), then falls back to HTML parsing of __NEXT_DATA__
# - Never writes files to /mnt/data (previous version caused FileNotFoundError)
from __future__ import annotations

import os, re, json, logging, asyncio
from typing import Any, Dict, List, Optional

try:
    # prefer getting the global session created by core_utils.init_resources()
    from utils_pkg.core_utils import get_session
except Exception:
    get_session = lambda: None  # type: ignore

logger = logging.getLogger("scanner.pumpfun")

_JSON_URL = os.getenv("PUMPFUN_JSON_URL", "https://pump.fun/api/trending")
_HTML_URL = os.getenv("PUMPFUN_TRENDING_URL", "https://pump.fun/trending")

def _pump_headers() -> Dict[str, str]:
    ua = os.getenv("PUMPFUN_UA") or "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126 Safari/537.36"
    cookie = os.getenv("PUMPFUN_COOKIE", "").strip()
    h = {
        "user-agent": ua,
        "accept": "text/html,application/json;q=0.9,*/*;q=0.8",
        "accept-language": "en-US,en;q=0.9",
        "cache-control": "no-cache",
        "pragma": "no-cache",
        "referer": "https://pump.fun/",
    }
    if cookie:
        h["cookie"] = cookie
    return h

def _unique_append(out: List[Dict[str, Any]], item: Dict[str, Any], seen: set) -> None:
    mint = item.get("mint")
    if not mint or mint in seen:
        return
    seen.add(mint)
    out.append(item)

def _extract_candidates_from_any(obj: Any) -> List[Dict[str, Any]]:
    """
    Walk arbitrary JSON looking for dicts that look like Pump.fun tokens.
    We consider keys like: mint, mintAddress, tokenAddress, tokenMint.
    """
    out: List[Dict[str, Any]] = []
    seen: set[str] = set()

    def visit(x: Any) -> None:
        if isinstance(x, dict):
            keys = set(x.keys())
            if keys & {"mint", "mintAddress", "tokenAddress", "tokenMint"}:
                mint = x.get("mint") or x.get("mintAddress") or x.get("tokenAddress") or x.get("tokenMint")
                sym  = x.get("symbol") or x.get("ticker") or (x.get("name") or "")[:12]
                liq  = x.get("liq_usd") or x.get("liquidity_usd") or x.get("usdLiquidity") or 0.0
                item = {"mint": str(mint), "symbol": str(sym or "")[:16], "liq_usd": float(liq) if liq else 0.0}
                _unique_append(out, item, seen)
            for v in x.values():
                visit(v)
        elif isinstance(x, list):
            for v in x:
                visit(v)

    visit(obj)
    return out

async def _try_json(session) -> List[Dict[str, Any]]:
    try:
        async with session.get(_JSON_URL, headers=_pump_headers(), timeout=12) as r:
            txt = await r.text()
            if r.status != 200:
                logger.warning("scanner.pumpfun: Pump.fun [%s] HTTP %s observed; first 120 chars: %s",
                               _JSON_URL, r.status, txt[:120].replace("\n", " "))
                return []
            try:
                data = json.loads(txt)
            except Exception:
                # sometimes Next API returns JS, not json; attempt to locate JSON object inside text
                m = re.search(r"\\{.*\\}", txt, re.S)
                data = json.loads(m.group(0)) if m else {}
            return _extract_candidates_from_any(data)
    except Exception as e:
        logger.warning("scanner.pumpfun: JSON attempt failed: %s", e)
        return []

async def _try_html(session) -> List[Dict[str, Any]]:
    try:
        async with session.get(_HTML_URL, headers=_pump_headers(), timeout=12) as r:
            html = await r.text()
            if r.status != 200:
                logger.warning("scanner.pumpfun: HTML fallback: status=%s", r.status)
            # Look for Next.js data
            m = re.search(r'<script[^>]*id="__NEXT_DATA__"[^>]*>(.*?)</script>', html, re.S | re.I)
            if not m:
                # Some deployments put it on window.__NEXT_DATA__
                m = re.search(r"__NEXT_DATA__\\s*=\\s*(\\{.*?\\})\\s*<", html, re.S)
            if not m:
                logger.warning("scanner.pumpfun: HTML fallback: __NEXT_DATA__ not found.")
                return []
            data = json.loads(m.group(1))
            # Typical place is props.pageProps.* — but we'll scan the whole object
            return _extract_candidates_from_any(data)
    except Exception as e:
        logger.warning("scanner.pumpfun: HTML attempt failed: %s", e)
        return []

async def scan_pumpfun(*args) -> List[Dict[str, Any]]:
    """
    Return a list of candidate dicts: at minimum include 'mint' and 'symbol' (others optional).
    Signature tolerant:
      scan_pumpfun()
      scan_pumpfun(redis)
      scan_pumpfun(redis, session)
    """
    # Pick session from args, else global
    http = None
    if len(args) >= 2:
        http = args[1]
    if http is None:
        try:
            http = get_session()
        except Exception:
            http = None
    if http is None:
        logger.warning("scanner.pumpfun: no HTTP session available")
        return []

    # Try JSON, then HTML
    cands = await _try_json(http)
    if not cands:
        cands = await _try_html(http)

    # light enrichment: add a placeholder for impact_1k_bps so downstream is happy
    for c in cands:
        c.setdefault("impact_1k_bps", None)
        c.setdefault("pool_age_min", None)
    return cands
