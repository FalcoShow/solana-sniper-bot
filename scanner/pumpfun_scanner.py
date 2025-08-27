# scanner/pumpfun_scanner.py
from __future__ import annotations

import os, re, json, logging, math, time
from typing import Any, Dict, List, Optional

from utils_pkg import core_utils as cu  # import the module, not symbols

LOG = logging.getLogger("scanner.pumpfun")

# --- small utils --------------------------------------------------------------
def _now_s() -> int:
    return int(time.time())

def _env(name: str, default: str = "") -> str:
    v = os.getenv(name, "")
    return v.strip() if isinstance(v, str) else default

def _headers() -> Dict[str, str]:
    ua = _env("PUMPFUN_UA") or "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126 Safari/537.36"
    cookie = _env("PUMPFUN_COOKIE")
    h = {
        "accept": "application/json, text/plain, */*",
        "accept-language": "en-US,en;q=0.9",
        "cache-control": "no-cache",
        "pragma": "no-cache",
        "origin": "https://pump.fun",
        "referer": "https://pump.fun/",
        "user-agent": ua,
    }
    if cookie:
        h["cookie"] = cookie
    else:
        LOG.warning("Pump.fun: no PUMPFUN_COOKIE set; you may get 403/404/530 from Cloudflare.")
    return h

def _first(items: List[Dict[str, Any]]) -> Dict[str, Any]:
    return items[0] if items else {}

def _norm_candidate(src: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Try to normalize arbitrary pump.fun JSON blobs into our candidate shape.
    We’re tolerant: look for typical keys and fall back safely.
    """
    mint = src.get("mint") or src.get("mintAddress") or src.get("tokenAddress") or src.get("address")
    if not mint or not isinstance(mint, str):
        return None

    symbol = src.get("symbol") or src.get("ticker") or (mint[:6])
    liq_usd = 0.0
    pool_age_min = None
    impact_1k_bps = None

    # common shapes we’ve seen
    try:
        liq_usd = float(
            src.get("liq_usd")
            or (src.get("liquidity") or {}).get("usd")
            or (src.get("liquidityUsd"))
            or 0.0
        )
    except Exception:
        liq_usd = 0.0

    # age in minutes if a timestamp exists
    for ts_key in ("createdAt", "launchedAt", "listedAt", "created_at", "launchTs", "timestamp"):
        ts = src.get(ts_key)
        try:
            if isinstance(ts, (int, float)) and ts > 1_000_000_000:
                pool_age_min = max(0, int((_now_s() - int(ts)) / 60))
                break
        except Exception:
            pass

    # sometimes price impact is provided; otherwise we leave None
    for k in ("impact_1k_bps", "priceImpact1kBps", "impact_bps"):
        v = src.get(k)
        try:
            impact_1k_bps = float(v) if v is not None else impact_1k_bps
        except Exception:
            pass

    return {
        "mint": mint,
        "symbol": str(symbol) if symbol is not None else mint[:6],
        "liq_usd": float(liq_usd),
        "pool_age_min": pool_age_min,
        "impact_1k_bps": impact_1k_bps,
        "source": "pumpfun",
    }

def _dedupe_keep_latest(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    for x in items:
        m = x.get("mint")
        if not m:
            continue
        # prefer newer/younger pools if we have an age
        if m not in out:
            out[m] = x
        else:
            a = out[m].get("pool_age_min")
            b = x.get("pool_age_min")
            if a is None and b is not None:
                out[m] = x
            elif a is not None and b is not None and b < a:
                out[m] = x
    return list(out.values())

# --- HTML fallback ------------------------------------------------------------
_NEXT_DATA_RE = re.compile(
    r'<script[^>]+id="__NEXT_DATA__"[^>]*>(?P<json>{.*?})</script>',
    re.DOTALL | re.IGNORECASE,
)

def _extract_next_data(html: str) -> Optional[Dict[str, Any]]:
    m = _NEXT_DATA_RE.search(html or "")
    if not m:
        return None
    try:
        return json.loads(m.group("json"))
    except Exception:
        return None

def _walk_for_mints(node: Any, found: List[Dict[str, Any]]) -> None:
    if isinstance(node, dict):
        # treat dicts that look like tokens
        if any(k in node for k in ("mint", "mintAddress", "tokenAddress", "address")):
            norm = _norm_candidate(node)
            if norm:
                found.append(norm)
        for v in node.values():
            _walk_for_mints(v, found)
    elif isinstance(node, list):
        for v in node:
            _walk_for_mints(v, found)

# --- main scanner -------------------------------------------------------------
async def scan_pumpfun(redis, session=None) -> List[Dict[str, Any]]:
    """
    Return a list of candidate dicts from Pump.fun.
    Signature: (redis, session=None)  ← works with sniper._maybe_call_scanner
    Falls back gracefully if API is blocked by Cloudflare.
    """
    s = session or cu.session
    if s is None or getattr(s, "closed", False):
        LOG.warning("Pump.fun scanner: no HTTP session available")
        return []

    hdrs = _headers()
    urls: List[str] = [
        # Try a few candidates; at least one usually works depending on CF rules.
        # These may change; the HTML fallback below keeps us alive.
        "https://pump.fun/api/trending?sort=buys_5m&limit=50",
        "https://pump.fun/api/trending?sort=volume_5m&limit=50",
        "https://pump.fun/api/trending?sort=recent&limit=50",
    ]

    # 1) Try JSON endpoints first
    for url in urls:
        try:
            async with s.get(url, headers=hdrs, timeout=8) as r:
                ct = r.headers.get("content-type", "")
                txt = await r.text()
                if r.status != 200:
                    LOG.warning("Pump.fun [%s] HTTP %s observed; first 120 chars: %s",
                                url.split("?")[0], r.status, txt[:120].replace("\n", " "))
                    continue

                if "application/json" in ct or txt.strip().startswith("{") or txt.strip().startswith("["):
                    try:
                        data = json.loads(txt)
                    except Exception:
                        LOG.warning("Pump.fun: JSON parse failed on %s", url)
                        continue

                    pairs = []
                    # common shapes: {"data":[ {...}, {...} ]} or {"pairs":[...]} or just list
                    if isinstance(data, dict):
                        for key in ("data", "pairs", "items", "coins"):
                            if isinstance(data.get(key), list):
                                pairs = data[key]
                                break
                    elif isinstance(data, list):
                        pairs = data

                    out: List[Dict[str, Any]] = []
                    for raw in pairs:
                        norm = _norm_candidate(raw) if isinstance(raw, dict) else None
                        if norm:
                            out.append(norm)

                    if out:
                        LOG.info("scanner.pumpfun: %d candidates from JSON", len(out))
                        return _dedupe_keep_latest(out)
                else:
                    LOG.warning("Pump.fun: %s returned non-JSON (ct=%s).", url, ct)
        except Exception as e:
            LOG.warning("Pump.fun request error on %s: %s", url, e)

    # 2) Fallback: fetch HTML and parse __NEXT_DATA__
    try:
        async with s.get("https://pump.fun/", headers=hdrs, timeout=8) as r:
            html = await r.text()
            if r.status != 200:
                LOG.warning("Pump.fun HTML fallback HTTP %s", r.status)
                return []
            blob = _extract_next_data(html)
            if not blob:
                LOG.warning("Pump.fun HTML fallback: __NEXT_DATA__ not found.")
                return []

            found: List[Dict[str, Any]] = []
            _walk_for_mints(blob, found)
            out = _dedupe_keep_latest(found)
            if out:
                LOG.info("scanner.pumpfun: %d candidates from HTML fallback", len(out))
            else:
                LOG.warning("Pump.fun HTML fallback produced 0 candidates.")
            return out
    except Exception as e:
        LOG.warning("Pump.fun HTML fallback error: %s", e)
        return []
