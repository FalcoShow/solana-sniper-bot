# scanner/pumpfun_scanner.py
# Robust Pump.fun scanner focused on *runners*:
# 1) JSON API (mirror → primary)
# 2) Next.js data via Playwright (/_next/data) [optional]
# 3) Apex DOM via Playwright [optional]
# 4) HTML __NEXT_DATA__ fallback (no Playwright)
from __future__ import annotations

import os
import re
import ssl
import json
import time
import logging
import asyncio
from typing import Any, Dict, List, Optional, Tuple

import aiohttp
import certifi

from config import (
    SCANNER_MIN_LIQ_USD,
    SCANNER_MIN_FDV_USD,
    SCANNER_MIN_BUYS_5M,
    SCANNER_MIN_BUYS_15M,
    SCANNER_MIN_VOLUME_5M_USD,
    SCANNER_MIN_VOLUME_15M_USD,
    SCANNER_MIN_BUY_RATIO,
    SCANNER_REQUIRE_BUY_PRESSURE,
    SCANNER_MIN_PRICE_CHANGE_5M,
    SCANNER_MIN_PRICE_CHANGE_15M,
    SCANNER_MAX_AGE_MIN,
    SCANNER_MAX_MC_USD,
    SCANNER_MIN_HOLDERS,
)

LOG = logging.getLogger("scanner.pumpfun")

# ──────────────────────────────────────────────────────────────────────────────
# Optional Cloudflare/Playwright helpers (gracefully degrade if missing)
# ──────────────────────────────────────────────────────────────────────────────
try:
    from utils_pkg.pumpfun_cf import (          # type: ignore
        apply_cookies_to_session,              # attach per-host cookies to aiohttp session
        get_cookie_for,                        # cookie string for a given URL/host
        flag_cookie_bad,                       # mark cookie stale (e.g., on 530/403/407)
        browser_fetch_json,                    # last-resort JSON via Playwright (API host)
        browser_fetch_trending_via_next_data,  # Next.js JSON from apex (/_next/data)
        browser_scrape_trending_dom,           # DOM scrape from apex pages (no API host)
    )
except Exception:  # pragma: no cover
    def apply_cookies_to_session(_session):  # type: ignore
        pass

    def get_cookie_for(_url_or_host: str) -> str:  # type: ignore
        # allow env cookie if provided
        return (
            os.getenv("CF_PUMP_COOKIE")
            or os.getenv("PUMPFUN_COOKIE")
            or os.getenv("PUMP_COOKIE")
            or ""
        )

    def flag_cookie_bad() -> None:  # type: ignore
        pass

    async def browser_fetch_json(_url: str):  # type: ignore
        return None

    async def browser_fetch_trending_via_next_data(*, limit: int = 50):  # type: ignore
        return None

    async def browser_scrape_trending_dom(*, limit: int = 50):  # type: ignore
        return None

# ──────────────────────────────────────────────────────────────────────────────
# Config / flags (with Pump-specific overrides on top of config defaults)
# ──────────────────────────────────────────────────────────────────────────────
UA = os.getenv("PUMPFUN_UA") or (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/126 Safari/537.36"
)

# Optional override *full URL* for debugging a single endpoint
OVERRIDE_URL = os.getenv("PUMPFUN_OVERRIDE_URL", "").strip()

# Optional HTTP proxy specifically for Pump.fun requests (aiohttp-compatible URI)
PUMPFUN_HTTP_PROXY = (os.getenv("PUMPFUN_HTTP_PROXY") or "").strip()

REQ_TIMEOUT_S  = int(os.getenv("PUMPFUN_HTTP_TIMEOUT_S", "15"))
SESSION_LIMIT  = int(os.getenv("PUMPFUN_HTTP_CONN_LIMIT", "30"))
API_LIMIT      = int(os.getenv("PUMPFUN_API_LIMIT", "20"))
DOM_LIMIT      = int(os.getenv("PUMPFUN_DOM_LIMIT", str(API_LIMIT)))
SKIP_NEXT      = os.getenv("PUMPFUN_SKIP_NEXT", "0").strip().lower() in {"1","true","yes","y"}
SKIP_HTML_NEXT = os.getenv("PUMPFUN_SKIP_HTMLNEXT", "0").strip().lower() in {"1","true","yes","y"}

# Pump.fun HTML routes to try for __NEXT_DATA__ fallback
FALLBACK_HTML_URLS = [
    "https://pump.fun/",
    "https://pump.fun/?tab=trending",
    "https://pump.fun/?chain=sol&tab=trending",
]

# Typical CF/anti-bot and tunnel errors
_BAD_CF_CODES       = {403, 407, 429, 530}
_PROXY_BREAK_CODES  = {522, 523, 524, 525, 526, 530}

# Runner thresholds (env overrides → config defaults)
MIN_LIQ_USD = float(os.getenv("PUMP_MIN_LIQ_USD",        str(SCANNER_MIN_LIQ_USD)))
MIN_FDV_USD = float(os.getenv("PUMP_MIN_FDV_USD",        str(SCANNER_MIN_FDV_USD)))
MIN_BUYS_5M = int(  os.getenv("PUMP_MIN_BUYS_5M",       str(SCANNER_MIN_BUYS_5M)))
MIN_BUYS_15M= int(  os.getenv("PUMP_MIN_BUYS_15M",      str(SCANNER_MIN_BUYS_15M)))
MIN_VOL_5M_USD = float(os.getenv("PUMP_MIN_VOLUME_5M_USD",  str(SCANNER_MIN_VOLUME_5M_USD)))
MIN_VOL_15M_USD= float(os.getenv("PUMP_MIN_VOLUME_15M_USD", str(SCANNER_MIN_VOLUME_15M_USD)))
MIN_BUY_RATIO   = float(os.getenv("PUMP_MIN_BUY_RATIO", str(SCANNER_MIN_BUY_RATIO)))
REQUIRE_BUY_PRESSURE = os.getenv(
    "PUMP_REQUIRE_BUY_PRESSURE",
    "1" if SCANNER_REQUIRE_BUY_PRESSURE else "0",
).strip().lower() in {"1", "true", "yes", "y"}
MIN_CHG_5M = float(os.getenv("PUMP_MIN_PRICE_CHANGE_5M",  str(SCANNER_MIN_PRICE_CHANGE_5M)))
MIN_CHG_15M= float(os.getenv("PUMP_MIN_PRICE_CHANGE_15M", str(SCANNER_MIN_PRICE_CHANGE_15M)))
MAX_AGE_MIN = int(os.getenv("PUMP_MAX_AGE_MIN",           str(SCANNER_MAX_AGE_MIN)))
MAX_MC_USD  = float(os.getenv("PUMP_MAX_MC_USD",          str(SCANNER_MAX_MC_USD)))
MIN_HOLDERS = int(os.getenv("PUMP_MIN_HOLDERS",           str(SCANNER_MIN_HOLDERS)))

# ──────────────────────────────────────────────────────────────────────────────
# Helpers: API bases & headers
# ──────────────────────────────────────────────────────────────────────────────
def _api_bases() -> List[str]:
    bases: List[str] = []
    env_base = os.getenv("PUMPFUN_API_BASE", "").strip().rstrip("/")
    if env_base:
        bases.append(env_base)
    bases += [
        "https://frontend-api.pump-mirror.fun",
        "https://frontend-api.pump.fun",
    ]
    seen, out = set(), []
    for b in bases:
        if b and b not in seen:
            seen.add(b)
            out.append(b)
    return out

def _hdrs(url: str) -> Dict[str, str]:
    h = {
        "User-Agent": UA,
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://pump.fun/trending",
        "Origin": "https://pump.fun",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
    }
    ck = get_cookie_for(url)
    if ck:
        h["Cookie"] = ck
    return h

# ──────────────────────────────────────────────────────────────────────────────
# Utilities & normalization
# ──────────────────────────────────────────────────────────────────────────────
BASE58_RE = re.compile(r"[1-9A-HJ-NP-Za-km-z]{32,44}")

def _now() -> float:
    return time.time()

def _nz(x, d=0.0) -> float:
    try:
        fx = float(x)
        if fx != fx:  # NaN
            return float(d)
        return fx
    except Exception:
        return float(d)

def _safe_int(x: Any, d: int = 0) -> int:
    try:
        return int(x)
    except Exception:
        return d

def _age_min(created_ts: Optional[float]) -> Optional[float]:
    try:
        if created_ts is None:
            return None
        t = float(created_ts)
        if t > 10_000_000_000:  # ms → s
            t = t / 1000.0
        return max(0.0, (_now() - t) / 60.0)
    except Exception:
        return None

def _norm_mint(val: Any) -> str:
    if not isinstance(val, str):
        return ""
    val = val.strip().replace(" ", "")
    m = BASE58_RE.search(val)
    return m.group(0) if m else ""

def _extract_metrics_from_obj(obj: Dict[str, Any]) -> Dict[str, Any]:
    """
    Try to pull out all metrics we care about for runner detection.
    Uses best-effort heuristics and approximations when only 24h data is present.
    """
    liq_usd = None
    for k in ("liquidity", "liquidityUsd", "liqUsd", "lpUsd", "liquidity_usd"):
        v = obj.get(k)
        try:
            if v is not None:
                liq_usd = float(v)
                break
        except Exception:
            pass

    fdv = _nz(
        obj.get("fdv")
        or obj.get("fdvUsd")
        or obj.get("fullyDilutedValuation")
        or obj.get("fullyDilutedMcUsd")
        or obj.get("fullyDilutedMarketCap"),
        0.0,
    )
    mc = _nz(
        obj.get("marketCap")
        or obj.get("marketCapUsd")
        or obj.get("mcap_usd")
        or fdv,
        0.0,
    )

    # Buys / sells – many variants exist, favor shorter timeframes, fall back to 24h
    buys_5m  = _safe_int(
        obj.get("buys5m") or obj.get("buys_5m") or obj.get("m5Buys") or obj.get("buysLast5m")
    )
    sells_5m = _safe_int(
        obj.get("sells5m") or obj.get("sells_5m") or obj.get("m5Sells") or obj.get("sellsLast5m")
    )
    buys_15m  = _safe_int(
        obj.get("buys15m") or obj.get("buys_15m") or obj.get("m15Buys") or obj.get("buysLast15m")
    )
    sells_15m = _safe_int(
        obj.get("sells15m") or obj.get("sells_15m") or obj.get("m15Sells") or obj.get("sellsLast15m")
    )

    buys_24h  = _safe_int(obj.get("buys24h") or obj.get("buyCount24h") or obj.get("totalBuys24h"))
    sells_24h = _safe_int(obj.get("sells24h") or obj.get("sellCount24h") or obj.get("totalSells24h"))

    # Approximate intraday buys if only 24h data is present
    if buys_5m == 0 and buys_24h > 0:
        buys_5m = max(1, buys_24h // 288)  # 24h / 5m
    if buys_15m == 0 and buys_24h > 0:
        buys_15m = max(1, buys_24h // 96)

    if sells_5m == 0 and sells_24h > 0:
        sells_5m = max(0, sells_24h // 288)
    if sells_15m == 0 and sells_24h > 0:
        sells_15m = max(0, sells_24h // 96)

    vol5 = _nz(obj.get("volumeUsd5m") or obj.get("volume5mUsd") or obj.get("vol5mUsd"), 0.0)
    vol15 = _nz(obj.get("volumeUsd15m") or obj.get("volume15mUsd") or obj.get("vol15mUsd"), 0.0)
    vol24 = _nz(
        obj.get("volumeUsd24h")
        or obj.get("volume24hUsd")
        or obj.get("usdVolume24Hours")
        or obj.get("volume_24h_usd"),
        0.0,
    )

    # Approximate 5m / 15m volume from 24h if needed
    if vol5 <= 0 and vol24 > 0:
        vol5 = vol24 / 288.0
    if vol15 <= 0 and vol24 > 0:
        vol15 = vol24 / 96.0

    chg5 = _nz(obj.get("priceChange5m") or obj.get("priceChange_5m"), 0.0)
    chg15 = _nz(obj.get("priceChange15m") or obj.get("priceChange_15m"), 0.0)

    if chg5 == 0 and "priceChange24h" in obj:
        chg5 = _nz(obj.get("priceChange24h"), 0.0) / 288.0
    if chg15 == 0 and "priceChange24h" in obj:
        chg15 = _nz(obj.get("priceChange24h"), 0.0) / 96.0

    created = (
        obj.get("createdAt")
        or obj.get("createdAtTs")
        or obj.get("created")
        or obj.get("createdOn")
    )
    age_min = _age_min(created if created is not None else None)

    holders = _safe_int(
        obj.get("holders")
        or obj.get("holderCount")
        or obj.get("totalHolders")
        or obj.get("holdersCount")
    )

    price_usd = _nz(
        obj.get("priceUsd")
        or obj.get("usdPrice")
        or obj.get("price")
        or obj.get("tokenPriceUsd"),
        0.0,
    )

    # impact for 1k order (optional)
    impact_bps = None
    for k in ("priceImpact1kBps", "impact1kBps", "impact_bps", "impact_1k_bps"):
        v = obj.get(k)
        try:
            if v is not None:
                impact_bps = float(v)
                break
        except Exception:
            pass

    return {
        "liq_usd": _nz(liq_usd, 0.0),
        "fdv_usd": fdv,
        "mc_usd": mc,
        "buys_5m": buys_5m,
        "sells_5m": sells_5m,
        "buys_15m": buys_15m,
        "sells_15m": sells_15m,
        "volume_5m_usd": vol5,
        "volume_15m_usd": vol15,
        "volume_24h_usd": vol24,
        "price_change_5m": chg5,
        "price_change_15m": chg15,
        "age_min": age_min,
        "holders": holders,
        "price_usd": price_usd,
        "impact_1k_bps": _nz(impact_bps, 0.0),
    }

def _runner_buy_ratio(buys: int, sells: int) -> float:
    total = max(1, buys + sells)
    return float(buys) / float(total)

def _map_coin(obj: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Normalize various Pump.fun shapes → common dict with runner metrics.
    """
    try:
        raw_mint = (
            obj.get("mintAddress")
            or obj.get("mint")
            or obj.get("baseMint")
            or obj.get("tokenMint")
            or obj.get("id")
            or obj.get("publicKey")
        )
        mint = _norm_mint(raw_mint)
        if not mint:
            return None

        symbol = (
            obj.get("symbol")
            or obj.get("tokenSymbol")
            or obj.get("name")
            or mint[:6]
        )

        m = _extract_metrics_from_obj(obj)
        buy_ratio_5m = _runner_buy_ratio(m["buys_5m"], m["sells_5m"])
        buy_ratio_15m = _runner_buy_ratio(m["buys_15m"], m["sells_15m"])

        out: Dict[str, Any] = {
            "source": "pumpfun",
            "mint": mint,
            "symbol": symbol,
            "liq_usd": m["liq_usd"],
            "pool_age_min": m["age_min"],
            "impact_1k_bps": m["impact_1k_bps"],
            "price_usd": m["price_usd"],
            "fdv_usd": m["fdv_usd"],
            "mc_usd": m["mc_usd"],
            "volume_5m_usd": m["volume_5m_usd"],
            "volume_15m_usd": m["volume_15m_usd"],
            "volume_24h_usd": m["volume_24h_usd"],
            "buys_5m": m["buys_5m"],
            "sells_5m": m["sells_5m"],
            "buys_15m": m["buys_15m"],
            "sells_15m": m["sells_15m"],
            "buy_ratio_5m": buy_ratio_5m,
            "buy_ratio_15m": buy_ratio_15m,
            "price_change_5m": m["price_change_5m"],
            "price_change_15m": m["price_change_15m"],
            "holders": m["holders"],
        }

        # Optional 24h volume for dashboards
        if m["volume_24h_usd"] > 0:
            out["volumeUsd24h"] = m["volume_24h_usd"]

        return out
    except Exception:
        return None

def _extract_items_from_payload(payload: Any) -> List[Dict[str, Any]]:
    items: List[Dict[str, Any]] = []

    if isinstance(payload, list):
        src = payload
    elif isinstance(payload, dict):
        src: List[Dict[str, Any]] | None = None
        for k in ("trending", "coins", "items", "projects", "data"):
            v = payload.get(k)
            if isinstance(v, list):
                src = v
                break
        if src is None and isinstance(payload.get("data"), dict):
            dd = payload["data"]
            for kk in ("trending", "coins", "items", "projects", "data"):
                v = dd.get(kk)
                if isinstance(v, list):
                    src = v
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

# ──────────────────────────────────────────────────────────────────────────────
# HTML __NEXT_DATA__ extraction (no Playwright required)
# ──────────────────────────────────────────────────────────────────────────────
_NEXT_RE = re.compile(
    r'<script[^>]+id=["\']__NEXT_DATA__["\'][^>]*>\s*({.*?})\s*</script>',
    re.DOTALL | re.IGNORECASE,
)

def _html_next_data(html: str):
    try:
        m = _NEXT_RE.search(html or "")
        if not m:
            return None
        return json.loads(m.group(1))
    except Exception:
        return None

# ──────────────────────────────────────────────────────────────────────────────
# Runner filters & scoring
# ──────────────────────────────────────────────────────────────────────────────
def _passes_runner_filters(c: Dict[str, Any]) -> bool:
    if c.get("liq_usd", 0.0) < MIN_LIQ_USD:
        return False

    fdv = c.get("fdv_usd", 0.0)
    if fdv and fdv < MIN_FDV_USD:
        return False

    mc = c.get("mc_usd", 0.0)
    if MAX_MC_USD > 0 and mc and mc > MAX_MC_USD:
        return False

    age = c.get("pool_age_min")
    if age is not None and age > MAX_AGE_MIN:
        return False

    holders = c.get("holders") or 0
    if MIN_HOLDERS > 0 and holders and holders < MIN_HOLDERS:
        return False

    buys5 = int(c.get("buys_5m") or 0)
    buys15 = int(c.get("buys_15m") or 0)
    if buys5 < MIN_BUYS_5M:
        return False
    if buys15 < MIN_BUYS_15M:
        return False

    vol5 = float(c.get("volume_5m_usd") or 0.0)
    vol15 = float(c.get("volume_15m_usd") or 0.0)
    if vol5 < MIN_VOL_5M_USD:
        return False
    if vol15 < MIN_VOL_15M_USD:
        return False

    chg5 = float(c.get("price_change_5m") or 0.0)
    chg15 = float(c.get("price_change_15m") or 0.0)
    if chg5 < MIN_CHG_5M:
        return False
    if chg15 < MIN_CHG_15M:
        return False

    if REQUIRE_BUY_PRESSURE:
        if float(c.get("buy_ratio_5m") or 0.0) < MIN_BUY_RATIO:
            return False

    return True

def _runner_score(c: Dict[str, Any]) -> float:
    """
    Heuristic score:
    - strong buy pressure
    - recent price appreciation
    - solid 5m volume
    - enough liquidity
    """
    buy_ratio = float(c.get("buy_ratio_5m") or 0.0)
    chg5 = float(c.get("price_change_5m") or 0.0)
    chg15 = float(c.get("price_change_15m") or 0.0)
    vol5 = float(c.get("volume_5m_usd") or 0.0)
    liq = float(c.get("liq_usd") or 0.0)

    buy_component = buy_ratio * 3.0
    price_component = max(0.0, chg5) / 10.0 + max(0.0, chg15) / 15.0
    vol_component = min(vol5, 50_000.0) / 10_000.0
    liq_component = min(liq, 200_000.0) / 50_000.0

    return buy_component + price_component + vol_component + liq_component

def _filter_and_rank(candidates: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for c in candidates:
        if not _passes_runner_filters(c):
            continue
        c = dict(c)
        c["runner_score"] = _runner_score(c)
        out.append(c)

    out.sort(key=lambda x: x.get("runner_score", 0.0), reverse=True)
    return out[:API_LIMIT]

# ──────────────────────────────────────────────────────────────────────────────
# HTTP helpers
# ──────────────────────────────────────────────────────────────────────────────
async def _fetch_text(session: aiohttp.ClientSession, url: str) -> Tuple[str, int, Dict[str, str]]:
    headers = {
        "User-Agent": UA,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Referer": "https://pump.fun/",
        "Origin": "https://pump.fun",
        "Accept-Language": "en-US,en;q=0.9",
    }
    ck = get_cookie_for(url)
    if ck:
        headers["Cookie"] = ck

    try:
        async with session.get(
            url,
            headers=headers,
            ssl=ssl.create_default_context(cafile=certifi.where()),
            timeout=REQ_TIMEOUT_S,
            proxy=PUMPFUN_HTTP_PROXY or None,
        ) as r:
            text = await r.text()
            return text, r.status, dict(r.headers)
    except Exception:
        return "", 0, {}

async def _fetch_json(session: aiohttp.ClientSession, url: str) -> Tuple[Optional[Any], int, str]:
    """
    GET `url` expecting JSON. Returns (data_or_None, http_status, first_120_chars_of_text).
    Robust to bad content-types and falls back to manual json.loads(text).
    """
    headers = _hdrs(url)
    status = 0
    first = ""
    text = ""

    try:
        async with session.get(
            url,
            headers=headers,
            ssl=ssl.create_default_context(cafile=certifi.where()),
            timeout=REQ_TIMEOUT_S,
            proxy=PUMPFUN_HTTP_PROXY or None,
        ) as r:
            status = r.status
            try:
                text = await r.text()
                first = (text or "")[:120]
            except Exception:
                text = ""
                first = ""

            data: Optional[Any] = None
            if status == 200:
                try:
                    data = await r.json(content_type=None)
                except Exception:
                    try:
                        if text:
                            data = json.loads(text)
                    except Exception:
                        data = None
            return data, status, first
    except (
        aiohttp.ClientProxyConnectionError,
        aiohttp.ClientHttpProxyError,
        aiohttp.ClientConnectorError,
        aiohttp.ServerTimeoutError,
        asyncio.TimeoutError,
    ) as e:
        LOG.warning("Pump.fun request failed %s: %s", type(e).__name__, url)
        return None, status, first
    except Exception as e:
        LOG.warning("Pump.fun request unexpected error at %s: %s", url, e)
        return None, status, first

def _join_url(base: str, path: str) -> str:
    base = base.rstrip("/")
    path = path.lstrip("/")
    return f"{base}/{path}"

# ──────────────────────────────────────────────────────────────────────────────
# Fallback: HTML __NEXT_DATA__ from apex pages
# ──────────────────────────────────────────────────────────────────────────────
async def _html_next_fallback(session: aiohttp.ClientSession) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []

    for url in FALLBACK_HTML_URLS:
        try:
            html, status, headers = await _fetch_text(session, url)
        except Exception as e:
            LOG.warning("pumpfun HTML fallback fetch failed at %s: %s", url, e)
            continue

        ctype = (headers.get("content-type") or "").lower()
        low = (html or "").lower()
        blocked = (
            status in _BAD_CF_CODES
            or "cloudflare" in low
            or "cf-chl-" in low
            or "just a moment" in low
        )
        if not html or blocked:
            LOG.warning("pumpfun HTML fallback: blocked page at %s (status=%s)", url, status)
            continue

        payload = _html_next_data(html)
        if not payload:
            LOG.debug("pumpfun HTML fallback: __NEXT_DATA__ not found at %s (status=%s ctype=%s)", url, status, ctype)
            continue

        # Heuristic extraction from Next.js JSON
        items = _extract_items_from_payload(payload)
        if not items:
            try:
                props = (payload.get("props") or {}).get("pageProps") or {}
                for key in ("coins", "trending", "trendingCoins", "data", "items", "list", "feed"):
                    v = props.get(key)
                    if isinstance(v, list):
                        items = _extract_items_from_payload(v)
                        if items:
                            break
            except Exception:
                items = []

        if items:
            filtered = _filter_and_rank(items)
            out.extend(filtered)
            LOG.info(
                "scanner.pumpfun: HTML __NEXT_DATA__ → %s runner items from %s",
                len(filtered),
                url,
            )
            break  # first success is enough

    # De-dup by 'mint'
    if out:
        seen: set[str] = set()
        uniq: List[Dict[str, Any]] = []
        for d in out:
            m = d.get("mint")
            if isinstance(m, str) and m not in seen:
                seen.add(m)
                uniq.append(d)
        out = uniq

    return out

# ──────────────────────────────────────────────────────────────────────────────
# Core scanning logic
# ──────────────────────────────────────────────────────────────────────────────
async def _fetch_api_json_with_fallback(
    session: aiohttp.ClientSession,
    path: str,
    tried_out: Optional[List[str]] = None,
) -> Optional[List[Dict[str, Any]]]:
    # Debug override
    if OVERRIDE_URL and OVERRIDE_URL.lower().startswith("http"):
        payload, status, first = await _fetch_json(session, OVERRIDE_URL)
        if tried_out is not None:
            tried_out.append(f"{OVERRIDE_URL} status={status}")
        if payload:
            items = _extract_items_from_payload(payload)
            if items:
                LOG.info("scanner.pumpfun: API JSON (override) → %s items", len(items))
                return items

    for base in _api_bases():
        url = _join_url(base, path)
        payload, status, first = await _fetch_json(session, url)
        if tried_out is not None:
            tried_out.append(f"{url} status={status}")
        if not payload:
            if status in (404, *_BAD_CF_CODES, *_PROXY_BREAK_CODES):
                LOG.warning("Pump.fun: [%s] HTTP %s; first 120: %s", url, status, first)
            continue
        items = _extract_items_from_payload(payload)
        if items:
            LOG.info("scanner.pumpfun: API JSON → %s items (base=%s)", len(items), base)
            return items

    return None

async def _collect_pumpfun_items(session: aiohttp.ClientSession) -> List[Dict[str, Any]]:
    """
    Try multiple Pump.fun sources in order, returning a *list* of normalized items.
    Order:
      1) JSON API (trending)
      2) JSON API (new)
      3) legacy endpoint
      4) HTML __NEXT_DATA__ scrape (when CF blocks JSON)
      5) Playwright JSON fetch to first base
    """
    tried: List[str] = []

    # 1) API: trending
    items = await _fetch_api_json_with_fallback(
        session, f"coins/trending?offset=0&limit={API_LIMIT}", tried_out=tried
    )
    if items:
        return items

    # 2) API: new
    items = await _fetch_api_json_with_fallback(
        session, f"coins/new?offset=0&limit={API_LIMIT}", tried_out=tried
    )
    if items:
        return items

    # 3) legacy endpoint (often 404) — still try once
    legacy = "https://pump.fun/api/trending"
    payload, status, first = await _fetch_json(session, legacy)
    tried.append(f"{legacy} status={status}")
    if payload:
        items = _extract_items_from_payload(payload)
        if items:
            LOG.info("scanner.pumpfun: legacy API JSON → %s items", len(items))
            return items

    # 4) HTML __NEXT_DATA__ fallback (no Playwright)
    if not SKIP_HTML_NEXT:
        items = await _html_next_fallback(session)
        if items:
            return items

    # 5) last resort: Playwright fetch to *first* available base (capped by API_LIMIT)
    try:
        first_base = _api_bases()[0]
    except Exception:
        first_base = "https://frontend-api.pump.fun"
    fallback_url = _join_url(first_base, f"coins/trending?offset=0&limit={API_LIMIT}")

    payload = None
    try:
        maybe = browser_fetch_json(fallback_url)
        payload = await maybe if asyncio.iscoroutine(maybe) else maybe  # robust to sync/async
    except Exception as e:
        LOG.debug("browser_fetch_json final fallback failed: %s", e)

    if payload:
        items = _extract_items_from_payload(payload)
        if items:
            LOG.info("scanner.pumpfun: Playwright API → %s items", len(items))
            return items

    if tried:
        LOG.warning("scanner.pumpfun: no items after tries: %s", " | ".join(tried))

    return []

async def _scan_inner(session: aiohttp.ClientSession) -> List[Dict[str, Any]]:
    apply_cookies_to_session(session)

    # 0) Next.js (/_next/data) via Playwright
    if not SKIP_NEXT:
        try:
            raw = await browser_fetch_trending_via_next_data(limit=DOM_LIMIT)
            if isinstance(raw, list) and raw:
                items: List[Dict[str, Any]] = []
                for it in raw:
                    if isinstance(it, dict):
                        m = _map_coin(it)
                        if m:
                            items.append(m)
                filtered = _filter_and_rank(items)
                if filtered:
                    LOG.info("scanner.pumpfun: next-data apex → %s runner items", len(filtered))
                    return filtered
        except Exception as e:
            LOG.warning("Pump.fun next-data apex fetch error: %s", e)
    else:
        LOG.debug("PUMPFUN_SKIP_NEXT=1 → skipping next-data step")

    # 1) Apex DOM via Playwright
    if not SKIP_NEXT:
        try:
            dom = await browser_scrape_trending_dom(limit=DOM_LIMIT)
            if isinstance(dom, list) and dom:
                items: List[Dict[str, Any]] = []
                for it in dom:
                    if isinstance(it, dict):
                        m = _map_coin(it)
                        if m:
                            items.append(m)
                filtered = _filter_and_rank(items)
                if filtered:
                    LOG.info("scanner.pumpfun: DOM apex → %s runner items", len(filtered))
                    return filtered
        except Exception as e:
            LOG.warning("Pump.fun apex DOM scrape error: %s", e)
    else:
        LOG.debug("PUMPFUN_SKIP_NEXT=1 → skipping DOM scrape step")

    # 2) API + fallbacks
    items = await _collect_pumpfun_items(session)
    if not items:
        return []
    filtered = _filter_and_rank(items)
    LOG.info("scanner.pumpfun: API/fallback → %s runner items (from %s raw)",
             len(filtered), len(items))
    return filtered

# ──────────────────────────────────────────────────────────────────────────────
# Public entrypoint
# ──────────────────────────────────────────────────────────────────────────────
HEADERS = {"User-Agent": "pumpfun-scanner/1.0"}

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
            items = await _scan_inner(sess) or []
            LOG.info("scanner.pumpfun: %s final runner candidates", len(items))
            return items
        except Exception as e:
            LOG.warning("Pump.fun scan error (session): %s", e)
            return []

    # Build our own session (honors env proxies via trust_env=True)
    sslctx = ssl.create_default_context(cafile=certifi.where())
    timeout = aiohttp.ClientTimeout(total=REQ_TIMEOUT_S + 5)
    conn = aiohttp.TCPConnector(ssl=sslctx, limit=SESSION_LIMIT)

    async with aiohttp.ClientSession(
        headers=HEADERS,
        connector=conn,
        trust_env=True,
        timeout=timeout,
    ) as session:
        try:
            items = await _scan_inner(session) or []
            LOG.info("scanner.pumpfun: %s final runner candidates", len(items))
            return items
        except Exception as e:
            LOG.warning("Pump.fun scan error: %s", e)
            return []

__all__ = ["scan_pumpfun"]
