# scanner/pumpfun_scanner.py
# Pump.fun scanner focused on *live runners* via shared PF filters + MOMO alerts.
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
    PF_SCAN_MIN_LIQ_USD,
    PF_SCAN_MIN_MCAP_USD,
    PF_SCAN_MAX_TOKEN_AGE_MIN,
)

from utils_pkg.alert_manager import AlertManager
from utils_pkg.pair_filters import (
    is_viable_pair,
    compute_momentum_score,
    compute_final_score,
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
# Env helpers
# ──────────────────────────────────────────────────────────────────────────────

def _b(name: str, default: bool = False) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return str(v).strip().lower() in {"1", "true", "t", "yes", "y", "on"}


def _s(name: str, default: str = "") -> str:
    v = os.getenv(name)
    return default if v is None else v.strip()


def _i(name: str, default: int) -> int:
    v = os.getenv(name)
    try:
        return int(v) if v not in (None, "") else default
    except Exception:
        return default


def _f(name: str, default: float) -> float:
    v = os.getenv(name)
    try:
        return float(v) if v not in (None, "") else default
    except Exception:
        return default


# ──────────────────────────────────────────────────────────────────────────────
# Config / flags (Pump-specific on top of PF_* defaults)
# ──────────────────────────────────────────────────────────────────────────────

UA = _s(
    "PUMPFUN_UA",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126 Safari/537.36",
)

# Optional override *full URL* for debugging a single endpoint
OVERRIDE_URL = _s("PUMPFUN_OVERRIDE_URL", "")

# Optional HTTP proxy specifically for Pump.fun requests (aiohttp-compatible URI)
PUMPFUN_HTTP_PROXY = _s("PUMPFUN_HTTP_PROXY", "")

REQ_TIMEOUT_S = _i("PUMPFUN_HTTP_TIMEOUT_S", 15)
SESSION_LIMIT = _i("PUMPFUN_HTTP_CONN_LIMIT", 30)
API_LIMIT = _i("PUMPFUN_API_LIMIT", 20)
DOM_LIMIT = _i("PUMPFUN_DOM_LIMIT", API_LIMIT)
SKIP_NEXT = _b("PUMPFUN_SKIP_NEXT", False)
SKIP_HTML_NEXT = _b("PUMPFUN_SKIP_HTMLNEXT", False)

# MOMO / alert behaviour
SEND_MOMO_ALERTS = _b("PUMP_SEND_MOMO_ALERTS", True)
MAX_ALERTS_PER_SCAN = _i("PUMP_MAX_ALERTS_PER_SCAN", 3)

# Pump.fun HTML routes to try for __NEXT_DATA__ fallback
FALLBACK_HTML_URLS = [
    "https://pump.fun/",
    "https://pump.fun/?tab=trending",
    "https://pump.fun/?chain=sol&tab=trending",
]

# Typical CF/anti-bot and tunnel errors
_BAD_CF_CODES = {403, 407, 429, 530}
_PROXY_BREAK_CODES = {522, 523, 524, 525, 526, 530}

# Soft prefilters BEFORE full PF logic (we still use PF for final viability)
SOFT_MIN_LIQ_USD = _f("PUMP_SOFT_MIN_LIQ_USD", PF_SCAN_MIN_LIQ_USD * 0.4)
SOFT_MIN_MCAP_USD = _f("PUMP_SOFT_MIN_MCAP_USD", PF_SCAN_MIN_MCAP_USD * 0.4)
SOFT_MAX_AGE_MIN = _f("PUMP_SOFT_MAX_AGE_MIN", PF_SCAN_MAX_TOKEN_AGE_MIN * 1.2)


# ──────────────────────────────────────────────────────────────────────────────
# Helpers: API bases & headers
# ──────────────────────────────────────────────────────────────────────────────

def _api_bases() -> List[str]:
    bases: List[str] = []
    env_base = _s("PUMPFUN_API_BASE", "").rstrip("/")
    if env_base:
        bases.append(env_base)
    bases += [
        "https://frontend-api.pump-mirror.fun",
        "https://frontend-api.pump.fun",
    ]
    seen: set[str] = set()
    out: List[str] = []
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


def _nz(x: Any, d: float = 0.0) -> float:
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
    buys_5m = _safe_int(
        obj.get("buys5m") or obj.get("buys_5m") or obj.get("m5Buys") or obj.get("buysLast5m")
    )
    sells_5m = _safe_int(
        obj.get("sells5m") or obj.get("sells_5m") or obj.get("m5Sells") or obj.get("sellsLast5m")
    )
    buys_15m = _safe_int(
        obj.get("buys15m") or obj.get("buys_15m") or obj.get("m15Buys") or obj.get("buysLast15m")
    )
    sells_15m = _safe_int(
        obj.get("sells15m") or obj.get("sells_15m") or obj.get("m15Sells") or obj.get("sellsLast15m")
    )

    buys_24h = _safe_int(obj.get("buys24h") or obj.get("buyCount24h") or obj.get("totalBuys24h"))
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


def _buy_ratio(buys: int, sells: int) -> float:
    total = max(1, buys + sells)
    return float(buys) / float(total)


def _map_coin_to_meta(obj: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Normalize various Pump.fun shapes → common PF "meta" dict.
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

        # Soft prefilters: skip ultra-dead trash before PF logic
        liq_usd = m["liq_usd"]
        mc_usd = m["mc_usd"]
        age_min = m["age_min"]

        if liq_usd < SOFT_MIN_LIQ_USD:
            return None
        if mc_usd < SOFT_MIN_MCAP_USD:
            return None
        if age_min is not None and age_min > SOFT_MAX_AGE_MIN:
            return None

        buys_5m = m["buys_5m"]
        sells_5m = m["sells_5m"]
        buys_15m = m["buys_15m"]
        sells_15m = m["sells_15m"]

        buy_ratio_5m = _buy_ratio(buys_5m, sells_5m)
        buy_ratio_15m = _buy_ratio(buys_15m, sells_15m)

        # We don't have explicit 1h txns, approximate from 24h if available
        txns_5m = buys_5m + sells_5m
        txns_1h = txns_5m * 12  # crude but consistent with PF heuristics

        pair_url = f"https://pump.fun/coin/{mint}"

        meta: Dict[str, Any] = {
            # identity
            "source": "pumpfun",
            "dex": "pumpfun",
            "chain": "solana",
            "mint": mint,
            "symbol": symbol,
            "pair_url": pair_url,

            # core liquidity / size
            "liq_usd": m["liq_usd"],
            "mc_usd": m["mc_usd"],
            "fdv_usd": m["fdv_usd"],

            # time
            "pool_age_min": m["age_min"],

            # recent activity
            "volume_5m_usd": m["volume_5m_usd"],
            "volume_15m_usd": m["volume_15m_usd"],
            "volume_24h_usd": m["volume_24h_usd"],
            "buys_5m": buys_5m,
            "sells_5m": sells_5m,
            "buys_15m": buys_15m,
            "sells_15m": sells_15m,
            "buy_ratio_5m": buy_ratio_5m,
            "buy_ratio_15m": buy_ratio_15m,
            "txns_5m": txns_5m,
            "txns_1h": txns_1h,

            # price action
            "price_change_5m": m["price_change_5m"],
            "price_change_15m": m["price_change_15m"],
            "price_usd": m["price_usd"],

            # misc
            "impact_1k_bps": m["impact_1k_bps"],
            "holders": m["holders"],

            # whale + AI fields (filled by other modules later; defaults are neutral)
            "whale_buys_5m": 0,
            "whale_volume_5m_sol": 0.0,
            "rug_risk_score": None,
            "ai_notes": "",
        }

        return meta
    except Exception as e:
        LOG.debug("pumpfun normalize error: %s", e)
        return None


def _extract_items_from_payload(payload: Any) -> List[Dict[str, Any]]:
    """
    Take raw JSON (list/dict) and return normalized PF meta entries.
    """
    items: List[Dict[str, Any]] = []

    if isinstance(payload, list):
        src = payload
    elif isinstance(payload, dict):
        src: Optional[List[Dict[str, Any]]] = None
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
            meta = _map_coin_to_meta(it)
            if meta:
                items.append(meta)
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
# PF-based filtering & ranking
# ──────────────────────────────────────────────────────────────────────────────

def _pf_filter_and_rank(metas: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Take normalized meta entries, apply is_viable_pair and PF scoring, and
    return the top API_LIMIT candidates ranked by final_score.
    """
    out: List[Dict[str, Any]] = []
    for meta in metas:
        try:
            if not is_viable_pair(meta):
                continue

            momentum = compute_momentum_score(meta)
            final = compute_final_score(meta, momentum=momentum)

            m = dict(meta)
            m["momentum_score"] = momentum
            m["final_score"] = final
            out.append(m)
        except Exception as e:
            LOG.debug("pumpfun PF filter error for %s: %s", meta.get("mint"), e)

    out.sort(key=lambda x: x.get("final_score", 0.0), reverse=True)
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
            # flag cookie if definitely CF blocked
            if status in _BAD_CF_CODES:
                flag_cookie_bad()
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
            runners = _pf_filter_and_rank(items)
            out.extend(runners)
            LOG.info(
                "scanner.pumpfun: HTML __NEXT_DATA__ → %s PF runner items from %s",
                len(runners),
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
                LOG.info("scanner.pumpfun: API JSON (override) → %s meta items", len(items))
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
            LOG.info("scanner.pumpfun: API JSON → %s meta items (base=%s)", len(items), base)
            return items

    return None


async def _collect_pumpfun_items(session: aiohttp.ClientSession) -> List[Dict[str, Any]]:
    """
    Try multiple Pump.fun sources in order, returning a *list* of normalized meta items.
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
            LOG.info("scanner.pumpfun: legacy API JSON → %s meta items", len(items))
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
            LOG.info("scanner.pumpfun: Playwright API → %s meta items", len(items))
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
                metas: List[Dict[str, Any]] = []
                for it in raw:
                    if isinstance(it, dict):
                        meta = _map_coin_to_meta(it)
                        if meta:
                            metas.append(meta)
                runners = _pf_filter_and_rank(metas)
                if runners:
                    LOG.info("scanner.pumpfun: next-data apex → %s PF runners", len(runners))
                    return runners
        except Exception as e:
            LOG.warning("Pump.fun next-data apex fetch error: %s", e)
    else:
        LOG.debug("PUMPFUN_SKIP_NEXT=1 → skipping next-data step")

    # 1) Apex DOM via Playwright
    if not SKIP_NEXT:
        try:
            dom = await browser_scrape_trending_dom(limit=DOM_LIMIT)
            if isinstance(dom, list) and dom:
                metas: List[Dict[str, Any]] = []
                for it in dom:
                    if isinstance(it, dict):
                        meta = _map_coin_to_meta(it)
                        if meta:
                            metas.append(meta)
                runners = _pf_filter_and_rank(metas)
                if runners:
                    LOG.info("scanner.pumpfun: DOM apex → %s PF runners", len(runners))
                    return runners
        except Exception as e:
            LOG.warning("Pump.fun apex DOM scrape error: %s", e)
    else:
        LOG.debug("PUMPFUN_SKIP_NEXT=1 → skipping DOM scrape step")

    # 2) API + fallbacks
    metas = await _collect_pumpfun_items(session)
    if not metas:
        return []

    runners = _pf_filter_and_rank(metas)
    LOG.info(
        "scanner.pumpfun: API/fallback → %s PF runners (from %s meta items)",
        len(runners),
        len(metas),
    )
    return runners


# ──────────────────────────────────────────────────────────────────────────────
# MOMO event wiring
# ──────────────────────────────────────────────────────────────────────────────

async def _send_momo_for_runners(runners: List[Dict[str, Any]]) -> None:
    """
    Send MOMO_OK alerts for the top N runner candidates.
    Anti-spam: capped by MAX_ALERTS_PER_SCAN and further filtered in AlertManager.
    """
    if not SEND_MOMO_ALERTS or not runners:
        return

    try:
        am = AlertManager()
    except Exception as e:
        LOG.warning("Pump.fun: failed to init AlertManager for MOMO: %s", e)
        return

    max_n = max(0, min(MAX_ALERTS_PER_SCAN, len(runners)))
    for c in runners[:max_n]:
        try:
            age_min = c.get("pool_age_min")
            age_sec = age_min * 60.0 if isinstance(age_min, (int, float)) else None
            ctx: Dict[str, Any] = {
                "symbol": c.get("symbol") or c.get("mint"),
                "mint": c.get("mint"),
                "pool": c.get("pair_url"),
                "age_sec": age_sec,
                "liq_usd": c.get("liq_usd"),
                "vol_5m": c.get("volume_5m_usd"),
                "up_pct": c.get("price_change_5m"),
                "reason": (
                    f"pumpfun PF runner final={c.get('final_score', 0.0):.3f} "
                    f"momentum={c.get('momentum_score', 0.0):.3f}"
                ),
            }
            await am.send_momo_event("MOMO_OK", ctx)
        except Exception as e:
            LOG.warning("Pump.fun: MOMO send error for %s: %s", c.get("mint"), e)


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
    Returns a list of PF-scored runner candidates (each is a meta dict).
    """
    sess: Optional[aiohttp.ClientSession] = None
    if len(args) >= 2 and isinstance(args[1], aiohttp.ClientSession):
        sess = args[1]
    elif len(args) == 1 and isinstance(args[0], aiohttp.ClientSession):
        sess = args[0]

    # If a shared session is provided, just use it
    if sess is not None:
        try:
            runners = await _scan_inner(sess) or []
            await _send_momo_for_runners(runners)
            LOG.info("scanner.pumpfun: %s final PF runner candidates", len(runners))
            return runners
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
            runners = await _scan_inner(session) or []
            await _send_momo_for_runners(runners)
            LOG.info("scanner.pumpfun: %s final PF runner candidates", len(runners))
            return runners
        except Exception as e:
            LOG.warning("Pump.fun scan error: %s", e)
            return []


__all__ = ["scan_pumpfun"]
