# scanner/pumpfun_scanner.py — v12
# Apex-first (next-data → DOM → HTML) with API mirror + fallback and proxy/cookie awareness.
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

# ---------------------------------------------------------------------
# Cloudflare / Playwright helpers
# ---------------------------------------------------------------------
try:
    from utils_pkg.pumpfun_cf import (
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
        return os.getenv("PUMPFUN_COOKIE_API", "") or os.getenv("PUMPFUN_COOKIE", "")

    def flag_cookie_bad() -> None:  # type: ignore
        pass

    async def browser_fetch_json(_url: str):  # type: ignore
        return None

    async def browser_fetch_trending_via_next_data(*, limit: int = 50):  # type: ignore
        return None

    async def browser_scrape_trending_dom(*, limit: int = 50):  # type: ignore
        return None


# ---------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------
UA = os.getenv("PUMPFUN_UA") or (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/126 Safari/537.36"
)

# Optional override *full URL* (for testing a single endpoint)
OVERRIDE_URL = os.getenv("PUMPFUN_URL", "").strip()

# Optional: skip Next.js/DOM steps entirely (quiet buildId/log noise)
SKIP_NEXT = (os.getenv("PUMPFUN_SKIP_NEXT", "").strip().lower() in ("1", "true", "yes", "y"))

REQ_TIMEOUT_S = int(os.getenv("PUMPFUN_HTTP_TIMEOUT_S", "15"))
SESSION_LIMIT = int(os.getenv("PUMPFUN_HTTP_CONN_LIMIT", "30"))

# Cap batch sizes (API & DOM/Next)
API_LIMIT = int(os.getenv("PUMPFUN_API_LIMIT", "20"))
DOM_LIMIT = int(os.getenv("PUMPFUN_DOM_LIMIT", str(API_LIMIT)))

# HTML regex scrape paths (no JS)
_HTML_PATHS_ENV = os.getenv("PUMPFUN_HTML_PATHS", "/trending,/advanced/scan")
HTML_PATHS = [p.strip() for p in _HTML_PATHS_ENV.split(",") if p.strip()] or ["/trending", "/advanced/scan"]

# Preferred API bases: env override → mirror → primary
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


# ---------------------------------------------------------------------
# Header builder
# ---------------------------------------------------------------------
def _hdrs(url: str) -> Dict[str, str]:
    # Always look like requests were initiated from pump.fun (helps with CF)
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


# ---------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------
BASE58_RE = re.compile(r"[1-9A-HJ-NP-Za-km-z]{32,44}")

def _now() -> float:
    return time.time()

def _nz(x, d=0.0) -> float:
    try:
        fx = float(x)
        if fx != fx:  # NaN guard
            return float(d)
        return fx
    except Exception:
        return float(d)

def _age_min(created_ts: Optional[float]) -> Optional[float]:
    try:
        if created_ts is None:
            return None
        if float(created_ts) > 10_000_000_000:  # ms → s
            created_ts = float(created_ts) / 1000.0
        return max(0.0, (_now() - float(created_ts)) / 60.0)
    except Exception:
        return None

def _norm_mint(val: Any) -> str:
    if not isinstance(val, str):
        return ""
    val = val.strip().replace(" ", "")
    m = BASE58_RE.search(val)
    return m.group(0) if m else ""


# Shapes → common fields
def _map_coin(obj: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    try:
        raw_mint = (
            obj.get("mintAddress")
            or obj.get("mint")
            or obj.get("baseMint")
            or obj.get("tokenMint")
            or obj.get("mint_id")
            or obj.get("id")
        )
        mint = _norm_mint(raw_mint)
        if not mint:
            return None

        symbol = obj.get("symbol") or obj.get("tokenSymbol") or obj.get("name") or mint[:6]

        liq_usd = None
        for k in ("liquidity", "liquidityUsd", "liqUsd", "lpUsd", "liquidity_usd"):
            v = obj.get(k)
            try:
                if v is not None:
                    liq_usd = float(v)
                    break
            except Exception:
                pass

        impact_bps = None
        for k in ("priceImpact1kBps", "impact1kBps", "impact_bps", "impact_1k_bps"):
            v = obj.get(k)
            try:
                if v is not None:
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

        # Provide numeric defaults where downstream scorers expect floats
        liq_usd_f = _nz(liq_usd, 0.0)
        impact_bps_f = _nz(impact_bps, 0.0)

        out = {
            "source": "pumpfun",
            "mint": mint,
            "symbol": symbol,
            "liq_usd": liq_usd_f,
            "pool_age_min": pool_age_min,
            "impact_1k_bps": impact_bps_f,
        }

        # Common merge helpers other scanners use
        if "volumeUsd24h" in obj and obj["volumeUsd24h"] is not None:
            try:
                out["volumeUsd24h"] = float(obj["volumeUsd24h"])
            except Exception:
                pass

        return out
    except Exception:
        return None

def _extract_items_from_payload(payload: Any) -> List[Dict[str, Any]]:
    items: List[Dict[str, Any]] = []

    if isinstance(payload, list):
        src = payload
    elif isinstance(payload, dict):
        src: List[Dict[str, Any]] | None = None
        # direct list containers
        for k in ("trending", "coins", "items", "projects", "data"):
            v = payload.get(k)
            if isinstance(v, list):
                src = v
                break
        # nested `data` object with list inside
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

# ---------------------------------------------------------------------
# HTTP helpers (JSON primary)
# ---------------------------------------------------------------------
_BAD_CF_CODES = {403, 407, 429, 530}

async def _fetch_json(session: aiohttp.ClientSession, url: str) -> Tuple[Optional[Any], int, str]:
    try:
            async with session.get(url, proxy=PROXY_URL, timeout=HTTP_TIMEOUT) as r:

            status = r.status
            text = await r.text()
            first = text[:120].replace("\n", " ")
            ctype = (r.headers.get("content-type") or "").lower()
	    PROXY_URL = os.getenv("https_proxy") or os.getenv("http_proxy") or os.getenv("all_proxy")
	    HTTP_TIMEOUT = aiohttp.ClientTimeout(total=float(os.getenv("HTTP_TIMEOUT", "12")))

            # CF/HTML gate
            if status in _BAD_CF_CODES or "text/html" in ctype or text.lstrip().startswith("<!DOCTYPE html"):
                if status in _BAD_CF_CODES:
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

def _join_url(base: str, path: str) -> str:
    base = base.rstrip("/")
    path = path.lstrip("/")
    return f"{base}/{path}"

async def _fetch_api_json_with_fallback(
    session: aiohttp.ClientSession,
    path: str,
    tried_out: Optional[List[str]] = None,
) -> Optional[List[Dict[str, Any]]]:
    # Override URL (debug single endpoint)
    if OVERRIDE_URL.lower().startswith("http"):
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
            if status in (404, *tuple(_BAD_CF_CODES)):
                LOG.warning("Pump.fun: [%s] HTTP %s; first 120: %s", url, status, first)
            continue
        items = _extract_items_from_payload(payload)
        if items:
            LOG.info("scanner.pumpfun: API JSON → %s items (base=%s)", len(items), base)
            return items

    return None


# ---------------------------------------------------------------------
# HTML helpers: scrape coin links directly from apex HTML (no JS)
# ---------------------------------------------------------------------
_BASE58 = r"[1-9A-HJ-NP-Za-km-z]{32,44}"

def _uniq(seq: List[str]) -> List[str]:
    seen = set()
    out: List[str] = []
    for x in seq:
        if x not in seen:
            seen.add(x)
            out.append(x)
    return out

async def _html_scrape_links(session: aiohttp.ClientSession, path: str, limit: int = 50) -> List[Dict[str, Any]]:
    url = f"https://pump.fun{path}"
    try:
        async with session.get(url, headers=_hdrs(url), timeout=aiohttp.ClientTimeout(total=REQ_TIMEOUT_S)) as r:
            html = await r.text()
    except Exception as e:
        LOG.warning("Pump.fun HTML link scrape failed (%s): %s", path, e)
        return []

    mints: List[str] = []
    for pattern in [
        rf'href="(?:https://pump\.fun)?/coin/({_BASE58})"',
        rf'href="(?:https://pump\.fun)?/coins/({_BASE58})"',
        rf'href="(?:https://pump\.fun)?/token/({_BASE58})"',
        rf'[?&]mint=({_BASE58})',
        rf'data-mint="({_BASE58})"',
    ]:
        mints += re.findall(pattern, html)

    mints = _uniq(mints)[:limit]
    return [{"mint": m} for m in mints]


# ---------------------------------------------------------------------
# HTML __NEXT_DATA__ fallback (legacy, may be absent)
# ---------------------------------------------------------------------
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


# ---------------------------------------------------------------------
# Scanner core
# ---------------------------------------------------------------------
async def _scan_inner(session: aiohttp.ClientSession) -> List[Dict[str, Any]]:
    apply_cookies_to_session(session)

    # 0) Apex Next.js data (/_next/data) via Playwright (cheap; may be absent)
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
                if items:
                    LOG.info("scanner.pumpfun: next-data apex → %s items", len(items))
                    return items
        except Exception as e:
            LOG.warning("Pump.fun next-data apex fetch error: %s", e)
    else:
        LOG.debug("PUMPFUN_SKIP_NEXT=1 → skipping next-data step")

    # 1) Apex DOM scrape via Playwright (no API host)
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
                if items:
                    LOG.info("scanner.pumpfun: DOM apex → %s items", len(items))
                    return items
        except Exception as e:
            LOG.warning("Pump.fun apex DOM scrape error: %s", e)
    else:
        LOG.debug("PUMPFUN_SKIP_NEXT=1 → skipping DOM scrape step")

    tried: List[str] = []

    # 2) Pure HTML (regex) from apex pages (no JS, no Playwright)
    html_items: List[Dict[str, Any]] = []
    for path in HTML_PATHS:
        try:
            links = await _html_scrape_links(session, path, limit=DOM_LIMIT)
            if links:
                html_items.extend(links)
        except Exception:
            pass
    if html_items:
        seen, uniq = set(), []
        for d in html_items:
            m = d.get("mint")
            if isinstance(m, str) and m not in seen:
                seen.add(m)
                uniq.append(d)
        items: List[Dict[str, Any]] = []
        for it in uniq:
            m = _map_coin(it)
            if m:
                items.append(m)
        if items:
            LOG.info("scanner.pumpfun: HTML apex → %s items", len(items))
            return items

    # 3) API JSON using mirror/primary fallback (capped by API_LIMIT)
    api_path_tr = f"/coins/trending?offset=0&limit={API_LIMIT}"
    api_path_new = f"/coins/new?offset=0&limit={API_LIMIT}"

    items = await _fetch_api_json_with_fallback(session, api_path_tr, tried_out=tried)
    if items:
        return items

    items = await _fetch_api_json_with_fallback(session, api_path_new, tried_out=tried)
    if items:
        return items

    # legacy endpoint (often 404) — still try once
    legacy = "https://pump.fun/api/trending"
    payload, status, first = await _fetch_json(session, legacy)
    tried.append(f"{legacy} status={status}")
    if payload:
        items = _extract_items_from_payload(payload)
        if items:
            LOG.info("scanner.pumpfun: legacy API JSON → %s items", len(items))
            return items

    # 4) HTML __NEXT_DATA__ fallback (legacy)
    if not SKIP_NEXT:
        items = await _html_next_data(session)
        if items:
            LOG.info("scanner.pumpfun: HTML __NEXT_DATA__ → %s items", len(items))
            return items

    # 5) Last resort: Playwright fetch to *first* available base (capped by API_LIMIT)
    try:
        first_base = _api_bases()[0]
    except Exception:
        first_base = "https://frontend-api.pump.fun"
    fallback_url = _join_url(first_base, f"/coins/trending?offset=0&limit={API_LIMIT}")
    payload = await browser_fetch_json(fallback_url)
    items = _extract_items_from_payload(payload)
    if items:
        LOG.info("scanner.pumpfun: Playwright API → %s items", len(items))
        return items

    LOG.warning("Pump.fun all endpoints failed: tried=%s", "; ".join(tried))
    return []


# ---------------------------------------------------------------------
# Public entrypoint
# ---------------------------------------------------------------------
# scanner/pumpfun_scanner.py  (public entry at the bottom)

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

    # Build our own session that honors proxies/certs/timeouts
    sslctx = ssl.create_default_context(cafile=certifi.where())
    timeout = aiohttp.ClientTimeout(total=REQ_TIMEOUT_S + 5)
    conn = aiohttp.TCPConnector(ssl=sslctx, limit=SESSION_LIMIT)

    async with aiohttp.ClientSession(
        headers=HEADERS,
        connector=conn,
        trust_env=True,         # pick up HTTP(S)_PROXY/ALL_PROXY from env
        timeout=timeout,
    ) as session:
        try:
            items = await _scan_inner(session)
            LOG.info("scanner.pumpfun: %s candidates", len(items))
            return items
        except Exception as e:
            LOG.warning("Pump.fun scan error: %s", e)
            return []

__all__ = ["scan_pumpfun"]