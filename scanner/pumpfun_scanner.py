# Write an updated pumpfun_scanner.py with stronger fallbacks (advanced/scan),
# tolerant endpoints (/coins/* and root), and more browser-like headers.
from textwrap import dedent
from pathlib import Path

code = dedent("""\
    import os
    import re
    import ssl
    import json
    import time
    import certifi
    import logging
    from typing import Any, Dict, List, Optional, Tuple

    import aiohttp

    # Scanner: Pump.fun
    # - Signature-flexible: scan_pumpfun(), scan_pumpfun(redis), scan_pumpfun(redis, session)
    # - Survives API changes (tries multiple endpoints + HTML/Next.js fallbacks)
    # - Works with/without Cloudflare cookies; add PUMPFUN_COOKIE to improve odds
    #
    # Environment knobs:
    #   PUMPFUN_UA      -> override User-Agent
    #   PUMPFUN_COOKIE  -> paste cookie string (cf_clearance=...; __cf_bm=...)
    #   PUMPFUN_URL     -> force a specific JSON endpoint
    #
    # Returns: List[Dict] with normalized keys:
    #   source='pumpfun', mint, symbol, liq_usd, pool_age_min, impact_1k_bps
    #
    LOG = logging.getLogger("scanner.pumpfun")

    DEF_UA = (
        os.getenv("PUMPFUN_UA")
        or "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126 Safari/537.36"
    )
    RAW_COOKIE = (os.getenv("PUMPFUN_COOKIE") or "").strip()
    OVERRIDE_URL = (os.getenv("PUMPFUN_URL") or "").strip()

    # --- headers ----------------------------------------------------------------
    def _hdrs() -> Dict[str, str]:
        # Cloudflare sometimes inspects fetch metadata & client hints.
        return {
            "User-Agent": DEF_UA,
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "en-US,en;q=0.9",
            "Origin": "https://pump.fun",
            "Referer": "https://pump.fun/advanced/scan",
            # Fetch metadata (best-effort)
            "Sec-Fetch-Site": "same-site",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Dest": "empty",
            # Client hints (harmless if ignored)
            "Sec-Ch-Ua": '"Chromium";v="126", "Not)A;Brand";v="24", "Google Chrome";v="126"',
            "Sec-Ch-Ua-Mobile": "?0",
            "Sec-Ch-Ua-Platform": '"Windows"',
            "Pragma": "no-cache",
            "Cache-Control": "no-cache",
        }

    def _attach_cookie(session: aiohttp.ClientSession) -> None:
        if not RAW_COOKIE:
            return
        try:
            pairs = {}
            for part in RAW_COOKIE.split(";"):
                if "=" in part:
                    k, v = part.split("=", 1)
                    pairs[k.strip()] = v.strip()
            if not pairs:
                return
            # Attach for both hosts we might hit.
            for host in ("https://pump.fun/", "https://frontend-api.pump.fun/"):
                session.cookie_jar.update_cookies(pairs, response_url=aiohttp.client_reqrep.URL(host))
        except Exception as e:
            LOG.warning("Pump.fun cookie attach failed: %s", e)

    # --- small utils -------------------------------------------------------------
    def _now() -> float: return time.time()

    def _age_min(created_ts: Optional[float]) -> Optional[float]:
        try:
            if created_ts is None:
                return None
            if created_ts > 10_000_000_000:  # ms -> s
                created_ts = created_ts / 1000.0
            return max(0.0, (_now() - float(created_ts)) / 60.0)
        except Exception:
            return None

    def _map_coin(obj: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        try:
            mint = obj.get("mintAddress") or obj.get("mint") or obj.get("baseMint") or obj.get("tokenMint")
            if not mint:
                return None
            symbol = obj.get("symbol") or obj.get("tokenSymbol") or (mint[:6])
            liq_usd = None
            for k in ("liquidity", "liquidityUsd", "liqUsd", "lpUsd", "liquidity_usd"):
                if k in obj and obj[k] is not None:
                    try:
                        liq_usd = float(obj[k]); break
                    except Exception:
                        pass
            impact_bps = None
            for k in ("priceImpact1kBps", "impact1kBps", "impact_bps", "impact_1k_bps"):
                if k in obj and obj[k] is not None:
                    try:
                        impact_bps = float(obj[k]); break
                    except Exception:
                        pass
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
                first = text[:120].replace("\\n", " ")
                # If html, bail early (Cloudflare error / landing page)
                if "text/html" in (r.headers.get("content-type") or "") or text.lstrip().startswith("<!DOCTYPE html"):
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
            LOG.warning("Pump.fun request failed: %s %s", url, e)
            return (None, 0, "")

    # --- HTML / Next.js fallbacks ------------------------------------------------
    async def _fetch_html(session: aiohttp.ClientSession, path: str) -> Tuple[str, int]:
        url = f"https://pump.fun/{path}"
        try:
            async with session.get(url, headers=_hdrs(), timeout=aiohttp.ClientTimeout(total=15)) as r:
                return (await r.text(), r.status)
        except Exception:
            return ("", 0)

    def _extract_from_next_data(data: Any) -> List[Dict[str, Any]]:
        try:
            page = data.get("pageProps", {}) if isinstance(data, dict) else {}
            candidates: List[Dict[str, Any]] = []

            # Common keys encountered
            for key in ("trending", "coins", "projects", "items", "data"):
                arr = page.get(key)
                if isinstance(arr, list) and arr:
                    for it in arr:
                        if isinstance(it, dict):
                            m = _map_coin(it)
                            if m:
                                candidates.append(m)
                    if candidates:
                        return candidates

            # Deep-ish walk
            def walk(x):
                if isinstance(x, dict):
                    for v in x.values(): walk(v)
                elif isinstance(x, list):
                    for v in x:
                        if isinstance(v, dict):
                            m = _map_coin(v)
                            if m: candidates.append(m)
                        else:
                            walk(v)
            walk(page)
            return candidates
        except Exception:
            return []

    async def _html_next_fallback(session: aiohttp.ClientSession) -> List[Dict[str, Any]]:
        # Try multiple pages that historically embed Next.js data
        for path in ("trending", "new", "advanced/scan"):
            html, status = await _fetch_html(session, path)
            if status >= 400 or not html:
                LOG.warning("Pump.fun HTML fallback: %s status=%s", path, status)
                continue

            # 1) Inline __NEXT_DATA__
            m = re.search(r'id="__NEXT_DATA__"[^>]*>\\s*({.*?})\\s*</script>', html, re.S)
            if m:
                try:
                    data = json.loads(m.group(1))
                    items = _extract_from_next_data(data)
                    if items:
                        return items
                except Exception:
                    pass

            # 2) Build ID -> /_next/data/<buildId>/<path>.json
            m2 = re.search(r"/_next/static/([^/]+)/_buildManifest\\.js", html)
            if not m2:
                continue
            build_id = m2.group(1)
            # Next.js routes drop slashes; map advanced/scan -> advanced/scan.json
            path_json = path + ".json" if not path.endswith(".json") else path
            data_url = f"https://pump.fun/_next/data/{build_id}/{path_json}"
            data, status2, _ = await _fetch_json(session, data_url)
            if data:
                items = _extract_from_next_data(data)
                if items:
                    return items

        return []

    # --- main scan pipeline ------------------------------------------------------
    async def _scan_inner(session: aiohttp.ClientSession) -> List[Dict[str, Any]]:
        _attach_cookie(session)

        tried: List[str] = []
        endpoints: List[str] = []

        if OVERRIDE_URL:
            endpoints.append(OVERRIDE_URL)

        # Current/legacy shapes we try in order. Both /coins/* and flat /* have been seen.
        endpoints += [
            # flat
            "https://frontend-api.pump.fun/trending?offset=0&limit=50",
            "https://frontend-api.pump.fun/new?offset=0&limit=50",
            # /coins
            "https://frontend-api.pump.fun/coins/trending?offset=0&limit=50",
            "https://frontend-api.pump.fun/coins/new?offset=0&limit=50",
            # legacy
            "https://pump.fun/api/trending",
        ]

        for url in endpoints:
            data, status, first = await _fetch_json(session, url)
            tried.append(f"{url} status={status}")
            if status in (404, 403, 429, 530) and not data:
                LOG.warning("scanner.pumpfun: %s HTTP %s observed; first 120 chars: %s", url, status, first)
                continue
            if not data:
                continue

            items: List[Dict[str, Any]] = []
            if isinstance(data, list):
                for it in data:
                    if isinstance(it, dict):
                        m = _map_coin(it)
                        if m: items.append(m)
            elif isinstance(data, dict):
                # Try common keys and nested data.* lists
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
                            if m: items.append(m)

            if items:
                return items

        # HTML/Next.js scraping fallback
        items = await _html_next_fallback(session)
        if items:
            return items

        LOG.warning("scanner.pumpfun: all endpoints failed. tried=%s", "; ".join(tried))
        return []

    async def scan_pumpfun(*args):
        \"\"\"Flexible entrypoint: scan_pumpfun(), scan_pumpfun(redis), or scan_pumpfun(redis, session).\"\"\"
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
                LOG.warning("scanner.pumpfun: error (session): %s", e)
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
                LOG.warning("scanner.pumpfun: error: %s", e)
                return []
    """)

Path("/mnt/data/pumpfun_scanner.py").write_text(code, encoding="utf-8")
print("Wrote /mnt/data/pumpfun_scanner.py")
