# scanner/dexscreener_scanner.py
# Pulls liquid Solana pairs from Dexscreener's public JSON API (resilient version).
from __future__ import annotations

import os
import ssl
import asyncio
import logging
from typing import List, Dict, Any, Optional, Tuple

import aiohttp
import certifi

logger = logging.getLogger("scanner.dexscreener")

# ──────────────────────────────────────────────────────────────────────────────
# Config / env
# ──────────────────────────────────────────────────────────────────────────────
UA = os.getenv(
    "SCANNER_UA",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) SniperBot/1.0 (+dexscreener)"
)
BASE = os.getenv("DEXSCREENER_BASE_URL", "https://api.dexscreener.com").rstrip("/")
MIN_LIQ_USD = float(os.getenv("DEX_MIN_LIQ_USD", "10000"))
LIMIT = int(os.getenv("DEX_LIMIT", "40"))
TIMEOUT_S = float(os.getenv("DEX_HTTP_TIMEOUT_SEC", "12"))
RETRIES = max(1, int(os.getenv("DEX_HTTP_RETRIES", "3")))
BACKOFF_S = max(0.1, float(os.getenv("DEX_HTTP_BACKOFF_SEC", "0.6")))
# Comma-separated search queries; first should be "solana" to keep scope tight.
SEARCH_QUERIES = [q.strip() for q in os.getenv("DEX_SEARCH_QUERIES", "solana").split(",") if q.strip()]

# Honor system proxy variables if present (aiohttp trust_env)
TRUST_ENV = (os.getenv("DEX_TRUST_ENV", "1").strip().lower() in {"1", "true", "yes", "y"})

# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────
def _nz_float(v: Any, d: float = 0.0) -> float:
    try:
        f = float(v)
        if f != f:  # NaN
            return d
        return f
    except Exception:
        return d

def _age_min(pair_created_ms: Optional[int]) -> Optional[float]:
    try:
        if pair_created_ms is None:
            return None
        t_ms = int(pair_created_ms)
        if t_ms <= 0:
            return None
        now_s = asyncio.get_running_loop().time()  # monotonic (seconds)
        # Convert monotonic to epoch-like diff by using wall time:
        import time as _time
        diff_s = max(0.0, (_time.time() - (t_ms / 1000.0)))
        return diff_s / 60.0
    except Exception:
        return None

def _headers() -> Dict[str, str]:
    return {
        "User-Agent": UA,
        "Accept": "application/json",
        "Accept-Language": "en-US,en;q=0.9",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
    }

# ──────────────────────────────────────────────────────────────────────────────
# HTTP
# ──────────────────────────────────────────────────────────────────────────────
async def _fetch_json(
    session: aiohttp.ClientSession,
    url: str,
    *,
    params: Optional[Dict[str, Any]] = None,
) -> Tuple[Optional[Dict[str, Any]], int, str]:
    """
    GET JSON with retries & backoff. Returns (json_or_none, http_status, first_120_chars_of_text).
    """
    status = 0
    body_first = ""
    for attempt in range(1, RETRIES + 1):
        try:
            async with session.get(
                url,
                params=params,
                headers=_headers(),
                timeout=aiohttp.ClientTimeout(total=TIMEOUT_S),
            ) as r:
                status = r.status
                text = await r.text()
                body_first = text[:120]
                if r.status == 200:
                    try:
                        data = await r.json(content_type=None)
                        return data, status, body_first
                    except Exception:
                        # Try manual parse to be extra tolerant
                        import json
                        try:
                            data = json.loads(text)
                            return data, status, body_first
                        except Exception:
                            logger.warning("Dexscreener: non-JSON 200 at %s (first=%r)", url, body_first)
                            return None, status, body_first

                # 4xx other than 429 are unlikely to improve with retries
                if 400 <= r.status < 500 and r.status != 429:
                    logger.warning("Dexscreener: HTTP %s at %s (first=%r)", r.status, url, body_first)
                    return None, status, body_first

                # 429 / 5xx → fall through to backoff
                logger.warning("Dexscreener: HTTP %s at %s (attempt %d/%d)", r.status, url, attempt, RETRIES)
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            logger.warning("Dexscreener: %s at %s (attempt %d/%d)", type(e).__name__, url, attempt, RETRIES)
        except Exception as e:
            logger.warning("Dexscreener: unexpected error at %s: %s", url, e)

        if attempt < RETRIES:
            # backoff with small jitter
            await asyncio.sleep(BACKOFF_S * attempt)

    return None, status, body_first

async def _fetch_pairs(session: aiohttp.ClientSession) -> List[Dict[str, Any]]:
    """
    Query Dexscreener search endpoint for each configured query and merge results.
    """
    url = f"{BASE}/latest/dex/search"
    all_pairs: List[Dict[str, Any]] = []
    seen_ids: set = set()

    for q in SEARCH_QUERIES:
        data, status, first = await _fetch_json(session, url, params={"q": q})
        if not data:
            continue
        pairs = data.get("pairs") or []
        for p in pairs:
            # Deduplicate by pairAddress if available, else by baseToken.address
            pid = p.get("pairAddress") or ((p.get("baseToken") or {}).get("address"))
            if pid and pid not in seen_ids:
                seen_ids.add(pid)
                all_pairs.append(p)
    return all_pairs

# ──────────────────────────────────────────────────────────────────────────────
# Normalize
# ──────────────────────────────────────────────────────────────────────────────
def _normalize(pair: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    try:
        if (pair.get("chainId") or "").lower() != "solana":
            return None

        liq = _nz_float((pair.get("liquidity") or {}).get("usd"), 0.0)
        if liq < MIN_LIQ_USD:
            return None

        base = pair.get("baseToken") or {}
        mint = (base.get("address") or "").strip()
        if not mint:
            return None

        symbol = (base.get("symbol") or "").strip() or mint[:6]
        price_usd = _nz_float(pair.get("priceUsd"), 0.0)

        tx5 = (pair.get("txns") or {}).get("m5") or {}
        buys = int(tx5.get("buys") or 0)
        sells = int(tx5.get("sells") or 0)
        skew5m = (buys + 1) / (sells + 1)

        pair_created_ms = pair.get("pairCreatedAt") or pair.get("pairCreatedAtMs") or pair.get("pairCreatedAtTs")
        pool_age_min = _age_min(pair_created_ms)

        url = pair.get("url") or pair.get("pairUrl") or f"https://dexscreener.com/solana/{mint}"

        out = {
            "source": "dexscreener",
            "mint": mint,
            "symbol": symbol,
            "liq_usd": liq,
            "price_usd": price_usd,
            "skew5m": float(skew5m),
            "pool_age_min": pool_age_min,
            "pair_url": url,
        }

        # Carry a few extra hints when present (safe to ignore downstream)
        fdv = _nz_float(pair.get("fdv"), 0.0)
        if fdv > 0:
            out["fdv_usd"] = fdv
        vol24 = _nz_float((pair.get("volume") or {}).get("h24"), 0.0)
        if vol24 > 0:
            out["volumeUsd24h"] = vol24

        return out
    except Exception:
        return None

# ──────────────────────────────────────────────────────────────────────────────
# Public entry
# ──────────────────────────────────────────────────────────────────────────────
async def scan_dexscreener(*args):
    """
    Flexible signature (compatible with other scanners):
      - scan_dexscreener()
      - scan_dexscreener(redis)
      - scan_dexscreener(redis, session)
      - scan_dexscreener(session)

    Only an aiohttp.ClientSession (if provided) is used.
    """
    sess: Optional[aiohttp.ClientSession] = None
    if len(args) >= 2 and isinstance(args[1], aiohttp.ClientSession):
        sess = args[1]
    elif len(args) == 1 and isinstance(args[0], aiohttp.ClientSession):
        sess = args[0]

    # Try borrowing a shared session from core utils (best-effort)
    if sess is None:
        try:
            from utils_pkg.core_utils import session as core_session  # type: ignore
            if isinstance(core_session, aiohttp.ClientSession):
                sess = core_session
        except Exception:
            sess = None

    # If still none, spin up a short-lived, TLS-verified session (trust_env for proxies)
    own_session = False
    if sess is None:
        sslctx = ssl.create_default_context(cafile=certifi.where())
        timeout = aiohttp.ClientTimeout(total=TIMEOUT_S + 3)
        conn = aiohttp.TCPConnector(ssl=sslctx, limit=50)
        sess = aiohttp.ClientSession(headers=_headers(), connector=conn, trust_env=TRUST_ENV, timeout=timeout)
        own_session = True

    try:
        logger.info("🔎 Dexscreener scan: queries=%s min_liq=$%.0f limit=%d", ",".join(SEARCH_QUERIES), MIN_LIQ_USD, LIMIT)
        pairs = await _fetch_pairs(sess)
        rows: List[Dict[str, Any]] = []
        for p in pairs:
            r = _normalize(p)
            if r:
                rows.append(r)

        # Rank: liquidity, then 5m buy/sell skew
        rows.sort(key=lambda x: (x.get("liq_usd", 0.0), x.get("skew5m", 1.0)), reverse=True)
        return rows[:LIMIT]
    except Exception as e:
        logger.warning("Dexscreener scan error: %s", e)
        return []
    finally:
        if own_session:
            with contextlib.suppress(Exception):
                await sess.close()

__all__ = ["scan_dexscreener"]
