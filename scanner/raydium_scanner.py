# scanner/raydium_scanner.py
# Finds Raydium pairs via Dexscreener (stable public JSON) — resilient edition.
from __future__ import annotations

import os
import ssl
import asyncio
import logging
import contextlib
from typing import List, Dict, Any, Optional, Tuple

import aiohttp
import certifi

logger = logging.getLogger("scanner.raydium")

# ──────────────────────────────────────────────────────────────────────────────
# Config / env
# ──────────────────────────────────────────────────────────────────────────────
UA = os.getenv(
    "SCANNER_UA",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) SniperBot/1.0 (+raydium)"
)
BASE = os.getenv("DEXSCREENER_BASE_URL", "https://api.dexscreener.com").rstrip("/")

LIMIT = int(os.getenv("RAYDIUM_LIMIT", "40"))
MIN_LIQ_USD = float(os.getenv("RAYDIUM_MIN_LIQ_USD", "10000"))
TIMEOUT_S = float(os.getenv("RAYDIUM_HTTP_TIMEOUT_SEC", "12"))
RETRIES = max(1, int(os.getenv("RAYDIUM_HTTP_RETRIES", "3")))
BACKOFF_S = max(0.1, float(os.getenv("RAYDIUM_HTTP_BACKOFF_SEC", "0.6")))
TRUST_ENV = (os.getenv("RAYDIUM_TRUST_ENV", "1").strip().lower() in {"1", "true", "yes", "y"})

# You can override the search queries; all results are filtered to dexId=raydium anyway.
SEARCH_QUERIES = [q.strip() for q in os.getenv("RAYDIUM_SEARCH_QUERIES", "raydium,solana").split(",") if q.strip()]

# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────
def _headers() -> Dict[str, str]:
    return {
        "User-Agent": UA,
        "Accept": "application/json",
        "Accept-Language": "en-US,en;q=0.9",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
    }

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
        import time as _time
        diff_s = max(0.0, (_time.time() - (t_ms / 1000.0)))
        return diff_s / 60.0
    except Exception:
        return None

# ──────────────────────────────────────────────────────────────────────────────
# HTTP
# ──────────────────────────────────────────────────────────────────────────────
async def _fetch_json(
    session: aiohttp.ClientSession,
    url: str,
    *,
    params: Optional[Dict[str, Any]] = None,
) -> Tuple[Optional[Dict[str, Any]], int, str]:
    status = 0
    first = ""
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
                first = text[:120]
                if r.status == 200:
                    try:
                        return await r.json(content_type=None), status, first
                    except Exception:
                        import json
                        try:
                            return json.loads(text), status, first
                        except Exception:
                            logger.warning("Raydium: non-JSON 200 at %s (first=%r)", url, first)
                            return None, status, first

                if 400 <= r.status < 500 and r.status != 429:
                    logger.warning("Raydium: HTTP %s at %s (first=%r)", r.status, url, first)
                    return None, status, first

                logger.warning("Raydium: HTTP %s at %s (attempt %d/%d)", r.status, url, attempt, RETRIES)
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            logger.warning("Raydium: %s at %s (attempt %d/%d)", type(e).__name__, url, attempt, RETRIES)
        except Exception as e:
            logger.warning("Raydium: unexpected error at %s: %s", url, e)

        if attempt < RETRIES:
            await asyncio.sleep(BACKOFF_S * attempt)

    return None, status, first

async def _fetch_pairs(session: aiohttp.ClientSession) -> List[Dict[str, Any]]:
    url = f"{BASE}/latest/dex/search"
    out: List[Dict[str, Any]] = []
    seen_ids: set = set()

    for q in SEARCH_QUERIES:
        data, _status, _first = await _fetch_json(session, url, params={"q": q})
        if not data:
            continue
        pairs = data.get("pairs") or []
        for p in pairs:
            pid = p.get("pairAddress") or ((p.get("baseToken") or {}).get("address"))
            if pid and pid not in seen_ids:
                seen_ids.add(pid)
                out.append(p)
    return out

# ──────────────────────────────────────────────────────────────────────────────
# Normalize
# ──────────────────────────────────────────────────────────────────────────────
def _normalize(pair: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    try:
        if (pair.get("chainId") or "").lower() != "solana":
            return None
        if (pair.get("dexId") or "").lower() != "raydium":
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
            "source": "raydium(dex)",
            "mint": mint,
            "symbol": symbol,
            "liq_usd": liq,
            "price_usd": price_usd,
            "skew5m": float(skew5m),
            "pool_age_min": pool_age_min,
            "pair_url": url,
        }

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
async def scan_raydium(*args):
    """
    Flexible signature:
      - scan_raydium()
      - scan_raydium(redis)
      - scan_raydium(redis, session)
      - scan_raydium(session)

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

    own_session = False
    if sess is None:
        sslctx = ssl.create_default_context(cafile=certifi.where())
        timeout = aiohttp.ClientTimeout(total=TIMEOUT_S + 3)
        conn = aiohttp.TCPConnector(ssl=sslctx, limit=50)
        sess = aiohttp.ClientSession(headers=_headers(), connector=conn, trust_env=TRUST_ENV, timeout=timeout)
        own_session = True

    try:
        logger.info("🔎 Raydium scan via Dexscreener: queries=%s min_liq=$%.0f limit=%d",
                    ",".join(SEARCH_QUERIES), MIN_LIQ_USD, LIMIT)
        pairs = await _fetch_pairs(sess)
        rows: List[Dict[str, Any]] = []
        for p in pairs:
            r = _normalize(p)
            if r:
                rows.append(r)
        # Sort by liquidity then 5m skew
        rows.sort(key=lambda x: (x.get("liq_usd", 0.0), x.get("skew5m", 1.0)), reverse=True)
        return rows[:LIMIT]
    except Exception as e:
        logger.warning("Raydium scan error: %s", e)
        return []
    finally:
        if own_session:
            with contextlib.suppress(Exception):
                await sess.close()

__all__ = ["scan_raydium"]
