# scanner/dexscreener_scanner.py
# Pulls active Solana "runner" pairs from Dexscreener's public JSON API.
from __future__ import annotations

import os
import ssl
import asyncio
import logging
import contextlib
from typing import List, Dict, Any, Optional, Tuple

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

logger = logging.getLogger("scanner.dexscreener")

# ──────────────────────────────────────────────────────────────────────────────
# Config / env
# ──────────────────────────────────────────────────────────────────────────────

UA = os.getenv(
    "SCANNER_UA",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) SniperBot/1.0 (+dexscreener)",
)

BASE = os.getenv("DEXSCREENER_BASE_URL", "https://api.dexscreener.com").rstrip("/")
LIMIT = int(os.getenv("DEX_LIMIT", "40"))
TIMEOUT_S = float(os.getenv("DEX_HTTP_TIMEOUT_SEC", "12"))
RETRIES = max(1, int(os.getenv("DEX_HTTP_RETRIES", "3")))
BACKOFF_S = max(0.1, float(os.getenv("DEX_HTTP_BACKOFF_SEC", "0.6")))

# Comma-separated search queries; first should be "solana" to keep scope tight.
SEARCH_QUERIES = [
    q.strip()
    for q in os.getenv("DEX_SEARCH_QUERIES", "solana").split(",")
    if q.strip()
]

# Honor system proxy variables if present (aiohttp trust_env)
TRUST_ENV = os.getenv("DEX_TRUST_ENV", "1").strip().lower() in {"1", "true", "yes", "y"}

# Thresholds (default to config values, but allow scanner-specific overrides)
MIN_LIQ_USD = float(os.getenv("DEX_MIN_LIQ_USD", str(SCANNER_MIN_LIQ_USD)))
MIN_FDV_USD = float(os.getenv("DEX_MIN_FDV_USD", str(SCANNER_MIN_FDV_USD)))
MIN_BUYS_5M = int(os.getenv("DEX_MIN_BUYS_5M", str(SCANNER_MIN_BUYS_5M)))
MIN_BUYS_15M = int(os.getenv("DEX_MIN_BUYS_15M", str(SCANNER_MIN_BUYS_15M)))
MIN_VOL_5M_USD = float(
    os.getenv("DEX_MIN_VOLUME_5M_USD", str(SCANNER_MIN_VOLUME_5M_USD))
)
MIN_VOL_15M_USD = float(
    os.getenv("DEX_MIN_VOLUME_15M_USD", str(SCANNER_MIN_VOLUME_15M_USD))
)
MIN_BUY_RATIO = float(
    os.getenv("DEX_MIN_BUY_RATIO", str(SCANNER_MIN_BUY_RATIO))
)  # 0..1
REQUIRE_BUY_PRESSURE = os.getenv(
    "DEX_REQUIRE_BUY_PRESSURE", "1" if SCANNER_REQUIRE_BUY_PRESSURE else "0"
).strip().lower() in {"1", "true", "yes", "y"}

MIN_CHG_5M = float(
    os.getenv("DEX_MIN_PRICE_CHANGE_5M", str(SCANNER_MIN_PRICE_CHANGE_5M))
)
MIN_CHG_15M = float(
    os.getenv("DEX_MIN_PRICE_CHANGE_15M", str(SCANNER_MIN_PRICE_CHANGE_15M))
)

MAX_AGE_MIN = int(os.getenv("DEX_MAX_AGE_MIN", str(SCANNER_MAX_AGE_MIN)))
MAX_MC_USD = float(os.getenv("DEX_MAX_MC_USD", str(SCANNER_MAX_MC_USD)))
MIN_HOLDERS = int(os.getenv("DEX_MIN_HOLDERS", str(SCANNER_MIN_HOLDERS)))

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


def _safe_int(v: Any, d: int = 0) -> int:
    try:
        return int(v)
    except Exception:
        return d


def _age_min(pair_created_ms: Optional[int]) -> Optional[float]:
    """
    Convert Dexscreener's pairCreatedAt (ms epoch) into minutes age.
    Returns None if unknown.
    """
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
    GET JSON with retries & backoff.
    Returns (json_or_none, http_status, first_120_chars_of_text).
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
                            logger.warning(
                                "Dexscreener: non-JSON 200 at %s (first=%r)",
                                url,
                                body_first,
                            )
                            return None, status, body_first

                # 4xx other than 429 are unlikely to improve with retries
                if 400 <= r.status < 500 and r.status != 429:
                    logger.warning(
                        "Dexscreener: HTTP %s at %s (first=%r)",
                        r.status,
                        url,
                        body_first,
                    )
                    return None, status, body_first

                # 429 / 5xx → fall through to backoff
                logger.warning(
                    "Dexscreener: HTTP %s at %s (attempt %d/%d)",
                    r.status,
                    url,
                    attempt,
                    RETRIES,
                )
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            logger.warning(
                "Dexscreener: %s at %s (attempt %d/%d)",
                type(e).__name__,
                url,
                attempt,
                RETRIES,
            )
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
# Metrics / filtering / normalization
# ──────────────────────────────────────────────────────────────────────────────
def _extract_metrics(pair: Dict[str, Any]) -> Dict[str, Any]:
    """Pulls out all the numbers we care about for runner detection."""
    base = pair.get("baseToken") or {}
    quote = pair.get("quoteToken") or {}

    liq = _nz_float((pair.get("liquidity") or {}).get("usd"), 0.0)
    fdv = _nz_float(pair.get("fdv"), 0.0)
    mc = _nz_float(pair.get("marketCap"), fdv or 0.0)

    txns = pair.get("txns") or {}
    m5 = txns.get("m5") or {}
    m15 = txns.get("m15") or {}

    buys5 = _safe_int(m5.get("buys"))
    sells5 = _safe_int(m5.get("sells"))
    buys15 = _safe_int(m15.get("buys"))
    sells15 = _safe_int(m15.get("sells"))

    buy_total5 = max(1, buys5 + sells5)
    buy_total15 = max(1, buys15 + sells15)

    buy_ratio5 = buys5 / buy_total5
    buy_ratio15 = buys15 / buy_total15

    vol = pair.get("volume") or {}
    vol5 = _nz_float(vol.get("m5"), 0.0)
    vol15 = _nz_float(vol.get("m15"), 0.0)
    vol24 = _nz_float(vol.get("h24"), 0.0)

    chg = pair.get("priceChange") or {}
    chg5 = _nz_float(chg.get("m5"), 0.0)
    chg15 = _nz_float(chg.get("m15"), 0.0)

    age_ms = (
        pair.get("pairCreatedAt")
        or pair.get("pairCreatedAtMs")
        or pair.get("pairCreatedAtTs")
    )
    age_min = _age_min(age_ms)

    holders = _safe_int(pair.get("holders") or 0)

    price_usd = _nz_float(pair.get("priceUsd"), 0.0)

    return {
        "liq_usd": liq,
        "fdv_usd": fdv,
        "mc_usd": mc,
        "buys_5m": buys5,
        "sells_5m": sells5,
        "buys_15m": buys15,
        "sells_15m": sells15,
        "buy_ratio_5m": buy_ratio5,
        "buy_ratio_15m": buy_ratio15,
        "volume_5m_usd": vol5,
        "volume_15m_usd": vol15,
        "volume_24h_usd": vol24,
        "price_change_5m": chg5,
        "price_change_15m": chg15,
        "age_min": age_min,
        "holders": holders,
        "price_usd": price_usd,
        "base_symbol": (base.get("symbol") or "").strip(),
        "base_mint": (base.get("address") or "").strip(),
        "quote_symbol": (quote.get("symbol") or "").strip(),
    }


def _passes_filters(pair: Dict[str, Any], m: Dict[str, Any]) -> bool:
    """Apply all runner-focused filters using config thresholds."""
    if (pair.get("chainId") or "").lower() != "solana":
        return False

    # Liquidity / size
    if m["liq_usd"] < MIN_LIQ_USD:
        return False

    if m["fdv_usd"] and m["fdv_usd"] < MIN_FDV_USD:
        return False

    if MAX_MC_USD > 0 and m["mc_usd"] and m["mc_usd"] > MAX_MC_USD:
        return False

    # Age – keep reasonably fresh charts
    if m["age_min"] is not None and m["age_min"] > MAX_AGE_MIN:
        return False

    # Holders – avoid total ghosts when Dexscreener exposes this
    if MIN_HOLDERS > 0 and m["holders"] and m["holders"] < MIN_HOLDERS:
        return False

    # Activity – buys and volume in 5m/15m
    if m["buys_5m"] < MIN_BUYS_5M:
        return False
    if m["buys_15m"] < MIN_BUYS_15M:
        return False

    if m["volume_5m_usd"] < MIN_VOL_5M_USD:
        return False
    if m["volume_15m_usd"] < MIN_VOL_15M_USD:
        return False

    # Price action – we want actual runners, not flat lines
    if m["price_change_5m"] < MIN_CHG_5M:
        return False
    if m["price_change_15m"] < MIN_CHG_15M:
        return False

    # Buy pressure – more buys than sells over last 5m
    if REQUIRE_BUY_PRESSURE and m["buy_ratio_5m"] < MIN_BUY_RATIO:
        return False

    return True


def _runner_score(m: Dict[str, Any]) -> float:
    """
    Heuristic score for ranking:
    - prefer strong buy pressure
    - decent recent price appreciation
    - solid recent volume
    - enough liquidity
    """
    buy_component = m["buy_ratio_5m"] * 3.0
    price_component = max(0.0, m["price_change_5m"]) / 10.0 + max(
        0.0, m["price_change_15m"]
    ) / 15.0
    vol_component = min(m["volume_5m_usd"], 50_000.0) / 10_000.0
    liq_component = min(m["liq_usd"], 200_000.0) / 50_000.0

    return buy_component + price_component + vol_component + liq_component


def _normalize(pair: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    try:
        metrics = _extract_metrics(pair)
        if not _passes_filters(pair, metrics):
            return None

        mint = metrics["base_mint"]
        if not mint:
            return None

        symbol = metrics["base_symbol"] or mint[:6]
        url = (
            pair.get("url")
            or pair.get("pairUrl")
            or f"https://dexscreener.com/solana/{mint}"
        )

        out = {
            "source": "dexscreener",
            "dex": pair.get("dexId") or "unknown",
            "pair_address": pair.get("pairAddress"),
            "mint": mint,
            "symbol": symbol,
            "price_usd": metrics["price_usd"],
            "liq_usd": metrics["liq_usd"],
            "fdv_usd": metrics["fdv_usd"],
            "mc_usd": metrics["mc_usd"],
            "volume_5m_usd": metrics["volume_5m_usd"],
            "volume_15m_usd": metrics["volume_15m_usd"],
            "volume_24h_usd": metrics["volume_24h_usd"],
            "buys_5m": metrics["buys_5m"],
            "sells_5m": metrics["sells_5m"],
            "buys_15m": metrics["buys_15m"],
            "sells_15m": metrics["sells_15m"],
            "buy_ratio_5m": metrics["buy_ratio_5m"],
            "buy_ratio_15m": metrics["buy_ratio_15m"],
            "price_change_5m": metrics["price_change_5m"],
            "price_change_15m": metrics["price_change_15m"],
            "age_min": metrics["age_min"],
            "holders": metrics["holders"],
            "pair_url": url,
        }

        out["runner_score"] = _runner_score(metrics)
        return out
    except Exception as e:
        logger.debug("Dexscreener normalize error: %s", e)
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
        sess = aiohttp.ClientSession(
            headers=_headers(), connector=conn, trust_env=TRUST_ENV, timeout=timeout
        )
        own_session = True

    try:
        logger.info(
            "🔎 Dexscreener scan: queries=%s liq>=$%.0f buys5m>=%d buys15m>=%d "
            "vol5m>=$%.0f vol15m>=$%.0f buy_ratio>=%.2f chg5m>=%s%% chg15m>=%s%% age<=%dmin",
            ",".join(SEARCH_QUERIES),
            MIN_LIQ_USD,
            MIN_BUYS_5M,
            MIN_BUYS_15M,
            MIN_VOL_5M_USD,
            MIN_VOL_15M_USD,
            MIN_BUY_RATIO,
            MIN_CHG_5M,
            MIN_CHG_15M,
            MAX_AGE_MIN,
        )

        pairs = await _fetch_pairs(sess)
        logger.info("Dexscreener: fetched %d raw pairs", len(pairs))

        rows: List[Dict[str, Any]] = []
        for p in pairs:
            r = _normalize(p)
            if r:
                rows.append(r)

        logger.info("Dexscreener: %d pairs passed runner filters", len(rows))

        # Rank: highest runner_score first
        rows.sort(key=lambda x: x.get("runner_score", 0.0), reverse=True)
        return rows[:LIMIT]
    except Exception as e:
        logger.warning("Dexscreener scan error: %s", e)
        return []
    finally:
        if own_session and sess is not None:
            with contextlib.suppress(Exception):
                await sess.close()


__all__ = ["scan_dexscreener"]
