# scanner/raydium_scanner.py
# Raydium scanner via Dexscreener — runner-focused + MOMO alerts.
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

from utils_pkg.alert_manager import AlertManager

logger = logging.getLogger("scanner.raydium")

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


# ──────────────────────────────────────────────────────────────────────────────
# Config / env (Raydium overrides on top of global scanner defaults)
# ──────────────────────────────────────────────────────────────────────────────

UA = _s(
    "SCANNER_UA",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) SniperBot/1.0 (+raydium)",
)
BASE = _s("DEXSCREENER_BASE_URL", "https://api.dexscreener.com").rstrip("/")

LIMIT = int(os.getenv("RAYDIUM_LIMIT", "40"))

MIN_LIQ_USD = float(os.getenv("RAYDIUM_MIN_LIQ_USD",        str(SCANNER_MIN_LIQ_USD)))
MIN_FDV_USD = float(os.getenv("RAYDIUM_MIN_FDV_USD",        str(SCANNER_MIN_FDV_USD)))
MIN_BUYS_5M = int(  os.getenv("RAYDIUM_MIN_BUYS_5M",        str(SCANNER_MIN_BUYS_5M)))
MIN_BUYS_15M= int(  os.getenv("RAYDIUM_MIN_BUYS_15M",       str(SCANNER_MIN_BUYS_15M)))
MIN_VOL_5M_USD  = float(os.getenv("RAYDIUM_MIN_VOLUME_5M_USD",  str(SCANNER_MIN_VOLUME_5M_USD)))
MIN_VOL_15M_USD = float(os.getenv("RAYDIUM_MIN_VOLUME_15M_USD", str(SCANNER_MIN_VOLUME_15M_USD)))
MIN_BUY_RATIO   = float(os.getenv("RAYDIUM_MIN_BUY_RATIO",  str(SCANNER_MIN_BUY_RATIO)))
REQUIRE_BUY_PRESSURE = _b(
    "RAYDIUM_REQUIRE_BUY_PRESSURE",
    SCANNER_REQUIRE_BUY_PRESSURE,
)
MIN_CHG_5M  = float(os.getenv("RAYDIUM_MIN_PRICE_CHANGE_5M",  str(SCANNER_MIN_PRICE_CHANGE_5M)))
MIN_CHG_15M = float(os.getenv("RAYDIUM_MIN_PRICE_CHANGE_15M", str(SCANNER_MIN_PRICE_CHANGE_15M)))
MAX_AGE_MIN = int(os.getenv("RAYDIUM_MAX_AGE_MIN",            str(SCANNER_MAX_AGE_MIN)))
MAX_MC_USD  = float(os.getenv("RAYDIUM_MAX_MC_USD",           str(SCANNER_MAX_MC_USD)))
MIN_HOLDERS = int(os.getenv("RAYDIUM_MIN_HOLDERS",            str(SCANNER_MIN_HOLDERS)))

TIMEOUT_S = float(os.getenv("RAYDIUM_HTTP_TIMEOUT_SEC", "12"))
RETRIES = max(1, int(os.getenv("RAYDIUM_HTTP_RETRIES", "3")))
BACKOFF_S = max(0.1, float(os.getenv("RAYDIUM_HTTP_BACKOFF_SEC", "0.6")))
TRUST_ENV = _b("RAYDIUM_TRUST_ENV", True)

# MOMO / alert behaviour
SEND_MOMO_ALERTS = _b("RAYDIUM_SEND_MOMO_ALERTS", True)
MAX_ALERTS_PER_SCAN = int(os.getenv("RAYDIUM_MAX_ALERTS_PER_SCAN", "3"))

# You can override the search queries; all results are filtered to dexId=raydium anyway.
SEARCH_QUERIES = [
    q.strip()
    for q in os.getenv("RAYDIUM_SEARCH_QUERIES", "raydium,solana").split(",")
    if q.strip()
]

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


def _safe_int(v: Any, d: int = 0) -> int:
    try:
        return int(v)
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


def _runner_buy_ratio(buys: int, sells: int) -> float:
    total = max(1, buys + sells)
    return float(buys) / float(total)


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
                        import json as _json
                        try:
                            return _json.loads(text), status, first
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
# Metric extraction & normalization
# ──────────────────────────────────────────────────────────────────────────────

def _extract_metrics(pair: Dict[str, Any]) -> Dict[str, Any]:
    liq_usd = _nz_float((pair.get("liquidity") or {}).get("usd"), 0.0)

    fdv = _nz_float(
        pair.get("fdv")
        or pair.get("fdvUsd")
        or pair.get("fullyDilutedValuation"),
        0.0,
    )
    mc  = _nz_float(
        pair.get("marketCap")
        or pair.get("marketCapUsd")
        or pair.get("mcap")
        or fdv,
        0.0,
    )

    txs = (pair.get("txns") or {})
    m5 = txs.get("m5") or {}
    h1 = txs.get("h1") or {}
    h24 = txs.get("h24") or {}

    buys_5m  = _safe_int(m5.get("buys"), 0)
    sells_5m = _safe_int(m5.get("sells"), 0)

    buys_1h  = _safe_int(h1.get("buys"), 0)
    sells_1h = _safe_int(h1.get("sells"), 0)

    buys_24h  = _safe_int(h24.get("buys"), 0)
    sells_24h = _safe_int(h24.get("sells"), 0)

    # Approximate 15m from 1h/24h when missing
    if buys_1h > 0:
        buys_15m = max(1, buys_1h // 4)
        sells_15m = max(0, sells_1h // 4)
    elif buys_24h > 0:
        buys_15m = max(1, buys_24h // 96)
        sells_15m = max(0, sells_24h // 96)
    else:
        buys_15m = 0
        sells_15m = 0

    # Volume: Dexscreener mainly has h24
    vol = pair.get("volume") or {}
    vol24 = _nz_float(vol.get("h24"), 0.0)
    vol5 = vol24 / 288.0 if vol24 > 0 else 0.0
    vol15 = vol24 / 96.0 if vol24 > 0 else 0.0

    # Price change – Dexscreener usually has h1,h6,h24
    pc = pair.get("priceChange") or {}
    chg1h = _nz_float(pc.get("h1"), 0.0)
    chg6h = _nz_float(pc.get("h6"), 0.0)
    chg24h = _nz_float(pc.get("h24"), 0.0)

    # Rough approximations for 5m / 15m
    chg5 = chg1h / 12.0 if chg1h else chg24h / 288.0
    chg15 = chg1h / 4.0 if chg1h else chg24h / 96.0

    price_usd = _nz_float(pair.get("priceUsd"), 0.0)

    pair_created_ms = (
        pair.get("pairCreatedAt")
        or pair.get("pairCreatedAtMs")
        or pair.get("pairCreatedAtTs")
    )
    age_min = _age_min(pair_created_ms)

    # Holders not exposed by Dexscreener; keep 0 (filters only apply when >0)
    holders = 0

    return {
        "liq_usd": liq_usd,
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
        "price_usd": price_usd,
        "age_min": age_min,
        "holders": holders,
    }


def _normalize(pair: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    try:
        if (pair.get("chainId") or "").lower() != "solana":
            return None
        if (pair.get("dexId") or "").lower() != "raydium":
            return None

        metrics = _extract_metrics(pair)

        if metrics["liq_usd"] < MIN_LIQ_USD:
            return None

        base = pair.get("baseToken") or {}
        mint = (base.get("address") or "").strip()
        if not mint:
            return None

        symbol = (base.get("symbol") or "").strip() or mint[:6]

        buys_5m = metrics["buys_5m"]
        sells_5m = metrics["sells_5m"]
        buys_15m = metrics["buys_15m"]
        sells_15m = metrics["sells_15m"]

        buy_ratio_5m = _runner_buy_ratio(buys_5m, sells_5m)
        buy_ratio_15m = _runner_buy_ratio(buys_15m, sells_15m)

        url = pair.get("url") or pair.get("pairUrl") or f"https://dexscreener.com/solana/{mint}"

        out: Dict[str, Any] = {
            "source": "raydium",
            "mint": mint,
            "symbol": symbol,
            "pair_url": url,
            "liq_usd": metrics["liq_usd"],
            "price_usd": metrics["price_usd"],
            "pool_age_min": metrics["age_min"],
            "fdv_usd": metrics["fdv_usd"],
            "mc_usd": metrics["mc_usd"],
            "volume_5m_usd": metrics["volume_5m_usd"],
            "volume_15m_usd": metrics["volume_15m_usd"],
            "volume_24h_usd": metrics["volume_24h_usd"],
            "buys_5m": buys_5m,
            "sells_5m": sells_5m,
            "buys_15m": buys_15m,
            "sells_15m": sells_15m,
            "buy_ratio_5m": buy_ratio_5m,
            "buy_ratio_15m": buy_ratio_15m,
            "price_change_5m": metrics["price_change_5m"],
            "price_change_15m": metrics["price_change_15m"],
            "holders": metrics["holders"],
        }

        return out
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
    - strong recent buy pressure
    - positive 5m/15m price change
    - solid 5m volume
    - decent liquidity
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
    return out[:LIMIT]


# ──────────────────────────────────────────────────────────────────────────────
# MOMO event wiring
# ──────────────────────────────────────────────────────────────────────────────

async def _send_momo_for_runners(runners: List[Dict[str, Any]]) -> None:
    """
    Send MOMO_OK alerts for the top N runner candidates.
    Anti-spam: capped by MAX_ALERTS_PER_SCAN and further filtered in AlertManager.
    """
    if not SEND_MOMO_ALERTS:
        return
    if not runners:
        return

    try:
        am = AlertManager()
    except Exception as e:
        logger.warning("Raydium: failed to init AlertManager for MOMO: %s", e)
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
                "reason": f"raydium runner score={c.get('runner_score', 0.0):.2f}",
            }
            await am.send_momo_event("MOMO_OK", ctx)
        except Exception as e:
            logger.warning("Raydium: MOMO send error for %s: %s", c.get("mint"), e)


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
    Returns a list of runner candidates.
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
        sess = aiohttp.ClientSession(
            headers=_headers(),
            connector=conn,
            trust_env=TRUST_ENV,
            timeout=timeout,
        )
        own_session = True

    try:
        logger.info(
            "🔎 Raydium scan via Dexscreener: queries=%s min_liq=$%.0f limit=%d",
            ",".join(SEARCH_QUERIES),
            MIN_LIQ_USD,
            LIMIT,
        )
        pairs = await _fetch_pairs(sess)
        normalized: List[Dict[str, Any]] = []
        for p in pairs:
            r = _normalize(p)
            if r:
                normalized.append(r)

        runners = _filter_and_rank(normalized)
        logger.info(
            "scanner.raydium: %s runner candidates (from %s normalized, %s raw pairs)",
            len(runners),
            len(normalized),
            len(pairs),
        )

        # MOMO alerts (non-blocking logic-wise, but awaited so errors get logged)
        await _send_momo_for_runners(runners)

        return runners
    except Exception as e:
        logger.warning("Raydium scan error: %s", e)
        return []
    finally:
        if own_session:
            with contextlib.suppress(Exception):
                await sess.close()

__all__ = ["scan_raydium"]
