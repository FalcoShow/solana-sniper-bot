# scanner/raydium_scanner.py
# Raydium scanner via Dexscreener — builds "meta" dicts compatible with
# utils_pkg.pair_filters and surfaces runner candidates for the sniper.
from __future__ import annotations

import os
import ssl
import asyncio
import logging
import contextlib
import time
from typing import List, Dict, Any, Optional, Tuple, Set

import aiohttp
import certifi

from config import (
    PF_SCAN_MIN_LIQ_USD,
    PF_SCAN_MIN_MCAP_USD,  # kept for future use / consistency
    PF_SCAN_MAX_TOKEN_AGE_MIN,
)

# Optional whale-size hint; safe fallback if not present
try:
    from config import PF_SCAN_MIN_WHALE_SIZE_SOL  # type: ignore
except Exception:  # pragma: no cover
    PF_SCAN_MIN_WHALE_SIZE_SOL = 3.0  # reasonable default

from utils_pkg.alert_manager import AlertManager
from utils_pkg.pair_filters import (
    is_viable_pair,
    compute_momentum_score,
    compute_final_score,
)

logger = logging.getLogger("scanner.raydium")

# -----------------------------------------------------------------------------
# Env helpers
# -----------------------------------------------------------------------------

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


# -----------------------------------------------------------------------------
# Config / env (Raydium-specific tuning on top of global PF_* thresholds)
# -----------------------------------------------------------------------------

UA = _s(
    "SCANNER_UA",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) SniperBot/1.0 (+raydium)",
)
BASE = _s("DEXSCREENER_BASE_URL", "https://api.dexscreener.com").rstrip("/")

# Dexscreener search limit per query (higher = more raw pairs to evaluate)
SEARCH_LIMIT = max(50, _i("RAYDIUM_SEARCH_LIMIT", 250))

# How many top candidates we keep per scan (after PF filtering + scoring/fallback)
LIMIT = _i("RAYDIUM_LIMIT", 40)

TIMEOUT_S = _f("RAYDIUM_HTTP_TIMEOUT_SEC", 12.0)
RETRIES = max(1, _i("RAYDIUM_HTTP_RETRIES", 3))
BACKOFF_S = max(0.1, _f("RAYDIUM_HTTP_BACKOFF_SEC", 0.6))
TRUST_ENV = _b("RAYDIUM_TRUST_ENV", True)

# MOMO / alert behaviour
SEND_MOMO_ALERTS = _b("RAYDIUM_SEND_MOMO_ALERTS", True)
MAX_ALERTS_PER_SCAN = _i("RAYDIUM_MAX_ALERTS_PER_SCAN", 3)

# Optional AI / whales enrichment toggles. Safe to leave off if infra is not ready.
AI_RUG_ENABLED = _b("RAYDIUM_AI_RUG_ENABLED", False)
AI_RUG_MAX_PER_SCAN = _i("RAYDIUM_AI_RUG_MAX_PER_SCAN", 15)
WHALES_ENABLED = _b("RAYDIUM_WHALES_ENABLED", False)

# Debug toggles
DEBUG_META_SAMPLE = _b("RAYDIUM_DEBUG_META_SAMPLE", True)
DEBUG_PF_RESULT = _b("RAYDIUM_DEBUG_PF_RESULT", True)

# You can override the search queries; all results are filtered to dexId=raydium anyway.
SEARCH_QUERIES = [
    q.strip()
    for q in os.getenv("RAYDIUM_SEARCH_QUERIES", "raydium,solana").split(",")
    if q.strip()
]

# Raydium ↔ PF interaction flags
# Default TRUE: bypass PF (only use fallback) unless you explicitly turn it off.
RAYDIUM_BYPASS_PF = _b("RAYDIUM_BYPASS_PF", True)
RAYDIUM_ENABLE_FALLBACK = _b("RAYDIUM_ENABLE_FALLBACK", True)

# New, simple fallback tuning (what you just set in env)
RAYDIUM_FB_MIN_BUYS_5M = _i("RAYDIUM_FB_MIN_BUYS_5M", 1)
RAYDIUM_FB_MIN_VOL5_USD = _f("RAYDIUM_FB_MIN_VOL5_USD", 300.0)
RAYDIUM_FB_MAX_RUNNERS = max(1, _i("RAYDIUM_FB_MAX_RUNNERS", 3))

# -------------------------------------------------------------------------
# Soft prefilters BEFORE PF_* (to cut truly dead dust but let more pass)
# -------------------------------------------------------------------------
_BASE_SOFT_LIQ_DEFAULT = max(0.0, PF_SCAN_MIN_LIQ_USD * 0.25)
_BASE_SOFT_MAX_AGE_DEFAULT = (
    PF_SCAN_MAX_TOKEN_AGE_MIN * 1.5 if PF_SCAN_MAX_TOKEN_AGE_MIN > 0 else 0.0
)

SOFT_MIN_LIQ_USD = _f("RAYDIUM_SOFT_MIN_LIQ_USD", _BASE_SOFT_LIQ_DEFAULT)
# NOTE: we do NOT soft-filter on mcap anymore; too many SOL/RAY new tokens have 0 mcap in Dexscreener.
SOFT_MAX_AGE_MIN = _f("RAYDIUM_SOFT_MAX_AGE_MIN", _BASE_SOFT_MAX_AGE_DEFAULT)


# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------

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
        diff_s = max(0.0, (time.time() - (t_ms / 1000.0)))
        return diff_s / 60.0
    except Exception:
        return None


def _buy_ratio(buys: int, sells: int) -> float:
    total = max(1, buys + sells)
    return float(buys) / float(total)


def _vol_key(vol: Dict[str, Any], key1: str, alt_key: Optional[str] = None) -> float:
    """
    Safely extract a USD volume for a given timeframe from Dexscreener's volume map.
    Handles both:
      - {"m5": 123.4}
      - {"m5": {"usd": 123.4}}
    """
    raw = vol.get(key1)
    if isinstance(raw, dict):
        raw = raw.get("usd")
    if raw is None and alt_key:
        raw = vol.get(alt_key)
        if isinstance(raw, dict):
            raw = raw.get("usd")
    return _nz_float(raw, 0.0)


# -----------------------------------------------------------------------------
# HTTP
# -----------------------------------------------------------------------------

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
                            logger.warning(
                                "Raydium: non-JSON 200 at %s (first=%r)", url, first
                            )
                            return None, status, first

                if 400 <= r.status < 500 and r.status != 429:
                    logger.warning(
                        "Raydium: HTTP %s at %s (first=%r)", r.status, url, first
                    )
                    return None, status, first

                logger.warning(
                    "Raydium: HTTP %s at %s (attempt %d/%d)",
                    r.status,
                    url,
                    attempt,
                    RETRIES,
                )
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            logger.warning(
                "Raydium: %s at %s (attempt %d/%d)",
                type(e).__name__,
                url,
                attempt,
                RETRIES,
            )
        except Exception as e:
            logger.warning("Raydium: unexpected error at %s: %s", url, e)

        if attempt < RETRIES:
            await asyncio.sleep(BACKOFF_S * attempt)

    return None, status, first


async def _fetch_pairs(session: aiohttp.ClientSession) -> List[Dict[str, Any]]:
    """
    Pull Raydium pairs from Dexscreener search.

    We query a few keywords (SEARCH_QUERIES) and dedupe by pair address.
    Dexscreener does NOT expose a "latest Raydium feed" endpoint like
    /latest/dex/pairs/solana, so we must use /latest/dex/search.
    """
    url = f"{BASE}/latest/dex/search"
    out: List[Dict[str, Any]] = []
    seen_ids: Set[str] = set()

    for q in SEARCH_QUERIES:
        params = {"q": q, "limit": SEARCH_LIMIT}
        data, _status, _first = await _fetch_json(session, url, params=params)
        if not data:
            continue

        pairs = data.get("pairs") or []
        for p in pairs:
            if (p.get("chainId") or "").lower() != "solana":
                continue
            if (p.get("dexId") or "").lower() != "raydium":
                continue

            pid = p.get("pairAddress") or ((p.get("baseToken") or {}).get("address"))
            if pid and pid not in seen_ids:
                seen_ids.add(pid)
                out.append(p)

    logger.info("Raydium: fetched %d raw Raydium pairs from Dexscreener", len(out))
    return out


# -----------------------------------------------------------------------------
# Metric extraction & normalization ⇒ PF meta
# -----------------------------------------------------------------------------

def _extract_metrics(pair: Dict[str, Any]) -> Dict[str, Any]:
    """
    Extract raw numeric metrics from a Dexscreener Raydium pair and map them
    into the names expected by utils_pkg.pair_filters.
    """
    liq = pair.get("liquidity") or {}
    liq_usd = _nz_float(liq.get("usd"), 0.0)

    fdv_usd = _nz_float(pair.get("fdv"), 0.0)
    # Market cap is often missing for fresh SOL/RAY pools; don't be strict here.
    mcap_usd = _nz_float(
        pair.get("marketCap")
        or pair.get("marketCapUsd")
        or pair.get("mcap")
        or fdv_usd,
        0.0,
    )

    txs = (pair.get("txns") or {})
    m5_tx = txs.get("m5") or {}
    h1_tx = txs.get("h1") or {}
    h24_tx = txs.get("h24") or {}

    buys_5m = _safe_int(m5_tx.get("buys"), 0)
    sells_5m = _safe_int(m5_tx.get("sells"), 0)

    buys_1h = _safe_int(h1_tx.get("buys"), 0)
    sells_1h = _safe_int(h1_tx.get("sells"), 0)

    buys_24h = _safe_int(h24_tx.get("buys"), 0)
    sells_24h = _safe_int(h24_tx.get("sells"), 0)

    vol = pair.get("volume") or {}

    vol_5m_usd = _vol_key(vol, "m5", "5m")
    vol_1h_usd = _vol_key(vol, "h1", "1h")
    vol_24h_usd = _vol_key(vol, "h24", "24h")

    # Fallbacks if only longer timeframes exist
    if vol_5m_usd <= 0.0 and vol_1h_usd > 0.0:
        vol_5m_usd = vol_1h_usd / 12.0
    if vol_1h_usd <= 0.0 and vol_24h_usd > 0.0:
        vol_1h_usd = vol_24h_usd / 24.0
    if vol_5m_usd <= 0.0 and vol_24h_usd > 0.0:
        vol_5m_usd = vol_24h_usd / 288.0

    pc = pair.get("priceChange") or {}
    chg_5m_pct = _nz_float(pc.get("m5") or pc.get("5m"), 0.0)
    chg_1h_pct = _nz_float(pc.get("h1") or pc.get("1h"), 0.0)
    chg_24h_pct = _nz_float(pc.get("h24") or pc.get("24h"), 0.0)

    # Fallbacks if 5m missing
    if chg_5m_pct == 0.0 and chg_1h_pct != 0.0:
        chg_5m_pct = chg_1h_pct / 12.0
    if chg_5m_pct == 0.0 and chg_24h_pct != 0.0:
        chg_5m_pct = chg_24h_pct / 288.0

    # 15m ≈ between 5m and 1h
    if chg_1h_pct != 0.0:
        chg_15m_pct = chg_1h_pct / 4.0
    else:
        chg_15m_pct = chg_24h_pct / 96.0 if chg_24h_pct != 0.0 else chg_5m_pct

    price_usd = _nz_float(pair.get("priceUsd") or pair.get("price"), 0.0)

    pair_created_ms = (
        pair.get("pairCreatedAt")
        or pair.get("pairCreatedAtMs")
        or pair.get("pairCreatedAtTs")
    )
    age_min = _age_min(pair_created_ms)
    token_age_min = age_min if age_min is not None else 9999.0

    # Holders not exposed; neutral
    holders = 0
    holders_5m_change = 0

    # Whales: Dexscreener does not expose; approximate from 5m volume + price.
    whales_5m = 0
    whale_vol_5m_usd = 0.0
    largest_buy_5m_sol = 0.0
    if price_usd > 0 and vol_5m_usd > 0:
        approx_largest_buy_usd = vol_5m_usd * 0.25
        approx_largest_buy_sol = approx_largest_buy_usd / price_usd
        largest_buy_5m_sol = approx_largest_buy_sol
        if approx_largest_buy_sol >= float(PF_SCAN_MIN_WHALE_SIZE_SOL):
            whales_5m = 1
            whale_vol_5m_usd = approx_largest_buy_usd

    return {
        "liq_usd": liq_usd,
        "mcap_usd": mcap_usd,
        "fdv_usd": fdv_usd,
        "token_age_min": token_age_min,
        "vol_5m_usd": vol_5m_usd,
        "vol_1h_usd": vol_1h_usd,
        "vol_24h_usd": vol_24h_usd,
        "buys_5m": buys_5m,
        "sells_5m": sells_5m,
        "buys_1h": buys_1h,
        "sells_1h": sells_1h,
        "buys_24h": buys_24h,
        "sells_24h": sells_24h,
        "price_change_5m_pct": chg_5m_pct,
        "price_change_15m_pct": chg_15m_pct,
        "price_change_1h_pct": chg_1h_pct,
        "price_usd": price_usd,
        "holders": holders,
        "holders_5m_change": holders_5m_change,
        "whales_5m": whales_5m,
        "whale_vol_5m_usd": whale_vol_5m_usd,
        "largest_buy_5m_sol": largest_buy_5m_sol,
    }


def _normalize_to_meta(pair: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Normalize raw Dexscreener Raydium pair into the "meta" dict that
    utils_pkg.pair_filters expects.
    """
    try:
        if (pair.get("chainId") or "").lower() != "solana":
            return None
        if (pair.get("dexId") or "").lower() != "raydium":
            return None

        metrics = _extract_metrics(pair)

        base = pair.get("baseToken") or {}
        mint = (base.get("address") or "").strip()
        if not mint:
            return None

        symbol = (base.get("symbol") or "").strip() or mint[:6]

        buys_5m = metrics["buys_5m"]
        sells_5m = metrics["sells_5m"]
        buy_ratio_5m = _buy_ratio(buys_5m, sells_5m)

        url = (
            pair.get("url")
            or pair.get("pairUrl")
            or f"https://dexscreener.com/solana/{mint}"
        )

        meta: Dict[str, Any] = {
            # identity
            "dex": "raydium",
            "chain": "solana",
            "source": "raydium",
            "mint": mint,
            "symbol": symbol,
            "pair_url": url,

            # core liquidity / size
            "liq_usd": metrics["liq_usd"],
            "mcap_usd": metrics["mcap_usd"],
            "fdv_usd": metrics["fdv_usd"],

            # age
            "token_age_min": metrics["token_age_min"],

            # recent activity
            "vol_5m_usd": metrics["vol_5m_usd"],
            "vol_1h_usd": metrics["vol_1h_usd"],
            "vol_24h_usd": metrics["vol_24h_usd"],
            "buys_5m": buys_5m,
            "sells_5m": sells_5m,
            "txns_5m": buys_5m + sells_5m,
            "txns_1h": metrics["buys_1h"] + metrics["sells_1h"],
            "buy_ratio_5m": buy_ratio_5m,

            # price action
            "price_usd": metrics["price_usd"],
            "price_change_5m_pct": metrics["price_change_5m_pct"],
            "price_change_15m_pct": metrics["price_change_15m_pct"],
            "price_change_1h_pct": metrics["price_change_1h_pct"],

            # holders
            "holders": metrics["holders"],
            "holders_5m_change": metrics["holders_5m_change"],

            # whale / AI fields
            "whales_5m": metrics["whales_5m"],
            "whale_vol_5m_usd": metrics["whale_vol_5m_usd"],
            "largest_buy_5m_sol": metrics["largest_buy_5m_sol"],
            "ai_rug_score": 0.0,  # neutral by default

            # security flags (no direct info here, so defaults are safe-ish)
            "is_honeypot": False,
            "trading_paused": False,
            "blacklisted": False,
            "top10_hold_pct": 0.0,
            "dev_hold_pct": 0.0,
            "mint_authority_disabled": True,
            "freeze_authority_disabled": True,
        }

        return meta
    except Exception as e:
        logger.debug("Raydium normalize error: %s", e)
        return None


# -----------------------------------------------------------------------------
# Optional AI / whales enrichment
# -----------------------------------------------------------------------------

async def _enrich_ai_and_whales(metas: List[Dict[str, Any]]) -> None:
    """
    Best-effort enrichment of meta with:
      - ai_rug_score (0–1) via utils_pkg.core_utils.ai_fraud_score
      - basic whale signal via utils_pkg.core_utils.is_whale_wallet
    """
    if not metas:
        return

    try:
        from utils_pkg.core_utils import ai_fraud_score, is_whale_wallet  # type: ignore
    except Exception:
        # AI / whales not wired; skip silently.
        return

    sem = asyncio.Semaphore(_i("RAYDIUM_AI_CONCURRENCY", 5))
    to_process = metas[: AI_RUG_MAX_PER_SCAN] if AI_RUG_ENABLED else []

    async def _handle(meta: Dict[str, Any]) -> None:
        mint = meta.get("mint")
        if not isinstance(mint, str) or not mint:
            return
        async with sem:
            # AI rug score
            if AI_RUG_ENABLED:
                try:
                    s01 = await ai_fraud_score(mint)
                    meta["ai_rug_score"] = max(0.0, min(1.0, float(s01)))
                except Exception:
                    pass
            # Whale presence (coarse)
            if WHALES_ENABLED:
                try:
                    px = float(meta.get("price_usd") or 0.0)
                    thresh_usd = (
                        float(PF_SCAN_MIN_WHALE_SIZE_SOL) * px if px > 0 else 50_000.0
                    )
                    has_whale = await is_whale_wallet(mint, usd_threshold=thresh_usd)
                    if has_whale:
                        meta["whales_5m"] = max(1, int(meta.get("whales_5m") or 0))
                except Exception:
                    pass

    tasks = [asyncio.create_task(_handle(m)) for m in to_process]
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)


# -----------------------------------------------------------------------------
# PF-based filtering & ranking + NEW fallback picker
# -----------------------------------------------------------------------------

def _apply_pf(metas: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Apply PF filters & scoring and return top LIMIT by final_score.
    (Note: we bypass this by default now unless RAYDIUM_BYPASS_PF=0.)
    """
    out: List[Dict[str, Any]] = []

    for meta in metas:
        try:
            if not is_viable_pair(meta):
                continue

            momentum = compute_momentum_score(meta)
            final = compute_final_score(meta)

            m = dict(meta)
            m["momentum_score"] = momentum
            m["final_score"] = final
            out.append(m)
        except Exception as e:
            logger.debug(
                "Raydium PF filter error for %s: %s", meta.get("mint"), e
            )

    out.sort(key=lambda x: x.get("final_score", 0.0), reverse=True)
    return out[:LIMIT]


def _fallback_pick_metas(
    metas: List[Dict[str, Any]],
    *,
    log: logging.Logger,
    why: str = "fb",
) -> List[Dict[str, Any]]:
    """
    Very simple, permissive Raydium fallback:
    - ignore age and PF scoring
    - keep anything with decent 5m volume + at least 1 buy
    - sort by 5m volume desc and cap by env limit

    Controlled by:
      - RAYDIUM_FB_MIN_BUYS_5M
      - RAYDIUM_FB_MIN_VOL5_USD
      - RAYDIUM_FB_MAX_RUNNERS
    """
    if not RAYDIUM_ENABLE_FALLBACK or not metas:
        return []

    fb_min_buys = int(RAYDIUM_FB_MIN_BUYS_5M)
    fb_min_vol = float(RAYDIUM_FB_MIN_VOL5_USD)
    fb_max = int(RAYDIUM_FB_MAX_RUNNERS)

    candidates: List[Dict[str, Any]] = []
    for m in metas:
        try:
            buys5 = int(m.get("buys_5m") or 0)
            vol5 = float(m.get("vol_5m_usd") or 0.0)

            if buys5 < fb_min_buys:
                continue
            if vol5 < fb_min_vol:
                continue

            candidates.append(m)
        except Exception:
            continue

    if not candidates:
        log.info(
            "Raydium fallback[%s]: 0 metas matched fb_min_buys=%d fb_min_vol=%s from %d metas",
            why,
            fb_min_buys,
            fb_min_vol,
            len(metas),
        )
        return []

    # Prefer the most actively traded in the last 5m
    candidates.sort(key=lambda m: float(m.get("vol_5m_usd") or 0.0), reverse=True)
    picked = candidates[:fb_max]

    log.info(
        "Raydium fallback[%s]: picked %d/%d metas (fb_min_buys=%d fb_min_vol=%s fb_max=%d)",
        why,
        len(picked),
        len(candidates),
        fb_min_buys,
        fb_min_vol,
        fb_max,
    )
    return picked


# -----------------------------------------------------------------------------
# MOMO event wiring (uses runners list)
# -----------------------------------------------------------------------------

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
        logger.warning("Raydium: failed to init AlertManager for MOMO: %s", e)
        return

    max_n = max(0, min(MAX_ALERTS_PER_SCAN, len(runners)))
    for c in runners[:max_n]:
        try:
            age_min = c.get("token_age_min")
            age_sec = age_min * 60.0 if isinstance(age_min, (int, float)) else None
            ctx: Dict[str, Any] = {
                "symbol": c.get("symbol") or c.get("mint"),
                "mint": c.get("mint"),
                # pool id used by filters / POOLS env
                "pool": "raydium",
                # separate URL for alerts
                "pool_url": c.get("pair_url"),
                "age_sec": age_sec,
                "liq_usd": c.get("liq_usd"),
                "vol_5m": c.get("vol_5m_usd"),
                "up_pct": c.get("price_change_5m_pct"),
                "reason": (
                    f"raydium runner liq=${c.get('liq_usd', 0):.0f} "
                    f"vol5=${c.get('vol_5m_usd', 0):.0f} up5={c.get('price_change_5m_pct', 0):.2f}%"
                ),
            }
            await am.send_momo_event("MOMO_OK", ctx)
        except Exception as e:
            logger.warning(
                "Raydium: MOMO send error for %s: %s", c.get("mint"), e
            )


# -----------------------------------------------------------------------------
# Public entry
# -----------------------------------------------------------------------------

async def scan_raydium(*args):
    """
    Flexible signature:
      - scan_raydium()
      - scan_raydium(redis)
      - scan_raydium(redis, session)
      - scan_raydium(session)

    Only an aiohttp.ClientSession (if provided) is used.
    Returns a list of runner candidates (each is a meta dict).
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
            "scanner.raydium: starting scan; queries=%s soft_min_liq=$%.0f "
            "pf_min_liq=$%.0f limit=%d search_limit=%d bypass_pf=%s fallback=%s",
            ",".join(SEARCH_QUERIES),
            SOFT_MIN_LIQ_USD,
            PF_SCAN_MIN_LIQ_USD,
            LIMIT,
            SEARCH_LIMIT,
            RAYDIUM_BYPASS_PF,
            RAYDIUM_ENABLE_FALLBACK,
        )
        pairs = await _fetch_pairs(sess)

        metas: List[Dict[str, Any]] = []
        for p in pairs:
            m = _normalize_to_meta(p)
            if m:
                metas.append(m)

        total_metas = len(metas)

        # --- soft prefilter: keep out truly dead dust but allow more than PF ---
        soft_metas: List[Dict[str, Any]] = []
        for m in metas:
            try:
                liq = float(m.get("liq_usd") or 0.0)
                age = float(m.get("token_age_min") or 9999.0)

                if SOFT_MIN_LIQ_USD > 0.0 and liq < SOFT_MIN_LIQ_USD:
                    continue
                if SOFT_MAX_AGE_MIN > 0.0 and age > SOFT_MAX_AGE_MIN:
                    continue

                soft_metas.append(m)
            except Exception:
                continue

        # ⚠️ Safety net: if soft filters nuked everything, fall back to raw metas
        if not soft_metas and metas:
            logger.info(
                "scanner.raydium: soft filters removed all metas (raw=%d); "
                "falling back to raw metas for PF/fallback",
                total_metas,
            )
            soft_metas = metas

        if DEBUG_META_SAMPLE:
            if soft_metas:
                sample = [
                    {
                        "sym": m.get("symbol"),
                        "liq": m.get("liq_usd"),
                        "vol5": m.get("vol_5m_usd"),
                        "buys5": m.get("buys_5m"),
                        "sells5": m.get("sells_5m"),
                        "chg5": m.get("price_change_5m_pct"),
                        "age_min": m.get("token_age_min"),
                    }
                    for m in soft_metas[:5]
                ]
                logger.info(
                    "RAY_META_SAMPLE count=%d (soft=%d, raw=%d) first=%s",
                    len(soft_metas),
                    len(soft_metas),
                    total_metas,
                    sample,
                )
            else:
                logger.info(
                    "RAY_META_SAMPLE count=0 (soft=0, raw=%d)", total_metas
                )

        # Optional AI / whale enrichment (no-op if disabled or imports fail).
        with contextlib.suppress(Exception):
            await _enrich_ai_and_whales(soft_metas)

        pf_runners: List[Dict[str, Any]] = []
        if not RAYDIUM_BYPASS_PF:
            pf_runners = _apply_pf(soft_metas)

        fb_metas: List[Dict[str, Any]] = []
        if pf_runners:
            runners = pf_runners
        else:
            fb_metas = _fallback_pick_metas(soft_metas, log=logger, why="ray")
            if not fb_metas and soft_metas:
                logger.warning(
                    "Raydium: PF produced 0 runners and fallback found nothing from %d metas (soft-filtered)",
                    len(soft_metas),
                )
            runners = fb_metas

        if DEBUG_PF_RESULT:
            logger.info(
                "RAY_PF_RESULT pf=%d fb=%d metas=%d pairs=%d",
                len(pf_runners),
                len(fb_metas),
                len(soft_metas),
                len(pairs),
            )

        logger.info(
            "scanner.raydium: %s runners returned (from %s metas, %s raw pairs)",
            len(runners),
            len(soft_metas),
            len(pairs),
        )

        # 🔥 Send MOMO for whatever runners we’re actually using
        if runners:
            await _send_momo_for_runners(runners)

        return runners
    except Exception as e:
        logger.warning("Raydium scan error: %s", e)
        return []
    finally:
        if own_session and sess:
            with contextlib.suppress(Exception):
                await sess.close()


__all__ = ["scan_raydium"]
