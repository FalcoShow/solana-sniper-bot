# scanner/dexscreener_scanner.py
# Pulls active Solana "runner" pairs from Dexscreener and feeds them into
# utils_pkg.pair_filters for shared filtering & scoring.
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
    PF_SCAN_MIN_LIQ_USD,
    PF_SCAN_MIN_MCAP_USD,
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

logger = logging.getLogger("scanner.dexscreener")

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
# Config / env (Dexscreener-specific on top of PF_* defaults)
# ──────────────────────────────────────────────────────────────────────────────

UA = _s(
    "SCANNER_UA",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) SniperBot/1.0 (+dexscreener)",
)

BASE = _s("DEXSCREENER_BASE_URL", "https://api.dexscreener.com").rstrip("/")
LIMIT = _i("DEX_LIMIT", 40)
TIMEOUT_S = _f("DEX_HTTP_TIMEOUT_SEC", 12.0)
RETRIES = max(1, _i("DEX_HTTP_RETRIES", 3))
BACKOFF_S = max(0.1, _f("DEX_HTTP_BACKOFF_SEC", 0.6))

# Comma-separated search queries; default "solana".
SEARCH_QUERIES = [
    q.strip()
    for q in os.getenv("DEX_SEARCH_QUERIES", "solana").split(",")
    if q.strip()
]

# Honor system proxy variables if present (aiohttp trust_env)
TRUST_ENV = _b("DEX_TRUST_ENV", True)

# Soft prefilters BEFORE PF logic (we still use PF for final viability)
SOFT_MIN_LIQ_USD = _f("DEX_SOFT_MIN_LIQ_USD", PF_SCAN_MIN_LIQ_USD * 0.4)
SOFT_MIN_MCAP_USD = _f("DEX_SOFT_MIN_MCAP_USD", PF_SCAN_MIN_MCAP_USD * 0.4)
SOFT_MAX_AGE_MIN = _f("DEX_SOFT_MAX_AGE_MIN", PF_SCAN_MAX_TOKEN_AGE_MIN * 1.2)

# MOMO / alert behaviour
SEND_MOMO_ALERTS = _b("DEX_SEND_MOMO_ALERTS", True)
MAX_ALERTS_PER_SCAN = _i("DEX_MAX_ALERTS_PER_SCAN", 3)

# Optional AI / whales enrichment toggles (off by default, safe if not configured)
DEX_AI_RUG_ENABLED = _b("DEX_AI_RUG_ENABLED", False)
DEX_AI_RUG_MAX_PER_SCAN = _i("DEX_AI_RUG_MAX_PER_SCAN", 20)
DEX_WHALES_ENABLED = _b("DEX_WHALES_ENABLED", False)


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
                        import json as _json
                        try:
                            data = _json.loads(text)
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
        data, _status, _first = await _fetch_json(session, url, params={"q": q})
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
# Metrics / normalization → PF meta shape
# ──────────────────────────────────────────────────────────────────────────────

def _extract_metrics(pair: Dict[str, Any]) -> Dict[str, Any]:
    """
    Pull out metrics in the shape that utils_pkg.pair_filters expects.

    We produce:
      - liq_usd, mcap_usd, fdv_usd
      - token_age_min
      - vol_5m_usd, vol_1h_usd, vol_24h_usd
      - buys_5m, sells_5m, buys_24h, sells_24h
      - price_change_5m_pct, price_change_15m_pct, price_change_1h_pct
      - holders, holders_5m_change
      - whales_5m, whale_vol_5m_usd, largest_buy_5m_sol (heuristic)
      - price_usd
    """
    base = pair.get("baseToken") or {}
    quote = pair.get("quoteToken") or {}

    liq_usd = _nz_float((pair.get("liquidity") or {}).get("usd"), 0.0)
    fdv_usd = _nz_float(
        pair.get("fdv")
        or pair.get("fdvUsd")
        or pair.get("fullyDilutedValuation")
        or pair.get("fullyDilutedMcUsd"),
        0.0,
    )
    mcap_usd = _nz_float(
        pair.get("marketCap")
        or pair.get("marketCapUsd")
        or pair.get("mcap")
        or fdv_usd,
        0.0,
    )

    txns = pair.get("txns") or {}
    m5 = txns.get("m5") or {}
    h1 = txns.get("h1") or {}
    h24 = txns.get("h24") or {}

    buys_5m = _safe_int(m5.get("buys"))
    sells_5m = _safe_int(m5.get("sells"))

    buys_1h = _safe_int(h1.get("buys"))
    sells_1h = _safe_int(h1.get("sells"))

    buys_24h = _safe_int(h24.get("buys"))
    sells_24h = _safe_int(h24.get("sells"))

    # If 5m buys are zero but 1h/24h exist, approximate
    if buys_5m == 0 and buys_1h > 0:
        buys_5m = max(1, buys_1h // 12)
    if buys_5m == 0 and buys_24h > 0:
        buys_5m = max(1, buys_24h // 288)

    if sells_5m == 0 and sells_1h > 0:
        sells_5m = max(0, sells_1h // 12)
    if sells_5m == 0 and sells_24h > 0:
        sells_5m = max(0, sells_24h // 288)

    vol = pair.get("volume") or {}
    vol_5m_usd = _nz_float(vol.get("m5"), 0.0)
    vol_1h_usd = _nz_float(vol.get("h1"), 0.0)
    vol_24h_usd = _nz_float(vol.get("h24"), 0.0)

    # Approximate missing windows from 24h
    if vol_1h_usd <= 0 and vol_24h_usd > 0:
        vol_1h_usd = vol_24h_usd / 24.0
    if vol_5m_usd <= 0 and vol_1h_usd > 0:
        vol_5m_usd = vol_1h_usd / 12.0
    if vol_5m_usd <= 0 and vol_24h_usd > 0:
        vol_5m_usd = vol_24h_usd / 288.0

    chg = pair.get("priceChange") or {}
    chg_5m_pct = _nz_float(chg.get("m5"), 0.0)
    chg_1h_pct = _nz_float(chg.get("h1"), 0.0)
    chg_24h_pct = _nz_float(chg.get("h24"), 0.0)

    # Dexscreener usually does not expose 15m; approximate from 1h or 24h
    chg_15m_pct = 0.0
    if chg_1h_pct != 0.0:
        chg_15m_pct = chg_1h_pct / 4.0
    elif chg_24h_pct != 0.0:
        chg_15m_pct = chg_24h_pct / 96.0

    # If no 5m, approximate from 1h/24h as last resort
    if chg_5m_pct == 0.0 and chg_1h_pct != 0.0:
        chg_5m_pct = chg_1h_pct / 12.0
    if chg_5m_pct == 0.0 and chg_24h_pct != 0.0:
        chg_5m_pct = chg_24h_pct / 288.0

    age_ms = (
        pair.get("pairCreatedAt")
        or pair.get("pairCreatedAtMs")
        or pair.get("pairCreatedAtTs")
    )
    age_min = _age_min(age_ms)
    token_age_min = age_min if age_min is not None else 9999.0

    holders = _safe_int(pair.get("holders") or 0)
    holders_5m_change = 0  # Dexscreener doesn't give short-interval holder deltas

    price_usd = _nz_float(pair.get("priceUsd"), 0.0)

    # Whales: Dexscreener doesn't give per-wallets; approximate from 5m volume + price.
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
        "holders": holders,
        "holders_5m_change": holders_5m_change,
        "price_usd": price_usd,
        "whales_5m": whales_5m,
        "whale_vol_5m_usd": whale_vol_5m_usd,
        "largest_buy_5m_sol": largest_buy_5m_sol,
        "base_symbol": (base.get("symbol") or "").strip(),
        "base_mint": (base.get("address") or "").strip(),
        "quote_symbol": (quote.get("symbol") or "").strip(),
    }


def _normalize_to_meta(pair: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Normalize a Dexscreener pair → PF-compatible "meta" dict.

    Applies:
      - chainId == solana
      - soft prefilters using SOFT_MIN_LIQ_USD / SOFT_MIN_MCAP_USD / SOFT_MAX_AGE_MIN
      - meta fields matching utils_pkg.pair_filters expectations
    """
    try:
        if (pair.get("chainId") or "").lower() != "solana":
            return None

        metrics = _extract_metrics(pair)

        # Soft prefilters
        liq_usd = metrics["liq_usd"]
        mcap_usd = metrics["mcap_usd"]
        token_age_min = metrics["token_age_min"]

        if liq_usd < SOFT_MIN_LIQ_USD:
            return None
        if mcap_usd < SOFT_MIN_MCAP_USD:
            return None
        if token_age_min > SOFT_MAX_AGE_MIN:
            return None

        mint = metrics["base_mint"]
        if not mint:
            return None

        symbol = metrics["base_symbol"] or mint[:6]
        dex_id = (pair.get("dexId") or "").lower() or "unknown"

        url = (
            pair.get("url")
            or pair.get("pairUrl")
            or f"https://dexscreener.com/solana/{mint}"
        )

        # We only have direct 5m trades; 1h txns approximated from 5m
        txns_5m = metrics["buys_5m"] + metrics["sells_5m"]
        txns_1h = txns_5m * 12

        buy_ratio_5m = (
            metrics["buys_5m"] / max(1, metrics["buys_5m"] + metrics["sells_5m"])
        )

        meta: Dict[str, Any] = {
            # identity
            "source": "dexscreener",
            "dex": dex_id,
            "chain": "solana",
            "pair_address": pair.get("pairAddress"),
            "mint": mint,
            "symbol": symbol,
            "pair_url": url,

            # core liquidity / size
            "liq_usd": metrics["liq_usd"],
            "mcap_usd": metrics["mcap_usd"],
            "fdv_usd": metrics["fdv_usd"],

            # time
            "token_age_min": metrics["token_age_min"],

            # recent activity
            "vol_5m_usd": metrics["vol_5m_usd"],
            "vol_1h_usd": metrics["vol_1h_usd"],
            "vol_24h_usd": metrics["vol_24h_usd"],
            "buys_5m": metrics["buys_5m"],
            "sells_5m": metrics["sells_5m"],
            "txns_5m": txns_5m,
            "txns_1h": txns_1h,
            "buy_ratio_5m": buy_ratio_5m,

            # price action
            "price_usd": metrics["price_usd"],
            "price_change_5m_pct": metrics["price_change_5m_pct"],
            "price_change_15m_pct": metrics["price_change_15m_pct"],
            "price_change_1h_pct": metrics["price_change_1h_pct"],

            # holders
            "holders": metrics["holders"],
            "holders_5m_change": metrics["holders_5m_change"],

            # whale + AI fields
            "whales_5m": metrics["whales_5m"],
            "whale_vol_5m_usd": metrics["whale_vol_5m_usd"],
            "largest_buy_5m_sol": metrics["largest_buy_5m_sol"],
            "ai_rug_score": 0.0,  # neutral by default

            # security flags (we don't have direct info here, so defaults are safe-ish)
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
        logger.debug("Dexscreener normalize error: %s", e)
        return None


# ──────────────────────────────────────────────────────────────────────────────
# Optional AI / whales enrichment
# ──────────────────────────────────────────────────────────────────────────────

async def _enrich_ai_and_whales(metas: List[Dict[str, Any]]) -> None:
    """
    Best-effort enrichment of meta with:
      - ai_rug_score (0–1) via utils_pkg.core_utils.ai_fraud_score
      - additional whale presence via utils_pkg.core_utils.is_whale_wallet

    Fully optional:
      - If imports fail, metas stay usable.
      - Concurrency + per-scan cap to avoid rate-limit issues.
    """
    if not metas:
        return

    try:
        from utils_pkg.core_utils import ai_fraud_score, is_whale_wallet  # type: ignore
    except Exception:
        return

    sem = asyncio.Semaphore(_i("DEX_AI_CONCURRENCY", 5))
    to_process = metas[: DEX_AI_RUG_MAX_PER_SCAN] if DEX_AI_RUG_ENABLED else []

    async def _handle(meta: Dict[str, Any]) -> None:
        mint = meta.get("mint")
        if not isinstance(mint, str) or not mint:
            return
        async with sem:
            if DEX_AI_RUG_ENABLED:
                try:
                    s01 = await ai_fraud_score(mint)
                    meta["ai_rug_score"] = max(0.0, min(1.0, float(s01)))
                except Exception:
                    pass
            if DEX_WHALES_ENABLED:
                try:
                    px = float(meta.get("price_usd") or 0.0)
                    thresh_usd = float(PF_SCAN_MIN_WHALE_SIZE_SOL) * px if px > 0 else 50_000.0
                    has_whale = await is_whale_wallet(mint, usd_threshold=thresh_usd)
                    if has_whale:
                        meta["whales_5m"] = max(1, int(meta.get("whales_5m") or 0))
                except Exception:
                    pass

    tasks = [asyncio.create_task(_handle(m)) for m in to_process]
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)


# ──────────────────────────────────────────────────────────────────────────────
# PF-based filtering & ranking
# ──────────────────────────────────────────────────────────────────────────────

def _pf_filter_and_rank(metas: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Take normalized meta entries, apply is_viable_pair and PF scoring, and
    return the top LIMIT candidates ranked by final_score.
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
            logger.debug("Dexscreener PF filter error for %s: %s", meta.get("mint"), e)

    out.sort(key=lambda x: x.get("final_score", 0.0), reverse=True)
    return out[:LIMIT]


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
        logger.warning("Dexscreener: failed to init AlertManager for MOMO: %s", e)
        return

    max_n = max(0, min(MAX_ALERTS_PER_SCAN, len(runners)))
    for c in runners[:max_n]:
        try:
            age_min = c.get("token_age_min")
            age_sec = age_min * 60.0 if isinstance(age_min, (int, float)) else None
            ctx: Dict[str, Any] = {
                "symbol": c.get("symbol") or c.get("mint"),
                "mint": c.get("mint"),
                "pool": c.get("pair_url"),
                "age_sec": age_sec,
                "liq_usd": c.get("liq_usd"),
                "vol_5m": c.get("vol_5m_usd"),
                "up_pct": c.get("price_change_5m_pct"),
                "reason": (
                    f"dexscreener PF runner final={c.get('final_score', 0.0):.3f} "
                    f"momentum={c.get('momentum_score', 0.0):.3f}"
                ),
            }
            await am.send_momo_event("MOMO_OK", ctx)
        except Exception as e:
            logger.warning("Dexscreener: MOMO send error for %s: %s", c.get("mint"), e)


# ──────────────────────────────────────────────────────────────────────────────
# Public entry
# ──────────────────────────────────────────────────────────────────────────────

async def _scan_inner(session: aiohttp.ClientSession) -> List[Dict[str, Any]]:
    logger.info(
        "🔎 Dexscreener scan: queries=%s soft_min_liq=$%.0f pf_min_liq=$%.0f limit=%d",
        ",".join(SEARCH_QUERIES),
        SOFT_MIN_LIQ_USD,
        PF_SCAN_MIN_LIQ_USD,
        LIMIT,
    )

    pairs = await _fetch_pairs(session)
    logger.info("Dexscreener: fetched %d raw pairs", len(pairs))

    metas: List[Dict[str, Any]] = []
    for p in pairs:
        m = _normalize_to_meta(p)
        if m:
            metas.append(m)

    if not metas:
        logger.info("Dexscreener: no metas after normalization")
        return []

    # Optional AI / whales enrichment
    try:
        coro = _enrich_ai_and_whales(metas)
        if asyncio.iscoroutine(coro):
            await coro
    except Exception:
        pass

    runners = _pf_filter_and_rank(metas)
    logger.info(
        "Dexscreener: %d PF runners (from %d metas, %d raw pairs)",
        len(runners),
        len(metas),
        len(pairs),
    )
    return runners


async def scan_dexscreener(*args):
    """
    Flexible signature (compatible with other scanners):
      - scan_dexscreener()
      - scan_dexscreener(redis)
      - scan_dexscreener(redis, session)
      - scan_dexscreener(session)

    Only an aiohttp.ClientSession (if provided) is used.
    Returns a list of PF-scored runner candidates (each is a meta dict).
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
            headers=_headers(), connector=conn, trust_env=TRUST_ENV, timeout=timeout
        )
        own_session = True

    try:
        runners = await _scan_inner(sess) or []
        await _send_momo_for_runners(runners)
        logger.info("Dexscreener: %d final PF runner candidates", len(runners))
        return runners
    except Exception as e:
        logger.warning("Dexscreener scan error: %s", e)
        return []
    finally:
        if own_session and sess is not None:
            with contextlib.suppress(Exception):
                await sess.close()


__all__ = ["scan_dexscreener"]
