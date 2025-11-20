# scanner/raydium_scanner.py
# Raydium scanner via Dexscreener — builds "meta" dicts compatible with
# utils_pkg.pair_filters and surfaces only high-quality runners.
from __future__ import annotations

import os
import ssl
import asyncio
import logging
import contextlib
import time
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

# How many top candidates we keep per scan (after PF filtering + scoring)
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

# You can override the search queries; all results are filtered to dexId=raydium anyway.
SEARCH_QUERIES = [
    q.strip()
    for q in os.getenv("RAYDIUM_SEARCH_QUERIES", "raydium,solana").split(",")
    if q.strip()
]

# -------------------------------------------------------------------------
# Soft prefilters BEFORE PF_* (to cut truly dead dust but let more pass)
# -------------------------------------------------------------------------
# We base these on PF_SCAN_* but loosen them, with floors so they never go crazy.
_BASE_SOFT_LIQ_DEFAULT = max(40.0, PF_SCAN_MIN_LIQ_USD * 0.3)
_BASE_SOFT_MCAP_DEFAULT = max(40.0, PF_SCAN_MIN_MCAP_USD * 0.25)
_BASE_SOFT_MAX_AGE_DEFAULT = max(30.0, PF_SCAN_MAX_TOKEN_AGE_MIN * 1.5)

SOFT_MIN_LIQ_USD = _f("RAYDIUM_SOFT_MIN_LIQ_USD", _BASE_SOFT_LIQ_DEFAULT)
SOFT_MIN_MCAP_USD = _f("RAYDIUM_SOFT_MIN_MCAP_USD", _BASE_SOFT_MCAP_DEFAULT)
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
    Pull recent Raydium pairs from Dexscreener search.
    We query a few keywords and dedupe by pair address.
    """
    url = f"{BASE}/latest/dex/search"
    out: List[Dict[str, Any]] = []
    seen_ids: set = set()

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
            # Prefer pairAddress for uniqueness, fall back to baseToken address
            pid = p.get("pairAddress") or ((p.get("baseToken") or {}).get("address"))
            if pid and pid not in seen_ids:
                seen_ids.add(pid)
                out.append(p)
    return out


# -----------------------------------------------------------------------------
# Metric extraction & normalization ⇒ PF meta
# -----------------------------------------------------------------------------

def _extract_metrics(pair: Dict[str, Any]) -> Dict[str, Any]:
    """
    Extract raw numeric metrics from a Dexscreener Raydium pair and map them
    into the names expected by utils_pkg.pair_filters.
    """
    liq_usd = _nz_float((pair.get("liquidity") or {}).get("usd"), 0.0)

    fdv_usd = _nz_float(
        pair.get("fdv")
        or pair.get("fdvUsd")
        or pair.get("fullyDilutedValuation"),
        0.0,
    )
    mcap_usd = _nz_float(
        pair.get("marketCap")
        or pair.get("marketCapUsd")
        or pair.get("mcap")
        or fdv_usd,
        0.0,
    )

    txs = (pair.get("txns") or {})
    m5 = txs.get("m5") or {}
    h1 = txs.get("h1") or {}
    h24 = txs.get("h24") or {}

    buys_5m = _safe_int(m5.get("buys"), 0)
    sells_5m = _safe_int(m5.get("sells"), 0)

    buys_1h = _safe_int(h1.get("buys"), 0)
    sells_1h = _safe_int(h1.get("sells"), 0)

    buys_24h = _safe_int(h24.get("buys"), 0)
    sells_24h = _safe_int(h24.get("sells"), 0)

    # Volume: Dexscreener mainly has h24; approximate lower windows.
    vol = pair.get("volume") or {}
    vol_24h_usd = _nz_float(vol.get("h24"), 0.0)
    vol_1h_usd = vol_24h_usd / 24.0 if vol_24h_usd > 0 else 0.0
    vol_5m_usd = vol_1h_usd / 12.0 if vol_1h_usd > 0 else 0.0

    # Price change – Dexscreener usually has h1, h6, h24; we derive 5m/15m.
    pc = pair.get("priceChange") or {}
    chg_1h_pct = _nz_float(pc.get("h1"), 0.0)
    chg_24h_pct = _nz_float(pc.get("h24"), 0.0)

    # Rough approximations for 5m / 15m if direct values are missing.
    chg_5m_pct = chg_1h_pct / 12.0 if chg_1h_pct else chg_24h_pct / 288.0
    chg_15m_pct = chg_1h_pct / 4.0 if chg_1h_pct else chg_24h_pct / 96.0

    price_usd = _nz_float(pair.get("priceUsd"), 0.0)

    pair_created_ms = (
        pair.get("pairCreatedAt")
        or pair.get("pairCreatedAtMs")
        or pair.get("pairCreatedAtTs")
    )
    age_min = _age_min(pair_created_ms)
    token_age_min = age_min if age_min is not None else 9999.0

    # Holders / holder change are not exposed; we leave at 0 so they do not block.
    holders = 0
    holders_5m_change = 0

    # Whales: Dexscreener does not expose; approximate from 5m volume + price.
    whales_5m = 0
    whale_vol_5m_usd = 0.0
    largest_buy_5m_sol = 0.0
    if price_usd > 0 and vol_5m_usd > 0:
        # Assume largest buy is ~25% of 5m volume for heuristic.
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

        # Soft prefilter: absolute trash is skipped even before PF logic
        liq_usd = metrics["liq_usd"]
        mcap_usd = metrics["mcap_usd"]
        age_min = metrics["token_age_min"]

        if liq_usd < SOFT_MIN_LIQ_USD:
            return None
        if mcap_usd < SOFT_MIN_MCAP_USD:
            return None
        if age_min > SOFT_MAX_AGE_MIN:
            return None

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

            # holders (not available from Dexscreener; left neutral)
            "holders": metrics["holders"],
            "holders_5m_change": metrics["holders_5m_change"],

            # whale / AI fields (some filled here, some optional later)
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

    This is fully optional:
      - If imports fail, or APIs are down, metas remain usable.
      - We cap concurrent calls + per-scan count to avoid rate-limit hell.
    """
    if not metas:
        return

    # Lazy imports to avoid hard dependency / cycles.
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
                    # threshold roughly based on PF_SCAN_MIN_WHALE_SIZE_SOL * price
                    px = float(meta.get("price_usd") or 0.0)
                    thresh_usd = float(PF_SCAN_MIN_WHALE_SIZE_SOL) * px if px > 0 else 50_000.0
                    has_whale = await is_whale_wallet(mint, usd_threshold=thresh_usd)
                    if has_whale:
                        # tilt whale metrics upward a bit so filters treat it as "has whales"
                        meta["whales_5m"] = max(1, int(meta.get("whales_5m") or 0))
                except Exception:
                    pass

    tasks = [asyncio.create_task(_handle(m)) for m in to_process]
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)


# -----------------------------------------------------------------------------
# PF-based filtering & ranking
# -----------------------------------------------------------------------------

def _filter_and_rank(metas: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Take normalized meta entries, apply is_viable_pair and scoring, and
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
            logger.debug(
                "Raydium PF filter error for %s: %s", meta.get("mint"), e
            )

    out.sort(key=lambda x: x.get("final_score", 0.0), reverse=True)
    return out[:LIMIT]


# -----------------------------------------------------------------------------
# MOMO event wiring (uses final_score / momentum_score)
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
                "pool": c.get("pair_url"),
                "age_sec": age_sec,
                "liq_usd": c.get("liq_usd"),
                "vol_5m": c.get("vol_5m_usd"),
                "up_pct": c.get("price_change_5m_pct"),
                "reason": (
                    f"raydium PF runner final={c.get('final_score', 0.0):.3f} "
                    f"momentum={c.get('momentum_score', 0.0):.3f}"
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
            headers=_headers(),
            connector=conn,
            trust_env=TRUST_ENV,
            timeout=timeout,
        )
        own_session = True

    try:
        logger.info(
            "scanner.raydium: starting scan; queries=%s soft_min_liq=$%.0f pf_min_liq=$%.0f limit=%d search_limit=%d",
            ",".join(SEARCH_QUERIES),
            SOFT_MIN_LIQ_USD,
            PF_SCAN_MIN_LIQ_USD,
            LIMIT,
            SEARCH_LIMIT,
        )
        pairs = await _fetch_pairs(sess)

        metas: List[Dict[str, Any]] = []
        for p in pairs:
            m = _normalize_to_meta(p)
            if m:
                metas.append(m)

        # Optional AI / whale enrichment (no-op if disabled or imports fail).
        with contextlib.suppress(Exception):
            await _enrich_ai_and_whales(metas)

        runners = _filter_and_rank(metas)
        logger.info(
            "scanner.raydium: %s PF runners (from %s metas, %s raw pairs)",
            len(runners),
            len(metas),
            len(pairs),
        )

        # MOMO alerts (awaited so errors get logged)
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
