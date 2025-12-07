# scanner/dexscreener_scanner.py
#
# Dexscreener scanner (Solana) – V3+++ / quant-grade
# ==================================================
#
# High-level responsibilities
# ---------------------------
#   • Fetch *live* Solana pairs from Dexscreener search.
#   • Normalize them into PF "meta" dicts compatible with utils_pkg.pair_filters.
#   • Apply:
#       - Regime-aware soft filters (liq / mcap / age / 24h volume / price).
#       - PF viability + momentum + final_score (when enabled).
#       - Optional AI fraud ("rug") scoring via ai_fraud_score.
#       - Optional whale enrichment via is_whale_wallet + smart_wallet tags.
#       - Optional AI entry scoring via feature_builder + ai_entry_logic.
#   • Rank candidates with rank_score:
#       - PF final_score + momentum + AI entry + whales + volume.
#       - Optional hard drop on high AI rug.
#       - Optional AI entry gating for runners (strict mode).
#   • Emit:
#       - MOMO_OK alerts for top runners via AlertManager.
#       - AI training rows (entry_candidates) via utils_pkg.data_logger.
#
# Public API
# ----------
#   async def scan_dexscreener(*args) -> List[Dict[str, Any]]
#
# Compatible signatures (mirrors other scanners):
#   - scan_dexscreener()
#   - scan_dexscreener(redis)
#   - scan_dexscreener(redis, session)
#   - scan_dexscreener(session)
#
# Only an aiohttp.ClientSession (if provided) is actually used.

from __future__ import annotations

import os
import re
import ssl
import time
import asyncio
import logging
import contextlib
from math import log1p
from typing import List, Dict, Any, Optional, Tuple, Set

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
if not logger.handlers:
    logging.basicConfig(level=logging.INFO)

# Optional AI training / regime helpers (soft imports)
try:
    from utils_pkg import data_logger  # type: ignore
except Exception:  # pragma: no cover
    data_logger = None  # type: ignore

# New-style regime_state (preferred)
try:  # pragma: no cover - optional
    from utils_pkg.regime import regime_state  # type: ignore
except Exception:  # pragma: no cover

    class _DummyRegime:
        @staticmethod
        def get() -> str:
            return "unknown"

        @staticmethod
        async def features() -> Dict[str, Any]:
            return {
                "regime_label_numeric": 0.0,
                "trend_score": 0.0,
                "volatility_score": 0.0,
                "liquidity_score": 0.5,
                "manipulation_risk": 0.0,
                "regime_label": "unknown",
            }

    regime_state = _DummyRegime()  # type: ignore

# Optional regime helper module (for regime_for_src)
try:
    from utils_pkg import regime as _regime  # type: ignore
except Exception:  # pragma: no cover
    _regime = None  # type: ignore

# Optional feature builder + AI entry + smart wallets
try:
    from utils_pkg import feature_builder as FB  # type: ignore
except Exception:  # pragma: no cover
    FB = None  # type: ignore

try:
    from utils_pkg import ai_entry_logic as AI_ENTRY  # type: ignore
except Exception:  # pragma: no cover
    AI_ENTRY = None  # type: ignore

try:
    from utils_pkg import smart_wallets as SW  # type: ignore
except Exception:  # pragma: no cover
    SW = None  # type: ignore


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


def _csv_set(name: str) -> Set[str]:
    raw = os.getenv(name, "")
    if not raw:
        return set()
    return {x.strip().lower() for x in raw.split(",") if x.strip()}


def _regex(name: str) -> Optional[re.Pattern]:
    pat = os.getenv(name, "")
    if not pat:
        return None
    try:
        return re.compile(pat, re.IGNORECASE)
    except Exception:
        logger.warning("Dexscreener: invalid regex in %s", name)
        return None


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
# Examples:
#   "solana"
#   "solana new"
#   "solana raydium"
SEARCH_QUERIES = [
    q.strip()
    for q in os.getenv("DEX_SEARCH_QUERIES", "solana").split(",")
    if q.strip()
]

# Honor system proxy variables if present (aiohttp trust_env)
TRUST_ENV = _b("DEX_TRUST_ENV", True)

# Soft prefilters BEFORE PF logic (liquidity / mcap / age)
_BASE_SOFT_LIQ = PF_SCAN_MIN_LIQ_USD * 0.4
_BASE_SOFT_MCAP = PF_SCAN_MIN_MCAP_USD * 0.4
SOFT_MIN_LIQ_USD = _f("DEX_SOFT_MIN_LIQ_USD", _BASE_SOFT_LIQ)
SOFT_MIN_MCAP_USD = _f("DEX_SOFT_MIN_MCAP_USD", _BASE_SOFT_MCAP)
SOFT_MAX_AGE_MIN = _f(
    "DEX_SOFT_MAX_AGE_MIN",
    PF_SCAN_MAX_TOKEN_AGE_MIN * 1.2 if PF_SCAN_MAX_TOKEN_AGE_MIN > 0 else 0.0,
)

# Extra soft filters (trades / price / volume)
SOFT_MIN_TXNS_5M = _i("DEX_SOFT_MIN_TXNS_5M", 0)
SOFT_MIN_H24_VOL_USD = _f("DEX_SOFT_MIN_H24_VOL_USD", 0.0)
SOFT_MIN_PRICE_USD = _f("DEX_SOFT_MIN_PRICE_USD", 0.0)
SOFT_MAX_PRICE_USD = _f("DEX_SOFT_MAX_PRICE_USD", 0.0)  # 0 = no max

# DEX / mint / symbol allowlists / blocklists
ALLOWED_DEX_IDS = _csv_set("DEX_ALLOWED_DEX_IDS")        # e.g. "raydium,orca"
BLOCKED_DEX_IDS = _csv_set("DEX_BLOCKED_DEX_IDS")
BLOCKED_BASE_MINTS = _csv_set("DEX_BLOCKED_BASE_MINTS")
BLOCKED_SYMBOL_RE = _regex("DEX_BLOCKED_SYMBOL_REGEX")   # e.g. "^(?i)(test|scam)"

# MOMO / alert behaviour
SEND_MOMO_ALERTS = _b("DEX_SEND_MOMO_ALERTS", True)
MAX_ALERTS_PER_SCAN = _i("DEX_MAX_ALERTS_PER_SCAN", 3)

# Optional AI / whales enrichment toggles (off by default, safe if not configured)
DEX_AI_RUG_ENABLED = _b("DEX_AI_RUG_ENABLED", False)
DEX_AI_RUG_MAX_PER_SCAN = _i("DEX_AI_RUG_MAX_PER_SCAN", 20)
DEX_MAX_AI_RUG = _f("DEX_MAX_AI_RUG", 0.88)  # hard ceiling for ruggy names
DEX_WHALES_ENABLED = _b("DEX_WHALES_ENABLED", False)

# AI entry scoring
DEX_AI_ENTRY_ENABLED = _b("DEX_AI_ENTRY_ENABLED", True)
DEX_AI_ENTRY_MAX_PER_SCAN = _i("DEX_AI_ENTRY_MAX_PER_SCAN", 60)

# AI entry guardrails (strict gating mode, similar to Pump.fun)
DEX_REQUIRE_AI_ENTRY_FOR_RUNNERS = _b("DEX_REQUIRE_AI_ENTRY_FOR_RUNNERS", False)
DEX_MIN_AI_ENTRY = _f("DEX_MIN_AI_ENTRY", 0.0)

# PF bypass / fallback behavior
DEX_BYPASS_PF = _b("DEX_BYPASS_PF", False)  # default: use PF
DEX_ENABLE_FALLBACK = _b("DEX_ENABLE_FALLBACK", True)

# Global rank_score guardrail
DEX_MIN_RANK_SCORE = _f("DEX_MIN_RANK_SCORE", 0.0)

# Fallback tuning (when PF yields nothing or bypassed)
DEX_FB_MIN_BUYS_5M = _i("DEX_FB_MIN_BUYS_5M", 1)
DEX_FB_MIN_VOL5_USD = _f("DEX_FB_MIN_VOL5_USD", 300.0)
DEX_FB_MAX_RUNNERS = max(1, _i("DEX_FB_MAX_RUNNERS", 5))
DEX_FB_MAX_IMPACT_1K_BPS = _f("DEX_FB_MAX_IMPACT_1K_BPS", 1200.0)  # ~12% slippage

# Postgres / AI logging toggles
PG_ENABLED = _b("PG_ENABLED", False)
DEX_LOG_CANDIDATES = _b("DEX_LOG_CANDIDATES", False)
DEX_REGIME_TAG_ENV = _s("DEX_REGIME_TAG", "")
DEX_WALLET_TAG_ENV = _s("DEX_WALLET_TAG", "")

# Debug toggles
DEBUG_META_SAMPLE = _b("DEX_DEBUG_META_SAMPLE", True)
DEBUG_PF_RESULT = _b("DEX_DEBUG_PF_RESULT", True)


# ──────────────────────────────────────────────────────────────────────────────
# Regime snapshot + tuning
# ──────────────────────────────────────────────────────────────────────────────

async def _regime_snapshot() -> Dict[str, Any]:
    """
    Compact regime snapshot used for:
      - logging / tagging
      - dynamic threshold tuning
      - AI entry features.
    """
    try:
        feats = await regime_state.features()
        return {
            "label_numeric": float(feats.get("regime_label_numeric", 0.0)),
            "trend_score": float(feats.get("trend_score", 0.0)),
            "volatility_score": float(feats.get("volatility_score", 0.0)),
            "liquidity_score": float(feats.get("liquidity_score", 0.5)),
            "manipulation_risk": float(feats.get("manipulation_risk", 0.0)),
            "label": str(feats.get("regime_label", "")) or "unknown",
        }
    except Exception:
        try:
            label = regime_state.get()
        except Exception:
            label = "unknown"
        return {
            "label_numeric": 0.0,
            "trend_score": 0.0,
            "volatility_score": 0.0,
            "liquidity_score": 0.5,
            "manipulation_risk": 0.0,
            "label": label,
        }


def _regime_tune_thresholds(
    soft_liq: float,
    soft_mcap: float,
    fb_min_vol5: float,
    fb_max_impact_bps: float,
    regime: Dict[str, Any],
) -> Tuple[float, float, float, float]:
    """
    Very lightweight regime-aware tuning:
      - risk_on (label_numeric >= +0.7, manip < 0.6):
          loosen liq/mcap/volume and relax impact cap slightly.
      - risk_off (label_numeric <= -0.7 OR manip >= 0.8):
          tighten liq/mcap/volume and impact.
    """
    ln = float(regime.get("label_numeric", 0.0))
    manip = float(regime.get("manipulation_risk", 0.0))

    s_liq = soft_liq
    s_mcap = soft_mcap
    fb_vol = fb_min_vol5
    fb_imp = fb_max_impact_bps

    if ln >= 0.7 and manip < 0.6:
        # risk-on: slightly more permissive
        s_liq *= 0.7
        s_mcap *= 0.7
        fb_vol *= 0.7
        fb_imp *= 1.2 if fb_imp > 0 else fb_imp
    elif ln <= -0.7 or manip >= 0.8:
        # risk-off / high manip: tighter
        s_liq *= 1.4
        s_mcap *= 1.4
        fb_vol *= 1.3
        fb_imp *= 0.85 if fb_imp > 0 else fb_imp

    s_liq = max(0.0, s_liq)
    s_mcap = max(0.0, s_mcap)
    fb_vol = max(0.0, fb_vol)
    fb_imp = max(0.0, fb_imp)
    return s_liq, s_mcap, fb_vol, fb_imp


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
        diff_s = max(0.0, (time.time() - (t_ms / 1000.0)))
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


def _dex_id(pair: Dict[str, Any]) -> str:
    return (pair.get("dexId") or "").strip().lower() or "dexscreener"


def _allowed_by_dex_filters(pair: Dict[str, Any]) -> bool:
    dex_id = _dex_id(pair)
    if dex_id in BLOCKED_DEX_IDS:
        return False
    if ALLOWED_DEX_IDS and dex_id not in ALLOWED_DEX_IDS:
        return False
    return True


def _allowed_by_symbol_and_mint(metrics: Dict[str, Any]) -> bool:
    mint = (metrics.get("base_mint") or "").strip().lower()
    if mint and mint in BLOCKED_BASE_MINTS:
        return False

    sym = (metrics.get("base_symbol") or "").strip()
    if BLOCKED_SYMBOL_RE is not None and sym:
        if BLOCKED_SYMBOL_RE.search(sym):
            return False
    return True


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


async def _fetch_pairs_for_query(
    session: aiohttp.ClientSession,
    q: str,
) -> List[Dict[str, Any]]:
    """
    Fetch Solana pairs for a single search query.
    """
    url = f"{BASE}/latest/dex/search"
    data, _status, _first = await _fetch_json(session, url, params={"q": q})
    if not data:
        return []
    pairs = data.get("pairs") or []
    out: List[Dict[str, Any]] = []
    for p in pairs:
        if (p.get("chainId") or "").lower() != "solana":
            continue
        out.append(p)
    return out


async def _fetch_pairs(session: aiohttp.ClientSession) -> List[Dict[str, Any]]:
    """
    Query Dexscreener search endpoint for each configured query and merge results.
    Only Solana pairs are considered; we dedupe by pairAddress/baseToken.address.
    """
    if not SEARCH_QUERIES:
        logger.info("Dexscreener: no SEARCH_QUERIES configured → skipping")
        return []

    tasks = [asyncio.create_task(_fetch_pairs_for_query(session, q)) for q in SEARCH_QUERIES]
    all_pairs_raw_lists = await asyncio.gather(*tasks, return_exceptions=True)

    all_pairs: List[Dict[str, Any]] = []
    seen_ids: Set[str] = set()

    for result in all_pairs_raw_lists:
        if isinstance(result, Exception):
            logger.warning("Dexscreener: query task error: %s", result)
            continue
        for p in result:
            if not _allowed_by_dex_filters(p):
                continue
            pid = p.get("pairAddress") or ((p.get("baseToken") or {}).get("address"))
            if pid:
                pid = str(pid).strip()
                if pid not in seen_ids:
                    seen_ids.add(pid)
                    all_pairs.append(p)

    logger.info("Dexscreener: fetched %d raw Solana pairs after dedupe", len(all_pairs))
    return all_pairs


# ──────────────────────────────────────────────────────────────────────────────
# Metrics / normalization → PF meta shape
# ──────────────────────────────────────────────────────────────────────────────

def _extract_metrics(pair: Dict[str, Any]) -> Dict[str, Any]:
    """
    Pull out metrics in the shape that utils_pkg.pair_filters expects.
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

    # Whales: approximate from 5m volume + price.
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

    impact_1k_bps = _nz_float(
        pair.get("priceImpact1kBps")
        or pair.get("impact1kBps")
        or pair.get("impactBps"),
        0.0,
    )

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
        "impact_1k_bps": impact_1k_bps,
        # Extra useful metadata for features
        "dex_id": _dex_id(pair),
        "h24_volume_usd": _nz_float((pair.get("volume") or {}).get("h24"), 0.0),
    }


def _normalize_to_meta(
    pair: Dict[str, Any],
    *,
    soft_liq: float,
    soft_mcap: float,
    soft_max_age: float,
    regime: Dict[str, Any],
) -> Optional[Dict[str, Any]]:
    """
    Normalize a Dexscreener pair → PF-compatible "meta" dict.
    Applies regime-aware soft prefilters (liq/mcap/age) + price/volume/trades.
    """
    try:
        if (pair.get("chainId") or "").lower() != "solana":
            return None

        metrics = _extract_metrics(pair)

        # Extra blocklists
        if not _allowed_by_symbol_and_mint(metrics):
            return None

        # Soft prefilters: liquidity / mcap / age
        liq_usd = metrics["liq_usd"]
        mcap_usd = metrics["mcap_usd"]
        token_age_min = metrics["token_age_min"]

        if soft_liq > 0 and liq_usd < soft_liq:
            return None
        if soft_mcap > 0 and mcap_usd < soft_mcap:
            return None
        if soft_max_age > 0 and token_age_min > soft_max_age:
            return None

        # Extra soft prefilters: trades / price / 24h volume
        txns5 = metrics["buys_5m"] + metrics["sells_5m"]
        if SOFT_MIN_TXNS_5M > 0 and txns5 < SOFT_MIN_TXNS_5M:
            return None

        price_usd = metrics["price_usd"]
        if SOFT_MIN_PRICE_USD > 0 and price_usd < SOFT_MIN_PRICE_USD:
            return None
        if SOFT_MAX_PRICE_USD > 0 and price_usd > SOFT_MAX_PRICE_USD:
            return None

        h24_vol = metrics["h24_volume_usd"]
        if SOFT_MIN_H24_VOL_USD > 0 and h24_vol < SOFT_MIN_H24_VOL_USD:
            return None

        mint = metrics["base_mint"]
        if not mint:
            return None

        symbol = metrics["base_symbol"] or mint[:6]
        dex_id = metrics["dex_id"] or "dexscreener"

        url = (
            pair.get("url")
            or pair.get("pairUrl")
            or f"https://dexscreener.com/solana/{mint}"
        )

        txns_5m = metrics["buys_5m"] + metrics["sells_5m"]
        txns_1h = txns_5m * 12  # coarse

        buy_ratio_5m = (
            metrics["buys_5m"] / max(1, metrics["buys_5m"] + metrics["sells_5m"])
        )

        # approximate 15m buy ratio from 1h or 24h activity
        buys_15m = max(0, metrics["buys_1h"] // 4) if metrics["buys_1h"] > 0 else 0
        sells_15m = max(0, metrics["sells_1h"] // 4) if metrics["sells_1h"] > 0 else 0
        if buys_15m == 0 and metrics["buys_24h"] > 0:
            buys_15m = max(1, metrics["buys_24h"] // 96)
        if sells_15m == 0 and metrics["sells_24h"] > 0:
            sells_15m = max(0, metrics["sells_24h"] // 96)
        buy_ratio_15m = buys_15m / max(1, buys_15m + sells_15m)

        cur_regime_label = regime.get("label", "unknown")
        cur_regime_numeric = float(regime.get("label_numeric", 0.0))
        cur_regime_manip = float(regime.get("manipulation_risk", 0.0))

        meta: Dict[str, Any] = {
            # identity
            "source": "dexscreener",
            "dex": dex_id,
            "chain": "solana",
            "pair_address": pair.get("pairAddress"),
            "mint": mint,
            "symbol": symbol,
            "pair_url": url,
            "scan_ts": time.time(),
            "regime": cur_regime_label,
            "regime_numeric": cur_regime_numeric,
            "regime_manipulation": cur_regime_manip,

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
            "buy_ratio_15m": buy_ratio_15m,

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
            "ai_rug_score": 0.0,        # neutral by default
            "ai_entry_score": None,     # filled by AI entry if enabled
            "ai_entry_reason": None,    # optional human-readable reason
            "rank_score": None,         # final ranking / tie-breaker score

            # security flags (no direct info here, but reserved for future)
            "is_honeypot": False,
            "trading_paused": False,
            "blacklisted": False,
            "top10_hold_pct": 0.0,
            "dev_hold_pct": 0.0,
            "mint_authority_disabled": True,
            "freeze_authority_disabled": True,

            # misc
            "impact_1k_bps": metrics["impact_1k_bps"],
            "dex_h24_volume_usd": h24_vol,
            "base_symbol": metrics["base_symbol"],
            "quote_symbol": metrics["quote_symbol"],
        }

        return meta
    except Exception as e:
        logger.debug("Dexscreener normalize error: %s", e)
        return None


# ──────────────────────────────────────────────────────────────────────────────
# Optional AI / whales / entry enrichment
# ──────────────────────────────────────────────────────────────────────────────

async def _enrich_ai_whales_entry(
    metas: List[Dict[str, Any]],
    regime: Dict[str, Any],
) -> None:
    """
    Best-effort enrichment of meta with:
      - ai_rug_score (0–1) via utils_pkg.core_utils.ai_fraud_score
      - additional whale presence via utils_pkg.core_utils.is_whale_wallet
      - smart wallet tags (if SW.enrich_meta / annotate_meta exists)
      - ai_entry_score + ai_entry_reason via feature_builder + ai_entry_logic
    """
    if not metas:
        return

    try:
        from utils_pkg.core_utils import ai_fraud_score, is_whale_wallet  # type: ignore
    except Exception:
        ai_fraud_score = None  # type: ignore
        is_whale_wallet = None  # type: ignore

    ai_conc = _i("DEX_AI_CONCURRENCY", 5)
    sem = asyncio.Semaphore(max(1, ai_conc))

    to_rug = metas[: DEX_AI_RUG_MAX_PER_SCAN] if (DEX_AI_RUG_ENABLED and ai_fraud_score) else []
    to_entry = metas[: DEX_AI_ENTRY_MAX_PER_SCAN] if (DEX_AI_ENTRY_ENABLED and FB and AI_ENTRY) else []

    async def _call(fn, *a, **k):
        if asyncio.iscoroutinefunction(fn):
            return await fn(*a, **k)
        return await asyncio.to_thread(fn, *a, **k)

    async def _handle(meta: Dict[str, Any]) -> None:
        mint = meta.get("mint")
        if not isinstance(mint, str) or not mint:
            return

        async with sem:
            # AI rug score
            if meta in to_rug and ai_fraud_score:
                try:
                    s01 = await ai_fraud_score(mint)
                    meta["ai_rug_score"] = max(0.0, min(1.0, float(s01)))
                except Exception:
                    pass

            # Whale presence (coarse, extra layer)
            if DEX_WHALES_ENABLED and is_whale_wallet:
                try:
                    px = float(meta.get("price_usd") or 0.0)
                    thresh_usd = float(PF_SCAN_MIN_WHALE_SIZE_SOL) * px if px > 0 else 50_000.0
                    has_whale = await is_whale_wallet(mint, usd_threshold=thresh_usd)
                    if has_whale:
                        meta["whales_5m"] = max(1, int(meta.get("whales_5m") or 0))
                except Exception:
                    pass

            # Smart wallet tags
            if SW is not None:
                try:
                    fn = getattr(SW, "enrich_meta", None) or getattr(SW, "annotate_meta", None)
                    if callable(fn):
                        if asyncio.iscoroutinefunction(fn):
                            await fn(meta)
                        else:
                            await asyncio.to_thread(fn, meta)
                except Exception:
                    pass

            # AI entry scoring (+ optional reason)
            if DEX_AI_ENTRY_ENABLED and FB is not None and AI_ENTRY is not None and meta in to_entry:
                try:
                    fb_fn = getattr(FB, "build_features_for_meta", None) or getattr(
                        FB, "build_features", None
                    )
                    if not callable(fb_fn):
                        return

                    feats = fb_fn(meta, regime=regime)  # type: ignore[arg-type]
                    meta["_features"] = feats

                    score = None
                    reason = None

                    # Try multiple AI entry APIs in priority order:
                    #   1) score_candidate(meta=..., features=..., regime=...)
                    #      + optional explain_candidate(...)
                    #   2) score_entry(features=..., meta=..., regime=...)
                    #   3) score_entry(features)
                    #   4) compute_entry(meta, features, regime) -> {"score":..,"reason":..}
                    score_fn = getattr(AI_ENTRY, "score_candidate", None)
                    if callable(score_fn):
                        score = await _call(
                            score_fn, meta=meta, features=feats, regime=regime
                        )
                        explain_fn = getattr(AI_ENTRY, "explain_candidate", None)
                        if callable(explain_fn):
                            try:
                                reason_val = await _call(
                                    explain_fn, meta=meta, features=feats, regime=regime
                                )
                                reason = str(reason_val)
                            except Exception:
                                reason = None
                    else:
                        score_entry_fn = getattr(AI_ENTRY, "score_entry", None)
                        compute_entry_fn = getattr(AI_ENTRY, "compute_entry", None)

                        if callable(score_entry_fn):
                            try:
                                score = await _call(
                                    score_entry_fn,
                                    feats,
                                    meta=meta,
                                    regime=regime,
                                )
                            except TypeError:
                                # Maybe signature is score_entry(features) only
                                score = await _call(score_entry_fn, feats)

                        if score is None and callable(compute_entry_fn):
                            try:
                                info = await _call(
                                    compute_entry_fn, meta, feats, regime
                                )
                                if isinstance(info, dict):
                                    score = info.get("score")
                                    reason_val = info.get("reason")
                                    if isinstance(reason_val, str):
                                        reason = reason_val
                            except Exception:
                                pass

                    if score is not None:
                        meta["ai_entry_score"] = float(score)
                    if reason is not None:
                        meta["ai_entry_reason"] = str(reason)
                except Exception:
                    pass

    tasks = [asyncio.create_task(_handle(m)) for m in metas]
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)


# ──────────────────────────────────────────────────────────────────────────────
# PF-based filtering & ranking + fallback
# ──────────────────────────────────────────────────────────────────────────────

def _pf_filter_and_rank(metas: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Take normalized meta entries, apply is_viable_pair and PF scoring, and
    return the top LIMIT candidates ranked by rank_score.

    rank_score is a blend of:
      - PF final_score
      - PF momentum_score
      - optional ai_entry_score
      - optional whale presence
      - lightly scaled by volume
      - hard drop on high ai_rug_score (when enabled)
    """
    out: List[Dict[str, Any]] = []

    for meta in metas:
        try:
            # Hard AI rug cutoff if enabled
            rug = float(meta.get("ai_rug_score") or 0.0)
            if DEX_AI_RUG_ENABLED and DEX_MAX_AI_RUG > 0 and rug >= DEX_MAX_AI_RUG:
                continue

            if not is_viable_pair(meta):
                continue

            momentum = compute_momentum_score(meta)
            final = compute_final_score(meta)

            ai_entry = meta.get("ai_entry_score")
            whales = int(meta.get("whales_5m") or 0)
            vol5 = float(meta.get("vol_5m_usd") or 0.0)

            # Optional AI entry gating (strict mode)
            if DEX_REQUIRE_AI_ENTRY_FOR_RUNNERS:
                if not isinstance(ai_entry, (int, float)):
                    continue
                if ai_entry < DEX_MIN_AI_ENTRY:
                    continue
            elif isinstance(ai_entry, (int, float)) and ai_entry < 0.0:
                ai_entry = 0.0

            # Base rank from PF
            rank = float(final)

            # small bonus for momentum + whales
            rank += 0.2 * float(momentum)
            if whales > 0:
                rank += 0.15

            # AI entry override if present
            if isinstance(ai_entry, (int, float)):
                vol_boost = max(1.0, log1p(max(vol5, 1.0)) / 5.0)
                rank = (0.6 * rank + 0.4 * float(ai_entry)) * vol_boost

            # Global rank guardrail
            if DEX_MIN_RANK_SCORE > 0.0 and rank < DEX_MIN_RANK_SCORE:
                continue

            m = dict(meta)
            m["momentum_score"] = float(momentum)
            m["final_score"] = float(final)
            m["rank_score"] = float(rank)
            out.append(m)
        except Exception as e:
            logger.debug("Dexscreener PF filter error for %s: %s", meta.get("mint"), e)

    out.sort(key=lambda x: x.get("rank_score", x.get("final_score", 0.0)), reverse=True)
    return out[:LIMIT]


def _fallback_pick_metas(
    metas: List[Dict[str, Any]],
    *,
    min_vol5_usd: float,
    max_impact_bps: float,
) -> List[Dict[str, Any]]:
    """
    Fallback runner selection when PF returns nothing or PF is bypassed:
      - require minimum 5m buys + 5m volume
      - optionally cap AI rug score
      - optional AI entry gating (same flags as PF path)
      - cap slippage using impact_1k_bps
      - sort by (whales_5m desc, vol_5m_usd desc, price_change_5m_pct desc)
    """
    if not DEX_ENABLE_FALLBACK or not metas:
        return []

    min_buys = int(DEX_FB_MIN_BUYS_5M)
    min_vol = float(min_vol5_usd)
    max_impact = float(max_impact_bps)
    ai_max = float(DEX_MAX_AI_RUG)
    fb_max = int(DEX_FB_MAX_RUNNERS)

    cands: List[Dict[str, Any]] = []
    for m in metas:
        try:
            buys5 = int(m.get("buys_5m") or 0)
            vol5 = float(m.get("vol_5m_usd") or 0.0)
            impact = float(m.get("impact_1k_bps") or 0.0)
            rug = float(m.get("ai_rug_score") or 0.0)

            if buys5 < min_buys:
                continue
            if vol5 < min_vol:
                continue
            if max_impact > 0 and impact > max_impact:
                continue
            if DEX_AI_RUG_ENABLED and ai_max > 0 and rug >= ai_max:
                continue

            # Optional AI entry gating in fallback too
            ai_entry = m.get("ai_entry_score")
            if DEX_REQUIRE_AI_ENTRY_FOR_RUNNERS:
                if not isinstance(ai_entry, (int, float)):
                    continue
                if ai_entry < DEX_MIN_AI_ENTRY:
                    continue

            cands.append(m)
        except Exception:
            continue

    if not cands:
        logger.info(
            "Dexscreener fallback: 0 metas matched (min_buys=%d min_vol=%.1f "
            "max_impact=%.1f ai_max=%.2f) from %d metas",
            min_buys,
            min_vol,
            max_impact,
            ai_max,
            len(metas),
        )
        return []

    def _fb_key(m: Dict[str, Any]) -> Tuple[int, float, float]:
        whales = int(m.get("whales_5m") or 0)
        vol5 = float(m.get("vol_5m_usd") or 0.0)
        up5 = float(m.get("price_change_5m_pct") or 0.0)
        return (whales, vol5, up5)

    cands.sort(key=_fb_key, reverse=True)
    picked = cands[:fb_max]

    logger.info(
        "Dexscreener fallback: picked %d/%d metas (min_buys=%d min_vol=%.1f "
        "max_impact=%.1f ai_max=%.2f)",
        len(picked),
        len(cands),
        min_buys,
        min_vol,
        max_impact,
        ai_max,
    )
    return picked


# ──────────────────────────────────────────────────────────────────────────────
# MOMO + AI logging wiring
# ──────────────────────────────────────────────────────────────────────────────

def _effective_regime_tag() -> Optional[str]:
    if DEX_REGIME_TAG_ENV:
        return DEX_REGIME_TAG_ENV
    try:
        r = regime_state.get()
        if r:
            return r
    except Exception:
        pass
    if _regime is not None and hasattr(_regime, "regime_for_src"):
        try:
            return _regime.regime_for_src("dexscreener", default="dexscreener_live")  # type: ignore[arg-type]
        except Exception:
            pass
    return "dexscreener_live"


def _effective_wallet_tag() -> Optional[str]:
    return DEX_WALLET_TAG_ENV or None


def _log_candidates_for_training(runners: List[Dict[str, Any]]) -> None:
    """
    Sync helper: write runner metas into entry_candidates for AI training.
    Safe no-op if PG is disabled or ai_db/data_logger aren't wired.

    Tries dedicated log_dexscreener_candidate if present, otherwise generic
    log_entry_candidate(src="dexscreener", ...).
    """
    if not (PG_ENABLED and DEX_LOG_CANDIDATES and data_logger and runners):
        return

    regime_tag = _effective_regime_tag()
    wallet_tag = _effective_wallet_tag()

    log_dex = getattr(data_logger, "log_dexscreener_candidate", None)
    log_entry = getattr(data_logger, "log_entry_candidate", None)

    for meta in runners:
        try:
            mint = (meta.get("mint") or "").strip()
            if not mint:
                continue
            symbol = (meta.get("symbol") or "").strip() or None

            snapshot = dict(meta)

            if callable(log_dex):
                # Dedicated Dexscreener logger (preferred if available)
                log_dex(  # type: ignore[misc]
                    mint=mint,
                    symbol=symbol,
                    meta=snapshot,
                    regime=regime_tag,
                    wallet_tag=wallet_tag,
                )
            elif callable(log_entry):
                # Generic entry_candidates logger
                log_entry(  # type: ignore[misc]
                    src="dexscreener",
                    mint=mint,
                    symbol=symbol,
                    snapshot=snapshot,
                    regime=regime_tag,
                    wallet_tag=wallet_tag,
                )
        except Exception:
            logger.exception(
                "Dexscreener: failed to log entry_candidate for mint=%s",
                meta.get("mint"),
            )


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
                "pool": c.get("dex") or "dexscreener",  # symbolic pool id
                "pool_url": c.get("pair_url"),
                "age_sec": age_sec,
                "liq_usd": c.get("liq_usd"),
                "vol_5m": c.get("vol_5m_usd"),
                "up_pct": c.get("price_change_5m_pct"),
                "ai_entry_score": c.get("ai_entry_score"),
                "ai_entry_reason": c.get("ai_entry_reason"),
                "ai_rug_score": c.get("ai_rug_score"),
                "rank_score": c.get("rank_score"),
                "reason": (
                    f"dexscreener runner liq=${c.get('liq_usd', 0):.0f} "
                    f"vol5=${c.get('vol_5m_usd', 0):.0f} "
                    f"up5={c.get('price_change_5m_pct', 0):.2f}% "
                    f"ai={c.get('ai_entry_score', 0)!r}"
                ),
            }
            await am.send_momo_event("MOMO_OK", ctx)
        except Exception as e:
            logger.warning("Dexscreener: MOMO send error for %s: %s", c.get("mint"), e)


# ──────────────────────────────────────────────────────────────────────────────
# Core scan
# ──────────────────────────────────────────────────────────────────────────────

async def _scan_inner(
    session: aiohttp.ClientSession,
    *,
    soft_liq: float,
    soft_mcap: float,
    soft_max_age: float,
    fb_min_vol5: float,
    fb_max_impact_bps: float,
    regime: Dict[str, Any],
) -> List[Dict[str, Any]]:
    logger.info(
        "🔎 Dexscreener scan: queries=%s soft_min_liq=$%.0f soft_min_mcap=$%.0f "
        "pf_min_liq=$%.0f pf_min_mcap=$%.0f limit=%d bypass_pf=%s fallback=%s "
        "regime_label=%.2f manip=%.2f tuned_soft_liq=$%.0f tuned_soft_mcap=$%.0f "
        "tuned_fb_min_vol5=$%.0f tuned_fb_max_impact=%.0f",
        ",".join(SEARCH_QUERIES),
        SOFT_MIN_LIQ_USD,
        SOFT_MIN_MCAP_USD,
        PF_SCAN_MIN_LIQ_USD,
        PF_SCAN_MIN_MCAP_USD,
        LIMIT,
        DEX_BYPASS_PF,
        DEX_ENABLE_FALLBACK,
        regime.get("label_numeric", 0.0),
        regime.get("manipulation_risk", 0.0),
        soft_liq,
        soft_mcap,
        fb_min_vol5,
        fb_max_impact_bps,
    )

    pairs = await _fetch_pairs(session)

    metas: List[Dict[str, Any]] = []
    for p in pairs:
        m = _normalize_to_meta(
            p,
            soft_liq=soft_liq,
            soft_mcap=soft_mcap,
            soft_max_age=soft_max_age,
            regime=regime,
        )
        if m:
            metas.append(m)

    if DEBUG_META_SAMPLE:
        if metas:
            sample = [
                {
                    "sym": m.get("symbol"),
                    "dex": m.get("dex"),
                    "liq": m.get("liq_usd"),
                    "vol5": m.get("vol_5m_usd"),
                    "buys5": m.get("buys_5m"),
                    "sells5": m.get("sells_5m"),
                    "chg5": m.get("price_change_5m_pct"),
                    "age_min": m.get("token_age_min"),
                }
                for m in metas[:5]
            ]
            logger.info(
                "DEX_META_SAMPLE count=%d first=%s",
                len(metas),
                sample,
            )
        else:
            logger.info("DEX_META_SAMPLE count=0")

    if not metas:
        logger.info("Dexscreener: no metas after normalization")
        return []

    # Optional AI / whales / AI entry enrichment
    try:
        await _enrich_ai_whales_entry(metas, regime=regime)
    except Exception:
        pass

    pf_runners: List[Dict[str, Any]] = []
    if not DEX_BYPASS_PF:
        pf_runners = _pf_filter_and_rank(metas)

    fb_runners: List[Dict[str, Any]] = []
    if pf_runners:
        runners = pf_runners
    else:
        fb_runners = _fallback_pick_metas(
            metas,
            min_vol5_usd=fb_min_vol5,
            max_impact_bps=fb_max_impact_bps,
        )
        runners = fb_runners

    if DEBUG_PF_RESULT:
        logger.info(
            "DEX_PF_RESULT pf=%d fb=%d metas=%d pairs=%d",
            len(pf_runners),
            len(fb_runners),
            len(metas),
            len(pairs),
        )

    logger.info(
        "Dexscreener: %d runners returned (pf=%d fb=%d metas=%d raw_pairs=%d)",
        len(runners),
        len(pf_runners),
        len(fb_runners),
        len(metas),
        len(pairs),
    )
    return runners


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
    Returns a list of runner candidates (each is a meta dict).
    """
    sess: Optional[aiohttp.ClientSession] = None
    if len(args) >= 2 and isinstance(args[1], aiohttp.ClientSession):
        sess = args[1]
    elif len(args) == 1 and isinstance(args[0], aiohttp.ClientSession):
        sess = args[0]

    # Regime snapshot once per scan + tuning
    regime = await _regime_snapshot()
    tuned_soft_liq, tuned_soft_mcap, tuned_fb_min_vol5, tuned_fb_max_impact = _regime_tune_thresholds(
        SOFT_MIN_LIQ_USD,
        SOFT_MIN_MCAP_USD,
        DEX_FB_MIN_VOL5_USD,
        DEX_FB_MAX_IMPACT_1K_BPS,
        regime,
    )

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
        runners = await _scan_inner(
            sess,
            soft_liq=tuned_soft_liq,
            soft_mcap=tuned_soft_mcap,
            soft_max_age=SOFT_MAX_AGE_MIN,
            fb_min_vol5=tuned_fb_min_vol5,
            fb_max_impact_bps=tuned_fb_max_impact,
            regime=regime,
        ) or []
        await _send_momo_for_runners(runners)
        _log_candidates_for_training(runners)
        logger.info("Dexscreener: %d final runner candidates", len(runners))
        return runners
    except Exception as e:
        logger.warning("Dexscreener scan error: %s", e)
        return []
    finally:
        if own_session and sess is not None:
            with contextlib.suppress(Exception):
                await sess.close()


__all__ = ["scan_dexscreener"]
