# scanner/raydium_scanner.py
#
# Raydium scanner via Dexscreener — V3+++ quant-grade version, wired into
# utils_pkg.scoring_service.
#
# Responsibilities:
#   - Pull Raydium pairs from Dexscreener.
#   - Normalize into "meta" dicts.
#   - Soft prefilters (liq / age) + optional AI rug / whales / smart wallets.
#   - Call scoring_service.score_candidate() for top activity pools.
#   - Gate by:
#       * combined score
#       * label (RED/YELLOW/GREEN)
#       * AI entry / AI rug
#       * risk manager (capacity / shutdown)
#   - Emit:
#       * MOMO_OK alerts for top runners.
#       * AI training rows via log_raydium_candidate & log_scored_token.
#       * scan summaries into data_logger.log_event.
#
# Public API (flexible signature):
#   async def scan_raydium(*args) -> List[Dict[str, Any]]
#
#   - scan_raydium()
#   - scan_raydium(redis)
#   - scan_raydium(redis, session)
#   - scan_raydium(session)

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
    PF_SCAN_MIN_MCAP_USD,  # kept for compatibility / future use
    PF_SCAN_MAX_TOKEN_AGE_MIN,
)

# Optional whale-size hint; safe fallback if not present
try:
    from config import PF_SCAN_MIN_WHALE_SIZE_SOL  # type: ignore
except Exception:  # pragma: no cover
    PF_SCAN_MIN_WHALE_SIZE_SOL = 3.0  # reasonable default

from utils_pkg.alert_manager import AlertManager

# PF gating only; scoring_service does heavy lifting now
try:
    from utils_pkg.pair_filters import is_viable_pair  # type: ignore
except Exception:  # pragma: no cover
    def is_viable_pair(_meta: Dict[str, Any]) -> bool:  # type: ignore
        return True

# Unified quant-grade scoring layer
try:
    from utils_pkg.scoring_service import score_candidate, ScoredToken  # type: ignore
except Exception:  # pragma: no cover
    score_candidate = None  # type: ignore
    ScoredToken = None  # type: ignore

# Structured logging for AI / analytics
try:
    from utils_pkg.data_logger import log_raydium_candidate, log_event  # type: ignore
except Exception:  # pragma: no cover
    def log_raydium_candidate(*_: Any, **__: Any) -> None:  # type: ignore
        return None

    async def log_event(*_: Any, **__: Any) -> None:  # type: ignore
        return None

# Optional scored-token logger (quant-grade archives)
try:
    from utils_pkg.data_logger import log_scored_token  # type: ignore
except Exception:  # pragma: no cover
    def log_scored_token(*_: Any, **__: Any) -> None:  # type: ignore
        return None

# Regime state (risk_on / normal / risk_off); safe dummy fallback
try:  # pragma: no cover - optional safety
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

# Risk manager snapshot (capacity / shutdown); safe dummy fallback
try:
    from utils_pkg.risk_manager import risk_capacity_snapshot  # type: ignore
except Exception:  # pragma: no cover

    async def risk_capacity_snapshot(_redis=None) -> Dict[str, Any]:
        return {
            "risk_capacity": 1.0,  # 0..1 fraction of "allowed risk"
            "risk_shutdown": False,  # if True → exit early
            "risk_mode": "normal",  # "panic", "cooldown", etc.
        }

logger = logging.getLogger("scanner.raydium")
if not logger.handlers:
    logging.basicConfig(level=logging.INFO)


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
# Config / env
# -----------------------------------------------------------------------------

UA = _s(
    "SCANNER_UA",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) SniperBot/1.0 (+raydium)",
)
BASE = _s("DEXSCREENER_BASE_URL", "https://api.dexscreener.com").rstrip("/")

# Dexscreener search limit per query (higher = more raw pairs to evaluate)
SEARCH_LIMIT = max(50, _i("RAYDIUM_SEARCH_LIMIT", 250))

# --- Scoring-service integration ---
SCORING_ENABLED = _b("RAYDIUM_SCORING_ENABLED", True)
SCORING_MAX_PER_SCAN = max(5, _i("RAYDIUM_SCORING_MAX_PER_SCAN", 80))
SCORING_CONCURRENCY = max(1, _i("RAYDIUM_SCORING_CONCURRENCY", 6))
SCORING_MIN_COMBINED = _f("RAYDIUM_SCORING_MIN_COMBINED", 55.0)
SCORING_MIN_LABEL = _s("RAYDIUM_SCORING_MIN_LABEL", "YELLOW").upper().strip()
SCORING_USE_PF_GATING = _b("RAYDIUM_SCORING_USE_PF_GATING", True)

# Legacy AI-entry envs (still honored as extra gates)
AI_ENTRY_ENV_MIN = _f("RAYDIUM_MIN_AI_ENTRY", 0.0)
RAYDIUM_REQUIRE_AI_ENTRY_FOR_RUNNERS = _b(
    "RAYDIUM_REQUIRE_AI_ENTRY_FOR_RUNNERS",
    False,
)

# Raydium runner caps
LIMIT = _i("RAYDIUM_LIMIT", 40)
RAYDIUM_MAX_RUNNERS = _i("RAYDIUM_MAX_RUNNERS", LIMIT)

TIMEOUT_S = _f("RAYDIUM_HTTP_TIMEOUT_SEC", 12.0)
RETRIES = max(1, _i("RAYDIUM_HTTP_RETRIES", 3))
BACKOFF_S = max(0.1, _f("RAYDIUM_HTTP_BACKOFF_SEC", 0.6))
TRUST_ENV = _b("RAYDIUM_TRUST_ENV", True)

# MOMO / alert behaviour
SEND_MOMO_ALERTS = _b("RAYDIUM_SEND_MOMO_ALERTS", True)
MAX_ALERTS_PER_SCAN = _i("RAYDIUM_MAX_ALERTS_PER_SCAN", 3)

# Optional AI rug / whales enrichment toggles.
AI_RUG_ENABLED = _b("RAYDIUM_AI_RUG_ENABLED", False)
AI_RUG_MAX_PER_SCAN = _i("RAYDIUM_AI_RUG_MAX_PER_SCAN", 15)
RAYDIUM_MAX_AI_RUG = _f("RAYDIUM_MAX_AI_RUG", 0.88)

WHALES_ENABLED = _b("RAYDIUM_WHALES_ENABLED", False)

# Risk-aware output shrink (0..1 multiplier on runner count)
RAYDIUM_RISK_SHRINK_ENABLED = _b("RAYDIUM_RISK_SHRINK_ENABLED", True)

# Debug / diagnostics
DEBUG_META_SAMPLE = _b("RAYDIUM_DEBUG_META_SAMPLE", True)
DEBUG_SCORE_RESULT = _b("RAYDIUM_DEBUG_SCORE_RESULT", True)

# Diagnostic scan summary logging → data_logger.log_event("raydium_scan_summary", ...)
RAYDIUM_LOG_SCAN_SUMMARY = _b("RAYDIUM_LOG_SCAN_SUMMARY", True)

# You can override the search queries; all results are filtered to dexId=raydium anyway.
SEARCH_QUERIES = [
    q.strip()
    for q in os.getenv("RAYDIUM_SEARCH_QUERIES", "raydium,solana").split(",")
    if q.strip()
]

# Soft prefilters BEFORE scoring (to cut truly dead dust but let more pass)
_BASE_SOFT_LIQ_DEFAULT = max(0.0, PF_SCAN_MIN_LIQ_USD * 0.25)
_BASE_SOFT_MAX_AGE_DEFAULT = (
    PF_SCAN_MAX_TOKEN_AGE_MIN * 1.5 if PF_SCAN_MAX_TOKEN_AGE_MIN > 0 else 0.0
)

SOFT_MIN_LIQ_USD = _f("RAYDIUM_SOFT_MIN_LIQ_USD", _BASE_SOFT_LIQ_DEFAULT)
SOFT_MAX_AGE_MIN = _f("RAYDIUM_SOFT_MAX_AGE_MIN", _BASE_SOFT_MAX_AGE_DEFAULT)

# Hard Raydium age cap (in minutes) – configurable (e.g. 1440)
RAY_MAX_AGE_MIN = _f("RAY_MAX_AGE_MIN", 0.0)  # 0 = no extra cap

# AI logging options
RAYDIUM_LOG_CANDIDATES = _b("RAYDIUM_LOG_CANDIDATES", True)
RAYDIUM_REGIME_TAG = _s("RAYDIUM_REGIME_TAG", "raydium_live")
RAYDIUM_WALLET_TAG = _s("RAYDIUM_WALLET_TAG", "")


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


async def _regime_snapshot() -> Dict[str, Any]:
    """
    Pull a compact regime snapshot used for:
      - logging / tagging
      - dynamic threshold tuning (risk-on vs risk-off).
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
    base_soft_liq: float,
    base_min_vol5: float,
    regime: Dict[str, Any],
) -> Tuple[float, float]:
    """
    Very lightweight regime-aware tuning:
      - risk_on (label_numeric >= +0.7): loosen liq / vol thresholds slightly.
      - risk_off (label_numeric <= -0.7): tighten liq / vol thresholds.
    """
    ln = float(regime.get("label_numeric", 0.0))
    manip = float(regime.get("manipulation_risk", 0.0))

    soft_liq = base_soft_liq
    min_vol5 = base_min_vol5

    if ln >= 0.7 and manip < 0.6:
        soft_liq *= 0.7
        min_vol5 *= 0.7
    elif ln <= -0.7 or manip >= 0.8:
        soft_liq *= 1.4
        min_vol5 *= 1.3

    soft_liq = max(0.0, soft_liq)
    min_vol5 = max(0.0, min_vol5)
    return soft_liq, min_vol5


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
# Metric extraction & normalization ⇒ meta
# -----------------------------------------------------------------------------

def _extract_metrics(pair: Dict[str, Any]) -> Dict[str, Any]:
    """
    Extract raw numeric metrics from a Dexscreener Raydium pair and map them
    into scanner-level meta fields.
    """
    liq = pair.get("liquidity") or {}
    liq_usd = _nz_float(liq.get("usd"), 0.0)

    fdv_usd = _nz_float(pair.get("fdv"), 0.0)
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
    scoring_service expects (and our scanners / traders can consume).
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

        ts = time.time()

        meta: Dict[str, Any] = {
            # identity / source
            "dex": "raydium",
            "chain": "SOL",
            "source": "raydium",
            "scanner": "raydium_scanner",
            "mint": mint,
            "symbol": symbol,
            "pair_url": url,
            "scan_ts": ts,
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
            "ai_rug_score": 0.0,      # updated by AI if enabled
            "rank_score": None,       # final ranking / tie-breaker score
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
# Optional AI rug / whales / entry enrichment (no scoring here)
# -----------------------------------------------------------------------------

async def _enrich_ai_and_whales(metas: List[Dict[str, Any]]) -> None:
    """
    Best-effort enrichment of meta with:
      - ai_rug_score (0–1) via utils_pkg.core_utils.ai_fraud_score
      - basic whale signal via utils_pkg.core_utils.is_whale_wallet
      - smart wallet tags if utils_pkg.smart_wallets is present
    """
    if not metas:
        return

    try:
        from utils_pkg.core_utils import ai_fraud_score, is_whale_wallet  # type: ignore
    except Exception:  # pragma: no cover
        ai_fraud_score = None  # type: ignore
        is_whale_wallet = None  # type: ignore

    try:
        from utils_pkg import smart_wallets as SW  # type: ignore
    except Exception:  # pragma: no cover
        SW = None  # type: ignore

    ai_targets = metas[: AI_RUG_MAX_PER_SCAN] if (AI_RUG_ENABLED and ai_fraud_score) else []
    whale_targets = metas if (WHALES_ENABLED and is_whale_wallet) else []

    conc = max(1, _i("RAYDIUM_AI_CONCURRENCY", 5))
    sem = asyncio.Semaphore(conc)

    async def _handle(meta: Dict[str, Any]) -> None:
        mint = meta.get("mint")
        if not isinstance(mint, str) or not mint:
            return

        async with sem:
            # AI rug
            if meta in ai_targets and ai_fraud_score:
                try:
                    s01 = await ai_fraud_score(mint)
                    if s01 is not None:
                        meta["ai_rug_score"] = max(0.0, min(1.0, float(s01)))
                except Exception:
                    pass

            # whales
            if meta in whale_targets and is_whale_wallet:
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

            # smart wallets
            if SW is not None:
                try:
                    fn = getattr(SW, "enrich_meta", None) or getattr(
                        SW, "annotate_meta", None
                    )
                    if callable(fn):
                        if asyncio.iscoroutinefunction(fn):
                            await fn(meta)
                        else:
                            await asyncio.to_thread(fn, meta)
                except Exception:
                    pass

    tasks = [asyncio.create_task(_handle(m)) for m in metas]
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)


# -----------------------------------------------------------------------------
# Scoring helper (via scoring_service)
# -----------------------------------------------------------------------------

_LABEL_LEVEL = {"RED": 0, "YELLOW": 1, "GREEN": 2}
_REQUIRED_LABEL_LEVEL = _LABEL_LEVEL.get(SCORING_MIN_LABEL, 0)


def _ai_entry_threshold_0_100() -> float:
    """
    Translate legacy AI-entry env threshold (0–1 or 0–100) into 0–100 scale.
    """
    raw = float(AI_ENTRY_ENV_MIN or 0.0)
    if raw <= 0.0:
        return 0.0
    if raw <= 1.0:
        return raw * 100.0
    return raw


async def _score_metas_with_service(
    metas: List[Dict[str, Any]],
    regime: Dict[str, Any],
    risk_ctx: Optional[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """
    Central scoring integration:

      - Optionally gate via is_viable_pair (cheap PF check).
      - Skip anything already flagged as high rug.
      - Sort by (vol_5m, buys_5m) and score top N.
      - Attach ScoredToken fields to meta.
    """
    if not SCORING_ENABLED or not metas or score_candidate is None:
        return []

    # PF gating (cheap) before we spend AI / feature budget
    pf_filtered: List[Dict[str, Any]] = []
    for m in metas:
        try:
            if SCORING_USE_PF_GATING and not is_viable_pair(m):
                continue
            rug = float(m.get("ai_rug_score") or 0.0)
            if AI_RUG_ENABLED and rug >= RAYDIUM_MAX_AI_RUG:
                continue
            pf_filtered.append(m)
        except Exception:
            continue

    if not pf_filtered:
        pf_filtered = metas

    # Sort so highest activity pools get scored first
    def _activity_key(m: Dict[str, Any]) -> Tuple[float, int]:
        return (
            float(m.get("vol_5m_usd") or 0.0),
            int(m.get("buys_5m") or 0),
        )

    pf_filtered.sort(key=_activity_key, reverse=True)
    targets = pf_filtered[:SCORING_MAX_PER_SCAN]

    conc = SCORING_CONCURRENCY
    sem = asyncio.Semaphore(conc)

    scored: List[Dict[str, Any]] = []

    async def _score_one(meta: Dict[str, Any]) -> None:
        raw = dict(meta)

        # Aliases for backwards compat / nice scoring_service view
        if "token_age_min" in raw and "age_min" not in raw:
            raw["age_min"] = raw["token_age_min"]
        if "price_change_5m_pct" in raw and "chg5" not in raw:
            raw["chg5"] = raw["price_change_5m_pct"]
        if "buys_5m" in raw and "buys5" not in raw:
            raw["buys5"] = raw["buys_5m"]
        if "sells_5m" in raw and "sells5" not in raw:
            raw["sells5"] = raw["sells_5m"]
        if not raw.get("chain"):
            raw["chain"] = "SOL"

        ctx = {
            "source": "raydium",
            "scanner": "raydium_scanner",
            "regime": dict(regime),
            "risk": dict(risk_ctx or {}),
        }

        async with sem:
            try:
                st: ScoredToken = await score_candidate(raw, context=ctx)  # type: ignore
            except Exception as exc:
                logger.warning(
                    "Raydium scoring error for %s: %s", meta.get("mint"), exc
                )
                return

        m = dict(meta)
        m["score_combined"] = float(st.combined_score)
        m["score_label"] = st.label
        m["score_momentum"] = float(st.momentum_score)
        m["score_risk"] = float(st.risk_score)
        m["score_quality"] = float(st.quality_score)
        m["score_ai"] = float(st.ai_score)
        m["score_reasons"] = st.reasons
        m["score_is_trade_viable"] = bool(st.is_trade_viable)
        m["score_trade_gate_reason"] = st.trade_gate_reason

        # Back-compat aliases
        m["rank_score"] = float(st.combined_score)
        m["ai_entry_score"] = float(st.ai_score)
        m["ai_entry_reason"] = st.reasons

        # Whole ScoredToken snapshot (dict) for downstream / logging
        m["scored_token"] = st.to_dict()
        scored.append(m)

        # Optional scored-token DB log
        try:
            log_scored_token(st.to_dict(), source="raydium")  # type: ignore
        except TypeError:
            try:
                log_scored_token(st.to_dict())  # type: ignore
            except Exception:
                pass

    tasks = [asyncio.create_task(_score_one(m)) for m in targets]
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)
    return scored


# -----------------------------------------------------------------------------
# Fallback picker (if scoring yields nothing)
# -----------------------------------------------------------------------------

def _fallback_pick_metas(
    metas: List[Dict[str, Any]],
    *,
    why: str = "fb",
    fb_min_vol5_usd: Optional[float] = None,
) -> List[Dict[str, Any]]:
    """
    Very simple, permissive Raydium fallback:
    - scoring_service is skipped (upstream age/liq filters already applied)
    - keep anything with decent 5m volume + at least 1 buy
    - skip anything with ai_rug_score above RAYDIUM_MAX_AI_RUG
    - sort by (whales_5m desc, vol_5m_usd desc) and cap by env limit
    """
    if not metas:
        return []

    fb_min_buys = int(_i("RAYDIUM_FB_MIN_BUYS_5M", 1))
    fb_min_vol = float(
        fb_min_vol5_usd
        if fb_min_vol5_usd is not None
        else _f("RAYDIUM_FB_MIN_VOL5_USD", 300.0)
    )
    fb_max = int(_i("RAYDIUM_FB_MAX_RUNNERS", 3))

    candidates: List[Dict[str, Any]] = []
    for m in metas:
        try:
            buys5 = int(m.get("buys_5m") or 0)
            vol5 = float(m.get("vol_5m_usd") or 0.0)
            rug = float(m.get("ai_rug_score") or 0.0)

            if buys5 < fb_min_buys:
                continue
            if vol5 < fb_min_vol:
                continue
            # Hard skip if AI says "very likely rug"
            if AI_RUG_ENABLED and rug >= RAYDIUM_MAX_AI_RUG:
                continue

            candidates.append(m)
        except Exception:
            continue

    if not candidates:
        logger.info(
            "Raydium fallback[%s]: 0 metas matched (fb_min_buys=%d fb_min_vol=%.1f "
            "ai_rug_max=%.2f) from %d metas",
            why,
            fb_min_buys,
            fb_min_vol,
            RAYDIUM_MAX_AI_RUG,
            len(metas),
        )
        return []

    def _fb_key(m: Dict[str, Any]) -> Tuple[int, float]:
        whales = int(m.get("whales_5m") or 0)
        vol5 = float(m.get("vol_5m_usd") or 0.0)
        return (whales, vol5)

    candidates.sort(key=_fb_key, reverse=True)
    picked = candidates[:fb_max]

    logger.info(
        "Raydium fallback[%s]: picked %d/%d metas (fb_min_buys=%d fb_min_vol=%.1f "
        "fb_max=%d ai_rug_max=%.2f)",
        why,
        len(picked),
        len(candidates),
        fb_min_buys,
        fb_min_vol,
        fb_max,
        RAYDIUM_MAX_AI_RUG,
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
                # expose scoring-service signals in MOMO context
                "score_combined": c.get("score_combined"),
                "score_label": c.get("score_label"),
                "score_ai": c.get("score_ai"),
                "score_risk": c.get("score_risk"),
                "score_quality": c.get("score_quality"),
                "score_momentum": c.get("score_momentum"),
                "ai_rug_score": c.get("ai_rug_score"),
                "rank_score": c.get("rank_score"),
                "risk_capacity": c.get("risk_capacity"),
                "reason": (
                    f"raydium runner liq=${c.get('liq_usd', 0):.0f} "
                    f"vol5=${c.get('vol_5m_usd', 0):.0f} "
                    f"up5={c.get('price_change_5m_pct', 0):.2f}% "
                    f"score={c.get('score_combined', 0):.1f} "
                    f"label={c.get('score_label')}"
                ),
            }
            await am.send_momo_event("MOMO_OK", ctx)
        except Exception as e:
            logger.warning(
                "Raydium: MOMO send error for %s: %s", c.get("mint"), e
            )


# -----------------------------------------------------------------------------
# AI logging wiring (uses runners list)
# -----------------------------------------------------------------------------

def _log_ai_candidates(
    runners: List[Dict[str, Any]],
    regime: Dict[str, Any],
    risk_ctx: Optional[Dict[str, Any]],
) -> None:
    """
    Log runner candidates into Postgres via utils_pkg.data_logger.

    This builds a meta snapshot for each runner and calls
    log_raydium_candidate(...). Any failures are logged but ignored
    so scanning is never blocked by DB issues.
    """
    if not RAYDIUM_LOG_CANDIDATES or not runners:
        return

    # Prefer live regime tag if available
    try:
        live_regime = regime.get("label") or regime_state.get()
    except Exception:
        live_regime = None

    regime_tag = live_regime or RAYDIUM_REGIME_TAG or "raydium_live"
    wallet_tag = RAYDIUM_WALLET_TAG or None

    risk_capacity = None
    risk_mode = None
    if risk_ctx:
        risk_capacity = float(risk_ctx.get("risk_capacity", 1.0) or 1.0)
        risk_mode = risk_ctx.get("risk_mode", None)

    for r in runners:
        try:
            mint = r.get("mint")
            if not isinstance(mint, str) or not mint:
                continue

            meta = {
                "liq": float(r.get("liq_usd") or 0.0),
                "vol5": float(r.get("vol_5m_usd") or 0.0),
                "buys5": int(r.get("buys_5m") or 0),
                "sells5": int(r.get("sells_5m") or 0),
                "chg5": float(r.get("price_change_5m_pct") or 0.0),
                "age_min": float(r.get("token_age_min") or 0.0),
                "scan_ts": float(r.get("scan_ts") or time.time()),
            }

            if "mcap_usd" in r:
                meta["mcap"] = float(r.get("mcap_usd") or 0.0)
            if "vol_1h_usd" in r:
                meta["vol1"] = float(r.get("vol_1h_usd") or 0.0)
            if "buys_1h" in r:
                meta["buys1"] = int(r.get("buys_1h") or 0)
            if "sells_1h" in r:
                meta["sells1"] = int(r.get("sells_1h") or 0)
            if "ai_rug_score" in r:
                meta["ai_rug_score"] = float(r.get("ai_rug_score") or 0.0)
            if "rank_score" in r and r["rank_score"] is not None:
                meta["rank_score"] = float(r["rank_score"])

            # scoring-service extras
            if "score_combined" in r:
                meta["score_combined"] = float(r.get("score_combined") or 0.0)
            if "score_label" in r:
                meta["score_label"] = str(r.get("score_label") or "")
            if "score_ai" in r:
                meta["score_ai"] = float(r.get("score_ai") or 0.0)
            if "score_risk" in r:
                meta["score_risk"] = float(r.get("score_risk") or 0.0)
            if "score_quality" in r:
                meta["score_quality"] = float(r.get("score_quality") or 0.0)
            if "score_momentum" in r:
                meta["score_momentum"] = float(r.get("score_momentum") or 0.0)

            if risk_capacity is not None:
                meta["risk_capacity"] = float(risk_capacity)
            if risk_mode is not None:
                meta["risk_mode"] = str(risk_mode)

            meta["raw_meta"] = dict(r)

            log_raydium_candidate(
                mint=mint,
                symbol=r.get("symbol"),
                meta=meta,
                regime=regime_tag,
                wallet_tag=wallet_tag,
            )
        except Exception:
            logger.exception(
                "Raydium: failed to log AI candidate for mint=%s", r.get("mint")
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

    Only an aiohttp.ClientSession (if provided) is used for HTTP,
    and a redis handle (if provided) is used for risk manager & logging.
    Returns a list of runner candidates (each is a meta dict enriched with
    scoring_service fields when enabled).
    """
    redis = None
    sess: Optional[aiohttp.ClientSession] = None

    # Parse args: try to detect redis vs aiohttp session
    if len(args) == 1:
        if isinstance(args[0], aiohttp.ClientSession):
            sess = args[0]
        else:
            redis = args[0]
    elif len(args) >= 2:
        redis = args[0]
        if isinstance(args[1], aiohttp.ClientSession):
            sess = args[1]

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

    regime: Dict[str, Any] = {}
    risk_ctx: Optional[Dict[str, Any]] = None
    pairs: List[Dict[str, Any]] = []
    metas: List[Dict[str, Any]] = []
    soft_metas: List[Dict[str, Any]] = []
    scored_metas: List[Dict[str, Any]] = []
    fb_metas: List[Dict[str, Any]] = []
    runners: List[Dict[str, Any]] = []

    try:
        # Risk snapshot once per scan (used for output shrink / shutdown)
        try:
            risk_ctx = await risk_capacity_snapshot(redis)
        except Exception:
            risk_ctx = None

        risk_capacity = float(
            (risk_ctx or {}).get("risk_capacity", 1.0) or 1.0
        )
        risk_shutdown = bool((risk_ctx or {}).get("risk_shutdown", False))

        if risk_shutdown or risk_capacity <= 0.0:
            logger.warning(
                "scanner.raydium: risk manager requested shutdown "
                "(risk_shutdown=%s risk_capacity=%.3f); returning no runners.",
                risk_shutdown,
                risk_capacity,
            )
            return []

        # Regime snapshot once per scan (used for tuning + logging + AI)
        regime = await _regime_snapshot()
        tuned_soft_liq, tuned_fb_min_vol5 = _regime_tune_thresholds(
            SOFT_MIN_LIQ_USD,
            _f("RAYDIUM_FB_MIN_VOL5_USD", 300.0),
            regime,
        )

        logger.info(
            "scanner.raydium: starting scan; queries=%s soft_min_liq=$%.0f "
            "pf_min_liq=$%.0f limit=%d search_limit=%d scoring_enabled=%s "
            "regime_label=%.2f manip=%.2f tuned_soft_liq=$%.0f tuned_min_vol5=$%.0f "
            "risk_capacity=%.3f risk_mode=%s",
            ",".join(SEARCH_QUERIES),
            SOFT_MIN_LIQ_USD,
            PF_SCAN_MIN_LIQ_USD,
            LIMIT,
            SEARCH_LIMIT,
            SCORING_ENABLED,
            regime.get("label_numeric", 0.0),
            regime.get("manipulation_risk", 0.0),
            tuned_soft_liq,
            tuned_fb_min_vol5,
            risk_capacity,
            (risk_ctx or {}).get("risk_mode", "normal"),
        )

        pairs = await _fetch_pairs(sess)

        for p in pairs:
            m = _normalize_to_meta(p)
            if m:
                # attach regime + risk so downstream scoring sees it too
                m["regime"] = regime.get("label", "unknown")
                m["regime_numeric"] = float(regime.get("label_numeric", 0.0))
                m["regime_manipulation"] = float(
                    regime.get("manipulation_risk", 0.0)
                )
                m["regime_ctx"] = dict(regime)
                m["risk_capacity"] = risk_capacity
                m["risk_mode"] = (risk_ctx or {}).get("risk_mode", "normal")
                metas.append(m)

        total_metas = len(metas)

        # --- soft prefilter: keep out truly dead dust but allow more than PF ---
        for m in metas:
            try:
                liq = float(m.get("liq_usd") or 0.0)
                age = float(m.get("token_age_min") or 9999.0)

                if tuned_soft_liq > 0.0 and liq < tuned_soft_liq:
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
                "falling back to raw metas for scoring.",
                total_metas,
            )
            soft_metas = metas

        # --- hard age cap using RAY_MAX_AGE_MIN (applies to scoring & fallback) ---
        if RAY_MAX_AGE_MIN > 0.0 and soft_metas:
            before = len(soft_metas)
            age_filtered: List[Dict[str, Any]] = []
            for m in soft_metas:
                age = m.get("token_age_min")
                if not isinstance(age, (int, float)):
                    # If we don't know the age, keep it (can tune later if needed)
                    age_filtered.append(m)
                elif age <= RAY_MAX_AGE_MIN:
                    age_filtered.append(m)
            soft_metas = age_filtered
            logger.info(
                "RAY_AGE_FILTER before=%d after=%d max_age_min=%.1f",
                before,
                len(soft_metas),
                RAY_MAX_AGE_MIN,
            )

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

        # Optional AI rug / whale / smart-wallet enrichment (no scoring here).
        with contextlib.suppress(Exception):
            await _enrich_ai_and_whales(soft_metas)

        # --- main quant-grade scoring path ---
        scored_metas = await _score_metas_with_service(
            soft_metas, regime=regime, risk_ctx=risk_ctx
        )

        runners = []
        ai_thr = _ai_entry_threshold_0_100()

        for m in scored_metas:
            try:
                label = str(m.get("score_label") or "").upper() or "YELLOW"
                combined = float(m.get("score_combined") or 0.0)
                ai_score = float(m.get("score_ai") or 0.0)
                trade_ok = bool(m.get("score_is_trade_viable", True))

                if not trade_ok:
                    continue
                if combined < SCORING_MIN_COMBINED:
                    continue

                lvl = _LABEL_LEVEL.get(label, 0)
                if lvl < _REQUIRED_LABEL_LEVEL:
                    continue

                if RAYDIUM_REQUIRE_AI_ENTRY_FOR_RUNNERS and ai_thr > 0.0:
                    if ai_score < ai_thr:
                        continue

                if AI_RUG_ENABLED:
                    rug = float(m.get("ai_rug_score") or 0.0)
                    if rug >= RAYDIUM_MAX_AI_RUG:
                        continue

                runners.append(m)
            except Exception:
                continue

        # --- Fallback if scoring produced 0 runners ---
        if not runners:
            fb_metas = _fallback_pick_metas(
                soft_metas, why="ray-score-fb", fb_min_vol5_usd=tuned_fb_min_vol5
            )
            runners = fb_metas

        # --- Sort + caps + risk shrink ---
        if runners:
            runners.sort(
                key=lambda x: x.get("score_combined", x.get("rank_score", 0.0)),
                reverse=True,
            )

            max_allowed = RAYDIUM_MAX_RUNNERS if RAYDIUM_MAX_RUNNERS > 0 else LIMIT
            runners = runners[:max_allowed]

            # Risk-aware shrink of runner count
            if RAYDIUM_RISK_SHRINK_ENABLED:
                raw_n = len(runners)
                # e.g. risk_capacity=0.5 → take ~50% of max, but at least 1 when >0
                allowed = int(raw_n * risk_capacity)
                if risk_capacity > 0.0 and allowed < 1:
                    allowed = 1
                if allowed < raw_n:
                    logger.info(
                        "scanner.raydium: risk shrink runners %d → %d (risk_capacity=%.3f)",
                        raw_n,
                        allowed,
                        risk_capacity,
                    )
                    runners = runners[:allowed]

        if DEBUG_SCORE_RESULT:
            logger.info(
                "RAY_SCORE_RESULT scored=%d fb=%d metas=%d pairs=%d runners=%d",
                len(scored_metas),
                len(fb_metas),
                len(soft_metas),
                len(pairs),
                len(runners),
            )

        logger.info(
            "scanner.raydium: %s runners returned (from %s metas, %s raw pairs)",
            len(runners),
            len(soft_metas),
            len(pairs),
        )

        # 🔥 Log AI training data for the runners
        _log_ai_candidates(runners, regime=regime, risk_ctx=risk_ctx)

        # 🔥 Send MOMO for whatever runners we’re actually using
        if runners:
            await _send_momo_for_runners(runners)

        # 🔍 Optional scan-level summary for dashboards / health
        if RAYDIUM_LOG_SCAN_SUMMARY:
            try:
                await log_event(
                    "raydium_scan_summary",
                    {
                        "raw_pairs": len(pairs),
                        "metas": len(metas),
                        "soft_metas": len(soft_metas),
                        "scored_metas": len(scored_metas),
                        "fb_runners": len(fb_metas),
                        "runners": len(runners),
                        "regime": dict(regime),
                        "risk": dict(risk_ctx or {}),
                        "config": {
                            "SOFT_MIN_LIQ_USD": SOFT_MIN_LIQ_USD,
                            "SOFT_MAX_AGE_MIN": SOFT_MAX_AGE_MIN,
                            "RAY_MAX_AGE_MIN": RAY_MAX_AGE_MIN,
                            "SCORING_ENABLED": SCORING_ENABLED,
                            "SCORING_MAX_PER_SCAN": SCORING_MAX_PER_SCAN,
                            "SCORING_MIN_COMBINED": SCORING_MIN_COMBINED,
                            "SCORING_MIN_LABEL": SCORING_MIN_LABEL,
                            "AI_RUG_ENABLED": AI_RUG_ENABLED,
                            "RAYDIUM_MAX_AI_RUG": RAYDIUM_MAX_AI_RUG,
                            "RAYDIUM_RISK_SHRINK_ENABLED": RAYDIUM_RISK_SHRINK_ENABLED,
                        },
                    },
                )
            except Exception:
                pass

        return runners
    except Exception as e:
        logger.warning("Raydium scan error: %s", e)
        return []
    finally:
        if own_session and sess:
            with contextlib.suppress(Exception):
                await sess.close()


__all__ = ["scan_raydium"]
