# scanner/pumpfun_scanner.py
#
# Pump.fun scanner (V3+++ / quant-grade)
# ======================================
#
# Responsibilities:
#   - Discover *live runners* on Pump.fun using multiple sources:
#       1) Playwright next-data (/_next/data) trending (apex)
#       2) Playwright DOM scrape (apex site)
#       3) JSON API + legacy + HTML __NEXT_DATA__ fallback
#   - Normalize all shapes → PF "meta" dict compatible with utils_pkg.pair_filters.
#   - Apply:
#       • Regime-aware soft filters (liq / mcap / age / slippage).
#       • Optional PF-based viability + scoring (final_score + momentum_score).
#       • Optional AI fraud ("rug") scoring (ai_fraud_score).
#       • Optional whale presence (is_whale_wallet + smart_wallet tags).
#       • Optional AI entry scoring via feature_builder + ai_entry_logic.
#   - Select runner candidates using rank_score (PF + AI + volume + whales).
#   - Emit:
#       • MOMO_OK alerts for top runners via AlertManager.
#       • AI training rows via utils_pkg.data_logger (entry_candidates / pumpfun tables).
#       • Optional scan summary via data_logger.log_event("pumpfun_scan_summary", ...).
#
# Public API (flexible signature, mirrors Raydium scanner):
#   async def scan_pumpfun(*args) -> List[Dict[str, Any]]
#
# Usage:
#   - scan_pumpfun()
#   - scan_pumpfun(redis)
#   - scan_pumpfun(redis, session)
#   - scan_pumpfun(session)
#
# Only aiohttp.ClientSession (if provided) is used.

from __future__ import annotations

import os
import re
import ssl
import json
import time
import logging
import asyncio
import contextlib
from math import log1p
from typing import Any, Dict, List, Optional, Tuple

import aiohttp
import certifi

from config import (
    PF_SCAN_MIN_LIQ_USD,
    PF_SCAN_MIN_MCAP_USD,
    PF_SCAN_MAX_TOKEN_AGE_MIN,
)

# Optional whale-size hint; safe fallback if not present.
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

LOG = logging.getLogger("scanner.pumpfun")
if not LOG.handlers:
    logging.basicConfig(level=logging.INFO)

# Optional AI training / DB logging module
try:
    from utils_pkg import data_logger  # type: ignore
except Exception:  # pragma: no cover
    data_logger = None  # type: ignore

# Optional scan-summary logger
try:
    from utils_pkg.data_logger import log_event  # type: ignore
except Exception:  # pragma: no cover

    async def log_event(*_: Any, **__: Any) -> None:  # type: ignore
        return None


# Regime state: preferred unified API
try:
    from utils_pkg.regime import regime_state  # type: ignore
except Exception:  # pragma: no cover

    class _DummyRegimeState:
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

    regime_state = _DummyRegimeState()  # type: ignore

# Optional helper for source-specific regime tags
try:
    from utils_pkg import regime as _regime  # type: ignore
except Exception:  # pragma: no cover
    _regime = None  # type: ignore


# ──────────────────────────────────────────────────────────────────────────────
# Optional Cloudflare/Playwright helpers (gracefully degrade if missing)
# ──────────────────────────────────────────────────────────────────────────────

try:
    from utils_pkg.pumpfun_cf import (  # type: ignore
        apply_cookies_to_session,              # attach per-host cookies to aiohttp session
        get_cookie_for,                        # cookie string for a given URL/host
        flag_cookie_bad,                       # mark cookie stale (e.g., on 530/403/407)
        browser_fetch_json,                    # last-resort JSON via Playwright (API host)
        browser_fetch_trending_via_next_data,  # Next.js JSON from apex (/_next/data)
        browser_scrape_trending_dom,           # DOM scrape from apex pages (no API host)
    )
except Exception:  # pragma: no cover

    def apply_cookies_to_session(_session: aiohttp.ClientSession) -> None:  # type: ignore
        return None

    def get_cookie_for(_url_or_host: str) -> str:  # type: ignore
        # allow env cookie if provided
        return (
            os.getenv("CF_PUMP_COOKIE")
            or os.getenv("PUMPFUN_COOKIE")
            or os.getenv("PUMP_COOKIE")
            or ""
        )

    def flag_cookie_bad() -> None:  # type: ignore
        return None

    async def browser_fetch_json(_url: str):  # type: ignore
        return None

    async def browser_fetch_trending_via_next_data(*, limit: int = 50):  # type: ignore
        return None

    async def browser_scrape_trending_dom(*, limit: int = 50):  # type: ignore
        return None


# ──────────────────────────────────────────────────────────────────────────────
# Optional AI entry + smart wallets (soft imports)
# ──────────────────────────────────────────────────────────────────────────────

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


# ──────────────────────────────────────────────────────────────────────────────
# Config / flags (Pump-specific on top of PF_* defaults)
# ──────────────────────────────────────────────────────────────────────────────

UA = _s(
    "PUMPFUN_UA",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126 Safari/537.36",
)

# Optional override *full URL* for debugging a single endpoint
OVERRIDE_URL = _s("PUMPFUN_OVERRIDE_URL", "")

# Optional HTTP proxy specifically for Pump.fun requests (aiohttp-compatible URI)
PUMPFUN_HTTP_PROXY = _s("PUMPFUN_HTTP_PROXY", "")

REQ_TIMEOUT_S = _i("PUMPFUN_HTTP_TIMEOUT_S", 15)
SESSION_LIMIT = _i("PUMPFUN_HTTP_CONN_LIMIT", 30)

# How many items we request from Pump.fun API / HTML sources.
API_LIMIT = _i("PUMPFUN_API_LIMIT", 20)
DOM_LIMIT = _i("PUMPFUN_DOM_LIMIT", API_LIMIT)

# Max number of final runners returned (hard cap)
PUMP_RUNNER_LIMIT = _i("PUMPFUN_RUNNER_LIMIT", API_LIMIT)

# Playwright/HTML behaviour
SKIP_NEXT = _b("PUMPFUN_SKIP_NEXT", False)          # skip Playwright-based Next/DOM
SKIP_HTML_NEXT = _b("PUMPFUN_SKIP_HTMLNEXT", False) # skip HTML __NEXT_DATA__ fallback

# MOMO / alert behaviour
SEND_MOMO_ALERTS = _b("PUMP_SEND_MOMO_ALERTS", True)
MAX_ALERTS_PER_SCAN = _i("PUMP_MAX_ALERTS_PER_SCAN", 3)

# Optional AI / whales enrichment toggles (off by default, safe if not configured)
PUMP_AI_RUG_ENABLED = _b("PUMP_AI_RUG_ENABLED", False)
PUMP_AI_RUG_MAX_PER_SCAN = _i("PUMP_AI_RUG_MAX_PER_SCAN", 15)
PUMP_WHALES_ENABLED = _b("PUMP_WHALES_ENABLED", False)

# AI entry scoring
PUMP_AI_ENTRY_ENABLED = _b("PUMP_AI_ENTRY_ENABLED", True)
PUMP_AI_ENTRY_MAX_PER_SCAN = _i("PUMP_AI_ENTRY_MAX_PER_SCAN", 60)

# AI rug & AI entry guardrails
PUMP_MAX_AI_RUG = _f("PUMP_MAX_AI_RUG", 0.88)
PUMP_REQUIRE_AI_ENTRY_FOR_RUNNERS = _b("PUMP_REQUIRE_AI_ENTRY_FOR_RUNNERS", False)
PUMP_MIN_AI_ENTRY = _f("PUMP_MIN_AI_ENTRY", 0.0)

# Global rank_score guardrail & runner cap
PUMP_MIN_RANK_SCORE = _f("PUMP_MIN_RANK_SCORE", 0.0)
PUMP_MAX_RUNNERS = _i("PUMP_MAX_RUNNERS", PUMP_RUNNER_LIMIT)

# Pump.fun HTML routes to try for __NEXT_DATA__ fallback
FALLBACK_HTML_URLS = [
    "https://pump.fun/",
    "https://pump.fun/?tab=trending",
    "https://pump.fun/?chain=sol&tab=trending",
]

# Typical CF/anti-bot and tunnel errors
_BAD_CF_CODES = {403, 407, 429, 530}
_PROXY_BREAK_CODES = {522, 523, 524, 525, 526, 530}

# Soft prefilters BEFORE full PF logic (we still use PF or fallback on top)
_BASE_SOFT_LIQ = PF_SCAN_MIN_LIQ_USD * 0.4
_BASE_SOFT_MCAP = PF_SCAN_MIN_MCAP_USD * 0.4
SOFT_MIN_LIQ_USD = _f("PUMP_SOFT_MIN_LIQ_USD", _BASE_SOFT_LIQ)
SOFT_MIN_MCAP_USD = _f("PUMP_SOFT_MIN_MCAP_USD", _BASE_SOFT_MCAP)
SOFT_MAX_AGE_MIN = _f(
    "PUMP_SOFT_MAX_AGE_MIN",
    PF_SCAN_MAX_TOKEN_AGE_MIN * 1.2 if PF_SCAN_MAX_TOKEN_AGE_MIN > 0 else 0.0,
)

# PF bypass / fallback behaviour
PUMP_BYPASS_PF = _b("PUMP_BYPASS_PF", False)          # default: use PF
PUMP_ENABLE_FALLBACK = _b("PUMP_ENABLE_FALLBACK", True)

PUMP_FB_MIN_BUYS_5M = _i("PUMP_FB_MIN_BUYS_5M", 1)
PUMP_FB_MIN_VOL5_USD = _f("PUMP_FB_MIN_VOL5_USD", 200.0)
PUMP_FB_MAX_RUNNERS = max(1, _i("PUMP_FB_MAX_RUNNERS", 6))
PUMP_FB_MAX_IMPACT_1K_BPS = _f("PUMP_FB_MAX_IMPACT_1K_BPS", 1500.0)  # ~15% impact cap

# Postgres / AI logging toggles
PG_ENABLED = _b("PG_ENABLED", False)
PUMP_LOG_CANDIDATES = _b("PUMPFUN_LOG_CANDIDATES", False)
PUMP_REGIME_TAG_ENV = _s("PUMPFUN_REGIME_TAG", "")
PUMP_WALLET_TAG_ENV = _s("PUMPFUN_WALLET_TAG", "")
PUMP_LOG_SCAN_SUMMARY = _b("PUMPFUN_LOG_SCAN_SUMMARY", True)

# Debug toggles
PUMP_DEBUG_META_SAMPLE = _b("PUMP_DEBUG_META_SAMPLE", True)
PUMP_DEBUG_PF_RESULT = _b("PUMP_DEBUG_PF_RESULT", True)


# ──────────────────────────────────────────────────────────────────────────────
# Regime snapshot + tuning
# ──────────────────────────────────────────────────────────────────────────────

async def _regime_snapshot() -> Dict[str, Any]:
    """
    Pull a compact regime snapshot used for:
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
# Helpers: API bases & headers
# ──────────────────────────────────────────────────────────────────────────────

def _api_bases() -> List[str]:
    """
    Priority-ordered list of Pump.fun API bases.
    First item can be overridden via PUMPFUN_API_BASE, then mirrors, etc.
    """
    bases: List[str] = []
    env_base = _s("PUMPFUN_API_BASE", "").rstrip("/")
    if env_base:
        bases.append(env_base)
    bases += [
        "https://frontend-api.pump-mirror.fun",
        "https://frontend-api.pump.fun",
    ]
    seen: set[str] = set()
    out: List[str] = []
    for b in bases:
        if b and b not in seen:
            seen.add(b)
            out.append(b)
    return out


def _hdrs(url: str) -> Dict[str, str]:
    h = {
        "User-Agent": UA,
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://pump.fun/trending",
        "Origin": "https://pump.fun",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
    }
    ck = get_cookie_for(url)
    if ck:
        h["Cookie"] = ck
    return h


# ──────────────────────────────────────────────────────────────────────────────
# Utilities & normalization
# ──────────────────────────────────────────────────────────────────────────────

BASE58_RE = re.compile(r"[1-9A-HJ-NP-Za-km-z]{32,44}")


def _now() -> float:
    return time.time()


def _nz(x: Any, d: float = 0.0) -> float:
    try:
        fx = float(x)
        if fx != fx:  # NaN
            return float(d)
        return fx
    except Exception:
        return float(d)


def _safe_int(x: Any, d: int = 0) -> int:
    try:
        return int(x)
    except Exception:
        return d


def _age_min(created_ts: Optional[float]) -> Optional[float]:
    try:
        if created_ts is None:
            return None
        t = float(created_ts)
        if t > 10_000_000_000:  # ms → s
            t = t / 1000.0
        return max(0.0, (_now() - t) / 60.0)
    except Exception:
        return None


def _norm_mint(val: Any) -> str:
    if not isinstance(val, str):
        return ""
    val = val.strip().replace(" ", "")
    m = BASE58_RE.search(val)
    return m.group(0) if m else ""


def _extract_metrics_from_obj(obj: Dict[str, Any]) -> Dict[str, Any]:
    """
    Try to pull out all metrics we care about for runner detection.
    Produces keys that match utils_pkg.pair_filters expectations.
    """
    liq_usd = None
    for k in ("liquidity", "liquidityUsd", "liqUsd", "lpUsd", "liquidity_usd"):
        v = obj.get(k)
        try:
            if v is not None:
                liq_usd = float(v)
                break
        except Exception:
            pass

    fdv_usd = _nz(
        obj.get("fdv")
        or obj.get("fdvUsd")
        or obj.get("fullyDilutedValuation")
        or obj.get("fullyDilutedMcUsd")
        or obj.get("fullyDilutedMarketCap"),
        0.0,
    )
    mcap_usd = _nz(
        obj.get("marketCap")
        or obj.get("marketCapUsd")
        or obj.get("mcap_usd")
        or fdv_usd,
        0.0,
    )

    # Buys / sells – many variants exist, favor shorter timeframes, fall back to 24h
    buys_5m = _safe_int(
        obj.get("buys5m")
        or obj.get("buys_5m")
        or obj.get("m5Buys")
        or obj.get("buysLast5m")
    )
    sells_5m = _safe_int(
        obj.get("sells5m")
        or obj.get("sells_5m")
        or obj.get("m5Sells")
        or obj.get("sellsLast5m")
    )

    buys_15m = _safe_int(
        obj.get("buys15m")
        or obj.get("buys_15m")
        or obj.get("m15Buys")
        or obj.get("buysLast15m")
    )
    sells_15m = _safe_int(
        obj.get("sells15m")
        or obj.get("sells_15m")
        or obj.get("m15Sells")
        or obj.get("sellsLast15m")
    )

    buys_24h = _safe_int(
        obj.get("buys24h") or obj.get("buyCount24h") or obj.get("totalBuys24h")
    )
    sells_24h = _safe_int(
        obj.get("sells24h") or obj.get("sellCount24h") or obj.get("totalSells24h")
    )

    # Approximate intraday buys if only 24h data is present
    if buys_5m == 0 and buys_24h > 0:
        buys_5m = max(1, buys_24h // 288)  # 24h / 5m
    if buys_15m == 0 and buys_24h > 0:
        buys_15m = max(1, buys_24h // 96)

    if sells_5m == 0 and sells_24h > 0:
        sells_5m = max(0, sells_24h // 288)
    if sells_15m == 0 and sells_24h > 0:
        sells_15m = max(0, sells_24h // 96)

    vol_5m_usd = _nz(
        obj.get("volumeUsd5m") or obj.get("volume5mUsd") or obj.get("vol5mUsd"), 0.0
    )
    vol_15m_usd = _nz(
        obj.get("volumeUsd15m") or obj.get("volume15mUsd") or obj.get("vol15mUsd"), 0.0
    )
    vol_24h_usd = _nz(
        obj.get("volumeUsd24h")
        or obj.get("volume24hUsd")
        or obj.get("usdVolume24Hours")
        or obj.get("volume_24h_usd"),
        0.0,
    )

    # Approximate 5m / 15m volume from 24h if needed
    if vol_5m_usd <= 0 and vol_24h_usd > 0:
        vol_5m_usd = vol_24h_usd / 288.0
    if vol_15m_usd <= 0 and vol_24h_usd > 0:
        vol_15m_usd = vol_24h_usd / 96.0

    # Build a rough 1h volume from 24h if direct not present
    vol_1h_usd = vol_24h_usd / 24.0 if vol_24h_usd > 0 else 0.0

    # Price changes
    chg_5m_pct = _nz(obj.get("priceChange5m") or obj.get("priceChange_5m"), 0.0)
    chg_15m_pct = _nz(obj.get("priceChange15m") or obj.get("priceChange_15m"), 0.0)

    # If only 24h change is available, approximate short windows
    chg_24h_pct = _nz(obj.get("priceChange24h") or obj.get("priceChange_24h"), 0.0)
    if chg_5m_pct == 0.0 and chg_24h_pct != 0.0:
        chg_5m_pct = chg_24h_pct / 288.0
    if chg_15m_pct == 0.0 and chg_24h_pct != 0.0:
        chg_15m_pct = chg_24h_pct / 96.0

    # Approximate 1h price change from 24h if no direct value is present
    chg_1h_pct = _nz(obj.get("priceChange1h") or obj.get("priceChange_1h"), 0.0)
    if chg_1h_pct == 0.0 and chg_24h_pct != 0.0:
        chg_1h_pct = chg_24h_pct / 24.0

    created = (
        obj.get("createdAt")
        or obj.get("createdAtTs")
        or obj.get("created")
        or obj.get("createdOn")
    )
    age_min = _age_min(created if created is not None else None)
    token_age_min = age_min if age_min is not None else 9999.0

    holders = _safe_int(
        obj.get("holders")
        or obj.get("holderCount")
        or obj.get("totalHolders")
        or obj.get("holdersCount")
    )
    holders_5m_change = 0  # Pump.fun doesn't expose differential holder data

    price_usd = _nz(
        obj.get("priceUsd")
        or obj.get("usdPrice")
        or obj.get("price")
        or obj.get("tokenPriceUsd"),
        0.0,
    )

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

    impact_1k_bps = _nz(
        obj.get("priceImpact1kBps")
        or obj.get("impact1kBps")
        or obj.get("impact_bps")
        or obj.get("impact_1k_bps"),
        0.0,
    )

    return {
        "liq_usd": _nz(liq_usd, 0.0),
        "mcap_usd": mcap_usd,
        "fdv_usd": fdv_usd,
        "token_age_min": token_age_min,
        "vol_5m_usd": vol_5m_usd,
        "vol_1h_usd": vol_1h_usd,
        "vol_24h_usd": vol_24h_usd,
        "buys_5m": buys_5m,
        "sells_5m": sells_5m,
        "buys_1h": 0,      # we don't have direct; PF mostly cares about 5m + 1h volume
        "sells_1h": 0,
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
        "impact_1k_bps": impact_1k_bps,
        "buys_15m": buys_15m,
        "sells_15m": sells_15m,
    }


def _buy_ratio(buys: int, sells: int) -> float:
    total = max(1, buys + sells)
    return float(buys) / float(total)


def _map_coin_to_meta(
    obj: Dict[str, Any],
    *,
    soft_liq: float,
    soft_mcap: float,
    soft_max_age: float,
    regime: Dict[str, Any],
) -> Optional[Dict[str, Any]]:
    """
    Normalize various Pump.fun JSON shapes → common PF "meta" dict compatible
    with utils_pkg.pair_filters, with regime-aware soft filters.
    """
    try:
        raw_mint = (
            obj.get("mintAddress")
            or obj.get("mint")
            or obj.get("baseMint")
            or obj.get("tokenMint")
            or obj.get("id")
            or obj.get("publicKey")
        )
        mint = _norm_mint(raw_mint)
        if not mint:
            return None

        symbol = (
            obj.get("symbol")
            or obj.get("tokenSymbol")
            or obj.get("name")
            or mint[:6]
        )

        m = _extract_metrics_from_obj(obj)

        # Soft prefilters: skip ultra-dead trash before PF / fallback logic
        liq_usd = m["liq_usd"]
        mcap_usd = m["mcap_usd"]
        token_age_min = m["token_age_min"]

        if soft_liq > 0 and liq_usd < soft_liq:
            return None
        if soft_mcap > 0 and mcap_usd < soft_mcap:
            return None
        if soft_max_age > 0 and token_age_min > soft_max_age:
            return None

        buys_5m = m["buys_5m"]
        sells_5m = m["sells_5m"]
        buys_15m = m.get("buys_15m", buys_5m)
        sells_15m = m.get("sells_15m", sells_5m)

        buy_ratio_5m = _buy_ratio(buys_5m, sells_5m)
        buy_ratio_15m = _buy_ratio(buys_15m, sells_15m)

        txns_5m = buys_5m + sells_5m
        txns_1h = txns_5m * 12  # crude but consistent with PF heuristics

        pair_url = f"https://pump.fun/coin/{mint}"

        cur_regime_label = regime.get("label", "unknown")
        cur_regime_numeric = float(regime.get("label_numeric", 0.0))
        cur_regime_manip = float(regime.get("manipulation_risk", 0.0))

        meta: Dict[str, Any] = {
            # identity
            "source": "pumpfun",
            "dex": "pumpfun",
            "chain": "solana",
            "mint": mint,
            "symbol": symbol,
            "pair_url": pair_url,
            "scan_ts": _now(),

            # regime context
            "regime": cur_regime_label,
            "regime_numeric": cur_regime_numeric,
            "regime_manipulation": cur_regime_manip,
            "regime_ctx": dict(regime),

            # core liquidity / size
            "liq_usd": m["liq_usd"],
            "mcap_usd": m["mcap_usd"],
            "fdv_usd": m["fdv_usd"],

            # time
            "token_age_min": m["token_age_min"],

            # recent activity
            "vol_5m_usd": m["vol_5m_usd"],
            "vol_1h_usd": m["vol_1h_usd"],
            "vol_24h_usd": m["vol_24h_usd"],
            "buys_5m": buys_5m,
            "sells_5m": sells_5m,
            "txns_5m": txns_5m,
            "txns_1h": txns_1h,
            "buy_ratio_5m": buy_ratio_5m,
            "buy_ratio_15m": buy_ratio_15m,

            # price action
            "price_usd": m["price_usd"],
            "price_change_5m_pct": m["price_change_5m_pct"],
            "price_change_15m_pct": m["price_change_15m_pct"],
            "price_change_1h_pct": m["price_change_1h_pct"],

            # holders
            "holders": m["holders"],
            "holders_5m_change": m["holders_5m_change"],

            # whale + AI fields
            "whales_5m": m["whales_5m"],
            "whale_vol_5m_usd": m["whale_vol_5m_usd"],
            "largest_buy_5m_sol": m["largest_buy_5m_sol"],
            "ai_rug_score": 0.0,        # neutral by default
            "ai_entry_score": None,     # filled by AI entry if enabled
            "ai_entry_reason": None,    # human-readable reason if AI provides it
            "rank_score": None,         # final ranking / tie-breaker score

            # security flags (no direct info here; defaults are conservative)
            "is_honeypot": False,
            "trading_paused": False,
            "blacklisted": False,
            "top10_hold_pct": 0.0,
            "dev_hold_pct": 0.0,
            "mint_authority_disabled": True,
            "freeze_authority_disabled": True,

            # slippage / impact info
            "impact_1k_bps": m["impact_1k_bps"],
        }

        return meta
    except Exception as e:
        LOG.debug("pumpfun normalize error: %s", e)
        return None


def _extract_items_from_payload(
    payload: Any,
    *,
    soft_liq: float,
    soft_mcap: float,
    soft_max_age: float,
    regime: Dict[str, Any],
) -> List[Dict[str, Any]]:
    """
    Take raw JSON (list/dict) and return normalized PF meta entries.
    """
    items: List[Dict[str, Any]] = []

    if isinstance(payload, list):
        src = payload
    elif isinstance(payload, dict):
        src: Optional[List[Dict[str, Any]]] = None
        for k in ("trending", "coins", "items", "projects", "data"):
            v = payload.get(k)
            if isinstance(v, list):
                src = v
                break
        if src is None and isinstance(payload.get("data"), dict):
            dd = payload["data"]
            for kk in ("trending", "coins", "items", "projects", "data"):
                v = dd.get(kk)
                if isinstance(v, list):
                    src = v
                    break
        src = src or []
    else:
        src = []

    for it in src:
        if isinstance(it, dict):
            meta = _map_coin_to_meta(
                it,
                soft_liq=soft_liq,
                soft_mcap=soft_mcap,
                soft_max_age=soft_max_age,
                regime=regime,
            )
            if meta:
                items.append(meta)
    return items


# ──────────────────────────────────────────────────────────────────────────────
# HTML __NEXT_DATA__ extraction (no Playwright required)
# ──────────────────────────────────────────────────────────────────────────────

_NEXT_RE = re.compile(
    r'<script[^>]+id=["\']__NEXT_DATA__["\'][^>]*>\s*({.*?})\s*</script>',
    re.DOTALL | re.IGNORECASE,
)


def _html_next_data(html: str):
    try:
        m = _NEXT_RE.search(html or "")
        if not m:
            return None
        return json.loads(m.group(1))
    except Exception:
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
      - basic whale signal via utils_pkg.core_utils.is_whale_wallet
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

    ai_conc = _i("PUMP_AI_CONCURRENCY", 5)
    sem = asyncio.Semaphore(max(1, ai_conc))

    to_rug = metas[: PUMP_AI_RUG_MAX_PER_SCAN] if (PUMP_AI_RUG_ENABLED and ai_fraud_score) else []
    to_entry = metas[: PUMP_AI_ENTRY_MAX_PER_SCAN] if (PUMP_AI_ENTRY_ENABLED and FB and AI_ENTRY) else []

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

            # Whale presence (coarse)
            if PUMP_WHALES_ENABLED and is_whale_wallet:
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

            # Smart wallet tags
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

            # AI entry scoring (+ reason)
            if (
                PUMP_AI_ENTRY_ENABLED
                and FB is not None
                and AI_ENTRY is not None
                and meta in to_entry
            ):
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

def _pf_filter_and_rank(
    metas: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """
    Take normalized meta entries, apply is_viable_pair and PF scoring, and
    return the top candidates ranked by rank_score.

    rank_score is a blend of:
      - PF final_score
      - PF momentum_score
      - optional ai_entry_score
      - optional whale presence
      - lightly scaled by volume
    """
    out: List[Dict[str, Any]] = []

    for meta in metas:
        try:
            # Apply AI rug guardrail first if enabled
            rug = float(meta.get("ai_rug_score") or 0.0)
            if PUMP_AI_RUG_ENABLED and PUMP_MAX_AI_RUG > 0 and rug >= PUMP_MAX_AI_RUG:
                continue

            if not is_viable_pair(meta):
                continue

            momentum = compute_momentum_score(meta)
            final = compute_final_score(meta)

            ai_entry = meta.get("ai_entry_score")
            whales = int(meta.get("whales_5m") or 0)
            vol5 = float(meta.get("vol_5m_usd") or 0.0)

            # Optional AI entry gating (strict mode)
            if PUMP_REQUIRE_AI_ENTRY_FOR_RUNNERS:
                if not isinstance(ai_entry, (int, float)):
                    continue
                if ai_entry < PUMP_MIN_AI_ENTRY:
                    continue
            elif isinstance(ai_entry, (int, float)) and ai_entry < 0.0:
                ai_entry = 0.0

            # Base rank from PF
            rank = float(final)

            # Small bonus for momentum + whales
            rank += 0.2 * float(momentum)
            if whales > 0:
                rank += 0.15

            # AI entry override if present
            if isinstance(ai_entry, (int, float)):
                vol_boost = max(1.0, log1p(max(vol5, 1.0)) / 5.0)
                rank = (0.6 * rank + 0.4 * float(ai_entry)) * vol_boost

            # Global rank guardrail if configured
            if PUMP_MIN_RANK_SCORE > 0.0 and rank < PUMP_MIN_RANK_SCORE:
                continue

            m = dict(meta)
            m["momentum_score"] = float(momentum)
            m["final_score"] = float(final)
            m["rank_score"] = float(rank)
            out.append(m)
        except Exception as e:
            LOG.debug("pumpfun PF filter error for %s: %s", meta.get("mint"), e)

    out.sort(key=lambda x: x.get("rank_score", x.get("final_score", 0.0)), reverse=True)

    max_runners = PUMP_MAX_RUNNERS if PUMP_MAX_RUNNERS > 0 else PUMP_RUNNER_LIMIT
    max_runners = max(1, max_runners)
    return out[: max_runners]


def _fallback_pick_metas(
    metas: List[Dict[str, Any]],
    *,
    min_vol5_usd: float,
    max_impact_bps: float,
) -> List[Dict[str, Any]]:
    """
    Fallback runner selection when PF returns nothing or PUMP_BYPASS_PF is true:
      - require minimum 5m buys + 5m volume
      - respect an AI rug ceiling if AI is enabled
      - optional AI entry gating (same flags as PF path)
      - cap slippage using impact_1k_bps
      - sort by (whales_5m desc, vol_5m_usd desc, price_change_5m_pct desc)
    """
    if not PUMP_ENABLE_FALLBACK or not metas:
        return []

    min_buys = int(PUMP_FB_MIN_BUYS_5M)
    min_vol = float(min_vol5_usd)
    max_impact = float(max_impact_bps)
    ai_max = float(PUMP_MAX_AI_RUG)
    fb_max = int(PUMP_FB_MAX_RUNNERS)

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
            if PUMP_AI_RUG_ENABLED and ai_max > 0 and rug >= ai_max:
                continue

            # Optional AI entry gating in fallback too
            ai_entry = m.get("ai_entry_score")
            if PUMP_REQUIRE_AI_ENTRY_FOR_RUNNERS:
                if not isinstance(ai_entry, (int, float)):
                    continue
                if ai_entry < PUMP_MIN_AI_ENTRY:
                    continue

            cands.append(m)
        except Exception:
            continue

    if not cands:
        LOG.info(
            "Pump.fun fallback: 0 metas matched (min_buys=%d min_vol=%.1f "
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

    LOG.info(
        "Pump.fun fallback: picked %d/%d metas (min_buys=%d min_vol=%.1f max_impact=%.1f ai_max=%.2f)",
        len(picked),
        len(cands),
        min_buys,
        min_vol,
        max_impact,
        ai_max,
    )
    return picked


# ──────────────────────────────────────────────────────────────────────────────
# HTTP helpers
# ──────────────────────────────────────────────────────────────────────────────

async def _fetch_text(
    session: aiohttp.ClientSession,
    url: str,
) -> Tuple[str, int, Dict[str, str]]:
    headers = {
        "User-Agent": UA,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Referer": "https://pump.fun/",
        "Origin": "https://pump.fun",
        "Accept-Language": "en-US,en;q=0.9",
    }
    ck = get_cookie_for(url)
    if ck:
        headers["Cookie"] = ck

    try:
        async with session.get(
            url,
            headers=headers,
            ssl=ssl.create_default_context(cafile=certifi.where()),
            timeout=REQ_TIMEOUT_S,
            proxy=PUMPFUN_HTTP_PROXY or None,
        ) as r:
            text = await r.text()
            return text, r.status, dict(r.headers)
    except Exception:
        return "", 0, {}


async def _fetch_json(
    session: aiohttp.ClientSession,
    url: str,
) -> Tuple[Optional[Any], int, str]:
    """
    GET `url` expecting JSON. Returns (data_or_None, http_status, first_120_chars_of_text).
    Robust to bad content-types and falls back to manual json.loads(text).
    """
    headers = _hdrs(url)
    status = 0
    first = ""
    text = ""

    try:
        async with session.get(
            url,
            headers=headers,
            ssl=ssl.create_default_context(cafile=certifi.where()),
            timeout=REQ_TIMEOUT_S,
            proxy=PUMPFUN_HTTP_PROXY or None,
        ) as r:
            status = r.status
            try:
                text = await r.text()
                first = (text or "")[:120]
            except Exception:
                text = ""
                first = ""

            data: Optional[Any] = None
            if status == 200:
                try:
                    data = await r.json(content_type=None)
                except Exception:
                    try:
                        if text:
                            data = json.loads(text)
                    except Exception:
                        data = None

            if status in _BAD_CF_CODES:
                flag_cookie_bad()

            return data, status, first
    except (
        aiohttp.ClientProxyConnectionError,
        aiohttp.ClientHttpProxyError,
        aiohttp.ClientConnectorError,
        aiohttp.ServerTimeoutError,
        asyncio.TimeoutError,
    ) as e:
        LOG.warning("Pump.fun request failed %s: %s", type(e).__name__, url)
        return None, status, first
    except Exception as e:
        LOG.warning("Pump.fun request unexpected error at %s: %s", url, e)
        return None, status, first


def _join_url(base: str, path: str) -> str:
    base = base.rstrip("/")
    path = path.lstrip("/")
    return f"{base}/{path}"


# ──────────────────────────────────────────────────────────────────────────────
# Fallback: HTML __NEXT_DATA__ from apex pages
# ──────────────────────────────────────────────────────────────────────────────

async def _html_next_fallback(
    session: aiohttp.ClientSession,
    *,
    soft_liq: float,
    soft_mcap: float,
    soft_max_age: float,
    regime: Dict[str, Any],
) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []

    for url in FALLBACK_HTML_URLS:
        try:
            html, status, headers = await _fetch_text(session, url)
        except Exception as e:
            LOG.warning("pumpfun HTML fallback fetch failed at %s: %s", url, e)
            continue

        ctype = (headers.get("content-type") or "").lower()
        low = (html or "").lower()
        blocked = (
            status in _BAD_CF_CODES
            or "cloudflare" in low
            or "cf-chl-" in low
            or "just a moment" in low
        )
        if not html or blocked:
            LOG.warning(
                "pumpfun HTML fallback: blocked page at %s (status=%s)", url, status
            )
            continue

        payload = _html_next_data(html)
        if not payload:
            LOG.debug(
                "pumpfun HTML fallback: __NEXT_DATA__ not found at %s (status=%s ctype=%s)",
                url,
                status,
                ctype,
            )
            continue

        items = _extract_items_from_payload(
            payload,
            soft_liq=soft_liq,
            soft_mcap=soft_mcap,
            soft_max_age=soft_max_age,
            regime=regime,
        )
        if not items:
            try:
                props = (payload.get("props") or {}).get("pageProps") or {}
                for key in (
                    "coins",
                    "trending",
                    "trendingCoins",
                    "data",
                    "items",
                    "list",
                    "feed",
                ):
                    v = props.get(key)
                    if isinstance(v, list):
                        items = _extract_items_from_payload(
                            v,
                            soft_liq=soft_liq,
                            soft_mcap=soft_mcap,
                            soft_max_age=soft_max_age,
                            regime=regime,
                        )
                        if items:
                            break
            except Exception:
                items = []

        if items:
            with contextlib.suppress(Exception):
                await _enrich_ai_whales_entry(items, regime=regime)

            pf_runners = _pf_filter_and_rank(items) if not PUMP_BYPASS_PF else []
            fb_runners = _fallback_pick_metas(
                items,
                min_vol5_usd=PUMP_FB_MIN_VOL5_USD,
                max_impact_bps=PUMP_FB_MAX_IMPACT_1K_BPS,
            ) if (PUMP_BYPASS_PF or not pf_runners) else []
            runners = pf_runners or fb_runners

            out.extend(runners)
            LOG.info(
                "scanner.pumpfun: HTML __NEXT_DATA__ → %s runners (pf=%d fb=%d) from %s",
                len(runners),
                len(pf_runners),
                len(fb_runners),
                url,
            )

            if PUMP_LOG_SCAN_SUMMARY:
                with contextlib.suppress(Exception):
                    asyncio.create_task(
                        log_event(
                            "pumpfun_scan_summary",
                            {
                                "source": "html_next",
                                "raw_items": len(items),
                                "pf_runners": len(pf_runners),
                                "fb_runners": len(fb_runners),
                                "runners": len(runners),
                                "regime": dict(regime),
                                "config": {
                                    "SOFT_MIN_LIQ_USD": SOFT_MIN_LIQ_USD,
                                    "SOFT_MIN_MCAP_USD": SOFT_MIN_MCAP_USD,
                                    "SOFT_MAX_AGE_MIN": SOFT_MAX_AGE_MIN,
                                    "PUMP_BYPASS_PF": PUMP_BYPASS_PF,
                                    "PUMP_ENABLE_FALLBACK": PUMP_ENABLE_FALLBACK,
                                    "PUMP_FB_MIN_BUYS_5M": PUMP_FB_MIN_BUYS_5M,
                                    "PUMP_FB_MIN_VOL5_USD": PUMP_FB_MIN_VOL5_USD,
                                    "PUMP_FB_MAX_RUNNERS": PUMP_FB_MAX_RUNNERS,
                                    "PUMP_FB_MAX_IMPACT_1K_BPS": PUMP_FB_MAX_IMPACT_1K_BPS,
                                    "PUMP_AI_RUG_ENABLED": PUMP_AI_RUG_ENABLED,
                                    "PUMP_MAX_AI_RUG": PUMP_MAX_AI_RUG,
                                    "PUMP_AI_ENTRY_ENABLED": PUMP_AI_ENTRY_ENABLED,
                                    "PUMP_REQUIRE_AI_ENTRY_FOR_RUNNERS": PUMP_REQUIRE_AI_ENTRY_FOR_RUNNERS,
                                },
                            },
                        )
                    )

            break  # first success is enough

    if out:
        seen: set[str] = set()
        uniq: List[Dict[str, Any]] = []
        for d in out:
            m = d.get("mint")
            if isinstance(m, str) and m not in seen:
                seen.add(m)
                uniq.append(d)
        out = uniq

    return out


# ──────────────────────────────────────────────────────────────────────────────
# Core scanning logic
# ──────────────────────────────────────────────────────────────────────────────

async def _fetch_api_json_with_fallback(
    session: aiohttp.ClientSession,
    path: str,
    *,
    soft_liq: float,
    soft_mcap: float,
    soft_max_age: float,
    regime: Dict[str, Any],
    tried_out: Optional[List[str]] = None,
) -> Optional[List[Dict[str, Any]]]:
    # Debug override
    if OVERRIDE_URL and OVERRIDE_URL.lower().startswith("http"):
        payload, status, first = await _fetch_json(session, OVERRIDE_URL)
        if tried_out is not None:
            tried_out.append(f"{OVERRIDE_URL} status={status}")
        if payload:
            items = _extract_items_from_payload(
                payload,
                soft_liq=soft_liq,
                soft_mcap=soft_mcap,
                soft_max_age=soft_max_age,
                regime=regime,
            )
            if items:
                LOG.info(
                    "scanner.pumpfun: API JSON (override) → %s meta items", len(items)
                )
                return items

    for base in _api_bases():
        url = _join_url(base, path)
        payload, status, first = await _fetch_json(session, url)
        if tried_out is not None:
            tried_out.append(f"{url} status={status}")
        if not payload:
            if status in (404, *_BAD_CF_CODES, *_PROXY_BREAK_CODES):
                LOG.warning("Pump.fun: [%s] HTTP %s; first 120: %s", url, status, first)
            continue
        items = _extract_items_from_payload(
            payload,
            soft_liq=soft_liq,
            soft_mcap=soft_mcap,
            soft_max_age=soft_max_age,
            regime=regime,
        )
        if items:
            LOG.info(
                "scanner.pumpfun: API JSON → %s meta items (base=%s)", len(items), base
            )
            return items

    return None


async def _collect_pumpfun_items(
    session: aiohttp.ClientSession,
    *,
    soft_liq: float,
    soft_mcap: float,
    soft_max_age: float,
    regime: Dict[str, Any],
) -> List[Dict[str, Any]]:
    """
    Try multiple Pump.fun sources in order, returning a *list* of normalized meta items.
    """
    tried: List[str] = []

    # 1) API: trending
    items = await _fetch_api_json_with_fallback(
        session,
        f"coins/trending?offset=0&limit={API_LIMIT}",
        soft_liq=soft_liq,
        soft_mcap=soft_mcap,
        soft_max_age=soft_max_age,
        regime=regime,
        tried_out=tried,
    )
    if items:
        return items

    # 2) API: new
    items = await _fetch_api_json_with_fallback(
        session,
        f"coins/new?offset=0&limit={API_LIMIT}",
        soft_liq=soft_liq,
        soft_mcap=soft_mcap,
        soft_max_age=soft_max_age,
        regime=regime,
        tried_out=tried,
    )
    if items:
        return items

    # 3) legacy endpoint (often 404) — still try once
    legacy = "https://pump.fun/api/trending"
    payload, status, first = await _fetch_json(session, legacy)
    tried.append(f"{legacy} status={status}")
    if payload:
        items = _extract_items_from_payload(
            payload,
            soft_liq=soft_liq,
            soft_mcap=soft_mcap,
            soft_max_age=soft_max_age,
            regime=regime,
        )
        if items:
            LOG.info("scanner.pumpfun: legacy API JSON → %s meta items", len(items))
            return items

    # 4) HTML __NEXT_DATA__ fallback (no Playwright)
    if not SKIP_HTML_NEXT:
        items = await _html_next_fallback(
            session,
            soft_liq=soft_liq,
            soft_mcap=soft_mcap,
            soft_max_age=soft_max_age,
            regime=regime,
        )
        if items:
            return items

    # 5) last resort: Playwright fetch to *first* available base (capped by API_LIMIT)
    try:
        first_base = _api_bases()[0]
    except Exception:
        first_base = "https://frontend-api.pump.fun"
    fallback_url = _join_url(first_base, f"coins/trending?offset=0&limit={API_LIMIT}")

    payload = None
    try:
        maybe = browser_fetch_json(fallback_url)
        payload = await maybe if asyncio.iscoroutine(maybe) else maybe
    except Exception as e:
        LOG.debug("browser_fetch_json final fallback failed: %s", e)

    if payload:
        items = _extract_items_from_payload(
            payload,
            soft_liq=soft_liq,
            soft_mcap=soft_mcap,
            soft_max_age=soft_max_age,
            regime=regime,
        )
        if items:
            LOG.info(
                "scanner.pumpfun: Playwright API → %s meta items", len(items)
            )
            return items

    if tried:
        LOG.warning(
            "scanner.pumpfun: no items after tries: %s", " | ".join(tried)
        )

    return []


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
    """
    Full Pump.fun scan using:
      1) Next.js via Playwright (if enabled).
      2) DOM scrape via Playwright (if enabled).
      3) JSON API + legacy + HTML fallback.
    Returns normalized runner metas (for strategy / sniper).
    """
    apply_cookies_to_session(session)

    # 0) Next.js (/_next/data) via Playwright
    if not SKIP_NEXT:
        try:
            raw = await browser_fetch_trending_via_next_data(limit=DOM_LIMIT)
            if isinstance(raw, list) and raw:
                metas: List[Dict[str, Any]] = []
                for it in raw:
                    if isinstance(it, dict):
                        meta = _map_coin_to_meta(
                            it,
                            soft_liq=soft_liq,
                            soft_mcap=soft_mcap,
                            soft_max_age=soft_max_age,
                            regime=regime,
                        )
                        if meta:
                            metas.append(meta)

                with contextlib.suppress(Exception):
                    await _enrich_ai_whales_entry(metas, regime=regime)

                pf_runners = _pf_filter_and_rank(metas) if not PUMP_BYPASS_PF else []
                fb_runners = _fallback_pick_metas(
                    metas,
                    min_vol5_usd=fb_min_vol5,
                    max_impact_bps=fb_max_impact_bps,
                ) if (PUMP_BYPASS_PF or not pf_runners) else []
                runners = pf_runners or fb_runners

                if runners:
                    LOG.info(
                        "scanner.pumpfun: next-data apex → %s runners (pf=%d fb=%d)",
                        len(runners),
                        len(pf_runners),
                        len(fb_runners),
                    )

                    if PUMP_LOG_SCAN_SUMMARY:
                        with contextlib.suppress(Exception):
                            asyncio.create_task(
                                log_event(
                                    "pumpfun_scan_summary",
                                    {
                                        "source": "next_data",
                                        "raw_items": len(metas),
                                        "pf_runners": len(pf_runners),
                                        "fb_runners": len(fb_runners),
                                        "runners": len(runners),
                                        "regime": dict(regime),
                                    },
                                )
                            )

                    return runners
        except Exception as e:
            LOG.warning("Pump.fun next-data apex fetch error: %s", e)
    else:
        LOG.debug("PUMPFUN_SKIP_NEXT=1 → skipping next-data step")

    # 1) Apex DOM via Playwright
    if not SKIP_NEXT:
        try:
            dom = await browser_scrape_trending_dom(limit=DOM_LIMIT)
            if isinstance(dom, list) and dom:
                metas: List[Dict[str, Any]] = []
                for it in dom:
                    if isinstance(it, dict):
                        meta = _map_coin_to_meta(
                            it,
                            soft_liq=soft_liq,
                            soft_mcap=soft_mcap,
                            soft_max_age=soft_max_age,
                            regime=regime,
                        )
                        if meta:
                            metas.append(meta)

                with contextlib.suppress(Exception):
                    await _enrich_ai_whales_entry(metas, regime=regime)

                pf_runners = _pf_filter_and_rank(metas) if not PUMP_BYPASS_PF else []
                fb_runners = _fallback_pick_metas(
                    metas,
                    min_vol5_usd=fb_min_vol5,
                    max_impact_bps=fb_max_impact_bps,
                ) if (PUMP_BYPASS_PF or not pf_runners) else []
                runners = pf_runners or fb_runners

                if runners:
                    LOG.info(
                        "scanner.pumpfun: DOM apex → %s runners (pf=%d fb=%d)",
                        len(runners),
                        len(pf_runners),
                        len(fb_runners),
                    )

                    if PUMP_LOG_SCAN_SUMMARY:
                        with contextlib.suppress(Exception):
                            asyncio.create_task(
                                log_event(
                                    "pumpfun_scan_summary",
                                    {
                                        "source": "dom",
                                        "raw_items": len(metas),
                                        "pf_runners": len(pf_runners),
                                        "fb_runners": len(fb_runners),
                                        "runners": len(runners),
                                        "regime": dict(regime),
                                    },
                                )
                            )

                    return runners
        except Exception as e:
            LOG.warning("Pump.fun apex DOM scrape error: %s", e)
    else:
        LOG.debug("PUMPFUN_SKIP_NEXT=1 → skipping DOM scrape step")

    # 2) API + fallbacks
    metas = await _collect_pumpfun_items(
        session,
        soft_liq=soft_liq,
        soft_mcap=soft_mcap,
        soft_max_age=soft_max_age,
        regime=regime,
    )
    if not metas:
        if PUMP_LOG_SCAN_SUMMARY:
            with contextlib.suppress(Exception):
                asyncio.create_task(
                    log_event(
                        "pumpfun_scan_summary",
                        {
                            "source": "api",
                            "raw_items": 0,
                            "pf_runners": 0,
                            "fb_runners": 0,
                            "runners": 0,
                            "regime": dict(regime),
                        },
                    )
                )
        return []

    if PUMP_DEBUG_META_SAMPLE:
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
            for m in metas[:5]
        ]
        LOG.info("PUMP_META_SAMPLE count=%d first=%s", len(metas), sample)

    with contextlib.suppress(Exception):
        await _enrich_ai_whales_entry(metas, regime=regime)

    pf_runners: List[Dict[str, Any]] = []
    if not PUMP_BYPASS_PF:
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

    if PUMP_DEBUG_PF_RESULT:
        LOG.info(
            "PUMP_PF_RESULT pf=%d fb=%d metas=%d",
            len(pf_runners),
            len(fb_runners),
            len(metas),
        )

    LOG.info(
        "scanner.pumpfun: API/fallback → %s runners (pf=%d fb=%d metas=%d)",
        len(runners),
        len(pf_runners),
        len(fb_runners),
        len(metas),
    )

    if PUMP_LOG_SCAN_SUMMARY:
        with contextlib.suppress(Exception):
            asyncio.create_task(
                log_event(
                    "pumpfun_scan_summary",
                    {
                        "source": "api",
                        "raw_items": len(metas),
                        "pf_runners": len(pf_runners),
                        "fb_runners": len(fb_runners),
                        "runners": len(runners),
                        "regime": dict(regime),
                    },
                )
            )

    max_runners = PUMP_MAX_RUNNERS if PUMP_MAX_RUNNERS > 0 else PUMP_RUNNER_LIMIT
    max_runners = max(1, max_runners)
    return runners[: max_runners]


# ──────────────────────────────────────────────────────────────────────────────
# MOMO + AI logging wiring
# ──────────────────────────────────────────────────────────────────────────────

def _effective_regime_tag() -> Optional[str]:
    if PUMP_REGIME_TAG_ENV:
        return PUMP_REGIME_TAG_ENV
    try:
        r = regime_state.get()
        if r:
            return r
    except Exception:
        pass
    if _regime is not None and hasattr(_regime, "regime_for_src"):
        try:
            return _regime.regime_for_src("pumpfun", default="pumpfun_live")  # type: ignore[arg-type]
        except Exception:
            pass
    return "pumpfun_live"


def _effective_wallet_tag() -> Optional[str]:
    return PUMP_WALLET_TAG_ENV or None


def _log_candidates_for_training(runners: List[Dict[str, Any]]) -> None:
    """
    Sync helper: write runner metas into entry_candidates for AI training.
    Safe no-op if PG is disabled or ai_db/data_logger aren't wired.

    Prefers source-specific function log_pumpfun_candidate if present,
    otherwise falls back to generic log_entry_candidate.
    """
    if not (PG_ENABLED and PUMP_LOG_CANDIDATES and data_logger and runners):
        return

    regime_tag = _effective_regime_tag()
    wallet_tag = _effective_wallet_tag()

    # Try to locate specific + generic logging functions once.
    log_pumpfun = getattr(data_logger, "log_pumpfun_candidate", None)
    log_entry = getattr(data_logger, "log_entry_candidate", None)

    for meta in runners:
        try:
            mint = (meta.get("mint") or "").strip()
            if not mint:
                continue
            symbol = (meta.get("symbol") or "").strip() or None

            snapshot = dict(meta)

            if callable(log_pumpfun):
                # Dedicated Pump.fun logger (preferred)
                log_pumpfun(  # type: ignore[misc]
                    mint=mint,
                    symbol=symbol,
                    meta=snapshot,
                    regime=regime_tag,
                    wallet_tag=wallet_tag,
                )
            elif callable(log_entry):
                # Generic entry_candidates logger
                log_entry(  # type: ignore[misc]
                    src="pumpfun",
                    mint=mint,
                    symbol=symbol,
                    snapshot=snapshot,
                    regime=regime_tag,
                    wallet_tag=wallet_tag,
                )
        except Exception:
            LOG.exception(
                "Pump.fun: failed to log entry_candidate for mint=%s",
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
        LOG.warning("Pump.fun: failed to init AlertManager for MOMO: %s", e)
        return

    max_n = max(0, min(MAX_ALERTS_PER_SCAN, len(runners)))
    for c in runners[:max_n]:
        try:
            age_min = c.get("token_age_min")
            age_sec = age_min * 60.0 if isinstance(age_min, (int, float)) else None
            ctx: Dict[str, Any] = {
                "symbol": c.get("symbol") or c.get("mint"),
                "mint": c.get("mint"),
                "pool": "pumpfun",               # pool id used by strategy / filters
                "pool_url": c.get("pair_url"),   # clickable URL
                "age_sec": age_sec,
                "liq_usd": c.get("liq_usd"),
                "vol_5m": c.get("vol_5m_usd"),
                "up_pct": c.get("price_change_5m_pct"),
                "ai_entry_score": c.get("ai_entry_score"),
                "ai_entry_reason": c.get("ai_entry_reason"),
                "ai_rug_score": c.get("ai_rug_score"),
                "rank_score": c.get("rank_score"),
                "reason": (
                    f"pumpfun runner liq=${c.get('liq_usd', 0):.0f} "
                    f"vol5=${c.get('vol_5m_usd', 0):.0f} "
                    f"up5={c.get('price_change_5m_pct', 0):.2f}% "
                    f"ai={c.get('ai_entry_score', 0)!r}"
                ),
            }
            await am.send_momo_event("MOMO_OK", ctx)
        except Exception as e:
            LOG.warning("Pump.fun: MOMO send error for %s: %s", c.get("mint"), e)


# ──────────────────────────────────────────────────────────────────────────────
# Public entrypoint
# ──────────────────────────────────────────────────────────────────────────────

HEADERS = {"User-Agent": "pumpfun-scanner/1.0"}


async def scan_pumpfun(*args):
    """
    Flexible entrypoint:
      - scan_pumpfun()
      - scan_pumpfun(redis)
      - scan_pumpfun(redis, session)
      - scan_pumpfun(session)

    Only the aiohttp.ClientSession (if provided) is used.
    Returns a list of runner candidates (each is a meta dict).
    """
    sess: Optional[aiohttp.ClientSession] = None
    if len(args) >= 2 and isinstance(args[1], aiohttp.ClientSession):
        sess = args[1]
    elif len(args) == 1 and isinstance(args[0], aiohttp.ClientSession):
        sess = args[0]

    # Regime snapshot once per scan
    regime = await _regime_snapshot()
    tuned_soft_liq, tuned_soft_mcap, tuned_fb_min_vol5, tuned_fb_max_impact = _regime_tune_thresholds(
        SOFT_MIN_LIQ_USD,
        SOFT_MIN_MCAP_USD,
        PUMP_FB_MIN_VOL5_USD,
        PUMP_FB_MAX_IMPACT_1K_BPS,
        regime,
    )

    LOG.info(
        "scanner.pumpfun: starting scan; soft_liq=$%.0f soft_mcap=$%.0f "
        "pf_min_liq=$%.0f pf_min_mcap=$%.0f bypass_pf=%s fallback=%s "
        "regime_label=%.2f manip=%.2f tuned_soft_liq=$%.0f tuned_soft_mcap=$%.0f "
        "tuned_fb_min_vol5=$%.0f tuned_fb_max_impact=%.0f",
        SOFT_MIN_LIQ_USD,
        SOFT_MIN_MCAP_USD,
        PF_SCAN_MIN_LIQ_USD,
        PF_SCAN_MIN_MCAP_USD,
        PUMP_BYPASS_PF,
        PUMP_ENABLE_FALLBACK,
        regime.get("label_numeric", 0.0),
        regime.get("manipulation_risk", 0.0),
        tuned_soft_liq,
        tuned_soft_mcap,
        tuned_fb_min_vol5,
        tuned_fb_max_impact,
    )

    # Try borrowing a shared session from core utils (best-effort)
    if sess is None:
        try:
            from utils_pkg.core_utils import session as core_session  # type: ignore
            if isinstance(core_session, aiohttp.ClientSession):
                sess = core_session
        except Exception:
            sess = None

    if sess is not None:
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
            LOG.info("scanner.pumpfun: %s final runner candidates", len(runners))
            return runners
        except Exception as e:
            LOG.warning("Pump.fun scan error (session): %s", e)
            return []

    sslctx = ssl.create_default_context(cafile=certifi.where())
    timeout = aiohttp.ClientTimeout(total=REQ_TIMEOUT_S + 5)
    conn = aiohttp.TCPConnector(ssl=sslctx, limit=SESSION_LIMIT)

    async with aiohttp.ClientSession(
        headers=HEADERS,
        connector=conn,
        trust_env=True,
        timeout=timeout,
    ) as session:
        try:
            runners = await _scan_inner(
                session,
                soft_liq=tuned_soft_liq,
                soft_mcap=tuned_soft_mcap,
                soft_max_age=SOFT_MAX_AGE_MIN,
                fb_min_vol5=tuned_fb_min_vol5,
                fb_max_impact_bps=tuned_fb_max_impact,
                regime=regime,
            ) or []
            await _send_momo_for_runners(runners)
            _log_candidates_for_training(runners)
            LOG.info("scanner.pumpfun: %s final runner candidates", len(runners))
            return runners
        except Exception as e:
            LOG.warning("Pump.fun scan error: %s", e)
            return []


__all__ = ["scan_pumpfun"]
