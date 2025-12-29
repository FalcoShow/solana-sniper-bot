# -*- coding: utf-8 -*-
from __future__ import annotations

"""
scanner/raydium_scanner.py — Raydium scanner via Dexscreener (V3+++ / God-Tier LOCK-IN)
======================================================================================

CONTROL PLANE ONLY (scanner)
----------------------------
- Finds Raydium pairs on Solana via Dexscreener search endpoint(s)
- Normalizes into a single CandidateSnapshot schema (canonical keys + compat aliases)
- Eliminates “silent no-buy”: every reject increments a reason bucket; summary is logged.

Hard rules
----------
- No swap / signing / tx execution here
- No network at import time
- Must not crash on missing optional modules unless RAYDIUM_FAST_FAIL=1
- Must be cancellation-safe (do NOT swallow asyncio.CancelledError)

Public API
----------
async def scan_raydium(*args) -> list[dict]
async def scan_once(*args) -> list[dict]
async def run(...)

Output
------
scan_raydium(...) returns: List[Dict[str, Any]] “runners” sorted best-first.
"""

import asyncio
import contextlib
import json
import os
import random
import ssl
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set, Tuple

# Optional deps: scanner should fail-closed if missing
try:
    import aiohttp  # type: ignore
except Exception:  # pragma: no cover
    aiohttp = None  # type: ignore

try:
    import certifi  # type: ignore
except Exception:  # pragma: no cover
    certifi = None  # type: ignore


# ──────────────────────────────────────────────────────────────────────────────
# Logger (safe)
# ──────────────────────────────────────────────────────────────────────────────
try:
    from logging_setup import get_logger  # type: ignore

    log = get_logger("scanner.raydium")
except Exception:  # pragma: no cover
    import logging

    log = logging.getLogger("scanner.raydium")
    if not log.handlers:
        logging.basicConfig(level=logging.INFO)


def _ensure_log_shims() -> None:
    if not hasattr(log, "event"):

        def _event(name: str, **kw: Any) -> None:  # pragma: no cover
            try:
                log.info("%s %s", name, kw)
            except Exception:
                pass

        setattr(log, "event", _event)

    if not hasattr(log, "every"):

        def _every(key: str, n: int = 60, msg: Optional[str] = None, **kw: Any) -> None:  # pragma: no cover
            now = time.time()
            store = getattr(_every, "_store", {})  # type: ignore[attr-defined]
            last = float(store.get(key, 0.0))
            if (now - last) >= float(n):
                store[key] = now
                setattr(_every, "_store", store)  # type: ignore[attr-defined]
                try:
                    getattr(log, "event")(msg or key, **kw)
                except Exception:
                    pass

        setattr(log, "every", _every)


_ensure_log_shims()


def _short(x: Any, n: int = 240) -> str:
    try:
        s = str(x)
        return s if len(s) <= n else (s[:n] + "…")
    except Exception:
        return "<?>"

# ──────────────────────────────────────────────────────────────────────────────
# Env/config helpers
# ──────────────────────────────────────────────────────────────────────────────
_TRUE = {"1", "true", "t", "yes", "y", "on"}


def _b(name: str, default: bool = False) -> bool:
    v = os.getenv(name)
    if v is None or str(v).strip() == "":
        return bool(default)
    return str(v).strip().lower() in _TRUE


def _s(name: str, default: str = "") -> str:
    v = os.getenv(name)
    return default if v is None else str(v).strip()


def _i(name: str, default: int) -> int:
    v = os.getenv(name)
    try:
        return int(float(v)) if v not in (None, "") else int(default)
    except Exception:
        return int(default)


def _f(name: str, default: float) -> float:
    v = os.getenv(name)
    try:
        return float(v) if v not in (None, "") else float(default)
    except Exception:
        return float(default)


def _clamp(x: float, lo: float, hi: float) -> float:
    return lo if x < lo else hi if x > hi else x


def _nzf(v: Any, d: float = 0.0) -> float:
    try:
        x = float(v)
        return d if x != x else x  # NaN guard
    except Exception:
        return d


def _nzi(v: Any, d: int = 0) -> int:
    try:
        return int(v)
    except Exception:
        return d


def _fast_fail() -> bool:
    return _b("RAYDIUM_FAST_FAIL", _b("MAIN_FAST_FAIL", False))


# safe config import (no network)
try:
    import config as C  # type: ignore
except Exception:  # pragma: no cover

    class _C:
        pass

    C = _C()  # type: ignore


def _cfg(name: str, default: Any) -> Any:
    """Env wins. Then config attr. Then default."""
    if os.getenv(name) not in (None, ""):
        if isinstance(default, bool):
            return _b(name, bool(default))
        if isinstance(default, int) and not isinstance(default, bool):
            return _i(name, int(default))
        if isinstance(default, float):
            return _f(name, float(default))
        return _s(name, str(default))
    return getattr(C, name, default)


# Optional mint normalizer (single source of truth if present)
try:
    from utils_pkg.mints import normalize_mint as _normalize_mint  # type: ignore
except Exception:  # pragma: no cover

    def _normalize_mint(m: str) -> str:
        return (m or "").strip()


# ──────────────────────────────────────────────────────────────────────────────
# Phase / Mode
# ──────────────────────────────────────────────────────────────────────────────
MASTER_PHASE = max(
    0,
    min(
        7,
        _i("MASTER_PHASE", _i("STRATEGY_PHASE", int(_cfg("MASTER_PHASE", 7) or 7))),
    ),
)

if MASTER_PHASE == 0:
    _default_mode = "off"
elif MASTER_PHASE <= 2:
    _default_mode = "simple"
elif MASTER_PHASE <= 4:
    _default_mode = "alpha"
else:
    _default_mode = "full"

RAYDIUM_MODE = _s("RAYDIUM_MODE", _default_mode).lower().strip()
if RAYDIUM_MODE not in {"off", "simple", "alpha", "full"}:
    RAYDIUM_MODE = "full"

ENABLE_RAYDIUM_SCANNER = bool(_cfg("ENABLE_RAYDIUM_SCANNER", True))
RAYDIUM_ENABLED = _b("RAYDIUM_ENABLED", (RAYDIUM_MODE != "off")) and ENABLE_RAYDIUM_SCANNER

# ──────────────────────────────────────────────────────────────────────────────
# Dexscreener fetch config
# ──────────────────────────────────────────────────────────────────────────────
UA = _s("SCANNER_UA", "Mozilla/5.0 SniperBot/1.0 (+raydium)")
BASE = _s("DEXSCREENER_BASE_URL", "https://api.dexscreener.com").rstrip("/")
SEARCH_PATH = _s("RAYDIUM_DEXSEARCH_PATH", "/latest/dex/search").strip() or "/latest/dex/search"
SEARCH_URL = f"{BASE}{SEARCH_PATH if SEARCH_PATH.startswith('/') else '/' + SEARCH_PATH}"

SEARCH_LIMIT = max(25, _i("RAYDIUM_SEARCH_LIMIT", 250))
SEARCH_QUERIES = [q.strip() for q in _s("RAYDIUM_SEARCH_QUERIES", "raydium,solana").split(",") if q.strip()]
FETCH_CONCURRENCY = max(1, _i("RAYDIUM_FETCH_CONCURRENCY", 3))

TIMEOUT_S = max(5.0, _f("RAYDIUM_HTTP_TIMEOUT_SEC", 12.0))
RETRIES = max(1, _i("RAYDIUM_HTTP_RETRIES", 3))
BACKOFF_S = max(0.1, _f("RAYDIUM_HTTP_BACKOFF_SEC", 0.6))
TRUST_ENV = _b("RAYDIUM_TRUST_ENV", True)
HTTP_PROXY = _s("RAYDIUM_HTTP_PROXY", "") or _s("SCANNER_HTTP_PROXY", "")

LIMIT = max(1, _i("RAYDIUM_LIMIT", 40))
RAYDIUM_MAX_RUNNERS = max(1, _i("RAYDIUM_MAX_RUNNERS", LIMIT))

# Quote constraints
REQUIRE_SOL_OR_USDC_QUOTE = _b("RAYDIUM_REQUIRE_SOL_OR_USDC_QUOTE", True)
ALLOWED_QUOTES = {q.strip().upper() for q in _s("RAYDIUM_ALLOWED_QUOTES", "SOL,USDC").split(",") if q.strip()}

# Common quote mints (Solana mainnet)
_SOL_MINT = "So11111111111111111111111111111111111111112"
_USDC_MINT = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"

# Prefilters (anti-dead / anti-toxic)
MIN_BUYS_5M = max(0, _i("RAYDIUM_MIN_BUYS_5M", 0))         # 0 disables
MIN_VOL5_USD = max(0.0, _f("RAYDIUM_MIN_VOL5_USD", 0.0))   # 0 disables
MAX_NEG_CHG5 = _f("RAYDIUM_MAX_NEG_CHG5_PCT", -35.0)       # reject extreme dumps (<=)

# Liquidity/age soft gates (cheap)
PF_SCAN_MIN_LIQ_USD = float(_cfg("PF_SCAN_MIN_LIQ_USD", 2500.0) or 2500.0)
PF_SCAN_MAX_TOKEN_AGE_MIN = float(_cfg("PF_SCAN_MAX_TOKEN_AGE_MIN", 0.0) or 0.0)
SOFT_MIN_LIQ_USD = _f("RAYDIUM_SOFT_MIN_LIQ_USD", max(0.0, PF_SCAN_MIN_LIQ_USD * 0.25))
SOFT_MAX_AGE_MIN = _f(
    "RAYDIUM_SOFT_MAX_AGE_MIN",
    (PF_SCAN_MAX_TOKEN_AGE_MIN * 1.5 if PF_SCAN_MAX_TOKEN_AGE_MIN > 0 else 0.0),
)
RAY_MAX_AGE_MIN = _f("RAY_MAX_AGE_MIN", 0.0)               # hard cap if set (>0)
HARD_MIN_LIQ_USD = _f("RAYDIUM_HARD_MIN_LIQ_USD", 0.0)     # optional (>0)

# Scoring integration
SCORING_ENABLED = _b("RAYDIUM_SCORING_ENABLED", True)
SCORING_MAX_PER_SCAN = max(5, _i("RAYDIUM_SCORING_MAX_PER_SCAN", 80))
SCORING_CONCURRENCY = max(1, _i("RAYDIUM_SCORING_CONCURRENCY", 6))
SCORING_MIN_COMBINED = _f("RAYDIUM_SCORING_MIN_COMBINED", 55.0)
SCORING_MIN_LABEL = _s("RAYDIUM_SCORING_MIN_LABEL", "YELLOW").upper().strip()
SCORING_USE_PF_GATING = _b("RAYDIUM_SCORING_USE_PF_GATING", True)

# Dynamic threshold tuning
DYNAMIC_THRESH_ENABLED = _b("RAYDIUM_DYNAMIC_THRESH_ENABLED", True)
DYNAMIC_THRESH_MAX_DELTA = _f("RAYDIUM_DYNAMIC_THRESH_MAX_DELTA", 8.0)

# Optional enrichment
AI_RUG_ENABLED = _b("RAYDIUM_AI_RUG_ENABLED", False)
AI_RUG_MAX_PER_SCAN = max(0, _i("RAYDIUM_AI_RUG_MAX_PER_SCAN", 15))
RAYDIUM_MAX_AI_RUG = _f("RAYDIUM_MAX_AI_RUG", 0.88)

SMART_WALLETS_ENRICH = _b("RAYDIUM_SMART_WALLETS_ENRICH", True)
SMART_WALLETS_MAX_PER_SCAN = max(0, _i("RAYDIUM_SMART_WALLETS_MAX_PER_SCAN", 80))

# Alerts / diagnostics
SEND_MOMO_ALERTS = _b("RAYDIUM_SEND_MOMO_ALERTS", True)
MAX_ALERTS_PER_SCAN = max(0, _i("RAYDIUM_MAX_ALERTS_PER_SCAN", 3))
ALERT_DEDUP_SEC = max(10.0, _f("RAYDIUM_ALERT_DEDUP_SEC", 180.0))

DEBUG_META_SAMPLE = _b("RAYDIUM_DEBUG_META_SAMPLE", True)
DEBUG_SCORE_RESULT = _b("RAYDIUM_DEBUG_SCORE_RESULT", True)
RAYDIUM_LOG_SCAN_SUMMARY = _b("RAYDIUM_LOG_SCAN_SUMMARY", True)
RAYDIUM_LOG_CANDIDATES = _b("RAYDIUM_LOG_CANDIDATES", True)
RAYDIUM_LOG_REJECTS = _b("RAYDIUM_LOG_REJECTS", True)
RAYDIUM_REJECT_SAMPLE_N = max(0, _i("RAYDIUM_REJECT_SAMPLE_N", 2))
RAYDIUM_REJECT_LOG_EVERY_SEC = max(1, _i("RAYDIUM_REJECT_LOG_EVERY_SEC", 10))

RAYDIUM_REGIME_TAG = _s("RAYDIUM_REGIME_TAG", "raydium_live")
RAYDIUM_WALLET_TAG = _s("RAYDIUM_WALLET_TAG", "")

_LABEL_LEVEL = {"RED": 0, "YELLOW": 1, "GREEN": 2}
_REQUIRED_LABEL_LEVEL = _LABEL_LEVEL.get(SCORING_MIN_LABEL, 0)

# Redis dedup (optional)
RUNNER_DEDUP_ENABLED = _b("RAYDIUM_RUNNER_DEDUP_ENABLED", True)
RUNNER_DEDUP_SEC = max(0, _i("RAYDIUM_RUNNER_DEDUP_SEC", 180))
REDIS_KEY_PREFIX = _s("RAYDIUM_REDIS_PREFIX", "raydium")

# Run-loop knobs (if you use run()/scan_loop())
SCAN_INTERVAL_SEC = max(0.25, _f("RAYDIUM_SCAN_INTERVAL_SEC", 2.0))
SCAN_JITTER_SEC = max(0.0, _f("RAYDIUM_SCAN_JITTER_SEC", 0.35))

_recent_alert_ts: Dict[str, float] = {}
_recent_alert_lock = asyncio.Lock()

_reject_log_last_ts: float = 0.0

# ──────────────────────────────────────────────────────────────────────────────
# Optional module hooks (soft imports)
# ──────────────────────────────────────────────────────────────────────────────
# PF gating helper (compat)
try:
    from utils_pkg.pair_filters import is_viable_pair  # type: ignore
except Exception:  # pragma: no cover
    try:
        from utils_pkg.pump_filters import is_viable_pair  # type: ignore
    except Exception:  # pragma: no cover

        def is_viable_pair(_meta: Dict[str, Any]) -> bool:  # type: ignore
            return True


# scoring_service
try:
    from utils_pkg.scoring_service import score_candidate  # type: ignore
except Exception:  # pragma: no cover
    score_candidate = None  # type: ignore


# Alerts
try:
    from utils_pkg.alert_manager import AlertManager  # type: ignore
except Exception:  # pragma: no cover
    AlertManager = None  # type: ignore


# Structured logging (optional)
try:
    from utils_pkg.data_logger import log_raydium_candidate, log_scored_token, log_event  # type: ignore
except Exception:  # pragma: no cover

    def log_raydium_candidate(*_: Any, **__: Any) -> None:  # type: ignore
        return None

    def log_scored_token(*_: Any, **__: Any) -> None:  # type: ignore
        return None

    async def log_event(*_: Any, **__: Any) -> None:  # type: ignore
        return None


# Regime (optional)
try:
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


# Risk manager snapshot (optional)
try:
    from utils_pkg.risk_manager import risk_capacity_snapshot  # type: ignore
except Exception:  # pragma: no cover

    async def risk_capacity_snapshot(_redis=None) -> Dict[str, Any]:
        return {"risk_capacity": 1.0, "risk_shutdown": False, "risk_mode": "normal"}


# ──────────────────────────────────────────────────────────────────────────────
# CandidateSnapshot normalization
# ──────────────────────────────────────────────────────────────────────────────
def _to_candidate_snapshot(meta: Dict[str, Any]) -> Dict[str, Any]:
    """
    Canonical schema for scanner -> sniper handoff.
    Keeps compat aliases used across older codepaths.
    """
    m = dict(meta)

    # Canonical fields
    mint = str(m.get("mint") or "").strip()
    sym = str(m.get("symbol") or "").strip() or (mint[:6] if mint else "")
    now = float(m.get("scan_ts") or time.time())

    snap: Dict[str, Any] = {
        # identity
        "mint": mint,
        "symbol": sym,
        "chain": str(m.get("chain") or "SOL"),
        "dex": str(m.get("dex") or "raydium"),
        "source": str(m.get("source") or "raydium"),
        "scanner": str(m.get("scanner") or "raydium_scanner"),
        "quote_symbol": str(m.get("quote_symbol") or ""),
        "quote_mint": str(m.get("quote_mint") or ""),
        "pair_address": str(m.get("pair_address") or ""),
        "pair_url": str(m.get("pair_url") or ""),
        "scan_ts": now,

        # core market stats (canonical names)
        "liq_usd": float(m.get("liq_usd") or 0.0),
        "vol_5m_usd": float(m.get("vol_5m_usd") or 0.0),
        "vol_1h_usd": float(m.get("vol_1h_usd") or 0.0),
        "vol_24h_usd": float(m.get("vol_24h_usd") or 0.0),
        "buys_5m": int(m.get("buys_5m") or 0),
        "sells_5m": int(m.get("sells_5m") or 0),
        "txns_5m": int(m.get("txns_5m") or (int(m.get("buys_5m") or 0) + int(m.get("sells_5m") or 0))),
        "price_usd": float(m.get("price_usd") or 0.0),
        "price_change_5m_pct": float(m.get("price_change_5m_pct") or 0.0),
        "price_change_15m_pct": float(m.get("price_change_15m_pct") or 0.0),
        "price_change_1h_pct": float(m.get("price_change_1h_pct") or 0.0),
        "price_change_24h_pct": float(m.get("price_change_24h_pct") or 0.0),
        "token_age_min": float(m.get("token_age_min") or 9999.0),

        # derived / hints
        "buy_ratio_5m": float(m.get("buy_ratio_5m") or 0.0),
        "vol_to_liq_5m": float(m.get("vol_to_liq_5m") or 0.0),
        "momo_hint": float(m.get("momo_hint") or 0.0),

        # scoring/enrichment (optional)
        "ai_rug_score": float(m.get("ai_rug_score") or 0.0),
        "rank_score": m.get("rank_score", None),
        "score_combined": m.get("score_combined", None),
        "score_label": m.get("score_label", None),
        "score_ai": m.get("score_ai", None),
        "score_risk": m.get("score_risk", None),
        "score_quality": m.get("score_quality", None),
        "score_momentum": m.get("score_momentum", None),

        # risk/regime context (optional)
        "risk_capacity": m.get("risk_capacity", None),
        "risk_mode": m.get("risk_mode", None),
        "regime_label": m.get("regime_label", None),
        "regime_numeric": m.get("regime_numeric", None),
        "regime_manipulation": m.get("regime_manipulation", None),
    }

    # Compatibility aliases expected by legacy filters/scorers
    snap["liq"] = snap["liq_usd"]
    snap["vol5"] = snap["vol_5m_usd"]
    snap["buys5"] = snap["buys_5m"]
    snap["sells5"] = snap["sells_5m"]
    snap["chg5"] = snap["price_change_5m_pct"]
    snap["age_min"] = snap["token_age_min"]

    # Keep original raw meta available for deep debugging/feature builder
    snap["raw_meta"] = m

    return snap


# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────
def _headers(accept: str = "application/json") -> Dict[str, str]:
    return {
        "User-Agent": UA,
        "Accept": accept,
        "Accept-Language": "en-US,en;q=0.9",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
    }


def _age_min(pair_created_ms: Optional[int]) -> Optional[float]:
    try:
        if pair_created_ms is None:
            return None
        t_ms = int(pair_created_ms)
        if t_ms <= 0:
            return None
        diff_s = max(0.0, time.time() - (t_ms / 1000.0))
        return diff_s / 60.0
    except Exception:
        return None


def _buy_ratio(buys: int, sells: int) -> float:
    total = max(1, buys + sells)
    return float(buys) / float(total)


def _vol_key(vol: Dict[str, Any], key1: str, alt_key: Optional[str] = None) -> float:
    raw = vol.get(key1)
    if isinstance(raw, dict):
        raw = raw.get("usd")
    if raw is None and alt_key:
        raw = vol.get(alt_key)
        if isinstance(raw, dict):
            raw = raw.get("usd")
    return _nzf(raw, 0.0)


def _sol_price_usd_best_effort() -> float:
    v = _f("SOL_PRICE_USD", 0.0)
    if v > 0:
        return v
    return 150.0


# ──────────────────────────────────────────────────────────────────────────────
# Redis helpers (optional; supports sync or async redis client)
# ──────────────────────────────────────────────────────────────────────────────
async def _maybe_await(x: Any) -> Any:
    if asyncio.iscoroutine(x):
        return await x
    return x


async def _redis_get(redis: Any, key: str) -> Optional[str]:
    if redis is None:
        return None
    try:
        v = await _maybe_await(redis.get(key))
        if v is None:
            return None
        if isinstance(v, (bytes, bytearray)):
            return v.decode("utf-8", "ignore")
        return str(v)
    except Exception:
        return None


async def _redis_setex(redis: Any, key: str, ttl: int, value: str) -> None:
    if redis is None:
        return
    try:
        if hasattr(redis, "setex"):
            await _maybe_await(redis.setex(key, int(ttl), value))
            return
        await _maybe_await(redis.set(key, value))
        if hasattr(redis, "expire"):
            await _maybe_await(redis.expire(key, int(ttl)))
    except Exception:
        return


# ──────────────────────────────────────────────────────────────────────────────
# Regime / dynamic thresholds
# ──────────────────────────────────────────────────────────────────────────────
async def _regime_snapshot() -> Dict[str, Any]:
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
        with contextlib.suppress(Exception):
            return {"label": regime_state.get(), "label_numeric": 0.0, "manipulation_risk": 0.0}  # type: ignore[misc]
        return {"label": "unknown", "label_numeric": 0.0, "manipulation_risk": 0.0}


def _regime_tune_soft(base_soft_liq: float, base_fb_min_vol5: float, regime: Dict[str, Any]) -> Tuple[float, float]:
    ln = float(regime.get("label_numeric", 0.0))
    manip = float(regime.get("manipulation_risk", 0.0))
    soft_liq = float(base_soft_liq)
    fb_min_vol5 = float(base_fb_min_vol5)

    if ln >= 0.7 and manip < 0.6:
        soft_liq *= 0.70
        fb_min_vol5 *= 0.70
    elif ln <= -0.7 or manip >= 0.8:
        soft_liq *= 1.40
        fb_min_vol5 *= 1.30

    return max(0.0, soft_liq), max(0.0, fb_min_vol5)


def _dynamic_min_combined(base: float, regime: Dict[str, Any], risk_capacity: float) -> float:
    if not DYNAMIC_THRESH_ENABLED:
        return float(base)

    base = float(base)
    ln = float(regime.get("label_numeric", 0.0))
    manip = float(regime.get("manipulation_risk", 0.0))
    cap = _clamp(float(risk_capacity), 0.0, 1.0)

    cap_delta = (1.0 - cap) * float(DYNAMIC_THRESH_MAX_DELTA)

    regime_delta = 0.0
    if ln >= 0.7:
        regime_delta -= 0.5 * float(DYNAMIC_THRESH_MAX_DELTA)
    elif ln <= -0.7:
        regime_delta += 0.5 * float(DYNAMIC_THRESH_MAX_DELTA)

    manip_delta = _clamp(manip, 0.0, 1.0) * (0.5 * float(DYNAMIC_THRESH_MAX_DELTA))

    delta = _clamp(
        cap_delta + regime_delta + manip_delta,
        -float(DYNAMIC_THRESH_MAX_DELTA),
        float(DYNAMIC_THRESH_MAX_DELTA),
    )
    return float(_clamp(base + delta, 0.0, 100.0))


# ──────────────────────────────────────────────────────────────────────────────
# HTTP fetch (Dexscreener)
# ──────────────────────────────────────────────────────────────────────────────
async def _fetch_json(
    session: "aiohttp.ClientSession",
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
                headers=_headers("application/json, text/plain, */*"),
                timeout=aiohttp.ClientTimeout(total=TIMEOUT_S),  # type: ignore[attr-defined]
                proxy=HTTP_PROXY or None,
            ) as r:
                status = int(r.status)
                text = (await r.text()) or ""
                first = text[:140]

                if status == 200:
                    try:
                        return await r.json(content_type=None), status, first
                    except Exception:
                        with contextlib.suppress(Exception):
                            return json.loads(text), status, first
                        return None, status, first

                if 400 <= status < 500 and status != 429:
                    log.every("RAY_HTTP_4XX", n=15, status=status, url=url, first=first)
                    return None, status, first

                if status == 429:
                    ra = (r.headers.get("Retry-After") or "").strip()
                    wait = float(ra) if ra.isdigit() else (BACKOFF_S * attempt * 2.0)
                    wait = _clamp(wait, 0.25, 8.0)
                    await asyncio.sleep(wait)
                else:
                    await asyncio.sleep(BACKOFF_S * attempt * (0.85 + 0.3 * random.random()))

        except asyncio.CancelledError:
            raise
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:  # type: ignore[attr-defined]
            log.every("RAY_HTTP_ERR", n=10, err=type(e).__name__, url=url, attempt=attempt, retries=RETRIES)
            if attempt < RETRIES:
                await asyncio.sleep(BACKOFF_S * attempt * (0.85 + 0.3 * random.random()))
        except Exception as e:
            log.every("RAY_HTTP_UNK", n=10, err=_short(e), url=url, attempt=attempt, retries=RETRIES)
            if attempt < RETRIES:
                await asyncio.sleep(BACKOFF_S * attempt * (0.85 + 0.3 * random.random()))

    return None, status, first


async def _fetch_pairs(session: "aiohttp.ClientSession") -> List[Dict[str, Any]]:
    sem = asyncio.Semaphore(FETCH_CONCURRENCY)
    out: List[Dict[str, Any]] = []
    seen_pair_ids: Set[str] = set()

    async def _one(q: str) -> None:
        try:
            params = {"q": q, "limit": SEARCH_LIMIT}
            async with sem:
                data, status, _first = await _fetch_json(session, SEARCH_URL, params=params)
            if not data:
                log.every("RAY_FETCH_EMPTY", n=20, q=q, status=status)
                return

            pairs = data.get("pairs") or []
            for p in pairs:
                if (p.get("chainId") or "").lower() != "solana":
                    continue
                if (p.get("dexId") or "").lower() != "raydium":
                    continue

                pid = (p.get("pairAddress") or "").strip()
                if not pid:
                    pid = ((p.get("baseToken") or {}).get("address") or "").strip()
                if not pid or pid in seen_pair_ids:
                    continue
                seen_pair_ids.add(pid)
                out.append(p)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            log.every("RAY_FETCH_ONE_FAIL", n=10, q=q, err=_short(e))

    qs = (SEARCH_QUERIES[:20] or ["raydium"])
    tasks = [asyncio.create_task(_one(q)) for q in qs]
    if tasks:
        await asyncio.gather(*tasks)

    log.every("RAY_FETCH_DONE", n=10, raw_pairs=len(out), queries=",".join(qs), limit=SEARCH_LIMIT)
    return out


# ──────────────────────────────────────────────────────────────────────────────
# Normalize pair -> meta (+ reject reason)
# ──────────────────────────────────────────────────────────────────────────────
def _extract_metrics(pair: Dict[str, Any]) -> Dict[str, Any]:
    liq = pair.get("liquidity") or {}
    liq_usd = _nzf(liq.get("usd"), 0.0)

    fdv_usd = _nzf(pair.get("fdv"), 0.0)
    mcap_usd = _nzf(pair.get("marketCap") or pair.get("marketCapUsd") or pair.get("mcap") or fdv_usd, 0.0)

    txs = pair.get("txns") or {}
    m5_tx = txs.get("m5") or txs.get("5m") or {}

    buys_5m = _nzi(m5_tx.get("buys"), 0)
    sells_5m = _nzi(m5_tx.get("sells"), 0)

    vol = pair.get("volume") or {}
    vol_5m_usd = _vol_key(vol, "m5", "5m")
    vol_1h_usd = _vol_key(vol, "h1", "1h")
    vol_24h_usd = _vol_key(vol, "h24", "24h")

    if vol_5m_usd <= 0.0 and vol_1h_usd > 0.0:
        vol_5m_usd = vol_1h_usd / 12.0
    if vol_1h_usd <= 0.0 and vol_24h_usd > 0.0:
        vol_1h_usd = vol_24h_usd / 24.0
    if vol_5m_usd <= 0.0 and vol_24h_usd > 0.0:
        vol_5m_usd = vol_24h_usd / 288.0

    pc = pair.get("priceChange") or {}
    chg_5m_pct = _nzf(pc.get("m5") or pc.get("5m"), 0.0)
    chg_1h_pct = _nzf(pc.get("h1") or pc.get("1h"), 0.0)
    chg_24h_pct = _nzf(pc.get("h24") or pc.get("24h"), 0.0)

    if chg_5m_pct == 0.0 and chg_1h_pct != 0.0:
        chg_5m_pct = chg_1h_pct / 12.0
    if chg_5m_pct == 0.0 and chg_24h_pct != 0.0:
        chg_5m_pct = chg_24h_pct / 288.0
    chg_15m_pct = (
        (chg_1h_pct / 4.0)
        if chg_1h_pct != 0.0
        else (chg_24h_pct / 96.0 if chg_24h_pct != 0.0 else chg_5m_pct)
    )

    price_usd = _nzf(pair.get("priceUsd") or pair.get("price"), 0.0)

    pair_created_ms = pair.get("pairCreatedAt") or pair.get("pairCreatedAtMs") or pair.get("pairCreatedAtTs")
    age_min = _age_min(pair_created_ms)
    token_age_min = age_min if age_min is not None else 9999.0

    whales_5m = 0
    whale_vol_5m_usd = 0.0
    approx_largest_buy_sol = 0.0
    if vol_5m_usd > 0:
        approx_largest_buy_usd = vol_5m_usd * 0.25
        sol_usd = max(1.0, _sol_price_usd_best_effort())
        approx_largest_buy_sol = approx_largest_buy_usd / sol_usd
        whale_vol_5m_usd = approx_largest_buy_usd
        pf_min_whale_sol = float(_cfg("PF_SCAN_MIN_WHALE_SIZE_SOL", 3.0) or 3.0)
        if approx_largest_buy_sol >= pf_min_whale_sol:
            whales_5m = 1

    buy_ratio_5m = _buy_ratio(buys_5m, sells_5m)
    vol_to_liq = (vol_5m_usd / max(1.0, liq_usd)) if liq_usd > 0 else 0.0
    momo_hint = (buy_ratio_5m * 100.0) + (min(2.0, vol_to_liq) * 25.0) + (max(-20.0, min(20.0, chg_5m_pct)) * 0.8)

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
        "txns_5m": buys_5m + sells_5m,
        "price_usd": price_usd,
        "price_change_5m_pct": chg_5m_pct,
        "price_change_15m_pct": chg_15m_pct,
        "price_change_1h_pct": chg_1h_pct,
        "price_change_24h_pct": chg_24h_pct,
        "buy_ratio_5m": buy_ratio_5m,
        "vol_to_liq_5m": vol_to_liq,
        "momo_hint": momo_hint,
        "whales_5m": whales_5m,
        "whale_vol_5m_usd": whale_vol_5m_usd,
        "approx_largest_buy_sol": approx_largest_buy_sol,
    }


def _quote_allowed(quote_symbol: str, quote_mint: str) -> bool:
    qs = (quote_symbol or "").strip().upper()
    qm = (quote_mint or "").strip()
    if not REQUIRE_SOL_OR_USDC_QUOTE:
        return True
    if qs and (qs in ALLOWED_QUOTES):
        return True
    if qm in {_SOL_MINT, _USDC_MINT}:
        return True
    return False


def _normalize_to_meta(pair: Dict[str, Any]) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    try:
        if (pair.get("chainId") or "").lower() != "solana":
            return None, "reject_chain"
        if (pair.get("dexId") or "").lower() != "raydium":
            return None, "reject_dex"

        base = pair.get("baseToken") or {}
        quote = pair.get("quoteToken") or {}

        mint = _normalize_mint((base.get("address") or "").strip())
        if not mint:
            return None, "reject_no_mint"

        symbol = (base.get("symbol") or "").strip() or mint[:6]
        quote_symbol = (quote.get("symbol") or "").strip().upper()
        quote_mint = (quote.get("address") or "").strip()

        if not _quote_allowed(quote_symbol, quote_mint):
            return None, "reject_quote"

        metrics = _extract_metrics(pair)
        pair_addr = (pair.get("pairAddress") or "").strip()

        url = pair.get("url") or pair.get("pairUrl")
        if not url:
            url = f"https://dexscreener.com/solana/{pair_addr}" if pair_addr else f"https://dexscreener.com/solana/{mint}"

        ts = time.time()

        meta = {
            "dex": "raydium",
            "chain": "SOL",
            "source": "raydium",
            "scanner": "raydium_scanner",
            "mint": mint,
            "symbol": symbol,
            "quote_symbol": quote_symbol,
            "quote_mint": quote_mint,
            "pair_address": pair_addr,
            "pair_url": url,
            "scan_ts": ts,
            **metrics,
            # compat placeholders
            "ai_rug_score": 0.0,
            "rank_score": None,
            "is_honeypot": False,
            "trading_paused": False,
            "blacklisted": False,
            "top10_hold_pct": 0.0,
            "dev_hold_pct": 0.0,
            "mint_authority_disabled": True,
            "freeze_authority_disabled": True,
        }
        return meta, None
    except asyncio.CancelledError:
        raise
    except Exception as e:
        log.every("RAY_NORMALIZE_FAIL", n=10, err=_short(e))
        return None, "reject_normalize_exc"


# ──────────────────────────────────────────────────────────────────────────────
# Optional enrichment (AI rug + smart wallets)
# ──────────────────────────────────────────────────────────────────────────────
async def _enrich_optional(metas: List[Dict[str, Any]]) -> None:
    if not metas:
        return

    ai_fraud_score = None
    if AI_RUG_ENABLED:
        with contextlib.suppress(Exception):
            from utils_pkg.core_utils import ai_fraud_score as _afs  # type: ignore
            ai_fraud_score = _afs  # type: ignore

    sw_tracker = None
    if SMART_WALLETS_ENRICH:
        with contextlib.suppress(Exception):
            from utils_pkg.smart_wallets import TRACKER as SW_TRACKER  # type: ignore
            sw_tracker = SW_TRACKER  # type: ignore

    if (AI_RUG_ENABLED and not ai_fraud_score) and _fast_fail():
        raise RuntimeError("RAYDIUM_AI_RUG_ENABLED=1 but utils_pkg.core_utils.ai_fraud_score missing")

    ai_targets = metas[:AI_RUG_MAX_PER_SCAN] if (AI_RUG_ENABLED and ai_fraud_score) else []
    sw_targets = metas[:SMART_WALLETS_MAX_PER_SCAN] if sw_tracker is not None else []
    if not ai_targets and not sw_targets:
        return

    ai_set = {id(m) for m in ai_targets}
    sw_set = {id(m) for m in sw_targets}

    sem = asyncio.Semaphore(max(1, _i("RAYDIUM_ENRICH_CONCURRENCY", 8)))

    async def _one(m: Dict[str, Any]) -> None:
        mint = m.get("mint")
        if not isinstance(mint, str) or not mint:
            return
        async with sem:
            if id(m) in ai_set and ai_fraud_score:
                s01 = await ai_fraud_score(mint)  # type: ignore[misc]
                if s01 is not None:
                    m["ai_rug_score"] = _clamp(float(s01), 0.0, 1.0)

            if id(m) in sw_set and sw_tracker is not None:
                feats = await sw_tracker.build_features(mint)  # type: ignore[misc]
                if isinstance(feats, dict):
                    m["smart_wallet_score"] = float(feats.get("smart_wallet_score", 0.5))
                    m["smart_signal_confidence"] = float(feats.get("smart_signal_confidence", 0.0))
                    m["smart_signal_quality"] = float(feats.get("smart_signal_quality", 0.0))
                    m["smart_risk_flag"] = float(feats.get("smart_risk_flag", 0.0))
                    m["smart_entry_boost_hint"] = float(feats.get("smart_entry_boost_hint", 0.0))
                    m["smart_exit_pressure_hint"] = float(feats.get("smart_exit_pressure_hint", 0.0))
                    m["smart_tradeable_flag"] = float(feats.get("smart_tradeable_flag", 0.0))

    tasks = [asyncio.create_task(_one(m)) for m in metas]
    if tasks:
        await asyncio.gather(*tasks)


# ──────────────────────────────────────────────────────────────────────────────
# Scoring
# ──────────────────────────────────────────────────────────────────────────────
async def _maybe_await2(x: Any) -> Any:
    if asyncio.iscoroutine(x):
        return await x
    return x


async def _score_metas(
    metas: List[Dict[str, Any]],
    *,
    regime: Dict[str, Any],
    risk_ctx: Optional[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    if not metas or not SCORING_ENABLED or score_candidate is None:
        return []

    pf: List[Dict[str, Any]] = []
    for m in metas:
        try:
            if SCORING_USE_PF_GATING and not is_viable_pair(m):
                continue
            if AI_RUG_ENABLED and float(m.get("ai_rug_score") or 0.0) >= float(RAYDIUM_MAX_AI_RUG):
                continue
            pf.append(m)
        except Exception:
            continue
    if not pf:
        pf = metas

    def _k(x: Dict[str, Any]) -> Tuple[float, float, int]:
        return (
            float(x.get("vol_5m_usd") or 0.0),
            float(x.get("momo_hint") or 0.0),
            int(x.get("buys_5m") or 0),
        )

    pf.sort(key=_k, reverse=True)
    targets = pf[:SCORING_MAX_PER_SCAN]

    sem = asyncio.Semaphore(SCORING_CONCURRENCY)
    out: List[Dict[str, Any]] = []

    async def _one(meta: Dict[str, Any]) -> None:
        raw = dict(meta)
        raw.setdefault("age_min", raw.get("token_age_min"))
        raw.setdefault("chg5", raw.get("price_change_5m_pct"))
        raw.setdefault("buys5", raw.get("buys_5m"))
        raw.setdefault("sells5", raw.get("sells_5m"))
        raw.setdefault("liq", raw.get("liq_usd"))
        raw.setdefault("vol5", raw.get("vol_5m_usd"))

        ctx = {
            "source": "raydium",
            "scanner": "raydium_scanner",
            "phase": MASTER_PHASE,
            "mode": RAYDIUM_MODE,
            "regime": dict(regime),
            "risk": dict(risk_ctx or {}),
        }

        async with sem:
            st = await _maybe_await2(score_candidate(raw, context=ctx))  # type: ignore[misc]

        m = dict(meta)
        m["score_combined"] = float(getattr(st, "combined_score", 0.0) or 0.0)
        m["score_label"] = str(getattr(st, "label", "") or "")
        m["score_momentum"] = float(getattr(st, "momentum_score", 0.0) or 0.0)
        m["score_risk"] = float(getattr(st, "risk_score", 0.0) or 0.0)
        m["score_quality"] = float(getattr(st, "quality_score", 0.0) or 0.0)
        m["score_ai"] = float(getattr(st, "ai_score", 0.0) or 0.0)
        m["score_reasons"] = getattr(st, "reasons", None)
        m["score_is_trade_viable"] = bool(getattr(st, "is_trade_viable", True))
        m["score_trade_gate_reason"] = getattr(st, "trade_gate_reason", None)

        m["rank_score"] = float(m["score_combined"])
        m["ai_entry_score"] = float(m["score_ai"])
        m["ai_entry_reason"] = m.get("score_reasons")

        with contextlib.suppress(Exception):
            if hasattr(st, "to_dict"):
                d = st.to_dict()
                m["scored_token"] = d
                try:
                    log_scored_token(d, source="raydium")  # type: ignore[misc]
                except TypeError:
                    log_scored_token(d)  # type: ignore[misc]

        out.append(m)

    tasks = [asyncio.create_task(_one(m)) for m in targets]
    if tasks:
        await asyncio.gather(*tasks)

    return out


# ──────────────────────────────────────────────────────────────────────────────
# Fallback pick if scoring yields nothing
# ──────────────────────────────────────────────────────────────────────────────
def _fallback_pick(metas: List[Dict[str, Any]], fb_min_vol5: float) -> List[Dict[str, Any]]:
    if not metas:
        return []

    fb_min_buys = int(_i("RAYDIUM_FB_MIN_BUYS_5M", 1))
    fb_min_vol = float(_f("RAYDIUM_FB_MIN_VOL5_USD", fb_min_vol5))
    fb_max = int(_i("RAYDIUM_FB_MAX_RUNNERS", 3))

    cand: List[Dict[str, Any]] = []
    for m in metas:
        try:
            if fb_min_buys > 0 and int(m.get("buys_5m") or 0) < fb_min_buys:
                continue
            if fb_min_vol > 0 and float(m.get("vol_5m_usd") or 0.0) < fb_min_vol:
                continue
            if AI_RUG_ENABLED and float(m.get("ai_rug_score") or 0.0) >= float(RAYDIUM_MAX_AI_RUG):
                continue
            cand.append(m)
        except Exception:
            continue

    if not cand:
        return []

    def _key(m: Dict[str, Any]) -> Tuple[float, float, float]:
        return (
            float(m.get("momo_hint") or 0.0),
            float(m.get("vol_5m_usd") or 0.0),
            float(m.get("liq_usd") or 0.0),
        )

    cand.sort(key=_key, reverse=True)
    return cand[:fb_max]


# ──────────────────────────────────────────────────────────────────────────────
# MOMO alerts + AI training logs
# ──────────────────────────────────────────────────────────────────────────────
async def _send_momo(runners: List[Dict[str, Any]]) -> None:
    if not SEND_MOMO_ALERTS or not runners or AlertManager is None:
        return
    try:
        am = AlertManager()  # type: ignore[misc]
    except Exception as e:
        log.every("RAY_ALERT_INIT_FAIL", n=30, err=_short(e))
        return

    max_n = max(0, min(MAX_ALERTS_PER_SCAN, len(runners)))

    async with _recent_alert_lock:
        now = time.time()
        cutoff = now - max(10.0, float(ALERT_DEDUP_SEC))
        for k in list(_recent_alert_ts.keys()):
            if _recent_alert_ts.get(k, 0.0) < cutoff:
                _recent_alert_ts.pop(k, None)

    for c in runners[:max_n]:
        try:
            mint = str(c.get("mint") or "")
            if not mint:
                continue

            async with _recent_alert_lock:
                now = time.time()
                last = _recent_alert_ts.get(mint, 0.0)
                if now - last < float(ALERT_DEDUP_SEC):
                    continue
                _recent_alert_ts[mint] = now

            age_min = c.get("token_age_min")
            age_sec = float(age_min) * 60.0 if isinstance(age_min, (int, float)) else None

            ctx: Dict[str, Any] = {
                "symbol": c.get("symbol") or mint,
                "mint": mint,
                "pool": "raydium",
                "pool_url": c.get("pair_url"),
                "age_sec": age_sec,
                "liq_usd": c.get("liq_usd"),
                "vol_5m": c.get("vol_5m_usd"),
                "up_pct": c.get("price_change_5m_pct"),
                "momo_hint": c.get("momo_hint"),
                "score_combined": c.get("score_combined"),
                "score_label": c.get("score_label"),
                "score_ai": c.get("score_ai"),
                "ai_rug_score": c.get("ai_rug_score"),
                "risk_capacity": c.get("risk_capacity"),
                "risk_mode": c.get("risk_mode"),
            }
            await am.send_momo_event("MOMO_OK", ctx)  # type: ignore[misc]
        except asyncio.CancelledError:
            raise
        except Exception as e:
            log.every("RAY_MOMO_SEND_FAIL", n=10, mint=str(c.get("mint")), err=_short(e))


def _log_ai_candidates(runners: List[Dict[str, Any]], *, regime: Dict[str, Any], risk_ctx: Optional[Dict[str, Any]]) -> None:
    if not RAYDIUM_LOG_CANDIDATES or not runners:
        return

    try:
        live_regime = regime.get("label") or regime_state.get()  # type: ignore[misc]
    except Exception:
        live_regime = None

    regime_tag = live_regime or RAYDIUM_REGIME_TAG or "raydium_live"
    wallet_tag = RAYDIUM_WALLET_TAG or None

    risk_capacity = None
    risk_mode = None
    if risk_ctx:
        with contextlib.suppress(Exception):
            risk_capacity = float(risk_ctx.get("risk_capacity", 1.0) or 1.0)
        risk_mode = risk_ctx.get("risk_mode", None)

    for r in runners:
        try:
            mint = r.get("mint")
            if not isinstance(mint, str) or not mint:
                continue

            meta: Dict[str, Any] = {
                "liq": float(r.get("liq_usd") or 0.0),
                "vol5": float(r.get("vol_5m_usd") or 0.0),
                "buys5": int(r.get("buys_5m") or 0),
                "sells5": int(r.get("sells_5m") or 0),
                "chg5": float(r.get("price_change_5m_pct") or 0.0),
                "age_min": float(r.get("token_age_min") or 0.0),
                "scan_ts": float(r.get("scan_ts") or time.time()),
                "ai_rug_score": float(r.get("ai_rug_score") or 0.0),
                "pair_url": r.get("pair_url"),
                "pair_address": r.get("pair_address"),
                "momo_hint": float(r.get("momo_hint") or 0.0),
            }

            for k in ("score_combined", "score_ai", "score_risk", "score_quality", "score_momentum"):
                if k in r:
                    with contextlib.suppress(Exception):
                        meta[k] = float(r.get(k) or 0.0)
            if "score_label" in r:
                meta["score_label"] = str(r.get("score_label") or "")

            if risk_capacity is not None:
                meta["risk_capacity"] = float(risk_capacity)
            if risk_mode is not None:
                meta["risk_mode"] = str(risk_mode)

            meta["raw_meta"] = dict(r)

            log_raydium_candidate(mint=mint, symbol=r.get("symbol"), meta=meta, regime=regime_tag, wallet_tag=wallet_tag)  # type: ignore[misc]
        except Exception:
            continue


# ──────────────────────────────────────────────────────────────────────────────
# Scan stats + reject tracking
# ──────────────────────────────────────────────────────────────────────────────
@dataclass
class _ScanStats:
    t0: float
    raw_pairs: int = 0
    metas: int = 0
    soft_metas: int = 0
    enriched: int = 0
    scored: int = 0
    runners: int = 0
    fb_runners: int = 0
    risk_capacity: float = 1.0
    risk_mode: str = "normal"
    regime_label: str = "unknown"
    regime_numeric: float = 0.0
    manip_risk: float = 0.0

    # reject bookkeeping
    rejects: Dict[str, int] = field(default_factory=dict)
    reject_samples: Dict[str, List[Dict[str, Any]]] = field(default_factory=dict)

    def rej(self, reason: str, sample: Optional[Dict[str, Any]] = None) -> None:
        r = reason or "reject_unknown"
        self.rejects[r] = int(self.rejects.get(r, 0)) + 1
        if sample is not None and RAYDIUM_REJECT_SAMPLE_N > 0:
            arr = self.reject_samples.get(r) or []
            if len(arr) < RAYDIUM_REJECT_SAMPLE_N:
                arr.append(sample)
                self.reject_samples[r] = arr


def _maybe_log_rejects(stats: _ScanStats, *, force: bool = False) -> None:
    global _reject_log_last_ts
    if not RAYDIUM_LOG_REJECTS:
        return
    if not stats.rejects:
        return
    now = time.time()
    if (not force) and (now - float(_reject_log_last_ts)) < float(RAYDIUM_REJECT_LOG_EVERY_SEC):
        return
    _reject_log_last_ts = now

    top = sorted(stats.rejects.items(), key=lambda kv: kv[1], reverse=True)[:12]
    payload: Dict[str, Any] = {
        "raw_pairs": stats.raw_pairs,
        "metas": stats.metas,
        "soft": stats.soft_metas,
        "scored": stats.scored,
        "runners": stats.runners,
        "fb": stats.fb_runners,
        "top_rejects": top,
    }
    if RAYDIUM_REJECT_SAMPLE_N > 0 and stats.reject_samples:
        payload["samples"] = stats.reject_samples
    log.every("RAY_REJECT_SUMMARY", n=5, **payload)


# ──────────────────────────────────────────────────────────────────────────────
# Public: scan_raydium
# ──────────────────────────────────────────────────────────────────────────────
async def scan_raydium(*args: Any) -> List[Dict[str, Any]]:
    """
    Flexible signatures supported:
      - scan_raydium()
      - scan_raydium(redis)
      - scan_raydium(session)
      - scan_raydium(redis, session)
    """
    if not RAYDIUM_ENABLED or RAYDIUM_MODE == "off":
        return []
    if aiohttp is None:
        log.every("RAY_NO_AIOHTTP", n=60, msg="aiohttp missing; raydium scanner disabled")
        return []

    redis = None
    sess: Optional["aiohttp.ClientSession"] = None

    if len(args) == 1:
        if isinstance(args[0], aiohttp.ClientSession):  # type: ignore[attr-defined]
            sess = args[0]
        else:
            redis = args[0]
    elif len(args) >= 2:
        redis = args[0]
        if isinstance(args[1], aiohttp.ClientSession):  # type: ignore[attr-defined]
            sess = args[1]

    own_session = False
    if sess is None:
        sslctx = None
        with contextlib.suppress(Exception):
            if certifi is not None:
                sslctx = ssl.create_default_context(cafile=certifi.where())  # type: ignore[attr-defined]
        timeout = aiohttp.ClientTimeout(total=TIMEOUT_S + 3.0)  # type: ignore[attr-defined]
        conn = aiohttp.TCPConnector(ssl=sslctx, limit=60)  # type: ignore[attr-defined]
        sess = aiohttp.ClientSession(
            headers=_headers(),
            connector=conn,
            trust_env=TRUST_ENV,
            timeout=timeout,
        )  # type: ignore[attr-defined]
        own_session = True

    stats = _ScanStats(t0=time.time())

    try:
        # Risk snapshot
        risk_ctx = None
        with contextlib.suppress(Exception):
            risk_ctx = await risk_capacity_snapshot(redis)

        risk_capacity = _clamp(float((risk_ctx or {}).get("risk_capacity", 1.0) or 1.0), 0.0, 1.0)
        risk_shutdown = bool((risk_ctx or {}).get("risk_shutdown", False))
        risk_mode = str((risk_ctx or {}).get("risk_mode", "normal") or "normal")
        stats.risk_capacity, stats.risk_mode = risk_capacity, risk_mode

        if risk_shutdown or risk_capacity <= 0.0:
            log.every(
                "RAY_RISK_SHUTDOWN",
                n=10,
                risk_shutdown=int(risk_shutdown),
                risk_capacity=round(risk_capacity, 3),
                risk_mode=risk_mode,
            )
            return []

        # Regime snapshot
        regime = await _regime_snapshot()
        stats.regime_label = str(regime.get("label", "unknown") or "unknown")
        stats.regime_numeric = float(regime.get("label_numeric", 0.0))
        stats.manip_risk = float(regime.get("manipulation_risk", 0.0))

        tuned_soft_liq, tuned_fb_min_vol5 = _regime_tune_soft(
            SOFT_MIN_LIQ_USD,
            _f("RAYDIUM_FB_MIN_VOL5_USD", 300.0),
            regime,
        )
        min_combined = _dynamic_min_combined(SCORING_MIN_COMBINED, regime, risk_capacity)

        log.every(
            "RAY_SCAN_START",
            n=5,
            phase=MASTER_PHASE,
            mode=RAYDIUM_MODE,
            queries=",".join(SEARCH_QUERIES),
            tuned_soft_liq=int(tuned_soft_liq),
            soft_max_age=round(SOFT_MAX_AGE_MIN, 2),
            hard_min_liq=int(HARD_MIN_LIQ_USD),
            limit=LIMIT,
            scoring=int(bool(SCORING_ENABLED and score_candidate)),
            min_combined=round(min_combined, 2),
            min_label=SCORING_MIN_LABEL,
            regime=stats.regime_label,
            manip=round(stats.manip_risk, 3),
            risk_capacity=round(risk_capacity, 3),
            risk_mode=risk_mode,
        )

        # Fetch pairs
        pairs = await _fetch_pairs(sess)
        stats.raw_pairs = len(pairs)

        # Normalize + dedupe by mint (keep best pool per mint)
        best_by_mint: Dict[str, Dict[str, Any]] = {}
        for p in pairs:
            m, reason = _normalize_to_meta(p)
            if not m:
                if reason:
                    stats.rej(reason)
                continue

            # hard heuristics (cheap)
            if HARD_MIN_LIQ_USD > 0 and float(m.get("liq_usd") or 0.0) < float(HARD_MIN_LIQ_USD):
                stats.rej("reject_hard_min_liq", {"mint": m.get("mint"), "liq": m.get("liq_usd")})
                continue
            if MIN_BUYS_5M > 0 and int(m.get("buys_5m") or 0) < MIN_BUYS_5M:
                stats.rej("reject_min_buys5", {"mint": m.get("mint"), "buys5": m.get("buys_5m")})
                continue
            if MIN_VOL5_USD > 0 and float(m.get("vol_5m_usd") or 0.0) < MIN_VOL5_USD:
                stats.rej("reject_min_vol5", {"mint": m.get("mint"), "vol5": m.get("vol_5m_usd")})
                continue
            if float(m.get("price_change_5m_pct") or 0.0) <= float(MAX_NEG_CHG5):
                stats.rej("reject_dump_chg5", {"mint": m.get("mint"), "chg5": m.get("price_change_5m_pct")})
                continue

            # attach regime+risk
            m["regime_label"] = stats.regime_label
            m["regime_numeric"] = stats.regime_numeric
            m["regime_manipulation"] = stats.manip_risk
            m["regime_ctx"] = dict(regime)
            m["risk_capacity"] = risk_capacity
            m["risk_mode"] = risk_mode

            mint = str(m.get("mint") or "")
            if not mint:
                stats.rej("reject_no_mint_post")
                continue

            cur = best_by_mint.get(mint)
            if cur is None:
                best_by_mint[mint] = m
                continue

            cand_key = (
                float(m.get("liq_usd") or 0.0),
                float(m.get("vol_5m_usd") or 0.0),
                float(m.get("momo_hint") or 0.0),
            )
            cur_key = (
                float(cur.get("liq_usd") or 0.0),
                float(cur.get("vol_5m_usd") or 0.0),
                float(cur.get("momo_hint") or 0.0),
            )
            if cand_key > cur_key:
                best_by_mint[mint] = m

        metas = list(best_by_mint.values())
        stats.metas = len(metas)

        # Soft prefilter
        soft: List[Dict[str, Any]] = []
        for m in metas:
            try:
                liq = float(m.get("liq_usd") or 0.0)
                age = float(m.get("token_age_min") or 9999.0)
                if tuned_soft_liq > 0.0 and liq < tuned_soft_liq:
                    stats.rej("soft_liq", {"mint": m.get("mint"), "liq": liq, "need": tuned_soft_liq})
                    continue
                if SOFT_MAX_AGE_MIN > 0.0 and age > SOFT_MAX_AGE_MIN:
                    stats.rej("soft_age", {"mint": m.get("mint"), "age": age, "max": SOFT_MAX_AGE_MIN})
                    continue
                soft.append(m)
            except Exception:
                stats.rej("soft_exc")
                continue

        if not soft and metas:
            # no silent no-buy: track that soft filter eliminated everything
            stats.rej("soft_all_filtered")
            soft = metas

        if RAY_MAX_AGE_MIN > 0.0 and soft:
            before = len(soft)
            soft = [m for m in soft if float(m.get("token_age_min") or 9999.0) <= float(RAY_MAX_AGE_MIN)]
            if before > 0 and not soft:
                stats.rej("reject_hard_age_cap")

        stats.soft_metas = len(soft)

        if DEBUG_META_SAMPLE:
            if soft:
                sample = [
                    {
                        "sym": m.get("symbol"),
                        "liq": m.get("liq_usd"),
                        "vol5": m.get("vol_5m_usd"),
                        "buys5": m.get("buys_5m"),
                        "sells5": m.get("sells_5m"),
                        "chg5": m.get("price_change_5m_pct"),
                        "age_min": m.get("token_age_min"),
                        "momo_hint": round(float(m.get("momo_hint") or 0.0), 2),
                    }
                    for m in soft[:5]
                ]
                log.every("RAY_META_SAMPLE", n=10, count=len(soft), raw=stats.metas, first=sample)
            else:
                log.every("RAY_META_SAMPLE", n=10, count=0, raw=stats.metas)

        # Optional enrichment
        if soft:
            await _enrich_optional(soft)
        stats.enriched = len(soft)

        # Scoring
        scored = await _score_metas(soft, regime=regime, risk_ctx=risk_ctx)
        stats.scored = len(scored)

        # Gate to runners
        runners: List[Dict[str, Any]] = []
        if scored:
            for m in scored:
                try:
                    label = str(m.get("score_label") or "").upper() or "YELLOW"
                    combined = float(m.get("score_combined") or 0.0)
                    trade_ok = bool(m.get("score_is_trade_viable", True))

                    if not trade_ok:
                        stats.rej("gate_score_not_viable", {"mint": m.get("mint")})
                        continue
                    if combined < float(min_combined):
                        stats.rej("gate_score_low", {"mint": m.get("mint"), "score": combined, "need": min_combined})
                        continue
                    if _LABEL_LEVEL.get(label, 0) < _REQUIRED_LABEL_LEVEL:
                        stats.rej("gate_label_low", {"mint": m.get("mint"), "label": label, "need": SCORING_MIN_LABEL})
                        continue
                    if AI_RUG_ENABLED and float(m.get("ai_rug_score") or 0.0) >= float(RAYDIUM_MAX_AI_RUG):
                        stats.rej("gate_ai_rug", {"mint": m.get("mint"), "ai": m.get("ai_rug_score")})
                        continue

                    runners.append(m)
                except Exception:
                    stats.rej("gate_exc")
                    continue

        # Fallback
        if not runners:
            fb = _fallback_pick(soft, fb_min_vol5=tuned_fb_min_vol5)
            runners = fb
            stats.fb_runners = len(fb)
            if soft and not runners:
                stats.rej("fallback_empty")

        # Sort + cap + risk shrink
        if runners:

            def _sort_key(x: Dict[str, Any]) -> Tuple[float, float, float, float]:
                return (
                    float(x.get("score_combined", x.get("momo_hint", 0.0)) or 0.0),
                    float(x.get("vol_5m_usd") or 0.0),
                    float(x.get("liq_usd") or 0.0),
                    float(x.get("price_change_5m_pct") or 0.0),
                )

            runners.sort(key=_sort_key, reverse=True)
            runners = runners[:max(1, int(RAYDIUM_MAX_RUNNERS))]

            raw_n = len(runners)
            allowed = int(raw_n * risk_capacity)
            if risk_capacity > 0.0 and allowed < 1:
                allowed = 1
            if allowed < raw_n:
                runners = runners[:allowed]
                log.every("RAY_RISK_SHRINK", n=10, before=raw_n, after=allowed, risk_capacity=round(risk_capacity, 3))

        # Redis runner dedup
        if redis is not None and RUNNER_DEDUP_ENABLED and RUNNER_DEDUP_SEC > 0 and runners:
            filtered: List[Dict[str, Any]] = []
            for r in runners:
                mint = str(r.get("mint") or "")
                if not mint:
                    continue
                k = f"{REDIS_KEY_PREFIX}:runner:{mint}"
                seen = await _redis_get(redis, k)
                if seen:
                    stats.rej("reject_redis_dedup", {"mint": mint})
                    continue
                await _redis_setex(redis, k, int(RUNNER_DEDUP_SEC), "1")
                filtered.append(r)
            runners = filtered or runners

        stats.runners = len(runners)

        if DEBUG_SCORE_RESULT:
            log.every(
                "RAY_SCORE_RESULT",
                n=10,
                raw_pairs=stats.raw_pairs,
                metas=stats.metas,
                soft=stats.soft_metas,
                enriched=stats.enriched,
                scored=stats.scored,
                fb=stats.fb_runners,
                runners=stats.runners,
            )

        # If we produced 0 runners, do NOT be silent: emit reject summary
        if not runners:
            _maybe_log_rejects(stats, force=True)
        else:
            _maybe_log_rejects(stats, force=False)

        # AI training logs
        _log_ai_candidates(runners, regime=regime, risk_ctx=risk_ctx)

        # MOMO alerts
        if runners:
            await _send_momo(runners)

        # Scan summary
        if RAYDIUM_LOG_SCAN_SUMMARY:
            with contextlib.suppress(Exception):
                await log_event(
                    "raydium_scan_summary",
                    {
                        "ts": time.time(),
                        "phase": MASTER_PHASE,
                        "mode": RAYDIUM_MODE,
                        "raw_pairs": stats.raw_pairs,
                        "metas": stats.metas,
                        "soft_metas": stats.soft_metas,
                        "enriched": stats.enriched,
                        "scored_metas": stats.scored,
                        "fb_runners": stats.fb_runners,
                        "runners": stats.runners,
                        "rejects": dict(stats.rejects),
                        "regime": dict(regime),
                        "risk": dict(risk_ctx or {}),
                        "tuned": {"tuned_soft_liq": tuned_soft_liq, "min_combined_dynamic": min_combined},
                        "elapsed_ms": int((time.time() - stats.t0) * 1000),
                    },
                )

        # FINAL: return CandidateSnapshot list (canonical + compat aliases)
        return [_to_candidate_snapshot(r) for r in runners]

    except asyncio.CancelledError:
        raise
    except Exception as e:
        log.every("RAY_SCAN_FAIL", n=5, err=_short(e))
        if _fast_fail():
            raise
        return []
    finally:
        if own_session and sess:
            with contextlib.suppress(Exception):
                await sess.close()


# ──────────────────────────────────────────────────────────────────────────────
# Compatibility entrypoints for scripts/main.py scanner supervisor
# ──────────────────────────────────────────────────────────────────────────────
async def scan_once(*args: Any, **_kw: Any) -> List[Dict[str, Any]]:
    return await scan_raydium(*args)


async def run_scanner(*args: Any, **kwargs: Any) -> None:
    await run(*args, **kwargs)


async def scan_loop(*args: Any, **kwargs: Any) -> None:
    await run(*args, **kwargs)


async def run(
    *args: Any,
    q: Optional[Any] = None,
    emit: Optional[Any] = None,
    stop_evt: Optional[asyncio.Event] = None,
    redis: Any = None,
    session: Any = None,
) -> None:
    """
    Emits runners into q/emit until stop_evt is set.

    - If q is provided: await q.put(dict)
    - If emit is provided: await emit(dict) or emit(dict)
    """
    stop_evt = stop_evt or asyncio.Event()

    # best-effort parse positional args: (q, stop_evt) or (emit, stop_evt) or (redis, session, ...)
    if args:
        for a in args:
            if isinstance(a, asyncio.Event):
                stop_evt = a
            elif aiohttp is not None and isinstance(a, aiohttp.ClientSession):  # type: ignore[attr-defined]
                session = a
            elif callable(a) and emit is None:
                emit = a
            elif q is None and hasattr(a, "put"):
                q = a
            elif redis is None:
                redis = a

    while not stop_evt.is_set():
        try:
            runners = (
                await scan_raydium(redis, session)
                if (redis is not None or session is not None)
                else await scan_raydium()
            )
            for r in runners:
                if stop_evt.is_set():
                    break
                if q is not None:
                    with contextlib.suppress(Exception):
                        out = q.put(r)
                        if asyncio.iscoroutine(out):
                            await out
                    continue
                if emit is not None:
                    with contextlib.suppress(Exception):
                        out = emit(r)
                        if asyncio.iscoroutine(out):
                            await out

            wait = SCAN_INTERVAL_SEC + random.uniform(-SCAN_JITTER_SEC, SCAN_JITTER_SEC)
            await asyncio.sleep(max(0.05, wait))

        except asyncio.CancelledError:
            raise
        except Exception as e:
            log.every("RAY_RUN_LOOP_ERR", n=10, err=_short(e))
            if _fast_fail():
                raise
            await asyncio.sleep(max(0.25, SCAN_INTERVAL_SEC))


__all__ = ["scan_raydium", "scan_once", "run", "run_scanner", "scan_loop"]

if __name__ == "__main__":  # pragma: no cover
    async def _demo() -> None:
        res = await scan_raydium()
        print("runners:", len(res))
        if res:
            r = res[0]
            print(
                r.get("symbol"),
                r.get("mint"),
                r.get("score_combined"),
                r.get("score_label"),
                r.get("vol_5m_usd"),
                r.get("liq_usd"),
            )

    asyncio.run(_demo())
