# -*- coding: utf-8 -*-
from __future__ import annotations

"""
scanner/dexscreener_scanner.py

Dexscreener Scanner (Solana) — V3+++ LOCK-IN (quant-grade / God-tier)
====================================================================

Role
----
High-signal Solana candidate discovery via Dexscreener search. Normalizes into a single
CandidateSnapshot-ish dict schema used across scanners and the entry pipeline.

Hard requirements
-----------------
- 24/7 safe: never hard-fail on optional modules or transient HTTP errors.
- Zero network at import time.
- Orchestrator-friendly signatures: accepts (redis/session/stop_evt/q/emit) in any order.
- Emits BOTH modern keys and legacy aliases (liq/vol5/buys5/chg5/age_min, etc.).
- Ranking path:
    1) utils_pkg.scoring_service.score_candidate (if present & enabled)
    2) utils_pkg.pair_filters legacy viability + momentum/final ranking
    3) Heuristic fallback

Silent no-buy elimination
-------------------------
Every drop/reject increments a reason bucket; each scan logs a compact summary
showing where candidates were rejected.

Public API
----------
- async def scan_dexscreener(*args, **kwargs) -> List[Dict[str, Any]]
- async def scan_once(*args, **kwargs) -> List[Dict[str, Any]]
- async def run_scanner(*args, **kwargs) -> None
"""

import asyncio
import contextlib
import json
import logging
import os
import random
import re
import ssl
import time
from dataclasses import dataclass, field
from math import log1p
from typing import Any, Awaitable, Callable, Dict, Iterable, List, Optional, Tuple

import aiohttp
import certifi

# =============================================================================
# Logging (best-effort + shims)
# =============================================================================
try:
    from logging_setup import get_logger as _get_logger  # type: ignore
except Exception:  # pragma: no cover
    _get_logger = None  # type: ignore

if _get_logger:
    try:
        LOG = _get_logger("scanner.dexscreener")  # type: ignore[misc]
    except TypeError:
        LOG = _get_logger()  # type: ignore[misc]
else:
    LOG = logging.getLogger("scanner.dexscreener")
    if not LOG.handlers:
        logging.basicConfig(level=logging.INFO)


def _ensure_logger_shims() -> None:
    if not hasattr(LOG, "event"):

        def _event(name: str, **fields: Any) -> None:  # pragma: no cover
            with contextlib.suppress(Exception):
                LOG.info("%s %s", name, fields)

        setattr(LOG, "event", _event)

    if not hasattr(LOG, "every"):

        def _every(key: str, n: int = 60, msg: Optional[str] = None, **fields: Any) -> None:  # pragma: no cover
            try:
                now_m = time.monotonic()
                store = getattr(_every, "_store", {})  # type: ignore[attr-defined]
                last = float(store.get(key, 0.0))
                if now_m - last >= float(n):
                    store[key] = now_m
                    setattr(_every, "_store", store)  # type: ignore[attr-defined]
                    with contextlib.suppress(Exception):
                        getattr(LOG, "event")(msg or key, **fields)  # type: ignore[misc]
            except Exception:
                return None

        setattr(LOG, "every", _every)


_ensure_logger_shims()

# =============================================================================
# Safe config imports (fallback defaults)
# =============================================================================
try:
    from config import (  # type: ignore
        PF_SCAN_MIN_LIQ_USD,
        PF_SCAN_MIN_MCAP_USD,
        PF_SCAN_MAX_TOKEN_AGE_MIN,
    )
except Exception:  # pragma: no cover
    PF_SCAN_MIN_LIQ_USD = 15_000.0
    PF_SCAN_MIN_MCAP_USD = 50_000.0
    PF_SCAN_MAX_TOKEN_AGE_MIN = 120.0

try:
    from config import PF_SCAN_MIN_WHALE_SIZE_SOL  # type: ignore
except Exception:  # pragma: no cover
    PF_SCAN_MIN_WHALE_SIZE_SOL = 3.0

# =============================================================================
# Optional internal modules (soft imports)
# =============================================================================
# Alerts
try:
    from utils_pkg.alert_manager import AlertManager  # type: ignore
except Exception:  # pragma: no cover
    AlertManager = None  # type: ignore

# Legacy PF filters/scoring
try:
    from utils_pkg.pair_filters import (  # type: ignore
        is_viable_pair,
        compute_momentum_score,
        compute_final_score,
    )
except Exception:  # pragma: no cover

    def is_viable_pair(_meta: Dict[str, Any]) -> bool:  # type: ignore
        return True

    def compute_momentum_score(_meta: Dict[str, Any]) -> float:  # type: ignore
        return 0.0

    def compute_final_score(_meta: Dict[str, Any]) -> float:  # type: ignore
        return 0.0


# Preferred unified scoring
try:
    from utils_pkg.scoring_service import score_candidate, ScoredToken  # type: ignore
except Exception:  # pragma: no cover
    score_candidate = None  # type: ignore
    ScoredToken = None  # type: ignore

# Data logger (optional)
try:
    from utils_pkg import data_logger  # type: ignore
except Exception:  # pragma: no cover
    data_logger = None  # type: ignore

try:
    from utils_pkg.data_logger import log_event as _dl_log_event  # type: ignore
except Exception:  # pragma: no cover
    _dl_log_event = None  # type: ignore

# Regime (optional)
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

# Risk snapshot (optional)
try:
    from utils_pkg.risk_manager import risk_capacity_snapshot  # type: ignore
except Exception:  # pragma: no cover

    async def risk_capacity_snapshot(_redis=None) -> Dict[str, Any]:  # type: ignore
        return {"risk_capacity": 1.0, "risk_shutdown": False, "risk_mode": "normal"}


# Feature builder + AI entry (optional enrichment)
try:
    from utils_pkg import feature_builder as FB  # type: ignore
except Exception:  # pragma: no cover
    FB = None  # type: ignore

try:
    from utils_pkg import ai_entry_logic as AI_ENTRY  # type: ignore
except Exception:  # pragma: no cover
    AI_ENTRY = None  # type: ignore

# Smart wallets enrichment
try:
    from utils_pkg import smart_wallets as SW  # type: ignore
except Exception:  # pragma: no cover
    SW = None  # type: ignore

# Core utils optional enrichment funcs
try:
    from utils_pkg.core_utils import ai_fraud_score, is_whale_wallet  # type: ignore
except Exception:  # pragma: no cover
    ai_fraud_score = None  # type: ignore
    is_whale_wallet = None  # type: ignore

# Mint normalizer
try:
    from utils_pkg.mints import normalize_mint as _normalize_mint  # type: ignore
except Exception:  # pragma: no cover

    def _normalize_mint(x: str) -> str:  # type: ignore
        return (x or "").strip()


# =============================================================================
# Env helpers
# =============================================================================
_TRUE = {"1", "true", "t", "yes", "y", "on"}
_FALSE = {"0", "false", "f", "no", "n", "off"}


def _b(name: str, default: bool = False) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    s = str(v).strip().lower()
    if s in _TRUE:
        return True
    if s in _FALSE:
        return False
    return default


def _s(name: str, default: str = "") -> str:
    v = os.getenv(name)
    return default if v is None else str(v).strip()


def _i(name: str, default: int) -> int:
    v = os.getenv(name)
    try:
        return int(float(v)) if v not in (None, "") else default
    except Exception:
        return default


def _f(name: str, default: float) -> float:
    v = os.getenv(name)
    try:
        return float(v) if v not in (None, "") else default
    except Exception:
        return default


def _csv_set(name: str) -> set[str]:
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
        LOG.warning("Dexscreener: invalid regex in %s", name)
        return None


def _kill_or_disabled() -> bool:
    return _b("KILL_SWITCH", False) or _b("DISABLE_SCANNERS", False) or _b("DEXSCREENER_SCANNER_DISABLED", False)


# =============================================================================
# Tunables / flags
# =============================================================================
UA = _s(
    "DEXSCREENER_UA",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126 Safari/537.36",
)
BASE = _s("DEXSCREENER_BASE_URL", "https://api.dexscreener.com").rstrip("/")
REQ_TIMEOUT_S = max(5, _i("DEXSCREENER_HTTP_TIMEOUT_S", 12))
RETRIES = max(1, _i("DEXSCREENER_HTTP_RETRIES", 3))
BACKOFF_S = max(0.1, _f("DEXSCREENER_HTTP_BACKOFF_S", 0.6))
TRUST_ENV = _b("DEXSCREENER_TRUST_ENV", True)
SESSION_LIMIT = max(10, _i("DEXSCREENER_HTTP_CONN_LIMIT", 50))

RPS_LIMIT = max(0.0, _f("DEXSCREENER_RPS_LIMIT", 0.0))
SEARCH_QUERIES = [q.strip() for q in _s("DEX_SEARCH_QUERIES", "solana").split(",") if q.strip()]
QUERY_CONCURRENCY = max(1, _i("DEX_QUERY_CONCURRENCY", 3))

# Soft prefilters (start permissive; later ranking tightens)
_BASE_SOFT_LIQ = max(0.0, float(PF_SCAN_MIN_LIQ_USD) * 0.4)
_BASE_SOFT_MCAP = max(0.0, float(PF_SCAN_MIN_MCAP_USD) * 0.4)

SOFT_MIN_LIQ_USD = _f("DEX_SOFT_MIN_LIQ_USD", _BASE_SOFT_LIQ)
SOFT_MIN_MCAP_USD = _f("DEX_SOFT_MIN_MCAP_USD", _BASE_SOFT_MCAP)
SOFT_MAX_AGE_MIN = _f(
    "DEX_SOFT_MAX_AGE_MIN",
    float(PF_SCAN_MAX_TOKEN_AGE_MIN) * 1.2 if float(PF_SCAN_MAX_TOKEN_AGE_MIN) > 0 else 0.0,
)

SOFT_MIN_TXNS_5M = max(0, _i("DEX_SOFT_MIN_TXNS_5M", 0))
SOFT_MIN_H24_VOL_USD = max(0.0, _f("DEX_SOFT_MIN_H24_VOL_USD", 0.0))
SOFT_MIN_PRICE_USD = max(0.0, _f("DEX_SOFT_MIN_PRICE_USD", 0.0))
SOFT_MAX_PRICE_USD = max(0.0, _f("DEX_SOFT_MAX_PRICE_USD", 0.0))  # 0=no max

ALLOWED_DEX_IDS = _csv_set("DEX_ALLOWED_DEX_IDS")
BLOCKED_DEX_IDS = _csv_set("DEX_BLOCKED_DEX_IDS")
BLOCKED_BASE_MINTS = _csv_set("DEX_BLOCKED_BASE_MINTS")
BLOCKED_SYMBOL_RE = _regex("DEX_BLOCKED_SYMBOL_REGEX")

DEX_MAX_RUNNERS = max(1, _i("DEX_MAX_RUNNERS", 8))
DEX_LIMIT = max(5, _i("DEX_LIMIT", 40))

DEX_RISK_SHRINK_ENABLED = _b("DEX_RISK_SHRINK_ENABLED", True)

# Alerts
SEND_MOMO_ALERTS = _b("DEX_SEND_MOMO_ALERTS", True)
MAX_ALERTS_PER_SCAN = max(0, _i("DEX_MAX_ALERTS_PER_SCAN", 3))
ALERT_DEDUP_TTL_S = max(30, _i("DEX_ALERT_DEDUP_TTL_S", 180))

# Enrichment toggles
DEX_AI_RUG_ENABLED = _b("DEX_AI_RUG_ENABLED", False)
DEX_AI_RUG_MAX_PER_SCAN = max(1, _i("DEX_AI_RUG_MAX_PER_SCAN", 20))
DEX_MAX_AI_RUG = float(_f("DEX_MAX_AI_RUG", 0.88))

DEX_WHALES_ENABLED = _b("DEX_WHALES_ENABLED", False)

DEX_AI_ENTRY_ENABLED = _b("DEX_AI_ENTRY_ENABLED", True)
DEX_AI_ENTRY_FORCE = _b("DEX_AI_ENTRY_FORCE", False)
DEX_AI_ENTRY_MAX_PER_SCAN = max(1, _i("DEX_AI_ENTRY_MAX_PER_SCAN", 60))
DEX_REQUIRE_AI_ENTRY_FOR_RUNNERS = _b("DEX_REQUIRE_AI_ENTRY_FOR_RUNNERS", False)
DEX_MIN_AI_ENTRY = float(_f("DEX_MIN_AI_ENTRY", 0.0))

# Preferred scoring_service path
SCORING_ENABLED = _b("DEX_SCORING_ENABLED", True) and (score_candidate is not None)
SCORING_MAX_PER_SCAN = max(5, _i("DEX_SCORING_MAX_PER_SCAN", 80))
SCORING_CONCURRENCY = max(1, _i("DEX_SCORING_CONCURRENCY", 6))
SCORING_MIN_COMBINED = float(_f("DEX_SCORING_MIN_COMBINED", 55.0))
SCORING_MIN_LABEL = _s("DEX_SCORING_MIN_LABEL", "YELLOW").upper().strip()

LEGACY_PF_ENABLED = _b("DEX_LEGACY_PF_ENABLED", True)
DEX_BYPASS_PF = _b("DEX_BYPASS_PF", False)
DEX_ENABLE_FALLBACK = _b("DEX_ENABLE_FALLBACK", True)

# Heuristic fallback tuning
FB_MIN_BUYS_5M = max(0, _i("DEX_FB_MIN_BUYS_5M", 1))
FB_MIN_VOL5_USD = max(0.0, _f("DEX_FB_MIN_VOL5_USD", 300.0))
FB_MAX_RUNNERS = max(1, _i("DEX_FB_MAX_RUNNERS", 5))
FB_MAX_IMPACT_1K_BPS = max(0.0, _f("DEX_FB_MAX_IMPACT_1K_BPS", 1200.0))

# Scan loop interval
SCAN_INTERVAL_SEC = max(0.2, _f("DEX_SCAN_INTERVAL_SEC", 2.5))
SCAN_JITTER_SEC = max(0.0, _f("DEX_SCAN_JITTER_SEC", 0.5))

# Logging toggles
DEX_LOG_SCAN_SUMMARY = _b("DEX_LOG_SCAN_SUMMARY", True)
DEBUG_META_SAMPLE = _b("DEX_DEBUG_META_SAMPLE", True)
DEBUG_RESULT = _b("DEX_DEBUG_RESULT", True)
DEX_LOG_REJECT_BUCKETS = _b("DEX_LOG_REJECT_BUCKETS", True)
DEX_REJECT_LOG_EVERY_SEC = max(1, _i("DEX_REJECT_LOG_EVERY_SEC", 10))

# Training toggles
PG_ENABLED = _b("PG_ENABLED", False)
DEX_LOG_CANDIDATES = _b("DEX_LOG_CANDIDATES", False)
DEX_REGIME_TAG_ENV = _s("DEX_REGIME_TAG", "")
DEX_WALLET_TAG_ENV = _s("DEX_WALLET_TAG", "")

_LABEL_LEVEL = {"RED": 0, "YELLOW": 1, "GREEN": 2}
_REQUIRED_LABEL_LEVEL = _LABEL_LEVEL.get(SCORING_MIN_LABEL, 0)

ENRICH_CONCURRENCY = max(1, _i("DEX_ENRICH_CONCURRENCY", 6))

# =============================================================================
# Internal types / metrics
# =============================================================================
@dataclass(frozen=True)
class _ScanCounts:
    raw_pairs: int
    metas: int
    enriched: int
    scored: int
    runners: int
    source: str


@dataclass
class _CallCtx:
    redis: Any = None
    session: Optional[aiohttp.ClientSession] = None
    stop_evt: Optional[asyncio.Event] = None
    q: Any = None
    emit: Optional[Callable[[Any], Any]] = None


@dataclass
class _Buckets:
    # one-shot scan-level buckets
    rejects: Dict[str, int] = field(default_factory=dict)
    notes: Dict[str, int] = field(default_factory=dict)

    def bump(self, d: Dict[str, int], k: str, inc: int = 1) -> None:
        d[k] = int(d.get(k, 0)) + int(inc)

    def r(self, k: str, inc: int = 1) -> None:
        self.bump(self.rejects, k, inc)

    def n(self, k: str, inc: int = 1) -> None:
        self.bump(self.notes, k, inc)


# =============================================================================
# Regime snapshot & tuning
# =============================================================================
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
            return {
                "label_numeric": 0.0,
                "trend_score": 0.0,
                "volatility_score": 0.0,
                "liquidity_score": 0.5,
                "manipulation_risk": 0.0,
                "label": str(regime_state.get() or "unknown"),
            }
        return {
            "label_numeric": 0.0,
            "trend_score": 0.0,
            "volatility_score": 0.0,
            "liquidity_score": 0.5,
            "manipulation_risk": 0.0,
            "label": "unknown",
        }


def _regime_tune_thresholds(
    soft_liq: float,
    soft_mcap: float,
    fb_min_vol5: float,
    fb_max_impact_bps: float,
    regime: Dict[str, Any],
) -> Tuple[float, float, float, float]:
    ln = float(regime.get("label_numeric", 0.0))
    manip = float(regime.get("manipulation_risk", 0.0))

    s_liq = float(soft_liq)
    s_mcap = float(soft_mcap)
    fb_vol = float(fb_min_vol5)
    fb_imp = float(fb_max_impact_bps)

    if ln >= 0.7 and manip < 0.6:
        s_liq *= 0.70
        s_mcap *= 0.70
        fb_vol *= 0.70
        fb_imp *= 1.20 if fb_imp > 0 else fb_imp
    elif ln <= -0.7 or manip >= 0.8:
        s_liq *= 1.40
        s_mcap *= 1.40
        fb_vol *= 1.30
        fb_imp *= 0.85 if fb_imp > 0 else fb_imp

    return max(0.0, s_liq), max(0.0, s_mcap), max(0.0, fb_vol), max(0.0, fb_imp)


# =============================================================================
# HTTP helpers
# =============================================================================
_SSLCTX: Optional[ssl.SSLContext] = None


def _sslctx() -> ssl.SSLContext:
    global _SSLCTX
    if _SSLCTX is None:
        _SSLCTX = ssl.create_default_context(cafile=certifi.where())
    return _SSLCTX


def _hdrs() -> Dict[str, str]:
    return {
        "User-Agent": UA,
        "Accept": "application/json",
        "Accept-Language": "en-US,en;q=0.9",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
    }


class _RpsGate:
    def __init__(self, rps: float) -> None:
        self.rps = float(rps)
        self._last = 0.0
        self._lock = asyncio.Lock()

    async def wait(self) -> None:
        if self.rps <= 0:
            return
        async with self._lock:
            now = time.monotonic()
            min_dt = 1.0 / max(0.0001, self.rps)
            dt = now - self._last
            if dt < min_dt:
                await asyncio.sleep(min_dt - dt)
            self._last = time.monotonic()


_RPS = _RpsGate(RPS_LIMIT)


async def _fetch_json(
    session: aiohttp.ClientSession,
    url: str,
    *,
    params: Optional[Dict[str, Any]] = None,
    stop_evt: Optional[asyncio.Event] = None,
) -> Tuple[Optional[Dict[str, Any]], int, str]:
    status = 0
    first = ""
    for attempt in range(1, RETRIES + 1):
        if stop_evt is not None and stop_evt.is_set():
            return None, 0, "stopped"

        await _RPS.wait()

        try:
            async with session.get(
                url,
                params=params,
                headers=_hdrs(),
                ssl=_sslctx(),
                timeout=aiohttp.ClientTimeout(total=REQ_TIMEOUT_S),
            ) as r:
                status = int(r.status)
                text = await r.text()
                first = (text or "")[:160]

                if status == 200:
                    try:
                        return await r.json(content_type=None), status, first
                    except Exception:
                        with contextlib.suppress(Exception):
                            return json.loads(text), status, first
                        return None, status, first

                if status == 429:
                    await asyncio.sleep(BACKOFF_S * attempt + random.uniform(0.0, 0.25))
                    continue

                if 400 <= status < 500:
                    return None, status, first

        except (aiohttp.ClientError, asyncio.TimeoutError):
            pass
        except Exception:
            pass

        if attempt < RETRIES:
            await asyncio.sleep(BACKOFF_S * attempt + random.uniform(0.0, 0.12))

    return None, status, first


# =============================================================================
# Redis helpers (optional, safe)
# =============================================================================
def _looks_like_queue(x: Any) -> bool:
    return x is not None and (callable(getattr(x, "put", None)) or callable(getattr(x, "put_nowait", None)))


def _looks_like_redis(x: Any) -> bool:
    if x is None:
        return False
    if isinstance(x, asyncio.Event) or _looks_like_queue(x):
        return False
    return any(callable(getattr(x, nm, None)) for nm in ("get", "set", "hget", "hset", "ping"))


def _redis_ok(r: Any) -> bool:
    if r is None:
        return False
    return any(callable(getattr(r, nm, None)) for nm in ("get", "set", "setex", "expire"))


async def _redis_get(r: Any, key: str) -> Optional[str]:
    if not _redis_ok(r):
        return None
    try:
        out = r.get(key)
        if asyncio.iscoroutine(out):
            out = await out
        if out is None:
            return None
        if isinstance(out, (bytes, bytearray)):
            return out.decode("utf-8", "ignore")
        return str(out)
    except Exception:
        return None


async def _redis_setex(r: Any, key: str, ttl_s: int, value: str) -> None:
    if not _redis_ok(r):
        return
    try:
        if callable(getattr(r, "setex", None)):
            out = r.setex(key, int(ttl_s), value)
            if asyncio.iscoroutine(out):
                await out
            return
        out = r.set(key, value)
        if asyncio.iscoroutine(out):
            await out
        if callable(getattr(r, "expire", None)):
            out2 = r.expire(key, int(ttl_s))
            if asyncio.iscoroutine(out2):
                await out2
    except Exception:
        return


# =============================================================================
# Normalization helpers (CandidateSnapshot-ish + legacy aliases)
# =============================================================================
def _nz(x: Any, d: float = 0.0) -> float:
    try:
        fx = float(x)
        if fx != fx:
            return float(d)
        return fx
    except Exception:
        return float(d)


def _safe_int(x: Any, d: int = 0) -> int:
    try:
        return int(x)
    except Exception:
        return d


def _age_min(created_ms: Any) -> Optional[float]:
    try:
        if created_ms is None:
            return None
        t = float(created_ms)
        if t <= 0:
            return None
        if t > 10_000_000_000:  # ms
            t = t / 1000.0
        return max(0.0, (time.time() - t) / 60.0)
    except Exception:
        return None


def _dex_id(pair: Dict[str, Any]) -> str:
    return (pair.get("dexId") or "").strip().lower() or "dexscreener"


def _allowed_by_lists(pair: Dict[str, Any], base_mint: str, base_symbol: str, buckets: _Buckets) -> bool:
    dex_id = _dex_id(pair)
    if dex_id in BLOCKED_DEX_IDS:
        buckets.r("blocked_dex")
        return False
    if ALLOWED_DEX_IDS and dex_id not in ALLOWED_DEX_IDS:
        buckets.r("dex_not_allowed")
        return False
    if base_mint and base_mint.strip().lower() in BLOCKED_BASE_MINTS:
        buckets.r("blocked_mint")
        return False
    if BLOCKED_SYMBOL_RE is not None and base_symbol:
        if BLOCKED_SYMBOL_RE.search(base_symbol):
            buckets.r("blocked_symbol")
            return False
    return True


def _buy_ratio(buys: int, sells: int) -> float:
    total = max(1, buys + sells)
    return float(buys) / float(total)


def _extract_metrics(pair: Dict[str, Any]) -> Dict[str, Any]:
    base = pair.get("baseToken") or {}
    quote = pair.get("quoteToken") or {}

    liq_usd = _nz((pair.get("liquidity") or {}).get("usd"), 0.0)
    fdv_usd = _nz(pair.get("fdv") or pair.get("fdvUsd") or pair.get("fullyDilutedValuation"), 0.0)
    mcap_usd = _nz(pair.get("marketCap") or pair.get("marketCapUsd") or pair.get("mcap") or fdv_usd, 0.0)

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

    # derive rough m5 if missing
    if buys_5m == 0 and buys_1h > 0:
        buys_5m = max(1, buys_1h // 12)
    if buys_5m == 0 and buys_24h > 0:
        buys_5m = max(1, buys_24h // 288)
    if sells_5m == 0 and sells_1h > 0:
        sells_5m = max(0, sells_1h // 12)
    if sells_5m == 0 and sells_24h > 0:
        sells_5m = max(0, sells_24h // 288)

    vol = pair.get("volume") or {}
    vol_5m_usd = _nz(vol.get("m5"), 0.0)
    vol_1h_usd = _nz(vol.get("h1"), 0.0)
    vol_24h_usd = _nz(vol.get("h24"), 0.0)

    if vol_1h_usd <= 0 and vol_24h_usd > 0:
        vol_1h_usd = vol_24h_usd / 24.0
    if vol_5m_usd <= 0 and vol_1h_usd > 0:
        vol_5m_usd = vol_1h_usd / 12.0
    if vol_5m_usd <= 0 and vol_24h_usd > 0:
        vol_5m_usd = vol_24h_usd / 288.0

    chg = pair.get("priceChange") or {}
    chg_5m_pct = _nz(chg.get("m5"), 0.0)
    chg_1h_pct = _nz(chg.get("h1"), 0.0)
    chg_24h_pct = _nz(chg.get("h24"), 0.0)

    chg_15m_pct = 0.0
    if chg_1h_pct != 0.0:
        chg_15m_pct = chg_1h_pct / 4.0
    elif chg_24h_pct != 0.0:
        chg_15m_pct = chg_24h_pct / 96.0

    if chg_5m_pct == 0.0 and chg_1h_pct != 0.0:
        chg_5m_pct = chg_1h_pct / 12.0
    if chg_5m_pct == 0.0 and chg_24h_pct != 0.0:
        chg_5m_pct = chg_24h_pct / 288.0

    created = pair.get("pairCreatedAt") or pair.get("pairCreatedAtMs") or pair.get("pairCreatedAtTs")
    age_min = _age_min(created)
    token_age_min = age_min if age_min is not None else 9999.0

    price_usd = _nz(pair.get("priceUsd"), 0.0)
    holders = _safe_int(pair.get("holders") or 0)

    impact_1k_bps = _nz(
        pair.get("priceImpact1kBps") or pair.get("impact1kBps") or pair.get("impactBps"),
        0.0,
    )

    base_symbol = str(base.get("symbol") or "").strip()
    base_mint = str(base.get("address") or "").strip()
    quote_symbol = str(quote.get("symbol") or "").strip()

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

    h24_vol_usd = float(_nz((pair.get("volume") or {}).get("h24"), 0.0))

    return {
        "liq_usd": float(liq_usd),
        "mcap_usd": float(mcap_usd),
        "fdv_usd": float(fdv_usd),
        "token_age_min": float(token_age_min),
        "vol_5m_usd": float(vol_5m_usd),
        "vol_1h_usd": float(vol_1h_usd),
        "vol_24h_usd": float(vol_24h_usd),
        "buys_5m": int(buys_5m),
        "sells_5m": int(sells_5m),
        "buys_1h": int(buys_1h),
        "sells_1h": int(sells_1h),
        "buys_24h": int(buys_24h),
        "sells_24h": int(sells_24h),
        "price_usd": float(price_usd),
        "price_change_5m_pct": float(chg_5m_pct),
        "price_change_15m_pct": float(chg_15m_pct),
        "price_change_1h_pct": float(chg_1h_pct),
        "holders": int(holders),
        "whales_5m": int(whales_5m),
        "whale_vol_5m_usd": float(whale_vol_5m_usd),
        "largest_buy_5m_sol": float(largest_buy_5m_sol),
        "impact_1k_bps": float(impact_1k_bps),
        "base_symbol": base_symbol,
        "base_mint": base_mint,
        "quote_symbol": quote_symbol,
        "dex_id": _dex_id(pair),
        "h24_volume_usd": h24_vol_usd,
    }


def _normalize_to_snapshot(
    pair: Dict[str, Any],
    *,
    soft_liq: float,
    soft_mcap: float,
    soft_max_age: float,
    regime: Dict[str, Any],
    buckets: _Buckets,
) -> Optional[Dict[str, Any]]:
    try:
        if (pair.get("chainId") or "").lower() != "solana":
            buckets.r("non_solana")
            return None

        m = _extract_metrics(pair)
        mint = _normalize_mint(m.get("base_mint") or "")
        sym = (m.get("base_symbol") or "").strip()
        if not mint:
            buckets.r("missing_mint")
            return None

        if not _allowed_by_lists(pair, mint, sym, buckets):
            return None

        if soft_liq > 0 and float(m["liq_usd"]) < soft_liq:
            buckets.r("soft_liq")
            return None
        if soft_mcap > 0 and float(m["mcap_usd"]) < soft_mcap:
            buckets.r("soft_mcap")
            return None
        if soft_max_age > 0 and float(m["token_age_min"]) > soft_max_age:
            buckets.r("soft_age")
            return None

        txns5 = int(m["buys_5m"]) + int(m["sells_5m"])
        if SOFT_MIN_TXNS_5M > 0 and txns5 < SOFT_MIN_TXNS_5M:
            buckets.r("soft_txns5")
            return None

        px = float(m["price_usd"])
        if SOFT_MIN_PRICE_USD > 0 and px < SOFT_MIN_PRICE_USD:
            buckets.r("soft_price_low")
            return None
        if SOFT_MAX_PRICE_USD > 0 and px > SOFT_MAX_PRICE_USD:
            buckets.r("soft_price_high")
            return None

        if SOFT_MIN_H24_VOL_USD > 0 and float(m["h24_volume_usd"]) < SOFT_MIN_H24_VOL_USD:
            buckets.r("soft_h24vol")
            return None

        symbol = sym or mint[:6]
        dex_id = str(m.get("dex_id") or "dexscreener")
        pair_addr = str(pair.get("pairAddress") or "").strip() or None
        url = pair.get("url") or pair.get("pairUrl") or (
            f"https://dexscreener.com/solana/{pair_addr}" if pair_addr else f"https://dexscreener.com/solana/{mint}"
        )

        buys_15m = max(0, int(m["buys_1h"]) // 4) if int(m["buys_1h"]) > 0 else 0
        sells_15m = max(0, int(m["sells_1h"]) // 4) if int(m["sells_1h"]) > 0 else 0
        if buys_15m == 0 and int(m["buys_24h"]) > 0:
            buys_15m = max(1, int(m["buys_24h"]) // 96)
        if sells_15m == 0 and int(m["sells_24h"]) > 0:
            sells_15m = max(0, int(m["sells_24h"]) // 96)

        age_min = float(m["token_age_min"])
        chg5 = float(m["price_change_5m_pct"])
        vol5 = float(m["vol_5m_usd"])
        liq = float(m["liq_usd"])
        mcap = float(m["mcap_usd"])

        ts = float(time.time())

        # CandidateSnapshot-ish canonical keys + legacy aliases
        snap: Dict[str, Any] = {
            # identity (canonical)
            "source": "dexscreener",
            "scanner": "dexscreener_scanner",
            "chain": "solana",
            "dex": dex_id,
            "mint": mint,
            "symbol": symbol,
            "pair_url": url,
            "scan_ts": ts,

            # identity (compat)
            "dexId": dex_id,
            "pair_address": pair_addr,
            "pairAddress": pair_addr,
            "pair": pair_addr,
            "sym": symbol,
            "url": url,
            "ts": ts,

            # regime context
            "regime": regime.get("label", "unknown"),
            "regime_numeric": float(regime.get("label_numeric", 0.0)),
            "regime_manipulation": float(regime.get("manipulation_risk", 0.0)),
            "regime_ctx": dict(regime),

            # modern metrics
            "liq_usd": liq,
            "mcap_usd": mcap,
            "fdv_usd": float(m["fdv_usd"]),
            "token_age_min": age_min,
            "vol_5m_usd": vol5,
            "vol_1h_usd": float(m["vol_1h_usd"]),
            "vol_24h_usd": float(m["vol_24h_usd"]),
            "buys_5m": int(m["buys_5m"]),
            "sells_5m": int(m["sells_5m"]),
            "txns_5m": int(txns5),
            "buys_15m": int(buys_15m),
            "sells_15m": int(sells_15m),
            "price_usd": float(m["price_usd"]),
            "price_change_5m_pct": chg5,
            "price_change_15m_pct": float(m["price_change_15m_pct"]),
            "price_change_1h_pct": float(m["price_change_1h_pct"]),
            "holders": int(m["holders"]),
            "impact_1k_bps": float(m["impact_1k_bps"]),
            "dex_h24_volume_usd": float(m["h24_volume_usd"]),

            # legacy aliases
            "liq": liq,
            "mcap": mcap,
            "fdv": float(m["fdv_usd"]),
            "age_min": age_min,
            "vol5": vol5,
            "vol1h": float(m["vol_1h_usd"]),
            "vol24h": float(m["vol_24h_usd"]),
            "buys5": int(m["buys_5m"]),
            "sells5": int(m["sells_5m"]),
            "txns5": int(txns5),
            "buys15": int(buys_15m),
            "sells15": int(sells_15m),
            "chg5": chg5,
            "chg15": float(m["price_change_15m_pct"]),
            "chg1h": float(m["price_change_1h_pct"]),
            "price": float(m["price_usd"]),
            "impact1k_bps": float(m["impact_1k_bps"]),

            # enrichment placeholders (filled later)
            "ai_rug_score": 0.0,
            "ai_entry_score": None,
            "ai_entry_reason": None,
            "whales_5m": int(m["whales_5m"]),
            "whale_vol_5m_usd": float(m["whale_vol_5m_usd"]),
            "largest_buy_5m_sol": float(m["largest_buy_5m_sol"]),

            # scoring outputs
            "rank_score": None,
            "score_combined": None,
            "score_label": None,
            "score_momentum": None,
            "score_quality": None,
            "score_risk": None,
            "score_ai": None,
            "score_reasons": None,
            "score_is_trade_viable": True,
            "score_trade_gate_reason": None,

            # safety defaults
            "is_honeypot": False,
            "trading_paused": False,
            "blacklisted": False,
        }

        snap["buy_ratio_5m"] = _buy_ratio(int(snap["buys5"]), int(snap["sells5"]))
        snap["buy_ratio_15m"] = _buy_ratio(int(snap["buys15"]), int(snap["sells15"]))
        return snap

    except Exception:
        buckets.r("normalize_exc")
        return None


def _dedupe_by_mint(metas: List[Dict[str, Any]], buckets: Optional[_Buckets] = None) -> List[Dict[str, Any]]:
    seen: set[str] = set()
    out: List[Dict[str, Any]] = []
    for m in metas:
        mint = m.get("mint")
        if isinstance(mint, str) and mint:
            if mint in seen:
                if buckets:
                    buckets.r("dedupe")
                continue
            seen.add(mint)
            out.append(m)
        else:
            if buckets:
                buckets.r("missing_mint_post")
    return out


def _attach_risk(metas: List[Dict[str, Any]], risk_ctx: Optional[Dict[str, Any]]) -> None:
    if not metas or not risk_ctx:
        return
    cap = 1.0
    with contextlib.suppress(Exception):
        cap = float(risk_ctx.get("risk_capacity", 1.0) or 1.0)
    mode = risk_ctx.get("risk_mode", "normal")
    shutdown = bool(risk_ctx.get("risk_shutdown", False))
    for m in metas:
        m["risk_capacity"] = cap
        m["risk_mode"] = mode
        m["risk_shutdown"] = shutdown


# =============================================================================
# Call-context parsing (orchestrator-safe)
# =============================================================================
def _parse_ctx(*args: Any, **kwargs: Any) -> _CallCtx:
    ctx = _CallCtx()

    for k in ("stop_evt", "stop_event", "stop"):
        v = kwargs.get(k)
        if isinstance(v, asyncio.Event):
            ctx.stop_evt = v
            break

    for k in ("session", "sess", "http", "client"):
        v = kwargs.get(k)
        if isinstance(v, aiohttp.ClientSession):
            ctx.session = v
            break

    for k in ("redis", "r"):
        v = kwargs.get(k)
        if _looks_like_redis(v):
            ctx.redis = v
            break

    for k in ("q", "queue", "out_q"):
        v = kwargs.get(k)
        if _looks_like_queue(v):
            ctx.q = v
            break

    v = kwargs.get("emit")
    if callable(v):
        ctx.emit = v

    for a in args:
        if ctx.stop_evt is None and isinstance(a, asyncio.Event):
            ctx.stop_evt = a
            continue
        if ctx.session is None and isinstance(a, aiohttp.ClientSession):
            ctx.session = a
            continue
        if ctx.q is None and _looks_like_queue(a):
            ctx.q = a
            continue
        if ctx.redis is None and _looks_like_redis(a):
            ctx.redis = a
            continue
        if ctx.emit is None and callable(a) and not isinstance(a, (str, bytes, dict, list, tuple, set)):
            ctx.emit = a  # type: ignore[assignment]
            continue

    return ctx


# =============================================================================
# Pair collection
# =============================================================================
async def _fetch_pairs_for_query(
    session: aiohttp.ClientSession,
    q: str,
    *,
    stop_evt: Optional[asyncio.Event] = None,
    buckets: Optional[_Buckets] = None,
) -> List[Dict[str, Any]]:
    url = f"{BASE}/latest/dex/search"
    payload, status, first = await _fetch_json(session, url, params={"q": q}, stop_evt=stop_evt)
    if not payload:
        if buckets:
            buckets.r("search_empty")
        if status:
            getattr(LOG, "every")(
                "DEX_SEARCH_EMPTY",
                10,
                status=status,
                q=q,
                first=first,
            )  # type: ignore[misc]
        return []
    pairs = payload.get("pairs") or []
    out: List[Dict[str, Any]] = []
    for p in pairs:
        if isinstance(p, dict):
            out.append(p)
        else:
            if buckets:
                buckets.r("pair_nondict")
    return out


async def _fetch_pairs(
    session: aiohttp.ClientSession,
    *,
    stop_evt: Optional[asyncio.Event] = None,
    buckets: Optional[_Buckets] = None,
) -> List[Dict[str, Any]]:
    if not SEARCH_QUERIES:
        if buckets:
            buckets.r("no_queries")
        return []

    sem = asyncio.Semaphore(QUERY_CONCURRENCY)
    all_pairs: List[Dict[str, Any]] = []
    seen_ids: set[str] = set()

    async def _one(q: str) -> None:
        async with sem:
            res = await _fetch_pairs_for_query(session, q, stop_evt=stop_evt, buckets=buckets)
        for p in res:
            try:
                pid = p.get("pairAddress") or ((p.get("baseToken") or {}).get("address")) or ""
                pid = str(pid).strip().lower()
                if not pid:
                    if buckets:
                        buckets.r("pair_missing_id")
                    continue
                if pid in seen_ids:
                    if buckets:
                        buckets.r("pair_dupe")
                    continue
                seen_ids.add(pid)
                all_pairs.append(p)
            except Exception:
                if buckets:
                    buckets.r("pair_id_exc")

    tasks = [asyncio.create_task(_one(q)) for q in SEARCH_QUERIES]
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)

    return all_pairs


# =============================================================================
# Enrichment (AI rug / whales / smart_wallets / optional ai_entry)
# =============================================================================
async def _call_maybe_async(fn: Any, *a: Any, **k: Any) -> Any:
    out = fn(*a, **k)
    if asyncio.iscoroutine(out):
        return await out
    return out


async def _enrich_metas(
    metas: List[Dict[str, Any]],
    *,
    regime: Dict[str, Any],
    stop_evt: Optional[asyncio.Event] = None,
    buckets: Optional[_Buckets] = None,
) -> None:
    if not metas:
        return

    sem = asyncio.Semaphore(ENRICH_CONCURRENCY)

    to_rug = metas[:DEX_AI_RUG_MAX_PER_SCAN] if (DEX_AI_RUG_ENABLED and ai_fraud_score) else []
    do_ai_entry = (DEX_AI_ENTRY_ENABLED and FB and AI_ENTRY and (DEX_AI_ENTRY_FORCE or (not SCORING_ENABLED)))
    to_entry = metas[:DEX_AI_ENTRY_MAX_PER_SCAN] if do_ai_entry else []
    whale_targets = metas if (DEX_WHALES_ENABLED and is_whale_wallet) else []

    async def _handle(meta: Dict[str, Any]) -> None:
        if stop_evt is not None and stop_evt.is_set():
            return
        mint = meta.get("mint")
        if not isinstance(mint, str) or not mint:
            if buckets:
                buckets.r("enrich_missing_mint")
            return

        async with sem:
            if meta in to_rug and ai_fraud_score:
                with contextlib.suppress(Exception):
                    s01 = await _call_maybe_async(ai_fraud_score, mint)
                    if s01 is not None:
                        meta["ai_rug_score"] = max(0.0, min(1.0, float(s01)))

            if meta in whale_targets and is_whale_wallet:
                with contextlib.suppress(Exception):
                    px = float(meta.get("price_usd") or meta.get("price") or 0.0)
                    thresh_usd = float(PF_SCAN_MIN_WHALE_SIZE_SOL) * px if px > 0 else 50_000.0
                    has_whale = await _call_maybe_async(is_whale_wallet, mint, usd_threshold=thresh_usd)
                    if bool(has_whale):
                        meta["whales_5m"] = max(1, int(meta.get("whales_5m") or 0))

            if SW is not None:
                with contextlib.suppress(Exception):
                    fn = getattr(SW, "enrich_meta", None) or getattr(SW, "annotate_meta", None)
                    if callable(fn):
                        await _call_maybe_async(fn, meta)

            if meta in to_entry and FB is not None and AI_ENTRY is not None:
                with contextlib.suppress(Exception):
                    fb_fn = getattr(FB, "build_features_for_meta", None) or getattr(FB, "build_features", None)
                    if callable(fb_fn):
                        feats = fb_fn(meta, regime=regime)  # type: ignore[arg-type]
                        meta["_features"] = feats

                        score: Optional[float] = None
                        reason: Optional[str] = None

                        score_fn = getattr(AI_ENTRY, "score_candidate", None)
                        if callable(score_fn):
                            with contextlib.suppress(Exception):
                                score = float(await _call_maybe_async(score_fn, meta=meta, features=feats, regime=regime))
                            explain_fn = getattr(AI_ENTRY, "explain_candidate", None)
                            if callable(explain_fn):
                                with contextlib.suppress(Exception):
                                    reason = str(await _call_maybe_async(explain_fn, meta=meta, features=feats, regime=regime))
                        else:
                            score_entry_fn = getattr(AI_ENTRY, "score_entry", None)
                            if callable(score_entry_fn):
                                try:
                                    score = float(await _call_maybe_async(score_entry_fn, feats, meta=meta, regime=regime))
                                except TypeError:
                                    score = float(await _call_maybe_async(score_entry_fn, feats))

                        if score is not None:
                            meta["ai_entry_score"] = float(score)
                            meta["ai_score"] = float(score)
                        if reason:
                            meta["ai_entry_reason"] = str(reason)

    tasks = [asyncio.create_task(_handle(m)) for m in metas]
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)


# =============================================================================
# Scoring paths
# =============================================================================
async def _score_with_service(
    metas: List[Dict[str, Any]],
    *,
    regime: Dict[str, Any],
    risk_ctx: Optional[Dict[str, Any]],
    stop_evt: Optional[asyncio.Event] = None,
    buckets: Optional[_Buckets] = None,
) -> List[Dict[str, Any]]:
    if not SCORING_ENABLED or not metas or score_candidate is None:
        return []

    # choose top-ish subset for scoring (don’t spend scoring budget on junk)
    metas_sorted = sorted(
        metas,
        key=lambda x: (float(x.get("vol5") or 0.0), int(x.get("buys5") or 0), float(x.get("liq") or 0.0)),
        reverse=True,
    )
    targets = metas_sorted[:SCORING_MAX_PER_SCAN]

    sem = asyncio.Semaphore(SCORING_CONCURRENCY)
    out: List[Dict[str, Any]] = []

    async def _score_one(meta: Dict[str, Any]) -> None:
        if stop_evt is not None and stop_evt.is_set():
            return

        # early viability check here too (and bucket it)
        if DEX_AI_RUG_ENABLED and float(meta.get("ai_rug_score") or 0.0) >= float(DEX_MAX_AI_RUG):
            if buckets:
                buckets.r("rug_high_pre_score")
            return
        try:
            if not is_viable_pair(meta):
                if buckets:
                    buckets.r("pf_not_viable_pre_score")
                return
        except Exception:
            pass

        raw = dict(meta)
        raw.setdefault("age_min", raw.get("token_age_min"))
        raw.setdefault("chg5", raw.get("price_change_5m_pct"))
        raw.setdefault("buys5", raw.get("buys_5m"))
        raw.setdefault("sells5", raw.get("sells_5m"))

        ctx = {
            "source": "dexscreener",
            "scanner": "dexscreener_scanner",
            "regime": dict(regime),
            "risk": dict(risk_ctx or {}),
        }

        async with sem:
            try:
                st: ScoredToken = await score_candidate(raw, context=ctx)  # type: ignore[misc]
            except Exception:
                if buckets:
                    buckets.r("score_exc")
                return

        m = dict(meta)
        m["score_combined"] = float(getattr(st, "combined_score", 0.0))
        m["score_label"] = str(getattr(st, "label", "YELLOW"))
        m["score_momentum"] = float(getattr(st, "momentum_score", 0.0))
        m["score_risk"] = float(getattr(st, "risk_score", 0.0))
        m["score_quality"] = float(getattr(st, "quality_score", 0.0))
        m["score_ai"] = float(getattr(st, "ai_score", 0.0))
        m["score_reasons"] = getattr(st, "reasons", None)
        m["score_is_trade_viable"] = bool(getattr(st, "is_trade_viable", True))
        m["score_trade_gate_reason"] = getattr(st, "trade_gate_reason", None)

        # unify rank_score used by entry pipeline
        m["rank_score"] = float(m["score_combined"])
        m["ai_entry_score"] = float(m.get("score_ai") or 0.0)
        m["ai_entry_reason"] = m.get("score_reasons")

        out.append(m)

    tasks = [asyncio.create_task(_score_one(m)) for m in targets]
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)

    return out


def _select_runners_from_scored(scored: List[Dict[str, Any]], buckets: Optional[_Buckets] = None) -> List[Dict[str, Any]]:
    if not scored:
        return []

    runners: List[Dict[str, Any]] = []
    for m in scored:
        try:
            label = str(m.get("score_label") or "").upper() or "YELLOW"
            combined = float(m.get("score_combined") or 0.0)
            ai_score = float(m.get("score_ai") or 0.0)
            trade_ok = bool(m.get("score_is_trade_viable", True))

            if not trade_ok:
                if buckets:
                    buckets.r("score_trade_not_viable")
                continue
            if combined < SCORING_MIN_COMBINED:
                if buckets:
                    buckets.r("score_combined_low")
                continue
            if _LABEL_LEVEL.get(label, 0) < _REQUIRED_LABEL_LEVEL:
                if buckets:
                    buckets.r("score_label_low")
                continue

            if DEX_REQUIRE_AI_ENTRY_FOR_RUNNERS and DEX_MIN_AI_ENTRY > 0.0:
                if ai_score < float(DEX_MIN_AI_ENTRY):
                    if buckets:
                        buckets.r("ai_entry_low_scored")
                    continue

            if DEX_AI_RUG_ENABLED and float(m.get("ai_rug_score") or 0.0) >= float(DEX_MAX_AI_RUG):
                if buckets:
                    buckets.r("rug_high_scored")
                continue

            runners.append(m)
        except Exception:
            if buckets:
                buckets.r("select_scored_exc")
            continue

    runners.sort(key=lambda x: float(x.get("score_combined") or 0.0), reverse=True)
    return runners[:DEX_MAX_RUNNERS]


def _legacy_pf_rank(metas: List[Dict[str, Any]], buckets: Optional[_Buckets] = None) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for meta in metas:
        try:
            if DEX_AI_RUG_ENABLED and float(meta.get("ai_rug_score") or 0.0) >= float(DEX_MAX_AI_RUG):
                if buckets:
                    buckets.r("rug_high_pf")
                continue
            if not is_viable_pair(meta):
                if buckets:
                    buckets.r("pf_not_viable")
                continue

            momentum = float(compute_momentum_score(meta))
            final = float(compute_final_score(meta))

            ai_entry = meta.get("ai_entry_score")
            whales = int(meta.get("whales_5m") or 0)
            vol5 = float(meta.get("vol5") or 0.0)

            if DEX_REQUIRE_AI_ENTRY_FOR_RUNNERS:
                if not isinstance(ai_entry, (int, float)):
                    if buckets:
                        buckets.r("ai_entry_missing_pf")
                    continue
                if float(ai_entry) < float(DEX_MIN_AI_ENTRY):
                    if buckets:
                        buckets.r("ai_entry_low_pf")
                    continue

            rank = final + 0.2 * momentum
            if whales > 0:
                rank += 0.15

            if isinstance(ai_entry, (int, float)):
                vol_boost = max(1.0, log1p(max(vol5, 1.0)) / 5.0)
                rank = (0.6 * rank + 0.4 * float(ai_entry)) * vol_boost

            m = dict(meta)
            m["final_score"] = final
            m["momentum_score"] = momentum
            m["rank_score"] = float(rank)
            out.append(m)
        except Exception:
            if buckets:
                buckets.r("pf_rank_exc")
            continue

    out.sort(key=lambda x: float(x.get("rank_score") or 0.0), reverse=True)
    return out[:DEX_MAX_RUNNERS]


def _fallback_pick(
    metas: List[Dict[str, Any]],
    *,
    min_vol5_usd: float,
    max_impact_bps: float,
    buckets: Optional[_Buckets] = None,
) -> List[Dict[str, Any]]:
    if not DEX_ENABLE_FALLBACK or not metas:
        return []

    cands: List[Dict[str, Any]] = []
    for m in metas:
        try:
            buys5 = int(m.get("buys5") or 0)
            vol5 = float(m.get("vol5") or 0.0)
            impact = float(m.get("impact1k_bps") or m.get("impact_1k_bps") or 0.0)
            rug = float(m.get("ai_rug_score") or 0.0)

            if buys5 < FB_MIN_BUYS_5M:
                if buckets:
                    buckets.r("fb_buys5_low")
                continue
            if vol5 < float(min_vol5_usd):
                if buckets:
                    buckets.r("fb_vol5_low")
                continue
            if max_impact_bps > 0 and impact > float(max_impact_bps):
                if buckets:
                    buckets.r("fb_impact_high")
                continue
            if DEX_AI_RUG_ENABLED and rug >= float(DEX_MAX_AI_RUG):
                if buckets:
                    buckets.r("fb_rug_high")
                continue

            ai_entry = m.get("ai_entry_score")
            if DEX_REQUIRE_AI_ENTRY_FOR_RUNNERS:
                if not isinstance(ai_entry, (int, float)):
                    if buckets:
                        buckets.r("fb_ai_entry_missing")
                    continue
                if float(ai_entry) < float(DEX_MIN_AI_ENTRY):
                    if buckets:
                        buckets.r("fb_ai_entry_low")
                    continue

            cands.append(m)
        except Exception:
            if buckets:
                buckets.r("fb_exc")
            continue

    if not cands:
        return []

    def _key(mm: Dict[str, Any]) -> Tuple[int, float, float]:
        whales = int(mm.get("whales_5m") or 0)
        vol5 = float(mm.get("vol5") or 0.0)
        up5 = float(mm.get("chg5") or 0.0)
        return (whales, vol5, up5)

    cands.sort(key=_key, reverse=True)
    picked = cands[:FB_MAX_RUNNERS]
    for m in picked:
        m.setdefault("rank_score", float(m.get("vol5") or 0.0))
    return picked


# =============================================================================
# Alerts + training logging
# =============================================================================
def _effective_regime_tag(regime: Dict[str, Any]) -> str:
    if DEX_REGIME_TAG_ENV:
        return DEX_REGIME_TAG_ENV
    lbl = str(regime.get("label") or "").strip()
    return lbl or "dexscreener_live"


def _effective_wallet_tag() -> Optional[str]:
    return DEX_WALLET_TAG_ENV or None


def _log_candidates_for_training(runners: List[Dict[str, Any]], *, regime: Dict[str, Any]) -> None:
    if not (PG_ENABLED and DEX_LOG_CANDIDATES and data_logger and runners):
        return

    regime_tag = _effective_regime_tag(regime)
    wallet_tag = _effective_wallet_tag()

    log_dex = getattr(data_logger, "log_dexscreener_candidate", None)
    log_entry = getattr(data_logger, "log_entry_candidate", None)

    for meta in runners:
        try:
            mint = (meta.get("mint") or "").strip()
            if not mint:
                continue
            symbol = (meta.get("symbol") or meta.get("sym") or "").strip() or None
            snapshot = dict(meta)

            if callable(log_dex):
                log_dex(  # type: ignore[misc]
                    mint=mint,
                    symbol=symbol,
                    meta=snapshot,
                    regime=regime_tag,
                    wallet_tag=wallet_tag,
                )
            elif callable(log_entry):
                log_entry(  # type: ignore[misc]
                    src="dexscreener",
                    mint=mint,
                    symbol=symbol,
                    snapshot=snapshot,
                    regime=regime_tag,
                    wallet_tag=wallet_tag,
                )
        except Exception:
            LOG.exception("Dexscreener: training log failed mint=%s", meta.get("mint"))


async def _send_momo_for_runners(runners: List[Dict[str, Any]], *, redis: Any = None) -> None:
    if not SEND_MOMO_ALERTS or not runners or AlertManager is None:
        return

    try:
        am = AlertManager()
    except Exception:
        return

    max_n = max(0, min(MAX_ALERTS_PER_SCAN, len(runners)))
    for c in runners[:max_n]:
        try:
            mint = (c.get("mint") or "").strip()
            if not mint:
                continue

            if _redis_ok(redis):
                k = f"dex:momo_sent:{mint}"
                prev = await _redis_get(redis, k)
                if prev:
                    continue
                await _redis_setex(redis, k, ALERT_DEDUP_TTL_S, "1")

            age_min = c.get("age_min") if c.get("age_min") is not None else c.get("token_age_min")
            age_sec = float(age_min) * 60.0 if isinstance(age_min, (int, float)) else None
            ctx: Dict[str, Any] = {
                "symbol": c.get("sym") or c.get("symbol") or c.get("mint"),
                "mint": mint,
                "pool": c.get("dex") or c.get("dexId") or "dexscreener",
                "pool_url": c.get("pair_url") or c.get("url"),
                "age_sec": age_sec,
                "liq_usd": c.get("liq") or c.get("liq_usd"),
                "vol_5m": c.get("vol5") or c.get("vol_5m_usd"),
                "up_pct": c.get("chg5") or c.get("price_change_5m_pct"),
                "ai_rug_score": c.get("ai_rug_score"),
                "rank_score": c.get("rank_score"),
                "risk_capacity": c.get("risk_capacity"),
                "risk_mode": c.get("risk_mode"),
                "score_combined": c.get("score_combined"),
                "score_label": c.get("score_label"),
                "score_ai": c.get("score_ai"),
                "reason": (
                    f"dex runner liq=${float(c.get('liq') or c.get('liq_usd') or 0):.0f} "
                    f"vol5=${float(c.get('vol5') or c.get('vol_5m_usd') or 0):.0f} "
                    f"up5={float(c.get('chg5') or c.get('price_change_5m_pct') or 0):.2f}% "
                    f"rank={c.get('rank_score')!r}"
                ),
            }
            await am.send_momo_event("MOMO_OK", ctx)
        except Exception:
            continue


async def _log_scan_summary(counts: _ScanCounts, *, regime: Dict[str, Any], risk_ctx: Optional[Dict[str, Any]], buckets: _Buckets) -> None:
    if not DEX_LOG_SCAN_SUMMARY:
        return

    payload = {
        "source": counts.source,
        "raw_pairs": counts.raw_pairs,
        "metas": counts.metas,
        "enriched": counts.enriched,
        "scored": counts.scored,
        "runners": counts.runners,
        "regime": dict(regime),
        "risk": dict(risk_ctx or {}),
        "rejects": dict(buckets.rejects),
        "notes": dict(buckets.notes),
    }

    # Prefer data_logger event if available
    if callable(_dl_log_event):
        with contextlib.suppress(Exception):
            out = _dl_log_event("dexscreener_scan_summary", payload)  # type: ignore[misc]
            if asyncio.iscoroutine(out):
                await out
            return

    getattr(LOG, "event")("DEX_SCAN_SUMMARY", **payload)  # type: ignore[misc]


def _maybe_log_reject_buckets(buckets: _Buckets) -> None:
    if not DEX_LOG_REJECT_BUCKETS:
        return
    getattr(LOG, "every")(
        "DEX_REJECT_BUCKETS",
        int(DEX_REJECT_LOG_EVERY_SEC),
        rejects=dict(sorted(buckets.rejects.items(), key=lambda kv: kv[1], reverse=True)[:12]),
    )  # type: ignore[misc]


# =============================================================================
# Emit helpers (orchestrator integration)
# =============================================================================
async def _emit_one(ctx: _CallCtx, item: Any) -> None:
    if ctx.emit is not None:
        with contextlib.suppress(Exception):
            out = ctx.emit(item)
            if asyncio.iscoroutine(out):
                await out
        return
    if ctx.q is not None:
        with contextlib.suppress(Exception):
            if callable(getattr(ctx.q, "put_nowait", None)):
                ctx.q.put_nowait(item)
                return
            if callable(getattr(ctx.q, "put", None)):
                out = ctx.q.put(item)
                if asyncio.iscoroutine(out):
                    await out


# =============================================================================
# Public scan implementation
# =============================================================================
async def scan_once(*args: Any, **kwargs: Any) -> List[Dict[str, Any]]:
    """
    One-shot scan. Returns a list of runner CandidateSnapshot-ish dicts.
    Accepts orchestrator params: redis/session/stop_evt/q/emit in any order.

    If q/emit passed, we best-effort emit each runner (and still return the list).
    """
    if _kill_or_disabled():
        return []

    ctx = _parse_ctx(*args, **kwargs)
    stop_evt = ctx.stop_evt
    buckets = _Buckets()

    # Risk snapshot (best-effort)
    try:
        risk_ctx = await risk_capacity_snapshot(ctx.redis)
    except Exception:
        risk_ctx = None
        buckets.n("risk_snapshot_exc")

    risk_capacity = float((risk_ctx or {}).get("risk_capacity", 1.0) or 1.0)
    risk_shutdown = bool((risk_ctx or {}).get("risk_shutdown", False))
    if risk_shutdown or risk_capacity <= 0.0:
        buckets.r("risk_shutdown")
        _maybe_log_reject_buckets(buckets)
        return []

    regime = await _regime_snapshot()
    tuned_soft_liq, tuned_soft_mcap, tuned_fb_min_vol5, tuned_fb_max_impact = _regime_tune_thresholds(
        SOFT_MIN_LIQ_USD,
        SOFT_MIN_MCAP_USD,
        FB_MIN_VOL5_USD,
        FB_MAX_IMPACT_1K_BPS,
        regime,
    )

    if DEBUG_META_SAMPLE:
        LOG.info(
            "scanner.dexscreener: start; scoring=%s legacy_pf=%s bypass_pf=%s "
            "soft_liq=$%.0f soft_mcap=$%.0f max_age=%.0f "
            "regime=%.2f manip=%.2f risk_capacity=%.3f risk_mode=%s queries=%s",
            SCORING_ENABLED,
            LEGACY_PF_ENABLED,
            DEX_BYPASS_PF,
            tuned_soft_liq,
            tuned_soft_mcap,
            SOFT_MAX_AGE_MIN,
            float(regime.get("label_numeric", 0.0)),
            float(regime.get("manipulation_risk", 0.0)),
            risk_capacity,
            (risk_ctx or {}).get("risk_mode", "normal"),
            ",".join(SEARCH_QUERIES),
        )

    async def _apply_risk_shrink(runners: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        if not runners or not DEX_RISK_SHRINK_ENABLED:
            return runners
        raw_n = len(runners)
        allowed = int(raw_n * risk_capacity)
        if risk_capacity > 0.0 and allowed < 1:
            allowed = 1
        if allowed < raw_n:
            buckets.n("risk_shrink")
            return runners[:allowed]
        return runners

    sess = ctx.session
    own_session = False
    if sess is None:
        conn = aiohttp.TCPConnector(ssl=_sslctx(), limit=SESSION_LIMIT)
        sess = aiohttp.ClientSession(
            headers={"User-Agent": UA},
            connector=conn,
            trust_env=TRUST_ENV,
            timeout=aiohttp.ClientTimeout(total=REQ_TIMEOUT_S + 5),
        )
        own_session = True

    counts = _ScanCounts(0, 0, 0, 0, 0, "dex_search")

    try:
        pairs = await _fetch_pairs(sess, stop_evt=stop_evt, buckets=buckets)

        metas: List[Dict[str, Any]] = []
        for p in pairs:
            if stop_evt is not None and stop_evt.is_set():
                buckets.n("stopped")
                break
            if not isinstance(p, dict):
                buckets.r("pair_nondict_post")
                continue
            snap = _normalize_to_snapshot(
                p,
                soft_liq=tuned_soft_liq,
                soft_mcap=tuned_soft_mcap,
                soft_max_age=SOFT_MAX_AGE_MIN,
                regime=regime,
                buckets=buckets,
            )
            if snap:
                metas.append(snap)

        metas = _dedupe_by_mint(metas, buckets=buckets)
        _attach_risk(metas, risk_ctx)

        if DEBUG_META_SAMPLE:
            sample = [
                {
                    "sym": m.get("sym"),
                    "dex": m.get("dex"),
                    "liq": m.get("liq"),
                    "vol5": m.get("vol5"),
                    "buys5": m.get("buys5"),
                    "chg5": m.get("chg5"),
                    "age_min": m.get("age_min"),
                    "impact": m.get("impact1k_bps"),
                }
                for m in metas[:5]
            ]
            LOG.info("DEX_META_SAMPLE pairs=%d metas=%d first=%s", len(pairs), len(metas), sample)

        if not metas:
            buckets.r("no_metas")
            _maybe_log_reject_buckets(buckets)
            return []

        await _enrich_metas(metas, regime=regime, stop_evt=stop_evt, buckets=buckets)

        runners: List[Dict[str, Any]] = []
        scored: List[Dict[str, Any]] = []

        if SCORING_ENABLED and (stop_evt is None or not stop_evt.is_set()):
            scored = await _score_with_service(metas, regime=regime, risk_ctx=risk_ctx, stop_evt=stop_evt, buckets=buckets)
            runners = _select_runners_from_scored(scored, buckets=buckets)
            if not runners:
                buckets.r("no_runners_scored")

        if not runners and LEGACY_PF_ENABLED and (not DEX_BYPASS_PF) and (stop_evt is None or not stop_evt.is_set()):
            runners = _legacy_pf_rank(metas, buckets=buckets)
            if not runners:
                buckets.r("no_runners_pf")

        if not runners and (stop_evt is None or not stop_evt.is_set()):
            runners = _fallback_pick(
                metas,
                min_vol5_usd=tuned_fb_min_vol5,
                max_impact_bps=tuned_fb_max_impact,
                buckets=buckets,
            )
            if not runners:
                buckets.r("no_runners_fallback")

        runners = _dedupe_by_mint(runners, buckets=buckets)
        runners.sort(key=lambda x: float(x.get("rank_score") or x.get("score_combined") or 0.0), reverse=True)

        runners = runners[:DEX_MAX_RUNNERS]
        runners = await _apply_risk_shrink(runners)

        counts = _ScanCounts(
            raw_pairs=len(pairs),
            metas=len(metas),
            enriched=len(metas),
            scored=len(scored),
            runners=len(runners),
            source="dex_search",
        )

        if DEBUG_RESULT:
            LOG.info(
                "DEX_RESULT pairs=%d metas=%d scored=%d runners=%d (scoring=%s legacy_pf=%s bypass_pf=%s fb_min_vol=%.0f fb_max_imp=%.0f)",
                len(pairs),
                len(metas),
                len(scored),
                len(runners),
                SCORING_ENABLED,
                LEGACY_PF_ENABLED,
                DEX_BYPASS_PF,
                tuned_fb_min_vol5,
                tuned_fb_max_impact,
            )

        _maybe_log_reject_buckets(buckets)

        with contextlib.suppress(Exception):
            await _send_momo_for_runners(runners, redis=ctx.redis)
        with contextlib.suppress(Exception):
            _log_candidates_for_training(runners, regime=regime)
        with contextlib.suppress(Exception):
            await _log_scan_summary(counts, regime=regime, risk_ctx=risk_ctx, buckets=buckets)

        if runners and (ctx.emit is not None or ctx.q is not None):
            for r in runners:
                if stop_evt is not None and stop_evt.is_set():
                    break
                await _emit_one(ctx, r)

        return runners

    except Exception as e:
        buckets.r("scan_once_exc")
        getattr(LOG, "every")("DEX_SCAN_CRASH", 10, err=f"{type(e).__name__}: {e}")  # type: ignore[misc]
        _maybe_log_reject_buckets(buckets)
        return []
    finally:
        if own_session and sess is not None:
            with contextlib.suppress(Exception):
                await sess.close()


async def scan_dexscreener(*args: Any, **kwargs: Any) -> List[Dict[str, Any]]:
    """Back-compat name expected by wrapper + scripts/main.py resolver."""
    return await scan_once(*args, **kwargs)


async def run_scanner(*args: Any, **kwargs: Any) -> None:
    """
    Long-running loop mode.
    Reuses a session across iterations if one isn't provided.
    """
    ctx = _parse_ctx(*args, **kwargs)
    stop_evt = ctx.stop_evt or asyncio.Event()

    sess = ctx.session
    own_session = False
    if sess is None:
        conn = aiohttp.TCPConnector(ssl=_sslctx(), limit=SESSION_LIMIT)
        sess = aiohttp.ClientSession(
            headers={"User-Agent": UA},
            connector=conn,
            trust_env=TRUST_ENV,
            timeout=aiohttp.ClientTimeout(total=REQ_TIMEOUT_S + 5),
        )
        own_session = True

    try:
        while not stop_evt.is_set():
            if _kill_or_disabled():
                await asyncio.sleep(0.75)
                continue
            with contextlib.suppress(Exception):
                await scan_once(redis=ctx.redis, session=sess, stop_evt=stop_evt, q=ctx.q, emit=ctx.emit)
            await asyncio.sleep(max(0.05, SCAN_INTERVAL_SEC + random.uniform(-SCAN_JITTER_SEC, SCAN_JITTER_SEC)))
    finally:
        if own_session and sess is not None:
            with contextlib.suppress(Exception):
                await sess.close()


# Friendly aliases
async def run(*args: Any, **kwargs: Any) -> None:
    await run_scanner(*args, **kwargs)


async def main(*args: Any, **kwargs: Any) -> None:
    await run_scanner(*args, **kwargs)


__all__ = ["scan_dexscreener", "scan_once", "run_scanner", "run", "main"]
