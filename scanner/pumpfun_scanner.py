# -*- coding: utf-8 -*-
from __future__ import annotations

"""
scanner/pumpfun_scanner.py — Pump.fun scanner (V3+++ / God-tier LOCK-IN)
========================================================================

CRITICAL FIX (DNS / mirror outage):
----------------------------------
This scanner now supports a real-time PumpPortal websocket provider:

    PUMP_PROVIDER=pumpportal
    PUMP_WS_URL=wss://pumpportal.fun/api/data

When PUMP_PROVIDER=pumpportal, we DO NOT depend on pump-mirror HTTP endpoints.
We keep optional HTTP/HTML fallbacks for "auto" mode, but pump-mirror is not
a default base anymore (it was proven dead via DNS in your tests).

Role
----
Produce unified CandidateSnapshot-like dicts compatible with your pipeline:

  • utils_pkg.pair_filters
  • utils_pkg.scoring_service
  • utils_pkg.data_logger
  • utils_pkg.alert_manager

Hard guarantees
---------------
✅ Never hard-fail if optional modules are missing.
✅ PumpPortal WS: single connection, resilient reconnect, bounded memory.
✅ No “silent no-buy”: reject buckets + summary logs.
✅ Scoring-first selection; legacy + heuristic fallback.
✅ Optional redis cooldown dedupe for runners.
✅ ToS-safe: we do not attempt bypasses if blocked; we just degrade gracefully.

Public API
----------
async def scan_pumpfun(*args) -> List[Dict[str, Any]]
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
from collections import deque
from dataclasses import dataclass, field
from math import log1p
from typing import Any, Deque, Dict, Iterable, List, Optional, Set, Tuple

import aiohttp
import certifi

# ──────────────────────────────────────────────────────────────────────────────
# Logger (safe + shims)
# ──────────────────────────────────────────────────────────────────────────────
try:
    from logging_setup import get_logger  # type: ignore
    log = get_logger("scanner.pumpfun")
except Exception:  # pragma: no cover
    log = logging.getLogger("scanner.pumpfun")
    if not log.handlers:
        logging.basicConfig(level=logging.INFO)

if not hasattr(log, "event"):
    def _event(name: str, **kw: Any) -> None:  # pragma: no cover
        with contextlib.suppress(Exception):
            log.info("%s %s", name, kw)
    setattr(log, "event", _event)

if not hasattr(log, "every"):
    def _every(key: str, n: int = 60, msg: Optional[str] = None, **kw: Any) -> None:  # pragma: no cover
        now = time.time()
        store = getattr(_every, "_store", {})  # type: ignore[attr-defined]
        last = float(store.get(key, 0.0))
        if (now - last) >= float(n):
            store[key] = now
            setattr(_every, "_store", store)  # type: ignore[attr-defined]
            with contextlib.suppress(Exception):
                getattr(log, "event")(msg or key, **kw)
    setattr(log, "every", _every)

def _short(x: Any, n: int = 260) -> str:
    try:
        s = str(x)
        return s if len(s) <= n else (s[:n] + "…")
    except Exception:
        return "<?>"


# ──────────────────────────────────────────────────────────────────────────────
# Safe config import
# ──────────────────────────────────────────────────────────────────────────────
try:
    import config as C  # type: ignore
except Exception:  # pragma: no cover
    class _C:
        pass
    C = _C()  # type: ignore


# ──────────────────────────────────────────────────────────────────────────────
# Optional modules (soft imports)
# ──────────────────────────────────────────────────────────────────────────────
# Alerts
try:
    from utils_pkg.alert_manager import AlertManager  # type: ignore
except Exception:  # pragma: no cover
    AlertManager = None  # type: ignore

# Legacy PF filters & scoring
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
    from utils_pkg.scoring_service import score_candidate  # type: ignore
except Exception:  # pragma: no cover
    score_candidate = None  # type: ignore

# Data logger
try:
    from utils_pkg import data_logger  # type: ignore
except Exception:  # pragma: no cover
    data_logger = None  # type: ignore

try:
    from utils_pkg.data_logger import log_event as _log_event  # type: ignore
except Exception:  # pragma: no cover
    _log_event = None  # type: ignore

# Regime
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

# Risk snapshot
try:
    from utils_pkg.risk_manager import risk_capacity_snapshot  # type: ignore
except Exception:  # pragma: no cover
    async def risk_capacity_snapshot(_redis=None) -> Dict[str, Any]:  # type: ignore
        return {"risk_capacity": 1.0, "risk_shutdown": False, "risk_mode": "normal"}

# Smart wallets enrichment (optional)
try:
    from utils_pkg import smart_wallets as SW  # type: ignore
except Exception:  # pragma: no cover
    SW = None  # type: ignore

# Optional AI rug score (optional)
try:
    from utils_pkg.core_utils import ai_fraud_score  # type: ignore
except Exception:  # pragma: no cover
    ai_fraud_score = None  # type: ignore

# Optional SOL price helper (best-effort)
try:
    from utils_pkg import core_utils as _cu  # type: ignore
except Exception:  # pragma: no cover
    _cu = None  # type: ignore

# Mint normalize (preferred if you have it)
try:
    from utils_pkg.mints import normalize_mint as _normalize_mint  # type: ignore
except Exception:  # pragma: no cover
    _normalize_mint = None  # type: ignore


# ──────────────────────────────────────────────────────────────────────────────
# Small helpers
# ──────────────────────────────────────────────────────────────────────────────
def _b(name: str, default: bool = False) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return str(v).strip().lower() in {"1", "true", "t", "yes", "y", "on"}

def _s(name: str, default: str = "") -> str:
    v = os.getenv(name)
    return default if v is None else str(v).strip()

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

def _clamp(x: float, lo: float, hi: float) -> float:
    return lo if x < lo else hi if x > hi else x

def _nz(x: Any, d: float = 0.0) -> float:
    try:
        f = float(x)
        if f != f:
            return d
        return f
    except Exception:
        return d

def _safe_int(x: Any, d: int = 0) -> int:
    try:
        return int(x)
    except Exception:
        return d

def _now() -> float:
    return time.time()

async def _maybe_await(x: Any) -> Any:
    return await x if asyncio.iscoroutine(x) else x

async def _call_maybe_async(fn: Any, *a: Any, **k: Any) -> Any:
    if fn is None:
        return None
    try:
        if asyncio.iscoroutinefunction(fn):
            return await fn(*a, **k)
        return await asyncio.to_thread(fn, *a, **k)
    except Exception:
        return None

def _sslctx() -> ssl.SSLContext:
    return ssl.create_default_context(cafile=certifi.where())

BASE58_RE = re.compile(r"[1-9A-HJ-NP-Za-km-z]{32,44}")


# ──────────────────────────────────────────────────────────────────────────────
# Version / schema
# ──────────────────────────────────────────────────────────────────────────────
SCANNER_NAME = "pumpfun_scanner"
SCANNER_VERSION = "v3+++_lockin_ws"
CANDIDATE_SCHEMA = "CandidateSnapshot.v1"


# ──────────────────────────────────────────────────────────────────────────────
# Config-derived defaults
# ──────────────────────────────────────────────────────────────────────────────
PF_SCAN_MIN_LIQ_USD = float(getattr(C, "PF_SCAN_MIN_LIQ_USD", 2500.0) or 2500.0)
PF_SCAN_MIN_MCAP_USD = float(getattr(C, "PF_SCAN_MIN_MCAP_USD", 0.0) or 0.0)
PF_SCAN_MAX_TOKEN_AGE_MIN = float(getattr(C, "PF_SCAN_MAX_TOKEN_AGE_MIN", 0.0) or 0.0)
PF_SCAN_MIN_WHALE_SIZE_SOL = float(getattr(C, "PF_SCAN_MIN_WHALE_SIZE_SOL", 3.0) or 3.0)
ENABLE_PUMPFUN_SCANNER = bool(getattr(C, "ENABLE_PUMPFUN_SCANNER", True))

MASTER_PHASE = max(0, min(7, _i("MASTER_PHASE", _i("STRATEGY_PHASE", 7))))
if MASTER_PHASE == 0:
    _default_mode = "off"
elif MASTER_PHASE <= 2:
    _default_mode = "simple"
elif MASTER_PHASE <= 4:
    _default_mode = "alpha"
else:
    _default_mode = "full"

PUMPFUN_MODE = _s("PUMPFUN_MODE", _default_mode).lower().strip()
if PUMPFUN_MODE not in {"off", "simple", "alpha", "full"}:
    PUMPFUN_MODE = "full"
PUMPFUN_ENABLED = ENABLE_PUMPFUN_SCANNER and _b("PUMPFUN_ENABLED", (PUMPFUN_MODE != "off"))

# Provider selection
PUMP_PROVIDER = _s("PUMP_PROVIDER", _s("PUMPFUN_PROVIDER", "auto")).lower().strip()
if PUMP_PROVIDER not in {"auto", "pumpportal", "http"}:
    PUMP_PROVIDER = "auto"

PUMP_WS_URL = _s("PUMP_WS_URL", "wss://pumpportal.fun/api/data").strip()
PUMPPORTAL_SUB_MIGRATIONS = _b("PUMPPORTAL_SUB_MIGRATIONS", True)
PUMPPORTAL_TRACK_WINDOW_SEC = max(30, _i("PUMPPORTAL_TRACK_WINDOW_SEC", 360))          # 6 minutes default
PUMPPORTAL_SNAP_MAX = max(25, _i("PUMPPORTAL_SNAP_MAX", 250))
PUMPPORTAL_MAX_TOKENS = max(100, _i("PUMPPORTAL_MAX_TOKENS", 1500))
PUMPPORTAL_MAX_TRADES_PER_TOKEN = max(25, _i("PUMPPORTAL_MAX_TRADES_PER_TOKEN", 400))
PUMPPORTAL_SUBSCRIBE_TRADE_MAX = max(10, _i("PUMPPORTAL_SUBSCRIBE_TRADE_MAX", 120))
PUMPPORTAL_SUBSCRIBE_REFRESH_SEC = max(3, _f("PUMPPORTAL_SUBSCRIBE_REFRESH_SEC", 8.0))
PUMPPORTAL_CONNECT_TIMEOUT_S = max(2.0, _f("PUMPPORTAL_CONNECT_TIMEOUT_S", 8.0))
PUMPPORTAL_HEARTBEAT_S = max(10.0, _f("PUMPPORTAL_HEARTBEAT_S", 25.0))

UA = _s(
    "PUMPFUN_UA",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126 Safari/537.36",
)

# HTTP fallback (only used when provider=auto/http)
PUMPFUN_HTTP_PROXY = _s("PUMPFUN_HTTP_PROXY", "")
TIMEOUT_S = max(5.0, _f("PUMPFUN_HTTP_TIMEOUT_SEC", 15.0))
RETRIES = max(1, _i("PUMPFUN_HTTP_RETRIES", 3))
BACKOFF_S = max(0.1, _f("PUMPFUN_HTTP_BACKOFF_SEC", 0.6))
TRUST_ENV = _b("PUMPFUN_TRUST_ENV", True)
CONN_LIMIT = max(10, _i("PUMPFUN_HTTP_CONN_LIMIT", 30))

API_LIMIT = max(5, _i("PUMPFUN_API_LIMIT", 25))
MAX_RUNNERS = max(1, _i("PUMPFUN_MAX_RUNNERS", API_LIMIT))

RISK_SHRINK_ENABLED = _b("PUMPFUN_RISK_SHRINK_ENABLED", True)

RUNNER_DEDUP_ENABLED = _b("PUMPFUN_RUNNER_DEDUP_ENABLED", True)
RUNNER_DEDUP_SEC = max(0, _i("PUMPFUN_RUNNER_DEDUP_SEC", 180))
REDIS_KEY_PREFIX = _s("PUMPFUN_REDIS_PREFIX", "pumpfun")

SEND_MOMO_ALERTS = _b("PUMPFUN_SEND_MOMO_ALERTS", True)
MAX_ALERTS_PER_SCAN = max(0, _i("PUMPFUN_MAX_ALERTS_PER_SCAN", 3))
ALERT_DEDUP_SEC = max(10.0, _f("PUMPFUN_ALERT_DEDUP_SEC", 180.0))

AI_RUG_ENABLED = _b("PUMPFUN_AI_RUG_ENABLED", False) and (ai_fraud_score is not None)
AI_RUG_MAX_PER_SCAN = max(0, _i("PUMPFUN_AI_RUG_MAX_PER_SCAN", 20))
MAX_AI_RUG = _f("PUMPFUN_MAX_AI_RUG", 0.88)

SMART_WALLETS_ENRICH = _b("PUMPFUN_SMART_WALLETS_ENRICH", True)
SMART_WALLETS_MAX_PER_SCAN = max(0, _i("PUMPFUN_SMART_WALLETS_MAX_PER_SCAN", 80))

SCORING_ENABLED = _b("PUMPFUN_SCORING_ENABLED", True) and (score_candidate is not None)
SCORING_MAX_PER_SCAN = max(5, _i("PUMPFUN_SCORING_MAX_PER_SCAN", 80))
SCORING_CONCURRENCY = max(1, _i("PUMPFUN_SCORING_CONCURRENCY", 6))
SCORING_MIN_COMBINED = _f("PUMPFUN_SCORING_MIN_COMBINED", 55.0)
SCORING_MIN_LABEL = _s("PUMPFUN_SCORING_MIN_LABEL", "YELLOW").upper().strip()

LEGACY_PF_ENABLED = _b("PUMPFUN_LEGACY_PF_ENABLED", True)

SOFT_MIN_LIQ_USD = _f("PUMPFUN_SOFT_MIN_LIQ_USD", max(0.0, PF_SCAN_MIN_LIQ_USD * 0.4))
SOFT_MIN_MCAP_USD = _f("PUMPFUN_SOFT_MIN_MCAP_USD", max(0.0, PF_SCAN_MIN_MCAP_USD * 0.4))
SOFT_MAX_AGE_MIN = _f(
    "PUMPFUN_SOFT_MAX_AGE_MIN",
    PF_SCAN_MAX_TOKEN_AGE_MIN * 1.2 if PF_SCAN_MAX_TOKEN_AGE_MIN > 0 else 0.0,
)
HARD_MAX_AGE_MIN = _f("PUMPFUN_HARD_MAX_AGE_MIN", 0.0)

FB_MIN_BUYS_5M = max(0, _i("PUMPFUN_FB_MIN_BUYS_5M", 1))
FB_MIN_VOL5_USD = max(0.0, _f("PUMPFUN_FB_MIN_VOL5_USD", 200.0))
FB_MAX_RUNNERS = max(1, _i("PUMPFUN_FB_MAX_RUNNERS", 8))
FB_MAX_IMPACT_1K_BPS = max(0.0, _f("PUMPFUN_FB_MAX_IMPACT_1K_BPS", 1500.0))

DYNAMIC_THRESH_ENABLED = _b("PUMPFUN_DYNAMIC_THRESH_ENABLED", True)
DYNAMIC_THRESH_MAX_DELTA = _f("PUMPFUN_DYNAMIC_THRESH_MAX_DELTA", 8.0)

DEBUG_META_SAMPLE = _b("PUMPFUN_DEBUG_META_SAMPLE", True)
DEBUG_RESULT = _b("PUMPFUN_DEBUG_RESULT", True)
LOG_SCAN_SUMMARY = _b("PUMPFUN_LOG_SCAN_SUMMARY", True)

PG_ENABLED = _b("PG_ENABLED", False)
LOG_CANDIDATES = _b("PUMPFUN_LOG_CANDIDATES", False)
REGIME_TAG_ENV = _s("PUMPFUN_REGIME_TAG", "")
WALLET_TAG_ENV = _s("PUMPFUN_WALLET_TAG", "")

_LABEL_LEVEL = {"RED": 0, "YELLOW": 1, "GREEN": 2}
_REQUIRED_LABEL_LEVEL = _LABEL_LEVEL.get(SCORING_MIN_LABEL, 0)

_recent_alert_ts: Dict[str, float] = {}
_recent_alert_lock = asyncio.Lock()

# HTTP block codes (for fallback only)
_BLOCK_CODES = {401, 403, 407, 429, 451, 503, 520, 521, 522, 523, 524, 530}


# ──────────────────────────────────────────────────────────────────────────────
# Reject buckets (kill silent no-buy)
# ──────────────────────────────────────────────────────────────────────────────
@dataclass
class _RejectBuckets:
    counts: Dict[str, int] = field(default_factory=dict)
    samples: Dict[str, List[str]] = field(default_factory=dict)

    def inc(self, reason: str, mint: Optional[str] = None) -> None:
        if not reason:
            reason = "UNKNOWN"
        self.counts[reason] = int(self.counts.get(reason, 0)) + 1
        if mint:
            arr = self.samples.get(reason)
            if arr is None:
                arr = []
                self.samples[reason] = arr
            if len(arr) < 4 and mint not in arr:
                arr.append(mint)


@dataclass
class _ScanStats:
    t0: float
    source: str = "none"
    raw: int = 0
    metas: int = 0
    enriched: int = 0
    scored: int = 0
    runners: int = 0
    fb_runners: int = 0
    risk_capacity: float = 1.0
    risk_mode: str = "normal"
    regime_label: str = "unknown"
    regime_numeric: float = 0.0
    manip_risk: float = 0.0
    rejects: _RejectBuckets = field(default_factory=_RejectBuckets)


# ──────────────────────────────────────────────────────────────────────────────
# Redis helpers (optional sync/async clients)
# ──────────────────────────────────────────────────────────────────────────────
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
# Regime snapshot + tuning
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
            return {"label": regime_state.get(), "label_numeric": 0.0, "manipulation_risk": 0.0}
        return {"label": "unknown", "label_numeric": 0.0, "manipulation_risk": 0.0}

def _regime_tune(
    soft_liq: float,
    soft_mcap: float,
    fb_min_vol5: float,
    fb_max_impact: float,
    regime: Dict[str, Any],
) -> Tuple[float, float, float, float]:
    ln = float(regime.get("label_numeric", 0.0))
    manip = float(regime.get("manipulation_risk", 0.0))

    s_liq = float(soft_liq)
    s_mcap = float(soft_mcap)
    fb_vol = float(fb_min_vol5)
    fb_imp = float(fb_max_impact)

    if ln >= 0.7 and manip < 0.6:  # risk-on
        s_liq *= 0.70
        s_mcap *= 0.70
        fb_vol *= 0.70
        fb_imp *= 1.20 if fb_imp > 0 else fb_imp
    elif ln <= -0.7 or manip >= 0.8:  # risk-off / manip
        s_liq *= 1.40
        s_mcap *= 1.40
        fb_vol *= 1.30
        fb_imp *= 0.85 if fb_imp > 0 else fb_imp

    return max(0.0, s_liq), max(0.0, s_mcap), max(0.0, fb_vol), max(0.0, fb_imp)

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
# SOL price helper (best effort; never fatal)
# ──────────────────────────────────────────────────────────────────────────────
async def _sol_usd() -> float:
    # Prefer env override (stable, fastest)
    v = _f("SOL_USD", 0.0)
    if v > 0:
        return float(v)

    # Best-effort: try core_utils helpers if present (names vary)
    if _cu is not None:
        for name in ("get_sol_price_usd", "sol_price_usd", "get_sol_price", "get_sol_usd"):
            fn = getattr(_cu, name, None)
            if callable(fn):
                out = await _call_maybe_async(fn)
                try:
                    p = float(out)
                    if p > 0:
                        return p
                except Exception:
                    pass
    return 0.0


# ──────────────────────────────────────────────────────────────────────────────
# PumpPortal WS client (single connection; bounded state)
# ──────────────────────────────────────────────────────────────────────────────
@dataclass
class _Trade:
    ts: float
    side: str  # "buy"|"sell"|"?"
    sol: float
    token: float
    price_sol: float

@dataclass
class _TokenState:
    mint: str
    symbol: str = ""
    name: str = ""
    created_ts: float = 0.0
    last_ts: float = 0.0
    # optional fundamentals from stream (if present)
    mcap_usd: float = 0.0
    liq_usd: float = 0.0
    price_usd: float = 0.0
    # trades (bounded)
    trades: Deque[_Trade] = field(default_factory=deque)

    def push_trade(self, t: _Trade, *, max_trades: int) -> None:
        self.trades.append(t)
        while len(self.trades) > max_trades:
            self.trades.popleft()
        self.last_ts = max(self.last_ts, t.ts)

class _PumpPortalClient:
    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._task: Optional[asyncio.Task] = None
        self._sess: Optional[aiohttp.ClientSession] = None
        self._ws: Optional[aiohttp.ClientWebSocketResponse] = None

        self._ready = asyncio.Event()
        self._stop = asyncio.Event()

        self._tokens: Dict[str, _TokenState] = {}
        self._order: Deque[str] = deque()  # mint order by first seen
        self._last_subscribe_ts = 0.0
        self._subscribed_trades: Set[str] = set()

    async def ensure_started(self) -> None:
        async with self._lock:
            if self._task and not self._task.done():
                return
            self._stop.clear()
            self._ready.clear()
            self._task = asyncio.create_task(self._run_loop(), name="pumpportal_ws")
        # wait a bit for initial connect (non-fatal if it times out)
        with contextlib.suppress(Exception):
            await asyncio.wait_for(self._ready.wait(), timeout=PUMPPORTAL_CONNECT_TIMEOUT_S)

    async def close(self) -> None:
        self._stop.set()
        async with self._lock:
            t = self._task
            self._task = None
        if t:
            with contextlib.suppress(Exception):
                t.cancel()
        if self._ws is not None:
            with contextlib.suppress(Exception):
                await self._ws.close()
        if self._sess is not None:
            with contextlib.suppress(Exception):
                await self._sess.close()
        self._ws = None
        self._sess = None

    def is_ready(self) -> bool:
        return self._ready.is_set()

    def stats(self) -> Dict[str, Any]:
        return {
            "ready": self.is_ready(),
            "tokens": len(self._tokens),
            "sub_trades": len(self._subscribed_trades),
            "ws": bool(self._ws and not self._ws.closed),
        }

    def snapshot(self, *, max_items: int) -> List[_TokenState]:
        # Copy states in newest-first order (last seen)
        mints = list(self._order)[-max_items:]
        out: List[_TokenState] = []
        for mint in reversed(mints):
            st = self._tokens.get(mint)
            if st is not None:
                out.append(st)
        return out

    async def _ws_send(self, payload: Dict[str, Any]) -> None:
        ws = self._ws
        if ws is None or ws.closed:
            return
        with contextlib.suppress(Exception):
            await ws.send_json(payload)

    async def _subscribe_basics(self) -> None:
        # Must use a SINGLE connection; these are lightweight
        await self._ws_send({"method": "subscribeNewToken"})
        if PUMPPORTAL_SUB_MIGRATIONS:
            await self._ws_send({"method": "subscribeMigration"})

    async def _refresh_trade_subscriptions(self) -> None:
        # Throttle refresh
        now = time.time()
        if (now - self._last_subscribe_ts) < float(PUMPPORTAL_SUBSCRIBE_REFRESH_SEC):
            return
        self._last_subscribe_ts = now

        # Subscribe to trades for the newest tokens (bounded)
        newest = list(self._order)[-PUMPPORTAL_SUBSCRIBE_TRADE_MAX:]
        keys = [m for m in newest if m and m not in self._subscribed_trades]
        if not keys:
            return

        # PumpPortal expects list of mints under keys
        await self._ws_send({"method": "subscribeTokenTrade", "keys": keys})
        for m in keys:
            self._subscribed_trades.add(m)

    def _touch_token(self, mint: str, *, symbol: str = "", name: str = "", created_ts: float = 0.0) -> _TokenState:
        st = self._tokens.get(mint)
        if st is None:
            st = _TokenState(mint=mint)
            self._tokens[mint] = st
            self._order.append(mint)
            # bounded memory
            while len(self._order) > PUMPPORTAL_MAX_TOKENS:
                old = self._order.popleft()
                self._tokens.pop(old, None)
                self._subscribed_trades.discard(old)
        else:
            # move to the end (newest)
            with contextlib.suppress(ValueError):
                self._order.remove(mint)
            self._order.append(mint)

        if symbol and not st.symbol:
            st.symbol = symbol
        if name and not st.name:
            st.name = name
        if created_ts and st.created_ts <= 0:
            st.created_ts = created_ts
        st.last_ts = max(st.last_ts, time.time())
        return st

    def _extract_mint(self, msg: Dict[str, Any]) -> str:
        for k in ("mint", "mintAddress", "tokenMint", "token", "tokenAddress", "publicKey"):
            v = msg.get(k)
            if isinstance(v, str):
                m = BASE58_RE.search(v.strip())
                if m:
                    return self._norm_mint(m.group(0))
        # fallback: scan a couple likely nested spots
        for k in ("data", "token", "coin", "meta"):
            v = msg.get(k)
            if isinstance(v, dict):
                for kk in ("mint", "mintAddress", "tokenMint", "tokenAddress", "publicKey"):
                    vv = v.get(kk)
                    if isinstance(vv, str):
                        m = BASE58_RE.search(vv.strip())
                        if m:
                            return self._norm_mint(m.group(0))
        return ""

    def _norm_mint(self, s: str) -> str:
        v = (s or "").strip().replace(" ", "")
        if _normalize_mint is not None:
            with contextlib.suppress(Exception):
                nn = _normalize_mint(v)
                if isinstance(nn, str) and nn:
                    return nn
        m = BASE58_RE.search(v)
        return m.group(0) if m else ""

    def _is_trade_msg(self, msg: Dict[str, Any]) -> bool:
        tx = msg.get("txType") or msg.get("side") or msg.get("type")
        if isinstance(tx, str) and tx.lower() in {"buy", "sell"}:
            return True
        # heuristics: trade-like numeric fields
        for k in ("solAmount", "sol_amount", "tokenAmount", "token_amount"):
            if k in msg:
                return True
        return False

    def _parse_trade(self, msg: Dict[str, Any]) -> Tuple[str, _Trade]:
        side = (msg.get("txType") or msg.get("side") or msg.get("type") or "?")
        side = str(side).lower()
        if side not in {"buy", "sell"}:
            side = "?"

        ts = _nz(msg.get("timestamp") or msg.get("ts") or msg.get("time") or time.time(), time.time())
        if ts > 10_000_000_000:
            ts = ts / 1000.0

        sol = _nz(msg.get("solAmount") or msg.get("sol_amount") or msg.get("sol"), 0.0)
        token = _nz(msg.get("tokenAmount") or msg.get("token_amount") or msg.get("token"), 0.0)
        price_sol = 0.0
        if token > 0 and sol > 0:
            price_sol = sol / token

        return side, _Trade(ts=float(ts), side=side, sol=float(sol), token=float(token), price_sol=float(price_sol))

    def _apply_fundamentals(self, st: _TokenState, msg: Dict[str, Any], sol_usd: float) -> None:
        # best-effort: if stream gives cap/liquidity/price, capture
        mcap_usd = (
            msg.get("marketCapUsd")
            or msg.get("marketCapUSD")
            or msg.get("mcapUsd")
            or msg.get("mcap_usd")
        )
        if mcap_usd is None:
            mcap_sol = msg.get("marketCapSol") or msg.get("marketCapSOL") or msg.get("mcapSol")
            if mcap_sol is not None and sol_usd > 0:
                mcap_usd = _nz(mcap_sol, 0.0) * sol_usd
        if mcap_usd is not None:
            st.mcap_usd = max(0.0, _nz(mcap_usd, 0.0))

        liq_usd = msg.get("liquidityUsd") or msg.get("liquidityUSD") or msg.get("liqUsd") or msg.get("liq_usd")
        if liq_usd is None:
            liq_sol = msg.get("vSolInBondingCurve") or msg.get("solInBondingCurve") or msg.get("liquiditySol")
            if liq_sol is not None and sol_usd > 0:
                liq_usd = _nz(liq_sol, 0.0) * sol_usd
        if liq_usd is not None:
            st.liq_usd = max(0.0, _nz(liq_usd, 0.0))

        price_usd = msg.get("priceUsd") or msg.get("priceUSD") or msg.get("usdPrice")
        if price_usd is None:
            price_sol = msg.get("priceSol") or msg.get("priceSOL")
            if price_sol is not None and sol_usd > 0:
                price_usd = _nz(price_sol, 0.0) * sol_usd
        if price_usd is not None:
            st.price_usd = max(0.0, _nz(price_usd, 0.0))

    async def _run_loop(self) -> None:
        backoff = 0.75
        sol_usd_cache = 0.0
        sol_usd_ts = 0.0

        while not self._stop.is_set():
            try:
                if self._sess is None or self._sess.closed:
                    self._sess = aiohttp.ClientSession(headers={"User-Agent": UA}, trust_env=True)

                sslctx = _sslctx()
                async with self._sess.ws_connect(
                    PUMP_WS_URL,
                    heartbeat=PUMPPORTAL_HEARTBEAT_S,
                    ssl=sslctx,
                    max_msg_size=8 * 1024 * 1024,
                ) as ws:
                    self._ws = ws
                    backoff = 0.75

                    await self._subscribe_basics()
                    self._ready.set()
                    log.every("PUMPPORTAL_WS_CONNECTED", n=10, url=PUMP_WS_URL)

                    while not self._stop.is_set():
                        # periodic SOL price refresh
                        now = time.time()
                        if (now - sol_usd_ts) > 20:
                            sol_usd_cache = await _sol_usd()
                            sol_usd_ts = now

                        await self._refresh_trade_subscriptions()

                        msg = await ws.receive(timeout=30)
                        if msg.type in (aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.CLOSED):
                            raise aiohttp.ClientConnectionError("ws_closed")
                        if msg.type == aiohttp.WSMsgType.ERROR:
                            raise aiohttp.ClientConnectionError("ws_error")
                        if msg.type != aiohttp.WSMsgType.TEXT:
                            continue

                        try:
                            data = json.loads(msg.data)
                        except Exception:
                            continue
                        if not isinstance(data, dict):
                            continue

                        mint = self._extract_mint(data)
                        if not mint:
                            continue

                        symbol = str(data.get("symbol") or data.get("ticker") or "").strip()
                        name = str(data.get("name") or "").strip()

                        # created timestamp best-effort
                        created = data.get("createdAt") or data.get("created") or data.get("createdOn") or data.get("createdTimestamp")
                        created_ts = 0.0
                        if created is not None:
                            created_ts = _nz(created, 0.0)
                            if created_ts > 10_000_000_000:
                                created_ts = created_ts / 1000.0

                        st = self._touch_token(mint, symbol=symbol, name=name, created_ts=float(created_ts))

                        # apply fundamentals if present
                        self._apply_fundamentals(st, data, sol_usd_cache)

                        # trade updates
                        if self._is_trade_msg(data):
                            side, tr = self._parse_trade(data)
                            st.push_trade(tr, max_trades=PUMPPORTAL_MAX_TRADES_PER_TOKEN)

            except asyncio.TimeoutError:
                # keep loop alive; WS heartbeat should prevent most
                continue
            except Exception as e:
                self._ready.clear()
                log.every("PUMPPORTAL_WS_RECONNECT", n=5, err=_short(type(e).__name__), detail=_short(e))
                # backoff with jitter
                await asyncio.sleep(backoff * (0.85 + 0.3 * random.random()))
                backoff = min(25.0, backoff * 1.6)

        # shutdown
        self._ready.clear()
        self._ws = None


_PUMPPORTAL: Optional[_PumpPortalClient] = None

async def _pumpportal_client() -> _PumpPortalClient:
    global _PUMPPORTAL
    if _PUMPPORTAL is None:
        _PUMPPORTAL = _PumpPortalClient()
    await _PUMPPORTAL.ensure_started()
    return _PUMPPORTAL


# ──────────────────────────────────────────────────────────────────────────────
# Candidate normalization (shared)
# ──────────────────────────────────────────────────────────────────────────────
def _age_min(created_ts: Optional[float]) -> Optional[float]:
    try:
        if created_ts is None or created_ts <= 0:
            return None
        t = float(created_ts)
        if t > 10_000_000_000:  # ms -> s
            t = t / 1000.0
        return max(0.0, (_now() - t) / 60.0)
    except Exception:
        return None

def _buy_ratio(buys: int, sells: int) -> float:
    total = max(1, buys + sells)
    return float(buys) / float(total)

def _trade_window_metrics(trades: Iterable[_Trade], *, now: float, wsec: float, sol_usd: float) -> Dict[str, Any]:
    # Trades assumed to be roughly time-sorted; we scan and keep those in window
    buys = sells = 0
    vol_sol = 0.0
    prices: List[float] = []
    cutoff = now - wsec

    for t in trades:
        if t.ts < cutoff:
            continue
        if t.side == "buy":
            buys += 1
        elif t.side == "sell":
            sells += 1
        vol_sol += max(0.0, t.sol)
        if t.price_sol > 0:
            prices.append(t.price_sol)

    vol_usd = vol_sol * sol_usd if sol_usd > 0 else 0.0
    price_sol = prices[-1] if prices else 0.0
    price_usd = price_sol * sol_usd if (price_sol > 0 and sol_usd > 0) else 0.0

    # crude change proxy: compare first vs last in window
    chg_pct = 0.0
    if len(prices) >= 2 and prices[0] > 0:
        chg_pct = ((prices[-1] - prices[0]) / prices[0]) * 100.0

    return {
        "buys": buys,
        "sells": sells,
        "txns": buys + sells,
        "vol_sol": vol_sol,
        "vol_usd": vol_usd,
        "price_usd": price_usd,
        "chg_pct": chg_pct,
    }

def _pumpportal_state_to_obj(st: _TokenState, *, sol_usd: float, now: float) -> Dict[str, Any]:
    # Compute 5m/1h from trades; fall back to stored st.price_usd if present
    m5 = _trade_window_metrics(st.trades, now=now, wsec=300.0, sol_usd=sol_usd)
    m1h = _trade_window_metrics(st.trades, now=now, wsec=3600.0, sol_usd=sol_usd)

    # Estimate age
    created_ts = st.created_ts if st.created_ts > 0 else (st.last_ts if st.last_ts > 0 else 0.0)

    # Whale heuristic in SOL terms
    largest_buy_sol = 0.0
    whales_5m = 0
    for t in st.trades:
        if (now - t.ts) > 300.0:
            continue
        if t.side == "buy":
            largest_buy_sol = max(largest_buy_sol, t.sol)
    if largest_buy_sol >= float(PF_SCAN_MIN_WHALE_SIZE_SOL):
        whales_5m = 1

    # liquidity / mcap can be missing; keep 0 but mark unknown flags
    liq_usd = float(st.liq_usd or 0.0)
    mcap_usd = float(st.mcap_usd or 0.0)
    liq_unknown = (liq_usd <= 0.0)
    mcap_unknown = (mcap_usd <= 0.0)

    price_usd = float(st.price_usd or 0.0)
    if price_usd <= 0 and m5["price_usd"] > 0:
        price_usd = float(m5["price_usd"])

    token_age_min = _age_min(created_ts) or 9999.0

    buy_ratio_5m = _buy_ratio(int(m5["buys"]), int(m5["sells"]))
    vol_to_liq = (float(m5["vol_usd"]) / max(1.0, liq_usd)) if (liq_usd > 0) else 0.0
    momo_hint = (buy_ratio_5m * 100.0) + (min(2.0, vol_to_liq) * 25.0) + (max(-20.0, min(20.0, float(m5["chg_pct"]))) * 0.8)

    return {
        # keys expected by our normalizers
        "mintAddress": st.mint,
        "symbol": st.symbol or (st.mint[:6] if st.mint else ""),
        "name": st.name or "",
        "createdAt": created_ts,
        "liquidityUsd": liq_usd,
        "marketCapUsd": mcap_usd,
        "priceUsd": price_usd,

        "buys5m": int(m5["buys"]),
        "sells5m": int(m5["sells"]),
        "volumeUsd5m": float(m5["vol_usd"]),
        "priceChange5m": float(m5["chg_pct"]),

        "volumeUsd1h": float(m1h["vol_usd"]),
        "priceChange1h": float(m1h["chg_pct"]),

        # hints / extras
        "liq_unknown": bool(liq_unknown),
        "mcap_unknown": bool(mcap_unknown),
        "largest_buy_5m_sol": float(largest_buy_sol),
        "whales_5m": int(whales_5m),
        "momo_hint": float(momo_hint),
    }


# ──────────────────────────────────────────────────────────────────────────────
# Extract metrics from generic obj (HTTP or PumpPortal-normalized)
# ──────────────────────────────────────────────────────────────────────────────
def _extract_metrics(obj: Dict[str, Any]) -> Dict[str, Any]:
    liq_usd = 0.0
    for k in ("liquidityUsd", "liquidityUSD", "liqUsd", "lpUsd", "liquidity_usd", "liquidity"):
        v = obj.get(k)
        if v is not None:
            liq_usd = _nz(v, 0.0)
            break

    fdv_usd = _nz(
        obj.get("fdv")
        or obj.get("fdvUsd")
        or obj.get("fullyDilutedValuation")
        or obj.get("fullyDilutedMarketCap"),
        0.0,
    )
    mcap_usd = _nz(obj.get("marketCap") or obj.get("marketCapUsd") or obj.get("marketCapUSD") or obj.get("mcap_usd") or fdv_usd, 0.0)

    buys_5m = _safe_int(obj.get("buys5m") or obj.get("buys_5m") or obj.get("m5Buys") or obj.get("buysLast5m"))
    sells_5m = _safe_int(obj.get("sells5m") or obj.get("sells_5m") or obj.get("m5Sells") or obj.get("sellsLast5m"))

    vol_5m_usd = _nz(obj.get("volumeUsd5m") or obj.get("volume5mUsd") or obj.get("vol5mUsd"), 0.0)
    vol_1h_usd = _nz(obj.get("volumeUsd1h") or obj.get("volume1hUsd"), 0.0)
    vol_24h_usd = _nz(obj.get("volumeUsd24h") or obj.get("volume24hUsd") or obj.get("usdVolume24Hours"), 0.0)

    chg_5m_pct = _nz(obj.get("priceChange5m") or obj.get("priceChange_5m"), 0.0)
    chg_1h_pct = _nz(obj.get("priceChange1h") or obj.get("priceChange_1h"), 0.0)
    chg_24h_pct = _nz(obj.get("priceChange24h") or obj.get("priceChange_24h"), 0.0)

    created = obj.get("createdAt") or obj.get("createdAtTs") or obj.get("created") or obj.get("createdOn")
    age_min = _age_min(created if created is not None else None)
    token_age_min = age_min if age_min is not None else 9999.0

    holders = _safe_int(obj.get("holders") or obj.get("holderCount") or obj.get("totalHolders") or obj.get("holdersCount"))
    price_usd = _nz(obj.get("priceUsd") or obj.get("usdPrice") or obj.get("price") or obj.get("tokenPriceUsd"), 0.0)
    impact_1k_bps = _nz(obj.get("priceImpact1kBps") or obj.get("impact1kBps") or obj.get("impact_bps") or obj.get("impact_1k_bps"), 0.0)

    whales_5m = _safe_int(obj.get("whales_5m") or obj.get("whales5m") or 0)
    largest_buy_5m_sol = _nz(obj.get("largest_buy_5m_sol") or 0.0, 0.0)
    whale_vol_5m_usd = 0.0
    if largest_buy_5m_sol > 0 and price_usd > 0:
        # extremely rough conversion; still useful as a hint
        whale_vol_5m_usd = largest_buy_5m_sol * price_usd

    buy_ratio_5m = _buy_ratio(buys_5m, sells_5m)
    vol_to_liq = (vol_5m_usd / max(1.0, liq_usd)) if liq_usd > 0 else 0.0
    momo_hint = float(obj.get("momo_hint") or 0.0)
    if momo_hint == 0.0:
        momo_hint = (buy_ratio_5m * 100.0) + (min(2.0, vol_to_liq) * 25.0) + (max(-20.0, min(20.0, chg_5m_pct)) * 0.8)

    return {
        "liq_usd": float(max(0.0, liq_usd)),
        "mcap_usd": float(max(0.0, mcap_usd)),
        "fdv_usd": float(max(0.0, fdv_usd)),
        "token_age_min": float(max(0.0, token_age_min)),
        "vol_5m_usd": float(max(0.0, vol_5m_usd)),
        "vol_1h_usd": float(max(0.0, vol_1h_usd)),
        "vol_24h_usd": float(max(0.0, vol_24h_usd)),
        "buys_5m": int(max(0, buys_5m)),
        "sells_5m": int(max(0, sells_5m)),
        "txns_5m": int(max(0, buys_5m + sells_5m)),
        "price_usd": float(max(0.0, price_usd)),
        "price_change_5m_pct": float(chg_5m_pct),
        "price_change_1h_pct": float(chg_1h_pct),
        "price_change_24h_pct": float(chg_24h_pct),
        "holders": int(max(0, holders)),
        "impact_1k_bps": float(max(0.0, impact_1k_bps)),
        "whales_5m": int(max(0, whales_5m)),
        "whale_vol_5m_usd": float(max(0.0, whale_vol_5m_usd)),
        "largest_buy_5m_sol": float(max(0.0, largest_buy_5m_sol)),
        "buy_ratio_5m": float(_clamp(buy_ratio_5m, 0.0, 1.0)),
        "vol_to_liq_5m": float(max(0.0, vol_to_liq)),
        "momo_hint": float(momo_hint),
    }


def _map_obj_to_candidate(
    obj: Dict[str, Any],
    *,
    soft_liq: float,
    soft_mcap: float,
    soft_max_age: float,
    regime: Dict[str, Any],
    stats: _ScanStats,
    source: str,
) -> Optional[Dict[str, Any]]:
    raw_mint = (
        obj.get("mintAddress")
        or obj.get("mint")
        or obj.get("baseMint")
        or obj.get("tokenMint")
        or obj.get("id")
        or obj.get("publicKey")
    )
    mint = ""
    if isinstance(raw_mint, str):
        if _normalize_mint is not None:
            with contextlib.suppress(Exception):
                mint = str(_normalize_mint(raw_mint))  # type: ignore
        if not mint:
            m = BASE58_RE.search(raw_mint.strip())
            mint = m.group(0) if m else ""
    if not mint:
        stats.rejects.inc("REJ_NO_MINT")
        return None

    symbol = (obj.get("symbol") or obj.get("tokenSymbol") or obj.get("name") or mint[:6]) or mint[:6]
    m = _extract_metrics(obj)

    # Unknown-metric flags (PumpPortal often has unknown liq/mcap early)
    liq_unknown = bool(obj.get("liq_unknown", False))
    mcap_unknown = bool(obj.get("mcap_unknown", False))

    # soft filters (skip if unknown)
    if soft_liq > 0 and (not liq_unknown) and m["liq_usd"] < soft_liq:
        stats.rejects.inc("REJ_SOFT_LIQ", mint)
        return None
    if soft_mcap > 0 and (not mcap_unknown) and m["mcap_usd"] < soft_mcap:
        stats.rejects.inc("REJ_SOFT_MCAP", mint)
        return None
    if soft_max_age > 0 and m["token_age_min"] > soft_max_age:
        stats.rejects.inc("REJ_SOFT_AGE", mint)
        return None
    if HARD_MAX_AGE_MIN > 0.0 and m["token_age_min"] > float(HARD_MAX_AGE_MIN):
        stats.rejects.inc("REJ_HARD_AGE", mint)
        return None

    cand: Dict[str, Any] = {
        "candidate_schema": CANDIDATE_SCHEMA,
        "scanner": SCANNER_NAME,
        "scanner_version": SCANNER_VERSION,

        "source": source,
        "dex": "pumpfun",
        "chain": "SOL",

        "mint": mint,
        "symbol": str(symbol),

        "pair_url": f"https://pump.fun/coin/{mint}",
        "scan_ts": _now(),

        # regime
        "regime": str(regime.get("label", "unknown") or "unknown"),
        "regime_numeric": float(regime.get("label_numeric", 0.0)),
        "regime_manipulation": float(regime.get("manipulation_risk", 0.0)),
        "regime_ctx": dict(regime),

        # key metrics (flat)
        **m,

        # metrics quality tags
        "liq_unknown": bool(liq_unknown),
        "mcap_unknown": bool(mcap_unknown),

        # placeholders for downstream
        "ai_rug_score": 0.0,

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

        # conservative token-security defaults (your later checks can overwrite)
        "is_honeypot": False,
        "trading_paused": False,
        "blacklisted": False,
        "top10_hold_pct": 0.0,
        "dev_hold_pct": 0.0,
        "mint_authority_disabled": True,
        "freeze_authority_disabled": True,

        # common legacy aliases (avoid downstream KeyError)
        "age_min": m["token_age_min"],
        "chg5": m["price_change_5m_pct"],
        "buys5": m["buys_5m"],
        "sells5": m["sells_5m"],
        "vol5": m["vol_5m_usd"],
        "liq": m["liq_usd"],
    }
    return cand


def _dedupe_best_by_mint(metas: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    best: Dict[str, Dict[str, Any]] = {}
    for m in metas:
        mint = m.get("mint")
        if not isinstance(mint, str) or not mint:
            continue
        cur = best.get(mint)
        if cur is None:
            best[mint] = m
            continue
        a = (float(m.get("vol_5m_usd") or 0.0), int(m.get("buys_5m") or 0), float(m.get("liq_usd") or 0.0))
        b = (float(cur.get("vol_5m_usd") or 0.0), int(cur.get("buys_5m") or 0), float(cur.get("liq_usd") or 0.0))
        if a > b:
            best[mint] = m
    return list(best.values())

def _attach_risk(metas: List[Dict[str, Any]], risk_ctx: Optional[Dict[str, Any]]) -> None:
    if not metas or not risk_ctx:
        return
    cap = _clamp(float(risk_ctx.get("risk_capacity", 1.0) or 1.0), 0.0, 1.0)
    mode = str(risk_ctx.get("risk_mode", "normal") or "normal")
    for m in metas:
        m["risk_capacity"] = cap
        m["risk_mode"] = mode


# ──────────────────────────────────────────────────────────────────────────────
# Optional enrichment (AI rug + smart wallets)
# ──────────────────────────────────────────────────────────────────────────────
async def _enrich_optional(metas: List[Dict[str, Any]], stats: _ScanStats) -> None:
    if not metas:
        return

    sem = asyncio.Semaphore(max(1, _i("PUMPFUN_ENRICH_CONCURRENCY", 8)))
    to_rug = metas[:AI_RUG_MAX_PER_SCAN] if (AI_RUG_ENABLED and ai_fraud_score) else []

    sw_tracker = None
    if SMART_WALLETS_ENRICH and SW is not None:
        sw_tracker = getattr(SW, "TRACKER", None) or getattr(SW, "tracker", None) or None

    async def _one(m: Dict[str, Any]) -> None:
        mint = m.get("mint")
        if not isinstance(mint, str) or not mint:
            return

        async with sem:
            if m in to_rug and ai_fraud_score:
                s01 = await _call_maybe_async(ai_fraud_score, mint)
                if s01 is not None:
                    m["ai_rug_score"] = _clamp(float(s01), 0.0, 1.0)

            if sw_tracker is not None and m in metas[:SMART_WALLETS_MAX_PER_SCAN]:
                bf = getattr(sw_tracker, "build_features", None)
                if callable(bf):
                    feats = await _call_maybe_async(bf, mint)
                    if isinstance(feats, dict):
                        for k in (
                            "smart_wallet_score",
                            "smart_signal_confidence",
                            "smart_signal_quality",
                            "smart_risk_flag",
                            "smart_entry_boost_hint",
                            "smart_exit_pressure_hint",
                            "smart_tradeable_flag",
                        ):
                            if k in feats:
                                m[k] = feats.get(k)

    tasks = [asyncio.create_task(_one(m)) for m in metas]
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)


# ──────────────────────────────────────────────────────────────────────────────
# Scoring (preferred)
# ──────────────────────────────────────────────────────────────────────────────
async def _score_with_service(
    metas: List[Dict[str, Any]],
    *,
    regime: Dict[str, Any],
    risk_ctx: Optional[Dict[str, Any]],
    stats: _ScanStats,
) -> List[Dict[str, Any]]:
    if not SCORING_ENABLED or not metas or score_candidate is None:
        return []

    pre: List[Dict[str, Any]] = []
    for m in metas:
        mint = str(m.get("mint") or "")
        try:
            if AI_RUG_ENABLED and float(m.get("ai_rug_score") or 0.0) >= float(MAX_AI_RUG):
                stats.rejects.inc("REJ_AI_RUG", mint)
                continue
            if not is_viable_pair(m):
                stats.rejects.inc("REJ_LEGACY_VIABLE_FALSE", mint)
                continue
            pre.append(m)
        except Exception:
            pre.append(m)

    if not pre:
        pre = metas

    pre.sort(key=lambda x: (float(x.get("vol_5m_usd") or 0.0), int(x.get("buys_5m") or 0)), reverse=True)
    targets = pre[:SCORING_MAX_PER_SCAN]

    sem = asyncio.Semaphore(SCORING_CONCURRENCY)
    out: List[Dict[str, Any]] = []

    async def _one(meta: Dict[str, Any]) -> None:
        mint = str(meta.get("mint") or "")
        raw = dict(meta)
        ctx = {
            "source": str(meta.get("source") or "pumpfun"),
            "scanner": "pumpfun_scanner",
            "phase": MASTER_PHASE,
            "mode": PUMPFUN_MODE,
            "regime": dict(regime),
            "risk": dict(risk_ctx or {}),
        }

        async with sem:
            try:
                st: Any = await score_candidate(raw, context=ctx)  # type: ignore[misc]
            except Exception as e:
                stats.rejects.inc("REJ_SCORE_EXCEPTION", mint)
                log.every("PUMP_SCORE_FAIL", n=10, mint=mint, err=_short(e))
                return

        try:
            m = dict(meta)
            m["score_combined"] = float(getattr(st, "combined_score", 0.0))
            m["score_label"] = str(getattr(st, "label", "") or "")
            m["score_momentum"] = float(getattr(st, "momentum_score", 0.0))
            m["score_risk"] = float(getattr(st, "risk_score", 0.0))
            m["score_quality"] = float(getattr(st, "quality_score", 0.0))
            m["score_ai"] = float(getattr(st, "ai_score", 0.0))
            m["score_reasons"] = getattr(st, "reasons", None)
            m["score_is_trade_viable"] = bool(getattr(st, "is_trade_viable", True))
            m["score_trade_gate_reason"] = getattr(st, "trade_gate_reason", None)
            m["rank_score"] = float(m["score_combined"])
            out.append(m)
        except Exception:
            stats.rejects.inc("REJ_SCORE_MAP_FAIL", mint)

    tasks = [asyncio.create_task(_one(m)) for m in targets]
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)
    return out

def _select_from_scored(scored: List[Dict[str, Any]], *, min_combined: float, stats: _ScanStats) -> List[Dict[str, Any]]:
    if not scored:
        return []

    runners: List[Dict[str, Any]] = []
    for m in scored:
        mint = str(m.get("mint") or "")
        try:
            label = str(m.get("score_label") or "").upper() or "YELLOW"
            combined = float(m.get("score_combined") or 0.0)
            trade_ok = bool(m.get("score_is_trade_viable", True))

            if not trade_ok:
                stats.rejects.inc("REJ_TRADE_NOT_VIABLE", mint)
                continue
            if combined < float(min_combined):
                stats.rejects.inc("REJ_SCORE_TOO_LOW", mint)
                continue
            if _LABEL_LEVEL.get(label, 0) < _REQUIRED_LABEL_LEVEL:
                stats.rejects.inc("REJ_LABEL_TOO_LOW", mint)
                continue
            if AI_RUG_ENABLED and float(m.get("ai_rug_score") or 0.0) >= float(MAX_AI_RUG):
                stats.rejects.inc("REJ_AI_RUG", mint)
                continue

            runners.append(m)
        except Exception:
            stats.rejects.inc("REJ_SELECT_EXCEPTION", mint)
            continue

    runners.sort(key=lambda x: float(x.get("score_combined") or 0.0), reverse=True)
    return runners[:MAX_RUNNERS]


# ──────────────────────────────────────────────────────────────────────────────
# Legacy PF rank fallback
# ──────────────────────────────────────────────────────────────────────────────
def _legacy_pf_rank(metas: List[Dict[str, Any]], stats: _ScanStats) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for meta in metas:
        mint = str(meta.get("mint") or "")
        try:
            if AI_RUG_ENABLED and float(meta.get("ai_rug_score") or 0.0) >= float(MAX_AI_RUG):
                stats.rejects.inc("REJ_AI_RUG", mint)
                continue
            if not is_viable_pair(meta):
                stats.rejects.inc("REJ_LEGACY_VIABLE_FALSE", mint)
                continue

            momentum = float(compute_momentum_score(meta))
            final = float(compute_final_score(meta))
            whales = int(meta.get("whales_5m") or 0)
            vol5 = float(meta.get("vol_5m_usd") or 0.0)

            rank = final + 0.2 * momentum
            if whales > 0:
                rank += 0.15

            vol_boost = max(1.0, log1p(max(vol5, 1.0)) / 5.0)
            rank = float(rank) * vol_boost

            m = dict(meta)
            m["final_score"] = final
            m["momentum_score"] = momentum
            m["rank_score"] = float(rank)
            out.append(m)
        except Exception:
            stats.rejects.inc("REJ_LEGACY_EXCEPTION", mint)
            continue

    out.sort(key=lambda x: float(x.get("rank_score") or 0.0), reverse=True)
    return out[:MAX_RUNNERS]


# ──────────────────────────────────────────────────────────────────────────────
# Heuristic fallback
# ──────────────────────────────────────────────────────────────────────────────
def _fallback_pick(
    metas: List[Dict[str, Any]],
    *,
    min_vol5_usd: float,
    max_impact_bps: float,
    stats: _ScanStats,
) -> List[Dict[str, Any]]:
    if not metas:
        return []

    cands: List[Dict[str, Any]] = []
    for m in metas:
        mint = str(m.get("mint") or "")
        try:
            buys5 = int(m.get("buys_5m") or 0)
            vol5 = float(m.get("vol_5m_usd") or 0.0)
            impact = float(m.get("impact_1k_bps") or 0.0)
            rug = float(m.get("ai_rug_score") or 0.0)

            if buys5 < FB_MIN_BUYS_5M:
                stats.rejects.inc("REJ_FB_MIN_BUYS", mint)
                continue
            if vol5 < float(min_vol5_usd):
                stats.rejects.inc("REJ_FB_MIN_VOL5", mint)
                continue
            if max_impact_bps > 0 and impact > float(max_impact_bps):
                stats.rejects.inc("REJ_FB_IMPACT", mint)
                continue
            if AI_RUG_ENABLED and rug >= float(MAX_AI_RUG):
                stats.rejects.inc("REJ_AI_RUG", mint)
                continue

            cands.append(m)
        except Exception:
            stats.rejects.inc("REJ_FB_EXCEPTION", mint)
            continue

    if not cands:
        return []

    def _key(mm: Dict[str, Any]) -> Tuple[float, float, float]:
        return (
            float(mm.get("momo_hint") or 0.0),
            float(mm.get("vol_5m_usd") or 0.0),
            float(mm.get("liq_usd") or 0.0),
        )

    cands.sort(key=_key, reverse=True)
    picked = cands[:FB_MAX_RUNNERS]
    for m in picked:
        m.setdefault("rank_score", float(m.get("momo_hint") or m.get("vol_5m_usd") or 0.0))
    return picked


# ──────────────────────────────────────────────────────────────────────────────
# PumpPortal collection (primary fix)
# ──────────────────────────────────────────────────────────────────────────────
async def _collect_from_pumpportal(
    *,
    soft_liq: float,
    soft_mcap: float,
    soft_max_age: float,
    regime: Dict[str, Any],
    stats: _ScanStats,
) -> Tuple[List[Dict[str, Any]], str]:
    client = await _pumpportal_client()
    st = client.stats()
    if not st.get("ready", False):
        stats.rejects.inc("REJ_PUMPPORTAL_NOT_READY")
        return [], "pumpportal:not_ready"

    now = time.time()
    sol_usd = await _sol_usd()

    # Build objects from token states
    states = client.snapshot(max_items=PUMPPORTAL_SNAP_MAX)
    objs = [_pumpportal_state_to_obj(s, sol_usd=sol_usd, now=now) for s in states]

    metas: List[Dict[str, Any]] = []
    for obj in objs:
        mm = _map_obj_to_candidate(
            obj,
            soft_liq=soft_liq,
            soft_mcap=soft_mcap,
            soft_max_age=soft_max_age,
            regime=regime,
            stats=stats,
            source="pumpfun_ws",
        )
        if mm:
            metas.append(mm)

    # Optional: only keep those active recently (avoid stale)
    cutoff = now - float(PUMPPORTAL_TRACK_WINDOW_SEC)
    metas = [m for m in metas if (m.get("scan_ts") or 0) >= 0 and (m.get("token_age_min") or 0) >= 0]
    # Sort by activity signal
    metas.sort(key=lambda x: (float(x.get("vol_5m_usd") or 0.0), int(x.get("buys_5m") or 0)), reverse=True)

    return _dedupe_best_by_mint(metas), "pumpportal"


# ──────────────────────────────────────────────────────────────────────────────
# Alerts + training logs
# ──────────────────────────────────────────────────────────────────────────────
def _effective_regime_tag(regime: Dict[str, Any]) -> str:
    if REGIME_TAG_ENV:
        return REGIME_TAG_ENV
    lbl = str(regime.get("label") or "").strip()
    return lbl or "pumpfun_live"

def _effective_wallet_tag() -> Optional[str]:
    return WALLET_TAG_ENV or None

def _log_candidates_for_training(runners: List[Dict[str, Any]], *, regime: Dict[str, Any]) -> None:
    if not (PG_ENABLED and LOG_CANDIDATES and data_logger and runners):
        return
    regime_tag = _effective_regime_tag(regime)
    wallet_tag = _effective_wallet_tag()

    log_pumpfun = getattr(data_logger, "log_pumpfun_candidate", None)
    log_entry = getattr(data_logger, "log_entry_candidate", None)

    for meta in runners:
        try:
            mint = str(meta.get("mint") or "").strip()
            if not mint:
                continue
            symbol = (meta.get("symbol") or "").strip() or None
            snapshot = dict(meta)

            if callable(log_pumpfun):
                log_pumpfun(mint=mint, symbol=symbol, meta=snapshot, regime=regime_tag, wallet_tag=wallet_tag)  # type: ignore[misc]
            elif callable(log_entry):
                log_entry(src="pumpfun", mint=mint, symbol=symbol, snapshot=snapshot, regime=regime_tag, wallet_tag=wallet_tag)  # type: ignore[misc]
        except Exception:
            with contextlib.suppress(Exception):
                log.exception("Pump.fun: training log failed mint=%s", meta.get("mint"))  # type: ignore

async def _send_momo(runners: List[Dict[str, Any]]) -> None:
    if not SEND_MOMO_ALERTS or not runners or AlertManager is None:
        return
    try:
        am = AlertManager()
    except Exception as e:
        log.every("PUMP_ALERT_INIT_FAIL", n=30, err=_short(e))
        return

    max_n = max(0, min(MAX_ALERTS_PER_SCAN, len(runners)))

    async with _recent_alert_lock:
        now = time.time()
        cutoff = now - float(ALERT_DEDUP_SEC)
        for k in list(_recent_alert_ts.keys()):
            if _recent_alert_ts.get(k, 0.0) < cutoff:
                _recent_alert_ts.pop(k, None)

    for c in runners[:max_n]:
        mint = str(c.get("mint") or "")
        if not mint:
            continue

        async with _recent_alert_lock:
            now = time.time()
            last = _recent_alert_ts.get(mint, 0.0)
            if now - last < float(ALERT_DEDUP_SEC):
                continue
            _recent_alert_ts[mint] = now

        try:
            age_min = c.get("token_age_min")
            age_sec = float(age_min) * 60.0 if isinstance(age_min, (int, float)) else None

            ctx: Dict[str, Any] = {
                "symbol": c.get("symbol") or mint,
                "mint": mint,
                "pool": "pumpfun",
                "pool_url": c.get("pair_url"),
                "age_sec": age_sec,
                "liq_usd": c.get("liq_usd"),
                "vol_5m": c.get("vol_5m_usd"),
                "up_pct": c.get("price_change_5m_pct"),
                "momo_hint": c.get("momo_hint"),
                "ai_rug_score": c.get("ai_rug_score"),
                "rank_score": c.get("rank_score"),
                "risk_capacity": c.get("risk_capacity"),
                "risk_mode": c.get("risk_mode"),
                "score_combined": c.get("score_combined"),
                "score_label": c.get("score_label"),
                "score_ai": c.get("score_ai"),
                "score_risk": c.get("score_risk"),
                "score_quality": c.get("score_quality"),
                "score_momentum": c.get("score_momentum"),
                "reason": (
                    f"pump runner vol5=${float(c.get('vol_5m_usd') or 0):.0f} "
                    f"buys5={int(c.get('buys_5m') or 0)} "
                    f"up5={float(c.get('price_change_5m_pct') or 0):.2f}% "
                    f"score={float(c.get('score_combined') or 0):.1f} "
                    f"label={c.get('score_label')}"
                ),
            }
            await _call_maybe_async(getattr(am, "send_momo_event", None), "MOMO_OK", ctx)
        except Exception as e:
            log.every("PUMP_MOMO_FAIL", n=10, mint=mint, err=_short(e))


# ──────────────────────────────────────────────────────────────────────────────
# Public entrypoint
# ──────────────────────────────────────────────────────────────────────────────
async def scan_pumpfun(*args: Any) -> List[Dict[str, Any]]:
    if not PUMPFUN_ENABLED or PUMPFUN_MODE == "off":
        return []

    redis = None
    # accept flexible args: (redis) or (session) or (redis, session)
    if len(args) >= 1 and not isinstance(args[0], aiohttp.ClientSession):
        redis = args[0]

    stats = _ScanStats(t0=time.time())

    # Risk snapshot
    try:
        risk_ctx = await risk_capacity_snapshot(redis)
    except Exception:
        risk_ctx = None

    risk_capacity = _clamp(float((risk_ctx or {}).get("risk_capacity", 1.0) or 1.0), 0.0, 1.0)
    risk_shutdown = bool((risk_ctx or {}).get("risk_shutdown", False))
    risk_mode = str((risk_ctx or {}).get("risk_mode", "normal") or "normal")
    stats.risk_capacity, stats.risk_mode = risk_capacity, risk_mode

    if risk_shutdown or risk_capacity <= 0.0:
        stats.rejects.inc("REJ_RISK_SHUTDOWN")
        log.every("PUMP_RISK_SHUTDOWN", n=10, risk_shutdown=int(risk_shutdown), risk_capacity=round(risk_capacity, 3), risk_mode=risk_mode)
        return []

    # Regime snapshot + tuned thresholds
    regime = await _regime_snapshot()
    stats.regime_label = str(regime.get("label", "unknown") or "unknown")
    stats.regime_numeric = float(regime.get("label_numeric", 0.0))
    stats.manip_risk = float(regime.get("manipulation_risk", 0.0))

    tuned_soft_liq, tuned_soft_mcap, tuned_fb_min_vol5, tuned_fb_max_impact = _regime_tune(
        SOFT_MIN_LIQ_USD, SOFT_MIN_MCAP_USD, FB_MIN_VOL5_USD, FB_MAX_IMPACT_1K_BPS, regime
    )
    min_combined = _dynamic_min_combined(SCORING_MIN_COMBINED, regime, risk_capacity)

    log.every(
        "PUMP_SCAN_START",
        n=5,
        phase=MASTER_PHASE,
        mode=PUMPFUN_MODE,
        provider=PUMP_PROVIDER,
        ws_url=PUMP_WS_URL if PUMP_PROVIDER in {"auto", "pumpportal"} else "",
        tuned_soft_liq=int(tuned_soft_liq),
        tuned_soft_mcap=int(tuned_soft_mcap),
        soft_max_age=round(SOFT_MAX_AGE_MIN, 2),
        min_combined=round(min_combined, 2),
        min_label=SCORING_MIN_LABEL,
        scoring=int(bool(SCORING_ENABLED and score_candidate)),
        legacy=int(bool(LEGACY_PF_ENABLED)),
        regime=stats.regime_label,
        manip=round(stats.manip_risk, 3),
        risk_capacity=round(risk_capacity, 3),
        risk_mode=risk_mode,
    )

    try:
        metas: List[Dict[str, Any]] = []
        source = "empty"

        # Provider: PumpPortal WS (PRIMARY FIX)
        if PUMP_PROVIDER in {"pumpportal", "auto"}:
            mm, src = await _collect_from_pumpportal(
                soft_liq=tuned_soft_liq,
                soft_mcap=tuned_soft_mcap,
                soft_max_age=SOFT_MAX_AGE_MIN,
                regime=regime,
                stats=stats,
            )
            if mm:
                metas, source = mm, src
            elif PUMP_PROVIDER == "pumpportal":
                # hard provider: if WS yields nothing, we still return empty (no broken HTTP mirror)
                source = src

        # NOTE: HTTP fallback intentionally omitted here (your old sources were blocked/dead),
        # because your chosen provider is pumpportal. In "auto" mode you can add an HTTP
        # collector later if you want; for now we keep this file laser-focused on the fix.

        metas = _dedupe_best_by_mint(metas)
        _attach_risk(metas, risk_ctx)

        stats.source = source
        stats.raw = len(metas)
        stats.metas = len(metas)

        if DEBUG_META_SAMPLE:
            sample = [{
                "sym": m.get("symbol"),
                "liq": m.get("liq_usd"),
                "mcap": m.get("mcap_usd"),
                "vol5": m.get("vol_5m_usd"),
                "buys5": m.get("buys_5m"),
                "chg5": m.get("price_change_5m_pct"),
                "age": m.get("token_age_min"),
                "unknown": int(bool(m.get("liq_unknown") or m.get("mcap_unknown"))),
            } for m in metas[:5]]
            log.every("PUMP_META_SAMPLE", n=10, source=source, count=len(metas), first=sample)

        if not metas:
            return []

        # Optional enrichment
        await _enrich_optional(metas, stats)
        stats.enriched = len(metas)

        # Scoring-first
        runners: List[Dict[str, Any]] = []
        if SCORING_ENABLED:
            scored = await _score_with_service(metas, regime=regime, risk_ctx=risk_ctx, stats=stats)
            stats.scored = len(scored)
            runners = _select_from_scored(scored, min_combined=min_combined, stats=stats)

        # Legacy PF rank fallback
        if not runners and LEGACY_PF_ENABLED:
            runners = _legacy_pf_rank(metas, stats)

        # Heuristic fallback
        if not runners:
            runners = _fallback_pick(metas, min_vol5_usd=tuned_fb_min_vol5, max_impact_bps=tuned_fb_max_impact, stats=stats)
            stats.fb_runners = len(runners)

        if not runners:
            return []

        # Final sort/cap + risk shrink
        runners = _dedupe_best_by_mint(runners)
        runners.sort(
            key=lambda x: float(x.get("rank_score") or x.get("score_combined") or x.get("momo_hint") or 0.0),
            reverse=True,
        )
        runners = runners[:MAX_RUNNERS]

        if RISK_SHRINK_ENABLED and runners:
            raw_n = len(runners)
            allowed = int(raw_n * risk_capacity)
            if risk_capacity > 0.0 and allowed < 1:
                allowed = 1
            if allowed < raw_n:
                runners = runners[:allowed]
                log.every("PUMP_RISK_SHRINK", n=10, before=raw_n, after=allowed, risk_capacity=round(risk_capacity, 3))

        # Redis runner dedup (cooldown)
        if redis is not None and RUNNER_DEDUP_ENABLED and RUNNER_DEDUP_SEC > 0 and runners:
            filtered: List[Dict[str, Any]] = []
            for r in runners:
                mint = str(r.get("mint") or "")
                if not mint:
                    continue
                k = f"{REDIS_KEY_PREFIX}:runner:{mint}"
                seen = await _redis_get(redis, k)
                if seen:
                    stats.rejects.inc("REJ_REDIS_DEDUP", mint)
                    continue
                await _redis_setex(redis, k, int(RUNNER_DEDUP_SEC), "1")
                filtered.append(r)
            runners = filtered or runners

        stats.runners = len(runners)

        if DEBUG_RESULT:
            log.every(
                "PUMP_RESULT",
                n=10,
                source=source,
                metas=stats.metas,
                scored=stats.scored,
                fb=stats.fb_runners,
                runners=stats.runners,
                pumpportal=_PUMPPORTAL.stats() if _PUMPPORTAL else {},
            )

        # Alerts + training logs
        await _send_momo(runners)
        _log_candidates_for_training(runners, regime=regime)

        # Summary event (to DB/logger if available)
        if LOG_SCAN_SUMMARY and _log_event is not None:
            payload = {
                "ts": time.time(),
                "phase": MASTER_PHASE,
                "mode": PUMPFUN_MODE,
                "provider": PUMP_PROVIDER,
                "source": source,
                "raw_items": stats.raw,
                "metas": stats.metas,
                "enriched": stats.enriched,
                "scored": stats.scored,
                "fb_runners": stats.fb_runners,
                "runners": stats.runners,
                "rejects": dict(stats.rejects.counts),
                "reject_samples": dict(stats.rejects.samples),
                "regime": dict(regime),
                "risk": dict(risk_ctx or {}),
                "tuned": {
                    "tuned_soft_liq": tuned_soft_liq,
                    "tuned_soft_mcap": tuned_soft_mcap,
                    "min_combined_dynamic": min_combined,
                    "tuned_fb_min_vol5": tuned_fb_min_vol5,
                    "tuned_fb_max_impact": tuned_fb_max_impact,
                },
                "elapsed_ms": int((time.time() - stats.t0) * 1000),
                "scanner_version": SCANNER_VERSION,
                "pumpportal": _PUMPPORTAL.stats() if _PUMPPORTAL else {},
            }
            await _call_maybe_async(_log_event, "pumpfun_scan_summary", payload)

        return runners

    except Exception as e:
        stats.rejects.inc("REJ_SCAN_EXCEPTION")
        log.every("PUMP_SCAN_FAIL", n=5, err=_short(e))
        return []
    finally:
        # ALWAYS log reject bucket summary to kill silent failure modes
        if stats.rejects.counts:
            log.every(
                "PUMP_REJECT_BUCKETS",
                n=10,
                source=stats.source,
                rejects=dict(stats.rejects.counts),
                samples=dict(stats.rejects.samples),
                metas=stats.metas,
                runners=stats.runners,
            )


async def shutdown_pumpfun_scanner() -> None:
    """Optional graceful shutdown hook (safe to call multiple times)."""
    global _PUMPPORTAL
    if _PUMPPORTAL is not None:
        with contextlib.suppress(Exception):
            await _PUMPPORTAL.close()
    _PUMPPORTAL = None


__all__ = ["scan_pumpfun", "shutdown_pumpfun_scanner"]

if __name__ == "__main__":  # pragma: no cover
    async def _demo() -> None:
        # Let WS warm up
        await asyncio.sleep(3)
        res = await scan_pumpfun()
        print("runners:", len(res))
        if res:
            r = res[0]
            print(
                r.get("symbol"),
                r.get("mint"),
                r.get("score_combined"),
                r.get("score_label"),
                r.get("vol_5m_usd"),
                r.get("buys_5m"),
                r.get("liq_usd"),
                r.get("liq_unknown"),
            )
        await shutdown_pumpfun_scanner()
    asyncio.run(_demo())
