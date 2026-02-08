from __future__ import annotations

"""
scanner/pumpfun_scanner.py — Pump.fun scanner (V3+++ / God-tier LOCK-IN)
========================================================================

Primary provider: PumpPortal WebSocket (single connection, resilient, bounded state).
Optional provider: HTTP fallback (OFF unless configured).

Hard requirements
-----------------
✅ No network at import time
✅ Render-friendly (TLS, timeouts, trust_env support)
✅ Bounded memory (tokens/trades)
✅ No silent no-buy: reject buckets + periodic summaries
✅ Candidate dict output compatible with downstream entry pipeline
✅ Backwards-compatible API: scan_pumpfun / scan_once / scan_loop / shutdown
✅ Cancellation-safe: never swallow asyncio.CancelledError

Canonical candidate shape (minimum)
-----------------------------------
  - chain, dex, mint, symbol, pair_url, scan_ts, source, scanner, scanner_version
  - liq_usd, mcap_usd, token_age_min
  - vol_5m_usd, buys_5m, sells_5m, txns_5m
  - price_usd, price_change_5m_pct
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
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from functools import lru_cache
from math import log1p
from typing import Any, Protocol, TypedDict, cast

import aiohttp
import certifi

# =============================================================================
# Logging (NO monkey-patching)
# =============================================================================


class _BaseLogger(Protocol):
    def info(self, msg: str, *args: Any, **kwargs: Any) -> None: ...
    def warning(self, msg: str, *args: Any, **kwargs: Any) -> None: ...
    def error(self, msg: str, *args: Any, **kwargs: Any) -> None: ...
    def exception(self, msg: str, *args: Any, **kwargs: Any) -> None: ...


class EventLogger:
    """
    Wrapper that provides .event() and .every() WITHOUT mutating the underlying logger.

    This avoids Phase-2 "logger monkey patch hotspots" while preserving the same call-site API.
    """

    __slots__ = ("_log", "_every_store")

    def __init__(self, base: _BaseLogger) -> None:
        self._log = base
        self._every_store: dict[str, float] = {}

    def info(self, msg: str, *args: Any, **kwargs: Any) -> None:
        self._log.info(msg, *args, **kwargs)

    def warning(self, msg: str, *args: Any, **kwargs: Any) -> None:
        self._log.warning(msg, *args, **kwargs)

    def error(self, msg: str, *args: Any, **kwargs: Any) -> None:
        self._log.error(msg, *args, **kwargs)

    def exception(self, msg: str, *args: Any, **kwargs: Any) -> None:
        self._log.exception(msg, *args, **kwargs)

    def event(self, name: str, **kw: Any) -> None:
        # Compact structured log without requiring custom logger methods.
        with contextlib.suppress(Exception):
            self._log.info("%s %s", name, kw)

    def every(self, key: str, n: float = 60.0, msg: str | None = None, **kw: Any) -> None:
        now = time.time()
        last = float(self._every_store.get(key, 0.0))
        if (now - last) >= float(n):
            self._every_store[key] = now
            self.event(msg or key, **kw)


def _make_logger(name: str) -> EventLogger:
    base: _BaseLogger
    try:
        from logging_setup import get_logger  # type: ignore

        base = cast(_BaseLogger, get_logger(name))
    except Exception:  # pragma: no cover
        base = logging.getLogger(name)
        if not getattr(base, "handlers", None):
            logging.basicConfig(level=logging.INFO)
    return EventLogger(base)


LOG = _make_logger("scanner.pumpfun")


def _short(x: Any, n: int = 240) -> str:
    try:
        s = str(x)
        return s if len(s) <= n else s[:n] + "…"
    except Exception:
        return "<?>"


# =============================================================================
# Safe config import (NO side effects required)
# =============================================================================
try:
    import config as C  # type: ignore
except Exception:  # pragma: no cover

    class _C:
        pass

    C = _C()  # type: ignore


# =============================================================================
# Optional modules (soft imports)
# =============================================================================
alert_manager_cls: type[Any] | None = None
try:
    from utils_pkg.alert_manager import AlertManager as _AlertManager  # type: ignore
except Exception:  # pragma: no cover
    pass
else:
    alert_manager_cls = _AlertManager

try:
    from utils_pkg.pair_filters import (  # type: ignore
        compute_final_score,
        compute_momentum_score,
        is_viable_pair,
    )
except Exception:  # pragma: no cover

    def is_viable_pair(_meta: dict[str, Any]) -> bool:  # type: ignore
        return True

    def compute_momentum_score(_meta: dict[str, Any]) -> float:  # type: ignore
        return 0.0

    def compute_final_score(_meta: dict[str, Any]) -> float:  # type: ignore
        return 0.0


try:
    from utils_pkg.scoring_service import score_candidate  # type: ignore
except Exception:  # pragma: no cover
    score_candidate = None  # type: ignore[assignment]

try:
    from utils_pkg import data_logger  # type: ignore
except Exception:  # pragma: no cover
    data_logger = None  # type: ignore[assignment]

try:
    from utils_pkg.data_logger import log_event as _log_event  # type: ignore
except Exception:  # pragma: no cover
    _log_event = None  # type: ignore[assignment]

try:
    from utils_pkg.regime import regime_state  # type: ignore
except Exception:  # pragma: no cover

    class _DummyRegimeState:
        @staticmethod
        def get() -> str:
            return "unknown"

        @staticmethod
        async def features() -> dict[str, Any]:
            return {
                "regime_label_numeric": 0.0,
                "trend_score": 0.0,
                "volatility_score": 0.0,
                "liquidity_score": 0.5,
                "manipulation_risk": 0.0,
                "regime_label": "unknown",
            }

    regime_state = _DummyRegimeState()  # type: ignore[assignment]


try:
    from utils_pkg.risk_manager import risk_capacity_snapshot  # type: ignore
except Exception:  # pragma: no cover

    async def risk_capacity_snapshot(_redis: Any = None) -> dict[str, Any]:  # type: ignore
        return {"risk_capacity": 1.0, "risk_shutdown": False, "risk_mode": "normal"}


try:
    from utils_pkg import smart_wallets as SW  # type: ignore
except Exception:  # pragma: no cover
    SW = None  # type: ignore[assignment]

AiFraudScoreFn = Callable[..., Any]

ai_fraud_score: AiFraudScoreFn | None = None
try:
    from utils_pkg.core_utils import ai_fraud_score as _ai_fraud_score  # type: ignore
except Exception:  # pragma: no cover
    pass
else:
    ai_fraud_score = _ai_fraud_score

try:
    from utils_pkg import core_utils as _cu  # type: ignore
except Exception:  # pragma: no cover
    _cu = None  # type: ignore[assignment]

try:
    from utils_pkg.mints import normalize_mint as _normalize_mint  # type: ignore
except Exception:  # pragma: no cover
    _normalize_mint = None  # type: ignore[assignment]


# =============================================================================
# Env helpers
# =============================================================================
_TRUE = {"1", "true", "t", "yes", "y", "on"}


def _b(name: str, default: bool = False) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return str(v).strip().lower() in _TRUE


def _s(name: str, default: str = "") -> str:
    v = os.getenv(name)
    return default if v is None else str(v).strip()


def _i(name: str, default: int) -> int:
    v = os.getenv(name)
    if v is None:
        return default
    vv = v.strip()
    if not vv:
        return default
    with contextlib.suppress(Exception):
        return int(vv)
    return default


def _f(name: str, default: float) -> float:
    v = os.getenv(name)
    if v is None:
        return default
    vv = v.strip()
    if not vv:
        return default
    with contextlib.suppress(Exception):
        return float(vv)
    return default


def _clamp(x: float, lo: float, hi: float) -> float:
    return lo if x < lo else hi if x > hi else x


def _nz(x: Any, d: float = 0.0) -> float:
    try:
        v = float(x)
        return d if v != v else v  # NaN -> default
    except Exception:
        return d


def _safe_int(x: Any, d: int = 0) -> int:
    with contextlib.suppress(Exception):
        return int(x)
    return d


async def _maybe_await(x: Any) -> Any:
    return await x if asyncio.iscoroutine(x) else x


async def _call_maybe_async(fn: Any, *a: Any, **k: Any) -> Any:
    """
    Call fn whether it's sync or async; return None on exception.
    Cancellation is respected (we do NOT swallow CancelledError).
    """
    if fn is None:
        return None
    try:
        if asyncio.iscoroutinefunction(fn):
            return await fn(*a, **k)
        return await asyncio.to_thread(fn, *a, **k)
    except asyncio.CancelledError:
        raise
    except Exception:
        return None


async def _gather_no_cancel(tasks: list[asyncio.Task[Any]]) -> None:
    """
    Gather tasks but NEVER swallow CancelledError.
    Other exceptions are allowed (fail-open enrichment/scoring).
    """
    if not tasks:
        return
    res = await asyncio.gather(*tasks, return_exceptions=True)
    for r in res:
        if isinstance(r, asyncio.CancelledError):
            raise r


@lru_cache(maxsize=1)
def _sslctx() -> ssl.SSLContext:
    return ssl.create_default_context(cafile=certifi.where())


# =============================================================================
# Identity / schema
# =============================================================================
BASE58_RE = re.compile(r"[1-9A-HJ-NP-Za-km-z]{32,44}")

SCANNER_NAME = "pumpfun_scanner"
SCANNER_VERSION = "v3+++_pumpportal_lockin"
CANDIDATE_SCHEMA = "CandidateSnapshot.v1"

_OFFLINE_ENV_KEYS = ("SAFE_MODE", "OFFLINE_MODE", "NO_NETWORK", "DRY_RUN")


def _offline_mode() -> bool:
    return any(_b(k, False) for k in _OFFLINE_ENV_KEYS)


# =============================================================================
# Config
# =============================================================================
@dataclass(frozen=True)
class PumpfunScannerConfig:
    enabled: bool
    mode: str
    provider: str

    ws_url: str
    sub_migrations: bool

    snap_max: int
    max_tokens: int
    max_trades_per_token: int
    subscribe_trade_max: int
    subscribe_refresh_s: float
    connect_timeout_s: float
    heartbeat_s: float

    receive_timeout_s: float
    max_msg_size_bytes: int
    user_agent: str

    soft_min_liq_usd: float
    soft_min_mcap_usd: float
    soft_max_age_min: float
    hard_max_age_min: float

    fb_min_buys_5m: int
    fb_min_vol5_usd: float
    fb_max_runners: int
    fb_max_impact_1k_bps: float

    scoring_enabled: bool
    scoring_max_per_scan: int
    scoring_concurrency: int
    scoring_min_combined: float
    scoring_min_label: str

    ai_rug_enabled: bool
    ai_rug_max_per_scan: int
    max_ai_rug: float

    smart_wallets_enrich: bool
    smart_wallets_max_per_scan: int

    max_runners: int
    runner_dedupe_enabled: bool
    runner_dedupe_sec: int
    redis_prefix: str

    send_momo_alerts: bool
    max_alerts_per_scan: int
    alert_dedupe_sec: float

    debug_meta_sample: bool
    debug_result: bool
    log_scan_summary: bool

    http_base_urls: tuple[str, ...]
    http_timeout_s: float
    http_retries: int
    http_backoff_s: float
    http_conn_limit: int
    http_trust_env: bool

    phase: int
    whale_min_sol: float

    @staticmethod
    def load() -> "PumpfunScannerConfig":
        enable_default = bool(getattr(C, "ENABLE_PUMPFUN_SCANNER", True))
        mode = _s("PUMPFUN_MODE", "full").lower().strip()
        if mode not in {"off", "simple", "alpha", "full"}:
            mode = "full"

        enabled = enable_default and _b("PUMPFUN_ENABLED", (mode != "off"))
        provider = _s("PUMP_PROVIDER", _s("PUMPFUN_PROVIDER", "auto")).lower().strip()
        if provider not in {"auto", "pumpportal", "http"}:
            provider = "auto"

        pf_min_liq = float(getattr(C, "PF_SCAN_MIN_LIQ_USD", 2500.0) or 2500.0)
        pf_min_mcap = float(getattr(C, "PF_SCAN_MIN_MCAP_USD", 0.0) or 0.0)
        pf_max_age = float(getattr(C, "PF_SCAN_MAX_TOKEN_AGE_MIN", 0.0) or 0.0)

        http_bases = tuple(
            u.strip() for u in _s("PUMPFUN_HTTP_BASE_URLS", "").split(",") if u.strip()
        )

        min_label = _s("PUMPFUN_SCORING_MIN_LABEL", "YELLOW").upper().strip()
        if min_label not in {"RED", "YELLOW", "GREEN"}:
            min_label = "YELLOW"

        phase = max(0, min(7, _i("MASTER_PHASE", _i("STRATEGY_PHASE", 7))))

        whale_default = float(getattr(C, "PF_SCAN_MIN_WHALE_SIZE_SOL", 3.0) or 3.0)
        whale_min_sol = max(0.0, _f("PUMPFUN_WHALE_MIN_SOL", whale_default))

        max_msg_default = 8 * 1024 * 1024
        max_msg_size = _i("PUMPPORTAL_MAX_MSG_SIZE_BYTES", max_msg_default)

        return PumpfunScannerConfig(
            enabled=enabled,
            mode=mode,
            provider=provider,
            ws_url=_s("PUMP_WS_URL", "wss://pumpportal.fun/api/data").strip(),
            sub_migrations=_b("PUMPPORTAL_SUB_MIGRATIONS", True),
            snap_max=max(25, _i("PUMPPORTAL_SNAP_MAX", 250)),
            max_tokens=max(100, _i("PUMPPORTAL_MAX_TOKENS", 1500)),
            max_trades_per_token=max(25, _i("PUMPPORTAL_MAX_TRADES_PER_TOKEN", 400)),
            subscribe_trade_max=max(10, _i("PUMPPORTAL_SUBSCRIBE_TRADE_MAX", 120)),
            subscribe_refresh_s=max(3.0, _f("PUMPPORTAL_SUBSCRIBE_REFRESH_SEC", 8.0)),
            connect_timeout_s=max(2.0, _f("PUMPPORTAL_CONNECT_TIMEOUT_S", 8.0)),
            heartbeat_s=max(10.0, _f("PUMPPORTAL_HEARTBEAT_S", 25.0)),
            receive_timeout_s=max(5.0, _f("PUMPPORTAL_RECEIVE_TIMEOUT_S", 30.0)),
            max_msg_size_bytes=max(64 * 1024, max_msg_size),
            user_agent=_s("PUMPFUN_UA", "pnlsniper"),
            soft_min_liq_usd=_f("PUMPFUN_SOFT_MIN_LIQ_USD", max(0.0, pf_min_liq * 0.4)),
            soft_min_mcap_usd=_f("PUMPFUN_SOFT_MIN_MCAP_USD", max(0.0, pf_min_mcap * 0.4)),
            soft_max_age_min=_f(
                "PUMPFUN_SOFT_MAX_AGE_MIN",
                (pf_max_age * 1.2) if pf_max_age > 0 else 0.0,
            ),
            hard_max_age_min=_f("PUMPFUN_HARD_MAX_AGE_MIN", 0.0),
            fb_min_buys_5m=max(0, _i("PUMPFUN_FB_MIN_BUYS_5M", 1)),
            fb_min_vol5_usd=max(0.0, _f("PUMPFUN_FB_MIN_VOL5_USD", 200.0)),
            fb_max_runners=max(1, _i("PUMPFUN_FB_MAX_RUNNERS", 8)),
            fb_max_impact_1k_bps=max(0.0, _f("PUMPFUN_FB_MAX_IMPACT_1K_BPS", 1500.0)),
            scoring_enabled=_b("PUMPFUN_SCORING_ENABLED", True) and (score_candidate is not None),
            scoring_max_per_scan=max(5, _i("PUMPFUN_SCORING_MAX_PER_SCAN", 80)),
            scoring_concurrency=max(1, _i("PUMPFUN_SCORING_CONCURRENCY", 6)),
            scoring_min_combined=_f("PUMPFUN_SCORING_MIN_COMBINED", 55.0),
            scoring_min_label=min_label,
            ai_rug_enabled=_b("PUMPFUN_AI_RUG_ENABLED", False) and (ai_fraud_score is not None),
            ai_rug_max_per_scan=max(0, _i("PUMPFUN_AI_RUG_MAX_PER_SCAN", 20)),
            max_ai_rug=_f("PUMPFUN_MAX_AI_RUG", 0.88),
            smart_wallets_enrich=_b("PUMPFUN_SMART_WALLETS_ENRICH", True),
            smart_wallets_max_per_scan=max(0, _i("PUMPFUN_SMART_WALLETS_MAX_PER_SCAN", 80)),
            max_runners=max(1, _i("PUMPFUN_MAX_RUNNERS", 25)),
            runner_dedupe_enabled=_b("PUMPFUN_RUNNER_DEDUP_ENABLED", True),
            runner_dedupe_sec=max(0, _i("PUMPFUN_RUNNER_DEDUP_SEC", 180)),
            redis_prefix=_s("PUMPFUN_REDIS_PREFIX", "pumpfun"),
            send_momo_alerts=_b("PUMPFUN_SEND_MOMO_ALERTS", True),
            max_alerts_per_scan=max(0, _i("PUMPFUN_MAX_ALERTS_PER_SCAN", 3)),
            alert_dedupe_sec=max(10.0, _f("PUMPFUN_ALERT_DEDUP_SEC", 180.0)),
            debug_meta_sample=_b("PUMPFUN_DEBUG_META_SAMPLE", True),
            debug_result=_b("PUMPFUN_DEBUG_RESULT", True),
            log_scan_summary=_b("PUMPFUN_LOG_SCAN_SUMMARY", True),
            http_base_urls=http_bases,
            http_timeout_s=max(5.0, _f("PUMPFUN_HTTP_TIMEOUT_SEC", 12.0)),
            http_retries=max(1, _i("PUMPFUN_HTTP_RETRIES", 2)),
            http_backoff_s=max(0.1, _f("PUMPFUN_HTTP_BACKOFF_SEC", 0.5)),
            http_conn_limit=max(10, _i("PUMPFUN_HTTP_CONN_LIMIT", 20)),
            http_trust_env=_b("PUMPFUN_HTTP_TRUST_ENV", True),
            phase=phase,
            whale_min_sol=whale_min_sol,
        )


CFG = PumpfunScannerConfig.load()

_LABEL_LEVEL = {"RED": 0, "YELLOW": 1, "GREEN": 2}
_REQUIRED_LABEL_LEVEL = _LABEL_LEVEL.get(CFG.scoring_min_label, 0)

_recent_alert_ts: dict[str, float] = {}
_recent_alert_lock: asyncio.Lock | None = None


def _alert_lock() -> asyncio.Lock:
    global _recent_alert_lock
    if _recent_alert_lock is None:
        _recent_alert_lock = asyncio.Lock()
    return _recent_alert_lock


# =============================================================================
# Reject buckets (kill silent no-buy)
# =============================================================================
@dataclass
class RejectBuckets:
    counts: dict[str, int] = field(default_factory=dict)
    samples: dict[str, list[str]] = field(default_factory=dict)

    def inc(self, reason: str, mint: str | None = None) -> None:
        r = (reason or "UNKNOWN").strip()
        self.counts[r] = int(self.counts.get(r, 0)) + 1
        if not mint:
            return
        arr = self.samples.get(r)
        if arr is None:
            arr = []
            self.samples[r] = arr
        if len(arr) < 4 and mint not in arr:
            arr.append(mint)


@dataclass
class ScanStats:
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
    rejects: RejectBuckets = field(default_factory=RejectBuckets)


# =============================================================================
# Redis helpers (optional sync/async clients)
# =============================================================================
async def _redis_get(redis: Any, key: str) -> str | None:
    if redis is None:
        return None
    try:
        v = await _maybe_await(redis.get(key))
        if v is None:
            return None
        if isinstance(v, (bytes, bytearray)):
            return v.decode("utf-8", "ignore")
        return str(v)
    except asyncio.CancelledError:
        raise
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
    except asyncio.CancelledError:
        raise
    except Exception:
        return


# =============================================================================
# Regime + SOL price (best effort)
# =============================================================================
async def _regime_snapshot() -> dict[str, Any]:
    try:
        feats = await _call_maybe_async(getattr(regime_state, "features", None))
        if isinstance(feats, dict):
            return {
                "label": str(feats.get("regime_label", "")) or "unknown",
                "label_numeric": float(feats.get("regime_label_numeric", 0.0)),
                "trend_score": float(feats.get("trend_score", 0.0)),
                "volatility_score": float(feats.get("volatility_score", 0.0)),
                "liquidity_score": float(feats.get("liquidity_score", 0.5)),
                "manipulation_risk": float(feats.get("manipulation_risk", 0.0)),
            }
    except asyncio.CancelledError:
        raise
    except Exception:
        pass

    with contextlib.suppress(Exception):
        return {
            "label": str(regime_state.get()),
            "label_numeric": 0.0,
            "manipulation_risk": 0.0,
        }

    return {"label": "unknown", "label_numeric": 0.0, "manipulation_risk": 0.0}


async def _sol_usd() -> float:
    v = _f("SOL_USD", 0.0)
    if v > 0:
        return float(v)

    if _cu is not None:
        for name in ("get_sol_price_usd", "sol_price_usd", "get_sol_price", "get_sol_usd"):
            fn = getattr(_cu, name, None)
            if callable(fn):
                out = await _call_maybe_async(fn)
                with contextlib.suppress(Exception):
                    p = float(out)
                    if p > 0:
                        return p
    return 0.0


def _dynamic_min_combined(base: float, regime: dict[str, Any], risk_capacity: float) -> float:
    """
    Dynamic gate:
      - low risk capacity => raise threshold
      - risk-on regime => slightly lower threshold
      - risk-off or manipulation => raise
    """
    base = float(base)
    ln = float(regime.get("label_numeric", 0.0))
    manip = float(regime.get("manipulation_risk", 0.0))
    cap = _clamp(float(risk_capacity), 0.0, 1.0)

    delta = 0.0
    delta += (1.0 - cap) * 8.0
    if ln >= 0.7:
        delta -= 4.0
    elif ln <= -0.7:
        delta += 4.0
    delta += _clamp(manip, 0.0, 1.0) * 4.0

    return float(_clamp(base + delta, 0.0, 100.0))


# =============================================================================
# PumpPortal WS client (single conn; bounded state)
# =============================================================================
@dataclass(frozen=True)
class Trade:
    ts: float
    side: str  # buy|sell|?
    sol: float
    token: float
    price_sol: float


@dataclass
class TokenState:
    mint: str
    symbol: str = ""
    name: str = ""
    created_ts: float = 0.0
    last_ts: float = 0.0
    mcap_usd: float = 0.0
    liq_usd: float = 0.0
    price_usd: float = 0.0
    trades: deque[Trade] = field(default_factory=deque)

    def push_trade(self, t: Trade, *, max_trades: int) -> None:
        self.trades.append(t)
        while len(self.trades) > max_trades:
            self.trades.popleft()
        self.last_ts = max(self.last_ts, t.ts)


class PumpPortalClient:
    """
    Dedicated background WS loop. Safe to call ensure_started() many times.
    snapshot() returns copies to avoid iterating over mutable state mid-update.

    LOCK-IN: close() cancels task and guarantees ws+session closure.
    """

    def __init__(self) -> None:
        self._start_lock = asyncio.Lock()
        self._state_lock = asyncio.Lock()

        self._task: asyncio.Task[None] | None = None
        self._sess: aiohttp.ClientSession | None = None
        self._ws: aiohttp.ClientWebSocketResponse | None = None
        self._ready = asyncio.Event()
        self._stop = asyncio.Event()

        self._tokens: dict[str, TokenState] = {}
        self._order: deque[str] = deque()
        self._subscribed_trades: set[str] = set()
        self._last_subscribe_ts = 0.0

        self._last_err: str | None = None
        self._last_connect_ts: float = 0.0

    async def ensure_started(self) -> None:
        if _offline_mode():
            return
        async with self._start_lock:
            if self._task and not self._task.done():
                return
            self._stop.clear()
            self._ready.clear()
            self._task = asyncio.create_task(self._run_loop(), name="pumpportal_ws")
        with contextlib.suppress(Exception):
            await asyncio.wait_for(self._ready.wait(), timeout=float(CFG.connect_timeout_s))

    async def close(self) -> None:
        """Full shutdown: stop loop, cancel task, close ws+session, clear state."""
        self._stop.set()

        async with self._start_lock:
            t = self._task
            self._task = None

        if t:
            with contextlib.suppress(Exception):
                t.cancel()
            with contextlib.suppress(Exception):
                await asyncio.wait_for(t, timeout=3.0)

        ws = self._ws
        self._ws = None
        if ws is not None:
            with contextlib.suppress(Exception):
                await ws.close()

        sess = self._sess
        self._sess = None
        if sess is not None:
            with contextlib.suppress(Exception):
                await sess.close()

        self._ready.clear()

        async with self._state_lock:
            self._tokens.clear()
            self._order.clear()
            self._subscribed_trades.clear()

    def stats(self) -> dict[str, Any]:
        ws_ok = bool(self._ws and not self._ws.closed)
        sess_ok = bool(self._sess and not self._sess.closed)
        return {
            "ready": self._ready.is_set(),
            "tokens": len(self._tokens),
            "sub_trades": len(self._subscribed_trades),
            "ws": ws_ok,
            "session": sess_ok,
            "url": CFG.ws_url,
            "last_err": self._last_err,
            "last_connect_ts": self._last_connect_ts,
        }

    async def snapshot(self, *, max_items: int) -> list[TokenState]:
        """
        Return copies of newest token states to avoid races with the WS loop.
        """
        async with self._state_lock:
            mints = list(self._order)[-max_items:]
            out: list[TokenState] = []
            for mint in reversed(mints):
                st = self._tokens.get(mint)
                if st is None:
                    continue
                out.append(
                    TokenState(
                        mint=st.mint,
                        symbol=st.symbol,
                        name=st.name,
                        created_ts=st.created_ts,
                        last_ts=st.last_ts,
                        mcap_usd=st.mcap_usd,
                        liq_usd=st.liq_usd,
                        price_usd=st.price_usd,
                        trades=deque(list(st.trades)),
                    )
                )
            return out

    def _extract_mint(self, msg: dict[str, Any]) -> str:
        for k in ("mint", "mintAddress", "tokenMint", "token", "tokenAddress", "publicKey"):
            v = msg.get(k)
            if isinstance(v, str):
                mm = BASE58_RE.search(v.strip())
                if mm:
                    return mm.group(0)

        for k in ("data", "token", "coin", "meta"):
            v = msg.get(k)
            if isinstance(v, dict):
                for kk in ("mint", "mintAddress", "tokenMint", "tokenAddress", "publicKey"):
                    vv = v.get(kk)
                    if isinstance(vv, str):
                        mm = BASE58_RE.search(vv.strip())
                        if mm:
                            return mm.group(0)
        return ""

    async def _touch(
        self,
        mint: str,
        *,
        symbol: str = "",
        name: str = "",
        created_ts: float = 0.0,
    ) -> TokenState:
        async with self._state_lock:
            st = self._tokens.get(mint)
            if st is None:
                st = TokenState(mint=mint)
                self._tokens[mint] = st
                self._order.append(mint)

                while len(self._order) > CFG.max_tokens:
                    old = self._order.popleft()
                    self._tokens.pop(old, None)
                    self._subscribed_trades.discard(old)
            else:
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

    async def _ws_send(self, payload: dict[str, Any]) -> None:
        ws = self._ws
        if ws is None or ws.closed:
            return
        with contextlib.suppress(Exception):
            await ws.send_json(payload)

    async def _subscribe_basics(self) -> None:
        await self._ws_send({"method": "subscribeNewToken"})
        if CFG.sub_migrations:
            await self._ws_send({"method": "subscribeMigration"})

    async def _refresh_trade_subs(self) -> None:
        now = time.time()
        if (now - self._last_subscribe_ts) < float(CFG.subscribe_refresh_s):
            return
        self._last_subscribe_ts = now

        async with self._state_lock:
            newest = list(self._order)[-CFG.subscribe_trade_max :]
            keys = [m for m in newest if m and m not in self._subscribed_trades]

        if not keys:
            return

        await self._ws_send({"method": "subscribeTokenTrade", "keys": keys})

        async with self._state_lock:
            self._subscribed_trades.update(keys)

    def _is_trade(self, msg: dict[str, Any]) -> bool:
        tx = msg.get("txType") or msg.get("side") or msg.get("type")
        if isinstance(tx, str) and tx.lower() in {"buy", "sell"}:
            return True
        return any(k in msg for k in ("solAmount", "sol_amount", "tokenAmount", "token_amount"))

    def _parse_trade(self, msg: dict[str, Any]) -> Trade:
        side = str(msg.get("txType") or msg.get("side") or msg.get("type") or "?").lower()
        if side not in {"buy", "sell"}:
            side = "?"
        ts = _nz(
            msg.get("timestamp") or msg.get("ts") or msg.get("time") or time.time(), time.time()
        )
        if ts > 10_000_000_000:
            ts = ts / 1000.0

        sol = _nz(msg.get("solAmount") or msg.get("sol_amount") or msg.get("sol"), 0.0)
        token = _nz(msg.get("tokenAmount") or msg.get("token_amount") or msg.get("token"), 0.0)
        price_sol = (sol / token) if (sol > 0 and token > 0) else 0.0
        return Trade(
            ts=float(ts), side=side, sol=float(sol), token=float(token), price_sol=float(price_sol)
        )

    def _apply_fundamentals(self, st: TokenState, msg: dict[str, Any], sol_usd: float) -> None:
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

        liq_usd = (
            msg.get("liquidityUsd")
            or msg.get("liquidityUSD")
            or msg.get("liqUsd")
            or msg.get("liq_usd")
        )
        if liq_usd is None:
            liq_sol = (
                msg.get("vSolInBondingCurve")
                or msg.get("solInBondingCurve")
                or msg.get("liquiditySol")
            )
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

    async def _ensure_session(self) -> aiohttp.ClientSession:
        if self._sess is not None and not self._sess.closed:
            return self._sess
        conn = aiohttp.TCPConnector(limit=25, ssl=_sslctx())
        self._sess = aiohttp.ClientSession(
            headers={"User-Agent": CFG.user_agent},
            trust_env=True,
            connector=conn,
        )
        return self._sess

    async def _run_loop(self) -> None:
        backoff = 0.8
        sol_usd_cache = 0.0
        sol_usd_ts = 0.0

        while not self._stop.is_set():
            ws: aiohttp.ClientWebSocketResponse | None = None
            try:
                if _offline_mode():
                    await asyncio.sleep(1.0)
                    continue

                sess = await self._ensure_session()
                self._last_err = None
                self._last_connect_ts = time.time()

                ws = await sess.ws_connect(
                    CFG.ws_url,
                    heartbeat=float(CFG.heartbeat_s),
                    ssl=_sslctx(),
                    max_msg_size=int(CFG.max_msg_size_bytes),
                    timeout=float(CFG.connect_timeout_s),
                )
                self._ws = ws
                backoff = 0.8

                await self._subscribe_basics()
                self._ready.set()
                LOG.every("PUMPPORTAL_WS_CONNECTED", n=10, url=CFG.ws_url)

                while not self._stop.is_set():
                    now = time.time()
                    if (now - sol_usd_ts) > 20:
                        sol_usd_cache = await _sol_usd()
                        sol_usd_ts = now

                    await self._refresh_trade_subs()

                    try:
                        msg = await ws.receive(timeout=float(CFG.receive_timeout_s))
                    except asyncio.TimeoutError:
                        continue

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

                    created = (
                        data.get("createdAt")
                        or data.get("created")
                        or data.get("createdOn")
                        or data.get("createdTimestamp")
                    )
                    created_ts = 0.0
                    if created is not None:
                        created_ts = _nz(created, 0.0)
                        if created_ts > 10_000_000_000:
                            created_ts = created_ts / 1000.0

                    st = await self._touch(
                        mint, symbol=symbol, name=name, created_ts=float(created_ts)
                    )
                    self._apply_fundamentals(st, data, sol_usd_cache)

                    if self._is_trade(data):
                        t = self._parse_trade(data)
                        async with self._state_lock:
                            st.push_trade(t, max_trades=int(CFG.max_trades_per_token))

            except asyncio.CancelledError:
                raise
            except Exception as e:
                self._ready.clear()
                self._last_err = f"{type(e).__name__}:{_short(e)}"
                LOG.every(
                    "PUMPPORTAL_WS_RECONNECT", n=5, err=_short(type(e).__name__), detail=_short(e)
                )
                await asyncio.sleep(backoff * (0.85 + 0.3 * random.random()))
                backoff = min(25.0, backoff * 1.6)
            finally:
                self._ready.clear()
                if ws is not None:
                    with contextlib.suppress(Exception):
                        await ws.close()
                if self._ws is ws:
                    self._ws = None

        self._ready.clear()
        self._ws = None


_PUMPPORTAL: PumpPortalClient | None = None


async def _pumpportal_client() -> PumpPortalClient:
    global _PUMPPORTAL
    if _PUMPPORTAL is None:
        _PUMPPORTAL = PumpPortalClient()
    await _PUMPPORTAL.ensure_started()
    return _PUMPPORTAL


def pumpfun_scanner_health() -> dict[str, Any]:
    return {"pumpportal": (_PUMPPORTAL.stats() if _PUMPPORTAL else {"ready": False, "tokens": 0})}


# =============================================================================
# Metrics + candidate normalization
# =============================================================================
class ExtractedMetrics(TypedDict):
    liq_usd: float
    mcap_usd: float
    token_age_min: float
    vol_5m_usd: float
    vol_1h_usd: float
    vol_24h_usd: float
    buys_5m: int
    sells_5m: int
    txns_5m: int
    price_usd: float
    price_change_5m_pct: float
    price_change_1h_pct: float
    price_change_24h_pct: float
    impact_1k_bps: float
    whales_5m: int
    largest_buy_5m_sol: float
    buy_ratio_5m: float
    vol_to_liq_5m: float
    momo_hint: float


def _age_min(ts: float | None) -> float | None:
    try:
        if ts is None or ts <= 0:
            return None
        t = float(ts)
        if t > 10_000_000_000:
            t = t / 1000.0
        return max(0.0, (time.time() - t) / 60.0)
    except Exception:
        return None


def _buy_ratio(buys: int, sells: int) -> float:
    total = max(1, buys + sells)
    return float(buys) / float(total)


def _trade_window_metrics(
    trades: Iterable[Trade], *, now: float, wsec: float, sol_usd: float
) -> dict[str, Any]:
    buys = 0
    sells = 0
    vol_sol = 0.0
    prices: list[float] = []
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


def _state_to_obj(
    st: TokenState, *, sol_usd: float, now: float, whale_min_sol: float
) -> dict[str, Any]:
    m5 = _trade_window_metrics(st.trades, now=now, wsec=300.0, sol_usd=sol_usd)
    m1h = _trade_window_metrics(st.trades, now=now, wsec=3600.0, sol_usd=sol_usd)

    created_ts = st.created_ts if st.created_ts > 0 else (st.last_ts if st.last_ts > 0 else 0.0)

    largest_buy_sol = 0.0
    for t in st.trades:
        if (now - t.ts) > 300.0:
            continue
        if t.side == "buy":
            largest_buy_sol = max(largest_buy_sol, t.sol)

    whales_5m = 1 if largest_buy_sol >= float(whale_min_sol) else 0

    liq_usd = float(st.liq_usd or 0.0)
    mcap_usd = float(st.mcap_usd or 0.0)
    liq_unknown = liq_usd <= 0.0
    mcap_unknown = mcap_usd <= 0.0

    price_usd = float(st.price_usd or 0.0)
    if price_usd <= 0 and float(m5["price_usd"]) > 0:
        price_usd = float(m5["price_usd"])

    buy_ratio_5m = _buy_ratio(int(m5["buys"]), int(m5["sells"]))
    vol_to_liq = (float(m5["vol_usd"]) / max(1.0, liq_usd)) if (liq_usd > 0) else 0.0
    momo_hint = (
        (buy_ratio_5m * 100.0)
        + (min(2.0, vol_to_liq) * 25.0)
        + (max(-20.0, min(20.0, float(m5["chg_pct"]))) * 0.8)
    )

    return {
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
        "liq_unknown": bool(liq_unknown),
        "mcap_unknown": bool(mcap_unknown),
        "largest_buy_5m_sol": float(largest_buy_sol),
        "whales_5m": int(whales_5m),
        "momo_hint": float(momo_hint),
    }


def _extract_metrics(obj: dict[str, Any]) -> ExtractedMetrics:
    liq_usd = _nz(obj.get("liquidityUsd") or obj.get("liqUsd") or obj.get("liq_usd") or 0.0, 0.0)
    mcap_usd = _nz(
        obj.get("marketCapUsd") or obj.get("mcap_usd") or obj.get("marketCap") or 0.0, 0.0
    )

    buys_5m = _safe_int(obj.get("buys5m") or obj.get("buys_5m") or 0)
    sells_5m = _safe_int(obj.get("sells5m") or obj.get("sells_5m") or 0)

    vol_5m_usd = _nz(obj.get("volumeUsd5m") or obj.get("vol5mUsd") or 0.0, 0.0)
    vol_1h_usd = _nz(obj.get("volumeUsd1h") or 0.0, 0.0)
    vol_24h_usd = _nz(obj.get("volumeUsd24h") or 0.0, 0.0)

    chg_5m = _nz(obj.get("priceChange5m") or 0.0, 0.0)
    chg_1h = _nz(obj.get("priceChange1h") or 0.0, 0.0)
    chg_24h = _nz(obj.get("priceChange24h") or 0.0, 0.0)

    age = _age_min(obj.get("createdAt") or obj.get("created") or None)
    token_age_min = float(age) if age is not None else 9999.0

    price_usd = _nz(obj.get("priceUsd") or obj.get("usdPrice") or 0.0, 0.0)
    impact_1k_bps = _nz(obj.get("priceImpact1kBps") or obj.get("impact_1k_bps") or 0.0, 0.0)

    whales_5m = _safe_int(obj.get("whales_5m") or 0)
    largest_buy_5m_sol = _nz(obj.get("largest_buy_5m_sol") or 0.0, 0.0)

    buy_ratio_5m = _buy_ratio(buys_5m, sells_5m)
    vol_to_liq = (vol_5m_usd / max(1.0, liq_usd)) if liq_usd > 0 else 0.0
    momo_hint = _nz(obj.get("momo_hint") or 0.0, 0.0)
    if momo_hint == 0.0:
        momo_hint = (
            (buy_ratio_5m * 100.0)
            + (min(2.0, vol_to_liq) * 25.0)
            + (max(-20.0, min(20.0, chg_5m)) * 0.8)
        )

    return {
        "liq_usd": float(max(0.0, liq_usd)),
        "mcap_usd": float(max(0.0, mcap_usd)),
        "token_age_min": float(max(0.0, token_age_min)),
        "vol_5m_usd": float(max(0.0, vol_5m_usd)),
        "vol_1h_usd": float(max(0.0, vol_1h_usd)),
        "vol_24h_usd": float(max(0.0, vol_24h_usd)),
        "buys_5m": int(max(0, buys_5m)),
        "sells_5m": int(max(0, sells_5m)),
        "txns_5m": int(max(0, buys_5m + sells_5m)),
        "price_usd": float(max(0.0, price_usd)),
        "price_change_5m_pct": float(chg_5m),
        "price_change_1h_pct": float(chg_1h),
        "price_change_24h_pct": float(chg_24h),
        "impact_1k_bps": float(max(0.0, impact_1k_bps)),
        "whales_5m": int(max(0, whales_5m)),
        "largest_buy_5m_sol": float(max(0.0, largest_buy_5m_sol)),
        "buy_ratio_5m": float(_clamp(buy_ratio_5m, 0.0, 1.0)),
        "vol_to_liq_5m": float(max(0.0, vol_to_liq)),
        "momo_hint": float(momo_hint),
    }


def _map_obj_to_candidate(
    obj: dict[str, Any],
    *,
    regime: dict[str, Any],
    stats: ScanStats,
    source: str,
) -> dict[str, Any] | None:
    raw_mint = (
        obj.get("mintAddress")
        or obj.get("mint")
        or obj.get("tokenMint")
        or obj.get("publicKey")
        or ""
    )
    mint = ""

    if isinstance(raw_mint, str) and raw_mint:
        if _normalize_mint is not None:
            with contextlib.suppress(Exception):
                mint = str(_normalize_mint(raw_mint))
        if not mint:
            mm = BASE58_RE.search(raw_mint.strip())
            mint = mm.group(0) if mm else ""

    if not mint:
        stats.rejects.inc("REJ_NO_MINT")
        return None

    symbol = (obj.get("symbol") or obj.get("tokenSymbol") or obj.get("name") or mint[:6]) or mint[
        :6
    ]
    metrics = _extract_metrics(obj)

    liq_unknown = bool(obj.get("liq_unknown", False))
    mcap_unknown = bool(obj.get("mcap_unknown", False))

    # soft filters: do not kill unknown metrics
    if CFG.soft_min_liq_usd > 0 and (not liq_unknown) and metrics["liq_usd"] < CFG.soft_min_liq_usd:
        stats.rejects.inc("REJ_SOFT_LIQ", mint)
        return None
    if (
        CFG.soft_min_mcap_usd > 0
        and (not mcap_unknown)
        and metrics["mcap_usd"] < CFG.soft_min_mcap_usd
    ):
        stats.rejects.inc("REJ_SOFT_MCAP", mint)
        return None
    if CFG.soft_max_age_min > 0 and metrics["token_age_min"] > CFG.soft_max_age_min:
        stats.rejects.inc("REJ_SOFT_AGE", mint)
        return None
    if CFG.hard_max_age_min > 0 and metrics["token_age_min"] > float(CFG.hard_max_age_min):
        stats.rejects.inc("REJ_HARD_AGE", mint)
        return None

    return {
        "candidate_schema": CANDIDATE_SCHEMA,
        "scanner": SCANNER_NAME,
        "scanner_version": SCANNER_VERSION,
        "source": source,
        "dex": "pumpfun",
        "chain": "SOL",
        "mint": mint,
        "symbol": str(symbol),
        "pair_url": f"https://pump.fun/coin/{mint}",
        "scan_ts": time.time(),
        "regime": str(regime.get("label", "unknown") or "unknown"),
        "regime_numeric": float(regime.get("label_numeric", 0.0)),
        "regime_manipulation": float(regime.get("manipulation_risk", 0.0)),
        "regime_ctx": dict(regime),
        **metrics,
        "liq_unknown": bool(liq_unknown),
        "mcap_unknown": bool(mcap_unknown),
        # optional enrichers
        "ai_rug_score": 0.0,
        # scoring placeholders
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
        # legacy aliases
        "age_min": metrics["token_age_min"],
        "chg5": metrics["price_change_5m_pct"],
        "buys5": metrics["buys_5m"],
        "sells5": metrics["sells_5m"],
        "vol5": metrics["vol_5m_usd"],
        "liq": metrics["liq_usd"],
    }


def _dedupe_best_by_mint(metas: list[dict[str, Any]]) -> list[dict[str, Any]]:
    best: dict[str, dict[str, Any]] = {}
    for meta in metas:
        mint = meta.get("mint")
        if not isinstance(mint, str) or not mint:
            continue
        cur = best.get(mint)
        if cur is None:
            best[mint] = meta
            continue
        a = (
            float(meta.get("vol_5m_usd") or 0.0),
            int(meta.get("buys_5m") or 0),
            float(meta.get("liq_usd") or 0.0),
        )
        b = (
            float(cur.get("vol_5m_usd") or 0.0),
            int(cur.get("buys_5m") or 0),
            float(cur.get("liq_usd") or 0.0),
        )
        if a > b:
            best[mint] = meta
    return list(best.values())


# =============================================================================
# Optional enrichment (AI rug + smart wallets)
# =============================================================================
async def _enrich_optional(metas: list[dict[str, Any]], stats: ScanStats) -> None:
    if not metas:
        return

    sem = asyncio.Semaphore(max(1, _i("PUMPFUN_ENRICH_CONCURRENCY", 8)))

    rug_targets = metas[: CFG.ai_rug_max_per_scan] if CFG.ai_rug_enabled else []
    rug_mints = {str(m.get("mint") or "") for m in rug_targets if str(m.get("mint") or "")}

    sw_tracker = None
    if CFG.smart_wallets_enrich and SW is not None:
        sw_tracker = getattr(SW, "TRACKER", None) or getattr(SW, "tracker", None) or None
    sw_targets = metas[: CFG.smart_wallets_max_per_scan] if sw_tracker is not None else []
    sw_mints = {str(m.get("mint") or "") for m in sw_targets if str(m.get("mint") or "")}

    async def _one(meta: dict[str, Any]) -> None:
        mint = str(meta.get("mint") or "")
        if not mint:
            return
        async with sem:
            if mint in rug_mints and ai_fraud_score is not None:
                # Prefer snapshot-style API, fallback to mint-style API
                s01 = await _call_maybe_async(ai_fraud_score, dict(meta))
                if s01 is None:
                    s01 = await _call_maybe_async(ai_fraud_score, mint)
                if s01 is not None:
                    meta["ai_rug_score"] = _clamp(float(s01), 0.0, 1.0)

            if mint in sw_mints and sw_tracker is not None:
                bf = getattr(sw_tracker, "build_features", None)
                if callable(bf):
                    feats = await _call_maybe_async(bf, mint)
                    if isinstance(feats, dict):
                        meta["smart_wallet_features"] = feats
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
                                meta[k] = feats.get(k)

    tasks = [asyncio.create_task(_one(m)) for m in metas]
    await _gather_no_cancel(tasks)


# =============================================================================
# Scoring service (preferred)
# =============================================================================
async def _score_with_service(
    metas: list[dict[str, Any]],
    *,
    regime: dict[str, Any],
    risk_ctx: dict[str, Any] | None,
    stats: ScanStats,
) -> list[dict[str, Any]]:
    if not CFG.scoring_enabled or not metas or score_candidate is None:
        return []

    pre: list[dict[str, Any]] = []
    for meta in metas:
        mint = str(meta.get("mint") or "")
        try:
            if CFG.ai_rug_enabled and float(meta.get("ai_rug_score") or 0.0) >= float(
                CFG.max_ai_rug
            ):
                stats.rejects.inc("REJ_AI_RUG", mint)
                continue
            if not is_viable_pair(meta):
                stats.rejects.inc("REJ_LEGACY_VIABLE_FALSE", mint)
                continue
            pre.append(meta)
        except Exception:
            pre.append(meta)

    pre.sort(
        key=lambda x: (float(x.get("vol_5m_usd") or 0.0), int(x.get("buys_5m") or 0)), reverse=True
    )
    targets = pre[: CFG.scoring_max_per_scan]

    sem = asyncio.Semaphore(int(CFG.scoring_concurrency))
    out: list[dict[str, Any]] = []

    def _get(obj: Any, k: str, default: Any = None) -> Any:
        if isinstance(obj, dict):
            return obj.get(k, default)
        return getattr(obj, k, default)

    async def _one(meta: dict[str, Any]) -> None:
        mint = str(meta.get("mint") or "")
        ctx = {
            "source": str(meta.get("source") or "pumpfun"),
            "scanner": SCANNER_NAME,
            "phase": CFG.phase,
            "mode": CFG.mode,
            "regime": dict(regime),
            "risk": dict(risk_ctx or {}),
        }

        async with sem:
            try:
                try:
                    st = await _call_maybe_async(score_candidate, dict(meta), context=ctx)
                except TypeError:
                    st = await _call_maybe_async(score_candidate, dict(meta))
                if st is None:
                    stats.rejects.inc("REJ_SCORE_NONE", mint)
                    return
            except asyncio.CancelledError:
                raise
            except Exception as e:
                stats.rejects.inc("REJ_SCORE_EXCEPTION", mint)
                LOG.every("PUMP_SCORE_FAIL", n=10, mint=mint, err=_short(e))
                return

        try:
            m2 = dict(meta)
            m2["score_combined"] = float(
                _get(st, "combined_score", _get(st, "score_combined", 0.0)) or 0.0
            )
            m2["score_label"] = str(_get(st, "label", _get(st, "score_label", "")) or "")
            m2["score_momentum"] = float(
                _get(st, "momentum_score", _get(st, "score_momentum", 0.0)) or 0.0
            )
            m2["score_risk"] = float(_get(st, "risk_score", _get(st, "score_risk", 0.0)) or 0.0)
            m2["score_quality"] = float(
                _get(st, "quality_score", _get(st, "score_quality", 0.0)) or 0.0
            )
            m2["score_ai"] = float(_get(st, "ai_score", _get(st, "score_ai", 0.0)) or 0.0)
            m2["score_reasons"] = _get(st, "reasons", _get(st, "score_reasons", None))
            m2["score_is_trade_viable"] = bool(_get(st, "is_trade_viable", True))
            m2["score_trade_gate_reason"] = _get(st, "trade_gate_reason", None)
            m2["rank_score"] = float(m2["score_combined"])
            out.append(m2)
        except Exception:
            stats.rejects.inc("REJ_SCORE_MAP_FAIL", mint)

    tasks = [asyncio.create_task(_one(m)) for m in targets]
    await _gather_no_cancel(tasks)

    return out


def _select_from_scored(
    scored: list[dict[str, Any]], *, min_combined: float, stats: ScanStats
) -> list[dict[str, Any]]:
    if not scored:
        return []
    runners: list[dict[str, Any]] = []

    for meta in scored:
        mint = str(meta.get("mint") or "")
        try:
            label = str(meta.get("score_label") or "").upper() or "YELLOW"
            combined = float(meta.get("score_combined") or 0.0)
            trade_ok = bool(meta.get("score_is_trade_viable", True))

            if not trade_ok:
                stats.rejects.inc("REJ_TRADE_NOT_VIABLE", mint)
                continue
            if combined < float(min_combined):
                stats.rejects.inc("REJ_SCORE_TOO_LOW", mint)
                continue
            if _LABEL_LEVEL.get(label, 0) < _REQUIRED_LABEL_LEVEL:
                stats.rejects.inc("REJ_LABEL_TOO_LOW", mint)
                continue
            if CFG.ai_rug_enabled and float(meta.get("ai_rug_score") or 0.0) >= float(
                CFG.max_ai_rug
            ):
                stats.rejects.inc("REJ_AI_RUG", mint)
                continue

            runners.append(meta)
        except Exception:
            stats.rejects.inc("REJ_SELECT_EXCEPTION", mint)

    runners.sort(key=lambda x: float(x.get("score_combined") or 0.0), reverse=True)
    return runners[: CFG.max_runners]


# =============================================================================
# Legacy + heuristic fallbacks
# =============================================================================
def _legacy_rank(metas: list[dict[str, Any]], stats: ScanStats) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for meta in metas:
        mint = str(meta.get("mint") or "")
        try:
            if CFG.ai_rug_enabled and float(meta.get("ai_rug_score") or 0.0) >= float(
                CFG.max_ai_rug
            ):
                stats.rejects.inc("REJ_AI_RUG", mint)
                continue
            if not is_viable_pair(meta):
                stats.rejects.inc("REJ_LEGACY_VIABLE_FALSE", mint)
                continue

            momentum = float(compute_momentum_score(meta))
            final = float(compute_final_score(meta))
            vol5 = float(meta.get("vol_5m_usd") or 0.0)
            whales = int(meta.get("whales_5m") or 0)

            rank = final + 0.2 * momentum + (0.15 if whales > 0 else 0.0)
            rank *= max(1.0, log1p(max(vol5, 1.0)) / 5.0)

            m2 = dict(meta)
            m2["final_score"] = final
            m2["momentum_score"] = momentum
            m2["rank_score"] = float(rank)
            out.append(m2)
        except Exception:
            stats.rejects.inc("REJ_LEGACY_EXCEPTION", mint)

    out.sort(key=lambda x: float(x.get("rank_score") or 0.0), reverse=True)
    return out[: CFG.max_runners]


def _fallback_pick(metas: list[dict[str, Any]], stats: ScanStats) -> list[dict[str, Any]]:
    cands: list[dict[str, Any]] = []
    for meta in metas:
        mint = str(meta.get("mint") or "")
        try:
            buys5 = int(meta.get("buys_5m") or 0)
            vol5 = float(meta.get("vol_5m_usd") or 0.0)
            impact = float(meta.get("impact_1k_bps") or 0.0)
            rug = float(meta.get("ai_rug_score") or 0.0)

            if buys5 < CFG.fb_min_buys_5m:
                stats.rejects.inc("REJ_FB_MIN_BUYS", mint)
                continue
            if vol5 < CFG.fb_min_vol5_usd:
                stats.rejects.inc("REJ_FB_MIN_VOL5", mint)
                continue
            if CFG.fb_max_impact_1k_bps > 0 and impact > CFG.fb_max_impact_1k_bps:
                stats.rejects.inc("REJ_FB_IMPACT", mint)
                continue
            if CFG.ai_rug_enabled and rug >= float(CFG.max_ai_rug):
                stats.rejects.inc("REJ_AI_RUG", mint)
                continue

            m2 = dict(meta)
            m2.setdefault("rank_score", float(m2.get("momo_hint") or m2.get("vol_5m_usd") or 0.0))
            cands.append(m2)
        except Exception:
            stats.rejects.inc("REJ_FB_EXCEPTION", mint)

    cands.sort(
        key=lambda x: (
            float(x.get("momo_hint") or 0.0),
            float(x.get("vol_5m_usd") or 0.0),
            float(x.get("liq_usd") or 0.0),
        ),
        reverse=True,
    )
    return cands[: CFG.fb_max_runners]


# =============================================================================
# Provider collectors
# =============================================================================
async def _collect_from_pumpportal(
    *, regime: dict[str, Any], stats: ScanStats
) -> tuple[list[dict[str, Any]], str]:
    if _offline_mode():
        stats.rejects.inc("REJ_OFFLINE_MODE")
        return [], "pumpportal:offline"

    client = await _pumpportal_client()
    st = client.stats()
    if not st.get("ready", False):
        stats.rejects.inc("REJ_PUMPPORTAL_NOT_READY")
        return [], "pumpportal:not_ready"

    now = time.time()
    sol_usd = await _sol_usd()
    whale_min_sol = float(CFG.whale_min_sol)

    states = await client.snapshot(max_items=int(CFG.snap_max))
    objs = [_state_to_obj(s, sol_usd=sol_usd, now=now, whale_min_sol=whale_min_sol) for s in states]

    metas: list[dict[str, Any]] = []
    for obj in objs:
        mm = _map_obj_to_candidate(obj, regime=regime, stats=stats, source="pumpfun_ws")
        if mm:
            metas.append(mm)

    metas = _dedupe_best_by_mint(metas)
    metas.sort(
        key=lambda x: (float(x.get("vol_5m_usd") or 0.0), int(x.get("buys_5m") or 0)), reverse=True
    )
    return metas, "pumpportal"


async def _collect_from_http(
    session: aiohttp.ClientSession,
    *,
    regime: dict[str, Any],
    stats: ScanStats,
) -> tuple[list[dict[str, Any]], str]:
    if _offline_mode():
        stats.rejects.inc("REJ_OFFLINE_MODE")
        return [], "http:offline"
    if not CFG.http_base_urls:
        stats.rejects.inc("REJ_HTTP_DISABLED")
        return [], "http:disabled"

    timeout = aiohttp.ClientTimeout(total=float(CFG.http_timeout_s))
    metas: list[dict[str, Any]] = []

    async def _try_one(base: str) -> None:
        nonlocal metas
        url = base.rstrip("/") + "/candidates"
        for attempt in range(int(CFG.http_retries)):
            try:
                async with session.get(url, timeout=timeout, ssl=_sslctx()) as r:
                    if r.status >= 400:
                        raise RuntimeError(f"http {r.status}")
                    data = await r.json()
                    if not isinstance(data, list):
                        raise RuntimeError("http_invalid_shape")
                    out: list[dict[str, Any]] = []
                    for obj in data:
                        if not isinstance(obj, dict):
                            continue
                        mm = _map_obj_to_candidate(
                            obj, regime=regime, stats=stats, source="pumpfun_http"
                        )
                        if mm:
                            out.append(mm)
                    metas = out
                    return
            except asyncio.CancelledError:
                raise
            except Exception as e:
                if attempt + 1 >= int(CFG.http_retries):
                    LOG.every("PUMPFUN_HTTP_FAIL", n=10, base=base, err=_short(e))
                    return
                await asyncio.sleep(float(CFG.http_backoff_s) * (1.1 + 0.4 * random.random()))

    for base in CFG.http_base_urls:
        await _try_one(base)
        if metas:
            break

    metas = _dedupe_best_by_mint(metas)
    return metas, ("http" if metas else "http:empty")


# =============================================================================
# Alerts + training logs
# =============================================================================
async def _send_momo(runners: list[dict[str, Any]]) -> None:
    if not CFG.send_momo_alerts or not runners or alert_manager_cls is None:
        return
    try:
        am = alert_manager_cls()
    except Exception as e:
        LOG.every("PUMP_ALERT_INIT_FAIL", n=30, err=_short(e))
        return

    max_n = max(0, min(int(CFG.max_alerts_per_scan), len(runners)))

    lock = _alert_lock()
    async with lock:
        now = time.time()
        cutoff = now - float(CFG.alert_dedupe_sec)
        for k in list(_recent_alert_ts.keys()):
            if _recent_alert_ts.get(k, 0.0) < cutoff:
                _recent_alert_ts.pop(k, None)

    for c in runners[:max_n]:
        mint = str(c.get("mint") or "")
        if not mint:
            continue

        async with lock:
            now = time.time()
            last = _recent_alert_ts.get(mint, 0.0)
            if now - last < float(CFG.alert_dedupe_sec):
                continue
            _recent_alert_ts[mint] = now

        try:
            ctx: dict[str, Any] = {
                "symbol": c.get("symbol") or mint,
                "mint": mint,
                "pool": "pumpfun",
                "pool_url": c.get("pair_url"),
                "liq_usd": c.get("liq_usd"),
                "vol_5m": c.get("vol_5m_usd"),
                "buys5": c.get("buys_5m"),
                "up_pct": c.get("price_change_5m_pct"),
                "ai_rug_score": c.get("ai_rug_score"),
                "rank_score": c.get("rank_score"),
                "score_combined": c.get("score_combined"),
                "score_label": c.get("score_label"),
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
            LOG.every("PUMP_MOMO_FAIL", n=10, mint=mint, err=_short(e))


def _log_candidates_for_training(runners: list[dict[str, Any]], regime: dict[str, Any]) -> None:
    if not runners or not data_logger:
        return
    log_pf = getattr(data_logger, "log_pumpfun_candidate", None)
    log_entry = getattr(data_logger, "log_entry_candidate", None)
    for r in runners:
        try:
            mint = str(r.get("mint") or "").strip()
            if not mint:
                continue
            symbol = (r.get("symbol") or "").strip() or None
            snapshot = dict(r)
            if callable(log_pf):
                log_pf(
                    mint=mint,
                    symbol=symbol,
                    meta=snapshot,
                    regime=str(regime.get("label") or "unknown"),
                )  # type: ignore[misc]
            elif callable(log_entry):
                log_entry(
                    src="pumpfun",
                    mint=mint,
                    symbol=symbol,
                    snapshot=snapshot,
                    regime=str(regime.get("label") or "unknown"),
                )  # type: ignore[misc]
        except Exception:
            continue


# =============================================================================
# Public entrypoint
# =============================================================================
async def scan_pumpfun(*args: Any, **kwargs: Any) -> list[dict[str, Any]]:
    """
    Backwards compatible call patterns:
      - scan_pumpfun()
      - scan_pumpfun(redis)
      - scan_pumpfun(redis, session)
      - scan_pumpfun(redis=..., session=...)

    Returns: runners (list of candidate dicts)
    """
    if not CFG.enabled or CFG.mode == "off":
        return []
    if _offline_mode():
        return []

    redis = kwargs.get("redis")
    session = kwargs.get("session")

    if len(args) >= 1 and redis is None and not isinstance(args[0], aiohttp.ClientSession):
        redis = args[0]
    if len(args) >= 1 and session is None and isinstance(args[0], aiohttp.ClientSession):
        session = args[0]
    if len(args) >= 2 and session is None and isinstance(args[1], aiohttp.ClientSession):
        session = args[1]

    stats = ScanStats(t0=time.time())

    # Risk snapshot
    try:
        risk_ctx = await _call_maybe_async(risk_capacity_snapshot, redis)
        if not isinstance(risk_ctx, dict):
            risk_ctx = None
    except asyncio.CancelledError:
        raise
    except Exception:
        risk_ctx = None

    risk_capacity = _clamp(float((risk_ctx or {}).get("risk_capacity", 1.0) or 1.0), 0.0, 1.0)
    risk_shutdown = bool((risk_ctx or {}).get("risk_shutdown", False))
    risk_mode = str((risk_ctx or {}).get("risk_mode", "normal") or "normal")

    stats.risk_capacity, stats.risk_mode = risk_capacity, risk_mode
    if risk_shutdown or risk_capacity <= 0.0:
        stats.rejects.inc("REJ_RISK_SHUTDOWN")
        LOG.every(
            "PUMP_RISK_SHUTDOWN",
            n=10,
            risk_shutdown=int(risk_shutdown),
            risk_capacity=round(risk_capacity, 3),
            risk_mode=risk_mode,
        )
        return []

    regime = await _regime_snapshot()
    stats.regime_label = str(regime.get("label", "unknown") or "unknown")
    stats.regime_numeric = float(regime.get("label_numeric", 0.0))
    stats.manip_risk = float(regime.get("manipulation_risk", 0.0))

    min_combined = _dynamic_min_combined(CFG.scoring_min_combined, regime, risk_capacity)

    LOG.every(
        "PUMP_SCAN_START",
        n=5,
        phase=CFG.phase,
        mode=CFG.mode,
        provider=CFG.provider,
        ws_url=CFG.ws_url if CFG.provider in {"auto", "pumpportal"} else "",
        soft_liq=int(CFG.soft_min_liq_usd),
        soft_mcap=int(CFG.soft_min_mcap_usd),
        soft_max_age=round(CFG.soft_max_age_min, 2),
        min_combined=round(min_combined, 2),
        min_label=CFG.scoring_min_label,
        scoring=int(bool(CFG.scoring_enabled)),
        regime=stats.regime_label,
        manip=round(stats.manip_risk, 3),
        risk_capacity=round(risk_capacity, 3),
        risk_mode=risk_mode,
        http_fallback=int(bool(CFG.http_base_urls)),
    )

    owns_session = False

    try:
        metas: list[dict[str, Any]] = []
        source = "empty"

        if CFG.provider in {"auto", "pumpportal"}:
            metas, source = await _collect_from_pumpportal(regime=regime, stats=stats)

        if (not metas) and (CFG.provider in {"auto", "http"}) and CFG.http_base_urls:
            if session is None:
                conn = aiohttp.TCPConnector(limit=int(CFG.http_conn_limit), ssl=_sslctx())
                session = aiohttp.ClientSession(
                    headers={"User-Agent": CFG.user_agent},
                    trust_env=CFG.http_trust_env,
                    connector=conn,
                )
                owns_session = True

            mm, src = await _collect_from_http(session, regime=regime, stats=stats)
            if mm:
                metas, source = mm, src

        metas = _dedupe_best_by_mint(metas)
        stats.source = source
        stats.raw = len(metas)
        stats.metas = len(metas)

        if CFG.debug_meta_sample:
            sample = [
                {
                    "sym": m.get("symbol"),
                    "liq": m.get("liq_usd"),
                    "mcap": m.get("mcap_usd"),
                    "vol5": m.get("vol_5m_usd"),
                    "buys5": m.get("buys_5m"),
                    "chg5": m.get("price_change_5m_pct"),
                    "age": m.get("token_age_min"),
                    "unknown": int(bool(m.get("liq_unknown") or m.get("mcap_unknown"))),
                }
                for m in metas[:5]
            ]
            LOG.every("PUMP_META_SAMPLE", n=10, source=source, count=len(metas), first=sample)

        if not metas:
            return []

        await _enrich_optional(metas, stats)
        stats.enriched = len(metas)

        runners: list[dict[str, Any]] = []

        if CFG.scoring_enabled:
            scored = await _score_with_service(metas, regime=regime, risk_ctx=risk_ctx, stats=stats)
            stats.scored = len(scored)
            runners = _select_from_scored(scored, min_combined=min_combined, stats=stats)

        if not runners:
            runners = _legacy_rank(metas, stats)

        if not runners:
            runners = _fallback_pick(metas, stats)
            stats.fb_runners = len(runners)

        if not runners:
            return []

        runners = _dedupe_best_by_mint(runners)
        runners.sort(
            key=lambda x: float(
                x.get("rank_score") or x.get("score_combined") or x.get("momo_hint") or 0.0
            ),
            reverse=True,
        )
        runners = runners[: CFG.max_runners]

        # Redis dedupe
        if (
            redis is not None
            and CFG.runner_dedupe_enabled
            and CFG.runner_dedupe_sec > 0
            and runners
        ):
            filtered: list[dict[str, Any]] = []
            ttl = int(CFG.runner_dedupe_sec)
            for r in runners:
                mint = str(r.get("mint") or "")
                if not mint:
                    continue
                k = f"{CFG.redis_prefix}:runner:{mint}"
                seen = await _redis_get(redis, k)
                if seen:
                    stats.rejects.inc("REJ_REDIS_DEDUP", mint)
                    continue
                await _redis_setex(redis, k, ttl, "1")
                filtered.append(r)
            runners = filtered or runners

        stats.runners = len(runners)

        if CFG.debug_result:
            LOG.every(
                "PUMP_RESULT",
                n=10,
                source=source,
                metas=stats.metas,
                enriched=stats.enriched,
                scored=stats.scored,
                fb=stats.fb_runners,
                runners=stats.runners,
                health=pumpfun_scanner_health(),
            )

        await _send_momo(runners)
        _log_candidates_for_training(runners, regime)

        if CFG.log_scan_summary and _log_event is not None:
            payload = {
                "ts": time.time(),
                "phase": CFG.phase,
                "mode": CFG.mode,
                "provider": CFG.provider,
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
                "elapsed_ms": int((time.time() - stats.t0) * 1000),
                "scanner_version": SCANNER_VERSION,
                "health": pumpfun_scanner_health(),
            }
            await _call_maybe_async(_log_event, "pumpfun_scan_summary", payload)

        return runners

    except asyncio.CancelledError:
        raise
    except Exception as e:
        stats.rejects.inc("REJ_SCAN_EXCEPTION")
        LOG.every("PUMP_SCAN_FAIL", n=5, err=_short(e))
        return []
    finally:
        if stats.rejects.counts:
            LOG.every(
                "PUMP_REJECT_BUCKETS",
                n=10,
                source=stats.source,
                rejects=dict(stats.rejects.counts),
                samples=dict(stats.rejects.samples),
                metas=stats.metas,
                runners=stats.runners,
            )

        if owns_session and session is not None:
            with contextlib.suppress(Exception):
                await session.close()


async def scan_once(*args: Any, **kwargs: Any) -> list[dict[str, Any]]:
    return await scan_pumpfun(*args, **kwargs)


async def scan_loop(
    *,
    redis: Any = None,
    poll_s: float = 6.0,
    jitter_s: float = 1.5,
    on_runners: Any | None = None,
) -> None:
    """
    Lightweight loop if you ever want pumpfun as its own worker.
    - on_runners: optional callable (sync or async) that receives runners list
    """
    poll_s = max(1.0, float(poll_s))
    jitter_s = max(0.0, float(jitter_s))
    while True:
        try:
            runners = await scan_pumpfun(redis=redis)
            if on_runners and runners:
                if asyncio.iscoroutinefunction(on_runners):
                    await on_runners(runners)
                else:
                    await asyncio.to_thread(on_runners, runners)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            LOG.every("PUMP_LOOP_ERR", n=5, err=_short(e))
        await asyncio.sleep(poll_s + (random.random() * jitter_s))


async def shutdown_pumpfun_scanner() -> None:
    """Graceful shutdown hook (safe to call multiple times)."""
    global _PUMPPORTAL
    if _PUMPPORTAL is not None:
        with contextlib.suppress(Exception):
            await _PUMPPORTAL.close()
    _PUMPPORTAL = None


# Common alias for registries that expect `shutdown`
async def shutdown() -> None:
    await shutdown_pumpfun_scanner()


__all__ = [
    "pumpfun_scanner_health",
    "scan_loop",
    "scan_once",
    "scan_pumpfun",
    "shutdown",
    "shutdown_pumpfun_scanner",
]


if __name__ == "__main__":  # pragma: no cover

    async def _demo() -> None:
        await asyncio.sleep(3)
        res = await scan_pumpfun()
        print("runners:", len(res))
        if res:
            r = res[0]
            print(
                r.get("symbol"),
                r.get("mint"),
                r.get("score_combined"),
                r.get("vol_5m_usd"),
                r.get("buys_5m"),
            )
        await shutdown_pumpfun_scanner()

    asyncio.run(_demo())
