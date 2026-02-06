from __future__ import annotations

"""
scanner/dexscreener_scanner.py

Dexscreener Scanner (Solana) — V3+++ LOCK-IN (quant-grade / God-tier)
====================================================================

High-level role
---------------
High-signal candidate discovery using Dexscreener search endpoints, normalized into a
single Candidate dict schema (scanner/registry.py is canonical if present).

Hard requirements (enforced)
----------------------------
- 24/7 safe: never hard-fail on optional modules or transient HTTP errors.
- Zero network at import time.
- Import side-effect free: does not create sessions or SSL contexts until runtime.
- Orchestrator-friendly signatures: accepts (redis/session/stop_evt/q/emit) in any order.
- Emits BOTH modern keys and legacy aliases (liq/vol5/buys5/chg5/age_min, etc.).
- Ranking path:
    1) utils_pkg.scoring_service.score_candidate (if present & enabled)
    2) utils_pkg.pair_filters viability + scoring (legacy path)
    3) Heuristic fallback
- Silent no-buy elimination: reject buckets incremented; compact reject summary logs.

Public API
----------
- async def scan_dexscreener(*args, **kwargs) -> list[dict[str, Any]]
- async def scan_once(*args, **kwargs) -> list[dict[str, Any]]
- async def run_scanner(*args, **kwargs) -> None
- def scanner_health() -> dict[str, Any]
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
from collections.abc import Callable
from dataclasses import dataclass, field
from math import log1p
from typing import Any, cast

# Optional HTTP deps: degrade gracefully if missing
try:
    import aiohttp  # type: ignore
except Exception:  # pragma: no cover
    aiohttp = None  # type: ignore

try:
    import certifi  # type: ignore
except Exception:  # pragma: no cover
    certifi = None  # type: ignore


# =============================================================================
# Logging (no monkey-patching; proxy provides event/once/every)
# =============================================================================
def _get_base_logger(name: str) -> Any:
    try:
        from logging_setup import get_logger  # type: ignore

        try:
            return get_logger(name)
        except TypeError:
            return get_logger()
    except Exception:
        lg = logging.getLogger(name)
        if not lg.handlers:
            logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO").upper())
        return lg


class _LogProxy:
    __slots__ = ("_lg", "_seen")

    def __init__(self, lg: Any) -> None:
        self._lg = lg
        self._seen: dict[str, float] = {}

    def __getattr__(self, item: str) -> Any:
        return getattr(self._lg, item)

    def event(self, name: str, **fields: Any) -> None:
        ev = getattr(self._lg, "event", None)
        if callable(ev):
            with contextlib.suppress(Exception):
                ev(name, **fields)
            return
        with contextlib.suppress(Exception):
            self._lg.info("%s %s", name, fields)

    def once(
        self,
        key: str,
        *,
        ttl: float = 30.0,
        level: int = logging.INFO,
        msg: str | None = None,
        **fields: Any,
    ) -> bool:
        fn = getattr(self._lg, "once", None)
        if callable(fn):
            with contextlib.suppress(Exception):
                return bool(fn(key, ttl=ttl, level=level, msg=msg, **fields))
        now = time.monotonic()
        last = float(self._seen.get(key, 0.0))
        if now - last < float(ttl):
            return False
        self._seen[key] = now
        with contextlib.suppress(Exception):
            self._lg.log(level, "%s %s", (msg or key), fields)
        return True

    def every(
        self,
        key: str,
        *,
        n: float = 10.0,
        level: int = logging.INFO,
        msg: str | None = None,
        **fields: Any,
    ) -> None:
        self.once(key, ttl=float(n), level=level, msg=msg or key, **fields)


LOG = _LogProxy(_get_base_logger("scanner.dexscreener"))


# =============================================================================
# Optional: scanner registry (canonical Candidate schema)
# =============================================================================
RegistryEnsure: Callable[[dict[str, Any]], dict[str, Any]] | None = None
RegistryValidate: Callable[[dict[str, Any]], bool] | None = None

for _try_mod in ("scanner.registry", "scanner.registry", "scanner.registry"):  # stable order
    if RegistryEnsure is not None or RegistryValidate is not None:
        break
    try:
        # Prefer package-local import if scanner is a package
        from scanner import registry as _REG  # type: ignore

        RegistryEnsure = getattr(_REG, "ensure_candidate", None) or getattr(
            _REG, "normalize_candidate", None
        )
        RegistryValidate = getattr(_REG, "validate_candidate", None)
    except Exception:  # pragma: no cover
        RegistryEnsure = None
        RegistryValidate = None
        break


# =============================================================================
# Optional internal modules (soft imports)
# =============================================================================
try:
    from utils_pkg.scoring_service import score_candidate  # type: ignore
except Exception:  # pragma: no cover
    score_candidate = None  # type: ignore

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

    regime_state = _DummyRegimeState()  # type: ignore


try:
    from utils_pkg.risk_manager import risk_capacity_snapshot  # type: ignore
except Exception:  # pragma: no cover

    async def risk_capacity_snapshot(_redis=None) -> dict[str, Any]:  # type: ignore
        return {"risk_capacity": 1.0, "risk_shutdown": False, "risk_mode": "normal"}


try:
    from utils_pkg.alert_manager import AlertManager  # type: ignore
except Exception:  # pragma: no cover
    AlertManager = None  # type: ignore

try:
    from utils_pkg.data_logger import log_event as _dl_log_event  # type: ignore
except Exception:  # pragma: no cover
    _dl_log_event = None  # type: ignore

try:
    from utils_pkg.core_utils import ai_fraud_score  # type: ignore
except Exception:  # pragma: no cover
    ai_fraud_score = None  # type: ignore

try:
    from utils_pkg.mints import normalize_mint as _normalize_mint  # type: ignore
except Exception:  # pragma: no cover

    def _normalize_mint(x: str) -> str:  # type: ignore
        return (x or "").strip()


# =============================================================================
# Config helpers (do not add new env keys here)
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
    if v is None or v == "":
        return default
    try:
        return int(float(v))
    except Exception:
        return default


def _f(name: str, default: float) -> float:
    v = os.getenv(name)
    if v is None or v == "":
        return default
    try:
        return float(v)
    except Exception:
        return default


def _csv_set(name: str) -> set[str]:
    raw = os.getenv(name, "")
    if not raw:
        return set()
    return {x.strip().lower() for x in raw.split(",") if x.strip()}


def _regex(name: str) -> re.Pattern[str] | None:
    pat = os.getenv(name, "")
    if not pat:
        return None
    try:
        return re.compile(pat, re.IGNORECASE)
    except Exception:
        LOG.every("DEX_INVALID_REGEX", n=30, level=logging.WARNING, var=name)
        return None


def _kill_or_disabled() -> bool:
    return (
        _b("KILL_SWITCH", False)
        or _b("DISABLE_SCANNERS", False)
        or _b("DEXSCREENER_SCANNER_DISABLED", False)
    )


# =============================================================================
# Defaults from config.py if present (safe import; no network)
# =============================================================================
try:
    from config import (  # type: ignore
        PF_SCAN_MAX_TOKEN_AGE_MIN,
        PF_SCAN_MIN_LIQ_USD,
        PF_SCAN_MIN_MCAP_USD,
    )
except Exception:  # pragma: no cover
    PF_SCAN_MIN_LIQ_USD = 15_000.0
    PF_SCAN_MIN_MCAP_USD = 50_000.0
    PF_SCAN_MAX_TOKEN_AGE_MIN = 120.0


@dataclass(frozen=True)
class DexConfig:
    # Base
    base_url: str = field(
        default_factory=lambda: _s("DEXSCREENER_BASE_URL", "https://api.dexscreener.com").rstrip(
            "/"
        )
    )
    ua: str = field(
        default_factory=lambda: _s(
            "DEXSCREENER_UA",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/126 Safari/537.36",
        )
    )

    # HTTP
    timeout_s: int = field(default_factory=lambda: max(5, _i("DEXSCREENER_HTTP_TIMEOUT_S", 12)))
    retries: int = field(default_factory=lambda: max(1, _i("DEXSCREENER_HTTP_RETRIES", 3)))
    backoff_s: float = field(
        default_factory=lambda: max(0.1, _f("DEXSCREENER_HTTP_BACKOFF_S", 0.6))
    )
    trust_env: bool = field(default_factory=lambda: _b("DEXSCREENER_TRUST_ENV", True))
    conn_limit: int = field(default_factory=lambda: max(10, _i("DEXSCREENER_HTTP_CONN_LIMIT", 50)))
    rps_limit: float = field(default_factory=lambda: max(0.0, _f("DEXSCREENER_RPS_LIMIT", 0.0)))

    # Queries
    queries: tuple[str, ...] = field(
        default_factory=lambda: tuple(
            q.strip() for q in _s("DEX_SEARCH_QUERIES", "solana").split(",") if q.strip()
        )
        or ("solana",)
    )
    query_concurrency: int = field(default_factory=lambda: max(1, _i("DEX_QUERY_CONCURRENCY", 3)))

    # Soft filters (defaults derived from PF thresholds, but looser)
    soft_min_liq_usd: float = field(
        default_factory=lambda: _f("DEX_SOFT_MIN_LIQ_USD", float(PF_SCAN_MIN_LIQ_USD) * 0.4)
    )
    soft_min_mcap_usd: float = field(
        default_factory=lambda: _f("DEX_SOFT_MIN_MCAP_USD", float(PF_SCAN_MIN_MCAP_USD) * 0.4)
    )
    soft_max_age_min: float = field(
        default_factory=lambda: _f(
            "DEX_SOFT_MAX_AGE_MIN",
            float(PF_SCAN_MAX_TOKEN_AGE_MIN) * 1.2 if float(PF_SCAN_MAX_TOKEN_AGE_MIN) > 0 else 0.0,
        )
    )
    soft_min_txns_5m: int = field(default_factory=lambda: max(0, _i("DEX_SOFT_MIN_TXNS_5M", 0)))
    soft_min_h24_vol_usd: float = field(
        default_factory=lambda: max(0.0, _f("DEX_SOFT_MIN_H24_VOL_USD", 0.0))
    )
    soft_min_price_usd: float = field(
        default_factory=lambda: max(0.0, _f("DEX_SOFT_MIN_PRICE_USD", 0.0))
    )
    soft_max_price_usd: float = field(
        default_factory=lambda: max(0.0, _f("DEX_SOFT_MAX_PRICE_USD", 0.0))
    )  # 0 = none

    # Allow/block lists
    allowed_dex_ids: set[str] = field(default_factory=lambda: _csv_set("DEX_ALLOWED_DEX_IDS"))
    blocked_dex_ids: set[str] = field(default_factory=lambda: _csv_set("DEX_BLOCKED_DEX_IDS"))
    blocked_base_mints: set[str] = field(default_factory=lambda: _csv_set("DEX_BLOCKED_BASE_MINTS"))
    blocked_symbol_re: re.Pattern[str] | None = field(
        default_factory=lambda: _regex("DEX_BLOCKED_SYMBOL_REGEX")
    )

    # Ranking / selection
    max_runners: int = field(default_factory=lambda: max(1, _i("DEX_MAX_RUNNERS", 8)))
    max_emit: int = field(default_factory=lambda: max(5, _i("DEX_LIMIT", 40)))

    scoring_enabled: bool = field(
        default_factory=lambda: _b("DEX_SCORING_ENABLED", True) and (score_candidate is not None)
    )
    scoring_max_per_scan: int = field(
        default_factory=lambda: max(5, _i("DEX_SCORING_MAX_PER_SCAN", 80))
    )
    scoring_concurrency: int = field(
        default_factory=lambda: max(1, _i("DEX_SCORING_CONCURRENCY", 6))
    )
    scoring_min_combined: float = field(
        default_factory=lambda: float(_f("DEX_SCORING_MIN_COMBINED", 55.0))
    )
    scoring_min_label: str = field(
        default_factory=lambda: _s("DEX_SCORING_MIN_LABEL", "YELLOW").upper().strip()
    )

    legacy_pf_enabled: bool = field(default_factory=lambda: _b("DEX_LEGACY_PF_ENABLED", True))
    bypass_pf: bool = field(default_factory=lambda: _b("DEX_BYPASS_PF", False))
    enable_fallback: bool = field(default_factory=lambda: _b("DEX_ENABLE_FALLBACK", True))

    # Fallback heuristic gates
    fb_min_buys_5m: int = field(default_factory=lambda: max(0, _i("DEX_FB_MIN_BUYS_5M", 1)))
    fb_min_vol5_usd: float = field(
        default_factory=lambda: max(0.0, _f("DEX_FB_MIN_VOL5_USD", 300.0))
    )
    fb_max_runners: int = field(default_factory=lambda: max(1, _i("DEX_FB_MAX_RUNNERS", 5)))
    fb_max_impact_1k_bps: float = field(
        default_factory=lambda: max(0.0, _f("DEX_FB_MAX_IMPACT_1K_BPS", 1200.0))
    )

    # AI rug (optional)
    ai_rug_enabled: bool = field(default_factory=lambda: _b("DEX_AI_RUG_ENABLED", False))
    ai_rug_max_per_scan: int = field(
        default_factory=lambda: max(1, _i("DEX_AI_RUG_MAX_PER_SCAN", 20))
    )
    ai_rug_max: float = field(default_factory=lambda: float(_f("DEX_MAX_AI_RUG", 0.88)))

    # Alerts/logging
    send_momo_alerts: bool = field(default_factory=lambda: _b("DEX_SEND_MOMO_ALERTS", True))
    max_alerts_per_scan: int = field(
        default_factory=lambda: max(0, _i("DEX_MAX_ALERTS_PER_SCAN", 3))
    )
    alert_dedupe_ttl_s: int = field(
        default_factory=lambda: max(30, _i("DEX_ALERT_DEDUP_TTL_S", 180))
    )
    log_scan_summary: bool = field(default_factory=lambda: _b("DEX_LOG_SCAN_SUMMARY", True))
    log_reject_buckets: bool = field(default_factory=lambda: _b("DEX_LOG_REJECT_BUCKETS", True))
    reject_log_every_s: int = field(
        default_factory=lambda: max(1, _i("DEX_REJECT_LOG_EVERY_SEC", 10))
    )
    debug_meta_sample: bool = field(default_factory=lambda: _b("DEX_DEBUG_META_SAMPLE", True))
    debug_result: bool = field(default_factory=lambda: _b("DEX_DEBUG_RESULT", True))

    # Run loop
    scan_interval_s: float = field(
        default_factory=lambda: max(0.2, _f("DEX_SCAN_INTERVAL_SEC", 2.5))
    )
    scan_jitter_s: float = field(default_factory=lambda: max(0.0, _f("DEX_SCAN_JITTER_SEC", 0.5)))

    # Redis dedupe
    seen_ttl_s: int = field(default_factory=lambda: max(0, _i("DEX_SEEN_TTL_S", 45)))


CFG = DexConfig()

_LABEL_LEVEL = {"RED": 0, "YELLOW": 1, "GREEN": 2}
_REQUIRED_LABEL_LEVEL = _LABEL_LEVEL.get(CFG.scoring_min_label, 0)

# =============================================================================
# Internal bounded state (no network at import)
# =============================================================================
_LAST_SUMMARY: dict[str, Any] = {}
_SSLCTX: ssl.SSLContext | None = None


def scanner_health() -> dict[str, Any]:
    """Non-network health snapshot (safe for /health)."""
    return {
        "name": "dexscreener_scanner",
        "base_url": CFG.base_url,
        "enabled": not _kill_or_disabled(),
        "scoring_enabled": bool(CFG.scoring_enabled),
        "legacy_pf_enabled": bool(CFG.legacy_pf_enabled),
        "last_summary": dict(_LAST_SUMMARY),
    }


# =============================================================================
# Buckets (reject reasons + notes)
# =============================================================================
@dataclass
class _Buckets:
    rejects: dict[str, int] = field(default_factory=dict)
    notes: dict[str, int] = field(default_factory=dict)
    _last_emit_ts: float = 0.0

    def r(self, k: str, inc: int = 1) -> None:
        self.rejects[k] = int(self.rejects.get(k, 0)) + int(inc)

    def n(self, k: str, inc: int = 1) -> None:
        self.notes[k] = int(self.notes.get(k, 0)) + int(inc)

    def maybe_log(self) -> None:
        if not CFG.log_reject_buckets:
            return
        now = time.monotonic()
        if now - self._last_emit_ts < float(CFG.reject_log_every_s):
            return
        self._last_emit_ts = now
        top = dict(sorted(self.rejects.items(), key=lambda kv: kv[1], reverse=True)[:12])
        LOG.event("DEX_REJECT_BUCKETS", rejects=top)


# =============================================================================
# Call context parsing (orchestrator-friendly)
# =============================================================================
@dataclass
class _CallCtx:
    redis: Any = None
    session: Any = None
    stop_evt: asyncio.Event | None = None
    q: Any = None
    emit: Callable[[Any], Any] | None = None


def _looks_like_queue(x: Any) -> bool:
    return x is not None and (
        callable(getattr(x, "put", None)) or callable(getattr(x, "put_nowait", None))
    )


def _looks_like_redis(x: Any) -> bool:
    if x is None:
        return False
    if isinstance(x, asyncio.Event) or _looks_like_queue(x):
        return False
    return any(callable(getattr(x, nm, None)) for nm in ("get", "set", "hget", "hset", "ping"))


def _is_aiohttp_session(x: Any) -> bool:
    if aiohttp is None:
        return False
    cls = cast(Any, getattr(aiohttp, "ClientSession", None))
    if cls is None:
        return False
    try:
        return isinstance(x, cls)
    except Exception:
        return False


def _parse_ctx(*args: Any, **kwargs: Any) -> _CallCtx:
    ctx = _CallCtx()

    for k in ("stop_evt", "stop_event", "stop"):
        v = kwargs.get(k)
        if isinstance(v, asyncio.Event):
            ctx.stop_evt = v
            break

    for k in ("session", "sess", "http", "client"):
        v = kwargs.get(k)
        if _is_aiohttp_session(v):
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

    v_emit = kwargs.get("emit")
    if callable(v_emit):
        ctx.emit = v_emit

    for a in args:
        if ctx.stop_evt is None and isinstance(a, asyncio.Event):
            ctx.stop_evt = a
            continue
        if ctx.session is None and _is_aiohttp_session(a):
            ctx.session = a
            continue
        if ctx.q is None and _looks_like_queue(a):
            ctx.q = a
            continue
        if ctx.redis is None and _looks_like_redis(a):
            ctx.redis = a
            continue
        if (
            ctx.emit is None
            and callable(a)
            and not isinstance(a, (str, bytes, dict, list, tuple, set))
        ):
            ctx.emit = a  # type: ignore[assignment]

    return ctx


# =============================================================================
# Redis helpers (optional)
# =============================================================================
def _redis_ok(r: Any) -> bool:
    return r is not None and any(
        callable(getattr(r, nm, None)) for nm in ("get", "set", "setex", "expire")
    )


async def _redis_get(r: Any, key: str) -> str | None:
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
# Regime snapshot + tuning
# =============================================================================
async def _regime_snapshot() -> dict[str, Any]:
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
        label = "unknown"
        with contextlib.suppress(Exception):
            label = str(getattr(regime_state, "get", lambda: "unknown")() or "unknown")
        return {
            "label_numeric": 0.0,
            "trend_score": 0.0,
            "volatility_score": 0.0,
            "liquidity_score": 0.5,
            "manipulation_risk": 0.0,
            "label": label,
        }


def _regime_tune_thresholds(regime: dict[str, Any]) -> tuple[float, float, float, float]:
    """
    Light-touch regime tuning:
      - Risk-on: relax soft liquidity/mcap & fallback gates.
      - Risk-off / manipulation: tighten.
    """
    ln = float(regime.get("label_numeric", 0.0))
    manip = float(regime.get("manipulation_risk", 0.0))

    soft_liq = float(CFG.soft_min_liq_usd)
    soft_mcap = float(CFG.soft_min_mcap_usd)
    fb_min_vol = float(CFG.fb_min_vol5_usd)
    fb_max_imp = float(CFG.fb_max_impact_1k_bps)

    if ln >= 0.7 and manip < 0.6:
        soft_liq *= 0.70
        soft_mcap *= 0.70
        fb_min_vol *= 0.70
        if fb_max_imp > 0:
            fb_max_imp *= 1.20
    elif ln <= -0.7 or manip >= 0.8:
        soft_liq *= 1.40
        soft_mcap *= 1.40
        fb_min_vol *= 1.30
        if fb_max_imp > 0:
            fb_max_imp *= 0.85

    return (
        max(0.0, soft_liq),
        max(0.0, soft_mcap),
        max(0.0, fb_min_vol),
        max(0.0, fb_max_imp),
    )


# =============================================================================
# HTTP + rate limiting (SSL ctx is lazy)
# =============================================================================
def _sslctx() -> ssl.SSLContext:
    global _SSLCTX
    if _SSLCTX is None:
        if certifi is not None:
            _SSLCTX = ssl.create_default_context(cafile=certifi.where())  # type: ignore[attr-defined]
        else:  # pragma: no cover
            _SSLCTX = ssl.create_default_context()
    return _SSLCTX


def _hdrs() -> dict[str, str]:
    return {
        "User-Agent": CFG.ua,
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


_RPS = _RpsGate(CFG.rps_limit)


async def _fetch_json(
    session: Any,
    url: str,
    *,
    params: dict[str, Any] | None = None,
    stop_evt: asyncio.Event | None = None,
) -> tuple[dict[str, Any] | None, int, str]:
    status = 0
    first = ""

    if aiohttp is None:  # pragma: no cover
        return None, 0, "aiohttp_missing"

    for attempt in range(1, int(CFG.retries) + 1):
        if stop_evt is not None and stop_evt.is_set():
            return None, 0, "stopped"

        await _RPS.wait()

        try:
            async with session.get(
                url,
                params=params,
                headers=_hdrs(),
                ssl=_sslctx(),
                timeout=aiohttp.ClientTimeout(total=float(CFG.timeout_s)),  # type: ignore[attr-defined]
            ) as r:
                status = int(getattr(r, "status", 0) or 0)
                text = await r.text()
                first = (text or "")[:180]

                if status == 200:
                    try:
                        return await r.json(content_type=None), status, first
                    except Exception:
                        with contextlib.suppress(Exception):
                            return json.loads(text), status, first
                        return None, status, first

                if status == 429:
                    await asyncio.sleep(float(CFG.backoff_s) * attempt + random.uniform(0.0, 0.25))
                    continue

                if 400 <= status < 500:
                    return None, status, first

        except asyncio.CancelledError:
            raise
        except Exception:
            pass

        if attempt < int(CFG.retries):
            await asyncio.sleep(float(CFG.backoff_s) * attempt + random.uniform(0.0, 0.12))

    return None, status, first


def _make_session() -> Any:
    if aiohttp is None:  # pragma: no cover
        return None
    conn = aiohttp.TCPConnector(ssl=_sslctx(), limit=int(CFG.conn_limit))  # type: ignore[attr-defined]
    return aiohttp.ClientSession(  # type: ignore[attr-defined]
        headers={"User-Agent": CFG.ua},
        connector=conn,
        trust_env=bool(CFG.trust_env),
        timeout=aiohttp.ClientTimeout(total=float(CFG.timeout_s) + 5.0),  # type: ignore[attr-defined]
    )


# =============================================================================
# Normalization helpers
# =============================================================================
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


def _age_min(created_ms_or_s: Any) -> float | None:
    try:
        if created_ms_or_s is None:
            return None
        t = float(created_ms_or_s)
        if t <= 0:
            return None
        if t > 10_000_000_000:  # ms
            t = t / 1000.0
        return max(0.0, (time.time() - t) / 60.0)
    except Exception:
        return None


def _dex_id(pair: dict[str, Any]) -> str:
    return (pair.get("dexId") or "").strip().lower() or "dexscreener"


def _buy_ratio(buys: int, sells: int) -> float:
    total = max(1, buys + sells)
    return float(buys) / float(total)


def _allowed_by_lists(
    pair: dict[str, Any], base_mint: str, base_symbol: str, buckets: _Buckets
) -> bool:
    dex_id = _dex_id(pair)
    if dex_id in CFG.blocked_dex_ids:
        buckets.r("blocked_dex")
        return False
    if CFG.allowed_dex_ids and dex_id not in CFG.allowed_dex_ids:
        buckets.r("dex_not_allowed")
        return False
    if base_mint and base_mint.strip().lower() in CFG.blocked_base_mints:
        buckets.r("blocked_mint")
        return False
    if CFG.blocked_symbol_re is not None and base_symbol:
        if CFG.blocked_symbol_re.search(base_symbol):
            buckets.r("blocked_symbol")
            return False
    return True


def _extract_metrics(pair: dict[str, Any]) -> dict[str, Any]:
    base = pair.get("baseToken") or {}
    quote = pair.get("quoteToken") or {}

    liq_usd = _nz((pair.get("liquidity") or {}).get("usd"), 0.0)
    fdv_usd = _nz(pair.get("fdv") or pair.get("fdvUsd") or pair.get("fullyDilutedValuation"), 0.0)
    mcap_usd = _nz(
        pair.get("marketCap") or pair.get("marketCapUsd") or pair.get("mcap") or fdv_usd, 0.0
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

    chg_15m_pct = (
        (chg_1h_pct / 4.0)
        if chg_1h_pct != 0.0
        else (chg_24h_pct / 96.0 if chg_24h_pct != 0.0 else 0.0)
    )
    if chg_5m_pct == 0.0 and chg_1h_pct != 0.0:
        chg_5m_pct = chg_1h_pct / 12.0
    if chg_5m_pct == 0.0 and chg_24h_pct != 0.0:
        chg_5m_pct = chg_24h_pct / 288.0

    created = (
        pair.get("pairCreatedAt") or pair.get("pairCreatedAtMs") or pair.get("pairCreatedAtTs")
    )
    age_min = _age_min(created)
    token_age_min = float(age_min) if age_min is not None else 9999.0

    price_usd = _nz(pair.get("priceUsd"), 0.0)
    holders = _safe_int(pair.get("holders") or 0)

    impact_1k_bps = _nz(
        pair.get("priceImpact1kBps") or pair.get("impact1kBps") or pair.get("impactBps"), 0.0
    )

    base_symbol = str(base.get("symbol") or "").strip()
    base_mint = str(base.get("address") or "").strip()
    quote_symbol = str(quote.get("symbol") or "").strip()

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
        "impact_1k_bps": float(impact_1k_bps),
        "dex_id": _dex_id(pair),
        "base_symbol": base_symbol,
        "base_mint": base_mint,
        "quote_symbol": quote_symbol,
        "h24_volume_usd": float(h24_vol_usd),
    }


def _normalize_pair_to_meta(
    pair: dict[str, Any],
    *,
    regime: dict[str, Any],
    buckets: _Buckets,
    soft_liq: float,
    soft_mcap: float,
) -> dict[str, Any] | None:
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
        if CFG.soft_max_age_min > 0 and float(m["token_age_min"]) > float(CFG.soft_max_age_min):
            buckets.r("soft_age")
            return None

        txns5 = int(m["buys_5m"]) + int(m["sells_5m"])
        if CFG.soft_min_txns_5m > 0 and txns5 < int(CFG.soft_min_txns_5m):
            buckets.r("soft_txns5")
            return None

        px = float(m["price_usd"])
        if CFG.soft_min_price_usd > 0 and px < float(CFG.soft_min_price_usd):
            buckets.r("soft_price_low")
            return None
        if CFG.soft_max_price_usd > 0 and px > float(CFG.soft_max_price_usd):
            buckets.r("soft_price_high")
            return None

        if CFG.soft_min_h24_vol_usd > 0 and float(m["h24_volume_usd"]) < float(
            CFG.soft_min_h24_vol_usd
        ):
            buckets.r("soft_h24vol")
            return None

        dex_id = str(m.get("dex_id") or "dexscreener")
        pair_addr = str(pair.get("pairAddress") or "").strip() or None
        url = (
            pair.get("url")
            or pair.get("pairUrl")
            or (
                f"https://dexscreener.com/solana/{pair_addr}"
                if pair_addr
                else f"https://dexscreener.com/solana/{mint}"
            )
        )

        symbol = sym or mint[:6]
        ts = float(time.time())

        meta: dict[str, Any] = {
            "source": "dexscreener",
            "src": "dexscreener",
            "scanner": "dexscreener_scanner",
            "scanner_id": "dexscreener_scanner",
            "chain": "solana",
            "dex": dex_id,
            "dexId": dex_id,
            "mint": mint,
            "symbol": symbol,
            "sym": symbol,
            "pair_address": pair_addr,
            "pairAddress": pair_addr,
            "pair_url": url,
            "url": url,
            "scan_ts": ts,
            "ts": ts,
            "regime": regime.get("label", "unknown"),
            "regime_numeric": float(regime.get("label_numeric", 0.0)),
            "regime_manipulation": float(regime.get("manipulation_risk", 0.0)),
            "regime_ctx": dict(regime),
            # modern metrics
            "liq_usd": float(m["liq_usd"]),
            "mcap_usd": float(m["mcap_usd"]),
            "fdv_usd": float(m["fdv_usd"]),
            "token_age_min": float(m["token_age_min"]),
            "vol_5m_usd": float(m["vol_5m_usd"]),
            "vol_1h_usd": float(m["vol_1h_usd"]),
            "vol_24h_usd": float(m["vol_24h_usd"]),
            "buys_5m": int(m["buys_5m"]),
            "sells_5m": int(m["sells_5m"]),
            "txns_5m": int(txns5),
            "price_usd": float(m["price_usd"]),
            "price_change_5m_pct": float(m["price_change_5m_pct"]),
            "price_change_15m_pct": float(m["price_change_15m_pct"]),
            "price_change_1h_pct": float(m["price_change_1h_pct"]),
            "holders": int(m["holders"]),
            "impact_1k_bps": float(m["impact_1k_bps"]),
            "dex_h24_volume_usd": float(m["h24_volume_usd"]),
            # legacy aliases
            "liq": float(m["liq_usd"]),
            "mcap": float(m["mcap_usd"]),
            "fdv": float(m["fdv_usd"]),
            "age_min": float(m["token_age_min"]),
            "vol5": float(m["vol_5m_usd"]),
            "vol1h": float(m["vol_1h_usd"]),
            "vol24h": float(m["vol_24h_usd"]),
            "buys5": int(m["buys_5m"]),
            "sells5": int(m["sells_5m"]),
            "txns5": int(txns5),
            "chg5": float(m["price_change_5m_pct"]),
            "chg15": float(m["price_change_15m_pct"]),
            "chg1h": float(m["price_change_1h_pct"]),
            "price": float(m["price_usd"]),
            "impact1k_bps": float(m["impact_1k_bps"]),
            # derived
            "buy_ratio_5m": _buy_ratio(int(m["buys_5m"]), int(m["sells_5m"])),
            # enrichment placeholders
            "ai_rug_score": 0.0,
            "rank_score": None,
        }
        return meta
    except Exception:
        buckets.r("normalize_exc")
        return None


def _dedupe_by_mint(items: list[dict[str, Any]], buckets: _Buckets) -> list[dict[str, Any]]:
    seen: set[str] = set()
    out: list[dict[str, Any]] = []
    for it in items:
        mint = it.get("mint")
        if not isinstance(mint, str) or not mint:
            buckets.r("missing_mint_post")
            continue
        if mint in seen:
            buckets.r("dedupe")
            continue
        seen.add(mint)
        out.append(it)
    return out


# =============================================================================
# Candidate assembly (registry-aware)
# =============================================================================
def _to_candidate(meta: dict[str, Any]) -> dict[str, Any] | None:
    cand: dict[str, Any] = dict(meta)

    cand.setdefault("source", "dexscreener")
    cand.setdefault("scanner", "dexscreener_scanner")
    cand.setdefault("chain", "solana")
    cand.setdefault("mint", meta.get("mint"))
    cand.setdefault("symbol", meta.get("symbol") or meta.get("sym"))
    cand.setdefault("pair_url", meta.get("pair_url") or meta.get("url"))

    if callable(RegistryEnsure):
        with contextlib.suppress(Exception):
            cand = RegistryEnsure(cand)  # type: ignore[misc]

    if callable(RegistryValidate):
        with contextlib.suppress(Exception):
            if not bool(RegistryValidate(cand)):  # type: ignore[misc]
                return None

    return cand


# =============================================================================
# Fetch pairs
# =============================================================================
async def _fetch_pairs_for_query(
    session: Any,
    q: str,
    *,
    stop_evt: asyncio.Event | None,
    buckets: _Buckets,
) -> list[dict[str, Any]]:
    url = f"{CFG.base_url}/latest/dex/search"
    payload, status, first = await _fetch_json(session, url, params={"q": q}, stop_evt=stop_evt)
    if not payload:
        buckets.r("search_empty")
        if status:
            LOG.every(
                "DEX_SEARCH_EMPTY",
                n=10,
                level=logging.INFO,
                status=status,
                q=q,
                first=first,
            )
        return []

    pairs = payload.get("pairs") or []
    out: list[dict[str, Any]] = []
    for p in pairs:
        if isinstance(p, dict):
            out.append(p)
        else:
            buckets.r("pair_nondict")
    return out


async def _fetch_pairs(
    session: Any,
    *,
    stop_evt: asyncio.Event | None,
    buckets: _Buckets,
) -> list[dict[str, Any]]:
    sem = asyncio.Semaphore(int(CFG.query_concurrency))
    all_pairs: list[dict[str, Any]] = []
    seen_ids: set[str] = set()

    async def _one(query: str) -> None:
        async with sem:
            res = await _fetch_pairs_for_query(session, query, stop_evt=stop_evt, buckets=buckets)
        for p in res:
            try:
                pid = p.get("pairAddress") or ((p.get("baseToken") or {}).get("address")) or ""
                pid_s = str(pid).strip().lower()
                if not pid_s:
                    buckets.r("pair_missing_id")
                    continue
                if pid_s in seen_ids:
                    buckets.r("pair_dupe")
                    continue
                seen_ids.add(pid_s)
                all_pairs.append(p)
            except Exception:
                buckets.r("pair_id_exc")

    tasks = [asyncio.create_task(_one(q)) for q in CFG.queries]
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)
    return all_pairs


# =============================================================================
# Enrichment (optional)
# =============================================================================
async def _maybe_call(fn: Any, *a: Any, **k: Any) -> Any:
    out = fn(*a, **k)
    if asyncio.iscoroutine(out):
        return await out
    return out


async def _attach_ai_rug(
    metas: list[dict[str, Any]],
    *,
    stop_evt: asyncio.Event | None,
    buckets: _Buckets,
) -> None:
    if not (CFG.ai_rug_enabled and (ai_fraud_score is not None) and metas):
        return

    targets = metas[: max(1, int(CFG.ai_rug_max_per_scan))]
    sem = asyncio.Semaphore(8)

    async def _one(m: dict[str, Any]) -> None:
        if stop_evt is not None and stop_evt.is_set():
            return
        mint = m.get("mint")
        if not isinstance(mint, str) or not mint:
            return
        async with sem:
            try:
                s01 = await _maybe_call(ai_fraud_score, mint)
                if s01 is None:
                    return
                m["ai_rug_score"] = max(0.0, min(1.0, float(s01)))
            except Exception:
                buckets.r("ai_rug_exc")

    await asyncio.gather(*[asyncio.create_task(_one(m)) for m in targets], return_exceptions=True)


# =============================================================================
# Scoring paths
# =============================================================================
async def _score_with_service(
    metas: list[dict[str, Any]],
    *,
    regime: dict[str, Any],
    risk_ctx: dict[str, Any] | None,
    stop_evt: asyncio.Event | None,
    buckets: _Buckets,
) -> list[dict[str, Any]]:
    if not (CFG.scoring_enabled and (score_candidate is not None) and metas):
        return []

    metas_sorted = sorted(
        metas,
        key=lambda x: (
            float(x.get("vol5") or 0.0),
            int(x.get("buys5") or 0),
            float(x.get("liq") or 0.0),
        ),
        reverse=True,
    )
    targets = metas_sorted[: int(CFG.scoring_max_per_scan)]
    sem = asyncio.Semaphore(int(CFG.scoring_concurrency))
    out: list[dict[str, Any]] = []

    async def _one(meta: dict[str, Any]) -> None:
        if stop_evt is not None and stop_evt.is_set():
            return

        if CFG.ai_rug_enabled and float(meta.get("ai_rug_score") or 0.0) >= float(CFG.ai_rug_max):
            buckets.r("rug_high_pre_score")
            return

        try:
            if not is_viable_pair(meta):
                buckets.r("pf_not_viable_pre_score")
                return
        except Exception:
            pass

        ctx = {
            "source": "dexscreener",
            "scanner": "dexscreener_scanner",
            "regime": dict(regime),
            "risk": dict(risk_ctx or {}),
        }

        async with sem:
            try:
                st = await _maybe_call(score_candidate, dict(meta), context=ctx)  # type: ignore[misc]
            except Exception:
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
        m["rank_score"] = float(m["score_combined"])
        out.append(m)

    await asyncio.gather(*[asyncio.create_task(_one(m)) for m in targets], return_exceptions=True)
    return out


def _select_runners_from_scored(
    scored: list[dict[str, Any]], buckets: _Buckets
) -> list[dict[str, Any]]:
    if not scored:
        return []
    runners: list[dict[str, Any]] = []
    for m in scored:
        try:
            label = str(m.get("score_label") or "").upper() or "YELLOW"
            combined = float(m.get("score_combined") or 0.0)
            ok = bool(m.get("score_is_trade_viable", True))
            if not ok:
                buckets.r("score_trade_not_viable")
                continue
            if combined < float(CFG.scoring_min_combined):
                buckets.r("score_combined_low")
                continue
            if _LABEL_LEVEL.get(label, 0) < _REQUIRED_LABEL_LEVEL:
                buckets.r("score_label_low")
                continue
            if CFG.ai_rug_enabled and float(m.get("ai_rug_score") or 0.0) >= float(CFG.ai_rug_max):
                buckets.r("rug_high_scored")
                continue
            runners.append(m)
        except Exception:
            buckets.r("select_scored_exc")

    runners.sort(key=lambda x: float(x.get("score_combined") or 0.0), reverse=True)
    return runners[: int(CFG.max_runners)]


def _legacy_pf_rank(metas: list[dict[str, Any]], buckets: _Buckets) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for meta in metas:
        try:
            if CFG.ai_rug_enabled and float(meta.get("ai_rug_score") or 0.0) >= float(
                CFG.ai_rug_max
            ):
                buckets.r("rug_high_pf")
                continue
            if not is_viable_pair(meta):
                buckets.r("pf_not_viable")
                continue

            momentum = float(compute_momentum_score(meta))
            final = float(compute_final_score(meta))

            vol5 = float(meta.get("vol5") or 0.0)
            up5 = float(meta.get("chg5") or 0.0)
            liq = float(meta.get("liq") or 0.0)
            age = float(meta.get("age_min") or 9999.0)

            rank = (0.70 * final) + (0.25 * momentum)
            rank += 0.02 * log1p(max(vol5, 0.0))
            rank += 0.01 * log1p(max(liq, 0.0))
            rank += 0.002 * max(0.0, up5)
            rank -= 0.003 * max(0.0, age - 5.0)

            meta2 = dict(meta)
            meta2["final_score"] = final
            meta2["momentum_score"] = momentum
            meta2["rank_score"] = float(rank)
            out.append(meta2)
        except Exception:
            buckets.r("pf_rank_exc")

    out.sort(key=lambda x: float(x.get("rank_score") or 0.0), reverse=True)
    return out[: int(CFG.max_runners)]


def _fallback_pick(
    metas: list[dict[str, Any]],
    *,
    fb_min_vol5: float,
    fb_max_imp: float,
    buckets: _Buckets,
) -> list[dict[str, Any]]:
    if not (CFG.enable_fallback and metas):
        return []

    cands: list[dict[str, Any]] = []
    for m in metas:
        try:
            buys5 = int(m.get("buys5") or 0)
            vol5 = float(m.get("vol5") or 0.0)
            impact = float(m.get("impact1k_bps") or m.get("impact_1k_bps") or 0.0)
            liq = float(m.get("liq") or 0.0)
            up5 = float(m.get("chg5") or 0.0)
            age = float(m.get("age_min") or 9999.0)

            if buys5 < int(CFG.fb_min_buys_5m):
                buckets.r("fb_buys5_low")
                continue
            if vol5 < float(fb_min_vol5):
                buckets.r("fb_vol5_low")
                continue
            if fb_max_imp > 0 and impact > float(fb_max_imp):
                buckets.r("fb_impact_high")
                continue
            if CFG.ai_rug_enabled and float(m.get("ai_rug_score") or 0.0) >= float(CFG.ai_rug_max):
                buckets.r("fb_rug_high")
                continue

            rank = (
                0.40 * log1p(max(vol5, 0.0))
                + 0.25 * log1p(max(liq, 0.0))
                + 0.20 * log1p(max(buys5, 0))
            )
            rank += 0.01 * max(0.0, up5)
            rank -= 0.003 * max(0.0, age - 5.0)

            mm = dict(m)
            mm["rank_score"] = float(rank)
            cands.append(mm)
        except Exception:
            buckets.r("fb_exc")

    cands.sort(key=lambda x: float(x.get("rank_score") or 0.0), reverse=True)
    return cands[: int(CFG.fb_max_runners)]


# =============================================================================
# Alerts + scan summary logging
# =============================================================================
async def _send_momo_for_runners(
    runners: list[dict[str, Any]],
    *,
    redis: Any,
    buckets: _Buckets,
) -> None:
    if not (CFG.send_momo_alerts and runners and AlertManager is not None):
        return
    try:
        am = AlertManager()  # type: ignore[call-arg]
    except Exception:
        buckets.r("alert_mgr_init_exc")
        return

    max_n = max(0, min(int(CFG.max_alerts_per_scan), len(runners)))
    for c in runners[:max_n]:
        try:
            mint = str(c.get("mint") or "").strip()
            if not mint:
                continue

            if _redis_ok(redis):
                k = f"dex:momo_sent:{mint}"
                prev = await _redis_get(redis, k)
                if prev:
                    continue
                await _redis_setex(redis, k, int(CFG.alert_dedupe_ttl_s), "1")

            ctx = {
                "symbol": c.get("symbol") or c.get("sym") or mint[:6],
                "mint": mint,
                "pool": c.get("dex") or c.get("dexId") or "dexscreener",
                "pool_url": c.get("pair_url") or c.get("url"),
                "age_min": c.get("age_min") or c.get("token_age_min"),
                "liq_usd": c.get("liq") or c.get("liq_usd"),
                "vol_5m": c.get("vol5") or c.get("vol_5m_usd"),
                "up_pct": c.get("chg5") or c.get("price_change_5m_pct"),
                "ai_rug_score": c.get("ai_rug_score"),
                "rank_score": c.get("rank_score"),
                "score_combined": c.get("score_combined"),
                "score_label": c.get("score_label"),
                "score_ai": c.get("score_ai"),
            }

            out = am.send_momo_event("MOMO_OK", ctx)
            if asyncio.iscoroutine(out):
                await out
        except Exception:
            buckets.r("alert_send_exc")


async def _log_scan_summary(summary: dict[str, Any]) -> None:
    global _LAST_SUMMARY
    _LAST_SUMMARY = dict(summary)

    if not CFG.log_scan_summary:
        return

    if callable(_dl_log_event):
        with contextlib.suppress(Exception):
            out = _dl_log_event("dexscreener_scan_summary", summary)  # type: ignore[misc]
            if asyncio.iscoroutine(out):
                await out
            return

    LOG.event("DEX_SCAN_SUMMARY", **summary)


# =============================================================================
# Emission helpers
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
# Public API
# =============================================================================
async def scan_once(*args: Any, **kwargs: Any) -> list[dict[str, Any]]:
    if _kill_or_disabled():
        return []

    if aiohttp is None:  # pragma: no cover
        LOG.every(
            "DEX_AIOHTTP_MISSING",
            n=60,
            level=logging.WARNING,
            msg="aiohttp not installed; dexscreener_scanner disabled",
        )
        return []

    ctx = _parse_ctx(*args, **kwargs)
    stop_evt = ctx.stop_evt
    buckets = _Buckets()

    try:
        risk_ctx = await risk_capacity_snapshot(ctx.redis)
    except Exception:
        risk_ctx = None
        buckets.n("risk_snapshot_exc")

    risk_capacity = float((risk_ctx or {}).get("risk_capacity", 1.0) or 1.0)
    risk_shutdown = bool((risk_ctx or {}).get("risk_shutdown", False))
    if risk_shutdown or risk_capacity <= 0.0:
        buckets.r("risk_shutdown")
        buckets.maybe_log()
        return []

    regime = await _regime_snapshot()
    soft_liq, soft_mcap, fb_min_vol, fb_max_imp = _regime_tune_thresholds(regime)

    if CFG.debug_meta_sample:
        LOG.info(
            "dexscreener_scanner start scoring=%s legacy_pf=%s bypass_pf=%s queries=%s soft_liq=%.0f soft_mcap=%.0f max_age=%.0f "
            "regime=%.2f manip=%.2f risk_capacity=%.3f risk_mode=%s",
            CFG.scoring_enabled,
            CFG.legacy_pf_enabled,
            CFG.bypass_pf,
            ",".join(CFG.queries),
            soft_liq,
            soft_mcap,
            float(CFG.soft_max_age_min),
            float(regime.get("label_numeric", 0.0)),
            float(regime.get("manipulation_risk", 0.0)),
            risk_capacity,
            (risk_ctx or {}).get("risk_mode", "normal"),
        )

    sess = ctx.session
    own_session = False
    if sess is None or not _is_aiohttp_session(sess):
        sess = _make_session()
        own_session = True

    if sess is None:  # pragma: no cover
        buckets.r("no_session")
        return []

    try:
        pairs = cast(list[Any], await _fetch_pairs(sess, stop_evt=stop_evt, buckets=buckets))

        metas: list[dict[str, Any]] = []
        for p in pairs:
            if stop_evt is not None and stop_evt.is_set():
                buckets.n("stopped")
                break
            if not isinstance(p, dict):
                buckets.r("pair_nondict_post")
                continue
            meta = _normalize_pair_to_meta(
                p,
                regime=regime,
                buckets=buckets,
                soft_liq=soft_liq,
                soft_mcap=soft_mcap,
            )
            if meta is not None:
                metas.append(meta)

        metas = _dedupe_by_mint(metas, buckets)

        # Optional Redis seen-dedupe
        if CFG.seen_ttl_s > 0 and _redis_ok(ctx.redis):
            filtered: list[dict[str, Any]] = []
            for m in metas:
                mint = m.get("mint")
                if not isinstance(mint, str) or not mint:
                    continue
                k = f"dex:seen:{mint}"
                if await _redis_get(ctx.redis, k):
                    buckets.r("seen_recently")
                    continue
                await _redis_setex(ctx.redis, k, int(CFG.seen_ttl_s), "1")
                filtered.append(m)
            metas = filtered

        if not metas:
            buckets.r("no_metas")
            buckets.maybe_log()
            return []

        await _attach_ai_rug(metas, stop_evt=stop_evt, buckets=buckets)

        scored: list[dict[str, Any]] = []
        runners: list[dict[str, Any]] = []

        # 1) scoring_service path
        if CFG.scoring_enabled and (stop_evt is None or not stop_evt.is_set()):
            scored = await _score_with_service(
                metas,
                regime=regime,
                risk_ctx=risk_ctx,
                stop_evt=stop_evt,
                buckets=buckets,
            )
            runners = _select_runners_from_scored(scored, buckets=buckets)
            if not runners:
                buckets.r("no_runners_scored")

        # 2) legacy pair_filters path
        if (
            not runners
            and CFG.legacy_pf_enabled
            and (not CFG.bypass_pf)
            and (stop_evt is None or not stop_evt.is_set())
        ):
            runners = _legacy_pf_rank(metas, buckets=buckets)
            if not runners:
                buckets.r("no_runners_pf")

        # 3) heuristic fallback
        if not runners and (stop_evt is None or not stop_evt.is_set()):
            runners = _fallback_pick(
                metas,
                fb_min_vol5=fb_min_vol,
                fb_max_imp=fb_max_imp,
                buckets=buckets,
            )
            if not runners:
                buckets.r("no_runners_fallback")

        runners = _dedupe_by_mint(runners, buckets)
        runners.sort(
            key=lambda x: float(x.get("rank_score") or x.get("score_combined") or 0.0),
            reverse=True,
        )
        runners = runners[: int(CFG.max_emit)]

        # Apply risk capacity as a throttle (keeps at least 1 if any exist)
        if runners and risk_capacity < 1.0:
            keep = int(len(runners) * max(0.0, min(1.0, risk_capacity)))
            keep = max(1, keep)
            if keep < len(runners):
                buckets.n("risk_shrink")
                runners = runners[:keep]

        candidates: list[dict[str, Any]] = []
        for r in runners:
            c = _to_candidate(r)
            if c is None:
                buckets.r("registry_invalid")
                continue
            candidates.append(c)

        if CFG.debug_result:
            LOG.info(
                "DEX_RESULT pairs=%d metas=%d scored=%d runners=%d candidates=%d",
                len(pairs),
                len(metas),
                len(scored),
                len(runners),
                len(candidates),
            )

        buckets.maybe_log()

        summary = {
            "source": "dexscreener",
            "scanner": "dexscreener_scanner",
            "pairs": len(pairs),
            "metas": len(metas),
            "scored": len(scored),
            "runners": len(runners),
            "candidates": len(candidates),
            "regime": dict(regime),
            "risk": dict(risk_ctx or {}),
            "rejects": dict(buckets.rejects),
            "notes": dict(buckets.notes),
        }
        await _log_scan_summary(summary)

        with contextlib.suppress(Exception):
            await _send_momo_for_runners(runners, redis=ctx.redis, buckets=buckets)

        if candidates and (ctx.emit is not None or ctx.q is not None):
            for c in candidates:
                if stop_evt is not None and stop_evt.is_set():
                    break
                await _emit_one(ctx, c)

        return candidates

    except asyncio.CancelledError:
        raise
    except Exception as e:
        buckets.r("scan_once_exc")
        LOG.every(
            "DEX_SCAN_CRASH",
            n=10,
            level=logging.ERROR,
            err=f"{type(e).__name__}: {e}",
        )
        buckets.maybe_log()
        return []
    finally:
        if own_session and sess is not None and _is_aiohttp_session(sess):
            with contextlib.suppress(Exception):
                await sess.close()  # type: ignore[func-returns-value]


async def scan_dexscreener(*args: Any, **kwargs: Any) -> list[dict[str, Any]]:
    return await scan_once(*args, **kwargs)


async def run_scanner(*args: Any, **kwargs: Any) -> None:
    ctx = _parse_ctx(*args, **kwargs)
    stop_evt = ctx.stop_evt or asyncio.Event()

    if aiohttp is None:  # pragma: no cover
        while not stop_evt.is_set():
            await asyncio.sleep(1.0)
        return

    sess = ctx.session
    own_session = False
    if sess is None or not _is_aiohttp_session(sess):
        sess = _make_session()
        own_session = True

    try:
        while not stop_evt.is_set():
            if _kill_or_disabled():
                await asyncio.sleep(0.75)
                continue

            with contextlib.suppress(Exception):
                await scan_once(
                    redis=ctx.redis,
                    session=sess,
                    stop_evt=stop_evt,
                    q=ctx.q,
                    emit=ctx.emit,
                )

            dt = float(CFG.scan_interval_s)
            if CFG.scan_jitter_s > 0:
                dt = max(
                    0.05,
                    dt + random.uniform(-float(CFG.scan_jitter_s), float(CFG.scan_jitter_s)),
                )
            await asyncio.sleep(max(0.05, dt))
    finally:
        if own_session and sess is not None and _is_aiohttp_session(sess):
            with contextlib.suppress(Exception):
                await sess.close()  # type: ignore[func-returns-value]


async def run(*args: Any, **kwargs: Any) -> None:
    await run_scanner(*args, **kwargs)


async def main(*args: Any, **kwargs: Any) -> None:
    await run_scanner(*args, **kwargs)


__all__ = ["main", "run", "run_scanner", "scan_dexscreener", "scan_once", "scanner_health"]
