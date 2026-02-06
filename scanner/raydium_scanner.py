from __future__ import annotations

"""
scanner/raydium_scanner.py — Raydium discovery scanner (P0 LOCK-IN, V3+++)
===========================================================================

Role (SCANNER ONLY)
-------------------
- Discover Solana Raydium pairs via Dexscreener search endpoint(s)
- Normalize into *canonical* Candidate objects defined in scanner/registry.py
- NO trading, NO signing, NO swap/route calls
- Zero network at import time
- Cancellation-safe (never swallow asyncio.CancelledError)
- Orchestrator-friendly call shapes (redis/session/stop_evt/q/emit accepted in any order)

Contracts
---------
- scan_raydium(...) returns: list[Candidate] (canonical)
- scan_once(...) alias returns the same
- run(...) optional loop helper (dev/tools/legacy): emits to queue/callback

Observability
-------------
- Every rejection increments a reason bucket (no silent no-buy)
- Periodic structured reject summaries for "why nothing is passing"

Notes
-----
- Degrades gracefully when optional modules are missing, unless FAST_FAIL is enabled.
"""

import asyncio
import contextlib
import hashlib
import inspect
import json
import logging
import os
import random
import ssl
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

# =============================================================================
# Optional HTTP deps (scanner should degrade gracefully unless FAST_FAIL)
# =============================================================================
try:
    import aiohttp as _aiohttp  # type: ignore[import-not-found]
except Exception:  # pragma: no cover
    _aiohttp = None
aiohttp: Any | None = _aiohttp

try:
    import certifi as _certifi  # type: ignore[import-not-found]
except Exception:  # pragma: no cover
    _certifi = None
certifi: Any | None = _certifi

# =============================================================================
# Canonical Candidate/Source (scanner/registry.py) — runtime optional
# =============================================================================
if TYPE_CHECKING:
    from .registry import Candidate as CandidateT, Source as SourceT
else:  # pragma: no cover
    CandidateT = Any  # type: ignore[assignment]
    SourceT = Any  # type: ignore[assignment]

CandidateClass: type[CandidateT] | None
try:
    from .registry import Candidate as _Candidate  # type: ignore[import-not-found]
except Exception:  # pragma: no cover
    CandidateClass = None
else:
    CandidateClass = _Candidate

_SourceEnum: Any | None
try:
    from .registry import Source as _Source  # type: ignore[import-not-found]
except Exception:  # pragma: no cover
    _SourceEnum = None
else:
    _SourceEnum = _Source


def _coerce_source(x: Any) -> SourceT:
    """
    Coerce 'source' into scanner.registry.Source when available; fallback to str.
    """
    if _SourceEnum is None:
        return str(x or "raydium")  # type: ignore[return-value]

    src = _SourceEnum
    if isinstance(x, src):
        return x  # type: ignore[return-value]

    s = str(x or "raydium").strip() or "raydium"

    # Enum name lookup: Source.RAYDIUM etc.
    with contextlib.suppress(Exception):
        return src[s.upper()]  # type: ignore[index,return-value]

    # Value ctor: Source("raydium") etc.
    with contextlib.suppress(Exception):
        return src(s)  # type: ignore[call-arg,return-value]

    # Attribute fallback
    with contextlib.suppress(Exception):
        return src.RAYDIUM  # type: ignore[return-value]

    # last resort: first enum member
    with contextlib.suppress(Exception):
        return list(src)[0]  # type: ignore[return-value]

    return str(s)  # type: ignore[return-value]


# =============================================================================
# Logging (Phase A compliant: canonical logger contract; no monkey patching)
# =============================================================================
def _fallback_setup_logging(*_: Any, **__: Any) -> None:
    return None


def _resolve_logger_base(name: str) -> Any:
    """
    Return a logger-like object.
    Prefer utils_pkg.log_compat.get_logger() if present; else stdlib logging.Logger.
    """
    try:
        from utils_pkg.log_compat import get_logger as canon_get_logger  # type: ignore

        return canon_get_logger(name)
    except Exception:
        return logging.getLogger(name)


def _resolve_setup_logging() -> Callable[..., None]:
    try:
        from utils_pkg.log_compat import setup_logging as canon_setup_logging  # type: ignore

        return canon_setup_logging
    except Exception:
        return _fallback_setup_logging


_SETUP_LOGGING = _resolve_setup_logging()


class _LogProxy:
    """
    Wraps a logger-like object and provides .event/.once/.every WITHOUT mutating it.
    If the base already provides event/once/every (canonical EventLogger), we defer to it.
    """

    def __init__(self, base: Any) -> None:
        self._base = base
        self._seen: dict[str, float] = {}

    def event(self, name: str, **fields: Any) -> None:
        fn = getattr(self._base, "event", None)
        if callable(fn):
            with contextlib.suppress(Exception):
                fn(name, **fields)
            return
        with contextlib.suppress(Exception):
            self._base.info("%s %s", name, fields)

    def once(
        self,
        key: str,
        ttl: float = 30.0,
        level: int = logging.INFO,
        msg: str | None = None,
        **fields: Any,
    ) -> bool:
        fn = getattr(self._base, "once", None)
        if callable(fn):
            with contextlib.suppress(Exception):
                return bool(fn(key, ttl=ttl, level=level, msg=msg, **fields))
            return False

        now = time.monotonic()
        last = float(self._seen.get(key, 0.0))
        if (now - last) < float(ttl):
            return False
        self._seen[key] = now
        with contextlib.suppress(Exception):
            self._base.log(level, "%s %s", (msg or key), fields)
        return True

    def every(
        self,
        key: str,
        n: float = 10.0,
        level: int = logging.INFO,
        msg: str | None = None,
        **fields: Any,
    ) -> None:
        fn = getattr(self._base, "every", None)
        if callable(fn):
            with contextlib.suppress(Exception):
                fn(key, n=n, level=level, msg=msg, **fields)
            return
        self.once(key, ttl=float(n), level=level, msg=(msg or key), **fields)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._base, name)


LOG = _LogProxy(_resolve_logger_base("scanner.raydium_scanner"))


def _short(x: Any, n: int = 240) -> str:
    try:
        s = str(x)
        return s if len(s) <= n else (s[:n] + "…")
    except Exception:
        return "<unprintable>"


# =============================================================================
# Env / config helpers (NO NEW ENV KEYS HERE)
# =============================================================================
_TRUE = {"1", "true", "t", "yes", "y", "on"}
_FALSE = {"0", "false", "f", "no", "n", "off"}


def _b(name: str, default: bool = False) -> bool:
    v = os.getenv(name)
    if v is None or str(v).strip() == "":
        return bool(default)
    s = str(v).strip().lower()
    if s in _TRUE:
        return True
    if s in _FALSE:
        return False
    return bool(default)


def _i(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None:
        return int(default)
    s = raw.strip()
    if s == "":
        return int(default)
    try:
        return int(float(s))
    except Exception:
        return int(default)


def _f(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None:
        return float(default)
    s = raw.strip()
    if s == "":
        return float(default)
    try:
        return float(s)
    except Exception:
        return float(default)


def _s(name: str, default: str = "") -> str:
    v = os.getenv(name)
    return default if v is None else str(v).strip()


def _clamp(x: float, lo: float, hi: float) -> float:
    return lo if x < lo else hi if x > hi else x


def _fast_fail() -> bool:
    return _b("RAYDIUM_FAST_FAIL", _b("MAIN_FAST_FAIL", False))


def _disabled() -> bool:
    return bool(
        _b("KILL_SWITCH", False)
        or _b("DISABLE_SCANNERS", False)
        or _b("RAYDIUM_SCANNER_DISABLED", False)
    )


def _coerce_int(value: Any, default: int) -> int:
    if value is None:
        return int(default)
    if isinstance(value, bool):
        return int(default)
    if isinstance(value, int):
        return int(value)
    if isinstance(value, float):
        with contextlib.suppress(Exception):
            return int(value)
        return int(default)
    if isinstance(value, str):
        s = value.strip()
        if s == "":
            return int(default)
        with contextlib.suppress(Exception):
            return int(float(s))
        return int(default)
    with contextlib.suppress(Exception):
        return int(value)  # type: ignore[arg-type]
    return int(default)


# config.py is optional; env always wins
try:
    import config as C  # type: ignore
except Exception:  # pragma: no cover

    class _C:
        pass

    C = _C()  # type: ignore


def _cfg(name: str, default: Any) -> Any:
    if os.getenv(name) not in (None, ""):
        if isinstance(default, bool):
            return _b(name, bool(default))
        if isinstance(default, int) and not isinstance(default, bool):
            return _i(name, int(default))
        if isinstance(default, float):
            return _f(name, float(default))
        return _s(name, str(default))
    return getattr(C, name, default)


# Optional mint normalizer (must be Any->str to avoid conditional signature mismatch)
def _normalize_mint_fallback(x: Any) -> str:
    return str(x or "").strip()


_normalize_mint: Callable[[Any], str] = _normalize_mint_fallback
try:
    from utils_pkg.mints import normalize_mint as _nm  # type: ignore

    _normalize_mint = _nm  # type: ignore[assignment]
except Exception:  # pragma: no cover
    _normalize_mint = _normalize_mint_fallback


# Optional cheap viability filter (scanner-level sanity, not final gate)
def _is_viable_pair_fallback(_: dict[str, Any]) -> bool:
    return True


_is_viable_pair: Callable[[dict[str, Any]], bool] = _is_viable_pair_fallback
try:
    from utils_pkg.pair_filters import is_viable_pair as _ivp  # type: ignore

    _is_viable_pair = _ivp  # type: ignore[assignment]
except Exception:  # pragma: no cover
    try:
        from utils_pkg.pump_filters import is_viable_pair as _ivp2  # type: ignore

        _is_viable_pair = _ivp2  # type: ignore[assignment]
    except Exception:  # pragma: no cover
        _is_viable_pair = _is_viable_pair_fallback


# Optional risk snapshot (signature must stay stable across variants)
async def _risk_capacity_snapshot_fallback(redis: Any | None = None) -> dict[str, Any]:
    _ = redis
    return {"risk_capacity": 1.0, "risk_shutdown": False, "risk_mode": "normal"}


_risk_capacity_snapshot: Callable[[Any | None], Awaitable[dict[str, Any]]] = (
    _risk_capacity_snapshot_fallback
)
try:
    from utils_pkg.risk_manager import risk_capacity_snapshot as _rcs  # type: ignore

    _risk_capacity_snapshot = _rcs  # type: ignore[assignment]
except Exception:  # pragma: no cover
    _risk_capacity_snapshot = _risk_capacity_snapshot_fallback


# Optional regime snapshot source (tags only)
try:
    from utils_pkg.regime import regime_state  # type: ignore
except Exception:  # pragma: no cover

    class _DummyRegime:
        @staticmethod
        def get() -> str:
            return "unknown"

        @staticmethod
        async def features() -> dict[str, Any]:
            return {
                "regime_label": "unknown",
                "regime_label_numeric": 0.0,
                "trend_score": 0.0,
                "volatility_score": 0.0,
                "liquidity_score": 0.5,
                "manipulation_risk": 0.0,
            }

    regime_state = _DummyRegime()  # type: ignore


# Optional alerting (best-effort)
AlertManagerClass: type[Any] | None
try:
    from utils_pkg.alert_manager import AlertManager as _AM  # type: ignore

    AlertManagerClass = _AM  # type: ignore[assignment]
except Exception:  # pragma: no cover
    AlertManagerClass = None


# Optional structured logging for training/debug
try:
    from utils_pkg.data_logger import log_event, log_raydium_candidate  # type: ignore
except Exception:  # pragma: no cover

    async def log_event(*_: Any, **__: Any) -> None:  # type: ignore
        return None

    def log_raydium_candidate(*_: Any, **__: Any) -> None:  # type: ignore
        return None


# =============================================================================
# Constants
# =============================================================================
_SOL_MINT = "So11111111111111111111111111111111111111112"
_USDC_MINT = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"


# =============================================================================
# Settings
# =============================================================================
@dataclass(frozen=True)
class Settings:
    enabled: bool
    mode: str  # off/simple/full

    # Dexscreener search
    base_url: str
    alt_bases: tuple[str, ...]
    search_path: str
    queries: tuple[str, ...]
    search_limit: int
    fetch_concurrency: int

    # HTTP
    ua: str
    timeout_s: float
    retries: int
    backoff_s: float
    trust_env: bool
    http_proxy: str
    conn_limit: int
    rps_limit: float

    # Selection
    out_limit: int
    require_quote: bool
    allowed_quotes: frozenset[str]

    # Cheap prefilters (scanner sanity only; entry layer still decides)
    hard_min_liq_usd: float
    hard_max_age_min: float
    min_buys_5m: int
    min_vol5_usd: float
    max_neg_chg5_pct: float

    # Redis dedupe (optional)
    seen_ttl_s: int

    # Observability / alerting
    debug_samples: bool
    log_rejects: bool
    reject_log_every_s: float
    sample_n: int

    momo_alerts: bool
    max_momo_alerts: int
    momo_dedup_s: float


_SETTINGS_CACHE: tuple[float, Settings | None] = (0.0, None)
_SETTINGS_TTL_S = 1.0


def _compute_settings() -> Settings:
    phase = int(_i("MASTER_PHASE", int(_cfg("MASTER_PHASE", 7) or 7)))
    default_mode = "off" if phase <= 0 else ("simple" if phase <= 2 else "full")
    mode = _s("RAYDIUM_MODE", default_mode).lower().strip()
    if mode not in {"off", "simple", "full"}:
        mode = "full"

    enabled = bool(_cfg("ENABLE_RAYDIUM_SCANNER", True)) and _b("RAYDIUM_ENABLED", mode != "off")

    base = _s("DEXSCREENER_BASE_URL", "https://api.dexscreener.com").rstrip("/")
    alt_raw = [
        x.strip().rstrip("/") for x in _s("DEXSCREENER_ALT_BASE_URLS", "").split(",") if x.strip()
    ]
    alt_bases = tuple([b for b in alt_raw if b and b != base][:5])

    path = _s("RAYDIUM_DEXSEARCH_PATH", "/latest/dex/search").strip() or "/latest/dex/search"

    queries = tuple(
        [q.strip() for q in _s("RAYDIUM_SEARCH_QUERIES", "raydium,solana").split(",") if q.strip()][
            :20
        ]
        or ["raydium"]
    )
    search_limit = max(25, _i("RAYDIUM_SEARCH_LIMIT", 250))
    fetch_concurrency = max(1, _i("RAYDIUM_FETCH_CONCURRENCY", 3))

    ua = _s("SCANNER_UA", "Mozilla/5.0 SniperBot/1.0 (+raydium)")
    timeout_s = max(5.0, _f("RAYDIUM_HTTP_TIMEOUT_SEC", 12.0))
    retries = max(1, _i("RAYDIUM_HTTP_RETRIES", 3))
    backoff_s = max(0.15, _f("RAYDIUM_HTTP_BACKOFF_SEC", 0.6))
    trust_env = _b("RAYDIUM_TRUST_ENV", True)
    http_proxy = _s("RAYDIUM_HTTP_PROXY", "") or _s("SCANNER_HTTP_PROXY", "")
    conn_limit = max(10, _i("RAYDIUM_HTTP_CONN_LIMIT", 64))
    rps_limit = max(0.0, _f("RAYDIUM_RPS_LIMIT", 0.0))

    out_limit = max(1, _i("RAYDIUM_LIMIT", 40))

    require_quote = _b("RAYDIUM_REQUIRE_SOL_OR_USDC_QUOTE", True)
    allowed_quotes = frozenset(
        {
            q.strip().upper()
            for q in _s("RAYDIUM_ALLOWED_QUOTES", "SOL,USDC").split(",")
            if q.strip()
        }
        or {"SOL", "USDC"}
    )

    hard_min_liq_usd = max(0.0, _f("RAYDIUM_HARD_MIN_LIQ_USD", 0.0))
    hard_max_age_min = max(0.0, _f("RAYDIUM_HARD_MAX_AGE_MIN", 0.0))
    min_buys_5m = max(0, _i("RAYDIUM_MIN_BUYS_5M", 0))
    min_vol5_usd = max(0.0, _f("RAYDIUM_MIN_VOL5_USD", 0.0))
    max_neg_chg5_pct = _f("RAYDIUM_MAX_NEG_CHG5_PCT", -35.0)

    seen_ttl_s = max(0, _i("RAYDIUM_SEEN_TTL_S", 0))

    debug_samples = _b("RAYDIUM_DEBUG_META_SAMPLE", True)
    log_rejects = _b("RAYDIUM_LOG_REJECTS", True)
    reject_log_every_s = max(1.0, float(_f("RAYDIUM_REJECT_LOG_EVERY_SEC", 10.0)))
    sample_n = max(0, _i("RAYDIUM_REJECT_SAMPLE_N", 2))

    momo_alerts = _b("RAYDIUM_SEND_MOMO_ALERTS", True)
    max_momo_alerts = max(0, _i("RAYDIUM_MAX_ALERTS_PER_SCAN", 3))
    momo_dedup_s = max(10.0, _f("RAYDIUM_ALERT_DEDUP_SEC", 180.0))

    return Settings(
        enabled=enabled,
        mode=mode,
        base_url=base,
        alt_bases=alt_bases,
        search_path=path,
        queries=queries,
        search_limit=search_limit,
        fetch_concurrency=fetch_concurrency,
        ua=ua,
        timeout_s=timeout_s,
        retries=retries,
        backoff_s=backoff_s,
        trust_env=trust_env,
        http_proxy=http_proxy,
        conn_limit=conn_limit,
        rps_limit=rps_limit,
        out_limit=out_limit,
        require_quote=require_quote,
        allowed_quotes=allowed_quotes,
        hard_min_liq_usd=hard_min_liq_usd,
        hard_max_age_min=hard_max_age_min,
        min_buys_5m=min_buys_5m,
        min_vol5_usd=min_vol5_usd,
        max_neg_chg5_pct=max_neg_chg5_pct,
        seen_ttl_s=seen_ttl_s,
        debug_samples=debug_samples,
        log_rejects=log_rejects,
        reject_log_every_s=reject_log_every_s,
        sample_n=sample_n,
        momo_alerts=momo_alerts,
        max_momo_alerts=max_momo_alerts,
        momo_dedup_s=momo_dedup_s,
    )


def _settings() -> Settings:
    now = time.time()
    ts, st = _SETTINGS_CACHE
    if st is None or (now - ts) >= _SETTINGS_TTL_S:
        st = _compute_settings()
        globals()["_SETTINGS_CACHE"] = (now, st)
    return st


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
    limit: int | None = None


def _looks_like_queue(x: Any) -> bool:
    return x is not None and (
        callable(getattr(x, "put", None)) or callable(getattr(x, "put_nowait", None))
    )


def _looks_like_redis(x: Any) -> bool:
    if x is None or isinstance(x, asyncio.Event) or _looks_like_queue(x):
        return False
    return any(callable(getattr(x, nm, None)) for nm in ("get", "set", "setex", "expire", "ping"))


def _parse_ctx(*args: Any, **kwargs: Any) -> _CallCtx:
    ctx = _CallCtx()

    for k in ("stop_evt", "stop_event", "stop"):
        v = kwargs.get(k)
        if isinstance(v, asyncio.Event):
            ctx.stop_evt = v
            break

    for k in ("session", "sess", "http", "client"):
        v = kwargs.get(k)
        if v is not None:
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

    lim = kwargs.get("limit", None)
    if isinstance(lim, int) and not isinstance(lim, bool):
        ctx.limit = lim

    # positional: accept any order
    for a in args:
        if ctx.stop_evt is None and isinstance(a, asyncio.Event):
            ctx.stop_evt = a
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
            continue
        if ctx.limit is None and isinstance(a, int) and not isinstance(a, bool):
            ctx.limit = a
            continue

    return ctx


# =============================================================================
# Redis helpers (optional; safe for sync/async clients)
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
# Stats / rejects (no silent no-buy)
# =============================================================================
@dataclass
class ScanStats:
    t0: float
    raw_pairs: int = 0
    normalized: int = 0
    passed: int = 0
    rejects: dict[str, int] = field(default_factory=dict)
    samples: dict[str, list[dict[str, Any]]] = field(default_factory=dict)

    def rej(
        self, reason: str, sample: dict[str, Any] | None = None, *, st: Settings | None = None
    ) -> None:
        r = reason or "reject_unknown"
        self.rejects[r] = int(self.rejects.get(r, 0)) + 1
        n = int(getattr(st, "sample_n", 0) or 0) if st else 0
        if sample is not None and n > 0:
            arr = self.samples.get(r) or []
            if len(arr) < n:
                arr.append(sample)
                self.samples[r] = arr


_REJECT_LAST_TS = 0.0


def _maybe_log_rejects(st: Settings, stats: ScanStats, *, force: bool = False) -> None:
    global _REJECT_LAST_TS
    if not st.log_rejects or not stats.rejects:
        return
    now = time.time()
    if (not force) and (now - float(_REJECT_LAST_TS)) < float(st.reject_log_every_s):
        return
    _REJECT_LAST_TS = now

    top = sorted(stats.rejects.items(), key=lambda kv: kv[1], reverse=True)[:12]
    payload: dict[str, Any] = {
        "raw_pairs": stats.raw_pairs,
        "normalized": stats.normalized,
        "passed": stats.passed,
        "top_rejects": top,
    }
    if st.sample_n > 0 and stats.samples:
        payload["samples"] = stats.samples

    LOG.event("RAY_REJECT_SUMMARY", **payload)


# =============================================================================
# OFFLINE candidates (no env keys; purely signature flag)
# =============================================================================
_B58_ALPH = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
_OFFLINE_TS = 1_700_000_000  # fixed deterministic timestamp


def _b58encode(data: bytes) -> str:
    if not data:
        return ""
    n = int.from_bytes(data, "big")
    out: list[str] = []
    while n > 0:
        n, r = divmod(n, 58)
        out.append(_B58_ALPH[r])
    pad = 0
    for b in data:
        if b == 0:
            pad += 1
        else:
            break
    return ("1" * pad) + "".join(reversed(out or ["1"]))


def _stable_pubkey(tag: str) -> str:
    h = hashlib.sha256(tag.encode("utf-8")).digest()
    return _b58encode(h)


def _offline_meta(limit: int) -> list[dict[str, Any]]:
    mint0 = _stable_pubkey("raydium-offline-mint-0")
    mint1 = _stable_pubkey("raydium-offline-mint-1")
    pair0 = _stable_pubkey("raydium-offline-pair-0")
    pair1 = _stable_pubkey("raydium-offline-pair-1")

    def _one(
        mint: str,
        pair: str,
        sym: str,
        liq: float,
        vol5: float,
        buys: int,
        sells: int,
        chg5: float,
        age: float,
    ) -> dict[str, Any]:
        ts = float(_OFFLINE_TS)
        buy_ratio = float(buys) / float(max(1, buys + sells))
        vol_to_liq = (float(vol5) / max(1.0, float(liq))) if liq > 0 else 0.0
        momo = (
            (buy_ratio * 100.0)
            + (min(2.0, vol_to_liq) * 25.0)
            + (max(-20.0, min(20.0, chg5)) * 0.8)
        )
        url = f"https://dexscreener.com/solana/{pair}"
        return {
            "scanner": "raydium_scanner",
            "scanner_id": "raydium_scanner",
            "source": "raydium",
            "src": "raydium",
            "dex": "raydium",
            "dexId": "raydium",
            "chain": "solana",
            "mint": mint,
            "symbol": sym,
            "sym": sym,
            "quote_symbol": "SOL",
            "quote_mint": _SOL_MINT,
            "pair_address": pair,
            "pairAddress": pair,
            "pair_url": url,
            "url": url,
            "scan_ts": ts,
            "ts": ts,
            "liq_usd": float(liq),
            "vol_5m_usd": float(vol5),
            "vol_1h_usd": float(vol5) * 6.0,
            "vol_24h_usd": float(vol5) * 288.0,
            "buys_5m": int(buys),
            "sells_5m": int(sells),
            "txns_5m": int(buys + sells),
            "price_usd": 0.000042,
            "price_change_5m_pct": float(chg5),
            "price_change_1h_pct": float(chg5) * 1.8,
            "price_change_24h_pct": float(chg5) * 4.0,
            "token_age_min": float(age),
            "buy_ratio_5m": float(buy_ratio),
            "vol_to_liq_5m": float(vol_to_liq),
            "momo_hint": float(momo),
            "offline": True,
            "raw_pair": {},
        }

    metas = [
        _one(mint0, pair0, "RAY_OFFLINE_A", 125_000.0, 8_400.0, 22, 12, 2.9, 7.0),
        _one(mint1, pair1, "RAY_OFFLINE_B", 310_000.0, 19_500.0, 44, 18, 5.8, 3.5),
    ]
    return metas[: max(1, int(limit))]


def _offline_enabled(
    offline: bool, dry: bool | None, no_network: bool | None, test_mode: bool | None
) -> bool:
    return bool(offline) or bool(dry) or bool(no_network) or bool(test_mode)


# =============================================================================
# Regime snapshot (tags only)
# =============================================================================
async def _regime_snapshot() -> dict[str, Any]:
    try:
        feats = await regime_state.features()
        return {
            "regime_label": str(feats.get("regime_label", "")) or "unknown",
            "regime_label_numeric": float(feats.get("regime_label_numeric", 0.0) or 0.0),
            "manipulation_risk": float(feats.get("manipulation_risk", 0.0) or 0.0),
            "trend_score": float(feats.get("trend_score", 0.0) or 0.0),
            "volatility_score": float(feats.get("volatility_score", 0.0) or 0.0),
            "liquidity_score": float(feats.get("liquidity_score", 0.5) or 0.5),
        }
    except Exception:
        with contextlib.suppress(Exception):
            return {
                "regime_label": str(getattr(regime_state, "get", lambda: "unknown")() or "unknown"),
                "regime_label_numeric": 0.0,
                "manipulation_risk": 0.0,
            }
        return {"regime_label": "unknown", "regime_label_numeric": 0.0, "manipulation_risk": 0.0}


# =============================================================================
# HTTP helpers
# =============================================================================
def _headers(st: Settings) -> dict[str, str]:
    return {
        "User-Agent": st.ua,
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
    }


_SSLCTX: ssl.SSLContext | None = None


def _sslctx() -> ssl.SSLContext | None:
    global _SSLCTX
    if _SSLCTX is not None:
        return _SSLCTX
    with contextlib.suppress(Exception):
        if certifi is not None:
            _SSLCTX = ssl.create_default_context(cafile=certifi.where())  # type: ignore[attr-defined]
            return _SSLCTX
    _SSLCTX = None
    return None


def _search_url(base: str, path: str) -> str:
    p = path if path.startswith("/") else ("/" + path)
    return f"{base}{p}"


class _RpsGate:
    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._last = 0.0
        self._rps = 0.0

    def set(self, rps: float) -> None:
        self._rps = float(max(0.0, rps))

    async def wait(self) -> None:
        rps = float(self._rps)
        if rps <= 0:
            return
        async with self._lock:
            now = time.monotonic()
            min_dt = 1.0 / max(0.0001, rps)
            dt = now - self._last
            if dt < min_dt:
                await asyncio.sleep(min_dt - dt)
            self._last = time.monotonic()


_RPS = _RpsGate()


async def _fetch_json(
    session: Any,
    st: Settings,
    url: str,
    *,
    params: dict[str, Any] | None = None,
    stop_evt: asyncio.Event | None = None,
) -> tuple[dict[str, Any] | None, int, str]:
    status = 0
    first = ""

    if aiohttp is None:
        return None, 0, "aiohttp_missing"

    for attempt in range(1, st.retries + 1):
        if stop_evt is not None and stop_evt.is_set():
            return None, 0, "stopped"

        await _RPS.wait()

        try:
            async with session.get(
                url,
                params=params,
                headers=_headers(st),
                timeout=aiohttp.ClientTimeout(total=st.timeout_s),  # type: ignore[attr-defined]
                proxy=(st.http_proxy or None),
                ssl=_sslctx(),
            ) as r:
                status = int(getattr(r, "status", 0) or 0)
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
                    return None, status, first

                if status == 429:
                    ra = (getattr(r, "headers", {}) or {}).get("Retry-After", "")
                    ra = (ra or "").strip()
                    wait = (
                        float(ra)
                        if ra.replace(".", "", 1).isdigit()
                        else (st.backoff_s * attempt * 2.0)
                    )
                    wait = _clamp(wait, 0.25, 8.0)
                    await asyncio.sleep(wait)
                else:
                    await asyncio.sleep(st.backoff_s * attempt * (0.85 + 0.3 * random.random()))
        except asyncio.CancelledError:
            raise
        except (TimeoutError, Exception) as e:
            LOG.every(
                "RAY_HTTP_ERR",
                n=10,
                level=logging.INFO,
                msg="RAY_HTTP_ERR",
                err=type(e).__name__,
                url=url,
                attempt=attempt,
                retries=st.retries,
            )
            if attempt < st.retries:
                await asyncio.sleep(st.backoff_s * attempt * (0.85 + 0.3 * random.random()))

    return None, status, first


async def _fetch_pairs(
    session: Any,
    st: Settings,
    stats: ScanStats,
    *,
    stop_evt: asyncio.Event | None,
) -> list[dict[str, Any]]:
    sem = asyncio.Semaphore(max(1, st.fetch_concurrency))
    out: list[dict[str, Any]] = []
    seen: set[str] = set()

    bases = (st.base_url,) + tuple([b for b in st.alt_bases if b and b != st.base_url])
    bases = bases[:5] or (st.base_url,)
    queries = st.queries[:20] or ("raydium",)

    async def _one_query(q: str) -> None:
        for base in bases:
            if stop_evt is not None and stop_evt.is_set():
                return
            try:
                url = _search_url(base, st.search_path)
                params = {"q": q, "limit": st.search_limit}
                async with sem:
                    data, status, _first = await _fetch_json(
                        session, st, url, params=params, stop_evt=stop_evt
                    )

                if not data:
                    stats.rej("fetch_empty", {"q": q, "status": status, "base": base}, st=st)
                    continue

                pairs = data.get("pairs") or []
                if not isinstance(pairs, list):
                    stats.rej("fetch_pairs_nolist", {"q": q, "base": base}, st=st)
                    continue

                for p in pairs:
                    if not isinstance(p, dict):
                        stats.rej("pair_nondict", {"q": q}, st=st)
                        continue
                    if (p.get("chainId") or "").lower() != "solana":
                        continue
                    if (p.get("dexId") or "").lower() != "raydium":
                        continue

                    pid = str((p.get("pairAddress") or "")).strip()
                    if not pid:
                        pid = str(((p.get("baseToken") or {}).get("address")) or "").strip()
                    pid_l = pid.lower()
                    if not pid_l or pid_l in seen:
                        continue
                    seen.add(pid_l)
                    out.append(p)

                return
            except asyncio.CancelledError:
                raise
            except Exception as e:
                stats.rej("fetch_exc", {"q": q, "base": base, "err": _short(e)}, st=st)

    tasks = [asyncio.create_task(_one_query(q)) for q in queries]
    if tasks:
        await asyncio.gather(*tasks)
    return out


# =============================================================================
# Normalization helpers
# =============================================================================
def _nzf(v: Any, d: float = 0.0) -> float:
    try:
        x = float(v)
        return d if x != x else x
    except Exception:
        return d


def _nzi(v: Any, d: int = 0) -> int:
    try:
        return int(v)
    except Exception:
        return d


def _age_min(created_ms_or_s: Any) -> float | None:
    try:
        if created_ms_or_s in (None, "", 0):
            return None
        t = float(created_ms_or_s)
        if t <= 0:
            return None
        if t > 10_000_000_000:
            t = t / 1000.0
        return max(0.0, (time.time() - t) / 60.0)
    except Exception:
        return None


def _buy_ratio(buys: int, sells: int) -> float:
    total = max(1, buys + sells)
    return float(buys) / float(total)


def _quote_allowed(st: Settings, quote_symbol: str, quote_mint: str) -> bool:
    if not st.require_quote:
        return True
    qs = (quote_symbol or "").strip().upper()
    qm = (quote_mint or "").strip()
    if qs and qs in st.allowed_quotes:
        return True
    return qm in {_SOL_MINT, _USDC_MINT}


def _metric_volume(vol: dict[str, Any], key: str, alt: str | None = None) -> float:
    raw = vol.get(key)
    if isinstance(raw, dict):
        raw = raw.get("usd")
    if raw is None and alt:
        raw = vol.get(alt)
        if isinstance(raw, dict):
            raw = raw.get("usd")
    return _nzf(raw, 0.0)


def _normalize_pair(st: Settings, p: dict[str, Any], stats: ScanStats) -> dict[str, Any] | None:
    try:
        base = p.get("baseToken") or {}
        quote = p.get("quoteToken") or {}

        mint = _normalize_mint((base.get("address") or ""))
        if not mint:
            stats.rej("reject_no_mint", st=st)
            return None

        symbol = str((base.get("symbol") or "")).strip() or mint[:6]
        quote_symbol = str((quote.get("symbol") or "")).strip().upper()
        quote_mint = str((quote.get("address") or "")).strip()

        if not _quote_allowed(st, quote_symbol, quote_mint):
            stats.rej("reject_quote", {"mint": mint, "quote": quote_symbol}, st=st)
            return None

        liq = p.get("liquidity") or {}
        liq_usd = _nzf((liq.get("usd") if isinstance(liq, dict) else None), 0.0)

        txs = p.get("txns") or {}
        m5 = (txs.get("m5") or txs.get("5m") or {}) if isinstance(txs, dict) else {}
        buys_5m = _nzi(m5.get("buys"), 0)
        sells_5m = _nzi(m5.get("sells"), 0)

        vol = p.get("volume") or {}
        vol_d = vol if isinstance(vol, dict) else {}
        vol_5m = _metric_volume(vol_d, "m5", "5m")
        vol_1h = _metric_volume(vol_d, "h1", "1h")
        vol_24h = _metric_volume(vol_d, "h24", "24h")

        pc = p.get("priceChange") or {}
        pc_d = pc if isinstance(pc, dict) else {}
        chg_5m = _nzf(pc_d.get("m5") or pc_d.get("5m"), 0.0)
        chg_1h = _nzf(pc_d.get("h1") or pc_d.get("1h"), 0.0)
        chg_24h = _nzf(pc_d.get("h24") or pc_d.get("24h"), 0.0)

        price_usd = _nzf(p.get("priceUsd") or p.get("price"), 0.0)
        created = p.get("pairCreatedAt") or p.get("pairCreatedAtMs") or p.get("pairCreatedAtTs")
        age = _age_min(created)
        token_age_min = float(age) if age is not None else 9999.0

        pair_address = str((p.get("pairAddress") or "")).strip()
        pair_url = p.get("url") or p.get("pairUrl")
        if not pair_url:
            pair_url = (
                f"https://dexscreener.com/solana/{pair_address}"
                if pair_address
                else f"https://dexscreener.com/solana/{mint}"
            )

        buy_ratio_5m = _buy_ratio(buys_5m, sells_5m)
        vol_to_liq_5m = (vol_5m / max(1.0, liq_usd)) if liq_usd > 0 else 0.0

        momo_hint = (
            (buy_ratio_5m * 100.0)
            + (min(2.0, vol_to_liq_5m) * 25.0)
            + (max(-20.0, min(20.0, chg_5m)) * 0.8)
        )

        ts = float(time.time())

        meta: dict[str, Any] = {
            "scanner": "raydium_scanner",
            "scanner_id": "raydium_scanner",
            "source": "raydium",
            "src": "raydium",
            "dex": "raydium",
            "dexId": "raydium",
            "chain": "solana",
            "mint": mint,
            "symbol": symbol,
            "sym": symbol,
            "quote_symbol": quote_symbol,
            "quote_mint": quote_mint,
            "pair_address": pair_address,
            "pairAddress": pair_address,
            "pair_url": pair_url,
            "url": pair_url,
            "scan_ts": ts,
            "ts": ts,
            "liq_usd": float(liq_usd),
            "vol_5m_usd": float(vol_5m),
            "vol_1h_usd": float(vol_1h),
            "vol_24h_usd": float(vol_24h),
            "buys_5m": int(buys_5m),
            "sells_5m": int(sells_5m),
            "txns_5m": int(buys_5m + sells_5m),
            "price_usd": float(price_usd),
            "price_change_5m_pct": float(chg_5m),
            "price_change_1h_pct": float(chg_1h),
            "price_change_24h_pct": float(chg_24h),
            "token_age_min": float(token_age_min),
            "buy_ratio_5m": float(buy_ratio_5m),
            "vol_to_liq_5m": float(vol_to_liq_5m),
            "momo_hint": float(momo_hint),
            "raw_pair": p,
        }

        if st.hard_min_liq_usd > 0 and meta["liq_usd"] < st.hard_min_liq_usd:
            stats.rej("reject_hard_min_liq", {"mint": mint, "liq": meta["liq_usd"]}, st=st)
            return None
        if st.hard_max_age_min > 0 and meta["token_age_min"] > st.hard_max_age_min:
            stats.rej("reject_hard_max_age", {"mint": mint, "age": meta["token_age_min"]}, st=st)
            return None
        if st.min_buys_5m > 0 and meta["buys_5m"] < st.min_buys_5m:
            stats.rej("reject_min_buys5", {"mint": mint, "buys5": meta["buys_5m"]}, st=st)
            return None
        if st.min_vol5_usd > 0 and meta["vol_5m_usd"] < st.min_vol5_usd:
            stats.rej("reject_min_vol5", {"mint": mint, "vol5": meta["vol_5m_usd"]}, st=st)
            return None
        if meta["price_change_5m_pct"] <= st.max_neg_chg5_pct:
            stats.rej(
                "reject_dump_chg5", {"mint": mint, "chg5": meta["price_change_5m_pct"]}, st=st
            )
            return None

        try:
            if not _is_viable_pair(meta):
                stats.rej("reject_viable_pair", {"mint": mint}, st=st)
                return None
        except Exception:
            stats.rej("reject_viable_pair_exc", {"mint": mint}, st=st)
            return None

        return meta
    except asyncio.CancelledError:
        raise
    except Exception as e:
        stats.rej("reject_normalize_exc", {"err": _short(e)}, st=st)
        return None


def _rank_key(meta: dict[str, Any]) -> tuple[float, float, float, float]:
    return (
        float(meta.get("vol_5m_usd") or 0.0),
        float(meta.get("liq_usd") or 0.0),
        float(meta.get("buy_ratio_5m") or 0.0),
        float(meta.get("momo_hint") or 0.0),
    )


# =============================================================================
# Candidate assembly (signature-aware, cached)
# =============================================================================
_CAND_FACTORY: Callable[[dict[str, Any]], CandidateT] | None = None


def _build_candidate_factory() -> Callable[[dict[str, Any]], CandidateT]:
    if CandidateClass is None:  # pragma: no cover
        raise RuntimeError("scanner.registry.Candidate missing — cannot emit candidates")

    # Prefer classmethods if present
    for fn_name in ("from_meta", "from_dict", "from_mapping"):
        fn = getattr(CandidateClass, fn_name, None)
        if callable(fn):

            def _wrap(meta: dict[str, Any], _fn: Any = fn) -> CandidateT:
                return _fn(meta)

            return _wrap

    # Signature-aware ctor mapping
    try:
        sig = inspect.signature(CandidateClass)
        params = set(sig.parameters.keys())
    except Exception:
        params = set()

    def _ctor(meta: dict[str, Any]) -> CandidateT:
        if CandidateClass is None:  # pragma: no cover
            raise RuntimeError("CandidateClass missing")

        kwargs: dict[str, Any] = {}

        def _set_if(key: str, value: Any) -> None:
            if key in params:
                kwargs[key] = value

        _set_if("scanner", meta.get("scanner"))
        _set_if("source", _coerce_source(meta.get("source") or "raydium"))
        _set_if("dex", meta.get("dex"))
        _set_if("chain", meta.get("chain"))
        _set_if("mint", meta.get("mint"))
        _set_if("symbol", meta.get("symbol"))
        _set_if("quote_mint", meta.get("quote_mint"))
        _set_if("quote_symbol", meta.get("quote_symbol"))
        _set_if("pair_address", meta.get("pair_address"))
        _set_if("pair_url", meta.get("pair_url"))
        _set_if("ts", meta.get("scan_ts"))
        _set_if("scan_ts", meta.get("scan_ts"))
        _set_if("meta", meta)
        _set_if("raw", meta)
        _set_if("data", meta)
        _set_if("extra", meta)

        # If we can match any parameters, try that first.
        if kwargs:
            with contextlib.suppress(TypeError):
                return CandidateClass(**kwargs)  # type: ignore[misc]

        # Fallback: Any-safe progressive ctor attempts (keeps mypy happy and runtime resilient)
        mint = str(meta.get("mint") or "").strip()
        symbol = str(meta.get("symbol") or "").strip() or (mint[:6] if mint else "UNKNOWN")
        src = _coerce_source(meta.get("source") or "raydium")

        cls_any: Any = CandidateClass
        variants: list[dict[str, Any]] = [
            {"mint": mint, "symbol": symbol, "source": src, "meta": meta},
            {"mint": mint, "symbol": symbol, "meta": meta},
            {"mint": mint, "symbol": symbol, "source": src},
            {"mint": mint, "symbol": symbol},
            {"mint": mint},
            {"meta": meta},
        ]
        for kw in variants:
            try:
                return cls_any(**kw)
            except TypeError:
                continue

        # Final fallback (should basically never happen)
        return cls_any(meta)

    return _ctor


def _make_candidate(meta: dict[str, Any]) -> CandidateT:
    global _CAND_FACTORY
    if _CAND_FACTORY is None:
        _CAND_FACTORY = _build_candidate_factory()
    return _CAND_FACTORY(meta)


# =============================================================================
# Public API
# =============================================================================
async def scan_raydium(
    *args: Any,
    offline: bool = False,
    dry: bool | None = None,
    no_network: bool | None = None,
    test_mode: bool | None = None,
    **kwargs: Any,
) -> list[CandidateT]:
    st = _settings()
    _RPS.set(st.rps_limit)

    if _disabled() or (not st.enabled) or st.mode == "off":
        return []

    ctx = _parse_ctx(*args, **kwargs)
    stop_evt = ctx.stop_evt

    raw_limit = ctx.limit if ctx.limit is not None else kwargs.get("limit", None)
    out_limit = max(1, _coerce_int(raw_limit, st.out_limit))

    # OFFLINE mode: deterministic candidates, no HTTP.
    if _offline_enabled(offline, dry, no_network, test_mode):
        if CandidateClass is None:
            return []
        metas = _offline_meta(out_limit)
        return [_make_candidate(m) for m in metas]

    if aiohttp is None:
        if _fast_fail():
            raise RuntimeError("aiohttp is required for raydium_scanner but is not installed")
        LOG.once("RAY_NO_AIOHTTP", ttl=60, msg="aiohttp missing; raydium scanner disabled")
        return []

    if CandidateClass is None:
        if _fast_fail():
            raise RuntimeError("scanner.registry.Candidate missing (cannot emit candidates)")
        LOG.once("RAY_NO_CANDIDATE", ttl=60, msg="Candidate missing; raydium scanner disabled")
        return []

    stats = ScanStats(t0=time.time())

    own_session = False
    sess: Any = None
    if ctx.session is not None:
        sess = ctx.session
    else:
        with contextlib.suppress(Exception):
            _SETUP_LOGGING(service="scanner.raydium_scanner")
        sslctx = _sslctx()
        timeout = aiohttp.ClientTimeout(total=float(st.timeout_s) + 3.0)  # type: ignore[attr-defined]
        conn = aiohttp.TCPConnector(ssl=sslctx, limit=int(st.conn_limit))  # type: ignore[attr-defined]
        sess = aiohttp.ClientSession(
            headers=_headers(st),
            connector=conn,
            trust_env=bool(st.trust_env),
            timeout=timeout,
        )  # type: ignore[attr-defined]
        own_session = True

    try:
        if stop_evt is not None and stop_evt.is_set():
            return []

        # risk capacity (tags only; do NOT trade here)
        try:
            risk_ctx = await _risk_capacity_snapshot(ctx.redis)
        except Exception:
            risk_ctx = {"risk_capacity": 1.0, "risk_shutdown": False, "risk_mode": "normal"}
            stats.rej("risk_snapshot_exc", st=st)

        risk_capacity = _clamp(float(risk_ctx.get("risk_capacity", 1.0) or 1.0), 0.0, 1.0)
        risk_shutdown = bool(risk_ctx.get("risk_shutdown", False))
        risk_mode = str(risk_ctx.get("risk_mode", "normal") or "normal")

        if risk_shutdown or risk_capacity <= 0.0:
            LOG.every(
                "RAY_RISK_SHUTDOWN",
                n=10,
                level=logging.INFO,
                msg="RAY_RISK_SHUTDOWN",
                risk_shutdown=int(risk_shutdown),
                risk_capacity=round(risk_capacity, 3),
                risk_mode=risk_mode,
            )
            return []

        regime = await _regime_snapshot()

        LOG.every(
            "RAY_SCAN_START",
            n=5,
            level=logging.INFO,
            msg="RAY_SCAN_START",
            mode=st.mode,
            queries=",".join(st.queries),
            out_limit=out_limit,
            hard_min_liq=st.hard_min_liq_usd,
            hard_max_age=st.hard_max_age_min,
            risk_capacity=round(risk_capacity, 3),
            risk_mode=risk_mode,
            regime=regime.get("regime_label"),
            manip=round(float(regime.get("manipulation_risk", 0.0)), 3),
        )

        pairs = await _fetch_pairs(sess, st, stats, stop_evt=stop_evt)
        stats.raw_pairs = len(pairs)

        # Normalize + dedupe by mint (keep best per mint)
        best_by_mint: dict[str, dict[str, Any]] = {}
        for p in pairs:
            if stop_evt is not None and stop_evt.is_set():
                break
            m = _normalize_pair(st, p, stats)
            if not m:
                continue

            # attach context tags (cheap)
            m["risk_capacity"] = risk_capacity
            m["risk_mode"] = risk_mode
            m["regime"] = regime

            mint = str(m.get("mint") or "").strip()
            if not mint:
                stats.rej("reject_no_mint_post", st=st)
                continue

            cur = best_by_mint.get(mint)
            if cur is None or _rank_key(m) > _rank_key(cur):
                best_by_mint[mint] = m

        metas = list(best_by_mint.values())
        stats.normalized = len(metas)

        # Optional redis seen-dedupe (default off)
        if st.seen_ttl_s > 0 and _redis_ok(ctx.redis) and metas:
            filtered: list[dict[str, Any]] = []
            for m in metas:
                mint = str(m.get("mint") or "").strip()
                if not mint:
                    continue
                k = f"ray:seen:{mint}"
                if await _redis_get(ctx.redis, k):
                    stats.rej("reject_seen_recent", {"mint": mint}, st=st)
                    continue
                await _redis_setex(ctx.redis, k, int(st.seen_ttl_s), "1")
                filtered.append(m)
            metas = filtered
            stats.normalized = len(metas)

        # Debug sample (human-friendly)
        if st.debug_samples and metas:
            sample = [
                {
                    "sym": m.get("symbol"),
                    "liq": m.get("liq_usd"),
                    "vol5": m.get("vol_5m_usd"),
                    "buys5": m.get("buys_5m"),
                    "sells5": m.get("sells_5m"),
                    "chg5": m.get("price_change_5m_pct"),
                    "age_min": m.get("token_age_min"),
                    "momo": round(float(m.get("momo_hint") or 0.0), 2),
                }
                for m in sorted(metas, key=_rank_key, reverse=True)[:5]
            ]
            LOG.every(
                "RAY_META_SAMPLE",
                n=10,
                level=logging.INFO,
                msg="RAY_META_SAMPLE",
                count=len(metas),
                first=sample,
            )

        # Sort + cap
        metas.sort(key=_rank_key, reverse=True)
        metas = metas[: max(1, out_limit)]

        # Apply risk capacity as a *soft* shrink (still just discovery)
        if risk_capacity < 1.0 and metas:
            allowed = max(1, int(len(metas) * risk_capacity))
            metas = metas[:allowed]

        # Convert to Candidate
        candidates: list[CandidateT] = []
        for m in metas:
            if stop_evt is not None and stop_evt.is_set():
                break
            try:
                candidates.append(_make_candidate(m))
            except asyncio.CancelledError:
                raise
            except Exception as e:
                stats.rej(
                    "reject_candidate_build", {"mint": m.get("mint"), "err": _short(e)}, st=st
                )
                if _fast_fail():
                    raise

        stats.passed = len(candidates)

        # Reject summary if nothing passed
        _maybe_log_rejects(st, stats, force=(stats.passed == 0))

        # Optional training/debug logging (best-effort)
        for m in metas[: min(25, len(metas))]:
            with contextlib.suppress(Exception):
                log_raydium_candidate(
                    mint=m.get("mint"),
                    symbol=m.get("symbol"),
                    meta=m,
                    regime=str((m.get("regime") or {}).get("regime_label") or "unknown"),
                    wallet_tag=None,
                )

        with contextlib.suppress(Exception):
            await log_event(
                "raydium_scan_summary",
                {
                    "ts": time.time(),
                    "mode": st.mode,
                    "raw_pairs": stats.raw_pairs,
                    "normalized": stats.normalized,
                    "passed": stats.passed,
                    "rejects": dict(stats.rejects),
                    "risk": dict(risk_ctx),
                    "regime": dict(regime),
                    "elapsed_ms": int((time.time() - stats.t0) * 1000),
                },
            )

        # Best-effort MOMO alerts (only top few)
        if st.momo_alerts and candidates and AlertManagerClass is not None:
            with contextlib.suppress(Exception):
                am: Any = AlertManagerClass()
                sent = 0
                for c in candidates:
                    if sent >= max(0, st.max_momo_alerts):
                        break
                    meta = (
                        getattr(c, "meta", None)
                        or getattr(c, "data", None)
                        or getattr(c, "raw", None)
                    )
                    if not isinstance(meta, dict):
                        continue

                    mint = str(meta.get("mint") or "").strip()
                    if mint and _redis_ok(ctx.redis) and st.momo_dedup_s > 0:
                        k = f"ray:momo:{mint}"
                        if await _redis_get(ctx.redis, k):
                            continue
                        await _redis_setex(ctx.redis, k, int(st.momo_dedup_s), "1")

                    out = am.send_momo_event(
                        "MOMO_OK",
                        {
                            "symbol": meta.get("symbol") or meta.get("mint"),
                            "mint": meta.get("mint"),
                            "pool": "raydium",
                            "pool_url": meta.get("pair_url"),
                            "liq_usd": meta.get("liq_usd"),
                            "vol_5m": meta.get("vol_5m_usd"),
                            "up_pct": meta.get("price_change_5m_pct"),
                            "momo_hint": meta.get("momo_hint"),
                            "risk_capacity": meta.get("risk_capacity"),
                            "risk_mode": meta.get("risk_mode"),
                            "regime": (meta.get("regime") or {}).get("regime_label"),
                        },
                    )
                    if asyncio.iscoroutine(out):
                        await out
                    sent += 1

        return candidates

    except asyncio.CancelledError:
        raise
    except Exception as e:
        LOG.every("RAY_SCAN_FAIL", n=5, level=logging.ERROR, msg="RAY_SCAN_FAIL", err=_short(e))
        if _fast_fail():
            raise
        return []
    finally:
        if own_session and sess is not None:
            with contextlib.suppress(Exception):
                await sess.close()


async def scan_once(*args: Any, **kwargs: Any) -> list[CandidateT]:
    return await scan_raydium(*args, **kwargs)


async def run(*args: Any, **kwargs: Any) -> None:
    """
    Simple run loop helper (dev/tools/legacy).
    - If q is provided: q.put(candidate)
    - If emit is provided: emit(candidate) (sync or async)

    Improvement vs old version:
    - Reuses a session for the loop if none is provided (lower overhead).
    """
    st = _settings()
    _RPS.set(st.rps_limit)

    ctx = _parse_ctx(*args, **kwargs)
    stop_evt = ctx.stop_evt or asyncio.Event()

    interval = max(0.25, float(kwargs.get("interval_s") or _f("RAYDIUM_SCAN_INTERVAL_SEC", 2.0)))
    jitter = max(0.0, float(kwargs.get("jitter_s") or _f("RAYDIUM_SCAN_JITTER_SEC", 0.35)))

    sess = ctx.session
    own_session = False
    if sess is None and aiohttp is not None:
        with contextlib.suppress(Exception):
            _SETUP_LOGGING(service="scanner.raydium_scanner")
        sslctx = _sslctx()
        timeout = aiohttp.ClientTimeout(total=float(st.timeout_s) + 3.0)  # type: ignore[attr-defined]
        conn = aiohttp.TCPConnector(ssl=sslctx, limit=int(st.conn_limit))  # type: ignore[attr-defined]
        sess = aiohttp.ClientSession(
            headers=_headers(st),
            connector=conn,
            trust_env=bool(st.trust_env),
            timeout=timeout,
        )  # type: ignore[attr-defined]
        own_session = True

    try:
        while not stop_evt.is_set():
            try:
                cands = await scan_raydium(redis=ctx.redis, session=sess, stop_evt=stop_evt)
                for c in cands:
                    if stop_evt.is_set():
                        break
                    if ctx.q is not None:
                        with contextlib.suppress(Exception):
                            out = ctx.q.put(c)
                            if asyncio.iscoroutine(out):
                                await out
                    if ctx.emit is not None:
                        with contextlib.suppress(Exception):
                            out2 = ctx.emit(c)
                            if asyncio.iscoroutine(out2):
                                await out2
                await asyncio.sleep(max(0.05, interval + random.uniform(-jitter, jitter)))
            except asyncio.CancelledError:
                raise
            except Exception as e:
                LOG.every(
                    "RAY_RUN_LOOP_ERR",
                    n=10,
                    level=logging.WARNING,
                    msg="RAY_RUN_LOOP_ERR",
                    err=_short(e),
                )
                if _fast_fail():
                    raise
                await asyncio.sleep(max(0.25, interval))
    finally:
        if own_session and sess is not None:
            with contextlib.suppress(Exception):
                await sess.close()


def scanner_health() -> dict[str, Any]:
    st = _settings()
    return {
        "name": "raydium_scanner",
        "enabled": bool(st.enabled) and (st.mode != "off") and (not _disabled()),
        "mode": st.mode,
        "base_url": st.base_url,
        "queries": list(st.queries),
        "search_limit": st.search_limit,
        "fetch_concurrency": st.fetch_concurrency,
        "rps_limit": st.rps_limit,
        "seen_ttl_s": st.seen_ttl_s,
    }


__all__ = ["run", "scan_once", "scan_raydium", "scanner_health"]
