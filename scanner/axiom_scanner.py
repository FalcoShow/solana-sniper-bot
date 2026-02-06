from __future__ import annotations

"""
scanner/axiom_scanner.py

Axiom Scanner (Solana) — V3+++ LOCK-IN (quant-grade / God-tier)
===============================================================

High-level role
---------------
Pull high-signal candidates from Axiom (web / browser-driven discovery), normalize into
a CandidateSnapshot-ish dict used across scanners, and optionally emit into a queue/callback.

Hard requirements (enforced)
----------------------------
- 24/7 safe: never hard-fail on optional deps (playwright/axiom libs). Degrade to [].
- Zero network/browser at import time. (No importing heavy browser deps at import.)
- Orchestrator-friendly signatures: accepts (redis/stop_evt/q/emit) in any order.
- Emits BOTH canonical keys and legacy aliases (mint/symbol/source/scanner + sym/url/ts).
- Persistent browser client with recycling on timeouts/crashes.
- Reason buckets: compact reject + failure summaries.

Public API
----------
- async def scan_axiom(*args, **kwargs) -> list[dict[str, Any]]
- async def scan_once(*args, **kwargs) -> list[dict[str, Any]]   (alias)
- async def run_scanner(*args, **kwargs) -> None                 (loop)
- async def shutdown() -> None

Notes
-----
- This module must remain import-light for contract tests and CI.
- True entry decisions happen later (filters/features/ai_entry_logic).
"""

import asyncio
import contextlib
import importlib
import logging
import os
import random
import re
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from types import ModuleType
from typing import Any, Protocol, TypeAlias, cast

JsonDict: TypeAlias = dict[str, Any]

# =============================================================================
# Phase A — Logging contract (stable EventLogger, no callable mismatch)
# =============================================================================


class EventLogger(Protocol):
    handlers: list[Any]

    def debug(self, msg: str, *args: Any, **kwargs: Any) -> None: ...
    def info(self, msg: str, *args: Any, **kwargs: Any) -> None: ...
    def warning(self, msg: str, *args: Any, **kwargs: Any) -> None: ...
    def error(self, msg: str, *args: Any, **kwargs: Any) -> None: ...
    def exception(self, msg: str, *args: Any, **kwargs: Any) -> None: ...
    def log(self, level: int, msg: str, *args: Any, **kwargs: Any) -> None: ...

    # best-effort helpers (log_compat may provide)
    def event(self, name: str, **kw: Any) -> None: ...
    def every(self, key: str, n: int = 10, msg: str | None = None, **kw: Any) -> None: ...


class _LogAdapter:
    """
    Logger adapter that:
      - always provides `.handlers` (typing compatibility)
      - forwards std logging calls
      - provides best-effort `.event()` and `.every()` without monkeypatching
    """

    def __init__(self, base: Any):
        self._base = base
        self.handlers: list[Any] = []
        with contextlib.suppress(Exception):
            h = getattr(base, "handlers", None)
            if isinstance(h, list):
                self.handlers = h
        self._every_store: dict[str, float] = {}

    def debug(self, msg: str, *args: Any, **kwargs: Any) -> None:
        cast(Any, self._base).debug(msg, *args, **kwargs)

    def info(self, msg: str, *args: Any, **kwargs: Any) -> None:
        cast(Any, self._base).info(msg, *args, **kwargs)

    def warning(self, msg: str, *args: Any, **kwargs: Any) -> None:
        fn = getattr(self._base, "warning", None) or getattr(self._base, "warn", None)
        if callable(fn):
            fn(msg, *args, **kwargs)
            return
        self.info(msg, *args, **kwargs)

    def error(self, msg: str, *args: Any, **kwargs: Any) -> None:
        cast(Any, self._base).error(msg, *args, **kwargs)

    def exception(self, msg: str, *args: Any, **kwargs: Any) -> None:
        fn = getattr(self._base, "exception", None)
        if callable(fn):
            fn(msg, *args, **kwargs)
            return
        self.error(msg, *args, **kwargs)

    def log(self, level: int, msg: str, *args: Any, **kwargs: Any) -> None:
        cast(Any, self._base).log(level, msg, *args, **kwargs)

    def event(self, name: str, **kw: Any) -> None:
        fn = getattr(self._base, "event", None)
        if callable(fn):
            fn(str(name), **kw)
            return
        self.info("%s %s", str(name), kw)

    def every(self, key: str, n: int = 10, msg: str | None = None, **kw: Any) -> None:
        """
        Time-based throttle.
        Historically, callers use `n` as seconds between emits.
        """
        try:
            now = time.monotonic()
            gap = float(max(1, int(n)))
            last = float(self._every_store.get(key, 0.0))
            if (now - last) < gap:
                return
            self._every_store[key] = now
        except Exception:
            # if timing store fails, just log once
            pass
        self.event(msg or key, **kw)


def setup_logging(service: str | None = None) -> None:
    """
    Best-effort; should not be required for tests.
    Does NOT force basicConfig at import time.
    """
    _ = service
    with contextlib.suppress(Exception):
        from utils_pkg.log_compat import setup_logging as _sl  # type: ignore

        _sl(service=service)
        return


def get_logger(name: str | None = None) -> EventLogger:
    nm = (name or "scanner.axiom_scanner").strip() or "scanner.axiom_scanner"

    with contextlib.suppress(Exception):
        from utils_pkg.log_compat import get_event_logger  # type: ignore

        return cast(EventLogger, _LogAdapter(get_event_logger(nm)))

    with contextlib.suppress(Exception):
        from utils_pkg.log_compat import get_logger as _gl  # type: ignore

        return cast(EventLogger, _LogAdapter(_gl(nm)))

    with contextlib.suppress(Exception):
        from logging_setup import get_logger as _gl2  # type: ignore[import-not-found]

        return cast(EventLogger, _LogAdapter(_gl2(nm)))

    return cast(EventLogger, _LogAdapter(logging.getLogger(nm)))


LOG: EventLogger = get_logger("scanner.axiom_scanner")


def _log_every(event: str, every_sec: float, level: str = "info", **fields: Any) -> None:
    """
    Throttled log compatible with EventLogger + stdlib fallback.
    """
    try:
        LOG.every(f"axiom_scanner:{event}", n=max(1, int(every_sec)), msg=event, **fields)
        return
    except Exception:
        pass

    fn = getattr(LOG, level, None)
    if callable(fn):
        with contextlib.suppress(Exception):
            fn("%s %s", event, fields)


# =============================================================================
# Env helpers (no side-effects)
# =============================================================================

_TRUE = {"1", "true", "t", "yes", "y", "on"}
_FALSE = {"0", "false", "f", "no", "n", "off"}


def _env_bool(name: str, default: bool = False) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    s = str(v).strip().lower()
    if s in _TRUE:
        return True
    if s in _FALSE:
        return False
    return default


def _env_str(name: str, default: str = "") -> str:
    v = os.getenv(name)
    return default if v is None else str(v).strip()


def _env_int(name: str, default: int) -> int:
    v = os.getenv(name)
    if v is None or v == "":
        return default
    try:
        return int(float(v))
    except Exception:
        return default


def _env_float(name: str, default: float) -> float:
    v = os.getenv(name)
    if v is None or v == "":
        return default
    try:
        return float(v)
    except Exception:
        return default


def _env_csv(name: str, default: str) -> list[str]:
    raw = _env_str(name, default)
    parts = [p.strip() for p in raw.split(",")]
    return [p for p in parts if p]


def _kill_or_disabled() -> bool:
    return (
        _env_bool("KILL_SWITCH", False)
        or _env_bool("DISABLE_SCANNERS", False)
        or _env_bool("AXIOM_SCANNER_DISABLED", False)
    )


def _offline_env_forced() -> bool:
    # Any true enables offline contract mode
    for k in ("SAFE_MODE", "OFFLINE_MODE", "NO_NETWORK", "DRY_RUN", "TEST_MODE"):
        if _env_bool(k, False):
            return True
    # If explicitly set, SCANNER_LIVE_TESTS=0 forces offline contract mode.
    if "SCANNER_LIVE_TESTS" in os.environ and not _env_bool("SCANNER_LIVE_TESTS", True):
        return True
    return False


# =============================================================================
# Tunables / flags (do NOT introduce new env keys here)
# =============================================================================

AXIOM_SCAN_ENABLED: bool = _env_bool("AXIOM_SCAN_ENABLED", True)
AXIOM_HEADLESS: bool = _env_bool("AXIOM_HEADLESS", True)

AXIOM_SCAN_LIMIT: int = max(1, _env_int("AXIOM_SCAN_LIMIT", 80))
AXIOM_LIMIT_PER_SECTION: int = max(
    1, _env_int("AXIOM_LIMIT_PER_SECTION", max(10, AXIOM_SCAN_LIMIT // 2))
)

AXIOM_TIMEOUT_SEC: int = max(10, _env_int("AXIOM_SCAN_TIMEOUT_SEC", 90))
AXIOM_RETRIES: int = max(1, _env_int("AXIOM_SCAN_RETRIES", 2))
AXIOM_BACKOFF_S: float = max(0.1, _env_float("AXIOM_SCAN_BACKOFF_S", 0.7))

AXIOM_SECTIONS: list[str] = _env_csv("AXIOM_SECTIONS", "Trending,Surge,DEX Screener,Pump Live")

AXIOM_SCAN_INTERVAL_SEC: float = max(0.3, _env_float("AXIOM_SCAN_INTERVAL_SEC", 5.0))
AXIOM_SCAN_JITTER_SEC: float = max(0.0, _env_float("AXIOM_SCAN_JITTER_SEC", 1.0))

AXIOM_EMIT_DEDUP_TTL_S: int = max(10, _env_int("AXIOM_EMIT_DEDUP_TTL_S", 120))
AXIOM_EMIT_DEDUP_PREFIX: str = _env_str("AXIOM_EMIT_DEDUP_PREFIX", "axiom:emit:")

AXIOM_CHAIN: str = _env_str("AXIOM_CHAIN", "solana").lower().strip() or "solana"

# =============================================================================
# Optional dependency loader (lazy import; NEVER at module import)
# =============================================================================

_AXIOMWEB_CLS: Any = None
_AXIOM_IMPORT_ERR: str | None = None
_axiom_import_lock = asyncio.Lock()


async def _load_axiomweb() -> Any:
    """
    Lazy import integrations.axiom_web.AxiomWeb (may be heavy).
    Never at module import time.
    """
    global _AXIOMWEB_CLS, _AXIOM_IMPORT_ERR

    async with _axiom_import_lock:
        if _AXIOMWEB_CLS is not None:
            return _AXIOMWEB_CLS
        if _AXIOM_IMPORT_ERR is not None:
            return None

        try:
            mod: ModuleType = importlib.import_module("integrations.axiom_web")
            cls = getattr(mod, "AxiomWeb", None)
            if cls is None:
                raise RuntimeError("integrations.axiom_web has no AxiomWeb")
            _AXIOMWEB_CLS = cls
            return _AXIOMWEB_CLS
        except Exception as e:  # pragma: no cover
            _AXIOM_IMPORT_ERR = f"{type(e).__name__}: {e}"
            return None


# =============================================================================
# Redis helpers (optional; sync or async clients)
# =============================================================================


def _looks_like_queue(x: Any) -> bool:
    return x is not None and (
        callable(getattr(x, "put", None)) or callable(getattr(x, "put_nowait", None))
    )


def _looks_like_redis(x: Any) -> bool:
    if x is None:
        return False
    if isinstance(x, asyncio.Event) or _looks_like_queue(x):
        return False
    return any(callable(getattr(x, nm, None)) for nm in ("get", "set", "setex", "expire", "ping"))


def _redis_ok(r: Any) -> bool:
    return r is not None and any(
        callable(getattr(r, nm, None)) for nm in ("get", "set", "setex", "expire")
    )


async def _maybe_await(x: Any) -> Any:
    if asyncio.iscoroutine(x):
        return await x
    return x


async def _redis_get(r: Any, key: str) -> str | None:
    if not _redis_ok(r):
        return None
    try:
        out = await _maybe_await(r.get(key))
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
            await _maybe_await(r.setex(key, int(ttl_s), value))
            return

        await _maybe_await(r.set(key, value))
        if callable(getattr(r, "expire", None)):
            await _maybe_await(r.expire(key, int(ttl_s)))
    except Exception:
        return


async def _redis_set_nx_ex(r: Any, key: str, value: str, ttl_s: int) -> bool:
    """
    Best-effort atomic dedupe using NX+EX if supported, else get+setex.
    """
    if not _redis_ok(r):
        return True

    with contextlib.suppress(Exception):
        out = await _maybe_await(r.set(key, value, nx=True, ex=int(ttl_s)))
        if out is not None:
            return bool(out)

    with contextlib.suppress(Exception):
        out2 = await _maybe_await(r.set(key, value, ex=int(ttl_s), nx=True))
        if out2 is not None:
            return bool(out2)

    prev = await _redis_get(r, key)
    if prev:
        return False
    await _redis_setex(r, key, ttl_s, value)
    return True


# =============================================================================
# Call-context parsing (orchestrator-safe)
# =============================================================================


@dataclass
class _CallCtx:
    redis: Any = None
    stop_evt: asyncio.Event | None = None
    q: Any = None
    emit: Callable[[Any], Any] | None = None


def _parse_ctx(*args: Any, **kwargs: Any) -> _CallCtx:
    """
    Accept redis/stop_evt/queue/emit in ANY order and via kwargs.
    Compatible with multiple orchestrator call styles.
    """
    ctx = _CallCtx()

    for k in ("stop_evt", "stop_event", "stop"):
        v = kwargs.get(k)
        if isinstance(v, asyncio.Event):
            ctx.stop_evt = v
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
        ctx.emit = cast(Callable[[Any], Any], v_emit)

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
            ctx.emit = cast(Callable[[Any], Any], a)
            continue

    return ctx


# =============================================================================
# Normalization helpers
# =============================================================================

_BASE58 = re.compile(r"^[1-9A-HJ-NP-Za-km-z]{32,64}$")


def _valid_mint(x: Any) -> bool:
    return isinstance(x, str) and bool(_BASE58.match(x.strip()))


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


@dataclass
class _Buckets:
    rejects: dict[str, int] = field(default_factory=dict)
    notes: dict[str, int] = field(default_factory=dict)

    def _bump(self, d: dict[str, int], k: str, inc: int = 1) -> None:
        d[k] = int(d.get(k, 0)) + int(inc)

    def r(self, k: str, inc: int = 1) -> None:
        self._bump(self.rejects, k, inc)

    def n(self, k: str, inc: int = 1) -> None:
        self._bump(self.notes, k, inc)


def _normalize_item(raw: JsonDict, *, section: str | None, buckets: _Buckets) -> JsonDict | None:
    """
    Convert an Axiom-discovered row into CandidateSnapshot-ish dict.
    Keeps original fields under meta for debugging.
    """
    mint = raw.get("mint") or raw.get("base_mint") or raw.get("token") or raw.get("address") or ""
    mint = str(mint).strip()
    if not _valid_mint(mint):
        buckets.r("bad_mint")
        return None

    symbol = str(raw.get("symbol") or raw.get("sym") or raw.get("ticker") or "").strip() or mint[:6]
    url_raw = str(raw.get("url") or raw.get("pair_url") or raw.get("link") or "").strip()
    url: str | None = url_raw or None

    liq_usd = _nz(
        raw.get("liq_usd") or raw.get("liq") or raw.get("liquidity_usd") or raw.get("liquidity"),
        0.0,
    )
    vol5 = _nz(
        raw.get("vol5") or raw.get("vol_5m_usd") or raw.get("volume5") or raw.get("vol"), 0.0
    )
    buys5 = _safe_int(raw.get("buys5") or raw.get("buys_5m") or raw.get("buys"), 0)
    sells5 = _safe_int(raw.get("sells5") or raw.get("sells_5m") or raw.get("sells"), 0)
    chg5 = _nz(raw.get("chg5") or raw.get("price_change_5m_pct") or raw.get("change5") or 0.0, 0.0)
    age_min = _nz(raw.get("age_min") or raw.get("token_age_min") or raw.get("age"), 0.0)

    ts = float(time.time())

    snap: JsonDict = {
        # canonical identity
        "source": "axiom",
        "src": "axiom",
        "scanner": "axiom_scanner",
        "scanner_id": "axiom_scanner",
        "chain": AXIOM_CHAIN,
        "dex": "axiom",
        "mint": mint,
        "symbol": symbol,
        "pair_url": url,
        "scan_ts": ts,
        "section": section,
        # compat identity
        "sym": symbol,
        "url": url,
        "ts": ts,
        # metrics (modern)
        "liq_usd": float(liq_usd),
        "vol_5m_usd": float(vol5),
        "buys_5m": int(buys5),
        "sells_5m": int(sells5),
        "price_change_5m_pct": float(chg5),
        "token_age_min": float(age_min),
        # legacy aliases
        "liq": float(liq_usd),
        "vol5": float(vol5),
        "buys5": int(buys5),
        "sells5": int(sells5),
        "chg5": float(chg5),
        "age_min": float(age_min),
        # downstream placeholders used by later layers
        "ai_rug_score": float(_nz(raw.get("ai_rug_score"), 0.0)),
        "ai_entry_score": raw.get("ai_entry_score"),
        "ai_entry_reason": raw.get("ai_entry_reason"),
        "rank_score": raw.get("rank_score"),
        "score_combined": raw.get("score_combined"),
        "score_label": raw.get("score_label"),
        "score_reasons": raw.get("score_reasons"),
        "is_honeypot": bool(raw.get("is_honeypot", False)),
        "trading_paused": bool(raw.get("trading_paused", False)),
        "blacklisted": bool(raw.get("blacklisted", False)),
        # debug raw
        "meta": dict(raw),
    }
    return snap


def _dedupe_by_mint(items: list[JsonDict], limit: int, buckets: _Buckets) -> list[JsonDict]:
    seen: set[str] = set()
    out: list[JsonDict] = []
    for d in items:
        mint = d.get("mint")
        if not isinstance(mint, str) or not mint:
            buckets.r("missing_mint_post")
            continue
        if mint in seen:
            buckets.r("dedupe")
            continue
        seen.add(mint)
        out.append(d)
        if len(out) >= limit:
            break
    return out


# =============================================================================
# Offline mode (schema-safe; no I/O)
# =============================================================================

_OFFLINE_TS = 1_700_000_000  # fixed deterministic timestamp


def _offline_candidates(limit: int | None = None) -> list[JsonDict]:
    # Must satisfy base58 regex [32..64]
    mint_a = "7QmVhQYcYjvZQYx6m5kzj9f3r2aPp8GqNwYcZpQn4v1a"
    mint_b = "9p8GqNwYcZpQn4v1a7QmVhQYcYjvZQYx6m5kzj9f3r2aP"

    base: JsonDict = {
        "source": "axiom",
        "src": "axiom",
        "scanner": "axiom_scanner",
        "scanner_id": "axiom_scanner",
        "chain": AXIOM_CHAIN,
        "dex": "axiom",
        "scan_ts": float(_OFFLINE_TS),
        "ts": float(_OFFLINE_TS),
        "section": "Offline",
        "pair_url": None,
        "url": None,
        "liq_usd": 250_000.0,
        "vol_5m_usd": 12_500.0,
        "buys_5m": 18,
        "sells_5m": 7,
        "price_change_5m_pct": 2.3,
        "token_age_min": 19.0,
        "liq": 250_000.0,
        "vol5": 12_500.0,
        "buys5": 18,
        "sells5": 7,
        "chg5": 2.3,
        "age_min": 19.0,
        "ai_rug_score": 0.0,
        "ai_entry_score": None,
        "ai_entry_reason": None,
        "rank_score": 0.0,
        "score_combined": 0.0,
        "score_label": "offline",
        "score_reasons": ["offline_candidate"],
        "is_honeypot": False,
        "trading_paused": False,
        "blacklisted": False,
        "meta": {"offline": True},
    }

    a = dict(base)
    a.update({"mint": mint_a, "symbol": "AXIOM_OFF_A", "sym": "AXIOM_OFF_A"})
    b = dict(base)
    b.update(
        {
            "mint": mint_b,
            "symbol": "AXIOM_OFF_B",
            "sym": "AXIOM_OFF_B",
            "liq_usd": 410_000.0,
            "liq": 410_000.0,
            "vol_5m_usd": 31_000.0,
            "vol5": 31_000.0,
            "buys_5m": 33,
            "buys5": 33,
            "price_change_5m_usd": None,
            "price_change_5m_pct": 4.1,
            "chg5": 4.1,
            "token_age_min": 9.0,
            "age_min": 9.0,
        }
    )

    items: list[JsonDict] = [a, b]
    if limit is None:
        return items
    try:
        lim = int(limit)
    except Exception:
        return items
    if lim <= 0:
        return items
    return items[:lim]


def _offline_enabled(
    offline: bool,
    dry: bool,
    dry_run: bool,
    no_network: bool,
    sample: bool,
    test_mode: bool,
) -> bool:
    return (
        bool(offline or dry or dry_run or no_network or sample or test_mode)
        or _offline_env_forced()
    )


# =============================================================================
# Persistent client (browser) with recycling (never initialized at import time)
# =============================================================================

_AX: Any = None
_ax_lock = asyncio.Lock()


def _construct_axiom_web(cls: Any) -> Any:
    """
    Construct AxiomWeb with best-effort signature compatibility.
    """
    with contextlib.suppress(TypeError):
        return cls(headless=AXIOM_HEADLESS)
    with contextlib.suppress(TypeError):
        return cls(headless=AXIOM_HEADLESS, chain=AXIOM_CHAIN)
    with contextlib.suppress(TypeError):
        return cls(headless=AXIOM_HEADLESS, logger=cast(Any, LOG))
    return cls()


async def _get_ax() -> Any:
    """
    Lazily create and launch the AxiomWeb client. Never called at import time.
    Raises if dependency missing; callers must handle.
    """
    global _AX
    async with _ax_lock:
        if _AX is not None:
            return _AX

        cls = await _load_axiomweb()
        if cls is None:
            raise RuntimeError(f"AxiomWeb unavailable ({_AXIOM_IMPORT_ERR or 'import failed'})")

        ax = _construct_axiom_web(cls)
        launch = getattr(ax, "launch", None)
        if not callable(launch):
            raise RuntimeError("AxiomWeb missing launch()")

        out = launch()
        if asyncio.iscoroutine(out):
            await out

        _AX = ax
        return _AX


async def _recycle_ax() -> None:
    global _AX
    async with _ax_lock:
        try:
            if _AX is not None:
                close = getattr(_AX, "close", None)
                if callable(close):
                    out = close()
                    if asyncio.iscoroutine(out):
                        await out
        finally:
            _AX = None


async def shutdown() -> None:
    """Explicit shutdown for tests / graceful stop."""
    await _recycle_ax()


async def _discover_sections(
    ax: Any, *, want_per_section: int, sections: list[str]
) -> dict[str, Any]:
    """
    Call ax.discover_sections with best-effort compatibility across versions.
    Expected runtime shape: {section_name: [items...], ...}
    """
    fn = getattr(ax, "discover_sections", None)
    if not callable(fn):
        raise RuntimeError("AxiomWeb missing discover_sections()")

    with contextlib.suppress(TypeError):
        out = fn(sections=sections, limit_per_section=want_per_section)
        out = await _maybe_await(out)
        if isinstance(out, dict):
            return out

    with contextlib.suppress(TypeError):
        out2 = fn(limit_per_section=want_per_section)
        out2 = await _maybe_await(out2)
        if isinstance(out2, dict):
            return out2

    out3 = fn(want_per_section)
    out3 = await _maybe_await(out3)
    if isinstance(out3, dict):
        return out3

    raise RuntimeError("AxiomWeb discover_sections returned unexpected type")


async def _fetch_once(
    *, want_per_section: int, stop_evt: asyncio.Event | None, buckets: _Buckets
) -> list[JsonDict]:
    if stop_evt is not None and stop_evt.is_set():
        buckets.n("stopped_pre_fetch")
        return []

    ax = await _get_ax()
    sections_map = await _discover_sections(
        ax, want_per_section=want_per_section, sections=AXIOM_SECTIONS
    )

    flat: list[JsonDict] = []
    for sec_name, lst in (sections_map or {}).items():
        if stop_evt is not None and stop_evt.is_set():
            buckets.n("stopped_mid_fetch")
            break
        if not isinstance(lst, list):
            buckets.r("section_not_list")
            continue
        for item in lst:
            if not isinstance(item, dict):
                buckets.r("item_not_dict")
                continue
            snap = _normalize_item(cast(JsonDict, item), section=str(sec_name), buckets=buckets)
            if snap:
                flat.append(snap)
    return flat


# =============================================================================
# Emit helpers
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
                out2 = ctx.q.put(item)
                if asyncio.iscoroutine(out2):
                    await out2


async def _emit_dedup_ok(redis: Any, mint: str) -> bool:
    """
    Return True if we should emit; False if dedupe blocks it.
    Uses NX+EX if possible; falls back to get+setex.
    """
    if not _redis_ok(redis):
        return True
    k = f"{AXIOM_EMIT_DEDUP_PREFIX}{mint}"
    try:
        return await _redis_set_nx_ex(redis, k, "1", AXIOM_EMIT_DEDUP_TTL_S)
    except Exception:
        return True


# =============================================================================
# Public scan implementation
# =============================================================================


async def scan_once(
    *args: Any,
    offline: bool = False,
    dry: bool = False,
    dry_run: bool = False,
    no_network: bool = False,
    sample: bool = False,
    test_mode: bool = False,
    limit: int | None = None,
    **kwargs: Any,
) -> list[JsonDict]:
    """
    Primary scan entrypoint.

    OFFLINE MODE (for tests/contract runs):
      scan_once(offline=True) -> deterministic sample candidates (no I/O)

    LIVE MODE:
      Uses AxiomWeb if available, else degrades to [] with throttled warnings.
    """
    if _kill_or_disabled():
        return []

    # Back-compat: allow flags passed via kwargs by older harnesses
    if "offline" in kwargs and not offline:
        offline = str(kwargs.pop("offline")).strip().lower() in _TRUE
    if "dry_run" in kwargs and not dry_run:
        dry_run = str(kwargs.pop("dry_run")).strip().lower() in _TRUE
    if "dry" in kwargs and not dry:
        dry = str(kwargs.pop("dry")).strip().lower() in _TRUE
    if "no_network" in kwargs and not no_network:
        no_network = str(kwargs.pop("no_network")).strip().lower() in _TRUE
    if "sample" in kwargs and not sample:
        sample = str(kwargs.pop("sample")).strip().lower() in _TRUE
    if "test_mode" in kwargs and not test_mode:
        test_mode = str(kwargs.pop("test_mode")).strip().lower() in _TRUE
    if "limit" in kwargs and limit is None:
        with contextlib.suppress(Exception):
            limit = int(kwargs.pop("limit"))

    ctx = _parse_ctx(*args, **kwargs)
    stop_evt = ctx.stop_evt
    buckets = _Buckets()

    if _offline_enabled(offline, dry, dry_run, no_network, sample, test_mode):
        items = _offline_candidates(limit=limit if limit is not None else AXIOM_SCAN_LIMIT)
        if items and (ctx.emit is not None or ctx.q is not None):
            for it in items:
                if stop_evt is not None and stop_evt.is_set():
                    break
                mint = it.get("mint")
                if isinstance(mint, str) and mint and await _emit_dedup_ok(ctx.redis, mint):
                    await _emit_one(ctx, it)
        return items

    if not AXIOM_SCAN_ENABLED:
        _log_every("AXIOM_DISABLED", 30.0, level="info")
        return []

    cls = await _load_axiomweb()
    if cls is None:
        _log_every("AXIOM_DEP_MISSING", 60.0, level="warning", err=_AXIOM_IMPORT_ERR or "unknown")
        return []

    # Ask for extra, then dedupe/trim
    want_per_section = max(1, int(AXIOM_LIMIT_PER_SECTION))
    want_per_section = max(want_per_section, (AXIOM_SCAN_LIMIT // max(1, len(AXIOM_SECTIONS))) + 3)

    last_err: str | None = None
    raw_n = 0

    for attempt in range(1, AXIOM_RETRIES + 1):
        if stop_evt is not None and stop_evt.is_set():
            buckets.n("stopped")
            break

        try:
            raw_items = await asyncio.wait_for(
                _fetch_once(want_per_section=want_per_section, stop_evt=stop_evt, buckets=buckets),
                timeout=float(AXIOM_TIMEOUT_SEC),
            )
            raw_n = len(raw_items)

            eff_limit = AXIOM_SCAN_LIMIT if limit is None else max(1, int(limit))
            items = _dedupe_by_mint(raw_items, limit=eff_limit, buckets=buckets)

            # Basic ranking: rank_score/score_combined, else vol5, else buys5
            def _key(m: JsonDict) -> tuple[float, float, int]:
                a = float(m.get("rank_score") or m.get("score_combined") or 0.0)
                v = float(m.get("vol5") or 0.0)
                buys = int(m.get("buys5") or 0)
                return (a, v, buys)

            items.sort(key=_key, reverse=True)

            LOG.info(
                "AXIOM_RESULT candidates=%d raw=%d limit=%d sections=%d attempt=%d/%d",
                len(items),
                raw_n,
                eff_limit,
                len(AXIOM_SECTIONS),
                attempt,
                AXIOM_RETRIES,
            )

            # Emit (optional)
            if items and (ctx.emit is not None or ctx.q is not None):
                for it in items:
                    if stop_evt is not None and stop_evt.is_set():
                        break
                    mint = it.get("mint")
                    if isinstance(mint, str) and mint:
                        if not await _emit_dedup_ok(ctx.redis, mint):
                            buckets.n("emit_dedup")
                            continue
                    await _emit_one(ctx, it)

            if buckets.rejects:
                _log_every(
                    "AXIOM_REJECT_BUCKETS",
                    12.0,
                    level="info",
                    rejects=dict(
                        sorted(buckets.rejects.items(), key=lambda kv: kv[1], reverse=True)[:10]
                    ),
                    notes=dict(
                        sorted(buckets.notes.items(), key=lambda kv: kv[1], reverse=True)[:8]
                    ),
                )

            return items

        except asyncio.TimeoutError:
            last_err = f"TimeoutError after {AXIOM_TIMEOUT_SEC}s"
            buckets.r("timeout")
            _log_every(
                "AXIOM_TIMEOUT",
                10.0,
                level="warning",
                attempt=attempt,
                timeout_s=AXIOM_TIMEOUT_SEC,
            )
            await _recycle_ax()
        except Exception as e:
            last_err = f"{type(e).__name__}: {e}"
            buckets.r("fetch_exc")
            _log_every("AXIOM_FETCH_ERR", 10.0, level="warning", attempt=attempt, err=last_err)
            await _recycle_ax()

        if attempt < AXIOM_RETRIES:
            await asyncio.sleep(float(AXIOM_BACKOFF_S) * attempt + random.uniform(0.0, 0.25))

    _log_every(
        "AXIOM_GIVE_UP",
        10.0,
        level="warning",
        attempts=AXIOM_RETRIES,
        last_err=last_err or "",
        raw_n=raw_n,
    )
    return []


async def scan_axiom(*args: Any, **kwargs: Any) -> list[JsonDict]:
    return await scan_once(*args, **kwargs)


async def run_scanner(*args: Any, **kwargs: Any) -> None:
    """
    Long-running loop. Safe to call from orchestrator.
    """
    ctx = _parse_ctx(*args, **kwargs)
    stop_evt = ctx.stop_evt or asyncio.Event()

    try:
        while not stop_evt.is_set():
            if _kill_or_disabled() or (not AXIOM_SCAN_ENABLED):
                await asyncio.sleep(1.0)
                continue

            with contextlib.suppress(Exception):
                setup_logging(service="scanner.axiom_scanner")

            with contextlib.suppress(Exception):
                await scan_once(redis=ctx.redis, stop_evt=stop_evt, q=ctx.q, emit=ctx.emit)

            sleep_for = float(AXIOM_SCAN_INTERVAL_SEC) + random.uniform(
                -float(AXIOM_SCAN_JITTER_SEC), float(AXIOM_SCAN_JITTER_SEC)
            )
            await asyncio.sleep(max(0.05, sleep_for))
    finally:
        with contextlib.suppress(Exception):
            await shutdown()


async def run(*args: Any, **kwargs: Any) -> None:
    await run_scanner(*args, **kwargs)


async def main(*args: Any, **kwargs: Any) -> None:
    await run_scanner(*args, **kwargs)


__all__ = [
    "LOG",
    "EventLogger",
    "get_logger",
    "main",
    "run",
    "run_scanner",
    "scan_axiom",
    "scan_once",
    "setup_logging",
    "shutdown",
]
