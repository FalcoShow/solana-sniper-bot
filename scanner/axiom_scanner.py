from __future__ import annotations

"""
scanner/axiom_scanner.py

Axiom Scanner (Solana) — V3+++ LOCK-IN (quant-grade / God-tier)
===============================================================

High-level role
---------------
Pull high-signal candidates from Axiom (web / browser-driven discovery), normalize into
the SAME CandidateSnapshot-ish schema used across scanners, and optionally emit into
an orchestrator queue / callback.

Hard requirements (enforced)
----------------------------
- 24/7 safe: never hard-fail on optional deps (playwright/axiom libs). Degrade to [].
- Zero network/browser at import time.
- Orchestrator-friendly signatures: accepts (redis/stop_evt/q/emit) in any order.
- Emits BOTH canonical keys and legacy aliases (mint/symbol/source/scanner + sym/url/ts).
- Persistent browser client with recycling on timeouts/crashes.
- Reason buckets: compact reject + failure summaries (helps diagnose “no trades”).

Public API
----------
- async def scan_axiom(*args, **kwargs) -> List[Dict[str, Any]]
- async def scan_once(*args, **kwargs) -> List[Dict[str, Any]]   (alias)
- async def run_scanner(*args, **kwargs) -> None                 (loop)

Notes
-----
- Canonical candidate schema lives in scanner/registry.py. This scanner produces
  a compatible "CandidateSnapshot-ish" dict: identity + best-effort metrics.
- True "entry decisions" happen later (pair_filters/feature_builder/ai_entry_logic).
"""

import asyncio
import contextlib
import logging
import os
import random
import re
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

# =============================================================================
# Logging (best-effort + shims)
# =============================================================================
try:
    from utils_pkg.logging_setup import get_logger as _get_logger  # preferred
except Exception:  # pragma: no cover
    try:
        from logging_setup import get_logger as _get_logger  # root shim
    except Exception:  # pragma: no cover
        _get_logger = None  # type: ignore

if _get_logger:
    try:
        LOG = _get_logger("scanner.axiom")  # type: ignore[misc]
    except TypeError:  # pragma: no cover
        LOG = _get_logger()  # type: ignore[misc]
else:
    LOG = logging.getLogger("scanner.axiom")
    if not LOG.handlers:
        logging.basicConfig(level=logging.INFO)


def _ensure_logger_shims() -> None:
    """Provide LOG.event and LOG.once if logger is stdlib-only."""
    if not hasattr(LOG, "event"):

        def _event(name: str, **fields: Any) -> None:  # pragma: no cover
            with contextlib.suppress(Exception):
                LOG.info("%s %s", name, fields)

        LOG.event = _event

    if not hasattr(LOG, "once"):
        _seen: dict[str, float] = {}

        def _once(
            key: str,
            ttl: float = 30.0,
            level: int = logging.INFO,
            msg: str | None = None,
            **fields: Any,
        ) -> bool:  # pragma: no cover
            now = time.monotonic()
            last = float(_seen.get(key, 0.0))
            if now - last < float(ttl):
                return False
            _seen[key] = now
            with contextlib.suppress(Exception):
                LOG.log(level, msg or key, extra={"event": msg or key, **fields})
            return True

        LOG.once = _once


_ensure_logger_shims()


def _log_throttled(event: str, every_sec: float, level: int = logging.INFO, **fields: Any) -> None:
    """Time-based throttle compatible with EventLogger (.once) and stdlib fallback."""
    try:
        once = getattr(LOG, "once", None)
        if callable(once):
            once(
                f"axiom:{event}",
                ttl=float(every_sec),
                level=level,
                msg=event,
                event=event,
                **fields,
            )
            return
    except Exception:
        pass
    with contextlib.suppress(Exception):
        LOG.event(event, **fields)  # type: ignore[misc]


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


def _csv(name: str, default: str) -> list[str]:
    raw = _s(name, default)
    parts = [p.strip() for p in raw.split(",")]
    return [p for p in parts if p]


def _kill_or_disabled() -> bool:
    return (
        _b("KILL_SWITCH", False)
        or _b("DISABLE_SCANNERS", False)
        or _b("AXIOM_SCANNER_DISABLED", False)
    )


# =============================================================================
# Tunables / flags
# =============================================================================
AXIOM_SCAN_ENABLED = _b("AXIOM_SCAN_ENABLED", True)
AXIOM_HEADLESS = _b("AXIOM_HEADLESS", True)

AXIOM_SCAN_LIMIT = max(1, _i("AXIOM_SCAN_LIMIT", 80))
AXIOM_LIMIT_PER_SECTION = max(1, _i("AXIOM_LIMIT_PER_SECTION", max(10, AXIOM_SCAN_LIMIT // 2)))

AXIOM_TIMEOUT_SEC = max(10, _i("AXIOM_SCAN_TIMEOUT_SEC", 90))
AXIOM_RETRIES = max(1, _i("AXIOM_SCAN_RETRIES", 2))
AXIOM_BACKOFF_S = max(0.1, _f("AXIOM_SCAN_BACKOFF_S", 0.7))

AXIOM_SECTIONS = _csv("AXIOM_SECTIONS", "Trending,Surge,DEX Screener,Pump Live")

AXIOM_SCAN_INTERVAL_SEC = max(0.3, _f("AXIOM_SCAN_INTERVAL_SEC", 5.0))
AXIOM_SCAN_JITTER_SEC = max(0.0, _f("AXIOM_SCAN_JITTER_SEC", 1.0))

# Optional: emit dedupe via redis
AXIOM_EMIT_DEDUP_TTL_S = max(10, _i("AXIOM_EMIT_DEDUP_TTL_S", 120))
AXIOM_EMIT_DEDUP_PREFIX = _s("AXIOM_EMIT_DEDUP_PREFIX", "axiom:emit:")

# Optional: normalize / enforce chain
AXIOM_CHAIN = _s("AXIOM_CHAIN", "solana").lower().strip() or "solana"

# =============================================================================
# Optional dependency: integrations.axiom_web.AxiomWeb (soft import)
# =============================================================================
AxiomWeb = None  # type: ignore
_AX_IMPORT_ERR: str | None = None
try:  # pragma: no cover
    from integrations.axiom_web import AxiomWeb as _AxiomWeb  # type: ignore

    AxiomWeb = _AxiomWeb  # type: ignore
except Exception as e:  # pragma: no cover
    _AX_IMPORT_ERR = f"{type(e).__name__}: {e}"


# =============================================================================
# Redis helpers (optional, safe)
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
    if r is None:
        return False
    return any(callable(getattr(r, nm, None)) for nm in ("get", "set", "setex", "expire"))


async def _redis_get(r: Any, key: str) -> str | None:
    if not _redis_ok(r):
        return None
    try:
        out = r.get(key)
        if asyncio.iscoroutine(out):
            out = await out
        if out is None:
            return None
        if isinstance(out, bytes | bytearray):
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
# Call-context parsing (orchestrator-safe)
# =============================================================================
@dataclass
class _CallCtx:
    redis: Any = None
    stop_evt: asyncio.Event | None = None
    q: Any = None
    emit: Callable[[Any], Any] | None = None


def _parse_ctx(*args: Any, **kwargs: Any) -> _CallCtx:
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

    v = kwargs.get("emit")
    if callable(v):
        ctx.emit = v

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
        # callable emit passed positionally
        if (
            ctx.emit is None
            and callable(a)
            and not isinstance(a, str | bytes | dict | list | tuple | set)
        ):
            ctx.emit = a  # type: ignore[assignment]
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


def _normalize_item(
    raw: dict[str, Any], *, section: str | None, buckets: _Buckets
) -> dict[str, Any] | None:
    """
    Convert an Axiom-discovered row into the same CandidateSnapshot-ish dict
    shape used elsewhere. Best-effort: keeps original fields under meta/raw.
    """
    mint = (
        raw.get("mint") or raw.get("base_mint") or raw.get("token") or raw.get("address") or ""
    ).strip()
    if not _valid_mint(mint):
        buckets.r("bad_mint")
        return None

    symbol = (raw.get("symbol") or raw.get("sym") or raw.get("ticker") or "").strip() or mint[:6]
    url = (raw.get("url") or raw.get("pair_url") or raw.get("link") or "").strip() or None

    # Common optional metrics (Axiom may provide some; keep best-effort aliases)
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

    snap: dict[str, Any] = {
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
        # best-effort metrics (modern)
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
        # keep raw payload for debugging
        "meta": dict(raw),
    }
    return snap


def _dedupe_by_mint(
    items: list[dict[str, Any]], limit: int, buckets: _Buckets
) -> list[dict[str, Any]]:
    seen: set[str] = set()
    out: list[dict[str, Any]] = []
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
# Persistent client (browser) with recycling
# =============================================================================
_AX: Any = None
_ax_lock = asyncio.Lock()


async def _get_ax() -> Any:
    """
    Lazily create and launch the AxiomWeb client. Never called at import time.
    Raises if dependency missing; callers must handle.
    """
    global _AX
    async with _ax_lock:
        if _AX is None:
            if AxiomWeb is None:
                raise RuntimeError(f"AxiomWeb unavailable ({_AX_IMPORT_ERR or 'import failed'})")
            ax = AxiomWeb(headless=AXIOM_HEADLESS)
            # best-effort launch
            out = ax.launch()
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
) -> dict[str, list[dict[str, Any]]]:
    """
    Call ax.discover_sections with best-effort compatibility across versions.
    Expected return: {section_name: [items...], ...}
    """
    fn = getattr(ax, "discover_sections", None)
    if not callable(fn):
        raise RuntimeError("AxiomWeb missing discover_sections()")

    # Try the most explicit signature first
    try:
        out = fn(sections=sections, limit_per_section=want_per_section)
        if asyncio.iscoroutine(out):
            out = await out
        if isinstance(out, dict):
            return out  # type: ignore[return-value]
    except TypeError:
        pass

    # Try without sections kw
    try:
        out = fn(limit_per_section=want_per_section)
        if asyncio.iscoroutine(out):
            out = await out
        if isinstance(out, dict):
            return out  # type: ignore[return-value]
    except TypeError:
        pass

    # Try positional (legacy)
    out2 = fn(want_per_section)
    if asyncio.iscoroutine(out2):
        out2 = await out2
    if isinstance(out2, dict):
        return out2  # type: ignore[return-value]

    raise RuntimeError("AxiomWeb discover_sections returned unexpected type")


async def _fetch_once(
    *, want_per_section: int, stop_evt: asyncio.Event | None, buckets: _Buckets
) -> list[dict[str, Any]]:
    if stop_evt is not None and stop_evt.is_set():
        buckets.n("stopped_pre_fetch")
        return []

    ax = await _get_ax()
    sections_map = await _discover_sections(
        ax, want_per_section=want_per_section, sections=AXIOM_SECTIONS
    )

    flat: list[dict[str, Any]] = []
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
            snap = _normalize_item(item, section=str(sec_name), buckets=buckets)
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
                out = ctx.q.put(item)
                if asyncio.iscoroutine(out):
                    await out


async def _emit_dedup_ok(redis: Any, mint: str) -> bool:
    """Return True if we should emit; False if dedupe blocks it."""
    if not _redis_ok(redis):
        return True
    k = f"{AXIOM_EMIT_DEDUP_PREFIX}{mint}"
    prev = await _redis_get(redis, k)
    if prev:
        return False
    await _redis_setex(redis, k, AXIOM_EMIT_DEDUP_TTL_S, "1")
    return True


# =============================================================================
# Public scan implementation
# =============================================================================
async def scan_once(*args: Any, **kwargs: Any) -> list[dict[str, Any]]:
    if _kill_or_disabled():
        return []
    if not AXIOM_SCAN_ENABLED:
        _log_throttled("AXIOM_DISABLED", 30, level=logging.INFO)
        return []
    if AxiomWeb is None:
        _log_throttled(
            "AXIOM_DEP_MISSING", 60, level=logging.WARNING, err=_AX_IMPORT_ERR or "unknown"
        )
        return []

    ctx = _parse_ctx(*args, **kwargs)
    stop_evt = ctx.stop_evt
    buckets = _Buckets()

    # Ask for extra then dedupe/trim.
    want_per_section = max(1, int(AXIOM_LIMIT_PER_SECTION))
    want_per_section = max(want_per_section, (AXIOM_SCAN_LIMIT // max(1, len(AXIOM_SECTIONS))) + 3)

    last_err: str | None = None
    raw_n: int = 0

    for attempt in range(1, AXIOM_RETRIES + 1):
        if stop_evt is not None and stop_evt.is_set():
            buckets.n("stopped")
            break

        try:
            raw_items = await asyncio.wait_for(
                _fetch_once(want_per_section=want_per_section, stop_evt=stop_evt, buckets=buckets),
                timeout=float(AXIOM_TIMEOUT_SEC),
            )
            raw_n = len(raw_items or [])
            items = _dedupe_by_mint(raw_items or [], limit=AXIOM_SCAN_LIMIT, buckets=buckets)

            # Basic ranking: prefer items that already have rank_score/score_combined, else vol5, else buys5.
            def _key(m: dict[str, Any]) -> tuple[float, float, int]:
                a = float(m.get("rank_score") or m.get("score_combined") or 0.0)
                v = float(m.get("vol5") or 0.0)
                b = int(m.get("buys5") or 0)
                return (a, v, b)

            items.sort(key=_key, reverse=True)

            LOG.info(
                "AXIOM_RESULT candidates=%d raw=%d limit=%d sections=%d attempt=%d/%d",
                len(items),
                raw_n,
                AXIOM_SCAN_LIMIT,
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

            # Log compact reject buckets occasionally
            if buckets.rejects:
                _log_throttled(
                    "AXIOM_REJECT_BUCKETS",
                    12,
                    level=logging.INFO,
                    rejects=dict(
                        sorted(buckets.rejects.items(), key=lambda kv: kv[1], reverse=True)[:10]
                    ),
                    notes=dict(
                        sorted(buckets.notes.items(), key=lambda kv: kv[1], reverse=True)[:8]
                    ),
                )

            return items

        except TimeoutError:
            last_err = f"TimeoutError after {AXIOM_TIMEOUT_SEC}s"
            buckets.r("timeout")
            _log_throttled(
                "AXIOM_TIMEOUT",
                10,
                level=logging.WARNING,
                attempt=attempt,
                timeout_s=AXIOM_TIMEOUT_SEC,
            )
            await _recycle_ax()
        except Exception as e:
            last_err = f"{type(e).__name__}: {e}"
            buckets.r("fetch_exc")
            _log_throttled(
                "AXIOM_FETCH_ERR", 10, level=logging.WARNING, attempt=attempt, err=last_err
            )
            await _recycle_ax()

        if attempt < AXIOM_RETRIES:
            await asyncio.sleep(float(AXIOM_BACKOFF_S) * attempt + random.uniform(0.0, 0.25))

    _log_throttled(
        "AXIOM_GIVE_UP",
        10,
        level=logging.WARNING,
        attempts=AXIOM_RETRIES,
        last_err=last_err or "",
        raw_n=raw_n,
    )
    return []


async def scan_axiom(*args: Any, **kwargs: Any) -> list[dict[str, Any]]:
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
                await scan_once(redis=ctx.redis, stop_evt=stop_evt, q=ctx.q, emit=ctx.emit)

            await asyncio.sleep(
                max(
                    0.05,
                    float(AXIOM_SCAN_INTERVAL_SEC)
                    + random.uniform(-float(AXIOM_SCAN_JITTER_SEC), float(AXIOM_SCAN_JITTER_SEC)),
                )
            )
    finally:
        with contextlib.suppress(Exception):
            await shutdown()


async def run(*args: Any, **kwargs: Any) -> None:
    await run_scanner(*args, **kwargs)


async def main(*args: Any, **kwargs: Any) -> None:
    await run_scanner(*args, **kwargs)


__all__ = ["main", "run", "run_scanner", "scan_axiom", "scan_once", "shutdown"]
