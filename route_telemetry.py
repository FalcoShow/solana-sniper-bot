from __future__ import annotations

"""
route_telemetry.py (ROOT) — V3+++ (God-Tier Overhaul)
=====================================================

Purpose
-------
Routing telemetry MUST be:
- Safe (never raises)
- Non-blocking (best-effort; bounded latency)
- No network at import time
- Single-source (uses shared Redis if available via utils_pkg.core_utils)

This module is intentionally lightweight and dependency-tolerant.
It supports:
- DEX label histograms (route used)
- Watchdog deviation histograms
- Optional extra counters for route decisions / quote outcomes (future-proof)

Backwards compatibility
-----------------------
Keeps the legacy keys by default:
  route:dex_used:<LABEL>
  route:dex_watchdog:<LABEL>

You can change prefix via ROUTE_TELEM_PREFIX, but default is "route".
"""

import asyncio
import contextlib
import os
import time
from collections.abc import Awaitable, Iterable, Sequence
from dataclasses import dataclass
from typing import Any

# -----------------------------------------------------------------------------
# Logging (shim-safe, B009-safe)
# -----------------------------------------------------------------------------
try:
    from logging_setup import get_logger  # type: ignore

    _LOG = get_logger(__name__)  # type: ignore
except Exception:  # pragma: no cover
    import logging

    _LOG = logging.getLogger(__name__)


_EVERY_STORE: dict[str, float] = {}


def _log_event(name: str, **fields: Any) -> None:
    """Never raises; prefers structured logger if available."""
    try:
        _LOG.event(name, **fields)  # type: ignore[attr-defined]
        return
    except Exception:
        pass
    with contextlib.suppress(Exception):
        _LOG.info("%s %s", name, fields)


def _log_every(key: str, n: int = 30, msg: str | None = None, **fields: Any) -> None:
    """Rate-limited log helper. n is in seconds. Never raises."""
    try:
        sec = float(max(1, int(n)))
        now_mono = time.monotonic()
        last_mono = float(_EVERY_STORE.get(key, 0.0))
        if (now_mono - last_mono) >= sec:
            _EVERY_STORE[key] = now_mono
            _log_event(msg or key, **fields)
    except Exception:
        return


# -----------------------------------------------------------------------------
# Env + config
# -----------------------------------------------------------------------------
_TRUE = {"1", "true", "yes", "y", "on", "t"}
_FALSE = {"0", "false", "no", "n", "off", "f"}


def _envs(name: str, default: str = "") -> str:
    raw = os.getenv(name)
    return (raw if raw is not None else default).strip()


def _envb(name: str, default: bool = False) -> bool:
    raw = _envs(name, "")
    if raw == "":
        return bool(default)
    low = raw.lower().strip()
    if low in _TRUE:
        return True
    if low in _FALSE:
        return False
    return bool(default)


def _envi(name: str, default: int) -> int:
    try:
        return int(float(_envs(name, str(default))))
    except Exception:
        return int(default)


ROUTE_TELEM_ENABLED: bool = _envb("ROUTE_TELEM_ENABLED", True)
ROUTE_TELEM_PREFIX: str = _envs("ROUTE_TELEM_PREFIX", "route")  # legacy default
ROUTE_TELEM_TIMEOUT_MS: int = _envi("ROUTE_TELEM_TIMEOUT_MS", 350)
ROUTE_TELEM_MAX_LABELS: int = _envi("ROUTE_TELEM_MAX_LABELS", 12)
ROUTE_TELEM_LABEL_MAXLEN: int = _envi("ROUTE_TELEM_LABEL_MAXLEN", 32)

# If prefix differs from "route", optionally mirror legacy keys too.
ROUTE_TELEM_MIRROR_LEGACY_KEYS: bool = _envb(
    "ROUTE_TELEM_MIRROR_LEGACY_KEYS", ROUTE_TELEM_PREFIX != "route"
)


# -----------------------------------------------------------------------------
# Label normalization (cardinality-safe)
# -----------------------------------------------------------------------------
def _norm(label: str) -> str:
    text = (label or "OTHER").strip().upper().replace(" ", "_")
    buf: list[str] = []
    for ch in text:
        if ch.isalnum() or ch in {"_", "-", "."}:
            buf.append(ch)
        else:
            buf.append("_")
    out = "".join(buf).strip("_") or "OTHER"
    max_len = max(8, int(ROUTE_TELEM_LABEL_MAXLEN))
    if len(out) > max_len:
        out = out[:max_len]
    return out


def _uniq_norm(labels: Iterable[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    max_labels = max(1, int(ROUTE_TELEM_MAX_LABELS))
    for raw in labels:
        if raw is None:
            continue
        lbl = _norm(str(raw))
        if lbl in seen:
            continue
        seen.add(lbl)
        out.append(lbl)
        if len(out) >= max_labels:
            break
    return out or ["OTHER"]


def _keys_for(kind: str, labels: Sequence[str]) -> tuple[list[str], list[str]]:
    """
    Returns (primary_keys, legacy_mirror_keys).
    Primary keys always use ROUTE_TELEM_PREFIX.
    Legacy mirror keys always use "route" prefix if enabled.
    """
    primary_keys = [f"{ROUTE_TELEM_PREFIX}:{kind}:{lbl}" for lbl in labels]
    legacy_keys: list[str] = []
    if ROUTE_TELEM_MIRROR_LEGACY_KEYS and ROUTE_TELEM_PREFIX != "route":
        legacy_keys = [f"route:{kind}:{lbl}" for lbl in labels]
    return primary_keys, legacy_keys


# -----------------------------------------------------------------------------
# Redis backend (shared-first, safe, cached; no import-time network)
# -----------------------------------------------------------------------------
@dataclass(frozen=True)
class _Cfg:
    timeout_ms: int = ROUTE_TELEM_TIMEOUT_MS


class _NoopPipe:
    def incrby(self, *_args: Any, **_kwargs: Any) -> "_NoopPipe":
        return self

    async def execute(self) -> None:
        return None


class _NoopRedis:
    async def incrby(self, *_args: Any, **_kwargs: Any) -> None:
        return None

    def pipeline(self, *_args: Any, **_kwargs: Any) -> _NoopPipe:
        return _NoopPipe()


class _RedisManager:
    def __init__(self, cfg: _Cfg) -> None:
        self._cfg = cfg
        self._lock = asyncio.Lock()
        self._client: Any = None
        self._last_err_mono = 0.0

    async def _try_shared(self) -> Any | None:
        try:
            from utils_pkg import core_utils as cu  # type: ignore

            fn = None
            with contextlib.suppress(Exception):
                fn = cu.get_redis_async  # type: ignore[attr-defined]
            if fn is not None:
                v = fn()
                if asyncio.iscoroutine(v):
                    v = await v
                return v

            fn2 = None
            with contextlib.suppress(Exception):
                fn2 = cu.get_redis  # type: ignore[attr-defined]
            if fn2 is not None:
                v2 = fn2()
                if asyncio.iscoroutine(v2):
                    v2 = await v2
                return v2
        except Exception:
            return None
        return None

    async def _try_direct(self) -> Any:
        try:
            import redis.asyncio as aioredis  # type: ignore

            redis_url = _envs("REDIS_URL", "redis://localhost:6379")
            if not redis_url:
                return _NoopRedis()
            return aioredis.from_url(redis_url, decode_responses=True)
        except Exception:
            return _NoopRedis()

    async def get(self) -> Any:
        if self._client is not None:
            return self._client

        async with self._lock:
            if self._client is not None:
                return self._client

            shared = await self._try_shared()
            if shared is not None:
                self._client = shared
                return self._client

            self._client = await self._try_direct()
            return self._client

    def mark_err(self) -> None:
        self._last_err_mono = time.monotonic()

    def err_recent(self, sec: float = 10.0) -> bool:
        return (time.monotonic() - float(self._last_err_mono)) < float(sec)


_RM = _RedisManager(_Cfg())

# Lightweight in-memory fallback (used if Redis missing/errors)
_MEM: dict[str, int] = {}
_MEM_LOCK = asyncio.Lock()


async def _with_timeout(task: Awaitable[Any], timeout_ms: int) -> Any:
    try:
        return await asyncio.wait_for(task, timeout=float(timeout_ms) / 1000.0)
    except Exception:
        return None


async def _mem_incr(keys: Sequence[str], delta: int = 1) -> None:
    async with _MEM_LOCK:
        inc = int(delta)
        for key in keys:
            _MEM[key] = int(_MEM.get(key, 0)) + inc


async def _redis_incr(keys: Sequence[str], delta: int = 1) -> None:
    client = await _RM.get()

    if isinstance(client, _NoopRedis):
        await _mem_incr(keys, delta=delta)
        return

    try:
        pipe = None
        with contextlib.suppress(Exception):
            pipe = client.pipeline(transaction=False)

        if pipe is not None:
            for key in keys:
                with contextlib.suppress(Exception):
                    pipe.incrby(key, int(delta))
            res = pipe.execute()
            if asyncio.iscoroutine(res):
                await _with_timeout(res, ROUTE_TELEM_TIMEOUT_MS)
            else:
                await _with_timeout(asyncio.sleep(0), ROUTE_TELEM_TIMEOUT_MS)
            return

        for key in keys:
            with contextlib.suppress(Exception):
                res2 = client.incrby(key, int(delta))
                if asyncio.iscoroutine(res2):
                    await _with_timeout(res2, ROUTE_TELEM_TIMEOUT_MS)

    except Exception as err:
        _RM.mark_err()
        await _mem_incr(keys, delta=delta)
        _log_every(
            "route_telem_redis_err",
            n=30,
            msg="ROUTE_TELEM_REDIS_ERR",
            err=str(err)[:240],
        )


# -----------------------------------------------------------------------------
# Public API (never raises)
# -----------------------------------------------------------------------------
async def count_route_used(labels: Iterable[str]) -> None:
    """
    Increment histogram counters for which DEX labels were used in the chosen route.
    Keys:
      <PREFIX>:dex_used:<LABEL>
      (optionally) route:dex_used:<LABEL>  [legacy mirror]
    """
    if not ROUTE_TELEM_ENABLED:
        return None
    try:
        norm_labels = _uniq_norm(labels)
        primary, legacy = _keys_for("dex_used", norm_labels)
        await _redis_incr(primary + legacy, delta=1)
    except Exception:
        return None


async def count_watchdog_hit(labels: Iterable[str]) -> None:
    """
    Increment histogram counters for watchdog deviation alerts by DEX label.
    Keys:
      <PREFIX>:dex_watchdog:<LABEL>
      (optionally) route:dex_watchdog:<LABEL>  [legacy mirror]
    """
    if not ROUTE_TELEM_ENABLED:
        return None
    try:
        norm_labels = _uniq_norm(labels)
        primary, legacy = _keys_for("dex_watchdog", norm_labels)
        await _redis_incr(primary + legacy, delta=1)
    except Exception:
        return None


# -----------------------------------------------------------------------------
# Extra “God-tier” telemetry helpers (optional for callers)
# -----------------------------------------------------------------------------
async def count_route_decision(
    action: str,
    reason: str = "",
    labels: Iterable[str] | None = None,
) -> None:
    """
    Optional: increment allow/block counters with reasons (cardinality-safe).

    Keys:
      <PREFIX>:decision:<ACTION>
      <PREFIX>:decision_reason:<ACTION>:<REASON>
      <PREFIX>:decision_by_dex:<ACTION>:<DEX> (if labels provided)

    NOTE: We do NOT mirror legacy keys for expanded counters; only for the legacy histograms.
    """
    if not ROUTE_TELEM_ENABLED:
        return None
    try:
        act = _norm(action or "UNKNOWN")
        rsn = _norm(reason or "UNKNOWN")

        keys: list[str] = [
            f"{ROUTE_TELEM_PREFIX}:decision:{act}",
            f"{ROUTE_TELEM_PREFIX}:decision_reason:{act}:{rsn}",
        ]

        if labels is not None:
            norm_labels = _uniq_norm(labels)
            keys.extend(
                [f"{ROUTE_TELEM_PREFIX}:decision_by_dex:{act}:{lbl}" for lbl in norm_labels]
            )

        await _redis_incr(keys, delta=1)
    except Exception:
        return None


async def count_quote_outcome(kind: str, ok: bool, status: int | None = None) -> None:
    """
    Optional: quote outcome counters (helps diagnose "why no routes").

    Keys:
      <PREFIX>:quote:<KIND>:OK / FAIL
      <PREFIX>:quote:<KIND>:STATUS:<CODE> (if status provided)
    """
    if not ROUTE_TELEM_ENABLED:
        return None
    try:
        kind_norm = _norm(kind or "QUOTE")
        keys = [f"{ROUTE_TELEM_PREFIX}:quote:{kind_norm}:{'OK' if ok else 'FAIL'}"]
        if status is not None:
            keys.append(f"{ROUTE_TELEM_PREFIX}:quote:{kind_norm}:STATUS:{int(status)}")
        await _redis_incr(keys, delta=1)
    except Exception:
        return None


async def snapshot_memory() -> dict[str, int]:
    """
    Debug helper: returns the in-memory counters (only used when Redis missing/errors).
    """
    async with _MEM_LOCK:
        return dict(_MEM)


__all__ = [
    "count_quote_outcome",
    "count_route_decision",
    "count_route_used",
    "count_watchdog_hit",
    "snapshot_memory",
]
