from __future__ import annotations

"""
utils_pkg/tick_stream.py

God-Tier Tick Stream — V3+++ LOCK-IN (production-first)
======================================================

Purpose
-------
A single, canonical "tick bus" that emits normalized tick events for ACTIVE mints
(open positions and/or watched candidates). It feeds:

  ✅ utils_pkg.ai_sell_logic.on_trade_tick(...)   (primary consumer)
  ✅ utils_pkg.data_logger / utils_pkg.ai_db      (best-effort telemetry)
  ✅ optional Redis stream "ticks" for decoupled consumers

Key guarantees
--------------
- Never crashes the bot: all dependencies are best-effort imports.
- Respects Jupiter plan limits: global RPS limiter (default 1 RPS).
- Uses realistic price quoting:
    * Prefer lightning/fast price helpers if present
    * Fall back to router.get_sell_quote(mint, amount_raw, slippage_bps)
- Maintains rolling history per mint for pct/vol metrics.
- Adaptive scheduling/backoff per mint (chronic failures slow down automatically).

Environment
-----------
# enable/disable
TICK_STREAM_ENABLED=1

# cadence / scaling
TICK_STREAM_POLL_S=1.0
TICK_STREAM_DISCOVER_EVERY_S=2.5
TICK_STREAM_MAX_MINTS=64
TICK_STREAM_MAX_PARALLEL=10
TICK_STREAM_HISTORY_LEN=180

# IMPORTANT: global quote rate limit (Basic is fixed 1 RPS)
TICK_STREAM_RPS=1.0

# optional redis stream
TICK_STREAM_PUBLISH_REDIS_STREAM=0
TICK_STREAM_REDIS_STREAM_KEY=ticks

# discovery keys (best-effort)
TICK_ACTIVE_MINTS_KEY=positions:open_mints
TICK_ACTIVE_POSITIONS_KEY=positions:open
TICK_ACTIVE_KEYS=positions:open_mints,positions:open,open_positions,positions

# quote defaults
TICK_QUOTE_SLIPPAGE_BPS=0
TICK_QUOTE_SAMPLE_PCT=0.005
TICK_QUOTE_MIN_TOKEN_UNITS=1
TICK_QUOTE_MAX_SAMPLE_RAW=500000000

# decimals cache TTL
TICK_DECIMALS_TTL_S=3600
"""

import asyncio
import dataclasses
import inspect
import json
import math
import os
import random
import time
from collections import deque
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Any, cast


# -----------------------------------------------------------------------------
# Logging (canonical when available; never breaks import)
# -----------------------------------------------------------------------------
def _build_logger() -> Any:
    """
    Build a logger safely without creating mypy signature conflicts.

    Key rule: do NOT rely on typed kwargs unpacking into get_logger(), because the
    repo uses conditional shims and mypy may “freeze” a narrower Callable type
    when checking the whole package (mypy .).
    """
    # Prefer canonical shim (but treat it as Any to avoid signature drift in typing)
    try:
        from utils_pkg.log_compat import get_logger as _get_logger  # type: ignore[import-not-found]

        gl = cast(Any, _get_logger)

        # Try the richest call first (filtered at runtime, but not typed)
        kw: dict[str, Any] = {"name": "tick_stream", "service": "tick_stream"}
        try:
            sig = inspect.signature(gl)
            allowed = set(sig.parameters.keys())
            kw2 = {k: v for k, v in kw.items() if k in allowed}
        except Exception:
            kw2 = {"name": "tick_stream"}

        try:
            return gl(**kw2)
        except TypeError:
            # Older variants may only accept positional name
            return gl("tick_stream")
    except Exception:
        pass

    # Fallback: stdlib logger
    import logging

    return logging.getLogger("tick_stream")


log = _build_logger()


def _short_exc(e: BaseException, n: int = 300) -> str:
    s = f"{type(e).__name__}: {e}"
    return (s[:n] + "…") if len(s) > n else s


def _log_event(name: str, level: str = "info", **fields: Any) -> None:
    """
    Emit an EventLogger-style event if available, else fall back to std logging.
    Never raises.
    """
    ev = getattr(log, "event", None)
    if callable(ev):
        try:
            ev(name, **fields)
            return
        except Exception:
            pass

    msg = f"{name} {fields}"
    fn = getattr(log, level, None)
    if not callable(fn):
        fn = getattr(log, "info", None)
    if callable(fn):
        try:
            fn(msg)
        except Exception:
            return


# -----------------------------------------------------------------------------
# Env helpers
# -----------------------------------------------------------------------------
_TRUE = {"1", "true", "t", "yes", "y", "on"}
_FALSE = {"0", "false", "f", "no", "n", "off"}


def _env_bool(key: str, default: bool = False) -> bool:
    v = os.getenv(key)
    if v is None:
        return default
    s = str(v).strip().lower()
    if s in _TRUE:
        return True
    if s in _FALSE:
        return False
    return default


def _env_int(key: str, default: int) -> int:
    try:
        return int(float(str(os.getenv(key, str(default))).strip()))
    except Exception:
        return default


def _env_float(key: str, default: float) -> float:
    try:
        return float(str(os.getenv(key, str(default))).strip())
    except Exception:
        return default


def _clamp(x: float, lo: float, hi: float) -> float:
    return lo if x < lo else hi if x > hi else x


def _now_ms() -> int:
    return int(time.time() * 1000)


def _decode(x: Any) -> str:
    if isinstance(x, (bytes, bytearray)):
        return x.decode("utf-8", "ignore")
    return str(x)


async def _maybe_await(x: Any) -> Any:
    if inspect.isawaitable(x):
        return await x
    return x


def _filter_kwargs(fn: Any, kwargs: dict[str, Any]) -> dict[str, Any]:
    try:
        sig = inspect.signature(fn)
        allowed = set(sig.parameters.keys())
        return {k: v for k, v in kwargs.items() if k in allowed}
    except Exception:
        return kwargs


# -----------------------------------------------------------------------------
# Config (DO NOT introduce new env keys — contract tests rely on this)
# -----------------------------------------------------------------------------
TICK_STREAM_ENABLED = _env_bool("TICK_STREAM_ENABLED", False)
TICK_STREAM_POLL_S = max(0.10, _env_float("TICK_STREAM_POLL_S", 1.0))
TICK_STREAM_DISCOVER_EVERY_S = max(0.50, _env_float("TICK_STREAM_DISCOVER_EVERY_S", 2.5))
TICK_STREAM_MAX_MINTS = max(1, _env_int("TICK_STREAM_MAX_MINTS", 64))
TICK_STREAM_MAX_PARALLEL = max(1, _env_int("TICK_STREAM_MAX_PARALLEL", 10))
TICK_STREAM_HISTORY_LEN = max(30, _env_int("TICK_STREAM_HISTORY_LEN", 180))

# Global quote budget (Basic plan is 1 RPS)
TICK_STREAM_RPS = _clamp(_env_float("TICK_STREAM_RPS", 1.0), 0.05, 100.0)

TICK_STREAM_PUBLISH_REDIS_STREAM = _env_bool("TICK_STREAM_PUBLISH_REDIS_STREAM", False)
TICK_STREAM_REDIS_STREAM_KEY = (
    os.getenv("TICK_STREAM_REDIS_STREAM_KEY", "ticks") or "ticks"
).strip()

# discovery keys
TICK_ACTIVE_MINTS_KEY = (
    os.getenv("TICK_ACTIVE_MINTS_KEY", "positions:open_mints") or "positions:open_mints"
).strip()
TICK_ACTIVE_POSITIONS_KEY = (
    os.getenv("TICK_ACTIVE_POSITIONS_KEY", "positions:open") or "positions:open"
).strip()
TICK_ACTIVE_KEYS = [
    s.strip()
    for s in (
        os.getenv(
            "TICK_ACTIVE_KEYS",
            f"{TICK_ACTIVE_MINTS_KEY},{TICK_ACTIVE_POSITIONS_KEY},open_positions,positions",
        )
        or ""
    ).split(",")
    if s.strip()
]

# quote defaults
TICK_QUOTE_SLIPPAGE_BPS = max(0, min(5000, _env_int("TICK_QUOTE_SLIPPAGE_BPS", 0)))
TICK_QUOTE_SAMPLE_PCT = _clamp(_env_float("TICK_QUOTE_SAMPLE_PCT", 0.005), 0.0001, 0.25)
TICK_QUOTE_MIN_TOKEN_UNITS = max(1, _env_int("TICK_QUOTE_MIN_TOKEN_UNITS", 1))
TICK_QUOTE_MAX_SAMPLE_RAW = max(1, _env_int("TICK_QUOTE_MAX_SAMPLE_RAW", 500_000_000))

# decimals cache ttl
TICK_DECIMALS_TTL_S = max(60, _env_int("TICK_DECIMALS_TTL_S", 3600))


# -----------------------------------------------------------------------------
# Best-effort imports (avoid mypy no-redef by importing into temp names, then assigning)
# -----------------------------------------------------------------------------
cu: Any | None = None
router: Any | None = None
L: Any | None = None
ai_sell_logic: Any | None = None

try:
    from utils_pkg import core_utils as _cu  # type: ignore[import-not-found]

    cu = _cu
except Exception:
    cu = None

try:
    from utils_pkg import router as _router  # type: ignore[import-not-found]

    router = _router
except Exception:
    router = None

try:
    from utils_pkg import lightning_impl as _L  # type: ignore[import-not-found]

    L = _L
except Exception:
    L = None

try:
    from utils_pkg import ai_sell_logic as _ai_sell_logic  # type: ignore[import-not-found]

    ai_sell_logic = _ai_sell_logic
except Exception:
    ai_sell_logic = None


# -----------------------------------------------------------------------------
# Data model
# -----------------------------------------------------------------------------
@dataclass
class PositionHint:
    mint: str
    symbol: str = ""
    amount_raw: int = 0
    decimals: int | None = None
    entry_price_sol: float | None = None
    entry_ts_ms: int | None = None
    meta: dict[str, Any] = field(default_factory=dict)


@dataclass
class Tick:
    mint: str
    ts_ms: int
    source: str

    # primary price outputs
    price_sol: float | None = None  # SOL per token (requires decimals)
    lamports_per_raw: float | None = None  # lamports per raw token unit

    # derived metrics
    pct_1m: float | None = None
    pct_5m: float | None = None
    vol_1m: float | None = None

    symbol: str = ""
    quote_amount_raw: int | None = None
    meta: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        d = dataclasses.asdict(self)
        return dict(d)


@dataclass
class _Hist:
    ts_ms: int
    price_sol: float | None
    lpr: float | None


# -----------------------------------------------------------------------------
# Simple global rate limiter (prevents exceeding Basic 1 RPS)
# -----------------------------------------------------------------------------
class _RateLimiter:
    def __init__(self, rps: float) -> None:
        self._min_interval = 1.0 / float(rps) if rps > 0 else 0.0
        self._lock = asyncio.Lock()
        self._next_allowed = 0.0  # loop.time()

    async def wait(self) -> None:
        if self._min_interval <= 0:
            return
        loop = asyncio.get_running_loop()
        async with self._lock:
            now = loop.time()
            if now < self._next_allowed:
                await asyncio.sleep(self._next_allowed - now)
                now = loop.time()
            self._next_allowed = now + self._min_interval


# -----------------------------------------------------------------------------
# Quote parsing helpers
# -----------------------------------------------------------------------------
def _json_loads_best(x: Any) -> Any:
    if x is None:
        return None
    if isinstance(x, (bytes, bytearray)):
        x = _decode(x)
    if isinstance(x, str):
        s = x.strip()
        if not s:
            return None
        if s[0] in "{[":
            try:
                return json.loads(s)
            except Exception:
                return None
        return x
    return x


def _parse_out_lamports(quote: Any) -> int | None:
    q = _json_loads_best(quote)
    if isinstance(q, dict):
        for k in ("outAmount", "out_amount", "outputAmount", "outLamports", "out_lamports"):
            if k in q and q[k] is not None:
                try:
                    return int(str(q[k]))
                except Exception:
                    return None
        data = q.get("data")
        if isinstance(data, dict):
            return _parse_out_lamports(data)
    return None


def _parse_in_amount_raw(quote: Any) -> int | None:
    q = _json_loads_best(quote)
    if isinstance(q, dict):
        for k in ("inAmount", "in_amount", "inputAmount"):
            if k in q and q[k] is not None:
                try:
                    return int(str(q[k]))
                except Exception:
                    return None
        data = q.get("data")
        if isinstance(data, dict):
            return _parse_in_amount_raw(data)
    return None


# -----------------------------------------------------------------------------
# Redis discovery helpers
# -----------------------------------------------------------------------------
async def _get_redis() -> Any:
    if cu is None:
        return None

    for nm in ("get_redis_async", "get_redis"):
        fn = getattr(cu, nm, None)
        if callable(fn):
            try:
                r = fn()
                return await r if inspect.isawaitable(r) else r
            except Exception:
                continue

    return getattr(cu, "redis", None)


def _extract_mint(obj: Any) -> str | None:
    o = _json_loads_best(obj)
    if isinstance(o, str):
        s = o.strip()
        if s and len(s) >= 20 and " " not in s and "{" not in s:
            return s
        return None
    if isinstance(o, dict):
        for k in ("mint", "token_mint", "baseMint", "base_mint", "address", "tokenAddress"):
            v = o.get(k)
            if isinstance(v, str) and v.strip():
                return v.strip()
    return None


def _extract_symbol(obj: Any) -> str:
    o = _json_loads_best(obj)
    if isinstance(o, dict):
        for k in ("symbol", "sym", "ticker"):
            v = o.get(k)
            if isinstance(v, str):
                return v.strip()
    return ""


def _extract_amount_raw(obj: Any) -> int:
    o = _json_loads_best(obj)
    if isinstance(o, dict):
        for k in ("amount_raw", "token_amount_raw", "size_raw", "qty_raw", "balance_raw"):
            v = o.get(k)
            if v is None:
                continue
            try:
                return int(float(str(v)))
            except Exception:
                continue
    return 0


def _extract_decimals(obj: Any) -> int | None:
    o = _json_loads_best(obj)
    if isinstance(o, dict):
        for k in ("decimals", "token_decimals", "mint_decimals"):
            v = o.get(k)
            if v is None:
                continue
            try:
                d = int(float(str(v)))
                return d if 0 <= d <= 18 else None
            except Exception:
                continue
    return None


async def _redis_type(r: Any, key: str) -> str:
    try:
        t = await _maybe_await(r.type(key))
        return _decode(t).lower()
    except Exception:
        return ""


async def _discover_from_redis(r: Any, keys: list[str]) -> dict[str, PositionHint]:
    out: dict[str, PositionHint] = {}

    for key in keys:
        if not key:
            continue

        kt = await _redis_type(r, key)
        if not kt:
            continue

        try:
            if kt == "set":
                members = await _maybe_await(r.smembers(key))
                for m in members or []:
                    mint = _extract_mint(m)
                    if mint:
                        out.setdefault(
                            mint, PositionHint(mint=mint, meta={"src": "redis:set", "key": key})
                        )
                continue

            if kt == "zset":
                members = await _maybe_await(r.zrange(key, 0, -1))
                for m in members or []:
                    mint = _extract_mint(m)
                    if mint:
                        out.setdefault(
                            mint, PositionHint(mint=mint, meta={"src": "redis:zset", "key": key})
                        )
                continue

            if kt == "hash":
                vals = await _maybe_await(r.hvals(key))
                if vals:
                    for v in vals:
                        mint = _extract_mint(v)
                        if not mint:
                            continue
                        ph = out.get(mint) or PositionHint(
                            mint=mint, meta={"src": "redis:hash", "key": key}
                        )
                        ph.symbol = ph.symbol or _extract_symbol(v)
                        ph.amount_raw = max(ph.amount_raw, _extract_amount_raw(v))
                        ph.decimals = (
                            ph.decimals if ph.decimals is not None else _extract_decimals(v)
                        )
                        out[mint] = ph
                else:
                    ks = await _maybe_await(r.hkeys(key))
                    for k in ks or []:
                        mint = _extract_mint(k)
                        if mint:
                            out.setdefault(
                                mint,
                                PositionHint(mint=mint, meta={"src": "redis:hkeys", "key": key}),
                            )
                continue

            if kt == "list":
                vals = await _maybe_await(r.lrange(key, 0, -1))
                for v in vals or []:
                    mint = _extract_mint(v)
                    if not mint:
                        continue
                    ph = out.get(mint) or PositionHint(
                        mint=mint, meta={"src": "redis:list", "key": key}
                    )
                    ph.symbol = ph.symbol or _extract_symbol(v)
                    ph.amount_raw = max(ph.amount_raw, _extract_amount_raw(v))
                    ph.decimals = ph.decimals if ph.decimals is not None else _extract_decimals(v)
                    out[mint] = ph
                continue

            if kt == "string":
                v = await _maybe_await(r.get(key))
                mint = _extract_mint(v)
                if mint:
                    ph = out.get(mint) or PositionHint(
                        mint=mint, meta={"src": "redis:string", "key": key}
                    )
                    ph.symbol = ph.symbol or _extract_symbol(v)
                    ph.amount_raw = max(ph.amount_raw, _extract_amount_raw(v))
                    ph.decimals = ph.decimals if ph.decimals is not None else _extract_decimals(v)
                    out[mint] = ph
                continue

        except Exception as e:
            _log_event("TICK_DISCOVER_KEY_ERR", level="warning", key=key, err=_short_exc(e))

    return out


async def _discover_from_ai_sell_logic() -> dict[str, PositionHint]:
    out: dict[str, PositionHint] = {}
    if ai_sell_logic is None:
        return out

    fn = getattr(ai_sell_logic, "get_open_mints", None)
    if callable(fn):
        try:
            mints = await _maybe_await(fn())
        except Exception:
            mints = None

        for m in mints or []:
            mint_str = str(m).strip()
            if mint_str:
                out.setdefault(
                    mint_str,
                    PositionHint(mint=mint_str, meta={"src": "ai_sell_logic:get_open_mints"}),
                )
        return out

    for nm in (
        "positions_snapshot",
        "get_positions_snapshot",
        "snapshot_positions",
        "get_positions",
    ):
        fn2 = getattr(ai_sell_logic, nm, None)
        if not callable(fn2):
            continue

        try:
            snap = await _maybe_await(fn2())
        except Exception:
            continue

        items: list[Any] = []
        if isinstance(snap, dict):
            items = list(snap.values())
        elif isinstance(snap, list):
            items = snap

        for it in items:
            mint_opt = _extract_mint(it)
            if not mint_opt:
                continue

            ph = out.get(mint_opt) or PositionHint(
                mint=mint_opt, meta={"src": f"ai_sell_logic:{nm}"}
            )
            ph.symbol = ph.symbol or _extract_symbol(it)
            ph.amount_raw = max(ph.amount_raw, _extract_amount_raw(it))
            ph.decimals = ph.decimals if ph.decimals is not None else _extract_decimals(it)
            out[mint_opt] = ph

        if out:
            return out

    return out


# -----------------------------------------------------------------------------
# Decimals cache (TTL) + best-effort fetch
# -----------------------------------------------------------------------------
_DEC_CACHE: dict[str, tuple[int, float]] = {}  # mint -> (decimals, expires_at_s)


async def _decimals_best_effort(mint: str) -> int | None:
    if not mint:
        return None

    now = time.time()
    cached = _DEC_CACHE.get(mint)
    if cached and cached[1] > now:
        return cached[0]

    if cu is not None:
        for nm in ("get_token_decimals", "token_decimals", "get_mint_decimals"):
            fn = getattr(cu, nm, None)
            if callable(fn):
                try:
                    d = await _maybe_await(fn(mint))
                except Exception:
                    continue
                if d is None:
                    continue
                try:
                    d2 = int(d)
                except Exception:
                    continue
                if 0 <= d2 <= 18:
                    _DEC_CACHE[mint] = (d2, now + TICK_DECIMALS_TTL_S)
                    return d2

    if router is not None:
        for nm in ("get_token_decimals", "get_mint_decimals", "token_decimals"):
            fn = getattr(router, nm, None)
            if callable(fn):
                try:
                    d = await _maybe_await(fn(mint))
                except Exception:
                    continue
                if d is None:
                    continue
                try:
                    d2 = int(d)
                except Exception:
                    continue
                if 0 <= d2 <= 18:
                    _DEC_CACHE[mint] = (d2, now + TICK_DECIMALS_TTL_S)
                    return d2

    return None


# -----------------------------------------------------------------------------
# Metrics helpers
# -----------------------------------------------------------------------------
def _pct(a: float | None, b: float | None) -> float | None:
    if a is None or b is None or a <= 0 or b <= 0:
        return None
    return ((b / a) - 1.0) * 100.0


def _vol_approx(prices: list[float]) -> float | None:
    if len(prices) < 6:
        return None
    rets: list[float] = []
    for i in range(1, len(prices)):
        p0 = prices[i - 1]
        p1 = prices[i]
        if p0 > 0 and p1 > 0:
            rets.append((p1 / p0) - 1.0)
    if len(rets) < 5:
        return None
    m = sum(rets) / len(rets)
    var = sum((r - m) ** 2 for r in rets) / max(1, (len(rets) - 1))
    return (var**0.5) * 100.0


# -----------------------------------------------------------------------------
# Telemetry sinks (best-effort)
# -----------------------------------------------------------------------------
async def _telemetry_tick(t: Tick) -> None:
    if ai_sell_logic is not None:
        fn = getattr(ai_sell_logic, "on_trade_tick", None)
        if callable(fn):
            try:
                out = fn(t.to_dict())
                if inspect.isawaitable(out):
                    await out
            except Exception:
                pass

    try:
        from utils_pkg import data_logger  # type: ignore[import-not-found]

        fn = getattr(data_logger, "log_event", None)
        if callable(fn):
            out = fn({"type": "tick", **t.to_dict()})
            if inspect.isawaitable(out):
                await out
    except Exception:
        pass

    try:
        from utils_pkg import ai_db  # type: ignore[import-not-found]

        fn = getattr(ai_db, "log_event", None)
        if callable(fn):
            out = fn({"type": "tick", **t.to_dict()})
            if inspect.isawaitable(out):
                await out
    except Exception:
        pass

    if TICK_STREAM_PUBLISH_REDIS_STREAM:
        r = await _get_redis()
        if r is not None:
            try:
                payload = t.to_dict()
                fields: dict[str, str] = {}
                for k, v in payload.items():
                    if isinstance(v, (dict, list)):
                        fields[k] = json.dumps(v, separators=(",", ":"), ensure_ascii=False)
                    else:
                        fields[k] = "" if v is None else str(v)
                await _maybe_await(
                    r.xadd(TICK_STREAM_REDIS_STREAM_KEY, fields, maxlen=20000, approximate=True)
                )
            except Exception:
                pass


# -----------------------------------------------------------------------------
# Price oracle
# -----------------------------------------------------------------------------
async def _get_price_via_lightning(
    mint: str,
) -> tuple[float | None, float | None, str, dict[str, Any]]:
    if L is None:
        return None, None, "none", {}

    for nm in ("get_token_price_sol", "get_price_sol", "price_sol"):
        fn = getattr(L, nm, None)
        if not callable(fn):
            continue
        try:
            out = await _maybe_await(fn(mint))
        except Exception:
            continue
        if out is None:
            continue
        try:
            p = float(out)
        except Exception:
            continue
        if p > 0 and math.isfinite(p):
            return p, None, f"lightning:{nm}", {}

    return None, None, "none", {}


async def _get_price_via_sell_quote(
    mint: str,
    hint: PositionHint,
    limiter: _RateLimiter,
    *,
    quote_fn: Any,
    source: str,
) -> tuple[float | None, float | None, str, dict[str, Any]]:
    if not callable(quote_fn):
        return None, None, "none", {}

    full_raw = int(hint.amount_raw or 0)

    dec = hint.decimals
    if dec is None:
        dec = await _decimals_best_effort(mint)

    min_raw = 1
    if dec is not None and 0 <= int(dec) <= 18:
        try:
            min_raw = int(TICK_QUOTE_MIN_TOKEN_UNITS) * (10 ** int(dec))
        except Exception:
            min_raw = 1

    if full_raw > 0:
        sample = int(max(min_raw, min(full_raw, int(full_raw * float(TICK_QUOTE_SAMPLE_PCT)))))
    else:
        sample = int(max(1, min_raw))

    sample = int(min(sample, int(TICK_QUOTE_MAX_SAMPLE_RAW)))
    sample = max(1, sample)

    kwargs = dict(mint=mint, amount_raw=int(sample), slippage_bps=int(TICK_QUOTE_SLIPPAGE_BPS))
    kwargs = _filter_kwargs(quote_fn, kwargs)

    await limiter.wait()

    try:
        quote = await _maybe_await(quote_fn(**kwargs))
    except Exception:
        return None, None, source, {}

    out_lamports = _parse_out_lamports(quote)
    in_raw = _parse_in_amount_raw(quote) or sample

    if out_lamports is None or in_raw <= 0:
        return None, None, source, {"quote": quote}

    lamports_per_raw = float(out_lamports) / float(in_raw)

    price_sol: float | None = None
    if dec is not None and 0 <= int(dec) <= 18:
        price_sol = (lamports_per_raw * float(10 ** int(dec))) / 1e9

    meta: dict[str, Any] = {"quote_amount_raw": int(in_raw), "out_lamports": int(out_lamports)}
    if dec is not None:
        meta["decimals"] = int(dec)
    return price_sol, lamports_per_raw, source, meta


async def _get_price_via_router_quote(
    mint: str, hint: PositionHint, limiter: _RateLimiter
) -> tuple[float | None, float | None, str, dict[str, Any]]:
    if router is not None:
        fn = getattr(router, "get_sell_quote", None)
        if callable(fn):
            return await _get_price_via_sell_quote(
                mint,
                hint,
                limiter,
                quote_fn=fn,
                source="router:get_sell_quote",
            )

    if L is not None:
        fn2 = getattr(L, "get_sell_quote", None)
        if callable(fn2):
            return await _get_price_via_sell_quote(
                mint,
                hint,
                limiter,
                quote_fn=fn2,
                source="lightning:get_sell_quote",
            )

    return None, None, "none", {}


# -----------------------------------------------------------------------------
# TickStream
# -----------------------------------------------------------------------------
TickCallback = Callable[[Tick], Awaitable[None]]


@dataclass
class _MintState:
    hint: PositionHint
    fails: int = 0
    next_due_s: float = 0.0


class TickStream:
    def __init__(
        self,
        *,
        poll_s: float = TICK_STREAM_POLL_S,
        discover_every_s: float = TICK_STREAM_DISCOVER_EVERY_S,
        max_mints: int = TICK_STREAM_MAX_MINTS,
        max_parallel: int = TICK_STREAM_MAX_PARALLEL,
        history_len: int = TICK_STREAM_HISTORY_LEN,
        rps: float = TICK_STREAM_RPS,
    ) -> None:
        self.poll_s = float(max(0.10, poll_s))
        self.discover_every_s = float(max(0.50, discover_every_s))
        self.max_mints = int(max(1, max_mints))
        self.max_parallel = int(max(1, max_parallel))
        self.history_len = int(max(30, history_len))

        self._stop = asyncio.Event()
        self._runner: asyncio.Task[None] | None = None
        self._discover_task: asyncio.Task[None] | None = None

        self._callbacks: list[TickCallback] = []

        self._sem = asyncio.Semaphore(self.max_parallel)
        self._limiter = _RateLimiter(rps=float(rps))

        self._mints: dict[str, _MintState] = {}
        self._hist: dict[str, deque[_Hist]] = {}

    def subscribe(self, cb: TickCallback) -> None:
        self._callbacks.append(cb)

    async def start(self) -> None:
        if self._runner and not self._runner.done():
            return
        self._stop.clear()
        self._discover_task = asyncio.create_task(self._discover_loop(), name="tick_discover")
        self._runner = asyncio.create_task(self._run_loop(), name="tick_stream")

    async def stop(self) -> None:
        self._stop.set()
        for t in (self._discover_task, self._runner):
            if t:
                t.cancel()
        try:
            await asyncio.gather(
                *(t for t in (self._discover_task, self._runner) if t), return_exceptions=True
            )
        except Exception:
            pass

    async def _discover_loop(self) -> None:
        while not self._stop.is_set():
            try:
                await self.discover_active()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                _log_event("TICK_DISCOVER_ERR", level="warning", err=_short_exc(e))
            await asyncio.sleep(self.discover_every_s)

    async def discover_active(self) -> None:
        merged: dict[str, PositionHint] = {}

        try:
            merged.update(await _discover_from_ai_sell_logic())
        except Exception:
            pass

        r = await _get_redis()
        if r is not None:
            try:
                found = await _discover_from_redis(r, TICK_ACTIVE_KEYS)
            except Exception:
                found = {}

            for mint, ph in found.items():
                if mint in merged:
                    cur = merged[mint]
                    cur.symbol = cur.symbol or ph.symbol
                    cur.amount_raw = max(cur.amount_raw, ph.amount_raw)
                    cur.decimals = cur.decimals if cur.decimals is not None else ph.decimals
                    cur.meta.update(ph.meta or {})
                    merged[mint] = cur
                else:
                    merged[mint] = ph

        cleaned: dict[str, PositionHint] = {}
        for mint, ph in merged.items():
            m = str(mint or "").strip()
            if m:
                cleaned[m] = ph

        ranked = sorted(
            cleaned.values(),
            key=lambda ph: (-int(ph.amount_raw > 0), -(ph.amount_raw or 0), ph.mint),
        )[: self.max_mints]

        loop = asyncio.get_running_loop()
        now = loop.time()

        new_set = {ph.mint: ph for ph in ranked}
        old_keys = set(self._mints.keys())
        new_keys = set(new_set.keys())

        for g in old_keys - new_keys:
            self._mints.pop(g, None)
            self._hist.pop(g, None)

        for mint, ph in new_set.items():
            st = self._mints.get(mint)
            if st is None:
                jitter = random.uniform(0.0, min(0.5, self.poll_s))
                self._mints[mint] = _MintState(hint=ph, fails=0, next_due_s=now + jitter)
            else:
                st.hint = ph
                self._mints[mint] = st

        _log_event("TICK_ACTIVE_REFRESH", count=len(self._mints))

    def _update_hist_and_metrics(self, mint: str, tick: Tick) -> None:
        h = self._hist.get(mint)
        if h is None:
            h = deque(maxlen=self.history_len)
            self._hist[mint] = h

        h.append(_Hist(ts_ms=tick.ts_ms, price_sol=tick.price_sol, lpr=tick.lamports_per_raw))

        use_sol = tick.price_sol is not None
        cur = tick.price_sol if use_sol else tick.lamports_per_raw
        if cur is None:
            return

        def price_at(age_ms: int) -> float | None:
            target = tick.ts_ms - age_ms
            for it in reversed(h):
                if it.ts_ms <= target:
                    return it.price_sol if use_sol else it.lpr
            if h:
                it0 = h[0]
                return it0.price_sol if use_sol else it0.lpr
            return None

        p1 = price_at(60_000)
        p5 = price_at(300_000)
        tick.pct_1m = _pct(p1, cur)
        tick.pct_5m = _pct(p5, cur)

        cutoff = tick.ts_ms - 60_000
        window: list[float] = []
        for it in h:
            if it.ts_ms >= cutoff:
                v = it.price_sol if use_sol else it.lpr
                if v is not None:
                    window.append(float(v))
        tick.vol_1m = _vol_approx(window)

    async def _emit(self, tick: Tick) -> None:
        for cb in self._callbacks:
            try:
                await cb(tick)
            except Exception:
                pass
        try:
            await _telemetry_tick(tick)
        except Exception:
            pass

    async def _poll_one(self, mint: str, st: _MintState) -> None:
        async with self._sem:
            hint = st.hint
            ts = _now_ms()

            price_sol, lpr, src, meta = await _get_price_via_lightning(mint)
            if price_sol is None and lpr is None:
                price_sol, lpr, src, meta = await _get_price_via_router_quote(
                    mint, hint, self._limiter
                )

            ok = (price_sol is not None) or (lpr is not None)
            loop = asyncio.get_running_loop()
            now = loop.time()

            if not ok:
                st.fails = min(50, st.fails + 1)
                backoff = min(20.0, self.poll_s * (2.0 ** min(6, st.fails)))
                st.next_due_s = now + backoff + random.uniform(0.0, 0.25)
                self._mints[mint] = st
                return

            st.fails = max(0, st.fails - 1)
            st.next_due_s = now + self.poll_s + random.uniform(0.0, min(0.25, self.poll_s))
            self._mints[mint] = st

            meta_dict = dict(meta or {})
            qv = meta_dict.get("quote_amount_raw")

            qar: int | None = None
            if qv is not None:
                try:
                    qar = int(qv)
                except Exception:
                    qar = None

            t = Tick(
                mint=mint,
                ts_ms=ts,
                source=src,
                price_sol=price_sol,
                lamports_per_raw=lpr,
                symbol=hint.symbol or "",
                quote_amount_raw=qar,
                meta=meta_dict,
            )

            self._update_hist_and_metrics(mint, t)
            await self._emit(t)

    async def _run_loop(self) -> None:
        if not TICK_STREAM_ENABLED:
            _log_event("TICK_STREAM_DISABLED", level="warning")
            return

        _log_event(
            "TICK_STREAM_START",
            poll_s=self.poll_s,
            discover_every_s=self.discover_every_s,
            max_mints=self.max_mints,
            max_parallel=self.max_parallel,
            history_len=self.history_len,
            rps=TICK_STREAM_RPS,
            publish_redis_stream=int(TICK_STREAM_PUBLISH_REDIS_STREAM),
            redis_stream_key=TICK_STREAM_REDIS_STREAM_KEY,
        )

        try:
            await self.discover_active()
        except Exception:
            pass

        loop = asyncio.get_running_loop()

        while not self._stop.is_set():
            try:
                if not self._mints:
                    await asyncio.sleep(self.poll_s)
                    continue

                now = loop.time()
                due: list[tuple[str, _MintState]] = [
                    (m, st) for (m, st) in self._mints.items() if now >= float(st.next_due_s)
                ]

                if not due:
                    soonest = min(st.next_due_s for st in self._mints.values())
                    await asyncio.sleep(max(0.05, min(0.25, soonest - now)))
                    continue

                due = due[: max(1, self.max_parallel * 2)]

                tasks = [
                    asyncio.create_task(self._poll_one(m, st), name=f"tick:{m[:6]}")
                    for m, st in due
                ]
                await asyncio.gather(*tasks, return_exceptions=True)

            except asyncio.CancelledError:
                raise
            except Exception as e:
                _log_event("TICK_STREAM_ERR", level="warning", err=_short_exc(e))
                await asyncio.sleep(max(0.10, self.poll_s))


# -----------------------------------------------------------------------------
# Public helpers (singleton)
# -----------------------------------------------------------------------------
_stream_singleton: TickStream | None = None


async def start_tick_stream() -> TickStream | None:
    global _stream_singleton
    if _stream_singleton is None:
        _stream_singleton = TickStream()
    await _stream_singleton.start()
    return _stream_singleton


async def stop_tick_stream() -> None:
    global _stream_singleton
    if _stream_singleton is not None:
        await _stream_singleton.stop()
        _stream_singleton = None


async def run_forever() -> None:
    ts = TickStream()
    await ts.start()
    try:
        while True:
            await asyncio.sleep(3600)
    finally:
        await ts.stop()


__all__ = [
    "PositionHint",
    "Tick",
    "TickStream",
    "run_forever",
    "start_tick_stream",
    "stop_tick_stream",
]
