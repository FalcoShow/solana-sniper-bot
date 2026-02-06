from __future__ import annotations

"""
utils_pkg/guardrails.py

Guardrails — God-Tier Safety Gate (Phase 0–7 LOCK-IN)
=====================================================

Single source of truth for: "are buys allowed right now?"

Hard guarantees
---------------
- NEVER crashes the bot: all public APIs are best-effort + exception-suppressed.
- Safe at import time (no network I/O).
- Works with or without Redis (process-local fallback).
- Cross-process consistent when Redis exists (hash fields + safe updates).
- Deterministic, stable block reasons (for logs/alerts/debug).
- Operator toggles are LIVE (env read each call; knobs cached with tiny TTL).
- Supports: daily caps + percent caps + breaker + streak caps + exposure caps.
- Optional: publish equity_sol + sol_usd to Redis for shared gating.

Stable public contract (do not break)
-------------------------------------
buys_allowed() -> bool
buy_block_reason() -> str
status() -> (bool, str, dict)
snapshot() -> dict

record_trade_open(notional_sol, mint=None, **kw) -> None
note_close(realized_pnl_sol, mint=None, close_notional_sol=None, **kw) -> None
record_buy_result(ok, reason, sig=None, mint=None, **kw) -> None
trip_breaker(seconds=None, minutes=None, reason="breaker") -> None

Optional helpers (safe if unused)
---------------------------------
buys_allowed_for(mint) -> bool
buy_block_reason_for(mint) -> str
set_equity_sol(sol) -> None
set_sol_usd(price) -> None
reset_today_state() -> None

Phase 2 compliance
------------------
- NO monkey-patching logger objects. We wrap a base logger in a small adapter
  that provides .event and .every without assigning methods to logger instances.

Notes
-----
This file previously triggered mypy "no-redef" on optional fallbacks because
annotated names were reintroduced via `import ... as NAME` / `from ... import ... as NAME`.
We avoid that pattern by importing into a temporary name and then assigning to the annotated
variable (an assignment is not a redefinition).
"""

import contextlib
import json
import logging
import os
import threading
import time
from typing import Any

# =============================================================================
# Optional dependencies (import-safe, mypy-safe)
# =============================================================================

# Optional redis (sync). Keep import-safe and typed as optional.
_redis_mod: Any | None
try:  # pragma: no cover
    import redis as _redis_tmp  # type: ignore[import-not-found]
except Exception:  # pragma: no cover
    _redis_mod = None
else:  # pragma: no cover
    _redis_mod = _redis_tmp

# Optional config fallback (env wins).
_C: Any | None
try:  # pragma: no cover
    import config as _config_tmp  # type: ignore[import-not-found]
except Exception:  # pragma: no cover
    _C = None
else:  # pragma: no cover
    _C = _config_tmp

# Optional legacy logger factory (prefer utils_pkg.log_compat when present).
_legacy_get_logger: Any | None
try:  # pragma: no cover
    from logging_setup import get_logger as _legacy_get_logger_tmp  # type: ignore[import-not-found]
except Exception:  # pragma: no cover
    _legacy_get_logger = None
else:  # pragma: no cover
    _legacy_get_logger = _legacy_get_logger_tmp

# Optional canonical logger (preferred).
_compat_get_logger: Any | None
try:  # pragma: no cover
    from utils_pkg.log_compat import get_logger as _compat_get_logger_tmp
except Exception:  # pragma: no cover
    _compat_get_logger = None
else:  # pragma: no cover
    _compat_get_logger = _compat_get_logger_tmp


# =============================================================================
# Logging (Phase 2 compliant: NO monkey-patching logger objects)
# =============================================================================

_TRUE = {"1", "true", "yes", "y", "on", "t"}
_FALSE = {"0", "false", "no", "n", "off", "f"}


def _base_logger() -> Any:
    """
    Best-effort logger acquisition.

    Priority:
      1) utils_pkg.log_compat.get_logger (canonical repo logger contract)
      2) legacy logging_setup.get_logger (if present)
      3) stdlib logging.getLogger
    """
    if callable(_compat_get_logger):
        with contextlib.suppress(Exception):
            lg = _compat_get_logger("sniper.guardrails")
            if lg is not None:
                return lg
        with contextlib.suppress(Exception):
            lg2 = _compat_get_logger()
            if lg2 is not None:
                return lg2

    if callable(_legacy_get_logger):
        with contextlib.suppress(Exception):
            lg3 = _legacy_get_logger("sniper.guardrails")
            if lg3 is not None:
                return lg3
        with contextlib.suppress(Exception):
            lg4 = _legacy_get_logger()
            if lg4 is not None:
                return lg4

    return logging.getLogger("sniper.guardrails")


class GuardLog:
    """
    Logger adapter that exposes .event and .every without patching the base logger.
    All methods are best-effort and exception-suppressed.
    """

    def __init__(self, base: Any) -> None:
        self._base = base
        self._lock = threading.Lock()
        self._store: dict[str, float] = {}

    def info(self, msg: str, *args: Any, **kwargs: Any) -> None:
        with contextlib.suppress(Exception):
            fn = getattr(self._base, "info", None)
            if callable(fn):
                fn(msg, *args, **kwargs)

    def warning(self, msg: str, *args: Any, **kwargs: Any) -> None:
        with contextlib.suppress(Exception):
            fn = getattr(self._base, "warning", None)
            if callable(fn):
                fn(msg, *args, **kwargs)

    def error(self, msg: str, *args: Any, **kwargs: Any) -> None:
        with contextlib.suppress(Exception):
            fn = getattr(self._base, "error", None)
            if callable(fn):
                fn(msg, *args, **kwargs)

    def event(self, name: str, **fields: Any) -> None:
        """
        Prefer base.event if it exists, otherwise log name + dict.
        """
        fn = getattr(self._base, "event", None)
        if callable(fn):
            with contextlib.suppress(TypeError, Exception):
                fn(name, **fields)
                return
            with contextlib.suppress(Exception):
                fn(name, msg=None, **fields)
                return

        with contextlib.suppress(Exception):
            self.info("%s %s", name, fields)

    def every(self, key: str, n: int = 30, msg: str | None = None, **fields: Any) -> None:
        """
        Prefer base.every if it exists; else throttle in-process by time.

        We treat `n` as seconds for throttling (matches prior fallback behavior).
        If the base supports `every_sec=`, we use it. Otherwise we try a positional call.
        """
        fn = getattr(self._base, "every", None)
        if callable(fn):
            with contextlib.suppress(Exception):
                fn(key, every_sec=float(max(1, int(n))), msg=msg, **fields)
                return
            with contextlib.suppress(Exception):
                fn(key, int(n), msg=msg, **fields)
                return

        try:
            period = float(max(1, int(n)))
        except Exception:
            period = 30.0

        now = time.monotonic()
        with self._lock:
            last = float(self._store.get(key, 0.0))
            if (now - last) < period:
                return
            self._store[key] = now

        self.event(msg or key, **fields)


log = GuardLog(_base_logger())


# =============================================================================
# Env / config helpers (safe, fast)
# =============================================================================


def _envs(k: str, d: str = "") -> str:
    try:
        v = os.getenv(k)
        return (v if v is not None else d).strip()
    except Exception:
        return str(d).strip()


def _envb(k: str, d: bool = False) -> bool:
    v = _envs(k, "")
    if v == "":
        return bool(d)
    s = v.lower().strip()
    if s in _TRUE:
        return True
    if s in _FALSE:
        return False
    return bool(d)


def _envi(k: str, d: int) -> int:
    v = _envs(k, "")
    if v == "":
        return int(d)
    try:
        return int(float(v.replace("_", "")))
    except Exception:
        return int(d)


def _envf(k: str, d: float) -> float:
    v = _envs(k, "")
    if v == "":
        return float(d)
    try:
        return float(v.replace("_", ""))
    except Exception:
        return float(d)


def _cfgf(name: str, default: float) -> float:
    """
    Read float from optional config module. Env always wins elsewhere.
    NOTE: _C is typed as Any|None so mypy does not treat this branch as unreachable.
    """
    if _C is None:
        return float(default)
    try:
        return float(getattr(_C, name))
    except Exception:
        return float(default)


def _cfgi(name: str, default: int) -> int:
    """
    Read int from optional config module. Env always wins elsewhere.
    NOTE: _C is typed as Any|None so mypy does not treat this branch as unreachable.
    """
    if _C is None:
        return int(default)
    try:
        return int(getattr(_C, name))
    except Exception:
        return int(default)


def _now() -> float:
    return time.time()


def _utc_day(ts: float | None = None) -> str:
    return time.strftime("%Y-%m-%d", time.gmtime(ts or _now()))


def _pct_to_frac(x: float) -> float:
    # accepts 0.18 (fraction) or 18 (percent)
    try:
        x2 = float(x)
    except Exception:
        return 0.0
    if x2 <= 0:
        return 0.0
    return x2 / 100.0 if x2 > 1.0 else x2


def _clamp(x: float, lo: float, hi: float) -> float:
    try:
        y = float(x)
    except Exception:
        return float(lo)
    if y < lo:
        return float(lo)
    if y > hi:
        return float(hi)
    return float(y)


def _short(v: Any, n: int = 160) -> str:
    try:
        s = str(v)
    except Exception:
        return "<?>"
    return s[:n]


def _csv_set(v: str) -> set[str]:
    if not v:
        return set()
    return {x.strip() for x in v.split(",") if x.strip()}


def _to_int(x: Any, d: int = 0) -> int:
    try:
        return int(float(x))
    except Exception:
        return int(d)


def _to_float(x: Any, d: float = 0.0) -> float:
    try:
        return float(x)
    except Exception:
        return float(d)


# =============================================================================
# Namespacing / keys (LIVE: recomputed per call)
# =============================================================================


def _prefix_ns() -> tuple[str, str]:
    prefix = (_envs("PF_GUARD_PREFIX", _envs("GUARD_PREFIX", "gr")) or "gr").strip().lower()
    ns = (
        (
            _envs("GUARD_NAMESPACE", "")
            or _envs("PF_GUARD_NS", "")
            or _envs("RENDER_SERVICE_NAME", "")
            or _envs("FLY_APP_NAME", "")
            or _envs("DYNO", "")
            or _envs("HOSTNAME", "")
            or "default"
        )
        .strip()
        .lower()
    )
    return prefix, ns


def _state_key(day: str, prefix: str, ns: str) -> str:
    return f"{prefix}:{ns}:{day}:state"


def _buylog_key(day: str, prefix: str, ns: str) -> str:
    return f"{prefix}:{ns}:{day}:buylog"


def _default_equity_sol_key(prefix: str, ns: str) -> str:
    return f"{prefix}:{ns}:equity_sol"


def _default_sol_usd_key(prefix: str, ns: str) -> str:
    return f"{prefix}:{ns}:sol_usd"


def _degraded_key(prefix: str, ns: str) -> str:
    return f"{prefix}:{ns}:degraded"


def _file_flag(path: str) -> bool:
    if not path:
        return False
    try:
        return bool(os.path.exists(path))
    except Exception:
        return False


def _state_ttl_sec() -> int:
    return _envi("GUARD_STATE_TTL_SEC", 4 * 24 * 3600)  # 4 days default


# =============================================================================
# Redis client (LAZY, cached, never throws)
# =============================================================================

_REDIS_URL_KEYS = ("REDIS_URL", "UPSTASH_REDIS_URL", "PF_REDIS_URL", "GUARD_REDIS_URL")

_R_SYNC: Any | None = None
_R_SYNC_AT: float = 0.0
_R_SYNC_URL: str = ""
_R_LOCK = threading.Lock()


def _redis_url() -> str:
    for k in _REDIS_URL_KEYS:
        v = os.getenv(k)
        if v:
            return str(v).strip()
    return ""


def _redis_kwargs() -> dict[str, Any]:
    return {
        "socket_timeout": float(
            _envs("PF_REDIS_SOCKET_TIMEOUT_S", _envs("GUARD_REDIS_SOCKET_TIMEOUT_S", "1.2"))
        ),
        "socket_connect_timeout": float(
            _envs("PF_REDIS_CONNECT_TIMEOUT_S", _envs("GUARD_REDIS_CONNECT_TIMEOUT_S", "1.2"))
        ),
        "retry_on_timeout": True,
        "decode_responses": True,
    }


def _get_redis_sync() -> Any | None:
    """
    Return a cached redis sync client or None.
    Never raises. Import-safe.
    """
    global _R_SYNC, _R_SYNC_AT, _R_SYNC_URL

    if _redis_mod is None:
        return None

    from_url = getattr(_redis_mod, "from_url", None)
    if not callable(from_url):
        return None

    refresh = max(1.0, float(_envf("GUARD_REDIS_REFRESH_SEC", 5.0)))
    now = _now()

    with _R_LOCK:
        if _R_SYNC is not None and (now - _R_SYNC_AT) < refresh:
            return _R_SYNC

        url = _redis_url()
        _R_SYNC_AT = now

        if not url:
            _R_SYNC = None
            _R_SYNC_URL = ""
            return None

        if _R_SYNC is not None and _R_SYNC_URL == url:
            return _R_SYNC

        _R_SYNC = None
        _R_SYNC_URL = ""
        with contextlib.suppress(Exception):
            _R_SYNC = from_url(url, **_redis_kwargs())
            _R_SYNC_URL = url

        return _R_SYNC


# =============================================================================
# State schema
# =============================================================================

_STATE_FIELDS = (
    "day",
    "buys",
    "buy_attempts",
    "buy_failures",
    "spent_sol",
    "realized_sol",
    "wins",
    "losses",
    "closes",
    "blocks",
    "last_block_reason",
    "breaker_until",
    "fail_bucket_start",
    "fail_bucket_count",
    "open_positions",
    "open_exposure_sol",
    "loss_streak",
    "fail_streak",
    "win_streak",
    "last_trade_ts",
    "last_close_ts",
    "roll_buy_start",
    "roll_buy_count",
    "roll_spent_start",
    "roll_spent_sol",
    "unrealized_sol",
    "ts",
)

_PROCESS_START_TS = _now()

_mem_lock = threading.Lock()
_mem_state: dict[str, dict[str, Any]] = {}

# Tiny per-process cache to reduce chatter on hot loops.
_STATE_CACHE_TTL_SEC = float(_envf("GUARD_STATE_CACHE_TTL_SEC", 0.35))
_cache_lock = threading.Lock()
_cache_key: str = ""
_cache_at: float = 0.0
_cache_val: dict[str, Any] | None = None


def _ctx_key(prefix: str, ns: str, day: str) -> str:
    return f"{prefix}|{ns}|{day}"


def _default_state(day: str) -> dict[str, Any]:
    return {
        "day": day,
        "buys": 0,
        "buy_attempts": 0,
        "buy_failures": 0,
        "spent_sol": 0.0,
        "realized_sol": 0.0,
        "wins": 0,
        "losses": 0,
        "closes": 0,
        "blocks": 0,
        "last_block_reason": "",
        "breaker_until": 0.0,
        "fail_bucket_start": 0.0,
        "fail_bucket_count": 0,
        "open_positions": 0,
        "open_exposure_sol": 0.0,
        "loss_streak": 0,
        "fail_streak": 0,
        "win_streak": 0,
        "last_trade_ts": 0.0,
        "last_close_ts": 0.0,
        "roll_buy_start": 0.0,
        "roll_buy_count": 0,
        "roll_spent_start": 0.0,
        "roll_spent_sol": 0.0,
        "unrealized_sol": 0.0,
        "ts": _now(),
    }


def _redis_hmget(r: Any, key: str, fields: tuple[str, ...]) -> dict[str, Any] | None:
    vals: Any = None
    with contextlib.suppress(Exception):
        vals = r.hmget(key, list(fields))
    if not isinstance(vals, list) or len(vals) != len(fields):
        return None

    out: dict[str, Any] = {}
    for f, v in zip(fields, vals, strict=False):
        if v is not None:
            out[f] = v
    return out


def _coerce_state(day: str, m: dict[str, Any]) -> dict[str, Any]:
    return {
        "day": day,
        "buys": _to_int(m.get("buys", 0)),
        "buy_attempts": _to_int(m.get("buy_attempts", 0)),
        "buy_failures": _to_int(m.get("buy_failures", 0)),
        "spent_sol": _to_float(m.get("spent_sol", 0.0)),
        "realized_sol": _to_float(m.get("realized_sol", 0.0)),
        "wins": _to_int(m.get("wins", 0)),
        "losses": _to_int(m.get("losses", 0)),
        "closes": _to_int(m.get("closes", 0)),
        "blocks": _to_int(m.get("blocks", 0)),
        "last_block_reason": str(m.get("last_block_reason", "") or ""),
        "breaker_until": _to_float(m.get("breaker_until", 0.0)),
        "fail_bucket_start": _to_float(m.get("fail_bucket_start", 0.0)),
        "fail_bucket_count": _to_int(m.get("fail_bucket_count", 0)),
        "open_positions": _to_int(m.get("open_positions", 0)),
        "open_exposure_sol": _to_float(m.get("open_exposure_sol", 0.0)),
        "loss_streak": _to_int(m.get("loss_streak", 0)),
        "fail_streak": _to_int(m.get("fail_streak", 0)),
        "win_streak": _to_int(m.get("win_streak", 0)),
        "last_trade_ts": _to_float(m.get("last_trade_ts", 0.0)),
        "last_close_ts": _to_float(m.get("last_close_ts", 0.0)),
        "roll_buy_start": _to_float(m.get("roll_buy_start", 0.0)),
        "roll_buy_count": _to_int(m.get("roll_buy_count", 0)),
        "roll_spent_start": _to_float(m.get("roll_spent_start", 0.0)),
        "roll_spent_sol": _to_float(m.get("roll_spent_sol", 0.0)),
        "unrealized_sol": _to_float(m.get("unrealized_sol", 0.0)),
        "ts": _to_float(m.get("ts", _now())),
    }


def _redis_init_day(r: Any, key: str, day: str) -> dict[str, Any]:
    st = _default_state(day)
    with contextlib.suppress(Exception):
        pipe = r.pipeline()
        pipe.hset(key, mapping={k: str(v) for k, v in st.items()})
        pipe.expire(key, _state_ttl_sec())
        pipe.execute()
    return st


def _cache_get(ck: str) -> dict[str, Any] | None:
    now = _now()
    with _cache_lock:
        if (
            _cache_val is not None
            and _cache_key == ck
            and (now - _cache_at) <= _STATE_CACHE_TTL_SEC
        ):
            return dict(_cache_val)
    return None


def _cache_put(ck: str, st: dict[str, Any]) -> None:
    now = _now()
    with _cache_lock:
        global _cache_key, _cache_at, _cache_val
        _cache_key = ck
        _cache_at = now
        _cache_val = dict(st)


def _cache_bust() -> None:
    with _cache_lock:
        global _cache_key, _cache_at, _cache_val
        _cache_key = ""
        _cache_at = 0.0
        _cache_val = None


def _load_state_uncached(day: str, prefix: str, ns: str) -> dict[str, Any]:
    key = _state_key(day, prefix, ns)
    r = _get_redis_sync()

    if r is not None:
        m = _redis_hmget(r, key, _STATE_FIELDS)
        if m and str(m.get("day", "")) == day:
            return _coerce_state(day, m)
        return _redis_init_day(r, key, day)

    mk = _ctx_key(prefix, ns, day)
    with _mem_lock:
        st = _mem_state.get(mk)
        if st is None:
            st = _default_state(day)
            _mem_state.clear()
            _mem_state[mk] = dict(st)
        return dict(st)


def _load_state(prefix: str, ns: str) -> dict[str, Any]:
    try:
        day = _utc_day()
        ck = _ctx_key(prefix, ns, day)
        c = _cache_get(ck)
        if c is not None:
            return c
        st = _load_state_uncached(day, prefix, ns)
        _cache_put(ck, st)
        return dict(st)
    except Exception:
        return _default_state(_utc_day())


def _save_state(st: dict[str, Any], prefix: str, ns: str) -> None:
    try:
        st2 = dict(st)
        st2["ts"] = _now()
        day = str(st2.get("day") or _utc_day())
        key = _state_key(day, prefix, ns)
        r = _get_redis_sync()

        if r is not None:
            with contextlib.suppress(Exception):
                pipe = r.pipeline()
                pipe.hset(key, mapping={k: str(v) for k, v in st2.items()})
                pipe.expire(key, _state_ttl_sec())
                pipe.execute()
                _cache_put(_ctx_key(prefix, ns, day), st2)
                return

        mk = _ctx_key(prefix, ns, day)
        with _mem_lock:
            _mem_state[mk] = dict(st2)
        _cache_put(_ctx_key(prefix, ns, day), st2)
    except Exception:
        return


def _hincr(key: str, field: str, amount: float) -> None:
    r = _get_redis_sync()
    if r is None:
        return
    with contextlib.suppress(Exception):
        pipe = r.pipeline()
        pipe.hincrbyfloat(key, field, float(amount))
        pipe.hset(key, "ts", str(_now()))
        pipe.expire(key, _state_ttl_sec())
        pipe.execute()
        _cache_bust()


def _hset(key: str, mapping: dict[str, Any]) -> None:
    r = _get_redis_sync()
    if r is None:
        return
    with contextlib.suppress(Exception):
        pipe = r.pipeline()
        pipe.hset(key, mapping={k: str(v) for k, v in mapping.items()})
        pipe.hset(key, "ts", str(_now()))
        pipe.expire(key, _state_ttl_sec())
        pipe.execute()
        _cache_bust()


# =============================================================================
# Knobs (env-first, config fallback) with tiny TTL refresh
# =============================================================================

_KNOBS_TTL_SEC = max(0.5, float(_envf("GUARD_KNOBS_REFRESH_SEC", 2.0)))
_knobs_at: float = 0.0
_knobs_cache: dict[str, Any] = {}
_knobs_lock = threading.Lock()


def _knobs(prefix: str, ns: str) -> dict[str, Any]:
    global _knobs_at, _knobs_cache
    now = _now()

    with _knobs_lock:
        if _knobs_cache and (now - _knobs_at) < _KNOBS_TTL_SEC:
            return _knobs_cache

        pause_switch = _envs("PAUSE_SWITCH", "PAUSE_TRADING")

        max_losses = _envi("PF_DAILY_MAX_LOSSES", _envi("DAILY_MAX_LOSSES", 0))
        max_buys = _envi("PF_DAILY_MAX_BUYS", _envi("DAILY_MAX_BUYS", 0))
        max_spend_sol = _envf("PF_DAILY_MAX_SPEND_SOL", _envf("DAILY_MAX_SPEND_SOL", 0.0))

        max_dd_frac = _pct_to_frac(
            _envf(
                "PF_DAILY_MAX_DRAWDOWN_PCT",
                _envf(
                    "RISK_MAX_DAILY_DRAWDOWN_PCT",
                    _envf("DAILY_MAX_DRAWDOWN_PCT", _cfgf("RISK_MAX_DAILY_DRAWDOWN_PCT", 0.0)),
                ),
            )
        )
        stop_after_profit_frac = _pct_to_frac(
            _envf(
                "PF_STOP_AFTER_PROFIT_PCT",
                _envf("STOP_AFTER_DAILY_PROFIT_PCT", _cfgf("STOP_AFTER_DAILY_PROFIT_PCT", 0.0)),
            )
        )
        daily_spend_cap_frac = _pct_to_frac(
            _envf("DAILY_SPEND_CAP_PCT", _cfgf("DAILY_SPEND_CAP_PCT", 0.0))
        )

        breaker_minutes = _envi(
            "RISK_CIRCUIT_BREAKER_MINUTES",
            _envi("GUARD_BREAKER_MINUTES", _cfgi("RISK_CIRCUIT_BREAKER_MINUTES", 0)),
        )
        breaker_enabled = _envb("GUARD_BREAKER_ENABLED", True)

        fail_breaker_enabled = _envb("GUARD_FAIL_BREAKER_ENABLED", False)
        fail_breaker_fails = _envi("GUARD_FAIL_BREAKER_FAILS", 8)
        fail_breaker_window_sec = _envf("GUARD_FAIL_BREAKER_WINDOW_SEC", 60.0)
        fail_breaker_cooldown_sec = _envf("GUARD_FAIL_BREAKER_COOLDOWN_SEC", 45.0)

        max_loss_streak = _envi("GUARD_MAX_LOSS_STREAK", 0)
        max_fail_streak = _envi("GUARD_MAX_FAIL_STREAK", 0)

        roll_window_sec = _envf("GUARD_ROLL_WINDOW_SEC", 0.0)
        roll_max_buys = _envi("GUARD_ROLL_MAX_BUYS", 0)
        roll_max_spend_sol = _envf("GUARD_ROLL_MAX_SPEND_SOL", 0.0)

        equity_sol_fallback = _envf("EQUITY_SOL_FALLBACK", 0.0)
        equity_usd_fallback = _envf("EQUITY_USD_FALLBACK", 1500.0)
        sol_usd_fallback = _envf("SOL_PRICE_USD_FALLBACK", 150.0)

        equity_sol_redis_key = _envs(
            "GUARD_EQUITY_SOL_REDIS_KEY", _default_equity_sol_key(prefix, ns)
        )
        sol_usd_redis_key = _envs("GUARD_SOL_USD_REDIS_KEY", _default_sol_usd_key(prefix, ns))

        max_open_positions = _envi("GUARD_MAX_OPEN_POSITIONS", 0)
        max_open_exposure_sol = _envf("GUARD_MAX_OPEN_EXPOSURE_SOL", 0.0)

        min_equity_sol = _envf("GUARD_MIN_EQUITY_SOL", 0.0)
        min_uptime_sec = _envf("GUARD_MIN_UPTIME_SEC", 0.0)

        degraded_blocks = _envb("GUARD_DEGRADED_BLOCKS_BUYS", True)

        fail_closed = _envb("GUARD_FAIL_CLOSED", False) or (
            _envs("GUARD_FAIL_MODE", "").lower() in {"closed", "fail_closed"}
        )

        force_allow = _envb("GUARD_FORCE_ALLOW_BUYS", False)
        force_block = _envb("GUARD_FORCE_BLOCK_BUYS", False)

        buylog_enabled = _envb("GUARD_BUYLOG_ENABLED", True)
        buylog_max = _envi("GUARD_BUYLOG_MAX", 500)

        allow_mints = _csv_set(_envs("GUARD_ALLOW_MINTS_CSV", ""))
        block_mints = _csv_set(_envs("GUARD_BLOCK_MINTS_CSV", ""))

        _knobs_cache = {
            "pause_switch": pause_switch,
            "max_losses": int(max_losses),
            "max_buys": int(max_buys),
            "max_spend_sol": float(max_spend_sol),
            "max_dd_frac": float(_clamp(max_dd_frac, 0.0, 1.0)),
            "stop_after_profit_frac": float(_clamp(stop_after_profit_frac, 0.0, 1.0)),
            "daily_spend_cap_frac": float(_clamp(daily_spend_cap_frac, 0.0, 1.0)),
            "breaker_minutes": int(max(0, breaker_minutes)),
            "breaker_enabled": bool(breaker_enabled),
            "fail_breaker_enabled": bool(fail_breaker_enabled),
            "fail_breaker_fails": int(max(0, fail_breaker_fails)),
            "fail_breaker_window_sec": float(max(0.0, fail_breaker_window_sec)),
            "fail_breaker_cooldown_sec": float(max(0.0, fail_breaker_cooldown_sec)),
            "max_loss_streak": int(max(0, max_loss_streak)),
            "max_fail_streak": int(max(0, max_fail_streak)),
            "roll_window_sec": float(max(0.0, roll_window_sec)),
            "roll_max_buys": int(max(0, roll_max_buys)),
            "roll_max_spend_sol": float(max(0.0, roll_max_spend_sol)),
            "equity_sol_fallback": float(max(0.0, equity_sol_fallback)),
            "equity_usd_fallback": float(max(0.0, equity_usd_fallback)),
            "sol_usd_fallback": float(max(0.0, sol_usd_fallback)),
            "equity_sol_redis_key": str(equity_sol_redis_key),
            "sol_usd_redis_key": str(sol_usd_redis_key),
            "max_open_positions": int(max(0, max_open_positions)),
            "max_open_exposure_sol": float(max(0.0, max_open_exposure_sol)),
            "min_equity_sol": float(max(0.0, min_equity_sol)),
            "min_uptime_sec": float(max(0.0, min_uptime_sec)),
            "degraded_blocks": bool(degraded_blocks),
            "fail_closed": bool(fail_closed),
            "force_allow": bool(force_allow),
            "force_block": bool(force_block),
            "buylog_enabled": bool(buylog_enabled),
            "buylog_max": int(max(50, buylog_max)),
            "allow_mints": allow_mints,
            "block_mints": block_mints,
        }
        _knobs_at = now
        return _knobs_cache


def _get_sol_usd_est(k: dict[str, Any]) -> float:
    r = _get_redis_sync()
    if r is not None and k.get("sol_usd_redis_key"):
        with contextlib.suppress(Exception):
            v = r.get(str(k["sol_usd_redis_key"]))
            if v:
                x = float(v)
                if x > 0:
                    return x
    return float(k.get("sol_usd_fallback") or 0.0)


def _get_equity_sol_est(k: dict[str, Any]) -> float:
    if float(k.get("equity_sol_fallback") or 0.0) > 0:
        return float(k["equity_sol_fallback"])

    r = _get_redis_sync()
    if r is not None and k.get("equity_sol_redis_key"):
        with contextlib.suppress(Exception):
            v = r.get(str(k["equity_sol_redis_key"]))
            if v:
                x = float(v)
                if x > 0:
                    return x

    sol_usd = _get_sol_usd_est(k)
    if sol_usd > 0:
        return float(k.get("equity_usd_fallback") or 0.0) / float(sol_usd)

    return 0.0


def _is_degraded(prefix: str, ns: str) -> bool:
    if _envb("DEGRADED_MODE", False) or _envb("SAFE_MODE", False):
        return True
    r = _get_redis_sync()
    if r is not None:
        with contextlib.suppress(Exception):
            v = r.get(_degraded_key(prefix, ns))
            if v is None:
                return False
            s = str(v).strip().lower()
            return s in _TRUE or s == "degraded"
    return False


# =============================================================================
# Deterministic compute: allow/block + reason
# =============================================================================


def _compute(
    st: dict[str, Any],
    k: dict[str, Any],
    *,
    prefix: str,
    ns: str,
    mint: str | None = None,
) -> tuple[bool, str]:
    if bool(k.get("force_allow")) and (not bool(k.get("force_block"))):
        return True, "force_allow"
    if bool(k.get("force_block")):
        return False, "force_block"

    pause_key = str(k.get("pause_switch") or "PAUSE_TRADING")
    pause_file = _envs("PAUSE_TRADING_FILE", _envs("PAUSE_FILE", ""))
    if _envb(pause_key, False) or _file_flag(pause_file):
        return False, "paused"

    kill_file = _envs("KILL_SWITCH_FILE", "")
    disable_file = _envs("DISABLE_BUYS_FILE", "")
    if _envb("KILL_SWITCH", False) or _file_flag(kill_file):
        return False, "kill_switch"
    if _envb("DISABLE_BUYS", False) or _file_flag(disable_file):
        return False, "disable_buys"

    if mint:
        with contextlib.suppress(Exception):
            m = str(mint)
            allow_m = k.get("allow_mints") or set()
            block_m = k.get("block_mints") or set()
            if allow_m and (m not in allow_m):
                return False, "mint_not_allowlisted"
            if block_m and (m in block_m):
                return False, "mint_blocklisted"

    if (
        bool(k.get("degraded_blocks", True))
        and _is_degraded(prefix, ns)
        and (not _envb("GUARD_ALLOW_WHEN_DEGRADED", False))
    ):
        return False, "degraded_mode"

    min_uptime = float(k.get("min_uptime_sec") or 0.0)
    if min_uptime > 0 and (_now() - _PROCESS_START_TS) < min_uptime:
        return False, "warmup"

    until = float(st.get("breaker_until", 0.0) or 0.0)
    if until > _now():
        return False, "breaker_cooldown"

    mop = int(k.get("max_open_positions") or 0)
    if mop > 0 and int(st.get("open_positions", 0) or 0) >= mop:
        return False, "max_open_positions"

    moe = float(k.get("max_open_exposure_sol") or 0.0)
    if moe > 0 and float(st.get("open_exposure_sol", 0.0) or 0.0) >= moe:
        return False, "max_open_exposure_sol"

    equity_sol = _get_equity_sol_est(k)
    min_eq = float(k.get("min_equity_sol") or 0.0)
    if min_eq > 0 and equity_sol > 0 and equity_sol < min_eq:
        return False, "min_equity_sol"

    max_loss_streak = int(k.get("max_loss_streak") or 0)
    if max_loss_streak > 0 and int(st.get("loss_streak", 0) or 0) >= max_loss_streak:
        return False, "max_loss_streak"

    max_fail_streak = int(k.get("max_fail_streak") or 0)
    if max_fail_streak > 0 and int(st.get("fail_streak", 0) or 0) >= max_fail_streak:
        return False, "max_fail_streak"

    roll_window = float(k.get("roll_window_sec") or 0.0)
    if roll_window > 0:
        now = _now()

        rb_start = float(st.get("roll_buy_start", 0.0) or 0.0)
        rb_cnt = int(st.get("roll_buy_count", 0) or 0)
        if rb_start <= 0.0 or (now - rb_start) > roll_window:
            rb_cnt = 0

        rs_start = float(st.get("roll_spent_start", 0.0) or 0.0)
        rs_sol = float(st.get("roll_spent_sol", 0.0) or 0.0)
        if rs_start <= 0.0 or (now - rs_start) > roll_window:
            rs_sol = 0.0

        roll_max_buys = int(k.get("roll_max_buys") or 0)
        if roll_max_buys > 0 and rb_cnt >= roll_max_buys:
            return False, "roll_max_buys"

        roll_max_spend_sol = float(k.get("roll_max_spend_sol") or 0.0)
        if roll_max_spend_sol > 0 and rs_sol >= roll_max_spend_sol:
            return False, "roll_max_spend_sol"

    max_losses = int(k.get("max_losses") or 0)
    if max_losses > 0 and int(st.get("losses", 0) or 0) >= max_losses:
        return False, "daily_max_losses"

    max_buys = int(k.get("max_buys") or 0)
    if max_buys > 0 and int(st.get("buys", 0) or 0) >= max_buys:
        return False, "daily_max_buys"

    spent_sol = float(st.get("spent_sol", 0.0) or 0.0)
    max_spend_sol = float(k.get("max_spend_sol") or 0.0)
    if max_spend_sol > 0 and spent_sol >= max_spend_sol:
        return False, "daily_max_spend_sol"

    spend_cap_frac = float(k.get("daily_spend_cap_frac") or 0.0)
    if spend_cap_frac > 0 and equity_sol > 0 and spent_sol >= (equity_sol * spend_cap_frac):
        return False, "daily_spend_cap_pct"

    realized_sol = float(st.get("realized_sol", 0.0) or 0.0)
    unrealized_sol = float(st.get("unrealized_sol", 0.0) or 0.0)
    include_unreal = _envb("GUARD_INCLUDE_UNREALIZED_IN_PNL", False)
    pnl_for_gates = realized_sol + (unrealized_sol if include_unreal else 0.0)

    stop_profit_frac = float(k.get("stop_after_profit_frac") or 0.0)
    if stop_profit_frac > 0 and equity_sol > 0:
        prof_sol = max(0.0, pnl_for_gates)
        if prof_sol >= (equity_sol * stop_profit_frac):
            return False, "stop_after_profit"

    max_dd_frac = float(k.get("max_dd_frac") or 0.0)
    if max_dd_frac > 0 and equity_sol > 0:
        dd_sol = -min(0.0, pnl_for_gates)
        if dd_sol >= (equity_sol * max_dd_frac):
            return False, "daily_drawdown"

    return True, "ok"


# =============================================================================
# Block accounting (throttled to prevent spam)
# =============================================================================

_block_lock = threading.Lock()
_last_block_reason: str = ""
_last_block_ts: float = 0.0


def _mark_block(prefix: str, ns: str, reason: str) -> None:
    try:
        global _last_block_reason, _last_block_ts

        reason2 = (reason or "blocked").strip()[:120]
        now = _now()
        throttle = float(_envf("GUARD_BLOCK_THROTTLE_SEC", 3.0))

        with _block_lock:
            if reason2 == _last_block_reason and (now - _last_block_ts) < throttle:
                return
            _last_block_reason = reason2
            _last_block_ts = now

        day = _utc_day()
        key = _state_key(day, prefix, ns)

        _hset(key, {"day": day, "last_block_reason": reason2})
        _hincr(key, "blocks", 1.0)

        if _get_redis_sync() is None:
            st = _load_state(prefix, ns)
            st["blocks"] = int(st.get("blocks", 0)) + 1
            st["last_block_reason"] = reason2
            _save_state(st, prefix, ns)

        log.every("GUARD_BLOCK", 10, reason=reason2, day=day, ns=ns)
    except Exception:
        pass


# =============================================================================
# Public API (NEVER raises)
# =============================================================================


def buys_allowed() -> bool:
    try:
        prefix, ns = _prefix_ns()
        st = _load_state(prefix, ns)
        k = _knobs(prefix, ns)
        ok, reason = _compute(st, k, prefix=prefix, ns=ns)
        if not ok:
            _mark_block(prefix, ns, reason)
        return bool(ok)
    except Exception:
        with contextlib.suppress(Exception):
            prefix, ns = _prefix_ns()
            if _knobs(prefix, ns).get("fail_closed"):
                return False
        return True


def buys_allowed_for(mint: str) -> bool:
    try:
        prefix, ns = _prefix_ns()
        st = _load_state(prefix, ns)
        k = _knobs(prefix, ns)
        ok, reason = _compute(st, k, prefix=prefix, ns=ns, mint=mint)
        if not ok:
            _mark_block(prefix, ns, reason)
        return bool(ok)
    except Exception:
        with contextlib.suppress(Exception):
            prefix, ns = _prefix_ns()
            if _knobs(prefix, ns).get("fail_closed"):
                return False
        return True


def buy_block_reason() -> str:
    try:
        prefix, ns = _prefix_ns()
        st = _load_state(prefix, ns)
        k = _knobs(prefix, ns)
        ok, reason = _compute(st, k, prefix=prefix, ns=ns)
        return "ok" if ok else str(reason)
    except Exception:
        return "unknown"


def buy_block_reason_for(mint: str) -> str:
    try:
        prefix, ns = _prefix_ns()
        st = _load_state(prefix, ns)
        k = _knobs(prefix, ns)
        ok, reason = _compute(st, k, prefix=prefix, ns=ns, mint=mint)
        return "ok" if ok else str(reason)
    except Exception:
        return "unknown"


def status() -> tuple[bool, str, dict[str, Any]]:
    try:
        prefix, ns = _prefix_ns()
        st = _load_state(prefix, ns)
        k = _knobs(prefix, ns)
        ok, reason = _compute(st, k, prefix=prefix, ns=ns)
        snap = snapshot()
        snap["computed"] = {"ok": bool(ok), "reason": str(reason)}
        return bool(ok), str(reason), snap
    except Exception:
        return False, "error", {"error": "status_failed"}


def snapshot() -> dict[str, Any]:
    try:
        prefix, ns = _prefix_ns()
        st = _load_state(prefix, ns)
        k = _knobs(prefix, ns)

        equity_sol = _get_equity_sol_est(k)
        sol_usd = _get_sol_usd_est(k)

        realized = float(st.get("realized_sol", 0.0) or 0.0)
        unreal = float(st.get("unrealized_sol", 0.0) or 0.0)
        include_unreal = _envb("GUARD_INCLUDE_UNREALIZED_IN_PNL", False)
        pnl_gate = realized + (unreal if include_unreal else 0.0)

        dd_sol = -min(0.0, pnl_gate)
        prof_sol = max(0.0, pnl_gate)

        ok, reason = _compute(st, k, prefix=prefix, ns=ns)

        return {
            "day": st.get("day"),
            "namespace": ns,
            "redis": bool(_get_redis_sync() is not None),
            "computed": {"ok": bool(ok), "reason": str(reason)},
            "state": {
                "buys": int(st.get("buys", 0)),
                "buy_attempts": int(st.get("buy_attempts", 0)),
                "buy_failures": int(st.get("buy_failures", 0)),
                "spent_sol": float(st.get("spent_sol", 0.0) or 0.0),
                "realized_sol": realized,
                "unrealized_sol": unreal,
                "wins": int(st.get("wins", 0)),
                "losses": int(st.get("losses", 0)),
                "closes": int(st.get("closes", 0)),
                "blocks": int(st.get("blocks", 0)),
                "last_block_reason": str(st.get("last_block_reason", "") or ""),
                "breaker_until": float(st.get("breaker_until", 0.0) or 0.0),
                "open_positions": max(0, int(st.get("open_positions", 0) or 0)),
                "open_exposure_sol": max(0.0, float(st.get("open_exposure_sol", 0.0) or 0.0)),
                "loss_streak": int(st.get("loss_streak", 0) or 0),
                "fail_streak": int(st.get("fail_streak", 0) or 0),
                "win_streak": int(st.get("win_streak", 0) or 0),
                "roll_buy_count": int(st.get("roll_buy_count", 0) or 0),
                "roll_spent_sol": float(st.get("roll_spent_sol", 0.0) or 0.0),
                "last_trade_ts": float(st.get("last_trade_ts", 0.0) or 0.0),
                "last_close_ts": float(st.get("last_close_ts", 0.0) or 0.0),
            },
            "estimates": {
                "equity_sol": float(equity_sol),
                "sol_usd": float(sol_usd),
                "dd_sol": float(dd_sol),
                "profit_sol": float(prof_sol),
                "dd_usd": float(dd_sol * sol_usd),
                "profit_usd": float(prof_sol * sol_usd),
                "pnl_gate_includes_unreal": int(include_unreal),
            },
            "limits": {
                "pause_switch": str(k.get("pause_switch") or "PAUSE_TRADING"),
                "max_losses": int(k.get("max_losses") or 0),
                "max_buys": int(k.get("max_buys") or 0),
                "max_spend_sol": float(k.get("max_spend_sol") or 0.0),
                "daily_spend_cap_pct": float(k.get("daily_spend_cap_frac") or 0.0),
                "max_dd_pct": float(k.get("max_dd_frac") or 0.0),
                "stop_after_profit_pct": float(k.get("stop_after_profit_frac") or 0.0),
                "breaker_minutes": int(k.get("breaker_minutes") or 0),
                "breaker_enabled": int(bool(k.get("breaker_enabled"))),
                "fail_breaker_enabled": int(bool(k.get("fail_breaker_enabled"))),
                "max_open_positions": int(k.get("max_open_positions") or 0),
                "max_open_exposure_sol": float(k.get("max_open_exposure_sol") or 0.0),
                "min_equity_sol": float(k.get("min_equity_sol") or 0.0),
                "min_uptime_sec": float(k.get("min_uptime_sec") or 0.0),
                "max_loss_streak": int(k.get("max_loss_streak") or 0),
                "max_fail_streak": int(k.get("max_fail_streak") or 0),
                "roll_window_sec": float(k.get("roll_window_sec") or 0.0),
                "roll_max_buys": int(k.get("roll_max_buys") or 0),
                "roll_max_spend_sol": float(k.get("roll_max_spend_sol") or 0.0),
                "degraded_blocks": int(bool(k.get("degraded_blocks"))),
                "fail_closed": int(bool(k.get("fail_closed"))),
                "force_allow": int(bool(k.get("force_allow"))),
                "force_block": int(bool(k.get("force_block"))),
                "mint_allowlist_size": int(len(k.get("allow_mints") or set())),
                "mint_blocklist_size": int(len(k.get("block_mints") or set())),
            },
            "files": {
                "kill_file": _short(_envs("KILL_SWITCH_FILE", "")),
                "disable_buys_file": _short(_envs("DISABLE_BUYS_FILE", "")),
                "pause_file": _short(_envs("PAUSE_TRADING_FILE", _envs("PAUSE_FILE", ""))),
            },
        }
    except Exception:
        return {"error": "snapshot_failed"}


# =============================================================================
# Mutators (best-effort, never crash)
# =============================================================================


def trip_breaker(
    seconds: float | None = None,
    *,
    minutes: int | None = None,
    reason: str = "breaker",
) -> None:
    try:
        prefix, ns = _prefix_ns()
        k = _knobs(prefix, ns)
        if not bool(k.get("breaker_enabled")):
            return

        if minutes is not None:
            sec = float(max(0, int(minutes))) * 60.0
        elif seconds is not None:
            sec = float(max(0.0, float(seconds)))
        else:
            sec = float(max(0, int(k.get("breaker_minutes") or 0))) * 60.0

        if sec <= 0:
            return

        until = _now() + sec
        day = _utc_day()
        key = _state_key(day, prefix, ns)
        rsn = (reason or "breaker").strip()[:120]

        r = _get_redis_sync()
        if r is not None:
            cur_f = 0.0
            with contextlib.suppress(Exception):
                cur = r.hget(key, "breaker_until")
                cur_f = float(cur) if cur else 0.0
            until2 = max(cur_f, until)
            _hset(key, {"day": day, "breaker_until": until2, "last_block_reason": f"breaker:{rsn}"})
            _hincr(key, "blocks", 1.0)
            return

        st = _load_state(prefix, ns)
        st["breaker_until"] = max(float(st.get("breaker_until", 0.0) or 0.0), until)
        st["last_block_reason"] = f"breaker:{rsn}"
        st["blocks"] = int(st.get("blocks", 0)) + 1
        _save_state(st, prefix, ns)
    except Exception:
        pass


def _roll_update_on_buy(st: dict[str, Any], k: dict[str, Any], notional: float) -> dict[str, Any]:
    roll_window = float(k.get("roll_window_sec") or 0.0)
    if roll_window <= 0:
        return st

    now = _now()

    rb_start = float(st.get("roll_buy_start", 0.0) or 0.0)
    rb_cnt = int(st.get("roll_buy_count", 0) or 0)
    if rb_start <= 0.0 or (now - rb_start) > roll_window:
        rb_start = now
        rb_cnt = 0
    rb_cnt += 1

    rs_start = float(st.get("roll_spent_start", 0.0) or 0.0)
    rs_sol = float(st.get("roll_spent_sol", 0.0) or 0.0)
    if rs_start <= 0.0 or (now - rs_start) > roll_window:
        rs_start = now
        rs_sol = 0.0
    rs_sol += max(0.0, float(notional or 0.0))

    st["roll_buy_start"] = rb_start
    st["roll_buy_count"] = rb_cnt
    st["roll_spent_start"] = rs_start
    st["roll_spent_sol"] = rs_sol
    return st


def record_trade_open(notional_sol: float, mint: str | None = None, **kw: Any) -> None:
    try:
        prefix, ns = _prefix_ns()
        k = _knobs(prefix, ns)

        notional = float(notional_sol or 0.0)
        if notional < 0:
            notional = 0.0

        day = _utc_day()
        key = _state_key(day, prefix, ns)
        ts = float(kw.get("ts") or _now())

        count_buy = bool(kw.get("count_buy", True))
        dop = int(kw.get("delta_open_positions", 1))
        dex = kw.get("delta_open_exposure_sol")
        dex_f = float(dex) if dex is not None else float(notional)

        r = _get_redis_sync()
        if r is not None:
            _hset(key, {"day": day, "last_trade_ts": ts})
            if count_buy:
                _hincr(key, "buys", 1.0)
            if dop != 0:
                _hincr(key, "open_positions", float(dop))
            if notional > 0:
                _hincr(key, "spent_sol", notional)
            if dex_f != 0:
                _hincr(key, "open_exposure_sol", float(dex_f))

            if float(k.get("roll_window_sec") or 0.0) > 0 and count_buy:
                st = _load_state(prefix, ns)
                st = _roll_update_on_buy(st, k, notional)
                _hset(
                    key,
                    {
                        "roll_buy_start": st.get("roll_buy_start", 0.0),
                        "roll_buy_count": st.get("roll_buy_count", 0),
                        "roll_spent_start": st.get("roll_spent_start", 0.0),
                        "roll_spent_sol": st.get("roll_spent_sol", 0.0),
                    },
                )
            _ = mint
            return

        st = _load_state(prefix, ns)
        if count_buy:
            st["buys"] = int(st.get("buys", 0)) + 1
        st["open_positions"] = max(0, int(st.get("open_positions", 0)) + dop)
        st["last_trade_ts"] = ts
        if notional > 0:
            st["spent_sol"] = float(st.get("spent_sol", 0.0)) + notional
        st["open_exposure_sol"] = max(0.0, float(st.get("open_exposure_sol", 0.0)) + dex_f)
        if float(k.get("roll_window_sec") or 0.0) > 0 and count_buy:
            st = _roll_update_on_buy(st, k, notional)
        _save_state(st, prefix, ns)

        _ = mint
    except Exception:
        pass


def note_close(
    realized_pnl_sol: float,
    mint: str | None = None,
    *,
    close_notional_sol: float | None = None,
    **kw: Any,
) -> None:
    try:
        prefix, ns = _prefix_ns()
        pnl = float(realized_pnl_sol or 0.0)
        ts = float(kw.get("ts") or _now())
        dop = int(kw.get("delta_open_positions", -1))

        dex = kw.get("delta_open_exposure_sol")
        if dex is None and close_notional_sol is not None:
            with contextlib.suppress(Exception):
                dex = -float(close_notional_sol or 0.0)
        dex_f = float(dex or 0.0)

        day = _utc_day()
        key = _state_key(day, prefix, ns)

        st = _load_state(prefix, ns)
        loss_streak = int(st.get("loss_streak", 0) or 0)
        win_streak = int(st.get("win_streak", 0) or 0)

        if pnl < 0:
            loss_streak += 1
            win_streak = 0
        elif pnl > 0:
            win_streak += 1
            loss_streak = 0

        st["loss_streak"] = loss_streak
        st["win_streak"] = win_streak

        if "unrealized_sol" in kw:
            with contextlib.suppress(Exception):
                st["unrealized_sol"] = float(kw.get("unrealized_sol") or 0.0)

        r = _get_redis_sync()
        if r is not None:
            _hset(
                key,
                {
                    "day": day,
                    "loss_streak": loss_streak,
                    "win_streak": win_streak,
                    "last_close_ts": ts,
                    **(
                        {"unrealized_sol": st.get("unrealized_sol", 0.0)}
                        if "unrealized_sol" in kw
                        else {}
                    ),
                },
            )
            _hincr(key, "closes", 1.0)
            _hincr(key, "realized_sol", pnl)
            if pnl < 0:
                _hincr(key, "losses", 1.0)
            elif pnl > 0:
                _hincr(key, "wins", 1.0)
            if dop != 0:
                _hincr(key, "open_positions", float(dop))
            if dex_f != 0:
                _hincr(key, "open_exposure_sol", float(dex_f))
        else:
            st["closes"] = int(st.get("closes", 0)) + 1
            st["realized_sol"] = float(st.get("realized_sol", 0.0)) + pnl
            st["last_close_ts"] = ts
            if pnl < 0:
                st["losses"] = int(st.get("losses", 0)) + 1
            elif pnl > 0:
                st["wins"] = int(st.get("wins", 0)) + 1
            st["open_positions"] = max(0, int(st.get("open_positions", 0)) + dop)
            st["open_exposure_sol"] = max(0.0, float(st.get("open_exposure_sol", 0.0)) + dex_f)
            _save_state(st, prefix, ns)

        k = _knobs(prefix, ns)
        if bool(k.get("breaker_enabled")) and int(k.get("breaker_minutes") or 0) > 0:
            st2 = _load_state(prefix, ns)
            ok, reason = _compute(st2, k, prefix=prefix, ns=ns)
            if (not ok) and reason in {"daily_drawdown", "max_loss_streak"}:
                trip_breaker(minutes=int(k.get("breaker_minutes") or 0), reason=str(reason))

        _ = mint
    except Exception:
        pass


def record_buy_result(
    ok: bool, reason: str, sig: str | None = None, mint: str | None = None, **kw: Any
) -> None:
    try:
        prefix, ns = _prefix_ns()
        day = _utc_day()
        key = _state_key(day, prefix, ns)

        st = _load_state(prefix, ns)
        fail_streak = int(st.get("fail_streak", 0) or 0)
        fail_streak = 0 if ok else (fail_streak + 1)
        st["fail_streak"] = fail_streak

        r = _get_redis_sync()
        if r is not None:
            _hset(key, {"day": day, "fail_streak": fail_streak})
            _hincr(key, "buy_attempts", 1.0)
            if not ok:
                _hincr(key, "buy_failures", 1.0)
        else:
            st["buy_attempts"] = int(st.get("buy_attempts", 0)) + 1
            if not ok:
                st["buy_failures"] = int(st.get("buy_failures", 0)) + 1
            _save_state(st, prefix, ns)

        k = _knobs(prefix, ns)
        if r is not None and bool(k.get("buylog_enabled", True)):
            with contextlib.suppress(Exception):
                entry = {
                    "ts": _now(),
                    "ok": bool(ok),
                    "reason": str(reason)[:180],
                    "sig": sig,
                    "mint": mint,
                    "meta": {kk: kw.get(kk) for kk in ("source", "worker_id", "pool") if kk in kw},
                }
                lk = _buylog_key(day, prefix, ns)
                r.lpush(lk, json.dumps(entry, separators=(",", ":"), ensure_ascii=False))
                r.ltrim(lk, 0, int(k.get("buylog_max") or 500))
                r.expire(lk, _state_ttl_sec())

        if (
            bool(k.get("fail_breaker_enabled"))
            and (not ok)
            and bool(k.get("breaker_enabled"))
            and int(k.get("fail_breaker_fails") or 0) > 0
            and float(k.get("fail_breaker_window_sec") or 0.0) > 0
            and float(k.get("fail_breaker_cooldown_sec") or 0.0) > 0
        ):
            with contextlib.suppress(Exception):
                stx = _load_state(prefix, ns)
                now = _now()
                start = float(stx.get("fail_bucket_start", 0.0) or 0.0)
                cnt = int(stx.get("fail_bucket_count", 0) or 0)
                window = float(k.get("fail_breaker_window_sec") or 0.0)

                if start <= 0.0 or (now - start) > window:
                    start = now
                    cnt = 1
                else:
                    cnt += 1

                if r is not None:
                    _hset(key, {"fail_bucket_start": start, "fail_bucket_count": cnt})
                else:
                    stx["fail_bucket_start"] = start
                    stx["fail_bucket_count"] = cnt
                    _save_state(stx, prefix, ns)

                if cnt >= int(k.get("fail_breaker_fails") or 0):
                    trip_breaker(
                        seconds=float(k.get("fail_breaker_cooldown_sec") or 0.0),
                        reason="buy_fail_breaker",
                    )

        max_fail_streak = int(k.get("max_fail_streak") or 0)
        if (
            max_fail_streak > 0
            and (not ok)
            and fail_streak >= max_fail_streak
            and bool(k.get("breaker_enabled"))
        ):
            mins = int(k.get("breaker_minutes") or 0)
            if mins > 0:
                trip_breaker(minutes=mins, reason="max_fail_streak")
            else:
                trip_breaker(
                    seconds=float(k.get("fail_breaker_cooldown_sec") or 30.0),
                    reason="max_fail_streak",
                )
    except Exception:
        pass


# =============================================================================
# Optional publishers/admin helpers
# =============================================================================


def set_equity_sol(sol: float) -> None:
    try:
        prefix, ns = _prefix_ns()
        r = _get_redis_sync()
        if r is None:
            return
        k = _knobs(prefix, ns)
        key = k.get("equity_sol_redis_key") or _default_equity_sol_key(prefix, ns)
        v = float(sol)
        if v <= 0:
            return
        r.set(str(key), str(v), ex=max(30, _envi("GUARD_EQUITY_PUBLISH_TTL_SEC", 120)))
    except Exception:
        pass


def set_sol_usd(price: float) -> None:
    try:
        prefix, ns = _prefix_ns()
        r = _get_redis_sync()
        if r is None:
            return
        k = _knobs(prefix, ns)
        key = k.get("sol_usd_redis_key") or _default_sol_usd_key(prefix, ns)
        v = float(price)
        if v <= 0:
            return
        r.set(str(key), str(v), ex=max(30, _envi("GUARD_SOL_USD_PUBLISH_TTL_SEC", 120)))
    except Exception:
        pass


def reset_today_state() -> None:
    try:
        prefix, ns = _prefix_ns()
        day = _utc_day()
        key = _state_key(day, prefix, ns)
        st = _default_state(day)

        r = _get_redis_sync()
        if r is not None:
            with contextlib.suppress(Exception):
                pipe = r.pipeline()
                pipe.delete(key)
                pipe.hset(key, mapping={k: str(v) for k, v in st.items()})
                pipe.expire(key, _state_ttl_sec())
                pipe.execute()
                _cache_bust()
                return

        mk = _ctx_key(prefix, ns, day)
        with _mem_lock:
            _mem_state.clear()
            _mem_state[mk] = dict(st)

        _cache_bust()
    except Exception:
        pass


__all__ = [
    "buy_block_reason",
    "buy_block_reason_for",
    "buys_allowed",
    "buys_allowed_for",
    "note_close",
    "record_buy_result",
    "record_trade_open",
    "reset_today_state",
    "set_equity_sol",
    "set_sol_usd",
    "snapshot",
    "status",
    "trip_breaker",
]
