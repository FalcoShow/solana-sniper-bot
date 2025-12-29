# -*- coding: utf-8 -*-
from __future__ import annotations

"""
utils_pkg/guardrails.py

Guardrails — God-Tier Safety Gate (Phase 0–7 LOCK-IN)
=====================================================

Single source of truth for: "are buys allowed right now?"

Designed to be imported everywhere:
  - scripts/main.py (pre-gate)
  - utils_pkg/pump_trader.py (execution gate + counters + audit)
  - utils_pkg/exit_engine.py / auto_exit_worker (close hooks)

Hard guarantees
---------------
- NEVER crashes the bot: all public functions are best-effort + exception-suppressed
- Safe at import time (no network I/O)
- Works with or without Redis (process-local fallback)
- Cross-process consistent when Redis exists (hash fields + pipelined ops)
- Deterministic, stable block reasons (for logs/alerts)
- Operator toggles are LIVE (env read each call; knobs cached with tiny TTL)
- Supports: daily caps + percent caps + breaker + streak caps + exposure caps
- Optional: publish equity_sol + sol_usd to Redis for shared gating

Stable public contract
----------------------
buys_allowed() -> bool
buy_block_reason() -> str
status() -> (bool, str, dict)
snapshot() -> dict

record_trade_open(notional_sol, mint=None, **kw) -> None
note_close(realized_pnl_sol, mint=None, close_notional_sol=None, **kw) -> None
record_buy_result(ok, reason, sig=None, mint=None, **kw) -> None
trip_breaker(seconds=None, minutes=None, reason="breaker") -> None

Optional helpers (safe if unused)
set_equity_sol(sol) -> None
set_sol_usd(price) -> None
reset_today_state() -> None
"""

import json
import os
import threading
import time
from typing import Any, Dict, List, Optional, Tuple

# Optional redis clients (sync + asyncio variant if installed)
try:  # pragma: no cover
    import redis  # type: ignore
except Exception:  # pragma: no cover
    redis = None  # type: ignore

try:  # pragma: no cover
    import redis.asyncio as redis_async  # type: ignore
except Exception:  # pragma: no cover
    redis_async = None  # type: ignore

# Optional config fallback (no hard dependency)
try:  # pragma: no cover
    import config as _C  # type: ignore
except Exception:  # pragma: no cover
    _C = None  # type: ignore

# Optional logger (best-effort)
try:  # pragma: no cover
    from logging_setup import get_logger as _get_logger  # type: ignore
except Exception:  # pragma: no cover
    _get_logger = None  # type: ignore


# =============================================================================
# Logging (best-effort + shims)
# =============================================================================
def _log() -> Any:
    lg = None
    if callable(_get_logger):
        try:
            lg = _get_logger("sniper.guardrails")  # preferred if supported
        except Exception:
            try:
                lg = _get_logger()  # fallback signature
            except Exception:
                lg = None

    if lg is not None:
        # shim .event/.every if missing
        if not hasattr(lg, "event"):

            def _event(name: str, **fields: Any) -> None:
                try:
                    lg.info("%s %s", name, fields)
                except Exception:
                    return None

            setattr(lg, "event", _event)

        if not hasattr(lg, "every"):

            def _every(key: str, n: int = 30, msg: Optional[str] = None, **fields: Any) -> None:
                try:
                    now = time.monotonic()
                    store = getattr(_every, "_store", {})  # type: ignore[attr-defined]
                    last = float(store.get(key, 0.0))
                    if now - last >= float(n):
                        store[key] = now
                        setattr(_every, "_store", store)  # type: ignore[attr-defined]
                        getattr(lg, "event")(msg or key, **fields)  # type: ignore[misc]
                except Exception:
                    return None

            setattr(lg, "every", _every)

        return lg

    class _Null:
        def info(self, *a: Any, **k: Any) -> None: return None
        def warning(self, *a: Any, **k: Any) -> None: return None
        def error(self, *a: Any, **k: Any) -> None: return None
        def event(self, *a: Any, **k: Any) -> None: return None
        def every(self, *a: Any, **k: Any) -> None: return None

    return _Null()


log = _log()


# =============================================================================
# Env helpers (fast, tolerant, no throws)
# =============================================================================
_TRUE = {"1", "true", "yes", "y", "on", "t"}
_FALSE = {"0", "false", "no", "n", "off", "f"}


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


def _now() -> float:
    return time.time()


def _utc_day(ts: Optional[float] = None) -> str:
    return time.strftime("%Y-%m-%d", time.gmtime(ts or _now()))


def _pct_to_frac(x: float) -> float:
    # accepts 0.18 (fraction) or 18 (percent)
    try:
        x = float(x)
    except Exception:
        return 0.0
    if x <= 0:
        return 0.0
    return x / 100.0 if x > 1.0 else x


def _clamp(x: float, lo: float, hi: float) -> float:
    try:
        x = float(x)
    except Exception:
        return float(lo)
    if x < lo:
        return float(lo)
    if x > hi:
        return float(hi)
    return float(x)


def _short(s: Any, n: int = 160) -> str:
    try:
        t = str(s)
    except Exception:
        t = "<?>"
    return t[:n]


def _cfgf(name: str, default: float) -> float:
    if _C is None:
        return float(default)
    try:
        return float(getattr(_C, name))  # type: ignore[arg-type]
    except Exception:
        return float(default)


def _cfgi(name: str, default: int) -> int:
    if _C is None:
        return int(default)
    try:
        return int(getattr(_C, name))  # type: ignore[arg-type]
    except Exception:
        return int(default)


# =============================================================================
# Namespacing / keys
# =============================================================================
_PREFIX = (_envs("PF_GUARD_PREFIX", _envs("GUARD_PREFIX", "gr")) or "gr").strip().lower()

# Prefer explicit namespace; else infer from common PaaS env vars
_NAMESPACE = (
    _envs("GUARD_NAMESPACE", "")
    or _envs("PF_GUARD_NS", "")
    or _envs("RENDER_SERVICE_NAME", "")
    or _envs("FLY_APP_NAME", "")
    or _envs("DYNO", "")
    or _envs("HOSTNAME", "")
    or "default"
).strip().lower()

_STATE_TTL_SEC = _envi("GUARD_STATE_TTL_SEC", 4 * 24 * 3600)  # 4 days default

# Optional file toggles (LIVE)
_KILL_FILE = _envs("KILL_SWITCH_FILE", "")
_DISABLE_BUYS_FILE = _envs("DISABLE_BUYS_FILE", "")
_PAUSE_FILE = _envs("PAUSE_TRADING_FILE", _envs("PAUSE_FILE", ""))


def _file_flag(path: str) -> bool:
    if not path:
        return False
    try:
        return bool(os.path.exists(path))
    except Exception:
        return False


def _state_key(day: Optional[str] = None) -> str:
    d = day or _utc_day()
    return f"{_PREFIX}:{_NAMESPACE}:{d}:state"


def _buylog_key(day: Optional[str] = None) -> str:
    d = day or _utc_day()
    return f"{_PREFIX}:{_NAMESPACE}:{d}:buylog"


# Published estimates (optional)
_DEFAULT_EQUITY_SOL_KEY = f"{_PREFIX}:{_NAMESPACE}:equity_sol"
_DEFAULT_SOL_USD_KEY = f"{_PREFIX}:{_NAMESPACE}:sol_usd"

# Optional degraded mode key
_DEGRADED_KEY = f"{_PREFIX}:{_NAMESPACE}:degraded"


# =============================================================================
# Redis client (LAZY, cached, never throws)
# =============================================================================
_REDIS_URL_KEYS = ("REDIS_URL", "UPSTASH_REDIS_URL", "PF_REDIS_URL", "GUARD_REDIS_URL")

_R_SYNC: Optional[Any] = None
_R_ASYNC: Optional[Any] = None
_R_SYNC_AT: float = 0.0
_R_ASYNC_AT: float = 0.0
_R_TTL: float = max(1.0, float(_envf("GUARD_REDIS_REFRESH_SEC", 5.0)))


def _redis_url() -> str:
    for k in _REDIS_URL_KEYS:
        v = os.getenv(k)
        if v:
            return str(v).strip()
    return ""


def _redis_kwargs() -> Dict[str, Any]:
    # redis-py connects on first command (no network at import time)
    return dict(
        socket_timeout=float(_envs("PF_REDIS_SOCKET_TIMEOUT_S", _envs("GUARD_REDIS_SOCKET_TIMEOUT_S", "1.2"))),
        socket_connect_timeout=float(_envs("PF_REDIS_CONNECT_TIMEOUT_S", _envs("GUARD_REDIS_CONNECT_TIMEOUT_S", "1.2"))),
        retry_on_timeout=True,
        decode_responses=True,
    )


def _get_redis_sync() -> Optional[Any]:
    global _R_SYNC, _R_SYNC_AT
    if redis is None:
        return None
    now = _now()
    if _R_SYNC is not None and (now - _R_SYNC_AT) < _R_TTL:
        return _R_SYNC
    _R_SYNC_AT = now
    url = _redis_url()
    if not url:
        _R_SYNC = None
        return None
    try:
        _R_SYNC = redis.from_url(url, **_redis_kwargs())  # type: ignore[attr-defined]
    except Exception:
        _R_SYNC = None
    return _R_SYNC


def _get_redis_async() -> Optional[Any]:
    global _R_ASYNC, _R_ASYNC_AT
    if redis_async is None:
        return None
    now = _now()
    if _R_ASYNC is not None and (now - _R_ASYNC_AT) < _R_TTL:
        return _R_ASYNC
    _R_ASYNC_AT = now
    url = _redis_url()
    if not url:
        _R_ASYNC = None
        return None
    try:
        _R_ASYNC = redis_async.from_url(url, **_redis_kwargs())  # type: ignore[attr-defined]
    except Exception:
        _R_ASYNC = None
    return _R_ASYNC


# =============================================================================
# In-process fallback state (when Redis missing/unavailable)
# =============================================================================
_mem_lock = threading.Lock()
_mem_state: Dict[str, Dict[str, Any]] = {}
_PROCESS_START_TS = _now()


# =============================================================================
# State schema (extend safely; missing fields treated as 0/empty)
# =============================================================================
_STATE_FIELDS: Tuple[str, ...] = (
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


def _default_state(day: str) -> Dict[str, Any]:
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


def _redis_hmget(r: Any, key: str, fields: Tuple[str, ...]) -> Optional[Dict[str, Any]]:
    try:
        vals = r.hmget(key, fields)
        if not isinstance(vals, list) or len(vals) != len(fields):
            return None
        out: Dict[str, Any] = {}
        for f, v in zip(fields, vals):
            if v is not None:
                out[f] = v
        return out
    except Exception:
        return None


def _redis_init_day(r: Any, key: str, day: str) -> Dict[str, Any]:
    st = _default_state(day)
    try:
        pipe = r.pipeline()
        pipe.hset(key, mapping={k: str(v) for k, v in st.items()})
        pipe.expire(key, _STATE_TTL_SEC)
        pipe.execute()
    except Exception:
        pass
    return st


def _load_state() -> Dict[str, Any]:
    day = _utc_day()
    key = _state_key(day)
    r = _get_redis_sync()

    if r is not None:
        m = _redis_hmget(r, key, _STATE_FIELDS)
        if m and str(m.get("day", "")) == day:
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
        return _redis_init_day(r, key, day)

    with _mem_lock:
        st = _mem_state.get(day)
        if st is None:
            st = _default_state(day)
            _mem_state.clear()
            _mem_state[day] = st
        return dict(st)


def _save_state(st: Dict[str, Any]) -> None:
    st = dict(st)
    st["ts"] = _now()
    day = str(st.get("day") or _utc_day())
    key = _state_key(day)
    r = _get_redis_sync()

    if r is not None:
        try:
            pipe = r.pipeline()
            pipe.hset(key, mapping={k: str(v) for k, v in st.items()})
            pipe.expire(key, _STATE_TTL_SEC)
            pipe.execute()
            return
        except Exception:
            pass

    with _mem_lock:
        _mem_state[day] = dict(st)


def _hincr(key: str, field: str, amount: float) -> None:
    r = _get_redis_sync()
    if r is None:
        return
    try:
        pipe = r.pipeline()
        pipe.hincrbyfloat(key, field, float(amount))
        pipe.hset(key, "ts", str(_now()))
        pipe.expire(key, _STATE_TTL_SEC)
        pipe.execute()
    except Exception:
        pass


def _hset(key: str, mapping: Dict[str, Any]) -> None:
    r = _get_redis_sync()
    if r is None:
        return
    try:
        pipe = r.pipeline()
        pipe.hset(key, mapping={k: str(v) for k, v in mapping.items()})
        pipe.hset(key, "ts", str(_now()))
        pipe.expire(key, _STATE_TTL_SEC)
        pipe.execute()
    except Exception:
        pass


# =============================================================================
# Knobs (env-first, config fallback) with tiny TTL refresh
# =============================================================================
_KNOBS_TTL_SEC = max(0.5, float(_envf("GUARD_KNOBS_REFRESH_SEC", 2.0)))
_knobs_at: float = 0.0
_knobs_cache: Dict[str, Any] = {}


def _knobs() -> Dict[str, Any]:
    global _knobs_at, _knobs_cache
    now = _now()
    if _knobs_cache and (now - _knobs_at) < _KNOBS_TTL_SEC:
        return _knobs_cache

    # Operator switches (always live)
    pause_switch = _envs("PAUSE_SWITCH", "PAUSE_TRADING")

    # Daily caps
    max_losses = _envi("PF_DAILY_MAX_LOSSES", _envi("DAILY_MAX_LOSSES", 0))
    max_buys = _envi("PF_DAILY_MAX_BUYS", _envi("DAILY_MAX_BUYS", 0))
    max_spend_sol = _envf("PF_DAILY_MAX_SPEND_SOL", _envf("DAILY_MAX_SPEND_SOL", 0.0))  # 0 => off

    # Percent caps
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
    daily_spend_cap_frac = _pct_to_frac(_envf("DAILY_SPEND_CAP_PCT", _cfgf("DAILY_SPEND_CAP_PCT", 0.0)))

    # Circuit breaker
    breaker_minutes = _envi(
        "RISK_CIRCUIT_BREAKER_MINUTES",
        _envi("GUARD_BREAKER_MINUTES", _cfgi("RISK_CIRCUIT_BREAKER_MINUTES", 0)),
    )
    breaker_enabled = _envb("GUARD_BREAKER_ENABLED", True)

    # Optional buy-failure breaker (rolling window on buy_result failures)
    fail_breaker_enabled = _envb("GUARD_FAIL_BREAKER_ENABLED", False)
    fail_breaker_fails = _envi("GUARD_FAIL_BREAKER_FAILS", 8)
    fail_breaker_window_sec = _envf("GUARD_FAIL_BREAKER_WINDOW_SEC", 60.0)
    fail_breaker_cooldown_sec = _envf("GUARD_FAIL_BREAKER_COOLDOWN_SEC", 45.0)

    # Optional streak limits
    max_loss_streak = _envi("GUARD_MAX_LOSS_STREAK", 0)  # 0 disables
    max_fail_streak = _envi("GUARD_MAX_FAIL_STREAK", 0)  # 0 disables

    # Optional rolling buy-rate caps
    roll_window_sec = _envf("GUARD_ROLL_WINDOW_SEC", 0.0)  # 0 disables
    roll_max_buys = _envi("GUARD_ROLL_MAX_BUYS", 0)
    roll_max_spend_sol = _envf("GUARD_ROLL_MAX_SPEND_SOL", 0.0)

    # Equity estimation (best-effort)
    equity_sol_fallback = _envf("EQUITY_SOL_FALLBACK", 0.0)
    equity_usd_fallback = _envf("EQUITY_USD_FALLBACK", 1500.0)
    sol_usd_fallback = _envf("SOL_PRICE_USD_FALLBACK", 150.0)

    equity_sol_redis_key = _envs("GUARD_EQUITY_SOL_REDIS_KEY", _DEFAULT_EQUITY_SOL_KEY)
    sol_usd_redis_key = _envs("GUARD_SOL_USD_REDIS_KEY", _DEFAULT_SOL_USD_KEY)

    # Exposure caps
    max_open_positions = _envi("GUARD_MAX_OPEN_POSITIONS", 0)
    max_open_exposure_sol = _envf("GUARD_MAX_OPEN_EXPOSURE_SOL", 0.0)

    # Minimum equity gate (optional)
    min_equity_sol = _envf("GUARD_MIN_EQUITY_SOL", 0.0)  # 0 disables

    # Process warmup (optional)
    min_uptime_sec = _envf("GUARD_MIN_UPTIME_SEC", 0.0)  # 0 disables

    # Degraded mode gate (shared)
    degraded_blocks = _envb("GUARD_DEGRADED_BLOCKS_BUYS", True)

    # Fail-open vs fail-closed
    fail_closed = _envb("GUARD_FAIL_CLOSED", False) or (_envs("GUARD_FAIL_MODE", "").lower() in {"closed", "fail_closed"})

    # Overrides
    force_allow = _envb("GUARD_FORCE_ALLOW_BUYS", False)
    force_block = _envb("GUARD_FORCE_BLOCK_BUYS", False)

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
        "equity_sol_redis_key": equity_sol_redis_key,
        "sol_usd_redis_key": sol_usd_redis_key,
        "max_open_positions": int(max(0, max_open_positions)),
        "max_open_exposure_sol": float(max(0.0, max_open_exposure_sol)),
        "min_equity_sol": float(max(0.0, min_equity_sol)),
        "min_uptime_sec": float(max(0.0, min_uptime_sec)),
        "degraded_blocks": bool(degraded_blocks),
        "fail_closed": bool(fail_closed),
        "force_allow": bool(force_allow),
        "force_block": bool(force_block),
    }
    _knobs_at = now
    return _knobs_cache


def _get_sol_usd_est(k: Dict[str, Any]) -> float:
    r = _get_redis_sync()
    if r is not None and k.get("sol_usd_redis_key"):
        try:
            v = r.get(str(k["sol_usd_redis_key"]))
            if v:
                x = float(v)
                if x > 0:
                    return x
        except Exception:
            pass
    return float(k.get("sol_usd_fallback") or 0.0)


def _get_equity_sol_est(k: Dict[str, Any]) -> float:
    # 1) explicit env
    if float(k.get("equity_sol_fallback") or 0.0) > 0:
        return float(k["equity_sol_fallback"])

    # 2) redis published value
    r = _get_redis_sync()
    if r is not None and k.get("equity_sol_redis_key"):
        try:
            v = r.get(str(k["equity_sol_redis_key"]))
            if v:
                x = float(v)
                if x > 0:
                    return x
        except Exception:
            pass

    # 3) derive from USD fallback
    sol_usd = _get_sol_usd_est(k)
    if sol_usd > 0:
        return float(k.get("equity_usd_fallback") or 0.0) / float(sol_usd)

    return 0.0


def _is_degraded() -> bool:
    # env override first
    if _envb("DEGRADED_MODE", False) or _envb("SAFE_MODE", False):
        return True
    r = _get_redis_sync()
    if r is not None:
        try:
            v = r.get(_DEGRADED_KEY)
            if v is None:
                return False
            s = str(v).strip().lower()
            return s in _TRUE or s == "degraded"
        except Exception:
            return False
    return False


# =============================================================================
# Deterministic compute: allow/block + reason
# =============================================================================
def _compute(st: Dict[str, Any], k: Dict[str, Any]) -> Tuple[bool, str]:
    # Hard overrides
    if bool(k.get("force_allow")) and (not bool(k.get("force_block"))):
        return True, "force_allow"
    if bool(k.get("force_block")):
        return False, "force_block"

    # Operator pause switch (live)
    pause_key = str(k.get("pause_switch") or "PAUSE_TRADING")
    if _envb(pause_key, False) or _file_flag(_PAUSE_FILE):
        return False, "paused"

    # Hard kill switches (live)
    if _envb("KILL_SWITCH", False) or _file_flag(_KILL_FILE):
        return False, "kill_switch"
    if _envb("DISABLE_BUYS", False) or _file_flag(_DISABLE_BUYS_FILE):
        return False, "disable_buys"

    # Degraded gate (shared)
    if bool(k.get("degraded_blocks", True)) and _is_degraded() and (not _envb("GUARD_ALLOW_WHEN_DEGRADED", False)):
        return False, "degraded_mode"

    # Warmup
    min_uptime = float(k.get("min_uptime_sec") or 0.0)
    if min_uptime > 0 and (_now() - _PROCESS_START_TS) < min_uptime:
        return False, "warmup"

    # Circuit breaker cooldown
    until = float(st.get("breaker_until", 0.0) or 0.0)
    if until > _now():
        return False, "breaker_cooldown"

    # Exposure caps
    mop = int(k.get("max_open_positions") or 0)
    if mop > 0 and int(st.get("open_positions", 0) or 0) >= mop:
        return False, "max_open_positions"

    moe = float(k.get("max_open_exposure_sol") or 0.0)
    if moe > 0 and float(st.get("open_exposure_sol", 0.0) or 0.0) >= moe:
        return False, "max_open_exposure_sol"

    # Equity estimate + min equity safety
    equity_sol = _get_equity_sol_est(k)
    min_eq = float(k.get("min_equity_sol") or 0.0)
    if min_eq > 0 and equity_sol > 0 and equity_sol < min_eq:
        return False, "min_equity_sol"

    # Streak caps
    max_loss_streak = int(k.get("max_loss_streak") or 0)
    if max_loss_streak > 0 and int(st.get("loss_streak", 0) or 0) >= max_loss_streak:
        return False, "max_loss_streak"

    max_fail_streak = int(k.get("max_fail_streak") or 0)
    if max_fail_streak > 0 and int(st.get("fail_streak", 0) or 0) >= max_fail_streak:
        return False, "max_fail_streak"

    # Rolling window caps
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

    # Daily caps
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
    if spend_cap_frac > 0 and equity_sol > 0:
        if spent_sol >= (equity_sol * spend_cap_frac):
            return False, "daily_spend_cap_pct"

    # PnL gates (realized only by default; optional unrealized publisher supported)
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
_BLOCK_THROTTLE_SEC = float(_envf("GUARD_BLOCK_THROTTLE_SEC", 3.0))


def _mark_block(reason: str) -> None:
    try:
        global _last_block_reason, _last_block_ts
        reason = (reason or "blocked").strip()[:120]
        now = _now()

        with _block_lock:
            if reason == _last_block_reason and (now - _last_block_ts) < _BLOCK_THROTTLE_SEC:
                return
            _last_block_reason = reason
            _last_block_ts = now

        day = _utc_day()
        key = _state_key(day)

        _hset(key, {"day": day, "last_block_reason": reason})
        _hincr(key, "blocks", 1.0)

        # mem fallback if redis missing
        if _get_redis_sync() is None:
            st = _load_state()
            st["blocks"] = int(st.get("blocks", 0)) + 1
            st["last_block_reason"] = reason
            _save_state(st)

        log.every("GUARD_BLOCK", 10, reason=reason, day=day, ns=_NAMESPACE)
    except Exception:
        pass


# =============================================================================
# Public API (NEVER raises)
# =============================================================================
def buys_allowed() -> bool:
    """Primary boolean gate."""
    try:
        st = _load_state()
        k = _knobs()
        ok, reason = _compute(st, k)
        if not ok:
            _mark_block(reason)
        return bool(ok)
    except Exception:
        try:
            if _knobs().get("fail_closed"):
                return False
        except Exception:
            pass
        return True


def buy_block_reason() -> str:
    """Computed reason (best-effort)."""
    try:
        st = _load_state()
        k = _knobs()
        ok, reason = _compute(st, k)
        return "ok" if ok else str(reason)
    except Exception:
        return "unknown"


def status() -> Tuple[bool, str, Dict[str, Any]]:
    """Returns (ok, reason, snapshot dict)."""
    try:
        st = _load_state()
        k = _knobs()
        ok, reason = _compute(st, k)
        snap = snapshot()
        snap["computed"] = {"ok": bool(ok), "reason": str(reason)}
        return bool(ok), str(reason), snap
    except Exception:
        return False, "error", {"error": "status_failed"}


def snapshot() -> Dict[str, Any]:
    """Safe state snapshot (no secrets)."""
    try:
        st = _load_state()
        k = _knobs()
        equity_sol = _get_equity_sol_est(k)
        sol_usd = _get_sol_usd_est(k)

        realized = float(st.get("realized_sol", 0.0) or 0.0)
        unreal = float(st.get("unrealized_sol", 0.0) or 0.0)
        include_unreal = _envb("GUARD_INCLUDE_UNREALIZED_IN_PNL", False)
        pnl_gate = realized + (unreal if include_unreal else 0.0)

        dd_sol = -min(0.0, pnl_gate)
        prof_sol = max(0.0, pnl_gate)

        ok, reason = _compute(st, k)

        return {
            "day": st.get("day"),
            "namespace": _NAMESPACE,
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
            },
            "files": {
                "kill_file": _short(_KILL_FILE),
                "disable_buys_file": _short(_DISABLE_BUYS_FILE),
                "pause_file": _short(_PAUSE_FILE),
            },
        }
    except Exception:
        return {"error": "snapshot_failed"}


# =============================================================================
# Mutators (best-effort, never crash)
# =============================================================================
def trip_breaker(
    seconds: Optional[float] = None,
    *,
    minutes: Optional[int] = None,
    reason: str = "breaker",
) -> None:
    """Manually trip cooldown breaker (cross-process if Redis exists)."""
    try:
        k = _knobs()
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
        key = _state_key(day)
        rsn = (reason or "breaker").strip()[:120]

        r = _get_redis_sync()
        if r is not None:
            try:
                cur = r.hget(key, "breaker_until")
                cur_f = float(cur) if cur else 0.0
            except Exception:
                cur_f = 0.0
            until2 = max(cur_f, until)
            _hset(key, {"day": day, "breaker_until": until2, "last_block_reason": f"breaker:{rsn}"})
            _hincr(key, "blocks", 1.0)
            return

        st = _load_state()
        st["breaker_until"] = max(float(st.get("breaker_until", 0.0) or 0.0), until)
        st["last_block_reason"] = f"breaker:{rsn}"
        st["blocks"] = int(st.get("blocks", 0)) + 1
        _save_state(st)
    except Exception:
        pass


def _roll_update_on_buy(st: Dict[str, Any], k: Dict[str, Any], notional: float) -> Dict[str, Any]:
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


def record_trade_open(notional_sol: float, mint: Optional[str] = None, **kw: Any) -> None:
    """
    Call when a BUY is accepted/submitted (or filled if you use it that way).
    Tracks: buys, spent_sol, open_positions, open_exposure_sol, rolling buckets.

    Optional kw:
      - count_buy: bool (default True)
      - delta_open_positions: int (default +1)
      - delta_open_exposure_sol: float (default +notional)
      - ts: float (override timestamp)
    """
    try:
        notional = float(notional_sol or 0.0)
        if notional < 0:
            notional = 0.0

        day = _utc_day()
        key = _state_key(day)
        k = _knobs()
        ts = float(kw.get("ts") or _now())

        count_buy = bool(kw.get("count_buy", True))
        dop = int(kw.get("delta_open_positions", 1))
        dex = kw.get("delta_open_exposure_sol", None)
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
                st = _load_state()
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
            return

        st = _load_state()
        if count_buy:
            st["buys"] = int(st.get("buys", 0)) + 1
        st["open_positions"] = max(0, int(st.get("open_positions", 0)) + dop)
        st["last_trade_ts"] = ts
        if notional > 0:
            st["spent_sol"] = float(st.get("spent_sol", 0.0)) + notional
        st["open_exposure_sol"] = max(0.0, float(st.get("open_exposure_sol", 0.0)) + dex_f)
        if float(k.get("roll_window_sec") or 0.0) > 0 and count_buy:
            st = _roll_update_on_buy(st, k, notional)
        _save_state(st)
    except Exception:
        pass


def note_close(
    realized_pnl_sol: float,
    mint: Optional[str] = None,
    *,
    close_notional_sol: Optional[float] = None,
    **kw: Any,
) -> None:
    """
    Call after a position is closed.
    Tracks realized P&L and win/loss counts and streaks.
    Optionally decrements open_exposure_sol if you pass close_notional_sol.
    Auto-trips breaker if drawdown/streak gate becomes active.

    Optional kw:
      - delta_open_positions: int (default -1)
      - delta_open_exposure_sol: float (default -close_notional_sol if provided)
      - ts: float (override timestamp)
      - unrealized_sol: float (optional publisher for gates if enabled)
    """
    try:
        pnl = float(realized_pnl_sol or 0.0)
        ts = float(kw.get("ts") or _now())
        dop = int(kw.get("delta_open_positions", -1))

        dex = kw.get("delta_open_exposure_sol", None)
        if dex is None and close_notional_sol is not None:
            try:
                dex = -float(close_notional_sol or 0.0)
            except Exception:
                dex = 0.0
        dex_f = float(dex or 0.0)

        day = _utc_day()
        key = _state_key(day)

        st = _load_state()
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
            try:
                st["unrealized_sol"] = float(kw.get("unrealized_sol") or 0.0)
            except Exception:
                pass

        r = _get_redis_sync()
        if r is not None:
            _hset(
                key,
                {
                    "day": day,
                    "loss_streak": loss_streak,
                    "win_streak": win_streak,
                    "last_close_ts": ts,
                    **({"unrealized_sol": st.get("unrealized_sol", 0.0)} if "unrealized_sol" in kw else {}),
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
            _save_state(st)

        k = _knobs()
        if bool(k.get("breaker_enabled")) and int(k.get("breaker_minutes") or 0) > 0:
            st2 = _load_state()
            ok, reason = _compute(st2, k)
            if (not ok) and reason in {"daily_drawdown", "max_loss_streak"}:
                trip_breaker(minutes=int(k.get("breaker_minutes") or 0), reason=str(reason))
    except Exception:
        pass


def record_buy_result(ok: bool, reason: str, sig: Optional[str] = None, mint: Optional[str] = None, **kw: Any) -> None:
    """
    Audit log for buy attempts and optional fail-breaker trip.
    Maintains fail_streak for repeated execution failures.
    """
    try:
        day = _utc_day()
        key = _state_key(day)

        st = _load_state()
        fail_streak = int(st.get("fail_streak", 0) or 0)
        if ok:
            fail_streak = 0
        else:
            fail_streak += 1
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
            _save_state(st)

        # rolling log (redis only)
        if r is not None and _envb("GUARD_BUYLOG_ENABLED", True):
            try:
                entry = {
                    "ts": _now(),
                    "ok": bool(ok),
                    "reason": str(reason)[:180],
                    "sig": sig,
                    "mint": mint,
                    "meta": {k: kw.get(k) for k in ("source", "worker_id", "pool") if k in kw},
                }
                lk = _buylog_key(day)
                r.lpush(lk, json.dumps(entry, separators=(",", ":"), ensure_ascii=False))
                r.ltrim(lk, 0, int(_envi("GUARD_BUYLOG_MAX", 500)))
                r.expire(lk, _STATE_TTL_SEC)
            except Exception:
                pass

        # optional failure breaker (rolling window)
        k = _knobs()
        if (
            bool(k.get("fail_breaker_enabled"))
            and (not ok)
            and bool(k.get("breaker_enabled"))
            and int(k.get("fail_breaker_fails") or 0) > 0
            and float(k.get("fail_breaker_window_sec") or 0.0) > 0
            and float(k.get("fail_breaker_cooldown_sec") or 0.0) > 0
        ):
            try:
                stx = _load_state()
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
                    _save_state(stx)

                if cnt >= int(k.get("fail_breaker_fails") or 0):
                    trip_breaker(seconds=float(k.get("fail_breaker_cooldown_sec") or 0.0), reason="buy_fail_breaker")
            except Exception:
                pass

        # optional fail-streak breaker
        max_fail_streak = int(k.get("max_fail_streak") or 0)
        if max_fail_streak > 0 and (not ok) and fail_streak >= max_fail_streak and bool(k.get("breaker_enabled")):
            mins = int(k.get("breaker_minutes") or 0)
            if mins > 0:
                trip_breaker(minutes=mins, reason="max_fail_streak")
            else:
                trip_breaker(seconds=float(k.get("fail_breaker_cooldown_sec") or 30.0), reason="max_fail_streak")
    except Exception:
        pass


# =============================================================================
# Optional publishers/admin helpers
# =============================================================================
def set_equity_sol(sol: float) -> None:
    """Publish equity SOL estimate to Redis (if available). No-op otherwise."""
    try:
        r = _get_redis_sync()
        if r is None:
            return
        key = _knobs().get("equity_sol_redis_key") or _DEFAULT_EQUITY_SOL_KEY
        v = float(sol)
        if v <= 0:
            return
        r.set(str(key), str(v), ex=max(30, _envi("GUARD_EQUITY_PUBLISH_TTL_SEC", 120)))
    except Exception:
        pass


def set_sol_usd(price: float) -> None:
    """Publish SOL/USD to Redis (if available). No-op otherwise."""
    try:
        r = _get_redis_sync()
        if r is None:
            return
        key = _knobs().get("sol_usd_redis_key") or _DEFAULT_SOL_USD_KEY
        v = float(price)
        if v <= 0:
            return
        r.set(str(key), str(v), ex=max(30, _envi("GUARD_SOL_USD_PUBLISH_TTL_SEC", 120)))
    except Exception:
        pass


def reset_today_state() -> None:
    """Reset today's counters (admin/debug). Safe no-op if Redis missing."""
    try:
        day = _utc_day()
        key = _state_key(day)
        st = _default_state(day)
        r = _get_redis_sync()
        if r is not None:
            pipe = r.pipeline()
            pipe.delete(key)
            pipe.hset(key, mapping={k: str(v) for k, v in st.items()})
            pipe.expire(key, _STATE_TTL_SEC)
            pipe.execute()
            return
        with _mem_lock:
            _mem_state.clear()
            _mem_state[day] = dict(st)
    except Exception:
        pass


__all__ = [
    "buys_allowed",
    "buy_block_reason",
    "status",
    "snapshot",
    "record_trade_open",
    "note_close",
    "record_buy_result",
    "trip_breaker",
    "set_equity_sol",
    "set_sol_usd",
    "reset_today_state",
]
