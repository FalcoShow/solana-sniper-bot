from __future__ import annotations

"""
utils_pkg/circuit_breakers.py

Circuit Breakers — God-Tier Panic Stops (Phase 0–7 LOCK-IN)
===========================================================

Role
----
This module is the "PANIC LAYER" that halts *trading* when conditions indicate
the system is unhealthy or leaking edge:

  • Kill switches / pause switches (operator control)
  • Routing / RPC error-rate meltdown (quote/swap failures spike)
  • Daily loss stop (realized PnL vs equity snapshot)
  • Loss streak stop (consecutive losing closes)
  • Optional breaker cooldown window ("cooldown" halt)
  • Degraded mode flag (shared key) that can block trading

IMPORTANT DEDUP RULE
--------------------
`utils_pkg/guardrails.py` is the canonical "are buys allowed right now?"
This module MUST NOT fork competing entry logic. It should:

  - Evaluate *panic* conditions that warrant halting trading
  - Trip guardrails breaker (preferred) so all processes observe the halt
  - Provide stable gating for entry loops

Design goals
------------
- Never raise exceptions to caller (hard safety)
- No network I/O at import time (Redis connection is lazy)
- Redis optional; in-memory fallback works
- Namespaced keys to avoid cross-bot collisions
- Backwards compatible env parsing

Stable public contract
----------------------
record_error(ok: bool, kind: str="router", where: str="", **meta) -> None
record_close(*, realized_pnl_sol=None, equity_sol=None, realized_pnl_usd=None, equity_usd=None, **meta) -> None
set_degraded(on: bool, reason: str="degraded", ttl_sec: int=120) -> None
is_degraded() -> bool

breaker_active() -> bool
trip_breaker(seconds: float | None = None, minutes: int | None = None, reason: str="breaker") -> None

should_halt() -> tuple[bool, str, dict]     # (halt, reason, snapshot)
trading_allowed() -> tuple[bool, str, dict] # inverse of should_halt()

status() -> dict    # alias of snapshot()
snapshot() -> dict
"""

import datetime as dt
import os
import threading
import time
from typing import Any

# -----------------------------------------------------------------------------
# Optional deps (best-effort only) — typed Optional to keep mypy happy
# -----------------------------------------------------------------------------
_redis_mod: Any | None = None
try:  # pragma: no cover
    import redis as _redis_mod  # type: ignore
except Exception:  # pragma: no cover
    _redis_mod = None

_guard: Any | None = None
try:  # pragma: no cover
    from utils_pkg import guardrails as _guard  # type: ignore
except Exception:  # pragma: no cover
    _guard = None

_get_logger: Any | None = None
try:  # pragma: no cover
    from utils_pkg.logging_setup import get_logger as _get_logger  # type: ignore
except Exception:  # pragma: no cover
    try:  # pragma: no cover
        from logging_setup import get_logger as _get_logger  # type: ignore
    except Exception:  # pragma: no cover
        _get_logger = None


# -----------------------------------------------------------------------------
# Logging shim (event + every)
# -----------------------------------------------------------------------------
def _mk_logger() -> Any:
    lg: Any = None
    if callable(_get_logger):
        try:
            lg = _get_logger("sniper.circuit_breakers")
        except Exception:
            try:
                lg = _get_logger()
            except Exception:
                lg = None

    if lg is None:
        # ultra-safe null logger
        class _Null:
            def info(self, *a: Any, **k: Any) -> None:  # noqa: ANN401
                return None

            def warning(self, *a: Any, **k: Any) -> None:  # noqa: ANN401
                return None

            def error(self, *a: Any, **k: Any) -> None:  # noqa: ANN401
                return None

            def event(self, *a: Any, **k: Any) -> None:  # noqa: ANN401
                return None

            def every(self, *a: Any, **k: Any) -> None:  # noqa: ANN401
                return None

        return _Null()

    # Attach .event if missing
    if not hasattr(lg, "event"):

        def _event(name: str, **fields: Any) -> None:  # pragma: no cover
            try:
                lg.info("%s %s", name, fields)
            except Exception:
                return None

        lg.event = _event  # type: ignore[attr-defined]

    # Attach .every if missing (time-throttled event logger)
    if not hasattr(lg, "every"):
        _store: dict[str, float] = {}

        def _every(
            key: str, n: int = 30, msg: str | None = None, **fields: Any
        ) -> None:  # pragma: no cover
            try:
                now = time.monotonic()
                last = float(_store.get(key, 0.0))
                if now - last >= float(n):
                    _store[key] = now
                    lg.event(msg or key, **fields)  # type: ignore[misc]
            except Exception:
                return None

        lg.every = _every  # type: ignore[attr-defined]

    return lg


log = _mk_logger()

_TRUE = {"1", "true", "yes", "y", "on", "t"}
_FALSE = {"0", "false", "no", "n", "off", "f"}


# -----------------------------------------------------------------------------
# Env helpers (backwards compatible + mypy-safe)
# -----------------------------------------------------------------------------
def _envs(k: str, d: str = "") -> str:
    try:
        v = os.getenv(k)
        return (v if v is not None else d).strip()
    except Exception:
        return str(d).strip()


def _envb(k: str, d: bool = False) -> bool:
    s = _envs(k, "")
    if s == "":
        return bool(d)
    v = s.lower().strip()
    if v in _TRUE:
        return True
    if v in _FALSE:
        return False
    return bool(d)


def _envi(k: str, d: int) -> int:
    s = _envs(k, "")
    if s == "":
        return int(d)
    try:
        return int(float(s.replace("_", "")))
    except Exception:
        return int(d)


def _envf(k: str, d: float) -> float:
    s = _envs(k, "")
    if s == "":
        return float(d)
    try:
        return float(s.replace("_", ""))
    except Exception:
        return float(d)


def _now() -> float:
    return time.time()


def _utc_day(ts: float | None = None) -> str:
    return dt.datetime.utcfromtimestamp(ts or _now()).strftime("%Y-%m-%d")


def _pct_to_frac(x: float) -> float:
    """
    Accepts 0.08 or 8 and returns fraction (0.08).
    """
    try:
        v = float(x)
    except Exception:
        return 0.0
    if v <= 0:
        return 0.0
    return v / 100.0 if v > 1.0 else v


def _clamp(x: float, lo: float, hi: float) -> float:
    try:
        v = float(x)
    except Exception:
        return float(lo)
    if v < lo:
        return float(lo)
    if v > hi:
        return float(hi)
    return float(v)


def _file_exists(path: str) -> bool:
    if not path:
        return False
    try:
        return bool(os.path.exists(path))
    except Exception:
        return False


# -----------------------------------------------------------------------------
# Namespacing / keys
# -----------------------------------------------------------------------------
_PREFIX = (_envs("CB_PREFIX", _envs("CIRCUIT_PREFIX", "cb")) or "cb").strip().lower()
_NAMESPACE = (
    (
        _envs("CB_NAMESPACE", "")
        or _envs("GUARD_NAMESPACE", "")
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

_TTL_SEC = int(max(60.0, float(_envi("CB_STATE_TTL_SEC", 4 * 24 * 3600))))

_DEGRADED_KEY = f"{_PREFIX}:{_NAMESPACE}:degraded"


def _k_err(day: str) -> str:
    return f"{_PREFIX}:{_NAMESPACE}:{day}:errs"


def _k_pnl(day: str) -> str:
    return f"{_PREFIX}:{_NAMESPACE}:{day}:pnl"


def _k_streak(day: str) -> str:
    return f"{_PREFIX}:{_NAMESPACE}:{day}:streak"


def _k_breaker(day: str) -> str:
    return f"{_PREFIX}:{_NAMESPACE}:{day}:breaker"


# -----------------------------------------------------------------------------
# Redis (lazy, cached, never throws)
# -----------------------------------------------------------------------------
_REDIS_URL_KEYS = (
    "REDIS_URL",
    "UPSTASH_REDIS_URL",
    "PF_REDIS_URL",
    "CB_REDIS_URL",
    "GUARD_REDIS_URL",
)

_R: Any | None = None
_R_AT: float = 0.0
_R_REFRESH = float(max(1.0, _envf("CB_REDIS_REFRESH_SEC", 5.0)))


def _redis_url() -> str:
    for k in _REDIS_URL_KEYS:
        v = os.getenv(k)
        if v:
            return str(v).strip()
    return ""


def _redis_kwargs() -> dict[str, Any]:
    # Keep minimal + safe timeouts. Avoid passing unsupported kwargs across redis-py versions.
    return dict(
        decode_responses=True,
        socket_timeout=float(_envf("CB_REDIS_SOCKET_TIMEOUT_S", 1.2)),
        socket_connect_timeout=float(_envf("CB_REDIS_CONNECT_TIMEOUT_S", 1.2)),
        retry_on_timeout=True,
    )


def _redis_from_url(url: str) -> Any | None:
    if _redis_mod is None:
        return None

    # Support both redis.from_url and redis.Redis.from_url depending on version
    try:
        fn = getattr(_redis_mod, "from_url", None)
        if callable(fn):
            return fn(url, **_redis_kwargs())
    except Exception:
        pass

    try:
        cls = getattr(_redis_mod, "Redis", None)
        if cls is not None and callable(getattr(cls, "from_url", None)):
            return cls.from_url(url, **_redis_kwargs())
    except Exception:
        pass

    return None


def _r() -> Any | None:
    global _R, _R_AT
    if _redis_mod is None:
        return None

    now = _now()
    if _R is not None and (now - _R_AT) < _R_REFRESH:
        return _R

    _R_AT = now
    url = _redis_url()
    if not url:
        _R = None
        return None

    try:
        _R = _redis_from_url(url)
    except Exception:
        _R = None
    return _R


# -----------------------------------------------------------------------------
# In-memory fallback (when Redis missing)
# -----------------------------------------------------------------------------
_mem_lock = threading.Lock()
_mem: dict[str, dict[str, Any]] = {}


def _mem_get(day: str) -> dict[str, Any]:
    with _mem_lock:
        st = _mem.get(day)
        if st is None:
            st = {
                "day": day,
                # error counters
                "errs_n": 0,
                "errs_bad": 0,
                # pnl channels
                "pnl_sol": 0.0,
                "eq_sol": 0.0,
                "pnl_usd": 0.0,
                "eq_usd": 0.0,
                # streak
                "losers": 0,
                "winners": 0,
                # breaker
                "breaker_until": 0.0,
                "last_reason": "",
                # misc
                "ts": _now(),
            }
            _mem[day] = st
        return dict(st)


def _mem_set(day: str, st: dict[str, Any]) -> None:
    with _mem_lock:
        _mem[day] = dict(st)


# -----------------------------------------------------------------------------
# Knobs (env-first, backwards compatible)
# -----------------------------------------------------------------------------
_KNOBS_TTL = float(max(0.5, _envf("CB_KNOBS_REFRESH_SEC", 2.0)))
_knobs_at: float = 0.0
_knobs_cache: dict[str, Any] = {}


def _knobs() -> dict[str, Any]:
    global _knobs_at, _knobs_cache
    now = _now()
    if _knobs_cache and (now - _knobs_at) < _KNOBS_TTL:
        return _knobs_cache

    # Backwards compatible names:
    #   DAILY_STOP_PCT, MAX_CONSECUTIVE_LOSSES, ERROR_RATE_HALT
    daily_stop_frac = _pct_to_frac(
        _envf(
            "CB_DAILY_STOP_PCT",
            _envf("DAILY_STOP_PCT", _envf("RISK_MAX_DAILY_DRAWDOWN_PCT", 0.0)),
        )
    )
    max_loss_streak = _envi(
        "CB_MAX_CONSECUTIVE_LOSSES",
        _envi("MAX_CONSECUTIVE_LOSSES", _envi("GUARD_MAX_LOSS_STREAK", 0)),
    )
    err_rate_halt = _pct_to_frac(_envf("CB_ERROR_RATE_HALT", _envf("ERROR_RATE_HALT", 0.0)))
    err_min_n = _envi("CB_ERROR_MIN_N", _envi("ERROR_MIN_N", 20))

    # Optional: when err rate breached, trip a breaker cooldown
    err_trip_cooldown_sec = float(max(0.0, _envf("CB_ERROR_COOLDOWN_SEC", 45.0)))

    # Local breaker support (if guardrails absent)
    breaker_minutes = int(
        max(0, _envi("CB_BREAKER_MINUTES", _envi("RISK_CIRCUIT_BREAKER_MINUTES", 0)))
    )
    breaker_enabled = bool(_envb("CB_BREAKER_ENABLED", True))

    # Kill/pause switches (live)
    pause_key = _envs("CB_PAUSE_SWITCH", _envs("PAUSE_SWITCH", "PAUSE_TRADING"))
    kill_file = _envs("CB_KILL_SWITCH_FILE", _envs("KILL_SWITCH_FILE", ""))
    pause_file = _envs("CB_PAUSE_FILE", _envs("PAUSE_TRADING_FILE", _envs("PAUSE_FILE", "")))

    # Degraded gate
    degraded_blocks = bool(_envb("CB_DEGRADED_BLOCKS_TRADING", True))

    # Fail-open vs fail-closed
    fail_closed = bool(_envb("CB_FAIL_CLOSED", False)) or (
        _envs("CB_FAIL_MODE", "").lower() in {"closed", "fail_closed"}
    )

    _knobs_cache = {
        "daily_stop_frac": float(_clamp(daily_stop_frac, 0.0, 1.0)),
        "max_loss_streak": int(max(0, max_loss_streak)),
        "err_rate_halt": float(_clamp(err_rate_halt, 0.0, 1.0)),
        "err_min_n": int(max(0, err_min_n)),
        "err_trip_cooldown_sec": float(err_trip_cooldown_sec),
        "breaker_minutes": int(breaker_minutes),
        "breaker_enabled": bool(breaker_enabled),
        "pause_key": str(pause_key or "PAUSE_TRADING"),
        "kill_file": str(kill_file or ""),
        "pause_file": str(pause_file or ""),
        "degraded_blocks": bool(degraded_blocks),
        "fail_closed": bool(fail_closed),
    }
    _knobs_at = now
    return _knobs_cache


# -----------------------------------------------------------------------------
# Degraded mode (shared key)
# -----------------------------------------------------------------------------
def set_degraded(on: bool, reason: str = "degraded", ttl_sec: int = 120) -> None:
    """Set a shared degraded-mode flag (cross-process if Redis exists)."""
    try:
        val = "1" if on else "0"
        ttl = max(5, int(ttl_sec))

        r = _r()
        if r is not None:
            try:
                r.set(_DEGRADED_KEY, val, ex=ttl)
            except Exception:
                pass
        else:
            # in-memory fallback: store under special day key
            day = "_degraded"
            st = _mem_get(day)
            st["on"] = bool(on)
            st["reason"] = str(reason)[:120]
            st["until"] = _now() + float(ttl)
            _mem_set(day, st)

        log.every("CB_DEGRADED", 10, on=int(bool(on)), reason=str(reason)[:120], ttl=ttl)
    except Exception:
        return


def is_degraded() -> bool:
    try:
        # env override always wins
        if _envb("DEGRADED_MODE", False) or _envb("SAFE_MODE", False):
            return True

        r = _r()
        if r is not None:
            try:
                v = r.get(_DEGRADED_KEY)
            except Exception:
                v = None
            if v is None:
                return False
            s = str(v).strip().lower()
            return s in _TRUE or s == "degraded"

        st = _mem_get("_degraded")
        until = float(st.get("until", 0.0) or 0.0)
        if until > _now():
            return bool(st.get("on", False))
        return False
    except Exception:
        return False


# -----------------------------------------------------------------------------
# Error tracking
# -----------------------------------------------------------------------------
def record_error(ok: bool, kind: str = "router", where: str = "", **meta: Any) -> None:
    """
    Record a health/error event.
    - ok=True increments total count
    - ok=False increments total + bad

    Suggested kinds:
      - "router_quote", "router_swap"
      - "rpc", "ws", "price_feed"
      - "db", "redis"
    """
    try:
        day = _utc_day()
        r = _r()

        kind_key = str(kind).strip()[:48] or "router"

        if r is not None:
            key = _k_err(day)
            try:
                pipe = r.pipeline()
                pipe.hincrby(key, "n", 1)
                pipe.hincrby(key, f"n:{kind_key}", 1)
                if not ok:
                    pipe.hincrby(key, "bad", 1)
                    pipe.hincrby(key, f"bad:{kind_key}", 1)
                pipe.hset(key, mapping={"day": day, "ts": str(_now())})
                pipe.expire(key, _TTL_SEC)
                pipe.execute()
            except Exception:
                st = _mem_get(day)
                st["errs_n"] = int(st.get("errs_n", 0) or 0) + 1
                if not ok:
                    st["errs_bad"] = int(st.get("errs_bad", 0) or 0) + 1
                st["ts"] = _now()
                _mem_set(day, st)
        else:
            st = _mem_get(day)
            st["errs_n"] = int(st.get("errs_n", 0) or 0) + 1
            if not ok:
                st["errs_bad"] = int(st.get("errs_bad", 0) or 0) + 1
            st["ts"] = _now()
            _mem_set(day, st)

        if not ok:
            log.every(
                "CB_ERR",
                6,
                kind=kind_key,
                where=str(where)[:64],
                meta=str({k: meta.get(k) for k in list(meta)[:6]})[:220],
            )
    except Exception:
        return


def _error_stats(day: str) -> dict[str, Any]:
    r = _r()
    if r is not None:
        try:
            m = r.hgetall(_k_err(day)) or {}
            n = int(float(m.get("n", 0) or 0))
            bad = int(float(m.get("bad", 0) or 0))
            rate = (bad / n) if n > 0 else 0.0
            return {"n": n, "bad": bad, "rate": float(rate)}
        except Exception:
            return {"n": 0, "bad": 0, "rate": 0.0}

    st = _mem_get(day)
    n = int(st.get("errs_n", 0) or 0)
    bad = int(st.get("errs_bad", 0) or 0)
    rate = (bad / n) if n > 0 else 0.0
    return {"n": n, "bad": bad, "rate": float(rate)}


def error_rate_breached() -> tuple[bool, str, dict[str, Any]]:
    """
    Returns (breached, reason, stats).
    Breach requires min sample size.
    """
    try:
        k = _knobs()
        day = _utc_day()
        stats = _error_stats(day)

        n = int(stats.get("n", 0) or 0)
        bad = int(stats.get("bad", 0) or 0)
        rate = float(stats.get("rate", 0.0) or 0.0)

        min_n = int(k.get("err_min_n") or 0)
        thr = float(k.get("err_rate_halt") or 0.0)

        if thr <= 0:
            return False, "disabled", {"day": day, **stats, "threshold": thr, "min_n": min_n}

        if n < min_n:
            return (
                False,
                "insufficient_samples",
                {"day": day, **stats, "threshold": thr, "min_n": min_n},
            )

        if rate >= thr:
            return (
                True,
                "error_rate_halt",
                {"day": day, "n": n, "bad": bad, "rate": rate, "threshold": thr, "min_n": min_n},
            )

        return (
            False,
            "ok",
            {"day": day, "n": n, "bad": bad, "rate": rate, "threshold": thr, "min_n": min_n},
        )
    except Exception:
        return False, "error_rate_check_failed", {"day": _utc_day(), "n": 0, "bad": 0, "rate": 0.0}


# -----------------------------------------------------------------------------
# PnL / streak tracking
# -----------------------------------------------------------------------------
def record_close(
    *,
    realized_pnl_sol: float | None = None,
    equity_sol: float | None = None,
    realized_pnl_usd: float | None = None,
    equity_usd: float | None = None,
    ts: float | None = None,
    **meta: Any,
) -> None:
    """
    Call after a position is CLOSED (realized PnL known).

    You may provide SOL or USD (or both). We store both channels independently.
    """
    try:
        day = _utc_day()
        t = float(ts or _now())
        r = _r()

        pnl_sol = float(realized_pnl_sol) if realized_pnl_sol is not None else None
        eq_sol = float(equity_sol) if equity_sol is not None else None
        pnl_usd = float(realized_pnl_usd) if realized_pnl_usd is not None else None
        eq_usd = float(equity_usd) if equity_usd is not None else None

        # streak basis prefers SOL pnl then USD pnl
        pnl_for_streak: float | None = pnl_sol if pnl_sol is not None else pnl_usd

        if r is not None:
            try:
                pipe = r.pipeline()

                if pnl_sol is not None:
                    pipe.hincrbyfloat(_k_pnl(day), "pnl_sol", float(pnl_sol))
                if eq_sol is not None:
                    pipe.hset(_k_pnl(day), "eq_sol", float(eq_sol))

                if pnl_usd is not None:
                    pipe.hincrbyfloat(_k_pnl(day), "pnl_usd", float(pnl_usd))
                if eq_usd is not None:
                    pipe.hset(_k_pnl(day), "eq_usd", float(eq_usd))

                pipe.hset(_k_pnl(day), mapping={"day": day, "t": str(t), "ts": str(_now())})
                pipe.expire(_k_pnl(day), _TTL_SEC)

                if pnl_for_streak is not None:
                    if float(pnl_for_streak) < 0:
                        pipe.hincrby(_k_streak(day), "losers", 1)
                        pipe.hset(_k_streak(day), "winners", 0)
                    elif float(pnl_for_streak) > 0:
                        pipe.hincrby(_k_streak(day), "winners", 1)
                        pipe.hset(_k_streak(day), "losers", 0)

                    pipe.hset(_k_streak(day), mapping={"day": day, "t": str(t), "ts": str(_now())})
                    pipe.expire(_k_streak(day), _TTL_SEC)

                pipe.execute()
            except Exception:
                st = _mem_get(day)
                if pnl_sol is not None:
                    st["pnl_sol"] = float(st.get("pnl_sol", 0.0) or 0.0) + float(pnl_sol)
                if eq_sol is not None:
                    st["eq_sol"] = float(eq_sol)

                if pnl_usd is not None:
                    st["pnl_usd"] = float(st.get("pnl_usd", 0.0) or 0.0) + float(pnl_usd)
                if eq_usd is not None:
                    st["eq_usd"] = float(eq_usd)

                if pnl_for_streak is not None:
                    if float(pnl_for_streak) < 0:
                        st["losers"] = int(st.get("losers", 0) or 0) + 1
                        st["winners"] = 0
                    elif float(pnl_for_streak) > 0:
                        st["winners"] = int(st.get("winners", 0) or 0) + 1
                        st["losers"] = 0

                st["ts"] = _now()
                _mem_set(day, st)
        else:
            st = _mem_get(day)
            if pnl_sol is not None:
                st["pnl_sol"] = float(st.get("pnl_sol", 0.0) or 0.0) + float(pnl_sol)
            if eq_sol is not None:
                st["eq_sol"] = float(eq_sol)

            if pnl_usd is not None:
                st["pnl_usd"] = float(st.get("pnl_usd", 0.0) or 0.0) + float(pnl_usd)
            if eq_usd is not None:
                st["eq_usd"] = float(eq_usd)

            if pnl_for_streak is not None:
                if float(pnl_for_streak) < 0:
                    st["losers"] = int(st.get("losers", 0) or 0) + 1
                    st["winners"] = 0
                elif float(pnl_for_streak) > 0:
                    st["winners"] = int(st.get("winners", 0) or 0) + 1
                    st["losers"] = 0

            st["ts"] = _now()
            _mem_set(day, st)

        halt, reason, _snap = should_halt()
        if halt and reason in {"daily_stop", "loss_streak", "error_rate_halt"}:
            trip_breaker(reason=reason)

        if pnl_for_streak is not None:
            log.every(
                "CB_CLOSE",
                10,
                pnl=float(pnl_for_streak),
                day=day,
                halt=int(bool(halt)),
                reason=str(reason)[:120],
                meta=str({k: meta.get(k) for k in list(meta)[:6]})[:220],
            )
    except Exception:
        return


def _pnl_stats(day: str) -> dict[str, Any]:
    r = _r()
    if r is not None:
        try:
            m = r.hgetall(_k_pnl(day)) or {}
            return {
                "pnl_sol": float(m.get("pnl_sol", 0.0) or 0.0),
                "eq_sol": float(m.get("eq_sol", 0.0) or 0.0),
                "pnl_usd": float(m.get("pnl_usd", 0.0) or 0.0),
                "eq_usd": float(m.get("eq_usd", 0.0) or 0.0),
            }
        except Exception:
            return {"pnl_sol": 0.0, "eq_sol": 0.0, "pnl_usd": 0.0, "eq_usd": 0.0}

    st = _mem_get(day)
    return {
        "pnl_sol": float(st.get("pnl_sol", 0.0) or 0.0),
        "eq_sol": float(st.get("eq_sol", 0.0) or 0.0),
        "pnl_usd": float(st.get("pnl_usd", 0.0) or 0.0),
        "eq_usd": float(st.get("eq_usd", 0.0) or 0.0),
    }


def _streak_stats(day: str) -> dict[str, Any]:
    r = _r()
    if r is not None:
        try:
            m = r.hgetall(_k_streak(day)) or {}
            return {
                "losers": int(float(m.get("losers", 0) or 0)),
                "winners": int(float(m.get("winners", 0) or 0)),
            }
        except Exception:
            return {"losers": 0, "winners": 0}

    st = _mem_get(day)
    return {"losers": int(st.get("losers", 0) or 0), "winners": int(st.get("winners", 0) or 0)}


# -----------------------------------------------------------------------------
# Breaker / cooldown (guardrails preferred)
# -----------------------------------------------------------------------------
def breaker_active() -> bool:
    """True if breaker cooldown is active (guardrails breaker counts too, if available)."""
    try:
        if _guard is not None:
            try:
                fn = getattr(_guard, "breaker_active", None)
                if callable(fn) and bool(fn()):  # type: ignore[misc]
                    return True
            except Exception:
                pass

            try:
                if hasattr(_guard, "buy_block_reason"):
                    rsn = str(_guard.buy_block_reason() or "")
                    if "breaker" in rsn or rsn == "breaker_cooldown":
                        return True
            except Exception:
                pass

        day = _utc_day()
        r = _r()
        if r is not None:
            try:
                until = float(r.hget(_k_breaker(day), "until") or 0.0)
                return until > _now()
            except Exception:
                return False

        st = _mem_get(day)
        return float(st.get("breaker_until", 0.0) or 0.0) > _now()
    except Exception:
        return False


def trip_breaker(
    seconds: float | None = None, minutes: int | None = None, reason: str = "breaker"
) -> None:
    """
    Trip a breaker cooldown.

    If guardrails is available, use guardrails.trip_breaker so all gating is centralized.
    Otherwise store a local breaker-until timestamp.
    """
    try:
        k = _knobs()
        if not bool(k.get("breaker_enabled", True)):
            return

        if minutes is not None:
            sec = float(max(0, int(minutes))) * 60.0
        elif seconds is not None:
            sec = float(max(0.0, float(seconds)))
        else:
            sec = float(max(0, int(k.get("breaker_minutes") or 0))) * 60.0

        if sec <= 0:
            sec = 45.0

        rsn = (reason or "breaker").strip()[:120]

        if _guard is not None:
            try:
                fn = getattr(_guard, "trip_breaker", None)
                if callable(fn):
                    fn(seconds=sec, reason=f"cb:{rsn}")  # type: ignore[misc]
                    log.every("CB_TRIP", 10, via="guardrails", sec=float(sec), reason=rsn)
                    return
            except Exception:
                pass

        day = _utc_day()
        until = _now() + sec
        r = _r()
        if r is not None:
            try:
                key = _k_breaker(day)
                pipe = r.pipeline()
                pipe.hset(
                    key, mapping={"day": day, "until": str(until), "reason": rsn, "ts": str(_now())}
                )
                pipe.expire(key, _TTL_SEC)
                pipe.execute()
                log.every("CB_TRIP", 10, via="local", sec=float(sec), reason=rsn)
                return
            except Exception:
                pass

        st = _mem_get(day)
        st["breaker_until"] = max(float(st.get("breaker_until", 0.0) or 0.0), until)
        st["last_reason"] = rsn
        st["ts"] = _now()
        _mem_set(day, st)
        log.every("CB_TRIP", 10, via="mem", sec=float(sec), reason=rsn)
    except Exception:
        return


# -----------------------------------------------------------------------------
# Gating: should_halt / trading_allowed
# -----------------------------------------------------------------------------
def _daily_stop_hit() -> tuple[bool, str, dict[str, Any]]:
    """
    Daily stop checks realized PnL vs equity snapshot, in SOL or USD.
    """
    try:
        k = _knobs()
        frac = float(k.get("daily_stop_frac") or 0.0)
        if frac <= 0:
            return False, "disabled", {}

        day = _utc_day()
        pnl = _pnl_stats(day)

        eq_sol = float(pnl.get("eq_sol", 0.0) or 0.0)
        pnl_sol = float(pnl.get("pnl_sol", 0.0) or 0.0)
        if eq_sol > 0:
            hit = pnl_sol <= (-abs(frac) * eq_sol)
            return (
                True if hit else False,
                "daily_stop",
                {
                    "day": day,
                    "channel": "sol",
                    "pnl": pnl_sol,
                    "eq": eq_sol,
                    "frac": frac,
                },
            )

        eq_usd = float(pnl.get("eq_usd", 0.0) or 0.0)
        pnl_usd = float(pnl.get("pnl_usd", 0.0) or 0.0)
        if eq_usd > 0:
            hit = pnl_usd <= (-abs(frac) * eq_usd)
            return (
                True if hit else False,
                "daily_stop",
                {
                    "day": day,
                    "channel": "usd",
                    "pnl": pnl_usd,
                    "eq": eq_usd,
                    "frac": frac,
                },
            )

        return False, "no_equity_snapshot", {"day": day, **pnl, "frac": frac}
    except Exception:
        return False, "daily_stop_check_failed", {}


def _loss_streak_hit() -> tuple[bool, str, dict[str, Any]]:
    try:
        k = _knobs()
        mx = int(k.get("max_loss_streak") or 0)
        if mx <= 0:
            return False, "disabled", {}

        day = _utc_day()
        st = _streak_stats(day)
        losers = int(st.get("losers", 0) or 0)

        if losers >= mx:
            return True, "loss_streak", {"day": day, "losers": losers, "limit": mx}
        return False, "ok", {"day": day, "losers": losers, "limit": mx}
    except Exception:
        return False, "loss_streak_check_failed", {}


def should_halt() -> tuple[bool, str, dict[str, Any]]:
    """
    Returns (halt, reason, snapshot).

    Notes:
    - If an error-rate breach is detected, we trip breaker cooldown.
    - If guardrails blocks buys, we respect it and return a halt reason (canonical gate).
    """
    try:
        k = _knobs()

        pause_key = str(k.get("pause_key") or "PAUSE_TRADING")
        if _envb(pause_key, False) or _file_exists(str(k.get("pause_file") or "")):
            return True, "paused", snapshot()

        if (
            _envb("KILL_SWITCH", False)
            or _envb("CB_KILL_SWITCH", False)
            or _file_exists(str(k.get("kill_file") or ""))
        ):
            return True, "kill_switch", snapshot()

        if (
            bool(k.get("degraded_blocks", True))
            and is_degraded()
            and (not _envb("CB_ALLOW_WHEN_DEGRADED", False))
        ):
            return True, "degraded_mode", snapshot()

        if _guard is not None:
            try:
                fn = getattr(_guard, "buys_allowed", None)
                if callable(fn) and (not bool(fn())):  # type: ignore[misc]
                    rsn_fn = getattr(_guard, "buy_block_reason", None)
                    rsn = str(rsn_fn() or "blocked") if callable(rsn_fn) else "blocked"
                    return True, f"guardrails:{rsn}", snapshot()
            except Exception:
                pass

        if breaker_active():
            return True, "breaker_cooldown", snapshot()

        hit, _, info = _daily_stop_hit()
        if hit:
            trip_breaker(reason="daily_stop")
            return True, "daily_stop", {**snapshot(), "daily_stop": info}

        hit2, _, info2 = _loss_streak_hit()
        if hit2:
            trip_breaker(reason="loss_streak")
            return True, "loss_streak", {**snapshot(), "loss_streak": info2}

        breached, rsn, estats = error_rate_breached()
        if breached:
            cd = float(k.get("err_trip_cooldown_sec") or 45.0)
            trip_breaker(seconds=cd, reason="error_rate_halt")
            return True, rsn, {**snapshot(), "errors": estats, "cooldown_sec": cd}

        return False, "ok", snapshot()

    except Exception:
        if bool(_knobs().get("fail_closed")):
            return True, "fail_closed", snapshot()
        return False, "fail_open", snapshot()


def trading_allowed() -> tuple[bool, str, dict[str, Any]]:
    """
    Convenience inverse of should_halt(): returns (allowed, reason, snapshot).
    """
    halt, reason, snap = should_halt()
    return (not halt), str(reason), snap


# -----------------------------------------------------------------------------
# Snapshot / status
# -----------------------------------------------------------------------------
def snapshot() -> dict[str, Any]:
    try:
        day = _utc_day()
        errs = _error_stats(day)
        pnl = _pnl_stats(day)
        streak = _streak_stats(day)

        br: dict[str, Any] = {"active": int(bool(breaker_active()))}
        r = _r()
        if r is not None:
            try:
                m = r.hgetall(_k_breaker(day)) or {}
                br["until"] = float(m.get("until", 0.0) or 0.0)
                br["reason"] = str(m.get("reason", "") or "")
            except Exception:
                pass
        else:
            st = _mem_get(day)
            br["until"] = float(st.get("breaker_until", 0.0) or 0.0)
            br["reason"] = str(st.get("last_reason", "") or "")

        k = _knobs()

        return {
            "day": day,
            "namespace": _NAMESPACE,
            "redis": int(bool(_r() is not None)),
            "degraded": int(bool(is_degraded())),
            "errors": errs,
            "pnl": pnl,
            "streak": streak,
            "breaker": br,
            "limits": {
                # NOTE: stored as fraction; name kept for compatibility
                "daily_stop_pct": float(k.get("daily_stop_frac") or 0.0),
                "max_loss_streak": int(k.get("max_loss_streak") or 0),
                "error_rate_halt": float(k.get("err_rate_halt") or 0.0),
                "error_min_n": int(k.get("err_min_n") or 0),
                "error_cooldown_sec": float(k.get("err_trip_cooldown_sec") or 0.0),
                "breaker_minutes": int(k.get("breaker_minutes") or 0),
                "breaker_enabled": int(bool(k.get("breaker_enabled", True))),
                "pause_key": str(k.get("pause_key") or "PAUSE_TRADING"),
                "fail_closed": int(bool(k.get("fail_closed"))),
            },
        }
    except Exception:
        return {"error": "snapshot_failed", "namespace": _NAMESPACE, "day": _utc_day()}


def status() -> dict[str, Any]:
    return snapshot()


__all__ = [
    "breaker_active",
    "error_rate_breached",
    "is_degraded",
    "record_close",
    "record_error",
    "set_degraded",
    "should_halt",
    "snapshot",
    "status",
    "trading_allowed",
    "trip_breaker",
]
