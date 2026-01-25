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
  • Optional local breaker cooldown window ("cooldown" halt)

IMPORTANT DEDUP RULE
--------------------
`utils_pkg/guardrails.py` is the canonical "are buys allowed right now?"
This module MUST NOT fork competing logic. It should:

  - Evaluate *panic* conditions that warrant halting trading
  - Trip guardrails breaker (preferred) so all processes observe the halt
  - Provide a stable gating function to be used in entry loop

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

Design goals
------------
- Never raise exceptions to caller (hard safety)
- No network I/O at import time
- Redis optional; in-memory fallback works
- Namespaced keys to avoid cross-bot collisions
- Backwards compatible env parsing
"""

import datetime as dt
import os
import threading
import time
from typing import Any

# ---------------------------------------------------------------------
# Optional deps (best-effort only)
# ---------------------------------------------------------------------
try:  # pragma: no cover
    import redis  # type: ignore
except Exception:  # pragma: no cover
    redis = None  # type: ignore

# Prefer guardrails breaker for cross-process halting
try:  # pragma: no cover
    from utils_pkg import guardrails as _guard  # type: ignore
except Exception:  # pragma: no cover
    _guard = None  # type: ignore

# Optional logger (best-effort)
try:  # pragma: no cover
    from logging_setup import get_logger as _get_logger  # type: ignore
except Exception:  # pragma: no cover
    _get_logger = None  # type: ignore


def _log() -> Any:
    lg = None
    if callable(_get_logger):
        try:
            lg = _get_logger("sniper.circuit_breakers")
        except Exception:
            try:
                lg = _get_logger()
            except Exception:
                lg = None

    if lg is not None:
        if not hasattr(lg, "event"):

            def _event(name: str, **fields: Any) -> None:
                try:
                    lg.info("%s %s", name, fields)
                except Exception:
                    return None

            lg.event = _event

        if not hasattr(lg, "every"):

            def _every(key: str, n: int = 30, msg: str | None = None, **fields: Any) -> None:
                try:
                    now = time.monotonic()
                    store = getattr(_every, "_store", {})  # type: ignore[attr-defined]
                    last = float(store.get(key, 0.0))
                    if now - last >= float(n):
                        store[key] = now
                        _every._store = store  # type: ignore[attr-defined]
                        lg.event(msg or key, **fields)  # type: ignore[misc]
                except Exception:
                    return None

            lg.every = _every

        return lg

    class _Null:
        def info(self, *a: Any, **k: Any) -> None:
            return None

        def warning(self, *a: Any, **k: Any) -> None:
            return None

        def error(self, *a: Any, **k: Any) -> None:
            return None

        def event(self, *a: Any, **k: Any) -> None:
            return None

        def every(self, *a: Any, **k: Any) -> None:
            return None

    return _Null()


log = _log()

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


def _utc_day(ts: float | None = None) -> str:
    return dt.datetime.utcfromtimestamp(ts or _now()).strftime("%Y-%m-%d")


def _pct_to_frac(x: float) -> float:
    # accepts 0.08 or 8
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


# ---------------------------------------------------------------------
# Namespacing / keys
# ---------------------------------------------------------------------
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

_TTL_SEC = _envi("CB_STATE_TTL_SEC", 4 * 24 * 3600)

# A shared degraded-mode key (also used by guardrails, if present)
_DEGRADED_KEY = f"{_PREFIX}:{_NAMESPACE}:degraded"


def _k_err(day: str) -> str:
    return f"{_PREFIX}:{_NAMESPACE}:{day}:errs"


def _k_pnl(day: str) -> str:
    return f"{_PREFIX}:{_NAMESPACE}:{day}:pnl"


def _k_streak(day: str) -> str:
    return f"{_PREFIX}:{_NAMESPACE}:{day}:streak"


def _k_breaker(day: str) -> str:
    return f"{_PREFIX}:{_NAMESPACE}:{day}:breaker"


# ---------------------------------------------------------------------
# Redis (lazy, cached, never throws)
# ---------------------------------------------------------------------
_REDIS_URL_KEYS = (
    "REDIS_URL",
    "UPSTASH_REDIS_URL",
    "PF_REDIS_URL",
    "CB_REDIS_URL",
    "GUARD_REDIS_URL",
)
_R: Any | None = None
_R_AT: float = 0.0
_R_REFRESH = max(1.0, float(_envf("CB_REDIS_REFRESH_SEC", 5.0)))


def _redis_url() -> str:
    for k in _REDIS_URL_KEYS:
        v = os.getenv(k)
        if v:
            return str(v).strip()
    return ""


def _redis_kwargs(url: str) -> dict[str, Any]:
    # keep minimal + safe timeouts
    return dict(
        decode_responses=True,
        socket_timeout=float(_envf("CB_REDIS_SOCKET_TIMEOUT_S", 1.2)),
        socket_connect_timeout=float(_envf("CB_REDIS_CONNECT_TIMEOUT_S", 1.2)),
        retry_on_timeout=True,
        # ssl is automatically inferred by redis-py from rediss://
        ssl=url.startswith("rediss://"),
    )


def _r() -> Any | None:
    global _R, _R_AT
    if redis is None:
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
        _R = redis.from_url(url, **_redis_kwargs(url))  # type: ignore[attr-defined]
    except Exception:
        _R = None
    return _R


# ---------------------------------------------------------------------
# In-memory fallback (when Redis missing)
# ---------------------------------------------------------------------
_mem_lock = threading.Lock()
_mem: dict[str, dict[str, Any]] = {}


def _mem_get(day: str) -> dict[str, Any]:
    with _mem_lock:
        st = _mem.get(day)
        if st is None:
            st = {
                "day": day,
                "errs_n": 0,
                "errs_bad": 0,
                "pnl_sol": 0.0,
                "eq_sol": 0.0,
                "pnl_usd": 0.0,
                "eq_usd": 0.0,
                "losers": 0,
                "winners": 0,
                "breaker_until": 0.0,
                "last_reason": "",
                "ts": _now(),
            }
            _mem[day] = st
        return dict(st)


def _mem_set(day: str, st: dict[str, Any]) -> None:
    with _mem_lock:
        _mem[day] = dict(st)


# ---------------------------------------------------------------------
# Knobs (env-first, backwards compatible)
# ---------------------------------------------------------------------
_KNOBS_TTL = max(0.5, float(_envf("CB_KNOBS_REFRESH_SEC", 2.0)))
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
            "CB_DAILY_STOP_PCT", _envf("DAILY_STOP_PCT", _envf("RISK_MAX_DAILY_DRAWDOWN_PCT", 0.0))
        )
    )
    max_loss_streak = _envi(
        "CB_MAX_CONSECUTIVE_LOSSES",
        _envi("MAX_CONSECUTIVE_LOSSES", _envi("GUARD_MAX_LOSS_STREAK", 0)),
    )
    err_rate_halt = _pct_to_frac(_envf("CB_ERROR_RATE_HALT", _envf("ERROR_RATE_HALT", 0.0)))
    err_min_n = _envi("CB_ERROR_MIN_N", _envi("ERROR_MIN_N", 20))

    # Optional: when err rate breached, trip a breaker cooldown
    err_trip_cooldown_sec = _envf("CB_ERROR_COOLDOWN_SEC", 45.0)

    # Local breaker support (if guardrails absent)
    breaker_minutes = _envi("CB_BREAKER_MINUTES", _envi("RISK_CIRCUIT_BREAKER_MINUTES", 0))
    breaker_enabled = _envb("CB_BREAKER_ENABLED", True)

    # Kill/pause switches (live)
    pause_key = _envs("CB_PAUSE_SWITCH", _envs("PAUSE_SWITCH", "PAUSE_TRADING"))
    kill_file = _envs("CB_KILL_SWITCH_FILE", _envs("KILL_SWITCH_FILE", ""))
    pause_file = _envs("CB_PAUSE_FILE", _envs("PAUSE_TRADING_FILE", _envs("PAUSE_FILE", "")))

    # Degraded gate
    degraded_blocks = _envb("CB_DEGRADED_BLOCKS_TRADING", True)

    # Fail-open vs fail-closed
    fail_closed = _envb("CB_FAIL_CLOSED", False) or (
        _envs("CB_FAIL_MODE", "").lower() in {"closed", "fail_closed"}
    )

    _knobs_cache = {
        "daily_stop_frac": float(_clamp(daily_stop_frac, 0.0, 1.0)),
        "max_loss_streak": int(max(0, max_loss_streak)),
        "err_rate_halt": float(_clamp(err_rate_halt, 0.0, 1.0)),
        "err_min_n": int(max(0, err_min_n)),
        "err_trip_cooldown_sec": float(max(0.0, err_trip_cooldown_sec)),
        "breaker_minutes": int(max(0, breaker_minutes)),
        "breaker_enabled": bool(breaker_enabled),
        "pause_key": pause_key,
        "kill_file": kill_file,
        "pause_file": pause_file,
        "degraded_blocks": bool(degraded_blocks),
        "fail_closed": bool(fail_closed),
    }
    _knobs_at = now
    return _knobs_cache


def _file_exists(path: str) -> bool:
    if not path:
        return False
    try:
        return bool(os.path.exists(path))
    except Exception:
        return False


# ---------------------------------------------------------------------
# Degraded mode (shared key)
# ---------------------------------------------------------------------
def set_degraded(on: bool, reason: str = "degraded", ttl_sec: int = 120) -> None:
    """Set a shared degraded-mode flag (cross-process if Redis exists)."""
    try:
        r = _r()
        val = "1" if on else "0"
        if r is not None:
            r.set(_DEGRADED_KEY, val, ex=max(5, int(ttl_sec)))
        else:
            # in-memory fallback: store under special day key
            day = "_degraded"
            st = _mem_get(day)
            st["on"] = bool(on)
            st["reason"] = str(reason)[:120]
            st["until"] = _now() + max(0, int(ttl_sec))
            _mem_set(day, st)
        log.every("CB_DEGRADED", 10, on=int(bool(on)), reason=str(reason)[:120], ttl=int(ttl_sec))
    except Exception:
        pass


def is_degraded() -> bool:
    try:
        # env override always wins
        if _envb("DEGRADED_MODE", False) or _envb("SAFE_MODE", False):
            return True

        r = _r()
        if r is not None:
            v = r.get(_DEGRADED_KEY)
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


# ---------------------------------------------------------------------
# Error tracking
# ---------------------------------------------------------------------
def record_error(ok: bool, kind: str = "router", where: str = "", **meta: Any) -> None:
    """
    Record any health/error event.
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
        if r is not None:
            key = _k_err(day)
            pipe = r.pipeline()
            pipe.hincrby(key, "n", 1)
            if not ok:
                pipe.hincrby(key, "bad", 1)
            # Optional: per-kind counters
            k2 = f"{kind}".strip()[:48] or "router"
            pipe.hincrby(key, f"n:{k2}", 1)
            if not ok:
                pipe.hincrby(key, f"bad:{k2}", 1)
            pipe.hset(key, mapping={"day": day, "ts": str(_now())})
            pipe.expire(key, _TTL_SEC)
            pipe.execute()
        else:
            st = _mem_get(day)
            st["errs_n"] = int(st.get("errs_n", 0)) + 1
            if not ok:
                st["errs_bad"] = int(st.get("errs_bad", 0)) + 1
            st["ts"] = _now()
            _mem_set(day, st)

        if not ok:
            log.every(
                "CB_ERR",
                6,
                kind=str(kind)[:48],
                where=str(where)[:64],
                meta=str({k: meta.get(k) for k in list(meta)[:6]})[:220],
            )
    except Exception:
        pass


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
        int(stats.get("bad", 0) or 0)
        rate = float(stats.get("rate", 0.0) or 0.0)

        if n < int(k.get("err_min_n") or 0):
            return False, "insufficient_samples", {"day": day, **stats}

        thr = float(k.get("err_rate_halt") or 0.0)
        if thr <= 0:
            return False, "disabled", {"day": day, **stats}

        if rate >= thr:
            return True, "error_rate_halt", {"day": day, **stats, "threshold": thr}
        return False, "ok", {"day": day, **stats, "threshold": thr}
    except Exception:
        return False, "error_rate_check_failed", {"day": _utc_day(), "n": 0, "bad": 0, "rate": 0.0}


# ---------------------------------------------------------------------
# PnL / streak tracking
# ---------------------------------------------------------------------
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

        # compute win/loss for streak logic:
        pnl_for_streak = None
        if pnl_sol is not None:
            pnl_for_streak = pnl_sol
        elif pnl_usd is not None:
            pnl_for_streak = pnl_usd

        if r is not None:
            pipe = r.pipeline()

            # pnl accumulators
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

            # streaks (consecutive winners/losers since last flip)
            if pnl_for_streak is not None:
                if float(pnl_for_streak) < 0:
                    pipe.hincrby(_k_streak(day), "losers", 1)
                    pipe.hset(_k_streak(day), "winners", 0)
                elif float(pnl_for_streak) > 0:
                    pipe.hincrby(_k_streak(day), "winners", 1)
                    pipe.hset(_k_streak(day), "losers", 0)
                else:
                    # flat: do nothing
                    pass
                pipe.hset(_k_streak(day), mapping={"day": day, "t": str(t), "ts": str(_now())})
                pipe.expire(_k_streak(day), _TTL_SEC)

            pipe.execute()
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

        # If this close worsens state into a halt condition, we can proactively trip breaker
        halt, reason, snap = should_halt()
        if halt and reason in {"daily_stop", "loss_streak", "error_rate_halt"}:
            # Trip guardrails breaker (preferred), or local breaker fallback
            trip_breaker(reason=reason)

        if pnl_for_streak is not None:
            log.every(
                "CB_CLOSE",
                10,
                pnl=float(pnl_for_streak),
                day=day,
                reason=reason if halt else "ok",
                meta=str({k: meta.get(k) for k in list(meta)[:6]})[:220],
            )
    except Exception:
        pass


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


# ---------------------------------------------------------------------
# Breaker / cooldown (guardrails preferred)
# ---------------------------------------------------------------------
def breaker_active() -> bool:
    """True if circuit breaker cooldown is active (guardrails breaker counts too, if available)."""
    try:
        # Prefer guardrails (canonical for buys)
        if (
            _guard is not None
            and hasattr(_guard, "buy_block_reason")
            and hasattr(_guard, "buys_allowed")
        ):
            try:
                # If guardrails says blocked due to breaker, treat as active.
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
            # fallback default for “panic stop”
            sec = 45.0

        rsn = (reason or "breaker").strip()[:120]

        # Prefer guardrails breaker
        if _guard is not None and hasattr(_guard, "trip_breaker"):
            try:
                _guard.trip_breaker(seconds=sec, reason=f"cb:{rsn}")  # type: ignore[attr-defined]
                log.every("CB_TRIP", 10, via="guardrails", sec=float(sec), reason=rsn)
                return
            except Exception:
                pass

        # Local breaker fallback
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
        pass


# ---------------------------------------------------------------------
# Gating: should_halt / trading_allowed
# ---------------------------------------------------------------------
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

        # Prefer SOL channel if equity known, else USD
        eq_sol = float(pnl.get("eq_sol", 0.0) or 0.0)
        pnl_sol = float(pnl.get("pnl_sol", 0.0) or 0.0)
        if eq_sol > 0:
            hit = pnl_sol <= -abs(frac) * eq_sol
            return (
                bool(hit),
                "daily_stop",
                {"day": day, "channel": "sol", "pnl": pnl_sol, "eq": eq_sol, "frac": frac},
            )

        eq_usd = float(pnl.get("eq_usd", 0.0) or 0.0)
        pnl_usd = float(pnl.get("pnl_usd", 0.0) or 0.0)
        if eq_usd > 0:
            hit = pnl_usd <= -abs(frac) * eq_usd
            return (
                bool(hit),
                "daily_stop",
                {"day": day, "channel": "usd", "pnl": pnl_usd, "eq": eq_usd, "frac": frac},
            )

        # If no equity snapshot, cannot enforce daily stop here (guardrails may still enforce its own drawdown gate)
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
    If an error-rate breach is detected, we may trip a breaker cooldown.
    """
    try:
        k = _knobs()

        # Operator pause/kill switches (live)
        pause_key = str(k.get("pause_key") or "PAUSE_TRADING")
        if _envb(pause_key, False) or _file_exists(str(k.get("pause_file") or "")):
            return True, "paused", snapshot()

        if (
            _envb("KILL_SWITCH", False)
            or _file_exists(str(k.get("kill_file") or ""))
            or _envb("CB_KILL_SWITCH", False)
        ):
            return True, "kill_switch", snapshot()

        # Degraded mode (shared)
        if (
            bool(k.get("degraded_blocks", True))
            and is_degraded()
            and (not _envb("CB_ALLOW_WHEN_DEGRADED", False))
        ):
            return True, "degraded_mode", snapshot()

        # If guardrails blocks buys, treat as a halt for entry (non-competing, just respecting canonical gate)
        if (
            _guard is not None
            and hasattr(_guard, "buys_allowed")
            and hasattr(_guard, "buy_block_reason")
        ):
            try:
                if not bool(_guard.buys_allowed()):  # type: ignore[attr-defined]
                    rsn = str(_guard.buy_block_reason() or "blocked")  # type: ignore[attr-defined]
                    return True, f"guardrails:{rsn}", snapshot()
            except Exception:
                pass

        # Breaker cooldown
        if breaker_active():
            return True, "breaker_cooldown", snapshot()

        # Daily stop (panic stop)
        hit, _, info = _daily_stop_hit()
        if hit:
            # trip breaker so it propagates across processes
            trip_breaker(reason="daily_stop")
            return True, "daily_stop", {**snapshot(), "daily_stop": info}

        # Loss streak (panic stop)
        hit2, _, info2 = _loss_streak_hit()
        if hit2:
            trip_breaker(reason="loss_streak")
            return True, "loss_streak", {**snapshot(), "loss_streak": info2}

        # Error rate meltdown (panic stop)
        breached, rsn, estats = error_rate_breached()
        if breached:
            # trip breaker to cool off + stop bleeding
            cd = float(k.get("err_trip_cooldown_sec") or 45.0)
            trip_breaker(seconds=cd, reason="error_rate_halt")
            return True, rsn, {**snapshot(), "errors": estats, "cooldown_sec": cd}

        return False, "ok", snapshot()

    except Exception:
        # Fail mode
        if _knobs().get("fail_closed"):
            return True, "fail_closed", snapshot()
        return False, "fail_open", snapshot()


def trading_allowed() -> tuple[bool, str, dict[str, Any]]:
    """
    Convenience inverse of should_halt(): returns (allowed, reason, snapshot).
    """
    halt, reason, snap = should_halt()
    return (not halt), (reason if not halt else reason), snap


# ---------------------------------------------------------------------
# Snapshot / status
# ---------------------------------------------------------------------
def snapshot() -> dict[str, Any]:
    try:
        day = _utc_day()
        errs = _error_stats(day)
        pnl = _pnl_stats(day)
        streak = _streak_stats(day)

        # breaker snapshot
        br = {"active": int(bool(breaker_active()))}
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
