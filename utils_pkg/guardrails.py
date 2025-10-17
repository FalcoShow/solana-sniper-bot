# utils_pkg/guardrails.py
from __future__ import annotations

import json
import os
import threading
import time
from typing import Optional, Dict, Any

# Optional Redis
try:
    import redis  # type: ignore
except Exception:  # pragma: no cover
    redis = None  # type: ignore

# ──────────────────────────────────────────────────────────────────────────────
# Env helpers
# ──────────────────────────────────────────────────────────────────────────────
def _envb(val: Optional[str], default: bool = False) -> bool:
    if val is None:
        return default
    return str(val).strip().lower() in {"1", "true", "yes", "y", "on"}

def _getb(k: str, default: bool = False) -> bool:
    return _envb(os.getenv(k), default)

def _getf(k: str, d: float) -> float:
    try:
        return float(os.getenv(k, str(d)))
    except Exception:
        return float(d)

def _geti(k: str, d: int) -> int:
    try:
        return int(os.getenv(k, str(d)))
    except Exception:
        return int(d)

# ──────────────────────────────────────────────────────────────────────────────
# Config (all optional; defaults disable limits)
# ──────────────────────────────────────────────────────────────────────────────
PAUSE_SWITCH         = "PAUSE_TRADING"

MAX_LOSSES           = _geti("PF_DAILY_MAX_LOSSES", _geti("DAILY_MAX_LOSSES", 0))
MAX_DD_PCT_FRACTION  = _getf("PF_DAILY_MAX_DRAWDOWN_PCT", _getf("DAILY_MAX_DRAWDOWN_PCT", 0.0)) / 100.0
MAX_BUYS             = _geti("PF_DAILY_MAX_BUYS", 0)                # 0 => unlimited
MAX_SPEND_SOL        = _getf("PF_DAILY_MAX_SPEND_SOL", 0.0)         # 0 => unlimited

# Reference values for drawdown → USD estimation
EQUITY_USD           = _getf("EQUITY_USD_FALLBACK", 1500.0)
SOL_USD              = _getf("SOL_PRICE_USD_FALLBACK", 150.0)

# Redis + namespacing
_REDIS_URL = os.getenv("REDIS_URL") or os.getenv("UPSTASH_REDIS_URL") or ""
_R: Optional["redis.Redis"] = None
if redis and _REDIS_URL:
    try:
        _R = redis.from_url(
            _REDIS_URL,
            socket_timeout=float(os.getenv("PF_REDIS_SOCKET_TIMEOUT_S", "1.2")),
            socket_connect_timeout=float(os.getenv("PF_REDIS_CONNECT_TIMEOUT_S", "1.2")),
            retry_on_timeout=True,
        )
    except Exception:
        _R = None

_PREFIX = (os.getenv("PF_GUARD_PREFIX") or "gr").strip().lower()
_NAMESPACE = (
    os.getenv("PF_GUARD_NS")
    or os.getenv("RENDER_SERVICE_NAME")
    or os.getenv("FLY_APP_NAME")
    or os.getenv("DYNO")
    or os.getenv("HOSTNAME")
    or "default"
).strip().lower()

def _k(*parts: str) -> str:
    return ":".join([_PREFIX, _namespace_day(), *parts])

# keep day (UTC) in key path to avoid cross-day race conditions
def _utc_day() -> str:
    return time.strftime("%Y-%m-%d", time.gmtime())

def _namespace_day() -> str:
    return f"{_NAMESPACE}:{_utc_day()}"

# How long to retain daily state (days → seconds)
_STATE_TTL_SEC = int(4 * 24 * 3600)

# ──────────────────────────────────────────────────────────────────────────────
# In-process fallback state
# ──────────────────────────────────────────────────────────────────────────────
_mem_lock = threading.Lock()
_mem_state: Dict[str, Dict[str, Any]] = {}  # keyed by day string

def _default_state(day: str) -> Dict[str, Any]:
    return {
        "day": day,                 # YYYY-MM-DD (UTC)
        "losses": 0,                # count of losing closes
        "realized_sol": 0.0,        # cumulative realized P&L in SOL (can be +/-)
        "buys": 0,                  # count of buy opens today
        "spent_sol": 0.0,           # sum of notional SOL opened today (best-effort)
        "ts": time.time(),          # last update timestamp
    }

def _load_state() -> Dict[str, Any]:
    day = _utc_day()
    if _R is not None:
        try:
            raw = _R.get(_k("state"))
            if raw:
                st = json.loads(raw)
                # sanity guard
                if st.get("day") == day:
                    return st
        except Exception:
            pass
        # initialize if missing or stale
        st = _default_state(day)
        try:
            _R.set(_k("state"), json.dumps(st), ex=_STATE_TTL_SEC)
        except Exception:
            pass
        return st

    # memory fallback
    with _mem_lock:
        st = _mem_state.get(day)
        if st is None:
            st = _default_state(day)
            _mem_state.clear()  # purge old day(s)
            _mem_state[day] = st
        return dict(st)

def _save_state(st: Dict[str, Any]) -> None:
    st = dict(st)
    st["ts"] = time.time()
    if _R is not None:
        try:
            _R.set(_k("state"), json.dumps(st), ex=_STATE_TTL_SEC)
            return
        except Exception:
            pass
    with _mem_lock:
        _mem_state[st["day"]] = dict(st)

# ──────────────────────────────────────────────────────────────────────────────
# Public gates & hooks
# ──────────────────────────────────────────────────────────────────────────────
def buys_allowed() -> bool:
    """
    Returns False if:
      • operator pause switch is on (PAUSE_TRADING),
      • daily losses >= MAX_LOSSES (if set),
      • daily drawdown in USD >= EQUITY_USD * MAX_DD_PCT_FRACTION (if set),
      • daily buys >= MAX_BUYS (if set),
      • daily spend (SOL) >= MAX_SPEND_SOL (if set).
    All limits are disabled when set to 0/0.0.
    """
    # operator kill-switch (checked live)
    if _getb(PAUSE_SWITCH, False):
        return False

    st = _load_state()

    # losses gate
    if MAX_LOSSES > 0 and int(st.get("losses", 0)) >= MAX_LOSSES:
        return False

    # drawdown gate (use only *negative* realized, converted to USD)
    realized_sol = float(st.get("realized_sol", 0.0))
    dd_sol = -min(0.0, realized_sol)  # e.g., -1.2 SOL → dd_sol = 1.2
    if MAX_DD_PCT_FRACTION > 0:
        dd_usd = dd_sol * float(SOL_USD)
        if dd_usd >= (float(EQUITY_USD) * float(MAX_DD_PCT_FRACTION)):
            return False

    # optional daily buy-count cap
    if MAX_BUYS > 0 and int(st.get("buys", 0)) >= MAX_BUYS:
        return False

    # optional daily spend cap (SOL notional)
    if MAX_SPEND_SOL > 0 and float(st.get("spent_sol", 0.0)) >= MAX_SPEND_SOL:
        return False

    return True

def record_trade_open(notional_sol: float) -> None:
    """
    Optional: call when a BUY is accepted to track daily buy count and SOL notional.
    Your code calls this when available; otherwise it's a no-op.
    """
    try:
        st = _load_state()
        st["buys"] = int(st.get("buys", 0)) + 1
        if notional_sol:
            st["spent_sol"] = float(st.get("spent_sol", 0.0)) + float(notional_sol)
        _save_state(st)
    except Exception:
        # Guardrails must never crash the app
        pass

def note_close(realized_pnl_sol: float) -> None:
    """
    Call after a position is closed to record realized P&L and loss count (if negative).
    """
    try:
        st = _load_state()
        pnl = float(realized_pnl_sol or 0.0)
        st["realized_sol"] = float(st.get("realized_sol", 0.0)) + pnl
        if pnl < 0:
            st["losses"] = int(st.get("losses", 0)) + 1
        _save_state(st)
    except Exception:
        pass

def record_buy_result(ok: bool, reason: str, sig: Optional[str] = None) -> None:
    """
    Lightweight audit log for buy attempts. If Redis is present, keeps a rolling list.
    """
    if _R is None:
        return
    try:
        entry = {"ts": time.time(), "ok": bool(ok), "reason": str(reason), "sig": sig}
        _R.lpush(_k("buylog"), json.dumps(entry))
        _R.ltrim(_k("buylog"), 0, 500)
    except Exception:
        pass

__all__ = ["buys_allowed", "record_buy_result", "note_close", "record_trade_open"]
