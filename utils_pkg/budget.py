# utils_pkg/budget.py
from __future__ import annotations

import json
import os
import threading
import time
from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple

try:
    import redis  # type: ignore
except Exception:  # redis is optional
    redis = None  # type: ignore

from logging_setup import get_logger

log = get_logger()

# ──────────────────────────────────────────────────────────────────────────────
# Env + helpers
# ──────────────────────────────────────────────────────────────────────────────
def _envb(k: str, d: bool = False) -> bool:
    v = (os.getenv(k, "") or "").strip().lower()
    return d if not v else v in {"1", "true", "yes", "y", "on"}

def _envf(k: str, d: float) -> float:
    try:
        return float(os.getenv(k, str(d)))
    except Exception:
        return float(d)

def _envi(k: str, d: int) -> int:
    try:
        return int(os.getenv(k, str(d)))
    except Exception:
        return int(d)

# Billing / caps (QuickNode credits)
MONTHLY_CAP = _envi("QN_MONTHLY_CREDITS", _envi("BUDGET_MONTHLY_CREDITS", 80_000_000))
# Stop issuing new calls at/after this fraction of cap (soft stop so we don’t hard wall)
SOFT_STOP_FRAC = max(0.50, min(0.99, _envf("BUDGET_SOFT_STOP_FRAC", 0.95)))
# Alert when projected > this fraction of monthly cap
ALERT_FRAC = max(0.10, min(0.99, _envf("BUDGET_ALERT_FRAC", 0.90)))

# Per-minute soft rate limits (advisory)
RPC_PER_MIN   = max(30, _envi("BUDGET_RPC_PER_MIN", 400))       # all RPC categories, combined
JUP_PER_MIN   = max(10, _envi("BUDGET_JUP_PER_MIN", 80))        # Jupiter quote fetches
# Optional daily cap for quotes to avoid “quote storms”
JUP_DAILY_CAP = max(0, _envi("BUDGET_JUP_DAILY_CAP", 50_000))

# Billing “month” anchor (e.g., your QN cycle starts on the 11th)
BILLING_RESET_DAY = min(28, max(1, _envi("BUDGET_RESET_DAY", _envi("QN_BILLING_RESET_DAY", 1))))

# Optional Redis (preferred for multi-dyno)
REDIS_URL = (os.getenv("REDIS_URL") or "").strip()

# Local fallback file (single-dyno / best-effort when Redis is absent)
STATE_FILE = os.getenv("PF_BUDGET_STATE", "/var/data/budget_state.json")

# Optional: per-method weights (QuickNode charges different credits per method).
# You can override by env like BUDGET_RPC_WEIGHT_getTransaction=5
_DEFAULT_WEIGHTS: Dict[str, int] = {
    # sane defaults (tune if you know QN’s method costs for your plan)
    "getBalance": 1,
    "getLatestBlockhash": 1,
    "getSignatureStatuses": 1,
    "getTransaction": 5,
    "getBlock": 4,
    "getProgramAccounts": 8,
    "getLogs": 4,
}
def _method_weight(method: str) -> int:
    # allow env override per method
    env_key = f"BUDGET_RPC_WEIGHT_{method}"
    try:
        v = os.getenv(env_key)
        if v is not None:
            return int(v)
    except Exception:
        pass
    return int(_DEFAULT_WEIGHTS.get(method, 1))

# ──────────────────────────────────────────────────────────────────────────────
# Time windows
# ──────────────────────────────────────────────────────────────────────────────
def _minute_key(ts: Optional[float] = None) -> str:
    t = int(ts or time.time())
    return time.strftime("%Y%m%d%H%M", time.gmtime(t))

def _day_key(ts: Optional[float] = None) -> str:
    t = int(ts or time.time())
    return time.strftime("%Y%m%d", time.gmtime(t))

def _billing_month_key(ts: Optional[float] = None) -> Tuple[str, int]:
    """
    Returns (month_key, period_start_epoch_utc) using BILLING_RESET_DAY anchor.
    If today < anchor, the window is last month’s anchor → this month’s anchor.
    """
    import datetime as _dt
    now = _dt.datetime.utcfromtimestamp(int(ts or time.time()))
    year = now.year
    month = now.month
    day = now.day
    # compute current cycle start
    if day >= BILLING_RESET_DAY:
        start = _dt.datetime(year, month, BILLING_RESET_DAY)
    else:
        # previous month (handle January)
        if month == 1:
            pyear, pmonth = year - 1, 12
        else:
            pyear, pmonth = year, month - 1
        start = _dt.datetime(pyear, pmonth, BILLING_RESET_DAY)
    month_key = start.strftime("%Y-%m")
    return month_key, int(start.replace(tzinfo=_dt.timezone.utc).timestamp())

# ──────────────────────────────────────────────────────────────────────────────
# Storage backends
# ──────────────────────────────────────────────────────────────────────────────
class _RedisStore:
    def __init__(self, url: str):
        self.r = redis.Redis.from_url(  # type: ignore[attr-defined]
            url,
            socket_timeout=1.5,
            socket_connect_timeout=1.5,
            decode_responses=True,
        )
        try:
            self.r.ping()
        except Exception as e:
            raise RuntimeError(f"Redis ping failed: {e}") from e

    # Minute counters (soft rate limit)
    def minute_incr(self, bucket: str, n: int) -> int:
        key = f"budget:minute:{bucket}"
        pipe = self.r.pipeline()
        pipe.incrby(key, int(n))
        pipe.expire(key, 120)  # keep ~2 minutes
        v, _ = pipe.execute()
        return int(v or 0)

    def minute_get(self, bucket: str) -> int:
        return int(self.r.get(f"budget:minute:{bucket}") or 0)

    # Daily counters for Jupiter quotes (optional)
    def day_incr(self, bucket: str, n: int) -> int:
        key = f"budget:day:{bucket}"
        pipe = self.r.pipeline()
        pipe.incrby(key, int(n))
        pipe.expire(key, 48 * 3600)
        v, _ = pipe.execute()
        return int(v or 0)

    def day_get(self, bucket: str) -> int:
        return int(self.r.get(f"budget:day:{bucket}") or 0)

    # Monthly credits (QuickNode)
    def month_incr(self, month_key: str, n: int) -> int:
        key = f"budget:month_used:{month_key}"
        return int(self.r.incrby(key, int(n)) or 0)

    def month_get(self, month_key: str) -> int:
        return int(self.r.get(f"budget:month_used:{month_key}") or 0)

class _FileStore:
    """
    Simple JSON file; best-effort single-dyno. Safe under one process,
    not atomic across multiple dynos. Provides per-bucket counters.
    """
    def __init__(self, path: str):
        self.path = path
        self._lock = threading.Lock()
        self._state: Dict[str, Any] = self._load()

    def _load(self) -> Dict[str, Any]:
        try:
            d = os.path.dirname(self.path)
            if d and not os.path.exists(d):
                os.makedirs(d, exist_ok=True)
            if os.path.exists(self.path):
                with open(self.path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    if isinstance(data, dict):
                        # ensure sub-structures
                        data.setdefault("minute", {})
                        data.setdefault("day", {})
                        data.setdefault("month_used", {})
                        return data
        except Exception:
            pass
        return {"minute": {}, "day": {}, "month_used": {}}

    def _save(self) -> None:
        try:
            with open(self.path, "w", encoding="utf-8") as f:
                json.dump(self._state, f)
        except Exception:
            pass

    def _prune_minutes(self, keep_bucket: str) -> None:
        # Keep only current minute buckets to cap growth
        pref = keep_bucket.split(":")[0] + ":"
        minute_root: Dict[str, int] = self._state.setdefault("minute", {})
        for k in list(minute_root.keys()):
            # drop any minute buckets not equal to current minute for same prefix
            if k.startswith(pref) and k != keep_bucket:
                minute_root.pop(k, None)

    def minute_incr(self, bucket: str, n: int) -> int:
        with self._lock:
            minute_root: Dict[str, int] = self._state.setdefault("minute", {})
            cur = int(minute_root.get(bucket, 0)) + int(n)
            minute_root[bucket] = cur
            self._prune_minutes(bucket)
            self._save()
            return cur

    def minute_get(self, bucket: str) -> int:
        with self._lock:
            minute_root: Dict[str, int] = self._state.setdefault("minute", {})
            return int(minute_root.get(bucket, 0))

    def day_incr(self, bucket: str, n: int) -> int:
        with self._lock:
            day_root: Dict[str, int] = self._state.setdefault("day", {})
            cur = int(day_root.get(bucket, 0)) + int(n)
            day_root[bucket] = cur
            # optional prune: keep only current day buckets for same prefix
            pref = bucket.split(":")[0] + ":"
            for k in list(day_root.keys()):
                if k.startswith(pref) and k != bucket:
                    day_root.pop(k, None)
            self._save()
            return cur

    def day_get(self, bucket: str) -> int:
        with self._lock:
            day_root: Dict[str, int] = self._state.setdefault("day", {})
            return int(day_root.get(bucket, 0))

    def month_incr(self, month_key: str, n: int) -> int:
        with self._lock:
            m: Dict[str, int] = self._state.setdefault("month_used", {})
            used = int(m.get(month_key, 0)) + int(n)
            m[month_key] = used
            self._save()
            return used

    def month_get(self, month_key: str) -> int:
        with self._lock:
            m: Dict[str, int] = self._state.setdefault("month_used", {})
            return int(m.get(month_key, 0))

# pick backend
_store: Any
if REDIS_URL and redis is not None:
    try:
        _store = _RedisStore(REDIS_URL)
    except Exception as e:
        log.event("BUDGET_REDIS_FAIL", err=str(e))
        _store = _FileStore(STATE_FILE)
else:
    _store = _FileStore(STATE_FILE)

# ──────────────────────────────────────────────────────────────────────────────
# Public API
# ──────────────────────────────────────────────────────────────────────────────
@dataclass
class BudgetSnapshot:
    month_key: str
    period_start: int
    used: int
    cap: int
    remaining: int
    used_frac: float
    rpcs_this_min: int
    jup_this_min: int
    jup_today: int
    forecast_frac: float

def _forecast_frac(used: int, period_start: int, cap: int) -> float:
    # naive linear forecast based on run-rate since period_start
    el = max(1.0, time.time() - period_start)
    per_sec = used / el
    approx_month_secs = 30 * 24 * 3600  # close enough for guard purposes
    try:
        return min(1.5, (per_sec * approx_month_secs) / max(1, cap))
    except Exception:
        return 0.0

def snapshot() -> BudgetSnapshot:
    mk, start = _billing_month_key()
    used = _store.month_get(mk)
    cap = max(1, MONTHLY_CAP)
    rem = max(0, cap - used)
    used_frac = used / cap
    minute = _minute_key()
    day = _day_key()
    rpcs_min = _store.minute_get(f"rpc:{minute}")
    jup_min  = _store.minute_get(f"jup:{minute}")
    jup_day  = _store.day_get(f"jup:{day}")
    return BudgetSnapshot(
        month_key=mk,
        period_start=start,
        used=used,
        cap=cap,
        remaining=rem,
        used_frac=used_frac,
        rpcs_this_min=rpcs_min,
        jup_this_min=jup_min,
        jup_today=jup_day,
        forecast_frac=_forecast_frac(used, start, cap),
    )

def allow_rpc(method: str, *, weight: Optional[int] = None) -> bool:
    """
    Soft gate. True => proceed; False => caller should skip/defer.
    You MUST call note_rpc(method, weight=...) AFTER a successful RPC to book the usage.
    """
    s = snapshot()
    if s.used_frac >= SOFT_STOP_FRAC:
        return False
    if s.forecast_frac >= ALERT_FRAC:
        # still allow, but we’ll log a warning
        log.every(
            "budget.forecast",
            n=10,
            msg="BUDGET_FORECAST_HIGH",
            used=s.used,
            cap=s.cap,
            forecast_frac=round(s.forecast_frac, 3),
        )
    # minute gate
    w = int(weight if weight is not None else _method_weight(method))
    cur_min = _store.minute_get(f"rpc:{_minute_key()}")
    return (cur_min + w) <= RPC_PER_MIN

def note_rpc(method: str, *, weight: Optional[int] = None) -> None:
    """Book RPC usage AFTER a successful call."""
    w = int(weight if weight is not None else _method_weight(method))
    mk, _ = _billing_month_key()
    # month usage
    used = _store.month_incr(mk, w)
    # minute bucket
    cur_min = _store.minute_incr(f"rpc:{_minute_key()}", w)
    if used / max(1, MONTHLY_CAP) >= ALERT_FRAC:
        log.every(
            "budget.alert",
            n=5,
            msg="BUDGET_ALERT_USAGE",
            used=used,
            cap=MONTHLY_CAP,
            pct=round(100 * used / max(1, MONTHLY_CAP), 2),
        )
    if cur_min > RPC_PER_MIN:
        log.every(
            "budget.rpm",
            n=5,
            msg="BUDGET_MINUTE_RPC_HOT",
            count=cur_min,
            limit=RPC_PER_MIN,
        )

def allow_quote(kind: str = "jup_quote", *, units: int = 1) -> bool:
    """
    Soft gate for quote fetches (e.g., Jupiter / pricing). Uses separate per-minute / daily caps.
    After a successful quote, call note_quote(kind, units=...).
    """
    minute = _store.minute_get(f"jup:{_minute_key()}")
    if (minute + units) > JUP_PER_MIN:
        return False
    if JUP_DAILY_CAP > 0:
        day = _store.day_get(f"jup:{_day_key()}")
        if (day + units) > JUP_DAILY_CAP:
            return False
    return True

def note_quote(kind: str = "jup_quote", *, units: int = 1) -> None:
    """Book quote usage AFTER a successful quote call."""
    cur = _store.minute_incr(f"jup:{_minute_key()}", units)
    day = _store.day_incr(f"jup:{_day_key()}", units)
    if cur > JUP_PER_MIN:
        log.every("budget.quote.rpm", n=5, msg="BUDGET_MINUTE_JUP_HOT", count=cur, limit=JUP_PER_MIN)
    if JUP_DAILY_CAP > 0 and day > JUP_DAILY_CAP:
        log.every("budget.quote.daily", n=5, msg="BUDGET_DAILY_JUP_HOT", count=day, limit=JUP_DAILY_CAP)

# Convenience wrappers you can drop around calls
def before_rpc(method: str, *, weight: Optional[int] = None) -> bool:
    """Return True if you *should* execute the RPC now (passes soft gates)."""
    ok = allow_rpc(method, weight=weight)
    if not ok:
        s = snapshot()
        log.event(
            "BUDGET_BLOCK_RPC",
            method=method,
            used=s.used,
            cap=s.cap,
            used_pct=round(100 * s.used / max(1, s.cap), 2),
            minute=_store.minute_get(f"rpc:{_minute_key()}"),
            rpm_limit=RPC_PER_MIN,
        )
    return ok

def after_rpc(method: str, *, weight: Optional[int] = None) -> None:
    """Call after success."""
    note_rpc(method, weight=weight)

def before_quote(units: int = 1) -> bool:
    ok = allow_quote(units=units)
    if not ok:
        s = snapshot()
        log.event(
            "BUDGET_BLOCK_QUOTE",
            minute=s.jup_this_min,
            rpm_limit=JUP_PER_MIN,
            day=s.jup_today,
            daily_limit=JUP_DAILY_CAP,
        )
    return ok

def after_quote(units: int = 1) -> None:
    note_quote(units=units)

__all__ = [
    "BudgetSnapshot",
    "snapshot",
    "allow_rpc",
    "note_rpc",
    "allow_quote",
    "note_quote",
    "before_rpc",
    "after_rpc",
    "before_quote",
    "after_quote",
]
