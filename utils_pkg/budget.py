from __future__ import annotations

"""
utils_pkg/budget.py — God-Tier Budget + Rate Guardrails (V3+++ / quant-grade) — LOCK-IN FINAL
===========================================================================================

Purpose
-------
Non-trading throttle + credit-accounting layer that protects:

  1) Provider “monthly credits” style caps (weighted RPC calls)
  2) Per-minute RPC pressure (avoid bans/timeouts)
  3) Per-minute + optional daily Jupiter quote storms (avoid 429 / degraded routing)
  4) Multi-dyno safety via Redis (preferred), with File fallback (best-effort)

Hard constraints
----------------
✅ NO network at import time
✅ Safe to call from async code WITHOUT blocking the event loop
✅ Sync API stays stable (back-compat)
✅ When called from inside an event loop, sync calls never block:
   - returns cached snapshot + local counters immediately
   - schedules async refresh / async writes best-effort

Stable public API
-----------------
  snapshot() -> BudgetSnapshot
  allow_rpc()/note_rpc()
  allow_quote()/note_quote()
  before_rpc()/after_rpc()
  before_quote()/after_quote()

Optional async API (recommended)
-------------------------------
  await async_snapshot()
  await async_allow_rpc()/async_note_rpc()
  await async_allow_quote()/async_note_quote()

Environment contract (key knobs)
--------------------------------
Enable/namespace:
  - BUDGET_ENABLED (default 1)
  - BUDGET_NAMESPACE (default SERVICE_NAME or "sniper")

Monthly credit cap:
  - QN_MONTHLY_CREDITS or BUDGET_MONTHLY_CREDITS (default 80_000_000)
  - BUDGET_RESET_DAY or QN_BILLING_RESET_DAY (default 1, 1..28)
  - BUDGET_SOFT_STOP_FRAC (default 0.95)  -> block new work above this usage
  - BUDGET_ALERT_FRAC (default 0.90)      -> alert above this usage (and forecast)

Minute rate limits:
  - BUDGET_RPC_PER_MIN (default 400 weighted credits/min)
  - BUDGET_JUP_PER_MIN (default 80 quote units/min)
  - BUDGET_JUP_DAILY_CAP (default 50_000, 0 disables)

Redis / store:
  - REDIS_URL or REDIS_TLS_URL
  - REDIS_FORCE_TLS (default 0)
  - BUDGET_FORCE_FILE (default 0)
  - PF_BUDGET_STATE (default /var/data/budget_state.json)
  - BUDGET_REDIS_VALIDATE (default 1)
  - BUDGET_REDIS_SOCKET_TIMEOUT_SEC (default 1.5)
  - BUDGET_REDIS_CONNECT_TIMEOUT_SEC (default 1.5)

Cache/behavior:
  - BUDGET_REFRESH_SEC (default 2.0)         -> config hot-reload TTL
  - BUDGET_SNAPSHOT_TTL_SEC (default 0.35)   -> snapshot cache TTL
  - BUDGET_LOCAL_MAX_KEYS (default 2000)     -> local LRU keys
  - BUDGET_CONSERVATIVE_ON_UNKNOWN (default 0) -> fail-closed snapshot when unknown
  - BUDGET_FAIL_OPEN (default 1)             -> if store errors, allow instead of block

Weights:
  - BUDGET_RPC_WEIGHT_getTransaction=7 (per-method override)

Notes
-----
- Redis is preferred for multi-dyno correctness.
- If Redis is missing/unavailable, FileStore is used (best-effort, single-process).
- No network occurs at import; network happens only on actual store ops.
"""

import asyncio
import contextlib
import json
import os
import ssl
import threading
import time
from collections import OrderedDict, deque
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlparse, urlunparse

try:  # pragma: no cover
    import certifi  # type: ignore
except Exception:  # pragma: no cover
    certifi = None  # type: ignore

try:  # pragma: no cover
    import redis  # type: ignore
except Exception:  # pragma: no cover
    redis = None  # type: ignore


# ──────────────────────────────────────────────────────────────────────────────
# Logging (shim-safe; logging_setup.get_logger is canonical)
# ──────────────────────────────────────────────────────────────────────────────
try:  # pragma: no cover
    from logging_setup import get_logger  # type: ignore
except Exception:  # pragma: no cover
    import logging

    def get_logger(name: str | None = None):  # type: ignore
        return logging.getLogger(name or "budget")


log = get_logger(__name__)

if not hasattr(log, "event"):

    def _event(name: str, **kw: Any) -> None:  # pragma: no cover
        with contextlib.suppress(Exception):
            log.info("%s %s", name, kw)

    log.event = _event

if not hasattr(log, "every"):

    def _every(
        key: str, n: int = 10, msg: str | None = None, **kw: Any
    ) -> None:  # pragma: no cover
        """
        Throttle by seconds (not by count). `n` is seconds.
        """
        try:
            now = time.monotonic()
            store = getattr(_every, "_store", {})  # type: ignore[attr-defined]
            last = float(store.get(key, 0.0))
            if (now - last) >= float(max(1, int(n))):
                store[key] = now
                _every._store = store  # type: ignore[attr-defined]
                log.event(msg or key, **kw)
        except Exception:
            return None

    log.every = _every


def _short(x: Any, n: int = 240) -> str:
    try:
        s = str(x)
        return s if len(s) <= n else (s[:n] + "…")
    except Exception:
        return "<?>"


# ──────────────────────────────────────────────────────────────────────────────
# Env helpers
# ──────────────────────────────────────────────────────────────────────────────
_TRUE = {"1", "true", "yes", "y", "on", "t"}
_FALSE = {"0", "false", "no", "n", "off", "f"}


def _envs(k: str, d: str = "") -> str:
    v = os.getenv(k)
    return (v if v is not None else d).strip()


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
    try:
        return int(float(_envs(k, str(d))))
    except Exception:
        return int(d)


def _envf(k: str, d: float) -> float:
    try:
        return float(_envs(k, str(d)))
    except Exception:
        return float(d)


BUDGET_VERSION = "v3+++_lockin_budget_2026-01-06"


# ──────────────────────────────────────────────────────────────────────────────
# Weights (provider credits)
# ──────────────────────────────────────────────────────────────────────────────
_DEFAULT_WEIGHTS: dict[str, int] = {
    "getBalance": 1,
    "getLatestBlockhash": 1,
    "getSignatureStatuses": 1,
    "getTransaction": 5,
    "getBlock": 4,
    "getProgramAccounts": 8,
    "getLogs": 4,
    "sendTransaction": 2,
    "simulateTransaction": 2,
}


def _method_weight(method: str) -> int:
    """
    Weighted RPC credits; supports per-method overrides:
      BUDGET_RPC_WEIGHT_getTransaction=7
    """
    m = (method or "").strip()
    if not m:
        return 1
    env_key = f"BUDGET_RPC_WEIGHT_{m}"
    try:
        v = os.getenv(env_key)
        if v is not None and str(v).strip():
            return max(1, int(float(v)))
    except Exception:
        pass
    return max(1, int(_DEFAULT_WEIGHTS.get(m, 1)))


# ──────────────────────────────────────────────────────────────────────────────
# Time keys (UTC)
# ──────────────────────────────────────────────────────────────────────────────
def _minute_key(ts: float | None = None) -> str:
    t = int(ts or time.time())
    return time.strftime("%Y%m%d%H%M", time.gmtime(t))


def _day_key(ts: float | None = None) -> str:
    t = int(ts or time.time())
    return time.strftime("%Y%m%d", time.gmtime(t))


def _billing_month_key(ts: float | None = None, reset_day: int = 1) -> tuple[str, int]:
    """
    Returns (month_key, period_start_epoch_utc) using reset_day anchor (1..28).
    If today < anchor, window starts last month at anchor.
    """
    import datetime as _dt

    reset_day = min(28, max(1, int(reset_day)))
    now = _dt.datetime.fromtimestamp(int(ts or time.time()), tz=_dt.UTC)

    year = now.year
    month = now.month
    day = now.day

    if day >= reset_day:
        start = _dt.datetime(year, month, reset_day, tzinfo=_dt.UTC)
    else:
        if month == 1:
            pyear, pmonth = year - 1, 12
        else:
            pyear, pmonth = year, month - 1
        start = _dt.datetime(pyear, pmonth, reset_day, tzinfo=_dt.UTC)

    month_key = start.strftime("%Y-%m")
    return month_key, int(start.timestamp())


# ──────────────────────────────────────────────────────────────────────────────
# Redis URL safety + TLS upgrade (Upstash-safe)
# ──────────────────────────────────────────────────────────────────────────────
def _mask_url(url: str) -> str:
    if not url:
        return ""
    try:
        u = urlparse(url)
        host = u.hostname or ""
        port = f":{u.port}" if u.port else ""
        scheme = u.scheme or "redis"
        return f"{scheme}://{host}{port}"
    except Exception:
        return ""


def _looks_like_upstash(host: str) -> bool:
    h = (host or "").lower()
    return h.endswith(".upstash.io") or "upstash" in h


def _effective_redis_url() -> str:
    return (os.getenv("REDIS_URL") or os.getenv("REDIS_TLS_URL") or "").strip()


def _upgrade_to_tls_if_needed(url: str) -> str:
    if not url:
        return ""
    try:
        u = urlparse(url)
        host = u.hostname or ""
        force_tls = _envb("REDIS_FORCE_TLS", False) or _looks_like_upstash(host)
        if force_tls and u.scheme == "redis":
            return urlunparse(u._replace(scheme="rediss"))
        return url
    except Exception:
        return url


def _redis_ssl_kwargs_if_needed(url_eff: str) -> dict[str, Any]:
    kw: dict[str, Any] = {}
    if url_eff.startswith("rediss://"):
        if certifi is not None:
            kw["ssl_ca_certs"] = certifi.where()
        kw["ssl_cert_reqs"] = ssl.CERT_REQUIRED
        kw["ssl_check_hostname"] = True
    return kw


# ──────────────────────────────────────────────────────────────────────────────
# Config (hot-reload TTL)
# ──────────────────────────────────────────────────────────────────────────────
_CFG_LOCK = threading.Lock()
_CFG_TTL_SEC = max(0.25, _envf("BUDGET_REFRESH_SEC", 2.0))
_cfg_at: float = 0.0
_cfg_cache: dict[str, Any] = {}


def _cfg() -> dict[str, Any]:
    global _cfg_at, _cfg_cache
    now = time.monotonic()

    with _CFG_LOCK:
        if _cfg_cache and (now - _cfg_at) < _CFG_TTL_SEC:
            return _cfg_cache

        enabled = _envb("BUDGET_ENABLED", True)

        monthly_cap = _envi("QN_MONTHLY_CREDITS", _envi("BUDGET_MONTHLY_CREDITS", 80_000_000))
        soft_stop_frac = max(0.50, min(0.999, _envf("BUDGET_SOFT_STOP_FRAC", 0.95)))
        alert_frac = max(0.10, min(0.999, _envf("BUDGET_ALERT_FRAC", 0.90)))

        rpc_per_min = max(30, _envi("BUDGET_RPC_PER_MIN", 400))  # weighted credits/min
        jup_per_min = max(10, _envi("BUDGET_JUP_PER_MIN", 80))  # quote units/min
        jup_daily_cap = max(0, _envi("BUDGET_JUP_DAILY_CAP", 50_000))

        reset_day = min(28, max(1, _envi("BUDGET_RESET_DAY", _envi("QN_BILLING_RESET_DAY", 1))))
        state_file = _envs("PF_BUDGET_STATE", "/var/data/budget_state.json")

        conservative_on_unknown = _envb("BUDGET_CONSERVATIVE_ON_UNKNOWN", False)
        fail_open = _envb("BUDGET_FAIL_OPEN", True)

        # Optionally count quotes into monthly credits too
        quote_credit_cost = max(0, _envi("BUDGET_QUOTE_CREDIT_COST", 0))  # 0 disables

        namespace = _envs("BUDGET_NAMESPACE", "") or _envs("SERVICE_NAME", "") or "sniper"

        snap_ttl = max(0.10, _envf("BUDGET_SNAPSHOT_TTL_SEC", 0.35))
        local_max_keys = max(200, _envi("BUDGET_LOCAL_MAX_KEYS", 2000))

        cfg = {
            "enabled": bool(enabled),
            "monthly_cap": max(1, int(monthly_cap)),
            "soft_stop_frac": float(soft_stop_frac),
            "alert_frac": float(alert_frac),
            "rpc_per_min": int(rpc_per_min),
            "jup_per_min": int(jup_per_min),
            "jup_daily_cap": int(jup_daily_cap),
            "reset_day": int(reset_day),
            "state_file": state_file,
            "conservative_on_unknown": bool(conservative_on_unknown),
            "fail_open": bool(fail_open),
            "quote_credit_cost": int(quote_credit_cost),
            "namespace": str(namespace),
            "snap_ttl": float(snap_ttl),
            "local_max_keys": int(local_max_keys),
        }

        _cfg_cache = cfg
        _cfg_at = now
        return cfg


# ──────────────────────────────────────────────────────────────────────────────
# Keys + namespace
# ──────────────────────────────────────────────────────────────────────────────
def _budget_prefix() -> str:
    ns = _cfg()["namespace"]
    return f"budget:v3:{ns}"


def _keys(prefix: str, minute: str, day: str, month_key: str) -> dict[str, str]:
    return {
        "rpc_min": f"{prefix}:minute:{minute}:rpc",
        "jup_min": f"{prefix}:minute:{minute}:jup",
        "jup_day": f"{prefix}:day:{day}:jup",
        "month": f"{prefix}:month:{month_key}:used",
    }


# ──────────────────────────────────────────────────────────────────────────────
# Store backends (lazy init; no network at import)
# ──────────────────────────────────────────────────────────────────────────────
_LUA_INCR_EXPIRE = r"""
local key = KEYS[1]
local by = tonumber(ARGV[1]) or 1
local ttl = tonumber(ARGV[2]) or 0
local v = redis.call('INCRBY', key, by)
if ttl and ttl > 0 then
  redis.call('EXPIRE', key, ttl)
end
return v
"""


class _AsyncRedisStore:
    """
    Async Redis backend (preferred).
    Uses redis.asyncio if available; otherwise uses sync redis via threads.

    No network at init; connection attempts happen when commands execute.
    """

    def __init__(self, url: str):
        if not url or redis is None:
            raise RuntimeError("redis missing or url empty")

        self.url_eff = _upgrade_to_tls_if_needed(url)
        self.url_mask = _mask_url(self.url_eff)

        sock_to = float(_envf("BUDGET_REDIS_SOCKET_TIMEOUT_SEC", 1.5))
        conn_to = float(_envf("BUDGET_REDIS_CONNECT_TIMEOUT_SEC", 1.5))
        ssl_kw = _redis_ssl_kwargs_if_needed(self.url_eff)

        self._async = None
        try:
            ra = getattr(redis, "asyncio", None)
            if ra is not None:
                self._async = ra.Redis.from_url(  # type: ignore[attr-defined]
                    self.url_eff,
                    decode_responses=True,
                    socket_timeout=sock_to,
                    socket_connect_timeout=conn_to,
                    **ssl_kw,
                )
        except Exception:
            self._async = None

        self._sync = None
        try:
            self._sync = redis.Redis.from_url(  # type: ignore[attr-defined]
                self.url_eff,
                decode_responses=True,
                socket_timeout=sock_to,
                socket_connect_timeout=conn_to,
                **ssl_kw,
            )
        except Exception:
            self._sync = None

        if self._async is None and self._sync is None:
            raise RuntimeError("could not build redis client")

    async def validate(self) -> None:
        """
        Best-effort ping validation. May do network (runtime only).
        """
        try:
            if self._async is not None:
                await self._async.ping()
                return
        except Exception:
            pass
        if self._sync is not None:
            await asyncio.to_thread(self._sync.ping)
            return
        raise RuntimeError("redis validate failed")

    async def get_many(self, keys: list[str]) -> list[int]:
        if self._async is not None:
            vals = await self._async.mget(keys)
            return [int(v or 0) for v in (vals or [])]
        assert self._sync is not None
        vals = await asyncio.to_thread(self._sync.mget, keys)
        return [int(v or 0) for v in (vals or [])]

    async def incr_with_ttl(self, key: str, n: int, ttl_s: int) -> int:
        """
        Atomic-ish INCRBY + EXPIRE:
        - Prefer Lua (atomic)
        - Fallback to pipeline if Lua not supported
        """
        n = int(n)
        ttl_s = int(ttl_s)

        if self._async is not None:
            try:
                v = await self._async.eval(_LUA_INCR_EXPIRE, 1, key, n, ttl_s)
                return int(v or 0)
            except Exception:
                pipe = self._async.pipeline()
                pipe.incrby(key, n)
                if ttl_s > 0:
                    pipe.expire(key, ttl_s)
                out = await pipe.execute()
                return int((out or [0])[0] or 0)

        assert self._sync is not None

        def _do() -> int:
            try:
                v = self._sync.eval(_LUA_INCR_EXPIRE, 1, key, n, ttl_s)
                return int(v or 0)
            except Exception:
                pipe = self._sync.pipeline()
                pipe.incrby(key, n)
                if ttl_s > 0:
                    pipe.expire(key, ttl_s)
                out = pipe.execute()
                return int((out or [0])[0] or 0)

        return await asyncio.to_thread(_do)


class _FileStore:
    """
    JSON file fallback. Not multi-dyno safe; best-effort.

    Suitable for: local dev, single-process, or emergency fallback.
    """

    def __init__(self, path: str):
        self.path = path
        self._lock = threading.Lock()
        self._state: dict[str, Any] = self._load()

    def _load(self) -> dict[str, Any]:
        try:
            d = os.path.dirname(self.path)
            if d and not os.path.exists(d):
                os.makedirs(d, exist_ok=True)
            if os.path.exists(self.path):
                with open(self.path, encoding="utf-8") as f:
                    data = json.load(f)
                    if isinstance(data, dict):
                        data.setdefault("kv", {})
                        return data
        except Exception:
            pass
        return {"kv": {}}

    def _save(self) -> None:
        try:
            with open(self.path, "w", encoding="utf-8") as f:
                json.dump(self._state, f)
        except Exception:
            pass

    def _prune(self) -> None:
        kv: dict[str, Any] = self._state.get("kv", {})
        if not isinstance(kv, dict):
            self._state["kv"] = {}
            return
        now = int(time.time())

        def _keep(k: str) -> bool:
            # prune minute keys older than ~15m
            if ":minute:" in k:
                try:
                    mk = k.split(":minute:")[1].split(":")[0]
                    import datetime as _dt

                    dt = _dt.datetime.strptime(mk, "%Y%m%d%H%M").replace(tzinfo=_dt.UTC)
                    return (now - int(dt.timestamp())) <= 15 * 60
                except Exception:
                    return True
            # prune day keys older than ~14d
            if ":day:" in k:
                try:
                    dk = k.split(":day:")[1].split(":")[0]
                    import datetime as _dt

                    dt = _dt.datetime.strptime(dk, "%Y%m%d").replace(tzinfo=_dt.UTC)
                    return (now - int(dt.timestamp())) <= 14 * 24 * 3600
                except Exception:
                    return True
            # month keys are kept (small count)
            return True

        for k in list(kv.keys()):
            if not _keep(k):
                kv.pop(k, None)

    def get_many(self, keys: list[str]) -> list[int]:
        with self._lock:
            kv: dict[str, Any] = self._state.get("kv", {})
            return [int(kv.get(k, 0) or 0) for k in keys]

    def incr(self, key: str, n: int) -> int:
        with self._lock:
            kv: dict[str, Any] = self._state.setdefault("kv", {})
            cur = int(kv.get(key, 0) or 0) + int(n)
            kv[key] = cur
            self._prune()
            self._save()
            return cur


_STORE_LOCK = threading.Lock()
_STORE: Any = None  # _AsyncRedisStore or _FileStore


def _get_store() -> Any:
    """
    Lazy store init. No network at import time; store does not connect until ops.
    """
    global _STORE
    if _STORE is not None:
        return _STORE

    with _STORE_LOCK:
        if _STORE is not None:
            return _STORE

        c = _cfg()
        url = _effective_redis_url()

        if _envb("BUDGET_FORCE_FILE", False) or (not url) or (redis is None):
            _STORE = _FileStore(c["state_file"])
            log.every("BUDGET_FILESTORE_OK", n=20, msg="BUDGET_FILESTORE_OK", path=c["state_file"])
            return _STORE

        try:
            st = _AsyncRedisStore(url)
            _STORE = st

            # Best-effort validate; never at import; only on first use
            if _envb("BUDGET_REDIS_VALIDATE", True):
                try:
                    loop = asyncio.get_running_loop()
                    loop.create_task(st.validate())
                except Exception:
                    # No running loop; validate in a background thread (avoid long blocking)
                    def _bg() -> None:
                        with contextlib.suppress(Exception):
                            asyncio.run(st.validate())

                    threading.Thread(target=_bg, daemon=True).start()

            log.event("BUDGET_REDIS_OK", url_mask=st.url_mask)
            return _STORE
        except Exception as e:
            log.every(
                "BUDGET_REDIS_FAIL",
                n=10,
                msg="BUDGET_REDIS_FAIL",
                err=_short(e),
                url_mask=_mask_url(_upgrade_to_tls_if_needed(url)),
            )
            _STORE = _FileStore(c["state_file"])
            log.every("BUDGET_FILESTORE_OK", n=20, msg="BUDGET_FILESTORE_OK", path=c["state_file"])
            return _STORE


def _store_is_redis(st: Any) -> bool:
    return isinstance(st, _AsyncRedisStore)


async def _store_get_many(keys: list[str]) -> list[int]:
    st = _get_store()
    if _store_is_redis(st):
        return await st.get_many(keys)
    if isinstance(st, _FileStore):
        return await asyncio.to_thread(st.get_many, keys)
    return [0 for _ in keys]


async def _store_incr(key: str, n: int, *, ttl_s: int) -> int:
    st = _get_store()
    if _store_is_redis(st):
        return await st.incr_with_ttl(key, int(n), int(ttl_s))
    if isinstance(st, _FileStore):
        return int(await asyncio.to_thread(st.incr, key, int(n)))
    return 0


# ──────────────────────────────────────────────────────────────────────────────
# Local counters (for non-blocking sync calls inside event loop)
# ──────────────────────────────────────────────────────────────────────────────
_LOCAL_LOCK = threading.Lock()
_LOCAL: OrderedDict[str, int] = OrderedDict()  # key -> count (LRU)


def _local_max() -> int:
    try:
        return max(200, int(_cfg().get("local_max_keys") or 2000))
    except Exception:
        return 2000


def _local_get(key: str) -> int:
    with _LOCAL_LOCK:
        v = int(_LOCAL.get(key, 0) or 0)
        if key in _LOCAL:
            _LOCAL.move_to_end(key, last=True)
        return v


def _local_incr(key: str, n: int) -> int:
    with _LOCAL_LOCK:
        cur = int(_LOCAL.get(key, 0) or 0) + int(n)
        _LOCAL[key] = cur
        _LOCAL.move_to_end(key, last=True)
        while len(_LOCAL) > _local_max():
            _LOCAL.popitem(last=False)
        return cur


def _local_clear() -> None:
    with _LOCAL_LOCK:
        _LOCAL.clear()


# ──────────────────────────────────────────────────────────────────────────────
# Snapshot caching + coalesced refresh
# ──────────────────────────────────────────────────────────────────────────────
@dataclass(frozen=True)
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
    enabled: bool
    version: str = BUDGET_VERSION

    def as_dict(self) -> dict[str, Any]:
        return {
            "month_key": self.month_key,
            "period_start": self.period_start,
            "used": self.used,
            "cap": self.cap,
            "remaining": self.remaining,
            "used_frac": self.used_frac,
            "rpcs_this_min": self.rpcs_this_min,
            "jup_this_min": self.jup_this_min,
            "jup_today": self.jup_today,
            "forecast_frac": self.forecast_frac,
            "enabled": self.enabled,
            "version": self.version,
        }


_SNAP_LOCK = threading.Lock()
_LAST_SNAP: BudgetSnapshot | None = None
_LAST_SNAP_AT: float = 0.0
_SNAP_INFLIGHT: bool = False


def _forecast_frac(used: int, period_start: int, cap: int) -> float:
    """
    Simple linear usage forecast for the remainder of a ~30-day month window.
    """
    el = max(1.0, time.time() - float(period_start))
    per_sec = float(max(0, used)) / el
    approx_month_secs = 30.0 * 24.0 * 3600.0
    try:
        return min(5.0, (per_sec * approx_month_secs) / float(max(1, int(cap))))
    except Exception:
        return 0.0


def _cache_set(snap: BudgetSnapshot) -> None:
    global _LAST_SNAP, _LAST_SNAP_AT
    with _SNAP_LOCK:
        _LAST_SNAP = snap
        _LAST_SNAP_AT = time.monotonic()


def _cache_get() -> BudgetSnapshot | None:
    with _SNAP_LOCK:
        return _LAST_SNAP


def _cache_fresh() -> bool:
    ttl = float(_cfg().get("snap_ttl") or 0.35)
    with _SNAP_LOCK:
        if _LAST_SNAP is None:
            return False
        return (time.monotonic() - float(_LAST_SNAP_AT)) <= ttl


def _default_snap() -> BudgetSnapshot:
    c = _cfg()
    mk, start = _billing_month_key(reset_day=int(c["reset_day"]))
    cap = int(c["monthly_cap"])
    if bool(c.get("conservative_on_unknown")):
        # fail-closed-ish: pretend we're hot (blocks) if conservative mode enabled
        return BudgetSnapshot(
            mk, start, cap, cap, 0, 1.0, 10**9, 10**9, 10**9, 5.0, bool(c["enabled"])
        )
    return BudgetSnapshot(mk, start, 0, cap, cap, 0.0, 0, 0, 0, 0.0, bool(c["enabled"]))


async def async_snapshot() -> BudgetSnapshot:
    c = _cfg()
    mk, start = _billing_month_key(reset_day=int(c["reset_day"]))
    minute = _minute_key()
    day = _day_key()

    prefix = _budget_prefix()
    ks = _keys(prefix, minute, day, mk)

    vals = await _store_get_many([ks["month"], ks["rpc_min"], ks["jup_min"], ks["jup_day"]])
    used = int((vals[0] if len(vals) > 0 else 0) or 0)
    rpc_min = int((vals[1] if len(vals) > 1 else 0) or 0)
    jup_min = int((vals[2] if len(vals) > 2 else 0) or 0)
    jup_day = int((vals[3] if len(vals) > 3 else 0) or 0)

    cap = max(1, int(c["monthly_cap"]))
    rem = max(0, cap - used)
    used_frac = float(used) / float(cap)

    snap = BudgetSnapshot(
        month_key=mk,
        period_start=int(start),
        used=int(used),
        cap=int(cap),
        remaining=int(rem),
        used_frac=float(used_frac),
        rpcs_this_min=int(rpc_min),
        jup_this_min=int(jup_min),
        jup_today=int(jup_day),
        forecast_frac=float(_forecast_frac(used, start, cap)),
        enabled=bool(c["enabled"]),
    )
    _cache_set(snap)
    return snap


def snapshot() -> BudgetSnapshot:
    """
    Sync snapshot (stable API).
    Never blocks the event loop if called from within it.
    """
    # No loop: allowed to block (runtime), never at import.
    try:
        asyncio.get_running_loop()
        in_loop = True
    except Exception:
        in_loop = False

    if not in_loop:
        try:
            return asyncio.run(async_snapshot())
        except Exception as e:
            log.every("BUDGET_SNAPSHOT_FAIL", n=10, msg="BUDGET_SNAPSHOT_FAIL", err=_short(e))
            return _cache_get() or _default_snap()

    # In loop: return cached immediately; schedule refresh if stale (coalesced).
    snap = _cache_get() or _default_snap()
    _cache_set(snap)

    global _SNAP_INFLIGHT
    if (not _cache_fresh()) and (not _SNAP_INFLIGHT):
        _SNAP_INFLIGHT = True

        async def _refresh() -> None:
            global _SNAP_INFLIGHT
            try:
                await async_snapshot()
            except Exception:
                pass
            finally:
                _SNAP_INFLIGHT = False

        try:
            asyncio.create_task(_refresh())
        except Exception:
            _SNAP_INFLIGHT = False

    return snap


# ──────────────────────────────────────────────────────────────────────────────
# Fail tracking (store issues)
# ──────────────────────────────────────────────────────────────────────────────
_FAIL_TIMES: deque[float] = deque(maxlen=64)


def _note_fail() -> None:
    _FAIL_TIMES.append(time.monotonic())


def _recent_fails(window_sec: float = 30.0) -> int:
    now = time.monotonic()
    return sum(1 for t in _FAIL_TIMES if (now - t) <= float(window_sec))


def _should_fail_open() -> bool:
    try:
        return bool(_cfg().get("fail_open", True))
    except Exception:
        return True


# ──────────────────────────────────────────────────────────────────────────────
# RPC gates
# ──────────────────────────────────────────────────────────────────────────────
async def async_allow_rpc(method: str, *, weight: int | None = None) -> bool:
    c = _cfg()
    if not bool(c["enabled"]):
        return True

    try:
        s = await async_snapshot()
    except Exception as e:
        _note_fail()
        log.every(
            "BUDGET_ALLOW_RPC_SNAP_FAIL", n=8, msg="BUDGET_ALLOW_RPC_SNAP_FAIL", err=_short(e)
        )
        return bool(_should_fail_open())

    # soft stop if too high used
    if s.used_frac >= float(c["soft_stop_frac"]):
        return False

    # forecast warning
    if s.forecast_frac >= float(c["alert_frac"]):
        log.every(
            "BUDGET_FORECAST_HIGH",
            n=10,
            msg="BUDGET_FORECAST_HIGH",
            used=s.used,
            cap=s.cap,
            forecast_frac=round(s.forecast_frac, 3),
        )

    w = int(weight if weight is not None else _method_weight(method))
    return (int(s.rpcs_this_min) + w) <= int(c["rpc_per_min"])


def allow_rpc(method: str, *, weight: int | None = None) -> bool:
    """
    Sync allow_rpc (stable API). Non-blocking inside an event loop.
    """
    c = _cfg()
    if not bool(c["enabled"]):
        return True

    try:
        asyncio.get_running_loop()
        in_loop = True
    except Exception:
        in_loop = False

    if not in_loop:
        try:
            return asyncio.run(async_allow_rpc(method, weight=weight))
        except Exception as e:
            _note_fail()
            log.every("BUDGET_ALLOW_RPC_FAIL", n=10, msg="BUDGET_ALLOW_RPC_FAIL", err=_short(e))
            return bool(_should_fail_open())

    # in-loop: cached snap + local counters only (non-blocking)
    s = snapshot()
    if s.used_frac >= float(c["soft_stop_frac"]):
        return False

    w = int(weight if weight is not None else _method_weight(method))

    prefix = _budget_prefix()
    mk, _ = _billing_month_key(reset_day=int(c["reset_day"]))
    ks = _keys(prefix, _minute_key(), _day_key(), mk)
    local_rpc = _local_get(ks["rpc_min"])

    return (int(s.rpcs_this_min) + int(local_rpc) + w) <= int(c["rpc_per_min"])


async def async_note_rpc(method: str, *, weight: int | None = None) -> None:
    c = _cfg()
    if not bool(c["enabled"]):
        return

    w = int(weight if weight is not None else _method_weight(method))

    mk, start = _billing_month_key(reset_day=int(c["reset_day"]))
    prefix = _budget_prefix()
    ks = _keys(prefix, _minute_key(), _day_key(), mk)

    try:
        used = await _store_incr(ks["month"], w, ttl_s=0)
        cur_min = await _store_incr(ks["rpc_min"], w, ttl_s=120)
    except Exception as e:
        _note_fail()
        log.every("BUDGET_NOTE_RPC_FAIL", n=10, msg="BUDGET_NOTE_RPC_FAIL", err=_short(e))
        return

    cap = max(1, int(c["monthly_cap"]))
    used_frac = float(used) / float(cap)

    if used_frac >= float(c["alert_frac"]):
        log.every(
            "BUDGET_ALERT_USAGE",
            n=5,
            msg="BUDGET_ALERT_USAGE",
            used=int(used),
            cap=int(cap),
            pct=round(100.0 * float(used) / float(cap), 2),
        )

    if int(cur_min) > int(c["rpc_per_min"]):
        log.every(
            "BUDGET_MINUTE_RPC_HOT",
            n=5,
            msg="BUDGET_MINUTE_RPC_HOT",
            count=int(cur_min),
            limit=int(c["rpc_per_min"]),
        )

    # Update cached snap cheaply (best-effort)
    try:
        snap = _cache_get()
        if snap is not None and snap.month_key == mk:
            _cache_set(
                BudgetSnapshot(
                    month_key=mk,
                    period_start=int(start),
                    used=int(used),
                    cap=int(cap),
                    remaining=max(0, int(cap) - int(used)),
                    used_frac=float(used_frac),
                    rpcs_this_min=int(cur_min),
                    jup_this_min=int(snap.jup_this_min),
                    jup_today=int(snap.jup_today),
                    forecast_frac=float(_forecast_frac(int(used), int(start), int(cap))),
                    enabled=bool(c["enabled"]),
                )
            )
    except Exception:
        pass


def note_rpc(method: str, *, weight: int | None = None) -> None:
    """
    Sync note_rpc (stable API). Non-blocking inside an event loop:
      - updates local counters immediately
      - schedules async store update best-effort
    """
    c = _cfg()
    if not bool(c["enabled"]):
        return

    w = int(weight if weight is not None else _method_weight(method))

    prefix = _budget_prefix()
    mk, _ = _billing_month_key(reset_day=int(c["reset_day"]))
    ks = _keys(prefix, _minute_key(), _day_key(), mk)

    _local_incr(ks["rpc_min"], w)
    _local_incr(ks["month"], w)

    try:
        loop = asyncio.get_running_loop()
        loop.create_task(async_note_rpc(method, weight=weight))
        return
    except Exception:
        pass

    # no loop: run now
    try:
        asyncio.run(async_note_rpc(method, weight=weight))
    except Exception as e:
        _note_fail()
        log.every("BUDGET_NOTE_RPC_FAIL", n=10, msg="BUDGET_NOTE_RPC_FAIL", err=_short(e))


# ──────────────────────────────────────────────────────────────────────────────
# Quote gates
# ──────────────────────────────────────────────────────────────────────────────
async def async_allow_quote(kind: str = "jup_quote", *, units: int = 1) -> bool:
    _ = kind  # reserved for per-kind buckets
    c = _cfg()
    if not bool(c["enabled"]):
        return True

    try:
        s = await async_snapshot()
    except Exception as e:
        _note_fail()
        log.every(
            "BUDGET_ALLOW_QUOTE_SNAP_FAIL", n=8, msg="BUDGET_ALLOW_QUOTE_SNAP_FAIL", err=_short(e)
        )
        return bool(_should_fail_open())

    if (int(s.jup_this_min) + int(units)) > int(c["jup_per_min"]):
        return False
    return not (
        int(c["jup_daily_cap"]) > 0 and int(s.jup_today) + int(units) > int(c["jup_daily_cap"])
    )


def allow_quote(kind: str = "jup_quote", *, units: int = 1) -> bool:
    _ = kind
    c = _cfg()
    if not bool(c["enabled"]):
        return True

    try:
        asyncio.get_running_loop()
        in_loop = True
    except Exception:
        in_loop = False

    if not in_loop:
        try:
            return asyncio.run(async_allow_quote(kind, units=units))
        except Exception as e:
            _note_fail()
            log.every("BUDGET_ALLOW_QUOTE_FAIL", n=10, msg="BUDGET_ALLOW_QUOTE_FAIL", err=_short(e))
            return bool(_should_fail_open())

    s = snapshot()
    prefix = _budget_prefix()
    mk, _ = _billing_month_key(reset_day=int(c["reset_day"]))
    ks = _keys(prefix, _minute_key(), _day_key(), mk)

    local_min = _local_get(ks["jup_min"])
    local_day = _local_get(ks["jup_day"])

    if (int(s.jup_this_min) + int(local_min) + int(units)) > int(c["jup_per_min"]):
        return False
    return not (
        int(c["jup_daily_cap"]) > 0
        and int(s.jup_today) + int(local_day) + int(units) > int(c["jup_daily_cap"])
    )


async def async_note_quote(kind: str = "jup_quote", *, units: int = 1) -> None:
    _ = kind
    c = _cfg()
    if not bool(c["enabled"]):
        return

    mk, start = _billing_month_key(reset_day=int(c["reset_day"]))
    prefix = _budget_prefix()
    ks = _keys(prefix, _minute_key(), _day_key(), mk)

    try:
        cur_min = await _store_incr(ks["jup_min"], int(units), ttl_s=120)
        cur_day = await _store_incr(ks["jup_day"], int(units), ttl_s=48 * 3600)
    except Exception as e:
        _note_fail()
        log.every("BUDGET_NOTE_QUOTE_FAIL", n=10, msg="BUDGET_NOTE_QUOTE_FAIL", err=_short(e))
        return

    qcost = int(c.get("quote_credit_cost") or 0)
    if qcost > 0:
        with contextlib.suppress(Exception):
            await _store_incr(ks["month"], int(qcost) * int(units), ttl_s=0)

    if int(cur_min) > int(c["jup_per_min"]):
        log.every(
            "BUDGET_MINUTE_JUP_HOT",
            n=5,
            msg="BUDGET_MINUTE_JUP_HOT",
            count=int(cur_min),
            limit=int(c["jup_per_min"]),
        )
    if int(c["jup_daily_cap"]) > 0 and int(cur_day) > int(c["jup_daily_cap"]):
        log.every(
            "BUDGET_DAILY_JUP_HOT",
            n=5,
            msg="BUDGET_DAILY_JUP_HOT",
            count=int(cur_day),
            limit=int(c["jup_daily_cap"]),
        )

    # Update cached snap (best-effort)
    try:
        snap = _cache_get()
        if snap is not None and snap.month_key == mk:
            _cache_set(
                BudgetSnapshot(
                    month_key=mk,
                    period_start=int(start),
                    used=int(snap.used),
                    cap=int(snap.cap),
                    remaining=int(snap.remaining),
                    used_frac=float(snap.used_frac),
                    rpcs_this_min=int(snap.rpcs_this_min),
                    jup_this_min=int(cur_min),
                    jup_today=int(cur_day),
                    forecast_frac=float(snap.forecast_frac),
                    enabled=bool(snap.enabled),
                )
            )
    except Exception:
        pass


def note_quote(kind: str = "jup_quote", *, units: int = 1) -> None:
    _ = kind
    c = _cfg()
    if not bool(c["enabled"]):
        return

    prefix = _budget_prefix()
    mk, _ = _billing_month_key(reset_day=int(c["reset_day"]))
    ks = _keys(prefix, _minute_key(), _day_key(), mk)

    _local_incr(ks["jup_min"], int(units))
    _local_incr(ks["jup_day"], int(units))

    qcost = int(c.get("quote_credit_cost") or 0)
    if qcost > 0:
        _local_incr(ks["month"], int(qcost) * int(units))

    try:
        loop = asyncio.get_running_loop()
        loop.create_task(async_note_quote(kind, units=units))
        return
    except Exception:
        pass

    try:
        asyncio.run(async_note_quote(kind, units=units))
    except Exception as e:
        _note_fail()
        log.every("BUDGET_NOTE_QUOTE_FAIL", n=10, msg="BUDGET_NOTE_QUOTE_FAIL", err=_short(e))


# ──────────────────────────────────────────────────────────────────────────────
# Convenience wrappers (stable)
# ──────────────────────────────────────────────────────────────────────────────
def before_rpc(method: str, *, weight: int | None = None) -> bool:
    ok = allow_rpc(method, weight=weight)
    if not ok:
        s = snapshot()
        c = _cfg()
        log.event(
            "BUDGET_BLOCK_RPC",
            method=method,
            used=s.used,
            cap=s.cap,
            used_pct=round(100 * s.used / max(1, s.cap), 2),
            rpcs_this_min=s.rpcs_this_min,
            rpm_limit=int(c["rpc_per_min"]),
        )
    return ok


def after_rpc(method: str, *, weight: int | None = None) -> None:
    note_rpc(method, weight=weight)


def before_quote(units: int = 1) -> bool:
    ok = allow_quote(units=units)
    if not ok:
        s = snapshot()
        c = _cfg()
        log.event(
            "BUDGET_BLOCK_QUOTE",
            minute=s.jup_this_min,
            rpm_limit=int(c["jup_per_min"]),
            day=s.jup_today,
            daily_limit=int(c["jup_daily_cap"]),
        )
    return ok


def after_quote(units: int = 1) -> None:
    note_quote(units=units)


__all__ = [
    "BUDGET_VERSION",
    "BudgetSnapshot",
    "after_quote",
    "after_rpc",
    "allow_quote",
    "allow_rpc",
    "async_allow_quote",
    "async_allow_rpc",
    "async_note_quote",
    "async_note_rpc",
    # optional async API
    "async_snapshot",
    "before_quote",
    "before_rpc",
    "note_quote",
    "note_rpc",
    "snapshot",
]
