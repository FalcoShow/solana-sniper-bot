from __future__ import annotations

"""
utils_pkg/moonshot_budget.py — Moonshot Sleeve Policy + Reservations (V3+++ / quant-grade) — LOCK-IN
===================================================================================================

Role
----
This module is a *strategy-layer* budget sleeve for high-variance plays (pump/meme “moonshots”).
It sits ABOVE sizing and risk gates and answers:

  - "Can we allocate this moonshot right now?"
  - "Reserve budget to prevent concurrent overspend across tasks/dynos"
  - "Release reservation after fill/fail"

Design goals
------------
✅ No network at import time
✅ Deterministic, async-first with safe sync wrappers
✅ Multi-dyno safe via Redis (preferred), with File fallback (best-effort)
✅ Back-compat helpers: moonshot_limits(), can_allocate(...)

What this is / isn't
--------------------
- This is a *budget sleeve guard* (a throttle + reservation ledger).
- It is NOT your position source-of-truth (position_store/ledger will be canonical).
- It is safe to use even before position_store is rewritten.

Integration points
------------------
- risk_manager: you can pass equity_usd / desired_usd from risk_manager outputs.
- budget.py: separate concern (RPC/Jupiter rate/cost); do not mix responsibilities here.

Environment contract (key knobs)
--------------------------------
Enable:
  - MOONSHOT_ENABLED (default 1)

Sleeve sizing:
  - MOONSHOT_SLEEVE_MAX_PCT (default 0.10)         # sleeve cap as % of equity
  - MOONSHOT_PER_TRADE_PCT  (default 0.03)         # per-trade cap as % of equity
  - MOONSHOT_SLEEVE_MAX_USD (optional, default 0)  # absolute sleeve cap (0 disables)
  - MOONSHOT_PER_TRADE_MAX_USD (optional, default 0) # absolute per-trade cap

Min trade:
  - MOONSHOT_MIN_TRADE_USD (default 0)             # ignore tiny reserves if desired

Reservations:
  - MOONSHOT_RESERVE_TTL_SEC (default 900)         # auto-expire reservations (seconds)
  - MOONSHOT_PRUNE_MAX (default 100)               # max expired reservations pruned per op
  - MOONSHOT_NAMESPACE (default SERVICE_NAME or "sniper")
  - MOONSHOT_FAIL_CLOSED_ON_STORE_ERRORS (default 0)  # if 1, block when store fails

Store:
  - REDIS_URL or REDIS_TLS_URL                      # preferred
  - REDIS_FORCE_TLS (default 0)                      # upgrade redis:// -> rediss://
  - MOONSHOT_FORCE_FILE (default 0)
  - MOONSHOT_STATE_FILE (default /var/data/moonshot_budget.json)
  - MOONSHOT_REDIS_VALIDATE (default 0)              # ping on first use (runtime only)
  - MOONSHOT_REDIS_SOCKET_TIMEOUT_SEC (default 1.5)
  - MOONSHOT_REDIS_CONNECT_TIMEOUT_SEC (default 1.5)

Public API (stable)
-------------------
Pure helpers (back-compat):
  - moonshot_limits() -> (sleeve_max_pct, per_trade_pct)
  - can_allocate(equity_usd, current_moonshot_alloc_usd, desired_usd) -> bool

Stateful, recommended:
  - snapshot(equity_usd) -> MoonshotSnapshot               (sync)
  - reserve(equity_usd, amount_usd, *, label="...") -> MoonshotReservation
  - release(reservation_id) -> bool

Async equivalents:
  - await async_snapshot(equity_usd)
  - await async_reserve(equity_usd, amount_usd, label="...")
  - await async_release(reservation_id)
"""

import asyncio
import contextlib
import json
import os
import ssl
import threading
import time
import uuid
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
# Logging (shim-safe; root logging_setup is canonical)
# ──────────────────────────────────────────────────────────────────────────────
try:  # pragma: no cover
    from logging_setup import get_logger  # type: ignore
except Exception:  # pragma: no cover
    import logging

    def get_logger(name: str | None = None):  # type: ignore
        return logging.getLogger(name or "moonshot_budget")


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


def _clamp(x: float, lo: float, hi: float) -> float:
    try:
        return max(float(lo), min(float(hi), float(x)))
    except Exception:
        return float(lo)


# ──────────────────────────────────────────────────────────────────────────────
# Public, back-compat pure helpers
# ──────────────────────────────────────────────────────────────────────────────
def moonshot_limits() -> tuple[float, float]:
    """
    Back-compat: returns (sleeve_max_pct, per_trade_pct).
    """
    sleeve_max = float(_envf("MOONSHOT_SLEEVE_MAX_PCT", 0.10))
    per_trade = float(_envf("MOONSHOT_PER_TRADE_PCT", 0.03))
    # sane clamps
    sleeve_max = _clamp(sleeve_max, 0.0, 1.0)
    per_trade = _clamp(per_trade, 0.0, 1.0)
    return sleeve_max, per_trade


def can_allocate(equity_usd: float, current_moonshot_alloc_usd: float, desired_usd: float) -> bool:
    """
    Back-compat: stateless check against pct caps.
    NOTE: This does NOT reserve; use reserve()/async_reserve() for multi-dyno safety.
    """
    sleeve_max_pct, per_trade_pct = moonshot_limits()
    eq = max(0.0, float(equity_usd or 0.0))
    cur = max(0.0, float(current_moonshot_alloc_usd or 0.0))
    des = max(0.0, float(desired_usd or 0.0))

    # Optional absolute caps
    sleeve_abs = max(0.0, float(_envf("MOONSHOT_SLEEVE_MAX_USD", 0.0)))
    per_abs = max(0.0, float(_envf("MOONSHOT_PER_TRADE_MAX_USD", 0.0)))

    # pct caps converted to USD
    sleeve_cap = sleeve_max_pct * eq
    per_cap = per_trade_pct * eq

    # apply absolute caps if enabled
    if sleeve_abs > 0.0:
        sleeve_cap = min(sleeve_cap, sleeve_abs) if sleeve_cap > 0 else sleeve_abs
    if per_abs > 0.0:
        per_cap = min(per_cap, per_abs) if per_cap > 0 else per_abs

    if des <= 0:
        return True
    if per_cap > 0 and des > per_cap:
        return False
    return not (sleeve_cap > 0 and cur + des > sleeve_cap)


# ──────────────────────────────────────────────────────────────────────────────
# Policy + snapshots
# ──────────────────────────────────────────────────────────────────────────────
@dataclass(frozen=True)
class MoonshotPolicy:
    enabled: bool
    sleeve_max_pct: float
    per_trade_pct: float
    sleeve_max_usd: float
    per_trade_max_usd: float
    min_trade_usd: float
    reserve_ttl_sec: int
    prune_max: int
    namespace: str
    fail_closed: bool

    def caps_usd(self, equity_usd: float) -> tuple[float, float]:
        """
        Returns (sleeve_cap_usd, per_trade_cap_usd).
        """
        eq = max(0.0, float(equity_usd or 0.0))
        sleeve_cap = max(0.0, float(self.sleeve_max_pct)) * eq
        per_cap = max(0.0, float(self.per_trade_pct)) * eq

        if self.sleeve_max_usd > 0.0:
            sleeve_cap = (
                min(sleeve_cap, self.sleeve_max_usd) if sleeve_cap > 0 else self.sleeve_max_usd
            )
        if self.per_trade_max_usd > 0.0:
            per_cap = (
                min(per_cap, self.per_trade_max_usd) if per_cap > 0 else self.per_trade_max_usd
            )

        # If equity unknown/zero, absolute caps (if provided) are the only usable caps.
        return float(sleeve_cap), float(per_cap)


@dataclass(frozen=True)
class MoonshotSnapshot:
    namespace: str
    equity_usd: float
    used_usd: float
    sleeve_cap_usd: float
    per_trade_cap_usd: float
    remaining_usd: float
    enabled: bool

    def as_dict(self) -> dict[str, Any]:
        return {
            "namespace": self.namespace,
            "equity_usd": self.equity_usd,
            "used_usd": self.used_usd,
            "sleeve_cap_usd": self.sleeve_cap_usd,
            "per_trade_cap_usd": self.per_trade_cap_usd,
            "remaining_usd": self.remaining_usd,
            "enabled": self.enabled,
        }


@dataclass(frozen=True)
class MoonshotReservation:
    ok: bool
    reservation_id: str
    reason: str
    requested_usd: float
    used_after_usd: float
    sleeve_cap_usd: float
    per_trade_cap_usd: float
    ttl_sec: int
    namespace: str


# ──────────────────────────────────────────────────────────────────────────────
# Config (hot reload)
# ──────────────────────────────────────────────────────────────────────────────
_CFG_LOCK = threading.Lock()
_CFG_TTL_SEC = max(0.25, _envf("MOONSHOT_REFRESH_SEC", 2.0))
_cfg_at: float = 0.0
_cfg_cache: MoonshotPolicy | None = None


def _policy() -> MoonshotPolicy:
    global _cfg_at, _cfg_cache
    now = time.monotonic()
    with _CFG_LOCK:
        if _cfg_cache is not None and (now - _cfg_at) < _CFG_TTL_SEC:
            return _cfg_cache

        enabled = _envb("MOONSHOT_ENABLED", True)

        sleeve_pct = _clamp(_envf("MOONSHOT_SLEEVE_MAX_PCT", 0.10), 0.0, 1.0)
        per_pct = _clamp(_envf("MOONSHOT_PER_TRADE_PCT", 0.03), 0.0, 1.0)

        sleeve_abs = max(0.0, _envf("MOONSHOT_SLEEVE_MAX_USD", 0.0))
        per_abs = max(0.0, _envf("MOONSHOT_PER_TRADE_MAX_USD", 0.0))

        min_trade = max(0.0, _envf("MOONSHOT_MIN_TRADE_USD", 0.0))

        ttl = max(60, _envi("MOONSHOT_RESERVE_TTL_SEC", 900))
        prune_max = max(10, _envi("MOONSHOT_PRUNE_MAX", 100))

        ns = _envs("MOONSHOT_NAMESPACE", "") or _envs("SERVICE_NAME", "") or "sniper"
        fail_closed = _envb("MOONSHOT_FAIL_CLOSED_ON_STORE_ERRORS", False)

        _cfg_cache = MoonshotPolicy(
            enabled=bool(enabled),
            sleeve_max_pct=float(sleeve_pct),
            per_trade_pct=float(per_pct),
            sleeve_max_usd=float(sleeve_abs),
            per_trade_max_usd=float(per_abs),
            min_trade_usd=float(min_trade),
            reserve_ttl_sec=int(ttl),
            prune_max=int(prune_max),
            namespace=str(ns),
            fail_closed=bool(fail_closed),
        )
        _cfg_at = now
        return _cfg_cache


# ──────────────────────────────────────────────────────────────────────────────
# Redis safety (Upstash-friendly TLS)
# ──────────────────────────────────────────────────────────────────────────────
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


def _redis_ssl_kwargs_if_needed(url_eff: str) -> dict[str, Any]:
    kw: dict[str, Any] = {}
    if url_eff.startswith("rediss://"):
        if certifi is not None:
            kw["ssl_ca_certs"] = certifi.where()
        kw["ssl_cert_reqs"] = ssl.CERT_REQUIRED
        kw["ssl_check_hostname"] = True
    return kw


# ──────────────────────────────────────────────────────────────────────────────
# Store backends
# ──────────────────────────────────────────────────────────────────────────────
class _AsyncRedisStore:
    def __init__(self, url: str):
        if not url or redis is None:
            raise RuntimeError("redis missing or url empty")

        self.url_eff = _upgrade_to_tls_if_needed(url)
        self.url_mask = _mask_url(self.url_eff)

        sock_to = float(_envf("MOONSHOT_REDIS_SOCKET_TIMEOUT_SEC", 1.5))
        conn_to = float(_envf("MOONSHOT_REDIS_CONNECT_TIMEOUT_SEC", 1.5))
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

    async def eval(self, script: str, keys: list[str], args: list[str]) -> Any:
        if self._async is not None:
            return await self._async.eval(script, len(keys), *(keys + args))
        assert self._sync is not None
        return await asyncio.to_thread(self._sync.eval, script, len(keys), *(keys + args))


class _FileStore:
    """
    File fallback. Best-effort; NOT multi-dyno safe.
    Stores:
      - used_usd: float
      - reservations: {rid: {"amt": float, "exp": int}}
    """

    def __init__(self, path: str):
        self.path = path
        self._lock = threading.Lock()
        self._state = self._load()

    def _load(self) -> dict[str, Any]:
        try:
            d = os.path.dirname(self.path)
            if d and not os.path.exists(d):
                os.makedirs(d, exist_ok=True)
            if os.path.exists(self.path):
                with open(self.path, encoding="utf-8") as f:
                    data = json.load(f)
                    if isinstance(data, dict):
                        data.setdefault("used_usd", 0.0)
                        data.setdefault("reservations", {})
                        return data
        except Exception:
            pass
        return {"used_usd": 0.0, "reservations": {}}

    def _save(self) -> None:
        try:
            with open(self.path, "w", encoding="utf-8") as f:
                json.dump(self._state, f)
        except Exception:
            pass

    def _prune(self, prune_max: int) -> float:
        now = int(time.time())
        res: dict[str, Any] = self._state.get("reservations", {})
        if not isinstance(res, dict):
            self._state["reservations"] = {}
            return 0.0

        removed = 0.0
        n = 0
        for rid in list(res.keys()):
            if n >= prune_max:
                break
            try:
                exp = int(res[rid].get("exp", 0))
                if exp > 0 and exp <= now:
                    amt = float(res[rid].get("amt", 0.0) or 0.0)
                    removed += max(0.0, amt)
                    res.pop(rid, None)
                    n += 1
            except Exception:
                continue

        if removed > 0.0:
            try:
                self._state["used_usd"] = max(
                    0.0, float(self._state.get("used_usd", 0.0) or 0.0) - removed
                )
            except Exception:
                self._state["used_usd"] = 0.0
        return removed

    def snapshot_used(self, prune_max: int) -> float:
        with self._lock:
            self._prune(prune_max)
            try:
                used = float(self._state.get("used_usd", 0.0) or 0.0)
            except Exception:
                used = 0.0
            self._save()
            return max(0.0, used)

    def reserve(self, rid: str, amount: float, ttl_sec: int, prune_max: int) -> float:
        with self._lock:
            self._prune(prune_max)
            used = max(0.0, float(self._state.get("used_usd", 0.0) or 0.0))
            res: dict[str, Any] = self._state.setdefault("reservations", {})
            if rid in res:
                # idempotent-ish: do nothing
                return used
            exp = int(time.time()) + int(ttl_sec)
            res[rid] = {"amt": float(amount), "exp": exp}
            used2 = used + max(0.0, float(amount))
            self._state["used_usd"] = used2
            self._save()
            return used2

    def release(self, rid: str, prune_max: int) -> float:
        with self._lock:
            self._prune(prune_max)
            used = max(0.0, float(self._state.get("used_usd", 0.0) or 0.0))
            res: dict[str, Any] = self._state.get("reservations", {})
            amt = 0.0
            if isinstance(res, dict) and rid in res:
                try:
                    amt = float(res[rid].get("amt", 0.0) or 0.0)
                except Exception:
                    amt = 0.0
                res.pop(rid, None)
            used2 = max(0.0, used - max(0.0, amt))
            self._state["used_usd"] = used2
            self._save()
            return used2


# ──────────────────────────────────────────────────────────────────────────────
# Store singleton
# ──────────────────────────────────────────────────────────────────────────────
_STORE_LOCK = threading.Lock()
_STORE: Any = None  # _AsyncRedisStore or _FileStore


def _get_store() -> Any:
    global _STORE
    if _STORE is not None:
        return _STORE

    with _STORE_LOCK:
        if _STORE is not None:
            return _STORE

        if _envb("MOONSHOT_FORCE_FILE", False) or (redis is None):
            path = _envs("MOONSHOT_STATE_FILE", "/var/data/moonshot_budget.json")
            _STORE = _FileStore(path)
            log.every("MOONSHOT_FILESTORE_OK", n=20, msg="MOONSHOT_FILESTORE_OK", path=path)
            return _STORE

        url = _effective_redis_url()
        if not url:
            path = _envs("MOONSHOT_STATE_FILE", "/var/data/moonshot_budget.json")
            _STORE = _FileStore(path)
            log.every("MOONSHOT_FILESTORE_OK", n=20, msg="MOONSHOT_FILESTORE_OK", path=path)
            return _STORE

        try:
            st = _AsyncRedisStore(url)
            _STORE = st
            if _envb("MOONSHOT_REDIS_VALIDATE", False):
                try:
                    loop = asyncio.get_running_loop()
                    loop.create_task(st.validate())
                except Exception:
                    with contextlib.suppress(Exception):
                        asyncio.run(st.validate())
            log.event("MOONSHOT_REDIS_OK", url_mask=st.url_mask)
            return _STORE
        except Exception as e:
            log.every(
                "MOONSHOT_REDIS_FAIL",
                n=10,
                msg="MOONSHOT_REDIS_FAIL",
                err=_short(e),
                url_mask=_mask_url(_upgrade_to_tls_if_needed(url)),
            )
            path = _envs("MOONSHOT_STATE_FILE", "/var/data/moonshot_budget.json")
            _STORE = _FileStore(path)
            log.every("MOONSHOT_FILESTORE_OK", n=20, msg="MOONSHOT_FILESTORE_OK", path=path)
            return _STORE


def _prefix(ns: str) -> str:
    return f"moonshot:v3:{(ns or 'sniper').strip()}"


def _keys(ns: str) -> dict[str, str]:
    p = _prefix(ns)
    return {
        "used": f"{p}:used_usd",  # string float
        "zexp": f"{p}:res_exp",  # zset score=exp_epoch member=rid
        "hamt": f"{p}:res_amt",  # hash rid->amt
    }


# ──────────────────────────────────────────────────────────────────────────────
# Redis scripts (atomic prune + op)
# ──────────────────────────────────────────────────────────────────────────────
# Return format for reserve: {ok_int, used_after, reason_str}
_RESERVE_LUA = r"""
local used_key = KEYS[1]
local zexp_key = KEYS[2]
local hamt_key = KEYS[3]

local now = tonumber(ARGV[1]) or 0
local prune_max = tonumber(ARGV[2]) or 100

local sleeve_cap = tonumber(ARGV[3]) or 0
local per_cap = tonumber(ARGV[4]) or 0
local amt = tonumber(ARGV[5]) or 0

local rid = ARGV[6]
local exp = tonumber(ARGV[7]) or (now + 900)

-- prune expired (bounded)
local expired = redis.call("ZRANGEBYSCORE", zexp_key, "-inf", now, "LIMIT", 0, prune_max)
local removed = 0
for i, id in ipairs(expired) do
  local a = redis.call("HGET", hamt_key, id)
  if a then
    removed = removed + (tonumber(a) or 0)
    redis.call("HDEL", hamt_key, id)
  end
  redis.call("ZREM", zexp_key, id)
end

local used = tonumber(redis.call("GET", used_key) or "0") or 0
if removed > 0 then
  used = used - removed
  if used < 0 then used = 0 end
end

-- per-trade cap check (if cap > 0)
if per_cap > 0 and amt > per_cap then
  redis.call("SET", used_key, tostring(used))
  return {0, used, "per_trade_cap"}
end

-- sleeve cap check (if cap > 0)
if sleeve_cap > 0 and (used + amt) > sleeve_cap then
  redis.call("SET", used_key, tostring(used))
  return {0, used, "sleeve_cap"}
end

-- reserve (idempotent if exists)
local existing = redis.call("HGET", hamt_key, rid)
if existing then
  redis.call("SET", used_key, tostring(used))
  return {1, used, "already_reserved"}
end

used = used + amt
redis.call("SET", used_key, tostring(used))
redis.call("HSET", hamt_key, rid, tostring(amt))
redis.call("ZADD", zexp_key, exp, rid)

return {1, used, "ok"}
"""

# Return format for release: {ok_int, used_after, reason_str}
_RELEASE_LUA = r"""
local used_key = KEYS[1]
local zexp_key = KEYS[2]
local hamt_key = KEYS[3]

local rid = ARGV[1]
local now = tonumber(ARGV[2]) or 0
local prune_max = tonumber(ARGV[3]) or 100

-- prune expired first (bounded)
local expired = redis.call("ZRANGEBYSCORE", zexp_key, "-inf", now, "LIMIT", 0, prune_max)
local removed = 0
for i, id in ipairs(expired) do
  local a = redis.call("HGET", hamt_key, id)
  if a then
    removed = removed + (tonumber(a) or 0)
    redis.call("HDEL", hamt_key, id)
  end
  redis.call("ZREM", zexp_key, id)
end

local used = tonumber(redis.call("GET", used_key) or "0") or 0
if removed > 0 then
  used = used - removed
  if used < 0 then used = 0 end
end

local a = redis.call("HGET", hamt_key, rid)
if not a then
  redis.call("SET", used_key, tostring(used))
  return {0, used, "not_found"}
end

local amt = tonumber(a) or 0
redis.call("HDEL", hamt_key, rid)
redis.call("ZREM", zexp_key, rid)

used = used - amt
if used < 0 then used = 0 end
redis.call("SET", used_key, tostring(used))

return {1, used, "ok"}
"""

# Return used after prune: used_float
_SNAPSHOT_LUA = r"""
local used_key = KEYS[1]
local zexp_key = KEYS[2]
local hamt_key = KEYS[3]

local now = tonumber(ARGV[1]) or 0
local prune_max = tonumber(ARGV[2]) or 100

local expired = redis.call("ZRANGEBYSCORE", zexp_key, "-inf", now, "LIMIT", 0, prune_max)
local removed = 0
for i, id in ipairs(expired) do
  local a = redis.call("HGET", hamt_key, id)
  if a then
    removed = removed + (tonumber(a) or 0)
    redis.call("HDEL", hamt_key, id)
  end
  redis.call("ZREM", zexp_key, id)
end

local used = tonumber(redis.call("GET", used_key) or "0") or 0
if removed > 0 then
  used = used - removed
  if used < 0 then used = 0 end
  redis.call("SET", used_key, tostring(used))
end

return used
"""


# ──────────────────────────────────────────────────────────────────────────────
# Core async operations
# ──────────────────────────────────────────────────────────────────────────────
async def _async_used_usd(ns: str, prune_max: int) -> float:
    st = _get_store()
    if isinstance(st, _FileStore):
        return float(await asyncio.to_thread(st.snapshot_used, prune_max))

    ks = _keys(ns)
    try:
        out = await st.eval(
            _SNAPSHOT_LUA,
            [ks["used"], ks["zexp"], ks["hamt"]],
            [str(int(time.time())), str(int(prune_max))],
        )
        return max(0.0, float(out or 0.0))
    except Exception as e:
        log.every("MOONSHOT_SNAPSHOT_FAIL", n=8, msg="MOONSHOT_SNAPSHOT_FAIL", err=_short(e))
        raise


async def async_snapshot(equity_usd: float) -> MoonshotSnapshot:
    pol = _policy()
    ns = pol.namespace
    sleeve_cap, per_cap = pol.caps_usd(equity_usd)

    used = 0.0
    if pol.enabled:
        try:
            used = await _async_used_usd(ns, pol.prune_max)
        except Exception:
            used = (sleeve_cap if sleeve_cap > 0 else 10**9) if pol.fail_closed else 0.0

    remaining = max(0.0, float(sleeve_cap) - float(used)) if sleeve_cap > 0 else float("inf")

    return MoonshotSnapshot(
        namespace=ns,
        equity_usd=max(0.0, float(equity_usd or 0.0)),
        used_usd=max(0.0, float(used)),
        sleeve_cap_usd=max(0.0, float(sleeve_cap)),
        per_trade_cap_usd=max(0.0, float(per_cap)),
        remaining_usd=float(remaining),
        enabled=bool(pol.enabled),
    )


async def async_reserve(
    equity_usd: float, amount_usd: float, *, label: str = ""
) -> MoonshotReservation:
    pol = _policy()
    ns = pol.namespace
    amt = max(0.0, float(amount_usd or 0.0))
    sleeve_cap, per_cap = pol.caps_usd(equity_usd)

    rid = f"ms_{uuid.uuid4().hex}"
    ttl = int(pol.reserve_ttl_sec)

    if not pol.enabled:
        return MoonshotReservation(True, rid, "disabled", amt, 0.0, sleeve_cap, per_cap, ttl, ns)

    if amt <= 0.0 or (pol.min_trade_usd > 0.0 and amt < pol.min_trade_usd):
        return MoonshotReservation(True, rid, "below_min", amt, 0.0, sleeve_cap, per_cap, ttl, ns)

    # Quick deterministic cap checks (avoid touching store if obviously too large)
    if per_cap > 0.0 and amt > per_cap:
        return MoonshotReservation(
            False, rid, "per_trade_cap", amt, 0.0, sleeve_cap, per_cap, ttl, ns
        )
    if sleeve_cap > 0.0 and amt > sleeve_cap:
        return MoonshotReservation(False, rid, "sleeve_cap", amt, 0.0, sleeve_cap, per_cap, ttl, ns)

    st = _get_store()
    exp = int(time.time()) + ttl
    prune_max = int(pol.prune_max)

    if isinstance(st, _FileStore):
        # file store is best-effort; still apply sleeve check using stored used
        try:
            used_before = float(await asyncio.to_thread(st.snapshot_used, prune_max))
            if sleeve_cap > 0.0 and (used_before + amt) > sleeve_cap:
                return MoonshotReservation(
                    False, rid, "sleeve_cap", amt, used_before, sleeve_cap, per_cap, ttl, ns
                )
            used_after = float(await asyncio.to_thread(st.reserve, rid, amt, ttl, prune_max))
            log.every(
                "MOONSHOT_RESERVE_OK",
                n=12,
                msg="MOONSHOT_RESERVE_OK",
                ns=ns,
                rid=rid,
                amt=round(amt, 4),
                used=round(used_after, 4),
                label=label[:120],
            )
            return MoonshotReservation(
                True, rid, "ok", amt, used_after, sleeve_cap, per_cap, ttl, ns
            )
        except Exception as e:
            log.every("MOONSHOT_RESERVE_FAIL", n=8, msg="MOONSHOT_RESERVE_FAIL", err=_short(e))
            if pol.fail_closed:
                return MoonshotReservation(
                    False, rid, "store_fail", amt, 0.0, sleeve_cap, per_cap, ttl, ns
                )
            return MoonshotReservation(
                True, rid, "store_fail_open", amt, 0.0, sleeve_cap, per_cap, ttl, ns
            )

    ks = _keys(ns)
    try:
        out = await st.eval(
            _RESERVE_LUA,
            [ks["used"], ks["zexp"], ks["hamt"]],
            [
                str(int(time.time())),
                str(prune_max),
                str(float(sleeve_cap)),
                str(float(per_cap)),
                str(float(amt)),
                rid,
                str(int(exp)),
            ],
        )
        ok = int((out[0] if isinstance(out, list | tuple) and len(out) > 0 else 0) or 0) == 1
        used_after = float(
            (out[1] if isinstance(out, list | tuple) and len(out) > 1 else 0.0) or 0.0
        )
        reason = str((out[2] if isinstance(out, list | tuple) and len(out) > 2 else "<?>") or "<?>")

        if ok and reason in {"ok", "already_reserved"}:
            log.every(
                "MOONSHOT_RESERVE_OK",
                n=12,
                msg="MOONSHOT_RESERVE_OK",
                ns=ns,
                rid=rid,
                amt=round(amt, 4),
                used=round(used_after, 4),
                label=label[:120],
            )
        elif not ok:
            log.every(
                "MOONSHOT_RESERVE_BLOCK",
                n=12,
                msg="MOONSHOT_RESERVE_BLOCK",
                ns=ns,
                rid=rid,
                reason=reason,
                amt=round(amt, 4),
                used=round(used_after, 4),
            )

        return MoonshotReservation(ok, rid, reason, amt, used_after, sleeve_cap, per_cap, ttl, ns)
    except Exception as e:
        log.every("MOONSHOT_RESERVE_FAIL", n=8, msg="MOONSHOT_RESERVE_FAIL", err=_short(e))
        if pol.fail_closed:
            return MoonshotReservation(
                False, rid, "store_fail", amt, 0.0, sleeve_cap, per_cap, ttl, ns
            )
        return MoonshotReservation(
            True, rid, "store_fail_open", amt, 0.0, sleeve_cap, per_cap, ttl, ns
        )


async def async_release(reservation_id: str) -> bool:
    pol = _policy()
    ns = pol.namespace
    rid = (reservation_id or "").strip()
    if not rid:
        return False
    if not pol.enabled:
        return True

    st = _get_store()
    prune_max = int(pol.prune_max)

    if isinstance(st, _FileStore):
        try:
            used_after = float(await asyncio.to_thread(st.release, rid, prune_max))
            log.every(
                "MOONSHOT_RELEASE_OK",
                n=12,
                msg="MOONSHOT_RELEASE_OK",
                ns=ns,
                rid=rid,
                used=round(used_after, 4),
            )
            return True
        except Exception as e:
            log.every("MOONSHOT_RELEASE_FAIL", n=8, msg="MOONSHOT_RELEASE_FAIL", err=_short(e))
            return False

    ks = _keys(ns)
    try:
        out = await st.eval(
            _RELEASE_LUA,
            [ks["used"], ks["zexp"], ks["hamt"]],
            [rid, str(int(time.time())), str(prune_max)],
        )
        ok = int((out[0] if isinstance(out, list | tuple) and len(out) > 0 else 0) or 0) == 1
        used_after = float(
            (out[1] if isinstance(out, list | tuple) and len(out) > 1 else 0.0) or 0.0
        )
        reason = str((out[2] if isinstance(out, list | tuple) and len(out) > 2 else "<?>") or "<?>")
        log.every(
            "MOONSHOT_RELEASE_OK" if ok else "MOONSHOT_RELEASE_MISS",
            n=12,
            msg="MOONSHOT_RELEASE_OK" if ok else "MOONSHOT_RELEASE_MISS",
            ns=ns,
            rid=rid,
            reason=reason,
            used=round(used_after, 4),
        )
        return ok
    except Exception as e:
        log.every("MOONSHOT_RELEASE_FAIL", n=8, msg="MOONSHOT_RELEASE_FAIL", err=_short(e))
        return False


# ──────────────────────────────────────────────────────────────────────────────
# Sync wrappers (stable; never block event loop)
# ──────────────────────────────────────────────────────────────────────────────
def snapshot(equity_usd: float) -> MoonshotSnapshot:
    """
    Sync snapshot.
    - If not in an event loop: runs async_snapshot().
    - If in a loop: returns a fast "best-effort" snapshot (async not awaited).
      For strict correctness inside loops, use await async_snapshot().
    """
    try:
        asyncio.get_running_loop()
        in_loop = True
    except Exception:
        in_loop = False

    if not in_loop:
        try:
            return asyncio.run(async_snapshot(equity_usd))
        except Exception:
            pol = _policy()
            sleeve_cap, per_cap = pol.caps_usd(equity_usd)
            used = (sleeve_cap if sleeve_cap > 0 else 10**9) if pol.fail_closed else 0.0
            return MoonshotSnapshot(
                pol.namespace,
                float(equity_usd or 0.0),
                used,
                sleeve_cap,
                per_cap,
                max(0.0, sleeve_cap - used),
                pol.enabled,
            )

    # In loop: do not block. Return computed caps with unknown used=0 (fail-open) or used=sleeve (fail-closed).
    pol = _policy()
    sleeve_cap, per_cap = pol.caps_usd(equity_usd)
    used = sleeve_cap if pol.fail_closed and sleeve_cap > 0 else 0.0
    remaining = max(0.0, sleeve_cap - used) if sleeve_cap > 0 else float("inf")
    return MoonshotSnapshot(
        pol.namespace, float(equity_usd or 0.0), used, sleeve_cap, per_cap, remaining, pol.enabled
    )


def reserve(equity_usd: float, amount_usd: float, *, label: str = "") -> MoonshotReservation:
    """
    Sync reserve.
    - If not in an event loop: blocks and returns actual result.
    - If in a loop: schedules async_reserve and returns an immediate conservative answer:
        * fail_closed -> blocks (ok=False)
        * fail_open   -> ok=True with reason="scheduled"
      Prefer await async_reserve() in async code.
    """
    pol = _policy()
    try:
        asyncio.get_running_loop()
        in_loop = True
    except Exception:
        in_loop = False

    if not in_loop:
        return asyncio.run(async_reserve(equity_usd, amount_usd, label=label))

    rid = f"ms_{uuid.uuid4().hex}"
    sleeve_cap, per_cap = pol.caps_usd(equity_usd)
    amt = max(0.0, float(amount_usd or 0.0))
    ttl = int(pol.reserve_ttl_sec)

    with contextlib.suppress(Exception):
        asyncio.create_task(async_reserve(equity_usd, amount_usd, label=label))

    if pol.fail_closed:
        return MoonshotReservation(
            False, rid, "scheduled_fail_closed", amt, 0.0, sleeve_cap, per_cap, ttl, pol.namespace
        )
    return MoonshotReservation(
        True, rid, "scheduled", amt, 0.0, sleeve_cap, per_cap, ttl, pol.namespace
    )


def release(reservation_id: str) -> bool:
    """
    Sync release.
    - If not in an event loop: blocks and returns actual result.
    - If in a loop: schedules async_release and returns True (best-effort).
      Prefer await async_release() in async code.
    """
    try:
        asyncio.get_running_loop()
        in_loop = True
    except Exception:
        in_loop = False

    if not in_loop:
        try:
            return bool(asyncio.run(async_release(reservation_id)))
        except Exception:
            return False

    with contextlib.suppress(Exception):
        asyncio.create_task(async_release(reservation_id))
    return True


__all__ = [
    # policy/snapshots
    "MoonshotPolicy",
    "MoonshotReservation",
    "MoonshotSnapshot",
    "async_release",
    "async_reserve",
    # async
    "async_snapshot",
    "can_allocate",
    # back-compat
    "moonshot_limits",
    "release",
    "reserve",
    "snapshot",
]
