from __future__ import annotations

"""
utils_pkg/dedupe.py — Canonical Idempotency + Replay Safety (V3+++ / LOCK-IN)
============================================================================

Role
----
This module provides ONE canonical “dedupe / idempotency” layer used across:
- entry (avoid duplicate buys for same candidate),
- routing (avoid duplicate swap submits),
- ledger writes (avoid double-recording),
- workers (avoid double-processing the same queue item).

Design goals
------------
- Deterministic keys (helpers to build stable, short keys)
- Redis-first (cross-process, cross-instance safety)
- Async-safe (redis.asyncio supported; no blocking event loop by default)
- Local fallback (process-only) if Redis absent/unhealthy
- “Inflight token” semantics (optional): claim -> heartbeat -> done/release
- Namespacing to prevent cross-env collisions when sharing Redis
- Strict timeouts so infra never stalls your bot

Backends
--------
- Redis (recommended): uses atomic SET NX EX and (optionally) Lua scripts
- Memory (fallback): process-only dict with monotonic expiry

Environment
-----------
Redis URL detection (first match):
- PF_REDIS_URL
- REDIS_URL
- UPSTASH_REDIS_URL
- UPSTASH_REDIS_REST_URL (NOT supported as redis protocol; ignored)

Namespaces:
- PF_DEDUPE_PREFIX (default "dedupe")
- PF_DEDUPE_NS (default derived from service/app/host)
- PF_DEDUPE_KEY_MAXLEN (default 128)  # hashed if longer

Timeouts:
- PF_REDIS_SOCKET_TIMEOUT_S (default 1.2)
- PF_REDIS_CONNECT_TIMEOUT_S (default 1.2)

Behavior:
- PF_DEDUPE_FORCE_BACKEND = "redis" | "memory" | "auto" (default "auto")
- PF_DEDUPE_DEBUG = 0/1 (default 0)

Compatibility
-------------
Keeps legacy API:
    claim_once(key: str, ttl: int = 600) -> bool

Preferred async API:
    await claim_once_async(key, ttl=600) -> bool

Advanced (optional) inflight lifecycle:
    token = await claim_inflight_async(key, ttl=45)
    await heartbeat_async(key, token, ttl=45)
    await mark_done_async(key, token, ttl=3600, result="...")
    await release_async(key, token)

Key helpers:
    make_key(kind="buy", parts=[wallet, mint, ...]) -> str

Notes
-----
- TTL clamped to >= 1 second.
- "done" keys are useful to prevent reprocessing even after inflight TTL expires.
- If you don’t need lifecycle, just use claim_once/claim_once_async.
"""

import contextlib
import hashlib
import json
import os
import threading
import time
from collections.abc import Iterable
from typing import Any

# -----------------------------------------------------------------------------
# Logging (Phase 2 compliant: NO monkey-patching the logger object)
# -----------------------------------------------------------------------------
try:  # pragma: no cover
    from logging_setup import get_logger  # type: ignore

    _LOG = get_logger(__name__)  # type: ignore
except Exception:  # pragma: no cover
    import logging

    _LOG = logging.getLogger(__name__)  # type: ignore


_TRUE = {"1", "true", "yes", "y", "on", "t"}


def _envs(k: str, d: str = "") -> str:
    v = os.getenv(k)
    return (v if v is not None else d).strip()


def _envb(k: str, d: bool = False) -> bool:
    v = _envs(k, "")
    if not v:
        return bool(d)
    return v.lower().strip() in _TRUE


def _envi(k: str, d: int) -> int:
    try:
        return int(float(_envs(k, str(d))))
    except Exception:
        return int(d)


def _short_exc(e: BaseException, n: int = 220) -> str:
    s = f"{type(e).__name__}: {e}"
    return (s[:n] + "…") if len(s) > n else s


_EVERY_LOCK = threading.Lock()
_EVERY_STORE: dict[str, float] = {}


def _log_event(name: str, **kw: Any) -> None:
    """
    Prefer structured logger methods if present, otherwise .info fallback.
    Never assign/patch methods onto the logger (Phase 2).
    """
    try:
        fn = getattr(_LOG, "event", None)
        if callable(fn):
            fn(name, **kw)
            return
    except Exception:
        pass
    with contextlib.suppress(Exception):
        _LOG.info("%s %s", name, kw)


def _log_every(key: str, n: int = 10, msg: str | None = None, **kw: Any) -> None:
    """
    Throttle logs per-process: at most once every n seconds per key.
    """
    try:
        gap = float(max(1, int(n)))
    except Exception:
        gap = 10.0
    now = time.monotonic()
    with _EVERY_LOCK:
        last = float(_EVERY_STORE.get(key, 0.0))
        if (now - last) < gap:
            return
        _EVERY_STORE[key] = now
    _log_event(msg or key, **kw)


# -----------------------------------------------------------------------------
# Namespace + key shaping
# -----------------------------------------------------------------------------
_PREFIX = (_envs("PF_DEDUPE_PREFIX", "dedupe")).strip().lower()
_NAMESPACE = (
    (
        _envs("PF_DEDUPE_NS")
        or _envs("RENDER_SERVICE_NAME")
        or _envs("FLY_APP_NAME")
        or _envs("DYNO")
        or _envs("HOSTNAME")
        or "default"
    )
    .strip()
    .lower()
)

_KEY_MAXLEN = max(32, min(512, _envi("PF_DEDUPE_KEY_MAXLEN", 128)))


def _rk(user_key: str) -> str:
    # Redis key fully-qualified
    return f"{_PREFIX}:{_NAMESPACE}:{user_key}"


def _sha(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8", "ignore")).hexdigest()


def make_key(*, kind: str, parts: Iterable[Any], salt: str = "") -> str:
    """
    Create a deterministic, short, namespace-stable key.

    Example:
        key = make_key(kind="buy", parts=[wallet, mint, qty_raw, route_name])

    If the raw key would exceed PF_DEDUPE_KEY_MAXLEN, it is replaced with a hash.
    """
    k = (kind or "k").strip().lower()
    segs: list[str] = []
    for p in parts:
        if p is None:
            segs.append("null")
        else:
            s = str(p).strip()
            segs.append(s if s else "empty")

    raw = f"{k}|" + "|".join(segs) + (f"|{salt}" if salt else "")
    if len(raw) <= _KEY_MAXLEN:
        return raw
    return f"{k}|h:{_sha(raw)[:24]}"


# -----------------------------------------------------------------------------
# Redis clients (lazy, cached, thread-safe init)
# -----------------------------------------------------------------------------
_REDIS_LOCK = threading.Lock()
_redis_sync: Any | None = None
_redis_async: Any | None = None


def _redis_url() -> str:
    # Keep EXACT env keys to avoid contract expansion.
    return (_envs("PF_REDIS_URL") or _envs("REDIS_URL") or _envs("UPSTASH_REDIS_URL") or "").strip()


def _redis_timeouts() -> tuple[float, float]:
    def _f(v: str, d: float) -> float:
        try:
            return float(v)
        except Exception:
            return d

    sock = _f(_envs("PF_REDIS_SOCKET_TIMEOUT_S", "1.2") or "1.2", 1.2)
    conn = _f(_envs("PF_REDIS_CONNECT_TIMEOUT_S", "1.2") or "1.2", 1.2)
    sock = max(0.2, min(10.0, sock))
    conn = max(0.2, min(10.0, conn))
    return sock, conn


def _force_backend() -> str:
    v = _envs("PF_DEDUPE_FORCE_BACKEND", "auto").strip().lower()
    if v not in ("auto", "redis", "memory"):
        v = "auto"
    return v


def _debug() -> bool:
    return _envb("PF_DEDUPE_DEBUG", False)


def _build_redis_sync(url: str) -> Any | None:
    try:
        import redis  # type: ignore

        sock, conn = _redis_timeouts()
        return redis.from_url(  # type: ignore[attr-defined]
            url,
            decode_responses=True,
            socket_timeout=sock,
            socket_connect_timeout=conn,
            retry_on_timeout=True,
            health_check_interval=30,
        )
    except Exception as e:
        if _debug():
            _log_event("DEDUPE_REDIS_SYNC_INIT_FAIL", err=_short_exc(e))
        return None


def _build_redis_async(url: str) -> Any | None:
    try:
        import redis.asyncio as redis_a  # type: ignore

        sock, conn = _redis_timeouts()
        return redis_a.from_url(  # type: ignore[attr-defined]
            url,
            decode_responses=True,
            socket_timeout=sock,
            socket_connect_timeout=conn,
            retry_on_timeout=True,
            health_check_interval=30,
        )
    except Exception as e:
        if _debug():
            _log_event("DEDUPE_REDIS_ASYNC_INIT_FAIL", err=_short_exc(e))
        return None


def _get_redis_sync() -> Any | None:
    global _redis_sync

    cached = _redis_sync
    if cached is not None:
        return cached

    url = _redis_url()
    if not url:
        return None

    with _REDIS_LOCK:
        cached = _redis_sync
        if cached is not None:
            return cached

        client = _build_redis_sync(url)
        _redis_sync = client
        return client


def _get_redis_async() -> Any | None:
    global _redis_async

    cached = _redis_async
    if cached is not None:
        return cached

    url = _redis_url()
    if not url:
        return None

    with _REDIS_LOCK:
        cached = _redis_async
        if cached is not None:
            return cached

        client = _build_redis_async(url)
        _redis_async = client
        return client


# -----------------------------------------------------------------------------
# Memory fallback (process-only)
# -----------------------------------------------------------------------------
_mem_seen: dict[str, tuple[float, str]] = {}
_mem_lock = threading.Lock()
_last_cleanup = 0.0
_CLEAN_EVERY = 60.0


def _clamp_ttl(ttl: int | None, default: int = 600) -> int:
    try:
        t = int(ttl if ttl is not None else default)
    except Exception:
        t = int(default)
    return 1 if t <= 0 else t


def _now_mono() -> float:
    return time.monotonic()


def _mem_cleanup(now: float) -> None:
    global _last_cleanup
    if (now - _last_cleanup) < _CLEAN_EVERY:
        return
    dead = [k for k, (exp, _val) in _mem_seen.items() if exp <= now]
    for k in dead:
        _mem_seen.pop(k, None)
    _last_cleanup = now


def _mem_setnx(key: str, val: str, ttl: int) -> bool:
    now = _now_mono()
    with _mem_lock:
        _mem_cleanup(now)
        exp, _ = _mem_seen.get(key, (0.0, ""))
        if exp > now:
            return False
        _mem_seen[key] = (now + float(ttl), val)
        return True


def _mem_get(key: str) -> str | None:
    now = _now_mono()
    with _mem_lock:
        exp, val = _mem_seen.get(key, (0.0, ""))
        if exp <= now:
            _mem_seen.pop(key, None)
            return None
        return val


def _mem_del_if(key: str, expected_val: str) -> bool:
    now = _now_mono()
    with _mem_lock:
        exp, val = _mem_seen.get(key, (0.0, ""))
        if exp <= now:
            _mem_seen.pop(key, None)
            return False
        if val != expected_val:
            return False
        _mem_seen.pop(key, None)
        return True


def _mem_set(key: str, val: str, ttl: int) -> None:
    now = _now_mono()
    with _mem_lock:
        _mem_cleanup(now)
        _mem_seen[key] = (now + float(ttl), val)


def _mem_expire_if(key: str, expected_val: str, ttl: int) -> bool:
    now = _now_mono()
    with _mem_lock:
        exp, val = _mem_seen.get(key, (0.0, ""))
        if exp <= now:
            _mem_seen.pop(key, None)
            return False
        if val != expected_val:
            return False
        _mem_seen[key] = (now + float(ttl), val)
        return True


# -----------------------------------------------------------------------------
# Value encoding for lifecycle semantics
# -----------------------------------------------------------------------------
def _mk_inflight(token: str, ts: int, extra: dict[str, Any] | None = None) -> str:
    payload: dict[str, Any] = {"state": "inflight", "token": token, "ts": ts}
    if extra:
        payload["extra"] = extra
    return json.dumps(payload, separators=(",", ":"), sort_keys=True)


def _mk_done(token: str, ts: int, result: Any | None = None) -> str:
    payload: dict[str, Any] = {"state": "done", "token": token, "ts": ts}
    if result is not None:
        payload["result"] = result
    return json.dumps(payload, separators=(",", ":"), sort_keys=True)


def _parse_val(val: str | None) -> dict[str, Any]:
    if not val:
        return {"state": "missing"}
    try:
        obj = json.loads(val)
        if isinstance(obj, dict) and "state" in obj:
            return obj
    except Exception:
        pass
    return {"state": "claimed", "raw": val}


def _new_token(user_key: str) -> str:
    h = _sha(f"{_NAMESPACE}|{user_key}|{time.time_ns()}")
    return h[:20]


# -----------------------------------------------------------------------------
# Public API — simple claim (legacy + async)
# -----------------------------------------------------------------------------
def claim_once(key: str, ttl: int = 600) -> bool:
    """
    Legacy sync API: claim `key` for ttl seconds. True iff claim succeeds.

    Redis (sync) if available; otherwise process-local fallback.
    """
    ttl_i = _clamp_ttl(ttl, 600)
    user_key = (key or "").strip()
    if not user_key:
        return False

    forced = _force_backend()
    if forced != "memory":
        r = _get_redis_sync()
        if r is not None:
            try:
                ok = r.set(_rk(user_key), "1", ex=ttl_i, nx=True)
                return bool(ok)
            except Exception as e:
                if _debug():
                    _log_event("DEDUPE_REDIS_SYNC_SETNX_FAIL", err=_short_exc(e), key=user_key)

    return _mem_setnx(user_key, "1", ttl_i)


async def claim_once_async(key: str, ttl: int = 600) -> bool:
    """
    Preferred async API: claim `key` for ttl seconds. True iff claim succeeds.

    Redis (async) if available; otherwise process-local fallback.
    """
    ttl_i = _clamp_ttl(ttl, 600)
    user_key = (key or "").strip()
    if not user_key:
        return False

    forced = _force_backend()
    if forced != "memory":
        r = _get_redis_async()
        if r is not None:
            try:
                ok = await r.set(_rk(user_key), "1", ex=ttl_i, nx=True)
                return bool(ok)
            except Exception as e:
                if _debug():
                    _log_event("DEDUPE_REDIS_ASYNC_SETNX_FAIL", err=_short_exc(e), key=user_key)

    return _mem_setnx(user_key, "1", ttl_i)


# -----------------------------------------------------------------------------
# Advanced lifecycle: inflight -> heartbeat -> done/release (async)
# -----------------------------------------------------------------------------
_LUA_RELEASE_IF_MATCH = r"""
if redis.call("GET", KEYS[1]) == ARGV[1] then
  return redis.call("DEL", KEYS[1])
end
return 0
"""

_LUA_EXPIRE_IF_MATCH = r"""
if redis.call("GET", KEYS[1]) == ARGV[1] then
  return redis.call("EXPIRE", KEYS[1], tonumber(ARGV[2]))
end
return 0
"""

_LUA_SET_DONE_IF_MATCH = r"""
if redis.call("GET", KEYS[1]) == ARGV[1] then
  return redis.call("SET", KEYS[1], ARGV[2], "EX", tonumber(ARGV[3]))
end
return 0
"""


async def claim_inflight_async(
    key: str,
    ttl: int = 45,
    *,
    token: str | None = None,
    extra: dict[str, Any] | None = None,
) -> str | None:
    """
    Claim a key as 'inflight'. Returns the token if claimed, else None.
    """
    ttl_i = _clamp_ttl(ttl, 45)
    user_key = (key or "").strip()
    if not user_key:
        return None

    tok = (token or _new_token(user_key)).strip()
    val = _mk_inflight(tok, int(time.time()), extra=extra)

    forced = _force_backend()
    if forced != "memory":
        r = _get_redis_async()
        if r is not None:
            try:
                ok = await r.set(_rk(user_key), val, ex=ttl_i, nx=True)
                return tok if bool(ok) else None
            except Exception as e:
                if _debug():
                    _log_event("DEDUPE_REDIS_ASYNC_INFLIGHT_FAIL", err=_short_exc(e), key=user_key)

    ok2 = _mem_setnx(user_key, val, ttl_i)
    return tok if ok2 else None


async def heartbeat_async(key: str, token: str, ttl: int = 45) -> bool:
    """
    Extend TTL if this process still owns the inflight claim.
    """
    ttl_i = _clamp_ttl(ttl, 45)
    user_key = (key or "").strip()
    tok = (token or "").strip()
    if not user_key or not tok:
        return False

    forced = _force_backend()
    if forced != "memory":
        r = _get_redis_async()
        if r is not None:
            k = _rk(user_key)
            try:
                cur = await r.get(k)
            except Exception:
                cur = None
            if not cur:
                return False

            cur_s = cur.decode() if isinstance(cur, (bytes, bytearray)) else str(cur)
            obj = _parse_val(cur_s)
            if obj.get("token") != tok:
                return False

            try:
                out = await r.eval(_LUA_EXPIRE_IF_MATCH, 1, k, cur_s, ttl_i)
                return bool(out)
            except Exception:
                try:
                    await r.expire(k, ttl_i)
                    return True
                except Exception:
                    return False

    # memory
    current = _mem_get(user_key)
    if not current:
        return False
    obj2 = _parse_val(current)
    if obj2.get("token") != tok:
        return False
    return _mem_expire_if(user_key, current, ttl_i)


async def mark_done_async(
    key: str, token: str, ttl: int = 3600, *, result: Any | None = None
) -> bool:
    """
    Mark a claimed inflight key as 'done' (stores result optionally) with a longer TTL.
    Returns True iff state transitioned by owner.
    """
    ttl_i = _clamp_ttl(ttl, 3600)
    user_key = (key or "").strip()
    tok = (token or "").strip()
    if not user_key or not tok:
        return False

    done_val = _mk_done(tok, int(time.time()), result=result)

    forced = _force_backend()
    if forced != "memory":
        r = _get_redis_async()
        if r is not None:
            k = _rk(user_key)
            try:
                cur = await r.get(k)
            except Exception:
                cur = None
            if not cur:
                return False

            cur_s = cur.decode() if isinstance(cur, (bytes, bytearray)) else str(cur)
            obj = _parse_val(cur_s)
            if obj.get("token") != tok:
                return False

            try:
                out = await r.eval(_LUA_SET_DONE_IF_MATCH, 1, k, cur_s, done_val, ttl_i)
                return bool(out)
            except Exception:
                try:
                    await r.set(k, done_val, ex=ttl_i)
                    return True
                except Exception:
                    return False

    # memory
    cur2 = _mem_get(user_key)
    if not cur2:
        return False
    obj2 = _parse_val(cur2)
    if obj2.get("token") != tok:
        return False
    _mem_set(user_key, done_val, ttl_i)
    return True


async def release_async(key: str, token: str) -> bool:
    """
    Release (delete) a claimed inflight key if owned by token.
    """
    user_key = (key or "").strip()
    tok = (token or "").strip()
    if not user_key or not tok:
        return False

    forced = _force_backend()
    if forced != "memory":
        r = _get_redis_async()
        if r is not None:
            k = _rk(user_key)
            try:
                cur = await r.get(k)
            except Exception:
                cur = None
            if not cur:
                return False

            cur_s = cur.decode() if isinstance(cur, (bytes, bytearray)) else str(cur)
            obj = _parse_val(cur_s)
            if obj.get("token") != tok:
                return False

            try:
                out = await r.eval(_LUA_RELEASE_IF_MATCH, 1, k, cur_s)
                return bool(out)
            except Exception:
                try:
                    await r.delete(k)
                    return True
                except Exception:
                    return False

    # memory
    cur2 = _mem_get(user_key)
    if not cur2:
        return False
    obj2 = _parse_val(cur2)
    if obj2.get("token") != tok:
        return False
    return _mem_del_if(user_key, cur2)


async def get_state_async(key: str) -> dict[str, Any]:
    """
    Inspect current state: {state, token?, ts?, result?} (best effort).
    """
    user_key = (key or "").strip()
    if not user_key:
        return {"state": "missing"}

    forced = _force_backend()
    if forced != "memory":
        r = _get_redis_async()
        if r is not None:
            try:
                v = await r.get(_rk(user_key))
                s = v.decode() if isinstance(v, (bytes, bytearray)) else (str(v) if v else None)
                return _parse_val(s)
            except Exception:
                pass

    return _parse_val(_mem_get(user_key))


async def already_done_async(key: str) -> bool:
    st = await get_state_async(key)
    return st.get("state") == "done"


__all__ = [
    "already_done_async",
    "claim_inflight_async",
    "claim_once",
    "claim_once_async",
    "get_state_async",
    "heartbeat_async",
    "make_key",
    "mark_done_async",
    "release_async",
]
