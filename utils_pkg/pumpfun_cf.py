from __future__ import annotations

"""
utils_pkg/pumpfun_cf.py

Pump.fun cookie/session manager (production-safe)
================================================

This module intentionally does NOT implement anti-bot or Cloudflare bypass.

It provides a deterministic way to:
  - store per-host cookie strings (env / cache file / authorized provider)
  - apply those cookies to aiohttp sessions (cookie jar)
  - track TTL / staleness and refresh in the background (optional)

Hosts
-----
- pump.fun (apex)
- frontend-api.pump.fun (api)

Env contract (inputs)
---------------------
- PUMPFUN_COOKIE               cookie string for pump.fun (optional)
- PUMPFUN_COOKIE_API           cookie string for frontend-api.pump.fun (optional)
- PUMPFUN_COOKIE_TTL_SEC       default TTL for cookies (default 900)
- PUMPFUN_COOKIE_CACHE_PATH    optional JSON cache file path
- PUMPFUN_COOKIE_AUTO_REFRESH  0/1 enable background refresh loop (default 1)
- PUMPFUN_COOKIE_POLL_SEC      refresh poll interval (default 30)

Optional authorized provider (advanced; must be legit/authorized)
----------------------------------------------------------------
- PUMPFUN_COOKIE_PROVIDER_URL      GET returns JSON mapping hosts -> cookie strings
- PUMPFUN_COOKIE_PROVIDER_TOKEN    Bearer token (optional)

Provider response formats supported:
  A) {"pump.fun":"...", "frontend-api.pump.fun":"...", "ttl_sec":900}
  B) {"cookies":{"pump.fun":"...", "frontend-api.pump.fun":"..."}, "ttl_sec":900}

Public API
----------
- ensure_cookie_refresher()
- refresh_now()
- get_cookie_for(url_or_host)
- set_cookie_for(host, cookie_str, ttl_sec=None, source="manual")
- flag_cookie_bad(host=None)
- apply_cookies_to_session(aiohttp_session)
- cookie_status()
"""

import asyncio
import importlib
import json
import logging
import os
import threading
import time
from dataclasses import dataclass
from typing import Any, Protocol, cast
from urllib.parse import urlparse

HOST_APEX = "pump.fun"
HOST_API = "frontend-api.pump.fun"
KNOWN_HOSTS: tuple[str, str] = (HOST_APEX, HOST_API)

_TRUE = {"1", "true", "t", "yes", "y", "on"}
_FALSE = {"0", "false", "f", "no", "n", "off"}


# -----------------------------------------------------------------------------
# Logging (repo-compatible: EventLogger preferred, falls back to stdlib logger)
# -----------------------------------------------------------------------------
class _LogLike(Protocol):
    def debug(self, msg: str, *args: Any, **kwargs: Any) -> Any: ...
    def info(self, msg: str, *args: Any, **kwargs: Any) -> Any: ...
    def warning(self, msg: str, *args: Any, **kwargs: Any) -> Any: ...
    def error(self, msg: str, *args: Any, **kwargs: Any) -> Any: ...
    def exception(self, msg: str, *args: Any, **kwargs: Any) -> Any: ...


def _maybe_import_attr(module_name: str, attr: str) -> Any | None:
    try:
        mod = importlib.import_module(module_name)
        return getattr(mod, attr, None)
    except Exception:
        return None


def _build_logger() -> _LogLike:
    """
    Prefer repo logger if present; wrap with EventLogger if available.
    Never throws at import time.
    """
    base: Any = logging.getLogger("utils.pumpfun_cf")

    # Prefer the repo's logger factory (newer compat first).
    for mod_name in ("utils_pkg.log_compat", "utils_pkg.logging_setup", "logging_setup"):
        get_logger = _maybe_import_attr(mod_name, "get_logger")
        if callable(get_logger):
            try:
                base = get_logger("utils.pumpfun_cf")
                break
            except Exception:
                base = logging.getLogger("utils.pumpfun_cf")

    # Wrap with EventLogger if present
    get_event_logger = _maybe_import_attr("utils_pkg.event_logger", "get_event_logger")
    if callable(get_event_logger):
        try:
            wrapped = get_event_logger(base)
            return cast(_LogLike, wrapped)
        except Exception:
            return cast(_LogLike, base)

    return cast(_LogLike, base)


LOG: _LogLike = _build_logger()


# -----------------------------------------------------------------------------
# Env helpers
# -----------------------------------------------------------------------------
def _envs(k: str, d: str = "") -> str:
    return (os.getenv(k) or d).strip()


def _envi(k: str, d: int) -> int:
    v = os.getenv(k)
    if v is None:
        return d
    try:
        return int(float(str(v).strip()))
    except Exception:
        return d


def _envb(k: str, d: bool) -> bool:
    v = os.getenv(k)
    if v is None:
        return d
    s = str(v).strip().lower()
    if s in _TRUE:
        return True
    if s in _FALSE:
        return False
    return d


def _now() -> float:
    return time.time()


def _default_ttl_sec() -> int:
    return max(60, _envi("PUMPFUN_COOKIE_TTL_SEC", 900))


def _poll_sec() -> int:
    return max(5, _envi("PUMPFUN_COOKIE_POLL_SEC", 30))


def _cache_path() -> str:
    return _envs("PUMPFUN_COOKIE_CACHE_PATH", "")


def _auto_refresh() -> bool:
    return _envb("PUMPFUN_COOKIE_AUTO_REFRESH", True)


def _provider_url() -> str:
    return _envs("PUMPFUN_COOKIE_PROVIDER_URL", "")


def _provider_token() -> str:
    return _envs("PUMPFUN_COOKIE_PROVIDER_TOKEN", "")


# -----------------------------------------------------------------------------
# Cookie parsing / host normalization
# -----------------------------------------------------------------------------
def _host_from_url_or_host(url_or_host: str) -> str:
    s = (url_or_host or "").strip()
    if not s:
        return ""
    try:
        u = urlparse(s)
        host = (u.hostname or "").strip().lower()
        if host:
            return host
    except Exception:
        pass
    return s.lower()


def _normalize_host(host: str) -> str:
    h = (host or "").strip().lower()
    if not h:
        return ""
    # Accept subdomains, but normalize to our canonical hosts.
    if h == HOST_API or h.endswith(f".{HOST_API}"):
        return HOST_API
    if h == HOST_APEX or h.endswith(f".{HOST_APEX}"):
        return HOST_APEX
    if h.endswith(HOST_API):
        return HOST_API
    if h.endswith(HOST_APEX):
        return HOST_APEX
    return h


def _mask_cookie(ck: str) -> str:
    ck_s = (ck or "").strip()
    return f"<cookie len={len(ck_s)}>" if ck_s else ""


def _strip_cookie_prefix(s: str) -> str:
    x = (s or "").strip()
    if x.lower().startswith("cookie:"):
        return x.split(":", 1)[1].strip()
    return x


def _parse_cookie_string(s: str) -> dict[str, str]:
    """
    Parse "k=v; a=b" into {"k":"v","a":"b"}.
    Ignores invalid segments.
    """
    out: dict[str, str] = {}
    raw = _strip_cookie_prefix(s)
    for part in raw.split(";"):
        p = part.strip()
        if not p or "=" not in p:
            continue
        k, v = p.split("=", 1)
        k_s = k.strip()
        v_s = v.strip()
        if k_s:
            out[k_s] = v_s
    return out


# -----------------------------------------------------------------------------
# State
# -----------------------------------------------------------------------------
@dataclass(slots=True)
class CookieEntry:
    cookie: str
    updated_ts: float
    exp_ts: float
    source: str

    def is_valid(self) -> bool:
        return bool(self.cookie) and _now() < float(self.exp_ts)

    def is_stale(self) -> bool:
        return (not self.cookie) or (_now() >= float(self.exp_ts))


_STATE_LOCK = threading.RLock()
_STORE: dict[str, CookieEntry] = {}

_REFRESH_TASK: asyncio.Task[None] | None = None

# Provider singleflight (lazy-init lock to keep imports maximally safe)
_PROVIDER_LOCK: asyncio.Lock | None = None
_PROVIDER_INFLIGHT: asyncio.Task[dict[str, Any]] | None = None


def _provider_lock() -> asyncio.Lock:
    global _PROVIDER_LOCK
    if _PROVIDER_LOCK is None:
        _PROVIDER_LOCK = asyncio.Lock()
    return _PROVIDER_LOCK


def _import_optional(module_name: str) -> Any | None:
    """
    Import optional deps without hard dependency / stub issues.
    """
    try:
        return importlib.import_module(module_name)
    except Exception:
        return None


# -----------------------------------------------------------------------------
# Cache file helpers
# -----------------------------------------------------------------------------
def _load_cache_file(path: str) -> dict[str, Any]:
    if not path:
        return {}
    try:
        with open(path, encoding="utf-8") as f:
            obj = json.load(f)
            return obj if isinstance(obj, dict) else {}
    except FileNotFoundError:
        return {}
    except Exception as e:
        LOG.warning("cookie cache read failed (%s): %s", path, e)
        return {}


def _atomic_write(path: str, data: dict[str, Any]) -> None:
    d = os.path.dirname(path)
    if d:
        try:
            os.makedirs(d, exist_ok=True)
        except Exception:
            pass

    tmp = f"{path}.tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, separators=(",", ":"))
    os.replace(tmp, path)


def _save_cache_file_unlocked(path: str) -> None:
    if not path:
        return
    try:
        cookies: dict[str, Any] = {}
        for host, entry in _STORE.items():
            cookies[host] = {
                "cookie": entry.cookie,
                "updated_ts": float(entry.updated_ts),
                "exp_ts": float(entry.exp_ts),
                "source": entry.source,
            }
        payload: dict[str, Any] = {"version": 1, "saved_ts": _now(), "cookies": cookies}
        _atomic_write(path, payload)
    except Exception as e:
        LOG.warning("cookie cache write failed (%s): %s", path, e)


def _seed_from_env_unlocked(ttl_sec: int) -> int:
    updates = 0
    now = _now()

    apex = _strip_cookie_prefix(_envs("PUMPFUN_COOKIE", ""))
    api = _strip_cookie_prefix(_envs("PUMPFUN_COOKIE_API", ""))

    if apex:
        _STORE[HOST_APEX] = CookieEntry(
            cookie=apex, updated_ts=now, exp_ts=now + float(ttl_sec), source="env"
        )
        updates += 1
    if api:
        _STORE[HOST_API] = CookieEntry(
            cookie=api, updated_ts=now, exp_ts=now + float(ttl_sec), source="env"
        )
        updates += 1

    return updates


def _seed_from_cache_unlocked(path: str) -> int:
    if not path:
        return 0

    obj = _load_cache_file(path)
    cookies_raw = obj.get("cookies")
    if not isinstance(cookies_raw, dict):
        return 0

    updates = 0
    for host, rec in cookies_raw.items():
        if not isinstance(host, str) or not isinstance(rec, dict):
            continue
        h = _normalize_host(host)
        if h not in KNOWN_HOSTS:
            continue

        ck = rec.get("cookie")
        if not isinstance(ck, str) or not ck.strip():
            continue
        ck_s = _strip_cookie_prefix(ck).strip()
        if not ck_s:
            continue

        try:
            updated_ts = float(rec.get("updated_ts") or _now())
        except Exception:
            updated_ts = _now()

        try:
            exp_ts = float(rec.get("exp_ts") or (_now() + float(_default_ttl_sec())))
        except Exception:
            exp_ts = _now() + float(_default_ttl_sec())

        source = str(rec.get("source") or "cache")

        cur = _STORE.get(h)
        if cur is None or float(updated_ts) >= float(cur.updated_ts):
            _STORE[h] = CookieEntry(
                cookie=ck_s,
                updated_ts=float(updated_ts),
                exp_ts=float(exp_ts),
                source=source,
            )
            updates += 1

    return updates


def _all_valid_unlocked() -> bool:
    for h in KNOWN_HOSTS:
        e = _STORE.get(h)
        if e is None or not e.is_valid():
            return False
    return True


def _any_stale_unlocked() -> bool:
    for h in KNOWN_HOSTS:
        e = _STORE.get(h)
        if e is None or e.is_stale():
            return True
    return False


# -----------------------------------------------------------------------------
# Provider fetch (optional, authorized)
# -----------------------------------------------------------------------------
def _extract_provider_cookies(payload: dict[str, Any]) -> dict[str, str]:
    """
    Accept either:
      A) {"pump.fun": "...", "frontend-api.pump.fun": "...", ...}
      B) {"cookies": {"pump.fun": "...", ...}, ...}
    """
    if not payload:
        return {}
    cookies_raw = payload.get("cookies")
    if isinstance(cookies_raw, dict):
        out: dict[str, str] = {}
        for k, v in cookies_raw.items():
            if isinstance(k, str) and isinstance(v, str):
                out[k] = v
        return out

    out2: dict[str, str] = {}
    for k, v in payload.items():
        if isinstance(k, str) and isinstance(v, str):
            out2[k] = v
    return out2


async def _fetch_from_provider(url: str, token: str) -> dict[str, Any]:
    if not url:
        return {}

    aiohttp = _import_optional("aiohttp")
    if aiohttp is None:
        LOG.warning("cookie provider configured but aiohttp not available")
        return {}

    headers: dict[str, str] = {}
    if token:
        headers["authorization"] = f"Bearer {token}"

    ClientTimeout = getattr(aiohttp, "ClientTimeout", None)
    ClientSession = getattr(aiohttp, "ClientSession", None)
    if ClientTimeout is None or ClientSession is None:
        LOG.warning("aiohttp missing ClientTimeout/ClientSession")
        return {}

    timeout = ClientTimeout(total=10)
    try:
        async with ClientSession(timeout=timeout) as s:
            async with s.get(url, headers=headers) as r:
                status = int(getattr(r, "status", 0) or 0)
                if status != 200:
                    try:
                        txt = await r.text()
                    except Exception:
                        txt = ""
                    LOG.warning(
                        "cookie provider HTTP %s: %s",
                        status,
                        (txt[:200] if txt else ""),
                    )
                    return {}

                try:
                    js = await r.json()
                    return js if isinstance(js, dict) else {}
                except Exception:
                    try:
                        txt = await r.text()
                    except Exception:
                        txt = ""
                    try:
                        obj = json.loads(txt) if txt else {}
                        return obj if isinstance(obj, dict) else {}
                    except Exception:
                        return {}
    except Exception as e:
        LOG.warning("cookie provider request failed: %s", e)
        return {}


async def _provider_singleflight(url: str, token: str) -> dict[str, Any]:
    """
    Ensure only one provider fetch is inflight per process to avoid storms.
    """
    global _PROVIDER_INFLIGHT
    lock = _provider_lock()

    async with lock:
        if _PROVIDER_INFLIGHT is None or _PROVIDER_INFLIGHT.done():
            _PROVIDER_INFLIGHT = asyncio.create_task(_fetch_from_provider(url, token))
        task = _PROVIDER_INFLIGHT

    try:
        return await task
    except Exception:
        return {}


def _apply_provider_payload_unlocked(payload: dict[str, Any]) -> int:
    if not isinstance(payload, dict) or not payload:
        return 0

    ttl_raw = payload.get("ttl_sec", _default_ttl_sec())
    try:
        ttl_sec = max(60, int(float(str(ttl_raw))))
    except Exception:
        ttl_sec = _default_ttl_sec()

    cookies_map = _extract_provider_cookies(payload)
    if not cookies_map:
        return 0

    now = _now()
    updates = 0
    for host in KNOWN_HOSTS:
        ck = cookies_map.get(host)
        if isinstance(ck, str):
            ck_s = _strip_cookie_prefix(ck).strip()
            if ck_s:
                _STORE[host] = CookieEntry(
                    cookie=ck_s,
                    updated_ts=now,
                    exp_ts=now + float(ttl_sec),
                    source="provider",
                )
                updates += 1
    return updates


# -----------------------------------------------------------------------------
# Public API
# -----------------------------------------------------------------------------
def ensure_cookie_refresher() -> None:
    """
    Start the background refresher if enabled. Safe to call multiple times.
    """
    global _REFRESH_TASK

    if not _auto_refresh():
        return

    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        # Called outside running loop; caller can call again once loop is running.
        LOG.warning("ensure_cookie_refresher() called without a running event loop")
        return

    if _REFRESH_TASK is not None and not _REFRESH_TASK.done():
        return

    _REFRESH_TASK = loop.create_task(_refresh_loop(), name="pumpfun_cookie_refresher")
    LOG.info(
        "pumpfun cookie refresher started (poll=%ss cache=%s provider=%s)",
        _poll_sec(),
        bool(_cache_path()),
        bool(_provider_url()),
    )


async def refresh_now(*, force: bool = False) -> None:
    """
    Refresh cookies from cache/env/provider (provider optional).
    If force=True, refresh even if existing cookies are still valid.
    """
    cache_path = _cache_path()
    ttl = _default_ttl_sec()
    url = _provider_url()
    token = _provider_token()

    need_provider = False
    with _STATE_LOCK:
        if not force and _all_valid_unlocked():
            return

        updates = 0
        if cache_path:
            updates += _seed_from_cache_unlocked(cache_path)
        updates += _seed_from_env_unlocked(ttl)

        need_provider = bool(url) and (force or _any_stale_unlocked())

        if updates and cache_path:
            _save_cache_file_unlocked(cache_path)

    # Provider fetch outside state lock (optional)
    if url and need_provider:
        payload = await _provider_singleflight(url, token)
        if payload:
            with _STATE_LOCK:
                updates2 = _apply_provider_payload_unlocked(payload)
                if updates2 and cache_path:
                    _save_cache_file_unlocked(cache_path)

                if updates2:
                    e_apex = _STORE.get(HOST_APEX)
                    e_api = _STORE.get(HOST_API)
                    LOG.info(
                        "cookies refreshed (provider): apex=%s api=%s",
                        _mask_cookie(e_apex.cookie if e_apex else ""),
                        _mask_cookie(e_api.cookie if e_api else ""),
                    )


def get_cookie_for(url_or_host: str) -> str:
    """
    Return cookie string for a URL/host, or "" if none/expired.
    """
    host = _normalize_host(_host_from_url_or_host(url_or_host))
    if host not in KNOWN_HOSTS:
        return ""
    with _STATE_LOCK:
        entry = _STORE.get(host)
        if not entry or not entry.is_valid():
            return ""
        return entry.cookie


def set_cookie_for(
    host: str,
    cookie_str: str,
    *,
    ttl_sec: int | None = None,
    source: str = "manual",
) -> None:
    """
    Set cookie string for a known host.
    """
    h = _normalize_host(host)
    if h not in KNOWN_HOSTS:
        raise ValueError(f"unknown host: {host!r}")

    ck = _strip_cookie_prefix(cookie_str).strip()
    if not ck:
        raise ValueError("cookie_str is empty")

    ttl = _default_ttl_sec() if ttl_sec is None else max(60, int(ttl_sec))
    now = _now()

    with _STATE_LOCK:
        _STORE[h] = CookieEntry(cookie=ck, updated_ts=now, exp_ts=now + float(ttl), source=source)
        path = _cache_path()
        if path:
            _save_cache_file_unlocked(path)

    # keep env consistent for other modules if they read it
    if h == HOST_APEX:
        os.environ["PUMPFUN_COOKIE"] = ck
    elif h == HOST_API:
        os.environ["PUMPFUN_COOKIE_API"] = ck

    LOG.info("cookie set for %s (%s)", h, source)


def flag_cookie_bad(host: str | None = None) -> None:
    """
    Mark cookies as stale so the refresher will try to refresh ASAP.
    If host is None, stale both known hosts.
    """
    targets = KNOWN_HOSTS if host is None else (host,)
    with _STATE_LOCK:
        for h0 in targets:
            h = _normalize_host(h0)
            if h in _STORE:
                e = _STORE[h]
                _STORE[h] = CookieEntry(
                    cookie=e.cookie,
                    updated_ts=float(e.updated_ts),
                    exp_ts=0.0,
                    source=str(e.source),
                )


def apply_cookies_to_session(session: Any) -> None:
    """
    Apply cookies to an aiohttp.ClientSession cookie jar.
    Safe no-op if aiohttp/yarl isn't present or session isn't compatible.
    """
    jar = getattr(session, "cookie_jar", None)
    if jar is None:
        return

    yarl = _import_optional("yarl")
    if yarl is None:
        return

    URL = getattr(yarl, "URL", None)
    if URL is None:
        return

    for host in KNOWN_HOSTS:
        ck = get_cookie_for(host)
        if not ck:
            continue
        cookies = _parse_cookie_string(ck)
        if not cookies:
            continue
        try:
            jar.update_cookies(cookies, response_url=URL(f"https://{host}/"))
        except Exception:
            continue


def cookie_status() -> dict[str, Any]:
    """
    Debug/status snapshot (safe; masks cookie values).
    """
    out: dict[str, Any] = {}
    now = _now()
    with _STATE_LOCK:
        for host in KNOWN_HOSTS:
            e = _STORE.get(host)
            out[host] = {
                "present": bool(e and e.cookie),
                "valid": bool(e and e.is_valid()),
                "stale": bool(e.is_stale()) if e else True,
                "source": e.source if e else "",
                "updated_age_s": int(now - e.updated_ts) if e else None,
                "expires_in_s": int(e.exp_ts - now) if e else None,
                "cookie": _mask_cookie(e.cookie) if e else "",
            }
    return out


# -----------------------------------------------------------------------------
# Background refresher
# -----------------------------------------------------------------------------
async def _refresh_loop() -> None:
    # initial seed (cache/env)
    try:
        with _STATE_LOCK:
            _seed_from_cache_unlocked(_cache_path())
            _seed_from_env_unlocked(_default_ttl_sec())
    except Exception:
        pass

    while True:
        try:
            with _STATE_LOCK:
                need = _any_stale_unlocked()
            if need:
                await refresh_now(force=True)
        except asyncio.CancelledError:
            break
        except Exception as e:
            LOG.warning("cookie refresh loop error: %s", e)

        await asyncio.sleep(float(_poll_sec()))


__all__ = [
    "HOST_APEX",
    "HOST_API",
    "KNOWN_HOSTS",
    "apply_cookies_to_session",
    "cookie_status",
    "ensure_cookie_refresher",
    "flag_cookie_bad",
    "get_cookie_for",
    "refresh_now",
    "set_cookie_for",
]
