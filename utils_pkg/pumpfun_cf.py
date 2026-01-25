from __future__ import annotations

"""
utils_pkg/pumpfun_cf.py

Pump.fun cookie/session manager (production-safe)
================================================

This module intentionally does NOT implement anti-bot or Cloudflare bypass.

It provides a clean, deterministic way to:
  - store per-host cookie strings (e.g., from env / file / your own authorized source)
  - apply those cookies to aiohttp sessions
  - track TTL / staleness and refresh in the background

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

This module should remain lightweight; network calls are optional and only used
for the provider refresh if configured.
"""

import asyncio
import json
import os
import time
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlparse

HOST_APEX = "pump.fun"
HOST_API = "frontend-api.pump.fun"
KNOWN_HOSTS = (HOST_APEX, HOST_API)

# -----------------------------
# Logging (canonical if present)
# -----------------------------
try:
    from utils_pkg.logging_setup import get_logger as _get_logger
except Exception:  # pragma: no cover
    try:
        from logging_setup import get_logger as _get_logger  # type: ignore
    except Exception:  # pragma: no cover
        _get_logger = None  # type: ignore

if _get_logger:
    try:
        LOG = _get_logger("utils.pumpfun_cf")  # type: ignore[misc]
    except TypeError:  # pragma: no cover
        LOG = _get_logger()  # type: ignore[misc]
else:
    import logging

    LOG = logging.getLogger("utils.pumpfun_cf")


_TRUE = {"1", "true", "t", "yes", "y", "on"}
_FALSE = {"0", "false", "f", "no", "n", "off"}


def _envs(k: str, d: str = "") -> str:
    return (os.getenv(k) or d).strip()


def _envi(k: str, d: int) -> int:
    try:
        return int(os.getenv(k, str(d)))
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


def _mask_cookie(ck: str) -> str:
    if not ck:
        return ""
    return f"<cookie len={len(ck)}>"


def _parse_cookie_string(s: str) -> dict[str, str]:
    out: dict[str, str] = {}
    for part in (s or "").split(";"):
        part = part.strip()
        if not part or "=" not in part:
            continue
        k, v = part.split("=", 1)
        k = k.strip()
        v = v.strip()
        if k:
            out[k] = v
    return out


@dataclass
class CookieEntry:
    cookie: str
    updated_ts: float
    exp_ts: float
    source: str

    def is_valid(self) -> bool:
        return bool(self.cookie) and _now() < self.exp_ts

    def is_stale(self) -> bool:
        # stale == expired or forced refresh
        return (not self.cookie) or (_now() >= self.exp_ts)


# -----------------------------
# State
# -----------------------------
_LOCK = asyncio.Lock()
_STORE: dict[str, CookieEntry] = {}
_REFRESH_TASK: asyncio.Task | None = None

DEFAULT_TTL_SEC = max(60, _envi("PUMPFUN_COOKIE_TTL_SEC", 900))
CACHE_PATH = _envs("PUMPFUN_COOKIE_CACHE_PATH", "")
AUTO_REFRESH = _envb("PUMPFUN_COOKIE_AUTO_REFRESH", True)
POLL_SEC = max(5, _envi("PUMPFUN_COOKIE_POLL_SEC", 30))

PROVIDER_URL = _envs("PUMPFUN_COOKIE_PROVIDER_URL", "")
PROVIDER_TOKEN = _envs("PUMPFUN_COOKIE_PROVIDER_TOKEN", "")


# -----------------------------
# Cache file helpers
# -----------------------------
def _load_cache_file() -> dict[str, Any]:
    if not CACHE_PATH:
        return {}
    try:
        with open(CACHE_PATH, encoding="utf-8") as f:
            return json.load(f) or {}
    except FileNotFoundError:
        return {}
    except Exception as e:
        LOG.warning("cookie cache read failed (%s): %s", CACHE_PATH, e)
        return {}


def _atomic_write(path: str, data: dict[str, Any]) -> None:
    tmp = f"{path}.tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, separators=(",", ":"))
    os.replace(tmp, path)


def _save_cache_file() -> None:
    if not CACHE_PATH:
        return
    try:
        payload: dict[str, Any] = {"version": 1, "saved_ts": _now(), "cookies": {}}
        for host, entry in _STORE.items():
            payload["cookies"][host] = {
                "cookie": entry.cookie,
                "updated_ts": entry.updated_ts,
                "exp_ts": entry.exp_ts,
                "source": entry.source,
            }
        _atomic_write(CACHE_PATH, payload)
    except Exception as e:
        LOG.warning("cookie cache write failed (%s): %s", CACHE_PATH, e)


def _seed_from_env_unlocked(ttl_sec: int) -> int:
    """
    Seed store from env vars (unlocked). Returns number of updates applied.
    """
    updates = 0
    apex = _envs("PUMPFUN_COOKIE", "")
    api = _envs("PUMPFUN_COOKIE_API", "")

    now = _now()
    if apex:
        _STORE[HOST_APEX] = CookieEntry(
            cookie=apex, updated_ts=now, exp_ts=now + ttl_sec, source="env"
        )
        updates += 1
    if api:
        _STORE[HOST_API] = CookieEntry(
            cookie=api, updated_ts=now, exp_ts=now + ttl_sec, source="env"
        )
        updates += 1
    return updates


def _seed_from_cache_unlocked() -> int:
    """
    Seed store from cache file (unlocked). Returns number of updates applied.
    """
    if not CACHE_PATH:
        return 0
    obj = _load_cache_file()
    cookies = obj.get("cookies") if isinstance(obj, dict) else None
    if not isinstance(cookies, dict):
        return 0

    updates = 0
    for host, rec in cookies.items():
        if not isinstance(host, str) or not isinstance(rec, dict):
            continue
        ck = rec.get("cookie")
        if not isinstance(ck, str) or not ck.strip():
            continue
        updated_ts = float(rec.get("updated_ts") or _now())
        exp_ts = float(rec.get("exp_ts") or (_now() + DEFAULT_TTL_SEC))
        source = str(rec.get("source") or "cache")
        # only accept known hosts (avoid poisoning)
        h = host.strip().lower()
        if h not in KNOWN_HOSTS:
            continue
        # only load if not already newer
        cur = _STORE.get(h)
        if cur is None or updated_ts >= cur.updated_ts:
            _STORE[h] = CookieEntry(
                cookie=ck.strip(), updated_ts=updated_ts, exp_ts=exp_ts, source=source
            )
            updates += 1
    return updates


async def _fetch_from_provider() -> dict[str, Any]:
    """
    Optional external provider fetch.
    Must be authorized and compliant with the upstream site's terms.
    """
    if not PROVIDER_URL:
        return {}

    try:
        import aiohttp  # local import to avoid hard dependency at import-time
    except Exception:
        LOG.warning("cookie provider configured but aiohttp not available")
        return {}

    headers = {}
    if PROVIDER_TOKEN:
        headers["authorization"] = f"Bearer {PROVIDER_TOKEN}"

    timeout = aiohttp.ClientTimeout(total=10)
    async with aiohttp.ClientSession(timeout=timeout) as s:
        async with s.get(PROVIDER_URL, headers=headers) as r:
            if r.status != 200:
                txt = await r.text()
                LOG.warning("cookie provider HTTP %s: %s", r.status, (txt[:200] if txt else ""))
                return {}
            try:
                return await r.json()
            except Exception:
                txt = await r.text()
                try:
                    return json.loads(txt)
                except Exception:
                    return {}


def _apply_provider_payload_unlocked(payload: dict[str, Any]) -> int:
    if not isinstance(payload, dict):
        return 0

    ttl = payload.get("ttl_sec", DEFAULT_TTL_SEC)
    try:
        ttl_sec = max(60, int(ttl))
    except Exception:
        ttl_sec = DEFAULT_TTL_SEC

    cookies = payload.get("cookies", payload)
    if not isinstance(cookies, dict):
        return 0

    now = _now()
    updates = 0
    for host in KNOWN_HOSTS:
        ck = cookies.get(host)
        if isinstance(ck, str) and ck.strip():
            _STORE[host] = CookieEntry(
                cookie=ck.strip(), updated_ts=now, exp_ts=now + ttl_sec, source="provider"
            )
            updates += 1

    return updates


# -----------------------------
# Public API
# -----------------------------
def ensure_cookie_refresher() -> None:
    """
    Start the background refresher if enabled.
    Safe to call multiple times.
    """
    global _REFRESH_TASK
    if not AUTO_REFRESH:
        return
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        # called outside running loop; caller can call later once loop is running
        LOG.warning("ensure_cookie_refresher() called without a running event loop")
        return

    if _REFRESH_TASK and not _REFRESH_TASK.done():
        return

    _REFRESH_TASK = loop.create_task(_refresh_loop(), name="pumpfun_cookie_refresher")
    LOG.info(
        "pumpfun cookie refresher started (poll=%ss cache=%s provider=%s)",
        POLL_SEC,
        bool(CACHE_PATH),
        bool(PROVIDER_URL),
    )


async def refresh_now(*, force: bool = False) -> None:
    """
    Refresh cookies from cache/env/provider (provider optional).
    If force=True, refresh even if existing cookies are still valid.
    """
    async with _LOCK:
        # if not forced and everything valid, no-op
        if not force and all((_STORE.get(h) and _STORE[h].is_valid()) for h in KNOWN_HOSTS):
            return

        updates = 0

        # 1) cache file
        updates += _seed_from_cache_unlocked()

        # 2) env (wins if present)
        updates += _seed_from_env_unlocked(DEFAULT_TTL_SEC)

        # 3) provider (optional)
        if PROVIDER_URL:
            # provider fetch outside lock would be better, but keep simple + consistent
            # to avoid multi-fetch storms.
            payload = await _fetch_from_provider()
            updates += _apply_provider_payload_unlocked(payload)

        if updates:
            _save_cache_file()
            LOG.info(
                "cookies refreshed: apex=%s api=%s",
                _mask_cookie(_STORE.get(HOST_APEX).cookie if _STORE.get(HOST_APEX) else ""),
                _mask_cookie(_STORE.get(HOST_API).cookie if _STORE.get(HOST_API) else ""),
            )


def get_cookie_for(url_or_host: str) -> str:
    """
    Return cookie string for a URL/host, or "" if none.
    """
    host = _host_from_url_or_host(url_or_host)
    if not host:
        return ""

    # normalize subdomains
    if host.endswith(HOST_API):
        key = HOST_API
    elif host.endswith(HOST_APEX):
        key = HOST_APEX
    else:
        key = host

    entry = _STORE.get(key)
    if not entry:
        return ""
    return entry.cookie if entry.is_valid() else ""


def set_cookie_for(
    host: str, cookie_str: str, *, ttl_sec: int | None = None, source: str = "manual"
) -> None:
    """
    Set cookie string for a known host.
    """
    h = (host or "").strip().lower()
    if h not in KNOWN_HOSTS:
        raise ValueError(f"unknown host: {host!r}")

    ck = (cookie_str or "").strip()
    if not ck:
        raise ValueError("cookie_str is empty")

    ttl = DEFAULT_TTL_SEC if ttl_sec is None else max(60, int(ttl_sec))
    now = _now()

    # no async here; keep it simple — called rarely
    _STORE[h] = CookieEntry(cookie=ck, updated_ts=now, exp_ts=now + ttl, source=source)
    _save_cache_file()

    # keep env consistent for other modules if they read it
    if h == HOST_APEX:
        os.environ["PUMPFUN_COOKIE"] = ck
    if h == HOST_API:
        os.environ["PUMPFUN_COOKIE_API"] = ck

    LOG.info("cookie set for %s (%s)", h, source)


def flag_cookie_bad(host: str | None = None) -> None:
    """
    Mark cookies as stale so the refresher will try to refresh ASAP.
    If host is None, stale both known hosts.
    """
    targets = KNOWN_HOSTS if host is None else ((host.strip().lower(),),)
    for h in targets:
        if h in _STORE:
            e = _STORE[h]
            _STORE[h] = CookieEntry(
                cookie=e.cookie, updated_ts=e.updated_ts, exp_ts=0.0, source=e.source
            )
    # no logging spam; callers might invoke frequently


def apply_cookies_to_session(session: Any) -> None:
    """
    Apply cookies to an aiohttp.ClientSession cookie jar.
    Safe no-op if aiohttp isn't present or session isn't compatible.
    """
    try:
        from aiohttp import client_reqrep  # type: ignore
    except Exception:
        return

    jar = getattr(session, "cookie_jar", None)
    if jar is None:
        return

    for host in KNOWN_HOSTS:
        ck = get_cookie_for(host)
        if not ck:
            continue
        jar.update_cookies(
            _parse_cookie_string(ck), response_url=client_reqrep.URL(f"https://{host}/")
        )


def cookie_status() -> dict[str, Any]:
    """
    Debug/status snapshot (safe; masks cookie values).
    """
    out: dict[str, Any] = {}
    for host in KNOWN_HOSTS:
        e = _STORE.get(host)
        out[host] = {
            "present": bool(e and e.cookie),
            "valid": bool(e and e.is_valid()),
            "stale": bool(e and e.is_stale()) if e else True,
            "source": e.source if e else "",
            "updated_age_s": int(_now() - e.updated_ts) if e else None,
            "expires_in_s": int(e.exp_ts - _now()) if e else None,
            "cookie": _mask_cookie(e.cookie) if e else "",
        }
    return out


# -----------------------------
# Background refresher
# -----------------------------
async def _refresh_loop() -> None:
    # One initial seed (cache/env)
    try:
        async with _LOCK:
            _seed_from_cache_unlocked()
            _seed_from_env_unlocked(DEFAULT_TTL_SEC)
    except Exception:
        pass

    while True:
        try:
            # refresh if anything stale/missing
            async with _LOCK:
                need = any((_STORE.get(h) is None) or _STORE[h].is_stale() for h in KNOWN_HOSTS)
            if need:
                await refresh_now(force=True)
        except asyncio.CancelledError:
            break
        except Exception as e:
            LOG.warning("cookie refresh loop error: %s", e)

        await asyncio.sleep(float(POLL_SEC))


__all__ = [
    "HOST_APEX",
    "HOST_API",
    "apply_cookies_to_session",
    "cookie_status",
    "ensure_cookie_refresher",
    "flag_cookie_bad",
    "get_cookie_for",
    "refresh_now",
    "set_cookie_for",
]
