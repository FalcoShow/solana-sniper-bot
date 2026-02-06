# -*- coding: utf-8 -*-
from __future__ import annotations

"""
utils_pkg/proxy_client.py — Hardened aiohttp client + proxy support (V3+++ LOCK-IN)
===============================================================================

Phase 2 rule:
- NO monkeypatching logger methods (no `log.event = ...`, no attribute assignment).

Features:
- aiohttp session factory w/ safe defaults
- retries (network + retryable statuses) w/ exp backoff, cap, optional jitter
- Retry-After handling for 429
- explicit proxy env compatibility (PF_NO_PROXY, PROXY_*)
- NO_PROXY / no_proxy respected even when explicit proxy is configured
- backwards-compatible wrappers: http_get/http_post
"""

import asyncio
import contextlib
import json as _json
import os
import random
import time
from collections.abc import Mapping
from typing import Any
from urllib.parse import quote, urlsplit

import aiohttp


# ──────────────────────────────────────────────────────────────────────────────
# Logging shim (NO monkeypatch). Lazy-resolves a repo logger if available.
# ──────────────────────────────────────────────────────────────────────────────
class _LogShim:
    __slots__ = ("_supplier", "_last")

    def __init__(self, supplier) -> None:
        self._supplier = supplier
        self._last: dict[str, float] = {}

    def _log(self) -> Any:
        try:
            return self._supplier()
        except Exception:
            return None

    def event(self, name: str, **fields: Any) -> None:
        lg = self._log()
        if lg is None:
            return

        fn = getattr(lg, "event", None)
        if callable(fn):
            with contextlib.suppress(Exception):
                fn(name, **fields)
                return

        info = getattr(lg, "info", None)
        if callable(info):
            with contextlib.suppress(Exception):
                info("%s %s", name, fields)
                return

    def every(self, key: str, n: int = 30, msg: str | None = None, **fields: Any) -> None:
        # throttle: once per N seconds per key
        try:
            now = time.monotonic()
            sec = float(max(1, int(n)))
            last = float(self._last.get(key, 0.0))
            if (now - last) >= sec:
                self._last[key] = now
                self.event(msg or key, **fields)
        except Exception:
            return


_LOGGER: Any | None = None


def _logger_supplier() -> Any:
    global _LOGGER
    if _LOGGER is not None:
        return _LOGGER

    # Prefer compat wrapper (won’t drift across repo)
    try:
        from utils_pkg.logging_setup import get_logger  # type: ignore

        _LOGGER = get_logger(__name__)  # type: ignore
        return _LOGGER
    except Exception:
        pass

    # Fallback to stdlib
    import logging

    _LOGGER = logging.getLogger(__name__)
    return _LOGGER


log = _LogShim(_logger_supplier)


# ──────────────────────────────────────────────────────────────────────────────
# Env helpers
# ──────────────────────────────────────────────────────────────────────────────
_TRUE = {"1", "true", "t", "yes", "y", "on"}
_FALSE = {"0", "false", "f", "no", "n", "off"}


def _env_str(k: str, d: str = "") -> str:
    v = os.getenv(k)
    return d if v is None else str(v).strip()


def _env_int(k: str, d: int) -> int:
    try:
        return int(float(_env_str(k, str(d))))
    except Exception:
        return int(d)


def _env_float(k: str, d: float) -> float:
    try:
        return float(_env_str(k, str(d)))
    except Exception:
        return float(d)


def _env_bool(k: str, d: bool = False) -> bool:
    v = os.getenv(k)
    if v is None:
        return d
    s = str(v).strip().lower()
    if s in _TRUE:
        return True
    if s in _FALSE:
        return False
    return d


def _jitter() -> float:
    return random.random()


def _policy() -> dict[str, Any]:
    # Read env dynamically (helps tests + runtime tuning)
    timeout_total = max(5.0, _env_float("HTTP_TIMEOUT_SEC", 25.0))
    timeout_connect = max(1.0, _env_float("HTTP_CONNECT_TIMEOUT_SEC", 6.0))
    timeout_sock_read = max(1.0, _env_float("HTTP_SOCK_READ_TIMEOUT_SEC", 25.0))

    retries = max(0, _env_int("HTTP_RETRIES", 2))  # attempts = retries + 1
    backoff_base = max(0.05, _env_float("HTTP_BACKOFF_BASE", 0.35))
    backoff_cap = max(0.25, _env_float("HTTP_BACKOFF_CAP", 6.0))
    backoff_jitter = _env_bool("HTTP_BACKOFF_JITTER", True)

    # statuses to retry
    retry_statuses_default = {408, 425, 429, 500, 502, 503, 504}
    rs_env = _env_str("HTTP_RETRY_STATUSES", "").replace(" ", "")
    retry_statuses = {int(x) for x in rs_env.split(",") if x} if rs_env else retry_statuses_default

    # methods to retry
    retry_methods_default = {"GET", "POST", "PUT", "DELETE"}
    rm_env = _env_str("HTTP_RETRY_METHODS", "")
    retry_methods = (
        {m.strip().upper() for m in rm_env.split(",") if m.strip()}
        if rm_env
        else retry_methods_default
    )

    connector_limit = max(20, _env_int("HTTP_CONNECTOR_LIMIT", 80))
    dns_ttl = max(30, _env_int("HTTP_DNS_TTL_SEC", 60))

    user_agent = _env_str("HTTP_USER_AGENT", "pnlsniper")
    trust_env_proxy = _env_bool("HTTP_TRUST_ENV_PROXY", True)

    return {
        "timeout_total": float(timeout_total),
        "timeout_connect": float(timeout_connect),
        "timeout_sock_read": float(timeout_sock_read),
        "retries": int(retries),
        "backoff_base": float(backoff_base),
        "backoff_cap": float(backoff_cap),
        "backoff_jitter": bool(backoff_jitter),
        "retry_statuses": retry_statuses,
        "retry_methods": retry_methods,
        "connector_limit": int(connector_limit),
        "dns_ttl": int(dns_ttl),
        "user_agent": str(user_agent),
        "trust_env_proxy": bool(trust_env_proxy),
    }


# ──────────────────────────────────────────────────────────────────────────────
# Proxy handling (compat with your envs) + NO_PROXY support
# ──────────────────────────────────────────────────────────────────────────────
def _no_proxy_list() -> list[str]:
    raw = _env_str("NO_PROXY", "") or _env_str("no_proxy", "")
    if not raw:
        return []
    parts = [p.strip() for p in raw.replace(";", ",").split(",")]
    return [p for p in parts if p]


def _host_in_no_proxy(host: str) -> bool:
    host = (host or "").strip().lower()
    if not host:
        return False

    for token in _no_proxy_list():
        t = token.strip().lower()
        if not t:
            continue
        if t == "*":
            return True
        if host == t:
            return True
        # domain suffix match: ".example.com" or "example.com"
        if t.startswith(".") and host.endswith(t):
            return True
        if host.endswith("." + t):
            return True
    return False


def _proxy_url_configured() -> str | None:
    """
    Proxy envs (compat):
    - PF_NO_PROXY=1 disables proxy usage entirely
    - PROXY_URL overrides all and uses exactly that URL
    - PROXY_HOSTPORT=host:port
    - PROXY_SCHEME=http|https (default https)
    - PROXY_USER / PROXY_PASS optional auth
    """
    if _env_str("PF_NO_PROXY", "") == "1":
        return None

    direct = _env_str("PROXY_URL", "")
    if direct:
        return direct.strip()

    hp = _env_str("PROXY_HOSTPORT", "")
    if not hp:
        return None

    scheme = (_env_str("PROXY_SCHEME", "https") or "https").strip()
    user = _env_str("PROXY_USER", "")
    pw = _env_str("PROXY_PASS", "")

    # safely escape credentials (avoid breaking URL)
    if user and pw:
        u = quote(user, safe="")
        p = quote(pw, safe="")
        return f"{scheme}://{u}:{p}@{hp}"
    return f"{scheme}://{hp}"


def _proxy_for_url(url: str) -> str | None:
    """
    If a proxy is configured, apply it unless NO_PROXY says to bypass for this host.
    """
    p = _proxy_url_configured()
    if not p:
        return None

    try:
        host = urlsplit(url).hostname or ""
    except Exception:
        host = ""

    if host and _host_in_no_proxy(host):
        return None
    return p


def _redact_url_auth(u: str) -> str:
    # avoid logging proxy creds
    try:
        parts = urlsplit(u)
        if not parts.username and not parts.password:
            return u
        netloc = parts.hostname or ""
        if parts.port:
            netloc = f"{netloc}:{parts.port}"
        scheme = parts.scheme or "http"
        return f"{scheme}://***:***@{netloc}"
    except Exception:
        return u


# ──────────────────────────────────────────────────────────────────────────────
# Session factory
# ──────────────────────────────────────────────────────────────────────────────
def make_session(
    *,
    timeout_sec: float | None = None,
    connector_limit: int | None = None,
    trust_env: bool | None = None,
    headers: Mapping[str, str] | None = None,
) -> aiohttp.ClientSession:
    """
    Return a ClientSession (use: async with make_session() as s: ...)

    Defaults are safe for production:
    - total timeout + connect + sock_read
    - connection pooling
    - trust_env respects HTTPS_PROXY/HTTP_PROXY if enabled
    """
    pol = _policy()

    t_total = float(timeout_sec if timeout_sec is not None else pol["timeout_total"])
    timeout = aiohttp.ClientTimeout(
        total=max(1.0, t_total),
        connect=max(1.0, float(pol["timeout_connect"])),
        sock_read=max(1.0, float(pol["timeout_sock_read"])),
    )

    limit = int(connector_limit if connector_limit is not None else pol["connector_limit"])
    connector = aiohttp.TCPConnector(
        limit=max(1, limit),
        ttl_dns_cache=int(pol["dns_ttl"]),
        enable_cleanup_closed=True,
    )

    base_headers = {
        "user-agent": str(pol["user_agent"]),
        "accept": "*/*",
    }
    if headers:
        base_headers.update({str(k): str(v) for k, v in headers.items()})

    return aiohttp.ClientSession(
        timeout=timeout,
        connector=connector,
        trust_env=bool(pol["trust_env_proxy"] if trust_env is None else trust_env),
        headers=base_headers,
    )


# ──────────────────────────────────────────────────────────────────────────────
# Core request w/ retries
# ──────────────────────────────────────────────────────────────────────────────
async def _sleep_backoff(attempt: int, *, retry_after_s: float | None = None) -> None:
    pol = _policy()

    if retry_after_s is not None and retry_after_s > 0:
        await asyncio.sleep(min(float(retry_after_s), float(pol["backoff_cap"])))
        return

    base = float(pol["backoff_base"]) * (2.0 ** max(0, attempt - 1))
    delay = min(float(pol["backoff_cap"]), base)
    if bool(pol["backoff_jitter"]):
        delay = delay * (0.7 + 0.6 * _jitter())
    await asyncio.sleep(max(0.0, delay))


def _parse_retry_after(headers: Mapping[str, str]) -> float | None:
    ra = headers.get("Retry-After") or headers.get("retry-after")
    if not ra:
        return None
    ra = str(ra).strip()
    # Retry-After can be seconds; we ignore date formats here.
    try:
        return float(ra)
    except Exception:
        return None


async def request_text(
    session: aiohttp.ClientSession,
    method: str,
    url: str,
    *,
    headers: Mapping[str, str] | None = None,
    json: Any = None,
    data: Any = None,
    timeout_sec: float | None = None,
    allow_redirects: bool = True,
    retries: int | None = None,
) -> tuple[int, str]:
    """
    Performs an HTTP request and returns (status, text).

    Retries:
    - network errors / timeouts
    - configured retry statuses (default includes 429, 5xx)
    """
    pol = _policy()
    m = str(method).upper().strip()
    rmax = int(pol["retries"] if retries is None else max(0, int(retries)))

    proxy = _proxy_for_url(url)

    req_timeout: aiohttp.ClientTimeout | None = None
    if timeout_sec is not None:
        req_timeout = aiohttp.ClientTimeout(
            total=max(1.0, float(timeout_sec)),
            connect=max(1.0, float(pol["timeout_connect"])),
            sock_read=max(1.0, float(pol["timeout_sock_read"])),
        )

    last_err: BaseException | None = None

    for attempt in range(0, rmax + 1):
        try:
            req_kw: dict[str, Any] = {
                "allow_redirects": bool(allow_redirects),
            }
            if headers:
                req_kw["headers"] = dict(headers)
            if req_timeout is not None:
                req_kw["timeout"] = req_timeout
            if proxy:
                req_kw["proxy"] = proxy

            # Avoid sending json=None as a real JSON payload ("null")
            if json is not None:
                req_kw["json"] = json
            if data is not None:
                req_kw["data"] = data

            async with session.request(m, url, **req_kw) as r:
                status = int(r.status)
                text = await r.text()

                if (
                    attempt < rmax
                    and (m in pol["retry_methods"])
                    and (status in pol["retry_statuses"])
                ):
                    ra = _parse_retry_after(r.headers) if status == 429 else None
                    log.every(
                        "HTTP_RETRY_STATUS",
                        n=5,
                        msg="HTTP_RETRY_STATUS",
                        method=m,
                        url=url,
                        status=status,
                        attempt=attempt + 1,
                        proxy=_redact_url_auth(proxy) if proxy else None,
                    )
                    await _sleep_backoff(attempt + 1, retry_after_s=ra)
                    continue

                return status, text

        except asyncio.CancelledError:
            raise
        except (TimeoutError, aiohttp.ClientError, OSError) as e:
            last_err = e
            if attempt < rmax and (m in pol["retry_methods"]):
                log.every(
                    "HTTP_RETRY_EXC",
                    n=5,
                    msg="HTTP_RETRY_EXC",
                    method=m,
                    url=url,
                    err=type(e).__name__,
                    attempt=attempt + 1,
                    proxy=_redact_url_auth(proxy) if proxy else None,
                )
                await _sleep_backoff(attempt + 1)
                continue
            break

    if last_err is not None:
        raise last_err
    raise RuntimeError(f"request_text failed: {m} {url}")


async def request_json(
    session: aiohttp.ClientSession,
    method: str,
    url: str,
    *,
    headers: Mapping[str, str] | None = None,
    json: Any = None,
    data: Any = None,
    timeout_sec: float | None = None,
    allow_redirects: bool = True,
    retries: int | None = None,
) -> tuple[int, Any]:
    status, text = await request_text(
        session,
        method,
        url,
        headers=headers,
        json=json,
        data=data,
        timeout_sec=timeout_sec,
        allow_redirects=allow_redirects,
        retries=retries,
    )
    try:
        return status, _json.loads(text)
    except Exception:
        return status, {"_raw": text}


# ──────────────────────────────────────────────────────────────────────────────
# Backwards-compatible wrappers (keep signatures stable)
# ──────────────────────────────────────────────────────────────────────────────
async def http_get(
    session: aiohttp.ClientSession,
    url: str,
    *,
    headers: Mapping[str, str] | None = None,
    timeout: float = 25,
) -> tuple[int, str]:
    return await request_text(
        session,
        "GET",
        url,
        headers=headers,
        timeout_sec=float(timeout),
        retries=None,
    )


async def http_post(
    session: aiohttp.ClientSession,
    url: str,
    *,
    headers: Mapping[str, str] | None = None,
    json: Any = None,
    timeout: float = 25,
) -> tuple[int, str]:
    return await request_text(
        session,
        "POST",
        url,
        headers=headers,
        json=json,
        timeout_sec=float(timeout),
        retries=None,
    )


__all__ = [
    "http_get",
    "http_post",
    "make_session",
    "request_json",
    "request_text",
]
