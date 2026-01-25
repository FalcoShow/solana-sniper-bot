# utils_pkg/proxy_client.py
from __future__ import annotations

import asyncio
import contextlib
import json as _json
import os
import random
import time
from collections.abc import Mapping
from typing import Any

import aiohttp

# ──────────────────────────────────────────────────────────────────────────────
# Logging shim (works with your repo style; falls back to stdlib logging)
# ──────────────────────────────────────────────────────────────────────────────
try:
    from logging_setup import get_logger  # type: ignore

    log = get_logger(__name__)  # type: ignore
except Exception:  # pragma: no cover
    import logging

    log = logging.getLogger(__name__)  # type: ignore[assignment]

if not hasattr(log, "event"):

    def _event(name: str, **kw: Any) -> None:  # pragma: no cover
        with contextlib.suppress(Exception):
            log.info("%s %s", name, kw)

    log.event = _event  # type: ignore[attr-defined]

if not hasattr(log, "every"):

    def _every(
        key: str, n: int = 30, msg: str | None = None, **kw: Any
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

    log.every = _every  # type: ignore[attr-defined]


# ──────────────────────────────────────────────────────────────────────────────
# Env knobs (safe defaults)
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


DEFAULT_TIMEOUT_SEC = max(5.0, _env_float("HTTP_TIMEOUT_SEC", 25.0))
CONNECT_TIMEOUT_SEC = max(2.0, _env_float("HTTP_CONNECT_TIMEOUT_SEC", 6.0))
SOCK_READ_TIMEOUT_SEC = max(2.0, _env_float("HTTP_SOCK_READ_TIMEOUT_SEC", 25.0))

HTTP_RETRIES = max(0, _env_int("HTTP_RETRIES", 2))  # total attempts = retries + 1
HTTP_BACKOFF_BASE = max(0.05, _env_float("HTTP_BACKOFF_BASE", 0.35))  # seconds
HTTP_BACKOFF_CAP = max(0.25, _env_float("HTTP_BACKOFF_CAP", 6.0))  # seconds
HTTP_JITTER = _env_bool("HTTP_BACKOFF_JITTER", True)

# statuses that are worth retrying
_RETRY_STATUSES_DEFAULT = {408, 425, 429, 500, 502, 503, 504}
_retry_statuses_env = _env_str("HTTP_RETRY_STATUSES", "")
RETRY_STATUSES = (
    set(int(x) for x in _retry_statuses_env.replace(" ", "").split(",") if x)
    if _retry_statuses_env
    else _RETRY_STATUSES_DEFAULT
)

# methods we retry by default
_RETRY_METHODS_DEFAULT = {"GET", "POST", "PUT", "DELETE"}
_retry_methods_env = _env_str("HTTP_RETRY_METHODS", "")
RETRY_METHODS = (
    set(m.strip().upper() for m in _retry_methods_env.split(",") if m.strip())
    if _retry_methods_env
    else _RETRY_METHODS_DEFAULT
)

USER_AGENT = _env_str("HTTP_USER_AGENT", "pnlsniper")
TRUST_ENV_PROXY = _env_bool("HTTP_TRUST_ENV_PROXY", True)


# Your proxy envs (kept for compatibility)
# PF_NO_PROXY=1 disables proxy usage entirely
# PROXY_HOSTPORT=host:port
# PROXY_USER / PROXY_PASS optional auth
# PROXY_SCHEME=http|https (default https)
# PROXY_URL overrides all and uses exactly that URL
def _proxy_url() -> str | None:
    if _env_str("PF_NO_PROXY", "") == "1":
        return None

    # explicit override
    direct = _env_str("PROXY_URL", "")
    if direct:
        return direct

    hp = _env_str("PROXY_HOSTPORT", "")
    if not hp:
        return None

    scheme = _env_str("PROXY_SCHEME", "https") or "https"
    user, pw = _env_str("PROXY_USER", ""), _env_str("PROXY_PASS", "")
    if user and pw:
        return f"{scheme}://{user}:{pw}@{hp}"
    return f"{scheme}://{hp}"


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
    t_total = float(timeout_sec if timeout_sec is not None else DEFAULT_TIMEOUT_SEC)
    timeout = aiohttp.ClientTimeout(
        total=max(1.0, t_total),
        connect=max(1.0, float(CONNECT_TIMEOUT_SEC)),
        sock_read=max(1.0, float(SOCK_READ_TIMEOUT_SEC)),
    )
    limit = int(
        connector_limit
        if connector_limit is not None
        else max(20, _env_int("HTTP_CONNECTOR_LIMIT", 80))
    )
    connector = aiohttp.TCPConnector(
        limit=limit,
        ttl_dns_cache=max(30, _env_int("HTTP_DNS_TTL_SEC", 60)),
        enable_cleanup_closed=True,
    )

    base_headers = {
        "user-agent": USER_AGENT,
        "accept": "*/*",
    }
    if headers:
        base_headers.update({str(k): str(v) for k, v in headers.items()})

    return aiohttp.ClientSession(
        timeout=timeout,
        connector=connector,
        trust_env=bool(TRUST_ENV_PROXY if trust_env is None else trust_env),
        headers=base_headers,
    )


# ──────────────────────────────────────────────────────────────────────────────
# Core request w/ retries
# ──────────────────────────────────────────────────────────────────────────────
async def _sleep_backoff(attempt: int, *, retry_after_s: float | None = None) -> None:
    if retry_after_s is not None and retry_after_s > 0:
        await asyncio.sleep(min(float(retry_after_s), float(HTTP_BACKOFF_CAP)))
        return

    # exp backoff with cap
    base = float(HTTP_BACKOFF_BASE) * (2.0 ** max(0, attempt - 1))
    delay = min(float(HTTP_BACKOFF_CAP), base)
    if HTTP_JITTER:
        delay = delay * (0.7 + 0.6 * _jitter())
    await asyncio.sleep(max(0.0, delay))


def _parse_retry_after(headers: Mapping[str, str]) -> float | None:
    ra = headers.get("Retry-After") or headers.get("retry-after")
    if not ra:
        return None
    ra = str(ra).strip()
    # Retry-After can be seconds; we ignore date formats here for simplicity.
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
    m = str(method).upper().strip()
    rmax = int(HTTP_RETRIES if retries is None else max(0, int(retries)))

    proxy = _proxy_url()

    # per-request override timeout (optional)
    req_timeout = None
    if timeout_sec is not None:
        req_timeout = aiohttp.ClientTimeout(
            total=max(1.0, float(timeout_sec)),
            connect=max(1.0, float(CONNECT_TIMEOUT_SEC)),
            sock_read=max(1.0, float(SOCK_READ_TIMEOUT_SEC)),
        )

    last_err: BaseException | None = None

    for attempt in range(0, rmax + 1):
        try:
            kw: dict[str, Any] = {
                "headers": dict(headers) if headers else None,
                "timeout": req_timeout,
                "allow_redirects": bool(allow_redirects),
            }
            if proxy:
                kw["proxy"] = proxy

            async with session.request(
                m, url, json=json, data=data, **{k: v for k, v in kw.items() if v is not None}
            ) as r:
                status = int(r.status)
                text = await r.text()

                if (attempt < rmax) and (m in RETRY_METHODS) and (status in RETRY_STATUSES):
                    ra = _parse_retry_after(r.headers) if status == 429 else None
                    log.every(
                        "HTTP_RETRY_STATUS",
                        n=5,
                        msg="HTTP_RETRY_STATUS",
                        method=m,
                        url=url,
                        status=status,
                        attempt=attempt + 1,
                    )
                    await _sleep_backoff(attempt + 1, retry_after_s=ra)
                    continue

                return status, text

        except asyncio.CancelledError:
            raise
        except (TimeoutError, aiohttp.ClientError, OSError) as e:
            last_err = e
            if attempt < rmax and (m in RETRY_METHODS):
                log.every(
                    "HTTP_RETRY_EXC",
                    n=5,
                    msg="HTTP_RETRY_EXC",
                    method=m,
                    url=url,
                    err=type(e).__name__,
                    attempt=attempt + 1,
                )
                await _sleep_backoff(attempt + 1)
                continue
            break

    # final failure
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
# Backwards-compatible wrappers (your code already calls these)
# ──────────────────────────────────────────────────────────────────────────────
async def http_get(session: aiohttp.ClientSession, url: str, *, headers=None, timeout=25):
    return await request_text(
        session, "GET", url, headers=headers, timeout_sec=float(timeout), retries=None
    )


async def http_post(
    session: aiohttp.ClientSession, url: str, *, headers=None, json=None, timeout=25
):
    return await request_text(
        session, "POST", url, headers=headers, json=json, timeout_sec=float(timeout), retries=None
    )


__all__ = [
    "http_get",
    "http_post",
    "make_session",
    "request_json",
    "request_text",
]
