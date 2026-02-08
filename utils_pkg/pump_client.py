from __future__ import annotations

"""
utils_pkg/pump_client.py

Pump.fun HTTP client — V3+++ LOCK-IN (production / Render-safe)
==============================================================

Role
----
Single, canonical HTTP client for Pump.fun “frontend-api” style endpoints used by:
- scanners (pump discovery / metadata)
- execution helpers (when applicable)
- support tooling that needs Pump.fun data

Hard requirements
-----------------
- NO network work at import time
- Multi-base failover (PUMPFUN_API_BASES) with retries + backoff
- Deterministic errors (include per-attempt status/exception context)
- Cloudflare / bot-wall fallback via browser client for POST (and optional GET)
- Backwards compatible entrypoints:
    - pump_get(path, timeout=..)-> str
    - pump_post(path, json_body, timeout=..)-> str

Optional ergonomics
-------------------
- pump_get_json / pump_post_json parse JSON safely
- request tags for logs
"""

import asyncio
import contextlib
import json
import logging
import os
import random
import time
from collections.abc import AsyncIterator, Callable, Mapping
from contextlib import asynccontextmanager
from dataclasses import asdict, dataclass
from typing import Any, Final

try:
    import aiohttp
except Exception:  # pragma: no cover
    aiohttp = None  # type: ignore[assignment]


# -----------------------------------------------------------------------------
# Logging (NO monkeypatching; safe shim)
# -----------------------------------------------------------------------------
class _LogShim:
    """
    Tiny logger adapter that:
      - never monkeypatches a logger
      - uses .event/.every if present, else falls back to info/warning/error
    """

    __slots__ = ("_supplier", "_every_store")

    def __init__(self, supplier: Callable[[], Any]) -> None:
        self._supplier = supplier
        self._every_store: dict[str, float] = {}

    def _log(self) -> Any:
        with contextlib.suppress(Exception):
            return self._supplier()
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

    def every(self, key: str, n: int = 30, msg: str | None = None, **fields: Any) -> None:
        try:
            now = time.monotonic()
            sec = float(max(1, int(n)))
            last = float(self._every_store.get(key, 0.0))
            if (now - last) >= sec:
                self._every_store[key] = now
                self.event(msg or key, **fields)
        except Exception:
            return

    def info(self, msg: str, *a: Any) -> None:
        lg = self._log()
        if lg is None:
            return
        fn = getattr(lg, "info", None)
        if callable(fn):
            with contextlib.suppress(Exception):
                fn(msg, *a)

    def warning(self, msg: str, *a: Any) -> None:
        lg = self._log()
        if lg is None:
            return
        fn = getattr(lg, "warning", None)
        if callable(fn):
            with contextlib.suppress(Exception):
                fn(msg, *a)

    def error(self, msg: str, *a: Any) -> None:
        lg = self._log()
        if lg is None:
            return
        fn = getattr(lg, "error", None)
        if callable(fn):
            with contextlib.suppress(Exception):
                fn(msg, *a)


_DEFAULT_LOGGER: Any | None = None


def _resolve_logger() -> Any:
    """
    Prefer canonical log_compat; fallback to logging_setup; then stdlib.
    """
    global _DEFAULT_LOGGER
    if _DEFAULT_LOGGER is not None:
        return _DEFAULT_LOGGER

    with contextlib.suppress(Exception):
        from utils_pkg.log_compat import get_logger as gl  # type: ignore

        _DEFAULT_LOGGER = gl("utils.pump_client")
        return _DEFAULT_LOGGER

    with contextlib.suppress(Exception):
        from utils_pkg.logging_setup import get_logger as gl2  # type: ignore

        try:
            _DEFAULT_LOGGER = gl2("utils.pump_client")
        except TypeError:
            _DEFAULT_LOGGER = gl2()
        return _DEFAULT_LOGGER

    lg = logging.getLogger("utils.pump_client")
    if not lg.handlers:
        logging.basicConfig(level=logging.INFO)
    _DEFAULT_LOGGER = lg
    return _DEFAULT_LOGGER


LOG = _LogShim(_resolve_logger)


# -----------------------------------------------------------------------------
# Soft imports (keep compatible, never do network at import)
# -----------------------------------------------------------------------------
def _safe_headers() -> dict[str, str]:
    """
    Pump headers provider (best-effort). Must never throw.
    """
    try:
        from utils_pkg.pumpfun_headers import pumpfun_headers

        h = pumpfun_headers() or {}
        if isinstance(h, dict):
            return {str(k): str(v) for k, v in h.items()}
    except Exception:
        pass

    return {
        "accept": "application/json, text/plain, */*",
        "content-type": "application/json",
        "user-agent": os.getenv("PUMPFUN_UA", "pnlsniper"),
    }


ProxyMakeSession = Callable[..., Any]

_make_session: ProxyMakeSession | None = None
_request_text: Callable[..., Any] | None = None
_http_get: Callable[..., Any] | None = None
_http_post: Callable[..., Any] | None = None

with contextlib.suppress(Exception):
    from utils_pkg.proxy_client import make_session as _ms  # type: ignore

    _make_session = _ms

with contextlib.suppress(Exception):
    from utils_pkg.proxy_client import request_text as _rt  # type: ignore

    _request_text = _rt

with contextlib.suppress(Exception):
    from utils_pkg.proxy_client import http_get as _hg  # type: ignore

    _http_get = _hg

with contextlib.suppress(Exception):
    from utils_pkg.proxy_client import http_post as _hp  # type: ignore

    _http_post = _hp

try:
    from utils_pkg.pump_browser_client import pump_post_via_browser  # type: ignore
except Exception:  # pragma: no cover
    pump_post_via_browser = None  # type: ignore[assignment]

try:
    from utils_pkg.pump_browser_client import pump_get_via_browser  # type: ignore
except Exception:  # pragma: no cover
    pump_get_via_browser = None  # type: ignore[assignment]


# -----------------------------------------------------------------------------
# Env helpers (mypy-safe)
# -----------------------------------------------------------------------------
_TRUE: Final[set[str]] = {"1", "true", "t", "yes", "y", "on"}
_FALSE: Final[set[str]] = {"0", "false", "f", "no", "n", "off"}


def _env_bool(name: str, default: bool = False) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    s = v.strip().lower()
    if s in _TRUE:
        return True
    if s in _FALSE:
        return False
    return default


def _env_int(name: str, default: int) -> int:
    v = os.getenv(name)
    if v is None:
        return default
    s = v.strip()
    if not s:
        return default
    try:
        return int(float(s))
    except Exception:
        return default


def _env_float(name: str, default: float) -> float:
    v = os.getenv(name)
    if v is None:
        return default
    s = v.strip()
    if not s:
        return default
    try:
        return float(s)
    except Exception:
        return default


def _env_csv(name: str, default: str) -> list[str]:
    raw = os.getenv(name, default)
    parts = [p.strip() for p in str(raw).split(",")]
    return [p for p in parts if p]


def _in_pytest() -> bool:
    return bool(os.getenv("PYTEST_CURRENT_TEST"))


def _no_sleep_mode() -> bool:
    """
    Prevent test hangs: never sleep/backoff when pytest is running or SAFE_MODE/NO_NETWORK.
    """
    if _in_pytest():
        return True
    if (
        _env_bool("SAFE_MODE", False)
        or _env_bool("NO_NETWORK", False)
        or _env_bool("DISABLE_NETWORK", False)
    ):
        return True
    if os.getenv("NETWORK", "").strip().lower() in _FALSE:
        return True
    return False


# -----------------------------------------------------------------------------
# Config (keep keys stable)
# -----------------------------------------------------------------------------
DEFAULT_BASES: Final[list[str]] = ["https://frontend-api.pump.fun"]

# Note: we keep exported names stable, but resolve bases via _api_bases() for runtime updates.
PUMPFUN_API_BASES = _env_csv("PUMPFUN_API_BASES", ",".join(DEFAULT_BASES))
PUMPFUN_RETRIES = max(1, _env_int("PUMPFUN_HTTP_RETRIES", 2))  # rounds
PUMPFUN_BACKOFF_S = max(0.05, _env_float("PUMPFUN_HTTP_BACKOFF_S", 0.6))
PUMPFUN_JITTER_S = max(0.0, _env_float("PUMPFUN_HTTP_JITTER_S", 0.25))
PUMPFUN_TIMEOUT_S = max(3, _env_int("PUMPFUN_HTTP_TIMEOUT_S", 25))

# CF / bot-wall fallback
PUMPFUN_BROWSER_FALLBACK = _env_bool("PUMPFUN_BROWSER_FALLBACK", True)
PUMPFUN_BROWSER_FALLBACK_ON_GET = _env_bool("PUMPFUN_BROWSER_FALLBACK_ON_GET", False)

# Response size guard (avoid memory bombs)
PUMPFUN_MAX_BODY_BYTES = max(64_000, _env_int("PUMPFUN_MAX_BODY_BYTES", 2_000_000))


def _api_bases() -> list[str]:
    bases = _env_csv("PUMPFUN_API_BASES", ",".join(DEFAULT_BASES))

    one = (os.getenv("PUMPFUN_API_BASE") or "").strip()
    if one:
        bases = [one] + [b for b in bases if b != one]

    out: list[str] = []
    for b in bases:
        b2 = b.strip().rstrip("/")
        if not b2:
            continue
        if not (b2.startswith("http://") or b2.startswith("https://")):
            continue
        out.append(b2)

    return out or DEFAULT_BASES[:]


def _join(base: str, path: str) -> str:
    if path.startswith("http://") or path.startswith("https://"):
        return path
    p = path if path.startswith("/") else "/" + path
    return base.rstrip("/") + p


# -----------------------------------------------------------------------------
# Errors
# -----------------------------------------------------------------------------
@dataclass(frozen=True)
class Attempt:
    base: str
    url: str
    status: int
    err: str
    took_ms: int


class PumpClientError(RuntimeError):
    pass


class PumpHTTPError(PumpClientError):
    def __init__(self, msg: str, *, attempts: list[Attempt]) -> None:
        super().__init__(msg)
        self.attempts = attempts

    def as_dict(self) -> dict[str, Any]:
        return {"message": str(self), "attempts": [asdict(a) for a in self.attempts]}


# -----------------------------------------------------------------------------
# Session provider (proxy_client preferred, aiohttp fallback)
# -----------------------------------------------------------------------------
@asynccontextmanager
async def _aiohttp_session_cm() -> AsyncIterator[Any]:
    if aiohttp is None:
        raise RuntimeError("aiohttp is not available, and proxy_client.make_session is missing")
    timeout = aiohttp.ClientTimeout(total=float(PUMPFUN_TIMEOUT_S))
    async with aiohttp.ClientSession(timeout=timeout) as s:
        yield s


def _session_cm() -> Any:
    # proxy_client.make_session returns aiohttp.ClientSession-like async CM
    if _make_session is not None:
        return _make_session()
    return _aiohttp_session_cm()


def _clip_body_bytes(body: str, max_bytes: int) -> str:
    if not body:
        return ""
    try:
        b = body.encode("utf-8", "ignore")
        if len(b) <= max_bytes:
            return body
        return b[:max_bytes].decode("utf-8", "ignore")
    except Exception:
        return body[: max(1, min(len(body), max_bytes))]


async def _read_text_limited(resp: Any, max_bytes: int) -> str:
    """
    Read response body with a hard cap to prevent memory bombs.
    """
    try:
        data: bytes = await resp.content.read(max_bytes + 1)
        if len(data) > max_bytes:
            data = data[:max_bytes]
        return data.decode("utf-8", "ignore")
    except Exception:
        with contextlib.suppress(Exception):
            txt = await resp.text()
            return _clip_body_bytes(str(txt), max_bytes)
    return ""


# -----------------------------------------------------------------------------
# Low-level HTTP shims
# -----------------------------------------------------------------------------
async def _aiohttp_get(
    session: Any, url: str, *, headers: Mapping[str, str], timeout: int
) -> tuple[int, str]:
    async with session.get(url, headers=dict(headers), timeout=timeout) as resp:
        body = await _read_text_limited(resp, PUMPFUN_MAX_BODY_BYTES)
        return int(resp.status), body


async def _aiohttp_post(
    session: Any,
    url: str,
    *,
    headers: Mapping[str, str],
    json_body: Mapping[str, Any],
    timeout: int,
) -> tuple[int, str]:
    async with session.post(
        url, headers=dict(headers), json=dict(json_body), timeout=timeout
    ) as resp:
        body = await _read_text_limited(resp, PUMPFUN_MAX_BODY_BYTES)
        return int(resp.status), body


def _is_retryable_status(status: int) -> bool:
    if status in (408, 409, 425, 429):
        return True
    return 500 <= status <= 599


def _looks_like_cf_block(status: int, body: str) -> bool:
    if status in (403, 503):
        b = (body or "").lower()
        return any(
            x in b for x in ("cloudflare", "cf-ray", "attention required", "challenge", "captcha")
        )
    return False


def _trim_body(body: str) -> str:
    if not body:
        return ""
    if len(body) <= 600:
        return body
    return body[:600] + "…"


def _sleep_backoff(round_i: int) -> float:
    # deterministic-ish backoff + bounded jitter
    base = float(PUMPFUN_BACKOFF_S) * float(max(1, round_i))
    jitter = random.uniform(0.0, float(PUMPFUN_JITTER_S))
    return max(0.0, base + jitter)


async def _maybe_sleep(seconds: float) -> None:
    if seconds <= 0:
        return
    if _no_sleep_mode():
        return
    await asyncio.sleep(seconds)


def _stringify_any(obj: Any) -> str:
    """
    Convert unknown browser/proxy output into a string safely.
    Never raises.
    """
    try:
        if obj is None:
            return ""
        if isinstance(obj, str):
            return obj
        if isinstance(obj, (bytes, bytearray)):
            return bytes(obj).decode("utf-8", "ignore")
        return json.dumps(obj, ensure_ascii=False, separators=(",", ":"), default=str)
    except Exception:
        with contextlib.suppress(Exception):
            return str(obj)
        return ""


# -----------------------------------------------------------------------------
# Core request
# -----------------------------------------------------------------------------
async def _do_request(
    session: Any,
    method_u: str,
    url: str,
    *,
    headers: dict[str, str],
    json_body: dict[str, Any] | None,
    timeout: int,
) -> tuple[int, str]:
    """
    One network attempt. Uses proxy_client.request_text when available (with retries=0)
    to avoid double-retry amplification (pump_client handles retries deterministically).
    """
    if callable(_request_text):
        status, text = await _request_text(  # type: ignore[misc]
            session,
            method_u,
            url,
            headers=headers,
            json=json_body if method_u == "POST" else None,
            timeout_sec=float(timeout),
            allow_redirects=True,
            retries=0,
        )
        return int(status), _clip_body_bytes(_stringify_any(text), PUMPFUN_MAX_BODY_BYTES)

    if method_u == "GET":
        if callable(_http_get):
            status_text = await _http_get(session, url, headers=headers, timeout=timeout)  # type: ignore[misc]
            return int(status_text[0]), _clip_body_bytes(
                _stringify_any(status_text[1]), PUMPFUN_MAX_BODY_BYTES
            )
        return await _aiohttp_get(session, url, headers=headers, timeout=timeout)

    # POST
    if callable(_http_post):
        status_text = await _http_post(  # type: ignore[misc]
            session,
            url,
            headers=headers,
            json=json_body or {},
            timeout=timeout,
        )
        return int(status_text[0]), _clip_body_bytes(
            _stringify_any(status_text[1]), PUMPFUN_MAX_BODY_BYTES
        )

    return await _aiohttp_post(
        session, url, headers=headers, json_body=json_body or {}, timeout=timeout
    )


async def _request(
    method: str,
    path: str,
    *,
    json_body: dict[str, Any] | None = None,
    timeout: int | None = None,
    headers: dict[str, str] | None = None,
    tag: str = "",
    allow_browser_fallback: bool = True,
) -> str:
    """
    Returns response body as text. Raises PumpHTTPError on failure.
    """
    method_u = method.upper().strip()
    if method_u not in ("GET", "POST"):
        raise ValueError(f"Unsupported method: {method}")

    tmo = int(timeout or PUMPFUN_TIMEOUT_S)
    hdrs: dict[str, str] = dict(_safe_headers())
    if headers:
        hdrs.update({str(k): str(v) for k, v in headers.items()})

    bases = _api_bases()
    attempts: list[Attempt] = []

    last_body = ""
    last_status = -1
    saw_cf_hint = False

    # One session per request for efficiency + consistent behavior.
    async with _session_cm() as s:
        rounds = int(PUMPFUN_RETRIES)
        if _no_sleep_mode():
            rounds = 1  # do not amplify retries in tests/safe-mode

        for round_i in range(1, rounds + 1):
            for base in bases:
                url = _join(base, path)
                t0 = time.monotonic()

                try:
                    status, body = await _do_request(
                        s, method_u, url, headers=hdrs, json_body=json_body, timeout=tmo
                    )

                    took_ms = int((time.monotonic() - t0) * 1000)
                    last_status, last_body = int(status), str(body)

                    if int(status) == 200:
                        if tag:
                            LOG.info("pump_client %s ok %s (%dms)", tag, url, took_ms)
                        return str(body)

                    if _looks_like_cf_block(int(status), str(body)):
                        saw_cf_hint = True

                    attempts.append(
                        Attempt(
                            base=base,
                            url=url,
                            status=int(status),
                            err=_trim_body(str(body)),
                            took_ms=took_ms,
                        )
                    )

                    if _is_retryable_status(int(status)):
                        continue

                    # non-retryable: don't waste retries across bases for this round
                    break

                except asyncio.CancelledError:
                    raise
                except (asyncio.TimeoutError, TimeoutError) as e:
                    took_ms = int((time.monotonic() - t0) * 1000)
                    attempts.append(
                        Attempt(
                            base=base,
                            url=url,
                            status=-1,
                            err=f"{type(e).__name__}({tmo}s)",
                            took_ms=took_ms,
                        )
                    )
                    last_status, last_body = -1, "timeout"
                    continue
                except Exception as e:
                    took_ms = int((time.monotonic() - t0) * 1000)
                    attempts.append(
                        Attempt(
                            base=base,
                            url=url,
                            status=-1,
                            err=f"{type(e).__name__}: {e}",
                            took_ms=took_ms,
                        )
                    )
                    last_status, last_body = -1, f"{type(e).__name__}: {e}"
                    continue

            if round_i < rounds:
                await _maybe_sleep(_sleep_backoff(round_i))

    # Browser fallback (last resort)
    if allow_browser_fallback and PUMPFUN_BROWSER_FALLBACK:
        is_get = method_u == "GET"
        may_try = (not is_get) or (is_get and PUMPFUN_BROWSER_FALLBACK_ON_GET)

        trigger = saw_cf_hint or last_status in (403, 503, -1)
        if may_try and trigger:
            try:
                if method_u == "POST":
                    if pump_post_via_browser is None:
                        raise RuntimeError("pump_post_via_browser not available")
                    LOG.warning("pump_client %s browser_fallback POST %s", tag or "-", path)

                    # IMPORTANT: cast result to Any to avoid mypy "unreachable" on type-narrowing.
                    out_any: Any = await pump_post_via_browser(path, json_body or {})
                    text = _stringify_any(out_any)
                    return _clip_body_bytes(text, PUMPFUN_MAX_BODY_BYTES)

                if pump_get_via_browser is None:
                    raise RuntimeError("pump_get_via_browser not available")
                LOG.warning("pump_client %s browser_fallback GET %s", tag or "-", path)

                out_any2: Any = await pump_get_via_browser(path)
                text2 = _stringify_any(out_any2)
                return _clip_body_bytes(text2, PUMPFUN_MAX_BODY_BYTES)

            except Exception as e:
                attempts.append(
                    Attempt(
                        base="browser",
                        url=f"browser:{path}",
                        status=-1,
                        err=f"{type(e).__name__}: {e}",
                        took_ms=0,
                    )
                )

    msg = (
        f"{method_u} failed for {path} across bases "
        f"(last_status={last_status}, last_body={_trim_body(last_body)})"
    )
    raise PumpHTTPError(msg, attempts=attempts)


# -----------------------------------------------------------------------------
# Public API (backwards compatible)
# -----------------------------------------------------------------------------
async def pump_get(path: str, *, timeout: int = 25) -> str:
    """
    Backwards compatible raw GET.
    Returns text body, raises PumpHTTPError on failure.
    """
    return await _request("GET", path, timeout=timeout, tag="get", allow_browser_fallback=True)


async def pump_post(path: str, json_body: dict[str, Any], timeout: int = 30) -> str:
    """
    Backwards compatible raw POST.
    Returns text body, raises PumpHTTPError on failure.
    """
    return await _request(
        "POST",
        path,
        json_body=json_body,
        timeout=int(timeout),
        tag="post",
        allow_browser_fallback=True,
    )


# -----------------------------------------------------------------------------
# Convenience helpers
# -----------------------------------------------------------------------------
def _safe_json_loads(txt: str) -> Any:
    t = (txt or "").strip()
    if not t:
        return {}
    return json.loads(t)


async def pump_get_json(path: str, *, timeout: int = 25) -> Any:
    body = await pump_get(path, timeout=timeout)
    try:
        return _safe_json_loads(body)
    except Exception as e:
        raise PumpClientError(
            f"GET json decode failed for {path}: {type(e).__name__}: {e} body={_trim_body(body)}"
        ) from e


async def pump_post_json(path: str, json_body: dict[str, Any], *, timeout: int = 30) -> Any:
    body = await pump_post(path, json_body, timeout=timeout)
    try:
        return _safe_json_loads(body)
    except Exception as e:
        raise PumpClientError(
            f"POST json decode failed for {path}: {type(e).__name__}: {e} body={_trim_body(body)}"
        ) from e


async def pump_healthcheck(*, timeout: int = 10) -> dict[str, Any]:
    """
    Cheap sanity call that verifies at least one base responds.
    Designed for wiring into scripts/healthcheck.py later.
    """
    bases = _api_bases()
    out: dict[str, Any] = {"ok": False, "bases": bases, "ts": int(time.time())}

    attempts: list[Attempt] = []
    try:
        async with _session_cm() as s:
            for b in bases:
                url = _join(b, "/")
                try:
                    status, body = await _do_request(
                        s,
                        "GET",
                        url,
                        headers=_safe_headers(),
                        json_body=None,
                        timeout=int(timeout),
                    )
                    attempts.append(
                        Attempt(
                            base=b,
                            url=url,
                            status=int(status),
                            err=_trim_body(body or ""),
                            took_ms=0,
                        )
                    )
                    out["ok"] = True
                    out["status"] = int(status)
                    return out
                except Exception as e:
                    attempts.append(
                        Attempt(
                            base=b,
                            url=url,
                            status=-1,
                            err=f"{type(e).__name__}: {e}",
                            took_ms=0,
                        )
                    )
    except Exception as e:
        attempts.append(
            Attempt(
                base="session",
                url="session",
                status=-1,
                err=f"{type(e).__name__}: {e}",
                took_ms=0,
            )
        )

    out["attempts"] = [asdict(a) for a in attempts]
    return out


__all__ = [
    "Attempt",
    "PumpClientError",
    "PumpHTTPError",
    "pump_get",
    "pump_get_json",
    "pump_healthcheck",
    "pump_post",
    "pump_post_json",
]
