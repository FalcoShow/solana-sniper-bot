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
- any support tooling that needs Pump.fun data

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
import json
import logging
import os
import random
import time
from dataclasses import dataclass
from typing import Any

# -----------------------------------------------------------------------------
# Logging (best-effort)
# -----------------------------------------------------------------------------
try:
    from utils_pkg.logging_setup import get_logger as _get_logger  # preferred
except Exception:  # pragma: no cover
    try:
        from logging_setup import get_logger as _get_logger  # root shim
    except Exception:  # pragma: no cover
        _get_logger = None  # type: ignore

if _get_logger:
    try:
        LOG = _get_logger("utils.pump_client")  # type: ignore[misc]
    except TypeError:  # pragma: no cover
        LOG = _get_logger()  # type: ignore[misc]
else:
    LOG = logging.getLogger("utils.pump_client")
    if not LOG.handlers:
        logging.basicConfig(level=logging.INFO)


# -----------------------------------------------------------------------------
# Imports (soft / future rewrites)
# -----------------------------------------------------------------------------
def _safe_headers() -> dict[str, str]:
    """
    Pump headers provider (rewritten separately).
    Must never throw.
    """
    try:
        from utils_pkg.pumpfun_headers import pumpfun_headers  # local import by design

        h = pumpfun_headers() or {}
        if isinstance(h, dict):
            return {str(k): str(v) for k, v in h.items()}
    except Exception:
        pass
    # minimal sane defaults
    return {
        "accept": "application/json, text/plain, */*",
        "content-type": "application/json",
        "user-agent": os.getenv("PUMPFUN_UA", "pnlsniper"),
    }


# proxy_client should provide make_session + http_get/http_post.
# We still guard and provide minimal fallbacks if signatures drift.
try:
    from utils_pkg.proxy_client import make_session  # type: ignore
except Exception as e:  # pragma: no cover
    raise RuntimeError(
        f"utils_pkg.proxy_client.make_session missing: {type(e).__name__}: {e}"
    ) from e

try:
    from utils_pkg.proxy_client import http_get as _http_get  # type: ignore
except Exception:  # pragma: no cover
    _http_get = None  # type: ignore

try:
    from utils_pkg.proxy_client import http_post as _http_post  # type: ignore
except Exception:  # pragma: no cover
    _http_post = None  # type: ignore

# Browser fallback for CF / bot wall
try:
    from utils_pkg.pump_browser_client import pump_post_via_browser  # type: ignore
except Exception:  # pragma: no cover
    pump_post_via_browser = None  # type: ignore

# Optional GET via browser (if you later add it)
try:
    from utils_pkg.pump_browser_client import pump_get_via_browser  # type: ignore
except Exception:  # pragma: no cover
    pump_get_via_browser = None  # type: ignore


# -----------------------------------------------------------------------------
# Env helpers
# -----------------------------------------------------------------------------
_TRUE = {"1", "true", "t", "yes", "y", "on"}
_FALSE = {"0", "false", "f", "no", "n", "off"}


def _b(name: str, default: bool = False) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    s = str(v).strip().lower()
    if s in _TRUE:
        return True
    if s in _FALSE:
        return False
    return default


def _i(name: str, default: int) -> int:
    v = os.getenv(name)
    try:
        return int(float(v)) if v not in (None, "") else default
    except Exception:
        return default


def _f(name: str, default: float) -> float:
    v = os.getenv(name)
    try:
        return float(v) if v not in (None, "") else default
    except Exception:
        return default


def _csv(name: str, default: str) -> list[str]:
    raw = os.getenv(name, default)
    parts = [p.strip() for p in str(raw).split(",")]
    return [p for p in parts if p]


# -----------------------------------------------------------------------------
# Config
# -----------------------------------------------------------------------------
DEFAULT_BASES = ["https://frontend-api.pump.fun"]
PUMPFUN_API_BASES = _csv("PUMPFUN_API_BASES", ",".join(DEFAULT_BASES))
PUMPFUN_RETRIES = max(1, _i("PUMPFUN_HTTP_RETRIES", 2))
PUMPFUN_BACKOFF_S = max(0.05, _f("PUMPFUN_HTTP_BACKOFF_S", 0.6))
PUMPFUN_JITTER_S = max(0.0, _f("PUMPFUN_HTTP_JITTER_S", 0.25))
PUMPFUN_TIMEOUT_S = max(3, _i("PUMPFUN_HTTP_TIMEOUT_S", 25))

# When to attempt browser fallback (CF / bot-wall)
PUMPFUN_BROWSER_FALLBACK = _b("PUMPFUN_BROWSER_FALLBACK", True)
PUMPFUN_BROWSER_FALLBACK_ON_GET = _b("PUMPFUN_BROWSER_FALLBACK_ON_GET", False)

# Response size guard (avoid memory bombs)
PUMPFUN_MAX_BODY_BYTES = max(64_000, _i("PUMPFUN_MAX_BODY_BYTES", 2_000_000))


def _api_bases() -> list[str]:
    bases = _csv("PUMPFUN_API_BASES", ",".join(DEFAULT_BASES))
    # allow a single override base too
    one = os.getenv("PUMPFUN_API_BASE") or ""
    one = one.strip()
    if one:
        bases = [one] + [b for b in bases if b != one]
    # sanitize
    out: list[str] = []
    for b in bases:
        b = b.strip().rstrip("/")
        if not b:
            continue
        if not b.startswith("http"):
            continue
        out.append(b)
    return out or DEFAULT_BASES[:]  # never empty


def _join(base: str, path: str) -> str:
    if path.startswith("http://") or path.startswith("https://"):
        return path
    p = path if path.startswith("/") else "/" + path
    return base.rstrip("/") + p


# -----------------------------------------------------------------------------
# Errors
# -----------------------------------------------------------------------------
@dataclass
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


# -----------------------------------------------------------------------------
# Low-level HTTP shims (in case proxy_client only provides make_session)
# -----------------------------------------------------------------------------
async def _fallback_http_get(
    session: Any, url: str, *, headers: dict[str, str], timeout: int
) -> tuple[int, str]:
    # Requires aiohttp-like session
    async with session.get(url, headers=headers, timeout=timeout) as resp:
        body = await resp.text()
        return int(resp.status), body


async def _fallback_http_post(
    session: Any, url: str, *, headers: dict[str, str], json_body: dict[str, Any], timeout: int
) -> tuple[int, str]:
    async with session.post(url, headers=headers, json=json_body, timeout=timeout) as resp:
        body = await resp.text()
        return int(resp.status), body


def _is_retryable_status(status: int) -> bool:
    # 429 / 5xx are usually transient; 403/503 sometimes CF (browser fallback)
    if status in (408, 409, 425, 429):
        return True
    return 500 <= status <= 599


def _looks_like_cf_block(status: int, body: str) -> bool:
    if status in (403, 503):
        b = (body or "").lower()
        # very broad heuristics; fine for deciding to try browser fallback
        return any(
            x in b for x in ("cloudflare", "cf-ray", "attention required", "challenge", "captcha")
        )
    return False


def _trim_body(body: str) -> str:
    if body is None:
        return ""
    if len(body) <= 600:
        return body
    return body[:600] + "…"


# -----------------------------------------------------------------------------
# Core request
# -----------------------------------------------------------------------------
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
    hdrs = dict(_safe_headers())
    if headers:
        hdrs.update({str(k): str(v) for k, v in headers.items()})

    bases = _api_bases()
    attempts: list[Attempt] = []

    # We rotate bases each call to avoid always hammering the first.
    if len(bases) > 1:
        shift = random.randint(0, len(bases) - 1)
        bases = bases[shift:] + bases[:shift]

    last_body = ""
    last_status = -1
    saw_cf_hint = False

    for attempt_i in range(1, PUMPFUN_RETRIES + 1):
        for base in bases:
            url = _join(base, path)
            t0 = time.monotonic()
            try:
                async with make_session() as s:
                    if method_u == "GET":
                        if callable(_http_get):
                            status, body = await _http_get(s, url, headers=hdrs, timeout=tmo)  # type: ignore[misc]
                        else:
                            status, body = await _fallback_http_get(
                                s, url, headers=hdrs, timeout=tmo
                            )
                    else:
                        jb = json_body or {}
                        if callable(_http_post):
                            status, body = await _http_post(
                                s, url, headers=hdrs, json=jb, timeout=tmo
                            )  # type: ignore[misc]
                        else:
                            status, body = await _fallback_http_post(
                                s, url, headers=hdrs, json_body=jb, timeout=tmo
                            )

                took_ms = int((time.monotonic() - t0) * 1000)
                body = body or ""
                if len(body.encode("utf-8", "ignore")) > PUMPFUN_MAX_BODY_BYTES:
                    body = body[:PUMPFUN_MAX_BODY_BYTES]

                last_body, last_status = body, int(status)

                # Success
                if int(status) == 200:
                    if tag:
                        LOG.info("pump_client %s ok %s %s (%dms)", tag, method_u, url, took_ms)
                    return body

                # Track CF hints
                if _looks_like_cf_block(int(status), body):
                    saw_cf_hint = True

                attempts.append(
                    Attempt(
                        base=base,
                        url=url,
                        status=int(status),
                        err=_trim_body(body),
                        took_ms=took_ms,
                    )
                )

                # Retry?
                if _is_retryable_status(int(status)):
                    continue

                # Non-retryable status; break base loop and maybe browser fallback
                break

            except TimeoutError:
                took_ms = int((time.monotonic() - t0) * 1000)
                attempts.append(
                    Attempt(
                        base=base, url=url, status=-1, err=f"TimeoutError({tmo}s)", took_ms=took_ms
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

        # Backoff between retry rounds
        if attempt_i < PUMPFUN_RETRIES:
            await asyncio.sleep(
                PUMPFUN_BACKOFF_S * attempt_i + random.uniform(0.0, PUMPFUN_JITTER_S)
            )

    # Browser fallback (best-effort, last resort)
    if allow_browser_fallback and PUMPFUN_BROWSER_FALLBACK:
        is_get = method_u == "GET"
        if (not is_get) or (is_get and PUMPFUN_BROWSER_FALLBACK_ON_GET):
            if (saw_cf_hint or last_status in (403, 503, -1)) and (
                pump_post_via_browser is not None or pump_get_via_browser is not None
            ):
                try:
                    if method_u == "POST":
                        if pump_post_via_browser is None:
                            raise RuntimeError("pump_post_via_browser not available")
                        LOG.warning("pump_client %s browser_fallback POST %s", tag or "-", path)
                        out = await pump_post_via_browser(path, json_body or {})
                        return out if isinstance(out, str) else json.dumps(out)
                    else:
                        if pump_get_via_browser is None:
                            raise RuntimeError("pump_get_via_browser not available")
                        LOG.warning("pump_client %s browser_fallback GET %s", tag or "-", path)
                        out = await pump_get_via_browser(path)
                        return out if isinstance(out, str) else json.dumps(out)
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

    # Final failure
    msg = f"{method_u} failed for {path} across bases (last_status={last_status}, last_body={_trim_body(last_body)})"
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
        "POST", path, json_body=json_body, timeout=timeout, tag="post", allow_browser_fallback=True
    )


# -----------------------------------------------------------------------------
# Convenience helpers
# -----------------------------------------------------------------------------
def _safe_json_loads(txt: str) -> Any:
    # tolerate empty
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
    You can wire this into scripts/healthcheck.py later.
    """
    bases = _api_bases()
    out: dict[str, Any] = {"ok": False, "bases": bases, "ts": int(time.time())}
    # Use a lightweight endpoint if you have one; otherwise "/" may still return HTML.
    # We'll just attempt GET "/" and accept any status for reachability.
    attempts: list[Attempt] = []
    for b in bases:
        url = _join(b, "/")
        try:
            async with make_session() as s:
                if callable(_http_get):
                    status, body = await _http_get(
                        s, url, headers=_safe_headers(), timeout=int(timeout)
                    )  # type: ignore[misc]
                else:
                    status, body = await _fallback_http_get(
                        s, url, headers=_safe_headers(), timeout=int(timeout)
                    )
            attempts.append(
                Attempt(base=b, url=url, status=int(status), err=_trim_body(body or ""), took_ms=0)
            )
            # if we got *anything*, call it reachable
            out["ok"] = True
            out["status"] = int(status)
            return out
        except Exception as e:
            attempts.append(
                Attempt(base=b, url=url, status=-1, err=f"{type(e).__name__}: {e}", took_ms=0)
            )
    out["attempts"] = [a.__dict__ for a in attempts]
    return out


__all__ = [
    "PumpClientError",
    "PumpHTTPError",
    "pump_get",
    "pump_get_json",
    "pump_healthcheck",
    "pump_post",
    "pump_post_json",
]
