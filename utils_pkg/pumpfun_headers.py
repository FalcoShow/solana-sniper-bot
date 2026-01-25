from __future__ import annotations

"""
utils_pkg/pumpfun_headers.py

Canonical header builder for Pump.fun HTTP calls (V3+++ / prod-safe)
===================================================================

Goal
----
One place to construct consistent headers for *direct* Pump.fun API calls
(frontend-api.pump.fun, pump.fun, mirrors). This is used by:
  - utils_pkg.pump_client (direct HTTP first)
  - any other lightweight HTTP tooling that needs the same header contract

Design rules
------------
- No network work at import time
- Cookie handling is best-effort (never crash if cookie store is missing/broken)
- Never log raw cookies
- Deterministic defaults, but fully overrideable via env or function args

Env contract
------------
- PUMPFUN_UA                 User-Agent (default: modern Chrome UA)
- PUMPFUN_ORIGIN             Origin header (default: https://pump.fun)
- PUMPFUN_REFERER            Referer header (default: https://pump.fun/)
- PUMPFUN_ACCEPT             Accept header (default: */*)
- PUMPFUN_ACCEPT_LANGUAGE    Accept-Language (optional)
- PUMPFUN_COOKIE             Fallback cookie string (optional)
- PUMPFUN_XTRPC_SOURCE        x-trpc-source header value (default: "web")
- PUMPFUN_INCLUDE_COOKIE      0/1 (default: 1)

Notes
-----
- Cookies normally come from utils_pkg.pf_cookie_store.cookie_string().
- If no cookie is available, we still return a usable header set.
"""

import os

# Cookie store is optional; never allow import failure to break callers.
try:
    from utils_pkg.pf_cookie_store import cookie_string as _cookie_string  # type: ignore
except Exception:  # pragma: no cover
    _cookie_string = None  # type: ignore


# Prefer canonical logging wrapper if present; fall back to std logging.
try:
    from utils_pkg.logging_setup import get_logger as _get_logger  # preferred
except Exception:  # pragma: no cover
    try:
        from logging_setup import get_logger as _get_logger  # root shim
    except Exception:  # pragma: no cover
        _get_logger = None  # type: ignore

if _get_logger:
    try:
        LOG = _get_logger("utils.pumpfun_headers")  # type: ignore[misc]
    except TypeError:  # pragma: no cover
        LOG = _get_logger()  # type: ignore[misc]
else:
    import logging

    LOG = logging.getLogger("utils.pumpfun_headers")


_TRUE = {"1", "true", "t", "yes", "y", "on"}
_FALSE = {"0", "false", "f", "no", "n", "off"}


def _envs(k: str, d: str = "") -> str:
    return (os.getenv(k) or d).strip()


def _envb(k: str, d: bool = False) -> bool:
    v = os.getenv(k)
    if v is None:
        return d
    s = str(v).strip().lower()
    if s in _TRUE:
        return True
    if s in _FALSE:
        return False
    return d


_DEFAULT_UA = _envs(
    "PUMPFUN_UA",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
)

_DEFAULT_ORIGIN = _envs("PUMPFUN_ORIGIN", "https://pump.fun")
_DEFAULT_REFERER = _envs("PUMPFUN_REFERER", "https://pump.fun/")
_DEFAULT_ACCEPT = _envs("PUMPFUN_ACCEPT", "*/*")
_DEFAULT_XTRPC = _envs("PUMPFUN_XTRPC_SOURCE", "web")
_DEFAULT_INCLUDE_COOKIE = _envb("PUMPFUN_INCLUDE_COOKIE", True)
_DEFAULT_ACCEPT_LANG = _envs("PUMPFUN_ACCEPT_LANGUAGE", "")


def _safe_cookie() -> str:
    """
    Best-effort cookie resolution:
      1) pf_cookie_store.cookie_string()
      2) PUMPFUN_COOKIE env
    Never raises.
    """
    # 1) cookie store
    if _cookie_string is not None:
        try:
            ck = _cookie_string()  # type: ignore[misc]
            if isinstance(ck, str) and ck.strip():
                return ck.strip()
        except Exception:
            # Do not spam logs; cookie store can be empty early in boot.
            pass

    # 2) env fallback
    ck2 = _envs("PUMPFUN_COOKIE", "")
    return ck2


def _mask_cookie(ck: str) -> str:
    if not ck:
        return ""
    # show only length-ish info; never reveal content
    return f"<cookie len={len(ck)}>"


def pumpfun_headers(
    extra: dict[str, str] | None = None,
    *,
    origin: str | None = None,
    referer: str | None = None,
    user_agent: str | None = None,
    accept: str | None = None,
    content_type: str | None = "application/json",
    include_cookie: bool | None = None,
    x_trpc_source: str | None = None,
) -> dict[str, str]:
    """
    Build a consistent header dict for Pump.fun HTTP requests.

    Args:
      extra:         merged last (overrides defaults)
      origin:        Origin header override
      referer:       Referer header override
      user_agent:    UA override
      accept:        Accept override
      content_type:  Content-Type; set None to omit
      include_cookie: include cookie header (default: env PUMPFUN_INCLUDE_COOKIE)
      x_trpc_source: add x-trpc-source header (default: env PUMPFUN_XTRPC_SOURCE)

    Returns:
      dict[str, str]
    """
    inc_ck = _DEFAULT_INCLUDE_COOKIE if include_cookie is None else bool(include_cookie)

    hdrs: dict[str, str] = {
        "accept": (accept or _DEFAULT_ACCEPT).strip() or "*/*",
        "origin": (origin or _DEFAULT_ORIGIN).strip() or "https://pump.fun",
        "referer": (referer or _DEFAULT_REFERER).strip() or "https://pump.fun/",
        "user-agent": (user_agent or _DEFAULT_UA).strip() or _DEFAULT_UA,
    }

    # optional language hint (helps look browser-like; harmless if absent)
    if _DEFAULT_ACCEPT_LANG:
        hdrs["accept-language"] = _DEFAULT_ACCEPT_LANG

    # content-type optional because some GETs should not set it
    if content_type is not None and str(content_type).strip():
        hdrs["content-type"] = str(content_type).strip()

    # tRPC calls often expect this; safe for non-tRPC endpoints too.
    xtrpc = (x_trpc_source or _DEFAULT_XTRPC).strip()
    if xtrpc:
        hdrs["x-trpc-source"] = xtrpc

    if inc_ck:
        ck = _safe_cookie()
        if ck:
            hdrs["cookie"] = ck

    if extra:
        # caller wins
        for k, v in dict(extra).items():
            if v is None:
                hdrs.pop(k, None)
            else:
                hdrs[str(k).lower()] = str(v)

    return hdrs


def pumpfun_headers_for_log(**kwargs: object) -> dict[str, str]:
    """
    Same as pumpfun_headers(), but masks cookie for safe logging.
    """
    h = pumpfun_headers(**kwargs)  # type: ignore[arg-type]
    if "cookie" in h:
        h = dict(h)
        h["cookie"] = _mask_cookie(h.get("cookie", ""))
    return h


__all__ = ["pumpfun_headers", "pumpfun_headers_for_log"]
