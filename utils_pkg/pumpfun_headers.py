from __future__ import annotations

"""
utils_pkg/pumpfun_headers.py

Canonical header builder for Pump.fun HTTP calls (V3+++ / prod-safe)
===================================================================

Goal
----
One place to construct consistent headers for direct Pump.fun API calls
(frontend-api.pump.fun, pump.fun, mirrors).

Design rules
------------
- No network work at import time
- Cookie handling is best-effort (never crash if cookie store is missing/broken)
- Never log raw cookies
- Deterministic defaults, but fully overrideable via env or function args
- No heavy/optional imports at module import time

Env contract
------------
- PUMPFUN_UA                  User-Agent (default: modern Chrome UA)
- PUMPFUN_ORIGIN              Origin header (default: https://pump.fun)
- PUMPFUN_REFERER             Referer header (default: https://pump.fun/)
- PUMPFUN_ACCEPT              Accept header (default: */*)
- PUMPFUN_ACCEPT_LANGUAGE     Accept-Language (optional)
- PUMPFUN_COOKIE              Fallback cookie string (optional)
- PUMPFUN_XTRPC_SOURCE        x-trpc-source header value (default: "web")
- PUMPFUN_INCLUDE_COOKIE      0/1 (default: 1)

Notes
-----
- Cookies normally come from utils_pkg.pf_cookie_store.cookie_string() (optional).
- If no cookie is available, we still return a usable header set.

Extra override contract
-----------------------
- extra is merged LAST (caller wins)
- extra values may be None to REMOVE a header key
- keys are normalized to lowercase for consistency
"""

import importlib
import logging
import os
from collections.abc import Mapping
from typing import Any, Protocol, cast

_TRUE = {"1", "true", "t", "yes", "y", "on"}
_FALSE = {"0", "false", "f", "no", "n", "off"}

_DEFAULT_UA_FALLBACK = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)


# -----------------------------------------------------------------------------
# Logging (repo-compatible; never required)
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
    base: Any = logging.getLogger("utils.pumpfun_headers")

    # Prefer canonical compat shim if present
    for mod_name in ("utils_pkg.log_compat", "utils_pkg.logging_setup", "logging_setup"):
        get_logger = _maybe_import_attr(mod_name, "get_logger")
        if callable(get_logger):
            try:
                base = get_logger("utils.pumpfun_headers")
                break
            except Exception:
                base = logging.getLogger("utils.pumpfun_headers")

    # Optional EventLogger wrapper
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
# Env helpers (read at call-time to avoid import-time coupling)
# -----------------------------------------------------------------------------
def _envs(k: str, d: str = "") -> str:
    return (os.getenv(k) or d).strip()


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


def _default_user_agent() -> str:
    ua = _envs("PUMPFUN_UA", _DEFAULT_UA_FALLBACK)
    return ua or _DEFAULT_UA_FALLBACK


def _default_origin() -> str:
    return _envs("PUMPFUN_ORIGIN", "https://pump.fun") or "https://pump.fun"


def _default_referer() -> str:
    return _envs("PUMPFUN_REFERER", "https://pump.fun/") or "https://pump.fun/"


def _default_accept() -> str:
    return _envs("PUMPFUN_ACCEPT", "*/*") or "*/*"


def _default_accept_language() -> str:
    return _envs("PUMPFUN_ACCEPT_LANGUAGE", "")


def _default_x_trpc_source() -> str:
    return _envs("PUMPFUN_XTRPC_SOURCE", "web") or "web"


def _default_include_cookie() -> bool:
    return _envb("PUMPFUN_INCLUDE_COOKIE", True)


# -----------------------------------------------------------------------------
# Cookie resolution (optional; best-effort; never raises)
# -----------------------------------------------------------------------------
def _strip_cookie_prefix(s: str) -> str:
    x = (s or "").strip()
    if x.lower().startswith("cookie:"):
        return x.split(":", 1)[1].strip()
    return x


def _mask_cookie(ck: str) -> str:
    ck_s = (ck or "").strip()
    return f"<cookie len={len(ck_s)}>" if ck_s else ""


def _cookie_from_store() -> str:
    """
    Try utils_pkg.pf_cookie_store.cookie_string() lazily.
    """
    try:
        mod = importlib.import_module("utils_pkg.pf_cookie_store")
        fn = getattr(mod, "cookie_string", None)
        if callable(fn):
            ck = fn()
            if isinstance(ck, str) and ck.strip():
                return ck.strip()
    except Exception as e:
        # debug only; cookie store may not exist early
        try:
            LOG.debug("pf_cookie_store unavailable: %s", e)
        except Exception:
            pass
    return ""


def _safe_cookie() -> str:
    """
    Best-effort cookie resolution:
      1) pf_cookie_store.cookie_string()
      2) PUMPFUN_COOKIE env
    Never raises.
    """
    ck = _cookie_from_store()
    if ck:
        return _strip_cookie_prefix(ck).strip()

    ck2 = _envs("PUMPFUN_COOKIE", "")
    return _strip_cookie_prefix(ck2).strip()


# -----------------------------------------------------------------------------
# Public API
# -----------------------------------------------------------------------------
def pumpfun_headers(
    extra: Mapping[str, str | None] | None = None,
    *,
    origin: str | None = None,
    referer: str | None = None,
    user_agent: str | None = None,
    accept: str | None = None,
    accept_language: str | None = None,
    content_type: str | None = "application/json",
    include_cookie: bool | None = None,
    x_trpc_source: str | None = None,
) -> dict[str, str]:
    """
    Build a consistent header dict for Pump.fun HTTP requests.

    Args:
      extra:           merged last (overrides defaults). Values of None remove keys.
      origin:          Origin header override
      referer:         Referer header override
      user_agent:      User-Agent override
      accept:          Accept override
      accept_language: Accept-Language override (or env default)
      content_type:    Content-Type; set None or "" to omit
      include_cookie:  include cookie header (default: env PUMPFUN_INCLUDE_COOKIE)
      x_trpc_source:   add x-trpc-source header (default: env PUMPFUN_XTRPC_SOURCE)

    Returns:
      dict[str, str] with lowercase header keys
    """
    inc_ck = _default_include_cookie() if include_cookie is None else bool(include_cookie)

    hdrs: dict[str, str] = {
        "accept": (accept or _default_accept()).strip() or "*/*",
        "origin": (origin or _default_origin()).strip() or "https://pump.fun",
        "referer": (referer or _default_referer()).strip() or "https://pump.fun/",
        "user-agent": (user_agent or _default_user_agent()).strip() or _DEFAULT_UA_FALLBACK,
    }

    lang = (accept_language if accept_language is not None else _default_accept_language()).strip()
    if lang:
        hdrs["accept-language"] = lang

    ct = "" if content_type is None else str(content_type).strip()
    if ct:
        hdrs["content-type"] = ct

    xtrpc = (x_trpc_source or _default_x_trpc_source()).strip()
    if xtrpc:
        hdrs["x-trpc-source"] = xtrpc

    if inc_ck:
        ck = _safe_cookie()
        if ck:
            hdrs["cookie"] = ck

    if extra:
        # Caller wins; normalize keys to lowercase for consistency
        for k, v in dict(extra).items():
            key = str(k).strip().lower()
            if not key:
                continue
            if v is None:
                hdrs.pop(key, None)
            else:
                hdrs[key] = str(v)

    return hdrs


def pumpfun_headers_for_log(
    extra: Mapping[str, str | None] | None = None,
    *,
    origin: str | None = None,
    referer: str | None = None,
    user_agent: str | None = None,
    accept: str | None = None,
    accept_language: str | None = None,
    content_type: str | None = "application/json",
    include_cookie: bool | None = None,
    x_trpc_source: str | None = None,
) -> dict[str, str]:
    """
    Same as pumpfun_headers(), but masks cookie for safe logging.
    """
    h = pumpfun_headers(
        extra,
        origin=origin,
        referer=referer,
        user_agent=user_agent,
        accept=accept,
        accept_language=accept_language,
        content_type=content_type,
        include_cookie=include_cookie,
        x_trpc_source=x_trpc_source,
    )
    if "cookie" in h:
        h2 = dict(h)
        h2["cookie"] = _mask_cookie(h2.get("cookie", ""))
        return h2
    return h


__all__ = ["pumpfun_headers", "pumpfun_headers_for_log"]
