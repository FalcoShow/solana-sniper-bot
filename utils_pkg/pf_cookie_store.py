from __future__ import annotations

"""
utils_pkg/pf_cookie_store.py

Pump.fun cookie persistence + compatibility layer (V3+++ / prod-safe)
=====================================================================

Purpose
-------
This module is ONLY responsible for:
  - persisting cookie header strings to disk (legacy txt OR json map)
  - loading them back deterministically
  - providing backward-compatible helpers used by older code

It is NOT a bypass module. It does not perform any anti-bot work.

Storage formats
---------------
1) Legacy text file (.txt or any non-json path):
   - stores ONE cookie header string (assumed pump.fun / apex)

2) JSON map file (.json):
   - stores per-host cookie header strings (pump.fun + frontend-api.pump.fun)
   - schema (preferred):
       {"version": 1, "saved_ts": 123.0, "cookies": {"pump.fun": "...", "frontend-api.pump.fun": "..."}}
   - we also accept "flat" maps for robustness:
       {"pump.fun":"...", "frontend-api.pump.fun":"..."}

Runtime integration
-------------------
If utils_pkg.pumpfun_cf exists (canonical runtime cookie manager), we:
  - prefer its in-memory cookies (if valid) after env overrides
  - optionally set/update it when saving cookies (best-effort)

Env contract
------------
- PF_COOKIE_PATH              override cookie file path (legacy default preserved)
- PUMPFUN_COOKIE              apex cookie header (highest priority)
- PUMPFUN_COOKIE_API          api cookie header (highest priority for API host)

Notes
-----
- We never log cookie values, only masked lengths.
- Writes are atomic (tmp + replace).
- No network work at import time.
"""

import importlib
import json
import logging
import os
import time
from collections.abc import Mapping
from pathlib import Path
from typing import Any, Protocol, cast
from urllib.parse import urlparse

# ---------------------------
# Hosts
# ---------------------------
HOST_APEX = "pump.fun"
HOST_API = "frontend-api.pump.fun"
KNOWN_HOSTS: tuple[str, str] = (HOST_APEX, HOST_API)


# -----------------------------------------------------------------------------
# Logging (repo-compatible: EventLogger preferred; falls back to stdlib logger)
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
    base: Any = logging.getLogger("utils.pf_cookie_store")

    # Prefer canonical compat shim if present
    for mod_name in ("utils_pkg.log_compat", "utils_pkg.logging_setup", "logging_setup"):
        get_logger = _maybe_import_attr(mod_name, "get_logger")
        if callable(get_logger):
            try:
                base = get_logger("utils.pf_cookie_store")
                break
            except Exception:
                base = logging.getLogger("utils.pf_cookie_store")

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


# ---------------------------
# Paths
# ---------------------------
def _default_cookie_path() -> Path:
    # Keep legacy default to avoid breaking existing deploys
    envp = (os.environ.get("PF_COOKIE_PATH") or "").strip()
    if envp:
        return Path(envp)
    return Path.home() / ".pf_cookies.txt"


# Keep this constant for backward-compat with any internal imports
DEFAULT_COOKIE_PATH: Path = _default_cookie_path()


def cookie_path() -> str:
    """Canonical persistence path (string). Respects PF_COOKIE_PATH at call-time."""
    return str(_default_cookie_path())


# ---------------------------
# Small utilities
# ---------------------------
def _now() -> float:
    return time.time()


def _strip_cookie_prefix(s: str) -> str:
    x = (s or "").strip()
    if x.lower().startswith("cookie:"):
        return x.split(":", 1)[1].strip()
    return x


def _mask(s: str) -> str:
    s2 = (s or "").strip()
    return f"<cookie len={len(s2)}>" if s2 else ""


def _atomic_write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(text, encoding="utf-8")
    os.replace(str(tmp), str(path))


def _atomic_write_json(path: Path, obj: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(
        json.dumps(dict(obj), ensure_ascii=False, separators=(",", ":")), encoding="utf-8"
    )
    os.replace(str(tmp), str(path))


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
    if h.endswith(HOST_API):
        return HOST_API
    if h.endswith(HOST_APEX):
        return HOST_APEX
    return h


def _parse_cookie_string(s: str) -> dict[str, str]:
    out: dict[str, str] = {}
    raw = _strip_cookie_prefix(s)
    for part in raw.split(";"):
        p = part.strip()
        if not p or "=" not in p:
            continue
        k, v = p.split("=", 1)
        k = k.strip()
        v = v.strip()
        if k:
            out[k] = v
    return out


def _cookie_dict_to_string(
    cookies: Mapping[str, str], *, allow: tuple[str, ...] | None = None
) -> str:
    """
    Convert cookie dict to `k=v; k2=v2` string.
    If allow is provided, only include those cookie names.
    """
    parts: list[str] = []
    for k, v in dict(cookies).items():
        kk = (k or "").strip()
        vv = (v or "").strip()
        if not kk or not vv:
            continue
        if allow and kk not in allow:
            continue
        parts.append(f"{kk}={vv}")
    return "; ".join(parts)


def _looks_like_json_path(path: Path) -> bool:
    return path.suffix.lower() == ".json"


def _looks_like_json_text(txt: str) -> bool:
    t = (txt or "").lstrip()
    return bool(t) and t[0] in ("{", "[")


# ---------------------------
# Provider priority (runtime)
# ---------------------------
def _env_cookie_for_host(host: str) -> str:
    h = _normalize_host(host)
    if h == HOST_APEX:
        return _strip_cookie_prefix(os.environ.get("PUMPFUN_COOKIE") or "").strip()
    if h == HOST_API:
        return _strip_cookie_prefix(os.environ.get("PUMPFUN_COOKIE_API") or "").strip()
    return ""


def _try_get_runtime_cookie(host: str) -> str:
    """
    Best-available cookie string:
      1) env vars (explicit operator override)
      2) pumpfun_cf.get_cookie_for (if available)
    """
    h = _normalize_host(host)

    # 1) env override
    env_ck = _env_cookie_for_host(h)
    if env_ck:
        return env_ck

    # 2) pumpfun_cf (best-effort)
    try:
        mod = importlib.import_module("utils_pkg.pumpfun_cf")
        fn = getattr(mod, "get_cookie_for", None)
        if callable(fn):
            ck = (fn(h) or "").strip()
            ck = _strip_cookie_prefix(ck).strip()
            if ck:
                return ck
    except Exception:
        pass

    return ""


def _try_set_runtime_cookie(host: str, cookie_str: str, *, source: str = "pf_cookie_store") -> None:
    """
    If pumpfun_cf is available, update it too so in-memory + env stay coherent.
    """
    h = _normalize_host(host)
    ck = _strip_cookie_prefix(cookie_str).strip()
    if not ck:
        return
    try:
        mod = importlib.import_module("utils_pkg.pumpfun_cf")
        setter = getattr(mod, "set_cookie_for", None)
        if callable(setter):
            setter(h, ck, source=source)  # type: ignore[misc]
    except Exception:
        pass


# ---------------------------
# Disk I/O (legacy + json)
# ---------------------------
def _path_obj(path: str | None) -> Path:
    return Path(path) if path else _default_cookie_path()


def _read_disk_raw(path: str | None = None) -> str:
    p = _path_obj(path)
    if not p.exists():
        return ""
    try:
        return p.read_text(encoding="utf-8").strip()
    except Exception:
        return ""


def _read_disk_json(path: str | None = None) -> dict[str, Any]:
    p = _path_obj(path)
    if not p.exists():
        return {}
    try:
        txt = p.read_text(encoding="utf-8").strip()
        if not txt:
            return {}
        obj = json.loads(txt)
        return obj if isinstance(obj, dict) else {}
    except Exception:
        return {}


def _extract_cookie_from_json(obj: dict[str, Any], host: str) -> str:
    """
    Supported payloads:
      A) {"version":1, "cookies": {"pump.fun":"..."}}
      B) {"pump.fun":"...", "frontend-api.pump.fun":"..."}  (flat)
    """
    h = _normalize_host(host)

    cookies = obj.get("cookies")
    if isinstance(cookies, dict):
        v = cookies.get(h)
        if isinstance(v, str):
            return _strip_cookie_prefix(v).strip()

    v2 = obj.get(h)
    if isinstance(v2, str):
        return _strip_cookie_prefix(v2).strip()

    return ""


def load_cookie_string_for_host(host: str, path: str | None = None) -> str:
    """
    Load cookie string for a host from disk.
    - If file is JSON (suffix .json OR parseable json): uses per-host mapping
    - Else: treats file as legacy string for pump.fun apex
    """
    h = _normalize_host(host)
    p = _path_obj(path)

    # Decide format
    if _looks_like_json_path(p):
        obj = _read_disk_json(str(p))
        return _extract_cookie_from_json(obj, h)

    # Robustness: try parse as json if content looks like json
    raw = _read_disk_raw(str(p))
    if raw and _looks_like_json_text(raw):
        try:
            obj2 = json.loads(raw)
            if isinstance(obj2, dict):
                return _extract_cookie_from_json(obj2, h)
        except Exception:
            pass

    # Legacy: only apex stored
    if h == HOST_APEX:
        return _strip_cookie_prefix(raw).strip()
    return ""


def save_cookie_string_for_host(host: str, cookie_str: str, path: str | None = None) -> str:
    """
    Save cookie string for a host.
    - If path ends with .json: writes/updates JSON map
    - Else: writes legacy string for apex only (pump.fun)
    """
    h = _normalize_host(host)
    ck = _strip_cookie_prefix(cookie_str).strip()
    if not ck:
        raise ValueError("cookie_str is empty")

    p = _path_obj(path)

    if _looks_like_json_path(p):
        existing: dict[str, Any] = _read_disk_json(str(p))

        cookies_any = existing.get("cookies")
        cookies: dict[str, Any] = cookies_any if isinstance(cookies_any, dict) else {}

        cookies[h] = ck
        payload: dict[str, Any] = dict(existing)
        payload["version"] = int(payload.get("version") or 1)
        payload["saved_ts"] = _now()
        payload["cookies"] = cookies

        _atomic_write_json(p, payload)
        try:
            LOG.info("saved cookie (json) host=%s path=%s %s", h, str(p), _mask(ck))
        except Exception:
            pass
    else:
        # legacy text file supports only apex
        if h != HOST_APEX:
            raise ValueError(
                "legacy cookie file only supports pump.fun (apex). Use a .json path for per-host cookies."
            )
        _atomic_write_text(p, ck)
        try:
            LOG.info("saved cookie (legacy) host=%s path=%s %s", h, str(p), _mask(ck))
        except Exception:
            pass

    # keep runtime in sync if available (best-effort)
    _try_set_runtime_cookie(h, ck)
    return str(p)


# ---------------------------
# Backwards-compatible API
# ---------------------------
def cookie_string() -> str:
    """
    Backwards-compat: return the Cookie header string for pump.fun (apex).
    Priority:
      1) env / pumpfun_cf runtime
      2) disk
    """
    ck = _try_get_runtime_cookie(HOST_APEX)
    if ck:
        return ck
    return load_cookie_string_for_host(HOST_APEX)


def load_pf_cookie_string(path: str | None = None) -> str:
    """Alias for cookie_string() but allows explicit path override."""
    if path is None:
        ck = _try_get_runtime_cookie(HOST_APEX)
        if ck:
            return ck
    return load_cookie_string_for_host(HOST_APEX, path)


def load_pf_cookies(path: str | None = None) -> dict[str, str]:
    """
    Backwards-compat: return apex cookies as a dict.
    Supports disk file containing "k=v; k2=v2" (or "Cookie: ...").
    """
    raw = load_pf_cookie_string(path)
    return _parse_cookie_string(raw)


def save_pf_cookies(cookies: Mapping[str, str], path: str | None = None) -> str:
    """
    Backwards-compat: accept a dict cookie jar and save a Cookie header string.

    Historically the legacy file favored a small set of CF-ish keys if present;
    keep that behavior but do not require them.
    """
    allow = ("__cf_bm", "_cfuvid", "cf_clearance")
    cookies_dict = dict(cookies or {})
    use_allow = any(k in cookies_dict for k in allow)
    s = _cookie_dict_to_string(cookies_dict, allow=allow if use_allow else None)
    return save_cookie_string_for_host(HOST_APEX, s, path)


def save_pf_cookie_string(cookie_header: str, path: str | None = None) -> str:
    """Explicitly save an apex Cookie header string."""
    return save_cookie_string_for_host(HOST_APEX, cookie_header, path)


# ---------------------------
# New preferred helpers
# ---------------------------
def get_cookie_header(url_or_host: str) -> str:
    """
    Preferred API: get cookie header for any known host.
    Accepts a host or URL.
    Priority:
      1) env / pumpfun_cf runtime
      2) disk
    """
    host = _normalize_host(_host_from_url_or_host(url_or_host))
    if host in KNOWN_HOSTS:
        ck = _try_get_runtime_cookie(host)
        if ck:
            return ck
        return load_cookie_string_for_host(host)
    return ""


def set_cookie_header(url_or_host: str, cookie_header: str, path: str | None = None) -> str:
    """
    Preferred API: set cookie header for any known host.
    Accepts a host or URL. Writes to disk + updates runtime manager if present.
    """
    host = _normalize_host(_host_from_url_or_host(url_or_host))
    if host not in KNOWN_HOSTS:
        raise ValueError(f"unknown host: {url_or_host!r}")
    return save_cookie_string_for_host(host, cookie_header, path)


__all__ = [
    "HOST_APEX",
    "HOST_API",
    "KNOWN_HOSTS",
    "cookie_path",
    "cookie_string",
    "get_cookie_header",
    "load_cookie_string_for_host",
    "load_pf_cookie_string",
    "load_pf_cookies",
    "save_cookie_string_for_host",
    "save_pf_cookie_string",
    "save_pf_cookies",
    "set_cookie_header",
]
