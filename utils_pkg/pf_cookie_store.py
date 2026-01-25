from __future__ import annotations

"""
utils_pkg/pf_cookie_store.py

Pump.fun cookie persistence + compatibility layer
=================================================

Purpose
-------
This module is ONLY responsible for:
  - persisting cookie header strings to disk (legacy txt OR json map)
  - loading them back deterministically
  - providing backward-compatible helpers used by older code

It is NOT a bypass module. It does not perform any anti-bot work.

Design
------
- Two storage formats:
  1) Legacy text file: stores ONE cookie header string (assumed pump.fun / apex)
  2) JSON map file: stores per-host cookie header strings (pump.fun + frontend-api.pump.fun)

- Backwards compatibility:
  - cookie_string() returns the apex cookie header string (best available)
  - load_pf_cookies() returns dict cookies parsed from the apex cookie header
  - save_pf_cookies() accepts dict cookies, writes apex cookie string

Integration
-----------
If utils_pkg.pumpfun_cf exists (your canonical runtime cookie manager), we will:
  - prefer its in-memory cookie values (if valid)
  - optionally set/update it when saving cookies

Env contract
------------
- PF_COOKIE_PATH              override cookie file path
- PUMPFUN_COOKIE              apex cookie header (highest priority)
- PUMPFUN_COOKIE_API          api cookie header (highest priority for API host)

Notes
-----
- We do not log cookie values, only masked lengths.
- Writes are atomic (temp + replace).
"""

import json
import os
import time
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

# ---------------------------
# Hosts
# ---------------------------
HOST_APEX = "pump.fun"
HOST_API = "frontend-api.pump.fun"
KNOWN_HOSTS = (HOST_APEX, HOST_API)


# ---------------------------
# Paths
# ---------------------------
def _default_cookie_path() -> Path:
    # Keep legacy default to avoid breaking existing deploys
    return Path(os.environ.get("PF_COOKIE_PATH") or (Path.home() / ".pf_cookies.txt"))


DEFAULT_COOKIE_PATH = _default_cookie_path()


def cookie_path() -> str:
    """Canonical persistence path (string)."""
    return str(DEFAULT_COOKIE_PATH)


# ---------------------------
# Small utilities
# ---------------------------
def _now() -> float:
    return time.time()


def _mask(s: str) -> str:
    if not s:
        return ""
    return f"<cookie len={len(s)}>"


def _atomic_write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(text, encoding="utf-8")
    tmp.replace(path)


def _atomic_write_json(path: Path, obj: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(obj, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")
    tmp.replace(path)


def _host_from_url_or_host(url_or_host: str) -> str:
    s = (url_or_host or "").strip()
    if not s:
        return ""
    try:
        u = urlparse(s)
        if u.hostname:
            return u.hostname.lower()
    except Exception:
        pass
    return s.lower()


def _normalize_host(host: str) -> str:
    h = (host or "").strip().lower()
    if h.endswith(HOST_API):
        return HOST_API
    if h.endswith(HOST_APEX):
        return HOST_APEX
    return h


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


def _cookie_dict_to_string(cookies: dict[str, str], *, allow: tuple[str, ...] | None = None) -> str:
    """
    Convert cookie dict to `k=v; k2=v2` string.
    If allow is provided, only include those cookie names.
    """
    parts = []
    for k, v in (cookies or {}).items():
        if not k or not v:
            continue
        if allow and k not in allow:
            continue
        parts.append(f"{k}={v}")
    return "; ".join(parts)


def _looks_like_json(path: Path) -> bool:
    return path.suffix.lower() == ".json"


# ---------------------------
# Provider priority (runtime)
# ---------------------------
def _try_get_runtime_cookie(host: str) -> str:
    """
    Best-available cookie string:
      1) env vars
      2) pumpfun_cf.get_cookie_for (if available)
    """
    h = _normalize_host(host)

    # env first (explicit operator override)
    if h == HOST_APEX:
        v = (os.environ.get("PUMPFUN_COOKIE") or "").strip()
        if v:
            return v
    if h == HOST_API:
        v = (os.environ.get("PUMPFUN_COOKIE_API") or "").strip()
        if v:
            return v

    # then pumpfun_cf if present
    try:
        from utils_pkg import pumpfun_cf  # local import; avoids import cycles

        ck = (pumpfun_cf.get_cookie_for(h) or "").strip()  # type: ignore[attr-defined]
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
    ck = (cookie_str or "").strip()
    if not ck:
        return
    try:
        from utils_pkg import pumpfun_cf  # local import

        setter = getattr(pumpfun_cf, "set_cookie_for", None)
        if callable(setter):
            setter(h, ck, source=source)  # type: ignore[misc]
    except Exception:
        pass


# ---------------------------
# Disk I/O (legacy + json)
# ---------------------------
def _read_disk_raw(path: str | None = None) -> str:
    p = Path(path) if path else DEFAULT_COOKIE_PATH
    if not p.exists():
        return ""
    try:
        return p.read_text(encoding="utf-8").strip()
    except Exception:
        return ""


def _read_disk_json(path: str | None = None) -> dict[str, Any]:
    p = Path(path) if path else DEFAULT_COOKIE_PATH
    if not p.exists():
        return {}
    try:
        txt = p.read_text(encoding="utf-8").strip()
        if not txt:
            return {}
        return json.loads(txt) if isinstance(txt, str) else {}
    except Exception:
        return {}


def load_cookie_string_for_host(host: str, path: str | None = None) -> str:
    """
    Load cookie string for a host from disk.
    - If file is JSON: uses obj["cookies"][host]
    - Else: treats file as legacy string for pump.fun
    """
    h = _normalize_host(host)
    p = Path(path) if path else DEFAULT_COOKIE_PATH

    if _looks_like_json(p):
        obj = _read_disk_json(str(p))
        cookies = obj.get("cookies") if isinstance(obj, dict) else None
        if isinstance(cookies, dict):
            v = cookies.get(h)
            if isinstance(v, str):
                return v.strip()
        return ""

    # legacy: only apex stored
    if h == HOST_APEX:
        return _read_disk_raw(str(p))
    return ""


def save_cookie_string_for_host(host: str, cookie_str: str, path: str | None = None) -> str:
    """
    Save cookie string for a host.
    - If path ends with .json: writes/updates JSON map
    - Else: writes legacy string for apex only (pump.fun)
    """
    h = _normalize_host(host)
    ck = (cookie_str or "").strip()
    if not ck:
        raise ValueError("cookie_str is empty")

    p = Path(path) if path else DEFAULT_COOKIE_PATH

    if _looks_like_json(p):
        existing = _read_disk_json(str(p))
        if not isinstance(existing, dict):
            existing = {}
        cookies = existing.get("cookies")
        if not isinstance(cookies, dict):
            cookies = {}
        cookies[h] = ck
        existing["version"] = int(existing.get("version") or 1)
        existing["saved_ts"] = _now()
        existing["cookies"] = cookies
        _atomic_write_json(p, existing)
    else:
        # legacy text file supports only apex
        if h != HOST_APEX:
            raise ValueError(
                "legacy cookie file only supports pump.fun (apex). Use a .json path for per-host."
            )
        _atomic_write_text(p, ck)

    # keep runtime in sync if available
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
    ck = _try_get_runtime_cookie(HOST_APEX)
    if ck and path is None:
        return ck
    return load_cookie_string_for_host(HOST_APEX, path)


def load_pf_cookies(path: str | None = None) -> dict[str, str]:
    """
    Backwards-compat: return apex cookies as a dict.
    Supports disk file containing "k=v; k2=v2".
    """
    raw = load_pf_cookie_string(path)
    return _parse_cookie_string(raw)


def save_pf_cookies(cookies: dict[str, str], path: str | None = None) -> str:
    """
    Backwards-compat: accept a dict cookie jar and save a Cookie header string.

    We keep common CF-ish names if present, but we do NOT require them.
    (Callers may provide any cookie dict.)
    """
    # historically the file wanted only a few keys; keep that behavior but allow all
    allow = ("__cf_bm", "_cfuvid", "cf_clearance")
    # if none of the allow keys exist, store everything (better UX)
    use_allow = any(k in (cookies or {}) for k in allow)
    s = _cookie_dict_to_string(cookies or {}, allow=allow if use_allow else None)
    return save_cookie_string_for_host(HOST_APEX, s, path)


def save_pf_cookie_string(cookie_header: str, path: str | None = None) -> str:
    """Explicitly save an apex Cookie header string."""
    return save_cookie_string_for_host(HOST_APEX, cookie_header, path)


# ---------------------------
# New preferred helpers
# ---------------------------
def get_cookie_header(host: str) -> str:
    """
    Preferred API: get cookie header for any known host.
    Priority:
      1) env / pumpfun_cf runtime
      2) disk
    """
    h = _normalize_host(host)
    ck = _try_get_runtime_cookie(h)
    if ck:
        return ck
    return load_cookie_string_for_host(h)


def set_cookie_header(host: str, cookie_header: str, path: str | None = None) -> str:
    """
    Preferred API: set cookie header for any known host.
    Writes to disk + updates runtime manager if present.
    """
    return save_cookie_string_for_host(host, cookie_header, path)


__all__ = [
    "HOST_APEX",
    "HOST_API",
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
