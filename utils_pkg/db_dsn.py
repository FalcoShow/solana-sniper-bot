# -*- coding: utf-8 -*-
from __future__ import annotations

"""
utils_pkg/db_dsn.py

God-Tier DSN Brain (LOCK-IN / PROD)
==================================

Goal
----
One canonical place to pick + sanitize a Postgres DSN for:
  - SQLAlchemy asyncpg engine (create_async_engine)
  - Direct asyncpg.connect usage (scripts/test_pg, scripts/check_schema, etc.)

Ship-blocker rules
------------------
1) NEVER let URL query params flow into SQLAlchemy's asyncpg dialect.
   SQLAlchemy forwards URL query params as asyncpg.connect(**kwargs).
   Unknown keys (sslmode, options, statement_cache_size, etc) can crash.

2) Extract sslmode from DSN/env, STRIP ALL query params from the final DSN,
   and translate sslmode into a real SSLContext via connect_args["ssl"].

3) Prefer Render INTERNAL hostname only when it resolves (Render/VPC).
   Otherwise prefer EXTERNAL hostname.

Selection
---------
Env candidates (priority):
  POSITIONS_PG_DSN
  SNIPER_DB_DSN
  DATABASE_URL
  POSTGRES_DSN
  PG_DSN
  PG_DSN_INTERNAL
  PG_DSN_EXTERNAL
  PNL_DSN

SSL controls:
  PG_SSLMODE            (disable / require / verify-ca / verify-full)
  PG_FORCE_SSLMODE
  PG_SSL_INSECURE=1     (debug only, disables verification)
  PG_CA_FILE            (optional path to CA bundle)

Selection controls:
  PG_PREFER_INTERNAL=1
  PG_PREFER_EXTERNAL=1

App name:
  PG_APP_NAME

Public API
----------
  pick_pg_dsn(...) -> (dsn_sa_no_query, connect_args)
  pick_asyncpg_dsn(...) -> (dsn_pg_no_query, connect_kwargs)
  pick_pg_dsn_debug(...) -> PickResult (metadata)

Notes on sslmode mapping
------------------------
Postgres/libpq behavior:
  - require     = encrypt, don't verify host, don't require CA validation
  - verify-ca   = encrypt + verify CA, no hostname check
  - verify-full = encrypt + verify CA + hostname check

We emulate that with Python SSLContext:
  require     -> CERT_NONE + check_hostname False
  verify-ca   -> CERT_REQUIRED + check_hostname False
  verify-full -> CERT_REQUIRED + check_hostname True
"""

import os
import re
import socket
import ssl
from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple
from urllib.parse import parse_qsl, urlparse, urlunparse

try:
    import certifi  # type: ignore
except Exception:  # pragma: no cover
    certifi = None  # type: ignore


_TRUE = {"1", "true", "t", "yes", "y", "on"}
_FALSE = {"0", "false", "f", "no", "n", "off"}


# -------------------------
# env helpers
# -------------------------
def _env_str(k: str, default: str = "") -> str:
    v = os.getenv(k)
    if v is None:
        return default
    s = str(v).strip()
    # Render sometimes stores quoted values; remove one layer of quotes.
    if (s.startswith('"') and s.endswith('"')) or (s.startswith("'") and s.endswith("'")):
        s = s[1:-1].strip()
    return s


def _env_bool(k: str, default: bool = False) -> bool:
    v = os.getenv(k)
    if v is None:
        return default
    s = str(v).strip().lower()
    if s in _TRUE:
        return True
    if s in _FALSE:
        return False
    return default


# -------------------------
# masking / parsing
# -------------------------
def mask_dsn(dsn: str) -> str:
    """Hide password but keep host/db visible."""
    try:
        u = urlparse(dsn)
        if not u.username:
            return dsn
        netloc = u.netloc
        netloc = re.sub(rf"{re.escape(u.username)}:([^@]+)@", f"{u.username}:***@", netloc)
        return urlunparse((u.scheme, netloc, u.path, u.params, u.query, u.fragment))
    except Exception:
        return "<dsn>"


def _ensure_sa_asyncpg_url(dsn: str) -> str:
    """
    SQLAlchemy async engine should use postgresql+asyncpg://
    Accepts: postgres://, postgresql://, postgresql+asyncpg://
    """
    dsn = (dsn or "").strip()
    if not dsn:
        return ""
    if dsn.startswith("postgresql+asyncpg://"):
        return dsn
    if dsn.startswith("postgres://"):
        return "postgresql+asyncpg://" + dsn[len("postgres://") :]
    if dsn.startswith("postgresql://"):
        return "postgresql+asyncpg://" + dsn[len("postgresql://") :]
    return dsn


def _to_plain_postgresql(dsn_sa: str) -> str:
    """
    Convert a SQLAlchemy DSN to a plain postgresql:// DSN for direct asyncpg.
    """
    dsn_sa = (dsn_sa or "").strip()
    if dsn_sa.startswith("postgresql+asyncpg://"):
        return "postgresql://" + dsn_sa[len("postgresql+asyncpg://") :]
    if dsn_sa.startswith("postgres://"):
        return "postgresql://" + dsn_sa[len("postgres://") :]
    return dsn_sa


def _split_strip_query(url: str) -> Tuple[str, Dict[str, str]]:
    """
    Return (url_without_query, query_dict).

    We strip ALL query params because SQLAlchemy asyncpg forwards them to
    asyncpg.connect(**kwargs), and unknown keys can crash.
    """
    u = urlparse(url)
    q = dict(parse_qsl(u.query, keep_blank_values=True))
    stripped = urlunparse((u.scheme, u.netloc, u.path, u.params, "", u.fragment))
    return stripped, q


def _host_from_url(url: str) -> str:
    try:
        return urlparse(url).hostname or ""
    except Exception:
        return ""


def _resolves(host: str) -> bool:
    if not host:
        return False
    try:
        socket.getaddrinfo(host, None)
        return True
    except OSError:
        return False
    except Exception:
        return False


def _is_render_runtime() -> bool:
    # Render commonly sets at least one of these; if not, DNS-resolve check still protects.
    for k in ("RENDER", "RENDER_SERVICE_ID", "RENDER_INSTANCE_ID", "RENDER_EXTERNAL_HOSTNAME"):
        if os.getenv(k):
            return True
    return False


# -------------------------
# SSL mapping
# -------------------------
def _effective_sslmode(dsn_q: Dict[str, str], host: str) -> str:
    # strongest override first
    force = _env_str("PG_FORCE_SSLMODE", "")
    if force:
        return force.strip()

    env_mode = _env_str("PG_SSLMODE", "")
    if env_mode:
        return env_mode.strip()

    q_mode = (dsn_q.get("sslmode") or "").strip()
    if q_mode:
        return q_mode

    # sensible defaults:
    # - On Render we generally want encryption even on internal links
    if _is_render_runtime():
        return "require"

    # - For obvious managed/hosted PG domains, default to require if nothing specified
    h = (host or "").lower()
    if "render.com" in h or "postgres" in h:
        return "require"

    return ""


def _ssl_ctx_from_mode(sslmode: str, insecure: bool, host: str) -> Optional[ssl.SSLContext]:
    mode = (sslmode or "").strip().lower()
    if mode in ("", "disable", "off", "0", "false", "none"):
        return None

    ca_file = _env_str("PG_CA_FILE", "")
    cafile = ca_file if ca_file else (certifi.where() if certifi else None)

    # Create a base TLS context
    ctx = ssl.create_default_context(cafile=cafile)

    # Map libpq behavior
    if mode == "require":
        # encrypt, no verification
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
    elif mode in ("verify-ca", "verify_ca"):
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_REQUIRED
    elif mode in ("verify-full", "verify_full"):
        ctx.check_hostname = True
        ctx.verify_mode = ssl.CERT_REQUIRED
    else:
        # unknown mode -> safest reasonable default
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_REQUIRED

    if insecure:
        # Debug only
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE

    # Hostname checking uses SNI automatically; keep host for caller logs if needed.
    _ = host
    return ctx


def _connect_args_base(timeout_s: Optional[float], app_name: str) -> Dict[str, Any]:
    ca: Dict[str, Any] = {}

    # asyncpg supports statement_cache_size (int)
    scs = _env_str("PG_STATEMENT_CACHE_SIZE", "0")
    try:
        ca["statement_cache_size"] = int(float(scs or "0"))
    except Exception:
        ca["statement_cache_size"] = 0

    # asyncpg supports command_timeout (float seconds)
    if timeout_s is not None:
        try:
            ca["command_timeout"] = float(timeout_s)
        except Exception:
            pass

    # application_name should be in server_settings for asyncpg
    ss = ca.get("server_settings")
    if not isinstance(ss, dict):
        ss = {}
    if app_name:
        ss["application_name"] = str(app_name)
    ca["server_settings"] = ss
    return ca


# -------------------------
# selection
# -------------------------
@dataclass
class PickResult:
    dsn_sa: str
    connect_args: Dict[str, Any]
    source: str
    host: str
    sslmode: str
    ssl_enabled: bool
    host_resolves: bool


def _first_nonempty(*keys: str) -> Tuple[str, str]:
    for k in keys:
        v = os.getenv(k)
        if v and str(v).strip():
            s = _env_str(k, "").strip()
            if s:
                return s, k
    return "", ""


def _choose_candidate() -> Tuple[str, str]:
    return _first_nonempty(
        "POSITIONS_PG_DSN",
        "SNIPER_DB_DSN",
        "DATABASE_URL",
        "POSTGRES_DSN",
        "PG_DSN",
        "PG_DSN_INTERNAL",
        "PG_DSN_EXTERNAL",
        "PNL_DSN",
    )


def _pick_internal_external(dsn_int: str, dsn_ext: str) -> Tuple[str, str]:
    prefer_int = _env_bool("PG_PREFER_INTERNAL", False)
    prefer_ext = _env_bool("PG_PREFER_EXTERNAL", False)

    if dsn_int and not dsn_ext:
        return dsn_int, "PG_DSN_INTERNAL"
    if dsn_ext and not dsn_int:
        return dsn_ext, "PG_DSN_EXTERNAL"
    if not dsn_int and not dsn_ext:
        return "", ""

    host_int = _host_from_url(_ensure_sa_asyncpg_url(dsn_int))
    internal_ok = _resolves(host_int)

    if prefer_ext:
        return dsn_ext, "PG_DSN_EXTERNAL(forced)"
    if prefer_int and internal_ok:
        return dsn_int, "PG_DSN_INTERNAL(forced)"

    if internal_ok:
        return dsn_int, "PG_DSN_INTERNAL(auto)"
    return dsn_ext, "PG_DSN_EXTERNAL(auto)"


# -------------------------
# public API
# -------------------------
def pick_pg_dsn(*, timeout_s: Optional[float] = None, app_name: Optional[str] = None) -> Tuple[str, Dict[str, Any]]:
    """
    Returns:
      (dsn_sa_no_query, connect_args)

    dsn_sa_no_query is postgresql+asyncpg://... with NO query params.
    connect_args includes ssl context + server_settings, safe for SQLAlchemy asyncpg.
    """
    pr = pick_pg_dsn_debug(timeout_s=timeout_s, app_name=app_name)
    return pr.dsn_sa, pr.connect_args


def pick_asyncpg_dsn(*, timeout_s: Optional[float] = None, app_name: Optional[str] = None) -> Tuple[str, Dict[str, Any]]:
    """
    Returns:
      (dsn_pg_no_query, connect_kwargs)

    dsn_pg_no_query is postgresql://... with NO query params.
    connect_kwargs can be splatted into asyncpg.connect(dsn, **kwargs).
    """
    dsn_sa, ca = pick_pg_dsn(timeout_s=timeout_s, app_name=app_name)
    return _to_plain_postgresql(dsn_sa), ca


def pick_pg_dsn_debug(*, timeout_s: Optional[float] = None, app_name: Optional[str] = None) -> PickResult:
    app = (app_name or _env_str("PG_APP_NAME", "pnlsniper")).strip() or "pnlsniper"

    # Prefer explicit internal/external pairing if present
    dsn_int = _env_str("PG_DSN_INTERNAL", "")
    dsn_ext = _env_str("PG_DSN_EXTERNAL", "")
    if dsn_int or dsn_ext:
        chosen, src = _pick_internal_external(dsn_int, dsn_ext)
        if not chosen:
            return PickResult("", {}, "", "", "", False, False)
        return _sanitize_full(chosen, src, timeout_s=timeout_s, app_name=app)

    chosen, src = _choose_candidate()
    if not chosen:
        return PickResult("", {}, "", "", "", False, False)

    return _sanitize_full(chosen, src or "ENV", timeout_s=timeout_s, app_name=app)


# -------------------------
# core sanitization
# -------------------------
def _sanitize_full(dsn: str, source: str, *, timeout_s: Optional[float], app_name: str) -> PickResult:
    sa_url = _ensure_sa_asyncpg_url(dsn)
    sa_url_noq, q = _split_strip_query(sa_url)

    host = _host_from_url(sa_url_noq)
    host_ok = _resolves(host)

    sslmode = _effective_sslmode(q, host)
    insecure = _env_bool("PG_SSL_INSECURE", False)

    ca = _connect_args_base(timeout_s, app_name)

    # Ensure we don't pass poison keys
    ca.pop("sslmode", None)

    ssl_ctx = _ssl_ctx_from_mode(sslmode, insecure=insecure, host=host)
    if ssl_ctx is not None:
        ca["ssl"] = ssl_ctx
        ssl_enabled = True
    else:
        ca.pop("ssl", None)
        ssl_enabled = False

    return PickResult(
        dsn_sa=sa_url_noq,
        connect_args=ca,
        source=source,
        host=host,
        sslmode=sslmode,
        ssl_enabled=ssl_enabled,
        host_resolves=host_ok,
    )


__all__ = ["pick_pg_dsn", "pick_asyncpg_dsn", "pick_pg_dsn_debug", "mask_dsn", "PickResult"]
