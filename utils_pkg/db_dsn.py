from __future__ import annotations

"""
utils_pkg/db_dsn.py

God-Tier DSN Brain (LOCK-IN / PROD)
==================================

Single canonical DSN picker + sanitizer for Postgres.

Why this exists (SHIP-BLOCKERS)
-------------------------------
1) NEVER let URL query params flow into SQLAlchemy asyncpg dialect.
   SQLAlchemy forwards URL query params as asyncpg.connect(**kwargs).
   Unknown keys (sslmode, options, statement_cache_size, etc.) can crash.

2) Extract sslmode from DSN/env, STRIP ALL query params from the final DSN,
   and translate sslmode into a real SSLContext via connect_args["ssl"].

3) Prefer INTERNAL hostname only when it resolves (Render/VPC), otherwise EXTERNAL.

Env candidates (priority)
-------------------------
  POSITIONS_PG_DSN
  SNIPER_DB_DSN
  DATABASE_URL
  POSTGRES_DSN
  PG_DSN
  PG_DSN_INTERNAL
  PG_DSN_EXTERNAL
  PNL_DSN

Selection controls
------------------
  PG_PREFER_INTERNAL=1   (force internal)
  PG_PREFER_EXTERNAL=1   (force external)

SSL controls
------------
  PG_FORCE_SSLMODE        (strongest override)
  PG_SSLMODE              (disable/require/verify-ca/verify-full/prefer/allow)
  PG_SSL_INSECURE=1       (DEBUG ONLY: disables verification)
  PG_CA_FILE              (optional CA bundle path)

Connect args
------------
  PG_APP_NAME              (default: "pnlsniper")
  PG_STATEMENT_CACHE_SIZE  (default: 0)
  PG_COMMAND_TIMEOUT_SEC   (optional float)
  PG_CONNECT_TIMEOUT_SEC   (optional float)

Public API (stable)
-------------------
  pick_pg_dsn(...) -> (dsn_sa_no_query, connect_args)
  pick_asyncpg_dsn(...) -> (dsn_pg_no_query, connect_kwargs)
  pick_pg_dsn_debug(...) -> PickResult
  mask_dsn(dsn) -> masked string

Compatibility aliases (do NOT remove)
-------------------------------------
  resolve_pg_dsn / resolve_pg_dsn_debug
  get_pg_dsn / get_pg_dsn_debug
"""

import contextlib
import os
import re
import socket
import ssl
from collections.abc import Iterable
from dataclasses import dataclass, field
from functools import lru_cache
from typing import Any
from urllib.parse import parse_qsl, urlparse, urlunparse

try:
    import certifi  # type: ignore
except Exception:  # pragma: no cover
    certifi = None  # type: ignore


_TRUE = {"1", "true", "t", "yes", "y", "on"}
_FALSE = {"0", "false", "f", "no", "n", "off"}

# Query keys we interpret (we STILL strip them from the final DSN)
_SAFE_Q_KEYS = {
    "sslmode",
    "application_name",
    "connect_timeout",
    "command_timeout",
    "statement_cache_size",
}

# Masking helpers (best-effort; never throw)
_URL_PASS_RE = re.compile(r"(://[^:/\s]+):([^@/\s]+)@")  # masks :pass@
_URL_CRED_RE = re.compile(r"(://)([^/@\s]+)@")  # masks user:pass@


# ──────────────────────────────────────────────────────────────────────────────
# Env helpers
# ──────────────────────────────────────────────────────────────────────────────
def _env_str(key: str, default: str = "") -> str:
    val = os.getenv(key)
    if val is None:
        return default
    s = str(val).strip()
    # Render/UI sometimes stores quoted values; remove one layer of quotes.
    if (s.startswith('"') and s.endswith('"')) or (s.startswith("'") and s.endswith("'")):
        s = s[1:-1].strip()
    return s


def _env_bool(key: str, default: bool = False) -> bool:
    val = os.getenv(key)
    if val is None:
        return default
    s = str(val).strip().lower()
    if s in _TRUE:
        return True
    if s in _FALSE:
        return False
    return default


def _env_int(key: str, default: int) -> int:
    try:
        raw = _env_str(key, str(default)).strip()
        return int(float(raw)) if raw else int(default)
    except Exception:
        return default


def _env_float(key: str, default: float | None = None) -> float | None:
    raw = _env_str(key, "").strip()
    if not raw:
        return default
    try:
        return float(raw)
    except Exception:
        return default


# ──────────────────────────────────────────────────────────────────────────────
# Masking
# ──────────────────────────────────────────────────────────────────────────────
def mask_dsn(dsn: str) -> str:
    """Hide password but keep host/db visible (best-effort, never throws)."""
    try:
        if not dsn:
            return ""
        s = str(dsn)
        s = _URL_PASS_RE.sub(r"\1:***@", s)
        s = _URL_CRED_RE.sub(r"\1***@", s)
        if "?" in s:
            base, _ = s.split("?", 1)
            return base + "?..."
        return s
    except Exception:
        return "<dsn>"


# ──────────────────────────────────────────────────────────────────────────────
# URL normalization + parsing
# ──────────────────────────────────────────────────────────────────────────────
def _coerce_url(raw: str) -> str:
    """
    Accept common DSN shapes, return a URL-ish string.
    - postgres://...        (ok)
    - postgresql://...      (ok)
    - postgresql+asyncpg:// (ok)
    - user:pass@host/db     (coerce to postgresql://...)
    """
    s = (raw or "").strip()
    if not s:
        return ""

    if "://" in s:
        return s

    # Looks like netloc/path DSN
    return "postgresql://" + s


def _ensure_sa_asyncpg_url(dsn: str) -> str:
    """
    SQLAlchemy async engine DSN must use postgresql+asyncpg://
    Accepts: postgres://, postgresql://, postgresql+asyncpg://
    """
    u = _coerce_url(dsn)
    if not u:
        return ""
    if u.startswith("postgresql+asyncpg://"):
        return u
    if u.startswith("postgres://"):
        return "postgresql+asyncpg://" + u[len("postgres://") :]
    if u.startswith("postgresql://"):
        return "postgresql+asyncpg://" + u[len("postgresql://") :]
    return u


def _to_plain_postgresql(dsn_any: str) -> str:
    """Convert SQLAlchemy DSN variants to plain postgresql:// for direct drivers."""
    s = (dsn_any or "").strip()
    if s.startswith("postgresql+asyncpg://"):
        return "postgresql://" + s[len("postgresql+asyncpg://") :]
    if s.startswith("postgresql+psycopg://"):
        return "postgresql://" + s[len("postgresql+psycopg://") :]
    if s.startswith("postgres://"):
        return "postgresql://" + s[len("postgres://") :]
    return s


def _split_strip_query(url: str) -> tuple[str, dict[str, str], list[str]]:
    """
    Return (url_without_query, query_dict, dropped_keys).

    We strip ALL query params because SQLAlchemy asyncpg forwards them to
    asyncpg.connect(**kwargs), and unknown keys can crash.
    """
    parsed = urlparse(url)
    items = list(parse_qsl(parsed.query, keep_blank_values=True))
    q = {k: v for (k, v) in items}
    dropped = [k for (k, _) in items]
    stripped = urlunparse(
        (parsed.scheme, parsed.netloc, parsed.path, parsed.params, "", parsed.fragment)
    )
    return stripped, q, dropped


def _host_from_url(url: str) -> str:
    try:
        return urlparse(url).hostname or ""
    except Exception:
        return ""


def _port_from_url(url: str) -> int | None:
    try:
        port = urlparse(url).port
        return int(port) if port else None
    except Exception:
        return None


# ──────────────────────────────────────────────────────────────────────────────
# Runtime heuristics
# ──────────────────────────────────────────────────────────────────────────────
def _is_render_runtime() -> bool:
    for key in ("RENDER", "RENDER_SERVICE_ID", "RENDER_INSTANCE_ID", "RENDER_EXTERNAL_HOSTNAME"):
        if os.getenv(key):
            return True
    return False


@lru_cache(maxsize=256)
def _resolves(host: str) -> bool:
    if not host:
        return False
    try:
        socket.getaddrinfo(host, None)
        return True
    except Exception:
        return False


# ──────────────────────────────────────────────────────────────────────────────
# SSL mapping
# ──────────────────────────────────────────────────────────────────────────────
def _normalize_sslmode(mode: str) -> str:
    m = (mode or "").strip().lower()
    if not m:
        return ""
    if m in ("prefer", "allow"):
        # We cannot safely implement "try ssl then fallback" at this layer.
        return "require"
    if m == "verify_ca":
        return "verify-ca"
    if m == "verify_full":
        return "verify-full"
    return m


def _effective_sslmode(dsn_q: dict[str, str], host: str) -> str:
    force = _env_str("PG_FORCE_SSLMODE", "")
    if force:
        return _normalize_sslmode(force)

    env_mode = _env_str("PG_SSLMODE", "")
    if env_mode:
        return _normalize_sslmode(env_mode)

    q_mode = (dsn_q.get("sslmode") or "").strip()
    if q_mode:
        return _normalize_sslmode(q_mode)

    if _is_render_runtime():
        return "require"

    h = (host or "").lower()
    if any(
        token in h
        for token in (
            "render.com",
            "supabase",
            "neon.tech",
            "railway",
            "elephantsql",
            "amazonaws.com",
            "azure.com",
            "googleapis.com",
        )
    ):
        return "require"

    return ""


def _ssl_ctx_from_mode(sslmode: str, *, insecure: bool) -> ssl.SSLContext | None:
    mode = _normalize_sslmode(sslmode)
    if mode in ("", "disable", "off", "0", "false", "none"):
        return None

    ca_file = _env_str("PG_CA_FILE", "").strip()
    cafile = ca_file if ca_file else (certifi.where() if certifi else None)

    ctx = ssl.create_default_context(cafile=cafile)

    if mode == "require":
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
    elif mode == "verify-ca":
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_REQUIRED
    elif mode == "verify-full":
        ctx.check_hostname = True
        ctx.verify_mode = ssl.CERT_REQUIRED
    else:
        # Unknown -> safer default: require CA validation, no hostname check.
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_REQUIRED

    if insecure:
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE

    return ctx


# ──────────────────────────────────────────────────────────────────────────────
# connect args (asyncpg-safe) + safe query overrides
# ──────────────────────────────────────────────────────────────────────────────
def _connect_args_base(timeout_s: float | None, app_name: str) -> dict[str, Any]:
    """
    Kwargs safe to pass to asyncpg.connect (and SQLAlchemy connect_args).
    """
    connect_args: dict[str, Any] = {}

    connect_args["statement_cache_size"] = int(_env_int("PG_STATEMENT_CACHE_SIZE", 0))

    env_conn_to = _env_float("PG_CONNECT_TIMEOUT_SEC", None)
    if env_conn_to is not None:
        connect_args["timeout"] = float(env_conn_to)
    elif timeout_s is not None:
        connect_args["timeout"] = float(timeout_s)

    env_cmd_to = _env_float("PG_COMMAND_TIMEOUT_SEC", None)
    if env_cmd_to is not None:
        connect_args["command_timeout"] = float(env_cmd_to)

    server_settings: dict[str, Any] = {}
    if app_name:
        server_settings["application_name"] = str(app_name)
    connect_args["server_settings"] = server_settings

    return connect_args


def _apply_safe_query_overrides(connect_args: dict[str, Any], q: dict[str, str]) -> None:
    # application_name
    app_name = (q.get("application_name") or "").strip()
    if app_name:
        ss = connect_args.get("server_settings")
        if not isinstance(ss, dict):
            ss = {}
        ss["application_name"] = app_name
        connect_args["server_settings"] = ss

    # connect_timeout -> asyncpg "timeout"
    raw_ct = (q.get("connect_timeout") or "").strip()
    if raw_ct:
        with contextlib.suppress(Exception):
            connect_args["timeout"] = float(raw_ct)

    # command_timeout -> asyncpg "command_timeout"
    raw_cmd = (q.get("command_timeout") or "").strip()
    if raw_cmd:
        with contextlib.suppress(Exception):
            connect_args["command_timeout"] = float(raw_cmd)

    # statement_cache_size
    raw_scs = (q.get("statement_cache_size") or "").strip()
    if raw_scs:
        with contextlib.suppress(Exception):
            connect_args["statement_cache_size"] = int(float(raw_scs))


# ──────────────────────────────────────────────────────────────────────────────
# Selection
# ──────────────────────────────────────────────────────────────────────────────
@dataclass
class PickResult:
    # primary outputs
    dsn_sa: str
    connect_args: dict[str, Any]

    # metadata
    source: str
    host: str
    sslmode: str
    ssl_enabled: bool
    host_resolves: bool

    # optional debug fields (non-breaking)
    dsn_pg: str = ""
    port: int | None = None
    dropped_query_keys: list[str] = field(default_factory=list)
    raw_query: dict[str, str] = field(default_factory=dict)
    warnings: list[str] = field(default_factory=list)


def _first_nonempty(keys: Iterable[str]) -> tuple[str, str]:
    for key in keys:
        s = _env_str(key, "").strip()
        if s:
            return s, key
    return "", ""


def _choose_candidate() -> tuple[str, str]:
    return _first_nonempty(
        (
            "POSITIONS_PG_DSN",
            "SNIPER_DB_DSN",
            "DATABASE_URL",
            "POSTGRES_DSN",
            "PG_DSN",
            "PG_DSN_INTERNAL",
            "PG_DSN_EXTERNAL",
            "PNL_DSN",
        )
    )


def _pick_internal_external(dsn_int: str, dsn_ext: str) -> tuple[str, str]:
    """
    Choose between internal/external DSNs.
    - Prefer internal only when its hostname resolves (Render/VPC)
    - Prefer external otherwise (local dev)
    """
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
    if prefer_int:
        return dsn_int, "PG_DSN_INTERNAL(forced)"

    if internal_ok:
        return dsn_int, "PG_DSN_INTERNAL(auto)"
    return dsn_ext, "PG_DSN_EXTERNAL(auto)"


# ──────────────────────────────────────────────────────────────────────────────
# Public API
# ──────────────────────────────────────────────────────────────────────────────
def pick_pg_dsn(
    *, timeout_s: float | None = None, app_name: str | None = None
) -> tuple[str, dict[str, Any]]:
    """
    Returns:
      (dsn_sa_no_query, connect_args)

    dsn_sa_no_query is postgresql+asyncpg://... with NO query params.
    connect_args is safe for SQLAlchemy asyncpg connect_args.
    """
    pr = pick_pg_dsn_debug(timeout_s=timeout_s, app_name=app_name)
    return pr.dsn_sa, pr.connect_args


def pick_asyncpg_dsn(
    *, timeout_s: float | None = None, app_name: str | None = None
) -> tuple[str, dict[str, Any]]:
    """
    Returns:
      (dsn_pg_no_query, connect_kwargs)

    dsn_pg_no_query is postgresql://... with NO query params.
    connect_kwargs can be splatted into asyncpg.connect(dsn, **kwargs).
    """
    dsn_sa, connect_args = pick_pg_dsn(timeout_s=timeout_s, app_name=app_name)
    return _to_plain_postgresql(dsn_sa), connect_args


def pick_pg_dsn_debug(*, timeout_s: float | None = None, app_name: str | None = None) -> PickResult:
    app = (app_name or _env_str("PG_APP_NAME", "pnlsniper")).strip() or "pnlsniper"

    dsn_int = _env_str("PG_DSN_INTERNAL", "")
    dsn_ext = _env_str("PG_DSN_EXTERNAL", "")
    if dsn_int or dsn_ext:
        chosen, source = _pick_internal_external(dsn_int, dsn_ext)
        if not chosen:
            return PickResult("", {}, "", "", "", False, False)
        return _sanitize_full(chosen, source, timeout_s=timeout_s, app_name=app)

    chosen, source = _choose_candidate()
    if not chosen:
        return PickResult("", {}, "", "", "", False, False)

    return _sanitize_full(chosen, source or "ENV", timeout_s=timeout_s, app_name=app)


# Compatibility aliases (other modules/scripts probe these names) — DO NOT REMOVE
def resolve_pg_dsn(
    *, timeout_s: float | None = None, app_name: str | None = None
) -> tuple[str, dict[str, Any]]:
    return pick_pg_dsn(timeout_s=timeout_s, app_name=app_name)


def resolve_pg_dsn_debug(
    *, timeout_s: float | None = None, app_name: str | None = None
) -> PickResult:
    return pick_pg_dsn_debug(timeout_s=timeout_s, app_name=app_name)


def get_pg_dsn(
    *, timeout_s: float | None = None, app_name: str | None = None
) -> tuple[str, dict[str, Any]]:
    return pick_pg_dsn(timeout_s=timeout_s, app_name=app_name)


def get_pg_dsn_debug(*, timeout_s: float | None = None, app_name: str | None = None) -> PickResult:
    return pick_pg_dsn_debug(timeout_s=timeout_s, app_name=app_name)


# ──────────────────────────────────────────────────────────────────────────────
# Core sanitization
# ──────────────────────────────────────────────────────────────────────────────
def _sanitize_full(dsn: str, source: str, *, timeout_s: float | None, app_name: str) -> PickResult:
    sa_url = _ensure_sa_asyncpg_url(dsn)
    sa_noq, raw_q, dropped_keys = _split_strip_query(sa_url)

    host = _host_from_url(sa_noq)
    port = _port_from_url(sa_noq)
    host_ok = _resolves(host)

    sslmode = _effective_sslmode(raw_q, host)
    insecure = _env_bool("PG_SSL_INSECURE", False)

    connect_args = _connect_args_base(timeout_s, app_name)

    q_safe = {k: v for k, v in raw_q.items() if k in _SAFE_Q_KEYS}
    _apply_safe_query_overrides(connect_args, q_safe)

    ssl_ctx = _ssl_ctx_from_mode(sslmode, insecure=insecure)
    ssl_enabled = False
    if ssl_ctx is not None:
        connect_args["ssl"] = ssl_ctx
        ssl_enabled = True
    else:
        connect_args.pop("ssl", None)
        ssl_enabled = False

    pr = PickResult(
        dsn_sa=sa_noq,
        dsn_pg=_to_plain_postgresql(sa_noq),
        connect_args=connect_args,
        source=source,
        host=host,
        port=port,
        sslmode=sslmode,
        ssl_enabled=ssl_enabled,
        host_resolves=host_ok,
        dropped_query_keys=dropped_keys,
        raw_query=raw_q,
    )

    if not host:
        pr.warnings.append("no host parsed from DSN")
    if host and not host_ok:
        pr.warnings.append(f"host does not resolve: {host}")
    if insecure:
        pr.warnings.append("PG_SSL_INSECURE=1 (verification disabled)")
    if raw_q:
        pr.warnings.append("DSN had query params; all stripped (safe)")
        if any(k not in _SAFE_Q_KEYS for k in raw_q):
            pr.warnings.append("DSN query had non-allowlisted keys (ignored)")

    return pr


__all__ = [
    "PickResult",
    "mask_dsn",
    "pick_asyncpg_dsn",
    "pick_pg_dsn",
    "pick_pg_dsn_debug",
    "resolve_pg_dsn",
    "resolve_pg_dsn_debug",
    "get_pg_dsn",
    "get_pg_dsn_debug",
]
