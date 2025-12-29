# -*- coding: utf-8 -*-
from __future__ import annotations

"""
utils_pkg/ai_db.py

God-Tier AI DB brain (V3+++ / quant-grade)
==========================================

Central Postgres utility for ALL persistent AI / quant data.

Production-first guarantees
--------------------------
- Import-safe: never bricks the bot at import time (no DSN resolution on import).
- DSN resolution + normalization (Render-friendly + local-friendly).
- asyncpg SSL handling for Render: explicit SSLContext (asyncpg may not honor libpq sslmode reliably).
- Safe, idempotent schema creation (never bricks the bot).
- Narrow, documented read/write helpers (no ad-hoc SQL elsewhere).
- Quant-grade aggregations that NEVER crash the bot (soft-fail defaults).
- Async wrappers for sync funcs via asyncio.to_thread (so the asyncio bot won’t block).

Fix included
------------
- ✅ Avoids f-string backslash SyntaxError pitfalls by never placing backslash escapes inside f-string expressions.
- ✅ Uses asyncpg server_settings for timeouts and app_name (no unsafe SET string building).

Key env vars (subset)
---------------------
Enable/disable:
  AI_DB_ENABLED=1
  AI_DB_AUTODISABLE_IF_NO_DSN=1
  AI_DB_AUTODISABLE_IF_NO_DRIVER=1
  AI_DB_READ_ONLY=0

Soft-fail:
  AI_DB_SOFT_FAIL_WRITES=1
  AI_DB_SOFT_FAIL_READS=1

Connection:
  AI_DB_APP_NAME=sniper_ai_db
  AI_DB_CONNECT_TIMEOUT_S=10
  AI_DB_CONNECT_RETRIES=2
  AI_DB_CONNECT_RETRY_BASE_S=0.35
  AI_DB_CONNECT_RETRY_MAX_S=2.5
  AI_DB_STATEMENT_TIMEOUT_MS=0
  AI_DB_IDLE_IN_TX_TIMEOUT_MS=0
  AI_DB_LOCK_TIMEOUT_MS=0

SSL / DSN normalization:
  AI_DB_FORCE_SSLMODE=require|prefer|disable|allow
  AI_DB_RENDER_SSLMODE=require          (default for external Render hosts)
  AI_DB_INTERNAL_SSLMODE=prefer         (default for internal Render dpg-* hosts)

Asyncpg SSL options:
  AI_DB_SSL_INSECURE=0                  (when 1: disables cert validation; DEBUG ONLY)
  PG_SSL_INSECURE=0                     (alias)
  AI_DB_SSL_CA_FILE=                    (optional CA bundle path)

DSN resolution priority (includes your script env name PNL_DSN):
  SNIPER_DB_DSN (recommended)
  PNL_DSN
  SNIPER_DB_URL
  DATABASE_URL
  POSTGRES_DSN
  POSTGRES_URL
  PG_DSN
  PG_DSN_INTERNAL
  PG_DSN_EXTERNAL
  RENDER_INTERNAL_DATABASE_URL
  RENDER_DATABASE_URL
  SNIPER_DB_DSN_FALLBACK (last resort)

Optional asyncpg pool:
  AI_DB_USE_ASYNCPG_POOL=0
  AI_DB_ASYNCPG_POOL_MIN_SIZE=1
  AI_DB_ASYNCPG_POOL_MAX_SIZE=4
  AI_DB_ASYNCPG_POOL_MAX_INACTIVE_S=60
"""

import asyncio
import copy
import dataclasses
import hashlib
import json
import os
import random
import re
import shlex
import ssl
import time
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from functools import wraps
from typing import Any, Dict, Iterator, List, Mapping, Optional, Sequence, Tuple, Union, cast

from logging_setup import get_logger

log = get_logger()

# ---------------------------------------------------------------------
# logging safety shim (so log.every always exists)
# ---------------------------------------------------------------------
if not hasattr(log, "every"):

    def _every(_key: str, n: int = 1000) -> bool:  # pragma: no cover
        return True

    setattr(log, "every", _every)

Row = Dict[str, Any]

# =============================================================================
# Optional imports (graceful)
# =============================================================================

_PSYCOPG2_OK = True
try:
    import psycopg2  # type: ignore
    from psycopg2.extensions import connection as _PGConn, cursor as _PGCursor  # type: ignore
    from psycopg2.extras import RealDictCursor, register_default_json, register_default_jsonb  # type: ignore
    from psycopg2.extras import execute_values as _execute_values  # type: ignore
except Exception:  # pragma: no cover
    _PSYCOPG2_OK = False
    psycopg2 = None  # type: ignore
    _PGConn = Any  # type: ignore
    _PGCursor = Any  # type: ignore
    RealDictCursor = Any  # type: ignore
    register_default_json = None  # type: ignore
    register_default_jsonb = None  # type: ignore
    _execute_values = None  # type: ignore

_ASYNCPG_OK = True
try:
    import asyncpg  # type: ignore
except Exception:  # pragma: no cover
    _ASYNCPG_OK = False
    asyncpg = None  # type: ignore


# =============================================================================
# Env helpers
# =============================================================================

def _env_str(key: str, default: str = "") -> str:
    v = os.getenv(key)
    return default if v is None else str(v)


def _env_int(key: str, default: int) -> int:
    raw = os.getenv(key, "")
    if raw is None or not str(raw).strip():
        return int(default)
    try:
        return int(str(raw).strip())
    except Exception:
        return int(default)


def _env_float(key: str, default: float) -> float:
    raw = os.getenv(key, "")
    if raw is None or not str(raw).strip():
        return float(default)
    try:
        return float(str(raw).strip())
    except Exception:
        return float(default)


def _env_bool(key: str, default: bool = False) -> bool:
    raw = os.getenv(key)
    if raw is None:
        return bool(default)
    s = str(raw).strip().lower()
    if s in ("1", "true", "yes", "y", "on"):
        return True
    if s in ("0", "false", "no", "n", "off"):
        return False
    return bool(default)


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _json_dumps(o: Any) -> str:
    return json.dumps(o, separators=(",", ":"), ensure_ascii=False, default=str)


def _ensure_obj(v: Any) -> Any:
    """
    psycopg2 may return json/jsonb as dict/list (preferred) or as str/bytes.
    Normalize to dict/list where possible; otherwise safe defaults.
    """
    if v is None:
        return {}
    if isinstance(v, (dict, list)):
        return v
    if isinstance(v, (bytes, bytearray, memoryview)):
        try:
            v = bytes(v).decode("utf-8", "ignore")
        except Exception:
            return {}
    if isinstance(v, str):
        s = v.strip()
        if not s:
            return {}
        try:
            return json.loads(s)
        except Exception:
            return {}
    return v


# =============================================================================
# Config
# =============================================================================

@dataclass(frozen=True)
class _DBCfg:
    enabled: bool
    autodisable_if_no_dsn: bool
    autodisable_if_no_driver: bool
    read_only: bool
    soft_fail_writes: bool
    soft_fail_reads: bool

    app_name: str
    connect_timeout_s: int
    connect_retries: int
    connect_retry_base_s: float
    connect_retry_max_s: float

    statement_timeout_ms: int
    idle_in_tx_timeout_ms: int
    lock_timeout_ms: int

    render_sslmode: str
    internal_sslmode: str
    force_sslmode: Optional[str]

    migrations_enabled: bool
    use_migration_lock: bool
    migration_lock_id: int
    migration_lock_timeout_s: int

    default_src: Optional[str]
    default_regime: Optional[str]
    default_wallet_tag: Optional[str]
    min_feature_version: Optional[int]
    win_labels: List[str]

    ssl_insecure: bool
    ssl_ca_file: Optional[str]

    # optional asyncpg pool
    use_asyncpg_pool: bool
    asyncpg_pool_min_size: int
    asyncpg_pool_max_size: int
    asyncpg_pool_max_inactive_s: int


_DSN_ENV_PRIMARY = "SNIPER_DB_DSN"
_DSN_ENV_CANDIDATES: List[str] = [
    _DSN_ENV_PRIMARY,
    "PNL_DSN",  # your scripts/check_schema.py uses this
    "SNIPER_DB_URL",
    "DATABASE_URL",
    "POSTGRES_DSN",
    "POSTGRES_URL",
    "PG_DSN",
    "PG_DSN_INTERNAL",
    "PG_DSN_EXTERNAL",
    "RENDER_INTERNAL_DATABASE_URL",
    "RENDER_DATABASE_URL",
]
DB_URL_FALLBACK = _env_str("SNIPER_DB_DSN_FALLBACK", "").strip()


def _has_any_dsn_env() -> bool:
    for k in _DSN_ENV_CANDIDATES:
        v = os.environ.get(k)
        if v and str(v).strip():
            return True
    return bool(DB_URL_FALLBACK)


def _load_cfg() -> _DBCfg:
    enabled = _env_bool("AI_DB_ENABLED", True)
    autodisable_if_no_dsn = _env_bool("AI_DB_AUTODISABLE_IF_NO_DSN", True)
    autodisable_if_no_driver = _env_bool("AI_DB_AUTODISABLE_IF_NO_DRIVER", True)
    read_only = _env_bool("AI_DB_READ_ONLY", False)

    soft_fail_writes = _env_bool("AI_DB_SOFT_FAIL_WRITES", True)
    soft_fail_reads = _env_bool("AI_DB_SOFT_FAIL_READS", True)

    app_name = _env_str("AI_DB_APP_NAME", "sniper_ai_db").strip() or "sniper_ai_db"
    connect_timeout_s = _env_int("AI_DB_CONNECT_TIMEOUT_S", 10)

    connect_retries = max(0, min(_env_int("AI_DB_CONNECT_RETRIES", 2), 10))
    connect_retry_base_s = max(0.05, min(_env_float("AI_DB_CONNECT_RETRY_BASE_S", 0.35), 10.0))
    connect_retry_max_s = max(connect_retry_base_s, min(_env_float("AI_DB_CONNECT_RETRY_MAX_S", 2.5), 30.0))

    statement_timeout_ms = _env_int("AI_DB_STATEMENT_TIMEOUT_MS", 0)
    idle_in_tx_timeout_ms = _env_int("AI_DB_IDLE_IN_TX_TIMEOUT_MS", 0)
    lock_timeout_ms = _env_int("AI_DB_LOCK_TIMEOUT_MS", 0)

    render_sslmode = (_env_str("AI_DB_RENDER_SSLMODE", "require").strip().lower() or "require")
    internal_sslmode = (_env_str("AI_DB_INTERNAL_SSLMODE", "prefer").strip().lower() or "prefer")
    force_sslmode = (_env_str("AI_DB_FORCE_SSLMODE", "").strip().lower() or None)

    migrations_enabled = _env_bool("AI_DB_MIGRATIONS_ENABLED", True)
    use_migration_lock = _env_bool("AI_DB_USE_MIGRATION_LOCK", True)
    migration_lock_id = int(_env_str("AI_DB_MIGRATION_LOCK_ID", "907331").strip() or "907331")
    migration_lock_timeout_s = _env_int("AI_DB_MIGRATION_LOCK_TIMEOUT_S", 20)

    default_src = _env_str("AI_DB_DEFAULT_SRC", "").strip() or None
    default_regime = _env_str("AI_DB_DEFAULT_REGIME", "").strip() or None
    default_wallet_tag = _env_str("AI_DB_DEFAULT_WALLET_TAG", "").strip() or None

    min_feat_raw = _env_str("AI_DB_MIN_FEATURE_VERSION", "").strip()
    if min_feat_raw:
        try:
            min_feature_version: Optional[int] = int(min_feat_raw)
        except Exception:
            log.warning("AI_DB_MIN_FEATURE_VERSION is not a valid int: %r", min_feat_raw)
            min_feature_version = None
    else:
        min_feature_version = None

    win_env = _env_str("AI_DB_WIN_LABELS", "").strip()
    if win_env:
        win_labels = [s.strip().lower() for s in win_env.split(",") if s.strip()]
    else:
        win_labels = ["good", "win", "green", "profit"]

    ssl_insecure = _env_bool("AI_DB_SSL_INSECURE", False) or _env_bool("PG_SSL_INSECURE", False)
    ssl_ca_file = _env_str("AI_DB_SSL_CA_FILE", "").strip() or None

    use_asyncpg_pool = _env_bool("AI_DB_USE_ASYNCPG_POOL", False)
    asyncpg_pool_min_size = max(1, min(_env_int("AI_DB_ASYNCPG_POOL_MIN_SIZE", 1), 32))
    asyncpg_pool_max_size = max(asyncpg_pool_min_size, min(_env_int("AI_DB_ASYNCPG_POOL_MAX_SIZE", 4), 64))
    asyncpg_pool_max_inactive_s = max(5, min(_env_int("AI_DB_ASYNCPG_POOL_MAX_INACTIVE_S", 60), 3600))

    # Auto-disable if enabled but no DSN provided
    if enabled and autodisable_if_no_dsn and not _has_any_dsn_env():
        if getattr(log, "every")("ai_db_no_dsn_autodisable", 1_000_000):
            log.warning(
                "ai_db: enabled but no DSN env found; auto-disabling "
                "(set AI_DB_AUTODISABLE_IF_NO_DSN=0 to override)"
            )
        enabled = False

    # Auto-disable only if BOTH drivers missing
    if enabled and autodisable_if_no_driver and (not _PSYCOPG2_OK) and (not _ASYNCPG_OK):
        if getattr(log, "every")("ai_db_no_driver_autodisable", 1_000_000):
            log.warning(
                "ai_db: enabled but neither psycopg2 nor asyncpg is installed; auto-disabling "
                "(install psycopg2-binary and/or asyncpg, or set AI_DB_AUTODISABLE_IF_NO_DRIVER=0)"
            )
        enabled = False
    elif enabled and autodisable_if_no_driver and (not _PSYCOPG2_OK) and _ASYNCPG_OK:
        if getattr(log, "every")("ai_db_psycopg2_missing_warn", 1_000_000):
            log.warning("ai_db: psycopg2 missing; sync DB helpers may soft-fail. Install psycopg2-binary.")

    return _DBCfg(
        enabled=enabled,
        autodisable_if_no_dsn=autodisable_if_no_dsn,
        autodisable_if_no_driver=autodisable_if_no_driver,
        read_only=read_only,
        soft_fail_writes=soft_fail_writes,
        soft_fail_reads=soft_fail_reads,
        app_name=app_name,
        connect_timeout_s=connect_timeout_s,
        connect_retries=connect_retries,
        connect_retry_base_s=connect_retry_base_s,
        connect_retry_max_s=connect_retry_max_s,
        statement_timeout_ms=statement_timeout_ms,
        idle_in_tx_timeout_ms=idle_in_tx_timeout_ms,
        lock_timeout_ms=lock_timeout_ms,
        render_sslmode=render_sslmode,
        internal_sslmode=internal_sslmode,
        force_sslmode=force_sslmode,
        migrations_enabled=migrations_enabled,
        use_migration_lock=use_migration_lock,
        migration_lock_id=migration_lock_id,
        migration_lock_timeout_s=migration_lock_timeout_s,
        default_src=default_src,
        default_regime=default_regime,
        default_wallet_tag=default_wallet_tag,
        min_feature_version=min_feature_version,
        win_labels=win_labels,
        ssl_insecure=ssl_insecure,
        ssl_ca_file=ssl_ca_file,
        use_asyncpg_pool=use_asyncpg_pool,
        asyncpg_pool_min_size=asyncpg_pool_min_size,
        asyncpg_pool_max_size=asyncpg_pool_max_size,
        asyncpg_pool_max_inactive_s=asyncpg_pool_max_inactive_s,
    )


CFG = _load_cfg()

AI_DB_ENABLED = CFG.enabled
AI_DB_READ_ONLY = CFG.read_only
AI_DB_SOFT_FAIL_WRITES = CFG.soft_fail_writes
AI_DB_SOFT_FAIL_READS = CFG.soft_fail_reads

AI_DB_APP_NAME = CFG.app_name
AI_DB_CONNECT_TIMEOUT_S = CFG.connect_timeout_s
AI_DB_CONNECT_RETRIES = CFG.connect_retries
AI_DB_CONNECT_RETRY_BASE_S = CFG.connect_retry_base_s
AI_DB_CONNECT_RETRY_MAX_S = CFG.connect_retry_max_s

AI_DB_STATEMENT_TIMEOUT_MS = CFG.statement_timeout_ms
AI_DB_IDLE_IN_TX_TIMEOUT_MS = CFG.idle_in_tx_timeout_ms
AI_DB_LOCK_TIMEOUT_MS = CFG.lock_timeout_ms

AI_DB_RENDER_SSLMODE = CFG.render_sslmode
AI_DB_INTERNAL_SSLMODE = CFG.internal_sslmode
AI_DB_FORCE_SSLMODE = CFG.force_sslmode

AI_DB_MIGRATIONS_ENABLED = CFG.migrations_enabled
AI_DB_USE_MIGRATION_LOCK = CFG.use_migration_lock
AI_DB_MIGRATION_LOCK_ID = CFG.migration_lock_id
AI_DB_MIGRATION_LOCK_TIMEOUT_S = CFG.migration_lock_timeout_s

AI_DB_DEFAULT_SRC = CFG.default_src
AI_DB_DEFAULT_REGIME = CFG.default_regime
AI_DB_DEFAULT_WALLET_TAG = CFG.default_wallet_tag
AI_DB_MIN_FEATURE_VERSION = CFG.min_feature_version
WIN_LABELS: List[str] = list(CFG.win_labels)

AI_DB_SSL_INSECURE = CFG.ssl_insecure
AI_DB_SSL_CA_FILE = CFG.ssl_ca_file

AI_DB_USE_ASYNCPG_POOL = CFG.use_asyncpg_pool
AI_DB_ASYNCPG_POOL_MIN_SIZE = CFG.asyncpg_pool_min_size
AI_DB_ASYNCPG_POOL_MAX_SIZE = CFG.asyncpg_pool_max_size
AI_DB_ASYNCPG_POOL_MAX_INACTIVE_S = CFG.asyncpg_pool_max_inactive_s


# =============================================================================
# DSN resolution / normalization + SSL inference
# =============================================================================

@dataclass(frozen=True)
class DSNInfo:
    source: str
    raw: str
    normalized_libpq: str
    normalized_asyncpg: str
    normalized_sqlalchemy_async: str
    host: Optional[str]
    db: Optional[str]
    sslmode: Optional[str]
    is_render_external: bool
    is_render_internal: bool


_NORMALIZED: Optional[DSNInfo] = None


def reset_dsn_cache() -> None:
    global _NORMALIZED
    _NORMALIZED = None


def _effective_min_feature_version(explicit: Optional[int]) -> Optional[int]:
    return explicit if explicit is not None else AI_DB_MIN_FEATURE_VERSION


def _looks_like_internal_render_host(host: Optional[str]) -> bool:
    if not host:
        return False
    h = host.lower()
    # Render internal hosts are often like: dpg-xxxx-a (no dot)
    return h.startswith("dpg-") and "." not in h


def _is_render_external_host(host: Optional[str]) -> bool:
    if not host:
        return False
    h = host.lower()
    return ("postgres.render.com" in h) or (h.endswith(".render.com")) or ("render.com" in h)


def _drop_query_key(q: str, key: str) -> str:
    q2 = re.sub(rf"(^|&){re.escape(key)}=[^&]*", "", q).strip("&")
    q2 = re.sub(r"&&+", "&", q2).strip("&")
    return q2


def _mask_dsn(raw: str) -> str:
    raw = (raw or "").strip()
    if not raw:
        return "<empty>"
    if "://" not in raw:
        try:
            kv = _parse_kv_dsn(raw)
            host = kv.get("host") or "<host>"
            port = f":{kv.get('port')}" if kv.get("port") else ""
            db = kv.get("dbname") or kv.get("database") or "<db>"
            return f"{host}{port}/{db}"
        except Exception:
            return "<kv-dsn>"
    try:
        import urllib.parse as urlparse

        p = urlparse.urlparse(raw)
        host = p.hostname or "<host>"
        port = f":{p.port}" if p.port else ""
        db = (p.path or "").lstrip("/") or "<db>"
        return f"{host}{port}/{db}"
    except Exception:
        return "<dsn>"


def _parse_kv_dsn(raw: str) -> Dict[str, str]:
    s = (raw or "").strip()
    if not s:
        return {}
    parts = shlex.split(s)
    out: Dict[str, str] = {}
    for token in parts:
        if "=" not in token:
            continue
        k, v = token.split("=", 1)
        k = k.strip()
        v = v.strip()
        if k:
            out[k] = v
    return out


def _kv_to_url(kv: Mapping[str, str]) -> str:
    import urllib.parse as urlparse

    host = kv.get("host") or kv.get("hostname") or ""
    db = kv.get("dbname") or kv.get("database") or ""
    user = kv.get("user") or ""
    password = kv.get("password") or ""
    port = kv.get("port") or ""

    if not host or not db:
        raise RuntimeError("kv DSN missing host/dbname; cannot convert to URL for asyncpg")

    userinfo = ""
    if user:
        userinfo = urlparse.quote(user, safe="")
        if password:
            userinfo += ":" + urlparse.quote(password, safe="")
        userinfo += "@"

    netloc = f"{userinfo}{host}"
    if port:
        netloc += f":{port}"

    q: List[str] = []
    sslmode = kv.get("sslmode") or ""
    if sslmode:
        q.append(f"sslmode={urlparse.quote(sslmode, safe='')}")
    if kv.get("application_name"):
        q.append(f"application_name={urlparse.quote(kv['application_name'], safe='')}")
    if kv.get("connect_timeout"):
        q.append(f"connect_timeout={urlparse.quote(kv['connect_timeout'], safe='')}")

    query = "&".join(q)
    return f"postgresql://{netloc}/{urlparse.quote(db, safe='')}" + (f"?{query}" if query else "")


def _normalize_kv_dsn(raw: str) -> Tuple[str, str, Optional[str], Optional[str], Optional[str], bool, bool]:
    s = (raw or "").strip()
    if not s:
        raise RuntimeError("Empty DB DSN")

    kv = _parse_kv_dsn(s)

    host = kv.get("host") or kv.get("hostname")
    db = kv.get("dbname") or kv.get("database")

    is_ext = _is_render_external_host(host)
    is_int = _looks_like_internal_render_host(host)

    sslmode = kv.get("sslmode")
    if AI_DB_FORCE_SSLMODE:
        kv["sslmode"] = AI_DB_FORCE_SSLMODE
        sslmode = AI_DB_FORCE_SSLMODE
    else:
        if not sslmode:
            if is_ext:
                kv["sslmode"] = AI_DB_RENDER_SSLMODE
                sslmode = AI_DB_RENDER_SSLMODE
            elif is_int:
                kv["sslmode"] = AI_DB_INTERNAL_SSLMODE
                sslmode = AI_DB_INTERNAL_SSLMODE

    if not kv.get("application_name"):
        kv["application_name"] = AI_DB_APP_NAME

    ordered_keys = ["host", "port", "dbname", "user", "password", "sslmode", "application_name"]
    seen = set(ordered_keys)
    tail = [k for k in kv.keys() if k not in seen]
    keys = ordered_keys + sorted(tail)

    def _q(v: str) -> str:
        if re.search(r"\s", v):
            return "'" + v.replace("'", "\\'") + "'"
        return v

    libpq = " ".join([f"{k}={_q(kv[k])}" for k in keys if k in kv and str(kv[k]).strip()])
    url = _kv_to_url(kv)
    return libpq, url, host, db, sslmode, is_ext, is_int


def _normalize_url_dsn(raw: str) -> Tuple[str, Optional[str], Optional[str], Optional[str], bool, bool]:
    import urllib.parse as urlparse

    raw = (raw or "").strip()
    if not raw:
        raise RuntimeError("Empty DB DSN")

    parsed = urlparse.urlparse(raw)

    scheme = parsed.scheme or "postgresql"
    if "+" in scheme:
        scheme = scheme.split("+", 1)[0]
    if scheme == "postgres":
        scheme = "postgresql"
    if not scheme.startswith("postgres"):
        scheme = "postgresql"

    host = getattr(parsed, "hostname", None)
    db = (parsed.path or "").lstrip("/") or None
    is_ext = _is_render_external_host(host)
    is_int = _looks_like_internal_render_host(host)

    query = parsed.query or ""
    sslmode: Optional[str] = None
    m = re.search(r"(^|&)sslmode=([^&]+)", query)
    if m:
        sslmode = (m.group(2) or "").strip() or None

    if AI_DB_FORCE_SSLMODE:
        query = _drop_query_key(query, "sslmode")
        query = (query + "&" if query else "") + f"sslmode={AI_DB_FORCE_SSLMODE}"
        sslmode = AI_DB_FORCE_SSLMODE
    else:
        if sslmode is None:
            if is_ext:
                sslmode = AI_DB_RENDER_SSLMODE
                query = (query + "&" if query else "") + f"sslmode={AI_DB_RENDER_SSLMODE}"
            elif is_int:
                sslmode = AI_DB_INTERNAL_SSLMODE
                query = (query + "&" if query else "") + f"sslmode={AI_DB_INTERNAL_SSLMODE}"

    if re.search(r"(^|&)application_name=", query) is None:
        query = (query + "&" if query else "") + f"application_name={AI_DB_APP_NAME}"

    normalized = urlparse.urlunparse((scheme, parsed.netloc, parsed.path, "", query, ""))
    return normalized, host, db, sslmode, is_ext, is_int


def _normalize_dsn(raw: str) -> DSNInfo:
    raw = (raw or "").strip()
    if not raw:
        raise RuntimeError("Empty DB DSN")

    if "://" not in raw:
        libpq, url, host, db, sslmode, is_ext, is_int = _normalize_kv_dsn(raw)
        normalized_sqlalchemy = url.replace("postgresql://", "postgresql+asyncpg://", 1)
        return DSNInfo(
            source="<kv>",
            raw=raw,
            normalized_libpq=libpq,
            normalized_asyncpg=url,
            normalized_sqlalchemy_async=normalized_sqlalchemy,
            host=host,
            db=db,
            sslmode=sslmode,
            is_render_external=is_ext,
            is_render_internal=is_int,
        )

    libpq_url, host, db, sslmode, is_ext, is_int = _normalize_url_dsn(raw)
    asyncpg_url = libpq_url
    normalized_sqlalchemy = libpq_url.replace("postgresql://", "postgresql+asyncpg://", 1)

    return DSNInfo(
        source="<url>",
        raw=raw,
        normalized_libpq=libpq_url,
        normalized_asyncpg=asyncpg_url,
        normalized_sqlalchemy_async=normalized_sqlalchemy,
        host=host,
        db=db,
        sslmode=sslmode,
        is_render_external=is_ext,
        is_render_internal=is_int,
    )


def _resolve_raw_dsn() -> Tuple[str, str]:
    for key in _DSN_ENV_CANDIDATES:
        v = os.environ.get(key)
        if v and str(v).strip():
            return str(v).strip(), key
    if DB_URL_FALLBACK:
        return DB_URL_FALLBACK, "SNIPER_DB_DSN_FALLBACK"
    raise RuntimeError(
        "No Postgres DSN found. Set SNIPER_DB_DSN (recommended) or one of: "
        f"{_DSN_ENV_CANDIDATES[1:]} or SNIPER_DB_DSN_FALLBACK."
    )


def resolve_dsn_info() -> DSNInfo:
    global _NORMALIZED
    if _NORMALIZED is not None:
        return _NORMALIZED

    raw, source = _resolve_raw_dsn()
    info = _normalize_dsn(raw)
    info = dataclasses.replace(info, source=source)
    _NORMALIZED = info

    log.info("ai_db: using DSN from %s (masked): %s", source, _mask_dsn(info.normalized_libpq))
    return info


def get_normalized_dsn_for_debug() -> str:
    try:
        return _mask_dsn(resolve_dsn_info().normalized_libpq)
    except Exception as e:
        return f"<unresolved: {e}>"


def describe_dsn_sources() -> Dict[str, Any]:
    candidates: Dict[str, str] = {}
    for key in _DSN_ENV_CANDIDATES + (["SNIPER_DB_DSN_FALLBACK"] if DB_URL_FALLBACK else []):
        raw = os.environ.get(key) if key != "SNIPER_DB_DSN_FALLBACK" else DB_URL_FALLBACK
        if raw and str(raw).strip():
            candidates[key] = _mask_dsn(str(raw).strip())

    selected = "<unresolved>"
    selected_source = None
    sslmode = None
    host = None
    try:
        info = resolve_dsn_info()
        selected = _mask_dsn(info.normalized_libpq)
        selected_source = info.source
        sslmode = info.sslmode
        host = info.host
    except Exception:
        pass

    return {
        "enabled": bool(AI_DB_ENABLED),
        "read_only": bool(AI_DB_READ_ONLY),
        "selected_source": selected_source,
        "selected": selected,
        "host": host,
        "sslmode": sslmode,
        "candidates": candidates,
        "migrations_enabled": bool(AI_DB_MIGRATIONS_ENABLED),
        "psycopg2_ok": bool(_PSYCOPG2_OK),
        "asyncpg_ok": bool(_ASYNCPG_OK),
        "use_asyncpg_pool": bool(AI_DB_USE_ASYNCPG_POOL),
    }


# =============================================================================
# SSL helpers for asyncpg
# =============================================================================

def dsn_wants_ssl(dsn: Optional[str] = None) -> bool:
    try:
        info = resolve_dsn_info() if not dsn else _normalize_dsn(dsn)
        sm = (info.sslmode or "").lower().strip()
        if sm == "disable":
            return False
        if sm in {"require", "verify-ca", "verify-full"}:
            return True
        if info.is_render_external or info.is_render_internal:
            # asyncpg doesn’t “prefer” downgrade in a safe way; prefer TLS by default on Render.
            return True
    except Exception:
        pass
    return False


def ssl_context_for_dsn(dsn: Optional[str] = None) -> Optional[ssl.SSLContext]:
    if not dsn_wants_ssl(dsn):
        return None

    ctx = ssl.create_default_context()

    if AI_DB_SSL_CA_FILE:
        try:
            ctx.load_verify_locations(cafile=AI_DB_SSL_CA_FILE)
        except Exception as e:
            log.warning("ai_db: failed to load AI_DB_SSL_CA_FILE=%r: %s", AI_DB_SSL_CA_FILE, e)

    if AI_DB_SSL_INSECURE:
        # DEBUG ONLY
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE

    return ctx


def _asyncpg_server_settings(statement_timeout_ms: Optional[int] = None) -> Dict[str, str]:
    stm = statement_timeout_ms if statement_timeout_ms is not None else AI_DB_STATEMENT_TIMEOUT_MS
    ss: Dict[str, str] = {"application_name": AI_DB_APP_NAME, "timezone": "UTC"}
    if stm and int(stm) > 0:
        ss["statement_timeout"] = str(int(stm))
    if AI_DB_IDLE_IN_TX_TIMEOUT_MS and int(AI_DB_IDLE_IN_TX_TIMEOUT_MS) > 0:
        ss["idle_in_transaction_session_timeout"] = str(int(AI_DB_IDLE_IN_TX_TIMEOUT_MS))
    if AI_DB_LOCK_TIMEOUT_MS and int(AI_DB_LOCK_TIMEOUT_MS) > 0:
        ss["lock_timeout"] = str(int(AI_DB_LOCK_TIMEOUT_MS))
    return ss


async def asyncpg_connect(
    dsn: Optional[str] = None,
    *,
    timeout: Optional[float] = None,
    statement_timeout_ms: Optional[int] = None,
) -> Any:
    """
    Central asyncpg connector with correct TLS behavior for Render.
    Returns asyncpg.Connection.

    Use this in scripts/check_schema.py etc.
    """
    if not _ASYNCPG_OK:
        raise RuntimeError("asyncpg not installed. pip install asyncpg")
    if not AI_DB_ENABLED:
        raise RuntimeError("AI DB disabled (AI_DB_ENABLED=0)")

    info = resolve_dsn_info() if dsn is None else _normalize_dsn(dsn)
    connect_timeout = float(timeout if timeout is not None else AI_DB_CONNECT_TIMEOUT_S)

    ssl_ctx = ssl_context_for_dsn(info.normalized_asyncpg)
    server_settings = _asyncpg_server_settings(statement_timeout_ms)

    last_err: Optional[BaseException] = None
    attempts = 1 + int(AI_DB_CONNECT_RETRIES)

    for i in range(1, attempts + 1):
        try:
            conn = await asyncpg.connect(  # type: ignore[union-attr]
                info.normalized_asyncpg,
                timeout=connect_timeout,
                ssl=ssl_ctx,
                server_settings=server_settings,
            )
            try:
                await conn.execute("SELECT 1")
            except Exception:
                pass
            return conn
        except Exception as e:
            last_err = e
            if i >= attempts:
                break
            if getattr(log, "every")("ai_db_asyncpg_connect_retry", 10):
                log.warning("ai_db: asyncpg connect failed (attempt %s/%s): %s", i, attempts, e)
            base = float(AI_DB_CONNECT_RETRY_BASE_S)
            cap = float(AI_DB_CONNECT_RETRY_MAX_S)
            delay = min(cap, base * (2.0 ** max(0, i - 1)))
            delay *= (0.9 + 0.2 * random.random())
            await asyncio.sleep(delay)

    assert last_err is not None
    raise last_err


_ASYNCPG_POOL: Any = None


async def get_asyncpg_pool() -> Any:
    """
    Optional shared asyncpg pool (disabled by default).
    If enabled, this is the fastest/safest path for high-frequency async writes.
    """
    global _ASYNCPG_POOL
    if _ASYNCPG_POOL is not None:
        return _ASYNCPG_POOL

    if not _ASYNCPG_OK:
        raise RuntimeError("asyncpg not installed. pip install asyncpg")
    if not AI_DB_ENABLED:
        raise RuntimeError("AI DB disabled (AI_DB_ENABLED=0)")
    if not AI_DB_USE_ASYNCPG_POOL:
        raise RuntimeError("AI_DB_USE_ASYNCPG_POOL=0 (pool disabled)")

    info = resolve_dsn_info()
    ssl_ctx = ssl_context_for_dsn(info.normalized_asyncpg)
    server_settings = _asyncpg_server_settings(None)

    _ASYNCPG_POOL = await asyncpg.create_pool(  # type: ignore[union-attr]
        dsn=info.normalized_asyncpg,
        min_size=int(AI_DB_ASYNCPG_POOL_MIN_SIZE),
        max_size=int(AI_DB_ASYNCPG_POOL_MAX_SIZE),
        max_inactive_connection_lifetime=float(AI_DB_ASYNCPG_POOL_MAX_INACTIVE_S),
        timeout=float(AI_DB_CONNECT_TIMEOUT_S),
        ssl=ssl_ctx,
        server_settings=server_settings,
    )
    return _ASYNCPG_POOL


async def close_asyncpg_pool() -> None:
    global _ASYNCPG_POOL
    if _ASYNCPG_POOL is not None:
        try:
            await _ASYNCPG_POOL.close()
        except Exception:
            pass
        _ASYNCPG_POOL = None


# =============================================================================
# psycopg2 connection helpers
# =============================================================================

def _require_psycopg2() -> None:
    if _PSYCOPG2_OK:
        return
    raise RuntimeError(
        "psycopg2 is not available. Install psycopg2-binary (or psycopg2) "
        "or set AI_DB_ENABLED=0 to disable DB features."
    )


def _apply_session_settings(conn: _PGConn) -> None:
    """
    Apply session-level settings. Best-effort and parameterized (no unsafe SQL building).
    """
    try:
        with conn.cursor() as cur:
            cur.execute("SET application_name = %s", (AI_DB_APP_NAME,))
            cur.execute("SET TIME ZONE 'UTC'")
            if AI_DB_STATEMENT_TIMEOUT_MS > 0:
                cur.execute("SET statement_timeout = %s", (int(AI_DB_STATEMENT_TIMEOUT_MS),))
            if AI_DB_IDLE_IN_TX_TIMEOUT_MS > 0:
                cur.execute(
                    "SET idle_in_transaction_session_timeout = %s",
                    (int(AI_DB_IDLE_IN_TX_TIMEOUT_MS),),
                )
            if AI_DB_LOCK_TIMEOUT_MS > 0:
                cur.execute("SET lock_timeout = %s", (int(AI_DB_LOCK_TIMEOUT_MS),))
    except Exception:
        log.exception("ai_db: failed to apply session settings")


def _sleep_backoff(attempt: int) -> None:
    base = float(AI_DB_CONNECT_RETRY_BASE_S)
    cap = float(AI_DB_CONNECT_RETRY_MAX_S)
    delay = min(cap, base * (2.0 ** max(0, attempt - 1)))
    delay *= (0.9 + 0.2 * random.random())
    time.sleep(delay)


def _connect_once() -> _PGConn:
    if not AI_DB_ENABLED:
        raise RuntimeError("AI DB disabled (AI_DB_ENABLED=0)")
    _require_psycopg2()

    info = resolve_dsn_info()
    dsn = info.normalized_libpq

    conn = cast(_PGConn, psycopg2.connect(dsn, connect_timeout=int(AI_DB_CONNECT_TIMEOUT_S)))  # type: ignore[union-attr]

    try:
        if register_default_json is not None:
            register_default_json(conn, loads=json.loads)
        if register_default_jsonb is not None:
            register_default_jsonb(conn, loads=json.loads)
    except Exception:
        pass

    _apply_session_settings(conn)
    return conn


def _connect() -> _PGConn:
    last_err: Optional[BaseException] = None
    attempts = 1 + int(AI_DB_CONNECT_RETRIES)
    for i in range(1, attempts + 1):
        try:
            return _connect_once()
        except Exception as e:
            last_err = e
            if i >= attempts:
                break
            if getattr(log, "every")("ai_db_connect_retry", 10):
                log.warning("ai_db: connect failed (attempt %s/%s): %s", i, attempts, e)
            _sleep_backoff(i)
    assert last_err is not None
    raise last_err


@contextmanager
def conn_cursor() -> Iterator[Tuple[_PGConn, _PGCursor]]:
    conn = _connect()
    try:
        with conn.cursor() as cur:
            yield conn, cur
        conn.commit()
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass
        log.exception("ai_db: DB error")
        raise
    finally:
        try:
            conn.close()
        except Exception:
            pass


@contextmanager
def conn_dict_cursor() -> Iterator[Tuple[_PGConn, RealDictCursor]]:
    conn = _connect()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:  # type: ignore[arg-type]
            yield conn, cur
        conn.commit()
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass
        log.exception("ai_db: DB error")
        raise
    finally:
        try:
            conn.close()
        except Exception:
            pass


@contextmanager
def cursor_only() -> Iterator[_PGCursor]:
    with conn_cursor() as (_c, cur):
        yield cur


@contextmanager
def dict_cursor_only() -> Iterator[RealDictCursor]:
    with conn_dict_cursor() as (_c, cur):
        yield cur


# =============================================================================
# Soft-fail decorators (deep-copy safe)
# =============================================================================

def _default_copy(v: Any) -> Any:
    try:
        return copy.deepcopy(v)
    except Exception:
        return v


def _softfail_write(default: Any):
    def deco(fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            if not AI_DB_ENABLED or AI_DB_READ_ONLY:
                return _default_copy(default)
            try:
                return fn(*args, **kwargs)
            except Exception:
                if AI_DB_SOFT_FAIL_WRITES:
                    if getattr(log, "every")("ai_db_write_fail", 50):
                        log.exception("ai_db: write failed in %s (soft-fail)", fn.__name__)
                    return _default_copy(default)
                raise

        return wrapper

    return deco


def _softfail_read(default: Any):
    def deco(fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            if not AI_DB_ENABLED:
                return _default_copy(default)
            try:
                return fn(*args, **kwargs)
            except Exception:
                if AI_DB_SOFT_FAIL_READS:
                    if getattr(log, "every")("ai_db_read_fail", 50):
                        log.exception("ai_db: read failed in %s (soft-fail)", fn.__name__)
                    return _default_copy(default)
                raise

        return wrapper

    return deco


# =============================================================================
# Migration lock
# =============================================================================

@contextmanager
def _migration_lock(cur: _PGCursor) -> Iterator[None]:
    """
    Advisory lock to prevent concurrent schema migrations across multiple instances.
    Best-effort: if timeout expires, proceed anyway (migrations are idempotent).
    """
    if not AI_DB_USE_MIGRATION_LOCK:
        yield
        return

    lock_id = int(AI_DB_MIGRATION_LOCK_ID)
    deadline = time.time() + max(1, int(AI_DB_MIGRATION_LOCK_TIMEOUT_S))

    got = False
    try:
        while time.time() < deadline:
            cur.execute("SELECT pg_try_advisory_lock(%s)", (lock_id,))
            row = cur.fetchone()
            if row and bool(row[0]):
                got = True
                break
            time.sleep(0.25)

        if not got:
            log.warning("ai_db: migration lock timeout; proceeding without lock (idempotent)")

        yield
    finally:
        if got:
            try:
                cur.execute("SELECT pg_advisory_unlock(%s)", (lock_id,))
            except Exception:
                pass


# =============================================================================
# Phase 0: Health / simple utilities
# =============================================================================

def ping() -> bool:
    if not AI_DB_ENABLED or not _PSYCOPG2_OK:
        return False
    try:
        with cursor_only() as cur:
            cur.execute("SELECT 1")
            row = cur.fetchone()
            return bool(row and int(row[0]) == 1)
    except Exception:
        log.exception("ai_db.ping() failed")
        return False


def describe_backend() -> str:
    dsn_mask = get_normalized_dsn_for_debug()
    if not AI_DB_ENABLED:
        return f"dsn={dsn_mask}, server=<disabled>, ping=False"

    if not _PSYCOPG2_OK:
        return f"dsn={dsn_mask}, server=<no-psycopg2>, ping=False"

    version = "unknown"
    ok = False
    try:
        with cursor_only() as cur:
            cur.execute("SHOW server_version")
            row = cur.fetchone()
            if row and row[0]:
                version = str(row[0])
            cur.execute("SELECT 1")
            row2 = cur.fetchone()
            ok = bool(row2 and int(row2[0]) == 1)
    except Exception as e:
        log.warning("describe_backend: failed: %s", e)
        ok = False

    return f"dsn={dsn_mask}, server={version}, ping={ok}"


# =============================================================================
# Phase 1: Schema / migrations
# =============================================================================

_SCHEMA_VERSION = "2"  # v2 adds exit_events + DSN env compat (code-only)


def ensure_ai_tables() -> None:
    """
    Create tables if they don't exist, and add missing columns/indexes safely.
    Safe to call repeatedly.

    Guardrails:
      - skips if AI_DB_MIGRATIONS_ENABLED=0
      - skips if AI_DB_READ_ONLY=1
      - requires psycopg2 for migration path
    """
    if not AI_DB_ENABLED or AI_DB_READ_ONLY or not AI_DB_MIGRATIONS_ENABLED:
        return
    if not _PSYCOPG2_OK:
        log.warning("ai_db.ensure_ai_tables: psycopg2 missing; skipping migrations (install psycopg2-binary).")
        return

    with conn_cursor() as (_conn, cur):
        with _migration_lock(cur):
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS ai_schema_meta (
                    k  TEXT PRIMARY KEY,
                    v  TEXT,
                    ts TIMESTAMPTZ NOT NULL DEFAULT now()
                );
                """
            )
            cur.execute(
                """
                INSERT INTO ai_schema_meta (k, v)
                VALUES ('schema_version', %s)
                ON CONFLICT (k) DO UPDATE SET v = EXCLUDED.v, ts = now();
                """,
                (_SCHEMA_VERSION,),
            )

            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS entry_candidates (
                    id              BIGSERIAL PRIMARY KEY,
                    ts              TIMESTAMPTZ NOT NULL DEFAULT now(),
                    src             TEXT NOT NULL,
                    mint            TEXT NOT NULL,
                    symbol          TEXT,
                    regime          TEXT,
                    wallet_tag      TEXT,
                    session_id      TEXT,
                    feature_version INT NOT NULL,
                    features        JSONB NOT NULL DEFAULT '{}'::jsonb,
                    raw             JSONB NOT NULL DEFAULT '{}'::jsonb
                );
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS entry_outcomes (
                    id           BIGSERIAL PRIMARY KEY,
                    candidate_id BIGINT UNIQUE REFERENCES entry_candidates(id) ON DELETE CASCADE,
                    label        TEXT NOT NULL,
                    pnl_pct      DOUBLE PRECISION,
                    hold_sec     INTEGER,
                    notes        TEXT,
                    extra        JSONB NOT NULL DEFAULT '{}'::jsonb,
                    ts           TIMESTAMPTZ NOT NULL DEFAULT now()
                );
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS trade_executions (
                    id           BIGSERIAL PRIMARY KEY,
                    ts           TIMESTAMPTZ NOT NULL DEFAULT now(),
                    trade_id     TEXT NOT NULL,
                    mint         TEXT NOT NULL,
                    side         TEXT NOT NULL,
                    qty_raw      NUMERIC,
                    price_usd    DOUBLE PRECISION,
                    slippage_bps DOUBLE PRECISION,
                    fees_usd     DOUBLE PRECISION,
                    tx_sig       TEXT,
                    candidate_id BIGINT REFERENCES entry_candidates(id) ON DELETE SET NULL,
                    session_id   TEXT,
                    extra        JSONB NOT NULL DEFAULT '{}'::jsonb
                );
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS exit_events (
                    id           BIGSERIAL PRIMARY KEY,
                    ts           TIMESTAMPTZ NOT NULL DEFAULT now(),
                    event_key    TEXT UNIQUE,
                    trade_id     TEXT,
                    mint         TEXT NOT NULL,
                    symbol       TEXT,
                    action       TEXT,
                    reason       TEXT,
                    pnl_pct      DOUBLE PRECISION,
                    pnl_usd      DOUBLE PRECISION,
                    qty_raw      NUMERIC,
                    price_usd    DOUBLE PRECISION,
                    slippage_bps DOUBLE PRECISION,
                    fees_usd     DOUBLE PRECISION,
                    tx_sig       TEXT,
                    candidate_id BIGINT REFERENCES entry_candidates(id) ON DELETE SET NULL,
                    session_id   TEXT,
                    extra        JSONB NOT NULL DEFAULT '{}'::jsonb
                );
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS regime_snapshots (
                    id         BIGSERIAL PRIMARY KEY,
                    ts         TIMESTAMPTZ NOT NULL DEFAULT now(),
                    regime     TEXT NOT NULL,
                    state      JSONB NOT NULL DEFAULT '{}'::jsonb,
                    session_id TEXT,
                    extra      JSONB NOT NULL DEFAULT '{}'::jsonb
                );
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS ai_events (
                    id             BIGSERIAL PRIMARY KEY,
                    ts             TIMESTAMPTZ NOT NULL DEFAULT now(),
                    event_type     TEXT NOT NULL,
                    policy_version TEXT,
                    experiment_tag TEXT,
                    src            TEXT,
                    regime         TEXT,
                    payload        JSONB NOT NULL DEFAULT '{}'::jsonb
                );
                """
            )

            # add-if-missing columns (safe idempotent)
            cur.execute("ALTER TABLE entry_candidates ADD COLUMN IF NOT EXISTS session_id TEXT;")
            cur.execute("ALTER TABLE entry_candidates ADD COLUMN IF NOT EXISTS wallet_tag TEXT;")
            cur.execute("ALTER TABLE trade_executions ADD COLUMN IF NOT EXISTS tx_sig TEXT;")
            cur.execute("ALTER TABLE trade_executions ADD COLUMN IF NOT EXISTS session_id TEXT;")
            cur.execute("ALTER TABLE exit_events ADD COLUMN IF NOT EXISTS event_key TEXT;")

            # Indexes
            cur.execute("CREATE INDEX IF NOT EXISTS idx_entry_candidates_ts ON entry_candidates (ts DESC);")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_entry_candidates_mint_ts ON entry_candidates (mint, ts DESC);")
            cur.execute(
                "CREATE INDEX IF NOT EXISTS idx_entry_candidates_src_regime_ts ON entry_candidates (src, regime, ts DESC);"
            )
            cur.execute(
                "CREATE INDEX IF NOT EXISTS idx_entry_candidates_wallet_tag_ts ON entry_candidates (wallet_tag, ts DESC);"
            )
            cur.execute(
                "CREATE INDEX IF NOT EXISTS idx_entry_candidates_session_ts ON entry_candidates (session_id, ts DESC);"
            )
            cur.execute("CREATE INDEX IF NOT EXISTS idx_entry_candidates_feature_version ON entry_candidates (feature_version);")

            cur.execute("CREATE INDEX IF NOT EXISTS idx_entry_outcomes_candidate_id ON entry_outcomes (candidate_id);")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_entry_outcomes_label_ts ON entry_outcomes (label, ts DESC);")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_entry_outcomes_ts ON entry_outcomes (ts DESC);")

            cur.execute("CREATE INDEX IF NOT EXISTS idx_trade_executions_ts ON trade_executions (ts DESC);")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_trade_executions_trade_id ON trade_executions (trade_id);")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_trade_executions_candidate_id ON trade_executions (candidate_id);")
            cur.execute(
                """
                CREATE UNIQUE INDEX IF NOT EXISTS uq_trade_executions_trade_side_mint
                ON trade_executions (trade_id, side, mint);
                """
            )

            cur.execute("CREATE INDEX IF NOT EXISTS idx_exit_events_ts ON exit_events (ts DESC);")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_exit_events_mint_ts ON exit_events (mint, ts DESC);")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_exit_events_trade_id ON exit_events (trade_id);")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_exit_events_candidate_id ON exit_events (candidate_id);")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_exit_events_reason_ts ON exit_events (reason, ts DESC);")
            cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS uq_exit_events_event_key ON exit_events (event_key);")

            cur.execute("CREATE INDEX IF NOT EXISTS idx_regime_snapshots_ts ON regime_snapshots (ts DESC);")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_regime_snapshots_regime_ts ON regime_snapshots (regime, ts DESC);")

            cur.execute("CREATE INDEX IF NOT EXISTS idx_ai_events_ts ON ai_events (ts DESC);")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_ai_events_type_ts ON ai_events (event_type, ts DESC);")

            # JSON decision meta indexes
            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_entry_candidates_policy_version
                ON entry_candidates ((raw->'_decision_meta'->>'ai_policy_version'));
                """
            )
            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_entry_candidates_experiment_tag
                ON entry_candidates ((raw->'_decision_meta'->>'ai_experiment_tag'));
                """
            )
            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_entry_candidates_decision
                ON entry_candidates ((raw->'_decision_meta'->>'decision'));
                """
            )
            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_entry_candidates_decision_score
                ON entry_candidates (((raw->'_decision_meta'->>'decision_score')::double precision));
                """
            )
            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_entry_outcomes_max_dd_pct
                ON entry_outcomes (((extra->>'max_dd_pct')::double precision));
                """
            )

    log.info("ai_db.ensure_ai_tables: schema ensured")


def bootstrap_ai_db() -> bool:
    """
    One-call bootstrap used by startup:
      - returns False if disabled or not reachable
      - ensures schema if reachable (unless migrations disabled or read-only)
    """
    if not AI_DB_ENABLED:
        return False
    try:
        if AI_DB_MIGRATIONS_ENABLED and not AI_DB_READ_ONLY:
            ensure_ai_tables()
            return True
        return ping()
    except Exception:
        if getattr(log, "every")("ai_db_bootstrap_fail", 10):
            log.exception("ai_db.bootstrap_ai_db failed")
        return False


# =============================================================================
# Phase 2: entry_candidates (training inputs)
# =============================================================================

@_softfail_write(0)
def insert_entry_candidate(
    *,
    src: str,
    mint: str,
    symbol: Optional[str],
    regime: Optional[str],
    wallet_tag: Optional[str],
    session_id: Optional[str] = None,
    feature_version: int,
    features: Mapping[str, Any],
    raw: Mapping[str, Any],
) -> int:
    with conn_cursor() as (_conn, cur):
        cur.execute(
            """
            INSERT INTO entry_candidates
                (src, mint, symbol, regime, wallet_tag, session_id, feature_version, features, raw)
            VALUES
                (%s,  %s,   %s,     %s,     %s,        %s,         %s,             %s::jsonb, %s::jsonb)
            RETURNING id
            """,
            (
                str(src),
                str(mint),
                symbol,
                regime,
                wallet_tag,
                session_id,
                int(feature_version),
                _json_dumps(dict(features or {})),
                _json_dumps(dict(raw or {})),
            ),
        )
        row = cur.fetchone()
        return int(row[0]) if row else 0


@_softfail_write(0)
def insert_entry_candidates_bulk(rows: Sequence[Mapping[str, Any]]) -> int:
    if not rows:
        return 0

    if _execute_values is None:
        n = 0
        for r in rows:
            cid = insert_entry_candidate(
                src=str(r.get("src") or ""),
                mint=str(r.get("mint") or ""),
                symbol=cast(Optional[str], r.get("symbol")),
                regime=cast(Optional[str], r.get("regime")),
                wallet_tag=cast(Optional[str], r.get("wallet_tag")),
                session_id=cast(Optional[str], r.get("session_id")),
                feature_version=int(r.get("feature_version") or 0),
                features=cast(Mapping[str, Any], r.get("features") or {}),
                raw=cast(Mapping[str, Any], r.get("raw") or {}),
            )
            n += 1 if cid else 0
        return n

    values = []
    for r in rows:
        values.append(
            (
                str(r.get("src") or ""),
                str(r.get("mint") or ""),
                r.get("symbol"),
                r.get("regime"),
                r.get("wallet_tag"),
                r.get("session_id"),
                int(r.get("feature_version") or 0),
                _json_dumps(dict(cast(Mapping[str, Any], r.get("features") or {}))),
                _json_dumps(dict(cast(Mapping[str, Any], r.get("raw") or {}))),
            )
        )

    with conn_cursor() as (_conn, cur):
        _execute_values(
            cur,
            """
            INSERT INTO entry_candidates
                (src, mint, symbol, regime, wallet_tag, session_id, feature_version, features, raw)
            VALUES %s
            """,
            values,
            template="(%s,%s,%s,%s,%s,%s,%s,%s::jsonb,%s::jsonb)",
            page_size=500,
        )
        return len(values)


@_softfail_read([])
def fetch_latest_candidates(
    *,
    limit: int = 50,
    src: Optional[str] = None,
    regime: Optional[str] = None,
    wallet_tag: Optional[str] = None,
    session_id: Optional[str] = None,
    min_feature_version: Optional[int] = None,
) -> List[Row]:
    limit = max(1, min(int(limit), 500))
    min_feature_version = _effective_min_feature_version(min_feature_version)

    clauses: List[str] = []
    params: List[Any] = []

    if src:
        clauses.append("src = %s")
        params.append(src)
    if regime:
        clauses.append("regime = %s")
        params.append(regime)
    if wallet_tag:
        clauses.append("wallet_tag = %s")
        params.append(wallet_tag)
    if session_id:
        clauses.append("session_id = %s")
        params.append(session_id)
    if min_feature_version is not None:
        clauses.append("feature_version >= %s")
        params.append(int(min_feature_version))

    where_sql = ("WHERE " + " AND ".join(clauses)) if clauses else ""
    sql = f"""
        SELECT id, ts, src, mint, symbol, regime, wallet_tag, session_id, feature_version, features, raw
        FROM entry_candidates
        {where_sql}
        ORDER BY ts DESC, id DESC
        LIMIT %s
    """
    params.append(limit)

    with dict_cursor_only() as cur:
        cur.execute(sql, params)
        rows = cur.fetchall() or []

    out: List[Row] = []
    for r in rows:
        rr = dict(r)
        rr["id"] = int(rr.get("id") or 0)
        rr["feature_version"] = int(rr.get("feature_version") or 0)
        rr["features"] = _ensure_obj(rr.get("features"))
        rr["raw"] = _ensure_obj(rr.get("raw"))
        out.append(rr)
    return out


@_softfail_read(None)
def fetch_candidate_by_id(candidate_id: int) -> Optional[Row]:
    with dict_cursor_only() as cur:
        cur.execute(
            """
            SELECT id, ts, src, mint, symbol, regime, wallet_tag, session_id, feature_version, features, raw
            FROM entry_candidates
            WHERE id = %s
            """,
            (int(candidate_id),),
        )
        row = cur.fetchone()
    if not row:
        return None
    r = dict(row)
    r["id"] = int(r.get("id") or 0)
    r["feature_version"] = int(r.get("feature_version") or 0)
    r["features"] = _ensure_obj(r.get("features"))
    r["raw"] = _ensure_obj(r.get("raw"))
    return r


@_softfail_write(0)
def update_entry_decision(
    *,
    candidate_id: int,
    decision: Optional[str] = None,
    decision_score: Optional[float] = None,
    decision_reason: Optional[str] = None,
    **extra_fields: Any,
) -> int:
    patch_meta: Dict[str, Any] = {
        "decision": decision,
        "decision_score": decision_score,
        "decision_reason": decision_reason,
        "ts": _utcnow().isoformat(),
    }
    if extra_fields:
        patch_meta.update(extra_fields)

    patch_json = _json_dumps({"_decision_meta": patch_meta})

    with conn_cursor() as (_conn, cur):
        cur.execute(
            """
            UPDATE entry_candidates
            SET raw = COALESCE(raw, '{}'::jsonb) || %s::jsonb
            WHERE id = %s
            RETURNING id
            """,
            (patch_json, int(candidate_id)),
        )
        row = cur.fetchone()
        return int(row[0]) if row else 0


# =============================================================================
# Phase 3: entry_outcomes (training labels)
# =============================================================================

@_softfail_write(0)
def insert_entry_outcome(
    *,
    candidate_id: int,
    label: str,
    pnl_pct: Optional[float] = None,
    hold_sec: Optional[int] = None,
    notes: Optional[str] = None,
    extra: Optional[Mapping[str, Any]] = None,
) -> int:
    with conn_cursor() as (_conn, cur):
        cur.execute(
            """
            INSERT INTO entry_outcomes
                (candidate_id, label, pnl_pct, hold_sec, notes, extra)
            VALUES
                (%s,           %s,    %s,       %s,       %s,    %s::jsonb)
            RETURNING id
            """,
            (int(candidate_id), str(label), pnl_pct, hold_sec, notes, _json_dumps(dict(extra or {}))),
        )
        row = cur.fetchone()
        return int(row[0]) if row else 0


@_softfail_write(0)
def mark_outcome_for_candidate(
    candidate_id: int,
    *,
    label: str,
    pnl_pct: Optional[float] = None,
    hold_sec: Optional[int] = None,
    notes: Optional[str] = None,
    extra: Optional[Mapping[str, Any]] = None,
) -> int:
    with conn_cursor() as (_conn, cur):
        cur.execute(
            """
            INSERT INTO entry_outcomes
                (candidate_id, label, pnl_pct, hold_sec, notes, extra)
            VALUES
                (%s,           %s,    %s,       %s,       %s,    %s::jsonb)
            ON CONFLICT (candidate_id)
            DO UPDATE SET
                label    = EXCLUDED.label,
                pnl_pct  = EXCLUDED.pnl_pct,
                hold_sec = EXCLUDED.hold_sec,
                notes    = EXCLUDED.notes,
                extra    = EXCLUDED.extra,
                ts       = now()
            RETURNING id
            """,
            (int(candidate_id), str(label), pnl_pct, hold_sec, notes, _json_dumps(dict(extra or {}))),
        )
        row = cur.fetchone()
        return int(row[0]) if row else 0


@_softfail_read([])
def fetch_recent_outcomes(*, limit: int = 100, since_candidate_id: Optional[int] = None) -> List[Row]:
    limit = max(1, min(int(limit), 1000))
    clauses: List[str] = []
    params: List[Any] = []
    if since_candidate_id is not None:
        clauses.append("candidate_id > %s")
        params.append(int(since_candidate_id))
    where_sql = ("WHERE " + " AND ".join(clauses)) if clauses else ""
    sql = f"""
        SELECT id, candidate_id, label, pnl_pct, hold_sec, notes, extra, ts
        FROM entry_outcomes
        {where_sql}
        ORDER BY ts DESC, id DESC
        LIMIT %s
    """
    params.append(limit)

    with dict_cursor_only() as cur:
        cur.execute(sql, params)
        rows = cur.fetchall() or []

    out: List[Row] = []
    for r in rows:
        rr = dict(r)
        rr["id"] = int(rr.get("id") or 0)
        rr["candidate_id"] = int(rr.get("candidate_id") or 0)
        rr["extra"] = _ensure_obj(rr.get("extra"))
        out.append(rr)
    return out


@_softfail_read([])
def fetch_unlabeled_candidates(
    *,
    limit: int = 500,
    min_feature_version: Optional[int] = None,
    src: Optional[str] = None,
    regime: Optional[str] = None,
    min_age_seconds: Optional[int] = None,
    wallet_tag: Optional[str] = None,
    session_id: Optional[str] = None,
) -> List[Row]:
    limit = max(1, min(int(limit), 5000))
    min_feature_version = _effective_min_feature_version(min_feature_version)

    clauses: List[str] = []
    params: List[Any] = []

    if src:
        clauses.append("c.src = %s")
        params.append(src)
    if regime:
        clauses.append("c.regime = %s")
        params.append(regime)
    if wallet_tag:
        clauses.append("c.wallet_tag = %s")
        params.append(wallet_tag)
    if session_id:
        clauses.append("c.session_id = %s")
        params.append(session_id)
    if min_feature_version is not None:
        clauses.append("c.feature_version >= %s")
        params.append(int(min_feature_version))
    if min_age_seconds is not None and int(min_age_seconds) > 0:
        clauses.append("c.ts <= now() - (%s::interval)")
        params.append(f"{int(min_age_seconds)} seconds")

    where_sql = (("WHERE " + " AND ".join(clauses) + " AND o.candidate_id IS NULL") if clauses else "WHERE o.candidate_id IS NULL")

    sql = f"""
        SELECT
            c.id, c.ts, c.src, c.mint, c.symbol, c.regime, c.wallet_tag, c.session_id,
            c.feature_version, c.features, c.raw
        FROM entry_candidates c
        LEFT JOIN entry_outcomes o
          ON o.candidate_id = c.id
        {where_sql}
        ORDER BY c.ts DESC, c.id DESC
        LIMIT %s
    """
    params.append(limit)

    with dict_cursor_only() as cur:
        cur.execute(sql, params)
        rows = cur.fetchall() or []

    out: List[Row] = []
    for r in rows:
        rr = dict(r)
        rr["id"] = int(rr.get("id") or 0)
        rr["feature_version"] = int(rr.get("feature_version") or 0)
        rr["features"] = _ensure_obj(rr.get("features"))
        rr["raw"] = _ensure_obj(rr.get("raw"))
        out.append(rr)
    return out


# =============================================================================
# Phase 3b: Executions, exit events, regime snapshots
# =============================================================================

@_softfail_write(0)
def insert_trade_execution(
    *,
    trade_id: str,
    mint: str,
    side: str,  # "BUY" / "SELL"
    qty_raw: int,
    price_usd: float,
    slippage_bps: Optional[float] = None,
    fees_usd: Optional[float] = None,
    tx_sig: Optional[str] = None,
    candidate_id: Optional[int] = None,
    session_id: Optional[str] = None,
    ts: Optional[Any] = None,
    extra: Optional[Mapping[str, Any]] = None,
) -> int:
    with conn_cursor() as (_conn, cur):
        cur.execute(
            """
            INSERT INTO trade_executions
                (ts, trade_id, mint, side, qty_raw, price_usd,
                 slippage_bps, fees_usd, tx_sig, candidate_id, session_id, extra)
            VALUES
                (COALESCE(%s::timestamptz, now()), %s, %s, %s, %s, %s,
                 %s, %s, %s, %s, %s, %s::jsonb)
            ON CONFLICT (trade_id, side, mint) DO NOTHING
            RETURNING id
            """,
            (
                ts,
                str(trade_id),
                str(mint),
                str(side).upper(),
                int(qty_raw),
                float(price_usd),
                slippage_bps,
                fees_usd,
                tx_sig,
                int(candidate_id) if candidate_id is not None else None,
                session_id,
                _json_dumps(dict(extra or {})),
            ),
        )
        row = cur.fetchone()
        return int(row[0]) if row else 0


def _mk_exit_event_key(
    *,
    trade_id: Optional[str],
    mint: str,
    action: Optional[str],
    reason: Optional[str],
    tx_sig: Optional[str],
    candidate_id: Optional[int],
    session_id: Optional[str],
    ts: Optional[Any],
) -> str:
    base = "|".join(
        [
            str(trade_id or ""),
            str(mint or ""),
            str((action or "").upper()),
            str(reason or ""),
            str(tx_sig or ""),
            str(candidate_id or ""),
            str(session_id or ""),
            str(ts or ""),
        ]
    )
    return hashlib.sha256(base.encode("utf-8", "ignore")).hexdigest()


@_softfail_write(0)
def insert_exit_event(
    *,
    mint: str,
    symbol: Optional[str] = None,
    trade_id: Optional[str] = None,
    action: Optional[str] = None,
    reason: Optional[str] = None,
    pnl_pct: Optional[float] = None,
    pnl_usd: Optional[float] = None,
    qty_raw: Optional[int] = None,
    price_usd: Optional[float] = None,
    slippage_bps: Optional[float] = None,
    fees_usd: Optional[float] = None,
    tx_sig: Optional[str] = None,
    candidate_id: Optional[int] = None,
    session_id: Optional[str] = None,
    ts: Optional[Any] = None,
    extra: Optional[Mapping[str, Any]] = None,
    event_key: Optional[str] = None,
) -> int:
    ek = event_key or _mk_exit_event_key(
        trade_id=trade_id,
        mint=mint,
        action=action,
        reason=reason,
        tx_sig=tx_sig,
        candidate_id=candidate_id,
        session_id=session_id,
        ts=ts,
    )

    with conn_cursor() as (_conn, cur):
        cur.execute(
            """
            INSERT INTO exit_events
                (ts, event_key, trade_id, mint, symbol, action, reason,
                 pnl_pct, pnl_usd, qty_raw, price_usd, slippage_bps, fees_usd,
                 tx_sig, candidate_id, session_id, extra)
            VALUES
                (COALESCE(%s::timestamptz, now()), %s, %s, %s, %s, %s, %s,
                 %s, %s, %s, %s, %s, %s,
                 %s, %s, %s, %s::jsonb)
            ON CONFLICT (event_key) DO NOTHING
            RETURNING id
            """,
            (
                ts,
                str(ek),
                trade_id,
                str(mint),
                symbol,
                (action or None),
                (reason or None),
                pnl_pct,
                pnl_usd,
                int(qty_raw) if qty_raw is not None else None,
                float(price_usd) if price_usd is not None else None,
                slippage_bps,
                fees_usd,
                tx_sig,
                int(candidate_id) if candidate_id is not None else None,
                session_id,
                _json_dumps(dict(extra or {})),
            ),
        )
        row = cur.fetchone()
        return int(row[0]) if row else 0


@_softfail_read([])
def fetch_recent_exit_events(
    *,
    limit: int = 200,
    mint: Optional[str] = None,
    trade_id: Optional[str] = None,
    candidate_id: Optional[int] = None,
) -> List[Row]:
    limit = max(1, min(int(limit), 2000))
    clauses: List[str] = []
    params: List[Any] = []
    if mint:
        clauses.append("mint = %s")
        params.append(mint)
    if trade_id:
        clauses.append("trade_id = %s")
        params.append(trade_id)
    if candidate_id is not None:
        clauses.append("candidate_id = %s")
        params.append(int(candidate_id))
    where_sql = ("WHERE " + " AND ".join(clauses)) if clauses else ""
    sql = f"""
        SELECT id, ts, event_key, trade_id, mint, symbol, action, reason,
               pnl_pct, pnl_usd, qty_raw, price_usd, slippage_bps, fees_usd, tx_sig,
               candidate_id, session_id, extra
        FROM exit_events
        {where_sql}
        ORDER BY ts DESC, id DESC
        LIMIT %s
    """
    params.append(limit)

    with dict_cursor_only() as cur:
        cur.execute(sql, params)
        rows = cur.fetchall() or []

    out: List[Row] = []
    for r in rows:
        rr = dict(r)
        rr["id"] = int(rr.get("id") or 0)
        rr["extra"] = _ensure_obj(rr.get("extra"))
        out.append(rr)
    return out


@_softfail_read([])
def fetch_recent_trade_executions(
    *,
    limit: int = 200,
    trade_id: Optional[str] = None,
    candidate_id: Optional[int] = None,
) -> List[Row]:
    limit = max(1, min(int(limit), 2000))

    clauses: List[str] = []
    params: List[Any] = []

    if trade_id:
        clauses.append("trade_id = %s")
        params.append(trade_id)
    if candidate_id is not None:
        clauses.append("candidate_id = %s")
        params.append(int(candidate_id))

    where_sql = ("WHERE " + " AND ".join(clauses)) if clauses else ""
    sql = f"""
        SELECT id, ts, trade_id, mint, side, qty_raw, price_usd,
               slippage_bps, fees_usd, tx_sig, candidate_id, session_id, extra
        FROM trade_executions
        {where_sql}
        ORDER BY ts DESC, id DESC
        LIMIT %s
    """
    params.append(limit)

    with dict_cursor_only() as cur:
        cur.execute(sql, params)
        rows = cur.fetchall() or []

    out: List[Row] = []
    for r in rows:
        rr = dict(r)
        rr["id"] = int(rr.get("id") or 0)
        rr["extra"] = _ensure_obj(rr.get("extra"))
        out.append(rr)
    return out


@_softfail_write(0)
def insert_regime_snapshot(
    *,
    regime: str,
    state: Mapping[str, Any],
    ts: Optional[Any] = None,
    session_id: Optional[str] = None,
    extra: Optional[Mapping[str, Any]] = None,
) -> int:
    with conn_cursor() as (_conn, cur):
        cur.execute(
            """
            INSERT INTO regime_snapshots
                (ts, regime, state, session_id, extra)
            VALUES
                (COALESCE(%s::timestamptz, now()), %s, %s::jsonb, %s, %s::jsonb)
            RETURNING id
            """,
            (ts, str(regime), _json_dumps(dict(state or {})), session_id, _json_dumps(dict(extra or {}))),
        )
        row = cur.fetchone()
        return int(row[0]) if row else 0


@_softfail_read([])
def fetch_recent_regime_snapshots(*, limit: int = 200, regime: Optional[str] = None) -> List[Row]:
    limit = max(1, min(int(limit), 2000))
    clauses: List[str] = []
    params: List[Any] = []
    if regime:
        clauses.append("regime = %s")
        params.append(regime)
    where_sql = ("WHERE " + " AND ".join(clauses)) if clauses else ""
    sql = f"""
        SELECT id, ts, regime, state, session_id, extra
        FROM regime_snapshots
        {where_sql}
        ORDER BY ts DESC, id DESC
        LIMIT %s
    """
    params.append(limit)

    with dict_cursor_only() as cur:
        cur.execute(sql, params)
        rows = cur.fetchall() or []

    out: List[Row] = []
    for r in rows:
        rr = dict(r)
        rr["id"] = int(rr.get("id") or 0)
        rr["state"] = _ensure_obj(rr.get("state"))
        rr["extra"] = _ensure_obj(rr.get("extra"))
        out.append(rr)
    return out


# =============================================================================
# Phase 3c: Composite helpers (single-call logging)
# =============================================================================

@_softfail_write({"execution_id": 0, "outcome_id": 0})
def record_trade_and_outcome(
    *,
    trade_id: str,
    mint: str,
    side: str,
    qty_raw: int,
    price_usd: float,
    candidate_id: Optional[int] = None,
    label: Optional[str] = None,
    pnl_pct: Optional[float] = None,
    hold_sec: Optional[int] = None,
    notes: Optional[str] = None,
    slippage_bps: Optional[float] = None,
    fees_usd: Optional[float] = None,
    tx_sig: Optional[str] = None,
    session_id: Optional[str] = None,
    trade_extra: Optional[Mapping[str, Any]] = None,
    outcome_extra: Optional[Mapping[str, Any]] = None,
) -> Dict[str, int]:
    exec_id = insert_trade_execution(
        trade_id=trade_id,
        mint=mint,
        side=side,
        qty_raw=qty_raw,
        price_usd=price_usd,
        slippage_bps=slippage_bps,
        fees_usd=fees_usd,
        tx_sig=tx_sig,
        candidate_id=candidate_id,
        session_id=session_id,
        extra=trade_extra or {},
    )

    out_id = 0
    if candidate_id is not None and label is not None:
        out_id = mark_outcome_for_candidate(
            candidate_id,
            label=label,
            pnl_pct=pnl_pct,
            hold_sec=hold_sec,
            notes=notes,
            extra=(outcome_extra or {}),
        )

    return {"execution_id": int(exec_id or 0), "outcome_id": int(out_id or 0)}


@_softfail_write({"exit_event_id": 0, "outcome_id": 0})
def record_exit_event_and_outcome(
    *,
    mint: str,
    symbol: Optional[str] = None,
    trade_id: Optional[str] = None,
    action: Optional[str] = None,
    reason: Optional[str] = None,
    pnl_pct: Optional[float] = None,
    pnl_usd: Optional[float] = None,
    qty_raw: Optional[int] = None,
    price_usd: Optional[float] = None,
    slippage_bps: Optional[float] = None,
    fees_usd: Optional[float] = None,
    tx_sig: Optional[str] = None,
    candidate_id: Optional[int] = None,
    session_id: Optional[str] = None,
    ts: Optional[Any] = None,
    exit_extra: Optional[Mapping[str, Any]] = None,
    label: Optional[str] = None,
    hold_sec: Optional[int] = None,
    notes: Optional[str] = None,
    outcome_extra: Optional[Mapping[str, Any]] = None,
    event_key: Optional[str] = None,
) -> Dict[str, int]:
    exit_id = insert_exit_event(
        mint=mint,
        symbol=symbol,
        trade_id=trade_id,
        action=action,
        reason=reason,
        pnl_pct=pnl_pct,
        pnl_usd=pnl_usd,
        qty_raw=qty_raw,
        price_usd=price_usd,
        slippage_bps=slippage_bps,
        fees_usd=fees_usd,
        tx_sig=tx_sig,
        candidate_id=candidate_id,
        session_id=session_id,
        ts=ts,
        extra=exit_extra or {},
        event_key=event_key,
    )

    out_id = 0
    if candidate_id is not None and label is not None:
        out_id = mark_outcome_for_candidate(
            candidate_id,
            label=label,
            pnl_pct=pnl_pct,
            hold_sec=hold_sec,
            notes=notes,
            extra=(outcome_extra or {}),
        )

    return {"exit_event_id": int(exit_id or 0), "outcome_id": int(out_id or 0)}


# =============================================================================
# Phase 4: Training + Backtest helpers
# =============================================================================

@_softfail_read([])
def fetch_training_rows(
    *,
    limit: int = 2000,
    require_outcome: bool = True,
    min_feature_version: Optional[int] = None,
    src: Optional[str] = None,
    regime: Optional[str] = None,
    policy_version: Optional[str] = None,
    experiment_tag: Optional[str] = None,
    label_in: Optional[Sequence[str]] = None,
    wallet_tag: Optional[str] = None,
    session_id: Optional[str] = None,
) -> List[Row]:
    limit = max(1, min(int(limit), 10_000))
    min_feature_version = _effective_min_feature_version(min_feature_version)

    join_type = "JOIN" if require_outcome else "LEFT JOIN"
    clauses: List[str] = []
    params: List[Any] = []

    if src:
        clauses.append("c.src = %s")
        params.append(src)
    if regime:
        clauses.append("c.regime = %s")
        params.append(regime)
    if wallet_tag:
        clauses.append("c.wallet_tag = %s")
        params.append(wallet_tag)
    if session_id:
        clauses.append("c.session_id = %s")
        params.append(session_id)
    if min_feature_version is not None:
        clauses.append("c.feature_version >= %s")
        params.append(int(min_feature_version))
    if policy_version:
        clauses.append("c.raw->'_decision_meta'->>'ai_policy_version' = %s")
        params.append(policy_version)
    if experiment_tag:
        clauses.append("c.raw->'_decision_meta'->>'ai_experiment_tag' = %s")
        params.append(experiment_tag)
    if label_in:
        clauses.append("o.label = ANY(%s)")
        params.append(list(label_in))

    where_sql = ("WHERE " + " AND ".join(clauses)) if clauses else ""
    sql = f"""
        SELECT
            c.id AS id,
            c.ts AS ts,
            c.src AS src,
            c.mint AS mint,
            c.symbol AS symbol,
            c.regime AS regime,
            c.wallet_tag AS wallet_tag,
            c.session_id AS session_id,
            c.feature_version AS feature_version,
            c.features AS features,
            c.raw AS raw,
            o.label AS label,
            o.pnl_pct AS pnl_pct,
            o.hold_sec AS hold_sec,
            (o.extra->>'max_dd_pct')::double precision AS max_dd_pct
        FROM entry_candidates c
        {join_type} entry_outcomes o
          ON o.candidate_id = c.id
        {where_sql}
        ORDER BY c.ts DESC, c.id DESC
        LIMIT %s
    """
    params.append(limit)

    with dict_cursor_only() as cur:
        cur.execute(sql, params)
        rows = cur.fetchall() or []

    out: List[Row] = []
    for r in rows:
        rr = dict(r)
        rr["id"] = int(rr.get("id") or 0)
        rr["feature_version"] = int(rr.get("feature_version") or 0)
        rr["features"] = _ensure_obj(rr.get("features"))
        rr["raw"] = _ensure_obj(rr.get("raw"))
        rr["has_outcome"] = rr.get("label") is not None
        out.append(rr)
    return out


def _parse_iso_dt(val: Any) -> Optional[datetime]:
    if val is None:
        return None
    if isinstance(val, datetime):
        return val
    s = str(val)
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception:
        return None


def iter_backtest_rows(config: Mapping[str, Any]) -> Iterator[Mapping[str, Any]]:
    if not AI_DB_ENABLED or not _PSYCOPG2_OK:
        return iter(())

    limit = int(config.get("BT_MAX_ROWS") or config.get("limit") or 50_000)
    limit = max(1, min(limit, 200_000))

    days_back = config.get("days_back")
    start_ts_raw = config.get("start_ts")
    end_ts_raw = config.get("end_ts")

    min_feature_version = config.get("BT_MIN_FEATURE_VERSION", config.get("min_feature_version"))
    try:
        min_feature_version = int(min_feature_version) if min_feature_version is not None else None
    except Exception:
        min_feature_version = None
    min_feature_version = _effective_min_feature_version(min_feature_version)

    src = config.get("src")
    regime = config.get("regime")
    wallet_tag = config.get("wallet_tag")
    session_id = config.get("session_id")
    policy_version = config.get("policy_version")
    experiment_tag = config.get("experiment_tag")
    label_in = config.get("label_in")
    label_not_in = config.get("label_not_in")
    min_pnl_pct = config.get("min_pnl_pct")
    max_pnl_pct = config.get("max_pnl_pct")

    require_outcome = True
    if "BT_REQUIRE_OUTCOME" in config:
        require_outcome = bool(config.get("BT_REQUIRE_OUTCOME"))
    elif "require_outcome" in config:
        require_outcome = bool(config.get("require_outcome"))

    join_type = "JOIN" if require_outcome else "LEFT JOIN"

    clauses: List[str] = []
    params: List[Any] = []

    if days_back is not None:
        try:
            db_int = max(1, min(int(days_back), 365))
            clauses.append("c.ts >= now() - (%s::interval)")
            params.append(f"{db_int} days")
        except Exception:
            pass

    if start_ts_raw:
        start_dt = _parse_iso_dt(start_ts_raw)
        if start_dt is not None:
            clauses.append("c.ts >= %s")
            params.append(start_dt)
    if end_ts_raw:
        end_dt = _parse_iso_dt(end_ts_raw)
        if end_dt is not None:
            clauses.append("c.ts <= %s")
            params.append(end_dt)

    if src:
        clauses.append("c.src = %s")
        params.append(src)
    if regime:
        clauses.append("c.regime = %s")
        params.append(regime)
    if wallet_tag:
        clauses.append("c.wallet_tag = %s")
        params.append(wallet_tag)
    if session_id:
        clauses.append("c.session_id = %s")
        params.append(session_id)
    if min_feature_version is not None:
        clauses.append("c.feature_version >= %s")
        params.append(int(min_feature_version))
    if policy_version:
        clauses.append("c.raw->'_decision_meta'->>'ai_policy_version' = %s")
        params.append(policy_version)
    if experiment_tag:
        clauses.append("c.raw->'_decision_meta'->>'ai_experiment_tag' = %s")
        params.append(experiment_tag)

    if label_in:
        clauses.append("o.label = ANY(%s)")
        params.append(list(label_in))
    if label_not_in:
        clauses.append("NOT (o.label = ANY(%s))")
        params.append(list(label_not_in))

    if min_pnl_pct is not None:
        try:
            clauses.append("o.pnl_pct >= %s")
            params.append(float(min_pnl_pct))
        except Exception:
            pass
    if max_pnl_pct is not None:
        try:
            clauses.append("o.pnl_pct <= %s")
            params.append(float(max_pnl_pct))
        except Exception:
            pass

    where_sql = ("WHERE " + " AND ".join(clauses)) if clauses else ""

    sql = f"""
        SELECT
            c.id,
            c.ts,
            c.src,
            c.mint,
            c.symbol,
            c.regime,
            c.wallet_tag,
            c.session_id,
            c.feature_version,
            c.features,
            c.raw,
            o.label,
            o.pnl_pct,
            o.hold_sec,
            (o.extra->>'max_dd_pct')::double precision AS max_dd_pct
        FROM entry_candidates c
        {join_type} entry_outcomes o
          ON o.candidate_id = c.id
        {where_sql}
        ORDER BY c.ts ASC, c.id ASC
        LIMIT %s
    """
    params2 = list(params) + [limit]

    def _gen() -> Iterator[Mapping[str, Any]]:
        conn: Optional[_PGConn] = None
        cur: Optional[_PGCursor] = None
        try:
            conn = _connect()
            cur_name = f"ai_db_backtest_cursor_{os.getpid()}"
            cur = conn.cursor(name=cur_name)
            cur.itersize = 2000
            cur.execute(sql, params2)

            for r in cur:
                (
                    cid,
                    ts,
                    src_val,
                    mint_val,
                    symbol_val,
                    regime_val,
                    wallet_tag_val,
                    session_id_val,
                    feat_ver,
                    features_json,
                    raw_json,
                    label,
                    pnl_pct,
                    hold_sec,
                    max_dd_pct,
                ) = r
                yield {
                    "id": int(cid),
                    "created_at": ts,
                    "ts": ts,
                    "src": src_val,
                    "mint": mint_val,
                    "symbol": symbol_val,
                    "regime": regime_val,
                    "wallet_tag": wallet_tag_val,
                    "session_id": session_id_val,
                    "feature_version": int(feat_ver),
                    "features": _ensure_obj(features_json),
                    "raw": _ensure_obj(raw_json),
                    "label": label,
                    "pnl_pct": float(pnl_pct) if pnl_pct is not None else None,
                    "hold_sec": float(hold_sec) if hold_sec is not None else None,
                    "max_dd_pct": float(max_dd_pct) if max_dd_pct is not None else None,
                }
        except Exception:
            log.exception("iter_backtest_rows failed")
            return
        finally:
            try:
                if cur is not None:
                    cur.close()
            except Exception:
                pass
            try:
                if conn is not None:
                    conn.close()
            except Exception:
                pass

    return _gen()


def iter_training_rows(config: Mapping[str, Any]) -> Iterator[Row]:
    rows = fetch_training_rows(
        limit=int(config.get("limit") or 2000),
        require_outcome=bool(config.get("require_outcome", True)),
        min_feature_version=config.get("min_feature_version"),
        src=config.get("src"),
        regime=config.get("regime"),
        wallet_tag=config.get("wallet_tag"),
        session_id=config.get("session_id"),
        policy_version=config.get("policy_version"),
        experiment_tag=config.get("experiment_tag"),
        label_in=config.get("label_in"),
    )
    for r in rows:
        yield r


def iter_training_jsonl(config: Mapping[str, Any]) -> Iterator[str]:
    for row in iter_training_rows(config):
        obj = dict(row)
        ts_val = obj.get("ts")
        if isinstance(ts_val, datetime):
            obj["ts"] = ts_val.isoformat()
        created_at = obj.get("created_at")
        if isinstance(created_at, datetime):
            obj["created_at"] = created_at.isoformat()
        yield json.dumps(obj, separators=(",", ":"), ensure_ascii=False, default=str)


def dump_training_jsonl_to_file(path: str, config: Mapping[str, Any]) -> int:
    n = 0
    with open(path, "w", encoding="utf-8") as f:
        for line in iter_training_jsonl(config):
            f.write(line + "\n")
            n += 1
    log.info("dump_training_jsonl_to_file: wrote %s rows to %s", n, path)
    return n


# =============================================================================
# Phase 5-7: Quant stats & dashboards
# =============================================================================

@_softfail_read(
    {
        "n_trades": 0,
        "n_labeled": 0,
        "n_wins": 0,
        "win_rate": 0.0,
        "avg_pnl_pct": None,
        "median_pnl_pct": None,
        "avg_hold_sec": None,
        "win_labels": [],
    }
)
def fetch_basic_stats(
    *,
    days_back: int = 7,
    src: Optional[str] = None,
    regime: Optional[str] = None,
    min_feature_version: Optional[int] = None,
    wallet_tag: Optional[str] = None,
    session_id: Optional[str] = None,
) -> Dict[str, Any]:
    days_back = max(1, min(int(days_back), 365))
    min_feature_version = _effective_min_feature_version(min_feature_version)

    clauses: List[str] = ["o.ts >= now() - (%s::interval)"]
    params: List[Any] = [f"{days_back} days"]

    if src:
        clauses.append("c.src = %s")
        params.append(src)
    if regime:
        clauses.append("c.regime = %s")
        params.append(regime)
    if wallet_tag:
        clauses.append("c.wallet_tag = %s")
        params.append(wallet_tag)
    if session_id:
        clauses.append("c.session_id = %s")
        params.append(session_id)
    if min_feature_version is not None:
        clauses.append("c.feature_version >= %s")
        params.append(int(min_feature_version))

    where_sql = "WHERE " + " AND ".join(clauses)
    win_labels = list(WIN_LABELS)

    sql = f"""
        SELECT
            count(*) AS n_total,
            count(*) FILTER (WHERE o.label IS NOT NULL) AS n_labeled,
            count(*) FILTER (WHERE lower(o.label) = ANY(%s)) AS n_wins,
            avg(o.pnl_pct) AS avg_pnl_pct,
            percentile_disc(0.5) WITHIN GROUP (ORDER BY o.pnl_pct) AS median_pnl_pct,
            avg(o.hold_sec) AS avg_hold_sec
        FROM entry_outcomes o
        JOIN entry_candidates c ON c.id = o.candidate_id
        {where_sql}
    """

    with cursor_only() as cur:
        cur.execute(sql, [win_labels] + params)
        row = cur.fetchone()

    if not row:
        raise RuntimeError("fetch_basic_stats: no row")

    n_total, n_labeled, n_wins, avg_pnl_pct, median_pnl_pct, avg_hold_sec = row
    n_total_i = int(n_total or 0)
    n_labeled_i = int(n_labeled or 0)
    n_wins_i = int(n_wins or 0)
    win_rate = float(n_wins_i) / float(n_labeled_i) if n_labeled_i else 0.0

    return {
        "n_trades": n_total_i,
        "n_labeled": n_labeled_i,
        "win_labels": win_labels,
        "n_wins": n_wins_i,
        "win_rate": win_rate,
        "avg_pnl_pct": float(avg_pnl_pct) if avg_pnl_pct is not None else None,
        "median_pnl_pct": float(median_pnl_pct) if median_pnl_pct is not None else None,
        "avg_hold_sec": float(avg_hold_sec) if avg_hold_sec is not None else None,
    }


@_softfail_read({})
def fetch_label_distribution(
    *,
    days_back: int = 30,
    src: Optional[str] = None,
    regime: Optional[str] = None,
    min_feature_version: Optional[int] = None,
    wallet_tag: Optional[str] = None,
    session_id: Optional[str] = None,
) -> Dict[str, int]:
    days_back = max(1, min(int(days_back), 365))
    min_feature_version = _effective_min_feature_version(min_feature_version)

    clauses: List[str] = ["o.ts >= now() - (%s::interval)"]
    params: List[Any] = [f"{days_back} days"]

    if src:
        clauses.append("c.src = %s")
        params.append(src)
    if regime:
        clauses.append("c.regime = %s")
        params.append(regime)
    if wallet_tag:
        clauses.append("c.wallet_tag = %s")
        params.append(wallet_tag)
    if session_id:
        clauses.append("c.session_id = %s")
        params.append(session_id)
    if min_feature_version is not None:
        clauses.append("c.feature_version >= %s")
        params.append(int(min_feature_version))

    where_sql = "WHERE " + " AND ".join(clauses)
    sql = f"""
        SELECT o.label, count(*) AS n
        FROM entry_outcomes o
        JOIN entry_candidates c ON c.id = o.candidate_id
        {where_sql}
        GROUP BY o.label
    """

    out: Dict[str, int] = {}
    with cursor_only() as cur:
        cur.execute(sql, params)
        rows = cur.fetchall() or []
    for label, n in rows:
        out[str(label)] = int(n)
    return out


@_softfail_read({"days_back": 7, "n_candidates": 0, "n_labeled": 0, "coverage": 0.0})
def fetch_outcome_coverage(
    *,
    days_back: int = 7,
    min_feature_version: Optional[int] = None,
    src: Optional[str] = None,
    regime: Optional[str] = None,
    wallet_tag: Optional[str] = None,
    session_id: Optional[str] = None,
) -> Dict[str, Any]:
    days_back = max(1, min(int(days_back), 365))
    min_feature_version = _effective_min_feature_version(min_feature_version)

    clauses: List[str] = ["c.ts >= now() - (%s::interval)"]
    params: List[Any] = [f"{days_back} days"]

    if src:
        clauses.append("c.src = %s")
        params.append(src)
    if regime:
        clauses.append("c.regime = %s")
        params.append(regime)
    if wallet_tag:
        clauses.append("c.wallet_tag = %s")
        params.append(wallet_tag)
    if session_id:
        clauses.append("c.session_id = %s")
        params.append(session_id)
    if min_feature_version is not None:
        clauses.append("c.feature_version >= %s")
        params.append(int(min_feature_version))

    where_sql = "WHERE " + " AND ".join(clauses)
    sql = f"""
        SELECT count(*) AS n_candidates, count(o.id) AS n_labeled
        FROM entry_candidates c
        LEFT JOIN entry_outcomes o ON o.candidate_id = c.id
        {where_sql}
    """

    with cursor_only() as cur:
        cur.execute(sql, params)
        row = cur.fetchone()
    if not row:
        raise RuntimeError("fetch_outcome_coverage: no row")

    n_candidates, n_labeled = row
    n_candidates_i = int(n_candidates or 0)
    n_labeled_i = int(n_labeled or 0)
    coverage = float(n_labeled_i) / float(n_candidates_i) if n_candidates_i else 0.0
    return {"days_back": days_back, "n_candidates": n_candidates_i, "n_labeled": n_labeled_i, "coverage": coverage}


@_softfail_read({"days_back": 30, "n_exit_events": 0, "sell_like": 0, "stop_like": 0})
def fetch_exit_event_stats(*, days_back: int = 30) -> Dict[str, Any]:
    days_back = max(1, min(int(days_back), 365))
    sql = """
        SELECT
            count(*) AS n,
            count(*) FILTER (WHERE lower(action) LIKE 'sell%%') AS sell_like,
            count(*) FILTER (WHERE lower(reason) LIKE 'stop%%' OR lower(reason) LIKE 'sl%%') AS stop_like
        FROM exit_events
        WHERE ts >= now() - (%s::interval)
    """
    with cursor_only() as cur:
        cur.execute(sql, (f"{days_back} days",))
        row = cur.fetchone()
    if not row:
        raise RuntimeError("fetch_exit_event_stats: no row")
    n, sell_like, stop_like = row
    return {
        "days_back": days_back,
        "n_exit_events": int(n or 0),
        "sell_like": int(sell_like or 0),
        "stop_like": int(stop_like or 0),
    }


@_softfail_read([])
def fetch_ai_event_summary(
    *,
    days_back: int = 30,
    event_types: Optional[Sequence[str]] = None,
) -> List[Dict[str, Any]]:
    days_back = max(1, min(int(days_back), 365))

    clauses: List[str] = ["ts >= now() - (%s::interval)"]
    params: List[Any] = [f"{days_back} days"]
    if event_types:
        clauses.append("event_type = ANY(%s)")
        params.append(list(event_types))

    where_sql = "WHERE " + " AND ".join(clauses)
    sql = f"""
        SELECT event_type, count(*) AS n_events, min(ts) AS first_ts, max(ts) AS last_ts
        FROM ai_events
        {where_sql}
        GROUP BY event_type
        ORDER BY event_type
    """

    out: List[Dict[str, Any]] = []
    with cursor_only() as cur:
        cur.execute(sql, params)
        rows = cur.fetchall() or []
    for event_type, n_events, first_ts, last_ts in rows:
        out.append(
            {
                "event_type": event_type,
                "n_events": int(n_events or 0),
                "first_ts": first_ts.isoformat() if isinstance(first_ts, datetime) else str(first_ts),
                "last_ts": last_ts.isoformat() if isinstance(last_ts, datetime) else str(last_ts),
            }
        )
    return out


@_softfail_write(0)
def log_ai_event(
    *,
    event_type: str,
    policy_version: Optional[str] = None,
    experiment_tag: Optional[str] = None,
    src: Optional[str] = None,
    regime: Optional[str] = None,
    payload: Optional[Mapping[str, Any]] = None,
) -> int:
    with conn_cursor() as (_conn, cur):
        cur.execute(
            """
            INSERT INTO ai_events
                (event_type, policy_version, experiment_tag, src, regime, payload)
            VALUES
                (%s,         %s,             %s,             %s,  %s,     %s::jsonb)
            RETURNING id
            """,
            (str(event_type), policy_version, experiment_tag, src, regime, _json_dumps(dict(payload or {}))),
        )
        row = cur.fetchone()
        return int(row[0]) if row else 0


def fetch_quant_dashboard(
    *,
    src: Optional[str] = None,
    regime: Optional[str] = None,
    wallet_tag: Optional[str] = None,
    session_id: Optional[str] = None,
    min_feature_version: Optional[int] = None,
    days_back_core: int = 7,
    days_back_coverage: int = 7,
    days_back_label_dist: int = 30,
    days_back_events: int = 30,
    days_back_exits: int = 30,
) -> Dict[str, Any]:
    min_feature_version = _effective_min_feature_version(min_feature_version)
    return {
        "core": fetch_basic_stats(
            days_back=days_back_core,
            src=src,
            regime=regime,
            wallet_tag=wallet_tag,
            session_id=session_id,
            min_feature_version=min_feature_version,
        ),
        "coverage": fetch_outcome_coverage(
            days_back=days_back_coverage,
            src=src,
            regime=regime,
            wallet_tag=wallet_tag,
            session_id=session_id,
            min_feature_version=min_feature_version,
        ),
        "label_distribution": fetch_label_distribution(
            days_back=days_back_label_dist,
            src=src,
            regime=regime,
            wallet_tag=wallet_tag,
            session_id=session_id,
            min_feature_version=min_feature_version,
        ),
        "exit_event_stats": fetch_exit_event_stats(days_back=days_back_exits),
        "ai_events_summary": fetch_ai_event_summary(days_back=days_back_events),
    }


def get_env_scoped_dashboard() -> Dict[str, Any]:
    return fetch_quant_dashboard(
        src=AI_DB_DEFAULT_SRC or None,
        regime=AI_DB_DEFAULT_REGIME or None,
        wallet_tag=AI_DB_DEFAULT_WALLET_TAG or None,
        session_id=None,
        min_feature_version=AI_DB_MIN_FEATURE_VERSION,
    )


# =============================================================================
# Housekeeping
# =============================================================================

@_softfail_write(0)
def prune_old_ai_data(*, max_age_days: int = 365) -> int:
    max_age_days = max(1, int(max_age_days))
    sql = """
        WITH deleted AS (
            DELETE FROM entry_candidates
            WHERE ts < now() - (%s::interval)
            RETURNING 1
        )
        SELECT count(*) FROM deleted
    """
    with cursor_only() as cur:
        cur.execute(sql, (f"{max_age_days} days",))
        row = cur.fetchone()
    return int(row[0] or 0) if row else 0


@_softfail_write(0)
def prune_old_events(*, max_age_days: int = 365) -> int:
    max_age_days = max(1, int(max_age_days))
    total = 0
    with conn_cursor() as (_conn, cur):
        for tbl in ("ai_events", "exit_events", "regime_snapshots", "trade_executions"):
            cur.execute(
                f"DELETE FROM {tbl} WHERE ts < now() - (%s::interval)",
                (f"{max_age_days} days",),
            )
            try:
                total += int(cur.rowcount or 0)
            except Exception:
                pass
    return int(total)


def ai_db_status() -> Dict[str, Any]:
    return {
        "enabled": bool(AI_DB_ENABLED),
        "read_only": bool(AI_DB_READ_ONLY),
        "dsn": describe_dsn_sources(),
        "backend": describe_backend(),
    }


# =============================================================================
# Async wrappers (so asyncio bot won't block)
# =============================================================================

async def abootstrap_ai_db() -> bool:
    return await asyncio.to_thread(bootstrap_ai_db)


async def ainsert_entry_candidate(**kwargs: Any) -> int:
    return await asyncio.to_thread(insert_entry_candidate, **kwargs)


async def amark_outcome_for_candidate(candidate_id: int, **kwargs: Any) -> int:
    return await asyncio.to_thread(mark_outcome_for_candidate, candidate_id, **kwargs)


async def ainsert_trade_execution(**kwargs: Any) -> int:
    return await asyncio.to_thread(insert_trade_execution, **kwargs)


async def arecord_trade_and_outcome(**kwargs: Any) -> Dict[str, int]:
    return await asyncio.to_thread(record_trade_and_outcome, **kwargs)


async def ainsert_exit_event(**kwargs: Any) -> int:
    return await asyncio.to_thread(insert_exit_event, **kwargs)


async def arecord_exit_event_and_outcome(**kwargs: Any) -> Dict[str, int]:
    return await asyncio.to_thread(record_exit_event_and_outcome, **kwargs)


async def alog_ai_event(**kwargs: Any) -> int:
    return await asyncio.to_thread(log_ai_event, **kwargs)


async def afetch_quant_dashboard(**kwargs: Any) -> Dict[str, Any]:
    return await asyncio.to_thread(fetch_quant_dashboard, **kwargs)


async def afetch_training_rows(**kwargs: Any) -> List[Row]:
    return await asyncio.to_thread(fetch_training_rows, **kwargs)


async def aget_env_scoped_dashboard() -> Dict[str, Any]:
    return await asyncio.to_thread(get_env_scoped_dashboard)


async def aai_db_status() -> Dict[str, Any]:
    return await asyncio.to_thread(ai_db_status)


__all__ = [
    # flags
    "AI_DB_ENABLED",
    "AI_DB_READ_ONLY",
    "AI_DB_MIN_FEATURE_VERSION",
    "WIN_LABELS",
    # dsn / ssl helpers
    "DSNInfo",
    "reset_dsn_cache",
    "resolve_dsn_info",
    "describe_dsn_sources",
    "get_normalized_dsn_for_debug",
    "dsn_wants_ssl",
    "ssl_context_for_dsn",
    "asyncpg_connect",
    "get_asyncpg_pool",
    "close_asyncpg_pool",
    # health / boot
    "ping",
    "describe_backend",
    "bootstrap_ai_db",
    "ensure_ai_tables",
    # writes/reads
    "insert_entry_candidate",
    "insert_entry_candidates_bulk",
    "fetch_latest_candidates",
    "fetch_candidate_by_id",
    "update_entry_decision",
    "insert_entry_outcome",
    "mark_outcome_for_candidate",
    "fetch_recent_outcomes",
    "fetch_unlabeled_candidates",
    "insert_trade_execution",
    "fetch_recent_trade_executions",
    "insert_exit_event",
    "fetch_recent_exit_events",
    "insert_regime_snapshot",
    "fetch_recent_regime_snapshots",
    "record_trade_and_outcome",
    "record_exit_event_and_outcome",
    "fetch_training_rows",
    "iter_backtest_rows",
    "iter_training_rows",
    "iter_training_jsonl",
    "dump_training_jsonl_to_file",
    "fetch_basic_stats",
    "fetch_label_distribution",
    "fetch_outcome_coverage",
    "fetch_exit_event_stats",
    "log_ai_event",
    "fetch_ai_event_summary",
    "fetch_quant_dashboard",
    "get_env_scoped_dashboard",
    "prune_old_ai_data",
    "prune_old_events",
    "ai_db_status",
    # async wrappers
    "abootstrap_ai_db",
    "ainsert_entry_candidate",
    "amark_outcome_for_candidate",
    "ainsert_trade_execution",
    "arecord_trade_and_outcome",
    "ainsert_exit_event",
    "arecord_exit_event_and_outcome",
    "alog_ai_event",
    "afetch_quant_dashboard",
    "afetch_training_rows",
    "aget_env_scoped_dashboard",
    "aai_db_status",
]
