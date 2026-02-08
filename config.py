# -*- coding: utf-8 -*-
from __future__ import annotations

"""
config.py

Centralized config for the Solana Sniper + DayTrader bot (V3+++ / quant-grade) — LOCK-IN (production-ready)

GOALS
-----
Single canonical env-schema + parsing + typed config object.

Hard rules:
- parse env → typed values
- validate → (errors, warnings)
- provide a safe redacted summarize() + stable fingerprint()

NO network calls. NO DB calls. NO file reads/writes. No side effects beyond logging warnings.
(STRICT_CONFIG=1 may raise on validation errors.)

Backward-compat:
- all Config fields exported as module-level constants
- legacy aliases remain intact (older modules keep working)

Preferred access pattern (new code):
  from config import C
  C.ENABLE_RAYDIUM_SCANNER, C.ENTRY_WORKERS, ...

Lock-in targets:
- Deterministic values across Windows + Render (Linux): path normalization & stable defaults
- DSN routing matches preflight: internal vs external selection + indirection (e.g. PG_DSN_EXTERNAL=DATABASE_URL)
- Correct env aliasing for Jupiter auth flags (JUP_STRICT_AUTH / JUP_ALLOW_PUBLIC_FALLBACK)
- Safe secret redaction integrates with logging_setup.py (if available), with robust fallback.

IMPORTANT:
- In pytest/SAFE_MODE, missing secrets are downgraded to warnings unless STRICT_CONFIG=1.

CAPABILITY FLAGS (LOCK-IN)
--------------------------
These are the single source of truth for “what is required”:
- buys_possible / live_trading
- scanning_possible
- wallet_required
- redis_required
- rpc_required
- db_required

validate_config() uses these exact flags so preflight/tests/runtime cannot drift.
"""

import dataclasses
import hashlib
import json
import logging
import os
import sys
import time
from collections.abc import ItemsView, Iterator, KeysView, Mapping as AbcMapping, ValuesView
from dataclasses import dataclass, field
from functools import lru_cache
from typing import Any, Iterable, Mapping, Sequence
from urllib.parse import urlparse

_LOG = logging.getLogger(__name__)

# =============================================================================
# Meta
# =============================================================================

CONFIG_VERSION = "v3+++_lockin_2026-01-24c"
CONFIG_LOADED_AT_UNIX = int(time.time())

# =============================================================================
# Optional integration: logging_setup redaction helper (safe fallback)
# =============================================================================


@lru_cache(maxsize=1)
def _get_logging_setup_redactor():
    """
    Lazy import to avoid config <-> logging_setup import cycles.
    Returns callable or None.
    """
    try:
        from logging_setup import redact_value as fn  # type: ignore

        return fn
    except Exception:
        return None


def _warn(msg: str, *args: Any) -> None:
    try:
        _LOG.warning(msg, *args)
    except Exception:
        # Never let logging break config.
        return


# =============================================================================
# Parsing primitives (robust input; strict outputs)
# =============================================================================

_TRUE = {"1", "true", "yes", "y", "on", "t"}
_FALSE = {"0", "false", "no", "n", "off", "f"}

_ALLOWED_ENVS = {"dev", "development", "test", "staging", "prod", "production", "local"}
_ALLOWED_QUEUE_DROP = {"drop_newest", "drop_oldest", "block"}
_ALLOWED_POS_BACKENDS = {"json", "sqlite", "pg"}
_ALLOWED_BACKTEST_SOURCE = {"pg", "csv", "json"}
_ALLOWED_COMMITMENT = {"processed", "confirmed", "finalized"}
_ALLOWED_PG_DSN_MODE = {"auto", "internal", "external"}

# Treat these as "base-like" paths for base URL detection (e.g., https://quote-api.jup.ag/v6)
_BASELIKE_PATHS = {
    "",
    "/v1",
    "/v2",
    "/v3",
    "/v4",
    "/v5",
    "/v6",
    "/api",
    "/swap/v1",  # legacy base-like convenience
}

# Best-effort secret key detection for summarize()/repr()
_SECRET_KEY_HINTS = (
    "KEY",
    "TOKEN",
    "SECRET",
    "PASSWORD",
    "PASS",
    "PRIVATE",
    "DSN",
    "WEBHOOK",
    "BEARER",
    "REDIS_URL",
)


def _is_pytest(environ: Mapping[str, str]) -> bool:
    """
    IMPORTANT LOCK-IN:
    pytest sets PYTEST_CURRENT_TEST only while executing tests, not during collection/import.
    To avoid “missing secrets” becoming hard errors during import in test runs,
    we also detect pytest via sys.modules.
    """
    if (
        environ.get("PYTEST_CURRENT_TEST")
        or environ.get("PYTEST_ADDOPTS")
        or environ.get("PYTEST_DISABLE_PLUGIN_AUTOLOAD")
    ):
        return True
    return "pytest" in sys.modules


def _raw(environ: Mapping[str, str], name: str) -> str | None:
    v = environ.get(name)
    if v is None:
        return None
    s = str(v).strip()
    return s if s != "" else None


def _is_safe_mode(environ: Mapping[str, str]) -> bool:
    """
    SAFE_MODE relaxes secret requirements (warnings instead of errors).
    If SAFE_MODE is not explicitly set, we default to SAFE_MODE under pytest to keep imports/test collection clean.
    """
    v = environ.get("SAFE_MODE")
    if v is None:
        return _is_pytest(environ)
    s = str(v).strip().lower()
    return s in _TRUE


def _pick_first(
    environ: Mapping[str, str], names: Sequence[str], default: str | None = None
) -> str | None:
    for key in names:
        v = _raw(environ, key)
        if v is not None:
            return v
    return default


def _coerce_num_str(value: str) -> str:
    # allow "1_000" and also tolerate commas
    return value.strip().replace("_", "").replace(",", "")


def _get_str(environ: Mapping[str, str], name: str, default: str = "") -> str:
    v = _raw(environ, name)
    return v if v is not None else default


def _get_bool(environ: Mapping[str, str], name: str, default: bool = False) -> bool:
    v = _raw(environ, name)
    if v is None:
        return bool(default)
    s = v.strip().lower()
    if s in _TRUE:
        return True
    if s in _FALSE:
        return False
    return bool(default)


def _get_int(environ: Mapping[str, str], name: str, default: int) -> int:
    v = _raw(environ, name)
    if v is None:
        return int(default)
    s = _coerce_num_str(v)
    try:
        return int(s)
    except Exception:
        try:
            f = float(s)
            return int(f)
        except Exception:
            return int(default)


def _get_float(environ: Mapping[str, str], name: str, default: float) -> float:
    v = _raw(environ, name)
    if v is None:
        return float(default)
    try:
        return float(_coerce_num_str(v))
    except Exception:
        return float(default)


def _get_frac(environ: Mapping[str, str], name: str, default: float) -> float:
    """
    Fraction parser with optional percent suffix.
      - "0.005"  -> 0.005
      - "0.5%"   -> 0.005
      - "1%"     -> 0.01
    """
    v = _raw(environ, name)
    if v is None:
        return float(default)
    s = v.strip().lower()
    try:
        if s.endswith("%"):
            return float(_coerce_num_str(s[:-1])) / 100.0
        return float(_coerce_num_str(s))
    except Exception:
        return float(default)


def _clamp_f(x: float, lo: float, hi: float) -> float:
    if x < lo:
        return lo
    if x > hi:
        return hi
    return x


def _clamp_i(x: int, lo: int, hi: int) -> int:
    if x < lo:
        return lo
    if x > hi:
        return hi
    return x


def _get_prob(environ: Mapping[str, str], name: str, default: float) -> float:
    """
    Accepts:
      - fraction: 0.62
      - percent: 62 or "62%"
    Returns fraction clamped to [0..1].
    """
    v = _raw(environ, name)
    if v is None:
        return _clamp_f(float(default), 0.0, 1.0)

    s = v.strip().lower()
    try:
        if s.endswith("%"):
            x = float(_coerce_num_str(s[:-1])) / 100.0
        else:
            x = float(_coerce_num_str(s))
            if x > 1.0:
                x = x / 100.0
        return _clamp_f(x, 0.0, 1.0)
    except Exception:
        return _clamp_f(float(default), 0.0, 1.0)


def _get_bps(environ: Mapping[str, str], name: str, default_bps: int) -> int:
    """
    Accepts:
      - basis points: 50
      - percent: "0.5%" -> 50 bps
    """
    v = _raw(environ, name)
    if v is None:
        return int(default_bps)
    s = v.strip().lower()
    try:
        if s.endswith("%"):
            pct = float(_coerce_num_str(s[:-1]))
            return int(round(pct * 100.0))
        return int(float(_coerce_num_str(s)))
    except Exception:
        return int(default_bps)


def _dedupe_preserve(items: Iterable[str]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for it in items:
        s = str(it).strip()
        if not s:
            continue
        if s in seen:
            continue
        seen.add(s)
        out.append(s)
    return out


def _get_list(
    environ: Mapping[str, str], name: str, default: Sequence[str] | None = None, sep: str = ","
) -> list[str]:
    """
    Accepts:
      - CSV: "a,b,c"
      - JSON list: ["a","b","c"]
    Strips entries, removes empties, preserves order, de-dupes.
    """
    v = _raw(environ, name)
    if v is None:
        return _dedupe_preserve(default or [])

    s = v.strip()
    if s.startswith("[") and s.endswith("]"):
        try:
            arr = json.loads(s)
            if isinstance(arr, list):
                return _dedupe_preserve([str(x) for x in arr])
        except Exception:
            pass

    return _dedupe_preserve([p for p in s.split(sep)])


def _get_json(environ: Mapping[str, str], name: str, default: Any) -> Any:
    v = _raw(environ, name)
    if v is None:
        return default
    try:
        return json.loads(v)
    except Exception:
        return default


def _normalize_url(u: str) -> str:
    s = str(u or "").strip()
    if not s:
        return ""
    return s.rstrip("/")


def _join_url(base: str, suffix: str) -> str:
    b = _normalize_url(base)
    if not b:
        return ""
    suf = (suffix or "").strip()
    if not suf:
        return b
    if not suf.startswith("/"):
        suf = "/" + suf
    return b + suf


def _looks_like_base_url(value: str) -> bool:
    """
    True if value appears to be a "base" URL (no meaningful endpoint path),
    including versioned bases like /v6 and legacy /swap/v1.
    """
    v = _normalize_url(value)
    if not v:
        return False
    try:
        parsed = urlparse(v)
        path = (parsed.path or "").rstrip("/")
        if path == "":
            path = ""
        return path.lower() in _BASELIKE_PATHS
    except Exception:
        return False


def _endpoint_kind_from_path(p: str) -> str | None:
    """
    Determine endpoint "kind" from a URL path or path-suffix.
    We only care about the *terminal* resource name to avoid false positives like
    /swap/v1/quote containing "swap" as a namespace.
    """
    s = (p or "").strip().lower()
    if not s:
        return None
    # strip query-ish remnants just in case
    s = s.split("?", 1)[0].rstrip("/")
    if s.endswith("/quote"):
        return "quote"
    if s.endswith("/swap"):
        return "swap"
    return None


def _ensure_url_with_path(value: str, base: str, path: str, label: str) -> str:
    """
    If value is empty -> base+path
    If value looks like a base URL -> value+path
    Otherwise -> normalized value
    """
    v = _normalize_url(value)
    if not v:
        return _join_url(base, path)

    if _looks_like_base_url(v):
        return _join_url(v, path)

    # Best-effort sanity warnings (non-fatal), but *endpoint-aware*.
    # Avoid substring traps like "/swap/v1/quote" where "swap" is a namespace.
    try:
        req_kind = _endpoint_kind_from_path(path or "")
        val_kind = _endpoint_kind_from_path(urlparse(v).path or "")
        if req_kind and val_kind and req_kind != val_kind:
            _warn(
                "[CONFIG] %s looks like %s url but path requests %s (%r)",
                label,
                val_kind,
                req_kind,
                v,
            )
    except Exception:
        # never block config on warning logic
        pass

    return v


def _normalize_prefix(p: str, suffix: str = ":") -> str:
    s = str(p or "").strip()
    if not s:
        return ""
    return s if s.endswith(suffix) else f"{s}{suffix}"


def _normalize_env_name(s: str) -> str:
    x = (s or "dev").strip().lower()
    if x == "production":
        return "prod"
    if x == "development":
        return "dev"
    return x


def _normalize_postgres_dsn(dsn: str) -> str:
    """
    Render often provides DATABASE_URL like:
      postgres://user:pass@host:5432/db
    Many libs prefer:
      postgresql://user:pass@host:5432/db
    """
    s = str(dsn or "").strip()
    if not s:
        return ""
    if s.startswith("postgres://"):
        return "postgresql://" + s[len("postgres://") :]
    return s


def _normalize_pathlike(p: str) -> str:
    """
    Normalize path-like strings so Windows + Linux behave identically.
    - Strip wrapping quotes
    - Convert backslashes to forward slashes
    """
    s = str(p or "").strip()
    if not s:
        return ""
    if (s.startswith('"') and s.endswith('"')) or (s.startswith("'") and s.endswith("'")):
        s = s[1:-1].strip()
    return s.replace("\\", "/")


def _mask(s: str, keep_head: int = 4, keep_tail: int = 0) -> str:
    if not s:
        return ""
    s2 = str(s)
    if len(s2) <= keep_head + keep_tail:
        return "*" * len(s2)
    head = s2[:keep_head] if keep_head > 0 else ""
    tail = s2[-keep_tail:] if keep_tail > 0 else ""
    return f"{head}…{'*' * 6}{tail}"


def _normalize_log_level(level: str) -> str:
    s = (level or "INFO").strip().upper()
    if s.isdigit():
        try:
            n = int(s)
            name = logging.getLevelName(n)
            if isinstance(name, str) and not name.startswith("Level "):
                return name
            return "INFO"
        except Exception:
            return "INFO"
    if s in {"CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG"}:
        return s
    _warn("[CONFIG] LOG_LEVEL=%r unrecognized; using INFO", s)
    return "INFO"


def _is_http_url(s: str) -> bool:
    t = (s or "").strip().lower()
    return t.startswith("http://") or t.startswith("https://")


def _is_secret_key(name: str) -> bool:
    up = (name or "").upper()
    return any(h in up for h in _SECRET_KEY_HINTS)


def _redact_value(value: str, key: str = "") -> str:
    """
    Best-effort redaction. If logging_setup.redact_value exists, we try calling it.
    Supports both signatures:
      - redact_value(value)
      - redact_value(key, value)

    Fallback: conservative masking.
    """
    s = str(value or "")
    if not s:
        return ""

    fn = _get_logging_setup_redactor()
    if fn is not None:
        try:
            out = fn(s)  # type: ignore[misc]
            return str(out)
        except TypeError:
            try:
                out = fn(key, s)  # type: ignore[misc]
                return str(out)
            except Exception:
                return _mask(s)
        except Exception:
            return _mask(s)

    return _mask(s)


def _redact_dsn(dsn: str) -> str:
    """
    Redact credentials inside DSNs (best-effort).
    """
    if not dsn:
        return ""
    red = _redact_value(dsn, key="DSN")
    if red and red != dsn:
        return red

    s = str(dsn)
    try:
        idx = s.find("://")
        if idx == -1:
            return _mask(s, 6, 0)
        prefix = s[: idx + 3]
        rest = s[idx + 3 :]
        at = rest.find("@")
        if at == -1:
            return prefix + _mask(rest, 6, 0)
        creds = rest[:at]
        after = rest[at:]
        if ":" in creds:
            user = creds.split(":", 1)[0]
            return f"{prefix}{user}:***{after}"
        return f"{prefix}{_mask(creds, 3, 0)}{after}"
    except Exception:
        return _mask(s, 6, 0)


def _redact_url(url: str, key: str = "URL") -> str:
    """
    Redact URLs that may contain credentials (redis://:pass@host/0, https://user:pass@...).
    Uses DSN redaction fallback.
    """
    if not url:
        return ""
    if "://" not in url:
        return _redact_value(url, key=key)
    return _redact_dsn(url)


# =============================================================================
# DSN routing (runtime matches preflight)
# =============================================================================

_RENDER_HINT_KEYS = (
    "RENDER",
    "RENDER_SERVICE_ID",
    "RENDER_INSTANCE_ID",
    "RENDER_GIT_COMMIT",
    "RENDER_REGION",
    "RENDER_EXTERNAL_URL",
)


def _is_render_like(environ: Mapping[str, str]) -> bool:
    # Robust: any Render hint env var implies "render-like" runtime
    for k in _RENDER_HINT_KEYS:
        if _raw(environ, k):
            return True
    return False


def _maybe_env_ref(token: str) -> str | None:
    t = (token or "").strip()
    if not t:
        return None
    if t.startswith("${") and t.endswith("}") and len(t) > 3:
        inner = t[2:-1].strip()
        return inner or None
    if t.startswith("$") and len(t) > 1:
        inner = t[1:].strip()
        return inner or None
    if t.lower().startswith("env:") and len(t) > 4:
        inner = t[4:].strip()
        return inner or None
    return None


def _resolve_env_indirect(
    environ: Mapping[str, str],
    name: str,
    default: str = "",
    *,
    max_depth: int = 4,
) -> tuple[str, str]:
    """
    Resolve env var allowing indirection like:
      PG_DSN_EXTERNAL=DATABASE_URL
    returning (resolved_value, route_string).
    The route_string is compatible with the preflight-style “A -> B” narrative.
    """
    rawv = _raw(environ, name)
    if rawv is None:
        return default, ""

    cur = rawv.strip()
    route = [name]

    for _ in range(max_depth):
        if not cur:
            break

        # If it already looks like a URL/DSN, stop.
        if "://" in cur:
            break

        # Support ${VAR}, $VAR, env:VAR
        ref = _maybe_env_ref(cur)
        if ref:
            cur = ref

        # If it exactly names another env var, follow it.
        if cur.isupper() and cur in environ:
            route.append(cur)
            nxt = (_raw(environ, cur) or "").strip()
            if nxt == cur:
                break
            cur = nxt
            continue

        break

    if len(route) >= 2:
        return cur, " -> ".join(route)
    return cur, name


def _dsn_host(dsn: str) -> str:
    d = (dsn or "").strip()
    if not d:
        return ""
    try:
        return urlparse(d).hostname or ""
    except Exception:
        return ""


def _route_postgres_dsn(
    environ: Mapping[str, str],
) -> tuple[str, str, str, str, bool, str, str, str, str]:
    """
    Returns:
      (selected_dsn, route, host, mode, render_like, pg_dsn, pg_dsn_internal, pg_dsn_external, database_url)

    Selection rules:
      1) POSTGRES_DSN (explicit override) wins if set (supports indirection).
      2) Else honor PG_DSN_MODE in {auto, internal, external}.
      3) auto:
         - render-like: internal > PG_DSN > DATABASE_URL > external
         - local-like : external > PG_DSN > DATABASE_URL > internal
      4) Indirection supported on all inputs (e.g., PG_DSN_EXTERNAL=DATABASE_URL).

    LOCK-IN FIX:
      Render-specific DSNs are always considered as fallbacks to internal/external slots,
      so explicit modes behave correctly on Render.
    """
    mode = (_raw(environ, "PG_DSN_MODE") or _raw(environ, "DB_DSN_MODE") or "auto").strip().lower()
    if mode not in _ALLOWED_PG_DSN_MODE:
        _warn("[CONFIG] PG_DSN_MODE=%r invalid; using 'auto'", mode)
        mode = "auto"

    render_like = _is_render_like(environ)

    # Primary knobs (may be indirect)
    pg_dsn_raw, pg_dsn_route = _resolve_env_indirect(environ, "PG_DSN", "")
    pg_int_raw, pg_int_route = _resolve_env_indirect(environ, "PG_DSN_INTERNAL", "")
    pg_ext_raw, pg_ext_route = _resolve_env_indirect(environ, "PG_DSN_EXTERNAL", "")
    db_url_raw, db_url_route = _resolve_env_indirect(environ, "DATABASE_URL", "")
    pg_explicit_raw, pg_explicit_route = _resolve_env_indirect(environ, "POSTGRES_DSN", "")

    # Render/common fallbacks
    render_int_raw, render_int_route = _resolve_env_indirect(
        environ, "RENDER_INTERNAL_DATABASE_URL", ""
    )
    render_ext_raw, render_ext_route = _resolve_env_indirect(
        environ, "RENDER_EXTERNAL_DATABASE_URL", ""
    )
    render_db_raw, render_db_route = _resolve_env_indirect(environ, "RENDER_DATABASE_URL", "")
    external_db_raw, external_db_route = _resolve_env_indirect(environ, "EXTERNAL_DATABASE_URL", "")
    render_internal_alias_raw, render_internal_alias_route = _resolve_env_indirect(
        environ, "RENDER_INTERNAL_DB_URL", ""
    )
    render_external_alias_raw, render_external_alias_route = _resolve_env_indirect(
        environ, "RENDER_EXTERNAL_DB_URL", ""
    )

    # Normalize
    pg_dsn = _normalize_postgres_dsn(pg_dsn_raw)
    pg_int = _normalize_postgres_dsn(pg_int_raw)
    pg_ext = _normalize_postgres_dsn(pg_ext_raw)
    db_url = _normalize_postgres_dsn(db_url_raw)
    pg_explicit = _normalize_postgres_dsn(pg_explicit_raw)

    render_int = _normalize_postgres_dsn(render_int_raw or render_internal_alias_raw)
    render_ext = _normalize_postgres_dsn(render_ext_raw or render_external_alias_raw)
    render_db = _normalize_postgres_dsn(render_db_raw)
    external_db = _normalize_postgres_dsn(external_db_raw)

    # Fill canonical slots from Render/common keys (ALWAYS, regardless of mode)
    if not pg_int:
        if render_int:
            pg_int = render_int
            pg_int_route = (
                render_int_route or render_internal_alias_route or "RENDER_INTERNAL_DATABASE_URL"
            )
        elif render_db:
            pg_int = render_db
            pg_int_route = render_db_route or "RENDER_DATABASE_URL"

    if not pg_ext:
        if render_ext:
            pg_ext = render_ext
            pg_ext_route = (
                render_ext_route or render_external_alias_route or "RENDER_EXTERNAL_DATABASE_URL"
            )
        elif external_db:
            pg_ext = external_db
            pg_ext_route = external_db_route or "EXTERNAL_DATABASE_URL"

    if not db_url and render_db:
        db_url = render_db
        db_url_route = render_db_route or "RENDER_DATABASE_URL"

    # Select
    selected = ""
    route = ""
    if pg_explicit:
        selected = pg_explicit
        route = pg_explicit_route or "POSTGRES_DSN"
    else:
        if mode == "internal":
            if pg_int:
                selected, route = pg_int, pg_int_route or "PG_DSN_INTERNAL"
            elif pg_dsn:
                selected, route = pg_dsn, pg_dsn_route or "PG_DSN"
            elif db_url:
                selected, route = db_url, db_url_route or "DATABASE_URL"
            elif pg_ext:
                selected, route = pg_ext, pg_ext_route or "PG_DSN_EXTERNAL"
        elif mode == "external":
            if pg_ext:
                selected, route = pg_ext, pg_ext_route or "PG_DSN_EXTERNAL"
            elif pg_dsn:
                selected, route = pg_dsn, pg_dsn_route or "PG_DSN"
            elif db_url:
                selected, route = db_url, db_url_route or "DATABASE_URL"
            elif pg_int:
                selected, route = pg_int, pg_int_route or "PG_DSN_INTERNAL"
        else:
            # auto
            if render_like:
                if pg_int:
                    selected, route = pg_int, pg_int_route or "PG_DSN_INTERNAL"
                elif pg_dsn:
                    selected, route = pg_dsn, pg_dsn_route or "PG_DSN"
                elif db_url:
                    selected, route = db_url, db_url_route or "DATABASE_URL"
                elif pg_ext:
                    selected, route = pg_ext, pg_ext_route or "PG_DSN_EXTERNAL"
            else:
                if pg_ext:
                    selected, route = pg_ext, pg_ext_route or "PG_DSN_EXTERNAL"
                elif pg_dsn:
                    selected, route = pg_dsn, pg_dsn_route or "PG_DSN"
                elif db_url:
                    selected, route = db_url, db_url_route or "DATABASE_URL"
                elif pg_int:
                    selected, route = pg_int, pg_int_route or "PG_DSN_INTERNAL"

    host = _dsn_host(selected)
    return selected, route, host, mode, render_like, pg_dsn, pg_int, pg_ext, db_url


def _resolve_dsn_with_fallback(environ: Mapping[str, str], name: str, fallback: str) -> str:
    v, _ = _resolve_env_indirect(environ, name, fallback)
    v = (v or "").strip()
    if not v:
        v = fallback
    return _normalize_postgres_dsn(v)


# =============================================================================
# Env schema (minimal public contract; .env.example is the full source of truth)
# =============================================================================


@dataclass(frozen=True, slots=True)
class EnvVarSpec:
    name: str
    kind: str
    default: str
    desc: str
    aliases: tuple[str, ...] = ()
    example: str = ""


ENV_SCHEMA: tuple[EnvVarSpec, ...] = (
    EnvVarSpec("ENV", "str", "dev", "Environment name (dev/staging/prod)."),
    EnvVarSpec("LOG_LEVEL", "str", "INFO", "Log level (DEBUG/INFO/WARNING/ERROR)."),
    EnvVarSpec("SAFE_MODE", "bool", "0", "Relax secret requirements for local/tests."),
    EnvVarSpec("STRICT_CONFIG", "bool", "0", "Raise if config validation finds errors."),
    EnvVarSpec("PG_DSN_MODE", "str", "auto", "Postgres DSN routing: auto|internal|external."),
    EnvVarSpec("MASTER_PHASE", "int", "7", "Master phase (0..7)."),
    EnvVarSpec("STRATEGY_PHASE", "int", "7", "Strategy phase (0..7)."),
    EnvVarSpec(
        "WALLET_PRIVATE_KEY",
        "str",
        "",
        "Wallet private key JSON int array (preferred).",
        aliases=("WALLET_SECRET_KEY", "SOLANA_WALLET_PRIVATE_KEY", "PRIVATE_KEY"),
        example="[1,2,3,...]",
    ),
    EnvVarSpec(
        "KEYPAIR_PATH",
        "str",
        "",
        "Path to Solana keypair JSON file (alternative).",
        aliases=("WALLET_KEYPAIR_PATH",),
    ),
    EnvVarSpec("RPC_URL", "str", "https://api.mainnet-beta.solana.com", "Solana RPC URL."),
    EnvVarSpec("REDIS_URL", "str", "redis://localhost:6379/0", "Redis for queues/state."),
    EnvVarSpec(
        "POSTGRES_DSN",
        "str",
        "",
        "Postgres DSN (selected). Supports indirection via routing keys.",
        aliases=("PG_DSN", "PG_DSN_INTERNAL", "PG_DSN_EXTERNAL", "DATABASE_URL"),
    ),
    EnvVarSpec("JUP_BASE_URL", "str", "https://api.jup.ag", "Jupiter base URL (legacy v1)."),
    EnvVarSpec("JUP_QUOTE_PATH", "str", "/swap/v1/quote", "Jupiter quote path (legacy v1)."),
    EnvVarSpec("JUP_SWAP_PATH", "str", "/swap/v1/swap", "Jupiter swap path (legacy v1)."),
    EnvVarSpec("JUPITER_API_BASE", "str", "https://quote-api.jup.ag/v6", "Jupiter v6 base URL."),
    EnvVarSpec("JUP_AS_LEGACY", "bool", "0", "Prefer legacy v1 swap endpoints instead of v6."),
    EnvVarSpec("PF_SLIPPAGE_BPS", "int", "50", "Default slippage in bps."),
    EnvVarSpec("ENABLE_TELEGRAM", "bool", "0", "Enable Telegram alerts."),
    EnvVarSpec("TELEGRAM_BOT_TOKEN", "str", "", "Telegram bot token (if enabled)."),
    EnvVarSpec("TELEGRAM_CHAT_ID", "str", "", "Telegram chat id (if enabled)."),
)

# =============================================================================
# Typed config object (single source of truth)
# =============================================================================


@dataclass(frozen=True, slots=True)
class Config(AbcMapping[str, Any]):
    # ---- meta ----
    config_version: str = CONFIG_VERSION
    loaded_at_unix: int = CONFIG_LOADED_AT_UNIX

    # ---- app / logging ----
    APP_NAME: str = "solana-sniper-bot-pro"
    ENV: str = "dev"
    LOG_LEVEL: str = "INFO"
    SAFE_MODE: bool = False
    STRICT_CONFIG: bool = False
    IS_PROD: bool = False
    ENABLE_METRICS: bool = True
    METRICS_PORT: int = 9108
    SENTRY_DSN: str = ""
    SENTRY_ENV: str = "dev"
    GIT_COMMIT: str = ""
    RUN_ID: str = ""

    # ---- orchestrator ----
    MASTER_PHASE: int = 7
    STRATEGY_PHASE: int = 7
    USE_LEGACY_MAIN: bool = False
    ENTRY_WORKERS: int = 2
    CANDIDATE_QUEUE_MAX: int = 2000
    TICK_QUEUE_MAX: int = 4000
    SUPERVISOR_SEC: int = 5
    RESTART_BACKOFF_SEC: float = 2.0
    RESTART_MAX_BACKOFF_SEC: float = 60.0
    RESTART_MAX_PER_HOUR: int = 60
    HEALTH_LOG_SEC: int = 30
    STARVATION_WARN_SEC: float = 180.0
    NO_BUY_WARN_SEC: float = 300.0
    MAIN_IDLE_SLEEP_SEC: float = 0.5
    SHUTDOWN_GRACE_SEC: float = 15.0
    QUEUE_DROP_POLICY: str = "drop_newest"
    SCANNER_MODULES: list[str] = field(default_factory=list)
    MAIN_RESTART_ON_CRASH: bool = False
    MAIN_CRASH_BACKOFF_SEC: float = 2.0
    MAIN_CRASH_BACKOFF_MAX_SEC: float = 60.0
    CAND_DEDUP_ENABLED: bool = True
    CAND_DEDUP_TTL_SEC: float = 10.0
    CAND_DEDUP_MAX: int = 75_000
    TICK_DEDUP_ENABLED: bool = True
    TICK_DEDUP_TTL_SEC: float = 2.0
    TICK_DEDUP_MAX: int = 200_000
    FORWARD_TICKS_FROM_SCANNERS: bool = False
    MAIN_FAST_FAIL: bool = False
    MAIN_VALIDATE_ONLY: bool = False
    MAIN_THREADIFY_SYNC_MODULES: bool = False
    ALLOW_SCHEMA_CREATE: bool = False
    SKIP_DSN_REQUIRED: bool = False

    # ---- kill switches ----
    KILL_SWITCH: bool = False
    DISABLE_BUYS: bool = False
    DISABLE_SELLS: bool = False
    DISABLE_SCANNERS: bool = False
    PAUSE_TRADING: bool = False
    KILL_SWITCH_FILE: str = ""
    DISABLE_BUYS_FILE: str = ""
    DISABLE_SELLS_FILE: str = ""
    DISABLE_SCANNERS_FILE: str = ""
    KILL_WATCHDOG_SEC: float = 2.0
    KILL_SHUTDOWN_ON_TRIGGER: bool = False

    # ---- chain / rpc ----
    NETWORK: str = "mainnet"
    RPC_URL: str = "https://api.mainnet-beta.solana.com"
    RPC_URLS: list[str] = field(default_factory=list)
    WEBSOCKET_URL: str = ""
    COMMITMENT: str = "confirmed"
    TX_RPC_URL: str = "https://api.mainnet-beta.solana.com"
    HELIUS_RPC_URL: str = ""
    ENABLE_JITO: bool = False
    JITO_BLOCK_ENGINE_URL: str = ""
    JITO_TIP_LAMPORTS: int = 50_000
    JITO_FEE_MICROLAMPORTS: int = 0
    SOL_MINT: str = "So11111111111111111111111111111111111111112"
    USDC_MINT: str = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"

    # ---- wallet ----
    WALLET_PRIVATE_KEY: str = ""
    KEYPAIR_PATH: str = ""
    WALLET_PUBKEY: str = ""
    SAVINGS_WALLET: str = ""
    OWNER: str = ""
    OWNER_B58: str = ""
    MIN_SOL_BALANCE: float = 0.20

    # ---- external APIs / scanners ----
    COINGECKO_SOL_API: str = (
        "https://api.coingecko.com/api/v3/simple/price?ids=solana&vs_currencies=usd"
    )
    RAYDIUM_API_URL: str = "https://api.raydium.io/pairs"
    RAYDIUM_SEARCH_QUERIES: list[str] = field(default_factory=lambda: ["raydium", "solana"])
    DEXSCREENER_API_URL: str = "https://api.dexscreener.com/latest/dex/tokens"
    DEXSCREENER_BASE_URL: str = "https://api.dexscreener.com"
    PUMPFUN_API_URL: str = "https://pump.fun/api/v1/tokens"
    PUMPFUN_API_BASE: str = ""
    PUMPFUN_HTTP_TIMEOUT_S: int = 15
    PUMPFUN_HTTP_CONN_LIMIT: int = 30
    PUMPFUN_API_LIMIT: int = 20
    PUMPFUN_DOM_LIMIT: int = 20
    PUMPFUN_SKIP_NEXT: bool = False
    PUMPFUN_SKIP_HTMLNEXT: bool = False
    BIRDEYE_API_URL: str = "https://public-api.birdeye.so"
    BIRDEYE_API_KEY: str = ""
    BIRDEYE_ENABLED: bool = False

    # ---- jupiter/router ----
    # Legacy v1
    JUP_BASE_URL: str = "https://api.jup.ag"
    JUP_ALT_BASE_URLS: list[str] = field(default_factory=list)
    JUP_QUOTE_PATH: str = "/swap/v1/quote"
    JUP_SWAP_PATH: str = "/swap/v1/swap"
    # Modern v6
    JUPITER_API_BASE: str = "https://quote-api.jup.ag/v6"
    JUP_AS_LEGACY: bool = False
    JUP_USE_SHARED_ACCOUNTS: bool = True
    JUP_ONLY_DIRECT: bool = False
    JUP_PRO_KEY: str = ""
    JUP_STRICT_AUTH: bool = True
    JUP_ALLOW_PUBLIC_FALLBACK: bool = False

    # Computed “final” endpoints (safe to call directly)
    JUPITER_QUOTE_API: str = ""
    JUPITER_SWAP_API: str = ""

    JUPITER_MAX_SLIPPAGE: float = 0.005  # fraction
    JUPITER_SLIPPAGE_BPS: int = 50
    PF_SLIPPAGE_BPS: int = 50
    PF_WRAP_UNWRAP_SOL: bool = True
    PF_COMPUTE_UNIT_LIMIT: int = 0
    PF_DEBUG_TX: bool = False

    # ---- alerts / integrations ----
    TELEGRAM_BOT_TOKEN: str = ""
    TELEGRAM_CHAT_ID: str = ""
    DISCORD_WEBHOOK: str = ""
    ENABLE_TELEGRAM: bool = False
    ENABLE_DISCORD: bool = False
    NOTION_TOKEN: str = ""
    NOTION_DATABASE_ID: str = ""
    NOTION_ENABLED: bool = False
    ENABLE_NOTION_LOGGING: bool = False
    NOTION_TITLE_PROP: str = "Token"
    NOTION_PROP_TOKEN: str = "Mint"
    NOTION_PROP_ENTRY: str = "Entry Price"
    NOTION_PROP_CURRENT: str = "Current Price"
    NOTION_PROP_PROFIT: str = "Profit"
    NOTION_PROP_REASON: str = "Reason"
    NOTION_PROP_TIME: str = "Timestamp"
    OPENAI_API_KEY: str = ""
    OPENAI_MODEL: str = "gpt-4o-mini"
    OPENAI_BASE_URL: str = ""
    FINE_TUNED_MODEL: str = ""

    # ---- storage ----
    REDIS_URL: str = "redis://localhost:6379/0"
    REDIS_PREFIX: str = "sniper:"
    SQLITE_DB_PATH: str = "data/state.db"
    POSITIONS_BACKEND: str = "json"

    # DSN routing metadata (non-breaking additions)
    PG_DSN_MODE: str = "auto"
    PG_DSN_ROUTE: str = ""
    PG_DSN_HOST: str = ""
    RENDER_LIKE: bool = False

    POSITIONS_JSON_PATH: str = "data/positions.json"
    PG_ENABLED: bool = False
    PG_DSN: str = ""
    PG_DSN_INTERNAL: str = ""
    PG_DSN_EXTERNAL: str = ""
    POSTGRES_DSN: str = ""
    SNIPER_DB_DSN: str = ""
    PNL_DB_DSN: str = ""
    PNL_DSN: str = ""
    POSITIONS_PG_DSN: str = ""
    POSITIONS_PG_ENABLED: bool = False

    # ---- trading / execution ----
    AUTO_BUY_ENABLED: bool = True
    AUTO_SELL_ENABLED: bool = True
    DRY_RUN: bool = False
    TRANSACTION_TIMEOUT: int = 20
    MAX_RETRIES: int = 3
    EXEC_DELAY: int = 0

    # ---- capability flags (LOCK-IN; used by validate_config + preflight) ----
    buys_possible: bool = False
    sells_possible: bool = False
    live_trading: bool = False
    scanning_possible: bool = False
    wallet_required: bool = False
    redis_required: bool = False
    rpc_required: bool = False
    db_required: bool = False

    # uppercase mirrors (exported constants; avoid style drift across older modules)
    BUYS_POSSIBLE: bool = False
    SELLS_POSSIBLE: bool = False
    LIVE_TRADING: bool = False
    SCANNING_POSSIBLE: bool = False
    WALLET_REQUIRED: bool = False
    REDIS_REQUIRED: bool = False
    RPC_REQUIRED: bool = False
    DB_REQUIRED: bool = False

    BUY_SOL_PER_TRADE: float = 0.02
    ENTRY_MIN_BUY_SOL: float = 0.015
    ENTRY_MAX_BUY_SOL: float = 0.10
    ENTRY_MIN_INITIAL_BUY_SOL: float = 0.0025

    MAX_RISK_PER_TRADE: float = 0.05
    MIN_VOLATILITY: float = 0.02
    PARTIAL_SELL_CHUNKS: int = 4

    TICKET_PCT_EQUITY: float = 0.04
    TICKET_USD_CAP: float = 300.0

    TARGET_PROFIT_PERCENT: float = 200.0
    STOP_LOSS_PERCENT: float = 20.0
    TRAILING_STOP_ENABLED: bool = True
    MAX_HOLD_TIME_MINUTES: int = 360
    HOLD_IF_SCORE_ABOVE: float = 0.85

    TAKE_PROFIT1_PCT: float = 30.0
    TAKE_PROFIT2_PCT: float = 60.0
    TAKE_PROFIT3_PCT: float = 120.0

    REINVESTOR_ENABLED: bool = True
    REINVEST_PERCENT: float = 80.0
    REINVESTMENT_PERCENT_CAP: float = 50.0
    REINVESTMENT_VOLUME_MIN: float = 5_000.0
    REINVESTMENT_MIN_PROFIT_SOL: float = 0.01
    REINVESTMENT_HONEYPOT_CHECK: bool = True

    # ---- risk manager ----
    RISK_PROFILE: str = "balanced"
    RISK_MODE_OVERRIDE: str | None = None
    RISK_MAX_EQUITY_PCT_PER_TRADE: float = 0.04
    RISK_MAX_EQUITY_PCT_PER_COIN: float = 0.10
    RISK_MAX_OPEN_POSITIONS: int = 6
    RISK_MAX_PARALLEL_BUYS: int = 3
    RISK_MAX_DAILY_DRAWDOWN_PCT: float = 18.0
    RISK_MAX_DAILY_LOSS_SOL: float = 9999.0
    RISK_CIRCUIT_BREAKER_MINUTES: int = 45
    STOP_AFTER_DAILY_PROFIT_PCT: float = 25.0
    DAILY_SPEND_CAP_PCT: float = 35.0
    RISK_HARD_KILL_ON_DRAWNDOWN: bool = True
    RISK_ALLOW_REENTRY_AFTER_BREAK: bool = True

    # ---- exit engine ----
    EXIT_STOP_LOSS_PCT: float = 20.0
    EXIT_TP1_PCT: float = 30.0
    EXIT_TP2_PCT: float = 60.0
    EXIT_TP3_PCT: float = 120.0
    EXIT_TSL_ENABLED: bool = True
    EXIT_TSL_ARM_PCT: float = 12.0
    EXIT_TSL_STEP_PCT: float = 4.5
    EXIT_STAGNANT_MAX_MIN: int = 10
    EXIT_STAGNANT_MIN_MOVE_PCT: float = 5.0
    EXIT_DEAD_COIN_MAX_MIN: int = 45
    EXIT_DEAD_MIN_LIQ_USD: float = 400.0
    EXIT_FORCE_AFTER_X_MULT: float = 8.0
    EXIT_LOCK_PROFIT_AFTER_MULT: float = 3.0
    EXIT_LOCK_PROFIT_TSL_STEP_PCT: float = 3.5
    AUTO_EXIT_WORKER_ENABLED: bool = True

    # ---- entry momentum guard ----
    ENTRY_MOMENTUM_ENABLED: bool = True
    ENTRY_MOMO_WINDOW_SEC: int = 25
    ENTRY_MOMO_MIN_TRADES: int = 2
    ENTRY_MOMO_MIN_UP_PCT: float = 2.0
    ENTRY_MOMO_ALLOW_NO_DATA_FOR_PUMP: bool = True

    # ---- scanner loop ----
    SCAN_INTERVAL: int = 60
    SCANNER_PERIOD_SEC: int = 4
    BUY_COOLDOWN_SEC: int = 3
    PF_MAX_CONCURRENT: int = 3
    PREWARM_TOP_N: int = 5
    SCANNER_MIN_LIQ_USD: float = 150.0
    MIN_LIQ_USD: float = 150.0
    MIN_POOL_AGE_MIN: int = 1
    NOROUTE_COOLDOWN_SEC: int = 600
    BLOCKLIST_MINTS: list[str] = field(default_factory=list)
    ALLOWLIST_MINTS: list[str] = field(default_factory=list)
    STRATEGY_PRESET: str = "max-pnl"

    TELEGRAM_HEARTBEATS: bool = True
    HEARTBEAT_EVERY_SEC: int = 300
    HEARTBEAT_ONLY_ON_CHANGE: bool = True
    TELEGRAM_NOTIFY_ROUTE_BLOCKS: bool = True
    PNL_TELEGRAM_HEARTBEAT: bool = True

    MOONSHOT_ENABLED: bool = True
    MOONSHOT_DAILY_USD: float = 100.0
    MOONSHOT_MAX_TRADE_USD: float = 25.0
    MOONSHOT_AGE_CUTOFF_MIN: int = 30
    MOONSHOT_LIQ_CUTOFF_USD: float = 20_000.0
    MOONSHOT_IMPACT_CUTOFF_BPS: int = 150

    # ---- scanners toggles ----
    ENABLE_RAYDIUM_SCANNER: bool = True
    ENABLE_PUMPFUN_SCANNER: bool = True
    ENABLE_DEXSCREENER_SCANNER: bool = True
    ENABLE_ARBITRAGE: bool = False
    DEXES: list[str] = field(default_factory=lambda: ["jupiter", "raydium", "orca"])

    SCANNER_MIN_FDV_USD: float = 0.0
    SCANNER_MIN_FRAUD_SCORE: float = 0.0
    SCANNER_ALERTS_ENABLED: bool = True
    SCANNER_MIN_BUYS_5M: int = 0
    SCANNER_MIN_BUYS_15M: int = 0
    SCANNER_MIN_VOLUME_5M_USD: float = 20.0
    SCANNER_MIN_VOLUME_15M_USD: float = 60.0
    SCANNER_REQUIRE_BUY_PRESSURE: bool = False
    SCANNER_MIN_BUY_RATIO: float = 0.0
    SCANNER_MIN_PRICE_CHANGE_5M: float = 0.0
    SCANNER_MIN_PRICE_CHANGE_15M: float = 0.0
    SCANNER_MAX_AGE_MIN: int = 720
    SCANNER_MAX_MC_USD: float = 50_000_000.0
    SCANNER_MIN_HOLDERS: int = 0

    ALERT_COOLDOWN_SECONDS: int = 900
    POSITION_MONITOR_INTERVAL: int = 20
    REINVESTMENT_CHECK_INTERVAL: int = 60

    # ---- pair filters ----
    PF_SCAN_MIN_LIQ_USD: float = 400.0
    PF_SCAN_MAX_LIQ_USD: float = 300_000.0
    PF_SCAN_MIN_MCAP_USD: float = 800.0
    PF_SCAN_MAX_MCAP_USD: float = 1_500_000.0
    PF_SCAN_MAX_TOKEN_AGE_MIN: float = 180.0
    PF_SCAN_MIN_5M_VOL_USD: float = 50.0
    PF_SCAN_MIN_5M_BUYS: int = 1
    PF_SCAN_MIN_BUY_TO_SELL_RATIO: float = 1.02
    PF_SCAN_MIN_WHALE_SIZE_SOL: float = 1.5
    PF_SCAN_MIN_WHALE_COUNT_5M: int = 0
    PF_MAX_RUG_RISK_SCORE: float = 0.80
    PF_MIN_MOMENTUM_SCORE: float = 0.14
    PF_MIN_FINAL_SCORE: float = 0.10
    PF_RISK_PENALTY_WEIGHT: float = 0.6

    # ---- AI knobs ----
    AI_ENABLED: bool = False
    AI_ENTRY_ENABLED: bool = True
    AI_ENTRY_MODEL: str = ""
    AI_ENTRY_SCORE_MIN: float = 0.60
    AI_ENTRY_SCORE_HARD_MIN: float = 0.50
    AI_ENTRY_RISK_ADJ_MIN: float = 0.50
    AI_ENTRY_THRESHOLD: float = 0.60
    AI_SCORE_GATE: float = 0.60
    AI_SCORE_SOFT_FLOOR: float = 0.50
    AI_SCORE_HARD_FLOOR: float = 0.38
    AI_ENTRY_MAX_CONCURRENCY: int = 8
    AI_PRE_SCORE_ENABLED: bool = True
    AI_PRE_SCORE_MIN: float = 0.10
    ENTRY_REQUIRE_RISK_CAPACITY: bool = True
    ENTRY_RISK_CAPACITY_MIN: float = 0.30

    AI_RUG_ENABLED: bool = True
    AI_RUG_MODEL: str = ""
    AI_RUG_MAX_SCORE: float = 0.80
    AI_RUG_MIN_CONFIDENCE: float = 0.55

    AIGATE_MAX_PRICE_IMPACT_BPS: int = 550
    AI_BAD_THRESHOLD: float = 0.35
    AI_GOOD_THRESHOLD: float = 0.70

    AI_WHALE_ECHO: bool = True
    AI_WHALE_ECHO_MIN_SIZE_USD: float = 300.0

    AI_ADAPTIVE_SELL: bool = True
    AI_SELL_ENABLED: bool = True
    AI_SELL_MODEL: str = ""
    AI_SELL_HOLD_THRESHOLD: float = 0.60
    AI_SELL_TRIM_THRESHOLD: float = 0.72
    AI_SELL_EXIT_THRESHOLD: float = 0.84

    STRATEGY_REDIS_PREFIX: str = "strategy:v1"
    STRATEGY_UPDATE_INTERVAL: int = 1800
    AI_FEATURE_VERSION: str = "v3+++"
    AI_BUY_FEATURE_VERSION: str = "v3+++"

    # ---- regime detection ----
    REGIME_ENABLED: bool = True
    REGIME_UPDATE_INTERVAL_SEC: int = 60
    REGIME_SOL_MIN_24H_VOL_USD: float = 200_000_000.0
    REGIME_MEME_MIN_24H_VOL_USD: float = 50_000_000.0
    REGIME_MAX_ACCEPTABLE_RUG_RATE: float = 0.25

    # ---- AI data logging / training ----
    LOG_ENTRY_CANDIDATES: bool = True
    LOG_TRADES: bool = True
    LOG_REGIME_SNAPSHOTS: bool = True
    TRAIN_LABEL_WINDOW_MIN: int = 60
    TRAIN_MIN_HOLD_MIN: int = 10
    TRAIN_MAX_HOLD_MIN: int = 720

    # ---- whales / smart wallets ----
    SMART_WALLETS_ENABLED: bool = True
    ENABLE_WHALE_FILTERS: bool = True
    DYNAMIC_WHALE_DISCOVERY: bool = True
    CURATED_WHALE_WALLETS: list[str] = field(default_factory=list)
    WHALE_WALLETS_JSON: dict[str, Any] = field(default_factory=dict)
    WHALE_WALLETS: dict[str, str] = field(default_factory=dict)

    ENABLE_SOCIAL_SCRAPING: bool = False
    X_BEARER_TOKEN: str = ""
    TELEGRAM_SCRAPE_BOT: str = ""
    DISCORD_SEARCH_CHANNEL_ID: str = ""

    # ---- daytrader (optional) ----
    ENABLE_DAYTRADER: bool = False
    DAYTRADER_SYMBOLS: list[str] = field(default_factory=lambda: ["SOL/USDC"])
    DAYTRADER_TIMEFRAMES: list[str] = field(default_factory=lambda: ["1m", "5m", "15m", "1h"])
    DAYTRADER_EXCHANGES: list[str] = field(default_factory=lambda: ["binance", "bybit", "okx"])
    DT_FEATURE_SET: list[str] = field(
        default_factory=lambda: ["rsi", "macd", "bbands", "atr", "orderflow"]
    )
    DT_MODEL_KIND: str = "xgboost"
    DT_PROB_THRESHOLD: float = 0.62
    DT_MAX_CONCURRENT: int = 3
    DT_MAX_LEVERAGE: float = 1.0
    DT_MAX_RISK_PER_TRADE: float = 0.02
    DT_TAKE_PROFIT_BPS: int = 120
    DT_STOP_LOSS_BPS: int = 60
    DT_TRAILING_BPS: int = 50
    DT_COOLDOWN_SEC: int = 60
    DT_PAPER_TRADING: bool = True

    # ---- backtest / diagnostics ----
    BACKTEST_ENABLED: bool = False
    BACKTEST_SOURCE: str = "pg"
    BACKTEST_MAX_WORKERS: int = 4
    DIAG_BT_DAYS_BACK: int = 5
    DIAG_BT_LIMIT_CANDIDATES: int = 200
    BT_DAYS_BACK: str = ""
    BT_LIMIT_CANDIDATES: str = ""

    # ---- legacy / compat exports (contract tests + old modules) ----
    SOLANA_ENDPOINT: str = ""
    SOLANA_RPC_URL: str = ""
    SOL_RPC_URL: str = ""
    DRYRUN: bool = False
    ENABLE_PUMP_SCANNER: bool = False
    LIQ_MIN_USD: float = 0.0
    MAX_CONCURRENT_POS: int = 0
    MAX_CONCURRENT_BUYS: int = 0
    MAX_CONCURRENCY: int = 0

    PF_TSL_ARM_PCT: float = 0.0
    PF_TSL_PCT: float = 0.0
    PF_SL_PCT: float = 0.0

    RAYDIUM_PAIRS_API: str = ""
    DEXSCREENER_TOKENS_API: str = ""
    PUMPFUN_TOKENS_API: str = ""

    PRIVATE_KEY: str = ""
    TARGET_MARKET: str = "SOL/USDC"

    def __repr__(self) -> str:
        # Safe repr (never dump secrets into logs)
        return (
            f"Config(env={self.ENV!r}, prod={self.IS_PROD}, "
            f"phase={self.MASTER_PHASE}, fp={self.fingerprint()})"
        )

    def fingerprint(self) -> str:
        """
        Deterministic (redacted) fingerprint for log correlation and “same config everywhere” checks.
        Does NOT include secrets.
        """
        payload = {
            "v": self.config_version,
            "env": self.ENV,
            "log": self.LOG_LEVEL,
            "phase": self.MASTER_PHASE,
            "strategy": self.STRATEGY_PHASE,
            "rpc": self.RPC_URL,
            "redis_prefix": self.REDIS_PREFIX,
            "pg_enabled": self.PG_ENABLED,
            "pg_mode": self.PG_DSN_MODE,
            "pg_route": self.PG_DSN_ROUTE,
            "positions_backend": self.POSITIONS_BACKEND,
            "jup_as_legacy": self.JUP_AS_LEGACY,
            "jup_quote": self.JUPITER_QUOTE_API,
            "jup_swap": self.JUPITER_SWAP_API,
            "dry_run": self.DRY_RUN,
            "caps": {
                "buys_possible": self.buys_possible,
                "live_trading": self.live_trading,
                "db_required": self.db_required,
                "redis_required": self.redis_required,
                "rpc_required": self.rpc_required,
            },
            "scanners": {
                "ray": self.ENABLE_RAYDIUM_SCANNER,
                "pf": self.ENABLE_PUMPFUN_SCANNER,
                "dex": self.ENABLE_DEXSCREENER_SCANNER,
            },
        }
        raw = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode(
            "utf-8"
        )
        return hashlib.sha256(raw).hexdigest()[:16]

    def summarize(self) -> dict[str, Any]:
        # IMPORTANT: summarize() must be safe to log (redact secrets)
        return {
            "meta": {
                "config_version": self.config_version,
                "loaded_at_unix": self.loaded_at_unix,
                "git_commit": (self.GIT_COMMIT[:12] if self.GIT_COMMIT else ""),
                "run_id": self.RUN_ID,
                "fingerprint": self.fingerprint(),
            },
            "env": {
                "app": self.APP_NAME,
                "env": self.ENV,
                "log_level": self.LOG_LEVEL,
                "phase": self.MASTER_PHASE,
                "strategy_phase": self.STRATEGY_PHASE,
                "safe_mode": self.SAFE_MODE,
                "strict_config": self.STRICT_CONFIG,
            },
            "caps": {
                "buys_possible": self.buys_possible,
                "sells_possible": self.sells_possible,
                "live_trading": self.live_trading,
                "scanning_possible": self.scanning_possible,
                "wallet_required": self.wallet_required,
                "redis_required": self.redis_required,
                "rpc_required": self.rpc_required,
                "db_required": self.db_required,
            },
            "dsn": {
                "render_like": self.RENDER_LIKE,
                "pg_enabled": self.PG_ENABLED,
                "pg_dsn_mode": self.PG_DSN_MODE,
                "pg_dsn_route": self.PG_DSN_ROUTE,
                "pg_dsn_host": self.PG_DSN_HOST,
                "postgres_dsn_set": bool(self.POSTGRES_DSN),
                "postgres_dsn_redacted": _redact_dsn(self.POSTGRES_DSN),
            },
            "kill": {
                "kill_switch": self.KILL_SWITCH,
                "disable_buys": self.DISABLE_BUYS,
                "disable_sells": self.DISABLE_SELLS,
                "disable_scanners": self.DISABLE_SCANNERS,
                "pause_trading": self.PAUSE_TRADING,
                "kill_file": bool(self.KILL_SWITCH_FILE),
            },
            "rpc": {
                "rpc_url": self.RPC_URL,
                "rpc_urls": self.RPC_URLS[:3],
                "ws": self.WEBSOCKET_URL,
                "commitment": self.COMMITMENT,
                "jito": self.ENABLE_JITO,
            },
            "jupiter": {
                "as_legacy": self.JUP_AS_LEGACY,
                "base_v1": self.JUP_BASE_URL,
                "base_v6": self.JUPITER_API_BASE,
                "quote": self.JUPITER_QUOTE_API,
                "swap": self.JUPITER_SWAP_API,
                "only_direct": self.JUP_ONLY_DIRECT,
                "shared_accounts": self.JUP_USE_SHARED_ACCOUNTS,
                "strict_auth": self.JUP_STRICT_AUTH,
                "allow_public_fallback": self.JUP_ALLOW_PUBLIC_FALLBACK,
                "pro_key": _redact_value(self.JUP_PRO_KEY, "JUP_PRO_KEY"),
            },
            "wallet": {
                "pubkey": self.WALLET_PUBKEY,
                "has_privkey": bool(self.WALLET_PRIVATE_KEY),
                "keypair_path": self.KEYPAIR_PATH,
                "min_sol_balance": self.MIN_SOL_BALANCE,
            },
            "toggles": {
                "dry_run": self.DRY_RUN,
                "auto_buy": self.AUTO_BUY_ENABLED,
                "auto_sell": self.AUTO_SELL_ENABLED,
            },
            "scanners": {
                "raydium": self.ENABLE_RAYDIUM_SCANNER,
                "pumpfun": self.ENABLE_PUMPFUN_SCANNER,
                "dexscreener": self.ENABLE_DEXSCREENER_SCANNER,
                "scanner_period_sec": self.SCANNER_PERIOD_SEC,
                "min_liq_usd": self.MIN_LIQ_USD,
            },
            "ai": {
                "enabled": self.AI_ENABLED,
                "entry_enabled": self.AI_ENTRY_ENABLED,
                "score_gate": self.AI_SCORE_GATE,
                "openai_key": _redact_value(self.OPENAI_API_KEY, "OPENAI_API_KEY"),
                "ft_model": self.FINE_TUNED_MODEL,
            },
            "storage": {
                "redis": _redact_url(self.REDIS_URL, "REDIS_URL"),
                "redis_prefix": self.REDIS_PREFIX,
                "positions_backend": self.POSITIONS_BACKEND,
                "sqlite_db_path": self.SQLITE_DB_PATH,
                "positions_json_path": self.POSITIONS_JSON_PATH,
            },
            "legacy": {
                "target_market": self.TARGET_MARKET,
                "private_key_set": bool(self.PRIVATE_KEY),
            },
        }

    # -------------------------------------------------------------------------
    # Dict / Mapping compatibility shim (LOCK-IN, zero-side-effect)
    # -------------------------------------------------------------------------

    @staticmethod
    @lru_cache(maxsize=1)
    def _field_names() -> tuple[str, ...]:
        # Cached for speed + determinism
        return tuple(f.name for f in dataclasses.fields(Config))

    def _resolve_key(self, key: str) -> str:
        """
        Accept exact field name first, then fall back to UPPERCASE.
        This matches how older code often uses cfg['RPC_URL'] etc.
        """
        k = (key or "").strip()
        if not k:
            return k
        names = self._field_names()
        if k in names:
            return k
        up = k.upper()
        if up in names:
            return up
        return k

    def __getitem__(self, key: str) -> Any:
        k = self._resolve_key(key)
        if not k:
            raise KeyError(key)
        if hasattr(self, k):
            return getattr(self, k)
        raise KeyError(key)

    def get(self, key: str, default: Any = None) -> Any:
        try:
            return self[key]
        except KeyError:
            return default

    def __iter__(self) -> Iterator[str]:
        # Mapping protocol: iterate keys
        yield from self._field_names()

    def __len__(self) -> int:
        return len(self._field_names())

    def __contains__(self, key: object) -> bool:
        if not isinstance(key, str):
            return False
        k = self._resolve_key(key)
        return bool(k) and (k in self._field_names())

    # NOTE: Mapping protocol requires view-returning methods.
    # Keep list-returning compatibility via *_list helpers below.
    def keys(self) -> KeysView[str]:
        return AbcMapping.keys(self)

    def items(self) -> ItemsView[str, Any]:
        return AbcMapping.items(self)

    def values(self) -> ValuesView[Any]:
        return AbcMapping.values(self)

    # Backward-compat helpers (old behavior)
    def keys_list(self) -> list[str]:
        return list(self.keys())

    def items_list(self) -> list[tuple[str, Any]]:
        return list(self.items())

    def values_list(self) -> list[Any]:
        return list(self.values())

    def to_dict(self, *, redact_secrets: bool = False) -> dict[str, Any]:
        """
        Full config as a dict.
        - redact_secrets=False: raw values (use carefully)
        - redact_secrets=True : best-effort redaction for string secrets/urls/dsns
        """
        out: dict[str, Any] = {}
        for k in self._field_names():
            v = getattr(self, k)
            if redact_secrets and isinstance(v, str) and v:
                # Heuristic: treat DSNs/URLs differently to avoid leaking creds
                up = k.upper()
                if "DSN" in up or up.endswith("_DSN") or "DATABASE" in up:
                    out[k] = _redact_dsn(v)
                elif "URL" in up or "WEBHOOK" in up or "REDIS" in up:
                    out[k] = _redact_url(v, key=k)
                elif _is_secret_key(k):
                    out[k] = _redact_value(v, key=k)
                else:
                    out[k] = v
            else:
                out[k] = v
        return out


# =============================================================================
# Safe iteration + legacy export system (slots-proof)
# =============================================================================


def _iter_config_items(cfg: Any) -> Iterable[tuple[str, Any]]:
    if dataclasses.is_dataclass(cfg):
        for f in dataclasses.fields(cfg):
            yield f.name, getattr(cfg, f.name)
        return

    d = getattr(cfg, "__dict__", None)
    if isinstance(d, dict):
        yield from d.items()
        return

    slots = getattr(cfg, "__slots__", ())
    if isinstance(slots, str):
        slots = (slots,)
    for name in slots:
        if not name or name.startswith("_"):
            continue
        yield name, getattr(cfg, name, None)


def _export_legacy(cfg: Config) -> None:
    """
    Export every Config field as a module-level constant for backward compatibility.
    (New code should use C / CONFIG directly.)
    """
    g = globals()
    reserved = {
        "Config",
        "CONFIG",
        "C",
        "ENV_SCHEMA",
        "validate_config",
        "validate_environment",
        "_iter_config_items",
        "_export_legacy",
        "_build_config",
        "build_config",
        "get_config",
        "reload_config_for_tests",
        "apply_config",
        "__getattr__",
        "__all__",
        "CONFIG_ERRORS",
        "CONFIG_WARNINGS",
        "CONFIG_FINGERPRINT",
        "_CONFIG_ERRORS",
        "_CONFIG_WARNINGS",
        "CONFIG_VERSION",
        "CONFIG_LOADED_AT_UNIX",
    }
    for k, v in _iter_config_items(cfg):
        if k in reserved or k.startswith("_"):
            continue
        g[k] = v


def __getattr__(name: str) -> Any:
    # Allows legacy "from config import SOME_FIELD" to keep working even if not exported for some reason.
    if hasattr(CONFIG, name):
        return getattr(CONFIG, name)
    raise AttributeError(name)


# =============================================================================
# Build config (single pass, stable defaults; local/Render drift-proof)
# =============================================================================


def _build_config(environ: Mapping[str, str]) -> Config:
    pytest_mode = _is_pytest(environ)
    safe_mode = _is_safe_mode(environ)

    # ---- app / logging ----
    app_name = _get_str(environ, "APP_NAME", "solana-sniper-bot-pro")
    env_name = _normalize_env_name(_get_str(environ, "ENV", "dev"))
    if env_name not in _ALLOWED_ENVS:
        _warn("[CONFIG] ENV=%r not recognized; treating as 'dev'", env_name)
        env_name = "dev"

    is_prod = env_name == "prod"
    strict_default = bool(is_prod and (not pytest_mode) and (not safe_mode))
    strict = _get_bool(environ, "STRICT_CONFIG", strict_default)

    log_level = _normalize_log_level(_get_str(environ, "LOG_LEVEL", "INFO"))
    enable_metrics = _get_bool(environ, "ENABLE_METRICS", True)
    metrics_port = _clamp_i(_get_int(environ, "METRICS_PORT", 9108), 1, 65535)
    sentry_dsn = _get_str(environ, "SENTRY_DSN", "")
    sentry_env = _get_str(environ, "SENTRY_ENV", env_name)

    git_commit = (
        _get_str(environ, "GIT_COMMIT", "")[:40] or _get_str(environ, "RENDER_GIT_COMMIT", "")[:40]
    ).strip()
    run_id = (
        _get_str(environ, "RUN_ID", "").strip()
        or _get_str(environ, "RENDER_INSTANCE_ID", "").strip()
        or _get_str(environ, "RENDER_SERVICE_ID", "").strip()
        or _get_str(environ, "HOSTNAME", "").strip()
        or _get_str(environ, "COMPUTERNAME", "").strip()
    )

    # ---- orchestrator ----
    master_phase = _clamp_i(_get_int(environ, "MASTER_PHASE", 7), 0, 7)
    strategy_phase = _clamp_i(_get_int(environ, "STRATEGY_PHASE", master_phase), 0, 7)
    allow_schema_create = _get_bool(environ, "ALLOW_SCHEMA_CREATE", False)
    skip_dsn_required = _get_bool(environ, "SKIP_DSN_REQUIRED", False)

    use_legacy_main = _get_bool(environ, "USE_LEGACY_MAIN", False)
    entry_workers = _clamp_i(_get_int(environ, "ENTRY_WORKERS", 2), 1, 64)
    cand_q_max = _clamp_i(_get_int(environ, "CANDIDATE_QUEUE_MAX", 2000), 100, 2_000_000)
    tick_q_max = _clamp_i(_get_int(environ, "TICK_QUEUE_MAX", 4000), 100, 2_000_000)

    supervisor_sec = _clamp_i(_get_int(environ, "SUPERVISOR_SEC", 5), 1, 60)
    restart_backoff = _clamp_f(_get_float(environ, "RESTART_BACKOFF_SEC", 2.0), 0.1, 600.0)
    restart_max_backoff = _clamp_f(
        _get_float(environ, "RESTART_MAX_BACKOFF_SEC", 60.0), 1.0, 3600.0
    )
    restart_max_per_hour = _clamp_i(_get_int(environ, "RESTART_MAX_PER_HOUR", 60), 1, 10_000)

    health_log_sec = _clamp_i(_get_int(environ, "HEALTH_LOG_SEC", 30), 5, 3600)
    starvation_warn_sec = _clamp_f(
        _get_float(environ, "STARVATION_WARN_SEC", 180.0), 5.0, 24 * 3600.0
    )
    no_buy_warn_sec = _clamp_f(_get_float(environ, "NO_BUY_WARN_SEC", 300.0), 5.0, 24 * 3600.0)

    main_idle_sleep = _clamp_f(_get_float(environ, "MAIN_IDLE_SLEEP_SEC", 0.5), 0.01, 10.0)
    shutdown_grace = _clamp_f(_get_float(environ, "SHUTDOWN_GRACE_SEC", 15.0), 1.0, 300.0)

    queue_drop = _get_str(environ, "QUEUE_DROP_POLICY", "drop_newest").strip().lower()
    if queue_drop not in _ALLOWED_QUEUE_DROP:
        _warn("[CONFIG] QUEUE_DROP_POLICY=%r invalid; using 'drop_newest'", queue_drop)
        queue_drop = "drop_newest"

    scanner_modules = _get_list(environ, "SCANNER_MODULES", [])
    main_restart_on_crash = _get_bool(environ, "MAIN_RESTART_ON_CRASH", False)
    main_crash_backoff = _clamp_f(_get_float(environ, "MAIN_CRASH_BACKOFF_SEC", 2.0), 0.1, 600.0)
    main_crash_backoff_max = _clamp_f(
        _get_float(environ, "MAIN_CRASH_BACKOFF_MAX_SEC", 60.0), 1.0, 3600.0
    )

    cand_dedup_enabled = _get_bool(environ, "CAND_DEDUP_ENABLED", True)
    cand_dedup_ttl = _clamp_f(_get_float(environ, "CAND_DEDUP_TTL_SEC", 10.0), 0.1, 3600.0)
    cand_dedup_max = _clamp_i(_get_int(environ, "CAND_DEDUP_MAX", 75_000), 1000, 10_000_000)

    tick_dedup_enabled = _get_bool(environ, "TICK_DEDUP_ENABLED", True)
    tick_dedup_ttl = _clamp_f(_get_float(environ, "TICK_DEDUP_TTL_SEC", 2.0), 0.05, 3600.0)
    tick_dedup_max = _clamp_i(_get_int(environ, "TICK_DEDUP_MAX", 200_000), 1000, 20_000_000)

    forward_ticks = _get_bool(environ, "FORWARD_TICKS_FROM_SCANNERS", False)
    main_fast_fail = _get_bool(environ, "MAIN_FAST_FAIL", False)
    main_validate_only = _get_bool(environ, "MAIN_VALIDATE_ONLY", False)
    main_threadify_sync = _get_bool(environ, "MAIN_THREADIFY_SYNC_MODULES", False)

    # ---- kill switches ----
    kill_switch = _get_bool(environ, "KILL_SWITCH", False)
    disable_buys = _get_bool(environ, "DISABLE_BUYS", False)
    disable_sells = _get_bool(environ, "DISABLE_SELLS", False)
    disable_scanners = _get_bool(environ, "DISABLE_SCANNERS", False)
    pause_trading = _get_bool(environ, "PAUSE_TRADING", False)

    kill_file = _normalize_pathlike(_get_str(environ, "KILL_SWITCH_FILE", ""))
    disable_buys_file = _normalize_pathlike(_get_str(environ, "DISABLE_BUYS_FILE", ""))
    disable_sells_file = _normalize_pathlike(_get_str(environ, "DISABLE_SELLS_FILE", ""))
    disable_scanners_file = _normalize_pathlike(_get_str(environ, "DISABLE_SCANNERS_FILE", ""))

    kill_watchdog_sec = _clamp_f(_get_float(environ, "KILL_WATCHDOG_SEC", 2.0), 0.1, 30.0)
    kill_shutdown_on_trigger = _get_bool(environ, "KILL_SHUTDOWN_ON_TRIGGER", False)

    # ---- rpc ----
    network = (
        _get_str(environ, "NETWORK", _get_str(environ, "SOLANA_CLUSTER", "mainnet")).strip()
        or "mainnet"
    )

    rpc_url = _normalize_url(
        _pick_first(
            environ, ["RPC_URL", "SOLANA_RPC_URL", "SOLANA_RPC", "SOL_RPC_URL"], default=None
        )
        or "https://api.mainnet-beta.solana.com"
    )
    rpc_urls = _get_list(environ, "RPC_URLS", [rpc_url])
    rpc_urls = _dedupe_preserve([_normalize_url(x) for x in rpc_urls if x]) or [rpc_url]

    websocket_url = _get_str(
        environ,
        "WEBSOCKET_URL",
        _get_str(environ, "RPC_WS_URL", _get_str(environ, "SOLANA_WS_URL", "")),
    ).strip()

    commitment = (
        _get_str(environ, "COMMITMENT", _get_str(environ, "RPC_COMMITMENT", "confirmed"))
        .strip()
        .lower()
    )
    if commitment not in _ALLOWED_COMMITMENT:
        _warn("[CONFIG] COMMITMENT=%r invalid; using 'confirmed'", commitment)
        commitment = "confirmed"

    tx_rpc_url = _normalize_url(_get_str(environ, "TX_RPC_URL", rpc_url).strip() or rpc_url)
    helius_rpc_url = _normalize_url(_get_str(environ, "HELIUS_RPC_URL", ""))

    enable_jito = _get_bool(environ, "ENABLE_JITO", _get_bool(environ, "JITO_ENABLED", False))
    jito_be = _get_str(environ, "JITO_BLOCK_ENGINE_URL", "")
    jito_tip = _clamp_i(_get_int(environ, "JITO_TIP_LAMPORTS", 50_000), 0, 10_000_000)
    jito_fee = _clamp_i(
        _get_int(environ, "JITO_FEE_MICROLAMPORTS", _get_int(environ, "JITO_FEE_MICROLamports", 0)),
        0,
        10_000_000,
    )

    sol_mint = _get_str(environ, "SOL_MINT", "So11111111111111111111111111111111111111112")
    usdc_mint = _get_str(environ, "USDC_MINT", "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v")

    # ---- wallet ----
    wallet_priv = (
        _pick_first(
            environ,
            ["WALLET_PRIVATE_KEY", "WALLET_SECRET_KEY", "SOLANA_WALLET_PRIVATE_KEY", "PRIVATE_KEY"],
            default="",
        )
        or ""
    )
    keypair_path = _normalize_pathlike(
        _get_str(environ, "KEYPAIR_PATH", _get_str(environ, "WALLET_KEYPAIR_PATH", ""))
    )
    wallet_pub = _get_str(environ, "WALLET_PUBKEY", _get_str(environ, "WALLET_PUBLIC_KEY", ""))
    savings_wallet = _get_str(environ, "SAVINGS_WALLET", "")
    owner = _get_str(environ, "OWNER", wallet_pub)
    owner_b58 = _get_str(environ, "OWNER_B58", owner)
    min_sol_balance = _clamp_f(_get_float(environ, "MIN_SOL_BALANCE", 0.20), 0.0, 10_000.0)

    # ---- external APIs ----
    coingecko = _get_str(
        environ,
        "COINGECKO_SOL_API",
        "https://api.coingecko.com/api/v3/simple/price?ids=solana&vs_currencies=usd",
    )
    ray_url = _normalize_url(_get_str(environ, "RAYDIUM_API_URL", "https://api.raydium.io/pairs"))
    ray_queries = _get_list(environ, "RAYDIUM_SEARCH_QUERIES", ["raydium", "solana"])

    dexs_tokens = _normalize_url(
        _get_str(environ, "DEXSCREENER_API_URL", "https://api.dexscreener.com/latest/dex/tokens")
    )
    dexs_base = _normalize_url(
        _get_str(environ, "DEXSCREENER_BASE_URL", "https://api.dexscreener.com")
    )

    pump_url = _normalize_url(
        _get_str(environ, "PUMPFUN_API_URL", "https://pump.fun/api/v1/tokens")
    )
    pump_base = _normalize_url(
        _get_str(environ, "PUMPFUN_API_BASE", _get_str(environ, "PUMPFUN_API_BASES", ""))
    )
    pump_timeout = _clamp_i(_get_int(environ, "PUMPFUN_HTTP_TIMEOUT_S", 15), 2, 120)
    pump_conn_limit = _clamp_i(_get_int(environ, "PUMPFUN_HTTP_CONN_LIMIT", 30), 1, 500)
    pump_api_limit = _clamp_i(_get_int(environ, "PUMPFUN_API_LIMIT", 20), 1, 500)
    pump_dom_limit = _clamp_i(_get_int(environ, "PUMPFUN_DOM_LIMIT", pump_api_limit), 1, 2000)
    pump_skip_next = _get_bool(environ, "PUMPFUN_SKIP_NEXT", False)
    pump_skip_htmlnext = _get_bool(environ, "PUMPFUN_SKIP_HTMLNEXT", False)

    birdeye_url = _normalize_url(
        _get_str(environ, "BIRDEYE_API_URL", "https://public-api.birdeye.so")
    )
    birdeye_key = _get_str(environ, "BIRDEYE_API_KEY", "")
    birdeye_enabled = _get_bool(environ, "BIRDEYE_ENABLED", bool(birdeye_key))

    # ---- Jupiter/router ----
    jup_base = _normalize_url(_get_str(environ, "JUP_BASE_URL", "https://api.jup.ag"))
    jup_alt = _get_list(environ, "JUP_ALT_BASE_URLS", [jup_base])
    jup_alt = _dedupe_preserve([_normalize_url(x) for x in jup_alt if x]) or (
        [jup_base] if jup_base else []
    )

    jup_quote_path = (
        _get_str(environ, "JUP_QUOTE_PATH", "/swap/v1/quote").strip() or "/swap/v1/quote"
    )
    jup_swap_path = _get_str(environ, "JUP_SWAP_PATH", "/swap/v1/swap").strip() or "/swap/v1/swap"

    jup_as_legacy = _get_bool(environ, "JUP_AS_LEGACY", _get_bool(environ, "AS_LEGACY", False))
    jup_shared = _get_bool(
        environ, "JUP_USE_SHARED_ACCOUNTS", _get_bool(environ, "USE_SHARED_ACCOUNTS", True)
    )
    jup_only_direct = _get_bool(
        environ, "JUP_ONLY_DIRECT", _get_bool(environ, "ONLY_DIRECT", False)
    )

    jup_key = _get_str(
        environ,
        "JUP_PRO_KEY",
        _get_str(environ, "JUP_API_KEY", _get_str(environ, "JUPITER_API_KEY", "")),
    )

    # LOCK-IN: correct prefixed env names (legacy fallbacks remain)
    jup_strict_auth = _get_bool(environ, "JUP_STRICT_AUTH", _get_bool(environ, "STRICT_AUTH", True))
    jup_allow_public_fallback = _get_bool(
        environ, "JUP_ALLOW_PUBLIC_FALLBACK", _get_bool(environ, "ALLOW_PUBLIC_FALLBACK", False)
    )

    # v6 base
    jup_v6_base = _normalize_url(
        _get_str(environ, "JUPITER_API_BASE", "https://quote-api.jup.ag/v6")
    )

    # Compute final endpoints (callable URLs)
    if jup_as_legacy:
        default_quote = _join_url(jup_base, jup_quote_path)
        default_swap = _join_url(jup_base, jup_swap_path)
    else:
        default_quote = _join_url(jup_v6_base, "/quote")
        default_swap = _join_url(jup_v6_base, "/swap")

    jup_quote_env = _get_str(environ, "JUPITER_QUOTE_API", "")
    jup_swap_env = _get_str(environ, "JUPITER_SWAP_API", "")

    if jup_as_legacy:
        jup_quote = _ensure_url_with_path(
            jup_quote_env, jup_base, jup_quote_path, "JUPITER_QUOTE_API"
        )
        jup_swap = _ensure_url_with_path(jup_swap_env, jup_base, jup_swap_path, "JUPITER_SWAP_API")
        if not jup_quote_env:
            jup_quote = default_quote
        if not jup_swap_env:
            jup_swap = default_swap
    else:
        jup_quote = _ensure_url_with_path(jup_quote_env, jup_v6_base, "/quote", "JUPITER_QUOTE_API")
        jup_swap = _ensure_url_with_path(jup_swap_env, jup_v6_base, "/swap", "JUPITER_SWAP_API")
        if not jup_quote_env:
            jup_quote = default_quote
        if not jup_swap_env:
            jup_swap = default_swap

    # Slippage
    slip_bps_from_env = _raw(environ, "JUPITER_SLIPPAGE_BPS")
    if slip_bps_from_env is not None:
        slip_bps = _clamp_i(_get_bps(environ, "JUPITER_SLIPPAGE_BPS", 50), 1, 5000)
        max_slip_frac = _clamp_f(slip_bps / 10_000.0, 0.0, 0.50)
    else:
        max_slip_frac = _clamp_f(_get_frac(environ, "JUPITER_MAX_SLIPPAGE", 0.005), 0.0, 0.50)
        slip_bps = int(round(max_slip_frac * 10_000))

    pf_slip_bps = _clamp_i(
        _get_bps(environ, "PF_SLIPPAGE_BPS", max(5, min(5000, slip_bps))), 1, 5000
    )
    wrap_unwrap = _get_bool(environ, "PF_WRAP_UNWRAP_SOL", True)
    cu_limit = _clamp_i(_get_int(environ, "PF_COMPUTE_UNIT_LIMIT", 0), 0, 3_000_000)
    debug_tx = _get_bool(environ, "PF_DEBUG_TX", False)

    # ---- alerts / integrations ----
    tg_token = _get_str(environ, "TELEGRAM_BOT_TOKEN", "")
    tg_chat = _get_str(environ, "TELEGRAM_CHAT_ID", "")
    discord_webhook = _get_str(environ, "DISCORD_WEBHOOK", "")

    enable_tg = _get_bool(environ, "ENABLE_TELEGRAM", bool(tg_token and tg_chat))
    enable_discord = _get_bool(environ, "ENABLE_DISCORD", bool(discord_webhook))

    notion_token = _get_str(environ, "NOTION_TOKEN", _get_str(environ, "NOTION_API_KEY", ""))
    notion_db = _get_str(environ, "NOTION_DATABASE_ID", _get_str(environ, "NOTION_DB_TRADES", ""))
    notion_enabled = _get_bool(environ, "NOTION_ENABLED", bool(notion_token and notion_db))
    enable_notion_logging = _get_bool(environ, "ENABLE_NOTION_LOGGING", notion_enabled)

    notion_title_prop = _get_str(environ, "NOTION_TITLE_PROP", "Token")
    notion_prop_token = _get_str(environ, "NOTION_PROP_TOKEN", "Mint")
    notion_prop_entry = _get_str(environ, "NOTION_PROP_ENTRY", "Entry Price")
    notion_prop_current = _get_str(environ, "NOTION_PROP_CURRENT", "Current Price")
    notion_prop_profit = _get_str(environ, "NOTION_PROP_PROFIT", "Profit")
    notion_prop_reason = _get_str(environ, "NOTION_PROP_REASON", "Reason")
    notion_prop_time = _get_str(environ, "NOTION_PROP_TIME", "Timestamp")

    openai_key = _get_str(environ, "OPENAI_API_KEY", "")
    openai_model = _get_str(environ, "OPENAI_MODEL", "gpt-4o-mini")
    openai_base_url = _get_str(environ, "OPENAI_BASE_URL", "")
    fine_tuned = _get_str(environ, "FINE_TUNED_MODEL", "")

    # Default AI to "on only if key is present", unless explicitly overridden
    ai_enabled = _get_bool(environ, "AI_ENABLED", bool(openai_key))

    # ---- storage (DSN routing lock-in) ----
    redis_url = (
        _pick_first(environ, ["REDIS_URL", "REDIS_TLS_URL"], default="redis://localhost:6379/0")
        or "redis://localhost:6379/0"
    )
    redis_prefix = _normalize_prefix(_get_str(environ, "REDIS_PREFIX", "sniper:"), ":")

    sqlite_path = (
        _normalize_pathlike(
            _get_str(environ, "SQLITE_DB_PATH", _get_str(environ, "SQLITE_DB", "data/state.db"))
        )
        or "data/state.db"
    )

    pos_backend = _get_str(environ, "POSITIONS_BACKEND", "json").strip().lower()
    if pos_backend not in _ALLOWED_POS_BACKENDS:
        _warn("[CONFIG] POSITIONS_BACKEND=%r invalid; using 'json'", pos_backend)
        pos_backend = "json"
    pos_json_path = (
        _normalize_pathlike(_get_str(environ, "POSITIONS_JSON_PATH", "data/positions.json"))
        or "data/positions.json"
    )

    selected_pg_dsn, pg_route, pg_host, pg_mode, render_like, pg_dsn, pg_dsn_i, pg_dsn_e, db_url = (
        _route_postgres_dsn(environ)
    )

    sniper_db_dsn = _resolve_dsn_with_fallback(environ, "SNIPER_DB_DSN", selected_pg_dsn)
    pnl_db_dsn = _resolve_dsn_with_fallback(environ, "PNL_DB_DSN", selected_pg_dsn)
    pnl_dsn = _resolve_dsn_with_fallback(environ, "PNL_DSN", selected_pg_dsn)
    pos_pg_dsn = _resolve_dsn_with_fallback(environ, "POSITIONS_PG_DSN", selected_pg_dsn)
    pos_pg_enabled = _get_bool(
        environ, "POSITIONS_PG_ENABLED", bool(pos_pg_dsn) or (pos_backend == "pg")
    )

    # ---- trading core ----
    auto_buy = _get_bool(
        environ, "AUTO_BUY_ENABLED", _get_bool(environ, "BUY_ENGINE_ENABLED", True)
    )
    auto_sell = _get_bool(
        environ,
        "AUTO_SELL_ENABLED",
        _get_bool(environ, "ENABLE_AUTO_SELL", _get_bool(environ, "EXIT_ENGINE_ENABLED", True)),
    )

    dry_run = _get_bool(environ, "DRY_RUN", _get_bool(environ, "DRYRUN", False))
    tx_timeout = _clamp_i(_get_int(environ, "TRANSACTION_TIMEOUT", 20), 5, 180)
    max_retries = _clamp_i(_get_int(environ, "MAX_RETRIES", 3), 0, 20)
    exec_delay = _clamp_i(
        _get_int(environ, "SNIPER_EXEC_DELAY", _get_int(environ, "EXEC_DELAY", 0)), 0, 30
    )

    # ---- scanners toggles ----
    enable_raydium = _get_bool(environ, "ENABLE_RAYDIUM_SCANNER", True)
    enable_pumpfun = _get_bool(
        environ, "ENABLE_PUMPFUN_SCANNER", _get_bool(environ, "ENABLE_PUMP_SCANNER", True)
    )
    enable_dexs = _get_bool(environ, "ENABLE_DEXSCREENER_SCANNER", True)

    enable_arbitrage = _get_bool(environ, "ENABLE_ARBITRAGE", False)
    dexes = _get_list(environ, "DEXES", ["jupiter", "raydium", "orca"])

    # ---- backtest ----
    backtest_enabled = _get_bool(environ, "BACKTEST_ENABLED", False)
    backtest_source = _get_str(environ, "BACKTEST_SOURCE", "pg").strip().lower()
    if backtest_source not in _ALLOWED_BACKTEST_SOURCE:
        _warn("[CONFIG] BACKTEST_SOURCE=%r invalid; using 'pg'", backtest_source)
        backtest_source = "pg"

    # ---- capability flags (LOCK-IN) ----
    scanning_possible = (
        (enable_raydium or enable_pumpfun or enable_dexs)
        and (not disable_scanners)
        and (not kill_switch)
        and (not main_validate_only)
    )

    buys_possible = (
        auto_buy
        and (not disable_buys)
        and (not kill_switch)
        and (not pause_trading)
        and (not dry_run)
    )

    sells_possible = (
        auto_sell
        and (not disable_sells)
        and (not kill_switch)
        and (not pause_trading)
        and (not dry_run)
    )

    live_trading = bool(buys_possible or sells_possible)

    wallet_required = live_trading
    redis_required = bool(live_trading or scanning_possible)
    rpc_required = bool(live_trading or scanning_possible)

    # DB requirement is driven by actual needs (not merely "PG_ENABLED"):
    db_required = bool(
        allow_schema_create
        or (pos_backend == "pg")
        or pos_pg_enabled
        or (backtest_enabled and backtest_source == "pg")
    )

    # PG_ENABLED defaults to true if DSN exists OR db is required.
    pg_enabled = _get_bool(environ, "PG_ENABLED", bool(selected_pg_dsn) or db_required)

    # ---- sizing / risk etc ----
    buy_sol_per_trade = _clamp_f(_get_float(environ, "BUY_SOL_PER_TRADE", 0.02), 0.0, 100.0)
    entry_min_buy = _clamp_f(_get_float(environ, "ENTRY_MIN_BUY_SOL", 0.015), 0.0, 100.0)
    entry_max_buy = _clamp_f(_get_float(environ, "ENTRY_MAX_BUY_SOL", 0.10), 0.0, 100.0)
    entry_min_initial = _clamp_f(
        _get_float(environ, "ENTRY_MIN_INITIAL_BUY_SOL", 0.0025), 0.0, 100.0
    )

    max_risk_per_trade = _clamp_f(_get_float(environ, "MAX_RISK_PER_TRADE", 0.05), 0.0, 1.0)
    min_volatility = _clamp_f(_get_float(environ, "MIN_VOLATILITY", 0.02), 0.0, 10.0)
    partial_chunks = _clamp_i(_get_int(environ, "PARTIAL_SELL_CHUNKS", 4), 1, 20)

    ticket_pct = _get_prob(environ, "TICKET_PCT_EQUITY", 0.04)
    ticket_usd_cap = _clamp_f(_get_float(environ, "TICKET_USD_CAP", 300.0), 0.0, 1_000_000.0)

    target_profit_pct = _clamp_f(
        _get_float(environ, "TARGET_PROFIT_PERCENT", 200.0), 0.0, 100_000.0
    )
    stop_loss_pct = _clamp_f(_get_float(environ, "STOP_LOSS_PERCENT", 20.0), 0.0, 99.9)
    tsl_enabled = _get_bool(environ, "TRAILING_STOP_ENABLED", True)
    max_hold_min = _clamp_i(_get_int(environ, "MAX_HOLD_TIME_MINUTES", 360), 1, 30_000)
    hold_if_score = _get_prob(environ, "HOLD_IF_SCORE_ABOVE", 0.85)

    tp1 = _clamp_f(_get_float(environ, "TAKE_PROFIT1_PCT", 30.0), 0.0, 100_000.0)
    tp2 = _clamp_f(_get_float(environ, "TAKE_PROFIT2_PCT", 60.0), 0.0, 100_000.0)
    tp3 = _clamp_f(_get_float(environ, "TAKE_PROFIT3_PCT", 120.0), 0.0, 100_000.0)

    reinvestor_enabled = _get_bool(environ, "REINVESTOR_ENABLED", True)
    reinvest_pct = _clamp_f(_get_float(environ, "REINVEST_PERCENT", 80.0), 0.0, 100.0)
    reinvest_cap = _clamp_f(_get_float(environ, "REINVESTMENT_PERCENT_CAP", 50.0), 0.0, 100.0)
    reinvest_vol_min = _clamp_f(
        _get_float(environ, "REINVESTMENT_VOLUME_MIN", 5_000.0), 0.0, 1_000_000_000.0
    )
    reinvest_min_profit_sol = _clamp_f(
        _get_float(environ, "REINVESTMENT_MIN_PROFIT_SOL", 0.01), 0.0, 1000.0
    )
    reinvest_hp_check = _get_bool(environ, "REINVESTMENT_HONEYPOT_CHECK", True)

    # ---- risk manager ----
    risk_profile = _get_str(environ, "RISK_PROFILE", "balanced").strip().lower()
    risk_mode_override = _raw(environ, "RISK_MODE_OVERRIDE")

    risk_max_trade = _get_prob(environ, "RISK_MAX_EQUITY_PCT_PER_TRADE", ticket_pct)
    risk_max_coin = _get_prob(environ, "RISK_MAX_EQUITY_PCT_PER_COIN", 0.10)
    risk_max_open = _clamp_i(_get_int(environ, "RISK_MAX_OPEN_POSITIONS", 6), 0, 500)
    risk_max_parallel = _clamp_i(_get_int(environ, "RISK_MAX_PARALLEL_BUYS", 3), 0, 100)

    risk_max_dd = _clamp_f(
        _get_float(
            environ,
            "RISK_MAX_DAILY_DRAWDOWN_PCT",
            _get_float(environ, "PF_DAILY_MAX_DRAWDOWN_PCT", 18.0),
        ),
        0.0,
        100.0,
    )
    risk_max_daily_loss_sol = _clamp_f(
        _get_float(environ, "RISK_MAX_DAILY_LOSS_SOL", 9999.0), 0.0, 1_000_000.0
    )
    risk_cb_min = _clamp_i(_get_int(environ, "RISK_CIRCUIT_BREAKER_MINUTES", 45), 1, 10_000)

    stop_after_profit = _clamp_f(
        _get_float(
            environ,
            "STOP_AFTER_DAILY_PROFIT_PCT",
            _get_float(environ, "PF_STOP_AFTER_PROFIT_PCT", 25.0),
        ),
        0.0,
        10_000.0,
    )
    daily_spend_cap = _clamp_f(_get_float(environ, "DAILY_SPEND_CAP_PCT", 35.0), 0.0, 100.0)

    risk_hard_kill = _get_bool(environ, "RISK_HARD_KILL_ON_DRAWNDOWN", True)
    risk_reentry = _get_bool(environ, "RISK_ALLOW_REENTRY_AFTER_BREAK", True)

    max_concurrent_pos = _get_int(environ, "MAX_CONCURRENT_POS", risk_max_open)
    max_concurrent_buys = _get_int(environ, "MAX_CONCURRENT_BUYS", risk_max_parallel)
    max_concurrency = _get_int(environ, "MAX_CONCURRENCY", max_concurrent_buys)

    # ---- exit engine ----
    exit_sl = _clamp_f(_get_float(environ, "EXIT_STOP_LOSS_PCT", stop_loss_pct), 0.0, 99.9)
    exit_tp1 = _clamp_f(_get_float(environ, "EXIT_TP1_PCT", tp1), 0.0, 100_000.0)
    exit_tp2 = _clamp_f(_get_float(environ, "EXIT_TP2_PCT", tp2), 0.0, 100_000.0)
    exit_tp3 = _clamp_f(_get_float(environ, "EXIT_TP3_PCT", tp3), 0.0, 100_000.0)

    exit_tsl_enabled = _get_bool(environ, "EXIT_TSL_ENABLED", True)
    exit_tsl_arm = _clamp_f(_get_float(environ, "EXIT_TSL_ARM_PCT", 12.0), 0.0, 100_000.0)
    exit_tsl_step = _clamp_f(_get_float(environ, "EXIT_TSL_STEP_PCT", 4.5), 0.1, 100_000.0)

    pf_tsl_arm = _clamp_f(_get_float(environ, "PF_TSL_ARM_PCT", exit_tsl_arm), 0.0, 100_000.0)
    pf_tsl_pct = _clamp_f(_get_float(environ, "PF_TSL_PCT", exit_tsl_step), 0.1, 100_000.0)
    pf_sl_pct = _clamp_f(_get_float(environ, "PF_SL_PCT", exit_sl), 0.0, 99.9)

    exit_stag_max = _clamp_i(_get_int(environ, "EXIT_STAGNANT_MAX_MIN", 10), 1, 10_000)
    exit_stag_min_move = _clamp_f(
        _get_float(environ, "EXIT_STAGNANT_MIN_MOVE_PCT", 5.0), 0.0, 10_000.0
    )
    exit_dead_max = _clamp_i(_get_int(environ, "EXIT_DEAD_COIN_MAX_MIN", 45), 1, 10_000)
    exit_dead_min_liq = _clamp_f(
        _get_float(environ, "EXIT_DEAD_MIN_LIQ_USD", 400.0), 0.0, 1_000_000_000.0
    )
    exit_force_x = _clamp_f(_get_float(environ, "EXIT_FORCE_AFTER_X_MULT", 8.0), 1.0, 10_000.0)
    exit_lock_after = _clamp_f(
        _get_float(environ, "EXIT_LOCK_PROFIT_AFTER_MULT", 3.0), 1.0, 10_000.0
    )
    exit_lock_step = _clamp_f(
        _get_float(environ, "EXIT_LOCK_PROFIT_TSL_STEP_PCT", 3.5), 0.1, 100_000.0
    )

    auto_exit_worker_enabled = _get_bool(environ, "AUTO_EXIT_WORKER_ENABLED", True)

    # ---- entry momentum ----
    entry_momo_enabled = _get_bool(
        environ, "ENTRY_MOMENTUM_ENABLED", _get_bool(environ, "AI_MOMO_FILTER", True)
    )
    entry_momo_window = _clamp_i(
        _get_int(environ, "ENTRY_MOMO_WINDOW_SEC", _get_int(environ, "MOMO_WINDOW_SEC", 25)), 1, 600
    )
    entry_momo_min_trades = _clamp_i(
        _get_int(environ, "ENTRY_MOMO_MIN_TRADES", _get_int(environ, "MOMO_MIN_TRADES", 2)),
        0,
        10_000,
    )
    entry_momo_min_up = _clamp_f(
        _get_float(environ, "ENTRY_MOMO_MIN_UP_PCT", _get_float(environ, "MOMO_MIN_UP_PCT", 2.0)),
        -100.0,
        10_000.0,
    )
    entry_momo_allow_nodata_pump = _get_bool(
        environ,
        "ENTRY_MOMO_ALLOW_NO_DATA_FOR_PUMP",
        _get_bool(environ, "MOMO_ALLOW_NO_DATA_FOR_PUMP", True),
    )

    # ---- scanner loop / scanners ----
    scan_interval = _clamp_i(_get_int(environ, "SCAN_INTERVAL", 60), 1, 3600)
    scanner_period = _clamp_i(_get_int(environ, "SCANNER_PERIOD_SEC", 4), 1, 60)
    buy_cooldown = _clamp_i(_get_int(environ, "BUY_COOLDOWN_SEC", 3), 0, 600)

    pf_max_concurrent = _clamp_i(
        _get_int(environ, "PF_MAX_CONCURRENT", max_concurrent_buys), 0, 1000
    )
    prewarm_top_n = _clamp_i(_get_int(environ, "PREWARM_TOP_N", 5), 0, 1000)

    scanner_min_liq = _clamp_f(
        _get_float(environ, "SCANNER_MIN_LIQ_USD", 150.0), 0.0, 1_000_000_000.0
    )
    min_liq = _clamp_f(_get_float(environ, "MIN_LIQ_USD", scanner_min_liq), 0.0, 1_000_000_000.0)
    min_pool_age = _clamp_i(_get_int(environ, "MIN_POOL_AGE_MIN", 1), 0, 10_000)

    noroute_cd = _clamp_i(_get_int(environ, "NOROUTE_COOLDOWN_SEC", 600), 0, 86_400)

    blocklist_mints = _get_list(environ, "BLOCKLIST_MINTS", [])
    allowlist_mints = _get_list(environ, "ALLOWLIST_MINTS", [])
    strategy_preset = _get_str(environ, "STRATEGY_PRESET", "max-pnl").strip().lower()

    tg_heartbeats = _get_bool(environ, "TELEGRAM_HEARTBEATS", True)
    hb_every = _clamp_i(_get_int(environ, "HEARTBEAT_EVERY_SEC", 300), 10, 86_400)
    hb_only_on_change = _get_bool(environ, "HEARTBEAT_ONLY_ON_CHANGE", True)
    tg_route_blocks = _get_bool(environ, "TELEGRAM_NOTIFY_ROUTE_BLOCKS", True)
    pnl_tg_hb = _get_bool(environ, "PNL_TELEGRAM_HEARTBEAT", True)

    moonshot_enabled = _get_bool(environ, "MOONSHOT_ENABLED", True)
    moonshot_daily_usd = _clamp_f(
        _get_float(environ, "MOONSHOT_DAILY_USD", 100.0), 0.0, 1_000_000.0
    )
    moonshot_max_trade_usd = _clamp_f(
        _get_float(environ, "MOONSHOT_MAX_TRADE_USD", 25.0), 0.0, 1_000_000.0
    )
    moonshot_age_cutoff = _clamp_i(_get_int(environ, "MOONSHOT_AGE_CUTOFF_MIN", 30), 0, 10_000)
    moonshot_liq_cutoff = _clamp_f(
        _get_float(environ, "MOONSHOT_LIQ_CUTOFF_USD", 20_000.0), 0.0, 1_000_000_000.0
    )
    moonshot_impact_cutoff = _clamp_i(
        _get_int(environ, "MOONSHOT_IMPACT_CUTOFF_BPS", 150), 0, 50_000
    )

    scanner_min_fdv = _clamp_f(
        _get_float(environ, "SCANNER_MIN_FDV_USD", 0.0), 0.0, 1_000_000_000_000.0
    )
    scanner_min_fraud = _clamp_f(_get_float(environ, "SCANNER_MIN_FRAUD_SCORE", 0.0), 0.0, 1.0)
    scanner_alerts_enabled = _get_bool(environ, "SCANNER_ALERTS_ENABLED", True)

    scanner_min_buys_5m = _clamp_i(_get_int(environ, "SCANNER_MIN_BUYS_5M", 0), 0, 1_000_000)
    scanner_min_buys_15m = _clamp_i(_get_int(environ, "SCANNER_MIN_BUYS_15M", 0), 0, 1_000_000)
    scanner_min_vol_5m = _clamp_f(
        _get_float(environ, "SCANNER_MIN_VOLUME_5M_USD", 20.0), 0.0, 1_000_000_000.0
    )
    scanner_min_vol_15m = _clamp_f(
        _get_float(environ, "SCANNER_MIN_VOLUME_15M_USD", 60.0), 0.0, 1_000_000_000.0
    )

    scanner_req_buy_pressure = _get_bool(environ, "SCANNER_REQUIRE_BUY_PRESSURE", False)
    scanner_min_buy_ratio = _clamp_f(_get_float(environ, "SCANNER_MIN_BUY_RATIO", 0.0), 0.0, 1000.0)

    scanner_min_chg_5m = _clamp_f(
        _get_float(environ, "SCANNER_MIN_PRICE_CHANGE_5M", 0.0), -100.0, 100_000.0
    )
    scanner_min_chg_15m = _clamp_f(
        _get_float(environ, "SCANNER_MIN_PRICE_CHANGE_15M", 0.0), -100.0, 100_000.0
    )

    scanner_max_age_min = _clamp_i(_get_int(environ, "SCANNER_MAX_AGE_MIN", 720), 0, 1_000_000)
    scanner_max_mc = _clamp_f(
        _get_float(environ, "SCANNER_MAX_MC_USD", 50_000_000.0), 0.0, 10_000_000_000_000.0
    )
    scanner_min_holders = _clamp_i(_get_int(environ, "SCANNER_MIN_HOLDERS", 0), 0, 1_000_000_000)

    alert_cd = _clamp_i(_get_int(environ, "ALERT_COOLDOWN_SECONDS", 900), 0, 86_400)
    pos_monitor_iv = _clamp_i(_get_int(environ, "POSITION_MONITOR_INTERVAL", 20), 1, 3600)
    reinvest_check_iv = _clamp_i(_get_int(environ, "REINVESTMENT_CHECK_INTERVAL", 60), 1, 3600)

    # ---- pair filters ----
    pf_scan_min_liq = _clamp_f(
        _get_float(environ, "PF_SCAN_MIN_LIQ_USD", 400.0), 0.0, 1_000_000_000.0
    )
    pf_scan_max_liq = _clamp_f(
        _get_float(environ, "PF_SCAN_MAX_LIQ_USD", 300_000.0), 0.0, 1_000_000_000.0
    )

    pf_scan_min_mcap = _clamp_f(
        _get_float(environ, "PF_SCAN_MIN_MCAP_USD", 800.0), 0.0, 10_000_000_000_000.0
    )
    pf_scan_max_mcap = _clamp_f(
        _get_float(environ, "PF_SCAN_MAX_MCAP_USD", 1_500_000.0), 0.0, 10_000_000_000_000.0
    )

    pf_scan_max_age = _clamp_f(
        _get_float(environ, "PF_SCAN_MAX_TOKEN_AGE_MIN", 180.0), 0.0, 10_000_000.0
    )
    pf_scan_min_vol = _clamp_f(
        _get_float(environ, "PF_SCAN_MIN_5M_VOL_USD", 50.0), 0.0, 1_000_000_000.0
    )
    pf_scan_min_buys = _clamp_i(_get_int(environ, "PF_SCAN_MIN_5M_BUYS", 1), 0, 1_000_000)
    pf_scan_min_ratio = _clamp_f(
        _get_float(environ, "PF_SCAN_MIN_BUY_TO_SELL_RATIO", 1.02), 0.0, 1000.0
    )

    pf_min_whale_size_sol = _clamp_f(
        _get_float(environ, "PF_SCAN_MIN_WHALE_SIZE_SOL", 1.5), 0.0, 1_000_000.0
    )
    pf_min_whale_count = _clamp_i(_get_int(environ, "PF_SCAN_MIN_WHALE_COUNT_5M", 0), 0, 1_000_000)

    pf_max_rug = _clamp_f(_get_float(environ, "PF_MAX_RUG_RISK_SCORE", 0.80), 0.0, 1.0)
    pf_min_momo = _clamp_f(_get_float(environ, "PF_MIN_MOMENTUM_SCORE", 0.14), 0.0, 1.0)
    pf_min_final = _clamp_f(_get_float(environ, "PF_MIN_FINAL_SCORE", 0.10), 0.0, 1.0)
    pf_risk_penalty_w = _clamp_f(_get_float(environ, "PF_RISK_PENALTY_WEIGHT", 0.6), 0.0, 10.0)

    # ---- AI models ----
    ai_entry_enabled = _get_bool(
        environ, "AI_ENTRY_ENABLED", _get_bool(environ, "AIGATE_AI_ENABLED", True)
    )
    ai_entry_model = _get_str(
        environ, "AI_ENTRY_MODEL", _get_str(environ, "ENTRY_MODEL", fine_tuned or openai_model)
    )
    ai_entry_score_min = _get_prob(
        environ, "AI_ENTRY_SCORE_MIN", _get_prob(environ, "ENTRY_MIN_SCORE", 0.60)
    )
    ai_entry_score_hard_min = _get_prob(environ, "AI_ENTRY_SCORE_HARD_MIN", 0.50)
    ai_entry_risk_adj_min = _get_prob(environ, "AI_ENTRY_RISK_ADJ_MIN", 0.50)

    ai_entry_threshold = _get_prob(environ, "AI_ENTRY_THRESHOLD", ai_entry_score_min)
    ai_score_gate = _get_prob(environ, "AI_SCORE_GATE", ai_entry_threshold)
    ai_score_soft = _get_prob(environ, "AI_SCORE_SOFT_FLOOR", ai_entry_score_hard_min)
    ai_score_hard = _get_prob(environ, "AI_SCORE_HARD_FLOOR", 0.38)
    ai_entry_max_conc = _clamp_i(_get_int(environ, "AI_ENTRY_MAX_CONCURRENCY", 8), 1, 256)

    ai_pre_score_enabled = _get_bool(environ, "AI_PRE_SCORE_ENABLED", True)
    ai_pre_score_min = _get_prob(environ, "AI_PRE_SCORE_MIN", 0.10)
    entry_require_risk_capacity = _get_bool(environ, "ENTRY_REQUIRE_RISK_CAPACITY", True)
    entry_risk_capacity_min = _get_prob(environ, "ENTRY_RISK_CAPACITY_MIN", 0.30)

    ai_rug_enabled = _get_bool(environ, "AI_RUG_ENABLED", True)
    ai_rug_model = _get_str(environ, "AI_RUG_MODEL", fine_tuned or openai_model)
    ai_rug_max = _clamp_f(_get_float(environ, "AI_RUG_MAX_SCORE", pf_max_rug), 0.0, 1.0)
    ai_rug_min_conf = _get_prob(environ, "AI_RUG_MIN_CONFIDENCE", 0.55)

    aigate_max_impact = _clamp_i(_get_int(environ, "AIGATE_MAX_PRICE_IMPACT_BPS", 550), 0, 50_000)
    ai_bad = _get_prob(environ, "AI_BAD_THRESHOLD", 0.35)
    ai_good = _get_prob(environ, "AI_GOOD_THRESHOLD", 0.70)

    ai_whale_echo = _get_bool(environ, "AI_WHALE_ECHO", True)
    ai_whale_echo_min_usd = _clamp_f(
        _get_float(environ, "AI_WHALE_ECHO_MIN_SIZE_USD", 300.0), 0.0, 1_000_000_000.0
    )

    ai_adaptive_sell = _get_bool(environ, "AI_ADAPTIVE_SELL", True)
    ai_sell_enabled = _get_bool(environ, "AI_SELL_ENABLED", True)
    ai_sell_model = _get_str(environ, "AI_SELL_MODEL", fine_tuned or openai_model)
    ai_sell_hold = _get_prob(environ, "AI_SELL_HOLD_THRESHOLD", 0.60)
    ai_sell_trim = _get_prob(environ, "AI_SELL_TRIM_THRESHOLD", 0.72)
    ai_sell_exit = _get_prob(environ, "AI_SELL_EXIT_THRESHOLD", 0.84)

    strategy_redis_prefix = _get_str(environ, "STRATEGY_REDIS_PREFIX", "strategy:v1").strip()
    strategy_update_interval = _clamp_i(
        _get_int(environ, "STRATEGY_UPDATE_INTERVAL", 1800), 30, 86_400
    )

    ai_feature_version = _get_str(environ, "AI_FEATURE_VERSION", "v3+++")
    ai_buy_feature_version = _get_str(environ, "AI_BUY_FEATURE_VERSION", ai_feature_version)

    # ---- regime ----
    regime_enabled = _get_bool(environ, "REGIME_ENABLED", True)
    regime_update_iv = _clamp_i(_get_int(environ, "REGIME_UPDATE_INTERVAL_SEC", 60), 5, 3600)
    regime_sol_min_vol = _clamp_f(
        _get_float(environ, "REGIME_SOL_MIN_24H_VOL_USD", 200_000_000.0), 0.0, 1e12
    )
    regime_meme_min_vol = _clamp_f(
        _get_float(environ, "REGIME_MEME_MIN_24H_VOL_USD", 50_000_000.0), 0.0, 1e12
    )
    regime_max_rug_rate = _clamp_f(
        _get_float(environ, "REGIME_MAX_ACCEPTABLE_RUG_RATE", 0.25), 0.0, 1.0
    )

    # ---- logging/training ----
    log_entry_candidates = _get_bool(environ, "LOG_ENTRY_CANDIDATES", True)
    log_trades = _get_bool(environ, "LOG_TRADES", True)
    log_regime_snapshots = _get_bool(environ, "LOG_REGIME_SNAPSHOTS", True)
    train_label_window = _clamp_i(_get_int(environ, "TRAIN_LABEL_WINDOW_MIN", 60), 1, 10_000)
    train_min_hold = _clamp_i(_get_int(environ, "TRAIN_MIN_HOLD_MIN", 10), 0, 10_000)
    train_max_hold = _clamp_i(_get_int(environ, "TRAIN_MAX_HOLD_MIN", 720), 1, 100_000)

    # ---- whales ----
    smart_wallets_enabled = _get_bool(environ, "SMART_WALLETS_ENABLED", True)
    enable_whale_filters = _get_bool(environ, "ENABLE_WHALE_FILTERS", True)
    dynamic_whale_discovery = _get_bool(environ, "DYNAMIC_WHALE_DISCOVERY", True)
    curated_whales = _get_list(environ, "CURATED_WHALE_WALLETS", [])
    whale_json = _get_json(environ, "WHALE_WALLETS_JSON", {})

    whale_wallets: dict[str, str] = {}
    if isinstance(whale_json, dict) and whale_json:
        whale_wallets.update({str(k): str(v) for k, v in whale_json.items() if str(v).strip()})
    for idx, addr in enumerate(curated_whales):
        if addr.strip():
            whale_wallets.setdefault(f"CURATED_{idx + 1}", addr.strip())

    enable_social_scraping = _get_bool(environ, "ENABLE_SOCIAL_SCRAPING", False)
    x_bearer = _get_str(environ, "X_BEARER_TOKEN", "")
    tg_scrape_bot = _get_str(environ, "TELEGRAM_SCRAPE_BOT", "")
    discord_search_channel = _get_str(environ, "DISCORD_SEARCH_CHANNEL_ID", "")

    # ---- daytrader ----
    enable_daytrader = _get_bool(environ, "ENABLE_DAYTRADER", False)
    daytrader_symbols = _get_list(environ, "DAYTRADER_SYMBOLS", ["SOL/USDC"])
    daytrader_timeframes = _get_list(environ, "DAYTRADER_TIMEFRAMES", ["1m", "5m", "15m", "1h"])
    daytrader_exchanges = _get_list(environ, "DAYTRADER_EXCHANGES", ["binance", "bybit", "okx"])
    dt_feature_set = _get_list(
        environ, "DT_FEATURE_SET", ["rsi", "macd", "bbands", "atr", "orderflow"]
    )
    dt_model_kind = _get_str(environ, "DT_MODEL_KIND", "xgboost")
    dt_prob_threshold = _get_prob(environ, "DT_PROB_THRESHOLD", 0.62)
    dt_max_concurrent = _clamp_i(_get_int(environ, "DT_MAX_CONCURRENT", 3), 0, 200)
    dt_max_leverage = _clamp_f(_get_float(environ, "DT_MAX_LEVERAGE", 1.0), 0.0, 100.0)
    dt_max_risk_per_trade = _get_prob(environ, "DT_MAX_RISK_PER_TRADE", 0.02)
    dt_tp_bps = _clamp_i(_get_int(environ, "DT_TAKE_PROFIT_BPS", 120), 0, 50_000)
    dt_sl_bps = _clamp_i(_get_int(environ, "DT_STOP_LOSS_BPS", 60), 0, 50_000)
    dt_trailing_bps = _clamp_i(_get_int(environ, "DT_TRAILING_BPS", 50), 0, 50_000)
    dt_cooldown_sec = _clamp_i(_get_int(environ, "DT_COOLDOWN_SEC", 60), 0, 86_400)
    dt_paper = _get_bool(environ, "DT_PAPER_TRADING", True)

    # ---- backtest rest ----
    backtest_max_workers = _clamp_i(_get_int(environ, "BACKTEST_MAX_WORKERS", 4), 1, 256)
    diag_days_back = _clamp_i(_get_int(environ, "DIAG_BT_DAYS_BACK", 5), 0, 3650)
    diag_limit_cands = _clamp_i(_get_int(environ, "DIAG_BT_LIMIT_CANDIDATES", 200), 1, 10_000_000)
    bt_days_back = _get_str(environ, "BT_DAYS_BACK", "")
    bt_limit_candidates = _get_str(environ, "BT_LIMIT_CANDIDATES", "")

    # ---- legacy / compat ----
    solana_endpoint = rpc_url
    solana_rpc_url = rpc_url
    sol_rpc_url = rpc_url
    dryrun = dry_run
    enable_pump_scanner = enable_pumpfun
    liq_min_usd = min_liq

    ray_pairs_api = ray_url
    dexs_tokens_api = dexs_tokens
    pump_tokens_api = pump_url

    target_market = _get_str(
        environ, "TARGET_MARKET", (daytrader_symbols[0] if daytrader_symbols else "SOL/USDC")
    ).strip()

    return Config(
        APP_NAME=app_name,
        ENV=env_name,
        LOG_LEVEL=log_level,
        SAFE_MODE=safe_mode,
        STRICT_CONFIG=strict,
        IS_PROD=is_prod,
        ENABLE_METRICS=enable_metrics,
        METRICS_PORT=metrics_port,
        SENTRY_DSN=sentry_dsn,
        SENTRY_ENV=sentry_env,
        GIT_COMMIT=git_commit,
        RUN_ID=run_id,
        MASTER_PHASE=master_phase,
        STRATEGY_PHASE=strategy_phase,
        USE_LEGACY_MAIN=use_legacy_main,
        ENTRY_WORKERS=entry_workers,
        CANDIDATE_QUEUE_MAX=cand_q_max,
        TICK_QUEUE_MAX=tick_q_max,
        SUPERVISOR_SEC=supervisor_sec,
        RESTART_BACKOFF_SEC=restart_backoff,
        RESTART_MAX_BACKOFF_SEC=restart_max_backoff,
        RESTART_MAX_PER_HOUR=restart_max_per_hour,
        HEALTH_LOG_SEC=health_log_sec,
        STARVATION_WARN_SEC=starvation_warn_sec,
        NO_BUY_WARN_SEC=no_buy_warn_sec,
        MAIN_IDLE_SLEEP_SEC=main_idle_sleep,
        SHUTDOWN_GRACE_SEC=shutdown_grace,
        QUEUE_DROP_POLICY=queue_drop,
        SCANNER_MODULES=scanner_modules,
        MAIN_RESTART_ON_CRASH=main_restart_on_crash,
        MAIN_CRASH_BACKOFF_SEC=main_crash_backoff,
        MAIN_CRASH_BACKOFF_MAX_SEC=main_crash_backoff_max,
        CAND_DEDUP_ENABLED=cand_dedup_enabled,
        CAND_DEDUP_TTL_SEC=cand_dedup_ttl,
        CAND_DEDUP_MAX=cand_dedup_max,
        TICK_DEDUP_ENABLED=tick_dedup_enabled,
        TICK_DEDUP_TTL_SEC=tick_dedup_ttl,
        TICK_DEDUP_MAX=tick_dedup_max,
        FORWARD_TICKS_FROM_SCANNERS=forward_ticks,
        MAIN_FAST_FAIL=main_fast_fail,
        MAIN_VALIDATE_ONLY=main_validate_only,
        MAIN_THREADIFY_SYNC_MODULES=main_threadify_sync,
        ALLOW_SCHEMA_CREATE=allow_schema_create,
        SKIP_DSN_REQUIRED=skip_dsn_required,
        KILL_SWITCH=kill_switch,
        DISABLE_BUYS=disable_buys,
        DISABLE_SELLS=disable_sells,
        DISABLE_SCANNERS=disable_scanners,
        PAUSE_TRADING=pause_trading,
        KILL_SWITCH_FILE=kill_file,
        DISABLE_BUYS_FILE=disable_buys_file,
        DISABLE_SELLS_FILE=disable_sells_file,
        DISABLE_SCANNERS_FILE=disable_scanners_file,
        KILL_WATCHDOG_SEC=kill_watchdog_sec,
        KILL_SHUTDOWN_ON_TRIGGER=kill_shutdown_on_trigger,
        NETWORK=network,
        RPC_URL=rpc_url,
        RPC_URLS=rpc_urls,
        WEBSOCKET_URL=websocket_url,
        COMMITMENT=commitment,
        TX_RPC_URL=tx_rpc_url,
        HELIUS_RPC_URL=helius_rpc_url,
        ENABLE_JITO=enable_jito,
        JITO_BLOCK_ENGINE_URL=jito_be,
        JITO_TIP_LAMPORTS=jito_tip,
        JITO_FEE_MICROLAMPORTS=jito_fee,
        SOL_MINT=sol_mint,
        USDC_MINT=usdc_mint,
        WALLET_PRIVATE_KEY=wallet_priv,
        KEYPAIR_PATH=keypair_path,
        WALLET_PUBKEY=wallet_pub,
        SAVINGS_WALLET=savings_wallet,
        OWNER=owner,
        OWNER_B58=owner_b58,
        MIN_SOL_BALANCE=min_sol_balance,
        COINGECKO_SOL_API=coingecko,
        RAYDIUM_API_URL=ray_url,
        RAYDIUM_SEARCH_QUERIES=ray_queries,
        DEXSCREENER_API_URL=dexs_tokens,
        DEXSCREENER_BASE_URL=dexs_base,
        PUMPFUN_API_URL=pump_url,
        PUMPFUN_API_BASE=pump_base,
        PUMPFUN_HTTP_TIMEOUT_S=pump_timeout,
        PUMPFUN_HTTP_CONN_LIMIT=pump_conn_limit,
        PUMPFUN_API_LIMIT=pump_api_limit,
        PUMPFUN_DOM_LIMIT=pump_dom_limit,
        PUMPFUN_SKIP_NEXT=pump_skip_next,
        PUMPFUN_SKIP_HTMLNEXT=pump_skip_htmlnext,
        BIRDEYE_API_URL=birdeye_url,
        BIRDEYE_API_KEY=birdeye_key,
        BIRDEYE_ENABLED=birdeye_enabled,
        JUP_BASE_URL=jup_base,
        JUP_ALT_BASE_URLS=jup_alt,
        JUP_QUOTE_PATH=jup_quote_path,
        JUP_SWAP_PATH=jup_swap_path,
        JUPITER_API_BASE=jup_v6_base,
        JUP_AS_LEGACY=jup_as_legacy,
        JUP_USE_SHARED_ACCOUNTS=jup_shared,
        JUP_ONLY_DIRECT=jup_only_direct,
        JUP_PRO_KEY=jup_key,
        JUP_STRICT_AUTH=jup_strict_auth,
        JUP_ALLOW_PUBLIC_FALLBACK=jup_allow_public_fallback,
        JUPITER_QUOTE_API=jup_quote,
        JUPITER_SWAP_API=jup_swap,
        JUPITER_MAX_SLIPPAGE=max_slip_frac,
        JUPITER_SLIPPAGE_BPS=slip_bps,
        PF_SLIPPAGE_BPS=pf_slip_bps,
        PF_WRAP_UNWRAP_SOL=wrap_unwrap,
        PF_COMPUTE_UNIT_LIMIT=cu_limit,
        PF_DEBUG_TX=debug_tx,
        TELEGRAM_BOT_TOKEN=tg_token,
        TELEGRAM_CHAT_ID=tg_chat,
        DISCORD_WEBHOOK=discord_webhook,
        ENABLE_TELEGRAM=enable_tg,
        ENABLE_DISCORD=enable_discord,
        NOTION_TOKEN=notion_token,
        NOTION_DATABASE_ID=notion_db,
        NOTION_ENABLED=notion_enabled,
        ENABLE_NOTION_LOGGING=enable_notion_logging,
        NOTION_TITLE_PROP=notion_title_prop,
        NOTION_PROP_TOKEN=notion_prop_token,
        NOTION_PROP_ENTRY=notion_prop_entry,
        NOTION_PROP_CURRENT=notion_prop_current,
        NOTION_PROP_PROFIT=notion_prop_profit,
        NOTION_PROP_REASON=notion_prop_reason,
        NOTION_PROP_TIME=notion_prop_time,
        OPENAI_API_KEY=openai_key,
        OPENAI_MODEL=openai_model,
        OPENAI_BASE_URL=openai_base_url,
        FINE_TUNED_MODEL=fine_tuned,
        REDIS_URL=redis_url,
        REDIS_PREFIX=redis_prefix,
        SQLITE_DB_PATH=sqlite_path,
        POSITIONS_BACKEND=pos_backend,
        POSITIONS_JSON_PATH=pos_json_path,
        PG_DSN_MODE=pg_mode,
        PG_DSN_ROUTE=pg_route,
        PG_DSN_HOST=pg_host,
        RENDER_LIKE=render_like,
        PG_ENABLED=pg_enabled,
        PG_DSN=pg_dsn,
        PG_DSN_INTERNAL=pg_dsn_i,
        PG_DSN_EXTERNAL=pg_dsn_e,
        POSTGRES_DSN=selected_pg_dsn,
        SNIPER_DB_DSN=sniper_db_dsn,
        PNL_DB_DSN=pnl_db_dsn,
        PNL_DSN=pnl_dsn,
        POSITIONS_PG_DSN=pos_pg_dsn,
        POSITIONS_PG_ENABLED=pos_pg_enabled,
        AUTO_BUY_ENABLED=auto_buy,
        AUTO_SELL_ENABLED=auto_sell,
        DRY_RUN=dry_run,
        TRANSACTION_TIMEOUT=tx_timeout,
        MAX_RETRIES=max_retries,
        EXEC_DELAY=exec_delay,
        buys_possible=buys_possible,
        sells_possible=sells_possible,
        live_trading=live_trading,
        scanning_possible=scanning_possible,
        wallet_required=wallet_required,
        redis_required=redis_required,
        rpc_required=rpc_required,
        db_required=db_required,
        BUYS_POSSIBLE=buys_possible,
        SELLS_POSSIBLE=sells_possible,
        LIVE_TRADING=live_trading,
        SCANNING_POSSIBLE=scanning_possible,
        WALLET_REQUIRED=wallet_required,
        REDIS_REQUIRED=redis_required,
        RPC_REQUIRED=rpc_required,
        DB_REQUIRED=db_required,
        BUY_SOL_PER_TRADE=buy_sol_per_trade,
        ENTRY_MIN_BUY_SOL=entry_min_buy,
        ENTRY_MAX_BUY_SOL=entry_max_buy,
        ENTRY_MIN_INITIAL_BUY_SOL=entry_min_initial,
        MAX_RISK_PER_TRADE=max_risk_per_trade,
        MIN_VOLATILITY=min_volatility,
        PARTIAL_SELL_CHUNKS=partial_chunks,
        TICKET_PCT_EQUITY=ticket_pct,
        TICKET_USD_CAP=ticket_usd_cap,
        TARGET_PROFIT_PERCENT=target_profit_pct,
        STOP_LOSS_PERCENT=stop_loss_pct,
        TRAILING_STOP_ENABLED=tsl_enabled,
        MAX_HOLD_TIME_MINUTES=max_hold_min,
        HOLD_IF_SCORE_ABOVE=hold_if_score,
        TAKE_PROFIT1_PCT=tp1,
        TAKE_PROFIT2_PCT=tp2,
        TAKE_PROFIT3_PCT=tp3,
        REINVESTOR_ENABLED=reinvestor_enabled,
        REINVEST_PERCENT=reinvest_pct,
        REINVESTMENT_PERCENT_CAP=reinvest_cap,
        REINVESTMENT_VOLUME_MIN=reinvest_vol_min,
        REINVESTMENT_MIN_PROFIT_SOL=reinvest_min_profit_sol,
        REINVESTMENT_HONEYPOT_CHECK=reinvest_hp_check,
        RISK_PROFILE=risk_profile,
        RISK_MODE_OVERRIDE=risk_mode_override,
        RISK_MAX_EQUITY_PCT_PER_TRADE=risk_max_trade,
        RISK_MAX_EQUITY_PCT_PER_COIN=risk_max_coin,
        RISK_MAX_OPEN_POSITIONS=risk_max_open,
        RISK_MAX_PARALLEL_BUYS=risk_max_parallel,
        RISK_MAX_DAILY_DRAWDOWN_PCT=risk_max_dd,
        RISK_MAX_DAILY_LOSS_SOL=risk_max_daily_loss_sol,
        RISK_CIRCUIT_BREAKER_MINUTES=risk_cb_min,
        STOP_AFTER_DAILY_PROFIT_PCT=stop_after_profit,
        DAILY_SPEND_CAP_PCT=daily_spend_cap,
        RISK_HARD_KILL_ON_DRAWNDOWN=risk_hard_kill,
        RISK_ALLOW_REENTRY_AFTER_BREAK=risk_reentry,
        EXIT_STOP_LOSS_PCT=exit_sl,
        EXIT_TP1_PCT=exit_tp1,
        EXIT_TP2_PCT=exit_tp2,
        EXIT_TP3_PCT=exit_tp3,
        EXIT_TSL_ENABLED=exit_tsl_enabled,
        EXIT_TSL_ARM_PCT=exit_tsl_arm,
        EXIT_TSL_STEP_PCT=exit_tsl_step,
        PF_TSL_ARM_PCT=pf_tsl_arm,
        PF_TSL_PCT=pf_tsl_pct,
        PF_SL_PCT=pf_sl_pct,
        EXIT_STAGNANT_MAX_MIN=exit_stag_max,
        EXIT_STAGNANT_MIN_MOVE_PCT=exit_stag_min_move,
        EXIT_DEAD_COIN_MAX_MIN=exit_dead_max,
        EXIT_DEAD_MIN_LIQ_USD=exit_dead_min_liq,
        EXIT_FORCE_AFTER_X_MULT=exit_force_x,
        EXIT_LOCK_PROFIT_AFTER_MULT=exit_lock_after,
        EXIT_LOCK_PROFIT_TSL_STEP_PCT=exit_lock_step,
        AUTO_EXIT_WORKER_ENABLED=auto_exit_worker_enabled,
        ENTRY_MOMENTUM_ENABLED=entry_momo_enabled,
        ENTRY_MOMO_WINDOW_SEC=entry_momo_window,
        ENTRY_MOMO_MIN_TRADES=entry_momo_min_trades,
        ENTRY_MOMO_MIN_UP_PCT=entry_momo_min_up,
        ENTRY_MOMO_ALLOW_NO_DATA_FOR_PUMP=entry_momo_allow_nodata_pump,
        SCAN_INTERVAL=scan_interval,
        SCANNER_PERIOD_SEC=scanner_period,
        BUY_COOLDOWN_SEC=buy_cooldown,
        PF_MAX_CONCURRENT=pf_max_concurrent,
        PREWARM_TOP_N=prewarm_top_n,
        SCANNER_MIN_LIQ_USD=scanner_min_liq,
        MIN_LIQ_USD=min_liq,
        MIN_POOL_AGE_MIN=min_pool_age,
        NOROUTE_COOLDOWN_SEC=noroute_cd,
        BLOCKLIST_MINTS=blocklist_mints,
        ALLOWLIST_MINTS=allowlist_mints,
        STRATEGY_PRESET=strategy_preset,
        TELEGRAM_HEARTBEATS=tg_heartbeats,
        HEARTBEAT_EVERY_SEC=hb_every,
        HEARTBEAT_ONLY_ON_CHANGE=hb_only_on_change,
        TELEGRAM_NOTIFY_ROUTE_BLOCKS=tg_route_blocks,
        PNL_TELEGRAM_HEARTBEAT=pnl_tg_hb,
        MOONSHOT_ENABLED=moonshot_enabled,
        MOONSHOT_DAILY_USD=moonshot_daily_usd,
        MOONSHOT_MAX_TRADE_USD=moonshot_max_trade_usd,
        MOONSHOT_AGE_CUTOFF_MIN=moonshot_age_cutoff,
        MOONSHOT_LIQ_CUTOFF_USD=moonshot_liq_cutoff,
        MOONSHOT_IMPACT_CUTOFF_BPS=moonshot_impact_cutoff,
        ENABLE_RAYDIUM_SCANNER=enable_raydium,
        ENABLE_PUMPFUN_SCANNER=enable_pumpfun,
        ENABLE_DEXSCREENER_SCANNER=enable_dexs,
        ENABLE_ARBITRAGE=enable_arbitrage,
        DEXES=dexes,
        SCANNER_MIN_FDV_USD=scanner_min_fdv,
        SCANNER_MIN_FRAUD_SCORE=scanner_min_fraud,
        SCANNER_ALERTS_ENABLED=scanner_alerts_enabled,
        SCANNER_MIN_BUYS_5M=scanner_min_buys_5m,
        SCANNER_MIN_BUYS_15M=scanner_min_buys_15m,
        SCANNER_MIN_VOLUME_5M_USD=scanner_min_vol_5m,
        SCANNER_MIN_VOLUME_15M_USD=scanner_min_vol_15m,
        SCANNER_REQUIRE_BUY_PRESSURE=scanner_req_buy_pressure,
        SCANNER_MIN_BUY_RATIO=scanner_min_buy_ratio,
        SCANNER_MIN_PRICE_CHANGE_5M=scanner_min_chg_5m,
        SCANNER_MIN_PRICE_CHANGE_15M=scanner_min_chg_15m,
        SCANNER_MAX_AGE_MIN=scanner_max_age_min,
        SCANNER_MAX_MC_USD=scanner_max_mc,
        SCANNER_MIN_HOLDERS=scanner_min_holders,
        ALERT_COOLDOWN_SECONDS=alert_cd,
        POSITION_MONITOR_INTERVAL=pos_monitor_iv,
        REINVESTMENT_CHECK_INTERVAL=reinvest_check_iv,
        PF_SCAN_MIN_LIQ_USD=pf_scan_min_liq,
        PF_SCAN_MAX_LIQ_USD=pf_scan_max_liq,
        PF_SCAN_MIN_MCAP_USD=pf_scan_min_mcap,
        PF_SCAN_MAX_MCAP_USD=pf_scan_max_mcap,
        PF_SCAN_MAX_TOKEN_AGE_MIN=pf_scan_max_age,
        PF_SCAN_MIN_5M_VOL_USD=pf_scan_min_vol,
        PF_SCAN_MIN_5M_BUYS=pf_scan_min_buys,
        PF_SCAN_MIN_BUY_TO_SELL_RATIO=pf_scan_min_ratio,
        PF_SCAN_MIN_WHALE_SIZE_SOL=pf_min_whale_size_sol,
        PF_SCAN_MIN_WHALE_COUNT_5M=pf_min_whale_count,
        PF_MAX_RUG_RISK_SCORE=pf_max_rug,
        PF_MIN_MOMENTUM_SCORE=pf_min_momo,
        PF_MIN_FINAL_SCORE=pf_min_final,
        PF_RISK_PENALTY_WEIGHT=pf_risk_penalty_w,
        AI_ENABLED=ai_enabled,
        AI_ENTRY_ENABLED=ai_entry_enabled,
        AI_ENTRY_MODEL=ai_entry_model,
        AI_ENTRY_SCORE_MIN=ai_entry_score_min,
        AI_ENTRY_SCORE_HARD_MIN=ai_entry_score_hard_min,
        AI_ENTRY_RISK_ADJ_MIN=ai_entry_risk_adj_min,
        AI_ENTRY_THRESHOLD=ai_entry_threshold,
        AI_SCORE_GATE=ai_score_gate,
        AI_SCORE_SOFT_FLOOR=ai_score_soft,
        AI_SCORE_HARD_FLOOR=ai_score_hard,
        AI_ENTRY_MAX_CONCURRENCY=ai_entry_max_conc,
        AI_PRE_SCORE_ENABLED=ai_pre_score_enabled,
        AI_PRE_SCORE_MIN=ai_pre_score_min,
        ENTRY_REQUIRE_RISK_CAPACITY=entry_require_risk_capacity,
        ENTRY_RISK_CAPACITY_MIN=entry_risk_capacity_min,
        AI_RUG_ENABLED=ai_rug_enabled,
        AI_RUG_MODEL=ai_rug_model,
        AI_RUG_MAX_SCORE=ai_rug_max,
        AI_RUG_MIN_CONFIDENCE=ai_rug_min_conf,
        AIGATE_MAX_PRICE_IMPACT_BPS=aigate_max_impact,
        AI_BAD_THRESHOLD=ai_bad,
        AI_GOOD_THRESHOLD=ai_good,
        AI_WHALE_ECHO=ai_whale_echo,
        AI_WHALE_ECHO_MIN_SIZE_USD=ai_whale_echo_min_usd,
        AI_ADAPTIVE_SELL=ai_adaptive_sell,
        AI_SELL_ENABLED=ai_sell_enabled,
        AI_SELL_MODEL=ai_sell_model,
        AI_SELL_HOLD_THRESHOLD=ai_sell_hold,
        AI_SELL_TRIM_THRESHOLD=ai_sell_trim,
        AI_SELL_EXIT_THRESHOLD=ai_sell_exit,
        STRATEGY_REDIS_PREFIX=strategy_redis_prefix,
        STRATEGY_UPDATE_INTERVAL=strategy_update_interval,
        AI_FEATURE_VERSION=ai_feature_version,
        AI_BUY_FEATURE_VERSION=ai_buy_feature_version,
        REGIME_ENABLED=regime_enabled,
        REGIME_UPDATE_INTERVAL_SEC=regime_update_iv,
        REGIME_SOL_MIN_24H_VOL_USD=regime_sol_min_vol,
        REGIME_MEME_MIN_24H_VOL_USD=regime_meme_min_vol,
        REGIME_MAX_ACCEPTABLE_RUG_RATE=regime_max_rug_rate,
        LOG_ENTRY_CANDIDATES=log_entry_candidates,
        LOG_TRADES=log_trades,
        LOG_REGIME_SNAPSHOTS=log_regime_snapshots,
        TRAIN_LABEL_WINDOW_MIN=train_label_window,
        TRAIN_MIN_HOLD_MIN=train_min_hold,
        TRAIN_MAX_HOLD_MIN=train_max_hold,
        SMART_WALLETS_ENABLED=smart_wallets_enabled,
        ENABLE_WHALE_FILTERS=enable_whale_filters,
        DYNAMIC_WHALE_DISCOVERY=dynamic_whale_discovery,
        CURATED_WHALE_WALLETS=curated_whales,
        WHALE_WALLETS_JSON=(whale_json if isinstance(whale_json, dict) else {}),
        WHALE_WALLETS=whale_wallets,
        ENABLE_SOCIAL_SCRAPING=enable_social_scraping,
        X_BEARER_TOKEN=x_bearer,
        TELEGRAM_SCRAPE_BOT=tg_scrape_bot,
        DISCORD_SEARCH_CHANNEL_ID=discord_search_channel,
        ENABLE_DAYTRADER=enable_daytrader,
        DAYTRADER_SYMBOLS=daytrader_symbols,
        DAYTRADER_TIMEFRAMES=daytrader_timeframes,
        DAYTRADER_EXCHANGES=daytrader_exchanges,
        DT_FEATURE_SET=dt_feature_set,
        DT_MODEL_KIND=dt_model_kind,
        DT_PROB_THRESHOLD=dt_prob_threshold,
        DT_MAX_CONCURRENT=dt_max_concurrent,
        DT_MAX_LEVERAGE=dt_max_leverage,
        DT_MAX_RISK_PER_TRADE=dt_max_risk_per_trade,
        DT_TAKE_PROFIT_BPS=dt_tp_bps,
        DT_STOP_LOSS_BPS=dt_sl_bps,
        DT_TRAILING_BPS=dt_trailing_bps,
        DT_COOLDOWN_SEC=dt_cooldown_sec,
        DT_PAPER_TRADING=dt_paper,
        BACKTEST_ENABLED=backtest_enabled,
        BACKTEST_SOURCE=backtest_source,
        BACKTEST_MAX_WORKERS=backtest_max_workers,
        DIAG_BT_DAYS_BACK=diag_days_back,
        DIAG_BT_LIMIT_CANDIDATES=diag_limit_cands,
        BT_DAYS_BACK=bt_days_back,
        BT_LIMIT_CANDIDATES=bt_limit_candidates,
        SOLANA_ENDPOINT=solana_endpoint,
        SOLANA_RPC_URL=solana_rpc_url,
        SOL_RPC_URL=sol_rpc_url,
        DRYRUN=dryrun,
        ENABLE_PUMP_SCANNER=enable_pump_scanner,
        LIQ_MIN_USD=liq_min_usd,
        MAX_CONCURRENT_POS=max_concurrent_pos,
        MAX_CONCURRENT_BUYS=max_concurrent_buys,
        MAX_CONCURRENCY=max_concurrency,
        RAYDIUM_PAIRS_API=ray_pairs_api,
        DEXSCREENER_TOKENS_API=dexs_tokens_api,
        PUMPFUN_TOKENS_API=pump_tokens_api,
        PRIVATE_KEY=wallet_priv,  # legacy alias
        TARGET_MARKET=target_market,
    )


# =============================================================================
# Validation (no side effects beyond returning lists + logging warnings)
# =============================================================================


class ConfigError(RuntimeError):
    pass


def _validate(cfg: Config) -> tuple[list[str], list[str]]:
    errors: list[str] = []
    warnings: list[str] = []

    # Basic sanity
    if cfg.ENV not in {"dev", "test", "staging", "prod", "local"}:
        warnings.append(f"ENV={cfg.ENV!r} not in canonical set; treated as dev-like.")

    # URLs required for capabilities
    if cfg.rpc_required and not cfg.RPC_URL:
        errors.append("rpc_required but RPC_URL is empty")
    if cfg.redis_required and not cfg.REDIS_URL:
        errors.append("redis_required but REDIS_URL is empty")

    # Wallet requirement
    has_wallet = bool(cfg.WALLET_PRIVATE_KEY) or bool(cfg.KEYPAIR_PATH)
    if cfg.wallet_required and not has_wallet:
        # In SAFE_MODE/tests allow as warning; in deploy strict it will be promoted.
        warnings.append(
            "wallet_required but wallet secrets are missing (SAFE_MODE or non-deploy context)"
        )

    # DB DSN requirement
    if cfg.db_required and not cfg.POSTGRES_DSN and not cfg.SKIP_DSN_REQUIRED:
        errors.append("db_required but POSTGRES_DSN is empty (and SKIP_DSN_REQUIRED=0)")

    # Jupiter auth policy
    if cfg.JUP_STRICT_AUTH and not cfg.JUP_PRO_KEY and not cfg.JUP_ALLOW_PUBLIC_FALLBACK:
        warnings.append("Strict Jupiter auth enabled but no pro key and no fallback")

    # Jupiter endpoints should be http(s)
    if cfg.JUPITER_QUOTE_API and not _is_http_url(cfg.JUPITER_QUOTE_API):
        errors.append("JUPITER_QUOTE_API must be http(s)")
    if cfg.JUPITER_SWAP_API and not _is_http_url(cfg.JUPITER_SWAP_API):
        errors.append("JUPITER_SWAP_API must be http(s)")

    # Slippage sanity
    if not (1 <= cfg.JUPITER_SLIPPAGE_BPS <= 5000):
        warnings.append(
            f"JUPITER_SLIPPAGE_BPS out of expected range (1..5000): {cfg.JUPITER_SLIPPAGE_BPS}"
        )

    return errors, warnings


# =============================================================================
# Public API (used by scripts.preflight / scripts.check_env)
# =============================================================================

_CONFIG_ERRORS: list[str] = []
_CONFIG_WARNINGS: list[str] = []

CONFIG_ERRORS: tuple[str, ...] = ()
CONFIG_WARNINGS: tuple[str, ...] = ()
CONFIG_FINGERPRINT: str = ""


def build_config(environ: Mapping[str, str] | None = None) -> Config:
    return _build_config(environ or os.environ)


@lru_cache(maxsize=1)
def get_config() -> Config:
    return _build_config(os.environ)


def apply_config(cfg: Config) -> None:
    """
    Force-apply a config object (used primarily by tests).
    Updates module-level exports and cached summaries.
    """
    global CONFIG, C, CONFIG_ERRORS, CONFIG_WARNINGS, CONFIG_FINGERPRINT
    CONFIG = cfg
    C = cfg
    _export_legacy(cfg)
    CONFIG_FINGERPRINT = cfg.fingerprint()


def reload_config_for_tests(environ: Mapping[str, str] | None = None) -> Config:
    """
    Clear cache and rebuild config (test helper).
    """
    get_config.cache_clear()
    cfg = _build_config(environ or os.environ)
    apply_config(cfg)
    return cfg


def validate_config(
    environ: Mapping[str, str] | None = None, *, strict: bool | None = None
) -> bool:
    """
    Validate the canonical config and optionally raise if strict.
    scripts.preflight relies on this.
    """
    global _CONFIG_ERRORS, _CONFIG_WARNINGS, CONFIG_ERRORS, CONFIG_WARNINGS, CONFIG_FINGERPRINT

    cfg = _build_config(environ or os.environ)
    errors, warnings = _validate(cfg)

    _CONFIG_ERRORS = errors
    _CONFIG_WARNINGS = warnings
    CONFIG_ERRORS = tuple(errors)
    CONFIG_WARNINGS = tuple(warnings)
    CONFIG_FINGERPRINT = cfg.fingerprint()

    # Log warnings (never secrets)
    for w in warnings:
        _warn("[CONFIG] %s", w)

    do_strict = cfg.STRICT_CONFIG if strict is None else bool(strict)
    if errors and do_strict:
        raise ConfigError("; ".join(errors))

    # Also apply config so module exports match what we validated.
    apply_config(cfg)
    return not bool(errors)


def validate_environment(
    environ: Mapping[str, str] | None = None,
    *,
    strict: bool | None = None,
) -> tuple[bool, list[str], list[str], dict[str, Any]]:
    """
    Used by scripts.check_env for canonical validation + optional details.
    Returns: (ok, errors, warnings, detail)
    """
    cfg = _build_config(environ or os.environ)
    errors, warnings = _validate(cfg)

    do_strict = cfg.STRICT_CONFIG if strict is None else bool(strict)
    ok = not bool(errors)

    if do_strict and errors:
        ok = False

    detail = {
        "config_version": cfg.config_version,
        "fingerprint": cfg.fingerprint(),
        "env": cfg.ENV,
        "phase": cfg.MASTER_PHASE,
        "strategy_phase": cfg.STRATEGY_PHASE,
        "caps": {
            "buys_possible": cfg.buys_possible,
            "scanning_possible": cfg.scanning_possible,
            "db_required": cfg.db_required,
            "wallet_required": cfg.wallet_required,
            "redis_required": cfg.redis_required,
            "rpc_required": cfg.rpc_required,
        },
        "dsn": {
            "pg_enabled": cfg.PG_ENABLED,
            "pg_dsn_mode": cfg.PG_DSN_MODE,
            "pg_dsn_route": cfg.PG_DSN_ROUTE,
            "pg_dsn_host": cfg.PG_DSN_HOST,
            "postgres_dsn_set": bool(cfg.POSTGRES_DSN),
            "postgres_dsn_redacted": _redact_dsn(cfg.POSTGRES_DSN),
        },
        "urls": {
            "RPC_URL": cfg.RPC_URL,
            "JUPITER_QUOTE_API": cfg.JUPITER_QUOTE_API,
            "JUPITER_SWAP_API": cfg.JUPITER_SWAP_API,
        },
    }

    # Apply/refresh module exports in non-strict mode too (keeps runtime consistent).
    apply_config(cfg)

    return ok, errors, warnings, detail


# =============================================================================
# Instantiate once (safe)
# =============================================================================

CONFIG = get_config()
C = CONFIG
_export_legacy(CONFIG)
CONFIG_FINGERPRINT = CONFIG.fingerprint()

__all__ = [
    "Config",
    "CONFIG",
    "C",
    "ENV_SCHEMA",
    "CONFIG_VERSION",
    "CONFIG_LOADED_AT_UNIX",
    "CONFIG_ERRORS",
    "CONFIG_WARNINGS",
    "CONFIG_FINGERPRINT",
    "build_config",
    "get_config",
    "apply_config",
    "reload_config_for_tests",
    "validate_config",
    "validate_environment",
    "ConfigError",
]
