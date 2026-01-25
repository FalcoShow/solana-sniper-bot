from __future__ import annotations

"""
scripts/smoke_pg_only.py — V3+++ Postgres-Only Smoke Test (LOCK-IN)
===================================================================

Goal
----
Validate the Postgres DSN layer is rock-solid (Render + local):
  - DSN selection works (utils_pkg.db_dsn.pick_pg_dsn)
  - DSN sanitization is driver-safe
  - SSL mode is correctly enforced (sslmode=require/verify-*)
  - We can connect and run SELECT 1

Hard rules
----------
- NO writes/migrations. Read-only: SELECT 1 + optional safe server introspection.
- NEVER prints secrets (passwords, tokens, full DSN).
- JSON output (when enabled) goes to STDOUT; human logs go to STDERR.

Exit codes
----------
0 = success
2 = failure
130 = ctrl-c
"""

import argparse
import asyncio
import contextlib
import json
import os
import platform
import re
import socket
import ssl
import sys
import time
import traceback
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

# Optional TLS CA bundle
try:
    import certifi  # type: ignore
except Exception:  # pragma: no cover
    certifi = None  # type: ignore


RC_OK = 0
RC_FAIL = 2
RC_CTRL_C = 130

_TRUE = {"1", "true", "yes", "y", "on", "t"}
_FALSE = {"0", "false", "no", "n", "off", "f"}

_URL_CRED_RE = re.compile(r"(://)([^/@\s]+)@")
_LONG_TOKENISH_RE = re.compile(r"\b[A-Za-z0-9_\-]{28,}\b")


# ──────────────────────────────────────────────────────────────────────────────
# Bootstrap repo root (direct-run friendly)
# ──────────────────────────────────────────────────────────────────────────────
def _bootstrap_repo_root() -> str:
    try:
        here = Path(__file__).resolve()
    except Exception:
        return ""

    markers = ("pyproject.toml", "requirements.txt", "setup.cfg", ".git")
    for p in [here.parent, *list(here.parents)]:
        with contextlib.suppress(Exception):
            if any((p / m).exists() for m in markers) or (p / "src").is_dir():
                root = str(p)
                if root and root not in sys.path:
                    sys.path.insert(0, root)
                os.environ.setdefault("REPO_ROOT", root)
                return root

    root = str(here.parents[1] if len(here.parents) > 1 else here.parent)
    if root and root not in sys.path:
        sys.path.insert(0, root)
    os.environ.setdefault("REPO_ROOT", root)
    return root


def _maybe_load_dotenv() -> None:
    if _envb("SMOKE_NO_DOTENV", False):
        return
    with contextlib.suppress(Exception):
        from dotenv import load_dotenv  # type: ignore

        load_dotenv(override=False)


# ──────────────────────────────────────────────────────────────────────────────
# Env helpers
# ──────────────────────────────────────────────────────────────────────────────
def _envs(k: str, d: str = "") -> str:
    v = os.getenv(k)
    s = v if v is not None else d
    s = str(s).strip()
    if (s.startswith('"') and s.endswith('"')) or (s.startswith("'") and s.endswith("'")):
        s = s[1:-1].strip()
    return s


def _envb(k: str, d: bool = False) -> bool:
    v = _envs(k, "")
    if v == "":
        return bool(d)
    s = v.lower().strip()
    if s in _TRUE:
        return True
    if s in _FALSE:
        return False
    return bool(d)


def _envi(k: str, d: int) -> int:
    try:
        return int(float(_envs(k, str(d))))
    except Exception:
        return int(d)


def _envf(k: str, d: float) -> float:
    try:
        return float(_envs(k, str(d)))
    except Exception:
        return float(d)


def _ts() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def _now_ms() -> int:
    return int(time.time() * 1000)


# ──────────────────────────────────────────────────────────────────────────────
# Safe output (cp1252-safe)
# ──────────────────────────────────────────────────────────────────────────────
def _safe_write(file: Any, s: str) -> None:
    try:
        file.write(s)
        file.flush()
    except UnicodeEncodeError:
        enc = getattr(file, "encoding", None) or "utf-8"
        b = s.encode(enc, errors="replace")
        with contextlib.suppress(Exception):
            if hasattr(file, "buffer"):
                file.buffer.write(b)
                file.buffer.flush()
                return
        with contextlib.suppress(Exception):
            file.write(b.decode(enc, errors="replace"))
            file.flush()
    except Exception:
        return


def _hlog(msg: str, *, json_only: bool) -> None:
    if json_only:
        return
    _safe_write(sys.stderr, f"[smoke_pg_only] {_ts()} {msg}\n")


# ──────────────────────────────────────────────────────────────────────────────
# Redaction / DSN masking
# ──────────────────────────────────────────────────────────────────────────────
_SENSITIVE_ENV_KEYS = [
    "DATABASE_URL",
    "POSTGRES_DSN",
    "PG_DSN",
    "PG_DSN_INTERNAL",
    "PG_DSN_EXTERNAL",
    "PNL_DSN",
    "OPENAI_API_KEY",
    "TELEGRAM_BOT_TOKEN",
    "DISCORD_TOKEN",
    "DISCORD_WEBHOOK_URL",
    "NOTION_TOKEN",
    "WALLET_PRIVATE_KEY",
    "KEYPAIR_B64",
    "KEYPAIR_PATH",
    "KEYPAIR_PATH",
    "REDIS_URL",
    "REDIS_TLS_URL",
]


def _sanitize(x: Any) -> str:
    if x is None:
        return ""
    s = str(x)
    s = _URL_CRED_RE.sub(r"\1***@", s)
    for k in _SENSITIVE_ENV_KEYS:
        v = os.getenv(k, "")
        if v and len(v) >= 6:
            s = s.replace(v, "***")
    if _envb("PREFLIGHT_MASK_LONG_TOKENS", True):
        s = _LONG_TOKENISH_RE.sub("***", s)
    return s


def _mask_dsn_password(dsn: str) -> str:
    """Hide password but keep host/db visible."""
    try:
        u = urlparse(dsn)
        if not u.username:
            return _sanitize(dsn)
        netloc = u.netloc
        # replace user:pass@ with user:***@
        netloc = re.sub(rf"{re.escape(u.username)}:([^@]+)@", f"{u.username}:***@", netloc)
        return _sanitize(urlunparse((u.scheme, netloc, u.path, u.params, u.query, u.fragment)))
    except Exception:
        return "<dsn>"


# ──────────────────────────────────────────────────────────────────────────────
# Driver detection
# ──────────────────────────────────────────────────────────────────────────────
def _asyncpg_is_usable() -> bool:
    """
    asyncpg must have its compiled protocol module available.
    Your error indicates a partial install (no protocol module).
    """
    try:
        import asyncpg  # type: ignore
        import asyncpg.protocol.protocol  # type: ignore  # noqa: F401

        return True
    except Exception:
        return False


def _psycopg_is_usable() -> bool:
    try:
        import psycopg  # type: ignore  # noqa: F401

        return True
    except Exception:
        return False


# ──────────────────────────────────────────────────────────────────────────────
# DSN normalization
# ──────────────────────────────────────────────────────────────────────────────
def _ensure_sa_asyncpg_url(dsn: str) -> str:
    """
    SQLAlchemy AsyncEngine expects postgresql+asyncpg://
    Accepts:
      - postgres://
      - postgresql://
      - postgresql+asyncpg://
    """
    dsn = (dsn or "").strip()
    if dsn.startswith("postgresql+asyncpg://"):
        return dsn
    if dsn.startswith("postgres://"):
        return "postgresql+asyncpg://" + dsn[len("postgres://") :]
    if dsn.startswith("postgresql://"):
        return "postgresql+asyncpg://" + dsn[len("postgresql://") :]
    return dsn


def _ensure_libpq_url(dsn: str) -> str:
    """
    psycopg/libpq expects postgresql:// or postgres:// (no +asyncpg).
    """
    dsn = (dsn or "").strip()
    if dsn.startswith("postgresql+asyncpg://"):
        return "postgresql://" + dsn[len("postgresql+asyncpg://") :]
    if dsn.startswith("postgresql+psycopg://"):
        return "postgresql://" + dsn[len("postgresql+psycopg://") :]
    if dsn.startswith("postgres://"):
        return "postgresql://" + dsn[len("postgres://") :]
    return dsn


def _strip_query(url: str) -> tuple[str, dict[str, str]]:
    """
    IMPORTANT: SQLAlchemy asyncpg dialect forwards URL query params into asyncpg.connect(**kwargs).
    Unknown keys can crash. We strip the query entirely after extracting what we need (sslmode).
    """
    u = urlparse(url)
    q = dict(parse_qsl(u.query, keep_blank_values=True))
    stripped = urlunparse((u.scheme, u.netloc, u.path, u.params, "", u.fragment))
    return stripped, q


def _ssl_ctx_for_mode(sslmode: str, insecure: bool) -> ssl.SSLContext | None:
    """
    Translate libpq sslmode into an SSLContext for asyncpg.
    """
    mode = (sslmode or "").strip().lower()
    if mode in ("", "disable", "0", "false", "off"):
        return None

    cafile = None
    if certifi is not None:
        with contextlib.suppress(Exception):
            cafile = certifi.where()  # type: ignore[attr-defined]

    ctx = ssl.create_default_context(cafile=cafile)

    if insecure:
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE

    return ctx


@dataclass
class PgSanitized:
    raw_dsn: str
    sa_url: str
    libpq_url: str
    host: str
    port: int
    db: str
    sslmode: str
    ssl_enabled: bool
    connect_args: dict[str, Any]


def _build_sanitized_pg(
    dsn: str,
    connect_args: dict[str, Any] | None,
    *,
    timeout_s: float,
    app_name: str,
    sslmode_override: str | None,
    insecure_ssl: bool,
) -> PgSanitized:
    dsn = (dsn or "").strip()
    if not dsn:
        raise ValueError("empty DSN")

    # SQLAlchemy+asyncpg path
    sa = _ensure_sa_asyncpg_url(dsn)
    sa_noq, q = _strip_query(sa)

    sslmode = (sslmode_override or q.get("sslmode") or _envs("PG_SSLMODE", "")).strip()
    ssl_required = sslmode.lower() in {"require", "verify-ca", "verify-full"}

    ca: dict[str, Any] = dict(connect_args or {})

    # Remove anything that can crash asyncpg
    ca.pop("sslmode", None)
    ca.pop("ssl", None)

    # Smoke-safe defaults
    ca["statement_cache_size"] = int(_envs("PG_STATEMENT_CACHE_SIZE", "0") or "0")
    ca["command_timeout"] = float(timeout_s)

    ss = ca.get("server_settings")
    if not isinstance(ss, dict):
        ss = {}
    ss.setdefault("application_name", app_name)
    ca["server_settings"] = ss

    insecure_env = _envb("PG_SSL_INSECURE", False)
    ssl_ctx = _ssl_ctx_for_mode(sslmode, insecure=bool(insecure_ssl or insecure_env))

    ssl_enabled = False
    if ssl_required or (ssl_ctx is not None):
        if ssl_ctx is not None:
            ca["ssl"] = ssl_ctx
            ssl_enabled = True
        else:
            raise RuntimeError("sslmode requires SSL but SSLContext could not be constructed")

    u = urlparse(sa_noq)
    host = u.hostname or ""
    port = int(u.port or 5432)
    db = (u.path or "").lstrip("/")

    # psycopg/libpq path
    libpq = _ensure_libpq_url(dsn)
    libpq_base, _libpq_q = _strip_query(libpq)
    qp: dict[str, str] = {}
    if sslmode:
        # if insecure requested while verify-full, downgrade to require for "connectivity-only" smoke
        sm = sslmode.strip().lower()
        if (insecure_ssl or insecure_env) and sm in {"verify-full", "verify-ca"}:
            qp["sslmode"] = "require"
        else:
            qp["sslmode"] = sslmode
    libpq_url = urlunparse(
        (
            urlparse(libpq_base).scheme or "postgresql",
            urlparse(libpq_base).netloc,
            urlparse(libpq_base).path,
            urlparse(libpq_base).params,
            urlencode(qp) if qp else "",
            urlparse(libpq_base).fragment,
        )
    )

    return PgSanitized(
        raw_dsn=dsn,
        sa_url=sa_noq,
        libpq_url=libpq_url,
        host=host,
        port=port,
        db=db,
        sslmode=sslmode,
        ssl_enabled=ssl_enabled,
        connect_args=ca,
    )


# ──────────────────────────────────────────────────────────────────────────────
# Probes (optional)
# ──────────────────────────────────────────────────────────────────────────────
def _dns_ips(host: str, port: int) -> list[str]:
    try:
        infos = socket.getaddrinfo(host, port, proto=socket.IPPROTO_TCP)
        out: list[str] = []
        for _fam, _typ, _pro, _cn, sockaddr in infos:
            ip = sockaddr[0]
            if ip not in out:
                out.append(ip)
        return out
    except Exception:
        return []


def _tcp_probe(host: str, port: int, timeout_s: float) -> tuple[bool, str]:
    try:
        with socket.create_connection((host, port), timeout=timeout_s):
            return True, "ok"
    except Exception as e:
        return False, f"{type(e).__name__}: {e}"


# ──────────────────────────────────────────────────────────────────────────────
# Core DB check (asyncpg path)
# ──────────────────────────────────────────────────────────────────────────────
async def _select1_sqlalchemy_asyncpg(
    sa_url: str, connect_args: dict[str, Any], timeout_s: float
) -> dict[str, Any]:
    eng = create_async_engine(
        sa_url,
        connect_args=connect_args,
        pool_pre_ping=True,
        pool_size=1,
        max_overflow=0,
        pool_timeout=max(1.0, float(timeout_s)),
    )
    t0 = _now_ms()
    try:
        async with eng.connect() as conn:
            row = (await conn.execute(text("SELECT 1"))).first()
            if not row:
                raise RuntimeError("SELECT 1 returned no row")
            dt = _now_ms() - t0
            return {
                "ok": True,
                "driver": "sqlalchemy+asyncpg",
                "select1": int(row[0]),
                "ms": int(dt),
            }
    finally:
        await eng.dispose()


# ──────────────────────────────────────────────────────────────────────────────
# Core DB check (psycopg path, runs in a worker thread)
# ──────────────────────────────────────────────────────────────────────────────
def _select1_psycopg_sync(dsn: str, timeout_s: float, app_name: str) -> dict[str, Any]:
    import psycopg  # type: ignore

    t0 = _now_ms()
    # connect_timeout is in seconds (int)
    with psycopg.connect(
        dsn, connect_timeout=max(1, int(timeout_s)), application_name=app_name
    ) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
            row = cur.fetchone()
            if not row:
                raise RuntimeError("SELECT 1 returned no row")
            dt = _now_ms() - t0
            return {"ok": True, "driver": "psycopg", "select1": int(row[0]), "ms": int(dt)}


async def _select1_with_best_driver(
    S: PgSanitized,
    *,
    timeout_s: float,
    app_name: str,
) -> dict[str, Any]:
    if _asyncpg_is_usable():
        return await _select1_sqlalchemy_asyncpg(S.sa_url, S.connect_args, timeout_s)

    if _psycopg_is_usable():
        # run sync psycopg connect in a thread so we stay asyncio-friendly
        return await asyncio.to_thread(_select1_psycopg_sync, S.libpq_url, timeout_s, app_name)

    raise RuntimeError(
        "No usable Postgres driver. Install one of:\n"
        "  pip install 'psycopg[binary]'\n"
        "  (or fix asyncpg installation for your Python version)"
    )


# ──────────────────────────────────────────────────────────────────────────────
# DSN picker (signature-drift tolerant)
# ──────────────────────────────────────────────────────────────────────────────
def _pick_pg_dsn_any() -> tuple[str, dict[str, Any], str]:
    """
    Returns: (dsn, connect_args, source_label)
    Tolerates pick_pg_dsn() returning:
      - (dsn, connect_args)
      - (dsn, connect_args, source)
      - {"dsn":..., "connect_args":..., "source":...}
    """
    from utils_pkg.db_dsn import pick_pg_dsn

    res = pick_pg_dsn()
    if isinstance(res, dict):
        dsn = str(res.get("dsn") or "")
        ca = res.get("connect_args") or {}
        if not isinstance(ca, dict):
            ca = {}
        src = str(res.get("source") or "pick_pg_dsn")
        return dsn, ca, src

    if isinstance(res, tuple | list) and len(res) >= 2:
        dsn = str(res[0] or "")
        ca = res[1] if isinstance(res[1], dict) else {}
        src = str(res[2]) if len(res) >= 3 else "pick_pg_dsn"
        return dsn, ca, src

    return str(res or ""), {}, "pick_pg_dsn"


def _short_exc(e: BaseException) -> str:
    return f"{type(e).__name__}: {e}"


# ──────────────────────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────────────────────
async def amain(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        description="Postgres-only connectivity smoke test (asyncpg if available; psycopg fallback)."
    )
    ap.add_argument("--timeout", type=float, default=float(_envf("SMOKE_TIMEOUT_S", 12.0)))
    ap.add_argument(
        "--total-timeout", type=float, default=float(_envf("SMOKE_TOTAL_TIMEOUT_S", 0.0))
    )
    ap.add_argument("--tries", type=int, default=int(_envi("SMOKE_TRIES", 3)))
    ap.add_argument(
        "--sslmode", type=str, default="", help="Override sslmode (require/disable/verify-*)."
    )
    ap.add_argument(
        "--insecure",
        type=int,
        default=1 if _envb("PG_SSL_INSECURE", False) else 0,
        help="Disable SSL verification (debug only).",
    )
    ap.add_argument("--app-name", type=str, default=_envs("PG_APP_NAME", "pnlsniper_smoke_pg_only"))
    ap.add_argument("--no-probe", type=int, default=1 if _envb("SMOKE_NO_PROBE", False) else 0)
    ap.add_argument("--json", action="store_true", default=_envb("SMOKE_JSON", False))
    ap.add_argument("--json-only", action="store_true", default=_envb("SMOKE_JSON_ONLY", False))
    ap.add_argument("--debug", action="store_true", default=_envb("SMOKE_DEBUG", False))
    args = ap.parse_args(argv if argv is not None else None)

    json_only = bool(args.json_only)
    emit_json = bool(args.json or args.json_only)
    debug = bool(args.debug)

    _bootstrap_repo_root()
    _maybe_load_dotenv()

    timeout_s = max(3.0, float(args.timeout))
    total_timeout_s = max(0.0, float(args.total_timeout))
    tries = max(1, int(args.tries))

    summary: dict[str, Any] = {
        "ts": _ts(),
        "ok": False,
        "step": "smoke_pg_only",
        "tries": tries,
        "timeout_s": float(timeout_s),
        "total_timeout_s": float(total_timeout_s),
        "dsn_source": None,
        "dsn_masked": None,
        "sa_url_masked": None,
        "libpq_url_masked": None,
        "host": None,
        "port": None,
        "db": None,
        "sslmode": None,
        "ssl_enabled": None,
        "driver": None,
        "probes": {"dns_ips": None, "tcp_ok": None, "tcp_err": None},
        "attempts": [],
        "errors": [],
        "warnings": [],
        "env": {
            "python": _sanitize(sys.version.split()[0]),
            "platform": _sanitize(platform.platform()),
            "PG_SSLMODE": _sanitize(_envs("PG_SSLMODE", "")),
            "PG_SSL_INSECURE": int(_envb("PG_SSL_INSECURE", False)),
        },
        "elapsed_ms": 0,
    }

    # Pick DSN
    try:
        dsn, connect_args, dsn_source = _pick_pg_dsn_any()
    except Exception as e:
        summary["errors"].append(_sanitize(f"pick_pg_dsn_failed:{type(e).__name__}:{e}"))
        if emit_json:
            print(json.dumps(summary, ensure_ascii=False, default=str))
        _hlog(f"FAIL: DSN selection failed: {_sanitize(e)}", json_only=json_only)
        return RC_FAIL

    if not dsn:
        summary["errors"].append("no_dsn_available")
        if emit_json:
            print(json.dumps(summary, ensure_ascii=False, default=str))
        _hlog(
            "FAIL: No DSN available. Set POSTGRES_DSN / PG_DSN_INTERNAL / DATABASE_URL etc.",
            json_only=json_only,
        )
        return RC_FAIL

    summary["dsn_source"] = _sanitize(dsn_source)

    # Sanitize DSN
    sslmode_override = (args.sslmode or "").strip() or None
    try:
        S = _build_sanitized_pg(
            dsn,
            connect_args,
            timeout_s=timeout_s,
            app_name=str(args.app_name),
            sslmode_override=sslmode_override,
            insecure_ssl=bool(int(args.insecure)),
        )
    except Exception as e:
        summary["errors"].append(_sanitize(f"dsn_sanitize_failed:{type(e).__name__}:{e}"))
        if emit_json:
            print(json.dumps(summary, ensure_ascii=False, default=str))
        _hlog(f"FAIL: DSN sanitize failed: {_short_exc(e)}", json_only=json_only)
        return RC_FAIL

    summary["dsn_masked"] = _mask_dsn_password(S.raw_dsn)
    summary["sa_url_masked"] = _mask_dsn_password(S.sa_url)
    summary["libpq_url_masked"] = _mask_dsn_password(S.libpq_url)
    summary["host"] = _sanitize(S.host)
    summary["port"] = int(S.port)
    summary["db"] = _sanitize(S.db)
    summary["sslmode"] = _sanitize(S.sslmode)
    summary["ssl_enabled"] = int(bool(S.ssl_enabled))

    if not json_only:
        _hlog("──────────────────────────────────────────────", json_only=False)
        _hlog(" V3+++ smoke_pg_only — Postgres DSN sanity     ", json_only=False)
        _hlog("──────────────────────────────────────────────", json_only=False)
        _hlog(f"dsn_source={summary['dsn_source']} dsn={summary['dsn_masked']}", json_only=False)
        _hlog(f"sa_url={summary['sa_url_masked']}", json_only=False)
        _hlog(f"libpq_url={summary['libpq_url_masked']}", json_only=False)
        _hlog(f"host={summary['host']} port={summary['port']} db={summary['db']}", json_only=False)
        _hlog(
            f"sslmode={summary['sslmode'] or '(none)'} ssl_enabled={bool(S.ssl_enabled)}",
            json_only=False,
        )

        keys = sorted([k for k in S.connect_args if k != "ssl"])
        _hlog(
            f"connect_args keys={keys} ssl={'yes' if 'ssl' in S.connect_args else 'no'}",
            json_only=False,
        )

        if not _asyncpg_is_usable():
            _hlog(
                "NOTE: asyncpg not usable here; using psycopg fallback if available.",
                json_only=False,
            )

    # Optional probes
    if int(args.no_probe) == 0 and S.host:
        ips = _dns_ips(S.host, S.port)
        ok, why = _tcp_probe(S.host, S.port, timeout_s=min(3.0, timeout_s))
        summary["probes"]["dns_ips"] = ips[:20]
        summary["probes"]["tcp_ok"] = int(bool(ok))
        summary["probes"]["tcp_err"] = None if ok else _sanitize(why)
        if debug and not json_only:
            _hlog(f"probe dns_ips={ips if ips else '(none)'}", json_only=False)
            _hlog(f"probe tcp_ok={ok} tcp_err={why if not ok else ''}", json_only=False)

    # Attempts
    start_ms = _now_ms()

    async def _run_attempts() -> int:
        last_err: str | None = None
        for i in range(1, tries + 1):
            t0 = _now_ms()
            try:
                if not json_only:
                    _hlog(f"TRY {i}/{tries} timeout={timeout_s}s", json_only=False)

                res = await asyncio.wait_for(
                    _select1_with_best_driver(S, timeout_s=timeout_s, app_name=str(args.app_name)),
                    timeout=timeout_s + 2.0,
                )
                dt = _now_ms() - t0
                summary["attempts"].append(
                    {
                        "i": i,
                        "ok": True,
                        "ms": int(dt),
                        "select1": res.get("select1"),
                        "driver": res.get("driver"),
                    }
                )
                summary["driver"] = res.get("driver")
                if not json_only:
                    _hlog(
                        f"OK: SELECT 1 -> {res.get('select1')} in {dt}ms (driver={res.get('driver')})",
                        json_only=False,
                    )

                summary["ok"] = True
                return RC_OK

            except Exception as e:
                dt = _now_ms() - t0
                last_err = _sanitize(f"{type(e).__name__}: {e}")
                summary["attempts"].append({"i": i, "ok": False, "ms": int(dt), "err": last_err})
                if not json_only:
                    _hlog(f"FAIL {i}/{tries}: {last_err}", json_only=False)
                await asyncio.sleep(0.6)

        if last_err:
            summary["errors"].append(f"all_attempts_failed:last={last_err}")
        return RC_FAIL

    try:
        if total_timeout_s > 0:
            await asyncio.wait_for(_run_attempts(), timeout=float(total_timeout_s))
        else:
            await _run_attempts()
    except TimeoutError:
        summary["errors"].append("total_timeout")
    finally:
        summary["elapsed_ms"] = int(_now_ms() - start_ms)

        all_msgs = " ".join(
            [str(x) for x in summary["errors"]]
            + [str(x.get("err", "")) for x in summary["attempts"]]
        ).lower()
        if "ssl" in all_msgs and ("certificate" in all_msgs or "verify" in all_msgs):
            summary["warnings"].append(
                "TLS verify failed. Check sslmode, CA bundle, or set PG_SSL_INSECURE=1 for debug only."
            )
        if "timeout" in all_msgs:
            summary["warnings"].append(
                "Timeouts detected. Increase --timeout/SMOKE_TIMEOUT_S or verify network path."
            )
        if "getaddrinfo" in all_msgs or "dns" in all_msgs:
            summary["warnings"].append(
                "DNS issue detected. Verify runtime DNS or host correctness."
            )
        if "asyncpg" in all_msgs:
            summary["warnings"].append(
                "asyncpg appears broken in this environment. Prefer psycopg[binary] or pin Python to 3.12."
            )

        if emit_json:
            print(json.dumps(summary, ensure_ascii=False, default=str))
        else:
            _hlog(
                ("OK" if summary.get("ok") else "FAIL")
                + f" elapsed_ms={summary.get('elapsed_ms')}",
                json_only=json_only,
            )

    return RC_OK if bool(summary.get("ok")) else RC_FAIL


def main(argv: list[str] | None = None) -> None:
    try:
        rc = asyncio.run(amain(argv))
    except KeyboardInterrupt:
        rc = RC_CTRL_C
    except Exception:
        err = _sanitize(traceback.format_exc())
        if _envb("SMOKE_JSON", False) or _envb("SMOKE_JSON_ONLY", False):
            print(
                json.dumps(
                    {"ts": _ts(), "ok": False, "step": "smoke_pg_only", "error": err[:900]},
                    ensure_ascii=False,
                )
            )
        else:
            _safe_write(sys.stderr, f"[smoke_pg_only] {_ts()} FAIL unhandled\n{err[:1800]}\n")
        rc = RC_FAIL
    raise SystemExit(int(rc))


if __name__ == "__main__":
    with contextlib.suppress(Exception):
        if os.name == "nt" and hasattr(asyncio, "WindowsSelectorEventLoopPolicy"):
            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())  # type: ignore[attr-defined]
    main()
