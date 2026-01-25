from __future__ import annotations

"""
scripts/test_pg.py

God-Tier PG / DSN Canary (READ ONLY, PROD-LOCKED) — P0 DSN BASELAYER
===================================================================

What this does
--------------
A zero-risk Postgres canary that proves connectivity + schema sanity WITHOUT modifying data.

Hard guarantees
---------------
- No writes: always runs inside explicit READ ONLY transaction.
- Never prints secrets: DSNs are masked; errors are sanitized.
- json-only contract: when --json-only, stdout contains ONLY the final JSON object.

Key fixes vs previous version
-----------------------------
- No more "unhandled asyncpg.protocol.protocol" crashes:
  * asyncpg is treated as optional, and we detect "broken asyncpg" safely.
- Exit codes normalized repo-clean:
  * 0 = OK / SKIP
  * 1 = FAIL (any reason, including missing deps, missing tables, no DSN, timeouts)
  * 130 = Ctrl-C

Driver selection (PG_TEST_DRIVER)
---------------------------------
auto (default) -> prefer asyncpg via SQLAlchemy if healthy; else psycopg/psycopg2 fallback.
asyncpg        -> force SQLAlchemy+asyncpg path
psycopg        -> force psycopg3 sync path
psycopg2       -> force psycopg2 sync path

Env (high-signal)
-----------------
SKIP_DSN_REQUIRED=0|1

REQUIRED_TABLES="trades,positions,ai_features"
PG_TEST_SCHEMA="public"

PG_TEST_TIMEOUT_S=15
PG_TEST_CONNECT_TIMEOUT_S=8
PG_TEST_TRY_ALL=0|1

PG_TEST_JSON=1|0          (default 1)
PG_TEST_JSON_ONLY=0|1
PG_TEST_VERBOSE=0|1

PG_TEST_RETRIES=2
PG_TEST_RETRY_BASE_S=0.35

PG_TEST_TCP_PROBE=1|0     (default 1)
PG_TEST_DNS_DEBUG=1|0     (default 1)

PG_TEST_STATEMENT_TIMEOUT_MS=8000
PG_TEST_LOCK_TIMEOUT_MS=5000
PG_TEST_IDLE_TX_TIMEOUT_MS=10000

Dotenv (local dev convenience)
------------------------------
PG_TEST_NO_DOTENV=1   (disable dotenv auto-load)

CLI
---
py scripts/test_pg.py --json-only
py scripts/test_pg.py --try-all --schema public --required-tables trades,positions
py scripts/test_pg.py --dsn "postgresql://..." --json-only
"""

import argparse
import asyncio
import contextlib
import inspect
import json
import os
import platform
import random
import re
import socket
import sys
import time
from dataclasses import dataclass
from typing import Any
from urllib.parse import parse_qs, urlparse

# =============================================================================
# exit codes (repo-clean)
# =============================================================================
RC_OK = 0
RC_FAIL = 1
RC_CTRL_C = 130

_TRUE = {"1", "true", "t", "yes", "y", "on"}
_FALSE = {"0", "false", "f", "no", "n", "off"}

_DSN_KEYS = (
    "POSITIONS_PG_DSN",
    "SNIPER_DB_DSN",
    "DATABASE_URL",
    "POSTGRES_DSN",
    "PG_DSN",
    "PG_DSN_INTERNAL",
    "PG_DSN_EXTERNAL",
    "PNL_DSN",
)

CONTRACT = {
    "script": "scripts.test_pg",
    "version": "v4",
    "exit_codes": {"ok": 0, "fatal": 1, "ctrl_c": 130},
    "no_writes": True,
    "stdout_json_only_when_json_only": True,
}


# =============================================================================
# repo-root bootstrap
# =============================================================================
def _bootstrap_repo_root() -> str:
    try:
        here = os.path.abspath(__file__)
    except Exception:
        return ""

    d = os.path.dirname(here)
    cur = d
    markers = ("pyproject.toml", "requirements.txt", "setup.cfg", ".git")
    for _ in range(10):
        try:
            if any(os.path.exists(os.path.join(cur, m)) for m in markers) or os.path.isdir(
                os.path.join(cur, "src")
            ):
                if cur and cur not in sys.path:
                    sys.path.insert(0, cur)
                os.environ.setdefault("REPO_ROOT", cur)
                return cur
        except Exception:
            pass
        nxt = os.path.dirname(cur)
        if nxt == cur:
            break
        cur = nxt

    root = os.path.abspath(os.path.join(d, ".."))
    if root and root not in sys.path:
        sys.path.insert(0, root)
    os.environ.setdefault("REPO_ROOT", root)
    return root


def _maybe_load_dotenv() -> None:
    if _envb("PG_TEST_NO_DOTENV", False):
        return
    with contextlib.suppress(Exception):
        from dotenv import load_dotenv  # type: ignore

        load_dotenv(override=False)


# =============================================================================
# env helpers
# =============================================================================
def _envs(k: str, d: str = "") -> str:
    v = os.getenv(k)
    s = (v if v is not None else d) if d is not None else (v or "")
    s = str(s).strip()
    if (s.startswith('"') and s.endswith('"')) or (s.startswith("'") and s.endswith("'")):
        s = s[1:-1].strip()
    return s


def _envb(k: str, d: bool = False) -> bool:
    v = _envs(k, "")
    if v == "":
        return bool(d)
    s = v.strip().lower()
    if s in _TRUE:
        return True
    if s in _FALSE:
        return False
    return bool(d)


def _envi(k: str, d: int) -> int:
    try:
        return int(float(_envs(k, str(d)) or str(d)))
    except Exception:
        return int(d)


def _envf(k: str, d: float) -> float:
    try:
        return float(_envs(k, str(d)) or str(d))
    except Exception:
        return float(d)


def _split_csv(s: str) -> list[str]:
    return [p.strip() for p in (s or "").replace(";", ",").split(",") if p.strip()]


def _ts() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


# =============================================================================
# safe output + redaction
# =============================================================================
_URL_CRED_RE = re.compile(r"(://)([^/@\s]+)@")
_LONG_TOKENISH_RE = re.compile(r"\b[A-Za-z0-9_\-]{28,}\b")
_KV_SECRET_RE = re.compile(
    r"(?i)\b(api[_-]?key|token|secret|password|pass)\b(\s*[:=]\s*)([^\s,;]+)"
)


def _safe_write_line(f, s: str) -> None:
    try:
        f.write(s + ("" if s.endswith("\n") else "\n"))
        f.flush()
    except UnicodeEncodeError:
        enc = getattr(f, "encoding", None) or "utf-8"
        b = s.encode(enc, errors="replace")
        with contextlib.suppress(Exception):
            f.buffer.write(b + b"\n")  # type: ignore[attr-defined]
            f.buffer.flush()  # type: ignore[attr-defined]
    except Exception:
        pass


def _hlog(msg: str, *, json_only: bool) -> None:
    if json_only:
        return
    _safe_write_line(sys.stderr, f"[test_pg] {_ts()} {msg}")


def _sanitize(x: Any) -> str:
    if x is None:
        return ""
    out = str(x)
    out = _URL_CRED_RE.sub(r"\1***@", out)
    out = _KV_SECRET_RE.sub(r"\1\2***", out)
    for k in (
        "DATABASE_URL",
        "POSTGRES_DSN",
        "PG_DSN",
        "PG_DSN_INTERNAL",
        "PG_DSN_EXTERNAL",
        "PNL_DSN",
        "REDIS_URL",
        "REDIS_TLS_URL",
        "NOTION_TOKEN",
        "TELEGRAM_BOT_TOKEN",
        "OPENAI_API_KEY",
        "WALLET_PRIVATE_KEY",
        "KEYPAIR_B64",
    ):
        v = os.getenv(k, "")
        if v and len(v) >= 6:
            out = out.replace(v, "***")
    out = _LONG_TOKENISH_RE.sub("***", out)
    return out


def _mask_url(u: str) -> str:
    if not u:
        return ""
    out = _URL_CRED_RE.sub(r"\1***@", str(u))
    out = _KV_SECRET_RE.sub(r"\1\2***", out)
    if "?" in out:
        out = out.split("?", 1)[0] + "?..."
    return out


# =============================================================================
# diagnostics
# =============================================================================
def _dns_debug(host: str, port: int) -> list[str]:
    try:
        infos = socket.getaddrinfo(host, port, proto=socket.IPPROTO_TCP)
        ips: list[str] = []
        for _, _, _, _, sockaddr in infos:
            ip = sockaddr[0]
            if ip not in ips:
                ips.append(ip)
        return ips
    except Exception:
        return []


def _tcp_probe(host: str, port: int, timeout_s: float) -> tuple[bool, str]:
    try:
        with socket.create_connection((host, port), timeout=timeout_s):
            return True, "ok"
    except Exception as e:
        return False, f"{type(e).__name__}: {e}"


# =============================================================================
# error classification + hints
# =============================================================================
def _classify_error(msg: str) -> str:
    m = (msg or "").lower()
    if not m:
        return "other"
    if "attempt_timeout" in m:
        return "timeout"
    if "timed out" in m or "timeout" in m:
        return "timeout"
    if (
        "getaddrinfo failed" in m
        or "could not translate host name" in m
        or "name or service not known" in m
    ):
        return "dns"
    if (
        "password authentication failed" in m
        or "authentication failed" in m
        or "no pg_hba.conf entry" in m
    ):
        return "auth"
    if "ssl" in m and ("handshake" in m or "certificate" in m or "required" in m or "alert" in m):
        return "ssl"
    if "connection refused" in m:
        return "refused"
    if "too many connections" in m:
        return "over_capacity"
    if "missing_deps" in m:
        return "missing_deps"
    if "missing_tables" in m:
        return "missing_tables"
    return "other"


def _fix_hints(
    results: list[dict[str, Any]], *, schema: str, required_tables: list[str]
) -> list[str]:
    kinds = [_classify_error(str(r.get("error", ""))) for r in results if not r.get("ok")]

    hints: list[str] = []
    if "missing_deps" in kinds:
        hints.append(
            "Missing/broken DB driver. On Python 3.13, asyncpg is often unavailable. "
            'Install psycopg3 binary: py -m pip install "psycopg[binary]"'
        )
    if "dns" in kinds:
        hints.append("DNS failed for the DB host. Check outbound DNS/network/VPN.")
    if "refused" in kinds:
        hints.append("Port refused. DB may block your IP/runtime or external access is disabled.")
    if "timeout" in kinds:
        hints.append("Timed out. Increase PG_TEST_TIMEOUT_S and/or PG_TEST_CONNECT_TIMEOUT_S.")
    if "ssl" in kinds:
        hints.append("SSL/TLS problem. Ensure sslmode=require/verify-* and correct cert policy.")
    if "auth" in kinds:
        hints.append("Auth failed. Verify username/password and DSN from Render dashboard.")
    if any(r.get("missing_tables") for r in results) and required_tables:
        hints.append(
            f"Connected but required tables missing in schema '{schema}'. Run schema/migrations."
        )
    if not hints:
        hints.append("No candidate connected. Verify DSN(s), network egress, and DB policy.")

    out: list[str] = []
    seen = set()
    for h in hints:
        if h not in seen:
            seen.add(h)
            out.append(h)
    return out[:10]


# =============================================================================
# retry policy
# =============================================================================
def _retry_cfg() -> tuple[int, float]:
    retries = max(0, _envi("PG_TEST_RETRIES", 2))
    base = max(0.05, float(_envf("PG_TEST_RETRY_BASE_S", 0.35)))
    return retries, base


async def _sleep_backoff(attempt: int, base: float) -> None:
    cap = 2.5
    t = min(cap, base * (1.6 ** max(0, attempt)))
    t = t * (0.85 + random.random() * 0.35)
    await asyncio.sleep(t)


# =============================================================================
# candidates
# =============================================================================
def _candidate_dsns(cli_dsns: list[str]) -> list[tuple[str, str]]:
    out: list[tuple[str, str]] = []
    for i, d in enumerate(cli_dsns):
        dd = (d or "").strip()
        if dd:
            out.append((f"CLI_{i + 1}", dd))

    for k in (
        "PNL_DSN",
        "POSTGRES_DSN",
        "PG_DSN",
        "DATABASE_URL",
        "PG_DSN_INTERNAL",
        "PG_DSN_EXTERNAL",
        "SNIPER_DB_DSN",
        "POSITIONS_PG_DSN",
    ):
        v = _envs(k, "")
        if v:
            out.append((k, v))

    seen = set()
    uniq: list[tuple[str, str]] = []
    for lbl, raw in out:
        key = raw.strip()
        if not key or key in seen:
            continue
        seen.add(key)
        uniq.append((lbl, key))
    return uniq


@contextlib.contextmanager
def _temp_env(overrides: dict[str, str | None]) -> Any:
    old: dict[str, str | None] = {}
    for k, v in overrides.items():
        old[k] = os.getenv(k)
        if v is None:
            os.environ.pop(k, None)
        else:
            os.environ[k] = v
    try:
        yield
    finally:
        for k, prev in old.items():
            if prev is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = prev


# =============================================================================
# db_dsn bridge (optional, signature tolerant)
# =============================================================================
def _filter_kwargs(fn: Any, kwargs: dict[str, Any]) -> dict[str, Any]:
    try:
        sig = inspect.signature(fn)
        params = sig.parameters.values()
        if any(p.kind == inspect.Parameter.VAR_KEYWORD for p in params):
            return kwargs
        allowed = {p.name for p in params}
        return {k: v for k, v in kwargs.items() if k in allowed}
    except Exception:
        return kwargs


def _get_attr(o: Any, name: str, default: Any = None) -> Any:
    if o is None:
        return default
    if isinstance(o, dict):
        return o.get(name, default)
    return getattr(o, name, default)


def _strip_sa_driver(dsn: str) -> str:
    s = (dsn or "").strip()
    if s.startswith("postgres://"):
        s = "postgresql://" + s[len("postgres://") :]
    if s.startswith("postgresql+asyncpg://"):
        s = "postgresql://" + s[len("postgresql+asyncpg://") :]
    return s


def _ensure_asyncpg_sa(dsn: str) -> str:
    s = _strip_sa_driver(dsn)
    if s.startswith("postgresql://"):
        return "postgresql+asyncpg://" + s[len("postgresql://") :]
    return s


@dataclass
class AttemptPlan:
    label: str
    raw: str
    dsn_sync: str
    dsn_sa_asyncpg: str
    connect_args: dict[str, Any]
    host: str
    port: int
    db: str
    sslmode: str
    ssl_enabled: bool


def _build_plan(
    dsnmod: Any | None,
    label: str,
    raw: str,
    *,
    app_name: str,
    connect_timeout_s: float,
) -> AttemptPlan:
    # Fallback parse (works even if db_dsn is unavailable)
    raw_sync = _strip_sa_driver(raw)
    raw_sa = _ensure_asyncpg_sa(raw_sync)
    u = urlparse(raw_sync)
    host = u.hostname or ""
    port = int(u.port or 5432)
    db = (u.path or "").lstrip("/").split("/", 1)[0] if u.path else ""

    sslmode = ""
    ssl_enabled = False
    connect_args: dict[str, Any] = {}
    if connect_timeout_s > 0:
        connect_args["timeout"] = float(connect_timeout_s)
    # God-tier polish: if db_dsn isn't available, still infer sslmode from DSN query params
    with contextlib.suppress(Exception):
        qs = parse_qs(u.query or "")
        sslmode_q = (qs.get("sslmode", [""])[0] or "").strip()
        if sslmode_q:
            sslmode = sslmode_q
        # conservative: treat anything other than disable/allow/prefer as SSL "enabled"
        if sslmode and sslmode.lower() not in {"disable", "allow", "prefer"}:
            ssl_enabled = True

    # If db_dsn exists, use it as source of truth for SSLContext/connect_args where possible
    if dsnmod is not None:
        overrides = {k: None for k in _DSN_KEYS}
        overrides["POSTGRES_DSN"] = raw_sync
        with _temp_env(overrides):
            pickers = [
                "pick_pg_dsn_debug",
                "pick_pg_dsn",
                "resolve_pg_dsn_debug",
                "resolve_pg_dsn",
                "get_pg_dsn_debug",
                "get_pg_dsn",
            ]
            pr = None
            for nm in pickers:
                fn = getattr(dsnmod, nm, None)
                if callable(fn):
                    kw = _filter_kwargs(fn, {"app_name": app_name})
                    pr = fn(**kw)
                    break
            if pr is not None:
                dsn_sa = _get_attr(pr, "dsn_sa", "") or _get_attr(pr, "dsn", "") or ""
                if dsn_sa:
                    raw_sa = _ensure_asyncpg_sa(str(dsn_sa))
                    raw_sync = _strip_sa_driver(str(dsn_sa))

                ca = _get_attr(pr, "connect_args", None)
                if isinstance(ca, dict):
                    connect_args.update(ca)

                host = str(_get_attr(pr, "host", host) or host)
                sslmode = str(_get_attr(pr, "sslmode", "") or "")
                ssl_enabled = bool(
                    _get_attr(pr, "ssl_enabled", False) or bool(connect_args.get("ssl"))
                )

                uu = urlparse(raw_sync)
                host = uu.hostname or host
                port = int(uu.port or port)
                db = (uu.path or "").lstrip("/").split("/", 1)[0] if uu.path else db

    return AttemptPlan(
        label=label,
        raw=raw,
        dsn_sync=raw_sync,
        dsn_sa_asyncpg=raw_sa,
        connect_args=dict(connect_args),
        host=host,
        port=port,
        db=db,
        sslmode=sslmode,
        ssl_enabled=ssl_enabled,
    )


# =============================================================================
# driver health + canary implementations
# =============================================================================
def _is_asyncpg_healthy() -> tuple[bool, str]:
    try:
        import asyncpg  # noqa: F401
        import asyncpg.protocol.protocol  # noqa: F401

        return True, "ok"
    except Exception as e:
        return False, f"{type(e).__name__}: {e}"


async def _canary_sqlalchemy_asyncpg(
    plan: AttemptPlan,
    *,
    schema: str,
    required_tables: list[str],
    timeout_s: float,
    statement_timeout_ms: int,
    lock_timeout_ms: int,
    idle_tx_timeout_ms: int,
) -> tuple[bool, dict[str, Any], list[str]]:
    # Import SQLAlchemy lazily
    try:
        from sqlalchemy import text  # type: ignore
        from sqlalchemy.ext.asyncio import create_async_engine  # type: ignore
        from sqlalchemy.pool import NullPool  # type: ignore
    except Exception as e:
        raise RuntimeError(f"missing_deps:sqlalchemy_import:{type(e).__name__}: {e}") from e

    # Create engine safely (asyncpg may be broken and must not crash unhandled)
    try:
        eng = create_async_engine(
            plan.dsn_sa_asyncpg,
            connect_args=dict(plan.connect_args or {}),
            poolclass=NullPool,
        )
    except Exception as e:
        raise RuntimeError(f"missing_deps:asyncpg_broken:{type(e).__name__}: {e}") from e

    missing: list[str] = []
    details: dict[str, Any] = {}

    async def _check_tables(conn: Any) -> list[str]:
        if not required_tables:
            return []
        safe: list[str] = []
        for t in required_tables:
            tt = (t or "").strip()
            if not tt:
                continue
            if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", tt):
                raise RuntimeError(f"bad_format:unsafe_table_name:{tt!r}")
            safe.append(tt)
        if not safe:
            return []
        ph = ", ".join([f":t{i}" for i in range(len(safe))])
        params: dict[str, Any] = {"schema": schema}
        for i, tt in enumerate(safe):
            params[f"t{i}"] = tt
        q = text(
            f"""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = :schema
              AND table_name IN ({ph})
            """
        )
        rows = (await conn.execute(q, params)).fetchall()
        present = {str(r[0]) for r in rows}
        return [tt for tt in safe if tt not in present]

    try:
        async with eng.connect() as conn:
            trans = await conn.begin()
            try:
                await conn.exec_driver_sql("SET TRANSACTION READ ONLY")

                with contextlib.suppress(Exception):
                    await conn.exec_driver_sql(
                        f"SET statement_timeout = {max(1000, int(statement_timeout_ms))}"
                    )
                with contextlib.suppress(Exception):
                    await conn.exec_driver_sql(
                        f"SET lock_timeout = {max(500, int(lock_timeout_ms))}"
                    )
                with contextlib.suppress(Exception):
                    await conn.exec_driver_sql(
                        f"SET idle_in_transaction_session_timeout = {max(1000, int(idle_tx_timeout_ms))}"
                    )

                ro = (await conn.execute(text("SHOW transaction_read_only"))).scalar()
                ro_s = str(ro).strip().lower() if ro is not None else ""
                if ro_s not in ("on", "true", "1"):
                    raise RuntimeError("read_only_enforcement_failed")

                ok1 = (await conn.execute(text("SELECT 1"))).scalar()
                if int(ok1 or 0) != 1:
                    raise RuntimeError("select1_failed")

                meta = (await conn.execute(text("SELECT now(), version()"))).first()
                who = (await conn.execute(text("SELECT current_database(), current_user"))).first()
                details = {
                    "now": str(meta[0]) if meta else "",
                    "version": (str(meta[1]).splitlines()[0] if meta and meta[1] else ""),
                    "db": str(who[0]) if who else "",
                    "user": str(who[1]) if who else "",
                    "transaction_read_only": ro_s,
                }

                missing = await _check_tables(conn)
            finally:
                with contextlib.suppress(Exception):
                    await trans.rollback()
    finally:
        with contextlib.suppress(Exception):
            await eng.dispose()

    return (len(missing) == 0), details, missing


def _canary_psycopg_sync(
    dsn: str,
    *,
    schema: str,
    required_tables: list[str],
    connect_timeout_s: float,
    statement_timeout_ms: int,
    lock_timeout_ms: int,
    idle_tx_timeout_ms: int,
    app_name: str,
    force_lib: str,
) -> tuple[bool, dict[str, Any], list[str], str]:
    # force_lib: "psycopg" or "psycopg2" or "auto"
    lib_used = "none"
    conn = None
    cur = None
    missing: list[str] = []
    details: dict[str, Any] = {}

    dsn_use = _strip_sa_driver(dsn)

    try:
        psycopg_mod = None
        psycopg2_mod = None

        if force_lib in ("auto", "psycopg"):
            with contextlib.suppress(Exception):
                import psycopg  # type: ignore

                psycopg_mod = psycopg

        if psycopg_mod is None and force_lib in ("auto", "psycopg2"):
            with contextlib.suppress(Exception):
                import psycopg2  # type: ignore

                psycopg2_mod = psycopg2

        if psycopg_mod is None and psycopg2_mod is None:
            raise RuntimeError("missing_deps:psycopg_not_installed")

        if psycopg_mod is not None:
            lib_used = "psycopg"
            conn = psycopg_mod.connect(
                dsn_use,
                connect_timeout=int(max(1.0, connect_timeout_s)),
                application_name=app_name,
            )
            cur = conn.cursor()
        else:
            lib_used = "psycopg2"
            conn = psycopg2_mod.connect(
                dsn_use,
                connect_timeout=int(max(1.0, connect_timeout_s)),
                application_name=app_name,
            )
            cur = conn.cursor()

        # Begin + enforce READ ONLY
        conn.autocommit = False
        cur.execute("BEGIN")
        cur.execute("SET TRANSACTION READ ONLY")

        # Timeouts (best effort)
        with contextlib.suppress(Exception):
            cur.execute(f"SET statement_timeout = {max(1000, int(statement_timeout_ms))}")
        with contextlib.suppress(Exception):
            cur.execute(f"SET lock_timeout = {max(500, int(lock_timeout_ms))}")
        with contextlib.suppress(Exception):
            cur.execute(
                f"SET idle_in_transaction_session_timeout = {max(1000, int(idle_tx_timeout_ms))}"
            )

        cur.execute("SHOW transaction_read_only")
        ro = cur.fetchone()
        ro_s = str(ro[0]).strip().lower() if ro and ro[0] is not None else ""
        if ro_s not in ("on", "true", "1"):
            raise RuntimeError("read_only_enforcement_failed")

        cur.execute("SELECT 1")
        one = cur.fetchone()
        if not one or int(one[0]) != 1:
            raise RuntimeError("select1_failed")

        cur.execute("SELECT now(), version()")
        meta = cur.fetchone()
        cur.execute("SELECT current_database(), current_user")
        who = cur.fetchone()

        details = {
            "now": str(meta[0]) if meta else "",
            "version": (str(meta[1]).splitlines()[0] if meta and meta[1] else ""),
            "db": str(who[0]) if who else "",
            "user": str(who[1]) if who else "",
            "transaction_read_only": ro_s,
        }

        # Required tables check
        safe: list[str] = []
        for t in required_tables:
            tt = (t or "").strip()
            if not tt:
                continue
            if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", tt):
                raise RuntimeError(f"bad_format:unsafe_table_name:{tt!r}")
            safe.append(tt)

        if safe:
            placeholders = ",".join(["%s"] * len(safe))
            q = (
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = %s AND table_name IN (" + placeholders + ")"
            )
            cur.execute(q, [schema, *safe])
            rows = cur.fetchall()
            present = {str(r[0]) for r in (rows or [])}
            missing = [tt for tt in safe if tt not in present]

        return (len(missing) == 0), details, missing, lib_used

    finally:
        with contextlib.suppress(Exception):
            if conn is not None:
                conn.rollback()
        with contextlib.suppress(Exception):
            if cur is not None:
                cur.close()
        with contextlib.suppress(Exception):
            if conn is not None:
                conn.close()


# =============================================================================
# main runner
# =============================================================================
async def _amain(argv: list[str]) -> int:
    ap = argparse.ArgumentParser(description="God-Tier Postgres canary (READ ONLY).")
    ap.add_argument("--dsn", action="append", default=[], help="Override DSN (repeatable).")
    ap.add_argument("--schema", default="", help="Schema for REQUIRED_TABLES.")
    ap.add_argument("--required-tables", default="", help="CSV list of required tables.")
    ap.add_argument("--timeout", type=float, default=None, help="Per-candidate timeout seconds.")
    ap.add_argument("--connect-timeout", type=float, default=None, help="Connect timeout seconds.")
    ap.add_argument("--try-all", action="store_true", help="Try all candidates.")
    ap.add_argument("--json", action="store_true", help="Emit JSON summary.")
    ap.add_argument("--json-only", action="store_true", help="Only JSON output to stdout.")
    ap.add_argument("--verbose", action="store_true", help="Verbose human logs.")
    ap.add_argument(
        "--app-name",
        default="",
        help="Override PG app_name (default: PG_APP_NAME or pnlsniper_test_pg).",
    )
    ap.add_argument("--no-tcp-probe", action="store_true", help="Disable TCP probe.")
    ap.add_argument("--no-dns-debug", action="store_true", help="Disable DNS debug.")
    args = ap.parse_args(argv)

    json_only = bool(args.json_only or _envb("PG_TEST_JSON_ONLY", False))
    emit_json = bool(
        args.json
        or args.json_only
        or _envb("PG_TEST_JSON", True)
        or _envb("PG_TEST_JSON_ONLY", False)
    )
    verbose = bool(args.verbose or _envb("PG_TEST_VERBOSE", False))

    if _envb("SKIP_DSN_REQUIRED", False):
        summary = {
            "ts": _ts(),
            "ok": True,
            "skipped": True,
            "exit_code": RC_OK,
            "reason": "SKIP_DSN_REQUIRED=1",
            "contract": CONTRACT,
        }
        if emit_json:
            print(json.dumps(summary, ensure_ascii=False, default=str))
        _hlog("SKIP (SKIP_DSN_REQUIRED=1)", json_only=json_only)
        return RC_OK

    schema = (args.schema or _envs("PG_TEST_SCHEMA", "public") or "public").strip() or "public"
    required_tables = _split_csv(args.required_tables or _envs("REQUIRED_TABLES", ""))

    timeout_s = max(
        4.0, float(args.timeout if args.timeout is not None else _envf("PG_TEST_TIMEOUT_S", 15.0))
    )
    connect_timeout_s = max(
        2.0,
        float(
            args.connect_timeout
            if args.connect_timeout is not None
            else _envf("PG_TEST_CONNECT_TIMEOUT_S", 8.0)
        ),
    )
    try_all = bool(args.try_all or _envb("PG_TEST_TRY_ALL", False))

    tcp_probe = _envb("PG_TEST_TCP_PROBE", True) and not args.no_tcp_probe
    dns_debug = _envb("PG_TEST_DNS_DEBUG", True) and not args.no_dns_debug

    statement_timeout_ms = _envi("PG_TEST_STATEMENT_TIMEOUT_MS", 8000)
    lock_timeout_ms = _envi("PG_TEST_LOCK_TIMEOUT_MS", 5000)
    idle_tx_timeout_ms = _envi("PG_TEST_IDLE_TX_TIMEOUT_MS", 10000)

    app_name = (
        args.app_name or _envs("PG_APP_NAME", "pnlsniper_test_pg") or "pnlsniper_test_pg"
    ).strip()

    driver_pref = (_envs("PG_TEST_DRIVER", "auto") or "auto").strip().lower()
    if driver_pref not in ("auto", "asyncpg", "psycopg", "psycopg2"):
        driver_pref = "auto"

    cli_dsns = [d for d in (args.dsn or []) if d]
    cands = _candidate_dsns(cli_dsns)
    if not cands:
        summary = {
            "ts": _ts(),
            "ok": False,
            "skipped": False,
            "exit_code": RC_FAIL,
            "error": "no_dsn_candidates",
            "detail": "Set one of PNL_DSN/POSTGRES_DSN/PG_DSN/DATABASE_URL/PG_DSN_INTERNAL/PG_DSN_EXTERNAL or pass --dsn",
            "contract": CONTRACT,
        }
        if emit_json:
            print(json.dumps(summary, ensure_ascii=False, default=str))
        _hlog("FAIL no DSN candidates", json_only=json_only)
        return RC_FAIL

    # Import db_dsn as best-effort (must never crash the script)
    dsnmod = None
    with contextlib.suppress(Exception):
        from utils_pkg import db_dsn as dsnmod  # type: ignore

    asyncpg_ok, asyncpg_reason = _is_asyncpg_healthy()

    if not json_only:
        _hlog("────────────────────────────────────", json_only=False)
        _hlog("God-Tier test_pg (READ ONLY)", json_only=False)
        _hlog("────────────────────────────────────", json_only=False)
        _hlog(
            f"timeout_s={timeout_s:.1f} connect_timeout_s={connect_timeout_s:.1f} schema={schema} "
            f"required_tables={','.join(required_tables) if required_tables else '-'} "
            f"try_all={int(try_all)} retries={_envi('PG_TEST_RETRIES', 2)} tcp_probe={int(tcp_probe)} "
            f"dns_debug={int(dns_debug)} verbose={int(verbose)} driver={driver_pref} asyncpg_ok={int(asyncpg_ok)}",
            json_only=False,
        )
        if not asyncpg_ok:
            _hlog(f"asyncpg_health={_sanitize(asyncpg_reason)}", json_only=False)

    results: list[dict[str, Any]] = []
    picked: dict[str, Any] | None = None

    retries, base = _retry_cfg()

    for label, raw in cands:
        t0 = time.monotonic()

        plan = _build_plan(
            dsnmod,
            label,
            raw,
            app_name=app_name,
            connect_timeout_s=connect_timeout_s,
        )

        attempt: dict[str, Any] = {
            "label": label,
            "ok": False,
            "driver": "",
            "dsn": _mask_url(plan.dsn_sync),
            "host": plan.host,
            "port": plan.port,
            "db_hint": plan.db,
            "sslmode": plan.sslmode,
            "ssl_enabled": int(bool(plan.ssl_enabled)),
            "ips": [],
            "tcp_probe_ok": None,
            "tcp_probe": None,
            "attempts": 0,
            "missing_tables": [],
            "details": {},
            "error": "",
            "error_kind": "",
            "sec": None,
        }

        if dns_debug and plan.host:
            attempt["ips"] = _dns_debug(plan.host, plan.port)

        if tcp_probe and plan.host:
            okp, msg = _tcp_probe(
                plan.host, plan.port, timeout_s=min(6.0, max(2.0, timeout_s * 0.4))
            )
            attempt["tcp_probe_ok"] = int(bool(okp))
            attempt["tcp_probe"] = _sanitize(msg)

        # Decide driver for this attempt
        want_asyncpg = False
        force_psycopg = "auto"
        if driver_pref == "asyncpg":
            want_asyncpg = True
        elif driver_pref == "psycopg":
            force_psycopg = "psycopg"
        elif driver_pref == "psycopg2":
            force_psycopg = "psycopg2"
        else:
            # auto
            want_asyncpg = asyncpg_ok
            force_psycopg = "auto"

        last_err: str = ""
        last_kind: str = ""

        for a in range(retries + 1):
            attempt["attempts"] = a + 1
            try:
                if want_asyncpg:
                    attempt["driver"] = "sqlalchemy+asyncpg"
                    ok, details, missing = await asyncio.wait_for(
                        _canary_sqlalchemy_asyncpg(
                            plan,
                            schema=schema,
                            required_tables=required_tables,
                            timeout_s=timeout_s,
                            statement_timeout_ms=statement_timeout_ms,
                            lock_timeout_ms=lock_timeout_ms,
                            idle_tx_timeout_ms=idle_tx_timeout_ms,
                        ),
                        timeout=timeout_s + 2.0,
                    )
                else:
                    attempt["driver"] = "psycopg_sync"
                    ok, details, missing, lib_used = await asyncio.wait_for(
                        asyncio.to_thread(
                            _canary_psycopg_sync,
                            plan.dsn_sync,
                            schema=schema,
                            required_tables=required_tables,
                            connect_timeout_s=connect_timeout_s,
                            statement_timeout_ms=statement_timeout_ms,
                            lock_timeout_ms=lock_timeout_ms,
                            idle_tx_timeout_ms=idle_tx_timeout_ms,
                            app_name=app_name,
                            force_lib=force_psycopg,
                        ),
                        timeout=timeout_s + 2.0,
                    )
                    attempt["driver"] = f"{attempt['driver']}:{lib_used}"

                attempt["details"] = {
                    **(details or {}),
                    "python": sys.version.split()[0],
                    "platform": platform.platform(),
                }
                attempt["missing_tables"] = list(missing or [])
                attempt["ok"] = bool(ok) and not attempt["missing_tables"]

                if attempt["missing_tables"]:
                    attempt["error"] = "missing_tables"
                    attempt["error_kind"] = "missing_tables"

                break

            except TimeoutError:
                last_err = "attempt_timeout"
                last_kind = "timeout"
            except Exception as e:
                msg = _sanitize(f"{type(e).__name__}: {e}")
                last_err = msg
                last_kind = _classify_error(msg)

                # If asyncpg is broken at runtime, automatically fall back to psycopg on next retry
                if want_asyncpg and "asyncpg" in msg.lower():
                    want_asyncpg = False

            attempt["error"] = last_err
            attempt["error_kind"] = last_kind

            if a < retries and last_kind in ("timeout", "ssl", "other", "missing_deps"):
                if verbose and not json_only:
                    _hlog(
                        f"retrying {label} kind={last_kind} attempt={a + 1}/{retries + 1}",
                        json_only=False,
                    )
                await _sleep_backoff(a, base)
                continue

            break

        attempt["sec"] = round(time.monotonic() - t0, 3)
        results.append(attempt)

        if not json_only:
            _hlog(
                f"RES {label} ok={int(bool(attempt['ok']))} driver={attempt['driver']} "
                f"attempts={attempt['attempts']} sec={attempt['sec']} kind={attempt.get('error_kind') or '-'}",
                json_only=False,
            )

        if attempt["ok"] and picked is None:
            picked = attempt
            if not try_all:
                break

        if attempt.get("missing_tables") and not try_all:
            break

    ok_final = bool(picked is not None)

    summary: dict[str, Any] = {
        "ts": _ts(),
        "ok": ok_final,
        "skipped": False,
        "exit_code": RC_OK if ok_final else RC_FAIL,
        "contract": CONTRACT,
        "schema": schema,
        "required_tables": required_tables,
        "driver_pref": driver_pref,
        "asyncpg_health": {"ok": int(asyncpg_ok), "detail": _sanitize(asyncpg_reason)[:500]},
        "picked": (picked.get("label") if picked else None),
        "results": results,
        "fix_hints": (
            []
            if picked is not None
            else _fix_hints(results, schema=schema, required_tables=required_tables)
        ),
    }

    if emit_json:
        print(json.dumps(summary, ensure_ascii=False, default=str))

    if ok_final:
        _hlog(f"SUMMARY ok=1 picked={summary['picked']}", json_only=json_only)
        return RC_OK

    _hlog("SUMMARY ok=0 (no candidate passed)", json_only=json_only)
    return RC_FAIL


def main() -> None:
    # Windows asyncio stability
    with contextlib.suppress(Exception):
        if os.name == "nt" and hasattr(asyncio, "WindowsSelectorEventLoopPolicy"):
            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())  # type: ignore[attr-defined]

    try:
        rc = asyncio.run(_amain(sys.argv[1:]))
    except KeyboardInterrupt:
        rc = RC_CTRL_C
    except Exception as e:
        msg = _sanitize(f"unhandled:{type(e).__name__}: {e}")[:900]
        json_only = _envb("PG_TEST_JSON_ONLY", False)
        emit_json = _envb("PG_TEST_JSON", True) or _envb("PG_TEST_JSON_ONLY", False)
        if emit_json:
            print(
                json.dumps(
                    {
                        "ts": _ts(),
                        "ok": False,
                        "skipped": False,
                        "exit_code": RC_FAIL,
                        "error": msg,
                        "contract": CONTRACT,
                    },
                    ensure_ascii=False,
                    default=str,
                )
            )
        _hlog(f"FAIL {msg}", json_only=json_only)
        rc = RC_FAIL

    raise SystemExit(int(rc))


if __name__ == "__main__":
    _bootstrap_repo_root()
    _maybe_load_dotenv()
    main()
