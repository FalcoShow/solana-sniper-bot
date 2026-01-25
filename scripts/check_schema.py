from __future__ import annotations

"""
scripts/check_schema.py — God-Tier Schema Guard (Phase 0–7 LOCK-IN)
===================================================================

Goal
----
Guarantee the Postgres schema your bot expects exists *before* anything live runs.

Hard rules
----------
- VERIFY mode by default (read-only transaction).
- NEVER drops anything.
- Only creates/upgrades when:
    ALLOW_SCHEMA_CREATE=1  OR  --allow-create
- DDL is safe/idempotent ONLY:
    CREATE SCHEMA IF NOT EXISTS
    CREATE TABLE  IF NOT EXISTS
    ALTER TABLE   ... ADD COLUMN IF NOT EXISTS
    CREATE INDEX  IF NOT EXISTS
    CREATE EXTENSION IF NOT EXISTS (optional; best-effort)
- NEVER prints secrets (DSNs always masked).

DSN brain (single source of truth)
----------------------------------
Uses utils_pkg/db_dsn.py:
- Picks/normalizes DSN
- Strips URL query params from SQLAlchemy DSN (prevents asyncpg kwargs crashes)
- Converts sslmode into a real SSLContext via connect_args["ssl"] when needed
- Internal/external preference handled by db_dsn (resolvability-aware)

Output contract (preflight-friendly)
------------------------------------
- Human logs -> STDERR (unless JSON_ONLY).
- JSON summary -> STDOUT when:
    SCHEMA_JSON=1 or SCHEMA_JSON_ONLY=1 or --json/--json-only
- JSON schema is stable and tooling-safe.

Exit codes
----------
0   OK (schema ready) / skipped
2   Missing items (verify-only OR not allowed to create)
3   Create/upgrade attempted but failed or still missing
1   Fatal / misconfigured (no DSN, missing deps, connection fatal, internal error)
124 Timeout (attempt and/or total)
"""

import argparse
import asyncio
import contextlib
import json
import os
import random
import re
import socket
import sys
import time
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any
from urllib.parse import parse_qs, urlparse

# Optional dotenv (local dev only)
try:
    from dotenv import load_dotenv  # type: ignore

    load_dotenv()
except Exception:
    pass

# Ensure repo root on sys.path
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

# Hard dependency: utils_pkg/db_dsn.py is canonical
try:
    from utils_pkg import db_dsn as dsnmod  # type: ignore
except Exception as e:  # pragma: no cover
    dsnmod = None  # type: ignore[assignment]
    _DB_DSN_IMPORT_ERR = e
else:
    _DB_DSN_IMPORT_ERR = None

# Optional dependency: SQLAlchemy async
try:
    from sqlalchemy import text  # type: ignore
    from sqlalchemy.ext.asyncio import create_async_engine  # type: ignore
except Exception as e:  # pragma: no cover
    text = None  # type: ignore
    create_async_engine = None  # type: ignore
    _SA_IMPORT_ERR = e
else:
    _SA_IMPORT_ERR = None


_TRUE = {"1", "true", "t", "yes", "y", "on"}
_FALSE = {"0", "false", "f", "no", "n", "off"}

# DSN env keys we consider when building candidates
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

# ---------------------------------------------------------------------
# env + safe io helpers
# ---------------------------------------------------------------------


def _envs(k: str, d: str = "") -> str:
    v = os.getenv(k)
    s = v if v is not None else d
    s = str(s).strip()
    # strip one layer of quotes (Render UI / .env often stores quoted strings)
    if (s.startswith('"') and s.endswith('"')) or (s.startswith("'") and s.endswith("'")):
        s = s[1:-1].strip()
    return s


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


def _envi(k: str, d: int) -> int:
    try:
        return int(float(_envs(k, str(d)) or str(d)))
    except Exception:
        return d


def _envf(k: str, d: float) -> float:
    try:
        return float(_envs(k, str(d)) or str(d))
    except Exception:
        return d


def _split_csv(s: str) -> list[str]:
    return [p.strip() for p in (s or "").replace(";", ",").split(",") if p.strip()]


def _ts() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def _safe_print(line: str, *, file) -> None:
    """Never crash on cp1252 consoles."""
    try:
        file.write(line + ("" if line.endswith("\n") else "\n"))
        file.flush()
        return
    except UnicodeEncodeError:
        enc = getattr(file, "encoding", None) or "utf-8"
        b = line.encode(enc, errors="replace")
        try:
            if hasattr(file, "buffer"):
                file.buffer.write(b + b"\n")  # type: ignore[attr-defined]
                file.buffer.flush()  # type: ignore[attr-defined]
            else:
                file.write(b.decode(enc, errors="replace") + "\n")
                file.flush()
        except Exception:
            return
    except Exception:
        return


def _hlog(msg: str, *, json_only: bool) -> None:
    if json_only:
        return
    _safe_print(f"[schema] {_ts()} {msg}", file=sys.stderr)


# ---------------------------------------------------------------------
# diagnostics + masking
# ---------------------------------------------------------------------


def _mask_dsn(raw: str) -> str:
    raw = str(raw or "")
    if dsnmod is not None and hasattr(dsnmod, "mask_dsn"):
        try:
            return str(dsnmod.mask_dsn(raw))
        except Exception:
            pass
    # fallback: strip creds + query
    try:
        u = urlparse(raw)
        host = u.hostname or ""
        port = u.port or 0
        db = (u.path or "").lstrip("/").split("/", 1)[0]
        scheme = u.scheme or "postgres"
        return f"{scheme}://***@{host}:{port}/{db}?..."
    except Exception:
        return "***"


def _resolves(host: str) -> bool:
    if not host:
        return False
    try:
        socket.getaddrinfo(host, None)
        return True
    except Exception:
        return False


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


def _dsn_sslmode(dsn: str) -> str:
    try:
        q = parse_qs(urlparse(dsn).query or "")
        v = (q.get("sslmode") or [""])[0]
        return (v or "").lower().strip()
    except Exception:
        low = (dsn or "").lower()
        return "require" if "sslmode=require" in low else ""


# ---------------------------------------------------------------------
# error classification + hints
# ---------------------------------------------------------------------


def _classify_error(msg: str) -> str:
    m = (msg or "").lower()
    if not m:
        return "other"
    if "missing_deps" in m:
        return "missing_deps"
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
    if "read_only" in m or "read-only" in m:
        return "read_only"
    if "connection was closed in the middle of operation" in m:
        return "server_closed_mid_op"
    return "other"


def _fix_hints(results: list[dict[str, Any]], *, allow_create: bool, schema: str) -> list[str]:
    kinds = [_classify_error(str(r.get("error", ""))) for r in results if not r.get("ok")]
    hints: list[str] = []

    if "missing_deps" in kinds:
        hints.append(
            "Missing deps. Install sqlalchemy[asyncio] and asyncpg, and ensure utils_pkg/db_dsn.py imports."
        )
    if "dns" in kinds:
        hints.append(
            "DNS failed for the DB host. Fix DSN host (full .render.com) or check your network/DNS."
        )
    if "timeout" in kinds:
        hints.append(
            "Timed out. Increase SCHEMA_CONNECT_TIMEOUT_S / SCHEMA_TIMEOUT_S or try another network."
        )
    if "ssl" in kinds:
        hints.append(
            "SSL/TLS issue. Prefer EXTERNAL DSN locally; INTERNAL DSN only on Render. Ensure sslmode is correct."
        )
    if "auth" in kinds:
        hints.append(
            "Auth failed. Verify DB username/password and that your runtime/IP is allowed."
        )
    if "server_closed_mid_op" in kinds:
        hints.append(
            "Server closed mid-operation. Often SSL mismatch or transient network resets; try a different DSN candidate."
        )
    if "read_only" in kinds and allow_create:
        hints.append(
            "DB appears read-only. Use a writable endpoint, or run schema create from the correct environment."
        )
    if any(r.get("missing") for r in results):
        hints.append(
            f"Schema items missing in '{schema}'. If intended schema differs, set SCHEMA_NAME or --schema."
        )
    if not hints:
        hints.append(
            "No candidate connected. Verify DSNs, network egress, and that Postgres is reachable."
        )

    out: list[str] = []
    seen = set()
    for h in hints:
        if h not in seen:
            seen.add(h)
            out.append(h)
    return out[:10]


# ---------------------------------------------------------------------
# retry policy
# ---------------------------------------------------------------------


def _retry_cfg() -> tuple[int, float]:
    retries = max(0, _envi("SCHEMA_RETRIES", 1))
    base = max(0.05, float(_envf("SCHEMA_RETRY_BASE_S", 0.35)))
    return retries, base


async def _sleep_backoff(attempt: int, base: float) -> None:
    cap = 2.5
    t = min(cap, base * (1.6 ** max(0, attempt)))
    t = t * (0.85 + random.random() * 0.35)
    await asyncio.sleep(t)


# ---------------------------------------------------------------------
# candidates + temporary env override for db_dsn
# ---------------------------------------------------------------------


def _candidate_dsns(cli_dsns: Sequence[str]) -> list[tuple[str, str]]:
    out: list[tuple[str, str]] = []
    for i, d in enumerate(cli_dsns):
        if d and str(d).strip():
            out.append((f"CLI_{i + 1}", str(d).strip()))

    # env candidates (priority-ish)
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

    # unique by raw string
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


@dataclass
class AttemptPlan:
    label: str
    raw: str
    dsn_sa: str
    connect_args: dict[str, Any]
    host: str
    port: int
    db: str
    sslmode: str
    ssl_enabled: bool
    host_resolves: int


def _build_plan(
    label: str, raw: str, *, timeout_s: float, connect_timeout_s: float, app_name: str
) -> AttemptPlan:
    """
    Build a plan for an arbitrary raw DSN via db_dsn.py, without calling its private internals.
    We do this by temporarily forcing env selection to pick this raw DSN.
    """
    if dsnmod is None:
        raise RuntimeError(f"missing_deps:utils_pkg.db_dsn import failed: {_DB_DSN_IMPORT_ERR}")

    overrides = {k: None for k in _DSN_KEYS}
    overrides["POSTGRES_DSN"] = raw  # force selection

    with _temp_env(overrides):
        pr = dsnmod.pick_pg_dsn_debug(timeout_s=timeout_s, app_name=app_name)

    if not getattr(pr, "dsn_sa", ""):
        raise RuntimeError("bad_format:empty_sanitized_dsn")

    u = urlparse(pr.dsn_sa)
    host = getattr(pr, "host", "") or (u.hostname or "")
    port = int(u.port or 5432)
    db = (u.path or "").lstrip("/").split("/", 1)[0] if u.path else ""

    ca = dict(getattr(pr, "connect_args", {}) or {})
    # asyncpg connect timeout (sqlalchemy passes connect_args to asyncpg.connect)
    if connect_timeout_s and connect_timeout_s > 0:
        ca.setdefault("timeout", float(connect_timeout_s))

    return AttemptPlan(
        label=label,
        raw=raw,
        dsn_sa=pr.dsn_sa,
        connect_args=ca,
        host=host,
        port=port,
        db=db,
        sslmode=str(getattr(pr, "sslmode", "") or ""),
        ssl_enabled=bool(getattr(pr, "ssl_enabled", False)),
        host_resolves=int(bool(host and _resolves(host))),
    )


# ---------------------------------------------------------------------
# schema plans
# ---------------------------------------------------------------------


@dataclass(frozen=True)
class TablePlan:
    name: str
    columns: list[tuple[str, str]]  # (col, ddl)
    indexes: list[str]  # CREATE INDEX IF NOT EXISTS ...

    @property
    def colmap(self) -> dict[str, str]:
        return {c: ddl for (c, ddl) in self.columns}


def _schema_profile() -> str:
    p = _envs("SCHEMA_PROFILE", "minimal").lower().strip()
    return "full" if p == "full" else "minimal"


def _default_required_tables(profile: str) -> list[str]:
    if profile == "full":
        return [
            "bot_meta",
            "trades",
            "positions",
            "candidate_events",
            "ai_features",
            "exit_state",
            "risk_events",
            "health_heartbeats",
        ]
    return ["trades", "positions", "ai_features"]


def _plans() -> dict[str, TablePlan]:
    return {
        "bot_meta": TablePlan(
            name="bot_meta",
            columns=[
                ("key", "TEXT PRIMARY KEY"),
                ("value", "JSONB NOT NULL DEFAULT '{}'::jsonb"),
                ("updated_at", "TIMESTAMPTZ NOT NULL DEFAULT now()"),
            ],
            indexes=[],
        ),
        "trades": TablePlan(
            name="trades",
            columns=[
                ("id", "BIGSERIAL PRIMARY KEY"),
                ("ts", "TIMESTAMPTZ NOT NULL DEFAULT now()"),
                ("mint", "TEXT NOT NULL"),
                ("side", "TEXT NOT NULL"),
                ("sig", "TEXT DEFAULT ''"),
                ("venue", "TEXT DEFAULT ''"),
                ("status", "TEXT DEFAULT 'unknown'"),
                ("amount_sol", "NUMERIC"),
                ("amount_raw", "BIGINT"),
                ("out_amount_sol", "NUMERIC"),
                ("out_amount_raw", "BIGINT"),
                ("price_sol", "NUMERIC"),
                ("fee_sol", "NUMERIC"),
                ("reason", "TEXT DEFAULT ''"),
                ("meta", "JSONB NOT NULL DEFAULT '{}'::jsonb"),
            ],
            indexes=[
                "CREATE INDEX IF NOT EXISTS idx_trades_ts ON trades (ts DESC);",
                "CREATE INDEX IF NOT EXISTS idx_trades_mint_ts ON trades (mint, ts DESC);",
                "CREATE INDEX IF NOT EXISTS idx_trades_sig ON trades (sig);",
            ],
        ),
        "positions": TablePlan(
            name="positions",
            columns=[
                ("mint", "TEXT PRIMARY KEY"),
                ("opened_at", "TIMESTAMPTZ NOT NULL DEFAULT now()"),
                ("updated_at", "TIMESTAMPTZ NOT NULL DEFAULT now()"),
                ("status", "TEXT DEFAULT 'open'"),
                ("qty_raw", "BIGINT DEFAULT 0"),
                ("avg_price_sol", "NUMERIC"),
                ("cost_sol", "NUMERIC"),
                ("realized_pnl_sol", "NUMERIC"),
                ("last_price_sol", "NUMERIC"),
                ("meta", "JSONB NOT NULL DEFAULT '{}'::jsonb"),
            ],
            indexes=[
                "CREATE INDEX IF NOT EXISTS idx_positions_updated ON positions (updated_at DESC);",
                "CREATE INDEX IF NOT EXISTS idx_positions_status ON positions (status);",
            ],
        ),
        "candidate_events": TablePlan(
            name="candidate_events",
            columns=[
                ("id", "BIGSERIAL PRIMARY KEY"),
                ("ts", "TIMESTAMPTZ NOT NULL DEFAULT now()"),
                ("mint", "TEXT NOT NULL"),
                ("source", "TEXT DEFAULT ''"),
                ("pool", "TEXT DEFAULT ''"),
                ("ai_ok", "BOOLEAN"),
                ("ai_score", "DOUBLE PRECISION"),
                ("ui", "JSONB NOT NULL DEFAULT '{}'::jsonb"),
            ],
            indexes=[
                "CREATE INDEX IF NOT EXISTS idx_candidates_ts ON candidate_events (ts DESC);",
                "CREATE INDEX IF NOT EXISTS idx_candidates_mint_ts ON candidate_events (mint, ts DESC);",
                "CREATE INDEX IF NOT EXISTS idx_candidates_source_ts ON candidate_events (source, ts DESC);",
            ],
        ),
        "ai_features": TablePlan(
            name="ai_features",
            columns=[
                ("id", "BIGSERIAL PRIMARY KEY"),
                ("ts", "TIMESTAMPTZ NOT NULL DEFAULT now()"),
                ("mint", "TEXT NOT NULL"),
                ("kind", "TEXT DEFAULT 'candidate'"),
                ("features", "JSONB NOT NULL DEFAULT '{}'::jsonb"),
                ("labels", "JSONB NOT NULL DEFAULT '{}'::jsonb"),
            ],
            indexes=[
                "CREATE INDEX IF NOT EXISTS idx_ai_features_ts ON ai_features (ts DESC);",
                "CREATE INDEX IF NOT EXISTS idx_ai_features_mint_ts ON ai_features (mint, ts DESC);",
            ],
        ),
        "exit_state": TablePlan(
            name="exit_state",
            columns=[
                ("mint", "TEXT PRIMARY KEY"),
                ("created_at", "TIMESTAMPTZ NOT NULL DEFAULT now()"),
                ("updated_at", "TIMESTAMPTZ NOT NULL DEFAULT now()"),
                ("state", "JSONB NOT NULL DEFAULT '{}'::jsonb"),
            ],
            indexes=[
                "CREATE INDEX IF NOT EXISTS idx_exit_state_updated ON exit_state (updated_at DESC);",
            ],
        ),
        "risk_events": TablePlan(
            name="risk_events",
            columns=[
                ("id", "BIGSERIAL PRIMARY KEY"),
                ("ts", "TIMESTAMPTZ NOT NULL DEFAULT now()"),
                ("level", "TEXT DEFAULT 'info'"),
                ("code", "TEXT DEFAULT ''"),
                ("msg", "TEXT DEFAULT ''"),
                ("meta", "JSONB NOT NULL DEFAULT '{}'::jsonb"),
            ],
            indexes=[
                "CREATE INDEX IF NOT EXISTS idx_risk_ts ON risk_events (ts DESC);",
                "CREATE INDEX IF NOT EXISTS idx_risk_code_ts ON risk_events (code, ts DESC);",
            ],
        ),
        "health_heartbeats": TablePlan(
            name="health_heartbeats",
            columns=[
                ("id", "BIGSERIAL PRIMARY KEY"),
                ("ts", "TIMESTAMPTZ NOT NULL DEFAULT now()"),
                ("component", "TEXT DEFAULT 'unknown'"),
                ("payload", "JSONB NOT NULL DEFAULT '{}'::jsonb"),
            ],
            indexes=[
                "CREATE INDEX IF NOT EXISTS idx_health_ts ON health_heartbeats (ts DESC);",
                "CREATE INDEX IF NOT EXISTS idx_health_component_ts ON health_heartbeats (component, ts DESC);",
            ],
        ),
    }


# ---------------------------------------------------------------------
# SQL helpers
# ---------------------------------------------------------------------


def _qident(name: str) -> str:
    n = (name or "").strip().replace('"', '""')
    return f'"{n}"'


def _index_name_from_sql(sql: str) -> str | None:
    t = " ".join((sql or "").split())
    m = re.search(r"(?i)create\s+index\s+if\s+not\s+exists\s+([a-zA-Z0-9_\".]+)\s+on\s+", t)
    if not m:
        return None
    name = m.group(1).strip().strip('"')
    if "." in name:
        name = name.split(".", 1)[-1]
    return name or None


def _build_create_table_sql(schema: str, plan: TablePlan) -> str:
    sch_q = _qident(schema)
    cols = ",\n  ".join([f"{_qident(c)} {ddl}" for (c, ddl) in plan.columns])
    return f"CREATE TABLE IF NOT EXISTS {sch_q}.{_qident(plan.name)} (\n  {cols}\n);"


async def _exec(conn: Any, sql: str, *, dryrun: bool) -> None:
    s = (sql or "").strip()
    if not s:
        return
    if dryrun:
        return
    await conn.exec_driver_sql(s)


# ---------------------------------------------------------------------
# introspection
# ---------------------------------------------------------------------


async def _schema_exists(conn: Any, schema: str) -> bool:
    r = await conn.execute(
        text("SELECT 1 FROM information_schema.schemata WHERE schema_name=:s"), {"s": schema}
    )
    return r.first() is not None


async def _tables_in_schema(conn: Any, schema: str) -> list[str]:
    r = await conn.execute(
        text("SELECT table_name FROM information_schema.tables WHERE table_schema=:s"),
        {"s": schema},
    )
    return [str(x[0]) for x in r.fetchall()]


async def _columns_in_table(conn: Any, schema: str, table: str) -> list[str]:
    r = await conn.execute(
        text(
            "SELECT column_name FROM information_schema.columns WHERE table_schema=:s AND table_name=:t"
        ),
        {"s": schema, "t": table},
    )
    return [str(x[0]) for x in r.fetchall()]


async def _index_names(conn: Any, schema: str, table: str) -> list[str]:
    r = await conn.execute(
        text("SELECT indexname FROM pg_indexes WHERE schemaname=:s AND tablename=:t"),
        {"s": schema, "t": table},
    )
    return [str(x[0]) for x in r.fetchall()]


# ---------------------------------------------------------------------
# advisory lock (prevents concurrent schema DDL fights)
# ---------------------------------------------------------------------


async def _try_schema_lock(conn: Any, *, schema: str, timeout_s: float) -> tuple[bool, str]:
    """
    Best-effort advisory lock: pg_try_advisory_lock(hashtext(...)).
    Not fatal if it fails; but prevents "two deploys racing DDL".
    """
    schema = (schema or "public").strip() or "public"
    key = f"pnlsniper_schema_guard:{schema}"
    # use two-int key derived from hashtext for portability
    # (hashtext returns int4; cast to int8 and use (k, 0))
    try:
        row = (
            await conn.execute(
                text("SELECT pg_try_advisory_lock(hashtext(:k)::bigint, 0)"), {"k": key}
            )
        ).first()
        if row and bool(row[0]):
            return True, "locked"
    except Exception as e:
        return False, f"lock_error:{type(e).__name__}:{e}"

    # wait loop
    end = time.monotonic() + max(0.0, float(timeout_s))
    while time.monotonic() < end:
        await asyncio.sleep(0.20 + random.random() * 0.20)
        try:
            row = (
                await conn.execute(
                    text("SELECT pg_try_advisory_lock(hashtext(:k)::bigint, 0)"), {"k": key}
                )
            ).first()
            if row and bool(row[0]):
                return True, "locked_after_wait"
        except Exception as e:
            return False, f"lock_error:{type(e).__name__}:{e}"
    return False, "lock_timeout"


async def _unlock_schema(conn: Any, *, schema: str) -> None:
    schema = (schema or "public").strip() or "public"
    key = f"pnlsniper_schema_guard:{schema}"
    with contextlib.suppress(Exception):
        await conn.execute(text("SELECT pg_advisory_unlock(hashtext(:k)::bigint, 0)"), {"k": key})


# ---------------------------------------------------------------------
# ensure logic
# ---------------------------------------------------------------------


async def _ensure_table(
    conn: Any,
    *,
    schema: str,
    plan: TablePlan,
    allow_create: bool,
    dryrun: bool,
    strict_index_verify: bool,
) -> tuple[list[str], list[str], list[str], bool]:
    """
    Returns: (applied, missing, warnings, attempted_ddl)
    """
    applied: list[str] = []
    missing: list[str] = []
    warnings: list[str] = []
    attempted_ddl = False

    schema = (schema or "public").strip() or "public"
    sch_q = _qident(schema)

    tables = set(await _tables_in_schema(conn, schema))
    if plan.name not in tables:
        if not allow_create:
            missing.append(f"missing table: {schema}.{plan.name}")
            return applied, missing, warnings, attempted_ddl
        attempted_ddl = True
        if schema != "public":
            await _exec(conn, f"CREATE SCHEMA IF NOT EXISTS {sch_q};", dryrun=dryrun)
            applied.append(f"ensured schema: {schema}")
        await _exec(conn, _build_create_table_sql(schema, plan), dryrun=dryrun)
        applied.append(f"created table: {schema}.{plan.name}")

    cols = set(await _columns_in_table(conn, schema, plan.name))
    for col, ddl in plan.colmap.items():
        if col in cols:
            continue
        if not allow_create:
            missing.append(f"missing column: {schema}.{plan.name}.{col}")
            continue
        attempted_ddl = True
        await _exec(
            conn,
            f"ALTER TABLE {sch_q}.{_qident(plan.name)} ADD COLUMN IF NOT EXISTS {_qident(col)} {ddl};",
            dryrun=dryrun,
        )
        applied.append(f"added column: {schema}.{plan.name}.{col}")

    # Indexes: if creating is allowed, ensure them; else optionally verify presence.
    expected_idx_names: list[str] = []
    for idx_sql in plan.indexes:
        nm = _index_name_from_sql(idx_sql)
        if nm:
            expected_idx_names.append(nm)

    if plan.indexes and allow_create:
        attempted_ddl = True
        for idx_sql in plan.indexes:
            await _exec(conn, idx_sql, dryrun=dryrun)
        applied.append(f"ensured indexes: {schema}.{plan.name} ({len(plan.indexes)})")
    elif expected_idx_names:
        try:
            present = set(await _index_names(conn, schema, plan.name))
            for nm in expected_idx_names:
                if nm not in present:
                    msg = f"missing index: {schema}.{nm}"
                    if strict_index_verify:
                        missing.append(msg)
                    else:
                        warnings.append(msg)
        except Exception as e:
            warnings.append(f"index_verify_warn:{schema}.{plan.name}:{type(e).__name__}:{e}")

    return applied, missing, warnings, attempted_ddl


async def _run_one(
    plan: AttemptPlan,
    *,
    schema: str,
    required_tables: list[str],
    allow_create: bool,
    dryrun: bool,
    strict_index_verify: bool,
    json_only: bool,
    attempt_timeout_s: float,
) -> dict[str, Any]:
    """
    Try one DSN candidate. Returns an attempt result dict (stable keys).
    """
    t0 = time.monotonic()

    out: dict[str, Any] = {
        "label": plan.label,
        "ok": False,
        "dsn": _mask_dsn(plan.raw),
        "dsn_sa": _mask_dsn(plan.dsn_sa),
        "host": plan.host,
        "host_resolves": plan.host_resolves,
        "port": plan.port,
        "db_hint": plan.db,
        "sslmode": plan.sslmode,
        "ssl_enabled": int(bool(plan.ssl_enabled)),
        "mode": ("create" if allow_create else "verify"),
        "dryrun": int(bool(dryrun)),
        "attempted_ddl": False,
        "applied": [],
        "missing": [],
        "warnings": [],
        "details": {},
        "error": "",
        "error_kind": "",
        "sec": None,
        "driver": "sqlalchemy+asyncpg",
        "ips": [],
        "tcp_probe_ok": None,
        "tcp_probe": None,
    }

    # Missing deps
    if dsnmod is None or _DB_DSN_IMPORT_ERR is not None:
        out["error"] = (
            f"missing_deps:utils_pkg.db_dsn:{type(_DB_DSN_IMPORT_ERR).__name__}:{_DB_DSN_IMPORT_ERR}"
        )
        out["error_kind"] = "missing_deps"
        out["sec"] = round(time.monotonic() - t0, 3)
        return out
    if create_async_engine is None or text is None:
        out["error"] = f"missing_deps:sqlalchemy:{type(_SA_IMPORT_ERR).__name__}:{_SA_IMPORT_ERR}"
        out["error_kind"] = "missing_deps"
        out["sec"] = round(time.monotonic() - t0, 3)
        return out

    dns_debug = _envb("SCHEMA_DNS_DEBUG", True)
    tcp_probe = _envb("SCHEMA_TCP_PROBE", True)
    lock_enabled = _envb("SCHEMA_ADVISORY_LOCK", True)

    if dns_debug and plan.host:
        out["ips"] = _dns_debug(plan.host, plan.port)
    if tcp_probe and plan.host:
        okp, msgp = _tcp_probe(
            plan.host, plan.port, timeout_s=min(6.0, max(2.0, attempt_timeout_s * 0.35))
        )
        out["tcp_probe_ok"] = int(bool(okp))
        out["tcp_probe"] = msgp

    # Build engine (tiny pool; one-shot tool)
    eng = create_async_engine(
        plan.dsn_sa,
        connect_args=dict(plan.connect_args or {}),
        pool_pre_ping=True,
        pool_size=1,
        max_overflow=0,
        pool_timeout=max(3.0, float(attempt_timeout_s)),
    )

    retries, base = _retry_cfg()
    last_err: BaseException | None = None

    try:
        for attempt in range(retries + 1):
            try:
                async with eng.connect() as conn:
                    # Session timeouts (safe in both modes; best-effort)
                    stmt_ms = int(_envi("SCHEMA_STATEMENT_TIMEOUT_MS", 20000))
                    lock_ms = int(_envi("SCHEMA_LOCK_TIMEOUT_MS", 5000))
                    idle_ms = int(_envi("SCHEMA_IDLE_TX_TIMEOUT_MS", 10000))

                    with contextlib.suppress(Exception):
                        await conn.exec_driver_sql(f"SET statement_timeout = {max(1500, stmt_ms)}")
                    with contextlib.suppress(Exception):
                        await conn.exec_driver_sql(f"SET lock_timeout = {max(500, lock_ms)}")
                    with contextlib.suppress(Exception):
                        await conn.exec_driver_sql(
                            f"SET idle_in_transaction_session_timeout = {max(1500, idle_ms)}"
                        )

                    # Ping
                    row = (await conn.execute(text("SELECT 1"))).first()
                    if not row or row[0] != 1:
                        raise RuntimeError("select1_failed")

                    # Begin tx
                    if not allow_create:
                        await conn.exec_driver_sql("BEGIN READ ONLY")
                    else:
                        await conn.exec_driver_sql("BEGIN")

                    try:
                        # advisory lock (only useful in create mode; harmless in verify)
                        if lock_enabled and allow_create:
                            ok_lock, why = await _try_schema_lock(
                                conn,
                                schema=schema,
                                timeout_s=float(_envf("SCHEMA_LOCK_WAIT_S", 8.0)),
                            )
                            out["details"]["advisory_lock"] = why
                            if not ok_lock:
                                # Not fatal, but helps explain drift / DDL races
                                out["warnings"].append(f"advisory_lock:{why}")

                        ro = (await conn.execute(text("SHOW transaction_read_only"))).scalar()
                        ro_s = str(ro).strip().lower() if ro is not None else ""
                        out["details"]["transaction_read_only"] = ro_s

                        if allow_create and ro_s in ("on", "true", "1"):
                            raise RuntimeError("db_is_read_only_cannot_apply_ddl")

                        sch = (schema or "public").strip() or "public"
                        sch_q = _qident(sch)

                        # Optional extension (best-effort)
                        if allow_create and _envb("SCHEMA_PGCRYPTO", False):
                            out["attempted_ddl"] = True
                            try:
                                await _exec(
                                    conn, "CREATE EXTENSION IF NOT EXISTS pgcrypto;", dryrun=dryrun
                                )
                                out["applied"].append("ensured extension: pgcrypto")
                            except Exception as e:
                                out["warnings"].append(f"pgcrypto_warn:{type(e).__name__}:{e}")

                        # Ensure schema exists if allowed
                        exists = await _schema_exists(conn, sch)
                        if not exists:
                            if not allow_create:
                                out["missing"].append(f"missing schema: {sch}")
                            else:
                                out["attempted_ddl"] = True
                                await _exec(
                                    conn, f"CREATE SCHEMA IF NOT EXISTS {sch_q};", dryrun=dryrun
                                )
                                out["applied"].append(f"ensured schema: {sch}")

                        # search_path so unqualified index SQL lands in correct schema
                        with contextlib.suppress(Exception):
                            await conn.exec_driver_sql(f"SET LOCAL search_path TO {sch_q}, public;")

                        plans = _plans()
                        for tname in required_tables:
                            plan_tbl = plans.get(tname)
                            if not plan_tbl:
                                # Unknown plan => verify only
                                tables = set(await _tables_in_schema(conn, sch))
                                if tname not in tables:
                                    out["missing"].append(
                                        f"missing table (unknown plan): {sch}.{tname}"
                                    )
                                continue

                            applied, missing, warnings, attempted = await _ensure_table(
                                conn,
                                schema=sch,
                                plan=plan_tbl,
                                allow_create=allow_create,
                                dryrun=dryrun,
                                strict_index_verify=strict_index_verify,
                            )
                            out["applied"].extend(applied)
                            out["missing"].extend(missing)
                            out["warnings"].extend(warnings)
                            out["attempted_ddl"] = bool(out["attempted_ddl"] or attempted)

                        # extra metadata
                        with contextlib.suppress(Exception):
                            r2 = (
                                await conn.execute(text("SELECT current_database(), current_user"))
                            ).first()
                            if r2:
                                out["details"]["db"] = str(r2[0])
                                out["details"]["user"] = str(r2[1])

                    finally:
                        # Unlock best-effort before finishing tx
                        if allow_create and lock_enabled:
                            await _unlock_schema(conn, schema=schema)

                        # Commit policy
                        # - In create mode, we commit by default (SCHEMA_COMMIT=1).
                        # - In verify mode, always rollback.
                        commit = bool(
                            allow_create and (not dryrun) and _envb("SCHEMA_COMMIT", True)
                        )
                        if commit:
                            with contextlib.suppress(Exception):
                                await conn.exec_driver_sql("COMMIT")
                        else:
                            with contextlib.suppress(Exception):
                                await conn.exec_driver_sql("ROLLBACK")

                out["ok"] = len(out["missing"]) == 0
                return out

            except Exception as e:
                last_err = e
                msg = f"{type(e).__name__}: {e}"
                out["error"] = msg
                out["error_kind"] = _classify_error(msg)

                # Retry only for flaky classes
                if attempt < retries and out["error_kind"] in (
                    "timeout",
                    "server_closed_mid_op",
                    "ssl",
                    "other",
                ):
                    await _sleep_backoff(attempt, base)
                    continue
                return out

        if last_err is not None:
            msg = f"{type(last_err).__name__}: {last_err}"
            out["error"] = msg
            out["error_kind"] = _classify_error(msg)
        return out
    finally:
        out["sec"] = round(time.monotonic() - t0, 3)
        with contextlib.suppress(Exception):
            await eng.dispose()


# ---------------------------------------------------------------------
# main runner
# ---------------------------------------------------------------------


async def _amain() -> int:
    RC_OK = 0
    RC_FATAL = 1
    RC_MISSING = 2
    RC_CREATE_FAILED = 3
    RC_TIMEOUT = 124

    ap = argparse.ArgumentParser(description="God-Tier Schema Guard (LOCK-IN).")
    ap.add_argument(
        "--dsn",
        action="append",
        default=[],
        help="DSN override (repeatable). If omitted, uses env candidates.",
    )
    ap.add_argument(
        "--schema",
        default=_envs("SCHEMA_NAME", _envs("PG_TEST_SCHEMA", "public") or "public") or "public",
        help="Schema name.",
    )
    ap.add_argument(
        "--profile", choices=["minimal", "full"], default=_schema_profile(), help="Schema profile."
    )
    ap.add_argument(
        "--required-tables",
        default=_envs("REQUIRED_TABLES", ""),
        help="CSV override for required tables.",
    )
    ap.add_argument(
        "--allow-create",
        action="store_true",
        help="Enable create/upgrade (or set ALLOW_SCHEMA_CREATE=1).",
    )
    ap.add_argument("--dryrun", action="store_true", help="No-op SQL (or set SCHEMA_DRYRUN=1).")
    ap.add_argument("--try-all", action="store_true", help="Try all candidates even if one passes.")
    ap.add_argument("--json", action="store_true", help="Emit JSON summary to STDOUT.")
    ap.add_argument("--json-only", action="store_true", help="Only emit JSON (no STDERR logs).")
    ap.add_argument(
        "--strict-index",
        action="store_true",
        help="Treat missing indexes as missing (fatal in verify-only).",
    )
    args = ap.parse_args()

    json_only = bool(args.json_only or _envb("SCHEMA_JSON_ONLY", False))
    emit_json = bool(
        args.json
        or args.json_only
        or _envb("SCHEMA_JSON", False)
        or _envb("SCHEMA_JSON_ONLY", False)
    )

    if _envb("SKIP_DSN_REQUIRED", False):
        payload = {"ts": _ts(), "ok": True, "skipped": True, "reason": "SKIP_DSN_REQUIRED=1"}
        if emit_json:
            print(json.dumps(payload, ensure_ascii=False, default=str))
        _hlog("SKIP (SKIP_DSN_REQUIRED=1)", json_only=json_only)
        return RC_OK

    schema = (args.schema or "public").strip() or "public"
    required_tables = _split_csv(args.required_tables or "")
    if not required_tables:
        required_tables = _default_required_tables(args.profile)

    allow_create = bool(args.allow_create or _envb("ALLOW_SCHEMA_CREATE", False))
    dryrun = bool(args.dryrun or _envb("SCHEMA_DRYRUN", False))
    strict_index_verify = bool(args.strict_index or _envb("SCHEMA_STRICT_INDEX", False))
    try_all = bool(args.try_all or _envb("SCHEMA_TRY_ALL", False))

    # timeouts
    connect_timeout_s = max(2.0, float(_envf("SCHEMA_CONNECT_TIMEOUT_S", 8.0)))
    timeout_s = max(6.0, float(_envf("SCHEMA_TIMEOUT_S", 20.0)))
    attempt_timeout_s = max(timeout_s, connect_timeout_s + 8.0)

    total_timeout_s = max(0.0, float(_envf("SCHEMA_TOTAL_TIMEOUT_S", 0.0)))
    started = time.monotonic()

    cands = _candidate_dsns([d for d in args.dsn if d])
    if not cands:
        payload = {
            "ts": _ts(),
            "ok": False,
            "skipped": False,
            "error": "no_dsn_candidates",
            "detail": "Set PNL_DSN/POSTGRES_DSN/PG_DSN/DATABASE_URL/PG_DSN_INTERNAL/PG_DSN_EXTERNAL or pass --dsn",
        }
        if emit_json:
            print(json.dumps(payload, ensure_ascii=False, default=str))
        _hlog("FATAL: no DSN candidates", json_only=json_only)
        return RC_FATAL

    if not json_only:
        _hlog("──────────────────────────────", json_only=False)
        _hlog("God-Tier check_schema (LOCK-IN)", json_only=False)
        _hlog("──────────────────────────────", json_only=False)
        _hlog(
            f"schema={schema} required_tables={required_tables} profile={args.profile} "
            f"allow_create={int(allow_create)} dryrun={int(dryrun)} try_all={int(try_all)} strict_index={int(strict_index_verify)}",
            json_only=False,
        )
        _hlog(
            f"connect_timeout_s={connect_timeout_s:.1f} timeout_s={timeout_s:.1f} attempt_timeout_s={attempt_timeout_s:.1f} total_timeout_s={total_timeout_s:.1f}",
            json_only=False,
        )
        _hlog(f"candidates={','.join([k for k, _ in cands])}", json_only=False)

    results: list[dict[str, Any]] = []
    picked: dict[str, Any] | None = None

    saw_timeout = False
    saw_fatal = False
    saw_missing = False
    saw_create_failed = False

    app_name = _envs("PG_APP_NAME", "pnlsniper_schema_guard") or "pnlsniper_schema_guard"

    for label, raw in cands:
        if total_timeout_s > 0 and (time.monotonic() - started) > total_timeout_s:
            saw_timeout = True
            results.append(
                {
                    "label": label,
                    "ok": False,
                    "dsn": _mask_dsn(raw),
                    "error": "total_timeout_exceeded",
                    "error_kind": "timeout",
                }
            )
            break

        # Plan build (db_dsn normalization happens here)
        try:
            plan = _build_plan(
                label,
                raw,
                timeout_s=timeout_s,
                connect_timeout_s=connect_timeout_s,
                app_name=app_name,
            )
        except Exception as e:
            msg = f"{type(e).__name__}: {e}"
            r = {
                "label": label,
                "ok": False,
                "dsn": _mask_dsn(raw),
                "error": f"bad_format:{msg}",
                "error_kind": _classify_error(msg) if "missing_deps" in str(e) else "bad_format",
                "sec": 0.0,
            }
            results.append(r)
            saw_fatal = True
            if not json_only:
                _hlog(f"RES {label} ok=0 error={r['error']}", json_only=False)
            continue

        if not json_only:
            _hlog(
                f"TRY {label} host={plan.host or '-'} port={plan.port} db={plan.db or '-'} "
                f"ssl_enabled={int(plan.ssl_enabled)} sslmode={plan.sslmode or '-'} dsn={_mask_dsn(raw)}",
                json_only=False,
            )
            if plan.host and plan.host.endswith(".render.com") and _dsn_sslmode(raw) != "require":
                _hlog(
                    "  note: DSN appears render-ish; sslmode=require is typical for external DSNs.",
                    json_only=False,
                )

        try:
            r = await asyncio.wait_for(
                _run_one(
                    plan,
                    schema=schema,
                    required_tables=required_tables,
                    allow_create=allow_create,
                    dryrun=dryrun,
                    strict_index_verify=strict_index_verify,
                    json_only=json_only,
                    attempt_timeout_s=attempt_timeout_s,
                ),
                timeout=attempt_timeout_s + 3.0,
            )
        except TimeoutError:
            saw_timeout = True
            r = {
                "label": label,
                "ok": False,
                "dsn": _mask_dsn(raw),
                "dsn_sa": _mask_dsn(plan.dsn_sa),
                "host": plan.host,
                "host_resolves": plan.host_resolves,
                "port": plan.port,
                "db_hint": plan.db,
                "sslmode": plan.sslmode,
                "ssl_enabled": int(plan.ssl_enabled),
                "mode": ("create" if allow_create else "verify"),
                "dryrun": int(bool(dryrun)),
                "attempted_ddl": False,
                "applied": [],
                "missing": ["attempt_timeout"],
                "warnings": [],
                "details": {},
                "error": "attempt_timeout",
                "error_kind": "timeout",
                "sec": round(attempt_timeout_s, 3),
                "driver": "watchdog",
            }

        results.append(r)

        if not json_only:
            _hlog(
                f"RES {label} ok={int(bool(r.get('ok')))} sec={r.get('sec')} kind={r.get('error_kind') or '-'}",
                json_only=False,
            )
            if r.get("error"):
                _hlog(f"  error={r['error']}", json_only=False)
            if r.get("missing"):
                _hlog(f"  missing_count={len(r['missing'])}", json_only=False)

        if r.get("ok") and picked is None:
            picked = r
            if not try_all:
                break

        kind = str(r.get("error_kind") or "")
        if kind == "timeout":
            saw_timeout = True
        if kind in {"bad_format", "missing_deps"}:
            saw_fatal = True
        if r.get("missing"):
            if allow_create:
                saw_create_failed = True
            else:
                saw_missing = True

    summary: dict[str, Any] = {
        "ts": _ts(),
        "ok": bool(picked is not None),
        "skipped": False,
        "schema": schema,
        "profile": args.profile,
        "required_tables": required_tables,
        "allow_create": bool(allow_create),
        "dryrun": bool(dryrun),
        "strict_index_verify": bool(strict_index_verify),
        "picked": (picked.get("label") if picked else None),
        "results": results,
        "fix_hints": _fix_hints(results, allow_create=allow_create, schema=schema),
    }

    if emit_json:
        print(json.dumps(summary, ensure_ascii=False, default=str))

    if picked is not None:
        _hlog("SUMMARY ok=1 (schema ready)", json_only=json_only)
        return RC_OK

    if saw_timeout and not (saw_missing or saw_create_failed or saw_fatal):
        _hlog("SUMMARY ok=0 (timeout)", json_only=json_only)
        return RC_TIMEOUT

    if saw_fatal and not (saw_missing or saw_create_failed):
        _hlog("SUMMARY ok=0 (fatal)", json_only=json_only)
        return RC_FATAL

    if allow_create:
        _hlog("SUMMARY ok=0 (DDL allowed but schema not ready)", json_only=json_only)
        return RC_CREATE_FAILED

    _hlog("SUMMARY ok=0 (verify-only; missing schema items)", json_only=json_only)
    return RC_MISSING


def main() -> None:
    try:
        rc = asyncio.run(_amain())
    except KeyboardInterrupt:
        rc = 130
    except Exception as e:
        _safe_print(f"[schema] {_ts()} FAIL unhandled {type(e).__name__}: {e}", file=sys.stderr)
        rc = 1
    raise SystemExit(rc)


if __name__ == "__main__":
    # Windows asyncio stability
    with contextlib.suppress(Exception):
        if os.name == "nt" and hasattr(asyncio, "WindowsSelectorEventLoopPolicy"):
            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())  # type: ignore[attr-defined]
    main()
