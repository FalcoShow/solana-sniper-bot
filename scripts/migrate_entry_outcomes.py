#!/usr/bin/env python3
from __future__ import annotations

"""
scripts/migrate_entry_outcomes.py

V3+++ / quant-grade, idempotent migration helper for entry_outcomes.

Ensures entry_outcomes has the columns required by the AI/data-logger stack:

  - pnl_pct    DOUBLE PRECISION
  - hold_sec   DOUBLE PRECISION
  - notes      TEXT
  - notes_json JSONB (default {})

Also ensures:
  - UNIQUE index on candidate_id (for ON CONFLICT upserts)
  - horizon_sec (if present) is nullable (optional compatibility relax)

DSN resolution
--------------
Priority (CLI --dsn overrides all):
  POSTGRES_DSN, PG_DSN, SNIPER_DB_DSN, DATABASE_URL, PNL_DSN, PNL_DB_DSN, PNL_PG_DSN,
  PG_DSN_INTERNAL, PG_DSN_EXTERNAL

Notes
-----
- Uses psycopg (v3) if available; falls back to psycopg2.
- Enforces sslmode=require if missing (safe for Render-style hosted DBs).
- Safe identifier quoting/validation (no SQL injection via schema/table names).
- Idempotent operations; safe to rerun.
"""

import argparse
import contextlib
import hashlib
import os
import re
from typing import Any

# Optional .env loader
try:  # pragma: no cover
    from dotenv import find_dotenv, load_dotenv  # type: ignore
except Exception:  # pragma: no cover
    load_dotenv = None  # type: ignore

if load_dotenv is not None:  # pragma: no cover
    load_dotenv(find_dotenv(), override=False)

# ---------------------------------------------------------------------------
# DSN
# ---------------------------------------------------------------------------

_DSN_KEYS = (
    "POSTGRES_DSN",
    "PG_DSN",
    "SNIPER_DB_DSN",
    "DATABASE_URL",
    "PNL_DSN",
    "PNL_DB_DSN",
    "PNL_PG_DSN",
    "PG_DSN_INTERNAL",
    "PG_DSN_EXTERNAL",
)


def _normalize_dsn(raw: str) -> str:
    dsn = (raw or "").strip()
    if not dsn:
        return dsn

    # normalize schemes
    if dsn.startswith("postgres://"):
        dsn = "postgresql://" + dsn[len("postgres://") :]
    if dsn.startswith("postgresql+asyncpg://"):
        dsn = "postgresql://" + dsn[len("postgresql+asyncpg://") :]

    # enforce sslmode=require if not specified (safe for hosted DBs)
    if "sslmode=" not in dsn:
        dsn += ("&" if "?" in dsn else "?") + "sslmode=require"

    return dsn


def pick_dsn(cli_dsn: str = "") -> tuple[str, str]:
    if cli_dsn:
        return _normalize_dsn(cli_dsn), "--dsn"

    for k in _DSN_KEYS:
        v = os.getenv(k)
        if v:
            return _normalize_dsn(v), k

    raise SystemExit(
        "No DSN found. Set one of: "
        + ", ".join(_DSN_KEYS)
        + " (PowerShell: $env:PG_DSN='postgresql://user:pass@host:5432/db?sslmode=require')"
    )


# ---------------------------------------------------------------------------
# Safe identifiers
# ---------------------------------------------------------------------------

_VALID_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _split_table_ref(ref: str) -> tuple[str, str]:
    ref = (ref or "").strip().strip('"')
    if not ref:
        raise ValueError("empty table ref")
    if "." in ref:
        s, t = ref.split(".", 1)
        return s.strip().strip('"'), t.strip().strip('"')
    return "public", ref.strip().strip('"')


def _qident(name: str) -> str:
    name = (name or "").strip().strip('"')
    if not _VALID_IDENT.match(name):
        raise ValueError(f"Invalid SQL identifier: {name!r}")
    return f'"{name}"'


def _qtable(schema: str, table: str) -> str:
    return f"{_qident(schema)}.{_qident(table)}"


def _stable_index_name(table: str, suffix: str, *, max_len: int = 63) -> str:
    """
    Postgres identifiers are limited (typically 63 bytes). Keep a stable, unique name.
    """
    base = f"{table}_{suffix}"
    if len(base) <= max_len:
        return base
    h = hashlib.sha1(base.encode("utf-8")).hexdigest()[:10]  # stable, short
    head = base[: max_len - 11]  # "_" + 10 chars
    return f"{head}_{h}"


# ---------------------------------------------------------------------------
# DB driver (psycopg3 preferred)
# ---------------------------------------------------------------------------


def _connect(dsn: str):
    """
    Prefer psycopg (v3). Fallback psycopg2.
    All raises inside except must chain cause (Ruff B904).
    """
    psycopg_err: Exception | None = None

    try:
        import psycopg  # type: ignore
    except Exception as err:  # pragma: no cover
        psycopg_err = err
    else:
        try:
            return psycopg.connect(dsn)
        except Exception as err:  # pragma: no cover
            psycopg_err = err

    # Fallback to psycopg2
    try:
        import psycopg2  # type: ignore
    except Exception as err:  # pragma: no cover
        msg = (
            "No Postgres driver available.\n"
            "Install ONE of:\n"
            "  pip install 'psycopg[binary]'\n"
            "  pip install psycopg2-binary\n"
        )
        if psycopg_err is not None:
            msg += f"\npsycopg error: {type(psycopg_err).__name__}: {psycopg_err}"
        msg += f"\npsycopg2 import error: {type(err).__name__}: {err}"
        raise SystemExit(msg) from err

    try:
        return psycopg2.connect(dsn)
    except Exception as err:  # pragma: no cover
        msg = (
            "Failed to connect to Postgres.\n"
            "Tried psycopg (v3) then psycopg2.\n"
            "Ensure your DSN is correct and network access is allowed.\n"
        )
        if psycopg_err is not None:
            msg += f"\npsycopg error: {type(psycopg_err).__name__}: {psycopg_err}"
        msg += f"\npsycopg2 connect error: {type(err).__name__}: {err}"
        raise SystemExit(msg) from err


# ---------------------------------------------------------------------------
# Introspection helpers
# ---------------------------------------------------------------------------


def _fetch_nullable(cur: Any, schema: str, table: str, column: str) -> bool | None:
    cur.execute(
        """
        SELECT is_nullable
        FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s AND column_name = %s
        """,
        (schema, table, column),
    )
    row = cur.fetchone()
    if not row:
        return None
    return row[0] == "YES"


def _table_exists(cur: Any, schema: str, table: str) -> bool:
    cur.execute("SELECT to_regclass(%s)", (f"{schema}.{table}",))
    row = cur.fetchone()
    return bool(row and row[0])


# ---------------------------------------------------------------------------
# Migration
# ---------------------------------------------------------------------------


def migrate(
    *,
    dsn: str,
    table_ref: str,
    lock_timeout_ms: int,
    statement_timeout_ms: int,
    dry_run: bool,
) -> None:
    try:
        schema, table = _split_table_ref(table_ref)
        qt = _qtable(schema, table)  # validates identifiers
    except Exception as err:
        raise SystemExit(f"Invalid --table value {table_ref!r}: {err}") from err

    idx_name = _stable_index_name(table, "candidate_id_uidx")
    q_idx_name = _qident(idx_name)

    ddls = [
        f"ALTER TABLE {qt} ADD COLUMN IF NOT EXISTS pnl_pct DOUBLE PRECISION",
        f"ALTER TABLE {qt} ADD COLUMN IF NOT EXISTS hold_sec DOUBLE PRECISION",
        f"ALTER TABLE {qt} ADD COLUMN IF NOT EXISTS notes TEXT",
        f"ALTER TABLE {qt} ADD COLUMN IF NOT EXISTS notes_json JSONB",
        # Ensure notes_json default + NOT NULL even if column already existed
        f"ALTER TABLE {qt} ALTER COLUMN {_qident('notes_json')} SET DEFAULT '{{}}'::jsonb",
        f"UPDATE {qt} SET {_qident('notes_json')}='{{}}'::jsonb WHERE {_qident('notes_json')} IS NULL",
        f"ALTER TABLE {qt} ALTER COLUMN {_qident('notes_json')} SET NOT NULL",
        # ensure unique index for ON CONFLICT upserts
        f"CREATE UNIQUE INDEX IF NOT EXISTS {q_idx_name} ON {qt} ({_qident('candidate_id')})",
    ]

    print(f"Target table: {schema}.{table}")
    print(f"Applying {len(ddls)} statements (idempotent).")

    if dry_run:
        print("\n--- DRY RUN ---")
        for s in ddls:
            print(s + ";")
        print("--- END ---")
        return

    conn = _connect(dsn)
    try:
        # psycopg3/psycopg2 compatibility
        if hasattr(conn, "autocommit"):
            conn.autocommit = True  # type: ignore[attr-defined]

        cur = conn.cursor()
        try:
            if lock_timeout_ms > 0:
                cur.execute(f"SET lock_timeout = '{int(lock_timeout_ms)}ms'")
            if statement_timeout_ms > 0:
                cur.execute(f"SET statement_timeout = '{int(statement_timeout_ms)}ms'")

            if not _table_exists(cur, schema, table):
                raise SystemExit(
                    f"Table does not exist: {schema}.{table} "
                    f"(set --table or ENTRY_OUTCOMES_TABLE correctly)"
                )

            for s in ddls:
                cur.execute(s)

            # Optional: relax horizon_sec NOT NULL if present & NOT NULL
            nullable = _fetch_nullable(cur, schema, table, "horizon_sec")
            if nullable is False:
                print("Relaxing horizon_sec: dropping NOT NULL...")
                cur.execute(f"ALTER TABLE {qt} ALTER COLUMN {_qident('horizon_sec')} DROP NOT NULL")
            else:
                print("OK: horizon_sec is nullable or missing.")

            print("Migration completed successfully.")
        finally:
            with contextlib.suppress(Exception):
                cur.close()
    finally:
        with contextlib.suppress(Exception):
            conn.close()


def build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Idempotent migration for entry_outcomes schema.")
    p.add_argument("--dsn", type=str, default="", help="Optional DSN override.")
    p.add_argument(
        "--table",
        type=str,
        default=os.getenv("ENTRY_OUTCOMES_TABLE", "public.entry_outcomes"),
        help="Target table (default: public.entry_outcomes or ENTRY_OUTCOMES_TABLE).",
    )
    p.add_argument(
        "--lock-timeout-ms", type=int, default=5000, help="Lock timeout in ms (default 5000)."
    )
    p.add_argument(
        "--statement-timeout-ms",
        type=int,
        default=30000,
        help="Statement timeout in ms (default 30000).",
    )
    p.add_argument("--dry-run", action="store_true", help="Print statements and exit.")
    return p


def main() -> None:
    args = build_arg_parser().parse_args()
    dsn, src = pick_dsn(args.dsn)
    print(f"Using DSN from {src} (credentials hidden).")

    migrate(
        dsn=dsn,
        table_ref=args.table,
        lock_timeout_ms=int(args.lock_timeout_ms),
        statement_timeout_ms=int(args.statement_timeout_ms),
        dry_run=bool(args.dry_run),
    )


if __name__ == "__main__":
    main()
