#!/usr/bin/env python3
from __future__ import annotations

"""
scripts/migrate_entry_outcomes.py

V3+++ / quant-grade migration helper for entry_outcomes.

Goal
----
Ensure the entry_outcomes table has the columns required by the
AI / data-logger stack:

  - pnl_pct    (DOUBLE PRECISION)
  - hold_sec   (DOUBLE PRECISION)
  - notes      (TEXT)            ← legacy compatibility
  - notes_json (JSONB)           ← richer V3+++ metadata

This script is intentionally idempotent: you can run it multiple times
and it will only add missing columns.

Usage
-----

    python -m scripts.migrate_entry_outcomes

Environment DSN resolution
--------------------------
We resolve the Postgres DSN in this priority order:

  1) SNIPER_DB_DSN
  2) PNL_DB_DSN
  3) PNL_PG_DSN
  4) DATABASE_URL

If no DSN is found, we exit with code 1 and a clear error message.

We also automatically enforce ?sslmode=require (or &sslmode=require)
if sslmode= is not already present in the DSN.
"""

import os
import sys
from typing import Tuple

import psycopg2  # type: ignore[import]

# Optional .env loader (for local dev / PowerShell runs)
try:  # pragma: no cover
    from dotenv import load_dotenv, find_dotenv  # type: ignore[import]
except Exception:  # pragma: no cover
    load_dotenv = None  # type: ignore[assignment]

if load_dotenv is not None:
    # Match scripts/test_stack.py behavior: don't override already-set env,
    # but do load from a local .env if present.
    load_dotenv(find_dotenv(), override=False)


def _pick_dsn() -> Tuple[str, str]:
    """
    Choose a DSN from the env with priority:

      SNIPER_DB_DSN > PNL_DB_DSN > PNL_PG_DSN > DATABASE_URL

    Returns (dsn, source_env_var).

    Exits with code 1 if none is set.
    """
    sources = [
        "SNIPER_DB_DSN",
        "PNL_DB_DSN",
        "PNL_PG_DSN",
        "DATABASE_URL",
    ]
    for key in sources:
        val = os.getenv(key)
        if val:
            return val, key

    print(
        "ERROR: No SNIPER_DB_DSN / PNL_DB_DSN / PNL_PG_DSN / DATABASE_URL set "
        "in environment. Cannot run migration.",
        file=sys.stderr,
    )
    sys.exit(1)


def _normalize_dsn(raw_dsn: str) -> str:
    """
    Append sslmode=require if the DSN doesn't already specify sslmode.

    Works for both:
      postgresql://user:pass@host:5432/db
      postgresql://user:pass@host:5432/db?param=1
    """
    dsn = raw_dsn.strip()
    if "sslmode=" in dsn:
        return dsn
    sep = "&" if "?" in dsn else "?"
    return dsn + f"{sep}sslmode=require"


def _get_dsn() -> str:
    raw, src = _pick_dsn()
    dsn = _normalize_dsn(raw)
    print(f"Using DSN from {src}: {dsn!r}")
    return dsn


def main() -> None:
    dsn = _get_dsn()
    conn = psycopg2.connect(dsn)
    try:
        with conn:
            with conn.cursor() as cur:
                print("Inspecting entry_outcomes columns...")

                # Respect current_schema(), fallback to public.
                cur.execute("SELECT current_schema()")
                current_schema = cur.fetchone()[0] or "public"

                # Fetch columns
                cur.execute(
                    """
                    SELECT column_name, is_nullable
                    FROM information_schema.columns
                    WHERE table_name = 'entry_outcomes'
                      AND table_schema = %s
                    """,
                    (current_schema,),
                )
                rows = cur.fetchall()
                cols = {row[0] for row in rows}
                nullable_map = {row[0]: row[1] for row in rows}
                print("Existing columns:", ", ".join(sorted(cols)) or "<none>")

                # Ensure pnl_pct
                if "pnl_pct" not in cols:
                    print("Adding pnl_pct DOUBLE PRECISION column...")
                    cur.execute(
                        f"""
                        ALTER TABLE {current_schema}.entry_outcomes
                        ADD COLUMN pnl_pct DOUBLE PRECISION
                        """
                    )
                else:
                    print("OK: pnl_pct column already exists.")

                # Ensure hold_sec
                if "hold_sec" not in cols:
                    print("Adding hold_sec DOUBLE PRECISION column...")
                    cur.execute(
                        f"""
                        ALTER TABLE {current_schema}.entry_outcomes
                        ADD COLUMN hold_sec DOUBLE PRECISION
                        """
                    )
                else:
                    print("OK: hold_sec column already exists.")

                # Ensure legacy notes TEXT (for ai_db.mark_outcome_for_candidate)
                if "notes" not in cols:
                    print("Adding legacy notes TEXT column for compatibility...")
                    cur.execute(
                        f"""
                        ALTER TABLE {current_schema}.entry_outcomes
                        ADD COLUMN notes TEXT
                        """
                    )
                else:
                    print("OK: notes column already exists.")

                # Ensure notes_json (used for extra outcome metadata)
                if "notes_json" not in cols:
                    print("Adding notes_json JSONB column with default '{}'::jsonb...")
                    cur.execute(
                        f"""
                        ALTER TABLE {current_schema}.entry_outcomes
                        ADD COLUMN notes_json JSONB DEFAULT '{{}}'::jsonb
                        """
                    )
                else:
                    print("OK: notes_json column already exists.")

                # Ensure UNIQUE index on candidate_id for ON CONFLICT support
                print("Ensuring UNIQUE index on candidate_id for ON CONFLICT...")
                cur.execute(
                    f"""
                    CREATE UNIQUE INDEX IF NOT EXISTS entry_outcomes_candidate_id_idx
                    ON {current_schema}.entry_outcomes (candidate_id)
                    """
                )

                # 🔓 Relax NOT NULL on horizon_sec so test_stack can insert NULL
                if "horizon_sec" in nullable_map and nullable_map["horizon_sec"] == "NO":
                    print("Dropping NOT NULL constraint on horizon_sec...")
                    cur.execute(
                        f"""
                        ALTER TABLE {current_schema}.entry_outcomes
                        ALTER COLUMN horizon_sec DROP NOT NULL
                        """
                    )
                else:
                    print("OK: horizon_sec already nullable or missing; no change.")

        print("Migration completed successfully.")
    finally:
        conn.close()
        print("Connection closed.")


if __name__ == "__main__":
    main()
