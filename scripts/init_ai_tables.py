#!/usr/bin/env python3
# scripts/init_ai_tables.py
# -*- coding: utf-8 -*-
from __future__ import annotations

"""
scripts/init_ai_tables.py

Quant-grade initializer / migrator for AI tables (V3+++ / P0 contract)
=====================================================================

Purpose
-------
Idempotently create/migrate the AI research/training tables used by:
  - utils_pkg.ai_db
  - utils_pkg.data_logger
  - utils_pkg.backtest_engine
  - utils_pkg.self_learning_ai_strategy
  - (optional) utils_pkg.ai_risk_model

P0 rules enforced
-----------------
- Uses canonical DSN resolution/normalization from utils_pkg.ai_db when possible.
- Safe to re-run (IF NOT EXISTS / ADD COLUMN IF NOT EXISTS).
- Never prints secrets (DSN masked).
- Exits non-zero on failure (CI / Render preflight friendly).
- Prefers Postgres; sqlite is legacy-only (explicit opt-in to allow).

Usage
-----
    python -m scripts.init_ai_tables
    python -m scripts.init_ai_tables --dry-run
    python -m scripts.init_ai_tables --dsn "postgresql://.../db?sslmode=require"
    python -m scripts.init_ai_tables --verify
    python -m scripts.init_ai_tables --json
    python -m scripts.init_ai_tables --allow-sqlite   # NOT recommended (legacy only)

Notes
-----
This script first attempts to delegate schema init to utils_pkg.ai_db if it
exposes a canonical initializer (preferred). If not available, it falls back
to embedded DDL that is aligned with the V3+++ schema.
"""

import argparse
import asyncio
import json
import os
import re
import sys
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

# ─────────────────────────────────────────────────────────────────────────────
# Ensure repo root on sys.path (for `python scripts/init_ai_tables.py` execution)
# ─────────────────────────────────────────────────────────────────────────────

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

try:
    from logging_setup import get_logger  # canonical
except Exception:  # pragma: no cover
    import logging

    def get_logger(name: str = "init_ai_tables") -> logging.Logger:
        logging.basicConfig(level=logging.INFO)
        return logging.getLogger(name)


log = get_logger(__name__)

# Canonical DB brain (preferred DSN resolver + backend descriptor)
import contextlib

from utils_pkg import ai_db

# ─────────────────────────────────────────────────────────────────────────────
# Optional Postgres drivers (psycopg3 preferred, psycopg2 fallback)
# ─────────────────────────────────────────────────────────────────────────────

_PSYCOPG_KIND = "none"
try:  # pragma: no cover
    import psycopg  # type: ignore

    _PSYCOPG_KIND = "psycopg3"
except Exception:  # pragma: no cover
    psycopg = None  # type: ignore

try:  # pragma: no cover
    import psycopg2  # type: ignore

    if _PSYCOPG_KIND == "none":
        _PSYCOPG_KIND = "psycopg2"
except Exception:  # pragma: no cover
    psycopg2 = None  # type: ignore


# ─────────────────────────────────────────────────────────────────────────────
# Embedded DDL (fallback only)
# ─────────────────────────────────────────────────────────────────────────────

DDL = r"""
-- ============================================================================
-- AI / Research tables (V3+++)
-- ============================================================================

-- 1) Core table for every entry candidate logged by scanners
CREATE TABLE IF NOT EXISTS entry_candidates (
    id                    BIGSERIAL PRIMARY KEY,
    ts                    TIMESTAMPTZ NOT NULL DEFAULT now(),
    src                   TEXT NOT NULL,
    mint                  TEXT NOT NULL,
    symbol                TEXT,
    regime                TEXT,
    wallet_tag            TEXT,
    feature_version       INT NOT NULL DEFAULT 1,
    ai_model_version      TEXT,
    features              JSONB NOT NULL,
    raw                   JSONB
);

-- Migrations / enrichments (idempotent)
ALTER TABLE entry_candidates
    ADD COLUMN IF NOT EXISTS created_at            TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS segment               TEXT,
    ADD COLUMN IF NOT EXISTS regime_label          TEXT,
    ADD COLUMN IF NOT EXISTS regime_label_numeric  DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS risk_capacity_score   DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS smart_wallet_score    DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS smart_wallet_flag     BOOLEAN,
    ADD COLUMN IF NOT EXISTS label_hint            TEXT,
    ADD COLUMN IF NOT EXISTS ai_entry_heur_score   DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS ai_entry_ai_score     DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS ai_entry_blended_score DOUBLE PRECISION;

-- Useful indexes
CREATE INDEX IF NOT EXISTS idx_entry_candidates_ts
    ON entry_candidates (ts DESC);
CREATE INDEX IF NOT EXISTS idx_entry_candidates_created_at
    ON entry_candidates (created_at DESC);
CREATE INDEX IF NOT EXISTS idx_entry_candidates_src_ts
    ON entry_candidates (src, ts DESC);
CREATE INDEX IF NOT EXISTS idx_entry_candidates_regime_ts
    ON entry_candidates (regime, ts DESC);
CREATE INDEX IF NOT EXISTS idx_entry_candidates_mint_ts
    ON entry_candidates (mint, ts DESC);
CREATE INDEX IF NOT EXISTS idx_entry_candidates_src_regime_ts
    ON entry_candidates (src, regime, ts DESC);
CREATE INDEX IF NOT EXISTS idx_entry_candidates_segment_ts
    ON entry_candidates (segment, ts DESC);
CREATE INDEX IF NOT EXISTS idx_entry_candidates_rc
    ON entry_candidates (risk_capacity_score);
CREATE INDEX IF NOT EXISTS idx_entry_candidates_sw_score
    ON entry_candidates (smart_wallet_score);

-- 2) Realized outcomes for candidates
CREATE TABLE IF NOT EXISTS entry_outcomes (
    id              BIGSERIAL PRIMARY KEY,
    candidate_id    BIGINT NOT NULL
        REFERENCES entry_candidates(id) ON DELETE CASCADE,

    label           TEXT NOT NULL,
    pnl_pct         DOUBLE PRECISION,
    max_dd_pct      DOUBLE PRECISION,
    hold_sec        INTEGER,
    exit_ts         TIMESTAMPTZ,
    notes           TEXT,
    extra           JSONB,

    ai_risk_score   DOUBLE PRECISION,
    ai_risk_conf    DOUBLE PRECISION,

    segment         TEXT,

    ts              TIMESTAMPTZ NOT NULL DEFAULT now()
);

ALTER TABLE entry_outcomes
    ADD COLUMN IF NOT EXISTS pnl_pct       DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS max_dd_pct    DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS hold_sec      INTEGER,
    ADD COLUMN IF NOT EXISTS exit_ts       TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS notes         TEXT,
    ADD COLUMN IF NOT EXISTS extra         JSONB,
    ADD COLUMN IF NOT EXISTS ai_risk_score DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS ai_risk_conf  DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS segment       TEXT,
    ADD COLUMN IF NOT EXISTS ts            TIMESTAMPTZ NOT NULL DEFAULT now();

-- Ensure unique candidate_id (upsert-safe)
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'entry_outcomes_candidate_id_key'
    ) THEN
        BEGIN
            ALTER TABLE entry_outcomes
                ADD CONSTRAINT entry_outcomes_candidate_id_key UNIQUE (candidate_id);
        EXCEPTION
            WHEN duplicate_table THEN NULL;
            WHEN duplicate_object THEN NULL;
        END;
    END IF;
END $$;

CREATE INDEX IF NOT EXISTS idx_entry_outcomes_candidate
    ON entry_outcomes (candidate_id);
CREATE INDEX IF NOT EXISTS idx_entry_outcomes_label_ts
    ON entry_outcomes (label, ts DESC);
CREATE INDEX IF NOT EXISTS idx_entry_outcomes_ts
    ON entry_outcomes (ts DESC);
CREATE INDEX IF NOT EXISTS idx_entry_outcomes_segment_ts
    ON entry_outcomes (segment, ts DESC);

-- 3) Backtest run summaries
CREATE TABLE IF NOT EXISTS ai_backtest_runs (
    id          BIGSERIAL PRIMARY KEY,
    ts          TIMESTAMPTZ NOT NULL DEFAULT now(),
    tag         TEXT,
    notes       TEXT,
    config_json JSONB NOT NULL,
    stats_json  JSONB NOT NULL
);

ALTER TABLE ai_backtest_runs
    ADD COLUMN IF NOT EXISTS ts          TIMESTAMPTZ NOT NULL DEFAULT now(),
    ADD COLUMN IF NOT EXISTS tag         TEXT,
    ADD COLUMN IF NOT EXISTS notes       TEXT,
    ADD COLUMN IF NOT EXISTS config_json JSONB,
    ADD COLUMN IF NOT EXISTS stats_json  JSONB;

CREATE INDEX IF NOT EXISTS idx_ai_backtest_runs_ts
    ON ai_backtest_runs (ts DESC);
CREATE INDEX IF NOT EXISTS idx_ai_backtest_runs_tag_ts
    ON ai_backtest_runs (tag, ts DESC);

-- 4) Backtest trade-level logs (optional)
CREATE TABLE IF NOT EXISTS ai_backtest_trades (
    id          BIGSERIAL PRIMARY KEY,
    run_id      BIGINT NOT NULL
        REFERENCES ai_backtest_runs(id) ON DELETE CASCADE,
    trade_index INTEGER NOT NULL,
    ts          TIMESTAMPTZ NOT NULL DEFAULT now(),
    trade_json  JSONB NOT NULL
);

ALTER TABLE ai_backtest_trades
    ADD COLUMN IF NOT EXISTS run_id      BIGINT,
    ADD COLUMN IF NOT EXISTS trade_index INTEGER,
    ADD COLUMN IF NOT EXISTS ts          TIMESTAMPTZ NOT NULL DEFAULT now(),
    ADD COLUMN IF NOT EXISTS trade_json  JSONB;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'ai_backtest_trades_run_id_trade_index_key'
    ) THEN
        BEGIN
            ALTER TABLE ai_backtest_trades
                ADD CONSTRAINT ai_backtest_trades_run_id_trade_index_key UNIQUE (run_id, trade_index);
        EXCEPTION
            WHEN duplicate_table THEN NULL;
            WHEN duplicate_object THEN NULL;
        END;
    END IF;
END $$;

CREATE INDEX IF NOT EXISTS idx_ai_backtest_trades_run_ts
    ON ai_backtest_trades (run_id, ts DESC);

-- 5) Self-learning tune events (optional)
CREATE TABLE IF NOT EXISTS ai_self_learning_tunes (
    id                    BIGSERIAL PRIMARY KEY,
    ts                    TIMESTAMPTZ NOT NULL DEFAULT now(),
    run_id                BIGINT,
    profile_name          TEXT,
    win_rate              DOUBLE PRECISION,
    window_pnl_pct        DOUBLE PRECISION,
    new_score_min         DOUBLE PRECISION,
    new_blended_gate      DOUBLE PRECISION,
    new_risk_mult         DOUBLE PRECISION,
    meta                  JSONB
);

ALTER TABLE ai_self_learning_tunes
    ADD COLUMN IF NOT EXISTS ts               TIMESTAMPTZ NOT NULL DEFAULT now(),
    ADD COLUMN IF NOT EXISTS run_id           BIGINT,
    ADD COLUMN IF NOT EXISTS profile_name     TEXT,
    ADD COLUMN IF NOT EXISTS win_rate         DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS window_pnl_pct   DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS new_score_min    DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS new_blended_gate DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS new_risk_mult    DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS meta             JSONB;

CREATE INDEX IF NOT EXISTS idx_ai_self_learning_tunes_ts
    ON ai_self_learning_tunes (ts DESC);
CREATE INDEX IF NOT EXISTS idx_ai_self_learning_tunes_profile_ts
    ON ai_self_learning_tunes (profile_name, ts DESC);
"""


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────


def _utc_now() -> str:
    return datetime.now(UTC).isoformat()


def mask_dsn(raw: str) -> str:
    """
    Mask DSN so we never print secrets. Keeps host/db/query (drops user:pass).
    """
    if not raw:
        return "<empty>"
    try:
        # handle URI-ish DSNs
        if "://" in raw:
            scheme, rest = raw.split("://", 1)
            rest = re.sub(r"^[^@/]*@", "", rest)  # drop userinfo@
            return f"{scheme}://{rest}"
        # fallback: redact password-like tokens
        return re.sub(r"password\s*=\s*\S+", "password=<redacted>", raw, flags=re.I)
    except Exception:
        return "<dsn>"


def _bool_env(key: str, default: bool = False) -> bool:
    v = os.getenv(key)
    if v is None:
        return default
    return str(v).strip().lower() in {"1", "true", "yes", "y", "on"}


def resolve_dsn(cli_dsn: str) -> str:
    """
    Prefer:
      1) CLI --dsn
      2) ai_db's canonical DSN resolver/normalizer if exposed
      3) ai_db private _get_dsn() (internal-only fallback)
    """
    if cli_dsn and cli_dsn.strip():
        d = cli_dsn.strip()
        # If ai_db exposes normalizer, use it for exact parity
        norm = getattr(ai_db, "normalize_dsn", None)
        if callable(norm):
            try:
                d = norm(d)  # type: ignore[misc]
            except Exception:
                pass
        return d

    # public-ish helpers first
    for name in ("resolve_dsn", "get_dsn", "dsn", "get_postgres_dsn"):
        fn = getattr(ai_db, name, None)
        if callable(fn):
            try:
                d = fn()  # type: ignore[misc]
                if isinstance(d, str) and d.strip():
                    return d.strip()
            except Exception:
                pass

    # internal fallback
    try:
        d = ai_db._get_dsn()  # type: ignore[attr-defined]
        if not isinstance(d, str) or not d.strip():
            raise RuntimeError("ai_db._get_dsn() returned empty DSN")
        return d.strip()
    except Exception as e:
        raise RuntimeError(f"Failed to resolve DSN via utils_pkg.ai_db: {e!r}") from e


def describe_backend_safe() -> str:
    try:
        return str(ai_db.describe_backend())
    except Exception as e:
        return f"<unknown backend: {e!r}>"


def _driver_available() -> bool:
    return _PSYCOPG_KIND in ("psycopg3", "psycopg2")


def _connect(dsn: str):
    """
    Return a connection using psycopg3 or psycopg2. Caller closes it.
    """
    if _PSYCOPG_KIND == "psycopg3":
        # psycopg.connect supports context-managed transactions
        return psycopg.connect(dsn)  # type: ignore[call-arg]
    if _PSYCOPG_KIND == "psycopg2":
        return psycopg2.connect(dsn)  # type: ignore[call-arg]
    raise RuntimeError("No Postgres driver found (install psycopg[binary] or psycopg2).")


def _exec_sql(conn, sql: str) -> None:
    if _PSYCOPG_KIND == "psycopg3":
        with conn.cursor() as cur:
            cur.execute(sql)
    else:
        with conn.cursor() as cur:
            cur.execute(sql)


def run_fallback_ddl(dsn: str) -> None:
    """
    Execute embedded DDL inside a single transaction when possible.
    """
    if not _driver_available():
        raise RuntimeError("Postgres driver missing; cannot run DDL fallback.")

    conn = _connect(dsn)
    try:
        if _PSYCOPG_KIND == "psycopg3":
            # psycopg3 uses implicit transaction; commit at end
            _exec_sql(conn, DDL)
            conn.commit()
        else:
            # psycopg2: transactional by default
            _exec_sql(conn, DDL)
            conn.commit()
    except Exception:
        with contextlib.suppress(Exception):
            conn.rollback()
        raise
    finally:
        with contextlib.suppress(Exception):
            conn.close()


async def maybe_delegate_to_ai_db() -> bool:
    """
    Prefer canonical schema initializer in ai_db if it exists.
    Returns True if we delegated successfully, else False.
    """
    # Common names we might have in ai_db
    candidates: Sequence[str] = (
        "ensure_schema",
        "init_schema",
        "create_schema",
        "migrate_schema",
        "ensure_ai_schema",
        "init_ai_schema",
        "bootstrap",
    )

    for name in candidates:
        fn = getattr(ai_db, name, None)
        if not callable(fn):
            continue
        try:
            if asyncio.iscoroutinefunction(fn):
                await fn()  # type: ignore[misc]
            else:
                fn()  # type: ignore[misc]
            return True
        except Exception as e:
            log.warning("ai_db.%s exists but failed (%r); will fallback to embedded DDL", name, e)
            return False

    return False


def verify_schema(dsn: str) -> dict[str, Any]:
    """
    Verify table existence and basic row/column presence (lightweight).
    """
    want_tables = [
        "entry_candidates",
        "entry_outcomes",
        "ai_backtest_runs",
        "ai_backtest_trades",
        "ai_self_learning_tunes",
    ]

    if not _driver_available():
        return {
            "ok": False,
            "error": "No Postgres driver found; cannot verify schema.",
            "driver": _PSYCOPG_KIND,
        }

    conn = _connect(dsn)
    try:

        def q(sql: str, params: tuple[Any, ...] | None = None) -> list[tuple[Any, ...]]:
            if _PSYCOPG_KIND == "psycopg3":
                with conn.cursor() as cur:
                    cur.execute(sql, params or ())
                    return list(cur.fetchall())
            with conn.cursor() as cur:
                cur.execute(sql, params or ())
                return list(cur.fetchall())

        rows = q(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
              AND table_name = ANY(%s)
            """,
            (want_tables,),
        )
        present = sorted({r[0] for r in rows})
        missing = [t for t in want_tables if t not in present]

        # Column spot checks (kept minimal)
        col_checks = {
            "entry_candidates": ["id", "ts", "src", "mint", "features"],
            "entry_outcomes": ["id", "candidate_id", "label", "ts"],
            "ai_backtest_runs": ["id", "ts", "config_json", "stats_json"],
        }
        cols_present: dict[str, list[str]] = {}
        cols_missing: dict[str, list[str]] = {}

        for table, cols in col_checks.items():
            if table not in present:
                cols_missing[table] = cols
                continue
            crows = q(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema='public' AND table_name=%s
                """,
                (table,),
            )
            have = {r[0] for r in crows}
            cols_present[table] = sorted(list(have))
            cols_missing[table] = [c for c in cols if c not in have]

        ok = (len(missing) == 0) and all(len(v) == 0 for v in cols_missing.values())
        return {
            "ok": ok,
            "tables_present": present,
            "tables_missing": missing,
            "column_missing": {k: v for k, v in cols_missing.items() if v},
            "driver": _PSYCOPG_KIND,
        }
    finally:
        with contextlib.suppress(Exception):
            conn.close()


# ─────────────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────────────


@dataclass
class Result:
    ok: bool
    ts: str
    backend: str
    driver: str
    dsn_masked: str
    delegated_to_ai_db: bool
    dry_run: bool
    verify: dict[str, Any] | None = None
    error: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "ok": self.ok,
            "ts": self.ts,
            "backend": self.backend,
            "driver": self.driver,
            "dsn": self.dsn_masked,
            "delegated_to_ai_db": self.delegated_to_ai_db,
            "dry_run": self.dry_run,
            "verify": self.verify,
            "error": self.error,
        }


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Initialize/migrate AI tables (V3+++). Idempotent & CI-friendly."
    )
    p.add_argument(
        "--dsn",
        default="",
        help="Optional explicit Postgres DSN. Otherwise uses utils_pkg.ai_db resolver.",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Resolve DSN/backend and exit without executing DDL.",
    )
    p.add_argument(
        "--verify",
        action="store_true",
        help="Verify schema after init (queries information_schema).",
    )
    p.add_argument(
        "--json",
        action="store_true",
        help="Emit machine-readable JSON result.",
    )
    p.add_argument(
        "--allow-sqlite",
        action="store_true",
        help="Allow non-Postgres backend (LEGACY ONLY; not recommended).",
    )
    return p


def main(argv: list[str] | None = None) -> None:
    args = build_parser().parse_args(argv)

    backend = describe_backend_safe()
    dsn = ""
    delegated = False
    verify_out: dict[str, Any] | None = None

    try:
        dsn = resolve_dsn(args.dsn)
        dsn_masked = mask_dsn(dsn)

        # Guard: sqlite/other backends are not allowed for P0 unless explicitly opted in
        if (not args.allow_sqlite) and ("sqlite" in backend.lower() or "memory" in backend.lower()):
            raise RuntimeError(
                f"Non-Postgres backend detected ({backend}). "
                f"Set Postgres DSN env vars (P0) or run with --allow-sqlite (legacy only)."
            )

        # Driver guard if we need to run fallback
        if not args.dry_run and not _driver_available():
            # We still might delegate to ai_db if it uses its own driver; keep going,
            # but if no delegator exists we'll fail later.
            log.warning("No psycopg driver found in this env (psycopg/psycopg2).")

        if args.dry_run:
            res = Result(
                ok=True,
                ts=_utc_now(),
                backend=backend,
                driver=_PSYCOPG_KIND,
                dsn_masked=dsn_masked,
                delegated_to_ai_db=False,
                dry_run=True,
            )
            if args.json:
                print(json.dumps(res.to_dict(), indent=2, sort_keys=True, default=str))
            else:
                print("Dry run (no DDL executed).")
                print(f"backend : {backend}")
                print(f"driver  : {_PSYCOPG_KIND}")
                print(f"dsn     : {dsn_masked}")
            return

        # Preferred path: delegate schema init to ai_db if available
        delegated = asyncio.run(maybe_delegate_to_ai_db())

        # Fallback path: run embedded DDL
        if not delegated:
            log.info("Running embedded DDL fallback (aligned with V3+++).")
            run_fallback_ddl(dsn)

        if args.verify:
            verify_out = verify_schema(dsn)

        ok = True if not verify_out else bool(verify_out.get("ok", False))
        res = Result(
            ok=ok,
            ts=_utc_now(),
            backend=backend,
            driver=_PSYCOPG_KIND,
            dsn_masked=dsn_masked,
            delegated_to_ai_db=delegated,
            dry_run=False,
            verify=verify_out,
        )

        if args.json:
            print(json.dumps(res.to_dict(), indent=2, sort_keys=True, default=str))
        else:
            print("✅ AI tables init complete.")
            print(f"backend : {backend}")
            print(f"driver  : {_PSYCOPG_KIND}")
            print(f"dsn     : {dsn_masked}")
            print(f"path    : {'ai_db delegator' if delegated else 'embedded DDL'}")
            if verify_out is not None:
                print(f"verify  : {'OK' if verify_out.get('ok') else 'FAIL'}")
                if verify_out.get("tables_missing"):
                    print("missing tables:", verify_out["tables_missing"])
                if verify_out.get("column_missing"):
                    print("missing columns:", verify_out["column_missing"])

        sys.exit(0 if ok else 1)

    except Exception as e:
        dsn_masked = mask_dsn(dsn) if dsn else "<unresolved>"
        res = Result(
            ok=False,
            ts=_utc_now(),
            backend=backend,
            driver=_PSYCOPG_KIND,
            dsn_masked=dsn_masked,
            delegated_to_ai_db=delegated,
            dry_run=bool(args.dry_run),
            verify=verify_out,
            error=str(e),
        )
        if args.json:
            print(json.dumps(res.to_dict(), indent=2, sort_keys=True, default=str))
        else:
            print("❌ init_ai_tables failed.")
            print(f"backend : {backend}")
            print(f"driver  : {_PSYCOPG_KIND}")
            print(f"dsn     : {dsn_masked}")
            print(f"error   : {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
