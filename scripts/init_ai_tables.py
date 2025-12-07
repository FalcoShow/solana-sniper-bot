# scripts/init_ai_tables.py
from __future__ import annotations

"""
scripts/init_ai_tables.py

Quant-grade initializer / migrator for AI training / research tables (V3+++).
=============================================================================

High-level goals
----------------
• Use the SAME DSN resolution / normalization as utils_pkg.ai_db
  (SNIPER_DB_DSN / DATABASE_URL / Render URLs, sslmode=require, etc.).
• Create / migrate all core AI tables used by:
      - utils_pkg.ai_db
      - utils_pkg.data_logger
      - utils_pkg.backtest_engine
      - utils_pkg.self_learning_ai_strategy
      - utils_pkg.ai_risk_model (optional)

Core tables (V3+++)
-------------------
1) entry_candidates
    • One row per *entry candidate* (scanner hit) we log.
    • Stores features JSON, raw snapshot, regime, smart-wallet flags, RC, etc.
    • Feature_version + ai_model_version so the feature set can evolve safely.

2) entry_outcomes
    • One row per realized *outcome* of a candidate.
    • Label + PnL% + hold_sec + drawdown + AI risk info, etc.
    • UNIQUE(candidate_id) so mark_outcome_for_candidate(...) can upsert cleanly.

3) ai_backtest_runs
    • One row per backtest run (config + aggregate stats_json).

4) ai_backtest_trades
    • Per-trade backtest records (optional, controlled by BT_LOG_TRADES).

5) ai_self_learning_tunes
    • Optional table for self-learning meta-tuning events over time.

Usage
-----
With DSN in env (recommended: SNIPER_DB_DSN / DATABASE_URL):

    python scripts/init_ai_tables.py

Optionally override DSN manually:

    python scripts/init_ai_tables.py --dsn "postgresql://user:pass@host/db?sslmode=require"

Optional flags:

    --dry-run     Show DSN/backend only, do not execute DDL.
"""

import argparse
import sys
from typing import Optional, List

import psycopg2

from utils_pkg import ai_db
from logging_setup import get_logger

log = get_logger(__name__)

# ──────────────────────────────────────────────────────────────────────────────
# DDL: main schema (aligned with ai_db + data_logger + backtest_engine)
# ──────────────────────────────────────────────────────────────────────────────
#
# Notes:
#   • We use TIMESTAMPTZ columns ("ts" / "created_at" / "exit_ts") for alignment
#     with ai_db / backtest_engine expectations.
#   • We keep CREATE TABLE IF NOT EXISTS and ALTER TABLE ... ADD COLUMN IF NOT EXISTS
#     so the script is idempotent and safe to re-run.
#   • Extra columns are added with generic ALTERs so older schemas are upgraded.
#
DDL = r"""
-- 1) Core table for every entry candidate logged by the scanners
CREATE TABLE IF NOT EXISTS entry_candidates (
    id                  BIGSERIAL PRIMARY KEY,

    -- canonical event timestamp
    ts                  TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- where it came from: 'raydium', 'pumpfun', 'dexscreener', 'webhook', ...
    src                 TEXT NOT NULL,

    -- core asset info
    mint                TEXT NOT NULL,
    symbol              TEXT,
    regime              TEXT,     -- e.g. 'raydium_live', 'pumpfun_live'
    wallet_tag          TEXT,     -- e.g. smart wallet cluster / tag

    -- feature + model versioning
    feature_version     INT NOT NULL DEFAULT 1,
    ai_model_version    TEXT,     -- logical name / version of AI entry model

    -- numeric + categorical features for AI entry logic
    features            JSONB NOT NULL,

    -- catch-all raw snapshot (orderbook, meta, scanner debug, etc.)
    raw                 JSONB
);

-- Backwards-compatible migrations: keep older installs up to date
ALTER TABLE entry_candidates
    ADD COLUMN IF NOT EXISTS ts                  TIMESTAMPTZ NOT NULL DEFAULT now(),
    ADD COLUMN IF NOT EXISTS src                 TEXT,
    ADD COLUMN IF NOT EXISTS mint                TEXT,
    ADD COLUMN IF NOT EXISTS symbol              TEXT,
    ADD COLUMN IF NOT EXISTS regime              TEXT,
    ADD COLUMN IF NOT EXISTS wallet_tag          TEXT,
    ADD COLUMN IF NOT EXISTS feature_version     INT NOT NULL DEFAULT 1,
    ADD COLUMN IF NOT EXISTS ai_model_version    TEXT,
    ADD COLUMN IF NOT EXISTS features            JSONB,
    ADD COLUMN IF NOT EXISTS raw                 JSONB;

-- Enriched V3+++ candidate-level helpers used by FeatureBuilder / backtests
ALTER TABLE entry_candidates
    ADD COLUMN IF NOT EXISTS created_at          TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS segment             TEXT,         -- 'train','test','val', etc.
    ADD COLUMN IF NOT EXISTS regime_label        TEXT,
    ADD COLUMN IF NOT EXISTS regime_label_numeric DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS risk_capacity_score DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS smart_wallet_score  DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS smart_wallet_flag   BOOLEAN,
    ADD COLUMN IF NOT EXISTS label_hint          TEXT,         -- coarse label hint at entry
    ADD COLUMN IF NOT EXISTS ai_entry_heur_score DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS ai_entry_ai_score   DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS ai_entry_blended_score DOUBLE PRECISION;

-- Helpful indexes for research / backtests
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

-- 2) Table for realized outcomes for those candidates
CREATE TABLE IF NOT EXISTS entry_outcomes (
    id                  BIGSERIAL PRIMARY KEY,
    candidate_id        BIGINT NOT NULL
        REFERENCES entry_candidates(id) ON DELETE CASCADE,

    -- final labeling + PnL stats
    label               TEXT NOT NULL,        -- e.g. 'mega_win','win','loss','rug',...
    pnl_pct             DOUBLE PRECISION,
    max_dd_pct          DOUBLE PRECISION,
    hold_sec            INTEGER,
    exit_ts             TIMESTAMPTZ,
    notes               TEXT,
    extra               JSONB,

    -- AI risk overlay snapshot (if known at exit time)
    ai_risk_score       DOUBLE PRECISION,
    ai_risk_conf        DOUBLE PRECISION,

    -- which dataset split this outcome belongs to (optional)
    segment             TEXT,

    ts                  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Migrate older schema variants if needed
ALTER TABLE entry_outcomes
    ADD COLUMN IF NOT EXISTS pnl_pct        DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS max_dd_pct     DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS hold_sec       INTEGER,
    ADD COLUMN IF NOT EXISTS exit_ts        TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS notes          TEXT,
    ADD COLUMN IF NOT EXISTS extra          JSONB,
    ADD COLUMN IF NOT EXISTS ai_risk_score  DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS ai_risk_conf   DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS segment        TEXT,
    ADD COLUMN IF NOT EXISTS ts             TIMESTAMPTZ NOT NULL DEFAULT now();

-- Ensure candidate_id is unique so mark_outcome_for_candidate(...) can upsert cleanly
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM   pg_constraint
        WHERE  conname = 'entry_outcomes_candidate_id_key'
    ) THEN
        BEGIN
            ALTER TABLE entry_outcomes
                ADD CONSTRAINT entry_outcomes_candidate_id_key
                UNIQUE (candidate_id);
        EXCEPTION
            WHEN duplicate_table THEN
                NULL;
            WHEN duplicate_object THEN
                NULL;
        END;
    END IF;
END $$;

-- Indexes for outcomes
CREATE INDEX IF NOT EXISTS idx_entry_outcomes_candidate
    ON entry_outcomes (candidate_id);

CREATE INDEX IF NOT EXISTS idx_entry_outcomes_label_ts
    ON entry_outcomes (label, ts DESC);

CREATE INDEX IF NOT EXISTS idx_entry_outcomes_ts
    ON entry_outcomes (ts DESC);

CREATE INDEX IF NOT EXISTS idx_entry_outcomes_segment_ts
    ON entry_outcomes (segment, ts DESC);

-- 3) Backtest run summaries (used by utils_pkg.data_logger / backtest_engine)
CREATE TABLE IF NOT EXISTS ai_backtest_runs (
    id              BIGSERIAL PRIMARY KEY,
    ts              TIMESTAMPTZ NOT NULL DEFAULT now(),
    tag             TEXT,          -- config_tag / BT_TAG_PREFIX
    notes           TEXT,
    config_json     JSONB NOT NULL,
    stats_json      JSONB NOT NULL
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

-- 4) Backtest trade-level logs (optional, when BT_LOG_TRADES is enabled)
CREATE TABLE IF NOT EXISTS ai_backtest_trades (
    id              BIGSERIAL PRIMARY KEY,
    run_id          BIGINT NOT NULL
        REFERENCES ai_backtest_runs(id) ON DELETE CASCADE,
    trade_index     INTEGER NOT NULL,     -- chronological index within that run
    ts              TIMESTAMPTZ NOT NULL DEFAULT now(),
    trade_json      JSONB NOT NULL
);

ALTER TABLE ai_backtest_trades
    ADD COLUMN IF NOT EXISTS run_id      BIGINT,
    ADD COLUMN IF NOT EXISTS trade_index INTEGER,
    ADD COLUMN IF NOT EXISTS ts          TIMESTAMPTZ NOT NULL DEFAULT now(),
    ADD COLUMN IF NOT EXISTS trade_json  JSONB;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM   pg_constraint
        WHERE  conname = 'ai_backtest_trades_run_id_trade_index_key'
    ) THEN
        BEGIN
            ALTER TABLE ai_backtest_trades
                ADD CONSTRAINT ai_backtest_trades_run_id_trade_index_key
                UNIQUE (run_id, trade_index);
        EXCEPTION
            WHEN duplicate_table THEN
                NULL;
            WHEN duplicate_object THEN
                NULL;
        END;
    END IF;
END $$;

CREATE INDEX IF NOT EXISTS idx_ai_backtest_trades_run_ts
    ON ai_backtest_trades (run_id, ts DESC);

-- 5) Self-learning tune events (optional; used by self_learning_ai_strategy)
CREATE TABLE IF NOT EXISTS ai_self_learning_tunes (
    id                  BIGSERIAL PRIMARY KEY,
    ts                  TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- optional association to a backtest run or live profile name
    run_id              BIGINT,
    profile_name        TEXT,

    -- window performance summary when the tune was applied
    win_rate            DOUBLE PRECISION,
    window_pnl_pct      DOUBLE PRECISION,

    -- knobs after tuning
    new_score_min       DOUBLE PRECISION,
    new_blended_gate    DOUBLE PRECISION,
    new_risk_mult       DOUBLE PRECISION,

    meta                JSONB
);

ALTER TABLE ai_self_learning_tunes
    ADD COLUMN IF NOT EXISTS ts              TIMESTAMPTZ NOT NULL DEFAULT now(),
    ADD COLUMN IF NOT EXISTS run_id          BIGINT,
    ADD COLUMN IF NOT EXISTS profile_name    TEXT,
    ADD COLUMN IF NOT EXISTS win_rate        DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS window_pnl_pct  DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS new_score_min   DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS new_blended_gate DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS new_risk_mult   DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS meta            JSONB;

CREATE INDEX IF NOT EXISTS idx_ai_self_learning_tunes_ts
    ON ai_self_learning_tunes (ts DESC);

CREATE INDEX IF NOT EXISTS idx_ai_self_learning_tunes_profile_ts
    ON ai_self_learning_tunes (profile_name, ts DESC);
"""


# ──────────────────────────────────────────────────────────────────────────────
# DSN resolution
# ──────────────────────────────────────────────────────────────────────────────


def resolve_dsn(cli_dsn: Optional[str]) -> str:
    """
    Resolve DSN from CLI or reuse utils_pkg.ai_db's resolution.

    Priority:
      1) --dsn argument if provided
      2) ai_db._get_dsn() (same as rest of bot – SNIPER_DB_DSN / DATABASE_URL / etc.)
    """
    if cli_dsn and cli_dsn.strip():
        dsn = cli_dsn.strip()
        print(f"Using explicit DSN from CLI (masked): {mask_dsn(dsn)}")
        return dsn

    # Fall back to ai_db's internal resolver/normalizer
    try:
        # _get_dsn is "private" by convention but safe to call from internal script.
        dsn = ai_db._get_dsn()  # type: ignore[attr-defined]
        print(f"Using DSN from env via utils_pkg.ai_db (masked): {mask_dsn(dsn)}")
        return dsn
    except Exception as e:  # pragma: no cover
        print(f"ERROR: Failed to resolve DSN via utils_pkg.ai_db: {e}")
        raise


def mask_dsn(raw: str) -> str:
    """
    Lightly mask a DSN for printing (hide user/password).

    Examples:
        'postgresql://user:pass@host:5432/db?sslmode=require'
            → 'host:5432/db?sslmode=require'
    """
    try:
        if "://" in raw:
            _, rest = raw.split("://", 1)
        else:
            rest = raw
        if "@" in rest:
            _, hostpart = rest.split("@", 1)
        else:
            hostpart = rest
        return hostpart
    except Exception:
        return "<dsn>"


# ──────────────────────────────────────────────────────────────────────────────
# Main DDL runner
# ──────────────────────────────────────────────────────────────────────────────


def run_ddl(dsn: str, ddl_blocks: List[str]) -> None:
    """
    Connect to Postgres and run the given DDL block(s).

    We use autocommit=True so multi-statement blocks work cleanly.
    """
    print(f"Connecting to Postgres at: {mask_dsn(dsn)}")
    conn = psycopg2.connect(dsn)
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            for idx, ddl in enumerate(ddl_blocks, start=1):
                print(f"Running DDL block {idx}/{len(ddl_blocks)}...")
                cur.execute(ddl)
        print("Done. Tables & indexes should now be in place.")
    finally:
        conn.close()


def main(argv: Optional[list[str]] = None) -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Initialize or migrate AI training / research tables "
            "(entry_candidates, entry_outcomes, ai_backtest_*, ai_self_learning_*)."
        )
    )
    parser.add_argument(
        "--dsn",
        help="Optional explicit Postgres DSN (otherwise utils_pkg.ai_db resolution is used).",
        default="",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print DSN/backend and exit without executing DDL.",
    )
    args = parser.parse_args(argv)

    dsn = resolve_dsn(args.dsn)

    # Show backend info if available
    try:
        backend = ai_db.describe_backend()
        print(f"Current backend: {backend}")
    except Exception as e:  # pragma: no cover
        print(f"Warning: ai_db.describe_backend() failed: {e}")

    if args.dry_run:
        print("Dry run requested – not executing DDL.")
        return

    try:
        run_ddl(dsn, [DDL])
    except Exception as e:
        print(f"ERROR: DDL execution failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
