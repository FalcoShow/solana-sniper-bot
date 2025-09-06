# db/schema.py — v2.0 (expanded for v7 logger/sniper)
from __future__ import annotations

import os
import asyncio
import asyncpg

DATABASE_URL = os.getenv("DATABASE_URL")

DDL_CORE = """
-- =========================
-- Legacy tables (kept same)
-- =========================
CREATE TABLE IF NOT EXISTS trade_candidates (
  id BIGSERIAL PRIMARY KEY,
  ts TIMESTAMPTZ DEFAULT now(),
  mint TEXT,
  symbol TEXT,
  source TEXT,
  pool_age_min REAL,
  liq_usd REAL,
  impact_1k_bps REAL,
  score REAL
);

CREATE TABLE IF NOT EXISTS trades (
  id BIGSERIAL PRIMARY KEY,
  ts TIMESTAMPTZ DEFAULT now(),
  mint TEXT,
  entry_px NUMERIC,
  exit_px NUMERIC,
  size_usd NUMERIC,
  pnl_usd NUMERIC,
  reason TEXT
);

-- =========================
-- New tables for v7 logger
-- =========================
-- Per-scan summary (how many seen vs kept, why)
CREATE TABLE IF NOT EXISTS scan_batches (
  id BIGSERIAL PRIMARY KEY,
  ts TIMESTAMPTZ NOT NULL DEFAULT now(),
  total INT,
  kept INT,
  reason TEXT
);

-- Per-candidate row (ranked/acted), with full JSON payload for forensics
CREATE TABLE IF NOT EXISTS scan_rows (
  id BIGSERIAL PRIMARY KEY,
  ts TIMESTAMPTZ NOT NULL DEFAULT now(),
  mint TEXT,
  symbol TEXT,
  source TEXT,
  pool_age_min REAL,
  liq_usd REAL,
  impact_1k_bps REAL,
  score REAL,
  rank INT,
  preset TEXT,
  action TEXT,     -- e.g. 'buy_ok', 'skip_hp', 'rejected_ai', etc.
  extras JSONB
);

CREATE TABLE IF NOT EXISTS pnl_snapshots (
  id BIGSERIAL PRIMARY KEY,
  ts TIMESTAMPTZ DEFAULT now(),
  equity_usd NUMERIC,
  open_risk_usd NUMERIC
);
CREATE INDEX IF NOT EXISTS idx_pnl_snapshots_ts ON pnl_snapshots(ts);

-- Trade events (buy/sell/fail etc.)
CREATE TABLE IF NOT EXISTS trade_events (
  id BIGSERIAL PRIMARY KEY,
  ts TIMESTAMPTZ NOT NULL DEFAULT now(),
  type TEXT,                       -- 'buy','sell','buy_fail','legacy_trade',...
  mint TEXT,
  symbol TEXT,
  entry_sol_per_token DOUBLE PRECISION,
  sol_spent DOUBLE PRECISION,
  usd_spent DOUBLE PRECISION,
  signature TEXT,
  cu_price INT,
  jito_tip BIGINT,
  data JSONB
);

-- Error events (so errors don’t get lost in stdout logs)
CREATE TABLE IF NOT EXISTS error_events (
  id BIGSERIAL PRIMARY KEY,
  ts TIMESTAMPTZ NOT NULL DEFAULT now(),
  "where" TEXT,
  error TEXT
);

-- Equity snapshots for PnL curves
CREATE TABLE IF NOT EXISTS pnl_snapshots (
  id BIGSERIAL PRIMARY KEY,
  ts TIMESTAMPTZ NOT NULL,
  equity_usd DOUBLE PRECISION,
  open_risk_usd DOUBLE PRECISION
);
"""

DDL_INDEXES = """
-- Light, non-concurrent indexes (safe in a single transaction)
CREATE INDEX IF NOT EXISTS trade_candidates_mint_ts_idx ON trade_candidates (mint, ts DESC);
CREATE INDEX IF NOT EXISTS trades_mint_ts_idx           ON trades (mint, ts DESC);

CREATE INDEX IF NOT EXISTS scan_rows_mint_ts_idx        ON scan_rows (mint, ts DESC);
CREATE INDEX IF NOT EXISTS scan_rows_action_ts_idx      ON scan_rows (action, ts DESC);

CREATE INDEX IF NOT EXISTS trade_events_mint_ts_idx     ON trade_events (mint, ts DESC);
CREATE INDEX IF NOT EXISTS trade_events_type_ts_idx     ON trade_events (type, ts DESC);

CREATE INDEX IF NOT EXISTS error_events_where_ts_idx    ON error_events ("where", ts DESC);

CREATE INDEX IF NOT EXISTS pnl_snapshots_ts_idx         ON pnl_snapshots (ts DESC);
"""

async def ensure_schema():
    """
    Creates all required tables and indexes. Idempotent.
    """
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL not set")

    conn: asyncpg.Connection
    conn = await asyncpg.connect(DATABASE_URL)
    try:
        # One transaction for core tables + indexes
        async with conn.transaction():
            await conn.execute(DDL_CORE)
            await conn.execute(DDL_INDEXES)
    finally:
        await conn.close()

# Handy CLI usage: `python -m db.schema`
if __name__ == "__main__":
    asyncio.run(ensure_schema())
