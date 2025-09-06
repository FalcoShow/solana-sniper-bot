# db/schema.py
import os, logging, asyncpg
log = logging.getLogger("db.schema")

DDL = """
CREATE TABLE IF NOT EXISTS scan_batches(
  id BIGSERIAL PRIMARY KEY,
  ts TIMESTAMPTZ NOT NULL DEFAULT now(),
  total INTEGER NOT NULL,
  kept INTEGER NOT NULL,
  reason_top TEXT
);

CREATE TABLE IF NOT EXISTS scan_rows(
  id BIGSERIAL PRIMARY KEY,
  ts TIMESTAMPTZ NOT NULL DEFAULT now(),
  source TEXT,
  mint TEXT,
  symbol TEXT,
  score DOUBLE PRECISION,
  liq_usd DOUBLE PRECISION,
  impact_1k_bps DOUBLE PRECISION
);

CREATE TABLE IF NOT EXISTS trades(
  id BIGSERIAL PRIMARY KEY,
  ts TIMESTAMPTZ NOT NULL DEFAULT now(),
  type TEXT NOT NULL,
  mint TEXT,
  symbol TEXT,
  sol_spent DOUBLE PRECISION,
  usd_spent DOUBLE PRECISION,
  entry_sol_per_token DOUBLE PRECISION,
  signature TEXT,
  cu_price INTEGER,
  jito_tip INTEGER
);

CREATE TABLE IF NOT EXISTS errors(
  id BIGSERIAL PRIMARY KEY,
  ts TIMESTAMPTZ NOT NULL DEFAULT now(),
  where_ TEXT,
  err TEXT
);

CREATE TABLE IF NOT EXISTS pnl_snapshots(
  id BIGSERIAL PRIMARY KEY,
  ts TIMESTAMPTZ NOT NULL,
  equity_usd NUMERIC,
  open_risk_usd NUMERIC
);
"""

MIGRATIONS = """
-- scan_rows: token_address -> mint (old builds)
DO $$ BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name='scan_rows' AND column_name='token_address'
    ) AND NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name='scan_rows' AND column_name='mint'
    ) THEN
        EXECUTE 'ALTER TABLE scan_rows RENAME COLUMN token_address TO mint';
    END IF;
END $$;

-- trades: token_address -> mint (old builds)
DO $$ BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name='trades' AND column_name='token_address'
    ) AND NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name='trades' AND column_name='mint'
    ) THEN
        EXECUTE 'ALTER TABLE trades RENAME COLUMN token_address TO mint';
    END IF;
END $$;
"""

INDEXES = """
CREATE INDEX IF NOT EXISTS idx_scan_rows_mint ON scan_rows(mint);
CREATE INDEX IF NOT EXISTS idx_trades_mint ON trades(mint);
CREATE INDEX IF NOT EXISTS idx_trades_ts ON trades(ts);
CREATE INDEX IF NOT EXISTS idx_snapshots_ts ON pnl_snapshots(ts);
"""

async def ensure_schema():
    url = (
        os.getenv("DATABASE_URL")
        or os.getenv("PG_DSN_INTERNAL")
        or os.getenv("PG_DSN_EXTERNAL")
    )
    if not url:
        raise RuntimeError("DATABASE_URL not set")

    conn = await asyncpg.connect(url)
    try:
        for sql in (DDL, MIGRATIONS, INDEXES):
            await conn.execute(sql)
        log.info("db.schema ensured (DDL + migrations + indexes).")
    finally:
        await conn.close()
