# utils_pkg/ledger.py
from __future__ import annotations
import os, sqlite3, time
from typing import Optional, Dict, Any

LEDGER_DB = os.getenv("PF_LEDGER_DB", "state/ledger.db")
SCHEMA = """
CREATE TABLE IF NOT EXISTS fills (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  ts INTEGER NOT NULL,
  side TEXT NOT NULL,         -- 'buy' | 'sell'
  mint TEXT NOT NULL,
  qty_raw INTEGER NOT NULL,
  decimals INTEGER NOT NULL,
  price_sol REAL NOT NULL,    -- per token (SOL)
  notional_sol REAL NOT NULL, -- qty_ui * price_sol
  sig TEXT
);
CREATE INDEX IF NOT EXISTS ix_fills_ts ON fills(ts);
CREATE INDEX IF NOT EXISTS ix_fills_mint ON fills(mint);
"""

def _conn():
    os.makedirs(os.path.dirname(LEDGER_DB), exist_ok=True)
    con = sqlite3.connect(LEDGER_DB)
    con.execute("PRAGMA journal_mode=WAL;")
    con.executescript(SCHEMA)
    return con

def record_fill(*, side: str, mint: str, qty_raw: int, decimals: int, price_sol: float, sig: Optional[str]) -> None:
    qty_ui = (qty_raw / (10 ** decimals)) if decimals else float(qty_raw)
    with _conn() as con:
        con.execute(
            "INSERT INTO fills(ts, side, mint, qty_raw, decimals, price_sol, notional_sol, sig) VALUES(?,?,?,?,?,?,?,?)",
            (int(time.time()), side, mint, int(qty_raw), int(decimals), float(price_sol), float(price_sol * qty_ui), sig),
        )
        con.commit()

def realized_pnl_today() -> float:
    """
    Sell notional minus buy notional for UTC day.
    (Approximation; for precise FIFO you’d maintain a position lot ledger.)
    """
    import datetime as dt
    start = int(dt.datetime.combine(dt.date.today(), dt.time.min).timestamp())
    end   = int(dt.datetime.combine(dt.date.today(), dt.time.max).timestamp())
    with _conn() as con:
        cur = con.execute("SELECT side, SUM(notional_sol) FROM fills WHERE ts BETWEEN ? AND ? GROUP BY side", (start, end))
        s = dict(cur.fetchall())
    buys  = float(s.get("buy", 0.0))
    sells = float(s.get("sell", 0.0))
    return sells - buys
