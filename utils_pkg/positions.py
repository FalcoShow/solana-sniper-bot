# utils_pkg/positions.py
from __future__ import annotations

import os
import sqlite3
import time
from typing import Any, Dict, List, Optional

# ──────────────────────────────────────────────────────────────────────────────
# Backend switch: Postgres (preferred) → SQLite (fallback)
# ──────────────────────────────────────────────────────────────────────────────
def _truthy(v: Optional[str]) -> bool:
    return str(v or "").strip().lower() in {"1", "true", "yes", "y", "on"}

_PG_ENABLED = _truthy(os.getenv("PG_ENABLED", "false"))

# Detect a DSN if present
_DSN = (os.getenv("PG_DSN_INTERNAL") or os.getenv("PG_DSN_EXTERNAL") or os.getenv("DATABASE_URL") or "").strip()

# Try to enable PG if both flag and DSN exist
_USE_PG = _PG_ENABLED and bool(_DSN)

_PG: Optional[object] = None
if _USE_PG:
    try:
        # Import our sync PG implementation (you added this file already)
        from utils_pkg import positions_pg as _PG  # type: ignore
        _USE_PG = True
    except Exception as _e:
        # Fall back to SQLite silently; keep working no matter what.
        _USE_PG = False

# ──────────────────────────────────────────────────────────────────────────────
# Public API (PG delegates) — identical signatures
# ──────────────────────────────────────────────────────────────────────────────
if _USE_PG:
    # PG path: delegate to positions_pg (record_buy / record_sell / …)
    def all() -> List[Dict[str, Any]]:
        return _PG.all()  # type: ignore[attr-defined]

    def get(mint: str) -> Optional[Dict[str, Any]]:
        return _PG.get(mint)  # type: ignore[attr-defined]

    def remove(mint: str) -> None:
        return _PG.remove(mint)  # type: ignore[attr-defined]

    def decrement_qty(mint: str, sold_raw: int) -> None:
        return _PG.decrement_qty(mint, sold_raw)  # type: ignore[attr-defined]

    def update_after_sell(*, mint: str, sold_raw: int, tx_sig: Optional[str], closed: bool) -> None:
        return _PG.update_after_sell(mint=mint, sold_raw=sold_raw, tx_sig=tx_sig, closed=closed)  # type: ignore[attr-defined]

    def record_sell(*, mint: str, qty_raw: int, sig: Optional[str] = None) -> None:
        return _PG.record_sell(mint=mint, qty_raw=qty_raw, sig=sig)  # type: ignore[attr-defined]

    def record_buy(
        *,
        mint: str,
        qty_raw: int,
        price_per_token_sol: float,
        decimals: int,
        token_program: str,
        owner_ata: Optional[str] = None,
        symbol: Optional[str] = None,
        pool: Optional[str] = "pump",
        sig: Optional[str] = None,
    ) -> None:
        return _PG.record_buy(  # type: ignore[attr-defined]
            mint=mint,
            qty_raw=qty_raw,
            price_per_token_sol=price_per_token_sol,
            decimals=decimals,
            token_program=token_program,
            owner_ata=owner_ata or "",
            symbol=symbol,
            pool=pool,
            sig=sig,
        )

    def upsert_raw(
        *,
        mint: str,
        symbol: Optional[str],
        pool: Optional[str],
        token_program: str,
        owner_ata: Optional[str],
        qty_raw: int,
        decimals: int,
        avg_cost_sol: float,
        opened_at: Optional[int] = None,  # PG path ignores custom opened_at
        last_sig: Optional[str] = None,
    ) -> None:
        """
        Best-effort UPSERT for PG:
        - We emulate a blind replace by removing and re-inserting with the desired values.
        - opened_at is set to 'now' (matching record_buy semantics).
        """
        if not mint:
            return
        if qty_raw < 0:
            qty_raw = 0
        # Replace row to match requested qty/avg_cost fields precisely.
        try:
            _PG.remove(mint)  # type: ignore[attr-defined]
        except Exception:
            pass
        _PG.record_buy(  # type: ignore[attr-defined]
            mint=mint,
            qty_raw=int(qty_raw),
            price_per_token_sol=float(avg_cost_sol),
            decimals=int(decimals),
            token_program=token_program,
            owner_ata=owner_ata or "",
            symbol=symbol,
            pool=pool,
            sig=last_sig,
        )

else:
    # ──────────────────────────────────────────────────────────────────────────
    # SQLite fallback (your original implementation, lightly cleaned)
    # ──────────────────────────────────────────────────────────────────────────
    DB_PATH = os.getenv("PF_POSITIONS_DB", "state/positions.db").strip() or "state/positions.db"

    SCHEMA = """
    CREATE TABLE IF NOT EXISTS positions(
      mint TEXT PRIMARY KEY,
      symbol TEXT,
      pool TEXT,
      token_program TEXT,
      owner_ata TEXT,
      qty_raw INTEGER NOT NULL,
      decimals INTEGER NOT NULL,
      avg_cost_sol REAL NOT NULL,
      opened_at INTEGER NOT NULL,
      last_sig TEXT
    );
    """

    def _ensure_dir() -> None:
        d = os.path.dirname(DB_PATH)
        if d and not os.path.exists(d):
            os.makedirs(d, exist_ok=True)

    def _connect() -> sqlite3.Connection:
        _ensure_dir()
        con = sqlite3.connect(DB_PATH, timeout=30, isolation_level=None)
        con.row_factory = sqlite3.Row
        try:
            con.execute("PRAGMA journal_mode=WAL;")
            con.execute("PRAGMA synchronous=NORMAL;")
            con.execute("PRAGMA temp_store=MEMORY;")
            con.execute("PRAGMA mmap_size=134217728;")
        except Exception:
            pass
        con.execute(SCHEMA)
        return con

    def _row_to_dict(row: sqlite3.Row) -> Dict[str, Any]:
        return dict(row) if row is not None else {}

    def _now() -> int:
        return int(time.time())

    # Public API (SQLite)
    def upsert_raw(
        *,
        mint: str,
        symbol: Optional[str],
        pool: Optional[str],
        token_program: str,
        owner_ata: Optional[str],
        qty_raw: int,
        decimals: int,
        avg_cost_sol: float,
        opened_at: Optional[int] = None,
        last_sig: Optional[str] = None,
    ) -> None:
        if not mint:
            return
        if qty_raw < 0:
            qty_raw = 0
        opened = int(opened_at or _now())
        with _connect() as con:
            con.execute(
                """
                INSERT INTO positions(mint,symbol,pool,token_program,owner_ata,qty_raw,decimals,avg_cost_sol,opened_at,last_sig)
                VALUES(?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(mint) DO UPDATE SET
                    symbol=excluded.symbol,
                    pool=excluded.pool,
                    token_program=excluded.token_program,
                    owner_ata=excluded.owner_ata,
                    qty_raw=excluded.qty_raw,
                    decimals=excluded.decimals,
                    avg_cost_sol=excluded.avg_cost_sol,
                    last_sig=excluded.last_sig
                """,
                (mint, symbol, pool, token_program, owner_ata, int(qty_raw), int(decimals), float(avg_cost_sol), opened, last_sig),
            )

    def record_buy(
        *,
        mint: str,
        qty_raw: int,
        price_per_token_sol: float,
        decimals: int,
        token_program: str,
        owner_ata: Optional[str] = None,
        symbol: Optional[str] = None,
        pool: Optional[str] = "pump",
        sig: Optional[str] = None,
    ) -> None:
        if qty_raw <= 0:
            return
        if decimals < 0:
            decimals = 0
        price_per_token_sol = float(price_per_token_sol or 0.0)

        with _connect() as con:
            cur = con.execute("SELECT * FROM positions WHERE mint=?", (mint,))
            row = cur.fetchone()

            if row is None:
                con.execute(
                    """
                    INSERT INTO positions(mint,symbol,pool,token_program,owner_ata,qty_raw,decimals,avg_cost_sol,opened_at,last_sig)
                    VALUES(?,?,?,?,?,?,?,?,?,?)
                    """,
                    (mint, symbol, pool, token_program, owner_ata, int(qty_raw), int(decimals), float(price_per_token_sol), _now(), sig),
                )
                return

            r = _row_to_dict(row)
            old_qty_raw = int(r.get("qty_raw") or 0)
            old_dec = int(r.get("decimals") or decimals)
            old_cost = float(r.get("avg_cost_sol") or 0.0)

            d = old_dec if old_dec is not None else decimals
            ui_old = old_qty_raw / (10 ** max(0, d))
            ui_add = qty_raw / (10 ** max(0, d))

            if ui_old <= 0:
                new_cost = price_per_token_sol
            else:
                new_cost = ((old_cost * ui_old) + (price_per_token_sol * ui_add)) / (ui_old + ui_add)

            con.execute(
                """
                UPDATE positions
                   SET qty_raw = ?,
                       decimals = ?,
                       avg_cost_sol = ?,
                       symbol = COALESCE(?, symbol),
                       pool = COALESCE(?, pool),
                       token_program = COALESCE(?, token_program),
                       owner_ata = COALESCE(?, owner_ata),
                       last_sig = COALESCE(?, last_sig)
                 WHERE mint = ?
                """,
                (int(old_qty_raw + qty_raw), int(d), float(new_cost), symbol, pool, token_program, owner_ata, sig, mint),
            )

    def record_sell(*, mint: str, qty_raw: int, sig: Optional[str] = None) -> None:
        if qty_raw <= 0:
            return
        with _connect() as con:
            cur = con.execute("SELECT qty_raw FROM positions WHERE mint=?", (mint,))
            row = cur.fetchone()
            if row is None:
                return
            current = int(row["qty_raw"] or 0)
            remain = max(0, current - int(qty_raw))
            if remain <= 0:
                con.execute("DELETE FROM positions WHERE mint=?", (mint,))
            else:
                con.execute("UPDATE positions SET qty_raw=?, last_sig=COALESCE(?, last_sig) WHERE mint=?", (remain, sig, mint))

    def update_after_sell(*, mint: str, sold_raw: int, tx_sig: Optional[str], closed: bool) -> None:
        if closed:
            remove(mint)
            return
        if sold_raw > 0:
            record_sell(mint=mint, qty_raw=sold_raw, sig=tx_sig)

    def decrement_qty(mint: str, sold_raw: int) -> None:
        if sold_raw <= 0:
            return
        with _connect() as con:
            cur = con.execute("SELECT qty_raw FROM positions WHERE mint=?", (mint,))
            row = cur.fetchone()
            if row is None:
                return
            remain = max(0, int(row["qty_raw"] or 0) - int(sold_raw))
            if remain <= 0:
                con.execute("DELETE FROM positions WHERE mint=?", (mint,))
            else:
                con.execute("UPDATE positions SET qty_raw=? WHERE mint=?", (remain, mint))

    def get(mint: str) -> Optional[Dict[str, Any]]:
        with _connect() as con:
            cur = con.execute("SELECT * FROM positions WHERE mint=?", (mint,))
            row = cur.fetchone()
            return _row_to_dict(row) if row else None

    def all() -> List[Dict[str, Any]]:
        with _connect() as con:
            cur = con.execute("SELECT * FROM positions ORDER BY opened_at ASC")
            return [_row_to_dict(r) for r in cur.fetchall()]

    def remove(mint: str) -> None:
        with _connect() as con:
            con.execute("DELETE FROM positions WHERE mint=?", (mint,))
