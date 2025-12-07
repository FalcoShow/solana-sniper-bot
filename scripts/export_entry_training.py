from __future__ import annotations

"""
scripts/export_entry_training.py

Export entry training dataset (features + heuristic labels) to JSONL.

This script is intentionally simple and robust:

  • Uses the same DB DSN family as the main sniper stack
    (AI_DB_DSN / DATABASE_URL / SNIPER_DB_DSN / PNL_DB_DSN)
  • Normalizes async-style DSNs like:
        postgresql+asyncpg://user:pass@host/db
    into psycopg2-friendly DSNs:
        postgresql://user:pass@host/db
  • Pulls joined rows from:
        ai_entry_candidates  (features_json, src, mint, symbol, created_at)
        ai_entry_outcomes    (pnl / runup / drawdown / holding_minutes)
  • Emits JSONL records of the form:
        {"input": {...}, "output": {...}}

Usage examples (on Render shell):

    # Last 1 day, max 100 examples, print to stdout:
    python -m scripts.export_entry_training --days 1 --limit 100 --out -

    # Write to file:
    python -m scripts.export_entry_training --days 7 --limit 2000 --out /tmp/entry_training.jsonl

"""

import argparse
import json
import os
import sys
from datetime import datetime, timedelta, timezone
from typing import Any, Dict

import psycopg2
import psycopg2.extras

# ───────────────────────────────────────────────────────────
# Config: tweak these if your table/column names differ
# ───────────────────────────────────────────────────────────

CANDIDATES_TABLE = "ai_entry_candidates"
OUTCOMES_TABLE = "ai_entry_outcomes"

# Column assumptions:
# - candidates: id, created_at, src, mint, symbol, features_json (JSON or JSONB)
# - outcomes:  candidate_id, realized_pnl_pct, max_runup_pct, max_drawdown_pct,
#              holding_minutes
#
# If your schema differs, adjust the SELECT in build_query() below.


# ───────────────────────────────────────────────────────────
# DSN helpers
# ───────────────────────────────────────────────────────────


def _get_raw_dsn() -> str:
    """
    Resolve the DB DSN from env, in the same spirit as ai_db/core_utils.

    Priority:
      1) AI_DB_DSN
      2) DATABASE_URL
      3) SNIPER_DB_DSN
      4) PNL_DB_DSN
    """
    env_keys = ["AI_DB_DSN", "DATABASE_URL", "SNIPER_DB_DSN", "PNL_DB_DSN"]
    for key in env_keys:
        value = os.getenv(key)
        if value:
            return value

    raise SystemExit(
        "[export] No DB DSN found in AI_DB_DSN / DATABASE_URL / "
        "SNIPER_DB_DSN / PNL_DB_DSN"
    )


def _normalize_dsn_for_psycopg2(raw: str) -> str:
    """
    psycopg2 does NOT understand the SQLAlchemy-style async postfix
    (e.g. 'postgresql+asyncpg://').

    Normalize common variants to plain 'postgresql://' / 'postgres://'.
    """
    normalized = raw.replace("postgresql+asyncpg://", "postgresql://")
    normalized = normalized.replace("postgres+asyncpg://", "postgres://")
    return normalized


# ───────────────────────────────────────────────────────────
# Labeling logic
# ───────────────────────────────────────────────────────────


def make_label(row: Dict[str, Any]) -> Dict[str, Any]:
    """
    Heuristic label for supervised training.

    Simple rule-based scheme (can be tuned later):

      • 'buy' if:
          - strong upside (max_runup_pct >= 20%) and drawdown not horrible
            (max_drawdown_pct > -25%), OR
          - realized_pnl_pct >= 10% with max_drawdown_pct > -20%

      • 'pass' otherwise, and any clear rug / nuclear loss forces 'pass':
          - realized_pnl_pct <= -15% OR max_drawdown_pct <= -40%
    """
    pnl = float(row.get("realized_pnl_pct") or 0.0)
    runup = float(row.get("max_runup_pct") or 0.0)
    drawdown = float(row.get("max_drawdown_pct") or 0.0)
    # holding minutes available if you want to use it later
    _hold_min = float(row.get("holding_minutes") or 0.0)

    good = False

    # Strong winner
    if runup >= 20.0 and drawdown > -25.0:
        good = True

    # Solid realized PnL
    if pnl >= 10.0 and drawdown > -20.0:
        good = True

    # Nuclear loss / rug → force bad
    if pnl <= -15.0 or drawdown <= -40.0:
        good = False

    verdict = "buy" if good else "pass"
    score = 0.8 if good else 0.2

    return {
        "score": score,
        "verdict": verdict,
        # Include raw targets as part of the label for multi-task training
        "realized_pnl_pct": pnl,
        "max_runup_pct": runup,
        "max_drawdown_pct": drawdown,
        "holding_minutes": _hold_min,
    }


# ───────────────────────────────────────────────────────────
# Query builder
# ───────────────────────────────────────────────────────────


def build_query() -> str:
    """
    Build the SQL query used to join candidates + outcomes.

    Adjust here if you ever rename tables/columns.
    """
    return f"""
        SELECT
            c.id AS candidate_id,
            c.created_at,
            c.src,
            c.mint,
            c.symbol,
            c.features_json,
            o.realized_pnl_pct,
            o.max_runup_pct,
            o.max_drawdown_pct,
            o.holding_minutes
        FROM {CANDIDATES_TABLE} c
        JOIN {OUTCOMES_TABLE} o
          ON o.candidate_id = c.id
        WHERE c.created_at >= %s
        ORDER BY c.created_at DESC
        LIMIT %s
    """


# ───────────────────────────────────────────────────────────
# Row → JSONL record
# ───────────────────────────────────────────────────────────


def _parse_features(features_raw: Any) -> Dict[str, Any]:
    """
    Normalize features_json (which might be JSON, JSONB, or text).
    """
    if features_raw is None:
        return {}

    if isinstance(features_raw, dict):
        return features_raw

    if isinstance(features_raw, str):
        try:
            return json.loads(features_raw)
        except Exception:
            # If it's corrupt, just drop features for that row
            return {}

    # Fallback: unknown type -> empty
    return {}


def row_to_record(row: Dict[str, Any]) -> Dict[str, Any]:
    """
    Convert a DB row into the training record schema.
    """
    features = _parse_features(row.get("features_json"))

    created_at = row.get("created_at")
    if isinstance(created_at, datetime):
        created_at_iso = created_at.isoformat()
    else:
        created_at_iso = None

    label = make_label(row)

    return {
        "input": {
            "features": features,
            "src": row.get("src"),
            "mint": row.get("mint"),
            "symbol": row.get("symbol"),
            "created_at": created_at_iso,
        },
        "output": label,
    }


# ───────────────────────────────────────────────────────────
# Main
# ───────────────────────────────────────────────────────────


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Export entry training dataset to JSONL"
    )
    parser.add_argument(
        "--days",
        type=int,
        default=14,
        help="Lookback window in days (default: 14)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=2000,
        help="Max examples (default: 2000)",
    )
    parser.add_argument(
        "--out",
        type=str,
        default="data/entry_training.jsonl",
        help="Output path, or '-' for stdout (default: data/entry_training.jsonl)",
    )
    args = parser.parse_args()

    raw_dsn = _get_raw_dsn()
    dsn = _normalize_dsn_for_psycopg2(raw_dsn)

    # Logging uses raw DSN for easier debugging
    print(f"[export] Connecting to DB: {raw_dsn}")

    # Create directory if we are writing to a file path
    out_path = args.out
    use_stdout = out_path == "-"

    if not use_stdout:
        out_dir = os.path.dirname(out_path)
        if out_dir:
            os.makedirs(out_dir, exist_ok=True)

    since = datetime.now(timezone.utc) - timedelta(days=args.days)
    query = build_query()

    conn = psycopg2.connect(dsn)
    conn.autocommit = True

    n = 0
    cursor_factory = psycopg2.extras.RealDictCursor

    # Decide output stream
    out_fh = sys.stdout if use_stdout else open(out_path, "w", encoding="utf-8")

    try:
        with conn, conn.cursor(cursor_factory=cursor_factory) as cur:
            cur.execute(query, (since, args.limit))
            for row in cur:
                record = row_to_record(row)
                out_fh.write(json.dumps(record, separators=(",", ":")) + "\n")
                n += 1
    finally:
        if not use_stdout:
            out_fh.close()
        conn.close()

    target_desc = "stdout" if use_stdout else out_path
    print(f"[export] Wrote {n} examples to {target_desc}", file=sys.stderr)


if __name__ == "__main__":
    main()
