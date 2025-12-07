# scripts/export_entry_training.py
from __future__ import annotations

import argparse
import json
import os
from datetime import datetime, timedelta

import psycopg2
import psycopg2.extras


# ───────────────────────────────────────────────────────────
# Config: tweak these if your table/column names differ
# ───────────────────────────────────────────────────────────

CANDIDATES_TABLE = "ai_entry_candidates"
OUTCOMES_TABLE = "ai_entry_outcomes"

# Column assumptions:
# - candidates: id, created_at, src, mint, symbol, features_json (JSON)
# - outcomes:  candidate_id, realized_pnl_pct, max_runup_pct, max_drawdown_pct,
#              holding_minutes
#
# If your columns differ, just fix the SELECT below.


def get_dsn() -> str:
    env_keys = ["SNIPER_DB_DSN", "DATABASE_URL", "PNL_DB_DSN"]
    for k in env_keys:
        v = os.getenv(k)
        if v:
            return v
    raise SystemExit("No DB DSN found in SNIPER_DB_DSN / DATABASE_URL / PNL_DB_DSN")


def make_label(row: dict) -> dict:
    """
    Heuristic label:
      - 'buy' if decent upside and not nuked.
      - 'pass' otherwise.
    """
    pnl = row.get("realized_pnl_pct") or 0.0
    runup = row.get("max_runup_pct") or 0.0
    drawdown = row.get("max_drawdown_pct") or 0.0
    hold_min = row.get("holding_minutes") or 0.0

    # Example heuristic: you can tune later
    good = False

    # Strong winner (hit 20%+ within reasonable time)
    if runup >= 20.0 and drawdown > -25.0:
        good = True

    # Solid realized pnl
    if pnl >= 10.0 and drawdown > -20.0:
        good = True

    # Clear loser (hard rug / deep DD)
    if pnl <= -15.0 or drawdown <= -40.0:
        good = False

    verdict = "buy" if good else "pass"
    score = 0.8 if good else 0.2

    return {
        "score": score,
        "verdict": verdict,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Export entry training dataset to JSONL")
    parser.add_argument("--days", type=int, default=14, help="Lookback window in days")
    parser.add_argument("--limit", type=int, default=2000, help="Max examples")
    parser.add_argument("--out", type=str, default="data/entry_training.jsonl")
    args = parser.parse_args()

    dsn = get_dsn()
    os.makedirs(os.path.dirname(args.out), exist_ok=True)

    since = datetime.utcnow() - timedelta(days=args.days)

    query = f"""
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

    print(f"[export] Connecting to DB: {dsn}")
    conn = psycopg2.connect(dsn)
    conn.autocommit = True

    n = 0
    with conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur, open(
        args.out, "w", encoding="utf-8"
    ) as f:
        cur.execute(query, (since, args.limit))
        for row in cur:
            features = row.get("features_json") or {}
            if isinstance(features, str):
                try:
                    features = json.loads(features)
                except Exception:
                    features = {}

            label = make_label(row)

            record = {
                "input": {
                    "features": features,
                    "src": row.get("src"),
                    "mint": row.get("mint"),
                    "symbol": row.get("symbol"),
                    "created_at": row.get("created_at").isoformat()
                    if row.get("created_at")
                    else None,
                },
                "output": label,
            }

            f.write(json.dumps(record, separators=(",", ":")) + "\n")
            n += 1

    print(f"[export] Wrote {n} examples to {args.out}")


if __name__ == "__main__":
    main()
