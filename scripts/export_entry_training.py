from __future__ import annotations

"""
scripts/export_entry_training.py

Export entry training dataset (JSONL) for AI entry model fine-tuning.

Design goals
------------
- Works with the existing sniper DB schema without needing code changes
  every time we tweak columns.
- Auto-detects the features column on `entry_candidates` rather than
  assuming a specific name like `features_json`.
- Uses a simple but tweakable heuristic to create labels (buy / pass)
  based on realized PnL / run-up / drawdown.
"""

import argparse
import json
import os
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable

import psycopg2
import psycopg2.extras

# -------------------------------------------------------------------
# Table configuration
# -------------------------------------------------------------------
# These defaults match what you inspected:
#   public.entry_candidates
#   public.entry_outcomes
CANDIDATES_TABLE = os.getenv("ENTRY_CANDIDATES_TABLE", "public.entry_candidates")
OUTCOMES_TABLE = os.getenv("ENTRY_OUTCOMES_TABLE", "public.entry_outcomes")


# -------------------------------------------------------------------
# DSN helpers
# -------------------------------------------------------------------


def _get_raw_dsn() -> str:
    """
    Return the raw DB DSN from env.

    Priority:
        1) SNIPER_DB_DSN
        2) DATABASE_URL
        3) PNL_DB_DSN
    """
    env_keys = ["SNIPER_DB_DSN", "DATABASE_URL", "PNL_DB_DSN"]
    for k in env_keys:
        v = os.getenv(k)
        if v:
            return v
    raise SystemExit("No DB DSN found in SNIPER_DB_DSN / DATABASE_URL / PNL_DB_DSN")


def _normalize_dsn_for_psycopg2(raw: str) -> str:
    """
    Convert an async-style DSN into something psycopg2 can understand.
    """
    if raw.startswith("postgresql+asyncpg://"):
        return "postgresql://" + raw[len("postgresql+asyncpg://") :]
    return raw


def get_dsn() -> str:
    raw = _get_raw_dsn()
    dsn = _normalize_dsn_for_psycopg2(raw)
    return dsn


# -------------------------------------------------------------------
# Feature extraction & labeling
# -------------------------------------------------------------------


_FEATURE_CANDIDATE_KEYS = [
    "features_json",
    "features",
    "features_v1",
    "features_v2",
    "raw_features",
    "feats_json",
    "feats",
]


def _extract_features(row: Dict[str, Any]) -> Dict[str, Any]:
    """
    Try to locate and parse the features payload from a candidate row.

    Strategy:
      1) Prefer known key names in a fixed priority list.
      2) If none present, fall back to any column whose name contains
         'feature' and looks like JSON / dict.
      3) Ensure we always return a dict (possibly empty).
    """
    if not isinstance(row, dict):
        # extra safety; should never happen with RealDictCursor
        return {}

    # First: explicit candidate keys in priority order
    val = None
    for key in _FEATURE_CANDIDATE_KEYS:
        if key in row and row[key] is not None:
            val = row[key]
            break

    # Fallback: any column containing "feature" in its name
    if val is None:
        for k, v in row.items():
            if "feature" in k and v is not None:
                val = v
                break

    if val is None:
        return {}

    # If it's already a dict-like, keep as-is
    if isinstance(val, dict):
        return val

    # If it's a string, try JSON decode
    if isinstance(val, str):
        try:
            parsed = json.loads(val)
            if isinstance(parsed, dict):
                return parsed
            return {"_raw_features": parsed}
        except Exception:
            return {}

    # Anything else (e.g. list, tuple, etc.)
    try:
        return dict(val)  # type: ignore[arg-type]
    except Exception:
        return {"_raw_features": val}


def make_label(row: Dict[str, Any]) -> Dict[str, Any]:
    """
    Heuristic label for entry training.

    - 'buy' if decent upside and not nuked.
    - 'pass' otherwise.

    Uses realized_pnl_pct / max_runup_pct / max_drawdown_pct / holding_minutes
    when available; missing fields default to 0.
    """
    pnl = float(row.get("realized_pnl_pct") or 0.0)
    runup = float(row.get("max_runup_pct") or 0.0)
    drawdown = float(row.get("max_drawdown_pct") or 0.0)
    hold_min = float(row.get("holding_minutes") or 0.0)

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
        "realized_pnl_pct": pnl,
        "max_runup_pct": runup,
        "max_drawdown_pct": drawdown,
        "holding_minutes": hold_min,
    }


# -------------------------------------------------------------------
# Main export logic
# -------------------------------------------------------------------


def _iter_rows(
    conn: psycopg2.extensions.connection, since: datetime, limit: int
) -> Iterable[Dict[str, Any]]:
    """
    Generator over joined candidate/outcome rows.

    NOTE: we select c.* and o.* instead of naming specific columns so
    that missing columns (like holding_minutes) don't break the query.
    Python will simply see row.get("holding_minutes") as None.
    """
    query = f"""
        SELECT
            c.*,
            o.*
        FROM {CANDIDATES_TABLE} c
        JOIN {OUTCOMES_TABLE} o
          ON o.candidate_id = c.id
        WHERE c.created_at >= %s
        ORDER BY c.created_at DESC
        LIMIT %s
    """

    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(query, (since, limit))
        for row in cur:
            # Each row is a dict from RealDictCursor
            yield row


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Export entry training dataset to JSONL"
    )
    parser.add_argument(
        "--days", type=int, default=14, help="Lookback window in days"
    )
    parser.add_argument(
        "--limit", type=int, default=2000, help="Max examples"
    )
    parser.add_argument(
        "--out", type=str, default="data/entry_training.jsonl", help="Output JSONL path"
    )
    args = parser.parse_args()

    dsn = get_dsn()
    print(f"[export] Connecting to DB: {dsn}")

    # Ensure output directory exists
    out_dir = os.path.dirname(args.out)
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)

    # Use timezone-aware UTC
    since = datetime.now(timezone.utc) - timedelta(days=args.days)

    conn = psycopg2.connect(dsn)
    conn.autocommit = True

    n = 0
    skipped_no_features = 0

    with conn, open(args.out, "w", encoding="utf-8") as f:
        for row in _iter_rows(conn, since, args.limit):
            features = _extract_features(row)
            if not features:
                skipped_no_features += 1
                continue

            created_at = row.get("created_at")
            if isinstance(created_at, datetime):
                created_str = created_at.isoformat()
            else:
                created_str = None

            label = make_label(row)

            record = {
                "input": {
                    "features": features,
                    "src": row.get("src"),
                    "mint": row.get("mint"),
                    "symbol": row.get("symbol"),
                    "created_at": created_str,
                },
                "output": label,
            }

            f.write(json.dumps(record, separators=(",", ":")) + "\n")
            n += 1

    print(f"[export] Wrote {n} examples to {args.out}")
    if skipped_no_features:
        print(f"[export] Skipped {skipped_no_features} rows with no features column")


if __name__ == "__main__":
    main()
