# scripts/export_ai_training.py
from __future__ import annotations

import asyncio
import json
import os
from typing import Any, Dict

import asyncpg


PG_DSN = os.getenv("PNL_DSN") or os.getenv("PG_DSN")
OUT_PATH = "ai_training.jsonl"

"""
Assumptions (adjust to your schema!):

We assume a table `trades` with columns like:
- mint TEXT
- symbol TEXT
- liq_usd NUMERIC
- age_sec INT
- buys_5m INT
- buys_15m INT
- vol_5m_usd NUMERIC
- vol_15m_usd NUMERIC
- chg_5m_pct NUMERIC
- chg_15m_pct NUMERIC
- pnl_pct NUMERIC          -- realized PnL %
- hold_sec INT             -- how long you held
- outcome TEXT             -- 'win' | 'loss' | 'breakeven' (or derive from pnl_pct)

You will need to tweak the query + fields to your actual schema.
"""


def make_label(row: asyncpg.Record) -> str:
    """Simple heuristic → you can tune this to be more/less aggressive."""
    pnl = float(row["pnl_pct"])
    hold_sec = int(row["hold_sec"] or 0)

    if pnl >= 40 and hold_sec <= 3600:
        return "BUY_HIGH"
    if pnl >= 15:
        return "BUY"
    if pnl <= -20:
        return "AVOID"
    return "SKIP"


def make_score(row: asyncpg.Record) -> float:
    """Turn pnl into a 0–1-ish score."""
    pnl = float(row["pnl_pct"])
    # compress pnl into 0–1
    if pnl <= -30:
        return 0.0
    if pnl >= 100:
        return 1.0
    # linear between -30 and 100
    return max(0.0, min(1.0, (pnl + 30.0) / 130.0))


def build_features(row: asyncpg.Record) -> Dict[str, Any]:
    """What the model sees as 'user' content (we send as JSON text)."""
    return {
        "mint": row["mint"],
        "symbol": row.get("symbol"),
        "liq_usd": float(row["liq_usd"] or 0),
        "age_sec": int(row["age_sec"] or 0),
        "buys_5m": int(row["buys_5m"] or 0),
        "buys_15m": int(row["buys_15m"] or 0),
        "vol_5m_usd": float(row["vol_5m_usd"] or 0),
        "vol_15m_usd": float(row["vol_15m_usd"] or 0),
        "chg_5m_pct": float(row["chg_5m_pct"] or 0),
        "chg_15m_pct": float(row["chg_15m_pct"] or 0),
        "pnl_pct": float(row["pnl_pct"] or 0),
        "hold_sec": int(row["hold_sec"] or 0),
    }


SYSTEM_PROMPT = (
    "You are an expert Solana meme-coin risk and quality model used in an automated "
    "sniper bot. You receive a JSON object describing a single token candidate with "
    "liquidity, age, recent volume, buy/sell pressure, and other stats.\n\n"
    "Your job is to decide if this token should have been bought, and how strong "
    "that decision is, based on historical profit and risk.\n\n"
    "You MUST reply in the following format:\n"
    "label: <BUY_HIGH|BUY|SKIP|AVOID>\n"
    "score: <float between 0 and 1>\n"
    "rationale: <1-2 sentence explanation>\n"
)


async def main() -> None:
    if not PG_DSN:
        raise SystemExit("PG_DSN / PNL_DSN not set")

    conn = await asyncpg.connect(PG_DSN)

    # TODO: adjust this query to your schema
    rows = await conn.fetch(
        """
        SELECT
          mint,
          symbol,
          liq_usd,
          age_sec,
          buys_5m,
          buys_15m,
          vol_5m_usd,
          vol_15m_usd,
          chg_5m_pct,
          chg_15m_pct,
          pnl_pct,
          hold_sec
        FROM sniper_trades
        WHERE pnl_pct IS NOT NULL
          AND created_at >= now() - interval '60 days'
        LIMIT 2000;
        """
    )

    await conn.close()

    n = 0
    with open(OUT_PATH, "w", encoding="utf-8") as f:
        for row in rows:
            features = build_features(row)
            label = make_label(row)
            score = make_score(row)

            assistant_reply = (
                f"label: {label}\n"
                f"score: {score:.3f}\n"
                "rationale: This label and score are based on historical pnl_pct, "
                "liquidity, age, and recent volume trends.\n"
            )

            record = {
                "messages": [
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {
                        "role": "user",
                        "content": json.dumps(features, separators=(",", ":")),
                    },
                    {"role": "assistant", "content": assistant_reply},
                ]
            }
            f.write(json.dumps(record) + "\n")
            n += 1

    print(f"Wrote {n} examples to {OUT_PATH}")


if __name__ == "__main__":
    asyncio.run(main())
