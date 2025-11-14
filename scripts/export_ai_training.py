# scripts/export_ai_training.py
#
# Export past trades from Postgres into an OpenAI fine-tuning JSONL file.
# Heuristic labels:
#   - GOOD:   strong positive pnl_usd
#   - BAD:    strong negative pnl_usd
#   - NEUTRAL: everything else
#
# This gives the AI gate signal actual "shape" instead of all-neutral.

from __future__ import annotations

import os
import json
import asyncio
import datetime as dt
from typing import Any, Dict

import asyncpg


DSN = os.environ.get("PNL_DSN") or os.environ.get("PG_DSN")
OUT_PATH = os.environ.get("AI_TRAINING_PATH", "ai_training.jsonl")

# How many rows to export
LIMIT = int(os.getenv("AI_TRAINING_LIMIT", "500"))

# Heuristic thresholds (USD PnL). Tune these as your bot grows.
GOOD_PNL_USD = float(os.getenv("AI_GOOD_PNL_USD", "5.0"))
BAD_PNL_USD = float(os.getenv("AI_BAD_PNL_USD", "-3.0"))


def _to_float(x: Any) -> float:
    try:
        return float(x)
    except Exception:
        return 0.0


def label_trade(row: Dict[str, Any]) -> Dict[str, Any]:
    """
    Decide GOOD / BAD / NEUTRAL based on pnl_usd.
    You can later add pnl_pct or other fields as your data gets richer.
    """
    pnl_usd = _to_float(row.get("pnl_usd"))

    if pnl_usd >= GOOD_PNL_USD:
        label = "GOOD"
        score = 0.9
    elif pnl_usd <= BAD_PNL_USD:
        label = "BAD"
        score = 0.9
    else:
        label = "NEUTRAL"
        score = 0.5

    return {"label": label, "score": score, "pnl_usd": pnl_usd}


def build_example(row: Dict[str, Any]) -> Dict[str, Any]:
    """
    Build a single fine-tuning example in the chat format OpenAI expects.
    """
    ts = row.get("ts")
    if isinstance(ts, dt.datetime):
        ts_str = ts.strftime("%Y-%m-%d %H:%M:%S %Z") or ts.isoformat()
    else:
        ts_str = str(ts)

    side = row.get("side")
    symbol = row.get("symbol")
    mint = row.get("mint") or row.get("token")
    reason = row.get("reason")

    entry = row.get("entry")
    current = row.get("current")
    profit = row.get("profit")

    pnl_usd = row.get("pnl_usd")
    pnl_sol = row.get("pnl_sol")

    lab = label_trade(row)

    user_content = (
        "Here is a past trade. Analyze it and decide if this type of setup is GOOD, BAD, or NEUTRAL:\n"
        f"Timestamp: {ts_str} | Side: {side} | Symbol: {symbol} | Mint: {mint}\n"
        f"Entry metric: {entry} | Exit metric (current): {current} | Realized profit metric: {profit}\n"
        f"PNL USD: {pnl_usd} | PNL SOL: {pnl_sol} | Entry reason: {reason}\n\n"
        'Respond with JSON like: {"label": "GOOD|BAD|NEUTRAL", "score": number between 0 and 1}.'
    )

    system_msg = (
        "You are an AI risk and PnL assistant for a Solana meme coin sniper bot. "
        "Given the trade context, you decide whether similar future trades are GOOD candidates, BAD candidates, "
        "or NEUTRAL. You respond with a JSON object containing 'label' and 'score'."
    )

    assistant_msg = json.dumps(
        {
            "label": lab["label"],
            "score": lab["score"],
        }
    )

    return {
        "messages": [
            {"role": "system", "content": system_msg},
            {"role": "user", "content": user_content},
            {"role": "assistant", "content": assistant_msg},
        ]
    }


async def main() -> None:
    if not DSN:
        raise SystemExit("No PNL_DSN/PG_DSN set in env")

    conn = await asyncpg.connect(DSN)
    rows = await conn.fetch(
        f"""
        SELECT *
        FROM trades
        WHERE pnl_usd IS NOT NULL
        ORDER BY ts DESC
        LIMIT {LIMIT};
        """
    )
    await conn.close()

    print(f"Fetched {len(rows)} rows from trades")

    out = []
    with open(OUT_PATH, "w", encoding="utf-8") as f:
        for r in rows:
            d = dict(r)
            ex = build_example(d)
            out.append(ex)
            f.write(json.dumps(ex, ensure_ascii=False) + "\n")

    print(f"Wrote {len(out)} examples to {OUT_PATH}")
    os.system(f"ls -lh {OUT_PATH}")
    os.system(f"head -n 3 {OUT_PATH}")


if __name__ == "__main__":
    asyncio.run(main())
