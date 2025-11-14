# scripts/export_ai_training.py
"""
Export past trades into OpenAI fine-tuning format (JSONL).

We use the existing `public.trades` table and derive:
- Features about the trade (side, size, prices, etc.)
- A label (GOOD / BAD) based on realized profit

Output: ai_training.jsonl (one {"messages":[...]} per line)
"""

from __future__ import annotations

import asyncio
import json
import math
import os
from datetime import datetime, timezone
from typing import Any, Dict, List

import asyncpg


def env_dsn() -> str:
    dsn = os.environ.get("PNL_DSN") or os.environ.get("PG_DSN")
    if not dsn:
        raise SystemExit("No PNL_DSN or PG_DSN in env")
    return dsn


# ──────────────────────────────────────────────────────────────────────────────
# Helpers to derive label / scoring from trade row
# ──────────────────────────────────────────────────────────────────────────────
def make_label(row: Dict[str, Any]) -> str:
    """
    Simple v1 label:
      - GOOD if profit or pnl_usd is clearly > 0
      - BAD  if clearly < 0
      - NEUTRAL otherwise (small/no change)
    """
    profit = row.get("profit")
    pnl_usd = row.get("pnl_usd")

    val = None
    if isinstance(profit, (int, float)):
        val = profit
    elif isinstance(pnl_usd, (int, float)):
        val = pnl_usd

    if val is None:
        return "NEUTRAL"

    if val > 0.0:
        return "GOOD"
    if val < 0.0:
        return "BAD"
    return "NEUTRAL"


def make_score(row: Dict[str, Any]) -> float:
    """
    Map profit → a soft score between 0 and 1 for training:
      - 0.5 baseline
      - > 0.5 if profitable
      - < 0.5 if loss
    """
    profit = row.get("profit")
    pnl_usd = row.get("pnl_usd")
    val = None
    if isinstance(profit, (int, float)):
        val = profit
    elif isinstance(pnl_usd, (int, float)):
        val = pnl_usd
    if val is None:
        return 0.5

    # Squash via tanh to avoid huge outliers dominating
    scaled = math.tanh(val / 20.0)  # 20 USD as soft scale
    return 0.5 + 0.5 * scaled


def build_feature_summary(row: Dict[str, Any]) -> str:
    """
    Turn the row into a short natural-language description the model can learn from.
    """
    ts = row.get("ts")
    if isinstance(ts, datetime):
        ts_str = ts.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    else:
        ts_str = str(ts)

    side = row.get("side") or "unknown"
    symbol = row.get("symbol") or "UNKNOWN"
    token = row.get("token") or row.get("mint") or "unknown"
    sol_amount = row.get("sol_amount")
    price_sol = row.get("price_sol")
    price_usd = row.get("price_usd")
    entry = row.get("entry")
    current = row.get("current")
    profit = row.get("profit")
    pnl_usd = row.get("pnl_usd")
    reason = row.get("reason")

    parts: List[str] = []
    parts.append(f"Timestamp: {ts_str}")
    parts.append(f"Side: {side}")
    parts.append(f"Symbol: {symbol}")
    parts.append(f"Mint: {token}")

    if sol_amount is not None:
        parts.append(f"Size (SOL): {sol_amount:.6f}")
    if price_sol is not None:
        parts.append(f"Entry price in SOL: {price_sol:.9f}")
    if price_usd is not None:
        parts.append(f"Entry price in USD: {price_usd:.6f}")
    if entry is not None:
        parts.append(f"Entry metric: {entry:.6f}")
    if current is not None:
        parts.append(f"Exit metric (current): {current:.6f}")
    if profit is not None:
        parts.append(f"Realized profit metric: {profit:.6f}")
    if pnl_usd is not None:
        parts.append(f"Realized PnL USD: {pnl_usd:.6f}")
    if reason:
        parts.append(f"Entry reason: {reason}")

    return " | ".join(parts)


def build_training_example(row: Dict[str, Any]) -> Dict[str, Any]:
    """
    Build one OpenAI chat-like training example from a trade row.
    """
    label = make_label(row)
    score = make_score(row)
    summary = build_feature_summary(row)

    # You can tune the "instructions" text later — this is v1.
    system_msg = {
        "role": "system",
        "content": (
            "You are an AI risk and PnL assistant for a Solana meme coin sniper bot. "
            "Given the trade context, you decide whether similar future trades are GOOD candidates, "
            "BAD candidates, or NEUTRAL. You respond with a JSON object containing 'label' and 'score'."
        ),
    }

    user_msg = {
        "role": "user",
        "content": (
            "Here is a past trade. Analyze it and decide if this type of setup is GOOD, BAD, or NEUTRAL:\n"
            f"{summary}\n\n"
            "Respond with JSON like: {\"label\": \"GOOD|BAD|NEUTRAL\", \"score\": number between 0 and 1}."
        ),
    }

    assistant_msg = {
        "role": "assistant",
        "content": json.dumps({"label": label, "score": round(score, 4)}),
    }

    return {"messages": [system_msg, user_msg, assistant_msg]}


# ──────────────────────────────────────────────────────────────────────────────
# Main async export
# ──────────────────────────────────────────────────────────────────────────────
async def main() -> None:
    dsn = env_dsn()
    conn = await asyncpg.connect(dsn)

    # Pull trades with a real profit signal in last 90 days
    rows = await conn.fetch(
        """
        SELECT
          id,
          ts,
          token,
          symbol,
          side,
          sol_amount,
          price_sol,
          price_usd,
          pnl_sol,
          pnl_usd,
          reason,
          signature,
          wallet,
          amount,
          entry,
          current,
          profit,
          mint
        FROM trades
        WHERE profit IS NOT NULL
          AND ts >= now() - interval '90 days'
        ORDER BY ts DESC
        LIMIT 2000;
        """
    )
    await conn.close()

    print(f"Fetched {len(rows)} rows from trades")

    out_path = "ai_training.jsonl"
    n_written = 0
    with open(out_path, "w", encoding="utf-8") as f:
        for r in rows:
            row_dict = dict(r)
            example = build_training_example(row_dict)
            f.write(json.dumps(example, separators=(",", ":")) + "\n")
            n_written += 1

    print(f"Wrote {n_written} examples to {out_path}")


if __name__ == "__main__":
    asyncio.run(main())
