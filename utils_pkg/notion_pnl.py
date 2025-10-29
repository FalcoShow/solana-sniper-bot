# utils_pkg/notion_pnl.py
from __future__ import annotations
import os
from typing import Dict, Any, Optional
from .notion_client import Notion

def push_daily_pnl(snapshot: Dict[str, Any]) -> Optional[str]:
    """
    snapshot is the same dict you already log/send (see DAILY_PNL_SNAPSHOT).
    Expected keys (use what you have): realized_sol, unrealized_sol, open_positions,
    wallet_sol, trades (optional), price_usd (optional)
    """
    if os.getenv("NOTION_ENABLED","0") != "1":
        return None
    token = os.getenv("NOTION_API_KEY")
    dbid  = os.getenv("NOTION_PNL_DB_ID")
    if not token or not dbid:
        return None

    # Convert your snapshot → Notion props
    iso_date = snapshot.get("date") or snapshot.get("date_utc") or snapshot.get("time","")[:10]
    if not iso_date:
        # fallback to today (UTC)
        import datetime as dt
        iso_date = dt.datetime.utcnow().date().isoformat()

    realized_sol   = float(snapshot.get("realized_sol", 0.0))
    unrealized_sol = float(snapshot.get("unrealized_sol", 0.0))
    open_pos       = int(snapshot.get("open_positions", 0))
    wallet_sol     = snapshot.get("wallet_sol")
    trades         = int(snapshot.get("trades", 0))
    price_usd      = snapshot.get("price_usd")  # optional

    realized_usd   = float(realized_sol * float(price_usd)) if price_usd else None
    unrealized_usd = float(unrealized_sol * float(price_usd)) if price_usd else None
    net_sol        = realized_sol + unrealized_sol
    outcome        = "Win" if net_sol > 0 else ("Flat" if abs(net_sol) < 1e-9 else "Loss")

    props = {
        "Realized SOL":   realized_sol,
        "Unrealized SOL": unrealized_sol,
        "Open Positions": open_pos,
        "Wallet SOL":     float(wallet_sol) if wallet_sol is not None else None,
        "Trades":         trades,
        "Realized USD":   realized_usd,
        "Unrealized USD": unrealized_usd,
        "Net SOL":        net_sol,
        "Day Outcome":    outcome,
        "Notes":          snapshot.get("note") or "",
    }

    notion = Notion(token, timeout=float(os.getenv("NOTION_TIMEOUT_SEC", "20")))
    return notion.upsert_daily_pnl(dbid, iso_date, props)
