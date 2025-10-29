# utils_pkg/notion_pnl.py
from __future__ import annotations

import os
from typing import Any, Dict, Optional
from .notion_client import Notion, NotionError


def _env_true(name: str, default: str = "0") -> bool:
    return (os.getenv(name, default) or "").strip() in ("1", "true", "True", "YES", "yes")


def push_daily_pnl(snapshot: Dict[str, Any]) -> Optional[str]:
    """
    Push (or update) one row for a given date into the Notion DB.

    Required env:
      - NOTION_ENABLED=1
      - NOTION_API_KEY=ntn_xxx
      - NOTION_PNL_DB_ID=<32-hex or dashed UUID>

    Optional env overrides:
      - NOTION_TITLE_PROP (default "Notes")
      - NOTION_DATE_PROP  (default "Date")
      - NOTION_TIMEOUT_SEC (default 20)
    """
    if not _env_true("NOTION_ENABLED", "0"):
        return None

    token = os.getenv("NOTION_API_KEY")
    dbid = os.getenv("NOTION_PNL_DB_ID")
    if not token or not dbid:
        return None

    timeout = float(os.getenv("NOTION_TIMEOUT_SEC", "20"))
    notion = Notion(token, timeout=timeout)

    # Resolve date (YYYY-MM-DD)
    iso_date = (
        snapshot.get("date")
        or snapshot.get("date_utc")
        or (snapshot.get("time") or "")[:10]
    )
    if not iso_date:
        import datetime as dt
        iso_date = dt.datetime.now(dt.timezone.utc).date().isoformat()

    # Build a values dict keyed by EXACT Notion property names.
    # Your DB columns (as created in Notion):
    #   Notes (title), Date (date), Realized SOL, Unrealized SOL, Open Positions,
    #   Wallet SOL, Trades, Realized USD, Unrealized USD, Net SOL, Day Outcome (formula or select)
    realized_sol = float(snapshot.get("realized_sol", 0.0) or 0.0)
    unrealized_sol = float(snapshot.get("unrealized_sol", 0.0) or 0.0)
    net_sol = realized_sol + unrealized_sol

    price_usd = snapshot.get("price_usd")
    realized_usd = float(realized_sol * float(price_usd)) if price_usd is not None else snapshot.get("realized_usd")
    unrealized_usd = float(unrealized_sol * float(price_usd)) if price_usd is not None else snapshot.get("unrealized_usd")

    # Friendly outcome label (only used if your DB has Day Outcome as select/text; if formula, we’ll skip)
    outcome = "Win" if net_sol > 0 else ("Flat" if abs(net_sol) < 1e-9 else "Loss")

    values: Dict[str, Any] = {
        # Title + Date (names can be overridden by env in the client)
        "Notes": snapshot.get("note") or snapshot.get("notes") or "",
        "Date": iso_date,

        # Numbers
        "Realized SOL": realized_sol,
        "Unrealized SOL": unrealized_sol,
        "Open Positions": int(snapshot.get("open_positions", 0) or 0),
        "Wallet SOL": float(snapshot.get("wallet_sol")) if snapshot.get("wallet_sol") is not None else None,
        "Trades": int(snapshot.get("trades", 0) or 0),
        "Realized USD": float(realized_usd) if realized_usd is not None else None,
        "Unrealized USD": float(unrealized_usd) if unrealized_usd is not None else None,
        "Net SOL": float(net_sol),

        # Optional (set only if DB has non-formula Day Outcome)
        "Day Outcome": outcome,
    }

    # Discover schema (title/date names & types), then upsert
    schema = notion.discover_schema(dbid)
    # Ensure our title value sits under the correct title name (in case you renamed it)
    if schema.title_prop != "Notes" and "Notes" in values:
        # copy to the real title prop so Notion gets {"title": ...} under the right key
        values[schema.title_prop] = values.pop("Notes")

    page_id = notion.upsert_page(dbid, schema, iso_date, values)
    return page_id
