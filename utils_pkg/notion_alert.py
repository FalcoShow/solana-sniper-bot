# utils_pkg/notion_alert.py
from __future__ import annotations

import os
import ssl
from typing import Optional, Dict
from datetime import datetime, timezone

import aiohttp
import certifi

NOTION_PAGES = "https://api.notion.com/v1/pages"
NOTION_VERSION = "2022-06-28"  # stable, widely supported


# ---------- helpers ----------
def _env(name: str, default: str = "") -> str:
    v = os.getenv(name)
    return v if v is not None else default


def _num(x) -> float:
    """Coerce to float; clamp tiny noise to 0.0."""
    try:
        v = float(x)
        return 0.0 if abs(v) < 1e-15 else v
    except Exception:
        return 0.0


def _prop_name(env_key: str, default_if_empty: str = "") -> str:
    """
    Read a property name from env.
    If blank, we will SKIP sending that property to Notion.
    """
    return _env(env_key, default_if_empty).strip()


# ---------- main API ----------
async def log_trade(
    *,
    token: str,
    symbol: str,
    balance: float,
    entry: float,
    current: float,
    profit: float,
    reason: str,
    database_id: str,
    token_auth: str,
    session: Optional[aiohttp.ClientSession] = None,
) -> None:
    """
    Create a row in the provided Notion database.

    IMPORTANT: we only send properties that you explicitly name in your .env.
    This avoids 400s like “<Prop> is not a property that exists”.

    Required:
      - token_auth   -> your Notion integration secret
      - database_id  -> your Notion database id
      - NOTION_TITLE_PROP must be set to your database's Title property name
                         (e.g., 'Token' or 'Name')

    Optional property name envs (leave unset or empty to skip):
      NOTION_PROP_TOKEN      -> rich_text (e.g., 'Mint')
      NOTION_PROP_SYMBOL     -> rich_text (e.g., 'Symbol')
      NOTION_PROP_BALANCE    -> number
      NOTION_PROP_ENTRY      -> number (e.g., 'Entry Price')
      NOTION_PROP_CURRENT    -> number (e.g., 'Current Price')
      NOTION_PROP_PROFIT     -> number
      NOTION_PROP_REASON     -> rich_text
      NOTION_PROP_TIME       -> date   (e.g., 'Timestamp')

    Example minimal env for your schema (matches what you showed earlier):
      NOTION_TITLE_PROP=Token
      NOTION_PROP_TOKEN=Mint
      NOTION_PROP_ENTRY=Entry Price
      NOTION_PROP_CURRENT=Current Price
      NOTION_PROP_TIME=Timestamp
      # (leave others blank or omit to skip them)

    We set the page title to `symbol` if present, else `token`.
    """
    # headers
    headers = {
        "Authorization": f"Bearer {token_auth}",
        "Notion-Version": NOTION_VERSION,
        "Content-Type": "application/json",
    }

    # property names from env
    title_prop = _prop_name("NOTION_TITLE_PROP", "")  # MUST be set in your .env
    if not title_prop:
        print("[notion_alert] Missing NOTION_TITLE_PROP (must be your Title column name). Skipping.")
        return

    p_token   = _prop_name("NOTION_PROP_TOKEN", "")
    p_symbol  = _prop_name("NOTION_PROP_SYMBOL", "")
    p_balance = _prop_name("NOTION_PROP_BALANCE", "")
    p_entry   = _prop_name("NOTION_PROP_ENTRY", "")
    p_current = _prop_name("NOTION_PROP_CURRENT", "")
    p_profit  = _prop_name("NOTION_PROP_PROFIT", "")
    p_reason  = _prop_name("NOTION_PROP_REASON", "")
    p_time    = _prop_name("NOTION_PROP_TIME", "")

    # assemble properties — only include those with a name
    title_value = symbol or token or "trade"
    props: Dict[str, dict] = {}

    # Title (required)
    props[title_prop] = {"title": [{"text": {"content": title_value}}]}

    # Rich text / numbers / date (optional, only if named)
    if p_token and p_token != title_prop:
        props[p_token] = {"rich_text": [{"text": {"content": token}}]}

    if p_symbol and p_symbol != title_prop:
        props[p_symbol] = {"rich_text": [{"text": {"content": symbol}}]}

    if p_balance:
        props[p_balance] = {"number": _num(balance)}

    if p_entry:
        props[p_entry] = {"number": _num(entry)}

    if p_current:
        props[p_current] = {"number": _num(current)}

    if p_profit:
        props[p_profit] = {"number": _num(profit)}

    if p_reason:
        props[p_reason] = {"rich_text": [{"text": {"content": reason or ''}}]}

    if p_time:
        props[p_time] = {"date": {"start": datetime.now(timezone.utc).isoformat()}}

    payload = {"parent": {"database_id": database_id}, "properties": props}

    # session handling
    close_when_done = False
    if session is None or session.closed:
        session = aiohttp.ClientSession(
            connector=aiohttp.TCPConnector(ssl=ssl.create_default_context(cafile=certifi.where()))
        )
        close_when_done = True

    try:
        async with session.post(NOTION_PAGES, json=payload, headers=headers) as resp:
            if resp.status not in (200, 201):
                text = await resp.text()
                print(f"[notion_alert] HTTP {resp.status}: {text[:400]}")
    finally:
        if close_when_done and session and not session.closed:
            await session.close()


# Optional convenience wrapper: reads token + DB from env.
# Use ONLY if your caller doesn't already pass them.
async def log_trade_from_env(
    *,
    token: str,
    symbol: str,
    balance: float,
    entry: float,
    current: float,
    profit: float,
    reason: str,
    session: Optional[aiohttp.ClientSession] = None,
) -> None:
    token_auth = _env("NOTION_API_KEY", "")
    database_id = _env("NOTION_DB_TRADES", "")
    if not token_auth or not database_id:
        print("[notion_alert] NOTION_API_KEY / NOTION_DB_TRADES not set. Skipping.")
        return

    await log_trade(
        token=token,
        symbol=symbol,
        balance=balance,
        entry=entry,
        current=current,
        profit=profit,
        reason=reason,
        database_id=database_id,
        token_auth=token_auth,
        session=session,
    )
