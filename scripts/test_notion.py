#!/usr/bin/env python3
"""
scripts/test_notion.py
Quick sanity check for Notion logging:
- Reads NOTION_* env vars (same names your bot uses)
- Creates a dummy page in the configured database
- Prints success/failure details

Usage:
  python scripts/test_notion.py
"""
import os, sys, json, time, datetime as dt, requests
from dotenv import load_dotenv

load_dotenv()  # <-- this loads .env into os.environ

import os
import sys
import json
import time
import datetime as dt
import requests

NOTION_API_URL = "https://api.notion.com/v1/pages"
NOTION_VERSION = "2022-06-28"

def env(name: str, default: str = "") -> str:
    return os.getenv(name, default).strip()

def build_properties():
    """
    Build Notion properties payload from your env-driven column names.
    Defaults match what you told me your DB uses.
    """
    title_prop   = env("NOTION_TITLE_PROP",   "Token")
    mint_prop    = env("NOTION_PROP_TOKEN",   "Mint")
    bal_prop     = env("NOTION_PROP_BALANCE", "Balance")
    entry_prop   = env("NOTION_PROP_ENTRY",   "Entry Price")
    curr_prop    = env("NOTION_PROP_CURRENT", "Current Price")
    profit_prop  = env("NOTION_PROP_PROFIT",  "Profit")
    reason_prop  = env("NOTION_PROP_REASON",  "Reason")
    time_prop    = env("NOTION_PROP_TIME",    "Timestamp")

    # Dummy sample data
    now_iso = dt.datetime.utcnow().isoformat()
    token_symbol = f"TEST-{int(time.time())}"
    mint = "So11111111111111111111111111111111111111112"  # WSOL (just for testing)
    balance = 1.2345
    entry_price = 0.0123
    current_price = 0.0456
    profit = round((current_price - entry_price) * balance, 6)
    reason = "Notion connection test ✅"

    props = {
        title_prop:   {"title": [{"text": {"content": token_symbol}}]},
        mint_prop:    {"rich_text": [{"text": {"content": mint}}]},
        bal_prop:     {"number": balance},
        entry_prop:   {"number": entry_price},
        curr_prop:    {"number": current_price},
        profit_prop:  {"number": profit},
        reason_prop:  {"rich_text": [{"text": {"content": reason}}]},
        time_prop:    {"date": {"start": now_iso}},
    }
    return props

def main():
    # Prefer NOTION_API_KEY; fall back to NOTION_TOKEN for compatibility
    notion_token = env("NOTION_API_KEY") or env("NOTION_TOKEN")
    database_id  = env("NOTION_DATABASE_ID")

    if not notion_token:
        print("❌ Missing NOTION_API_KEY (or NOTION_TOKEN) in environment.", file=sys.stderr)
        sys.exit(1)
    if not database_id:
        print("❌ Missing NOTION_DATABASE_ID in environment.", file=sys.stderr)
        sys.exit(1)

    props = build_properties()

    headers = {
        "Authorization": f"Bearer {notion_token}",
        "Notion-Version": NOTION_VERSION,
        "Content-Type": "application/json",
    }
    body = {
        "parent": {"database_id": database_id},
        "properties": props,
    }

    print("➡️  Sending test row to Notion…")
    try:
        resp = requests.post(NOTION_API_URL, headers=headers, json=body, timeout=20)
    except requests.RequestException as e:
        print(f"❌ Network error talking to Notion: {e}", file=sys.stderr)
        sys.exit(2)

    if resp.status_code in (200, 201):
        data = resp.json()
        page_id = data.get("id")
        url = data.get("url")
        print("✅ Notion test succeeded!")
        print(f"   Page ID: {page_id}")
        print(f"   URL:     {url}")
        sys.exit(0)
    else:
        print("❌ Notion test failed.")
        print(f"   HTTP {resp.status_code}")
        try:
            print("   Response:", json.dumps(resp.json(), indent=2))
        except Exception:
            print("   Response (raw):", resp.text[:1000])
        print("\nTroubleshooting tips:")
        print("  • Verify NOTION_API_KEY is a valid internal integration token (starts with `ntn_`).")
        print("  • Ensure your integration is invited to the database (Share → Invite).")
        print("  • Confirm property names in your DB exactly match your env:")
        print("      NOTION_TITLE_PROP, NOTION_PROP_TOKEN, NOTION_PROP_BALANCE,")
        print("      NOTION_PROP_ENTRY, NOTION_PROP_CURRENT, NOTION_PROP_PROFIT,")
        print("      NOTION_PROP_REASON, NOTION_PROP_TIME")
        sys.exit(3)

if __name__ == "__main__":
    main()
