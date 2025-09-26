# scripts/notion_smoke.py
"""
Create a single test row in your Notion trades DB (no trade required).

Env it understands (both new & legacy names supported):
- NOTION_TOKEN or NOTION_API_KEY
- NOTION_DATABASE_ID or NOTION_DB_TRADES
- NOTION_API_VERSION (optional, default 2022-06-28)

Column name mapping (DB property names), with flexible overrides:
- Defaults (if present in DB): Token, Mint, Balance, Entry Price,
  Current Price, Profit, Reason, Timestamp
- You can override via any of these:
    NOTION_COL_TITLE / NOTION_TITLE_PROP
    NOTION_COL_MINT  / NOTION_PROP_TOKEN
    NOTION_COL_BALANCE / NOTION_PROP_BALANCE
    NOTION_COL_ENTRY / NOTION_PROP_ENTRY
    NOTION_COL_CURRENT / NOTION_PROP_CURRENT
    NOTION_COL_PROFIT / NOTION_PROP_PROFIT
    NOTION_COL_REASON / NOTION_PROP_REASON
    NOTION_COL_TIMESTAMP / NOTION_PROP_TIME
  Or set NOTION_FIELDS_JSON / NOTION_MAP_JSON with a JSON object like:
    {"title": "Token", "mint": "Mint", "entry": "Entry Price", ...}

Usage examples:
  python -m scripts.notion_smoke \
    --title "SMOKE TEST" --mint TEST --entry 1 --current 1.1 --profit 0.1 --reason check

  # Minimal:
  python -m scripts.notion_smoke
"""
import os, sys, json, argparse, datetime, requests
from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv())

def env_first(keys, default=None):
    """Return the first non-empty env value among keys."""
    for k in keys:
        v = os.getenv(k)
        if v is not None and str(v).strip() != "":
            return str(v).strip()
    return default

def fields_map():
    """
    Resolve column names from env.
    Supports both the new NOTION_COL_* and legacy NOTION_*_PROP names.
    Optional NOTION_FIELDS_JSON / NOTION_MAP_JSON can override any key.
    Keys we use internally: title, mint, balance, entry, current, profit, reason, timestamp
    """
    m = {
        "title":     env_first(["NOTION_COL_TITLE","NOTION_TITLE_PROP"], "Token"),
        "mint":      env_first(["NOTION_COL_MINT","NOTION_PROP_TOKEN"], "Mint"),
        "balance":   env_first(["NOTION_COL_BALANCE","NOTION_PROP_BALANCE"], "Balance"),
        "entry":     env_first(["NOTION_COL_ENTRY","NOTION_PROP_ENTRY"], "Entry Price"),
        "current":   env_first(["NOTION_COL_CURRENT","NOTION_PROP_CURRENT"], "Current Price"),
        "profit":    env_first(["NOTION_COL_PROFIT","NOTION_PROP_PROFIT"], "Profit"),
        "reason":    env_first(["NOTION_COL_REASON","NOTION_PROP_REASON"], "Reason"),
        "timestamp": env_first(["NOTION_COL_TIMESTAMP","NOTION_PROP_TIME"], "Timestamp"),
    }
    blob = os.getenv("NOTION_FIELDS_JSON") or os.getenv("NOTION_MAP_JSON")
    if blob:
        try:
            overrides = json.loads(blob)
            if isinstance(overrides, dict):
                m.update({str(k): str(v) for k, v in overrides.items()})
        except Exception:
            # ignore bad override payloads; keep defaults
            pass
    return m

def get_db_props(session, db_id, headers):
    """Fetch the database schema and return {prop_name: type}."""
    r = session.get(f"https://api.notion.com/v1/databases/{db_id}", headers=headers, timeout=20)
    r.raise_for_status()
    js = r.json()
    props = js.get("properties", {}) or {}
    return { name: props[name].get("type") for name in props }

def build_properties(schema_names, fmap, sample):
    """
    Build the 'properties' dict for page creation, but only include
    keys that actually exist in the Notion DB schema.
    """
    props = {}

    def include(col_name, payload):
        if col_name in schema_names:
            props[col_name] = payload
        else:
            print(f"[notion_smoke] (skip) column not found in Notion DB: {col_name}")

    # Title
    include(fmap["title"], {"title": [{"text": {"content": sample["title"]}}]})
    # Mint
    include(fmap["mint"], {"rich_text": [{"text": {"content": sample["mint"]}}]})
    # Balance
    include(fmap["balance"], {"number": sample["balance"]})
    # Entry / Current / Profit
    include(fmap["entry"], {"number": sample["entry"]})
    include(fmap["current"], {"number": sample["current"]})
    include(fmap["profit"], {"number": sample["profit"]})
    # Reason
    include(fmap["reason"], {"rich_text": [{"text": {"content": sample["reason"]}}]})
    # Timestamp
    include(fmap["timestamp"], {"date": {"start": sample["timestamp"]}})
    return props

def main():
    ap = argparse.ArgumentParser(description="Create a single test row in your Notion trades DB.")
    ap.add_argument("--title", default="SMOKE TEST", help="Title text for the test page (default: SMOKE TEST)")
    ap.add_argument("--mint", default="TEST_MINT", help="Mint/token string")
    ap.add_argument("--balance", type=float, default=0.0, help="Balance number")
    ap.add_argument("--entry", type=float, default=0.0, help="Entry price")
    ap.add_argument("--current", type=float, default=0.0, help="Current price")
    ap.add_argument("--profit", type=float, default=0.0, help="Profit number")
    ap.add_argument("--reason", default="notion_smoke", help="Reason text")
    args = ap.parse_args()

    token = env_first(["NOTION_TOKEN","NOTION_API_KEY"])
    db_id = env_first(["NOTION_DATABASE_ID","NOTION_DB_TRADES"])
    if not token or not db_id:
        print("ERROR: set NOTION_TOKEN/NOTION_API_KEY and NOTION_DATABASE_ID/NOTION_DB_TRADES in your .env")
        sys.exit(2)

    fmap = fields_map()
    now_iso = datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
    sample = dict(
        title=args.title,
        mint=args.mint,
        balance=args.balance,
        entry=args.entry,
        current=args.current,
        profit=args.profit,
        reason=args.reason,
        timestamp=now_iso,
    )

    headers = {
        "Authorization": f"Bearer {token}",
        "Notion-Version": os.getenv("NOTION_API_VERSION", "2022-06-28"),
        "Content-Type": "application/json",
    }

    with requests.Session() as s:
        try:
            # fetch schema & build safe properties payload
            schema = get_db_props(s, db_id, headers)
            schema_names = set(schema.keys())
            props = build_properties(schema_names, fmap, sample)

            payload = {"parent": {"database_id": db_id}, "properties": props}
            r = s.post("https://api.notion.com/v1/pages",
                       headers=headers, data=json.dumps(payload), timeout=20)

            if r.status_code >= 400:
                print("[notion_smoke] HTTP", r.status_code, r.text[:400])
                sys.exit(1)

            js = r.json()
            page_id = js.get("id")
            print("✅ Created Notion page:", page_id)
        except requests.RequestException as e:
            print("[notion_smoke] network error:", e)
            sys.exit(3)
        except Exception as e:
            print("[notion_smoke] error:", e)
            sys.exit(4)

if __name__ == "__main__":
    main()
