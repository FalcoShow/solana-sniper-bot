#!/usr/bin/env python3
import os, sys, json, argparse, datetime, requests

from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv())

API = "https://api.notion.com/v1/databases/{db}/query"
VER = "2022-06-28"

def getenv(name, default=None):
    v = os.getenv(name)
    return v if v not in (None, "") else default

def env_props():
    return {
        "title": getenv("NOTION_TITLE_PROP", getenv("NOTION_COL_TITLE", "Token")),
        "mint": getenv("NOTION_PROP_TOKEN", getenv("NOTION_COL_MINT", "Mint")),
        "entry": getenv("NOTION_PROP_ENTRY", "Entry Price"),
        "current": getenv("NOTION_PROP_CURRENT", "Current Price"),
        "profit": getenv("NOTION_PROP_PROFIT", "Profit"),
        "reason": getenv("NOTION_PROP_REASON", "Reason"),
        "ts": getenv("NOTION_PROP_TIME", getenv("NOTION_COL_TIMESTAMP", "Timestamp")),
    }

def to_iso(d):
    if isinstance(d, datetime.date):
        return d.isoformat()
    return str(d)

def build_filter(args, props):
    clauses = []

    # Title filter
    if args.title:
        clauses.append({
            "property": props["title"],
            "title": {"contains": args.title}
        })

    # Mint filter
    if args.mint:
        clauses.append({
            "property": props["mint"],
            "rich_text": {"contains": args.mint}
        })

    # Date range filter on Timestamp property if present, else created_time
    date_prop = props["ts"]
    date_filter_key = {"property": date_prop, "date": {}}

    since = args.since
    until = args.until
    if since or until:
        df = {}
        if since:
            df["on_or_after"] = since
        if until:
            df["on_or_before"] = until
        clauses.append({"property": date_prop, "date": df})
    # If user wants to sort by created, we can still filter by created_time
    # via special "timestamp" filter (Notion supports created_time separately).
    # We'll skip unless explicitly asked; above uses the property date.

    if not clauses:
        return None
    return {"and": clauses}

def build_sorts(args, props):
    sorts = []
    if args.sort == "timestamp":
        sorts.append({"property": props["ts"], "direction": "descending"})
    else:
        sorts.append({"timestamp": "created_time", "direction": "descending"})
    return sorts

def extract_text(prop):
    t = prop.get("title") or prop.get("rich_text")
    if not t:
        return None
    return "".join([x.get("plain_text","") for x in t]) or None

def extract_number(prop):
    return prop.get("number")

def extract_date(prop):
    d = prop.get("date")
    if not d:
        return None
    return d.get("start")

def parse_row(p, props):
    props_obj = p["properties"]
    row = {
        "Created": p.get("created_time"),
        "Title": extract_text(props_obj.get(props["title"], {})) if props["title"] in props_obj else None,
        "Mint": extract_text(props_obj.get(props["mint"], {})) if props["mint"] in props_obj else None,
        "Entry": extract_number(props_obj.get(props["entry"], {})) if props["entry"] in props_obj else None,
        "Current": extract_number(props_obj.get(props["current"], {})) if props["current"] in props_obj else None,
        "Profit": extract_number(props_obj.get(props["profit"], {})) if props["profit"] in props_obj else None,
        "Reason": extract_text(props_obj.get(props["reason"], {})) if props["reason"] in props_obj else None,
        "TS": extract_date(props_obj.get(props["ts"], {})) if props["ts"] in props_obj else None,
    }
    return row

def fmt_row(row, widths):
    cols = [
        row.get("Created") or "",
        row.get("Title") or "",
        row.get("Mint") or "",
        "" if row.get("Entry") is None else str(row["Entry"]),
        "" if row.get("Current") is None else str(row["Current"]),
        "" if row.get("Profit") is None else str(row["Profit"]),
        row.get("Reason") or "",
        row.get("TS") or "",
    ]
    return " | ".join(s.ljust(w)[:w] for s, w in zip(cols, widths))

def main():
    ap = argparse.ArgumentParser(description="Tail Notion trades DB with optional filters.")
    ap.add_argument("--limit", type=int, default=10)
    ap.add_argument("--sort", choices=["created", "timestamp"], default="created")
    ap.add_argument("--json", action="store_true", help="Output JSON instead of table")
    ap.add_argument("--title", help="Filter title contains (e.g., symbol)")
    ap.add_argument("--mint", help="Filter mint contains")
    ap.add_argument("--since", help="Filter Timestamp on_or_after (YYYY-MM-DD)")
    ap.add_argument("--until", help="Filter Timestamp on_or_before (YYYY-MM-DD)")
    args = ap.parse_args()

    key = getenv("NOTION_API_KEY")
    db = getenv("NOTION_DB_TRADES")
    if not key or not db:
        print("Set NOTION_API_KEY and NOTION_DB_TRADES", file=sys.stderr)
        sys.exit(2)

    props = env_props()
    url = API.format(db=db)
    headers = {
        "Authorization": f"Bearer {key}",
        "Notion-Version": VER,
        "Content-Type": "application/json",
    }

    payload = {
        "page_size": max(1, min(100, args.limit)),
        "sorts": build_sorts(args, props),
    }
    flt = build_filter(args, props)
    if flt:
        payload["filter"] = flt

    r = requests.post(url, headers=headers, data=json.dumps(payload))
    if r.status_code != 200:
        print(f"[notion_tail] HTTP {r.status_code}: {r.text}", file=sys.stderr)
        sys.exit(1)

    data = r.json()
    rows = [parse_row(p, props) for p in data.get("results", [])]

    if args.json:
        print(json.dumps(rows, indent=2))
        return

    headers_row = ["Created", "Title", "Mint", "Entry", "Current", "Profit", "Reason", "TS"]
    # compute widths
    widths = [max(len(h), 10) for h in headers_row]
    for row in rows:
        for i, k in enumerate(headers_row):
            widths[i] = max(widths[i], len("" if row.get(k) is None else str(row[k])))

    print(" | ".join(h.ljust(w) for h, w in zip(headers_row, widths)))
    print("-" * (sum(widths) + 3*(len(widths)-1)))
    for row in rows:
        print(fmt_row(row, widths))

if __name__ == "__main__":
    main()
