#!/usr/bin/env python3
"""
scripts/notion_tail.py

God-Tier Notion Tail (READ-ONLY, secret-safe, Render+local identical)
====================================================================

Mission
-------
Tail your Notion “trades” database safely and reliably:

  • Query latest rows with pagination (Notion DB query API)
  • Optional filters: title contains, mint contains, date range
  • Sort by created_time / last_edited_time / a date property
  • Output: human table (default) or JSON / JSONL

Hard guarantees
---------------
- READ ONLY: uses only GET database schema + POST database query.
- Never prints secrets: tokens are never echoed; errors are sanitized.
- Works even if property names differ: can auto-discover by schema + heuristics.

Env (preferred; compatible with your repo)
-----------------------------------------
Auth:
  NOTION_API_KEY     (preferred)
  NOTION_TOKEN       (fallback)

DB id (any one):
  NOTION_DB_TRADES
  NOTION_DATABASE_ID
  NOTION_DB_ID

Property-name overrides (optional)
----------------------------------
  NOTION_TITLE_PROP / NOTION_COL_TITLE
  NOTION_PROP_TOKEN / NOTION_COL_MINT
  NOTION_PROP_ENTRY
  NOTION_PROP_CURRENT
  NOTION_PROP_PROFIT
  NOTION_PROP_REASON
  NOTION_PROP_TIME / NOTION_COL_TIMESTAMP

HTTP tuning (optional)
----------------------
  NOTION_HTTP_TIMEOUT_S   (default 20)
  NOTION_HTTP_RETRIES     (default 2)

CLI
---
python -m scripts.notion_tail --limit 15
python -m scripts.notion_tail --title BONK --since 2026-01-01
python -m scripts.notion_tail --mint So1111 --sort edited --json
python -m scripts.notion_tail --date-mode property --date-prop "Timestamp" --since 2026-01-01 --until 2026-01-04
python -m scripts.notion_tail --discover

Exit codes
----------
0 OK
1 HTTP/API failure
2 Missing env/args/deps
3 Invalid args / schema mismatch / malformed response
"""

from __future__ import annotations

import argparse
import contextlib
import datetime as dt
import json
import os
import re
import sys
import time
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any

# Optional dotenv (local dev only)
try:
    from dotenv import find_dotenv, load_dotenv  # type: ignore
except Exception:
    find_dotenv = None  # type: ignore[assignment]
    load_dotenv = None  # type: ignore[assignment]
else:
    with contextlib.suppress(Exception):
        load_dotenv(find_dotenv(), override=False)

try:
    import requests  # type: ignore
except Exception:
    requests = None  # type: ignore[assignment]


NOTION_VERSION = "2022-06-28"
API_DB_QUERY = "https://api.notion.com/v1/databases/{db}/query"
API_DB_GET = "https://api.notion.com/v1/databases/{db}"

_TRUE = {"1", "true", "t", "yes", "y", "on"}
_FALSE = {"0", "false", "f", "no", "n", "off"}

_URL_CRED_RE = re.compile(r"(://)([^/@\s]+)@")
_LONG_TOKENISH_RE = re.compile(r"\b[A-Za-z0-9_\-]{28,}\b")


# =============================================================================
# env + safe printing
# =============================================================================
def _envs(k: str, d: str = "") -> str:
    v = os.getenv(k)
    s = v if v is not None else d
    s = str(s).strip()
    if (s.startswith('"') and s.endswith('"')) or (s.startswith("'") and s.endswith("'")):
        s = s[1:-1].strip()
    return s


def _envb(k: str, d: bool = False) -> bool:
    v = os.getenv(k)
    if v is None:
        return d
    s = str(v).strip().lower()
    if s in _TRUE:
        return True
    if s in _FALSE:
        return False
    return d


def _envi(k: str, d: int = 0) -> int:
    s = _envs(k, "")
    if not s:
        return d
    with contextlib.suppress(Exception):
        return int(str(s).strip())
    return d


def _envf(k: str, d: float = 0.0) -> float:
    s = _envs(k, "")
    if not s:
        return d
    with contextlib.suppress(Exception):
        return float(str(s).strip())
    return d


def _ts() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def _safe_print(line: str, *, file) -> None:
    try:
        file.write(line + ("" if line.endswith("\n") else "\n"))
        file.flush()
    except UnicodeEncodeError:
        enc = getattr(file, "encoding", None) or "utf-8"
        b = line.encode(enc, errors="replace")
        with contextlib.suppress(Exception):
            file.buffer.write(b + b"\n")  # type: ignore[attr-defined]
            file.buffer.flush()  # type: ignore[attr-defined]
    except Exception:
        pass


def _hlog(msg: str, *, quiet: bool) -> None:
    if quiet:
        return
    _safe_print(f"[notion_tail] {_ts()} {msg}", file=sys.stderr)


def _sanitize(x: Any) -> str:
    if x is None:
        return ""
    s = str(x)
    s = _URL_CRED_RE.sub(r"\1***@", s)
    s = _LONG_TOKENISH_RE.sub("***", s)
    return s


def _need_requests_or_exit() -> None:
    if requests is None:
        _safe_print("❌ Missing dependency: requests", file=sys.stderr)
        raise SystemExit(2)


# =============================================================================
# date parsing (accepts YYYY-MM-DD or ISO timestamps)
# =============================================================================
def _parse_date_like(s: str) -> str:
    s = (s or "").strip()
    if not s:
        return ""
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", s):
        return s
    try:
        t = s.replace("Z", "+00:00")
        d = dt.datetime.fromisoformat(t)
        if d.tzinfo is None:
            d = d.replace(tzinfo=dt.UTC)
        return d.astimezone(dt.UTC).isoformat().replace("+00:00", "Z")
    except Exception:
        return s


# =============================================================================
# Notion schema + property mapping
# =============================================================================
@dataclass(frozen=True)
class PropMap:
    title: str
    mint: str
    entry: str
    current: str
    profit: str
    reason: str
    ts: str


@dataclass(frozen=True)
class SchemaIndex:
    props: dict[str, dict[str, Any]]
    types: dict[str, str]
    title_prop: str

    @classmethod
    def from_db_json(cls, db_json: dict[str, Any]) -> SchemaIndex:
        raw = (db_json or {}).get("properties") or {}
        props: dict[str, dict[str, Any]] = {}
        for k, v in raw.items() if isinstance(raw, dict) else []:
            props[str(k)] = v if isinstance(v, dict) else {}
        types = {k: str(props.get(k, {}).get("type") or "") for k in props}
        title_prop = next((k for k, t in types.items() if t == "title"), "")
        return cls(props=props, types=types, title_prop=title_prop)


def _env_prop_map() -> PropMap:
    return PropMap(
        title=_envs("NOTION_TITLE_PROP", _envs("NOTION_COL_TITLE", "Token")) or "Token",
        mint=_envs("NOTION_PROP_TOKEN", _envs("NOTION_COL_MINT", "Mint")) or "Mint",
        entry=_envs("NOTION_PROP_ENTRY", "Entry Price") or "Entry Price",
        current=_envs("NOTION_PROP_CURRENT", "Current Price") or "Current Price",
        profit=_envs("NOTION_PROP_PROFIT", "Profit") or "Profit",
        reason=_envs("NOTION_PROP_REASON", "Reason") or "Reason",
        ts=_envs("NOTION_PROP_TIME", _envs("NOTION_COL_TIMESTAMP", "Timestamp")) or "Timestamp",
    )


def _pick_db_id(cli_db: str | None) -> str:
    if cli_db:
        return cli_db.strip()
    for k in ("NOTION_DB_TRADES", "NOTION_DATABASE_ID", "NOTION_DB_ID"):
        v = _envs(k, "")
        if v:
            return v
    return ""


def _pick_token() -> str:
    return _envs("NOTION_API_KEY", "") or _envs("NOTION_TOKEN", "")


def _headers(token: str) -> dict[str, str]:
    return {
        "Authorization": f"Bearer {token}",
        "Notion-Version": NOTION_VERSION,
        "Content-Type": "application/json",
    }


# =============================================================================
# HTTP helpers (retryable)
# =============================================================================
def _http_post_json(
    sess: Any,
    url: str,
    headers: dict[str, str],
    payload: dict[str, Any],
    *,
    timeout_s: float,
    retries: int,
    quiet: bool,
) -> tuple[int, dict[str, Any], str]:
    err = ""
    for attempt in range(max(0, retries) + 1):
        try:
            r = sess.post(url, headers=headers, json=payload, timeout=timeout_s)
            sc = int(getattr(r, "status_code", 0))
            if sc in (429, 500, 502, 503, 504) and attempt < retries:
                wait = min(8.0, 0.6 * (1.7**attempt))
                _hlog(
                    f"retryable HTTP {sc}; backoff {wait:.2f}s (attempt {attempt + 1}/{retries})",
                    quiet=quiet,
                )
                time.sleep(wait)
                continue
            if sc != 200:
                return sc, {}, _sanitize(getattr(r, "text", "")[:2000])
            with contextlib.suppress(Exception):
                return sc, r.json(), ""
            return sc, {}, "bad_json"
        except Exception as e:
            err = _sanitize(f"{type(e).__name__}: {e}")
            if attempt < retries:
                wait = min(8.0, 0.6 * (1.7**attempt))
                _hlog(f"network error; backoff {wait:.2f}s ({err})", quiet=quiet)
                time.sleep(wait)
                continue
            return 0, {}, err
    return 0, {}, err


def _http_get_json(
    sess: Any,
    url: str,
    headers: dict[str, str],
    *,
    timeout_s: float,
    retries: int,
    quiet: bool,
) -> tuple[int, dict[str, Any], str]:
    err = ""
    for attempt in range(max(0, retries) + 1):
        try:
            r = sess.get(url, headers=headers, timeout=timeout_s)
            sc = int(getattr(r, "status_code", 0))
            if sc in (429, 500, 502, 503, 504) and attempt < retries:
                wait = min(8.0, 0.6 * (1.7**attempt))
                _hlog(
                    f"retryable HTTP {sc}; backoff {wait:.2f}s (attempt {attempt + 1}/{retries})",
                    quiet=quiet,
                )
                time.sleep(wait)
                continue
            if sc != 200:
                return sc, {}, _sanitize(getattr(r, "text", "")[:2000])
            with contextlib.suppress(Exception):
                return sc, r.json(), ""
            return sc, {}, "bad_json"
        except Exception as e:
            err = _sanitize(f"{type(e).__name__}: {e}")
            if attempt < retries:
                wait = min(8.0, 0.6 * (1.7**attempt))
                _hlog(f"network error; backoff {wait:.2f}s ({err})", quiet=quiet)
                time.sleep(wait)
                continue
            return 0, {}, err
    return 0, {}, err


def _discover_prop_map(index: SchemaIndex, fallback: PropMap) -> PropMap:
    if not index.props:
        return fallback

    def find_by_names(names: Sequence[str]) -> str | None:
        low = {k.lower(): k for k in index.props}
        for n in names:
            k = low.get(str(n).lower())
            if k:
                return k
        for n in names:
            needle = str(n).lower()
            for k in index.props:
                if needle in k.lower():
                    return k
        return None

    title_name = index.title_prop or fallback.title
    mint = (
        find_by_names(["Mint", "Token", "Token Mint", "Address", "Mint Address"]) or fallback.mint
    )
    entry = find_by_names(["Entry", "Entry Price", "Buy", "Buy Price"]) or fallback.entry
    current = find_by_names(["Current", "Current Price", "Price", "Mark"]) or fallback.current
    profit = find_by_names(["Profit", "PnL", "P&L", "Realized", "Unrealized"]) or fallback.profit
    reason = find_by_names(["Reason", "Notes", "Why", "Tag"]) or fallback.reason
    ts = find_by_names(["Timestamp", "Time", "TS", "Date"]) or fallback.ts

    return PropMap(
        title=title_name,
        mint=mint,
        entry=entry,
        current=current,
        profit=profit,
        reason=reason,
        ts=ts,
    )


# =============================================================================
# property extraction (robust across number/formula/rollup)
# =============================================================================
def _join_plain_text(items: Any) -> str | None:
    if not isinstance(items, list):
        return None
    out = [str(x.get("plain_text")) for x in items if isinstance(x, dict) and x.get("plain_text")]
    s = "".join(out).strip()
    return s or None


def _extract_text(prop: dict[str, Any]) -> str | None:
    if not isinstance(prop, dict):
        return None

    t = prop.get("title")
    if isinstance(t, list):
        return _join_plain_text(t)

    rt = prop.get("rich_text")
    if isinstance(rt, list):
        return _join_plain_text(rt)

    sel = prop.get("select")
    if isinstance(sel, dict) and sel.get("name"):
        return str(sel["name"])

    ms = prop.get("multi_select")
    if isinstance(ms, list):
        names = [x.get("name") for x in ms if isinstance(x, dict) and x.get("name")]
        return ",".join(map(str, names)) if names else None

    if prop.get("type") == "formula":
        f = prop.get("formula") or {}
        if isinstance(f, dict) and f.get("type") == "string":
            v = f.get("string")
            return str(v) if v else None

    if prop.get("type") == "rollup":
        r = prop.get("rollup") or {}
        if isinstance(r, dict) and r.get("type") == "array":
            arr = r.get("array")
            if isinstance(arr, list):
                for item in arr:
                    if isinstance(item, dict):
                        # best-effort: first string-ish field
                        for key in ("title", "rich_text"):
                            v = item.get(key)
                            if isinstance(v, list):
                                s = _join_plain_text(v)
                                if s:
                                    return s
                        if item.get("name"):
                            return str(item["name"])
    return None


def _extract_number(prop: dict[str, Any]) -> float | None:
    if not isinstance(prop, dict):
        return None

    if prop.get("type") == "number":
        v = prop.get("number")
        return float(v) if isinstance(v, int | float) else None

    if prop.get("type") == "formula":
        f = prop.get("formula") or {}
        if isinstance(f, dict) and f.get("type") == "number":
            v = f.get("number")
            return float(v) if isinstance(v, int | float) else None

    if prop.get("type") == "rollup":
        r = prop.get("rollup") or {}
        if isinstance(r, dict):
            rt = r.get("type")
            if rt == "number":
                v = r.get("number")
                return float(v) if isinstance(v, int | float) else None
            if rt == "array":
                arr = r.get("array")
                if isinstance(arr, list):
                    for item in arr:
                        if isinstance(item, dict):
                            pv = item.get("number")
                            if isinstance(pv, int | float):
                                return float(pv)
    return None


def _extract_date(prop: dict[str, Any]) -> str | None:
    if not isinstance(prop, dict):
        return None

    if prop.get("type") == "date":
        d = prop.get("date")
        if isinstance(d, dict) and d.get("start"):
            return str(d.get("start"))
        return None

    if prop.get("type") == "formula":
        f = prop.get("formula") or {}
        if isinstance(f, dict) and f.get("type") == "date":
            d = f.get("date") or {}
            if isinstance(d, dict) and d.get("start"):
                return str(d.get("start"))
    return None


# =============================================================================
# notion query building
# =============================================================================
def _text_filter_clause(
    prop_name: str, prop_type: str, contains: str, *, quiet: bool, label: str
) -> dict[str, Any] | None:
    pt = (prop_type or "").lower()
    if pt == "title":
        return {"property": prop_name, "title": {"contains": contains}}
    if pt == "rich_text":
        return {"property": prop_name, "rich_text": {"contains": contains}}
    if pt == "email":
        return {"property": prop_name, "email": {"contains": contains}}
    if pt == "url":
        return {"property": prop_name, "url": {"contains": contains}}
    if pt == "phone_number":
        return {"property": prop_name, "phone_number": {"contains": contains}}
    if pt == "formula":
        return {"property": prop_name, "formula": {"string": {"contains": contains}}}

    _hlog(
        f"warn: skipping {label} filter; property '{prop_name}' has unsupported type '{prop_type}'",
        quiet=quiet,
    )
    return None


def _build_filter(
    *,
    title_contains: str | None,
    mint_contains: str | None,
    since: str | None,
    until: str | None,
    date_mode: str,
    date_prop: str | None,
    props: PropMap,
    index: SchemaIndex,
    quiet: bool,
) -> dict[str, Any] | None:
    clauses: list[dict[str, Any]] = []

    if title_contains:
        p = props.title
        c = _text_filter_clause(
            p, index.types.get(p, ""), title_contains, quiet=quiet, label="title"
        )
        if c:
            clauses.append(c)

    if mint_contains:
        p = props.mint
        c = _text_filter_clause(p, index.types.get(p, ""), mint_contains, quiet=quiet, label="mint")
        if c:
            clauses.append(c)

    s = _parse_date_like(since or "") if since else ""
    u = _parse_date_like(until or "") if until else ""

    if s or u:
        if date_mode == "created":
            df: dict[str, Any] = {}
            if s:
                df["on_or_after"] = s
            if u:
                df["on_or_before"] = u
            clauses.append({"timestamp": "created_time", "created_time": df})

        elif date_mode == "edited":
            df2: dict[str, Any] = {}
            if s:
                df2["on_or_after"] = s
            if u:
                df2["on_or_before"] = u
            clauses.append({"timestamp": "last_edited_time", "last_edited_time": df2})

        else:
            prop_name = (date_prop or props.ts).strip()
            pt = (index.types.get(prop_name, "") or "").lower()
            df3: dict[str, Any] = {}
            if s:
                df3["on_or_after"] = s
            if u:
                df3["on_or_before"] = u

            if pt == "date":
                clauses.append({"property": prop_name, "date": df3})
            elif pt == "formula":
                clauses.append({"property": prop_name, "formula": {"date": df3}})
            else:
                _hlog(
                    f"warn: skipping date filter; property '{prop_name}' has type '{pt}' (use --date-mode created/edited or --date-prop)",
                    quiet=quiet,
                )

    if not clauses:
        return None
    return clauses[0] if len(clauses) == 1 else {"and": clauses}


def _build_sorts(
    *,
    sort_mode: str,
    props: PropMap,
    date_prop: str | None,
    index: SchemaIndex,
    quiet: bool,
) -> list[dict[str, Any]]:
    if sort_mode == "edited":
        return [{"timestamp": "last_edited_time", "direction": "descending"}]
    if sort_mode == "timestamp":
        prop_name = (date_prop or props.ts).strip()
        pt = (index.types.get(prop_name, "") or "").lower()
        if pt in {"date", "formula"}:
            return [{"property": prop_name, "direction": "descending"}]
        _hlog(
            f"warn: sort=timestamp requested but '{prop_name}' is type '{pt}'; falling back to created_time",
            quiet=quiet,
        )
    return [{"timestamp": "created_time", "direction": "descending"}]


# =============================================================================
# formatting
# =============================================================================
def _truncate(s: str, n: int) -> str:
    s = s or ""
    if n <= 0:
        return s
    return s if len(s) <= n else (s[: max(0, n - 1)] + "…")


def _fmt_num(x: float | None) -> str:
    if x is None:
        return ""
    if abs(x) >= 1000:
        return f"{x:,.2f}"
    return f"{x:.6f}".rstrip("0").rstrip(".")


def _row_from_page(
    page: dict[str, Any], props: PropMap, *, date_prop: str | None
) -> dict[str, Any]:
    p = page.get("properties") or {}
    if not isinstance(p, dict):
        p = {}

    title_v = _extract_text(p.get(props.title, {}))
    mint_v = _extract_text(p.get(props.mint, {}))
    entry_v = _extract_number(p.get(props.entry, {}))
    cur_v = _extract_number(p.get(props.current, {}))
    prof_v = _extract_number(p.get(props.profit, {}))
    reason_v = _extract_text(p.get(props.reason, {}))

    ts_name = (date_prop or props.ts).strip()
    ts_v = _extract_date(p.get(ts_name, {}))

    return {
        "Created": page.get("created_time"),
        "Edited": page.get("last_edited_time"),
        "Title": title_v,
        "Mint": mint_v,
        "Entry": entry_v,
        "Current": cur_v,
        "Profit": prof_v,
        "Reason": reason_v,
        "TS": ts_v,
        "URL": page.get("url"),
        "ID": page.get("id"),
    }


def _print_table(rows: list[dict[str, Any]], *, wide: bool, no_trunc: bool) -> None:
    headers = ["Created", "Title", "Mint", "Entry", "Current", "Profit", "Reason", "TS"]
    maxw = {
        "Created": 20,
        "Title": 18 if not wide else 32,
        "Mint": 16 if not wide else 44,
        "Entry": 12,
        "Current": 12,
        "Profit": 12,
        "Reason": 18 if not wide else 44,
        "TS": 20,
    }

    table: list[list[str]] = [
        [
            str(r.get("Created") or ""),
            str(r.get("Title") or ""),
            str(r.get("Mint") or ""),
            _fmt_num(r.get("Entry")),
            _fmt_num(r.get("Current")),
            _fmt_num(r.get("Profit")),
            str(r.get("Reason") or ""),
            str(r.get("TS") or ""),
        ]
        for r in rows
    ]

    widths: list[int] = []
    for i, h in enumerate(headers):
        w = max(len(h), 8, *(len(row[i]) for row in table)) if table else max(len(h), 8)
        cap = maxw.get(h, 24)
        widths.append(w if no_trunc else min(w, cap))

    def fmt_row(cols: Sequence[str]) -> str:
        out: list[str] = []
        for i, s in enumerate(cols):
            v = s or ""
            if not no_trunc:
                v = _truncate(v, widths[i])
            out.append(v.ljust(widths[i]))
        return " | ".join(out)

    print(fmt_row(headers))
    print("-" * (sum(widths) + 3 * (len(widths) - 1)))
    for row in table:
        print(fmt_row(row))


# =============================================================================
# main
# =============================================================================
def main(argv: Sequence[str] | None = None) -> int:
    _need_requests_or_exit()

    ap = argparse.ArgumentParser(description="Tail Notion trades DB (read-only).")
    ap.add_argument(
        "--db", help="Override Notion database id (else env NOTION_DB_TRADES/NOTION_DATABASE_ID)."
    )
    ap.add_argument("--limit", type=int, default=10, help="Max rows to print (1..5000).")
    ap.add_argument("--page-size", type=int, default=50, help="Notion page_size (1..100).")
    ap.add_argument("--max-pages", type=int, default=50, help="Safety cap on pagination pages.")
    ap.add_argument("--sort", choices=["created", "edited", "timestamp"], default="created")
    ap.add_argument(
        "--date-mode",
        choices=["property", "created", "edited"],
        default="property",
        help="Which field 'since/until' applies to.",
    )
    ap.add_argument(
        "--date-prop",
        help="Name of the date property used when date-mode=property or sort=timestamp.",
    )
    ap.add_argument("--title", help="Filter: title contains")
    ap.add_argument("--mint", help="Filter: mint contains")
    ap.add_argument("--since", help="Date filter start (YYYY-MM-DD or ISO).")
    ap.add_argument("--until", help="Date filter end (YYYY-MM-DD or ISO).")
    ap.add_argument("--json", action="store_true", help="Output JSON rows instead of a table.")
    ap.add_argument("--jsonl", action="store_true", help="Output JSON Lines (one row per line).")
    ap.add_argument("--wide", action="store_true", help="Wider columns in table mode.")
    ap.add_argument("--no-trunc", action="store_true", help="Disable truncation in table mode.")
    ap.add_argument("--quiet", action="store_true", help="Suppress human logs.")
    ap.add_argument("--timeout", type=float, default=_envf("NOTION_HTTP_TIMEOUT_S", 20.0))
    ap.add_argument("--retries", type=int, default=_envi("NOTION_HTTP_RETRIES", 2))
    ap.add_argument(
        "--discover",
        action="store_true",
        help="Fetch schema + print the discovered property mapping and exit (no query).",
    )
    args = ap.parse_args(list(argv) if argv is not None else None)

    token = _pick_token()
    db = _pick_db_id(args.db)

    if not token:
        _safe_print("❌ Missing NOTION_API_KEY (or NOTION_TOKEN).", file=sys.stderr)
        return 2
    if not db:
        _safe_print(
            "❌ Missing DB id. Set NOTION_DB_TRADES (or NOTION_DATABASE_ID) or pass --db.",
            file=sys.stderr,
        )
        return 2

    limit = max(1, min(5000, int(args.limit)))
    page_size = max(1, min(100, int(args.page_size)))
    max_pages = max(1, min(500, int(args.max_pages)))

    sess = requests.Session()  # type: ignore[union-attr]
    headers = _headers(token)

    sc, db_json, err = _http_get_json(
        sess,
        API_DB_GET.format(db=db),
        headers,
        timeout_s=max(5.0, float(args.timeout)),
        retries=max(0, int(args.retries)),
        quiet=bool(args.quiet),
    )
    if sc != 200:
        _safe_print(f"❌ Notion DB schema fetch failed HTTP {sc}: {err}", file=sys.stderr)
        return 1

    index = SchemaIndex.from_db_json(db_json)
    env_map = _env_prop_map()
    prop_map = _discover_prop_map(index, env_map)

    if args.discover:
        out = {
            "db": db,
            "props_env": env_map.__dict__,
            "props_discovered": prop_map.__dict__,
            "note": "If discovered looks right, you can copy these into env vars to lock-in.",
        }
        print(json.dumps(out, indent=2, ensure_ascii=False))
        return 0

    flt = _build_filter(
        title_contains=args.title,
        mint_contains=args.mint,
        since=args.since,
        until=args.until,
        date_mode=args.date_mode,
        date_prop=args.date_prop,
        props=prop_map,
        index=index,
        quiet=bool(args.quiet),
    )
    sorts = _build_sorts(
        sort_mode=args.sort,
        props=prop_map,
        date_prop=args.date_prop,
        index=index,
        quiet=bool(args.quiet),
    )

    url = API_DB_QUERY.format(db=db)
    rows: list[dict[str, Any]] = []
    cursor: str | None = None

    for page_idx in range(max_pages):
        remaining = limit - len(rows)
        if remaining <= 0:
            break

        payload: dict[str, Any] = {"page_size": min(page_size, remaining), "sorts": sorts}
        if flt:
            payload["filter"] = flt
        if cursor:
            payload["start_cursor"] = cursor

        sc2, data, err2 = _http_post_json(
            sess,
            url,
            headers,
            payload,
            timeout_s=max(5.0, float(args.timeout)),
            retries=max(0, int(args.retries)),
            quiet=bool(args.quiet),
        )
        if sc2 != 200:
            _safe_print(f"❌ Notion query failed HTTP {sc2}: {err2}", file=sys.stderr)
            return 1

        results = data.get("results") or []
        if not isinstance(results, list):
            _safe_print("❌ Notion response malformed: results not a list", file=sys.stderr)
            return 3

        rows.extend(
            _row_from_page(p, prop_map, date_prop=args.date_prop)
            for p in results
            if isinstance(p, dict)
        )

        if len(rows) >= limit:
            rows = rows[:limit]
            break

        has_more = bool(data.get("has_more"))
        cursor = data.get("next_cursor")
        if not has_more or not cursor:
            break

        _hlog(
            f"page {page_idx + 1} fetched {len(results)} rows (total {len(rows)})",
            quiet=bool(args.quiet),
        )

    if args.jsonl:
        for r in rows:
            print(json.dumps(r, ensure_ascii=False, default=str))
        return 0

    if args.json:
        print(json.dumps(rows, indent=2, ensure_ascii=False, default=str))
        return 0

    _print_table(rows, wide=bool(args.wide), no_trunc=bool(args.no_trunc))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
