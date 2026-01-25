#!/usr/bin/env python3
from __future__ import annotations

"""
scripts/notion_smoke.py

God-Tier Notion Smoke (WRITE 1 PAGE, secret-safe, Render+local identical)
========================================================================

Mission
-------
Create exactly ONE test page in your Notion “trades” database to verify:
  • Auth works
  • DB id is correct
  • Column mapping matches your DB
  • Notion API is reachable from this runtime

Safety / guarantees
-------------------
- Writes ONLY a single new page (no updates, no deletes).
- Never prints secrets (token redacted from all logs).
- Schema-aware: fetches DB schema first, then only writes properties that exist
  AND match compatible types (skips mismatches with clear reasons).

Env (preferred; compatible with your repo)
-----------------------------------------
Auth:
  NOTION_API_KEY     (preferred)
  NOTION_TOKEN       (fallback)

DB id (any one):
  NOTION_DB_TRADES
  NOTION_DATABASE_ID
  NOTION_DB_ID

Optional API version:
  NOTION_API_VERSION (default 2022-06-28)

Property name mapping overrides (optional)
-----------------------------------------
You can override ANY of these via env:
  NOTION_TITLE_PROP / NOTION_COL_TITLE
  NOTION_PROP_TOKEN / NOTION_COL_MINT
  NOTION_PROP_BALANCE / NOTION_COL_BALANCE
  NOTION_PROP_ENTRY
  NOTION_PROP_CURRENT
  NOTION_PROP_PROFIT
  NOTION_PROP_REASON
  NOTION_PROP_TIME / NOTION_COL_TIMESTAMP

Or provide a JSON mapping:
  NOTION_FIELDS_JSON or NOTION_MAP_JSON
Example:
  {"title":"Token","mint":"Mint","entry":"Entry Price","timestamp":"Timestamp"}

CLI
---
python -m scripts.notion_smoke
python -m scripts.notion_smoke --title "SMOKE" --mint "So111..." --entry 0.01 --current 0.011 --profit 0.001
python -m scripts.notion_smoke --discover
python -m scripts.notion_smoke --json

Exit codes
----------
0 OK (page created)
1 HTTP/API failure
2 Missing env/args/deps
3 Schema/args mismatch or invalid request payload
"""

import argparse
import contextlib
import datetime as dt
import json
import os
import random
import re
import sys
import time
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any

# Optional dotenv (local dev only)
try:
    from dotenv import find_dotenv, load_dotenv  # type: ignore

    load_dotenv(find_dotenv(), override=False)
except Exception:
    pass

try:
    import requests  # type: ignore

    _HAVE_REQUESTS = True
except Exception:
    requests = None  # type: ignore
    _HAVE_REQUESTS = False


NOTION_API_BASE = "https://api.notion.com/v1"
DEFAULT_NOTION_VERSION = "2022-06-28"

_TRUE = {"1", "true", "t", "yes", "y", "on"}
_FALSE = {"0", "false", "f", "no", "n", "off"}

_URL_CRED_RE = re.compile(r"(://)([^/@\s]+)@")
_LONG_TOKENISH_RE = re.compile(r"\b[A-Za-z0-9_\-]{28,}\b")


# =============================================================================
# helpers (env/log/sanitize)
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
    _safe_print(f"[notion_smoke] {_ts()} {msg}", file=sys.stderr)


def _sanitize(s: Any) -> str:
    if s is None:
        return ""
    x = str(s)
    x = _URL_CRED_RE.sub(r"\1***@", x)
    x = _LONG_TOKENISH_RE.sub("***", x)
    return x


def _need_requests_or_exit() -> None:
    if not _HAVE_REQUESTS:
        _safe_print("❌ Missing dependency: requests", file=sys.stderr)
        raise SystemExit(2)


def _iso_utc_now() -> str:
    # Use tz-aware UTC (preferred in modern Python)
    d = dt.datetime.now(dt.UTC).replace(microsecond=0)
    return d.isoformat().replace("+00:00", "Z")


def _rand_suffix(n: int = 6) -> str:
    alphabet = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"
    return "".join(random.choice(alphabet) for _ in range(max(3, min(12, n))))


# =============================================================================
# mapping + schema
# =============================================================================
@dataclass
class PropMap:
    title: str
    mint: str
    balance: str
    entry: str
    current: str
    profit: str
    reason: str
    timestamp: str


def _pick_token() -> str:
    return _envs("NOTION_API_KEY", "") or _envs("NOTION_TOKEN", "")


def _pick_db_id(cli_db: str | None) -> str:
    if cli_db:
        return cli_db.strip()
    for k in ("NOTION_DB_TRADES", "NOTION_DATABASE_ID", "NOTION_DB_ID"):
        v = _envs(k, "")
        if v:
            return v
    return ""


def _env_first(keys: Sequence[str], default: str = "") -> str:
    for k in keys:
        v = _envs(k, "")
        if v:
            return v
    return default


def _env_prop_map() -> PropMap:
    return PropMap(
        title=_env_first(["NOTION_COL_TITLE", "NOTION_TITLE_PROP"], "Token") or "Token",
        mint=_env_first(["NOTION_COL_MINT", "NOTION_PROP_TOKEN"], "Mint") or "Mint",
        balance=_env_first(["NOTION_COL_BALANCE", "NOTION_PROP_BALANCE"], "Balance") or "Balance",
        entry=_env_first(["NOTION_COL_ENTRY", "NOTION_PROP_ENTRY"], "Entry Price") or "Entry Price",
        current=_env_first(["NOTION_COL_CURRENT", "NOTION_PROP_CURRENT"], "Current Price")
        or "Current Price",
        profit=_env_first(["NOTION_COL_PROFIT", "NOTION_PROP_PROFIT"], "Profit") or "Profit",
        reason=_env_first(["NOTION_COL_REASON", "NOTION_PROP_REASON"], "Reason") or "Reason",
        timestamp=_env_first(["NOTION_COL_TIMESTAMP", "NOTION_PROP_TIME"], "Timestamp")
        or "Timestamp",
    )


def _apply_json_overrides(pm: PropMap) -> PropMap:
    blob = _envs("NOTION_FIELDS_JSON", "") or _envs("NOTION_MAP_JSON", "")
    if not blob:
        return pm
    try:
        j = json.loads(blob)
        if not isinstance(j, dict):
            return pm
        d = dict(pm.__dict__)
        for k, v in j.items():
            ks = str(k).strip()
            vs = str(v).strip()
            if ks in d and vs:
                d[ks] = vs
        return PropMap(**d)
    except Exception:
        return pm


def _headers(token: str, notion_version: str) -> dict[str, str]:
    return {
        "Authorization": f"Bearer {token}",
        "Notion-Version": notion_version,
        "Content-Type": "application/json",
    }


def _http_get_json(
    sess: requests.Session,
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
            sc = int(r.status_code)
            if sc in (429, 500, 502, 503, 504) and attempt < retries:
                wait = min(8.0, 0.6 * (1.7**attempt))
                _hlog(f"retryable HTTP {sc}; backoff {wait:.2f}s", quiet=quiet)
                time.sleep(wait)
                continue
            if sc != 200:
                return sc, {}, _sanitize(r.text[:2000])
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


def _http_post_json(
    sess: requests.Session,
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
            sc = int(r.status_code)
            if sc in (429, 500, 502, 503, 504) and attempt < retries:
                wait = min(8.0, 0.6 * (1.7**attempt))
                _hlog(f"retryable HTTP {sc}; backoff {wait:.2f}s", quiet=quiet)
                time.sleep(wait)
                continue
            if sc not in (200, 201):
                detail = ""
                with contextlib.suppress(Exception):
                    j = r.json()
                    if isinstance(j, dict):
                        detail = f"{j.get('code', '')} {j.get('message', '')}".strip()
                detail = detail or r.text[:2000]
                return sc, {}, _sanitize(detail)
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


def _discover_title_prop(schema_props: dict[str, Any]) -> str | None:
    for name, meta in schema_props.items():
        if isinstance(meta, dict) and meta.get("type") == "title":
            return str(name)
    return None


def _find_prop_by_names(schema_props: dict[str, Any], names: Sequence[str]) -> str | None:
    keys = list(schema_props.keys())
    low = {str(k).lower(): str(k) for k in keys}

    for n in names:
        k = low.get(str(n).lower())
        if k:
            return k

    for n in names:
        nlow = str(n).lower()
        for k in keys:
            if nlow in str(k).lower():
                return str(k)

    return None


def _discover_prop_map(schema_props: dict[str, Any], fallback: PropMap) -> PropMap:
    title_schema = _discover_title_prop(schema_props)
    title = fallback.title

    # ruff SIM102 fix: single combined conditional
    if (
        title not in schema_props or (schema_props.get(title, {}).get("type") != "title")
    ) and title_schema:
        title = title_schema

    def pick(existing: str, names: Sequence[str]) -> str:
        if existing in schema_props:
            return existing
        found = _find_prop_by_names(schema_props, names)
        return found or existing

    return PropMap(
        title=title,
        mint=pick(fallback.mint, ["Mint", "Token", "Token Mint", "Address", "Mint Address"]),
        balance=pick(fallback.balance, ["Balance", "Qty", "Amount", "Tokens"]),
        entry=pick(fallback.entry, ["Entry", "Entry Price", "Buy Price", "Avg Entry"]),
        current=pick(fallback.current, ["Current", "Current Price", "Price", "Mark"]),
        profit=pick(fallback.profit, ["Profit", "PnL", "P&L", "Realized", "Unrealized"]),
        reason=pick(fallback.reason, ["Reason", "Notes", "Why", "Tag"]),
        timestamp=pick(fallback.timestamp, ["Timestamp", "Time", "TS", "Date"]),
    )


# =============================================================================
# payload builders
# =============================================================================
def _as_rich_text(s: str) -> dict[str, Any]:
    return {"rich_text": [{"text": {"content": s}}]}


def _as_title(s: str) -> dict[str, Any]:
    return {"title": [{"text": {"content": s}}]}


def _as_number(x: float) -> dict[str, Any]:
    return {"number": float(x)}


def _as_date_iso(iso: str) -> dict[str, Any]:
    return {"date": {"start": iso}}


def _as_select(name: str) -> dict[str, Any]:
    return {"select": {"name": name}}


def _as_multi_select(names: list[str]) -> dict[str, Any]:
    return {"multi_select": [{"name": n} for n in names if n]}


def _build_prop_value(prop_type: str, value: Any) -> dict[str, Any] | None:
    t = (prop_type or "").strip().lower()

    if t == "title":
        return _as_title(str(value))
    if t == "rich_text":
        return _as_rich_text(str(value))
    if t == "number":
        try:
            return _as_number(float(value))
        except Exception:
            return None
    if t == "date":
        return _as_date_iso(str(value))
    if t == "checkbox":
        return {"checkbox": bool(value)}
    if t == "url":
        return {"url": str(value)}
    if t == "email":
        return {"email": str(value)}
    if t == "phone_number":
        return {"phone_number": str(value)}
    if t == "select":
        return _as_select(str(value))
    if t == "multi_select":
        if isinstance(value, list | tuple):
            return _as_multi_select([str(x) for x in value])
        return _as_multi_select([str(value)])
    if t == "status":
        return {"status": {"name": str(value)}}

    return None


def _build_properties(
    schema_props: dict[str, Any],
    pm: PropMap,
    sample: dict[str, Any],
    *,
    quiet: bool,
) -> dict[str, Any]:
    out: dict[str, Any] = {}

    def put(field_key: str, prop_name: str, value: Any) -> None:
        meta = schema_props.get(prop_name)
        if not isinstance(meta, dict):
            _hlog(f"(skip) missing column in DB schema: {prop_name}", quiet=quiet)
            return

        ptype = str(meta.get("type") or "")
        payload = _build_prop_value(ptype, value)
        if payload is None:
            _hlog(
                f"(skip) incompatible type for '{prop_name}' type={ptype} field={field_key}",
                quiet=quiet,
            )
            return

        out[prop_name] = payload

    put("title", pm.title, sample["title"])
    put("mint", pm.mint, sample["mint"])
    put("balance", pm.balance, sample["balance"])
    put("entry", pm.entry, sample["entry"])
    put("current", pm.current, sample["current"])
    put("profit", pm.profit, sample["profit"])
    put("reason", pm.reason, sample["reason"])
    put("timestamp", pm.timestamp, sample["timestamp"])

    return out


# =============================================================================
# CLI + main
# =============================================================================
def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Create exactly one test page in a Notion database (schema-aware)."
    )
    p.add_argument(
        "--db", help="Override database id (else env NOTION_DB_TRADES/NOTION_DATABASE_ID)."
    )

    p.add_argument("--title", default="", help="Title for the test page (default auto-generated).")
    p.add_argument(
        "--mint",
        default="So11111111111111111111111111111111111111112",
        help="Mint/token string.",
    )
    p.add_argument("--balance", type=float, default=0.0, help="Balance number.")
    p.add_argument("--entry", type=float, default=0.0, help="Entry price number.")
    p.add_argument("--current", type=float, default=0.0, help="Current price number.")
    p.add_argument("--profit", type=float, default=0.0, help="Profit number.")
    p.add_argument("--reason", default="notion_smoke", help="Reason/notes text.")

    p.add_argument(
        "--discover",
        action="store_true",
        help="Print discovered schema + mapping and exit (no write).",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Build payload and print it, but do not create a page.",
    )
    p.add_argument(
        "--json", action="store_true", help="Emit JSON result (mapping + created page id/url)."
    )
    p.add_argument("--quiet", action="store_true", help="Suppress human logs.")

    p.add_argument(
        "--timeout", type=float, default=float(_envs("NOTION_HTTP_TIMEOUT_S", "20") or "20")
    )
    p.add_argument(
        "--retries", type=int, default=int(float(_envs("NOTION_HTTP_RETRIES", "2") or "2"))
    )

    return p.parse_args()


def main() -> None:
    _need_requests_or_exit()
    args = parse_args()

    token = _pick_token()
    db_id = _pick_db_id(args.db)
    notion_version = _envs("NOTION_API_VERSION", DEFAULT_NOTION_VERSION) or DEFAULT_NOTION_VERSION

    if not token:
        _safe_print("❌ Missing NOTION_API_KEY (or NOTION_TOKEN).", file=sys.stderr)
        raise SystemExit(2)
    if not db_id:
        _safe_print(
            "❌ Missing DB id. Set NOTION_DB_TRADES (or NOTION_DATABASE_ID) or pass --db.",
            file=sys.stderr,
        )
        raise SystemExit(2)

    quiet = bool(args.quiet)
    timeout_s = max(5.0, float(args.timeout))
    retries = max(0, int(args.retries))

    title = args.title.strip()
    if not title:
        title = f"SMOKE-{_ts()}-{_rand_suffix(6)}"

    sample = {
        "title": title,
        "mint": str(args.mint).strip(),
        "balance": float(args.balance),
        "entry": float(args.entry),
        "current": float(args.current),
        "profit": float(args.profit),
        "reason": str(args.reason).strip(),
        "timestamp": _iso_utc_now(),
    }

    hdrs = _headers(token, notion_version)

    with requests.Session() as sess:
        url_db = f"{NOTION_API_BASE}/databases/{db_id}"
        sc, db_json, err = _http_get_json(
            sess, url_db, hdrs, timeout_s=timeout_s, retries=retries, quiet=quiet
        )
        if sc != 200:
            _safe_print(f"❌ Notion DB schema fetch failed HTTP {sc}: {err}", file=sys.stderr)
            raise SystemExit(1)

        schema_props = (db_json or {}).get("properties") or {}
        if not isinstance(schema_props, dict) or not schema_props:
            _safe_print("❌ Notion DB schema malformed: no properties.", file=sys.stderr)
            raise SystemExit(3)

        pm_env = _apply_json_overrides(_env_prop_map())
        pm = _discover_prop_map(schema_props, pm_env)

        if args.discover:
            out = {
                "ts": _ts(),
                "db_id": db_id,
                "notion_version": notion_version,
                "props_env": pm_env.__dict__,
                "props_discovered": pm.__dict__,
                "schema_types": {
                    k: (schema_props.get(k, {}) or {}).get("type") for k in schema_props
                },
            }
            print(json.dumps(out, indent=2, ensure_ascii=False, default=str))
            return

        props_payload = _build_properties(schema_props, pm, sample, quiet=quiet)
        req_payload = {"parent": {"database_id": db_id}, "properties": props_payload}

        if args.dry_run:
            out = {
                "ts": _ts(),
                "db_id": db_id,
                "notion_version": notion_version,
                "mapping": pm.__dict__,
                "sample": sample,
                "payload": req_payload,
                "note": "dry-run: no page was created",
            }
            print(json.dumps(out, indent=2, ensure_ascii=False, default=str))
            return

        url_pages = f"{NOTION_API_BASE}/pages"
        sc2, resp_json, err2 = _http_post_json(
            sess, url_pages, hdrs, req_payload, timeout_s=timeout_s, retries=retries, quiet=quiet
        )
        if sc2 not in (200, 201):
            _safe_print(f"❌ Notion create page failed HTTP {sc2}: {err2}", file=sys.stderr)
            raise SystemExit(1)

        page_id = (resp_json or {}).get("id")
        page_url = (resp_json or {}).get("url")

        result = {
            "ts": _ts(),
            "ok": True,
            "db_id": db_id,
            "page_id": page_id,
            "url": page_url,
            "mapping": pm.__dict__,
            "written_props": list(props_payload.keys()),
        }

        if args.json:
            print(json.dumps(result, indent=2, ensure_ascii=False, default=str))
        else:
            _hlog("✅ Notion smoke OK: created page", quiet=False)
            print(f"page_id={page_id}")
            if page_url:
                print(f"url={page_url}")
            print("written_props=" + ",".join(result["written_props"]))

        raise SystemExit(0)


if __name__ == "__main__":
    main()
