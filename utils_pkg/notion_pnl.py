# utils_pkg/notion_pnl.py
from __future__ import annotations

"""
utils_pkg/notion_pnl.py

Daily PnL → Notion (God-Tier / production-grade)
================================================

Role
----
Upsert ONE row per UTC day into your Notion PnL database (by Date property).
This is the canonical writer for daily performance snapshots (realized/unrealized,
wallet balance, trades, open positions, etc).

Key requirements
----------------
- Best-effort (never crashes trading loop / report job)
- Idempotent upsert by date (find existing row for YYYY-MM-DD and update it)
- Schema discovery + safe property mapping
- Works even if your Notion DB uses different column names

How it works
------------
- Reads NOTION_TOKEN/NOTION_API_KEY and NOTION_PNL_DB_ID (plus legacy fallbacks).
- Discovers schema using utils_pkg.notion_client.Notion (sync).
- Resolves properties:
    1) explicit env mapping (NOTION_PNL_PROP_* or legacy NOTION_PROP_*), else
    2) auto-match common property names, else
    3) skip unknown properties (no 400s for missing columns)

Public API
----------
- push_daily_pnl(snapshot) -> Optional[str]   (sync, for scripts/daily_report)
- build_pnl_snapshot(...) -> Dict[str, Any]   (helper)

Env
---
Enable:
  NOTION_PNL_ENABLED=1            (preferred)
  NOTION_ENABLED=1                (legacy; used if NOTION_PNL_ENABLED not set)

Auth/DB:
  NOTION_TOKEN or NOTION_API_KEY
  NOTION_PNL_DB_ID                (preferred)
  NOTION_DB_PNL / NOTION_PNL_DB   (legacy)
  NOTION_DATABASE_ID              (last-resort legacy fallback)

Client:
  NOTION_TIMEOUT_SEC=20
  NOTION_TITLE_PROP               (optional override)
  NOTION_DATE_PROP                (optional override)
  NOTION_PNL_TITLE_PROP           (optional override; takes priority)
  NOTION_PNL_DATE_PROP            (optional override; takes priority)

Behavior:
  NOTION_PNL_ALLOW_DRYRUN=1        (default true)
  NOTION_PNL_STRICT=0              (default false; if true, raises on failures)

Optional property mapping (leave blank to skip):
  NOTION_PNL_PROP_REALIZED_SOL
  NOTION_PNL_PROP_UNREALIZED_SOL
  NOTION_PNL_PROP_NET_SOL
  NOTION_PNL_PROP_WALLET_SOL
  NOTION_PNL_PROP_OPEN_POSITIONS
  NOTION_PNL_PROP_TRADES
  NOTION_PNL_PROP_SOL_PRICE_USD
  NOTION_PNL_PROP_REALIZED_USD
  NOTION_PNL_PROP_UNREALIZED_USD
  NOTION_PNL_PROP_NET_USD
  NOTION_PNL_PROP_OUTCOME
  NOTION_PNL_PROP_NOTES

Compatibility:
- Also honors your older naming pattern (NOTION_PROP_*).
"""

import os
from collections.abc import Sequence
from datetime import UTC, datetime
from typing import Any

# safe logger fallback
try:  # pragma: no cover
    from logging_setup import get_logger  # type: ignore
except Exception:  # pragma: no cover
    import logging

    def get_logger(name: str | None = None):  # type: ignore
        return logging.getLogger(name or "notion_pnl")


log = get_logger(__name__)

from collections.abc import Mapping

from .notion_client import DbSchema, Notion, NotionError

# ──────────────────────────────────────────────────────────────────────────────
# Env helpers
# ──────────────────────────────────────────────────────────────────────────────


def _b(name: str, default: bool = False) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return str(v).strip().lower() in ("1", "true", "t", "yes", "y", "on")


def _s(name: str, default: str = "") -> str:
    v = os.getenv(name)
    return default if v is None else str(v).strip()


def _f(name: str, default: float) -> float:
    v = os.getenv(name)
    if v is None or str(v).strip() == "":
        return default
    try:
        return float(str(v).strip())
    except Exception:
        return default


def _dryrun_on() -> bool:
    return _b("DRYRUN", False) or _b("DRY_RUN", False) or _b("IS_DRYRUN", False)


def _clip(s: str, n: int) -> str:
    s = (s or "").strip()
    return s if len(s) <= n else s[: max(0, n - 1)] + "…"


def _num(x: Any) -> float | None:
    try:
        v = float(x)
        return 0.0 if abs(v) < 1e-15 else v
    except Exception:
        return None


def _int(x: Any) -> int | None:
    try:
        return int(float(x))
    except Exception:
        return None


def _today_utc_date() -> str:
    return datetime.now(UTC).date().isoformat()


def _resolve_enabled() -> bool:
    # preferred toggle
    if os.getenv("NOTION_PNL_ENABLED") is not None:
        return _b("NOTION_PNL_ENABLED", False)
    # legacy
    return _b("NOTION_ENABLED", False)


def _get_token() -> str:
    return _s("NOTION_TOKEN") or _s("NOTION_API_KEY")


def _get_pnl_dbid() -> str:
    return (
        _s("NOTION_PNL_DB_ID")
        or _s("NOTION_DB_PNL")
        or _s("NOTION_PNL_DB")
        or _s("NOTION_DATABASE_ID")
    )


# ──────────────────────────────────────────────────────────────────────────────
# Property resolution
# ──────────────────────────────────────────────────────────────────────────────


def _env_prop(*keys: str) -> str:
    """
    Return the first non-empty env var value among keys, stripped.
    """
    for k in keys:
        v = _s(k, "").strip()
        if v:
            return v
    return ""


def _pick_prop(schema: DbSchema, candidates: Sequence[str]) -> str:
    """
    Find first schema property name matching any candidate (case-insensitive).
    """
    lower_map = {k.lower(): k for k in schema.has_prop}
    for c in candidates:
        key = (c or "").strip().lower()
        if not key:
            continue
        if key in lower_map:
            return lower_map[key]
    return ""


def _prop_ok(schema: DbSchema, name: str) -> bool:
    n = (name or "").strip()
    if not n:
        return False
    if not schema.has_prop.get(n, False):
        return False
    return (schema.prop_types.get(n) or "") != "formula"


def _resolve_props(schema: DbSchema) -> dict[str, str]:
    """
    Resolve DB property names for each semantic field.
    Priority:
      1) NOTION_PNL_PROP_* env
      2) legacy NOTION_PROP_* env (if user reused them)
      3) auto-match common names
    """
    props: dict[str, str] = {}

    # Notes/title-ish
    props["notes"] = _env_prop(
        "NOTION_PNL_PROP_NOTES", "NOTION_PROP_NOTES", "NOTION_PROP_REASON"
    ) or _pick_prop(schema, ["Notes", "Note", "Comment", "Memo"])

    # Numbers (SOL)
    props["realized_sol"] = _env_prop(
        "NOTION_PNL_PROP_REALIZED_SOL", "NOTION_PROP_REALIZED_SOL"
    ) or _pick_prop(schema, ["Realized SOL", "Realized", "RealizedSol", "PnL Realized SOL"])
    props["unrealized_sol"] = _env_prop(
        "NOTION_PNL_PROP_UNREALIZED_SOL", "NOTION_PROP_UNREALIZED_SOL"
    ) or _pick_prop(schema, ["Unrealized SOL", "Unrealized", "UnrealizedSol", "PnL Unrealized SOL"])
    props["net_sol"] = _env_prop("NOTION_PNL_PROP_NET_SOL", "NOTION_PROP_NET_SOL") or _pick_prop(
        schema, ["Net SOL", "NetSol", "Total SOL", "PnL SOL"]
    )
    props["wallet_sol"] = _env_prop(
        "NOTION_PNL_PROP_WALLET_SOL", "NOTION_PROP_WALLET_SOL"
    ) or _pick_prop(schema, ["Wallet SOL", "Wallet", "Balance SOL", "SOL Balance"])

    # Counts
    props["open_positions"] = _env_prop(
        "NOTION_PNL_PROP_OPEN_POSITIONS", "NOTION_PROP_OPEN_POSITIONS"
    ) or _pick_prop(schema, ["Open Positions", "Positions", "Open", "OpenPos"])
    props["trades"] = _env_prop("NOTION_PNL_PROP_TRADES", "NOTION_PROP_TRADES") or _pick_prop(
        schema, ["Trades", "Trade Count", "Count", "Fills"]
    )

    # USD + price
    props["sol_price_usd"] = _env_prop(
        "NOTION_PNL_PROP_SOL_PRICE_USD", "NOTION_PROP_SOL_PRICE_USD"
    ) or _pick_prop(schema, ["SOL Price USD", "SOL USD", "Price USD", "SOL/USD"])
    props["realized_usd"] = _env_prop(
        "NOTION_PNL_PROP_REALIZED_USD", "NOTION_PROP_REALIZED_USD"
    ) or _pick_prop(schema, ["Realized USD", "Realized$", "Realized (USD)"])
    props["unrealized_usd"] = _env_prop(
        "NOTION_PNL_PROP_UNREALIZED_USD", "NOTION_PROP_UNREALIZED_USD"
    ) or _pick_prop(schema, ["Unrealized USD", "Unrealized$", "Unrealized (USD)"])
    props["net_usd"] = _env_prop("NOTION_PNL_PROP_NET_USD", "NOTION_PROP_NET_USD") or _pick_prop(
        schema, ["Net USD", "Net$", "Total USD", "PnL USD"]
    )

    # Outcome label
    props["outcome"] = _env_prop(
        "NOTION_PNL_PROP_OUTCOME", "NOTION_PROP_OUTCOME", "NOTION_PROP_DAY_OUTCOME"
    ) or _pick_prop(schema, ["Day Outcome", "Outcome", "Result", "Day Result"])

    # Remove invalid / formula / missing
    for k, v in list(props.items()):
        if v and not _prop_ok(schema, v):
            props[k] = ""

    # Never collide with title/date props (handled via upsert_page core logic)
    for k, v in list(props.items()):
        if v in (schema.title_prop, schema.date_prop):
            props[k] = ""

    return props


# ──────────────────────────────────────────────────────────────────────────────
# Snapshot builder
# ──────────────────────────────────────────────────────────────────────────────


def build_pnl_snapshot(
    *,
    date_utc: str | None = None,
    realized_sol: float = 0.0,
    unrealized_sol: float = 0.0,
    wallet_sol: float | None = None,
    open_positions: int = 0,
    trades: int = 0,
    sol_price_usd: float | None = None,
    note: str = "",
) -> dict[str, Any]:
    """
    Helper to construct a snapshot dict with canonical keys.
    """
    iso_date = (date_utc or "").strip() or _today_utc_date()
    net_sol = float(realized_sol or 0.0) + float(unrealized_sol or 0.0)

    realized_usd = None
    unrealized_usd = None
    net_usd = None
    if sol_price_usd is not None:
        realized_usd = float(realized_sol or 0.0) * float(sol_price_usd)
        unrealized_usd = float(unrealized_sol or 0.0) * float(sol_price_usd)
        net_usd = float(net_sol) * float(sol_price_usd)

    return {
        "date": iso_date,
        "realized_sol": float(realized_sol or 0.0),
        "unrealized_sol": float(unrealized_sol or 0.0),
        "net_sol": float(net_sol),
        "wallet_sol": float(wallet_sol) if wallet_sol is not None else None,
        "open_positions": int(open_positions or 0),
        "trades": int(trades or 0),
        "sol_price_usd": float(sol_price_usd) if sol_price_usd is not None else None,
        "realized_usd": realized_usd,
        "unrealized_usd": unrealized_usd,
        "net_usd": net_usd,
        "note": (note or "").strip(),
    }


def _infer_iso_date(snapshot: Mapping[str, Any]) -> str:
    iso = (
        str(snapshot.get("date") or "")
        or str(snapshot.get("date_utc") or "")
        or str(snapshot.get("day") or "")
        or str(snapshot.get("time") or "")[:10]
        or str(snapshot.get("ts") or "")[:10]
    ).strip()
    if len(iso) >= 10:
        iso = iso[:10]
    return iso if iso else _today_utc_date()


def _outcome_label(net_sol: float) -> str:
    if abs(net_sol) < 1e-9:
        return "Flat"
    return "Win" if net_sol > 0 else "Loss"


# ──────────────────────────────────────────────────────────────────────────────
# Main API (sync)
# ──────────────────────────────────────────────────────────────────────────────


def push_daily_pnl(snapshot: dict[str, Any]) -> str | None:
    """
    Upsert one Notion row for the day.

    Returns:
      page_id (str) on success,
      None if disabled/missing config/best-effort failure.
    """
    strict = _b("NOTION_PNL_STRICT", False)

    if not _resolve_enabled():
        return None

    token = _get_token()
    dbid = _get_pnl_dbid()
    if not token or not dbid:
        return None

    allow_dryrun = _b("NOTION_PNL_ALLOW_DRYRUN", True)
    if _dryrun_on() and not allow_dryrun:
        return None

    timeout = _f("NOTION_TIMEOUT_SEC", 20.0)
    notion = Notion(token, timeout=timeout)

    try:
        schema = notion.discover_schema(dbid)

        # Resolve ISO date (YYYY-MM-DD)
        iso_date = _infer_iso_date(snapshot)

        # Pull canonical values (be flexible about input keys)
        realized_sol = _num(snapshot.get("realized_sol", snapshot.get("realized", 0.0))) or 0.0
        unrealized_sol = (
            _num(snapshot.get("unrealized_sol", snapshot.get("unrealized", 0.0))) or 0.0
        )
        net_sol = _num(snapshot.get("net_sol"))  # allow caller override
        if net_sol is None:
            net_sol = float(realized_sol) + float(unrealized_sol)

        wallet_sol = _num(snapshot.get("wallet_sol", snapshot.get("wallet")))
        open_positions = _int(snapshot.get("open_positions", snapshot.get("positions", 0))) or 0
        trades = _int(snapshot.get("trades", snapshot.get("fills", 0))) or 0

        sol_price_usd = _num(snapshot.get("sol_price_usd", snapshot.get("price_usd")))

        realized_usd = _num(snapshot.get("realized_usd"))
        unrealized_usd = _num(snapshot.get("unrealized_usd"))
        net_usd = _num(snapshot.get("net_usd"))

        # If price provided but USD not, derive
        if sol_price_usd is not None:
            if realized_usd is None:
                realized_usd = float(realized_sol) * float(sol_price_usd)
            if unrealized_usd is None:
                unrealized_usd = float(unrealized_sol) * float(sol_price_usd)
            if net_usd is None:
                net_usd = float(net_sol) * float(sol_price_usd)

        note = str(snapshot.get("note") or snapshot.get("notes") or "").strip()
        outcome = str(snapshot.get("outcome") or "").strip() or _outcome_label(float(net_sol))

        # Property mapping
        props = _resolve_props(schema)

        # Build values dict keyed by EXACT Notion property names we intend to write
        values: dict[str, Any] = {}

        # Always provide "Notes" fallback for title text inside notion_client upsert_page
        # (it will copy/resolve to schema.title_prop internally)
        title_text = note or f"Daily PnL {iso_date}"
        values["Notes"] = title_text
        # Also set actual title prop explicitly (helps if notion_client prefers it)
        values[schema.title_prop] = title_text

        # For date, notion_client will always set schema.date_prop from iso_date
        values[schema.date_prop] = iso_date

        # Numbers
        if props["realized_sol"] and realized_sol is not None:
            values[props["realized_sol"]] = float(realized_sol)
        if props["unrealized_sol"] and unrealized_sol is not None:
            values[props["unrealized_sol"]] = float(unrealized_sol)
        if props["net_sol"] and net_sol is not None:
            values[props["net_sol"]] = float(net_sol)
        if props["wallet_sol"] and wallet_sol is not None:
            values[props["wallet_sol"]] = float(wallet_sol)

        # Counts
        if props["open_positions"]:
            values[props["open_positions"]] = int(open_positions)
        if props["trades"]:
            values[props["trades"]] = int(trades)

        # USD + price
        if props["sol_price_usd"] and sol_price_usd is not None:
            values[props["sol_price_usd"]] = float(sol_price_usd)
        if props["realized_usd"] and realized_usd is not None:
            values[props["realized_usd"]] = float(realized_usd)
        if props["unrealized_usd"] and unrealized_usd is not None:
            values[props["unrealized_usd"]] = float(unrealized_usd)
        if props["net_usd"] and net_usd is not None:
            values[props["net_usd"]] = float(net_usd)

        # Notes (if you have a separate Notes prop distinct from title)
        if props["notes"] and note:
            values[props["notes"]] = note

        # Outcome (select/rich_text supported; formulas are auto-skipped by notion_client)
        if props["outcome"] and outcome:
            values[props["outcome"]] = outcome

        page_id = notion.upsert_page(dbid, schema, iso_date, values)

        log.info(
            "NOTION_PNL_UPSERT_OK",
            extra={
                "date": iso_date,
                "page_id": page_id,
                "realized_sol": float(realized_sol),
                "unrealized_sol": float(unrealized_sol),
                "net_sol": float(net_sol),
                "trades": int(trades),
            },
        )
        return page_id

    except Exception as e:
        msg = str(e)
        if isinstance(e, NotionError):
            msg = msg
        log.warning("NOTION_PNL_UPSERT_FAIL", extra={"err": _clip(msg, 600)})
        if strict:
            raise
        return None


__all__ = ["build_pnl_snapshot", "push_daily_pnl"]
