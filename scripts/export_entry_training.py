from __future__ import annotations

"""
scripts/export_entry_training.py

Quant-grade export for ENTRY training dataset (V3+++).
====================================================

Exports entry_candidates (+ optional outcomes via LEFT JOIN) into JSONL suitable for:

  1) Chat fine-tuning (default):
     {"messages":[...], "metadata":{...}}

  2) Simple supervised format (--format simple):
     {"input": {...}, "output": {...}, "metadata": {...}}

Critical constraint
-------------------
- The USER prompt must include ONLY entry-time fields + features.
- Outcome fields are used ONLY to derive labels + metadata.

Key production upgrades (LOCK-IN)
---------------------------------
- psycopg v3 sync (Windows/Render friendly)
- DSN resolution + normalization + sslmode default + redaction
- Retry connect + try all resolved IPs (hostaddr pinning)
- Schema introspection for drift tolerance
- LEFT JOIN outcomes by default to include NO_OUTCOME negatives
- Labeling: outcome.label text → verdict first (big_win => BUY), numeric fallback
- Optional negative downsampling (--neg-ratio)
- Atomic JSONL writes
- Stratified train/val split (keeps BUY/PASS distribution stable)
- --connect-only / --dry-run diagnostics

Examples
--------
  # Connect test
  py -m scripts.export_entry_training --connect-only

  # Dry run (prints chosen columns + SQL + params):
  py -m scripts.export_entry_training --dry-run

  # Export last 365 days including NO_OUTCOME negatives (default), no downsample:
  py -m scripts.export_entry_training --days-back 365 --limit 50000 --out data/entry_train.jsonl

  # Export and downsample negatives to 3x positives:
  py -m scripts.export_entry_training --days-back 365 --limit 50000 --neg-ratio 3 --out data/entry_train.jsonl

  # Train/val split:
  py -m scripts.export_entry_training --days-back 365 --limit 50000 `
    --out data/entry_train.jsonl --val-out data/entry_val.jsonl --val-fraction 0.15

"""

import argparse
import contextlib
import datetime as dt
import json
import os
import random
import re
import socket
import sys
import tempfile
import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

# psycopg v3 (required)
try:
    import psycopg
    from psycopg.rows import dict_row
except Exception as err:  # pragma: no cover
    # B904: preserve the original import error chain
    raise SystemExit(
        "psycopg (v3) is required for this exporter.\n"
        "Install:\n"
        "  pip install psycopg[binary]\n"
        f"Import error: {err!r}"
    ) from err


# ---------------------------------------------------------------------------
# Repo bootstrap (root + src/)
# ---------------------------------------------------------------------------


def _bootstrap_repo_root() -> str:
    try:
        here = Path(__file__).resolve()
    except Exception:
        return ""

    markers = ("pyproject.toml", "requirements.txt", "setup.cfg", ".git")
    root: Path | None = None
    for p in [here.parent, *list(here.parents)]:
        try:
            if any((p / m).exists() for m in markers):
                root = p
                break
            if (p / "src").is_dir() and any(
                (p / "src" / x).exists() for x in ("utils_pkg", "scanner", "scripts")
            ):
                root = p
                break
        except Exception:
            continue

    if root is None:
        root = here.parents[1] if len(here.parents) > 1 else here.parent

    root_str = str(root)
    if root_str and root_str not in sys.path:
        sys.path.insert(0, root_str)

    src = root / "src"
    if src.is_dir():
        src_str = str(src)
        if src_str not in sys.path:
            sys.path.insert(0, src_str)

    os.environ.setdefault("REPO_ROOT", root_str)
    return root_str


_REPO_ROOT = _bootstrap_repo_root()


# ---------------------------------------------------------------------------
# Logging (use logging_setup if available)
# ---------------------------------------------------------------------------


def _get_logger():
    try:
        from logging_setup import get_logger  # type: ignore

        try:
            return get_logger(__name__)
        except TypeError:
            return get_logger()
    except Exception:
        import logging

        logging.basicConfig(
            level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
        )
        return logging.getLogger("export_entry_training")


log = _get_logger()


# ---------------------------------------------------------------------------
# Env helpers
# ---------------------------------------------------------------------------

_TRUE = {"1", "true", "yes", "y", "on", "t"}
_FALSE = {"0", "false", "no", "n", "off", "f"}


def _env_str(key: str, default: str = "") -> str:
    return str(os.getenv(key, default) or "").strip()


def _env_int(key: str, default: int) -> int:
    raw = os.getenv(key)
    if raw is None or str(raw).strip() == "":
        return default
    try:
        return int(str(raw).strip())
    except Exception:
        return default


def _env_float(key: str, default: float) -> float:
    raw = os.getenv(key)
    if raw is None or str(raw).strip() == "":
        return default
    try:
        return float(str(raw).strip())
    except Exception:
        return default


def _env_bool(key: str, default: bool = False) -> bool:
    raw = os.getenv(key)
    if raw is None or str(raw).strip() == "":
        return default
    return str(raw).strip().lower() in _TRUE


def _parse_bool_cli(v: Any) -> bool:
    if isinstance(v, bool):
        return v
    if v is None:
        return True
    s = str(v).strip().lower()
    if s in _TRUE:
        return True
    return s not in _FALSE


def _add_bool_flag(p: argparse.ArgumentParser, name: str, default: bool, help_text: str) -> None:
    dest = name.replace("-", "_")
    p.add_argument(
        f"--{name}",
        dest=dest,
        nargs="?",
        const=True,
        default=default,
        type=_parse_bool_cli,
        help=f"{help_text} (use --{name} false or --no-{name})",
    )
    p.add_argument(f"--no-{name}", dest=dest, action="store_false", help=argparse.SUPPRESS)


# ---------------------------------------------------------------------------
# DSN selection + normalization + redaction
# ---------------------------------------------------------------------------

_DSN_KEYS = (
    "POSTGRES_DSN",
    "PG_DSN",
    "SNIPER_DB_DSN",
    "DATABASE_URL",
    "PNL_DSN",
    "PNL_DB_DSN",
    "PG_DSN_INTERNAL",
    "PG_DSN_EXTERNAL",
)


def _normalize_dsn(raw: str) -> str:
    raw = (raw or "").strip()
    if not raw:
        return raw
    if raw.startswith("postgres://"):
        raw = "postgresql://" + raw[len("postgres://") :]
    if raw.startswith("postgresql+asyncpg://"):
        raw = "postgresql://" + raw[len("postgresql+asyncpg://") :]
    return raw


def _redact_dsn(dsn: str) -> str:
    try:
        u = urlsplit(dsn)
        if not u.username:
            return dsn
        netloc = u.hostname or ""
        if u.port:
            netloc = f"{netloc}:{u.port}"
        netloc = f"{u.username}:***@{netloc}"
        return urlunsplit((u.scheme, netloc, u.path, u.query, u.fragment))
    except Exception:
        return "postgresql://***"


def _set_query_param(dsn: str, key: str, value: str) -> str:
    u = urlsplit(dsn)
    q = dict(parse_qsl(u.query, keep_blank_values=True))
    q[key] = value
    new_q = urlencode(q)
    return urlunsplit((u.scheme, u.netloc, u.path, new_q, u.fragment))


def _ensure_sslmode(dsn: str, sslmode_default: str = "require") -> str:
    u = urlsplit(dsn)
    q = dict(parse_qsl(u.query, keep_blank_values=True))
    if "sslmode" not in q and sslmode_default:
        q["sslmode"] = sslmode_default
        return urlunsplit((u.scheme, u.netloc, u.path, urlencode(q), u.fragment))
    return dsn


def pick_dsn(cli_dsn: str = "") -> str:
    if cli_dsn and str(cli_dsn).strip():
        return _ensure_sslmode(_normalize_dsn(cli_dsn))
    for k in _DSN_KEYS:
        v = os.getenv(k)
        if v and str(v).strip():
            return _ensure_sslmode(_normalize_dsn(str(v)))
    raise SystemExit(
        "No DSN found. Set one of: " + ", ".join(_DSN_KEYS) + "\n"
        "PowerShell example:\n"
        "  $env:PG_DSN='postgresql://user:pass@host:5432/db?sslmode=require'"
    )


# ---------------------------------------------------------------------------
# Table ref parsing + safe quoting
# ---------------------------------------------------------------------------

_VALID_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


@dataclass(frozen=True)
class TableRef:
    schema: str
    name: str

    @staticmethod
    def parse(ref: str, default_schema: str = "public") -> "TableRef":
        ref = (ref or "").strip()
        if not ref:
            raise ValueError("empty table ref")
        if "." in ref:
            sch, tbl = ref.split(".", 1)
            return TableRef(sch.strip().strip('"') or default_schema, tbl.strip().strip('"'))
        return TableRef(default_schema, ref.strip().strip('"'))

    def fq(self) -> str:
        return f"{qident(self.schema)}.{qident(self.name)}"


def qident(name: str) -> str:
    name = (name or "").strip().strip('"')
    if not _VALID_IDENT.match(name):
        raise SystemExit(f"Invalid SQL identifier: {name!r}")
    return f'"{name}"'


# ---------------------------------------------------------------------------
# Introspection + heuristics
# ---------------------------------------------------------------------------


def _pick_first(existing: Sequence[str], candidates: Sequence[str]) -> str | None:
    s = set(existing)
    for c in candidates:
        if c in s:
            return c
    return None


def _detect_candidate_ts_col(cols: Sequence[str]) -> str | None:
    # Prefer created_at first (your table has it), then ts.
    return _pick_first(cols, ("created_at", "ts", "inserted_at", "time", "timestamp"))


def _detect_features_col(cols: Sequence[str]) -> str | None:
    pri = (
        "features",
        "features_json",
        "features_v3",
        "features_v2",
        "features_v1",
        "raw_features",
        "feats",
        "feats_json",
    )
    hit = _pick_first(cols, pri)
    if hit:
        return hit
    for c in cols:
        lc = c.lower()
        if "feature" in lc or lc.startswith("feat"):
            return c
    return None


def _canonical_outcome_map(cols: Sequence[str]) -> dict[str, str]:
    """
    Returns canonical -> actual mappings for common outcome fields.
    """
    s = set(cols)
    out: dict[str, str] = {}

    # label text (IMPORTANT for your current dataset)
    for c in ("label", "outcome_label", "ai_label"):
        if c in s:
            out["label"] = c
            break

    # time fields
    for c in ("created_at", "decided_at", "ts"):
        if c in s:
            out["outcome_ts"] = c
            break

    # pnl/runup/dd/hold (fallback)
    for c in ("realized_pnl_pct", "pnl_pct", "net_pnl_pct", "pnl_total_pct"):
        if c in s:
            out["pnl_pct"] = c
            break
    for c in ("max_runup_pct", "runup_pct", "max_run_up_pct"):
        if c in s:
            out["runup_pct"] = c
            break
    for c in ("max_drawdown_pct", "drawdown_pct", "max_draw_down_pct"):
        if c in s:
            out["drawdown_pct"] = c
            break
    for c in ("hold_sec", "holding_seconds", "hold_seconds"):
        if c in s:
            out["hold_sec"] = c
            break
    for c in ("holding_minutes", "hold_min", "hold_minutes"):
        if c in s:
            out["hold_min"] = c
            break

    return out


def _fetch_columns(conn: psycopg.Connection, t: TableRef) -> list[str]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema=%s AND table_name=%s
            ORDER BY ordinal_position
            """,
            (t.schema, t.name),
        )
        return [r[0] for r in cur.fetchall()]


# ---------------------------------------------------------------------------
# Connection helper (try all IPs + retries)
# ---------------------------------------------------------------------------


def _resolve_ips(host: str, port: int) -> list[str]:
    ips: list[str] = []
    try:
        infos = socket.getaddrinfo(host, port, type=socket.SOCK_STREAM)
        for _, _, _, _, sockaddr in infos:
            ip = sockaddr[0]
            if ip and ip not in ips:
                ips.append(ip)
    except Exception:
        pass
    return ips


def connect_pg(
    dsn: str,
    *,
    tries: int = 6,
    base_delay: float = 0.6,
    connect_timeout: float = 20.0,
    application_name: str = "export_entry_training",
    try_all_addrs: bool = True,
) -> psycopg.Connection:
    dsn = _ensure_sslmode(_normalize_dsn(dsn))
    red = _redact_dsn(dsn)

    u = urlsplit(dsn)
    host = u.hostname or ""
    port = int(u.port or 5432)

    ips = _resolve_ips(host, port) if (try_all_addrs and host) else []
    targets: list[str | None] = ips if ips else [None]

    last: BaseException | None = None
    for addr in targets:
        for i in range(max(1, int(tries))):
            try:
                kwargs: dict[str, Any] = dict(
                    connect_timeout=float(connect_timeout),
                    application_name=application_name,
                )
                if addr:
                    kwargs["hostaddr"] = addr
                    kwargs["host"] = host
                    kwargs["port"] = port
                conn = psycopg.connect(dsn, **kwargs)
                return conn
            except Exception as e:
                last = e
                delay = base_delay * (2**i)
                log.warning(
                    "PG connect failed (%s/%s) target=%s dsn=%s err=%r",
                    i + 1,
                    tries,
                    addr or "dns",
                    red,
                    e,
                )
                if i + 1 < tries:
                    time.sleep(delay)

    raise SystemExit(
        f"Postgres connection failed after retries.\nDSN: {red}\nLast error: {last!r}\n"
    )


# ---------------------------------------------------------------------------
# Date helpers
# ---------------------------------------------------------------------------


def _parse_date(s: str) -> dt.datetime | None:
    s = (s or "").strip()
    if not s:
        return None

    # Small robustness: accept ISO strings ending in 'Z'
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"

    try:
        x = dt.datetime.fromisoformat(s) if "T" in s else dt.datetime.strptime(s, "%Y-%m-%d")
        if x.tzinfo is None:
            x = x.replace(tzinfo=dt.UTC)
        return x
    except Exception:
        return None


def _resolve_range(
    days_back: int, start_date: str, end_date: str
) -> tuple[dt.datetime | None, dt.datetime | None]:
    start_ts = _parse_date(start_date)
    end_ts = _parse_date(end_date)

    if start_ts and end_ts:
        if end_ts.time() == dt.time.min:
            end_ts = end_ts + dt.timedelta(days=1)
        return start_ts, end_ts

    if days_back > 0:
        end_ts = dt.datetime.now(dt.UTC)
        start_ts = end_ts - dt.timedelta(days=days_back)
        return start_ts, end_ts

    return None, None


# ---------------------------------------------------------------------------
# Feature formatting
# ---------------------------------------------------------------------------


def _parse_features(val: Any) -> dict[str, Any]:
    if val is None:
        return {}
    if isinstance(val, dict):
        return val
    if isinstance(val, str):
        try:
            parsed = json.loads(val)
            return parsed if isinstance(parsed, dict) else {"_raw": parsed}
        except Exception:
            return {}
    try:
        return dict(val)  # type: ignore[arg-type]
    except Exception:
        return {"_raw": val}


def _select_feature_keys(feats: dict[str, Any], keys: list[str]) -> dict[str, Any]:
    if not keys:
        return feats
    return {k: feats[k] for k in keys if k in feats}


def _truncate_json(obj: Any, max_chars: int) -> str:
    s = json.dumps(obj, ensure_ascii=False, separators=(",", ":"), default=str)
    if max_chars > 0 and len(s) > max_chars:
        return s[: max_chars - 3] + "..."
    return s


def _ts_to_str(ts: Any) -> str:
    if isinstance(ts, dt.datetime):
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=dt.UTC)
        return ts.isoformat()
    return str(ts) if ts is not None else ""


def _system_prompt(task: str) -> str:
    task = task or "entry_gate_v3"
    return (
        "You are an AI assistant for a Solana trading system.\n"
        "You will receive an ENTRY-TIME snapshot of a token candidate, including features.\n"
        "Decide whether this setup should be allowed through the entry gate.\n\n"
        'Respond with exactly ONE JSON object: {"verdict":"BUY|PASS","score":0.0-1.0}\n'
        "No extra keys. No explanation.\n\n"
        f"Task profile: {task}"
    )


# ---------------------------------------------------------------------------
# Labeling (outcome-label first, numeric fallback)
# ---------------------------------------------------------------------------

_WIN_RE = re.compile(r"(big[_\-\s]?win|win|moon|send|profit|green)", re.I)
_LOSS_RE = re.compile(r"(loss|rug|scam|dead|red|dump|rekt)", re.I)


def _to_float(x: Any, default: float = 0.0) -> float:
    try:
        if x is None:
            return default
        return float(x)
    except Exception:
        return default


def _clamp(x: float, lo: float, hi: float) -> float:
    return lo if x < lo else hi if x > hi else x


def derive_label(
    row: Mapping[str, Any],
    *,
    label_mode: str,
    min_runup_buy: float,
    min_pnl_buy: float,
    max_dd_for_buy: float,
    loss_pnl_cut: float,
    rug_dd_cut: float,
) -> dict[str, Any]:
    """
    Returns: verdict/score + canonical metrics.
    label_mode:
      - "label_first" (default): use outcome label text if present, else numeric
      - "numeric_only": ignore outcome label text, numeric only
      - "label_only": label text only; if no label/no outcome => PASS
    """
    has_outcome = bool(row.get("_has_outcome"))
    out_label = (row.get("label") or "").strip()

    # No outcome row => negative PASS
    if not has_outcome:
        return {"verdict": "PASS", "score": 0.20, "label_source": "no_outcome"}

    # Outcome exists
    if label_mode in ("label_first", "label_only"):
        if out_label:
            if _WIN_RE.search(out_label):
                score = 0.90 if re.search(r"big[_\-\s]?win", out_label, re.I) else 0.78
                return {
                    "verdict": "BUY",
                    "score": float(score),
                    "label_source": f"label:{out_label}",
                }
            if _LOSS_RE.search(out_label):
                return {"verdict": "PASS", "score": 0.12, "label_source": f"label:{out_label}"}
            # Unknown label => fall back if allowed
            if label_mode == "label_only":
                return {
                    "verdict": "PASS",
                    "score": 0.22,
                    "label_source": f"label_unknown:{out_label}",
                }
        elif label_mode == "label_only":
            return {"verdict": "PASS", "score": 0.20, "label_source": "label_only_no_label"}

    # Numeric fallback
    pnl = _to_float(row.get("pnl_pct"), 0.0)
    runup = _to_float(row.get("runup_pct"), 0.0)
    dd = _to_float(row.get("drawdown_pct"), 0.0)

    hold_min = _to_float(row.get("hold_min"), 0.0)
    if hold_min <= 0.0:
        hold_sec = _to_float(row.get("hold_sec"), 0.0)
        if hold_sec > 0.0:
            hold_min = hold_sec / 60.0

    good = False
    if runup >= min_runup_buy and dd >= max_dd_for_buy:
        good = True
    if pnl >= min_pnl_buy and dd >= (max_dd_for_buy - 5.0):
        good = True
    if pnl <= loss_pnl_cut or dd <= rug_dd_cut:
        good = False

    verdict = "BUY" if good else "PASS"
    if verdict == "BUY":
        raw = 0.65 + 0.006 * max(pnl, 0.0) + 0.004 * max(runup, 0.0) + 0.002 * dd
        score = _clamp(raw, 0.60, 0.99)
    else:
        raw = 0.25 + 0.006 * abs(min(pnl, 0.0)) + 0.004 * abs(min(dd, 0.0))
        score = _clamp(raw, 0.05, 0.45)

    return {
        "verdict": verdict,
        "score": float(score),
        "pnl_pct": float(pnl),
        "runup_pct": float(runup),
        "drawdown_pct": float(dd),
        "hold_min": float(hold_min),
        "label_source": "numeric",
    }


# ---------------------------------------------------------------------------
# Fetch joined rows (LEFT JOIN by default)
# ---------------------------------------------------------------------------


def _fetch_rows(
    conn: psycopg.Connection,
    *,
    candidates: TableRef,
    outcomes: TableRef,
    limit: int,
    start_ts: dt.datetime | None,
    end_ts: dt.datetime | None,
    src: str | None,
    regime: str | None,
    wallet_tag: str | None,
    min_feature_version: int,
    include_no_outcome: bool,
    dry_run: bool,
) -> list[dict[str, Any]]:
    c_cols = _fetch_columns(conn, candidates)
    o_cols = _fetch_columns(conn, outcomes)

    if not c_cols:
        raise SystemExit(f"No columns found for candidates table: {candidates.fq()}")
    if not o_cols:
        raise SystemExit(f"No columns found for outcomes table: {outcomes.fq()}")

    c_id = _pick_first(c_cols, ("id", "candidate_id"))
    o_cid = _pick_first(o_cols, ("candidate_id",))
    if c_id is None:
        raise SystemExit(f"{candidates.fq()} must have id/candidate_id column for join.")
    if o_cid is None:
        raise SystemExit(f"{outcomes.fq()} must have candidate_id column for join.")

    ts_col = _detect_candidate_ts_col(c_cols)
    feat_col = _detect_features_col(c_cols)
    if feat_col is None:
        raise SystemExit(f"{candidates.fq()} has no detectable features column.")

    outmap = _canonical_outcome_map(o_cols)

    select_parts: list[str] = []
    select_parts.append(f"c.{qident(c_id)} AS candidate_id")
    select_parts.append(f"c.{qident(feat_col)} AS features")

    # Candidate meta (optional)
    for col in ("src", "regime", "mint", "symbol", "sym", "wallet_tag", "feature_version"):
        if col in c_cols:
            select_parts.append(f"c.{qident(col)} AS {qident(col)}")

    if ts_col:
        select_parts.append(f"c.{qident(ts_col)} AS ts")
    else:
        select_parts.append("NULL::timestamptz AS ts")

    # Include a simple outcome existence marker
    select_parts.append(f"(o.{qident(o_cid)} IS NOT NULL) AS _has_outcome")

    # Outcome fields (aliased canonical)
    for canon, actual in outmap.items():
        select_parts.append(f"o.{qident(actual)} AS {qident(canon)}")

    conditions: list[str] = []
    params: list[Any] = []

    if ts_col and start_ts is not None:
        conditions.append(f"c.{qident(ts_col)} >= %s")
        params.append(start_ts)
    if ts_col and end_ts is not None:
        conditions.append(f"c.{qident(ts_col)} < %s")
        params.append(end_ts)

    if src and "src" in c_cols:
        conditions.append(f"c.{qident('src')} = %s")
        params.append(src)
    if regime and "regime" in c_cols:
        conditions.append(f"c.{qident('regime')} = %s")
        params.append(regime)
    if wallet_tag and "wallet_tag" in c_cols:
        conditions.append(f"c.{qident('wallet_tag')} = %s")
        params.append(wallet_tag)
    if min_feature_version > 0 and "feature_version" in c_cols:
        conditions.append(f"COALESCE(c.{qident('feature_version')}, 0) >= %s")
        params.append(int(min_feature_version))

    where_clause = " AND ".join(conditions) if conditions else "TRUE"

    join_type = "LEFT JOIN" if include_no_outcome else "JOIN"
    order_clause = (
        f"ORDER BY c.{qident(ts_col)} DESC" if ts_col else f"ORDER BY c.{qident(c_id)} DESC"
    )

    sql = f"""
        SELECT {", ".join(select_parts)}
        FROM {candidates.fq()} c
        {join_type} {outcomes.fq()} o
          ON o.{qident(o_cid)} = c.{qident(c_id)}
        WHERE {where_clause}
        {order_clause}
        LIMIT %s
    """.strip()

    params2 = [*params, int(limit)]

    if dry_run:
        log.info(
            "DRY RUN: include_no_outcome=%s features_col=%s ts_col=%s join=(o.%s=c.%s) outmap=%s",
            include_no_outcome,
            feat_col,
            ts_col,
            o_cid,
            c_id,
            outmap,
        )
        print("\n--- SQL ---\n" + sql + "\n--- PARAMS ---\n" + repr(params2))
        return []

    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(sql, params2)
        return [dict(r) for r in cur.fetchall()]


# ---------------------------------------------------------------------------
# Record builders
# ---------------------------------------------------------------------------


def build_chat_example(
    row: Mapping[str, Any],
    *,
    task: str,
    feature_keys: list[str],
    feature_max_chars: int,
) -> dict[str, Any]:
    feats = _parse_features(row.get("features"))
    feats_sel = _select_feature_keys(feats, feature_keys)
    feats_str = _truncate_json(feats_sel, feature_max_chars)

    ts_str = _ts_to_str(row.get("ts"))
    src = row.get("src") or ""
    regime = row.get("regime") or ""
    symbol = row.get("symbol") or row.get("sym") or ""
    mint = row.get("mint") or ""
    wallet_tag = row.get("wallet_tag") or ""
    feature_version = row.get("feature_version")

    # USER PROMPT: entry-time only + features (NO outcomes)
    lines = [
        "ENTRY snapshot:",
        f"ts: {ts_str}",
        f"src: {src}",
        f"regime: {regime}",
        f"symbol: {symbol}",
        f"mint: {mint}",
    ]
    if wallet_tag:
        lines.append(f"wallet_tag: {wallet_tag}")
    if feature_version is not None:
        lines.append(f"feature_version: {feature_version}")

    lines.append("features_json: " + feats_str)
    lines.append('Respond ONLY with {"verdict":"BUY|PASS","score":0.0-1.0}.')

    assistant = json.dumps(
        {"verdict": row["label_verdict"], "score": float(row["label_score"])},
        separators=(",", ":"),
    )

    return {
        "messages": [
            {"role": "system", "content": _system_prompt(task)},
            {"role": "user", "content": "\n".join(lines)},
            {"role": "assistant", "content": assistant},
        ],
        "metadata": {
            "candidate_id": row.get("candidate_id"),
            "ts": ts_str,
            "src": src,
            "regime": regime,
            "symbol": symbol,
            "mint": mint,
            "wallet_tag": wallet_tag or None,
            "feature_version": feature_version,
            # outcome-derived (metadata only)
            "pnl_pct": row.get("pnl_pct"),
            "runup_pct": row.get("runup_pct"),
            "drawdown_pct": row.get("drawdown_pct"),
            "hold_min": row.get("hold_min"),
            "outcome_label": row.get("label"),
            "label_source": row.get("label_source"),
            "verdict": row["label_verdict"],
            "score": float(row["label_score"]),
        },
    }


def build_simple_record(row: Mapping[str, Any], *, feature_keys: list[str]) -> dict[str, Any]:
    feats = _parse_features(row.get("features"))
    feats_sel = _select_feature_keys(feats, feature_keys)
    return {
        "input": {
            "ts": _ts_to_str(row.get("ts")),
            "src": row.get("src"),
            "regime": row.get("regime"),
            "symbol": row.get("symbol") or row.get("sym"),
            "mint": row.get("mint"),
            "wallet_tag": row.get("wallet_tag"),
            "feature_version": row.get("feature_version"),
            "features": feats_sel,
        },
        "output": {"verdict": row["label_verdict"], "score": float(row["label_score"])},
        "metadata": {
            "candidate_id": row.get("candidate_id"),
            "pnl_pct": row.get("pnl_pct"),
            "runup_pct": row.get("runup_pct"),
            "drawdown_pct": row.get("drawdown_pct"),
            "hold_min": row.get("hold_min"),
            "outcome_label": row.get("label"),
            "label_source": row.get("label_source"),
        },
    }


# ---------------------------------------------------------------------------
# Atomic JSONL write + preview
# ---------------------------------------------------------------------------


def _atomic_write_jsonl(path: str, records: Sequence[Mapping[str, Any]]) -> None:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix=p.name + ".", suffix=".tmp", dir=str(p.parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as f:
            for r in records:
                f.write(json.dumps(r, ensure_ascii=False, default=str) + "\n")
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, str(p))
    finally:
        try:
            if os.path.exists(tmp):
                os.remove(tmp)
        except Exception:
            pass


def _preview(path: str, n: int) -> None:
    if n <= 0:
        return
    print(f"\nPreview: {path} (first {n} lines)")
    try:
        with open(path, encoding="utf-8") as f:
            for _ in range(n):
                line = f.readline()
                if not line:
                    break
                print(line.rstrip())
    except Exception as e:
        print(f"(preview failed: {e})")


# ---------------------------------------------------------------------------
# Stratified split
# ---------------------------------------------------------------------------


def _stratified_split(
    records: list[dict[str, Any]],
    *,
    label_fn,
    val_fraction: float,
    rng: random.Random,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    if not records or val_fraction <= 0.0:
        return records, []
    val_fraction = max(0.0, min(0.5, float(val_fraction)))

    groups: dict[str, list[dict[str, Any]]] = {}
    for r in records:
        groups.setdefault(str(label_fn(r)), []).append(r)

    train: list[dict[str, Any]] = []
    val: list[dict[str, Any]] = []
    for g in groups.values():
        rng.shuffle(g)
        n = len(g)
        n_val = int(round(n * val_fraction))
        n_val = max(1, min(n - 1, n_val)) if n >= 2 else 0
        val.extend(g[:n_val])
        train.extend(g[n_val:])

    rng.shuffle(train)
    rng.shuffle(val)
    return train, val


# ---------------------------------------------------------------------------
# Optional: negative downsampling
# ---------------------------------------------------------------------------


def _downsample_negatives(
    labeled_rows: list[dict[str, Any]],
    *,
    neg_ratio: float,
    rng: random.Random,
) -> list[dict[str, Any]]:
    """
    Keep at most (BUY * neg_ratio) PASS rows.
    If neg_ratio <= 0: no downsampling.
    """
    if neg_ratio is None or float(neg_ratio) <= 0:
        return labeled_rows

    buys = [r for r in labeled_rows if r.get("label_verdict") == "BUY"]
    passes = [r for r in labeled_rows if r.get("label_verdict") == "PASS"]

    if not buys:
        # If we have zero buys, keep everything (don't destroy the dataset)
        return labeled_rows

    max_pass = int(round(len(buys) * float(neg_ratio)))
    if len(passes) <= max_pass:
        return labeled_rows

    rng.shuffle(passes)
    kept_passes = passes[:max_pass]
    out = buys + kept_passes
    rng.shuffle(out)
    return out


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Export ENTRY training dataset (candidates + optional outcomes) to JSONL."
    )

    p.add_argument(
        "--dsn",
        type=str,
        default="",
        help="Optional DSN override. Otherwise uses env POSTGRES_DSN/PG_DSN/...",
    )
    p.add_argument(
        "--sslmode",
        type=str,
        default="",
        help="Optional sslmode override (require|verify-full|disable).",
    )

    p.add_argument(
        "--candidates-table",
        type=str,
        default=_env_str("ENTRY_CANDIDATES_TABLE", "public.entry_candidates"),
    )
    p.add_argument(
        "--outcomes-table",
        type=str,
        default=_env_str("ENTRY_OUTCOMES_TABLE", "public.entry_outcomes"),
    )

    p.add_argument("--task", type=str, default=_env_str("ENTRY_TASK_NAME", "entry_gate_v3"))
    p.add_argument(
        "--format",
        choices=["chat", "simple"],
        default=_env_str("ENTRY_EXPORT_FORMAT", "chat") or "chat",
    )

    p.add_argument("--limit", type=int, default=_env_int("ENTRY_LIMIT", 4000))
    p.add_argument("--days-back", type=int, default=_env_int("ENTRY_DAYS_BACK", 14))
    p.add_argument("--start-date", type=str, default=_env_str("ENTRY_START_DATE", ""))
    p.add_argument("--end-date", type=str, default=_env_str("ENTRY_END_DATE", ""))

    p.add_argument("--src", type=str, default=_env_str("ENTRY_SRC", ""))
    p.add_argument("--regime", type=str, default=_env_str("ENTRY_REGIME", ""))
    p.add_argument("--wallet-tag", type=str, default=_env_str("ENTRY_WALLET_TAG", ""))
    p.add_argument(
        "--min-feature-version", type=int, default=_env_int("ENTRY_MIN_FEATURE_VERSION", 0)
    )

    # labeling thresholds (numeric fallback only)
    p.add_argument("--min-runup-buy", type=float, default=_env_float("ENTRY_MIN_RUNUP_BUY", 20.0))
    p.add_argument("--min-pnl-buy", type=float, default=_env_float("ENTRY_MIN_PNL_BUY", 10.0))
    p.add_argument(
        "--max-dd-for-buy", type=float, default=_env_float("ENTRY_MAX_DD_FOR_BUY", -25.0)
    )
    p.add_argument("--loss-pnl-cut", type=float, default=_env_float("ENTRY_LOSS_PNL_CUT", -15.0))
    p.add_argument("--rug-dd-cut", type=float, default=_env_float("ENTRY_RUG_DD_CUT", -40.0))

    # label mode
    p.add_argument(
        "--label-mode",
        choices=["label_first", "numeric_only", "label_only"],
        default=_env_str("ENTRY_LABEL_MODE", "label_first") or "label_first",
        help="How to derive labels. Default label_first.",
    )

    # include negatives via LEFT JOIN
    _add_bool_flag(
        p,
        "include-no-outcome",
        _env_bool("ENTRY_INCLUDE_NO_OUTCOME", True),
        "Include candidates with no outcome row (LEFT JOIN)",
    )

    # negative downsampling
    p.add_argument(
        "--neg-ratio",
        type=float,
        default=_env_float("ENTRY_NEG_RATIO", 0.0),
        help="If >0, keep at most BUY*neg_ratio PASS rows (downsample negatives). 0 disables.",
    )

    # output + split
    p.add_argument("--out", type=str, default=_env_str("ENTRY_OUT_PATH", "data/entry_train.jsonl"))
    p.add_argument("--val-out", type=str, default=_env_str("ENTRY_VAL_OUT_PATH", ""))
    p.add_argument("--val-fraction", type=float, default=_env_float("ENTRY_VAL_FRACTION", 0.0))

    # shuffle / seed / preview
    _add_bool_flag(p, "shuffle", _env_bool("ENTRY_SHUFFLE", True), "Shuffle examples")
    p.add_argument("--seed", type=int, default=_env_int("ENTRY_SEED", 0))
    p.add_argument("--preview-count", type=int, default=_env_int("ENTRY_PREVIEW_COUNT", 3))
    _add_bool_flag(p, "preview", True, "Print preview lines after export")

    # feature control
    p.add_argument("--feature-keys", type=str, default=_env_str("ENTRY_FEATURE_KEYS", ""))
    p.add_argument(
        "--feature-max-chars", type=int, default=_env_int("ENTRY_FEATURE_MAX_CHARS", 6000)
    )

    # diagnostics
    _add_bool_flag(p, "dry-run", _env_bool("ENTRY_DRY_RUN", False), "Print SQL+params and exit")
    _add_bool_flag(p, "connect-only", False, "Only test DB connection (SELECT 1) and exit")
    p.add_argument("--connect-tries", type=int, default=_env_int("ENTRY_CONNECT_TRIES", 6))
    p.add_argument(
        "--connect-timeout", type=float, default=_env_float("ENTRY_CONNECT_TIMEOUT", 20.0)
    )
    _add_bool_flag(p, "try-all-addrs", True, "Try all resolved IPs for the host (hostaddr pinning)")

    return p


def main() -> None:
    args = build_arg_parser().parse_args()

    dsn = pick_dsn(args.dsn)
    if args.sslmode:
        dsn = _set_query_param(dsn, "sslmode", str(args.sslmode).strip())
    dsn = _ensure_sslmode(dsn, "require")

    start_ts, end_ts = _resolve_range(int(args.days_back), args.start_date, args.end_date)

    shuffle = bool(args.shuffle)
    rng = random.Random(int(args.seed)) if int(args.seed) else random.Random()

    feature_keys = [x.strip() for x in (args.feature_keys or "").split(",") if x.strip()]

    c_ref = TableRef.parse(args.candidates_table)
    o_ref = TableRef.parse(args.outcomes_table)

    print(
        "Export params:\n"
        f"  repo_root           : {_REPO_ROOT}\n"
        f"  candidates_table    : {c_ref.fq()}\n"
        f"  outcomes_table      : {o_ref.fq()}\n"
        f"  limit               : {int(args.limit)}\n"
        f"  start_ts            : {start_ts}\n"
        f"  end_ts              : {end_ts}\n"
        f"  src_filter          : {(args.src or '').strip() or None}\n"
        f"  regime_filter       : {(args.regime or '').strip() or None}\n"
        f"  wallet_tag_filter   : {(args.wallet_tag or '').strip() or None}\n"
        f"  min_feature_version : {int(args.min_feature_version or 0) or None}\n"
        f"  include_no_outcome  : {bool(args.include_no_outcome)}\n"
        f"  label_mode          : {args.label_mode}\n"
        f"  neg_ratio           : {float(args.neg_ratio or 0.0)}\n"
        f"  format              : {args.format}\n"
        f"  out                 : {args.out}\n"
        f"  val_out             : {args.val_out}\n"
        f"  val_fraction        : {float(args.val_fraction or 0.0)}\n"
        f"  shuffle             : {shuffle} seed={int(args.seed or 0)}\n"
        f"  dsn                 : {_redact_dsn(dsn)}\n"
        f"  try_all_addrs       : {bool(args.try_all_addrs)}\n"
    )

    conn = connect_pg(
        dsn,
        tries=int(args.connect_tries),
        connect_timeout=float(args.connect_timeout),
        try_all_addrs=bool(args.try_all_addrs),
        application_name="export_entry_training",
    )

    try:
        if args.connect_only:
            with conn.cursor() as cur:
                cur.execute("SELECT 1 AS ok")
                v = cur.fetchone()
            print("CONNECT OK:", v[0] if v else None)
            return

        rows = _fetch_rows(
            conn,
            candidates=c_ref,
            outcomes=o_ref,
            limit=int(args.limit),
            start_ts=start_ts,
            end_ts=end_ts,
            src=(args.src or "").strip() or None,
            regime=(args.regime or "").strip() or None,
            wallet_tag=(args.wallet_tag or "").strip() or None,
            min_feature_version=int(args.min_feature_version or 0),
            include_no_outcome=bool(args.include_no_outcome),
            dry_run=bool(args.dry_run),
        )
    finally:
        with contextlib.suppress(Exception):
            conn.close()

    if args.dry_run:
        return

    if not rows:
        print("No rows returned. (Window/filters excluded everything or DB has no candidates.)")
        return

    # Label rows
    labeled: list[dict[str, Any]] = []
    counts = {"BUY": 0, "PASS": 0}

    for r in rows:
        lab = derive_label(
            r,
            label_mode=str(args.label_mode),
            min_runup_buy=float(args.min_runup_buy),
            min_pnl_buy=float(args.min_pnl_buy),
            max_dd_for_buy=float(args.max_dd_for_buy),
            loss_pnl_cut=float(args.loss_pnl_cut),
            rug_dd_cut=float(args.rug_dd_cut),
        )
        rr = dict(r)
        rr["label_verdict"] = lab["verdict"]
        rr["label_score"] = float(lab["score"])
        rr["label_source"] = lab.get("label_source")

        # canonical numeric fields (may not exist if label-only)
        if "pnl_pct" in lab:
            rr["pnl_pct"] = lab["pnl_pct"]
        if "runup_pct" in lab:
            rr["runup_pct"] = lab["runup_pct"]
        if "drawdown_pct" in lab:
            rr["drawdown_pct"] = lab["drawdown_pct"]
        if "hold_min" in lab:
            rr["hold_min"] = lab["hold_min"]

        counts[lab["verdict"]] = counts.get(lab["verdict"], 0) + 1
        labeled.append(rr)

    # Optional negative downsample
    labeled = _downsample_negatives(labeled, neg_ratio=float(args.neg_ratio or 0.0), rng=rng)

    # Shuffle rows (pre-record)
    if shuffle:
        rng.shuffle(labeled)

    # Build export records
    records: list[dict[str, Any]] = []
    if args.format == "chat":
        for r in labeled:
            records.append(
                build_chat_example(
                    r,
                    task=args.task,
                    feature_keys=feature_keys,
                    feature_max_chars=int(args.feature_max_chars),
                )
            )
    else:
        for r in labeled:
            records.append(build_simple_record(r, feature_keys=feature_keys))

    # Train/val split (stratified)
    train_records, val_records = records, []
    if args.val_out and float(args.val_fraction or 0.0) > 0.0:
        train_records, val_records = _stratified_split(
            records,
            label_fn=lambda rec: (rec.get("metadata", {}) or {}).get("verdict", ""),
            val_fraction=float(args.val_fraction),
            rng=rng,
        )

    _atomic_write_jsonl(args.out, train_records)
    print(f"Wrote {len(train_records)} records → {args.out}")

    if args.val_out and val_records:
        _atomic_write_jsonl(args.val_out, val_records)
        print(f"Wrote {len(val_records)} records → {args.val_out}")

    # Recount verdicts from final records (authoritative)
    final_counts = {"BUY": 0, "PASS": 0}
    for rec in records:
        v = (rec.get("metadata", {}) or {}).get("verdict")
        if v in final_counts:
            final_counts[v] += 1

    print("Label counts (pre-downsample):", counts)
    print("Label counts (final export):   ", final_counts)

    if bool(args.preview):
        _preview(args.out, int(args.preview_count or 3))
        if args.val_out and val_records:
            _preview(args.val_out, min(int(args.preview_count or 3), len(val_records)))


if __name__ == "__main__":
    main()
