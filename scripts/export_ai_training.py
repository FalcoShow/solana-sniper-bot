from __future__ import annotations

"""
scripts/export_ai_training.py — Quant-grade AI Training Exporter (LOCK-IN FINAL)
===============================================================================

Exports labeled historical examples from Postgres into *chat-style JSONL*
for OpenAI fine-tuning (or offline training pipelines).

Key upgrades in this LOCK-IN rewrite
------------------------------------
- ✅ Robust Postgres connect (asyncpg) with:
    - ssl=True by default (matches sslmode=require behavior)
    - retries + exponential backoff
    - clearer diagnostics on common Windows/network failures
- ✅ DSN resolution without importing ai_db by default (avoids side-effect warnings)
- ✅ DSN redaction (never prints credentials)
- ✅ Power-user knobs:
    --connect-only     : just test connection + SELECT 1
    --connect-tries    : retry attempts
    --connect-timeout  : connect timeout seconds
    --command-timeout  : query timeout seconds
    --ssl/--no-ssl     : force ssl behavior (default: ssl)
- ✅ Schema-aware querying:
    - introspects information_schema.columns
    - selects only columns that exist (NULL placeholders otherwise)
- ✅ Dataset hygiene:
    - stratified train/val split
    - optional bucket caps
    - deterministic shuffle
    - atomic JSONL writes

Quick checks
------------
  py -m py_compile scripts\\export_ai_training.py
  py -m scripts.export_ai_training --help

Examples
--------
  py -m scripts.export_ai_training --dataset trades --limit 4000 --out data/train.jsonl
  py -m scripts.export_ai_training --dataset candidates --limit 4000 --out data/train.jsonl

If Windows can't reach Render Postgres (common: outbound 5432 blocked),
run the exporter from a Render shell instead (same command).
"""

import argparse
import asyncio
import datetime as dt
import json
import os
import random
import re
import sys
import tempfile
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit, urlunsplit

import asyncpg


# =============================================================================
# Repo bootstrap (root + src/)
# =============================================================================
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


# =============================================================================
# Logging (canonical if present)
# =============================================================================
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
        return logging.getLogger("export_ai_training")


log = _get_logger()


# =============================================================================
# Env helpers
# =============================================================================
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


def _parse_bool_cli(v: Any) -> bool:
    if isinstance(v, bool):
        return v
    if v is None:
        return True
    s = str(v).strip().lower()
    if s in _TRUE:
        return True
    if s in _FALSE:
        return False
    # default: treat presence as True
    return True


def _add_bool_flag(p: argparse.ArgumentParser, name: str, default: bool, help_text: str) -> None:
    """
    Supports:
      --flag
      --flag true/false
      --no-flag
    """
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


def _normalize_dsn(dsn: str) -> str:
    dsn = (dsn or "").strip()
    if not dsn:
        return dsn
    if dsn.startswith("postgres://"):
        dsn = "postgresql://" + dsn[len("postgres://") :]
    if dsn.startswith("postgresql+asyncpg://"):
        dsn = "postgresql://" + dsn[len("postgresql+asyncpg://") :]
    return dsn


def _redact_dsn(dsn: str) -> str:
    """
    Redact password if present.
    postgresql://user:pass@host:5432/db -> postgresql://user:***@host:5432/db
    """
    try:
        u = urlsplit(dsn)
        if not u.username:
            return dsn
        netloc = u.hostname or ""
        if u.port:
            netloc = f"{netloc}:{u.port}"
        user = u.username
        if user:
            netloc = f"{user}:***@{netloc}"
        return urlunsplit((u.scheme, netloc, u.path, u.query, u.fragment))
    except Exception:
        return "postgresql://***"


async def resolve_dsn(cli_dsn: str | None) -> str:
    """
    DSN selection order:
      1) --dsn
      2) env fallbacks (canonical-ish keys)
    (We intentionally do NOT import ai_db here to avoid side-effect warnings.)
    """
    if cli_dsn and str(cli_dsn).strip():
        return _normalize_dsn(str(cli_dsn))

    for key in (
        "POSTGRES_DSN",
        "PG_DSN",
        "PG_DSN_INTERNAL",
        "PG_DSN_EXTERNAL",
        "SNIPER_DB_DSN",
        "PNL_DSN",
        "DATABASE_URL",
    ):
        v = os.getenv(key)
        if v and str(v).strip():
            return _normalize_dsn(str(v))

    raise SystemExit(
        "No DSN found. Set POSTGRES_DSN/PG_DSN (or pass --dsn).\n"
        "Example: set PG_DSN=postgresql://user:pass@host:5432/db?sslmode=require"
    )


# =============================================================================
# Defaults / thresholds (env-tunable)
# =============================================================================
DEFAULT_OUT_PATH = _env_str("AI_TRAINING_PATH", "data/ai_training.jsonl")
DEFAULT_LIMIT = _env_int("AI_TRAINING_LIMIT", 2000)

# PnL → bucket thresholds (pct preferred)
MONSTER_WIN_PCT = _env_float("AI_MONSTER_WIN_PCT", 40.0)
GOOD_PNL_PCT = _env_float("AI_GOOD_PNL_PCT", 10.0)
BAD_PNL_PCT = _env_float("AI_BAD_PNL_PCT", -10.0)
RUG_PNL_PCT = _env_float("AI_RUG_PNL_PCT", -40.0)

# USD fallback thresholds
GOOD_PNL_USD = _env_float("AI_GOOD_PNL_USD", 5.0)
BAD_PNL_USD = _env_float("AI_BAD_PNL_USD", -3.0)

DEFAULT_DAYS_BACK = _env_int("AI_DAYS_BACK", 90)
DEFAULT_MAX_PER_BUCKET = _env_int("AI_MAX_PER_BUCKET", 0)  # 0=no cap
DEFAULT_GATE_MODE = _env_str("AI_GATE_MODE", "pnl+label")  # pnl|label|pnl+label
DEFAULT_INCLUDE_NEUTRAL = _parse_bool_cli(os.getenv("AI_INCLUDE_NEUTRAL", "true"))
DEFAULT_MIN_FEAT_VERSION = _env_int("AI_MIN_FEATURE_VERSION", 0)

DEFAULT_TRADES_TABLE = _env_str("AI_TRADES_TABLE", "trades")
DEFAULT_CANDIDATES_TABLE = _env_str("AI_CANDIDATES_TABLE", "entry_candidates")
DEFAULT_OUTCOMES_TABLE = _env_str("AI_OUTCOMES_TABLE", "entry_outcomes")

DEFAULT_FEATURE_KEYS = _env_str(
    "AI_FEATURE_KEYS",
    "pf_momentum_score,pf_final_score,pf_risk_adjusted_final,"
    "ai_entry_score,ai_fraud_score,ai_risk_score,"
    "risk_capacity_score,ctx_open_positions,ctx_max_positions,"
    "ctx_daily_realized_pnl_pct,ctx_daily_drawdown_pct,"
    "vol_5m,buys_5m,sells_5m,liq_usd,whale_buy_score,alpha_wallet_score",
)


# =============================================================================
# Date helpers
# =============================================================================
def _parse_date(s: str) -> dt.datetime | None:
    s = (s or "").strip()
    if not s:
        return None
    try:
        if "T" in s:
            x = dt.datetime.fromisoformat(s)
            return x if x.tzinfo else x.replace(tzinfo=dt.UTC)
        x = dt.datetime.strptime(s, "%Y-%m-%d")
        return x.replace(tzinfo=dt.UTC)
    except Exception:
        return None


def _resolve_date_range(
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


# =============================================================================
# Connect helper (SSL + retries + better errors)
# =============================================================================
async def connect_pg(
    dsn: str,
    *,
    tries: int = 6,
    base_delay: float = 0.6,
    connect_timeout: float = 20.0,
    command_timeout: float = 60.0,
    use_ssl: bool = True,
    application_name: str = "export_ai_training",
) -> asyncpg.Connection:
    last: BaseException | None = None
    red = _redact_dsn(dsn)

    for i in range(max(1, int(tries))):
        try:
            return await asyncpg.connect(
                dsn,
                ssl=True if use_ssl else None,
                timeout=float(connect_timeout),
                command_timeout=float(command_timeout),
                server_settings={"application_name": application_name},
            )
        except Exception as e:
            last = e
            delay = base_delay * (2**i)
            log.warning("PG connect failed (%s/%s) dsn=%s err=%r", i + 1, tries, red, e)
            if i + 1 < tries:
                await asyncio.sleep(delay)

    # Final, user-facing diagnostic
    msg = (
        "Postgres connection failed after retries.\n"
        f"DSN: {red}\n"
        f"Last error: {last!r}\n\n"
        "Most common causes on Windows:\n"
        "  • Outbound port 5432 blocked by ISP/router/firewall\n"
        "  • DNS resolution issues\n"
        "  • Corporate/School network blocks DB ports\n\n"
        "Try:\n"
        "  1) nslookup <your-host>\n"
        "  2) Test-NetConnection <your-host> -Port 5432\n"
        "If blocked, run this exporter from a Render shell (same repo/command).\n"
    )
    raise SystemExit(msg)


# =============================================================================
# Labeling logic
# =============================================================================
def _to_float(x: Any) -> float:
    try:
        return float(x)
    except Exception:
        return 0.0


def _to_optional_float(x: Any) -> float | None:
    if x is None:
        return None
    try:
        return float(x)
    except Exception:
        return None


def _pick_bucket(pnl_usd: float, pnl_pct: float | None) -> str:
    if pnl_pct is not None:
        if pnl_pct >= MONSTER_WIN_PCT:
            return "monster_win"
        if pnl_pct >= GOOD_PNL_PCT:
            return "win"
        if pnl_pct <= RUG_PNL_PCT:
            return "rug"
        if pnl_pct <= BAD_PNL_PCT:
            return "loss"
        return "scratch"

    if pnl_usd >= GOOD_PNL_USD * 4:
        return "monster_win"
    if pnl_usd >= GOOD_PNL_USD:
        return "win"
    if pnl_usd <= BAD_PNL_USD * 4:
        return "rug"
    if pnl_usd <= BAD_PNL_USD:
        return "loss"
    return "scratch"


def _bucket_to_gate(bucket: str) -> tuple[str, float]:
    if bucket == "monster_win":
        return "GOOD", 0.98
    if bucket == "win":
        return "GOOD", 0.93
    if bucket == "rug":
        return "BAD", 0.98
    if bucket == "loss":
        return "BAD", 0.90
    return "NEUTRAL", 0.55


def _merge_label_sources(
    *, bucket_gate_label: str, bucket_score: float, raw_label: str | None, gate_mode: str
) -> tuple[str, float, str]:
    gate_mode = (gate_mode or "pnl+label").lower()
    raw = (raw_label or "").strip().lower()

    raw_gate: str | None = None
    if raw in {"good", "ok", "buy", "entry_ok", "green", "win", "monster_win"}:
        raw_gate = "GOOD"
    elif raw in {"bad", "avoid", "no_buy", "red", "loss", "rug", "scam"}:
        raw_gate = "BAD"
    elif raw in {"neutral", "meh", "scratch"}:
        raw_gate = "NEUTRAL"

    if gate_mode == "pnl":
        return bucket_gate_label, bucket_score, raw_label or bucket_gate_label

    if gate_mode == "label":
        if raw_gate is not None:
            return raw_gate, max(bucket_score, 0.92), raw_label or raw_gate
        return bucket_gate_label, bucket_score, raw_label or bucket_gate_label

    # pnl+label
    if raw_gate is None:
        return bucket_gate_label, bucket_score, raw_label or bucket_gate_label

    if raw_gate == bucket_gate_label:
        return raw_gate, min(1.0, bucket_score + 0.03), raw_label or raw_gate

    if raw in {"rug", "scam"} and raw_gate == "BAD":
        return "BAD", max(bucket_score, 0.97), raw_label or raw_gate

    return bucket_gate_label, max(0.70, min(bucket_score, 0.90)), raw_label or bucket_gate_label


def build_label_info(row: Mapping[str, Any], gate_mode: str) -> dict[str, Any]:
    pnl_usd = _to_float(
        row.get("pnl_usd") or row.get("realized_usd") or row.get("net_pnl_usd") or 0.0
    )
    pnl_pct = _to_optional_float(row.get("pnl_pct"))

    bucket = _pick_bucket(pnl_usd, pnl_pct)
    base_gate, base_score = _bucket_to_gate(bucket)

    raw_label = row.get("label") or row.get("outcome_label") or row.get("ai_label")
    gate_label, score, merged_label = _merge_label_sources(
        bucket_gate_label=base_gate,
        bucket_score=base_score,
        raw_label=str(raw_label) if raw_label is not None else None,
        gate_mode=gate_mode,
    )

    return {
        "bucket": bucket,
        "gate_label": gate_label,
        "label": merged_label or gate_label,
        "score": float(score),
        "pnl_usd": float(pnl_usd),
        "pnl_pct": float(pnl_pct) if pnl_pct is not None else None,
    }


# =============================================================================
# Example builder (chat JSONL)
# =============================================================================
def _format_ts(ts: Any) -> str:
    if isinstance(ts, dt.datetime):
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=dt.UTC)
        return ts.isoformat()
    return str(ts)


def _format_maybe_float(v: Any) -> str:
    f = _to_optional_float(v)
    if f is None:
        return "null"
    return f"{f:.3f}"


def _parse_feature_keys(s: str) -> list[str]:
    out: list[str] = []
    for part in (s or "").replace(";", ",").split(","):
        p = part.strip()
        if p:
            out.append(p)
    return out


def _build_feature_snippet(row: Mapping[str, Any], *, keys: Sequence[str], max_chars: int) -> str:
    feats = row.get("features") or {}
    if isinstance(feats, str):
        try:
            feats = json.loads(feats)
        except Exception:
            feats = {}
    if not isinstance(feats, dict):
        return ""

    bits: list[str] = []
    for k in keys:
        if k in feats:
            bits.append(f"{k}={feats.get(k)!r}")
    s = ", ".join(bits)
    if max_chars > 0 and len(s) > max_chars:
        return s[: max_chars - 12] + " ...[trim]"
    return s


def _system_prompt(task: str) -> str:
    task = task or "entry_gate_v3"
    return (
        "You are an AI assistant for a Solana trading system.\n"
        "You will receive a compact snapshot of a historical trade or entry candidate plus its realized outcome.\n\n"
        "Decide whether similar future setups should be allowed through the entry gate.\n"
        "Return ONLY a single JSON object:\n"
        '  {"label":"GOOD|BAD|NEUTRAL","score":0.0-1.0}\n'
        "No explanations. No extra keys.\n\n"
        f"Task profile: {task}"
    )


def build_chat_example(
    row: Mapping[str, Any],
    label_info: Mapping[str, Any],
    *,
    dataset: str,
    task: str,
    feature_keys: Sequence[str],
    feature_max_chars: int,
) -> dict[str, Any]:
    ts_str = _format_ts(row.get("ts"))
    src = row.get("src") or row.get("source") or dataset
    regime = row.get("regime") or "unknown_regime"
    symbol = row.get("symbol") or row.get("sym") or row.get("token") or ""
    mint = row.get("mint") or row.get("token") or "unknown_mint"
    side = row.get("side") or row.get("direction") or "BUY"

    wallet_tag = row.get("wallet_tag")
    feature_version = row.get("feature_version")
    risk_profile = row.get("risk_profile")
    exit_policy_name = row.get("exit_policy_name")
    hold_sec = row.get("hold_sec") or row.get("hold_time_sec")
    reason = (
        row.get("reason")
        or row.get("entry_reason")
        or row.get("exit_reason")
        or row.get("notes")
        or ""
    )

    feature_snippet = _build_feature_snippet(row, keys=feature_keys, max_chars=feature_max_chars)

    bucket = str(label_info["bucket"])
    gate_label = str(label_info["gate_label"])
    raw_label = str(label_info.get("label", gate_label))
    pnl_usd = float(label_info["pnl_usd"])
    pnl_pct = label_info.get("pnl_pct")

    lines: list[str] = []
    lines.append("Past outcome snapshot:")
    lines.append(f"Timestamp: {ts_str}")
    lines.append(f"Source: {src} | Regime: {regime}")
    lines.append(f"Side: {side} | Symbol: {symbol} | Mint: {mint}")
    if wallet_tag:
        lines.append(f"Wallet tag: {wallet_tag}")
    if feature_version is not None:
        lines.append(f"Feature version: {feature_version}")
    if risk_profile:
        lines.append(f"Risk profile: {risk_profile}")
    if exit_policy_name:
        lines.append(f"Exit policy: {exit_policy_name}")

    lines.append(f"Realized PnL: pnl_usd={pnl_usd:.3f}, pnl_pct={_format_maybe_float(pnl_pct)}")
    if hold_sec is not None:
        lines.append(f"Hold seconds: {hold_sec}")
    if reason:
        lines.append(f"Notes: {reason}")
    if feature_snippet:
        lines.append("Feature snapshot: " + feature_snippet)

    lines.append(f"Reference bucket: {bucket} (raw_label={raw_label}, gate_label={gate_label})")
    lines.append('Respond ONLY as: {"label":"GOOD|BAD|NEUTRAL","score":0.0-1.0}')

    user_content = "\n".join(lines)
    assistant_content = json.dumps(
        {"label": gate_label, "score": float(label_info["score"])}, ensure_ascii=False
    )

    return {
        "messages": [
            {"role": "system", "content": _system_prompt(task)},
            {"role": "user", "content": user_content},
            {"role": "assistant", "content": assistant_content},
        ],
        "metadata": {
            "dataset": dataset,
            "task": task,
            "label_bucket": bucket,
            "gate_label": gate_label,
            "raw_label": raw_label,
            "pnl_usd": pnl_usd,
            "pnl_pct": pnl_pct,
            "src": src,
            "regime": regime,
            "symbol": symbol,
            "mint": mint,
            "side": side,
            "wallet_tag": wallet_tag,
            "feature_version": feature_version,
            "candidate_id": row.get("candidate_id"),
            "trade_id": row.get("id") or row.get("trade_id"),
        },
    }


# =============================================================================
# Atomic IO
# =============================================================================
def _atomic_write_jsonl(path: str, examples: Sequence[Mapping[str, Any]]) -> None:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix=p.name + ".", suffix=".tmp", dir=str(p.parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as f:
            for ex in examples:
                f.write(json.dumps(ex, ensure_ascii=False, default=str) + "\n")
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)
    finally:
        try:
            if os.path.exists(tmp):
                os.remove(tmp)
        except Exception:
            pass


def _preview_file(path: str, max_lines: int) -> None:
    print(f"\nPreview of {path} (first {max_lines} lines):")
    try:
        with open(path, encoding="utf-8") as f:
            for _ in range(max_lines):
                line = f.readline()
                if not line:
                    break
                print(line.rstrip())
    except FileNotFoundError:
        print("  (file not found)")
    except Exception as e:
        print(f"  (preview failed: {e})")


# =============================================================================
# Schema-aware SQL helpers
# =============================================================================
_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def qi(name: str) -> str:
    """
    Quote a SQL identifier safely (and reject unexpected characters).
    """
    if not name or not _IDENT_RE.match(name):
        raise SystemExit(f"Unsafe identifier: {name!r}")
    return f'"{name}"'


@dataclass(frozen=True)
class TableRef:
    schema: str
    name: str

    @staticmethod
    def parse(s: str, default_schema: str = "public") -> TableRef:
        s = (s or "").strip()
        if not s:
            return TableRef(default_schema, s)
        if "." in s:
            a, b = s.split(".", 1)
            return TableRef(a.strip() or default_schema, b.strip())
        return TableRef(default_schema, s)

    def fq(self) -> str:
        return f"{qi(re.sub(r'[^A-Za-z0-9_]', '', self.schema))}.{qi(re.sub(r'[^A-Za-z0-9_]', '', self.name))}"


async def _get_columns(conn: asyncpg.Connection, t: TableRef) -> set[str]:
    rows = await conn.fetch(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema=$1 AND table_name=$2
        """,
        t.schema,
        t.name,
    )
    return {str(r["column_name"]) for r in rows}


def _first_present(cols: set[str], *cands: str) -> str | None:
    for c in cands:
        if c in cols:
            return c
    return None


# =============================================================================
# Fetchers
# =============================================================================
async def fetch_trades(
    conn: asyncpg.Connection,
    *,
    table: TableRef,
    limit: int,
    start_ts: dt.datetime | None,
    end_ts: dt.datetime | None,
    src_filter: str | None,
    regime_filter: str | None,
    wallet_tag_filter: str | None,
    min_feature_version: int | None,
    dry_run: bool,
) -> list[dict[str, Any]]:
    cols = await _get_columns(conn, table)
    if not cols:
        raise SystemExit(f"Table not found or no columns visible: {table.fq()}")

    ts_col = _first_present(cols, "ts", "time", "created_at")
    if ts_col is None:
        raise SystemExit(f"{table.fq()} must have a timestamp column (ts/time/created_at).")

    src_col = _first_present(cols, "src", "source")
    regime_col = _first_present(cols, "regime")
    symbol_col = _first_present(cols, "symbol", "sym", "token_symbol")
    mint_col = _first_present(cols, "mint", "token_mint", "token")
    side_col = _first_present(cols, "side", "direction")
    pnl_usd_col = _first_present(cols, "pnl_usd", "realized_usd", "net_pnl_usd")
    pnl_pct_col = _first_present(cols, "pnl_pct", "net_pnl_pct", "pnl_total_pct")
    hold_col = _first_present(cols, "hold_sec", "hold_time_sec")
    wallet_tag_col = _first_present(cols, "wallet_tag")
    feat_ver_col = _first_present(cols, "feature_version")
    features_col = _first_present(cols, "features", "feature_json")
    label_col = _first_present(cols, "label", "outcome_label", "ai_label")
    reason_col = _first_present(cols, "reason", "notes", "entry_reason", "exit_reason")
    risk_profile_col = _first_present(cols, "risk_profile")
    exit_policy_col = _first_present(cols, "exit_policy_name")

    select_cols: list[str] = []

    def add(alias: str, col: str | None) -> None:
        if col:
            select_cols.append(f"{qi(col)} AS {qi(alias)}")
        else:
            select_cols.append(f"NULL AS {qi(alias)}")

    add("ts", ts_col)
    add("src", src_col)
    add("regime", regime_col)
    add("symbol", symbol_col)
    add("mint", mint_col)
    add("side", side_col)
    add("pnl_usd", pnl_usd_col)
    add("pnl_pct", pnl_pct_col)
    add("hold_sec", hold_col)
    add("wallet_tag", wallet_tag_col)
    add("feature_version", feat_ver_col)
    add("features", features_col)
    add("label", label_col)
    add("reason", reason_col)
    add("risk_profile", risk_profile_col)
    add("exit_policy_name", exit_policy_col)

    conditions: list[str] = []
    params: list[Any] = []
    idx = 1

    # require some outcome signal
    checks: list[str] = []
    if pnl_usd_col:
        checks.append(f"{qi(pnl_usd_col)} IS NOT NULL")
    if pnl_pct_col:
        checks.append(f"{qi(pnl_pct_col)} IS NOT NULL")
    if label_col:
        checks.append(f"{qi(label_col)} IS NOT NULL")
    if checks:
        conditions.append("(" + " OR ".join(checks) + ")")

    if start_ts is not None:
        conditions.append(f"{qi(ts_col)} >= ${idx}")
        params.append(start_ts)
        idx += 1
    if end_ts is not None:
        conditions.append(f"{qi(ts_col)} < ${idx}")
        params.append(end_ts)
        idx += 1
    if src_filter and src_col:
        conditions.append(f"{qi(src_col)} = ${idx}")
        params.append(src_filter)
        idx += 1
    if regime_filter and regime_col:
        conditions.append(f"{qi(regime_col)} = ${idx}")
        params.append(regime_filter)
        idx += 1
    if wallet_tag_filter and wallet_tag_col:
        conditions.append(f"{qi(wallet_tag_col)} = ${idx}")
        params.append(wallet_tag_filter)
        idx += 1
    if min_feature_version is not None and min_feature_version > 0 and feat_ver_col:
        conditions.append(f"COALESCE({qi(feat_ver_col)}, 0) >= ${idx}")
        params.append(int(min_feature_version))
        idx += 1

    where_clause = " AND ".join(conditions) if conditions else "TRUE"
    sql = f"""
        SELECT {", ".join(select_cols)}
        FROM {table.fq()}
        WHERE {where_clause}
        ORDER BY {qi(ts_col)} DESC
        LIMIT ${idx}
    """
    params.append(int(limit))

    if dry_run:
        print("\n--- DRY RUN SQL (trades) ---")
        print(sql.strip())
        print("params:", params)
        return []

    rows = await conn.fetch(sql, *params)
    return [dict(r) for r in rows]


async def fetch_candidates_with_outcomes(
    conn: asyncpg.Connection,
    *,
    candidates_table: TableRef,
    outcomes_table: TableRef,
    limit: int,
    start_ts: dt.datetime | None,
    end_ts: dt.datetime | None,
    src_filter: str | None,
    regime_filter: str | None,
    wallet_tag_filter: str | None,
    min_feature_version: int | None,
    dry_run: bool,
) -> list[dict[str, Any]]:
    ccols = await _get_columns(conn, candidates_table)
    ocols = await _get_columns(conn, outcomes_table)
    if not ccols:
        raise SystemExit(f"Table not found or no columns visible: {candidates_table.fq()}")
    if not ocols:
        raise SystemExit(f"Table not found or no columns visible: {outcomes_table.fq()}")

    c_ts = _first_present(ccols, "ts", "time", "created_at")
    c_id = _first_present(ccols, "id", "candidate_id")
    if c_ts is None or c_id is None:
        raise SystemExit(
            "entry_candidates must have id + ts columns (id/candidate_id, ts/time/created_at)."
        )

    # candidate cols
    c_src = _first_present(ccols, "src", "source")
    c_regime = _first_present(ccols, "regime")
    c_mint = _first_present(ccols, "mint", "token", "token_mint")
    c_symbol = _first_present(ccols, "symbol", "sym", "token_symbol")
    c_wallet = _first_present(ccols, "wallet_tag")
    c_fv = _first_present(ccols, "feature_version")
    c_features = _first_present(ccols, "features", "feature_json")
    c_risk_profile = _first_present(ccols, "risk_profile")
    c_exit_policy = _first_present(ccols, "exit_policy_name")

    # outcome cols
    o_cid = _first_present(ocols, "candidate_id")
    if o_cid is None:
        raise SystemExit("entry_outcomes must have candidate_id column.")
    o_label = _first_present(ocols, "label", "outcome_label")
    o_pnl_pct = _first_present(ocols, "pnl_pct", "net_pnl_pct", "pnl_total_pct")
    o_pnl_usd = _first_present(ocols, "pnl_usd", "realized_usd", "net_pnl_usd")
    o_hold = _first_present(ocols, "hold_sec", "hold_time_sec")

    select_cols: list[str] = []

    def add(expr: str, alias: str) -> None:
        select_cols.append(f"{expr} AS {qi(alias)}")

    add(f"c.{qi(c_id)}", "candidate_id")
    add(f"c.{qi(c_ts)}", "ts")
    add(f"c.{qi(c_src)}" if c_src else "NULL", "src")
    add(f"c.{qi(c_regime)}" if c_regime else "NULL", "regime")
    add(f"c.{qi(c_mint)}" if c_mint else "NULL", "mint")
    add(f"c.{qi(c_symbol)}" if c_symbol else "NULL", "symbol")
    add(f"c.{qi(c_wallet)}" if c_wallet else "NULL", "wallet_tag")
    add(f"c.{qi(c_fv)}" if c_fv else "NULL", "feature_version")
    add(f"c.{qi(c_features)}" if c_features else "NULL", "features")
    add(f"c.{qi(c_risk_profile)}" if c_risk_profile else "NULL", "risk_profile")
    add(f"c.{qi(c_exit_policy)}" if c_exit_policy else "NULL", "exit_policy_name")

    add(f"o.{qi(o_label)}" if o_label else "NULL", "outcome_label")
    add(f"o.{qi(o_pnl_pct)}" if o_pnl_pct else "NULL", "pnl_pct")
    add(f"o.{qi(o_pnl_usd)}" if o_pnl_usd else "NULL", "pnl_usd")
    add(f"o.{qi(o_hold)}" if o_hold else "NULL", "hold_sec")

    conditions: list[str] = []
    params: list[Any] = []
    idx = 1

    checks: list[str] = []
    if o_pnl_usd:
        checks.append(f"o.{qi(o_pnl_usd)} IS NOT NULL")
    if o_pnl_pct:
        checks.append(f"o.{qi(o_pnl_pct)} IS NOT NULL")
    if o_label:
        checks.append(f"o.{qi(o_label)} IS NOT NULL")
    if checks:
        conditions.append("(" + " OR ".join(checks) + ")")

    if start_ts is not None:
        conditions.append(f"c.{qi(c_ts)} >= ${idx}")
        params.append(start_ts)
        idx += 1
    if end_ts is not None:
        conditions.append(f"c.{qi(c_ts)} < ${idx}")
        params.append(end_ts)
        idx += 1
    if src_filter and c_src:
        conditions.append(f"c.{qi(c_src)} = ${idx}")
        params.append(src_filter)
        idx += 1
    if regime_filter and c_regime:
        conditions.append(f"c.{qi(c_regime)} = ${idx}")
        params.append(regime_filter)
        idx += 1
    if wallet_tag_filter and c_wallet:
        conditions.append(f"c.{qi(c_wallet)} = ${idx}")
        params.append(wallet_tag_filter)
        idx += 1
    if min_feature_version is not None and min_feature_version > 0 and c_fv:
        conditions.append(f"COALESCE(c.{qi(c_fv)}, 0) >= ${idx}")
        params.append(int(min_feature_version))
        idx += 1

    where_clause = " AND ".join(conditions) if conditions else "TRUE"

    sql = f"""
        SELECT {", ".join(select_cols)}
        FROM {candidates_table.fq()} c
        JOIN {outcomes_table.fq()}  o
          ON o.{qi(o_cid)} = c.{qi(c_id)}
        WHERE {where_clause}
        ORDER BY c.{qi(c_ts)} DESC
        LIMIT ${idx}
    """
    params.append(int(limit))

    if dry_run:
        print("\n--- DRY RUN SQL (candidates) ---")
        print(sql.strip())
        print("params:", params)
        return []

    rows = await conn.fetch(sql, *params)
    return [dict(r) for r in rows]


# =============================================================================
# Balancing + splitting
# =============================================================================
def _balance_examples(
    rows: Iterable[Mapping[str, Any]],
    *,
    dataset: str,
    task: str,
    gate_mode: str,
    include_neutral: bool,
    max_per_bucket: int,
    global_limit: int,
    feature_keys: Sequence[str],
    feature_max_chars: int,
) -> tuple[list[dict[str, Any]], dict[str, int], dict[str, int]]:
    examples: list[dict[str, Any]] = []
    gate_counts: dict[str, int] = {}
    bucket_counts: dict[str, int] = {}

    caps: dict[str, int] = {}
    if max_per_bucket > 0:
        caps = {b: int(max_per_bucket) for b in ("monster_win", "win", "scratch", "loss", "rug")}

    for row in rows:
        if global_limit > 0 and len(examples) >= global_limit:
            break

        li = build_label_info(row, gate_mode=gate_mode)
        bucket = str(li["bucket"])
        gate = str(li["gate_label"])

        if (not include_neutral) and gate == "NEUTRAL":
            continue

        if caps:
            used = bucket_counts.get(bucket, 0)
            if used >= caps.get(bucket, max_per_bucket):
                continue

        ex = build_chat_example(
            row,
            li,
            dataset=dataset,
            task=task,
            feature_keys=feature_keys,
            feature_max_chars=feature_max_chars,
        )
        examples.append(ex)

        bucket_counts[bucket] = bucket_counts.get(bucket, 0) + 1
        gate_counts[gate] = gate_counts.get(gate, 0) + 1

    return examples, gate_counts, bucket_counts


def _stratified_split(
    examples: list[dict[str, Any]],
    *,
    key_fn: Callable[[dict[str, Any]], str],
    val_fraction: float,
    rng: random.Random,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    if not examples or val_fraction <= 0:
        return examples, []
    val_fraction = max(0.0, min(0.5, float(val_fraction)))

    groups: dict[str, list[dict[str, Any]]] = {}
    for ex in examples:
        k = key_fn(ex)
        groups.setdefault(k, []).append(ex)

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


# =============================================================================
# CLI
# =============================================================================
def build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Export AI training data from Postgres into chat JSONL."
    )

    p.add_argument(
        "--dsn",
        default=_env_str("AI_DSN", ""),
        help="Optional DSN override. Otherwise uses POSTGRES_DSN/PG_DSN/...",
    )
    p.add_argument(
        "--dataset",
        choices=["trades", "candidates"],
        default=_env_str("AI_EXPORT_SOURCE", "trades") or "trades",
    )
    p.add_argument("--task", default=_env_str("AI_TASK_NAME", "entry_gate_v3"))

    p.add_argument("--limit", type=int, default=DEFAULT_LIMIT)
    p.add_argument("--days-back", type=int, default=_env_int("AI_DAYS_BACK", DEFAULT_DAYS_BACK))
    p.add_argument("--start-date", default=_env_str("AI_START_DATE", ""))
    p.add_argument("--end-date", default=_env_str("AI_END_DATE", ""))

    p.add_argument("--src", default=_env_str("AI_EXPORT_SRC", ""))
    p.add_argument("--regime", default=_env_str("AI_EXPORT_REGIME", ""))
    p.add_argument("--wallet-tag", default=_env_str("AI_WALLET_TAG", ""))
    p.add_argument(
        "--min-feature-version",
        type=int,
        default=_env_int("AI_MIN_FEATURE_VERSION", DEFAULT_MIN_FEAT_VERSION),
    )

    p.add_argument(
        "--gate-mode",
        choices=["pnl", "label", "pnl+label"],
        default=_env_str("AI_GATE_MODE", DEFAULT_GATE_MODE),
    )

    _add_bool_flag(p, "include-neutral", DEFAULT_INCLUDE_NEUTRAL, "Include NEUTRAL examples")
    p.add_argument(
        "--max-per-bucket", type=int, default=_env_int("AI_MAX_PER_BUCKET", DEFAULT_MAX_PER_BUCKET)
    )

    p.add_argument("--out", default=DEFAULT_OUT_PATH)
    p.add_argument("--val-out", default=_env_str("AI_VAL_PATH", ""))
    p.add_argument("--val-fraction", type=float, default=_env_float("AI_VAL_FRACTION", 0.0))

    _add_bool_flag(p, "shuffle", True, "Shuffle rows/examples")
    p.add_argument("--seed", type=int, default=_env_int("AI_SEED", 0))

    p.add_argument("--preview-count", type=int, default=_env_int("AI_PREVIEW_COUNT", 3))
    _add_bool_flag(p, "preview", True, "Print preview lines after export")

    p.add_argument("--feature-keys", default=_env_str("AI_FEATURE_KEYS", DEFAULT_FEATURE_KEYS))
    p.add_argument("--feature-max-chars", type=int, default=_env_int("AI_FEATURE_MAX_CHARS", 800))

    p.add_argument("--trades-table", default=_env_str("AI_TRADES_TABLE", DEFAULT_TRADES_TABLE))
    p.add_argument(
        "--candidates-table", default=_env_str("AI_CANDIDATES_TABLE", DEFAULT_CANDIDATES_TABLE)
    )
    p.add_argument(
        "--outcomes-table", default=_env_str("AI_OUTCOMES_TABLE", DEFAULT_OUTCOMES_TABLE)
    )

    _add_bool_flag(
        p,
        "dry-run",
        bool(_parse_bool_cli(os.getenv("AI_DRY_RUN", "false"))),
        "Print SQL + params and exit",
    )

    # Connect knobs
    _add_bool_flag(p, "connect-only", False, "Only test DB connection (SELECT 1) and exit")
    p.add_argument("--connect-tries", type=int, default=_env_int("AI_CONNECT_TRIES", 6))
    p.add_argument("--connect-timeout", type=float, default=_env_float("AI_CONNECT_TIMEOUT", 20.0))
    p.add_argument("--command-timeout", type=float, default=_env_float("AI_COMMAND_TIMEOUT", 60.0))
    _add_bool_flag(p, "ssl", True, "Use SSL for DB connection (default true)")

    return p


# =============================================================================
# Main
# =============================================================================
async def _run(args: argparse.Namespace) -> int:
    dsn = await resolve_dsn(args.dsn or None)
    start_ts, end_ts = _resolve_date_range(int(args.days_back or 0), args.start_date, args.end_date)

    src_filter = (args.src or "").strip() or None
    regime_filter = (args.regime or "").strip() or None
    wallet_tag_filter = (args.wallet_tag or "").strip() or None
    min_fv = int(args.min_feature_version) if int(args.min_feature_version or 0) > 0 else None

    feature_keys = _parse_feature_keys(args.feature_keys)
    feature_max_chars = int(args.feature_max_chars or 0)

    trades_table = TableRef.parse(args.trades_table)
    candidates_table = TableRef.parse(args.candidates_table)
    outcomes_table = TableRef.parse(args.outcomes_table)

    print(
        "Export params:\n"
        f"  repo_root           : {_REPO_ROOT}\n"
        f"  dataset             : {args.dataset}\n"
        f"  limit               : {int(args.limit)}\n"
        f"  start_ts            : {start_ts}\n"
        f"  end_ts              : {end_ts}\n"
        f"  src_filter          : {src_filter}\n"
        f"  regime_filter       : {regime_filter}\n"
        f"  wallet_tag_filter   : {wallet_tag_filter}\n"
        f"  min_feature_version : {min_fv}\n"
        f"  gate_mode           : {args.gate_mode}\n"
        f"  include_neutral     : {bool(args.include_neutral)}\n"
        f"  max_per_bucket      : {int(args.max_per_bucket)}\n"
        f"  out                 : {args.out}\n"
        f"  val_out             : {args.val_out}\n"
        f"  val_fraction        : {float(args.val_fraction or 0.0)}\n"
        f"  shuffle             : {bool(args.shuffle)} seed={int(args.seed or 0)}\n"
        f"  dsn                 : {_redact_dsn(dsn)}\n"
        f"  ssl                 : {bool(args.ssl)}\n"
    )

    conn = await connect_pg(
        dsn,
        tries=int(args.connect_tries),
        connect_timeout=float(args.connect_timeout),
        command_timeout=float(args.command_timeout),
        use_ssl=bool(args.ssl),
        application_name="export_ai_training",
    )

    try:
        if args.connect_only:
            v = await conn.fetchval("SELECT 1")
            print("CONNECT OK:", v)
            return 0

        if args.dataset == "trades":
            rows = await fetch_trades(
                conn,
                table=trades_table,
                limit=int(args.limit),
                start_ts=start_ts,
                end_ts=end_ts,
                src_filter=src_filter,
                regime_filter=regime_filter,
                wallet_tag_filter=wallet_tag_filter,
                min_feature_version=min_fv,
                dry_run=bool(args.dry_run),
            )
        else:
            rows = await fetch_candidates_with_outcomes(
                conn,
                candidates_table=candidates_table,
                outcomes_table=outcomes_table,
                limit=int(args.limit),
                start_ts=start_ts,
                end_ts=end_ts,
                src_filter=src_filter,
                regime_filter=regime_filter,
                wallet_tag_filter=wallet_tag_filter,
                min_feature_version=min_fv,
                dry_run=bool(args.dry_run),
            )
    finally:
        await conn.close()

    if args.dry_run:
        print("\nDRY RUN complete (no DB fetch).")
        return 0

    print(f"Fetched {len(rows)} row(s).")
    if not rows:
        print("No rows returned. Check DSN, tables, and time window/filters.")
        return 0

    rng = random.Random(int(args.seed or 0)) if int(args.seed or 0) else random.Random()
    if args.shuffle:
        rng.shuffle(rows)

    examples, gate_counts, bucket_counts = _balance_examples(
        rows,
        dataset=args.dataset,
        task=args.task,
        gate_mode=args.gate_mode,
        include_neutral=bool(args.include_neutral),
        max_per_bucket=int(args.max_per_bucket),
        global_limit=int(args.limit),
        feature_keys=feature_keys,
        feature_max_chars=feature_max_chars,
    )

    if not examples:
        print("No examples survived filtering/balancing.")
        print("Gate label counts:", gate_counts)
        print("Bucket counts    :", bucket_counts)
        return 0

    if args.shuffle:
        rng.shuffle(examples)

    train, val = examples, []
    if args.val_out and float(args.val_fraction or 0.0) > 0.0:
        train, val = _stratified_split(
            examples,
            key_fn=lambda ex: str(ex.get("metadata", {}).get("label_bucket", "")),
            val_fraction=float(args.val_fraction),
            rng=rng,
        )

    _atomic_write_jsonl(args.out, train)
    print(f"Wrote {len(train)} training example(s) → {args.out}")

    if args.val_out and val:
        _atomic_write_jsonl(args.val_out, val)
        print(f"Wrote {len(val)} validation example(s) → {args.val_out}")

    print("Gate label counts:", gate_counts)
    print("Bucket counts    :", bucket_counts)

    if args.preview:
        _preview_file(args.out, max_lines=int(args.preview_count or 3))
        if args.val_out and val:
            _preview_file(args.val_out, max_lines=min(int(args.preview_count or 3), len(val)))

    return 0


def main() -> None:
    args = build_arg_parser().parse_args()
    try:
        rc = asyncio.run(_run(args))
    except KeyboardInterrupt:
        print("Interrupted.")
        rc = 130
    raise SystemExit(rc)


if __name__ == "__main__":
    main()
