from __future__ import annotations

"""
scripts/export_ai_training.py

Quant-grade exporter for AI training data (V3+++).
=================================================

Goals
-----
• Pull labeled outcomes from Postgres (trades or entry_candidates+entry_outcomes).
• Turn them into **chat-style JSONL** examples for OpenAI fine-tuning.
• Bake in:
    - PnL-based bucket + GOOD/BAD/NEUTRAL gate labels
    - Rich feature snippets (FeatureBuilder V3+++ outputs)
    - Regime/src filters, feature_version gates, wallet tags
    - Days-back / start-end date windows
    - Class balancing / per-bucket caps
    - Optional shuffling & deterministic seeding
    - Optional train/val split

Output format
-------------
Each line is a JSON object:

    {
      "messages": [
        {"role": "system", "content": "..."},
        {"role": "user", "content": "..."},
        {"role": "assistant", "content": "{\"label\": \"GOOD\", \"score\": 0.97}"}
      ],
      "metadata": {
        "label_bucket": "monster_win",
        "gate_label": "GOOD",
        "raw_label": "win",
        "pnl_usd": 123.45,
        "pnl_pct": 240.0,
        "src": "pumpfun",
        "regime": "pumpfun_live",
        "symbol": "DOG",
        "mint": "...",
        "side": "BUY",
        "wallet_tag": "main",
        "feature_version": 3,
        "dataset": "entry_gate",
        "task": "entry_gate_v3"
      }
    }

Usage examples
--------------
# default: trades→entry_gate, 2000 rows, single JSONL
    python -m scripts.export_ai_training

# candidates dataset, only pumpfun_live, min feature_version=3, skip NEUTRAL
    AI_EXPORT_SOURCE=candidates \
    AI_MIN_FEATURE_VERSION=3 \
    AI_INCLUDE_NEUTRAL=0 \
    AI_EXPORT_REGIME=pumpfun_live \
        python -m scripts.export_ai_training --limit 5000

# candidates dataset, src pumpfun, 60 days back, balanced per bucket, train/val split
    AI_EXPORT_SOURCE=candidates \
    AI_DAYS_BACK=60 \
    AI_MAX_PER_BUCKET=2000 \
        python -m scripts.export_ai_training \
            --limit 8000 \
            --out data/entry_gate_train.jsonl \
            --val-out data/entry_gate_val.jsonl \
            --val-fraction 0.15 \
            --shuffle \
            --seed 42
"""

import argparse
import asyncio
import datetime as dt
import json
import os
import random
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import asyncpg

# ---------------------------------------------------------------------------
# Env helpers / DSN selection
# ---------------------------------------------------------------------------


def _env_str(key: str, default: str = "") -> str:
    return os.getenv(key, default).strip()


def _env_int(key: str, default: int) -> int:
    raw = os.getenv(key)
    if raw is None or raw == "":
        return default
    try:
        return int(raw)
    except Exception:
        return default


def _env_float(key: str, default: float) -> float:
    raw = os.getenv(key)
    if raw is None or raw == "":
        return default
    try:
        return float(raw)
    except Exception:
        return default


def _env_bool(key: str, default: bool = False) -> bool:
    raw = os.getenv(key)
    if raw is None or raw == "":
        return default
    return raw.strip().lower() in {"1", "true", "yes", "y", "on"}


def _pick_dsn() -> str:
    """
    Prefer SNIPER_DB_DSN if present, otherwise fall back to PNL_DSN / PG_DSN.
    """
    for key in ("SNIPER_DB_DSN", "PNL_DSN", "PG_DSN"):
        v = os.getenv(key)
        if v:
            return v
    raise SystemExit("No SNIPER_DB_DSN/PNL_DSN/PG_DSN set in env – cannot export.")


# ---------------------------------------------------------------------------
# Defaults / thresholds (tunable via env)
# ---------------------------------------------------------------------------

DEFAULT_OUT_PATH = _env_str("AI_TRAINING_PATH", "ai_training.jsonl")
DEFAULT_LIMIT = _env_int("AI_TRAINING_LIMIT", 2000)

# PnL → bucket thresholds (pct)
MONSTER_WIN_PCT = _env_float("AI_MONSTER_WIN_PCT", 40.0)
GOOD_PNL_PCT = _env_float("AI_GOOD_PNL_PCT", 10.0)
BAD_PNL_PCT = _env_float("AI_BAD_PNL_PCT", -10.0)
RUG_PNL_PCT = _env_float("AI_RUG_PNL_PCT", -40.0)

# Fallback USD thresholds
GOOD_PNL_USD = _env_float("AI_GOOD_PNL_USD", 5.0)
BAD_PNL_USD = _env_float("AI_BAD_PNL_USD", -3.0)

# Time window defaults
DEFAULT_DAYS_BACK = _env_int("AI_DAYS_BACK", 90)

# Class balancing
DEFAULT_MAX_PER_BUCKET = _env_int("AI_MAX_PER_BUCKET", 0)  # 0 = no cap

# Gate mode: how we combine outcome label vs PnL buckets
DEFAULT_GATE_MODE = _env_str("AI_GATE_MODE", "pnl+label")  # pnl|label|pnl+label

# Include NEUTRAL?
DEFAULT_INCLUDE_NEUTRAL = _env_bool("AI_INCLUDE_NEUTRAL", True)

# Minimum feature_version for candidates/trades features
DEFAULT_MIN_FEAT_VERSION = _env_int("AI_MIN_FEATURE_VERSION", 0)


# ---------------------------------------------------------------------------
# Date helpers
# ---------------------------------------------------------------------------


def _parse_date(s: str) -> Optional[dt.datetime]:
    s = s.strip()
    if not s:
        return None
    try:
        if "T" in s:
            return dt.datetime.fromisoformat(s)
        return dt.datetime.strptime(s, "%Y-%m-%d")
    except Exception:
        return None


def _resolve_date_range(
    days_back: int,
    start_date: str,
    end_date: str,
) -> Tuple[Optional[dt.datetime], Optional[dt.datetime]]:
    """
    Resolve (start_ts, end_ts) from days_back or explicit start/end.
    """
    start_ts = _parse_date(start_date)
    end_ts = _parse_date(end_date)

    if start_ts and end_ts:
        # If end has no time, treat as inclusive day end.
        if end_ts.time() == dt.time.min:
            end_ts = end_ts + dt.timedelta(days=1)
        return start_ts, end_ts

    if days_back > 0:
        end_ts = dt.datetime.utcnow().replace(tzinfo=dt.timezone.utc)
        start_ts = end_ts - dt.timedelta(days=days_back)
        return start_ts, end_ts

    return None, None


# ---------------------------------------------------------------------------
# Labeling logic (PnL → bucket → gate label)
# ---------------------------------------------------------------------------


def _to_float(x: Any) -> float:
    try:
        return float(x)
    except Exception:
        return 0.0


def _to_optional_float(x: Any) -> Optional[float]:
    if x is None:
        return None
    try:
        return float(x)
    except Exception:
        return None


def _pick_bucket(pnl_usd: float, pnl_pct: Optional[float]) -> str:
    """
    Quant-ish bucket based on PnL. Uses pct if available, falls back to USD.
    Buckets: monster_win, win, scratch, loss, rug
    """
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

    # Fallback to USD-only
    if pnl_usd >= GOOD_PNL_USD * 4:
        return "monster_win"
    if pnl_usd >= GOOD_PNL_USD:
        return "win"
    if pnl_usd <= BAD_PNL_USD * 4:
        return "rug"
    if pnl_usd <= BAD_PNL_USD:
        return "loss"
    return "scratch"


def _bucket_to_gate(bucket: str) -> Tuple[str, float]:
    """
    Map bucket → (GOOD/BAD/NEUTRAL, confidence score 0..1).
    """
    if bucket in ("monster_win", "win"):
        return ("GOOD", 0.98 if bucket == "monster_win" else 0.93)
    if bucket in ("loss", "rug"):
        return ("BAD", 0.98 if bucket == "rug" else 0.90)
    return ("NEUTRAL", 0.55)


def _merge_label_sources(
    *,
    bucket_gate_label: str,
    bucket_score: float,
    raw_label: Optional[str],
    gate_mode: str,
) -> Tuple[str, float, str]:
    """
    Combine PnL-derived gate label with any explicit label/outcome_label.

    gate_mode:
        - "pnl"       → ignore raw_label, use bucket_gate_label
        - "label"     → if raw_label maps to gate label use that, else bucket_gate_label
        - "pnl+label" → if raw_label is strong (e.g. explicit 'rug'), bias toward it
    """
    gate_mode = (gate_mode or "pnl+label").lower()

    # Normalize raw_label into possible gate label
    raw = (raw_label or "").strip().lower()
    raw_gate: Optional[str] = None
    if raw in {"good", "ok", "buy", "entry_ok", "green"}:
        raw_gate = "GOOD"
    elif raw in {"bad", "avoid", "no_buy", "red"}:
        raw_gate = "BAD"
    elif raw in {"neutral", "meh"}:
        raw_gate = "NEUTRAL"
    elif raw in {"rug", "scam"}:
        raw_gate = "BAD"
    elif raw in {"monster_win", "win"}:
        raw_gate = "GOOD"
    # else: unknown → None

    # Mode: pnl only
    if gate_mode == "pnl":
        return bucket_gate_label, bucket_score, raw_label or bucket_gate_label

    # Mode: label only – fall back to pnl if we cannot map
    if gate_mode == "label":
        if raw_gate is not None:
            # Give slightly higher confidence if explicit label present.
            score = max(bucket_score, 0.92)
            return raw_gate, score, raw_label or raw_gate
        return bucket_gate_label, bucket_score, raw_label or bucket_gate_label

    # Mode: pnl+label
    if raw_gate is None:
        return bucket_gate_label, bucket_score, raw_label or bucket_gate_label

    # If raw gate matches bucket gate → boost confidence
    if raw_gate == bucket_gate_label:
        score = min(1.0, bucket_score + 0.03)
        return raw_gate, score, raw_label or raw_gate

    # Disagreement: soft compromise – but prefer explicit strong labels (e.g. rug)
    if raw in {"rug", "scam"} and raw_gate == "BAD":
        return "BAD", max(bucket_score, 0.97), raw_label or raw_gate

    # Otherwise favor PnL but down-weight confidence a bit
    mixed_score = max(0.7, min(bucket_score, 0.9))
    return bucket_gate_label, mixed_score, raw_label or bucket_gate_label


def build_label_info(row: Dict[str, Any], gate_mode: str) -> Dict[str, Any]:
    """
    Compute:
      - pnl_usd / pnl_pct
      - bucket
      - gate_label (GOOD/BAD/NEUTRAL)
      - score (0..1)
      - raw_label (if present)
    """
    pnl_usd = _to_float(
        row.get("pnl_usd")
        or row.get("realized_usd")
        or row.get("net_pnl_usd")
        or 0.0
    )
    pnl_pct = _to_optional_float(row.get("pnl_pct"))
    bucket = _pick_bucket(pnl_usd, pnl_pct)
    base_gate_label, base_score = _bucket_to_gate(bucket)

    raw_label = row.get("label") or row.get("outcome_label") or row.get("ai_label")
    gate_label, score, merged_label = _merge_label_sources(
        bucket_gate_label=base_gate_label,
        bucket_score=base_score,
        raw_label=raw_label,
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


# ---------------------------------------------------------------------------
# Example builder (chat JSONL)
# ---------------------------------------------------------------------------


def _format_ts(ts: Any) -> str:
    if isinstance(ts, dt.datetime):
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=dt.timezone.utc)
        return ts.isoformat()
    return str(ts)


def _format_maybe_float(v: Any) -> str:
    f = _to_optional_float(v)
    if f is None:
        return "null"
    return f"{f:.3f}"


def _build_feature_snippet(row: Dict[str, Any]) -> str:
    """
    Build a compact feature snapshot string from the 'features' JSON column.
    """
    feats = row.get("features") or {}
    if not isinstance(feats, dict):
        return ""

    interesting_keys = [
        # PairFilter / momentum
        "pf_momentum_score",
        "pf_final_score",
        "pf_risk_adjusted_final",
        # AI / fraud / risk overlay
        "ai_entry_score",
        "ai_fraud_score",
        "ai_risk_score",
        # Context / risk_manager
        "risk_capacity_score",
        "ctx_open_positions",
        "ctx_max_positions",
        "ctx_daily_realized_pnl_pct",
        "ctx_daily_drawdown_pct",
        # Microstructure
        "vol_5m",
        "buys_5m",
        "sells_5m",
        "liq_usd",
        # Smart wallets
        "whale_buy_score",
        "alpha_wallet_score",
    ]

    bits: List[str] = []
    for k in interesting_keys:
        if k in feats:
            bits.append(f"{k}={feats[k]!r}")

    return ", ".join(bits)


def _base_system_prompt(task: str) -> str:
    """
    Task-specific system prompt text.
    Currently we unify around an entry_gate / trade-quality task, but the
    'task' string is kept so we can extend to fraud / risk / exit later.
    """
    task = task or "entry_gate_v3"
    return (
        "You are an AI assistant for a Solana meme-coin sniper bot.\n"
        "You receive a compact snapshot of a historical trade or entry "
        "candidate plus its realized outcome.\n\n"
        "Your job is to decide whether **similar future trades** should be "
        "considered GOOD candidates, BAD candidates, or NEUTRAL (meh) for the "
        "strategy's entry gate.\n\n"
        "Always respond with a single JSON object:\n"
        '  { "label": "GOOD|BAD|NEUTRAL", "score": number_between_0_and_1 }\n'
        "- 'label' is the gate decision for this type of setup.\n"
        "- 'score' is your confidence (0..1) that similar setups should be "
        "allowed through the entry gate.\n"
        "Do not add explanations or extra fields.\n\n"
        f"Task profile: {task}"
    )


def build_chat_example(
    row: Dict[str, Any],
    label_info: Dict[str, Any],
    *,
    dataset: str,
    task: str,
) -> Dict[str, Any]:
    """
    Build a single fine-tuning example in chat format.
    """
    ts_str = _format_ts(row.get("ts"))
    src = row.get("src") or row.get("source") or "trades"
    regime = row.get("regime") or "unknown_regime"
    symbol = row.get("symbol") or row.get("sym") or row.get("token")
    mint = row.get("mint") or row.get("token") or "unknown_mint"
    side = row.get("side") or row.get("direction") or "BUY"

    wallet_tag = row.get("wallet_tag")
    feature_version = row.get("feature_version")
    risk_profile = row.get("risk_profile")
    exit_policy_name = row.get("exit_policy_name")

    pnl_usd = label_info["pnl_usd"]
    pnl_pct = label_info.get("pnl_pct")
    hold_sec = row.get("hold_sec") or row.get("hold_time_sec")

    reason = (
        row.get("reason")
        or row.get("entry_reason")
        or row.get("exit_reason")
        or row.get("notes")
        or ""
    )

    feature_snippet = _build_feature_snippet(row)

    bucket = label_info["bucket"]
    label = label_info["label"]
    gate_label = label_info["gate_label"]

    # -------- system & user messages --------
    system_msg = _base_system_prompt(task)

    lines: List[str] = []
    lines.append("Here is a past trade / candidate outcome snapshot.")
    lines.append(f"Timestamp: {ts_str}")
    lines.append(f"Source: {src} | Regime: {regime}")
    lines.append(f"Side: {side} | Symbol: {symbol} | Mint: {mint}")

    if wallet_tag:
        lines.append(f"Wallet tag: {wallet_tag}")
    if feature_version is not None:
        lines.append(f"Feature version: {feature_version}")
    if risk_profile:
        lines.append(f"Risk profile at entry: {risk_profile}")
    if exit_policy_name:
        lines.append(f"Exit policy: {exit_policy_name}")

    pnl_pct_str = _format_maybe_float(pnl_pct)
    lines.append(
        f"Realized PnL: pnl_usd={pnl_usd:.3f}, pnl_pct={pnl_pct_str}"
    )

    if hold_sec is not None:
        lines.append(f"Hold time (seconds): {hold_sec}")

    if reason:
        lines.append(f"Reason / notes: {reason}")

    if feature_snippet:
        lines.append("Feature snapshot: " + feature_snippet)

    lines.append(
        "Bucketed outcome class (for your reference only): "
        f"{bucket} (raw_label={label}, gate_label={gate_label})."
    )
    lines.append(
        'Respond ONLY with a JSON object like '
        '{"label": "GOOD|BAD|NEUTRAL", "score": 0.0-1.0}.'
    )

    user_content = "\n".join(lines)

    assistant_msg = json.dumps(
        {
            "label": gate_label,
            "score": float(label_info["score"]),
        }
    )

    example = {
        "messages": [
            {"role": "system", "content": system_msg},
            {"role": "user", "content": user_content},
            {"role": "assistant", "content": assistant_msg},
        ],
        "metadata": {
            "label_bucket": bucket,
            "gate_label": gate_label,
            "raw_label": label,
            "pnl_usd": label_info["pnl_usd"],
            "pnl_pct": label_info.get("pnl_pct"),
            "src": src,
            "regime": regime,
            "symbol": symbol,
            "mint": mint,
            "side": side,
            "wallet_tag": wallet_tag,
            "feature_version": feature_version,
            "dataset": dataset,
            "task": task,
        },
    }
    return example


# ---------------------------------------------------------------------------
# Query helpers
# ---------------------------------------------------------------------------


async def fetch_trades(
    conn: asyncpg.Connection,
    *,
    limit: int,
    start_ts: Optional[dt.datetime],
    end_ts: Optional[dt.datetime],
    src_filter: Optional[str],
    regime_filter: Optional[str],
    min_feature_version: Optional[int],
) -> Sequence[asyncpg.Record]:
    """
    trade schema (assumed):
      trades(
          ts, src, regime, symbol, mint, side,
          pnl_usd, pnl_pct, hold_sec,
          wallet_tag, feature_version, features jsonb, ...
      )
    """
    conditions: List[str] = ["pnl_usd IS NOT NULL"]
    params: List[Any] = []
    idx = 1

    if start_ts is not None:
        conditions.append(f"ts >= ${idx}")
        params.append(start_ts)
        idx += 1
    if end_ts is not None:
        conditions.append(f"ts < ${idx}")
        params.append(end_ts)
        idx += 1
    if src_filter:
        conditions.append(f"src = ${idx}")
        params.append(src_filter)
        idx += 1
    if regime_filter:
        conditions.append(f"regime = ${idx}")
        params.append(regime_filter)
        idx += 1
    if min_feature_version is not None and min_feature_version > 0:
        conditions.append(f"COALESCE(feature_version, 0) >= ${idx}")
        params.append(min_feature_version)
        idx += 1

    where_clause = " AND ".join(conditions) if conditions else "TRUE"

    sql = f"""
        SELECT *
        FROM trades
        WHERE {where_clause}
        ORDER BY ts DESC
        LIMIT ${idx};
    """
    params.append(limit)
    return await conn.fetch(sql, *params)


async def fetch_candidates_with_outcomes(
    conn: asyncpg.Connection,
    *,
    limit: int,
    start_ts: Optional[dt.datetime],
    end_ts: Optional[dt.datetime],
    src_filter: Optional[str],
    regime_filter: Optional[str],
    min_feature_version: Optional[int],
) -> Sequence[asyncpg.Record]:
    """
    Join entry_candidates + entry_outcomes, newest first.
    Assumes a schema like:
      entry_candidates(
          id, ts, src, regime, mint, symbol,
          wallet_tag, feature_version, features jsonb,
          risk_profile, exit_policy_name, ...
      )
      entry_outcomes(
          candidate_id, label, pnl_pct, pnl_usd, hold_sec, ...
      )
    """
    conditions: List[str] = ["(o.pnl_usd IS NOT NULL OR o.pnl_pct IS NOT NULL)"]
    params: List[Any] = []
    idx = 1

    if start_ts is not None:
        conditions.append(f"c.ts >= ${idx}")
        params.append(start_ts)
        idx += 1
    if end_ts is not None:
        conditions.append(f"c.ts < ${idx}")
        params.append(end_ts)
        idx += 1
    if src_filter:
        conditions.append(f"c.src = ${idx}")
        params.append(src_filter)
        idx += 1
    if regime_filter:
        conditions.append(f"c.regime = ${idx}")
        params.append(regime_filter)
        idx += 1
    if min_feature_version is not None and min_feature_version > 0:
        conditions.append(f"COALESCE(c.feature_version, 0) >= ${idx}")
        params.append(min_feature_version)
        idx += 1

    where_clause = " AND ".join(conditions) if conditions else "TRUE"

    sql = f"""
        SELECT
            c.id              AS candidate_id,
            c.ts              AS ts,
            c.src             AS src,
            c.regime          AS regime,
            c.mint            AS mint,
            c.symbol          AS symbol,
            c.wallet_tag      AS wallet_tag,
            c.feature_version AS feature_version,
            c.features        AS features,
            c.risk_profile    AS risk_profile,
            c.exit_policy_name AS exit_policy_name,
            o.label           AS outcome_label,
            o.pnl_pct         AS pnl_pct,
            o.pnl_usd         AS pnl_usd,
            o.hold_sec        AS hold_sec
        FROM entry_candidates c
        JOIN entry_outcomes  o
          ON o.candidate_id = c.id
        WHERE {where_clause}
        ORDER BY c.ts DESC
        LIMIT ${idx};
    """
    params.append(limit)
    return await conn.fetch(sql, *params)


# ---------------------------------------------------------------------------
# CLI / argument parsing
# ---------------------------------------------------------------------------


def build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Export AI training data from Postgres into OpenAI chat JSONL."
    )

    # Dataset source (schema-level)
    p.add_argument(
        "--dataset",
        choices=["trades", "candidates"],
        default=_env_str("AI_EXPORT_SOURCE", "trades") or "trades",
        help=(
            '"trades" uses the trades table; '
            '"candidates" joins entry_candidates + entry_outcomes.'
        ),
    )

    # Abstract task name (mainly for system prompt + metadata)
    p.add_argument(
        "--task",
        type=str,
        default=_env_str("AI_TASK_NAME", "entry_gate_v3"),
        help="Logical task name (for system prompt flavor + metadata.task).",
    )

    # Limits / time windows
    p.add_argument(
        "--limit",
        type=int,
        default=DEFAULT_LIMIT,
        help=f"Max rows to consider from DB (default: {DEFAULT_LIMIT}, env: AI_TRAINING_LIMIT).",
    )
    p.add_argument(
        "--days-back",
        type=int,
        default=_env_int("AI_DAYS_BACK", DEFAULT_DAYS_BACK),
        help="Lookback window in days if no explicit start/end given (0 = disabled).",
    )
    p.add_argument(
        "--start-date",
        type=str,
        default=_env_str("AI_START_DATE", ""),
        help="Optional start date (YYYY-MM-DD or ISO); overrides days-back if paired with end-date.",
    )
    p.add_argument(
        "--end-date",
        type=str,
        default=_env_str("AI_END_DATE", ""),
        help="Optional end date (YYYY-MM-DD or ISO).",
    )

    # Filters (src/regime/feature_version/wallet tag)
    p.add_argument(
        "--src",
        type=str,
        default=_env_str("AI_EXPORT_SRC", ""),
        help='Optional src filter (e.g. "raydium","pumpfun").',
    )
    p.add_argument(
        "--regime",
        type=str,
        default=_env_str("AI_EXPORT_REGIME", ""),
        help='Optional regime filter (e.g. "raydium_live","pumpfun_live").',
    )
    p.add_argument(
        "--min-feature-version",
        type=int,
        default=_env_int("AI_MIN_FEATURE_VERSION", DEFAULT_MIN_FEAT_VERSION),
        help="Only include rows with feature_version >= this (0 = disabled).",
    )

    # Gate & label options
    p.add_argument(
        "--gate-mode",
        choices=["pnl", "label", "pnl+label"],
        default=_env_str("AI_GATE_MODE", DEFAULT_GATE_MODE),
        help='How to combine PnL-derived buckets with explicit labels: "pnl", "label", "pnl+label".',
    )
    p.add_argument(
        "--include-neutral",
        action="store_true",
        default=_env_bool("AI_INCLUDE_NEUTRAL", DEFAULT_INCLUDE_NEUTRAL),
        help="Include NEUTRAL gate_label examples (default from AI_INCLUDE_NEUTRAL).",
    )
    p.add_argument(
        "--max-per-bucket",
        type=int,
        default=_env_int("AI_MAX_PER_BUCKET", DEFAULT_MAX_PER_BUCKET),
        help="Max examples per bucket (monster_win/win/scratch/loss/rug). 0 = no cap.",
    )

    # Output files
    p.add_argument(
        "--out",
        type=str,
        default=DEFAULT_OUT_PATH,
        help=f"Primary output JSONL path (default: {DEFAULT_OUT_PATH}).",
    )
    p.add_argument(
        "--val-out",
        type=str,
        default=_env_str("AI_VAL_PATH", ""),
        help="Optional validation JSONL path. If set, we will split off a validation subset.",
    )
    p.add_argument(
        "--val-fraction",
        type=float,
        default=_env_float("AI_VAL_FRACTION", 0.0),
        help="Fraction of examples to send to validation file (0..0.5, 0 = no split).",
    )

    # Shuffling / determinism
    p.add_argument(
        "--shuffle",
        action="store_true",
        default=_env_bool("AI_SHUFFLE", True),
        help="Shuffle examples before export (recommended).",
    )
    p.add_argument(
        "--seed",
        type=int,
        default=_env_int("AI_SEED", 0),
        help="Random seed for shuffling / sampling (0 = nondeterministic).",
    )

    # Preview / verbosity
    p.add_argument(
        "--no-preview",
        action="store_true",
        help="Do not print the first few examples after export.",
    )
    p.add_argument(
        "--preview-count",
        type=int,
        default=_env_int("AI_PREVIEW_COUNT", 3),
        help="How many lines to preview (if preview is enabled).",
    )

    return p


# ---------------------------------------------------------------------------
# Core export logic
# ---------------------------------------------------------------------------


def _balance_and_filter_examples(
    rows: Iterable[Dict[str, Any]],
    *,
    dataset_name: str,
    task_name: str,
    gate_mode: str,
    include_neutral: bool,
    max_per_bucket: int,
    global_limit: int,
) -> Tuple[List[Dict[str, Any]], Dict[str, int], Dict[str, int]]:
    """
    Convert raw DB rows → labeled chat examples with:
      - GOOD/BAD/NEUTRAL gate labels
      - Optional NEUTRAL exclusion
      - Optional per-bucket caps
      - Global limit

    Returns:
      (examples, gate_label_counts, bucket_counts)
    """
    examples: List[Dict[str, Any]] = []
    gate_counts: Dict[str, int] = {}
    bucket_counts: Dict[str, int] = {}

    per_bucket_caps: Dict[str, int] = {}
    if max_per_bucket > 0:
        per_bucket_caps = {
            "monster_win": max_per_bucket,
            "win": max_per_bucket,
            "scratch": max_per_bucket,
            "loss": max_per_bucket,
            "rug": max_per_bucket,
        }

    for row in rows:
        if global_limit > 0 and len(examples) >= global_limit:
            break

        label_info = build_label_info(row, gate_mode=gate_mode)
        bucket = label_info["bucket"]
        gate_label = label_info["gate_label"]

        # NEUTRAL exclusion
        if not include_neutral and gate_label == "NEUTRAL":
            continue

        # Per-bucket caps (on outcome buckets, not gate labels)
        if per_bucket_caps:
            used = bucket_counts.get(bucket, 0)
            cap = per_bucket_caps.get(bucket, max_per_bucket)
            if used >= cap:
                continue

        ex = build_chat_example(
            row,
            label_info,
            dataset=dataset_name,
            task=task_name,
        )
        examples.append(ex)

        # Stats
        bucket_counts[bucket] = bucket_counts.get(bucket, 0) + 1
        gate_counts[gate_label] = gate_counts.get(gate_label, 0) + 1

    return examples, gate_counts, bucket_counts


def _split_train_val(
    examples: List[Dict[str, Any]],
    *,
    val_fraction: float,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    if not examples or val_fraction <= 0.0:
        return examples, []

    val_fraction = max(0.0, min(0.5, val_fraction))
    n_total = len(examples)
    n_val = max(1, int(n_total * val_fraction))
    if n_val >= n_total:
        n_val = max(1, n_total - 1)

    val_examples = examples[:n_val]
    train_examples = examples[n_val:]
    return train_examples, val_examples


def _write_jsonl(path: str, examples: Sequence[Dict[str, Any]]) -> None:
    with open(path, "w", encoding="utf-8") as f:
        for ex in examples:
            line = json.dumps(ex, ensure_ascii=False, default=str)
            f.write(line + "\n")


def _preview_file(path: str, max_lines: int) -> None:
    print(f"\nPreview of {path} (first {max_lines} lines):")
    try:
        with open(path, "r", encoding="utf-8") as f:
            for i in range(max_lines):
                line = f.readline()
                if not line:
                    break
                print(line.rstrip())
    except FileNotFoundError:
        print("  (file not found)")
    except Exception as e:
        print(f"  (preview failed: {e})")


async def _run_export(args: argparse.Namespace) -> None:
    dsn = _pick_dsn()
    conn = await asyncpg.connect(dsn)
    try:
        print(f"Connected to Postgres via DSN={dsn!r}")

        start_ts, end_ts = _resolve_date_range(
            days_back=args.days_back,
            start_date=args.start_date,
            end_date=args.end_date,
        )
        src_filter = args.src.strip() or None
        regime_filter = args.regime.strip() or None
        min_feature_version = (
            args.min_feature_version if args.min_feature_version > 0 else None
        )

        print(
            "Export query params:\n"
            f"  dataset            : {args.dataset}\n"
            f"  limit              : {args.limit}\n"
            f"  start_ts           : {start_ts}\n"
            f"  end_ts             : {end_ts}\n"
            f"  src_filter         : {src_filter}\n"
            f"  regime_filter      : {regime_filter}\n"
            f"  min_feature_version: {min_feature_version}\n"
            f"  gate_mode          : {args.gate_mode}\n"
            f"  include_neutral    : {bool(args.include_neutral)}\n"
            f"  max_per_bucket     : {args.max_per_bucket}\n"
        )

        # Fetch raw rows
        if args.dataset == "trades":
            rows = await fetch_trades(
                conn,
                limit=args.limit,
                start_ts=start_ts,
                end_ts=end_ts,
                src_filter=src_filter,
                regime_filter=regime_filter,
                min_feature_version=min_feature_version,
            )
        else:
            rows = await fetch_candidates_with_outcomes(
                conn,
                limit=args.limit,
                start_ts=start_ts,
                end_ts=end_ts,
                src_filter=src_filter,
                regime_filter=regime_filter,
                min_feature_version=min_feature_version,
            )
    finally:
        await conn.close()

    print(f"Fetched {len(rows)} rows from dataset='{args.dataset}'")

    # Convert asyncpg.Record → dict
    row_dicts: List[Dict[str, Any]] = [dict(r) for r in rows]

    # Shuffle before balancing / limiting? We'll use the seed if provided.
    if args.seed:
        random.seed(args.seed)
    if args.shuffle:
        random.shuffle(row_dicts)

    # Build examples with label/bucket info + caps
    examples, gate_counts, bucket_counts = _balance_and_filter_examples(
        row_dicts,
        dataset_name=args.dataset,
        task_name=args.task,
        gate_mode=args.gate_mode,
        include_neutral=bool(args.include_neutral),
        max_per_bucket=args.max_per_bucket,
        global_limit=args.limit,
    )

    if not examples:
        print("No training examples survived filtering – nothing to export.")
        print("Label counts:", gate_counts)
        print("Bucket counts:", bucket_counts)
        return

    # Shuffle again after balancing (so train/val split is randomized)
    if args.shuffle:
        if args.seed:
            random.seed(args.seed + 1)
        random.shuffle(examples)

    # Train / val split (optional)
    train_examples = examples
    val_examples: List[Dict[str, Any]] = []
    if args.val_out and args.val_fraction > 0.0:
        train_examples, val_examples = _split_train_val(
            examples,
            val_fraction=args.val_fraction,
        )

    # Write files
    _write_jsonl(args.out, train_examples)
    print(f"Wrote {len(train_examples)} training examples to {args.out}")

    if args.val_out and val_examples:
        _write_jsonl(args.val_out, val_examples)
        print(f"Wrote {len(val_examples)} validation examples to {args.val_out}")

    print("Gate label counts:", gate_counts)
    print("Bucket counts    :", bucket_counts)

    # Preview a few lines
    if not args.no_preview:
        _preview_file(args.out, max_lines=args.preview_count)
        if args.val_out and val_examples:
            _preview_file(args.val_out, max_lines=min(args.preview_count, len(val_examples)))


def main() -> None:
    parser = build_arg_parser()
    args = parser.parse_args()
    asyncio.run(_run_export(args))


if __name__ == "__main__":
    main()
