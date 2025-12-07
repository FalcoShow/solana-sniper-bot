# scripts/show_latest_candidates.py
from __future__ import annotations

"""
show_latest_candidates.py — Quant-grade viewer for AI entry candidates (V3+++)

Role
----
Quick, scriptable window into what the AI brain is seeing/logging in
entry_candidates (and optionally entry_outcomes). Designed to help you:

  • Inspect latest candidates from any src (raydium, pumpfun, dexscreener…)
  • Filter by:
      - src (scanner source)
      - regime (e.g. raydium_live, pumpfun_live, dexscreener_live)
      - score field (pf_risk_adjusted_final, ai_entry_score, risk_capacity_score…)
      - minimum score threshold
  • See performance context when outcomes are logged:
      - label (good/bad/hold/etc.)
      - pnl_pct / realized_pnl_pct
      - hold_sec
  • See the most important feature and context fields:
      - pf_momentum_score / pf_final_score / pf_risk_adjusted_final
      - ai_entry_score / ai_fraud_score / ai_risk_score
      - risk_capacity_score, ctx_*, regime_*, etc.
  • Summarize performance:
      - per-block stats: count, avg score, win-rate, avg pnl, median hold time
  • Export to JSON for offline notebooks and deeper quant analysis.
  • Run in “watch” mode as a lightweight live dashboard.

Examples
--------
# Show last 5 candidates (default)
python -m scripts.show_latest_candidates

# Show last 20 Raydium candidates with outcomes and features
python -m scripts.show_latest_candidates --limit 20 --src raydium --with-outcomes --with-features

# Filter by regime and enforce a minimum risk-adjusted score
python -m scripts.show_latest_candidates --regime raydium_live,pumpfun_live \
    --score-field pf_risk_adjusted_final --min-score 0.60 --summary

# Live watch for pumpfun candidates, refreshing every 10s
python -m scripts.show_latest_candidates --src pumpfun --with-features --watch 10

# Emit JSON instead of human-readable text (for notebooks / dashboards)
python -m scripts.show_latest_candidates --src dexscreener --json --limit 50
"""

import argparse
import json
import os
import sys
from pathlib import Path
from statistics import mean, median
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

# ──────────────────────────────────────────────────────────────────────────────
# PYTHONPATH / .env bootstrap
# ──────────────────────────────────────────────────────────────────────────────

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# Optional .env load so SNIPER_DB_DSN / PG_ENABLED are available locally
try:
    from dotenv import load_dotenv  # type: ignore[import]
except Exception:  # pragma: no cover
    load_dotenv = None  # type: ignore[assignment]

if load_dotenv is not None:
    env_path = ROOT / ".env"
    if env_path.exists():
        load_dotenv(env_path, override=False)

# Core DB helper (sync API)
from utils_pkg import ai_db  # noqa: E402


# ──────────────────────────────────────────────────────────────────────────────
# Env helpers
# ──────────────────────────────────────────────────────────────────────────────

def _env_bool(key: str, default: bool = False) -> bool:
    v = os.getenv(key)
    if v is None:
        return default
    return str(v).strip().lower() in {"1", "true", "yes", "y", "on"}


def _pg_enabled() -> bool:
    # Mirror PG_ENABLED semantics from data_logger
    return _env_bool("PG_ENABLED", True)


# ──────────────────────────────────────────────────────────────────────────────
# Parsing / formatting helpers
# ──────────────────────────────────────────────────────────────────────────────

def _parse_csv_strs(s: str) -> List[str]:
    out: List[str] = []
    for part in s.replace(";", ",").split(","):
        p = part.strip()
        if p:
            out.append(p)
    return out


def _fmt_float(v: Any, digits: int = 3) -> str:
    try:
        return f"{float(v):.{digits}f}"
    except Exception:
        return str(v)


def _fmt_pct(v: Any, digits: int = 2) -> str:
    try:
        return f"{float(v):.{digits}f}%"
    except Exception:
        return str(v)


def _safe_float(v: Any) -> Optional[float]:
    try:
        return float(v)
    except Exception:
        return None


def _score_from_row(
    row: Mapping[str, Any],
    score_field: str,
    fallback_field: Optional[str] = None,
) -> Optional[float]:
    """
    Pull a score out of row["features"], row["raw"], or row itself.

    score_field examples:
        pf_final_score, pf_risk_adjusted_final, ai_entry_score,
        ai_risk_score, risk_capacity_score, micro_momo_score, etc.
    """
    feats = row.get("features") or {}
    raw = row.get("raw") or {}

    # Prefer features dict
    if isinstance(feats, Mapping) and score_field in feats:
        sf = _safe_float(feats.get(score_field))
        if sf is not None:
            return sf

    # Some scores may live on raw snapshot
    if isinstance(raw, Mapping) and score_field in raw:
        sf = _safe_float(raw.get(score_field))
        if sf is not None:
            return sf

    # Fallback (e.g. ai_entry_score present as top-level)
    if fallback_field and fallback_field in row:
        sf = _safe_float(row.get(fallback_field))
        if sf is not None:
            return sf

    # Final fallback: try top-level score_field directly
    if score_field in row:
        sf = _safe_float(row.get(score_field))
        if sf is not None:
            return sf

    return None


def _build_outcome_index(
    outcomes: Sequence[Mapping[str, Any]]
) -> Dict[int, Mapping[str, Any]]:
    """
    Build candidate_id -> outcome row index.
    """
    idx: Dict[int, Mapping[str, Any]] = {}
    for row in outcomes:
        try:
            cid = int(row.get("candidate_id", 0))
        except Exception:
            continue
        if cid > 0:
            idx[cid] = row
    return idx


# ──────────────────────────────────────────────────────────────────────────────
# Backend summary / small diagnostics
# ──────────────────────────────────────────────────────────────────────────────

def backend_summary() -> None:
    print("=== show_latest_candidates.py (V3+++) ===")
    try:
        if hasattr(ai_db, "describe_backend"):
            summary = ai_db.describe_backend()
            print(f"[ai_db] backend: {summary}")
        else:
            print("[ai_db] imported but no describe_backend() helper")
    except Exception as e:
        print(f"[ai_db] backend summary failed: {e}")
    print("")


# ──────────────────────────────────────────────────────────────────────────────
# Presentation
# ──────────────────────────────────────────────────────────────────────────────

# Fields we try to show from row["features"] when --with-features is used.
# Only printed if present in the row.
INTERESTING_FEATURE_KEYS = [
    # PF / entry scoring
    "pf_momentum_score",
    "pf_final_score",
    "pf_risk_adjusted_final",
    "ai_entry_score",
    "ai_risk_score",
    "risk_capacity_score",
    "micro_momo_score",
    "micro_orderflow_score",
    # Regime & environment context
    "regime_label_numeric",
    "regime_volatility_score",
    "regime_trend_score",
    "regime_liquidity_score",
    # Risk / capacity context
    "ctx_open_positions",
    "ctx_max_positions",
    "ctx_risk_budget_pct",
    "ctx_daily_realized_pnl_pct",
    "ctx_daily_drawdown_pct",
]

# Fields on row["raw"] we show as “meta hints” if present
META_KEYS = [
    "_decision_meta",
    "_ai_signals",
    "_position_context",
    "_router_meta",
    "_risk_notes",
]


def _print_candidate_row(
    row: Mapping[str, Any],
    *,
    outcome: Optional[Mapping[str, Any]],
    show_features: bool,
    score_field: Optional[str],
) -> None:
    cid = row.get("id")
    ts = row.get("ts")
    src = row.get("src")
    mint = row.get("mint")
    symbol = row.get("symbol")
    regime = row.get("regime")
    wallet_tag = row.get("wallet_tag") or row.get("wallet_label")
    fv = row.get("feature_version") or row.get("fv")

    base_line = (
        f"id={cid}, ts={ts}, src={src}, mint={mint}, symbol={symbol}, "
        f"regime={regime}, wallet_tag={wallet_tag}, fv={fv}"
    )

    # Derived primary score (if requested)
    if score_field:
        sc = _score_from_row(
            row,
            score_field=score_field,
            fallback_field="ai_entry_score",
        )
        if sc is not None:
            base_line += f", {score_field}={_fmt_float(sc, 4)}"

    # Outcome summary (if available)
    if outcome:
        label = outcome.get("label") or outcome.get("class")
        pnl_pct = outcome.get("pnl_pct") or outcome.get("realized_pnl_pct")
        hold_sec = outcome.get("hold_sec")
        pnl_s = _fmt_float(pnl_pct, 2) if pnl_pct is not None else "?"
        base_line += (
            f" | outcome: label={label}, pnl_pct={pnl_s}, "
            f"hold_sec={hold_sec}"
        )

    print(base_line)

    if not show_features:
        return

    feats = row.get("features") or {}
    raw = row.get("raw") or {}

    # Feature subset
    feat_bits: List[str] = []
    if isinstance(feats, Mapping):
        for k in INTERESTING_FEATURE_KEYS:
            if k in feats:
                feat_bits.append(f"{k}={_fmt_float(feats[k], 4)}")

    if feat_bits:
        print("    features:", ", ".join(feat_bits))

    # Meta hints (just to tell you something interesting is logged)
    meta_bits: List[str] = []
    if isinstance(raw, Mapping):
        for k in META_KEYS:
            if k in raw:
                meta_bits.append(f"raw.{k}")

    if meta_bits:
        print("    meta:", ", ".join(meta_bits))


# ──────────────────────────────────────────────────────────────────────────────
# Block-level summary (for quant sanity checks)
# ──────────────────────────────────────────────────────────────────────────────

def _build_block_summary(
    rows_and_scores: Sequence[Tuple[Mapping[str, Any], Optional[float]]],
    outcome_index: Mapping[int, Mapping[str, Any]],
    score_field: Optional[str],
) -> Dict[str, Any]:
    """
    Compute basic stats across a block of candidates:
      • count
      • score distribution (if available)
      • outcome stats: labeled_count, win_rate, avg_pnl, median_hold_sec, etc.
    """
    n = len(rows_and_scores)
    scores: List[float] = []
    pnl_list: List[float] = []
    hold_list: List[float] = []
    wins = 0
    labeled = 0

    for row, sc in rows_and_scores:
        if sc is not None:
            scores.append(sc)

        cid = int(row.get("id", 0))
        outcome = outcome_index.get(cid)
        if not outcome:
            continue

        labeled += 1
        pnl = outcome.get("pnl_pct") or outcome.get("realized_pnl_pct")
        pnl_f = _safe_float(pnl)
        if pnl_f is not None:
            pnl_list.append(pnl_f)
            if pnl_f > 0:
                wins += 1

        hold_sec = _safe_float(outcome.get("hold_sec"))
        if hold_sec is not None and hold_sec > 0:
            hold_list.append(hold_sec)

    summary: Dict[str, Any] = {
        "count_candidates": n,
        "has_scores": bool(scores),
        "score_field": score_field,
        "score_mean": mean(scores) if scores else None,
        "score_min": min(scores) if scores else None,
        "score_max": max(scores) if scores else None,
        "labeled_count": labeled,
        "win_rate": (wins / labeled) if labeled > 0 else None,
        "avg_pnl_pct": mean(pnl_list) if pnl_list else None,
        "median_hold_sec": median(hold_list) if hold_list else None,
    }
    return summary


def _print_block_summary(summary: Mapping[str, Any]) -> None:
    n = summary.get("count_candidates", 0)
    sf = summary.get("score_field")
    print("")
    print("  Summary:")
    print(f"    candidates: {n}")
    if summary.get("has_scores") and sf:
        print(
            f"    {sf}: mean={_fmt_float(summary.get('score_mean'), 4)}, "
            f"min={_fmt_float(summary.get('score_min'), 4)}, "
            f"max={_fmt_float(summary.get('score_max'), 4)}"
        )
    labeled = summary.get("labeled_count")
    if labeled:
        win_rate = summary.get("win_rate")
        avg_pnl = summary.get("avg_pnl_pct")
        med_hold = summary.get("median_hold_sec")
        print(
            "    outcomes: labeled="
            f"{labeled}, win_rate={_fmt_pct(win_rate * 100 if win_rate is not None else None) if win_rate is not None else 'n/a'}, "
            f"avg_pnl={_fmt_float(avg_pnl, 2) if avg_pnl is not None else 'n/a'}%, "
            f"median_hold_sec={_fmt_float(med_hold, 2) if med_hold is not None else 'n/a'}"
        )
    else:
        print("    outcomes: no labeled outcomes in this block yet.")


# ──────────────────────────────────────────────────────────────────────────────
# Core logic
# ──────────────────────────────────────────────────────────────────────────────

def _fetch_candidates_for_block(
    *,
    limit: int,
    src: Optional[str],
    regime: Optional[str],
) -> List[Mapping[str, Any]]:
    """
    Small wrapper to ai_db.fetch_latest_candidates.

    Keeps the call-site ultra simple and lets us patch the behaviour later
    (e.g. switch to a joined view or stored procedure).
    """
    try:
        if hasattr(ai_db, "fetch_latest_candidates"):
            rows = ai_db.fetch_latest_candidates(
                limit=limit,
                src=src,
                regime=regime,
            )
        else:
            # Legacy fallback if your ai_db exposes a different helper
            fetch_fn = getattr(ai_db, "get_latest_candidates", None)
            if callable(fetch_fn):
                rows = fetch_fn(limit=limit, src=src, regime=regime)  # type: ignore[call-arg]
            else:
                rows = []
        return list(rows or [])
    except Exception as e:
        print(f"[ai_db] fetch_latest_candidates failed (src={src}, regime={regime}): {e}")
        return []


def run_show_candidates(
    *,
    limit: int,
    src_filter: Optional[List[str]],
    regime_filter: Optional[List[str]],
    with_outcomes: bool,
    with_features: bool,
    score_field: Optional[str],
    min_score: Optional[float],
    as_json: bool,
    show_summary: bool,
) -> None:
    """
    Main runner (sync; ai_db is sync).
    """
    # Handle PG disabled
    if not _pg_enabled():
        print("PG_ENABLED=0 — Postgres logging is disabled; nothing to show.")
        return

    src_list = src_filter or [None]  # type: ignore[list-item]
    regime_list = regime_filter or [None]  # type: ignore[list-item]

    # Preload outcome index once if needed
    outcome_index: Dict[int, Mapping[str, Any]] = {}
    if with_outcomes:
        try:
            if hasattr(ai_db, "fetch_recent_outcomes"):
                outcomes = ai_db.fetch_recent_outcomes(limit=max(limit * 5, 200))
            else:
                outcomes_fn = getattr(ai_db, "get_recent_outcomes", None)
                if callable(outcomes_fn):
                    outcomes = outcomes_fn(limit=max(limit * 5, 200))  # type: ignore[call-arg]
                else:
                    outcomes = []
            outcome_index = _build_outcome_index(outcomes)
        except Exception as e:
            print(f"[ai_db] fetch_recent_outcomes failed: {e}")
            outcome_index = {}

    all_blocks_payload: List[Dict[str, Any]] = []

    for src in src_list:
        for regime in regime_list:
            hdr = f"Latest candidates (limit={limit}"
            if src:
                hdr += f", src={src}"
            if regime:
                hdr += f", regime={regime}"
            if score_field:
                hdr += f", score_field={score_field}"
                if min_score is not None:
                    hdr += f", min_score={min_score:.3f}"
            hdr += ")"

            rows = _fetch_candidates_for_block(limit=limit, src=src, regime=regime)
            if not rows:
                if not as_json:
                    print("")
                    print(hdr)
                    print("-" * len(hdr))
                    print("No rows in entry_candidates for this filter.")
                continue

            # Compute score & apply filters
            filtered: List[Tuple[Mapping[str, Any], Optional[float]]] = []
            for row in rows:
                sc: Optional[float] = None
                if score_field:
                    sc = _score_from_row(
                        row,
                        score_field=score_field,
                        fallback_field="ai_entry_score",
                    )
                    if min_score is not None and sc is not None and sc < min_score:
                        # Below threshold – skip
                        continue
                filtered.append((row, sc))

            if not filtered:
                if not as_json:
                    print("")
                    print(hdr)
                    print("-" * len(hdr))
                    print("No candidates passed the score filters.")
                continue

            # Sort by score (desc) when available; otherwise keep original order
            if score_field:
                filtered.sort(key=lambda rs: rs[1] if rs[1] is not None else -1.0, reverse=True)

            # Print / build payload
            if not as_json:
                print("")
                print(hdr)
                print("-" * len(hdr))

            block_payload: Dict[str, Any] = {
                "src": src,
                "regime": regime,
                "limit": limit,
                "score_field": score_field,
                "min_score": min_score,
                "candidates": [],
                "summary": None,
            }

            for row, sc in filtered:
                cid = int(row.get("id", 0))
                outcome = outcome_index.get(cid) if with_outcomes else None

                if not as_json:
                    _print_candidate_row(
                        row,
                        outcome=outcome,
                        show_features=with_features,
                        score_field=score_field,
                    )

                payload_row = {
                    "row": row,
                    "score": sc,
                    "outcome": outcome,
                }
                block_payload["candidates"].append(payload_row)

            # Block-level quant summary
            if show_summary:
                summary = _build_block_summary(filtered, outcome_index, score_field)
                block_payload["summary"] = summary
                if not as_json:
                    _print_block_summary(summary)

            all_blocks_payload.append(block_payload)

            if not as_json:
                print("")

    if as_json:
        print(json.dumps(all_blocks_payload, indent=2, default=str))


# ──────────────────────────────────────────────────────────────────────────────
# CLI
# ──────────────────────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Show latest AI entry candidates from Postgres (V3+++)."
    )
    p.add_argument(
        "--limit",
        type=int,
        default=int(os.getenv("SHOW_CANDIDATES_LIMIT", "5")),
        help="Number of candidates to show per src/regime combination (default: 5 or SHOW_CANDIDATES_LIMIT).",
    )
    p.add_argument(
        "--src",
        type=str,
        default=os.getenv("SHOW_CANDIDATES_SRC", ""),
        help="Optional comma-separated src filter (e.g. raydium,pumpfun,dexscreener).",
    )
    p.add_argument(
        "--regime",
        type=str,
        default=os.getenv("SHOW_CANDIDATES_REGIME", ""),
        help="Optional comma-separated regime filter (e.g. raydium_live,pumpfun_live,dexscreener_live).",
    )
    p.add_argument(
        "--with-outcomes",
        action="store_true",
        help="Also fetch and display recent outcomes (labels / pnl / hold time).",
    )
    p.add_argument(
        "--with-features",
        action="store_true",
        help="Also display a subset of interesting feature fields for each candidate.",
    )
    p.add_argument(
        "--score-field",
        type=str,
        default=os.getenv("SHOW_CANDIDATES_SCORE_FIELD", ""),
        help=(
            "Optional score field to sort/filter by "
            "(e.g. pf_final_score, pf_risk_adjusted_final, ai_entry_score, "
            "ai_risk_score, risk_capacity_score). "
            "If omitted, rows are shown in DB order."
        ),
    )
    p.add_argument(
        "--min-score",
        type=float,
        default=float(os.getenv("SHOW_CANDIDATES_MIN_SCORE", "0.0")),
        help="Optional minimum score; candidates below this are hidden (default: 0.0).",
    )
    p.add_argument(
        "--json",
        action="store_true",
        help="Emit JSON payload instead of human-readable text.",
    )
    p.add_argument(
        "--summary",
        action="store_true",
        help="Print per-block summary stats (count, avg score, win-rate, pnl, hold time).",
    )
    p.add_argument(
        "--watch",
        type=int,
        default=int(os.getenv("SHOW_CANDIDATES_WATCH_SEC", "0")),
        help="If >0, refresh every N seconds until Ctrl+C (live dashboard mode).",
    )
    return p.parse_args()


def _run_once(args: argparse.Namespace) -> None:
    limit = max(1, min(int(args.limit), 500))
    src_filter = _parse_csv_strs(args.src) or None
    regime_filter = _parse_csv_strs(args.regime) or None

    score_field = args.score_field.strip() or None
    min_score = float(args.min_score) if args.min_score is not None else None
    # Treat min_score==0 as “no filter” when not explicitly set via CLI/env
    if min_score is not None and min_score <= 0:
        min_score = None

    run_show_candidates(
        limit=limit,
        src_filter=src_filter,
        regime_filter=regime_filter,
        with_outcomes=args.with_outcomes,
        with_features=args.with_features,
        score_field=score_field,
        min_score=min_score,
        as_json=args.json,
        show_summary=args.summary,
    )


def main() -> None:
    args = parse_args()
    backend_summary()

    if args.watch and args.watch > 0 and not args.json:
        # Live dashboard mode (text only)
        interval = max(1, int(args.watch))
        try:
            while True:
                os.system("cls" if os.name == "nt" else "clear")
                backend_summary()
                _run_once(args)
                print(f"\n[watch] refreshing in {interval}s (Ctrl+C to stop)…")
                try:
                    import time
                    time.sleep(interval)
                except KeyboardInterrupt:
                    raise
        except KeyboardInterrupt:
            print("\n[show_latest_candidates] stopped by user.")
    else:
        _run_once(args)


if __name__ == "__main__":
    main()
