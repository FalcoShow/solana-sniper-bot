# scripts/show_latest_candidates.py
from __future__ import annotations

"""
show_latest_candidates.py — Production viewer for entry candidates (V3+++)

Purpose
-------
A quant-grade, scriptable window into what the system is logging into Postgres:
- entry_candidates (required)
- entry_outcomes (optional)

You can:
- Filter by src / regime (comma-separated lists)
- Sort/filter by a score field that may live in:
    row["features"], row["raw"], or top-level row
- Join recent outcomes (label/pnl/hold)
- Print summary stats (win-rate, avg pnl, score stats)
- Emit JSON for notebooks/dashboards
- Watch mode (refresh every N seconds)

Examples
--------
python .\\scripts\\show_latest_candidates.py --limit 10
python .\\scripts\\show_latest_candidates.py --src raydium,pumpfun --with-outcomes --with-features
python .\\scripts\\show_latest_candidates.py --score-field pf_risk_adjusted_final --min-score 0.60 --summary
python .\\scripts\\show_latest_candidates.py --watch 10 --src pumpfun --with-features
python .\\scripts\\show_latest_candidates.py --json --limit 50 > latest.json
"""

import argparse
import asyncio
import inspect
import json
import os
import sys
import time
from collections.abc import Mapping, Sequence
from pathlib import Path
from statistics import mean, median
from typing import Any

# ──────────────────────────────────────────────────────────────────────────────
# Repo-root bootstrap (so running as a script always works)
# ──────────────────────────────────────────────────────────────────────────────
ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


# Explicit dotenv load (avoids python-dotenv stack-frame asserts)
def _load_dotenv_explicit(dotenv_path: str | None) -> None:
    try:
        from dotenv import load_dotenv  # type: ignore
    except Exception:
        return

    if dotenv_path:
        p = Path(dotenv_path).expanduser().resolve()
        if p.exists():
            load_dotenv(p, override=False)
        return

    p = (ROOT / ".env").resolve()
    if p.exists():
        load_dotenv(p, override=False)


# Must import after sys.path bootstrap
from utils_pkg import ai_db

# ──────────────────────────────────────────────────────────────────────────────
# Env / parsing helpers
# ──────────────────────────────────────────────────────────────────────────────
_TRUE = {"1", "true", "t", "yes", "y", "on"}
_FALSE = {"0", "false", "f", "no", "n", "off"}


def _env_bool(key: str, default: bool = False) -> bool:
    v = os.getenv(key)
    if v is None:
        return default
    s = str(v).strip().lower()
    if s in _TRUE:
        return True
    if s in _FALSE:
        return False
    return default


def _pg_enabled() -> bool:
    # Mirror the rest of your stack: Postgres is assumed ON by default
    return _env_bool("PG_ENABLED", True)


def _parse_csv(s: str) -> list[str]:
    if not s:
        return []
    out: list[str] = []
    for part in s.replace(";", ",").split(","):
        p = part.strip()
        if p:
            out.append(p)
    return out


def _safe_float(v: Any) -> float | None:
    try:
        return float(v)
    except Exception:
        return None


def _fmt_float(v: Any, digits: int = 4) -> str:
    f = _safe_float(v)
    if f is None:
        return str(v)
    return f"{f:.{digits}f}"


def _fmt_pct(v: Any, digits: int = 2) -> str:
    f = _safe_float(v)
    if f is None:
        return str(v)
    return f"{f:.{digits}f}%"


def _ts_to_human(ts_any: Any) -> str:
    """
    Accept seconds or ms; return compact UTC-ish string.
    """
    f = _safe_float(ts_any)
    if f is None or f <= 0:
        return str(ts_any)
    # heuristic: ms if > 10^11
    if f > 1e11:
        f = f / 1000.0
    try:
        return time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(f))
    except Exception:
        return str(ts_any)


# ──────────────────────────────────────────────────────────────────────────────
# ai_db call adapter (sync or async, signature-safe)
# ──────────────────────────────────────────────────────────────────────────────
async def _maybe_await(x: Any) -> Any:
    if inspect.isawaitable(x):
        return await x
    return x


def _filter_kwargs(fn: Any, kwargs: dict[str, Any]) -> dict[str, Any]:
    try:
        sig = inspect.signature(fn)
    except Exception:
        return kwargs
    ok: dict[str, Any] = {}
    for k, v in kwargs.items():
        if v is None:
            continue
        if k in sig.parameters:
            ok[k] = v
    return ok


async def _call_ai_db(fn_names: Sequence[str], **kwargs: Any) -> Any:
    """
    Try multiple function names for compatibility across ai_db versions.
    Filters kwargs by signature.
    Returns None if no function exists.
    """
    for name in fn_names:
        fn = getattr(ai_db, name, None)
        if not callable(fn):
            continue
        call_kwargs = _filter_kwargs(fn, dict(kwargs))
        try:
            return await _maybe_await(fn(**call_kwargs))
        except TypeError:
            # signature mismatch: try calling with nothing (very legacy)
            try:
                return await _maybe_await(fn())
            except Exception:
                continue
        except Exception as e:
            raise RuntimeError(f"ai_db.{name} failed: {e}") from e
    return None


# ──────────────────────────────────────────────────────────────────────────────
# Data extraction (score + fields)
# ──────────────────────────────────────────────────────────────────────────────
def _score_from_row(row: Mapping[str, Any], score_field: str) -> float | None:
    """
    Pull score from row["features"], row["raw"], or top-level row.
    """
    feats = row.get("features") or {}
    raw = row.get("raw") or {}

    if isinstance(feats, Mapping) and score_field in feats:
        v = _safe_float(feats.get(score_field))
        if v is not None:
            return v

    if isinstance(raw, Mapping) and score_field in raw:
        v = _safe_float(raw.get(score_field))
        if v is not None:
            return v

    if score_field in row:
        v = _safe_float(row.get(score_field))
        if v is not None:
            return v

    return None


def _candidate_core_fields(row: Mapping[str, Any]) -> dict[str, Any]:
    """
    Normalize common top-level fields across schema versions.
    """
    return {
        "id": row.get("id"),
        "ts": row.get("ts") or row.get("ts_ms"),
        "src": row.get("src"),
        "regime": row.get("regime"),
        "mint": row.get("mint"),
        "symbol": row.get("symbol"),
        "wallet_tag": row.get("wallet_tag") or row.get("wallet_label"),
        "feature_version": row.get("feature_version") or row.get("fv"),
    }


DEFAULT_FEATURE_KEYS = [
    # entry / PF
    "pf_momentum_score",
    "pf_final_score",
    "pf_risk_adjusted_final",
    "ai_entry_score",
    "ai_risk_score",
    "ai_fraud_score",
    "risk_capacity_score",
    # microstructure
    "micro_momo_score",
    "micro_orderflow_score",
    # regime/context (if present)
    "regime_label_numeric",
    "regime_volatility_score",
    "regime_trend_score",
    "regime_liquidity_score",
    "ctx_open_positions",
    "ctx_max_positions",
    "ctx_risk_budget_pct",
    "ctx_daily_realized_pnl_pct",
    "ctx_daily_drawdown_pct",
]


def _pick_features(row: Mapping[str, Any], keys: Sequence[str]) -> dict[str, Any]:
    feats = row.get("features") or {}
    if not isinstance(feats, Mapping):
        return {}
    out: dict[str, Any] = {}
    for k in keys:
        if k in feats:
            out[k] = feats.get(k)
    return out


# ──────────────────────────────────────────────────────────────────────────────
# Outcomes join
# ──────────────────────────────────────────────────────────────────────────────
def _build_outcome_index(outcomes: Sequence[Mapping[str, Any]]) -> dict[int, Mapping[str, Any]]:
    idx: dict[int, Mapping[str, Any]] = {}
    for row in outcomes:
        try:
            cid = int(row.get("candidate_id", 0))
        except Exception:
            continue
        if cid > 0:
            idx[cid] = row
    return idx


def _extract_outcome_bits(out: Mapping[str, Any]) -> dict[str, Any]:
    label = out.get("label") or out.get("class")
    pnl = out.get("pnl_pct") or out.get("realized_pnl_pct")
    hold = out.get("hold_sec")
    return {"label": label, "pnl_pct": pnl, "hold_sec": hold}


# ──────────────────────────────────────────────────────────────────────────────
# Printing
# ──────────────────────────────────────────────────────────────────────────────
def _print_header(title: str) -> None:
    print(title)
    print("-" * len(title))


def _print_candidate(
    row: Mapping[str, Any],
    *,
    score_field: str | None,
    score_value: float | None,
    features: Mapping[str, Any] | None,
    outcome: Mapping[str, Any] | None,
) -> None:
    core = _candidate_core_fields(row)

    line = (
        f"id={core['id']}, "
        f"ts={_ts_to_human(core['ts'])}, "
        f"src={core['src']}, "
        f"regime={core['regime']}, "
        f"mint={core['mint']}, "
        f"symbol={core['symbol']}"
    )
    if core.get("wallet_tag"):
        line += f", wallet_tag={core['wallet_tag']}"
    if core.get("feature_version"):
        line += f", fv={core['feature_version']}"

    if score_field and score_value is not None:
        line += f", {score_field}={_fmt_float(score_value, 4)}"

    if outcome:
        ob = _extract_outcome_bits(outcome)
        pnl_s = _fmt_float(ob["pnl_pct"], 2) if ob["pnl_pct"] is not None else "?"
        line += f" | outcome: label={ob['label']}, pnl_pct={pnl_s}, hold_sec={ob['hold_sec']}"

    print(line)

    if features:
        feat_bits = [f"{k}={_fmt_float(v, 4)}" for k, v in features.items()]
        if feat_bits:
            print("    features:", ", ".join(feat_bits))


def _build_summary(
    rows_scores: Sequence[tuple[Mapping[str, Any], float | None]],
    outcome_index: Mapping[int, Mapping[str, Any]],
    score_field: str | None,
) -> dict[str, Any]:
    scores: list[float] = []
    labeled = 0
    wins = 0
    pnl_list: list[float] = []
    hold_list: list[float] = []

    for row, sc in rows_scores:
        if sc is not None:
            scores.append(sc)

        try:
            cid = int(row.get("id", 0))
        except Exception:
            continue
        out = outcome_index.get(cid)
        if not out:
            continue

        labeled += 1
        pnl = _safe_float(out.get("pnl_pct") or out.get("realized_pnl_pct"))
        if pnl is not None:
            pnl_list.append(pnl)
            if pnl > 0:
                wins += 1

        hold = _safe_float(out.get("hold_sec"))
        if hold is not None and hold > 0:
            hold_list.append(hold)

    return {
        "count_candidates": len(rows_scores),
        "score_field": score_field,
        "score_mean": mean(scores) if scores else None,
        "score_min": min(scores) if scores else None,
        "score_max": max(scores) if scores else None,
        "labeled_count": labeled,
        "win_rate": (wins / labeled) if labeled else None,
        "avg_pnl_pct": mean(pnl_list) if pnl_list else None,
        "median_hold_sec": median(hold_list) if hold_list else None,
    }


def _print_summary(s: Mapping[str, Any]) -> None:
    print("")
    print("  Summary:")
    print(f"    candidates: {s.get('count_candidates')}")
    sf = s.get("score_field")
    if sf and s.get("score_mean") is not None:
        print(
            f"    {sf}: mean={_fmt_float(s.get('score_mean'), 4)}, "
            f"min={_fmt_float(s.get('score_min'), 4)}, "
            f"max={_fmt_float(s.get('score_max'), 4)}"
        )
    if s.get("labeled_count"):
        wr = s.get("win_rate")
        wr_s = _fmt_pct((wr * 100.0) if wr is not None else None) if wr is not None else "n/a"
        avgp = s.get("avg_pnl_pct")
        medh = s.get("median_hold_sec")
        print(
            f"    outcomes: labeled={s.get('labeled_count')}, "
            f"win_rate={wr_s}, "
            f"avg_pnl={_fmt_float(avgp, 2) if avgp is not None else 'n/a'}%, "
            f"median_hold_sec={_fmt_float(medh, 2) if medh is not None else 'n/a'}"
        )
    else:
        print("    outcomes: none (yet)")


# ──────────────────────────────────────────────────────────────────────────────
# Backend summary
# ──────────────────────────────────────────────────────────────────────────────
async def _backend_summary() -> None:
    print("=== show_latest_candidates.py (V3+++) ===")
    try:
        desc = await _call_ai_db(["describe_backend"])
        if desc is not None:
            print(f"[ai_db] backend: {desc}")
        else:
            print("[ai_db] backend: (no describe_backend())")
    except Exception as e:
        print(f"[ai_db] backend summary failed: {e}")
    print("")


# ──────────────────────────────────────────────────────────────────────────────
# Core runner
# ──────────────────────────────────────────────────────────────────────────────
async def _fetch_candidates(
    limit: int, src: str | None, regime: str | None
) -> list[Mapping[str, Any]]:
    rows = await _call_ai_db(
        ["fetch_latest_candidates", "get_latest_candidates", "latest_candidates"],
        limit=limit,
        src=src,
        regime=regime,
    )
    return list(rows or [])


async def _fetch_outcomes(limit: int) -> list[Mapping[str, Any]]:
    rows = await _call_ai_db(
        ["fetch_recent_outcomes", "get_recent_outcomes", "recent_outcomes"],
        limit=limit,
    )
    return list(rows or [])


async def run_once(args: argparse.Namespace) -> Any:
    if not _pg_enabled():
        msg = "PG_ENABLED=0 — Postgres logging disabled; nothing to show."
        if args.json:
            print(json.dumps({"error": msg}))
            return {"error": msg}
        print(msg)
        return None

    limit = max(1, min(int(args.limit), 500))
    src_filters = _parse_csv(args.src) or [None]
    regime_filters = _parse_csv(args.regime) or [None]

    score_field = (args.score_field or "").strip() or None
    min_score = float(args.min_score) if args.min_score is not None else None
    if min_score is not None and min_score <= 0:
        min_score = None

    # Features to show
    feature_keys = _parse_csv(args.feature_keys) or DEFAULT_FEATURE_KEYS

    # Outcomes index (optional)
    outcome_index: dict[int, Mapping[str, Any]] = {}
    if args.with_outcomes:
        # pull a larger window so joins work even if outcomes lag behind candidates
        out_rows = await _fetch_outcomes(limit=max(limit * 8, 300))
        outcome_index = _build_outcome_index(out_rows)

    all_blocks: list[dict[str, Any]] = []

    for src in src_filters:
        for regime in regime_filters:
            title = f"Latest candidates (limit={limit}"
            if src:
                title += f", src={src}"
            if regime:
                title += f", regime={regime}"
            if score_field:
                title += f", score_field={score_field}"
                if min_score is not None:
                    title += f", min_score={min_score:.3f}"
            title += ")"

            try:
                rows = await _fetch_candidates(limit=limit, src=src, regime=regime)
            except Exception as e:
                if args.json:
                    all_blocks.append({"src": src, "regime": regime, "error": str(e)})
                    continue
                _print_header(title)
                print(f"[error] {e}\n")
                continue

            if not rows:
                if args.json:
                    all_blocks.append(
                        {"src": src, "regime": regime, "candidates": [], "summary": None}
                    )
                    continue
                _print_header(title)
                print("No rows found.\n")
                continue

            rows_scores: list[tuple[Mapping[str, Any], float | None]] = []
            for row in rows:
                sc = _score_from_row(row, score_field) if score_field else None
                if min_score is not None and sc is not None and sc < min_score:
                    continue
                rows_scores.append((row, sc))

            if not rows_scores:
                if args.json:
                    all_blocks.append(
                        {"src": src, "regime": regime, "candidates": [], "summary": None}
                    )
                    continue
                _print_header(title)
                print("No candidates passed filters.\n")
                continue

            # Sort by score if requested (desc by default)
            if score_field:
                rows_scores.sort(
                    key=lambda rs: rs[1] if rs[1] is not None else -1e18, reverse=not args.asc
                )

            if not args.json:
                _print_header(title)

            block_payload: dict[str, Any] = {
                "src": src,
                "regime": regime,
                "limit": limit,
                "score_field": score_field,
                "min_score": min_score,
                "candidates": [],
                "summary": None,
            }

            for row, sc in rows_scores:
                oc: Mapping[str, Any] | None = None
                if args.with_outcomes:
                    try:
                        cid = int(row.get("id", 0))
                    except Exception:
                        cid = 0
                    if cid:
                        oc = outcome_index.get(cid)

                feats = _pick_features(row, feature_keys) if args.with_features else None

                if not args.json:
                    _print_candidate(
                        row,
                        score_field=score_field,
                        score_value=sc,
                        features=feats,
                        outcome=oc,
                    )

                block_payload["candidates"].append(
                    {
                        "row": row,
                        "score": sc,
                        "features": feats if args.with_features else None,
                        "outcome": _extract_outcome_bits(oc) if oc else None,
                    }
                )

            if args.summary:
                s = _build_summary(rows_scores, outcome_index, score_field)
                block_payload["summary"] = s
                if not args.json:
                    _print_summary(s)

            if not args.json:
                print("")

            all_blocks.append(block_payload)

    if args.json:
        print(json.dumps(all_blocks, indent=2, default=str))
        return all_blocks

    return None


# ──────────────────────────────────────────────────────────────────────────────
# CLI
# ──────────────────────────────────────────────────────────────────────────────
def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Show latest entry candidates from Postgres (V3+++).")
    p.add_argument("--dotenv", default="", help="Optional path to a .env file (explicit load).")
    p.add_argument("--limit", type=int, default=int(os.getenv("SHOW_CANDIDATES_LIMIT", "5")))
    p.add_argument(
        "--src",
        default=os.getenv("SHOW_CANDIDATES_SRC", ""),
        help="Comma list: raydium,pumpfun,...",
    )
    p.add_argument(
        "--regime",
        default=os.getenv("SHOW_CANDIDATES_REGIME", ""),
        help="Comma list: raydium_live,...",
    )
    p.add_argument(
        "--with-outcomes", action="store_true", help="Join recent outcomes (label/pnl/hold)."
    )
    p.add_argument("--with-features", action="store_true", help="Show selected feature keys.")
    p.add_argument(
        "--feature-keys",
        default=os.getenv("SHOW_CANDIDATES_FEATURE_KEYS", ""),
        help="Comma list of feature keys to print (default: built-in list).",
    )
    p.add_argument(
        "--score-field",
        default=os.getenv("SHOW_CANDIDATES_SCORE_FIELD", ""),
        help="Score key to sort/filter by (e.g. pf_risk_adjusted_final, ai_entry_score).",
    )
    p.add_argument(
        "--min-score",
        type=float,
        default=float(os.getenv("SHOW_CANDIDATES_MIN_SCORE", "0.0")),
        help="Minimum score threshold (0 disables).",
    )
    p.add_argument(
        "--asc", action="store_true", help="Sort ascending by score instead of descending."
    )
    p.add_argument("--summary", action="store_true", help="Print summary stats.")
    p.add_argument("--json", action="store_true", help="Emit JSON instead of text.")
    p.add_argument("--watch", type=int, default=int(os.getenv("SHOW_CANDIDATES_WATCH_SEC", "0")))
    p.add_argument("--no-clear", action="store_true", help="Do not clear screen in watch mode.")
    return p.parse_args()


async def main() -> int:
    args = parse_args()
    _load_dotenv_explicit(args.dotenv or None)

    await _backend_summary()

    if args.watch and args.watch > 0 and not args.json:
        interval = max(1, int(args.watch))
        try:
            while True:
                if not args.no_clear:
                    os.system("cls" if os.name == "nt" else "clear")
                await _backend_summary()
                await run_once(args)
                print(f"[watch] refreshing in {interval}s (Ctrl+C to stop)…")
                await asyncio.sleep(interval)
        except KeyboardInterrupt:
            print("\n[show_latest_candidates] stopped by user.")
            return 0
    else:
        await run_once(args)
        return 0

    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
