from __future__ import annotations

"""
scripts/test_ai_logging.py

Quant-grade smoke test for the AI logging + training stack (V3+++).
===================================================================

Purpose
-------
End-to-end sanity check that our **AI logging + training pipeline** is wired:

  • PG / ai_db connectivity works.
  • data_logger can:
      - insert entry_candidates (BUY/SKIP/NONE),
      - patch decision metadata (JSONB update),
      - append outcomes into entry_outcomes.
  • risk_manager context can be embedded into the candidate row.
  • Feature / AI / risk context lands in the expected JSON blobs:
      - snapshot                 → PF / microstructure
      - wallet_signals           → smart money
      - _ai_signals              → AI scores + policy/version tags
      - _position_context        → risk_manager.risk_capacity_snapshot

V3+++ extras
------------
This version is aware of the upgraded quant stack:

  • Richer PF / AI / risk fields aligned with FeatureBuilder V3+++:
      - pf_momentum_score, pf_final_score, pf_risk_adjusted_final
      - ai_entry_score, ai_fraud_score, risk_capacity_score
      - ctx_open_positions / ctx_max_positions / PnL & DD context
  • Tags for training analysis:
      - entry_model_name
      - risk_profile / exit_policy_name
      - policy_version / experiment_tag (strategy / backtest tags)
  • Candidate-level meta:
      - candidate_key (src:regime:mint:idx)
      - _demo_source so we can filter out test rows in training queries.
  • Optional JSON summary for quick inspection / automation.

Typical usage (from repo root)
------------------------------
# Basic single-row BUY insert
  python -m scripts.test_ai_logging

# Insert 5 demo candidates with BUY decisions and outcomes
  python -m scripts.test_ai_logging --count 5 --decision BUY --with-outcome

# Force SKIP logging + decision JSONB update only (no outcomes)
  python -m scripts.test_ai_logging --count 3 --decision SKIP --update-decision

# Use custom src/regime/wallet-tag for training analysis
  python -m scripts.test_ai_logging --src raydium --regime raydium_live --wallet-tag raydium_prod

# Emit a JSON summary instead of only pretty print
  python -m scripts.test_ai_logging --count 3 --with-outcome --json-summary
"""

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

# ---------------------------------------------------------------------------
# Repo root / .env bootstrap
# ---------------------------------------------------------------------------

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

try:
    from dotenv import load_dotenv  # type: ignore[import]
except Exception:  # pragma: no cover
    load_dotenv = None  # type: ignore[assignment]

if load_dotenv is not None:
    env_path = ROOT / ".env"
    if env_path.exists():
        load_dotenv(env_path, override=False)

from utils_pkg import ai_db, data_logger  # noqa: E402
from utils_pkg import risk_manager  # noqa: E402


# ════════════════════════════════════════════════════════
# Env / backend helpers
# ════════════════════════════════════════════════════════


def _env_bool(key: str, default: bool = False) -> bool:
    v = os.getenv(key)
    if v is None:
        return default
    return str(v).strip().lower() in {"1", "true", "yes", "y", "on"}


def _pg_enabled() -> bool:
    """
    Mirror data_logger logic: if PG_ENABLED=0 we treat DB logging as off.
    """
    return _env_bool("PG_ENABLED", True)


def backend_summary() -> None:
    """
    Print a short summary of the DB backend & connectivity.
    """
    print("=== scripts.test_ai_logging :: backend summary ===")
    try:
        if hasattr(ai_db, "describe_backend"):
            summary = ai_db.describe_backend()
            print(f"[ai_db] backend: {summary}")
        else:
            print("[ai_db] imported, but no describe_backend() helper")
    except Exception as e:
        print(f"[ai_db] backend summary failed: {e}")

    try:
        if hasattr(ai_db, "ping"):
            ok = ai_db.ping()
            if not ok:
                print("ERROR: ai_db.ping() returned False – cannot reach Postgres.")
        else:
            print("[ai_db] no ping() helper exposed")
    except Exception as e:
        print(f"WARNING: ai_db.ping() raised: {e}")


# ════════════════════════════════════════════════════════
# Demo payload builders (PF / AI / risk context)
# ════════════════════════════════════════════════════════


def build_demo_snapshot(idx: int = 0) -> Dict[str, Any]:
    """
    Build a fake PF/Raydium-style snapshot for logging tests.

    We deliberately vary fields by idx so multiple demo rows are slightly
    different – this helps smoke-test:
      • PF scoring,
      • AI buy scoring,
      • feature extraction logic in FeatureBuilder / data_logger.

    NOTE: This payload mimics the shape the scanners would normally pass in.
    """
    # Simple, deterministic bump per idx
    base_liq = 320_000.0 + 16_000.0 * idx
    base_vol5 = 7_100.0 + 150.0 * idx
    base_chg5 = 4.0 + 0.25 * idx
    base_mcap = 900_000.0 + 25_000.0 * idx

    pf_momentum = 0.55 + 0.03 * idx
    pf_final = 0.60 + 0.04 * idx
    pf_risk_adj = pf_final * (0.95 + 0.01 * idx)

    # Rug / fraud stays low; we just scale a bit by idx.
    ai_fraud_score = max(0.0, min(0.9, 0.05 * idx))

    snapshot: Dict[str, Any] = {
        # Microstructure / liquidity
        "liq": base_liq,
        "vol5": base_vol5,
        "buys5": 24 + idx,
        "sells5": 9 + idx // 2,
        "chg5": base_chg5,
        "age_min": 12.3 + 0.5 * idx,
        "mcap": base_mcap,
        "vol1": 1_500.0 + 25.0 * idx,
        "buys1": 10 + idx,
        "sells1": 3 + idx // 2,
        # PairFilter-style scores (what FeatureBuilder / entry logic consumes)
        "pf_momentum_score": pf_momentum,
        "pf_final_score": pf_final,
        "pf_risk_adjusted_final": pf_risk_adj,
        # AI / fraud
        "ai_fraud_score": ai_fraud_score,
        # Extra debug blob – helpful for inspecting what made it into features
        "_filter_debug": {
            "momentum_score": pf_momentum,
            "final_score": pf_final,
            "risk_adjusted_final": pf_risk_adj,
            "ai_fraud_score": ai_fraud_score,
            "debug_note": f"dbg idx={idx}",
        },
        # Extra flags so we can spot demo rows
        "example_flag": True,
        "note": f"test insert from test_ai_logging.py idx={idx}",
    }
    return snapshot


def build_demo_wallet_signals(idx: int = 0) -> Dict[str, Any]:
    """
    Minimal wallet/smart-money context; shape-compatible with smart_wallets
    and what data_logger expects for 'wallet_signals'.
    """
    return {
        "smart_wallet_score": 0.70 + 0.02 * idx,
        "n_smart_buys_5m": 1 + (idx % 3),
        "smart_flow_tag": "demo_cluster",
        "whale_buy_flag": bool(idx % 2 == 0),
        # For richer feature sets:
        "whale_buy_score": 0.5 + 0.1 * (idx % 3),
    }


def build_demo_ai_signals(
    idx: int = 0,
    *,
    decision: Optional[str],
    entry_model_name: Optional[str],
    policy_version: Optional[str],
    experiment_tag: Optional[str],
) -> Dict[str, Any]:
    """
    Simulate AI entry logic outputs: score + fraud + value + policy tags.

    This blob should be compatible with what data_logger expects to embed
    into raw["_ai_signals"] and is also what we later use downstream for
    training (export_ai_training).
    """
    base_score = 0.60 + 0.03 * idx
    if decision and decision.upper() == "SKIP":
        base_score *= 0.7  # SKIPs get lower entry scores in this demo

    fraud_score = 0.10 + 0.02 * idx

    return {
        "entry_score": base_score,
        "fraud_score": fraud_score,
        "value_estimate": 1.5 + 0.1 * idx,
        "tags": ["demo", "smoke_test", f"idx_{idx}"],
        "model": entry_model_name or os.getenv("ENTRY_MODEL_NAME", "demo_entry_model"),
        "policy_version": policy_version or os.getenv("BT_POLICY_VERSION", "v3_demo"),
        "experiment_tag": experiment_tag or os.getenv(
            "BT_EXPERIMENT_TAG", "local_smoke"
        ),
        # This makes it explicit that these examples are for the entry gate task.
        "task": "entry_gate_v3",
    }


def _try_risk_capacity_snapshot() -> Optional[Dict[str, Any]]:
    """
    Best-effort attempt to call risk_manager.risk_capacity_snapshot() if present.

    We wrap this to avoid blowing up the smoke test if risk_manager isn't fully
    wired yet; the overall shape is still useful for downstream training even
    without the live risk snapshot.
    """
    snap_fn = getattr(risk_manager, "risk_capacity_snapshot", None)
    if snap_fn is None:
        return None

    try:
        import asyncio

        if asyncio.iscoroutinefunction(snap_fn):

            async def _do() -> Dict[str, Any]:
                return await snap_fn()

            return asyncio.run(_do())

        # In case it's sync for some reason
        return snap_fn()  # type: ignore[call-arg]
    except Exception:
        return None


def build_demo_position_context(
    idx: int = 0,
    *,
    risk_profile: Optional[str],
) -> Dict[str, Any]:
    """
    Simulated risk/portfolio context from risk_manager / strategy.

    This is the shape we expect to feed into data_logger as position_context
    so that downstream FeatureBuilder and export_ai_training can get:
      • ctx_open_positions / ctx_max_positions
      • session PnL & drawdown
      • risk capacity / risk mode
    """
    ctx: Dict[str, Any] = {
        "open_positions": 1 + (idx % 3),
        "max_positions": 5,
        "daily_realized_pnl_pct": 1.2,
        "daily_drawdown_pct": -0.4,
        "risk_mode": 1,
        "risk_capacity_score": 0.85,
        "session_id": "local_demo_session",
        "risk_profile": risk_profile or os.getenv("RISK_PROFILE", "balanced"),
    }

    # Best-effort enrichment from real risk_manager, if available.
    snap = _try_risk_capacity_snapshot()
    if isinstance(snap, dict):
        ctx["_risk_capacity_snapshot"] = snap

    return ctx


# ════════════════════════════════════════════════════════
# Core run logic: insert candidates, optional updates/outcomes
# ════════════════════════════════════════════════════════


def run_once(
    *,
    src: str,
    mint: str,
    symbol: str,
    regime: str,
    wallet_tag: str,
    idx: int,
    decision: Optional[str],
    with_decision_update: bool,
    with_outcome: bool,
    entry_model_name: Optional[str],
    policy_version: Optional[str],
    experiment_tag: Optional[str],
    risk_profile: Optional[str],
    exit_policy_name: Optional[str],
) -> Optional[int]:
    """
    Insert a single candidate row (BUY or SKIP), optionally:

      • Update decision metadata after insert
            (JSONB patch via log_entry_decision_update).
      • Log a synthetic outcome row
            (ai_db.mark_outcome_for_candidate).

    Returns:
      candidate_id or None if not inserted (e.g. PG disabled / sampling).
    """
    snap = build_demo_snapshot(idx)
    wallet_signals = build_demo_wallet_signals(idx)
    ai_signals = build_demo_ai_signals(
        idx,
        decision=decision,
        entry_model_name=entry_model_name,
        policy_version=policy_version,
        experiment_tag=experiment_tag,
    )
    position_context = build_demo_position_context(idx, risk_profile=risk_profile)

    decision_upper = decision.upper() if decision else None
    decision_label = decision_upper or "NONE"

    print(
        f"Inserting candidate idx={idx} | mint={mint}, symbol={symbol}, "
        f"regime={regime}, decision={decision_label}"
    )

    # Extra meta for training / experiment analysis – attached into raw["meta"]
    extra_meta: Dict[str, Any] = {
        "RISK_PROFILE": risk_profile,
        "EXIT_POLICY_NAME": exit_policy_name,
        "ENTRY_MODEL_NAME": entry_model_name,
        "ai_policy_version": policy_version,
        "ai_experiment_tag": experiment_tag,
        "_demo_source": "scripts.test_ai_logging",
    }

    candidate_key = f"{src}:{regime}:{mint}:idx{idx}"

    # SKIP path: we call the dedicated logger
    if decision_upper == "SKIP":
        row_id = data_logger.log_skip_candidate(
            src=src,
            mint=mint,
            symbol=symbol,
            snapshot=snap,
            regime=regime,
            wallet_tag=wallet_tag,
            wallet_signals=wallet_signals,
            ai_signals=ai_signals,
            position_context=position_context,
            candidate_key=candidate_key,
            extra_meta=extra_meta,
        )

    else:
        # BUY / NONE path: normal entry candidate logger
        kwargs: Dict[str, Any] = dict(
            src=src,
            mint=mint,
            symbol=symbol,
            snapshot=snap,
            regime=regime,
            wallet_tag=wallet_tag,
            wallet_signals=wallet_signals,
            ai_signals=ai_signals,
            position_context=position_context,
            candidate_key=candidate_key,
            extra_meta=extra_meta,
        )

        # If explicitly BUY, attach decision meta on first insert
        if decision_upper == "BUY":
            kwargs["decision"] = "BUY"
            kwargs["decision_score"] = ai_signals["entry_score"]
            kwargs["decision_reason"] = "demo_buy_from_test_script"

        row_id = data_logger.log_entry_candidate(**kwargs)

    print(f" -> log_entry_candidate / log_skip_candidate returned id={row_id}")
    if row_id is None or row_id <= 0:
        # 0 = logging disabled / sampled out; -1 = DB error with AI_LOG_STRICT=0
        print(
            "    (row_id <= 0 – logging may be disabled, PG_ENABLED=0, or "
            "sampling filtered this out.)"
        )
        return None

    # Optional decision update (second pass JSONB merge)
    if with_decision_update:
        print(f"    updating decision metadata for candidate_id={row_id} ...")
        try:
            data_logger.log_entry_decision_update(
                candidate_id=row_id,
                decision=decision_upper or "BUY",
                decision_score=ai_signals["entry_score"],
                decision_reason="posthoc_decision_update_from_test_ai_logging",
                extra={
                    "update_source": "test_ai_logging.py",
                    "policy_version": policy_version,
                    "experiment_tag": experiment_tag,
                },
            )
        except Exception as e:
            print(f"    decision update failed: {e}")

    # Optional synthetic outcome → populates entry_outcomes
    if with_outcome:
        # Simple toy pnl profile: slightly increasing PnL per idx
        base_pnl_pct = 8.0 + 3.0 * idx  # percentage
        if decision_upper == "SKIP":
            # Example behavior: treat SKIP as "missed opportunity" (negative)
            base_pnl_pct *= -0.3

        hold_sec = 300 + 60 * idx
        label = "win" if base_pnl_pct >= 0 else "loss"

        print(
            f"    logging outcome for candidate_id={row_id} "
            f"(pnl_pct={base_pnl_pct:.2f}, hold_sec={hold_sec}, label={label}) ..."
        )
        try:
            ai_db.mark_outcome_for_candidate(
                candidate_id=row_id,
                label=label,
                pnl_pct=base_pnl_pct,
                hold_sec=hold_sec,
                notes="demo outcome from test_ai_logging.py",
                extra={
                    "demo_idx": idx,
                    "from_test_script": True,
                    "decision": decision_upper,
                    "risk_profile": risk_profile,
                    "exit_policy_name": exit_policy_name,
                    "policy_version": policy_version,
                    "experiment_tag": experiment_tag,
                },
            )
        except Exception as e:
            print(f"    outcome logging failed: {e}")

    return row_id


# ════════════════════════════════════════════════════════
# Introspection of last inserted candidate
# ════════════════════════════════════════════════════════


def show_latest_candidate(candidate_id: int) -> Dict[str, Any]:
    """
    Fetch and print a compact summary of the candidate row we just inserted.

    Returns:
        dict representation of the row (for optional JSON summary).
    """
    try:
        row = ai_db.fetch_candidate_by_id(candidate_id)
    except Exception as e:
        print(f"[ai_db] fetch_candidate_by_id({candidate_id}) failed: {e}")
        return {}

    if not row:
        print(f"[ai_db] No row found for id={candidate_id}")
        return {}

    print("\n=== Fetched candidate from DB ===")
    print(f"id       : {row.get('id')}")
    print(f"ts       : {row.get('ts')}")
    print(f"src      : {row.get('src')}")
    print(f"mint     : {row.get('mint')}")
    print(f"symbol   : {row.get('symbol')}")
    print(f"regime   : {row.get('regime')}")
    print(f"wallet   : {row.get('wallet_tag')}")
    print(f"feat_ver : {row.get('feature_version')}")
    print(f"risk_prof: {row.get('risk_profile')}")
    print(f"exit_pol : {row.get('exit_policy_name')}")

    feats = row.get("features") or {}
    raw = row.get("raw") or {}

    print("features :")
    interesting_keys = [
        "pf_momentum_score",
        "pf_final_score",
        "pf_risk_adjusted_final",
        "ai_entry_score",
        "ai_fraud_score",
        "ctx_open_positions",
        "ctx_daily_drawdown_pct",
        "ctx_daily_realized_pnl_pct",
        "risk_capacity_score",
        "smart_wallet_score",
        "whale_buy_score",
    ]
    for k in interesting_keys:
        if k in feats:
            print(f"  - {k}: {feats[k]!r}")

    # Indicate that raw has debug & meta blobs
    if "_decision_meta" in raw:
        print("raw._decision_meta:", raw["_decision_meta"])
    if "_ai_signals" in raw:
        print("raw._ai_signals:", raw["_ai_signals"])
    if "_position_context" in raw:
        print("raw._position_context:", raw["_position_context"])
    if "meta" in raw:
        print("raw.meta:", raw["meta"])
    print("=== end candidate summary ===\n")

    return row


# ════════════════════════════════════════════════════════
# CLI
# ════════════════════════════════════════════════════════


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=(
            "Smoke-test AI logging (entry_candidates + optional decision "
            "updates + outcomes)."
        )
    )

    # How many rows / core asset tags
    p.add_argument(
        "--count",
        type=int,
        default=1,
        help="How many demo rows to insert (default: 1).",
    )
    p.add_argument(
        "--mint",
        type=str,
        default="TEST_MINT_123",
        help="Mint string to use for demo rows.",
    )
    p.add_argument(
        "--symbol",
        type=str,
        default="TEST",
        help="Symbol to use for demo rows.",
    )

    # Source / regime / wallet tagging (for training analysis)
    p.add_argument(
        "--src",
        type=str,
        default="test_script",
        help="Source tag (src column) for entry_candidates.",
    )
    p.add_argument(
        "--regime",
        type=str,
        default="test_regime",
        help="Regime tag used when logging candidates.",
    )
    p.add_argument(
        "--wallet-tag",
        type=str,
        default=os.getenv("RAYDIUM_WALLET_TAG", "test_wallet_cluster"),
        help="Wallet cluster tag for training/analysis.",
    )

    # Decision / outcome behaviors
    p.add_argument(
        "--decision",
        type=str,
        default="BUY",
        choices=["BUY", "SKIP", "NONE", "buy", "skip", "none"],
        help=(
            "Decision to attach to candidates: BUY, SKIP, or NONE "
            "(no decision metadata). Default: BUY."
        ),
    )
    p.add_argument(
        "--update-decision",
        action="store_true",
        help="After insert, call log_entry_decision_update(...) to test JSONB patching.",
    )
    p.add_argument(
        "--with-outcome",
        action="store_true",
        help="Also log synthetic outcomes via ai_db.mark_outcome_for_candidate.",
    )
    p.add_argument(
        "--no-fetch",
        action="store_true",
        help="Do not fetch/print the last candidate from DB after inserts.",
    )

    # AI / strategy tagging
    p.add_argument(
        "--entry-model",
        type=str,
        default=os.getenv("ENTRY_MODEL_NAME", ""),
        help="Optional entry model name override for this test run.",
    )
    p.add_argument(
        "--policy-version",
        type=str,
        default=os.getenv("BT_POLICY_VERSION", ""),
        help="Optional policy_version tag for training/analysis.",
    )
    p.add_argument(
        "--experiment-tag",
        type=str,
        default=os.getenv("BT_EXPERIMENT_TAG", ""),
        help="Optional experiment_tag for training/analysis.",
    )

    # Risk / exit tagging
    p.add_argument(
        "--risk-profile",
        type=str,
        default=os.getenv("RISK_PROFILE", "balanced"),
        help='Risk profile label, e.g. "conservative","balanced","aggressive".',
    )
    p.add_argument(
        "--exit-policy",
        type=str,
        default=os.getenv("EXIT_POLICY_NAME", "hybrid"),
        help='Exit policy label, e.g. "dca","hard_stop","hybrid".',
    )

    # Output formatting
    p.add_argument(
        "--json-summary",
        action="store_true",
        help="Emit a JSON summary of inserted candidate_ids and last row.",
    )

    return p.parse_args()


def main() -> None:
    args = parse_args()
    backend_summary()

    if not _pg_enabled():
        print("PG_ENABLED=0 — Postgres logging is disabled; not inserting demo rows.")
        return

    decision_arg = args.decision.upper()
    if decision_arg not in {"BUY", "SKIP", "NONE"}:
        decision_arg = "BUY"
    decision: Optional[str] = None if decision_arg == "NONE" else decision_arg

    entry_model = args.entry_model.strip() or None
    policy_version = args.policy_version.strip() or None
    experiment_tag = args.experiment_tag.strip() or None
    risk_profile = args.risk_profile.strip() or None
    exit_policy_name = args.exit_policy.strip() or None

    inserted_ids: List[int] = []
    last_row_id: Optional[int] = None

    for i in range(args.count):
        row_id = run_once(
            src=args.src,
            mint=args.mint,
            symbol=args.symbol,
            regime=args.regime,
            wallet_tag=args.wallet_tag,
            idx=i,
            decision=decision,
            with_decision_update=args.update_decision,
            with_outcome=args.with_outcome,
            entry_model_name=entry_model,
            policy_version=policy_version,
            experiment_tag=experiment_tag,
            risk_profile=risk_profile,
            exit_policy_name=exit_policy_name,
        )
        if row_id is not None and row_id > 0:
            inserted_ids.append(row_id)
            last_row_id = row_id

    print("Done.", end=" ")
    last_row: Dict[str, Any] = {}
    if last_row_id is not None:
        print(f"Last inserted candidate_id={last_row_id}")
        if not args.no_fetch:
            last_row = show_latest_candidate(last_row_id)
    else:
        print("(No rows inserted – see messages above.)")

    if args.json_summary:
        summary = {
            "inserted_ids": inserted_ids,
            "last_row_id": last_row_id,
            "last_row": last_row or None,
        }
        print(json.dumps(summary, indent=2, default=str))


if __name__ == "__main__":
    main()
