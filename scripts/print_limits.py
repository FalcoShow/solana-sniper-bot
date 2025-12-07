# scripts/print_limits.py
from __future__ import annotations

"""
print_limits.py — Quant-grade live risk / regime snapshot (V3+++)

Fast CLI probe for:
  • Environment risk knobs (caps, profiles, feature gates)
  • Current limits from utils_pkg.risk_manager.current_limits()
  • Risk capacity (0..1) and discrete risk_mode (0..3)
  • Daily governance (realized PnL, spend, equity / drawdown context)
  • Regime engine state (risk_on / normal / risk_off + scores)
  • Entry gate (should_allow_new_entry + basic AI / regime context)

Usage
-----

From repo root:

    python -m scripts.print_limits

Options:

    --json              Emit machine-readable JSON instead of pretty text
    --watch 10          Refresh every N seconds (live dashboard in terminal)
    --no-ai             Skip AI entry / strategy diagnostics
    --no-regime         Skip regime details (trend/vol/liquidity…)
"""

import argparse
import asyncio
import json
import os
import sys
from typing import Any, Dict, Optional

# ---------------------------------------------------------------------------
# Ensure repo root on PYTHONPATH so scripts/ can import utils_pkg/
# ---------------------------------------------------------------------------
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

# Core imports (quant-grade infra)
from utils_pkg.risk_manager import (  # type: ignore[import]
    current_limits,
    risk_snapshot,
    risk_capacity_snapshot,
    risk_capacity_score,
    should_allow_new_entry,
)
from utils_pkg.regime import regime_state  # type: ignore[import]

# Optional AI / entry diagnostics (best-effort)
try:  # pragma: no cover - optional
    from utils_pkg import ai_entry_logic  # type: ignore[import]
except Exception:  # pragma: no cover
    ai_entry_logic = None  # type: ignore[assignment]


# ---------------------------------------------------------------------------
# Small helpers
# ---------------------------------------------------------------------------

def _env_or(keys: list[str], default: str = "<none>") -> str:
    for k in keys:
        v = os.getenv(k)
        if v:
            return v
    return default


def _env_bool(name: str, default: bool = False) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return str(v).strip().lower() in {"1", "true", "yes", "y", "on"}


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)))
    except Exception:
        return default


def _pct(x: float) -> str:
    try:
        return f"{x * 100:.2f}%"
    except Exception:
        return "?.??%"


def _fmt_usd(x: Optional[float]) -> str:
    if x is None:
        return "$  ?.??"
    try:
        return f"${x:,.2f}"
    except Exception:
        return f"${x}"


def _fmt_bool(b: bool) -> str:
    return "YES" if b else "no"


def _collect_env_summary() -> Dict[str, Any]:
    """
    Key knobs that affect risk / behaviour (non-secret, safe to print).
    """
    return {
        # Network / wallet
        "NETWORK": os.getenv("NETWORK") or os.getenv("SOLANA_CLUSTER") or "<auto>",
        "RISK_PROFILE": os.getenv("RISK_PROFILE", "<default>"),
        "WALLET_PUBKEY": _env_or(
            ["WALLET_PUBKEY", "WALLET_PUBLIC_KEY", "WALLET_ADDRESS"], "<none>"
        ),

        # Entry feature flags
        "ENTRY_MOMENTUM_ENABLED": os.getenv("ENTRY_MOMENTUM_ENABLED", "1"),
        "ENTRY_AI_ENABLED": os.getenv("AI_ENTRY_ENABLED", os.getenv("ENTRY_AI_ENABLED", "1")),
        "ENTRY_REQUIRE_RISK_CAPACITY": os.getenv("ENTRY_REQUIRE_RISK_CAPACITY", "0"),
        "ENTRY_MIN_INITIAL_BUY_SOL": os.getenv("ENTRY_MIN_INITIAL_BUY_SOL", "<env>"),

        # Filters / exits / risk engines
        "LIQ_FILTER_ENABLED": os.getenv("LIQ_FILTER_ENABLED", "1"),
        "EXIT_ENGINE_ENABLED": os.getenv("EXIT_ENGINE_ENABLED", "1"),
        "RISK_MANAGER_ENABLED": os.getenv("RISK_MANAGER_ENABLED", "1"),
        "REGIME_ENABLED": os.getenv("REGIME_ENABLED", "1"),
        "RISK_MODE_OVERRIDE": os.getenv("RISK_MODE_OVERRIDE", "<none>"),

        # Daily governance (caps / stops)
        "STOP_AFTER_DAILY_PROFIT_PCT": os.getenv("STOP_AFTER_DAILY_PROFIT_PCT", "4"),
        "STOP_AFTER_DAILY_LOSS_PCT": os.getenv("STOP_AFTER_DAILY_LOSS_PCT", "0"),
        "DAILY_SPEND_CAP_PCT": os.getenv("DAILY_SPEND_CAP_PCT", "0.15"),
        "DAILY_SPEND_CAP_USD": os.getenv("DAILY_SPEND_CAP_USD", "<auto>"),

        # Per-trade / concurrency
        "RISK_MAX_PER_TRADE": os.getenv("RISK_MAX_PER_TRADE", "0.045"),
        "MAX_CONCURRENT_POS": os.getenv("MAX_CONCURRENT_POS", "4"),
    }


async def _maybe_ai_entry_diag() -> Dict[str, Any]:
    """
    Best-effort AI entry diagnostics.

    If utils_pkg.ai_entry_logic exposes any of:
      • diag_snapshot()
      • gate_snapshot()
      • current_diag
    we surface it here. Otherwise returns {}.
    """
    if ai_entry_logic is None:
        return {}

    try:
        fn = getattr(ai_entry_logic, "diag_snapshot", None)
        if callable(fn):
            out = fn()
            if isinstance(out, dict):
                return out
    except Exception:
        pass

    try:
        fn = getattr(ai_entry_logic, "gate_snapshot", None)
        if callable(fn):
            out = fn()
            if isinstance(out, dict):
                return out
    except Exception:
        pass

    try:
        cur = getattr(ai_entry_logic, "current_diag", None)
        if isinstance(cur, dict):
            return cur
    except Exception:
        pass

    return {}


# ---------------------------------------------------------------------------
# Core probe
# ---------------------------------------------------------------------------

async def _one_snapshot(include_regime: bool = True, include_ai: bool = True) -> Dict[str, Any]:
    """
    Pull a single consistent snapshot from the live risk / regime engines.
    """
    # Run core async calls concurrently for speed
    limits_task = asyncio.create_task(current_limits())
    risk_snap_task = asyncio.create_task(risk_snapshot())
    cap_snap_task = asyncio.create_task(risk_capacity_snapshot())
    cap_score_task = asyncio.create_task(risk_capacity_score())
    allow_entry_task = asyncio.create_task(should_allow_new_entry())

    # Regime features (optional)
    regime_feats_task = None
    if include_regime:
        regime_feats_task = asyncio.create_task(regime_state.features())

    # AI entry diagnostics (optional, best-effort)
    ai_diag_task = None
    if include_ai:
        ai_diag_task = asyncio.create_task(_maybe_ai_entry_diag())

    limits = await limits_task
    r_snap = await risk_snap_task
    cap_snap = await cap_snap_task
    cap_score = await cap_score_task
    allow_entry = await allow_entry_task

    regime_feats: Dict[str, Any] | None = None
    if regime_feats_task is not None:
        try:
            regime_feats = await regime_feats_task
        except Exception:
            regime_feats = None

    ai_diag: Dict[str, Any] | None = None
    if ai_diag_task is not None:
        try:
            ai_diag = await ai_diag_task
        except Exception:
            ai_diag = None

    env_summary = _collect_env_summary()

    snapshot: Dict[str, Any] = {
        "env": env_summary,
        "limits": limits,
        "risk_snapshot": r_snap,
        "risk_capacity_snapshot": cap_snap,
        "risk_capacity_score": cap_score,
        "allow_new_entry": bool(allow_entry),
        "regime_features": regime_feats or {},
        "ai_entry_diag": ai_diag or {},
    }
    return snapshot


# ---------------------------------------------------------------------------
# Pretty printer
# ---------------------------------------------------------------------------

def _print_pretty(snap: Dict[str, Any]) -> None:
    env = snap.get("env", {}) or {}
    limits = snap.get("limits", {}) or {}
    rcap = snap.get("risk_capacity_snapshot", {}) or {}
    rfeats = snap.get("regime_features", {}) or {}
    cap_score = float(snap.get("risk_capacity_score", 0.0) or 0.0)
    allow_entry = bool(snap.get("allow_new_entry", True))

    # Core capital / limits
    equity = float(limits.get("equity_usd") or 0.0)
    ticket = float(limits.get("ticket_usd") or 0.0)
    pos_pct_cap = float(limits.get("pos_pct_cap") or 0.0)
    max_conc = int(limits.get("max_concurrent") or 0)
    daily_spend = limits.get("daily_spend_usd")
    daily_spend = float(daily_spend) if daily_spend not in (None, "") else 0.0
    risk_mode = int(limits.get("risk_mode") or 1)
    risk_mode_override = limits.get("risk_mode_override")

    # Daily governance from risk_capacity_snapshot
    winrate20 = float(rcap.get("winrate20") or 0.0)
    rolling_dd = float(rcap.get("rolling_dd_pct") or 0.0)
    day_pnl_usd = float(rcap.get("daily_realized_pnl_usd") or 0.0)
    day_pnl_pct = float(rcap.get("daily_realized_pnl_pct") or 0.0)

    regime_label = (
        rcap.get("regime_label")
        or rfeats.get("regime_label")
        or "normal"
    )
    regime_mult = float(
        rcap.get("regime_risk_multiplier")
        or rfeats.get("regime_risk_multiplier")
        or 1.0
    )
    trend = float(
        rfeats.get("regime_trend")
        or rfeats.get("trend_score")
        or 0.0
    )
    vol = float(
        rfeats.get("regime_volatility")
        or rfeats.get("volatility_score")
        or 0.0
    )
    liq = float(rfeats.get("regime_liquidity") or rfeats.get("liquidity_score") or 0.5)
    manip = float(
        rfeats.get("regime_manipulation_risk")
        or rfeats.get("manipulation_score")
        or 0.0
    )

    # AI entry diagnostics (optional)
    ai_diag = snap.get("ai_entry_diag", {}) or {}
    ai_gate = ai_diag.get("ai_score_gate")
    ai_min_momo = ai_diag.get("min_momentum_score")
    ai_meta = ai_diag.get("meta")

    # Derived env caps
    stop_profit_pct = _env_float("STOP_AFTER_DAILY_PROFIT_PCT", 0.0)
    stop_loss_pct = _env_float("STOP_AFTER_DAILY_LOSS_PCT", 0.0)
    daily_spend_pct = _env_float("DAILY_SPEND_CAP_PCT", 0.0)
    daily_spend_cap_usd_env = os.getenv("DAILY_SPEND_CAP_USD")

    print("")
    print("=== 📊 Current Limits / Risk Snapshot (V3+++) ===")
    print("")

    # ENV
    print("[ENV / FEATURE GATES]")
    for k, v in sorted(env.items()):
        print(f"  {k:28s} = {v}")
    print("")

    # LIMITS
    print("[CAPITAL & POSITION LIMITS]")
    print(f"  equity_usd               : {_fmt_usd(equity)}")
    print(f"  ticket_usd               : {_fmt_usd(ticket)}")
    print(f"  pos_pct_cap              : {_pct(pos_pct_cap)}")
    print(f"  max_concurrent           : {max_conc}")
    print(f"  daily_spend_usd          : {_fmt_usd(daily_spend)}")

    # Derived spend caps
    if equity > 0 and daily_spend_pct > 0:
        spend_cap_from_pct = equity * daily_spend_pct
        cap_str = _fmt_usd(spend_cap_from_pct)
        print(f"  daily_spend_cap_pct      : {_pct(daily_spend_pct)}  (~{cap_str})")
    else:
        print(f"  daily_spend_cap_pct      : {_pct(daily_spend_pct)}")

    if daily_spend_cap_usd_env not in (None, "", "<auto>"):
        try:
            env_cap = float(daily_spend_cap_usd_env)
            print(f"  daily_spend_cap_usd_env  : {_fmt_usd(env_cap)}")
        except Exception:
            print(f"  daily_spend_cap_usd_env  : {daily_spend_cap_usd_env}")
    print("")

    # RISK CAPACITY & GOVERNANCE
    print("[RISK CAPACITY / GOVERNANCE]")
    print(f"  risk_mode                : {risk_mode}  "
          f"({'OFF' if risk_mode == 0 else 'DEFENSIVE' if risk_mode == 3 else 'AGGR' if risk_mode == 2 else 'NORMAL'})")
    print(f"  risk_mode_override       : {risk_mode_override}")
    print(f"  risk_capacity_score      : {cap_score:.3f}")
    print(f"  allow_new_entry          : {_fmt_bool(allow_entry)}")
    print("")
    print(f"  winrate_last_20_trades   : {_pct(winrate20)}")
    print(f"  rolling_drawdown_pct     : {_pct(rolling_dd)}")
    print(f"  daily_realized_pnl_usd   : {_fmt_usd(day_pnl_usd)}")
    print(f"  daily_realized_pnl_pct   : {_pct(day_pnl_pct)}")

    # Governance thresholds (profit / loss stops)
    if stop_profit_pct > 0 or stop_loss_pct > 0:
        print("")
        print("  daily_profit_stop_pct    : "
              f"{_pct(stop_profit_pct)} (hard governance if enabled)")
        if equity > 0 and stop_profit_pct > 0:
            target_usd = equity * stop_profit_pct
            print(f"    ↳ profit_stop_usd      : {_fmt_usd(target_usd)}")
        print(f"  daily_loss_stop_pct      : {_pct(stop_loss_pct)}")
        if equity > 0 and stop_loss_pct > 0:
            loss_cap_usd = equity * abs(stop_loss_pct)
            print(f"    ↳ loss_stop_usd        : {_fmt_usd(loss_cap_usd)}")
    print("")

    # REGIME
    print("[REGIME ENGINE]")
    print(f"  regime_label             : {regime_label}")
    print(f"  regime_risk_multiplier   : {regime_mult:.3f}")
    print(f"  trend_score              : {trend:+.3f}")
    print(f"  volatility_score         : {vol:.3f}")
    print(f"  liquidity_score          : {liq:.3f}")
    print(f"  manipulation_risk        : {manip:.3f}")
    print("")

    # AI ENTRY (optional)
    if ai_diag:
        print("[AI ENTRY / GATE (if available)]")
        if ai_gate is not None:
            try:
                print(f"  ai_score_gate            : {float(ai_gate):.3f}")
            except Exception:
                print(f"  ai_score_gate            : {ai_gate}")
        if ai_min_momo is not None:
            try:
                print(f"  min_momentum_score       : {float(ai_min_momo):.3f}")
            except Exception:
                print(f"  min_momentum_score       : {ai_min_momo}")
        if ai_meta and isinstance(ai_meta, dict):
            # Only show a couple of high-signal fields
            score_src = ai_meta.get("score_model") or ai_meta.get("model")
            last_update = ai_meta.get("last_update")
            print(f"  score_model              : {score_src or '<n/a>'}")
            if last_update:
                print(f"  ai_last_update           : {last_update}")
        print("")

    # Simple “traffic light” hint
    if risk_mode == 0:
        status = "🔴 TRADING HALTED (risk_mode=0)"
    elif risk_mode == 3 or cap_score < 0.35:
        status = "🟠 DEFENSIVE (size down / tighten filters)"
    elif cap_score >= 0.80 and str(regime_label).lower() == "risk_on":
        status = "🟢 AGGRESSIVE window (risk_on + high capacity)"
    else:
        status = "🟡 NORMAL (steady state)"

    print(f"Status: {status}")
    print("")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Print current risk limits / regime snapshot (V3+++)."
    )
    p.add_argument(
        "--json",
        action="store_true",
        help="Emit JSON instead of human-readable text.",
    )
    p.add_argument(
        "--watch",
        type=int,
        default=0,
        help="If >0, refresh every N seconds until Ctrl+C.",
    )
    p.add_argument(
        "--no-ai",
        action="store_true",
        help="Skip AI entry diagnostics (faster / lighter).",
    )
    p.add_argument(
        "--no-regime",
        action="store_true",
        help="Skip regime details (only limits / capacity).",
    )
    return p


async def _run_once(args: argparse.Namespace) -> None:
    snap = await _one_snapshot(
        include_regime=not args.no_regime,
        include_ai=not args.no_ai,
    )
    if args.json:
        print(json.dumps(snap, indent=2, default=str))
    else:
        _print_pretty(snap)


async def _run_watch(args: argparse.Namespace) -> None:
    interval = max(1, int(args.watch))
    try:
        while True:
            # Clear screen for a live-dashboard feel
            os.system("cls" if os.name == "nt" else "clear")
            await _run_once(args)
            print(f"[print_limits] refreshing in {interval}s (Ctrl+C to stop)…")
            await asyncio.sleep(interval)
    except KeyboardInterrupt:
        print("\n[print_limits] stopped by user.")


def main() -> None:
    parser = build_arg_parser()
    args = parser.parse_args()

    if args.watch > 0:
        asyncio.run(_run_watch(args))
    else:
        asyncio.run(_run_once(args))


if __name__ == "__main__":
    main()
