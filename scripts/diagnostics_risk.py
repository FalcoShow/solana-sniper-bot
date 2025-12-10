from __future__ import annotations

"""
scripts/diagnostics_risk.py

Risk engine diagnostics / CLI (V3+++)
=====================================

High-level role
---------------
One-stop CLI tool for inspecting and nudging the quant-grade risk engine:

  • Snapshot current limits / equity / capacity / regime.
  • Understand *why* entries are blocked (gate breakdown).
  • Inspect capacity model / AI overlay / Kelly clamp.
  • Inspect daily budget / spend / trades.
  • Read the published risk:ctx hash (for FeatureBuilder sanity checks).
  • Aggregate "health" view for dashboards / ops.
  • Operator tools:
        - Set / clear manual block (risk:manual_block).
        - Force a risk_mode override (0..3) or clear it.
        - Set / clear risk profile override (conservative / balanced / aggressive / auto).
        - Clear risk cooldown (consecutive-loss / DD based).
  • Optional watch mode for live top-style monitoring.

Usage
-----
    # Basic one-shot summary (default command)
    python -m scripts.diagnostics_risk
    python -m scripts.diagnostics_risk summary

    # Detailed entry gate breakdown
    python -m scripts.diagnostics_risk gate

    # Capacity model & AI overlay
    python -m scripts.diagnostics_risk capacity

    # Risk budget view
    python -m scripts.diagnostics_risk budget

    # Raw risk:ctx hash (as seen by FeatureBuilder / scanners)
    python -m scripts.diagnostics_risk ctx

    # Aggregate risk health
    python -m scripts.diagnostics_risk health

    # Live watch (refresh every 15 seconds)
    python -m scripts.diagnostics_risk watch --interval 15

    # Ops controls
    python -m scripts.diagnostics_risk block --on
    python -m scripts.diagnostics_risk block --off
    python -m scripts.diagnostics_risk mode --set 0        # force OFF
    python -m scripts.diagnostics_risk mode --set 1        # NORMAL
    python -m scripts.diagnostics_risk mode --set 2        # AGGRESSIVE
    python -m scripts.diagnostics_risk mode --set 3        # DEFENSIVE
    python -m scripts.diagnostics_risk mode --clear        # clear override
    python -m scripts.diagnostics_risk profile --set aggressive
    python -m scripts.diagnostics_risk profile --set auto
    python -m scripts.diagnostics_risk profile --clear
    python -m scripts.diagnostics_risk cooldown --clear

JSON output
-----------
Add `--json` to any read-only command to get pure JSON on stdout:

    python -m scripts.diagnostics_risk summary --json
    python -m scripts.diagnostics_risk gate --json
    python -m scripts.diagnostics_risk capacity --json
    python -m scripts.diagnostics_risk budget --json
    python -m scripts.diagnostics_risk ctx --json
    python -m scripts.diagnostics_risk health --json
"""

import argparse
import asyncio
import json
import os
import sys
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Callable

try:
    from logging_setup import get_logger  # type: ignore
except Exception:  # pragma: no cover
    import logging

    def get_logger() -> "logging.Logger":
        logging.basicConfig(level=logging.INFO)
        return logging.getLogger("diagnostics_risk")


from utils_pkg import risk_manager as RM  # canonical risk brain

log = get_logger()


# --------------------------------------------------------------------------- #
# Small formatting helpers
# --------------------------------------------------------------------------- #

def _fmt_ts(ts: Optional[float]) -> str:
    if not ts:
        return "-"
    try:
        dt = datetime.fromtimestamp(float(ts), tz=timezone.utc)
        return dt.strftime("%Y-%m-%d %H:%M:%S UTC")
    except Exception:
        return f"{ts:.0f}"


def _fmt_bool(x: bool) -> str:
    return "yes" if x else "no"


def _fmt_pct(x: float, decimals: int = 2) -> str:
    try:
        return f"{float(x) * 100:.{decimals}f}%"
    except Exception:
        return str(x)


def _fmt_num(x: Any, decimals: int = 2) -> str:
    try:
        v = float(x)
    except Exception:
        return str(x)
    if abs(v) >= 1000:
        return f"{v:,.{decimals}f}"
    return f"{v:.{decimals}f}"


def _print_kv(title: str, mapping: Dict[str, Any]) -> None:
    print(f"\n{title}")
    print("-" * len(title))
    if not mapping:
        print("  (no data)")
        return
    max_key = max(len(k) for k in mapping.keys())
    for k, v in mapping.items():
        print(f"  {k.ljust(max_key)} : {v}")


def _maybe_print_json(data: Any, *, as_json: bool) -> bool:
    """
    If as_json is True, print JSON and return True (caller should then return).
    """
    if not as_json:
        return False
    print(json.dumps(data, indent=2, sort_keys=True, default=str))
    return True


# --------------------------------------------------------------------------- #
# Command handlers (async)
# --------------------------------------------------------------------------- #

async def cmd_summary(args: argparse.Namespace) -> None:
    """
    High-level one-shot summary of risk, budget and gating.
    """
    snap = await RM.risk_snapshot()
    gate = await RM.entry_gate_debug()
    cap = await RM.risk_capacity_snapshot()

    out = {
        "snapshot": snap,
        "entry_gate": gate,
        "capacity": cap,
    }

    if _maybe_print_json(out, as_json=args.json):
        return

    print("\n=== RISK SUMMARY (V3+++) ===")
    print(f"engine      : {snap.get('risk_engine_version', 'n/a')}")
    print(f"equity_usd  : {_fmt_num(snap.get('equity_usd', 0.0))} "
          f"(HWM={_fmt_num(snap.get('equity_hwm_usd', 0.0))})")
    print(f"ticket_usd  : {_fmt_num(snap.get('ticket_usd', 0.0))} "
          f"(pos_cap={_fmt_pct(snap.get('pos_pct_cap', 0.0), 2)})")
    print(f"max_conc    : {snap.get('max_concurrent', 0)}")
    print(f"daily_spend : cap={_fmt_num(snap.get('daily_spend_usd', 0.0))} "
          f" spent={_fmt_num(snap.get('daily_spent_usd', 0.0))} "
          f" used={_fmt_pct(snap.get('daily_spend_used_pct', 0.0) / 100.0, 2)}")
    print(f"pnl_today   : {_fmt_num(snap.get('daily_realized_pnl_usd', 0.0))} "
          f"(winrate20={_fmt_pct(snap.get('winrate20', 0.0), 1)}, "
          f"dd={_fmt_pct(snap.get('rolling_dd_pct', 0.0), 2)})")
    print(f"risk_mode   : {snap.get('risk_mode', 1)} "
          f"(override={snap.get('risk_mode_override')}, "
          f"profile={snap.get('risk_profile')} "
          f"env={snap.get('risk_profile_env')}, "
          f"sandbox={_fmt_bool(snap.get('sandbox_mode', False))})")
    print(f"capacity    : {_fmt_num(snap.get('risk_capacity', 1.0), 3)} "
          f"(ai_delta={_fmt_num(snap.get('ai_capacity_delta', 0.0), 3)}, "
          f"kelly_cap={_fmt_pct(snap.get('kelly_pos_pct_cap', 0.0), 2)})")
    print(f"equity_util : {_fmt_pct(snap.get('equity_utilization_pct', 0.0) / 100.0, 2)}")
    print(f"regime      : {snap.get('regime_label')} "
          f"(mult={_fmt_num(snap.get('regime_risk_multiplier', 1.0), 2)}, "
          f"trend={_fmt_num(snap.get('regime_trend', 0.0), 2)}, "
          f"vol={_fmt_num(snap.get('regime_volatility', 0.0), 2)}, "
          f"chop={_fmt_num(snap.get('regime_choppy_score', 0.0), 2)})")
    print(f"plan        : equity_today={_fmt_num(snap.get('plan_equity_today_usd', 0.0))} "
          f"gap={_fmt_pct(snap.get('plan_gap_pct', 0.0), 2)} "
          f"status={snap.get('plan_status')}")
    print(f"trades      : today={snap.get('daily_trades_count', 0)} "
          f"cap={snap.get('daily_trades_cap', 0)}")
    print(f"equity_floor: {snap.get('equity_floor_usd', 0.0)}")
    print(f"trading_win : open={_fmt_bool(snap.get('trading_window_open', True))} "
          f"reason={snap.get('trading_window_reason', 'n/a')}")

    # Entry gate summary
    allowed = gate.get("allowed", False)
    reasons = gate.get("reasons_blocking", [])
    print("\n--- ENTRY GATE ---")
    print(f"allowed     : {_fmt_bool(bool(allowed))}")
    if not allowed and reasons:
        print("reasons     :")
        for r in reasons:
            print(f"  - {r}")
    else:
        print("reasons     : (none)")

    # Extra detail: halt_reason
    halt_reason = snap.get("halt_reason") or gate.get("halt_reason")
    if halt_reason:
        print(f"halt_reason : {halt_reason}")


async def cmd_gate(args: argparse.Namespace) -> None:
    """
    Detailed entry gate breakdown.
    """
    gate = await RM.entry_gate_debug()

    if _maybe_print_json(gate, as_json=args.json):
        return

    print("\n=== ENTRY GATE DEBUG ===")
    print(f"allowed        : {_fmt_bool(bool(gate.get('allowed', False)))}")
    print(f"risk_mode      : {gate.get('limits', {}).get('risk_mode')}")
    print(f"risk_capacity  : {_fmt_num(gate.get('limits', {}).get('risk_capacity', 1.0), 3)}")
    print(f"equity_usd     : {_fmt_num(gate.get('limits', {}).get('equity_usd', 0.0))}")
    print(f"ticket_usd     : {_fmt_num(gate.get('limits', {}).get('ticket_usd', 0.0))}")
    print(f"max_concurrent : {gate.get('limits', {}).get('max_concurrent')}")
    print(f"daily_spend_usd: {_fmt_num(gate.get('limits', {}).get('daily_spend_usd', 0.0))}")
    print(f"daily_realized : {_fmt_num(gate.get('daily_realized_usd', 0.0))} "
          f"(profit_stop={_fmt_num(gate.get('profit_stop_usd', 0.0))}, "
          f"loss_cap={_fmt_num(gate.get('loss_cap_usd', 0.0))})")
    print(f"spent_usd      : {_fmt_num(gate.get('spent_usd', 0.0))}")
    print(f"sol_balance    : {_fmt_num(gate.get('sol_balance', 0.0), 4)}")
    print(f"cooldown       : {_fmt_bool(gate.get('cooldown_active', False))}")
    print(f"manual_block   : {_fmt_bool(gate.get('manual_block', False))}")
    print(f"infra_halt     : {_fmt_bool(gate.get('infra_halt', False))}")
    print(f"hard_halt      : {_fmt_bool(gate.get('hard_halt', False))}")
    print(f"hard_risk_off  : {_fmt_bool(gate.get('hard_risk_off', False))}")
    print(f"plan_gap_pct   : {_fmt_pct(gate.get('plan_gap_pct', 0.0), 2)} "
          f"(ahead_stop={_fmt_pct(gate.get('plan_ahead_hard_stop', 0.0), 2)})")
    print(f"trades_today   : {gate.get('trade_count_today', 0)} / "
          f"{gate.get('trade_cap_today', 0)}")
    print(f"equity_floor   : {_fmt_num(gate.get('equity_floor_usd', 0.0))}")
    print(f"sandbox_mode   : {_fmt_bool(gate.get('sandbox_mode', False))}")
    print(f"risk_profile   : {gate.get('risk_profile')} (env={gate.get('risk_profile_env')})")
    print(f"regime_gate    : {_fmt_num(gate.get('regime_entry_gate_factor', 1.0), 3)}")
    print(f"trading_win    : open={_fmt_bool(gate.get('trading_window_open', True))} "
          f"reason={gate.get('trading_window_reason', 'n/a')}")
    print(f"engine_version : {gate.get('risk_engine_version', 'n/a')}")

    reasons = gate.get("reasons_blocking", [])
    print("\nReasons blocking entries:")
    if reasons:
        for r in reasons:
            print(f"  - {r}")
    else:
        print("  (none — entries allowed)")


async def cmd_capacity(args: argparse.Namespace) -> None:
    """
    Risk capacity / regime / AI overlay snapshot.
    """
    snap = await RM.risk_capacity_snapshot()

    if _maybe_print_json(snap, as_json=args.json):
        return

    print("\n=== RISK CAPACITY SNAPSHOT ===")
    print(f"risk_capacity          : {_fmt_num(snap.get('risk_capacity', 1.0), 3)}")
    print(f"risk_mode              : {snap.get('risk_mode', 1)} "
          f"(override={snap.get('risk_mode_override')})")
    print(f"winrate20              : {_fmt_pct(snap.get('winrate20', 0.0), 2)}")
    print(f"rolling_dd_pct         : {_fmt_pct(snap.get('rolling_dd_pct', 0.0), 2)}")
    print(f"equity_usd             : {_fmt_num(snap.get('equity_usd', 0.0))} "
          f"(HWM={_fmt_num(snap.get('equity_hwm_usd', 0.0))})")
    print(f"daily_realized_pnl_usd : {_fmt_num(snap.get('daily_realized_pnl_usd', 0.0))}")
    print(f"daily_realized_pnl_pct : {_fmt_pct(snap.get('daily_realized_pnl_pct', 0.0), 2)}")
    print(f"daily_drawdown_pct     : {_fmt_pct(snap.get('daily_drawdown_pct', 0.0), 2)}")
    print(f"regime_label           : {snap.get('regime_label')}")
    print(f"regime_risk_multiplier : {_fmt_num(snap.get('regime_risk_multiplier', 1.0), 3)}")
    print(f"regime_label_numeric   : {_fmt_num(snap.get('regime_label_numeric', 0.0), 2)}")
    print(f"regime_stress_score    : {_fmt_num(snap.get('regime_stress_score', 0.0), 2)}")
    print(f"regime_signal_strength : {_fmt_num(snap.get('regime_signal_strength', 0.0), 2)}")
    print(f"regime_entry_gate_fact : {_fmt_num(snap.get('regime_entry_gate_factor', 1.0), 3)}")
    print(f"consecutive_losses     : {snap.get('consecutive_losses', 0)}")
    print(f"cooldown_active        : {_fmt_bool(snap.get('cooldown_active', False))}")
    print(f"max_risk_per_trade     : {_fmt_pct(snap.get('max_risk_per_trade', 0.0), 2)}")
    print(f"min_sol_balance        : {_fmt_num(snap.get('min_sol_balance', 0.0), 4)}")
    print(f"strat_mode             : {snap.get('strat_mode')}")
    print(f"plan_equity_today_usd  : {_fmt_num(snap.get('plan_equity_today_usd', 0.0))}")
    print(f"plan_gap_pct           : {_fmt_pct(snap.get('plan_gap_pct', 0.0), 2)} "
          f"status={snap.get('plan_status')}")
    print(f"strat_daily_spend_cap  : "
          f"pct={_fmt_pct(snap.get('strat_daily_spend_cap_pct', 0.0), 2)} "
          f"usd={_fmt_num(snap.get('strat_daily_spend_cap_usd', 0.0))}")
    print(f"strat_max_conc_overr   : {snap.get('strat_max_concurrent_override')}")
    print(f"ai_capacity_delta      : {_fmt_num(snap.get('ai_capacity_delta', 0.0), 3)}")
    print(f"halt_reason            : {snap.get('halt_reason')}")
    print(f"daily_trades_count     : {snap.get('daily_trades_count', 0)} "
          f"/ {snap.get('daily_trades_cap', 0)}")
    print(f"sandbox_mode           : {_fmt_bool(snap.get('sandbox_mode', False))}")
    print(f"kelly_raw              : {_fmt_num(snap.get('kelly_raw', 0.0), 3)}")
    print(f"kelly_pos_pct_cap      : {_fmt_pct(snap.get('kelly_pos_pct_cap', 0.0), 2)}")
    print(f"equity_override_usd    : {_fmt_num(snap.get('equity_override_usd', 0.0))}")
    print(f"risk_profile           : {snap.get('risk_profile')} "
          f"(env={snap.get('risk_profile_env')}, "
          f"override={snap.get('risk_profile_override')})")
    print(f"engine_version         : {snap.get('risk_engine_version', 'n/a')}")
    print(f"trading_window_open    : {_fmt_bool(snap.get('trading_window_open', True))} "
          f"reason={snap.get('trading_window_reason', 'n/a')}")


async def cmd_budget(args: argparse.Namespace) -> None:
    """
    Daily budget / spend summary.
    """
    summary = await RM.risk_budget_summary()

    if _maybe_print_json(summary, as_json=args.json):
        return

    print("\n=== RISK BUDGET SUMMARY ===")
    print(f"equity_usd               : {_fmt_num(summary.get('equity_usd', 0.0))}")
    print(f"daily_spend_cap_usd      : {_fmt_num(summary.get('daily_spend_cap_usd', 0.0))}")
    print(f"daily_spent_usd          : {_fmt_num(summary.get('daily_spent_usd', 0.0))}")
    print(f"daily_spend_remaining_usd: "
          f"{_fmt_num(summary.get('daily_spend_remaining_usd', 0.0))}")
    print(f"daily_spend_used_pct     : "
          f"{_fmt_pct(summary.get('daily_spend_used_pct', 0.0) / 100.0, 2)}")
    print(f"sandbox_mode             : {_fmt_bool(summary.get('sandbox_mode', False))}")
    print(f"trading_window_open      : "
          f"{_fmt_bool(summary.get('trading_window_open', True))}")


async def cmd_ctx(args: argparse.Namespace) -> None:
    """
    Raw risk:ctx hash view.
    """
    ctx = await RM.read_risk_ctx_hash()

    if _maybe_print_json(ctx, as_json=args.json):
        return

    print("\n=== RISK CTX (risk:ctx hash) ===")
    _print_kv("ctx", {k: _fmt_num(v, 4) for k, v in ctx.items()})


async def cmd_health(args: argparse.Namespace) -> None:
    """
    Aggregate health view from risk_manager.risk_health().
    """
    # risk_health is defined in the "part 2" of risk_manager
    health = await RM.risk_health()

    if _maybe_print_json(health, as_json=args.json):
        return

    print("\n=== RISK HEALTH ===")
    # Expecting a dict with nested fields; be defensive.
    headline = health.get("headline") or {}
    details = health.get("details") or {}

    _print_kv("headline", {k: headline[k] for k in sorted(headline.keys())})
    for section_name, section in sorted(details.items()):
        if isinstance(section, dict):
            _print_kv(section_name, section)
        else:
            print(f"\n{section_name}")
            print("-" * len(section_name))
            print(section)


async def cmd_watch(args: argparse.Namespace) -> None:
    """
    Live watch mode: repeatedly print summary every N seconds.
    """
    interval = float(args.interval)
    if interval <= 0:
        interval = 10.0

    try:
        while True:
            # Clear screen for a "top"-like view
            if os.name == "nt":
                os.system("cls")
            else:
                os.system("clear")

            try:
                await cmd_summary(args)  # uses args.json=False in typical watch usage
            except Exception as e:
                log.error("watch: summary failed: %s", e)

            print(f"\n(Updated at {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}, "
                  f"refresh every {interval:.1f}s – Ctrl+C to stop)")
            await asyncio.sleep(interval)
    except KeyboardInterrupt:
        print("\nwatch: stopped by user")


async def cmd_block(args: argparse.Namespace) -> None:
    """
    Set or clear manual block (risk:manual_block).
    """
    if args.on and args.off:
        print("block: cannot use --on and --off at the same time", file=sys.stderr)
        sys.exit(1)

    if args.on:
        await RM.set_manual_block(True)
        print("✅ manual_block set to 1 (new entries will be blocked)")
    elif args.off:
        await RM.clear_manual_block()
        print("✅ manual_block cleared (manual risk block disabled)")
    else:
        # Just print current state
        dbg = await RM.entry_gate_debug()
        manual = dbg.get("manual_block", False)
        print(f"manual_block = {manual} "
              f"(set with: block --on / block --off)")


async def cmd_mode(args: argparse.Namespace) -> None:
    """
    View or set risk_mode override (0=OFF,1=NORMAL,2=AGGRESSIVE,3=DEFENSIVE).
    """
    if args.clear and args.set is not None:
        print("mode: cannot use --set and --clear together", file=sys.stderr)
        sys.exit(1)

    if args.set is not None:
        if args.set not in (0, 1, 2, 3):
            print("mode: --set must be one of 0,1,2,3", file=sys.stderr)
            sys.exit(1)
        await RM.force_risk_mode(args.set)
        print(f"✅ risk_mode override set to {args.set}")
        return

    if args.clear:
        await RM.force_risk_mode(None)
        print("✅ risk_mode override cleared (revert to capacity-based mode)")
        return

    # Just show current
    snap = await RM.risk_capacity_snapshot()
    print("\n=== RISK MODE OVERRIDE ===")
    print(f"risk_mode          : {snap.get('risk_mode', 1)}")
    print(f"risk_mode_override : {snap.get('risk_mode_override')}")
    print("(use: mode --set 0/1/2/3 or mode --clear)")


async def cmd_profile(args: argparse.Namespace) -> None:
    """
    View or set risk profile override (conservative / balanced / aggressive / auto).
    """
    if args.clear and args.set:
        print("profile: cannot use --set and --clear together", file=sys.stderr)
        sys.exit(1)

    if args.set:
        name = args.set.strip().lower()
        if name not in ("conservative", "balanced", "aggressive", "auto"):
            print(
                "profile: --set must be one of "
                "'conservative', 'balanced', 'aggressive', 'auto'",
                file=sys.stderr,
            )
            sys.exit(1)
        await RM.set_risk_profile_override(name)
        print(f"✅ risk profile override set to '{name}'")
        return

    if args.clear:
        await RM.set_risk_profile_override(None)
        print("✅ risk profile override cleared (revert to env/auto)")
        return

    snap = await RM.risk_snapshot()
    print("\n=== RISK PROFILE OVERRIDE ===")
    print(f"active_profile : {snap.get('risk_profile')}")
    print(f"env_profile    : {snap.get('risk_profile_env')}")
    print(f"override       : {snap.get('risk_profile_override')}")
    print("(use: profile --set NAME or profile --clear)")


async def cmd_cooldown(args: argparse.Namespace) -> None:
    """
    Clear risk cooldown (consecutive-loss / DD-based).

    This is an *ops* tool – use sparingly.
    """
    if not args.clear:
        # Just show whether cooldown is active
        dbg = await RM.entry_gate_debug()
        active = dbg.get("cooldown_active", False)
        print(f"cooldown_active = {active} "
              f"(clear with: cooldown --clear)")
        return

    await RM.clear_risk_cooldown()
    print("✅ risk cooldown cleared")


# --------------------------------------------------------------------------- #
# Argparse wiring
# --------------------------------------------------------------------------- #

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Risk manager diagnostics / ops CLI (V3+++)",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Output JSON for the selected read-only command.",
    )

    sub = parser.add_subparsers(dest="command", metavar="CMD")

    # summary (default)
    sub.add_parser("summary", help="One-shot high-level risk summary (default).")

    # gate
    sub.add_parser("gate", help="Detailed entry gate breakdown.")

    # capacity
    sub.add_parser("capacity", help="Risk capacity / regime snapshot.")

    # budget
    sub.add_parser("budget", help="Daily budget / spend summary.")

    # ctx
    sub.add_parser("ctx", help="Raw risk:ctx hash view.")

    # health
    sub.add_parser("health", help="Aggregate risk health view.")

    # watch
    p_watch = sub.add_parser("watch", help="Live watch mode (top-style).")
    p_watch.add_argument(
        "--interval",
        type=float,
        default=20.0,
        help="Refresh interval in seconds (default: 20).",
    )

    # block
    p_block = sub.add_parser("block", help="Set / clear manual risk block.")
    p_block.add_argument("--on", action="store_true", help="Set manual block=1.")
    p_block.add_argument("--off", action="store_true", help="Clear manual block.")

    # mode
    p_mode = sub.add_parser(
        "mode",
        help="View or set risk_mode override (0=OFF,1=NORMAL,2=AGGRESSIVE,3=DEFENSIVE).",
    )
    p_mode.add_argument(
        "--set",
        type=int,
        help="Force a specific risk_mode (0..3).",
    )
    p_mode.add_argument(
        "--clear",
        action="store_true",
        help="Clear risk_mode override.",
    )

    # profile
    p_profile = sub.add_parser(
        "profile",
        help="View or set risk profile override (conservative / balanced / aggressive / auto).",
    )
    p_profile.add_argument(
        "--set",
        type=str,
        help="Set profile override.",
    )
    p_profile.add_argument(
        "--clear",
        action="store_true",
        help="Clear profile override.",
    )

    # cooldown
    p_cd = sub.add_parser(
        "cooldown",
        help="View or clear risk cooldown (consecutive-loss / DD-based).",
    )
    p_cd.add_argument(
        "--clear",
        action="store_true",
        help="Clear active cooldown.",
    )

    return parser


COMMAND_HANDLERS: Dict[str, Callable[[argparse.Namespace], "asyncio.Future[Any]"]] = {
    "summary": cmd_summary,
    "gate": cmd_gate,
    "capacity": cmd_capacity,
    "budget": cmd_budget,
    "ctx": cmd_ctx,
    "health": cmd_health,
    "watch": cmd_watch,
    "block": cmd_block,
    "mode": cmd_mode,
    "profile": cmd_profile,
    "cooldown": cmd_cooldown,
}


async def _main_async(argv: Optional[list[str]] = None) -> None:
    parser = build_parser()
    args = parser.parse_args(argv)

    # Default command = summary
    command = args.command or "summary"

    # For watch + ops commands, JSON output isn't meaningful; ignore silently.
    if command in ("watch", "block", "mode", "profile", "cooldown"):
        args.json = False

    handler = COMMAND_HANDLERS.get(command)
    if handler is None:
        parser.print_help()
        sys.exit(1)

    await handler(args)


def main() -> None:
    try:
        asyncio.run(_main_async())
    except KeyboardInterrupt:
        print("\nCancelled by user.")
    except Exception as e:
        log.error("diagnostics_risk: fatal error: %s", e)
        sys.exit(1)


if __name__ == "__main__":
    main()
