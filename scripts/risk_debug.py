# scripts/risk_debug.py
# -*- coding: utf-8 -*-
from __future__ import annotations

"""
Risk debug / health check helper

Usage (from repo root):

    .\.venv\Scripts\python.exe scripts\risk_debug.py
    .\.venv\Scripts\python.exe scripts\risk_debug.py --watch 10
"""

import argparse
import asyncio
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from pprint import pformat

# ─────────────────────────────────────────────────────────────────────────────
# Ensure project root (folder that contains `utils_pkg`) is on sys.path
# ─────────────────────────────────────────────────────────────────────────────
_here = Path(__file__).resolve().parent
for parent in (_here, *_here.parents):
    if (parent / "utils_pkg").is_dir():
        if str(parent) not in sys.path:
            sys.path.insert(0, str(parent))
        break

from utils_pkg import risk_manager as rm  # type: ignore[attr-defined]


def _clear_screen() -> None:
    # nice for --watch mode
    try:
        os.system("cls" if os.name == "nt" else "clear")
    except Exception:
        pass


def _ts() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


async def _print_once(verbose: bool = False) -> None:
    # High-level combined health (snapshot + gate)
    health = await rm.risk_health()
    snap = health.get("snapshot", {})
    gate = health.get("entry_gate", {})

    allowed = bool(gate.get("allowed", False))
    reasons = gate.get("reasons_blocking", []) or []

    # Focused capacity view (for AI / regime / plan)
    cap = await rm.risk_capacity_snapshot()

    # Daily budget summary
    budget = await rm.risk_budget_summary()

    print("=" * 72)
    print(f"RISK DEBUG   {_ts()}")
    print("=" * 72)
    print()

    # ------------------------------------------------------------------ #
    # 1) Entry gate status
    # ------------------------------------------------------------------ #
    print("ENTRY STATUS")
    print("------------")
    print(f"  allowed: {allowed}")
    if reasons:
        print("  reasons blocking:")
        for r in reasons:
            print(f"    - {r}")
    else:
        print("  reasons blocking: (none)")
    print()

    # ------------------------------------------------------------------ #
    # 2) Key limits / sizing
    # ------------------------------------------------------------------ #
    equity = float(snap.get("equity_usd", 0.0))
    ticket = float(snap.get("ticket_usd", 0.0))
    pos_pct = float(snap.get("pos_pct_cap", 0.0))
    max_conc = int(snap.get("max_concurrent", 0))
    daily_spend = float(snap.get("daily_spend_usd", 0.0))
    daily_spent = float(snap.get("daily_spent_usd", 0.0))
    spend_used_pct = float(snap.get("daily_spend_used_pct", 0.0))

    print("SIZING / LIMITS")
    print("---------------")
    print(f"  equity_usd:           {equity:10.2f}")
    print(f"  ticket_usd:           {ticket:10.2f}")
    print(f"  pos_pct_cap:          {pos_pct * 100:10.2f}%")
    print(f"  max_concurrent:       {max_conc}")
    print(f"  daily_spend_cap_usd:  {daily_spend:10.2f}")
    print(f"  daily_spent_usd:      {daily_spent:10.2f}  ({spend_used_pct:5.2f}%)")
    print()

    # ------------------------------------------------------------------ #
    # 3) Risk capacity / regime / performance
    # ------------------------------------------------------------------ #
    winrate = float(snap.get("winrate20", 0.0))
    dd = float(snap.get("rolling_dd_pct", 0.0))
    daily_pnl = float(snap.get("daily_realized_pnl_usd", 0.0))
    risk_capacity = float(snap.get("risk_capacity", 0.0))
    risk_mode = int(snap.get("risk_mode", 1))
    profile = str(snap.get("risk_profile", "unknown"))

    regime_label = str(snap.get("regime_label", "unknown"))
    regime_mult = float(snap.get("regime_risk_multiplier", 1.0))
    regime_gate = float(snap.get("regime_entry_gate_factor", 1.0))

    kelly_raw = float(snap.get("kelly_raw", 0.0))
    kelly_cap = float(snap.get("kelly_pos_pct_cap", 0.0))
    eq_util = float(snap.get("equity_utilization_pct", 0.0))

    print("RISK / REGIME / PERFORMANCE")
    print("---------------------------")
    print(f"  risk_capacity:        {risk_capacity:6.3f}")
    print(f"  risk_mode:            {risk_mode}   (0=off,1=normal,2=agg,3=def)")
    print(f"  risk_profile:         {profile}")
    print(f"  winrate20:            {winrate:6.3f}")
    print(f"  rolling_dd_pct:       {dd * 100:6.2f}%")
    print(f"  daily_realized_pnl:   {daily_pnl:10.2f} USD")
    print(f"  regime_label:         {regime_label}")
    print(f"  regime_risk_mult:     {regime_mult:6.3f}")
    print(f"  regime_entry_gate:    {regime_gate:6.3f}")
    print(f"  kelly_raw:            {kelly_raw:6.3f}")
    print(f"  kelly_pos_pct_cap:    {kelly_cap * 100:6.2f}%")
    print(f"  equity_utilization:   {eq_util:6.2f}%")
    print()

    # ------------------------------------------------------------------ #
    # 4) Trading window & sandbox
    # ------------------------------------------------------------------ #
    tw_open = bool(cap.get("trading_window_open", snap.get("trading_window_open", True)))
    tw_reason = cap.get("trading_window_reason") or snap.get("trading_window_reason")
    sandbox = bool(snap.get("sandbox_mode", False))

    print("EXECUTION CONTEXT")
    print("------------------")
    print(f"  trading_window_open:  {tw_open}  ({tw_reason or 'n/a'})")
    print(f"  sandbox_mode:         {sandbox}")
    print()

    # ------------------------------------------------------------------ #
    # 5) Budget quick view (from risk_budget_summary)
    # ------------------------------------------------------------------ #
    print("BUDGET SUMMARY")
    print("--------------")
    print(f"  equity_usd:           {budget.get('equity_usd', 0.0):10.2f}")
    print(f"  daily_spend_cap_usd:  {budget.get('daily_spend_cap_usd', 0.0):10.2f}")
    print(f"  daily_spent_usd:      {budget.get('daily_spent_usd', 0.0):10.2f}")
    print(f"  daily_spend_remaining:{budget.get('daily_spend_remaining_usd', 0.0):10.2f}")
    print(f"  daily_spend_used_pct: {budget.get('daily_spend_used_pct', 0.0):6.2f}%")
    print(f"  trading_window_open:  {budget.get('trading_window_open', True)}")
    print()

    if verbose:
        print("RAW SNAPSHOT (risk_snapshot)")
        print("----------------------------")
        print(pformat(snap, sort_dicts=True))
        print()
        print("RAW CAPACITY SNAPSHOT (risk_capacity_snapshot)")
        print("--------------------------------------------")
        print(pformat(cap, sort_dicts=True))
        print()
        print("RAW ENTRY GATE DEBUG (entry_gate_debug)")
        print("--------------------------------------")
        print(pformat(gate, sort_dicts=True))
        print()


async def _run(watch: float, verbose: bool) -> None:
    if watch <= 0:
        await _print_once(verbose=verbose)
        return

    # watch mode: loop every `watch` seconds, clearing the screen
    while True:
        _clear_screen()
        await _print_once(verbose=verbose)
        await asyncio.sleep(watch)


def main() -> None:
    parser = argparse.ArgumentParser(description="Risk engine debug helper")
    parser.add_argument(
        "--watch",
        type=float,
        default=0.0,
        help="Refresh every N seconds (0 = one-shot)",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Print full raw dictionaries for deep debugging",
    )
    args = parser.parse_args()
    asyncio.run(_run(watch=args.watch, verbose=args.verbose))


if __name__ == "__main__":
    main()
