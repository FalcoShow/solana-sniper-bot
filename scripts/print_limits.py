#!/usr/bin/env python3
from __future__ import annotations

"""
scripts/print_limits.py — Live risk / regime / entry-gate snapshot (P0-ready, V3+++)

Purpose
-------
A *fast*, *safe* CLI probe that answers: “WHY aren’t we trading?” and “WHAT are the
current limits?” without requiring any other tooling.

It surfaces:
  • Key ENV knobs (non-secret)
  • risk_manager.current_limits() + risk_snapshot() + capacity signals
  • allow_new_entry / gating context
  • regime_state.features() (if available)
  • optional AI entry diagnostics (best-effort, non-fatal)

Design goals
------------
- Production-safe: uses core_utils.init_resources()/close_resources()
- Degrades gracefully if optional modules/APIs differ or are missing
- JSON mode + watch dashboard mode
- No side-effects (read-only)

Usage
-----
  python -m scripts.print_limits
  python scripts/print_limits.py --json
  python scripts/print_limits.py --watch 10
  python scripts/print_limits.py --watch 5 --no-ai
"""

import argparse
import asyncio
import json
import os
import sys
from collections.abc import Awaitable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

# ──────────────────────────────────────────────────────────────
# Repo root bootstrap (so scripts/ can import utils_pkg/)
# ──────────────────────────────────────────────────────────────

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# Optional .env bootstrap (local only; Render uses env vars)
try:
    from dotenv import find_dotenv, load_dotenv  # type: ignore
except Exception:  # pragma: no cover
    load_dotenv = None  # type: ignore
    find_dotenv = None  # type: ignore

if load_dotenv and find_dotenv:
    load_dotenv(find_dotenv(), override=False)

# Shared infra (Redis / Solana / HTTP clients). Must be initialized.
import utils_pkg.core_utils as cu  # type: ignore

# Optional modules (degrade gracefully)
try:  # pragma: no cover
    from utils_pkg import risk_manager as rm  # type: ignore
except Exception:  # pragma: no cover
    rm = None  # type: ignore

try:  # pragma: no cover
    from utils_pkg.regime import regime_state  # type: ignore
except Exception:  # pragma: no cover
    regime_state = None  # type: ignore

try:  # pragma: no cover
    from utils_pkg import ai_entry_logic  # type: ignore
except Exception:  # pragma: no cover
    ai_entry_logic = None  # type: ignore


# ──────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────


def _now_iso() -> str:
    return datetime.now(UTC).isoformat()


def _env_str(name: str, default: str = "") -> str:
    v = os.getenv(name)
    return v.strip() if isinstance(v, str) and v.strip() else default


def _env_bool(name: str, default: bool = False) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return str(v).strip().lower() in {"1", "true", "yes", "y", "on"}


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except Exception:
        return default


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)))
    except Exception:
        return default


def _pct01(x: float | None) -> str:
    if x is None:
        return "?.??%"
    try:
        return f"{float(x) * 100:.2f}%"
    except Exception:
        return "?.??%"


def _pct100(x: float | None) -> str:
    if x is None:
        return "?.??%"
    try:
        return f"{float(x):.2f}%"
    except Exception:
        return "?.??%"


def _usd(x: float | None) -> str:
    if x is None:
        return "$?.??"
    try:
        return f"${float(x):,.2f}"
    except Exception:
        return f"${x}"


def _num(x: Any, default: float = 0.0) -> float:
    try:
        if x is None or x == "":
            return default
        return float(x)
    except Exception:
        return default


def _int(x: Any, default: int = 0) -> int:
    try:
        if x is None or x == "":
            return default
        return int(float(x))
    except Exception:
        return default


async def _maybe_await(v: Any) -> Any:
    if asyncio.iscoroutine(v) or isinstance(v, Awaitable):
        return await v
    return v


async def _call_maybe_async(fn: Any, *args: Any, **kwargs: Any) -> Any:
    """
    Call sync or async function safely.
    """
    if fn is None or not callable(fn):
        return None
    try:
        out = fn(*args, **kwargs)
        return await _maybe_await(out)
    except Exception:
        return None


def _clear_screen() -> None:
    os.system("cls" if os.name == "nt" else "clear")


def _collect_env_summary() -> dict[str, Any]:
    """
    Key knobs affecting behavior (avoid secrets).
    Keep this list compact but high-signal.
    """
    # Wallet pubkey is safe, private key is not. Never print priv key / seed.
    wallet_pub = (
        _env_str("WALLET_PUBKEY")
        or _env_str("WALLET_PUBLIC_KEY")
        or _env_str("WALLET_ADDRESS")
        or "<unset>"
    )

    # Normalize AI entry enable flags (some repos use AI_ENTRY_ENABLED vs ENTRY_AI_ENABLED)
    ai_enabled = _env_str("AI_ENTRY_ENABLED") or _env_str("ENTRY_AI_ENABLED") or "1"

    return {
        "NETWORK": _env_str("NETWORK") or _env_str("SOLANA_CLUSTER") or "<auto>",
        "RISK_PROFILE": _env_str("RISK_PROFILE") or "<default>",
        "WALLET_PUBKEY": wallet_pub,
        "RISK_MANAGER_ENABLED": _env_str("RISK_MANAGER_ENABLED", "1"),
        "REGIME_ENABLED": _env_str("REGIME_ENABLED", "1"),
        "EXIT_ENGINE_ENABLED": _env_str("EXIT_ENGINE_ENABLED", "1"),
        "ENTRY_MOMENTUM_ENABLED": _env_str("ENTRY_MOMENTUM_ENABLED", "1"),
        "ENTRY_AI_ENABLED": ai_enabled,
        "ENTRY_REQUIRE_RISK_CAPACITY": _env_str("ENTRY_REQUIRE_RISK_CAPACITY", "0"),
        "ENTRY_MIN_INITIAL_BUY_SOL": _env_str("ENTRY_MIN_INITIAL_BUY_SOL") or "<unset>",
        "RISK_MAX_PER_TRADE": _env_str("RISK_MAX_PER_TRADE", "0.045"),
        "MAX_CONCURRENT_POS": _env_str("MAX_CONCURRENT_POS", "4"),
        "STOP_AFTER_DAILY_PROFIT_PCT": _env_str("STOP_AFTER_DAILY_PROFIT_PCT", "0"),
        "STOP_AFTER_DAILY_LOSS_PCT": _env_str("STOP_AFTER_DAILY_LOSS_PCT", "0"),
        "DAILY_SPEND_CAP_PCT": _env_str("DAILY_SPEND_CAP_PCT", "0.15"),
        "DAILY_SPEND_CAP_USD": _env_str("DAILY_SPEND_CAP_USD") or "<auto>",
        "RISK_MODE_OVERRIDE": _env_str("RISK_MODE_OVERRIDE") or "<none>",
        "HALT_KEYS_USED": ["halt:infra", "halt:manual", "halt:daily_loss"],
    }


async def _maybe_ai_diag() -> dict[str, Any]:
    """
    Best-effort AI entry diagnostics. Non-fatal.
    We probe for a few common patterns across rewrites.
    """
    if ai_entry_logic is None:
        return {}

    # Preferred: ai_entry_logic.diag_snapshot() / gate_snapshot()
    for attr in ("diag_snapshot", "gate_snapshot"):
        fn = getattr(ai_entry_logic, attr, None)
        out = await _call_maybe_async(fn)
        if isinstance(out, dict) and out:
            out["_source"] = attr
            return out

    # Fallback: ai_entry_logic.current_diag dict
    cur = getattr(ai_entry_logic, "current_diag", None)
    if isinstance(cur, dict) and cur:
        d = dict(cur)
        d["_source"] = "current_diag"
        return d

    return {}


async def _risk_calls(include_limits: bool = True) -> dict[str, Any]:
    """
    Gather risk_manager data with compatibility across older/newer APIs.
    """
    out: dict[str, Any] = {
        "current_limits": {},
        "risk_snapshot": {},
        "risk_capacity_snapshot": {},
        "risk_capacity_score": None,
        "allow_new_entry": None,
        "errors": [],
    }
    if rm is None:
        out["errors"].append("risk_manager import missing")
        return out

    # Current limits
    if include_limits:
        v = await _call_maybe_async(getattr(rm, "current_limits", None))
        if isinstance(v, dict):
            out["current_limits"] = v
        elif v is None:
            out["errors"].append("current_limits unavailable")

    # Risk snapshot
    v = await _call_maybe_async(getattr(rm, "risk_snapshot", None))
    if isinstance(v, dict):
        out["risk_snapshot"] = v
    elif v is None:
        # Some repos return snapshot via risk_capacity_snapshot only; ok.
        pass

    # Capacity snapshot
    v = await _call_maybe_async(getattr(rm, "risk_capacity_snapshot", None))
    if isinstance(v, dict):
        out["risk_capacity_snapshot"] = v

    # Capacity score
    v = await _call_maybe_async(getattr(rm, "risk_capacity_score", None))
    if isinstance(v, int | float):
        out["risk_capacity_score"] = float(v)

    # Allow entry
    v = await _call_maybe_async(getattr(rm, "should_allow_new_entry", None))
    if isinstance(v, bool):
        out["allow_new_entry"] = v

    return out


async def _regime_calls() -> dict[str, Any]:
    """
    Gather regime_state features with compatibility (sync/async, optional).
    """
    if regime_state is None:
        return {"features": {}, "errors": ["regime_state import missing"]}

    feats = await _call_maybe_async(getattr(regime_state, "features", None))
    if isinstance(feats, dict):
        return {"features": feats, "errors": []}

    # fallback: maybe regime_state.snapshot()
    snap = await _call_maybe_async(getattr(regime_state, "snapshot", None))
    if isinstance(snap, dict):
        return {"features": snap, "errors": []}

    return {"features": {}, "errors": ["regime_state features unavailable"]}


def _derive_status(
    limits: dict[str, Any],
    rcap_score: float | None,
    allow_entry: bool | None,
    regime_label: str,
) -> str:
    """
    Simple traffic-light status string.
    """
    risk_mode = _int(limits.get("risk_mode"), 1)
    if risk_mode == 0:
        return "🔴 HALTED (risk_mode=0)"
    if allow_entry is False:
        return "🔴 HALTED (gate denies new entry)"
    if rcap_score is not None and rcap_score < 0.35:
        return "🟠 DEFENSIVE (capacity low)"
    if str(regime_label).lower() in {"risk_off", "off", "danger"}:
        return "🟠 DEFENSIVE (regime risk_off)"
    if (
        rcap_score is not None
        and rcap_score >= 0.80
        and str(regime_label).lower() in {"risk_on", "on"}
    ):
        return "🟢 AGGRESSIVE window (risk_on + high capacity)"
    return "🟡 NORMAL"


def _pretty_print(snapshot: dict[str, Any]) -> None:
    env = snapshot.get("env", {}) or {}

    risk = snapshot.get("risk", {}) or {}
    limits = risk.get("current_limits", {}) or {}
    rsnap = risk.get("risk_snapshot", {}) or {}
    rcap = risk.get("risk_capacity_snapshot", {}) or {}
    rcap_score = risk.get("risk_capacity_score", None)
    allow_entry = risk.get("allow_new_entry", None)

    regime = snapshot.get("regime", {}) or {}
    rfeats = regime.get("features", {}) or {}

    ai = snapshot.get("ai_entry_diag", {}) or {}

    # Pull commonly used fields defensively
    equity = _num(limits.get("equity_usd") or rsnap.get("equity_usd"), 0.0)
    ticket = _num(limits.get("ticket_usd") or rsnap.get("ticket_usd"), 0.0)
    pos_pct_cap = _num(limits.get("pos_pct_cap"), 0.0)
    max_conc = _int(limits.get("max_concurrent"), _env_int("MAX_CONCURRENT_POS", 4))
    daily_spend = _num(limits.get("daily_spend_usd") or rcap.get("daily_spend_usd"), 0.0)

    risk_mode = _int(limits.get("risk_mode") or rsnap.get("risk_mode"), 1)
    risk_mode_override = limits.get("risk_mode_override") or env.get("RISK_MODE_OVERRIDE")

    winrate20 = _num(rcap.get("winrate20"), 0.0)
    rolling_dd_pct = _num(rcap.get("rolling_dd_pct"), 0.0)
    day_pnl_usd = _num(rcap.get("daily_realized_pnl_usd"), 0.0)
    day_pnl_pct = _num(rcap.get("daily_realized_pnl_pct"), 0.0)

    regime_label = rcap.get("regime_label") or rfeats.get("regime_label") or "normal"
    regime_mult = _num(
        rcap.get("regime_risk_multiplier") or rfeats.get("regime_risk_multiplier"),
        1.0,
    )

    # Derived governance thresholds
    stop_profit_pct = _env_float(
        "STOP_AFTER_DAILY_PROFIT_PCT", 0.0
    )  # env is % or fraction? assume fraction (0.04)
    stop_loss_pct = _env_float("STOP_AFTER_DAILY_LOSS_PCT", 0.0)  # fraction
    spend_cap_pct = _env_float("DAILY_SPEND_CAP_PCT", 0.0)  # fraction
    spend_cap_usd_env = _env_str("DAILY_SPEND_CAP_USD", "")

    status = _derive_status(
        limits,
        rcap_score if isinstance(rcap_score, int | float) else None,
        allow_entry,
        str(regime_label),
    )

    print("")
    print("=== 📊 Live Limits / Risk / Regime Snapshot (V3+++ / P0) ===")
    print(f"generated_at: {snapshot.get('generated_at')}")
    print("")

    # ENV
    print("[ENV / FEATURE GATES]")
    for k in sorted(env.keys()):
        print(f"  {k:28s} = {env.get(k)}")
    print("")

    # LIMITS
    print("[CAPITAL & LIMITS]")
    print(f"  equity_usd               : {_usd(equity)}")
    print(f"  ticket_usd               : {_usd(ticket)}")
    print(f"  pos_pct_cap              : {_pct01(pos_pct_cap)}")
    print(f"  max_concurrent           : {max_conc}")
    print(f"  daily_spend_usd          : {_usd(daily_spend)}")
    if equity > 0 and spend_cap_pct > 0:
        print(
            f"  daily_spend_cap_pct      : {_pct01(spend_cap_pct)}  (~{_usd(equity * spend_cap_pct)})"
        )
    else:
        print(f"  daily_spend_cap_pct      : {_pct01(spend_cap_pct)}")
    if spend_cap_usd_env and spend_cap_usd_env != "<auto>":
        try:
            print(f"  daily_spend_cap_usd_env  : {_usd(float(spend_cap_usd_env))}")
        except Exception:
            print(f"  daily_spend_cap_usd_env  : {spend_cap_usd_env}")
    print("")

    # GOVERNANCE / CAPACITY
    print("[RISK CAPACITY / GOVERNANCE]")
    mode_lbl = (
        "OFF"
        if risk_mode == 0
        else "DEFENSIVE"
        if risk_mode == 3
        else "AGGR"
        if risk_mode == 2
        else "NORMAL"
    )
    print(f"  risk_mode                : {risk_mode} ({mode_lbl})")
    print(f"  risk_mode_override       : {risk_mode_override}")
    if isinstance(rcap_score, int | float):
        print(f"  risk_capacity_score      : {float(rcap_score):.3f}")
    else:
        print("  risk_capacity_score      : <n/a>")
    if allow_entry is None:
        print("  allow_new_entry          : <n/a>")
    else:
        print(f"  allow_new_entry          : {'YES' if allow_entry else 'NO'}")
    print("")
    print(f"  winrate_last_20_trades   : {_pct01(winrate20)}")
    print(f"  rolling_drawdown_pct     : {_pct01(rolling_dd_pct)}")
    print(f"  daily_realized_pnl_usd   : {_usd(day_pnl_usd)}")
    print(f"  daily_realized_pnl_pct   : {_pct01(day_pnl_pct)}")

    if stop_profit_pct > 0 or stop_loss_pct > 0:
        print("")
        print("[DAILY STOPS]")
        print(
            f"  stop_after_daily_profit  : {_pct01(stop_profit_pct)}"
            + (
                f" (~{_usd(equity * stop_profit_pct)})"
                if equity > 0 and stop_profit_pct > 0
                else ""
            )
        )
        print(
            f"  stop_after_daily_loss    : {_pct01(stop_loss_pct)}"
            + (
                f" (~{_usd(equity * abs(stop_loss_pct))})"
                if equity > 0 and stop_loss_pct > 0
                else ""
            )
        )
    print("")

    # REGIME
    print("[REGIME]")
    print(f"  regime_label             : {regime_label}")
    print(f"  regime_risk_multiplier   : {regime_mult:.3f}")
    # Print a few common feature keys if present
    for k in (
        "trend_score",
        "regime_trend",
        "volatility_score",
        "regime_volatility",
        "liquidity_score",
        "regime_liquidity",
        "manipulation_score",
        "regime_manipulation_risk",
    ):
        if k in rfeats:
            try:
                print(f"  {k:24s} : {float(rfeats[k]):+.3f}")
            except Exception:
                print(f"  {k:24s} : {rfeats[k]}")
    print("")

    # AI (optional)
    if ai:
        print("[AI ENTRY DIAGNOSTICS]")
        src = ai.get("_source", "<unknown>")
        print(f"  source                  : {src}")
        # Print a few high-signal keys if they exist
        for k in (
            "ai_score_gate",
            "score_gate",
            "min_momentum_score",
            "model",
            "score_model",
            "last_update",
            "version",
        ):
            if k in ai:
                print(f"  {k:24s} : {ai.get(k)}")
        # Keep remaining keys small
        extra_keys = [
            k
            for k in ai
            if k
            not in {
                "_source",
                "ai_score_gate",
                "score_gate",
                "min_momentum_score",
                "model",
                "score_model",
                "last_update",
                "version",
            }
        ]
        if extra_keys:
            print(
                f"  extra_keys              : {', '.join(sorted(extra_keys)[:12])}"
                + (" ..." if len(extra_keys) > 12 else "")
            )
        print("")

    # Errors, if any
    errors = []
    errors.extend(risk.get("errors") or [])
    errors.extend(regime.get("errors") or [])
    if errors:
        print("[WARNINGS]")
        for e in errors:
            print(f"  - {e}")
        print("")

    print(f"Status: {status}")
    print("")


# ──────────────────────────────────────────────────────────────
# Snapshot gatherer
# ──────────────────────────────────────────────────────────────


async def collect_snapshot(*, include_ai: bool, include_regime: bool) -> dict[str, Any]:
    env = _collect_env_summary()

    risk_task = asyncio.create_task(_risk_calls(include_limits=True))
    regime_task = asyncio.create_task(_regime_calls()) if include_regime else None
    ai_task = asyncio.create_task(_maybe_ai_diag()) if include_ai else None

    risk = await risk_task
    regime = (
        await regime_task
        if regime_task is not None
        else {"features": {}, "errors": ["regime skipped"]}
    )
    ai = await ai_task if ai_task is not None else {}

    snap: dict[str, Any] = {
        "generated_at": _now_iso(),
        "env": env,
        "risk": risk,
        "regime": regime,
        "ai_entry_diag": ai,
    }
    return snap


# ──────────────────────────────────────────────────────────────
# CLI
# ──────────────────────────────────────────────────────────────


def build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Print live risk / limits / regime snapshot (V3+++).")
    p.add_argument("--json", action="store_true", help="Emit JSON instead of pretty text.")
    p.add_argument("--watch", type=int, default=0, help="Refresh every N seconds (dashboard mode).")
    p.add_argument("--no-ai", action="store_true", help="Skip AI entry diagnostics.")
    p.add_argument("--no-regime", action="store_true", help="Skip regime details.")
    p.add_argument("--no-clear", action="store_true", help="Do not clear screen in watch mode.")
    return p


async def run_once(args: argparse.Namespace) -> None:
    snap = await collect_snapshot(include_ai=not args.no_ai, include_regime=not args.no_regime)
    if args.json:
        print(json.dumps(snap, indent=2, default=str))
    else:
        _pretty_print(snap)


async def run_watch(args: argparse.Namespace) -> None:
    interval = max(1, int(args.watch))
    try:
        while True:
            if not args.no_clear and not args.json:
                _clear_screen()
            await run_once(args)
            if not args.json:
                print(f"[print_limits] refreshing in {interval}s (Ctrl+C to stop)…")
            await asyncio.sleep(interval)
    except KeyboardInterrupt:
        print("\n[print_limits] stopped.")


def main() -> None:
    args = build_arg_parser().parse_args()

    # IMPORTANT: bring up shared infra so risk_manager/regime can hit Redis safely.
    asyncio.run(_main_async(args))


async def _main_async(args: argparse.Namespace) -> None:
    await cu.init_resources()
    try:
        if args.watch and args.watch > 0:
            await run_watch(args)
        else:
            await run_once(args)
    finally:
        await cu.close_resources()


if __name__ == "__main__":
    main()
