# scripts/risk_debug.py
from __future__ import annotations

"""
scripts/risk_debug.py

Risk debug / health helper (P0-safe, production-friendly)
=========================================================

This file is intentionally **non-canonical** and exists for:
- quick human debugging,
- watch-mode monitoring,
- backward compatibility with old workflows.

Canonical tool is:
    python -m scripts.diagnostics_risk ...

This script stays safe:
- best-effort imports
- timeouts on risk_manager calls
- never prints secrets
- optional JSON output + file output
- optional CI-style gating (exit non-zero if blocked)

Usage
-----
One shot:
    python -m scripts.risk_debug
    python -m scripts.risk_debug --verbose
    python -m scripts.risk_debug --json

Watch mode:
    python -m scripts.risk_debug --watch 10
    python -m scripts.risk_debug --watch 10 --no-clear

CI-ish gating:
    python -m scripts.risk_debug --require-allowed
    python -m scripts.risk_debug --require-capacity 0.30
"""

import argparse
import asyncio
import contextlib
import json
import os
import sys
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from pprint import pformat
from typing import Any

# ─────────────────────────────────────────────────────────────────────────────
# Ensure project root is on sys.path (folder that contains utils_pkg/)
# ─────────────────────────────────────────────────────────────────────────────

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# Optional: load .env locally (Render uses env vars, so this is just convenience)
try:  # pragma: no cover
    from dotenv import find_dotenv, load_dotenv  # type: ignore
except Exception:  # pragma: no cover
    load_dotenv = None  # type: ignore
    find_dotenv = None  # type: ignore

if load_dotenv and find_dotenv:
    with contextlib.suppress(Exception):
        load_dotenv(find_dotenv(), override=False)

# Canonical logging
try:
    from logging_setup import get_logger  # type: ignore

    log = get_logger(__name__)
except Exception:  # pragma: no cover
    import logging

    logging.basicConfig(level=logging.INFO)
    log = logging.getLogger("risk_debug")

# Canonical risk brain
try:
    from utils_pkg import risk_manager as RM  # type: ignore
except Exception as e:  # pragma: no cover
    RM = None  # type: ignore[assignment]
    log.error("Failed to import utils_pkg.risk_manager: %r", e)

# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────


def _utc_ts() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S UTC")


def _clear_screen() -> None:
    with contextlib.suppress(Exception):
        os.system("cls" if os.name == "nt" else "clear")


def _safe_str(x: Any, max_len: int = 220) -> str:
    try:
        s = str(x)
    except Exception:
        s = repr(x)
    if len(s) > max_len:
        return s[: max_len - 3] + "..."
    return s


def _fmt_bool(x: Any) -> str:
    return "yes" if bool(x) else "no"


def _fmt_num(x: Any, decimals: int = 2) -> str:
    try:
        v = float(x)
    except Exception:
        return str(x)
    if abs(v) >= 1000:
        return f"{v:,.{decimals}f}"
    return f"{v:.{decimals}f}"


def _fmt_pct(x: Any, decimals: int = 2, *, assume_fraction: bool = True) -> str:
    try:
        v = float(x)
    except Exception:
        return str(x)
    if assume_fraction:
        v *= 100.0
    return f"{v:.{decimals}f}%"


def _risk_mode_label(mode: Any) -> str:
    try:
        m = int(mode)
    except Exception:
        return "unknown"
    return {0: "OFF", 1: "NORMAL", 2: "AGGRESSIVE", 3: "DEFENSIVE"}.get(m, "unknown")


def _as_dict(x: Any) -> dict[str, Any]:
    return x if isinstance(x, dict) else {"value": x}


async def _maybe_await(x: Any) -> Any:
    if asyncio.iscoroutine(x):
        return await x
    return x


async def _call_with_timeout(
    fn: Callable[..., Any], timeout_s: float, *args: Any, **kwargs: Any
) -> Any:
    async def _runner() -> Any:
        return await _maybe_await(fn(*args, **kwargs))

    return await asyncio.wait_for(_runner(), timeout=timeout_s)


@dataclass
class CallResult:
    ok: bool
    value: Any = None
    error: str | None = None
    ms: int = 0


async def _safe_call(
    obj: Any, name: str, timeout_s: float, *args: Any, **kwargs: Any
) -> CallResult:
    t0 = time.perf_counter()
    if obj is None:
        return CallResult(
            False, error="risk_manager not importable", ms=int((time.perf_counter() - t0) * 1000)
        )

    fn = getattr(obj, name, None)
    if not callable(fn):
        return CallResult(
            False,
            error=f"risk_manager.{name} not available",
            ms=int((time.perf_counter() - t0) * 1000),
        )

    try:
        val = await _call_with_timeout(fn, timeout_s, *args, **kwargs)
        return CallResult(True, value=val, ms=int((time.perf_counter() - t0) * 1000))
    except TimeoutError:
        return CallResult(
            False,
            error=f"{name} timed out after {timeout_s}s",
            ms=int((time.perf_counter() - t0) * 1000),
        )
    except Exception as e:
        return CallResult(False, error=_safe_str(e), ms=int((time.perf_counter() - t0) * 1000))


def _atomic_write(path: Path, data: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(data, encoding="utf-8")
    tmp.replace(path)


def _emit(payload: Any, *, as_json: bool, out: str | None) -> None:
    text = json.dumps(payload, indent=2, sort_keys=True, default=str) if as_json else str(payload)

    if out:
        p = Path(out)
        if not p.is_absolute():
            p = (ROOT / p).resolve()
        _atomic_write(p, text + ("" if text.endswith("\n") else "\n"))
        print(f"✅ Wrote {p}")
    else:
        print(text, end="" if text.endswith("\n") else "\n")


# ─────────────────────────────────────────────────────────────────────────────
# Data collection
# ─────────────────────────────────────────────────────────────────────────────


async def _collect(timeout_s: float) -> dict[str, Any]:
    """
    Collect risk state defensively. We try:
      - risk_health (if present)
      - plus explicit snapshots for stability across risk_manager drift
    """
    health_r = await _safe_call(RM, "risk_health", timeout_s)
    snap_r = await _safe_call(RM, "risk_snapshot", timeout_s)
    gate_r = await _safe_call(RM, "entry_gate_debug", timeout_s)
    cap_r = await _safe_call(RM, "risk_capacity_snapshot", timeout_s)
    bud_r = await _safe_call(RM, "risk_budget_summary", timeout_s)
    ctx_r = await _safe_call(RM, "read_risk_ctx_hash", timeout_s)

    health = _as_dict(health_r.value) if health_r.ok else {"_error": health_r.error}
    snap = _as_dict(snap_r.value) if snap_r.ok else {"_error": snap_r.error}
    gate = _as_dict(gate_r.value) if gate_r.ok else {"_error": gate_r.error}
    cap = _as_dict(cap_r.value) if cap_r.ok else {"_error": cap_r.error}
    budget = _as_dict(bud_r.value) if bud_r.ok else {"_error": bud_r.error}
    ctx = _as_dict(ctx_r.value) if ctx_r.ok else {"_error": ctx_r.error}

    # Some versions of risk_health include snapshot/entry_gate; if so, prefer them
    if isinstance(health, dict):
        if isinstance(health.get("snapshot"), dict) and "_error" not in health["snapshot"]:
            snap = health["snapshot"]
        if isinstance(health.get("entry_gate"), dict) and "_error" not in health["entry_gate"]:
            gate = health["entry_gate"]

    return {
        "generated_at": _utc_ts(),
        "timings_ms": {
            "risk_health": health_r.ms,
            "risk_snapshot": snap_r.ms,
            "entry_gate_debug": gate_r.ms,
            "risk_capacity_snapshot": cap_r.ms,
            "risk_budget_summary": bud_r.ms,
            "read_risk_ctx_hash": ctx_r.ms,
        },
        "health": health,
        "snapshot": snap,
        "entry_gate": gate,
        "capacity": cap,
        "budget": budget,
        "ctx": ctx,
        "note": "Canonical CLI is scripts/diagnostics_risk.py (this file is a convenience wrapper).",
    }


# ─────────────────────────────────────────────────────────────────────────────
# Presentation
# ─────────────────────────────────────────────────────────────────────────────


def _print_human(data: dict[str, Any], *, verbose: bool) -> None:
    snap = _as_dict(data.get("snapshot", {}))
    gate = _as_dict(data.get("entry_gate", {}))
    cap = _as_dict(data.get("capacity", {}))
    budget = _as_dict(data.get("budget", {}))
    ctx = _as_dict(data.get("ctx", {}))

    print("=" * 78)
    print(f"RISK DEBUG  |  {data.get('generated_at')}   (use: python -m scripts.diagnostics_risk)")
    print("=" * 78)
    print("")

    # Entry gate
    print("ENTRY STATUS")
    print("------------")
    if "_error" in gate:
        print(f"  entry_gate_debug: ERROR: {gate['_error']}")
    else:
        allowed = bool(gate.get("allowed", False))
        reasons = gate.get("reasons_blocking", []) or []
        print(f"  allowed            : {_fmt_bool(allowed)}")
        if not allowed and reasons:
            print("  reasons_blocking   :")
            for r in reasons:
                print(f"    - {r}")
        else:
            print("  reasons_blocking   : (none)")
        halt_reason = gate.get("halt_reason") or snap.get("halt_reason")
        if halt_reason:
            print(f"  halt_reason        : {halt_reason}")
        print(f"  manual_block       : {_fmt_bool(gate.get('manual_block', False))}")
        print(f"  infra_halt         : {_fmt_bool(gate.get('infra_halt', False))}")
        print(f"  cooldown_active    : {_fmt_bool(gate.get('cooldown_active', False))}")
    print("")

    # Sizing / limits (from snapshot)
    print("SIZING / LIMITS")
    print("---------------")
    if "_error" in snap:
        print(f"  risk_snapshot: ERROR: {snap['_error']}")
    else:
        equity = snap.get("equity_usd", 0.0)
        hwm = snap.get("equity_hwm_usd", 0.0)
        ticket = snap.get("ticket_usd", 0.0)
        pos_pct = snap.get("pos_pct_cap", 0.0)
        max_conc = snap.get("max_concurrent", snap.get("max_concurrent_pos", 0))
        daily_cap = snap.get("daily_spend_usd", snap.get("daily_spend_cap_usd", 0.0))
        daily_spent = snap.get("daily_spent_usd", 0.0)
        used_pct = snap.get("daily_spend_used_pct", None)

        print(f"  equity_usd          : {_fmt_num(equity)}  (HWM={_fmt_num(hwm)})")
        print(f"  ticket_usd          : {_fmt_num(ticket)}  (pos_cap={_fmt_pct(pos_pct, 2)})")
        print(f"  max_concurrent      : {max_conc}")
        if used_pct is None:
            print(
                f"  daily_spend         : cap={_fmt_num(daily_cap)}  spent={_fmt_num(daily_spent)}"
            )
        else:
            # used_pct is often already in percent units (0..100)
            try:
                used_pct_f = float(used_pct)
                used_str = f"{used_pct_f:5.2f}%"
            except Exception:
                used_str = str(used_pct)
            print(
                f"  daily_spend         : cap={_fmt_num(daily_cap)}  spent={_fmt_num(daily_spent)}  used={used_str}"
            )

        trades_today = snap.get("daily_trades_count", None)
        trades_cap = snap.get("daily_trades_cap", None)
        if trades_today is not None or trades_cap is not None:
            print(f"  trades_today        : {trades_today} / {trades_cap}")
    print("")

    # Capacity / performance
    print("CAPACITY / PERFORMANCE")
    print("----------------------")
    if "_error" in cap and "_error" in snap:
        print("  (capacity unavailable)")
    else:
        # Prefer cap snapshot if present; fallback to snap
        rc = cap.get("risk_capacity", snap.get("risk_capacity", None))
        mode = cap.get("risk_mode", snap.get("risk_mode", 1))
        mode_over = cap.get("risk_mode_override", snap.get("risk_mode_override", None))
        profile = cap.get("risk_profile", snap.get("risk_profile", "unknown"))
        winrate = cap.get("winrate20", snap.get("winrate20", 0.0))
        dd = cap.get("rolling_dd_pct", snap.get("rolling_dd_pct", 0.0))
        daily_pnl = cap.get("daily_realized_pnl_usd", snap.get("daily_realized_pnl_usd", 0.0))
        daily_pnl_pct = cap.get("daily_realized_pnl_pct", snap.get("daily_realized_pnl_pct", 0.0))
        kelly_raw = cap.get("kelly_raw", snap.get("kelly_raw", 0.0))
        kelly_cap = cap.get("kelly_pos_pct_cap", snap.get("kelly_pos_pct_cap", 0.0))
        eq_util = cap.get("equity_utilization_pct", snap.get("equity_utilization_pct", 0.0))

        print(f"  risk_capacity       : {_fmt_num(rc, 3)}")
        print(f"  risk_mode           : {mode} ({_risk_mode_label(mode)})  (override={mode_over})")
        print(f"  risk_profile        : {profile}")
        print(f"  winrate20           : {_fmt_pct(winrate, 2)}")
        print(f"  rolling_dd_pct      : {_fmt_pct(dd, 2)}")
        print(f"  daily_realized_pnl   : {_fmt_num(daily_pnl)} USD  ({_fmt_pct(daily_pnl_pct, 2)})")
        print(f"  kelly_raw           : {_fmt_num(kelly_raw, 3)}")
        print(f"  kelly_pos_pct_cap   : {_fmt_pct(kelly_cap, 2)}")
        # equity_utilization_pct is typically already percent units (0..100)
        try:
            print(f"  equity_utilization  : {float(eq_util):6.2f}%")
        except Exception:
            print(f"  equity_utilization  : {eq_util}")
    print("")

    # Regime / execution context
    print("EXECUTION CONTEXT")
    print("-----------------")
    regime_label = cap.get("regime_label", snap.get("regime_label", "unknown"))
    regime_mult = cap.get("regime_risk_multiplier", snap.get("regime_risk_multiplier", 1.0))
    regime_gate = cap.get("regime_entry_gate_factor", snap.get("regime_entry_gate_factor", 1.0))
    sandbox = snap.get("sandbox_mode", False)
    tw_open = cap.get("trading_window_open", snap.get("trading_window_open", True))
    tw_reason = cap.get("trading_window_reason", snap.get("trading_window_reason", None))

    print(f"  regime_label        : {regime_label}")
    print(f"  regime_mult         : {_fmt_num(regime_mult, 3)}")
    print(f"  regime_entry_gate   : {_fmt_num(regime_gate, 3)}")
    print(f"  trading_window_open : {_fmt_bool(tw_open)}  (reason={tw_reason or 'n/a'})")
    print(f"  sandbox_mode        : {_fmt_bool(sandbox)}")
    print("")

    # Budget
    print("BUDGET SUMMARY")
    print("--------------")
    if "_error" in budget:
        print(f"  risk_budget_summary: ERROR: {budget['_error']}")
    else:
        print(f"  equity_usd          : {_fmt_num(budget.get('equity_usd', 0.0))}")
        print(
            f"  spend_cap_usd       : {_fmt_num(budget.get('daily_spend_cap_usd', budget.get('daily_spend_usd', 0.0)))}"
        )
        print(f"  spent_usd           : {_fmt_num(budget.get('daily_spent_usd', 0.0))}")
        print(f"  remaining_usd       : {_fmt_num(budget.get('daily_spend_remaining_usd', 0.0))}")
        used = budget.get("daily_spend_used_pct", None)
        if used is None:
            print("  used_pct           : n/a")
        else:
            try:
                print(f"  used_pct           : {float(used):6.2f}%")
            except Exception:
                print(f"  used_pct           : {used}")
        print(f"  trading_window_open : {_fmt_bool(budget.get('trading_window_open', True))}")
    print("")

    # Risk ctx hash (useful when debugging FeatureBuilder drift)
    print("RISK CTX")
    print("--------")
    if "_error" in ctx:
        print(f"  read_risk_ctx_hash: ERROR: {ctx['_error']}")
    else:
        # keep compact
        if isinstance(ctx, dict) and ctx:
            keys = sorted(ctx.keys())
            show = {k: ctx[k] for k in keys[:16]}
            print("  " + _safe_str(show, 500))
            if len(keys) > 16:
                print(f"  ... ({len(keys)} keys total)")
        else:
            print(f"  {ctx}")
    print("")

    # Suggested next action if blocked
    if isinstance(gate, dict) and "_error" not in gate and not bool(gate.get("allowed", False)):
        print("NEXT ACTIONS (quick)")
        print("--------------------")
        print("  - See full gate breakdown:")
        print("      python -m scripts.diagnostics_risk gate")
        print("  - Watch live risk state:")
        print("      python -m scripts.diagnostics_risk watch --interval 15")
        print("")

    # Verbose raw dumps
    if verbose:
        print("RAW DICTS (verbose)")
        print("-------------------")
        print("snapshot:")
        print(pformat(snap, sort_dicts=True))
        print("\nentry_gate:")
        print(pformat(gate, sort_dicts=True))
        print("\ncapacity:")
        print(pformat(cap, sort_dicts=True))
        print("\nbudget:")
        print(pformat(budget, sort_dicts=True))
        print("\nctx:")
        print(pformat(ctx, sort_dicts=True))
        print("")


# ─────────────────────────────────────────────────────────────────────────────
# Gating / exit code logic
# ─────────────────────────────────────────────────────────────────────────────


def _evaluate_exit(
    data: dict[str, Any],
    *,
    require_allowed: bool,
    require_capacity: float | None,
) -> tuple[int, str]:
    """
    Returns (exit_code, reason).
    0 = ok
    1 = failed requirement or missing critical data
    """
    gate = _as_dict(data.get("entry_gate", {}))
    cap = _as_dict(data.get("capacity", {}))
    snap = _as_dict(data.get("snapshot", {}))

    if RM is None:
        return 1, "risk_manager not importable"

    if require_allowed:
        if "_error" in gate:
            return 1, f"entry_gate_debug error: {gate['_error']}"
        allowed = bool(gate.get("allowed", False))
        if not allowed:
            reasons = gate.get("reasons_blocking", []) or []
            reason = reasons[0] if reasons else "entries blocked"
            return 1, f"require_allowed failed: {reason}"

    if require_capacity is not None:
        # Prefer cap risk_capacity, fallback to snapshot risk_capacity
        rc = None
        if "_error" not in cap:
            rc = cap.get("risk_capacity", None)
        if rc is None and "_error" not in snap:
            rc = snap.get("risk_capacity", None)
        try:
            rc_f = float(rc)
        except Exception:
            return 1, "require_capacity failed: risk_capacity unavailable"
        if rc_f < float(require_capacity):
            return 1, f"require_capacity failed: {rc_f:.3f} < {float(require_capacity):.3f}"

    return 0, "ok"


# ─────────────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────────────


async def _run_once(args: argparse.Namespace) -> int:
    data = await _collect(timeout_s=float(args.timeout))

    if args.json:
        _emit(data, as_json=True, out=args.out)
    else:
        if args.out:
            # Human mode + out: write text version to file
            # (still print to console for convenience)
            # Build a single string via capturing prints? Keep simple: just write JSON in this case.
            # If you want text-to-file, run with --json for now.
            pass
        _print_human(data, verbose=bool(args.verbose))

    code, reason = _evaluate_exit(
        data,
        require_allowed=bool(args.require_allowed),
        require_capacity=float(args.require_capacity)
        if args.require_capacity is not None
        else None,
    )
    if code != 0:
        log.warning("risk_debug exit non-zero: %s", reason)
    return code


async def _run_watch(args: argparse.Namespace) -> int:
    interval = float(args.watch)
    if interval <= 0:
        return await _run_once(args)

    # watch is always human output (console)
    args.json = False
    args.out = None

    try:
        while True:
            if not args.no_clear:
                _clear_screen()
            code = await _run_once(args)
            print(
                f"(Updated: {_utc_ts()} | refresh={interval:.1f}s | last_exit={code} | Ctrl+C to stop)"
            )
            await asyncio.sleep(interval)
    except KeyboardInterrupt:
        print("\nwatch: stopped by user")
        return 0


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Risk debug helper (use diagnostics_risk for canonical ops)."
    )
    p.add_argument(
        "--watch", type=float, default=0.0, help="Refresh every N seconds (0 = one-shot)."
    )
    p.add_argument("--no-clear", action="store_true", help="Do not clear screen in watch mode.")
    p.add_argument("-v", "--verbose", action="store_true", help="Print raw dicts (deep debugging).")
    p.add_argument("--json", action="store_true", help="Emit JSON instead of human output.")
    p.add_argument(
        "--out",
        type=str,
        default=None,
        help="Write JSON output to file path (relative to repo root ok).",
    )
    p.add_argument(
        "--timeout",
        type=float,
        default=float(os.getenv("RISK_DEBUG_TIMEOUT_S", "12")),
        help="Per-call timeout seconds (default 12; env RISK_DEBUG_TIMEOUT_S).",
    )

    # Optional gating for CI/smoke flows
    p.add_argument(
        "--require-allowed", action="store_true", help="Exit non-zero if entry gate is not allowed."
    )
    p.add_argument(
        "--require-capacity",
        type=float,
        default=None,
        help="Exit non-zero if risk_capacity is below this threshold (e.g. 0.30).",
    )

    return p


def main() -> None:
    args = build_parser().parse_args()
    try:
        code = asyncio.run(_run_watch(args))
    except KeyboardInterrupt:
        print("\nCancelled by user.")
        code = 0
    except Exception as e:
        log.error("risk_debug fatal: %s", e)
        code = 1
    raise SystemExit(code)


if __name__ == "__main__":
    main()
