# scripts/diagnostics_risk.py
from __future__ import annotations

"""
Risk engine diagnostics / ops CLI (V3+++ / quant-grade, P0-safe).

This script is intentionally:
- **Read-heavy, write-light**: lots of introspection; ops commands are guarded.
- **Best-effort**: never explodes because one optional method is missing.
- **Canonical**: uses `logging_setup.py` and `utils_pkg.risk_manager` as the source of truth.

What you get
------------
READ commands (JSON-capable):
  - summary     : one-shot risk snapshot + gate + capacity (high-signal)
  - gate        : detailed entry gate breakdown (why blocked)
  - capacity    : capacity model / regime / clamps / overlays
  - budget      : daily spend / remaining / utilization
  - ctx         : published risk ctx hash (feature_builder sanity)
  - health      : aggregate health status
  - describe    : shows which risk_manager functions are available

WATCH:
  - watch       : live "top-style" refreshing summary

OPS commands (mutating; require --yes):
  - block       : set/clear manual block (if supported)
  - mode        : set/clear risk_mode override (0..3)
  - profile     : set/clear risk profile override (conservative/balanced/aggressive/auto)
  - cooldown    : clear cooldown (if supported)

Exit codes
----------
0 success
1 error
2 refused (missing --yes for a mutating action)

Usage
-----
  python -m scripts.diagnostics_risk
  python -m scripts.diagnostics_risk summary --json
  python -m scripts.diagnostics_risk watch --interval 15

  # Ops require --yes
  python -m scripts.diagnostics_risk block --on --yes
  python -m scripts.diagnostics_risk mode --set 0 --yes
  python -m scripts.diagnostics_risk mode --clear --yes
  python -m scripts.diagnostics_risk profile --set aggressive --yes
  python -m scripts.diagnostics_risk cooldown --clear --yes
"""

import argparse
import asyncio
import contextlib
import json
import os
import sys
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

# ──────────────────────────────────────────────────────────────
# Repo root on path (scripts/ is not a package)
# ──────────────────────────────────────────────────────────────

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# Optional dotenv (local convenience only)
try:  # pragma: no cover
    from dotenv import find_dotenv, load_dotenv  # type: ignore
except Exception:  # pragma: no cover
    load_dotenv = None  # type: ignore
    find_dotenv = None  # type: ignore

if load_dotenv and find_dotenv:
    with contextlib.suppress(Exception):
        load_dotenv(find_dotenv(), override=False)

# Logging (canonical)
try:
    from logging_setup import get_logger  # type: ignore

    log = get_logger(__name__)
except Exception:  # pragma: no cover
    import logging

    logging.basicConfig(level=logging.INFO)
    log = logging.getLogger("diagnostics_risk")

# Canonical risk brain
try:
    from utils_pkg import risk_manager as RM  # type: ignore
except Exception as e:  # pragma: no cover
    RM = None  # type: ignore[assignment]
    log.error("Failed to import utils_pkg.risk_manager: %r", e)


# ──────────────────────────────────────────────────────────────
# Formatting helpers
# ──────────────────────────────────────────────────────────────


def _utc_now() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds")


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
    """
    If assume_fraction=True, x=0.12 -> 12.0%
    If assume_fraction=False, x=12 -> 12.0%
    """
    try:
        v = float(x)
    except Exception:
        return str(x)
    if assume_fraction:
        v *= 100.0
    return f"{v:.{decimals}f}%"


def _clear_screen() -> None:
    with contextlib.suppress(Exception):
        os.system("cls" if os.name == "nt" else "clear")


def _print_kv(title: str, mapping: dict[str, Any]) -> None:
    print(f"\n{title}")
    print("-" * len(title))
    if not mapping:
        print("  (no data)")
        return
    max_key = max(len(k) for k in mapping)
    for k, v in mapping.items():
        print(f"  {k.ljust(max_key)} : {v}")


def _atomic_write(path: Path, data: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(data, encoding="utf-8")
    tmp.replace(path)


# ──────────────────────────────────────────────────────────────
# Async helpers
# ──────────────────────────────────────────────────────────────


async def _maybe_await(x: Any) -> Any:
    if asyncio.iscoroutine(x):
        return await x
    return x


async def _call(
    fn: Callable[..., Any],
    *args: Any,
    timeout_s: float,
    **kwargs: Any,
) -> Any:
    async def _runner() -> Any:
        return await _maybe_await(fn(*args, **kwargs))

    return await asyncio.wait_for(_runner(), timeout=timeout_s)


@dataclass
class CallResult:
    ok: bool
    value: Any = None
    error: str | None = None
    duration_ms: int = 0


async def _safe_call(
    obj: Any,
    name: str,
    *args: Any,
    timeout_s: float,
    **kwargs: Any,
) -> CallResult:
    t0 = time.perf_counter()
    if obj is None:
        return CallResult(False, error="risk_manager not importable")

    fn = getattr(obj, name, None)
    if not callable(fn):
        return CallResult(False, error=f"risk_manager.{name} not available")

    try:
        val = await _call(fn, *args, timeout_s=timeout_s, **kwargs)
        return CallResult(True, value=val, duration_ms=int((time.perf_counter() - t0) * 1000))
    except asyncio.TimeoutError:
        return CallResult(
            False,
            error=f"{name} timed out after {timeout_s}s",
            duration_ms=int((time.perf_counter() - t0) * 1000),
        )
    except Exception as e:
        return CallResult(
            False, error=_safe_str(e), duration_ms=int((time.perf_counter() - t0) * 1000)
        )


# ──────────────────────────────────────────────────────────────
# Output helpers
# ──────────────────────────────────────────────────────────────


def _emit(data: Any, *, as_json: bool, out_path: str | None) -> None:
    payload = json.dumps(data, indent=2, sort_keys=True, default=str) if as_json else str(data)

    if out_path:
        p = Path(out_path)
        if not p.is_absolute():
            p = (ROOT / p).resolve()
        _atomic_write(p, payload + ("" if payload.endswith("\n") else "\n"))
        print(f"✅ Wrote {p}")
    else:
        print(payload, end="" if payload.endswith("\n") else "\n")


def _require_yes(args: argparse.Namespace, action_desc: str) -> None:
    if getattr(args, "yes", False):
        return
    print(f"Refusing to run mutating action without --yes: {action_desc}", file=sys.stderr)
    print("Add --yes to confirm.", file=sys.stderr)
    raise SystemExit(2)


# ──────────────────────────────────────────────────────────────
# Data shaping (keep output stable across drift)
# ──────────────────────────────────────────────────────────────


def _coerce_dict(x: Any) -> dict[str, Any]:
    return x if isinstance(x, dict) else {"value": x}


def _risk_mode_label(mode: Any) -> str:
    try:
        m = int(mode)
    except Exception:
        return "unknown"
    return {0: "OFF", 1: "NORMAL", 2: "AGGRESSIVE", 3: "DEFENSIVE"}.get(m, "unknown")


def _pct_from_maybe_percent(x: Any) -> str:
    """
    Some producers emit 'used_pct' as 0..100, others as 0..1. Make it readable.
    """
    try:
        v = float(x)
    except Exception:
        return "n/a"
    if v <= 1.0:
        return _fmt_pct(v, 2, assume_fraction=True)
    return _fmt_pct(v / 100.0, 2, assume_fraction=True)


# ──────────────────────────────────────────────────────────────
# Command handlers
# ──────────────────────────────────────────────────────────────


async def cmd_describe(args: argparse.Namespace) -> None:
    if RM is None:
        _emit(
            {"ok": False, "error": "utils_pkg.risk_manager not importable"},
            as_json=True,
            out_path=args.out,
        )
        return

    wanted = [
        "risk_snapshot",
        "risk_capacity_snapshot",
        "risk_capacity_score",
        "entry_gate_debug",
        "current_limits",
        "risk_budget_summary",
        "read_risk_ctx_hash",
        "risk_health",
        "set_manual_block",
        "clear_manual_block",
        "force_risk_mode",
        "set_risk_profile_override",
        "clear_risk_cooldown",
    ]
    avail = {name: callable(getattr(RM, name, None)) for name in wanted}
    meta = {
        "generated_at": _utc_now(),
        "module": getattr(RM, "__name__", "risk_manager"),
        "file": getattr(RM, "__file__", None),
        "available": avail,
    }

    if args.json:
        _emit(meta, as_json=True, out_path=args.out)
        return

    print("\n=== RISK MANAGER CAPABILITIES ===")
    print(f"generated_at: {meta['generated_at']}")
    print(f"module      : {meta['module']}")
    if meta["file"]:
        print(f"file        : {meta['file']}")
    print("")
    for k in wanted:
        print(f"- {k:24s} : {'YES' if avail[k] else 'no'}")
    print("")


async def cmd_summary(args: argparse.Namespace) -> None:
    timeout = float(args.timeout)

    snap_r = await _safe_call(RM, "risk_snapshot", timeout_s=timeout)
    gate_r = await _safe_call(RM, "entry_gate_debug", timeout_s=timeout)
    cap_r = await _safe_call(RM, "risk_capacity_snapshot", timeout_s=timeout)

    out = {
        "generated_at": _utc_now(),
        "snapshot": _coerce_dict(snap_r.value) if snap_r.ok else {"_error": snap_r.error},
        "entry_gate": _coerce_dict(gate_r.value) if gate_r.ok else {"_error": gate_r.error},
        "capacity": _coerce_dict(cap_r.value) if cap_r.ok else {"_error": cap_r.error},
        "timings_ms": {
            "risk_snapshot": snap_r.duration_ms,
            "entry_gate_debug": gate_r.duration_ms,
            "risk_capacity_snapshot": cap_r.duration_ms,
        },
    }

    if args.json:
        _emit(out, as_json=True, out_path=args.out)
        return

    snap = out["snapshot"]
    gate = out["entry_gate"]
    cap = out["capacity"]

    print("\n=== RISK SUMMARY (V3+++) ===")
    print(f"generated_at: {out['generated_at']}")
    if "_error" in snap:
        print(f"\n[risk_snapshot] ERROR: {snap['_error']}")
    else:
        mode = snap.get("risk_mode", snap.get("risk_mode_override", 1))
        print(f"engine      : {snap.get('risk_engine_version', 'n/a')}")
        print(
            f"risk_mode   : {mode} ({_risk_mode_label(mode)}) "
            f"(override={snap.get('risk_mode_override')}, profile={snap.get('risk_profile')})"
        )
        print(
            f"equity_usd  : {_fmt_num(snap.get('equity_usd', 0.0))} "
            f"(HWM={_fmt_num(snap.get('equity_hwm_usd', 0.0))})"
        )
        print(
            f"ticket_usd  : {_fmt_num(snap.get('ticket_usd', 0.0))} "
            f"(pos_cap={_fmt_pct(snap.get('pos_pct_cap', 0.0), 2)})"
        )
        print(f"max_conc    : {snap.get('max_concurrent', 0)}")

        spend_cap = snap.get("daily_spend_usd", snap.get("daily_spend_cap_usd", 0.0))
        spent = snap.get("daily_spent_usd", snap.get("daily_spend_spent_usd", 0.0))
        used_pct = snap.get("daily_spend_used_pct", None)
        used_str = _pct_from_maybe_percent(used_pct) if used_pct is not None else "n/a"
        print(f"daily_spend : cap={_fmt_num(spend_cap)} spent={_fmt_num(spent)} used={used_str}")

        print(
            f"pnl_today   : {_fmt_num(snap.get('daily_realized_pnl_usd', 0.0))} "
            f"(winrate20={_fmt_pct(snap.get('winrate20', 0.0), 1)}, "
            f"dd={_fmt_pct(snap.get('rolling_dd_pct', 0.0), 2)})"
        )
        print(
            f"regime      : {snap.get('regime_label')} "
            f"(mult={_fmt_num(snap.get('regime_risk_multiplier', 1.0), 2)})"
        )

        rc = snap.get("risk_capacity", cap.get("risk_capacity", None))
        if rc is not None:
            print(
                f"capacity    : {_fmt_num(rc, 3)} "
                f"(ai_delta={_fmt_num(snap.get('ai_capacity_delta', 0.0), 3)})"
            )

    print("\n--- ENTRY GATE ---")
    if "_error" in gate:
        print(f"[entry_gate_debug] ERROR: {gate['_error']}")
    else:
        allowed = bool(gate.get("allowed", False))
        print(f"allowed     : {_fmt_bool(allowed)}")
        reasons = gate.get("reasons_blocking", []) or []
        if not allowed and reasons:
            print("reasons     :")
            for r in reasons:
                print(f"  - {r}")
        else:
            print("reasons     : (none)")
        halt_reason = gate.get("halt_reason")
        if halt_reason:
            print(f"halt_reason : {halt_reason}")

    print("\n--- CAPACITY ---")
    if "_error" in cap:
        print(f"[risk_capacity_snapshot] ERROR: {cap['_error']}")
    else:
        print(f"risk_capacity        : {_fmt_num(cap.get('risk_capacity', 1.0), 3)}")
        print(f"cooldown_active      : {_fmt_bool(cap.get('cooldown_active', False))}")
        print(f"consecutive_losses   : {cap.get('consecutive_losses', 0)}")
        print(f"regime_label         : {cap.get('regime_label')}")
        print(f"regime_mult          : {_fmt_num(cap.get('regime_risk_multiplier', 1.0), 3)}")

    print("")


async def cmd_gate(args: argparse.Namespace) -> None:
    timeout = float(args.timeout)
    gate_r = await _safe_call(RM, "entry_gate_debug", timeout_s=timeout)
    gate = _coerce_dict(gate_r.value) if gate_r.ok else {"_error": gate_r.error}

    if args.json:
        _emit({"generated_at": _utc_now(), "entry_gate": gate}, as_json=True, out_path=args.out)
        return

    print("\n=== ENTRY GATE DEBUG ===")
    if "_error" in gate:
        print(f"ERROR: {gate['_error']}")
        return

    allowed = bool(gate.get("allowed", False))
    limits = gate.get("limits") if isinstance(gate.get("limits"), dict) else {}
    print(f"allowed        : {_fmt_bool(allowed)}")
    print(f"risk_mode      : {limits.get('risk_mode', gate.get('risk_mode'))}")
    print(
        f"risk_capacity  : {_fmt_num(limits.get('risk_capacity', gate.get('risk_capacity', 1.0)), 3)}"
    )
    print(f"equity_usd     : {_fmt_num(limits.get('equity_usd', gate.get('equity_usd', 0.0)))}")
    print(f"ticket_usd     : {_fmt_num(limits.get('ticket_usd', gate.get('ticket_usd', 0.0)))}")
    print(f"max_concurrent : {limits.get('max_concurrent', gate.get('max_concurrent'))}")
    print(f"manual_block   : {_fmt_bool(gate.get('manual_block', False))}")
    print(f"infra_halt     : {_fmt_bool(gate.get('infra_halt', False))}")
    print(f"cooldown       : {_fmt_bool(gate.get('cooldown_active', False))}")
    print(f"sandbox_mode   : {_fmt_bool(gate.get('sandbox_mode', False))}")
    print(f"risk_profile   : {gate.get('risk_profile')} (env={gate.get('risk_profile_env')})")
    print(
        f"trading_window : open={_fmt_bool(gate.get('trading_window_open', True))} "
        f"reason={gate.get('trading_window_reason', 'n/a')}"
    )
    print(f"engine_version : {gate.get('risk_engine_version', 'n/a')}")

    reasons = gate.get("reasons_blocking", []) or []
    print("\nReasons blocking entries:")
    if reasons:
        for r in reasons:
            print(f"  - {r}")
    else:
        print("  (none — entries allowed)")
    print("")


async def cmd_capacity(args: argparse.Namespace) -> None:
    timeout = float(args.timeout)
    cap_r = await _safe_call(RM, "risk_capacity_snapshot", timeout_s=timeout)
    cap = _coerce_dict(cap_r.value) if cap_r.ok else {"_error": cap_r.error}

    if args.json:
        _emit({"generated_at": _utc_now(), "capacity": cap}, as_json=True, out_path=args.out)
        return

    print("\n=== RISK CAPACITY SNAPSHOT ===")
    if "_error" in cap:
        print(f"ERROR: {cap['_error']}")
        return

    print(f"risk_capacity          : {_fmt_num(cap.get('risk_capacity', 1.0), 3)}")
    print(
        f"risk_mode              : {cap.get('risk_mode', 1)} "
        f"(override={cap.get('risk_mode_override')})"
    )
    print(f"winrate20              : {_fmt_pct(cap.get('winrate20', 0.0), 2)}")
    print(f"rolling_dd_pct         : {_fmt_pct(cap.get('rolling_dd_pct', 0.0), 2)}")
    print(f"daily_realized_pnl_usd : {_fmt_num(cap.get('daily_realized_pnl_usd', 0.0))}")
    print(f"daily_realized_pnl_pct : {_fmt_pct(cap.get('daily_realized_pnl_pct', 0.0), 2)}")
    print(f"regime_label           : {cap.get('regime_label')}")
    print(f"regime_risk_multiplier : {_fmt_num(cap.get('regime_risk_multiplier', 1.0), 3)}")
    print(f"cooldown_active        : {_fmt_bool(cap.get('cooldown_active', False))}")
    print(f"consecutive_losses     : {cap.get('consecutive_losses', 0)}")
    print(f"kelly_raw              : {_fmt_num(cap.get('kelly_raw', 0.0), 3)}")
    print(f"kelly_pos_pct_cap      : {_fmt_pct(cap.get('kelly_pos_pct_cap', 0.0), 2)}")
    print(
        f"risk_profile           : {cap.get('risk_profile')} "
        f"(env={cap.get('risk_profile_env')}, override={cap.get('risk_profile_override')})"
    )
    print(
        f"trading_window_open    : {_fmt_bool(cap.get('trading_window_open', True))} "
        f"reason={cap.get('trading_window_reason', 'n/a')}"
    )
    print(f"engine_version         : {cap.get('risk_engine_version', 'n/a')}")
    print("")


async def cmd_budget(args: argparse.Namespace) -> None:
    timeout = float(args.timeout)
    bud_r = await _safe_call(RM, "risk_budget_summary", timeout_s=timeout)
    bud = _coerce_dict(bud_r.value) if bud_r.ok else {"_error": bud_r.error}

    if args.json:
        _emit({"generated_at": _utc_now(), "budget": bud}, as_json=True, out_path=args.out)
        return

    print("\n=== RISK BUDGET SUMMARY ===")
    if "_error" in bud:
        print(f"ERROR: {bud['_error']}")
        return

    print(f"equity_usd                : {_fmt_num(bud.get('equity_usd', 0.0))}")
    print(
        f"daily_spend_cap_usd       : {_fmt_num(bud.get('daily_spend_cap_usd', bud.get('daily_spend_usd', 0.0)))}"
    )
    print(f"daily_spent_usd           : {_fmt_num(bud.get('daily_spent_usd', 0.0))}")
    print(f"daily_spend_remaining_usd : {_fmt_num(bud.get('daily_spend_remaining_usd', 0.0))}")
    used_pct = bud.get("daily_spend_used_pct", None)
    used_str = _pct_from_maybe_percent(used_pct) if used_pct is not None else "n/a"
    print(f"daily_spend_used_pct      : {used_str}")
    print(f"sandbox_mode              : {_fmt_bool(bud.get('sandbox_mode', False))}")
    print(f"trading_window_open       : {_fmt_bool(bud.get('trading_window_open', True))}")
    print("")


async def cmd_ctx(args: argparse.Namespace) -> None:
    timeout = float(args.timeout)
    ctx_r = await _safe_call(RM, "read_risk_ctx_hash", timeout_s=timeout)
    ctx = _coerce_dict(ctx_r.value) if ctx_r.ok else {"_error": ctx_r.error}

    if args.json:
        _emit({"generated_at": _utc_now(), "ctx": ctx}, as_json=True, out_path=args.out)
        return

    print("\n=== RISK CTX (risk:ctx hash) ===")
    if "_error" in ctx:
        print(f"ERROR: {ctx['_error']}")
        return
    if isinstance(ctx, dict):
        _print_kv("ctx", {k: ctx[k] for k in sorted(ctx.keys())})
    else:
        print(ctx)
    print("")


async def cmd_health(args: argparse.Namespace) -> None:
    timeout = float(args.timeout)
    h_r = await _safe_call(RM, "risk_health", timeout_s=timeout)
    health = _coerce_dict(h_r.value) if h_r.ok else {"_error": h_r.error}

    if args.json:
        _emit({"generated_at": _utc_now(), "health": health}, as_json=True, out_path=args.out)
        return

    print("\n=== RISK HEALTH ===")
    if "_error" in health:
        print(f"ERROR: {health['_error']}")
        return

    headline = health.get("headline") if isinstance(health.get("headline"), dict) else {}
    details = health.get("details") if isinstance(health.get("details"), dict) else {}

    if headline:
        _print_kv("headline", {k: headline[k] for k in sorted(headline.keys())})
    else:
        print("(no headline)")

    for section_name in sorted(details.keys()):
        section = details[section_name]
        if isinstance(section, dict):
            _print_kv(section_name, {k: section[k] for k in sorted(section.keys())})
        else:
            print(f"\n{section_name}")
            print("-" * len(section_name))
            print(_safe_str(section, 2000))
    print("")


async def cmd_watch(args: argparse.Namespace) -> None:
    interval = float(args.interval)
    if interval <= 0:
        interval = 10.0

    # watch is always human output
    args.json = False
    args.out = None

    try:
        while True:
            _clear_screen()
            await cmd_summary(args)
            print(f"(Updated: {_utc_now()} | refresh={interval:.1f}s | Ctrl+C to stop)")
            await asyncio.sleep(interval)
    except KeyboardInterrupt:
        print("\nwatch: stopped by user")


# ──────────────────────────────────────────────────────────────
# OPS commands (mutating; require --yes)
# ──────────────────────────────────────────────────────────────


async def cmd_block(args: argparse.Namespace) -> None:
    if args.on and args.off:
        print("block: cannot use --on and --off together", file=sys.stderr)
        raise SystemExit(1)

    timeout = float(args.timeout)

    if args.on:
        _require_yes(args, "set manual_block=1")
        r = await _safe_call(RM, "set_manual_block", True, timeout_s=timeout)
        if not r.ok:
            print(f"❌ set_manual_block not available/failed: {r.error}", file=sys.stderr)
            raise SystemExit(1)
        print("✅ manual_block set (new entries should be blocked)")
        return

    if args.off:
        _require_yes(args, "clear manual_block")
        r = await _safe_call(RM, "clear_manual_block", timeout_s=timeout)
        if not r.ok:
            print(f"❌ clear_manual_block not available/failed: {r.error}", file=sys.stderr)
            raise SystemExit(1)
        print("✅ manual_block cleared")
        return

    gate_r = await _safe_call(RM, "entry_gate_debug", timeout_s=timeout)
    if not gate_r.ok:
        print(f"❌ entry_gate_debug failed: {gate_r.error}", file=sys.stderr)
        raise SystemExit(1)
    gate = _coerce_dict(gate_r.value)
    print(f"manual_block = {bool(gate.get('manual_block', False))}  (use: block --on/--off --yes)")


async def cmd_mode(args: argparse.Namespace) -> None:
    timeout = float(args.timeout)
    if args.clear and args.set is not None:
        print("mode: cannot use --set and --clear together", file=sys.stderr)
        raise SystemExit(1)

    if args.set is not None:
        if args.set not in (0, 1, 2, 3):
            print("mode: --set must be one of 0,1,2,3", file=sys.stderr)
            raise SystemExit(1)
        _require_yes(args, f"set risk_mode override to {args.set}")
        r = await _safe_call(RM, "force_risk_mode", int(args.set), timeout_s=timeout)
        if not r.ok:
            print(f"❌ force_risk_mode failed: {r.error}", file=sys.stderr)
            raise SystemExit(1)
        print(f"✅ risk_mode override set to {args.set} ({_risk_mode_label(args.set)})")
        return

    if args.clear:
        _require_yes(args, "clear risk_mode override")
        r = await _safe_call(RM, "force_risk_mode", None, timeout_s=timeout)
        if not r.ok:
            print(f"❌ force_risk_mode(clear) failed: {r.error}", file=sys.stderr)
            raise SystemExit(1)
        print("✅ risk_mode override cleared (revert to capacity-based mode)")
        return

    cap_r = await _safe_call(RM, "risk_capacity_snapshot", timeout_s=timeout)
    if not cap_r.ok:
        print(f"❌ risk_capacity_snapshot failed: {cap_r.error}", file=sys.stderr)
        raise SystemExit(1)
    cap = _coerce_dict(cap_r.value)
    print("\n=== RISK MODE OVERRIDE ===")
    print(
        f"risk_mode          : {cap.get('risk_mode', 1)} ({_risk_mode_label(cap.get('risk_mode', 1))})"
    )
    print(f"risk_mode_override : {cap.get('risk_mode_override')}")
    print("(use: mode --set 0/1/2/3 --yes  OR  mode --clear --yes)")


async def cmd_profile(args: argparse.Namespace) -> None:
    timeout = float(args.timeout)
    if args.clear and args.set:
        print("profile: cannot use --set and --clear together", file=sys.stderr)
        raise SystemExit(1)

    if args.set:
        name = args.set.strip().lower()
        if name not in ("conservative", "balanced", "aggressive", "auto"):
            print(
                "profile: --set must be one of conservative/balanced/aggressive/auto",
                file=sys.stderr,
            )
            raise SystemExit(1)
        _require_yes(args, f"set risk profile override to '{name}'")
        r = await _safe_call(RM, "set_risk_profile_override", name, timeout_s=timeout)
        if not r.ok:
            print(f"❌ set_risk_profile_override failed: {r.error}", file=sys.stderr)
            raise SystemExit(1)
        print(f"✅ risk profile override set to '{name}'")
        return

    if args.clear:
        _require_yes(args, "clear risk profile override")
        r = await _safe_call(RM, "set_risk_profile_override", None, timeout_s=timeout)
        if not r.ok:
            print(f"❌ clear risk profile override failed: {r.error}", file=sys.stderr)
            raise SystemExit(1)
        print("✅ risk profile override cleared (revert to env/auto)")
        return

    snap_r = await _safe_call(RM, "risk_snapshot", timeout_s=timeout)
    if not snap_r.ok:
        print(f"❌ risk_snapshot failed: {snap_r.error}", file=sys.stderr)
        raise SystemExit(1)
    snap = _coerce_dict(snap_r.value)
    print("\n=== RISK PROFILE OVERRIDE ===")
    print(f"active_profile : {snap.get('risk_profile')}")
    print(f"env_profile    : {snap.get('risk_profile_env')}")
    print(f"override       : {snap.get('risk_profile_override')}")
    print("(use: profile --set NAME --yes  OR  profile --clear --yes)")


async def cmd_cooldown(args: argparse.Namespace) -> None:
    timeout = float(args.timeout)
    if not args.clear:
        gate_r = await _safe_call(RM, "entry_gate_debug", timeout_s=timeout)
        if not gate_r.ok:
            print(f"❌ entry_gate_debug failed: {gate_r.error}", file=sys.stderr)
            raise SystemExit(1)
        gate = _coerce_dict(gate_r.value)
        print(
            f"cooldown_active = {bool(gate.get('cooldown_active', False))}  (clear with: cooldown --clear --yes)"
        )
        return

    _require_yes(args, "clear risk cooldown")
    r = await _safe_call(RM, "clear_risk_cooldown", timeout_s=timeout)
    if not r.ok:
        print(f"❌ clear_risk_cooldown failed: {r.error}", file=sys.stderr)
        raise SystemExit(1)
    print("✅ risk cooldown cleared")


# ──────────────────────────────────────────────────────────────
# Argparse wiring
# ──────────────────────────────────────────────────────────────


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Risk manager diagnostics / ops CLI (V3+++).")
    p.add_argument("--json", action="store_true", help="Emit JSON for read-only commands.")
    p.add_argument("--out", type=str, default=None, help="Write output to file (JSON/text).")
    p.add_argument(
        "--timeout",
        type=float,
        default=float(os.getenv("RISK_DIAG_TIMEOUT_S", "12")),
        help="Per-call timeout seconds (default 12; env RISK_DIAG_TIMEOUT_S).",
    )
    p.add_argument("--yes", action="store_true", help="Required for mutating commands (safety).")

    sub = p.add_subparsers(dest="command", metavar="CMD")

    sub.add_parser("summary", help="One-shot high-level risk summary (default).")
    sub.add_parser("gate", help="Detailed entry gate breakdown.")
    sub.add_parser("capacity", help="Risk capacity / regime snapshot.")
    sub.add_parser("budget", help="Daily budget / spend summary.")
    sub.add_parser("ctx", help="Raw risk ctx hash view.")
    sub.add_parser("health", help="Aggregate risk health view.")
    sub.add_parser("describe", help="Show which risk_manager functions exist (drift check).")

    w = sub.add_parser("watch", help="Live watch mode (top-style).")
    w.add_argument(
        "--interval", type=float, default=20.0, help="Refresh interval seconds (default 20)."
    )

    b = sub.add_parser("block", help="Set / clear manual risk block.")
    b.add_argument("--on", action="store_true", help="Set manual_block=1.")
    b.add_argument("--off", action="store_true", help="Clear manual_block.")

    m = sub.add_parser("mode", help="View or set risk_mode override (0..3).")
    m.add_argument("--set", type=int, default=None, help="Force risk_mode to 0..3.")
    m.add_argument("--clear", action="store_true", help="Clear risk_mode override.")

    pr = sub.add_parser("profile", help="View or set risk profile override.")
    pr.add_argument(
        "--set",
        type=str,
        default=None,
        help="Set profile override (conservative/balanced/aggressive/auto).",
    )
    pr.add_argument("--clear", action="store_true", help="Clear profile override.")

    cd = sub.add_parser("cooldown", help="View or clear risk cooldown.")
    cd.add_argument("--clear", action="store_true", help="Clear cooldown (requires --yes).")

    return p


HANDLERS: dict[str, Callable[[argparse.Namespace], Awaitable[None]]] = {
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
    "describe": cmd_describe,
}


async def _main_async(argv: list[str] | None = None) -> None:
    parser = build_parser()
    args = parser.parse_args(argv)

    cmd = args.command or "summary"

    # JSON/output file are for read-only commands; watch and ops are interactive
    if cmd in {"watch", "block", "mode", "profile", "cooldown"}:
        args.json = False
        args.out = None

    handler = HANDLERS.get(cmd)
    if handler is None:
        parser.print_help()
        raise SystemExit(1)

    await handler(args)


def main() -> None:
    try:
        asyncio.run(_main_async())
    except SystemExit:
        raise
    except KeyboardInterrupt:
        print("\nCancelled by user.")
    except Exception as err:
        # B904 fix: chain the original exception
        log.error("diagnostics_risk: fatal error", exc_info=True)
        raise SystemExit(1) from err


if __name__ == "__main__":
    main()
