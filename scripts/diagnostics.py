# scripts/diagnostics.py
from __future__ import annotations

"""
Deep diagnostics / sanity checker (V3+++ / quant-grade).

Usage
-----

Basic run (most common):

    python -m scripts.diagnostics

Options:

    python -m scripts.diagnostics --json                  # machine-readable
    python -m scripts.diagnostics --no-backtest           # skip quick BT
    python -m scripts.diagnostics --days-back 7           # backtest horizon
    python -m scripts.diagnostics --limit-candidates 200  # BT cap
    python -m scripts.diagnostics --deep                  # heavier checks

Goals
-----

  - Quickly answer: "Is everything wired correctly and safe to trade?"

  - Check & summarize:
        • Environment + feature flags
        • Postgres / ai_db connectivity
        • Redis connectivity & infra:halt flag
        • Core market data: SOL price
        • Router / Jupiter stack (importable + basic describe)
        • Regime engine state (trend/vol/liquidity/manipulation)
        • Risk manager limits + risk capacity (0..1 / mode 0..3)
        • Entry AI / feature pipeline sanity (if modules available)
        • Wider AI stack presence (buy scoring, feature builder, self-learning)
        • Optional mini backtest sanity (small sample, short horizon)

  - Never crash on errors: instead, capture them in the report with
    ok=false and an error message.

  - Emit either human-readable text or JSON for dashboards / scripts.
"""

import argparse
import asyncio
import json
import os
import sys
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, Optional

from logging_setup import get_logger

log = get_logger(__name__)

# ──────────────────────────────────────────────────────────────────────────────
# Core infra / quant modules (best-effort imports where appropriate)
# ──────────────────────────────────────────────────────────────────────────────

from utils_pkg.core_utils import (
    get_sol_usd,
    redis as shared_redis,
)
from utils_pkg.regime import regime_state
from utils_pkg import ai_db
from utils_pkg.risk_manager import (
    current_limits,
    risk_snapshot,
    risk_capacity_snapshot,
    risk_capacity_score,
    should_allow_new_entry,
)

# Optional: router / Jupiter stack
try:  # pragma: no cover
    from utils_pkg import router as _router_mod  # type: ignore[assignment]
except Exception:  # pragma: no cover
    _router_mod = None  # type: ignore[assignment]

# Optional: backtest engine & AI entry logic (wrapped in try/except)
try:  # pragma: no cover
    from utils_pkg.backtest_engine import run_backtest
except Exception:  # pragma: no cover
    run_backtest = None  # type: ignore[assignment]

try:  # pragma: no cover
    from utils_pkg import ai_entry_logic
except Exception:  # pragma: no cover
    ai_entry_logic = None  # type: ignore[assignment]

# Optional: wider AI stack modules
try:  # pragma: no cover
    from utils_pkg import ai_buy_scoring as _ai_buy_scoring  # type: ignore[assignment]
except Exception:  # pragma: no cover
    _ai_buy_scoring = None  # type: ignore[assignment]

try:  # pragma: no cover
    from utils_pkg import feature_builder as _feature_builder  # type: ignore[assignment]
except Exception:  # pragma: no cover
    _feature_builder = None  # type: ignore[assignment]

try:  # pragma: no cover
    from utils_pkg import self_learning_ai_strategy as _self_learning  # type: ignore[assignment]
except Exception:  # pragma: no cover
    _self_learning = None  # type: ignore[assignment]

try:  # pragma: no cover
    from utils_pkg import ai_sell_logic as _ai_sell_logic  # type: ignore[assignment]
except Exception:  # pragma: no cover
    _ai_sell_logic = None  # type: ignore[assignment]


# =============================================================================
# Small helpers
# =============================================================================


def _env_bool(key: str, default: bool = False) -> bool:
    v = os.getenv(key)
    if v is None:
        return default
    return str(v).strip().lower() in {"1", "true", "yes", "y", "on"}


def _safe_str(v: Any, max_len: int = 64) -> str:
    s = str(v)
    if len(s) > max_len:
        return s[: max_len - 3] + "..."
    return s


async def _get_redis():
    """
    Prefer shared redis from core_utils; if not available, we just report
    "no redis" instead of failing.
    """
    try:
        if shared_redis is not None:
            return shared_redis
    except Exception:
        pass
    return None


# =============================================================================
# Result structures
# =============================================================================


@dataclass
class CheckResult:
    ok: bool
    detail: str = ""
    extra: Dict[str, Any] | None = None

    def to_dict(self) -> Dict[str, Any]:
        d = {
            "ok": bool(self.ok),
            "detail": self.detail,
        }
        if self.extra:
            d["extra"] = self.extra
        return d


@dataclass
class DiagnosticsReport:
    generated_at: str
    env: Dict[str, Any]
    db: CheckResult
    redis: CheckResult
    market_data: CheckResult
    regime: CheckResult
    risk: CheckResult
    router: CheckResult
    entry_ai: CheckResult
    ai_stack: CheckResult
    backtest: Optional[CheckResult] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "generated_at": self.generated_at,
            "env": self.env,
            "db": self.db.to_dict(),
            "redis": self.redis.to_dict(),
            "market_data": self.market_data.to_dict(),
            "regime": self.regime.to_dict(),
            "risk": self.risk.to_dict(),
            "router": self.router.to_dict(),
            "entry_ai": self.entry_ai.to_dict(),
            "ai_stack": self.ai_stack.to_dict(),
            "backtest": self.backtest.to_dict() if self.backtest else None,
        }


# =============================================================================
# Individual checks
# =============================================================================


async def check_db() -> CheckResult:
    try:
        backend_desc = ai_db.describe_backend()
    except Exception as e:
        return CheckResult(False, f"describe_backend failed: {e!r}")

    try:
        ok = ai_db.ping()
    except Exception as e:
        return CheckResult(False, f"ai_db.ping() raised: {e!r}")

    if not ok:
        return CheckResult(False, "ai_db.ping() returned False")

    return CheckResult(
        True,
        "Postgres reachable",
        extra={
            "backend": backend_desc,
        },
    )


async def check_redis() -> CheckResult:
    redis = await _get_redis()
    if redis is None:
        return CheckResult(False, "No Redis client available from core_utils.")

    try:
        pong = await redis.ping()
    except Exception as e:
        return CheckResult(False, f"redis.ping() failed: {e!r}")

    if pong is not True:
        return CheckResult(False, f"redis.ping() returned {pong!r}")

    # infra:halt flag + basic keys presence
    try:
        halted = await redis.get("infra:halt")
        halt_reason = await redis.get("infra:halt_reason")
        has_metrics = bool(await redis.exists("metrics:winrate20"))
    except Exception as e:
        return CheckResult(
            True,
            "Redis reachable, but metadata fetch failed.",
            extra={"error": str(e)},
        )

    extra = {
        "infra_halt": halted == "1",
        "infra_halt_reason": halt_reason,
        "has_winrate20": has_metrics,
    }
    detail = "Redis reachable"
    if halted == "1":
        detail += " (infra:halt=1 – entries blocked)"

    return CheckResult(True, detail, extra=extra)


async def check_market_data() -> CheckResult:
    try:
        sol_usd = await get_sol_usd()
    except Exception as e:
        return CheckResult(False, f"get_sol_usd() failed: {e!r}")

    if not sol_usd or sol_usd <= 0:
        return CheckResult(False, f"Invalid SOL/USD quote: {sol_usd!r}")

    return CheckResult(
        True,
        "Market data OK",
        extra={
            "sol_usd": float(sol_usd),
        },
    )


async def check_regime() -> CheckResult:
    try:
        # Force at least one refresh so we have recent stats
        await regime_state.refresh_once()
        feats = await regime_state.features()
    except Exception as e:
        return CheckResult(False, f"regime_state check failed: {e!r}")

    label_num = feats.get("regime_label_numeric", 0.0)
    trend = feats.get("trend_score", 0.0)
    vol = feats.get("volatility_score", 0.0)
    liq = feats.get("liquidity_score", 0.5)
    manip = feats.get("manipulation_risk", 0.0)

    try:
        regime_label = regime_state.label()
    except Exception:
        regime_label = feats.get("regime_label", "unknown")

    detail = (
        f"Regime={regime_label} "
        f"(trend={trend:+.3f}, vol={vol:.3f}, liq={liq:.3f}, manip={manip:.3f})"
    )

    return CheckResult(
        True,
        detail,
        extra={
            "regime_label": regime_label,
            "regime_label_numeric": label_num,
            "trend_score": trend,
            "volatility_score": vol,
            "liquidity_score": liq,
            "manipulation_risk": manip,
        },
    )


async def check_risk() -> CheckResult:
    try:
        limits = await current_limits()
        snap = await risk_snapshot()
        cap_snap = await risk_capacity_snapshot()
        capacity = await risk_capacity_score()
        can_enter = await should_allow_new_entry()
    except Exception as e:
        return CheckResult(False, f"Risk manager check failed: {e!r}")

    detail = (
        f"ticket≈${limits['ticket_usd']:.2f}, "
        f"max_conc={limits['max_concurrent']}, "
        f"risk_mode={limits.get('risk_mode', 1)}, "
        f"capacity={capacity:.3f}, "
        f"allow_new_entry={bool(can_enter)}"
    )

    extra = {
        "equity_usd": limits.get("equity_usd", 0.0),
        "ticket_usd": limits.get("ticket_usd", 0.0),
        "pos_pct_cap": limits.get("pos_pct_cap", 0.0),
        "max_concurrent": limits.get("max_concurrent", 0),
        "daily_spend_usd": limits.get("daily_spend_usd", 0.0),
        "risk_capacity": capacity,
        "risk_mode": limits.get("risk_mode", 1),
        "risk_mode_override": limits.get("risk_mode_override"),
        "allow_new_entry": bool(can_enter),
        # a few performance fields
        "winrate20": cap_snap.get("winrate20", 0.5),
        "rolling_dd_pct": cap_snap.get("rolling_dd_pct", 0.0),
        "daily_realized_pnl_usd": cap_snap.get("daily_realized_pnl_usd", 0.0),
        "daily_realized_pnl_pct": cap_snap.get("daily_realized_pnl_pct", 0.0),
        # snapshot can hold more granular stuff; keep it compact in text,
        # but still expose raw in extra for JSON:
        "risk_snapshot": snap,
        "risk_capacity_snapshot": cap_snap,
    }

    return CheckResult(True, detail, extra=extra)


async def check_router() -> CheckResult:
    """
    Router / Jupiter stack health. We keep this light and *never* crash:
    - Ensure utils_pkg.router is importable.
    - Optionally call describe_router() or read simple attributes.
    We intentionally don't fire heavy network requests here.
    """
    if _router_mod is None:
        return CheckResult(
            False,
            "utils_pkg.router not importable (router/Jupiter stack missing?).",
        )

    extra: Dict[str, Any] = {}
    detail_bits = []

    # Optional describe_router helper
    try:
        describe = getattr(_router_mod, "describe_router", None)
        if callable(describe):
            if asyncio.iscoroutinefunction(describe):
                desc = await describe()
            else:
                desc = describe()
            extra["describe_router"] = desc
            detail_bits.append(_safe_str(desc, max_len=80))
    except Exception as e:
        extra["describe_router_error"] = str(e)

    # Basic static fields (if present)
    try:
        base = getattr(_router_mod, "JUP_BASE_URL", None) or getattr(
            _router_mod, "BASE", None
        )
        if base:
            extra["jup_base_url"] = base
            detail_bits.append(f"JUP_BASE={base}")
    except Exception:
        pass

    try:
        as_legacy = getattr(_router_mod, "AS_LEGACY", None)
        if as_legacy is not None:
            extra["as_legacy"] = bool(as_legacy)
    except Exception:
        pass

    if not detail_bits:
        detail = "Router module importable."
    else:
        detail = "Router OK: " + " | ".join(detail_bits)

    return CheckResult(True, detail, extra=extra)


async def check_entry_ai() -> CheckResult:
    if ai_entry_logic is None:
        return CheckResult(
            False,
            "utils_pkg.ai_entry_logic not importable.",
        )

    extra: Dict[str, Any] = {}
    detail_parts = []

    # Try to introspect model / config if helpers exist
    try:
        describe = getattr(ai_entry_logic, "describe_entry_model", None)
        if callable(describe):
            if asyncio.iscoroutinefunction(describe):
                desc = await describe()
            else:
                desc = describe()
            extra["model_description"] = desc
            detail_parts.append(_safe_str(desc, max_len=80))
    except Exception as e:
        extra["model_description_error"] = str(e)

    try:
        cfg = getattr(ai_entry_logic, "current_entry_config", None)
        if callable(cfg):
            if asyncio.iscoroutinefunction(cfg):
                cfg_val = await cfg()
            else:
                cfg_val = cfg()
            extra["entry_config"] = cfg_val
            if isinstance(cfg_val, dict) and "ENTRY_MODEL_NAME" in cfg_val:
                detail_parts.append(f"model={cfg_val['ENTRY_MODEL_NAME']}")
    except Exception as e:
        extra["entry_config_error"] = str(e)

    if not detail_parts:
        detail = "AI entry logic present (no describe helpers)."
    else:
        detail = "AI entry OK: " + " | ".join(detail_parts)

    return CheckResult(True, detail, extra=extra)


async def check_ai_stack() -> CheckResult:
    """
    Wider AI stack coverage:
      - ai_entry_logic
      - ai_buy_scoring
      - feature_builder
      - self_learning_ai_strategy
      - ai_sell_logic
    We treat missing entry_logic or feature_builder as more serious than
    missing self-learning (still reported in extra).
    """
    present = []
    missing = []

    # Map logical names to modules
    modules = {
        "ai_entry_logic": ai_entry_logic,
        "ai_buy_scoring": _ai_buy_scoring,
        "feature_builder": _feature_builder,
        "self_learning_ai_strategy": _self_learning,
        "ai_sell_logic": _ai_sell_logic,
    }

    for name, mod in modules.items():
        if mod is not None:
            present.append(name)
        else:
            missing.append(name)

    # Hard requirements for "ok": entry_logic + feature_builder at least
    ok = ("ai_entry_logic" in present) and ("feature_builder" in present)

    detail = f"AI stack modules: present={present or '[]'}"
    if missing:
        detail += f"; missing={missing}"

    extra = {
        "present_modules": present,
        "missing_modules": missing,
    }

    return CheckResult(ok, detail, extra=extra)


async def check_backtest_sanity(
    days_back: int,
    limit_candidates: int,
) -> CheckResult:
    """
    Quick, tiny backtest to verify pipeline wiring (DB → feature builder → AI → exits).
    This is intentionally small, just to see that it runs, not to optimize PnL.
    """
    if run_backtest is None:
        return CheckResult(False, "utils_pkg.backtest_engine.run_backtest not importable.")

    cfg = {
        "ENTRY_SCORE_MIN": 0.60,
        "max_positions": 1,
        "days_back": int(days_back),
        "limit_candidates": int(limit_candidates),
        # keep everything else default / env-driven
    }

    try:
        stats = await run_backtest(cfg)
    except Exception as e:
        return CheckResult(False, f"run_backtest failed: {e!r}")

    n_trades = int(stats.get("n_trades", 0))
    pnl = float(stats.get("pnl_total_pct", stats.get("net_pnl_pct", 0.0)))

    detail = f"Backtest ran: trades={n_trades}, pnl_total_pct={pnl:.2f}"
    return CheckResult(True, detail, extra=stats)


# =============================================================================
# Env summary
# =============================================================================


def collect_env_summary() -> Dict[str, Any]:
    """
    Capture the key knobs that influence behaviour. We strip values and trim
    secrets so this is safe to print locally.
    """

    def _clean(key: str, max_len: int = 40) -> Optional[str]:
        v = os.getenv(key)
        if not v:
            return None
        return _safe_str(v, max_len=max_len)

    env: Dict[str, Any] = {
        # Network / cluster
        "NETWORK": _clean("NETWORK") or _clean("SOLANA_CLUSTER"),
        # Core feature flags
        "ENTRY_MOMENTUM_ENABLED": _clean("ENTRY_MOMENTUM_ENABLED"),
        "ENTRY_AI_ENABLED": _clean("ENTRY_AI_ENABLED"),
        "ENTRY_REQUIRE_RISK_CAPACITY": _clean("ENTRY_REQUIRE_RISK_CAPACITY"),
        "REGIME_ENABLED": _clean("REGIME_ENABLED"),
        "RISK_MODE_OVERRIDE": _clean("RISK_MODE_OVERRIDE"),
        # Risk / capital protection
        "STOP_AFTER_DAILY_PROFIT_PCT": _clean("STOP_AFTER_DAILY_PROFIT_PCT"),
        "DAILY_SPEND_CAP_PCT": _clean("DAILY_SPEND_CAP_PCT"),
        "MAX_CONCURRENT_POS": _clean("MAX_CONCURRENT_POS"),
        "RISK_PROFILE": _clean("RISK_PROFILE"),
        # Scanner toggles (if present)
        "ENABLE_RAYDIUM_SCANNER": _clean("ENABLE_RAYDIUM_SCANNER"),
        "ENABLE_PUMPFUN_SCANNER": _clean("ENABLE_PUMPFUN_SCANNER"),
        "ENABLE_DEXSCREENER_SCANNER": _clean("ENABLE_DEXSCREENER_SCANNER"),
        # Diagnostics / BT
        "BT_DAYS_BACK": _clean("BT_DAYS_BACK"),
        "BT_LIMIT_CANDIDATES": _clean("BT_LIMIT_CANDIDATES"),
    }

    # Remove None entries for cleanliness
    return {k: v for k, v in env.items() if v is not None}


# =============================================================================
# Pretty printer
# =============================================================================


def print_report(report: DiagnosticsReport) -> None:
    print("")
    print("=== Solana Sniper Diagnostics (V3+++) ===")
    print(f"Generated at: {report.generated_at}")
    print("")

    print("[ENV]")
    for k, v in sorted(report.env.items()):
        print(f"  {k}: {v}")
    print("")

    def _section(name: str, res: CheckResult):
        status = "OK" if res.ok else "FAIL"
        print(f"[{name}]  [{status}]")
        print(f"  {res.detail}")
        if res.extra:
            for ek, ev in sorted(res.extra.items()):
                print(f"    - {ek}: {_safe_str(ev, 80)}")
        print("")

    _section("DB", report.db)
    _section("REDIS", report.redis)
    _section("MARKET_DATA", report.market_data)
    _section("REGIME", report.regime)
    _section("RISK", report.risk)
    _section("ROUTER", report.router)
    _section("ENTRY_AI", report.entry_ai)
    _section("AI_STACK", report.ai_stack)

    if report.backtest is not None:
        _section("BACKTEST", report.backtest)

    # Simple overall status
    all_ok = (
        report.db.ok
        and report.redis.ok
        and report.market_data.ok
        and report.regime.ok
        and report.risk.ok
        and report.router.ok
        and report.entry_ai.ok
        and report.ai_stack.ok
        and (report.backtest.ok if report.backtest else True)
    )
    print("Overall:", "ALL GREEN ✅" if all_ok else "Issues detected ⚠️")
    print("")


# =============================================================================
# CLI / main
# =============================================================================


def build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Deep diagnostics for the Solana sniper bot (V3+++ / quant-grade)."
    )
    p.add_argument(
        "--json",
        action="store_true",
        help="Emit JSON instead of human-readable text.",
    )
    p.add_argument(
        "--no-backtest",
        action="store_true",
        help="Skip the small sanity backtest.",
    )
    p.add_argument(
        "--days-back",
        type=int,
        default=int(os.getenv("DIAG_BT_DAYS_BACK", "5")),
        help="Days back for sanity backtest (default from DIAG_BT_DAYS_BACK or 5).",
    )
    p.add_argument(
        "--limit-candidates",
        type=int,
        default=int(os.getenv("DIAG_BT_LIMIT_CANDIDATES", "200")),
        help="Max candidates for sanity backtest (default from env or 200).",
    )
    p.add_argument(
        "--deep",
        action="store_true",
        help="Enable deeper checks (currently: more aggressive BT horizon/cap).",
    )
    return p


async def run_diagnostics(args: argparse.Namespace) -> DiagnosticsReport:
    env_summary = collect_env_summary()

    db_res = await check_db()
    redis_res = await check_redis()
    mkt_res = await check_market_data()
    regime_res = await check_regime()
    risk_res = await check_risk()
    router_res = await check_router()
    entry_ai_res = await check_entry_ai()
    ai_stack_res = await check_ai_stack()

    bt_res: Optional[CheckResult] = None
    if not args.no_backtest:
        days_back = int(args.days_back)
        limit_cand = int(args.limit_candidates)
        if args.deep:
            # Deep mode: slightly more demanding sanity BT
            days_back = max(days_back, 14)
            limit_cand = max(limit_cand, 500)
        bt_res = await check_backtest_sanity(
            days_back=days_back,
            limit_candidates=limit_cand,
        )

    report = DiagnosticsReport(
        generated_at=datetime.utcnow().isoformat() + "Z",
        env=env_summary,
        db=db_res,
        redis=redis_res,
        market_data=mkt_res,
        regime=regime_res,
        risk=risk_res,
        router=router_res,
        entry_ai=entry_ai_res,
        ai_stack=ai_stack_res,
        backtest=bt_res,
    )
    return report


def main() -> None:
    parser = build_arg_parser()
    args = parser.parse_args()
    try:
        report = asyncio.run(run_diagnostics(args))
    except KeyboardInterrupt:
        print("Interrupted by user.", file=sys.stderr)
        sys.exit(130)
    except Exception as e:
        print(f"Diagnostics crashed unexpectedly: {e!r}", file=sys.stderr)
        sys.exit(1)

    if args.json:
        print(json.dumps(report.to_dict(), indent=2, default=str))
    else:
        print_report(report)

    # Exit code non-zero if any critical check failed (for CI / Render checks)
    crit = [
        report.db.ok,
        report.redis.ok,
        report.market_data.ok,
        report.regime.ok,
        report.risk.ok,
        report.router.ok,
        report.entry_ai.ok,
        report.ai_stack.ok,
    ]
    if report.backtest is not None:
        crit.append(report.backtest.ok)

    sys.exit(0 if all(crit) else 1)


if __name__ == "__main__":
    main()
