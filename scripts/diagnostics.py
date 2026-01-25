# scripts/diagnostics.py
from __future__ import annotations

"""
Deep diagnostics / sanity checker (V3+++ / quant-grade, P0-ready).

Purpose
-------
Fast, high-signal answer to: "Is everything wired correctly and safe to trade?"

What it checks (best-effort, never hard-crashes)
------------------------------------------------
- Env presence (names only; no secrets)
- Core infra init/close (utils_pkg.core_utils.init_resources / close_resources)
- Postgres / ai_db connectivity (ai_db.describe_backend + ai_db.ping)
- Redis connectivity + halt flags
- Market data (SOL/USD quote)
- Router/Jupiter module import + lightweight describe
- Regime engine refresh + features
- Risk manager limits + capacity + allow_new_entry
- Entry AI module presence + diag snapshot (if available)
- Wider AI stack module presence
- Optional mini backtest sanity run (if backtest engine present)

Usage
-----
  python -m scripts.diagnostics
  python -m scripts.diagnostics --json
  python -m scripts.diagnostics --out exports/diagnostics.json --json
  python -m scripts.diagnostics --no-backtest
  python -m scripts.diagnostics --deep
  python -m scripts.diagnostics --timeout 15

Exit codes
----------
0  => all critical checks OK
1  => one or more critical checks failed
2  => diagnostics crashed unexpectedly (should be rare)
"""

import argparse
import asyncio
import contextlib
import json
import os
import sys
import time
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from logging_setup import get_logger

log = get_logger(__name__)

# Ensure repo root importable (running as module already, but keep consistent)
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


# ──────────────────────────────────────────────────────────────
# Constants / knobs
# ──────────────────────────────────────────────────────────────

HALT_KEYS = ["halt:infra", "halt:manual", "halt:daily_loss", "infra:halt", "infra:halt_reason"]

# Names only. Never print values.
ENV_KEY_GROUPS: dict[str, Sequence[str]] = {
    "wallet": ("WALLET_PRIVATE_KEY", "KEYPAIR_PATH", "WALLET_PUBKEY"),
    "rpc": ("RPC_URL", "HELIUS_RPC_URL"),
    "redis": ("REDIS_URL",),
    "postgres": ("POSTGRES_DSN", "PG_DSN", "PG_DSN_EXTERNAL", "PG_DSN_INTERNAL"),
    "notion": ("NOTION_TOKEN", "NOTION_API_KEY", "NOTION_DATABASE_ID", "NOTION_DB_TRADES"),
    "alerts": ("TELEGRAM_BOT_TOKEN", "TELEGRAM_CHAT_ID", "DISCORD_BOT_TOKEN"),
    "ai": ("OPENAI_API_KEY", "OPENAI_MODEL", "OPENAI_BASE_URL"),
}

CRITICAL_GROUPS = ("wallet", "rpc")  # things that must exist for live trading to function
SOFT_CRITICAL_GROUPS = ("redis", "postgres")  # strongly recommended for full pipeline


# ──────────────────────────────────────────────────────────────
# Data structures
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


@dataclass
class CheckResult:
    ok: bool
    detail: str = ""
    error: str | None = None
    extra: dict[str, Any] = field(default_factory=dict)
    duration_ms: int = 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "ok": bool(self.ok),
            "detail": self.detail,
            "error": self.error,
            "extra": self.extra,
            "duration_ms": self.duration_ms,
        }


@dataclass
class DiagnosticsReport:
    generated_at: str
    args: dict[str, Any]
    env: dict[str, Any]
    env_presence: dict[str, Any]
    infra: CheckResult
    db: CheckResult
    redis: CheckResult
    market_data: CheckResult
    regime: CheckResult
    risk: CheckResult
    router: CheckResult
    entry_ai: CheckResult
    ai_stack: CheckResult
    backtest: CheckResult | None = None
    summary: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "generated_at": self.generated_at,
            "args": self.args,
            "env": self.env,
            "env_presence": self.env_presence,
            "infra": self.infra.to_dict(),
            "db": self.db.to_dict(),
            "redis": self.redis.to_dict(),
            "market_data": self.market_data.to_dict(),
            "regime": self.regime.to_dict(),
            "risk": self.risk.to_dict(),
            "router": self.router.to_dict(),
            "entry_ai": self.entry_ai.to_dict(),
            "ai_stack": self.ai_stack.to_dict(),
            "backtest": self.backtest.to_dict() if self.backtest else None,
            "summary": self.summary,
        }


# ──────────────────────────────────────────────────────────────
# Generic wrappers
# ──────────────────────────────────────────────────────────────


async def _maybe_await(x: Any) -> Any:
    if asyncio.iscoroutine(x):
        return await x
    return x


async def run_check(
    name: str,
    fn: Callable[[], Awaitable[CheckResult]],
    *,
    timeout_s: int,
) -> CheckResult:
    t0 = time.perf_counter()
    try:
        res = await asyncio.wait_for(fn(), timeout=timeout_s)
        if not isinstance(res, CheckResult):
            res = CheckResult(ok=False, detail=f"{name}: invalid result type {type(res)}")
    except TimeoutError:
        res = CheckResult(ok=False, detail=f"{name} timed out after {timeout_s}s", error="timeout")
    except Exception as e:
        res = CheckResult(ok=False, detail=f"{name} failed", error=_safe_str(e))
    res.duration_ms = int((time.perf_counter() - t0) * 1000)
    return res


def _atomic_write(path: Path, data: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(data, encoding="utf-8")
    tmp.replace(path)


# ──────────────────────────────────────────────────────────────
# Env summary
# ──────────────────────────────────────────────────────────────


def collect_env_summary() -> dict[str, Any]:
    """
    Only non-secret knobs (values trimmed). This is for human context,
    but we avoid obvious secret keys.
    """

    def _get(k: str, max_len: int = 60) -> str | None:
        v = os.getenv(k)
        if not v:
            return None
        return _safe_str(v, max_len=max_len)

    keys = [
        "NETWORK",
        "SOLANA_CLUSTER",
        "RISK_PROFILE",
        "ENTRY_MOMENTUM_ENABLED",
        "ENTRY_AI_ENABLED",
        "AI_ENTRY_ENABLED",
        "REGIME_ENABLED",
        "RISK_MANAGER_ENABLED",
        "EXIT_ENGINE_ENABLED",
        "RISK_MODE_OVERRIDE",
        "MAX_CONCURRENT_POS",
        "DAILY_SPEND_CAP_PCT",
        "STOP_AFTER_DAILY_PROFIT_PCT",
        "STOP_AFTER_DAILY_LOSS_PCT",
        "ENTRY_MIN_INITIAL_BUY_SOL",
        "BUY_SOL_PER_TRADE",
        "SCANNER_MIN_BUYS_5M",
    ]
    out: dict[str, Any] = {}
    for k in keys:
        v = _get(k)
        if v is not None:
            out[k] = v
    return out


def env_presence_summary() -> dict[str, Any]:
    """
    Presence-only: for each group, list which keys exist (names only).
    """
    groups: dict[str, dict[str, Any]] = {}
    for g, ks in ENV_KEY_GROUPS.items():
        present = [k for k in ks if os.getenv(k) not in (None, "")]
        missing = [k for k in ks if k not in present]
        groups[g] = {"present": present, "missing": missing}
    # rollups
    crit_missing = []
    for g in CRITICAL_GROUPS:
        crit_missing.extend(groups.get(g, {}).get("missing", []))
    soft_missing = []
    for g in SOFT_CRITICAL_GROUPS:
        soft_missing.extend(groups.get(g, {}).get("missing", []))
    return {
        "groups": groups,
        "critical_missing": sorted(set(crit_missing)),
        "soft_missing": sorted(set(soft_missing)),
    }


# ──────────────────────────────────────────────────────────────
# Shared infra init/close
# ──────────────────────────────────────────────────────────────


async def check_infra_init() -> CheckResult:
    """
    Bring up shared resources if available: init_resources().
    We treat failure as critical because market/redis checks depend on it.
    """
    try:
        from utils_pkg import core_utils as cu  # type: ignore
    except Exception as e:
        return CheckResult(
            ok=False, detail="utils_pkg.core_utils not importable", error=_safe_str(e)
        )

    init_fn = getattr(cu, "init_resources", None)
    if not callable(init_fn):
        # Not fatal, but likely indicates drift.
        return CheckResult(ok=True, detail="core_utils.init_resources not found (skipping init).")

    try:
        await _maybe_await(init_fn())
        return CheckResult(ok=True, detail="init_resources OK")
    except Exception as e:
        return CheckResult(ok=False, detail="init_resources failed", error=_safe_str(e))


async def close_infra_best_effort() -> None:
    try:
        from utils_pkg import core_utils as cu  # type: ignore

        close_fn = getattr(cu, "close_resources", None)
        if callable(close_fn):
            await _maybe_await(close_fn())
    except Exception:
        return


# ──────────────────────────────────────────────────────────────
# Individual checks (best-effort)
# ──────────────────────────────────────────────────────────────


async def check_db() -> CheckResult:
    try:
        from utils_pkg import ai_db  # type: ignore
    except Exception as e:
        return CheckResult(ok=False, detail="utils_pkg.ai_db not importable", error=_safe_str(e))

    extra: dict[str, Any] = {}
    try:
        desc = ai_db.describe_backend()
        extra["backend"] = desc
    except Exception as e:
        extra["describe_backend_error"] = _safe_str(e)

    try:
        ping_fn = getattr(ai_db, "ping", None)
        if not callable(ping_fn):
            return CheckResult(ok=False, detail="ai_db.ping not found", extra=extra)
        ok = await _maybe_await(ping_fn())
    except Exception as e:
        return CheckResult(ok=False, detail="ai_db.ping raised", error=_safe_str(e), extra=extra)

    if not ok:
        return CheckResult(ok=False, detail="Postgres ping returned False", extra=extra)

    return CheckResult(ok=True, detail="Postgres reachable", extra=extra)


async def _get_redis_client() -> tuple[Any | None, dict[str, Any]]:
    """
    Prefer canonical async getter if available; otherwise use exported client.
    """
    meta: dict[str, Any] = {}
    try:
        from utils_pkg import core_utils as cu  # type: ignore
    except Exception as e:
        meta["core_utils_import_error"] = _safe_str(e)
        return None, meta

    # 1) canonical async getter (preferred)
    get_fn = getattr(cu, "get_redis_async", None)
    if callable(get_fn):
        try:
            r = await _maybe_await(get_fn())
            if r is not None:
                meta["redis_source"] = "core_utils.get_redis_async"
                return r, meta
        except Exception as e:
            meta["get_redis_async_error"] = _safe_str(e)

    # 2) exported client
    try:
        r = getattr(cu, "redis", None)
        if r is not None:
            meta["redis_source"] = "core_utils.redis"
            return r, meta
    except Exception as e:
        meta["redis_attr_error"] = _safe_str(e)

    return None, meta


async def check_redis() -> CheckResult:
    r, meta = await _get_redis_client()
    if r is None:
        return CheckResult(ok=False, detail="No Redis client available", extra=meta)

    try:
        pong = await _maybe_await(r.ping())
    except Exception as e:
        return CheckResult(ok=False, detail="redis.ping failed", error=_safe_str(e), extra=meta)

    if pong not in (True, "PONG", b"PONG"):
        meta["ping_return"] = pong
        return CheckResult(ok=False, detail="redis.ping returned unexpected value", extra=meta)

    extra = dict(meta)
    # Halt flags
    halts: dict[str, Any] = {}
    try:
        for k in HALT_KEYS:
            try:
                v = await _maybe_await(r.get(k))
            except Exception:
                v = None
            if v is not None:
                halts[k] = v
    except Exception as e:
        extra["halt_read_error"] = _safe_str(e)

    extra["halts"] = halts
    halted = False
    # Any known halt key indicating stop
    if str(halts.get("halt:infra", "")).strip() in {"1", "true", "True"}:
        halted = True
    if str(halts.get("infra:halt", "")).strip() in {"1", "true", "True"}:
        halted = True
    if str(halts.get("halt:manual", "")).strip() in {"1", "true", "True"}:
        halted = True
    if str(halts.get("halt:daily_loss", "")).strip() in {"1", "true", "True"}:
        halted = True

    detail = "Redis reachable"
    if halted:
        detail += " (HALT ACTIVE — entries likely blocked)"

    return CheckResult(ok=True, detail=detail, extra=extra)


async def check_market_data() -> CheckResult:
    try:
        from utils_pkg import core_utils as cu  # type: ignore
    except Exception as e:
        return CheckResult(ok=False, detail="core_utils not importable", error=_safe_str(e))

    get_fn = getattr(cu, "get_sol_usd", None)
    if not callable(get_fn):
        return CheckResult(ok=False, detail="core_utils.get_sol_usd not found")

    try:
        sol = await _maybe_await(get_fn())
    except Exception as e:
        return CheckResult(ok=False, detail="get_sol_usd failed", error=_safe_str(e))

    try:
        sol_f = float(sol)
    except Exception:
        sol_f = -1.0

    if sol_f <= 0:
        return CheckResult(ok=False, detail=f"Invalid SOL/USD quote: {sol!r}")

    return CheckResult(ok=True, detail="Market data OK", extra={"sol_usd": sol_f})


async def check_router() -> CheckResult:
    try:
        from utils_pkg import router as r  # type: ignore
    except Exception as e:
        return CheckResult(ok=False, detail="utils_pkg.router not importable", error=_safe_str(e))

    extra: dict[str, Any] = {}
    # lightweight describe (never swaps)
    try:
        describe = getattr(r, "describe_router", None)
        if callable(describe):
            extra["describe_router"] = await _maybe_await(describe())
    except Exception as e:
        extra["describe_router_error"] = _safe_str(e)

    # common config constants (best-effort)
    for k in ("JUP_BASE_URL", "JUPITER_BASE_URL", "BASE_URL", "ROUTE_GUARD", "ROUTE_GUARD_MODE"):
        try:
            v = getattr(r, k, None)
            if v is not None:
                extra[k] = v
        except Exception:
            continue

    detail = "Router import OK"
    if "describe_router" in extra:
        detail = "Router OK: " + _safe_str(extra["describe_router"], 140)

    return CheckResult(ok=True, detail=detail, extra=extra)


async def check_regime() -> CheckResult:
    try:
        from utils_pkg.regime import regime_state  # type: ignore
    except Exception as e:
        return CheckResult(
            ok=False, detail="utils_pkg.regime.regime_state not importable", error=_safe_str(e)
        )

    extra: dict[str, Any] = {}
    try:
        refresh = getattr(regime_state, "refresh_once", None)
        if callable(refresh):
            await _maybe_await(refresh())
        feats = await _maybe_await(regime_state.features())
        if isinstance(feats, dict):
            extra.update(feats)
    except Exception as e:
        return CheckResult(ok=False, detail="regime_state failed", error=_safe_str(e))

    # Compact summary
    label = None
    try:
        label_fn = getattr(regime_state, "label", None)
        if callable(label_fn):
            label = await _maybe_await(label_fn())
    except Exception:
        label = None

    def _f(key: str, default: float = 0.0) -> float:
        try:
            return float(extra.get(key, default))
        except Exception:
            return float(default)

    trend = _f("trend_score", 0.0)
    vol = _f("volatility_score", 0.0)
    liq = _f("liquidity_score", 0.5)
    manip = _f("manipulation_risk", _f("regime_manipulation_risk", 0.0))
    regime_label = label or extra.get("regime_label") or "unknown"

    detail = f"Regime={regime_label} (trend={trend:+.3f}, vol={vol:.3f}, liq={liq:.3f}, manip={manip:.3f})"
    # keep extra bounded for JSON (but don’t delete; just avoid huge payloads)
    return CheckResult(
        ok=True,
        detail=detail,
        extra={
            "regime_label": regime_label,
            "trend_score": trend,
            "volatility_score": vol,
            "liquidity_score": liq,
            "manipulation_risk": manip,
            # keep raw too if you want dashboards to consume it
            "raw": extra,
        },
    )


async def check_risk() -> CheckResult:
    try:
        from utils_pkg.risk_manager import (  # type: ignore
            current_limits,
            risk_capacity_score,
            risk_capacity_snapshot,
            risk_snapshot,
            should_allow_new_entry,
        )
    except Exception as e:
        return CheckResult(
            ok=False, detail="utils_pkg.risk_manager not importable", error=_safe_str(e)
        )

    try:
        limits_task = asyncio.create_task(current_limits())
        snap_task = asyncio.create_task(risk_snapshot())
        cap_task = asyncio.create_task(risk_capacity_snapshot())
        score_task = asyncio.create_task(risk_capacity_score())
        allow_task = asyncio.create_task(should_allow_new_entry())

        limits = await limits_task
        snap = await snap_task
        cap = await cap_task
        score = await score_task
        allow = await allow_task
    except Exception as e:
        return CheckResult(ok=False, detail="Risk manager calls failed", error=_safe_str(e))

    # Compact summary
    def _getf(d: dict[str, Any], k: str, default: float = 0.0) -> float:
        try:
            return float(d.get(k, default))
        except Exception:
            return float(default)

    ticket = _getf(limits, "ticket_usd", 0.0)
    equity = _getf(limits, "equity_usd", 0.0)
    max_conc = int(limits.get("max_concurrent") or 0)
    risk_mode = int(limits.get("risk_mode") or 1)
    daily_spend = _getf(limits, "daily_spend_usd", 0.0)

    winrate20 = _getf(cap, "winrate20", 0.0)
    rolling_dd = _getf(cap, "rolling_dd_pct", 0.0)
    day_pnl_pct = _getf(cap, "daily_realized_pnl_pct", 0.0)

    detail = (
        f"equity=${equity:.2f}, ticket=${ticket:.2f}, max_conc={max_conc}, "
        f"risk_mode={risk_mode}, capacity={float(score):.3f}, allow_new_entry={bool(allow)}"
    )

    extra = {
        "limits": limits,
        "risk_snapshot": snap,
        "risk_capacity_snapshot": cap,
        "risk_capacity_score": float(score) if score is not None else None,
        "allow_new_entry": bool(allow),
        "high_signal": {
            "winrate20": winrate20,
            "rolling_dd_pct": rolling_dd,
            "daily_realized_pnl_pct": day_pnl_pct,
            "daily_spend_usd": daily_spend,
        },
    }
    return CheckResult(ok=True, detail=detail, extra=extra)


async def check_entry_ai() -> CheckResult:
    try:
        from utils_pkg import ai_entry_logic  # type: ignore
    except Exception as e:
        return CheckResult(
            ok=False, detail="utils_pkg.ai_entry_logic not importable", error=_safe_str(e)
        )

    extra: dict[str, Any] = {}
    # Best-effort diag snapshot
    for attr in ("diag_snapshot", "gate_snapshot"):
        try:
            fn = getattr(ai_entry_logic, attr, None)
            if callable(fn):
                out = await _maybe_await(fn())
                if isinstance(out, dict):
                    extra[attr] = out
        except Exception as e:
            extra[f"{attr}_error"] = _safe_str(e)

    try:
        cur = getattr(ai_entry_logic, "current_diag", None)
        if isinstance(cur, dict):
            extra["current_diag"] = cur
    except Exception:
        pass

    # Compact summary
    gate = None
    try:
        gate = extra.get("diag_snapshot", {}).get("ai_score_gate")  # type: ignore[union-attr]
    except Exception:
        gate = None

    detail = "AI entry logic present"
    if gate is not None:
        detail = f"AI entry OK (ai_score_gate={_safe_str(gate, 50)})"

    return CheckResult(ok=True, detail=detail, extra=extra)


async def check_ai_stack() -> CheckResult:
    """
    Wider AI/quant stack presence (imports only).
    We treat feature_builder + ai_entry_logic as hard requirements for P0.
    """
    mods = {
        "feature_builder": "utils_pkg.feature_builder",
        "ai_entry_logic": "utils_pkg.ai_entry_logic",
        "ai_buy_scoring": "utils_pkg.ai_buy_scoring",
        "ai_sell_logic": "utils_pkg.ai_sell_logic",
        "self_learning_ai_strategy": "utils_pkg.self_learning_ai_strategy",
        "exit_engine": "utils_pkg.exit_engine",
        "scanner_registry": "scanner.registry",
    }

    present, missing = [], []
    for name, path in mods.items():
        try:
            __import__(path)
            present.append(name)
        except Exception:
            missing.append(name)

    ok = ("feature_builder" in present) and ("ai_entry_logic" in present)
    detail = f"present={present}"
    if missing:
        detail += f"; missing={missing}"
    return CheckResult(ok=ok, detail=detail, extra={"present": present, "missing": missing})


async def check_backtest(days_back: int, limit_candidates: int, deep: bool) -> CheckResult:
    """
    Tiny sanity backtest (optional). Never used as “truth” for performance.
    Only to prove the pipeline can run end-to-end.
    """
    try:
        from utils_pkg.backtest_engine import run_backtest  # type: ignore
    except Exception as e:
        return CheckResult(
            ok=False, detail="utils_pkg.backtest_engine not importable", error=_safe_str(e)
        )

    # Deep makes it a bit more demanding, but still bounded
    db = max(2, int(days_back))
    lim = max(50, int(limit_candidates))
    if deep:
        db = max(db, 14)
        lim = max(lim, 500)

    cfg = {
        "days_back": db,
        "limit_candidates": lim,
        "max_positions": 1,
        "ENTRY_SCORE_MIN": 0.60,
    }

    try:
        stats = await _maybe_await(run_backtest(cfg))
        if not isinstance(stats, dict):
            return CheckResult(ok=False, detail=f"run_backtest returned {type(stats)}")
    except Exception as e:
        return CheckResult(
            ok=False, detail="run_backtest failed", error=_safe_str(e), extra={"cfg": cfg}
        )

    n_trades = int(stats.get("n_trades", stats.get("trades", 0)) or 0)
    pnl = stats.get("pnl_total_pct", stats.get("net_pnl_pct", None))
    detail = f"Backtest ran: trades={n_trades}, pnl_total_pct={_safe_str(pnl, 20)}"
    return CheckResult(ok=True, detail=detail, extra={"cfg": cfg, "stats": stats})


# ──────────────────────────────────────────────────────────────
# Summary logic / pretty printing
# ──────────────────────────────────────────────────────────────


def _traffic(risk_mode: int | None, allow_new_entry: bool | None, halted: bool) -> str:
    if halted or (risk_mode == 0) or (allow_new_entry is False):
        return "🔴 BLOCKED"
    if risk_mode == 3:
        return "🟠 DEFENSIVE"
    if risk_mode == 2:
        return "🟢 AGGRESSIVE"
    return "🟡 NORMAL"


def print_report(rep: DiagnosticsReport) -> None:
    print("")
    print("=== Solana Sniper Diagnostics (V3+++) ===")
    print(f"Generated at: {rep.generated_at}")
    print("")

    def sec(title: str, r: CheckResult) -> None:
        status = "OK" if r.ok else "FAIL"
        print(f"[{title}] [{status}] ({r.duration_ms} ms)")
        print(f"  {r.detail}")
        if r.error:
            print(f"  error: {r.error}")
        if r.extra:
            # keep human output compact
            for k in sorted(r.extra.keys()):
                v = r.extra[k]
                if k in {"raw", "limits", "risk_snapshot", "risk_capacity_snapshot", "stats"}:
                    continue
                print(f"  - {k}: {_safe_str(v, 160)}")
        print("")

    # Env
    print("[ENV (non-secret knobs)]")
    if rep.env:
        for k, v in sorted(rep.env.items()):
            print(f"  {k}: {v}")
    else:
        print("  (none)")
    print("")

    # Env presence
    pres = rep.env_presence.get("groups", {})
    print("[ENV PRESENCE (names only)]")
    for g in sorted(pres.keys()):
        pg = pres[g]
        print(f"  {g}: present={pg.get('present', [])} missing={pg.get('missing', [])}")
    crit_missing = rep.env_presence.get("critical_missing", [])
    if crit_missing:
        print(f"  CRITICAL MISSING: {crit_missing}")
    soft_missing = rep.env_presence.get("soft_missing", [])
    if soft_missing:
        print(f"  SOFT MISSING: {soft_missing}")
    print("")

    # Checks
    sec("INFRA", rep.infra)
    sec("DB", rep.db)
    sec("REDIS", rep.redis)
    sec("MARKET", rep.market_data)
    sec("REGIME", rep.regime)
    sec("RISK", rep.risk)
    sec("ROUTER", rep.router)
    sec("ENTRY_AI", rep.entry_ai)
    sec("AI_STACK", rep.ai_stack)
    if rep.backtest is not None:
        sec("BACKTEST", rep.backtest)

    # Summary / readiness
    s = rep.summary or {}
    print("=== SUMMARY ===")
    print(f"Readiness: {s.get('readiness', 'unknown')}")
    if s.get("notes"):
        for n in s["notes"]:
            print(f"- {n}")
    print("")


# ──────────────────────────────────────────────────────────────
# Main runner
# ──────────────────────────────────────────────────────────────


def build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Deep diagnostics / sanity checker (V3+++).")
    p.add_argument("--json", action="store_true", help="Emit JSON output.")
    p.add_argument("--out", type=str, default=None, help="Write output to file (json or text).")
    p.add_argument(
        "--timeout",
        type=int,
        default=int(os.getenv("DIAG_TIMEOUT_S", "12")),
        help="Per-check timeout in seconds (default: 12 or DIAG_TIMEOUT_S).",
    )
    p.add_argument("--no-backtest", action="store_true", help="Skip mini backtest sanity run.")
    p.add_argument(
        "--days-back",
        type=int,
        default=int(os.getenv("DIAG_BT_DAYS_BACK", "5")),
        help="Backtest days horizon (default: 5 or DIAG_BT_DAYS_BACK).",
    )
    p.add_argument(
        "--limit-candidates",
        type=int,
        default=int(os.getenv("DIAG_BT_LIMIT_CANDIDATES", "200")),
        help="Backtest candidate cap (default: 200 or DIAG_BT_LIMIT_CANDIDATES).",
    )
    p.add_argument("--deep", action="store_true", help="Heavier checks (still bounded).")
    return p


async def run_all(args: argparse.Namespace) -> DiagnosticsReport:
    env = collect_env_summary()
    env_pres = env_presence_summary()

    # 1) infra init (first)
    infra = await run_check("infra", check_infra_init, timeout_s=max(5, args.timeout))

    # 2) run checks (some depend on infra)
    # Keep concurrency, but do not spam too hard.
    tasks = {
        "db": asyncio.create_task(run_check("db", check_db, timeout_s=args.timeout)),
        "redis": asyncio.create_task(run_check("redis", check_redis, timeout_s=args.timeout)),
        "market": asyncio.create_task(
            run_check("market", check_market_data, timeout_s=args.timeout)
        ),
        "regime": asyncio.create_task(run_check("regime", check_regime, timeout_s=args.timeout)),
        "risk": asyncio.create_task(run_check("risk", check_risk, timeout_s=args.timeout)),
        "router": asyncio.create_task(run_check("router", check_router, timeout_s=args.timeout)),
        "entry_ai": asyncio.create_task(
            run_check("entry_ai", check_entry_ai, timeout_s=args.timeout)
        ),
        "ai_stack": asyncio.create_task(
            run_check("ai_stack", check_ai_stack, timeout_s=args.timeout)
        ),
    }

    db = await tasks["db"]
    redis = await tasks["redis"]
    market = await tasks["market"]
    regime = await tasks["regime"]
    risk = await tasks["risk"]
    router = await tasks["router"]
    entry_ai = await tasks["entry_ai"]
    ai_stack = await tasks["ai_stack"]

    backtest_res: CheckResult | None = None
    if not args.no_backtest:
        backtest_res = await run_check(
            "backtest",
            lambda: check_backtest(args.days_back, args.limit_candidates, args.deep),
            timeout_s=max(args.timeout, 20 if args.deep else 12),
        )

    # Build summary hints
    notes = []
    if env_pres.get("critical_missing"):
        notes.append(f"Critical env missing: {env_pres['critical_missing']}")
    if infra.ok is False:
        notes.append("Infra init failed (init_resources). Many checks may be unreliable.")

    halted = False
    try:
        halts = (redis.extra or {}).get("halts", {})
        if isinstance(halts, dict):
            # any halt "truthy" indicates likely block
            for v in halts.values():
                if str(v).strip() in {"1", "true", "True", "YES", "yes", "on"}:
                    halted = True
    except Exception:
        halted = False

    # risk_mode + allow_new_entry best-effort
    risk_mode = None
    allow_new_entry = None
    with contextlib.suppress(Exception):
        allow_new_entry = (risk.extra or {}).get("allow_new_entry")
    try:
        limits = (risk.extra or {}).get("limits", {})
        if isinstance(limits, dict):
            risk_mode = int(limits.get("risk_mode") or 1)
    except Exception:
        pass

    readiness = _traffic(risk_mode, allow_new_entry, halted)

    # Decide what is "critical"
    critical_ok = all(
        [
            infra.ok,
            db.ok,  # brain memory
            redis.ok,  # halt/pnl + infra gating
            market.ok,  # market data
            regime.ok,  # regime engine
            risk.ok,  # risk manager
            router.ok,  # execution pipe
            entry_ai.ok,  # entry intelligence
            ai_stack.ok,  # pipeline wiring
        ]
    )

    if backtest_res is not None and args.deep:
        # In deep mode, treat BT failure as advisory but mention it
        if not backtest_res.ok:
            notes.append("Backtest sanity failed in --deep mode (advisory).")

    if readiness.startswith("🔴"):
        notes.append("Trading appears BLOCKED (halt key, risk_mode=0, or allow_new_entry=false).")
    if not critical_ok:
        notes.append("One or more critical checks failed — do NOT go live until fixed.")

    rep = DiagnosticsReport(
        generated_at=_utc_now(),
        args={
            "json": bool(args.json),
            "out": args.out,
            "timeout": int(args.timeout),
            "no_backtest": bool(args.no_backtest),
            "days_back": int(args.days_back),
            "limit_candidates": int(args.limit_candidates),
            "deep": bool(args.deep),
        },
        env=env,
        env_presence=env_pres,
        infra=infra,
        db=db,
        redis=redis,
        market_data=market,
        regime=regime,
        risk=risk,
        router=router,
        entry_ai=entry_ai,
        ai_stack=ai_stack,
        backtest=backtest_res,
        summary={
            "readiness": readiness,
            "critical_ok": bool(critical_ok),
            "risk_mode": risk_mode,
            "allow_new_entry": allow_new_entry,
            "halted": halted,
            "notes": notes,
        },
    )
    return rep


def main() -> None:
    args = build_arg_parser().parse_args()

    try:
        rep = asyncio.run(run_all(args))
    except KeyboardInterrupt:
        print("Interrupted.", file=sys.stderr)
        sys.exit(130)
    except Exception as e:
        print(f"Diagnostics crashed unexpectedly: {_safe_str(e)}", file=sys.stderr)
        sys.exit(2)
    finally:
        # Always try to close shared infra
        with contextlib.suppress(Exception):
            asyncio.run(close_infra_best_effort())

    # Output
    if args.json:
        payload = json.dumps(rep.to_dict(), indent=2, default=str)
    else:
        # Build a text payload by printing into a string
        # (so --out writes exactly what you see)
        from io import StringIO

        buf = StringIO()
        stdout = sys.stdout
        try:
            sys.stdout = buf
            print_report(rep)
        finally:
            sys.stdout = stdout
        payload = buf.getvalue()

    if args.out:
        out_path = Path(args.out)
        if not out_path.is_absolute():
            out_path = (ROOT / out_path).resolve()
        _atomic_write(out_path, payload)
        print(f"✅ Wrote {out_path}")
    else:
        print(payload, end="" if payload.endswith("\n") else "\n")

    # Exit code: 0 only if critical_ok
    sys.exit(0 if rep.summary.get("critical_ok") else 1)


if __name__ == "__main__":
    main()
