#!/usr/bin/env python3
from __future__ import annotations

"""
scripts/pnl_peek.py — Quant-grade PnL / risk snapshot (V3+++)

What this script shows
----------------------
Primary (from Redis, risk_manager-compatible keys):
  • Daily realized PnL (USD) for today and recent days:
        "pnl:day:realized_usd:YYYYMMDD"
  • Daily spend / budget used (USD) when available:
        "budget:sniper:spent_usd:YYYYMMDD"
  • Aggregate metrics (from risk_manager):
        "metrics:winrate20"
        "metrics:rolling_dd_pct"
        "metrics:equity_hwm_usd"
        "metrics:trades_total"
        "metrics:wins_total"

Secondary (live risk / regime / AI, optional best-effort):
  • risk_manager.risk_snapshot()
  • regime_state.features()
  • utils_pkg.data_logger.summarize_trading_day() (if present)

Features
--------
  • Human-readable table or JSON (`--json`).
  • Watch mode (`--watch N`): refresh every N seconds (PnL dashboard).
  • Safe on missing Redis / risk_manager / regime — degrades gracefully.
"""

import argparse
import asyncio
import json
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

# ──────────────────────────────────────────────────────────────
# Repo root / .env bootstrap
# ──────────────────────────────────────────────────────────────

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

try:
    from dotenv import load_dotenv, find_dotenv  # type: ignore[import]
except Exception:  # pragma: no cover
    load_dotenv = None  # type: ignore[assignment]

if load_dotenv is not None:
    load_dotenv(find_dotenv(), override=False)

# Core infra: Redis / Solana / pricefeed
import utils_pkg.core_utils as cu  # type: ignore[import]

# Optional risk/regime/AI telemetry
try:  # pragma: no cover - optional
    from utils_pkg import risk_manager  # type: ignore[import]
except Exception:  # pragma: no cover
    risk_manager = None  # type: ignore[assignment]

try:  # pragma: no cover - optional
    from utils_pkg.regime import regime_state  # type: ignore[import]
except Exception:  # pragma: no cover
    regime_state = None  # type: ignore[assignment]

try:  # pragma: no cover - optional
    from utils_pkg.data_logger import summarize_trading_day  # type: ignore[import]
except Exception:  # pragma: no cover
    summarize_trading_day = None  # type: ignore[assignment]


# ──────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────


def _date_utc(days_ago: int) -> datetime.date:
    return datetime.now(timezone.utc).date() - timedelta(days=days_ago)


def _date_str(days_ago: int) -> str:
    return _date_utc(days_ago).strftime("%Y%m%d")


def _daily_pnl_key(date_str: str) -> str:
    # Mirrors utils_pkg.risk_manager._pnl_key_today() pattern
    return f"pnl:day:realized_usd:{date_str}"


def _daily_budget_key(date_str: str) -> str:
    return f"budget:sniper:spent_usd:{date_str}"


def _fmt_money(v: Optional[float]) -> str:
    if v is None:
        return "   ??.??"
    return f"{v:8.2f}"


def _fmt_pct(v: Optional[float]) -> str:
    if v is None:
        return " ??.??%"
    return f"{v:6.2f}%"


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except Exception:
        return default


def _env_bool(name: str, default: bool = False) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return str(v).strip().lower() in {"1", "true", "yes", "y", "on"}


async def _fetch_float(redis, key: str) -> Optional[float]:
    if redis is None:
        return None
    try:
        val = await redis.get(key)
        if val is None:
            return None
        if isinstance(val, bytes):
            val = val.decode()
        return float(val)
    except Exception:
        return None


async def _fetch_metrics(redis) -> Dict[str, Optional[float]]:
    """
    Fetch aggregate risk metrics from Redis.

    Keys:
      - metrics:winrate20
      - metrics:rolling_dd_pct
      - metrics:equity_hwm_usd
      - metrics:trades_total
      - metrics:wins_total
    """
    base = {
        "winrate20": None,
        "rolling_dd_pct": None,
        "equity_hwm_usd": None,
        "trades_total": None,
        "wins_total": None,
    }
    if redis is None:
        return base

    keys = [
        "metrics:winrate20",
        "metrics:rolling_dd_pct",
        "metrics:equity_hwm_usd",
        "metrics:trades_total",
        "metrics:wins_total",
    ]
    try:
        vals = await redis.mget(*keys)
    except Exception:
        vals = [None] * len(keys)

    out: Dict[str, Optional[float]] = {}
    for k, v in zip(keys, vals):
        short = k.split("metrics:")[-1]
        if v is None:
            out[short] = None
            continue
        if isinstance(v, bytes):
            v = v.decode()
        try:
            out[short] = float(v)
        except Exception:
            out[short] = None

    # Normalized keys
    return {
        "winrate20": out.get("winrate20"),
        "rolling_dd_pct": out.get("rolling_dd_pct"),
        "equity_hwm_usd": out.get("equity_hwm_usd"),
        "trades_total": out.get("trades_total"),
        "wins_total": out.get("wins_total"),
    }


async def _collect_snapshot(
    redis,
    days: int,
    include_live_risk: bool = True,
) -> Dict[str, Any]:
    """
    Build a structured snapshot of daily pnl + metrics for the last N days.

    Sources:
      • Redis daily PnL / budget keys.
      • Redis risk metrics (metrics:*).
      • Optional live risk/regime/AI summaries.
    """
    daily: List[Dict[str, Any]] = []
    days = max(1, min(days, 60))

    for i in range(days - 1, -1, -1):
        ds = _date_str(i)
        pnl_key = _daily_pnl_key(ds)
        bud_key = _daily_budget_key(ds)
        pnl = await _fetch_float(redis, pnl_key)
        budget = await _fetch_float(redis, bud_key)

        daily.append(
            {
                "date": ds,
                "pnl_usd": pnl,
                "budget_spent_usd": budget,
                "pnl_redis_key": pnl_key,
                "budget_redis_key": bud_key,
            }
        )

    metrics = await _fetch_metrics(redis)

    # Derive additional metrics if missing
    winrate20 = metrics.get("winrate20")
    rolling_dd = metrics.get("rolling_dd_pct")
    eq_hwm = metrics.get("equity_hwm_usd")
    trades_total = metrics.get("trades_total")
    wins_total = metrics.get("wins_total")

    if winrate20 is None and trades_total and wins_total is not None and trades_total > 0:
        try:
            winrate20 = float(wins_total) / float(trades_total)
        except Exception:
            winrate20 = None

    extras: Dict[str, Any] = {}

    # Live risk snapshot (portfolio-level)
    if include_live_risk and risk_manager is not None:
        try:
            snap = await risk_manager.risk_snapshot()  # type: ignore[func-returns-value]
            extras["risk_snapshot"] = snap
        except Exception as e:
            extras["risk_snapshot_error"] = str(e)

    # Live regime features (market state)
    if include_live_risk and regime_state is not None:
        try:
            feats = await regime_state.features()  # type: ignore[func-returns-value]
            extras["regime_features"] = feats
        except Exception as e:
            extras["regime_features_error"] = str(e)

    # Optional AI / strategy-day summary
    if include_live_risk and summarize_trading_day is not None:
        try:
            ai_day = await summarize_trading_day()  # type: ignore[func-returns-value]
            extras["ai_day_summary"] = ai_day
        except Exception as e:
            extras["ai_day_summary_error"] = str(e)

    snapshot: Dict[str, Any] = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "days": daily,
        "metrics": {
            "winrate_last_20": winrate20,
            "rolling_drawdown_pct": rolling_dd,
            "equity_hwm_usd": eq_hwm,
            "trades_total": trades_total,
            "wins_total": wins_total,
        },
        "extras": extras,
    }
    return snapshot


# ──────────────────────────────────────────────────────────────────────────────
# Printing
# ──────────────────────────────────────────────────────────────────────────────


def _print_snapshot(snapshot: Dict[str, Any]) -> None:
    print("=== 📈 PnL / Risk Snapshot (V3+++) ===")
    print(f"generated_at: {snapshot.get('generated_at')}")
    print("")

    days: List[Dict[str, Any]] = snapshot.get("days", []) or []

    # Daily table
    print("Daily PnL (USD) and budget spent:")
    print("  date       |   pnl_usd | budget_spent")
    print("  -----------+-----------+-------------")
    for row in days:
        ds = row.get("date", "")
        pnl = _fmt_money(row.get("pnl_usd"))
        bud = _fmt_money(row.get("budget_spent_usd"))
        print(f"  {ds} | {pnl} | {bud}")
    print("")

    # Aggregated metrics
    m = snapshot.get("metrics", {}) or {}
    winrate = m.get("winrate_last_20")
    dd = m.get("rolling_drawdown_pct")
    eq_hwm = m.get("equity_hwm_usd")
    trades = m.get("trades_total")
    wins = m.get("wins_total")

    print("Risk metrics (from risk_manager metrics:*):")
    # winrate20 stored as fraction; display as %
    wr_pct = winrate * 100 if winrate is not None else None
    dd_pct = dd * 100 if dd is not None else None
    print(f"  winrate_last_20        : {_fmt_pct(wr_pct)}")
    print(f"  rolling_drawdown_pct   : {_fmt_pct(dd_pct)}")
    print(f"  equity_high_water_mark : {_fmt_money(eq_hwm)}")
    if trades is not None or wins is not None:
        print(f"  trades_total / wins    : {int(trades or 0)} / {int(wins or 0)}")
    print("")

    # Extras (live risk / regime / AI)
    extras = snapshot.get("extras", {}) or {}

    risk_snap = extras.get("risk_snapshot")
    if isinstance(risk_snap, dict):
        print("Live risk snapshot:")
        print(
            f"  equity_usd      : {_fmt_money(risk_snap.get('equity_usd'))}  "
            f"(capacity={_fmt_pct((risk_snap.get('risk_capacity') or 0) * 100 if risk_snap.get('risk_capacity') is not None else None)})"
        )
        ticket = risk_snap.get("ticket_usd")
        max_ccy = risk_snap.get("max_concurrent")
        mode = risk_snap.get("risk_mode")
        print(f"  ticket_usd      : {_fmt_money(ticket)}")
        print(f"  max_concurrent  : {int(max_ccy or 0)}   | risk_mode={mode}")
        print("")

    regime_feats = extras.get("regime_features")
    if isinstance(regime_feats, dict):
        print("Regime features:")
        label = regime_feats.get("regime_label") or "?"
        mult = regime_feats.get("regime_risk_multiplier")
        print(f"  regime_label          : {label}")
        if mult is not None:
            print(f"  regime_risk_multiplier: {_fmt_pct((mult - 1.0) * 100 + 100 if isinstance(mult, (int, float)) else None)}")
        print("")

    ai_day = extras.get("ai_day_summary")
    if isinstance(ai_day, dict) and ai_day:
        print("AI / strategy day summary (from data_logger):")
        # Keep this generic since structure may evolve
        for k, v in ai_day.items():
            print(f"  {k}: {v}")
        print("")


# ──────────────────────────────────────────────────────────────────────────────
# CLI / entrypoints
# ──────────────────────────────────────────────────────────────────────────────


def build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Peek at daily PnL and risk metrics from Redis (V3+++)."
    )
    p.add_argument(
        "--days",
        type=int,
        default=_env_int("PNL_PEEK_DAYS", 7),
        help="How many days back to display (default: 7, max: 60).",
    )
    p.add_argument(
        "--json",
        action="store_true",
        help="Emit JSON snapshot instead of human-readable table.",
    )
    p.add_argument(
        "--watch",
        type=int,
        default=_env_int("PNL_PEEK_WATCH_SEC", 0),
        help="If >0, refresh every N seconds (live dashboard mode).",
    )
    p.add_argument(
        "--no-live-risk",
        action="store_true",
        help="Skip live risk/regime/AI extras (faster / lighter).",
    )
    return p


async def _run_once(args: argparse.Namespace) -> None:
    await cu.init_resources()
    try:
        redis = getattr(cu, "redis", None) or getattr(cu, "get_redis", lambda: None)()
        if redis is None:
            print("Redis client is not initialized. Check REDIS_URL / init_resources().")
            return

        snapshot = await _collect_snapshot(
            redis,
            args.days,
            include_live_risk=not args.no_live_risk,
        )
        if args.json:
            print(json.dumps(snapshot, indent=2, default=str))
        else:
            _print_snapshot(snapshot)
    finally:
        await cu.close_resources()


async def _run_watch(args: argparse.Namespace) -> None:
    """
    Watch mode: keep a single resource init and just refresh the snapshot.
    """
    interval = max(1, int(args.watch))

    await cu.init_resources()
    try:
        redis = getattr(cu, "redis", None) or getattr(cu, "get_redis", lambda: None)()
        if redis is None:
            print("Redis client is not initialized. Check REDIS_URL / init_resources().")
            return

        while True:
            # Clear the terminal for a dashboard-like feel
            os.system("cls" if os.name == "nt" else "clear")
            snapshot = await _collect_snapshot(
                redis,
                args.days,
                include_live_risk=not args.no_live_risk,
            )
            if args.json:
                print(json.dumps(snapshot, indent=2, default=str))
            else:
                _print_snapshot(snapshot)
            print(f"[pnl_peek] refreshing in {interval}s (Ctrl+C to stop)…")
            try:
                await asyncio.sleep(interval)
            except KeyboardInterrupt:
                raise
    except KeyboardInterrupt:
        print("\n[pnl_peek] stopped by user.")
    finally:
        await cu.close_resources()


def main() -> None:
    parser = build_arg_parser()
    args = parser.parse_args()

    if args.watch and args.watch > 0 and not args.json:
        asyncio.run(_run_watch(args))
    else:
        asyncio.run(_run_once(args))


if __name__ == "__main__":
    main()
