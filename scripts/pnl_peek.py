#!/usr/bin/env python3
from __future__ import annotations

"""
scripts/pnl_peek.py — Quant-grade PnL / Risk Snapshot (V3+++ / Phase 0–7 LOCK-IN)
===============================================================================

Purpose
-------
Fast, safe visibility into "why aren’t we trading / how are we doing":

Primary (Redis, risk_manager-compatible keys)
- Daily realized PnL (USD):
    pnl:day:realized_usd:YYYYMMDD
- Daily spend / budget used (USD), if you track it:
    budget:sniper:spent_usd:YYYYMMDD
- Aggregate risk metrics (if present):
    metrics:winrate20
    metrics:rolling_dd_pct
    metrics:equity_hwm_usd
    metrics:trades_total
    metrics:wins_total

Secondary (best-effort, optional)
- utils_pkg.risk_manager.risk_snapshot()
- utils_pkg.regime.regime_state.features()
- utils_pkg.data_logger.summarize_trading_day()

Hard rules
----------
- Never requires optional modules; degrades gracefully.
- Human output goes to STDERR (for dashboards/log tails), JSON goes to STDOUT.
- Safe redaction of token-ish strings and env-backed secrets in errors.

Usage
-----
python -m scripts.pnl_peek
python -m scripts.pnl_peek --days 14
python -m scripts.pnl_peek --json
python -m scripts.pnl_peek --watch 10
python -m scripts.pnl_peek --watch 10 --json   (NDJSON stream)
"""

import argparse
import asyncio
import contextlib
import json
import os
import platform
import re
import sys
import time
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

# ──────────────────────────────────────────────────────────────
# Repo root / .env bootstrap (never required)
# ──────────────────────────────────────────────────────────────

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

with contextlib.suppress(Exception):
    from dotenv import find_dotenv, load_dotenv  # type: ignore

    load_dotenv(find_dotenv(), override=False)

# ──────────────────────────────────────────────────────────────
# Logging (canonical root logging_setup.py)
# ──────────────────────────────────────────────────────────────


def _get_logger():
    try:
        from logging_setup import get_logger  # type: ignore

        return get_logger()
    except Exception:
        import logging

        return logging.getLogger("pnl_peek")


logger = _get_logger()

# ──────────────────────────────────────────────────────────────
# Redaction / safety
# ──────────────────────────────────────────────────────────────

_URL_CRED_RE = re.compile(r"(://)([^/@\s]+)@")
_LONG_TOKENISH_RE = re.compile(r"\b[A-Za-z0-9_\-]{28,}\b")


def _envs(k: str, d: str = "") -> str:
    v = os.getenv(k)
    return (v if v is not None else d).strip()


_TRUE = {"1", "true", "yes", "y", "on", "t"}
_FALSE = {"0", "false", "no", "n", "off", "f"}


def _envb(k: str, d: bool = False) -> bool:
    v = _envs(k, "")
    if v == "":
        return bool(d)
    s = v.lower().strip()
    if s in _TRUE:
        return True
    if s in _FALSE:
        return False
    return bool(d)


def _envi(k: str, d: int) -> int:
    try:
        return int(float(_envs(k, str(d))))
    except Exception:
        return int(d)


def _sanitize_text(x: Any) -> str:
    if x is None:
        return ""
    s = str(x)
    s = _URL_CRED_RE.sub(r"\1***@", s)

    sensitive_keys = [
        "OPENAI_API_KEY",
        "TELEGRAM_BOT_TOKEN",
        "DISCORD_TOKEN",
        "WALLET_PRIVATE_KEY",
        "KEYPAIR_B64",
        "KEYPAIR_PATH",
        "DATABASE_URL",
        "PG_DSN",
        "PG_DSN_INTERNAL",
        "PG_DSN_EXTERNAL",
        "POSTGRES_DSN",
        "REDIS_URL",
        "REDIS_TLS_URL",
        "RPC_URL",
        "SOL_RPC_URL",
        "JUP_API_KEY",
        "JUP_PRO_KEY",
    ]
    for k in sensitive_keys:
        v = os.getenv(k, "")
        if v and len(v) >= 6:
            s = s.replace(v, "***")

    if _envb("PNL_PEEK_MASK_LONG_TOKENS", True):
        s = _LONG_TOKENISH_RE.sub("***", s)

    return s


def _ts() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def _hlog(msg: str, *, quiet: bool) -> None:
    if quiet:
        return
    try:
        sys.stderr.write(f"[pnl_peek] {_ts()} {msg}\n")
        sys.stderr.flush()
    except Exception:
        pass


# ──────────────────────────────────────────────────────────────
# Optional modules (best-effort)
# ──────────────────────────────────────────────────────────────

# Core infra: Redis init/close
import utils_pkg.core_utils as cu  # type: ignore

risk_manager = None
with contextlib.suppress(Exception):
    from utils_pkg import risk_manager as _rm  # type: ignore

    risk_manager = _rm

regime_state = None
with contextlib.suppress(Exception):
    from utils_pkg.regime import regime_state as _rs  # type: ignore

    regime_state = _rs

summarize_trading_day = None
with contextlib.suppress(Exception):
    from utils_pkg.data_logger import summarize_trading_day as _std  # type: ignore

    summarize_trading_day = _std


# ──────────────────────────────────────────────────────────────
# Redis key contract (LOCK-IN)
# ──────────────────────────────────────────────────────────────


def _date_str(days_ago: int) -> str:
    return (datetime.now(UTC).date() - timedelta(days=days_ago)).strftime("%Y%m%d")


def _daily_pnl_key(date_str: str) -> str:
    return f"pnl:day:realized_usd:{date_str}"


def _daily_budget_key(date_str: str) -> str:
    return f"budget:sniper:spent_usd:{date_str}"


_METRIC_KEYS = {
    "winrate20": "metrics:winrate20",
    "rolling_dd_pct": "metrics:rolling_dd_pct",
    "equity_hwm_usd": "metrics:equity_hwm_usd",
    "trades_total": "metrics:trades_total",
    "wins_total": "metrics:wins_total",
}


# ──────────────────────────────────────────────────────────────
# Data helpers
# ──────────────────────────────────────────────────────────────


async def _maybe_await(x: Any) -> Any:
    return await x if asyncio.iscoroutine(x) else x


async def _get_redis() -> Any:
    """
    Prefer an async redis getter if your core_utils provides it; fall back to cu.redis.
    """
    # Common patterns:
    # - await cu.get_redis_async()
    # - await cu.get_redis()
    # - cu.redis already set after init_resources()
    for nm in ("get_redis_async", "get_redis"):
        fn = getattr(cu, nm, None)
        if callable(fn):
            with contextlib.suppress(Exception):
                r = await _maybe_await(fn())
                if r is not None:
                    return r
    return getattr(cu, "redis", None)


async def _fetch_str(redis: Any, key: str) -> str | None:
    if redis is None:
        return None
    try:
        v = await redis.get(key)
        if v is None:
            return None
        if isinstance(v, bytes):
            v = v.decode("utf-8", "ignore")
        s = str(v).strip()
        return s if s != "" else None
    except Exception:
        return None


async def _fetch_float(redis: Any, key: str) -> float | None:
    s = await _fetch_str(redis, key)
    if s is None:
        return None
    try:
        return float(s)
    except Exception:
        return None


def _as_fraction(x: float | None) -> float | None:
    """
    Normalize value that might be:
      - fraction (0.12)
      - percent (12)
    If abs(x) > 1.0, assume percent and divide by 100.
    """
    if x is None:
        return None
    try:
        v = float(x)
    except Exception:
        return None
    if abs(v) > 1.0:
        v = v / 100.0
    return v


def _fmt_money(v: float | None) -> str:
    if v is None:
        return "   ??.??"
    return f"{v:8.2f}"


def _fmt_pct(v: float | None) -> str:
    """
    Input v as fraction (0.12 => 12.00%).
    """
    if v is None:
        return " ??.??%"
    return f"{(v * 100.0):6.2f}%"


async def _fetch_metrics(redis: Any) -> dict[str, float | None]:
    out: dict[str, float | None] = {k: None for k in _METRIC_KEYS}
    if redis is None:
        return out

    keys = list(_METRIC_KEYS.values())
    vals: list[Any]
    try:
        vals = await redis.mget(*keys)
    except Exception:
        vals = [None] * len(keys)

    for name, raw in zip(_METRIC_KEYS.keys(), vals, strict=False):
        if raw is None:
            out[name] = None
            continue
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8", "ignore")
        try:
            out[name] = float(str(raw).strip())
        except Exception:
            out[name] = None
    return out


def _sum(values: list[float | None]) -> float | None:
    s = 0.0
    seen = False
    for v in values:
        if v is None:
            continue
        seen = True
        s += float(v)
    return s if seen else None


# ──────────────────────────────────────────────────────────────
# Snapshot builder
# ──────────────────────────────────────────────────────────────


@dataclass
class PeekConfig:
    days: int
    include_extras: bool
    include_key_names: bool
    strict: bool


async def _collect_snapshot(redis: Any, cfg: PeekConfig) -> dict[str, Any]:
    days = int(max(1, min(60, cfg.days)))

    daily: list[dict[str, Any]] = []
    for i in range(days - 1, -1, -1):
        ds = _date_str(i)
        k_pnl = _daily_pnl_key(ds)
        k_bud = _daily_budget_key(ds)
        pnl = await _fetch_float(redis, k_pnl)
        bud = await _fetch_float(redis, k_bud)

        row: dict[str, Any] = {"date": ds, "pnl_usd": pnl, "budget_spent_usd": bud}
        if cfg.include_key_names:
            row["pnl_redis_key"] = k_pnl
            row["budget_redis_key"] = k_bud
        daily.append(row)

    metrics_raw = await _fetch_metrics(redis)
    winrate = _as_fraction(metrics_raw.get("winrate20"))
    rolling_dd = _as_fraction(metrics_raw.get("rolling_dd_pct"))
    eq_hwm = metrics_raw.get("equity_hwm_usd")
    trades_total = metrics_raw.get("trades_total")
    wins_total = metrics_raw.get("wins_total")

    # If winrate missing, derive from totals (if meaningful)
    if winrate is None and trades_total is not None and wins_total is not None and trades_total > 0:
        with contextlib.suppress(Exception):
            winrate = float(wins_total) / float(trades_total)

    # Totals
    pnl_total = _sum([r.get("pnl_usd") for r in daily])
    bud_total = _sum([r.get("budget_spent_usd") for r in daily])
    net_total = None
    if pnl_total is not None or bud_total is not None:
        net_total = float(pnl_total or 0.0) - float(bud_total or 0.0)

    extras: dict[str, Any] = {}
    if cfg.include_extras:
        # Live risk snapshot
        if risk_manager is not None and callable(getattr(risk_manager, "risk_snapshot", None)):
            try:
                snap = risk_manager.risk_snapshot()  # type: ignore
                snap = await _maybe_await(snap)
                extras["risk_snapshot"] = snap
            except Exception as e:
                extras["risk_snapshot_error"] = _sanitize_text(e)

        # Regime features
        if regime_state is not None and callable(getattr(regime_state, "features", None)):
            try:
                feats = regime_state.features()  # type: ignore
                feats = await _maybe_await(feats)
                extras["regime_features"] = feats
            except Exception as e:
                extras["regime_features_error"] = _sanitize_text(e)

        # AI / strategy day summary
        if summarize_trading_day is not None and callable(summarize_trading_day):
            try:
                day = summarize_trading_day()  # type: ignore
                day = await _maybe_await(day)
                extras["ai_day_summary"] = day
            except Exception as e:
                extras["ai_day_summary_error"] = _sanitize_text(e)

    snapshot: dict[str, Any] = {
        "ok": True,
        "generated_at": datetime.now(UTC).isoformat(),
        "host": _sanitize_text(platform.node()),
        "python": sys.version.split()[0],
        "days_requested": int(cfg.days),
        "days_returned": int(days),
        "daily": daily,
        "totals": {
            "pnl_usd": pnl_total,
            "budget_spent_usd": bud_total,
            "net_usd": net_total,
        },
        "metrics": {
            "winrate_last_20": winrate,
            "rolling_drawdown": rolling_dd,
            "equity_hwm_usd": eq_hwm,
            "trades_total": trades_total,
            "wins_total": wins_total,
        },
        "extras": extras,
    }

    # Strict mode: fail if redis missing (or everything empty)
    if cfg.strict:
        any_pnl = any((r.get("pnl_usd") is not None) for r in daily)
        any_bud = any((r.get("budget_spent_usd") is not None) for r in daily)
        if not any_pnl and not any_bud and all(v is None for v in metrics_raw.values()):
            snapshot["ok"] = False
            snapshot["error"] = (
                "no pnl/budget/metrics keys found (check risk_manager wiring + redis db)"
            )
    return snapshot


# ──────────────────────────────────────────────────────────────
# Rendering
# ──────────────────────────────────────────────────────────────


def _print_human(snapshot: dict[str, Any]) -> None:
    # Print to STDERR so you can `| tee` while still machine-parse JSON from STDOUT if desired.
    out = sys.stderr

    def w(s: str = "") -> None:
        out.write(s + "\n")

    w("=== 📈 PnL / Risk Snapshot (V3+++) ===")
    w(f"generated_at: {snapshot.get('generated_at')}")
    w("")

    daily: list[dict[str, Any]] = snapshot.get("daily", []) or []
    w("Daily PnL (USD) and budget spent:")
    w("  date       |   pnl_usd | budget_spent |    net")
    w("  -----------+-----------+--------------+-----------")
    for row in daily:
        ds = str(row.get("date", ""))
        pnl = row.get("pnl_usd")
        bud = row.get("budget_spent_usd")
        net = (
            (float(pnl or 0.0) - float(bud or 0.0))
            if (pnl is not None or bud is not None)
            else None
        )
        w(f"  {ds} | {_fmt_money(pnl)} | {_fmt_money(bud)} | {_fmt_money(net)}")

    totals = snapshot.get("totals", {}) or {}
    w("  -----------+-----------+--------------+-----------")
    w(
        f"  TOTAL      | {_fmt_money(totals.get('pnl_usd'))} | {_fmt_money(totals.get('budget_spent_usd'))} | {_fmt_money(totals.get('net_usd'))}"
    )
    w("")

    m = snapshot.get("metrics", {}) or {}
    w("Risk metrics (Redis metrics:*):")
    w(f"  winrate_last_20        : {_fmt_pct(m.get('winrate_last_20'))}")
    w(f"  rolling_drawdown       : {_fmt_pct(m.get('rolling_drawdown'))}")
    w(f"  equity_high_water_mark : {_fmt_money(m.get('equity_hwm_usd'))}")
    if m.get("trades_total") is not None or m.get("wins_total") is not None:
        w(
            f"  trades_total / wins    : {int(m.get('trades_total') or 0)} / {int(m.get('wins_total') or 0)}"
        )
    w("")

    extras = snapshot.get("extras", {}) or {}

    rs = extras.get("risk_snapshot")
    if isinstance(rs, dict):
        w("Live risk snapshot:")
        # Keep generic (structure may evolve)
        for k in (
            "risk_mode",
            "equity_usd",
            "risk_capacity",
            "ticket_usd",
            "max_concurrent",
            "halted",
        ):
            if k in rs:
                v = rs.get(k)
                if k in ("risk_capacity",):
                    v = _fmt_pct(_as_fraction(v))
                elif k.endswith("_usd"):
                    v = _fmt_money(v if isinstance(v, int | float) else None)
                w(f"  {k:14s}: {v}")
        w("")

    rg = extras.get("regime_features")
    if isinstance(rg, dict):
        w("Regime features:")
        for k in ("regime_label", "regime_risk_multiplier", "vol_regime", "momo_regime"):
            if k in rg:
                w(f"  {k:22s}: {rg.get(k)}")
        w("")

    ai = extras.get("ai_day_summary")
    if isinstance(ai, dict) and ai:
        w("AI / strategy day summary:")
        for k, v in ai.items():
            w(f"  {k}: {v}")
        w("")

    # Errors if any
    if not snapshot.get("ok", True):
        w("⚠️  snapshot not OK:")
        w(f"  error: {snapshot.get('error')}")


def _json_dump(snapshot: dict[str, Any], *, pretty: bool) -> str:
    if pretty:
        return json.dumps(snapshot, indent=2, ensure_ascii=False, default=str)
    return json.dumps(snapshot, ensure_ascii=False, default=str)


# ──────────────────────────────────────────────────────────────
# CLI / run loop
# ──────────────────────────────────────────────────────────────


def build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Peek at PnL / risk metrics from Redis (V3+++).")
    p.add_argument(
        "--days",
        type=int,
        default=_envi("PNL_PEEK_DAYS", 7),
        help="Days back to display (default 7, max 60).",
    )
    p.add_argument(
        "--json",
        action="store_true",
        default=_envb("PNL_PEEK_JSON", False),
        help="Emit JSON (to STDOUT).",
    )
    p.add_argument(
        "--json-pretty",
        action="store_true",
        default=_envb("PNL_PEEK_JSON_PRETTY", False),
        help="Pretty JSON (slower).",
    )
    p.add_argument(
        "--json-only",
        action="store_true",
        default=_envb("PNL_PEEK_JSON_ONLY", False),
        help="Suppress human output.",
    )
    p.add_argument(
        "--watch",
        type=int,
        default=_envi("PNL_PEEK_WATCH_SEC", 0),
        help="If >0, refresh every N seconds.",
    )
    p.add_argument(
        "--no-extras",
        action="store_true",
        default=_envb("PNL_PEEK_NO_EXTRAS", False),
        help="Skip live risk/regime/AI extras.",
    )
    p.add_argument(
        "--include-keys",
        action="store_true",
        default=_envb("PNL_PEEK_INCLUDE_KEYS", False),
        help="Include redis key names in output JSON.",
    )
    p.add_argument(
        "--strict",
        action="store_true",
        default=_envb("PNL_PEEK_STRICT", False),
        help="Fail if no keys found.",
    )
    p.add_argument(
        "--clear",
        action="store_true",
        default=_envb("PNL_PEEK_CLEAR", False),
        help="Clear screen each refresh (watch mode).",
    )
    return p


async def _run_once(args: argparse.Namespace) -> int:
    quiet = bool(args.json_only)
    cfg = PeekConfig(
        days=int(args.days),
        include_extras=not bool(args.no_extras),
        include_key_names=bool(args.include_keys),
        strict=bool(args.strict),
    )

    await cu.init_resources()
    try:
        redis = await _get_redis()
        if redis is None:
            snap = {
                "ok": False,
                "generated_at": datetime.now(UTC).isoformat(),
                "error": "redis not initialized (check REDIS_URL / init_resources)",
            }
            if args.json:
                print(_json_dump(snap, pretty=bool(args.json_pretty)))
            if not quiet:
                _hlog("FAIL redis not initialized", quiet=False)
                _print_human(snap)
            return 2

        snap = await _collect_snapshot(redis, cfg)

        if args.json:
            print(_json_dump(snap, pretty=bool(args.json_pretty)))

        if not quiet:
            _print_human(snap)

        return 0 if snap.get("ok", True) else 2
    finally:
        await cu.close_resources()


async def _run_watch(args: argparse.Namespace) -> int:
    quiet = bool(args.json_only)
    interval = max(1, int(args.watch))

    cfg = PeekConfig(
        days=int(args.days),
        include_extras=not bool(args.no_extras),
        include_key_names=bool(args.include_keys),
        strict=bool(args.strict),
    )

    await cu.init_resources()
    try:
        redis = await _get_redis()
        if redis is None:
            snap = {
                "ok": False,
                "generated_at": datetime.now(UTC).isoformat(),
                "error": "redis not initialized (check REDIS_URL / init_resources)",
            }
            if args.json:
                print(_json_dump(snap, pretty=bool(args.json_pretty)))
            if not quiet:
                _print_human(snap)
            return 2

        while True:
            if bool(args.clear):
                os.system("cls" if os.name == "nt" else "clear")

            snap = await _collect_snapshot(redis, cfg)

            # JSON watch mode: NDJSON (one snapshot per line) unless --json-pretty
            if args.json:
                print(_json_dump(snap, pretty=bool(args.json_pretty)))
            if not quiet and not args.json:
                _print_human(snap)

            if not quiet and not args.json:
                _hlog(f"refreshing in {interval}s (Ctrl+C to stop)", quiet=False)

            await asyncio.sleep(interval)
    except KeyboardInterrupt:
        if not quiet:
            _hlog("stopped by user", quiet=False)
        return 0
    finally:
        await cu.close_resources()


def main() -> None:
    args = build_arg_parser().parse_args()

    # Windows-friendly loop policy
    with contextlib.suppress(Exception):
        if os.name == "nt" and hasattr(asyncio, "WindowsSelectorEventLoopPolicy"):
            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())  # type: ignore[attr-defined]

    if args.watch and int(args.watch) > 0:
        rc = asyncio.run(_run_watch(args))
    else:
        rc = asyncio.run(_run_once(args))
    raise SystemExit(int(rc))


if __name__ == "__main__":
    main()
