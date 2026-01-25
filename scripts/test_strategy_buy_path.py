# scripts/test_strategy_buy_path.py
from __future__ import annotations

"""
scripts/test_strategy_buy_path.py

God-Tier DRY-RUN “BUY PATH” sanity harness (P0/P1 bridge)
========================================================

Purpose
-------
A *safe*, *repeatable* micro-harness you can run on Render (or locally) to verify:
  1) Config loads + trading safety flags are visible
  2) PairFilters accept a “strong” candidate meta (no real RPC)
  3) AlertManager MOMO pipeline is callable (and won’t crash if Redis/TG missing)
  4) Router quote path is callable (and won’t crash if Jupiter/RPC unreachable)

What this script does NOT do
----------------------------
- Does NOT place trades
- Does NOT require wallet secrets
- Does NOT require Postgres
- Will *attempt* network quote calls only if enabled (default: enabled)

Run (Render shell)
-----------------
  cd /opt/render/project/src
  set -a
  . /etc/secrets/.env.extra
  set +a
  python -m scripts.test_strategy_buy_path

Run (local)
-----------
  python -m scripts.test_strategy_buy_path --no-router     # offline mode
  python -m scripts.test_strategy_buy_path --router-timeout 8

Exit codes
----------
- 0 = script finished (even if optional steps skipped)
- 2 = config/pair_filters critical failure (import/wiring)
"""

import argparse
import asyncio
import os
import time
from pprint import pprint
from typing import Any


# ──────────────────────────────────────────────────────────────────────────────
# Minimal logging helper (canonical root logging_setup.py)
# ──────────────────────────────────────────────────────────────────────────────
def _get_log():
    try:
        from logging_setup import get_logger  # canonical

        return get_logger()
    except Exception:
        import logging

        logging.basicConfig(level=logging.INFO)
        return logging.getLogger("test_strategy_buy_path")


log = _get_log()


# ──────────────────────────────────────────────────────────────────────────────
# Utilities
# ──────────────────────────────────────────────────────────────────────────────
def _truthy(v: Any) -> bool:
    if isinstance(v, bool):
        return v
    s = str(v).strip().lower()
    return s in ("1", "true", "yes", "on")


def _subset(d: dict | None, keys: tuple[str, ...]) -> dict | None:
    if not isinstance(d, dict):
        return None
    return {k: d.get(k) for k in keys}


def _safe_float(x: Any, default: float = 0.0) -> float:
    try:
        return float(x)
    except Exception:
        return default


async def _maybe_init_resources() -> dict[str, Any]:
    """
    Best-effort initialization for shared aiohttp session / redis.
    We do NOT fail the script if these aren’t available.
    """
    out: dict[str, Any] = {"did_init": False}
    try:
        from utils_pkg import core_utils as cu  # type: ignore

        init_fn = getattr(cu, "init_resources", None)
        if callable(init_fn):
            await init_fn()
            out["did_init"] = True

        # expose redis/session if present (for debugging prints)
        out["redis"] = getattr(cu, "redis", None)
        out["session"] = getattr(cu, "session", None)
        return out
    except Exception as e:
        out["error"] = repr(e)
        return out


async def _maybe_close_resources(did_init: bool) -> None:
    if not did_init:
        return
    try:
        from utils_pkg import core_utils as cu  # type: ignore

        close_fn = getattr(cu, "close_resources", None)
        if callable(close_fn):
            await close_fn()
    except Exception:
        pass


# ──────────────────────────────────────────────────────────────────────────────
# Fake meta builder (PairFilters expects a dict; field names vary across versions)
# ──────────────────────────────────────────────────────────────────────────────
def _build_strong_fake_meta(C: Any) -> dict[str, Any]:
    """
    Construct a “strong” candidate that should pass most reasonable thresholds.
    We include multiple alias keys so older/newer pair_filters can read it.
    """
    # Config fallbacks (don’t assume any exist)
    min_liq = _safe_float(getattr(C, "PF_SCAN_MIN_LIQ_USD", 400.0), 400.0)
    min_mcap = _safe_float(getattr(C, "PF_SCAN_MIN_MCAP_USD", 800.0), 800.0)
    min_vol5 = _safe_float(getattr(C, "PF_SCAN_MIN_5M_VOL_USD", 50.0), 50.0)
    min_buys5 = int(_safe_float(getattr(C, "PF_SCAN_MIN_5M_BUYS", 0), 0))
    min_whale = _safe_float(getattr(C, "PF_SCAN_MIN_WHALE_SIZE_SOL", 1.5), 1.5)

    liq_usd = max(1500.0, min_liq * 2.5)
    mcap_usd = max(20_000.0, min_mcap * 30.0)
    vol_5m = max(1500.0, min_vol5 * 30.0)
    buys_5m = max(25, min_buys5 + 20)

    fake = {
        # identity
        "mint": "FAKE_MINT_11111111111111111111111111111",
        "symbol": "FAKECOIN",
        "sym": "FAKECOIN",
        "dex": "raydium",
        "source": "debug-harness",
        # fundamentals
        "price_usd": 0.01,
        "liq_usd": liq_usd,
        "liq": liq_usd,  # alias some scanners use
        "mcap_usd": mcap_usd,
        "market_cap_usd": mcap_usd,
        "token_age_min": 45.0,
        "age_min": 45.0,  # alias
        # activity / flow
        "vol_5m_usd": vol_5m,
        "vol5": vol_5m,  # alias
        "vol_1h_usd": 3_000.0,
        "buys_5m": buys_5m,
        "buys5": buys_5m,  # alias
        "sells_5m": 5,
        "sells5": 5,  # alias
        "unique_buyers_5m": 20,
        # whales
        "whales_5m": 2,
        "whale_vol_5m_usd": 1_200.0,
        "largest_buy_5m_sol": max(4.0, min_whale + 2.0),
        # price action
        "price_change_5m_pct": 85.0,
        "chg5": 85.0,  # alias
        "price_change_15m_pct": 65.0,
        "price_change_1h_pct": 120.0,
        "chg60": 120.0,  # alias-ish
        # safety-ish
        "holders": 250,
        "holders_5m_change": 35,
        "is_honeypot": False,
        "honeypot": False,
        "trading_paused": False,
        "blacklisted": False,
        "ai_rug_score": 0.15,  # lower = safer in many versions
        "fraud_score": 0.15,  # alias
        "top10_hold_pct": 60.0,
        "dev_hold_pct": 12.0,
        "mint_authority_disabled": True,
        "freeze_authority_disabled": True,
        # debug timestamp
        "ts": int(time.time()),
    }
    return fake


# ──────────────────────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────────────────────
async def main() -> int:
    ap = argparse.ArgumentParser(description="Dry-run buy path harness (no trading).")
    ap.add_argument("--no-pairfilters", action="store_true", help="Skip pair_filters checks.")
    ap.add_argument("--no-alert", action="store_true", help="Skip AlertManager MOMO event.")
    ap.add_argument("--no-router", action="store_true", help="Skip router quotes (offline mode).")
    ap.add_argument(
        "--amount-sol", type=float, default=0.01, help="SOL amount for buy-quote sanity."
    )
    ap.add_argument(
        "--sell-raw",
        type=int,
        default=1_000_000,
        help="Raw token amount for sell-quote sanity (e.g., 1 USDC = 1_000_000).",
    )
    ap.add_argument("--slippage-bps", type=int, default=200, help="Slippage bps for quote sanity.")
    ap.add_argument(
        "--router-timeout", type=float, default=10.0, help="Timeout seconds for each router call."
    )
    args = ap.parse_args()

    print("=== TEST: Strategy-ish BUY path (DRY-RUN) ===")

    # Best-effort init (aiohttp session / redis)
    res = await _maybe_init_resources()
    if res.get("did_init"):
        print("Resources: init_resources() ✅")
    else:
        print("Resources: init_resources() skipped (ok)")

    try:
        # ------------------------------------------------------------------
        # 0) Imports + config sanity
        # ------------------------------------------------------------------
        try:
            import config as C  # type: ignore
        except Exception as e:
            print("\n❌ CRITICAL: could not import config:", repr(e))
            return 2

        dryrun = _truthy(getattr(C, "DRYRUN", os.getenv("DRYRUN", "1")))
        pause = _truthy(getattr(C, "PAUSE_TRADING", os.getenv("PAUSE_TRADING", "0")))
        auto_buy = getattr(C, "AUTO_BUY_ENABLED", None)

        print("Config flags:")
        print("  DRYRUN          =", dryrun)
        print("  PAUSE_TRADING   =", pause)
        print("  AUTO_BUY_ENABLED=", auto_buy)

        # Hard safety note (this is a script, so we just warn)
        if not dryrun and not pause:
            print("\n⚠️  WARNING: DRYRUN=0 and PAUSE_TRADING=0 detected.")
            print("    This script still does NOT place trades, but your environment is 'live'.\n")

        # ------------------------------------------------------------------
        # 1) PairFilters acceptance check
        # ------------------------------------------------------------------
        fake_meta = _build_strong_fake_meta(C)

        mom = final = ok = None
        dbg: dict[str, Any] = {}

        if not args.no_pairfilters:
            try:
                from utils_pkg import pair_filters as PF  # type: ignore

                # compute scores if available
                if hasattr(PF, "compute_momentum_score"):
                    mom = PF.compute_momentum_score(fake_meta)  # type: ignore
                if hasattr(PF, "compute_final_score"):
                    final = PF.compute_final_score(fake_meta)  # type: ignore
                if hasattr(PF, "is_viable_pair"):
                    ok = PF.is_viable_pair(fake_meta)  # type: ignore
                else:
                    raise RuntimeError("pair_filters missing is_viable_pair(meta)")

                dbg = fake_meta.get("_filter_debug", {}) or {}

                print("\n>>> PairFilters decision:")
                print("  OK      :", ok)
                print("  momentum:", mom)
                print("  final   :", final)
                print("  debug   :")
                pprint(dbg)

                if not ok:
                    print("\n❌ CRITICAL: PairFilters rejected strong fake meta.")
                    print("   Fix thresholds or PairFilters logic before going live.")
                    return 2
            except Exception as e:
                print("\n❌ CRITICAL: PairFilters check failed:", repr(e))
                return 2
        else:
            print("\n>>> PairFilters skipped (--no-pairfilters).")

        # ------------------------------------------------------------------
        # 2) AlertManager MOMO path (best-effort)
        # ------------------------------------------------------------------
        if args.no_alert:
            print("\n>>> AlertManager skipped (--no-alert).")
        else:
            try:
                from utils_pkg.alert_manager import AlertManager  # type: ignore

                am = AlertManager()
                ctx = {
                    "symbol": fake_meta["symbol"],
                    "mint": fake_meta["mint"],
                    "pool": "debug-raydium-fake-pool",
                    "age_min": fake_meta.get("token_age_min") or fake_meta.get("age_min"),
                    "liq_usd": fake_meta["liq_usd"],
                    "vol_5m_usd": fake_meta.get("vol_5m_usd") or fake_meta.get("vol5"),
                    "up_pct": fake_meta.get("price_change_5m_pct") or fake_meta.get("chg5"),
                    "fraud_score": fake_meta.get("ai_rug_score") or fake_meta.get("fraud_score"),
                    "momentum": None if mom is None else round(float(mom), 3),
                    "final": None if final is None else round(float(final), 3),
                    "reason": "DRYRUN harness: pair_filters accepted strong meta",
                    "source": "scripts.test_strategy_buy_path",
                }

                print("\n>>> Sending MOMO_OK via AlertManager (may be rate-limited / cooldown)…")
                # contract: send_momo_event(event_name, ctx)
                if hasattr(am, "send_momo_event") and callable(am.send_momo_event):
                    await am.send_momo_event("MOMO_OK", ctx)  # type: ignore
                    print(
                        "AlertManager: send_momo_event ✅ (if TG/Redis configured, you should see 1 ping)"
                    )
                else:
                    print(
                        "AlertManager: send_momo_event not found (skipped; update AlertManager contract)"
                    )
            except Exception as e:
                print("\n⚠️  AlertManager step failed (non-critical):", repr(e))
                print(
                    "    This does NOT block trading, but you should fix alert wiring for observability."
                )

        # ------------------------------------------------------------------
        # 3) Router quote sanity (best-effort; network)
        # ------------------------------------------------------------------
        if args.no_router:
            print("\n>>> Router quotes skipped (--no-router).")
        else:
            try:
                from utils_pkg import router  # type: ignore

                USDC = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"

                print("\n>>> Router sanity quotes (may be None; should not crash):")

                async def _q_buy():
                    return await router.get_buy_quote(  # type: ignore
                        mint=USDC,
                        amount_sol=float(args.amount_sol),
                        slippage_bps=int(args.slippage_bps),
                    )

                async def _q_sell():
                    return await router.get_sell_quote(  # type: ignore
                        mint=USDC,
                        amount_raw=int(args.sell_raw),
                        slippage_bps=int(args.slippage_bps),
                    )

                try:
                    q_buy = await asyncio.wait_for(_q_buy(), timeout=float(args.router_timeout))
                except Exception as e:
                    q_buy = None
                    print("BUY quote failed (non-critical):", repr(e))

                print(
                    "BUY quote (SOL->USDC), subset:",
                    _subset(
                        q_buy,
                        (
                            "in_amount",
                            "out_amount",
                            "price_impact_bps",
                            "route_source",
                            "dex",
                            "slippage_bps",
                        ),
                    ),
                )

                try:
                    q_sell = await asyncio.wait_for(_q_sell(), timeout=float(args.router_timeout))
                except Exception as e:
                    q_sell = None
                    print("SELL quote failed (non-critical):", repr(e))

                print(
                    "SELL quote (USDC->SOL), subset:",
                    _subset(
                        q_sell,
                        (
                            "in_amount",
                            "out_amount",
                            "price_impact_bps",
                            "route_source",
                            "dex",
                            "slippage_bps",
                        ),
                    ),
                )

                print("Router: quote path callable ✅ (even if routes were None)")
            except Exception as e:
                print("\n⚠️  Router step failed (non-critical):", repr(e))
                print(
                    "    If this fails in production, you’ll see no fills. Fix router/RPC wiring."
                )

        print(
            "\n✅ BUY-path DRY-RUN micro test finished.\n"
            "   • config imports OK\n"
            "   • pair_filters checked (unless skipped)\n"
            "   • AlertManager attempted (unless skipped)\n"
            "   • router quotes attempted (unless skipped)\n"
        )
        return 0

    finally:
        await _maybe_close_resources(bool(res.get("did_init")))


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
