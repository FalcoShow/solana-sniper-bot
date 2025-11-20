# scripts/test_strategy_buy_path.py
from __future__ import annotations

"""
Dry-run sanity check for the sniper stack.

What this test proves:
  - config + DRYRUN flags are loaded
  - pair_filters will ACCEPT a strong meta (momentum/final above thresholds)
  - AlertManager MOMO pipeline works (and respects Redis cooldown)
  - Router quote path is callable for a simple SOL -> USDC style route

Run on Render shell with:

  cd /opt/render/project/src
  set -a
  . /etc/secrets/.env.extra
  set +a
  python -m scripts.test_strategy_buy_path
"""

import asyncio
from pprint import pprint

from logging_setup import get_logger

log = get_logger()


async def main() -> None:
    import config as C
    from utils_pkg import pair_filters as PF
    from utils_pkg.alert_manager import AlertManager
    from utils_pkg import router

    print("=== TEST: Strategy-ish BUY path (DRYRUN) ===")
    print(
        "DRY_RUN=", C.DRYRUN,
        "AUTO_BUY_ENABLED=", getattr(C, "AUTO_BUY_ENABLED", None),
        "PAUSE_TRADING=", getattr(C, "PAUSE_TRADING", None),
    )

    # ------------------------------------------------------------------
    # 1) Build a strong fake meta that should be clearly "good"
    # ------------------------------------------------------------------
    fake_meta = {
        "mint": "FAKE_MINT_11111111111111111111111111111",
        "symbol": "FAKECOIN",
        "dex": "raydium",

        # size / fundamentals
        "price_usd": 0.01,
        "liq_usd": max(1500.0, getattr(C, "PF_SCAN_MIN_LIQ_USD", 400.0) * 2),
        "mcap_usd": max(20_000.0, getattr(C, "PF_SCAN_MIN_MCAP_USD", 800.0) * 30),
        "token_age_min": 45.0,

        # recent activity
        "vol_5m_usd": max(1500.0, getattr(C, "PF_SCAN_MIN_5M_VOL_USD", 50.0) * 30),
        "vol_1h_usd": 3_000.0,
        "buys_5m": max(25, getattr(C, "PF_SCAN_MIN_5M_BUYS", 0) + 20),
        "sells_5m": 5,
        "unique_buyers_5m": 20,

        # whales
        "whales_5m": 2,
        "whale_vol_5m_usd": 1_200.0,
        "largest_buy_5m_sol": max(4.0, getattr(C, "PF_SCAN_MIN_WHALE_SIZE_SOL", 1.5) + 2),

        # price action
        "price_change_5m_pct": 85.0,
        "price_change_15m_pct": 65.0,
        "price_change_1h_pct": 120.0,

        # holders / safety
        "holders": 250,
        "holders_5m_change": 35,
        "is_honeypot": False,
        "trading_paused": False,
        "blacklisted": False,
        "ai_rug_score": 0.15,  # nice and low risk
        "top10_hold_pct": 60.0,
        "dev_hold_pct": 12.0,
        "mint_authority_disabled": True,
        "freeze_authority_disabled": True,
    }

    mom = PF.compute_momentum_score(fake_meta)
    final = PF.compute_final_score(fake_meta)
    ok = PF.is_viable_pair(fake_meta)
    dbg = fake_meta.get("_filter_debug", {})

    log.info("FAKE META", extra={"momentum": mom, "final": final})
    log.info(
        "PAIR_FILTER",
        extra={"ok": ok, "reasons": dbg.get("reasons", []), "momentum": mom, "final": final},
    )

    print("\n>>> PairFilter decision:")
    print("OK:", ok)
    print("momentum:", mom)
    print("final   :", final)
    print("debug   :")
    pprint(dbg)

    if not ok:
        print("\n❌ PairFilters REJECTED the fake meta – check thresholds in config.")
        return

    # ------------------------------------------------------------------
    # 2) Fire a MOMO_OK alert for this meta (tests Redis cooldown + TG)
    # ------------------------------------------------------------------
    am = AlertManager()

    ctx = {
        "symbol": fake_meta["symbol"],
        "mint": fake_meta["mint"],
        "pool": "debug-raydium-fake-pool",
        "age_min": fake_meta["token_age_min"],
        "liq_usd": fake_meta["liq_usd"],
        "vol_5m_usd": fake_meta["vol_5m_usd"],
        "up_pct": fake_meta["price_change_5m_pct"],
        "fraud_score": fake_meta["ai_rug_score"],
        "momentum": round(mom, 3),
        "final": round(final, 3),
        "reason": "PF runner final=%.3f momentum=%.3f" % (final, mom),
    }

    print("\n>>> Sending MOMO_OK via AlertManager (should ping Telegram once)…")
    await am.send_momo_event("MOMO_OK", ctx)

    # ------------------------------------------------------------------
    # 3) Sanity Jupiter/router quote, just to ensure router stack is live
    # ------------------------------------------------------------------
    SOL = "So11111111111111111111111111111111111111112"
    USDC = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"

    print("\n>>> Router sanity quotes (this may be None if no route, that's okay):")
    q_buy = await router.get_buy_quote(
        mint=USDC,
        amount_sol=0.01,
        slippage_bps=200,
    )
    print("BUY quote (SOL->USDC), subset:", q_buy and {
        k: q_buy.get(k) for k in ("in_amount", "out_amount", "price_impact_bps", "route_source")
    })

    q_sell = await router.get_sell_quote(
        mint=USDC,
        amount_raw=1_000_000,  # 1 USDC
        slippage_bps=200,
    )
    print("SELL quote (USDC->SOL), subset:", q_sell and {
        k: q_sell.get(k) for k in ("in_amount", "out_amount", "price_impact_bps", "route_source")
    })

    print("\n✅ BUY-path dry-run micro test finished.\n"
          "   • pair_filters accepted fake runner\n"
          "   • MOMO_OK alert sent (subject to Redis cooldown)\n"
          "   • router quote path callable\n")


if __name__ == "__main__":
    asyncio.run(main())
