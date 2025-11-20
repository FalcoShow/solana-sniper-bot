from __future__ import annotations

import asyncio
import time
from types import SimpleNamespace

import config as C
from utils_pkg import pair_filters as PF
from utils_pkg import router
from logging_setup import get_logger

# IMPORTANT: adjust this import if your Strategy class lives elsewhere
from scripts.main import Strategy  # noqa: E402

log = get_logger()


def _make_fake_meta() -> dict:
    """
    Build an overpowered meta that should absolutely pass all PF + momentum
    filters and look like a very strong candidate.
    """
    now = time.time()
    return {
        "mint": "FakeMint11111111111111111111111111111111111",
        "symbol": "FAKE",
        "dex": "raydium",
        "pool_url": "https://dexscreener.com/solana/fake",
        "price_usd": 0.01,
        "liq_usd": max(500.0, C.PF_SCAN_MIN_LIQ_USD + 200),
        "mcap_usd": max(2_000.0, C.PF_SCAN_MIN_MCAP_USD + 1_000),
        "fdv_usd": 5_000_000.0,
        "token_age_min": 10.0,
        "vol_5m_usd": max(200.0, C.PF_SCAN_MIN_5M_VOL_USD + 100),
        "vol_1h_usd": 10 * max(200.0, C.PF_SCAN_MIN_5M_VOL_USD + 100),
        "buys_5m": max(10, C.PF_SCAN_MIN_5M_BUYS + 5),
        "sells_5m": 2,
        "unique_buyers_5m": 8,
        "whales_5m": max(1, C.PF_SCAN_MIN_WHALE_COUNT_5M),
        "whale_vol_5m_usd": 1_000.0,
        "largest_buy_5m_sol": max(2.0, C.PF_SCAN_MIN_WHALE_SIZE_SOL + 0.5),
        "price_change_5m_pct": 45.0,
        "price_change_15m_pct": 30.0,
        "price_change_1h_pct": 80.0,
        "holders": 250,
        "holders_5m_change": 25,
        "is_honeypot": False,
        "trading_paused": False,
        "blacklisted": False,
        "ai_rug_score": 0.2,
        "top10_hold_pct": 40.0,
        "dev_hold_pct": 5.0,
        "mint_authority_disabled": True,
        "freeze_authority_disabled": True,
    }


async def _build_fake_runner():
    """
    Wrap the meta into whatever minimal runner structure Strategy expects.
    We mimic the shape your scanners produce: a dict with meta + scores.
    """
    meta = _make_fake_meta()
    momentum = PF.compute_momentum_score(meta)
    final = PF.compute_final_score(meta)

    meta["momentum_score"] = momentum
    meta["final_score"] = final

    log.info("FAKE META", extra={"momentum": momentum, "final": final})

    # Ensure it passes the viability check; if not, print reasons.
    ok = PF.is_viable_pair(meta)
    dbg = meta.get("_filter_debug", {})
    log.info(
        "PAIR_FILTER",
        extra={"ok": ok, "reasons": dbg.get("reasons"), "momentum": dbg.get("momentum_score"),
               "final": dbg.get("final_score")},
    )
    if not ok:
        print("❌ PairFilter still rejecting fake meta; check reasons above.")
        return None

    # Runner shape – adapt keys if your Strategy expects slightly different names
    runner = {
        "mint": meta["mint"],
        "symbol": meta["symbol"],
        "dex": meta["dex"],
        "pool_url": meta["pool_url"],
        "meta": meta,
        "momentum_score": momentum,
        "final_score": final,
        "liq_usd": meta["liq_usd"],
        "vol_5m_usd": meta["vol_5m_usd"],
        "price_change_5m_pct": meta["price_change_5m_pct"],
        "created_at_ts": time.time(),
        "_source": "TEST_FAKE",
    }
    return runner


async def main():
    print("=== TEST: Strategy BUY path (DRYRUN) ===")
    print(f"DRY_RUN={C.DRY_RUN} AUTO_BUY_ENABLED={C.AUTO_BUY_ENABLED} PAUSE_TRADING={C.PAUSE_TRADING}")

    if not C.DRY_RUN:
        print("⚠️ For this test you should set DRY_RUN=1 in env so no real trades are sent.")
    if not C.AUTO_BUY_ENABLED:
        print("⚠️ AUTO_BUY_ENABLED=0 – Strategy may refuse to buy by config.")

    runner = await _build_fake_runner()
    if runner is None:
        return

    # Instantiate Strategy with minimal args; adjust if your __init__ needs more
    strat = Strategy()

    # We synthesize a tiny batch with just our fake runner.
    runners = [runner]

    print("\n>>> Feeding fake runner into Strategy in DRYRUN mode…\n")

    # You may have a different public entry; adapt to your Strategy implementation.
    # Most common pattern is a method that evaluates candidates and maybe buys:
    if hasattr(strat, "_handle_scanner_batch"):
        await strat._handle_scanner_batch("raydium", runners)
    elif hasattr(strat, "process_runners"):
        await strat.process_runners("raydium", runners)
    else:
        # Fallback: directly call a 'maybe buy' style method – adapt as needed.
        buy_method = getattr(strat, "_maybe_buy_runner", None)
        if buy_method is None:
            print("❌ Strategy has no obvious runner entrypoint; adjust test_strategy_buy_path.")
            return
        for r in runners:
            await buy_method("raydium", r)

    print("\n=== TEST DONE ===")
    print("Check logs / Telegram for BUY_DRYRUN or similar buy-path messages.")


if __name__ == "__main__":
    asyncio.run(main())
