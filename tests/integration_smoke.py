import asyncio
import os
import json
from scripts import main as M

# ensure dryrun enforced
os.environ["DRYRUN"] = "1"
os.environ["IS_DRYRUN"] = "1"

async def fake_screen_token_ok(mint):
    return True, {"ok": True, "decimals": 9, "priceImpactPct": 0.1}

async def fake_screen_token_no_account(mint):
    return False, {"ok": False, "reason": "no_account"}

async def fake_maybe_buy(*, mint, ui):
    # This should not execute real buy while DRYRUN=1; but return a fake sig for flow
    return "FAKE_TX_SIG_123"

class FakeExit:
    async def on_trade_tick(self, *_a, **_k):
        return None
    async def on_buy_filled(self, raw_id_or_mint, tx_sig):
        return None

async def run_integration():
    # patch functions
    M.screen_token = fake_screen_token_ok
    M.maybe_buy = fake_maybe_buy
    global exit_engine
    M.exit_engine = FakeExit()
    strat = M.Strategy()

    # create a fresh new-token event
    now = int(_dt := __import__("datetime").datetime.now(__import__("datetime").timezone.utc).timestamp())
    raw_event = {
        "pool": "pump",
        "symbol": "TEST",
        "name": "Test Token",
        "created_at": now * 1000,  # ms timestamp
        "txType": "create",
        "initialBuy": 0.1,
        "mint": "FakeMintTest111111111111111111111111111111",
    }

    # call on_new_token and on_trade to simulate trades during momentum window
    await strat.on_new_token(dict(raw_event))
    # simulate trades during window
    await strat.on_trade({"mint": "FakeMintTest111111111111111111111111111111"})
    await asyncio.sleep(M.ENTRY_MOMO_WINDOW_SEC + 1)  # allow the gate to evaluate

if __name__ == "__main__":
    asyncio.run(run_integration())
