import asyncio, os
from scripts import main as M

class FakeExit:
    async def on_buy_filled(self, raw_id_or_mint, tx_sig):
        print("FakeExit.on_buy_filled called:", raw_id_or_mint, tx_sig)

async def test():
    M._pkg_is_dryrun = False   # simulate package-level not-dryrun (we still have env DRYRUN=1)
    M.exit_engine = FakeExit()
    strat = M.Strategy()
    # simulate buy returned a tx_sig
    await strat._gate_candidate({"symbol":"T","pool":"pump"}, "mint1", "mint1")
    # Note: _gate_candidate contains buy logic; to force immediate buy path you'd need to bypass momo flow
if __name__ == "__main__":
    asyncio.run(test())
