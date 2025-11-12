from __future__ import annotations

import asyncio
import json
import os

import httpx

from utils_pkg import lightning_impl as L
from utils_pkg.pumpportal_client import PumpPortalClient


async def test_jup_quote() -> None:
    print("=== [1] JUP QUOTE TEST ===")
    url = L.BASE + "/swap/v1/quote"
    params = {
        "inputMint":  "So11111111111111111111111111111111111111112",
        "outputMint": "So11111111111111111111111111111111111111112",
        "amount": 1_000_000,  # 0.001 SOL
        "slippageBps": 50,
        "swapMode": "ExactIn",
        "onlyDirectRoutes": "false",
        "asLegacyTransaction": "false",
    }
    async with httpx.AsyncClient(timeout=10) as c:
        r = await c.get(url, params=params, headers=L._jup_headers())
        print("status:", r.status_code)
        if r.status_code != 200:
            print("body:", r.text[:500])
            return
        data = r.json()
        print("keys:", list(data.keys())[:5])
    print("Jupiter quote OK ✅")


async def test_pumpportal_once() -> None:
    print("\n=== [2] PUMPPORTAL EVENT TEST ===")
    async with PumpPortalClient() as client:
        agen = client.events()
        msg = await asyncio.wait_for(agen.__anext__(), timeout=30.0)
        print("kind:", msg.get("kind"))
        print("snippet:", json.dumps(msg.get("raw", {}))[:300], "...")
    print("PumpPortal events OK ✅")


async def main() -> None:
    print("DRYRUN =", os.getenv("DRYRUN"))
    await test_jup_quote()
    await test_pumpportal_once()
    print("\nAll stack smoke tests completed ✅")


if __name__ == "__main__":
    asyncio.run(main())
