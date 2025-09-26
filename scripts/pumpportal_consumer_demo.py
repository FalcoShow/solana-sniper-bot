import asyncio
from utils_pkg.pumpportal_client import PumpPortalClient

async def main():
    client = PumpPortalClient()  # reads env vars
    print("Connecting to:", client.masked_uri())
    async for ev in client.events():
        k = ev["kind"]
        m = ev["raw"]
        if k == "new_token":
            print("NEW:", m.get("symbol"), m.get("name"), m.get("mint"))
        elif k == "trade":
            print("TRADE:", m.get("mint"), m.get("txType"), m.get("uiAmount"))
        elif k == "warning":
            print("WARN:", m.get("errors"))
        # ignore 'info' and 'unknown' to keep output clean

asyncio.run(main())
