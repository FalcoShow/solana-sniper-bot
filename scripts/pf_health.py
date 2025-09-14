# scripts/pf_health.py
import os, asyncio
from utils_pkg.pump_client import pump_post

async def main():
    bases = os.environ.get("PUMPFUN_API_BASES", "https://frontend-api.pump.fun")
    print("PF bases:", bases)
    while True:
        try:
            txt = await pump_post("/api/trpc/board.latest?batch=1", json_body={"0":{"json":{}}}, timeout=20)
            print("PF API OK:", len(txt), txt[:200].replace("\n"," "))
            return
        except Exception as e:
            print("still down:", type(e).__name__, e)
            await asyncio.sleep(10)

if __name__ == "__main__":
    asyncio.run(main())
