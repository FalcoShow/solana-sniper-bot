# scripts/pf_scan.py
import asyncio, json, time
from utils_pkg.pump_client import pump_post
from utils_pkg.pump_browser_client import pump_post_via_browser

async def once():
    path = "/api/trpc/board.latest?batch=1"
    payload = {"0": {"json": {}}}

    # try fast aiohttp client first
    try:
        txt = await pump_post(path, json_body=payload, timeout=20)
        print("AIOHTTP OK", len(txt))
        return txt
    except Exception as e1:
        print("aiohttp failed, trying browser:", e1)

    # browser fallback (very reliable through CF)
    txt = await pump_post_via_browser(path, payload, timeout_ms=30000)
    print("BROWSER OK", len(txt))
    return txt

async def main():
    while True:
        try:
            txt = await once()
            # TODO: parse tRPC batch and process items here
        except Exception as e:
            print("scan error:", e)
        await asyncio.sleep(3)

if __name__ == "__main__":
    asyncio.run(main())
