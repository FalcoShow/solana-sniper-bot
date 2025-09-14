# scripts/onchain_scan.py
import asyncio, json
from utils_pkg.onchain_scanner import onchain_scan

async def handle(ev):
    print(json.dumps({
        "slot": ev["slot"],
        "sig": ev["signature"],
        "mint": ev["mint"],
        "preview": ev["logs"][:5],
        "ts": ev["ts"],
    }, ensure_ascii=False))

if __name__ == "__main__":
    asyncio.run(onchain_scan(handle))
