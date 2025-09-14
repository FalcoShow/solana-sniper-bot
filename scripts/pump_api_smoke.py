# scripts/pump_api_smoke.py
import asyncio
from utils_pkg.pump_client import pump_get, pump_post

async def main():
    txt = await pump_get("/api/trpc/board.latest?batch=1&input=%7B%7D")
    print("GET /board.latest OK, bytes:", len(txt))

    txt2 = await pump_post("/api/trpc/board.latest?batch=1",
                           json_body={"0": {"json": {}}})
    print("POST /board.latest OK, bytes:", len(txt2))

if __name__ == "__main__":
    asyncio.run(main())
