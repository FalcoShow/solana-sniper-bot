from __future__ import annotations

import asyncio
import json

from utils_pkg.pumpportal_client import PumpPortalClient


async def main() -> None:
    print("=== PumpPortal smoke test ===")
    async with PumpPortalClient() as client:
        agen = client.events()
        # Grab just 1 event (with a timeout) so we don't hang forever.
        try:
            msg = await asyncio.wait_for(agen.__anext__(), timeout=30.0)
        except asyncio.TimeoutError:
            print("Timed out waiting for pump event")
            return

        print("Received event kind:", msg.get("kind"))
        raw = msg.get("raw") or {}
        print("Raw snippet:", json.dumps(raw)[:300], "...")
        print("PumpPortal OK ✅")


if __name__ == "__main__":
    asyncio.run(main())
