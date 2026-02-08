from __future__ import annotations

"""
whale_feed.py (compat shim)
===========================

This repo historically had whale_feed.py at repo root.
The canonical implementation now lives at utils_pkg.whale_feed.

Do not add logic here. Keep it as a stable re-export layer.
"""

from utils_pkg.whale_feed import WhaleCandidate, main, run_whale_feed

__all__ = ["WhaleCandidate", "run_whale_feed", "main"]

if __name__ == "__main__":
    import asyncio

    asyncio.run(main())
