from __future__ import annotations

import asyncio

from utils_pkg.velocity_worker import VelocityCandidate, main, run_velocity

__all__ = ["VelocityCandidate", "run_velocity", "main"]

if __name__ == "__main__":  # pragma: no cover
    asyncio.run(main())
