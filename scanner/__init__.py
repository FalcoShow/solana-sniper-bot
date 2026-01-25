# scanner/__init__.py
from __future__ import annotations

"""
scanner/__init__.py — Scanner package (P0 LOCK-IN, V3+++)
========================================================

Hard rules
----------
✅ Importing `scanner` must NEVER perform network I/O.
✅ `scanner.registry` is the single source of truth for scanner wiring.
✅ Downstream code should not import individual scanners directly in production
   (they remain available for tools/dev/tests, but are not re-exported here).

Primary API
-----------
    from scanner import REGISTRY, scan_all, scan_one, scan_flat, health

Design
------
- Keep this file minimal and deterministic.
- Provide small convenience wrappers so strategy code doesn’t reach into internals.
- Re-export only stable types (ScannerRegistry, ScannerSpec) and the singleton registry.

"""

from collections.abc import Sequence
from typing import Any

from .registry import REGISTRY, ScannerRegistry, ScannerSpec

# Convenience wrappers (no side effects; registry handles lazy imports)


async def scan_one(
    name: str,
    *,
    redis: Any = None,
    session: Any = None,
    timeout_s: float | None = None,
    **kw: Any,
):
    """
    Run one scanner by name and return canonical Candidates.

    Example:
        cands = await scan_one("pumpfun", redis=r, session=sess)
    """
    return await REGISTRY.scan_one(name, redis=redis, session=session, timeout_s=timeout_s, **kw)


async def scan_all(
    *,
    redis: Any = None,
    session: Any = None,
    names: Sequence[str] | None = None,
    concurrency: int | None = None,
    timeout_s: float | None = None,
    **kw: Any,
) -> dict[str, list[Any]]:
    """
    Run enabled scanners concurrently and return {scanner_name: [Candidate, ...]}.
    """
    return await REGISTRY.scan_all(
        redis=redis,
        session=session,
        names=names,
        concurrency=concurrency,
        timeout_s=timeout_s,
        **kw,
    )


async def scan_flat(
    *,
    redis: Any = None,
    session: Any = None,
    names: Sequence[str] | None = None,
    concurrency: int | None = None,
    timeout_s: float | None = None,
    **kw: Any,
) -> list[Any]:
    """
    Convenience: run scan_all and flatten results into a single list.
    """
    return await REGISTRY.scan_flat(
        redis=redis,
        session=session,
        names=names,
        concurrency=concurrency,
        timeout_s=timeout_s,
        **kw,
    )


def health(*, names: Sequence[str] | None = None) -> dict[str, Any]:
    """
    Get health snapshot for enabled scanners (best-effort).
    """
    return REGISTRY.health(names=names)


async def shutdown_all(*, names: Sequence[str] | None = None) -> None:
    """
    Gracefully shutdown scanners (best-effort).
    """
    await REGISTRY.shutdown_all(names=names)


__all__ = [
    "REGISTRY",
    "ScannerRegistry",
    "ScannerSpec",
    "health",
    "scan_all",
    "scan_flat",
    "scan_one",
    "shutdown_all",
]
