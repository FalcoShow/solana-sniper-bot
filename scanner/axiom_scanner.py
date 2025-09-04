# scanner/axiom_scanner.py — v1.0
from __future__ import annotations

import asyncio
import logging
import os
import re
from typing import List, Dict, Any

from integrations.axiom_web import axiom_discover_once

LOG = logging.getLogger("scanner.axiom")

# ---------- Env knobs ----------
AXIOM_SCAN_ENABLED = int(os.getenv("AXIOM_SCAN_ENABLED", "1"))  # set 0 to disable
AXIOM_SCAN_LIMIT = int(os.getenv("AXIOM_SCAN_LIMIT", "80"))     # post-dedup cap
AXIOM_SCAN_TIMEOUT_SEC = int(os.getenv("AXIOM_SCAN_TIMEOUT_SEC", "60"))

_BASE58 = re.compile(r"^[1-9A-HJ-NP-Za-km-z]{32,64}$")

def _valid_mint(x: str) -> bool:
    return isinstance(x, str) and _BASE58.match(x) is not None

def _dedup(items: List[Dict[str, Any]], limit: int) -> List[Dict[str, Any]]:
    seen = set()
    out: List[Dict[str, Any]] = []
    for d in items:
        m = (d or {}).get("mint")
        if not _valid_mint(m):
            continue
        if m in seen:
            continue
        seen.add(m)
        # normalize keys
        d.setdefault("source", "axiom")
        d.setdefault("scanner", "axiom")
        out.append(d)
        if len(out) >= limit:
            break
    return out

async def scan_axiom() -> List[Dict[str, Any]]:
    """
    Pull candidates from Axiom Discover (Trending / Surge / DEX Screener / Pump Live).
    Returns: [{ mint, source:'axiom', scanner:'axiom', ... }, ...]
    """
    if not AXIOM_SCAN_ENABLED:
        LOG.info("scanner.axiom: disabled via AXIOM_SCAN_ENABLED=0")
        return []

    try:
        # Ask for a bit more than our final cap; we’ll dedup & trim.
        want = max(AXIOM_SCAN_LIMIT, 1)
        raw: List[Dict[str, Any]] = await asyncio.wait_for(
            axiom_discover_once(limit=want * 2),
            timeout=AXIOM_SCAN_TIMEOUT_SEC,
        )
        items = _dedup(raw or [], limit=want)
        LOG.info("scanner.axiom: %d candidates (raw=%d, cap=%d)",
                 len(items), len(raw or []), want)
        return items
    except asyncio.TimeoutError:
        LOG.warning("scanner.axiom: timed out after %ss", AXIOM_SCAN_TIMEOUT_SEC)
        return []
    except Exception as e:
        LOG.warning("scanner.axiom error: %s", e)
        return []

__all__ = ["scan_axiom"]
