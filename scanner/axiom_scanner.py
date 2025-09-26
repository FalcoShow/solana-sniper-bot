# scanner/axiom_scanner.py — v1.1 (persistent client + retry)
from __future__ import annotations

import asyncio
import logging
import os
import re
from typing import List, Dict, Any, Optional

from integrations.axiom_web import AxiomWeb

LOG = logging.getLogger("scanner.axiom")

# ---------- Env knobs ----------
AXIOM_SCAN_ENABLED = int(os.getenv("AXIOM_SCAN_ENABLED", "1"))
AXIOM_SCAN_LIMIT = int(os.getenv("AXIOM_SCAN_LIMIT", "80"))
AXIOM_SCAN_TIMEOUT_SEC = int(os.getenv("AXIOM_SCAN_TIMEOUT_SEC", "90"))
AXIOM_SECTIONS_ENV = os.getenv("AXIOM_SECTIONS", "Trending,Surge,DEX Screener,Pump Live")

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
        d = dict(d or {})
        d.setdefault("source", "axiom")
        d.setdefault("scanner", "axiom")
        out.append(d)
        if len(out) >= limit:
            break
    return out

# ---------- persistent client ----------
_AX: Optional[AxiomWeb] = None
_ax_lock = asyncio.Lock()

async def _get_ax() -> AxiomWeb:
    global _AX
    async with _ax_lock:
        if _AX is None:
            _AX = AxiomWeb(headless=True)
            await _AX.launch()
        return _AX

async def _recycle_ax() -> None:
    global _AX
    async with _ax_lock:
        try:
            if _AX:
                await _AX.close()
        finally:
            _AX = None

async def _fetch_once(want: int) -> List[Dict[str, Any]]:
    ax = await _get_ax()
    sections = await ax.discover_sections(limit_per_section=want)
    # flatten
    flat: List[Dict[str, Any]] = []
    for lst in sections.values():
        flat.extend(lst)
    return flat

async def scan_axiom() -> List[Dict[str, Any]]:
    if not AXIOM_SCAN_ENABLED:
        LOG.info("scanner.axiom: disabled via AXIOM_SCAN_ENABLED=0")
        return []

    want = max(AXIOM_SCAN_LIMIT, 1) * 2  # ask for extra, then dedup/trim
    attempts = 2
    last_err = None

    for i in range(1, attempts + 1):
        try:
            raw = await asyncio.wait_for(_fetch_once(want), timeout=AXIOM_SCAN_TIMEOUT_SEC)
            items = _dedup(raw or [], limit=AXIOM_SCAN_LIMIT)
            LOG.info("scanner.axiom: %d candidates (raw=%d, cap=%d) [attempt %d/%d]",
                     len(items), len(raw or []), AXIOM_SCAN_LIMIT, i, attempts)
            return items
        except asyncio.TimeoutError as e:
            last_err = e
            LOG.warning("scanner.axiom: attempt %d/%d timed out after %ss",
                        i, attempts, AXIOM_SCAN_TIMEOUT_SEC)
            await _recycle_ax()  # drop and recreate browser next round
        except Exception as e:
            last_err = e
            LOG.warning("scanner.axiom: attempt %d/%d failed: %s", i, attempts, e)
            await _recycle_ax()

    LOG.warning("scanner.axiom: giving up after %d attempts: %s", attempts, last_err or "")
    return []

__all__ = ["scan_axiom"]
