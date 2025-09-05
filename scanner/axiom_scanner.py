# scanner/axiom_scanner.py — v1.1
from __future__ import annotations

import asyncio
import logging
import os
import re
from typing import List, Dict, Any

from integrations.axiom_web import axiom_discover_once

LOG = logging.getLogger("scanner.axiom")

# ---------- Env knobs ----------
AXIOM_SCAN_ENABLED: int = int(os.getenv("AXIOM_SCAN_ENABLED", "1"))   # set 0 to disable
AXIOM_SCAN_LIMIT: int = int(os.getenv("AXIOM_SCAN_LIMIT", "80"))      # post-dedup cap
AXIOM_SCAN_TIMEOUT_SEC: int = int(os.getenv("AXIOM_SCAN_TIMEOUT_SEC", "90"))
AXIOM_SCAN_RETRIES: int = max(1, int(os.getenv("AXIOM_SCAN_RETRIES", "2")))
AXIOM_SCAN_BACKOFF_MS: int = max(0, int(os.getenv("AXIOM_SCAN_BACKOFF_MS", "1200")))

# Base58 (Solana) – keep the same bounds used elsewhere in the project
_BASE58 = re.compile(r"^[1-9A-HJ-NP-Za-km-z]{32,64}$")

def _valid_mint(x: str) -> bool:
    return isinstance(x, str) and bool(_BASE58.match(x))

def _dedup(items: List[Dict[str, Any]], limit: int) -> List[Dict[str, Any]]:
    seen = set()
    out: List[Dict[str, Any]] = []
    for d in items or []:
        m = (d or {}).get("mint")
        if not _valid_mint(m):
            continue
        if m in seen:
            continue
        seen.add(m)
        # normalize keys for downstream
        d = dict(d)
        d.setdefault("source", "axiom")
        d.setdefault("scanner", "axiom")
        out.append(d)
        if len(out) >= max(1, limit):
            break
    return out

async def _scan_once(want: int, timeout_sec: int) -> List[Dict[str, Any]]:
    """
    Single attempt at pulling from Axiom Discover, with its own timeout.
    We ask for more than we need (2x) and then dedup+cap locally.
    """
    raw: List[Dict[str, Any]] = await asyncio.wait_for(
        axiom_discover_once(limit=max(1, want * 2)),
        timeout=timeout_sec,
    )
    return _dedup(raw, want)

async def scan_axiom() -> List[Dict[str, Any]]:
    """
    Pull candidates from Axiom Discover (Trending / Surge / DEX Screener / Pump Live).
    Returns: [{ mint, source:'axiom', scanner:'axiom', ... }, ...]
    """
    if not AXIOM_SCAN_ENABLED:
        LOG.info("scanner.axiom: disabled via AXIOM_SCAN_ENABLED=0")
        return []

    want = max(1, AXIOM_SCAN_LIMIT)
    timeout_sec = max(15, AXIOM_SCAN_TIMEOUT_SEC)  # guardrail

    last_err: Exception | None = None
    for attempt in range(1, AXIOM_SCAN_RETRIES + 1):
        try:
            items = await _scan_once(want, timeout_sec)
            LOG.info("scanner.axiom: %d candidates (cap=%d) [attempt %d/%d]",
                     len(items), want, attempt, AXIOM_SCAN_RETRIES)
            return items
        except asyncio.TimeoutError as e:
            last_err = e
            LOG.warning("scanner.axiom: attempt %d/%d timed out after %ss",
                        attempt, AXIOM_SCAN_RETRIES, timeout_sec)
        except Exception as e:
            last_err = e
            LOG.warning("scanner.axiom: attempt %d/%d error: %s",
                        attempt, AXIOM_SCAN_RETRIES, e)

        # backoff before retrying (except after final attempt)
        if attempt < AXIOM_SCAN_RETRIES and AXIOM_SCAN_BACKOFF_MS > 0:
            await asyncio.sleep(AXIOM_SCAN_BACKOFF_MS / 1000.0)

    # All attempts failed
    if last_err:
        LOG.warning("scanner.axiom: giving up after %d attempts: %s",
                    AXIOM_SCAN_RETRIES, last_err)
    return []

__all__ = ["scan_axiom"]
