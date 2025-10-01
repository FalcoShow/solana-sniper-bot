# scripts/pp_utils.py
"""
Small PumpPortal utilities shared by the runtime.
"""

from __future__ import annotations

# Allowed Base58 alphabet (no 0, O, I, l)
B58 = set("123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz")


def normalize_mint(m: str) -> str | None:
    """
    Normalize PumpPortal "mint"/id strings coming from the websocket.
    Behavior:
      - accept strings only
      - trim whitespace
      - strip trailing 'pump' or 'bonk' suffixes that PP often appends
      - validate length (32..44) and Base58 charset
      - return normalized mint or None if invalid
    """
    if not isinstance(m, str):
        return None
    m = m.strip()
    for suf in ("pump", "bonk"):
        if m.endswith(suf):
            m = m[:-len(suf)]
    return m if (32 <= len(m) <= 44 and set(m) <= B58) else None


__all__ = ["normalize_mint", "B58"]
