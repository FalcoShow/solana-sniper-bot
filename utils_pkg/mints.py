# utils_pkg/mints.py
from __future__ import annotations

import re
from typing import Optional

# ──────────────────────────────────────────────────────────────────────────────
# Canonical mints (Solana)
# ──────────────────────────────────────────────────────────────────────────────
# NOTE: These are used across the bot (router, tests, etc.), so keep stable.
SOL_MINT = "So11111111111111111111111111111111111111112"  # wSOL "pseudo-mint"
USDT_MINT = "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB"  # USDT
USDC_MINT = "EPjFWdd5AufqSSqeMzQ1NzxypaC8G4weEGKzwyTDt1v"  # USDC (native/CCTP)
BONK_MINT = "DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263"  # BONK

# Common symbol aliases (case-insensitive). We resolve "USDC" to native/CCTP.
_SYM_TO_MINT = {
    "SOL": SOL_MINT,
    "WSOL": SOL_MINT,
    "W-SOL": SOL_MINT,
    "W_SOL": SOL_MINT,
    "USDT": USDT_MINT,
    "TETHER": USDT_MINT,
    "USDC": USDC_MINT,
    "USDC.SOL": USDC_MINT,
    "USDC.CCTP": USDC_MINT,
    "BONK": BONK_MINT,
}

# Base58 (Bitcoin alphabet; excludes 0,O,I,l)
_B58 = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
_B58_IDX = {c: i for i, c in enumerate(_B58)}

# Up to 44 base58 chars for a 32-byte key (depending on leading zeros)
_MAX_B58_LEN = 44

# Some feeds append suffix-y words (pump.fun, raydium, etc.)
_SUS_SUFFIXES = ("pump", "bonk", "raydium", "rayd", "dex", "sol", "solana")

# Broad base58-looking finder for URLs/blobs
_B58_RE = re.compile(r"[1-9A-HJ-NP-Za-km-z]{20,90}")


# ──────────────────────────────────────────────────────────────────────────────
# Minimal base58 decode (no checksum) and strict 32-byte check
# ──────────────────────────────────────────────────────────────────────────────
def _b58decode(s: str) -> bytes:
    """
    Minimal base58 decode (Bitcoin alphabet), used only for length=32 sanity.
    """
    s = (s or "").strip()
    if not s:
        return b""
    n = 0
    for ch in s:
        i = _B58_IDX.get(ch)
        if i is None:
            raise ValueError("invalid base58 character")
        n = n * 58 + i
    raw = n.to_bytes((n.bit_length() + 7) // 8, "big") if n else b""
    # preserve leading '1' => zero bytes
    pad = len(s) - len(s.lstrip("1"))
    return (b"\x00" * pad) + raw


def _is_base58_32(s: str) -> bool:
    """Return True iff `s` decodes to exactly 32 bytes of base58."""
    try:
        return len(_b58decode(s)) == 32
    except Exception:
        return False


def _strip_suffixes_and_clip(s: str) -> str:
    """
    Remove known suffix words ('pump', 'raydium', etc.); then hard-cap to 44
    chars (max base58 length for a 32-byte pubkey).
    """
    low = s.lower()
    changed = True
    while changed:
        changed = False
        for suf in _SUS_SUFFIXES:
            if low.endswith(suf):
                s = s[: -len(suf)]
                low = s.lower()
                changed = True
                break

    if len(s) > _MAX_B58_LEN:
        s = s[:_MAX_B58_LEN]
    return s


def _extract_base58_32(blob: str) -> Optional[str]:
    """
    Recover a *real* 32-byte base58 key from arbitrary text/URL.

    Strategy:
      - Direct exact check.
      - Try last path segment from URL-ish strings.
      - Scan for base58-looking chunks; strip suffixes; validate len==32 bytes;
        if overly long, try first 44 chars and re-check.
    """
    if not blob:
        return None

    t = blob.strip()

    # Direct exact check
    if _is_base58_32(t):
        return t

    # Try last path segment for URLs like .../coin/<mint>
    try:
        parts = re.split(r"[/?#]", t)
        if parts:
            last = parts[-1]
            if _is_base58_32(last):
                return last
    except Exception:
        pass

    # Scan for base58-looking segments inside the blob
    for cand in _B58_RE.findall(t):
        clean = _strip_suffixes_and_clip(cand)
        if _is_base58_32(clean):
            return clean

        # If original candidate is longer than 44 chars, try the first 44
        if len(cand) > _MAX_B58_LEN:
            clipped = cand[:_MAX_B58_LEN]
            if _is_base58_32(clipped):
                return clipped

    return None


# ──────────────────────────────────────────────────────────────────────────────
# Public API
# ──────────────────────────────────────────────────────────────────────────────
def normalize_mint(m: str) -> str:
    """
    Return a canonical, 32-byte base58 mint address.

    Accepts:
      - Symbols: SOL, WSOL, USDT, USDC, BONK, etc.
      - Exact base58 address (must decode to 32 bytes).
      - Pump.fun / dexscreener-style URLs or strings with a mint inside.
      - Salted IDs where someone tacked on words like '...bonk'/'...pump'/
        '...raydium' that are still pure base58.

    This is intentionally *strict*: if it doesn't decode to 32 bytes,
    we treat it as invalid so we *never* send bad mints to Jupiter.
    """
    if not m or not str(m).strip():
        raise ValueError("empty mint")

    s = str(m).strip()

    # 1) Symbol alias?
    sym = s.upper()
    aliased = _SYM_TO_MINT.get(sym)
    if aliased:
        return aliased

    # 2) Try to recover a real 32-byte mint from the blob/URL/string.
    cand = _extract_base58_32(s)
    if cand and _is_base58_32(cand):
        return cand

    # If we got here, it's garbage for our purposes.
    raise ValueError(f"cannot normalize mint from input: {s!r}")


def is_base58_mint(s: str) -> bool:
    """
    Public helper for quick validation.
    True iff the string decodes to exactly 32 bytes of base58.
    """
    return _is_base58_32((s or "").strip())


__all__ = [
    "normalize_mint",
    "is_base58_mint",
    "SOL_MINT",
    "USDT_MINT",
    "USDC_MINT",
    "BONK_MINT",
]
