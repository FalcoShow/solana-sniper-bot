# utils_pkg/mints.py
from __future__ import annotations

import re
from typing import Optional

# ──────────────────────────────────────────────────────────────────────────────
# Canonical mints (Solana)
# ──────────────────────────────────────────────────────────────────────────────
SOL_MINT   = "So11111111111111111111111111111111111111112"  # wSOL "pseudo-mint"
USDT_MINT  = "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB"  # USDT
USDC_MINT  = "EPjFWdd5AufqSSqeMzQ1NzxypaC8G4weEGKzwyTDt1v"  # USDC (native/CCTP)
BONK_MINT  = "DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263"  # BONK (common gotcha)

# Common symbol aliases (case-insensitive). We resolve "USDC" to native/CCTP.
_SYM_TO_MINT = {
    "SOL": SOL_MINT,
    "WSOL": SOL_MINT,
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

# Some feeds append suffix-y words
_SUS_SUFFIXES = ("pump", "bonk", "raydium", "rayd", "dex", "sol", "solana")

# Broad base58-looking finder for URLs/blobs
_B58_RE = re.compile(r"[1-9A-HJ-NP-Za-km-z]{20,90}")


# ──────────────────────────────────────────────────────────────────────────────
# Minimal base58 decode (no checksum); used only to verify 32-byte length
# ──────────────────────────────────────────────────────────────────────────────
def _b58decode(s: str) -> bytes:
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
    try:
        return len(_b58decode(s)) == 32
    except Exception:
        return False


def _strip_suffixes_and_clip(s: str) -> str:
    """Remove known suffix words; then hard-cap to 44 chars."""
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
    Recover a 32-byte base58 key from arbitrary text/URL.
    Strategy:
      - Direct exact check
      - Try last path segment from URL-ish strings
      - Scan for base58-looking chunks; strip salts; validate; or try first 44
    """
    if not blob:
        return None

    t = blob.strip()
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

    for cand in _B58_RE.findall(t):
        clean = _strip_suffixes_and_clip(cand)
        if _is_base58_32(clean):
            return clean
        if len(cand) > _MAX_B58_LEN and _is_base58_32(cand[:_MAX_B58_LEN]):
            return cand[:_MAX_B58_LEN]

    return None


# ──────────────────────────────────────────────────────────────────────────────
# Public API
# ──────────────────────────────────────────────────────────────────────────────
def normalize_mint(m: str) -> str:
    """
    Return a canonical, 32-byte base58 mint address.

    Accepts:
      - Symbols: SOL, WSOL, USDT, USDC (maps to **EPjF…**).
      - Exact base58 address.
      - Pump.fun / dexscreener-style URLs or strings with a mint inside.
      - Salted IDs where someone tacked on words like '...bonk'/'...pump'.

    Raises:
      ValueError if a valid 32-byte base58 key cannot be recovered.
    """
    if not m or not str(m).strip():
        raise ValueError("empty mint")

    s = str(m).strip()

    # Symbol?
    sym = s.upper()
    if sym in _SYM_TO_MINT:
        s = _SYM_TO_MINT[sym]
    else:
        # Extract from URL/blob/salted string
        cand = _extract_base58_32(s)
        if not cand:
            raise ValueError(f"cannot normalize mint from input: {s!r}")
        s = cand

    # Final sanity: must decode to 32 bytes
    if not _is_base58_32(s):
        raise ValueError(f"not a 32-byte base58 mint: {s!r}")

    return s


def is_base58_mint(s: str) -> bool:
    """Public helper for quick validation."""
    return _is_base58_32((s or "").strip())


__all__ = [
    "normalize_mint",
    "is_base58_mint",
    "SOL_MINT",
    "USDT_MINT",
    "USDC_MINT",
    "BONK_MINT",
]
