# utils_pkg/mints.py
from __future__ import annotations

import re
from typing import Optional

# ──────────────────────────────────────────────────────────────────────────────
# Canonical mints (Solana / Jupiter)
# ──────────────────────────────────────────────────────────────────────────────
SOL_MINT        = "So11111111111111111111111111111111111111112"  # wSOL "pseudo-mint"
USDT_MINT       = "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB"
USDC_CLASSIC    = "EPjFWdd5AufqSSqeMzQ1NzxypaC8G4weEGKzwyTDt1v"  # NOT tradable on /swap/v1
USDC_CCTP       = "DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263"  # Tradable USDC on /swap/v1

# Common symbol aliases (case-insensitive). We resolve "USDC" to CCTP by default.
_SYM_TO_MINT = {
    "SOL": SOL_MINT,
    "WSOL": SOL_MINT,
    "USDT": USDT_MINT,
    "USDC": USDC_CCTP,
    "USDC.CCTP": USDC_CCTP,
    "USDC.EPJ": USDC_CLASSIC,  # will be remapped to CCTP later anyway
}

# Base58 alphabet (Bitcoin style; excludes 0,O,I,l)
_B58_ALPHABET = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
_B58_IDX = {c: i for i, c in enumerate(_B58_ALPHABET)}

# Up to 44 base58 chars for 32 bytes (depends on leading zeroes)
_MAX_MINT_LEN = 44

# When streams append base58-looking salts/words ('pump','bonk', etc.)
_SUS_SUFFIXES = ("pump", "bonk", "raydium", "rayd", "dex", "sol", "solana")

# Regex to pull base58 substrings out of URLs/text blobs
_B58_RE = re.compile(r"[1-9A-HJ-NP-Za-km-z]{20,90}")  # 20..90 to avoid tiny noise


# ──────────────────────────────────────────────────────────────────────────────
# Minimal base58 decode (no checksum) so we can verify 32-byte keys
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
    # Convert to bytes, preserving leading '1' -> zero prefix
    full = n.to_bytes((n.bit_length() + 7) // 8, "big") if n else b""
    pad = len(s) - len(s.lstrip("1"))
    return (b"\x00" * pad) + full


def _is_base58_32(s: str) -> bool:
    try:
        return len(_b58decode(s)) == 32
    except Exception:
        return False


def _strip_suspicious_suffixes(s: str) -> str:
    """Remove known 'salt' words some feeds append, then hard-cap length."""
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
    if len(s) > _MAX_MINT_LEN:
        s = s[:_MAX_MINT_LEN]
    return s


def _extract_base58_32(blob: str) -> Optional[str]:
    """
    Find a 32-byte base58 key inside arbitrary text/URL.
    Strategy:
      1) Pull candidate base58 substrings.
      2) For each: strip suspicious suffixes, cap to 44, validate length=32 bytes.
      3) Fallback: if raw candidate > 44, try first 44 chars.
    """
    if not blob:
        return None

    # Direct hit first (fast path)
    t = blob.strip()
    if _is_base58_32(t):
        return t

    # URLs like https://pump.fun/coin/<mint>, https://dexscreener.com/solana/<mint>, etc.
    # Try the last path segment first.
    try:
        parts = re.split(r"[/?#]", t)
        if parts:
            last = parts[-1]
            if _is_base58_32(last):
                return last
    except Exception:
        pass

    # Scan for base58-like chunks
    for cand in _B58_RE.findall(t):
        clean = _strip_suspicious_suffixes(cand)
        if _is_base58_32(clean):
            return clean
        # If trimming didn’t work but chunk is long, try the first 44 chars
        if len(cand) > _MAX_MINT_LEN and _is_base58_32(cand[:_MAX_MINT_LEN]):
            return cand[:_MAX_MINT_LEN]

    return None


# ──────────────────────────────────────────────────────────────────────────────
# Public API
# ──────────────────────────────────────────────────────────────────────────────
def normalize_mint(m: str) -> str:
    """
    Return a canonical, 32-byte base58 mint address.

    Accepts:
      - Exact base58 mint (any case/whitespace tolerated).
      - Common symbols: SOL, WSOL, USDT, USDC (maps to **USDC CCTP**).
      - URLs or salted IDs that contain a mint (we extract it).
      - USDC classic (EPjF…) is **auto-remapped** to USDC CCTP (DezX…).

    Raises:
      ValueError if no valid 32-byte base58 key can be recovered.
    """
    if not m or not str(m).strip():
        raise ValueError("empty mint")

    s = str(m).strip()

    # Symbol aliases (case-insensitive)
    sym = s.upper()
    if sym in _SYM_TO_MINT:
        s = _SYM_TO_MINT[sym]
    else:
        # Try to recover a proper 32-byte base58 key from the string/URL
        cand = _extract_base58_32(s)
        if not cand:
            raise ValueError(f"cannot normalize mint from input: {s!r}")
        s = cand

    # Final sanity: must be a 32-byte base58 key
    if not _is_base58_32(s):
        raise ValueError(f"not a 32-byte base58 mint: {s!r}")

    # USDC classic is not tradable on /swap/v1; map to CCTP
    if s == USDC_CLASSIC:
        return USDC_CCTP

    return s


# ──────────────────────────────────────────────────────────────────────────────
# Optional helpers (exported)
# ──────────────────────────────────────────────────────────────────────────────
def is_base58_mint(s: str) -> bool:
    """Public helper used by callers that want to preflight inputs."""
    return _is_base58_32((s or "").strip())


__all__ = [
    "normalize_mint",
    "is_base58_mint",
    "SOL_MINT",
    "USDT_MINT",
    "USDC_CLASSIC",
    "USDC_CCTP",
]
