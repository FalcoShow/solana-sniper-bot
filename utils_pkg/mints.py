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
BONK_MINT = "DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263"  # BONK (common gotcha)

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

# Typical Solana pubkey length in base58 is ~32–44 chars.
# We allow a bit wider window to be lenient for salted strings.
_MINT_STR_MAX = 44          # max length of the *actual* mint string
_BLOB_STR_MAX = 70          # max length of entire blob we consider
_MINT_DECODE_MIN = 28       # some real-world mints decode to 30 bytes
_MINT_DECODE_MAX = 40       # small safety window around 32 bytes

# Some feeds append suffix-y words (pump.fun etc.)
_SUS_SUFFIXES = ("pump", "bonk", "raydium", "rayd", "dex", "sol", "solana")

# Broad base58-looking finder for URLs/blobs
_B58_RE = re.compile(r"[1-9A-HJ-NP-Za-km-z]{20,90}")


# ──────────────────────────────────────────────────────────────────────────────
# Minimal base58 decode (no checksum)
# ──────────────────────────────────────────────────────────────────────────────
def _b58decode(s: str) -> bytes:
    """
    Minimal base58 decode.

    We *do not* require exactly 32 bytes anymore – some valid live mints as
    seen via Dexscreener / Jupiter decode to ~30 bytes, so we accept a small
    band around 32 instead of rejecting them.
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


def _is_base58_32ish(s: str) -> bool:
    """
    "32-ish" check for mints:

      • all chars in base58 alphabet
      • base58-decoded length between MINT_DECODE_MIN and MINT_DECODE_MAX

    This is strict enough to filter garbage but lenient enough to keep real
    mints like FeR8V… that decode to 30 bytes.
    """
    if not s:
        return False
    s = s.strip()
    if not (20 <= len(s) <= _BLOB_STR_MAX):
        return False
    try:
        for ch in s:
            if ch not in _B58_IDX:
                return False
        raw = _b58decode(s)
        return _MINT_DECODE_MIN <= len(raw) <= _MINT_DECODE_MAX
    except Exception:
        return False


def _looks_base58ish(s: str) -> bool:
    """
    Fast lenient check: only charset + basic length.

    Used for helpers like is_base58_mint(); normalize_mint() goes through
    the stricter _is_base58_32ish() path.
    """
    if not s:
        return False
    s = s.strip()
    if not (20 <= len(s) <= _BLOB_STR_MAX):
        return False
    for ch in s:
        if ch not in _B58_IDX:
            return False
    return True


def _strip_suffixes_and_clip(s: str) -> str:
    """
    Remove known suffix words (pump, raydium, etc.) and hard-cap to
    MINT_STR_MAX characters.

    This is the key tweak that lets us handle things like:
      https://dexscreener.com/solana/<mint>raydium
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
    if len(s) > _MINT_STR_MAX:
        s = s[:_MINT_STR_MAX]
    return s


def _extract_base58_32(blob: str) -> Optional[str]:
    """
    Recover a base58 *mint-like* string from arbitrary text/URL.

    Strategy:
      1) If the whole blob looks like a mint:
         - if length <= MINT_STR_MAX -> accept
         - if longer -> strip suffixes + clip, then re-check.
      2) Try last path segment from URL-ish strings (…/coin/<mint>).
      3) Scan for base58-looking chunks; strip suffixes; validate; or clip.

    Returns:
      canonical-looking mint string, or None if we can't confidently recover one.
    """
    if not blob:
        return None

    t = blob.strip()

    # 1) Entire blob as a candidate
    if _looks_base58ish(t):
        if len(t) <= _MINT_STR_MAX and _is_base58_32ish(t):
            return t
        # Likely salted (e.g. <mint>raydium)
        clean = _strip_suffixes_and_clip(t)
        if _is_base58_32ish(clean):
            return clean

    # 2) Last path segment of a URL-ish string
    try:
        parts = re.split(r"[/?#]", t)
        if parts:
            last = parts[-1]
            if last:
                clean = _strip_suffixes_and_clip(last)
                if _is_base58_32ish(clean):
                    return clean
    except Exception:
        pass

    # 3) Scan for base58-looking segments inside the blob
    for cand in _B58_RE.findall(t):
        clean = _strip_suffixes_and_clip(cand)
        if _is_base58_32ish(clean):
            return clean
        if len(cand) > _MINT_STR_MAX:
            clipped = cand[:_MINT_STR_MAX]
            if _is_base58_32ish(clipped):
                return clipped

    return None


# ──────────────────────────────────────────────────────────────────────────────
# Public API
# ──────────────────────────────────────────────────────────────────────────────
def normalize_mint(m: str) -> str:
    """
    Return a canonical mint address suitable for router / Jupiter / Solana RPC.

    Accepts:
      - Symbols: SOL, WSOL, USDT, USDC, BONK, etc.
      - Exact base58-ish address (lenient: 28–40 decoded bytes).
      - Pump.fun / dexscreener-style URLs or strings with a mint inside.
      - Salted IDs where someone tacked on words like '...bonk'/'...pump'
        or '...raydium' to the end.

    If we cannot confidently recover a base58-ish mint, we raise ValueError.
    """
    if not m or not str(m).strip():
        raise ValueError("empty mint")

    s = str(m).strip()

    # 1) Symbol alias
    sym = s.upper()
    if sym in _SYM_TO_MINT:
        return _SYM_TO_MINT[sym]

    # 2) Try to recover a base58-ish mint from the blob
    cand = _extract_base58_32(s)
    if not cand:
        raise ValueError(f"cannot normalize mint from input: {s!r}")

    return cand


def is_base58_mint(s: str) -> bool:
    """
    Public helper for quick validation.

    Uses lenient charset+length rules; anything this returns True for will
    *likely* be accepted by normalize_mint().
    """
    return _looks_base58ish((s or "").strip())


__all__ = [
    "normalize_mint",
    "is_base58_mint",
    "SOL_MINT",
    "USDT_MINT",
    "USDC_MINT",
    "BONK_MINT",
]
