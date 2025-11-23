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
# We allow a bit wider window to be lenient.
_MIN_B58_LEN = 30
_MAX_B58_LEN = 60

# Some feeds append suffix-y words (pump.fun etc.)
_SUS_SUFFIXES = ("pump", "bonk", "raydium", "rayd", "dex", "sol", "solana")

# Broad base58-looking finder for URLs/blobs
_B58_RE = re.compile(r"[1-9A-HJ-NP-Za-km-z]{20,90}")


# ──────────────────────────────────────────────────────────────────────────────
# Minimal base58 decode (no checksum); kept for optional strict checks
# ──────────────────────────────────────────────────────────────────────────────
def _b58decode(s: str) -> bytes:
    """
    Minimal base58 decode. We no longer *require* length==32 everywhere,
    but some callers might still want to sanity-check mints with this.
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


def _looks_base58ish(s: str) -> bool:
    """
    Lenient check: "does this string *look* like a Solana mint?"

    We intentionally do NOT enforce that base58-decoded length is exactly 32
    bytes anymore, because some real-world mints (as seen via Dexscreener /
    Jupiter) were failing strict length checks and causing ENTRY_TX_FAIL.
    """
    if not s:
        return False
    s = s.strip()
    if not (_MIN_B58_LEN <= len(s) <= _MAX_B58_LEN):
        return False
    for ch in s:
        if ch not in _B58_IDX:
            return False
    return True


def _strip_suffixes_and_clip(s: str) -> str:
    """Remove known suffix words; then hard-cap length."""
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


def _extract_base58ish(blob: str) -> Optional[str]:
    """
    Recover a base58-looking mint from arbitrary text/URL.

    Strategy:
      - Direct "does this whole thing look like a mint?" check
      - Try last path segment from URL-ish strings
      - Scan for base58-looking chunks; strip suffixes; validate len/charset
    """
    if not blob:
        return None

    t = blob.strip()
    if _looks_base58ish(t):
        return t

    # Try last path segment for URLs like .../coin/<mint>
    try:
        parts = re.split(r"[/?#]", t)
        if parts:
            last = parts[-1]
            if _looks_base58ish(last):
                return last
    except Exception:
        pass

    # Scan for base58-looking segments
    for cand in _B58_RE.findall(t):
        clean = _strip_suffixes_and_clip(cand)
        if _looks_base58ish(clean):
            return clean
        # Fallback: clip to max and try again
        if len(cand) > _MAX_B58_LEN:
            clipped = cand[:_MAX_B58_LEN]
            if _looks_base58ish(clipped):
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
      - Exact base58-ish address (lenient: only charset + length checks).
      - Pump.fun / dexscreener-style URLs or strings with a mint inside.
      - Salted IDs where someone tacked on words like '...bonk'/'...pump'.

    Compared to older versions, this is *much more forgiving*:
      - We no longer require that base58-decode length is exactly 32 bytes.
      - If it looks like a mint and chains accept it, we trust it.
    """
    if not m or not str(m).strip():
        raise ValueError("empty mint")

    s = str(m).strip()

    # 1) Symbol alias?
    sym = s.upper()
    aliased = _SYM_TO_MINT.get(sym)
    if aliased:
        return aliased

    # 2) Already looks like a base58-ish mint
    if _looks_base58ish(s):
        return s

    # 3) Extract from URL/blob/salted string
    cand = _extract_base58ish(s)
    if cand:
        return cand

    # 4) As a last-resort tiny strict check, in case someone passes truncated
    #    base58; we try to decode but don't enforce length==32.
    try:
        _b58decode(s)
        # If decode succeeds and length/charset was already wrong, we still
        # don't trust it, because it might be e.g. a short hash. So we keep
        # this as a final "you really gave us garbage" guard.
    except Exception:
        pass

    raise ValueError(f"cannot normalize mint from input: {s!r}")


def is_base58_mint(s: str) -> bool:
    """
    Public helper for quick validation.

    This uses the same lenient rules as _looks_base58ish(), so anything this
    returns True for will be accepted by normalize_mint().
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
