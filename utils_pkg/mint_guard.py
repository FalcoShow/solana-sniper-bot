# utils_pkg/mint_guard.py
from __future__ import annotations
import re
from typing import Iterable, List

# Base58 (no 0 O I l) — allow 32..44 chars to cover typical Solana pubkeys
_BASE58_WHOLE_RE = re.compile(r"^[1-9A-HJ-NP-Za-km-z]{32,44}$")
_BASE58_CHUNK_RE = re.compile(r"[A-Za-z0-9]{32,44}")

# Optional hook a router might provide; tests may set this to None
_router_is_valid = None  # type: ignore


def _is_valid_pk(s: str) -> bool:
    """Lightweight validity check; tests may monkeypatch this."""
    if not s:
        return False
    return bool(_BASE58_WHOLE_RE.fullmatch(s))


def is_probable_mint(s: str) -> bool:
    """
    Return True if s *looks like* a Solana mint/pubkey.
    If a router validator is present, prefer it; otherwise fall back to regex.
    """
    if not s:
        return False
    # Prefer external validator if available
    try:
        if callable(_router_is_valid):  # type: ignore
            return bool(_router_is_valid(s))  # type: ignore
    except Exception:
        pass
    # Fallback: regex shape
    return _is_valid_pk(s)


def _dedupe_preserve_order(xs: Iterable[str]) -> List[str]:
    seen = set()
    out: List[str] = []
    for x in xs:
        if x not in seen:
            seen.add(x)
            out.append(x)
    return out


def normalize_from_any(raw: str) -> str:
    """
    Pull a single mint from an arbitrary string.
    - If none found → return "".
    - If exactly one found → return it.
    - If multiple valid candidates → raise ValueError (ambiguous).
    Tests monkeypatch _is_valid_pk to force 'valid' for chunks.
    """
    text = (raw or "").strip()
    if not text:
        return ""

    # collect base58-like chunks and validate them
    chunks = _BASE58_CHUNK_RE.findall(text)
    if not chunks:
        return ""

    candidates = _dedupe_preserve_order([c for c in chunks if _is_valid_pk(c)])

    if not candidates:
        return ""

    if len(candidates) > 1:
        # Ambiguous on purpose (tests expect an exception here)
        raise ValueError("ambiguous: multiple candidates found")

    return candidates[0]
