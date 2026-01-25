# scripts/pp_utils.py
from __future__ import annotations

"""
PumpPortal utilities (P0-safe)
==============================

This file exists to provide **tiny, dependency-free** helpers for dealing with
PumpPortal websocket “mint” / id strings and extracting *real* Solana mints.

Design goals
------------
- **Strict by default**: a Solana mint/pubkey is 32 bytes → Base58-decodes to 32 bytes.
- **Robust to PumpPortal suffixes**: e.g. "...pump" / "...bonk" sometimes appended.
- **Robust to messy payloads**: if given a larger string, we can best-effort extract
  a Base58-like pubkey inside it.
- **Zero external deps**: no base58 package required.

Public API
----------
- normalize_mint(x) -> str | None
- normalize_mint_lenient(x) -> str | None     (fallback if strict decode is not possible)
- extract_mint_from_text(text) -> str | None  (best-effort find a pubkey in a blob)
- is_base58(s) -> bool
- b58_decode(s) -> bytes                      (minimal implementation)
"""

from collections.abc import Sequence
from typing import Any

# Base58 alphabet used by Solana (Bitcoin alphabet) — no 0, O, I, l
B58_ALPHABET: str = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
B58_SET = set(B58_ALPHABET)
B58_INDEX = {c: i for i, c in enumerate(B58_ALPHABET)}

# PumpPortal-style suffixes we’ve observed
_PP_SUFFIXES: Sequence[str] = ("pump", "bonk")


# ──────────────────────────────────────────────────────────────
# Base58 (minimal) + validation
# ──────────────────────────────────────────────────────────────


def is_base58(s: str) -> bool:
    """True if s is non-empty and contains only Base58 characters."""
    return isinstance(s, str) and bool(s) and (set(s) <= B58_SET)


def b58_decode(s: str) -> bytes:
    """
    Decode a Base58 string to bytes (minimal, dependency-free).
    Raises ValueError on invalid characters.

    Note: Solana pubkeys decode to 32 bytes.
    """
    if not isinstance(s, str) or not s:
        raise ValueError("empty base58 string")

    num = 0
    for ch in s:
        try:
            num = num * 58 + B58_INDEX[ch]
        except KeyError as e:
            raise ValueError(f"invalid base58 char: {ch!r}") from e

    # Convert integer to bytes
    full = num.to_bytes((num.bit_length() + 7) // 8 or 1, byteorder="big")

    # Leading '1' chars represent leading zero bytes
    n_pad = 0
    for ch in s:
        if ch == "1":
            n_pad += 1
        else:
            break

    return b"\x00" * n_pad + full.lstrip(b"\x00") if num != 0 else b"\x00" * n_pad


def _decodes_to_32_bytes(s: str) -> bool:
    """Strict Solana pubkey check."""
    if not is_base58(s):
        return False
    # Common base58 lengths for 32-byte pubkeys are typically 43–44 chars,
    # but we validate by decoding length (true source of truth).
    try:
        return len(b58_decode(s)) == 32
    except Exception:
        return False


# ──────────────────────────────────────────────────────────────
# PumpPortal normalization
# ──────────────────────────────────────────────────────────────


def _strip_suffixes(s: str) -> Sequence[str]:
    """
    Generate candidate strings by stripping known suffixes.
    We only include stripped candidates if the string actually endswith suffix.
    """
    out = [s]
    for suf in _PP_SUFFIXES:
        if s.endswith(suf) and len(s) > len(suf) + 8:  # basic sanity
            out.append(s[: -len(suf)])
    # Dedup while preserving order
    seen = set()
    uniq = []
    for x in out:
        if x not in seen:
            uniq.append(x)
            seen.add(x)
    return uniq


def normalize_mint(x: Any) -> str | None:
    """
    Strict mint normalization.

    - Accepts strings only (others -> None)
    - Trims whitespace
    - Tries raw and suffix-stripped variants
    - Requires Base58 decode length == 32 bytes (Solana pubkey)
    """
    if not isinstance(x, str):
        return None

    s = x.strip()
    if not s:
        return None

    # Fast path: strict decode
    if _decodes_to_32_bytes(s):
        return s

    # Try suffix-stripped candidates
    for cand in _strip_suffixes(s):
        if cand == s:
            continue
        cand = cand.strip()
        if _decodes_to_32_bytes(cand):
            return cand

    # Try extracting a pubkey from messy text
    return extract_mint_from_text(s)


def normalize_mint_lenient(x: Any) -> str | None:
    """
    Lenient mint normalization (fallback).

    Use this only if you *must* accept non-strict “mint-like” ids.

    Rules:
    - Try strict normalize first
    - Else strip suffixes and accept base58-only strings length 32..44
    """
    m = normalize_mint(x)
    if m:
        return m

    if not isinstance(x, str):
        return None
    s = x.strip()
    if not s:
        return None

    for cand in _strip_suffixes(s):
        cand = cand.strip()
        if 32 <= len(cand) <= 44 and is_base58(cand):
            return cand
    return None


def extract_mint_from_text(text: str) -> str | None:
    """
    Best-effort extraction of a Solana pubkey from a blob of text.

    Strategy:
    - Scan for Base58 substrings of plausible length (32..60)
    - Prefer candidates that decode to exactly 32 bytes
    - If multiple, prefer the shortest (most canonical) representation
    """
    if not isinstance(text, str):
        return None

    s = text.strip()
    if not s:
        return None

    # If the string itself is base58-ish but not strict, still consider it
    candidates: list[str] = []
    if is_base58(s) and 32 <= len(s) <= 60:
        candidates.extend(_strip_suffixes(s))

    # Sliding scan: collect contiguous base58 runs
    run = []
    for ch in s:
        if ch in B58_SET:
            run.append(ch)
        else:
            if run:
                chunk = "".join(run)
                if 32 <= len(chunk) <= 60:
                    candidates.append(chunk)
                run = []
    if run:
        chunk = "".join(run)
        if 32 <= len(chunk) <= 60:
            candidates.append(chunk)

    # Also try suffix stripping on any found candidates
    expanded: list[str] = []
    for c in candidates:
        expanded.extend(_strip_suffixes(c))

    # Dedup
    seen = set()
    uniq = []
    for c in expanded:
        c = c.strip()
        if c and c not in seen:
            uniq.append(c)
            seen.add(c)

    # Prefer strict 32-byte decodes
    strict = [c for c in uniq if _decodes_to_32_bytes(c)]
    if strict:
        strict.sort(key=len)
        return strict[0]

    return None


__all__ = [
    "B58_ALPHABET",
    "B58_SET",
    "b58_decode",
    "extract_mint_from_text",
    "is_base58",
    "normalize_mint",
    "normalize_mint_lenient",
]
