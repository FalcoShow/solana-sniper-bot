# utils_pkg/mint_guard.py
from __future__ import annotations

"""
utils_pkg/mint_guard.py — God-Tier Mint Guardrails (V3+++ LOCK-IN)
==================================================================

Purpose
-------
Hard guardrails around mint inputs before they ever hit Jupiter/router:

✅ Normalize ANY input into canonical mint (via utils_pkg.mints.normalize_mint)
✅ Denylist/allowlist support (symbols or pubkeys)
✅ Reject obvious non-mint program IDs (system/token/ATA/memo/etc.)
✅ Enforce pair sanity (input != output, optional “must include quote mint”)
✅ Tiny, dependency-free, no network at import

This file is intentionally “boring + strict” to reduce money-losing edge cases.
"""

import os
from dataclasses import dataclass
from typing import Any, Iterable, Optional, Sequence, Set, Tuple

from utils_pkg.mints import (
    BONK_MINT,
    SOL_MINT,
    USDC_MINT,
    USDT_MINT,
    is_base58_pubkey,
    normalize_mint,
    try_normalize_mint,
)

_TRUE = {"1", "true", "t", "yes", "y", "on"}


def _env_bool(name: str, default: bool = False) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return str(v).strip().lower() in _TRUE


def _split_csv(s: str) -> Sequence[str]:
    return [x.strip() for x in (s or "").split(",") if x.strip()]


# Common Solana program IDs / non-mint addresses that must never be treated as token mints.
# (These are stable, widely used constants; blocking them prevents “garbage mints” going to Jupiter.)
_DEFAULT_NON_MINT_PUBKEYS: Set[str] = {
    "11111111111111111111111111111111",  # System Program
    "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA",  # SPL Token Program (legacy)
    "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb",  # SPL Token-2022
    "ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL",  # Associated Token Program
    "MemoSq4gqABAXKb96qnH8TysNcWxMyWCqXgDLGmfcHr",  # Memo program
    "ComputeBudget111111111111111111111111111111",  # Compute budget
    "AddressLookupTab1e1111111111111111111111111",  # Address Lookup Table program (ALT)
}


# Default quote/stable set (safe, liquid anchors)
_DEFAULT_ANCHOR_MINTS: Set[str] = {SOL_MINT, USDC_MINT, USDT_MINT}


@dataclass(frozen=True)
class MintGuardError(ValueError):
    code: str
    detail: str

    def __str__(self) -> str:
        return f"MintGuardError({self.code}): {self.detail}"


def _resolve_list_items(items: Iterable[str]) -> Set[str]:
    """
    Accepts mix of symbols and pubkeys; normalizes what it can.
    Unknown entries are ignored (so config typos do not crash startup).
    """
    out: Set[str] = set()
    for it in items:
        if not it:
            continue
        m = try_normalize_mint(it)
        if m and is_base58_pubkey(m):
            out.add(m)
    return out


def _load_env_sets() -> Tuple[Set[str], Set[str], Set[str]]:
    """
    Returns (deny, allow, non_mint).
    """
    deny_raw = _split_csv(os.getenv("MINT_GUARD_DENYLIST", ""))
    allow_raw = _split_csv(os.getenv("MINT_GUARD_ALLOWLIST", ""))
    nonmint_raw = _split_csv(os.getenv("MINT_GUARD_NON_MINTS", ""))

    deny = _resolve_list_items(deny_raw)
    allow = _resolve_list_items(allow_raw)

    non_mint = set(_DEFAULT_NON_MINT_PUBKEYS)
    non_mint |= _resolve_list_items(nonmint_raw)  # allow adding more blocks
    return deny, allow, non_mint


def is_non_mint_address(mint: str) -> bool:
    _, _, non_mint = _load_env_sets()
    return mint in non_mint


def guard_mint(
    x: Any,
    *,
    allow_sol: bool = True,
    allow_stables: bool = True,
    allow_memes: bool = True,
    extra_deny: Optional[Sequence[str]] = None,
    extra_allow: Optional[Sequence[str]] = None,
) -> str:
    """
    Normalize + enforce hard guardrails.

    - allowlist (if present) is authoritative: mint must be in allowlist.
    - denylist always blocks.
    - rejects obvious non-mint program IDs.
    - optionally blocks SOL/stables/memes if your strategy wants that.

    Returns canonical mint (string) or raises MintGuardError.
    """
    deny, allow, non_mint = _load_env_sets()

    if extra_deny:
        deny |= _resolve_list_items(extra_deny)
    if extra_allow:
        allow |= _resolve_list_items(extra_allow)

    try:
        mint = normalize_mint(x)
    except Exception as e:  # noqa: BLE001
        raise MintGuardError("normalize_failed", f"{e!r}") from e

    if not is_base58_pubkey(mint):
        raise MintGuardError("not_pubkey", f"normalized mint is not strict pubkey: {mint!r}")

    if mint in non_mint:
        raise MintGuardError("non_mint_program_id", f"address is a program id, not a mint: {mint}")

    if allow:
        if mint not in allow:
            raise MintGuardError("not_in_allowlist", f"mint={mint} not allowed")

    if mint in deny:
        raise MintGuardError("denylisted", f"mint={mint} is denylisted")

    if not allow_sol and mint == SOL_MINT:
        raise MintGuardError("sol_blocked", "SOL mint blocked by config")

    if not allow_stables and mint in (USDC_MINT, USDT_MINT):
        raise MintGuardError("stable_blocked", "stable mint blocked by config")

    if not allow_memes and mint == BONK_MINT:
        raise MintGuardError("meme_blocked", "meme mint blocked by config")

    return mint


def guard_pair(
    input_mint: Any,
    output_mint: Any,
    *,
    require_distinct: bool = True,
    require_anchor: bool = False,
    anchors: Optional[Sequence[str]] = None,
    **mint_kwargs: Any,
) -> Tuple[str, str]:
    """
    Guard a trade pair (input/output).

    - Normalizes and validates both sides.
    - Optionally requires input != output.
    - Optionally requires at least one side is an “anchor” (SOL/USDC/USDT by default).

    Returns (input_mint_canon, output_mint_canon).
    """
    a = guard_mint(input_mint, **mint_kwargs)
    b = guard_mint(output_mint, **mint_kwargs)

    if require_distinct and a == b:
        raise MintGuardError("same_mints", f"input==output ({a})")

    if require_anchor:
        anchor_set = _resolve_list_items(anchors or [])
        if not anchor_set:
            anchor_set = set(_DEFAULT_ANCHOR_MINTS)
        if (a not in anchor_set) and (b not in anchor_set):
            raise MintGuardError("no_anchor", f"pair lacks anchor mint (anchors={sorted(anchor_set)})")

    return a, b


def guard_jupiter_quote_args(
    input_mint: Any,
    output_mint: Any,
    *,
    amount_raw: int,
    require_anchor: bool = True,
    **mint_kwargs: Any,
) -> Tuple[str, str, int]:
    """
    Convenience guard used right before calling Jupiter / router.

    Ensures:
      - canonical mints
      - sane pair
      - amount_raw is positive int

    Returns (input_mint, output_mint, amount_raw).
    """
    if not isinstance(amount_raw, int) or amount_raw <= 0:
        raise MintGuardError("bad_amount", f"amount_raw must be positive int, got {amount_raw!r}")

    a, b = guard_pair(input_mint, output_mint, require_anchor=require_anchor, **mint_kwargs)
    return a, b, amount_raw


__all__ = [
    "MintGuardError",
    "is_non_mint_address",
    "guard_mint",
    "guard_pair",
    "guard_jupiter_quote_args",
]
