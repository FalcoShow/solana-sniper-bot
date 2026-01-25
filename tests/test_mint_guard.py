# tests/test_mint_guard.py
from __future__ import annotations

"""
tests/test_mint_guard.py — V3+++ Mint Guard LOCK-IN (P0)
========================================================

Goal
----
Lock the canonical mint validation + normalization layer:

  - guard_mint(): normalize + validate mint inputs (symbols/pubkeys)
  - guard_pair(): validate trading pairs + optional anchor requirements
  - guard_jupiter_quote_args(): safe args for router/quote path
  - is_non_mint_address(): rejects known program IDs / non-mint addresses

Hard rules
----------
- Deterministic (no network).
- Hermetic env (tests must not depend on local secrets).
- Strict error codes (prevents drift across pipeline).

Expected error codes (LOCKED)
-----------------------------
normalize_failed
non_mint_program_id
same_mints
no_anchor
not_in_allowlist
denylisted
sol_blocked
stable_blocked
meme_blocked
bad_amount
"""

import hashlib
import importlib
import inspect
from collections.abc import Callable, Sequence
from typing import Any

import pytest

import mint_guard as mg


def _mg():
    return importlib.import_module("utils_pkg.mint_guard")


import contextlib

from utils_pkg.mints import BONK_MINT, SOL_MINT, USDC_MINT, USDT_MINT

BASE58_ALPHABET = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"


def _b58_from_seed(seed: str, n: int = 44) -> str:
    """Deterministic base58-ish pubkey generator (no deps, no network)."""
    h = hashlib.sha256(seed.encode("utf-8", "ignore")).digest()
    out: list[str] = []
    i = 0
    while len(out) < n:
        b = h[i % len(h)]
        out.append(BASE58_ALPHABET[b % len(BASE58_ALPHABET)])
        i += 1
        if i % len(h) == 0:
            h = hashlib.sha256(h).digest()
    return "".join(out[:n])


NON_ANCHOR_A = _b58_from_seed("non_anchor_a", 44)
NON_ANCHOR_B = _b58_from_seed("non_anchor_b", 44)


# ──────────────────────────────────────────────────────────────────────────────
# Test helpers
# ──────────────────────────────────────────────────────────────────────────────
def _assert_mge(excinfo: pytest.ExceptionInfo, code: str) -> None:
    e = excinfo.value
    assert isinstance(e, mg.MintGuardError)
    got = getattr(e, "code", None)
    assert got == code, f"expected code={code!r}, got {got!r} ({e})"


def _call_filtered(fn: Callable[..., Any], /, **kwargs: Any) -> Any:
    """
    Call fn with only kwargs it actually accepts (signature-drift tolerant),
    but keep tests strict on the *behavior* and error codes.
    """
    try:
        sig = inspect.signature(fn)
    except Exception:
        # If we cannot introspect, just try raw call.
        return fn(**kwargs)

    accepted = {}
    for k, v in kwargs.items():
        if k in sig.parameters:
            accepted[k] = v
    return fn(**accepted)


def _call_pair(
    a: str,
    b: str,
    *,
    require_anchor: bool = True,
    anchors: Sequence[str] | None = None,
) -> tuple[str, str]:
    return _call_filtered(
        mg.guard_pair, a=a, b=b, mint_a=a, mint_b=b, require_anchor=require_anchor, anchors=anchors
    )


def _call_guard_mint(m: str, **kwargs: Any) -> str:
    # allow multiple common parameter names (keep it resilient)
    return _call_filtered(mg.guard_mint, mint=m, m=m, x=m, **kwargs)


def _call_quote_args(
    in_mint: str,
    out_mint: str,
    *,
    amount_raw: Any,
    require_anchor: bool | None = None,
) -> tuple[str, str, int]:
    kw: dict[str, Any] = {"in_mint": in_mint, "out_mint": out_mint, "amount_raw": amount_raw}
    if require_anchor is not None:
        kw["require_anchor"] = require_anchor
    return _call_filtered(mg.guard_jupiter_quote_args, **kw)


# ──────────────────────────────────────────────────────────────────────────────
# Fixtures: hermetic environment + optional cache reset
# ──────────────────────────────────────────────────────────────────────────────
@pytest.fixture(autouse=True)
def _clean_env(monkeypatch: pytest.MonkeyPatch) -> None:
    # Keep tests hermetic regardless of local/CI env.
    for k in (
        "MINT_GUARD_DENYLIST",
        "MINT_GUARD_ALLOWLIST",
        "MINT_GUARD_NON_MINTS",
    ):
        monkeypatch.delenv(k, raising=False)

    # If mint_guard has any module-level parsed cache, clear it best-effort.
    for attr in (
        "_ENV_CACHE",
        "_PARSED_ENV",
        "_CACHE",
        "_ALLOW_CACHE",
        "_DENY_CACHE",
        "_NON_MINT_CACHE",
    ):
        if hasattr(mg, attr):
            with contextlib.suppress(Exception):
                setattr(mg, attr, None)


# ──────────────────────────────────────────────────────────────────────────────
# Basics: normalization + sanity
# ──────────────────────────────────────────────────────────────────────────────
def test_guard_mint_accepts_canonical_pubkeys() -> None:
    assert _call_guard_mint(SOL_MINT) == SOL_MINT
    assert _call_guard_mint(USDC_MINT) == USDC_MINT
    assert _call_guard_mint(USDT_MINT) == USDT_MINT


def test_guard_mint_strips_whitespace_and_normalizes_case() -> None:
    assert _call_guard_mint("  SOL  ") == SOL_MINT
    assert _call_guard_mint("sol") == SOL_MINT
    assert _call_guard_mint("  usdc") == USDC_MINT
    assert _call_guard_mint("USDT  ") == USDT_MINT


def test_guard_mint_rejects_garbage_input() -> None:
    with pytest.raises(mg.MintGuardError) as ei:
        _call_guard_mint("not a mint")
    _assert_mge(ei, "normalize_failed")


def test_is_non_mint_address_blocks_system_program() -> None:
    # System Program (32 x '1') — should always be treated as non-mint.
    sys_prog = "11111111111111111111111111111111"
    assert mg.is_non_mint_address(sys_prog) is True
    with pytest.raises(mg.MintGuardError) as ei:
        _call_guard_mint(sys_prog)
    _assert_mge(ei, "non_mint_program_id")


def test_env_non_mints_blocks_custom_addresses(monkeypatch: pytest.MonkeyPatch) -> None:
    # If user adds custom non-mints, guard must block them.
    monkeypatch.setenv("MINT_GUARD_NON_MINTS", NON_ANCHOR_A)
    assert mg.is_non_mint_address(NON_ANCHOR_A) is True
    with pytest.raises(mg.MintGuardError) as ei:
        _call_guard_mint(NON_ANCHOR_A)
    _assert_mge(ei, "non_mint_program_id")


# ──────────────────────────────────────────────────────────────────────────────
# Pair rules: distinct + anchor requirement
# ──────────────────────────────────────────────────────────────────────────────
def test_guard_pair_requires_distinct_mints_by_default() -> None:
    with pytest.raises(mg.MintGuardError) as ei:
        _call_pair(SOL_MINT, SOL_MINT, require_anchor=False)
    _assert_mge(ei, "same_mints")


def test_guard_pair_anchor_rule_default_set() -> None:
    # Two non-anchors should fail when require_anchor=True
    with pytest.raises(mg.MintGuardError) as ei:
        _call_pair(NON_ANCHOR_A, NON_ANCHOR_B, require_anchor=True)
    _assert_mge(ei, "no_anchor")

    # Any side anchor passes
    a, b = _call_pair(SOL_MINT, NON_ANCHOR_A, require_anchor=True)
    assert a == SOL_MINT and b == NON_ANCHOR_A


def test_guard_pair_custom_anchor_list() -> None:
    # Use BONK as the only anchor for this test
    anchors = [BONK_MINT]

    with pytest.raises(mg.MintGuardError) as ei:
        _call_pair(SOL_MINT, NON_ANCHOR_A, require_anchor=True, anchors=anchors)
    _assert_mge(ei, "no_anchor")

    a, b = _call_pair(BONK_MINT, NON_ANCHOR_A, require_anchor=True, anchors=anchors)
    assert a == BONK_MINT and b == NON_ANCHOR_A


# ──────────────────────────────────────────────────────────────────────────────
# Allow/Deny lists (env + extra_*)
# ──────────────────────────────────────────────────────────────────────────────
def test_env_allowlist_is_authoritative(monkeypatch: pytest.MonkeyPatch) -> None:
    # allowlist set -> mint must be in allowlist
    monkeypatch.setenv("MINT_GUARD_ALLOWLIST", "USDC")
    assert _call_guard_mint(USDC_MINT) == USDC_MINT

    with pytest.raises(mg.MintGuardError) as ei:
        _call_guard_mint(SOL_MINT)
    _assert_mge(ei, "not_in_allowlist")


def test_env_allowlist_parsing_tolerates_spaces_commas_newlines(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("MINT_GUARD_ALLOWLIST", "  SOL,\nUSDC ,  USDT ")
    assert _call_guard_mint("SOL") == SOL_MINT
    assert _call_guard_mint("USDC") == USDC_MINT
    assert _call_guard_mint("USDT") == USDT_MINT


def test_env_denylist_blocks(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MINT_GUARD_DENYLIST", "SOL")  # symbols should be accepted as inputs
    with pytest.raises(mg.MintGuardError) as ei:
        _call_guard_mint(SOL_MINT)
    _assert_mge(ei, "denylisted")


def test_deny_overrides_allow(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MINT_GUARD_ALLOWLIST", "SOL,USDC")
    monkeypatch.setenv("MINT_GUARD_DENYLIST", "SOL")

    with pytest.raises(mg.MintGuardError) as ei:
        _call_guard_mint("SOL")
    _assert_mge(ei, "denylisted")

    # USDC should still be allowed
    assert _call_guard_mint("USDC") == USDC_MINT


def test_extra_allow_and_extra_deny() -> None:
    # extra_allow: only allow SOL
    assert _call_guard_mint("SOL", extra_allow=["SOL"]) == SOL_MINT
    with pytest.raises(mg.MintGuardError) as ei:
        _call_guard_mint("USDC", extra_allow=["SOL"])
    _assert_mge(ei, "not_in_allowlist")

    # extra_deny: block USDC
    with pytest.raises(mg.MintGuardError) as ei2:
        _call_guard_mint("USDC", extra_deny=["USDC"])
    _assert_mge(ei2, "denylisted")


# ──────────────────────────────────────────────────────────────────────────────
# Strategy toggles
# ──────────────────────────────────────────────────────────────────────────────
def test_allow_sol_toggle_blocks_sol() -> None:
    with pytest.raises(mg.MintGuardError) as ei:
        _call_guard_mint("SOL", allow_sol=False)
    _assert_mge(ei, "sol_blocked")


def test_allow_stables_toggle_blocks_usdc_usdt() -> None:
    with pytest.raises(mg.MintGuardError) as ei:
        _call_guard_mint("USDC", allow_stables=False)
    _assert_mge(ei, "stable_blocked")

    with pytest.raises(mg.MintGuardError) as ei2:
        _call_guard_mint("USDT", allow_stables=False)
    _assert_mge(ei2, "stable_blocked")


def test_allow_memes_toggle_blocks_bonk() -> None:
    # BONK is the only meme constant we lock into this suite.
    with pytest.raises(mg.MintGuardError) as ei:
        _call_guard_mint(BONK_MINT, allow_memes=False)
    _assert_mge(ei, "meme_blocked")


# ──────────────────────────────────────────────────────────────────────────────
# Jupiter quote arg helper
# ──────────────────────────────────────────────────────────────────────────────
def test_guard_jupiter_quote_args_happy_path() -> None:
    a, b, amt = _call_quote_args("SOL", "USDC", amount_raw=12345)
    assert a == SOL_MINT
    assert b == USDC_MINT
    assert amt == 12345


def test_guard_jupiter_quote_args_rejects_bad_amount() -> None:
    for bad in (0, -1):
        with pytest.raises(mg.MintGuardError) as ei:
            _call_quote_args("SOL", "USDC", amount_raw=bad)
        _assert_mge(ei, "bad_amount")

    with pytest.raises(mg.MintGuardError) as ei2:
        _call_quote_args("SOL", "USDC", amount_raw="123")  # type: ignore[arg-type]
    _assert_mge(ei2, "bad_amount")


def test_guard_jupiter_quote_args_anchor_requirement_default() -> None:
    # default require_anchor=True → non-anchor pair should fail
    with pytest.raises(mg.MintGuardError) as ei:
        _call_quote_args(NON_ANCHOR_A, NON_ANCHOR_B, amount_raw=1)
    _assert_mge(ei, "no_anchor")

    # if require_anchor=False → ok (if function supports it)
    a, b, amt = _call_quote_args(NON_ANCHOR_A, NON_ANCHOR_B, amount_raw=1, require_anchor=False)
    assert a == NON_ANCHOR_A
    assert b == NON_ANCHOR_B
    assert amt == 1
