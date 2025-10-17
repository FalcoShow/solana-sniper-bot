# utils_pkg/solana_token.py
from __future__ import annotations

from typing import Optional, Tuple

from utils_pkg.txlearn import _Rpc, RPC_URL, TX_COMMITMENT  # reuse lightweight RPC

TOKEN_PROGRAM_LEGACY = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
TOKEN_PROGRAM_2022   = "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb"
NATIVE_SOL_MINT      = "So11111111111111111111111111111111111111112"


async def _get_account_parsed(rpc: _Rpc, addr: str) -> Optional[dict]:
    """
    Best-effort fetch of a single account in jsonParsed form.
    Tries getMultipleAccounts first (vectorized path), then getAccountInfo.
    Returns the parsed account *object* (not the outer {"value": ...} wrapper).
    """
    # 1) Vectorized path
    try:
        res = await rpc.get_multiple_accounts([addr])
        v = (res or {}).get("value") or []
        acc = v[0] if v else None
        # For getMultipleAccounts, `acc` is already the account dict with "data"
        if acc and isinstance(acc.get("data"), dict):
            return acc
    except Exception:
        pass

    # 2) Fallback to getAccountInfo
    try:
        res = await rpc.get_account_info(addr)
        val = (res or {}).get("value")
        # Here `val` is the account dict with "data"
        if val and isinstance(val.get("data"), dict):
            return val
    except Exception:
        pass

    return None


def classify_token_program(owner_pid: str) -> str:
    """Map program id -> human string for router/db."""
    if owner_pid == TOKEN_PROGRAM_2022:
        return "token-2022"
    return "spl-token"  # default / legacy


async def fetch_mint_info(mint: str, rpc_url: str | None = None) -> Optional[Tuple[int, str]]:
    """
    Returns (decimals, token_program_owner) for an SPL mint account.
    If the account is not a valid mint (or not yet created), returns None.
    """
    if not mint or mint == NATIVE_SOL_MINT:
        return None

    rpc = _Rpc(rpc_url or RPC_URL)
    acc = await _get_account_parsed(rpc, mint)
    if not acc:
        return None

    # Expect jsonParsed → parsed.type == "mint"
    try:
        owner = str(acc.get("owner") or "")
        data = acc.get("data") or {}
        parsed = data.get("parsed") or {}
        if parsed.get("type") != "mint":
            return None
        info = parsed.get("info") or {}
        dec = int(info.get("decimals", 0))
        return dec, owner
    except Exception:
        return None


async def is_valid_mint(mint: str, rpc_url: str | None = None) -> bool:
    """Quick validity check using jsonParsed account.type == 'mint'."""
    try:
        return (await fetch_mint_info(mint, rpc_url=rpc_url)) is not None
    except Exception:
        return False


def ui_from_raw(amount_raw: int, decimals: int) -> float:
    """Convert raw integer tokens → UI float."""
    if decimals <= 0:
        return float(amount_raw)
    return float(amount_raw) / float(10 ** int(decimals))


__all__ = [
    "TOKEN_PROGRAM_LEGACY",
    "TOKEN_PROGRAM_2022",
    "NATIVE_SOL_MINT",
    "classify_token_program",
    "fetch_mint_info",
    "is_valid_mint",
    "ui_from_raw",
]
