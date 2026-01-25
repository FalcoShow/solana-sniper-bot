# utils_pkg/solana_token.py
from __future__ import annotations

"""
utils_pkg/solana_token.py — SPL Token primitives (decimals, ATA, balances, conversions)
=====================================================================================

LOCK-IN GOALS (P0 / Chain primitives)
-------------------------------------
✅ Canonical mint metadata: decimals + token program owner (spl-token vs token-2022)
✅ Canonical ATA derivation (supports token-2022 seed)
✅ Safe, bounded caches (TTL + max size + inflight dedupe)
✅ Best-effort RPC reads via utils_pkg.solana_wallet failover (NO network at import)
✅ Backwards-compatible helpers:
   - fetch_mint_info(), get_mint_info(), get_token_decimals(), get_decimals()
   - ui_from_raw(), raw_from_ui()
   - is_valid_mint()
✅ Useful extras:
   - get_associated_token_address()
   - get_token_accounts_by_owner() / get_token_balance_by_owner()
   - lamports<->sol helpers

Notes
-----
- This module does NOT create ATAs or build token instructions. Execution layer handles that.
- RPC parsing is defensive: supports jsonParsed when available, otherwise returns None.
"""

import asyncio
import contextlib
import time
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any, Union

# solders primitives
from solders.pubkey import Pubkey

# Canonical RPC pipe + wallet primitives
from utils_pkg import solana_wallet as sw

# ──────────────────────────────────────────────────────────────────────────────
# Program IDs / constants
# ──────────────────────────────────────────────────────────────────────────────
TOKEN_PROGRAM_LEGACY = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
TOKEN_PROGRAM_2022 = "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb"

ASSOCIATED_TOKEN_PROGRAM = "ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL"
SYSTEM_PROGRAM = "11111111111111111111111111111111"

NATIVE_SOL_MINT = "So11111111111111111111111111111111111111112"
NATIVE_SOL_DECIMALS = 9

LAMPORTS_PER_SOL = 1_000_000_000

# Conservative defaults: mint decimals don't change after creation.
_DEFAULT_TTL_S = 6 * 60 * 60  # 6h
_NEGATIVE_TTL_S = 5 * 60  # 5m for "not a mint"/missing
_MAX_CACHE = 50_000  # safety cap (bounded memory)
_INFLIGHT_WAIT_S = 10.0  # cap for concurrent dedupe waits

# rpc defaults
_RPC_COMMITMENT = "confirmed"


# ──────────────────────────────────────────────────────────────────────────────
# Types
# ──────────────────────────────────────────────────────────────────────────────
PubkeyLike = Union[str, Pubkey]


@dataclass(frozen=True, slots=True)
class MintInfo:
    decimals: int
    owner_program: str  # token program owner pid
    program_kind: str  # "spl-token" | "token-2022" | "unknown"

    def as_tuple(self) -> tuple[int, str]:
        return self.decimals, self.owner_program


@dataclass(frozen=True, slots=True)
class TokenAccount:
    pubkey: str
    mint: str
    owner: str
    amount_raw: int
    decimals: int
    program: str  # token program id


# ──────────────────────────────────────────────────────────────────────────────
# Caches (process-local)
# ──────────────────────────────────────────────────────────────────────────────
# mint -> (expires_at_monotonic, MintInfo|None)
_MINT_CACHE: dict[str, tuple[float, MintInfo | None]] = {}
# mint -> Future[Optional[MintInfo]] (dedupe concurrent fetches)
_INFLIGHT: dict[str, asyncio.Future] = {}
_CACHE_LOCK = asyncio.Lock()


def _now() -> float:
    return time.monotonic()


def classify_token_program(owner_pid: str) -> str:
    if owner_pid == TOKEN_PROGRAM_2022:
        return "token-2022"
    if owner_pid == TOKEN_PROGRAM_LEGACY:
        return "spl-token"
    # Some RPCs return program owner of the mint account; if it's not token
    # programs, it's not a valid SPL mint.
    return "unknown"


def _is_probable_pubkey_str(s: str) -> bool:
    ss = (s or "").strip()
    if not ss:
        return False
    # Lightweight guard only; real validation via Pubkey.from_string()
    return 32 <= len(ss) <= 60


def _to_pubkey(x: PubkeyLike) -> Pubkey:
    if isinstance(x, Pubkey):
        return x
    return Pubkey.from_string(str(x).strip())


def _to_str_pk(x: PubkeyLike) -> str:
    return str(x) if isinstance(x, Pubkey) else str(x).strip()


async def _rpc_read(method: str, params: list) -> Any:
    """
    Canonical read path: use solana_wallet failover (no duplicate networking code).
    """
    fn = getattr(sw, "_rpc_call_failover_async", None)
    if callable(fn):
        return await fn(method, params, kind="read")  # type: ignore[misc]
    # Last-ditch (should not happen if solana_wallet is updated)
    fn2 = getattr(sw, "_rpc_call_failover", None)
    if callable(fn2):
        return await asyncio.to_thread(fn2, method, params, kind="read")  # type: ignore[misc]
    raise RuntimeError("solana_wallet RPC helpers not found")


def _parse_mint_info_from_account_value(val: dict) -> MintInfo | None:
    """
    Parse getAccountInfo.value (jsonParsed) into MintInfo.
    Expected:
      value = {"owner": <token program id>, "data": {"program":"spl-token","parsed":{...}}}
    """
    try:
        owner = str(val.get("owner") or "").strip()
        data = val.get("data")
        if not isinstance(data, dict):
            return None
        parsed = data.get("parsed")
        if not isinstance(parsed, dict):
            return None
        if str(parsed.get("type") or "") != "mint":
            return None
        info = parsed.get("info")
        if not isinstance(info, dict):
            return None
        decimals = int(info.get("decimals", -1))
        if decimals < 0 or decimals > 18:
            return None
        kind = classify_token_program(owner)
        if kind == "unknown":
            # Not an SPL mint (owner isn't token program)
            return None
        return MintInfo(decimals=decimals, owner_program=owner, program_kind=kind)
    except Exception:
        return None


async def _get_account_info_json_parsed(addr: str) -> dict | None:
    """
    Returns getAccountInfo.value dict (jsonParsed) or None.
    """
    if not _is_probable_pubkey_str(addr):
        return None
    try:
        res = await _rpc_read(
            "getAccountInfo",
            [addr, {"encoding": "jsonParsed", "commitment": _RPC_COMMITMENT}],
        )
        if isinstance(res, dict):
            val = res.get("value")
            return val if isinstance(val, dict) else None
        return None
    except Exception:
        return None


async def fetch_mint_info(
    mint: str,
    rpc_url: str | None = None,  # kept for back-compat; ignored (RPC is centralized)
    *,
    rpc: Any | None = None,  # optional back-compat hook (txlearn._Rpc etc.)
) -> tuple[int, str] | None:
    """
    Backwards-compatible API:
    Returns (decimals, token_program_owner) for an SPL mint account.
    Returns None if not a valid mint / not created.

    Note: For SOL (native), returns None by design (use get_token_decimals()).
    """
    _ = rpc_url  # centralized; kept for signature stability
    if not mint or mint == NATIVE_SOL_MINT:
        return None
    mi = await get_mint_info(mint, rpc=rpc)
    return mi.as_tuple() if mi else None


async def get_mint_info(
    mint: str,
    rpc_url: str | None = None,  # kept for signature stability; ignored
    *,
    rpc: Any | None = None,
    cache_ttl_s: float = _DEFAULT_TTL_S,
    negative_ttl_s: float = _NEGATIVE_TTL_S,
    use_cache: bool = True,
) -> MintInfo | None:
    """
    Canonical mint metadata fetcher with TTL cache + in-flight dedupe.

    - Returns MintInfo(decimals, owner_program, program_kind)
    - Returns None if not a valid SPL mint account
    - Best-effort (never raises)
    """
    _ = rpc_url  # centralized; kept for signature stability

    m = (mint or "").strip()
    if not m or m == NATIVE_SOL_MINT:
        return None
    if not _is_probable_pubkey_str(m):
        return None

    # Cache / inflight fast path
    if use_cache:
        async with _CACHE_LOCK:
            hit = _MINT_CACHE.get(m)
            if hit:
                exp, mi = hit
                if exp > _now():
                    return mi
                _MINT_CACHE.pop(m, None)

            fut = _INFLIGHT.get(m)
            if fut:
                try:
                    return await asyncio.wait_for(fut, timeout=_INFLIGHT_WAIT_S)
                except Exception:
                    _INFLIGHT.pop(m, None)

            loop = asyncio.get_running_loop()
            f = loop.create_future()
            _INFLIGHT[m] = f

    mi: MintInfo | None = None
    try:
        # If a caller provides a lightweight RPC client, try it first (back-compat)
        if rpc is not None:
            with contextlib.suppress(Exception):
                # Try common shapes: get_account_info / get_multiple_accounts
                if hasattr(rpc, "get_account_info") and callable(rpc.get_account_info):
                    res = await rpc.get_account_info(m)  # type: ignore[misc]
                    val = (res or {}).get("value")
                    if isinstance(val, dict):
                        mi = _parse_mint_info_from_account_value(val)
                if (
                    mi is None
                    and hasattr(rpc, "get_multiple_accounts")
                    and callable(rpc.get_multiple_accounts)
                ):
                    res = await rpc.get_multiple_accounts([m])  # type: ignore[misc]
                    v = (res or {}).get("value") or []
                    acc = v[0] if v else None
                    if isinstance(acc, dict):
                        mi = _parse_mint_info_from_account_value(acc)

        if mi is None:
            val = await _get_account_info_json_parsed(m)
            if val:
                mi = _parse_mint_info_from_account_value(val)

        return mi
    except Exception:
        return None
    finally:
        if use_cache:
            async with _CACHE_LOCK:
                fut2 = _INFLIGHT.pop(m, None)
                if fut2 and not fut2.done():
                    fut2.set_result(mi)

                # Evict if needed (simple expiry-ordered trim)
                if len(_MINT_CACHE) >= _MAX_CACHE:
                    items = list(_MINT_CACHE.items())
                    items.sort(key=lambda kv: float(kv[1][0]))  # exp asc
                    for k, _v in items[: max(1, len(items) // 100)]:
                        _MINT_CACHE.pop(k, None)

                ttl = float(cache_ttl_s) if mi else float(negative_ttl_s)
                _MINT_CACHE[m] = (_now() + max(2.0, ttl), mi)


# ──────────────────────────────────────────────────────────────────────────────
# Canonical decimals API (use this everywhere)
# ──────────────────────────────────────────────────────────────────────────────
async def get_token_decimals(
    mint: str,
    rpc_url: str | None = None,
    *,
    rpc: Any | None = None,
    default: int | None = None,
) -> int | None:
    """
    Canonical decimals accessor.

    - SOL mint -> 9 (always)
    - SPL mint -> decimals from chain (cached)
    - Unknown/invalid -> default (or None)
    """
    _ = rpc_url
    m = (mint or "").strip()
    if not m:
        return default
    if m == NATIVE_SOL_MINT:
        return NATIVE_SOL_DECIMALS

    mi = await get_mint_info(m, rpc=rpc)
    return mi.decimals if mi else default


# Common aliases (defensive: older code may use these names)
async def get_decimals(
    mint: str, rpc_url: str | None = None, *, rpc: Any | None = None
) -> int | None:
    return await get_token_decimals(mint, rpc_url=rpc_url, rpc=rpc, default=None)


async def get_sol_decimals() -> int:
    return NATIVE_SOL_DECIMALS


async def sol_decimals() -> int:
    return NATIVE_SOL_DECIMALS


async def is_valid_mint(mint: str, rpc_url: str | None = None) -> bool:
    _ = rpc_url
    try:
        return (await get_mint_info(mint)) is not None
    except Exception:
        return False


# ──────────────────────────────────────────────────────────────────────────────
# ATA derivation (legacy + token-2022)
# ──────────────────────────────────────────────────────────────────────────────
def get_associated_token_address(
    owner: PubkeyLike,
    mint: PubkeyLike,
    *,
    token_program_id: str = TOKEN_PROGRAM_LEGACY,
) -> str:
    """
    Deterministic ATA derivation:
      ATA = find_program_address(
              [owner, token_program_id, mint],
              ASSOCIATED_TOKEN_PROGRAM
            )

    For token-2022 mints/accounts, pass token_program_id=TOKEN_PROGRAM_2022.
    """
    o = _to_pubkey(owner)
    m = _to_pubkey(mint)
    tp = _to_pubkey(token_program_id)
    ata_prog = _to_pubkey(ASSOCIATED_TOKEN_PROGRAM)

    seeds = [bytes(o), bytes(tp), bytes(m)]
    ata, _bump = Pubkey.find_program_address(seeds, ata_prog)
    return str(ata)


async def resolve_token_program_for_mint(mint: str) -> str:
    """
    Returns TOKEN_PROGRAM_LEGACY or TOKEN_PROGRAM_2022 for a given SPL mint.
    Defaults to legacy if unknown.
    """
    mi = await get_mint_info(mint)
    if mi and mi.owner_program == TOKEN_PROGRAM_2022:
        return TOKEN_PROGRAM_2022
    return TOKEN_PROGRAM_LEGACY


# ──────────────────────────────────────────────────────────────────────────────
# Token account queries (jsonParsed)
# ──────────────────────────────────────────────────────────────────────────────
def _parse_token_amount(info: dict) -> tuple[int, int]:
    """
    tokenAmount:
      { "amount": "123", "decimals": 6, "uiAmount": 0.000123, "uiAmountString": "0.000123" }
    """
    amt_raw = 0
    dec = 0
    try:
        ta = info.get("tokenAmount") or {}
        if isinstance(ta, dict):
            dec = int(ta.get("decimals") or 0)
            amt_s = ta.get("amount")
            amt_raw = int(ta.get("uiAmount") or 0) if amt_s is None else int(str(amt_s))
    except Exception:
        amt_raw, dec = 0, 0
    return max(0, int(amt_raw)), max(0, int(dec))


def _parse_token_account_item(item: dict, *, program_id: str) -> TokenAccount | None:
    """
    Item from getTokenAccountsByOwner.value:
      {"pubkey": "...", "account": {"data": {"program":"spl-token","parsed":{...}} , ...}}
    """
    try:
        pk = str(item.get("pubkey") or "").strip()
        acc = item.get("account") or {}
        if not pk or not isinstance(acc, dict):
            return None
        data = acc.get("data")
        if not isinstance(data, dict):
            return None
        parsed = data.get("parsed")
        if not isinstance(parsed, dict):
            return None
        if str(parsed.get("type") or "") != "account":
            return None
        info = parsed.get("info")
        if not isinstance(info, dict):
            return None
        mint = str(info.get("mint") or "").strip()
        owner = str(info.get("owner") or "").strip()
        amt_raw, dec = _parse_token_amount(info)
        if not mint or not owner:
            return None
        return TokenAccount(
            pubkey=pk, mint=mint, owner=owner, amount_raw=amt_raw, decimals=dec, program=program_id
        )
    except Exception:
        return None


async def get_token_accounts_by_owner(
    owner: PubkeyLike,
    *,
    mint: str | None = None,
    token_program_id: str | None = None,
) -> list[TokenAccount]:
    """
    Returns parsed token accounts for an owner.

    - If token_program_id is None:
        - If mint provided and we can resolve its program => query that program only
        - Else query BOTH legacy + token-2022 and merge
    - If mint is provided, filters by mint at RPC level (fast path).
    """
    owner_s = _to_str_pk(owner)
    mint_s = (mint or "").strip() or None

    programs: list[str] = []
    if token_program_id:
        programs = [str(token_program_id)]
    else:
        if mint_s:
            programs = [await resolve_token_program_for_mint(mint_s)]
        else:
            programs = [TOKEN_PROGRAM_LEGACY, TOKEN_PROGRAM_2022]

    out: list[TokenAccount] = []
    for prog in programs:
        # RPC filter: either {"mint": mint} OR {"programId": tokenProgram} (or both?)
        # We prefer:
        #   - If mint specified: use mint filter (works for both programs on most RPCs)
        #   - Else: use programId filter to enumerate all token accounts under that program
        filt: dict[str, Any] = {"mint": mint_s} if mint_s else {"programId": prog}
        try:
            res = await _rpc_read(
                "getTokenAccountsByOwner",
                [owner_s, filt, {"encoding": "jsonParsed", "commitment": _RPC_COMMITMENT}],
            )
            vals = (res or {}).get("value") if isinstance(res, dict) else None
            if not isinstance(vals, list):
                continue
            for it in vals:
                if isinstance(it, dict):
                    ta = _parse_token_account_item(it, program_id=prog)
                    if ta:
                        out.append(ta)
        except Exception:
            continue

    return out


async def get_token_balance_by_owner(
    owner: PubkeyLike,
    mint: str,
    *,
    token_program_id: str | None = None,
) -> tuple[int, int | None]:
    """
    Sum of all token accounts for (owner, mint).
    Returns: (amount_raw_sum, decimals_if_known)
    """
    mint_s = (mint or "").strip()
    if not mint_s:
        return 0, None

    accts = await get_token_accounts_by_owner(owner, mint=mint_s, token_program_id=token_program_id)
    if not accts:
        # If owner has no token accounts, decimals might still exist; try mint
        dec = await get_token_decimals(mint_s, default=None)
        return 0, dec

    total = 0
    dec: int | None = None
    for a in accts:
        total += int(a.amount_raw)
        if dec is None:
            dec = int(a.decimals)
    return int(total), dec


# ──────────────────────────────────────────────────────────────────────────────
# Conversions (safe + consistent)
# ──────────────────────────────────────────────────────────────────────────────
def ui_from_raw(amount_raw: int, decimals: int) -> float:
    """Convert raw integer tokens → UI float."""
    try:
        d = int(decimals)
        if d <= 0:
            return float(amount_raw)
        return float(amount_raw) / float(10**d)
    except Exception:
        return float(amount_raw)


def raw_from_ui(amount_ui: float | str | Decimal, decimals: int) -> int:
    """
    Convert UI amount → raw integer amount.
    Uses Decimal for stability. Accepts float/str/Decimal.
    """
    d = int(decimals)
    if d <= 0:
        try:
            return int(Decimal(str(amount_ui)))
        except Exception:
            return int(float(amount_ui))

    try:
        x = amount_ui if isinstance(amount_ui, Decimal) else Decimal(str(amount_ui))
        scale = Decimal(10) ** Decimal(d)
        raw = x * scale
        # round half-up to nearest integer (typical UI→raw expectation)
        return int(raw.to_integral_value(rounding="ROUND_HALF_UP"))
    except (InvalidOperation, ValueError):
        return int(round(float(amount_ui) * float(10**d)))


def lamports_to_sol(lamports: int) -> float:
    try:
        return float(int(lamports)) / float(LAMPORTS_PER_SOL)
    except Exception:
        return 0.0


def sol_to_lamports(sol: float | str | Decimal) -> int:
    try:
        x = sol if isinstance(sol, Decimal) else Decimal(str(sol))
        raw = x * Decimal(LAMPORTS_PER_SOL)
        return int(raw.to_integral_value(rounding="ROUND_HALF_UP"))
    except Exception:
        return int(round(float(sol) * float(LAMPORTS_PER_SOL)))


# ──────────────────────────────────────────────────────────────────────────────
# Admin/debug helpers
# ──────────────────────────────────────────────────────────────────────────────
def clear_mint_cache() -> None:
    """Safe to call; clears local mint cache + inflight."""
    _MINT_CACHE.clear()
    for _k, fut in list(_INFLIGHT.items()):
        with contextlib.suppress(Exception):
            if not fut.done():
                fut.cancel()
    _INFLIGHT.clear()


# Back-compat export
RPC_URL = getattr(sw, "RPC_URL", "")

__all__ = [
    "ASSOCIATED_TOKEN_PROGRAM",
    "LAMPORTS_PER_SOL",
    "NATIVE_SOL_DECIMALS",
    "NATIVE_SOL_MINT",
    "RPC_URL",
    "SYSTEM_PROGRAM",
    "TOKEN_PROGRAM_2022",
    "TOKEN_PROGRAM_LEGACY",
    "MintInfo",
    "TokenAccount",
    "classify_token_program",
    "clear_mint_cache",
    "fetch_mint_info",
    "get_associated_token_address",
    "get_decimals",
    "get_mint_info",
    "get_sol_decimals",
    "get_token_accounts_by_owner",
    "get_token_balance_by_owner",
    "get_token_decimals",
    "is_valid_mint",
    "lamports_to_sol",
    "raw_from_ui",
    "resolve_token_program_for_mint",
    "sol_decimals",
    "sol_to_lamports",
    "ui_from_raw",
]
