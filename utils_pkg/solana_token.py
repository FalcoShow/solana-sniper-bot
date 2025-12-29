# utils_pkg/solana_token.py
from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from typing import Any, Optional, Tuple, Dict

from utils_pkg.txlearn import _Rpc, RPC_URL  # reuse lightweight RPC

TOKEN_PROGRAM_LEGACY = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
TOKEN_PROGRAM_2022 = "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb"

NATIVE_SOL_MINT = "So11111111111111111111111111111111111111112"
NATIVE_SOL_DECIMALS = 9

# Conservative defaults: RPC mints do not change decimals once created.
_DEFAULT_TTL_S = 6 * 60 * 60  # 6 hours
_MAX_CACHE = 50_000  # safety cap (won't grow unbounded)


@dataclass(frozen=True, slots=True)
class MintInfo:
    decimals: int
    owner_program: str  # token program owner pid
    program_kind: str   # "spl-token" | "token-2022"

    def as_tuple(self) -> Tuple[int, str]:
        return self.decimals, self.owner_program


# ──────────────────────────────────────────────────────────────────────────────
# Small in-memory caches (process-local)
# ──────────────────────────────────────────────────────────────────────────────

# mint -> (expires_at_ts, MintInfo)
_MINT_CACHE: Dict[str, Tuple[float, MintInfo]] = {}

# mint -> in-flight future (dedupe concurrent requests)
_INFLIGHT: Dict[str, asyncio.Future] = {}

# global lock for cache/inflight maps
_CACHE_LOCK = asyncio.Lock()


def classify_token_program(owner_pid: str) -> str:
    """Map program id -> human string for router/db."""
    if owner_pid == TOKEN_PROGRAM_2022:
        return "token-2022"
    return "spl-token"  # default / legacy


def _now() -> float:
    return time.monotonic()


def _is_probable_pubkey(s: str) -> bool:
    # Lightweight sanity check; do NOT reject aggressively.
    if not s:
        return False
    if len(s) < 32 or len(s) > 60:
        return False
    return True


async def _maybe_close_rpc(rpc: Any) -> None:
    """
    Best-effort close for _Rpc implementations that wrap aiohttp/httpx sessions.
    Safe no-op if not supported.
    """
    if rpc is None:
        return
    try:
        aclose = getattr(rpc, "aclose", None)
        if callable(aclose):
            await aclose()
            return
    except Exception:
        pass
    try:
        close = getattr(rpc, "close", None)
        if callable(close):
            out = close()
            if asyncio.iscoroutine(out):
                await out
            return
    except Exception:
        pass


async def _get_account_parsed(rpc: _Rpc, addr: str) -> Optional[dict]:
    """
    Best-effort fetch of a single account in jsonParsed form.
    Tries getMultipleAccounts first (vectorized path), then getAccountInfo.
    Returns the account dict with "data" (jsonParsed) or None.
    """
    # 1) Vectorized path
    try:
        res = await rpc.get_multiple_accounts([addr])
        v = (res or {}).get("value") or []
        acc = v[0] if v else None
        if acc and isinstance(acc.get("data"), dict):
            return acc
    except Exception:
        pass

    # 2) Fallback to getAccountInfo
    try:
        res = await rpc.get_account_info(addr)
        val = (res or {}).get("value")
        if val and isinstance(val.get("data"), dict):
            return val
    except Exception:
        pass

    return None


def _parse_mint_info_from_account(acc: dict) -> Optional[MintInfo]:
    """
    Parse a jsonParsed account into MintInfo if it is an SPL mint.
    """
    try:
        owner = str(acc.get("owner") or "")
        data = acc.get("data") or {}
        parsed = (data.get("parsed") or {}) if isinstance(data, dict) else {}
        if (parsed or {}).get("type") != "mint":
            return None
        info = (parsed.get("info") or {})
        decimals = int(info.get("decimals", 0))
        if decimals < 0 or decimals > 18:
            # weird/invalid, but don't crash the system
            return None
        kind = classify_token_program(owner)
        return MintInfo(decimals=decimals, owner_program=owner, program_kind=kind)
    except Exception:
        return None


async def fetch_mint_info(
    mint: str,
    rpc_url: str | None = None,
    *,
    rpc: _Rpc | None = None,
) -> Optional[Tuple[int, str]]:
    """
    Backwards-compatible API:
    Returns (decimals, token_program_owner) for an SPL mint account.
    If not a valid mint (or not created), returns None.

    Note: For SOL (native), returns None by design (use get_token_decimals()).
    """
    if not mint or mint == NATIVE_SOL_MINT:
        return None
    mi = await get_mint_info(mint, rpc_url=rpc_url, rpc=rpc)
    return mi.as_tuple() if mi else None


async def get_mint_info(
    mint: str,
    rpc_url: str | None = None,
    *,
    rpc: _Rpc | None = None,
    cache_ttl_s: float = _DEFAULT_TTL_S,
    use_cache: bool = True,
) -> Optional[MintInfo]:
    """
    Canonical mint metadata fetcher with TTL cache + in-flight dedupe.

    - Returns MintInfo(decimals, owner_program, program_kind)
    - Returns None if not a valid mint account
    - Never raises (best-effort)
    """
    if not mint or mint == NATIVE_SOL_MINT:
        return None
    if not _is_probable_pubkey(mint):
        return None

    # Cache fast-path
    if use_cache:
        async with _CACHE_LOCK:
            hit = _MINT_CACHE.get(mint)
            if hit:
                exp, mi = hit
                if exp > _now():
                    return mi
                else:
                    _MINT_CACHE.pop(mint, None)

            inflight = _INFLIGHT.get(mint)
            if inflight:
                try:
                    return await asyncio.wait_for(inflight, timeout=8.0)
                except Exception:
                    # if inflight is stuck/bad, fall through to a fresh request
                    _INFLIGHT.pop(mint, None)

            loop = asyncio.get_running_loop()
            fut = loop.create_future()
            _INFLIGHT[mint] = fut

    created_rpc = False
    _rpc = rpc
    try:
        if _rpc is None:
            _rpc = _Rpc(rpc_url or RPC_URL)
            created_rpc = True

        acc = await _get_account_parsed(_rpc, mint)
        mi = _parse_mint_info_from_account(acc) if acc else None

        if use_cache:
            async with _CACHE_LOCK:
                # Resolve inflight first (so awaiters release promptly)
                fut2 = _INFLIGHT.pop(mint, None)
                if fut2 and not fut2.done():
                    fut2.set_result(mi)

                # Cache only if valid
                if mi:
                    if len(_MINT_CACHE) >= _MAX_CACHE:
                        # Simple eviction: drop ~1% oldest by expiry order (fast enough)
                        # Avoid heavy LRU structures for simplicity.
                        items = list(_MINT_CACHE.items())
                        items.sort(key=lambda kv: kv[1][0])  # exp asc
                        for k, _v in items[: max(1, len(items) // 100)]:
                            _MINT_CACHE.pop(k, None)

                    _MINT_CACHE[mint] = (_now() + float(cache_ttl_s), mi)

        return mi

    except Exception:
        if use_cache:
            async with _CACHE_LOCK:
                fut2 = _INFLIGHT.pop(mint, None)
                if fut2 and not fut2.done():
                    fut2.set_result(None)
        return None

    finally:
        if created_rpc:
            await _maybe_close_rpc(_rpc)


# ──────────────────────────────────────────────────────────────────────────────
# Canonical decimals API (this is what router/check_router should use)
# ──────────────────────────────────────────────────────────────────────────────

async def get_token_decimals(
    mint: str,
    rpc_url: str | None = None,
    *,
    rpc: _Rpc | None = None,
    default: int | None = None,
) -> int | None:
    """
    Canonical decimals accessor.

    - SOL mint -> 9 (always)
    - SPL mint -> decimals from chain (cached)
    - Unknown/invalid -> default (or None)
    """
    if not mint:
        return default
    if mint == NATIVE_SOL_MINT:
        return NATIVE_SOL_DECIMALS

    mi = await get_mint_info(mint, rpc_url=rpc_url, rpc=rpc)
    if mi:
        return mi.decimals
    return default


# Common aliases (defensive: some parts of the code may look for these names)
async def get_decimals(mint: str, rpc_url: str | None = None, *, rpc: _Rpc | None = None) -> int | None:
    return await get_token_decimals(mint, rpc_url=rpc_url, rpc=rpc, default=None)


async def get_sol_decimals() -> int:
    return NATIVE_SOL_DECIMALS


async def sol_decimals() -> int:
    return NATIVE_SOL_DECIMALS


async def is_valid_mint(mint: str, rpc_url: str | None = None) -> bool:
    """Quick validity check using jsonParsed account.type == 'mint'."""
    try:
        return (await get_mint_info(mint, rpc_url=rpc_url)) is not None
    except Exception:
        return False


def ui_from_raw(amount_raw: int, decimals: int) -> float:
    """Convert raw integer tokens → UI float."""
    if decimals <= 0:
        return float(amount_raw)
    return float(amount_raw) / float(10 ** int(decimals))


def raw_from_ui(amount_ui: float, decimals: int) -> int:
    """Convert UI float → raw integer tokens."""
    if decimals <= 0:
        return int(amount_ui)
    return int(round(float(amount_ui) * float(10 ** int(decimals))))


def clear_mint_cache() -> None:
    """Debug/admin helper (safe to call)."""
    _MINT_CACHE.clear()
    _INFLIGHT.clear()


__all__ = [
    "TOKEN_PROGRAM_LEGACY",
    "TOKEN_PROGRAM_2022",
    "NATIVE_SOL_MINT",
    "NATIVE_SOL_DECIMALS",
    "MintInfo",
    "classify_token_program",
    "fetch_mint_info",
    "get_mint_info",
    "get_token_decimals",
    "get_decimals",
    "get_sol_decimals",
    "sol_decimals",
    "is_valid_mint",
    "ui_from_raw",
    "raw_from_ui",
    "clear_mint_cache",
]
