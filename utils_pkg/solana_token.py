from __future__ import annotations

"""
utils_pkg/solana_token.py — SPL Token primitives (decimals, ATA, balances, conversions)
=====================================================================================

P0 / Chain primitives (NO business logic)
-----------------------------------------
✅ Canonical mint metadata: decimals + token program owner (spl-token vs token-2022)
✅ Canonical ATA derivation (token-2022 seed supported)
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
✅ Defensive parsing:
   - Supports jsonParsed when available
   - Falls back to base64 mint layout for decimals when jsonParsed missing

Phase A — Logging contract hardening
------------------------------------
✅ Guarantees: .handlers + .info/.warning/.error/.debug + .event/.every
✅ NO monkeypatching / NO method-assign
✅ Safe fallbacks to stdlib logging

Notes
-----
- This module does NOT create ATAs or build token instructions. Execution layer handles that.
- Import-time safety: importing this module must not touch the network.
"""

import asyncio
import contextlib
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any, Final, Protocol, TypeAlias

from solders.pubkey import Pubkey

from utils_pkg import solana_wallet as sw

# ──────────────────────────────────────────────────────────────────────────────
# Logger contract (Phase A): handlers + classic methods + event/every
# ──────────────────────────────────────────────────────────────────────────────


class LoggerLike(Protocol):
    @property
    def handlers(self) -> Sequence[Any]: ...

    def info(self, msg: str, *a: Any, **k: Any) -> Any: ...
    def warning(self, msg: str, *a: Any, **k: Any) -> Any: ...
    def error(self, msg: str, *a: Any, **k: Any) -> Any: ...
    def debug(self, msg: str, *a: Any, **k: Any) -> Any: ...

    def event(self, name: str, **fields: Any) -> Any: ...
    def every(self, key: str, n: int = 30, msg: str | None = None, **fields: Any) -> Any: ...


class _LoggerAdapter:
    """
    Adapter that guarantees:
      - .handlers exists (read-only)
      - .info/.warning/.error/.debug exist
      - .event/.every exist
    WITHOUT mutating the underlying logger.
    """

    __slots__ = ("_get", "_every_store")

    def __init__(self, supplier: Callable[[], Any]) -> None:
        self._get = supplier
        self._every_store: dict[str, float] = {}

    def _safe(self) -> Any:
        with contextlib.suppress(Exception):
            return self._get()
        return None

    @property
    def handlers(self) -> Sequence[Any]:
        lg = self._safe()
        hs = getattr(lg, "handlers", None) if lg is not None else None
        if isinstance(hs, (list, tuple)):
            return hs
        return []

    def _call(self, method: str, msg: str, *a: Any, **k: Any) -> None:
        lg = self._safe()
        if lg is None:
            return
        fn = getattr(lg, method, None)
        if callable(fn):
            with contextlib.suppress(Exception):
                fn(msg, *a, **k)

    def info(self, msg: str, *a: Any, **k: Any) -> None:
        self._call("info", msg, *a, **k)

    def warning(self, msg: str, *a: Any, **k: Any) -> None:
        self._call("warning", msg, *a, **k)

    def error(self, msg: str, *a: Any, **k: Any) -> None:
        self._call("error", msg, *a, **k)

    def debug(self, msg: str, *a: Any, **k: Any) -> None:
        self._call("debug", msg, *a, **k)

    def event(self, name: str, **fields: Any) -> None:
        lg = self._safe()
        if lg is None:
            return
        fn = getattr(lg, "event", None)
        if callable(fn):
            with contextlib.suppress(Exception):
                fn(name, **fields)
                return
        # fallback
        self.info("%s %s", name, fields)

    def every(self, key: str, n: int = 30, msg: str | None = None, **fields: Any) -> None:
        try:
            now = time.monotonic()
            sec = float(max(1, int(n)))
            last = float(self._every_store.get(key, 0.0))
            if (now - last) >= sec:
                self._every_store[key] = now
                self.event(msg or key, **fields)
        except Exception:
            return


def _resolve_logger() -> Any:
    # Prefer repo compat
    with contextlib.suppress(Exception):
        from utils_pkg.log_compat import get_logger as gl  # type: ignore

        return gl(__name__)
    with contextlib.suppress(Exception):
        from utils_pkg.logging_setup import get_logger as gl2  # type: ignore

        try:
            return gl2(__name__)
        except TypeError:
            return gl2()

    import logging

    lg = logging.getLogger("solana_token")
    if not getattr(lg, "handlers", None):
        logging.basicConfig(level=logging.INFO)
    return lg


log: LoggerLike = _LoggerAdapter(_resolve_logger)

# ──────────────────────────────────────────────────────────────────────────────
# Program IDs / constants
# ──────────────────────────────────────────────────────────────────────────────
TOKEN_PROGRAM_LEGACY: Final[str] = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
TOKEN_PROGRAM_2022: Final[str] = "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb"

ASSOCIATED_TOKEN_PROGRAM: Final[str] = "ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL"
SYSTEM_PROGRAM: Final[str] = "11111111111111111111111111111111"

NATIVE_SOL_MINT: Final[str] = "So11111111111111111111111111111111111111112"
NATIVE_SOL_DECIMALS: Final[int] = 9

LAMPORTS_PER_SOL: Final[int] = 1_000_000_000

# Conservative defaults: mint decimals don't change after creation.
_DEFAULT_TTL_S: Final[float] = 6 * 60 * 60  # 6h
_NEGATIVE_TTL_S: Final[float] = 5 * 60  # 5m for "not a mint"/missing
_MAX_CACHE: Final[int] = 50_000  # bounded memory cap
_INFLIGHT_WAIT_S: Final[float] = 10.0  # cap for concurrent dedupe waits

_RPC_COMMITMENT: Final[str] = "confirmed"

# ──────────────────────────────────────────────────────────────────────────────
# Types
# ──────────────────────────────────────────────────────────────────────────────
PubkeyLike: TypeAlias = str | Pubkey


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
# mint -> Future[MintInfo|None] (dedupe concurrent fetches)
_INFLIGHT: dict[str, asyncio.Future[MintInfo | None]] = {}
_CACHE_LOCK = asyncio.Lock()


def _now() -> float:
    return time.monotonic()


def classify_token_program(owner_pid: str) -> str:
    if owner_pid == TOKEN_PROGRAM_2022:
        return "token-2022"
    if owner_pid == TOKEN_PROGRAM_LEGACY:
        return "spl-token"
    return "unknown"


def _is_probable_pubkey_str(s: str) -> bool:
    ss = (s or "").strip()
    if not ss:
        return False
    # lightweight guard only; full validation via Pubkey.from_string()
    return 32 <= len(ss) <= 60


def _to_pubkey(x: PubkeyLike) -> Pubkey:
    if isinstance(x, Pubkey):
        return x
    return Pubkey.from_string(str(x).strip())


def _to_str_pk(x: PubkeyLike) -> str:
    return str(x) if isinstance(x, Pubkey) else str(x).strip()


def _unwrap_value(obj: Any) -> Any:
    """
    Normalize common RPC shapes:
      - {"value": ...}
      - {"result": {"value": ...}}
      - pass-through otherwise
    """
    if isinstance(obj, dict):
        if "value" in obj:
            return obj.get("value")
        res = obj.get("result")
        if isinstance(res, dict) and "value" in res:
            return res.get("value")
    return obj


async def _rpc_read(method: str, params: list[Any]) -> Any:
    """
    Canonical read path: use solana_wallet failover helpers.
    (No networking at import; only when called.)
    """
    fn = getattr(sw, "_rpc_call_failover_async", None)
    if callable(fn):
        return await fn(method, params, kind="read")  # type: ignore[misc]
    fn2 = getattr(sw, "_rpc_call_failover", None)
    if callable(fn2):
        return await asyncio.to_thread(fn2, method, params, kind="read")  # type: ignore[misc]
    raise RuntimeError("solana_wallet RPC helpers not found")


def _parse_decimals_from_base64_mint_data(data: Any) -> int | None:
    """
    Fallback decimals parser for SPL mint account bytes.
    SPL Mint layout places decimals at byte offset 44 (0-based) and requires len>=45.
    Works for legacy and token-2022 mints in practice (shared prefix).
    """
    try:
        # expected: data = [<base64str>, <encoding>]
        if not isinstance(data, (list, tuple)) or len(data) < 1:
            return None
        b64 = data[0]
        if not isinstance(b64, str) or not b64:
            return None
        import base64

        raw = base64.b64decode(b64)
        if len(raw) < 45:
            return None
        dec = int(raw[44])
        if 0 <= dec <= 18:
            return dec
        return None
    except Exception:
        return None


def _parse_mint_info_from_account_value(val: dict[str, Any]) -> MintInfo | None:
    """
    Parse getAccountInfo.value into MintInfo.

    Supports:
      - jsonParsed: value = {"owner": <pid>, "data": {"parsed": {...}}}
      - base64:     value = {"owner": <pid>, "data": ["....", "base64"], ...}
    """
    try:
        owner = str(val.get("owner") or "").strip()
        if not owner:
            return None

        kind = classify_token_program(owner)
        if kind == "unknown":
            return None

        data = val.get("data")

        # jsonParsed path
        if isinstance(data, dict):
            parsed = data.get("parsed")
            if not isinstance(parsed, dict):
                return None
            if str(parsed.get("type") or "") != "mint":
                return None
            info = parsed.get("info")
            if not isinstance(info, dict):
                return None
            decimals = int(info.get("decimals", -1))
            if 0 <= decimals <= 18:
                return MintInfo(decimals=decimals, owner_program=owner, program_kind=kind)
            return None

        # base64 path
        dec = _parse_decimals_from_base64_mint_data(data)
        if dec is None:
            return None
        return MintInfo(decimals=dec, owner_program=owner, program_kind=kind)
    except Exception:
        return None


async def _get_account_info(addr: str) -> dict[str, Any] | None:
    """
    Returns getAccountInfo.value dict (best-effort) or None.
    Tries jsonParsed first; if absent, accepts base64 layout parsing later.
    """
    if not _is_probable_pubkey_str(addr):
        return None
    try:
        res = await _rpc_read(
            "getAccountInfo",
            [addr, {"encoding": "jsonParsed", "commitment": _RPC_COMMITMENT}],
        )
        val = _unwrap_value(res)
        return val if isinstance(val, dict) else None
    except Exception:
        return None


# ──────────────────────────────────────────────────────────────────────────────
# Mint info APIs (back-compat + canonical)
# ──────────────────────────────────────────────────────────────────────────────
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
    _ = rpc_url
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
    _ = rpc_url

    m = (mint or "").strip()
    if not m or m == NATIVE_SOL_MINT:
        return None
    if not _is_probable_pubkey_str(m):
        return None

    # ---- Cache / inflight fast path ----
    fut: asyncio.Future[MintInfo | None] | None = None
    do_fetch = True

    if use_cache:
        async with _CACHE_LOCK:
            hit = _MINT_CACHE.get(m)
            if hit:
                exp, cached = hit
                if exp > _now():
                    return cached
                _MINT_CACHE.pop(m, None)

            fut = _INFLIGHT.get(m)
            if fut is None:
                fut = asyncio.get_running_loop().create_future()
                _INFLIGHT[m] = fut
                do_fetch = True
            else:
                do_fetch = False

    if use_cache and not do_fetch and fut is not None:
        try:
            return await asyncio.wait_for(fut, timeout=_INFLIGHT_WAIT_S)
        except Exception:
            do_fetch = True

    mi: MintInfo | None = None
    err: Exception | None = None

    try:
        # Back-compat: if caller provides an rpc object, try it first.
        if rpc is not None:
            with contextlib.suppress(Exception):
                fn = getattr(rpc, "get_account_info", None)
                if callable(fn):
                    res = await fn(m)  # type: ignore[misc]
                    val = _unwrap_value(res)
                    if isinstance(val, dict):
                        mi = _parse_mint_info_from_account_value(val)

            if mi is None:
                with contextlib.suppress(Exception):
                    fn2 = getattr(rpc, "get_multiple_accounts", None)
                    if callable(fn2):
                        res = await fn2([m])  # type: ignore[misc]
                        vals = _unwrap_value(res) or []
                        acc = vals[0] if isinstance(vals, list) and vals else None
                        if isinstance(acc, dict):
                            mi = _parse_mint_info_from_account_value(acc)

        if mi is None:
            val = await _get_account_info(m)
            if isinstance(val, dict):
                mi = _parse_mint_info_from_account_value(val)

        return mi
    except Exception as e:
        err = e
        return None
    finally:
        if use_cache:
            async with _CACHE_LOCK:
                fut2 = _INFLIGHT.pop(m, None)
                if fut2 is not None and not fut2.done():
                    fut2.set_result(mi)

                if len(_MINT_CACHE) >= _MAX_CACHE:
                    items = list(_MINT_CACHE.items())
                    items.sort(key=lambda kv: float(kv[1][0]))  # exp asc
                    drop_n = max(1, len(items) // 100)
                    for k, _v in items[:drop_n]:
                        _MINT_CACHE.pop(k, None)

                ttl = float(cache_ttl_s) if mi else float(negative_ttl_s)
                _MINT_CACHE[m] = (_now() + max(2.0, ttl), mi)

        if err is not None:
            with contextlib.suppress(Exception):
                log.debug("MINT_INFO_FETCH_FAIL %s", m[:44], extra={"err": str(err)[:180]})


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


# Common aliases (older code may use these names)
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
    return (await get_mint_info(mint)) is not None


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
      ATA = find_program_address([owner, token_program_id, mint], ASSOCIATED_TOKEN_PROGRAM)

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
    Defaults to legacy if unknown/unresolvable.
    """
    mi = await get_mint_info(mint)
    if mi and mi.owner_program == TOKEN_PROGRAM_2022:
        return TOKEN_PROGRAM_2022
    return TOKEN_PROGRAM_LEGACY


# ──────────────────────────────────────────────────────────────────────────────
# Token account queries (jsonParsed)
# ──────────────────────────────────────────────────────────────────────────────
def _parse_token_amount(info: dict[str, Any]) -> tuple[int, int]:
    """
    tokenAmount:
      {
        "amount": "123",
        "decimals": 6,
        "uiAmount": 0.000123,
        "uiAmountString": "0.000123"
      }
    Prefer `amount` string; fall back to uiAmountString/uiAmount if needed.
    """
    amt_raw = 0
    dec = 0
    try:
        ta = info.get("tokenAmount") or {}
        if not isinstance(ta, dict):
            return 0, 0

        dec = int(ta.get("decimals") or 0)

        amt_s = ta.get("amount")
        if amt_s is not None:
            amt_raw = int(str(amt_s))
            return max(0, amt_raw), max(0, dec)

        uas = ta.get("uiAmountString")
        if uas is not None:
            amt_raw = raw_from_ui(uas, dec)
            return max(0, int(amt_raw)), max(0, dec)

        ua = ta.get("uiAmount")
        if ua is not None:
            amt_raw = raw_from_ui(str(ua), dec)
            return max(0, int(amt_raw)), max(0, dec)
    except Exception:
        return 0, 0
    return max(0, int(amt_raw)), max(0, int(dec))


def _parse_token_account_item(item: dict[str, Any], *, program_id: str) -> TokenAccount | None:
    """
    Item from getTokenAccountsByOwner.value:
      {"pubkey": "...", "account": {"data": {"program":"spl-token","parsed":{...}} , ...}}
    """
    try:
        pk = str(item.get("pubkey") or "").strip()
        acc = item.get("account")
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
        if not mint or not owner:
            return None

        amt_raw, dec = _parse_token_amount(info)

        return TokenAccount(
            pubkey=pk,
            mint=mint,
            owner=owner,
            amount_raw=int(amt_raw),
            decimals=int(dec),
            program=str(program_id),
        )
    except Exception:
        return None


async def _rpc_get_token_accounts_by_owner(
    owner_s: str, filt: dict[str, Any]
) -> list[dict[str, Any]]:
    try:
        res = await _rpc_read(
            "getTokenAccountsByOwner",
            [owner_s, filt, {"encoding": "jsonParsed", "commitment": _RPC_COMMITMENT}],
        )
        vals = _unwrap_value(res)
        if isinstance(vals, list):
            return [x for x in vals if isinstance(x, dict)]
        if isinstance(res, dict):
            vv = res.get("value")
            if isinstance(vv, list):
                return [x for x in vv if isinstance(x, dict)]
        return []
    except Exception:
        return []


async def get_token_accounts_by_owner(
    owner: PubkeyLike,
    *,
    mint: str | None = None,
    token_program_id: str | None = None,
) -> list[TokenAccount]:
    """
    Returns parsed token accounts for an owner.

    Robustness rule (token-2022 safe):
      - If mint is provided, we *first* try RPC mint-filter (fast path).
      - If that returns empty (some RPCs won’t return token-2022 via mint filter),
        we fall back to programId queries and filter client-side.

    - If token_program_id is provided, only query that program.
    - If mint is not provided and token_program_id is None, query BOTH programs.
    """
    owner_s = _to_str_pk(owner)
    mint_s = (mint or "").strip() or None

    # Fast path: mint filter (often cheapest)
    if mint_s:
        raw_items = await _rpc_get_token_accounts_by_owner(owner_s, {"mint": mint_s})
        parsed_fast: list[TokenAccount] = []
        for it in raw_items:
            ta = _parse_token_account_item(it, program_id="")  # program filled later
            if ta:
                # program is not reliably known from mint-filter query; keep empty for now
                parsed_fast.append(ta)
        if parsed_fast:
            # Normalize program field best-effort by resolving mint program once.
            prog = token_program_id or (await resolve_token_program_for_mint(mint_s))
            return [
                TokenAccount(
                    pubkey=a.pubkey,
                    mint=a.mint,
                    owner=a.owner,
                    amount_raw=a.amount_raw,
                    decimals=a.decimals,
                    program=prog,
                )
                for a in parsed_fast
            ]

    programs: list[str]
    if token_program_id:
        programs = [str(token_program_id)]
    else:
        programs = [TOKEN_PROGRAM_LEGACY, TOKEN_PROGRAM_2022]

    out: list[TokenAccount] = []

    for prog in programs:
        items = await _rpc_get_token_accounts_by_owner(owner_s, {"programId": prog})
        for it in items:
            ta = _parse_token_account_item(it, program_id=prog)
            if not ta:
                continue
            if mint_s and ta.mint != mint_s:
                continue
            out.append(ta)

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

    decimals: int | None = None
    if not accts:
        decimals = await get_token_decimals(mint_s, default=None)
        return 0, decimals

    total = 0
    for a in accts:
        total += int(a.amount_raw)
        if decimals is None:
            decimals = int(a.decimals)

    return int(total), decimals


# ──────────────────────────────────────────────────────────────────────────────
# Conversions (safe + consistent)
# ──────────────────────────────────────────────────────────────────────────────
def ui_from_raw(amount_raw: int, decimals: int) -> float:
    """Convert raw integer tokens → UI float."""
    try:
        d = int(decimals)
        if d <= 0:
            return float(amount_raw)
        return float(int(amount_raw)) / float(10**d)
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
    """Clears local mint cache + inflight (safe to call)."""
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
