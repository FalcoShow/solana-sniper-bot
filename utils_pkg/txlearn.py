# utils_pkg/txlearn.py
from __future__ import annotations

"""
utils_pkg/txlearn.py

TxLearn (God-Tier Overhaul – V3+++ / quant-grade)
=================================================

Purpose
-------
After a BUY fill (signature known), TxLearn resolves the *true* mint (especially
when you started from a pump id / ambiguous identifier) and waits until:

  1) the transaction is visible (status/tx reachable), and
  2) your wallet shows a non-zero token balance for that mint.

It returns a single, rich dict payload suitable for:
  - positions store (single source of truth)
  - ledger / AI DB logging
  - exit engine wiring (owner_ata, decimals, qty)

Design goals
------------
- Production-first: safe sessions, deterministic polling, graceful fallbacks
- Render-friendly: env driven, no side effects at import
- Budget-aware: fails open (skips expensive calls if budget blocks)
- Minimal RPC blast radius: tiny TTL caches + status poll floor
- Backwards-compatible public API:
    - learn_position(...)
    - resolve_then_learn_position(...)
    - TxLearner.resolve_then_learn(...)
    - TxLearner.learn(...)
    - resolve_mint_best_effort(...)

Environment contract (high-signal knobs)
----------------------------------------
Core RPC:
  SOL_RPC_URL                 default mainnet
  SOL_RPC_TX_URL              optional preferred tx/status RPC
  SOL_RPC_TX_URL              (alias) SOL_RPC_TX_URL is used as-is

Helius:
  HELIUS_API_KEY              enables HELIUS_RPC_URL + Parse API

Learner:
  PF_TXLEARN_COMMITMENT       processed|confirmed|finalized (default: processed)
  PF_TXLEARN_TRIES            poll tries (default: 20)
  PF_TXLEARN_DELAY_SEC        base delay between polls (default: 1.2)
  PF_TXLEARN_WARMUP_SEC        optional warmup before polling (default: 2.0)
  PF_POS_LEARN_BASE_DELAY_SEC  optional extra base sleep per attempt (default: 0)

Resolver:
  PF_TXLEARN_RESOLVE_TRIES    resolve attempts (default: 6)
  PF_TXLEARN_RESOLVE_DELAY_SEC delay between resolve attempts (default: 0.8)

Rate limiting / caching:
  PF_STATUS_POLL_SEC          status poll floor seconds (default: 0.6, min 0.2)
  PF_RPC_CACHE_TTL_SEC        small cache TTL seconds (default: 1.5)

Debug:
  PF_DEBUG_TX                 enable extra events (default: 0)
"""

import asyncio
import os
import random
import time
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any

import aiohttp

from logging_setup import bump, get_logger

# Budget hooks (soft gates). Fail-open if module not present.
try:
    from utils_pkg import budget  # type: ignore
except Exception:  # pragma: no cover

    class _BudgetNoop:
        def before_quote(self, units: int = 1) -> bool:
            return True

        def after_quote(self, units: int = 1) -> None:
            return None

        def before_rpc(self, method: str, *, weight: int | None = None) -> bool:
            return True

        def after_rpc(self, method: str, *, weight: int | None = None) -> None:
            return None

    budget = _BudgetNoop()  # type: ignore


# ──────────────────────────────────────────────────────────────────────────────
# Config
# ──────────────────────────────────────────────────────────────────────────────


def _env_str(name: str, default: str = "") -> str:
    return (os.getenv(name, default) or "").strip()


def _env_int(name: str, default: int) -> int:
    try:
        return int(_env_str(name, str(default)) or str(default))
    except Exception:
        return default


def _env_float(name: str, default: float) -> float:
    try:
        return float(_env_str(name, str(default)) or str(default))
    except Exception:
        return default


def _env_bool(name: str, default: bool = False) -> bool:
    v = _env_str(name, "1" if default else "0").lower()
    return v in {"1", "true", "yes", "y", "on"}


RPC_URL = _env_str("SOL_RPC_URL") or "https://api.mainnet-beta.solana.com"
TX_RPC_URL = _env_str("SOL_RPC_TX_URL") or RPC_URL

HELIUS_API_KEY = _env_str("HELIUS_API_KEY")
HELIUS_RPC_URL = (
    f"https://mainnet.helius-rpc.com/?api-key={HELIUS_API_KEY}" if HELIUS_API_KEY else ""
)
HELIUS_PARSE_URL = (
    f"https://api.helius.xyz/v0/transactions?api-key={HELIUS_API_KEY}" if HELIUS_API_KEY else ""
)

TX_COMMITMENT = (_env_str("PF_TXLEARN_COMMITMENT", "processed") or "processed").lower()

TX_TRIES = max(1, _env_int("PF_TXLEARN_TRIES", 20))
TX_DELAY_SEC = max(0.0, _env_float("PF_TXLEARN_DELAY_SEC", 1.2))
TX_WARMUP_SEC = max(0.0, _env_float("PF_TXLEARN_WARMUP_SEC", 2.0))
POS_LEARN_BASE_DELAY_SEC = max(0.0, _env_float("PF_POS_LEARN_BASE_DELAY_SEC", 0.0))

RESOLVE_TRIES = max(1, _env_int("PF_TXLEARN_RESOLVE_TRIES", 6))
RESOLVE_DELAY_SEC = max(0.0, _env_float("PF_TXLEARN_RESOLVE_DELAY_SEC", 0.8))

DEBUG = _env_bool("PF_DEBUG_TX", False)

STATUS_POLL_SEC = max(0.2, _env_float("PF_STATUS_POLL_SEC", 0.6))
RPC_CACHE_TTL_SEC = max(0.0, _env_float("PF_RPC_CACHE_TTL_SEC", 1.5))

SPL_TOKEN = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
TOKEN_2022 = "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb"

_UA = {"user-agent": "pnlsniper/1.0 (+txlearn)"}

log = get_logger()

_BASE58 = set("123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz")


def _jitter(t: float, frac: float = 0.25) -> float:
    if t <= 0:
        return 0.0
    d = t * frac
    return max(0.0, t + random.uniform(-d, d))


def _is_likely_pubkey(s: str) -> bool:
    s = (s or "").strip()
    # "....pump" ids are not base58 pubkeys
    if s.endswith("pump"):
        return False
    if not (32 <= len(s) <= 64):
        return False
    return all(ch in _BASE58 for ch in s)


def _is_probably_pump_id(s: str) -> bool:
    s = (s or "").strip()
    return s.endswith("pump") and len(s) >= 36


# ──────────────────────────────────────────────────────────────────────────────
# Tiny TTL cache
# ──────────────────────────────────────────────────────────────────────────────


class _TTLCache:
    __slots__ = ("_m", "_ttl")

    def __init__(self, ttl_sec: float):
        self._ttl = float(ttl_sec)
        self._m: dict[str, tuple[float, Any]] = {}

    def get(self, k: str) -> Any | None:
        if self._ttl <= 0:
            return None
        now = time.time()
        rec = self._m.get(k)
        if not rec:
            return None
        exp, val = rec
        if exp < now:
            self._m.pop(k, None)
            return None
        return val

    def set(self, k: str, v: Any) -> None:
        if self._ttl <= 0:
            return
        self._m[k] = (time.time() + self._ttl, v)


# ──────────────────────────────────────────────────────────────────────────────
# Minimal JSON-RPC client (budget-aware + TTL caches)
# ──────────────────────────────────────────────────────────────────────────────


class _SoftBudgetBlock(Exception):
    """Internal: budget asked us to skip a call this tick."""

    pass


@dataclass
class _RpcOptions:
    timeout_total: float = 20.0
    timeout_connect: float = 7.0


class _Rpc:
    """
    Minimal JSON-RPC client for Solana.

    Notes:
    - Budget-aware (soft blocks return None / [])
    - Small TTL caches for getSignatureStatuses + getTransaction
    - Status poll floor to avoid hammering
    """

    def __init__(self, url: str, *, opts: _RpcOptions | None = None):
        self.url = (url or "").strip()
        self._session: aiohttp.ClientSession | None = None
        self._req_id = 1000 + random.randint(0, 999)
        self._opts = opts or _RpcOptions()

        self._status_cache = _TTLCache(RPC_CACHE_TTL_SEC)
        self._tx_cache = _TTLCache(RPC_CACHE_TTL_SEC)
        self._last_status_call_ts: float = 0.0

    @property
    def present(self) -> bool:
        return bool(self.url)

    async def _ensure(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            timeout = aiohttp.ClientTimeout(
                total=self._opts.timeout_total, connect=self._opts.timeout_connect
            )
            self._session = aiohttp.ClientSession(timeout=timeout, headers=_UA)
        return self._session

    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()

    async def _call_raw(self, method: str, params: list, *, weight: int = 1) -> Any:
        if not self.present:
            raise RuntimeError("RPC URL not set")

        if not budget.before_rpc(method, weight=weight):
            raise _SoftBudgetBlock()

        session = await self._ensure()
        self._req_id += 1
        payload = {"jsonrpc": "2.0", "id": self._req_id, "method": method, "params": params}

        try:
            async with session.post(self.url, json=payload) as resp:
                txt = await resp.text()
                if resp.status != 200:
                    raise RuntimeError(f"HTTP {resp.status}: {txt[:240]}")
                try:
                    data = await resp.json(content_type=None)
                except Exception as err:
                    # B904: explicit chaining distinguishes parse failure vs except-clause error
                    raise RuntimeError(f"json parse error: {txt[:240]}") from err
        finally:
            budget.after_rpc(method, weight=weight)

        if isinstance(data, dict) and data.get("error"):
            raise RuntimeError(str(data["error"]))
        return data.get("result") if isinstance(data, dict) else None

    # Convenience wrappers (all return None/[] on soft block or failures)

    async def get_signature_status(self, sig: str) -> dict | None:
        now = time.time()

        # Status poll floor:
        if (now - self._last_status_call_ts) < STATUS_POLL_SEC:
            cached = self._status_cache.get(sig)
            if cached is not None:
                return cached

        cached = self._status_cache.get(sig)
        if cached is not None:
            return cached

        self._last_status_call_ts = now

        try:
            res = await self._call_raw(
                "getSignatureStatuses",
                [[sig], {"searchTransactionHistory": True}],
                weight=1,
            )
            arr = (res or {}).get("value") or []
            out = arr[0] if arr else None
            if out is not None:
                self._status_cache.set(sig, out)
            return out
        except _SoftBudgetBlock:
            return None
        except Exception:
            return None

    async def get_transaction(self, sig: str, commitment: str) -> dict | None:
        cached = self._tx_cache.get(sig)
        if cached is not None:
            return cached

        # Try a few compatible option sets
        attempts = [
            {
                "commitment": commitment,
                "encoding": "jsonParsed",
                "maxSupportedTransactionVersion": 0,
            },
            {"commitment": commitment, "encoding": "jsonParsed"},
            {"commitment": commitment, "encoding": "base64"},
        ]
        for i, cfg in enumerate(attempts, start=1):
            try:
                res = await self._call_raw("getTransaction", [sig, cfg], weight=2)
                if res:
                    self._tx_cache.set(sig, res)
                    return res
            except _SoftBudgetBlock:
                return None
            except Exception:
                # tiny backoff between encodings
                if i < len(attempts):
                    await asyncio.sleep(0.2)

        return None

    async def get_account_info(self, addr: str) -> dict | None:
        try:
            return await self._call_raw(
                "getAccountInfo", [addr, {"encoding": "jsonParsed"}], weight=1
            )
        except _SoftBudgetBlock:
            return None
        except Exception:
            return None

    async def get_multiple_accounts(self, addrs: list[str]) -> dict | None:
        if not addrs:
            return None
        try:
            return await self._call_raw(
                "getMultipleAccounts",
                [addrs, {"encoding": "jsonParsed", "commitment": TX_COMMITMENT}],
                weight=2,
            )
        except _SoftBudgetBlock:
            return None
        except Exception:
            return None

    async def get_token_accounts_by_owner_mint(self, owner: str, mint: str) -> list[dict]:
        try:
            res = await self._call_raw(
                "getTokenAccountsByOwner",
                [owner, {"mint": mint}, {"encoding": "jsonParsed", "commitment": TX_COMMITMENT}],
                weight=2,
            )
            return (res or {}).get("value") or []
        except _SoftBudgetBlock:
            return []
        except Exception:
            return []

    async def get_balance(self, owner: str) -> dict | None:
        try:
            return await self._call_raw(
                "getBalance", [owner, {"commitment": TX_COMMITMENT}], weight=1
            )
        except _SoftBudgetBlock:
            return None
        except Exception:
            return None


# ──────────────────────────────────────────────────────────────────────────────
# Token helpers
# ──────────────────────────────────────────────────────────────────────────────


def _extract_ui_amount_from_acc(acc: dict) -> tuple[int, int, float]:
    """
    Returns (raw, decimals, ui) from getTokenAccountsByOwner parsed account item.
    """
    try:
        info = acc["account"]["data"]["parsed"]["info"]
        amt = info["tokenAmount"]
        raw = int(str(amt.get("amount", "0")))
        dec = int(amt.get("decimals", 0) or 0)
        ui_val = amt.get("uiAmount")
        if ui_val is None:
            # uiAmountString is sometimes present
            ui_str = amt.get("uiAmountString")
            ui = float(ui_str) if ui_str is not None else raw / 10**dec if dec else float(raw)
        else:
            ui = float(ui_val)
        return raw, dec, ui
    except Exception:
        return 0, 0, 0.0


def _pick_best_owner_ata(accounts: list[dict]) -> tuple[str | None, int, int, float]:
    best_pub: str | None = None
    best_raw = 0
    best_dec = 0
    best_ui = 0.0
    for a in accounts:
        pub = a.get("pubkey")
        raw, dec, ui = _extract_ui_amount_from_acc(a)
        if ui > best_ui:
            best_pub, best_raw, best_dec, best_ui = pub, raw, dec, ui
    return best_pub, best_raw, best_dec, best_ui


async def _sum_owned_ui_amount(rpc: _Rpc, owner: str, mint: str) -> float:
    accts = await rpc.get_token_accounts_by_owner_mint(owner, mint)
    return float(sum(_extract_ui_amount_from_acc(a)[2] for a in accts))


async def _mint_program_owner(rpc: _Rpc, mint: str) -> str:
    """
    Returns owning program of the mint account (Tokenkeg... vs Tokenz...).
    Defaults to SPL_TOKEN on uncertainty.
    """
    info = await rpc.get_account_info(mint)
    try:
        owner = ((info or {}).get("value") or {}).get("owner")
        if owner in (SPL_TOKEN, TOKEN_2022):
            return owner
    except Exception:
        pass
    return SPL_TOKEN


async def _is_valid_mint_account(rpc: _Rpc, addr: str) -> bool:
    if not _is_likely_pubkey(addr):
        return False

    info = await rpc.get_account_info(addr)
    try:
        val = (info or {}).get("value")
        if not val:
            return False
        data = val.get("data")
        # jsonParsed mint returns {"parsed": {"type": "mint", ...}, "program": "...", ...}
        if isinstance(data, dict):
            parsed = data.get("parsed") or {}
            if parsed.get("type") == "mint":
                return True
        # Some RPCs may not parse; accept if owner matches known token programs
        owner = val.get("owner")
        if owner in (SPL_TOKEN, TOKEN_2022):
            return True
    except Exception:
        return False

    return False


# ──────────────────────────────────────────────────────────────────────────────
# Mint inference
# ──────────────────────────────────────────────────────────────────────────────


def _ui_amount_int(bal: dict) -> int:
    """
    Robustly coerce uiTokenAmount.amount (string integer) into int.
    """
    try:
        ui = (bal or {}).get("uiTokenAmount") or {}
        amt = ui.get("amount", "0")
        return int(str(amt))
    except Exception:
        return 0


async def _infer_mint_from_rpc_tx(
    tx_rpcs: Iterable[_Rpc],
    sig: str,
    owner: str | None,
    *,
    commitment: str,
) -> str | None:
    """
    Infer mint from getTransaction meta postTokenBalances.
    Prefer balances belonging to `owner`, choose largest positive.
    """
    for rpc in tx_rpcs:
        if not rpc.present:
            continue
        tx = await rpc.get_transaction(sig, commitment)
        if not tx:
            continue

        meta = (tx or {}).get("meta") or {}
        post = meta.get("postTokenBalances") or []
        if not post:
            continue

        # owner-preferred
        best_owner: tuple[str, int] | None = None
        best_any: tuple[str, int] | None = None

        for b in post:
            try:
                m = (b or {}).get("mint")
                if not _is_likely_pubkey(m or ""):
                    continue
                amt = _ui_amount_int(b)
                if amt <= 0:
                    continue
                who = (b or {}).get("owner")
                if owner and who == owner:
                    if best_owner is None or amt > best_owner[1]:
                        best_owner = (m, amt)
                if best_any is None or amt > best_any[1]:
                    best_any = (m, amt)
            except Exception:
                continue

        if best_owner:
            return best_owner[0]
        if best_any:
            return best_any[0]

    return None


async def _hel_parse_for_mint(
    session: aiohttp.ClientSession,
    sig: str,
    owner: str | None,
) -> str | None:
    """
    Use Helius Parse API to infer the purchased mint:
      - prefer positive tokenBalanceChanges for `owner`
      - else largest positive change
      - else scan event mint-like fields
    """
    if not HELIUS_PARSE_URL:
        return None

    if not budget.before_rpc("helius_parse", weight=2):
        return None

    try:
        async with session.post(
            HELIUS_PARSE_URL, json={"transactions": [sig]}, headers=_UA
        ) as resp:
            if resp.status != 200:
                return None
            data = await resp.json(content_type=None)
    except Exception:
        return None
    finally:
        budget.after_rpc("helius_parse", weight=2)

    if not isinstance(data, list) or not data:
        return None

    tx = data[0] or {}
    tbc = tx.get("tokenBalanceChanges") or []

    best_owner: tuple[str, float] | None = None
    best_any: tuple[str, float] | None = None

    for change in tbc:
        try:
            mint = (change or {}).get("mint")
            if not _is_likely_pubkey(mint or ""):
                continue

            # Helius provides both tokenAmount and rawTokenAmount structures; normalize to ui float
            ui = None
            if "tokenAmount" in change and change["tokenAmount"] is not None:
                ui = float(change["tokenAmount"])
            else:
                raw_amt = float(
                    ((change or {}).get("rawTokenAmount") or {}).get("tokenAmount", 0) or 0
                )
                dec = int(((change or {}).get("rawTokenAmount") or {}).get("decimals", 0) or 0)
                ui = raw_amt / (10**dec) if dec else raw_amt

            if ui is None or ui <= 0:
                continue

            who = (change or {}).get("userAccount") or (change or {}).get("owner")
            if owner and who == owner and (best_owner is None or ui > best_owner[1]):
                best_owner = (mint, ui)
            if best_any is None or ui > best_any[1]:
                best_any = (mint, ui)
        except Exception:
            continue

    if best_owner:
        return best_owner[0]
    if best_any:
        return best_any[0]

    # Fallback: scan events for mint-ish keys
    events = tx.get("events") or {}
    for v in events.values():
        seq = v if isinstance(v, list) else [v]
        for ev in seq:
            if not isinstance(ev, dict):
                continue
            m = ev.get("mint") or ev.get("mintAddress") or ev.get("token")
            if _is_likely_pubkey(str(m or "")):
                return str(m)

    return None


async def _maybe_await(x: Any) -> Any:
    if asyncio.iscoroutine(x):
        return await x
    return x


async def resolve_mint_best_effort(
    tx_rpcs: list[_Rpc] | tuple[_Rpc, ...],
    rpc_general: _Rpc,
    raw_id_or_mint: str,
    *,
    signature: str | None,
    owner: str | None,
) -> str | None:
    """
    Best-effort mint resolver.

    Order:
      1) Helius Parse API (if enabled)
      2) RPC getTransaction inference (postTokenBalances)
      3) Pump resolver hooks (if raw looks like pump id)
      4) Direct validation if `raw` already looks like a mint
    """
    raw = (raw_id_or_mint or "").strip()
    owner = (owner or "").strip() or None

    # 1) Helius Parse API
    if signature and HELIUS_API_KEY:
        try:
            # Prefer reusing an existing rpc session if available
            session: aiohttp.ClientSession | None = None
            created = False
            for r in tx_rpcs:
                try:
                    if r.present:
                        session = await r._ensure()  # reuse headers/timeout
                        break
                except Exception:
                    session = None
            if session is None:
                timeout = aiohttp.ClientTimeout(total=20, connect=7)
                session = aiohttp.ClientSession(timeout=timeout, headers=_UA)
                created = True
            try:
                m = await _hel_parse_for_mint(session, signature, owner)
                if m:
                    return m
            finally:
                if created and session and not session.closed:
                    await session.close()
        except Exception:
            pass

    # 2) RPC getTransaction inference
    if signature:
        try:
            m = await _infer_mint_from_rpc_tx(
                [r for r in tx_rpcs if r.present],
                signature,
                owner,
                commitment=TX_COMMITMENT,
            )
            if m:
                return m
        except Exception:
            pass

    # 3) Pump resolvers
    if _is_probably_pump_id(raw):
        try:
            from utils_pkg.pump_trader import (  # type: ignore
                resolve_true_mint as _rtm,
                resolve_true_mint_from_pump_id as _rtm_pid,
            )
        except Exception:
            _rtm = None  # type: ignore
            _rtm_pid = None  # type: ignore

        if signature and _rtm:
            try:
                m = await _maybe_await(_rtm(raw, signature=signature))
                if m:
                    return m
            except Exception:
                pass

        if _rtm_pid:
            try:
                m = await _maybe_await(_rtm_pid(raw))
                if m:
                    return m
            except Exception:
                pass

    # 4) Direct mint
    if _is_likely_pubkey(raw):
        try:
            if await _is_valid_mint_account(rpc_general, raw):
                return raw
        except Exception:
            pass

    return None


# ──────────────────────────────────────────────────────────────────────────────
# Learner
# ──────────────────────────────────────────────────────────────────────────────


class TxLearner:
    """
    Learns position details after a buy.

    Primary entry points:
      - resolve_then_learn(raw_id_or_mint, tx_sig)
      - learn(mint_or_id, tx_sig)

    Return payload is a dict (stable contract).
    """

    def __init__(
        self,
        rpc_url_tx: str = TX_RPC_URL,
        rpc_url_general: str = RPC_URL,
        owner: str | None = None,
        commitment: str = TX_COMMITMENT,
        tries: int = TX_TRIES,
        delay_sec: float = TX_DELAY_SEC,
        warmup_sec: float = TX_WARMUP_SEC,
    ) -> None:
        self.tx_primary = _Rpc(rpc_url_tx)
        self.tx_helius = _Rpc(HELIUS_RPC_URL) if HELIUS_RPC_URL else _Rpc("")
        self.rpc = _Rpc(rpc_url_general)

        self.owner = (owner or _env_str("PF_PP_ACCOUNT")).strip() or ""
        self.commitment = (commitment or TX_COMMITMENT).strip().lower() or "processed"
        self.tries = max(1, int(tries))
        # never tighter than status poll floor
        self.delay = max(STATUS_POLL_SEC, float(delay_sec))
        self.warmup = max(0.0, float(warmup_sec))

    async def close(self) -> None:
        await self.tx_primary.close()
        await self.tx_helius.close()
        await self.rpc.close()

    async def resolve_then_learn(self, raw_id_or_mint: str, tx_sig: str) -> dict[str, Any]:
        """
        Preferred when you have a pump id or uncertain mint.
        """
        t0 = time.time()
        if self.warmup > 0:
            await asyncio.sleep(self.warmup)

        resolved: str | None = None

        for attempt in range(1, RESOLVE_TRIES + 1):
            resolved = await resolve_mint_best_effort(
                (self.tx_primary, self.tx_helius),
                self.rpc,
                raw_id_or_mint,
                signature=tx_sig,
                owner=self.owner or None,
            )
            if resolved:
                break
            if attempt < RESOLVE_TRIES:
                await asyncio.sleep(_jitter(RESOLVE_DELAY_SEC))

        if not resolved:
            log.event(
                "TXLEARN_RESOLVE_FAIL", raw=raw_id_or_mint, sig=tx_sig, attempts=RESOLVE_TRIES
            )
            return await self._learn_tx_only(tx_sig, attempts=RESOLVE_TRIES, t0=t0)

        log.event(
            "TXLEARN_RESOLVED_MINT", raw=raw_id_or_mint, mint=resolved, sig=tx_sig, attempts=attempt
        )
        return await self.learn(resolved, tx_sig, t0=t0)

    async def learn(
        self, mint_or_id: str, tx_sig: str, *, t0: float | None = None
    ) -> dict[str, Any]:
        """
        Learn with known/validated mint. If it doesn't look like a mint, it will
        attempt one quick best-effort resolve pass.
        """
        t0 = t0 or time.time()
        original = (mint_or_id or "").strip()

        mint = original
        if (not _is_likely_pubkey(mint)) or mint.endswith("pump"):
            quick = await resolve_mint_best_effort(
                (self.tx_primary, self.tx_helius),
                self.rpc,
                original,
                signature=tx_sig,
                owner=self.owner or None,
            )
            if quick:
                mint = quick

        if not _is_likely_pubkey(mint):
            log.event("TXLEARN_UNRESOLVED_ID_TXONLY", raw=original, sig=tx_sig)
            return await self._learn_tx_only(tx_sig, t0=t0)

        # Validate mint (best-effort). If mint not ready yet, fall back to tx-only.
        try:
            ok_mint = await _is_valid_mint_account(self.rpc, mint)
        except Exception:
            ok_mint = False
        if not ok_mint:
            log.event("TXLEARN_MINT_NOT_READY", mint=mint, sig=tx_sig)
            return await self._learn_tx_only(tx_sig, t0=t0)

        token_program = await _mint_program_owner(self.rpc, mint)

        last_tx: dict | None = None
        last_status: dict | None = None
        last_owner_ui: float = 0.0
        attempt = 0

        if DEBUG:
            log.event(
                "TXLEARN_BEGIN",
                sig=tx_sig,
                mint=mint,
                owner=self.owner or None,
                commitment=self.commitment,
            )

        for attempt in range(1, self.tries + 1):
            # 1) status (cheap)
            for txrpc in (self.tx_primary, self.tx_helius):
                if not txrpc.present:
                    continue
                st = await txrpc.get_signature_status(tx_sig)
                if st:
                    last_status = st
                    break

            if last_status and last_status.get("err") is not None:
                log.event(
                    "TXLEARN_TX_ERROR", mint=mint, sig=tx_sig, err=str(last_status.get("err"))
                )
                return self._result(
                    t0=t0,
                    tx_found=False,
                    sig=tx_sig,
                    mint=mint,
                    token_program=token_program,
                    status=last_status,
                    tx=None,
                    owner_ata=None,
                    qty_raw=0,
                    qty_ui=0.0,
                    decimals=0,
                    attempts=attempt,
                    block_time=None,
                )

            # 2) tx fetch (may lag indexing)
            for txrpc in (self.tx_primary, self.tx_helius):
                if not txrpc.present:
                    continue
                tx = await txrpc.get_transaction(tx_sig, self.commitment)
                if tx:
                    last_tx = tx
                    break

            # 3) wallet balance for mint
            try:
                last_owner_ui = (
                    await _sum_owned_ui_amount(self.rpc, self.owner, mint) if self.owner else 0.0
                )
            except Exception:
                last_owner_ui = 0.0

            if last_owner_ui > 0:
                break

            if attempt < self.tries:
                if POS_LEARN_BASE_DELAY_SEC > 0:
                    await asyncio.sleep(POS_LEARN_BASE_DELAY_SEC)
                await asyncio.sleep(_jitter(self.delay))

        owner_ata: str | None = None
        qty_raw = 0
        decimals = 0
        qty_ui = 0.0

        if last_owner_ui > 0 and self.owner:
            accounts = await self.rpc.get_token_accounts_by_owner_mint(self.owner, mint)
            owner_ata, qty_raw, decimals, qty_ui = _pick_best_owner_ata(accounts)

        block_time = None
        try:
            if last_tx and "blockTime" in last_tx:
                block_time = last_tx["blockTime"]
        except Exception:
            block_time = None

        result = self._result(
            t0=t0,
            tx_found=bool(last_tx),
            sig=tx_sig,
            mint=mint,
            token_program=token_program,
            status=last_status,
            tx=last_tx,
            owner_ata=owner_ata,
            qty_raw=qty_raw,
            qty_ui=qty_ui,
            decimals=decimals,
            attempts=attempt,
            block_time=block_time,
        )

        log.event(
            "TXLEARN_DONE",
            sig=tx_sig,
            mint=mint,
            token_program=token_program,
            owner_ata=owner_ata,
            qty_ui=qty_ui,
            decimals=decimals,
            tx_found=bool(last_tx),
            attempts=attempt,
            ms=int((time.time() - t0) * 1000),
        )
        bump("txlearn_done")
        return result

    async def _learn_tx_only(
        self, tx_sig: str, attempts: int = 0, *, t0: float | None = None
    ) -> dict[str, Any]:
        """
        Transaction-only visibility when mint can't be resolved/validated.
        """
        t0 = t0 or time.time()
        last_tx: dict | None = None
        last_status: dict | None = None

        max_tries = min(5, self.tries)
        for i in range(1, max_tries + 1):
            for txrpc in (self.tx_primary, self.tx_helius):
                if not txrpc.present:
                    continue
                st = await txrpc.get_signature_status(tx_sig)
                if st:
                    last_status = st
                    break

            if last_status and last_status.get("err") is not None:
                log.event("TXLEARN_TXONLY_ERROR", sig=tx_sig, err=str(last_status.get("err")))
                break

            for txrpc in (self.tx_primary, self.tx_helius):
                if not txrpc.present:
                    continue
                tx = await txrpc.get_transaction(tx_sig, self.commitment)
                if tx:
                    last_tx = tx
                    break

            if last_tx:
                break

            if i < max_tries:
                if POS_LEARN_BASE_DELAY_SEC > 0:
                    await asyncio.sleep(POS_LEARN_BASE_DELAY_SEC)
                await asyncio.sleep(_jitter(self.delay))

        log.event(
            "TXLEARN_TXONLY",
            sig=tx_sig,
            tx_found=bool(last_tx),
            attempts=attempts + min(max_tries, self.tries),
            ms=int((time.time() - t0) * 1000),
        )
        return self._result(
            t0=t0,
            tx_found=bool(last_tx),
            sig=tx_sig,
            mint=None,
            token_program=None,
            status=last_status,
            tx=last_tx,
            owner_ata=None,
            qty_raw=0,
            qty_ui=0.0,
            decimals=0,
            attempts=attempts + min(max_tries, self.tries),
            block_time=None,
        )

    def _result(
        self,
        *,
        t0: float,
        tx_found: bool,
        sig: str,
        mint: str | None,
        token_program: str | None,
        status: dict | None,
        tx: dict | None,
        owner_ata: str | None,
        qty_raw: int,
        qty_ui: float,
        decimals: int,
        attempts: int,
        block_time: int | None,
    ) -> dict[str, Any]:
        return {
            "mint": mint,
            "token_program": token_program,
            "owner": self.owner or None,
            "owner_ata": owner_ata,
            "qty_raw": int(qty_raw),
            "qty_ui": float(qty_ui),
            "decimals": int(decimals),
            "sig": sig,
            "block_time": block_time,
            "status": status,
            "transaction": tx,
            "tx_found": bool(tx_found),
            "attempts": int(attempts),
            "elapsed_ms": int((time.time() - t0) * 1000),
        }


# ──────────────────────────────────────────────────────────────────────────────
# Top-level helpers (stable public API)
# ──────────────────────────────────────────────────────────────────────────────


async def learn_position(
    mint: str,
    tx_sig: str,
    *,
    owner: str | None = None,
    rpc_url_tx: str = TX_RPC_URL,
    rpc_url_general: str = RPC_URL,
) -> dict[str, Any]:
    """
    Use when you already know the real mint.
    """
    learner = TxLearner(rpc_url_tx=rpc_url_tx, rpc_url_general=rpc_url_general, owner=owner)
    try:
        return await learner.learn(mint, tx_sig)
    finally:
        await learner.close()


async def resolve_then_learn_position(
    raw_id_or_mint: str,
    tx_sig: str,
    *,
    owner: str | None = None,
    rpc_url_tx: str = TX_RPC_URL,
    rpc_url_general: str = RPC_URL,
) -> dict[str, Any]:
    """
    Preferred when you have a pump id or uncertain mint.
    """
    learner = TxLearner(rpc_url_tx=rpc_url_tx, rpc_url_general=rpc_url_general, owner=owner)
    try:
        return await learner.resolve_then_learn(raw_id_or_mint, tx_sig)
    finally:
        await learner.close()


__all__ = [
    "RPC_URL",
    "TX_COMMITMENT",
    "TX_RPC_URL",
    "TxLearner",
    "_Rpc",
    "learn_position",
    "resolve_mint_best_effort",
    "resolve_then_learn_position",
]
