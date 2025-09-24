# utils_pkg/txlearn.py
from __future__ import annotations

import asyncio
import os
import random
import time
from typing import Any, Dict, List, Optional, Tuple

import aiohttp
from logging_setup import get_logger, bump

# Budget hooks (soft gates). Fail open if module not present.
try:
    from utils_pkg import budget  # type: ignore
except Exception:  # graceful no-ops
    class _BudgetNoop:
        def before_quote(self, units: int = 1) -> bool: return True
        def after_quote(self, units: int = 1) -> None:  return None
        def before_rpc(self, method: str, *, weight: Optional[int] = None) -> bool: return True
        def after_rpc(self, method: str, *, weight: Optional[int] = None) -> None:  return None
    budget = _BudgetNoop()  # type: ignore

# ──────────────────────────────────────────────────────────────────────────────
# Config (env-driven)
# ──────────────────────────────────────────────────────────────────────────────

# Prefer a dedicated TX RPC for faster signature/tx visibility.
RPC_URL = os.getenv("SOL_RPC_URL", "").strip() or "https://api.mainnet-beta.solana.com"
TX_RPC_URL = os.getenv("SOL_RPC_TX_URL", "").strip() or RPC_URL

# Optional Helius JSON-RPC fallback (same method set) and Parse API.
HELIUS_API_KEY = (os.getenv("HELIUS_API_KEY") or "").strip()
HELIUS_RPC_URL = f"https://mainnet.helius-rpc.com/?api-key={HELIUS_API_KEY}" if HELIUS_API_KEY else ""
HELIUS_PARSE_URL = f"https://api.helius.xyz/v0/transactions?api-key={HELIUS_API_KEY}" if HELIUS_API_KEY else ""

# Commitment for transaction fetches / status checks.
TX_COMMITMENT = (os.getenv("PF_TXLEARN_COMMITMENT", "processed").strip().lower() or "processed")

# Learner pacing.
TX_TRIES = int(os.getenv("PF_TXLEARN_TRIES", "20"))
TX_DELAY_SEC = float(os.getenv("PF_TXLEARN_DELAY_SEC", "1.2"))
TX_WARMUP_SEC = float(os.getenv("PF_TXLEARN_WARMUP_SEC", "2.0"))
POS_LEARN_BASE_DELAY_SEC = float(os.getenv("PF_POS_LEARN_BASE_DELAY_SEC", "0"))

# Resolver retries before dropping to tx-only mode.
RESOLVE_TRIES = int(os.getenv("PF_TXLEARN_RESOLVE_TRIES", "6"))
RESOLVE_DELAY_SEC = float(os.getenv("PF_TXLEARN_RESOLVE_DELAY_SEC", "0.8"))

# Optional debug spew.
DEBUG = (os.getenv("PF_DEBUG_TX", "0").strip().lower() in {"1", "true", "yes", "y", "on"})

# Status poll cadence floor (align with router if you set PF_STATUS_POLL_SEC).
STATUS_POLL_SEC = max(0.2, float(os.getenv("PF_STATUS_POLL_SEC", "0.6")))

# Tiny RPC caches to avoid double-polling same sig every tick.
RPC_CACHE_TTL_SEC = max(0.0, float(os.getenv("PF_RPC_CACHE_TTL_SEC", "1.5")))

# Program IDs
SPL_TOKEN = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
TOKEN_2022 = "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb"

# Logger
log = get_logger()

# Simple base58 whitelist (fast sanity check; not a full validator).
_BASE58 = set("123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz")

_UA = {"user-agent": "pnlsniper/1.0 (+txlearn)"}


def _is_likely_pubkey(s: str) -> bool:
    s = (s or "").strip()
    if s.endswith("pump"):
        return False
    if not (32 <= len(s) <= 64):
        return False
    return all(ch in _BASE58 for ch in s)


def _is_probably_pump_id(s: str) -> bool:
    s = (s or "").strip()
    return s.endswith("pump") and len(s) >= 36


def _jitter(t: float, frac: float = 0.25) -> float:
    if t <= 0:
        return 0.0
    d = t * frac
    return max(0.0, t + random.uniform(-d, d))


# ──────────────────────────────────────────────────────────────────────────────
# Minimal JSON-RPC client (budget-aware + tiny TTL caches)
# ──────────────────────────────────────────────────────────────────────────────

class _SoftBudgetBlock(Exception):
    """Internal: budget asked us to skip a call this tick."""
    pass


class _Rpc:
    def __init__(self, url: str):
        self.url = url
        self._session: Optional[aiohttp.ClientSession] = None
        self._req_id = 1
        # tiny per-client caches
        self._status_cache: Dict[str, Tuple[float, dict]] = {}
        self._tx_cache: Dict[str, Tuple[float, dict]] = {}
        self._last_status_call_ts: float = 0.0

    @property
    def present(self) -> bool:
        return bool(self.url)

    async def _ensure(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            timeout = aiohttp.ClientTimeout(total=20, connect=7)
            self._session = aiohttp.ClientSession(timeout=timeout, headers=_UA)
        return self._session

    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()

    async def _call_raw(self, method: str, params: list, *, weight: int = 1) -> Any:
        if not self.present:
            raise RuntimeError("RPC URL not set")
        # soft budget gate
        if not budget.before_rpc(method, weight=weight):
            raise _SoftBudgetBlock()

        session = await self._ensure()
        self._req_id += 1
        payload = {"jsonrpc": "2.0", "id": self._req_id, "method": method, "params": params}
        try:
            async with session.post(self.url, json=payload) as resp:
                txt = await resp.text()
                if resp.status != 200:
                    raise RuntimeError(f"HTTP {resp.status}: {txt[:200]}")
                try:
                    data = await resp.json(content_type=None)
                except Exception:
                    raise RuntimeError(f"json parse error: {txt[:200]}")
        finally:
            # always release budget “ticket” for this method
            budget.after_rpc(method, weight=weight)

        if "error" in data and data["error"]:
            raise RuntimeError(str(data["error"]))
        return data.get("result")

    # Convenience wrappers (all return None on soft budget blocks or network errors)

    async def get_signature_status(self, sig: str) -> Optional[dict]:
        # rate-limit status polls by STATUS_POLL_SEC floor
        now = time.time()
        if (now - self._last_status_call_ts) < STATUS_POLL_SEC:
            # try cached answer if recent
            rec = self._status_cache.get(sig)
            if rec and rec[0] >= now:
                return rec[1]
        self._last_status_call_ts = now

        # cache hit
        if RPC_CACHE_TTL_SEC > 0:
            rec = self._status_cache.get(sig)
            if rec and rec[0] >= now:
                return rec[1]

        try:
            res = await self._call_raw("getSignatureStatuses", [[sig], {"searchTransactionHistory": True}], weight=1)
            arr = (res or {}).get("value") or []
            out = arr[0] if arr else None
            if out and RPC_CACHE_TTL_SEC > 0:
                self._status_cache[sig] = (now + RPC_CACHE_TTL_SEC, out)
            return out
        except _SoftBudgetBlock:
            return None
        except Exception:
            return None

    async def get_transaction(self, sig: str, commitment: str) -> Optional[dict]:
        now = time.time()
        # cache hit
        if RPC_CACHE_TTL_SEC > 0:
            rec = self._tx_cache.get(sig)
            if rec and rec[0] >= now:
                return rec[1]

        # attempt sequence with compatible encodings
        for attempt in (1, 2, 3):
            try:
                if attempt == 1:
                    res = await self._call_raw(
                        "getTransaction",
                        [sig, {"commitment": commitment, "encoding": "jsonParsed",
                               "maxSupportedTransactionVersion": 0}],
                        weight=2,
                    )
                elif attempt == 2:
                    res = await self._call_raw(
                        "getTransaction",
                        [sig, {"commitment": commitment, "encoding": "jsonParsed"}],
                        weight=2,
                    )
                else:
                    res = await self._call_raw(
                        "getTransaction",
                        [sig, {"commitment": commitment, "encoding": "base64"}],
                        weight=2,
                    )
                if res and RPC_CACHE_TTL_SEC > 0:
                    self._tx_cache[sig] = (now + RPC_CACHE_TTL_SEC, res)
                return res
            except _SoftBudgetBlock:
                return None
            except Exception:
                await asyncio.sleep(0.2)
        return None

    async def get_multiple_accounts(self, addrs: List[str]) -> Optional[dict]:
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

    async def get_account_info(self, addr: str) -> Optional[dict]:
        try:
            return await self._call_raw("getAccountInfo", [addr, {"encoding": "jsonParsed"}], weight=1)
        except _SoftBudgetBlock:
            return None
        except Exception:
            return None

    async def get_token_accounts_by_owner_mint(self, owner: str, mint: str) -> List[dict]:
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

    async def get_balance(self, owner: str) -> Optional[dict]:
        """
        Returns {"value": <lamports>} or None on error/soft budget block.
        """
        try:
            return await self._call_raw("getBalance", [owner, {"commitment": TX_COMMITMENT}], weight=1)
        except _SoftBudgetBlock:
            return None
        except Exception:
            return None


# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────

def _extract_ui_amount_from_acc(acc: dict) -> Tuple[int, int, float]:
    """
    (raw, decimals, ui) from getTokenAccountsByOwner parsed account.
    """
    try:
        info = acc["account"]["data"]["parsed"]["info"]
        amt = info["tokenAmount"]
        raw = int(amt["amount"])
        dec = int(amt.get("decimals", 0))
        ui = float(amt.get("uiAmount", raw / (10 ** dec) if dec else raw))
        return raw, dec, ui
    except Exception:
        return 0, 0, 0.0


def _pick_best_owner_ata(accounts: List[dict]) -> Tuple[Optional[str], int, int, float]:
    """
    Choose the ATA with the largest UI balance (common when dust is split).
    Returns (pubkey, raw, decimals, ui).
    """
    best_pub: Optional[str] = None
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
    accounts = await rpc.get_token_accounts_by_owner_mint(owner, mint)
    return sum(_extract_ui_amount_from_acc(a)[2] for a in accounts)


async def _mint_program_owner(rpc: _Rpc, mint: str) -> str:
    """
    Returns SPL token program id that owns the mint account (SPL Token vs Token-2022).
    """
    info = await rpc.get_account_info(mint)
    try:
        owner = ((info or {}).get("value") or {}).get("owner")
        if owner in (SPL_TOKEN, TOKEN_2022):
            return owner
    except Exception:
        pass
    return SPL_TOKEN  # default safe guess


async def _is_valid_mint_account(rpc: _Rpc, addr: str) -> bool:
    if not _is_likely_pubkey(addr):
        return False
    res = await rpc.get_multiple_accounts([addr])
    try:
        v = (res or {}).get("value") or []
        if not v or not v[0]:
            return False
        data = v[0].get("data")
        if isinstance(data, dict):
            parsed = data.get("parsed") or {}
            return parsed.get("type") == "mint"
    except Exception:
        return False
    return False


async def _hel_parse_for_mint(session: aiohttp.ClientSession, sig: str, owner: Optional[str]) -> Optional[str]:
    """
    Use Helius Parse API to infer the purchased mint (owner-first, then largest positive delta).
    """
    if not HELIUS_PARSE_URL:
        return None

    # budget wrap (treat as an RPC-like call)
    if not budget.before_rpc("helius_parse", weight=2):
        return None

    try:
        async with session.post(HELIUS_PARSE_URL, json={"transactions": [sig]}, headers=_UA) as resp:
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
    best_owner: Optional[Tuple[str, float]] = None
    best_any: Optional[Tuple[str, float]] = None
    for change in tbc:
        try:
            mint = change.get("mint")
            raw = float(change.get("rawTokenAmount", {}).get("tokenAmount", 0))
            dec = int(change.get("rawTokenAmount", {}).get("decimals", 0))
            ui = float(change.get("tokenAmount", raw / (10 ** dec) if dec else raw))
            if not (_is_likely_pubkey(mint) and ui > 0):
                continue
            who = change.get("userAccount") or change.get("owner")
            if owner and who == owner:
                if best_owner is None or ui > best_owner[1]:
                    best_owner = (mint, ui)
            if best_any is None or ui > best_any[1]:
                best_any = (mint, ui)
        except Exception:
            continue

    if best_owner:
        return best_owner[0]
    if best_any:
        return best_any[0]

    # Fallback: look for "mint" fields in events
    events = tx.get("events") or {}
    for group in events.values():
        seq = group if isinstance(group, list) else [group]
        for ev in seq:
            if isinstance(ev, dict):
                m = ev.get("mint") or ev.get("mintAddress") or ev.get("token")
                if _is_likely_pubkey(m):
                    return m
    return None


async def _infer_mint_from_rpc_tx(rpcs: List[_Rpc], sig: str, owner: Optional[str]) -> Optional[str]:
    """
    Infer mint from meta.postTokenBalances via RPC; tries primary then Helius RPC.
    """
    for rpc in rpcs:
        if not rpc.present:
            continue
        try:
            tx = await rpc.get_transaction(sig, TX_COMMITMENT)
        except Exception:
            tx = None
        if not tx:
            continue

        meta = (tx or {}).get("meta") or {}
        post = meta.get("postTokenBalances") or []
        if not post:
            continue

        # Prefer entries owned by `owner`
        best: Optional[Tuple[str, int]] = None
        for b in post:
            try:
                if owner and b.get("owner") and b.get("owner") != owner:
                    continue
                m = b.get("mint")
                ui = b.get("uiTokenAmount") or {}
                amt = int(str(ui.get("amount", "0")))
                if amt > 0 and _is_likely_pubkey(m):
                    if best is None or amt > best[1]:
                        best = (m, amt)
            except Exception:
                continue
        if best:
            return best[0]

        # Fallback: largest post amount regardless of owner
        for b in post:
            try:
                m = b.get("mint")
                ui = b.get("uiTokenAmount") or {}
                amt = int(str(ui.get("amount", "0")))
                if amt > 0 and _is_likely_pubkey(m):
                    if best is None or amt > best[1]:
                        best = (m, amt)
            except Exception:
                continue
        if best:
            return best[0]
    return None


# ──────────────────────────────────────────────────────────────────────────────
# Learner
# ──────────────────────────────────────────────────────────────────────────────

class TxLearner:
    """
    After a BUY, resolve the true mint and wait until:
      - the transaction is visible, and
      - your wallet shows a non-zero balance for that mint.

    Returns a rich dict suitable for your position store.
    """

    def __init__(
        self,
        rpc_url_tx: str = TX_RPC_URL,
        rpc_url_general: str = RPC_URL,
        owner: Optional[str] = None,
        commitment: str = TX_COMMITMENT,
        tries: int = TX_TRIES,
        delay_sec: float = TX_DELAY_SEC,
        warmup_sec: float = TX_WARMUP_SEC,
    ) -> None:
        self.tx_primary = _Rpc(rpc_url_tx)
        self.tx_helius = _Rpc(HELIUS_RPC_URL) if HELIUS_RPC_URL else _Rpc("")
        self.rpc = _Rpc(rpc_url_general)

        self.owner = (owner or os.getenv("PF_PP_ACCOUNT", "")).strip()
        self.commitment = commitment
        self.tries = max(1, int(tries))
        self.delay = max(STATUS_POLL_SEC, float(delay_sec))  # never tighter than STATUS_POLL_SEC
        self.warmup = max(0.0, float(warmup_sec))

    async def close(self) -> None:
        await self.tx_primary.close()
        if self.tx_helius.present:
            await self.tx_helius.close()
        await self.rpc.close()

    # ———— Resolve → Learn (preferred path) ————
    async def resolve_then_learn(self, raw_id_or_mint: str, tx_sig: str) -> Dict[str, Any]:
        start_ts = time.time()
        if self.warmup > 0:
            await asyncio.sleep(self.warmup)

        resolved: Optional[str] = None
        attempts = 0

        # 1) Try a few resolve passes
        for attempts in range(1, RESOLVE_TRIES + 1):
            resolved = await resolve_mint_best_effort(
                [self.tx_primary, self.tx_helius],
                self.rpc,
                raw_id_or_mint,
                signature=tx_sig,
                owner=self.owner,
            )
            if resolved:
                break
            if attempts < RESOLVE_TRIES:
                await asyncio.sleep(_jitter(RESOLVE_DELAY_SEC))

        if not resolved:
            log.event("TXLEARN_RESOLVE_FAIL", raw=raw_id_or_mint, sig=tx_sig, attempts=attempts)
            return await self._learn_tx_only(tx_sig, attempts=attempts, t0=start_ts)

        log.event("TXLEARN_RESOLVED_MINT", raw=raw_id_or_mint, mint=resolved, sig=tx_sig, attempts=attempts)
        return await self.learn(resolved, tx_sig, t0=start_ts)

    # ———— Learn with known/validated mint ————
    async def learn(self, mint_or_id: str, tx_sig: str, *, t0: Optional[float] = None) -> Dict[str, Any]:
        t0 = t0 or time.time()
        original = (mint_or_id or "").strip()

        # If this doesn't look like a mint, attempt one quick resolve pass.
        mint = original
        if (not _is_likely_pubkey(mint)) or mint.endswith("pump"):
            quick = await resolve_mint_best_effort(
                [self.tx_primary, self.tx_helius],
                self.rpc,
                original,
                signature=tx_sig,
                owner=self.owner,
            )
            if quick:
                mint = quick

        if not _is_likely_pubkey(mint):
            log.event("TXLEARN_UNRESOLVED_ID_TXONLY", raw=original, sig=tx_sig)
            return await self._learn_tx_only(tx_sig, t0=t0)

        # Validate the mint account type right away.
        if not await _is_valid_mint_account(self.rpc, mint):
            log.event("TXLEARN_MINT_NOT_READY", mint=mint, sig=tx_sig)
            return await self._learn_tx_only(tx_sig, t0=t0)

        # Identify token program (SPL Token vs Token-2022)
        token_program = await _mint_program_owner(self.rpc, mint)

        # Poll for tx + owned balance
        last_tx: Optional[dict] = None
        last_status: Optional[dict] = None
        last_owner_ui = 0.0
        attempt = 0

        for attempt in range(1, self.tries + 1):
            # Signature status (cheap)
            for txrpc in (self.tx_primary, self.tx_helius):
                if not txrpc.present:
                    continue
                st = await txrpc.get_signature_status(tx_sig)
                if st:
                    last_status = st
                    break

            # Early exit on error
            if last_status and last_status.get("err") is not None:
                log.event("TXLEARN_TX_ERROR", mint=mint, sig=tx_sig, err=str(last_status.get("err")))
                return self._result(
                    t0=t0, tx_found=False, sig=tx_sig, mint=mint, token_program=token_program,
                    status=last_status, tx=None, owner_ata=None, qty_raw=0, qty_ui=0.0, decimals=0, attempts=attempt
                )

            # Try to fetch the tx (some RPCs are slow to index)
            for txrpc in (self.tx_primary, self.tx_helius):
                if not txrpc.present:
                    continue
                tx = await txrpc.get_transaction(tx_sig, self.commitment)
                if tx:
                    last_tx = tx
                    break

            # Check owned balance for the mint
            try:
                last_owner_ui = await _sum_owned_ui_amount(self.rpc, self.owner, mint)
            except Exception:
                last_owner_ui = 0.0

            if last_owner_ui > 0:
                break  # we own it—good enough to proceed

            if attempt < self.tries:
                if POS_LEARN_BASE_DELAY_SEC > 0:
                    await asyncio.sleep(POS_LEARN_BASE_DELAY_SEC)
                await asyncio.sleep(_jitter(self.delay))

        # With a positive balance, find the best owner ATA and precise qty/decimals.
        owner_ata = None
        qty_raw = 0
        decimals = 0
        qty_ui = 0.0

        if last_owner_ui > 0:
            accounts = await self.rpc.get_token_accounts_by_owner_mint(self.owner, mint)
            owner_ata, qty_raw, decimals, qty_ui = _pick_best_owner_ata(accounts)

        block_time = None
        if last_tx and "blockTime" in last_tx:
            block_time = last_tx["blockTime"]

        result = self._result(
            t0=t0, tx_found=bool(last_tx), sig=tx_sig, mint=mint, token_program=token_program,
            status=last_status, tx=last_tx, owner_ata=owner_ata, qty_raw=qty_raw,
            qty_ui=qty_ui, decimals=decimals, attempts=attempt, block_time=block_time
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

    # ———— Transaction-only visibility when mint can't be resolved/validated ————
    async def _learn_tx_only(self, tx_sig: str, attempts: int = 0, *, t0: Optional[float] = None) -> Dict[str, Any]:
        t0 = t0 or time.time()
        last_tx: Optional[dict] = None
        last_status: Optional[dict] = None

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
            t0=t0, tx_found=bool(last_tx), sig=tx_sig, mint=None, token_program=None,
            status=last_status, tx=last_tx, owner_ata=None, qty_raw=0, qty_ui=0.0, decimals=0,
            attempts=attempts + min(max_tries, self.tries)
        )

    # ———— Result shaper ————
    def _result(
        self,
        *,
        t0: float,
        tx_found: bool,
        sig: str,
        mint: Optional[str],
        token_program: Optional[str],
        status: Optional[dict],
        tx: Optional[dict],
        owner_ata: Optional[str],
        qty_raw: int,
        qty_ui: float,
        decimals: int,
        attempts: int,
        block_time: Optional[int] = None,
    ) -> Dict[str, Any]:
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
# Resolver (best effort)
# ──────────────────────────────────────────────────────────────────────────────

async def resolve_mint_best_effort(
    tx_rpcs: List[_Rpc] | Tuple[_Rpc, ...],
    rpc_general: _Rpc,
    raw_id_or_mint: str,
    *,
    signature: Optional[str],
    owner: Optional[str],
) -> Optional[str]:
    raw = (raw_id_or_mint or "").strip()

    # 1) Helius Parse API (if available)
    if signature and HELIUS_API_KEY:
        try:
            session = await (tx_rpcs[0]._ensure() if tx_rpcs else aiohttp.ClientSession(headers=_UA))
            m = await _hel_parse_for_mint(session, signature, owner)
            if m:
                return m
        except Exception:
            pass

    # 2) RPC getTransaction inference
    m = await _infer_mint_from_rpc_tx([r for r in tx_rpcs if r.present], signature, owner) if signature else None
    if m:
        return m

    # 3) Local pump resolver(s)
    if _is_probably_pump_id(raw):
        try:
            from utils_pkg.pump_trader import (
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

    # 4) Direct validation if caller already passed a mint
    if _is_likely_pubkey(raw):
        if await _is_valid_mint_account(rpc_general, raw):
            return raw

    return None


async def _maybe_await(x):
    if asyncio.iscoroutine(x):
        return await x
    return x


# ──────────────────────────────────────────────────────────────────────────────
# Top-level helpers
# ──────────────────────────────────────────────────────────────────────────────

async def learn_position(
    mint: str,
    tx_sig: str,
    *,
    owner: Optional[str] = None,
    rpc_url_tx: str = TX_RPC_URL,
    rpc_url_general: str = RPC_URL,
) -> Dict[str, Any]:
    """
    Use when you already know the real mint.
    Returns a dict with mint, token_program, owner_ata, qty_raw, qty_ui, decimals, status/tx objects, etc.
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
    owner: Optional[str] = None,
    rpc_url_tx: str = TX_RPC_URL,
    rpc_url_general: str = RPC_URL,
) -> Dict[str, Any]:
    """
    Preferred when you have a pump id or uncertain mint.
    Returns a dict with mint, token_program, owner_ata, qty_raw, qty_ui, decimals, status/tx objects, etc.
    """
    learner = TxLearner(rpc_url_tx=rpc_url_tx, rpc_url_general=rpc_url_general, owner=owner)
    try:
        return await learner.resolve_then_learn(raw_id_or_mint, tx_sig)
    finally:
        await learner.close()


__all__ = [
    "RPC_URL",
    "TX_RPC_URL",
    "TX_COMMITMENT",
    "_Rpc",
    "learn_position",
    "resolve_then_learn_position",
]
