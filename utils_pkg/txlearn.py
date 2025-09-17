# utils_pkg/txlearn.py
from __future__ import annotations

import asyncio
import os
import random
from typing import Any, Dict, List, Optional, Tuple

import aiohttp

# ------------------------ env + defaults ------------------------

RPC_URL = os.getenv("SOL_RPC_URL", "").strip() or "https://api.mainnet-beta.solana.com"

TX_COMMITMENT = (os.getenv("PF_TXLEARN_COMMITMENT", "confirmed").strip().lower()
                 or "confirmed")  # "processed" | "confirmed" | "finalized"

TX_TRIES      = int(os.getenv("PF_TXLEARN_TRIES", "20"))
TX_DELAY_SEC  = float(os.getenv("PF_TXLEARN_DELAY_SEC", "1.2"))
TX_WARMUP_SEC = float(os.getenv("PF_TXLEARN_WARMUP_SEC", "2.0"))

# Optional base delay between learner cycles (helps keep noise down)
POS_LEARN_BASE_DELAY_SEC = float(os.getenv("PF_POS_LEARN_BASE_DELAY_SEC", "0"))

# Resolver knobs (pre-learn)
RESOLVE_TRIES     = int(os.getenv("PF_TXLEARN_RESOLVE_TRIES", "6"))
RESOLVE_DELAY_SEC = float(os.getenv("PF_TXLEARN_RESOLVE_DELAY_SEC", "0.8"))

HELIUS_API_KEY   = (os.getenv("HELIUS_API_KEY") or "").strip()
HELIUS_PARSE_URL = f"https://api.helius.xyz/v0/transactions?api-key={HELIUS_API_KEY}" if HELIUS_API_KEY else ""

# Optional noisy debug
DEBUG = str(os.getenv("PF_DEBUG_TX", "0")).strip().lower() in {"1", "true", "yes", "y", "on"}

# Simple base58 char whitelist (NOT a full validator—just fast sanity)
_BASE58 = set("123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz")


def _is_likely_pubkey(s: str) -> bool:
    s = (s or "").strip()
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


# ------------------------ HTTP/RPC helpers ------------------------

class _Rpc:
    def __init__(self, url: str) -> None:
        self.url = url
        self._session: Optional[aiohttp.ClientSession] = None
        self._req_id = 1

    async def _ensure(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            timeout = aiohttp.ClientTimeout(total=20, connect=7)
            self._session = aiohttp.ClientSession(timeout=timeout)
        return self._session

    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()

    async def call(self, method: str, params: list) -> Any:
        """
        Barebones JSON-RPC 2.0 call with defensive parsing.
        """
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
        except Exception as e:
            raise RuntimeError(str(e))
        if "error" in data and data["error"]:
            raise RuntimeError(str(data["error"]))
        return data.get("result")

    # --- specific RPC helpers ---

    async def get_signature_status(self, sig: str) -> Optional[dict]:
        try:
            res = await self.call("getSignatureStatuses", [[sig], {"searchTransactionHistory": True}])
            arr = (res or {}).get("value") or []
            return arr[0] if arr else None
        except Exception:
            return None

    async def get_transaction(self, sig: str, commitment: str) -> Optional[dict]:
        """
        Try several encodings/params so we work across mixed RPC providers.
        """
        for attempt in (1, 2, 3):
            try:
                if attempt == 1:
                    return await self.call(
                        "getTransaction",
                        [sig, {"commitment": commitment, "encoding": "jsonParsed",
                               "maxSupportedTransactionVersion": 0}],
                    )
                elif attempt == 2:
                    return await self.call(
                        "getTransaction",
                        [sig, {"commitment": commitment, "encoding": "jsonParsed"}],
                    )
                else:
                    return await self.call(
                        "getTransaction",
                        [sig, {"commitment": commitment, "encoding": "base64"}],
                    )
            except Exception:
                await asyncio.sleep(0.2)
        return None

    async def get_token_accounts_by_owner_mint(self, owner: str, mint: str) -> List[dict]:
        """
        Query by 'mint' (works for both Token program and Token-2022).
        Returns a list of parsed accounts.
        """
        try:
            res = await self.call(
                "getTokenAccountsByOwner",
                [
                    owner,
                    {"mint": mint},
                    {"encoding": "jsonParsed", "commitment": TX_COMMITMENT},
                ],
            )
            return (res or {}).get("value") or []
        except Exception as e:
            if DEBUG:
                print(f"[txlearn] getTokenAccountsByOwner failed for {mint}: {e}", flush=True)
            return []

    async def get_multiple_accounts(self, addrs: List[str]) -> Optional[dict]:
        if not addrs:
            return None
        try:
            return await self.call(
                "getMultipleAccounts",
                [addrs, {"encoding": "jsonParsed", "commitment": TX_COMMITMENT}],
            )
        except Exception:
            return None


# ------------------------ balance helpers ------------------------

def _extract_ui_amount(token_acc: dict) -> Tuple[int, int, float]:
    """
    From a single getTokenAccountsByOwner account entry, extract:
      (raw amount as int, decimals, uiAmount float)
    """
    try:
        info = token_acc["account"]["data"]["parsed"]["info"]
        amt = info["tokenAmount"]
        raw = int(amt["amount"])
        dec = int(amt.get("decimals", 0))
        ui  = float(amt.get("uiAmount", raw / (10 ** dec) if dec else raw))
        return raw, dec, ui
    except Exception:
        return 0, 0, 0.0


async def _sum_owned_ui_amount(rpc: _Rpc, owner: str, mint: str) -> float:
    accounts = await rpc.get_token_accounts_by_owner_mint(owner, mint)
    if DEBUG:
        print(f"[txlearn] owner ATAs for {mint}: {len(accounts)} found", flush=True)
    total_ui = 0.0
    for acc in accounts:
        _, _, ui = _extract_ui_amount(acc)
        total_ui += ui
    return total_ui


# ------------------------ mint validation & resolution ------------------------

async def _is_valid_mint_account(rpc: _Rpc, addr: str) -> bool:
    """
    Quick mint sanity: RPC getMultipleAccounts parsed.type == 'mint'
    """
    if not _is_likely_pubkey(addr):
        return False
    res = await rpc.get_multiple_accounts([addr])
    try:
        v = (res or {}).get("value") or []
        if not v or not v[0]:
            return False
        data = v[0].get("data")
        if isinstance(data, dict):  # jsonParsed
            parsed = data.get("parsed") or {}
            if parsed.get("type") == "mint":
                return True
    except Exception:
        return False
    return False


async def _hel_parse_for_mint(session: aiohttp.ClientSession, sig: str, owner: Optional[str]) -> Optional[str]:
    """
    Ask Helius Parse API for the transaction and infer the purchased mint
    from token balance changes or events.
    """
    if not HELIUS_PARSE_URL:
        return None
    try:
        async with session.post(HELIUS_PARSE_URL, json={"transactions": [sig]}) as resp:
            if resp.status != 200:
                return None
            data = await resp.json(content_type=None)
    except Exception:
        return None

    if not isinstance(data, list) or not data:
        return None
    tx = data[0] or {}

    # 1) tokenBalanceChanges: look for +amount on our owner (if given)
    tbc = tx.get("tokenBalanceChanges") or []
    best: Optional[Tuple[str, float]] = None  # (mint, delta_ui)
    for change in tbc:
        try:
            who = change.get("userAccount") or change.get("owner")
            if owner and who and who != owner:
                continue
            mint = change.get("mint")
            raw = float(change.get("rawTokenAmount", {}).get("tokenAmount", 0))
            dec = int(change.get("rawTokenAmount", {}).get("decimals", 0))
            ui  = float(change.get("tokenAmount", raw / (10 ** dec) if dec else raw))
            if ui > 0 and _is_likely_pubkey(mint):
                if best is None or ui > best[1]:
                    best = (mint, ui)
        except Exception:
            continue
    if best:
        return best[0]

    # 2) fallback: events list sometimes includes "mint" field
    events = tx.get("events") or {}
    for group in events.values():
        seq = group if isinstance(group, list) else [group]
        for ev in seq:
            if isinstance(ev, dict):
                m = ev.get("mint") or ev.get("mintAddress") or ev.get("token")
                if _is_likely_pubkey(m):
                    return m
    return None


async def _maybe_await(x):
    if asyncio.iscoroutine(x):
        return await x
    return x


async def resolve_mint_best_effort(
    rpc: _Rpc,
    raw_id_or_mint: str,
    signature: Optional[str],
    owner: Optional[str],
) -> Optional[str]:
    """
    Resolve a real spl-token mint address from:
      1) Helius parse of `signature` (best)
      2) local pump resolver if the id ends with 'pump'
      3) direct RPC validation if the input is already a mint
    """
    raw = (raw_id_or_mint or "").strip()

    # 1) Helius parse by signature (if available)
    if signature and HELIUS_API_KEY:
        session = await rpc._ensure()
        m = await _hel_parse_for_mint(session, signature, owner)
        if m:
            return m

    # 2) pump id -> mint via your utils_pkg.pump_trader resolvers
    if _is_probably_pump_id(raw):
        try:
            # Lazy import to avoid cycles
            from utils_pkg.pump_trader import resolve_true_mint as _rtm, resolve_true_mint_from_pump_id as _rtm_pid
        except Exception:
            _rtm = None  # type: ignore
            _rtm_pid = None  # type: ignore

        if signature and _rtm:
            try:
                m = await _maybe_await(_rtm(raw, signature=signature))  # supports async or sync impl
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

    # 3) If it's already a plausible key, verify it's a mint account
    if _is_likely_pubkey(raw):
        if await _is_valid_mint_account(rpc, raw):
            return raw

    return None


# ------------------------ learner ------------------------

class TxLearner:
    """
    Polls a recent buy transaction until:
      - the RPC returns it, and/or
      - the wallet shows a non-zero balance for the mint.

    Intended to be called *after* you submit a buy so the exit engine
    can start managing the position.
    """

    def __init__(
        self,
        rpc_url: str = RPC_URL,
        owner: Optional[str] = None,
        commitment: str = TX_COMMITMENT,
        tries: int = TX_TRIES,
        delay_sec: float = TX_DELAY_SEC,
        warmup_sec: float = TX_WARMUP_SEC,
    ) -> None:
        self.rpc = _Rpc(rpc_url)
        self.owner = (owner or os.getenv("PF_PP_ACCOUNT", "")).strip()
        self.commitment = commitment
        self.tries = max(1, int(tries))
        self.delay = max(0.1, float(delay_sec))
        self.warmup = max(0.0, float(warmup_sec))

    async def close(self) -> None:
        await self.rpc.close()

    async def resolve_then_learn(self, raw_id_or_mint: str, tx_sig: str) -> Dict[str, Any]:
        """
        Fully managed path:
          - Try to resolve a real token mint (signature → pump id → rpc).
          - If resolved, learn balances/tx.
          - If not yet resolved, retry a few times with backoff, then return best-effort.
        """
        # Warmup lets the cluster/indexers catch up
        if self.warmup > 0:
            await asyncio.sleep(self.warmup)

        resolved: Optional[str] = None
        attempts = 0

        for attempts in range(1, RESOLVE_TRIES + 1):
            resolved = await resolve_mint_best_effort(self.rpc, raw_id_or_mint, signature=tx_sig, owner=self.owner)
            if resolved:
                break
            if attempts < RESOLVE_TRIES:
                await asyncio.sleep(_jitter(RESOLVE_DELAY_SEC))

        if not resolved:
            print(f"[txlearn] mint unresolved for id='{raw_id_or_mint}' sig={tx_sig} after {attempts} attempts", flush=True)
            # Best effort: try learning only from tx visibility (balance probe would spam if mint invalid)
            return await self._learn_tx_only(tx_sig, attempts=attempts)

        # With a real mint, proceed normally
        return await self.learn(resolved, tx_sig)

    async def _learn_tx_only(self, tx_sig: str, attempts: int = 0) -> Dict[str, Any]:
        """
        Only check signature status + transaction presence, skip mint balance probes.
        Used when we can't resolve a safe mint yet.
        """
        tx_obj: Optional[dict] = None
        status: Optional[dict] = None

        # A small bounded probe so we still get visibility
        max_tries = min(5, self.tries)
        i = 0
        for i in range(1, max_tries + 1):
            status = await self.rpc.get_signature_status(tx_sig)

            # Early-exit on explicit tx failure
            if status and status.get("err") is not None:
                return {
                    "tx_found": False,
                    "owner_balance_ui": 0.0,
                    "attempts": attempts + i,
                    "status": status,
                    "transaction": None,
                }

            tx_obj = await self.rpc.get_transaction(tx_sig, self.commitment)
            if tx_obj:
                break
            if i < max_tries:
                if POS_LEARN_BASE_DELAY_SEC > 0:
                    await asyncio.sleep(POS_LEARN_BASE_DELAY_SEC)
                await asyncio.sleep(_jitter(self.delay))

        return {
            "tx_found": tx_obj is not None,
            "owner_balance_ui": 0.0,
            "attempts": attempts + i,
            "status": status,
            "transaction": tx_obj,
        }

    async def learn(self, mint_or_id: str, tx_sig: str) -> Dict[str, Any]:
        """
        Learn using a mint or pump id. If a pump id / invalid mint is provided,
        we will attempt a single best-effort resolution; if still unresolved,
        we switch to tx-only learning to avoid RPC mint errors.
        """
        original = (mint_or_id or "").strip()

        # One quick best-effort resolve pass if this doesn't look like a mint
        mint = original
        resolved_once: Optional[str] = None
        if (not _is_likely_pubkey(mint)) or mint.endswith("pump"):
            resolved_once = await resolve_mint_best_effort(self.rpc, original, signature=tx_sig, owner=self.owner)
            if resolved_once:
                if DEBUG:
                    print(f"[txlearn] resolved id -> mint {resolved_once} (from {original})", flush=True)
                mint = resolved_once

        if not _is_likely_pubkey(mint):
            print(f"[txlearn] unresolved id '{original}', switching to tx-only learn", flush=True)
            return await self._learn_tx_only(tx_sig)

        if not _is_likely_pubkey(self.owner):
            print(f"[txlearn] WARNING: owner '{self.owner}' does not look like a base58 pubkey", flush=True)

        # Validate mint account first to avoid 'could not find mint' spam.
        if not await _is_valid_mint_account(self.rpc, mint):
            print(f"[txlearn] '{mint}' is not a valid mint account yet → tx-only learn", flush=True)
            return await self._learn_tx_only(tx_sig)

        print(f"[txlearn] start mint={mint} from tx={tx_sig}", flush=True)

        # Let the cluster catch up a moment before the first probe
        if self.warmup > 0:
            await asyncio.sleep(self.warmup)

        tx_obj: Optional[dict] = None
        status: Optional[dict] = None
        last_balance_ui: float = 0.0
        noisy_every = 3  # throttle getTransaction 'no result' prints

        attempt = 0
        for attempt in range(1, self.tries + 1):
            # 1) Check signature status (cheap)
            status = await self.rpc.get_signature_status(tx_sig)

            # ---- Early-exit on explicit transaction error ----
            if status and status.get("err") is not None:
                return {
                    "tx_found": False,
                    "owner_balance_ui": 0.0,
                    "attempts": attempt,
                    "status": status,
                    "transaction": None,
                }

            # 2) Try to fetch the transaction (some RPCs are slow to index)
            tx_obj = await self.rpc.get_transaction(tx_sig, self.commitment)
            if not tx_obj and (DEBUG or attempt % noisy_every == 0):
                print(f"[txlearn] getTransaction returned no result (sig={tx_sig})", flush=True)

            # 3) Check owned balance for the mint
            try:
                last_balance_ui = await _sum_owned_ui_amount(self.rpc, self.owner, mint)
            except Exception:
                last_balance_ui = 0.0

            if last_balance_ui > 0:
                # Good! We own some tokens—exit early
                break

            if attempt < self.tries:
                # Optional extra backoff between cycles
                if POS_LEARN_BASE_DELAY_SEC > 0:
                    await asyncio.sleep(POS_LEARN_BASE_DELAY_SEC)
                await asyncio.sleep(_jitter(self.delay))

        return {
            "tx_found": tx_obj is not None,
            "owner_balance_ui": float(last_balance_ui),
            "attempts": attempt,
            "status": status,
            "transaction": tx_obj,
        }


# ------------------------ convenience top-level ------------------------

async def learn_position(
    mint: str,
    tx_sig: str,
    owner: Optional[str] = None,
    rpc_url: str = RPC_URL,
) -> Dict[str, Any]:
    """
    One-shot helper when you already know the real mint:
        from utils_pkg.txlearn import learn_position
        res = await learn_position(mint, sig, owner=PUBKEY)
    """
    learner = TxLearner(rpc_url=rpc_url, owner=owner)
    try:
        return await learner.learn(mint, tx_sig)
    finally:
        await learner.close()


async def resolve_then_learn_position(
    raw_id_or_mint: str,
    tx_sig: str,
    owner: Optional[str] = None,
    rpc_url: str = RPC_URL,
) -> Dict[str, Any]:
    """
    Preferred one-shot when you have a pump id or uncertain mint:
        from utils_pkg.txlearn import resolve_then_learn_position
        res = await resolve_then_learn_position(raw_id, sig, owner=PUBKEY)
    """
    learner = TxLearner(rpc_url=rpc_url, owner=owner)
    try:
        return await learner.resolve_then_learn(raw_id_or_mint, tx_sig)
    finally:
        await learner.close()
