# utils_pkg/txlearn.py
from __future__ import annotations

import asyncio
import os
from typing import Any, Dict, List, Optional, Tuple

import aiohttp


# ------------------------ env + defaults ------------------------

RPC_URL = os.getenv("SOL_RPC_URL", "").strip() or "https://api.mainnet-beta.solana.com"

TX_COMMITMENT = (os.getenv("PF_TXLEARN_COMMITMENT", "confirmed").strip().lower()
                 or "confirmed")  # "processed" | "confirmed" | "finalized"

TX_TRIES       = int(os.getenv("PF_TXLEARN_TRIES", "20"))
TX_DELAY_SEC   = float(os.getenv("PF_TXLEARN_DELAY_SEC", "1.2"))
TX_WARMUP_SEC  = float(os.getenv("PF_TXLEARN_WARMUP_SEC", "2.0"))

# Optional extra wait between learn cycles if your stack wants it.
POS_LEARN_BASE_DELAY_SEC = float(os.getenv("PF_POS_LEARN_BASE_DELAY_SEC", "0"))

# Optional noisy debug
DEBUG = str(os.getenv("PF_DEBUG_TX", "0")).strip().lower() in {"1","true","yes","y","on"}

# Simple base58 char whitelist (NOT a full validator—just fast sanity)
_BASE58 = set("123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz")


def _is_likely_pubkey(s: str) -> bool:
    s = (s or "").strip()
    if not (32 <= len(s) <= 64):
        return False
    return all(ch in _BASE58 for ch in s)


# ------------------------ HTTP/RPC helpers ------------------------

class _Rpc:
    def __init__(self, url: str) -> None:
        self.url = url
        self._session: Optional[aiohttp.ClientSession] = None
        self._req_id = 1

    async def _ensure(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            timeout = aiohttp.ClientTimeout(total=15, connect=5)
            self._session = aiohttp.ClientSession(timeout=timeout)
        return self._session

    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()

    async def call(self, method: str, params: list) -> Any:
        """
        Barebones JSON-RPC 2.0 call with very defensive parsing.
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
        # Try #1: jsonParsed + v0 support flag (best)
        for attempt in (1, 2, 3):
            try:
                if attempt == 1:
                    return await self.call(
                        "getTransaction",
                        [sig, {"commitment": commitment, "encoding": "jsonParsed",
                               "maxSupportedTransactionVersion": 0}],
                    )
                elif attempt == 2:
                    # Some nodes don't like maxSupportedTransactionVersion
                    return await self.call(
                        "getTransaction",
                        [sig, {"commitment": commitment, "encoding": "jsonParsed"}],
                    )
                else:
                    # Fallback to base64 if the node refuses parsed
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
    print(f"[txlearn] owner ATAs for {mint}: {len(accounts)} found", flush=True)
    total_ui = 0.0
    for acc in accounts:
        _, _, ui = _extract_ui_amount(acc)
        total_ui += ui
    return total_ui


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
        self.owner = owner or os.getenv("PF_PP_ACCOUNT", "").strip()
        self.commitment = commitment
        self.tries = max(1, int(tries))
        self.delay = max(0.1, float(delay_sec))
        self.warmup = max(0.0, float(warmup_sec))

    async def close(self) -> None:
        await self.rpc.close()

    async def learn(self, mint: str, tx_sig: str) -> Dict[str, Any]:
        """
        Returns a dict with what we learned:
          {
            "tx_found": bool,
            "owner_balance_ui": float,
            "attempts": int,
            "status": Optional[dict],   # from getSignatureStatuses (best-effort)
            "transaction": Optional[dict],  # from getTransaction (best-effort)
          }
        """
        if not _is_likely_pubkey(mint):
            print(f"[txlearn] WARNING: '{mint}' does not look like a base58 mint", flush=True)

        if not _is_likely_pubkey(self.owner):
            print(f"[txlearn] WARNING: owner '{self.owner}' does not look like a base58 pubkey", flush=True)

        print(f"[txlearn] start mint={mint} from tx={tx_sig}", flush=True)

        # Let the cluster catch up a moment before the first probe
        if self.warmup > 0:
            await asyncio.sleep(self.warmup)

        tx_obj: Optional[dict] = None
        status: Optional[dict] = None
        last_balance_ui: float = 0.0

        for attempt in range(1, self.tries + 1):
            # 1) Check signature status (cheap)
            status = await self.rpc.get_signature_status(tx_sig)

            # 2) Try to fetch the transaction (some RPCs are slow to index)
            tx_obj = await self.rpc.get_transaction(tx_sig, self.commitment)
            if not tx_obj:
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
                await asyncio.sleep(self.delay)

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
    One-shot helper you can import and call:
        from utils_pkg.txlearn import learn_position
        res = await learn_position(mint, sig, owner=PUBKEY)

    Returns the same dict as TxLearner.learn().
    """
    learner = TxLearner(rpc_url=rpc_url, owner=owner)
    try:
        return await learner.learn(mint, tx_sig)
    finally:
        await learner.close()
