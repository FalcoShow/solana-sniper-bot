# utils_pkg/solana_wallet.py
# Async Solana wallet utilities: client/keypair singletons, SOL transfers, SPL balances, tx helpers.

from __future__ import annotations

import os
import json
import time
import asyncio
import logging
from typing import Dict, List, Optional, Tuple, Any

from solana.rpc.async_api import AsyncClient
from solana.rpc.types import TxOpts, TokenAccountOpts
from solana.rpc.commitment import Confirmed, Processed
from solana.transaction import Transaction
from solana.system_program import TransferParams, transfer

# solders-native key + pubkey (solana-py >=0.28 uses solders under the hood)
from solders.keypair import Keypair
from solders.pubkey import Pubkey as PublicKey

logger = logging.getLogger("solana_wallet")
if not logger.handlers:
    logging.basicConfig(level=logging.INFO)

# ---- env/config ----
# Prefer RPC_URL; if not set, fall back to HELIUS_RPC_URL; then default to mainnet.
RPC_URL = os.getenv("RPC_URL") or os.getenv("HELIUS_RPC_URL") or "https://api.mainnet-beta.solana.com"

# Optional: commitment string ("processed" | "confirmed" | "finalized")
DEFAULT_COMMITMENT = (os.getenv("SOLANA_COMMITMENT") or "confirmed").strip().lower()

# Key material:
# - WALLET_PRIVATE_KEY: JSON array of 64 ints (secret key) or 32 ints (seed)
# - WALLET_KEYPAIR_PATH: path to a JSON file with that array
WALLET_PRIVATE_KEY = os.getenv("WALLET_PRIVATE_KEY")
WALLET_KEYPAIR_PATH = os.getenv("WALLET_KEYPAIR_PATH")

# ---- module singletons ----
_CLIENT: Optional[AsyncClient] = None
_CLIENT_LOCK = asyncio.Lock()
_KEYPAIR: Optional[Keypair] = None
_KEYPAIR_LOCK = asyncio.Lock()


# ---- helpers ----

def _commitment_obj():
    # Map to the commitment enum objects expected by some APIs
    if DEFAULT_COMMITMENT in ("processed", "recent"):
        return Processed
    return Confirmed


def sol_to_lamports(amount_sol: float) -> int:
    return int(round(amount_sol * 1_000_000_000))


def lamports_to_sol(lamports: int) -> float:
    return lamports / 1_000_000_000.0


def _load_keypair_from_bytes(arr: List[int]) -> Keypair:
    """
    Accepts a 64-byte secret key (ed25519 expanded) or a 32-byte seed.
    """
    b = bytes(arr)
    if len(b) == 64:
        # Full secret key
        return Keypair.from_bytes(b)
    if len(b) == 32:
        # Seed (solders accepts 32-byte seed)
        return Keypair.from_seed(b)
    raise ValueError("Key array must be 32 or 64 bytes")


# ---- keypair management ----

async def get_keypair() -> Keypair:
    """
    Lazy-load and cache the wallet Keypair. Prefers WALLET_PRIVATE_KEY (JSON array).
    Fallback: WALLET_KEYPAIR_PATH pointing to a keypair JSON file (array of 64 or 32 ints).
    """
    global _KEYPAIR
    async with _KEYPAIR_LOCK:
        if _KEYPAIR is not None:
            return _KEYPAIR

        # 1) JSON from env
        if WALLET_PRIVATE_KEY:
            try:
                arr = json.loads(WALLET_PRIVATE_KEY)
                if not isinstance(arr, list):
                    raise ValueError("WALLET_PRIVATE_KEY must be a JSON array")
                _KEYPAIR = _load_keypair_from_bytes(arr)
                logger.info("Loaded Keypair from WALLET_PRIVATE_KEY.")
                return _KEYPAIR
            except Exception as e:
                logger.error(f"Failed to parse WALLET_PRIVATE_KEY: {e}")

        # 2) File path
        if WALLET_KEYPAIR_PATH and os.path.exists(WALLET_KEYPAIR_PATH):
            try:
                with open(WALLET_KEYPAIR_PATH, "r", encoding="utf-8") as f:
                    arr = json.load(f)
                if not isinstance(arr, list):
                    raise ValueError("Keypair file must contain a JSON array")
                _KEYPAIR = _load_keypair_from_bytes(arr)
                logger.info(f"Loaded Keypair from {WALLET_KEYPAIR_PATH}.")
                return _KEYPAIR
            except Exception as e:
                logger.error(f"Failed to load keypair file {WALLET_KEYPAIR_PATH}: {e}")

        raise RuntimeError("No valid wallet keypair found. Set WALLET_PRIVATE_KEY or WALLET_KEYPAIR_PATH.")


def get_public_key_str() -> str:
    if _KEYPAIR is None:
        raise RuntimeError("Keypair not initialized yet. Call get_keypair() first.")
    return str(_KEYPAIR.pubkey())


# ---- client management ----

async def get_client() -> AsyncClient:
    """Lazy-init and cache the AsyncClient."""
    global _CLIENT
    async with _CLIENT_LOCK:
        if _CLIENT is not None:
            return _CLIENT
        _CLIENT = AsyncClient(RPC_URL, commitment=_commitment_obj())
        logger.info(f"AsyncClient connected to {RPC_URL} (commitment={DEFAULT_COMMITMENT})")
        return _CLIENT


async def close_client():
    global _CLIENT
    async with _CLIENT_LOCK:
        if _CLIENT is not None:
            await _CLIENT.close()
            _CLIENT = None
            logger.info("AsyncClient closed.")


# ---- basic RPC helpers ----

async def get_latest_blockhash() -> Tuple[str, int]:
    """
    Returns (blockhash, last_valid_block_height)
    """
    client = await get_client()
    resp = await client.get_latest_blockhash()
    if not resp or not getattr(resp, "value", None):
        raise RuntimeError(f"get_latest_blockhash failed: {resp}")
    return str(resp.value.blockhash), int(resp.value.last_valid_block_height)


async def confirm_tx(signature: str, timeout: float = 25.0) -> bool:
    """Poll signature status until confirmation or timeout."""
    client = await get_client()
    end = time.time() + timeout
    while time.time() < end:
        try:
            res = await client.get_signature_statuses([signature])
            st = res.value[0] if res and getattr(res, "value", None) else None
            if st and st.confirmation_status in ("confirmed", "finalized"):
                return True
        except Exception:
            pass
        await asyncio.sleep(0.5)
    return False


# ---- balances & accounts ----

async def get_balance_sol(pubkey: Optional[str] = None) -> float:
    client = await get_client()
    if pubkey is None:
        kp = await get_keypair()
        pubkey = str(kp.pubkey())
    res = await client.get_balance(PublicKey.from_string(pubkey))
    val = getattr(res, "value", None)
    return lamports_to_sol(int(val)) if val is not None else 0.0


async def _extract_mint_from_parsed_account(parsed_ai: Any) -> Optional[str]:
    """
    Works with jsonParsed account format. Returns mint string or None.
    """
    try:
        return parsed_ai.value.data.parsed["info"]["mint"]
    except Exception:
        try:
            # Sometimes response is plain dict
            return parsed_ai["value"]["data"]["parsed"]["info"]["mint"]
        except Exception:
            return None


async def get_token_accounts_by_owner(owner_pubkey: Optional[str] = None) -> List[Dict]:
    """
    Returns a simplified list: [{mint, token_account, raw_amount, ui_amount, decimals}]
    Uses jsonParsed encoding for robust mint/amount extraction.
    """
    client = await get_client()
    if owner_pubkey is None:
        kp = await get_keypair()
        owner_pubkey = str(kp.pubkey())

    try:
        resp = await client.get_token_accounts_by_owner(
            PublicKey.from_string(owner_pubkey),
            TokenAccountOpts(program_id="TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA", encoding="jsonParsed"),
            commitment=DEFAULT_COMMITMENT,
        )
        out: List[Dict] = []
        if not resp or not getattr(resp, "value", None):
            return out

        # solana-py returns a typed container; also support raw dicts when present
        items = resp.value if isinstance(resp.value, list) else []
        for it in items:
            try:
                # Typed access
                ta_pubkey = str(it.pubkey)
                parsed = await client.get_parsed_account_info(it.pubkey)
                mint = await _extract_mint_from_parsed_account(parsed)
                bal = await client.get_token_account_balance(it.pubkey)
                decimals = int(bal.value.decimals)
                raw = int(bal.value.amount)
                ui = float(bal.value.ui_amount) if bal.value.ui_amount is not None else raw / (10 ** decimals)
            except Exception:
                # Fallback best-effort for dict-shaped responses
                d = it if isinstance(it, dict) else {}
                ta_pubkey = d.get("pubkey") or d.get("account", {}).get("owner")
                _acc = d.get("account", {})
                mint = None
                try:
                    mint = _acc["data"]["parsed"]["info"]["mint"]
                except Exception:
                    pass
                bal = await client.get_token_account_balance(PublicKey.from_string(ta_pubkey))
                decimals = int(getattr(bal.value, "decimals", 9) or 9)
                raw = int(getattr(bal.value, "amount", 0) or 0)
                ui = float(bal.value.ui_amount) if getattr(bal.value, "ui_amount", None) is not None else raw / (10 ** decimals)

            if mint:
                out.append({
                    "token_account": ta_pubkey,
                    "mint": mint,
                    "raw_amount": raw,
                    "ui_amount": ui,
                    "decimals": decimals,
                })
        return out
    except Exception as e:
        logger.error(f"get_token_accounts_by_owner error: {e}")
        return []


async def get_spl_balance(owner_pubkey: Optional[str], mint_address: str) -> float:
    accounts = await get_token_accounts_by_owner(owner_pubkey)
    for a in accounts:
        if a["mint"] == mint_address:
            return float(a["ui_amount"])
    return 0.0


# ---- transactions ----

async def build_and_send_tx(
    instructions: List,  # list[Instruction]
    signers: Optional[List[Keypair]] = None,
    *,
    skip_preflight: bool = True,
    max_retries: int = 3,
    timeout: float = 25.0,
) -> Tuple[bool, str]:
    """
    Build, sign, send, and confirm a legacy Transaction with given instructions.
    Returns (ok, signature_or_error).
    """
    client = await get_client()
    kp = await get_keypair()
    signers = signers or [kp]

    try:
        blockhash, _ = await get_latest_blockhash()
        tx = Transaction()
        for ix in instructions:
            tx.add(ix)
        tx.recent_blockhash = blockhash
        tx.fee_payer = kp.pubkey()

        tx.sign(*signers)

        # send
        opts = TxOpts(skip_preflight=skip_preflight, max_retries=max_retries)
        resp = await client.send_raw_transaction(tx.serialize(), opts=opts)
        sig = resp.value
        ok = await confirm_tx(sig, timeout=timeout)
        if not ok:
            logger.warning(f"Tx {sig} not confirmed within timeout")
        return ok, sig
    except Exception as e:
        logger.error(f"build_and_send_tx error: {e}")
        return False, str(e)


async def send_sol(to_pubkey: str, amount_sol: float) -> Tuple[bool, str]:
    """
    Simple SOL transfer using SystemProgram.
    """
    kp = await get_keypair()
    lamports = sol_to_lamports(amount_sol)
    ix = transfer(
        TransferParams(
            from_pubkey=kp.pubkey(),
            to_pubkey=PublicKey.from_string(to_pubkey),
            lamports=lamports,
        )
    )
    return await build_and_send_tx([ix])


# ---- dev helpers ----

async def airdrop_devnet(amount_sol: float = 1.0) -> Tuple[bool, str]:
    """
    Devnet only. Will fail on mainnet RPCs. Handy for local tests.
    """
    client = await get_client()
    kp = await get_keypair()
    try:
        res = await client.request_airdrop(kp.pubkey(), sol_to_lamports(amount_sol))
        sig = res.value
        ok = await confirm_tx(sig, timeout=30.0)
        return ok, sig
    except Exception as e:
        return False, str(e)
