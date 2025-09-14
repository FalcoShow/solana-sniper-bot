# utils_pkg/solana_wallet.py
import os, json, base64
from typing import Optional
from solders.pubkey import Pubkey

from base58 import b58decode
from solana.rpc.async_api import AsyncClient
from solana.rpc.types import TxOpts
from solders.keypair import Keypair
from solders.transaction import VersionedTransaction as VTx, Transaction as LTx  # v0 + legacy

RPC_URL = os.getenv("SOL_RPC_URL") or "https://api.mainnet-beta.solana.com"

def _load_keypair_from_env_or_file() -> Keypair:
    """
    Loads a keypair from either:
      - WALLET_PRIVKEY_B58  (base58-encoded 64-byte secret key)  [optional]
      - WALLET_KEYPAIR_PATH (Solana-CLI JSON array path)         [default]
    """
    b58 = (os.getenv("WALLET_PRIVKEY_B58") or "").strip()
    if b58:
        sk = b58decode(b58)
        if len(sk) != 64:
            raise RuntimeError("WALLET_PRIVKEY_B58 must be a base58-encoded 64-byte secret key")
        return Keypair.from_bytes(sk)

    path = (os.getenv("WALLET_KEYPAIR_PATH") or os.path.expanduser("~/.config/solana/id.json")).strip()
    with open(path, "r", encoding="utf-8") as f:
        arr = json.load(f)
    return Keypair.from_bytes(bytes(arr))

def load_keypair() -> Keypair:
    return _load_keypair_from_env_or_file()

async def get_client() -> AsyncClient:
    return AsyncClient(RPC_URL, timeout=30)

async def get_balance_sol(pub: str | Pubkey) -> float:
    pk = pub if isinstance(pub, Pubkey) else Pubkey.from_string(str(pub))
    async with await get_client() as c:
        r = await c.get_balance(pk)
        # solana 0.36 returns a dataclass with .value
        return r.value / 1e9

async def sign_and_send_b64_tx(b64_tx: str, kp: Keypair, *, skip_preflight: bool = True) -> str:
    """
    Accepts a base64-encoded (legacy or v0) transaction, signs with solders.Keypair, broadcasts, confirms.
    """
    raw = base64.b64decode(b64_tx)

    # Try v0 first, then legacy
    try:
        vtx = VTx.from_bytes(raw).sign([kp])
        tx_bytes = bytes(vtx)
    except Exception:
        ltx = LTx.from_bytes(raw).sign([kp])
        tx_bytes = bytes(ltx)

    async with await get_client() as c:
        resp = await c.send_raw_transaction(
            tx_bytes, opts=TxOpts(skip_preflight=skip_preflight, max_retries=3)
        )
        sig = resp.value if hasattr(resp, "value") else (resp.get("result") or resp.get("value"))
        if not isinstance(sig, str):
            raise RuntimeError(f"Unexpected send response: {resp}")
        await c.confirm_transaction(sig, commitment="confirmed")
        return sig
