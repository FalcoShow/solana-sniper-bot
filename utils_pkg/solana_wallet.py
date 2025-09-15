# utils_pkg/solana_wallet.py
import os, json, base64
from typing import Optional
from solders.pubkey import Pubkey

from base58 import b58decode
from solana.rpc.async_api import AsyncClient
from solana.rpc.types import TxOpts
from solders.keypair import Keypair
from solders.transaction import VersionedTransaction as VTx, Transaction as LTx  # v0 + legacy
from base64 import b64decode
from solana.rpc.api import Client as RpcClient
from solana.transaction import VersionedTransaction as PVersionedTx, Transaction as PLegacyTx

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

async def sign_and_send_b64_tx(tx_b64: str, kp, *, skip_preflight: bool = True) -> str:
    """
    Decode the base64 Jupiter tx, sign it (v0 or legacy), and send via RPC.
    Works with solana==0.36.x and solders==0.26.x.
    """
    raw = b64decode(tx_b64)
    client = RpcClient(os.getenv("SOL_RPC_URL"))

    # Try v0 first; if it doesn't parse, fall back to legacy
    wire = None
    try:
        vtx = PVersionedTx.deserialize(raw)   # v0 tx
        vtx.sign([kp])                        # accepts solders.Keypair
        wire = bytes(vtx.serialize())
    except Exception:
        ltx = PLegacyTx.deserialize(raw)      # legacy tx
        ltx.sign(kp)
        wire = bytes(ltx.serialize())

    resp = client.send_raw_transaction(
        wire,
        opts={
            "skip_preflight": skip_preflight,
            "preflight_commitment": "confirmed",
            "max_retries": 3,
        },
    )
    # solana-py returns a dict with "result"
    sig = resp["result"] if isinstance(resp, dict) else str(resp)
    return sig