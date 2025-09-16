# utils_pkg/solana_wallet.py
import os
import json
from typing import Union
from base64 import b64decode

from base58 import b58decode

from solana.rpc.async_api import AsyncClient
from solana.rpc.api import Client as RpcClient
from solana.rpc.types import TxOpts

from solders.keypair import Keypair
from solders.pubkey import Pubkey
from solders.signature import Signature


# -----------------------------------------------------------------------------
# RPC endpoint
# -----------------------------------------------------------------------------
RPC_URL = os.getenv("SOL_RPC_URL") or "https://api.mainnet-beta.solana.com"


# -----------------------------------------------------------------------------
# Keypair loading
# -----------------------------------------------------------------------------
def _load_keypair_from_env_or_file() -> Keypair:
    """
    Load a keypair from either:
      - WALLET_PRIVKEY_B58  : base58-encoded 64-byte secret key (ed25519)
      - WALLET_KEYPAIR_PATH : Solana CLI JSON array path (default ~/.config/solana/id.json)
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
    sk_bytes = bytes(arr)
    if len(sk_bytes) != 64:
        raise RuntimeError(f"Keypair at {path} must contain a 64-byte secret key array")
    return Keypair.from_bytes(sk_bytes)


def load_keypair() -> Keypair:
    return _load_keypair_from_env_or_file()


# -----------------------------------------------------------------------------
# Async RPC helpers
# -----------------------------------------------------------------------------
async def get_client() -> AsyncClient:
    return AsyncClient(RPC_URL, timeout=30)


async def get_balance_sol(pub: Union[str, Pubkey]) -> float:
    pk = pub if isinstance(pub, Pubkey) else Pubkey.from_string(str(pub))
    async with AsyncClient(RPC_URL, timeout=30) as c:
        r = await c.get_balance(pk)
        return r.value / 1e9


# -----------------------------------------------------------------------------
# Transaction signing & sending (LEGACY ONLY)
# -----------------------------------------------------------------------------
def _is_legacy_tx_bytes(raw: bytes) -> bool:
    """
    Legacy wire format begins with the shortvec-encoded signature count.
    Versioned (v0) sets MSB=1 in the very first byte to indicate a versioned prefix.
    """
    return (raw[0] & 0x80) == 0


def _read_shortvec(data: bytes, offset: int = 0) -> tuple[int, int]:
    """Decode Solana shortvec at data[offset:]. Returns (value, next_index)."""
    value = 0
    shift = 0
    i = offset
    while True:
        b = data[i]
        i += 1
        value |= (b & 0x7F) << shift
        if (b & 0x80) == 0:
            break
        shift += 7
    return value, i


def _encode_shortvec(n: int) -> bytes:
    """Encode an integer to Solana shortvec bytes."""
    out = bytearray()
    while True:
        b = n & 0x7F
        n >>= 7
        if n:
            b |= 0x80
        out.append(b)
        if not n:
            break
    return bytes(out)


async def sign_and_send_b64_tx(tx_b64: str, kp: Keypair, *, skip_preflight: bool = True) -> str:
    """
    Decode the base64 Jupiter tx, sign it (LEGACY ONLY), and submit via RPC.

    We force legacy by sending asLegacyTransaction=true to BOTH /quote and /swap.
    If a versioned (v0) tx is received, we fail fast.
    """
    raw = b64decode(tx_b64)

    # Guard: we expect legacy bytes
    if not _is_legacy_tx_bytes(raw):
        raise RuntimeError(
            "Received a versioned (v0) transaction but legacy is expected. "
            "Ensure _jup_quote(asLegacyTransaction='true') and "
            "_jup_swap(asLegacyTransaction=True)."
        )

    # Parse the existing signature section (usually 0) and extract message bytes
    sig_count, pos = _read_shortvec(raw, 0)
    pos += 64 * sig_count  # skip any present signatures (Jupiter usually sends 0)
    msg_bytes = raw[pos:]

    # Sign the canonical message bytes
    sig: Signature = kp.sign_message(msg_bytes)

    # Rebuild a proper legacy wire: [shortvec(1)][signature][message_bytes]
    wire = _encode_shortvec(1) + bytes(sig) + msg_bytes

    # Send
    client = RpcClient(RPC_URL)
    resp = client.send_raw_transaction(
        wire,
        opts=TxOpts(skip_preflight=skip_preflight, preflight_commitment="confirmed", max_retries=3),
    )
    return resp["result"] if isinstance(resp, dict) else str(resp)
