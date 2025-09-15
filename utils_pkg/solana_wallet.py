# utils_pkg/solana_wallet.py
import os
import json
from base64 import b64decode
from typing import Union

from base58 import b58decode
from solana.rpc.async_api import AsyncClient
from solana.rpc.api import Client as RpcClient
from solana.rpc.types import TxOpts

from solders.pubkey import Pubkey
from solders.keypair import Keypair
from solders.transaction import Transaction as LTx      # legacy transaction
from solders.message import Message
from solders.signature import Signature

# ---------------------------------------------------------------------------
# RPC endpoint
# ---------------------------------------------------------------------------
RPC_URL = os.getenv("SOL_RPC_URL") or "https://api.mainnet-beta.solana.com"


# ---------------------------------------------------------------------------
# Keypair loading
# ---------------------------------------------------------------------------
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
            raise RuntimeError(
                "WALLET_PRIVKEY_B58 must be a base58-encoded 64-byte secret key"
            )
        return Keypair.from_bytes(sk)

    path = (os.getenv("WALLET_KEYPAIR_PATH") or os.path.expanduser("~/.config/solana/id.json")).strip()
    with open(path, "r", encoding="utf-8") as f:
        arr = json.load(f)
    sk_bytes = bytes(arr)
    if len(sk_bytes) != 64:
        raise RuntimeError(f"Keypair at {path} must contain a 64-byte secret key array")
    return Keypair.from_bytes(sk_bytes)


def load_keypair() -> Keypair:
    """Public loader used by the rest of the app."""
    return _load_keypair_from_env_or_file()


# ---------------------------------------------------------------------------
# Lightweight async RPC helpers
# ---------------------------------------------------------------------------
async def get_client() -> AsyncClient:
    """Return an AsyncClient bound to the configured RPC URL."""
    return AsyncClient(RPC_URL, timeout=30)


async def get_balance_sol(pub: Union[str, Pubkey]) -> float:
    """Return SOL balance for a pubkey (in SOL, not lamports)."""
    pk = pub if isinstance(pub, Pubkey) else Pubkey.from_string(str(pub))
    async with AsyncClient(RPC_URL, timeout=30) as c:
        resp = await c.get_balance(pk)
        return resp.value / 1e9


# ---------------------------------------------------------------------------
# Transaction signing & sending (LEGACY ONLY)
# ---------------------------------------------------------------------------
def _is_legacy_tx_bytes(raw: bytes) -> bool:
    """
    Legacy wire format encodes the signature count (first byte) with MSB = 0.
    Versioned (v0) sets MSB = 1 and encodes the version in the lower 7 bits.
    """
    return (raw[0] & 0x80) == 0


async def sign_and_send_b64_tx(tx_b64: str, kp: Keypair, *, skip_preflight: bool = True) -> str:
    """
    Decode the base64 Jupiter tx, sign it (LEGACY only), and submit via RPC.

    We force legacy via asLegacyTransaction='true' on the quote **and** swap,
    and via JUP_AS_LEGACY=1 in the environment. If a versioned (v0) tx is
    received, we fail fast with a clear error.
    """
    raw = b64decode(tx_b64)

    # Fail fast if this is not legacy
    if not _is_legacy_tx_bytes(raw):
        raise RuntimeError(
            "Received a versioned (v0) transaction but legacy is expected. "
            "Ensure BOTH: _jup_quote(asLegacyTransaction='true') and "
            "_jup_swap(asLegacyTransaction=True)."
        )

    # Deserialize legacy tx, sign the message, rebuild tx with our signature
    tx_legacy: LTx = LTx.from_bytes(raw)           # unsigned legacy tx from Jupiter
    msg: Message = tx_legacy.message               # compiled message
    sig: Signature = kp.sign_message(msg.serialize())

    signed: LTx = LTx([sig], msg)                  # reconstruct signed tx
    wire: bytes = bytes(signed.to_bytes())

    client = RpcClient(RPC_URL)
    resp = client.send_raw_transaction(
        wire,
        opts=TxOpts(
            skip_preflight=skip_preflight,
            preflight_commitment="confirmed",
            max_retries=3,
        ),
    )

    # solana-py Client returns an RPCResponse dict
    if isinstance(resp, dict) and "result" in resp:
        return resp["result"]
    return str(resp)
