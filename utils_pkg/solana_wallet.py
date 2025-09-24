# utils_pkg/solana_wallet.py
from __future__ import annotations

import os
import json
from base64 import b64decode
from typing import Union, Optional

from base58 import b58decode

# solana-py (sync client) + solders key types
from solana.rpc.api import Client as RpcClient
from solana.rpc.types import TxOpts
from solders.keypair import Keypair
from solders.pubkey import Pubkey
from solders.signature import Signature

import asyncio


# ──────────────────────────────────────────────────────────────────────────────
# RPC endpoint
# ──────────────────────────────────────────────────────────────────────────────
RPC_URL = (os.getenv("SOL_RPC_URL") or "https://api.mainnet-beta.solana.com").strip()


# ──────────────────────────────────────────────────────────────────────────────
# Keypair loading
# ──────────────────────────────────────────────────────────────────────────────

def _load_keypair_from_env_or_file() -> Keypair:
    """
    Load a keypair from either:
      - WALLET_PRIVKEY_B58 : base58-encoded 64-byte secret key (ed25519)
      - WALLET_PRIVKEY_JSON / WALLET_KEYPAIR_JSON : JSON array string of 64 bytes
      - WALLET_KEYPAIR_PATH : path to Solana CLI keypair JSON (default ~/.config/solana/id.json)
    """
    # 1) base58 64-byte secret
    b58 = (os.getenv("WALLET_PRIVKEY_B58") or "").strip()
    if b58:
        sk = b58decode(b58)
        if len(sk) != 64:
            raise RuntimeError("WALLET_PRIVKEY_B58 must be a base58-encoded 64-byte secret key")
        return Keypair.from_bytes(sk)

    # 2) JSON array as env text
    for key in ("WALLET_PRIVKEY_JSON", "WALLET_KEYPAIR_JSON"):
        js = (os.getenv(key) or "").strip()
        if js:
            arr = json.loads(js)
            sk_bytes = bytes(arr)
            if len(sk_bytes) != 64:
                raise RuntimeError(f"{key} must be a JSON array of 64 bytes")
            return Keypair.from_bytes(sk_bytes)

    # 3) file path (Solana CLI format)
    path = (os.getenv("WALLET_KEYPAIR_PATH") or os.path.expanduser("~/.config/solana/id.json")).strip()
    with open(path, "r", encoding="utf-8") as f:
        arr = json.load(f)
    sk_bytes = bytes(arr)
    if len(sk_bytes) != 64:
        raise RuntimeError(f"Keypair at {path} must contain a 64-byte secret key array")
    return Keypair.from_bytes(sk_bytes)


def load_keypair() -> Keypair:
    """Return the process trading keypair."""
    return _load_keypair_from_env_or_file()


# ──────────────────────────────────────────────────────────────────────────────
# Pubkey helpers
# ──────────────────────────────────────────────────────────────────────────────

def get_pubkey(kp: Keypair) -> Pubkey:
    """Return a Pubkey from a solders.Keypair."""
    return kp.pubkey()


def get_pubkey_str(kp: Keypair) -> str:
    """Base58 string form of the wallet pubkey."""
    return str(get_pubkey(kp))


# ──────────────────────────────────────────────────────────────────────────────
# Transaction signing & sending (LEGACY ONLY)
# ──────────────────────────────────────────────────────────────────────────────

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


async def sign_and_send_b64_tx(
    tx_b64: str,
    kp: Keypair,
    *,
    skip_preflight: bool = True,
) -> str:
    """
    Decode the base64 Jupiter tx, sign it (LEGACY ONLY), and submit via RPC.

    We force legacy by sending asLegacyTransaction=true to BOTH /quote and /swap
    in the router. If a versioned (v0) tx is received, we fail fast.

    Returns: transaction signature (str)
    """
    raw = b64decode(tx_b64)

    # Guard: we expect legacy bytes
    if not _is_legacy_tx_bytes(raw):
        raise RuntimeError(
            "Received a versioned (v0) transaction but legacy is expected. "
            "Ensure Jupiter requests use asLegacyTransaction=True."
        )

    # Parse existing signatures and extract the message bytes
    sig_count, pos = _read_shortvec(raw, 0)
    pos += 64 * sig_count  # skip any present signatures (Jupiter typically sends 0)
    msg_bytes = raw[pos:]

    # Sign canonical message bytes
    sig: Signature = kp.sign_message(msg_bytes)

    # Rebuild proper legacy wire: [shortvec(1)][signature][message_bytes]
    wire = _encode_shortvec(1) + bytes(sig) + msg_bytes

    # Send via solana-py Client in a worker thread to avoid blocking the event loop
    commitment = (os.getenv("PF_TX_SEND_COMMITMENT") or "confirmed").strip()
    max_retries_env = os.getenv("PF_TX_SEND_MAX_RETRIES")
    max_retries = int(max_retries_env) if (max_retries_env or "").isdigit() else 3

    def _send_sync() -> str:
        client = RpcClient(RPC_URL)
        resp = client.send_raw_transaction(
            wire,
            opts=TxOpts(
                skip_preflight=skip_preflight,
                preflight_commitment=commitment,
                max_retries=max_retries,
            ),
        )
        # solana-py may return a str or a dict depending on version
        return resp["result"] if isinstance(resp, dict) else str(resp)

    return await asyncio.to_thread(_send_sync)


__all__ = [
    "RPC_URL",
    "load_keypair",
    "get_pubkey",
    "get_pubkey_str",
    "sign_and_send_b64_tx",
]
