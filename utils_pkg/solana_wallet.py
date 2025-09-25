# utils_pkg/solana_wallet.py
from __future__ import annotations

import os
import json
import asyncio
from base64 import b64decode
from typing import Optional

import requests

# Try to import base58; fall back to tiny local decoder
try:
    from base58 import b58decode as _b58decode  # type: ignore
except Exception:  # minimal base58 (no checksum)
    _B58_ALPHABET = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
    _B58_IDX = {c: i for i, c in enumerate(_B58_ALPHABET)}
    def _b58decode(s: str) -> bytes:
        s = (s or "").strip()
        if not s:
            return b""
        n = 0
        for ch in s:
            i = _B58_IDX.get(ch)
            if i is None:
                raise ValueError("invalid base58 char")
            n = n * 58 + i
        out = n.to_bytes((n.bit_length() + 7) // 8, "big") if n else b""
        pad = len(s) - len(s.lstrip("1"))
        return b"\x00" * pad + out

# solana-py client + solders keys
from solana.rpc.api import Client as RpcClient
from solana.rpc.types import TxOpts
from solders.keypair import Keypair
from solders.pubkey import Pubkey
from solders.signature import Signature

# =============================================================================
# RPC endpoints & knobs
# =============================================================================
# Prefer dedicated TX endpoint for submissions; fall back to read RPC.
_SEND_RPC_URL = (
    os.getenv("SOL_RPC_TX_URL")
    or os.getenv("SOL_RPC_URL")
    or "https://api.mainnet-beta.solana.com"
).strip()

# Public constant for compatibility with existing imports
RPC_URL = _SEND_RPC_URL

_RPC_TIMEOUT_SEC = float(os.getenv("ROUTER_RPC_TIMEOUT_SEC", "10") or 10.0)
_TX_COMMITMENT = (
    os.getenv("PF_TX_SEND_COMMITMENT")
    or os.getenv("PF_TXLEARN_COMMITMENT")
    or os.getenv("SOL_COMMITMENT")
    or "confirmed"
).strip()

_UA = {"user-agent": "pnlsniper/1.0 (+wallet)"}

# Cache the loaded keypair to avoid re-reading files/env repeatedly
_KP: Optional[Keypair] = None

# =============================================================================
# Keypair loading
# =============================================================================
def _load_keypair_from_env_or_file() -> Keypair:
    """
    Load a keypair from either:
      - WALLET_PRIVKEY_B58 : base58-encoded 64-byte secret key (secret||pub)
      - WALLET_PRIVKEY_JSON / WALLET_KEYPAIR_JSON : JSON array string of 64 bytes
      - WALLET_KEYPAIR_PATH / KEYPAIR_PATH : path to JSON array file (64 ints)
      - default ~/.config/solana/id.json
    """
    # 1) base58 secret (64 bytes)
    b58 = (os.getenv("WALLET_PRIVKEY_B58") or "").strip()
    if b58:
        sk = _b58decode(b58)
        if len(sk) != 64:
            raise RuntimeError("WALLET_PRIVKEY_B58 must be base58 of a 64-byte secret key")
        return Keypair.from_bytes(sk)

    # 2) inline JSON array in env
    for key in ("WALLET_PRIVKEY_JSON", "WALLET_KEYPAIR_JSON"):
        js = (os.getenv(key) or "").strip()
        if js:
            arr = json.loads(js)
            sk_bytes = bytes(arr)
            if len(sk_bytes) != 64:
                raise RuntimeError(f"{key} must be a JSON array of 64 bytes")
            return Keypair.from_bytes(sk_bytes)

    # 3) file path(s)
    path = (
        os.getenv("WALLET_KEYPAIR_PATH")
        or os.getenv("KEYPAIR_PATH")  # legacy env name in some setups
        or os.path.expanduser("~/.config/solana/id.json")
    ).strip()
    with open(path, "r", encoding="utf-8") as f:
        arr = json.load(f)
    sk_bytes = bytes(arr)
    if len(sk_bytes) != 64:
        raise RuntimeError(f"Keypair at {path} must contain a JSON array of 64 bytes")
    return Keypair.from_bytes(sk_bytes)

def load_keypair() -> Keypair:
    """Return the process trading keypair (cached)."""
    global _KP
    if _KP is None:
        _KP = _load_keypair_from_env_or_file()
    return _KP

# =============================================================================
# Pubkey helpers
# =============================================================================
def get_pubkey(kp: Keypair) -> Pubkey:
    """Return a Pubkey from a solders.Keypair."""
    return kp.pubkey()

def get_pubkey_str(kp: Keypair) -> str:
    """Base58 string form of the wallet pubkey."""
    return str(get_pubkey(kp))

# =============================================================================
# Balance helper used by entrypoint autosizer
# =============================================================================
def _rpc_url_for_balance() -> str:
    # Prefer read RPC (SOL_RPC_URL) for balances; fall back to TX RPC if needed.
    return (os.getenv("SOL_RPC_URL") or os.getenv("SOL_RPC_TX_URL") or RPC_URL).strip()

async def get_balance_sol(owner: Optional[str] = None) -> float:
    """
    Async helper: returns SOL balance for `owner` (or current wallet if None).
    Uses a lightweight JSON-RPC POST via requests in a worker thread.
    """
    try:
        if not owner:
            kp = load_keypair()
            owner = get_pubkey_str(kp)
        url = _rpc_url_for_balance()
        if not (url and owner):
            return 0.0

        def _call() -> float:
            payload = {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "getBalance",
                "params": [owner, {"commitment": _TX_COMMITMENT}],
            }
            r = requests.post(url, json=payload, timeout=_RPC_TIMEOUT_SEC, headers=_UA)
            r.raise_for_status()
            js = r.json()
            lamports = int((js.get("result") or {}).get("value", 0))
            return lamports / 1_000_000_000.0

        return await asyncio.to_thread(_call)
    except Exception:
        return 0.0

# =============================================================================
# Transaction signing & sending (LEGACY ONLY)
# =============================================================================
def _is_legacy_tx_bytes(raw: bytes) -> bool:
    """
    Legacy wire format begins with shortvec-encoded signature count.
    Versioned (v0) sets MSB=1 in first byte (versioned prefix).
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

    We expect legacy bytes because the router sets asLegacyTransaction=True.
    If a versioned (v0) tx is received, we fail fast with a clear message.

    Returns:
        str: transaction signature (base58)
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

    commitment = _TX_COMMITMENT
    max_retries_env = os.getenv("PF_TX_SEND_MAX_RETRIES", "")
    try:
        max_retries = int(max_retries_env) if max_retries_env else 3
    except Exception:
        max_retries = 3

    def _send_sync() -> str:
        client = RpcClient(_SEND_RPC_URL)
        resp = client.send_raw_transaction(
            wire,
            opts=TxOpts(
                skip_preflight=skip_preflight,
                preflight_commitment=commitment,
                max_retries=max_retries,
            ),
        )
        # solana-py can return various shapes; normalize to string
        if isinstance(resp, dict) and "result" in resp and resp["result"]:
            return str(resp["result"])
        return str(resp)

    return await asyncio.to_thread(_send_sync)

__all__ = [
    "RPC_URL",
    "load_keypair",
    "get_pubkey",
    "get_pubkey_str",
    "get_balance_sol",
    "sign_and_send_b64_tx",
]
