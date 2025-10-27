# utils_pkg/solana_wallet.py
from __future__ import annotations

import os
import json
import asyncio
from base64 import b64decode
from typing import Optional, Tuple, Any, Dict

import requests

# ──────────────────────────────────────────────────────────────────────────────
# Optional base58 (local fallback)
# ──────────────────────────────────────────────────────────────────────────────
try:
    from base58 import b58decode as _b58decode  # type: ignore
except Exception:
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

# ──────────────────────────────────────────────────────────────────────────────
# solana-py / solders
# ──────────────────────────────────────────────────────────────────────────────
from solana.rpc.api import Client as RpcClient
from solana.rpc.types import TxOpts
from solana.rpc.commitment import Commitment, Confirmed, Finalized, Processed
from solders.keypair import Keypair
from solders.signature import Signature
from solders.pubkey import Pubkey

# ──────────────────────────────────────────────────────────────────────────────
# Small helpers
# ──────────────────────────────────────────────────────────────────────────────
def _sanitize_url(u: Optional[str]) -> str:
    """
    Treat literal placeholders like ${RPC_URL} as unset.
    Return a trimmed string or empty string.
    """
    s = (u or "").strip()
    if s.startswith("${") and s.endswith("}"):
        return ""
    return s

_UA = {
    "user-agent": "pnlsniper/1.0 (+wallet)",
    "accept": "application/json",
    "content-type": "application/json",
}

# ──────────────────────────────────────────────────────────────────────────────
# RPC endpoints & knobs
# ──────────────────────────────────────────────────────────────────────────────
# Primary TX RPC (prefer explicit TX var; fall back to generic; avoid ${...})
_SEND_RPC_URL = (
    _sanitize_url(os.getenv("TX_RPC_URL"))
    or _sanitize_url(os.getenv("SOL_RPC_TX_URL"))
    or _sanitize_url(os.getenv("SOL_RPC_URL"))
    or _sanitize_url(os.getenv("SOLANA_RPC_URL"))
    or "https://api.mainnet-beta.solana.com"
)

# Public alias used elsewhere
RPC_URL = _SEND_RPC_URL

# Read RPC for balance (prefer read endpoint, fall back to TX)
_READ_RPC_URL = (
    _sanitize_url(os.getenv("SOL_RPC_URL"))
    or _sanitize_url(os.getenv("SOLANA_RPC_URL"))
    or _SEND_RPC_URL
)

_RPC_TIMEOUT_SEC = float(os.getenv("ROUTER_RPC_TIMEOUT_SEC", "10") or 10.0)

_TX_COMMITMENT_RAW = (
    os.getenv("PF_TX_SEND_COMMITMENT")
    or os.getenv("PF_TXLEARN_COMMITMENT")
    or os.getenv("SOL_COMMITMENT")
    or "confirmed"
).strip()

# cached keypair
_KP: Optional[Keypair] = None

# ──────────────────────────────────────────────────────────────────────────────
# Commitment normalization
# ──────────────────────────────────────────────────────────────────────────────
def _norm_commitment(c: object) -> Commitment:
    """
    Accept a Commitment instance or string ('processed'|'confirmed'|'finalized')
    and return a proper Commitment enum instance. Defaults to Confirmed.
    """
    if hasattr(c, "value"):
        return c  # type: ignore[return-value]
    s = str(c or "").strip().lower()
    mapping = {"processed": Processed, "confirmed": Confirmed, "finalized": Finalized, "finalised": Finalized}
    return mapping.get(s, Confirmed)

# ──────────────────────────────────────────────────────────────────────────────
# Keypair loading (many sources)
# ──────────────────────────────────────────────────────────────────────────────
def _load_keypair_from_env_or_file() -> Keypair:
    """
    Load a keypair from:
      - WALLET_PRIVKEY_B58                       : base58-encoded 64-byte secret (secret||pub)
      - WALLET_PRIVKEY_JSON                      : JSON array of 64 ints
      - WALLET_KEYPAIR_JSON / KEYPAIR_JSON / WALLET_JSON : JSON array of 64 ints
      - WALLET_PRIVATE_KEY / WALLET_KEYPAIR_PATH / KEYPAIR_PATH : path to JSON array (64 ints)
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
    for key in ("WALLET_PRIVKEY_JSON", "WALLET_KEYPAIR_JSON", "KEYPAIR_JSON", "WALLET_JSON"):
        js = (os.getenv(key) or "").strip()
        if js:
            arr = json.loads(js)
            sk_bytes = bytes(arr)
            if len(sk_bytes) != 64:
                raise RuntimeError(f"{key} must be a JSON array of 64 bytes")
            return Keypair.from_bytes(sk_bytes)

    # 3) file path(s)
    path = (
        os.getenv("WALLET_PRIVATE_KEY")
        or os.getenv("WALLET_KEYPAIR_PATH")
        or os.getenv("KEYPAIR_PATH")
        or os.path.expanduser("~/.config/solana/id.json")
    ).strip()
    with open(path, "r", encoding="utf-8") as f:
        arr = json.load(f)
    sk_bytes = bytes(arr)
    if len(sk_bytes) != 64:
        raise RuntimeError(f"Keypair at {path} must contain a JSON array of 64 bytes")
    return Keypair.from_bytes(sk_bytes)

def load_keypair() -> Keypair:
    """Return the cached trading keypair (lazy-loaded)."""
    global _KP
    if _KP is None:
        _KP = _load_keypair_from_env_or_file()
    return _KP

# ──────────────────────────────────────────────────────────────────────────────
# Pubkey helpers
# ──────────────────────────────────────────────────────────────────────────────
def get_pubkey(kp: Keypair) -> Pubkey:
    return kp.pubkey()

def get_pubkey_str(kp: Keypair) -> str:
    return str(kp.pubkey())

# ──────────────────────────────────────────────────────────────────────────────
# Balance helper (used by autosizer)
# ──────────────────────────────────────────────────────────────────────────────
async def get_balance_sol(owner: Optional[str] = None) -> float:
    """
    Async helper: returns SOL balance for `owner` (or current wallet if None).
    Uses a lightweight JSON-RPC POST via requests in a worker thread.
    """
    try:
        if not owner:
            kp = load_keypair()
            owner = get_pubkey_str(kp)
        url = _READ_RPC_URL
        if not (url and owner):
            return 0.0

        def _call() -> float:
            payload = {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "getBalance",
                "params": [owner, {"commitment": _TX_COMMITMENT_RAW}],
            }
            r = requests.post(url, json=payload, timeout=_RPC_TIMEOUT_SEC, headers=_UA)
            r.raise_for_status()
            js = r.json()
            lamports = int((js.get("result") or {}).get("value", 0))
            return lamports / 1_000_000_000.0

        return await asyncio.to_thread(_call)
    except Exception:
        return 0.0

# ──────────────────────────────────────────────────────────────────────────────
# Legacy tx helpers
# ──────────────────────────────────────────────────────────────────────────────
def _is_legacy_tx_bytes(raw: bytes) -> bool:
    """
    Legacy wire format begins with shortvec-encoded signature count.
    Versioned (v0) sets MSB=1 in first byte (versioned prefix).
    """
    return (raw[0] & 0x80) == 0

def _read_shortvec(data: bytes, offset: int = 0) -> Tuple[int, int]:
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

def _unwrap_send_resp(resp: Any) -> str:
    """
    Normalize solana-py's send_raw_transaction response to a base58 signature string.
    Handles: plain str, dicts, and SendTransactionResp / RPCResponse objects with .value.
    """
    # 1) Already a string
    if isinstance(resp, str):
        return resp
    # 2) Newer solana-py typed responses (SendTransactionResp-like)
    if hasattr(resp, "value"):
        try:
            return str(getattr(resp, "value"))
        except Exception:
            pass
    # 3) Dict-ish RPC responses
    if isinstance(resp, dict):
        if isinstance(resp.get("result"), str):
            return str(resp["result"])
        r = resp.get("result")
        if isinstance(r, dict) and r.get("value"):
            return str(r["value"])
        if resp.get("value"):
            return str(resp["value"])
    # Fallback
    return str(resp)

# ──────────────────────────────────────────────────────────────────────────────
# Low-level RPC helper (used by diagnostics)
# ──────────────────────────────────────────────────────────────────────────────
def _rpc_post(method: str, params: list, *, url: Optional[str] = None) -> Dict[str, Any]:
    u = _sanitize_url(url) or _SEND_RPC_URL
    payload = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
    r = requests.post(u, json=payload, timeout=_RPC_TIMEOUT_SEC, headers=_UA)
    r.raise_for_status()
    return r.json()

async def _send_b64_with_diag(tx_b64: str, *, skip_preflight: bool = True) -> str:
    """
    Send a base64 legacy tx; on failure, run simulateTransaction (encoding=base64, sigVerify=True)
    and raise a detailed RuntimeError. Useful for step-by-step diagnostics.
    """
    kp = load_keypair()
    sig = await sign_and_send_b64_tx(tx_b64, kp, skip_preflight=skip_preflight)
    return sig

# ──────────────────────────────────────────────────────────────────────────────
# Signing & sending Jupiter swap transactions (LEGACY ONLY)
# ──────────────────────────────────────────────────────────────────────────────
async def sign_and_send_b64_tx(
    tx_b64: str,
    kp: Keypair,
    *,
    skip_preflight: bool = True,
) -> str:
    """
    Decode base64 swap transaction from Jupiter, sign (LEGACY ONLY), and submit.

    The router requests `asLegacyTransaction=True`, so we expect legacy bytes.
    If a versioned (v0) tx is ever received, fail fast with a clear message.

    Returns:
        str: transaction signature (base58 string)

    Raises:
        Exception on any send failure (NO fake signatures are ever returned).
    """
    raw = b64decode(tx_b64)

    # Guard: must be legacy
    if not _is_legacy_tx_bytes(raw):
        raise RuntimeError(
            "Received a versioned (v0) transaction but legacy is expected. "
            "Ensure Jupiter requests use asLegacyTransaction=True."
        )

    # Parse existing signatures + extract message bytes
    sig_count, pos = _read_shortvec(raw, 0)
    pos += 64 * sig_count  # skip any present (often 0 or a placeholder)
    msg_bytes = raw[pos:]

    # Sign canonical message bytes
    sig_obj: Signature = kp.sign_message(msg_bytes)

    # Rebuild wire: [shortvec(1)][signature][message_bytes]
    wire = _encode_shortvec(1) + bytes(sig_obj) + msg_bytes

    # Tx options
    commitment = _norm_commitment(_TX_COMMITMENT_RAW)
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
                skip_preflight=bool(skip_preflight),
                preflight_commitment=commitment,
                max_retries=max_retries,
            ),
        )
        return _unwrap_send_resp(resp)

    # Try send; if it fails, run simulateTransaction for a clear error and re-raise
    try:
        return await asyncio.to_thread(_send_sync)
    except Exception as e:
        # Simulate with encoding=base64, sigVerify=True (NO replaceRecentBlockhash)
        try:
            sim = _rpc_post(
                "simulateTransaction",
                [tx_b64, {"encoding": "base64", "sigVerify": True, "commitment": _TX_COMMITMENT_RAW}],
            )
            res = sim.get("result") or {}
            err = res.get("err")
            logs = res.get("logs")
            post = res.get("postBalances")
            msg = f"send_raw_transaction failed: {e!r}; simulate err={err}; logs={logs}; postBalances={post}"
            raise RuntimeError(msg)
        except Exception as e2:
            raise RuntimeError(f"send_raw_transaction failed: {e!r}; simulate secondary error: {e2!r}") from e

# ──────────────────────────────────────────────────────────────────────────────
# Public API
# ──────────────────────────────────────────────────────────────────────────────
__all__ = [
    "RPC_URL",
    "load_keypair",
    "get_pubkey",
    "get_pubkey_str",
    "get_balance_sol",
    "sign_and_send_b64_tx",
    "_send_b64_with_diag",  # handy for tests/diagnostics
]
