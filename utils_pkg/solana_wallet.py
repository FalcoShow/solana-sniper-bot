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
# Small helpers / constants
# ──────────────────────────────────────────────────────────────────────────────
def _sanitize_url(u: Optional[str]) -> str:
    s = (u or "").strip()
    if s.startswith("${") and s.endswith("}"):  # treat literal placeholders as unset
        return ""
    return s

_UA = {
    "user-agent": "sniper.wallet/1.0",
    "accept": "application/json",
    "content-type": "application/json",
}

LAMPORTS_PER_SOL = 1_000_000_000

# RPC selection
_SEND_RPC_URL = (
    _sanitize_url(os.getenv("TX_RPC_URL"))
    or _sanitize_url(os.getenv("SOL_RPC_TX_URL"))
    or _sanitize_url(os.getenv("SOL_RPC_URL"))
    or _sanitize_url(os.getenv("SOLANA_RPC_URL"))
    or "https://api.mainnet-beta.solana.com"
)
RPC_URL = _SEND_RPC_URL  # public alias used by other modules

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
    Load a keypair from (first match wins):
      - WALLET_PRIVKEY_B58                  (base58 64-byte secret)
      - WALLET_PRIVKEY_JSON                 (JSON array of 64 ints)
      - WALLET_KEYPAIR_JSON / KEYPAIR_JSON / WALLET_JSON  (JSON array of 64 ints)
      - WALLET_PRIVATE_KEY / WALLET_KEYPAIR_PATH / KEYPAIR_PATH (file path)
      - default: ~/.config/solana/id.json
    """
    b58 = (os.getenv("WALLET_PRIVKEY_B58") or "").strip()
    if b58:
        sk = _b58decode(b58)
        if len(sk) != 64:
            raise RuntimeError("WALLET_PRIVKEY_B58 must be base58 of a 64-byte secret key")
        return Keypair.from_bytes(sk)

    for key in ("WALLET_PRIVKEY_JSON", "WALLET_KEYPAIR_JSON", "KEYPAIR_JSON", "WALLET_JSON"):
        js = (os.getenv(key) or "").strip()
        if js:
            arr = json.loads(js)
            sk_bytes = bytes(arr)
            if len(sk_bytes) != 64:
                raise RuntimeError(f"{key} must be a JSON array of 64 bytes")
            return Keypair.from_bytes(sk_bytes)

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
# Generic RPC POST (used below)
# ──────────────────────────────────────────────────────────────────────────────
def _rpc_post(method: str, params: list, *, url: Optional[str] = None) -> Dict[str, Any]:
    u = _sanitize_url(url) or _SEND_RPC_URL
    payload = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
    r = requests.post(u, json=payload, timeout=_RPC_TIMEOUT_SEC, headers=_UA)
    r.raise_for_status()
    return r.json()

# ──────────────────────────────────────────────────────────────────────────────
# Balance helpers (sync + async)
# ──────────────────────────────────────────────────────────────────────────────
def get_balance_lamports(owner: Optional[str] = None) -> int:
    try:
        if not owner:
            kp = load_keypair()
            owner = get_pubkey_str(kp)
        js = _rpc_post(
            "getBalance",
            [owner, {"commitment": _TX_COMMITMENT_RAW}],
            url=_READ_RPC_URL,
        )
        return int((js.get("result") or {}).get("value", 0) or 0)
    except Exception:
        return 0

async def get_balance_sol(owner: Optional[str] = None) -> float:
    def _call() -> float:
        lamports = get_balance_lamports(owner)
        return float(lamports) / LAMPORTS_PER_SOL
    try:
        return await asyncio.to_thread(_call)
    except Exception:
        return 0.0

# Back-compat alias some code expects
get_sol_balance = get_balance_sol

# ──────────────────────────────────────────────────────────────────────────────
# Legacy tx parsing helpers
# ──────────────────────────────────────────────────────────────────────────────
def _is_legacy_tx_bytes(raw: bytes) -> bool:
    return (raw[0] & 0x80) == 0  # v0 sets MSB=1 in first byte

def _read_shortvec(data: bytes, offset: int = 0) -> Tuple[int, int]:
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
    if isinstance(resp, str):
        return resp
    if hasattr(resp, "value"):
        try:
            return str(getattr(resp, "value"))
        except Exception:
            pass
    if isinstance(resp, dict):
        if isinstance(resp.get("result"), str):
            return str(resp["result"])
        r = resp.get("result")
        if isinstance(r, dict) and r.get("value"):
            return str(r["value"])
        if resp.get("value"):
            return str(resp["value"])
    return str(resp)

# ──────────────────────────────────────────────────────────────────────────────
# Send + confirm helpers
# ──────────────────────────────────────────────────────────────────────────────
async def sign_and_send_b64_tx(
    tx_b64: str,
    kp: Keypair,
    *,
    skip_preflight: bool = True,
) -> str:
    """
    Decode base64 swap transaction from Jupiter, sign (LEGACY ONLY), and submit.
    The router requests asLegacyTransaction=True, so versioned v0 bytes are rejected.
    Returns base58 signature (never fakes success).
    """
    raw = b64decode(tx_b64)

    if not _is_legacy_tx_bytes(raw):
        raise RuntimeError(
            "Received a versioned (v0) transaction but legacy is expected. "
            "Ensure Jupiter requests use asLegacyTransaction=True."
        )

    sig_count, pos = _read_shortvec(raw, 0)
    pos += 64 * sig_count
    msg_bytes = raw[pos:]

    sig_obj: Signature = kp.sign_message(msg_bytes)

    wire = _encode_shortvec(1) + bytes(sig_obj) + msg_bytes

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

    try:
        return await asyncio.to_thread(_send_sync)
    except Exception as e:
        # Try to simulate for clearer errors
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

async def _send_b64_with_diag(tx_b64: str, *, skip_preflight: bool = True) -> str:
    kp = load_keypair()
    return await sign_and_send_b64_tx(tx_b64, kp, skip_preflight=skip_preflight)

def _status_from_sig(sig: str) -> Dict[str, Any]:
    """
    Calls getSignatureStatuses. Tries with and without searchTransactionHistory
    for maximum node compatibility.
    """
    try:
        js = _rpc_post("getSignatureStatuses", [[sig], {"searchTransactionHistory": True}], url=_READ_RPC_URL)
    except Exception:
        js = _rpc_post("getSignatureStatuses", [[sig]], url=_READ_RPC_URL)
    res = (js.get("result") or {}).get("value") or []
    return res[0] if res else {}

def _is_confirmed_ok(status: Dict[str, Any], want: str) -> bool:
    # want in {"processed","confirmed","finalized"}
    if not status:
        return False
    if status.get("err"):
        return False
    conf = int(status.get("confirmations") or 0) if status.get("confirmations") is not None else None
    # Solana RPC may return confirmationStatus: "processed"/"confirmed"/"finalized"
    lvl = (status.get("confirmationStatus") or "").lower()
    if lvl in ("confirmed", "finalized"):
        # good for confirmed+
        return want in ("processed", "confirmed") or lvl == "finalized"
    if lvl == "processed":
        return want == "processed"
    # Fallback: if confirmations is None => finalized on some nodes
    if conf is None:
        return want in ("processed", "confirmed", "finalized")
    # If we only got a number, consider >=1 as processed; >=2 as confirmed-ish
    return (want == "processed" and conf >= 1) or (want != "processed" and conf >= 2)

async def confirm_signature(
    signature: str,
    *,
    commitment: str = "confirmed",
    timeout_s: float = 60.0,
    poll_interval_s: float = 0.8,
) -> bool:
    """
    Polls getSignatureStatuses until the tx is confirmed at the requested level
    or the timeout elapses. Returns True on success, False otherwise.
    """
    want = str(commitment).strip().lower()
    deadline = asyncio.get_event_loop().time() + float(timeout_s)
    while asyncio.get_event_loop().time() < deadline:
        try:
            st = _status_from_sig(signature)
            if st and st.get("err"):
                return False
            if _is_confirmed_ok(st, want):
                return True
        except Exception:
            pass
        await asyncio.sleep(poll_interval_s)
    return False

# ──────────────────────────────────────────────────────────────────────────────
# Public API
# ──────────────────────────────────────────────────────────────────────────────
__all__ = [
    "RPC_URL",
    "LAMPORTS_PER_SOL",
    "load_keypair",
    "get_pubkey",
    "get_pubkey_str",
    "get_balance_lamports",
    "get_balance_sol",
    "get_sol_balance",          # alias used elsewhere
    "sign_and_send_b64_tx",
    "_send_b64_with_diag",
    "confirm_signature",        # ★ required by pump_trader
]
