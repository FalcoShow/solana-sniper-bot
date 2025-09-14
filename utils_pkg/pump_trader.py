# utils_pkg/pump_trader.py

from __future__ import annotations
import os, json, base64, time, typing as t

import requests
from solders.keypair import Keypair
from solders.pubkey import Pubkey
from solders.transaction import VersionedTransaction
from solana.rpc.async_api import AsyncClient
from solana.rpc.types import TxOpts

PP_API_KEY = os.getenv("PF_PP_KEY", "")
RPC_URL    = os.getenv("SOL_RPC_URL", "https://api.mainnet-beta.solana.com")

DEFAULT_SLIPPAGE_BPS = int(float(os.getenv("PF_SLIPPAGE_BPS", "1000")))  # 10.00%
DEFAULT_PRIORITY_SOL = float(os.getenv("PF_PRIORITY_SOL", "0.0005"))     # 0.0005 SOL

# --- PumpPortal (simple & fast on fresh mints) --------------------------------

def build_buy_tx_via_portal(
    mint: str,
    payer_pubkey: str,
    *,
    amount_sol: float,
    slippage_bps: int = DEFAULT_SLIPPAGE_BPS,
    priority_sol: float = DEFAULT_PRIORITY_SOL,
) -> str:
    """
    Returns a base64 unsigned transaction that buys `amount_sol` of `mint`.
    """
    url = "https://pumpportal.fun/api/trade"
    headers = {"content-type": "application/json", "x-api-key": PP_API_KEY}
    body = {
        "apiKey": PP_API_KEY,         # some setups read header, some read body; we set both
        "publicKey": payer_pubkey,
        "action": "buy",
        "mint": mint,
        "amount": amount_sol,         # SOL in
        "slippage": slippage_bps,     # bps
        "priorityFee": priority_sol,  # SOL
        "pool": "pump",
    }
    r = requests.post(url, headers=headers, data=json.dumps(body), timeout=25)
    r.raise_for_status()
    js = r.json()
    tx_b64 = js.get("transaction") or js.get("tx")
    if not tx_b64:
        raise RuntimeError(f"PumpPortal buy: missing transaction in {js}")
    return tx_b64


def build_sell_tx_via_portal(
    mint: str,
    payer_pubkey: str,
    *,
    token_amount: float | None = None,
    sol_amount: float | None = None,
    slippage_bps: int = DEFAULT_SLIPPAGE_BPS,
    priority_sol: float = DEFAULT_PRIORITY_SOL,
) -> str:
    """
    Returns a base64 unsigned transaction that sells either a `token_amount` (tokens)
    or for a target `sol_amount` (SOL out). Provide one of them.
    """
    if (token_amount is None) == (sol_amount is None):
        raise ValueError("Provide exactly one of token_amount or sol_amount")

    url = "https://pumpportal.fun/api/trade"
    headers = {"content-type": "application/json", "x-api-key": PP_API_KEY}
    body = {
        "apiKey": PP_API_KEY,
        "publicKey": payer_pubkey,
        "action": "sell",
        "mint": mint,
        "slippage": slippage_bps,
        "priorityFee": priority_sol,
        "pool": "pump",
    }
    if token_amount is not None:
        body["tokenAmount"] = token_amount   # many builders accept this for sells
    else:
        body["amount"] = sol_amount          # or target SOL out

    r = requests.post(url, headers=headers, data=json.dumps(body), timeout=25)
    r.raise_for_status()
    js = r.json()
    tx_b64 = js.get("transaction") or js.get("tx")
    if not tx_b64:
        raise RuntimeError(f"PumpPortal sell: missing transaction in {js}")
    return tx_b64


# --- Jupiter v6 (best execution after real DEX liq forms) ---------------------

JUP_BASE = "https://quote-api.jup.ag"

def jup_quote(
    in_mint: str,
    out_mint: str,
    *,
    in_amount_atoms: int,
    slippage_bps: int = DEFAULT_SLIPPAGE_BPS,
    max_accounts: int = 64,
) -> dict:
    """
    Get a quote response dict from Jupiter v6.
    """
    url = f"{JUP_BASE}/v6/quote"
    params = {
        "inputMint": in_mint,
        "outputMint": out_mint,
        "amount": in_amount_atoms,
        "slippageBps": slippage_bps,
        "swapMode": "ExactIn",
        "maxAccounts": max_accounts,
        "onlyDirectRoutes": "false",
        "asLegacyTransaction": "false",
        "preferDirectRoutes": "true",
        "platformFeeBps": 0,
    }
    r = requests.get(url, params=params, timeout=15)
    r.raise_for_status()
    return r.json()


def jup_swap_tx(
    quote_response: dict,
    user_pubkey: str,
    *,
    wrap_and_unwrap_sol: bool = True,
) -> str:
    """
    Create Jupiter swap transaction (base64 unsigned) from a quoteResponse.
    """
    url = f"{JUP_BASE}/v6/swap"
    body = {
        "userPublicKey": user_pubkey,
        "wrapAndUnwrapSol": wrap_and_unwrap_sol,
        "useSharedAccounts": True,
        "quoteResponse": quote_response,
        "asLegacyTransaction": False,
        # You can add "dynamicComputeUnitLimit" / "dynamicSlippage" if desired.
    }
    r = requests.post(url, json=body, timeout=25)
    r.raise_for_status()
    js = r.json()
    tx_b64 = js.get("swapTransaction")
    if not tx_b64:
        raise RuntimeError(f"Jupiter swap: missing swapTransaction in {js}")
    return tx_b64


# --- Sign + send (solders only) ----------------------------------------------

def _sign_b64_with_keypair(tx_b64: str, kp: Keypair) -> bytes:
    """
    Decode an unsigned base64 VersionedTransaction, sign with `kp`, return raw bytes.
    """
    tx = VersionedTransaction.from_bytes(base64.b64decode(tx_b64))
    # Recreate a new VersionedTransaction with the same message and our signer.
    signed = VersionedTransaction(tx.message, [kp])
    return bytes(signed)


async def sign_and_send_b64_tx(
    tx_b64: str,
    kp: Keypair,
    *,
    rpc_url: str = RPC_URL,
    skip_preflight: bool = True,
    max_retries: int = 3,
) -> str:
    """
    Signs base64 v0 tx and sends it. Returns the signature (str).
    """
    raw = _sign_b64_with_keypair(tx_b64, kp)

    async with AsyncClient(rpc_url) as c:
        resp = await c.send_raw_transaction(
            raw,
            opts=TxOpts(skip_preflight=skip_preflight, max_retries=max_retries),
        )
        # solana-py returns RPCResponse; try to pull the value / signature gracefully
        sig = None
        try:
            sig = resp.value  # type: ignore[attr-defined]
        except Exception:
            try:
                sig = resp["result"]
            except Exception:
                pass
        if not sig:
            raise RuntimeError(f"send_raw_transaction failed: {resp}")
        return str(sig)
