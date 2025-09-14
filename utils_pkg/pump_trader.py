# utils_pkg/pump_trader.py
from __future__ import annotations
import os, json, base64, typing as t
import requests

from solders.keypair import Keypair
from solders.pubkey import Pubkey
from solders.transaction import VersionedTransaction
from solana.rpc.async_api import AsyncClient
from solana.rpc.types import TxOpts

# ---------------------- ENV / defaults ----------------------
PP_API_KEY = os.getenv("PF_PP_KEY", "")
RPC_URL    = os.getenv("SOL_RPC_URL", "https://api.mainnet-beta.solana.com")

# Slippage:
# - PF_SLIPPAGE_BPS for PumpPortal (integer bps)
# - JUPITER_MAX_SLIPPAGE for Jupiter (decimal; e.g. 0.0125 -> 125 bps)
def _env_pp_slippage_bps() -> int:
    return int(float(os.getenv("PF_SLIPPAGE_BPS", "1000")))  # 10.00%

def _env_jup_slippage_bps() -> int:
    dec = float(os.getenv("JUPITER_MAX_SLIPPAGE", "0.01"))  # 1.00% default
    return max(1, int(round(dec * 10_000)))

DEFAULT_PP_SLIPPAGE_BPS = _env_pp_slippage_bps()
DEFAULT_JUP_SLIPPAGE_BPS = _env_jup_slippage_bps()
DEFAULT_PRIORITY_SOL = float(os.getenv("PF_PRIORITY_SOL", "0.0005"))
DRY_RUN = os.getenv("DRY_RUN", "1").strip() not in ("0", "false", "False", "")

SOL_MINT = "So11111111111111111111111111111111111111112"

# =================== PumpPortal trade builders ===================

def build_buy_tx_via_portal(
    mint: str,
    payer_pubkey: str,
    *,
    amount_sol: float,
    slippage_bps: int = DEFAULT_PP_SLIPPAGE_BPS,
    priority_sol: float = DEFAULT_PRIORITY_SOL,
) -> str:
    """Return unsigned base64 v0 tx to buy `amount_sol` of `mint` via PumpPortal."""
    url = "https://pumpportal.fun/api/trade"
    headers = {"content-type": "application/json", "x-api-key": PP_API_KEY}
    body = {
        "apiKey": PP_API_KEY,
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
    token_amount: float | None = None,   # amount of tokens (ui amount)
    sol_amount: float | None = None,     # target SOL out
    slippage_bps: int = DEFAULT_PP_SLIPPAGE_BPS,
    priority_sol: float = DEFAULT_PRIORITY_SOL,
) -> str:
    """Return unsigned base64 v0 tx to sell via PumpPortal."""
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
        body["tokenAmount"] = token_amount
    else:
        body["amount"] = sol_amount

    r = requests.post(url, headers=headers, data=json.dumps(body), timeout=25)
    r.raise_for_status()
    js = r.json()
    tx_b64 = js.get("transaction") or js.get("tx")
    if not tx_b64:
        raise RuntimeError(f"PumpPortal sell: missing transaction in {js}")
    return tx_b64

# =================== Jupiter v6 quote + swap ===================

JUP_BASE = "https://quote-api.jup.ag"

def jup_quote(
    in_mint: str,
    out_mint: str,
    *,
    in_amount_atoms: int,
    slippage_bps: int = DEFAULT_JUP_SLIPPAGE_BPS,
    max_accounts: int = 64,
) -> dict:
    """Fetch Jupiter v6 quote (ExactIn)."""
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
    """Build unsigned base64 v0 tx from a Jupiter quoteResponse."""
    url = f"{JUP_BASE}/v6/swap"
    body = {
        "userPublicKey": user_pubkey,
        "wrapAndUnwrapSol": wrap_and_unwrap_sol,
        "useSharedAccounts": True,
        "quoteResponse": quote_response,
        "asLegacyTransaction": False,
    }
    r = requests.post(url, json=body, timeout=25)
    r.raise_for_status()
    js = r.json()
    tx_b64 = js.get("swapTransaction")
    if not tx_b64:
        raise RuntimeError(f"Jupiter swap: missing swapTransaction in {js}")
    return tx_b64

# =================== Sign + send (solders) ===================

def _sign_b64_with_keypair(tx_b64: str, kp: Keypair) -> bytes:
    tx = VersionedTransaction.from_bytes(base64.b64decode(tx_b64))
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
    raw = _sign_b64_with_keypair(tx_b64, kp)
    async with AsyncClient(rpc_url) as c:
        resp = await c.send_raw_transaction(
            raw, opts=TxOpts(skip_preflight=skip_preflight, max_retries=max_retries)
        )
        sig = None
        try:
            sig = resp.value  # type: ignore
        except Exception:
            try:
                sig = resp["result"]
            except Exception:
                pass
        if not sig:
            raise RuntimeError(f"send_raw_transaction failed: {resp}")
        return str(sig)

# =================== Thin wrappers used by scripts/main.py ===================

async def maybe_buy(
    kp: Keypair,
    mint: str,
    *,
    amount_sol: float,
    venue: str = "portal",  # "portal" (default) or "jupiter"
    slippage_bps: int | None = None,
    priority_sol: float | None = None,
) -> str:
    """
    Build -> (maybe) sign+send a buy. Honors DRY_RUN.
    - portal: best for fresh Pump pools
    - jupiter: SOL -> token via aggregators (requires quote)
    """
    slippage_bps = slippage_bps if slippage_bps is not None else DEFAULT_PP_SLIPPAGE_BPS
    priority_sol  = priority_sol  if priority_sol  is not None else DEFAULT_PRIORITY_SOL

    if venue == "portal":
        tx_b64 = build_buy_tx_via_portal(
            mint, str(kp.pubkey()),
            amount_sol=amount_sol,
            slippage_bps=slippage_bps,
            priority_sol=priority_sol,
        )
    elif venue == "jupiter":
        # Jupiter needs EXACT IN (SOL atoms)
        sol_atoms = int(round(amount_sol * 1_000_000_000))
        quote = jup_quote(SOL_MINT, mint, in_amount_atoms=sol_atoms, slippage_bps=_env_jup_slippage_bps())
        tx_b64 = jup_swap_tx(quote, str(kp.pubkey()))
    else:
        raise ValueError("venue must be 'portal' or 'jupiter'")

    if DRY_RUN:
        print(f"[DRYRUN] buy {mint} via {venue} for {amount_sol} SOL (slip={slippage_bps}bps)")
        return "DRYRUN"
    return await sign_and_send_b64_tx(tx_b64, kp)

async def maybe_sell(
    kp: Keypair,
    mint: str,
    *,
    token_amount_atoms: int | None = None,   # required for Jupiter
    target_sol: float | None = None,         # PumpPortal mode
    venue: str = "jupiter",                  # default: Jupiter for exits
    slippage_bps: int | None = None,
    priority_sol: float | None = None,
) -> str:
    """
    Build -> (maybe) sign+send a sell. Honors DRY_RUN.
    - venue='jupiter': requires token_amount_atoms (ExactIn token -> SOL)
    - venue='portal' : can pass token_amount (UI) OR target_sol
    """
    slippage_bps = slippage_bps if slippage_bps is not None else (
        DEFAULT_JUP_SLIPPAGE_BPS if venue == "jupiter" else DEFAULT_PP_SLIPPAGE_BPS
    )
    priority_sol  = priority_sol  if priority_sol  is not None else DEFAULT_PRIORITY_SOL

    if venue == "jupiter":
        if token_amount_atoms is None:
            raise ValueError("maybe_sell(jupiter): token_amount_atoms is required for ExactIn")
        quote = jup_quote(mint, SOL_MINT, in_amount_atoms=token_amount_atoms, slippage_bps=slippage_bps)
        tx_b64 = jup_swap_tx(quote, str(kp.pubkey()))
    elif venue == "portal":
        # Convert atoms to UI if provided
        token_amount_ui = None
        if token_amount_atoms is not None:
            token_amount_ui = token_amount_atoms / 1_000_000.0  # assumes 6 dp; adjust if you track decimals
        tx_b64 = build_sell_tx_via_portal(
            mint, str(kp.pubkey()),
            token_amount=token_amount_ui,
            sol_amount=target_sol,
            slippage_bps=slippage_bps,
            priority_sol=priority_sol,
        )
    else:
        raise ValueError("venue must be 'jupiter' or 'portal'")

    if DRY_RUN:
        desc = f"tokens={token_amount_atoms}" if token_amount_atoms is not None else f"target_sol={target_sol}"
        print(f"[DRYRUN] sell {mint} via {venue} ({desc}) slip={slippage_bps}bps")
        return "DRYRUN"
    return await sign_and_send_b64_tx(tx_b64, kp)

__all__ = [
    "build_buy_tx_via_portal",
    "build_sell_tx_via_portal",
    "jup_quote",
    "jup_swap_tx",
    "sign_and_send_b64_tx",
    "maybe_buy",
    "maybe_sell",
]
