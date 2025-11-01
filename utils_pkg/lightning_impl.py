# utils_pkg/lightning_impl.py
from __future__ import annotations

import os
import base64
from typing import Optional, Tuple, Dict, Any

import httpx

# Local deps already in your repo
from logging_setup import get_logger
from utils_pkg.mints import normalize_mint
from utils_pkg.solana_wallet import (
    load_keypair,         # loads keypair (Render id.json or env)
    get_pubkey_str,       # returns base58 pubkey string
    sign_and_send_b64_tx, # signs & sends a base64 transaction, returns signature
)

log = get_logger()

# ──────────────────────────────────────────────────────────────────────────────
# Env / constants
# ──────────────────────────────────────────────────────────────────────────────

def _env_str(name: str, default: str = "") -> str:
    v = os.getenv(name)
    return default if v is None else v.strip()

def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)).strip())
    except Exception:
        return default

def _env_bool(name: str, default: bool = False) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return v.strip().lower() in ("1", "true", "yes", "on")

# Jupiter upstream(s)
JUP_BASE = _env_str("JUP_BASE_URL", "https://public.jupiterapi.com")
JUP_ALT  = _env_str("JUP_ALT_BASE_URLS", "https://api.jup.ag")
JUP_API_KEY = _env_str("JUP_API_KEY", "")

# Trading knobs (align with your .env/dashboard)
DEFAULT_SLIPPAGE_BPS = _env_int("PF_SLIPPAGE_BPS", 30)          # 0.30%
PLATFORM_FEE_BPS     = _env_int("PF_FEE_BPS", 20)               # 0.20% (optional)
WRAP_UNWRAP_SOL      = _env_bool("PF_WRAP_UNWRAP_SOL", True)
LEGACY_TX            = _env_bool("PF_USE_LEGACY_TX", False)

# Optional priority fees & CU limit (Jito / CU tweak handled in your signer)
JITO_TIP_LAMPORTS    = _env_int("PF_JITO_TIP_LAMPORTS", 0)      # 0 = let RPC decide
COMPUTE_UNIT_LIMIT   = _env_int("PF_COMPUTE_UNIT_LIMIT", 0)     # 0 = default

SOL_MINT = "So11111111111111111111111111111111111111112"
LAMPORTS_PER_SOL = 1_000_000_000

# ──────────────────────────────────────────────────────────────────────────────
# HTTP helpers
# ──────────────────────────────────────────────────────────────────────────────

def _headers() -> Dict[str, str]:
    h = {"content-type": "application/json"}
    if JUP_API_KEY:
        h["x-api-key"] = JUP_API_KEY
    return h

def _pick_base() -> str:
    # prefer public endpoint; fall back to alt if needed
    return JUP_BASE or "https://public.jupiterapi.com"

def _merge_platform_fee(params: Dict[str, Any]) -> Dict[str, Any]:
    if PLATFORM_FEE_BPS > 0:
        params["platformFeeBps"] = PLATFORM_FEE_BPS
    return params

# ──────────────────────────────────────────────────────────────────────────────
# Core Jupiter flows (quote → swap tx → sign & send)
# ──────────────────────────────────────────────────────────────────────────────

async def _jup_quote(
    *,
    input_mint: str,
    output_mint: str,
    amount: int,
    slippage_bps: int,
) -> Dict[str, Any]:
    """
    Calls /quote and returns parsed JSON (raises on non-2xx).
    """
    base = _pick_base()
    params = {
        "inputMint":  input_mint,
        "outputMint": output_mint,
        "amount":     str(amount),
        "slippageBps": int(slippage_bps),
        "swapMode":   "ExactIn",
        "onlyDirectRoutes": "false",
        "asLegacyTransaction": "true" if LEGACY_TX else "false",
    }
    _merge_platform_fee(params)

    async with httpx.AsyncClient(timeout=15) as c:
        r = await c.get(f"{base}/quote", params=params, headers=_headers())
        r.raise_for_status()
        return r.json()

async def _jup_swap_tx(
    *,
    user_pubkey: str,
    input_mint: str,
    output_mint: str,
    amount: int,
    slippage_bps: int,
) -> str:
    """
    Calls /swap to build a swap transaction. Returns base64-encoded tx.
    """
    base = _pick_base()
    body: Dict[str, Any] = {
        "userPublicKey": user_pubkey,
        "wrapAndUnwrapSol": WRAP_UNWRAP_SOL,
        "asLegacyTransaction": LEGACY_TX,
        "payload": {
            "inputMint":  input_mint,
            "outputMint": output_mint,
            "amount":     str(amount),
            "slippageBps": int(slippage_bps),
            "swapMode":   "ExactIn",
        },
        # Jupiter v6 supports priority fee knobs; they're optional.
        "prioritizationFeeLamports": JITO_TIP_LAMPORTS if JITO_TIP_LAMPORTS > 0 else None,
        "computeUnitLimit": COMPUTE_UNIT_LIMIT if COMPUTE_UNIT_LIMIT > 0 else None,
    }
    _merge_platform_fee(body["payload"])

    async with httpx.AsyncClient(timeout=20) as c:
        r = await c.post(f"{base}/swap", json=body, headers=_headers())
        r.raise_for_status()
        data = r.json()
        # Jupiter returns {"swapTransaction": "<base64>"} on success
        tx_b64 = data.get("swapTransaction")
        if not tx_b64:
            raise RuntimeError(f"Jupiter /swap did not return swapTransaction: {data}")
        return tx_b64

# ──────────────────────────────────────────────────────────────────────────────
# Public API (used by router)
# ──────────────────────────────────────────────────────────────────────────────

async def buy_via_lightning(
    *,
    mint: str,
    amount_sol: float,
    slippage_bps: int = DEFAULT_SLIPPAGE_BPS,
) -> Tuple[bool, Optional[str]]:
    """
    Buy `mint` using SOL via Jupiter. Returns (ok, signature_or_error).
    """
    try:
        mint = normalize_mint(mint)
        kp = load_keypair()
        user_pk = get_pubkey_str(kp)

        lamports_in = max(1, int(amount_sol * LAMPORTS_PER_SOL))

        # (Optional, but good for logs/guardrail): get a quote first
        quote = await _jup_quote(
            input_mint=SOL_MINT,
            output_mint=mint,
            amount=lamports_in,
            slippage_bps=slippage_bps,
        )
        log.info(
            "quote-buy",
            extra={
                "label": "quote-buy",
                "in_lamports": lamports_in,
                "out_amount": quote.get("outAmount"),
                "priceImpactPct": quote.get("priceImpactPct"),
            },
        )

        # Build tx
        tx_b64 = await _jup_swap_tx(
            user_pubkey=user_pk,
            input_mint=SOL_MINT,
            output_mint=mint,
            amount=lamports_in,
            slippage_bps=slippage_bps,
        )

        # Sign + send
        sig = await sign_and_send_b64_tx(tx_b64)
        log.info("ENTRY_TX_OK", extra={"sig": sig})
        return True, sig

    except httpx.HTTPError as e:
        msg = f"HTTPError during buy: {e}"
        log.warning("ENTRY_TX_HTTP_ERROR", extra={"err": msg})
        return False, msg
    except Exception as e:
        msg = f"buy exception: {e}"
        log.warning("ENTRY_TX_FAIL", extra={"err": msg})
        return False, msg


async def sell_via_lightning(
    *,
    mint: str,
    token_program: Optional[str],
    amount_raw: int,
    slippage_bps: int = DEFAULT_SLIPPAGE_BPS,
) -> Tuple[bool, Optional[str]]:
    """
    Sell `mint` back to SOL via Jupiter. `amount_raw` is in mint's base units.
    `token_program` is optional; Jupiter resolves it if omitted.
    """
    try:
        mint = normalize_mint(mint)
        kp = load_keypair()
        user_pk = get_pubkey_str(kp)

        # Quote for logs / sanity
        quote = await _jup_quote(
            input_mint=mint,
            output_mint=SOL_MINT,
            amount=amount_raw,
            slippage_bps=slippage_bps,
        )
        log.info(
            "quote-sell",
            extra={
                "label": "quote-sell",
                "in_raw": amount_raw,
                "out_amount": quote.get("outAmount"),
                "priceImpactPct": quote.get("priceImpactPct"),
            },
        )

        # Build tx (Jupiter will include the correct token program in the tx)
        tx_b64 = await _jup_swap_tx(
            user_pubkey=user_pk,
            input_mint=mint,
            output_mint=SOL_MINT,
            amount=amount_raw,
            slippage_bps=slippage_bps,
        )

        sig = await sign_and_send_b64_tx(tx_b64)
        log.info("EXIT_TX_OK", extra={"sig": sig})
        return True, sig

    except httpx.HTTPError as e:
        msg = f"HTTPError during sell: {e}"
        log.warning("EXIT_TX_HTTP_ERROR", extra={"err": msg})
        return False, msg
    except Exception as e:
        msg = f"sell exception: {e}"
        log.warning("EXIT_TX_FAIL", extra={"err": msg})
        return False, msg
