from __future__ import annotations

"""
utils_pkg/lightning_impl.py — Jupiter v6 swapper (stable)

Design goals
- Target Jupiter v6 at https://public.jupiterapi.com (no legacy by default)
- Do NOT send platform fees unless fully configured (prevents 422)
- Clean headers (origin/referer) and optional API key/bearer
- Quote → Swap with quoteResponse passthrough (no payload re-shaping)
- Optional priority fee (lamports) & compute unit limit
- DRY_RUN support for safe testing
- Friendly logs on non-200 responses (emit JSON body when possible)

Environment knobs (recommended)
- JUP_UPSTREAM (default: https://public.jupiterapi.com)
- JUP_API_KEY / JUP_PRO_KEY (optional), JUP_BEARER (optional)
- JUP_AS_LEGACY=false (v6)
- JUP_ONLY_DIRECT=false
- JUP_PLATFORM_FEE_BPS (default: 0 → disabled)
- JUP_PLATFORM_FEE_ACCOUNT (fee account; required if fee enabled)
- PF_SLIPPAGE_BPS (default: 30)
- PF_PRIORITY_LAMPORTS (optional) or PF_PRIORITY_SOL (optional)
- PF_COMPUTE_UNIT_LIMIT (optional)
- PF_WRAP_UNWRAP_SOL=true
- JITO_FEE_MICROLAMPORTS (optional; fallback → lamports)
- DRYRUN / DRY_RUN / IS_DRYRUN (optional)

Public API
- buy_via_lightning(mint: str, amount_sol: float, slippage_bps: int = ...)
- sell_via_lightning(mint: str, token_program: Optional[str], amount_raw: int, slippage_bps: int = ...)
"""

import os
import json
from typing import Optional, Tuple, Dict, Any

import httpx

from logging_setup import get_logger
from utils_pkg.mints import normalize_mint
from utils_pkg.solana_wallet import (
    load_keypair,
    get_pubkey_str,
    sign_and_send_b64_tx,
)

# Prefer robust sender w/ simulate diagnostics if available
try:  # pragma: no cover
    from utils_pkg.solana_wallet import _send_b64_with_diag  # type: ignore
except Exception:  # pragma: no cover
    _send_b64_with_diag = None  # type: ignore

log = get_logger()

# ──────────────────────────────────────────────────────────────────────────────
# Env helpers
# ──────────────────────────────────────────────────────────────────────────────

def _env_str(name: str, default: str = "") -> str:
    v = os.getenv(name)
    return default if v is None else v.strip()

def _env_int(name: str, default: int) -> int:
    try:
        return int((os.getenv(name, str(default)) or str(default)).strip())
    except Exception:
        return default

def _env_float(name: str, default: float) -> float:
    try:
        return float((os.getenv(name, str(default)) or str(default)).strip())
    except Exception:
        return default

def _env_bool(name: str, default: bool = False) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return v.strip().lower() in ("1", "true", "yes", "on")

# ──────────────────────────────────────────────────────────────────────────────
# Config (Jupiter + trade knobs)
# ──────────────────────────────────────────────────────────────────────────────

BASE: str = (_env_str("JUP_UPSTREAM") or _env_str("JUP_BASE_URL") or "https://public.jupiterapi.com").rstrip("/")
AS_LEGACY: bool = _env_bool("JUP_AS_LEGACY", False)
ONLY_DIRECT: bool = _env_bool("JUP_ONLY_DIRECT", False)

# v6: avoid alt bases (e.g., https://api.jup.ag 404s on /quote); keep empty
ALT_BASES: list[str] = []

# Auth/headers
JUP_API_KEY: str = _env_str("JUP_API_KEY", _env_str("JUP_PRO_KEY", ""))
JUP_BEARER: str   = _env_str("JUP_BEARER", "")

# Fees — DISABLED by default to avoid 422s unless fully configured
PLATFORM_FEE_BPS: int      = _env_int("JUP_PLATFORM_FEE_BPS", 0)
PLATFORM_FEE_ACCOUNT: str  = _env_str("JUP_PLATFORM_FEE_ACCOUNT", "")
USE_PLATFORM_FEE: bool     = PLATFORM_FEE_BPS > 0 and bool(PLATFORM_FEE_ACCOUNT)

# Slippage / CU / priority fee
DEFAULT_SLIPPAGE_BPS: int = _env_int("PF_SLIPPAGE_BPS", 30)
COMPUTE_UNIT_LIMIT: int   = _env_int("PF_COMPUTE_UNIT_LIMIT", 0)  # 0 = dynamic by Jupiter

# --- JITO nicety: fallback from JITO_FEE_MICROLAMPORTS → lamports ---
def _micro_to_lamports(micro: int) -> int:
    # 1 lamport = 1_000_000 microlamports
    try:
        return int(round(max(0, micro) / 1_000_000))
    except Exception:
        return 0

_fallback_micro = _env_int("JITO_FEE_MICROLAMPORTS", 0)
_fallback_lamports = _micro_to_lamports(_fallback_micro)

# If PF_PRIORITY_LAMPORTS is not set, auto-use JITO_FEE_MICROLAMPORTS (converted)
PF_PRIORITY_LAMPORTS: int = _env_int("PF_PRIORITY_LAMPORTS", _fallback_lamports)
PF_PRIORITY_SOL: float    = _env_float("PF_PRIORITY_SOL", 0.0)

WRAP_UNWRAP_SOL: bool = _env_bool("PF_WRAP_UNWRAP_SOL", True)

# Dry-run toggle
DRY_RUN: bool = _env_bool("DRYRUN", False) or _env_bool("DRY_RUN", False) or _env_bool("IS_DRYRUN", False)

# Chain constants
SOL_MINT = "So11111111111111111111111111111111111111112"
LAMPORTS_PER_SOL = 1_000_000_000

# ──────────────────────────────────────────────────────────────────────────────
# HTTP helpers (headers + get/post with friendly error logs)
# ──────────────────────────────────────────────────────────────────────────────

def _jup_headers() -> Dict[str, str]:
    h = {
        "accept": "application/json",
        "content-type": "application/json",
        "origin": "https://jup.ag",
        "referer": "https://jup.ag/",
    }
    if JUP_API_KEY:
        h["x-api-key"] = JUP_API_KEY
    if JUP_BEARER:
        h["x-jup-auth"] = f"Bearer {JUP_BEARER}"
    return h

async def _http_get(url: str, *, params: Dict[str, Any]) -> Dict[str, Any]:
    async with httpx.AsyncClient(timeout=20) as c:
        r = await c.get(url, params=params, headers=_jup_headers())
        if r.status_code != 200:
            try:
                body: Any = r.json()
            except Exception:
                body = {"text": r.text}
            log.warning("JUP_GET_NON200", extra={"url": url, "status": r.status_code, "body": body})
            r.raise_for_status()
        return r.json()

async def _http_post(url: str, *, json_body: Dict[str, Any]) -> Dict[str, Any]:
    async with httpx.AsyncClient(timeout=25) as c:
        r = await c.post(url, json=json_body, headers=_jup_headers())
        if r.status_code != 200:
            try:
                body: Any = r.json()
            except Exception:
                body = {"text": r.text}
            log.warning("JUP_POST_NON200", extra={"url": url, "status": r.status_code, "body": body})
            r.raise_for_status()
        return r.json()

# ──────────────────────────────────────────────────────────────────────────────
# Jupiter v6 flows (quote → swap)
# ──────────────────────────────────────────────────────────────────────────────

async def _get_quote(*, input_mint: str, output_mint: str, amount: int, slippage_bps: int) -> Dict[str, Any]:
    params: Dict[str, Any] = {
        "inputMint": input_mint,
        "outputMint": output_mint,
        "amount": str(amount),
        "slippageBps": int(slippage_bps),
        "swapMode": "ExactIn",
        "onlyDirectRoutes": str(ONLY_DIRECT).lower(),
        "asLegacyTransaction": str(AS_LEGACY).lower(),
    }
    # Include fee only when properly configured
    if USE_PLATFORM_FEE:
        params["platformFeeBps"] = int(PLATFORM_FEE_BPS)

    url = f"{BASE}/quote" if not AS_LEGACY else f"{BASE}/swap/v1/quote"
    js = await _http_get(url, params=params)
    log.info("quote", extra={
        "label": "quote",
        "input": input_mint,
        "output": output_mint,
        "in": str(amount),
        "out": js.get("outAmount"),
        "priceImpactPct": js.get("priceImpactPct"),
    })
    return js

def _choose_priority_lamports() -> Optional[int]:
    if PF_PRIORITY_LAMPORTS > 0:
        return int(PF_PRIORITY_LAMPORTS)
    if PF_PRIORITY_SOL > 0:
        try:
            return int(PF_PRIORITY_SOL * LAMPORTS_PER_SOL)
        except Exception:
            return None
    return None

async def _build_swap_tx(*, user_pubkey: str, quote: Dict[str, Any]) -> str:
    body: Dict[str, Any] = {
        "userPublicKey": user_pubkey,
        "wrapAndUnwrapSol": WRAP_UNWRAP_SOL,
        "dynamicComputeUnitLimit": False if (COMPUTE_UNIT_LIMIT and COMPUTE_UNIT_LIMIT > 0) else True,
        "quoteResponse": quote,
        "asLegacyTransaction": bool(AS_LEGACY),
    }

    prio = _choose_priority_lamports()
    if prio is not None and prio > 0:
        body["prioritizationFeeLamports"] = int(prio)
    if COMPUTE_UNIT_LIMIT and COMPUTE_UNIT_LIMIT > 0:
        body["computeUnitLimit"] = int(COMPUTE_UNIT_LIMIT)

    if USE_PLATFORM_FEE:
        body["platformFee"] = {
            "feeBps": int(PLATFORM_FEE_BPS),
            "platform": "sniper",
            "feeAccount": PLATFORM_FEE_ACCOUNT,
        }

    url = f"{BASE}/swap" if not AS_LEGACY else f"{BASE}/swap/v1/swap"
    js = await _http_post(url, json_body=body)
    tx_b64 = js.get("swapTransaction")
    if not tx_b64:
        raise RuntimeError(f"Jupiter /swap missing swapTransaction: {js}")
    return tx_b64

async def _send_tx_b64(tx_b64: str) -> str:
    # Prefer diagnostic sender if available
    if _send_b64_with_diag is not None:
        try:
            return await _send_b64_with_diag(tx_b64, skip_preflight=True)  # type: ignore[arg-type]
        except Exception as e:
            log.warning("SEND_WITH_DIAG_FAIL", extra={"err": str(e)[:200]})
    # Fallback to simple signer
    kp = load_keypair()
    return await sign_and_send_b64_tx(tx_b64, kp, skip_preflight=True)

# ──────────────────────────────────────────────────────────────────────────────
# Public API (called by router)
# ──────────────────────────────────────────────────────────────────────────────

async def buy_via_lightning(*, mint: str, amount_sol: float, slippage_bps: int = DEFAULT_SLIPPAGE_BPS) -> Tuple[bool, Optional[str]]:
    """Buy `mint` spending SOL. Returns (ok, signature|error)."""
    try:
        m = normalize_mint(mint)
        lamports_in = max(1, int(float(amount_sol) * LAMPORTS_PER_SOL))

        quote = await _get_quote(input_mint=SOL_MINT, output_mint=m, amount=lamports_in, slippage_bps=slippage_bps)

        if DRY_RUN:
            log.info("ENTRY_TX_DRYRUN", extra={"mint": m, "lamports_in": lamports_in, "slippage_bps": slippage_bps})
            return True, None

        user_pk = get_pubkey_str(load_keypair())
        tx_b64 = await _build_swap_tx(user_pubkey=user_pk, quote=quote)
        sig = await _send_tx_b64(tx_b64)
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

async def sell_via_lightning(*, mint: str, token_program: Optional[str], amount_raw: int, slippage_bps: int = DEFAULT_SLIPPAGE_BPS) -> Tuple[bool, Optional[str]]:
    """Sell `amount_raw` units of `mint` back to SOL. token_program is ignored (Jupiter resolves)."""
    del token_program
    try:
        m = normalize_mint(mint)

        quote = await _get_quote(input_mint=m, output_mint=SOL_MINT, amount=amount_raw, slippage_bps=slippage_bps)

        if DRY_RUN:
            log.info("EXIT_TX_DRYRUN", extra={"mint": m, "amount_raw": amount_raw, "slippage_bps": slippage_bps})
            return True, None

        user_pk = get_pubkey_str(load_keypair())
        tx_b64 = await _build_swap_tx(user_pubkey=user_pk, quote=quote)
        sig = await _send_tx_b64(tx_b64)
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
