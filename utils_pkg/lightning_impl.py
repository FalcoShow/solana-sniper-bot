# src/utils_pkg/lightning_impl.py
from __future__ import annotations

"""
utils_pkg/lightning_impl.py — Jupiter swapper (modern /swap/v1/* endpoints)

Key guarantees in this build
- Quote path is **non-legacy** (asLegacyTransaction=false) to see Pump routes.
- Swap POST echoes the **full quoteResponse** (unmodified) and
  **useSharedAccounts=false** (required for Simple AMMs / Pump).
- We never mutate fields like inAmount → amount (no renaming).
- Unroutable-pair cache + one-shot bump retry.
- DRY-RUN safe; robust logging; optional platform fee/prio fee knobs.
"""

import os
import time
from typing import Optional, Dict, Any

import httpx
from httpx import HTTPStatusError

from logging_setup import get_logger
from utils_pkg.mints import normalize_mint
from utils_pkg.solana_wallet import (
    load_keypair,
    get_pubkey_str,
    sign_and_send_b64_tx,
)

# Prefer diagnostic sender when available
try:  # pragma: no cover
    from utils_pkg.solana_wallet import _send_b64_with_diag  # type: ignore
except Exception:  # pragma: no cover
    _send_b64_with_diag = None  # type: ignore

log = get_logger()

# ─────────────────────────────────────────────────────────────────────────────
# Env helpers
# ─────────────────────────────────────────────────────────────────────────────

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
    return str(v).strip().lower() in ("1", "true", "t", "yes", "y", "on")

# ─────────────────────────────────────────────────────────────────────────────
# Config (Jupiter + trade knobs)
# ─────────────────────────────────────────────────────────────────────────────

BASE: str = (_env_str("JUP_BASE_URL") or _env_str("JUP_UPSTREAM") or "https://api.jup.ag").rstrip("/")

# Kept for telemetry only; **quote path ignores this and forces non-legacy**
AS_LEGACY_ENV: bool = _env_bool("JUP_AS_LEGACY", False)
ONLY_DIRECT: bool = _env_bool("JUP_ONLY_DIRECT", False)

# Variant toggles (we explicitly override to useSharedAccounts=False on POST)
USE_SHARED_ACCOUNTS_ENV: bool = _env_bool("JUP_USE_SHARED_ACCOUNTS", False)
USE_TOKEN_LEDGER_ENV: bool = _env_bool("JUP_USE_TOKEN_LEDGER", False)

# Auth/headers
JUP_API_KEY: str = _env_str("JUP_API_KEY", _env_str("JUP_PRO_KEY", ""))
JUP_BEARER: str = _env_str("JUP_BEARER", "")

# Fees (disabled unless fully configured)
PLATFORM_FEE_BPS: int = _env_int("JUP_PLATFORM_FEE_BPS", 0)
PLATFORM_FEE_ACCOUNT: str = _env_str("JUP_PLATFORM_FEE_ACCOUNT", "")
USE_PLATFORM_FEE: bool = PLATFORM_FEE_BPS > 0 and bool(PLATFORM_FEE_ACCOUNT)

# Slippage / CU / priority fee
DEFAULT_SLIPPAGE_BPS: int = _env_int("PF_SLIPPAGE_BPS", 30)
COMPUTE_UNIT_LIMIT: int = _env_int("PF_COMPUTE_UNIT_LIMIT", 0)  # 0 → dynamic
WRAP_UNWRAP_SOL: bool = _env_bool("PF_WRAP_UNWRAP_SOL", True)

# Priority fee helpers
def _micro_to_lamports(micro: int) -> int:
    try:
        return int(round(max(0, micro) / 1_000_000))
    except Exception:
        return 0

_fallback_micro = _env_int("JITO_FEE_MICROLAMPORTS", 0)
PF_PRIORITY_LAMPORTS: int = _env_int("PF_PRIORITY_LAMPORTS", _micro_to_lamports(_fallback_micro))
PF_PRIORITY_SOL: float = _env_float("PF_PRIORITY_SOL", 0.0)

# Dry-run toggle
DRY_RUN: bool = _env_bool("DRYRUN", False) or _env_bool("DRY_RUN", False) or _env_bool("IS_DRYRUN", False)

# Unroutable + bump knobs
UNROUTABLE_TTL_SEC: int = max(30, _env_int("JUP_UNROUTABLE_TTL_SEC", 1800))
REQUOTE_BUMP_PCT: int = max(1, _env_int("JUP_REQUOTE_BUMP_PCT", 5))

# Chain constants
SOL_MINT = "So11111111111111111111111111111111111111112"
LAMPORTS_PER_SOL = 1_000_000_000

# ─────────────────────────────────────────────────────────────────────────────
# Unroutable PAIR cache
# ─────────────────────────────────────────────────────────────────────────────

_UNROUTABLE_UNTIL: dict[tuple[str, str], float] = {}

def _is_unroutable_pair(inp: str, out: str) -> bool:
    exp = _UNROUTABLE_UNTIL.get((inp, out))
    return bool(exp and exp > time.time())

def _mark_unroutable(inp: str, out: str, *, ttl: int | None = None) -> None:
    _UNROUTABLE_UNTIL[(inp, out)] = time.time() + float(ttl or UNROUTABLE_TTL_SEC)

# ─────────────────────────────────────────────────────────────────────────────
# HTTP helpers
# ─────────────────────────────────────────────────────────────────────────────

def _jup_headers() -> Dict[str, str]:
    h = {
        "accept": "application/json",
        "content-type": "application/json",
        "origin": "https://jup.ag",
        "referer": "https://jup.ag/",
        "user-agent": "pnlsniper/router",
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
                body = r.json()
            except Exception:
                body = {"text": r.text}
            log.warning("JUP_GET_NON200", extra={"url": url, "status": r.status_code, "body": body})
            ec, em = (body or {}).get("errorCode"), (body or {}).get("error")
            raise RuntimeError(f"HTTP {r.status_code} {ec or ''} {em or ''}".strip())
        return r.json()

async def _http_post(url: str, *, json_body: Dict[str, Any]) -> Dict[str, Any]:
    async with httpx.AsyncClient(timeout=25) as c:
        r = await c.post(url, json=json_body, headers=_jup_headers())
        if r.status_code != 200:
            try:
                body = r.json()
            except Exception:
                body = {"text": r.text}
            log.warning("JUP_POST_NON200", extra={"url": url, "status": r.status_code, "body": body})
            ec, em = (body or {}).get("errorCode"), (body or {}).get("error")
            raise HTTPStatusError(f"{r.status_code} {ec or ''} {em or ''}".strip(), r.request, r)
        return r.json()

# ─────────────────────────────────────────────────────────────────────────────
# Jupiter endpoints
# ─────────────────────────────────────────────────────────────────────────────

def _quote_url() -> str:
    return f"{BASE}/swap/v1/quote"

def _swap_url() -> str:
    return f"{BASE}/swap/v1/swap"

# ─────────────────────────────────────────────────────────────────────────────
# Quote → Swap
# ─────────────────────────────────────────────────────────────────────────────

def _quote_params(*, input_mint: str, output_mint: str, amount: int, slippage_bps: int) -> Dict[str, Any]:
    # **Force non-legacy** to ensure Pump routes are discoverable.
    p: Dict[str, Any] = {
        "inputMint": input_mint,
        "outputMint": output_mint,
        "amount": str(int(amount)),
        "slippageBps": int(slippage_bps),
        "swapMode": "ExactIn",
        "onlyDirectRoutes": ONLY_DIRECT,
        "asLegacyTransaction": False,
    }
    if USE_PLATFORM_FEE:
        p["platformFeeBps"] = int(PLATFORM_FEE_BPS)
    return p

async def _get_quote_kwonly(*, input_mint: str, output_mint: str, amount: int, slippage_bps: int) -> Dict[str, Any]:
    # unroutable cache gate
    if _is_unroutable_pair(input_mint, output_mint):
        log.info("JUP_NO_ROUTE_CACHED", extra={"pair": [input_mint, output_mint], "ttl": UNROUTABLE_TTL_SEC})
        raise RuntimeError("jup_no_route_cached")

    url = _quote_url()
    params = _quote_params(input_mint=input_mint, output_mint=output_mint, amount=amount, slippage_bps=slippage_bps)

    try:
        js = await _http_get(url, params=params)
    except HTTPStatusError as e:
        # inspect body for error code
        err_code = ""
        if getattr(e, "response", None) is not None:
            try:
                err_code = (e.response.json() or {}).get("errorCode") or ""
            except Exception:
                pass

        if err_code == "COULD_NOT_FIND_ANY_ROUTE":
            _mark_unroutable(input_mint, output_mint)
            bumped = max(int(amount * (100 + REQUOTE_BUMP_PCT) / 100), amount + 1)
            log.info("JUP_REQUOTE_BUMP", extra={"pair": [input_mint, output_mint], "amount": amount, "bumped": bumped})
            try:
                js = await _http_get(url, params=_quote_params(
                    input_mint=input_mint, output_mint=output_mint, amount=bumped, slippage_bps=slippage_bps
                ))
            except HTTPStatusError as e2:
                c2 = ""
                if getattr(e2, "response", None) is not None:
                    try:
                        c2 = (e2.response.json() or {}).get("errorCode") or ""
                    except Exception:
                        pass
                if c2 == "COULD_NOT_FIND_ANY_ROUTE":
                    log.info("JUP_NO_ROUTE_FINAL", extra={"pair": [input_mint, output_mint]})
                    raise RuntimeError("jup_no_route")
                raise
        else:
            if err_code:
                _mark_unroutable(input_mint, output_mint)
                raise RuntimeError("jup_no_route")
            raise

    log.info("QUOTE_OK", extra={
        "base": BASE,
        "legacy_env": AS_LEGACY_ENV,
        "input": input_mint,
        "output": output_mint,
        "in": str(amount),
        "out": js.get("outAmount"),
        "priceImpactPct": js.get("priceImpactPct"),
    })
    return js

async def _get_quote(*args, **kwargs) -> Dict[str, Any]:  # compat wrapper
    if args and not kwargs and len(args) == 4:
        return await _get_quote_kwonly(
            input_mint=args[0], output_mint=args[1], amount=args[2], slippage_bps=args[3]
        )
    return await _get_quote_kwonly(**kwargs)

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
    """
    Build swap using FULL quoteResponse (unmodified) and **useSharedAccounts=false**.
    """
    dynamic_cu = not (COMPUTE_UNIT_LIMIT and COMPUTE_UNIT_LIMIT > 0)

    body: Dict[str, Any] = {
        "quoteResponse": quote,                 # ← pass through, includes inAmount
        "userPublicKey": user_pubkey,
        "wrapAndUnwrapSol": WRAP_UNWRAP_SOL,
        "dynamicComputeUnitLimit": dynamic_cu,
        "asLegacyTransaction": False,           # keep swap non-legacy too
        "useSharedAccounts": False,             # ← REQUIRED for Pump/simple AMMs
    }

    prio = _choose_priority_lamports()
    if prio is not None and prio > 0:
        body["prioritizationFeeLamports"] = int(prio)
    if not dynamic_cu:
        body["computeUnitLimit"] = int(COMPUTE_UNIT_LIMIT)

    if USE_PLATFORM_FEE:
        body["platformFee"] = {
            "feeBps": int(PLATFORM_FEE_BPS),
            "platform": "sniper",
            "feeAccount": PLATFORM_FEE_ACCOUNT,
        }

    js = await _http_post(_swap_url(), json_body=body)
    tx_b64 = js.get("swapTransaction")
    if not tx_b64:
        raise RuntimeError(f"Jupiter /swap missing swapTransaction: {js}")
    log.info("SWAP_BUILT", extra={"variant": "useSharedAccounts:false"})
    return tx_b64

async def _send_tx_b64(tx_b64: str) -> str:
    if _send_b64_with_diag is not None:
        try:
            return await _send_b64_with_diag(tx_b64, skip_preflight=True)  # type: ignore[arg-type]
        except Exception as e:
            log.warning("SEND_WITH_DIAG_FAIL", extra={"err": str(e)[:200]})
    kp = load_keypair()
    return await sign_and_send_b64_tx(tx_b64, kp, skip_preflight=True)

# ─────────────────────────────────────────────────────────────────────────────
# Public API (router calls)
# ─────────────────────────────────────────────────────────────────────────────

async def buy_via_lightning(*, mint: str, amount_sol: float, slippage_bps: int = DEFAULT_SLIPPAGE_BPS) -> Optional[str]:
    try:
        m = normalize_mint(mint)
        lamports_in = max(1, int(float(amount_sol) * LAMPORTS_PER_SOL))

        quote = await _get_quote(
            input_mint=SOL_MINT,
            output_mint=m,
            amount=lamports_in,
            slippage_bps=slippage_bps,
        )

        if DRY_RUN:
            log.info("ENTRY_TX_DRYRUN", extra={"mint": m, "lamports_in": lamports_in, "slippage_bps": slippage_bps})
            return None

        user_pk = get_pubkey_str(load_keypair())
        tx_b64 = await _build_swap_tx(user_pubkey=user_pk, quote=quote)
        sig = await _send_tx_b64(tx_b64)
        log.info("ENTRY_TX_OK", extra={"sig": sig})
        return sig

    except RuntimeError as e:
        if str(e) in {"jup_no_route", "jup_no_route_cached"}:
            log.info("ENTRY_SKIP_NO_ROUTE", extra={"mint": mint})
            return None
        log.warning("ENTRY_TX_RT_FAIL", extra={"err": f"{e}"})
        return None
    except httpx.HTTPError as e:
        log.warning("ENTRY_TX_HTTP_ERROR", extra={"err": f"{e}"})
        return None
    except Exception as e:
        log.warning("ENTRY_TX_FAIL", extra={"err": f"{e}"})
        return None

async def sell_via_lightning(*, mint: str, token_program: Optional[str], amount_raw: int, slippage_bps: int = DEFAULT_SLIPPAGE_BPS) -> Optional[str]:
    del token_program  # not needed for Jupiter v6
    try:
        m = normalize_mint(mint)

        quote = await _get_quote(
            input_mint=m,
            output_mint=SOL_MINT,
            amount=amount_raw,
            slippage_bps=slippage_bps,
        )

        if DRY_RUN:
            log.info("EXIT_TX_DRYRUN", extra={"mint": m, "amount_raw": amount_raw, "slippage_bps": slippage_bps})
            return None

        user_pk = get_pubkey_str(load_keypair())
        tx_b64 = await _build_swap_tx(user_pubkey=user_pk, quote=quote)
        sig = await _send_tx_b64(tx_b64)
        log.info("EXIT_TX_OK", extra={"sig": sig})
        return sig

    except RuntimeError as e:
        if str(e) in {"jup_no_route", "jup_no_route_cached"}:
            log.info("EXIT_SKIP_NO_ROUTE", extra={"mint": mint})
            return None
        log.warning("EXIT_TX_RT_FAIL", extra={"err": f"{e}"})
        return None
    except httpx.HTTPError as e:
        log.warning("EXIT_TX_HTTP_ERROR", extra={"err": f"{e}"})
        return None
    except Exception as e:
        log.warning("EXIT_TX_FAIL", extra={"err": f"{e}"})
        return None
