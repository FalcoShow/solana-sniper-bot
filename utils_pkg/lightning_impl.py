from __future__ import annotations

"""
utils_pkg/lightning_impl.py — Jupiter v6/legacy swapper

Fixes & features
- Authenticated requests to https://api.jup.ag using x-api-key (and optional bearer).
- v6 (default) or legacy (/swap/v1/*) flow via JUP_AS_LEGACY.
- v6 swap variants: useSharedAccounts / useTokenLedger (prefer Shared Accounts).
- Stable headers to avoid CDN/WAF edge issues.
- Pair-scoped unroutable cache (TTL) to avoid hammering Jupiter.
- One-shot (+JUP_REQUOTE_BUMP_PCT%) amount bump if "COULD_NOT_FIND_ANY_ROUTE".
- Quote → Swap via quoteResponse passthrough.
- Priority fee / compute-unit knobs, wrap/unwrap SOL, DRY-RUN safe.

Environment knobs
- JUP_BASE_URL=https://api.jup.ag        (default)
- JUP_UPSTREAM                           (fallback if BASE unset)
- JUP_API_KEY / JUP_PRO_KEY              (either works)
- JUP_BEARER                             (optional bearer)
- JUP_AS_LEGACY=0                        (set 1 to force legacy)
- JUP_ONLY_DIRECT=0
- JUP_USE_SHARED_ACCOUNTS=1              (recommended)
- JUP_USE_TOKEN_LEDGER=0
- JUP_PLATFORM_FEE_BPS=0                 (and JUP_PLATFORM_FEE_ACCOUNT if >0)
- PF_SLIPPAGE_BPS=30
- PF_PRIORITY_LAMPORTS (or PF_PRIORITY_SOL)
- PF_COMPUTE_UNIT_LIMIT=0                (0 → dynamic)
- PF_WRAP_UNWRAP_SOL=1
- JITO_FEE_MICROLAMPORTS                 (fallback → lamports)
- DRYRUN / DRY_RUN / IS_DRYRUN
- JUP_UNROUTABLE_TTL_SEC=1800            (unroutable PAIR cache TTL)
- JUP_REQUOTE_BUMP_PCT=5                 (amount bump for 1 retry on no-route)
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

# Prefer robust sender (if available)
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
    return v.strip().lower() in ("1", "true", "yes", "on")

# ──────────────────────────────────────────────────────────────────────────────
# Config (Jupiter + trade knobs)
# ──────────────────────────────────────────────────────────────────────────────

BASE: str = (_env_str("JUP_BASE_URL") or _env_str("JUP_UPSTREAM") or "https://api.jup.ag").rstrip("/")
AS_LEGACY: bool = _env_bool("JUP_AS_LEGACY", False)  # v6 by default; legacy if True
ONLY_DIRECT: bool = _env_bool("JUP_ONLY_DIRECT", False)

# New variant toggles (prefer shared accounts if both true)
USE_SHARED_ACCOUNTS: bool = _env_bool("JUP_USE_SHARED_ACCOUNTS", True)
USE_TOKEN_LEDGER: bool = _env_bool("JUP_USE_TOKEN_LEDGER", False)
if USE_SHARED_ACCOUNTS and USE_TOKEN_LEDGER:
    log.warning(
        "JUP_VARIANT_BOTH_SET",
        extra={"note": "Both SharedAccounts and TokenLedger set; preferring SharedAccounts"},
    )
    USE_TOKEN_LEDGER = False  # prefer shared

# Auth/headers
JUP_API_KEY: str = _env_str("JUP_API_KEY", _env_str("JUP_PRO_KEY", ""))
JUP_BEARER: str = _env_str("JUP_BEARER", "")

# Fees — DISABLED by default to avoid 4xx unless fully configured
PLATFORM_FEE_BPS: int = _env_int("JUP_PLATFORM_FEE_BPS", 0)
PLATFORM_FEE_ACCOUNT: str = _env_str("JUP_PLATFORM_FEE_ACCOUNT", "")
USE_PLATFORM_FEE: bool = PLATFORM_FEE_BPS > 0 and bool(PLATFORM_FEE_ACCOUNT)

# Slippage / CU / priority fee
DEFAULT_SLIPPAGE_BPS: int = _env_int("PF_SLIPPAGE_BPS", 30)
COMPUTE_UNIT_LIMIT: int = _env_int("PF_COMPUTE_UNIT_LIMIT", 0)  # 0 → dynamic

# Priority fee sources
def _micro_to_lamports(micro: int) -> int:
    # 1 lamport = 1_000_000 microlamports
    try:
        return int(round(max(0, micro) / 1_000_000))
    except Exception:
        return 0

_fallback_micro = _env_int("JITO_FEE_MICROLAMPORTS", 0)
_fallback_lamports = _micro_to_lamports(_fallback_micro)

PF_PRIORITY_LAMPORTS: int = _env_int("PF_PRIORITY_LAMPORTS", _fallback_lamports)
PF_PRIORITY_SOL: float = _env_float("PF_PRIORITY_SOL", 0.0)

WRAP_UNWRAP_SOL: bool = _env_bool("PF_WRAP_UNWRAP_SOL", True)

# Dry-run toggle
DRY_RUN: bool = _env_bool("DRYRUN", False) or _env_bool("DRY_RUN", False) or _env_bool("IS_DRYRUN", False)

# Unroutable + bump knobs
UNROUTABLE_TTL_SEC: int = max(30, _env_int("JUP_UNROUTABLE_TTL_SEC", 1800))
REQUOTE_BUMP_PCT: int = max(1, _env_int("JUP_REQUOTE_BUMP_PCT", 5))

# Chain constants
SOL_MINT = "So11111111111111111111111111111111111111112"
LAMPORTS_PER_SOL = 1_000_000_000

# ──────────────────────────────────────────────────────────────────────────────
# Unroutable PAIR cache
# ──────────────────────────────────────────────────────────────────────────────

# Cache is keyed by (inputMint, outputMint) → expiry_ts
_UNROUTABLE_UNTIL: dict[tuple[str, str], float] = {}

def _is_unroutable_pair(inp: str, out: str) -> bool:
    exp = _UNROUTABLE_UNTIL.get((inp, out))
    return bool(exp and exp > time.time())

def _mark_unroutable(inp: str, out: str, *, ttl: int | None = None) -> None:
    _UNROUTABLE_UNTIL[(inp, out)] = time.time() + float(ttl or UNROUTABLE_TTL_SEC)

# ──────────────────────────────────────────────────────────────────────────────
# HTTP helpers (headers + get/post with friendly error logs)
# ──────────────────────────────────────────────────────────────────────────────

def _jup_headers() -> Dict[str, str]:
    """
    Common headers for Jupiter. Must include x-api-key when present.
    Keep a stable user-agent; include origin/referer as most clients do.
    """
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
                body: Any = r.json()
            except Exception:
                body = {"text": r.text}
            log.warning("JUP_GET_NON200", extra={"url": url, "status": r.status_code, "body": body})
            # Raise HTTPStatusError but include Jupiter errorCode/error in message; keep response attached
            ec = (body or {}).get("errorCode")
            em = (body or {}).get("error")
            raise HTTPStatusError(
                f"{r.status_code} {ec or ''} {em or ''}".strip(),
                request=r.request,
                response=r,
            )
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
            ec = (body or {}).get("errorCode")
            em = (body or {}).get("error")
            raise HTTPStatusError(
                f"{r.status_code} {ec or ''} {em or ''}".strip(),
                request=r.request,
                response=r,
            )
        return r.json()

# ──────────────────────────────────────────────────────────────────────────────
# Jupiter flows (quote → swap)
# ──────────────────────────────────────────────────────────────────────────────

def _quote_url() -> str:
    return f"{BASE}/quote" if not AS_LEGACY else f"{BASE}/swap/v1/quote"

def _swap_url() -> str:
    return f"{BASE}/swap" if not AS_LEGACY else f"{BASE}/swap/v1/swap"

def _quote_params(*, input_mint: str, output_mint: str, amount: int, slippage_bps: int) -> Dict[str, Any]:
    p: Dict[str, Any] = {
        "inputMint": input_mint,
        "outputMint": output_mint,
        "amount": str(int(amount)),          # as string for big ints
        "slippageBps": int(slippage_bps),
        "swapMode": "ExactIn",
        "onlyDirectRoutes": ONLY_DIRECT,
        "asLegacyTransaction": AS_LEGACY,    # ignored by v6; used by legacy
    }
    if USE_PLATFORM_FEE:
        p["platformFeeBps"] = int(PLATFORM_FEE_BPS)
    return p

async def _get_quote(*, input_mint: str, output_mint: str, amount: int, slippage_bps: int) -> Dict[str, Any]:
    """
    Ask Jupiter for a quote (v6 by default; legacy if AS_LEGACY=1).
    Adds:
      - pair-scoped unroutable cache check
      - one-shot +REQUOTE_BUMP_PCT% amount bump if "COULD_NOT_FIND_ANY_ROUTE"
      - robust handling of HTTPStatusError by inspecting response JSON for errorCode
    """
    # Guard: unroutable pair cache
    if _is_unroutable_pair(input_mint, output_mint):
        log.info("JUP_NO_ROUTE_CACHED", extra={"pair": [input_mint, output_mint], "ttl": UNROUTABLE_TTL_SEC})
        raise RuntimeError("jup_no_route_cached")

    url = _quote_url()
    params = _quote_params(input_mint=input_mint, output_mint=output_mint, amount=amount, slippage_bps=slippage_bps)

    # First attempt
    try:
        js = await _http_get(url, params=params)
    except HTTPStatusError as e:
        # Parse Jupiter errorCode reliably from the response body
        err_code, err_text = "", ""
        resp = getattr(e, "response", None)
        if resp is not None:
            try:
                body = resp.json()
                err_code = (body or {}).get("errorCode") or ""
                err_text = (body or {}).get("error") or ""
            except Exception:
                try:
                    err_text = resp.text
                except Exception:
                    pass

        msg = f"{err_code} {err_text}".strip()

        if "COULD_NOT_FIND_ANY_ROUTE" in msg:
            # mark unroutable then try a single bump
            _mark_unroutable(input_mint, output_mint)
            bumped = max(int(amount * (100 + REQUOTE_BUMP_PCT) / 100), amount + 1)
            log.info("JUP_REQUOTE_BUMP", extra={"pair": [input_mint, output_mint], "amount": amount, "bumped": bumped})
            try:
                js = await _http_get(url, params=_quote_params(
                    input_mint=input_mint, output_mint=output_mint, amount=bumped, slippage_bps=slippage_bps
                ))
            except HTTPStatusError as e2:
                # If bump also fails on routing, finalize as no route
                code2 = ""
                r2 = getattr(e2, "response", None)
                if r2 is not None:
                    try:
                        code2 = (r2.json() or {}).get("errorCode") or ""
                    except Exception:
                        pass
                if code2 == "COULD_NOT_FIND_ANY_ROUTE":
                    log.info("JUP_NO_ROUTE_FINAL", extra={"pair": [input_mint, output_mint]})
                    raise RuntimeError("jup_no_route")
                # Other HTTP errors bubble up
                raise
        elif any(tag in msg for tag in ("TOKEN_NOT_TRADABLE", "MINT_NOT_FOUND", "POOL_NOT_FOUND", "COULD_NOT_FIND_ANY_ROUTE")):
            _mark_unroutable(input_mint, output_mint)
            raise RuntimeError("jup_no_route")
        else:
            # Different HTTP error → bubble up (callers may soft-skip)
            raise

    # OK
    log.info(
        "QUOTE_OK",
        extra={
            "base": BASE,
            "legacy": AS_LEGACY,
            "input": input_mint,
            "output": output_mint,
            "in": str(amount),
            "out": js.get("outAmount"),
            "priceImpactPct": js.get("priceImpactPct"),
        },
    )
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

def _variant_flags() -> Dict[str, bool]:
    """
    Decide which NEW Jupiter swap variant to request.
    Prefers Shared Accounts when both toggles are true or neither provided.
    """
    if USE_SHARED_ACCOUNTS:
        return {"useSharedAccounts": True}
    if USE_TOKEN_LEDGER:
        return {"useTokenLedger": True}
    log.warning("JUP_VARIANT_DEFAULTED", extra={"note": "Neither variant env set; defaulting to useSharedAccounts"})
    return {"useSharedAccounts": True}

async def _build_swap_tx(*, user_pubkey: str, quote: Dict[str, Any]) -> str:
    """
    Build the swap transaction using the selected variant.
    """
    dynamic_cu = not (COMPUTE_UNIT_LIMIT and COMPUTE_UNIT_LIMIT > 0)

    body: Dict[str, Any] = {
        "userPublicKey": user_pubkey,
        "wrapAndUnwrapSol": WRAP_UNWRAP_SOL,
        "dynamicComputeUnitLimit": dynamic_cu,
        "quoteResponse": quote,
        "asLegacyTransaction": bool(AS_LEGACY),
        **_variant_flags(),  # Shared Accounts or Token Ledger
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
    log.info("SWAP_BUILT", extra={"variant": "SharedAccounts" if "useSharedAccounts" in body else "TokenLedger"})
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

async def buy_via_lightning(*, mint: str, amount_sol: float, slippage_bps: int = DEFAULT_SLIPPAGE_BPS) -> Optional[str]:
    """Buy `mint` by spending SOL. Returns tx signature on success, else None."""
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
        # "jup_no_route" or "jup_no_route_cached" → clean skip
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
    """Sell `amount_raw` of `mint` to SOL. Returns tx signature on success, else None."""
    del token_program  # Jupiter v6 doesn’t require us to choose token program here
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
