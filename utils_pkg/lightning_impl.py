from __future__ import annotations

"""
Jupiter swapper using /swap/v1/* (v6-style).

Goals:
- Quotes are non-legacy so Pump.fun + simple AMM routes are visible.
- Swap POST sends the FULL quoteResponse (unmodified).
- No field renames (inAmount/outAmount preserved).
- Unroutable-pair cache + one bumped requote attempt.
- DRYRUN-safe; robust logging; optional platform/prio fees.
"""

import os
import time
from typing import Optional, Dict, Any, Tuple, Union

import httpx

from logging_setup import get_logger
from utils_pkg.mints import normalize_mint
from utils_pkg.solana_wallet import (
    load_keypair,
    get_pubkey_str,
    sign_and_send_b64_tx,
)

# Prefer diagnostic sender when available (Render shell tests, etc.)
try:  # pragma: no cover
    from utils_pkg.solana_wallet import _send_b64_with_diag  # type: ignore
except Exception:  # pragma: no cover
    _send_b64_with_diag = None  # type: ignore

log = get_logger()

# ============================================================================
# Small typed error to carry Jupiter response JSON when non-200
# ============================================================================


class JupHTTPError(RuntimeError):
    def __init__(self, status: int, url: str, body: dict | str):
        self.status = status
        self.url = url
        self.body = body
        super().__init__(f"HTTP {status} {url}")


# ============================================================================
# Env helpers
# ============================================================================


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


# ============================================================================
# Config (Jupiter + trade knobs)
# ============================================================================

BASE: str = (
    _env_str("JUP_BASE_URL")
    or _env_str("JUP_UPSTREAM")
    or "https://api.jup.ag"
).rstrip("/")

# These are mostly telemetry / debugging flags
AS_LEGACY_ENV: bool = _env_bool("JUP_AS_LEGACY", False)
AS_LEGACY = AS_LEGACY_ENV
ONLY_DIRECT: bool = _env_bool("JUP_ONLY_DIRECT", False)

# Variant toggles (swap still enforces useSharedAccounts=False)
USE_SHARED_ACCOUNTS_ENV: bool = _env_bool("JUP_USE_SHARED_ACCOUNTS", False)
USE_TOKEN_LEDGER_ENV: bool = _env_bool("JUP_USE_TOKEN_LEDGER", False)

# Auth / headers
JUP_API_KEY: str = _env_str("JUP_API_KEY", _env_str("JUP_PRO_KEY", ""))
JUP_BEARER: str = _env_str("JUP_BEARER", "")

# Platform fee (off unless fully configured)
PLATFORM_FEE_BPS: int = _env_int("JUP_PLATFORM_FEE_BPS", 0)
PLATFORM_FEE_ACCOUNT: str = _env_str("JUP_PLATFORM_FEE_ACCOUNT", "")
USE_PLATFORM_FEE: bool = PLATFORM_FEE_BPS > 0 and bool(PLATFORM_FEE_ACCOUNT)

# Slippage / CU / priority fee
DEFAULT_SLIPPAGE_BPS: int = _env_int(
    "PF_SLIPPAGE_BPS",
    _env_int("SLIPPAGE_BPS", 30),
)
COMPUTE_UNIT_LIMIT: int = _env_int("PF_COMPUTE_UNIT_LIMIT", 0)  # 0 → dynamic
WRAP_UNWRAP_SOL: bool = _env_bool("PF_WRAP_UNWRAP_SOL", True)


def _micro_to_lamports(micro: int) -> int:
    try:
        return int(round(max(0, micro) / 1_000_000))
    except Exception:
        return 0


_fallback_micro = _env_int("JITO_FEE_MICROLAMPORTS", 0)
PF_PRIORITY_LAMPORTS: int = _env_int(
    "PF_PRIORITY_LAMPORTS",
    _micro_to_lamports(_fallback_micro),
)
PF_PRIORITY_SOL: float = _env_float("PF_PRIORITY_SOL", 0.0)

# Dry-run toggle (shared with rest of app)
DRY_RUN: bool = (
    _env_bool("DRYRUN", False)
    or _env_bool("DRY_RUN", False)
    or _env_bool("IS_DRYRUN", False)
)

# Unroutable + bump knobs
UNROUTABLE_TTL_SEC: int = max(30, _env_int("JUP_UNROUTABLE_TTL_SEC", 1800))
REQUOTE_BUMP_PCT: int = max(1, _env_int("JUP_REQUOTE_BUMP_PCT", 5))

# Chain constants (allow overrides, default to mainnet)
SOL_MINT = _env_str(
    "SOL_MINT",
    "So11111111111111111111111111111111111111112",
)
USDC_MINT = _env_str(
    "USDC_MINT",
    "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
)
USDT_MINT = _env_str(
    "USDT_MINT",
    "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB",
)
LAMPORTS_PER_SOL = 1_000_000_000


# ============================================================================
# Unroutable pair cache
# ============================================================================

_UNROUTABLE_UNTIL: dict[tuple[str, str], float] = {}


def _is_unroutable_pair(inp: str, out: str) -> bool:
    exp = _UNROUTABLE_UNTIL.get((inp, out))
    return bool(exp and exp > time.time())


def _mark_unroutable(inp: str, out: str, *, ttl: int | None = None) -> None:
    _UNROUTABLE_UNTIL[(inp, out)] = time.time() + float(ttl or UNROUTABLE_TTL_SEC)


# ============================================================================
# HTTP helpers
# ============================================================================


def _jup_headers() -> Dict[str, str]:
    h: Dict[str, str] = {
        "accept": "application/json",
        "content-type": "application/json",
        "origin": "https://jup.ag",
        "referer": "https://jup.ag/",
        "user-agent": "pnlsniper",
    }
    if JUP_API_KEY:
        h["x-api-key"] = JUP_API_KEY
    if JUP_BEARER:
        # Official Jupiter internal header for bearer-based auth
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
            log.warning(
                "JUP_GET_NON200",
                extra={"url": url, "status": r.status_code, "body": body},
            )
            raise JupHTTPError(status=r.status_code, url=url, body=body)
        return r.json()


async def _http_post(url: str, *, json_body: Dict[str, Any]) -> Dict[str, Any]:
    async with httpx.AsyncClient(timeout=25) as c:
        r = await c.post(url, json=json_body, headers=_jup_headers())
        if r.status_code != 200:
            try:
                body: Any = r.json()
            except Exception:
                body = {"text": r.text}
            log.warning(
                "JUP_POST_NON200",
                extra={"url": url, "status": r.status_code, "body": body},
            )
            raise JupHTTPError(status=r.status_code, url=url, body=body)
        return r.json()


# ============================================================================
# Jupiter endpoints + flags
# ============================================================================


def _quote_url() -> str:
    return f"{BASE}/swap/v1/quote"


def _swap_url() -> str:
    return f"{BASE}/swap/v1/swap"


def _variant_flags() -> Dict[str, Any]:
    """
    Helper used in shell tests to introspect config.
    """
    return {
        "base": BASE,
        "as_legacy_env": AS_LEGACY_ENV,
        "only_direct": ONLY_DIRECT,
        "use_shared_accounts_env": USE_SHARED_ACCOUNTS_ENV,
        "use_token_ledger_env": USE_TOKEN_LEDGER_ENV,
        "use_platform_fee": USE_PLATFORM_FEE,
        "platform_fee_bps": PLATFORM_FEE_BPS,
    }


# ============================================================================
# Quote → Swap core
# ============================================================================


def _quote_params(
    *,
    input_mint: str,
    output_mint: str,
    amount: int,
    slippage_bps: int,
) -> Dict[str, Any]:
    """
    Non-legacy quote to ensure v6 routes (Pump, simple AMMs) are included.
    """
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


async def _get_quote_kwonly(
    *,
    input_mint: str,
    output_mint: str,
    amount: int,
    slippage_bps: int,
) -> Dict[str, Any]:
    """
    Low-level Jupiter quote wrapper.

    Raises:
      - RuntimeError("jup_no_route")          → no route even after bump
      - RuntimeError("jup_no_route_cached")   → cached unroutable
      - JupHTTPError                          → for other HTTP failures
    """
    # unroutable cache gate
    if _is_unroutable_pair(input_mint, output_mint):
        log.info(
            "JUP_NO_ROUTE_CACHED",
            extra={"pair": [input_mint, output_mint], "ttl": UNROUTABLE_TTL_SEC},
        )
        raise RuntimeError("jup_no_route_cached")

    url = _quote_url()
    params = _quote_params(
        input_mint=input_mint,
        output_mint=output_mint,
        amount=amount,
        slippage_bps=slippage_bps,
    )

    try:
        js = await _http_get(url, params=params)
    except JupHTTPError as e:
        body = e.body or {}
        err_code = body.get("errorCode", "") if isinstance(body, dict) else ""
        # Jupiter returns COULD_NOT_FIND_ANY_ROUTE / CIRCULAR_ARBITRAGE_IS_DISABLED
        if err_code in {"COULD_NOT_FIND_ANY_ROUTE", "CIRCULAR_ARBITRAGE_IS_DISABLED"}:
            _mark_unroutable(input_mint, output_mint)
            bumped = max(int(amount * (100 + REQUOTE_BUMP_PCT) / 100), amount + 1)
            log.info(
                "JUP_REQUOTE_BUMP",
                extra={
                    "pair": [input_mint, output_mint],
                    "amount": amount,
                    "bumped": bumped,
                    "slippage_bps": slippage_bps,
                },
            )
            try:
                js = await _http_get(
                    url,
                    params=_quote_params(
                        input_mint=input_mint,
                        output_mint=output_mint,
                        amount=bumped,
                        slippage_bps=slippage_bps,
                    ),
                )
            except JupHTTPError as e2:
                body2 = e2.body or {}
                err2 = body2.get("errorCode", "") if isinstance(body2, dict) else ""
                if err2 in {
                    "COULD_NOT_FIND_ANY_ROUTE",
                    "CIRCULAR_ARBITRAGE_IS_DISABLED",
                }:
                    log.info(
                        "JUP_NO_ROUTE_FINAL",
                        extra={"pair": [input_mint, output_mint]},
                    )
                    raise RuntimeError("jup_no_route")
                raise
        else:
            # If Jupiter gave a code, mark unroutable; otherwise bubble up
            if err_code:
                _mark_unroutable(input_mint, output_mint)
                raise RuntimeError("jup_no_route")
            raise

    log.info(
        "JUP_QUOTE_OK",
        extra={
            "base": BASE,
            "legacy_env": AS_LEGACY_ENV,
            "input": input_mint,
            "output": output_mint,
            "in": js.get("inAmount", str(amount)),
            "out": js.get("outAmount"),
            "priceImpactPct": js.get("priceImpactPct"),
        },
    )
    return js


async def _get_quote(*args, **kwargs) -> Dict[str, Any]:
    """
    Positional-compatible wrapper around _get_quote_kwonly.

    Accepts positional:
        _get_quote(input_mint, output_mint, amount, slippage_bps)

    or keyword-only:
        _get_quote(input_mint=..., output_mint=..., amount=..., slippage_bps=...)
    """
    if args and not kwargs and len(args) == 4:
        return await _get_quote_kwonly(
            input_mint=args[0],
            output_mint=args[1],
            amount=args[2],
            slippage_bps=args[3],
        )
    return await _get_quote_kwonly(**kwargs)


def _choose_priority_lamports() -> Optional[int]:
    """
    Pick a priority-fee value based on PF_PRIORITY_* envs.
    """
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
    Build swap using FULL quoteResponse (unmodified) and useSharedAccounts=False.
    """
    dynamic_cu = not (COMPUTE_UNIT_LIMIT and COMPUTE_UNIT_LIMIT > 0)

    body: Dict[str, Any] = {
        "quoteResponse": quote,        # ← includes inAmount/outAmount
        "userPublicKey": user_pubkey,
        "wrapAndUnwrapSol": WRAP_UNWRAP_SOL,
        "dynamicComputeUnitLimit": dynamic_cu,
        "asLegacyTransaction": False,
        "useSharedAccounts": False,    # ← REQUIRED for Pump/simple AMMs
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
    log.info(
        "JUP_SWAP_BUILT",
        extra={
            "variant": "useSharedAccounts:false",
            "dynamic_cu": dynamic_cu,
            "prio_lamports": prio,
        },
    )
    return tx_b64


async def _send_tx_b64(tx_b64: str) -> str:
    """
    Send a base64 transaction via diagnostic sender if available, else sign locally.
    """
    if _send_b64_with_diag is not None:
        try:
            return await _send_b64_with_diag(tx_b64, skip_preflight=True)  # type: ignore[arg-type]
        except Exception as e:  # noqa: BLE001
            log.warning("SEND_WITH_DIAG_FAIL", extra={"err": str(e)[:200]})

    kp = load_keypair()
    return await sign_and_send_b64_tx(tx_b64, kp, skip_preflight=True)


# ============================================================================
# Public API (entry / exit)
# ============================================================================

async def buy_via_lightning(
    *,
    mint: str,
    amount_sol: float,
    slippage_bps: int = DEFAULT_SLIPPAGE_BPS,
    return_meta: bool = False,
) -> Union[Optional[str], Tuple[Optional[str], Dict[str, Any]]]:
    """
    SOL → mint using Jupiter v6.

    - amount_sol: how much SOL to spend
    - slippage_bps: basis-point slippage

    Returns:
      - if return_meta=False (default): tx signature string or None
      - if return_meta=True: (tx_sig_or_None, meta_dict)

    meta_dict ALWAYS contains (when quote succeeds):
      - mint: normalized output mint
      - amount_raw: int        (raw out token amount, from quote["outAmount"])
      - in_amount_raw: int     (raw in amount, from quote["inAmount"])
      - outAmount: same as amount_raw
      - inAmount: same as in_amount_raw
      - priceImpactPct: quote["priceImpactPct"]
    """
    meta: Dict[str, Any] = {}
    try:
        m = normalize_mint(mint)
        lamports_in = max(1, int(float(amount_sol) * LAMPORTS_PER_SOL))

        quote = await _get_quote(
            input_mint=SOL_MINT,
            output_mint=m,
            amount=lamports_in,
            slippage_bps=slippage_bps,
        )

        # Build meta for downstream Strategy / exit engine / Notion logging
        in_raw = int(quote.get("inAmount", lamports_in) or lamports_in)
        out_raw = int(quote.get("outAmount", 0) or 0)

        meta = {
            "mint": m,
            "in_amount_raw": in_raw,
            "amount_raw": out_raw,
            "inAmount": in_raw,
            "outAmount": out_raw,
            "priceImpactPct": quote.get("priceImpactPct"),
        }

        if DRY_RUN:
            log.info(
                "ENTRY_TX_DRYRUN",
                extra={
                    "mint": m,
                    "lamports_in": lamports_in,
                    "slippage_bps": slippage_bps,
                    "out_raw": out_raw,
                },
            )
            return (None, meta) if return_meta else None

        user_pk = get_pubkey_str(load_keypair())
        tx_b64 = await _build_swap_tx(user_pubkey=user_pk, quote=quote)
        sig = await _send_tx_b64(tx_b64)
        log.info(
            "ENTRY_TX_OK",
            extra={
                "sig": sig,
                "mint": m,
                "lamports_in": lamports_in,
                "out_raw": out_raw,
            },
        )
        return (sig, meta) if return_meta else sig

    except RuntimeError as e:
        if str(e) in {"jup_no_route", "jup_no_route_cached"}:
            log.info("ENTRY_SKIP_NO_ROUTE", extra={"mint": mint})
            return (None, meta) if return_meta else None
        log.warning("ENTRY_TX_RT_FAIL", extra={"err": f"{e}"})
        return (None, meta) if return_meta else None
    except httpx.HTTPError as e:
        log.warning("ENTRY_TX_HTTP_ERROR", extra={"err": f"{e}"})
        return (None, meta) if return_meta else None
    except Exception as e:
        log.warning("ENTRY_TX_FAIL", extra={"err": f"{e}"})
        return (None, meta) if return_meta else None


async def sell_via_lightning(
    *,
    mint: str,
    token_program: Optional[str],
    amount_raw: int,
    slippage_bps: int = DEFAULT_SLIPPAGE_BPS,
) -> Optional[str]:
    """
    mint → SOL using Jupiter v6.

    - amount_raw: raw token units (already adjusted for decimals)
    - token_program: ignored for Jupiter v6, kept for call-site compatibility
    """
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
            log.info(
                "EXIT_TX_DRYRUN",
                extra={
                    "mint": m,
                    "amount_raw": amount_raw,
                    "slippage_bps": slippage_bps,
                },
            )
            return None

        user_pk = get_pubkey_str(load_keypair())
        tx_b64 = await _build_swap_tx(user_pubkey=user_pk, quote=quote)
        sig = await _send_tx_b64(tx_b64)
        log.info(
            "EXIT_TX_OK",
            extra={"sig": sig, "mint": m, "amount_raw": amount_raw},
        )
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


# ============================================================================
# Optional debug helpers (for Render shell smoke tests)
# ============================================================================

async def debug_get_quote_sol_to_usdc(
    amount_sol: float = 0.002,
    slippage_bps: int = DEFAULT_SLIPPAGE_BPS,
) -> Dict[str, Any]:
    """
    Quick smoke test for /swap/v1/quote:

    python - <<'PY'
    import asyncio
    from utils_pkg import lightning_impl as L
    async def main():
        q = await L.debug_get_quote_sol_to_usdc(0.002)
        print("quote keys:", sorted(q.keys()))
        print("inAmount:", q.get("inAmount"), "outAmount:", q.get("outAmount"))
    asyncio.run(main())
    PY
    """
    lamports = max(1, int(amount_sol * LAMPORTS_PER_SOL))
    return await _get_quote(
        input_mint=SOL_MINT,
        output_mint=USDC_MINT,
        amount=lamports,
        slippage_bps=slippage_bps,
    )


async def debug_build_only_sol_to_usdc(
    amount_sol: float = 0.002,
    slippage_bps: int = DEFAULT_SLIPPAGE_BPS,
) -> Tuple[int, int]:
    """
    Return (inAmount_lamports, swap_tx_b64_len). Does NOT send the tx.
    Useful for verifying /swap wiring without spending SOL.
    """
    lamports = max(1, int(amount_sol * LAMPORTS_PER_SOL))
    q = await _get_quote(
        input_mint=SOL_MINT,
        output_mint=USDC_MINT,
        amount=lamports,
        slippage_bps=slippage_bps,
    )
    user_pk = get_pubkey_str(load_keypair())
    tx_b64 = await _build_swap_tx(user_pubkey=user_pk, quote=q)
    return int(q.get("inAmount", lamports)), len(tx_b64)
