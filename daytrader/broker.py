# daytrader/broker.py
from __future__ import annotations

"""
daytrader.broker
----------------
Thin, SAFE bridge from daytrader strategy -> your canonical Jupiter swap path.

Hard rules (God-Tier contract)
-----------------------------
- NO side-effects unless explicitly enabled via env (DT_BROKER_ENABLED=1).
- NO competing execution engine: we ONLY call your existing swap bridge.
- Deterministic validation + clean logging; no background tasks.
- Unit model is explicit: amount_input is always "exact in" amount in input-token units.

Public API (backwards compatible)
---------------------------------
- async place_order(pair, side, amount_input, slippage=..., reason="daytrader") -> (ok, signature)
- async buy_base_with_quote(pair, quote_amount, ...) -> (ok, signature)
- async sell_base_for_quote(pair, base_amount, ...) -> (ok, signature)

Env (safe defaults)
-------------------
DT_BROKER_ENABLED=0|1            default 0 (OFF)
DT_BROKER_DRY_RUN=0|1            default 0 (if 1, logs and returns ok with fake sig)
DT_BROKER_ALLOW_PAIRS=...        optional comma list, e.g. "SOL/USDC,JUP/USDC" (if set, others blocked)

DT_BROKER_DEFAULT_SLIPPAGE=0.005 default 0.5%
DT_BROKER_MAX_SLIPPAGE=0.03      default 3%
DT_BROKER_MIN_AMOUNT=0.0         default 0 (set >0 to block dust)

DT_TOKEN_REGISTRY_JSON=...       optional JSON to extend/override TOKENS:
  {"BONK":{"mint":"...","decimals":5}, ...}

Notes
-----
- We call utils_pkg.core_utils.swap_token_jupiter with multiple possible arg names
  to stay compatible across your refactors.
"""

import json
import logging
import math
import os
import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from decimal import ROUND_DOWN, Decimal, InvalidOperation, getcontext
from typing import Any

# Increase Decimal precision for token math
getcontext().prec = 50

# -----------------------------------------------------------------------------
# Logging (shim-safe; no hard dependency on repo logger)
# -----------------------------------------------------------------------------
_LOG = logging.getLogger("daytrader.broker")
# Do NOT basicConfig here (import-time side effect). Let app configure logging.
try:
    from utils_pkg.core_utils import logger as _CORE_LOG  # type: ignore

    _LOG = _CORE_LOG  # type: ignore[assignment]
except Exception:
    pass


# -----------------------------------------------------------------------------
# Canonical swap bridge import (no alternate engines)
# -----------------------------------------------------------------------------
_SWAP_FN: Callable[..., Any] | None = None
_SWAP_IMPORT_ERR: str | None = None
try:
    from utils_pkg.core_utils import swap_token_jupiter as _SWAP_FN  # type: ignore
except Exception as err:
    _SWAP_IMPORT_ERR = str(err)
    _SWAP_FN = None


# -----------------------------------------------------------------------------
# Token registry
# -----------------------------------------------------------------------------
TOKENS: dict[str, dict[str, Any]] = {
    "SOL": {"mint": "So11111111111111111111111111111111111111112", "decimals": 9},
    "USDC": {"mint": "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v", "decimals": 6},
    "JUP": {"mint": "JUP4Fb2cqiRUcaTHdrPC8h2gNsA2ETXiPDD33WcGuJB", "decimals": 6},
    "RAY": {"mint": "4k3Dyjzvzp8eMZWUXbBCjEvwSkkk59S5iCNLY3QrkX6R", "decimals": 6},
}

# Optional informational list (not enforced unless DT_BROKER_ALLOW_PAIRS set)
PAIRS = ["SOL/USDC", "JUP/SOL", "RAY/SOL", "JUP/USDC", "RAY/USDC"]


# -----------------------------------------------------------------------------
# Env helpers
# -----------------------------------------------------------------------------
_TRUE = {"1", "true", "yes", "y", "on", "t"}


def _env_bool(name: str, default: bool = False) -> bool:
    raw = str(os.getenv(name, "")).strip()
    if raw == "":
        return bool(default)
    return raw.lower() in _TRUE


def _env_float(name: str, default: float) -> float:
    raw = str(os.getenv(name, "")).strip()
    if raw == "":
        return float(default)
    try:
        v = float(raw)
        if not math.isfinite(v):
            return float(default)
        return v
    except Exception:
        return float(default)


def _env_str(name: str, default: str = "") -> str:
    v = os.getenv(name)
    return default if v is None else str(v)


def _parse_allow_pairs() -> set[str] | None:
    raw = _env_str("DT_BROKER_ALLOW_PAIRS", "").strip()
    if not raw:
        return None
    out: set[str] = set()
    for part in raw.split(","):
        p = part.strip().upper()
        if p:
            out.add(p)
    return out if out else None


def _load_token_registry_overrides() -> None:
    """
    Allows JSON env extension/override without editing code.
    Safe: never raises.
    """
    raw = _env_str("DT_TOKEN_REGISTRY_JSON", "").strip()
    if not raw:
        return
    try:
        data = json.loads(raw)
        if not isinstance(data, dict):
            return
        for sym, meta in data.items():
            if not isinstance(sym, str) or not isinstance(meta, dict):
                continue
            sym_u = sym.strip().upper()
            mint = meta.get("mint")
            decimals = meta.get("decimals")
            if not isinstance(mint, str) or not mint.strip():
                continue
            try:
                dec_i = int(decimals)
            except Exception:
                continue
            if dec_i < 0 or dec_i > 18:
                continue
            TOKENS[sym_u] = {"mint": mint.strip(), "decimals": dec_i}
    except Exception as exc:
        _LOG.warning("[daytrader.broker] DT_TOKEN_REGISTRY_JSON parse failed: %s", exc)


_load_token_registry_overrides()


# -----------------------------------------------------------------------------
# Types
# -----------------------------------------------------------------------------
@dataclass(frozen=True)
class Token:
    symbol: str
    mint: str
    decimals: int


@dataclass(frozen=True)
class PairInfo:
    pair: str
    base: Token
    quote: Token


# -----------------------------------------------------------------------------
# Registry helpers
# -----------------------------------------------------------------------------
def _get_token(symbol: str) -> Token:
    sym = str(symbol).strip().upper()
    meta = TOKENS.get(sym)
    if not meta:
        raise ValueError(f"[broker] Unknown token symbol '{sym}'")
    mint = str(meta.get("mint", "")).strip()
    try:
        decimals = int(meta.get("decimals", -1))
    except Exception as err:
        raise ValueError(f"[broker] Bad token decimals for '{sym}'") from err
    if not mint or decimals < 0:
        raise ValueError(f"[broker] Bad token meta for '{sym}'")
    return Token(symbol=sym, mint=mint, decimals=decimals)


def _parse_pair(pair: str) -> PairInfo:
    p = str(pair).strip().upper()
    if "/" not in p:
        raise ValueError(f"[broker] Pair must be BASE/QUOTE, got '{pair}'")
    base_sym, quote_sym = p.split("/", 1)
    base = _get_token(base_sym)
    quote = _get_token(quote_sym)
    return PairInfo(pair=f"{base.symbol}/{quote.symbol}", base=base, quote=quote)


# -----------------------------------------------------------------------------
# Amount helpers
# -----------------------------------------------------------------------------
def _to_decimal(x: Any) -> Decimal:
    try:
        return Decimal(str(x).strip())
    except (InvalidOperation, ValueError, TypeError) as err:
        # B904: raise from err
        raise ValueError(f"[broker] Invalid numeric value: {x!r}") from err


def _quantize_units(amount: Decimal, decimals: int) -> Decimal:
    """
    Quantize amount to token decimals WITHOUT rounding up.
    """
    if decimals < 0:
        return amount
    quantum = Decimal("1") / (Decimal(10) ** Decimal(decimals))
    return amount.quantize(quantum, rounding=ROUND_DOWN)


def _validate_amount(amount_input: Any, decimals: int, min_amount: float) -> Decimal:
    amt = _to_decimal(amount_input)
    if (not amt.is_finite()) or amt <= 0:
        raise ValueError(f"[broker] Reject non-positive amount_input={amount_input!r}")
    amt_q = _quantize_units(amt, decimals)
    if amt_q <= 0:
        raise ValueError(f"[broker] Amount too small after quantize ({decimals}dp): {amt}")
    if min_amount > 0:
        if amt_q < Decimal(str(min_amount)):
            raise ValueError(f"[broker] Amount below DT_BROKER_MIN_AMOUNT ({min_amount}): {amt_q}")
    return amt_q


def _clamp_slippage(slip: Any) -> float:
    try:
        s = float(slip)
    except Exception:
        s = 0.0
    if (not math.isfinite(s)) or s < 0.0:
        s = 0.0
    max_s = _env_float("DT_BROKER_MAX_SLIPPAGE", 0.03)
    if max_s < 0:
        max_s = 0.0
    if s > max_s:
        s = max_s
    return s


# -----------------------------------------------------------------------------
# Swap call adapter (signature drift tolerant)
# -----------------------------------------------------------------------------
async def _maybe_await(x: Any) -> Any:
    # supports sync swap functions too
    if hasattr(x, "__await__"):
        return await x
    return x


def _normalize_swap_return(res: Any) -> tuple[bool, str | None]:
    """
    Normalize a wide set of return shapes:
      - (ok, sig)
      - (sig, ok)
      - {"ok": bool, "sig"/"signature"/"txid": "..."}
      - {"signature": "..."} (assume ok)
      - True/False (no sig)
    """
    if isinstance(res, tuple) and res:
        a0 = res[0]
        a1 = res[1] if len(res) > 1 else None
        if isinstance(a0, bool):
            ok = bool(a0)
            sig = a1
            return ok, (str(sig) if sig else None)
        if isinstance(a1, bool):
            ok = bool(a1)
            sig = a0
            return ok, (str(sig) if sig else None)
        # fallback: treat first element truthiness as ok
        ok = bool(a0)
        sig = a1
        return ok, (str(sig) if sig else None)

    if isinstance(res, dict):
        ok = res.get("ok")
        sig = res.get("sig") or res.get("signature") or res.get("txid") or res.get("txSig")
        if ok is None and sig:
            ok = True
        return bool(ok), (str(sig) if sig else None)

    return bool(res), None


async def _call_swap(
    *,
    input_mint: str,
    output_mint: str,
    amount_in_float: float,
    buy: bool,
    slippage: float,
    context: dict[str, Any],
) -> tuple[bool, str | None]:
    """
    Calls your canonical swap bridge with compatibility across signatures.
    Never tries alternate engines.

    Expected return: (ok: bool, signature: Optional[str]) OR dict-like.
    """
    if _SWAP_FN is None:
        raise RuntimeError(f"[broker] swap_token_jupiter unavailable: {_SWAP_IMPORT_ERR}")

    fn = _SWAP_FN  # type: ignore[assignment]
    side = "buy" if buy else "sell"

    # Keep attempts conservative: only include extra context in later attempts.
    attempts: list[dict[str, Any]] = [
        {
            "input_mint": input_mint,
            "output_mint": output_mint,
            "amount_float": amount_in_float,
            "buy": buy,
            "slippage": slippage,
        },
        {
            "input_mint": input_mint,
            "output_mint": output_mint,
            "amount": amount_in_float,
            "buy": buy,
            "slippage": slippage,
        },
        {
            "input_mint": input_mint,
            "output_mint": output_mint,
            "amount_in": amount_in_float,
            "buy": buy,
            "slippage": slippage,
        },
        {
            "input_mint": input_mint,
            "output_mint": output_mint,
            "amount_float": amount_in_float,
            "side": side,
            "slippage": slippage,
        },
        {
            "in_mint": input_mint,
            "out_mint": output_mint,
            "amount_float": amount_in_float,
            "buy": buy,
            "slippage": slippage,
        },
        # Context-friendly attempts (only if swap fn accepts them)
        {
            "input_mint": input_mint,
            "output_mint": output_mint,
            "amount_float": amount_in_float,
            "side": side,
            "slippage": slippage,
            "reason": context.get("reason"),
            "order_id": context.get("order_id"),
            "pair": context.get("pair"),
        },
    ]

    last_err: Exception | None = None
    for kwargs in attempts:
        try:
            res = await _maybe_await(fn(**kwargs))  # type: ignore[misc]
            return _normalize_swap_return(res)
        except TypeError as err:
            last_err = err
            continue
        except Exception as err:
            last_err = err
            break

    if last_err is not None:
        raise last_err
    raise RuntimeError("[broker] swap call failed (unknown)")


# -----------------------------------------------------------------------------
# Public API
# -----------------------------------------------------------------------------
DEFAULT_SLIPPAGE = _env_float("DT_BROKER_DEFAULT_SLIPPAGE", 0.005)  # 0.5%


async def place_order(
    pair: str,
    side: str,  # "buy" or "sell"
    amount_input: float,  # exact-in, in INPUT token units
    slippage: float = DEFAULT_SLIPPAGE,
    reason: str = "daytrader",
) -> tuple[bool, str | None]:
    """
    Place a market-style Jupiter swap via your canonical core utils bridge.

    UNIT MODEL:
      For pair "BASE/QUOTE":
        - BUY:  input token = QUOTE. amount_input = QUOTE amount to spend
        - SELL: input token = BASE.  amount_input = BASE amount to dump
    """
    # hard kill-switch so Daytrader cannot silently compete with sniper
    if not _env_bool("DT_BROKER_ENABLED", False):
        return False, None

    allow_pairs = _parse_allow_pairs()
    pair_u = str(pair).strip().upper()
    if allow_pairs is not None and pair_u not in allow_pairs:
        _LOG.warning("[broker] pair blocked by DT_BROKER_ALLOW_PAIRS: %s", pair_u)
        return False, None

    side_l = str(side).strip().lower()
    if side_l not in ("buy", "sell"):
        _LOG.warning("[broker] invalid side=%r expected buy|sell", side)
        return False, None

    dry_run = _env_bool("DT_BROKER_DRY_RUN", False)
    min_amt = _env_float("DT_BROKER_MIN_AMOUNT", 0.0)

    # Parse pair/tokens
    try:
        info = _parse_pair(pair_u)
    except Exception as err:
        _LOG.warning("%s", err)
        return False, None

    # Determine exact-in token
    if side_l == "buy":
        in_tok, out_tok = info.quote, info.base
    else:
        in_tok, out_tok = info.base, info.quote

    # Validate/quantize amount in input token units
    try:
        amt_dec = _validate_amount(amount_input, in_tok.decimals, min_amt)
    except Exception as err:
        _LOG.warning("%s", err)
        return False, None

    slip = _clamp_slippage(slippage)

    # Trace id for debugging/telemetry correlation
    order_id = uuid.uuid4().hex[:10]
    unix_ts = int(time.time())

    _LOG.info(
        "[broker] order=%s t=%s side=%s pair=%s in=%s out=%s amt_in=%s slip=%s reason=%s",
        order_id,
        unix_ts,
        side_l.upper(),
        info.pair,
        in_tok.symbol,
        out_tok.symbol,
        str(amt_dec),
        slip,
        reason,
    )

    if dry_run:
        fake_sig = f"DRYRUN-{order_id}"
        _LOG.info("[broker] order=%s dry_run=1 -> ok sig=%s", order_id, fake_sig)
        return True, fake_sig

    # Convert to float for the canonical bridge (quantized first)
    amt_float = float(amt_dec)

    context = {"order_id": order_id, "pair": info.pair, "reason": reason}

    try:
        ok, sig = await _call_swap(
            input_mint=in_tok.mint,
            output_mint=out_tok.mint,
            amount_in_float=amt_float,
            buy=(side_l == "buy"),
            slippage=slip,
            context=context,
        )
    except Exception as err:
        _LOG.warning("[broker] order=%s swap exception: %s", order_id, err)
        return False, None

    if ok:
        _LOG.info("[broker] order=%s ✅ filled sig=%s", order_id, sig)
    else:
        _LOG.warning("[broker] order=%s ❌ failed (no fill)", order_id)
    return ok, sig


async def buy_base_with_quote(
    pair: str,
    quote_amount: float,
    slippage: float = DEFAULT_SLIPPAGE,
    reason: str = "daytrader",
) -> tuple[bool, str | None]:
    """BUY BASE/QUOTE spending quote_amount units of QUOTE (exact-in)."""
    return await place_order(pair, "buy", quote_amount, slippage=slippage, reason=reason)


async def sell_base_for_quote(
    pair: str,
    base_amount: float,
    slippage: float = DEFAULT_SLIPPAGE,
    reason: str = "daytrader",
) -> tuple[bool, str | None]:
    """SELL BASE/QUOTE dumping base_amount units of BASE (exact-in)."""
    return await place_order(pair, "sell", base_amount, slippage=slippage, reason=reason)


__all__ = [
    "PAIRS",
    "TOKENS",
    "buy_base_with_quote",
    "place_order",
    "sell_base_for_quote",
]
