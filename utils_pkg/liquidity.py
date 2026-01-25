from __future__ import annotations

"""
utils_pkg/liquidity.py

Canonical liquidity + impact + mint-info utilities (V3+++ / production).
======================================================================

Purpose
-------
Single source of truth for:
  - Mint validation (base58 pubkey shape)
  - Mint account fetch + parsing (jsonParsed preferred, raw/base64 fallback)
  - SPL Mint decoding (decimals + mint authority + freeze authority from raw bytes)
  - Quote probing via router (Jupiter) to estimate:
        * route viability
        * price impact (normalized)
        * derived impact_bps for FeatureBuilder / pair_filters
  - Fast "screen_token" gate for entry path safety

Design goals
------------
- Deterministic and robust to missing/dirty RPC responses.
- Safe timeouts (never hang your scanners).
- Minimal dependencies, works in Render and local.
- Backwards compatible exports:
      looks_like_mint
      fetch_mint_account
      screen_token

Notes
-----
This module does NOT decide whether to buy; it only provides primitives.
Upstream components (pair_filters / ai_entry_logic / risk_manager) make decisions.
"""

import asyncio
import base64
import os
import struct
import time
from dataclasses import dataclass
from typing import Any

from logging_setup import get_logger

log = get_logger()

# =============================================================================
# Config / env (keep env overrides for quick tuning, but prefer config if present)
# =============================================================================

try:
    import config as C  # type: ignore
except Exception:  # pragma: no cover

    class _C:
        # Quote probe sizing
        ENTRY_LIQ_PROBE_SOL = float(os.getenv("ENTRY_LIQ_PROBE_SOL", "0.25"))
        # Impact caps (percent)
        ENTRY_MAX_PRICE_IMPACT_PCT = float(os.getenv("ENTRY_MAX_PRICE_IMPACT_PCT", "85"))
        # Require no freeze authority (security gate)
        ENTRY_REQUIRE_NO_FREEZE = int(os.getenv("ENTRY_REQUIRE_NO_FREEZE", "0"))
        # Require minimum decimals (avoid weird mints)
        ENTRY_MIN_DECIMALS = int(os.getenv("ENTRY_MIN_DECIMALS", "0"))

        # Timeouts (seconds)
        LIQ_RPC_TIMEOUT_S = float(os.getenv("LIQ_RPC_TIMEOUT_S", "6.0"))
        LIQ_QUOTE_TIMEOUT_S = float(os.getenv("LIQ_QUOTE_TIMEOUT_S", "6.0"))

        # Cache TTL (seconds)
        LIQ_MINT_CACHE_TTL_S = float(os.getenv("LIQ_MINT_CACHE_TTL_S", "300"))

        # Optional: treat missing freeze data as unsafe if REQUIRE_NO_FREEZE
        LIQ_STRICT_FREEZE_CHECK = int(os.getenv("LIQ_STRICT_FREEZE_CHECK", "0"))

    C = _C()  # type: ignore

# =============================================================================
# Imports that depend on your existing stack
# =============================================================================

# txlearn provides your RPC wrapper and RPC_URL constants
try:
    from utils_pkg.txlearn import RPC_URL, TX_RPC_URL, _Rpc  # type: ignore
except Exception:  # pragma: no cover
    _Rpc = None  # type: ignore
    RPC_URL = os.getenv("RPC_URL", "")  # type: ignore
    TX_RPC_URL = os.getenv("TX_RPC_URL", "")  # type: ignore

# Router quote function (canonical)
try:
    from utils_pkg.router import get_buy_quote  # type: ignore
except Exception as e:  # pragma: no cover
    get_buy_quote = None  # type: ignore
    log.warning(f"utils_pkg.liquidity: router.get_buy_quote import failed: {e}")

# =============================================================================
# Constants / helpers
# =============================================================================

# Base58 alphabet (no 0,O,I,l). Solana pubkeys are base58 and typically 32–44 chars.
_BASE58_ALPHABET = set("123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz")

# SPL Token Mint layout (spl-token program):
#   mint_authority_option: u32 (0/1)
#   mint_authority: [u8;32] (present if option=1)
#   supply: u64
#   decimals: u8
#   is_initialized: u8
#   freeze_authority_option: u32 (0/1)
#   freeze_authority: [u8;32] (present if option=1)
#
# Total length = 82 bytes
_SPL_MINT_MIN_LEN = 82


def looks_like_mint(s: str) -> bool:
    s = (s or "").strip()
    return 32 <= len(s) <= 44 and all(ch in _BASE58_ALPHABET for ch in s)


def _env_bool(name: str, default: str = "0") -> bool:
    return os.getenv(name, default).strip().lower() in {"1", "true", "yes", "y", "on"}


def _safe_float(x: Any, default: float = 0.0) -> float:
    try:
        f = float(x)
        if f != f or f == float("inf") or f == float("-inf"):
            return default
        return f
    except Exception:
        return default


def _normalize_price_impact_pct(raw_pi: Any) -> float:
    """
    Normalize router/Jupiter priceImpactPct into a true percentage (0..100+).

    Handles:
      - fraction form: 0.13 == 13%
      - percent form: 13.0 == 13%
      - string form
    """
    pi = _safe_float(raw_pi, 0.0)
    if pi <= 1.0:
        # Treat <=1 as fraction (common in v6 responses)
        pi *= 100.0
    if pi < 0:
        pi = 0.0
    return pi


def _impact_bps_from_price_impact_pct(pi_pct: float) -> float:
    """
    Convert percent impact -> basis points.
      1% = 100 bps
    """
    return max(0.0, float(pi_pct) * 100.0)


# =============================================================================
# RPC client (single instance)
# =============================================================================

_RPC_URL = (
    getattr(C, "RPC_URL", "")
    or RPC_URL
    or TX_RPC_URL
    or os.getenv("RPC_URL", "")
    or os.getenv("TX_RPC_URL", "")
).strip()

_rpc = _Rpc(_RPC_URL) if _Rpc is not None and _RPC_URL else None


# =============================================================================
# Cache (mint info)
# =============================================================================


@dataclass
class MintInfo:
    mint: str
    decimals: int | None
    mint_authority_present: bool | None
    freeze_authority_present: bool | None
    # raw parsed fields (if known)
    mint_authority: str | None = None
    freeze_authority: str | None = None
    source: str = "unknown"  # "jsonParsed" | "raw" | "unknown"


# mint -> (expires_at, MintInfo)
_MINT_CACHE: dict[str, tuple[float, MintInfo]] = {}
_MINT_CACHE_LOCK = asyncio.Lock()


async def _cache_get(mint: str) -> MintInfo | None:
    ttl = float(getattr(C, "LIQ_MINT_CACHE_TTL_S", 300.0))
    if ttl <= 0:
        return None
    async with _MINT_CACHE_LOCK:
        row = _MINT_CACHE.get(mint)
        if not row:
            return None
        exp, info = row
        if time.time() >= exp:
            _MINT_CACHE.pop(mint, None)
            return None
        return info


async def _cache_set(mint: str, info: MintInfo) -> None:
    ttl = float(getattr(C, "LIQ_MINT_CACHE_TTL_S", 300.0))
    if ttl <= 0:
        return
    async with _MINT_CACHE_LOCK:
        _MINT_CACHE[mint] = (time.time() + ttl, info)


# =============================================================================
# RPC fetch helpers
# =============================================================================


async def _get_multiple_accounts(mints: list[str], encoding: str) -> dict[str, Any] | None:
    if _rpc is None:
        return None
    timeout_s = float(getattr(C, "LIQ_RPC_TIMEOUT_S", 6.0))
    try:
        return await asyncio.wait_for(
            _rpc.get_multiple_accounts(mints, encoding=encoding), timeout=timeout_s
        )
    except Exception:
        return None


async def _get_account_jsonparsed(mint: str) -> dict[str, Any] | None:
    """
    Fetch mint account in jsonParsed format if RPC supports it.
    Expected shape:
      {"value":[{"data":{"program":"spl-token","parsed":{"type":"mint","info":{...}}}}]}
    """
    res = await _get_multiple_accounts([mint], encoding="jsonParsed")
    if not res:
        return None
    val = res.get("value") or []
    return (val[0] if val else None) or None


async def _get_account_raw(mint: str) -> dict[str, Any] | None:
    """
    Fetch mint account in base64 format (raw bytes) for deterministic parsing.
    Expected shape:
      {"value":[{"data":["<base64>","base64"], ...}]}
    """
    res = await _get_multiple_accounts([mint], encoding="base64")
    if not res:
        return None
    val = res.get("value") or []
    return (val[0] if val else None) or None


async def fetch_mint_account(mint: str) -> dict[str, Any] | None:
    """
    Backwards compatible "unified fetch":
      1) jsonParsed (preferred)
      2) raw base64 (fallback)
    """
    acc = await _get_account_jsonparsed(mint)
    if acc:
        return acc
    return await _get_account_raw(mint)


# =============================================================================
# Parsing helpers
# =============================================================================


def _parse_mint_info_from_parsed(acc: dict[str, Any]) -> tuple[int | None, str | None, str | None]:
    """
    Return (decimals, mintAuthority, freezeAuthority) when jsonParsed is present.
    """
    try:
        data = acc.get("data")
        if isinstance(data, dict):
            parsed = data.get("parsed") or {}
            info = parsed.get("info") or {}
            dec = info.get("decimals", None)
            decimals = int(dec) if dec is not None else None
            mint_auth = info.get("mintAuthority")
            freeze_auth = info.get("freezeAuthority")
            return decimals, mint_auth, freeze_auth
    except Exception:
        pass
    return None, None, None


def _raw_bytes_from_acc(acc: dict[str, Any]) -> bytes | None:
    try:
        data = acc.get("data")
        # common shape: ["<b64>","base64"]
        if isinstance(data, list) and data:
            raw_b64 = data[0]
            return base64.b64decode(raw_b64)
        # sometimes RPCs nest differently; be defensive
        if isinstance(data, dict) and "data" in data:
            inner = data.get("data")
            if isinstance(inner, list) and inner:
                return base64.b64decode(inner[0])
    except Exception:
        return None
    return None


def _parse_spl_mint_from_raw(buf: bytes) -> tuple[int | None, bool | None, bool | None]:
    """
    Parse SPL mint from raw bytes.
    Returns: (decimals, mint_auth_present, freeze_auth_present)
    """
    if not buf or len(buf) < _SPL_MINT_MIN_LEN:
        return None, None, None

    try:
        # u32 little-endian
        mint_auth_opt = struct.unpack_from("<I", buf, 0)[0]
        # mint auth pubkey at offset 4 (32 bytes) if opt=1
        # supply at offset 36 (u64)
        # decimals at offset 44 (u8)
        decimals = int(buf[44])

        freeze_auth_opt = struct.unpack_from("<I", buf, 46)[0]

        mint_present = mint_auth_opt == 1
        freeze_present = freeze_auth_opt == 1

        return decimals, mint_present, freeze_present
    except Exception:
        return None, None, None


# =============================================================================
# Quote probe helpers
# =============================================================================


@dataclass
class QuoteProbe:
    ok: bool
    out_amount_raw: int
    price_impact_pct: float
    impact_bps: float
    raw_quote: dict[str, Any]
    reason: str | None = None


async def probe_buy_quote(mint: str, amount_sol: float) -> QuoteProbe:
    """
    Probe router/Jupiter for a buy quote of amount_sol SOL into `mint`.
    Used to estimate:
      - route viability (outAmount>0)
      - price impact pct (normalized)
      - derived impact_bps

    Never raises; returns QuoteProbe with ok=False on failure.
    """
    if get_buy_quote is None:
        return QuoteProbe(
            ok=False,
            out_amount_raw=0,
            price_impact_pct=0.0,
            impact_bps=0.0,
            raw_quote={},
            reason="router_unavailable",
        )

    timeout_s = float(getattr(C, "LIQ_QUOTE_TIMEOUT_S", 6.0))
    try:
        q = await asyncio.wait_for(
            get_buy_quote(mint=mint, amount_sol=float(amount_sol)), timeout=timeout_s
        )
        if not isinstance(q, dict):
            return QuoteProbe(
                ok=False,
                out_amount_raw=0,
                price_impact_pct=0.0,
                impact_bps=0.0,
                raw_quote={},
                reason="quote_not_dict",
            )
    except Exception as e:
        return QuoteProbe(
            ok=False,
            out_amount_raw=0,
            price_impact_pct=0.0,
            impact_bps=0.0,
            raw_quote={},
            reason=f"quote_fail:{e}",
        )

    out_raw = 0
    try:
        out_raw = int(str(q.get("outAmount", "0")) or "0")
    except Exception:
        out_raw = 0

    pi_pct = _normalize_price_impact_pct(q.get("priceImpactPct", 0.0))
    impact_bps = _impact_bps_from_price_impact_pct(pi_pct)

    route_ok = out_raw > 0
    return QuoteProbe(
        ok=route_ok,
        out_amount_raw=out_raw,
        price_impact_pct=pi_pct,
        impact_bps=impact_bps,
        raw_quote=q,
        reason=None if route_ok else "no_route",
    )


# =============================================================================
# High-level mint info fetch (cached)
# =============================================================================


async def fetch_mint_info(mint: str) -> MintInfo | None:
    """
    Fetch and parse mint info:
      - jsonParsed first (best)
      - raw base64 fallback (deterministic)

    Uses TTL cache.
    """
    if not looks_like_mint(mint):
        return None

    cached = await _cache_get(mint)
    if cached is not None:
        return cached

    acc = await fetch_mint_account(mint)
    if not acc:
        return None

    # Try jsonParsed parse
    decimals, mint_auth, freeze_auth = _parse_mint_info_from_parsed(acc)
    if decimals is not None:
        info = MintInfo(
            mint=mint,
            decimals=decimals,
            mint_authority_present=(mint_auth is not None),
            freeze_authority_present=(freeze_auth is not None),
            mint_authority=mint_auth,
            freeze_authority=freeze_auth,
            source="jsonParsed",
        )
        await _cache_set(mint, info)
        return info

    # Raw fallback
    buf = _raw_bytes_from_acc(acc)
    if buf:
        dec2, mint_present, freeze_present = _parse_spl_mint_from_raw(buf)
        info = MintInfo(
            mint=mint,
            decimals=dec2,
            mint_authority_present=mint_present,
            freeze_authority_present=freeze_present,
            mint_authority=None,
            freeze_authority=None,
            source="raw",
        )
        await _cache_set(mint, info)
        return info

    return None


# =============================================================================
# Main screen (public)
# =============================================================================


async def screen_token(mint: str) -> tuple[bool, dict[str, Any]]:
    """
    Quick liquidity + safety screen.

    Gates:
      • valid mint-ish string
      • mint account exists, parse decimals (jsonParsed or raw)
      • optional: require NO freeze authority (ENTRY_REQUIRE_NO_FREEZE)
      • router buy quote with probe size (ENTRY_LIQ_PROBE_SOL)
      • require route_ok and impact <= ENTRY_MAX_PRICE_IMPACT_PCT

    Returns:
        (ok, report)

    report fields:
      mint, ok, reason (if rejected),
      decimals, mintAuthorityPresent, freezeAuthorityPresent, source,
      probe_sol, priceImpactPct, impactBps, route_ok
    """
    report: dict[str, Any] = {"mint": mint, "ok": False}

    if not looks_like_mint(mint):
        report["reason"] = "not_mint"
        return False, report

    min_decimals = int(getattr(C, "ENTRY_MIN_DECIMALS", int(os.getenv("ENTRY_MIN_DECIMALS", "0"))))
    require_no_freeze = bool(
        int(getattr(C, "ENTRY_REQUIRE_NO_FREEZE", int(_env_bool("ENTRY_REQUIRE_NO_FREEZE", "0"))))
    )
    strict_freeze = bool(int(getattr(C, "LIQ_STRICT_FREEZE_CHECK", 0)))

    info = await fetch_mint_info(mint)
    if info is None:
        report["reason"] = "no_account_or_parse"
        return False, report

    report["decimals"] = info.decimals
    report["mintAuthorityPresent"] = info.mint_authority_present
    report["freezeAuthorityPresent"] = info.freeze_authority_present
    report["source"] = info.source

    # Decimals gate
    if info.decimals is None:
        report["reason"] = "no_decimals"
        return False, report
    if int(info.decimals) < min_decimals:
        report["reason"] = "low_decimals"
        return False, report

    # Freeze gate
    if require_no_freeze:
        if info.freeze_authority_present is True:
            report["reason"] = "freeze_authority_present"
            return False, report
        if strict_freeze and info.freeze_authority_present is None:
            report["reason"] = "freeze_unknown_strict"
            return False, report

    # Quote probe
    probe_sol = float(
        getattr(C, "ENTRY_LIQ_PROBE_SOL", float(os.getenv("ENTRY_LIQ_PROBE_SOL", "0.25")))
    )
    max_pi_pct = float(
        getattr(
            C, "ENTRY_MAX_PRICE_IMPACT_PCT", float(os.getenv("ENTRY_MAX_PRICE_IMPACT_PCT", "85"))
        )
    )

    report["probe_sol"] = probe_sol

    qp = await probe_buy_quote(mint, amount_sol=probe_sol)
    report["route_ok"] = bool(qp.ok)
    report["priceImpactPct"] = float(qp.price_impact_pct)
    report["impactBps"] = float(qp.impact_bps)

    if not qp.ok:
        report["reason"] = qp.reason or "no_route"
        return False, report

    if qp.price_impact_pct > max_pi_pct:
        report["reason"] = "impact_too_high"
        return False, report

    report["ok"] = True
    return True, report


# =============================================================================
# Backwards-compatible exports
# =============================================================================

__all__ = [
    "MintInfo",
    "QuoteProbe",
    "fetch_mint_account",
    "fetch_mint_info",
    "looks_like_mint",
    "probe_buy_quote",
    "screen_token",
]
