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
- Import-time safety: importing this module must not touch the network or DB.
- Deterministic and robust to missing/dirty RPC responses.
- Safe timeouts (never hang scanners).
- Minimal dependencies; works in Render and local.
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
import contextlib
import os
import struct
import time
from dataclasses import dataclass
from typing import Any, Callable, Coroutine, Final

# ──────────────────────────────────────────────────────────────────────────────
# Logger (safe import; no hard dependency)
# ──────────────────────────────────────────────────────────────────────────────
try:  # pragma: no cover
    from utils_pkg.log_compat import get_logger  # type: ignore
except Exception:  # pragma: no cover
    try:
        from logging_setup import get_logger  # type: ignore
    except Exception:  # pragma: no cover
        import logging

        def get_logger(name: str | None = None):  # type: ignore
            return logging.getLogger(name or "liquidity")


log = get_logger(__name__)

# ──────────────────────────────────────────────────────────────────────────────
# Config (prefer config module, fallback to env-only shim)
# ──────────────────────────────────────────────────────────────────────────────
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

# ──────────────────────────────────────────────────────────────────────────────
# Constants / helpers
# ──────────────────────────────────────────────────────────────────────────────
_BASE58_ALPHABET: Final[set[str]] = set(
    "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
)

# SPL Token Mint layout length = 82 bytes
_SPL_MINT_MIN_LEN: Final[int] = 82


def looks_like_mint(s: str) -> bool:
    """
    Solana pubkeys are base58 and typically 32–44 chars.
    This is a fast shape check; it does not confirm on-chain existence.
    """
    s = (s or "").strip()
    return 32 <= len(s) <= 44 and all(ch in _BASE58_ALPHABET for ch in s)


def _env_bool(name: str, default: str = "0") -> bool:
    return os.getenv(name, default).strip().lower() in {"1", "true", "yes", "y", "on"}


def _safe_float(x: Any, default: float = 0.0) -> float:
    try:
        f = float(x)
        if f != f or f in (float("inf"), float("-inf")):
            return default
        return f
    except Exception:
        return default


def _safe_int(x: Any, default: int = 0) -> int:
    try:
        return int(float(x))
    except Exception:
        return default


def _now_ts() -> int:
    return int(time.time())


def _log_event(name: str, **fields: Any) -> None:
    """
    Support both custom logger.event(...) and standard logger.info(...).
    Never crash from logging.
    """
    try:
        if hasattr(log, "event") and callable(getattr(log, "event", None)):
            log.event(name, **fields)  # type: ignore[attr-defined]
        else:
            log.info(name, extra=fields)
    except Exception:
        return


def _normalize_price_impact_pct(raw_pi: Any) -> float:
    """
    Normalize router/Jupiter priceImpactPct into a true percentage (0..100+).

    Handles:
      - fraction form: 0.13 == 13%
      - percent form: 13.0 == 13%
      - string form
    """
    pi = _safe_float(raw_pi, 0.0)
    if 0.0 <= pi <= 1.0:
        # Treat <=1 as fraction (common in v6 responses)
        pi *= 100.0
    if pi < 0.0:
        pi = 0.0
    return float(pi)


def _impact_bps_from_price_impact_pct(pi_pct: float) -> float:
    """
    Convert percent impact -> basis points.
      1% = 100 bps
    """
    return max(0.0, float(pi_pct) * 100.0)


# ──────────────────────────────────────────────────────────────────────────────
# Lazy imports (RPC + router) — DO NOT initialize network clients at import-time
# ──────────────────────────────────────────────────────────────────────────────
_RPC_SINGLETON: Any | None = None
_RPC_LOCK = asyncio.Lock()


def _pick_rpc_url() -> str:
    # Prefer config if present; fall back to env and txlearn constants.
    with contextlib.suppress(Exception):
        v = str(getattr(C, "RPC_URL", "") or "").strip()
        if v:
            return v
    with contextlib.suppress(Exception):
        v = str(getattr(C, "TX_RPC_URL", "") or "").strip()
        if v:
            return v

    # Best-effort import from txlearn without breaking import-time safety
    with contextlib.suppress(Exception):
        from utils_pkg.txlearn import RPC_URL as _RPC_URL  # type: ignore

        v = str(_RPC_URL or "").strip()
        if v:
            return v
    with contextlib.suppress(Exception):
        from utils_pkg.txlearn import TX_RPC_URL as _TX_RPC_URL  # type: ignore

        v = str(_TX_RPC_URL or "").strip()
        if v:
            return v

    return (os.getenv("RPC_URL", "") or os.getenv("TX_RPC_URL", "") or "").strip()


async def _get_rpc() -> Any | None:
    """
    Lazily create the RPC wrapper instance. Returns None if unavailable.

    NOTE: Use a local snapshot to avoid mypy incorrectly treating the second
    double-check as unreachable across an 'await' boundary.
    """
    global _RPC_SINGLETON

    rpc = _RPC_SINGLETON
    if rpc is not None:
        return rpc

    async with _RPC_LOCK:
        rpc = _RPC_SINGLETON  # re-read after awaiting the lock
        if rpc is not None:
            return rpc

        rpc_url = _pick_rpc_url()
        if not rpc_url:
            return None

        try:
            from utils_pkg.txlearn import _Rpc  # type: ignore
        except Exception:
            return None

        try:
            rpc = _Rpc(rpc_url)  # type: ignore[call-arg]
        except Exception:
            return None

        _RPC_SINGLETON = rpc
        return rpc


def _get_buy_quote_fn() -> Callable[..., Coroutine[Any, Any, Any]] | None:
    """
    Lazily import router.get_buy_quote.
    """
    with contextlib.suppress(Exception):
        from utils_pkg.router import get_buy_quote  # type: ignore

        return get_buy_quote
    return None


# ──────────────────────────────────────────────────────────────────────────────
# Cache (mint info)
# ──────────────────────────────────────────────────────────────────────────────
@dataclass(frozen=True)
class MintInfo:
    mint: str
    decimals: int | None
    mint_authority_present: bool | None
    freeze_authority_present: bool | None
    mint_authority: str | None = None
    freeze_authority: str | None = None
    source: str = "unknown"  # "jsonParsed" | "raw" | "unknown"


_MINT_CACHE: dict[str, tuple[float, MintInfo]] = {}
_MINT_CACHE_LOCK = asyncio.Lock()


def _cache_ttl_s() -> float:
    try:
        return float(getattr(C, "LIQ_MINT_CACHE_TTL_S", 300.0))
    except Exception:
        return 300.0


async def _cache_get(mint: str) -> MintInfo | None:
    ttl = _cache_ttl_s()
    if ttl <= 0:
        return None

    async with _MINT_CACHE_LOCK:
        row = _MINT_CACHE.get(mint)
        if not row:
            return None
        exp_ts, info = row
        if time.time() >= exp_ts:
            _MINT_CACHE.pop(mint, None)
            return None
        return info


async def _cache_set(mint: str, info: MintInfo) -> None:
    ttl = _cache_ttl_s()
    if ttl <= 0:
        return
    async with _MINT_CACHE_LOCK:
        _MINT_CACHE[mint] = (time.time() + ttl, info)


# ──────────────────────────────────────────────────────────────────────────────
# RPC fetch helpers
# ──────────────────────────────────────────────────────────────────────────────
def _rpc_timeout_s() -> float:
    try:
        return float(getattr(C, "LIQ_RPC_TIMEOUT_S", 6.0))
    except Exception:
        return 6.0


def _quote_timeout_s() -> float:
    try:
        return float(getattr(C, "LIQ_QUOTE_TIMEOUT_S", 6.0))
    except Exception:
        return 6.0


def _extract_value_list(res: Any) -> list[Any]:
    """
    Support a few common RPC wrapper shapes:
      - {"value":[...]}
      - {"result":{"value":[...]}}
      - {"result": {"value": [...]}, "error": ...}
    """
    if not isinstance(res, dict):
        return []
    if "value" in res and isinstance(res.get("value"), list):
        return res["value"]
    result = res.get("result")
    if isinstance(result, dict) and isinstance(result.get("value"), list):
        return result["value"]
    return []


async def _get_multiple_accounts(mints: list[str], *, encoding: str) -> dict[str, Any] | None:
    rpc = await _get_rpc()
    if rpc is None:
        return None

    timeout_s = _rpc_timeout_s()
    try:
        res = await asyncio.wait_for(
            rpc.get_multiple_accounts(mints, encoding=encoding),  # type: ignore[attr-defined]
            timeout=timeout_s,
        )
        if isinstance(res, dict):
            return res
        return None
    except Exception:
        return None


async def _get_account_jsonparsed(mint: str) -> dict[str, Any] | None:
    """
    Fetch mint account in jsonParsed format if RPC supports it.
    """
    res = await _get_multiple_accounts([mint], encoding="jsonParsed")
    if not res:
        return None
    vals = _extract_value_list(res)
    acc = (vals[0] if vals else None) or None
    return acc if isinstance(acc, dict) else None


async def _get_account_raw(mint: str) -> dict[str, Any] | None:
    """
    Fetch mint account in base64 format (raw bytes) for deterministic parsing.
    """
    res = await _get_multiple_accounts([mint], encoding="base64")
    if not res:
        return None
    vals = _extract_value_list(res)
    acc = (vals[0] if vals else None) or None
    return acc if isinstance(acc, dict) else None


async def fetch_mint_account(mint: str) -> dict[str, Any] | None:
    """
    Backwards compatible "unified fetch":
      1) jsonParsed (preferred)
      2) raw base64 (fallback)
    """
    if not looks_like_mint(mint):
        return None

    acc = await _get_account_jsonparsed(mint)
    if acc:
        return acc
    return await _get_account_raw(mint)


# ──────────────────────────────────────────────────────────────────────────────
# Parsing helpers
# ──────────────────────────────────────────────────────────────────────────────
def _parse_mint_info_from_parsed(acc: dict[str, Any]) -> tuple[int | None, str | None, str | None]:
    """
    Return (decimals, mintAuthority, freezeAuthority) when jsonParsed is present.
    Accepts a few variant shapes.
    """
    try:
        data = acc.get("data")
        if isinstance(data, dict):
            parsed = data.get("parsed")
            if isinstance(parsed, dict):
                info = parsed.get("info")
                if isinstance(info, dict):
                    dec = info.get("decimals", None)
                    decimals = int(dec) if dec is not None else None
                    mint_auth = info.get("mintAuthority")
                    freeze_auth = info.get("freezeAuthority")
                    return (
                        decimals,
                        (mint_auth if isinstance(mint_auth, str) else None),
                        (freeze_auth if isinstance(freeze_auth, str) else None),
                    )
    except Exception:
        pass
    return None, None, None


def _raw_bytes_from_acc(acc: dict[str, Any]) -> bytes | None:
    """
    Extract base64 bytes from account response.
    Typical shape:
      {"data":["<b64>","base64"], ...}
    """
    try:
        data = acc.get("data")
        if isinstance(data, list) and data:
            raw_b64 = data[0]
            if isinstance(raw_b64, str) and raw_b64:
                return base64.b64decode(raw_b64)
        if isinstance(data, dict):
            inner = data.get("data")
            if isinstance(inner, list) and inner:
                raw_b64 = inner[0]
                if isinstance(raw_b64, str) and raw_b64:
                    return base64.b64decode(raw_b64)
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
        mint_auth_opt = struct.unpack_from("<I", buf, 0)[0]
        decimals = int(buf[44])
        freeze_auth_opt = struct.unpack_from("<I", buf, 46)[0]
        return decimals, (mint_auth_opt == 1), (freeze_auth_opt == 1)
    except Exception:
        return None, None, None


# ──────────────────────────────────────────────────────────────────────────────
# Quote probe helpers
# ──────────────────────────────────────────────────────────────────────────────
@dataclass(frozen=True)
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

    Never raises; returns QuoteProbe(ok=False, reason=...) on failure.
    """
    if not looks_like_mint(mint):
        return QuoteProbe(
            ok=False,
            out_amount_raw=0,
            price_impact_pct=0.0,
            impact_bps=0.0,
            raw_quote={},
            reason="not_mint",
        )

    fn = _get_buy_quote_fn()
    if fn is None:
        return QuoteProbe(
            ok=False,
            out_amount_raw=0,
            price_impact_pct=0.0,
            impact_bps=0.0,
            raw_quote={},
            reason="router_unavailable",
        )

    timeout_s = _quote_timeout_s()
    try:
        q = await asyncio.wait_for(fn(mint=mint, amount_sol=float(amount_sol)), timeout=timeout_s)
    except Exception as e:
        return QuoteProbe(
            ok=False,
            out_amount_raw=0,
            price_impact_pct=0.0,
            impact_bps=0.0,
            raw_quote={},
            reason=f"quote_fail:{e}",
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

    out_raw = _safe_int(q.get("outAmount", 0), 0)
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


# ──────────────────────────────────────────────────────────────────────────────
# High-level mint info fetch (cached)
# ──────────────────────────────────────────────────────────────────────────────
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

    # Try jsonParsed
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


# ──────────────────────────────────────────────────────────────────────────────
# Main screen (public)
# ──────────────────────────────────────────────────────────────────────────────
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

    min_decimals = _safe_int(
        getattr(C, "ENTRY_MIN_DECIMALS", os.getenv("ENTRY_MIN_DECIMALS", "0")), 0
    )
    require_no_freeze = bool(
        _safe_int(
            getattr(C, "ENTRY_REQUIRE_NO_FREEZE", os.getenv("ENTRY_REQUIRE_NO_FREEZE", "0")), 0
        )
    )
    strict_freeze = bool(
        _safe_int(
            getattr(C, "LIQ_STRICT_FREEZE_CHECK", os.getenv("LIQ_STRICT_FREEZE_CHECK", "0")), 0
        )
    )

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
    if int(info.decimals) < int(min_decimals):
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
    probe_sol = _safe_float(
        getattr(C, "ENTRY_LIQ_PROBE_SOL", os.getenv("ENTRY_LIQ_PROBE_SOL", "0.25")), 0.25
    )
    max_pi_pct = _safe_float(
        getattr(C, "ENTRY_MAX_PRICE_IMPACT_PCT", os.getenv("ENTRY_MAX_PRICE_IMPACT_PCT", "85")),
        85.0,
    )

    report["probe_sol"] = float(probe_sol)

    qp = await probe_buy_quote(mint, amount_sol=float(probe_sol))
    report["route_ok"] = bool(qp.ok)
    report["priceImpactPct"] = float(qp.price_impact_pct)
    report["impactBps"] = float(qp.impact_bps)

    if not qp.ok:
        report["reason"] = qp.reason or "no_route"
        return False, report

    if qp.price_impact_pct > float(max_pi_pct):
        report["reason"] = "impact_too_high"
        return False, report

    report["ok"] = True
    return True, report


# ──────────────────────────────────────────────────────────────────────────────
# Backwards-compatible exports
# ──────────────────────────────────────────────────────────────────────────────
__all__ = [
    "MintInfo",
    "QuoteProbe",
    "fetch_mint_account",
    "fetch_mint_info",
    "looks_like_mint",
    "probe_buy_quote",
    "screen_token",
]
