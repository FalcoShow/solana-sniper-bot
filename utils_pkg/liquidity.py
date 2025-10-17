# utils_pkg/liquidity.py
from __future__ import annotations

import base64
import os
from typing import Any, Dict, Optional, Tuple

from utils_pkg.txlearn import _Rpc, RPC_URL, TX_RPC_URL
from utils_pkg.router import get_buy_quote

# ──────────────────────────────────────────────────────────────────────────────
# Heuristics / thresholds (tunable via env)
# ──────────────────────────────────────────────────────────────────────────────
MAX_PRICE_IMPACT_PCT = float(os.getenv("ENTRY_MAX_PRICE_IMPACT_PCT", "85"))     # reject if > this (percent)
PROBE_SOL            = float(os.getenv("ENTRY_LIQ_PROBE_SOL", "0.25"))          # quote buy size used for the probe
REQUIRE_NO_FREEZE    = (os.getenv("ENTRY_REQUIRE_NO_FREEZE", "0").strip().lower()
                        in {"1", "true", "yes", "y", "on"})
MIN_DECIMALS         = int(os.getenv("ENTRY_MIN_DECIMALS", "0"))                # e.g., 0..9

# Base58 alphabet (no 0,O,I,l). Solana mints are typically 32–44 chars.
_BASE58_ALPHABET = set("123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz")

def looks_like_mint(s: str) -> bool:
    s = (s or "").strip()
    return 32 <= len(s) <= 44 and all(ch in _BASE58_ALPHABET for ch in s)

# ──────────────────────────────────────────────────────────────────────────────
# RPC helpers
# ──────────────────────────────────────────────────────────────────────────────
_rpc = _Rpc(RPC_URL or TX_RPC_URL or "")

async def _get_account_jsonparsed(mint: str) -> Optional[Dict[str, Any]]:
    """
    Try to fetch a jsonParsed mint account. Many RPCs return:
      {"value":[{"data":{"program":"spl-token","parsed":{"type":"mint","info":{...}}}}]}
    Fallbacks handled elsewhere.
    """
    try:
        res = await _rpc.get_multiple_accounts([mint], encoding="jsonParsed")
        val = (res or {}).get("value") or []
        acc = (val[0] if val else None) or None
        return acc
    except Exception:
        return None

async def _get_account_raw(mint: str) -> Optional[Dict[str, Any]]:
    """
    Fallback to raw/base64 layout if jsonParsed isn't available.
    """
    try:
        res = await _rpc.get_multiple_accounts([mint], encoding="base64")
        val = (res or {}).get("value") or []
        acc = (val[0] if val else None) or None
        return acc
    except Exception:
        return None

# SPL Mint: decimals at byte offset 44. The presence of freeze authority is not
# directly available from raw bytes without full layout parsing; we only assert
# 'no-freeze' when jsonParsed exposes it.
def _decimals_from_raw_b64(acc: Dict[str, Any]) -> Optional[int]:
    try:
        data = acc.get("data")
        if isinstance(data, list) and data:
            raw_b64 = data[0]
            buf = base64.b64decode(raw_b64)
            if len(buf) >= 45:
                return int(buf[44])
    except Exception:
        pass
    return None

def _parse_mint_info_from_parsed(acc: Dict[str, Any]) -> Tuple[int, Optional[str], Optional[str]]:
    """
    Return (decimals, mintAuthority, freezeAuthority) when jsonParsed is present.
    """
    try:
        data = acc.get("data", {})
        if isinstance(data, dict):
            parsed = data.get("parsed") or {}
            info = parsed.get("info") or {}
            dec = int(info.get("decimals", 0))
            return dec, info.get("mintAuthority"), info.get("freezeAuthority")
    except Exception:
        pass
    return 0, None, None

async def fetch_mint_account(mint: str) -> Optional[Dict[str, Any]]:
    """
    Unified fetch:
      1) Try jsonParsed (preferred)
      2) Else try base64
    """
    acc = await _get_account_jsonparsed(mint)
    if acc:
        return acc
    return await _get_account_raw(mint)

# ──────────────────────────────────────────────────────────────────────────────
# Main screen
# ──────────────────────────────────────────────────────────────────────────────
async def screen_token(mint: str) -> Tuple[bool, Dict[str, Any]]:
    """
    Quick liquidity & safety screen:
      • Confirm account exists; parse decimals (from jsonParsed or raw bytes)
      • Optional: reject if freezeAuthority present (REQUIRE_NO_FREEZE)
      • Jupiter buy quote with PROBE_SOL; require some route + acceptable price impact
    Returns (ok, report). 'report' contains: decimals, priceImpactPct, route_ok, reason (on reject).
    """
    report: Dict[str, Any] = {"mint": mint, "ok": False}

    if not looks_like_mint(mint):
        report["reason"] = "not_mint"
        return False, report

    acc = await fetch_mint_account(mint)
    if not acc:
        report["reason"] = "no_account"
        return False, report

    # Parse jsonParsed first (more complete), else raw/base64 for decimals
    dec, mint_auth, freeze_auth = _parse_mint_info_from_parsed(acc)
    if dec == 0 and (mint_auth is None and freeze_auth is None):
        # Try raw fallback for decimals only
        raw_dec = _decimals_from_raw_b64(acc)
        if raw_dec is not None:
            dec = int(raw_dec)

    report["decimals"] = dec
    if mint_auth is not None:
        report["mintAuthority"] = mint_auth
    if freeze_auth is not None:
        report["freezeAuthority"] = freeze_auth

    if dec < MIN_DECIMALS:
        report["reason"] = "low_decimals"
        return False, report

    if REQUIRE_NO_FREEZE and (freeze_auth is not None):
        report["reason"] = "freeze_authority_present"
        return False, report

    # Price/route probe via router (uses Jupiter v6)
    try:
        q = await get_buy_quote(mint=mint, amount_sol=PROBE_SOL)
    except Exception as e:
        report["reason"] = "quote_fail"
        report["error"] = str(e)
        return False, report

    # v6 returns priceImpactPct as a fraction (e.g., 0.13 == 13%)
    # Some gateways may already return percent; handle both safely.
    raw_pi = q.get("priceImpactPct", 0.0)
    try:
        price_impact_pct = float(raw_pi)
        if price_impact_pct <= 1.0:  # treat as fraction → convert to percent
            price_impact_pct *= 100.0
    except Exception:
        price_impact_pct = 0.0

    out_raw = int(str(q.get("outAmount", "0")) or "0")
    route_ok = out_raw > 0

    report["priceImpactPct"] = price_impact_pct
    report["route_ok"] = bool(route_ok)

    if (not route_ok) or (price_impact_pct > MAX_PRICE_IMPACT_PCT):
        report["reason"] = "route_or_impact"
        return False, report

    report["ok"] = True
    return True, report

__all__ = ["looks_like_mint", "fetch_mint_account", "screen_token"]
