# utils_pkg/honeypot.py
# Comprehensive honeypot / risk checks for Solana tokens.
# - Dexscreener liquidity & flags
# - Birdeye price (optional)
# - Rugcheck.xyz (optional)
# - On-chain mint/freeze authority flags (optional, via shared RPC)
# - Jupiter v6 SELL route probe (mint -> WSOL) as pre-trade safety
#
# Public API:
#   ok, report = await honeypot_check(mint)
#   if not ok:
#       logger.info(report["reasons"])
#
# Also exposes:
#   - _get_session()               → shared/local aiohttp.ClientSession
#   - BIRDEYE_API_URL              → for ai_gate
#   - _headers_birdeye()           → for ai_gate
#   - JUPITER_QUOTE_URL, WSOL_MINT → for ai_gate + routeguard
#   - SHARED_SOLANA_CLIENT         → for ai_gate token_decimals

from __future__ import annotations

import os
import asyncio
import atexit
import base64
import logging
from typing import Any, Dict, Optional, Tuple, List

import aiohttp

# ---------------------------------------------------------------------------
# Shared app-wide resources (best-effort, non-fatal if missing)
# ---------------------------------------------------------------------------
try:
    from utils_pkg.core_utils import (  # type: ignore
        session as SHARED_SESSION,             # aiohttp.ClientSession | None
        solana_client as SHARED_SOLANA_CLIENT  # AsyncClient | None
    )
except Exception:
    SHARED_SESSION = None
    SHARED_SOLANA_CLIENT = None

logger = logging.getLogger("honeypot")
if not logger.handlers:
    logging.basicConfig(level=logging.INFO)

# ---------------------------------------------------------------------------
# Config / env
# ---------------------------------------------------------------------------

DEXSCREENER_TOKEN_URL = os.getenv(
    "DEXSCREENER_TOKEN_URL",
    "https://api.dexscreener.com/latest/dex/tokens/{mint}",
)

BIRDEYE_API_URL = os.getenv("BIRDEYE_API_URL", "https://public-api.birdeye.so")
BIRDEYE_API_KEY = os.getenv("BIRDEYE_API_KEY", "")

RUGCHECK_API_URL = os.getenv(
    "RUGCHECK_API_URL",
    "https://api.rugcheck.xyz/v1/tokens/{mint}",
)

# Jupiter v6 base (we append /quote)
JUPITER_BASE = os.getenv("JUPITER_SWAP_API", "https://quote-api.jup.ag/v6")
JUPITER_QUOTE_URL = JUPITER_BASE.rstrip("/") + "/quote"

# So111... (wSOL)
WSOL_MINT = os.getenv(
    "WSOL_MINT",
    "So11111111111111111111111111111111111111112",
)

# Policy thresholds — tune via env/AI
MIN_LIQUIDITY_USD = float(os.getenv("HONEYPOT_MIN_LIQUIDITY_USD", "3000"))
MAX_SELL_TAX_BPS = int(os.getenv("HONEYPOT_MAX_SELL_TAX_BPS", "1500"))  # 15%

REQUIRE_JUP_SELL_ROUTE = (
    os.getenv("HONEYPOT_REQUIRE_JUP_SELL_ROUTE", "true").strip().lower()
    in ("1", "true", "yes", "on")
)

RPC_MINT_CHECK = (
    os.getenv("HONEYPOT_RPC_MINT_CHECK", "true").strip().lower()
    in ("1", "true", "yes", "on")
)

HTTP_TIMEOUT = float(os.getenv("HONEYPOT_HTTP_TIMEOUT", "10"))

# Simple blacklist/allowlist (comma-separated mints)
BLACKLIST_CSV = os.getenv("HONEYPOT_BLACKLIST_CSV")
ALLOWLIST_CSV = os.getenv("HONEYPOT_ALLOWLIST_CSV")

# ---------------------------------------------------------------------------
# Session management (also used by ai_gate + routeguard)
# ---------------------------------------------------------------------------

_SESSION: Optional[aiohttp.ClientSession] = None
_SESSION_LOCK = asyncio.Lock()


async def _get_session() -> aiohttp.ClientSession:
    """
    Return an aiohttp.ClientSession.

    Priority:
      1) Reuse app-wide SHARED_SESSION if present & open
      2) Reuse module-local _SESSION if present & open
      3) Create a new local session for this module
    """
    global _SESSION

    # 1) App-wide shared session from core_utils
    if SHARED_SESSION is not None and not SHARED_SESSION.closed:
        return SHARED_SESSION

    # 2) Local module session
    async with _SESSION_LOCK:
        if _SESSION is not None and not _SESSION.closed:
            return _SESSION
        _SESSION = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=HTTP_TIMEOUT)
        )
        return _SESSION


async def close_local_session() -> None:
    """
    Close only the local session created in this module.
    Never touches SHARED_SESSION from core_utils.
    """
    global _SESSION
    async with _SESSION_LOCK:
        if _SESSION is not None and not _SESSION.closed:
            await _SESSION.close()
        _SESSION = None


def _atexit_close() -> None:
    """
    Close our local session automatically when the interpreter exits.

    This helps `python -m utils_pkg.honeypot` / unit tests avoid
    "Unclosed client session" warnings. We do NOT close SHARED_SESSION.
    """
    try:
        if SHARED_SESSION is None:
            asyncio.run(close_local_session())
    except Exception:
        # At process shutdown, event loop lifecycles are messy; ignore.
        pass


atexit.register(_atexit_close)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _headers_birdeye() -> Dict[str, str]:
    """
    Shared Birdeye headers (used by this module and ai_gate).
    """
    h = {"accept": "application/json", "x-chain": "solana"}
    if BIRDEYE_API_KEY:
        h["X-API-KEY"] = BIRDEYE_API_KEY
    return h


def _in_csv_list(mint: str, csv_val: Optional[str]) -> bool:
    if not csv_val:
        return False
    return mint in {x.strip() for x in csv_val.split(",") if x.strip()}


async def _fetch_json(
    url: str,
    *,
    headers: Optional[Dict[str, str]] = None,
    params: Optional[Dict[str, str]] = None,
) -> Optional[Dict[str, Any]]:
    """
    Helper to GET JSON with our session; logs non-200 but never raises.
    """
    sess = await _get_session()
    try:
        async with sess.get(url, headers=headers, params=params) as resp:
            if resp.status == 200:
                return await resp.json()
            txt = await resp.text()
            logger.debug(f"GET {url} -> {resp.status}: {txt}")
            return None
    except Exception as e:
        logger.debug(f"_fetch_json error for {url}: {e}")
        return None


# ---------------------------------------------------------------------------
# Data sources
# ---------------------------------------------------------------------------

async def _dexscreener_check(mint: str) -> Dict[str, Any]:
    """
    Dexscreener: liquidity & simple flags.

    Schema is best-effort and resilient to missing fields.
    """
    url = DEXSCREENER_TOKEN_URL.format(mint=mint)
    data = await _fetch_json(url)
    result: Dict[str, Any] = {
        "source": "dexscreener",
        "liquidity_usd": 0.0,
        "pair_count": 0,
        "flags": [],
        "ok": True,
        "reasons": [],
    }
    if not data or "pairs" not in data:
        result["ok"] = False
        result["reasons"].append("dexscreener:no_data")
        return result

    pairs = data.get("pairs") or []
    result["pair_count"] = len(pairs)

    best_liq = 0.0
    for p in pairs:
        # liquidity field can be dict or number; normalize
        liq_info = p.get("liquidity") or {}
        if isinstance(liq_info, dict):
            liq = float(liq_info.get("usd") or 0)
        else:
            try:
                liq = float(liq_info or 0)
            except Exception:
                liq = 0.0
        best_liq = max(best_liq, liq)

        if p.get("priceUsd") in (None, "0", 0):
            result["flags"].append("no_price")
        if p.get("isHoneyPot"):
            result["flags"].append("flag_honeypot")
        if p.get("tradingPaused"):
            result["flags"].append("trading_paused")
        if p.get("pairCreatedAt") is None:
            result["flags"].append("no_pair_created_at")

    result["liquidity_usd"] = best_liq

    if best_liq < MIN_LIQUIDITY_USD:
        result["ok"] = False
        result["reasons"].append(f"dexscreener:low_liquidity<{MIN_LIQUIDITY_USD}")

    if "trading_paused" in result["flags"]:
        result["ok"] = False
        result["reasons"].append("dexscreener:trading_paused")

    return result


async def _birdeye_check(mint: str) -> Dict[str, Any]:
    """
    Birdeye price / basic signal. Non-blocking if disabled or no data.
    """
    result: Dict[str, Any] = {
        "source": "birdeye",
        "price": None,
        "liquidity_usd": None,
        "ok": True,
        "reasons": [],
    }
    if not BIRDEYE_API_URL:
        result["ok"] = False
        result["reasons"].append("birdeye:disabled")
        return result

    url_price = f"{BIRDEYE_API_URL.rstrip('/')}/public/price"
    price_js = await _fetch_json(
        url_price,
        headers=_headers_birdeye(),
        params={"address": mint},
    )

    if price_js and price_js.get("success", True):
        val = price_js.get("data", {}).get("value")
        try:
            result["price"] = float(val) if val is not None else None
        except Exception:
            result["price"] = None
    else:
        result["reasons"].append("birdeye:no_price")

    # You can expand this with liquidity/holders endpoints later.
    return result


async def _rugcheck_check(mint: str) -> Dict[str, Any]:
    """
    Rugcheck.xyz: heuristic risk signals.

    If API is down or schema changes, we don't hard-fail; we just mark reasons.
    """
    url = RUGCHECK_API_URL.format(mint=mint)
    data = await _fetch_json(url)
    res: Dict[str, Any] = {
        "source": "rugcheck",
        "score": None,
        "risks": [],
        "ok": True,
        "reasons": [],
    }
    if not data:
        res["reasons"].append("rugcheck:no_data")
        return res

    res["score"] = data.get("score")
    risks = data.get("risks") or []
    res["risks"] = risks

    text_join = " ".join(str(r).lower() for r in risks)
    if "honeypot" in text_join or "trading disabled" in text_join:
        res["ok"] = False
        res["reasons"].append("rugcheck:honeypot_or_trading_blocked")

    return res


async def _rpc_mint_flags(mint: str) -> Dict[str, Any]:
    """
    Check token mint & freeze authorities from chain (optional).

    Requires shared AsyncClient with get_account_info on mint:
      SHARED_SOLANA_CLIENT.get_account_info(Pubkey)
    """
    out: Dict[str, Any] = {
        "source": "rpc",
        "has_freeze_authority": None,
        "has_mint_authority": None,
        "ok": True,
        "reasons": [],
    }

    if not RPC_MINT_CHECK or SHARED_SOLANA_CLIENT is None:
        out["reasons"].append("rpc:disabled_or_no_client")
        return out

    try:
        from solders.pubkey import Pubkey

        resp = await SHARED_SOLANA_CLIENT.get_account_info(
            Pubkey.from_string(mint)
        )
        if not resp.value:
            out["ok"] = False
            out["reasons"].append("rpc:no_mint_account")
            return out

        data_b64, _encoding = resp.value.data
        raw = base64.b64decode(data_b64)

        # SPL Mint (simplified offsets):
        # mint_authority: [0:32]
        # freeze_authority: [44:76] (heuristic)
        mint_auth = raw[0:32]
        freeze_auth = raw[44:76] if len(raw) >= 76 else b"\x00" * 32

        out["has_mint_authority"] = (mint_auth != b"\x00" * 32)
        out["has_freeze_authority"] = (freeze_auth != b"\x00" * 32)

        if out["has_freeze_authority"]:
            out["reasons"].append("rpc:freeze_authority_present")

    except Exception as e:
        out["reasons"].append(f"rpc:error:{e}")

    return out


async def _jupiter_sell_route_check(
    mint: str,
    test_amount_base_units: int = 1_000_000,
) -> Dict[str, Any]:
    """
    Ask Jupiter v6 for a SELL quote mint -> WSOL for a small test amount.

    If there's no routePlan or outAmount is tiny, treat as potential honeypot / illiquid.
    """
    out: Dict[str, Any] = {
        "source": "jupiter",
        "route_available": None,
        "out_amount": None,
        "ok": True,
        "reasons": [],
    }

    params = {
        "inputMint": mint,
        "outputMint": WSOL_MINT,
        "amount": str(test_amount_base_units),
        "slippageBps": "300",  # 3% probe
        "swapMode": "ExactIn",
        "onlyDirectRoutes": "false",
    }

    sess = await _get_session()
    try:
        async with sess.get(JUPITER_QUOTE_URL, params=params) as resp:
            if resp.status != 200:
                out["ok"] = False
                out["reasons"].append(f"jupiter:http_{resp.status}")
                return out
            js = await resp.json()
    except Exception as e:
        out["ok"] = False
        out["reasons"].append(f"jupiter:error:{e}")
        return out

    route_plan: List[Any] = js.get("routePlan") or []
    out_amt = js.get("outAmount")

    if not route_plan:
        out["route_available"] = False
        if REQUIRE_JUP_SELL_ROUTE:
            out["ok"] = False
            out["reasons"].append("jupiter:no_route")
        return out

    try:
        out["out_amount"] = int(out_amt) if out_amt is not None else None
    except Exception:
        out["out_amount"] = None

    out["route_available"] = True

    if REQUIRE_JUP_SELL_ROUTE and (out["out_amount"] is None or out["out_amount"] <= 0):
        out["ok"] = False
        out["reasons"].append("jupiter:route_zero_out")

    return out


# ---------------------------------------------------------------------------
# Public main API
# ---------------------------------------------------------------------------

async def honeypot_check(mint: str) -> Tuple[bool, Dict[str, Any]]:
    """
    Aggregated honeypot / risk decision.

    Returns: (is_safe, report)

    report = {
      "mint": str,
      "ok": bool,
      "reasons": [str],
      "sources": {
        "dexscreener": {...},
        "birdeye": {...},
        "jupiter": {...},
        "rugcheck": {...},
        "rpc": {...},
      },
      "policy": {...},
    }
    """
    # Allowlist short-circuit
    if _in_csv_list(mint, ALLOWLIST_CSV):
        return True, {
            "mint": mint,
            "ok": True,
            "reasons": ["allowlist"],
            "sources": {},
            "policy": {
                "min_liquidity_usd": MIN_LIQUIDITY_USD,
                "max_sell_tax_bps": MAX_SELL_TAX_BPS,
                "require_jup_sell_route": REQUIRE_JUP_SELL_ROUTE,
            },
        }

    # Blacklist short-circuit
    if _in_csv_list(mint, BLACKLIST_CSV):
        return False, {
            "mint": mint,
            "ok": False,
            "reasons": ["blacklist"],
            "sources": {},
            "policy": {
                "min_liquidity_usd": MIN_LIQUIDITY_USD,
                "max_sell_tax_bps": MAX_SELL_TAX_BPS,
                "require_jup_sell_route": REQUIRE_JUP_SELL_ROUTE,
            },
        }

    tasks: List[asyncio.Future] = [
        _dexscreener_check(mint),
        _birdeye_check(mint),
        _jupiter_sell_route_check(mint),
        _rugcheck_check(mint),
    ]
    if RPC_MINT_CHECK:
        tasks.append(_rpc_mint_flags(mint))

    results = await asyncio.gather(*tasks, return_exceptions=True)

    combined_ok = True
    reasons: List[str] = []
    sources: Dict[str, Any] = {}

    for r in results:
        if isinstance(r, Exception):
            combined_ok = False
            reasons.append(f"exception:{r}")
            continue

        if not isinstance(r, dict):
            combined_ok = False
            reasons.append("bad_result_type")
            continue

        src = r.get("source", "unknown")
        sources[src] = r

        if not r.get("ok", True):
            combined_ok = False
            reasons.extend(r.get("reasons", []))

    # Policy: flag excessive sell tax if Rugcheck hints at it (best-effort)
    rc = sources.get("rugcheck", {})
    rc_risks = " ".join(str(x).lower() for x in rc.get("risks", []))
    if "sell tax" in rc_risks and MAX_SELL_TAX_BPS is not None:
        combined_ok = False
        reasons.append(f"tax:high_sell_tax>{MAX_SELL_TAX_BPS}bps")

    report: Dict[str, Any] = {
        "mint": mint,
        "ok": combined_ok,
        "reasons": reasons,
        "sources": sources,
        "policy": {
            "min_liquidity_usd": MIN_LIQUIDITY_USD,
            "max_sell_tax_bps": MAX_SELL_TAX_BPS,
            "require_jup_sell_route": REQUIRE_JUP_SELL_ROUTE,
        },
    }
    return combined_ok, report


async def is_token_safe(mint: str) -> bool:
    """
    Simple boolean wrapper for honeypot_check.
    """
    ok, _ = await honeypot_check(mint)
    return ok


# ---------------------------------------------------------------------------
# Self-test
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    async def _t() -> None:
        import sys

        if len(sys.argv) < 2:
            print("Usage: python -m utils_pkg.honeypot <MINT>")
            return

        mint = sys.argv[1]
        ok, rep = await honeypot_check(mint)
        print("SAFE:", ok)
        print("REASONS:", rep.get("reasons"))
        await close_local_session()

    asyncio.run(_t())


__all__ = [
    "BIRDEYE_API_URL",
    "JUPITER_QUOTE_URL",
    "WSOL_MINT",
    "SHARED_SESSION",
    "SHARED_SOLANA_CLIENT",
    "_get_session",
    "close_local_session",
    "_headers_birdeye",
    "honeypot_check",
    "is_token_safe",
]
