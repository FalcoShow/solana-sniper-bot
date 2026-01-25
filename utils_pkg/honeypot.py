from __future__ import annotations

"""
utils_pkg/honeypot.py

Canonical pre-buy safety / honeypot checks (V3+++ / production).
===============================================================

Role
----
This module answers: "Is this mint *likely* sellable / not obviously trapped?"

It provides a single, explainable, cacheable API:

    ok, report = await honeypot_check(mint)

Report is structured for logs, FeatureBuilder enrichment, and AI gates.

Design
------
- Fast: concurrent checks + strict timeouts
- Robust: hard vs soft failures (no dead trading due to rate limits)
- Deterministic: uses on-chain mint parsing via utils_pkg.liquidity
- Compatible: keeps exports referenced by older modules (ai_gate, route guards)

Hard vs Soft failures
---------------------
Hard failures block entry.
Soft failures annotate report but do not block entry unless strict mode enabled.

Typical hard failures:
- Explicit blacklist
- Dexscreener says trading paused / honeypot flag
- On-chain freeze authority present (if required)
- Jupiter sell route definitively absent (if required)
- Rugcheck signals honeypot/trading disabled (if strict)

Soft failures:
- External API down / 429 / 5xx
- Missing optional sources (Birdeye key missing)
"""

import asyncio
import atexit
import os
import time
from dataclasses import dataclass
from typing import Any

import aiohttp

from logging_setup import get_logger

log = get_logger()

# -----------------------------------------------------------------------------
# Optional shared app-wide resources (best-effort)
# -----------------------------------------------------------------------------
try:
    from utils_pkg.core_utils import (  # type: ignore
        session as SHARED_SESSION,  # aiohttp.ClientSession | None
        solana_client as SHARED_SOLANA_CLIENT,  # AsyncClient | None
    )
except Exception:  # pragma: no cover
    SHARED_SESSION = None
    SHARED_SOLANA_CLIENT = None

# Canonical mint parsing + probe helpers
try:
    from utils_pkg.liquidity import fetch_mint_info  # type: ignore
except Exception:  # pragma: no cover
    fetch_mint_info = None  # type: ignore

# Router (preferred for route probing)
try:
    from utils_pkg import router as _router  # type: ignore
except Exception:  # pragma: no cover
    _router = None  # type: ignore

# -----------------------------------------------------------------------------
# Config / env
# -----------------------------------------------------------------------------
try:
    import config as C  # type: ignore
except Exception:  # pragma: no cover

    class _C:
        HONEYPOT_CACHE_TTL_S = float(os.getenv("HONEYPOT_CACHE_TTL_S", "180"))
        HONEYPOT_HTTP_TIMEOUT_S = float(os.getenv("HONEYPOT_HTTP_TIMEOUT", "10"))
        HONEYPOT_TOTAL_BUDGET_S = float(os.getenv("HONEYPOT_TOTAL_BUDGET_S", "12"))

        HONEYPOT_MIN_LIQUIDITY_USD = float(os.getenv("HONEYPOT_MIN_LIQUIDITY_USD", "3000"))
        HONEYPOT_REQUIRE_JUP_SELL_ROUTE = int(os.getenv("HONEYPOT_REQUIRE_JUP_SELL_ROUTE", "1"))
        HONEYPOT_JUP_STRICT = int(os.getenv("HONEYPOT_JUP_STRICT", "0"))
        HONEYPOT_RUGCHECK_STRICT = int(os.getenv("HONEYPOT_RUGCHECK_STRICT", "0"))

        HONEYPOT_REQUIRE_NO_FREEZE = int(os.getenv("HONEYPOT_REQUIRE_NO_FREEZE", "0"))
        HONEYPOT_REQUIRE_MINT_AUTH_DISABLED = int(
            os.getenv("HONEYPOT_REQUIRE_MINT_AUTH_DISABLED", "0")
        )

        HONEYPOT_ENABLE_DEXSCREENER = int(os.getenv("HONEYPOT_ENABLE_DEXSCREENER", "1"))
        HONEYPOT_ENABLE_RUGCHECK = int(os.getenv("HONEYPOT_ENABLE_RUGCHECK", "0"))
        HONEYPOT_ENABLE_BIRDEYE = int(os.getenv("HONEYPOT_ENABLE_BIRDEYE", "0"))

        HONEYPOT_BLACKLIST_CSV = os.getenv("HONEYPOT_BLACKLIST_CSV", "")
        HONEYPOT_ALLOWLIST_CSV = os.getenv("HONEYPOT_ALLOWLIST_CSV", "")

        # Probe sizing
        HONEYPOT_SELL_PROBE_SLIPPAGE_BPS = int(os.getenv("HONEYPOT_SELL_PROBE_SLIPPAGE_BPS", "300"))
        HONEYPOT_SELL_PROBE_MAX_IMPACT_PCT = float(
            os.getenv("HONEYPOT_SELL_PROBE_MAX_IMPACT_PCT", "90")
        )

    C = _C()  # type: ignore

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

JUPITER_BASE = os.getenv("JUPITER_SWAP_API", "https://quote-api.jup.ag/v6")
JUPITER_QUOTE_URL = JUPITER_BASE.rstrip("/") + "/quote"

WSOL_MINT = os.getenv(
    "WSOL_MINT",
    "So11111111111111111111111111111111111111112",
)

# -----------------------------------------------------------------------------
# Session management (compatible with ai_gate expectations)
# -----------------------------------------------------------------------------
_SESSION: aiohttp.ClientSession | None = None
_SESSION_LOCK = asyncio.Lock()


async def _get_session() -> aiohttp.ClientSession:
    """
    Return an aiohttp.ClientSession.

    Priority:
      1) SHARED_SESSION if present & open
      2) module-local session
    """
    global _SESSION
    if SHARED_SESSION is not None and not getattr(SHARED_SESSION, "closed", True):
        return SHARED_SESSION  # type: ignore[return-value]

    timeout_s = float(getattr(C, "HONEYPOT_HTTP_TIMEOUT_S", 10.0))
    async with _SESSION_LOCK:
        if _SESSION is not None and not _SESSION.closed:
            return _SESSION
        _SESSION = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=timeout_s))
        return _SESSION


async def close_local_session() -> None:
    global _SESSION
    async with _SESSION_LOCK:
        if _SESSION is not None and not _SESSION.closed:
            await _SESSION.close()
        _SESSION = None


def _atexit_close() -> None:
    try:
        if SHARED_SESSION is None:
            asyncio.run(close_local_session())
    except Exception:
        pass


atexit.register(_atexit_close)


# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------
def _headers_birdeye() -> dict[str, str]:
    h = {"accept": "application/json", "x-chain": "solana"}
    if BIRDEYE_API_KEY:
        h["X-API-KEY"] = BIRDEYE_API_KEY
    return h


def _csv_has(mint: str, csv_val: str) -> bool:
    if not csv_val:
        return False
    s = mint.strip()
    return s in {x.strip() for x in csv_val.split(",") if x.strip()}


def _safe_float(x: Any, default: float = 0.0) -> float:
    try:
        f = float(x)
        if f != f or f in (float("inf"), float("-inf")):
            return default
        return f
    except Exception:
        return default


def _normalize_price_impact_pct(raw_pi: Any) -> float:
    """
    Jupiter sometimes returns fraction (0.12) or percent (12).
    Normalize to percent.
    """
    pi = _safe_float(raw_pi, 0.0)
    if pi <= 1.0:
        pi *= 100.0
    return max(0.0, pi)


async def _fetch_json(
    url: str, *, headers: dict[str, str] | None = None, params: dict[str, str] | None = None
) -> dict[str, Any] | None:
    sess = await _get_session()
    try:
        async with sess.get(url, headers=headers, params=params) as resp:
            if resp.status == 200:
                js = await resp.json()
                return js if isinstance(js, dict) else None
            # don’t spam logs at info level
            try:
                txt = await resp.text()
            except Exception:
                txt = "<no-body>"
            log.debug(f"honeypot GET {url} -> {resp.status} {txt[:500]}")
            return None
    except Exception as e:
        log.debug(f"honeypot _fetch_json error {url}: {e}")
        return None


# -----------------------------------------------------------------------------
# Cache (mint -> report)
# -----------------------------------------------------------------------------
_HP_CACHE: dict[str, tuple[float, dict[str, Any]]] = {}
_HP_CACHE_LOCK = asyncio.Lock()


async def _cache_get(mint: str) -> dict[str, Any] | None:
    ttl = float(getattr(C, "HONEYPOT_CACHE_TTL_S", 180.0))
    if ttl <= 0:
        return None
    now = time.time()
    async with _HP_CACHE_LOCK:
        row = _HP_CACHE.get(mint)
        if not row:
            return None
        exp, rep = row
        if now >= exp:
            _HP_CACHE.pop(mint, None)
            return None
        return rep


async def _cache_set(mint: str, rep: dict[str, Any]) -> None:
    ttl = float(getattr(C, "HONEYPOT_CACHE_TTL_S", 180.0))
    if ttl <= 0:
        return
    async with _HP_CACHE_LOCK:
        _HP_CACHE[mint] = (time.time() + ttl, rep)


# -----------------------------------------------------------------------------
# Source checks
# -----------------------------------------------------------------------------
async def _dexscreener_check(mint: str) -> dict[str, Any]:
    enabled = bool(int(getattr(C, "HONEYPOT_ENABLE_DEXSCREENER", 1)))
    out: dict[str, Any] = {
        "source": "dexscreener",
        "enabled": enabled,
        "ok": True,
        "hard_fail": False,
        "reasons": [],
        "data": {},
    }
    if not enabled:
        out["ok"] = True
        out["reasons"].append("dexscreener:disabled")
        return out

    js = await _fetch_json(DEXSCREENER_TOKEN_URL.format(mint=mint))
    if not js or "pairs" not in js:
        # external API issue -> soft by default
        out["ok"] = True
        out["reasons"].append("dexscreener:no_data:soft")
        return out

    pairs = js.get("pairs") or []
    out["data"]["pair_count"] = len(pairs)

    best_liq = 0.0
    any_paused = False
    any_hp_flag = False

    for p in pairs:
        liq = 0.0
        liq_info = p.get("liquidity") or {}
        if isinstance(liq_info, dict):
            liq = _safe_float(liq_info.get("usd"), 0.0)
        else:
            liq = _safe_float(liq_info, 0.0)

        best_liq = max(best_liq, liq)

        # Dexscreener flags (best-effort; schema varies)
        if bool(p.get("tradingPaused")):
            any_paused = True
        if bool(p.get("isHoneyPot")):
            any_hp_flag = True

    out["data"]["liquidity_usd_best"] = best_liq

    min_liq = float(getattr(C, "HONEYPOT_MIN_LIQUIDITY_USD", 3000.0))
    if best_liq < min_liq:
        out["hard_fail"] = True
        out["ok"] = False
        out["reasons"].append(f"dexscreener:low_liquidity<{min_liq:.0f}")

    if any_paused:
        out["hard_fail"] = True
        out["ok"] = False
        out["reasons"].append("dexscreener:trading_paused")

    if any_hp_flag:
        out["hard_fail"] = True
        out["ok"] = False
        out["reasons"].append("dexscreener:honeypot_flag")

    return out


async def _birdeye_price_check(mint: str) -> dict[str, Any]:
    enabled = bool(int(getattr(C, "HONEYPOT_ENABLE_BIRDEYE", 0)))
    out: dict[str, Any] = {
        "source": "birdeye",
        "enabled": enabled,
        "ok": True,
        "hard_fail": False,
        "reasons": [],
        "data": {},
    }

    if not enabled:
        out["reasons"].append("birdeye:disabled")
        return out

    if not BIRDEYE_API_KEY:
        out["reasons"].append("birdeye:no_api_key:soft")
        return out

    url = f"{BIRDEYE_API_URL.rstrip('/')}/public/price"
    js = await _fetch_json(url, headers=_headers_birdeye(), params={"address": mint})
    if not js:
        out["reasons"].append("birdeye:no_data:soft")
        return out

    try:
        val = js.get("data", {}).get("value")
        out["data"]["price"] = float(val) if val is not None else None
    except Exception:
        out["data"]["price"] = None
        out["reasons"].append("birdeye:bad_price:soft")

    return out


async def _rugcheck_check(mint: str) -> dict[str, Any]:
    enabled = bool(int(getattr(C, "HONEYPOT_ENABLE_RUGCHECK", 0)))
    strict = bool(int(getattr(C, "HONEYPOT_RUGCHECK_STRICT", 0)))

    out: dict[str, Any] = {
        "source": "rugcheck",
        "enabled": enabled,
        "ok": True,
        "hard_fail": False,
        "reasons": [],
        "data": {},
    }
    if not enabled:
        out["reasons"].append("rugcheck:disabled")
        return out

    js = await _fetch_json(RUGCHECK_API_URL.format(mint=mint))
    if not js:
        # External API issue: soft unless strict
        if strict:
            out["ok"] = False
            out["hard_fail"] = True
            out["reasons"].append("rugcheck:no_data:strict")
        else:
            out["reasons"].append("rugcheck:no_data:soft")
        return out

    # Best-effort parse
    score = js.get("score", None)
    risks = js.get("risks") or js.get("risk") or []
    out["data"]["score"] = score
    out["data"]["risks"] = risks

    risk_text = " ".join(str(r).lower() for r in risks)
    # Heuristic "bad" indicators
    bad = any(
        k in risk_text
        for k in [
            "honeypot",
            "trading disabled",
            "sell blocked",
            "cannot sell",
            "blacklist",
            "freeze",
        ]
    )
    if bad:
        out["ok"] = False
        out["hard_fail"] = True
        out["reasons"].append("rugcheck:high_risk_flags")

    return out


async def _mint_authority_check(mint: str) -> dict[str, Any]:
    """
    On-chain mint/freeze authority flags via utils_pkg.liquidity.fetch_mint_info().
    This is deterministic and does not rely on external APIs.
    """
    out: dict[str, Any] = {
        "source": "mint",
        "enabled": True,
        "ok": True,
        "hard_fail": False,
        "reasons": [],
        "data": {},
    }

    if fetch_mint_info is None:
        # If liquidity module isn't available, treat as soft by default.
        out["reasons"].append("mint:fetch_mint_info_unavailable:soft")
        return out

    info = await fetch_mint_info(mint)
    if info is None:
        out["ok"] = False
        out["hard_fail"] = True
        out["reasons"].append("mint:no_account_or_parse")
        return out

    out["data"]["decimals"] = info.decimals
    out["data"]["mint_authority_present"] = info.mint_authority_present
    out["data"]["freeze_authority_present"] = info.freeze_authority_present
    out["data"]["source"] = info.source

    require_no_freeze = bool(int(getattr(C, "HONEYPOT_REQUIRE_NO_FREEZE", 0)))
    require_mint_disabled = bool(int(getattr(C, "HONEYPOT_REQUIRE_MINT_AUTH_DISABLED", 0)))

    if require_no_freeze and info.freeze_authority_present is True:
        out["ok"] = False
        out["hard_fail"] = True
        out["reasons"].append("mint:freeze_authority_present")

    if require_mint_disabled and info.mint_authority_present is True:
        out["ok"] = False
        out["hard_fail"] = True
        out["reasons"].append("mint:mint_authority_present")

    return out


def _pick_sell_probe_amount_raw(decimals: int | None) -> int:
    """
    Choose a small-but-not-tiny raw amount for sell-route probing.
    Goal: trigger route discovery without needing a real balance.
    """
    if decimals is None:
        return 1_000_000  # legacy default (works often for 6-9 dec tokens)
    d = int(decimals)
    if d <= 0:
        return 1
    # ~0.01 token in raw units
    amt = int((10**d) * 0.01)
    # Clamp to a sane range
    return max(1, min(amt, 50_000_000))


async def _router_sell_quote_probe(mint: str, amount_raw: int) -> dict[str, Any] | None:
    """
    Prefer router.get_sell_quote() if present; fall back to router.get_quote() shape if needed.
    Returns quote dict or None.
    """
    if _router is None:
        return None

    # Best guess functions
    fn = getattr(_router, "get_sell_quote", None)
    if callable(fn):
        # Try common signatures
        for kwargs in (
            {"mint": mint, "amount_raw": amount_raw},
            {"mint": mint, "amount": amount_raw},
            {"mint": mint, "amount_in": amount_raw},
            {"input_mint": mint, "amount_raw": amount_raw},
        ):
            try:
                q = await fn(**kwargs)  # type: ignore[misc]
                return q if isinstance(q, dict) else None
            except TypeError:
                continue
            except Exception:
                return None

    # Generic quote (if router exposes it)
    fn2 = getattr(_router, "get_quote", None)
    if callable(fn2):
        for kwargs in (
            {"input_mint": mint, "output_mint": WSOL_MINT, "amount_raw": amount_raw},
            {"inputMint": mint, "outputMint": WSOL_MINT, "amount": amount_raw},
        ):
            try:
                q = await fn2(**kwargs)  # type: ignore[misc]
                return q if isinstance(q, dict) else None
            except TypeError:
                continue
            except Exception:
                return None

    return None


async def _jupiter_sell_route_check(mint: str, decimals: int | None) -> dict[str, Any]:
    """
    Probes for a SELL route mint -> WSOL.
    Hard-fails only when:
      - require_jup_sell_route == True AND
      - we definitively see "no route" (not just HTTP/rate limit)
      - OR strict mode is enabled and we can't probe reliably
    """
    require_route = bool(int(getattr(C, "HONEYPOT_REQUIRE_JUP_SELL_ROUTE", 1)))
    strict = bool(int(getattr(C, "HONEYPOT_JUP_STRICT", 0)))
    max_impact_pct = float(getattr(C, "HONEYPOT_SELL_PROBE_MAX_IMPACT_PCT", 90.0))
    slip_bps = int(getattr(C, "HONEYPOT_SELL_PROBE_SLIPPAGE_BPS", 300))

    out: dict[str, Any] = {
        "source": "jupiter",
        "enabled": True,
        "ok": True,
        "hard_fail": False,
        "reasons": [],
        "data": {},
    }

    amount_raw = _pick_sell_probe_amount_raw(decimals)
    out["data"]["amount_raw"] = amount_raw

    # 1) Prefer router probe (keeps your money-pipe deterministic)
    q = await _router_sell_quote_probe(mint, amount_raw=amount_raw)
    if isinstance(q, dict) and q:
        route_plan = q.get("routePlan") or q.get("route_plan") or []
        out_amt = q.get("outAmount") or q.get("out_amount")
        pi_pct = _normalize_price_impact_pct(
            q.get("priceImpactPct", q.get("price_impact_pct", 0.0))
        )

        out["data"]["via"] = "router"
        out["data"]["route_len"] = len(route_plan) if isinstance(route_plan, list) else 0
        out["data"]["outAmount"] = out_amt
        out["data"]["priceImpactPct"] = pi_pct

        route_ok = bool(route_plan) and _safe_float(out_amt, 0.0) > 0.0
        if not route_ok:
            if require_route:
                out["ok"] = False
                out["hard_fail"] = True
                out["reasons"].append("jupiter:no_route(router)")
            else:
                out["reasons"].append("jupiter:no_route(router):soft")
            return out

        if pi_pct > max_impact_pct and strict:
            out["ok"] = False
            out["hard_fail"] = True
            out["reasons"].append(f"jupiter:sell_impact_too_high>{max_impact_pct:.0f}%")
        return out

    # 2) Fallback to direct HTTP probe (kept for compatibility)
    params = {
        "inputMint": mint,
        "outputMint": WSOL_MINT,
        "amount": str(amount_raw),
        "slippageBps": str(slip_bps),
        "swapMode": "ExactIn",
        "onlyDirectRoutes": "false",
    }

    sess = await _get_session()
    try:
        async with sess.get(JUPITER_QUOTE_URL, params=params) as resp:
            status = resp.status
            if status != 200:
                tag = f"jupiter:http_{status}"
                if strict and require_route:
                    out["ok"] = False
                    out["hard_fail"] = True
                    out["reasons"].append(tag)
                else:
                    out["reasons"].append(tag + ":soft")
                return out
            js = await resp.json()
    except Exception as e:
        tag = f"jupiter:error:{e}"
        if strict and require_route:
            out["ok"] = False
            out["hard_fail"] = True
            out["reasons"].append(tag)
        else:
            out["reasons"].append(tag + ":soft")
        return out

    route_plan = js.get("routePlan") or []
    out_amt = js.get("outAmount")
    pi_pct = _normalize_price_impact_pct(js.get("priceImpactPct", 0.0))

    out["data"]["via"] = "http"
    out["data"]["route_len"] = len(route_plan) if isinstance(route_plan, list) else 0
    out["data"]["outAmount"] = out_amt
    out["data"]["priceImpactPct"] = pi_pct

    route_ok = bool(route_plan) and _safe_float(out_amt, 0.0) > 0.0
    if not route_ok:
        if require_route:
            out["ok"] = False
            out["hard_fail"] = True
            out["reasons"].append("jupiter:no_route(http)")
        else:
            out["reasons"].append("jupiter:no_route(http):soft")
        return out

    if pi_pct > max_impact_pct and strict:
        out["ok"] = False
        out["hard_fail"] = True
        out["reasons"].append(f"jupiter:sell_impact_too_high>{max_impact_pct:.0f}%")

    return out


# -----------------------------------------------------------------------------
# Public API
# -----------------------------------------------------------------------------
@dataclass
class HoneypotPolicy:
    min_liquidity_usd: float
    require_jup_sell_route: bool
    jup_strict: bool
    rugcheck_strict: bool
    require_no_freeze: bool
    require_mint_auth_disabled: bool

    @staticmethod
    def current() -> HoneypotPolicy:
        return HoneypotPolicy(
            min_liquidity_usd=float(getattr(C, "HONEYPOT_MIN_LIQUIDITY_USD", 3000.0)),
            require_jup_sell_route=bool(int(getattr(C, "HONEYPOT_REQUIRE_JUP_SELL_ROUTE", 1))),
            jup_strict=bool(int(getattr(C, "HONEYPOT_JUP_STRICT", 0))),
            rugcheck_strict=bool(int(getattr(C, "HONEYPOT_RUGCHECK_STRICT", 0))),
            require_no_freeze=bool(int(getattr(C, "HONEYPOT_REQUIRE_NO_FREEZE", 0))),
            require_mint_auth_disabled=bool(
                int(getattr(C, "HONEYPOT_REQUIRE_MINT_AUTH_DISABLED", 0))
            ),
        )


async def honeypot_check(mint: str, *, force: bool = False) -> tuple[bool, dict[str, Any]]:
    """
    Aggregated honeypot / safety decision.

    Returns:
      (ok, report)

    report = {
      "mint": str,
      "ok": bool,
      "hard_reasons": [str],
      "soft_reasons": [str],
      "reasons": [str],                 # merged
      "summary": { ...meta-friendly... },
      "sources": { "dexscreener": {...}, "mint": {...}, "jupiter": {...}, ... },
      "policy": { ... }
    }
    """
    mint = (mint or "").strip()
    policy = HoneypotPolicy.current()

    # Allowlist / blacklist
    allow_csv = str(getattr(C, "HONEYPOT_ALLOWLIST_CSV", "") or "")
    block_csv = str(getattr(C, "HONEYPOT_BLACKLIST_CSV", "") or "")

    if _csv_has(mint, allow_csv):
        rep = {
            "mint": mint,
            "ok": True,
            "hard_reasons": [],
            "soft_reasons": ["allowlist"],
            "reasons": ["allowlist"],
            "summary": {"is_honeypot": False, "trading_paused": False, "blacklisted": False},
            "sources": {},
            "policy": policy.__dict__,
        }
        return True, rep

    if _csv_has(mint, block_csv):
        rep = {
            "mint": mint,
            "ok": False,
            "hard_reasons": ["blacklist"],
            "soft_reasons": [],
            "reasons": ["blacklist"],
            "summary": {"is_honeypot": True, "trading_paused": False, "blacklisted": True},
            "sources": {},
            "policy": policy.__dict__,
        }
        return False, rep

    if not force:
        cached = await _cache_get(mint)
        if cached is not None:
            return bool(cached.get("ok", False)), cached

    # Budget and concurrency
    total_budget_s = float(getattr(C, "HONEYPOT_TOTAL_BUDGET_S", 12.0))

    async def _run_all() -> dict[str, Any]:
        # We always run mint + jupiter probes, because they’re the most "money-pipe" relevant.
        # External sources are optional.
        tasks = []

        # mint auth check
        tasks.append(_mint_authority_check(mint))

        # dexscreener
        tasks.append(_dexscreener_check(mint))

        # rugcheck
        tasks.append(_rugcheck_check(mint))

        # birdeye (optional)
        tasks.append(_birdeye_price_check(mint))

        # jupiter probe needs decimals; we’ll compute after mint check finishes if possible.
        # But to keep concurrency, we’ll compute decimals from mint result post-gather then rerun probe if needed.
        # First gather partials (excluding jupiter), then run jupiter with decimals.
        res = await asyncio.gather(*tasks, return_exceptions=True)

        sources: dict[str, Any] = {}
        decimals: int | None = None

        for item in res:
            if isinstance(item, Exception):
                # treat as soft failure unless strict policy later forces it
                tag = f"exception:{item}"
                sources.setdefault(
                    "exceptions",
                    {
                        "source": "exceptions",
                        "ok": True,
                        "hard_fail": False,
                        "reasons": [tag],
                        "data": {},
                    },
                )
                continue
            if isinstance(item, dict):
                src = str(item.get("source", "unknown"))
                sources[src] = item
                if src == "mint":
                    try:
                        decimals = item.get("data", {}).get("decimals")
                        decimals = int(decimals) if decimals is not None else None
                    except Exception:
                        decimals = None

        # Now run jupiter sell route probe with decimals knowledge
        jup = await _jupiter_sell_route_check(mint, decimals=decimals)
        sources["jupiter"] = jup

        return sources

    try:
        sources = await asyncio.wait_for(_run_all(), timeout=total_budget_s)
    except Exception as e:
        # If honeypot pipeline itself fails, default to "soft fail" (do not block)
        rep = {
            "mint": mint,
            "ok": True,
            "hard_reasons": [],
            "soft_reasons": [f"honeypot:internal_error:{e}:soft"],
            "reasons": [f"honeypot:internal_error:{e}:soft"],
            "summary": {"is_honeypot": False, "trading_paused": False, "blacklisted": False},
            "sources": {
                "internal": {
                    "source": "internal",
                    "ok": True,
                    "hard_fail": False,
                    "reasons": [str(e)],
                    "data": {},
                }
            },
            "policy": policy.__dict__,
        }
        await _cache_set(mint, rep)
        return True, rep

    hard: list[str] = []
    soft: list[str] = []

    # Summaries for meta wiring
    liq_usd = None
    trading_paused = False
    is_honeypot = False

    for src, r in sources.items():
        if not isinstance(r, dict):
            continue
        rs = list(r.get("reasons") or [])
        if not r.get("ok", True) and r.get("hard_fail", False):
            hard.extend(rs)
        else:
            # add reasons as soft unless it's a clean OK/no reasons
            for x in rs:
                if (x and x.endswith(":soft")) or (
                    x and (src in {"birdeye", "rugcheck", "dexscreener"} and "no_data" in x)
                ):
                    soft.append(x)
                elif x:
                    # non-hard reason from a source
                    soft.append(x)

        # extract some canonical summaries
        if src == "dexscreener":
            liq_usd = (r.get("data", {}) or {}).get("liquidity_usd_best")
            # if hard reason includes paused/honeypot, reflect in summary
            if any("trading_paused" in s for s in (r.get("reasons") or [])):
                trading_paused = True
            if any("honeypot" in s for s in (r.get("reasons") or [])):
                is_honeypot = True

        if src == "mint":
            if any("freeze_authority_present" in s for s in (r.get("reasons") or [])):
                is_honeypot = True  # treated as unsafe if required

        if src == "jupiter" and any("no_route" in s for s in (r.get("reasons") or [])):
            is_honeypot = True  # sell route missing is effectively a honeypot

    ok = len(hard) == 0

    # If require_jup_sell_route and we got ONLY soft jupiter errors, keep OK unless strict
    # (already handled in the jupiter check logic)

    reasons = hard + soft

    rep: dict[str, Any] = {
        "mint": mint,
        "ok": ok,
        "hard_reasons": hard,
        "soft_reasons": soft,
        "reasons": reasons,
        "summary": {
            "is_honeypot": bool(is_honeypot) if not ok else False,
            "trading_paused": bool(trading_paused),
            "blacklisted": False,
            "liq_usd": _safe_float(liq_usd, 0.0) if liq_usd is not None else None,
            # Convenience fields for downstream:
            "jup_sell_route_ok": bool(sources.get("jupiter", {}).get("ok", True))
            if isinstance(sources.get("jupiter"), dict)
            else None,
            "sell_price_impact_pct": (sources.get("jupiter", {}).get("data", {}) or {}).get(
                "priceImpactPct"
            )
            if isinstance(sources.get("jupiter"), dict)
            else None,
        },
        "sources": sources,
        "policy": policy.__dict__,
    }

    await _cache_set(mint, rep)
    return ok, rep


async def is_token_safe(mint: str) -> bool:
    ok, _ = await honeypot_check(mint)
    return ok


__all__ = [
    # compatibility exports
    "BIRDEYE_API_URL",
    "JUPITER_QUOTE_URL",
    "SHARED_SESSION",
    "SHARED_SOLANA_CLIENT",
    "WSOL_MINT",
    "HoneypotPolicy",
    "_get_session",
    "_headers_birdeye",
    "close_local_session",
    # public API
    "honeypot_check",
    "is_token_safe",
]
