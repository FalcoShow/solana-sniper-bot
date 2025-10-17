# utils_pkg/price_feed.py
from __future__ import annotations

import os
import math
import asyncio
import time
from typing import Optional, Dict, Any

import aiohttp

from utils_pkg.solana_token import fetch_mint_info

# ──────────────────────────────────────────────────────────────────────────────
# Config / ENV
# ──────────────────────────────────────────────────────────────────────────────
WSOL = "So11111111111111111111111111111111111111112"
JUP_QUOTE_URL = os.getenv("JUP_QUOTE_URL", "https://quote-api.jup.ag/v6/quote")

def _envf(k: str, d: float) -> float:
    try:
        return float(os.getenv(k, str(d)))
    except Exception:
        return float(d)

def _envi(k: str, d: int) -> int:
    try:
        return int(os.getenv(k, str(d)))
    except Exception:
        return int(d)

# Cached price TTL (seconds)
TTL_SEC = _envf("PF_PRICE_TTL_SEC", 5.0)

# Retry knobs (for transient quote hiccups)
RETRIES = max(0, _envi("PF_PRICE_RETRIES", 1))
BACKOFF_BASE = max(0.05, _envf("PF_PRICE_BACKOFF_BASE", 0.25))  # seconds

# ──────────────────────────────────────────────────────────────────────────────
# Lightweight async HTTP
# ──────────────────────────────────────────────────────────────────────────────
_session: aiohttp.ClientSession | None = None

async def _get_session() -> aiohttp.ClientSession:
    global _session
    if _session is None or _session.closed:
        timeout = aiohttp.ClientTimeout(total=10, connect=4)
        _session = aiohttp.ClientSession(timeout=timeout, headers={"user-agent": "pnlsniper/1.0 (+pricefeed)"})
    return _session

async def _http_json(url: str, params: Dict[str, Any]) -> Optional[dict]:
    sess = await _get_session()
    for attempt in range(RETRIES + 1):
        try:
            async with sess.get(url, params=params) as r:
                txt = await r.text()
                if r.status != 200:
                    if attempt < RETRIES:
                        await asyncio.sleep(BACKOFF_BASE * (attempt + 1))
                    continue
                try:
                    return await r.json(content_type=None)
                except Exception:
                    # Malformed JSON; retry if allowed
                    if attempt < RETRIES:
                        await asyncio.sleep(BACKOFF_BASE * (attempt + 1))
                    continue
        except Exception:
            if attempt < RETRIES:
                await asyncio.sleep(BACKOFF_BASE * (attempt + 1))
            else:
                return None
    return None

# ──────────────────────────────────────────────────────────────────────────────
# Decimals cache (async)
# ──────────────────────────────────────────────────────────────────────────────
_DECIMALS: Dict[str, int] = {}

async def _decimals_async(mint: str) -> Optional[int]:
    d = _DECIMALS.get(mint)
    if d is not None:
        return d
    try:
        info = await fetch_mint_info(mint)
        if not info:
            return None
        dec, _owner = info
        _DECIMALS[mint] = int(dec)
        return int(dec)
    except Exception:
        return None

# ──────────────────────────────────────────────────────────────────────────────
# In-process cache for prices (monotonic TTL)
# ──────────────────────────────────────────────────────────────────────────────
# key -> (price_sol_per_token (float), expires_at_monotonic (float))
_CACHE: Dict[str, tuple[float, float]] = {}

def _now_mono() -> float:
    return time.monotonic()

def _cache_get(key: str) -> Optional[float]:
    rec = _CACHE.get(key)
    if not rec:
        return None
    price, exp = rec
    if _now_mono() < exp:
        return price
    _CACHE.pop(key, None)
    return None

def _cache_put(key: str, price: float) -> None:
    _CACHE[key] = (float(price), _now_mono() + TTL_SEC)

# ──────────────────────────────────────────────────────────────────────────────
# Quote helpers
# ──────────────────────────────────────────────────────────────────────────────
async def _jup_quote(input_mint: str, output_mint: str, amount_iu: int) -> Optional[dict]:
    """
    Minimal Jupiter v6 /quote wrapper.
    - swapMode: ExactIn
    - slippage is irrelevant for pricing
    """
    if amount_iu <= 0:
        return None
    params = {
        "inputMint": input_mint,
        "outputMint": output_mint,
        "amount": str(amount_iu),
        "slippageBps": "50",        # accepted by API; not used for pricing
        "swapMode": "ExactIn",
        "onlyDirectRoutes": "false",
        "asLegacyTransaction": "true",
    }
    return await _http_json(JUP_QUOTE_URL, params)

async def _price_from_sell_probe(mint: str, decimals: int, probe_tokens_ui: float) -> float:
    """
    Compute price by selling a tiny token amount and dividing outAmount(SOL) / tokens_ui.
    """
    tokens_ui = max(1e-9, float(probe_tokens_ui))
    amount_raw = int(max(1, round(tokens_ui * (10 ** int(decimals)))))
    data = await _jup_quote(mint, WSOL, amount_raw)
    if not data:
        return 0.0
    try:
        lamports = int(data.get("outAmount", "0"))
        if lamports <= 0:
            return 0.0
        sol_out = lamports / 1_000_000_000.0
        px = sol_out / tokens_ui if tokens_ui > 0 else 0.0
        return px if px > 0 and math.isfinite(px) else 0.0
    except Exception:
        return 0.0

async def _price_from_buy_invert(mint: str) -> float:
    """
    Fallback: buy with 1 SOL and invert (price = 1 / tokens_ui).
    """
    data = await _jup_quote(WSOL, mint, 1_000_000_000)  # 1 SOL
    if not data:
        return 0.0
    try:
        out_raw = int(data.get("outAmount", "0"))
        dec = await _decimals_async(mint)
        if dec is None or out_raw <= 0:
            return 0.0
        tokens_ui = out_raw / (10 ** int(dec))
        return (1.0 / tokens_ui) if tokens_ui > 0 else 0.0
    except Exception:
        return 0.0

# ──────────────────────────────────────────────────────────────────────────────
# Public API
# ──────────────────────────────────────────────────────────────────────────────
async def price_in_sol(mint: str, decimals_hint: Optional[int] = None, probe_tokens: float = 0.001) -> float:
    """
    Return estimated price in SOL for 1 token (UI units) for `mint`.
    - Uses tiny *sell* quote first (ExactIn), then falls back to *buy invert*.
    - Results are cached in-process for TTL_SEC seconds.
    - `decimals_hint` speeds up the first lookup; we’ll self-resolve if not provided.
    """
    # Resolve decimals (if unknown)
    decimals: Optional[int] = int(decimals_hint) if decimals_hint is not None else None
    if decimals is None or decimals < 0:
        decimals = await _decimals_async(mint)
        if decimals is None:
            # Fallback path that doesn’t need decimals
            price = await _price_from_buy_invert(mint)
            if price > 0:
                _cache_put(f"{mint}:inv", price)
                return price
            return 0.0

    # Cache lookup keyed by (mint, dec, probe bucket)
    probe = max(1e-6, float(probe_tokens))
    key = f"{mint}:{decimals}:{round(probe, 6)}"
    cached = _cache_get(key)
    if cached is not None:
        return cached

    # Prefer sell-probe (more representative for exits)
    price = await _price_from_sell_probe(mint, decimals, probe)
    if price <= 0:
        # Fallback: buy 1 SOL and invert
        price = await _price_from_buy_invert(mint)

    if price > 0:
        _cache_put(key, price)
    return price

# Convenience aliases expected by other modules
async def get_price_ui(mint: str) -> float:
    return await price_in_sol(mint)

async def price_of_ui(mint: str) -> float:
    return await price_in_sol(mint)

async def price_of(mint: str) -> float:
    return await price_in_sol(mint)

# Optional graceful close for aiohttp session (call on shutdown if desired)
async def close():
    global _session
    if _session and not _session.closed:
        await _session.close()
        _session = None

__all__ = [
    "price_in_sol",
    "get_price_ui",
    "price_of_ui",
    "price_of",
    "close",
]
