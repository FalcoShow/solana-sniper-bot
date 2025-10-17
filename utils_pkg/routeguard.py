# utils_pkg/routeguard.py
from __future__ import annotations

import os
import time
from typing import Any, Dict, Optional, Tuple, Iterable

from logging_setup import get_logger
from utils_pkg.router import (
    get_buy_quote,
    get_sell_quote,
    normalize_pump_id_to_mint,
    is_valid_pubkey_str,  # public alias from router
)

log = get_logger()

# -----------------------------------------------------------------------------
# Logger compatibility: treat .every as .event if missing
# -----------------------------------------------------------------------------
if not hasattr(log, "every"):
    def _every(_key: str, n: int = 10, msg: Optional[str] = None, **kw):
        return log.event(msg or _key, **kw)
    setattr(log, "every", _every)  # type: ignore[attr-defined]

# -----------------------------------------------------------------------------
# Env helpers
# -----------------------------------------------------------------------------
def _envb(k: str, d: bool = False) -> bool:
    v = (os.getenv(k, "") or "").strip().lower()
    if not v:
        return d
    return v in {"1", "true", "yes", "y", "on"}

def _envi(k: str, d: int) -> int:
    try:
        return int(os.getenv(k, str(d)))
    except Exception:
        return int(d)

def _envf(k: str, d: float) -> float:
    try:
        return float(os.getenv(k, str(d)))
    except Exception:
        return float(d)

def _envs(k: str, d: str) -> str:
    return os.getenv(k, d)

# -----------------------------------------------------------------------------
# Parse a ladder like "50k,200k,1m" → [50000, 200000, 1000000]
# -----------------------------------------------------------------------------
def _parse_raw_ladder(v: str) -> list[int]:
    out: list[int] = []
    for part in (v or "").replace(" ", "").split(","):
        if not part:
            continue
        mult = 1
        p = part.lower()
        if p.endswith("k"):
            mult = 1_000
            p = p[:-1]
        elif p.endswith("m"):
            mult = 1_000_000
            p = p[:-1]
        try:
            n = int(float(p) * mult)
        except Exception:
            continue
        if n > 0:
            out.append(n)
    return out

# -----------------------------------------------------------------------------
# Policy / knobs (aligned with trader & router defaults)
# -----------------------------------------------------------------------------
ALLOW_NO_BUY_ROUTE   = _envb("ROUTEGUARD_ALLOW_NO_ROUTE", True)
REQUIRE_SELL_ROUTE   = _envb("ROUTEGUARD_REQUIRE_SELL", True)

# Use router's quote TTL if present, else fall back to PF_PRICE_TTL_SEC
QUOTE_TTL_SEC        = max(1, _envi("ROUTER_QUOTE_TTL_SEC", _envi("PF_PRICE_TTL_SEC", 5)))

# Guard: block extreme price impact (bps). 0 disables.
MAX_IMPACT_BPS       = max(0, _envi("ROUTEGUARD_MAX_IMPACT_BPS", 1500))

# Sell-probe ladder (raw token units). Tries in order until one works.
# Defaults are robust for 6–9 decimals without being dust.
_SELL_LADDER_ENV     = _envs("ROUTEGUARD_SELL_PROBE_LADDER", "50k,500k,5m")
SELL_PROBE_LADDER    = _parse_raw_ladder(_SELL_LADDER_ENV) or [50_000, 500_000, 5_000_000]

# Hard floor for any single probe (just in case custom ladder is too tiny)
SELL_PROBE_FLOOR_RAW = max(50_000, _envi("ROUTEGUARD_SELL_PROBE_FLOOR_RAW", 50_000))

# Per-mint cooldown when no Jupiter route is available
NOROUTE_COOLDOWN_SEC = max(60, _envi("NOROUTE_COOLDOWN_SEC", _envi("ROUTEGUARD_NOROUTE_COOLDOWN_SEC", 600)))

# (Optional) cap for in-memory caches to avoid unlimited growth
CACHE_MAX_KEYS       = max(100, _envi("ROUTEGUARD_CACHE_MAX_KEYS", 2000))

# -----------------------------------------------------------------------------
# Small caches
# -----------------------------------------------------------------------------
# key: (normalized_mint, lamports_in) -> (ts, quote_dict)
_buy_cache: dict[tuple[str, int], tuple[float, Dict[str, Any]]] = {}
# normalized_mint -> resume_ts
_noroute_until: dict[str, float] = {}

def clear_cache() -> None:
    _buy_cache.clear()
    _noroute_until.clear()

def _trim_cache_if_needed() -> None:
    if len(_buy_cache) > CACHE_MAX_KEYS:
        # drop oldest ~20% entries
        cutoff = int(len(_buy_cache) * 0.2)
        for k, _ in sorted(_buy_cache.items(), key=lambda kv: kv[1][0])[:cutoff]:
            _buy_cache.pop(k, None)

def _get_cached_buy(norm_mint: str, lamports_in: int) -> Optional[Dict[str, Any]]:
    key = (norm_mint, int(lamports_in))
    rec = _buy_cache.get(key)
    if not rec:
        return None
    ts, q = rec
    if (time.time() - ts) <= QUOTE_TTL_SEC:
        return q
    _buy_cache.pop(key, None)
    return None

def _put_cached_buy(norm_mint: str, lamports_in: int, quote: Dict[str, Any]) -> None:
    _buy_cache[(norm_mint, int(lamports_in))] = (time.time(), quote)
    _trim_cache_if_needed()

def _cooldown_ok(norm_mint: str) -> bool:
    return time.time() >= _noroute_until.get(norm_mint, 0.0)

def _note_no_route(norm_mint: str) -> None:
    _noroute_until[norm_mint] = time.time() + NOROUTE_COOLDOWN_SEC

def _impact_bps(q: Dict[str, Any]) -> int:
    """
    Jupiter v6 returns priceImpactPct as a fraction (e.g. 0.0123 = 1.23%).
    Convert to BPS for easier thresholding.
    """
    try:
        pip = float(q.get("priceImpactPct") or 0.0)
        return int(abs(pip) * 10_000)
    except Exception:
        return 0

def _ladder_with_floor(raws: Iterable[int]) -> list[int]:
    # ensure we never probe below the floor
    return [max(int(x), SELL_PROBE_FLOOR_RAW) for x in raws if int(x) > 0]

# -----------------------------------------------------------------------------
# Public API
# -----------------------------------------------------------------------------
async def preflight(*, mint: str, lamports_in: int) -> Tuple[bool, Optional[Dict[str, Any]]]:
    """
    Decide if we should attempt a Jupiter buy for `mint` with `lamports_in`.

    Returns:
      (True, quote)   → proceed with Jupiter using this quote (fresh or cached)
      (True, None)    → allowed to buy *without* Jupiter route (fallback path OK)
      (False, _)      → blocked (route missing/bad, high impact, or cooldown)

    Notes:
      • Accepts Pump Portal ids like '...pump' / '...bonk'. We normalize to a
        real mint for routing checks, but if the *raw* input is not a valid
        32-byte base58 pubkey, we FORCE fallback by returning (True, None) so
        the trader will use PumpPortal instead of Jupiter (prevents a later
        swap failure when Jupiter sees a non-mint id).
      • We cache buy quotes briefly to save credits and back off noisy mints
        with a per-mint no-route cooldown.
    """
    lamports_in = int(lamports_in)
    if lamports_in <= 0:
        log.event("RG_BAD_AMOUNT", mint=mint, lamports_in=lamports_in)
        return False, None

    # 1) Normalize the id to a real mint for quoting / liquidity checks
    try:
        norm = normalize_pump_id_to_mint(mint)
    except Exception:
        # Can't normalize → no Jupiter routing. Allow fallback if permitted.
        if ALLOW_NO_BUY_ROUTE:
            log.event("RG_NORMALIZE_FAIL_ALLOW", raw=mint)
            return True, None
        log.event("RG_NORMALIZE_FAIL_BLOCK", raw=mint)
        return False, None

    # 2) If we're in per-mint cooldown, block quickly
    if not _cooldown_ok(norm):
        log.event("RG_COOLDOWN", mint=norm, secs_left=int(_noroute_until[norm] - time.time()))
        return False, None

    # 3) If the RAW input isn't a valid mint string, force PumpPortal fallback.
    #    (Even if Jupiter would work with the normalized mint, the trader will
    #     forward the original raw string to buy_via_lightning; forcing fallback
    #     here avoids a failed Jupiter swap later.)
    raw_is_valid_mint = is_valid_pubkey_str(mint)
    force_fallback_only = not raw_is_valid_mint

    # 4) Optionally run Jupiter checks on the normalized mint to guard liquidity
    q_buy = _get_cached_buy(norm, lamports_in)
    if q_buy is None:
        try:
            q_buy = await get_buy_quote(mint=norm, lamports_in=lamports_in)
            _put_cached_buy(norm, lamports_in, q_buy)
            log.every(
                "rg.buy", n=12, msg="RG_BUY_QUOTE_OK",
                mint=norm, lamports_in=lamports_in,
                out_raw=int(q_buy.get("outAmount", 0)),
                impact_bps=_impact_bps(q_buy),
            )
        except Exception as e:
            if not ALLOW_NO_BUY_ROUTE:
                _note_no_route(norm)
                log.event("RG_BUY_QUOTE_FAIL_BLOCK", mint=norm, err=str(e))
                return False, None
            # allow fallback path (PumpPortal)
            log.event("RG_BUY_QUOTE_FAIL_ALLOW", mint=norm, err=str(e))
            return True, None

    # Non-zero outAmount?
    out_raw = int(q_buy.get("outAmount", 0) or 0)
    if out_raw <= 0:
        if not ALLOW_NO_BUY_ROUTE:
            _note_no_route(norm)
            log.event("RG_NO_BUY_ROUTE", mint=norm)
            return False, None
        log.event("RG_NO_BUY_ROUTE_ALLOW", mint=norm)
        return True, None

    # Impact guard
    ibps = _impact_bps(q_buy)
    if MAX_IMPACT_BPS and ibps > MAX_IMPACT_BPS:
        _note_no_route(norm)
        log.event("RG_BLOCK_IMPACT", mint=norm, impact_bps=ibps, max_bps=MAX_IMPACT_BPS)
        return False, None

    # 5) Require a tiny sell path? Use a probe ladder to avoid 400s on dust.
    if REQUIRE_SELL_ROUTE:
        ladder = _ladder_with_floor(SELL_PROBE_LADDER)
        sell_ok = False
        sell_err: Optional[str] = None
        for probe_raw in ladder:
            try:
                q_sell = await get_sell_quote(mint=norm, amount_raw=int(probe_raw))
                lam = int(q_sell.get("outAmount", 0) or q_sell.get("lamports_out", 0))
                if lam > 0:
                    log.every("rg.sell", n=20, msg="RG_SELL_ROUTE_OK",
                              mint=norm, probe_raw=int(probe_raw), lamports_out=lam)
                    sell_ok = True
                    break
                else:
                    sell_err = f"zero_out (probe_raw={probe_raw})"
                    # try next size
            except Exception as e:
                sell_err = str(e)
                # try next size in ladder
        if not sell_ok:
            _note_no_route(norm)
            log.event("RG_SELL_ROUTE_FAIL", mint=norm, err=sell_err, ladder=ladder)
            return False, None

    # 6) If we need to force fallback because the raw id isn't a mint, do it now.
    if force_fallback_only:
        log.event("RG_FORCE_FALLBACK_RAW_NOT_MINT", raw=mint, norm=norm)
        return True, None

    # 7) Otherwise we’re good to use Jupiter directly with this buy quote
    return True, q_buy


__all__ = ["preflight", "clear_cache"]
