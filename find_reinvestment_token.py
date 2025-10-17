# find_reinvestment_token.py
# Smart reinvestment selector:
# - pulls fresh pairs from Dexscreener (Solana)
# - filters by min volume/liquidity and optional honeypot check
# - ranks by momentum (m5/h1/h6) + normalized volume + liquidity health
# - enforces AI fraud-score threshold (HOLD_IF_SCORE_ABOVE)
# - supports Redis de-dupe so we don't pick recently tried tokens
# - returns a dict with rich context for logging/alerts

from __future__ import annotations

import asyncio
import logging
import math
import time
from typing import Dict, List, Optional, Tuple

from config import (
    REINVESTMENT_VOLUME_MIN,
    REINVESTMENT_HONEYPOT_CHECK,
    HOLD_IF_SCORE_ABOVE,
    ENABLE_WHALE_FILTERS,
    DEXES,
)

# we fetch the global session + helpers from core_utils
from utils_pkg.core_utils import session, ai_fraud_score  # session is initialized in init_resources()

# try to import your honeypot helper; fall back to a no-op if not present
try:
    from utils_pkg.honeypot import is_honeypot  # async def is_honeypot(mint) -> bool
except Exception:
    async def is_honeypot(token_mint: str) -> bool:
        return False

# optional: if you have a whale detector, wire it in
try:
    from scanner.dexscreener_scanner import has_whale_bias  # async def has_whale_bias(pair)->bool
except Exception:
    async def has_whale_bias(_pair: dict) -> bool:
        # basic heuristic if module not available: prefer more buys than sells in last hour
        tx = (_pair.get("txns") or {}).get("h1") or {}
        return (tx.get("buys", 0) - tx.get("sells", 0)) > 5

log = logging.getLogger("reinvest_selector")

DEXSCREENER_PAIRS_CHAIN = "https://api.dexscreener.io/latest/dex/pairs/solana"

# recent-pick TTL to avoid running in circles (seconds)
RECENT_PICK_TTL = 30 * 60  # 30 minutes

# minimal liquidity sanity (USD)
MIN_LIQ_USD = 3_000.0

# composite score weights (sum ~ 1.0)
W_M5 = 0.35
W_H1 = 0.25
W_H6 = 0.15
W_VOL = 0.20
W_LIQ = 0.05


def _pct(v) -> float:
    try:
        return float(v)
    except Exception:
        return 0.0


def _nz_num(v) -> float:
    try:
        return float(v or 0.0)
    except Exception:
        return 0.0


def _log_norm(x: float, k: float = 10_000.0) -> float:
    """
    Cheap volume normalizer: log(1 + x) / log(1 + k).
    For x=k -> score ~1. For x<<k -> score in (0,1).
    """
    if x <= 0:
        return 0.0
    return math.log1p(x) / math.log1p(k)


def _liq_score(liq_usd: float) -> float:
    # tiny smoothing — >=50k USD liquidity ~ 1.0, min_liq ~ 0.0-0.1
    if liq_usd <= 0:
        return 0.0
    return min(1.0, liq_usd / 50_000.0)


def _composite_score(pair: dict) -> float:
    pc = pair.get("priceChange") or {}
    m5 = _pct(pc.get("m5"))
    h1 = _pct(pc.get("h1"))
    h6 = _pct(pc.get("h6"))

    vol = (pair.get("volume") or {}).get("h24")
    vol = _nz_num(vol)

    liq = ((pair.get("liquidity") or {}).get("usd")) or 0
    liq = _nz_num(liq)

    # normalize volume; k is soft target ~ $250k so 250k -> ~0.8
    vol_s = _log_norm(vol, k=250_000.0)
    liq_s = _liq_score(liq)

    # final weighted score
    score = W_M5 * (m5 / 100.0) + W_H1 * (h1 / 100.0) + W_H6 * (h6 / 100.0) + W_VOL * vol_s + W_LIQ * liq_s
    return float(score)


async def _fetch_pairs() -> List[dict]:
    if not session:
        log.error("HTTP session is not initialized. Call init_resources() first.")
        return []
    try:
        async with session.get(DEXSCREENER_PAIRS_CHAIN, timeout=10) as r:
            if r.status != 200:
                txt = await r.text()
                log.warning("Dexscreener pairs status %s: %s", r.status, txt[:240])
                return []
            data = await r.json()
            return data.get("pairs") or []
    except Exception as e:
        log.exception("Dexscreener fetch failed: %s", e)
        return []


async def _filter_candidates(
    pairs: List[dict],
    min_volume: float,
    require_honeypot_check: bool,
) -> List[dict]:
    out = []
    for p in pairs:
        try:
            vol_24 = _nz_num((p.get("volume") or {}).get("h24"))
            if vol_24 < min_volume:
                continue

            liq = _nz_num(((p.get("liquidity") or {}).get("usd")))
            if liq < MIN_LIQ_USD:
                continue

            # prefer SOL pairs or USDC pools
            qsym = ((p.get("quoteToken") or {}).get("symbol") or "").upper()
            if qsym not in ("SOL", "USDC", "USDT"):
                # still allow, but lightly penalize later via score (low liquidity weight)
                pass

            base = p.get("baseToken") or {}
            mint = (base.get("address") or "").strip()
            symbol = (base.get("symbol") or "").strip()
            if not mint:
                continue

            if require_honeypot_check:
                try:
                    if await is_honeypot(mint):
                        log.info("Skip honeypot: %s (%s)", symbol, mint)
                        continue
                except Exception as he:
                    log.warning("Honeypot check failed for %s: %s", mint, he)

            out.append(p)
        except Exception:
            # ignore malformed pair
            continue
    return out


async def _rank_candidates(cands: List[dict]) -> List[Tuple[float, dict]]:
    ranked: List[Tuple[float, dict]] = []
    for p in cands:
        score = _composite_score(p)
        if ENABLE_WHALE_FILTERS:
            try:
                if await has_whale_bias(p):
                    score *= 1.10  # give a small boost
            except Exception:
                pass
        ranked.append((score, p))
    ranked.sort(key=lambda t: t[0], reverse=True)
    return ranked


async def _ai_risk_gate(mint: str) -> bool:
    """Enforce AI fraud/quality threshold."""
    try:
        score = await ai_fraud_score(mint)  # expected 0..1, higher = safer
        if score is None:
            return False
        return float(score) >= float(HOLD_IF_SCORE_ABOVE)
    except Exception as e:
        log.warning("AI risk gate failed for %s: %s", mint, e)
        # be conservative if AI is down
        return False


async def find_reinvestment_token(
    redis=None,
    min_volume: Optional[float] = None,
    honeypot_filter: Optional[bool] = None,
    top_n: int = 8,
) -> Optional[Dict]:
    """
    Returns dict like:
    {
        "mint": "...",
        "symbol": "TOKEN",
        "dex_url": "https://dexscreener.com/solana/<pairAddress>",
        "score": 0.42,
        "metrics": {
            "vol_h24": 12345.6,
            "liq_usd": 54321.0,
            "m5": 10.2, "h1": 25.5, "h6": 80.1
        },
        "reason": "momentum+volume+ai_pass"
    }
    """
    mv = float(min_volume if min_volume is not None else REINVESTMENT_VOLUME_MIN)
    hp = bool(honeypot_filter if honeypot_filter is not None else REINVESTMENT_HONEYPOT_CHECK)

    pairs = await _fetch_pairs()
    if not pairs:
        return None

    # 1) pre-filter
    filtered = await _filter_candidates(pairs, min_volume=mv, require_honeypot_check=hp)
    if not filtered:
        log.info("No pairs passed initial filters (volume/liquidity/honeypot).")
        return None

    # 2) rank
    ranked = await _rank_candidates(filtered)

    # 3) walk top-N and pick first that passes AI risk + redis dedupe
    now = int(time.time())
    for score, p in ranked[: max(5, top_n)]:
        base = p.get("baseToken") or {}
        mint = (base.get("address") or "").strip()
        symbol = (base.get("symbol") or "").strip() or mint[:6]
        pair_addr = p.get("pairAddress") or mint

        # redis de-dupe: skip if we just tried this recently
        if redis is not None:
            key = f"reinvest:recent:{mint}"
            try:
                if await redis.exists(key):
                    log.info("Skip reinvest (recent): %s", mint)
                    continue
            except Exception:
                pass

        # AI risk gate
        ok = await _ai_risk_gate(mint)
        if not ok:
            log.info("Skip (AI risk gate): %s", mint)
            continue

        # passed all gates — mark as recent and return
        if redis is not None:
            try:
                await redis.setex(f"reinvest:recent:{mint}", RECENT_PICK_TTL, "1")
            except Exception:
                pass

        vol_24 = _nz_num((p.get("volume") or {}).get("h24"))
        liq = _nz_num(((p.get("liquidity") or {}).get("usd")))
        pc = p.get("priceChange") or {}
        m5 = _pct(pc.get("m5"))
        h1 = _pct(pc.get("h1"))
        h6 = _pct(pc.get("h6"))

        return {
            "mint": mint,
            "symbol": symbol,
            "dex_url": f"https://dexscreener.com/solana/{pair_addr}",
            "score": round(float(score), 6),
            "metrics": {
                "vol_h24": vol_24,
                "liq_usd": liq,
                "m5": m5,
                "h1": h1,
                "h6": h6,
            },
            "reason": "momentum+volume+ai_pass",
            "preferred_dex": (DEXES[0] if DEXES else "jupiter"),
        }

    return None


# quick manual test
if __name__ == "__main__":
    import asyncio
    from utils_pkg.core_utils import init_resources, close_resources, redis as shared_redis

    async def _t():
        await init_resources()
        try:
            pick = await find_reinvestment_token(redis=shared_redis)
            print("PICK =", pick)
        finally:
            await close_resources()

    asyncio.run(_t())
