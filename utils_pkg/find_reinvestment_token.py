from __future__ import annotations

"""
utils_pkg/find_reinvestment_token.py

Quant-grade reinvestment candidate selector (V3+++)
===================================================

Responsibilities
----------------
  • Pull fresh pairs from Dexscreener (Solana).
  • Filter by:
        - minimum 24h volume (USD)
        - minimum liquidity (USD)
        - age window (avoid both brand-new & ancient / dead pairs)
        - optional honeypot check
  • Optionally boost pairs with whale / smart-money bias.
  • Enforce AI fraud / safety gate (ai_fraud_score >= HOLD_IF_SCORE_ABOVE).
  • Optionally enforce AI buy-score gate (if ai_buy_scoring is available).
  • Use Redis to avoid re-picking recently tried mints.
  • Make risk-aware tweaks based on global risk / regime context.
  • Return a mint address suitable for reinvestment.

Primary public API
------------------
    async def find_reinvestment_token(
        redis=None,
        min_volume: float | None = None,
        honeypot_filter: bool | None = None,
        top_n: int = 10,
    ) -> str | None

Additional helper for backtests / dashboards
--------------------------------------------
    async def preview_reinvestment_candidates(
        redis=None,
        min_volume: float | None = None,
        honeypot_filter: bool | None = None,
        top_n: int = 10,
        limit_return: int = 5,
    ) -> list[dict]

      → Returns rich metadata for the top candidates (but does NOT trade).
"""

import logging
import math
import os
import time
from dataclasses import dataclass, asdict
from typing import Any, Dict, List, Optional, Tuple

from utils_pkg.core_utils import (
    session,        # shared aiohttp.ClientSession (init_resources)
    ai_fraud_score, # async def ai_fraud_score(mint) -> 0..1 or None
)

# ---------------------------------------------------------------------------
# Config / env fallbacks
# ---------------------------------------------------------------------------

try:
    # Prefer config-level knobs if present
    from config import (  # type: ignore
        REINVESTMENT_VOLUME_MIN,
        REINVESTMENT_HONEYPOT_CHECK,
        HOLD_IF_SCORE_ABOVE,
        ENABLE_WHALE_FILTERS,
        DEXES,
    )
except Exception:  # pragma: no cover
    REINVESTMENT_VOLUME_MIN = float(os.getenv("REINVESTMENT_VOLUME_MIN", "5000"))
    REINVESTMENT_HONEYPOT_CHECK = (
        os.getenv("REINVESTMENT_HONEYPOT_CHECK", "true").strip().lower()
        in ("1", "true", "yes", "on")
    )
    HOLD_IF_SCORE_ABOVE = float(os.getenv("HOLD_IF_SCORE_ABOVE", "0.70"))
    ENABLE_WHALE_FILTERS = (
        os.getenv("ENABLE_WHALE_FILTERS", "true").strip().lower()
        in ("1", "true", "yes", "on")
    )
    # default DEX preference only used for logging/metadata
    DEXES: List[str] = ["raydium"]

# Additional env knobs specific to this selector
REINVESTMENT_MIN_LIQ_USD = float(os.getenv("REINVESTMENT_MIN_LIQ_USD", "3000"))
REINVESTMENT_PREF_LIQ_USD = float(os.getenv("REINVESTMENT_PREF_LIQ_USD", "50000"))
REINVESTMENT_MAX_AGE_SEC = float(os.getenv("REINVESTMENT_MAX_AGE_SEC", str(7 * 24 * 3600)))
REINVESTMENT_MIN_AGE_SEC = float(os.getenv("REINVESTMENT_MIN_AGE_SEC", "300"))  # 5 min

# AI gate behaviour when the service fails
REINVESTMENT_AI_STRICT = (
    os.getenv("REINVESTMENT_AI_STRICT", "1").strip().lower()
    in ("1", "true", "yes", "on")
)

# Optional AI buy-scoring gate
REINVESTMENT_USE_AI_BUY_SCORE = (
    os.getenv("REINVESTMENT_USE_AI_BUY_SCORE", "1").strip().lower()
    in ("1", "true", "yes", "on")
)
REINVESTMENT_AI_BUY_MIN_SCORE = float(
    os.getenv("REINVESTMENT_AI_BUY_MIN_SCORE", "0.40")
)

# recent-pick TTL to avoid looping the same mint (seconds)
RECENT_PICK_TTL = int(os.getenv("REINVEST_RECENT_PICK_TTL", str(30 * 60)))  # 30 minutes

# minimal liquidity sanity (USD) if config-level not present
MIN_LIQ_USD = REINVESTMENT_MIN_LIQ_USD

# composite score weights (sum ~ 1.0)
W_M5 = 0.30
W_H1 = 0.25
W_H6 = 0.15
W_VOL = 0.20
W_LIQ = 0.05
W_FLOW = 0.05  # microstructure / flow

DEXSCREENER_PAIRS_URL = "https://api.dexscreener.io/latest/dex/pairs/solana"

log = logging.getLogger("reinvest_selector")
if not log.handlers:
    logging.basicConfig(level=logging.INFO)

# ---------------------------------------------------------------------------
# Optional modules: honeypot, whale bias, risk manager, AI buy scoring
# ---------------------------------------------------------------------------

# Honeypot helper (optional)
try:
    from utils_pkg.honeypot import is_honeypot  # async def is_honeypot(mint) -> bool
except Exception:  # pragma: no cover

    async def is_honeypot(_mint: str) -> bool:
        return False

# Whale/smart-money detector
try:
    # e.g. scanner.dexscreener_scanner.has_whale_bias(pair) -> bool
    from scanner.dexscreener_scanner import has_whale_bias  # type: ignore
except Exception:  # pragma: no cover

    async def has_whale_bias(pair: dict) -> bool:
        """
        Fallback heuristic: prefer pairs that have clearly more buys than sells
        in the last hour.
        """
        tx = (pair.get("txns") or {}).get("h1") or {}
        return (tx.get("buys", 0) - tx.get("sells", 0)) > 5

# Risk context (for regime-aware tweaks)
try:
    from utils_pkg.risk_manager import risk_capacity_snapshot  # type: ignore
except Exception:  # pragma: no cover

    async def risk_capacity_snapshot(_redis=None) -> Dict[str, Any]:
        return {
            "risk_capacity": 1.0,
            "regime_label_numeric": 0.0,
            "regime_stress_score": 0.0,
        }

# Optional AI buy-scoring hook
try:
    from utils_pkg.ai_buy_scoring import (  # type: ignore
        Candidate as BuyCandidate,
        score_only as ai_buy_score_only,
    )
except Exception:  # pragma: no cover
    BuyCandidate = None  # type: ignore[assignment]

    async def ai_buy_score_only(*_args: Any, **_kwargs: Any) -> float:
        return 0.0  # effectively "disabled"


# ---------------------------------------------------------------------------
# Dataclass: internal candidate representation
# ---------------------------------------------------------------------------

@dataclass
class ReinvestCandidate:
    mint: str
    symbol: str
    pair_address: str
    dex: str

    vol24_usd: float
    liq_usd: float
    price_change_m5: float
    price_change_h1: float
    price_change_h6: float

    buys_h1: int
    sells_h1: int
    buys_m5: int
    sells_m5: int

    age_sec: Optional[float]

    composite_score: float
    whale_bias: bool = False
    ai_fraud_score_val: Optional[float] = None
    ai_buy_score: Optional[float] = None

    def to_preview_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        # Small rounding to keep logs / Notion tidy
        for k in ("composite_score", "ai_fraud_score_val", "ai_buy_score"):
            if d.get(k) is not None:
                d[k] = round(float(d[k]), 4)
        d["url"] = f"https://dexscreener.com/solana/{self.pair_address}"
        return d


# ---------------------------------------------------------------------------
# Small helpers
# ---------------------------------------------------------------------------

def _pct(v: Any) -> float:
    try:
        return float(v)
    except Exception:
        return 0.0


def _nz_num(v: Any) -> float:
    try:
        return float(v or 0.0)
    except Exception:
        return 0.0


def _log_norm(x: float, k: float = 250_000.0) -> float:
    """
    Cheap volume normalizer: log(1 + x) / log(1 + k).
    For x = k -> score ~ 1. For x << k -> score in (0, 1).
    """
    if x <= 0:
        return 0.0
    return math.log1p(x) / math.log1p(k)


def _liq_score(liq_usd: float) -> float:
    """
    Liquidity score in [0,1]. ~1.0 around REINVESTMENT_PREF_LIQ_USD, 0 near MIN_LIQ_USD.
    """
    if liq_usd <= 0:
        return 0.0
    upper = max(REINVESTMENT_PREF_LIQ_USD, MIN_LIQ_USD * 2.0)
    return max(0.0, min(1.0, liq_usd / upper))


def _age_seconds(pair: dict) -> Optional[float]:
    """
    Dexscreener pairs sometimes expose "age" in seconds or a timestamp.
    We treat anything non-parsable as None and let filters decide.
    """
    age_raw = pair.get("age")
    if age_raw is None:
        return None
    try:
        return float(age_raw)
    except Exception:
        return None


def _flow_score(pair: dict) -> float:
    """
    Microstructure / flow score based on txns.h1 & txns.m5.
    """
    tx = pair.get("txns") or {}
    h1 = tx.get("h1") or {}
    m5 = tx.get("m5") or {}

    buys_h1 = int(h1.get("buys", 0) or 0)
    sells_h1 = int(h1.get("sells", 0) or 0)
    buys_m5 = int(m5.get("buys", 0) or 0)
    sells_m5 = int(m5.get("sells", 0) or 0)

    total_h1 = max(1, buys_h1 + sells_h1)
    imb_h1 = (buys_h1 - sells_h1) / total_h1

    total_m5 = max(1, buys_m5 + sells_m5)
    imb_m5 = (buys_m5 - sells_m5) / total_m5

    # blend hour and 5m imbalance; clamp to [-1,1] then map → [0,1]
    imb = max(-1.0, min(1.0, 0.6 * imb_h1 + 0.4 * imb_m5))
    return 0.5 + 0.5 * imb  # -1→0, 0→0.5, +1→1


def _composite_score(pair: dict) -> Tuple[float, Dict[str, float]]:
    """
    Momentum + volume + liquidity + flow composite score.

    Returns:
        (score, components_dict)
    """
    pc = pair.get("priceChange") or {}
    m5 = _pct(pc.get("m5"))
    h1 = _pct(pc.get("h1"))
    h6 = _pct(pc.get("h6"))

    vol_24 = _nz_num((pair.get("volume") or {}).get("h24"))
    liq_usd = _nz_num(((pair.get("liquidity") or {}).get("usd")))

    vol_s = _log_norm(vol_24, k=250_000.0)
    liq_s = _liq_score(liq_usd)
    flow_s = _flow_score(pair)

    # Normalize pct moves to [-1, 1]
    m5_s = max(-2.0, min(2.0, m5 / 15.0)) / 2.0
    h1_s = max(-2.0, min(2.0, h1 / 20.0)) / 2.0
    h6_s = max(-2.0, min(2.0, h6 / 40.0)) / 2.0

    score = (
        W_M5 * m5_s
        + W_H1 * h1_s
        + W_H6 * h6_s
        + W_VOL * vol_s
        + W_LIQ * liq_s
        + W_FLOW * flow_s
    )

    return float(score), {
        "m5_s": float(m5_s),
        "h1_s": float(h1_s),
        "h6_s": float(h6_s),
        "vol_s": float(vol_s),
        "liq_s": float(liq_s),
        "flow_s": float(flow_s),
    }


# ---------------------------------------------------------------------------
# Fetch & filter
# ---------------------------------------------------------------------------

async def _fetch_pairs() -> List[dict]:
    if session is None:
        log.error("HTTP session is not initialized. Call init_resources() first.")
        return []
    try:
        async with session.get(DEXSCREENER_PAIRS_URL, timeout=10) as r:
            if r.status != 200:
                txt = await r.text()
                log.warning(
                    "Dexscreener pairs status %s: %s",
                    r.status,
                    txt[:240],
                )
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
    risk_ctx: Optional[Dict[str, Any]],
) -> List[ReinvestCandidate]:
    """
    Hard filter + composite scoring, but *without* AI gates or Redis dedupe.
    """
    out: List[ReinvestCandidate] = []
    now = time.time()

    # Risk-aware tweaks: in bearish / stressed regimes, we favour higher liq/vol.
    regime_num = 0.0
    stress = 0.0
    if risk_ctx:
        regime_num = float(risk_ctx.get("regime_label_numeric", 0.0) or 0.0)
        stress = float(risk_ctx.get("regime_stress_score", 0.0) or 0.0)

    liq_floor = MIN_LIQ_USD
    vol_floor = min_volume
    if regime_num < -0.3 or stress > 0.7:
        liq_floor *= 1.5
        vol_floor *= 1.5
    elif regime_num > 0.4 and stress < 0.4:
        liq_floor *= 0.8  # slightly more open when regime is strong/bullish

    for p in pairs:
        try:
            base = p.get("baseToken") or {}
            mint = (base.get("address") or "").strip()
            symbol = (base.get("symbol") or "").strip()
            if not mint:
                continue

            vol_24 = _nz_num((p.get("volume") or {}).get("h24"))
            if vol_24 < vol_floor:
                continue

            liq_usd = _nz_num(((p.get("liquidity") or {}).get("usd")))
            if liq_usd < liq_floor:
                continue

            age_sec = _age_seconds(p)
            if age_sec is not None:
                if age_sec < REINVESTMENT_MIN_AGE_SEC:
                    # Too new / can be sniped elsewhere via normal entry logic
                    continue
                if age_sec > REINVESTMENT_MAX_AGE_SEC:
                    # Too old / likely "grindy", not ideal for reinvest rockets
                    continue

            # Prefer SOL / USDC / USDT quote; others allowed but naturally rare
            qsym = ((p.get("quoteToken") or {}).get("symbol") or "").upper()
            if qsym not in ("SOL", "USDC", "USDT"):
                # keep, but they will naturally have worse liquidity/volume scores
                pass

            if require_honeypot_check:
                try:
                    if await is_honeypot(mint):
                        log.info("Skip honeypot: %s (%s)", symbol or mint[:6], mint)
                        continue
                except Exception as he:
                    log.warning("Honeypot check failed for %s: %s", mint, he)

            comp_score, _components = _composite_score(p)

            # Flow stats
            tx = p.get("txns") or {}
            h1 = tx.get("h1") or {}
            m5 = tx.get("m5") or {}

            buys_h1 = int(h1.get("buys", 0) or 0)
            sells_h1 = int(h1.get("sells", 0) or 0)
            buys_m5 = int(m5.get("buys", 0) or 0)
            sells_m5 = int(m5.get("sells", 0) or 0)

            pair_addr = p.get("pairAddress") or mint

            cand = ReinvestCandidate(
                mint=mint,
                symbol=symbol or mint[:6],
                pair_address=pair_addr,
                dex=(DEXES[0] if DEXES else "raydium"),
                vol24_usd=vol_24,
                liq_usd=liq_usd,
                price_change_m5=_pct((p.get("priceChange") or {}).get("m5")),
                price_change_h1=_pct((p.get("priceChange") or {}).get("h1")),
                price_change_h6=_pct((p.get("priceChange") or {}).get("h6")),
                buys_h1=buys_h1,
                sells_h1=sells_h1,
                buys_m5=buys_m5,
                sells_m5=sells_m5,
                age_sec=age_sec,
                composite_score=comp_score,
            )
            out.append(cand)
        except Exception:
            # ignore malformed pairs
            continue

    return out


async def _rank_candidates(cands: List[ReinvestCandidate]) -> List[ReinvestCandidate]:
    """
    Apply whale boosts and sort descending by composite score.
    """
    ranked: List[ReinvestCandidate] = []

    for cand in cands:
        score = cand.composite_score

        # Optional whale / smart-money boost
        if ENABLE_WHALE_FILTERS:
            try:
                # We need the raw Dexscreener pair for this; approximation:
                # we can't easily pass it here, so treat whale_bias via txns.
                # (Flow-heavy in buys_h1 vs sells_h1 acts as a fallback).
                whale = False
                # If txns show strong sustained net buys, treat as whale_bias
                total = max(1, cand.buys_h1 + cand.sells_h1)
                imb = (cand.buys_h1 - cand.sells_h1) / total
                if imb > 0.25 and cand.buys_h1 > 10:
                    whale = True
                # If scanner.dexscreener_scanner.has_whale_bias is available,
                # it will be used upstream rather than here (more accurate).
                cand.whale_bias = whale
                if whale:
                    score *= 1.10  # gentle boost
            except Exception:
                pass

        cand.composite_score = score
        ranked.append(cand)

    ranked.sort(key=lambda c: c.composite_score, reverse=True)
    return ranked


# ---------------------------------------------------------------------------
# AI gates
# ---------------------------------------------------------------------------

async def _ai_risk_gate(mint: str) -> Tuple[bool, Optional[float]]:
    """
    Enforce AI fraud / safety threshold.

    ai_fraud_score(mint) is expected to return 0..1 where higher is safer.
    """
    try:
        score = await ai_fraud_score(mint)
        if score is None:
            if REINVESTMENT_AI_STRICT:
                return False, None
            # non-strict mode: allow if AI is down
            return True, None

        score_f = float(score)
        safe = score_f >= float(HOLD_IF_SCORE_ABOVE)
        if not safe:
            log.info(
                "AI risk gate rejected %s (score=%.3f, threshold=%.3f)",
                mint,
                score_f,
                float(HOLD_IF_SCORE_ABOVE),
            )
        return safe, score_f
    except Exception as e:
        log.warning("AI risk gate failed for %s: %s", mint, e)
        if REINVESTMENT_AI_STRICT:
            return False, None
        return True, None


async def _ai_buy_gate(cand: ReinvestCandidate) -> Tuple[bool, Optional[float]]:
    """
    Optional AI entry/buy-score gate, reusing ai_buy_scoring if available.

    Returns (ok, score_or_none).
    """
    if not REINVESTMENT_USE_AI_BUY_SCORE or BuyCandidate is None:
        return True, None

    try:
        # Build minimal candidate for AI buy scoring; we don't need full FeatureBuilder
        bc = BuyCandidate(
            mint=cand.mint,
            symbol=cand.symbol,
            dex=cand.dex,
            liq_usd=cand.liq_usd,
            vol24_usd=cand.vol24_usd,
            m5_change=cand.price_change_m5,
            h1_change=cand.price_change_h1,
            h6_change=cand.price_change_h6,
            buys_1h=cand.buys_h1,
            sells_1h=cand.sells_h1,
            buys_5m=cand.buys_m5,
            sells_5m=cand.sells_m5,
            # Any additional fields are optional and default inside ai_buy_scoring
        )
        score = await ai_buy_score_only(bc)  # 0..1
        if score is None:
            return True, None

        sf = float(score)
        ok = sf >= REINVESTMENT_AI_BUY_MIN_SCORE
        if not ok:
            log.info(
                "AI buy-score gate rejected %s (score=%.3f < min=%.3f)",
                cand.mint,
                sf,
                REINVESTMENT_AI_BUY_MIN_SCORE,
            )
        return ok, sf
    except Exception as e:
        log.warning("AI buy-score gate failed for %s: %s", cand.mint, e)
        # On failure, we don't want to hard-block reinvestment if AI is flaky.
        return True, None


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

async def find_reinvestment_token(
    redis=None,
    min_volume: Optional[float] = None,
    honeypot_filter: Optional[bool] = None,
    top_n: int = 10,
) -> Optional[str]:
    """
    Pick a single token mint for reinvestment.

    Returns:
        mint (str) on success, or None if nothing passed filters.
    """
    mv = float(min_volume if min_volume is not None else REINVESTMENT_VOLUME_MIN)
    hp = bool(
        honeypot_filter
        if honeypot_filter is not None
        else REINVESTMENT_HONEYPOT_CHECK
    )

    # Risk snapshot (for regime-aware filters)
    risk_ctx: Optional[Dict[str, Any]] = None
    try:
        risk_ctx = await risk_capacity_snapshot(redis)
    except Exception:
        risk_ctx = None

    pairs = await _fetch_pairs()
    if not pairs:
        return None

    # 1) basic filters (volume/liquidity/age/honeypot) + composite score build
    candidates = await _filter_candidates(
        pairs,
        min_volume=mv,
        require_honeypot_check=hp,
        risk_ctx=risk_ctx,
    )
    if not candidates:
        log.info("No pairs passed initial filters (volume/liquidity/age/honeypot).")
        return None

    # 2) rank by composite score (+ internal whale & flow boosts)
    ranked = await _rank_candidates(candidates)
    if not ranked:
        return None

    # 3) walk top-N and pick first that passes AI risk gate + AI buy gate + Redis dedupe
    limit = max(5, int(top_n))

    for idx, cand in enumerate(ranked[:limit]):
        mint = cand.mint

        # redis dedupe: skip if we picked this recently
        if redis is not None:
            key = f"reinvest:recent:{mint}"
            try:
                if await redis.exists(key):
                    log.info("Skip reinvest (recent): %s", mint)
                    continue
            except Exception:
                pass

        # AI safety / fraud gate
        ok_fraud, fraud_score = await _ai_risk_gate(mint)
        cand.ai_fraud_score_val = fraud_score
        if not ok_fraud:
            continue

        # AI buy-score gate (optional)
        ok_buy, buy_score = await _ai_buy_gate(cand)
        cand.ai_buy_score = buy_score
        if not ok_buy:
            continue

        # success: mark as recent & log, then return mint
        if redis is not None:
            try:
                await redis.setex(f"reinvest:recent:{mint}", RECENT_PICK_TTL, "1")
            except Exception:
                pass

        log.info(
            "[REINVEST_PICK] rank=%d mint=%s sym=%s score=%.4f vol24=%.1f liq=%.1f "
            "m5=%.1f h1=%.1f h6=%.1f buys1h=%d sells1h=%d ai_fraud=%.3f ai_buy=%.3f "
            "dex=%s url=%s",
            idx + 1,
            cand.mint,
            cand.symbol,
            cand.composite_score,
            cand.vol24_usd,
            cand.liq_usd,
            cand.price_change_m5,
            cand.price_change_h1,
            cand.price_change_h6,
            cand.buys_h1,
            cand.sells_h1,
            (cand.ai_fraud_score_val if cand.ai_fraud_score_val is not None else -1.0),
            (cand.ai_buy_score if cand.ai_buy_score is not None else -1.0),
            cand.dex,
            f"https://dexscreener.com/solana/{cand.pair_address}",
        )
        return mint

    # If we got here, nothing in top-N passed all the gates.
    log.info("No reinvestment candidate passed AI/risk/dedupe gates in top-N.")
    return None


async def preview_reinvestment_candidates(
    redis=None,
    min_volume: Optional[float] = None,
    honeypot_filter: Optional[bool] = None,
    top_n: int = 10,
    limit_return: int = 5,
) -> List[Dict[str, Any]]:
    """
    Backtest/debug helper: return rich metadata for the top candidates.

    DOES NOT change Redis recent keys and does NOT trade.

    Returns:
        list of dicts with fields:
          - mint, symbol, url, composite_score, ai_fraud_score_val,
            ai_buy_score, vol24_usd, liq_usd, price_change_m5/1h/6h,
            buys/sells stats, age_sec, whale_bias, rank, skipped_recent,
            passed_ai_fraud, passed_ai_buy.
    """
    mv = float(min_volume if min_volume is not None else REINVESTMENT_VOLUME_MIN)
    hp = bool(
        honeypot_filter
        if honeypot_filter is not None
        else REINVESTMENT_HONEYPOT_CHECK
    )

    try:
        risk_ctx = await risk_capacity_snapshot(redis)
    except Exception:
        risk_ctx = None

    pairs = await _fetch_pairs()
    if not pairs:
        return []

    cands = await _filter_candidates(
        pairs,
        min_volume=mv,
        require_honeypot_check=hp,
        risk_ctx=risk_ctx,
    )
    ranked = await _rank_candidates(cands)
    if not ranked:
        return []

    out: List[Dict[str, Any]] = []
    limit = max(5, int(top_n))
    max_ret = max(1, int(limit_return))

    for idx, cand in enumerate(ranked[:limit]):
        mint = cand.mint

        skipped_recent = False
        passed_ai_fraud = False
        passed_ai_buy = False

        # recent dedupe INFO (but no writes)
        if redis is not None:
            key = f"reinvest:recent:{mint}"
            try:
                if await redis.exists(key):
                    skipped_recent = True
            except Exception:
                skipped_recent = False

        ok_fraud, fraud_score = await _ai_risk_gate(mint)
        cand.ai_fraud_score_val = fraud_score
        passed_ai_fraud = ok_fraud

        ok_buy, buy_score = await _ai_buy_gate(cand)
        cand.ai_buy_score = buy_score
        passed_ai_buy = ok_buy

        d = cand.to_preview_dict()
        d.update(
            {
                "rank": idx + 1,
                "skipped_recent": skipped_recent,
                "passed_ai_fraud": passed_ai_fraud,
                "passed_ai_buy": passed_ai_buy,
            }
        )
        out.append(d)

        if len(out) >= max_ret:
            break

    return out


__all__ = ["find_reinvestment_token", "preview_reinvestment_candidates"]


# ---------------------------------------------------------------------------
# CLI / quick manual test
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import asyncio
    from utils_pkg.core_utils import (
        init_resources,
        close_resources,
        redis as shared_redis,
    )

    async def _demo():
        await init_resources()
        try:
            print("=== Preview top candidates ===")
            preview = await preview_reinvestment_candidates(
                redis=shared_redis,
                limit_return=5,
            )
            for row in preview:
                print(row)

            print("\n=== Final pick ===")
            mint = await find_reinvestment_token(redis=shared_redis)
            print("PICK =", mint)
        finally:
            await close_resources()

    asyncio.run(_demo())
