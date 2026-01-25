from __future__ import annotations

"""
utils_pkg/find_reinvestment_token.py

Quant-grade reinvestment candidate selector (V3+++ / production-first)
=====================================================================

High-level role
---------------
Select a *reinvestment mint* (Solana SPL token mint) from Dexscreener pairs with:
  - hard market sanity filters (liq/vol/age/tx activity)
  - optional honeypot + mint-guard checks
  - optional whale/smart-money bias
  - optional AI fraud safety gate (ai_fraud_score)
  - optional AI buy-score gate (ai_buy_scoring)
  - Redis dedupe so reinvestor doesn’t loop the same mint

This module is deliberately *conservative* (it’s for reinvesting profits, not sniping).
It must be reliable under partial dependency failures (Dexscreener slow, AI down, Redis down).

Primary public API
------------------
    async def find_reinvestment_token(
        redis=None,
        min_volume: float | None = None,
        honeypot_filter: bool | None = None,
        top_n: int = 10,
    ) -> str | None

Preview helper
--------------
    async def preview_reinvestment_candidates(
        redis=None,
        min_volume: float | None = None,
        honeypot_filter: bool | None = None,
        top_n: int = 10,
        limit_return: int = 5,
    ) -> list[dict]

Environment contract (module-local)
-----------------------------------
Core:
  - REINVESTMENT_ENABLED (default 1)
  - REINVESTMENT_VOLUME_MIN (fallback if config missing, default 5000)
  - REINVESTMENT_MIN_LIQ_USD (default 3000)
  - REINVESTMENT_PREF_LIQ_USD (default 50000)
  - REINVESTMENT_MIN_AGE_SEC (default 300)
  - REINVESTMENT_MAX_AGE_SEC (default 7d)
  - REINVESTMENT_PREF_AGE_SEC (default 6h)   # used only for scoring

Activity sanity:
  - REINVESTMENT_MIN_TX_H1 (default 8)
  - REINVESTMENT_MIN_BUYS_H1 (default 4)
  - REINVESTMENT_MIN_TX_M5 (default 1)       # avoids dead pairs

Selection:
  - REINVESTMENT_MIN_COMPOSITE (default 0.35) # score floor after normalization
  - REINVESTMENT_TOPN (default 10)
  - REINVEST_RECENT_PICK_TTL (default 1800)   # seconds

AI gates:
  - REINVESTMENT_AI_STRICT (default 1)              # if 1, AI failures block
  - HOLD_IF_SCORE_ABOVE (fallback if config missing, default 0.70)  # ai_fraud_score threshold (higher=safer)
  - REINVESTMENT_USE_AI_BUY_SCORE (default 1)
  - REINVESTMENT_AI_BUY_MIN_SCORE (default 0.40)
  - REINVESTMENT_AI_MAX_PARALLEL (default 6)        # bounded concurrency

HTTP:
  - REINVESTMENT_HTTP_TIMEOUT_SEC (default 10)
  - REINVESTMENT_HTTP_RETRIES (default 2)
  - REINVESTMENT_HTTP_BACKOFF_SEC (default 0.6)
  - REINVESTMENT_HTTP_USER_AGENT (default "sniper-reinvest/1.0")

Blacklists:
  - REINVESTMENT_SYMBOL_BLACKLIST (comma-separated symbols)
  - REINVESTMENT_MINT_BLACKLIST (comma-separated mints)

Notes
-----
- This module DOES NOT place trades. It only returns a mint to reinvest into.
- Prefer to pass an initialized Redis + core_utils resources (session) from the orchestrator.
"""

import asyncio
import contextlib
import math
import os
import random
import time
from dataclasses import asdict, dataclass
from typing import Any

# ──────────────────────────────────────────────────────────────────────────────
# Logging (shim-safe; root logging_setup is canonical)
# ──────────────────────────────────────────────────────────────────────────────
try:  # pragma: no cover
    from logging_setup import get_logger  # type: ignore
except Exception:  # pragma: no cover
    import logging

    def get_logger(name: str | None = None):  # type: ignore
        return logging.getLogger(name or "reinvest_selector")


log = get_logger(__name__)
if not hasattr(log, "event"):

    def _event(name: str, **kw: Any) -> None:  # pragma: no cover
        with contextlib.suppress(Exception):
            log.info("%s %s", name, kw)

    log.event = _event

if not hasattr(log, "every"):

    def _every(
        key: str, n: int = 10, msg: str | None = None, **kw: Any
    ) -> None:  # pragma: no cover
        try:
            now = time.monotonic()
            store = getattr(_every, "_store", {})  # type: ignore[attr-defined]
            last = float(store.get(key, 0.0))
            if (now - last) >= float(max(1, int(n))):
                store[key] = now
                _every._store = store  # type: ignore[attr-defined]
                log.event(msg or key, **kw)
        except Exception:
            return None

    log.every = _every


# ──────────────────────────────────────────────────────────────────────────────
# Config fallbacks (prefer config.py if present)
# ──────────────────────────────────────────────────────────────────────────────
def _envs(k: str, d: str = "") -> str:
    v = os.getenv(k)
    return (v if v is not None else d).strip()


def _envb(k: str, d: bool = False) -> bool:
    v = _envs(k, "")
    if v == "":
        return bool(d)
    s = v.lower()
    return s in {"1", "true", "yes", "y", "on"}


def _envi(k: str, d: int) -> int:
    try:
        return int(float(_envs(k, str(d))))
    except Exception:
        return int(d)


def _envf(k: str, d: float) -> float:
    try:
        return float(_envs(k, str(d)))
    except Exception:
        return float(d)


try:
    from config import (  # type: ignore
        DEXES,
        ENABLE_WHALE_FILTERS,
        HOLD_IF_SCORE_ABOVE,
        REINVESTMENT_HONEYPOT_CHECK,
        REINVESTMENT_VOLUME_MIN,
    )
except Exception:  # pragma: no cover
    REINVESTMENT_VOLUME_MIN = float(_envf("REINVESTMENT_VOLUME_MIN", 5000.0))
    REINVESTMENT_HONEYPOT_CHECK = _envb("REINVESTMENT_HONEYPOT_CHECK", True)
    HOLD_IF_SCORE_ABOVE = float(_envf("HOLD_IF_SCORE_ABOVE", 0.70))
    ENABLE_WHALE_FILTERS = _envb("ENABLE_WHALE_FILTERS", True)
    DEXES: list[str] = ["raydium"]

REINVESTMENT_ENABLED = _envb("REINVESTMENT_ENABLED", True)

REINVESTMENT_MIN_LIQ_USD = float(_envf("REINVESTMENT_MIN_LIQ_USD", 3000.0))
REINVESTMENT_PREF_LIQ_USD = float(_envf("REINVESTMENT_PREF_LIQ_USD", 50000.0))

REINVESTMENT_MAX_AGE_SEC = float(_envf("REINVESTMENT_MAX_AGE_SEC", float(7 * 24 * 3600)))
REINVESTMENT_MIN_AGE_SEC = float(_envf("REINVESTMENT_MIN_AGE_SEC", 300.0))  # 5 min
REINVESTMENT_PREF_AGE_SEC = float(_envf("REINVESTMENT_PREF_AGE_SEC", float(6 * 3600)))  # 6 hours

REINVESTMENT_AI_STRICT = _envb("REINVESTMENT_AI_STRICT", True)

REINVESTMENT_USE_AI_BUY_SCORE = _envb("REINVESTMENT_USE_AI_BUY_SCORE", True)
REINVESTMENT_AI_BUY_MIN_SCORE = float(_envf("REINVESTMENT_AI_BUY_MIN_SCORE", 0.40))

REINVESTMENT_MIN_TX_H1 = _envi("REINVESTMENT_MIN_TX_H1", 8)
REINVESTMENT_MIN_BUYS_H1 = _envi("REINVESTMENT_MIN_BUYS_H1", 4)
REINVESTMENT_MIN_TX_M5 = _envi("REINVESTMENT_MIN_TX_M5", 1)

REINVESTMENT_MIN_COMPOSITE = float(_envf("REINVESTMENT_MIN_COMPOSITE", 0.35))

RECENT_PICK_TTL = int(_envi("REINVEST_RECENT_PICK_TTL", 30 * 60))

AI_MAX_PARALLEL = max(1, _envi("REINVESTMENT_AI_MAX_PARALLEL", 6))

HTTP_TIMEOUT = float(_envf("REINVESTMENT_HTTP_TIMEOUT_SEC", 10.0))
HTTP_RETRIES = max(0, _envi("REINVESTMENT_HTTP_RETRIES", 2))
HTTP_BACKOFF = float(_envf("REINVESTMENT_HTTP_BACKOFF_SEC", 0.6))
HTTP_UA = _envs("REINVESTMENT_HTTP_USER_AGENT", "sniper-reinvest/1.0")

SYMBOL_BLACKLIST = {
    s.strip().upper() for s in _envs("REINVESTMENT_SYMBOL_BLACKLIST", "").split(",") if s.strip()
}
MINT_BLACKLIST = {
    s.strip() for s in _envs("REINVESTMENT_MINT_BLACKLIST", "").split(",") if s.strip()
}

DEXSCREENER_PAIRS_URL = "https://api.dexscreener.io/latest/dex/pairs/solana"

# ──────────────────────────────────────────────────────────────────────────────
# Shared resources + optional hooks
# ──────────────────────────────────────────────────────────────────────────────
# Core resources (preferred: initialized by orchestrator)
from utils_pkg.core_utils import (  # type: ignore
    ai_fraud_score,  # async def ai_fraud_score(mint) -> 0..1 or None
    session as shared_session,  # shared aiohttp.ClientSession (init_resources)
)

# Optional: honeypot check
try:
    from utils_pkg.honeypot import is_honeypot  # type: ignore
except Exception:  # pragma: no cover

    async def is_honeypot(_mint: str) -> bool:
        return False


# Optional: mint guard (extra sanity like "not SOL mint", not blacklisted, etc.)
try:
    from utils_pkg.mint_guard import mint_is_tradeable  # type: ignore
except Exception:  # pragma: no cover

    async def mint_is_tradeable(_mint: str) -> bool:
        return True


# Optional: risk context
try:
    from utils_pkg.risk_manager import risk_capacity_snapshot  # type: ignore
except Exception:  # pragma: no cover

    async def risk_capacity_snapshot(_redis=None) -> dict[str, Any]:
        return {"risk_capacity": 1.0, "regime_label_numeric": 0.0, "regime_stress_score": 0.0}


# Optional: AI buy scoring
try:
    from utils_pkg.ai_buy_scoring import (  # type: ignore
        Candidate as BuyCandidate,
        score_only as ai_buy_score_only,
    )
except Exception:  # pragma: no cover
    BuyCandidate = None  # type: ignore[assignment]

    async def ai_buy_score_only(*_args: Any, **_kwargs: Any) -> float:
        return 0.0


# Optional: whale bias (if you have a stronger detector, we’ll use it)
try:
    from scanner.dexscreener_scanner import has_whale_bias  # type: ignore
except Exception:  # pragma: no cover

    async def has_whale_bias(_pair: dict) -> bool:
        return False


# ──────────────────────────────────────────────────────────────────────────────
# Candidate representation
# ──────────────────────────────────────────────────────────────────────────────
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

    age_sec: float | None

    composite_score: float
    whale_bias: bool = False
    ai_fraud_score_val: float | None = None
    ai_buy_score: float | None = None

    def url(self) -> str:
        return f"https://dexscreener.com/solana/{self.pair_address}"

    def to_preview_dict(self) -> dict[str, Any]:
        d = asdict(self)
        d["url"] = self.url()
        # tidy rounding for dashboards/logs
        for k in (
            "vol24_usd",
            "liq_usd",
            "price_change_m5",
            "price_change_h1",
            "price_change_h6",
            "composite_score",
        ):
            with contextlib.suppress(Exception):
                d[k] = round(float(d[k]), 4)
        for k in ("ai_fraud_score_val", "ai_buy_score"):
            if d.get(k) is not None:
                d[k] = round(float(d[k]), 4)
        return d


# ──────────────────────────────────────────────────────────────────────────────
# Numeric helpers + scoring (stable, bounded)
# ──────────────────────────────────────────────────────────────────────────────
def _f(v: Any, d: float = 0.0) -> float:
    try:
        return float(v)
    except Exception:
        return float(d)


def _i(v: Any, d: int = 0) -> int:
    try:
        return int(v)
    except Exception:
        return int(d)


def _clamp(x: float, lo: float, hi: float) -> float:
    try:
        return max(lo, min(hi, float(x)))
    except Exception:
        return lo


def _log_norm(x: float, k: float) -> float:
    """log1p normalization into [0,1] around k."""
    x = max(0.0, float(x or 0.0))
    if x <= 0:
        return 0.0
    return _clamp(math.log1p(x) / math.log1p(max(1.0, k)), 0.0, 1.0)


def _sigmoid(x: float) -> float:
    # stable sigmoid
    x = _clamp(x, -20.0, 20.0)
    return 1.0 / (1.0 + math.exp(-x))


def _age_score(age_sec: float | None) -> float:
    """Prefer mid-age (not too new, not too old) as a smooth bump [0,1]."""
    if age_sec is None:
        return 0.5
    a = float(age_sec)
    if a <= 0:
        return 0.0
    # bump centered at PREF_AGE with width scale
    center = max(60.0, float(REINVESTMENT_PREF_AGE_SEC))
    width = max(60.0, center * 0.9)
    z = (a - center) / width
    return float(math.exp(-0.5 * z * z))  # gaussian bump


def _flow_score(buys: int, sells: int, buys_m5: int, sells_m5: int) -> float:
    """Blend of imbalance + activity into [0,1]."""
    tot1 = max(1, int(buys) + int(sells))
    imb1 = (int(buys) - int(sells)) / tot1  # [-1,1]
    tot5 = max(1, int(buys_m5) + int(sells_m5))
    imb5 = (int(buys_m5) - int(sells_m5)) / tot5

    imb = 0.65 * imb1 + 0.35 * imb5  # [-1,1]
    imb01 = 0.5 + 0.5 * _clamp(imb, -1.0, 1.0)

    # activity pressure: ~0 at tiny activity, ~1 at decent activity
    act = _sigmoid((tot1 - 10) / 12.0)
    return _clamp(0.65 * imb01 + 0.35 * act, 0.0, 1.0)


def _momentum_score(m5: float, h1: float, h6: float) -> float:
    """
    Smooth momentum score in [0,1].
    We down-weight extreme spikes and penalize strongly negative momentum.
    """
    # scale pct moves into roughly [-2,2] then sigmoid to [0,1]
    m5z = _clamp(m5 / 8.0, -2.0, 2.0)
    h1z = _clamp(h1 / 12.0, -2.0, 2.0)
    h6z = _clamp(h6 / 20.0, -2.0, 2.0)

    # weighted blend
    z = 0.45 * m5z + 0.35 * h1z + 0.20 * h6z

    # mild penalty if all horizons are negative
    if (m5 < 0) and (h1 < 0) and (h6 < 0):
        z -= 0.6

    return _clamp(_sigmoid(z), 0.0, 1.0)


def _composite(pair: dict, risk_ctx: dict[str, Any] | None) -> tuple[float, dict[str, float]]:
    pc = pair.get("priceChange") or {}
    m5 = _f(pc.get("m5"))
    h1 = _f(pc.get("h1"))
    h6 = _f(pc.get("h6"))

    vol24 = _f((pair.get("volume") or {}).get("h24"))
    liq = _f((pair.get("liquidity") or {}).get("usd"))

    tx = pair.get("txns") or {}
    h1tx = tx.get("h1") or {}
    m5tx = tx.get("m5") or {}
    buys_h1 = _i(h1tx.get("buys"))
    sells_h1 = _i(h1tx.get("sells"))
    buys_m5 = _i(m5tx.get("buys"))
    sells_m5 = _i(m5tx.get("sells"))

    age = _f(pair.get("age"), d=float("nan"))
    age_sec = None if (not math.isfinite(age)) else float(age)

    momo = _momentum_score(m5, h1, h6)
    vol_s = _log_norm(vol24, k=250_000.0)
    liq_s = _log_norm(max(liq, 0.0), k=max(10_000.0, float(REINVESTMENT_PREF_LIQ_USD)))
    flow_s = _flow_score(buys_h1, sells_h1, buys_m5, sells_m5)
    age_s = _age_score(age_sec)

    # Risk-aware tightening (bear/stress => require more liq/vol and more flow)
    regime = float((risk_ctx or {}).get("regime_label_numeric", 0.0) or 0.0)
    stress = float((risk_ctx or {}).get("regime_stress_score", 0.0) or 0.0)
    risk_cap = float((risk_ctx or {}).get("risk_capacity", 1.0) or 1.0)

    tighten = 0.0
    if regime < -0.25:
        tighten += 0.10
    if stress > 0.70:
        tighten += 0.12
    if risk_cap < 0.50:
        tighten += 0.10

    # Weighted blend to [0,1]
    score = 0.32 * momo + 0.22 * vol_s + 0.16 * liq_s + 0.20 * flow_s + 0.10 * age_s

    # Apply tightening by lowering score a bit (bounded)
    score = _clamp(score - tighten, 0.0, 1.0)

    comps = {
        "momo": float(momo),
        "vol": float(vol_s),
        "liq": float(liq_s),
        "flow": float(flow_s),
        "age": float(age_s),
        "tighten": float(tighten),
    }
    return float(score), comps


# ──────────────────────────────────────────────────────────────────────────────
# Redis helpers (supports async redis or sync redis)
# ──────────────────────────────────────────────────────────────────────────────
async def _redis_exists(redis_obj: Any, key: str) -> bool:
    if redis_obj is None:
        return False
    try:
        fn = getattr(redis_obj, "exists", None)
        if fn is None:
            return False
        out = fn(key)
        if asyncio.iscoroutine(out):
            out = await out
        return bool(int(out or 0))
    except Exception:
        return False


async def _redis_setex(redis_obj: Any, key: str, ttl: int, value: str) -> bool:
    if redis_obj is None:
        return False
    try:
        fn = getattr(redis_obj, "setex", None)
        if fn is None:
            return False
        out = fn(key, int(ttl), value)
        if asyncio.iscoroutine(out):
            out = await out
        return bool(out)
    except Exception:
        return False


# ──────────────────────────────────────────────────────────────────────────────
# HTTP fetch (uses shared aiohttp session if available; otherwise ephemeral)
# ──────────────────────────────────────────────────────────────────────────────
async def _fetch_pairs() -> list[dict]:
    """
    Fetch Dexscreener Solana pairs. Returns [] on failure.
    """
    # Lazy import to keep import-time light
    import aiohttp  # type: ignore

    headers = {"User-Agent": HTTP_UA}

    async def _one(sess: aiohttp.ClientSession) -> list[dict]:
        try:
            timeout = aiohttp.ClientTimeout(total=HTTP_TIMEOUT)
            async with sess.get(DEXSCREENER_PAIRS_URL, timeout=timeout, headers=headers) as r:
                if r.status != 200:
                    txt = await r.text()
                    log.every(
                        "REINV_DS_HTTP", n=8, msg="REINV_DS_HTTP", status=r.status, body=txt[:220]
                    )
                    return []
                data = await r.json()
                pairs = data.get("pairs") or []
                return pairs if isinstance(pairs, list) else []
        except Exception as e:
            log.every("REINV_DS_FAIL", n=8, msg="REINV_DS_FAIL", err=str(e)[:240])
            return []

    # Prefer shared session if initialized
    if shared_session is not None:
        for attempt in range(HTTP_RETRIES + 1):
            pairs = await _one(shared_session)
            if pairs:
                return pairs
            if attempt < HTTP_RETRIES:
                await asyncio.sleep(HTTP_BACKOFF * (1.0 + 0.25 * random.random()))
        return []

    # Fallback: ephemeral session (safe but less ideal)
    async with aiohttp.ClientSession() as sess:
        for attempt in range(HTTP_RETRIES + 1):
            pairs = await _one(sess)
            if pairs:
                return pairs
            if attempt < HTTP_RETRIES:
                await asyncio.sleep(HTTP_BACKOFF * (1.0 + 0.25 * random.random()))
        return []


# ──────────────────────────────────────────────────────────────────────────────
# Filtering & ranking
# ──────────────────────────────────────────────────────────────────────────────
def _extract_mint_and_symbol(pair: dict) -> tuple[str, str]:
    base = pair.get("baseToken") or {}
    mint = str(base.get("address") or "").strip()
    sym = str(base.get("symbol") or "").strip()
    if not sym:
        sym = mint[:6] if mint else "<?>"
    return mint, sym


def _pair_age_sec(pair: dict) -> float | None:
    age = pair.get("age")
    if age is None:
        return None
    try:
        v = float(age)
        return v if v >= 0 else None
    except Exception:
        return None


def _tx_stats(pair: dict) -> tuple[int, int, int, int]:
    tx = pair.get("txns") or {}
    h1 = tx.get("h1") or {}
    m5 = tx.get("m5") or {}
    return (
        _i(h1.get("buys")),
        _i(h1.get("sells")),
        _i(m5.get("buys")),
        _i(m5.get("sells")),
    )


async def _build_candidates(
    pairs: list[dict],
    *,
    min_volume: float,
    require_honeypot_check: bool,
    risk_ctx: dict[str, Any] | None,
) -> list[ReinvestCandidate]:
    out: list[ReinvestCandidate] = []

    # Risk-aware floors
    liq_floor = float(REINVESTMENT_MIN_LIQ_USD)
    vol_floor = float(min_volume)

    if risk_ctx:
        regime = float(risk_ctx.get("regime_label_numeric", 0.0) or 0.0)
        stress = float(risk_ctx.get("regime_stress_score", 0.0) or 0.0)
        cap = float(risk_ctx.get("risk_capacity", 1.0) or 1.0)
        if regime < -0.25 or stress > 0.70 or cap < 0.50:
            liq_floor *= 1.5
            vol_floor *= 1.4
        elif regime > 0.35 and stress < 0.45 and cap > 0.85:
            liq_floor *= 0.85  # slightly more open in healthy regimes

    for p in pairs:
        try:
            mint, sym = _extract_mint_and_symbol(p)
            if not mint:
                continue

            if mint in MINT_BLACKLIST:
                continue
            if sym.upper() in SYMBOL_BLACKLIST:
                continue

            # quick quote sanity (keep pairs in common quote tokens, but don't hard-block)
            # qsym = str(((p.get("quoteToken") or {}).get("symbol") or "")).upper().strip()

            vol24 = _f((p.get("volume") or {}).get("h24"))
            if vol24 < vol_floor:
                continue

            liq = _f((p.get("liquidity") or {}).get("usd"))
            if liq < liq_floor:
                continue

            age_sec = _pair_age_sec(p)
            if age_sec is not None:
                if age_sec < float(REINVESTMENT_MIN_AGE_SEC):
                    continue
                if age_sec > float(REINVESTMENT_MAX_AGE_SEC):
                    continue

            buys_h1, sells_h1, buys_m5, sells_m5 = _tx_stats(p)
            tx_h1 = buys_h1 + sells_h1
            tx_m5 = buys_m5 + sells_m5

            if tx_h1 < int(REINVESTMENT_MIN_TX_H1):
                continue
            if buys_h1 < int(REINVESTMENT_MIN_BUYS_H1):
                continue
            if tx_m5 < int(REINVESTMENT_MIN_TX_M5):
                continue

            # Optional honeypot check (can be expensive; do after hard filters)
            if require_honeypot_check:
                try:
                    if await is_honeypot(mint):
                        continue
                except Exception as e:
                    # If honeypot module fails, we do NOT crash; treat as "unknown".
                    # In strict mode, you might prefer to block, but keep it tolerant here.
                    log.every(
                        "REINV_HP_FAIL", n=8, msg="REINV_HP_FAIL", mint=mint[:10], err=str(e)[:200]
                    )

            # Optional mint tradeability guard
            try:
                ok_tradeable = await mint_is_tradeable(mint)
                if not ok_tradeable:
                    continue
            except Exception:
                # If guard fails, do not block unless you prefer strictness
                pass

            score, _comps = _composite(p, risk_ctx)
            if score < float(REINVESTMENT_MIN_COMPOSITE):
                continue

            pc = p.get("priceChange") or {}
            m5 = _f(pc.get("m5"))
            h1 = _f(pc.get("h1"))
            h6 = _f(pc.get("h6"))

            pair_addr = str(p.get("pairAddress") or "").strip() or mint
            dex = DEXES[0] if DEXES else "raydium"

            out.append(
                ReinvestCandidate(
                    mint=mint,
                    symbol=sym,
                    pair_address=pair_addr,
                    dex=dex,
                    vol24_usd=vol24,
                    liq_usd=liq,
                    price_change_m5=m5,
                    price_change_h1=h1,
                    price_change_h6=h6,
                    buys_h1=buys_h1,
                    sells_h1=sells_h1,
                    buys_m5=buys_m5,
                    sells_m5=sells_m5,
                    age_sec=age_sec,
                    composite_score=float(score),
                )
            )
        except Exception:
            continue

    return out


async def _apply_whale_bias(cands: list[ReinvestCandidate], pair_by_mint: dict[str, dict]) -> None:
    """
    Mutates cands in-place: sets whale_bias and applies a mild boost.
    """
    if not ENABLE_WHALE_FILTERS:
        return

    for c in cands:
        # cheap heuristic first
        tot = max(1, c.buys_h1 + c.sells_h1)
        imb = (c.buys_h1 - c.sells_h1) / tot
        whale = bool(imb > 0.25 and c.buys_h1 >= 12)

        # stronger hook if available
        try:
            raw_pair = pair_by_mint.get(c.mint)
            if raw_pair is not None:
                whale = whale or bool(await has_whale_bias(raw_pair))
        except Exception:
            pass

        c.whale_bias = whale
        if whale:
            c.composite_score = _clamp(c.composite_score * 1.08, 0.0, 1.0)


def _rank(cands: list[ReinvestCandidate]) -> list[ReinvestCandidate]:
    # stable sort: score desc, then liq desc, then vol desc
    return sorted(
        cands,
        key=lambda c: (c.composite_score, c.liq_usd, c.vol24_usd),
        reverse=True,
    )


# ──────────────────────────────────────────────────────────────────────────────
# AI gates
# ──────────────────────────────────────────────────────────────────────────────
async def _ai_fraud_gate(mint: str) -> tuple[bool, float | None, str]:
    """
    ai_fraud_score expected 0..1 where higher is safer.
    """
    try:
        s = await ai_fraud_score(mint)
        if s is None:
            return (
                (False, None, "ai_none") if REINVESTMENT_AI_STRICT else (True, None, "ai_none_open")
            )
        sf = float(s)
        ok = sf >= float(HOLD_IF_SCORE_ABOVE)
        return ok, sf, "ok" if ok else "below_threshold"
    except Exception as e:
        return (
            (False, None, f"ai_err:{str(e)[:60]}")
            if REINVESTMENT_AI_STRICT
            else (True, None, "ai_err_open")
        )


async def _ai_buy_gate(c: ReinvestCandidate) -> tuple[bool, float | None, str]:
    if (not REINVESTMENT_USE_AI_BUY_SCORE) or (BuyCandidate is None):
        return True, None, "disabled"

    try:
        bc = BuyCandidate(
            mint=c.mint,
            symbol=c.symbol,
            dex=c.dex,
            liq_usd=c.liq_usd,
            vol24_usd=c.vol24_usd,
            m5_change=c.price_change_m5,
            h1_change=c.price_change_h1,
            h6_change=c.price_change_h6,
            buys_1h=c.buys_h1,
            sells_1h=c.sells_h1,
            buys_5m=c.buys_m5,
            sells_5m=c.sells_m5,
        )
        s = await ai_buy_score_only(bc)
        sf = float(s or 0.0)
        ok = sf >= float(REINVESTMENT_AI_BUY_MIN_SCORE)
        return ok, sf, "ok" if ok else "below_min"
    except Exception as e:
        # buy-score is optional; failure should not hard-block reinvest unless strict is desired
        return True, None, f"err_open:{str(e)[:60]}"


# ──────────────────────────────────────────────────────────────────────────────
# Public API
# ──────────────────────────────────────────────────────────────────────────────
async def find_reinvestment_token(
    redis=None,
    min_volume: float | None = None,
    honeypot_filter: bool | None = None,
    top_n: int = 10,
) -> str | None:
    """
    Pick a single mint for reinvestment.
    Returns mint (str) or None.
    """
    if not REINVESTMENT_ENABLED:
        log.every("REINV_DISABLED", n=30, msg="REINV_DISABLED")
        return None

    mv = float(min_volume if min_volume is not None else float(REINVESTMENT_VOLUME_MIN))
    hp = bool(honeypot_filter if honeypot_filter is not None else bool(REINVESTMENT_HONEYPOT_CHECK))
    limit = max(5, int(top_n or 10))

    # Risk snapshot (regime-aware tightening)
    try:
        risk_ctx = await risk_capacity_snapshot(redis)
    except Exception:
        risk_ctx = None

    # If global risk says “no capacity”, we bail early
    try:
        cap = float((risk_ctx or {}).get("risk_capacity", 1.0) or 1.0)
        if cap <= 0.05:
            log.every("REINV_NO_CAP", n=10, msg="REINV_NO_CAP", cap=cap)
            return None
    except Exception:
        pass

    pairs = await _fetch_pairs()
    if not pairs:
        return None

    # Build candidates; also keep a mapping for whale hook
    pair_by_mint: dict[str, dict] = {}
    for p in pairs:
        m, _s = _extract_mint_and_symbol(p)
        if m:
            pair_by_mint[m] = p

    cands = await _build_candidates(
        pairs, min_volume=mv, require_honeypot_check=hp, risk_ctx=risk_ctx
    )
    if not cands:
        log.every("REINV_NONE_AFTER_FILTERS", n=10, msg="REINV_NONE_AFTER_FILTERS", mv=mv, hp=hp)
        return None

    await _apply_whale_bias(cands, pair_by_mint)
    ranked = _rank(cands)

    # Iterate top-N and pick first passing dedupe + AI gates
    for idx, c in enumerate(ranked[:limit], start=1):
        mint = c.mint

        # Redis dedupe
        if redis is not None and await _redis_exists(redis, f"reinvest:recent:{mint}"):
            continue

        # AI fraud/safety
        ok_fraud, fraud_score, fraud_reason = await _ai_fraud_gate(mint)
        c.ai_fraud_score_val = fraud_score
        if not ok_fraud:
            continue

        # Optional AI buy-score
        ok_buy, buy_score, _buy_reason = await _ai_buy_gate(c)
        c.ai_buy_score = buy_score
        if not ok_buy:
            continue

        # Mark recent (best-effort)
        if redis is not None:
            await _redis_setex(redis, f"reinvest:recent:{mint}", int(RECENT_PICK_TTL), "1")

        log.event(
            "REINVEST_PICK",
            rank=idx,
            mint=mint,
            sym=c.symbol,
            score=round(c.composite_score, 4),
            vol24=round(c.vol24_usd, 2),
            liq=round(c.liq_usd, 2),
            m5=round(c.price_change_m5, 3),
            h1=round(c.price_change_h1, 3),
            h6=round(c.price_change_h6, 3),
            buys_h1=c.buys_h1,
            sells_h1=c.sells_h1,
            ai_fraud=(round(fraud_score, 4) if fraud_score is not None else None),
            fraud_reason=fraud_reason,
            ai_buy=(round(buy_score, 4) if buy_score is not None else None),
            whale=c.whale_bias,
            dex=c.dex,
            url=c.url(),
        )
        return mint

    log.every(
        "REINV_NONE_PASSED_GATES",
        n=10,
        msg="REINV_NONE_PASSED_GATES",
        considered=min(limit, len(ranked)),
    )
    return None


async def preview_reinvestment_candidates(
    redis=None,
    min_volume: float | None = None,
    honeypot_filter: bool | None = None,
    top_n: int = 10,
    limit_return: int = 5,
) -> list[dict[str, Any]]:
    """
    Preview top candidates with gate outcomes.
    Does NOT write Redis “recent” keys and does NOT trade.
    """
    if not REINVESTMENT_ENABLED:
        return []

    mv = float(min_volume if min_volume is not None else float(REINVESTMENT_VOLUME_MIN))
    hp = bool(honeypot_filter if honeypot_filter is not None else bool(REINVESTMENT_HONEYPOT_CHECK))
    limit = max(5, int(top_n or 10))
    max_ret = max(1, int(limit_return or 5))

    try:
        risk_ctx = await risk_capacity_snapshot(redis)
    except Exception:
        risk_ctx = None

    pairs = await _fetch_pairs()
    if not pairs:
        return []

    pair_by_mint: dict[str, dict] = {}
    for p in pairs:
        m, _s = _extract_mint_and_symbol(p)
        if m:
            pair_by_mint[m] = p

    cands = await _build_candidates(
        pairs, min_volume=mv, require_honeypot_check=hp, risk_ctx=risk_ctx
    )
    if not cands:
        return []

    await _apply_whale_bias(cands, pair_by_mint)
    ranked = _rank(cands)

    out: list[dict[str, Any]] = []
    for idx, c in enumerate(ranked[:limit], start=1):
        skipped_recent = False
        if redis is not None:
            skipped_recent = await _redis_exists(redis, f"reinvest:recent:{c.mint}")

        ok_fraud, fraud_score, fraud_reason = await _ai_fraud_gate(c.mint)
        c.ai_fraud_score_val = fraud_score

        ok_buy, buy_score, buy_reason = await _ai_buy_gate(c)
        c.ai_buy_score = buy_score

        d = c.to_preview_dict()
        d.update(
            {
                "rank": idx,
                "skipped_recent": bool(skipped_recent),
                "passed_ai_fraud": bool(ok_fraud),
                "ai_fraud_reason": fraud_reason,
                "passed_ai_buy": bool(ok_buy),
                "ai_buy_reason": buy_reason,
            }
        )
        out.append(d)
        if len(out) >= max_ret:
            break

    return out


__all__ = ["find_reinvestment_token", "preview_reinvestment_candidates"]


# ──────────────────────────────────────────────────────────────────────────────
# CLI quick test (dev-only; safe if core_utils init_resources exists)
# ──────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":  # pragma: no cover
    import asyncio

    try:
        from utils_pkg.core_utils import (  # type: ignore
            close_resources,
            init_resources,
            redis as shared_redis,
        )
    except Exception:
        init_resources = None  # type: ignore
        close_resources = None  # type: ignore
        shared_redis = None  # type: ignore

    async def _demo() -> None:
        if init_resources:
            await init_resources()
        try:
            print("=== Preview top candidates ===")
            prev = await preview_reinvestment_candidates(redis=shared_redis, limit_return=5)
            for row in prev:
                print(row)

            print("\n=== Final pick ===")
            mint = await find_reinvestment_token(redis=shared_redis)
            print("PICK =", mint)
        finally:
            if close_resources:
                await close_resources()

    asyncio.run(_demo())
