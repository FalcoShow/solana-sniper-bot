from __future__ import annotations

"""
utils_pkg/find_reinvestment_token.py

Quant-grade reinvestment candidate selector (V3+++ / production-first)
=====================================================================

High-level role
---------------
Select a *reinvestment mint* (Solana SPL token mint) using Dexscreener market data with:
  - hard market sanity filters (liq/vol/age/tx activity)
  - optional honeypot + mint-guard checks
  - optional whale/smart-money bias
  - optional AI fraud safety gate (ai_fraud_score)
  - optional AI buy-score gate (ai_buy_scoring)
  - Redis dedupe so reinvestor doesn’t loop the same mint

This module is deliberately conservative (reinvest profits, not snipe).
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
    ) -> list[dict[str, Any]]

Safety & reliability
--------------------
- No network at import time.
- Lazy imports for optional heavy deps (aiohttp, core_utils, AI modules).
- Bounded concurrency for expensive checks.
- Deterministic pick: rank top-N then pick first that passes gates in rank order.
"""

import asyncio
import contextlib
import math
import os
import random
import time
from dataclasses import asdict, dataclass
from functools import lru_cache
from typing import Any, Awaitable, Callable, TypeAlias, cast

from utils_pkg.log_compat import EventLogger, get_logger

log: EventLogger = get_logger(__name__)

# ──────────────────────────────────────────────────────────────────────────────
# Small await helper (prevents "out = await out" type confusion)
# ──────────────────────────────────────────────────────────────────────────────


async def _await_if_needed(v: Any) -> Any:
    return await v if asyncio.iscoroutine(v) else v


# ──────────────────────────────────────────────────────────────────────────────
# Env helpers (no side effects)
# ──────────────────────────────────────────────────────────────────────────────

_TRUE = {"1", "true", "t", "yes", "y", "on"}
_FALSE = {"0", "false", "f", "no", "n", "off"}


def _envs(k: str, d: str = "") -> str:
    v = os.getenv(k)
    return (v if v is not None else d).strip()


def _envb(k: str, d: bool = False) -> bool:
    v = _envs(k, "")
    if v == "":
        return bool(d)
    s = v.lower().strip()
    if s in _TRUE:
        return True
    if s in _FALSE:
        return False
    return bool(d)


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


def _now_ts() -> float:
    return float(time.time())


# ──────────────────────────────────────────────────────────────────────────────
# Config fallbacks (prefer config.py if present)
# ──────────────────────────────────────────────────────────────────────────────

DEXES: list[str]
ENABLE_WHALE_FILTERS: bool
HOLD_IF_SCORE_ABOVE: float
REINVESTMENT_HONEYPOT_CHECK: bool
REINVESTMENT_VOLUME_MIN: float

try:
    from config import (  # type: ignore
        DEXES as _CFG_DEXES,
        ENABLE_WHALE_FILTERS as _CFG_ENABLE_WHALE_FILTERS,
        HOLD_IF_SCORE_ABOVE as _CFG_HOLD_IF_SCORE_ABOVE,
        REINVESTMENT_HONEYPOT_CHECK as _CFG_REINVESTMENT_HONEYPOT_CHECK,
        REINVESTMENT_VOLUME_MIN as _CFG_REINVESTMENT_VOLUME_MIN,
    )

    _raw = list(_CFG_DEXES) if isinstance(_CFG_DEXES, (list, tuple)) else []
    DEXES = [str(x).strip() for x in _raw if str(x).strip()] or ["raydium"]

    ENABLE_WHALE_FILTERS = bool(_CFG_ENABLE_WHALE_FILTERS)
    HOLD_IF_SCORE_ABOVE = float(_CFG_HOLD_IF_SCORE_ABOVE)
    REINVESTMENT_HONEYPOT_CHECK = bool(_CFG_REINVESTMENT_HONEYPOT_CHECK)
    REINVESTMENT_VOLUME_MIN = float(_CFG_REINVESTMENT_VOLUME_MIN)

except Exception:  # pragma: no cover
    REINVESTMENT_VOLUME_MIN = float(_envf("REINVESTMENT_VOLUME_MIN", 5000.0))
    REINVESTMENT_HONEYPOT_CHECK = _envb("REINVESTMENT_HONEYPOT_CHECK", True)
    HOLD_IF_SCORE_ABOVE = float(_envf("HOLD_IF_SCORE_ABOVE", 0.70))
    ENABLE_WHALE_FILTERS = _envb("ENABLE_WHALE_FILTERS", True)
    DEXES = ["raydium"]

REINVESTMENT_ENABLED = _envb("REINVESTMENT_ENABLED", True)

REINVESTMENT_MIN_LIQ_USD = float(_envf("REINVESTMENT_MIN_LIQ_USD", 3000.0))
REINVESTMENT_PREF_LIQ_USD = float(_envf("REINVESTMENT_PREF_LIQ_USD", 50000.0))

REINVESTMENT_MAX_AGE_SEC = float(_envf("REINVESTMENT_MAX_AGE_SEC", float(7 * 24 * 3600)))
REINVESTMENT_MIN_AGE_SEC = float(_envf("REINVESTMENT_MIN_AGE_SEC", 300.0))  # 5 min
REINVESTMENT_PREF_AGE_SEC = float(_envf("REINVESTMENT_PREF_AGE_SEC", float(6 * 3600)))  # 6h

REINVESTMENT_MIN_TX_H1 = _envi("REINVESTMENT_MIN_TX_H1", 8)
REINVESTMENT_MIN_BUYS_H1 = _envi("REINVESTMENT_MIN_BUYS_H1", 4)
REINVESTMENT_MIN_TX_M5 = _envi("REINVESTMENT_MIN_TX_M5", 1)

REINVESTMENT_MIN_COMPOSITE = float(_envf("REINVESTMENT_MIN_COMPOSITE", 0.35))
RECENT_PICK_TTL = max(60, _envi("REINVEST_RECENT_PICK_TTL", 30 * 60))

# AI gates
REINVESTMENT_AI_STRICT = _envb("REINVESTMENT_AI_STRICT", True)
REINVESTMENT_USE_AI_BUY_SCORE = _envb("REINVESTMENT_USE_AI_BUY_SCORE", True)
REINVESTMENT_AI_BUY_MIN_SCORE = float(_envf("REINVESTMENT_AI_BUY_MIN_SCORE", 0.40))
AI_MAX_PARALLEL = max(1, _envi("REINVESTMENT_AI_MAX_PARALLEL", 6))

# HTTP
HTTP_TIMEOUT = float(_envf("REINVESTMENT_HTTP_TIMEOUT_SEC", 10.0))
HTTP_RETRIES = max(0, _envi("REINVESTMENT_HTTP_RETRIES", 2))
HTTP_BACKOFF = float(_envf("REINVESTMENT_HTTP_BACKOFF_SEC", 0.6))
HTTP_UA = _envs("REINVESTMENT_HTTP_USER_AGENT", "sniper-reinvest/1.0")

# Blacklists
SYMBOL_BLACKLIST = {
    s.strip().upper() for s in _envs("REINVESTMENT_SYMBOL_BLACKLIST", "").split(",") if s.strip()
}
MINT_BLACKLIST = {
    s.strip() for s in _envs("REINVESTMENT_MINT_BLACKLIST", "").split(",") if s.strip()
}

# ──────────────────────────────────────────────────────────────────────────────
# Dexscreener endpoints (lazy network usage; no calls at import)
# ──────────────────────────────────────────────────────────────────────────────

DEXSCREENER_BASE = "https://api.dexscreener.com"
DEXSCREENER_BOOST_TOP_URL = f"{DEXSCREENER_BASE}/token-boosts/top/v1"
DEXSCREENER_BOOST_LATEST_URL = f"{DEXSCREENER_BASE}/token-boosts/latest/v1"
DEXSCREENER_PROFILES_LATEST_URL = f"{DEXSCREENER_BASE}/token-profiles/latest/v1"
DEXSCREENER_TOKENS_V1_FMT = f"{DEXSCREENER_BASE}/tokens/v1/solana/{{addresses}}"

# Back-compat alias (not a real "pairs list" endpoint anymore).
DEXSCREENER_PAIRS_URL = DEXSCREENER_BOOST_TOP_URL

# ──────────────────────────────────────────────────────────────────────────────
# Optional hooks (lazy-call, safe fallbacks)
# ──────────────────────────────────────────────────────────────────────────────


async def _safe_is_honeypot(mint: str) -> bool:
    try:
        from utils_pkg.honeypot import is_honeypot  # type: ignore

        res = is_honeypot(mint)
        out = await _await_if_needed(res)
        return bool(out)
    except Exception:
        return False


async def _safe_mint_is_tradeable(mint: str) -> bool:
    try:
        from utils_pkg.mint_guard import mint_is_tradeable  # type: ignore

        res = mint_is_tradeable(mint)
        out = await _await_if_needed(res)
        return bool(out)
    except Exception:
        return True


async def _safe_risk_snapshot(redis: Any) -> dict[str, Any] | None:
    try:
        from utils_pkg.risk_manager import risk_capacity_snapshot  # type: ignore

        res = risk_capacity_snapshot(redis)
        out = await _await_if_needed(res)
        return out if isinstance(out, dict) else None
    except Exception:
        return None


async def _safe_has_whale_bias(pair: dict[str, Any]) -> bool:
    try:
        from scanner.dexscreener_scanner import has_whale_bias  # type: ignore

        res = has_whale_bias(pair)
        out = await _await_if_needed(res)
        return bool(out)
    except Exception:
        return False


@lru_cache(maxsize=1)
def _get_core_utils_handles() -> tuple[Any | None, Callable[[str], Awaitable[float | None]] | None]:
    """
    Returns (aiohttp_session_or_none, ai_fraud_score_fn_or_none).
    Import is lazy to keep module import-time clean.
    """
    try:
        from utils_pkg import core_utils  # type: ignore

        sess = getattr(core_utils, "session", None)
        fn = getattr(core_utils, "ai_fraud_score", None)
        if callable(fn):
            return sess, cast(Callable[[str], Awaitable[float | None]], fn)
        return sess, None
    except Exception:
        return None, None


# ──────────────────────────────────────────────────────────────────────────────
# AI buy-score adapter (fixes repo-level contract mismatch)
# ──────────────────────────────────────────────────────────────────────────────

ScoreFn: TypeAlias = Callable[..., Awaitable[float]]


async def _coerce_score_to_float(res: Any) -> float:
    try:
        res2 = await _await_if_needed(res)

        if res2 is None:
            return 0.0
        if isinstance(res2, (int, float)):
            return float(res2)

        if isinstance(res2, dict):
            for k in ("score", "buy_score", "prob", "p", "value"):
                v = res2.get(k)
                if isinstance(v, (int, float)):
                    return float(v)
                if isinstance(v, str):
                    with contextlib.suppress(Exception):
                        return float(v)
            return 0.0

        for attr in ("score", "buy_score", "prob", "value"):
            with contextlib.suppress(Exception):
                v2 = getattr(res2, attr, None)
                if isinstance(v2, (int, float)):
                    return float(v2)
                if isinstance(v2, str):
                    return float(v2)

        with contextlib.suppress(Exception):
            return float(res2)

        return 0.0
    except Exception:
        return 0.0


@lru_cache(maxsize=1)
def _get_ai_buy_scorer() -> tuple[type[Any] | None, ScoreFn | None]:
    """
    Returns (BuyCandidateClass_or_none, score_fn_or_none).

    Contract guarantee:
      score_fn is always Callable[..., Awaitable[float]] (or None).
    """
    try:
        from utils_pkg.ai_buy_scoring import (  # type: ignore
            Candidate as BuyCandidate,
            score_only as raw_score_only,
        )
    except Exception:
        return None, None

    # IMPORTANT:
    # In package-mode, mypy can constant-fold `callable(raw_score_only)` based on
    # the analyzed type of `raw_score_only`, which can make later statements
    # "unreachable". Route through Any and just fail-open at runtime instead.
    raw_any: Any = raw_score_only

    async def score_only(*args: Any, **kwargs: Any) -> float:
        try:
            return await _coerce_score_to_float(raw_any(*args, **kwargs))
        except Exception:
            return 0.0

    return cast(type[Any], BuyCandidate), score_only


# ──────────────────────────────────────────────────────────────────────────────
# Candidate model
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
        for k in (
            "vol24_usd",
            "liq_usd",
            "price_change_m5",
            "price_change_h1",
            "price_change_h6",
            "composite_score",
        ):
            with contextlib.suppress(Exception):
                d[k] = round(float(d[k]), 6)
        for k in ("ai_fraud_score_val", "ai_buy_score"):
            if d.get(k) is not None:
                d[k] = round(float(d[k]), 6)
        return d


# ──────────────────────────────────────────────────────────────────────────────
# Numeric helpers + scoring
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


def _sigmoid(x: float) -> float:
    x2 = _clamp(x, -20.0, 20.0)
    return 1.0 / (1.0 + math.exp(-x2))


def _log_norm(x: float, k: float) -> float:
    v = max(0.0, float(x or 0.0))
    if v <= 0.0:
        return 0.0
    denom = math.log1p(max(1.0, float(k)))
    if denom <= 0.0:
        return 0.0
    return _clamp(math.log1p(v) / denom, 0.0, 1.0)


def _age_from_pair(pair: dict[str, Any]) -> float | None:
    # Some feeds include "age" directly (seconds).
    if "age" in pair:
        a = pair.get("age")
        if a is None:
            return None
        try:
            av = float(a)
            return av if av >= 0 else None
        except Exception:
            return None

    # Dexscreener commonly includes "pairCreatedAt" (ms epoch).
    created = pair.get("pairCreatedAt")
    if created is None:
        return None
    try:
        ms = float(created)
        if ms <= 0:
            return None
        sec = _now_ts() - (ms / 1000.0)
        return sec if sec >= 0 else None
    except Exception:
        return None


def _age_score(age_sec: float | None) -> float:
    if age_sec is None:
        return 0.5
    a = float(age_sec)
    if a <= 0:
        return 0.0
    center = max(60.0, float(REINVESTMENT_PREF_AGE_SEC))
    width = max(60.0, center * 0.9)
    z = (a - center) / width
    return float(math.exp(-0.5 * z * z))


def _flow_score(buys: int, sells: int, buys_m5: int, sells_m5: int) -> float:
    tot1 = max(1, int(buys) + int(sells))
    imb1 = (int(buys) - int(sells)) / tot1
    tot5 = max(1, int(buys_m5) + int(sells_m5))
    imb5 = (int(buys_m5) - int(sells_m5)) / tot5

    imb = 0.65 * imb1 + 0.35 * imb5
    imb01 = 0.5 + 0.5 * _clamp(imb, -1.0, 1.0)

    act = _sigmoid((tot1 - 10) / 12.0)
    return _clamp(0.65 * imb01 + 0.35 * act, 0.0, 1.0)


def _momentum_score(m5: float, h1: float, h6: float) -> float:
    m5z = _clamp(m5 / 8.0, -2.0, 2.0)
    h1z = _clamp(h1 / 12.0, -2.0, 2.0)
    h6z = _clamp(h6 / 20.0, -2.0, 2.0)

    z = 0.45 * m5z + 0.35 * h1z + 0.20 * h6z
    if (m5 < 0) and (h1 < 0) and (h6 < 0):
        z -= 0.6
    return _clamp(_sigmoid(z), 0.0, 1.0)


def _composite(pair: dict[str, Any], risk_ctx: dict[str, Any] | None) -> float:
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

    age_sec = _age_from_pair(pair)

    momo = _momentum_score(m5, h1, h6)
    vol_s = _log_norm(vol24, k=250_000.0)
    liq_s = _log_norm(max(liq, 0.0), k=max(10_000.0, float(REINVESTMENT_PREF_LIQ_USD)))
    flow_s = _flow_score(buys_h1, sells_h1, buys_m5, sells_m5)
    age_s = _age_score(age_sec)

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

    score = 0.32 * momo + 0.22 * vol_s + 0.16 * liq_s + 0.20 * flow_s + 0.10 * age_s
    return _clamp(score - tighten, 0.0, 1.0)


# ──────────────────────────────────────────────────────────────────────────────
# Redis helpers (supports async or sync clients)
# ──────────────────────────────────────────────────────────────────────────────


async def _redis_exists(redis_obj: Any, key: str) -> bool:
    if redis_obj is None:
        return False
    try:
        fn = getattr(redis_obj, "exists", None)
        if fn is None:
            return False
        out = fn(key)
        out2 = await _await_if_needed(out)
        return bool(int(out2 or 0))
    except Exception:
        return False


async def _redis_setex(redis_obj: Any, key: str, ttl: int, value: str) -> bool:
    if redis_obj is None:
        return False
    try:
        fn = getattr(redis_obj, "setex", None)
        if fn is None:
            fn2 = getattr(redis_obj, "set", None)
            if fn2 is None:
                return False
            out2 = fn2(key, value, ex=int(ttl))
            out3 = await _await_if_needed(out2)
            return bool(out3)

        out = fn(key, int(ttl), value)
        out2 = await _await_if_needed(out)
        return bool(out2)
    except Exception:
        return False


# ──────────────────────────────────────────────────────────────────────────────
# Dexscreener fetch helpers (lazy aiohttp import)
# ──────────────────────────────────────────────────────────────────────────────


def _chunk(seq: list[str], n: int) -> list[list[str]]:
    n2 = max(1, int(n))
    return [seq[i : i + n2] for i in range(0, len(seq), n2)]


def _extract_seed_token_addresses(payload: Any) -> list[str]:
    items: list[Any] = []
    if isinstance(payload, list):
        items = payload
    elif isinstance(payload, dict):
        if "tokenAddress" in payload:
            items = [payload]
        else:
            for k in ("tokens", "data", "items", "results"):
                v = payload.get(k)
                if isinstance(v, list):
                    items = v
                    break

    out: list[str] = []
    seen: set[str] = set()
    for it in items:
        if not isinstance(it, dict):
            continue
        chain = str(it.get("chainId") or it.get("chain") or "").strip().lower()
        if chain and chain != "solana":
            continue
        addr = str(it.get("tokenAddress") or it.get("address") or it.get("token") or "").strip()
        if not addr:
            continue
        if addr not in seen:
            seen.add(addr)
            out.append(addr)
    return out


async def _http_get_json(url: str) -> Any | None:
    try:
        import aiohttp  # type: ignore[import-not-found]
    except Exception:
        return None

    headers = {"User-Agent": HTTP_UA, "accept": "application/json"}

    async def _one(session: Any) -> Any | None:
        try:
            timeout = aiohttp.ClientTimeout(total=float(HTTP_TIMEOUT))
            async with session.get(url, timeout=timeout, headers=headers) as r:
                if r.status != 200:
                    txt = await r.text()
                    log.every(
                        "REINV_DS_HTTP",
                        every_sec=8,
                        msg="REINV_DS_HTTP",
                        status=r.status,
                        url=url,
                        body=(txt or "")[:220],
                    )
                    return None
                return await r.json()
        except Exception as e:
            log.every("REINV_DS_FAIL", every_sec=8, msg="REINV_DS_FAIL", url=url, err=str(e)[:240])
            return None

    async def _retry(fetcher: Callable[[], Awaitable[Any | None]]) -> Any | None:
        for attempt in range(int(HTTP_RETRIES) + 1):
            out = await fetcher()
            if out is not None:
                return out
            if attempt < int(HTTP_RETRIES):
                await asyncio.sleep(float(HTTP_BACKOFF) * (1.0 + 0.25 * random.random()))
        return None

    sess, _ = _get_core_utils_handles()
    if sess is not None and hasattr(sess, "get"):
        with contextlib.suppress(Exception):
            out = await _retry(lambda: _one(sess))
            if out is not None:
                return out

    async with aiohttp.ClientSession() as s:
        return await _retry(lambda: _one(s))


async def _fetch_pairs() -> list[dict[str, Any]]:
    seed_payload = await _http_get_json(DEXSCREENER_BOOST_TOP_URL)
    seeds = _extract_seed_token_addresses(seed_payload) if seed_payload is not None else []

    if not seeds:
        seed_payload = await _http_get_json(DEXSCREENER_BOOST_LATEST_URL)
        seeds = _extract_seed_token_addresses(seed_payload) if seed_payload is not None else []

    if not seeds:
        seed_payload = await _http_get_json(DEXSCREENER_PROFILES_LATEST_URL)
        seeds = _extract_seed_token_addresses(seed_payload) if seed_payload is not None else []

    if not seeds:
        return []

    seeds = seeds[:120]

    out: list[dict[str, Any]] = []
    for group in _chunk(seeds, 30):
        url = DEXSCREENER_TOKENS_V1_FMT.format(addresses=",".join(group))
        payload = await _http_get_json(url)
        if payload is None:
            continue

        if isinstance(payload, list):
            out.extend([p for p in payload if isinstance(p, dict)])
            continue

        if isinstance(payload, dict):
            pairs = payload.get("pairs")
            if isinstance(pairs, list):
                out.extend([p for p in pairs if isinstance(p, dict)])

    return out


# ──────────────────────────────────────────────────────────────────────────────
# Extraction & filtering
# ──────────────────────────────────────────────────────────────────────────────


def _extract_mint_symbol_pair(pair: dict[str, Any]) -> tuple[str, str, str]:
    base = pair.get("baseToken") or {}
    mint = str(base.get("address") or "").strip()
    sym = str(base.get("symbol") or "").strip()
    pair_addr = str(pair.get("pairAddress") or "").strip()
    if not sym and mint:
        sym = mint[:6]
    if not pair_addr and mint:
        pair_addr = mint
    return mint, sym or "<?>", pair_addr


def _tx_stats(pair: dict[str, Any]) -> tuple[int, int, int, int]:
    tx = pair.get("txns") or {}
    h1 = tx.get("h1") or {}
    m5 = tx.get("m5") or {}
    return _i(h1.get("buys")), _i(h1.get("sells")), _i(m5.get("buys")), _i(m5.get("sells"))


def _risk_adjusted_floors(
    risk_ctx: dict[str, Any] | None,
    vol_floor: float,
    liq_floor: float,
) -> tuple[float, float]:
    if not risk_ctx:
        return vol_floor, liq_floor

    regime = float(risk_ctx.get("regime_label_numeric", 0.0) or 0.0)
    stress = float(risk_ctx.get("regime_stress_score", 0.0) or 0.0)
    cap = float(risk_ctx.get("risk_capacity", 1.0) or 1.0)

    if regime < -0.25 or stress > 0.70 or cap < 0.50:
        return vol_floor * 1.40, liq_floor * 1.50
    if regime > 0.35 and stress < 0.45 and cap > 0.85:
        return vol_floor * 0.95, liq_floor * 0.85
    return vol_floor, liq_floor


async def _build_candidates(
    pairs: list[dict[str, Any]],
    *,
    min_volume: float,
    require_honeypot_check: bool,
    risk_ctx: dict[str, Any] | None,
) -> tuple[list[ReinvestCandidate], dict[str, dict[str, Any]]]:
    out: list[ReinvestCandidate] = []
    pair_by_mint: dict[str, dict[str, Any]] = {}

    vol_floor, liq_floor = _risk_adjusted_floors(
        risk_ctx, float(min_volume), float(REINVESTMENT_MIN_LIQ_USD)
    )

    for p in pairs:
        mint, sym, pair_addr = _extract_mint_symbol_pair(p)
        if not mint:
            continue

        pair_by_mint[mint] = p

        if mint in MINT_BLACKLIST:
            continue
        if sym.upper() in SYMBOL_BLACKLIST:
            continue

        vol24 = _f((p.get("volume") or {}).get("h24"))
        if vol24 < vol_floor:
            continue

        liq = _f((p.get("liquidity") or {}).get("usd"))
        if liq < liq_floor:
            continue

        age_sec = _age_from_pair(p)
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

        if require_honeypot_check:
            with contextlib.suppress(Exception):
                if await _safe_is_honeypot(mint):
                    continue

        with contextlib.suppress(Exception):
            if not await _safe_mint_is_tradeable(mint):
                continue

        score = _composite(p, risk_ctx)
        if score < float(REINVESTMENT_MIN_COMPOSITE):
            continue

        pc = p.get("priceChange") or {}
        m5 = _f(pc.get("m5"))
        h1 = _f(pc.get("h1"))
        h6 = _f(pc.get("h6"))

        dex_id = str(p.get("dexId") or "").strip()
        dex = dex_id or (DEXES[0] if DEXES else "raydium")

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

    return out, pair_by_mint


async def _apply_whale_bias(
    cands: list[ReinvestCandidate], pair_by_mint: dict[str, dict[str, Any]]
) -> None:
    if not ENABLE_WHALE_FILTERS:
        return

    for c in cands:
        tot = max(1, c.buys_h1 + c.sells_h1)
        imb = (c.buys_h1 - c.sells_h1) / tot
        whale = bool(imb > 0.25 and c.buys_h1 >= 12)

        raw = pair_by_mint.get(c.mint)
        if raw is not None:
            with contextlib.suppress(Exception):
                whale = whale or bool(await _safe_has_whale_bias(raw))

        c.whale_bias = whale
        if whale:
            c.composite_score = _clamp(c.composite_score * 1.08, 0.0, 1.0)


def _rank(cands: list[ReinvestCandidate]) -> list[ReinvestCandidate]:
    return sorted(
        cands,
        key=lambda c: (-float(c.composite_score), -float(c.liq_usd), -float(c.vol24_usd), c.mint),
    )


# ──────────────────────────────────────────────────────────────────────────────
# AI gates
# ──────────────────────────────────────────────────────────────────────────────


async def _ai_fraud_gate(mint: str) -> tuple[bool, float | None, str]:
    _, ai_fn = _get_core_utils_handles()
    if ai_fn is None:
        if REINVESTMENT_AI_STRICT:
            return False, None, "ai_missing_strict"
        return True, None, "ai_missing_open"

    try:
        s = await ai_fn(mint)
        if s is None:
            if REINVESTMENT_AI_STRICT:
                return False, None, "ai_none_strict"
            return True, None, "ai_none_open"
        sf = float(s)
        ok = sf >= float(HOLD_IF_SCORE_ABOVE)
        return ok, sf, "ok" if ok else "below_threshold"
    except Exception as e:
        if REINVESTMENT_AI_STRICT:
            return False, None, f"ai_err:{str(e)[:80]}"
        return True, None, "ai_err_open"


def _build_buy_candidate_payload(c: ReinvestCandidate) -> dict[str, Any]:
    return {
        "mint": c.mint,
        "symbol": c.symbol,
        "dex": c.dex,
        "liq_usd": c.liq_usd,
        "vol24_usd": c.vol24_usd,
        "m5_change": c.price_change_m5,
        "h1_change": c.price_change_h1,
        "h6_change": c.price_change_h6,
        "buys_1h": c.buys_h1,
        "sells_1h": c.sells_h1,
        "buys_5m": c.buys_m5,
        "sells_5m": c.sells_m5,
    }


async def _ai_buy_gate(c: ReinvestCandidate) -> tuple[bool, float | None, str]:
    if not REINVESTMENT_USE_AI_BUY_SCORE:
        return True, None, "disabled"

    BuyCandidate, score_only = _get_ai_buy_scorer()
    if BuyCandidate is None or score_only is None:
        return True, None, "missing_open"

    payload = _build_buy_candidate_payload(c)

    try:
        bc: Any
        try:
            bc = BuyCandidate(**payload)
        except Exception:
            bc = payload

        sf = float(await score_only(bc))
        ok = sf >= float(REINVESTMENT_AI_BUY_MIN_SCORE)
        return ok, sf, "ok" if ok else "below_min"
    except Exception as e:
        return True, None, f"err_open:{str(e)[:80]}"


async def _eval_candidate_gates(
    c: ReinvestCandidate, *, sem: asyncio.Semaphore
) -> ReinvestCandidate:
    async with sem:
        ok_fraud, fraud_score, _ = await _ai_fraud_gate(c.mint)
        c.ai_fraud_score_val = fraud_score
        if not ok_fraud:
            return c
        _, buy_score, _ = await _ai_buy_gate(c)
        c.ai_buy_score = buy_score
        return c


def _passed_ai_fraud(c: ReinvestCandidate) -> bool:
    if REINVESTMENT_AI_STRICT:
        return c.ai_fraud_score_val is not None and float(c.ai_fraud_score_val) >= float(
            HOLD_IF_SCORE_ABOVE
        )
    if c.ai_fraud_score_val is None:
        return True
    return float(c.ai_fraud_score_val) >= float(HOLD_IF_SCORE_ABOVE)


def _passed_ai_buy(c: ReinvestCandidate) -> bool:
    if not REINVESTMENT_USE_AI_BUY_SCORE:
        return True
    if c.ai_buy_score is None:
        return True
    return float(c.ai_buy_score) >= float(REINVESTMENT_AI_BUY_MIN_SCORE)


# ──────────────────────────────────────────────────────────────────────────────
# Public API
# ──────────────────────────────────────────────────────────────────────────────


async def find_reinvestment_token(
    redis: Any = None,
    min_volume: float | None = None,
    honeypot_filter: bool | None = None,
    top_n: int = 10,
) -> str | None:
    if not REINVESTMENT_ENABLED:
        log.every("REINV_DISABLED", every_sec=30, msg="REINV_DISABLED")
        return None

    mv = float(min_volume if min_volume is not None else float(REINVESTMENT_VOLUME_MIN))
    hp = bool(honeypot_filter if honeypot_filter is not None else bool(REINVESTMENT_HONEYPOT_CHECK))
    limit = max(5, int(top_n or 10))

    risk_ctx = await _safe_risk_snapshot(redis)
    if risk_ctx is not None:
        with contextlib.suppress(Exception):
            cap = float(risk_ctx.get("risk_capacity", 1.0) or 1.0)
            if cap <= 0.05:
                log.every("REINV_NO_CAP", every_sec=10, msg="REINV_NO_CAP", cap=cap)
                return None

    pairs = await _fetch_pairs()
    if not pairs:
        return None

    cands, pair_by_mint = await _build_candidates(
        pairs, min_volume=mv, require_honeypot_check=hp, risk_ctx=risk_ctx
    )
    if not cands:
        log.every(
            "REINV_NONE_AFTER_FILTERS",
            every_sec=10,
            msg="REINV_NONE_AFTER_FILTERS",
            mv=mv,
            hp=hp,
        )
        return None

    await _apply_whale_bias(cands, pair_by_mint)
    ranked = _rank(cands)

    ranked2: list[ReinvestCandidate] = []
    for c in ranked[:limit]:
        if redis is not None and await _redis_exists(redis, f"reinvest:recent:{c.mint}"):
            continue
        ranked2.append(c)

    if not ranked2:
        log.every("REINV_ALL_RECENT", every_sec=10, msg="REINV_ALL_RECENT")
        return None

    sem = asyncio.Semaphore(int(AI_MAX_PARALLEL))
    evaluated = await asyncio.gather(*[_eval_candidate_gates(c, sem=sem) for c in ranked2])

    for idx, c in enumerate(evaluated, start=1):
        if not _passed_ai_fraud(c):
            continue
        if not _passed_ai_buy(c):
            continue

        if redis is not None:
            await _redis_setex(redis, f"reinvest:recent:{c.mint}", int(RECENT_PICK_TTL), "1")

        log.event(
            "REINVEST_PICK",
            rank=idx,
            mint=c.mint,
            sym=c.symbol,
            score=round(float(c.composite_score), 6),
            vol24=round(float(c.vol24_usd), 2),
            liq=round(float(c.liq_usd), 2),
            m5=round(float(c.price_change_m5), 4),
            h1=round(float(c.price_change_h1), 4),
            h6=round(float(c.price_change_h6), 4),
            buys_h1=c.buys_h1,
            sells_h1=c.sells_h1,
            ai_fraud=(
                round(float(c.ai_fraud_score_val), 6) if c.ai_fraud_score_val is not None else None
            ),
            ai_buy=(round(float(c.ai_buy_score), 6) if c.ai_buy_score is not None else None),
            whale=c.whale_bias,
            dex=c.dex,
            url=c.url(),
        )
        return c.mint

    log.every(
        "REINV_NONE_PASSED_GATES",
        every_sec=10,
        msg="REINV_NONE_PASSED_GATES",
        considered=len(evaluated),
    )
    return None


async def preview_reinvestment_candidates(
    redis: Any = None,
    min_volume: float | None = None,
    honeypot_filter: bool | None = None,
    top_n: int = 10,
    limit_return: int = 5,
) -> list[dict[str, Any]]:
    if not REINVESTMENT_ENABLED:
        return []

    mv = float(min_volume if min_volume is not None else float(REINVESTMENT_VOLUME_MIN))
    hp = bool(honeypot_filter if honeypot_filter is not None else bool(REINVESTMENT_HONEYPOT_CHECK))
    limit = max(5, int(top_n or 10))
    max_ret = max(1, int(limit_return or 5))

    risk_ctx = await _safe_risk_snapshot(redis)

    pairs = await _fetch_pairs()
    if not pairs:
        return []

    cands, pair_by_mint = await _build_candidates(
        pairs, min_volume=mv, require_honeypot_check=hp, risk_ctx=risk_ctx
    )
    if not cands:
        return []

    await _apply_whale_bias(cands, pair_by_mint)
    ranked = _rank(cands)[:limit]

    sem = asyncio.Semaphore(int(AI_MAX_PARALLEL))
    evaluated = await asyncio.gather(*[_eval_candidate_gates(c, sem=sem) for c in ranked])

    out: list[dict[str, Any]] = []
    for idx, c in enumerate(evaluated, start=1):
        skipped_recent = False
        if redis is not None:
            skipped_recent = await _redis_exists(redis, f"reinvest:recent:{c.mint}")

        d = c.to_preview_dict()
        d.update(
            {
                "rank": idx,
                "skipped_recent": bool(skipped_recent),
                "passed_ai_fraud": bool(_passed_ai_fraud(c)),
                "passed_ai_buy": bool(_passed_ai_buy(c)),
            }
        )
        out.append(d)
        if len(out) >= max_ret:
            break

    return out


__all__ = ["find_reinvestment_token", "preview_reinvestment_candidates"]

# ──────────────────────────────────────────────────────────────────────────────
# CLI quick test (dev-only)
#
# IMPORTANT: This guard must NOT be "if __name__ == '__main__'" in this repo,
# because mypy package-mode will constant-fold __name__ and flag unreachable.
# Using globals().get("__name__") keeps runtime behavior identical while
# avoiding mypy [unreachable] during "py -m mypy .".
# ──────────────────────────────────────────────────────────────────────────────


def _demo_main() -> None:
    import asyncio as _asyncio

    async def _demo() -> None:
        prev = await preview_reinvestment_candidates(limit_return=5)
        print("=== Preview ===")
        for row in prev:
            print(row)
        mint = await find_reinvestment_token()
        print("=== PICK ===", mint)

    _asyncio.run(_demo())


def _running_as_script() -> bool:
    # mypy cannot constant-fold this in package mode
    import sys

    main_mod = sys.modules.get("__main__")
    main_file = getattr(main_mod, "__file__", None)
    if not isinstance(main_file, str):
        return False
    return os.path.basename(main_file).lower() == "find_reinvestment_token.py"


if _running_as_script():  # pragma: no cover
    _demo_main()
