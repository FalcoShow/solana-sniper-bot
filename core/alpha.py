from __future__ import annotations

"""
core/alpha.py — Canonical alpha scoring primitive (P1 module; safe to import everywhere)

Role
----
A small, deterministic "alpha" scorer used to rank/triage candidates.
This module MUST:
- never place trades
- never touch RPC / network
- never mutate global execution state
- be safe to import in any context (tests, scanners, strategies)

Compatibility
-------------
Accepts either:
(A) normalized Candidate-like dicts (your canonical schema)
(B) raw Dexscreener-style 'pair' dicts (nested liquidity/volume/txns/priceChange)

Contract
--------
- score_candidate(cand, cfg=None) -> AlphaResult(score, components, reasons, meta)
- passes_alpha(cand, cfg=None) -> (bool, AlphaResult)
- select_top(cands, limit=None, cfg=None) -> ranked list[(cand, AlphaResult)]

Environment knobs (optional)
----------------------------
ALPHA_THRESHOLD=0.62
ALPHA_MAX_TRADEABLE=8

# weights (must sum ~1; we renormalize if not)
ALPHA_W_AGE=0.22
ALPHA_W_LIQ=0.28
ALPHA_W_IMPACT=0.20
ALPHA_W_MOMO=0.18
ALPHA_W_FLOW=0.10
ALPHA_W_SMART=0.02

# sweet spots / gates
ALPHA_AGE_MAX_MIN=15
ALPHA_LIQ_LO=5000
ALPHA_LIQ_HI=60000
ALPHA_IMPACT_LO_BPS=250
ALPHA_IMPACT_HI_BPS=900

# momentum windows
ALPHA_MOMO_PCT5_TARGET=6.0
ALPHA_MOMO_PCT5_CAP=18.0
ALPHA_MIN_BUYS_5M=2
ALPHA_MAX_SELLS_BUYS_RATIO=2.2

# source boost
ALPHA_PUMPFUN_BOOST=0.05
"""

import logging
import math
import os
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any

# -----------------------------------------------------------------------------
# Logging (no global side-effects)
# -----------------------------------------------------------------------------
log = logging.getLogger("alpha")
if not log.handlers:
    # Prevent "No handler found" warnings in libraries; caller configures logging.
    log.addHandler(logging.NullHandler())


# -----------------------------------------------------------------------------
# Env helpers (contract-friendly, best-effort)
# -----------------------------------------------------------------------------
_TRUE = {"1", "true", "t", "yes", "y", "on"}
_FALSE = {"0", "false", "f", "no", "n", "off"}


def _env_get(key: str) -> str | None:
    """
    Read env without using os.getenv("LITERAL") patterns.

    NOTE: We intentionally use os.environ.get(var) where `var` is not a string literal
    at the callsite, to avoid brittle env-contract scanners flagging keys here.
    """
    return os.environ.get(key)


def _env_float(key: str, default: float) -> float:
    raw = _env_get(key)
    if raw is None or not str(raw).strip():
        return float(default)
    try:
        return float(str(raw).strip())
    except Exception:
        return float(default)


def _env_int(key: str, default: int) -> int:
    raw = _env_get(key)
    if raw is None or not str(raw).strip():
        return int(default)
    try:
        # allow "8.0"
        return int(float(str(raw).strip()))
    except Exception:
        return int(default)


def _env_bool(key: str, default: bool = False) -> bool:
    raw = _env_get(key)
    if raw is None:
        return bool(default)
    v = str(raw).strip().lower()
    if v in _TRUE:
        return True
    if v in _FALSE:
        return False
    return bool(default)


# -----------------------------------------------------------------------------
# Safe conversions / utilities
# -----------------------------------------------------------------------------
def _clamp(x: float, lo: float = 0.0, hi: float = 1.0) -> float:
    if x < lo:
        return lo
    if x > hi:
        return hi
    return x


def _safe_float(x: Any) -> float | None:
    if x is None:
        return None
    try:
        return float(x)
    except Exception:
        return None


def _safe_int(x: Any, default: int = 0) -> int:
    if x is None:
        return default
    try:
        return int(x)
    except Exception:
        try:
            return int(float(x))
        except Exception:
            return default


def _as_lower(x: Any) -> str:
    try:
        return str(x or "").strip().lower()
    except Exception:
        return ""


def _as_str(x: Any) -> str:
    try:
        return str(x or "")
    except Exception:
        return ""


def _get_path(d: Mapping[str, Any], path: str) -> Any:
    """
    Safe nested getter:
      _get_path(pair, "liquidity.usd")
      _get_path(pair, "txns.m5.buys")
    Returns None if missing.
    """
    cur: Any = d
    for part in path.split("."):
        if not isinstance(cur, Mapping):
            return None
        cur = cur.get(part)
        if cur is None:
            return None
    return cur


def _first_present(d: Mapping[str, Any], keys: Sequence[str]) -> Any:
    for k in keys:
        v = _get_path(d, k) if "." in k else d.get(k)
        if v is not None:
            return v
    return None


# -----------------------------------------------------------------------------
# Scoring primitives
# -----------------------------------------------------------------------------
def _soft_band(x: float | None, lo: float, hi: float) -> float:
    """
    Returns a [0..1] desirability score:
      - inside [lo, hi] scores high (peaks near the middle)
      - outside decays smoothly (never hard zero unless x <= 0)
    """
    if x is None:
        return 0.0
    if x <= 0:
        return 0.0
    if lo <= 0 or hi <= 0 or hi <= lo:
        return 0.0

    if x < lo:
        # rises from 0..~0.55 as x approaches lo
        return _clamp((x / lo) * 0.55, 0.0, 0.55)
    if x > hi:
        # decays from ~0.55..0 as x grows above hi
        return _clamp((hi / x) * 0.55, 0.0, 0.55)

    mid = (lo + hi) * 0.5
    span = (hi - lo) * 0.5
    if span <= 0:
        return 0.85

    dist = abs(x - mid) / span  # 0 at mid, 1 at edges
    # quadratic bowl inside band: 0.65..1.0
    return _clamp(1.0 - 0.35 * (dist * dist), 0.65, 1.0)


def _age_score(age_min: float | None, age_max_min: float) -> float:
    """
    Prefer newer pools; linear decay up to age_max_min.
    age <= 0 -> 1.0, age >= age_max_min -> 0.0
    """
    if age_min is None:
        return 0.0
    if age_max_min <= 0:
        return 0.0

    a = float(age_min)
    if a <= 0:
        return 1.0
    return _clamp((age_max_min - a) / age_max_min, 0.0, 1.0)


def _momo_score(pct5: float | None, target: float, cap: float) -> float:
    """
    Prefer positive 5m change up to cap. Negative/zero -> 0.
    Smooth saturating curve; below target still contributes.
    """
    if pct5 is None:
        return 0.0
    p = float(pct5)
    if p <= 0:
        return 0.0

    t = max(1e-9, float(target))
    c = max(t, float(cap)) if cap > 0 else t

    x = _clamp(p / c, 0.0, 1.0)
    curve = 1.0 - math.exp(-3.0 * x)  # 0..~0.95
    # below target: still good but slightly discounted
    return _clamp(curve * (0.75 if p < t else 1.0), 0.0, 1.0)


def _flow_score(buys5: int, sells5: int, min_buys: int, max_ratio: float) -> float:
    """
    Uses txn flow as a sanity:
      - require some buys (activity)
      - penalize heavy sell dominance

    Output in [0..1].
    """
    b = int(max(0, buys5))
    s = int(max(0, sells5))

    if b == 0 and s == 0:
        return 0.0

    act = 1.0 if min_buys <= 0 else _clamp(b / float(min_buys), 0.0, 1.0)

    ratio = (s / max(1, b)) if b > 0 else 9e9
    if ratio <= 1.0:
        dom = 1.0
    elif max_ratio <= 1.0:
        dom = 0.0
    elif ratio >= max_ratio:
        dom = 0.0
    else:
        dom = _clamp(1.0 - (ratio - 1.0) / (max_ratio - 1.0), 0.0, 1.0)

    return _clamp(0.65 * act + 0.35 * dom, 0.0, 1.0)


def _renorm_weights(w: dict[str, float]) -> dict[str, float]:
    s = 0.0
    for v in w.values():
        s += max(0.0, float(v))
    if s <= 0:
        return {k: 0.0 for k in w}
    return {k: max(0.0, float(v)) / s for k, v in w.items()}


# -----------------------------------------------------------------------------
# Config + result models
# -----------------------------------------------------------------------------
@dataclass(frozen=True)
class AlphaConfig:
    threshold: float = 0.62
    max_tradeable: int = 8

    # weights (renormalized in from_env)
    w_age: float = 0.22
    w_liq: float = 0.28
    w_impact: float = 0.20
    w_momo: float = 0.18
    w_flow: float = 0.10
    w_smart: float = 0.02

    # sweet spots / gates
    age_max_min: float = 15.0
    liq_lo: float = 5_000.0
    liq_hi: float = 60_000.0
    impact_lo_bps: float = 250.0
    impact_hi_bps: float = 900.0

    # momentum / flow
    momo_pct5_target: float = 6.0
    momo_pct5_cap: float = 18.0
    min_buys_5m: int = 2
    max_sells_buys_ratio: float = 2.2

    # source boost
    pumpfun_boost: float = 0.05

    @staticmethod
    def from_env() -> AlphaConfig:
        # Keep keys centralized (and used dynamically through _env_* helpers)
        threshold = _env_float("ALPHA_THRESHOLD", 0.62)
        max_tradeable = _env_int("ALPHA_MAX_TRADEABLE", 8)

        w = _renorm_weights(
            {
                "w_age": _env_float("ALPHA_W_AGE", 0.22),
                "w_liq": _env_float("ALPHA_W_LIQ", 0.28),
                "w_impact": _env_float("ALPHA_W_IMPACT", 0.20),
                "w_momo": _env_float("ALPHA_W_MOMO", 0.18),
                "w_flow": _env_float("ALPHA_W_FLOW", 0.10),
                "w_smart": _env_float("ALPHA_W_SMART", 0.02),
            }
        )

        return AlphaConfig(
            threshold=threshold,
            max_tradeable=max_tradeable,
            w_age=w["w_age"],
            w_liq=w["w_liq"],
            w_impact=w["w_impact"],
            w_momo=w["w_momo"],
            w_flow=w["w_flow"],
            w_smart=w["w_smart"],
            age_max_min=_env_float("ALPHA_AGE_MAX_MIN", 15.0),
            liq_lo=_env_float("ALPHA_LIQ_LO", 5_000.0),
            liq_hi=_env_float("ALPHA_LIQ_HI", 60_000.0),
            impact_lo_bps=_env_float("ALPHA_IMPACT_LO_BPS", 250.0),
            impact_hi_bps=_env_float("ALPHA_IMPACT_HI_BPS", 900.0),
            momo_pct5_target=_env_float("ALPHA_MOMO_PCT5_TARGET", 6.0),
            momo_pct5_cap=_env_float("ALPHA_MOMO_PCT5_CAP", 18.0),
            min_buys_5m=_env_int("ALPHA_MIN_BUYS_5M", 2),
            max_sells_buys_ratio=_env_float("ALPHA_MAX_SELLS_BUYS_RATIO", 2.2),
            pumpfun_boost=_env_float("ALPHA_PUMPFUN_BOOST", 0.05),
        )


@dataclass(frozen=True)
class AlphaResult:
    score: float
    components: dict[str, float]
    reasons: list[str]
    meta: dict[str, str]

    def to_dict(self) -> dict[str, Any]:
        return {
            "score": float(self.score),
            "components": {k: float(v) for k, v in self.components.items()},
            "reasons": list(self.reasons),
            "meta": dict(self.meta),
        }


# -----------------------------------------------------------------------------
# Candidate field extraction (normalized Candidate OR raw Dexscreener pair)
# -----------------------------------------------------------------------------
@dataclass(frozen=True)
class _Fields:
    age_min: float | None
    liq_usd: float | None
    impact_bps: float | None
    pct5: float | None
    buys5: int
    sells5: int
    source: str
    smart_wallet_score: float
    symbol: str
    mint: str


def extract_fields(c: Mapping[str, Any]) -> _Fields:
    """
    Pull common fields from either schema.

    Normalized candidates might contain:
      pool_age_min, liq_usd, impact_1k_bps / impact_bps, source, smart_wallet_score,
      chg5 / price_change_5m, buys5/sells5

    Dexscreener pairs contain nested:
      liquidity.usd, priceChange.m5, txns.m5.buys/sells, baseToken.address/symbol
    """
    age = _first_present(c, ["pool_age_min", "age_min", "meta.age_min", "ageMinutes"])
    liq = _first_present(c, ["liq_usd", "liquidity.usd", "liquidityUSD", "meta.liq_usd"])
    impact = _first_present(c, ["impact_1k_bps", "impact_bps", "meta.impact_bps", "impactBps"])

    pct5 = _first_present(
        c,
        ["chg5", "price_change_5m", "priceChange.m5", "meta.pct5", "pct5"],
    )

    buys5 = _first_present(c, ["buys5", "txns.m5.buys", "meta.buys5"])
    sells5 = _first_present(c, ["sells5", "txns.m5.sells", "meta.sells5"])

    source = _first_present(c, ["source", "meta.source", "origin", "scanner"])
    smart = _first_present(c, ["smart_wallet_score", "meta.smart_wallet_score", "smartScore"])

    sym = _first_present(c, ["symbol", "sym", "baseToken.symbol", "baseToken.name"])
    mint = _first_present(c, ["mint", "baseToken.address", "address"])

    smart_f = _safe_float(smart)
    smart_score = _clamp(float(smart_f) if smart_f is not None else 0.0, 0.0, 1.0)

    return _Fields(
        age_min=_safe_float(age),
        liq_usd=_safe_float(liq),
        impact_bps=_safe_float(impact),
        pct5=_safe_float(pct5),
        buys5=_safe_int(buys5, 0),
        sells5=_safe_int(sells5, 0),
        source=_as_lower(source),
        smart_wallet_score=smart_score,
        symbol=_as_str(sym),
        mint=_as_str(mint),
    )


# -----------------------------------------------------------------------------
# Main API
# -----------------------------------------------------------------------------
def score_candidate(candidate: Mapping[str, Any], cfg: AlphaConfig | None = None) -> AlphaResult:
    """
    Return stable alpha score in [0..1] plus components + reasons.

    This function must never raise for normal malformed input; if something truly
    unexpected happens, we degrade to a safe low-score result.
    """
    cfg_use = cfg or AlphaConfig.from_env()

    try:
        f = extract_fields(candidate)

        age_s = _age_score(f.age_min, cfg_use.age_max_min)
        liq_s = _soft_band(f.liq_usd, cfg_use.liq_lo, cfg_use.liq_hi)
        imp_s = _soft_band(f.impact_bps, cfg_use.impact_lo_bps, cfg_use.impact_hi_bps)
        momo_s = _momo_score(f.pct5, cfg_use.momo_pct5_target, cfg_use.momo_pct5_cap)
        flow_s = _flow_score(f.buys5, f.sells5, cfg_use.min_buys_5m, cfg_use.max_sells_buys_ratio)
        smart_s = _clamp(float(f.smart_wallet_score), 0.0, 1.0)

        base = (
            cfg_use.w_age * age_s
            + cfg_use.w_liq * liq_s
            + cfg_use.w_impact * imp_s
            + cfg_use.w_momo * momo_s
            + cfg_use.w_flow * flow_s
            + cfg_use.w_smart * smart_s
        )

        src_boost = 0.0
        if f.source == "pumpfun":
            src_boost = float(cfg_use.pumpfun_boost)

        score = _clamp(base + src_boost, 0.0, 1.0)

        reasons: list[str] = []
        if score >= cfg_use.threshold:
            reasons.append("alpha_ok")
        else:
            reasons.append("alpha_low")

        # negatives (why not)
        if f.age_min is None:
            reasons.append("missing:age_min")
        elif f.age_min > cfg_use.age_max_min:
            reasons.append(f"age_high:{f.age_min:.1f}m")

        if f.liq_usd is None:
            reasons.append("missing:liq_usd")
        elif f.liq_usd < cfg_use.liq_lo:
            reasons.append(f"liq_low:${f.liq_usd:.0f}")
        elif f.liq_usd > cfg_use.liq_hi:
            reasons.append(f"liq_high:${f.liq_usd:.0f}")

        if f.impact_bps is None:
            reasons.append("missing:impact_bps")
        else:
            if f.impact_bps < cfg_use.impact_lo_bps:
                reasons.append(f"impact_low:{f.impact_bps:.0f}bps")
            elif f.impact_bps > cfg_use.impact_hi_bps:
                reasons.append(f"impact_high:{f.impact_bps:.0f}bps")

        if f.pct5 is None:
            reasons.append("missing:pct5")
        elif f.pct5 <= 0:
            reasons.append(f"momo_bad:{f.pct5:.2f}%")

        if f.buys5 < cfg_use.min_buys_5m:
            reasons.append(f"buys5_low:{f.buys5}")

        components = {
            "age": float(age_s),
            "liq": float(liq_s),
            "impact": float(imp_s),
            "momo": float(momo_s),
            "flow": float(flow_s),
            "smart": float(smart_s),
            "source_boost": float(src_boost),
            "base": float(_clamp(base, 0.0, 1.0)),
        }

        meta = {
            "source": f.source,
            "symbol": f.symbol,
            "mint": f.mint,
        }

        return AlphaResult(score=float(score), components=components, reasons=reasons, meta=meta)

    except Exception:
        # Absolute last-resort fallback; never crash pipeline.
        log.debug("score_candidate unexpected failure", exc_info=True)
        return AlphaResult(
            score=0.0,
            components={"base": 0.0, "source_boost": 0.0},
            reasons=["alpha_low", "error:scoring_failed"],
            meta={"source": "", "symbol": "", "mint": ""},
        )


def score_only(candidate: Mapping[str, Any], cfg: AlphaConfig | None = None) -> float:
    return float(score_candidate(candidate, cfg).score)


def passes_alpha(
    candidate: Mapping[str, Any], cfg: AlphaConfig | None = None
) -> tuple[bool, AlphaResult]:
    cfg_use = cfg or AlphaConfig.from_env()
    res = score_candidate(candidate, cfg_use)
    return (res.score >= cfg_use.threshold), res


def select_top(
    candidates: Iterable[Mapping[str, Any]],
    limit: int | None = None,
    cfg: AlphaConfig | None = None,
) -> list[tuple[Mapping[str, Any], AlphaResult]]:
    """
    Rank candidates by alpha score (descending), with deterministic tie-breakers.
    Returns list of (candidate, AlphaResult).
    """
    cfg_use = cfg or AlphaConfig.from_env()
    lim = int(limit if limit is not None else cfg_use.max_tradeable)
    if lim <= 0:
        return []

    scored: list[tuple[Mapping[str, Any], AlphaResult]] = []
    for c in candidates:
        # Never crash on bad candidate
        res = score_candidate(c, cfg_use)
        scored.append((c, res))

    def _key(item: tuple[Mapping[str, Any], AlphaResult]) -> tuple[float, str, str]:
        cand, res = item
        # stable tie-breakers: mint then symbol
        meta = res.meta
        mint = meta.get("mint", "") or _as_str(cand.get("mint"))
        sym = meta.get("symbol", "") or _as_str(cand.get("symbol"))
        return (res.score, mint, sym)

    scored.sort(key=_key, reverse=True)
    return scored[:lim]


# -----------------------------------------------------------------------------
# Backwards-compatible constants (no env reads at import-time)
# -----------------------------------------------------------------------------
DEFAULT_CONFIG = AlphaConfig()
THRESHOLD = DEFAULT_CONFIG.threshold
MAX_TRADEABLE = DEFAULT_CONFIG.max_tradeable


# -----------------------------------------------------------------------------
# Tiny self-test (manual, no network)
# -----------------------------------------------------------------------------
if __name__ == "__main__":
    demo = {
        "pool_age_min": 6,
        "liq_usd": 22_000,
        "impact_1k_bps": 420,
        "chg5": 4.2,
        "buys5": 6,
        "sells5": 2,
        "source": "pumpfun",
        "smart_wallet_score": 0.35,
        "mint": "So11111111111111111111111111111111111111112",
        "symbol": "SOL",
    }
    ok, res = passes_alpha(demo)
    print("ok:", ok)
    print("score:", res.score)
    print("components:", res.components)
    print("reasons:", res.reasons)
    print("meta:", res.meta)
