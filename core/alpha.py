# core/alpha.py
from __future__ import annotations

"""
core/alpha.py — Canonical alpha scoring primitive (P1 module; safe to import everywhere)

Role
----
A *small*, deterministic "alpha" scorer used to rank/triage candidates.
This MUST NOT place trades, access RPC, or mutate global execution state.
It should be safe for:
  - scanners to rank candidates
  - strategy wiring to filter candidates
  - tests/smokes to validate scoring invariants

Compatibility
-------------
Supports both:
  (A) normalized Candidate dicts (your canonical Candidate schema)
  (B) raw Dexscreener-style 'pair' dicts (nested liquidity/volume/txns/priceChange)

Contract
--------
- score_candidate(cand) -> AlphaResult(score, components, reasons)
- passes_alpha(cand) -> (bool, AlphaResult)
- select_top(cands) -> ranked list (cand, AlphaResult)

Env knobs (optional)
--------------------
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
ALPHA_MOMO_PCT5_TARGET=6.0      # "good" 5m change
ALPHA_MOMO_PCT5_CAP=18.0        # cap normalization
ALPHA_MIN_BUYS_5M=2             # "flow" / activity
ALPHA_MAX_SELLS_BUYS_RATIO=2.2  # optional penalty when sells dominate too hard

# source boost
ALPHA_PUMPFUN_BOOST=0.05
"""

import math
import os
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any


# -----------------------------------------------------------------------------
# Logging (optional)
# -----------------------------------------------------------------------------
def _get_logger():
    try:
        from logging_setup import get_logger  # type: ignore

        return get_logger("alpha")
    except Exception:
        import logging

        logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO").upper())
        return logging.getLogger("alpha")


log = _get_logger()


# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------
def _env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)))
    except Exception:
        return float(default)


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except Exception:
        return int(default)


def _clamp(x: float, lo: float = 0.0, hi: float = 1.0) -> float:
    return max(lo, min(hi, x))


def _safe_float(x: Any, default: float = 0.0) -> float:
    try:
        if x is None:
            return default
        return float(x)
    except Exception:
        return default


def _safe_int(x: Any, default: int = 0) -> int:
    try:
        if x is None:
            return default
        return int(x)
    except Exception:
        return default


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


def _soft_band(x: float | None, lo: float, hi: float) -> float:
    """
    Returns [0..1] score where:
      - inside [lo, hi] scores high (peaks near middle)
      - outside decays smoothly (never hard zero unless x <= 0)
    """
    if x is None:
        return 0.0
    x = float(x)
    if x <= 0:
        return 0.0
    if lo <= 0 or hi <= 0 or hi <= lo:
        return 0.0

    if x < lo:
        # rises from 0..~0.5 as x approaches lo
        return _clamp((x / lo) * 0.55, 0.0, 0.55)
    if x > hi:
        # decays from ~0.55..0 as x grows above hi
        return _clamp((hi / x) * 0.55, 0.0, 0.55)

    # inside band: 0.65..1.0, peak at mid, smooth drop to edges
    mid = (lo + hi) * 0.5
    span = (hi - lo) * 0.5
    if span <= 0:
        return 0.85
    dist = abs(x - mid) / span  # 0 at mid, 1 at edges
    # quadratic bowl
    return _clamp(1.0 - 0.35 * (dist**2), 0.65, 1.0)


def _age_score(age_min: float | None, age_max_min: float) -> float:
    """
    Prefer newer pools; linear decay up to age_max_min.
    """
    if age_min is None:
        return 0.0
    a = float(age_min)
    if a <= 0:
        return 1.0
    if age_max_min <= 0:
        return 0.0
    return _clamp((age_max_min - a) / age_max_min, 0.0, 1.0)


def _momo_score(pct5: float | None, target: float, cap: float) -> float:
    """
    Prefer positive 5m change up to a cap. Negative -> 0.
    Uses a gentle curve: below target is still okay, above target saturates.
    """
    if pct5 is None:
        return 0.0
    p = float(pct5)
    if p <= 0:
        return 0.0
    if cap <= 0:
        return 0.0
    # Normalize to [0..1] where p == target -> ~0.7
    t = max(1e-9, float(target))
    c = max(t, float(cap))
    x = _clamp(p / c, 0.0, 1.0)
    # curve: more sensitive early, saturates
    return _clamp(1.0 - math.exp(-3.0 * x), 0.0, 1.0) * (0.75 if p < t else 1.0)


def _flow_score(buys5: int | None, sells5: int | None, min_buys: int, max_ratio: float) -> float:
    """
    Use txn flow as a sanity: need some buys, and penalize heavy sell dominance.
    """
    b = int(buys5 or 0)
    s = int(sells5 or 0)
    if b <= 0 and s <= 0:
        return 0.0

    # activity component
    act = 1.0 if min_buys <= 0 else _clamp(b / float(min_buys), 0.0, 1.0)

    # sell dominance penalty
    ratio = (s / max(1, b)) if b > 0 else 9e9
    if ratio <= 1.0:
        dom = 1.0
    elif ratio >= max_ratio:
        dom = 0.0
    else:
        # linear fade to 0
        dom = _clamp(1.0 - (ratio - 1.0) / max(1e-9, (max_ratio - 1.0)), 0.0, 1.0)

    return _clamp(0.65 * act + 0.35 * dom, 0.0, 1.0)


def _renorm_weights(w: dict[str, float]) -> dict[str, float]:
    s = sum(max(0.0, float(v)) for v in w.values())
    if s <= 0:
        return {k: 0.0 for k in w}
    return {k: max(0.0, float(v)) / s for k, v in w.items()}


# -----------------------------------------------------------------------------
# Config + result
# -----------------------------------------------------------------------------
@dataclass(frozen=True)
class AlphaConfig:
    threshold: float
    max_tradeable: int

    # weights
    w_age: float
    w_liq: float
    w_impact: float
    w_momo: float
    w_flow: float
    w_smart: float

    # sweet spots / gates
    age_max_min: float
    liq_lo: float
    liq_hi: float
    impact_lo_bps: float
    impact_hi_bps: float

    # momentum / flow
    momo_pct5_target: float
    momo_pct5_cap: float
    min_buys_5m: int
    max_sells_buys_ratio: float

    # source boost
    pumpfun_boost: float

    @staticmethod
    def from_env() -> AlphaConfig:
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
            threshold=_env_float("ALPHA_THRESHOLD", 0.62),
            max_tradeable=_env_int("ALPHA_MAX_TRADEABLE", 8),
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


# -----------------------------------------------------------------------------
# Candidate field extraction (normalized Candidate OR raw Dexscreener pair)
# -----------------------------------------------------------------------------
def extract_fields(c: Mapping[str, Any]) -> dict[str, Any]:
    """
    Pull common fields from either schema.

    Normalized candidates might contain:
      pool_age_min, liq_usd, impact_1k_bps, source, smart_wallet_score,
      chg5 / price_change_5m, buys5/sells5, vol5, etc.

    Dexscreener pairs contain nested:
      liquidity.usd, priceChange.m5, txns.m5.buys/sells, volume.m5, baseToken.address/symbol
    """
    age = _first_present(c, ["pool_age_min", "age_min", "meta.age_min", "ageMinutes"])
    liq = _first_present(c, ["liq_usd", "liquidity.usd", "liquidityUSD", "meta.liq_usd"])
    impact = _first_present(c, ["impact_1k_bps", "impact_bps", "meta.impact_bps", "impactBps"])

    # 5m change (Dexscreener uses priceChange.m5)
    pct5 = _first_present(
        c,
        [
            "chg5",
            "price_change_5m",
            "priceChange.m5",
            "meta.pct5",
            "pct5",
        ],
    )

    buys5 = _first_present(c, ["buys5", "txns.m5.buys", "meta.buys5"])
    sells5 = _first_present(c, ["sells5", "txns.m5.sells", "meta.sells5"])

    source = _first_present(c, ["source", "meta.source", "origin", "scanner"])
    smart = _first_present(c, ["smart_wallet_score", "meta.smart_wallet_score", "smartScore"])

    sym = _first_present(c, ["symbol", "sym", "baseToken.symbol", "baseToken.name"])
    mint = _first_present(c, ["mint", "baseToken.address", "address"])

    return {
        "age_min": _safe_float(age, None) if age is not None else None,
        "liq_usd": _safe_float(liq, None) if liq is not None else None,
        "impact_bps": _safe_float(impact, None) if impact is not None else None,
        "pct5": _safe_float(pct5, None) if pct5 is not None else None,
        "buys5": _safe_int(buys5, 0),
        "sells5": _safe_int(sells5, 0),
        "source": str(source or "").lower(),
        "smart_wallet_score": _clamp(_safe_float(smart, 0.0), 0.0, 1.0),
        "symbol": str(sym or ""),
        "mint": str(mint or ""),
    }


# -----------------------------------------------------------------------------
# Scoring
# -----------------------------------------------------------------------------
def score_candidate(candidate: Mapping[str, Any], cfg: AlphaConfig | None = None) -> AlphaResult:
    """
    Returns a stable alpha score in [0..1] and reasons/components for observability.
    """
    cfg = cfg or AlphaConfig.from_env()
    f = extract_fields(candidate)

    age_s = _age_score(f["age_min"], cfg.age_max_min)
    liq_s = _soft_band(f["liq_usd"], cfg.liq_lo, cfg.liq_hi)
    imp_s = _soft_band(f["impact_bps"], cfg.impact_lo_bps, cfg.impact_hi_bps)
    momo_s = _momo_score(f["pct5"], cfg.momo_pct5_target, cfg.momo_pct5_cap)
    flow_s = _flow_score(f["buys5"], f["sells5"], cfg.min_buys_5m, cfg.max_sells_buys_ratio)
    smart_s = _clamp(float(f["smart_wallet_score"]), 0.0, 1.0)

    # Core weighted score
    base = (
        cfg.w_age * age_s
        + cfg.w_liq * liq_s
        + cfg.w_impact * imp_s
        + cfg.w_momo * momo_s
        + cfg.w_flow * flow_s
        + cfg.w_smart * smart_s
    )

    # Small deterministic source boost
    src_boost = 0.0
    if f["source"] == "pumpfun":
        src_boost = float(cfg.pumpfun_boost)

    score = _clamp(base + src_boost, 0.0, 1.0)

    reasons: list[str] = []
    # explain "why not"
    if f["age_min"] is None:
        reasons.append("missing:age_min")
    elif f["age_min"] > cfg.age_max_min:
        reasons.append(f"age_high:{f['age_min']:.1f}m")

    if f["liq_usd"] is None:
        reasons.append("missing:liq_usd")
    elif f["liq_usd"] < cfg.liq_lo:
        reasons.append(f"liq_low:${f['liq_usd']:.0f}")
    elif f["liq_usd"] > cfg.liq_hi:
        reasons.append(f"liq_high:${f['liq_usd']:.0f}")

    if f["impact_bps"] is None:
        reasons.append("missing:impact_bps")
    else:
        if f["impact_bps"] < cfg.impact_lo_bps:
            reasons.append(f"impact_low:{f['impact_bps']:.0f}bps")
        elif f["impact_bps"] > cfg.impact_hi_bps:
            reasons.append(f"impact_high:{f['impact_bps']:.0f}bps")

    if f["pct5"] is None:
        reasons.append("missing:pct5")
    elif f["pct5"] <= 0:
        reasons.append(f"momo_bad:{f['pct5']:.2f}%")

    if f["buys5"] < cfg.min_buys_5m:
        reasons.append(f"buys5_low:{f['buys5']}")

    # helpful positives
    if score >= cfg.threshold:
        reasons.insert(0, "alpha_ok")
    else:
        reasons.insert(0, "alpha_low")

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

    return AlphaResult(score=float(score), components=components, reasons=reasons)


def score_only(candidate: Mapping[str, Any], cfg: AlphaConfig | None = None) -> float:
    return float(score_candidate(candidate, cfg).score)


def passes_alpha(
    candidate: Mapping[str, Any], cfg: AlphaConfig | None = None
) -> tuple[bool, AlphaResult]:
    cfg = cfg or AlphaConfig.from_env()
    res = score_candidate(candidate, cfg)
    return (res.score >= cfg.threshold), res


def select_top(
    candidates: Iterable[Mapping[str, Any]],
    limit: int | None = None,
    cfg: AlphaConfig | None = None,
) -> list[tuple[Mapping[str, Any], AlphaResult]]:
    """
    Rank candidates by alpha score (descending). Returns list of (candidate, result).
    """
    cfg = cfg or AlphaConfig.from_env()
    lim = int(limit if limit is not None else cfg.max_tradeable)
    scored: list[tuple[Mapping[str, Any], AlphaResult]] = []
    for c in candidates:
        try:
            scored.append((c, score_candidate(c, cfg)))
        except Exception:
            # never crash pipeline due to one bad candidate
            log.debug("score_candidate failed for a candidate", exc_info=True)
            continue

    scored.sort(key=lambda x: x[1].score, reverse=True)
    return scored[: max(0, lim)]


# -----------------------------------------------------------------------------
# Backwards-compatible constants (your old file exposed these)
# -----------------------------------------------------------------------------
THRESHOLD = AlphaConfig.from_env().threshold
MAX_TRADEABLE = AlphaConfig.from_env().max_tradeable


# -----------------------------------------------------------------------------
# Tiny self-test (manual)
# -----------------------------------------------------------------------------
if __name__ == "__main__":
    # Minimal sanity checks (no network, no imports)
    demo = {
        "pool_age_min": 6,
        "liq_usd": 22000,
        "impact_1k_bps": 420,
        "chg5": 4.2,
        "buys5": 6,
        "sells5": 2,
        "source": "pumpfun",
        "smart_wallet_score": 0.35,
    }
    ok, res = passes_alpha(demo)
    print("ok:", ok)
    print("score:", res.score)
    print("components:", res.components)
    print("reasons:", res.reasons)
