# utils_pkg/scoring_service.py
# -*- coding: utf-8 -*-
from __future__ import annotations

"""
Scoring Service (V3+++ / quant-grade)
=====================================

Single, unified scoring entrypoint used by scanners / traders / radar:

    from utils_pkg.scoring_service import ScoredToken, score_candidate

    scored = await score_candidate(raw_pool)
    print(scored.combined_score, scored.label, scored.reasons)

High-level workflow
-------------------
1. Normalize raw pool meta (mint, symbol, liq, age, etc.).
2. Build a feature dict (FeatureBuilder) if available.
3. Ask AI entry logic / AI risk model (if present) for scores.
4. Fall back to deterministic heuristics when AI not available.
5. Combine into:
      - momentum_score (0..100)
      - risk_score     (0..100; higher = riskier)
      - quality_score  (0..100)
      - combined_score (0..100; higher = better)
6. Derive a GREEN / YELLOW / RED label + human-readable reasons.

This module is *intentionally* defensive:
- If any optional dependency is missing or has a different function
  signature, we gracefully fall back without crashing (and log why).

Environment tuning knobs
------------------------
All are optional; sensible defaults are provided.

  SCORING_MIN_LIQ_USD            (default: 3_000)
  SCORING_MAX_AGE_MIN            (default: 720)
  SCORING_HIGH_SCORE_THRESHOLD   (default: 80)
  SCORING_MEDIUM_SCORE_THRESHOLD (default: 60)
  SCORING_MIN_MOMO_GREEN         (default: 70)
  SCORING_MAX_RISK_GREEN         (default: 70)
  SCORING_MIN_QUALITY_GREEN      (default: 60)

Weights for combined_score:

  SCORING_W_AI       (default: 0.35)
  SCORING_W_MOMENTUM (default: 0.35)
  SCORING_W_QUALITY  (default: 0.20)
  SCORING_W_RISK     (default: 0.10)

Notes
-----
- combined_score is "higher is better" (we invert risk internally).
- risk_score is "higher is more dangerous" (0 = risk-free, 100 = max risk).
"""

import math
import os
import time
import inspect
from dataclasses import dataclass, asdict
from typing import Any, Dict, Optional, Iterable, List, Tuple

from logging_setup import get_logger

# ──────────────────────────────────────────────────────────────────────────────
# Optional imports (feature builder / AI logic / risk)
# ──────────────────────────────────────────────────────────────────────────────

try:
    from utils_pkg import feature_builder as FB  # type: ignore
except Exception:  # pragma: no cover - optional
    FB = None  # type: ignore

try:
    from utils_pkg import ai_entry_logic as AEL  # type: ignore
except Exception:  # pragma: no cover - optional
    AEL = None  # type: ignore

try:
    from utils_pkg import ai_risk_model as ARM  # type: ignore
except Exception:  # pragma: no cover - optional
    ARM = None  # type: ignore

try:
    from utils_pkg import ai_buy_scoring as ABS  # type: ignore
except Exception:  # pragma: no cover - optional
    ABS = None  # type: ignore

log = get_logger(__name__)

# safety shim so log.every(...) always exists
if not hasattr(log, "every"):
    def _every(_key: str, n: int = 60) -> bool:
        # Very simple: log every n-th time per key (could be upgraded later)
        # Here we just always log for simplicity.
        return True
    setattr(log, "every", _every)


# ──────────────────────────────────────────────────────────────────────────────
# Configuration dataclass
# ──────────────────────────────────────────────────────────────────────────────

@dataclass
class ScoreConfig:
    min_liq_usd: float = 3_000.0
    max_age_min: float = 720.0

    high_score_threshold: float = 80.0
    medium_score_threshold: float = 60.0

    min_momentum_for_green: float = 70.0
    max_risk_for_green: float = 70.0
    min_quality_for_green: float = 60.0

    w_ai: float = 0.35
    w_momentum: float = 0.35
    w_quality: float = 0.20
    w_risk: float = 0.10  # this is applied to (100 - risk_score) internally

    @classmethod
    def from_env(cls) -> "ScoreConfig":
        def _f(name: str, default: float) -> float:
            v = os.getenv(name)
            if v is None:
                return default
            try:
                return float(v)
            except ValueError:
                log.warning("SCORING: env %s=%r is not a float, using default=%s", name, v, default)
                return default

        cfg = cls(
            min_liq_usd=_f("SCORING_MIN_LIQ_USD", 3_000.0),
            max_age_min=_f("SCORING_MAX_AGE_MIN", 720.0),
            high_score_threshold=_f("SCORING_HIGH_SCORE_THRESHOLD", 80.0),
            medium_score_threshold=_f("SCORING_MEDIUM_SCORE_THRESHOLD", 60.0),
            min_momentum_for_green=_f("SCORING_MIN_MOMO_GREEN", 70.0),
            max_risk_for_green=_f("SCORING_MAX_RISK_GREEN", 70.0),
            min_quality_for_green=_f("SCORING_MIN_QUALITY_GREEN", 60.0),
            w_ai=_f("SCORING_W_AI", 0.35),
            w_momentum=_f("SCORING_W_MOMENTUM", 0.35),
            w_quality=_f("SCORING_W_QUALITY", 0.20),
            w_risk=_f("SCORING_W_RISK", 0.10),
        )

        # Normalize weights in case user did something weird.
        total_w = cfg.w_ai + cfg.w_momentum + cfg.w_quality + cfg.w_risk
        if total_w <= 0:
            log.warning("SCORING: total weights <= 0, resetting to defaults")
            return cls()

        cfg.w_ai /= total_w
        cfg.w_momentum /= total_w
        cfg.w_quality /= total_w
        cfg.w_risk /= total_w

        return cfg


CONFIG = ScoreConfig.from_env()


# ──────────────────────────────────────────────────────────────────────────────
# Core data model
# ──────────────────────────────────────────────────────────────────────────────

@dataclass
class ScoredToken:
    # Basic identity
    mint: str
    symbol: str
    chain: str

    # Raw structural stats
    liq_usd: float
    age_min: float

    # Component scores (0..100)
    risk_score: float
    momentum_score: float
    quality_score: float
    ai_score: float

    # Combined + label
    combined_score: float
    label: str  # "GREEN", "YELLOW", "RED"

    # Explanations
    reasons: str

    # Backing features (for DB / debugging / analytics)
    raw_features: Dict[str, Any]

    # Meta
    created_at_ts: float  # unix timestamp (UTC)

    # Optional gate info for trading usage
    is_trade_viable: bool
    trade_gate_reason: str

    def to_dict(self) -> Dict[str, Any]:
        """Convenience for JSON / DB insertion."""
        return asdict(self)


# ──────────────────────────────────────────────────────────────────────────────
# Small helpers
# ──────────────────────────────────────────────────────────────────────────────

def _clamp(x: float, lo: float = 0.0, hi: float = 100.0) -> float:
    return max(lo, min(hi, x))


def _first_present(*vals: Any, default: float | None = None) -> Optional[float]:
    for v in vals:
        if v is None:
            continue
        try:
            # we treat bools as ints; filter them out
            if isinstance(v, bool):
                continue
            return float(v)
        except (TypeError, ValueError):
            continue
    return default


async def _maybe_await(result: Any) -> Any:
    if inspect.isawaitable(result):
        return await result
    return result


# ──────────────────────────────────────────────────────────────────────────────
# Pool meta normalization
# ──────────────────────────────────────────────────────────────────────────────

def _normalize_pool_meta(raw_pool: Dict[str, Any]) -> Dict[str, Any]:
    """
    Try to normalize a heterogeneous raw pool dict to a minimal schema.

    Expected / normalized keys:
        mint      : str
        symbol    : str
        chain     : str (default 'SOL')
        liq_usd   : float
        age_min   : float
    """
    mint = (
        raw_pool.get("mint")
        or raw_pool.get("base_mint")
        or raw_pool.get("baseMint")
        or raw_pool.get("mint_address")
        or raw_pool.get("base_token_mint")
        or ""
    )
    symbol = (
        raw_pool.get("symbol")
        or raw_pool.get("sym")
        or raw_pool.get("base_symbol")
        or raw_pool.get("baseSymbol")
        or "UNKNOWN"
    )
    chain = raw_pool.get("chain") or "SOL"

    # Liquidity USD
    liq_usd = _first_present(
        raw_pool.get("liq_usd"),
        raw_pool.get("liquidity_usd"),
        raw_pool.get("liquidity"),
        raw_pool.get("liq"),
        default=0.0,
    ) or 0.0

    # Age minutes
    age_min = _first_present(
        raw_pool.get("age_min"),
        raw_pool.get("ageMinutes"),
        raw_pool.get("age_minute"),
        default=0.0,
    ) or 0.0

    return {
        "mint": str(mint),
        "symbol": str(symbol),
        "chain": str(chain),
        "liq_usd": float(liq_usd),
        "age_min": float(age_min),
    }


# ──────────────────────────────────────────────────────────────────────────────
# Feature builder adapter
# ──────────────────────────────────────────────────────────────────────────────

async def _build_features(raw_pool: Dict[str, Any],
                          context: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """
    Adapter around utils_pkg.feature_builder (if present).

    We try several common function names / signatures to avoid tight coupling.
    Fallback: return {} (pure heuristic scoring still works).
    """
    if FB is None:
        if log.every("SCORING_NO_FB", 600):
            log.warning("SCORING: feature_builder not available; using empty features.")
        return {}

    # Try in order: build_entry_features, build_features_for_pool, build_features
    candidates = [
        "build_entry_features",
        "build_features_for_pool",
        "build_features",
    ]

    for fname in candidates:
        fn = getattr(FB, fname, None)
        if not callable(fn):
            continue

        try:
            res = fn(raw_pool, context=context or {})
        except TypeError:
            # maybe it only accepts raw_pool
            try:
                res = fn(raw_pool)
            except TypeError:
                continue

        try:
            features = await _maybe_await(res)
        except Exception as exc:  # pragma: no cover
            if log.every(f"SCORING_FB_ERR_{fname}", 600):
                log.exception("SCORING: feature_builder.%s failed: %s", fname, exc)
            continue

        if not isinstance(features, dict):
            if log.every(f"SCORING_FB_BAD_{fname}", 600):
                log.warning("SCORING: feature_builder.%s returned non-dict type=%s",
                            fname, type(features))
            continue

        return features

    if log.every("SCORING_FB_NO_FUNC", 600):
        log.warning(
            "SCORING: no usable feature_builder function found; using empty features."
        )
    return {}


# ──────────────────────────────────────────────────────────────────────────────
# AI entry logic adapter
# ──────────────────────────────────────────────────────────────────────────────

def _call_ai_entry_logic(features: Dict[str, Any],
                         pool_meta: Dict[str, Any],
                         context: Optional[Dict[str, Any]] = None
                         ) -> Tuple[Optional[float], str]:
    """
    Ask utils_pkg.ai_entry_logic (if present) for an entry score in [0, 1] or [0, 100].

    Returns:
        (ai_score_0_100_or_None, reason_string)
    """
    if AEL is None:
        return None, "ai_entry_logic not available"

    # Try 'score_entry' first (our preferred API)
    for fname in ("score_entry", "score_only", "score"):
        fn = getattr(AEL, fname, None)
        if not callable(fn):
            continue

        try:
            res = fn(
                features=features,
                meta={
                    "mint": pool_meta["mint"],
                    "symbol": pool_meta["symbol"],
                    "chain": pool_meta["chain"],
                    **(context or {}),
                },
            )
        except TypeError:
            # fallback: assume single positional arg
            try:
                res = fn(features)
            except TypeError:
                continue

        # Normalize result
        score_0_100: Optional[float] = None
        reason = ""

        if isinstance(res, (float, int)):
            score_0_100 = float(res)
        elif isinstance(res, dict):
            sc = res.get("score")
            if sc is not None:
                score_0_100 = float(sc)
            reason = str(res.get("reason") or res.get("why") or "")
        elif isinstance(res, (list, tuple)):
            # (score, reason, extra?) or (score, reason)
            if len(res) >= 1:
                try:
                    score_0_100 = float(res[0])
                except (TypeError, ValueError):
                    pass
            if len(res) >= 2:
                reason = str(res[1])

        if score_0_100 is None:
            continue

        # If score seems to be 0..1, rescale
        if 0.0 <= score_0_100 <= 1.0:
            score_0_100 *= 100.0

        score_0_100 = _clamp(score_0_100)
        return score_0_100, reason or "ai_entry_logic used"

    return None, "ai_entry_logic has no usable scoring function"


# ──────────────────────────────────────────────────────────────────────────────
# AI risk model adapter (optional)
# ──────────────────────────────────────────────────────────────────────────────

def _call_ai_risk_model(features: Dict[str, Any],
                        pool_meta: Dict[str, Any],
                        context: Optional[Dict[str, Any]] = None
                        ) -> Tuple[Optional[float], str]:
    """
    Ask utils_pkg.ai_risk_model (if present) for a risk score in [0, 100].

    Returns:
        (risk_score_or_None, reason)
    """
    if ARM is None:
        return None, "ai_risk_model not available"

    for fname in ("score_risk", "risk_score", "score"):
        fn = getattr(ARM, fname, None)
        if not callable(fn):
            continue

        try:
            res = fn(
                features=features,
                meta={
                    "mint": pool_meta["mint"],
                    "symbol": pool_meta["symbol"],
                    "chain": pool_meta["chain"],
                    **(context or {}),
                },
            )
        except TypeError:
            try:
                res = fn(features)
            except TypeError:
                continue

        risk_score = None
        reason = ""

        if isinstance(res, (float, int)):
            risk_score = float(res)
        elif isinstance(res, dict):
            sc = res.get("risk_score") or res.get("score")
            if sc is not None:
                risk_score = float(sc)
            reason = str(res.get("reason") or res.get("why") or "")
        elif isinstance(res, (tuple, list)):
            if len(res) >= 1:
                try:
                    risk_score = float(res[0])
                except (TypeError, ValueError):
                    pass
            if len(res) >= 2:
                reason = str(res[1])

        if risk_score is None:
            continue

        # risk_score is defined as [0..100], higher = more dangerous
        if 0.0 <= risk_score <= 1.0:
            # if they gave a 0..1 risk probability, scale up
            risk_score *= 100.0

        risk_score = _clamp(risk_score)
        return risk_score, reason or "ai_risk_model used"

    return None, "ai_risk_model has no usable scoring function"


# ──────────────────────────────────────────────────────────────────────────────
# Heuristic component calculators
# ──────────────────────────────────────────────────────────────────────────────

def _compute_momentum_score(features: Dict[str, Any],
                            raw_pool: Dict[str, Any]) -> Tuple[float, str]:
    """
    Compute a heuristic momentum score [0..100].

    Uses whatever is available:
        - momo_5m_pct / momo_1m_pct / etc.
        - buys / sells counts
        - raw chg5 from Raydium-style feeds
    """
    # Try to find a short-horizon price change
    chg_short = _first_present(
        features.get("momo_1m_pct"),
        features.get("pct_change_1m"),
        features.get("chg_1m_pct"),
        features.get("chg_5m_pct"),
        raw_pool.get("chg5"),
        default=0.0,
    ) or 0.0

    # buys vs sells (5m window maybe)
    buys = _first_present(
        features.get("buys_5m"),
        features.get("buys_1m"),
        raw_pool.get("buys5"),
        default=0.0,
    ) or 0.0
    sells = _first_present(
        features.get("sells_5m"),
        features.get("sells_1m"),
        raw_pool.get("sells5"),
        default=0.0,
    ) or 0.0

    # Normalize change: 0%..+30% => 0..100; negative => penalty
    if chg_short <= 0:
        base = 20.0 + max(-30.0, chg_short)  # negative change drags it down
    else:
        base = min(100.0, chg_short / 30.0 * 100.0)

    # Buy/sell imbalance: strong skew to buys increases momentum
    total_trades = buys + sells
    if total_trades > 0:
        buy_ratio = buys / total_trades
    else:
        buy_ratio = 0.5

    imbalance_boost = (buy_ratio - 0.5) * 60.0  # -30..+30
    score = _clamp(base + imbalance_boost)

    reason_parts = [
        f"short-term change ≈ {chg_short:.1f}%",
        f"buys/sells={int(buys)}/{int(sells)} (buy_ratio={buy_ratio:.2f})",
    ]
    return score, "; ".join(reason_parts)


def _compute_risk_score(features: Dict[str, Any],
                        pool_meta: Dict[str, Any],
                        ai_risk_score: Optional[float]) -> Tuple[float, str]:
    """
    Compute heuristic risk [0..100], higher = more dangerous.

    We start from AI risk if present, then adjust for obvious hazards:
        - suspicious honeypot / rug flags
        - very low liquidity
        - extremely young pools
        - extreme concentration of holders
    """
    base = ai_risk_score if ai_risk_score is not None else 50.0
    reasons: List[str] = []
    if ai_risk_score is not None:
        reasons.append(f"AI risk={ai_risk_score:.1f}")

    liq = pool_meta["liq_usd"]
    age = pool_meta["age_min"]

    # Liquidity: very low = very risky
    if liq < CONFIG.min_liq_usd:
        excess = CONFIG.min_liq_usd - liq
        bump = min(25.0, excess / CONFIG.min_liq_usd * 50.0)
        base += bump
        reasons.append(f"low liq (${liq:,.0f} < ${CONFIG.min_liq_usd:,.0f}) (+{bump:.1f})")
    else:
        reasons.append("liq OK")

    # Age: super fresh pools can be rug-prone, but older than max_age_min = decaying risk
    if age < 30.0:
        base += 10.0
        reasons.append("very young pool (<30min) (+10)")
    elif age < 120.0:
        base += 5.0
        reasons.append("young pool (<2h) (+5)")
    elif age > CONFIG.max_age_min:
        base -= 5.0
        reasons.append(f"older than {CONFIG.max_age_min:.0f}min (-5)")

    # Explicit flags from features if available
    honeypot_flags = [
        features.get("is_honeypot"),
        features.get("honeypot_flag"),
        features.get("rugpull_flag"),
        features.get("can_sell") is False,
    ]
    if any(bool(f) for f in honeypot_flags):
        base = max(base, 90.0)
        reasons.append("honeypot / rug flag detected (risk>=90)")

    # Big holder concentration
    top1 = _first_present(
        features.get("top_holder_pct"),
        features.get("top1_holder_pct"),
        default=None,
    )
    top10 = _first_present(
        features.get("top10_holder_pct"),
        default=None,
    )
    if top1 is not None and top1 > 40.0:
        base += 15.0
        reasons.append(f"top1 holder {top1:.1f}% (+15)")
    if top10 is not None and top10 > 80.0:
        base += 10.0
        reasons.append(f"top10 holders {top10:.1f}% (+10)")

    # Clamp
    risk = _clamp(base)
    return risk, "; ".join(reasons)


def _compute_quality_score(features: Dict[str, Any],
                           pool_meta: Dict[str, Any]) -> Tuple[float, str]:
    """
    Quality tries to capture 'structure' vs pure momo/risk:
        - adequate, but not insane, liquidity
        - reasonable age (not 30 seconds, not fully dead yet)
        - holder count
        - volume consistency (if available)
    """
    liq = pool_meta["liq_usd"]
    age = pool_meta["age_min"]

    score = 50.0
    reasons: List[str] = []

    # Liquidity sweet spot: between min_liq_usd and maybe 30x that
    if liq < CONFIG.min_liq_usd:
        # we already penalized risk; here we just keep quality modest
        reasons.append(f"low liq (${liq:,.0f})")
    else:
        # map liq from [min_liq, 30x min_liq] → [60..95]
        hi = CONFIG.min_liq_usd * 30.0
        liq_clamped = _clamp(liq, CONFIG.min_liq_usd, hi)
        frac = (liq_clamped - CONFIG.min_liq_usd) / (hi - CONFIG.min_liq_usd + 1e-9)
        liq_score = 60.0 + frac * 35.0
        score = max(score, liq_score)
        reasons.append(f"liq in good range (${liq:,.0f}) → ~{liq_score:.1f}")

    # Age: super fresh <10min => quality a bit unknown, older 30min+ slightly better
    if age < 10.0:
        score -= 5.0
        reasons.append("very new (<10min)")
    elif age < 60.0:
        score += 5.0
        reasons.append("new but maturing (<1h)")
    else:
        score += 0.0

    # Holder count
    holders = _first_present(
        features.get("holder_count"),
        features.get("holders"),
        default=None,
    )
    if holders is not None:
        if holders < 50:
            score -= 5.0
            reasons.append(f"low holder count ({int(holders)})")
        elif holders < 300:
            score += 0.0
            reasons.append(f"moderate holder count ({int(holders)})")
        else:
            score += 5.0
            reasons.append(f"good holder count ({int(holders)})")

    return _clamp(score), "; ".join(reasons)


# ──────────────────────────────────────────────────────────────────────────────
# Combined score + labeling
# ──────────────────────────────────────────────────────────────────────────────

def _combine_scores(momentum_score: float,
                    risk_score: float,
                    quality_score: float,
                    ai_entry_score: Optional[float]) -> float:
    """
    Combine component scores into a single [0..100] combined_score.

    Higher = better opportunity. Risk is inverted because higher risk_score
    means more dangerous.
    """
    ai_sc = ai_entry_score if ai_entry_score is not None else (momentum_score + quality_score) / 2.0
    safe_risk = _clamp(risk_score)

    # Invert risk: 0 risk → 100 contribution, 100 risk → 0
    risk_contrib = 100.0 - safe_risk

    combined = (
        CONFIG.w_ai * ai_sc
        + CONFIG.w_momentum * momentum_score
        + CONFIG.w_quality * quality_score
        + CONFIG.w_risk * risk_contrib
    )

    return _clamp(combined)


def _derive_label(combined_score: float,
                  momentum_score: float,
                  risk_score: float,
                  quality_score: float) -> str:
    """
    Label:
        GREEN  - strong combined score + acceptable risk / quality
        YELLOW - medium combined score OR some blemishes
        RED    - low combined score or very risky
    """
    if (
        combined_score >= CONFIG.high_score_threshold
        and momentum_score >= CONFIG.min_momentum_for_green
        and risk_score <= CONFIG.max_risk_for_green
        and quality_score >= CONFIG.min_quality_for_green
    ):
        return "GREEN"

    if combined_score >= CONFIG.medium_score_threshold and risk_score <= 90.0:
        return "YELLOW"

    return "RED"


def _format_reasons(momentum_score: float,
                    momentum_reason: str,
                    risk_score: float,
                    risk_reason: str,
                    quality_score: float,
                    quality_reason: str,
                    ai_entry_score: Optional[float],
                    ai_reason: str,
                    combined_score: float,
                    label: str) -> str:
    parts: List[str] = []

    parts.append(f"Label={label}, combined={combined_score:.1f}/100")

    if ai_entry_score is not None:
        parts.append(f"AI entry score={ai_entry_score:.1f}/100 ({ai_reason})")
    else:
        parts.append(f"AI entry score=N/A ({ai_reason})")

    parts.append(f"Momentum={momentum_score:.1f}/100 ({momentum_reason})")
    parts.append(f"Risk={risk_score:.1f}/100 (higher=worse; {risk_reason})")
    parts.append(f"Quality={quality_score:.1f}/100 ({quality_reason})")

    return " | ".join(parts)


# ──────────────────────────────────────────────────────────────────────────────
# Trade viability (optional extra for sniper usage)
# ──────────────────────────────────────────────────────────────────────────────

def _derive_trade_gate(pool_meta: Dict[str, Any],
                       combined_score: float,
                       label: str) -> Tuple[bool, str]:
    """
    Simple, generic gate hint for actual trading.

    We keep this conservative; radar / alert logic can use looser thresholds.
    """
    if pool_meta["liq_usd"] < CONFIG.min_liq_usd:
        return False, f"liq ${pool_meta['liq_usd']:.0f} < min ${CONFIG.min_liq_usd:.0f}"

    if pool_meta["age_min"] > CONFIG.max_age_min:
        return False, f"age {pool_meta['age_min']:.1f}min > max {CONFIG.max_age_min:.1f}min"

    if label == "RED":
        return False, "RED label (combined score too low)"

    if combined_score < CONFIG.medium_score_threshold:
        return False, f"combined score {combined_score:.1f} < medium threshold {CONFIG.medium_score_threshold:.1f}"

    return True, "OK"


# ──────────────────────────────────────────────────────────────────────────────
# Public API
# ──────────────────────────────────────────────────────────────────────────────

async def score_candidate(raw_pool: Dict[str, Any],
                          context: Optional[Dict[str, Any]] = None) -> ScoredToken:
    """
    Main scoring entrypoint.

    Args:
        raw_pool: dict from scanner (Raydium, Pump.fun, Dexscreener, etc.)
        context: optional extra info (regime, account, etc.)

    Returns:
        ScoredToken instance with component scores, label, reasons, raw features.
    """
    if context is None:
        context = {}

    pool_meta = _normalize_pool_meta(raw_pool)

    # Step 1: features
    features = await _build_features(raw_pool, context=context)

    # Step 2: AI entry score (if available)
    ai_entry_score, ai_reason = _call_ai_entry_logic(features, pool_meta, context=context)

    # Step 3: AI risk (if available)
    ai_risk_score, ai_risk_reason = _call_ai_risk_model(features, pool_meta, context=context)

    # Step 4: heuristics
    momentum_score, momentum_reason = _compute_momentum_score(features, raw_pool)
    risk_score, risk_reason_h = _compute_risk_score(features, pool_meta, ai_risk_score)
    quality_score, quality_reason = _compute_quality_score(features, pool_meta)

    # Combine AI risk reason with heuristic risk reason
    risk_reason = "; ".join([r for r in [ai_risk_reason, risk_reason_h] if r])

    # Step 5: combined score + label
    combined_score = _combine_scores(momentum_score, risk_score, quality_score, ai_entry_score)
    label = _derive_label(combined_score, momentum_score, risk_score, quality_score)

    # Step 6: formatted reasons
    reasons = _format_reasons(
        momentum_score=momentum_score,
        momentum_reason=momentum_reason,
        risk_score=risk_score,
        risk_reason=risk_reason,
        quality_score=quality_score,
        quality_reason=quality_reason,
        ai_entry_score=ai_entry_score,
        ai_reason=ai_reason,
        combined_score=combined_score,
        label=label,
    )

    # Step 7: trade gate hint
    is_trade_viable, gate_reason = _derive_trade_gate(pool_meta, combined_score, label)

    scored = ScoredToken(
        mint=pool_meta["mint"],
        symbol=pool_meta["symbol"],
        chain=pool_meta["chain"],
        liq_usd=pool_meta["liq_usd"],
        age_min=pool_meta["age_min"],
        risk_score=_clamp(risk_score),
        momentum_score=_clamp(momentum_score),
        quality_score=_clamp(quality_score),
        ai_score=_clamp(ai_entry_score) if ai_entry_score is not None else 0.0,
        combined_score=_clamp(combined_score),
        label=label,
        reasons=reasons,
        raw_features=features,
        created_at_ts=time.time(),
        is_trade_viable=is_trade_viable,
        trade_gate_reason=gate_reason,
    )

    if log.every("SCORING_SAMPLE", 60):
        log.info(
            "SCORING: %s (%s) label=%s combined=%.1f liq=$%.0f age=%.1fmin",
            scored.symbol,
            scored.mint,
            scored.label,
            scored.combined_score,
            scored.liq_usd,
            scored.age_min,
        )

    return scored


async def score_many(raw_pools: Iterable[Dict[str, Any]],
                     context: Optional[Dict[str, Any]] = None) -> List[ScoredToken]:
    """
    Convenience helper: score many raw pools (sequentially for now).

    You can parallelize at the caller level if needed.
    """
    results: List[ScoredToken] = []
    for raw in raw_pools:
        try:
            scored = await score_candidate(raw, context=context)
            results.append(scored)
        except Exception as exc:  # pragma: no cover
            if log.every("SCORING_MANY_ERR", 60):
                log.exception("SCORING: error scoring pool %r: %s", raw.get("mint"), exc)
    return results
