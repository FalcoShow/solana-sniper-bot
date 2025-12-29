# utils_pkg/scoring_service.py
# -*- coding: utf-8 -*-
from __future__ import annotations

"""
Scoring Service — God-Tier V3+++ / quant-grade (Phase 0–7)
==========================================================

Single unified scoring + trade-gate entrypoint for *all* scanners/traders:

    from utils_pkg.scoring_service import ScoredToken, score_candidate, score_many

Why this exists
---------------
- One stable API across Raydium / Pump.fun / Dexscreener / radar feeds.
- Defensive: never hard-fails when optional modules are missing or signatures differ.
- Quant-ish: component scores 0..100 + combined score 0..100 + label.
- Actionable: reasons + gate result + sizing hint (if present in features).
- Fast: optional in-memory TTL cache (and max-size eviction).

Soft integrations (best-effort, lazy to avoid cycles)
-----------------------------------------------------
- utils_pkg.feature_builder         (canonical feature vector, entry_quality/entry_risk)
- utils_pkg.ai_entry_logic          (AI entry score / routing)
- utils_pkg.ai_risk_model           (AI risk score)
- utils_pkg.ai_buy_scoring          (hard gate + optional score)
- utils_pkg.regime                  (optional context; tighten in risk-off/manip)

Contract
--------
- score_candidate(raw_pool: dict, context: dict|None) -> ScoredToken
- raw_pool can be any scanner dict; we normalize it broadly.
- context is optional, can include:
    {
      "regime": RegimeState | dict,
      "risk_context": dict,
      "wallet_signals": dict,
      "ai_signals": dict,
      "history": list[dict],
      "scanner_source": "raydium"|"dexscreener"|"pumpfun"|"momo"|...
    }

No network / DB calls here.
"""

import asyncio
import contextlib
import inspect
import math
import os
import time
from collections import OrderedDict
from dataclasses import dataclass, asdict
from typing import Any, Dict, Iterable, List, Optional, Tuple


# ──────────────────────────────────────────────────────────────────────────────
# Logging (robust)
# ──────────────────────────────────────────────────────────────────────────────
try:
    from logging_setup import get_logger  # type: ignore

    try:
        log = get_logger(__name__)  # type: ignore[arg-type]
    except TypeError:
        log = get_logger()  # type: ignore[misc]
except Exception:  # pragma: no cover
    import logging

    log = logging.getLogger("utils_pkg.scoring_service")
    if not log.handlers:
        logging.basicConfig(level=logging.INFO)

# log.every shim (per-key "log every N occurrences")
if not hasattr(log, "every"):
    _EVERY_STATE: Dict[str, int] = {}

    def _every(key: str, n: int = 60) -> bool:
        try:
            n = int(n)
        except Exception:
            n = 60
        if n <= 1:
            return True
        c = _EVERY_STATE.get(key, 0) + 1
        _EVERY_STATE[key] = c
        return (c % n) == 1

    setattr(log, "every", _every)


# ──────────────────────────────────────────────────────────────────────────────
# Small helpers
# ──────────────────────────────────────────────────────────────────────────────
def _clamp(x: Any, lo: float = 0.0, hi: float = 100.0) -> float:
    try:
        v = float(x)
    except Exception:
        return float(lo)
    if v != v or math.isinf(v):  # NaN/inf
        return float(lo)
    return float(max(lo, min(hi, v)))


def _clip01(x: Any) -> float:
    return _clamp(x, 0.0, 1.0)


def _safe_ratio(num: float, den: float, default: float = 0.0) -> float:
    try:
        d = float(den)
        if d == 0.0:
            return float(default)
        return float(num) / d
    except Exception:
        return float(default)


def _log1p_pos(x: float) -> float:
    try:
        x = float(x)
        if x <= 0.0:
            return 0.0
        return float(math.log1p(x))
    except Exception:
        return 0.0


def _first_present(*vals: Any, default: float | None = None) -> Optional[float]:
    for v in vals:
        if v is None:
            continue
        if isinstance(v, bool):
            continue
        try:
            return float(v)
        except Exception:
            continue
    return default


def _age_min_from_ts(ts: Any) -> Optional[float]:
    try:
        if ts is None:
            return None
        t = float(ts)
        if t <= 0:
            return None
        # ms epoch → s
        if t > 10_000_000_000:
            t = t / 1000.0
        return max(0.0, (time.time() - t) / 60.0)
    except Exception:
        return None


def _normalize_mint(m: Any) -> str:
    s = str(m or "").strip()
    if not s:
        return ""
    # Prefer your SSoT if it exists
    with contextlib.suppress(Exception):
        from utils_pkg.mints import normalize_mint  # type: ignore

        return str(normalize_mint(s)).strip()
    with contextlib.suppress(Exception):
        from utils_pkg.mint_guard import normalize_from_any  # type: ignore

        return str(normalize_from_any(s)).strip()
    return s


def _b_env(name: str, default: bool = False) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return str(v).strip().lower() in {"1", "true", "t", "yes", "y", "on"}


def _f_env(name: str, default: float) -> float:
    v = os.getenv(name)
    if v is None or v == "":
        return float(default)
    try:
        return float(v)
    except Exception:
        if log.every(f"SCORING_BAD_FLOAT_{name}", 600):
            log.warning("SCORING: env %s=%r not float; using default=%s", name, v, default)
        return float(default)


def _i_env(name: str, default: int) -> int:
    v = os.getenv(name)
    if v is None or v == "":
        return int(default)
    try:
        return int(float(v))
    except Exception:
        if log.every(f"SCORING_BAD_INT_{name}", 600):
            log.warning("SCORING: env %s=%r not int; using default=%s", name, v, default)
        return int(default)


# ──────────────────────────────────────────────────────────────────────────────
# Config
# ──────────────────────────────────────────────────────────────────────────────
@dataclass
class ScoreConfig:
    # Structural constraints (trade viability)
    min_liq_usd: float = 3_000.0
    max_age_min: float = 720.0

    # Label thresholds
    high_score_threshold: float = 80.0
    medium_score_threshold: float = 60.0

    # GREEN component constraints
    min_momentum_for_green: float = 70.0
    max_risk_for_green: float = 70.0
    min_quality_for_green: float = 60.0

    # Combined score weights (normalized on load)
    w_ai: float = 0.35
    w_momentum: float = 0.35
    w_quality: float = 0.20
    w_risk: float = 0.10  # applied to (100 - risk)

    # Optional tightening/loosening
    regime_risk_penalty: float = 12.0
    manip_risk_penalty: float = 18.0

    # Cache controls
    cache_ttl_s: float = 0.0        # 0 disables TTL caching
    cache_max_items: int = 2048     # eviction if ttl enabled (or manually enabled)

    # Feature toggles
    enable_ai_entry: bool = True
    enable_ai_risk: bool = True
    enable_abs_gate: bool = True
    prefer_abs_ai_score: bool = False

    # Parallel scoring helper default (score_many_parallel)
    max_concurrency: int = 12

    @classmethod
    def from_env(cls) -> "ScoreConfig":
        cfg = cls(
            min_liq_usd=_f_env("SCORING_MIN_LIQ_USD", 3_000.0),
            max_age_min=_f_env("SCORING_MAX_AGE_MIN", 720.0),
            high_score_threshold=_f_env("SCORING_HIGH_SCORE_THRESHOLD", 80.0),
            medium_score_threshold=_f_env("SCORING_MEDIUM_SCORE_THRESHOLD", 60.0),
            min_momentum_for_green=_f_env("SCORING_MIN_MOMO_GREEN", 70.0),
            max_risk_for_green=_f_env("SCORING_MAX_RISK_GREEN", 70.0),
            min_quality_for_green=_f_env("SCORING_MIN_QUALITY_GREEN", 60.0),
            w_ai=_f_env("SCORING_W_AI", 0.35),
            w_momentum=_f_env("SCORING_W_MOMENTUM", 0.35),
            w_quality=_f_env("SCORING_W_QUALITY", 0.20),
            w_risk=_f_env("SCORING_W_RISK", 0.10),
            regime_risk_penalty=_f_env("SCORING_REGIME_RISK_PENALTY", 12.0),
            manip_risk_penalty=_f_env("SCORING_MANIP_RISK_PENALTY", 18.0),
            cache_ttl_s=_f_env("SCORING_CACHE_TTL_S", 0.0),
            cache_max_items=_i_env("SCORING_CACHE_MAX_ITEMS", 2048),
            enable_ai_entry=_b_env("SCORING_ENABLE_AI_ENTRY", True),
            enable_ai_risk=_b_env("SCORING_ENABLE_AI_RISK", True),
            enable_abs_gate=_b_env("SCORING_ENABLE_ABS_GATE", True),
            prefer_abs_ai_score=_b_env("SCORING_PREFER_ABS_AI_SCORE", False),
            max_concurrency=_i_env("SCORING_MAX_CONCURRENCY", 12),
        )

        # Normalize weights safely
        total = float(cfg.w_ai + cfg.w_momentum + cfg.w_quality + cfg.w_risk)
        if total <= 0:
            if log.every("SCORING_BAD_WEIGHTS", 600):
                log.warning("SCORING: weights sum <= 0; resetting to defaults")
            return cls()

        cfg.w_ai /= total
        cfg.w_momentum /= total
        cfg.w_quality /= total
        cfg.w_risk /= total
        cfg.cache_max_items = max(128, int(cfg.cache_max_items))
        cfg.max_concurrency = max(1, int(cfg.max_concurrency))
        return cfg


CONFIG = ScoreConfig.from_env()


def reload_config() -> ScoreConfig:
    global CONFIG
    CONFIG = ScoreConfig.from_env()
    return CONFIG


# ──────────────────────────────────────────────────────────────────────────────
# Output model
# ──────────────────────────────────────────────────────────────────────────────
@dataclass
class ScoredToken:
    mint: str
    symbol: str
    chain: str

    liq_usd: float
    age_min: float

    # Component scores (0..100)
    risk_score: float
    momentum_score: float
    quality_score: float
    ai_score: float

    combined_score: float
    label: str  # GREEN/YELLOW/RED

    reasons: str

    # Useful extras for the rest of the stack
    is_trade_viable: bool
    trade_gate_reason: str
    size_hint: float                 # 0..1 (if features provide it), else 0
    entry_block_flag: float          # 0/1 (if features provide it), else 0

    raw_features: Dict[str, Any]     # feature_builder output (best-effort)
    created_at_ts: float

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


# ──────────────────────────────────────────────────────────────────────────────
# TTL + size-bounded cache (in-memory)
# ──────────────────────────────────────────────────────────────────────────────
# We use OrderedDict so eviction is deterministic: oldest first.
_SCORE_CACHE: "OrderedDict[str, Tuple[float, ScoredToken]]" = OrderedDict()


def _cache_enabled() -> bool:
    return float(CONFIG.cache_ttl_s or 0.0) > 0.0


def _cache_get(mint: str) -> Optional[ScoredToken]:
    if not mint or not _cache_enabled():
        return None
    ttl = float(CONFIG.cache_ttl_s or 0.0)
    item = _SCORE_CACHE.get(mint)
    if not item:
        return None
    ts, val = item
    if (time.time() - ts) <= ttl:
        # touch to mark recent
        _SCORE_CACHE.move_to_end(mint, last=True)
        return val
    _SCORE_CACHE.pop(mint, None)
    return None


def _cache_put(mint: str, st: ScoredToken) -> None:
    if not mint or not _cache_enabled():
        return
    _SCORE_CACHE[mint] = (time.time(), st)
    _SCORE_CACHE.move_to_end(mint, last=True)

    # Evict if too big
    max_items = int(CONFIG.cache_max_items or 2048)
    while len(_SCORE_CACHE) > max_items:
        _SCORE_CACHE.popitem(last=False)


# ──────────────────────────────────────────────────────────────────────────────
# Pool meta normalization (broad, robust)
# ──────────────────────────────────────────────────────────────────────────────
def _normalize_pool_meta(raw_pool: Dict[str, Any]) -> Dict[str, Any]:
    mint = (
        raw_pool.get("mint")
        or raw_pool.get("base_mint")
        or raw_pool.get("baseMint")
        or raw_pool.get("mint_address")
        or raw_pool.get("base_token_mint")
        or raw_pool.get("token_mint")
        or ""
    )
    symbol = (
        raw_pool.get("symbol")
        or raw_pool.get("sym")
        or raw_pool.get("base_symbol")
        or raw_pool.get("baseSymbol")
        or raw_pool.get("ticker")
        or "UNKNOWN"
    )
    chain = raw_pool.get("chain") or raw_pool.get("chainId") or raw_pool.get("network") or "SOL"

    mint_s = _normalize_mint(mint)
    sym_s = str(symbol or "UNKNOWN").strip() or "UNKNOWN"
    chain_s = str(chain or "SOL").strip() or "SOL"

    # Liquidity
    liq_usd = _first_present(
        raw_pool.get("liq_usd"),
        raw_pool.get("liquidity_usd"),
        (raw_pool.get("liquidity") or {}).get("usd") if isinstance(raw_pool.get("liquidity"), dict) else None,
        raw_pool.get("liquidity"),
        raw_pool.get("liq"),
        default=0.0,
    ) or 0.0

    # Age
    age_min = _first_present(
        raw_pool.get("age_min"),
        raw_pool.get("token_age_min"),
        raw_pool.get("ageMinutes"),
        raw_pool.get("age_minute"),
        raw_pool.get("pool_age_min"),
        default=None,
    )
    if age_min is None or age_min <= 0:
        age_min = _age_min_from_ts(
            raw_pool.get("pairCreatedAt")
            or raw_pool.get("pairCreatedAtMs")
            or raw_pool.get("pairCreatedAtTs")
            or raw_pool.get("created_at")
            or raw_pool.get("createdAt")
            or raw_pool.get("ts_created")
        ) or 0.0

    # Common “meta” fields (best-effort)
    mcap = _first_present(
        raw_pool.get("mcap_usd"),
        raw_pool.get("mcap"),
        raw_pool.get("marketCap"),
        raw_pool.get("marketCapUsd"),
        raw_pool.get("fdv"),
        raw_pool.get("fdv_usd"),
        default=0.0,
    ) or 0.0

    # Volume / orderflow (5m preferred)
    vol5 = _first_present(
        raw_pool.get("vol_5m_usd"),
        raw_pool.get("vol5"),
        (raw_pool.get("volume") or {}).get("m5") if isinstance(raw_pool.get("volume"), dict) else None,
        (raw_pool.get("volumeUsd") or {}).get("m5") if isinstance(raw_pool.get("volumeUsd"), dict) else None,
        default=0.0,
    ) or 0.0

    buys5 = int(_first_present(raw_pool.get("buys_5m"), raw_pool.get("buys5"), default=0.0) or 0.0)
    sells5 = int(_first_present(raw_pool.get("sells_5m"), raw_pool.get("sells5"), default=0.0) or 0.0)

    # Momentum / price
    px = _first_present(raw_pool.get("price_usd"), raw_pool.get("px_usd"), raw_pool.get("priceUsd"), raw_pool.get("price"), default=0.0) or 0.0
    chg1 = _first_present(raw_pool.get("price_change_1m_pct"), raw_pool.get("chg1"), raw_pool.get("chg_1m_pct"), default=0.0) or 0.0
    chg5 = _first_present(raw_pool.get("price_change_5m_pct"), raw_pool.get("chg5"), raw_pool.get("chg_5m_pct"), default=0.0) or 0.0

    # Slippage/impact (if scanner provides)
    impact_bps = _first_present(raw_pool.get("impact_1k_bps"), raw_pool.get("impact_1000usd_bps"), raw_pool.get("priceImpact1kBps"), default=0.0) or 0.0

    # Rug/fraud (if scanner provides)
    ai_rug = _first_present(raw_pool.get("ai_rug_score_01"), raw_pool.get("ai_rug_score"), raw_pool.get("ai_fraud_score"), default=0.0) or 0.0

    return {
        "mint": mint_s,
        "symbol": sym_s,
        "chain": chain_s,
        "liq_usd": float(liq_usd),
        "age_min": float(age_min),
        "mcap_usd": float(mcap),
        "vol_5m_usd": float(vol5),
        "buys_5m": int(buys5),
        "sells_5m": int(sells5),
        "price_usd": float(px),
        "chg_1m_pct": float(chg1),
        "chg_5m_pct": float(chg5),
        "impact_1k_bps": float(impact_bps),
        "ai_rug_score": float(ai_rug),
    }


# ──────────────────────────────────────────────────────────────────────────────
# Lazy imports (avoid cycles)
# ──────────────────────────────────────────────────────────────────────────────
def _lazy_imports() -> Tuple[Any, Any, Any, Any]:
    FB = None
    AEL = None
    ARM = None
    ABS = None
    with contextlib.suppress(Exception):
        from utils_pkg import feature_builder as FB  # type: ignore
    with contextlib.suppress(Exception):
        from utils_pkg import ai_entry_logic as AEL  # type: ignore
    with contextlib.suppress(Exception):
        from utils_pkg import ai_risk_model as ARM  # type: ignore
    with contextlib.suppress(Exception):
        from utils_pkg import ai_buy_scoring as ABS  # type: ignore
    return FB, AEL, ARM, ABS


async def _maybe_await(x: Any) -> Any:
    return await x if inspect.isawaitable(x) else x


# ──────────────────────────────────────────────────────────────────────────────
# Feature builder adapter (canonical)
# ──────────────────────────────────────────────────────────────────────────────
async def _build_features(raw_pool: Dict[str, Any], pool_meta: Dict[str, Any], context: Dict[str, Any]) -> Dict[str, Any]:
    FB, _, _, _ = _lazy_imports()
    if FB is None:
        if log.every("SCORING_NO_FB", 600):
            log.warning("SCORING: feature_builder not available; using empty features.")
        return {}

    # Prefer FeatureBuilder().build(...) (your canonical API)
    with contextlib.suppress(Exception):
        cls = getattr(FB, "FeatureBuilder", None)
        if cls is not None:
            fb = cls()
            history = context.get("history")
            regime = context.get("regime")
            wallet_signals = context.get("wallet_signals")
            ai_signals = context.get("ai_signals")
            scanner_source = context.get("scanner_source")
            risk_context = context.get("risk_context")

            res = fb.build(
                symbol=pool_meta.get("symbol"),
                mint=pool_meta.get("mint"),
                snapshot=raw_pool,
                history=history,
                regime=regime,
                wallet_signals=wallet_signals,
                ai_signals=ai_signals,
                scanner_source=scanner_source,
                risk_context=risk_context,
            )
            feats = await _maybe_await(res)
            if isinstance(feats, dict):
                return feats

    # Fallback: module-level build_features(snapshot)
    for fname in ("build_features",):
        fn = getattr(FB, fname, None)
        if callable(fn):
            with contextlib.suppress(Exception):
                feats = await _maybe_await(fn(raw_pool))
                if isinstance(feats, dict):
                    return feats

    if log.every("SCORING_FB_NO_USABLE", 600):
        log.warning("SCORING: no usable feature_builder API found; using empty features.")
    return {}


# ──────────────────────────────────────────────────────────────────────────────
# AI adapters (async-safe, best-effort)
# ──────────────────────────────────────────────────────────────────────────────
def _normalize_score_reason(res: Any, *, key: str = "score") -> Tuple[Optional[float], str]:
    """
    Normalize outputs into (score, reason).

    Supported:
      - float/int
      - dict with {key} or {"score"} or {"risk_score"} and reason/why
      - tuple/list: (score, reason) or (ok, score, why) handled elsewhere
    """
    if res is None:
        return None, ""
    if isinstance(res, (int, float)) and not isinstance(res, bool):
        return float(res), ""
    if isinstance(res, dict):
        sc = res.get(key)
        if sc is None and key != "score":
            sc = res.get("score")
        why = str(res.get("reason") or res.get("why") or res.get("msg") or "")
        if sc is None:
            return None, why
        with contextlib.suppress(Exception):
            return float(sc), why
        return None, why
    if isinstance(res, (tuple, list)) and len(res) >= 1:
        sc = None
        why = ""
        with contextlib.suppress(Exception):
            sc = float(res[0])
        if len(res) >= 2:
            why = str(res[1])
        return sc, why
    return None, ""


def _clamp01_to_100(sc: float) -> float:
    sc = float(sc)
    if 0.0 <= sc <= 1.0:
        sc *= 100.0
    return _clamp(sc)


def _normalize_pass_gate(res: Any) -> Tuple[bool, Optional[float], str]:
    """
    pass_gate outputs:
      - (ok:bool, score:float, why:str)
      - (ok:bool, score:float)
      - dict {ok/allow/pass, score, why}
      - bool
    """
    if res is None:
        return True, None, ""
    if isinstance(res, bool):
        return bool(res), None, ""
    if isinstance(res, dict):
        ok = bool(res.get("ok", res.get("allow", res.get("pass", True))))
        sc, why = _normalize_score_reason(res, key="score")
        why = why or str(res.get("why") or "")
        return ok, sc, why
    if isinstance(res, (tuple, list)) and len(res) >= 1:
        ok = bool(res[0])
        sc = None
        why = ""
        if len(res) >= 2 and isinstance(res[1], (int, float)) and not isinstance(res[1], bool):
            sc = float(res[1])
        if len(res) >= 3:
            why = str(res[2])
        return ok, sc, why
    return True, None, ""


def _candidate_kwargs(pool_meta: Dict[str, Any], features: Dict[str, Any]) -> Dict[str, Any]:
    """
    Best-effort mapping for ai_buy_scoring.Candidate(...)
    We keep it minimal but include common names.
    """
    kw: Dict[str, Any] = {}
    kw["mint"] = pool_meta.get("mint")
    kw["symbol"] = pool_meta.get("symbol")
    kw["liq_usd"] = pool_meta.get("liq_usd", features.get("liq_usd", 0.0))
    kw["age_min"] = pool_meta.get("age_min", features.get("age_min", features.get("token_age_min", 0.0)))

    # Momentum-ish
    kw["chg5"] = pool_meta.get("chg_5m_pct", features.get("price_change_5m_pct", features.get("chg_5m_pct", 0.0)))
    kw["buys5"] = pool_meta.get("buys_5m", features.get("buys_5m", features.get("buys5", 0)))
    kw["sells5"] = pool_meta.get("sells_5m", features.get("sells_5m", features.get("sells5", 0)))
    kw["vol5"] = pool_meta.get("vol_5m_usd", features.get("vol_5m_usd", features.get("vol5", 0.0)))
    kw["mcap_usd"] = pool_meta.get("mcap_usd", features.get("mcap_usd", features.get("mcap", 0.0)))

    # If Candidate supports extra kwargs, we can pass more via **features later (we don't here).
    return kw


async def _call_ai_buy_scoring(features: Dict[str, Any], pool_meta: Dict[str, Any]) -> Tuple[Optional[float], bool, str]:
    """
    Returns: (ai_score_0_100_or_None, gate_ok, reason)
    If module present but only gate exists -> score None, gate True/False.
    """
    _, _, _, ABS = _lazy_imports()
    if ABS is None:
        return None, True, "ai_buy_scoring not available"

    # score_only(features) (if exists)
    with contextlib.suppress(Exception):
        score_only = getattr(ABS, "score_only", None)
        if callable(score_only):
            res = await _maybe_await(score_only(features))
            sc, why = _normalize_score_reason(res, key="score")
            if sc is not None:
                return _clamp01_to_100(sc), True, why or "ai_buy_scoring.score_only"

    # pass_gate(Candidate(...))
    with contextlib.suppress(Exception):
        pass_gate = getattr(ABS, "pass_gate", None)
        Candidate = getattr(ABS, "Candidate", None)
        if callable(pass_gate) and Candidate is not None:
            c = Candidate(**_candidate_kwargs(pool_meta, features))  # type: ignore
            res = await _maybe_await(pass_gate(c))
            ok, sc, why = _normalize_pass_gate(res)
            if sc is not None:
                return _clamp01_to_100(sc), bool(ok), why or "ai_buy_scoring.pass_gate"
            return None, bool(ok), why or ("ai_buy_scoring gate OK" if ok else "ai_buy_scoring gate FAIL")

    return None, True, "ai_buy_scoring has no usable API"


async def _call_ai_entry(features: Dict[str, Any], pool_meta: Dict[str, Any], context: Dict[str, Any]) -> Tuple[Optional[float], str]:
    """
    Returns (ai_entry_score_0_100_or_None, reason)
    Preference order:
      1) ai_buy_scoring numeric score (if prefer_abs_ai_score)
      2) ai_entry_logic scoring
      3) feature_builder entry_quality_score scaled (fallback)
    """
    if not CONFIG.enable_ai_entry:
        return None, "AI entry disabled"

    # ai_buy_scoring as preferred numeric score (optional)
    abs_sc, abs_gate_ok, abs_reason = await _call_ai_buy_scoring(features, pool_meta)
    if abs_sc is not None and CONFIG.prefer_abs_ai_score:
        return float(abs_sc), f"ABS: {abs_reason} (gate_ok={abs_gate_ok})"

    _, AEL, _, _ = _lazy_imports()
    if AEL is None:
        # fallback: use feature_builder entry_quality_score if available
        eq = features.get("entry_quality_score")
        if eq is not None:
            return _clamp(float(eq) / 1.5 * 100.0), "fallback: entry_quality_score"
        return None, "ai_entry_logic not available"

    # Try common APIs
    for fname in ("score_candidate", "score_entry", "score_only", "score"):
        fn = getattr(AEL, fname, None)
        if not callable(fn):
            continue

        attempts: List[Tuple[Tuple[Any, ...], Dict[str, Any]]] = [
            ((), {"meta": pool_meta, "features": features, **context}),
            ((), {"meta": pool_meta, "features": features}),
            ((), {"features": features, "meta": pool_meta}),
            ((features,), {"meta": pool_meta}),
            ((features,), {}),
        ]

        for args, kwargs in attempts:
            try:
                res = fn(*args, **kwargs)
                res = await _maybe_await(res)
                sc, why = _normalize_score_reason(res, key="score")
                if sc is None:
                    sc, why = _normalize_score_reason(res, key="entry_score")
                if sc is None:
                    continue
                return _clamp01_to_100(sc), why or f"ai_entry_logic.{fname}"
            except TypeError:
                continue
            except Exception as exc:
                if log.every(f"SCORING_AEL_ERR_{fname}", 600):
                    log.warning("SCORING: ai_entry_logic.%s error: %s", fname, exc)
                continue

    # fallback: feature_builder entry_quality_score if available
    eq = features.get("entry_quality_score")
    if eq is not None:
        return _clamp(float(eq) / 1.5 * 100.0), "fallback: entry_quality_score"
    return None, "ai_entry_logic has no usable scoring function"


async def _call_ai_risk(features: Dict[str, Any], pool_meta: Dict[str, Any], context: Dict[str, Any]) -> Tuple[Optional[float], str]:
    """
    Returns (ai_risk_score_0_100_or_None, reason)
    Preference:
      1) ai_risk_model
      2) feature_builder entry_risk_score scaled
    """
    if not CONFIG.enable_ai_risk:
        return None, "AI risk disabled"

    _, _, ARM, _ = _lazy_imports()
    if ARM is None:
        er = features.get("entry_risk_score")
        if er is not None:
            return _clamp(float(er) * 100.0), "fallback: entry_risk_score"
        return None, "ai_risk_model not available"

    for fname in ("score_risk", "risk_score", "score"):
        fn = getattr(ARM, fname, None)
        if not callable(fn):
            continue

        attempts: List[Tuple[Tuple[Any, ...], Dict[str, Any]]] = [
            ((), {"meta": pool_meta, "features": features, **context}),
            ((), {"meta": pool_meta, "features": features}),
            ((), {"features": features, "meta": pool_meta}),
            ((features,), {"meta": pool_meta}),
            ((features,), {}),
        ]

        for args, kwargs in attempts:
            try:
                res = fn(*args, **kwargs)
                res = await _maybe_await(res)
                sc, why = _normalize_score_reason(res, key="risk_score")
                if sc is None:
                    sc, why = _normalize_score_reason(res, key="score")
                if sc is None:
                    continue
                return _clamp01_to_100(sc), why or f"ai_risk_model.{fname}"
            except TypeError:
                continue
            except Exception as exc:
                if log.every(f"SCORING_ARM_ERR_{fname}", 600):
                    log.warning("SCORING: ai_risk_model.%s error: %s", fname, exc)
                continue

    er = features.get("entry_risk_score")
    if er is not None:
        return _clamp(float(er) * 100.0), "fallback: entry_risk_score"
    return None, "ai_risk_model has no usable scoring function"


# ──────────────────────────────────────────────────────────────────────────────
# Heuristic component scoring (uses feature_builder first, then meta fallbacks)
# ──────────────────────────────────────────────────────────────────────────────
def _compute_momentum_score(features: Dict[str, Any], pool_meta: Dict[str, Any]) -> Tuple[float, str]:
    # Prefer canonical features produced by your FeatureBuilder
    chg_1m = _first_present(features.get("price_change_1m_pct"), features.get("chg_1m_pct"), pool_meta.get("chg_1m_pct"), default=0.0) or 0.0
    chg_5m = _first_present(features.get("price_change_5m_pct"), features.get("chg_5m_pct"), pool_meta.get("chg_5m_pct"), default=0.0) or 0.0

    buys = _first_present(features.get("buys_5m"), pool_meta.get("buys_5m"), default=0.0) or 0.0
    sells = _first_present(features.get("sells_5m"), pool_meta.get("sells_5m"), default=0.0) or 0.0
    vol5 = _first_present(features.get("vol_5m_usd"), pool_meta.get("vol_5m_usd"), default=0.0) or 0.0
    liq = float(pool_meta.get("liq_usd") or 0.0)

    # Pump-aware boost if present
    pump_vel = _first_present(features.get("pump_velocity_score"), default=0.0) or 0.0
    pump_vel_norm = _clip01(_safe_ratio(pump_vel, 10.0, 0.0))  # 0..1

    # Momentum base from change: shape both 1m + 5m
    # Positive 5m matters most; 1m is "urgency"
    pos5 = max(0.0, min(120.0, chg_5m))
    pos1 = max(0.0, min(80.0, chg_1m))
    neg5 = max(0.0, min(120.0, -chg_5m))

    base = 30.0 + (pos5 / 30.0) * 45.0 + (pos1 / 10.0) * 15.0 - (neg5 / 20.0) * 35.0
    base = _clamp(base)

    # Orderflow imbalance
    total = buys + sells
    buy_ratio = (buys / total) if total > 0 else 0.5
    imbalance = (buy_ratio - 0.5) * 70.0  # -35..+35

    # Activity intensity vs liquidity (cap)
    v_rel = 0.0
    if liq > 0 and vol5 > 0:
        v_rel = min(3.0, vol5 / max(1.0, liq))  # 0..3
    intensity = v_rel * 14.0  # 0..42

    score = _clamp(base + imbalance + intensity + 10.0 * pump_vel_norm)
    why = f"chg1m={chg_1m:.2f}% chg5m={chg_5m:.2f}%; br={buy_ratio:.2f}; vol5/liq={v_rel:.2f}; pumpVelN={pump_vel_norm:.2f}"
    return score, why


def _compute_quality_score(features: Dict[str, Any], pool_meta: Dict[str, Any]) -> Tuple[float, str]:
    # If FeatureBuilder already computed entry_quality_score (0..1.5), prefer it
    eq = features.get("entry_quality_score")
    if isinstance(eq, (int, float)) and not isinstance(eq, bool):
        sc = _clamp(float(eq) / 1.5 * 100.0)
        return sc, f"from entry_quality_score={float(eq):.3f}"

    liq = float(pool_meta.get("liq_usd") or 0.0)
    age = float(pool_meta.get("age_min") or 0.0)
    mcap = float(pool_meta.get("mcap_usd") or 0.0)
    holders = _first_present(features.get("holders"), features.get("holders_total"), pool_meta.get("holders"), default=None)

    score = 45.0
    reasons: List[str] = []

    # Liquidity (log shaped)
    if liq <= 0:
        score -= 10.0
        reasons.append("liq=0")
    else:
        ref = max(50.0, float(CONFIG.min_liq_usd))
        x = max(0.0, math.log10(max(1.0, liq / ref)))  # 0 at ref
        liq_score = 60.0 + min(32.0, x * 12.0)
        if liq < ref:
            liq_score = 40.0 + (liq / ref) * 20.0
        score = max(score, liq_score)
        reasons.append(f"liq=${liq:,.0f}→{liq_score:.0f}")

    # Age: slightly prefer “not 30 seconds old”
    if age < 5:
        score -= 8.0
        reasons.append("age<5m")
    elif age < 30:
        score -= 2.0
        reasons.append("age<30m")
    elif age < 180:
        score += 4.0
        reasons.append("age good")
    else:
        reasons.append("age ok")

    # Market cap sanity
    if mcap > 0:
        if mcap < 25_000:
            score -= 4.0
            reasons.append("mcap tiny")
        elif mcap > 250_000:
            score += 2.0
            reasons.append("mcap ok")

    # Holders
    if holders is not None:
        h = float(holders)
        if h < 30:
            score -= 6.0
            reasons.append(f"holders={int(h)} low")
        elif h < 200:
            reasons.append(f"holders={int(h)} mid")
        else:
            score += 4.0
            reasons.append(f"holders={int(h)} good")

    return _clamp(score), "; ".join(reasons)


def _compute_risk_score(features: Dict[str, Any], pool_meta: Dict[str, Any], ai_risk_score: Optional[float], context: Dict[str, Any]) -> Tuple[float, str]:
    # Prefer FeatureBuilder entry_risk_score (0..1) if AI missing
    entry_risk = features.get("entry_risk_score")
    fb_risk_100 = None
    if isinstance(entry_risk, (int, float)) and not isinstance(entry_risk, bool):
        fb_risk_100 = _clamp(float(entry_risk) * 100.0)

    base = float(ai_risk_score) if ai_risk_score is not None else (fb_risk_100 if fb_risk_100 is not None else 50.0)
    reasons: List[str] = []
    if ai_risk_score is not None:
        reasons.append(f"AI={ai_risk_score:.1f}")
    elif fb_risk_100 is not None:
        reasons.append(f"FB={fb_risk_100:.1f}")

    liq = float(pool_meta.get("liq_usd") or 0.0)
    age = float(pool_meta.get("age_min") or 0.0)
    impact = float(pool_meta.get("impact_1k_bps") or 0.0)
    ai_rug = float(pool_meta.get("ai_rug_score") or 0.0)

    # Rug score (0..1 or 0..100)
    if ai_rug > 0:
        rug100 = ai_rug * 100.0 if 0.0 <= ai_rug <= 1.0 else ai_rug
        base = max(base, min(95.0, rug100))
        reasons.append(f"rug≈{rug100:.0f}")

    # Low liquidity raises risk
    ref = max(50.0, float(CONFIG.min_liq_usd))
    if liq < ref:
        bump = min(28.0, (ref - liq) / ref * 45.0)
        base += bump
        reasons.append(f"low_liq(+{bump:.0f})")

    # Very young pools are rug-prone
    if age < 3:
        base += 14.0
        reasons.append("age<3m(+14)")
    elif age < 15:
        base += 8.0
        reasons.append("age<15m(+8)")
    elif age < 60:
        base += 3.0
        reasons.append("age<60m(+3)")
    elif age > float(CONFIG.max_age_min or 720.0):
        base -= 4.0
        reasons.append("old(-4)")

    # Price impact as manip/slippage proxy (higher => riskier)
    if impact > 0:
        # interpret bps directly; beyond ~1200 bps (12%) gets ugly
        bump = min(20.0, max(0.0, (impact - 1200.0) / 1200.0) * 30.0)
        base += bump
        if bump > 0:
            reasons.append(f"impact{impact:.0f}bps(+{bump:.0f})")

    # Explicit honeypot/rug flags
    flags = [
        features.get("is_honeypot"),
        features.get("honeypot_flag"),
        features.get("rugpull_flag"),
        features.get("can_sell") is False,
        pool_meta.get("is_honeypot"),
    ]
    if any(bool(x) for x in flags):
        base = max(base, 92.0)
        reasons.append("honeypot_flag(>=92)")

    # Authority flags (if present)
    if features.get("mint_authority_disabled") is False or pool_meta.get("mint_authority_disabled") is False:
        base += 10.0
        reasons.append("mint_auth_on(+10)")
    if features.get("freeze_authority_disabled") is False or pool_meta.get("freeze_authority_disabled") is False:
        base += 10.0
        reasons.append("freeze_auth_on(+10)")

    # Concentration flags if present
    top1 = _first_present(features.get("top_holder_pct"), features.get("top1_holder_pct"), pool_meta.get("top1_hold_pct"), default=None)
    top10 = _first_present(features.get("top10_holder_pct"), pool_meta.get("top10_hold_pct"), default=None)
    if top1 is not None and float(top1) > 40.0:
        base += 14.0
        reasons.append("top1>40(+14)")
    if top10 is not None and float(top10) > 80.0:
        base += 10.0
        reasons.append("top10>80(+10)")

    # Regime penalty (context-aware; supports dict or RegimeState-like)
    regime = context.get("regime")
    ln = 0.0
    manip = 0.0
    if isinstance(regime, dict):
        with contextlib.suppress(Exception):
            ln = float(regime.get("label_numeric", regime.get("labelNumeric", 0.0)) or 0.0)
        with contextlib.suppress(Exception):
            manip = float(regime.get("manipulation_risk", regime.get("manipulationRisk", 0.0)) or 0.0)
    else:
        # RegimeState-like
        with contextlib.suppress(Exception):
            if hasattr(regime, "as_numeric"):
                ln = float(regime.as_numeric())
        with contextlib.suppress(Exception):
            if hasattr(regime, "manipulation_risk"):
                manip = float(getattr(regime, "manipulation_risk"))

    if ln <= -0.7:
        base += 8.0
        reasons.append("regime_risk_off(+8)")
    if manip >= 0.8:
        base += 14.0
        reasons.append("manip_high(+14)")
    elif manip >= 0.6:
        base += 7.0
        reasons.append("manip_mid(+7)")

    return _clamp(base), "; ".join(reasons)


# ──────────────────────────────────────────────────────────────────────────────
# Combine + label + trade gate
# ──────────────────────────────────────────────────────────────────────────────
def _combine_scores(momentum: float, risk: float, quality: float, ai_entry: Optional[float], context: Dict[str, Any]) -> float:
    # If AI entry missing, approximate from momentum+quality (stable fallback)
    ai_sc = float(ai_entry) if ai_entry is not None else (float(momentum) + float(quality)) / 2.0
    risk_contrib = 100.0 - _clamp(risk)

    combined = (
        CONFIG.w_ai * _clamp(ai_sc)
        + CONFIG.w_momentum * _clamp(momentum)
        + CONFIG.w_quality * _clamp(quality)
        + CONFIG.w_risk * _clamp(risk_contrib)
    )

    # Optional separate regime penalties to combined score
    regime = context.get("regime")
    ln = 0.0
    manip = 0.0
    if isinstance(regime, dict):
        with contextlib.suppress(Exception):
            ln = float(regime.get("label_numeric", regime.get("labelNumeric", 0.0)) or 0.0)
        with contextlib.suppress(Exception):
            manip = float(regime.get("manipulation_risk", regime.get("manipulationRisk", 0.0)) or 0.0)
    else:
        with contextlib.suppress(Exception):
            if hasattr(regime, "as_numeric"):
                ln = float(regime.as_numeric())
        with contextlib.suppress(Exception):
            if hasattr(regime, "manipulation_risk"):
                manip = float(getattr(regime, "manipulation_risk"))

    penalty = 0.0
    if ln <= -0.7:
        penalty += float(CONFIG.regime_risk_penalty)
    if manip >= 0.8:
        penalty += float(CONFIG.manip_risk_penalty)
    elif manip >= 0.6:
        penalty += float(CONFIG.manip_risk_penalty) * 0.5

    if penalty > 0:
        combined = max(0.0, combined - penalty)

    return _clamp(combined)


def _derive_label(combined: float, momentum: float, risk: float, quality: float) -> str:
    if (
        combined >= CONFIG.high_score_threshold
        and momentum >= CONFIG.min_momentum_for_green
        and risk <= CONFIG.max_risk_for_green
        and quality >= CONFIG.min_quality_for_green
    ):
        return "GREEN"
    if combined >= CONFIG.medium_score_threshold and risk <= 90.0:
        return "YELLOW"
    return "RED"


async def _derive_trade_gate(pool_meta: Dict[str, Any], features: Dict[str, Any], combined: float, label: str, risk: float) -> Tuple[bool, str]:
    liq = float(pool_meta.get("liq_usd") or 0.0)
    age = float(pool_meta.get("age_min") or 0.0)

    # Structural constraints
    if liq < float(CONFIG.min_liq_usd):
        return False, f"liq ${liq:.0f} < min ${CONFIG.min_liq_usd:.0f}"
    if age > float(CONFIG.max_age_min):
        return False, f"age {age:.1f}m > max {CONFIG.max_age_min:.0f}m"

    # FeatureBuilder hard-block (if present)
    entry_block_flag = float(features.get("entry_block_flag", 0.0) or 0.0)
    if entry_block_flag >= 1.0:
        return False, "feature_builder entry_block_flag"

    if label == "RED":
        return False, "RED label"
    if combined < float(CONFIG.medium_score_threshold):
        return False, f"combined {combined:.1f} < {CONFIG.medium_score_threshold:.1f}"

    # Optional ABS gate enforcement
    if CONFIG.enable_abs_gate:
        abs_sc, abs_ok, abs_reason = await _call_ai_buy_scoring(features, pool_meta)
        if not abs_ok:
            return False, abs_reason or "ai_buy_scoring gate blocked"

    # Hard-ish risk guard
    if risk >= 92.0:
        return False, f"risk {risk:.0f} too high"

    # Optional “soft gate” flags from FeatureBuilder (if available)
    # If they exist and say "not ok", respect them in a conservative way.
    # (Keeps wiring consistent with your gate_* feature plan.)
    soft_all = features.get("gate_all_soft_ok")
    if isinstance(soft_all, (int, float)) and not isinstance(soft_all, bool):
        if float(soft_all) <= 0.0 and label == "YELLOW":
            return False, "feature_builder soft-gate"

    return True, "OK"


def _format_reasons(
    label: str,
    combined: float,
    ai_entry: Optional[float],
    ai_entry_reason: str,
    momentum: float,
    momentum_reason: str,
    risk: float,
    risk_reason: str,
    quality: float,
    quality_reason: str,
    size_hint: float,
    gate_ok: bool,
    gate_reason: str,
) -> str:
    parts: List[str] = []
    parts.append(f"Label={label} combined={combined:.1f}/100 gate={gate_ok}({gate_reason}) size≈{size_hint:.2f}")
    if ai_entry is None:
        parts.append(f"AI=N/A ({ai_entry_reason})")
    else:
        parts.append(f"AI={ai_entry:.1f}/100 ({ai_entry_reason})")
    parts.append(f"MOMO={momentum:.1f}/100 ({momentum_reason})")
    parts.append(f"RISK={risk:.1f}/100 (higher=worse; {risk_reason})")
    parts.append(f"QUAL={quality:.1f}/100 ({quality_reason})")
    return " | ".join(parts)


# ──────────────────────────────────────────────────────────────────────────────
# Public API
# ──────────────────────────────────────────────────────────────────────────────
async def score_candidate(raw_pool: Dict[str, Any], context: Optional[Dict[str, Any]] = None) -> ScoredToken:
    """
    Main scoring entrypoint.
    raw_pool: dict from scanner (Raydium / Pumpfun / Dexscreener / etc.)
    context: optional info (regime/risk_context/history/wallet_signals/ai_signals/scanner_source)
    """
    if context is None:
        context = {}

    pool_meta = _normalize_pool_meta(raw_pool)
    mint = pool_meta["mint"] or _normalize_mint(raw_pool.get("mint") or raw_pool.get("base_mint") or "")
    pool_meta["mint"] = mint

    # Cache (mint must exist)
    if mint:
        cached = _cache_get(mint)
        if cached is not None:
            return cached

    # Build features (canonical FeatureBuilder)
    features = await _build_features(raw_pool, pool_meta, context)

    # AI entry + AI risk (async-safe)
    ai_entry_score, ai_entry_reason = await _call_ai_entry(features, pool_meta, context)
    ai_risk_score, ai_risk_reason = await _call_ai_risk(features, pool_meta, context)

    # Heuristic components (feature-aware)
    momentum_score, momentum_reason = _compute_momentum_score(features, pool_meta)
    quality_score, quality_reason = _compute_quality_score(features, pool_meta)
    risk_score, risk_reason_h = _compute_risk_score(features, pool_meta, ai_risk_score, context)

    risk_reason = "; ".join([x for x in [ai_risk_reason, risk_reason_h] if x])

    # Combined + label
    combined_score = _combine_scores(momentum_score, risk_score, quality_score, ai_entry_score, context)
    label = _derive_label(combined_score, momentum_score, risk_score, quality_score)

    # Trade gate
    is_trade_viable, gate_reason = await _derive_trade_gate(pool_meta, features, combined_score, label, risk_score)

    # Sizing hint + block flag from features (if present)
    size_hint = 0.0
    with contextlib.suppress(Exception):
        size_hint = float(features.get("position_size_scalar", 0.0) or 0.0)
    size_hint = _clamp(size_hint, 0.0, 1.0)
    entry_block_flag = _clamp(features.get("entry_block_flag", 0.0) or 0.0, 0.0, 1.0)

    reasons = _format_reasons(
        label=label,
        combined=combined_score,
        ai_entry=ai_entry_score,
        ai_entry_reason=ai_entry_reason,
        momentum=momentum_score,
        momentum_reason=momentum_reason,
        risk=risk_score,
        risk_reason=risk_reason,
        quality=quality_score,
        quality_reason=quality_reason,
        size_hint=size_hint,
        gate_ok=bool(is_trade_viable),
        gate_reason=str(gate_reason or ""),
    )

    st = ScoredToken(
        mint=str(pool_meta["mint"] or ""),
        symbol=str(pool_meta["symbol"] or "UNKNOWN"),
        chain=str(pool_meta["chain"] or "SOL"),
        liq_usd=float(pool_meta.get("liq_usd") or 0.0),
        age_min=float(pool_meta.get("age_min") or 0.0),
        risk_score=_clamp(risk_score),
        momentum_score=_clamp(momentum_score),
        quality_score=_clamp(quality_score),
        ai_score=_clamp(ai_entry_score) if ai_entry_score is not None else 0.0,
        combined_score=_clamp(combined_score),
        label=str(label).upper().strip(),
        reasons=reasons,
        is_trade_viable=bool(is_trade_viable),
        trade_gate_reason=str(gate_reason or ""),
        size_hint=float(size_hint),
        entry_block_flag=float(entry_block_flag),
        raw_features=features if isinstance(features, dict) else {},
        created_at_ts=time.time(),
    )

    if mint:
        _cache_put(mint, st)

    if log.every("SCORING_SAMPLE", 60):
        log.info(
            "SCORING: %s (%s) label=%s combined=%.1f liq=$%.0f age=%.1fmin trade_ok=%s size=%.2f",
            st.symbol,
            st.mint,
            st.label,
            st.combined_score,
            st.liq_usd,
            st.age_min,
            st.is_trade_viable,
            st.size_hint,
        )

    return st


async def score_many(raw_pools: Iterable[Dict[str, Any]], context: Optional[Dict[str, Any]] = None) -> List[ScoredToken]:
    """
    Convenience helper: score many sequentially (predictable, low overhead).
    """
    if context is None:
        context = {}
    out: List[ScoredToken] = []
    for raw in raw_pools:
        try:
            out.append(await score_candidate(raw, context=context))
        except Exception as exc:  # pragma: no cover
            if log.every("SCORING_MANY_ERR", 60):
                log.warning("SCORING: error scoring pool mint=%r: %s", (raw or {}).get("mint"), exc)
    return out


async def score_many_parallel(raw_pools: Iterable[Dict[str, Any]], context: Optional[Dict[str, Any]] = None, *, max_concurrency: Optional[int] = None) -> List[ScoredToken]:
    """
    Parallel scoring helper (bounded). Use this for radar / wide scans.
    """
    if context is None:
        context = {}

    pools = list(raw_pools)
    if not pools:
        return []

    mc = int(max_concurrency or CONFIG.max_concurrency or 12)
    mc = max(1, mc)
    sem = asyncio.Semaphore(mc)

    out: List[Optional[ScoredToken]] = [None] * len(pools)

    async def _one(i: int, p: Dict[str, Any]) -> None:
        async with sem:
            try:
                out[i] = await score_candidate(p, context=context)
            except Exception as exc:  # pragma: no cover
                if log.every("SCORING_PAR_ERR", 60):
                    log.warning("SCORING: parallel error mint=%r: %s", (p or {}).get("mint"), exc)

    await asyncio.gather(*[_one(i, p) for i, p in enumerate(pools)])
    return [x for x in out if x is not None]


__all__ = [
    "ScoreConfig",
    "ScoredToken",
    "CONFIG",
    "reload_config",
    "score_candidate",
    "score_many",
    "score_many_parallel",
]
