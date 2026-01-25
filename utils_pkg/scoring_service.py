# utils_pkg/scoring_service.py
from __future__ import annotations

"""
Scoring Service — God-Tier V3+++ / quant-grade (Phase 0–7)
=========================================================

Single unified scoring + trade-gate entrypoint for *all* scanners/traders:

    from utils_pkg.scoring_service import ScoredToken, score_candidate, score_many, score_many_parallel

Primary goals
-------------
- One stable API across Raydium / Pump.fun / Dexscreener / radar feeds.
- Defensive: never hard-fails when optional modules are missing or signatures differ.
- Quant-ish: component scores 0..100 + combined score 0..100 + label + gate.
- Actionable: reasons + gate result + sizing hint (if present in features).
- Fast: optional TTL + bounded in-memory cache, plus in-flight de-dupe.
- Predictable: latency caps (timeouts) per sub-step, fail-open where safe.

Soft integrations (best-effort, lazy imports)
---------------------------------------------
- utils_pkg.feature_builder   (canonical feature dict generator; NO DB calls expected)
- utils_pkg.ai_entry_logic    (AI/heuristic entry scoring)
- utils_pkg.ai_risk_model     (AI risk scoring)
- utils_pkg.ai_buy_scoring    (hard gate + optional score)
- utils_pkg.regime            (optional regime context in `context["regime"]`)

Contract
--------
- score_candidate(raw_pool, context=None) -> ScoredToken
- raw_pool can be dict-like, or an object with `to_dict()` / `dict()` / `__dict__`.

Context supported keys (all optional):
    {
      "regime": RegimeState | dict,
      "risk_context": dict,
      "wallet_signals": dict,
      "ai_signals": dict,
      "history": list[dict],
      "scanner_source": "raydium"|"dexscreener"|"pumpfun"|"momo"|...,
      "cache_key": str  # explicit cache key override
    }

Hard rule: This module itself performs NO network and NO DB calls.
(It may call other modules that you configure; keep those pure/fast.)
"""

import asyncio
import contextlib
import dataclasses
import inspect
import json
import math
import os
import time
from collections import OrderedDict
from collections.abc import Iterable, Mapping
from dataclasses import asdict, dataclass
from typing import Any

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
    _EVERY_STATE: dict[str, int] = {}

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

    log.every = _every


# ──────────────────────────────────────────────────────────────────────────────
# Tiny helpers
# ──────────────────────────────────────────────────────────────────────────────
def _now_mono() -> float:
    return time.monotonic()


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


def _first_present(*vals: Any, default: float | None = None) -> float | None:
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


def _score01_to_100(sc: float | None) -> float | None:
    if sc is None:
        return None
    s = float(sc)
    if 0.0 <= s <= 1.0:
        s *= 100.0
    return _clamp(s, 0.0, 100.0)


def _age_min_from_ts(ts: Any) -> float | None:
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
    with contextlib.suppress(Exception):
        from utils_pkg.mints import normalize_mint  # type: ignore

        return str(normalize_mint(s)).strip()
    with contextlib.suppress(Exception):
        from utils_pkg.mint_guard import normalize_from_any  # type: ignore

        return str(normalize_from_any(s)).strip()
    return s


def _as_dict(raw: Any) -> dict[str, Any]:
    if raw is None:
        return {}
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, Mapping):
        return dict(raw)
    with contextlib.suppress(Exception):
        if hasattr(raw, "to_dict"):
            d = raw.to_dict()
            if isinstance(d, dict):
                return d
    with contextlib.suppress(Exception):
        if hasattr(raw, "dict"):
            d = raw.dict()  # pydantic-style
            if isinstance(d, dict):
                return d
    with contextlib.suppress(Exception):
        d = dict(raw)  # iterable of pairs
        if isinstance(d, dict):
            return d
    with contextlib.suppress(Exception):
        d = getattr(raw, "__dict__", None)
        if isinstance(d, dict):
            return dict(d)
    return {}


async def _maybe_await(x: Any) -> Any:
    return await x if inspect.isawaitable(x) else x


async def _await_with_timeout(x: Any, timeout_s: float) -> Any:
    if timeout_s <= 0:
        return await _maybe_await(x)
    if inspect.isawaitable(x):
        return await asyncio.wait_for(x, timeout=timeout_s)
    return x


# ──────────────────────────────────────────────────────────────────────────────
# Config
# ──────────────────────────────────────────────────────────────────────────────
def _load_source_overrides() -> dict[str, dict[str, Any]]:
    raw = os.getenv("SCORING_SOURCE_OVERRIDES_JSON", "").strip()
    if not raw:
        return {}
    try:
        data = json.loads(raw)
        if isinstance(data, dict):
            out: dict[str, dict[str, Any]] = {}
            for k, v in data.items():
                if isinstance(k, str) and isinstance(v, dict):
                    out[k.strip().lower()] = v
            return out
    except Exception:
        if log.every("SCORING_BAD_SOURCE_OVERRIDES", 600):
            log.warning("SCORING: SCORING_SOURCE_OVERRIDES_JSON is not valid JSON")
    return {}


@dataclass(frozen=True, slots=True)
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

    # Combined score weights (normalized)
    w_ai: float = 0.35
    w_momentum: float = 0.35
    w_quality: float = 0.20
    w_risk: float = 0.10  # applied to (100 - risk)

    # Optional tightening/loosening penalties (applied to combined)
    regime_risk_penalty: float = 12.0
    manip_risk_penalty: float = 18.0

    # Cache controls
    cache_ttl_s: float = 0.0  # 0 disables TTL caching
    cache_max_items: int = 2048
    cache_key_mode: str = "mint"  # mint | mint_source | mint_source_regime | explicit_only

    # Feature toggles
    enable_ai_entry: bool = True
    enable_ai_risk: bool = True
    enable_abs_gate: bool = True
    prefer_abs_ai_score: bool = False  # use ai_buy_scoring numeric score as ai_entry if present

    # Parallel scoring helper default
    max_concurrency: int = 12

    # Latency caps (seconds). 0 disables timeout for that step.
    timeout_total_s: float = 0.20
    timeout_feature_build_s: float = 0.08
    timeout_abs_s: float = 0.04
    timeout_ai_entry_s: float = 0.05
    timeout_ai_risk_s: float = 0.05

    # Behavior
    fail_open: bool = True  # when a sub-module errors/timeouts, continue with fallbacks

    # Source overrides (optional)
    source_overrides: dict[str, dict[str, Any]] = dataclasses.field(default_factory=dict)

    @classmethod
    def from_env(cls) -> ScoreConfig:
        base = cls(
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
            cache_key_mode=str(os.getenv("SCORING_CACHE_KEY_MODE", "mint")).strip() or "mint",
            enable_ai_entry=_b_env("SCORING_ENABLE_AI_ENTRY", True),
            enable_ai_risk=_b_env("SCORING_ENABLE_AI_RISK", True),
            enable_abs_gate=_b_env("SCORING_ENABLE_ABS_GATE", True),
            prefer_abs_ai_score=_b_env("SCORING_PREFER_ABS_AI_SCORE", False),
            max_concurrency=_i_env("SCORING_MAX_CONCURRENCY", 12),
            timeout_total_s=_f_env("SCORING_TIMEOUT_TOTAL_S", 0.20),
            timeout_feature_build_s=_f_env("SCORING_TIMEOUT_FEATURE_BUILD_S", 0.08),
            timeout_abs_s=_f_env("SCORING_TIMEOUT_ABS_S", 0.04),
            timeout_ai_entry_s=_f_env("SCORING_TIMEOUT_AI_ENTRY_S", 0.05),
            timeout_ai_risk_s=_f_env("SCORING_TIMEOUT_AI_RISK_S", 0.05),
            fail_open=_b_env("SCORING_FAIL_OPEN", True),
            source_overrides=_load_source_overrides(),
        )

        # Normalize weights safely
        total = float(base.w_ai + base.w_momentum + base.w_quality + base.w_risk)
        if total <= 0:
            if log.every("SCORING_BAD_WEIGHTS", 600):
                log.warning("SCORING: weights sum <= 0; resetting to defaults")
            base = cls(source_overrides=base.source_overrides)

        cache_max_items = max(128, int(base.cache_max_items))
        max_conc = max(1, int(base.max_concurrency))
        cache_key_mode = base.cache_key_mode.lower().strip()
        if cache_key_mode not in {"mint", "mint_source", "mint_source_regime", "explicit_only"}:
            cache_key_mode = "mint"

        w_ai = float(base.w_ai) / total if total > 0 else cls().w_ai
        w_m = float(base.w_momentum) / total if total > 0 else cls().w_momentum
        w_q = float(base.w_quality) / total if total > 0 else cls().w_quality
        w_r = float(base.w_risk) / total if total > 0 else cls().w_risk

        return dataclasses.replace(
            base,
            w_ai=w_ai,
            w_momentum=w_m,
            w_quality=w_q,
            w_risk=w_r,
            cache_max_items=cache_max_items,
            max_concurrency=max_conc,
            cache_key_mode=cache_key_mode,
        )

    def for_source(self, source: str) -> ScoreConfig:
        s = (source or "").strip().lower()
        if not s:
            return self
        ov = (self.source_overrides or {}).get(s)
        if not isinstance(ov, dict) or not ov:
            return self

        allowed = {
            "min_liq_usd",
            "max_age_min",
            "high_score_threshold",
            "medium_score_threshold",
            "min_momentum_for_green",
            "max_risk_for_green",
            "min_quality_for_green",
            "w_ai",
            "w_momentum",
            "w_quality",
            "w_risk",
            "regime_risk_penalty",
            "manip_risk_penalty",
            "enable_ai_entry",
            "enable_ai_risk",
            "enable_abs_gate",
            "prefer_abs_ai_score",
            "timeout_total_s",
            "timeout_feature_build_s",
            "timeout_abs_s",
            "timeout_ai_entry_s",
            "timeout_ai_risk_s",
            "fail_open",
        }

        patch: dict[str, Any] = {k: v for k, v in ov.items() if k in allowed}
        if not patch:
            return self

        derived = dataclasses.replace(self, **patch)
        total = float(derived.w_ai + derived.w_momentum + derived.w_quality + derived.w_risk)
        if total > 0:
            derived = dataclasses.replace(
                derived,
                w_ai=float(derived.w_ai) / total,
                w_momentum=float(derived.w_momentum) / total,
                w_quality=float(derived.w_quality) / total,
                w_risk=float(derived.w_risk) / total,
            )
        return derived


CONFIG: ScoreConfig = ScoreConfig.from_env()


def reload_config() -> ScoreConfig:
    global CONFIG
    CONFIG = ScoreConfig.from_env()
    return CONFIG


# ──────────────────────────────────────────────────────────────────────────────
# Output model
# ──────────────────────────────────────────────────────────────────────────────
@dataclass(slots=True)
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
    ai_score: float  # (entry-like)

    combined_score: float
    label: str  # GREEN/YELLOW/RED

    reasons: str

    # Useful extras for the rest of the stack
    is_trade_viable: bool
    trade_gate_reason: str
    size_hint: float  # 0..1 (if features provide it), else 0
    entry_block_flag: float  # 0/1 (if features provide it), else 0

    raw_features: dict[str, Any]  # feature_builder output (best-effort)
    created_at_ts: float

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    def compact(self) -> dict[str, Any]:
        return {
            "sym": self.symbol,
            "mint": self.mint,
            "label": self.label,
            "combined": round(float(self.combined_score), 2),
            "liq": round(float(self.liq_usd), 2),
            "age_min": round(float(self.age_min), 2),
            "ok": bool(self.is_trade_viable),
            "gate": self.trade_gate_reason,
            "size": round(float(self.size_hint), 3),
        }


# ──────────────────────────────────────────────────────────────────────────────
# In-memory TTL cache + in-flight de-dupe
# ──────────────────────────────────────────────────────────────────────────────
_SCORE_CACHE: OrderedDict[str, tuple[float, ScoredToken]] = OrderedDict()
_CACHE_LOCK = asyncio.Lock()
_INFLIGHT: dict[str, asyncio.Future[ScoredToken]] = {}


def cache_clear() -> None:
    _SCORE_CACHE.clear()


def _cache_enabled(cfg: ScoreConfig) -> bool:
    return float(cfg.cache_ttl_s or 0.0) > 0.0


def _regime_cache_key(regime: Any) -> str:
    if regime is None:
        return ""
    if isinstance(regime, dict):
        ln = regime.get("label_numeric", regime.get("labelNumeric", ""))
        mr = regime.get("manipulation_risk", regime.get("manipulationRisk", ""))
        return f"ln={ln}|mr={mr}"
    with contextlib.suppress(Exception):
        ln = (
            regime.as_numeric()
            if hasattr(regime, "as_numeric")
            else getattr(regime, "label_numeric", "")
        )
    with contextlib.suppress(Exception):
        mr = getattr(regime, "manipulation_risk", "")
    return f"ln={ln}|mr={mr}"


def _cache_key(mint: str, context: dict[str, Any], cfg: ScoreConfig) -> str:
    explicit = str(context.get("cache_key") or "").strip()
    if explicit:
        return explicit

    mode = (cfg.cache_key_mode or "mint").strip().lower()
    if mode == "explicit_only":
        return ""

    source = str(context.get("scanner_source") or "").strip().lower()
    if mode == "mint_source":
        return f"{mint}:{source}"
    if mode == "mint_source_regime":
        reg = _regime_cache_key(context.get("regime"))
        return f"{mint}:{source}:{reg}"
    return mint


async def _cache_get(key: str, cfg: ScoreConfig) -> ScoredToken | None:
    if not key or not _cache_enabled(cfg):
        return None
    ttl = float(cfg.cache_ttl_s or 0.0)
    async with _CACHE_LOCK:
        item = _SCORE_CACHE.get(key)
        if not item:
            return None
        ts_mono, val = item
        if (_now_mono() - ts_mono) <= ttl:
            _SCORE_CACHE.move_to_end(key, last=True)
            return val
        _SCORE_CACHE.pop(key, None)
    return None


async def _cache_put(key: str, st: ScoredToken, cfg: ScoreConfig) -> None:
    if not key or not _cache_enabled(cfg):
        return
    async with _CACHE_LOCK:
        _SCORE_CACHE[key] = (_now_mono(), st)
        _SCORE_CACHE.move_to_end(key, last=True)
        max_items = int(cfg.cache_max_items or 2048)
        while len(_SCORE_CACHE) > max_items:
            _SCORE_CACHE.popitem(last=False)


# ──────────────────────────────────────────────────────────────────────────────
# Pool meta normalization (broad, robust)
# ──────────────────────────────────────────────────────────────────────────────
def _normalize_pool_meta(raw_pool: Mapping[str, Any]) -> dict[str, Any]:
    rp = raw_pool

    mint = (
        rp.get("mint")
        or rp.get("base_mint")
        or rp.get("baseMint")
        or rp.get("mint_address")
        or rp.get("base_token_mint")
        or rp.get("token_mint")
        or ""
    )
    symbol = (
        rp.get("symbol")
        or rp.get("sym")
        or rp.get("base_symbol")
        or rp.get("baseSymbol")
        or rp.get("ticker")
        or "UNKNOWN"
    )
    chain = rp.get("chain") or rp.get("chainId") or rp.get("network") or "SOL"

    mint_s = _normalize_mint(mint)
    sym_s = str(symbol or "UNKNOWN").strip() or "UNKNOWN"
    chain_s = str(chain or "SOL").strip() or "SOL"

    liq_usd = (
        _first_present(
            rp.get("liq_usd"),
            rp.get("liquidity_usd"),
            (rp.get("liquidity") or {}).get("usd")
            if isinstance(rp.get("liquidity"), dict)
            else None,
            rp.get("liquidity"),
            rp.get("liq"),
            default=0.0,
        )
        or 0.0
    )

    age_min = _first_present(
        rp.get("age_min"),
        rp.get("token_age_min"),
        rp.get("ageMinutes"),
        rp.get("age_minute"),
        rp.get("pool_age_min"),
        default=None,
    )
    if age_min is None or age_min <= 0:
        age_min = (
            _age_min_from_ts(
                rp.get("pairCreatedAt")
                or rp.get("pairCreatedAtMs")
                or rp.get("pairCreatedAtTs")
                or rp.get("created_at")
                or rp.get("createdAt")
                or rp.get("ts_created")
            )
            or 0.0
        )

    mcap = (
        _first_present(
            rp.get("mcap_usd"),
            rp.get("mcap"),
            rp.get("marketCap"),
            rp.get("marketCapUsd"),
            rp.get("fdv"),
            rp.get("fdv_usd"),
            default=0.0,
        )
        or 0.0
    )

    vol5 = (
        _first_present(
            rp.get("vol_5m_usd"),
            rp.get("vol5"),
            (rp.get("volume") or {}).get("m5") if isinstance(rp.get("volume"), dict) else None,
            (rp.get("volumeUsd") or {}).get("m5")
            if isinstance(rp.get("volumeUsd"), dict)
            else None,
            rp.get("volume5m"),
            default=0.0,
        )
        or 0.0
    )

    buys5 = int(
        _first_present(
            rp.get("buys_5m"),
            rp.get("buys5"),
            (rp.get("txns") or {}).get("m5", {}).get("buys")
            if isinstance(rp.get("txns"), dict)
            else None,
            default=0.0,
        )
        or 0.0
    )
    sells5 = int(
        _first_present(
            rp.get("sells_5m"),
            rp.get("sells5"),
            (rp.get("txns") or {}).get("m5", {}).get("sells")
            if isinstance(rp.get("txns"), dict)
            else None,
            default=0.0,
        )
        or 0.0
    )

    px = (
        _first_present(
            rp.get("price_usd"), rp.get("px_usd"), rp.get("priceUsd"), rp.get("price"), default=0.0
        )
        or 0.0
    )
    chg1 = (
        _first_present(
            rp.get("price_change_1m_pct"),
            rp.get("chg1"),
            rp.get("chg_1m_pct"),
            (rp.get("priceChange") or {}).get("m1")
            if isinstance(rp.get("priceChange"), dict)
            else None,
            default=0.0,
        )
        or 0.0
    )
    chg5 = (
        _first_present(
            rp.get("price_change_5m_pct"),
            rp.get("chg5"),
            rp.get("chg_5m_pct"),
            (rp.get("priceChange") or {}).get("m5")
            if isinstance(rp.get("priceChange"), dict)
            else None,
            default=0.0,
        )
        or 0.0
    )

    impact_bps = (
        _first_present(
            rp.get("impact_1k_bps"),
            rp.get("impact_1000usd_bps"),
            rp.get("priceImpact1kBps"),
            default=0.0,
        )
        or 0.0
    )
    ai_rug = (
        _first_present(
            rp.get("ai_rug_score_01"), rp.get("ai_rug_score"), rp.get("ai_fraud_score"), default=0.0
        )
        or 0.0
    )

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
def _lazy_imports() -> tuple[Any, Any, Any, Any]:
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


# ──────────────────────────────────────────────────────────────────────────────
# Feature builder adapter (canonical)
# ──────────────────────────────────────────────────────────────────────────────
async def _build_features(
    raw_pool: Mapping[str, Any],
    pool_meta: dict[str, Any],
    context: dict[str, Any],
    cfg: ScoreConfig,
) -> dict[str, Any]:
    FB, _, _, _ = _lazy_imports()
    if FB is None:
        if log.every("SCORING_NO_FB", 600):
            log.warning("SCORING: feature_builder not available; using empty features.")
        return {}

    # Prefer FeatureBuilder().build(...) (canonical API)
    with contextlib.suppress(Exception):
        cls = getattr(FB, "FeatureBuilder", None)
        if cls is not None:
            fb = cls()
            res = fb.build(
                symbol=pool_meta.get("symbol"),
                mint=pool_meta.get("mint"),
                snapshot=dict(raw_pool),
                history=context.get("history"),
                regime=context.get("regime"),
                wallet_signals=context.get("wallet_signals"),
                ai_signals=context.get("ai_signals"),
                scanner_source=context.get("scanner_source"),
                risk_context=context.get("risk_context"),
            )
            feats = await _await_with_timeout(res, float(cfg.timeout_feature_build_s or 0.0))
            if isinstance(feats, dict):
                return feats
            with contextlib.suppress(Exception):
                if hasattr(feats, "to_dict"):
                    d = feats.to_dict()
                    if isinstance(d, dict):
                        return d

    # Fallback: module-level build_features(snapshot)
    fn = getattr(FB, "build_features", None)
    if callable(fn):
        with contextlib.suppress(Exception):
            feats = await _await_with_timeout(
                fn(dict(raw_pool)), float(cfg.timeout_feature_build_s or 0.0)
            )
            if isinstance(feats, dict):
                return feats

    if log.every("SCORING_FB_NO_USABLE", 600):
        log.warning("SCORING: no usable feature_builder API found; using empty features.")
    return {}


# ──────────────────────────────────────────────────────────────────────────────
# Normalizers for optional module outputs
# ──────────────────────────────────────────────────────────────────────────────
def _normalize_score_reason(res: Any, *, key: str = "score") -> tuple[float | None, str]:
    if res is None:
        return None, ""
    if isinstance(res, int | float) and not isinstance(res, bool):
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
    if isinstance(res, tuple | list) and len(res) >= 1:
        sc = None
        why = ""
        with contextlib.suppress(Exception):
            sc = float(res[0])
        if len(res) >= 2:
            why = str(res[1])
        return sc, why
    return None, ""


def _normalize_pass_gate(res: Any) -> tuple[bool, float | None, str]:
    if res is None:
        return True, None, ""
    if isinstance(res, bool):
        return bool(res), None, ""
    if isinstance(res, dict):
        ok = bool(res.get("ok", res.get("allow", res.get("pass", True))))
        sc, why = _normalize_score_reason(res, key="score")
        why = why or str(res.get("why") or "")
        return ok, sc, why
    if isinstance(res, tuple | list) and len(res) >= 1:
        ok = bool(res[0])
        sc = None
        why = ""
        if len(res) >= 2 and isinstance(res[1], int | float) and not isinstance(res[1], bool):
            sc = float(res[1])
        if len(res) >= 3:
            why = str(res[2])
        return ok, sc, why
    return True, None, ""


def _candidate_kwargs(pool_meta: dict[str, Any], features: dict[str, Any]) -> dict[str, Any]:
    kw: dict[str, Any] = {}
    kw["mint"] = pool_meta.get("mint")
    kw["symbol"] = pool_meta.get("symbol")
    kw["liq_usd"] = pool_meta.get("liq_usd", features.get("liq_usd", 0.0))
    kw["age_min"] = pool_meta.get(
        "age_min", features.get("age_min", features.get("token_age_min", 0.0))
    )
    kw["chg5"] = pool_meta.get(
        "chg_5m_pct", features.get("price_change_5m_pct", features.get("chg_5m_pct", 0.0))
    )
    kw["buys5"] = pool_meta.get("buys_5m", features.get("buys_5m", features.get("buys5", 0)))
    kw["sells5"] = pool_meta.get("sells_5m", features.get("sells_5m", features.get("sells5", 0)))
    kw["vol5"] = pool_meta.get("vol_5m_usd", features.get("vol_5m_usd", features.get("vol5", 0.0)))
    kw["mcap_usd"] = pool_meta.get("mcap_usd", features.get("mcap_usd", features.get("mcap", 0.0)))
    return kw


# ──────────────────────────────────────────────────────────────────────────────
# AI adapters (async-safe, best-effort, latency-capped)
# ──────────────────────────────────────────────────────────────────────────────
async def _call_ai_buy_scoring(
    features: dict[str, Any],
    pool_meta: dict[str, Any],
    cfg: ScoreConfig,
) -> tuple[float | None, bool, str]:
    _, _, _, ABS = _lazy_imports()
    if ABS is None:
        return None, True, "ai_buy_scoring not available"

    # score_only(features)
    with contextlib.suppress(Exception):
        score_only = getattr(ABS, "score_only", None)
        if callable(score_only):
            res = score_only(features)
            res = await _await_with_timeout(res, float(cfg.timeout_abs_s or 0.0))
            sc, why = _normalize_score_reason(res, key="score")
            sc100 = _score01_to_100(sc)
            if sc100 is not None:
                return sc100, True, why or "ai_buy_scoring.score_only"

    # pass_gate(Candidate(...))
    with contextlib.suppress(Exception):
        pass_gate = getattr(ABS, "pass_gate", None)
        Candidate = getattr(ABS, "Candidate", None)
        if callable(pass_gate) and Candidate is not None:
            c = Candidate(**_candidate_kwargs(pool_meta, features))  # type: ignore
            res = pass_gate(c)
            res = await _await_with_timeout(res, float(cfg.timeout_abs_s or 0.0))
            ok, sc, why = _normalize_pass_gate(res)
            sc100 = _score01_to_100(sc)
            if sc100 is not None:
                return sc100, bool(ok), why or "ai_buy_scoring.pass_gate"
            return (
                None,
                bool(ok),
                why or ("ai_buy_scoring gate OK" if ok else "ai_buy_scoring gate FAIL"),
            )

    return None, True, "ai_buy_scoring has no usable API"


async def _call_ai_entry(
    features: dict[str, Any],
    pool_meta: dict[str, Any],
    context: dict[str, Any],
    cfg: ScoreConfig,
) -> tuple[float | None, str]:
    if not cfg.enable_ai_entry:
        return None, "AI entry disabled"

    # Optional prefer ABS numeric score
    abs_sc, abs_gate_ok, abs_reason = await _call_ai_buy_scoring(features, pool_meta, cfg)
    if abs_sc is not None and cfg.prefer_abs_ai_score:
        return float(abs_sc), f"ABS: {abs_reason} (gate_ok={abs_gate_ok})"

    _, AEL, _, _ = _lazy_imports()
    if AEL is None:
        eq = features.get("entry_quality_score")
        if eq is not None:
            return _clamp(float(eq) / 1.5 * 100.0), "fallback: entry_quality_score"
        return None, "ai_entry_logic not available"

    for fname in ("score_candidate", "score_entry", "score_only", "score"):
        fn = getattr(AEL, fname, None)
        if not callable(fn):
            continue

        attempts: list[tuple[tuple[Any, ...], dict[str, Any]]] = [
            ((), {"meta": pool_meta, "features": features, **context}),
            ((), {"meta": pool_meta, "features": features}),
            ((), {"features": features, "meta": pool_meta}),
            ((features,), {"meta": pool_meta}),
            ((features,), {}),
        ]

        for args, kwargs in attempts:
            try:
                res = fn(*args, **kwargs)
                res = await _await_with_timeout(res, float(cfg.timeout_ai_entry_s or 0.0))
                sc, why = _normalize_score_reason(res, key="score")
                if sc is None:
                    sc, why = _normalize_score_reason(res, key="entry_score")
                sc100 = _score01_to_100(sc)
                if sc100 is None:
                    continue
                return float(sc100), why or f"ai_entry_logic.{fname}"
            except (TimeoutError, TypeError):
                continue
            except Exception as exc:
                if log.every(f"SCORING_AEL_ERR_{fname}", 600):
                    log.warning("SCORING: ai_entry_logic.%s error: %s", fname, exc)
                if not cfg.fail_open:
                    break
                continue

    eq = features.get("entry_quality_score")
    if eq is not None:
        return _clamp(float(eq) / 1.5 * 100.0), "fallback: entry_quality_score"
    return None, "ai_entry_logic has no usable scoring function"


async def _call_ai_risk(
    features: dict[str, Any],
    pool_meta: dict[str, Any],
    context: dict[str, Any],
    cfg: ScoreConfig,
) -> tuple[float | None, str]:
    if not cfg.enable_ai_risk:
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

        attempts: list[tuple[tuple[Any, ...], dict[str, Any]]] = [
            ((), {"meta": pool_meta, "features": features, **context}),
            ((), {"meta": pool_meta, "features": features}),
            ((), {"features": features, "meta": pool_meta}),
            ((features,), {"meta": pool_meta}),
            ((features,), {}),
        ]

        for args, kwargs in attempts:
            try:
                res = fn(*args, **kwargs)
                res = await _await_with_timeout(res, float(cfg.timeout_ai_risk_s or 0.0))
                sc, why = _normalize_score_reason(res, key="risk_score")
                if sc is None:
                    sc, why = _normalize_score_reason(res, key="score")
                sc100 = _score01_to_100(sc)
                if sc100 is None:
                    continue
                return float(sc100), why or f"ai_risk_model.{fname}"
            except (TimeoutError, TypeError):
                continue
            except Exception as exc:
                if log.every(f"SCORING_ARM_ERR_{fname}", 600):
                    log.warning("SCORING: ai_risk_model.%s error: %s", fname, exc)
                if not cfg.fail_open:
                    break
                continue

    er = features.get("entry_risk_score")
    if er is not None:
        return _clamp(float(er) * 100.0), "fallback: entry_risk_score"
    return None, "ai_risk_model has no usable scoring function"


# ──────────────────────────────────────────────────────────────────────────────
# Heuristic component scoring (feature-first, meta fallback)
# ──────────────────────────────────────────────────────────────────────────────
def _compute_momentum_score(
    features: dict[str, Any], pool_meta: dict[str, Any]
) -> tuple[float, str]:
    chg_1m = (
        _first_present(
            features.get("price_change_1m_pct"),
            features.get("chg_1m_pct"),
            pool_meta.get("chg_1m_pct"),
            default=0.0,
        )
        or 0.0
    )
    chg_5m = (
        _first_present(
            features.get("price_change_5m_pct"),
            features.get("chg_5m_pct"),
            pool_meta.get("chg_5m_pct"),
            default=0.0,
        )
        or 0.0
    )

    buys = _first_present(features.get("buys_5m"), pool_meta.get("buys_5m"), default=0.0) or 0.0
    sells = _first_present(features.get("sells_5m"), pool_meta.get("sells_5m"), default=0.0) or 0.0
    vol5 = (
        _first_present(features.get("vol_5m_usd"), pool_meta.get("vol_5m_usd"), default=0.0) or 0.0
    )
    liq = float(pool_meta.get("liq_usd") or 0.0)

    pump_vel = _first_present(features.get("pump_velocity_score"), default=0.0) or 0.0
    pump_vel_norm = _clip01(_safe_ratio(pump_vel, 10.0, 0.0))  # 0..1

    pos5 = max(0.0, min(120.0, chg_5m))
    pos1 = max(0.0, min(80.0, chg_1m))
    neg5 = max(0.0, min(120.0, -chg_5m))

    base = 30.0 + (pos5 / 30.0) * 45.0 + (pos1 / 10.0) * 15.0 - (neg5 / 20.0) * 35.0
    base = _clamp(base)

    total = buys + sells
    buy_ratio = (buys / total) if total > 0 else 0.5
    imbalance = (buy_ratio - 0.5) * 70.0  # -35..+35

    v_rel = 0.0
    if liq > 0 and vol5 > 0:
        v_rel = min(3.0, vol5 / max(1.0, liq))  # 0..3
    intensity = v_rel * 14.0  # 0..42

    score = _clamp(base + imbalance + intensity + 10.0 * pump_vel_norm)
    why = f"chg1m={chg_1m:.2f}% chg5m={chg_5m:.2f}%; br={buy_ratio:.2f}; vol5/liq={v_rel:.2f}; pumpVelN={pump_vel_norm:.2f}"
    return score, why


def _compute_quality_score(
    cfg: ScoreConfig, features: dict[str, Any], pool_meta: dict[str, Any]
) -> tuple[float, str]:
    eq = features.get("entry_quality_score")
    if isinstance(eq, int | float) and not isinstance(eq, bool):
        sc = _clamp(float(eq) / 1.5 * 100.0)
        return sc, f"from entry_quality_score={float(eq):.3f}"

    liq = float(pool_meta.get("liq_usd") or 0.0)
    age = float(pool_meta.get("age_min") or 0.0)
    mcap = float(pool_meta.get("mcap_usd") or 0.0)
    holders = _first_present(
        features.get("holders"),
        features.get("holders_total"),
        pool_meta.get("holders"),
        default=None,
    )

    score = 45.0
    reasons: list[str] = []

    if liq <= 0:
        score -= 10.0
        reasons.append("liq=0")
    else:
        ref = max(50.0, float(cfg.min_liq_usd))
        x = max(0.0, math.log10(max(1.0, liq / ref)))
        liq_score = 60.0 + min(32.0, x * 12.0)
        if liq < ref:
            liq_score = 40.0 + (liq / ref) * 20.0
        score = max(score, liq_score)
        reasons.append(f"liq=${liq:,.0f}→{liq_score:.0f}")

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

    if mcap > 0:
        if mcap < 25_000:
            score -= 4.0
            reasons.append("mcap tiny")
        elif mcap > 250_000:
            score += 2.0
            reasons.append("mcap ok")

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


def _compute_risk_score(
    cfg: ScoreConfig,
    features: dict[str, Any],
    pool_meta: dict[str, Any],
    ai_risk_score: float | None,
    context: dict[str, Any],
) -> tuple[float, str]:
    entry_risk = features.get("entry_risk_score")
    fb_risk_100 = None
    if isinstance(entry_risk, int | float) and not isinstance(entry_risk, bool):
        fb_risk_100 = _clamp(float(entry_risk) * 100.0)

    base = (
        float(ai_risk_score)
        if ai_risk_score is not None
        else (fb_risk_100 if fb_risk_100 is not None else 50.0)
    )
    reasons: list[str] = []
    if ai_risk_score is not None:
        reasons.append(f"AI={ai_risk_score:.1f}")
    elif fb_risk_100 is not None:
        reasons.append(f"FB={fb_risk_100:.1f}")

    liq = float(pool_meta.get("liq_usd") or 0.0)
    age = float(pool_meta.get("age_min") or 0.0)
    impact = float(pool_meta.get("impact_1k_bps") or 0.0)
    ai_rug = float(pool_meta.get("ai_rug_score") or 0.0)

    if ai_rug > 0:
        rug100 = ai_rug * 100.0 if 0.0 <= ai_rug <= 1.0 else ai_rug
        base = max(base, min(95.0, rug100))
        reasons.append(f"rug≈{rug100:.0f}")

    ref = max(50.0, float(cfg.min_liq_usd))
    if liq < ref:
        bump = min(28.0, (ref - liq) / ref * 45.0)
        base += bump
        reasons.append(f"low_liq(+{bump:.0f})")

    if age < 3:
        base += 14.0
        reasons.append("age<3m(+14)")
    elif age < 15:
        base += 8.0
        reasons.append("age<15m(+8)")
    elif age < 60:
        base += 3.0
        reasons.append("age<60m(+3)")
    elif age > float(cfg.max_age_min or 720.0):
        base -= 4.0
        reasons.append("old(-4)")

    if impact > 0:
        bump = min(20.0, max(0.0, (impact - 1200.0) / 1200.0) * 30.0)
        base += bump
        if bump > 0:
            reasons.append(f"impact{impact:.0f}bps(+{bump:.0f})")

    flags = [
        features.get("is_honeypot"),
        features.get("honeypot_flag"),
        features.get("rugpull_flag"),
        features.get("can_sell") is False,
        pool_meta.get("is_honeypot"),
        features.get("token_blacklisted"),
        features.get("blacklisted"),
    ]
    if any(bool(x) for x in flags):
        base = max(base, 92.0)
        reasons.append("honeypot/blacklist(>=92)")

    if (
        features.get("mint_authority_disabled") is False
        or pool_meta.get("mint_authority_disabled") is False
    ):
        base += 10.0
        reasons.append("mint_auth_on(+10)")
    if (
        features.get("freeze_authority_disabled") is False
        or pool_meta.get("freeze_authority_disabled") is False
    ):
        base += 10.0
        reasons.append("freeze_auth_on(+10)")

    top1 = _first_present(
        features.get("top_holder_pct"),
        features.get("top1_holder_pct"),
        pool_meta.get("top1_hold_pct"),
        default=None,
    )
    top10 = _first_present(
        features.get("top10_holder_pct"), pool_meta.get("top10_hold_pct"), default=None
    )
    if top1 is not None and float(top1) > 40.0:
        base += 14.0
        reasons.append("top1>40(+14)")
    if top10 is not None and float(top10) > 80.0:
        base += 10.0
        reasons.append("top10>80(+10)")

    regime = context.get("regime")
    ln = 0.0
    manip = 0.0
    if isinstance(regime, dict):
        with contextlib.suppress(Exception):
            ln = float(regime.get("label_numeric", regime.get("labelNumeric", 0.0)) or 0.0)
        with contextlib.suppress(Exception):
            manip = float(
                regime.get("manipulation_risk", regime.get("manipulationRisk", 0.0)) or 0.0
            )
    else:
        with contextlib.suppress(Exception):
            if hasattr(regime, "as_numeric"):
                ln = float(regime.as_numeric())
        with contextlib.suppress(Exception):
            if hasattr(regime, "manipulation_risk"):
                manip = float(regime.manipulation_risk)

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
def _combine_scores(
    cfg: ScoreConfig,
    momentum: float,
    risk: float,
    quality: float,
    ai_entry: float | None,
    context: dict[str, Any],
) -> float:
    ai_sc = float(ai_entry) if ai_entry is not None else (float(momentum) + float(quality)) / 2.0
    risk_contrib = 100.0 - _clamp(risk)

    combined = (
        cfg.w_ai * _clamp(ai_sc)
        + cfg.w_momentum * _clamp(momentum)
        + cfg.w_quality * _clamp(quality)
        + cfg.w_risk * _clamp(risk_contrib)
    )

    regime = context.get("regime")
    ln = 0.0
    manip = 0.0
    if isinstance(regime, dict):
        with contextlib.suppress(Exception):
            ln = float(regime.get("label_numeric", regime.get("labelNumeric", 0.0)) or 0.0)
        with contextlib.suppress(Exception):
            manip = float(
                regime.get("manipulation_risk", regime.get("manipulationRisk", 0.0)) or 0.0
            )
    else:
        with contextlib.suppress(Exception):
            if hasattr(regime, "as_numeric"):
                ln = float(regime.as_numeric())
        with contextlib.suppress(Exception):
            if hasattr(regime, "manipulation_risk"):
                manip = float(regime.manipulation_risk)

    penalty = 0.0
    if ln <= -0.7:
        penalty += float(cfg.regime_risk_penalty)
    if manip >= 0.8:
        penalty += float(cfg.manip_risk_penalty)
    elif manip >= 0.6:
        penalty += float(cfg.manip_risk_penalty) * 0.5

    if penalty > 0:
        combined = max(0.0, combined - penalty)

    return _clamp(combined)


def _derive_label(
    cfg: ScoreConfig, combined: float, momentum: float, risk: float, quality: float
) -> str:
    if (
        combined >= cfg.high_score_threshold
        and momentum >= cfg.min_momentum_for_green
        and risk <= cfg.max_risk_for_green
        and quality >= cfg.min_quality_for_green
    ):
        return "GREEN"
    if combined >= cfg.medium_score_threshold and risk <= 90.0:
        return "YELLOW"
    return "RED"


async def _derive_trade_gate(
    cfg: ScoreConfig,
    pool_meta: dict[str, Any],
    features: dict[str, Any],
    combined: float,
    label: str,
    risk: float,
) -> tuple[bool, str]:
    liq = float(pool_meta.get("liq_usd") or 0.0)
    age = float(pool_meta.get("age_min") or 0.0)

    # Structural constraints first (cheap)
    if liq < float(cfg.min_liq_usd):
        return False, f"liq ${liq:.0f} < min ${cfg.min_liq_usd:.0f}"
    if age > float(cfg.max_age_min):
        return False, f"age {age:.1f}m > max {cfg.max_age_min:.0f}m"

    # FeatureBuilder hard blocks (common naming patterns)
    entry_block_flag = float(features.get("entry_block_flag", 0.0) or 0.0)
    if entry_block_flag >= 1.0:
        return False, "feature_builder entry_block_flag"
    if bool(features.get("gate_hard_block", False)):
        return False, "feature_builder gate_hard_block"
    if bool(features.get("is_honeypot", False)) or bool(features.get("honeypot_flag", False)):
        return False, "honeypot flag"
    if bool(features.get("token_blacklisted", False)) or bool(features.get("blacklisted", False)):
        return False, "blacklisted flag"

    if label == "RED":
        return False, "RED label"
    if combined < float(cfg.medium_score_threshold):
        return False, f"combined {combined:.1f} < {cfg.medium_score_threshold:.1f}"

    # Optional ABS gate enforcement (latency capped)
    if cfg.enable_abs_gate:
        try:
            _, abs_ok, abs_reason = await _call_ai_buy_scoring(features, pool_meta, cfg)
            if not abs_ok:
                return False, abs_reason or "ai_buy_scoring gate blocked"
        except TimeoutError:
            if not cfg.fail_open:
                return False, "ai_buy_scoring timeout"
        except Exception as exc:
            if log.every("SCORING_ABS_GATE_ERR", 600):
                log.warning("SCORING: ABS gate error: %s", exc)
            if not cfg.fail_open:
                return False, "ai_buy_scoring error"

    if risk >= 92.0:
        return False, f"risk {risk:.0f} too high"

    soft_all = features.get("gate_all_soft_ok")
    if isinstance(soft_all, int | float) and not isinstance(soft_all, bool):
        if float(soft_all) <= 0.0 and label == "YELLOW":
            return False, "feature_builder soft-gate"

    return True, "OK"


def _format_reasons(
    label: str,
    combined: float,
    ai_entry: float | None,
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
    parts: list[str] = []
    parts.append(
        f"Label={label} combined={combined:.1f}/100 gate={gate_ok}({gate_reason}) size≈{size_hint:.2f}"
    )
    parts.append(f"AI={'N/A' if ai_entry is None else f'{ai_entry:.1f}/100'} ({ai_entry_reason})")
    parts.append(f"MOMO={momentum:.1f}/100 ({momentum_reason})")
    parts.append(f"RISK={risk:.1f}/100 (higher=worse; {risk_reason})")
    parts.append(f"QUAL={quality:.1f}/100 ({quality_reason})")
    return " | ".join(parts)


# ──────────────────────────────────────────────────────────────────────────────
# Public API
# ──────────────────────────────────────────────────────────────────────────────
async def score_candidate(raw_pool: Any, context: dict[str, Any] | None = None) -> ScoredToken:
    """
    Main scoring entrypoint.
    raw_pool: dict-like from scanner (Raydium / Pumpfun / Dexscreener / etc.)
    context: optional info (regime/risk_context/history/wallet_signals/ai_signals/scanner_source)
    """
    if context is None:
        context = {}

    start = _now_mono()
    src = str(context.get("scanner_source") or "").strip().lower()
    cfg = CONFIG.for_source(src)

    raw_dict = _as_dict(raw_pool)
    pool_meta = _normalize_pool_meta(raw_dict)
    mint = pool_meta.get("mint") or _normalize_mint(
        raw_dict.get("mint") or raw_dict.get("base_mint") or ""
    )
    pool_meta["mint"] = mint

    key = _cache_key(mint, context, cfg) if mint else ""
    if key:
        cached = await _cache_get(key, cfg)
        if cached is not None:
            return cached

    # In-flight de-dupe (prevents stampedes in parallel scans)
    if key:
        async with _CACHE_LOCK:
            fut = _INFLIGHT.get(key)
            if fut is not None and not fut.done():
                return await fut
            loop = asyncio.get_running_loop()
            fut = loop.create_future()
            _INFLIGHT[key] = fut

    async def _compute() -> ScoredToken:
        # Features (canonical)
        features: dict[str, Any] = {}
        try:
            features = await _build_features(raw_dict, pool_meta, context, cfg)
            if not isinstance(features, dict):
                features = {}
        except TimeoutError:
            if log.every("SCORING_FB_TIMEOUT", 120):
                log.warning("SCORING: feature_builder timeout (src=%s mint=%s)", src or "na", mint)
            if not cfg.fail_open:
                raise
        except Exception as exc:
            if log.every("SCORING_FB_ERR", 120):
                log.warning(
                    "SCORING: feature_builder error (src=%s mint=%s): %s", src or "na", mint, exc
                )
            if not cfg.fail_open:
                raise
            features = {}

        # Optional time budget guard: if we already burned most budget, skip heavy AI calls
        elapsed = _now_mono() - start
        remaining = float(cfg.timeout_total_s or 0.0) - elapsed
        if float(cfg.timeout_total_s or 0.0) > 0 and remaining <= 0:
            ai_entry_score, ai_entry_reason = None, "skipped: total timeout"
            ai_risk_score, ai_risk_reason = None, "skipped: total timeout"
        else:
            ai_entry_score, ai_entry_reason = await _call_ai_entry(
                features, pool_meta, context, cfg
            )
            ai_risk_score, ai_risk_reason = await _call_ai_risk(features, pool_meta, context, cfg)

        momentum_score, momentum_reason = _compute_momentum_score(features, pool_meta)
        quality_score, quality_reason = _compute_quality_score(cfg, features, pool_meta)
        risk_score, risk_reason_h = _compute_risk_score(
            cfg, features, pool_meta, ai_risk_score, context
        )
        risk_reason = "; ".join([x for x in [ai_risk_reason, risk_reason_h] if x])

        combined_score = _combine_scores(
            cfg, momentum_score, risk_score, quality_score, ai_entry_score, context
        )
        label = _derive_label(cfg, combined_score, momentum_score, risk_score, quality_score)

        is_trade_viable, gate_reason = await _derive_trade_gate(
            cfg, pool_meta, features, combined_score, label, risk_score
        )

        size_hint = 0.0
        for k in ("position_size_scalar", "size_scalar", "size_hint"):
            with contextlib.suppress(Exception):
                v = float(features.get(k, 0.0) or 0.0)
                if v > 0:
                    size_hint = v
                    break
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
            mint=str(pool_meta.get("mint") or ""),
            symbol=str(pool_meta.get("symbol") or "UNKNOWN"),
            chain=str(pool_meta.get("chain") or "SOL"),
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
            raw_features=features,
            created_at_ts=time.time(),
        )
        return st

    try:
        st = await _compute()
        if key:
            await _cache_put(key, st, cfg)

        if log.every("SCORING_SAMPLE", 60):
            log.info(
                "SCORING: %s (%s) src=%s label=%s combined=%.1f liq=$%.0f age=%.1fmin ok=%s size=%.2f",
                st.symbol,
                st.mint,
                src or "na",
                st.label,
                st.combined_score,
                st.liq_usd,
                st.age_min,
                st.is_trade_viable,
                st.size_hint,
            )

        return st
    finally:
        if key:
            # Resolve inflight future
            async with _CACHE_LOCK:
                fut = _INFLIGHT.get(key)
                if fut is not None and not fut.done():
                    # If we got here via exception, we still want callers to proceed.
                    # Best-effort: set exception; callers using fail_open paths won't usually hit this.
                    try:
                        # st may or may not exist in scope; guard safely.
                        if "st" in locals():
                            fut.set_result(locals()["st"])
                        else:
                            fut.set_exception(RuntimeError("scoring_service: inflight aborted"))
                    except Exception:
                        pass
                _INFLIGHT.pop(key, None)


async def score_many(
    raw_pools: Iterable[Any], context: dict[str, Any] | None = None
) -> list[ScoredToken]:
    """
    Convenience helper: score many sequentially (predictable, low overhead).
    """
    if context is None:
        context = {}
    out: list[ScoredToken] = []
    for raw in raw_pools:
        try:
            out.append(await score_candidate(raw, context=context))
        except Exception as exc:  # pragma: no cover
            if log.every("SCORING_MANY_ERR", 60):
                r = _as_dict(raw)
                log.warning(
                    "SCORING: error scoring pool mint=%r: %s",
                    r.get("mint") or r.get("base_mint"),
                    exc,
                )
    return out


async def score_many_parallel(
    raw_pools: Iterable[Any],
    context: dict[str, Any] | None = None,
    *,
    max_concurrency: int | None = None,
    keep_order: bool = True,
) -> list[ScoredToken]:
    """
    Parallel scoring helper (bounded). Use this for radar / wide scans.
    keep_order=True returns results aligned to input order (dropping failures).
    """
    if context is None:
        context = {}

    pools = list(raw_pools)
    if not pools:
        return []

    cfg = CONFIG.for_source(str(context.get("scanner_source") or "").strip().lower())
    mc = int(max_concurrency or cfg.max_concurrency or 12)
    mc = max(1, mc)
    sem = asyncio.Semaphore(mc)

    out: list[ScoredToken | None] = [None] * len(pools)

    async def _one(i: int, p: Any) -> None:
        async with sem:
            try:
                out[i] = await score_candidate(p, context=context)
            except Exception as exc:  # pragma: no cover
                if log.every("SCORING_PAR_ERR", 60):
                    r = _as_dict(p)
                    log.warning(
                        "SCORING: parallel error mint=%r: %s",
                        r.get("mint") or r.get("base_mint"),
                        exc,
                    )

    await asyncio.gather(*[_one(i, p) for i, p in enumerate(pools)])

    if keep_order:
        return [x for x in out if x is not None]
    return [x for x in out if x is not None]


__all__ = [
    "CONFIG",
    "ScoreConfig",
    "ScoredToken",
    "cache_clear",
    "reload_config",
    "score_candidate",
    "score_many",
    "score_many_parallel",
]
