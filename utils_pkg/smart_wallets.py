from __future__ import annotations

# -*- coding: utf-8 -*-
"""
utils_pkg/smart_wallets.py  —  V3+++ / quant-grade / God-Tier (FINAL rewrite)

Smart wallet & whale orderflow engine (NO network calls).
Production goals:
- Backwards compatible names + helpers
- Runtime-tunable via env snapshot TTL
- Quant-grade features for entry/exit IQ + regime/risk_manager
- Bounded memory + per-mint caches
- Safe, deterministic behavior (never blocks on I/O; no external deps)
- Logging contract safe: no hard dependency on logging_setup; supports .event if available

Primary consumers (expected):
- utils_pkg/feature_builder.py (wallet_signals / flattened meta)
- utils_pkg/ai_entry_logic.py / ai_buy_scoring.py
- scanners (observe_* adapters)
- risk_manager / regime / self_learning_ai_strategy
"""

import asyncio
import contextlib
import json
import os
import time
from dataclasses import dataclass
from typing import Any, Protocol, cast

# =============================================================================
# Logging (contract-safe, no hard import)
# =============================================================================


class _LogLike(Protocol):
    def debug(self, msg: str, *args: Any, **kwargs: Any) -> None: ...
    def info(self, msg: str, *args: Any, **kwargs: Any) -> None: ...
    def warning(self, msg: str, *args: Any, **kwargs: Any) -> None: ...
    def error(self, msg: str, *args: Any, **kwargs: Any) -> None: ...
    def exception(self, msg: str, *args: Any, **kwargs: Any) -> None: ...

    # optional structured logger hook used elsewhere in your repo
    def event(self, name: str, **fields: Any) -> None: ...  # type: ignore[empty-body]


def _maybe_import_attr(module_name: str, attr: str) -> Any | None:
    try:
        mod = __import__(module_name, fromlist=[attr])
        return getattr(mod, attr, None)
    except Exception:
        return None


def _build_logger() -> _LogLike:
    import logging

    base = logging.getLogger("sniper.smart_wallets")

    # Prefer your repo’s compat shims if present, but never hard-fail.
    for mod_name in ("utils_pkg.log_compat", "utils_pkg.logging_setup", "logging_setup"):
        get_logger = _maybe_import_attr(mod_name, "get_logger")
        if callable(get_logger):
            try:
                base = get_logger("sniper.smart_wallets")
                break
            except Exception:
                base = logging.getLogger("sniper.smart_wallets")

    # Optional structured wrapper (if your repo provides it)
    get_event_logger = _maybe_import_attr("utils_pkg.event_logger", "get_event_logger")
    if callable(get_event_logger):
        with contextlib.suppress(Exception):
            return cast(_LogLike, get_event_logger(base))

    return cast(_LogLike, base)


log = _build_logger()


# =============================================================================
# Env helpers (strictly local parsing; no side-effects)
# =============================================================================


def _env_bool(key: str, default: bool) -> bool:
    v = os.getenv(key)
    if v is None:
        return default
    s = str(v).strip().lower()
    if s in ("1", "true", "yes", "y", "on"):
        return True
    if s in ("0", "false", "no", "n", "off"):
        return False
    return default


def _env_int(key: str, default: int) -> int:
    raw = os.getenv(key)
    if raw is None:
        return default
    try:
        return int(str(raw).strip())
    except Exception:
        return default


def _env_float(key: str, default: float) -> float:
    raw = os.getenv(key)
    if raw is None:
        return default
    try:
        return float(str(raw).strip())
    except Exception:
        return default


def _env_list(key: str) -> list[str]:
    raw = os.getenv(key, "").strip()
    if not raw:
        return []
    out: list[str] = []
    for chunk in raw.replace("\n", ",").split(","):
        c = chunk.strip()
        if c:
            out.append(c)
    return out


def _env_set(key: str) -> set[str]:
    return set(_env_list(key))


def _env_json(key: str) -> Any:
    raw = os.getenv(key, "").strip()
    if not raw:
        return None
    try:
        return json.loads(raw)
    except Exception as e:
        if hasattr(log, "event"):
            with contextlib.suppress(Exception):
                log.event("SMART_ENV_JSON_FAIL", key=key, error=str(e)[:200])
        else:
            log.warning("SMART_ENV_JSON_FAIL key=%s error=%s", key, str(e)[:200])
        return None


def _clamp(x: float, lo: float, hi: float) -> float:
    if x < lo:
        return lo
    if x > hi:
        return hi
    return x


def _decay_factor(dt: float, half_life: float) -> float:
    """Exponential half-life decay factor."""
    if half_life <= 0.0 or dt <= 0.0:
        return 1.0
    return 0.5 ** (dt / half_life)


def _decay_update(
    value: float, last_ts: float, now: float, half_life: float, *, increment: float = 0.0
) -> float:
    dt = max(0.0, now - last_ts)
    decayed = value * _decay_factor(dt, half_life)
    return decayed + increment


def _to_01_from_count(count: float, scale: float) -> float:
    """Soft clip a non-negative count into [0,1]."""
    sc = max(1e-6, float(scale))
    x = max(0.0, float(count)) / sc
    return 1.0 if x >= 1.0 else float(x)


def _net_minus_one_to_one(pos: float, neg: float) -> float:
    """Convert (positive_count, negative_count) -> net in [-1,1]."""
    p = max(0.0, float(pos))
    n = max(0.0, float(neg))
    denom = p + n
    if denom <= 0.0:
        return 0.0
    return float(_clamp((p - n) / denom, -1.0, 1.0))


def _mono() -> float:
    # monotonic for TTL checks (safe across clock shifts)
    return time.monotonic()


# =============================================================================
# Phase-aware config snapshot (TTL)
# =============================================================================


@dataclass(frozen=True)
class _Cfg:
    cfg_ttl_sec: float
    registry_ttl_sec: float

    # phase/mode
    phase: int
    mode: str  # off/simple/alpha/full

    # toggles
    enabled: bool
    whale_enabled: bool
    breadth_enabled: bool
    alpha_only: bool

    # weights / mapping
    min_score_for_rich_features: float
    cluster_weights: dict[str, float]
    profile_overrides: dict[str, Any]
    alpha_clusters: set[str]

    # smart decay & contribution
    smart_half_life_sec: float
    smart_max_abs_score: float
    smart_min_notional_usd: float
    smart_max_contrib_per_trade: float
    smart_sell_multiplier: float

    # smart volume (anti-spoof / quality)
    smart_volume_half_life_sec: float
    smart_spoof_avg_notional_usd: float

    # whales
    whale_min_notional_usd: float
    whale_min_notional_sol: float
    whale_half_life_sec: float
    whale_footprint_anchor_usd: float

    # breadth uniques
    unique_addr_min_gap_sec: float
    unique_half_life_sec: float

    # score scales
    smart_event_scale: float
    whale_side_scale: float
    unique_buyers_scale: float
    smart_unique_buyers_scale: float

    # conflict/domination thresholds
    whale_dom_threshold: float
    conflict_net_min: float
    accum_alpha_scale: float
    panic_dump_scale: float

    # signal meta
    signal_recency_sec: float
    signal_min_events_for_conf: float

    # cache + memory bounds
    feature_cache_sec: float
    max_mints: int
    idle_prune_sec: float
    max_addrs_per_mint: int
    addr_prune_older_sec: float

    # reinvest safety
    reinvest_only_mints: set[str]
    reinvest_exclude_mints: set[str]
    reinvest_require_tradeable: bool
    reinvest_max_panic: float
    reinvest_max_risk: float
    reinvest_max_conflict: float
    reinvest_return_mint_only: bool

    # tradeable thresholds
    tradeable_min_conf: float
    tradeable_min_quality: float
    tradeable_min_heat: float
    tradeable_max_risk: float

    # debug
    debug_log: bool


_CFG: _Cfg | None = None
_CFG_AT_M: float = 0.0

# Back-compat globals (refreshed from cfg)
MASTER_PHASE: int = 7
SMART_WALLET_MODE: str = "full"
SMART_WALLET_ENABLED: bool = True
SMART_WHALE_ENABLED: bool = True
SMART_BREADTH_ENABLED: bool = True
SMART_ALPHA_ONLY_MODE: bool = False
SMART_DEBUG_LOG: bool = False


def _compute_default_mode(phase: int) -> str:
    if phase <= 0:
        return "off"
    if phase <= 2:
        return "simple"
    if phase <= 4:
        return "alpha"
    return "full"


def _apply_cfg_to_globals(cfg: _Cfg) -> None:
    global MASTER_PHASE, SMART_WALLET_MODE, SMART_WALLET_ENABLED, SMART_WHALE_ENABLED
    global SMART_BREADTH_ENABLED, SMART_ALPHA_ONLY_MODE, SMART_DEBUG_LOG
    MASTER_PHASE = cfg.phase
    SMART_WALLET_MODE = cfg.mode
    SMART_WALLET_ENABLED = cfg.enabled
    SMART_WHALE_ENABLED = cfg.whale_enabled
    SMART_BREADTH_ENABLED = cfg.breadth_enabled
    SMART_ALPHA_ONLY_MODE = cfg.alpha_only
    SMART_DEBUG_LOG = cfg.debug_log


def _load_cfg() -> _Cfg:
    phase = _env_int("MASTER_PHASE", _env_int("STRATEGY_PHASE", 7))
    phase = max(0, min(7, int(phase)))

    default_mode = _compute_default_mode(phase)
    mode = os.getenv("SMART_WALLET_MODE", default_mode).strip().lower()
    if mode not in {"off", "simple", "alpha", "full"}:
        mode = default_mode

    enabled = _env_bool("SMART_WALLET_ENABLED", mode != "off")
    whale_enabled = _env_bool("SMART_WALLET_WHALE_ENABLED", mode != "off")
    breadth_enabled = _env_bool("SMART_WALLET_BREADTH_ENABLED", mode != "off")
    alpha_only = _env_bool("SMART_WALLET_ALPHA_ONLY_MODE", mode == "alpha")

    min_score_for_rich = _env_float("SMART_WALLET_MIN_SCORE_FOR_RICH_FEATURES", 0.05)

    cluster_weights = _env_json("SMART_WALLET_CLUSTER_WEIGHTS_JSON") or {}
    if not isinstance(cluster_weights, dict):
        cluster_weights = {}

    profile_overrides = _env_json("SMART_WALLET_PROFILE_OVERRIDES_JSON") or {}
    if not isinstance(profile_overrides, dict):
        profile_overrides = {}

    alpha_clusters = set(n.lower() for n in _env_list("SMART_WALLET_ALPHA_CLUSTERS"))

    smart_half_life = _env_float("SMART_WALLET_DECAY_HALF_LIFE_SEC", 900.0)
    smart_max_abs = _env_float("SMART_WALLET_MAX_SCORE", 1.2)
    smart_min_notional = _env_float("SMART_WALLET_MIN_NOTIONAL_USD", 50.0)
    smart_max_contrib = _env_float("SMART_WALLET_MAX_CONTRIB_PER_TRADE", 1.0)
    smart_sell_mult = _env_float("SMART_WALLET_SELL_MULTIPLIER", 0.5)

    smart_vol_hl = _env_float("SMART_WALLET_VOLUME_HALF_LIFE_SEC", 600.0)
    spoof_avg_usd = _env_float("SMART_WALLET_SPOOF_AVG_NOTIONAL_USD", 120.0)

    whale_min_usd = _env_float("SMART_WALLET_WHALE_MIN_NOTIONAL_USD", 1_000.0)
    whale_min_sol = _env_float("SMART_WALLET_WHALE_MIN_NOTIONAL_SOL", 5.0)
    whale_half_life = _env_float("SMART_WALLET_WHALE_HALF_LIFE_SEC", 300.0)
    footprint_anchor = _env_float("SMART_WALLET_WHALE_FOOTPRINT_ANCHOR_USD", 50_000.0)

    unique_gap = _env_float("SMART_WALLET_UNIQUE_ADDR_MIN_GAP_SEC", 60.0)
    unique_half_life = _env_float("SMART_WALLET_UNIQUE_HALF_LIFE_SEC", whale_half_life)

    smart_event_scale = _env_float("SMART_WALLET_COUNT_SCORE_SCALE", 5.0)
    whale_side_scale = _env_float("SMART_WALLET_WHALE_SIDE_SCORE_SCALE", 5.0)
    unique_buyers_scale = _env_float("SMART_WALLET_UNIQUE_BUYERS_SCALE", 10.0)
    smart_unique_scale = _env_float("SMART_WALLET_SMART_UNIQUE_BUYERS_SCALE", 6.0)

    whale_dom_thr = _env_float("SMART_WALLET_WHALE_DOM_THRESHOLD", 0.7)
    conflict_net_min = _env_float("SMART_WALLET_CONFLICT_NET_MIN", 0.4)
    accum_alpha_scale = _env_float("SMART_WALLET_ACCUM_ALPHA_SCALE", 1.0)
    panic_dump_scale = _env_float("SMART_WALLET_PANIC_DUMP_SCALE", 1.0)

    recency_sec = _env_float("SMART_WALLET_SIGNAL_RECENCY_SEC", 900.0)
    min_events_conf = _env_float("SMART_WALLET_SIGNAL_MIN_EVENTS_FOR_CONF", 3.0)

    feature_cache_sec = _env_float("SMART_WALLET_FEATURE_CACHE_SEC", 0.5)
    max_mints = int(_env_float("SMART_WALLET_MAX_MINTS", 4000))
    idle_prune_sec = _env_float("SMART_WALLET_IDLE_PRUNE_SEC", 7200.0)
    max_addrs_per_mint = int(_env_float("SMART_WALLET_MAX_ADDRS_PER_MINT", 1500))
    addr_prune_older_sec = _env_float("SMART_WALLET_ADDR_PRUNE_OLDER_SEC", 6 * 3600.0)

    reinvest_only = _env_set("SMART_WALLET_REINVEST_ONLY_MINTS")
    reinvest_excl = _env_set("SMART_WALLET_REINVEST_EXCLUDE_MINTS")
    reinvest_require_tradeable = _env_bool("SMART_WALLET_REINVEST_REQUIRE_TRADEABLE", True)
    reinvest_max_panic = _env_float("SMART_WALLET_REINVEST_MAX_PANIC", 0.65)
    reinvest_max_risk = _env_float("SMART_WALLET_REINVEST_MAX_RISK", 0.70)
    reinvest_max_conflict = _env_float("SMART_WALLET_REINVEST_MAX_CONFLICT", 0.75)
    reinvest_return_mint_only = _env_bool("SMART_WALLET_REINVEST_RETURN_MINT_ONLY", False)

    trade_min_conf = _env_float("SMART_WALLET_TRADEABLE_MIN_CONF", 0.25)
    trade_min_qual = _env_float("SMART_WALLET_TRADEABLE_MIN_QUALITY", 0.25)
    trade_min_heat = _env_float("SMART_WALLET_TRADEABLE_MIN_HEAT", 0.10)
    trade_max_risk = _env_float("SMART_WALLET_TRADEABLE_MAX_RISK", 0.85)

    debug_log = _env_bool("SMART_WALLET_DEBUG_LOG", False)
    cfg_ttl = _env_float("SMART_WALLET_CFG_TTL_SEC", 2.0)
    registry_ttl = _env_float("SMART_WALLET_REGISTRY_TTL_SEC", 0.0)

    cfg = _Cfg(
        cfg_ttl_sec=max(0.0, float(cfg_ttl)),
        registry_ttl_sec=max(0.0, float(registry_ttl)),
        phase=phase,
        mode=mode,
        enabled=bool(enabled),
        whale_enabled=bool(whale_enabled),
        breadth_enabled=bool(breadth_enabled),
        alpha_only=bool(alpha_only),
        min_score_for_rich_features=float(_clamp(min_score_for_rich, 0.0, 1.0)),
        cluster_weights=cast(dict[str, float], cluster_weights),
        profile_overrides=profile_overrides,
        alpha_clusters=alpha_clusters,
        smart_half_life_sec=max(1e-6, float(smart_half_life)),
        smart_max_abs_score=max(0.1, float(smart_max_abs)),
        smart_min_notional_usd=max(0.0, float(smart_min_notional)),
        smart_max_contrib_per_trade=max(0.0, float(smart_max_contrib)),
        smart_sell_multiplier=max(0.0, float(smart_sell_mult)),
        smart_volume_half_life_sec=max(1e-6, float(smart_vol_hl)),
        smart_spoof_avg_notional_usd=max(1e-6, float(spoof_avg_usd)),
        whale_min_notional_usd=max(0.0, float(whale_min_usd)),
        whale_min_notional_sol=max(0.0, float(whale_min_sol)),
        whale_half_life_sec=max(1e-6, float(whale_half_life)),
        whale_footprint_anchor_usd=max(1e-6, float(footprint_anchor)),
        unique_addr_min_gap_sec=max(0.0, float(unique_gap)),
        unique_half_life_sec=max(1e-6, float(unique_half_life)),
        smart_event_scale=max(1e-6, float(smart_event_scale)),
        whale_side_scale=max(1e-6, float(whale_side_scale)),
        unique_buyers_scale=max(1e-6, float(unique_buyers_scale)),
        smart_unique_buyers_scale=max(1e-6, float(smart_unique_scale)),
        whale_dom_threshold=float(_clamp(whale_dom_thr, 0.0, 1.0)),
        conflict_net_min=float(_clamp(conflict_net_min, 0.0, 1.0)),
        accum_alpha_scale=max(0.0, float(accum_alpha_scale)),
        panic_dump_scale=max(0.0, float(panic_dump_scale)),
        signal_recency_sec=max(1e-6, float(recency_sec)),
        signal_min_events_for_conf=max(0.0, float(min_events_conf)),
        feature_cache_sec=max(0.0, float(feature_cache_sec)),
        max_mints=max(0, int(max_mints)),
        idle_prune_sec=max(0.0, float(idle_prune_sec)),
        max_addrs_per_mint=max(0, int(max_addrs_per_mint)),
        addr_prune_older_sec=max(60.0, float(addr_prune_older_sec)),
        reinvest_only_mints=reinvest_only,
        reinvest_exclude_mints=reinvest_excl,
        reinvest_require_tradeable=bool(reinvest_require_tradeable),
        reinvest_max_panic=float(_clamp(reinvest_max_panic, 0.0, 1.0)),
        reinvest_max_risk=float(_clamp(reinvest_max_risk, 0.0, 1.0)),
        reinvest_max_conflict=float(_clamp(reinvest_max_conflict, 0.0, 1.0)),
        reinvest_return_mint_only=bool(reinvest_return_mint_only),
        tradeable_min_conf=float(_clamp(trade_min_conf, 0.0, 1.0)),
        tradeable_min_quality=float(_clamp(trade_min_qual, 0.0, 1.0)),
        tradeable_min_heat=float(_clamp(trade_min_heat, 0.0, 1.0)),
        tradeable_max_risk=float(_clamp(trade_max_risk, 0.0, 1.0)),
        debug_log=bool(debug_log),
    )

    _apply_cfg_to_globals(cfg)
    return cfg


def _get_cfg() -> _Cfg:
    global _CFG, _CFG_AT_M
    now_m = _mono()
    if _CFG is None:
        _CFG = _load_cfg()
        _CFG_AT_M = now_m
        return _CFG
    if _CFG.cfg_ttl_sec <= 0.0:
        return _CFG
    if (now_m - _CFG_AT_M) <= _CFG.cfg_ttl_sec:
        return _CFG
    _CFG = _load_cfg()
    _CFG_AT_M = now_m
    return _CFG


# =============================================================================
# Registry
# =============================================================================


@dataclass(frozen=True)
class SmartWalletMeta:
    cluster: str
    weight: float  # -2..+2 (negative = anti/exit wallet)
    is_anti: bool = False
    half_life_override_sec: float | None = None


class SmartWalletRegistry:
    """
    Static-ish mapping: wallet address -> SmartWalletMeta.

    Env sources:
      SMART_WALLET_CLUSTERS_JSON       JSON clusters of smart wallets
      SMART_WALLET_LIST               comma/newline list of smart wallets
      SMART_WALLET_ANTI_CLUSTERS_JSON  JSON clusters of anti wallets
      SMART_WALLET_ANTI_LIST           list of anti wallets

    Notes:
    - No network calls, purely env-driven.
    - Optional TTL-based refresh via SMART_WALLET_REGISTRY_TTL_SEC (seconds).
    """

    def __init__(self) -> None:
        self._by_addr: dict[str, SmartWalletMeta] = {}
        self._loaded_at_m: float = 0.0
        self._load_from_env()

    @staticmethod
    def _normalize_addr(addr: str) -> str:
        # Do NOT lower-case (Solana base58 is case-sensitive)
        return (addr or "").strip()

    def _maybe_refresh(self) -> None:
        cfg = _get_cfg()
        ttl = float(cfg.registry_ttl_sec)
        if ttl <= 0.0:
            return
        now_m = _mono()
        if self._loaded_at_m <= 0.0 or (now_m - self._loaded_at_m) >= ttl:
            self._load_from_env()

    def _load_clusters_from_json(self, raw: Any, *, is_anti: bool, default_cluster: str) -> None:
        cfg = _get_cfg()
        if not isinstance(raw, dict):
            return

        for cluster_name, spec in raw.items():
            if not isinstance(spec, dict):
                continue

            addrs = spec.get("addresses") or spec.get("wallets") or []
            # IMPORTANT: must be a tuple of types, not a union (runtime-safe)
            if not isinstance(addrs, (list, tuple)):
                continue

            cname = str(cluster_name or default_cluster).strip() or default_cluster

            try:
                weight = float(spec.get("weight", -1.0 if is_anti else 1.0))
            except Exception:
                weight = -1.0 if is_anti else 1.0
            weight = _clamp(weight, -2.0, 2.0)

            # optional per-cluster weight override
            if isinstance(cfg.cluster_weights, dict) and cname in cfg.cluster_weights:
                with contextlib.suppress(Exception):
                    weight = _clamp(float(cfg.cluster_weights[cname]), -2.0, 2.0)

            hl = None
            if "half_life_sec" in spec and spec["half_life_sec"] is not None:
                try:
                    hl = float(spec["half_life_sec"])
                except Exception:
                    hl = None

            for addr in addrs:
                addr_norm = self._normalize_addr(str(addr))
                if not addr_norm:
                    continue
                self._by_addr[addr_norm] = SmartWalletMeta(
                    cluster=cname,
                    weight=weight,
                    is_anti=is_anti or (weight < 0),
                    half_life_override_sec=hl,
                )

    def _load_flat_list(
        self, addrs: list[str], *, is_anti: bool, default_cluster: str, weight: float
    ) -> None:
        weight = _clamp(float(weight), -2.0, 2.0)
        for addr in addrs:
            addr_norm = self._normalize_addr(addr)
            if not addr_norm:
                continue
            if addr_norm in self._by_addr:
                continue
            self._by_addr[addr_norm] = SmartWalletMeta(
                cluster=default_cluster,
                weight=weight,
                is_anti=is_anti or (weight < 0),
                half_life_override_sec=None,
            )

    def _load_from_env(self) -> None:
        clusters = _env_json("SMART_WALLET_CLUSTERS_JSON")
        flat_list = _env_list("SMART_WALLET_LIST")
        anti_clusters = _env_json("SMART_WALLET_ANTI_CLUSTERS_JSON")
        anti_flat = _env_list("SMART_WALLET_ANTI_LIST")

        self._by_addr.clear()

        # smart
        self._load_clusters_from_json(clusters, is_anti=False, default_cluster="smart_default")
        if flat_list:
            self._load_flat_list(
                flat_list, is_anti=False, default_cluster="smart_default", weight=1.0
            )

        # anti
        self._load_clusters_from_json(anti_clusters, is_anti=True, default_cluster="anti_default")
        if anti_flat:
            self._load_flat_list(
                anti_flat, is_anti=True, default_cluster="anti_default", weight=-1.0
            )

        self._loaded_at_m = _mono()

        if hasattr(log, "event"):
            with contextlib.suppress(Exception):
                log.event("SMART_REGISTRY_LOADED", count=len(self._by_addr))
        else:
            log.info("SMART_REGISTRY_LOADED count=%d", len(self._by_addr))

    def reload_from_env(self) -> None:
        self._load_from_env()

    def register_wallet(
        self,
        address: str,
        *,
        cluster: str = "runtime_smart",
        weight: float = 1.0,
        is_anti: bool = False,
        half_life_override_sec: float | None = None,
    ) -> None:
        addr_norm = self._normalize_addr(address)
        if not addr_norm:
            return
        w = _clamp(float(weight), -2.0, 2.0)
        self._by_addr[addr_norm] = SmartWalletMeta(
            cluster=cluster or "runtime_smart",
            weight=w,
            is_anti=is_anti or (w < 0),
            half_life_override_sec=half_life_override_sec,
        )

    def remove_wallet(self, address: str) -> None:
        self._by_addr.pop(self._normalize_addr(address), None)

    @property
    def by_addr(self) -> dict[str, SmartWalletMeta]:
        self._maybe_refresh()
        return self._by_addr

    def get_meta(self, address: str) -> SmartWalletMeta | None:
        self._maybe_refresh()
        return self._by_addr.get(self._normalize_addr(address))

    def is_smart(self, address: str) -> bool:
        m = self.get_meta(address)
        return bool(m and m.weight > 0)

    def is_anti(self, address: str) -> bool:
        m = self.get_meta(address)
        return bool(m and m.weight < 0)

    def cluster_for_addr(self, address: str) -> str | None:
        m = self.get_meta(address)
        return m.cluster if m else None

    def weight_for_addr(self, address: str) -> float | None:
        m = self.get_meta(address)
        return m.weight if m else None

    def explain_wallet(self, address: str) -> str:
        m = self.get_meta(address)
        if m is None:
            return f"{address}: <unregistered>"
        role = "anti" if (m.is_anti or m.weight < 0) else "smart"
        hl = "default" if m.half_life_override_sec is None else f"{m.half_life_override_sec:.0f}s"
        return f"{address}: cluster={m.cluster}, role={role}, weight={m.weight:.2f}, half_life={hl}"

    def summary(self) -> dict[str, Any]:
        cfg = _get_cfg()
        clusters: dict[str, int] = {}
        smart = anti = 0
        for meta in self._by_addr.values():
            clusters[meta.cluster] = clusters.get(meta.cluster, 0) + 1
            if meta.weight >= 0:
                smart += 1
            else:
                anti += 1
        return {
            "total": len(self._by_addr),
            "smart_count": smart,
            "anti_count": anti,
            "clusters": clusters,
            "mode": cfg.mode,
            "phase": cfg.phase,
        }


REGISTRY = SmartWalletRegistry()


# =============================================================================
# Tracker (decayed per-mint state)
# =============================================================================


class SmartWalletTracker:
    """
    Tracks recent smart-wallet + whale + breadth activity per mint with decay.
    All state is in-memory. NO network calls.

    API:
      - observe_swap(...) / observe_swap_event(...) / bulk_observe_swap_events(...)
      - build_features(mint) -> Dict[str, float]
      - attach_features_to_meta(meta, mint)
      - entry_hint(mint) / exit_hint(mint)
      - global_regime_hint()
      - describe_mint(mint)
      - top_active_mints()
      - has_meaningful_signal()
      - clear_mint() / reset_all()
    """

    def __init__(self) -> None:
        # signed smart flow
        self._scores: dict[str, float] = {}
        self._last_ts: dict[str, float] = {}
        self._last_buy_ts: dict[str, float] = {}
        self._last_sell_ts: dict[str, float] = {}
        self._last_cluster: dict[str, str] = {}

        # smart event count (decayed)
        self._smart_event_score: dict[str, float] = {}
        self._smart_event_last_ts: dict[str, float] = {}

        # smart side counts (decayed)
        self._smart_buy_count: dict[str, float] = {}
        self._smart_sell_count: dict[str, float] = {}
        self._smart_side_last_ts: dict[str, float] = {}

        # smart notional volume (decayed)
        self._smart_volume: dict[str, float] = {}
        self._smart_volume_last_ts: dict[str, float] = {}

        # whales count + volume (decayed)
        self._whale_count: dict[str, float] = {}
        self._whale_volume: dict[str, float] = {}
        self._whale_last_ts: dict[str, float] = {}

        # whale side counts (decayed)
        self._whale_buy_count: dict[str, float] = {}
        self._whale_sell_count: dict[str, float] = {}
        self._whale_side_last_ts: dict[str, float] = {}

        # breadth uniques (all)
        self._addr_last_seen: dict[str, dict[str, float]] = {}
        self._unique_score: dict[str, float] = {}
        self._unique_last_ts: dict[str, float] = {}

        # breadth uniques (smart subset)
        self._smart_addr_last_seen: dict[str, dict[str, float]] = {}
        self._smart_unique_score: dict[str, float] = {}
        self._smart_unique_last_ts: dict[str, float] = {}

        # housekeeping
        self._last_any_activity_ts: dict[str, float] = {}
        self._op_counter: int = 0

        # per-mint feature cache: mint -> (ts, feats)
        self._feature_cache: dict[str, tuple[float, dict[str, float]]] = {}

        self._lock = asyncio.Lock()

    # ------------------------------------------------------------------
    # Maintenance
    # ------------------------------------------------------------------

    async def clear_mint(self, mint: str) -> None:
        mint = (mint or "").strip()
        if not mint:
            return
        async with self._lock:
            for d in (
                self._scores,
                self._last_ts,
                self._last_buy_ts,
                self._last_sell_ts,
                self._last_cluster,
                self._smart_event_score,
                self._smart_event_last_ts,
                self._smart_buy_count,
                self._smart_sell_count,
                self._smart_side_last_ts,
                self._smart_volume,
                self._smart_volume_last_ts,
                self._whale_count,
                self._whale_volume,
                self._whale_last_ts,
                self._whale_buy_count,
                self._whale_sell_count,
                self._whale_side_last_ts,
                self._addr_last_seen,
                self._unique_score,
                self._unique_last_ts,
                self._smart_addr_last_seen,
                self._smart_unique_score,
                self._smart_unique_last_ts,
                self._last_any_activity_ts,
                self._feature_cache,
            ):
                d.pop(mint, None)

    async def reset_all(self) -> None:
        async with self._lock:
            self._scores.clear()
            self._last_ts.clear()
            self._last_buy_ts.clear()
            self._last_sell_ts.clear()
            self._last_cluster.clear()
            self._smart_event_score.clear()
            self._smart_event_last_ts.clear()
            self._smart_buy_count.clear()
            self._smart_sell_count.clear()
            self._smart_side_last_ts.clear()
            self._smart_volume.clear()
            self._smart_volume_last_ts.clear()
            self._whale_count.clear()
            self._whale_volume.clear()
            self._whale_last_ts.clear()
            self._whale_buy_count.clear()
            self._whale_sell_count.clear()
            self._whale_side_last_ts.clear()
            self._addr_last_seen.clear()
            self._unique_score.clear()
            self._unique_last_ts.clear()
            self._smart_addr_last_seen.clear()
            self._smart_unique_score.clear()
            self._smart_unique_last_ts.clear()
            self._last_any_activity_ts.clear()
            self._feature_cache.clear()
            self._op_counter = 0

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    @staticmethod
    def _normalize_notional(amount_usd: float | None, amount_sol: float | None) -> float:
        """
        Normalize trade size into rough USD notional.
        If only SOL is provided, uses crude 100 USD/SOL proxy (good enough for bucketing).
        """
        try:
            if amount_usd is not None:
                v = float(amount_usd)
                return 0.0 if v < 0 else v
            if amount_sol is not None:
                v = float(amount_sol) * 100.0
                return 0.0 if v < 0 else v
        except Exception:
            return 0.0
        return 0.0

    @staticmethod
    def _is_buy(side: str) -> bool:
        s = (side or "").strip().lower()
        return s.startswith("b") or s in ("in", "long", "buy")

    @staticmethod
    def _is_sell(side: str) -> bool:
        s = (side or "").strip().lower()
        return s.startswith("s") or s in ("out", "short", "sell")

    @staticmethod
    def _smart_contribution_from_notional(amt_usd: float, cfg: _Cfg) -> float:
        """
        Map notional -> contribution in [0, smart_max_contrib_per_trade].
        Small trades are tapered so noise doesn't dominate.
        """
        if amt_usd <= 0.0:
            return 0.0

        if cfg.smart_min_notional_usd > 0 and amt_usd < cfg.smart_min_notional_usd:
            return 0.1 * (amt_usd / max(cfg.smart_min_notional_usd, 1.0))

        # roughly: $5k = 1.0 contrib (then capped)
        val = amt_usd / 5_000.0
        return float(_clamp(val, 0.0, max(cfg.smart_max_contrib_per_trade, 0.0)))

    @staticmethod
    def _is_whale(amt_usd: float, amt_sol: float | None, cfg: _Cfg) -> bool:
        if amt_usd >= cfg.whale_min_notional_usd:
            return True
        if amt_sol is not None:
            try:
                if float(amt_sol) >= cfg.whale_min_notional_sol:
                    return True
            except Exception:
                pass
        return False

    def _touch_mint_locked(self, mint: str, now: float) -> None:
        self._last_any_activity_ts[mint] = now
        self._op_counter += 1
        self._feature_cache.pop(mint, None)  # invalidate cache on mutation

    def _prune_addr_map_locked(self, addr_map: dict[str, float], *, now: float, cfg: _Cfg) -> None:
        if not addr_map:
            return

        older_than = now - cfg.addr_prune_older_sec
        if older_than > 0 and len(addr_map) > max(200, cfg.max_addrs_per_mint):
            for k in [a for a, ts in addr_map.items() if ts < older_than]:
                addr_map.pop(k, None)

        if cfg.max_addrs_per_mint > 0 and len(addr_map) > cfg.max_addrs_per_mint:
            items = sorted(addr_map.items(), key=lambda kv: kv[1])  # oldest first
            drop_n = len(items) - cfg.max_addrs_per_mint
            for i in range(max(0, drop_n)):
                addr_map.pop(items[i][0], None)

    def _maybe_prune_locked(self, now: float, cfg: _Cfg) -> None:
        if cfg.max_mints <= 0:
            return
        if self._op_counter % 500 != 0:
            return

        mints = list(self._last_any_activity_ts.keys())
        if len(mints) <= cfg.max_mints:
            return

        idle_threshold = now - cfg.idle_prune_sec
        stale = [m for m in mints if self._last_any_activity_ts.get(m, 0.0) < idle_threshold]

        if not stale:
            mints_sorted = sorted(mints, key=lambda m: self._last_any_activity_ts.get(m, 0.0))
            stale = mints_sorted[: max(0, len(mints_sorted) - cfg.max_mints)]

        for mint in stale:
            for d in (
                self._scores,
                self._last_ts,
                self._last_buy_ts,
                self._last_sell_ts,
                self._last_cluster,
                self._smart_event_score,
                self._smart_event_last_ts,
                self._smart_buy_count,
                self._smart_sell_count,
                self._smart_side_last_ts,
                self._smart_volume,
                self._smart_volume_last_ts,
                self._whale_count,
                self._whale_volume,
                self._whale_last_ts,
                self._whale_buy_count,
                self._whale_sell_count,
                self._whale_side_last_ts,
                self._addr_last_seen,
                self._unique_score,
                self._unique_last_ts,
                self._smart_addr_last_seen,
                self._smart_unique_score,
                self._smart_unique_last_ts,
                self._last_any_activity_ts,
                self._feature_cache,
            ):
                d.pop(mint, None)

        if hasattr(log, "event"):
            with contextlib.suppress(Exception):
                log.event(
                    "SMART_PRUNE_MINTS",
                    pruned=len(stale),
                    remaining=len(self._last_any_activity_ts),
                )
        else:
            log.info(
                "SMART_PRUNE_MINTS pruned=%d remaining=%d",
                len(stale),
                len(self._last_any_activity_ts),
            )

    def _get_decayed_smart_score_locked(self, mint: str, now: float, cfg: _Cfg) -> float:
        old = self._scores.get(mint, 0.0)
        last_ts = self._last_ts.get(mint, now)
        new = _decay_update(old, last_ts, now, cfg.smart_half_life_sec, increment=0.0)
        self._scores[mint] = new
        self._last_ts[mint] = now
        return float(new)

    # ------------------------------------------------------------------
    # Ingest (observe)
    # ------------------------------------------------------------------

    async def observe_swap(
        self,
        *,
        mint: str,
        trader: str,
        amount_usd: float | None = None,
        amount_sol: float | None = None,
        side: str = "buy",
    ) -> None:
        """
        Ingest a single swap event (best effort, NO I/O).
        """
        mint = (mint or "").strip()
        trader = (trader or "").strip()
        if not mint or not trader:
            return

        cfg = _get_cfg()
        if cfg.mode == "off" or (
            not cfg.enabled and not cfg.whale_enabled and not cfg.breadth_enabled
        ):
            return

        now = time.time()
        is_buy = self._is_buy(side)
        is_sell = self._is_sell(side)

        amt_usd = self._normalize_notional(amount_usd, amount_sol)
        is_whale = self._is_whale(amt_usd, amount_sol, cfg)

        meta = REGISTRY.get_meta(trader) if cfg.enabled else None

        async with self._lock:
            self._touch_mint_locked(mint, now)

            # ---------------------- SMART FLOW ----------------------
            if cfg.enabled and meta is not None:
                meta_for_use: SmartWalletMeta | None = meta
                if cfg.alpha_only:
                    cl = (meta.cluster or "").lower()
                    if (
                        ("alpha" not in cl)
                        and ("smart" not in cl)
                        and (cl not in cfg.alpha_clusters)
                    ):
                        meta_for_use = None

                if meta_for_use is not None:
                    contrib_norm = self._smart_contribution_from_notional(amt_usd, cfg)
                    if contrib_norm > 0.0:
                        old_score = self._scores.get(mint, 0.0)
                        last_ts = self._last_ts.get(mint, now)

                        half_life = meta_for_use.half_life_override_sec or cfg.smart_half_life_sec
                        score_decayed = _decay_update(
                            old_score, last_ts, now, half_life, increment=0.0
                        )

                        base_w = _clamp(float(meta_for_use.weight), -2.0, 2.0)

                        # direction: wallet weight + side
                        if base_w >= 0:
                            direction = (
                                1.0
                                if (is_buy or not (is_buy or is_sell))
                                else -cfg.smart_sell_multiplier
                            )
                            signed_w = base_w
                        else:
                            direction = (
                                -1.0
                                if (is_buy or not (is_buy or is_sell))
                                else cfg.smart_sell_multiplier
                            )
                            signed_w = abs(base_w)

                        contrib = contrib_norm * direction * signed_w
                        new_score = _clamp(
                            score_decayed + contrib,
                            -cfg.smart_max_abs_score,
                            cfg.smart_max_abs_score,
                        )

                        self._scores[mint] = new_score
                        self._last_ts[mint] = now
                        self._last_cluster[mint] = meta_for_use.cluster or ""
                        if is_buy or not (is_buy or is_sell):
                            self._last_buy_ts[mint] = now
                        elif is_sell:
                            self._last_sell_ts[mint] = now

                        # smart event count (decayed)
                        ev_old = self._smart_event_score.get(mint, 0.0)
                        ev_last = self._smart_event_last_ts.get(mint, now)
                        self._smart_event_score[mint] = _decay_update(
                            ev_old, ev_last, now, cfg.smart_half_life_sec, increment=1.0
                        )
                        self._smart_event_last_ts[mint] = now

                        # smart side counts (decayed)
                        sb_old = self._smart_buy_count.get(mint, 0.0)
                        ss_old = self._smart_sell_count.get(mint, 0.0)
                        side_last = self._smart_side_last_ts.get(mint, now)

                        sb_inc = 1.0 if (is_buy or not (is_buy or is_sell)) else 0.0
                        ss_inc = 1.0 if is_sell else 0.0

                        self._smart_buy_count[mint] = _decay_update(
                            sb_old, side_last, now, cfg.smart_half_life_sec, increment=sb_inc
                        )
                        self._smart_sell_count[mint] = _decay_update(
                            ss_old, side_last, now, cfg.smart_half_life_sec, increment=ss_inc
                        )
                        self._smart_side_last_ts[mint] = now

                        # smart volume (decayed)
                        v_old = self._smart_volume.get(mint, 0.0)
                        v_last = self._smart_volume_last_ts.get(mint, now)
                        self._smart_volume[mint] = _decay_update(
                            v_old,
                            v_last,
                            now,
                            cfg.smart_volume_half_life_sec,
                            increment=max(0.0, amt_usd),
                        )
                        self._smart_volume_last_ts[mint] = now

                        # smart unique buyers subset
                        addr_map = self._smart_addr_last_seen.setdefault(mint, {})
                        last_seen = addr_map.get(trader, 0.0)
                        addr_map[trader] = now
                        is_new = (now - last_seen) >= cfg.unique_addr_min_gap_sec

                        su_old = self._smart_unique_score.get(mint, 0.0)
                        su_last = self._smart_unique_last_ts.get(mint, now)
                        self._smart_unique_score[mint] = _decay_update(
                            su_old,
                            su_last,
                            now,
                            cfg.unique_half_life_sec,
                            increment=1.0 if is_new else 0.0,
                        )
                        self._smart_unique_last_ts[mint] = now

                        if cfg.max_addrs_per_mint > 0:
                            self._prune_addr_map_locked(addr_map, now=now, cfg=cfg)

                        if cfg.debug_log and hasattr(log, "event"):
                            with contextlib.suppress(Exception):
                                log.event(
                                    "SMART_SWAP_OBSERVED",
                                    mint=mint,
                                    trader=trader,
                                    cluster=meta_for_use.cluster,
                                    weight=meta_for_use.weight,
                                    amt_usd=amt_usd,
                                    side=str(side),
                                    contrib=contrib,
                                    score=new_score,
                                )

            # ---------------------- WHALES ----------------------
            if cfg.whale_enabled and is_whale:
                wc_old = self._whale_count.get(mint, 0.0)
                wv_old = self._whale_volume.get(mint, 0.0)
                wc_last = self._whale_last_ts.get(mint, now)

                self._whale_count[mint] = _decay_update(
                    wc_old, wc_last, now, cfg.whale_half_life_sec, increment=1.0
                )
                self._whale_volume[mint] = _decay_update(
                    wv_old, wc_last, now, cfg.whale_half_life_sec, increment=max(0.0, amt_usd)
                )
                self._whale_last_ts[mint] = now

                wb_old = self._whale_buy_count.get(mint, 0.0)
                ws_old = self._whale_sell_count.get(mint, 0.0)
                side_last = self._whale_side_last_ts.get(mint, now)

                wb_inc = 1.0 if (is_buy or not (is_buy or is_sell)) else 0.0
                ws_inc = 1.0 if is_sell else 0.0

                self._whale_buy_count[mint] = _decay_update(
                    wb_old, side_last, now, cfg.whale_half_life_sec, increment=wb_inc
                )
                self._whale_sell_count[mint] = _decay_update(
                    ws_old, side_last, now, cfg.whale_half_life_sec, increment=ws_inc
                )
                self._whale_side_last_ts[mint] = now

            # ---------------------- BREADTH ----------------------
            if cfg.breadth_enabled:
                addr_map_all = self._addr_last_seen.setdefault(mint, {})
                last_seen_all = addr_map_all.get(trader, 0.0)
                addr_map_all[trader] = now
                is_new = (now - last_seen_all) >= cfg.unique_addr_min_gap_sec

                u_old = self._unique_score.get(mint, 0.0)
                u_last = self._unique_last_ts.get(mint, now)
                self._unique_score[mint] = _decay_update(
                    u_old, u_last, now, cfg.unique_half_life_sec, increment=1.0 if is_new else 0.0
                )
                self._unique_last_ts[mint] = now

                if cfg.max_addrs_per_mint > 0:
                    self._prune_addr_map_locked(addr_map_all, now=now, cfg=cfg)

            self._maybe_prune_locked(now, cfg)

    async def observe_swap_event(self, ev: dict[str, Any]) -> None:
        """
        Generic adapter for scanner/trader events.

        Best-effort keys:
          mint: mint / mintAddress / mint_address / tokenMint
          trader: trader / owner / wallet / user / buyer
          notional USD: amount_usd / size_usd / notional_usd
          notional SOL: amount_sol / size_sol / notional_sol
          side: side / direction ("buy"/"sell")
        """
        mint = (
            ev.get("mint") or ev.get("mintAddress") or ev.get("mint_address") or ev.get("tokenMint")
        )
        trader = (
            ev.get("trader")
            or ev.get("owner")
            or ev.get("wallet")
            or ev.get("user")
            or ev.get("buyer")
        )
        if not mint or not trader:
            return

        # use None-preserving reads (avoid treating 0.0 as missing)
        amt_usd = ev.get("amount_usd")
        if amt_usd is None:
            amt_usd = ev.get("size_usd")
        if amt_usd is None:
            amt_usd = ev.get("notional_usd")

        amt_sol = ev.get("amount_sol")
        if amt_sol is None:
            amt_sol = ev.get("size_sol")
        if amt_sol is None:
            amt_sol = ev.get("notional_sol")

        side = ev.get("side") or ev.get("direction") or "buy"

        try:
            amt_usd_f = float(amt_usd) if amt_usd is not None else None
        except Exception:
            amt_usd_f = None
        try:
            amt_sol_f = float(amt_sol) if amt_sol is not None else None
        except Exception:
            amt_sol_f = None

        await self.observe_swap(
            mint=str(mint),
            trader=str(trader),
            amount_usd=amt_usd_f,
            amount_sol=amt_sol_f,
            side=str(side),
        )

    async def bulk_observe_swap_events(self, events: list[dict[str, Any]]) -> None:
        for ev in events:
            try:
                await self.observe_swap_event(ev)
            except Exception:
                if _get_cfg().debug_log:
                    log.exception("SMART_BULK_EVENT_FAIL ev=%s", ev)

    # ------------------------------------------------------------------
    # Features
    # ------------------------------------------------------------------

    async def _maybe_cached_features(
        self, mint: str, now: float, cfg: _Cfg
    ) -> dict[str, float] | None:
        if cfg.feature_cache_sec <= 0.0:
            return None
        async with self._lock:
            item = self._feature_cache.get(mint)
        if not item:
            return None
        ts, feats = item
        if (now - ts) > cfg.feature_cache_sec:
            return None
        return dict(feats)

    async def _store_cached_features(
        self, mint: str, feats: dict[str, float], now: float, cfg: _Cfg
    ) -> None:
        if cfg.feature_cache_sec <= 0.0:
            return
        async with self._lock:
            self._feature_cache[mint] = (now, dict(feats))

    async def get_raw_score(self, mint: str) -> float:
        cfg = _get_cfg()
        if not cfg.enabled or cfg.mode == "off":
            return 0.0
        mint = (mint or "").strip()
        if not mint:
            return 0.0
        now = time.time()
        async with self._lock:
            return self._get_decayed_smart_score_locked(mint, now, cfg)

    async def get_score_01(self, mint: str) -> float:
        cfg = _get_cfg()
        if not cfg.enabled or cfg.mode == "off":
            return 0.5
        raw = await self.get_raw_score(mint)
        x = _clamp(raw / max(cfg.smart_max_abs_score, 1e-6), -1.0, 1.0)
        return float(0.5 * (x + 1.0))

    async def build_features(self, mint: str) -> dict[str, float]:
        """
        Build wallet_signals for a given mint.

        IMPORTANT: Keys returned here are relied upon by other modules.
        This method remains stable and deterministic.
        """
        cfg = _get_cfg()
        mint = (mint or "").strip()
        now = time.time()

        cached = await self._maybe_cached_features(mint, now, cfg)
        if cached is not None:
            return cached

        # Neutral defaults if off / empty
        if (
            not mint
            or cfg.mode == "off"
            or (not cfg.enabled and not cfg.whale_enabled and not cfg.breadth_enabled)
        ):
            feats: dict[str, float] = {
                "smart_wallet_score": 0.5,
                "smart_flow_raw": 0.0,
                "smart_last_buy_sec_ago": 1e9,
                "smart_last_sell_sec_ago": 1e9,
                "smart_last_cluster_is_alpha": 0.0,
                "smart_wallet_count_5m": 0.0,
                "smart_buy_score_5m": 0.0,
                "smart_sell_score_5m": 0.0,
                "smart_net_side_5m": 0.0,
                "smart_vol_5m_usd": 0.0,
                "smart_avg_trade_usd": 0.0,
                "smart_spoofiness": 0.0,
                "whales_5m": 0.0,
                "whale_vol_5m_usd": 0.0,
                "whale_buy_score": 0.0,
                "whale_sell_score": 0.0,
                "whale_net_side_5m": 0.0,
                "unique_buyers_5m": 0.0,
                "smart_unique_buyers_5m": 0.0,
                "whale_share_5m": 0.0,
                "holders_growth_5m": 0.0,
                "smart_alpha_bias": 0.0,
                "smart_whale_alignment": 0.0,
                "smart_whale_alignment_abs": 0.0,
                "whale_domination_5m": 0.0,
                "smart_vs_whale_conflict": 0.0,
                "smart_accumulation_score": 0.0,
                "smart_panic_dump_score": 0.0,
                "whale_liquidity_footprint": 0.0,
                "smart_signal_confidence": 0.0,
                "smart_signal_quality": 0.0,
                "smart_risk_flag": 0.0,
                "smart_recent_side": 0.0,
                "whale_recent_side": 0.0,
                "smart_crowding_score": 0.0,
                "smart_signal_state": 0.0,
                "smart_heat_score": 0.0,
                "whale_heat_score": 0.0,
                "smart_whale_pressure_ratio": 0.0,
                "smart_entry_boost_hint": 0.0,
                "smart_exit_pressure_hint": 0.0,
                "smart_regime_accumulation": 0.0,
                "smart_regime_panic": 0.0,
                "smart_regime_conflict": 0.0,
                "smart_regime_chop": 1.0,
                "smart_whale_regime_code": 0.0,
                "smart_tradeable_flag": 0.0,
            }
            await self._store_cached_features(mint, feats, now, cfg)
            return feats

        # Snapshot internal state under lock
        async with self._lock:
            if cfg.enabled:
                raw_smart = self._get_decayed_smart_score_locked(mint, now, cfg)
                smart_01 = 0.5 * (
                    _clamp(raw_smart / max(cfg.smart_max_abs_score, 1e-6), -1.0, 1.0) + 1.0
                )
            else:
                raw_smart = 0.0
                smart_01 = 0.5

            last_buy_ts = self._last_buy_ts.get(mint, 0.0)
            last_sell_ts = self._last_sell_ts.get(mint, 0.0)
            last_cluster = self._last_cluster.get(mint, "")

            ev_score = self._smart_event_score.get(mint, 0.0)
            ev_last_ts = self._smart_event_last_ts.get(mint, now)

            sb = self._smart_buy_count.get(mint, 0.0)
            ss = self._smart_sell_count.get(mint, 0.0)
            smart_side_last_ts = self._smart_side_last_ts.get(mint, now)

            sv = self._smart_volume.get(mint, 0.0)
            sv_last_ts = self._smart_volume_last_ts.get(mint, now)

            wc = self._whale_count.get(mint, 0.0)
            wv = self._whale_volume.get(mint, 0.0)
            wc_last_ts = self._whale_last_ts.get(mint, now)

            wb = self._whale_buy_count.get(mint, 0.0)
            ws = self._whale_sell_count.get(mint, 0.0)
            whale_side_last_ts = self._whale_side_last_ts.get(mint, now)

            u_score = self._unique_score.get(mint, 0.0)
            u_last_ts = self._unique_last_ts.get(mint, now)

            su_score = self._smart_unique_score.get(mint, 0.0)
            su_last_ts = self._smart_unique_last_ts.get(mint, now)

        buy_ago = float(now - last_buy_ts) if last_buy_ts > 0 else 1e9
        sell_ago = float(now - last_sell_ts) if last_sell_ts > 0 else 1e9

        smart_wallet_count_5m = ev_score * _decay_factor(
            max(0.0, now - ev_last_ts), cfg.smart_half_life_sec
        )

        decay_s = _decay_factor(max(0.0, now - smart_side_last_ts), cfg.smart_half_life_sec)
        sb_dec = sb * decay_s
        ss_dec = ss * decay_s
        smart_buy_score_5m = _to_01_from_count(sb_dec, cfg.smart_event_scale)
        smart_sell_score_5m = _to_01_from_count(ss_dec, cfg.smart_event_scale)
        smart_net_side_5m = _net_minus_one_to_one(sb_dec, ss_dec)

        decay_sv = _decay_factor(max(0.0, now - sv_last_ts), cfg.smart_volume_half_life_sec)
        smart_vol_5m_usd = sv * decay_sv
        smart_avg_trade_usd = (
            0.0
            if smart_wallet_count_5m <= 0
            else (smart_vol_5m_usd / max(smart_wallet_count_5m, 1.0))
        )

        decay_w = _decay_factor(max(0.0, now - wc_last_ts), cfg.whale_half_life_sec)
        whales_5m = wc * decay_w
        whale_vol_5m_usd = wv * decay_w

        decay_ws = _decay_factor(max(0.0, now - whale_side_last_ts), cfg.whale_half_life_sec)
        wb_dec = wb * decay_ws
        ws_dec = ws * decay_ws
        whale_buy_score = _to_01_from_count(wb_dec, cfg.whale_side_scale)
        whale_sell_score = _to_01_from_count(ws_dec, cfg.whale_side_scale)
        whale_net_side_5m = _net_minus_one_to_one(wb_dec, ws_dec)

        decay_u = _decay_factor(max(0.0, now - u_last_ts), cfg.unique_half_life_sec)
        unique_buyers_5m = u_score * decay_u

        decay_su = _decay_factor(max(0.0, now - su_last_ts), cfg.unique_half_life_sec)
        smart_unique_buyers_5m = su_score * decay_su

        whale_share_5m = (
            0.0
            if unique_buyers_5m <= 0
            else _clamp(whales_5m / max(unique_buyers_5m, 1.0), 0.0, 1.0)
        )
        holders_growth_5m = min(unique_buyers_5m / max(cfg.unique_buyers_scale, 1.0), 0.5)

        cluster_lower = (last_cluster or "").lower()
        is_alpha_cluster = (
            ("alpha" in cluster_lower)
            or ("smart" in cluster_lower)
            or (cluster_lower in cfg.alpha_clusters)
        )
        smart_last_cluster_is_alpha = 1.0 if is_alpha_cluster else 0.0

        smart_alpha_bias = 0.0
        if smart_wallet_count_5m > 0 and is_alpha_cluster:
            smart_alpha_bias = min(1.0, smart_wallet_count_5m / max(cfg.smart_event_scale, 1.0))

        smart_whale_alignment = smart_net_side_5m * whale_net_side_5m
        smart_whale_alignment_abs = abs(smart_whale_alignment)

        if whale_share_5m <= cfg.whale_dom_threshold:
            whale_domination_5m = 0.0
        else:
            denom = max(1e-6, 1.0 - cfg.whale_dom_threshold)
            whale_domination_5m = _clamp(
                (whale_share_5m - cfg.whale_dom_threshold) / denom, 0.0, 1.0
            )

        smart_vs_whale_conflict = 0.0
        if (
            abs(smart_net_side_5m) >= cfg.conflict_net_min
            and abs(whale_net_side_5m) >= cfg.conflict_net_min
            and (smart_net_side_5m * whale_net_side_5m) < 0
        ):
            smart_vs_whale_conflict = min(1.0, abs(smart_net_side_5m - whale_net_side_5m) / 2.0)

        accum_raw = max(0.0, smart_net_side_5m) * max(0.0, whale_net_side_5m)
        if smart_alpha_bias > 0:
            accum_raw *= 1.0 + 0.5 * smart_alpha_bias
        smart_accumulation_score = _clamp(accum_raw * cfg.accum_alpha_scale, 0.0, 1.0)

        dump_raw = max(0.0, -smart_net_side_5m) * max(0.0, -whale_net_side_5m)
        dump_raw *= 1.0 + whale_domination_5m
        smart_panic_dump_score = _clamp(dump_raw * cfg.panic_dump_scale, 0.0, 1.0)

        whale_liquidity_footprint = _clamp(
            whale_vol_5m_usd / cfg.whale_footprint_anchor_usd, 0.0, 1.0
        )

        smart_spoofiness = 0.0
        if smart_wallet_count_5m >= 2.0:
            avg = smart_avg_trade_usd
            smart_spoofiness = _clamp(
                1.0 - (avg / max(cfg.smart_spoof_avg_notional_usd, 1e-6)), 0.0, 1.0
            )

        last_active_ts = max(last_buy_ts, last_sell_ts)
        if last_active_ts <= 0:
            recency_factor = 0.0
        else:
            age = now - last_active_ts
            recency_factor = (
                0.0
                if age >= cfg.signal_recency_sec
                else max(0.0, 1.0 - age / cfg.signal_recency_sec)
            )

        volume_factor = _to_01_from_count(
            smart_wallet_count_5m, max(cfg.signal_min_events_for_conf, 1e-6)
        )
        smart_signal_confidence = _clamp(0.6 * recency_factor + 0.4 * volume_factor, 0.0, 1.0)

        quality_raw = 0.0
        quality_raw += 0.40 * smart_alpha_bias
        quality_raw += 0.25 * smart_whale_alignment_abs
        quality_raw += 0.20 * (1.0 - smart_vs_whale_conflict)
        quality_raw += 0.15 * (1.0 - smart_spoofiness)
        quality_raw -= 0.15 * whale_domination_5m
        smart_signal_quality = _clamp(quality_raw, 0.0, 1.0)

        risk_raw = 0.0
        risk_raw += 0.65 * smart_vs_whale_conflict
        risk_raw += 0.45 * whale_domination_5m
        risk_raw += 0.35 * smart_spoofiness
        risk_raw *= min(1.0, abs(smart_net_side_5m) + abs(whale_net_side_5m))
        smart_risk_flag = _clamp(risk_raw, 0.0, 1.0)

        smart_recent_side = (
            1.0 if smart_net_side_5m > 0.15 else (-1.0 if smart_net_side_5m < -0.15 else 0.0)
        )
        whale_recent_side = (
            1.0 if whale_net_side_5m > 0.15 else (-1.0 if whale_net_side_5m < -0.15 else 0.0)
        )

        smart_crowding_score = (
            0.0
            if unique_buyers_5m <= 0
            else _clamp(smart_unique_buyers_5m / max(unique_buyers_5m, 1.0), 0.0, 1.0)
        )
        smart_signal_state = smart_net_side_5m * smart_signal_quality * smart_signal_confidence

        smart_heat_score = _clamp(
            abs(smart_net_side_5m) * smart_signal_confidence * (0.5 + 0.5 * smart_signal_quality),
            0.0,
            1.0,
        )
        whale_heat_score = _clamp(
            abs(whale_net_side_5m)
            * min(1.0, whales_5m / max(1.0, cfg.smart_event_scale))
            * (0.5 + 0.5 * whale_liquidity_footprint),
            0.0,
            1.0,
        )

        if smart_heat_score < cfg.min_score_for_rich_features:
            smart_vs_whale_conflict *= 0.5
            whale_domination_5m *= 0.5
            smart_risk_flag *= 0.5
            smart_spoofiness *= 0.5

        total_heat = smart_heat_score + whale_heat_score
        smart_whale_pressure_ratio = (
            0.0 if total_heat <= 0 else min(1.0, whale_heat_score / max(total_heat, 1e-6))
        )

        smart_entry_boost_hint = smart_signal_state  # [-1..+1]
        smart_exit_pressure_hint = _clamp(
            smart_panic_dump_score
            * (0.5 + 0.5 * whale_domination_5m)
            * (0.5 + 0.5 * smart_risk_flag),
            0.0,
            1.0,
        )

        regime_accum = 1.0 if smart_accumulation_score >= 0.5 else 0.0
        regime_panic = 1.0 if smart_panic_dump_score >= 0.5 else 0.0
        regime_conflict = 1.0 if smart_vs_whale_conflict >= 0.5 else 0.0

        if (
            smart_heat_score < max(cfg.tradeable_min_heat, 0.12)
            and whale_heat_score < max(cfg.tradeable_min_heat, 0.12)
            and regime_accum == 0.0
            and regime_panic == 0.0
        ):
            regime_chop = 1.0
        else:
            regime_chop = 0.0

        if regime_accum >= 0.5 and regime_panic < 0.5:
            smart_whale_regime_code = 1.0
        elif regime_panic >= 0.5 and regime_accum < 0.5:
            smart_whale_regime_code = 2.0
        elif regime_conflict >= 0.5:
            smart_whale_regime_code = 3.0
        elif regime_chop >= 0.5:
            smart_whale_regime_code = 0.0
        else:
            smart_whale_regime_code = 4.0

        is_tradeable = (
            smart_signal_confidence >= cfg.tradeable_min_conf
            and smart_signal_quality >= cfg.tradeable_min_quality
            and smart_heat_score >= cfg.tradeable_min_heat
            and smart_risk_flag <= cfg.tradeable_max_risk
        )
        smart_tradeable_flag = 1.0 if is_tradeable else 0.0

        feats = {
            "smart_wallet_score": float(smart_01),
            "smart_flow_raw": float(raw_smart),
            "smart_last_buy_sec_ago": float(buy_ago),
            "smart_last_sell_sec_ago": float(sell_ago),
            "smart_last_cluster_is_alpha": float(smart_last_cluster_is_alpha),
            "smart_wallet_count_5m": float(smart_wallet_count_5m),
            "smart_buy_score_5m": float(smart_buy_score_5m),
            "smart_sell_score_5m": float(smart_sell_score_5m),
            "smart_net_side_5m": float(smart_net_side_5m),
            "smart_unique_buyers_5m": float(smart_unique_buyers_5m),
            "smart_vol_5m_usd": float(smart_vol_5m_usd),
            "smart_avg_trade_usd": float(smart_avg_trade_usd),
            "smart_spoofiness": float(smart_spoofiness),
            "whales_5m": float(whales_5m),
            "whale_vol_5m_usd": float(whale_vol_5m_usd),
            "whale_buy_score": float(whale_buy_score),
            "whale_sell_score": float(whale_sell_score),
            "whale_net_side_5m": float(whale_net_side_5m),
            "unique_buyers_5m": float(unique_buyers_5m),
            "whale_share_5m": float(whale_share_5m),
            "holders_growth_5m": float(holders_growth_5m),
            "smart_alpha_bias": float(smart_alpha_bias),
            "smart_whale_alignment": float(smart_whale_alignment),
            "smart_whale_alignment_abs": float(smart_whale_alignment_abs),
            "whale_domination_5m": float(whale_domination_5m),
            "smart_vs_whale_conflict": float(smart_vs_whale_conflict),
            "smart_accumulation_score": float(smart_accumulation_score),
            "smart_panic_dump_score": float(smart_panic_dump_score),
            "whale_liquidity_footprint": float(whale_liquidity_footprint),
            "smart_signal_confidence": float(smart_signal_confidence),
            "smart_signal_quality": float(smart_signal_quality),
            "smart_risk_flag": float(smart_risk_flag),
            "smart_recent_side": float(smart_recent_side),
            "whale_recent_side": float(whale_recent_side),
            "smart_crowding_score": float(smart_crowding_score),
            "smart_signal_state": float(smart_signal_state),
            "smart_heat_score": float(smart_heat_score),
            "whale_heat_score": float(whale_heat_score),
            "smart_whale_pressure_ratio": float(smart_whale_pressure_ratio),
            "smart_entry_boost_hint": float(smart_entry_boost_hint),
            "smart_exit_pressure_hint": float(smart_exit_pressure_hint),
            "smart_regime_accumulation": float(regime_accum),
            "smart_regime_panic": float(regime_panic),
            "smart_regime_conflict": float(regime_conflict),
            "smart_regime_chop": float(regime_chop),
            "smart_whale_regime_code": float(smart_whale_regime_code),
            "smart_tradeable_flag": float(smart_tradeable_flag),
        }

        if cfg.debug_log:
            log.debug(
                "SMART_FEATURES mint=%s feats=%s", mint, {k: round(v, 3) for k, v in feats.items()}
            )

        await self._store_cached_features(mint, feats, now, cfg)
        return feats

    async def attach_features_to_meta(self, meta: dict[str, Any], mint: str) -> None:
        feats = await self.build_features(mint)
        with contextlib.suppress(Exception):
            meta["wallet_signals"] = dict(feats)
        meta.update(feats)

    async def snapshot(self, mint: str) -> dict[str, Any]:
        feats = await self.build_features(mint)
        keys = [
            "smart_wallet_score",
            "smart_flow_raw",
            "smart_wallet_count_5m",
            "smart_recent_side",
            "whale_recent_side",
            "whale_share_5m",
            "smart_heat_score",
            "whale_heat_score",
            "smart_signal_confidence",
            "smart_signal_quality",
            "smart_risk_flag",
            "smart_exit_pressure_hint",
            "smart_tradeable_flag",
            "smart_whale_regime_code",
        ]
        return {k: float(feats.get(k, 0.0)) for k in keys}

    async def top_active_mints(
        self, *, limit: int = 20, min_heat: float = 0.0
    ) -> list[dict[str, Any]]:
        async with self._lock:
            mints = list(self._last_any_activity_ts.keys())

        results: list[dict[str, Any]] = []
        for mint in mints:
            feats = await self.build_features(mint)
            s_heat = float(feats.get("smart_heat_score", 0.0))
            w_heat = float(feats.get("whale_heat_score", 0.0))
            total = s_heat + w_heat
            if total < min_heat:
                continue
            results.append(
                {
                    "mint": mint,
                    "total_heat": total,
                    "smart_heat": s_heat,
                    "whale_heat": w_heat,
                    "smart_wallet_score": float(feats.get("smart_wallet_score", 0.5)),
                    "smart_net_side_5m": float(feats.get("smart_net_side_5m", 0.0)),
                    "whale_net_side_5m": float(feats.get("whale_net_side_5m", 0.0)),
                    "smart_exit_pressure_hint": float(feats.get("smart_exit_pressure_hint", 0.0)),
                }
            )

        results.sort(key=lambda x: x["total_heat"], reverse=True)
        return results[:limit] if limit > 0 else results

    async def global_regime_hint(
        self, *, limit: int = 50, min_heat: float = 0.1
    ) -> dict[str, float]:
        top = await self.top_active_mints(limit=limit, min_heat=min_heat)
        if not top:
            return {
                "smart_heat_global": 0.0,
                "whale_heat_global": 0.0,
                "smart_side_global": 0.0,
                "whale_side_global": 0.0,
                "smart_panic_global": 0.0,
                "smart_conflict_global": 0.0,
                "smart_risk_global": 0.0,
                "smart_crowding_global": 0.0,
                "smart_tradeable_global": 0.0,
            }

        sum_s_heat = sum_w_heat = 0.0
        sum_s_side = sum_w_side = 0.0
        sum_panic = sum_conf = sum_risk = 0.0
        sum_crowd = 0.0
        sum_tradeable = 0.0
        n = 0

        for row in top:
            mint = row["mint"]
            feats = await self.build_features(mint)

            s_heat = float(feats.get("smart_heat_score", 0.0))
            w_heat = float(feats.get("whale_heat_score", 0.0))
            s_side = float(feats.get("smart_net_side_5m", 0.0))
            w_side = float(feats.get("whale_net_side_5m", 0.0))

            panic = float(feats.get("smart_exit_pressure_hint", 0.0))
            conflict = float(feats.get("smart_vs_whale_conflict", 0.0))
            risk = float(feats.get("smart_risk_flag", 0.0))
            crowd = float(feats.get("smart_crowding_score", 0.0))
            tradeable = float(feats.get("smart_tradeable_flag", 0.0))

            sum_s_heat += s_heat
            sum_w_heat += w_heat
            sum_s_side += s_side * s_heat
            sum_w_side += w_side * w_heat
            sum_panic += panic * s_heat
            sum_conf += conflict * (s_heat + w_heat)
            sum_risk += risk * (s_heat + w_heat)
            sum_crowd += crowd * (s_heat + w_heat)
            sum_tradeable += tradeable
            n += 1

        n = max(1, n)
        smart_heat_global = _clamp(sum_s_heat / n, 0.0, 1.0)
        whale_heat_global = _clamp(sum_w_heat / n, 0.0, 1.0)

        denom_s = max(sum_s_heat, 1e-6)
        denom_w = max(sum_w_heat, 1e-6)
        smart_side_global = _clamp(sum_s_side / denom_s, -1.0, 1.0)
        whale_side_global = _clamp(sum_w_side / denom_w, -1.0, 1.0)

        smart_panic_global = _clamp(sum_panic / max(sum_s_heat, 1e-6), 0.0, 1.0)
        smart_conflict_global = _clamp(sum_conf / max(sum_s_heat + sum_w_heat, 1e-6), 0.0, 1.0)
        smart_risk_global = _clamp(sum_risk / max(sum_s_heat + sum_w_heat, 1e-6), 0.0, 1.0)
        smart_crowding_global = _clamp(sum_crowd / max(sum_s_heat + sum_w_heat, 1e-6), 0.0, 1.0)
        smart_tradeable_global = _clamp(sum_tradeable / max(float(n), 1.0), 0.0, 1.0)

        return {
            "smart_heat_global": float(smart_heat_global),
            "whale_heat_global": float(whale_heat_global),
            "smart_side_global": float(smart_side_global),
            "whale_side_global": float(whale_side_global),
            "smart_panic_global": float(smart_panic_global),
            "smart_conflict_global": float(smart_conflict_global),
            "smart_risk_global": float(smart_risk_global),
            "smart_crowding_global": float(smart_crowding_global),
            "smart_tradeable_global": float(smart_tradeable_global),
        }

    async def entry_hint(self, mint: str) -> dict[str, Any]:
        feats = await self.build_features(mint)
        return {
            "boost": float(feats.get("smart_entry_boost_hint", 0.0)),
            "confidence": float(feats.get("smart_signal_confidence", 0.0)),
            "quality": float(feats.get("smart_signal_quality", 0.0)),
            "risk_flag": float(feats.get("smart_risk_flag", 0.0)),
            "recent_side_smart": float(feats.get("smart_recent_side", 0.0)),
            "recent_side_whale": float(feats.get("whale_recent_side", 0.0)),
            "crowding": float(feats.get("smart_crowding_score", 0.0)),
            "heat_smart": float(feats.get("smart_heat_score", 0.0)),
            "heat_whale": float(feats.get("whale_heat_score", 0.0)),
            "pressure_ratio": float(feats.get("smart_whale_pressure_ratio", 0.0)),
        }

    async def exit_hint(self, mint: str) -> dict[str, Any]:
        feats = await self.build_features(mint)
        return {
            "panic_pressure": float(feats.get("smart_exit_pressure_hint", 0.0)),
            "whale_domination": float(feats.get("whale_domination_5m", 0.0)),
            "conflict": float(feats.get("smart_vs_whale_conflict", 0.0)),
            "risk_flag": float(feats.get("smart_risk_flag", 0.0)),
            "recent_side_smart": float(feats.get("smart_recent_side", 0.0)),
            "recent_side_whale": float(feats.get("whale_recent_side", 0.0)),
            "heat_smart": float(feats.get("smart_heat_score", 0.0)),
            "heat_whale": float(feats.get("whale_heat_score", 0.0)),
        }

    async def describe_mint(self, mint: str) -> str:
        feats = await self.build_features(mint)
        s_score = float(feats.get("smart_wallet_score", 0.5))
        s_side = float(feats.get("smart_net_side_5m", 0.0))
        w_side = float(feats.get("whale_net_side_5m", 0.0))
        s_heat = float(feats.get("smart_heat_score", 0.0))
        w_heat = float(feats.get("whale_heat_score", 0.0))
        panic = float(feats.get("smart_exit_pressure_hint", 0.0))
        risk = float(feats.get("smart_risk_flag", 0.0))

        def _side_label(x: float) -> str:
            if x > 0.15:
                return "buy"
            if x < -0.15:
                return "sell"
            return "flat"

        return (
            f"mint={mint} | smart={s_score:.2f} ({_side_label(s_side)}) | "
            f"whales={_side_label(w_side)} | heat S={s_heat:.2f} W={w_heat:.2f} | "
            f"panic={panic:.2f} risk={risk:.2f}"
        )

    async def has_meaningful_signal(
        self, mint: str, *, min_conf: float = 0.2, min_heat: float = 0.1
    ) -> bool:
        feats = await self.build_features(mint)
        conf = float(feats.get("smart_signal_confidence", 0.0))
        s_heat = float(feats.get("smart_heat_score", 0.0))
        w_heat = float(feats.get("whale_heat_score", 0.0))
        return conf >= min_conf and (s_heat + w_heat) >= min_heat


# Global tracker (used everywhere)
TRACKER = SmartWalletTracker()

# Backwards-compatible alias
smart_wallets = TRACKER


# =============================================================================
# Risk-ctx helper
# =============================================================================


async def build_risk_ctx_smart(*, limit: int = 50, min_heat: float = 0.1) -> dict[str, float]:
    return await TRACKER.global_regime_hint(limit=limit, min_heat=min_heat)


# =============================================================================
# Reinvestment target picker (PnL-focused)
# =============================================================================


async def pick_reinvestment_target(
    budget_sol: float | None = None,
    *,
    min_24h_volume_usd: float | None = None,
    redis: Any = None,  # signature compatibility
    meta: dict[str, Any] | None = None,
    universe: dict[str, dict[str, Any]] | None = None,
    **_: Any,
) -> dict[str, Any] | str | None:
    """
    Returns either:
      • dict: {"mint": <str>, "score": <float>, "features": {...}} (preferred)
      • str:  <mint> (legacy compatibility; enable via SMART_WALLET_REINVEST_RETURN_MINT_ONLY=1)
    """
    cfg = _get_cfg()
    if not cfg.enabled or cfg.mode == "off":
        return None

    meta = meta or {}
    profile_raw = meta.get("profile") or meta.get("bucket") or meta.get("reinvest_profile")
    profile = str(profile_raw).lower() if profile_raw is not None else ""

    if profile in ("defensive", "income", "savings"):
        risk_penalty = 1.2
        min_conf = 0.25
        min_heat = 0.12
    elif profile in ("degen", "aggressive"):
        risk_penalty = 0.7
        min_conf = 0.10
        min_heat = 0.05
    elif profile in ("explore", "experimental", "beta"):
        risk_penalty = 0.9
        min_conf = 0.15
        min_heat = 0.08
    else:
        risk_penalty = 1.0
        min_conf = 0.20
        min_heat = 0.10

    try:
        if isinstance(cfg.profile_overrides, dict) and profile:
            override = cfg.profile_overrides.get(profile)
            if isinstance(override, dict):
                if "risk_penalty" in override:
                    risk_penalty = float(override["risk_penalty"])
                if "min_conf" in override:
                    min_conf = float(override["min_conf"])
                if "min_heat" in override:
                    min_heat = float(override["min_heat"])
    except Exception:
        pass

    if budget_sol is not None:
        try:
            b = float(budget_sol)
            if b < 0.03:
                min_conf = min(1.0, min_conf + 0.05)
                min_heat = min(1.0, min_heat + 0.03)
        except Exception:
            pass

    candidates = await TRACKER.top_active_mints(limit=80, min_heat=min_heat)
    if not candidates:
        return None

    best_score = float("-inf")
    best_payload: dict[str, Any] | None = None

    for row in candidates:
        mint = row["mint"]

        if cfg.reinvest_only_mints and mint not in cfg.reinvest_only_mints:
            continue
        if mint in cfg.reinvest_exclude_mints:
            continue

        feats = await TRACKER.build_features(mint)

        boost = float(feats.get("smart_entry_boost_hint", 0.0))  # -1..+1
        conf = float(feats.get("smart_signal_confidence", 0.0))  # 0..1
        qual = float(feats.get("smart_signal_quality", 0.0))  # 0..1
        risk_flag = float(feats.get("smart_risk_flag", 0.0))  # 0..1
        panic = float(feats.get("smart_exit_pressure_hint", 0.0))  # 0..1
        conflict = float(feats.get("smart_vs_whale_conflict", 0.0))  # 0..1
        whale_dom = float(feats.get("whale_domination_5m", 0.0))  # 0..1
        s_heat = float(feats.get("smart_heat_score", 0.0))
        w_heat = float(feats.get("whale_heat_score", 0.0))
        tradeable = float(feats.get("smart_tradeable_flag", 0.0))

        if min_24h_volume_usd is not None and universe is not None:
            meta_row = universe.get(mint) or {}
            try:
                vol_24h = float(meta_row.get("volume_24h_usd", 0.0))
            except Exception:
                vol_24h = 0.0
            if vol_24h < float(min_24h_volume_usd):
                continue

        if conf < min_conf:
            continue
        if boost <= 0.0:
            continue

        if cfg.reinvest_require_tradeable and tradeable < 0.5:
            continue
        if panic > cfg.reinvest_max_panic:
            continue
        if risk_flag > cfg.reinvest_max_risk:
            continue
        if conflict > cfg.reinvest_max_conflict:
            continue

        core_alpha = boost * conf * (0.5 + 0.5 * qual)
        heat_bonus = 0.15 * s_heat + 0.05 * w_heat
        penalty = 0.8 * panic + 0.7 * conflict + 0.9 * risk_flag * risk_penalty + 0.3 * whale_dom
        score = core_alpha + heat_bonus - penalty

        if score > best_score:
            best_score = score
            best_payload = {
                "mint": mint,
                "score": float(score),
                "features": {
                    "boost": boost,
                    "confidence": conf,
                    "quality": qual,
                    "risk_flag": risk_flag,
                    "panic_pressure": panic,
                    "conflict": conflict,
                    "whale_domination": whale_dom,
                    "smart_heat": s_heat,
                    "whale_heat": w_heat,
                    "tradeable_flag": tradeable,
                },
            }

    if best_payload is None:
        return None

    if cfg.reinvest_return_mint_only:
        return str(best_payload.get("mint", "")) or None

    return best_payload


__all__ = [
    "REGISTRY",
    "TRACKER",
    "SmartWalletMeta",
    "SmartWalletRegistry",
    "SmartWalletTracker",
    "build_risk_ctx_smart",
    "pick_reinvestment_target",
    "smart_wallets",
]


if __name__ == "__main__":  # pragma: no cover

    async def _demo() -> None:
        REGISTRY.register_wallet("wallet_smart_1", cluster="ct_alpha_snipers", weight=1.5)
        await TRACKER.observe_swap(
            mint="DEMO_MINT", trader="wallet_smart_1", amount_usd=1500.0, side="buy"
        )
        await TRACKER.observe_swap(
            mint="DEMO_MINT", trader="wallet_whale_1", amount_usd=12000.0, side="buy"
        )

        feats = await TRACKER.build_features("DEMO_MINT")
        desc = await TRACKER.describe_mint("DEMO_MINT")
        print("desc:", desc)
        print("feat_count:", len(feats))
        print("sample:", {k: round(float(feats[k]), 3) for k in sorted(feats)[:12]})

        target = await pick_reinvestment_target(
            0.5,
            min_24h_volume_usd=10_000,
            universe={"DEMO_MINT": {"volume_24h_usd": 1_000_000}},
        )
        print("pick_reinvestment_target:", target)

    asyncio.run(_demo())
