# utils_pkg/smart_wallets.py
from __future__ import annotations

"""
utils_pkg/smart_wallets.py  —  V3+++ / quant-grade / God-Tier

Smart wallet & whale orderflow engine.

This module is intentionally:
  • In-memory only (no network calls)
  • Async-safe (single internal lock)
  • Cheap enough for live streaming (PumpPortal) + scanners
  • Feature-rich for AI entry/exit + regime + risk_manager + reinvestor

Public API:
  - SmartWalletMeta
  - SmartWalletRegistry
  - SmartWalletTracker
  - REGISTRY
  - TRACKER (alias: smart_wallets)
  - build_risk_ctx_smart()
  - pick_reinvestment_target()
  - get_wallet_state()           <-- used by scripts/main.py daily pnl notes
  - smart_wallets_loop(stop_evt) <-- used by scripts/main.py bg tasks
"""

import asyncio
import json
import os
import random
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

from logging_setup import get_logger

log = get_logger("sniper.smart_wallets")

# -----------------------------------------------------------------------------
# logging shim: ensure log.every exists (some logger setups omit it)
# -----------------------------------------------------------------------------
if not hasattr(log, "every"):

    def _every(key: str, n: int = 60, **kwargs: Any) -> None:
        # crude rate-limit: only log every n seconds per key
        now = time.time()
        _m = getattr(_every, "_m", {})  # type: ignore[attr-defined]
        last = _m.get(key, 0.0)
        if now - last >= float(n):
            _m[key] = now
            setattr(_every, "_m", _m)  # type: ignore[attr-defined]
            try:
                if hasattr(log, "event"):
                    log.event(key, **kwargs)
                else:
                    log.info("%s %s", key, kwargs)
            except Exception:
                pass

    setattr(log, "every", _every)


# =============================================================================
# Env helpers / config
# =============================================================================


def _env_json(key: str) -> Any:
    raw = (os.getenv(key, "") or "").strip()
    if not raw:
        return None
    try:
        return json.loads(raw)
    except Exception as e:
        try:
            log.event("SMART_ENV_JSON_FAIL", key=key, error=str(e)[:200])
        except Exception:
            pass
        return None


def _env_list(key: str) -> List[str]:
    raw = (os.getenv(key, "") or "").strip()
    if not raw:
        return []
    out: List[str] = []
    for chunk in raw.replace("\n", ",").split(","):
        c = chunk.strip()
        if c:
            out.append(c)
    return out


def _env_set(key: str) -> set[str]:
    return set(_env_list(key))


def _env_float(key: str, default: float) -> float:
    raw = os.getenv(key)
    if raw is None:
        return default
    try:
        return float(str(raw).strip())
    except Exception:
        return default


def _env_int(key: str, default: int) -> int:
    raw = os.getenv(key)
    if raw is None:
        return default
    try:
        return int(str(raw).strip())
    except Exception:
        return default


def _env_bool(key: str, default: bool) -> bool:
    raw = os.getenv(key)
    if raw is None:
        return default
    v = str(raw).strip().lower()
    if v in ("1", "true", "yes", "y", "on"):
        return True
    if v in ("0", "false", "no", "n", "off"):
        return False
    return default


def _clamp(x: float, lo: float, hi: float) -> float:
    if x < lo:
        return lo
    if x > hi:
        return hi
    return x


def _decay_factor(dt: float, half_life: float) -> float:
    if half_life <= 0.0 or dt <= 0.0:
        return 1.0
    return 0.5 ** (dt / half_life)


def _decay_update(
    value: float,
    last_ts: float,
    now: float,
    half_life: float,
    *,
    increment: float = 0.0,
) -> float:
    dt = max(0.0, now - last_ts)
    return (value * _decay_factor(dt, half_life)) + increment


def _to_01_from_count(count: float, scale: float) -> float:
    if scale <= 0:
        scale = 1.0
    x = max(0.0, count) / scale
    return float(_clamp(x, 0.0, 1.0))


def _net_minus_one_to_one(pos: float, neg: float) -> float:
    p = max(0.0, pos)
    n = max(0.0, neg)
    denom = p + n
    if denom <= 0.0:
        return 0.0
    return float(_clamp((p - n) / denom, -1.0, 1.0))


# =============================================================================
# Phase-level master switch (Master Roadmap Phase 0–7)
# =============================================================================

MASTER_PHASE = _env_int("MASTER_PHASE", _env_int("STRATEGY_PHASE", 7))
MASTER_PHASE = max(0, min(7, MASTER_PHASE))

if MASTER_PHASE == 0:
    _default_mode = "off"
elif MASTER_PHASE <= 2:
    _default_mode = "simple"
elif MASTER_PHASE <= 4:
    _default_mode = "alpha"
else:
    _default_mode = "full"

SMART_WALLET_MODE = (os.getenv("SMART_WALLET_MODE", _default_mode) or "").strip().lower()
if SMART_WALLET_MODE not in {"off", "simple", "alpha", "full"}:
    SMART_WALLET_MODE = "full"

SMART_WALLET_ENABLED = _env_bool("SMART_WALLET_ENABLED", SMART_WALLET_MODE != "off")
SMART_WHALE_ENABLED = _env_bool("SMART_WALLET_WHALE_ENABLED", SMART_WALLET_MODE != "off")
SMART_BREADTH_ENABLED = _env_bool(
    "SMART_WALLET_BREADTH_ENABLED", SMART_WALLET_MODE != "off"
)

SMART_ALPHA_ONLY_MODE = _env_bool(
    "SMART_WALLET_ALPHA_ONLY_MODE", SMART_WALLET_MODE == "alpha"
)

# If heat is tiny, fade secondary features to avoid noise.
SMART_MIN_SCORE_FOR_RICH_FEATURES = _env_float(
    "SMART_WALLET_MIN_SCORE_FOR_RICH_FEATURES", 0.05
)

# Cluster weight overrides (tuning without code changes)
SMART_CLUSTER_WEIGHTS = _env_json("SMART_WALLET_CLUSTER_WEIGHTS_JSON") or {}

# Per-profile reinvest tuning overrides
SMART_PROFILE_OVERRIDES = _env_json("SMART_WALLET_PROFILE_OVERRIDES_JSON") or {}

# Core smart-wallet knobs
SMART_WALLET_DECAY_HALF_LIFE_SEC = _env_float("SMART_WALLET_DECAY_HALF_LIFE_SEC", 900.0)
SMART_WALLET_MAX_SCORE = _env_float("SMART_WALLET_MAX_SCORE", 1.2)

SMART_WALLET_MIN_NOTIONAL_USD = _env_float("SMART_WALLET_MIN_NOTIONAL_USD", 50.0)
SMART_WALLET_MAX_CONTRIB_PER_TRADE = _env_float(
    "SMART_WALLET_MAX_CONTRIB_PER_TRADE", 1.0
)
SMART_WALLET_SELL_MULTIPLIER = _env_float("SMART_WALLET_SELL_MULTIPLIER", 0.5)

# Whale detection / tracking
WHALE_MIN_NOTIONAL_USD = _env_float("SMART_WALLET_WHALE_MIN_NOTIONAL_USD", 1_000.0)
WHALE_MIN_NOTIONAL_SOL = _env_float("SMART_WALLET_WHALE_MIN_NOTIONAL_SOL", 5.0)
WHALE_HALF_LIFE_SEC = _env_float("SMART_WALLET_WHALE_HALF_LIFE_SEC", 300.0)

# Unique breadth identity debounce
UNIQUE_ADDR_MIN_GAP_SEC = _env_float("SMART_WALLET_UNIQUE_ADDR_MIN_GAP_SEC", 60.0)

# Map counts → scores
SMART_EVENT_SCALE = _env_float("SMART_WALLET_COUNT_SCORE_SCALE", 5.0)
WHALE_SIDE_SCALE = _env_float("SMART_WALLET_WHALE_SIDE_SCORE_SCALE", 5.0)
UNIQUE_BUYERS_SCALE = _env_float("SMART_WALLET_UNIQUE_BUYERS_SCALE", 10.0)

# Domination / conflict knobs
WHALE_DOM_THRESHOLD = _env_float("SMART_WALLET_WHALE_DOM_THRESHOLD", 0.7)
SMART_WHALE_CONFLICT_NET_MIN = _env_float("SMART_WALLET_CONFLICT_NET_MIN", 0.4)

# Quant extras
SMART_ACCUM_ALPHA_SCALE = _env_float("SMART_WALLET_ACCUM_ALPHA_SCALE", 1.0)
SMART_PANIC_DUMP_SCALE = _env_float("SMART_WALLET_PANIC_DUMP_SCALE", 1.0)
WHALE_FOOTPRINT_ANCHOR_USD = _env_float(
    "SMART_WALLET_WHALE_FOOTPRINT_ANCHOR_USD", 50_000.0
)

SMART_SIGNAL_RECENCY_SEC = _env_float("SMART_WALLET_SIGNAL_RECENCY_SEC", 900.0)
SMART_SIGNAL_MIN_EVENTS_FOR_CONF = _env_float(
    "SMART_WALLET_SIGNAL_MIN_EVENTS_FOR_CONF", 3.0
)

SMART_DEBUG_LOG = _env_bool("SMART_WALLET_DEBUG_LOG", False)

# Prevent unbounded growth
SMART_WALLET_MAX_MINTS = _env_int("SMART_WALLET_MAX_MINTS", 4000)
SMART_WALLET_IDLE_PRUNE_SEC = _env_float("SMART_WALLET_IDLE_PRUNE_SEC", 7200.0)

# Address map bounds (important on hot tokens)
SMART_WALLET_MAX_ADDRS_PER_MINT = _env_int("SMART_WALLET_MAX_ADDRS_PER_MINT", 2500)
SMART_WALLET_ADDR_PRUNE_AGE_SEC = _env_float("SMART_WALLET_ADDR_PRUNE_AGE_SEC", 900.0)
SMART_WALLET_ADDR_PRUNE_SAMPLE = _env_int("SMART_WALLET_ADDR_PRUNE_SAMPLE", 32)

# Tiny per-mint feature cache (avoid recompute spam in a single tick)
SMART_FEATURE_CACHE_SEC = _env_float("SMART_WALLET_FEATURE_CACHE_SEC", 0.5)

# Alpha clusters (extra names treated as alpha)
ALPHA_CLUSTER_NAMES = set(n.lower() for n in _env_list("SMART_WALLET_ALPHA_CLUSTERS"))

# Reinvest filters
SMART_REINVEST_EXCLUDE_MINTS = _env_set("SMART_WALLET_REINVEST_EXCLUDE_MINTS")
SMART_REINVEST_ONLY_MINTS = _env_set("SMART_WALLET_REINVEST_ONLY_MINTS")

REINVEST_MAX_PANIC = _env_float("SMART_WALLET_REINVEST_MAX_PANIC", 0.65)
REINVEST_MAX_RISK = _env_float("SMART_WALLET_REINVEST_MAX_RISK", 0.7)
REINVEST_MAX_CONFLICT = _env_float("SMART_WALLET_REINVEST_MAX_CONFLICT", 0.75)

SMART_REINVEST_REQUIRE_TRADEABLE = _env_bool(
    "SMART_WALLET_REINVEST_REQUIRE_TRADEABLE", True
)

# Regime / tradeability thresholds
SMART_REGIME_ACCUM_THRESHOLD = _env_float("SMART_WALLET_REGIME_ACCUM_THRESHOLD", 0.5)
SMART_REGIME_PANIC_THRESHOLD = _env_float("SMART_WALLET_REGIME_PANIC_THRESHOLD", 0.5)
SMART_REGIME_CONFLICT_THRESHOLD = _env_float(
    "SMART_WALLET_REGIME_CONFLICT_THRESHOLD", 0.5
)

SMART_TRADEABLE_MIN_CONF = _env_float("SMART_WALLET_TRADEABLE_MIN_CONF", 0.25)
SMART_TRADEABLE_MIN_QUALITY = _env_float("SMART_WALLET_TRADEABLE_MIN_QUALITY", 0.25)
SMART_TRADEABLE_MIN_HEAT = _env_float("SMART_WALLET_TRADEABLE_MIN_HEAT", 0.10)
SMART_TRADEABLE_MAX_RISK = _env_float("SMART_WALLET_TRADEABLE_MAX_RISK", 0.85)


# =============================================================================
# Registry
# =============================================================================


@dataclass(frozen=True)
class SmartWalletMeta:
    cluster: str
    weight: float  # -2..+2 (negative = anti / exit)
    is_anti: bool = False
    half_life_override_sec: Optional[float] = None


class SmartWalletRegistry:
    """
    Static mapping: wallet address → SmartWalletMeta.

    Env sources:
      SMART_WALLET_CLUSTERS_JSON
      SMART_WALLET_LIST
      SMART_WALLET_ANTI_CLUSTERS_JSON
      SMART_WALLET_ANTI_LIST
    """

    def __init__(self) -> None:
        self._by_addr: Dict[str, SmartWalletMeta] = {}
        self._load_from_env()

    @staticmethod
    def _normalize_addr(addr: str) -> str:
        return (addr or "").strip()

    def reload(self) -> None:
        """Rebuild registry from env (safe to call; best-effort)."""
        self._by_addr.clear()
        self._load_from_env()

    def _load_clusters_from_json(
        self,
        raw: Any,
        *,
        is_anti: bool,
        default_cluster: str,
    ) -> None:
        if not isinstance(raw, dict):
            return
        for cluster_name, spec in raw.items():
            try:
                if not isinstance(spec, dict):
                    continue
                addrs = spec.get("addresses") or spec.get("wallets") or []
                if not isinstance(addrs, (list, tuple)):
                    continue

                weight = float(spec.get("weight", -1.0 if is_anti else 1.0))
                weight = _clamp(weight, -2.0, 2.0)

                cname = str(cluster_name or default_cluster).strip() or default_cluster
                # optional override
                if cname in SMART_CLUSTER_WEIGHTS:
                    try:
                        weight = _clamp(float(SMART_CLUSTER_WEIGHTS[cname]), -2.0, 2.0)
                    except Exception:
                        pass

                hl_raw = spec.get("half_life_sec")
                hl: Optional[float]
                try:
                    hl = float(hl_raw) if hl_raw is not None else None
                except Exception:
                    hl = None

                for addr in addrs:
                    a = self._normalize_addr(str(addr))
                    if not a:
                        continue
                    self._by_addr[a] = SmartWalletMeta(
                        cluster=cname,
                        weight=weight,
                        is_anti=is_anti or (weight < 0),
                        half_life_override_sec=hl,
                    )
            except Exception as e:
                log.every(
                    "SMART_REGISTRY_CLUSTER_FAIL",
                    n=10,
                    cluster=str(cluster_name),
                    error=str(e)[:200],
                )

    def _load_flat_list(
        self,
        addrs: List[str],
        *,
        is_anti: bool,
        default_cluster: str,
        weight: float,
    ) -> None:
        weight = _clamp(float(weight), -2.0, 2.0)
        for addr in addrs:
            a = self._normalize_addr(addr)
            if not a:
                continue
            if a in self._by_addr:
                continue
            self._by_addr[a] = SmartWalletMeta(
                cluster=default_cluster,
                weight=weight,
                is_anti=is_anti or (weight < 0),
                half_life_override_sec=None,
            )

    def _load_from_env(self) -> None:
        clusters = _env_json("SMART_WALLET_CLUSTERS_JSON")
        flat = _env_list("SMART_WALLET_LIST")
        anti_clusters = _env_json("SMART_WALLET_ANTI_CLUSTERS_JSON")
        anti_flat = _env_list("SMART_WALLET_ANTI_LIST")

        self._load_clusters_from_json(clusters, is_anti=False, default_cluster="smart_default")
        if flat:
            self._load_flat_list(flat, is_anti=False, default_cluster="smart_default", weight=1.0)

        self._load_clusters_from_json(anti_clusters, is_anti=True, default_cluster="anti_default")
        if anti_flat:
            self._load_flat_list(anti_flat, is_anti=True, default_cluster="anti_default", weight=-1.0)

        log.every("SMART_REGISTRY_LOADED", n=60, count=len(self._by_addr), mode=SMART_WALLET_MODE, phase=MASTER_PHASE)

    # runtime mutation (optional)
    def register_wallet(
        self,
        address: str,
        *,
        cluster: str = "runtime_smart",
        weight: float = 1.0,
        is_anti: bool = False,
        half_life_override_sec: Optional[float] = None,
    ) -> None:
        a = self._normalize_addr(address)
        if not a:
            return
        w = _clamp(float(weight), -2.0, 2.0)
        self._by_addr[a] = SmartWalletMeta(
            cluster=cluster or "runtime_smart",
            weight=w,
            is_anti=is_anti or (w < 0),
            half_life_override_sec=half_life_override_sec,
        )

    def remove_wallet(self, address: str) -> None:
        a = self._normalize_addr(address)
        self._by_addr.pop(a, None)

    # query
    def get_meta(self, address: str) -> Optional[SmartWalletMeta]:
        return self._by_addr.get(self._normalize_addr(address))

    def is_smart(self, address: str) -> bool:
        m = self.get_meta(address)
        return bool(m and m.weight > 0)

    def is_anti(self, address: str) -> bool:
        m = self.get_meta(address)
        return bool(m and m.weight < 0)

    def explain_wallet(self, address: str) -> str:
        m = self.get_meta(address)
        if m is None:
            return f"{address}: <unregistered>"
        role = "anti" if m.is_anti or m.weight < 0 else "smart"
        hl = "default" if m.half_life_override_sec is None else f"{m.half_life_override_sec:.0f}s"
        return f"{address}: cluster={m.cluster} role={role} weight={m.weight:.2f} half_life={hl}"

    def summary(self) -> Dict[str, Any]:
        clusters: Dict[str, int] = {}
        smart = anti = 0
        for m in self._by_addr.values():
            clusters[m.cluster] = clusters.get(m.cluster, 0) + 1
            if m.weight >= 0:
                smart += 1
            else:
                anti += 1
        return {
            "total": len(self._by_addr),
            "smart_count": smart,
            "anti_count": anti,
            "clusters": clusters,
            "mode": SMART_WALLET_MODE,
            "phase": MASTER_PHASE,
        }


REGISTRY = SmartWalletRegistry()


# =============================================================================
# Tracker
# =============================================================================


class SmartWalletTracker:
    """
    Tracks decayed smart-wallet + whale + breadth stats per mint.
    """

    def __init__(self) -> None:
        # smart signed flow
        self._scores: Dict[str, float] = {}
        self._last_ts: Dict[str, float] = {}
        self._last_buy_ts: Dict[str, float] = {}
        self._last_sell_ts: Dict[str, float] = {}
        self._last_cluster: Dict[str, str] = {}

        # smart event counts
        self._smart_event_score: Dict[str, float] = {}
        self._smart_event_last_ts: Dict[str, float] = {}
        self._smart_buy_count: Dict[str, float] = {}
        self._smart_sell_count: Dict[str, float] = {}
        self._smart_side_last_ts: Dict[str, float] = {}

        # whales
        self._whale_count: Dict[str, float] = {}
        self._whale_volume: Dict[str, float] = {}
        self._whale_last_ts: Dict[str, float] = {}
        self._whale_buy_count: Dict[str, float] = {}
        self._whale_sell_count: Dict[str, float] = {}
        self._whale_side_last_ts: Dict[str, float] = {}

        # breadth
        self._addr_last_seen: Dict[str, Dict[str, float]] = {}
        self._unique_score: Dict[str, float] = {}
        self._unique_last_ts: Dict[str, float] = {}

        # smart breadth subset
        self._smart_addr_last_seen: Dict[str, Dict[str, float]] = {}
        self._smart_unique_score: Dict[str, float] = {}
        self._smart_unique_last_ts: Dict[str, float] = {}

        # pruning + cache
        self._last_any_activity_ts: Dict[str, float] = {}
        self._op_counter: int = 0
        self._feature_cache: Dict[str, Tuple[float, Dict[str, float]]] = {}

        self._lock = asyncio.Lock()

    # -------------------- maintenance --------------------

    async def clear_mint(self, mint: str) -> None:
        m = (mint or "").strip()
        if not m:
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
                d.pop(m, None)

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

    # -------------------- internal helpers --------------------

    @staticmethod
    def _normalize_notional(amount_usd: Optional[float], amount_sol: Optional[float]) -> float:
        # rough USD notional; if SOL given, use coarse anchor (only for sizing buckets)
        try:
            if amount_usd is not None:
                return max(0.0, float(amount_usd))
            if amount_sol is not None:
                return max(0.0, float(amount_sol) * 100.0)
        except Exception:
            return 0.0
        return 0.0

    @staticmethod
    def _smart_contribution_from_notional(amt_usd: float) -> float:
        if amt_usd <= 0.0:
            return 0.0
        if amt_usd < SMART_WALLET_MIN_NOTIONAL_USD:
            return 0.1 * (amt_usd / max(SMART_WALLET_MIN_NOTIONAL_USD, 1.0))
        val = amt_usd / 5_000.0
        val = min(val, SMART_WALLET_MAX_CONTRIB_PER_TRADE)
        return max(0.0, float(val))

    @staticmethod
    def _is_whale(amt_usd: float, amt_sol: Optional[float]) -> bool:
        if amt_usd >= WHALE_MIN_NOTIONAL_USD:
            return True
        if amt_sol is not None and amt_sol >= WHALE_MIN_NOTIONAL_SOL:
            return True
        return False

    @staticmethod
    def _is_alpha_cluster(cluster: str) -> bool:
        c = (cluster or "").lower()
        return ("alpha" in c) or ("smart" in c) or (c in ALPHA_CLUSTER_NAMES)

    def _touch_mint(self, mint: str, now: float) -> None:
        self._last_any_activity_ts[mint] = now
        self._op_counter += 1
        self._feature_cache.pop(mint, None)

    def _prune_addr_map(self, addr_map: Dict[str, float], now: float) -> None:
        # Cheap prune by age + random sampling to avoid O(n) scans each trade.
        if not addr_map:
            return

        if len(addr_map) <= SMART_WALLET_MAX_ADDRS_PER_MINT:
            # opportunistic age-based cleanup for old keys (sample)
            sample_n = min(SMART_WALLET_ADDR_PRUNE_SAMPLE, len(addr_map))
            if sample_n <= 0:
                return
            keys = random.sample(list(addr_map.keys()), sample_n)
            cutoff = now - SMART_WALLET_ADDR_PRUNE_AGE_SEC
            for k in keys:
                ts = addr_map.get(k, now)
                if ts < cutoff:
                    addr_map.pop(k, None)
            return

        # Over limit: evict oldest-ish by sampling
        sample_n = min(max(SMART_WALLET_ADDR_PRUNE_SAMPLE, 16), len(addr_map))
        keys = random.sample(list(addr_map.keys()), sample_n)
        # remove a handful of the stalest in this sample
        keys.sort(key=lambda k: addr_map.get(k, 0.0))
        evict_n = max(8, int(0.02 * len(addr_map)))  # 2% or at least 8
        for k in keys[: min(evict_n, len(keys))]:
            addr_map.pop(k, None)

    def _maybe_prune_mints(self, now: float) -> None:
        if SMART_WALLET_MAX_MINTS <= 0:
            return
        if self._op_counter % 750 != 0:
            return
        mints = list(self._last_any_activity_ts.keys())
        if len(mints) <= SMART_WALLET_MAX_MINTS:
            return
        idle_cutoff = now - SMART_WALLET_IDLE_PRUNE_SEC
        stale = [m for m in mints if self._last_any_activity_ts.get(m, 0.0) < idle_cutoff]
        if not stale:
            return
        # prune stale mints
        for mint in stale:
            self._scores.pop(mint, None)
            self._last_ts.pop(mint, None)
            self._last_buy_ts.pop(mint, None)
            self._last_sell_ts.pop(mint, None)
            self._last_cluster.pop(mint, None)
            self._smart_event_score.pop(mint, None)
            self._smart_event_last_ts.pop(mint, None)
            self._smart_buy_count.pop(mint, None)
            self._smart_sell_count.pop(mint, None)
            self._smart_side_last_ts.pop(mint, None)
            self._whale_count.pop(mint, None)
            self._whale_volume.pop(mint, None)
            self._whale_last_ts.pop(mint, None)
            self._whale_buy_count.pop(mint, None)
            self._whale_sell_count.pop(mint, None)
            self._whale_side_last_ts.pop(mint, None)
            self._addr_last_seen.pop(mint, None)
            self._unique_score.pop(mint, None)
            self._unique_last_ts.pop(mint, None)
            self._smart_addr_last_seen.pop(mint, None)
            self._smart_unique_score.pop(mint, None)
            self._smart_unique_last_ts.pop(mint, None)
            self._last_any_activity_ts.pop(mint, None)
            self._feature_cache.pop(mint, None)

        log.every("SMART_PRUNE_MINTS", n=60, pruned=len(stale), remaining=len(self._last_any_activity_ts))

    # -------------------- ingestion --------------------

    async def observe_swap(
        self,
        *,
        mint: str,
        trader: str,
        amount_usd: float | None = None,
        amount_sol: float | None = None,
        side: str = "buy",
    ) -> None:
        mint = (mint or "").strip()
        trader = (trader or "").strip()
        if not mint or not trader:
            return

        if not (SMART_WALLET_ENABLED or SMART_WHALE_ENABLED or SMART_BREADTH_ENABLED):
            return

        now = time.time()
        side_s = (side or "buy").strip().lower()
        is_buy = side_s.startswith("b")
        is_sell = side_s.startswith("s")

        amt_usd = self._normalize_notional(amount_usd, amount_sol)
        whale = self._is_whale(amt_usd, amount_sol)

        meta = REGISTRY.get_meta(trader) if SMART_WALLET_ENABLED else None
        alpha_ok = True
        if meta and SMART_ALPHA_ONLY_MODE:
            alpha_ok = self._is_alpha_cluster(meta.cluster)

        async with self._lock:
            self._touch_mint(mint, now)

            # --- breadth (all) ---
            if SMART_BREADTH_ENABLED:
                addr_map = self._addr_last_seen.setdefault(mint, {})
                last_seen = addr_map.get(trader, 0.0)
                addr_map[trader] = now
                self._prune_addr_map(addr_map, now)

                new_identity = (now - last_seen) >= UNIQUE_ADDR_MIN_GAP_SEC
                u_old = self._unique_score.get(mint, 0.0)
                u_last = self._unique_last_ts.get(mint, now)
                u_new = _decay_update(
                    u_old, u_last, now, WHALE_HALF_LIFE_SEC, increment=1.0 if new_identity else 0.0
                )
                self._unique_score[mint] = u_new
                self._unique_last_ts[mint] = now

            # --- smart wallet signal ---
            if meta is not None and SMART_WALLET_ENABLED and alpha_ok:
                contrib_norm = self._smart_contribution_from_notional(amt_usd)
                if contrib_norm > 0.0:
                    old = self._scores.get(mint, 0.0)
                    last = self._last_ts.get(mint, now)

                    half_life = meta.half_life_override_sec or SMART_WALLET_DECAY_HALF_LIFE_SEC
                    base = _decay_update(old, last, now, half_life, increment=0.0)

                    # direction mapping:
                    #   smart weight>0: buy=+1, sell=-SELL_MULT
                    #   anti weight<0:  buy=-|w|, sell=+|w|*SELL_MULT  (inverts)
                    w = float(meta.weight)
                    if w >= 0:
                        direction = 1.0 if (is_buy or not (is_buy or is_sell)) else -SMART_WALLET_SELL_MULTIPLIER
                    else:
                        direction = -abs(w) if (is_buy or not (is_buy or is_sell)) else abs(w) * SMART_WALLET_SELL_MULTIPLIER

                    contrib = float(contrib_norm) * float(direction)
                    base += contrib

                    max_abs = max(SMART_WALLET_MAX_SCORE, 0.1)
                    base = _clamp(base, -max_abs, max_abs)

                    self._scores[mint] = base
                    self._last_ts[mint] = now
                    self._last_cluster[mint] = meta.cluster
                    if is_buy or not (is_buy or is_sell):
                        self._last_buy_ts[mint] = now
                    elif is_sell:
                        self._last_sell_ts[mint] = now

                    # smart event count
                    ev_old = self._smart_event_score.get(mint, 0.0)
                    ev_last = self._smart_event_last_ts.get(mint, now)
                    ev_new = _decay_update(ev_old, ev_last, now, SMART_WALLET_DECAY_HALF_LIFE_SEC, increment=1.0)
                    self._smart_event_score[mint] = ev_new
                    self._smart_event_last_ts[mint] = now

                    # side-specific
                    sb_old = self._smart_buy_count.get(mint, 0.0)
                    ss_old = self._smart_sell_count.get(mint, 0.0)
                    s_last = self._smart_side_last_ts.get(mint, now)

                    sb_inc = 1.0 if (is_buy or not (is_buy or is_sell)) else 0.0
                    ss_inc = 1.0 if is_sell else 0.0

                    sb_new = _decay_update(sb_old, s_last, now, SMART_WALLET_DECAY_HALF_LIFE_SEC, increment=sb_inc)
                    ss_new = _decay_update(ss_old, s_last, now, SMART_WALLET_DECAY_HALF_LIFE_SEC, increment=ss_inc)

                    self._smart_buy_count[mint] = sb_new
                    self._smart_sell_count[mint] = ss_new
                    self._smart_side_last_ts[mint] = now

                    # smart-unique (subset)
                    smap = self._smart_addr_last_seen.setdefault(mint, {})
                    s_last_seen = smap.get(trader, 0.0)
                    smap[trader] = now
                    self._prune_addr_map(smap, now)

                    s_new_ident = (now - s_last_seen) >= UNIQUE_ADDR_MIN_GAP_SEC
                    su_old = self._smart_unique_score.get(mint, 0.0)
                    su_last = self._smart_unique_last_ts.get(mint, now)
                    su_new = _decay_update(
                        su_old, su_last, now, WHALE_HALF_LIFE_SEC, increment=1.0 if s_new_ident else 0.0
                    )
                    self._smart_unique_score[mint] = su_new
                    self._smart_unique_last_ts[mint] = now

                    if SMART_DEBUG_LOG:
                        try:
                            log.event(
                                "SMART_SWAP_OBSERVED",
                                mint=mint,
                                trader=trader,
                                cluster=meta.cluster,
                                weight=meta.weight,
                                amt_usd=amt_usd,
                                side=side_s,
                                contrib=contrib,
                                score=base,
                            )
                        except Exception:
                            pass

            # --- whales ---
            if SMART_WHALE_ENABLED and whale:
                wc_old = self._whale_count.get(mint, 0.0)
                wv_old = self._whale_volume.get(mint, 0.0)
                w_last = self._whale_last_ts.get(mint, now)

                wc_new = _decay_update(wc_old, w_last, now, WHALE_HALF_LIFE_SEC, increment=1.0)
                wv_new = _decay_update(wv_old, w_last, now, WHALE_HALF_LIFE_SEC, increment=amt_usd)

                self._whale_count[mint] = wc_new
                self._whale_volume[mint] = wv_new
                self._whale_last_ts[mint] = now

                wb_old = self._whale_buy_count.get(mint, 0.0)
                ws_old = self._whale_sell_count.get(mint, 0.0)
                ws_last = self._whale_side_last_ts.get(mint, now)

                wb_inc = 1.0 if (is_buy or not (is_buy or is_sell)) else 0.0
                ws_inc = 1.0 if is_sell else 0.0

                wb_new = _decay_update(wb_old, ws_last, now, WHALE_HALF_LIFE_SEC, increment=wb_inc)
                ws_new = _decay_update(ws_old, ws_last, now, WHALE_HALF_LIFE_SEC, increment=ws_inc)

                self._whale_buy_count[mint] = wb_new
                self._whale_sell_count[mint] = ws_new
                self._whale_side_last_ts[mint] = now

            self._maybe_prune_mints(now)

    async def observe_swap_event(self, ev: Dict[str, Any]) -> None:
        """
        Generic adapter for trade events from PumpPortal / Raydium / Dexscreener.

        Best-effort keys:
          mint: mint/mintAddress/mint_address/tokenMint/baseMint/address
          trader: trader/owner/wallet/user/buyer/maker/taker
          amount_usd: amount_usd/size_usd/notional_usd/quote_usd
          amount_sol: amount_sol/size_sol/sol/quote_sol
          side: side/direction ("buy"/"sell")
        """
        mint = (
            ev.get("mint")
            or ev.get("mintAddress")
            or ev.get("mint_address")
            or ev.get("tokenMint")
            or ev.get("baseMint")
            or ev.get("address")
        )
        trader = (
            ev.get("trader")
            or ev.get("owner")
            or ev.get("wallet")
            or ev.get("user")
            or ev.get("buyer")
            or ev.get("maker")
            or ev.get("taker")
        )
        if not mint or not trader:
            return

        amt_usd = ev.get("amount_usd") or ev.get("size_usd") or ev.get("notional_usd") or ev.get("quote_usd")
        amt_sol = ev.get("amount_sol") or ev.get("size_sol") or ev.get("sol") or ev.get("quote_sol")
        side = ev.get("side") or ev.get("direction") or ev.get("isBuy") or "buy"

        # normalize side
        if isinstance(side, bool):
            side_s = "buy" if side else "sell"
        else:
            side_s = str(side).lower().strip()
            if side_s in ("true", "1"):
                side_s = "buy"
            if side_s in ("false", "0"):
                side_s = "sell"

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
            side=side_s,
        )

    async def bulk_observe_swap_events(self, events: List[Dict[str, Any]]) -> None:
        for ev in events:
            try:
                await self.observe_swap_event(ev)
            except Exception:
                if SMART_DEBUG_LOG:
                    log.exception("SMART_BULK_EVENT_FAIL ev=%s", ev)

    # -------------------- reading / features --------------------

    async def _get_decayed_smart_score(self, mint: str, now: Optional[float] = None) -> float:
        if now is None:
            now = time.time()
        async with self._lock:
            old = self._scores.get(mint, 0.0)
            last = self._last_ts.get(mint, now)
            new = _decay_update(old, last, now, SMART_WALLET_DECAY_HALF_LIFE_SEC, increment=0.0)
            self._scores[mint] = new
            self._last_ts[mint] = now
            return float(new)

    async def get_raw_score(self, mint: str) -> float:
        if not SMART_WALLET_ENABLED or SMART_WALLET_MODE == "off":
            return 0.0
        return await self._get_decayed_smart_score(mint)

    async def get_score_01(self, mint: str) -> float:
        raw = await self.get_raw_score(mint)
        max_abs = max(SMART_WALLET_MAX_SCORE, 1e-6)
        x = _clamp(raw / max_abs, -1.0, 1.0)
        return 0.5 * (x + 1.0)

    async def _maybe_cached_features(self, mint: str, now: float) -> Optional[Dict[str, float]]:
        if SMART_FEATURE_CACHE_SEC <= 0:
            return None
        async with self._lock:
            item = self._feature_cache.get(mint)
        if not item:
            return None
        ts, feats = item
        if now - ts > SMART_FEATURE_CACHE_SEC:
            return None
        return dict(feats)

    async def _store_cached_features(self, mint: str, feats: Dict[str, float], now: float) -> None:
        if SMART_FEATURE_CACHE_SEC <= 0:
            return
        async with self._lock:
            self._feature_cache[mint] = (now, dict(feats))

    async def build_features(self, mint: str) -> Dict[str, float]:
        mint = (mint or "").strip()
        now = time.time()

        cached = await self._maybe_cached_features(mint, now)
        if cached is not None:
            return cached

        if not SMART_WALLET_ENABLED or SMART_WALLET_MODE == "off":
            raw_smart = 0.0
            smart_01 = 0.5
        else:
            raw_smart = await self._get_decayed_smart_score(mint, now=now)
            max_abs = max(SMART_WALLET_MAX_SCORE, 1e-6)
            x = _clamp(raw_smart / max_abs, -1.0, 1.0)
            smart_01 = 0.5 * (x + 1.0)

        async with self._lock:
            last_buy_ts = self._last_buy_ts.get(mint, 0.0)
            last_sell_ts = self._last_sell_ts.get(mint, 0.0)
            last_cluster = self._last_cluster.get(mint, "")

            ev_score = self._smart_event_score.get(mint, 0.0)
            ev_last_ts = self._smart_event_last_ts.get(mint, now)

            sb = self._smart_buy_count.get(mint, 0.0)
            ss = self._smart_sell_count.get(mint, 0.0)
            smart_side_last_ts = self._smart_side_last_ts.get(mint, now)

            wc = self._whale_count.get(mint, 0.0)
            wv = self._whale_volume.get(mint, 0.0)
            wc_last_ts = self._whale_last_ts.get(mint, now)

            wb = self._whale_buy_count.get(mint, 0.0)
            ws = self._whale_sell_count.get(mint, 0.0)
            ws_last_ts = self._whale_side_last_ts.get(mint, now)

            u_score = self._unique_score.get(mint, 0.0)
            u_last_ts = self._unique_last_ts.get(mint, now)

            su_score = self._smart_unique_score.get(mint, 0.0)
            su_last_ts = self._smart_unique_last_ts.get(mint, now)

        buy_ago = float(now - last_buy_ts) if last_buy_ts > 0 else 1e9
        sell_ago = float(now - last_sell_ts) if last_sell_ts > 0 else 1e9

        # smart event count
        smart_events_decayed = ev_score * _decay_factor(max(0.0, now - ev_last_ts), SMART_WALLET_DECAY_HALF_LIFE_SEC)
        smart_wallet_count_5m = float(smart_events_decayed)

        # smart side counts
        decay_s = _decay_factor(max(0.0, now - smart_side_last_ts), SMART_WALLET_DECAY_HALF_LIFE_SEC)
        sb_decayed = sb * decay_s
        ss_decayed = ss * decay_s
        smart_buy_score_5m = _to_01_from_count(sb_decayed, SMART_EVENT_SCALE)
        smart_sell_score_5m = _to_01_from_count(ss_decayed, SMART_EVENT_SCALE)
        smart_net_side_5m = _net_minus_one_to_one(sb_decayed, ss_decayed)

        # whales
        decay_wc = _decay_factor(max(0.0, now - wc_last_ts), WHALE_HALF_LIFE_SEC)
        whales_5m = float(wc * decay_wc)
        whale_vol_5m_usd = float(wv * decay_wc)

        # whale side
        decay_side = _decay_factor(max(0.0, now - ws_last_ts), WHALE_HALF_LIFE_SEC)
        wb_decayed = wb * decay_side
        ws_decayed = ws * decay_side
        whale_buy_score = _to_01_from_count(wb_decayed, WHALE_SIDE_SCALE)
        whale_sell_score = _to_01_from_count(ws_decayed, WHALE_SIDE_SCALE)
        whale_net_side_5m = _net_minus_one_to_one(wb_decayed, ws_decayed)

        # breadth
        unique_buyers_5m = float(u_score * _decay_factor(max(0.0, now - u_last_ts), WHALE_HALF_LIFE_SEC))
        smart_unique_buyers_5m = float(su_score * _decay_factor(max(0.0, now - su_last_ts), WHALE_HALF_LIFE_SEC))

        whale_share_5m = 0.0 if unique_buyers_5m <= 0 else _clamp(whales_5m / max(unique_buyers_5m, 1.0), 0.0, 1.0)
        holders_growth_5m = min(unique_buyers_5m / max(UNIQUE_BUYERS_SCALE, 1.0), 0.5)

        # alpha
        is_alpha_cluster = self._is_alpha_cluster(last_cluster)
        smart_last_cluster_is_alpha = 1.0 if is_alpha_cluster else 0.0
        smart_alpha_bias = 0.0
        if smart_wallet_count_5m > 0.0 and is_alpha_cluster:
            smart_alpha_bias = min(1.0, smart_wallet_count_5m / max(SMART_EVENT_SCALE, 1.0))

        # alignment / domination / conflict
        smart_whale_alignment = float(smart_net_side_5m * whale_net_side_5m)  # -1..+1
        smart_whale_alignment_abs = abs(smart_whale_alignment)

        if whale_share_5m <= WHALE_DOM_THRESHOLD:
            whale_domination_5m = 0.0
        else:
            denom = max(1.0 - WHALE_DOM_THRESHOLD, 1e-6)
            whale_domination_5m = _clamp((whale_share_5m - WHALE_DOM_THRESHOLD) / denom, 0.0, 1.0)

        conflict = 0.0
        if (
            abs(smart_net_side_5m) >= SMART_WHALE_CONFLICT_NET_MIN
            and abs(whale_net_side_5m) >= SMART_WHALE_CONFLICT_NET_MIN
            and (smart_net_side_5m * whale_net_side_5m) < 0
        ):
            conflict = min(1.0, abs(smart_net_side_5m - whale_net_side_5m) / 2.0)
        smart_vs_whale_conflict = float(conflict)

        # accumulation / dump
        accum_raw = max(0.0, smart_net_side_5m) * max(0.0, whale_net_side_5m)
        if smart_alpha_bias > 0.0:
            accum_raw *= (1.0 + 0.5 * smart_alpha_bias)
        smart_accumulation_score = _clamp(accum_raw * SMART_ACCUM_ALPHA_SCALE, 0.0, 1.0)

        dump_raw = max(0.0, -smart_net_side_5m) * max(0.0, -whale_net_side_5m)
        dump_raw *= (1.0 + whale_domination_5m)
        smart_panic_dump_score = _clamp(dump_raw * SMART_PANIC_DUMP_SCALE, 0.0, 1.0)

        # whale footprint
        whale_liquidity_footprint = 0.0
        if WHALE_FOOTPRINT_ANCHOR_USD > 0:
            whale_liquidity_footprint = _clamp(whale_vol_5m_usd / WHALE_FOOTPRINT_ANCHOR_USD, 0.0, 1.0)

        # confidence (recency + volume)
        last_active_ts = max(last_buy_ts, last_sell_ts)
        if last_active_ts <= 0:
            recency_factor = 0.0
        else:
            age = now - last_active_ts
            recency_factor = 0.0 if age >= SMART_SIGNAL_RECENCY_SEC else max(0.0, 1.0 - (age / max(SMART_SIGNAL_RECENCY_SEC, 1.0)))

        volume_factor = _to_01_from_count(smart_wallet_count_5m, SMART_SIGNAL_MIN_EVENTS_FOR_CONF)
        smart_signal_confidence = _clamp(0.6 * recency_factor + 0.4 * volume_factor, 0.0, 1.0)

        # quality
        quality_raw = 0.0
        quality_raw += 0.4 * smart_alpha_bias
        quality_raw += 0.35 * smart_whale_alignment_abs
        quality_raw += 0.25 * (1.0 - smart_vs_whale_conflict)
        quality_raw -= 0.15 * whale_domination_5m
        smart_signal_quality = _clamp(quality_raw, 0.0, 1.0)

        # risk flag
        risk_raw = (0.7 * smart_vs_whale_conflict + 0.5 * whale_domination_5m)
        risk_raw *= min(1.0, abs(smart_net_side_5m) + abs(whale_net_side_5m))
        smart_risk_flag = _clamp(risk_raw, 0.0, 1.0)

        # sides
        smart_recent_side = 1.0 if smart_net_side_5m > 0.15 else (-1.0 if smart_net_side_5m < -0.15 else 0.0)
        whale_recent_side = 1.0 if whale_net_side_5m > 0.15 else (-1.0 if whale_net_side_5m < -0.15 else 0.0)

        # crowding
        smart_crowding_score = 0.0 if unique_buyers_5m <= 0 else _clamp(smart_unique_buyers_5m / max(unique_buyers_5m, 1.0), 0.0, 1.0)

        # compressed state
        smart_signal_state = float(smart_net_side_5m * smart_signal_quality * smart_signal_confidence)

        # heat
        smart_heat_raw = abs(smart_net_side_5m) * smart_signal_confidence * (0.5 + 0.5 * smart_signal_quality)
        smart_heat_score = _clamp(smart_heat_raw, 0.0, 1.0)

        whale_heat_raw = abs(whale_net_side_5m) * min(1.0, whales_5m / max(1.0, SMART_EVENT_SCALE)) * (0.5 + 0.5 * whale_liquidity_footprint)
        whale_heat_score = _clamp(whale_heat_raw, 0.0, 1.0)

        if smart_heat_score < SMART_MIN_SCORE_FOR_RICH_FEATURES:
            smart_vs_whale_conflict *= 0.5
            whale_domination_5m *= 0.5
            smart_risk_flag *= 0.5

        total_heat = smart_heat_score + whale_heat_score
        smart_whale_pressure_ratio = 0.0 if total_heat <= 0 else min(1.0, whale_heat_score / max(total_heat, 1e-6))

        smart_entry_boost_hint = float(smart_signal_state)
        smart_exit_pressure_hint = _clamp(
            smart_panic_dump_score * (0.5 + 0.5 * whale_domination_5m) * (0.5 + 0.5 * smart_risk_flag),
            0.0,
            1.0,
        )

        regime_accum = 1.0 if smart_accumulation_score >= SMART_REGIME_ACCUM_THRESHOLD else 0.0
        regime_panic = 1.0 if smart_panic_dump_score >= SMART_REGIME_PANIC_THRESHOLD else 0.0
        regime_conflict = 1.0 if smart_vs_whale_conflict >= SMART_REGIME_CONFLICT_THRESHOLD else 0.0

        regime_chop = 0.0
        if (
            smart_heat_score < max(SMART_TRADEABLE_MIN_HEAT, 0.12)
            and whale_heat_score < max(SMART_TRADEABLE_MIN_HEAT, 0.12)
            and regime_accum == 0.0
            and regime_panic == 0.0
        ):
            regime_chop = 1.0

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
            smart_signal_confidence >= SMART_TRADEABLE_MIN_CONF
            and smart_signal_quality >= SMART_TRADEABLE_MIN_QUALITY
            and smart_heat_score >= SMART_TRADEABLE_MIN_HEAT
            and smart_risk_flag <= SMART_TRADEABLE_MAX_RISK
        )
        smart_tradeable_flag = 1.0 if is_tradeable else 0.0

        feats: Dict[str, float] = {
            "smart_wallet_score": float(smart_01),
            "smart_flow_raw": float(raw_smart),
            "smart_last_buy_sec_ago": float(buy_ago),
            "smart_last_sell_sec_ago": float(sell_ago),
            "smart_last_cluster_is_alpha": float(smart_last_cluster_is_alpha),
            "smart_wallet_count_5m": float(smart_wallet_count_5m),
            "smart_buy_score_5m": float(smart_buy_score_5m),
            "smart_sell_score_5m": float(smart_sell_score_5m),
            "smart_net_side_5m": float(smart_net_side_5m),
            "whales_5m": float(whales_5m),
            "whale_vol_5m_usd": float(whale_vol_5m_usd),
            "whale_buy_score": float(whale_buy_score),
            "whale_sell_score": float(whale_sell_score),
            "whale_net_side_5m": float(whale_net_side_5m),
            "unique_buyers_5m": float(unique_buyers_5m),
            "smart_unique_buyers_5m": float(smart_unique_buyers_5m),
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

        await self._store_cached_features(mint, feats, now)
        return feats

    async def attach_features_to_meta(self, meta: Dict[str, Any], mint: str) -> None:
        feats = await self.build_features(mint)
        meta.update(feats)

    async def snapshot(self, mint: str) -> Dict[str, Any]:
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

    async def top_active_mints(self, *, limit: int = 20, min_heat: float = 0.0) -> List[Dict[str, Any]]:
        async with self._lock:
            mints = list(self._last_any_activity_ts.keys())

        out: List[Dict[str, Any]] = []
        for mint in mints:
            feats = await self.build_features(mint)
            s_heat = float(feats.get("smart_heat_score", 0.0))
            w_heat = float(feats.get("whale_heat_score", 0.0))
            total = s_heat + w_heat
            if total < min_heat:
                continue
            out.append(
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

        out.sort(key=lambda r: r["total_heat"], reverse=True)
        return out[:limit] if limit > 0 else out

    async def global_regime_hint(self, *, limit: int = 50, min_heat: float = 0.1) -> Dict[str, float]:
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
        sum_trade = 0.0
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
            trade = float(feats.get("smart_tradeable_flag", 0.0))

            sum_s_heat += s_heat
            sum_w_heat += w_heat
            sum_s_side += s_side * s_heat
            sum_w_side += w_side * w_heat
            sum_panic += panic * s_heat
            sum_conf += conflict * (s_heat + w_heat)
            sum_risk += risk * (s_heat + w_heat)
            sum_crowd += crowd * (s_heat + w_heat)
            sum_trade += trade
            n += 1

        if n <= 0:
            n = 1

        denom_s = max(sum_s_heat, 1e-6)
        denom_w = max(sum_w_heat, 1e-6)
        denom_hw = max(sum_s_heat + sum_w_heat, 1e-6)

        return {
            "smart_heat_global": _clamp(sum_s_heat / n, 0.0, 1.0),
            "whale_heat_global": _clamp(sum_w_heat / n, 0.0, 1.0),
            "smart_side_global": _clamp(sum_s_side / denom_s, -1.0, 1.0),
            "whale_side_global": _clamp(sum_w_side / denom_w, -1.0, 1.0),
            "smart_panic_global": _clamp(sum_panic / denom_s, 0.0, 1.0),
            "smart_conflict_global": _clamp(sum_conf / denom_hw, 0.0, 1.0),
            "smart_risk_global": _clamp(sum_risk / denom_hw, 0.0, 1.0),
            "smart_crowding_global": _clamp(sum_crowd / denom_hw, 0.0, 1.0),
            "smart_tradeable_global": _clamp(sum_trade / max(float(n), 1.0), 0.0, 1.0),
        }

    async def entry_hint(self, mint: str) -> Dict[str, Any]:
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
            "tradeable": float(feats.get("smart_tradeable_flag", 0.0)),
        }

    async def exit_hint(self, mint: str) -> Dict[str, Any]:
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

    async def has_meaningful_signal(self, mint: str, *, min_conf: float = 0.2, min_heat: float = 0.1) -> bool:
        feats = await self.build_features(mint)
        conf = float(feats.get("smart_signal_confidence", 0.0))
        s_heat = float(feats.get("smart_heat_score", 0.0))
        w_heat = float(feats.get("whale_heat_score", 0.0))
        return conf >= min_conf and (s_heat + w_heat) >= min_heat


# Global tracker instance
TRACKER = SmartWalletTracker()
smart_wallets = TRACKER  # backward-friendly alias


# =============================================================================
# Risk ctx helper
# =============================================================================


async def build_risk_ctx_smart(*, limit: int = 50, min_heat: float = 0.1) -> Dict[str, float]:
    return await TRACKER.global_regime_hint(limit=limit, min_heat=min_heat)


# =============================================================================
# Reinvestment target picker (used by reinvestor.py)
# =============================================================================


async def pick_reinvestment_target(
    budget_sol: float | None = None,
    *,
    min_24h_volume_usd: float | None = None,
    redis: Any = None,  # kept for signature compatibility
    meta: Optional[Dict[str, Any]] = None,
    universe: Optional[Dict[str, Dict[str, Any]]] = None,
    **_: Any,
) -> Optional[Dict[str, Any] | str]:
    if not SMART_WALLET_ENABLED or SMART_WALLET_MODE == "off":
        return None

    meta = meta or {}
    profile_raw = meta.get("profile") or meta.get("bucket") or meta.get("reinvest_profile")
    profile = str(profile_raw).lower() if profile_raw is not None else ""

    # profile defaults
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

    # override via env JSON
    try:
        if isinstance(SMART_PROFILE_OVERRIDES, dict) and profile:
            o = SMART_PROFILE_OVERRIDES.get(profile)
            if isinstance(o, dict):
                if "risk_penalty" in o:
                    risk_penalty = float(o["risk_penalty"])
                if "min_conf" in o:
                    min_conf = float(o["min_conf"])
                if "min_heat" in o:
                    min_heat = float(o["min_heat"])
    except Exception:
        pass

    # tiny budgets need stronger conviction
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
    best_payload: Optional[Dict[str, Any]] = None

    for row in candidates:
        mint = row["mint"]

        if SMART_REINVEST_ONLY_MINTS and mint not in SMART_REINVEST_ONLY_MINTS:
            continue
        if mint in SMART_REINVEST_EXCLUDE_MINTS:
            continue

        feats = await TRACKER.build_features(mint)

        boost = float(feats.get("smart_entry_boost_hint", 0.0))      # -1..+1
        conf = float(feats.get("smart_signal_confidence", 0.0))      # 0..1
        qual = float(feats.get("smart_signal_quality", 0.0))         # 0..1
        risk_flag = float(feats.get("smart_risk_flag", 0.0))         # 0..1
        panic = float(feats.get("smart_exit_pressure_hint", 0.0))    # 0..1
        conflict = float(feats.get("smart_vs_whale_conflict", 0.0))  # 0..1
        whale_dom = float(feats.get("whale_domination_5m", 0.0))     # 0..1
        smart_heat = float(feats.get("smart_heat_score", 0.0))
        whale_heat = float(feats.get("whale_heat_score", 0.0))
        tradeable_flag = float(feats.get("smart_tradeable_flag", 0.0))

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

        if SMART_REINVEST_REQUIRE_TRADEABLE and tradeable_flag < 0.5:
            continue
        if panic > REINVEST_MAX_PANIC:
            continue
        if risk_flag > REINVEST_MAX_RISK:
            continue
        if conflict > REINVEST_MAX_CONFLICT:
            continue

        core_alpha = boost * conf * (0.5 + 0.5 * qual)
        heat_bonus = 0.15 * smart_heat + 0.05 * whale_heat
        penalty = (0.8 * panic) + (0.7 * conflict) + (0.9 * risk_flag * risk_penalty) + (0.3 * whale_dom)

        score = core_alpha + heat_bonus - penalty
        if score <= best_score:
            continue

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
                "smart_heat": smart_heat,
                "whale_heat": whale_heat,
                "tradeable_flag": tradeable_flag,
            },
        }

    return best_payload


# =============================================================================
# scripts/main.py helpers (wired)
# =============================================================================


async def get_wallet_state(*, limit: int = 10, min_heat: float = 0.15) -> Dict[str, Any]:
    """
    Used by scripts/main.py daily pnl notes:
      from utils_pkg.smart_wallets import get_wallet_state
    """
    state: Dict[str, Any] = {
        "ts": time.time(),
        "enabled": bool(SMART_WALLET_ENABLED),
        "mode": SMART_WALLET_MODE,
        "phase": MASTER_PHASE,
        "registry": REGISTRY.summary(),
    }

    try:
        state["global"] = await TRACKER.global_regime_hint(limit=50, min_heat=min_heat)
    except Exception:
        state["global"] = {}

    try:
        top = await TRACKER.top_active_mints(limit=limit, min_heat=min_heat)
        state["top"] = top
    except Exception:
        state["top"] = []

    return state


async def smart_wallets_loop(stop_evt: asyncio.Event) -> None:
    """
    Background maintenance loop (optional).
    Scheduled by scripts/main.py if present.

    Responsibilities:
      - periodic lightweight logging
      - opportunistic pruning safety (most pruning is already opportunistic on ingest)
      - optional registry reload hook (for local dev)
    """
    interval = max(2.0, _env_float("SMART_WALLET_LOOP_SEC", 60.0))
    log_every_sec = max(15.0, _env_float("SMART_WALLET_LOOP_LOG_EVERY_SEC", 180.0))
    allow_reload = _env_bool("SMART_WALLET_LOOP_ALLOW_REGISTRY_RELOAD", False)

    last_log = 0.0
    while not stop_evt.is_set():
        now = time.time()

        # optional registry reload for local/dev (env rarely changes on Render)
        if allow_reload and _env_bool("SMART_WALLET_FORCE_RELOAD", False):
            try:
                REGISTRY.reload()
                os.environ["SMART_WALLET_FORCE_RELOAD"] = "0"
                log.every("SMART_REGISTRY_RELOADED", n=10, ok=1, count=REGISTRY.summary().get("total", 0))
            except Exception as e:
                log.every("SMART_REGISTRY_RELOAD_FAIL", n=10, error=str(e)[:200])

        # lightweight periodic status
        if (now - last_log) >= log_every_sec:
            last_log = now
            try:
                g = await TRACKER.global_regime_hint(limit=30, min_heat=0.1)
                log.event(
                    "SMART_WALLET_STATE",
                    enabled=int(SMART_WALLET_ENABLED),
                    mode=SMART_WALLET_MODE,
                    phase=MASTER_PHASE,
                    registry_total=int(REGISTRY.summary().get("total", 0)),
                    smart_heat_global=round(float(g.get("smart_heat_global", 0.0)), 3),
                    whale_heat_global=round(float(g.get("whale_heat_global", 0.0)), 3),
                    smart_risk_global=round(float(g.get("smart_risk_global", 0.0)), 3),
                )
            except Exception:
                pass

        try:
            await asyncio.sleep(interval)
        except asyncio.CancelledError:
            break


__all__ = [
    "SmartWalletMeta",
    "SmartWalletRegistry",
    "SmartWalletTracker",
    "REGISTRY",
    "TRACKER",
    "smart_wallets",
    "build_risk_ctx_smart",
    "pick_reinvestment_target",
    "get_wallet_state",
    "smart_wallets_loop",
]


if __name__ == "__main__":  # pragma: no cover
    async def _demo() -> None:
        REGISTRY.register_wallet("wallet_smart_1", cluster="ct_alpha_snipers", weight=1.5)
        await TRACKER.observe_swap(mint="DEMO_MINT", trader="wallet_smart_1", amount_usd=1500.0, side="buy")
        await TRACKER.observe_swap(mint="DEMO_MINT", trader="wallet_whale_1", amount_usd=12000.0, side="buy")

        feats = await TRACKER.build_features("DEMO_MINT")
        desc = await TRACKER.describe_mint("DEMO_MINT")
        print("features:", {k: round(v, 3) for k, v in feats.items()})
        print("desc:", desc)

        universe = {"DEMO_MINT": {"volume_24h_usd": 1_000_000}}
        target = await pick_reinvestment_target(0.5, min_24h_volume_usd=10_000, universe=universe)
        print("pick_reinvestment_target:", target)

        state = await get_wallet_state()
        print("get_wallet_state:", state)

    asyncio.run(_demo())
