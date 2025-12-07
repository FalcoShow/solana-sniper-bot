from __future__ import annotations

"""
utils_pkg/smart_wallets.py  —  V3+++ / quant-grade

Smart wallet & whale orderflow engine.

Core responsibilities
---------------------
  • Maintain a registry of "smart" and "anti" (exit/dumb) wallets.
  • Track decayed smart/whale/breadth activity per mint:
        - Direction (net buying vs selling)
        - Intensity (how many events)
        - Recency (timestamps)
        - Cluster quality (alpha vs generic)

  • Produce a compact feature vector for:
        - FeatureBuilder (wallet_signals)
        - ai_entry_logic / ai_buy_scoring
        - regime engine
        - self_learning_ai_strategy / risk_manager (via risk:ctx)
        - dashboards / diagnostics

Key outputs (per mint)
----------------------
Smart flow:
  - smart_wallet_score          0..1 (0.5 ~ neutral; >0.5 net smart buy)
  - smart_flow_raw              signed, -max..+max
  - smart_last_buy_sec_ago      seconds since last smart buy
  - smart_last_sell_sec_ago     seconds since last smart sell
  - smart_last_cluster_is_alpha 0/1

Smart side & breadth:
  - smart_wallet_count_5m       decayed smart events
  - smart_buy_score_5m          0..1
  - smart_sell_score_5m         0..1
  - smart_net_side_5m           -1..+1
  - smart_unique_buyers_5m      decayed unique smart addresses

Whales:
  - whales_5m                   decayed whale trades
  - whale_vol_5m_usd            decayed whale volume
  - whale_buy_score             0..1
  - whale_sell_score            0..1
  - whale_net_side_5m           -1..+1

Breadth:
  - unique_buyers_5m            decayed unique buyers
  - whale_share_5m              0..1 (whales / uniques)
  - holders_growth_5m           ~0..0.5 proxy from unique buyers

Quant-grade / V3+++ extras:
  - smart_alpha_bias            0..1  (alpha cluster bias)
  - smart_whale_alignment       -1..+1
  - smart_whale_alignment_abs   0..1
  - whale_domination_5m         0..1
  - smart_vs_whale_conflict     0..1
  - smart_accumulation_score    0..1
  - smart_panic_dump_score      0..1
  - whale_liquidity_footprint   0..1

Meta signals for AI:
  - smart_signal_confidence     0..1
  - smart_signal_quality        0..1
  - smart_risk_flag             0..1

V3+++ meta convenience:
  - smart_recent_side           -1 / 0 / +1
  - whale_recent_side           -1 / 0 / +1
  - smart_crowding_score        0..1  (smart uniques / uniques)
  - smart_signal_state          -1..+1 (compressed smart side * quality * conf)

Heat scores:
  - smart_heat_score            0..1
  - whale_heat_score            0..1

Extra “edge” helpers:
  - smart_whale_pressure_ratio  0..1  (whale_heat / (smart_heat + whale_heat))
  - smart_entry_boost_hint      -1..+1 (directional hint for entry logic)
  - smart_exit_pressure_hint    0..1  (panic/exit risk from smart+whales)

Module-level extras for V3+++
-----------------------------
  - SmartWalletRegistry  → static mapping of smart/anti wallets (with clusters)
  - SmartWalletTracker   → in-memory decayed stats per mint
        • observe_swap / observe_swap_event / bulk_observe_swap_events
        • build_features / attach_features_to_meta
        • entry_hint(mint) / exit_hint(mint)
        • snapshot(mint) / top_active_mints()
        • global_regime_hint()  ← compact global hint for risk:ctx/regime
        • describe_mint(mint)   – human-readable one-liner for logs / Telegram
        • reset_all() / clear_mint() for debugging / tests

Public module API
-----------------
  - SmartWalletMeta
  - SmartWalletRegistry
  - SmartWalletTracker
  - REGISTRY
  - TRACKER
  - smart_wallets
  - pick_reinvestment_target   ← used by reinvestor.py
"""

import asyncio
import json
import os
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

from logging_setup import get_logger

log = get_logger("sniper.smart_wallets")


# =============================================================================
# Env helpers / config
# =============================================================================


def _env_json(key: str) -> Any:
    raw = os.getenv(key, "").strip()
    if not raw:
        return None
    try:
        return json.loads(raw)
    except Exception as e:
        if hasattr(log, "event"):
            log.event("SMART_ENV_JSON_FAIL", key=key, error=str(e)[:200])
        else:
            log.warning("SMART_ENV_JSON_FAIL key=%s error=%s", key, str(e)[:200])
        return None


def _env_list(key: str) -> List[str]:
    raw = os.getenv(key, "").strip()
    if not raw:
        return []
    out: List[str] = []
    for chunk in raw.replace("\n", ",").split(","):
        c = chunk.strip()
        if c:
            out.append(c)
    return out


def _env_set(key: str) -> set[str]:
    """Small helper for "only" / "exclude" lists from env."""
    return set(_env_list(key))


def _env_float(key: str, default: float) -> float:
    try:
        return float(os.getenv(key, str(default)))
    except Exception:
        return default


def _env_bool(key: str, default: bool) -> bool:
    v = os.getenv(key)
    if v is None:
        return default
    v = v.strip().lower()
    if v in ("1", "true", "yes", "y", "on"):
        return True
    if v in ("0", "false", "no", "n", "off"):
        return False
    return default


def _decay_factor(dt: float, half_life: float) -> float:
    """
    Standard exponential half-life decay factor.
    """
    if half_life <= 0.0 or dt <= 0.0:
        return 1.0
    return 0.5 ** (dt / half_life)


def _to_01_from_count(count: float, scale: float) -> float:
    """
    Soft clipping of a non-negative count into [0, 1].
    """
    if scale <= 0:
        scale = 1.0
    x = max(0.0, count) / scale
    if x > 1.0:
        x = 1.0
    return float(x)


def _net_minus_one_to_one(pos: float, neg: float) -> float:
    """
    Convert (positive_count, negative_count) → net in [-1, 1].
    """
    p = max(0.0, pos)
    n = max(0.0, neg)
    denom = p + n
    if denom <= 0:
        return 0.0
    net = (p - n) / denom
    if net > 1.0:
        net = 1.0
    if net < -1.0:
        net = -1.0
    return float(net)


def _decay_update(
    value: float,
    last_ts: float,
    now: float,
    half_life: float,
    *,
    increment: float = 0.0,
) -> float:
    """
    Generic "decay then add" helper used across smart / whale / breadth stats.
    """
    dt = max(0.0, now - last_ts)
    decayed = value * _decay_factor(dt, half_life)
    return decayed + increment


# ---- global mode switch (V3+++) --------------------------------------------

# SMART_WALLET_MODE can be:
#   "off"        → completely disable signal (features ~ neutral)
#   "simple"     → only basic counts/sides, no heavy extras (future hook)
#   "alpha"      → alpha/cluster-driven only
#   "full"       → everything enabled (default)
SMART_WALLET_MODE = os.getenv("SMART_WALLET_MODE", "full").strip().lower()
if SMART_WALLET_MODE not in {"off", "simple", "alpha", "full"}:
    SMART_WALLET_MODE = "full"

# ---- feature toggles (for ablation / safe mode) ----------------------------

SMART_WALLET_ENABLED = _env_bool("SMART_WALLET_ENABLED", SMART_WALLET_MODE != "off")
SMART_WHALE_ENABLED = _env_bool(
    "SMART_WALLET_WHALE_ENABLED", SMART_WALLET_MODE != "off"
)
SMART_BREADTH_ENABLED = _env_bool(
    "SMART_WALLET_BREADTH_ENABLED", SMART_WALLET_MODE != "off"
)

# Optional alpha-only mode (treat only alpha clusters as signal)
SMART_ALPHA_ONLY_MODE = _env_bool(
    "SMART_WALLET_ALPHA_ONLY_MODE", SMART_WALLET_MODE == "alpha"
)

# If smart signal strength is extremely low, we fade some secondary features
SMART_MIN_SCORE_FOR_RICH_FEATURES = _env_float(
    "SMART_WALLET_MIN_SCORE_FOR_RICH_FEATURES", 0.05
)

# Cluster-level JSON overrides for weights (fine-tuning without redeploy)
# Example:
# SMART_WALLET_CLUSTER_WEIGHTS_JSON='{"ct_alpha_snipers":1.8,"exit_farm":-1.2}'
SMART_CLUSTER_WEIGHTS = _env_json("SMART_WALLET_CLUSTER_WEIGHTS_JSON") or {}

# ---- core knobs for smart-wallet behaviour ---------------------------------

SMART_WALLET_DECAY_HALF_LIFE_SEC = _env_float(
    "SMART_WALLET_DECAY_HALF_LIFE_SEC", 900.0  # ~15m half-life
)
SMART_WALLET_MAX_SCORE = _env_float("SMART_WALLET_MAX_SCORE", 1.2)

SMART_WALLET_MIN_NOTIONAL_USD = _env_float(
    "SMART_WALLET_MIN_NOTIONAL_USD", 50.0
)
SMART_WALLET_MAX_CONTRIB_PER_TRADE = _env_float(
    "SMART_WALLET_MAX_CONTRIB_PER_TRADE", 1.0
)
SMART_WALLET_SELL_MULTIPLIER = _env_float(
    "SMART_WALLET_SELL_MULTIPLIER", 0.5
)

# Whale detection / tracking
WHALE_MIN_NOTIONAL_USD = _env_float(
    "SMART_WALLET_WHALE_MIN_NOTIONAL_USD", 1_000.0
)
WHALE_MIN_NOTIONAL_SOL = _env_float(
    "SMART_WALLET_WHALE_MIN_NOTIONAL_SOL", 5.0
)
WHALE_HALF_LIFE_SEC = _env_float("SMART_WALLET_WHALE_HALF_LIFE_SEC", 300.0)

# Unique buyer breadth
UNIQUE_ADDR_MIN_GAP_SEC = _env_float(
    "SMART_WALLET_UNIQUE_ADDR_MIN_GAP_SEC", 60.0
)

# Soft mapping scales → scores
SMART_EVENT_SCALE = _env_float("SMART_WALLET_COUNT_SCORE_SCALE", 5.0)
WHALE_SIDE_SCALE = _env_float("SMART_WALLET_WHALE_SIDE_SCORE_SCALE", 5.0)
UNIQUE_BUYERS_SCALE = _env_float("SMART_WALLET_UNIQUE_BUYERS_SCALE", 10.0)
SMART_UNIQUE_BUYERS_SCALE = _env_float(
    "SMART_WALLET_SMART_UNIQUE_BUYERS_SCALE", 6.0
)

# Domination / alignment knobs
WHALE_DOM_THRESHOLD = _env_float(
    "SMART_WALLET_WHALE_DOM_THRESHOLD", 0.7
)  # share where domination "starts"
SMART_WHALE_CONFLICT_NET_MIN = _env_float(
    "SMART_WALLET_CONFLICT_NET_MIN", 0.4
)  # min |side| for conflict to count

# Quant-extras: how aggressive accumulation/dump scores can be
SMART_ACCUM_ALPHA_SCALE = _env_float(
    "SMART_WALLET_ACCUM_ALPHA_SCALE", 1.0
)
SMART_PANIC_DUMP_SCALE = _env_float(
    "SMART_WALLET_PANIC_DUMP_SCALE", 1.0
)
WHALE_FOOTPRINT_ANCHOR_USD = _env_float(
    "SMART_WALLET_WHALE_FOOTPRINT_ANCHOR_USD", 50_000.0
)

# Meta signal controls
SMART_SIGNAL_RECENCY_SEC = _env_float(
    "SMART_WALLET_SIGNAL_RECENCY_SEC", 900.0  # last 15 min
)
SMART_SIGNAL_MIN_EVENTS_FOR_CONF = _env_float(
    "SMART_WALLET_SIGNAL_MIN_EVENTS_FOR_CONF", 3.0
)

SMART_DEBUG_LOG = _env_bool("SMART_WALLET_DEBUG_LOG", False)

# Memory / prune controls (prevent unbounded growth in long-running bot)
SMART_WALLET_MAX_MINTS = int(_env_float("SMART_WALLET_MAX_MINTS", 4000))
SMART_WALLET_IDLE_PRUNE_SEC = _env_float(
    "SMART_WALLET_IDLE_PRUNE_SEC", 7200.0  # 2h idle → prune
)

# Clusters treated as "alpha" even if the name doesn't contain 'alpha'
ALPHA_CLUSTER_NAMES = set(
    n.lower() for n in _env_list("SMART_WALLET_ALPHA_CLUSTERS")
)

# Tiny per-mint feature cache to avoid recomputing 10x in a single tick
SMART_FEATURE_CACHE_SEC = _env_float(
    "SMART_WALLET_FEATURE_CACHE_SEC", 0.5
)  # 0 = off

# Reinvestment / profile tuning (V3+++)
SMART_REINVEST_EXCLUDE_MINTS = _env_set("SMART_WALLET_REINVEST_EXCLUDE_MINTS")
SMART_REINVEST_ONLY_MINTS = _env_set("SMART_WALLET_REINVEST_ONLY_MINTS")

REINVEST_MAX_PANIC = _env_float("SMART_WALLET_REINVEST_MAX_PANIC", 0.65)
REINVEST_MAX_RISK = _env_float("SMART_WALLET_REINVEST_MAX_RISK", 0.7)
REINVEST_MAX_CONFLICT = _env_float("SMART_WALLET_REINVEST_MAX_CONFLICT", 0.75)


# =============================================================================
# Registry
# =============================================================================


@dataclass(frozen=True)
class SmartWalletMeta:
    cluster: str
    weight: float  # -2..+2 (negative weight = "anti" / exit wallet)
    is_anti: bool = False
    half_life_override_sec: Optional[float] = None  # per-cluster decay override


class SmartWalletRegistry:
    """
    Static mapping: wallet address → SmartWalletMeta.

    Sources (env):
      SMART_WALLET_CLUSTERS_JSON      – JSON clusters of smart wallets
      SMART_WALLET_LIST               – comma/newline list of smart wallets
      SMART_WALLET_ANTI_CLUSTERS_JSON – JSON clusters of anti wallets
      SMART_WALLET_ANTI_LIST          – list of anti wallets

    JSON format example (smart side):
      {
        "ct_alpha_snipers": {
          "addresses": ["addr1", "addr2"],
          "weight": 1.5,
          "half_life_sec": 1200
        },
        "ct_exit_farm": {
          "wallets": ["addr3"],
          "weight": -1.5
        }
      }

    V3+++ extras:
      • Dynamic runtime register/remove APIs for tuning tools / web UI.
      • cluster_for_addr / weight_for_addr helpers for diagnostics / AI.
    """

    def __init__(self) -> None:
        self._by_addr: Dict[str, SmartWalletMeta] = {}
        self._load_from_env()

    # ---- loading helpers -------------------------------------------------

    @staticmethod
    def _normalize_addr(addr: str) -> str:
        # Leave case as-is, but trim whitespace.
        return addr.strip()

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
                weight = max(-2.0, min(2.0, weight))

                # Optional overrides via SMART_CLUSTER_WEIGHTS
                cname_key = str(cluster_name or default_cluster).strip()
                if cname_key in SMART_CLUSTER_WEIGHTS:
                    try:
                        w_override = float(SMART_CLUSTER_WEIGHTS[cname_key])
                        weight = max(-2.0, min(2.0, w_override))
                    except Exception:
                        pass

                half_life = spec.get("half_life_sec")
                try:
                    hl: Optional[float] = (
                        float(half_life) if half_life is not None else None
                    )
                except Exception:
                    hl = None

                c_name = cname_key or default_cluster
                for addr in addrs:
                    addr_norm = self._normalize_addr(str(addr))
                    if not addr_norm:
                        continue
                    self._by_addr[addr_norm] = SmartWalletMeta(
                        cluster=c_name,
                        weight=weight,
                        is_anti=is_anti,
                        half_life_override_sec=hl,
                    )
            except Exception as e:
                if hasattr(log, "event"):
                    log.event(
                        "SMART_REGISTRY_CLUSTER_FAIL",
                        cluster=str(cluster_name),
                        error=str(e)[:200],
                    )
                else:
                    log.warning(
                        "SMART_REGISTRY_CLUSTER_FAIL cluster=%s error=%s",
                        cluster_name,
                        str(e)[:200],
                    )

    def _load_flat_list(
        self,
        addrs: List[str],
        *,
        is_anti: bool,
        default_cluster: str,
        weight: float,
    ) -> None:
        weight = max(-2.0, min(2.0, weight))
        for addr in addrs:
            addr_norm = self._normalize_addr(addr)
            if not addr_norm:
                continue
            # JSON definitions win.
            if addr_norm in self._by_addr:
                continue
            self._by_addr[addr_norm] = SmartWalletMeta(
                cluster=default_cluster,
                weight=weight,
                is_anti=is_anti,
                half_life_override_sec=None,
            )

    def _load_from_env(self) -> None:
        clusters = _env_json("SMART_WALLET_CLUSTERS_JSON")
        flat_list = _env_list("SMART_WALLET_LIST")
        anti_clusters = _env_json("SMART_WALLET_ANTI_CLUSTERS_JSON")
        anti_flat = _env_list("SMART_WALLET_ANTI_LIST")

        # smart side
        self._load_clusters_from_json(
            clusters,
            is_anti=False,
            default_cluster="smart_default",
        )
        if flat_list:
            self._load_flat_list(
                flat_list,
                is_anti=False,
                default_cluster="smart_default",
                weight=1.0,
            )

        # anti / exit side
        self._load_clusters_from_json(
            anti_clusters,
            is_anti=True,
            default_cluster="anti_default",
        )
        if anti_flat:
            self._load_flat_list(
                anti_flat,
                is_anti=True,
                default_cluster="anti_default",
                weight=-1.0,
            )

        if hasattr(log, "event"):
            log.event("SMART_REGISTRY_LOADED", count=len(self._by_addr))
        else:
            log.info("SMART_REGISTRY_LOADED count=%d", len(self._by_addr))

    # ---- dynamic runtime mutation (quant-grade tuning tools) -------------

    def register_wallet(
        self,
        address: str,
        *,
        cluster: str = "runtime_smart",
        weight: float = 1.0,
        is_anti: bool = False,
        half_life_override_sec: Optional[float] = None,
    ) -> None:
        """
        Add or update a wallet definition at runtime (e.g., from a tuning tool
        or web UI) without redeploying.
        """
        addr_norm = self._normalize_addr(address)
        if not addr_norm:
            return
        weight = max(-2.0, min(2.0, float(weight)))
        self._by_addr[addr_norm] = SmartWalletMeta(
            cluster=cluster or "runtime_smart",
            weight=weight,
            is_anti=is_anti,
            half_life_override_sec=half_life_override_sec,
        )

    def remove_wallet(self, address: str) -> None:
        addr_norm = self._normalize_addr(address)
        if addr_norm in self._by_addr:
            self._by_addr.pop(addr_norm, None)

    # ---- public query API ------------------------------------------------

    def is_smart(self, address: str) -> bool:
        meta = self._by_addr.get(self._normalize_addr(address))
        return bool(meta and meta.weight > 0)

    def is_anti(self, address: str) -> bool:
        meta = self._by_addr.get(self._normalize_addr(address))
        return bool(meta and meta.weight < 0)

    def get_meta(self, address: str) -> Optional[SmartWalletMeta]:
        return self._by_addr.get(self._normalize_addr(address))

    def cluster_for_addr(self, address: str) -> Optional[str]:
        """
        Convenience: return cluster label for diagnostics / dashboards.
        """
        meta = self.get_meta(address)
        return meta.cluster if meta else None

    def weight_for_addr(self, address: str) -> Optional[float]:
        """
        Convenience: return raw weight (negative = exit/anti wallet).
        """
        meta = self.get_meta(address)
        return meta.weight if meta else None

    def summary(self) -> Dict[str, Any]:
        clusters: Dict[str, int] = {}
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
            "mode": SMART_WALLET_MODE,
        }


REGISTRY = SmartWalletRegistry()


# =============================================================================
# Smart wallet & whale tracker
# =============================================================================


class SmartWalletTracker:
    """
    Tracks recent smart-wallet + whale activity per mint with exponential decay.

    All state is in-memory and cheap. No network calls here.

    V3+++ extras:
      • top_active_mints() to feed regime / dashboards.
      • entry_hint() and exit_hint() for ai_entry_logic / ai_sell_logic.
      • describe_mint() for human-readable diagnostics / Telegram debug.
      • global_regime_hint() for risk_manager / self_learning_ai_strategy.
      • reset_all() / clear_mint() for tests and live debugging.
    """

    def __init__(self) -> None:
        # smart wallet flow (signed)
        self._scores: Dict[str, float] = {}
        self._last_ts: Dict[str, float] = {}
        self._last_buy_ts: Dict[str, float] = {}
        self._last_sell_ts: Dict[str, float] = {}
        self._last_cluster: Dict[str, str] = {}

        # smart event count (for smart_wallet_count_5m)
        self._smart_event_score: Dict[str, float] = {}
        self._smart_event_last_ts: Dict[str, float] = {}

        # side-specific smart events
        self._smart_buy_count: Dict[str, float] = {}
        self._smart_sell_count: Dict[str, float] = {}
        self._smart_side_last_ts: Dict[str, float] = {}

        # whale counts & volume
        self._whale_count: Dict[str, float] = {}
        self._whale_volume: Dict[str, float] = {}
        self._whale_last_ts: Dict[str, float] = {}

        # whale directional flow
        self._whale_buy_count: Dict[str, float] = {}
        self._whale_sell_count: Dict[str, float] = {}
        self._whale_side_last_ts: Dict[str, float] = {}

        # breadth: per-mint → {addr: last_ts}
        self._addr_last_seen: Dict[str, Dict[str, float]] = {}
        self._unique_score: Dict[str, float] = {}
        self._unique_last_ts: Dict[str, float] = {}

        # smart-unique buyers (subset of above)
        self._smart_addr_last_seen: Dict[str, Dict[str, float]] = {}
        self._smart_unique_score: Dict[str, float] = {}
        self._smart_unique_last_ts: Dict[str, float] = {}

        # internal bookkeeping for pruning
        self._last_any_activity_ts: Dict[str, float] = {}
        self._op_counter: int = 0

        # small feature cache: mint → (ts, feats)
        self._feature_cache: Dict[str, Tuple[float, Dict[str, float]]] = {}

        # lock to keep updates consistent
        self._lock = asyncio.Lock()

    # ---- basic maintenance ----------------------------------------------

    async def clear_mint(self, mint: str) -> None:
        """
        Clear all state for a single mint (useful in tests or emergency).
        """
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
        """
        Hard reset of the tracker (e.g. in tests / full restart scenarios).
        """
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

    # ---- helpers --------------------------------------------------------

    @staticmethod
    def _normalize_notional(
        amount_usd: Optional[float],
        amount_sol: Optional[float],
    ) -> float:
        """
        Normalize trade size into a rough USD notional.
        """
        amt = 0.0
        try:
            if amount_usd is not None:
                amt = float(amount_usd)
            elif amount_sol is not None:
                # Very rough proxy; we only need order-of-magnitude.
                amt = float(amount_sol) * 100.0
        except Exception:
            amt = 0.0
        if amt < 0.0:
            amt = 0.0
        return amt

    @staticmethod
    def _smart_contribution_from_notional(amt_usd: float) -> float:
        """
        Map trade size → smart contribution weight for this event.
        """
        if amt_usd <= 0:
            return 0.0
        if amt_usd < SMART_WALLET_MIN_NOTIONAL_USD:
            # Small but non-trivial; taper to avoid noise.
            return 0.1 * (amt_usd / max(SMART_WALLET_MIN_NOTIONAL_USD, 1.0))
        val = amt_usd / 5_000.0
        if val > SMART_WALLET_MAX_CONTRIB_PER_TRADE:
            val = SMART_WALLET_MAX_CONTRIB_PER_TRADE
        if val < 0.0:
            val = 0.0
        return val

    @staticmethod
    def _is_whale(amt_usd: float, amt_sol: Optional[float]) -> bool:
        if amt_usd >= WHALE_MIN_NOTIONAL_USD:
            return True
        if amt_sol is not None and amt_sol >= WHALE_MIN_NOTIONAL_SOL:
            return True
        return False

    def _touch_mint(self, mint: str, now: float) -> None:
        self._last_any_activity_ts[mint] = now
        self._op_counter += 1
        # Invalidate per-mint feature cache on mutation
        self._feature_cache.pop(mint, None)

    def _maybe_prune(self, now: float) -> None:
        """
        Best-effort pruning of very old mints to avoid unbounded memory.
        Called opportunistically inside lock on some operations.
        """
        if SMART_WALLET_MAX_MINTS <= 0:
            return
        # cheap throttle
        if self._op_counter % 500 != 0:
            return

        mints = list(self._last_any_activity_ts.keys())
        if len(mints) <= SMART_WALLET_MAX_MINTS:
            return

        idle_threshold = now - SMART_WALLET_IDLE_PRUNE_SEC
        stale = [
            m for m in mints
            if self._last_any_activity_ts.get(m, 0.0) < idle_threshold
        ]
        if not stale:
            return

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

        if hasattr(log, "event"):
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

    # ---- core update: observe swap --------------------------------------

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
        Ingest a single swap.

        Parameters
        ----------
        mint : str
            Token mint.
        trader : str
            Trader wallet address.
        amount_usd : float | None
            Approx notional in USD (preferred).
        amount_sol : float | None
            Notional in SOL (fallback).
        side : {"buy","sell"}
            From the trader's perspective.
        """
        mint = (mint or "").strip()
        trader = (trader or "").strip()
        if not mint or not trader:
            return

        now = time.time()
        side_str = str(side).lower().strip()
        is_buy = side_str.startswith("b")
        is_sell = side_str.startswith("s")

        amt_usd = self._normalize_notional(amount_usd, amount_sol)
        is_whale = self._is_whale(amt_usd, amount_sol)

        meta = REGISTRY.get_meta(trader) if SMART_WALLET_ENABLED else None

        async with self._lock:
            self._touch_mint(mint, now)

            # ---- smart wallet contribution --------------------------------
            if meta is not None and SMART_WALLET_ENABLED:
                # Optional alpha-only mode:
                # treat non-alpha clusters as no-op signal (still tracked as whales).
                if SMART_ALPHA_ONLY_MODE:
                    cl = (meta.cluster or "").lower()
                    if (
                        "alpha" not in cl
                        and "smart" not in cl
                        and cl not in ALPHA_CLUSTER_NAMES
                    ):
                        meta_for_use: Optional[SmartWalletMeta] = None
                    else:
                        meta_for_use = meta
                else:
                    meta_for_use = meta

                if meta_for_use is not None:
                    contrib_norm = self._smart_contribution_from_notional(amt_usd)
                    if contrib_norm > 0.0:
                        old_score = self._scores.get(mint, 0.0)
                        last_ts = self._last_ts.get(mint, now)

                        half_life = (
                            meta_for_use.half_life_override_sec
                            or SMART_WALLET_DECAY_HALF_LIFE_SEC
                        )
                        new_score = _decay_update(
                            old_score,
                            last_ts,
                            now,
                            half_life,
                            increment=0.0,
                        )

                        # direction depends on wallet weight and side
                        base_w = meta_for_use.weight
                        if base_w >= 0:
                            # regular smart wallet
                            if is_buy or not (is_buy or is_sell):
                                direction = 1.0
                            else:
                                direction = -SMART_WALLET_SELL_MULTIPLIER
                        else:
                            # anti wallet: invert semantics
                            if is_buy or not (is_buy or is_sell):
                                direction = -abs(base_w)
                            else:
                                direction = abs(base_w) * SMART_WALLET_SELL_MULTIPLIER

                        contrib = contrib_norm * direction
                        new_score += contrib

                        max_abs = max(SMART_WALLET_MAX_SCORE, 0.1)
                        if new_score > max_abs:
                            new_score = max_abs
                        if new_score < -max_abs:
                            new_score = -max_abs

                        self._scores[mint] = new_score
                        self._last_ts[mint] = now
                        self._last_cluster[mint] = meta_for_use.cluster
                        if is_buy or not (is_buy or is_sell):
                            self._last_buy_ts[mint] = now
                        elif is_sell:
                            self._last_sell_ts[mint] = now

                        # decayed smart event count (for smart_wallet_count_5m)
                        ev_old = self._smart_event_score.get(mint, 0.0)
                        ev_last_ts = self._smart_event_last_ts.get(mint, now)
                        ev_new = _decay_update(
                            ev_old,
                            ev_last_ts,
                            now,
                            SMART_WALLET_DECAY_HALF_LIFE_SEC,
                            increment=1.0,
                        )
                        self._smart_event_score[mint] = ev_new
                        self._smart_event_last_ts[mint] = now

                        # side-specific smart events
                        sb_old = self._smart_buy_count.get(mint, 0.0)
                        ss_old = self._smart_sell_count.get(mint, 0.0)
                        side_last_ts = self._smart_side_last_ts.get(mint, now)
                        sb = _decay_update(
                            sb_old,
                            side_last_ts,
                            now,
                            SMART_WALLET_DECAY_HALF_LIFE_SEC,
                            increment=1.0 if (is_buy or not (is_buy or is_sell)) else 0.0,
                        )
                        ss = _decay_update(
                            ss_old,
                            side_last_ts,
                            now,
                            SMART_WALLET_DECAY_HALF_LIFE_SEC,
                            increment=1.0 if is_sell else 0.0,
                        )
                        self._smart_buy_count[mint] = sb
                        self._smart_sell_count[mint] = ss
                        self._smart_side_last_ts[mint] = now

                        # smart-unique buyers (breadth of smart flow)
                        addr_map = self._smart_addr_last_seen.setdefault(mint, {})
                        last_seen_smart = addr_map.get(trader, 0.0)
                        addr_map[trader] = now
                        is_new_smart_identity = (
                            now - last_seen_smart
                        ) >= UNIQUE_ADDR_MIN_GAP_SEC

                        su_old = self._smart_unique_score.get(mint, 0.0)
                        su_last_ts = self._smart_unique_last_ts.get(mint, now)
                        if is_new_smart_identity:
                            su_new = _decay_update(
                                su_old,
                                su_last_ts,
                                now,
                                WHALE_HALF_LIFE_SEC,
                                increment=1.0,
                            )
                        else:
                            su_new = _decay_update(
                                su_old,
                                su_last_ts,
                                now,
                                WHALE_HALF_LIFE_SEC,
                                increment=0.0,
                            )
                        self._smart_unique_score[mint] = su_new
                        self._smart_unique_last_ts[mint] = now

                        if SMART_DEBUG_LOG and hasattr(log, "event"):
                            log.event(
                                "SMART_SWAP_OBSERVED",
                                mint=mint,
                                trader=trader,
                                cluster=meta_for_use.cluster,
                                weight=meta_for_use.weight,
                                is_anti=meta_for_use.is_anti,
                                amt_usd=amt_usd,
                                side=side_str,
                                contrib=contrib,
                                score=new_score,
                            )

            # ---- whale tracking -------------------------------------------
            if SMART_WHALE_ENABLED and is_whale:
                wc_old = self._whale_count.get(mint, 0.0)
                wv_old = self._whale_volume.get(mint, 0.0)
                wc_last_ts = self._whale_last_ts.get(mint, now)

                wc_new = _decay_update(
                    wc_old,
                    wc_last_ts,
                    now,
                    WHALE_HALF_LIFE_SEC,
                    increment=1.0,
                )
                wv_new = _decay_update(
                    wv_old,
                    wc_last_ts,
                    now,
                    WHALE_HALF_LIFE_SEC,
                    increment=amt_usd,
                )

                self._whale_count[mint] = wc_new
                self._whale_volume[mint] = wv_new
                self._whale_last_ts[mint] = now

                # directional whales
                wb_old = self._whale_buy_count.get(mint, 0.0)
                ws_old = self._whale_sell_count.get(mint, 0.0)
                ws_last_ts = self._whale_side_last_ts.get(mint, now)

                wb_inc = 1.0 if (is_buy or not (is_buy or is_sell)) else 0.0
                ws_inc = 1.0 if is_sell else 0.0

                wb = _decay_update(
                    wb_old,
                    ws_last_ts,
                    now,
                    WHALE_HALF_LIFE_SEC,
                    increment=wb_inc,
                )
                ws = _decay_update(
                    ws_old,
                    ws_last_ts,
                    now,
                    WHALE_HALF_LIFE_SEC,
                    increment=ws_inc,
                )

                self._whale_buy_count[mint] = wb
                self._whale_sell_count[mint] = ws
                self._whale_side_last_ts[mint] = now

            # ---- unique buyer breadth ------------------------------------
            if SMART_BREADTH_ENABLED:
                addr_map_all = self._addr_last_seen.setdefault(mint, {})
                last_seen_all = addr_map_all.get(trader, 0.0)
                addr_map_all[trader] = now
                is_new_identity = (now - last_seen_all) >= UNIQUE_ADDR_MIN_GAP_SEC

                u_old = self._unique_score.get(mint, 0.0)
                u_last_ts = self._unique_last_ts.get(mint, now)

                if is_new_identity:
                    u_new = _decay_update(
                        u_old,
                        u_last_ts,
                        now,
                        WHALE_HALF_LIFE_SEC,
                        increment=1.0,
                    )
                else:
                    u_new = _decay_update(
                        u_old,
                        u_last_ts,
                        now,
                        WHALE_HALF_LIFE_SEC,
                        increment=0.0,
                    )

                self._unique_score[mint] = u_new
                self._unique_last_ts[mint] = now

            # prune occasionally
            self._maybe_prune(now)

    # ---- read API -------------------------------------------------------

    async def _get_decayed_smart_score(
        self,
        mint: str,
        now: Optional[float] = None,
    ) -> float:
        if now is None:
            now = time.time()
        async with self._lock:
            old_score = self._scores.get(mint, 0.0)
            last_ts = self._last_ts.get(mint, now)
            new_score = _decay_update(
                old_score,
                last_ts,
                now,
                SMART_WALLET_DECAY_HALF_LIFE_SEC,
                increment=0.0,
            )
            self._scores[mint] = new_score
            self._last_ts[mint] = now
        return float(new_score)

    async def get_raw_score(self, mint: str) -> float:
        """
        Signed smart-flow score (negative = anti/sellers dominate).
        """
        if not SMART_WALLET_ENABLED or SMART_WALLET_MODE == "off":
            return 0.0
        return await self._get_decayed_smart_score(mint)

    async def get_score_01(self, mint: str) -> float:
        """
        0–1 smart wallet score:
          ~0.5 = neutral
          >0.5 = net smart buying / anti selling
          <0.5 = net smart selling / anti buying
        """
        raw = await self.get_raw_score(mint)
        max_abs = max(SMART_WALLET_MAX_SCORE, 1e-6)
        x = max(-max_abs, min(max_abs, raw)) / max_abs  # [-1, 1]
        return 0.5 * (x + 1.0)

    async def _maybe_cached_features(
        self,
        mint: str,
        now: float,
    ) -> Optional[Dict[str, float]]:
        if SMART_FEATURE_CACHE_SEC <= 0:
            return None
        async with self._lock:
            item = self._feature_cache.get(mint)
        if not item:
            return None
        ts, feats = item
        if now - ts > SMART_FEATURE_CACHE_SEC:
            return None
        return feats

    async def _store_cached_features(
        self,
        mint: str,
        feats: Dict[str, float],
        now: float,
    ) -> None:
        if SMART_FEATURE_CACHE_SEC <= 0:
            return
        async with self._lock:
            self._feature_cache[mint] = (now, feats)

    async def build_features(self, mint: str) -> Dict[str, float]:
        """
        Build wallet_signals for a given mint.

        See module docstring for full list of keys.
        """
        now = time.time()

        # tiny cache for "same tick" multiple consumers (entry, exit, logging)
        cached = await self._maybe_cached_features(mint, now)
        if cached is not None:
            return dict(cached)

        # headline smart score
        if not SMART_WALLET_ENABLED or SMART_WALLET_MODE == "off":
            raw_smart = 0.0
            smart_01 = 0.5
        else:
            raw_smart = await self._get_decayed_smart_score(mint, now=now)
            max_abs = max(SMART_WALLET_MAX_SCORE, 1e-6)
            x = max(-max_abs, min(max_abs, raw_smart)) / max_abs  # [-1, 1]
            smart_01 = 0.5 * (x + 1.0)

        # snapshot all internal state under lock
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

        # smart events → decayed count
        dt_ev = max(0.0, now - ev_last_ts)
        smart_events_decayed = ev_score * _decay_factor(
            dt_ev, SMART_WALLET_DECAY_HALF_LIFE_SEC
        )
        smart_wallet_count_5m = smart_events_decayed

        # side-specific smart buy/sell scores
        dt_s = max(0.0, now - smart_side_last_ts)
        decay_s = _decay_factor(dt_s, SMART_WALLET_DECAY_HALF_LIFE_SEC)
        sb_decayed = sb * decay_s
        ss_decayed = ss * decay_s
        smart_buy_score_5m = _to_01_from_count(sb_decayed, SMART_EVENT_SCALE)
        smart_sell_score_5m = _to_01_from_count(ss_decayed, SMART_EVENT_SCALE)
        smart_net_side_5m = _net_minus_one_to_one(sb_decayed, ss_decayed)

        # whales
        dt_wc = max(0.0, now - wc_last_ts)
        decay_wc = _decay_factor(dt_wc, WHALE_HALF_LIFE_SEC)
        whales_5m = wc * decay_wc
        whale_vol_5m_usd = wv * decay_wc

        # directional whale flow
        dt_side = max(0.0, now - ws_last_ts)
        decay_side = _decay_factor(dt_side, WHALE_HALF_LIFE_SEC)
        wb_decayed = wb * decay_side
        ws_decayed = ws * decay_side
        whale_buy_score = _to_01_from_count(wb_decayed, WHALE_SIDE_SCALE)
        whale_sell_score = _to_01_from_count(ws_decayed, WHALE_SIDE_SCALE)
        whale_net_side_5m = _net_minus_one_to_one(wb_decayed, ws_decayed)

        # unique buyers (all)
        dt_u = max(0.0, now - u_last_ts)
        decay_u = _decay_factor(dt_u, WHALE_HALF_LIFE_SEC)
        unique_buyers_5m = u_score * decay_u

        # smart unique buyers
        dt_su = max(0.0, now - su_last_ts)
        decay_su = _decay_factor(dt_su, WHALE_HALF_LIFE_SEC)
        smart_unique_buyers_5m = su_score * decay_su

        # whale_share_5m ≈ whales / unique (capped to [0,1])
        if unique_buyers_5m <= 0:
            whale_share_5m = 0.0
        else:
            whale_share_5m = max(
                0.0, min(1.0, whales_5m / max(unique_buyers_5m, 1.0))
            )

        # holders_growth_5m – scaled version of unique buyers; capped to ~0.5
        holders_growth_5m = min(
            unique_buyers_5m / max(UNIQUE_BUYERS_SCALE, 1.0), 0.5
        )

        # alpha cluster flag
        cluster_lower = last_cluster.lower() if last_cluster else ""
        is_alpha_cluster = (
            "alpha" in cluster_lower
            or "smart" in cluster_lower
            or cluster_lower in ALPHA_CLUSTER_NAMES
        )
        smart_last_cluster_is_alpha = 1.0 if is_alpha_cluster else 0.0

        # smart_alpha_bias: boost if we have recent smart events AND alpha cluster
        if smart_wallet_count_5m > 0 and is_alpha_cluster:
            smart_alpha_bias = min(
                1.0,
                smart_wallet_count_5m / max(SMART_EVENT_SCALE, 1.0),
            )
        else:
            smart_alpha_bias = 0.0

        # Alignment & domination features (for quant-grade AI)
        smart_whale_alignment = smart_net_side_5m * whale_net_side_5m  # -1..+1
        smart_whale_alignment_abs = abs(smart_whale_alignment)

        # Whale domination: how far whale_share is above domination threshold, 0..1
        if whale_share_5m <= WHALE_DOM_THRESHOLD:
            whale_domination_5m = 0.0
        else:
            denom = max(1.0 - WHALE_DOM_THRESHOLD, 1e-6)
            whale_domination_5m = max(
                0.0,
                min(1.0, (whale_share_5m - WHALE_DOM_THRESHOLD) / denom),
            )

        # Conflict: strong opposing sides between smart and whales
        conflict = 0.0
        if (
            abs(smart_net_side_5m) >= SMART_WHALE_CONFLICT_NET_MIN
            and abs(whale_net_side_5m) >= SMART_WHALE_CONFLICT_NET_MIN
            and smart_net_side_5m * whale_net_side_5m < 0
        ):
            # scale conflict to strength of disagreement
            conflict = min(
                1.0,
                abs(smart_net_side_5m - whale_net_side_5m) / 2.0,
            )
        smart_vs_whale_conflict = conflict

        # Accumulation vs panic dump scores for quant strategies:
        accum_raw = max(0.0, smart_net_side_5m) * max(
            0.0, whale_net_side_5m
        )
        if smart_alpha_bias > 0:
            accum_raw *= (1.0 + 0.5 * smart_alpha_bias)
        smart_accumulation_score = max(
            0.0, min(1.0, accum_raw * SMART_ACCUM_ALPHA_SCALE)
        )

        dump_raw = max(0.0, -smart_net_side_5m) * max(
            0.0, -whale_net_side_5m
        )
        dump_raw *= (1.0 + whale_domination_5m)
        smart_panic_dump_score = max(
            0.0, min(1.0, dump_raw * SMART_PANIC_DUMP_SCALE)
        )

        # Whale liquidity footprint – normalized whale_vol_5m_usd
        if WHALE_FOOTPRINT_ANCHOR_USD <= 0:
            whale_liquidity_footprint = 0.0
        else:
            whale_liquidity_footprint = max(
                0.0,
                min(1.0, whale_vol_5m_usd / WHALE_FOOTPRINT_ANCHOR_USD),
            )

        # ---- meta signals for AI entry / regime --------------------------

        # smart_signal_confidence – combines recent events + recency
        last_active_ts = max(last_buy_ts, last_sell_ts)
        if last_active_ts <= 0:
            recency_factor = 0.0
        else:
            age = now - last_active_ts
            if age >= SMART_SIGNAL_RECENCY_SEC:
                recency_factor = 0.0
            else:
                recency_factor = max(
                    0.0, 1.0 - age / max(SMART_SIGNAL_RECENCY_SEC, 1.0)
                )

        volume_factor = _to_01_from_count(
            smart_wallet_count_5m, SMART_SIGNAL_MIN_EVENTS_FOR_CONF
        )
        smart_signal_confidence = max(
            0.0, min(1.0, 0.6 * recency_factor + 0.4 * volume_factor)
        )

        # smart_signal_quality – "how tradeable" the smart signal looks:
        quality_raw = 0.0
        quality_raw += 0.4 * smart_alpha_bias
        quality_raw += 0.35 * smart_whale_alignment_abs
        quality_raw += 0.25 * (1.0 - smart_vs_whale_conflict)
        quality_raw -= 0.15 * whale_domination_5m  # ultra whale-heavy = less robust
        smart_signal_quality = max(0.0, min(1.0, quality_raw))

        # smart_risk_flag – highlight dangerous regimes:
        risk_raw = 0.0
        risk_raw += 0.7 * smart_vs_whale_conflict
        risk_raw += 0.5 * whale_domination_5m
        risk_raw *= min(1.0, abs(smart_net_side_5m) + abs(whale_net_side_5m))
        smart_risk_flag = max(0.0, min(1.0, risk_raw))

        # ---- V3+++ convenience signals -----------------------------------

        # recent dominant smart side (-1 sell, 0 neutral, +1 buy)
        if smart_net_side_5m > 0.15:
            smart_recent_side = 1.0
        elif smart_net_side_5m < -0.15:
            smart_recent_side = -1.0
        else:
            smart_recent_side = 0.0

        # recent dominant whale side (-1 sell, 0 neutral, +1 buy)
        if whale_net_side_5m > 0.15:
            whale_recent_side = 1.0
        elif whale_net_side_5m < -0.15:
            whale_recent_side = -1.0
        else:
            whale_recent_side = 0.0

        # smart_crowding_score – fraction of uniques that are smart
        if unique_buyers_5m <= 0:
            smart_crowding_score = 0.0
        else:
            smart_crowding_score = max(
                0.0,
                min(1.0, smart_unique_buyers_5m / max(unique_buyers_5m, 1.0)),
            )

        # smart_signal_state – compressed summary: side * quality * confidence
        signed_side = smart_net_side_5m  # already -1..+1
        smart_signal_state = (
            signed_side * smart_signal_quality * smart_signal_confidence
        )

        # ---- meta heat & edge helpers ------------------------------------

        # smart_heat_score: "how loud" the smart signal is (direction-agnostic)
        smart_heat_raw = (
            abs(smart_net_side_5m)
            * smart_signal_confidence
            * (0.5 + 0.5 * smart_signal_quality)
        )
        smart_heat_score = max(0.0, min(1.0, smart_heat_raw))

        # whale_heat_score: "how loud" the whale signal is (direction-agnostic)
        whale_heat_raw = (
            abs(whale_net_side_5m)
            * min(1.0, whales_5m / max(1.0, SMART_EVENT_SCALE))
            * (0.5 + 0.5 * whale_liquidity_footprint)
        )
        whale_heat_score = max(0.0, min(1.0, whale_heat_raw))

        # If the entire smart signal is extremely weak, fade some secondary details
        if smart_heat_score < SMART_MIN_SCORE_FOR_RICH_FEATURES:
            smart_vs_whale_conflict *= 0.5
            whale_domination_5m *= 0.5
            smart_risk_flag *= 0.5

        # Pressure ratio: whale heat vs combined heat (0..1)
        total_heat = smart_heat_score + whale_heat_score
        if total_heat <= 0:
            smart_whale_pressure_ratio = 0.0
        else:
            smart_whale_pressure_ratio = min(
                1.0, whale_heat_score / max(total_heat, 1e-6)
            )

        # Entry boost hint: directional [-1..+1] hint for ai_entry_logic
        smart_entry_boost_hint = smart_signal_state  # already side * quality * conf

        # Exit pressure hint: "how much does this look like smart panic/exit?"
        smart_exit_pressure_hint = max(
            0.0,
            min(
                1.0,
                smart_panic_dump_score
                * (0.5 + 0.5 * whale_domination_5m)
                * (0.5 + 0.5 * smart_risk_flag),
            ),
        )

        feats: Dict[str, float] = {
            "smart_wallet_score": float(smart_01),
            "smart_flow_raw": float(raw_smart),
            "smart_last_buy_sec_ago": buy_ago,
            "smart_last_sell_sec_ago": sell_ago,
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
            # V3+++ extras:
            "smart_recent_side": float(smart_recent_side),
            "whale_recent_side": float(whale_recent_side),
            "smart_crowding_score": float(smart_crowding_score),
            "smart_signal_state": float(smart_signal_state),
            # meta heat:
            "smart_heat_score": float(smart_heat_score),
            "whale_heat_score": float(whale_heat_score),
            # extra PnL-focused hints:
            "smart_whale_pressure_ratio": float(smart_whale_pressure_ratio),
            "smart_entry_boost_hint": float(smart_entry_boost_hint),
            "smart_exit_pressure_hint": float(smart_exit_pressure_hint),
        }

        if SMART_DEBUG_LOG:
            log.debug(
                "SMART_FEATURES mint=%s feats=%s",
                mint,
                {k: round(v, 3) for k, v in feats.items()},
            )

        await self._store_cached_features(mint, feats, now)
        return feats

    async def attach_features_to_meta(self, meta: Dict[str, Any], mint: str) -> None:
        """
        Mutate `meta` in place, adding smart-wallet & whale features.
        Typically called from FeatureBuilder.
        """
        feats = await self.build_features(mint)
        meta.update(feats)

    async def snapshot(self, mint: str) -> Dict[str, Any]:
        """
        Lightweight diagnostic snapshot for dashboards / /healthcheck.

        Small, fixed subset of features to make logs easy to read.
        """
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
        ]
        return {k: float(feats.get(k, 0.0)) for k in keys}

    async def top_active_mints(
        self,
        *,
        limit: int = 20,
        min_heat: float = 0.0,
    ) -> List[Dict[str, Any]]:
        """
        Quant-grade helper for dashboards / regime / scanners:

        Returns a list of up to `limit` mints sorted by smart_heat_score +
        whale_heat_score, optionally filtering by `min_heat`.

        This can be used by:
          • regime engine to find "interesting" tokens for regime inference
          • self-learning strategy to introspect where alpha is currently flowing
        """
        async with self._lock:
            mints = list(self._last_any_activity_ts.keys())

        results: List[Dict[str, Any]] = []
        for mint in mints:
            feats = await self.build_features(mint)
            smart_heat = float(feats.get("smart_heat_score", 0.0))
            whale_heat = float(feats.get("whale_heat_score", 0.0))
            total_heat = smart_heat + whale_heat
            if total_heat < min_heat:
                continue
            results.append(
                {
                    "mint": mint,
                    "total_heat": total_heat,
                    "smart_heat": smart_heat,
                    "whale_heat": whale_heat,
                    "smart_wallet_score": float(feats.get("smart_wallet_score", 0.5)),
                    "smart_net_side_5m": float(feats.get("smart_net_side_5m", 0.0)),
                    "whale_net_side_5m": float(feats.get("whale_net_side_5m", 0.0)),
                    "smart_exit_pressure_hint": float(
                        feats.get("smart_exit_pressure_hint", 0.0)
                    ),
                }
            )

        results.sort(key=lambda x: x["total_heat"], reverse=True)
        if limit > 0:
            results = results[:limit]
        return results

    async def global_regime_hint(
        self,
        *,
        limit: int = 50,
        min_heat: float = 0.1,
    ) -> Dict[str, float]:
        """
        Compact global hint for regime / risk_manager / self_learning_ai_strategy.

        Aggregates smart + whale state across the hottest mints and returns a
        tiny dict that can be dropped into risk:ctx, for example:

            {
              "smart_heat_global": ...,
              "whale_heat_global": ...,
              "smart_side_global": ...,
              "whale_side_global": ...,
              "smart_panic_global": ...,
              "smart_conflict_global": ...,
              "smart_risk_global": ...,
              "smart_crowding_global": ...,
            }

        All values are bounded and smoothed, so safe to use directly as inputs.
        """
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
            }

        sum_s_heat = sum_w_heat = 0.0
        sum_s_side = sum_w_side = 0.0
        sum_panic = sum_conf = sum_risk = 0.0
        sum_crowding = 0.0
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

            sum_s_heat += s_heat
            sum_w_heat += w_heat
            sum_s_side += s_side * s_heat  # weight sides by heat
            sum_w_side += w_side * w_heat
            sum_panic += panic * s_heat
            sum_conf += conflict * (s_heat + w_heat)
            sum_risk += risk * (s_heat + w_heat)
            sum_crowding += crowd * (s_heat + w_heat)
            n += 1

        if n <= 0:
            n = 1.0

        smart_heat_global = max(0.0, min(1.0, sum_s_heat / n))
        whale_heat_global = max(0.0, min(1.0, sum_w_heat / n))

        # Average side stays in [-1,1]
        denom_s = max(sum_s_heat, 1e-6)
        denom_w = max(sum_w_heat, 1e-6)
        smart_side_global = max(-1.0, min(1.0, sum_s_side / denom_s))
        whale_side_global = max(-1.0, min(1.0, sum_w_side / denom_w))

        # Panic/conflict/risk are also clipped to [0,1]
        smart_panic_global = max(0.0, min(1.0, sum_panic / max(sum_s_heat, 1e-6)))
        smart_conflict_global = max(
            0.0, min(1.0, sum_conf / max(sum_s_heat + sum_w_heat, 1e-6))
        )
        smart_risk_global = max(
            0.0, min(1.0, sum_risk / max(sum_s_heat + sum_w_heat, 1e-6))
        )
        smart_crowding_global = max(
            0.0, min(1.0, sum_crowding / max(sum_s_heat + sum_w_heat, 1e-6))
        )

        return {
            "smart_heat_global": smart_heat_global,
            "whale_heat_global": whale_heat_global,
            "smart_side_global": smart_side_global,
            "whale_side_global": whale_side_global,
            "smart_panic_global": smart_panic_global,
            "smart_conflict_global": smart_conflict_global,
            "smart_risk_global": smart_risk_global,
            "smart_crowding_global": smart_crowding_global,
        }

    # ---- compact hints for entry/exit engines ---------------------------

    async def entry_hint(self, mint: str) -> Dict[str, Any]:
        """
        Compact summary for ai_entry_logic / filtering.

        Returns:
            {
              "boost": float (-1..+1),
              "confidence": float 0..1,
              "quality": float 0..1,
              "risk_flag": float 0..1,
              "recent_side_smart": -1/0/1,
              "recent_side_whale": -1/0/1,
              "crowding": float 0..1,
              "heat_smart": float 0..1,
              "heat_whale": float 0..1,
              "pressure_ratio": float 0..1,
            }
        """
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

    async def exit_hint(self, mint: str) -> Dict[str, Any]:
        """
        Compact summary for ai_sell_logic / exit_engine.

        Returns:
            {
              "panic_pressure": float 0..1,
              "whale_domination": float 0..1,
              "conflict": float 0..1,
              "risk_flag": float 0..1,
              "recent_side_smart": -1/0/1,
              "recent_side_whale": -1/0/1,
              "heat_smart": float 0..1,
              "heat_whale": float 0..1,
            }
        """
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
        """
        Human-readable one-liner for logs / Telegram:

        Example:
          "mint=XYZ | smart=0.72 (buy) | whales=0.31 (sell) |
           heat S=0.54 W=0.41 | panic=0.12 risk=0.08"
        """
        feats = await self.build_features(mint)

        s_score = feats.get("smart_wallet_score", 0.5)
        s_side = feats.get("smart_net_side_5m", 0.0)
        w_side = feats.get("whale_net_side_5m", 0.0)
        s_heat = feats.get("smart_heat_score", 0.0)
        w_heat = feats.get("whale_heat_score", 0.0)
        panic = feats.get("smart_exit_pressure_hint", 0.0)
        risk = feats.get("smart_risk_flag", 0.0)

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
        self,
        mint: str,
        *,
        min_conf: float = 0.2,
        min_heat: float = 0.1,
    ) -> bool:
        """
        Tiny helper for filters: quickly answer "is there any real smart/whale
        signal here yet?" so entry logic can skip dead-flow coins.
        """
        feats = await self.build_features(mint)
        conf = float(feats.get("smart_signal_confidence", 0.0))
        s_heat = float(feats.get("smart_heat_score", 0.0))
        w_heat = float(feats.get("whale_heat_score", 0.0))
        return conf >= min_conf and (s_heat + w_heat) >= min_heat

    # ---- generic event adapter ------------------------------------------

    async def observe_swap_event(self, ev: Dict[str, Any]) -> None:
        """
        Adapter that extracts fields from a generic swap/log event.

        Best-effort keys:
          mint: mint / mintAddress / mint_address / tokenMint
          trader: trader / owner / wallet / user / buyer
          notional: amount_usd / size_usd / notional_usd
          notional_sol: amount_sol / size_sol
          side: side / direction ("buy"/"sell")
        """
        mint = (
            ev.get("mint")
            or ev.get("mintAddress")
            or ev.get("mint_address")
            or ev.get("tokenMint")
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

        amt_usd = (
            ev.get("amount_usd")
            or ev.get("size_usd")
            or ev.get("notional_usd")
        )
        amt_sol = ev.get("amount_sol") or ev.get("size_sol")
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

    async def bulk_observe_swap_events(self, events: List[Dict[str, Any]]) -> None:
        """
        Convenience helper to ingest a list of events (e.g. from Dexscreener /
        Raydium websocket batch).
        """
        for ev in events:
            try:
                await self.observe_swap_event(ev)
            except Exception:
                if SMART_DEBUG_LOG:
                    log.exception("SMART_BULK_EVENT_FAIL ev=%s", ev)


# Global tracker instance (used everywhere)
TRACKER = SmartWalletTracker()

# Backwards-compatible alias
smart_wallets = TRACKER


# =============================================================================
# Reinvestment target picker (used by reinvestor.py)
# =============================================================================


async def pick_reinvestment_target(
    budget_sol: float | None = None,
    *,
    min_24h_volume_usd: float | None = None,
    redis: Any = None,           # kept for signature compatibility
    meta: Optional[Dict[str, Any]] = None,
    universe: Optional[Dict[str, Dict[str, Any]]] = None,
    **_: Any,
) -> Optional[Dict[str, Any] | str]:
    """
    Quant-grade helper for reinvestor:

    Returns either:
        • dict: {"mint": <str>, "score": <float>, "features": {...}}  (preferred)
        • str:  just the mint (for backwards compatibility)

    Inputs are intentionally loose; older callers may only pass:
        await pick_reinvestment_target(budget_sol)

    Scoring idea (PnL-focused):
        • Favour strong positive smart_entry_boost_hint
        • Require some smart/whale heat & confidence
        • Penalize high panic / conflict / risk_flag
        • Mildly penalize whale domination (fragile orderbook)
        • Nudge by profile (core/growth/explore/defensive/degen)
        • Optional universe + volume filter (if provided)
    """
    if not SMART_WALLET_ENABLED or SMART_WALLET_MODE == "off":
        return None

    meta = meta or {}
    profile_raw = (
        meta.get("profile") or meta.get("bucket") or meta.get("reinvest_profile")
    )
    profile = str(profile_raw).lower() if profile_raw is not None else ""

    # profile scales for aggression
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
    else:  # core / growth / default
        risk_penalty = 1.0
        min_conf = 0.20
        min_heat = 0.10

    # Start from hottest mints by total heat
    candidates = await TRACKER.top_active_mints(limit=80, min_heat=min_heat)
    if not candidates:
        return None

    best_score = float("-inf")
    best_payload: Optional[Dict[str, Any]] = None

    for row in candidates:
        mint = row["mint"]

        # static include/exclude lists (cheap guardrails)
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

        # optional volume filtering if caller/universe provides it
        if min_24h_volume_usd is not None and universe is not None:
            meta_row = universe.get(mint) or {}
            vol_24h = float(meta_row.get("volume_24h_usd", 0.0))
            if vol_24h < float(min_24h_volume_usd):
                continue

        # basic filters: need some confidence + directional boost
        if conf < min_conf:
            continue
        if boost <= 0.0:
            # skip mints where smart signal isn't net positive
            continue

        # hard guardrails from env
        if panic > REINVEST_MAX_PANIC:
            continue
        if risk_flag > REINVEST_MAX_RISK:
            continue
        if conflict > REINVEST_MAX_CONFLICT:
            continue

        # scoring function
        #   + boost * conf * quality  (core alpha)
        #   + mild reward for smart heat (active alpha)
        #   - penalize panic / conflict / risk
        #   - mild penalize whale domination (fragile order book)
        core_alpha = boost * conf * (0.5 + 0.5 * qual)
        heat_bonus = 0.15 * smart_heat + 0.05 * whale_heat
        penalty = (
            0.8 * panic
            + 0.7 * conflict
            + 0.9 * risk_flag * risk_penalty
            + 0.3 * whale_dom
        )

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
            },
        }

    if best_payload is None:
        return None

    # For callers that only expect a string, returning dict is still safe:
    # reinvestor.py already supports dict or list of dicts with "mint".
    return best_payload


__all__ = [
    "SmartWalletMeta",
    "SmartWalletRegistry",
    "SmartWalletTracker",
    "REGISTRY",
    "TRACKER",
    "smart_wallets",
    "pick_reinvestment_target",
]


# =============================================================================
# Minimal CLI/debug hook
# =============================================================================

if __name__ == "__main__":  # pragma: no cover
    async def _demo():
        # Very small local smoke test
        REGISTRY.register_wallet(
            "wallet_smart_1",
            cluster="ct_alpha_snipers",
            weight=1.5,
        )
        await TRACKER.observe_swap(
            mint="DEMO_MINT",
            trader="wallet_smart_1",
            amount_usd=1_500.0,
            side="buy",
        )
        await TRACKER.observe_swap(
            mint="DEMO_MINT",
            trader="wallet_whale_1",
            amount_usd=12_000.0,
            side="buy",
        )

        feats = await TRACKER.build_features("DEMO_MINT")
        desc = await TRACKER.describe_mint("DEMO_MINT")
        print("features:", {k: round(v, 3) for k, v in feats.items()})
        print("desc:", desc)

        # mock-universe just to show signature; volume check is optional
        universe = {"DEMO_MINT": {"volume_24h_usd": 1_000_000}}
        target = await pick_reinvestment_target(
            0.5,
            min_24h_volume_usd=10_000,
            universe=universe,
        )
        print("pick_reinvestment_target:", target)

    asyncio.run(_demo())
