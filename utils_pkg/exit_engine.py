from __future__ import annotations

"""
utils_pkg/exit_engine.py

Unified Exit Engine — V3+++ / quant-grade / God-Tier (LOCK-IN, PRODUCTION)
========================================================================

ROLE
----
Central exit coordinator that enforces deterministic protection around open
positions, while delegating execution to the best available mechanism:

  ✅ Hard stop-loss (never overridden by AI)
  ✅ Trailing-stop (arm + trail)
  ✅ Time-stop / stagnant coin exit
  ✅ Early follow-through protection (bad/no follow-through)
  ✅ Liquidity collapse exit (if liquidity data exists)
  ✅ Quote sanity (optional) before enqueue/direct fallback
  ✅ Optional TP ladder (only if provided in position payload; no env keys added)
  ✅ Optional risk_manager gating hook (best-effort; no hard dependency)

It also:
  • Tracks open positions (in-memory + optional reconciliation via position_store)
  • Forwards ticks to AI sell brain (utils_pkg.ai_sell_logic) best-effort
  • Delegates execution to: AI hook → worker enqueue → direct fallback
  • Mirrors state into Redis for dashboards (best-effort)
  • Logs exit events/outcomes to utils_pkg.data_logger (best-effort)
  • Notifies risk_manager / guardrails / self_learning hooks (best-effort)
  • DRYRUN / SAFE_MODE short-circuit execution but still returns decisions

HARD COMPATIBILITY CONTRACTS (DO NOT BREAK)
------------------------------------------
Public API (stable):
    async def on_buy_filled(ev: dict[str, Any]) -> None
    async def on_trade_tick(...) -> dict[str, Any] | None
    async def on_buy_filled_event(ev: dict[str, Any]) -> None
    async def on_buy_filled_legacy(raw_id_or_mint: str, tx_sig: str) -> None

Optional / additive:
    async def on_sell_filled(ev: dict[str, Any]) -> None
    async def request_exit(ev: dict[str, Any]) -> bool
    def get_open_positions_snapshot() -> dict[str, dict[str, Any]]
    def reload_exit_engine_config() -> None

Worker entrypoint (needed by scripts/main.py):
    async def run_worker(stop_evt: asyncio.Event) -> None
    async def exit_engine_loop(stop_evt: asyncio.Event) -> None  # alias

TEST CONTRACT (DO NOT BREAK)
----------------------------
The contract tests treat on_trade_tick as the "decider" function and require:
  - signature accepts a position-like payload OR **kwargs
  - with realistic payload, returns dict-like decision OR None (HOLD)
  - never crashes on missing optional fields
  - deterministic for fixed inputs in SAFE_MODE (best-effort)
  - if action signals exit/sell => include non-empty reason

NOTES
-----
- Best-effort: must never crash the process.
- Mints/allowlists are base58 and CASE-SENSITIVE; do NOT lowercase mints.
"""

import asyncio
import contextlib
import datetime as _dt
import importlib
import inspect
import json
import os
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any

# =============================================================================
# Identity / constants
# =============================================================================
EXIT_ENGINE_VERSION = "V3+++_LOCKIN_2026-02-06"
_SOL_MINT = "So11111111111111111111111111111111111111112"
_BASE58_ALPHABET = set("123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz")

# kept for downstream compatibility
LAMPORTS_PER_SOL = 1_000_000_000

INSTANCE_ID = (
    (os.getenv("INSTANCE_ID") or "").strip()
    or (os.getenv("RENDER_INSTANCE_ID") or "").strip()
    or (os.getenv("HOSTNAME") or "").strip()
    or "local"
)

# =============================================================================
# Logger (soft-compatible: .event + .every)
# =============================================================================
log: Any
try:
    from logging_setup import get_logger  # type: ignore

    log = get_logger("sniper.exit_engine")  # type: ignore
except Exception:  # pragma: no cover
    import logging

    log = logging.getLogger("sniper.exit_engine")
    if not log.handlers:
        logging.basicConfig(level=logging.INFO)


def _install_log_shims() -> None:
    if not hasattr(log, "event"):

        def _event(name: str, **fields: Any) -> None:  # pragma: no cover
            with contextlib.suppress(Exception):
                log.info("%s %s", name, fields)

        log.event = _event  # type: ignore[attr-defined]

    if not hasattr(log, "every"):

        def _every(key: str, n: int = 60, **fields: Any) -> None:  # pragma: no cover
            now = time.time()
            store = getattr(_every, "_store", {})  # type: ignore[attr-defined]
            last = float(store.get(key, 0.0))
            if now - last >= float(n):
                store[key] = now
                _every._store = store  # type: ignore[attr-defined]
                with contextlib.suppress(Exception):
                    log.event(key, **fields)  # type: ignore[misc]

        log.every = _every  # type: ignore[attr-defined]


_install_log_shims()


def _log(name: str, level: str = "info", **fields: Any) -> None:
    with contextlib.suppress(Exception):
        log.event(name, **fields)  # type: ignore[misc]
        return
    with contextlib.suppress(Exception):
        msg = f"{name} {fields}"
        if level == "warning":
            log.warning(msg)
        elif level == "error":
            log.error(msg)
        else:
            log.info(msg)


# =============================================================================
# Env helpers (DO NOT add new env keys lightly; contract tests enforce them)
# =============================================================================
_TRUE = {"1", "true", "t", "yes", "y", "on"}
_FALSE = {"0", "false", "f", "no", "n", "off"}


def _env_bool(key: str, default: bool = False) -> bool:
    v = os.getenv(key)
    if v is None:
        return default
    s = str(v).strip().lower()
    if s in _TRUE:
        return True
    if s in _FALSE:
        return False
    return default


def _env_int(key: str, default: int) -> int:
    try:
        return int(float(str(os.getenv(key, str(default))).strip()))
    except Exception:
        return default


def _env_float(key: str, default: float) -> float:
    try:
        return float(str(os.getenv(key, str(default))).strip())
    except Exception:
        return default


def _env_str(key: str, default: str = "") -> str:
    return (os.getenv(key, default) or default).strip()


def _env_csv_set(key: str, *, lower: bool = False) -> set[str]:
    raw = (os.getenv(key, "") or "").strip()
    if not raw:
        return set()
    out: set[str] = set()
    for part in raw.split(","):
        p = (part or "").strip()
        if not p:
            continue
        out.add(p.lower() if lower else p)
    return out


def _finite(x: Any, d: float = 0.0) -> float:
    try:
        v = float(x)
        if v != v or v in (float("inf"), float("-inf")):
            return d
        return v
    except Exception:
        return d


def _clamp(x: float, lo: float, hi: float) -> float:
    return lo if x < lo else hi if x > hi else x


def _short_exc(e: BaseException, n: int = 260) -> str:
    s = f"{type(e).__name__}: {e}"
    return (s[:n] + "…") if len(s) > n else s


async def _maybe_await(x: Any) -> Any:
    if inspect.isawaitable(x):
        return await x
    return x


def _json_default(o: Any) -> str:
    with contextlib.suppress(Exception):
        return str(o)
    return "<unserializable>"


def _json_dumps(payload: Any) -> str:
    return json.dumps(payload, ensure_ascii=False, separators=(",", ":"), default=_json_default)


# =============================================================================
# SAFE_MODE / Network policy
# =============================================================================
def _is_safe_mode() -> bool:
    # tests usually set PYTEST_MODE/SAFE_MODE/DRYRUN
    return bool(
        _env_bool("PYTEST_MODE", False)
        or _env_bool("SAFE_MODE", False)
        or _env_bool("NO_NETWORK", False)
        or _env_bool("DRYRUN", False)
        or _env_bool("DRY_RUN", False)
        or _env_bool("BACKTEST_MODE", False)
    )


def _sells_allowed() -> bool:
    if _is_safe_mode():
        return False
    if _env_bool("DISABLE_SELLS", False):
        return False
    return _env_bool("AUTO_SELL_ENABLED", True)


def _allow_network() -> bool:
    # Default OFF in tests; ON in production unless explicitly disabled.
    if _is_safe_mode():
        return False
    if _env_bool("DISABLE_NETWORK", False):
        return False
    return _env_bool("EXIT_ENGINE_ALLOW_NETWORK", True)


# =============================================================================
# Lazy imports (avoid cycles / optional modules)
# =============================================================================
_MOD_CACHE: dict[str, Any] = {}


def _get_mod(name: str) -> Any:
    if name in _MOD_CACHE:
        return _MOD_CACHE[name]
    try:
        m = importlib.import_module(name)
    except Exception:
        m = None
    _MOD_CACHE[name] = m
    return m


# =============================================================================
# Mint / sig normalization
# =============================================================================
def _looks_base58(s: str) -> bool:
    if not s:
        return False
    return all(ch in _BASE58_ALPHABET for ch in s)


def _looks_like_sig(s: str) -> bool:
    ss = (s or "").strip()
    return len(ss) >= 64 and _looks_base58(ss)


def _looks_like_mint(s: str) -> bool:
    ss = (s or "").strip()
    return 28 <= len(ss) <= 52 and _looks_base58(ss) and (not _looks_like_sig(ss))


def _norm_mint(m: str) -> str:
    s = (m or "").strip()
    if not s:
        return ""
    try:
        mm = _get_mod("utils_pkg.mints")
        fn = getattr(mm, "normalize_mint", None) if mm else None
        if callable(fn):
            out = str(fn(s)).strip()
            return out
    except Exception:
        pass
    return s


def _pool_normalize(pool: str | None) -> str:
    if not pool:
        return "unknown"
    return str(pool).strip().lower() or "unknown"


# =============================================================================
# Robust parsing helpers (never throws)
# =============================================================================
def _as_mapping(x: Any) -> Mapping[str, Any] | None:
    return x if isinstance(x, Mapping) else None


def _to_float(v: Any) -> float | None:
    if v is None:
        return None
    try:
        if isinstance(v, bool):
            return float(int(v))
        if isinstance(v, (int, float)):
            f = float(v)
            if f != f or f in (float("inf"), float("-inf")):
                return None
            return f
        if isinstance(v, str):
            s = v.strip()
            if not s:
                return None
            s = s.replace(",", "")
            f2 = float(s)
            if f2 != f2 or f2 in (float("inf"), float("-inf")):
                return None
            return f2
    except Exception:
        return None
    return None


def _to_int(v: Any) -> int | None:
    if v is None:
        return None
    try:
        if isinstance(v, bool):
            return int(v)
        if isinstance(v, int):
            return int(v)
        if isinstance(v, float):
            if v != v or v in (float("inf"), float("-inf")):
                return None
            return int(v)
        if isinstance(v, str):
            s = v.strip()
            if not s:
                return None
            s = s.replace(",", "")
            return int(float(s))  # allow "123.0"
    except Exception:
        return None
    return None


def _coerce_int_opt(v: Any) -> int | None:
    iv = _to_int(v)
    if iv is None or iv <= 0:
        return None
    return int(iv)


def _parse_ts_any(v: Any) -> float | None:
    """
    Accepts:
      - unix seconds (int/float)
      - numeric strings
      - ISO8601 strings (with optional 'Z')
    """
    if v is None:
        return None

    if isinstance(v, (int, float)) and not isinstance(v, bool):
        fv = float(v)
        return fv if fv > 0 else None

    if isinstance(v, str):
        s = v.strip()
        if not s:
            return None

        # numeric string path
        fv_opt = _to_float(s)
        if fv_opt is not None and fv_opt > 0:
            return float(fv_opt)

        # ISO8601 path
        with contextlib.suppress(Exception):
            ss = s.replace("Z", "+00:00")
            dt = _dt.datetime.fromisoformat(ss)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=_dt.UTC)
            return float(dt.timestamp())

    return None


def _extract_ts(ev: Mapping[str, Any] | None, now: float) -> float:
    if ev:
        for k in ("ts", "timestamp", "time", "now_ts", "now", "created_ts", "entry_ts"):
            if ev.get(k) is None:
                continue
            t = _parse_ts_any(ev.get(k))
            if t is not None and t > 0:
                return float(t)
    return float(now)


def _extract_mint(ev: Mapping[str, Any] | None) -> str | None:
    if not ev:
        return None
    for key in ("mint", "mintAddress", "mint_address", "tokenMint", "baseMint", "token", "address"):
        v = ev.get(key)
        if isinstance(v, str) and v.strip():
            m = _norm_mint(v)
            return m if m else None

    for k in ("pair", "token", "base", "asset", "meta", "position"):
        o = ev.get(k)
        if isinstance(o, Mapping):
            for kk in ("mint", "address", "id"):
                vv = o.get(kk)
                if isinstance(vv, str) and vv.strip():
                    m = _norm_mint(vv)
                    return m if m else None
    return None


def _extract_symbol(ev: Mapping[str, Any] | None, mint: str) -> str:
    if ev:
        for key in ("symbol", "sym", "ticker"):
            v = ev.get(key)
            if isinstance(v, str) and v.strip():
                return v.strip()
    return (mint[:4] if mint else "").strip()


def _extract_pool(ev: Mapping[str, Any] | None) -> str:
    if ev:
        for key in ("pool", "source", "venue", "dex", "market"):
            v = ev.get(key)
            if isinstance(v, str) and v.strip():
                return _pool_normalize(v)
    return "unknown"


def _extract_price_any(ev: Mapping[str, Any] | None) -> float | None:
    if not ev:
        return None

    for key in (
        "price",
        "price_ui",
        "priceUi",
        "ui_price",
        "price_in_sol",
        "priceInSol",
        "sol_per_token",
        "token_price_sol",
        "last_price",
        "lastPrice",
        "mark",
        "mid",
        "close",
        "p",
    ):
        f = _to_float(ev.get(key))
        if f is not None and f > 0:
            return float(f)

    for key in ("price_usd", "usd_price", "priceUsd", "priceUSD"):
        f = _to_float(ev.get(key))
        if f is not None and f > 0:
            return float(f)

    return None


def _extract_trades_count(ev: Mapping[str, Any] | None) -> int | None:
    if not ev:
        return None
    for k in ("trades", "trade_count", "txns", "transactions", "txnCount", "txCount"):
        v = ev.get(k)
        if v is None:
            continue
        if isinstance(v, Mapping):
            with contextlib.suppress(Exception):
                total = 0
                for kk in ("total", "count", "txns", "trades", "n", "buys", "sells"):
                    if kk in v and v.get(kk) is not None:
                        iv = _to_int(v.get(kk))
                        if iv is not None:
                            total += int(iv)
                return max(0, int(total))
        iv2 = _to_int(v)
        if iv2 is not None and iv2 >= 0:
            return int(iv2)
    return None


def _extract_spend_sol(ev: Mapping[str, Any] | None) -> float:
    if not ev:
        return 0.0
    for key in (
        "spend_sol",
        "entry_spend_sol",
        "buy_size_sol",
        "amount_sol",
        "basis_sol",
        "spent_sol",
        "in_sol",
    ):
        f = _to_float(ev.get(key))
        if f is not None:
            return float(f)
    return 0.0


def _extract_amount_raw(ev: Mapping[str, Any] | None) -> int | None:
    if not ev:
        return None
    for key in (
        "amount_raw",
        "token_amount_raw",
        "base_amount_raw",
        "out_amount_raw",
        "size_raw",
        "amountRaw",
    ):
        iv = _to_int(ev.get(key))
        if iv is not None and iv > 0:
            return int(iv)
    return None


def _extract_liq_usd(ev: Mapping[str, Any] | None) -> float | None:
    if not ev:
        return None
    for k in ("liq", "liquidity", "liquidityUsd", "liquidity_usd", "liq_usd", "liquidityUSD"):
        f = _to_float(ev.get(k))
        if f is not None and f > 0:
            return float(f)
    meta = ev.get("meta")
    if isinstance(meta, Mapping):
        for k in ("liq", "liquidity", "liquidityUsd", "liquidity_usd", "liq_usd"):
            f = _to_float(meta.get(k))
            if f is not None and f > 0:
                return float(f)
    return None


# =============================================================================
# Config
# =============================================================================
@dataclass(frozen=True)
class ExitCfg:
    master_phase: int
    enabled: bool
    dryrun: bool

    stop_loss_pct: float
    trailing_arm_pct: float
    trailing_stop_pct: float
    time_stop_min: int
    time_stop_min_profit_pct: float
    stagnant_max_min: int
    stagnant_min_move_pct: float

    follow_enabled: bool
    follow_window_sec: int
    follow_grace_sec: int
    follow_extend_low_trades_sec: int
    follow_min_trades: int
    follow_min_up_pct: float
    follow_max_dd_pct: float
    follow_min_pnl_pct: float
    follow_early_catastrophic_dd_pct: float

    liq_collapse_pct: float
    min_liq_usd: float

    follow_pools: set[str]
    follow_min_spend_sol: float
    follow_mint_whitelist: set[str]
    follow_mint_blacklist: set[str]

    tick_throttle_ms: int
    cleanup_every_sec: int
    track_max_age_sec: int

    allow_worker_enqueue: bool
    allow_direct_fallback: bool
    fallback_sell_pct: float
    fallback_slippage_bps: int
    fallback_priority: int

    quote_sanity_enabled: bool
    quote_timeout_s: float
    quote_max_price_impact_pct: float
    quote_allow_fail_on_hard_stop: bool
    quote_reduce_soft_exits: bool
    quote_reduce_floor_sell_pct: float

    publish_redis: bool
    redis_ttl_sec: int
    health_redis_key: str
    health_every_sec: int
    open_snapshot_key: str
    publish_open_snapshot: bool
    open_snapshot_every_sec: int

    exit_dedupe_sec: int
    burst_cap_per_min: int
    max_daily_exits: int
    daily_key_prefix: str


def _load_cfg() -> ExitCfg:
    master = max(0, min(8, _env_int("MASTER_PHASE", _env_int("STRATEGY_PHASE", 7))))

    dryrun = bool(
        _env_bool("DRYRUN", False)
        or _env_bool("DRY_RUN", False)
        or _env_bool("PAPER_TRADING", False)
    )
    enabled_flag = _env_bool("EXIT_ENGINE_ENABLED", True)
    enabled = bool(
        (master > 0 or _is_safe_mode())
        and _env_bool("EXIT_ENGINE", True)
        and enabled_flag
        and (not _env_bool("KILL_SWITCH", False))
    )

    stop_loss = max(0.1, float(_env_float("EXIT_STOP_LOSS_PCT", _env_float("PF_SL_PCT", 8.0))))
    arm = max(0.0, float(_env_float("EXIT_TSL_ARM_PCT", _env_float("PF_TSL_ARM_PCT", 10.0))))
    tsl = max(0.1, float(_env_float("EXIT_TSL_PCT", _env_float("PF_TSL_PCT", 4.5))))

    time_stop_min = max(0, _env_int("EXIT_TIME_STOP_MIN", _env_int("EXIT_MAX_HOLD_MIN", 0)))
    time_stop_min_profit = float(_env_float("EXIT_TIME_STOP_MIN_PROFIT_PCT", 0.0))

    stagnant_max = max(0, _env_int("EXIT_STAGNANT_MAX_MIN", 0))
    stagnant_move = float(_env_float("EXIT_STAGNANT_MIN_MOVE_PCT", 0.0))

    follow_enabled = _env_bool("EXIT_FOLLOW_ENABLED", True)
    follow_window = max(3, _env_int("EXIT_FOLLOW_WINDOW_SEC", 35))
    follow_grace = max(0, _env_int("EXIT_FOLLOW_GRACE_SEC", 5))
    follow_extend = max(0, _env_int("EXIT_FOLLOW_EXTEND_LOW_TRADES_SEC", 15))
    follow_min_trades = max(0, _env_int("EXIT_FOLLOW_MIN_TRADES", 5))
    follow_min_up = float(_env_float("EXIT_FOLLOW_MIN_UP_PCT", 6.0))
    follow_max_dd = max(0.0, float(_env_float("EXIT_FOLLOW_MAX_DD_PCT", 5.0)))
    follow_min_pnl = float(_env_float("EXIT_FOLLOW_MIN_PNL_PCT", 0.0))
    catastrophic_dd = max(0.0, float(_env_float("EXIT_FOLLOW_EARLY_CATASTROPHIC_DD_PCT", 0.0)))

    liq_collapse = max(0.0, float(_env_float("EXIT_LIQ_COLLAPSE_PCT", 0.0)))
    min_liq = max(0.0, float(_env_float("EXIT_MIN_LIQ_USD", 0.0)))

    pools = _env_csv_set("EXIT_FOLLOW_POOLS", lower=True)
    wl_raw = _env_csv_set("EXIT_FOLLOW_MINT_WHITELIST", lower=False)
    bl_raw = _env_csv_set("EXIT_FOLLOW_MINT_BLACKLIST", lower=False)
    mint_whitelist = {_norm_mint(x) for x in wl_raw if _norm_mint(x)}
    mint_blacklist = {_norm_mint(x) for x in bl_raw if _norm_mint(x)}

    tick_throttle = max(0, _env_int("EXIT_TICK_THROTTLE_MS", 250))
    cleanup_every = max(1, _env_int("EXIT_CLEANUP_EVERY_SEC", 30))
    track_max_age = max(60, _env_int("EXIT_TRACK_MAX_AGE_SEC", 6 * 60 * 60))

    allow_worker = _env_bool("EXIT_ALLOW_WORKER_ENQUEUE", True)
    allow_direct = _env_bool("EXIT_ALLOW_DIRECT_FALLBACK", bool(master >= 3))

    sell_pct = _clamp(float(_env_float("EXIT_FALLBACK_SELL_PCT", 1.0)), 0.01, 1.0)
    slip = int(_env_int("EXIT_FALLBACK_SLIPPAGE_BPS", _env_int("PF_SLIPPAGE_BPS", 200)))
    slip = max(5, min(5000, slip))
    prio = max(0, _env_int("EXIT_FALLBACK_PRIORITY", 1))

    quote_enabled = _env_bool("EXIT_QUOTE_SANITY", True)
    quote_timeout = max(0.15, float(_env_float("EXIT_QUOTE_TIMEOUT_S", 0.65)))
    quote_pi = float(_env_float("EXIT_QUOTE_MAX_PRICE_IMPACT_PCT", 85.0))
    quote_allow_hard = _env_bool("EXIT_QUOTE_ALLOW_FAIL_ON_HARD_STOP", True)
    quote_reduce_soft = _env_bool("EXIT_QUOTE_REDUCE_SOFT_EXITS", True)
    quote_floor_pct = _clamp(float(_env_float("EXIT_QUOTE_REDUCE_FLOOR_SELL_PCT", 0.10)), 0.01, 1.0)

    publish_redis = _env_bool("EXIT_PUBLISH_REDIS", True)
    redis_ttl = max(30, _env_int("EXIT_REDIS_TTL_SEC", 3600))
    health_key = _env_str("EXIT_HEALTH_REDIS_KEY", "exit_engine:health") or "exit_engine:health"
    health_every = max(0, _env_int("EXIT_HEALTH_EVERY_SEC", 30))
    open_key = (
        _env_str("EXIT_OPEN_SNAPSHOT_KEY", "exit_engine:open_positions")
        or "exit_engine:open_positions"
    )
    publish_open = _env_bool("EXIT_PUBLISH_OPEN_SNAPSHOT", False)
    open_every = max(5, _env_int("EXIT_OPEN_SNAPSHOT_EVERY_SEC", 30))

    dedupe = max(3, _env_int("EXIT_DEDUPE_SEC", 25))
    burst = max(0, _env_int("EXIT_BURST_CAP_PER_MIN", 0))
    max_daily = max(0, _env_int("EXIT_MAX_DAILY_EXITS", 0))
    daily_prefix = _env_str("EXIT_DAILY_KEY_PREFIX", "exit_engine:daily") or "exit_engine:daily"

    return ExitCfg(
        master_phase=int(master),
        enabled=bool(enabled),
        dryrun=bool(dryrun),
        stop_loss_pct=float(stop_loss),
        trailing_arm_pct=float(arm),
        trailing_stop_pct=float(tsl),
        time_stop_min=int(time_stop_min),
        time_stop_min_profit_pct=float(time_stop_min_profit),
        stagnant_max_min=int(stagnant_max),
        stagnant_min_move_pct=float(stagnant_move),
        follow_enabled=bool(follow_enabled),
        follow_window_sec=int(follow_window),
        follow_grace_sec=int(follow_grace),
        follow_extend_low_trades_sec=int(follow_extend),
        follow_min_trades=int(follow_min_trades),
        follow_min_up_pct=float(follow_min_up),
        follow_max_dd_pct=float(follow_max_dd),
        follow_min_pnl_pct=float(follow_min_pnl),
        follow_early_catastrophic_dd_pct=float(catastrophic_dd),
        liq_collapse_pct=float(liq_collapse),
        min_liq_usd=float(min_liq),
        follow_pools=set(pools),
        follow_min_spend_sol=max(0.0, float(_env_float("EXIT_FOLLOW_MIN_SPEND_SOL", 0.0))),
        follow_mint_whitelist=set(mint_whitelist),
        follow_mint_blacklist=set(mint_blacklist),
        tick_throttle_ms=int(tick_throttle),
        cleanup_every_sec=int(cleanup_every),
        track_max_age_sec=int(track_max_age),
        allow_worker_enqueue=bool(allow_worker),
        allow_direct_fallback=bool(allow_direct),
        fallback_sell_pct=float(sell_pct),
        fallback_slippage_bps=int(slip),
        fallback_priority=int(prio),
        quote_sanity_enabled=bool(quote_enabled),
        quote_timeout_s=float(quote_timeout),
        quote_max_price_impact_pct=float(quote_pi),
        quote_allow_fail_on_hard_stop=bool(quote_allow_hard),
        quote_reduce_soft_exits=bool(quote_reduce_soft),
        quote_reduce_floor_sell_pct=float(quote_floor_pct),
        publish_redis=bool(publish_redis),
        redis_ttl_sec=int(redis_ttl),
        health_redis_key=str(health_key),
        health_every_sec=int(health_every),
        open_snapshot_key=str(open_key),
        publish_open_snapshot=bool(publish_open),
        open_snapshot_every_sec=int(open_every),
        exit_dedupe_sec=int(dedupe),
        burst_cap_per_min=int(burst),
        max_daily_exits=int(max_daily),
        daily_key_prefix=str(daily_prefix),
    )


CFG: ExitCfg = _load_cfg()


def reload_exit_engine_config() -> None:
    global CFG
    CFG = _load_cfg()
    _log(
        "EXIT_CFG_RELOADED",
        version=EXIT_ENGINE_VERSION,
        instance=INSTANCE_ID,
        phase=int(CFG.master_phase),
        enabled=int(CFG.enabled),
        dryrun=int(CFG.dryrun),
        safe=int(_is_safe_mode()),
        allow_network=int(_allow_network()),
    )


# =============================================================================
# Optional hooks
# =============================================================================
def _sell_hook(name: str) -> Callable[..., Any] | None:
    m = _get_mod("utils_pkg.ai_sell_logic")
    fn = getattr(m, name, None) if m else None
    return fn if callable(fn) else None


async def _call_optional_hook(
    mod_name: str, fn_candidates: tuple[str, ...], event: dict[str, Any]
) -> None:
    m = _get_mod(mod_name)
    if not m:
        return
    for nm in fn_candidates:
        fn = getattr(m, nm, None)
        if not callable(fn):
            continue
        try:
            if asyncio.iscoroutinefunction(fn):
                await fn(event)  # type: ignore[misc]
            else:
                await asyncio.to_thread(fn, event)  # type: ignore[misc]
        except Exception:
            pass
        break


# =============================================================================
# Redis (best-effort)
# =============================================================================
async def _get_redis_async() -> Any:
    cu = _get_mod("utils_pkg.core_utils")
    if not cu:
        return None

    r = getattr(cu, "redis", None)
    if r is not None:
        return r

    gra = getattr(cu, "get_redis_async", None)
    if callable(gra):
        with contextlib.suppress(Exception):
            out = gra()
            return await _maybe_await(out)

    gr = getattr(cu, "get_redis", None)
    if callable(gr):
        with contextlib.suppress(Exception):
            out2 = gr()
            return await _maybe_await(out2)

    return None


def _today_key(prefix: str) -> str:
    now = _dt.datetime.now(_dt.UTC)
    return f"{prefix}:{now.strftime('%Y%m%d')}"


async def _redis_get_int(redis: Any, key: str) -> int:
    if not redis:
        return 0
    try:
        raw = await _maybe_await(redis.get(key))
        if raw is None:
            return 0
        if isinstance(raw, (bytes, bytearray)):
            raw = raw.decode(errors="ignore")
        iv = _to_int(raw)
        return int(iv) if iv is not None else 0
    except Exception:
        return 0


async def _redis_incr_with_ttl(redis: Any, key: str, ttl_sec: int) -> None:
    if not redis:
        return
    with contextlib.suppress(Exception):
        pipe = redis.pipeline()
        pipe.incr(key)
        pipe.expire(key, int(ttl_sec))
        await _maybe_await(pipe.execute())
        return
    with contextlib.suppress(Exception):
        await _maybe_await(redis.incr(key))
        await _maybe_await(redis.expire(key, int(ttl_sec)))


async def _redis_set_json(redis: Any, key: str, payload: dict[str, Any], ttl_sec: int) -> None:
    if not redis:
        return
    with contextlib.suppress(Exception):
        await _maybe_await(redis.set(key, _json_dumps(payload), ex=int(ttl_sec)))


# =============================================================================
# In-memory state
# =============================================================================
@dataclass
class _Pos:
    mint: str
    symbol: str
    pool: str
    entry_sig: str
    spend_sol: float
    amount_raw: int | None
    candidate_id: Any | None
    entry_ts: float
    opened_from: str

    entry_price: float | None = None
    entry_liq_usd: float | None = None

    last_tick_ts: float = 0.0
    last_eval_ts: float = 0.0

    trades_after_entry: int = 0
    last_feed_trade_counter: int | None = None

    last_price: float | None = None
    max_price: float | None = None
    min_price: float | None = None

    last_liq_usd: float | None = None

    follow_done: bool = False
    follow_pass: bool = False

    # Optional profit-taking ladder index (only used if ladder is provided in payload/meta)
    tp_next_idx: int = 0

    closed: bool = False


_OPEN: dict[str, _Pos] = {}
_OPEN_LOCK = asyncio.Lock()

_LAST_OPEN_SNAPSHOT: dict[str, dict[str, Any]] = {}
_LAST_OPEN_CACHE_TS = 0.0
_LAST_OPEN_PUBLISH_TS = 0.0
_LAST_HEALTH_TS = 0.0
_LAST_CLEANUP_TS = 0.0

_RECENT_EXIT_TS: dict[str, float] = {}
_BURST_BUCKET: dict[str, int] = {}


def _minute_key(now: float) -> str:
    dt = _dt.datetime.fromtimestamp(now, tz=_dt.UTC)
    return dt.strftime("%Y%m%d%H%M")


def _exit_dedupe_ok(mint: str, now: float) -> bool:
    last = float(_RECENT_EXIT_TS.get(mint, 0.0))
    if now - last < float(CFG.exit_dedupe_sec):
        return False
    _RECENT_EXIT_TS[mint] = now
    if len(_RECENT_EXIT_TS) > 5000:
        cutoff = now - 3600.0
        for k, v in list(_RECENT_EXIT_TS.items()):
            if v < cutoff:
                _RECENT_EXIT_TS.pop(k, None)
    return True


def _burst_cap_ok(now: float) -> bool:
    cap = int(CFG.burst_cap_per_min)
    if cap <= 0:
        return True
    mk = _minute_key(now)
    _BURST_BUCKET[mk] = int(_BURST_BUCKET.get(mk, 0)) + 1
    if len(_BURST_BUCKET) > 10:
        for k in list(_BURST_BUCKET.keys()):
            if k != mk:
                _BURST_BUCKET.pop(k, None)
    return _BURST_BUCKET[mk] <= cap


def _build_open_snapshot_nolock(now: float) -> dict[str, dict[str, Any]]:
    snap: dict[str, dict[str, Any]] = {}
    for mint, pos in _OPEN.items():
        if pos.closed:
            continue
        snap[mint] = {
            "mint": pos.mint,
            "symbol": pos.symbol,
            "pool": pos.pool,
            "age_sec": float(now - pos.entry_ts),
            "spend_sol": float(pos.spend_sol),
            "amount_raw": pos.amount_raw,
            "trades_after_entry": int(pos.trades_after_entry),
            "entry_price": pos.entry_price,
            "max_price": pos.max_price,
            "min_price": pos.min_price,
            "last_price": pos.last_price,
            "entry_liq_usd": pos.entry_liq_usd,
            "last_liq_usd": pos.last_liq_usd,
            "candidate_id": pos.candidate_id,
            "opened_from": pos.opened_from,
            "follow_done": bool(pos.follow_done),
            "follow_pass": bool(pos.follow_pass),
            "tp_next_idx": int(pos.tp_next_idx),
            "last_tick_ts": float(pos.last_tick_ts),
            "version": EXIT_ENGINE_VERSION,
            "instance": INSTANCE_ID,
        }
    return snap


async def _refresh_snapshot_cache(now: float) -> None:
    global _LAST_OPEN_SNAPSHOT, _LAST_OPEN_CACHE_TS
    if now - _LAST_OPEN_CACHE_TS < 0.25:
        return
    async with _OPEN_LOCK:
        _LAST_OPEN_SNAPSHOT = _build_open_snapshot_nolock(now)
        _LAST_OPEN_CACHE_TS = now


async def _cleanup_if_needed(now: float) -> None:
    global _LAST_CLEANUP_TS
    if now - _LAST_CLEANUP_TS < float(max(1, CFG.cleanup_every_sec)):
        return
    _LAST_CLEANUP_TS = now
    async with _OPEN_LOCK:
        for mint, pos in list(_OPEN.items()):
            if pos.closed or (now - pos.entry_ts) > float(CFG.track_max_age_sec):
                _OPEN.pop(mint, None)


async def _publish_open_snapshot(now: float) -> None:
    global _LAST_OPEN_PUBLISH_TS
    if not (CFG.publish_redis and CFG.publish_open_snapshot):
        return
    if now - _LAST_OPEN_PUBLISH_TS < float(max(5, CFG.open_snapshot_every_sec)):
        return
    await _refresh_snapshot_cache(now)
    r = await _get_redis_async()
    if not r:
        return
    await _redis_set_json(
        r,
        CFG.open_snapshot_key,
        {
            "ts": now,
            "open": dict(_LAST_OPEN_SNAPSHOT),
            "version": EXIT_ENGINE_VERSION,
            "instance": INSTANCE_ID,
        },
        int(CFG.redis_ttl_sec),
    )
    _LAST_OPEN_PUBLISH_TS = now


async def _publish_health(now: float) -> None:
    global _LAST_HEALTH_TS
    if not (CFG.publish_redis and CFG.health_every_sec > 0):
        return
    if now - _LAST_HEALTH_TS < float(max(5, CFG.health_every_sec)):
        return
    _LAST_HEALTH_TS = now
    await _refresh_snapshot_cache(now)

    r = await _get_redis_async()
    if not r:
        return

    payload = {
        "ts": float(now),
        "version": EXIT_ENGINE_VERSION,
        "instance": INSTANCE_ID,
        "enabled": int(CFG.enabled),
        "dryrun": int(CFG.dryrun),
        "phase": int(CFG.master_phase),
        "safe": int(_is_safe_mode()),
        "allow_network": int(_allow_network()),
        "open_count": int(len(_LAST_OPEN_SNAPSHOT)),
        "sl_pct": float(CFG.stop_loss_pct),
        "arm_pct": float(CFG.trailing_arm_pct),
        "tsl_pct": float(CFG.trailing_stop_pct),
        "time_stop_min": int(CFG.time_stop_min),
        "stagnant_max_min": int(CFG.stagnant_max_min),
        "liq_collapse_pct": float(CFG.liq_collapse_pct),
        "quote": int(CFG.quote_sanity_enabled),
        "dedupe_sec": int(CFG.exit_dedupe_sec),
        "burst_cap": int(CFG.burst_cap_per_min),
        "worker": int(CFG.allow_worker_enqueue),
        "direct": int(CFG.allow_direct_fallback),
    }
    await _redis_set_json(r, CFG.health_redis_key, payload, int(max(30, CFG.redis_ttl_sec)))


async def _housekeeping(now: float) -> None:
    await _cleanup_if_needed(now)
    await _refresh_snapshot_cache(now)
    await _publish_open_snapshot(now)
    await _publish_health(now)


# =============================================================================
# Scope filters (follow-through + rule exits only)
# =============================================================================
def _scope_ok(snapshot: Mapping[str, Any]) -> bool:
    mint = _norm_mint(str(snapshot.get("mint") or ""))
    if not mint:
        return False
    if CFG.follow_mint_blacklist and mint in CFG.follow_mint_blacklist:
        return False
    if CFG.follow_mint_whitelist and mint not in CFG.follow_mint_whitelist:
        return False
    pool = _pool_normalize(str(snapshot.get("pool") or "unknown"))
    if CFG.follow_pools and pool not in CFG.follow_pools:
        return False
    spend = _finite(snapshot.get("spend_sol") or 0.0, 0.0)
    return not (CFG.follow_min_spend_sol > 0.0 and spend < float(CFG.follow_min_spend_sol))


# =============================================================================
# Metrics / decision object
# =============================================================================
def _compute_metrics(
    entry: float | None, maxp: float | None, minp: float | None, last: float | None
) -> dict[str, float]:
    out = {"pnl_pct": 0.0, "up_pct": 0.0, "dd_pct": 0.0, "peak_dd_pct": 0.0, "range_pct": 0.0}
    if entry and last and entry > 0:
        out["pnl_pct"] = (float(last) / float(entry) - 1.0) * 100.0
    if entry and maxp and entry > 0:
        out["up_pct"] = (float(maxp) / float(entry) - 1.0) * 100.0
    if entry and minp and entry > 0:
        out["dd_pct"] = (float(minp) / float(entry) - 1.0) * 100.0
    if maxp and last and maxp > 0:
        out["peak_dd_pct"] = (float(last) / float(maxp) - 1.0) * 100.0
    if maxp and minp and minp > 0:
        out["range_pct"] = (float(maxp) / float(minp) - 1.0) * 100.0
    return out


def _decision(
    *,
    action: str,
    reason: str,
    mint: str,
    symbol: str,
    pool: str,
    now_ts: float,
    hard: bool = False,
    metrics: Mapping[str, Any] | None = None,
    extras: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    d: dict[str, Any] = {
        "action": str(action),
        "reason": str(reason or "").strip(),
        "hard": bool(hard),
        "mint": str(mint),
        "symbol": str(symbol),
        "pool": str(pool),
        "ts": int(float(now_ts)),
        "engine_version": EXIT_ENGINE_VERSION,
        "instance": INSTANCE_ID,
        "safe": int(_is_safe_mode()),
        "dryrun": int(bool(CFG.dryrun)),
    }
    if metrics:
        d["metrics"] = {k: _finite(v, 0.0) for k, v in dict(metrics).items()}
    if extras:
        safe_ex: dict[str, Any] = {}
        for k, v in dict(extras).items():
            if isinstance(v, (str, int, float, bool)) or v is None:
                safe_ex[str(k)] = v
            else:
                safe_ex[str(k)] = str(v)
        d["extras"] = safe_ex
    return d


# =============================================================================
# Best-effort logging / learning hooks
# =============================================================================
async def _log_exit_event_safe(event: dict[str, Any]) -> None:
    m = _get_mod("utils_pkg.data_logger")
    fn = getattr(m, "log_exit_event", None) if m else None
    if callable(fn):
        with contextlib.suppress(Exception):
            if asyncio.iscoroutinefunction(fn):
                await fn(event)  # type: ignore[misc]
            else:
                await asyncio.to_thread(fn, event)  # type: ignore[misc]

    await _call_optional_hook(
        "utils_pkg.self_learning_ai_strategy", ("on_exit_event", "on_forced_exit"), event
    )
    await _call_optional_hook("utils_pkg.risk_manager", ("on_forced_exit", "on_exit_event"), event)


# =============================================================================
# Optional position_store reconciliation (best-effort)
# =============================================================================
async def _load_position_from_store_best_effort(mint: str) -> Mapping[str, Any] | None:
    if not mint:
        return None
    ps = _get_mod("utils_pkg.position_store")
    if not ps:
        ps = _get_mod("utils_pkg.positions")
    if not ps:
        return None

    candidates = (
        "get_position",
        "get_open_position",
        "get_position_snapshot",
        "get_open_position_snapshot",
        "load_position",
    )
    for nm in candidates:
        fn = getattr(ps, nm, None)
        if not callable(fn):
            continue
        try:
            if asyncio.iscoroutinefunction(fn):
                out = await fn(mint)  # type: ignore[misc]
            else:
                out = await asyncio.to_thread(fn, mint)  # type: ignore[misc]
            if isinstance(out, Mapping):
                return out
        except Exception:
            continue
    return None


# =============================================================================
# Optional risk_manager gate (best-effort; NO hard dependency)
# =============================================================================
async def _risk_gate_async(
    decision: Mapping[str, Any], snapshot: Mapping[str, Any]
) -> tuple[bool, str]:
    hard = bool(decision.get("hard") or False)
    rm = _get_mod("utils_pkg.risk_manager")
    if not rm:
        return True, "no_risk_manager"

    candidates = (
        "allow_exit",
        "can_exit",
        "exit_allowed",
        "check_exit_allowed",
        "is_exit_allowed",
        "guard_exit",
        "gate_exit",
    )
    for nm in candidates:
        fn = getattr(rm, nm, None)
        if not callable(fn):
            continue
        try:
            out = fn(dict(decision), dict(snapshot))
            out = await _maybe_await(out)
            if isinstance(out, bool):
                if (not out) and hard:
                    return True, "risk_gate_block_soft_only"
                return bool(out), "risk_gate_bool"
            if isinstance(out, Mapping):
                allow = out.get("allow")
                if isinstance(allow, bool):
                    if (not allow) and hard:
                        return True, "risk_gate_block_soft_only"
                    return bool(allow), str(out.get("reason") or "risk_gate_map")
        except Exception:
            continue
    return True, "risk_gate_unknown"


# =============================================================================
# Execution delegation (AI → worker → direct)
# =============================================================================
async def _enqueue_exit_request_best_effort(**kwargs: Any) -> bool:
    candidates = [
        ("utils_pkg.auto_exit_worker", "enqueue_exit_request"),
        ("utils_pkg.auto_exit_worker.worker", "enqueue_exit_request"),
        ("utils_pkg.exit_worker", "enqueue_exit_request"),
        ("utils_pkg.exit_worker.worker", "enqueue_exit_request"),
    ]
    for mod_name, fn_name in candidates:
        mod = _get_mod(mod_name)
        fn = getattr(mod, fn_name, None) if mod else None
        if not callable(fn):
            continue
        try:
            await _maybe_await(fn(**kwargs))
            return True
        except Exception:
            continue
    return False


def _scale_amount(amount_raw: int, sell_pct: float) -> int:
    if amount_raw <= 0:
        return 0
    pct = _clamp(float(sell_pct), 0.01, 1.0)
    amt = int(amount_raw * pct)
    return max(1, amt)


@dataclass(frozen=True)
class _QuoteSanity:
    ok: bool
    price_impact_pct: float | None = None
    notes: str = ""


def _parse_price_impact_pct(v: Any) -> float | None:
    f = _to_float(v)
    if f is None:
        return None
    if f != f:
        return None
    if 0.0 <= f <= 1.0:
        return f * 100.0
    if 0.0 <= f <= 10000.0:
        return f
    return None


async def _quote_sell_best_effort(
    *, mint: str, amount_raw: int, slippage_bps: int, timeout_s: float
) -> _QuoteSanity:
    if not (CFG.quote_sanity_enabled and _allow_network() and _sells_allowed()):
        return _QuoteSanity(ok=True, notes="disabled")

    probes: list[tuple[str, tuple[str, ...]]] = [
        ("utils_pkg.router", ("quote_sell", "sell_quote", "quote", "get_quote", "quote_swap")),
        ("utils_pkg.lightning_impl", ("quote_sell", "quote", "get_quote")),
        ("utils_pkg.jup_client", ("quote", "get_quote")),
        ("utils_pkg.jupiter", ("quote", "get_quote")),
    ]

    async def _call(fn: Callable[..., Any]) -> Any:
        with contextlib.suppress(TypeError):
            return await _maybe_await(
                fn(mint=mint, amount_raw=int(amount_raw), slippage_bps=int(slippage_bps))
            )
        with contextlib.suppress(TypeError):
            return await _maybe_await(fn(mint, int(amount_raw), int(slippage_bps)))
        with contextlib.suppress(TypeError):
            return await _maybe_await(
                fn(
                    input_mint=mint,
                    output_mint=_SOL_MINT,
                    amount_raw=int(amount_raw),
                    slippage_bps=int(slippage_bps),
                )
            )
        with contextlib.suppress(TypeError):
            return await _maybe_await(
                fn(mint=mint, amount=int(amount_raw), slippage_bps=int(slippage_bps))
            )
        return await _maybe_await(fn(mint, int(amount_raw)))

    try:
        for mod_name, fn_names in probes:
            mod = _get_mod(mod_name)
            if not mod:
                continue
            for nm in fn_names:
                fn = getattr(mod, nm, None)
                if not callable(fn):
                    continue
                try:
                    out = await asyncio.wait_for(_call(fn), timeout=float(timeout_s))
                except asyncio.TimeoutError:
                    return _QuoteSanity(ok=False, notes=f"timeout:{mod_name}.{nm}")
                except Exception:
                    continue

                if isinstance(out, Mapping):
                    pi = None
                    for k in (
                        "priceImpactPct",
                        "price_impact_pct",
                        "priceImpact",
                        "price_impact",
                        "pi_pct",
                    ):
                        if k in out:
                            pi = _parse_price_impact_pct(out.get(k))
                            if pi is not None:
                                break
                    return _QuoteSanity(ok=True, price_impact_pct=pi, notes=f"{mod_name}.{nm}")

                pi2 = _parse_price_impact_pct(out)
                if pi2 is not None:
                    return _QuoteSanity(
                        ok=True, price_impact_pct=pi2, notes=f"{mod_name}.{nm}:scalar"
                    )

        return _QuoteSanity(ok=False, notes="no_quote_fn")
    except Exception as e:
        return _QuoteSanity(ok=False, notes=_short_exc(e))


async def _direct_sell_best_effort(*, mint: str, amount_raw: int, slippage_bps: int) -> str | None:
    if not _sells_allowed():
        return None

    with contextlib.suppress(Exception):
        li = _get_mod("utils_pkg.lightning_impl")
        fn = getattr(li, "sell_via_lightning", None) if li else None
        if callable(fn):
            with contextlib.suppress(TypeError):
                sig = await _maybe_await(
                    fn(mint=mint, amount_raw=int(amount_raw), slippage_bps=int(slippage_bps))
                )
                if isinstance(sig, str) and sig.strip():
                    return sig.strip()
            sig = await _maybe_await(fn(mint, int(amount_raw), int(slippage_bps)))
            if isinstance(sig, str) and sig.strip():
                return sig.strip()

    with contextlib.suppress(Exception):
        router = _get_mod("utils_pkg.router")
        fn2 = (
            (getattr(router, "sell", None) or getattr(router, "sell_token", None))
            if router
            else None
        )
        if callable(fn2):
            with contextlib.suppress(TypeError):
                sig2 = await _maybe_await(
                    fn2(mint=mint, amount_raw=int(amount_raw), slippage_bps=int(slippage_bps))
                )
                if isinstance(sig2, str) and sig2.strip():
                    return sig2.strip()
            sig2 = await _maybe_await(fn2(mint, int(amount_raw), int(slippage_bps)))
            if isinstance(sig2, str) and sig2.strip():
                return sig2.strip()

    return None


async def _delegate_exit(
    *, decision: dict[str, Any], snapshot: Mapping[str, Any], amount_raw: int | None
) -> bool:
    mint = _norm_mint(str(decision.get("mint") or ""))
    if not mint or mint == _SOL_MINT:
        return False

    now = float(_to_float(decision.get("ts")) or time.time())

    # gate: risk_manager (best-effort). Hard exits are allowed even if it tries to block.
    allow, gate_note = await _risk_gate_async(decision, snapshot)
    if not allow and not bool(decision.get("hard") or False):
        _log(
            "EXIT_BLOCKED_BY_RISK_GATE",
            level="warning",
            mint=mint,
            note=gate_note,
            reason=str(decision.get("reason") or ""),
        )
        return False

    if not _exit_dedupe_ok(mint, now) or not _burst_cap_ok(now):
        return False

    r = await _get_redis_async()
    if r and int(CFG.max_daily_exits) > 0:
        day_key = _today_key(CFG.daily_key_prefix)
        daily = await _redis_get_int(r, day_key)
        if daily >= int(CFG.max_daily_exits):
            _log(
                "EXIT_DAILY_CAP_REACHED",
                level="warning",
                mint=mint,
                daily=daily,
                cap=int(CFG.max_daily_exits),
            )
            return False

    if not _sells_allowed() or bool(CFG.dryrun):
        return False

    amt_full = int(amount_raw) if isinstance(amount_raw, int) and amount_raw > 0 else 0
    if amt_full <= 0:
        return False

    sell_pct_val = _to_float(snapshot.get("sell_pct"))
    sell_pct = float(sell_pct_val) if sell_pct_val is not None else float(CFG.fallback_sell_pct)
    sell_pct = _clamp(sell_pct, 0.01, 1.0)

    slippage_bps = int(CFG.fallback_slippage_bps)
    priority = int(CFG.fallback_priority)

    scaled_amt = _scale_amount(int(amt_full), sell_pct)
    hard = bool(decision.get("hard") or False)

    quote_info: _QuoteSanity | None = None
    if CFG.quote_sanity_enabled and _allow_network():
        quote_info = await _quote_sell_best_effort(
            mint=mint,
            amount_raw=int(scaled_amt),
            slippage_bps=int(slippage_bps),
            timeout_s=float(CFG.quote_timeout_s),
        )

        pi = quote_info.price_impact_pct if quote_info else None
        if quote_info and not quote_info.ok:
            if hard and not bool(CFG.quote_allow_fail_on_hard_stop):
                _log(
                    "EXIT_QUOTE_FAIL_BLOCK_HARD", level="warning", mint=mint, note=quote_info.notes
                )
                return False
            _log(
                "EXIT_QUOTE_FAIL_SOFT",
                level="warning",
                mint=mint,
                hard=int(hard),
                note=quote_info.notes,
            )

        if (
            (pi is not None)
            and (pi > float(CFG.quote_max_price_impact_pct))
            and (not hard)
            and bool(CFG.quote_reduce_soft_exits)
        ):
            floor_pct = float(CFG.quote_reduce_floor_sell_pct)
            cur_pct = float(sell_pct)
            cur_amt = int(scaled_amt)
            for _ in range(6):
                if cur_pct <= floor_pct or cur_amt <= 1:
                    break
                cur_pct = max(floor_pct, cur_pct * 0.5)
                cur_amt = max(1, _scale_amount(int(amt_full), cur_pct))
                qi2 = await _quote_sell_best_effort(
                    mint=mint,
                    amount_raw=int(cur_amt),
                    slippage_bps=int(slippage_bps),
                    timeout_s=float(CFG.quote_timeout_s),
                )
                quote_info = qi2
                pi2 = qi2.price_impact_pct
                if pi2 is None or pi2 <= float(CFG.quote_max_price_impact_pct):
                    sell_pct = cur_pct
                    scaled_amt = cur_amt
                    break

            if (
                quote_info
                and quote_info.price_impact_pct is not None
                and quote_info.price_impact_pct > float(CFG.quote_max_price_impact_pct)
            ):
                _log(
                    "EXIT_QUOTE_PI_TOO_HIGH_BLOCK_SOFT",
                    level="warning",
                    mint=mint,
                    pi_pct=float(quote_info.price_impact_pct),
                    max_pi=float(CFG.quote_max_price_impact_pct),
                    sell_pct=float(sell_pct),
                )
                return False

    # AI sell brain hook (best effort)
    for hook_name in ("schedule_force_exit", "force_exit", "request_exit", "exit_now"):
        hook = _sell_hook(hook_name)
        if hook is None:
            continue
        try:
            payload = {
                "mint": mint,
                "amount_raw": int(amt_full),
                "sell_pct": float(sell_pct),
                "reason": str(decision.get("reason") or "exit"),
                "hard": int(bool(hard)),
                "priority": int(priority),
                "slippage_bps": int(slippage_bps),
                "ts": now,
                "version": EXIT_ENGINE_VERSION,
                "instance": INSTANCE_ID,
                "decision": decision,
                "snapshot": dict(snapshot),
            }
            if quote_info:
                payload["quote_sanity"] = {
                    "ok": int(bool(quote_info.ok)),
                    "pi_pct": quote_info.price_impact_pct,
                    "note": quote_info.notes,
                }
            await _maybe_await(hook(payload))
            return True
        except Exception as e:
            _log("EXIT_AI_HOOK_FAIL", level="warning", mint=mint, hook=hook_name, err=_short_exc(e))

    if CFG.allow_worker_enqueue:
        ok = await _enqueue_exit_request_best_effort(
            mint=mint,
            amount_raw=int(scaled_amt),
            reason=str(decision.get("reason") or "exit"),
            sell_pct=float(sell_pct),
            entry_spend_sol=float(_to_float(snapshot.get("spend_sol")) or 0.0),
            symbol=str(snapshot.get("symbol") or ""),
            pool=str(snapshot.get("pool") or ""),
            slippage_bps=int(slippage_bps),
            priority=int(priority),
            candidate_id=snapshot.get("candidate_id"),
            hard=int(bool(hard)),
            dryrun=int(bool(CFG.dryrun)),
            version=EXIT_ENGINE_VERSION,
            instance=INSTANCE_ID,
            decision=decision,
            quote_pi_pct=(quote_info.price_impact_pct if quote_info else None),
            quote_note=(quote_info.notes if quote_info else None),
        )
        if ok:
            if r and int(CFG.max_daily_exits) > 0:
                await _redis_incr_with_ttl(r, _today_key(CFG.daily_key_prefix), 60 * 60 * 36)
            return True

    if not CFG.allow_direct_fallback:
        return False

    sig = await _direct_sell_best_effort(
        mint=mint, amount_raw=int(scaled_amt), slippage_bps=int(slippage_bps)
    )
    if sig and r and int(CFG.max_daily_exits) > 0:
        await _redis_incr_with_ttl(r, _today_key(CFG.daily_key_prefix), 60 * 60 * 36)
    return bool(sig)


# =============================================================================
# Public API: manual request_exit
# =============================================================================
async def request_exit(ev: dict[str, Any]) -> bool:
    if not CFG.enabled:
        return False

    mint = _extract_mint(ev) or _norm_mint(str(ev.get("mint") or ""))
    if not mint:
        return False

    amount_raw = _extract_amount_raw(ev)
    symbol = _extract_symbol(ev, mint)
    pool = _extract_pool(ev)
    now = time.time()

    snapshot: dict[str, Any] = dict(ev)
    if "sell_pct" not in snapshot and "sellPct" in snapshot:
        snapshot["sell_pct"] = snapshot.get("sellPct")

    decision = _decision(
        action="EXIT",
        reason=str(ev.get("reason") or "external_request"),
        mint=mint,
        symbol=symbol,
        pool=pool,
        now_ts=now,
        hard=bool(ev.get("hard") or False),
        extras={"source": "exit_engine.request_exit"},
    )
    await _log_exit_event_safe({**decision, "label": "exit_requested"})
    ok = await _delegate_exit(decision=decision, snapshot=snapshot, amount_raw=amount_raw)
    return bool(ok)


# =============================================================================
# Public API: buy filled
# =============================================================================
async def on_buy_filled(ev: dict[str, Any]) -> None:
    if not CFG.enabled:
        return

    mint = _extract_mint(ev) or _norm_mint(str(ev.get("mint") or ""))
    if not mint or mint == _SOL_MINT:
        return

    symbol = _extract_symbol(ev, mint)
    pool = _extract_pool(ev)
    spend_sol = float(_extract_spend_sol(ev) or 0.0)
    amount_raw = _extract_amount_raw(ev)

    entry_ts = _extract_ts(ev, time.time())
    entry_price = None
    for k in ("entry_price", "price_entry", "price_entry_ui", "avg_entry_price", "entryPrice"):
        f = _to_float(ev.get(k))
        if f is not None and f > 0:
            entry_price = float(f)
            break

    liq_usd = _extract_liq_usd(ev)
    cand = ev.get("candidate_id") or ev.get("entry_candidate_id") or ev.get("cand_id")
    entry_sig = str(ev.get("signature") or ev.get("tx_sig") or ev.get("sig") or "").strip()

    pos = _Pos(
        mint=mint,
        symbol=symbol,
        pool=pool,
        entry_sig=entry_sig,
        spend_sol=float(spend_sol),
        amount_raw=_coerce_int_opt(amount_raw),
        candidate_id=cand,
        entry_ts=float(entry_ts),
        opened_from="on_buy_filled",
        entry_price=entry_price,
        entry_liq_usd=liq_usd,
        last_liq_usd=liq_usd,
        last_price=entry_price,
        max_price=entry_price,
        min_price=entry_price,
        last_tick_ts=float(entry_ts),
    )

    async with _OPEN_LOCK:
        _OPEN[mint] = pos

    await _refresh_snapshot_cache(time.time())

    if not _is_safe_mode():
        fn = _sell_hook("schedule_after_buy")
        if fn is not None:
            with contextlib.suppress(Exception):
                payload = dict(ev)
                payload.update(
                    {
                        "mint": mint,
                        "symbol": symbol,
                        "pool": pool,
                        "spend_sol": float(spend_sol),
                        "amount_raw": amount_raw,
                        "candidate_id": cand,
                        "ts": time.time(),
                        "dryrun": int(bool(CFG.dryrun)),
                        "exit_engine_version": EXIT_ENGINE_VERSION,
                        "instance": INSTANCE_ID,
                    }
                )
                await _maybe_await(fn(payload))


async def on_buy_filled_event(ev: dict[str, Any]) -> None:
    await on_buy_filled(ev)


async def on_buy_filled_legacy(raw_id_or_mint: str, tx_sig: str) -> None:
    ev: dict[str, Any] = {
        "mint": raw_id_or_mint,
        "signature": tx_sig,
        "symbol": "",
        "spend_sol": 0.0,
        "pool": "legacy",
        "ts": time.time(),
    }
    await on_buy_filled(ev)


# =============================================================================
# Public API: sell filled (best-effort outcome logging)
# =============================================================================
async def on_sell_filled(ev: dict[str, Any]) -> None:
    mint = _extract_mint(ev) or _norm_mint(str(ev.get("mint") or ""))
    if not mint:
        return

    async with _OPEN_LOCK:
        _OPEN.pop(mint, None)

    payload = dict(ev)
    payload.setdefault("mint", mint)
    payload.setdefault("ts", time.time())
    payload.setdefault("version", EXIT_ENGINE_VERSION)
    payload.setdefault("instance", INSTANCE_ID)
    payload.setdefault("dryrun", int(bool(CFG.dryrun)))
    await _log_exit_event_safe({**payload, "label": payload.get("label") or "sell_filled"})

    await _call_optional_hook("utils_pkg.guardrails", ("note_sell_fill", "on_sell_filled"), payload)
    await _call_optional_hook(
        "utils_pkg.reinvestor", ("on_sell_filled", "on_realized_profit"), payload
    )
    await _refresh_snapshot_cache(time.time())


# =============================================================================
# TP ladder (optional): only if provided in payload/meta; no env keys
# =============================================================================
def _parse_tp_ladder_from_any(obj: Mapping[str, Any] | None) -> list[tuple[float, float]]:
    """
    Accepts:
      - tp_ladder: [(pnl_pct, sell_pct), ...]
      - tp_ladder: [{"pnl_pct": 25, "sell_pct": 0.25}, ...]
      - tp_levels / take_profit_levels similarly
    Returns sorted unique ladder by pnl_pct ascending.
    """
    if not obj:
        return []
    for key in ("tp_ladder", "tp_levels", "take_profit_levels", "take_profit"):
        raw = obj.get(key)
        if raw is None:
            continue
        ladder: list[tuple[float, float]] = []
        if isinstance(raw, (list, tuple)):
            for it in raw:
                if isinstance(it, (list, tuple)) and len(it) >= 2:
                    a = _to_float(it[0])
                    b = _to_float(it[1])
                    if a is not None and b is not None:
                        ladder.append((float(a), float(b)))
                elif isinstance(it, Mapping):
                    a2 = _to_float(
                        it.get("pnl_pct") or it.get("tp_pct") or it.get("pnl") or it.get("pct")
                    )
                    b2 = _to_float(
                        it.get("sell_pct") or it.get("sell") or it.get("portion") or it.get("size")
                    )
                    if a2 is not None and b2 is not None:
                        ladder.append((float(a2), float(b2)))
        if ladder:
            norm: dict[float, float] = {}
            for pnl, sp in ladder:
                if pnl != pnl:
                    continue
                sp2 = _clamp(float(sp), 0.01, 1.0)
                norm[float(pnl)] = sp2
            out = sorted(norm.items(), key=lambda x: x[0])
            return [(p, s) for (p, s) in out]
    return []


# =============================================================================
# The DECIDER (contract tests call this)
# =============================================================================
def _map_position_fields(pos: Mapping[str, Any]) -> dict[str, Any]:
    mint = _extract_mint(pos) or _norm_mint(str(pos.get("mint") or ""))
    entry_ts = _extract_ts(pos, time.time())
    spend_sol = float(_extract_spend_sol(pos) or 0.0)
    amount_raw = _extract_amount_raw(pos)

    entry_price = None
    for k in ("entry_price", "avg_entry_price", "price_entry", "price_entry_ui", "entryPrice"):
        f = _to_float(pos.get(k))
        if f is not None and f > 0:
            entry_price = float(f)
            break

    return {
        "mint": mint,
        "symbol": str(pos.get("symbol") or pos.get("sym") or (mint[:4] if mint else "")).strip(),
        "pool": _pool_normalize(str(pos.get("pool") or pos.get("source") or "unknown")),
        "entry_ts": float(entry_ts),
        "spend_sol": float(spend_sol),
        "amount_raw": _coerce_int_opt(amount_raw),
        "candidate_id": pos.get("candidate_id"),
        "entry_price": entry_price,
        "entry_liq_usd": _extract_liq_usd(pos),
        "follow_done": bool(pos.get("follow_done") or False),
        "follow_pass": bool(pos.get("follow_pass") or False),
        "tp_ladder": _parse_tp_ladder_from_any(pos),
    }


def _map_tick_fields(tick: Mapping[str, Any], now: float) -> dict[str, Any]:
    mint = _extract_mint(tick) or _norm_mint(str(tick.get("mint") or ""))
    return {
        "mint": mint,
        "ts": float(_extract_ts(tick, now)),
        "price": _extract_price_any(tick),
        "trades": _extract_trades_count(tick),
        "liq_usd": _extract_liq_usd(tick),
    }


def _deterministic_now(
    now_ts: int | float | None, tick_in: Mapping[str, Any] | None, pos_in: Mapping[str, Any] | None
) -> float:
    if now_ts is not None:
        f = _to_float(now_ts)
        if f is not None:
            return float(f)

    if _is_safe_mode():
        if tick_in:
            t = _parse_ts_any(tick_in.get("ts") or tick_in.get("time") or tick_in.get("timestamp"))
            if t is not None:
                return float(t)
        if pos_in:
            t2 = _parse_ts_any(pos_in.get("now_ts") or pos_in.get("ts") or pos_in.get("entry_ts"))
            if t2 is not None:
                return float(t2)

    return float(time.time())


async def on_trade_tick(
    raw_event: dict[str, Any] | None = None,
    *,
    position: Mapping[str, Any] | None = None,
    pos: Mapping[str, Any] | None = None,
    payload: Mapping[str, Any] | None = None,
    position_payload: Mapping[str, Any] | None = None,
    state: Mapping[str, Any] | None = None,
    position_state: Mapping[str, Any] | None = None,
    trade: Mapping[str, Any] | None = None,
    tick: Mapping[str, Any] | None = None,
    meta: Mapping[str, Any] | None = None,
    now_ts: int | float | None = None,
    **kwargs: Any,
) -> dict[str, Any] | None:
    if not CFG.enabled:
        return None

    pos_in = (
        _as_mapping(position)
        or _as_mapping(pos)
        or _as_mapping(payload)
        or _as_mapping(position_payload)
        or _as_mapping(state)
        or _as_mapping(position_state)
        or _as_mapping(trade)
    )
    tick_in = _as_mapping(tick) or _as_mapping(raw_event) or {}
    meta_in = _as_mapping(meta) or {}

    now = _deterministic_now(now_ts, tick_in, pos_in)
    with contextlib.suppress(Exception):
        await _housekeeping(now)

    mint = _extract_mint(tick_in) or (_extract_mint(pos_in) if pos_in else None)
    mint = _norm_mint(mint or "")
    if not mint or mint == _SOL_MINT:
        return None

    tick_norm = (
        _map_tick_fields(tick_in, now)
        if tick_in
        else {"mint": mint, "ts": now, "price": None, "trades": None, "liq_usd": None}
    )
    pos_norm: dict[str, Any] | None = _map_position_fields(pos_in) if pos_in else None

    if pos_norm is None or not pos_norm.get("mint"):
        async with _OPEN_LOCK:
            p = _OPEN.get(mint)
            if p and not p.closed:
                pos_norm = {
                    "mint": p.mint,
                    "symbol": p.symbol,
                    "pool": p.pool,
                    "entry_ts": float(p.entry_ts),
                    "spend_sol": float(p.spend_sol),
                    "amount_raw": p.amount_raw,
                    "candidate_id": p.candidate_id,
                    "entry_price": p.entry_price,
                    "entry_liq_usd": p.entry_liq_usd,
                    "follow_done": bool(p.follow_done),
                    "follow_pass": bool(p.follow_pass),
                    "tp_ladder": [],
                }

    if pos_norm is None or not pos_norm.get("mint"):
        with contextlib.suppress(Exception):
            store_pos = await _load_position_from_store_best_effort(mint)
            if store_pos:
                pos_norm = _map_position_fields(store_pos)

    if pos_norm is None:
        return _decision(
            action="HOLD",
            reason="no_position",
            mint=mint,
            symbol=_extract_symbol(tick_in, mint),
            pool=_extract_pool(tick_in),
            now_ts=now,
            extras={"note": "position_missing"},
        )

    symbol = str(pos_norm.get("symbol") or _extract_symbol(tick_in, mint) or mint[:4])
    pool = str(pos_norm.get("pool") or _extract_pool(tick_in) or "unknown")
    entry_ts = float(_to_float(pos_norm.get("entry_ts")) or now)
    spend_sol = float(_to_float(pos_norm.get("spend_sol")) or 0.0)

    entry_price = _to_float(pos_norm.get("entry_price"))
    entry_liq = _to_float(pos_norm.get("entry_liq_usd"))
    last_liq = _to_float(tick_norm.get("liq_usd")) or _to_float(_extract_liq_usd(meta_in))

    last_price = _to_float(tick_norm.get("price"))
    trades = _to_int(tick_norm.get("trades"))

    trades_after: int
    max_price: float | None
    min_price: float | None
    amount_raw_opt: int | None
    tp_next_idx: int

    async with _OPEN_LOCK:
        p = _OPEN.get(mint)
        if p and not p.closed:
            if CFG.tick_throttle_ms > 0 and (now - p.last_eval_ts) * 1000.0 < float(
                CFG.tick_throttle_ms
            ):
                return _decision(
                    action="HOLD",
                    reason="throttled",
                    mint=mint,
                    symbol=symbol,
                    pool=pool,
                    now_ts=now,
                )

            p.last_eval_ts = now
            p.last_tick_ts = now

            if trades is not None:
                if p.last_feed_trade_counter is None:
                    p.last_feed_trade_counter = int(trades)
                else:
                    delta = int(trades) - int(p.last_feed_trade_counter)
                    p.last_feed_trade_counter = int(trades)
                    p.trades_after_entry += max(0, delta)

            if last_liq is not None:
                p.last_liq_usd = float(last_liq)
                if p.entry_liq_usd is None and entry_liq is not None:
                    p.entry_liq_usd = float(entry_liq)

            if last_price is not None:
                lp = float(last_price)
                p.last_price = lp
                if p.entry_price is None and entry_price is not None:
                    p.entry_price = float(entry_price)
                if p.entry_price is None:
                    p.entry_price = lp
                if p.max_price is None or lp > float(p.max_price):
                    p.max_price = lp
                if p.min_price is None or lp < float(p.min_price):
                    p.min_price = lp

            entry_price = p.entry_price
            last_price = p.last_price
            max_price = p.max_price
            min_price = p.min_price
            trades_after = int(p.trades_after_entry)
            amount_raw_opt = p.amount_raw
            tp_next_idx = int(p.tp_next_idx)
        else:
            max_price = last_price
            min_price = last_price
            trades_after = int(trades or 0)
            amount_raw_opt = _coerce_int_opt(pos_norm.get("amount_raw"))
            tp_next_idx = 0

    if max_price is None:
        mp = _to_float(meta_in.get("max_price") or meta_in.get("high") or meta_in.get("ath"))
        if mp is not None:
            max_price = float(mp)
    if min_price is None:
        mn = _to_float(meta_in.get("min_price") or meta_in.get("low"))
        if mn is not None:
            min_price = float(mn)

    age_sec = max(0.0, float(now - float(entry_ts)))
    age_min = age_sec / 60.0

    metrics = _compute_metrics(entry_price, max_price, min_price, last_price)

    # TP ladder can come from position OR meta
    tp_ladder = []
    with contextlib.suppress(Exception):
        if isinstance(pos_norm.get("tp_ladder"), list):
            tp_ladder = [
                (float(a), float(b))
                for (a, b) in pos_norm["tp_ladder"]
                if isinstance(a, (int, float)) and isinstance(b, (int, float))
            ]
    if not tp_ladder:
        tp_ladder = _parse_tp_ladder_from_any(meta_in)

    snapshot: dict[str, Any] = {
        "mint": mint,
        "symbol": symbol,
        "pool": pool,
        "entry_ts": float(entry_ts),
        "age_sec": float(age_sec),
        "spend_sol": float(spend_sol),
        "amount_raw": amount_raw_opt,  # int|None
        "candidate_id": pos_norm.get("candidate_id"),
        "entry_price": entry_price,
        "last_price": last_price,
        "max_price": max_price,
        "min_price": min_price,
        "entry_liq_usd": entry_liq,
        "last_liq_usd": last_liq,
        "trades_after_entry": int(trades_after),
        "sell_pct": float(CFG.fallback_sell_pct),
    }
    amt_for_exit: int | None = amount_raw_opt

    if not _scope_ok(snapshot):
        return _decision(
            action="HOLD",
            reason="scope_block",
            mint=mint,
            symbol=symbol,
            pool=pool,
            now_ts=now,
            metrics=metrics,
        )

    if (
        entry_price is None
        or last_price is None
        or float(entry_price) <= 0
        or float(last_price) <= 0
    ):
        return _decision(
            action="HOLD",
            reason="missing_price",
            mint=mint,
            symbol=symbol,
            pool=pool,
            now_ts=now,
            extras={"age_sec": age_sec},
        )

    # -----------------------------
    # RULES (deterministic)
    # -----------------------------

    # 0) Hard stop-loss
    if float(metrics["pnl_pct"]) <= -float(CFG.stop_loss_pct):
        dec = _decision(
            action="EXIT",
            reason="stop_loss",
            mint=mint,
            symbol=symbol,
            pool=pool,
            now_ts=now,
            hard=True,
            metrics=metrics,
            extras={"age_sec": age_sec},
        )
        await _log_exit_event_safe({**dec, "label": "exit_signal"})
        _ = await _delegate_exit(decision=dec, snapshot=snapshot, amount_raw=amt_for_exit)
        return dec

    # 1) Hard liquidity floor
    if float(CFG.min_liq_usd) > 0.0 and snapshot.get("last_liq_usd") is not None:
        if float(_to_float(snapshot["last_liq_usd"]) or 0.0) < float(CFG.min_liq_usd):
            dec = _decision(
                action="EXIT",
                reason="liq_collapse_hard",
                mint=mint,
                symbol=symbol,
                pool=pool,
                now_ts=now,
                hard=True,
                metrics=metrics,
                extras={"liq_usd": snapshot.get("last_liq_usd")},
            )
            await _log_exit_event_safe({**dec, "label": "exit_signal"})
            _ = await _delegate_exit(decision=dec, snapshot=snapshot, amount_raw=amt_for_exit)
            return dec

    # 2) Liquidity collapse % from entry
    if (
        float(CFG.liq_collapse_pct) > 0.0
        and snapshot.get("entry_liq_usd")
        and snapshot.get("last_liq_usd")
    ):
        e = float(_to_float(snapshot["entry_liq_usd"]) or 0.0)
        c = float(_to_float(snapshot["last_liq_usd"]) or 0.0)
        if e > 0 and c > 0:
            drop_pct = (c / e - 1.0) * 100.0
            if drop_pct <= -float(CFG.liq_collapse_pct):
                dec = _decision(
                    action="EXIT",
                    reason="liq_collapse",
                    mint=mint,
                    symbol=symbol,
                    pool=pool,
                    now_ts=now,
                    hard=False,
                    metrics={**metrics, "liq_drop_pct": drop_pct},
                    extras={"age_sec": age_sec},
                )
                await _log_exit_event_safe({**dec, "label": "exit_signal"})
                _ = await _delegate_exit(decision=dec, snapshot=snapshot, amount_raw=amt_for_exit)
                return dec

    # 3) Optional TP ladder (only if provided)
    if tp_ladder and float(metrics["pnl_pct"]) > 0.0:
        if 0 <= tp_next_idx < len(tp_ladder):
            target_pnl, sell_pct = tp_ladder[tp_next_idx]
            if float(metrics["pnl_pct"]) >= float(target_pnl):
                snapshot["sell_pct"] = _clamp(float(sell_pct), 0.01, 1.0)
                dec = _decision(
                    action="EXIT",
                    reason=f"tp_ladder_{tp_next_idx}",
                    mint=mint,
                    symbol=symbol,
                    pool=pool,
                    now_ts=now,
                    hard=False,
                    metrics=metrics,
                    extras={
                        "tp_target_pnl_pct": float(target_pnl),
                        "sell_pct": float(snapshot["sell_pct"]),
                    },
                )
                await _log_exit_event_safe({**dec, "label": "exit_signal"})
                ok = await _delegate_exit(decision=dec, snapshot=snapshot, amount_raw=amt_for_exit)
                if ok:
                    async with _OPEN_LOCK:
                        p5 = _OPEN.get(mint)
                        if p5 and not p5.closed:
                            p5.tp_next_idx = int(p5.tp_next_idx) + 1
                return dec

    # 4) Trailing stop (armed by up%)
    if float(CFG.trailing_arm_pct) > 0.0:
        if float(metrics["up_pct"]) >= float(CFG.trailing_arm_pct) and float(
            metrics["peak_dd_pct"]
        ) <= -float(CFG.trailing_stop_pct):
            dec = _decision(
                action="EXIT",
                reason="trailing_stop",
                mint=mint,
                symbol=symbol,
                pool=pool,
                now_ts=now,
                hard=False,
                metrics=metrics,
                extras={"age_sec": age_sec},
            )
            await _log_exit_event_safe({**dec, "label": "exit_signal"})
            _ = await _delegate_exit(decision=dec, snapshot=snapshot, amount_raw=amt_for_exit)
            return dec

    # 5) Time stop
    if int(CFG.time_stop_min) > 0 and age_min >= float(CFG.time_stop_min):
        if float(metrics["pnl_pct"]) < float(CFG.time_stop_min_profit_pct):
            dec = _decision(
                action="EXIT",
                reason="time_stop",
                mint=mint,
                symbol=symbol,
                pool=pool,
                now_ts=now,
                hard=False,
                metrics=metrics,
                extras={"age_min": age_min},
            )
            await _log_exit_event_safe({**dec, "label": "exit_signal"})
            _ = await _delegate_exit(decision=dec, snapshot=snapshot, amount_raw=amt_for_exit)
            return dec

    # 6) Stagnant exit
    if int(CFG.stagnant_max_min) > 0 and age_min >= float(CFG.stagnant_max_min):
        if float(metrics["range_pct"]) < float(CFG.stagnant_min_move_pct):
            dec = _decision(
                action="EXIT",
                reason="stagnant",
                mint=mint,
                symbol=symbol,
                pool=pool,
                now_ts=now,
                hard=False,
                metrics=metrics,
                extras={"age_min": age_min},
            )
            await _log_exit_event_safe({**dec, "label": "exit_signal"})
            _ = await _delegate_exit(decision=dec, snapshot=snapshot, amount_raw=amt_for_exit)
            return dec

    # 7) Follow-through gate
    if CFG.follow_enabled:
        follow_done = bool(pos_norm.get("follow_done") or False)
        follow_pass_prev = bool(pos_norm.get("follow_pass") or False)
        async with _OPEN_LOCK:
            p2 = _OPEN.get(mint)
            if p2 and not p2.closed:
                follow_done = bool(p2.follow_done)
                follow_pass_prev = bool(p2.follow_pass)

        if not (follow_done and follow_pass_prev):
            warmup_end = float(CFG.follow_window_sec + max(0, CFG.follow_grace_sec))
            if age_sec >= warmup_end:
                if float(CFG.follow_early_catastrophic_dd_pct) > 0.0 and float(
                    metrics["dd_pct"]
                ) <= -float(CFG.follow_early_catastrophic_dd_pct):
                    dec = _decision(
                        action="EXIT",
                        reason="catastrophic_dd",
                        mint=mint,
                        symbol=symbol,
                        pool=pool,
                        now_ts=now,
                        hard=True,
                        metrics=metrics,
                        extras={"age_sec": age_sec},
                    )
                    await _log_exit_event_safe({**dec, "label": "exit_signal"})
                    _ = await _delegate_exit(
                        decision=dec, snapshot=snapshot, amount_raw=amt_for_exit
                    )
                    async with _OPEN_LOCK:
                        p3 = _OPEN.get(mint)
                        if p3 and not p3.closed:
                            p3.follow_done = True
                            p3.follow_pass = False
                    return dec

                extend_end = warmup_end + float(max(0, CFG.follow_extend_low_trades_sec))
                if int(trades_after) < int(CFG.follow_min_trades) and age_sec < extend_end:
                    return _decision(
                        action="HOLD",
                        reason="follow_wait",
                        mint=mint,
                        symbol=symbol,
                        pool=pool,
                        now_ts=now,
                        metrics=metrics,
                        extras={"trades": trades_after},
                    )

                trades_ok = int(trades_after) >= int(CFG.follow_min_trades)
                up_ok = float(metrics["up_pct"]) >= float(CFG.follow_min_up_pct)
                dd_ok = float(metrics["dd_pct"]) >= -float(CFG.follow_max_dd_pct)
                pnl_ok = (
                    True
                    if float(CFG.follow_min_pnl_pct) <= 0.0
                    else (float(metrics["pnl_pct"]) >= float(CFG.follow_min_pnl_pct))
                )
                follow_pass = bool(trades_ok and up_ok and dd_ok and pnl_ok)

                async with _OPEN_LOCK:
                    p4 = _OPEN.get(mint)
                    if p4 and not p4.closed:
                        p4.follow_done = True
                        p4.follow_pass = bool(follow_pass)

                if not follow_pass:
                    dec = _decision(
                        action="EXIT",
                        reason="follow_through_fail",
                        mint=mint,
                        symbol=symbol,
                        pool=pool,
                        now_ts=now,
                        hard=False,
                        metrics=metrics,
                        extras={"trades": trades_after},
                    )
                    await _log_exit_event_safe({**dec, "label": "exit_signal"})
                    _ = await _delegate_exit(
                        decision=dec, snapshot=snapshot, amount_raw=amt_for_exit
                    )
                    return dec

    # AI hook: informational only (no hard dependency)
    if not _is_safe_mode():
        fn_ai = _sell_hook("on_trade_tick")
        if fn_ai is not None:
            with contextlib.suppress(Exception):
                await _maybe_await(fn_ai(dict(raw_event or tick_in)))  # type: ignore[misc]

    return _decision(
        action="HOLD",
        reason="hold",
        mint=mint,
        symbol=symbol,
        pool=pool,
        now_ts=now,
        metrics=metrics,
        extras={"age_sec": age_sec},
    )


# =============================================================================
# Contract surface aliases
# =============================================================================
async def decide_exit_async(*args: Any, **kwargs: Any) -> dict[str, Any] | None:
    return await on_trade_tick(*args, **kwargs)


async def evaluate_position_async(*args: Any, **kwargs: Any) -> dict[str, Any] | None:
    return await on_trade_tick(*args, **kwargs)


async def on_position_tick_async(*args: Any, **kwargs: Any) -> dict[str, Any] | None:
    return await on_trade_tick(*args, **kwargs)


async def should_exit_async(*args: Any, **kwargs: Any) -> bool:
    d = await on_trade_tick(*args, **kwargs)
    return bool(d and str(d.get("action")) == "EXIT")


class ExitPolicy:
    async def decide(self, *args: Any, **kwargs: Any) -> dict[str, Any] | None:
        return await on_trade_tick(*args, **kwargs)


class ExitEngine(ExitPolicy):
    pass


def _run_or_schedule(coro: Any) -> Any:
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(coro)
    else:
        return loop.create_task(coro)


def decide_exit(*args: Any, **kwargs: Any) -> Any:
    return _run_or_schedule(decide_exit_async(*args, **kwargs))


def evaluate_position(*args: Any, **kwargs: Any) -> Any:
    return _run_or_schedule(evaluate_position_async(*args, **kwargs))


def on_position_tick(*args: Any, **kwargs: Any) -> Any:
    return _run_or_schedule(on_position_tick_async(*args, **kwargs))


def should_exit(*args: Any, **kwargs: Any) -> Any:
    return _run_or_schedule(should_exit_async(*args, **kwargs))


compute_exit = decide_exit
get_exit_actions = decide_exit
maybe_exit = decide_exit
exit_decision = decide_exit


# =============================================================================
# Diagnostics (stable)
# =============================================================================
def get_open_positions_snapshot() -> dict[str, dict[str, Any]]:
    return dict(_LAST_OPEN_SNAPSHOT)


# =============================================================================
# Worker loop (stable entrypoints)
# =============================================================================
async def run_worker(stop_evt: asyncio.Event) -> None:
    if _env_bool("EXIT_RELOAD_CFG_ON_START", True):
        with contextlib.suppress(Exception):
            reload_exit_engine_config()

    _log(
        "EXIT_ENGINE_WORKER_START",
        version=EXIT_ENGINE_VERSION,
        phase=int(CFG.master_phase),
        enabled=int(CFG.enabled),
        dryrun=int(CFG.dryrun),
        safe=int(_is_safe_mode()),
        instance=INSTANCE_ID,
    )

    if not CFG.enabled:
        await stop_evt.wait()
        return

    try:
        while not stop_evt.is_set():
            now = time.time()
            with contextlib.suppress(Exception):
                await _housekeeping(now)

            with contextlib.suppress(Exception):
                log.every(  # type: ignore[misc]
                    "EXIT_ENGINE_HEARTBEAT",
                    30,
                    enabled=int(CFG.enabled),
                    dryrun=int(CFG.dryrun),
                    safe=int(_is_safe_mode()),
                    phase=int(CFG.master_phase),
                    open_count=int(len(_LAST_OPEN_SNAPSHOT)),
                    instance=INSTANCE_ID,
                    version=EXIT_ENGINE_VERSION,
                )

            await asyncio.sleep(0.20)
    except asyncio.CancelledError:
        raise
    except Exception as e:
        _log("EXIT_ENGINE_WORKER_CRASH", level="error", err=_short_exc(e), instance=INSTANCE_ID)
        await asyncio.sleep(1.0)
        await stop_evt.wait()


async def exit_engine_loop(stop_evt: asyncio.Event) -> None:
    await run_worker(stop_evt)
