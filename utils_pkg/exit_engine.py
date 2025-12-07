from __future__ import annotations

"""
utils_pkg/exit_engine.py

Unified exit engine (V3+++ / quant-grade)
========================================

High-level role
---------------
Central exit coordinator that:

  • Tracks fresh positions after BUY fills (in-memory follow-through state).
  • Streams trade ticks into the AI sell brain (utils_pkg.ai_sell_logic).
  • Detects "dead" / failed follow-through coins and triggers exits.
  • Delegates real SELL execution to:
        1) ai_sell_logic.* hooks (schedule_after_buy / schedule_force_exit /
           request_exit / exit_now)
        2) utils_pkg.auto_exit_worker.exit_engine (Redis-backed worker)
        3) LAST-RESORT: direct Jupiter V6 probe sell via utils_pkg.lightning_impl.
  • Mirrors follow-through state into Redis for dashboards / feature_builder.
  • Logs exit signals into utils_pkg.data_logger for AI training + analytics.
  • Notifies self-learning strategy & risk manager modules about exit events
    when present, so they can self-tune risk / parameters over time.

Public API (stable)
-------------------
    async def on_buy_filled(ev: Dict[str, Any]) -> None
        # preferred: dict payload from Strategy/sniper

    async def on_trade_tick(raw_event: Dict[str, Any]) -> None
        # each trade tick from scanner/Strategy

    async def on_buy_filled_event(ev: Dict[str, Any]) -> None
        # legacy alias → forwards to on_buy_filled

    async def on_buy_filled_legacy(raw_id_or_mint: str, tx_sig: str) -> None
        # old positional signature → wraps into dict, forwards

Safety guarantees
-----------------
  • If ai_sell_logic is missing → follow-through + worker/fallback still protect.
  • If Redis / worker are misconfigured → we HOLD by default; only very small
    last-resort probe sells are attempted.
  • DRYRUN short-circuits actual sells but preserves all signals / logging.
"""

import asyncio
import json
import logging
import os
import time
from typing import Any, Dict, Optional, Callable, Set

log = logging.getLogger("sniper.exit_engine")
if not log.handlers:
    logging.basicConfig(level=logging.INFO)

# ──────────────────────────────────────────────────────────────────────────────
# Env helpers
# ──────────────────────────────────────────────────────────────────────────────


def _env_bool(key: str, default: bool = False) -> bool:
    v = os.getenv(key)
    if v is None:
        return default
    return str(v).strip().lower() in ("1", "true", "t", "yes", "y", "on")


def _env_int(key: str, default: int) -> int:
    try:
        return int(os.getenv(key, str(default)))
    except Exception:
        return default


def _env_float(key: str, default: float) -> float:
    try:
        return float(os.getenv(key, str(default)))
    except Exception:
        return default


def _env_csv(key: str) -> Set[str]:
    raw = os.getenv(key, "").strip()
    if not raw:
        return set()
    return {p.strip().lower() for p in raw.split(",") if p.strip()}


# ──────────────────────────────────────────────────────────────────────────────
# Global config from env
# ──────────────────────────────────────────────────────────────────────────────

DRYRUN = (
    _env_bool("DRYRUN", False)
    or _env_bool("DRY_RUN", False)
    or _env_bool("IS_DRYRUN", False)
)

EXIT_ENGINE_FLAG = _env_bool("EXIT_ENGINE", True)
EXIT_ENGINE_ENABLED = _env_bool("EXIT_ENGINE_ENABLED", True)
EXIT_ENGINE_ACTIVE = EXIT_ENGINE_FLAG and EXIT_ENGINE_ENABLED

LAMPORTS_PER_SOL = 1_000_000_000
MIN_SELL_SOL = _env_float("PF_MIN_SELL_SOL", 0.001)
DEFAULT_SLIPPAGE_BPS = _env_int("PF_SLIPPAGE_BPS", 200)

# Follow-through / "dead coin" config
EXIT_FOLLOW_ENABLED = _env_bool("EXIT_FOLLOW_ENABLED", True)
EXIT_FOLLOW_WINDOW_SEC = _env_int("EXIT_FOLLOW_WINDOW_SEC", 35)
EXIT_FOLLOW_MIN_TRADES = _env_int("EXIT_FOLLOW_MIN_TRADES", 5)
EXIT_FOLLOW_MIN_UP_PCT = _env_float("EXIT_FOLLOW_MIN_UP_PCT", 6.0)
EXIT_FOLLOW_MAX_DD_PCT = _env_float("EXIT_FOLLOW_MAX_DD_PCT", 5.0)
EXIT_FOLLOW_DEBUG = _env_bool("EXIT_FOLLOW_DEBUG", False)

# Optional: require at least some unrealized PnL for "follow_ok" classification.
# 0.0 = disabled (only EXIT_FOLLOW_MIN_UP_PCT / MAX_DD_PCT drive classification)
EXIT_FOLLOW_MIN_PNL_PCT = _env_float("EXIT_FOLLOW_MIN_PNL_PCT", 0.0)

# Only apply follow-through logic to these pools if non-empty (e.g. "pump,raydium").
EXIT_FOLLOW_POOLS = _env_csv("EXIT_FOLLOW_POOLS")

# Ignore tiny dust positions for forced follow-through exits.
EXIT_FOLLOW_MIN_SPEND_SOL = _env_float("EXIT_FOLLOW_MIN_SPEND_SOL", 0.0)

# Hard max age for follow-through tracking; after this we stop evaluating.
EXIT_FOLLOW_MAX_AGE_SEC = _env_int("EXIT_FOLLOW_MAX_AGE_SEC", 30 * 60)  # 30 min

# Optional label for AI training on follow-through exits
EXIT_FOLLOW_EXIT_LABEL = os.getenv("EXIT_FOLLOW_EXIT_LABEL", "dead_follow")

# How big the probe should be for the *direct Jup* fallback if we get that far
EXIT_FOLLOW_PROBE_FRACTION = _env_float("EXIT_FOLLOW_PROBE_FRACTION", 0.10)

# Optional feature flag: log exits into data_logger for AI training.
EXIT_LOG_TO_DATA_LOGGER = _env_bool("EXIT_LOG_TO_DATA_LOGGER", True)

# Optional: mirror follow-through state into Redis for dashboards / features
EXIT_FOLLOW_PUBLISH_REDIS = _env_bool("EXIT_FOLLOW_PUBLISH_REDIS", True)

# Optional: whitelist/blacklist for mints that should/shouldn't use follow-through
EXIT_FOLLOW_MINT_WHITELIST = _env_csv("EXIT_FOLLOW_MINT_WHITELIST")
EXIT_FOLLOW_MINT_BLACKLIST = _env_csv("EXIT_FOLLOW_MINT_BLACKLIST")

# Optional: cap how many follow-through exits per day (0 = unlimited)
EXIT_FOLLOW_MAX_DAILY_EXITS = _env_int("EXIT_FOLLOW_MAX_DAILY_EXITS", 0)

# Redis key prefix for daily follow exits
EXIT_FOLLOW_DAILY_KEY_PREFIX = "exit_follow:daily"

# Optional: notify self-learning / risk_manager brains when exit events fire
EXIT_NOTIFY_SELF_LEARNING = _env_bool("EXIT_NOTIFY_SELF_LEARNING", True)
EXIT_NOTIFY_RISK_MANAGER = _env_bool("EXIT_NOTIFY_RISK_MANAGER", True)

# ──────────────────────────────────────────────────────────────────────────────
# Optional: AI-driven / advanced sell engine hook (ai_sell_logic)
# ──────────────────────────────────────────────────────────────────────────────

try:
    # ai_sell_logic contains the "brains" for exits and Redis wiring.
    from utils_pkg import ai_sell_logic as SELL  # type: ignore[attr-defined]
except Exception:  # pragma: no cover
    SELL = None  # type: ignore[assignment]
    log.debug(
        "ai_sell_logic not available; exit_engine will use follow-through + "
        "worker / fallback only"
    )


def _get_sell_hook(name: str) -> Optional[Callable[..., Any]]:
    """
    Resolve a callable from ai_sell_logic, or None if missing.
    """
    if SELL is None:
        return None
    fn = getattr(SELL, name, None)
    if callable(fn):
        return fn
    return None


# ──────────────────────────────────────────────────────────────────────────────
# Optional: data_logger / self-learning / risk manager integrations
# ──────────────────────────────────────────────────────────────────────────────

try:
    from utils_pkg import data_logger as _DL  # type: ignore[attr-defined]
except Exception:  # pragma: no cover
    _DL = None  # type: ignore[assignment]

try:
    from utils_pkg import self_learning_ai_strategy as _SLM  # type: ignore[attr-defined]
except Exception:  # pragma: no cover
    _SLM = None  # type: ignore[assignment]

try:
    from utils_pkg import risk_manager as _RM  # type: ignore[attr-defined]
except Exception:  # pragma: no cover
    _RM = None  # type: ignore[assignment]


async def _call_optional_hook(mod: Any, fn_candidates: tuple[str, ...], event: Dict[str, Any]) -> None:
    """
    Try a list of candidate function names on a module; call the first found.

    Supports sync or async functions, never raises out.
    """
    if mod is None:
        return
    for name in fn_candidates:
        fn = getattr(mod, name, None)
        if not callable(fn):
            continue
        try:
            if asyncio.iscoroutinefunction(fn):
                await fn(event)  # type: ignore[misc]
            else:
                await asyncio.to_thread(fn, event)  # type: ignore[misc]
        except Exception:
            # Swallow – this is strictly best-effort.
            pass
        break


async def _log_exit_event_safe(event: Dict[str, Any]) -> None:
    """
    Best-effort integration with:
      - utils_pkg.data_logger.log_exit_event
      - utils_pkg.self_learning_ai_strategy (on_exit_event / on_forced_exit / on_exit_signal)
      - utils_pkg.risk_manager (on_forced_exit / on_exit_event)

    Supports both sync and async implementations.
    Never raises.
    """
    # 1) Core logging into data_logger
    if EXIT_LOG_TO_DATA_LOGGER and _DL is not None:
        fn = getattr(_DL, "log_exit_event", None)
        if callable(fn):
            try:
                if asyncio.iscoroutinefunction(fn):
                    await fn(event)  # type: ignore[misc]
                else:
                    await asyncio.to_thread(fn, event)  # type: ignore[misc]
            except Exception:
                # Never let logging blow up the engine.
                pass

    # 2) Self-learning brain notification
    if EXIT_NOTIFY_SELF_LEARNING and _SLM is not None:
        await _call_optional_hook(
            _SLM,
            ("on_exit_event", "on_forced_exit", "on_exit_signal"),
            event,
        )

    # 3) Risk manager notification
    if EXIT_NOTIFY_RISK_MANAGER and _RM is not None:
        await _call_optional_hook(
            _RM,
            ("on_forced_exit", "on_exit_event"),
            event,
        )


# ──────────────────────────────────────────────────────────────────────────────
# Optional Redis mirror for follow-through state
# ──────────────────────────────────────────────────────────────────────────────


def _get_redis():
    """
    Lazy import of core_utils.redis (or None if unavailable).
    """
    try:
        from utils_pkg.core_utils import redis as _redis  # type: ignore
        return _redis
    except Exception:  # noqa: BLE001
        return None


def _today_key(prefix: str) -> str:
    import datetime as _dt

    now = _dt.datetime.now(_dt.timezone.utc)
    return f"{prefix}:{now.strftime('%Y%m%d')}"


async def _bump_daily_follow_exit(redis) -> None:
    if not redis or EXIT_FOLLOW_MAX_DAILY_EXITS <= 0:
        return
    try:
        key = _today_key(EXIT_FOLLOW_DAILY_KEY_PREFIX)
        pipe = redis.pipeline()
        pipe.incr(key)
        pipe.expire(key, 60 * 60 * 36)  # ~1.5 days
        await pipe.execute()
    except Exception:
        return


async def _get_daily_follow_exit(redis) -> int:
    if not redis or EXIT_FOLLOW_MAX_DAILY_EXITS <= 0:
        return 0
    try:
        key = _today_key(EXIT_FOLLOW_DAILY_KEY_PREFIX)
        raw = await redis.get(key)
        return int(raw or "0")
    except Exception:
        return 0


async def _publish_follow_state(
    *,
    pos: "_Pos",
    age_sec: float,
    up_pct: float,
    dd_pct: float,
    follow_pass: bool,
) -> None:
    """
    Optionally mirror follow-through state into Redis key:

        follow_state:{mint} -> JSON blob

    This is purely for dashboards / feature_builder and is safe to ignore if
    Redis is unavailable.
    """
    if not EXIT_FOLLOW_PUBLISH_REDIS:
        return

    redis = _get_redis()
    if not redis:
        return

    key = f"follow_state:{pos.mint}"
    payload = {
        "mint": pos.mint,
        "symbol": pos.symbol,
        "pool": pos.pool,
        "age_sec": age_sec,
        "trades_after_entry": pos.trades_after_entry,
        "up_pct": up_pct,
        "dd_pct": dd_pct,
        "spend_sol": pos.spend_sol,
        "follow_pass": bool(follow_pass),
        "ts": time.time(),
        "candidate_id": pos.candidate_id,
        # thresholds for debugging / training
        "window_sec": EXIT_FOLLOW_WINDOW_SEC,
        "min_trades": EXIT_FOLLOW_MIN_TRADES,
        "min_up_pct": EXIT_FOLLOW_MIN_UP_PCT,
        "max_dd_pct": EXIT_FOLLOW_MAX_DD_PCT,
        "min_pnl_pct": EXIT_FOLLOW_MIN_PNL_PCT,
    }

    try:
        await redis.set(key, json.dumps(payload))
    except Exception:
        # Only observability; ignore errors
        return


# ──────────────────────────────────────────────────────────────────────────────
# Fallback direct Jupiter V6 seller (LAST RESORT ONLY)
# ──────────────────────────────────────────────────────────────────────────────


async def _direct_sell_via_jup(
    mint: str,
    amount_raw: int,
    slippage_bps: int = DEFAULT_SLIPPAGE_BPS,
) -> Optional[str]:
    """
    Minimal direct sell path using Lightning / Jupiter V6 plumbing.

    Only used when:
      - AI engine is unavailable or failed, AND
      - auto_exit_worker integration isn't available, AND
      - we have a clear "dead coin" context.
    """
    try:
        from utils_pkg.lightning_impl import sell_via_lightning

        sig = await sell_via_lightning(
            mint=mint,
            token_program=None,
            amount_raw=amount_raw,
            slippage_bps=slippage_bps,
        )
        return sig
    except Exception as e:  # pragma: no cover - defensive
        log.warning(
            "DIRECT_SELL_FAIL",
            extra={"mint": mint, "err": str(e)[:200]},
        )
        return None


# ──────────────────────────────────────────────────────────────────────────────
# In-memory position state for follow-through logic
# ──────────────────────────────────────────────────────────────────────────────


class _Pos:
    __slots__ = (
        "mint",
        "symbol",
        "pool",
        "entry_ts",
        "entry_sig",
        "spend_sol",
        "amount_raw",
        "entry_price",
        "max_price",
        "min_price",
        "trades_after_entry",
        "candidate_id",
        "closed",
    )

    def __init__(
        self,
        mint: str,
        symbol: str,
        pool: str,
        entry_sig: str,
        spend_sol: float,
        amount_raw: Optional[int] = None,
        entry_price: Optional[float] = None,
        candidate_id: Optional[Any] = None,
    ) -> None:
        now = time.time()
        self.mint = mint
        self.symbol = symbol
        self.pool = pool
        self.entry_ts = now
        self.entry_sig = entry_sig
        self.spend_sol = float(spend_sol or 0.0)
        self.amount_raw = amount_raw
        self.entry_price = entry_price
        self.max_price = entry_price
        self.min_price = entry_price
        self.trades_after_entry = 0
        self.candidate_id = candidate_id
        self.closed = False


_OPEN: Dict[str, _Pos] = {}
_OPEN_LOCK = asyncio.Lock()


def _extract_price(ev: Dict[str, Any]) -> Optional[float]:
    """
    Try to pull a float price (in SOL terms or any consistent quote) from a trade tick.
    If nothing looks like a price, return None.
    """
    for key in (
        "priceUi",
        "price_ui",
        "price",
        "priceInSol",
        "price_in_sol",
        "ui_price",
    ):
        v = ev.get(key)
        if v is None:
            continue
        try:
            f = float(v)
            if f > 0:
                return f
        except Exception:
            continue
    return None


def _extract_mint(ev: Dict[str, Any]) -> Optional[str]:
    for key in ("mint", "mintAddress", "mint_address", "id", "tokenMint"):
        v = ev.get(key)
        if isinstance(v, str) and v:
            return v
    return None


def _pool_normalize(pool: Optional[str]) -> str:
    if not pool:
        return "unknown"
    return str(pool).strip().lower()


def _mint_ok_for_follow(mint: str, spend_sol: float, pool: str) -> bool:
    """
    Decide whether follow-through logic should be applied to this position.
    """
    if not EXIT_FOLLOW_ENABLED:
        return False
    if EXIT_FOLLOW_POOLS and pool not in EXIT_FOLLOW_POOLS:
        return False
    if EXIT_FOLLOW_MINT_WHITELIST and mint.lower() not in EXIT_FOLLOW_MINT_WHITELIST:
        return False
    if EXIT_FOLLOW_MINT_BLACKLIST and mint.lower() in EXIT_FOLLOW_MINT_BLACKLIST:
        return False
    if spend_sol < EXIT_FOLLOW_MIN_SPEND_SOL:
        # Tiny / dust positions: don't waste AI or follow-through budget.
        return False
    return True


# ──────────────────────────────────────────────────────────────────────────────
# Core hook: called when a BUY has filled (DICT STYLE, primary API)
# ──────────────────────────────────────────────────────────────────────────────


async def on_buy_filled(ev: Dict[str, Any]) -> None:
    """
    Primary entrypoint from Strategy / sniper core (dict-style).

    ev: {
      mint, signature, symbol?, pool?, spend_sol?, amount_raw?,
      price_entry_ui?, candidate_id?, ...
    }

    Responsibilities:
      - Registers a follow-through position in memory.
      - Delegates to ai_sell_logic.schedule_after_buy(...) if available
        (this writes buy_time, entry price, candidate_id, and snapshots into Redis).
      - In DRYRUN mode, still updates internal state but never sells.
    """
    if not EXIT_ENGINE_ACTIVE:
        log.info(
            "EXIT_ENGINE_DISABLED",
            extra={
                "mint": ev.get("mint"),
                "symbol": ev.get("symbol"),
                "pool": ev.get("pool"),
            },
        )
        return

    mint = _extract_mint(ev) or ""
    tx_sig = (
        ev.get("signature")
        or ev.get("tx_sig")
        or ev.get("id")
        or ev.get("tx")
        or ""
    )
    if not mint or not tx_sig:
        log.warning(
            "EXIT_HOOK_BAD_EVENT",
            extra={"ev_keys": list(ev.keys())[:12]},
        )
        return

    symbol = (ev.get("symbol") or mint[:4]).strip()
    pool = _pool_normalize(ev.get("pool") or ev.get("source"))

    # Spend size (in SOL) if provided
    spend_sol = 0.0
    for key in ("spend_sol", "buy_size_sol", "amount_sol", "entry_size_sol"):
        if key in ev and ev[key] is not None:
            try:
                spend_sol = float(ev[key])
                break
            except Exception:
                continue

    # Token amount in base units (lamports, etc.)
    amount_raw: Optional[int] = None
    for key in ("amount_raw", "amount_out", "base_amount_raw"):
        v = ev.get(key)
        if v is None:
            continue
        try:
            amount_raw = int(v)
            break
        except Exception:
            continue

    # Entry price (if scanner provided one)
    price = ev.get("price_entry_ui")
    try:
        price = float(price) if price is not None else None
    except Exception:
        price = None

    candidate_id = ev.get("candidate_id") or ev.get("entry_candidate_id")

    log.info(
        "EXIT_HOOK_START",
        extra={
            "mint": mint,
            "symbol": symbol,
            "pool": pool,
            "spend_sol": spend_sol,
            "amount_raw": amount_raw,
            "tx_sig": tx_sig,
            "dryrun": DRYRUN,
            "candidate_id": candidate_id,
        },
    )

    # Register internal position for follow-through logic
    if _mint_ok_for_follow(mint, spend_sol, pool):
        async with _OPEN_LOCK:
            _OPEN[mint] = _Pos(
                mint=mint,
                symbol=symbol,
                pool=pool,
                entry_sig=tx_sig,
                spend_sol=spend_sol,
                amount_raw=amount_raw,
                entry_price=price,
                candidate_id=candidate_id,
            )
        if EXIT_FOLLOW_DEBUG:
            log.info(
                "EXIT_FOLLOW_OPEN",
                extra={
                    "mint": mint,
                    "symbol": symbol,
                    "pool": pool,
                    "spend_sol": spend_sol,
                    "candidate_id": candidate_id,
                },
            )

    # Delegate to advanced AI sell engine if available
    schedule_fn = _get_sell_hook("schedule_after_buy")
    if schedule_fn is not None:
        try:
            payload = dict(ev)
            payload.update(
                {
                    "mint": mint,
                    "signature": tx_sig,
                    "ts": time.time(),
                    "pool": pool,
                    "symbol": symbol,
                    "spend_sol": spend_sol,
                    "amount_raw": amount_raw,
                    "candidate_id": candidate_id,
                }
            )
            await schedule_fn(payload)
            log.info(
                "EXIT_SCHED_OK",
                extra={
                    "mint": mint,
                    "symbol": symbol,
                    "engine": "ai_sell_logic.schedule_after_buy",
                },
            )
            return
        except Exception as e:  # pragma: no cover - defensive log
            log.warning(
                "EXIT_SCHED_FAIL_AI",
                extra={
                    "mint": mint,
                    "symbol": symbol,
                    "err": str(e)[:200],
                },
            )

    # If we reach here, AI engine isn't available or failed.
    # We do NOT exit immediately; follow-through logic + DRYRUN protects us.
    if DRYRUN:
        log.info(
            "EXIT_FALLBACK_DRYRUN",
            extra={"mint": mint, "symbol": symbol, "pool": pool},
        )
        return

    log.info(
        "EXIT_NO_AI_ENGINE",
        extra={"mint": mint, "symbol": symbol, "pool": pool},
    )


# ──────────────────────────────────────────────────────────────────────────────
# Trade tick hook: feeds both AI engine and follow-through logic
# ──────────────────────────────────────────────────────────────────────────────


async def on_trade_tick(raw_event: Dict[str, Any]) -> None:
    """
    Called for every trade tick the Strategy sees.

    Responsibilities:
      - Forward tick to AI sell engine (ai_sell_logic.on_trade_tick) if present;
      - Maintain follow-through counters (trades_after_entry, max_up, drawdown);
      - After EXIT_FOLLOW_WINDOW_SEC, if follow-through failed:
          * ask AI engine to schedule a force exit (schedule_force_exit / request_exit);
          * or, if AI is missing, schedule an exit via auto_exit_worker.exit_engine;
          * or, as a last resort, do a tiny direct Jupiter probe sell.
    """
    if not EXIT_ENGINE_ACTIVE:
        return

    mint = _extract_mint(raw_event)
    if not mint:
        return

    now = time.time()
    price = _extract_price(raw_event)

    # ---- Forward to AI engine first (best-effort) -------------------------
    ai_tick = _get_sell_hook("on_trade_tick")
    if ai_tick is not None:
        try:
            await ai_tick(raw_event)
        except Exception as e:  # pragma: no cover
            log.warning(
                "EXIT_AI_TICK_FAIL",
                extra={"mint": mint, "err": str(e)[:200]},
            )

    if not EXIT_FOLLOW_ENABLED:
        return

    # ---- Follow-through state lookup -------------------------------------
    async with _OPEN_LOCK:
        pos = _OPEN.get(mint)

    if pos is None or pos.closed:
        return

    # Age guard (max tracking window)
    age = now - pos.entry_ts
    if age > EXIT_FOLLOW_MAX_AGE_SEC:
        # Stop tracking; let DCA / AI deal with very old positions.
        async with _OPEN_LOCK:
            pos.closed = True
            _OPEN.pop(mint, None)
        if EXIT_FOLLOW_DEBUG:
            log.info(
                "EXIT_FOLLOW_MAX_AGE_DROP",
                extra={
                    "mint": pos.mint,
                    "symbol": pos.symbol,
                    "age": age,
                    "max_age": EXIT_FOLLOW_MAX_AGE_SEC,
                },
            )
        return

    # Update tick counters
    pos.trades_after_entry += 1

    if price is not None:
        if pos.entry_price is None:
            pos.entry_price = price
            pos.max_price = price
            pos.min_price = price
        else:
            if pos.max_price is None or price > pos.max_price:
                pos.max_price = price
            if pos.min_price is None or price < pos.min_price:
                pos.min_price = price

    # Evaluate follow-through only after the configured window
    if age < EXIT_FOLLOW_WINDOW_SEC:
        # Still in the early phase: only keep tracking / publishing.
        if EXIT_FOLLOW_DEBUG:
            log.info(
                "EXIT_FOLLOW_WARMUP",
                extra={
                    "mint": pos.mint,
                    "symbol": pos.symbol,
                    "age": age,
                    "window": EXIT_FOLLOW_WINDOW_SEC,
                    "trades_after_entry": pos.trades_after_entry,
                },
            )
        return

    # Only decide once
    if pos.closed:
        return

    trades_ok = pos.trades_after_entry >= EXIT_FOLLOW_MIN_TRADES

    up_ok = True
    dd_ok = True
    pnl_ok = True
    up_pct = 0.0
    dd_pct = 0.0
    pnl_pct = 0.0

    if pos.entry_price and pos.max_price:
        up_pct = (pos.max_price / pos.entry_price - 1.0) * 100.0
        up_ok = up_pct >= EXIT_FOLLOW_MIN_UP_PCT

    if pos.entry_price and pos.min_price:
        dd_pct = (pos.min_price / pos.entry_price - 1.0) * 100.0
        # dd_pct is negative when the price is below entry
        dd_ok = dd_pct >= -EXIT_FOLLOW_MAX_DD_PCT

    # Approximate "current" pnl as max_price vs entry, or last seen price if no max
    if pos.entry_price:
        ref_price = pos.max_price or pos.entry_price
        pnl_pct = (ref_price / pos.entry_price - 1.0) * 100.0
        if EXIT_FOLLOW_MIN_PNL_PCT > 0.0:
            pnl_ok = pnl_pct >= EXIT_FOLLOW_MIN_PNL_PCT

    # Full follow-through pass
    follow_pass = trades_ok and up_ok and dd_ok and pnl_ok

    # Optional: mirror state into Redis for dashboards / features
    await _publish_follow_state(
        pos=pos,
        age_sec=age,
        up_pct=up_pct,
        dd_pct=dd_pct,
        follow_pass=follow_pass,
    )

    if EXIT_FOLLOW_DEBUG:
        log.info(
            "EXIT_FOLLOW_EVAL",
            extra={
                "mint": pos.mint,
                "symbol": pos.symbol,
                "age": age,
                "trades_after_entry": pos.trades_after_entry,
                "up_pct": up_pct,
                "dd_pct": dd_pct,
                "pnl_pct": pnl_pct,
                "trades_ok": int(trades_ok),
                "up_ok": int(up_ok),
                "dd_ok": int(dd_ok),
                "pnl_ok": int(pnl_ok),
                "follow_pass": int(follow_pass),
            },
        )

    if follow_pass:
        # Good follow-through → stop evaluating; AI / DCA can manage the rest.
        async with _OPEN_LOCK:
            pos.closed = True
            _OPEN.pop(mint, None)
        await _log_exit_event_safe(
            {
                "mint": pos.mint,
                "symbol": pos.symbol,
                "pool": pos.pool,
                "label": "follow_ok",
                "age_sec": age,
                "trades_after_entry": pos.trades_after_entry,
                "up_pct": up_pct,
                "dd_pct": dd_pct,
                "pnl_pct": pnl_pct,
                "spend_sol": pos.spend_sol,
                "candidate_id": pos.candidate_id,
                "source": "exit_engine.follow_through",
            }
        )
        return

    # At this point we consider the coin "dead" / failed follow-through.
    log.info(
        "EXIT_FOLLOW_TRIGGER",
        extra={
            "mint": pos.mint,
            "symbol": pos.symbol,
            "age": age,
            "trades_after_entry": pos.trades_after_entry,
            "up_pct": up_pct,
            "dd_pct": dd_pct,
            "pnl_pct": pnl_pct,
        },
    )

    async with _OPEN_LOCK:
        pos.closed = True
        _OPEN.pop(mint, None)

    # Daily cap safeguard for how many forced exits we are allowed to trigger
    redis = _get_redis()
    if redis and EXIT_FOLLOW_MAX_DAILY_EXITS > 0:
        daily = await _get_daily_follow_exit(redis)
        if daily >= EXIT_FOLLOW_MAX_DAILY_EXITS:
            log.info(
                "EXIT_FOLLOW_DAILY_CAP_REACHED",
                extra={
                    "mint": pos.mint,
                    "symbol": pos.symbol,
                    "daily": daily,
                    "cap": EXIT_FOLLOW_MAX_DAILY_EXITS,
                },
            )
            await _log_exit_event_safe(
                {
                    "mint": pos.mint,
                    "symbol": pos.symbol,
                    "pool": pos.pool,
                    "label": "follow_fail_daily_cap",
                    "age_sec": age,
                    "trades_after_entry": pos.trades_after_entry,
                    "up_pct": up_pct,
                    "dd_pct": dd_pct,
                    "pnl_pct": pnl_pct,
                    "spend_sol": pos.spend_sol,
                    "candidate_id": pos.candidate_id,
                    "source": "exit_engine.follow_through",
                }
            )
            return

    if DRYRUN:
        log.info(
            "EXIT_FOLLOW_DRYRUN",
            extra={"mint": pos.mint, "symbol": pos.symbol},
        )
        await _log_exit_event_safe(
            {
                "mint": pos.mint,
                "symbol": pos.symbol,
                "pool": pos.pool,
                "label": "follow_fail_dryrun",
                "age_sec": age,
                "trades_after_entry": pos.trades_after_entry,
                "up_pct": up_pct,
                "dd_pct": dd_pct,
                "pnl_pct": pnl_pct,
                "spend_sol": pos.spend_sol,
                "candidate_id": pos.candidate_id,
                "source": "exit_engine.follow_through",
            }
        )
        return

    # Preferred path #1: ask AI engine to schedule a forced exit.
    exit_ev: Dict[str, Any] = {
        "mint": pos.mint,
        "symbol": pos.symbol,
        "pool": pos.pool,
        "reason": "follow_through_fail",
        "age": age,
        "trades_after_entry": pos.trades_after_entry,
        "up_pct": up_pct,
        "dd_pct": dd_pct,
        "pnl_pct": pnl_pct,
        "exit_label": EXIT_FOLLOW_EXIT_LABEL,
        "candidate_id": pos.candidate_id,
        "source": "exit_engine.follow_through",
        "spend_sol": pos.spend_sol,
    }

    # Log for AI training / analytics / self-learning / risk manager.
    await _log_exit_event_safe(
        {
            **exit_ev,
            "label": "follow_fail",
        }
    )

    for hook_name in ("schedule_force_exit", "force_exit", "request_exit", "exit_now"):
        hook = _get_sell_hook(hook_name)
        if hook is None:
            continue
        try:
            await hook(exit_ev)
            log.info(
                "EXIT_FOLLOW_DELEGATED_AI",
                extra={
                    "mint": pos.mint,
                    "symbol": pos.symbol,
                    "engine_hook": hook_name,
                },
            )
            if redis:
                await _bump_daily_follow_exit(redis)
            return
        except Exception as e:  # pragma: no cover
            log.warning(
                "EXIT_FOLLOW_ENGINE_FAIL",
                extra={
                    "mint": pos.mint,
                    "symbol": pos.symbol,
                    "hook": hook_name,
                    "err": str(e)[:200],
                },
            )

    # Preferred path #2: schedule via auto_exit_worker exit_engine (Redis exit_request)
    try:
        from utils_pkg.auto_exit_worker import exit_engine as WORKER_ENGINE  # type: ignore

        if pos.amount_raw is not None and pos.amount_raw > 0:
            await WORKER_ENGINE.enqueue_exit_request(
                mint=pos.mint,
                amount_raw=int(pos.amount_raw),
                reason="follow_through_fail",
                sell_pct=1.0,
                entry_spend_sol=pos.spend_sol,
                follow_stats={
                    "up_pct": up_pct,
                    "dd_pct": dd_pct,
                    "pnl_pct": pnl_pct,
                    "age_sec": age,
                    "trades_after_entry": pos.trades_after_entry,
                    "exit_label": EXIT_FOLLOW_EXIT_LABEL,
                    "candidate_id": pos.candidate_id,
                },
            )
            log.info(
                "EXIT_FOLLOW_DELEGATED_WORKER",
                extra={
                    "mint": pos.mint,
                    "symbol": pos.symbol,
                    "amount_raw": int(pos.amount_raw),
                },
            )
            if redis:
                await _bump_daily_follow_exit(redis)
            return
    except Exception as e:  # pragma: no cover
        log.warning(
            "EXIT_FOLLOW_WORKER_FAIL",
            extra={"mint": pos.mint, "symbol": pos.symbol, "err": str(e)[:200]},
        )

    # Final fallback: tiny direct sell via Jupiter (probe only).
    probe_fraction = max(0.01, min(1.0, EXIT_FOLLOW_PROBE_FRACTION))
    probe_sol = (
        max(MIN_SELL_SOL, pos.spend_sol * probe_fraction)
        if pos.spend_sol > 0
        else MIN_SELL_SOL
    )
    probe_raw = int(probe_sol * LAMPORTS_PER_SOL)
    sig = await _direct_sell_via_jup(mint=pos.mint, amount_raw=probe_raw)
    log.info(
        "EXIT_FOLLOW_FALLBACK_SELL",
        extra={
            "mint": pos.mint,
            "symbol": pos.symbol,
            "probe_sol": probe_sol,
            "sig": sig,
        },
    )
    if redis:
        await _bump_daily_follow_exit(redis)


# ──────────────────────────────────────────────────────────────────────────────
# Legacy wrappers (kept for backwards compatibility)
# ──────────────────────────────────────────────────────────────────────────────


async def on_buy_filled_event(ev: Dict[str, Any]) -> None:
    """
    Backwards-compatible name for call sites that already pass a single event dict.
    Just forwards to on_buy_filled(ev).
    """
    await on_buy_filled(ev)


async def on_buy_filled_legacy(raw_id_or_mint: str, tx_sig: str) -> None:
    """
    Old positional signature kept for compatibility with previous versions.

    Some old call sites may still call:
        await exit_engine.on_buy_filled_legacy(mint, tx_sig)

    We synthesize a minimal event and feed it into the main handler.
    """
    ev: Dict[str, Any] = {
        "mint": raw_id_or_mint,
        "signature": tx_sig,
        "symbol": "",
        "spend_sol": MIN_SELL_SOL,
        "pool": "legacy",
        "ts": time.time(),
    }
    await on_buy_filled(ev)


# ──────────────────────────────────────────────────────────────────────────────
# Diagnostics helpers (for backtests / scripts/diagnostics.py)
# ──────────────────────────────────────────────────────────────────────────────


def get_open_positions_snapshot() -> Dict[str, Dict[str, Any]]:
    """
    Return a shallow snapshot of the current follow-through tracking state.

    Intended for diagnostics / HTTP health endpoints / debugging.
    """
    snap: Dict[str, Dict[str, Any]] = {}
    now = time.time()
    for mint, pos in list(_OPEN.items()):
        if pos.closed:
            continue
        age = now - pos.entry_ts
        snap[mint] = {
            "mint": pos.mint,
            "symbol": pos.symbol,
            "pool": pos.pool,
            "age_sec": age,
            "spend_sol": pos.spend_sol,
            "amount_raw": pos.amount_raw,
            "trades_after_entry": pos.trades_after_entry,
            "entry_price": pos.entry_price,
            "max_price": pos.max_price,
            "min_price": pos.min_price,
            "candidate_id": pos.candidate_id,
        }
    return snap


__all__ = [
    "on_buy_filled",
    "on_trade_tick",
    "on_buy_filled_event",
    "on_buy_filled_legacy",
    "get_open_positions_snapshot",
]
