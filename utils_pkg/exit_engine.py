# utils_pkg/exit_engine.py
# Unified exit engine:
#   - Tracks new positions after BUY fills (in-memory follow-through state)
#   - Feeds AI sell logic (ai_sell_logic.schedule_after_buy / on_trade_tick / schedule_force_exit)
#   - Optionally triggers "dead coin" exits when there is no follow-through
#   - Provides a last-resort direct Jupiter sell fallback if AI engine is unavailable
#
# Public API (unchanged):
#   async def on_buy_filled(ev: Dict[str, Any]) -> None
#   async def on_trade_tick(raw_event: Dict[str, Any]) -> None
#   async def on_buy_filled_event(ev: Dict[str, Any]) -> None
#   async def on_buy_filled_legacy(raw_id_or_mint: str, tx_sig: str) -> None
#
# This module is intentionally self-contained and safe:
#   - If ai_sell_logic is missing, it still runs (but exits are simpler).
#   - If Redis or auto_exit_worker is misconfigured, we default to HOLD / no-op,
#     except for the last-resort direct Jup fallback when safe to do so.

from __future__ import annotations

import asyncio
import logging
import os
import time
from typing import Any, Dict, Optional, Callable

log = logging.getLogger("sniper.exit_engine")

# ============================================================================
# Env helpers
# ============================================================================


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


# ============================================================================
# Global config from env
# ============================================================================

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

# ============================================================================
# Optional: AI-driven / advanced sell engine hook (ai_sell_logic)
# ============================================================================

try:
    # ai_sell_logic contains the "brains" for exits and Redis wiring.
    from utils_pkg import ai_sell_logic as SELL  # type: ignore[attr-defined]
except Exception:  # pragma: no cover
    SELL = None  # type: ignore[assignment]
    log.debug("ai_sell_logic not available; exit_engine will use simple follow-through + fallback only")


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


# ============================================================================
# Fallback direct Jupiter V6 seller (LAST RESORT ONLY)
# ============================================================================


async def _direct_sell_via_jup(
    mint: str,
    amount_raw: int,
    slippage_bps: int = DEFAULT_SLIPPAGE_BPS,
) -> Optional[str]:
    """
    Minimal direct sell path using Lightning / Jupiter V6 plumbing.
    Only used when:
      - AI engine is unavailable, AND
      - we have a clear "dead coin" or test condition.
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


# ============================================================================
# In-memory position state for follow-through logic
# ============================================================================


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


# ============================================================================
# Core hook: called when a BUY has filled (DICT STYLE, primary API)
# ============================================================================


async def on_buy_filled(ev: Dict[str, Any]) -> None:
    """
    Primary entrypoint from Strategy / sniper core (dict-style).

    ev: {
      mint, signature, symbol?, pool?, spend_sol?, amount_raw?,
      price_entry_ui?, ...
    }

    Responsibilities:
      - Registers a follow-through position in memory.
      - Delegates to ai_sell_logic.schedule_after_buy(...) if available
        (this writes buy_time, entry price, and snapshots into Redis).
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
    pool = (ev.get("pool") or "unknown").lower()

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
        },
    )

    # Register internal position for follow-through logic
    if EXIT_FOLLOW_ENABLED:
        async with _OPEN_LOCK:
            _OPEN[mint] = _Pos(
                mint=mint,
                symbol=symbol,
                pool=pool,
                entry_sig=tx_sig,
                spend_sol=spend_sol,
                amount_raw=amount_raw,
                entry_price=price,
            )
        if EXIT_FOLLOW_DEBUG:
            log.info(
                "EXIT_FOLLOW_OPEN",
                extra={
                    "mint": mint,
                    "symbol": symbol,
                    "pool": pool,
                    "spend_sol": spend_sol,
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

    # No AI sell engine: we simply keep the follow-through state alive and
    # rely on on_trade_tick() to eventually decide if a direct sell is needed.
    log.info(
        "EXIT_NO_AI_ENGINE",
        extra={"mint": mint, "symbol": symbol, "pool": pool},
    )


# ============================================================================
# Trade tick hook: feeds both AI engine and follow-through logic
# ============================================================================


async def on_trade_tick(raw_event: Dict[str, Any]) -> None:
    """
    Called for every trade tick the Strategy sees.

    Responsibilities:
      - Forward tick to AI sell engine (ai_sell_logic.on_trade_tick) if present;
      - Maintain follow-through counters (trades_after_entry, max_up, drawdown);
      - After EXIT_FOLLOW_WINDOW_SEC, if follow-through failed:
          * ask AI engine to schedule a force exit (schedule_force_exit / request_exit);
          * or, if AI is missing, optionally do a tiny direct Jupiter sell as fallback.
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
    age = now - pos.entry_ts
    if age < EXIT_FOLLOW_WINDOW_SEC:
        return

    # Only decide once
    if pos.closed:
        return

    trades_ok = pos.trades_after_entry >= EXIT_FOLLOW_MIN_TRADES

    up_ok = True
    dd_ok = True
    up_pct = 0.0
    dd_pct = 0.0

    if pos.entry_price and pos.max_price:
        up_pct = (pos.max_price / pos.entry_price - 1.0) * 100.0
        up_ok = up_pct >= EXIT_FOLLOW_MIN_UP_PCT

    if pos.entry_price and pos.min_price:
        dd_pct = (pos.min_price / pos.entry_price - 1.0) * 100.0
        # dd_pct is negative when the price is below entry
        dd_ok = dd_pct >= -EXIT_FOLLOW_MAX_DD_PCT

    follow_pass = trades_ok and up_ok and dd_ok

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
                "trades_ok": int(trades_ok),
                "up_ok": int(up_ok),
                "dd_ok": int(dd_ok),
                "follow_pass": int(follow_pass),
            },
        )

    if follow_pass:
        # Good follow-through → stop evaluating; AI / DCA can manage the rest.
        async with _OPEN_LOCK:
            pos.closed = True
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
        },
    )

    async with _OPEN_LOCK:
        pos.closed = True
        _OPEN.pop(mint, None)

    if DRYRUN:
        log.info(
            "EXIT_FOLLOW_DRYRUN",
            extra={"mint": pos.mint, "symbol": pos.symbol},
        )
        return

    # Preferred path: ask AI engine to schedule a forced exit.
    for hook_name in ("schedule_force_exit", "force_exit", "request_exit", "exit_now"):
        hook = _get_sell_hook(hook_name)
        if hook is None:
            continue
        try:
            await hook(
                {
                    "mint": pos.mint,
                    "symbol": pos.symbol,
                    "pool": pos.pool,
                    "reason": "follow_through_fail",
                    "age": age,
                    "trades_after_entry": pos.trades_after_entry,
                    "up_pct": up_pct,
                    "dd_pct": dd_pct,
                }
            )
            log.info(
                "EXIT_FOLLOW_DELEGATED",
                extra={
                    "mint": pos.mint,
                    "symbol": pos.symbol,
                    "engine_hook": hook_name,
                },
            )
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

    # Final fallback: tiny direct sell via Jupiter.
    probe_sol = max(MIN_SELL_SOL, pos.spend_sol * 0.10) if pos.spend_sol > 0 else MIN_SELL_SOL
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


# ============================================================================
# Legacy wrappers (kept for backwards compatibility)
# ============================================================================


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
