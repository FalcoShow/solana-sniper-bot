# sniper/auto_sell.py
from __future__ import annotations

"""
sniper/auto_sell.py

God-Tier Auto Sell Adapter — V3+++ LOCK-IN (production-first)
=============================================================

DEDUP / CONTRACT (LOCKED)
------------------------
- utils_pkg/exit_engine.py is the *canonical exit brain* (decision logic).
- utils_pkg/auto_exit_worker/exit_engine.py is the *canonical executor* (SELL queue + crash-safe execution).
- This module is ONLY a thin adapter:
    positions -> (price snapshot) -> exit_engine decision -> enqueue_exit_request

ABSOLUTELY NO competing exit logic lives here:
- no trailing stop math
- no AI should_hold_token
- no direct sell_via_lightning here
- no partial TP ladders here
All of that belongs in utils_pkg/exit_engine.py.

Runtime mode
------------
This adapter runs as an asyncio loop (optional service) that:
- loads positions
- gets wallet token balances (authoritative sellable amount)
- gets current price snapshot (best-effort)
- calls utils_pkg.exit_engine (best-effort)
- enqueues sell requests for the worker

If exit_engine is missing / errors, we HOLD (safe default) and log.

Environment (compat + new)
--------------------------
- AUTO_SELL_ENABLED=1|0                      (default: 1)
- AUTO_SELL_INTERVAL_S                      (default: 20)
- AUTO_SELL_MAX_PARALLEL                    (default: 8)
- AUTO_SELL_QUEUE_COOLDOWN_S                (default: 12)   per-mint queue spam guard
- AUTO_SELL_PRICE_TIMEOUT_S                 (default: 6)
- DRYRUN / DRY_RUN / PAPER_TRADING          respected by worker; adapter still queues
- POSITION_MONITOR_INTERVAL                 (legacy alias for AUTO_SELL_INTERVAL_S)

Notes
-----
- We intentionally compute amount_raw from *wallet balances* (authoritative),
  not from stale position dicts.
- We normalize Pump IDs -> real mint for pricing/decimals, but we preserve the
  original mint key for position storage.

"""

import asyncio
import contextlib
import inspect
import os
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

# -----------------------------------------------------------------------------
# Logging (logging_setup compatible; safe fallback)
# -----------------------------------------------------------------------------
try:
    from logging_setup import get_logger  # type: ignore

    log = get_logger("sniper.auto_sell")  # type: ignore
except Exception:  # pragma: no cover
    import logging

    log = logging.getLogger("sniper.auto_sell")
    if not log.handlers:
        logging.basicConfig(level=logging.INFO)


def _log(name: str, level: str = "info", **fields: Any) -> None:
    with contextlib.suppress(Exception):
        if hasattr(log, "event"):
            log.event(name, **fields)  # type: ignore[attr-defined]
            return
    msg = f"{name} {fields}"
    with contextlib.suppress(Exception):
        if level == "warning":
            log.warning(msg)
        elif level == "error":
            log.error(msg)
        else:
            log.info(msg)


def _short_exc(e: BaseException, n: int = 320) -> str:
    s = f"{type(e).__name__}: {e}"
    return (s[:n] + "…") if len(s) > n else s


# -----------------------------------------------------------------------------
# Env helpers
# -----------------------------------------------------------------------------
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


def _now_utc() -> datetime:
    return datetime.now(UTC)


# -----------------------------------------------------------------------------
# Config
# -----------------------------------------------------------------------------
AUTO_SELL_ENABLED = _env_bool("AUTO_SELL_ENABLED", True)

# Legacy alias: POSITION_MONITOR_INTERVAL
AUTO_SELL_INTERVAL_S = max(
    2, _env_int("AUTO_SELL_INTERVAL_S", _env_int("POSITION_MONITOR_INTERVAL", 20))
)
AUTO_SELL_MAX_PARALLEL = max(1, _env_int("AUTO_SELL_MAX_PARALLEL", 8))
AUTO_SELL_QUEUE_COOLDOWN_S = max(1, _env_int("AUTO_SELL_QUEUE_COOLDOWN_S", 12))
AUTO_SELL_PRICE_TIMEOUT_S = max(1, _env_int("AUTO_SELL_PRICE_TIMEOUT_S", 6))

WALLET_PUBKEY = (os.getenv("WALLET_PUBKEY") or os.getenv("WALLET_PUBLIC_KEY") or "").strip()


# -----------------------------------------------------------------------------
# Core utils + positions (best-effort imports)
# -----------------------------------------------------------------------------
try:
    from utils_pkg import core_utils as cu  # type: ignore
except Exception:  # pragma: no cover
    cu = None  # type: ignore

# positions: prefer services.*, fallback to project root
try:  # pragma: no cover
    from services.positions import load_positions, remove_position  # type: ignore
except Exception:  # pragma: no cover
    from positions import load_positions, remove_position  # type: ignore

# Router: used ONLY for normalization/decimals/price snapshot (no sells here).
try:
    from utils_pkg.router import (  # type: ignore
        get_decimals,
        get_price_ui,
        normalize_pump_id_to_mint,
    )
except Exception:  # pragma: no cover
    normalize_pump_id_to_mint = None  # type: ignore
    get_decimals = None  # type: ignore
    get_price_ui = None  # type: ignore

# Exit brain (canonical decisions)
try:
    from utils_pkg import exit_engine as exit_brain  # type: ignore
except Exception:  # pragma: no cover
    exit_brain = None  # type: ignore

# Exit executor queue (canonical execution worker)
try:
    from utils_pkg.auto_exit_worker import enqueue_exit_request  # type: ignore
except Exception:  # pragma: no cover
    enqueue_exit_request = None  # type: ignore

# optional: circuit breakers (we still allow exits; these are for telemetry only)
try:
    from utils_pkg.circuit_breakers import trading_halted  # type: ignore
except Exception:  # pragma: no cover

    async def trading_halted(*_a: Any, **_k: Any) -> bool:  # type: ignore
        return False


# -----------------------------------------------------------------------------
# Async helpers
# -----------------------------------------------------------------------------
async def _maybe_await(x: Any) -> Any:
    if asyncio.iscoroutine(x):
        return await x
    return x


def _filter_kwargs(fn: Any, kwargs: dict[str, Any]) -> dict[str, Any]:
    try:
        sig = inspect.signature(fn)
        allowed = set(sig.parameters.keys())
        return {k: v for k, v in kwargs.items() if k in allowed}
    except Exception:
        return kwargs


async def _init_resources_best_effort() -> None:
    if cu is None:
        return
    fn = getattr(cu, "init_resources", None)
    if callable(fn):
        with contextlib.suppress(Exception):
            out = fn()
            if asyncio.iscoroutine(out):
                await out


async def _close_resources_best_effort() -> None:
    if cu is None:
        return
    fn = getattr(cu, "close_resources", None)
    if callable(fn):
        with contextlib.suppress(Exception):
            out = fn()
            if asyncio.iscoroutine(out):
                await out


async def _get_redis() -> Any:
    if cu is None:
        return None
    for nm in ("get_redis_async", "get_redis"):
        fn = getattr(cu, nm, None)
        if callable(fn):
            with contextlib.suppress(Exception):
                out = fn()
                if asyncio.iscoroutine(out):
                    return await out
                return out
    return getattr(cu, "redis", None)


# -----------------------------------------------------------------------------
# Wallet / positions snapshots
# -----------------------------------------------------------------------------
async def _wallet_token_balances() -> dict[str, float]:
    """
    Returns mint -> ui balance (float). Best-effort.
    """
    if cu is None:
        return {}
    fn = getattr(cu, "get_wallet_tokens", None)
    if not callable(fn) or not WALLET_PUBKEY:
        return {}
    try:
        toks = await _maybe_await(fn(WALLET_PUBKEY))
        out: dict[str, float] = {}
        for t in toks or []:
            try:
                mint = str(t.get("mint") or "").strip()
                bal = float(t.get("balance") or 0.0)
                if mint:
                    out[mint] = bal
            except Exception:
                continue
        return out
    except Exception as e:
        _log("AUTOSELL_WALLET_TOKENS_ERR", level="warning", err=_short_exc(e))
        return {}


async def _price_ui(norm_mint: str) -> float:
    """
    Best-effort router price snapshot.
    """
    if not callable(get_price_ui):
        return 0.0
    try:
        # timebox price retrieval (avoid stalling loop)
        return float(
            await asyncio.wait_for(
                _maybe_await(get_price_ui(norm_mint)), timeout=float(AUTO_SELL_PRICE_TIMEOUT_S)
            )
            or 0.0
        )
    except Exception:
        return 0.0


async def _decimals(norm_mint: str) -> int:
    if not callable(get_decimals):
        return 9
    try:
        d = await _maybe_await(get_decimals(norm_mint))
        d = int(d) if d is not None else 9
        return max(0, min(18, d))
    except Exception:
        return 9


def _normalize_mint(mint: str) -> str:
    if callable(normalize_pump_id_to_mint):
        with contextlib.suppress(Exception):
            nm = normalize_pump_id_to_mint(mint)  # type: ignore[misc]
            nm = str(nm or "").strip()
            if nm:
                return nm
    return str(mint or "").strip()


def _position_symbol(pos: dict[str, Any], mint: str) -> str:
    sym = str(pos.get("symbol") or "").strip()
    return sym or mint[:6]


# -----------------------------------------------------------------------------
# Exit decision plumbing (thin adapter)
# -----------------------------------------------------------------------------
@dataclass
class ExitAction:
    mint: str
    amount_raw: int
    reason: str
    sell_pct: float = 1.0
    symbol: str = ""
    priority: int = 5
    meta: dict[str, Any] = None  # type: ignore[assignment]


def _as_float(x: Any, default: float = 0.0) -> float:
    try:
        return float(x)
    except Exception:
        return default


def _as_int(x: Any, default: int = 0) -> int:
    try:
        return int(x)
    except Exception:
        return default


def _parse_exit_actions(
    decision: Any,
    *,
    mint: str,
    symbol: str,
    amount_raw: int,
) -> list[ExitAction]:
    """
    Accepts multiple possible decision shapes from utils_pkg.exit_engine.
    Returns 0..N exit actions.
    """
    if decision is None:
        return []

    # Common pattern: (should_exit, reason, sell_pct, meta?)
    if isinstance(decision, tuple) and len(decision) >= 2:
        try:
            should_exit = bool(decision[0])
            if not should_exit:
                return []
            reason = str(decision[1] or "exit")
            sell_pct = _as_float(decision[2], 1.0) if len(decision) >= 3 else 1.0
            meta = dict(decision[3] or {}) if len(decision) >= 4 else {}
            return [
                ExitAction(
                    mint=mint,
                    amount_raw=int(amount_raw),
                    reason=reason,
                    sell_pct=max(0.0, min(1.0, float(sell_pct))),
                    symbol=symbol,
                    priority=_as_int(meta.get("priority"), 5),
                    meta=meta,
                )
            ]
        except Exception:
            return []

    # Dict decision
    if isinstance(decision, dict):
        # either: {"action":"sell", ...} or {"sell": {...}} or {"actions":[...]}
        if isinstance(decision.get("actions"), list):
            out: list[ExitAction] = []
            for a in decision["actions"]:
                if not isinstance(a, dict):
                    continue
                if str(a.get("type") or a.get("action") or "").lower() not in {"sell", "exit"}:
                    continue
                amt = _as_int(a.get("amount_raw"), 0) or int(amount_raw)
                if amt <= 0:
                    continue
                out.append(
                    ExitAction(
                        mint=mint,
                        amount_raw=amt,
                        reason=str(a.get("reason") or "exit"),
                        sell_pct=max(0.0, min(1.0, _as_float(a.get("sell_pct"), 1.0))),
                        symbol=str(a.get("symbol") or symbol),
                        priority=_as_int(a.get("priority"), 5),
                        meta=dict(a.get("meta") or {}),
                    )
                )
            return out

        act = str(decision.get("action") or decision.get("type") or "").lower()
        if act in {"sell", "exit"} or decision.get("should_exit") is True:
            amt = _as_int(decision.get("amount_raw"), 0) or int(amount_raw)
            if amt <= 0:
                return []
            return [
                ExitAction(
                    mint=mint,
                    amount_raw=amt,
                    reason=str(decision.get("reason") or "exit"),
                    sell_pct=max(0.0, min(1.0, _as_float(decision.get("sell_pct"), 1.0))),
                    symbol=str(decision.get("symbol") or symbol),
                    priority=_as_int(decision.get("priority"), 5),
                    meta=dict(decision.get("meta") or {}),
                )
            ]

        return []

    # Boolean decision
    if isinstance(decision, bool):
        if not decision:
            return []
        return [
            ExitAction(
                mint=mint,
                amount_raw=int(amount_raw),
                reason="exit",
                sell_pct=1.0,
                symbol=symbol,
                priority=5,
                meta={},
            )
        ]

    return []


async def _call_exit_brain(
    *,
    mint: str,
    norm_mint: str,
    symbol: str,
    pos: dict[str, Any],
    price_ui: float,
    entry_price: float | None,
    buy_time: datetime | None,
    balance_ui: float,
    amount_raw: int,
    decimals: int,
    redis: Any,
) -> list[ExitAction]:
    """
    Calls utils_pkg.exit_engine via best-effort function detection.
    """
    if exit_brain is None:
        return []

    # Candidate function names (ordered)
    candidates = [
        "evaluate_position",
        "decide_exit",
        "on_position_tick",
        "compute_exit",
        "get_exit_actions",
        "maybe_exit",
        "should_exit",  # may return bool/tuple
    ]

    fn = None
    for name in candidates:
        f = getattr(exit_brain, name, None)
        if callable(f):
            fn = f
            break
    if fn is None:
        return []

    kwargs = {
        # identifiers
        "mint": mint,
        "norm_mint": norm_mint,
        "symbol": symbol,
        # snapshots
        "position": pos,
        "pos": pos,
        "price_ui": float(price_ui),
        "current_price": float(price_ui),
        "entry_price": float(entry_price) if entry_price is not None else None,
        "buy_time": buy_time,
        "now": _now_utc(),
        # balances
        "balance_ui": float(balance_ui),
        "amount_raw": int(amount_raw),
        "decimals": int(decimals),
        # deps
        "redis": redis,
    }

    kwargs2 = _filter_kwargs(fn, kwargs)
    try:
        decision = await _maybe_await(fn(**kwargs2))
        return _parse_exit_actions(decision, mint=mint, symbol=symbol, amount_raw=amount_raw)
    except Exception as e:
        _log(
            "AUTOSELL_EXIT_BRAIN_ERR",
            level="warning",
            mint=mint,
            err=_short_exc(e),
            fn=getattr(fn, "__name__", "?"),
        )
        return []


# -----------------------------------------------------------------------------
# Queue spam guard
# -----------------------------------------------------------------------------
async def _mint_queue_guard(redis: Any, mint: str) -> bool:
    """
    Returns True if allowed to enqueue now, False if recently queued.
    """
    if redis is None:
        return True
    key = f"autosell:queued:{mint}"
    try:
        ok = await _maybe_await(redis.set(key, "1", nx=True, ex=int(AUTO_SELL_QUEUE_COOLDOWN_S)))
        return bool(ok)
    except Exception:
        return True


# -----------------------------------------------------------------------------
# Main sweep loop
# -----------------------------------------------------------------------------
async def sweep_once() -> int:
    """
    One sweep pass. Returns number of enqueue actions performed.
    """
    if not AUTO_SELL_ENABLED:
        return 0

    await _init_resources_best_effort()
    r = await _get_redis()

    # We still allow exits even if halted; this is for visibility.
    with contextlib.suppress(Exception):
        halted = await _maybe_await(trading_halted(r))
        if halted:
            _log("AUTOSELL_TRADING_HALTED", level="warning")

    # Load positions
    try:
        positions = await _maybe_await(load_positions()) or {}
        if not isinstance(positions, dict):
            positions = {}
    except Exception as e:
        _log("AUTOSELL_LOAD_POS_ERR", level="warning", err=_short_exc(e))
        return 0

    if not positions:
        return 0

    # Wallet balances (authoritative)
    wallet_map = await _wallet_token_balances()

    # Parallelize per-mint work, bounded
    sem = asyncio.Semaphore(int(AUTO_SELL_MAX_PARALLEL))
    enqueued = 0

    async def _handle_one(mint_key: str, pos: dict[str, Any]) -> int:
        nonlocal r
        async with sem:
            mint_key = str(mint_key or "").strip()
            if not mint_key:
                return 0

            symbol = _position_symbol(pos, mint_key)

            # Determine mint used for wallet & router
            norm_mint = _normalize_mint(mint_key)

            # Get wallet balance using normalized mint first, fallback to key
            bal_ui = float(wallet_map.get(norm_mint, wallet_map.get(mint_key, 0.0)) or 0.0)
            if bal_ui <= 0:
                # Position no longer in wallet; cleanup best-effort
                with contextlib.suppress(Exception):
                    await _maybe_await(remove_position(mint_key))
                _log("AUTOSELL_POSITION_CLEANUP", mint=mint_key, symbol=symbol)
                return 0

            # Price snapshot (best-effort)
            px = await _price_ui(norm_mint)
            if px <= 0:
                _log("AUTOSELL_PRICE_MISSING", level="warning", mint=mint_key, symbol=symbol)
                return 0

            # Entry/buy_time are optional hints for the brain; it may ignore.
            entry_price = None
            with contextlib.suppress(Exception):
                v = pos.get("buy_price") if isinstance(pos, dict) else None
                if isinstance(v, int | float) and float(v) > 0:
                    entry_price = float(v)

            buy_time = None
            with contextlib.suppress(Exception):
                ts = pos.get("timestamp") or pos.get("ts")
                if ts:
                    dt = datetime.fromisoformat(str(ts))
                    buy_time = dt if dt.tzinfo else dt.replace(tzinfo=UTC)

            dec = await _decimals(norm_mint)
            amount_raw = int(max(0.0, bal_ui) * (10 ** int(dec)))

            if amount_raw <= 0:
                return 0

            # Ask exit brain for actions
            actions = await _call_exit_brain(
                mint=mint_key,
                norm_mint=norm_mint,
                symbol=symbol,
                pos=pos,
                price_ui=px,
                entry_price=entry_price,
                buy_time=buy_time,
                balance_ui=bal_ui,
                amount_raw=amount_raw,
                decimals=dec,
                redis=r,
            )
            if not actions:
                return 0

            if enqueue_exit_request is None:
                _log("AUTOSELL_NO_ENQUEUE_FN", level="error", mint=mint_key, symbol=symbol)
                return 0

            did = 0
            for a in actions:
                # spam guard per mint per interval
                if not await _mint_queue_guard(r, mint_key):
                    continue

                meta = dict(a.meta or {})
                meta.update(
                    {
                        "source": "sniper.auto_sell",
                        "norm_mint": norm_mint,
                        "price_ui": float(px),
                        "balance_ui": float(bal_ui),
                        "decimals": int(dec),
                        "ts": time.time(),
                    }
                )

                # enqueue
                try:
                    rid = await _maybe_await(
                        enqueue_exit_request(
                            mint=norm_mint,  # executor should trade real mint
                            amount_raw=int(a.amount_raw),
                            reason=str(a.reason or "exit"),
                            sell_pct=float(a.sell_pct or 1.0),
                            symbol=str(a.symbol or symbol),
                            priority=int(a.priority or 5),
                            meta=meta,
                            # keep original key for bookkeeping/debug
                            pos_key=mint_key,
                        )
                    )
                    if rid:
                        did += 1
                        _log(
                            "AUTOSELL_ENQUEUED",
                            rid=str(rid),
                            mint=mint_key,
                            norm_mint=norm_mint,
                            symbol=symbol,
                            reason=str(a.reason or "exit"),
                            amount_raw=int(a.amount_raw),
                            sell_pct=float(a.sell_pct or 1.0),
                        )
                except Exception as e:
                    _log("AUTOSELL_ENQUEUE_ERR", level="warning", mint=mint_key, err=_short_exc(e))

            return did

    tasks = [
        asyncio.create_task(_handle_one(m, p), name=f"autosell_{m}")
        for m, p in positions.items()
        if isinstance(p, dict)
    ]
    if tasks:
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for x in results:
            if isinstance(x, Exception):
                continue
            try:
                enqueued += int(x)
            except Exception:
                continue

    return enqueued


async def sweep_loop() -> None:
    """
    Long-running loop for Render/CLI.
    """
    if not AUTO_SELL_ENABLED:
        _log("AUTOSELL_DISABLED")
        return

    _log(
        "AUTOSELL_START",
        enabled=int(AUTO_SELL_ENABLED),
        interval_s=int(AUTO_SELL_INTERVAL_S),
        max_parallel=int(AUTO_SELL_MAX_PARALLEL),
        queue_cooldown_s=int(AUTO_SELL_QUEUE_COOLDOWN_S),
        price_timeout_s=int(AUTO_SELL_PRICE_TIMEOUT_S),
    )

    try:
        while True:
            try:
                await sweep_once()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                _log("AUTOSELL_LOOP_ERR", level="warning", err=_short_exc(e))
            await asyncio.sleep(float(AUTO_SELL_INTERVAL_S))
    finally:
        await _close_resources_best_effort()


async def main() -> None:
    await _init_resources_best_effort()
    try:
        await sweep_loop()
    finally:
        await _close_resources_best_effort()


if __name__ == "__main__":
    asyncio.run(main())
