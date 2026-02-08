from __future__ import annotations

"""
sniper/auto_sell.py

God-Tier Auto Sell Adapter — V3+++ LOCK-IN (production-first)
=============================================================

DEDUP / CONTRACT (LOCKED)
------------------------
- utils_pkg/exit_engine.py is the *canonical exit brain* (decision logic).
- utils_pkg/auto_exit_worker.py is the *canonical executor* (SELL queue + crash-safe execution).
- This module is ONLY a thin adapter:
    positions -> (price snapshot) -> exit_engine decision -> enqueue_exit_request

ABSOLUTELY NO competing exit logic lives here:
- no trailing stop math
- no AI should_hold_token
- no direct sell execution here
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

Safety defaults
---------------
- If exit_engine is missing/errors -> HOLD (safe default)
- If price missing -> HOLD
- If wallet balance is zero -> cleanup position best-effort
- Never throws to caller (service loop resiliency)
- No network I/O at import time (lazy imports)

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
- Amount_raw is computed from *wallet balances* (authoritative), not from positions.
- We normalize Pump IDs -> real mint for pricing/decimals, but preserve the original
  mint key for position storage.
"""

import asyncio
import contextlib
import importlib
import inspect
import logging
import os
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any


# =============================================================================
# Logging (safe; compatible with logging_setup if present)
# =============================================================================
def _mk_logger() -> Any:
    # Prefer project logger if available, but never break import.
    for mod_name in ("utils_pkg.logging_setup", "logging_setup"):
        with contextlib.suppress(Exception):
            m = importlib.import_module(mod_name)
            fn = getattr(m, "get_logger", None)
            if callable(fn):
                with contextlib.suppress(Exception):
                    return fn("sniper.auto_sell")
    lg = logging.getLogger("sniper.auto_sell")
    if not lg.handlers:
        logging.basicConfig(level=logging.INFO)
    return lg


log = _mk_logger()


def _log(name: str, level: str = "info", **fields: Any) -> None:
    """
    Log event-style if logger supports .event, otherwise standard logging.
    """
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


# =============================================================================
# Env helpers (tiny + stable)
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


# =============================================================================
# Config (module-level defaults; loop reads these)
# =============================================================================
AUTO_SELL_ENABLED = _env_bool("AUTO_SELL_ENABLED", True)

# Legacy alias: POSITION_MONITOR_INTERVAL
AUTO_SELL_INTERVAL_S = max(
    2, _env_int("AUTO_SELL_INTERVAL_S", _env_int("POSITION_MONITOR_INTERVAL", 20))
)
AUTO_SELL_MAX_PARALLEL = max(1, _env_int("AUTO_SELL_MAX_PARALLEL", 8))
AUTO_SELL_QUEUE_COOLDOWN_S = max(1, _env_int("AUTO_SELL_QUEUE_COOLDOWN_S", 12))
AUTO_SELL_PRICE_TIMEOUT_S = max(1, _env_int("AUTO_SELL_PRICE_TIMEOUT_S", 6))

WALLET_PUBKEY = (os.getenv("WALLET_PUBKEY") or os.getenv("WALLET_PUBLIC_KEY") or "").strip()


def _now_utc() -> datetime:
    return datetime.now(UTC)


# =============================================================================
# Lazy imports (NO heavy imports at module import time)
# =============================================================================
_mod_cache: dict[str, Any] = {}


def _import_optional(module: str) -> Any | None:
    if module in _mod_cache:
        return _mod_cache[module]
    try:
        m = importlib.import_module(module)
    except Exception:
        m = None
    _mod_cache[module] = m
    return m


async def _maybe_await(x: Any) -> Any:
    return await x if asyncio.iscoroutine(x) else x


def _filter_kwargs(fn: Any, kwargs: dict[str, Any]) -> dict[str, Any]:
    """
    Only pass kwargs accepted by fn signature (avoids TypeError across versions).
    """
    try:
        sig = inspect.signature(fn)
        allowed = set(sig.parameters.keys())
        return {k: v for k, v in kwargs.items() if k in allowed}
    except Exception:
        return kwargs


# =============================================================================
# Positions access (services.positions preferred; fallback to positions; stub-safe)
# =============================================================================
async def _load_positions() -> dict[str, Any]:
    m = _import_optional("services.positions") or _import_optional("positions")
    if m is None:
        return {}
    fn = getattr(m, "load_positions", None)
    if not callable(fn):
        return {}
    try:
        out = await _maybe_await(fn())
        return out if isinstance(out, dict) else {}
    except Exception as e:
        _log("AUTOSELL_LOAD_POS_ERR", level="warning", err=_short_exc(e))
        return {}


async def _remove_position(pos_key: str) -> None:
    m = _import_optional("services.positions") or _import_optional("positions")
    if m is None:
        return
    fn = getattr(m, "remove_position", None)
    if not callable(fn):
        return
    with contextlib.suppress(Exception):
        await _maybe_await(fn(pos_key))


# =============================================================================
# core_utils resources (best-effort)
# =============================================================================
async def _init_resources_best_effort() -> None:
    cu = _import_optional("utils_pkg.core_utils")
    if cu is None:
        return
    fn = getattr(cu, "init_resources", None)
    if callable(fn):
        with contextlib.suppress(Exception):
            await _maybe_await(fn())


async def _close_resources_best_effort() -> None:
    cu = _import_optional("utils_pkg.core_utils")
    if cu is None:
        return
    fn = getattr(cu, "close_resources", None)
    if callable(fn):
        with contextlib.suppress(Exception):
            await _maybe_await(fn())


async def _get_redis() -> Any:
    cu = _import_optional("utils_pkg.core_utils")
    if cu is None:
        return None
    for nm in ("get_redis_async", "get_redis"):
        fn = getattr(cu, nm, None)
        if callable(fn):
            with contextlib.suppress(Exception):
                return await _maybe_await(fn())
    return getattr(cu, "redis", None)


# =============================================================================
# Wallet snapshot (authoritative sellable amounts)
# =============================================================================
async def _wallet_token_balances() -> dict[str, float]:
    """
    Returns mint -> ui balance (float). Best-effort.
    """
    if not WALLET_PUBKEY:
        return {}

    cu = _import_optional("utils_pkg.core_utils")
    if cu is None:
        return {}

    fn = getattr(cu, "get_wallet_tokens", None)
    if not callable(fn):
        return {}

    try:
        toks = await _maybe_await(fn(WALLET_PUBKEY))
        out: dict[str, float] = {}
        for t in toks or []:
            if not isinstance(t, dict):
                continue
            mint = str(t.get("mint") or "").strip()
            if not mint:
                continue
            try:
                bal = float(t.get("balance") or 0.0)
            except Exception:
                continue
            out[mint] = bal
        return out
    except Exception as e:
        _log("AUTOSELL_WALLET_TOKENS_ERR", level="warning", err=_short_exc(e))
        return {}


# =============================================================================
# Router helpers (normalize, decimals, price snapshot) — best-effort, timeboxed
# =============================================================================
def _router() -> Any | None:
    return _import_optional("utils_pkg.router")


def _normalize_mint(mint: str) -> str:
    m = str(mint or "").strip()
    r = _router()
    if r is None:
        return m
    fn = getattr(r, "normalize_pump_id_to_mint", None)
    if callable(fn):
        with contextlib.suppress(Exception):
            nm = str(fn(m) or "").strip()
            if nm:
                return nm
    return m


async def _decimals(norm_mint: str) -> int:
    r = _router()
    fn = getattr(r, "get_decimals", None) if r is not None else None
    if not callable(fn):
        return 9
    try:
        d = await _maybe_await(fn(norm_mint))
        d2 = int(d) if d is not None else 9
        return max(0, min(18, d2))
    except Exception:
        return 9


async def _price_ui(norm_mint: str) -> float:
    r = _router()
    fn = getattr(r, "get_price_ui", None) if r is not None else None
    if not callable(fn):
        return 0.0
    try:
        out = await asyncio.wait_for(
            _maybe_await(fn(norm_mint)),
            timeout=float(AUTO_SELL_PRICE_TIMEOUT_S),
        )
        return float(out or 0.0)
    except Exception:
        return 0.0


def _position_symbol(pos: dict[str, Any], mint: str) -> str:
    sym = str(pos.get("symbol") or "").strip()
    return sym or mint[:6]


def _parse_buy_time(pos: dict[str, Any]) -> datetime | None:
    """
    Best-effort parse of timestamp fields. Accepts ISO strings with optional 'Z'.
    """
    ts = pos.get("timestamp") or pos.get("ts")
    if not ts:
        return None
    s = str(ts).strip()
    if not s:
        return None
    # normalize trailing Z -> +00:00 for fromisoformat
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    with contextlib.suppress(Exception):
        dtv = datetime.fromisoformat(s)
        return dtv if dtv.tzinfo else dtv.replace(tzinfo=UTC)
    return None


def _parse_entry_price(pos: dict[str, Any]) -> float | None:
    v = pos.get("buy_price")
    if isinstance(v, (int, float)) and float(v) > 0:
        return float(v)

    if v is None:
        return None

    with contextlib.suppress(Exception):
        # Coerce through str to avoid mypy "Any|None" arg-type complaint.
        s = str(v).strip()
        if not s:
            return None
        f = float(s)
        return f if f > 0 else None

    return None


# =============================================================================
# Exit decision plumbing (thin adapter only)
# =============================================================================
@dataclass(frozen=True)
class ExitAction:
    mint: str
    amount_raw: int
    reason: str
    sell_pct: float = 1.0
    symbol: str = ""
    priority: int = 5
    meta: dict[str, Any] = field(default_factory=dict)


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


def _clamp01(x: float) -> float:
    if x < 0.0:
        return 0.0
    if x > 1.0:
        return 1.0
    return x


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

    # tuple: (should_exit, reason, sell_pct?, meta?)
    if isinstance(decision, tuple) and len(decision) >= 2:
        try:
            should_exit = bool(decision[0])
            if not should_exit:
                return []
            reason = str(decision[1] or "exit")
            sell_pct = _clamp01(_as_float(decision[2], 1.0)) if len(decision) >= 3 else 1.0
            meta = dict(decision[3] or {}) if len(decision) >= 4 else {}
            return [
                ExitAction(
                    mint=mint,
                    amount_raw=int(amount_raw),
                    reason=reason,
                    sell_pct=sell_pct,
                    symbol=symbol,
                    priority=_as_int(meta.get("priority"), 5),
                    meta=meta,
                )
            ]
        except Exception:
            return []

    # dict decisions: {"actions":[...]} or {"action":"sell", ...} or {"should_exit":True, ...}
    if isinstance(decision, dict):
        actions_obj = decision.get("actions")
        if isinstance(actions_obj, list):
            out: list[ExitAction] = []
            for a in actions_obj:
                if not isinstance(a, dict):
                    continue
                act = str(a.get("type") or a.get("action") or "").lower()
                if act not in {"sell", "exit"}:
                    continue
                amt = _as_int(a.get("amount_raw"), 0) or int(amount_raw)
                if amt <= 0:
                    continue
                out.append(
                    ExitAction(
                        mint=mint,
                        amount_raw=int(amt),
                        reason=str(a.get("reason") or "exit"),
                        sell_pct=_clamp01(_as_float(a.get("sell_pct"), 1.0)),
                        symbol=str(a.get("symbol") or symbol),
                        priority=_as_int(a.get("priority"), 5),
                        meta=dict(a.get("meta") or {}),
                    )
                )
            return out

        act2 = str(decision.get("action") or decision.get("type") or "").lower()
        if act2 in {"sell", "exit"} or decision.get("should_exit") is True:
            amt2 = _as_int(decision.get("amount_raw"), 0) or int(amount_raw)
            if amt2 <= 0:
                return []
            return [
                ExitAction(
                    mint=mint,
                    amount_raw=int(amt2),
                    reason=str(decision.get("reason") or "exit"),
                    sell_pct=_clamp01(_as_float(decision.get("sell_pct"), 1.0)),
                    symbol=str(decision.get("symbol") or symbol),
                    priority=_as_int(decision.get("priority"), 5),
                    meta=dict(decision.get("meta") or {}),
                )
            ]
        return []

    # bool decisions: True => sell 100%
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
    brain = _import_optional("utils_pkg.exit_engine")
    if brain is None:
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
        f = getattr(brain, name, None)
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


# =============================================================================
# Enqueue plumbing (signature-filtered to avoid drift across executor versions)
# =============================================================================
def _enqueuer() -> Any | None:
    m = _import_optional("utils_pkg.auto_exit_worker")
    if m is None:
        return None
    fn = getattr(m, "enqueue_exit_request", None)
    return fn if callable(fn) else None


async def _enqueue_exit_request_safe(fn: Any, kwargs: dict[str, Any]) -> Any:
    kw = _filter_kwargs(fn, kwargs)
    return await _maybe_await(fn(**kw))


# =============================================================================
# Queue spam guard
# =============================================================================
async def _mint_queue_guard(redis: Any, mint: str) -> bool:
    """
    Returns True if allowed to enqueue now, False if recently queued.
    """
    if redis is None:
        return True
    key = f"autosell:queued:{mint}"
    try:
        ok = await _maybe_await(redis.set(key, "1", nx=True, ex=int(AUTO_SELL_QUEUE_COOLDOWN_S)))
        # redis can return True/False, "OK", or None depending on client
        return bool(ok)
    except Exception:
        return True


# =============================================================================
# Circuit breaker visibility (telemetry only; exits still allowed)
# =============================================================================
async def _log_halt_state_best_effort() -> None:
    cb = _import_optional("utils_pkg.circuit_breakers")
    if cb is None:
        return
    fn = getattr(cb, "should_halt", None)
    if not callable(fn):
        return
    with contextlib.suppress(Exception):
        halt, reason, _snap = fn()
        if bool(halt):
            _log("AUTOSELL_TRADING_HALTED", level="warning", reason=str(reason)[:160])


# =============================================================================
# Main sweep loop
# =============================================================================
async def sweep_once() -> int:
    """
    One sweep pass. Returns number of enqueue actions performed.
    """
    if not AUTO_SELL_ENABLED:
        return 0

    await _init_resources_best_effort()
    redis = await _get_redis()

    # Telemetry only (we still allow exits)
    await _log_halt_state_best_effort()

    positions = await _load_positions()
    if not positions:
        return 0

    wallet_map = await _wallet_token_balances()

    sem = asyncio.Semaphore(int(AUTO_SELL_MAX_PARALLEL))
    enqueued = 0

    async def _handle_one(mint_key: str, pos: dict[str, Any]) -> int:
        nonlocal redis
        async with sem:
            mk = str(mint_key or "").strip()
            if not mk or not isinstance(pos, dict):
                return 0

            symbol = _position_symbol(pos, mk)
            norm_mint = _normalize_mint(mk)

            bal_ui = float(wallet_map.get(norm_mint, wallet_map.get(mk, 0.0)) or 0.0)
            if bal_ui <= 0:
                # Position no longer in wallet; cleanup best-effort
                await _remove_position(mk)
                _log("AUTOSELL_POSITION_CLEANUP", mint=mk, symbol=symbol)
                return 0

            px = await _price_ui(norm_mint)
            if px <= 0:
                _log("AUTOSELL_PRICE_MISSING", level="warning", mint=mk, symbol=symbol)
                return 0

            dec = await _decimals(norm_mint)
            # floor to avoid overselling due to float rounding
            amount_raw = int(max(0.0, bal_ui) * (10 ** int(dec)))
            if amount_raw <= 0:
                return 0

            entry_price = _parse_entry_price(pos)
            buy_time = _parse_buy_time(pos)

            actions = await _call_exit_brain(
                mint=mk,
                norm_mint=norm_mint,
                symbol=symbol,
                pos=pos,
                price_ui=px,
                entry_price=entry_price,
                buy_time=buy_time,
                balance_ui=bal_ui,
                amount_raw=amount_raw,
                decimals=dec,
                redis=redis,
            )
            if not actions:
                return 0

            fn_enq = _enqueuer()
            if fn_enq is None:
                _log("AUTOSELL_NO_ENQUEUE_FN", level="error", mint=mk, symbol=symbol)
                return 0

            did = 0
            for a in actions:
                if not await _mint_queue_guard(redis, mk):
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

                # Build kwargs and signature-filter to avoid executor drift.
                req = {
                    "mint": norm_mint,  # executor should trade real mint
                    "amount_raw": int(a.amount_raw),
                    "reason": str(a.reason or "exit"),
                    "sell_pct": float(a.sell_pct or 1.0),
                    "symbol": str(a.symbol or symbol),
                    "priority": int(a.priority or 5),
                    "meta": meta,
                    # keep original key for bookkeeping/debug when supported
                    "pos_key": mk,
                }

                try:
                    rid = await _enqueue_exit_request_safe(fn_enq, req)
                    if rid:
                        did += 1
                        _log(
                            "AUTOSELL_ENQUEUED",
                            rid=str(rid),
                            mint=mk,
                            norm_mint=norm_mint,
                            symbol=symbol,
                            reason=str(a.reason or "exit"),
                            amount_raw=int(a.amount_raw),
                            sell_pct=float(a.sell_pct or 1.0),
                        )
                except Exception as e:
                    _log("AUTOSELL_ENQUEUE_ERR", level="warning", mint=mk, err=_short_exc(e))

            return did

    tasks = [
        asyncio.create_task(_handle_one(m, p), name=f"autosell_{str(m)[:24]}")
        for m, p in positions.items()
        if isinstance(p, dict)
    ]
    if tasks:
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for x in results:
            # gather(..., return_exceptions=True) can return BaseException (not always Exception)
            if isinstance(x, BaseException):
                continue
            enqueued += int(x)

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


__all__ = ["sweep_once", "sweep_loop", "main"]


if __name__ == "__main__":
    asyncio.run(main())
