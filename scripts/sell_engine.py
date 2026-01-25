# scripts/sell_engine.py
from __future__ import annotations

"""
scripts/sell_engine.py

God-Tier Sell/Exit Harness (V3+++ / quant-grade) — NON-COMPETING
===============================================================

This script is intentionally *NOT* a second exit engine.

LOCK-IN (P0/P2 dedup rules)
--------------------------
- utils_pkg/exit_engine.py is the canonical "exit brain".
- utils_pkg/auto_exit_worker/exit_engine.py is the canonical "execution worker".
- This script is a *harness* / *smoke tool* that:
    1) Reads positions (utils_pkg.positions)
    2) Gets a current price (utils_pkg.router)
    3) Asks exit brain for a decision (best-effort compatibility layer)
    4) Enqueues exit requests into the worker queue
    5) Optionally drains the queue (local only) via worker.process_one()

Defaults are SAFE:
- DRY_RUN behavior is default unless you explicitly pass --live
- If the exit brain surface is unknown/unavailable, we refuse to enqueue.

Usage
-----
# report only (default)
python -m scripts.sell_engine --once

# enqueue exit requests (still dry-run unless --live)
python -m scripts.sell_engine --once --enqueue

# enqueue + locally process (useful for local smoke)
python -m scripts.sell_engine --once --enqueue --drain

# continuous monitor (low-frequency)
python -m scripts.sell_engine --loop --enqueue --interval 10

Notes
-----
- This is meant to be used as: "Is the exit path wired? Why isn't it exiting?"
- Production exit execution should be done by the auto-exit worker service.
"""

import argparse
import asyncio
import importlib
import inspect
import os
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from logging_setup import bump, get_logger

log = get_logger()


# ──────────────────────────────────────────────────────────────────────────────
# Env helpers
# ──────────────────────────────────────────────────────────────────────────────
def _envf(k: str, d: float) -> float:
    try:
        return float(os.getenv(k, str(d)))
    except Exception:
        return float(d)


def _envi(k: str, d: int) -> int:
    try:
        return int(os.getenv(k, str(d)))
    except Exception:
        return int(d)


def _envb(k: str, d: bool = False) -> bool:
    v = (os.getenv(k, "") or "").strip().lower()
    if not v:
        return d
    return v in {"1", "true", "yes", "y", "on"}


# ──────────────────────────────────────────────────────────────────────────────
# Tunables
# ──────────────────────────────────────────────────────────────────────────────
SLIPPAGE_BPS_DEFAULT = max(1, _envi("PF_SLIPPAGE_BPS", 600))
PRIORITY_SOL_DEFAULT = max(0.0, _envf("PF_PRIORITY_SOL", 0.002))
MIN_NOTIONAL_SOL = max(0.0, _envf("PF_MIN_SELL_SOL", 0.0))
PRICE_TTL_SEC = max(1, _envi("PF_PRICE_TTL_SEC", 5))
DRAIN_MAX_STEPS = max(1, _envi("SELL_ENGINE_DRAIN_MAX_STEPS", 200))
DRAIN_IDLE_BREAK_SEC = max(1, _envi("SELL_ENGINE_DRAIN_IDLE_BREAK_SEC", 2))


# ──────────────────────────────────────────────────────────────────────────────
# Positions source (LOCK-IN: utils_pkg.positions is canonical)
# ──────────────────────────────────────────────────────────────────────────────
def _load_positions_module():
    try:
        return importlib.import_module("utils_pkg.positions")
    except Exception:
        return importlib.import_module("positions")


def _iter_positions(pos_mod: Any) -> list[dict[str, Any]]:
    """
    Contract:
      - preferred: positions.all() -> Iterable[dict]
      - acceptable: positions.load_positions() -> dict[mint, dict]
    """
    if hasattr(pos_mod, "all") and callable(pos_mod.all):
        try:
            out = list(pos_mod.all())  # type: ignore[attr-defined]
            return [p for p in out if isinstance(p, dict)]
        except Exception:
            return []
    if hasattr(pos_mod, "load_positions") and callable(pos_mod.load_positions):
        try:
            m = pos_mod.load_positions()  # type: ignore[attr-defined]
            if isinstance(m, dict):
                out = []
                for mint, p in m.items():
                    if isinstance(p, dict):
                        p = dict(p)
                        p.setdefault("mint", mint)
                        out.append(p)
                return out
        except Exception:
            return []
    return []


# ──────────────────────────────────────────────────────────────────────────────
# Router helpers (price + mint normalization)
# ──────────────────────────────────────────────────────────────────────────────
_price_cache: dict[str, tuple[float, float]] = {}


async def _router_price_ui(mint: str) -> float:
    """
    Canonical price getter:
      utils_pkg.router.get_price_ui(mint) -> float

    Fallback: best-effort quote-based derivation if router exposes get_sell_quote.
    Returns 0.0 on failure.
    """
    now = time.time()
    rec = _price_cache.get(mint)
    if rec and (now - rec[0]) <= PRICE_TTL_SEC:
        return float(rec[1])

    try:
        router = importlib.import_module("utils_pkg.router")
    except Exception:
        _price_cache[mint] = (now, 0.0)
        return 0.0

    # 1) direct getter
    fn = getattr(router, "get_price_ui", None)
    if callable(fn):
        try:
            px = float(await fn(mint) or 0.0)  # type: ignore[misc]
            _price_cache[mint] = (now, px)
            return px
        except Exception:
            pass

    # 2) quote fallback
    q_fn = getattr(router, "get_sell_quote", None)
    dec_fn = getattr(router, "get_decimals", None)
    if callable(q_fn) and callable(dec_fn):
        try:
            decimals = int(await dec_fn(mint) or 0)  # type: ignore[misc]
            decimals = max(0, min(18, decimals))
            tiny_raw = max(1, 10 ** max(0, decimals - 3))  # ~0.001 token
            q = await q_fn(mint=mint, amount_raw=int(tiny_raw))  # type: ignore[misc]
            if not isinstance(q, dict):
                _price_cache[mint] = (now, 0.0)
                return 0.0
            out_sol = (
                q.get("out_amount_sol")
                or q.get("out_sol")
                or q.get("outSOL")
                or (q.get("lamports_out", 0) / 1e9 if q.get("lamports_out") else 0.0)
                or 0.0
            )
            if not out_sol:
                _price_cache[mint] = (now, 0.0)
                return 0.0
            token_ui = tiny_raw / (10**decimals) if decimals else float(tiny_raw)
            px = float(out_sol) / float(token_ui) if token_ui else 0.0
            _price_cache[mint] = (now, px)
            return px
        except Exception:
            pass

    _price_cache[mint] = (now, 0.0)
    return 0.0


def _normalize_mint(mint: str) -> str:
    try:
        router = importlib.import_module("utils_pkg.router")
        fn = getattr(router, "normalize_pump_id_to_mint", None)
        if callable(fn):
            return str(fn(mint))
    except Exception:
        pass
    return mint


# ──────────────────────────────────────────────────────────────────────────────
# Exit brain compatibility layer (best-effort but SAFE)
# ──────────────────────────────────────────────────────────────────────────────
@dataclass
class ExitDecision:
    should_exit: bool
    reason: str
    sell_pct: float  # 0..1
    confidence: float = 0.0
    meta: dict[str, Any] = None  # type: ignore[assignment]

    def __post_init__(self) -> None:
        if self.meta is None:
            self.meta = {}
        self.sell_pct = float(max(0.0, min(1.0, self.sell_pct)))


def _pick_exit_callable(exit_mod: Any) -> Callable[..., Any] | None:
    """
    Prefer stable function entrypoints; fall back to common names.
    """
    for name in (
        "decide_exit",
        "evaluate_position",
        "compute_exit",
        "on_position_tick",
        "get_exit_actions",
        "maybe_exit",
        "should_exit",
        "exit_decision",
    ):
        fn = getattr(exit_mod, name, None)
        if callable(fn):
            return fn

    # Class-based engine
    cls = getattr(exit_mod, "ExitEngine", None)
    if callable(cls):
        try:
            eng = cls()  # type: ignore[call-arg]
        except Exception:
            return None
        for m in ("decide_exit", "evaluate_position", "on_position_tick", "compute_exit"):
            fn = getattr(eng, m, None)
            if callable(fn):
                # bind method with instance already
                return fn  # type: ignore[return-value]

    return None


def _call_with_accepted_kwargs(fn: Callable[..., Any], kwargs: dict[str, Any]) -> Any:
    """
    Only pass kwargs that the function accepts (prevents signature mismatch crashes).
    If signature can't be inspected, call with the full kwargs best-effort.
    """
    try:
        sig = inspect.signature(fn)
        accepted = {}
        for k, v in kwargs.items():
            if k in sig.parameters:
                accepted[k] = v
        return fn(**accepted)
    except Exception:
        return fn(**kwargs)


async def _exit_decision_for_position(p: dict[str, Any], price_sol: float) -> ExitDecision | None:
    """
    Asks utils_pkg.exit_engine for a decision.

    If exit engine interface is unknown or returns an unexpected type,
    we return None (SAFE: do not enqueue).
    """
    try:
        exit_mod = importlib.import_module("utils_pkg.exit_engine")
    except Exception:
        return None

    fn = _pick_exit_callable(exit_mod)
    if fn is None:
        return None

    mint = str(p.get("mint") or "")
    symbol = str(p.get("symbol") or mint[:6] or "UNK")
    entry = float(p.get("avg_cost_sol", 0.0) or 0.0)

    # Common context payload; we filter by signature.
    payload: dict[str, Any] = {
        "mint": mint,
        "symbol": symbol,
        "position": p,
        "pos": p,
        "price_ui": price_sol,
        "current_price": price_sol,
        "current_price_sol": price_sol,
        "entry_price": entry,
        "entry_price_sol": entry,
        "now": None,
        "redis": None,
    }

    try:
        if asyncio.iscoroutinefunction(fn):
            out = await _call_with_accepted_kwargs(fn, payload)  # type: ignore[misc]
        else:
            out = _call_with_accepted_kwargs(fn, payload)
            if inspect.isawaitable(out):
                out = await out
    except Exception as e:
        log.event("EXIT_BRAIN_ERROR", mint=mint, symbol=symbol, error=str(e))
        bump("exit_brain_err")
        return None

    # Normalize output into ExitDecision
    # Supported patterns:
    #   - dict with keys: should_exit/exit, reason, sell_pct/pct, confidence, meta
    #   - tuple: (bool, reason, sell_pct) or (bool, reason)
    #   - boolean: should_exit (sell_pct=1)
    try:
        if isinstance(out, ExitDecision):
            return out
        if isinstance(out, dict):
            should = bool(
                out.get("should_exit") or out.get("exit") or out.get("should_sell") or False
            )
            reason = str(out.get("reason") or out.get("why") or "")
            sell_pct = float(
                out.get("sell_pct")
                or out.get("pct")
                or out.get("sell_fraction")
                or (1.0 if should else 0.0)
            )
            conf = float(out.get("confidence") or out.get("conf") or 0.0)
            meta = dict(out.get("meta") or {})
            return ExitDecision(
                should_exit=should,
                reason=reason or ("EXIT" if should else ""),
                sell_pct=sell_pct,
                confidence=conf,
                meta=meta,
            )
        if isinstance(out, tuple) and len(out) >= 2:
            should = bool(out[0])
            reason = str(out[1] or "")
            sell_pct = float(out[2]) if len(out) >= 3 else (1.0 if should else 0.0)
            return ExitDecision(
                should_exit=should,
                reason=reason or ("EXIT" if should else ""),
                sell_pct=sell_pct,
                confidence=0.0,
                meta={},
            )
        if isinstance(out, bool):
            return ExitDecision(
                should_exit=bool(out),
                reason="EXIT" if out else "",
                sell_pct=1.0 if out else 0.0,
                confidence=0.0,
                meta={},
            )
    except Exception:
        return None

    return None


# ──────────────────────────────────────────────────────────────────────────────
# Worker queue integration (canonical)
# ──────────────────────────────────────────────────────────────────────────────
async def _enqueue_exit(
    *,
    mint: str,
    symbol: str,
    qty_raw: int,
    sell_pct: float,
    reason: str,
    slippage_bps: int,
    priority_sol: float,
    meta: dict[str, Any],
) -> str | None:
    """
    Enqueue into utils_pkg.auto_exit_worker.exit_engine queue.
    Returns request_id or None.
    """
    try:
        worker = importlib.import_module("utils_pkg.auto_exit_worker.exit_engine")
    except Exception as e:
        log.event("EXIT_WORKER_IMPORT_ERROR", error=str(e))
        return None

    mint_n = _normalize_mint(mint)

    # If sell_pct is partial, scale raw.
    sell_pct = float(max(0.0, min(1.0, sell_pct)))
    if sell_pct <= 0.0:
        return None
    amt_raw = int(max(1, min(qty_raw, int(qty_raw * sell_pct))))

    meta2 = dict(meta or {})
    meta2.setdefault("slippage_bps", int(slippage_bps))
    meta2.setdefault("priority_sol", float(priority_sol))
    meta2.setdefault("apply_sell_pct", False)

    try:
        rid = await worker.enqueue_exit_request(
            mint=mint_n,
            amount_raw=int(amt_raw),
            reason=str(reason),
            sell_pct=float(sell_pct),
            symbol=str(symbol),
            priority=5,
            meta=meta2,
        )
        return str(rid) if rid else None
    except Exception as e:
        log.event("EXIT_ENQUEUE_ERROR", mint=mint_n, symbol=symbol, error=str(e))
        bump("exit_enqueue_err")
        return None


async def _drain_queue(max_steps: int) -> dict[str, Any]:
    """
    Local-only: drain the worker queue by repeatedly calling process_one().
    This will no-op if the queue is empty.
    """
    try:
        worker = importlib.import_module("utils_pkg.auto_exit_worker.exit_engine")
    except Exception as e:
        return {"ok": False, "error": f"worker import failed: {e}"}

    steps = 0
    did_any = 0
    last_progress = time.time()
    while steps < max_steps:
        steps += 1
        try:
            did = await worker.process_one(worker_id="sell_engine")
        except Exception as e:
            return {"ok": False, "error": f"process_one error: {e}", "steps": steps, "did": did_any}
        if did:
            did_any += 1
            last_progress = time.time()
            continue
        # no work
        if (time.time() - last_progress) >= DRAIN_IDLE_BREAK_SEC:
            break
        await asyncio.sleep(0.05)

    return {"ok": True, "steps": steps, "did": did_any}


# ──────────────────────────────────────────────────────────────────────────────
# Main harness loop
# ──────────────────────────────────────────────────────────────────────────────
async def _one_pass(*, enqueue: bool, live: bool, max_positions: int) -> dict[str, Any]:
    pos_mod = _load_positions_module()
    positions = _iter_positions(pos_mod)
    if max_positions > 0:
        positions = positions[:max_positions]

    if not positions:
        return {"ok": True, "positions": 0, "decisions": 0, "enqueued": 0}

    decisions = 0
    enqueued = 0

    for p in positions:
        mint = str(p.get("mint") or "")
        if not mint:
            continue

        symbol = str(p.get("symbol") or mint[:6])
        qty_raw = int(p.get("qty_raw", 0) or 0)
        decimals = int(p.get("decimals", 0) or 0)
        entry = float(p.get("avg_cost_sol", 0.0) or 0.0)

        if qty_raw <= 0 or entry <= 0:
            log.event(
                "SELL_ENGINE_SKIP_BAD_POS", mint=mint, symbol=symbol, qty_raw=qty_raw, entry=entry
            )
            continue

        mint_n = _normalize_mint(mint)
        price = await _router_price_ui(mint_n)
        if price <= 0.0:
            log.every(
                f"sell_engine.no_price.{mint}",
                n=12,
                msg="SELL_ENGINE_NO_PRICE",
                mint=mint,
                symbol=symbol,
            )
            continue

        notional_sol = price * (qty_raw / (10 ** max(0, min(18, decimals))) if decimals else 0.0)
        change = (price / entry) - 1.0

        dec = await _exit_decision_for_position(
            {
                **p,
                "mint": mint,
                "symbol": symbol,
                "qty_raw": qty_raw,
                "decimals": decimals,
                "avg_cost_sol": entry,
            },
            price_sol=price,
        )

        if dec is None:
            # SAFE: no decision => no enqueue
            log.every(
                f"sell_engine.no_decision.{mint}",
                n=12,
                msg="SELL_ENGINE_NO_DECISION",
                mint=mint,
                symbol=symbol,
                price_sol=price,
                entry_sol=entry,
                change=change,
                notional_sol=notional_sol,
            )
            bump("sell_engine_no_decision")
            continue

        decisions += 1

        log.event(
            "SELL_ENGINE_DECISION",
            mint=mint,
            mint_norm=mint_n,
            symbol=symbol,
            price_sol=price,
            entry_sol=entry,
            change=change,
            notional_sol=notional_sol,
            should_exit=int(dec.should_exit),
            reason=dec.reason,
            sell_pct=dec.sell_pct,
            confidence=dec.confidence,
        )

        if not dec.should_exit:
            continue

        # Optional notional gate (prevents dust spam)
        if MIN_NOTIONAL_SOL > 0 and notional_sol < MIN_NOTIONAL_SOL:
            log.event(
                "SELL_ENGINE_SKIP_DUST",
                mint=mint,
                symbol=symbol,
                notional_sol=notional_sol,
                min_sol=MIN_NOTIONAL_SOL,
            )
            continue

        if not enqueue:
            continue

        # Live safety: require explicit flag
        if not live:
            log.event(
                "SELL_ENGINE_DRY_ENQUEUE_SKIP",
                mint=mint,
                symbol=symbol,
                reason=dec.reason,
                sell_pct=dec.sell_pct,
            )
            bump("sell_engine_dry")
            continue

        rid = await _enqueue_exit(
            mint=mint,
            symbol=symbol,
            qty_raw=qty_raw,
            sell_pct=dec.sell_pct if dec.sell_pct > 0 else 1.0,
            reason=dec.reason or "EXIT",
            slippage_bps=int(dec.meta.get("slippage_bps", SLIPPAGE_BPS_DEFAULT)),
            priority_sol=float(dec.meta.get("priority_sol", PRIORITY_SOL_DEFAULT)),
            meta=dec.meta or {},
        )
        if rid:
            enqueued += 1
            bump("exit_enqueued")
            log.event(
                "SELL_ENGINE_ENQUEUED",
                mint=mint,
                symbol=symbol,
                rid=rid,
                reason=dec.reason,
                sell_pct=dec.sell_pct,
            )

    return {"ok": True, "positions": len(positions), "decisions": decisions, "enqueued": enqueued}


async def main_async(args: argparse.Namespace) -> int:
    # Best-effort init hooks (some repos expose init_resources; not required here)
    try:
        cu = importlib.import_module("utils_pkg.core_utils")
        init_fn = getattr(cu, "init_resources", None)
        if callable(init_fn):
            if asyncio.iscoroutinefunction(init_fn):
                await init_fn()
            else:
                init_fn()
    except Exception:
        pass

    log.event(
        "SELL_ENGINE_START",
        once=int(args.once),
        loop=int(args.loop),
        enqueue=int(args.enqueue),
        drain=int(args.drain),
        live=int(args.live),
        interval_sec=args.interval,
        max_positions=args.max_positions,
        min_notional_sol=MIN_NOTIONAL_SOL,
        slippage_bps_default=SLIPPAGE_BPS_DEFAULT,
        priority_sol_default=PRIORITY_SOL_DEFAULT,
        price_ttl_sec=PRICE_TTL_SEC,
    )

    try:
        if args.once:
            res = await _one_pass(
                enqueue=args.enqueue, live=args.live, max_positions=args.max_positions
            )
            log.event("SELL_ENGINE_PASS", **res)
            if args.drain:
                dr = await _drain_queue(max_steps=DRAIN_MAX_STEPS)
                log.event("SELL_ENGINE_DRAIN", **dr)
            return 0

        stop_evt = asyncio.Event()

        async def _run_loop():
            while not stop_evt.is_set():
                res = await _one_pass(
                    enqueue=args.enqueue, live=args.live, max_positions=args.max_positions
                )
                log.event("SELL_ENGINE_PASS", **res)
                if args.drain:
                    dr = await _drain_queue(max_steps=DRAIN_MAX_STEPS)
                    log.event("SELL_ENGINE_DRAIN", **dr)
                await asyncio.sleep(max(2.0, float(args.interval)))

        task = asyncio.create_task(_run_loop())
        try:
            await task
        except asyncio.CancelledError:
            stop_evt.set()
            raise
        return 0

    finally:
        try:
            cu = importlib.import_module("utils_pkg.core_utils")
            close_fn = getattr(cu, "close_resources", None)
            if callable(close_fn):
                if asyncio.iscoroutinefunction(close_fn):
                    await close_fn()
                else:
                    close_fn()
        except Exception:
            pass


def _build_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(prog="sell_engine", description="Exit harness (non-competing).")
    mode = p.add_mutually_exclusive_group(required=False)
    mode.add_argument("--once", action="store_true", help="Run a single pass (default).")
    mode.add_argument("--loop", action="store_true", help="Run continuously.")
    p.add_argument(
        "--enqueue",
        action="store_true",
        help="Enqueue exit requests when the exit brain says to exit.",
    )
    p.add_argument(
        "--drain", action="store_true", help="Drain worker queue locally (process_one loop)."
    )
    p.add_argument(
        "--live",
        action="store_true",
        help="ALLOW live enqueue (danger). Without --live, enqueue is skipped (dry).",
    )
    p.add_argument(
        "--interval", type=float, default=10.0, help="Loop interval seconds (when --loop)."
    )
    p.add_argument(
        "--max-positions", type=int, default=0, help="Limit positions processed (0 = all)."
    )
    args = p.parse_args()
    if not args.loop:
        args.once = True
    return args


def main() -> None:
    args = _build_args()
    try:
        asyncio.run(main_async(args))
    except KeyboardInterrupt:
        log.event("SELL_ENGINE_STOP", reason="KeyboardInterrupt")


if __name__ == "__main__":
    main()
