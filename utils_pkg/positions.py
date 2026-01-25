# utils_pkg/positions.py
from __future__ import annotations

"""
utils_pkg/positions.py

LEGACY POSITIONS COMPAT LAYER (P0-safe, P2 migration target)
===========================================================

LOCK-IN (single source of truth)
--------------------------------
- Canonical truth for open positions is: utils_pkg.position_store (Redis-first).
- Postgres mirror (utils_pkg.positions_pg) is **analytics/reporting/recovery only**.

Why this file exists
--------------------
You still have older call-sites that import:
    from utils_pkg import positions
    positions.record_buy(...)
    positions.update_after_sell(...)
    positions.all() / positions.get(...)

Those legacy functions are **sync** and mint-centric.
Your new system is **async** and keyed by (wallet, mint, pool).

This module:
- Provides the legacy API without forking business logic.
- Uses PositionStore whenever it can (best effort).
- Falls back to Postgres mirror ONLY when:
    - we are inside a running event loop and a sync read is requested (cannot block),
    - or Redis/store is unavailable,
    - or store operations fail.

Rules
-----
- New code MUST call utils_pkg.position_store directly (async).
- Legacy code may keep using this file until it is fully migrated.
- This file must stay minimal, deterministic, and safe to import.

Env knobs (optional)
--------------------
- POSITIONS_BACKEND: "store" (default) or "pg"
- POSITIONS_PG_FALLBACK: "1" (default) enables safe fallback to positions_pg
"""

import asyncio
import contextlib
import os
import time
from typing import Any

# safe logger
try:  # pragma: no cover
    from logging_setup import get_logger  # type: ignore
except Exception:  # pragma: no cover
    import logging

    def get_logger(name: str | None = None):  # type: ignore
        return logging.getLogger(name or "positions")


log = get_logger(__name__)


# ──────────────────────────────────────────────────────────────────────────────
# small helpers
# ──────────────────────────────────────────────────────────────────────────────
def _truthy(x: Any) -> bool:
    return str(x or "").strip().lower() in {"1", "true", "yes", "y", "on"}


def _safe_int(x: Any, default: int = 0) -> int:
    try:
        return int(float(x))
    except Exception:
        return default


def _safe_float(x: Any, default: float = 0.0) -> float:
    try:
        v = float(x)
        if v != v:  # NaN
            return default
        return v
    except Exception:
        return default


def _now_ts() -> int:
    return int(time.time())


def _get_config_attr(name: str) -> Any:
    try:
        import config as C  # type: ignore

        return getattr(C, name)
    except Exception:
        return None


def _first_nonempty(*vals: Any) -> str:
    for v in vals:
        s = str(v or "").strip()
        if s:
            return s
    return ""


def _default_wallet() -> str:
    cfg = _get_config_attr
    return _first_nonempty(
        os.getenv("POSITIONS_DEFAULT_WALLET"),
        cfg("WALLET_ADDRESS"),
        cfg("WALLET_PUBKEY"),
        cfg("PUBLIC_KEY"),
        os.getenv("WALLET_ADDRESS"),
        os.getenv("WALLET_PUBKEY"),
        os.getenv("PUBLIC_KEY"),
        "unknown_wallet",
    )


def _backend_mode() -> str:
    cfg = _get_config_attr
    mode = _first_nonempty(
        os.getenv("POSITIONS_BACKEND"),
        cfg("POSITIONS_BACKEND"),
        "store",
    ).lower()
    return "pg" if mode in {"pg", "postgres", "postgresql"} else "store"


def _pg_fallback_enabled() -> bool:
    cfg = _get_config_attr
    return _truthy(
        _first_nonempty(os.getenv("POSITIONS_PG_FALLBACK"), cfg("POSITIONS_PG_FALLBACK"), "1")
    )


def _log_event(name: str, **fields: Any) -> None:
    try:
        if hasattr(log, "event") and callable(log.event):
            log.event(name, **fields)  # type: ignore
        else:
            log.info(name, extra=fields)
    except Exception:
        return


def _qty_ui(qty_raw: int, decimals: int) -> float:
    try:
        d = int(decimals)
        if d < 0:
            d = 0
        if d > 18:
            d = 18
        return float(qty_raw) / (10**d)
    except Exception:
        return 0.0


# ──────────────────────────────────────────────────────────────────────────────
# PositionStore bridge (async)
# ──────────────────────────────────────────────────────────────────────────────
async def _store_list_open(wallet: str) -> list[Any]:
    from . import position_store as ps  # local import (avoids cycles)

    return await ps.list_open_positions(wallet)


async def _store_find_open_for_mint(wallet: str, mint: str, pool: str | None = None) -> Any | None:
    mint = str(mint or "").strip()
    if not mint:
        return None
    ps = await _store_list_open(wallet)
    cand = [p for p in ps if getattr(p, "mint", "") == mint]
    if pool:
        pool_s = str(pool).strip()
        cand2 = [p for p in cand if getattr(p, "pool", "") == pool_s]
        if cand2:
            cand = cand2
    if not cand:
        return None
    # newest first
    cand.sort(
        key=lambda x: int(getattr(x, "opened_ts", 0) or getattr(x, "created_ts", 0) or 0),
        reverse=True,
    )
    return cand[0]


def _pos_to_legacy_dict(p: Any) -> dict[str, Any]:
    """
    Convert PositionStore.Position -> legacy dict schema (best effort).
    """
    meta = {}
    try:
        meta = (
            p.meta
            if isinstance(getattr(p, "meta", None), dict)
            else (getattr(p, "meta", None) or {})
        )
    except Exception:
        meta = {}

    decimals = _safe_int(meta.get("decimals"), 0)
    token_program = meta.get("token_program")
    owner_ata = meta.get("owner_ata") or meta.get("ata") or meta.get("ownerAta")

    # Keep naming consistent with older positions_pg/sqlite style:
    return {
        "wallet": getattr(p, "wallet", ""),
        "mint": getattr(p, "mint", ""),
        "pool": getattr(p, "pool", None),
        "symbol": getattr(p, "symbol", None),
        "qty_raw": _safe_int(getattr(p, "amount_raw", 0), 0),
        "qty_ui": _safe_float(getattr(p, "amount_ui", 0.0), 0.0),
        "decimals": decimals,
        "token_program": token_program,
        "owner_ata": owner_ata,
        # old schema used avg_cost_sol; we map to entry_price when price unit is sol_per_token
        "avg_cost_sol": _safe_float(getattr(p, "entry_price", 0.0), 0.0),
        "opened_at": _safe_float(getattr(p, "opened_ts", 0), 0.0),
        "last_sig": getattr(p, "buy_sig", "") or meta.get("last_buy_sig") or "",
        "status": getattr(getattr(p, "status", None), "value", getattr(p, "status", "")),
        "updated_ts": _safe_int(getattr(p, "updated_ts", 0), 0),
        "meta": meta,
    }


async def async_all(wallet: str | None = None) -> list[dict[str, Any]]:
    """
    Async canonical: list open positions from PositionStore (legacy dict form).
    """
    w = str(wallet or _default_wallet()).strip()
    try:
        ps = await _store_list_open(w)
        return [_pos_to_legacy_dict(p) for p in ps]
    except Exception as e:
        log.warning("POSITIONS_ASYNC_ALL_FAIL", extra={"err": str(e)[:200]})
        return []


async def async_get(mint: str, wallet: str | None = None) -> dict[str, Any] | None:
    """
    Async canonical: get newest open position for mint from PositionStore (legacy dict form).
    """
    w = str(wallet or _default_wallet()).strip()
    m = str(mint or "").strip()
    if not m:
        return None
    try:
        p = await _store_find_open_for_mint(w, m, pool=None)
        return _pos_to_legacy_dict(p) if p else None
    except Exception as e:
        log.warning("POSITIONS_ASYNC_GET_FAIL", extra={"err": str(e)[:200]})
        return None


async def async_record_buy(
    *,
    mint: str,
    qty_raw: int,
    price_per_token_sol: float,
    decimals: int,
    token_program: str | None,
    owner_ata: str = "",
    symbol: str | None = None,
    pool: str | None = None,
    sig: str | None = None,
    wallet: str | None = None,
) -> None:
    """
    Async canonical best-effort mapping of legacy "record_buy" onto PositionStore.

    Supports:
    - first buy: NEW -> BOUGHT
    - additional buys (DCA) while BOUGHT/MANAGED: upsert aggregate qty + weighted avg entry_price

    Notes:
    - We treat entry_price as SOL per token (same as legacy args).
    - We accumulate size_sol as estimated SOL cost (price_per_token_sol * qty_ui).
    """
    from . import position_store as ps  # local import

    w = str(wallet or _default_wallet()).strip()
    m = str(mint or "").strip()
    p = str(pool or "unknown_pool").strip()
    if not m:
        return

    q_raw = _safe_int(qty_raw, 0)
    if q_raw <= 0:
        return

    dec = max(0, min(18, _safe_int(decimals, 0)))
    q_ui = _qty_ui(q_raw, dec)
    px = _safe_float(price_per_token_sol, 0.0)
    est_sol_cost = px * q_ui

    meta_patch: dict[str, Any] = {
        "price_unit": "sol_per_token",
        "decimals": dec,
        "token_program": token_program,
        "owner_ata": owner_ata or None,
        "last_buy_sig": sig or None,
        "buy_px_sol_per_token": px,
    }

    # ensure exists
    with contextlib.suppress(Exception):
        await ps.register_new(
            wallet=w,
            mint=m,
            pool=p,
            symbol=(symbol or ""),
            reason="LEGACY_RECORD_BUY",
            meta=meta_patch,
        )

    cur = await ps.get_position(w, m, p)

    # first buy: attempt transition NEW->BOUGHT
    if (
        cur is None
        or getattr(getattr(cur, "status", None), "value", getattr(cur, "status", "")) == "NEW"
    ):
        try:
            await ps.mark_bought(
                wallet=w,
                mint=m,
                pool=p,
                buy_sig=(sig or ""),
                amount_raw=q_raw,
                amount_ui=q_ui,
                size_sol=est_sol_cost,
                entry_price=px,
                symbol=(symbol or ""),
                meta=meta_patch,
                ttl_open_sec=None,
            )
            return
        except Exception:
            # fall through to aggregate upsert
            cur = await ps.get_position(w, m, p)

    # aggregate add-on for BOUGHT/MANAGED
    if cur is None:
        return

    try:
        old_raw = _safe_int(getattr(cur, "amount_raw", 0), 0)
        old_ui = _safe_float(getattr(cur, "amount_ui", 0.0), 0.0)
        old_px = _safe_float(getattr(cur, "entry_price", 0.0), 0.0)
        old_sol = _safe_float(getattr(cur, "size_sol", 0.0), 0.0)

        new_raw = old_raw + q_raw
        new_ui = old_ui + q_ui
        if new_ui > 0:
            new_px = (
                ((old_px * old_ui) + (px * q_ui)) / new_ui
                if (old_ui > 0 and q_ui > 0)
                else (px or old_px)
            )
        else:
            new_px = px or old_px

        # merge meta
        merged = {}
        try:
            merged = cur.meta if isinstance(getattr(cur, "meta", None), dict) else {}
        except Exception:
            merged = {}
        merged.update(meta_patch)

        # update in-place + upsert (no status change)
        cur.amount_raw = new_raw
        cur.amount_ui = new_ui
        cur.entry_price = new_px
        cur.size_sol = old_sol + est_sol_cost
        if symbol:
            cur.symbol = symbol
        cur.set_meta(merged)  # type: ignore

        await ps.get_store()  # ensures singleton exists
        s = await ps.get_store()
        await s.upsert(cur, ttl_sec=None)
    except Exception as e:
        log.warning("POSITIONS_ASYNC_RECORD_BUY_FAIL", extra={"err": str(e)[:200], "mint": m})
        return


async def async_update_after_sell(
    *,
    mint: str,
    sold_raw: int,
    tx_sig: str | None,
    closed: bool,
    wallet: str | None = None,
    pool: str | None = None,
) -> None:
    """
    Async canonical best-effort mapping of legacy "update_after_sell" onto PositionStore.
    - If closed=True or remaining <= 0: mark_sold(...)
    - Else: decrement aggregate qty and upsert (status unchanged)
    """
    from . import position_store as ps  # local import

    w = str(wallet or _default_wallet()).strip()
    m = str(mint or "").strip()
    if not m:
        return

    s_raw = _safe_int(sold_raw, 0)
    if s_raw <= 0:
        return

    # If pool provided, use it; else pick newest open position for mint.
    pos = await _store_find_open_for_mint(w, m, pool=pool)
    if pos is None:
        return

    p = getattr(pos, "pool", "unknown_pool")
    try:
        old_raw = _safe_int(getattr(pos, "amount_raw", 0), 0)
        old_ui = _safe_float(getattr(pos, "amount_ui", 0.0), 0.0)

        # derive ui decrement using stored decimals if present
        meta = {}
        try:
            meta = pos.meta if isinstance(getattr(pos, "meta", None), dict) else {}
        except Exception:
            meta = {}
        dec = max(0, min(18, _safe_int(meta.get("decimals"), 0)))
        sold_ui = _qty_ui(s_raw, dec)

        new_raw = max(0, old_raw - s_raw)
        new_ui = max(0.0, old_ui - sold_ui)

        # merge meta
        meta = dict(meta or {})
        meta["last_sell_sig"] = tx_sig or None
        meta["last_sell_raw"] = s_raw
        meta["last_sell_ui"] = sold_ui

        if closed or new_raw <= 0:
            # close it (pnl unknown here; exit_engine/ledger handles pnl truth)
            await ps.mark_sold(
                wallet=w,
                mint=m,
                pool=p,
                sell_sig=(tx_sig or ""),
                pnl_pct=0.0,
                pnl_usd=0.0,
                last_price=_safe_float(getattr(pos, "last_price", 0.0), 0.0),
                reason="LEGACY_UPDATE_AFTER_SELL",
                meta=meta,
                ttl_closed_sec=60 * 60 * 24,  # 24h
            )
            return

        # partial: decrement + upsert without status change
        pos.amount_raw = new_raw
        pos.amount_ui = new_ui
        pos.sell_sig = tx_sig or getattr(pos, "sell_sig", "")
        pos.set_meta(meta)  # type: ignore

        s = await ps.get_store()
        await s.upsert(pos, ttl_sec=None)
    except Exception as e:
        log.warning(
            "POSITIONS_ASYNC_UPDATE_AFTER_SELL_FAIL", extra={"err": str(e)[:200], "mint": m}
        )
        return


# ──────────────────────────────────────────────────────────────────────────────
# PG fallback (sync, legacy)
# ──────────────────────────────────────────────────────────────────────────────
def _import_pg():
    """
    Lazy import to avoid import-time DSN failures.
    """
    from . import positions_pg as pg  # type: ignore

    return pg


def _in_running_loop() -> bool:
    try:
        asyncio.get_running_loop()
        return True
    except RuntimeError:
        return False


def _run_sync(coro):
    """
    Run an async coroutine from sync legacy calls.
    - If no running loop: asyncio.run (safe)
    - If running loop: return sentinel ("__IN_LOOP__") so callers can fallback safely
    """
    if _in_running_loop():
        return "__IN_LOOP__"
    return asyncio.run(coro)


# ──────────────────────────────────────────────────────────────────────────────
# Legacy API (sync) — do not use in new code
# ──────────────────────────────────────────────────────────────────────────────
def all() -> list[dict[str, Any]]:
    """
    Legacy sync: list open positions.

    Behavior:
    - If backend=store and we are NOT inside an event loop: returns PositionStore snapshot.
    - If inside an event loop: falls back to PG snapshot (if enabled), else empty list.
    """
    mode = _backend_mode()

    if mode == "store":
        res = _run_sync(async_all())
        if res != "__IN_LOOP__" and isinstance(res, list):
            return res
        # running loop → fallback
        if _pg_fallback_enabled():
            try:
                return list(_import_pg().all())  # type: ignore
            except Exception:
                return []
        return []

    # mode == "pg"
    try:
        return list(_import_pg().all())  # type: ignore
    except Exception:
        return []


def get(mint: str) -> dict[str, Any] | None:
    """
    Legacy sync: get newest open position for mint.
    """
    m = str(mint or "").strip()
    if not m:
        return None

    mode = _backend_mode()
    if mode == "store":
        res = _run_sync(async_get(m))
        if res != "__IN_LOOP__" and isinstance(res, dict):
            return res
        if _pg_fallback_enabled():
            try:
                return _import_pg().get(m)  # type: ignore
            except Exception:
                return None
        return None

    try:
        return _import_pg().get(m)  # type: ignore
    except Exception:
        return None


def remove(mint: str) -> None:
    """
    Legacy sync: remove/close all open positions for this mint (best effort).

    On store backend:
    - marks matching open positions as FAILED (does not resurrect)
    """
    m = str(mint or "").strip()
    if not m:
        return

    mode = _backend_mode()
    if mode == "store":

        async def _c():
            from . import position_store as ps  # local import

            w = _default_wallet()
            opens = await _store_list_open(w)
            matches = [p for p in opens if getattr(p, "mint", "") == m]
            for p in matches:
                try:
                    await ps.mark_failed(
                        wallet=w,
                        mint=m,
                        pool=getattr(p, "pool", "unknown_pool"),
                        fail_reason="LEGACY_REMOVE",
                        meta={"removed": True},
                        ttl_closed_sec=60 * 60 * 24,
                    )
                except Exception:
                    continue

        res = _run_sync(_c())
        if res == "__IN_LOOP__":
            # best effort: schedule (side effect ok)
            try:
                asyncio.get_running_loop().create_task(_c())  # type: ignore
            except Exception:
                pass
        return

    # pg mode
    try:
        _import_pg().remove(m)  # type: ignore
    except Exception:
        return


def decrement_qty(mint: str, sold_raw: int) -> None:
    """
    Legacy sync: decrement qty for a mint (partial sell bookkeeping).
    """
    update_after_sell(mint=mint, sold_raw=sold_raw, tx_sig=None, closed=False)


def update_after_sell(*, mint: str, sold_raw: int, tx_sig: str | None, closed: bool) -> None:
    """
    Legacy sync: record sell, decrement qty, optionally close.

    On store backend, if called in a running loop:
    - schedules an async task and returns immediately (side effect only).
    """
    m = str(mint or "").strip()
    if not m:
        return

    mode = _backend_mode()
    if mode == "store":
        coro = async_update_after_sell(
            mint=m, sold_raw=int(sold_raw or 0), tx_sig=tx_sig, closed=bool(closed)
        )
        res = _run_sync(coro)
        if res == "__IN_LOOP__":
            try:
                asyncio.get_running_loop().create_task(coro)  # type: ignore
            except Exception:
                pass
        return

    try:
        _import_pg().update_after_sell(
            mint=m, sold_raw=int(sold_raw or 0), tx_sig=tx_sig, closed=bool(closed)
        )  # type: ignore
    except Exception:
        return


def record_sell(*, mint: str, qty_raw: int, sig: str | None) -> None:
    """
    Legacy sync: convenience wrapper for partial sell.
    """
    update_after_sell(mint=mint, sold_raw=int(qty_raw or 0), tx_sig=sig, closed=False)


def record_buy(
    *,
    mint: str,
    qty_raw: int,
    price_per_token_sol: float,
    decimals: int,
    token_program: str | None,
    owner_ata: str = "",
    symbol: str | None = None,
    pool: str | None = None,
    sig: str | None = None,
) -> None:
    """
    Legacy sync: record a buy.

    On store backend, if called in a running loop:
    - schedules an async task and returns immediately (side effect only).
    """
    m = str(mint or "").strip()
    if not m:
        return

    mode = _backend_mode()
    if mode == "store":
        coro = async_record_buy(
            mint=m,
            qty_raw=int(qty_raw or 0),
            price_per_token_sol=float(price_per_token_sol or 0.0),
            decimals=int(decimals or 0),
            token_program=token_program,
            owner_ata=owner_ata or "",
            symbol=symbol,
            pool=pool,
            sig=sig,
        )
        res = _run_sync(coro)
        if res == "__IN_LOOP__":
            try:
                asyncio.get_running_loop().create_task(coro)  # type: ignore
            except Exception:
                pass
        return

    try:
        _import_pg().record_buy(  # type: ignore
            mint=m,
            qty_raw=int(qty_raw or 0),
            price_per_token_sol=float(price_per_token_sol or 0.0),
            decimals=int(decimals or 0),
            token_program=token_program,
            owner_ata=owner_ata or "",
            symbol=symbol,
            pool=pool,
            sig=sig,
        )
    except Exception:
        return


__all__ = [
    # legacy sync API (do not use in new code)
    "all",
    # new async helpers for migration
    "async_all",
    "async_get",
    "async_record_buy",
    "async_update_after_sell",
    "decrement_qty",
    "get",
    "record_buy",
    "record_sell",
    "remove",
    "update_after_sell",
]
