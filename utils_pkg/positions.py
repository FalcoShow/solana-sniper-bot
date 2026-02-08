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
from typing import Any, Coroutine, Literal, Optional, TypedDict, TypeVar, cast

T = TypeVar("T")

# ──────────────────────────────────────────────────────────────────────────────
# Logging (safe, no side-effects)
# ──────────────────────────────────────────────────────────────────────────────


def _get_logger() -> Any:
    """
    Prefer repo's compat logger; fallback to stdlib logging.
    Must not crash at import time.
    """
    # 1) preferred compat layer (already used elsewhere in repo)
    with contextlib.suppress(Exception):
        from utils_pkg.log_compat import get_logger as gl  # type: ignore

        return gl(__name__)

    # 2) internal logging_setup module (package-qualified)
    with contextlib.suppress(Exception):
        from utils_pkg.logging_setup import get_logger as gl2  # type: ignore

        return gl2(__name__)

    # 3) last resort: stdlib logger
    import logging

    return logging.getLogger(__name__)


log: Any = _get_logger()


def _log_event(name: str, **fields: Any) -> None:
    """
    Emit structured events if log.event exists, else log.info.
    Never raise.
    """
    try:
        evt = getattr(log, "event", None)
        if callable(evt):
            evt(name, **fields)
        else:
            log.info(name, extra=fields)
    except Exception:
        return


# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────


def _truthy(x: Any) -> bool:
    return str(x or "").strip().lower() in {"1", "true", "yes", "y", "on"}


def _safe_int(x: Any, default: int = 0) -> int:
    try:
        return int(float(x))
    except Exception:
        return int(default)


def _safe_float(x: Any, default: float = 0.0) -> float:
    try:
        v = float(x)
        # NaN guard
        if v != v:
            return float(default)
        return v
    except Exception:
        return float(default)


def _now_ts() -> int:
    return int(time.time())


def _get_config_attr(name: str) -> Any:
    """
    Lazy config import. Never raises.
    """
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


def _backend_mode() -> Literal["store", "pg"]:
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


def _qty_ui(qty_raw: int, decimals: int) -> float:
    try:
        d = int(decimals)
        if d < 0:
            d = 0
        if d > 18:
            d = 18
        return float(int(qty_raw)) / (10**d)
    except Exception:
        return 0.0


# ──────────────────────────────────────────────────────────────────────────────
# Legacy schema (best-effort) typing
# ──────────────────────────────────────────────────────────────────────────────


class LegacyPositionDict(TypedDict, total=False):
    wallet: str
    mint: str
    pool: str | None
    symbol: str | None
    qty_raw: int
    qty_ui: float
    decimals: int
    token_program: str | None
    owner_ata: str | None
    avg_cost_sol: float
    opened_at: float
    last_sig: str
    status: Any
    updated_ts: int
    meta: dict[str, Any]


def _pos_to_legacy_dict(p: Any) -> LegacyPositionDict:
    """
    Convert PositionStore.Position -> legacy dict schema (best effort).
    Never raises.
    """
    meta: dict[str, Any] = {}
    try:
        raw_meta = getattr(p, "meta", None)
        if isinstance(raw_meta, dict):
            meta = raw_meta
        elif raw_meta:
            # if it's some mapping-like, try dict()
            with contextlib.suppress(Exception):
                meta = dict(raw_meta)
    except Exception:
        meta = {}

    decimals = _safe_int(meta.get("decimals"), 0)
    token_program = cast(Optional[str], meta.get("token_program"))
    owner_ata = cast(
        Optional[str], meta.get("owner_ata") or meta.get("ata") or meta.get("ownerAta")
    )

    status_obj = getattr(p, "status", "")
    status_val: Any = getattr(status_obj, "value", status_obj)

    return LegacyPositionDict(
        wallet=str(getattr(p, "wallet", "") or ""),
        mint=str(getattr(p, "mint", "") or ""),
        pool=cast(Optional[str], getattr(p, "pool", None)),
        symbol=cast(Optional[str], getattr(p, "symbol", None)),
        qty_raw=_safe_int(getattr(p, "amount_raw", 0), 0),
        qty_ui=_safe_float(getattr(p, "amount_ui", 0.0), 0.0),
        decimals=int(decimals),
        token_program=token_program,
        owner_ata=owner_ata,
        # legacy arg was SOL per token; we map to entry_price in same unit
        avg_cost_sol=_safe_float(getattr(p, "entry_price", 0.0), 0.0),
        opened_at=_safe_float(getattr(p, "opened_ts", 0), 0.0),
        last_sig=str(getattr(p, "buy_sig", "") or meta.get("last_buy_sig") or ""),
        status=status_val,
        updated_ts=_safe_int(getattr(p, "updated_ts", 0), 0),
        meta=meta,
    )


def _set_meta(obj: Any, meta: dict[str, Any]) -> None:
    """
    Helper to apply meta without assuming .set_meta exists.
    """
    try:
        fn = getattr(obj, "set_meta", None)
        if callable(fn):
            fn(meta)
            return
        # fallback: assign attribute
        obj.meta = meta
    except Exception:
        return


# ──────────────────────────────────────────────────────────────────────────────
# PositionStore bridge (async)
# ──────────────────────────────────────────────────────────────────────────────


async def _store_list_open(wallet: str) -> list[Any]:
    from . import position_store as ps  # local import (avoids cycles)

    return await ps.list_open_positions(wallet)


async def _store_find_open_for_mint(wallet: str, mint: str, pool: str | None = None) -> Any | None:
    mm = str(mint or "").strip()
    if not mm:
        return None

    positions = await _store_list_open(wallet)
    cand = [p for p in positions if str(getattr(p, "mint", "") or "") == mm]

    if pool:
        pool_s = str(pool).strip()
        filtered = [p for p in cand if str(getattr(p, "pool", "") or "") == pool_s]
        if filtered:
            cand = filtered

    if not cand:
        return None

    cand.sort(
        key=lambda x: int(getattr(x, "opened_ts", 0) or getattr(x, "created_ts", 0) or 0),
        reverse=True,
    )
    return cand[0]


async def async_all(wallet: str | None = None) -> list[LegacyPositionDict]:
    """
    Async canonical: list open positions from PositionStore (legacy dict form).
    """
    w = str(wallet or _default_wallet()).strip()
    try:
        ps = await _store_list_open(w)
        return [_pos_to_legacy_dict(p) for p in ps]
    except Exception as e:
        with contextlib.suppress(Exception):
            log.warning("POSITIONS_ASYNC_ALL_FAIL", extra={"err": str(e)[:200]})
        return []


async def async_get(mint: str, wallet: str | None = None) -> LegacyPositionDict | None:
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
        with contextlib.suppress(Exception):
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
    Async canonical mapping of legacy record_buy -> PositionStore.

    Supports:
    - first buy: NEW -> BOUGHT
    - additional buys (DCA) while BOUGHT/MANAGED: upsert aggregate qty + weighted avg entry_price

    Notes:
    - entry_price is SOL per token
    - size_sol accumulates estimated SOL cost (px * qty_ui)
    """
    from . import position_store as ps  # local import

    w = str(wallet or _default_wallet()).strip()
    m = str(mint or "").strip()
    if not m:
        return

    p = str(pool or "unknown_pool").strip()

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
        "owner_ata": (owner_ata or None),
        "last_buy_sig": (sig or None),
        "buy_px_sol_per_token": px,
        "ts": _now_ts(),
    }

    # Ensure existence (idempotent). Do not fail hard.
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

    # First buy attempt: NEW->BOUGHT
    try:
        cur_status = getattr(getattr(cur, "status", None), "value", getattr(cur, "status", "NEW"))
    except Exception:
        cur_status = "NEW"

    if cur is None or str(cur_status) == "NEW":
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

    if cur is None:
        return

    # Aggregate add-on for existing open position
    try:
        old_raw = _safe_int(getattr(cur, "amount_raw", 0), 0)
        old_ui = _safe_float(getattr(cur, "amount_ui", 0.0), 0.0)
        old_px = _safe_float(getattr(cur, "entry_price", 0.0), 0.0)
        old_sol = _safe_float(getattr(cur, "size_sol", 0.0), 0.0)

        new_raw = old_raw + q_raw
        new_ui = old_ui + q_ui

        if new_ui > 0.0 and old_ui > 0.0 and q_ui > 0.0:
            new_px = ((old_px * old_ui) + (px * q_ui)) / new_ui
        else:
            new_px = px or old_px

        # merge meta
        merged: dict[str, Any] = {}
        with contextlib.suppress(Exception):
            raw_meta = getattr(cur, "meta", None)
            if isinstance(raw_meta, dict):
                merged = dict(raw_meta)
        merged.update(meta_patch)

        # update fields
        with contextlib.suppress(Exception):
            cur.amount_raw = new_raw
            cur.amount_ui = new_ui
            cur.entry_price = new_px
            cur.size_sol = old_sol + est_sol_cost
            if symbol:
                cur.symbol = symbol
            _set_meta(cur, merged)

        store = await ps.get_store()
        await store.upsert(cur, ttl_sec=None)
    except Exception as e:
        with contextlib.suppress(Exception):
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
    Async canonical mapping of legacy update_after_sell -> PositionStore.
    - If closed=True or remaining <= 0: mark_sold(...)
    - Else: decrement qty and upsert (status unchanged)
    """
    from . import position_store as ps  # local import

    w = str(wallet or _default_wallet()).strip()
    m = str(mint or "").strip()
    if not m:
        return

    s_raw = _safe_int(sold_raw, 0)
    if s_raw <= 0:
        return

    pos = await _store_find_open_for_mint(w, m, pool=pool)
    if pos is None:
        return

    p = str(getattr(pos, "pool", "unknown_pool") or "unknown_pool")

    try:
        old_raw = _safe_int(getattr(pos, "amount_raw", 0), 0)
        old_ui = _safe_float(getattr(pos, "amount_ui", 0.0), 0.0)

        meta: dict[str, Any] = {}
        with contextlib.suppress(Exception):
            raw_meta = getattr(pos, "meta", None)
            if isinstance(raw_meta, dict):
                meta = dict(raw_meta)

        dec = max(0, min(18, _safe_int(meta.get("decimals"), 0)))
        sold_ui = _qty_ui(s_raw, dec)

        new_raw = max(0, old_raw - s_raw)
        new_ui = max(0.0, old_ui - sold_ui)

        meta["last_sell_sig"] = tx_sig or None
        meta["last_sell_raw"] = s_raw
        meta["last_sell_ui"] = sold_ui
        meta["ts"] = _now_ts()

        if closed or new_raw <= 0:
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

        # partial: decrement & upsert without status change
        with contextlib.suppress(Exception):
            pos.amount_raw = new_raw
            pos.amount_ui = new_ui
            if tx_sig:
                pos.sell_sig = tx_sig
            _set_meta(pos, meta)

        store = await ps.get_store()
        await store.upsert(pos, ttl_sec=None)
    except Exception as e:
        with contextlib.suppress(Exception):
            log.warning(
                "POSITIONS_ASYNC_UPDATE_AFTER_SELL_FAIL", extra={"err": str(e)[:200], "mint": m}
            )
        return


# ──────────────────────────────────────────────────────────────────────────────
# PG fallback (sync, legacy)
# ──────────────────────────────────────────────────────────────────────────────


def _import_pg() -> Any:
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


_IN_LOOP = object()


def _run_sync(coro: Coroutine[Any, Any, T]) -> T | object:
    """
    Run an async coroutine from sync legacy calls.
    - If no running loop: asyncio.run (safe)
    - If running loop: return sentinel so callers can fallback safely
    """
    if _in_running_loop():
        return _IN_LOOP
    return asyncio.run(coro)


# ──────────────────────────────────────────────────────────────────────────────
# Legacy API (sync) — do not use in new code
# ──────────────────────────────────────────────────────────────────────────────


def all() -> list[LegacyPositionDict]:
    """
    Legacy sync: list open positions.

    Behavior:
    - If backend=store and we are NOT inside an event loop: returns PositionStore snapshot.
    - If inside an event loop: falls back to PG snapshot (if enabled), else empty list.
    """
    mode = _backend_mode()

    if mode == "store":
        res = _run_sync(async_all())
        if res is not _IN_LOOP:
            return cast(list[LegacyPositionDict], res)
        # running loop → fallback
        if _pg_fallback_enabled():
            try:
                return list(_import_pg().all())  # type: ignore[misc]
            except Exception:
                return []
        return []

    # mode == "pg"
    try:
        return list(_import_pg().all())  # type: ignore[misc]
    except Exception:
        return []


def get(mint: str) -> LegacyPositionDict | None:
    """
    Legacy sync: get newest open position for mint.
    """
    m = str(mint or "").strip()
    if not m:
        return None

    mode = _backend_mode()
    if mode == "store":
        res = _run_sync(async_get(m))
        if res is not _IN_LOOP:
            return cast(Optional[LegacyPositionDict], res)
        if _pg_fallback_enabled():
            try:
                return cast(Optional[LegacyPositionDict], _import_pg().get(m))  # type: ignore[misc]
            except Exception:
                return None
        return None

    try:
        return cast(Optional[LegacyPositionDict], _import_pg().get(m))  # type: ignore[misc]
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

        async def _c() -> None:
            from . import position_store as ps  # local import

            w = _default_wallet()
            opens = await _store_list_open(w)
            matches = [p for p in opens if str(getattr(p, "mint", "") or "") == m]
            for p in matches:
                try:
                    await ps.mark_failed(
                        wallet=w,
                        mint=m,
                        pool=str(getattr(p, "pool", "unknown_pool") or "unknown_pool"),
                        fail_reason="LEGACY_REMOVE",
                        meta={"removed": True, "ts": _now_ts()},
                        ttl_closed_sec=60 * 60 * 24,
                    )
                except Exception:
                    continue

        coro = _c()
        res = _run_sync(coro)
        if res is _IN_LOOP:
            # best-effort: schedule task
            try:
                asyncio.get_running_loop().create_task(coro)
            except Exception:
                pass
        return

    # pg mode
    try:
        _import_pg().remove(m)  # type: ignore[misc]
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
            mint=m,
            sold_raw=int(sold_raw or 0),
            tx_sig=tx_sig,
            closed=bool(closed),
        )
        res = _run_sync(coro)
        if res is _IN_LOOP:
            try:
                asyncio.get_running_loop().create_task(coro)
            except Exception:
                pass
        return

    try:
        _import_pg().update_after_sell(
            mint=m,
            sold_raw=int(sold_raw or 0),
            tx_sig=tx_sig,
            closed=bool(closed),
        )  # type: ignore[misc]
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
            owner_ata=(owner_ata or ""),
            symbol=symbol,
            pool=pool,
            sig=sig,
        )
        res = _run_sync(coro)
        if res is _IN_LOOP:
            try:
                asyncio.get_running_loop().create_task(coro)
            except Exception:
                pass
        return

    try:
        _import_pg().record_buy(
            mint=m,
            qty_raw=int(qty_raw or 0),
            price_per_token_sol=float(price_per_token_sol or 0.0),
            decimals=int(decimals or 0),
            token_program=token_program,
            owner_ata=(owner_ata or ""),
            symbol=symbol,
            pool=pool,
            sig=sig,
        )  # type: ignore[misc]
    except Exception:
        return


__all__ = [
    # legacy sync API (do not use in new code)
    "all",
    "get",
    "remove",
    "record_buy",
    "record_sell",
    "decrement_qty",
    "update_after_sell",
    # new async helpers for migration
    "async_all",
    "async_get",
    "async_record_buy",
    "async_update_after_sell",
]
