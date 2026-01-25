# utils_pkg/position_store.py
from __future__ import annotations

"""
utils_pkg/position_store.py

God-Tier Position Store (V3+++ / production-grade)
==================================================

Single source of truth for open positions.

Design goals (LOCK-IN)
----------------------
- Canonical, deterministic position state machine with *bounded* transitions.
- Concurrency-safe within a process (per-position asyncio locks) + best-effort
  optimistic concurrency when Redis supports WATCH/MULTI/EXEC.
- Redis-first for hot state; optional Postgres mirror hooks (best-effort, never
  required for correctness).
- Works with:
    - redis.asyncio client
    - “thin” Redis wrappers (e.g., Upstash REST wrappers) that may lack pipeline/sets

Keying
------
pos:{wallet}:{mint}:{pool}

Storage
-------
- Position record: Redis HASH at pos key
- Indices (best-effort):
    Preferred:
      - ZSET for open positions by timestamp (fast list newest-first)
      - SET for status membership
    Fallback:
      - SET for open/all/status
    Last resort:
      - JSON arrays stored as Redis STRINGs with TTL

This module owns NO global resources:
- core_utils owns redis lifecycle (get_redis_async)
- this module is safe to import anywhere

Public API
----------
- register_new(...)
- mark_bought(...)
- mark_managed(...)
- mark_sold(...)
- mark_failed(...)
- touch_price(...)
- touch_pnl(...)
- get(...)
- get_by_key(...)
- list_open(...)
- list_by_status(...)
- count_open(...)
"""

import asyncio
import contextlib
import json
import time
from collections.abc import Sequence
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

# -----------------------------------------------------------------------------
# Logger (safe fallback)
# -----------------------------------------------------------------------------
try:  # pragma: no cover
    from logging_setup import get_logger  # type: ignore
except Exception:  # pragma: no cover
    import logging

    def get_logger(name: str | None = None):  # type: ignore
        return logging.getLogger(name or "position_store")


log = get_logger(__name__)

# -----------------------------------------------------------------------------
# Status machine
# -----------------------------------------------------------------------------


class PositionStatus(str, Enum):
    NEW = "NEW"
    BOUGHT = "BOUGHT"
    MANAGED = "MANAGED"
    SOLD = "SOLD"
    FAILED = "FAILED"


_ALLOWED_TRANSITIONS: dict[PositionStatus, tuple[PositionStatus, ...]] = {
    PositionStatus.NEW: (PositionStatus.BOUGHT, PositionStatus.FAILED),
    PositionStatus.BOUGHT: (PositionStatus.MANAGED, PositionStatus.SOLD, PositionStatus.FAILED),
    PositionStatus.MANAGED: (PositionStatus.SOLD, PositionStatus.FAILED),
    PositionStatus.SOLD: (),
    PositionStatus.FAILED: (),
}


def _now_ts() -> int:
    return int(time.time())


def _safe_str(x: Any, max_len: int = 512) -> str:
    try:
        s = str(x)
    except Exception:
        s = ""
    s = s.strip()
    if len(s) > max_len:
        s = s[:max_len]
    return s


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


def _json_dumps(obj: Any) -> str:
    try:
        return json.dumps(obj, separators=(",", ":"), ensure_ascii=False)
    except Exception:
        return "{}"


def _json_loads(raw: Any) -> Any:
    try:
        if raw is None:
            return {}
        if isinstance(raw, bytes | bytearray):
            raw = raw.decode("utf-8", "ignore")
        txt = str(raw).strip()
        if not txt:
            return {}
        return json.loads(txt)
    except Exception:
        return {}


def _parse_status(raw: Any) -> PositionStatus:
    s = _safe_str(raw, 32).upper()
    # accept either enum names or values
    if s in PositionStatus.__members__:
        return PositionStatus[s]
    try:
        return PositionStatus(s)
    except Exception:
        return PositionStatus.NEW


# -----------------------------------------------------------------------------
# Position model
# -----------------------------------------------------------------------------


@dataclass
class Position:
    wallet: str
    mint: str
    pool: str

    status: PositionStatus = PositionStatus.NEW
    symbol: str = ""

    created_ts: int = field(default_factory=_now_ts)
    opened_ts: int = 0
    updated_ts: int = field(default_factory=_now_ts)
    closed_ts: int = 0

    # sizing / fills
    amount_raw: int = 0
    amount_ui: float = 0.0
    size_sol: float = 0.0

    # prices
    entry_price: float = 0.0
    last_price: float = 0.0

    # pnl
    pnl_usd: float = 0.0
    pnl_pct: float = 0.0

    # tx signatures
    buy_sig: str = ""
    sell_sig: str = ""

    # explainability
    reason: str = ""
    fail_reason: str = ""

    # free-form metadata (stored as JSON string for hash simplicity)
    meta_json: str = "{}"

    # ---- helpers ----
    def key(self) -> str:
        return PositionStore.make_key(self.wallet, self.mint, self.pool)

    @property
    def meta(self) -> dict[str, Any]:
        v = _json_loads(self.meta_json)
        return v if isinstance(v, dict) else {}

    def set_meta(self, meta: dict[str, Any]) -> None:
        self.meta_json = _json_dumps(meta if isinstance(meta, dict) else {})

    def merge_meta(self, patch: dict[str, Any] | None) -> None:
        if not patch:
            return
        m = self.meta
        for k, v in patch.items():
            m[str(k)] = v
        self.set_meta(m)

    def to_hash(self) -> dict[str, str]:
        # Store as string-only mapping for Redis hash compatibility
        return {
            "wallet": _safe_str(self.wallet, 96),
            "mint": _safe_str(self.mint, 96),
            "pool": _safe_str(self.pool, 96),
            "status": _safe_str(self.status.value, 16),
            "symbol": _safe_str(self.symbol, 64),
            "created_ts": str(_safe_int(self.created_ts)),
            "opened_ts": str(_safe_int(self.opened_ts)),
            "updated_ts": str(_safe_int(self.updated_ts)),
            "closed_ts": str(_safe_int(self.closed_ts)),
            "amount_raw": str(_safe_int(self.amount_raw)),
            "amount_ui": str(_safe_float(self.amount_ui)),
            "size_sol": str(_safe_float(self.size_sol)),
            "entry_price": str(_safe_float(self.entry_price)),
            "last_price": str(_safe_float(self.last_price)),
            "pnl_usd": str(_safe_float(self.pnl_usd)),
            "pnl_pct": str(_safe_float(self.pnl_pct)),
            "buy_sig": _safe_str(self.buy_sig, 180),
            "sell_sig": _safe_str(self.sell_sig, 180),
            "reason": _safe_str(self.reason, 512),
            "fail_reason": _safe_str(self.fail_reason, 512),
            "meta_json": _safe_str(self.meta_json, 50_000),
        }

    @staticmethod
    def from_hash(h: dict[str, Any]) -> Position:
        def g(k: str, default: Any = "") -> Any:
            v = h.get(k, default)
            if isinstance(v, bytes | bytearray):
                v = v.decode("utf-8", "ignore")
            return v

        # Accept legacy aliases (sym/liq/etc don't matter; we only parse position keys)
        p = Position(
            wallet=_safe_str(g("wallet", "")),
            mint=_safe_str(g("mint", "")),
            pool=_safe_str(g("pool", "")),
        )
        p.status = _parse_status(g("status", PositionStatus.NEW.value))
        p.symbol = _safe_str(g("symbol", ""), 64)

        p.created_ts = _safe_int(g("created_ts", _now_ts()))
        p.opened_ts = _safe_int(g("opened_ts", 0))
        p.updated_ts = _safe_int(g("updated_ts", _now_ts()))
        p.closed_ts = _safe_int(g("closed_ts", 0))

        p.amount_raw = _safe_int(g("amount_raw", 0))
        p.amount_ui = _safe_float(g("amount_ui", 0.0))
        p.size_sol = _safe_float(g("size_sol", 0.0))

        p.entry_price = _safe_float(g("entry_price", 0.0))
        p.last_price = _safe_float(g("last_price", 0.0))

        p.pnl_usd = _safe_float(g("pnl_usd", 0.0))
        p.pnl_pct = _safe_float(g("pnl_pct", 0.0))

        p.buy_sig = _safe_str(g("buy_sig", ""), 180)
        p.sell_sig = _safe_str(g("sell_sig", ""), 180)

        p.reason = _safe_str(g("reason", ""), 512)
        p.fail_reason = _safe_str(g("fail_reason", ""), 512)

        p.meta_json = _safe_str(g("meta_json", "{}"), 50_000)
        return p


# -----------------------------------------------------------------------------
# PositionStore
# -----------------------------------------------------------------------------


class PositionStore:
    """
    Canonical positions API.

    NOTE: This file is the *source of truth* for the rest of the system.
    Future files (positions_backend/positions_pg/positions.py) must wrap/adapt
    to this API — not fork the logic.
    """

    # in-process per-key locks (prevents double-writes inside one worker)
    _locks: dict[str, asyncio.Lock] = {}
    _locks_guard = asyncio.Lock()

    # ---------------------------
    # Key + index naming
    # ---------------------------
    @staticmethod
    def make_key(wallet: str, mint: str, pool: str) -> str:
        w = _safe_str(wallet, 96) or "unknown_wallet"
        m = _safe_str(mint, 96) or "unknown_mint"
        p = _safe_str(pool, 96) or "unknown_pool"
        return f"pos:{w}:{m}:{p}"

    @staticmethod
    def _idx_all_set() -> str:
        return "pos:set:all"

    @staticmethod
    def _idx_open_set(wallet: str) -> str:
        return f"pos:set:open:{_safe_str(wallet, 96) or 'unknown_wallet'}"

    @staticmethod
    def _idx_status_set(status: PositionStatus) -> str:
        return f"pos:set:status:{status.value}"

    @staticmethod
    def _idx_open_zset(wallet: str) -> str:
        return f"pos:zset:open:{_safe_str(wallet, 96) or 'unknown_wallet'}"

    @staticmethod
    def _idx_all_json() -> str:
        return "pos:idx:all"

    @staticmethod
    def _idx_open_json(wallet: str) -> str:
        return f"pos:idx:open:{_safe_str(wallet, 96) or 'unknown_wallet'}"

    @staticmethod
    def _idx_status_json(status: PositionStatus) -> str:
        return f"pos:idx:status:{status.value}"

    # ---------------------------
    # Redis capabilities
    # ---------------------------
    @staticmethod
    def _has(r: Any, *methods: str) -> bool:
        return all(hasattr(r, m) for m in methods)

    @staticmethod
    def _supports_pipeline(r: Any) -> bool:
        return PositionStore._has(r, "pipeline")

    @staticmethod
    def _supports_sets(r: Any) -> bool:
        return PositionStore._has(r, "sadd", "srem", "smembers")

    @staticmethod
    def _supports_zsets(r: Any) -> bool:
        # zadd + zrevrange preferred for ordered open listing
        return PositionStore._has(r, "zadd", "zrem", "zrevrange")

    # ---------------------------
    # Redis accessor
    # ---------------------------
    @staticmethod
    async def _redis() -> Any:
        from . import core_utils as cu  # local import (avoid import cycles)

        return await cu.get_redis_async()

    # ---------------------------
    # Locking
    # ---------------------------
    @staticmethod
    async def _get_lock(key: str) -> asyncio.Lock:
        async with PositionStore._locks_guard:
            lk = PositionStore._locks.get(key)
            if lk is None:
                lk = asyncio.Lock()
                PositionStore._locks[key] = lk
            return lk

    # -----------------------------------------------------------------------------
    # Index helpers (best-effort; never critical)
    # -----------------------------------------------------------------------------
    async def _idx_add(
        self, r: Any, key: str, wallet: str, status: PositionStatus, opened_ts: int
    ) -> None:
        try:
            if r is None:
                return

            # Preferred open index = zset (newest-first retrieval)
            if self._supports_zsets(r):
                # open positions only tracked in open index (BOUGHT/MANAGED)
                if status in (PositionStatus.BOUGHT, PositionStatus.MANAGED):
                    await r.zadd(self._idx_open_zset(wallet), {key: float(opened_ts or _now_ts())})
                else:
                    await r.zrem(self._idx_open_zset(wallet), key)

            # Membership sets for "all" and "status" (optional)
            if self._supports_sets(r):
                await r.sadd(self._idx_all_set(), key)
                await r.sadd(self._idx_status_set(status), key)
                # open set as fallback index (for wrappers without zset)
                if status in (PositionStatus.BOUGHT, PositionStatus.MANAGED):
                    await r.sadd(self._idx_open_set(wallet), key)
                else:
                    await r.srem(self._idx_open_set(wallet), key)
                return

            # JSON fallback
            await self._idx_json_add(r, self._idx_all_json(), key)
            await self._idx_json_add(r, self._idx_status_json(status), key)
            if status in (PositionStatus.BOUGHT, PositionStatus.MANAGED):
                await self._idx_json_add(r, self._idx_open_json(wallet), key)
            else:
                await self._idx_json_remove(r, self._idx_open_json(wallet), key)
        except Exception:
            return

    async def _idx_remove(self, r: Any, key: str, wallet: str, status: PositionStatus) -> None:
        try:
            if r is None:
                return

            if self._supports_zsets(r):
                await r.zrem(self._idx_open_zset(wallet), key)

            if self._supports_sets(r):
                await r.srem(self._idx_all_set(), key)
                await r.srem(self._idx_status_set(status), key)
                await r.srem(self._idx_open_set(wallet), key)
                return

            await self._idx_json_remove(r, self._idx_all_json(), key)
            await self._idx_json_remove(r, self._idx_status_json(status), key)
            await self._idx_json_remove(r, self._idx_open_json(wallet), key)
        except Exception:
            return

    async def _idx_json_add(
        self, r: Any, idx_key: str, pos_key: str, *, max_len: int = 8000
    ) -> None:
        try:
            raw = await r.get(idx_key)
            arr = _json_loads(raw)
            if not isinstance(arr, list):
                arr = []
            s = {str(x) for x in arr if x}
            s.add(pos_key)
            out = list(s)
            if len(out) > max_len:
                out = out[-max_len:]
            # keep indices around for some time; refresh on writes
            if hasattr(r, "set"):
                await r.set(idx_key, _json_dumps(out), ex=60 * 60 * 24 * 7)
        except Exception:
            return

    async def _idx_json_remove(self, r: Any, idx_key: str, pos_key: str) -> None:
        try:
            raw = await r.get(idx_key)
            arr = _json_loads(raw)
            if not isinstance(arr, list):
                return
            out = [x for x in arr if str(x) != pos_key]
            if hasattr(r, "set"):
                await r.set(idx_key, _json_dumps(out), ex=60 * 60 * 24 * 7)
        except Exception:
            return

    async def _idx_list_open_keys(self, r: Any, wallet: str, *, limit: int = 500) -> list[str]:
        if r is None:
            return []

        # Preferred: zset newest-first
        if self._supports_zsets(r):
            try:
                keys = await r.zrevrange(self._idx_open_zset(wallet), 0, max(0, int(limit) - 1))
                out: list[str] = []
                for k in keys or []:
                    if isinstance(k, bytes | bytearray):
                        k = k.decode("utf-8", "ignore")
                    out.append(str(k))
                return out
            except Exception:
                pass

        # Fallback: open set
        if self._supports_sets(r):
            try:
                keys = await r.smembers(self._idx_open_set(wallet))
                out = []
                for k in keys or []:
                    if isinstance(k, bytes | bytearray):
                        k = k.decode("utf-8", "ignore")
                    out.append(str(k))
                return out[:limit]
            except Exception:
                pass

        # Last resort: JSON list
        try:
            raw = await r.get(self._idx_open_json(wallet))
            arr = _json_loads(raw)
            if isinstance(arr, list):
                return [str(x) for x in arr if x][:limit]
        except Exception:
            pass
        return []

    # -----------------------------------------------------------------------------
    # Basic read/write
    # -----------------------------------------------------------------------------
    async def get(self, wallet: str, mint: str, pool: str) -> Position | None:
        return await self.get_by_key(self.make_key(wallet, mint, pool))

    async def get_by_key(self, key: str) -> Position | None:
        r = await self._redis()
        if r is None:
            return None
        try:
            h = await r.hgetall(key)
            if not h:
                return None
            return Position.from_hash(h)
        except Exception:
            return None

    async def upsert(self, pos: Position, *, ttl_sec: int | None = None) -> bool:
        r = await self._redis()
        if r is None:
            return False
        key = pos.key()
        pos.updated_ts = _now_ts()
        try:
            await r.hset(key, mapping=pos.to_hash())
            if ttl_sec and ttl_sec > 0 and hasattr(r, "expire"):
                await r.expire(key, int(ttl_sec))
            await self._idx_add(r, key, pos.wallet, pos.status, pos.opened_ts or pos.created_ts)
            return True
        except Exception:
            return False

    # -----------------------------------------------------------------------------
    # State transitions (best-effort optimistic concurrency)
    # -----------------------------------------------------------------------------
    async def _transition(
        self,
        *,
        wallet: str,
        mint: str,
        pool: str,
        to_status: PositionStatus,
        expected: Sequence[PositionStatus] | None,
        updates: dict[str, Any],
        ttl_sec: int | None,
    ) -> tuple[bool, Position | None]:
        r = await self._redis()
        if r is None:
            return (False, None)

        key = self.make_key(wallet, mint, pool)
        lk = await self._get_lock(key)

        async with lk:
            # 1) Try WATCH/MULTI if available
            if self._supports_pipeline(r):
                try:
                    from redis.exceptions import WatchError  # type: ignore
                except Exception:  # pragma: no cover
                    WatchError = Exception  # type: ignore

                for _ in range(6):
                    pipe = None
                    try:
                        pipe = r.pipeline(transaction=True)
                        # watch current key
                        await pipe.watch(key)
                        h = await pipe.hgetall(key)
                        cur = (
                            Position.from_hash(h)
                            if h
                            else Position(wallet=wallet, mint=mint, pool=pool)
                        )

                        cur_status = cur.status
                        if expected is not None and cur_status not in expected:
                            with contextlib.suppress(Exception):
                                await pipe.unwatch()
                            return (False, cur)

                        # enforce allowed transitions
                        if to_status not in _ALLOWED_TRANSITIONS.get(cur_status, ()):
                            with contextlib.suppress(Exception):
                                await pipe.unwatch()
                            # keep record, but reject transition
                            cur.fail_reason = (
                                cur.fail_reason
                                or f"illegal_transition:{cur_status.value}->{to_status.value}"
                            )
                            return (False, cur)

                        # apply updates
                        prev_status = cur.status
                        self._apply_updates(cur, updates)

                        # set new status + timestamps
                        cur.status = to_status
                        cur.updated_ts = _now_ts()
                        if to_status == PositionStatus.BOUGHT and cur.opened_ts <= 0:
                            cur.opened_ts = cur.updated_ts
                        if to_status in (PositionStatus.SOLD, PositionStatus.FAILED):
                            cur.closed_ts = cur.closed_ts or cur.updated_ts

                        await pipe.multi()
                        pipe.hset(key, mapping=cur.to_hash())
                        if ttl_sec and ttl_sec > 0 and hasattr(pipe, "expire"):
                            pipe.expire(key, int(ttl_sec))
                        await pipe.execute()

                        # index updates outside txn (best-effort)
                        with contextlib.suppress(Exception):
                            await self._idx_remove(r, key, wallet, prev_status)
                        with contextlib.suppress(Exception):
                            await self._idx_add(
                                r, key, wallet, to_status, cur.opened_ts or cur.created_ts
                            )

                        return (True, cur)
                    except WatchError:
                        continue
                    except Exception as e:
                        try:
                            if pipe is not None:
                                await pipe.reset()
                        except Exception:
                            pass
                        log.warning(
                            "POS_TRANSITION_PIPE_FAIL", extra={"key": key, "err": str(e)[:200]}
                        )
                        break

            # 2) Fallback: read + write (in-process lock ensures local serialization)
            try:
                h = await r.hgetall(key)
                cur = Position.from_hash(h) if h else Position(wallet=wallet, mint=mint, pool=pool)

                if expected is not None and cur.status not in expected:
                    return (False, cur)
                if to_status not in _ALLOWED_TRANSITIONS.get(cur.status, ()):
                    cur.fail_reason = (
                        cur.fail_reason
                        or f"illegal_transition:{cur.status.value}->{to_status.value}"
                    )
                    return (False, cur)

                prev_status = cur.status
                self._apply_updates(cur, updates)

                cur.status = to_status
                cur.updated_ts = _now_ts()
                if to_status == PositionStatus.BOUGHT and cur.opened_ts <= 0:
                    cur.opened_ts = cur.updated_ts
                if to_status in (PositionStatus.SOLD, PositionStatus.FAILED):
                    cur.closed_ts = cur.closed_ts or cur.updated_ts

                await r.hset(key, mapping=cur.to_hash())
                if ttl_sec and ttl_sec > 0 and hasattr(r, "expire"):
                    await r.expire(key, int(ttl_sec))

                with contextlib.suppress(Exception):
                    await self._idx_remove(r, key, wallet, prev_status)
                with contextlib.suppress(Exception):
                    await self._idx_add(r, key, wallet, to_status, cur.opened_ts or cur.created_ts)

                return (True, cur)
            except Exception:
                return (False, None)

    @staticmethod
    def _apply_updates(cur: Position, updates: dict[str, Any]) -> None:
        """
        Apply updates with light coercion and meta fallback for unknown fields.
        """
        if not updates:
            return
        for k, v in updates.items():
            if not hasattr(cur, k):
                # stash unknowns into meta
                m = cur.meta
                m[str(k)] = v
                cur.set_meta(m)
                continue

            if k.endswith("_ts") or k in ("amount_raw",):
                setattr(cur, k, _safe_int(v))
            elif k in ("amount_ui", "size_sol", "entry_price", "last_price", "pnl_usd", "pnl_pct"):
                setattr(cur, k, _safe_float(v))
            elif k in ("symbol", "reason", "fail_reason", "buy_sig", "sell_sig"):
                setattr(cur, k, _safe_str(v, 512 if k in ("reason", "fail_reason") else 180))
            elif k == "meta_json":
                # allow dict or json string
                if isinstance(v, dict):
                    cur.set_meta(v)
                else:
                    cur.meta_json = _safe_str(v, 50_000) or "{}"
            else:
                setattr(cur, k, v)

    # -----------------------------------------------------------------------------
    # Optional Postgres mirror hooks (best-effort)
    # -----------------------------------------------------------------------------
    @staticmethod
    async def _pg_upsert_best_effort(pos: Position) -> None:
        try:
            from . import core_utils as cu  # type: ignore

            fn = getattr(cu, "pg_upsert_position", None)
            if not callable(fn):
                return
            await fn(
                wallet=pos.wallet,
                mint=pos.mint,
                pool=pos.pool,
                symbol=pos.symbol or None,
                size_raw=float(pos.amount_raw) if pos.amount_raw else None,
                size_sol=float(pos.size_sol) if pos.size_sol else None,
                entry_price=float(pos.entry_price) if pos.entry_price else None,
                status="open"
                if pos.status in (PositionStatus.BOUGHT, PositionStatus.MANAGED)
                else "other",
            )
        except Exception:
            return

    @staticmethod
    async def _pg_close_best_effort(pos: Position, *, status: str) -> None:
        try:
            from . import core_utils as cu  # type: ignore

            fn = getattr(cu, "pg_close_position", None)
            if not callable(fn):
                return
            await fn(wallet=pos.wallet, mint=pos.mint, pool=pos.pool, status=status)
        except Exception:
            return

    # -----------------------------------------------------------------------------
    # Lifecycle API
    # -----------------------------------------------------------------------------
    async def register_new(
        self,
        *,
        wallet: str,
        mint: str,
        pool: str,
        symbol: str = "",
        reason: str = "CANDIDATE",
        meta: dict[str, Any] | None = None,
        ttl_sec: int | None = None,
    ) -> tuple[bool, Position | None]:
        """
        Idempotently ensure a position exists (NEW or already active).
        Never resurrect terminal positions.
        """
        key = self.make_key(wallet, mint, pool)
        lk = await self._get_lock(key)
        async with lk:
            cur = await self.get(wallet, mint, pool)
            if cur and cur.status in (PositionStatus.SOLD, PositionStatus.FAILED):
                return (False, cur)

            if cur is None:
                pos = Position(wallet=wallet, mint=mint, pool=pool, status=PositionStatus.NEW)
                pos.symbol = symbol or ""
                pos.reason = reason or "CANDIDATE"
                pos.set_meta(meta or {})
                ok = await self.upsert(pos, ttl_sec=ttl_sec)
                return (ok, pos)

            # update non-terminal record (keep current status)
            if symbol:
                cur.symbol = symbol
            if reason:
                cur.reason = reason
            cur.merge_meta(meta)
            ok = await self.upsert(cur, ttl_sec=ttl_sec)
            return (ok, cur)

    async def mark_bought(
        self,
        *,
        wallet: str,
        mint: str,
        pool: str,
        buy_sig: str,
        amount_raw: int = 0,
        amount_ui: float = 0.0,
        size_sol: float = 0.0,
        entry_price: float = 0.0,
        symbol: str = "",
        meta: dict[str, Any] | None = None,
        ttl_open_sec: int | None = None,
    ) -> tuple[bool, Position | None]:
        """
        NEW -> BOUGHT
        """
        updates: dict[str, Any] = {
            "buy_sig": buy_sig,
            "amount_raw": amount_raw,
            "amount_ui": amount_ui,
            "size_sol": size_sol,
            "entry_price": entry_price,
        }
        if symbol:
            updates["symbol"] = symbol
        if meta:
            updates["meta_json"] = meta  # dict accepted
        ok, pos = await self._transition(
            wallet=wallet,
            mint=mint,
            pool=pool,
            to_status=PositionStatus.BOUGHT,
            expected=(PositionStatus.NEW,),
            updates=updates,
            ttl_sec=ttl_open_sec,
        )
        if ok and pos:
            await self._pg_upsert_best_effort(pos)
        return (ok, pos)

    async def mark_managed(
        self,
        *,
        wallet: str,
        mint: str,
        pool: str,
        meta_patch: dict[str, Any] | None = None,
        ttl_open_sec: int | None = None,
    ) -> tuple[bool, Position | None]:
        """
        BOUGHT -> MANAGED (and optionally patch metadata)
        """
        updates: dict[str, Any] = {}
        if meta_patch:
            cur = await self.get(wallet, mint, pool)
            if cur:
                cur.merge_meta(meta_patch)
                updates["meta_json"] = cur.meta  # dict accepted
            else:
                updates["meta_json"] = meta_patch

        ok, pos = await self._transition(
            wallet=wallet,
            mint=mint,
            pool=pool,
            to_status=PositionStatus.MANAGED,
            expected=(PositionStatus.BOUGHT,),
            updates=updates,
            ttl_sec=ttl_open_sec,
        )
        if ok and pos:
            await self._pg_upsert_best_effort(pos)
        return (ok, pos)

    async def mark_sold(
        self,
        *,
        wallet: str,
        mint: str,
        pool: str,
        sell_sig: str,
        pnl_pct: float = 0.0,
        pnl_usd: float = 0.0,
        last_price: float = 0.0,
        reason: str = "EXIT",
        meta: dict[str, Any] | None = None,
        ttl_closed_sec: int | None = None,
    ) -> tuple[bool, Position | None]:
        """
        BOUGHT/MANAGED -> SOLD
        """
        updates: dict[str, Any] = {
            "sell_sig": sell_sig,
            "pnl_pct": pnl_pct,
            "pnl_usd": pnl_usd,
            "last_price": last_price,
            "reason": reason,
        }
        if meta:
            updates["meta_json"] = meta

        ok, pos = await self._transition(
            wallet=wallet,
            mint=mint,
            pool=pool,
            to_status=PositionStatus.SOLD,
            expected=(PositionStatus.BOUGHT, PositionStatus.MANAGED),
            updates=updates,
            ttl_sec=ttl_closed_sec,
        )
        if ok and pos:
            await self._pg_close_best_effort(pos, status="closed")
        return (ok, pos)

    async def mark_failed(
        self,
        *,
        wallet: str,
        mint: str,
        pool: str,
        fail_reason: str,
        meta: dict[str, Any] | None = None,
        ttl_closed_sec: int | None = None,
    ) -> tuple[bool, Position | None]:
        """
        NEW/BOUGHT/MANAGED -> FAILED
        """
        updates: dict[str, Any] = {"fail_reason": fail_reason or "FAILED"}
        if meta:
            updates["meta_json"] = meta

        ok, pos = await self._transition(
            wallet=wallet,
            mint=mint,
            pool=pool,
            to_status=PositionStatus.FAILED,
            expected=(PositionStatus.NEW, PositionStatus.BOUGHT, PositionStatus.MANAGED),
            updates=updates,
            ttl_sec=ttl_closed_sec,
        )
        if ok and pos:
            await self._pg_close_best_effort(pos, status="failed")
        return (ok, pos)

    # -----------------------------------------------------------------------------
    # Non-status updates (fast path)
    # -----------------------------------------------------------------------------
    async def touch_price(
        self,
        *,
        wallet: str,
        mint: str,
        pool: str,
        last_price: float,
        ttl_open_sec: int | None = None,
    ) -> bool:
        """
        Update last_price on an open position without changing status.
        """
        pos = await self.get(wallet, mint, pool)
        if not pos or pos.status not in (PositionStatus.BOUGHT, PositionStatus.MANAGED):
            return False
        pos.last_price = float(last_price or 0.0)
        return await self.upsert(pos, ttl_sec=ttl_open_sec)

    async def touch_pnl(
        self,
        *,
        wallet: str,
        mint: str,
        pool: str,
        pnl_pct: float,
        pnl_usd: float = 0.0,
        ttl_open_sec: int | None = None,
    ) -> bool:
        """
        Update pnl fields on an open position without changing status.
        """
        pos = await self.get(wallet, mint, pool)
        if not pos or pos.status not in (PositionStatus.BOUGHT, PositionStatus.MANAGED):
            return False
        pos.pnl_pct = float(pnl_pct or 0.0)
        pos.pnl_usd = float(pnl_usd or 0.0)
        return await self.upsert(pos, ttl_sec=ttl_open_sec)

    # -----------------------------------------------------------------------------
    # Queries
    # -----------------------------------------------------------------------------
    async def list_open(self, wallet: str, *, limit: int = 200) -> list[Position]:
        r = await self._redis()
        if r is None:
            return []

        keys = await self._idx_list_open_keys(r, wallet, limit=limit)
        out: list[Position] = []

        for k in keys:
            p = await self.get_by_key(k)
            if not p:
                continue
            if p.status in (PositionStatus.BOUGHT, PositionStatus.MANAGED):
                out.append(p)
            else:
                # stale index cleanup (best-effort)
                with contextlib.suppress(Exception):
                    await self._idx_remove(r, k, wallet, p.status)

        # newest first
        out.sort(key=lambda x: int(x.opened_ts or x.created_ts or 0), reverse=True)
        return out[:limit]

    async def list_by_status(
        self,
        status: PositionStatus,
        *,
        wallet: str | None = None,
        limit: int = 500,
    ) -> list[Position]:
        """
        Best-effort status listing. If wallet is provided, we filter results to that wallet.
        Uses indices if available; otherwise returns empty (safe).
        """
        r = await self._redis()
        if r is None:
            return []

        keys: list[str] = []
        if self._supports_sets(r):
            try:
                raw = await r.smembers(self._idx_status_set(status))
                for k in raw or []:
                    if isinstance(k, bytes | bytearray):
                        k = k.decode("utf-8", "ignore")
                    keys.append(str(k))
            except Exception:
                keys = []
        else:
            try:
                raw = await r.get(self._idx_status_json(status))
                arr = _json_loads(raw)
                if isinstance(arr, list):
                    keys = [str(x) for x in arr if x]
            except Exception:
                keys = []

        out: list[Position] = []
        for k in keys[:limit]:
            p = await self.get_by_key(k)
            if not p:
                continue
            if wallet and p.wallet != wallet:
                continue
            if p.status == status:
                out.append(p)
        out.sort(key=lambda x: int(x.updated_ts or 0), reverse=True)
        return out

    async def count_open(self, wallet: str) -> int:
        return len(await self.list_open(wallet, limit=10_000))

    async def any_open_for_mint(self, wallet: str, mint: str) -> bool:
        return any(p.mint == mint for p in await self.list_open(wallet, limit=2000))


# -----------------------------------------------------------------------------
# Singleton + functional wrappers (drop-in convenience)
# -----------------------------------------------------------------------------
_STORE: PositionStore | None = None
_STORE_LOCK = asyncio.Lock()


async def get_store() -> PositionStore:
    global _STORE
    if _STORE is not None:
        return _STORE
    async with _STORE_LOCK:
        if _STORE is None:
            _STORE = PositionStore()
        return _STORE


async def register_new(**kwargs: Any) -> tuple[bool, Position | None]:
    s = await get_store()
    return await s.register_new(**kwargs)


async def mark_bought(**kwargs: Any) -> tuple[bool, Position | None]:
    s = await get_store()
    return await s.mark_bought(**kwargs)


async def mark_managed(**kwargs: Any) -> tuple[bool, Position | None]:
    s = await get_store()
    return await s.mark_managed(**kwargs)


async def mark_sold(**kwargs: Any) -> tuple[bool, Position | None]:
    s = await get_store()
    return await s.mark_sold(**kwargs)


async def mark_failed(**kwargs: Any) -> tuple[bool, Position | None]:
    s = await get_store()
    return await s.mark_failed(**kwargs)


async def touch_price(**kwargs: Any) -> bool:
    s = await get_store()
    return await s.touch_price(**kwargs)


async def touch_pnl(**kwargs: Any) -> bool:
    s = await get_store()
    return await s.touch_pnl(**kwargs)


async def get_position(wallet: str, mint: str, pool: str) -> Position | None:
    s = await get_store()
    return await s.get(wallet, mint, pool)


async def get_position_by_key(key: str) -> Position | None:
    s = await get_store()
    return await s.get_by_key(key)


async def list_open_positions(wallet: str, *, limit: int = 200) -> list[Position]:
    s = await get_store()
    return await s.list_open(wallet, limit=limit)


__all__ = [
    "Position",
    "PositionStatus",
    "PositionStore",
    "get_position",
    "get_position_by_key",
    "get_store",
    "list_open_positions",
    "mark_bought",
    "mark_failed",
    "mark_managed",
    "mark_sold",
    "register_new",
    "touch_pnl",
    "touch_price",
]
