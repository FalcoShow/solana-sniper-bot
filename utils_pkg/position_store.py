# utils_pkg/position_store.py
from __future__ import annotations

"""
utils_pkg/position_store.py

God-Tier Position Store (V3+++ / production-grade)
==================================================

Single source of truth for open positions (Redis + optional Postgres mirror).

Goals
-----
- Deterministic state machine:
    NEW  -> BOUGHT -> MANAGED -> SOLD
    NEW  -> FAILED
    BOUGHT/ MANAGED -> FAILED (rare: swap failed, force-close fail, etc.)

- Concurrency safe:
    - Atomic-ish transitions using Redis WATCH/MULTI/EXEC when available.
    - Best-effort fallback when Redis lacks pipelines (e.g., Upstash REST wrapper).

- Operationally safe:
    - No resource ownership here (core_utils owns redis lifecycle).
    - No side effects on import (lazy PG/utility calls).
    - Backwards-compatible helpers for "simple dict" callers.

Keying
------
Position key:
    pos:{wallet}:{mint}:{pool}

We maintain:
  - Hash at pos key (all fields)
  - Optional indices:
      * Redis Sets (native redis) OR JSON indices (fallback)
        - pos:idx:open:{wallet}
        - pos:idx:status:{STATUS}
        - pos:idx:all

If native sets are supported, we use:
  - pos:set:open:{wallet}
  - pos:set:status:{STATUS}
  - pos:set:all

This module never assumes scanners/sniper exist; they can call:
  - register_new(...)
  - mark_bought(...)
  - mark_managed(...)
  - mark_sold(...)
  - mark_failed(...)
  - get(...)
  - list_open(...)

"""

import asyncio
import json
import time
from dataclasses import dataclass, field, asdict
from enum import Enum
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

# local logger (safe fallback)
try:  # pragma: no cover
    from logging_setup import get_logger  # type: ignore
except Exception:  # pragma: no cover
    import logging

    def get_logger(name: str | None = None):  # type: ignore
        return logging.getLogger(name or "position_store")


log = get_logger(__name__)

# ──────────────────────────────────────────────────────────────────────────────
# Status machine
# ──────────────────────────────────────────────────────────────────────────────


class PositionStatus(str, Enum):
    NEW = "NEW"
    BOUGHT = "BOUGHT"
    MANAGED = "MANAGED"
    SOLD = "SOLD"
    FAILED = "FAILED"


_ALLOWED_TRANSITIONS: Dict[PositionStatus, Tuple[PositionStatus, ...]] = {
    PositionStatus.NEW: (PositionStatus.BOUGHT, PositionStatus.FAILED),
    PositionStatus.BOUGHT: (PositionStatus.MANAGED, PositionStatus.SOLD, PositionStatus.FAILED),
    PositionStatus.MANAGED: (PositionStatus.SOLD, PositionStatus.FAILED),
    PositionStatus.SOLD: (),
    PositionStatus.FAILED: (),
}


def _now_ts() -> int:
    return int(time.time())


def _clamp_float(x: Any, default: float = 0.0) -> float:
    try:
        v = float(x)
        if v != v:  # NaN
            return default
        return v
    except Exception:
        return default


def _clamp_int(x: Any, default: int = 0) -> int:
    try:
        return int(float(x))
    except Exception:
        return default


def _safe_str(x: Any, max_len: int = 512) -> str:
    try:
        s = str(x)
    except Exception:
        s = ""
    s = s.strip()
    if len(s) > max_len:
        s = s[:max_len]
    return s


def _json_dumps(obj: Any) -> str:
    try:
        return json.dumps(obj, separators=(",", ":"), ensure_ascii=False)
    except Exception:
        return "{}"


def _json_loads(s: Any) -> Any:
    try:
        if s is None:
            return {}
        if isinstance(s, (bytes, bytearray)):
            s = s.decode("utf-8", "ignore")
        txt = str(s).strip()
        if not txt:
            return {}
        return json.loads(txt)
    except Exception:
        return {}


# ──────────────────────────────────────────────────────────────────────────────
# Dataclass representation
# ──────────────────────────────────────────────────────────────────────────────


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

    # size / pricing
    amount_raw: int = 0  # out token raw (if known)
    amount_ui: float = 0.0  # token UI (if known)
    size_sol: float = 0.0  # SOL spent (entry) or SOL received (exit)
    entry_price: float = 0.0  # in USD if available, else "price units" used by your stack
    last_price: float = 0.0

    # PnL
    pnl_usd: float = 0.0
    pnl_pct: float = 0.0

    # tx sigs
    buy_sig: str = ""
    sell_sig: str = ""

    # reasons + metadata
    reason: str = ""
    fail_reason: str = ""
    meta_json: str = "{}"  # store as json string for redis hash simplicity

    def key(self) -> str:
        return PositionStore.make_key(self.wallet, self.mint, self.pool)

    @property
    def meta(self) -> Dict[str, Any]:
        v = _json_loads(self.meta_json)
        return v if isinstance(v, dict) else {}

    def set_meta(self, meta: Dict[str, Any]) -> None:
        self.meta_json = _json_dumps(meta if isinstance(meta, dict) else {})

    def to_hash(self) -> Dict[str, str]:
        # keep all values as strings (redis hash)
        return {
            "wallet": _safe_str(self.wallet, 96),
            "mint": _safe_str(self.mint, 96),
            "pool": _safe_str(self.pool, 96),
            "status": _safe_str(self.status.value, 16),
            "symbol": _safe_str(self.symbol, 64),
            "created_ts": str(_clamp_int(self.created_ts)),
            "opened_ts": str(_clamp_int(self.opened_ts)),
            "updated_ts": str(_clamp_int(self.updated_ts)),
            "closed_ts": str(_clamp_int(self.closed_ts)),
            "amount_raw": str(_clamp_int(self.amount_raw)),
            "amount_ui": str(_clamp_float(self.amount_ui)),
            "size_sol": str(_clamp_float(self.size_sol)),
            "entry_price": str(_clamp_float(self.entry_price)),
            "last_price": str(_clamp_float(self.last_price)),
            "pnl_usd": str(_clamp_float(self.pnl_usd)),
            "pnl_pct": str(_clamp_float(self.pnl_pct)),
            "buy_sig": _safe_str(self.buy_sig, 160),
            "sell_sig": _safe_str(self.sell_sig, 160),
            "reason": _safe_str(self.reason, 512),
            "fail_reason": _safe_str(self.fail_reason, 512),
            "meta_json": _safe_str(self.meta_json, 20000),
        }

    @staticmethod
    def from_hash(h: Dict[str, Any]) -> "Position":
        def g(k: str, default: Any = "") -> Any:
            v = h.get(k, default)
            if isinstance(v, (bytes, bytearray)):
                v = v.decode("utf-8", "ignore")
            return v

        p = Position(
            wallet=_safe_str(g("wallet", "")),
            mint=_safe_str(g("mint", "")),
            pool=_safe_str(g("pool", "")),
        )
        st = _safe_str(g("status", PositionStatus.NEW.value), 16).upper()
        p.status = PositionStatus(st) if st in PositionStatus.__members__.values() else PositionStatus.NEW  # type: ignore
        # robust enum parse:
        try:
            p.status = PositionStatus(st)
        except Exception:
            p.status = PositionStatus.NEW

        p.symbol = _safe_str(g("symbol", ""), 64)
        p.created_ts = _clamp_int(g("created_ts", _now_ts()))
        p.opened_ts = _clamp_int(g("opened_ts", 0))
        p.updated_ts = _clamp_int(g("updated_ts", _now_ts()))
        p.closed_ts = _clamp_int(g("closed_ts", 0))

        p.amount_raw = _clamp_int(g("amount_raw", 0))
        p.amount_ui = _clamp_float(g("amount_ui", 0.0))
        p.size_sol = _clamp_float(g("size_sol", 0.0))
        p.entry_price = _clamp_float(g("entry_price", 0.0))
        p.last_price = _clamp_float(g("last_price", 0.0))

        p.pnl_usd = _clamp_float(g("pnl_usd", 0.0))
        p.pnl_pct = _clamp_float(g("pnl_pct", 0.0))

        p.buy_sig = _safe_str(g("buy_sig", ""), 160)
        p.sell_sig = _safe_str(g("sell_sig", ""), 160)

        p.reason = _safe_str(g("reason", ""), 512)
        p.fail_reason = _safe_str(g("fail_reason", ""), 512)
        p.meta_json = _safe_str(g("meta_json", "{}"), 20000)
        return p


# ──────────────────────────────────────────────────────────────────────────────
# Store implementation
# ──────────────────────────────────────────────────────────────────────────────


class PositionStore:
    """
    Single-truth position store.

    Uses core_utils.get_redis_async() for redis client (native or Upstash REST wrapper).
    Optionally mirrors BOUGHT/SOLD to Postgres via core_utils.pg_upsert_position / pg_close_position.

    Public API (what other modules should use):
      - register_new(...)
      - mark_bought(...)
      - mark_managed(...)
      - mark_sold(...)
      - mark_failed(...)
      - get(...)
      - list_open(...)
      - count_open(...)
    """

    # in-process per-key locks (prevents double-write in a single worker process)
    _locks: Dict[str, asyncio.Lock] = {}
    _locks_guard = asyncio.Lock()

    @staticmethod
    def make_key(wallet: str, mint: str, pool: str) -> str:
        w = _safe_str(wallet, 96) or "unknown_wallet"
        m = _safe_str(mint, 96) or "unknown_mint"
        p = _safe_str(pool, 96) or "unknown_pool"
        return f"pos:{w}:{m}:{p}"

    @staticmethod
    def _idx_set_all() -> str:
        return "pos:set:all"

    @staticmethod
    def _idx_set_open(wallet: str) -> str:
        return f"pos:set:open:{_safe_str(wallet, 96) or 'unknown_wallet'}"

    @staticmethod
    def _idx_set_status(status: PositionStatus) -> str:
        return f"pos:set:status:{status.value}"

    @staticmethod
    def _idx_json_all() -> str:
        return "pos:idx:all"

    @staticmethod
    def _idx_json_open(wallet: str) -> str:
        return f"pos:idx:open:{_safe_str(wallet, 96) or 'unknown_wallet'}"

    @staticmethod
    def _idx_json_status(status: PositionStatus) -> str:
        return f"pos:idx:status:{status.value}"

    @staticmethod
    async def _get_lock(key: str) -> asyncio.Lock:
        async with PositionStore._locks_guard:
            lk = PositionStore._locks.get(key)
            if lk is None:
                lk = asyncio.Lock()
                PositionStore._locks[key] = lk
            return lk

    @staticmethod
    def _supports_sets(r: Any) -> bool:
        # redis.asyncio client has sadd/srem/smembers
        return all(hasattr(r, m) for m in ("sadd", "srem", "smembers"))

    @staticmethod
    def _supports_pipeline(r: Any) -> bool:
        return hasattr(r, "pipeline")

    @staticmethod
    async def _redis() -> Any:
        from . import core_utils as cu  # local import to avoid cycles

        return await cu.get_redis_async()

    # ──────────────────────────────────────────────────────────────────────────
    # Index management (sets preferred, JSON fallback)
    # ──────────────────────────────────────────────────────────────────────────
    async def _idx_add(self, r: Any, key: str, wallet: str, status: PositionStatus) -> None:
        try:
            if r is None:
                return
            if self._supports_sets(r):
                await r.sadd(self._idx_set_all(), key)
                await r.sadd(self._idx_set_open(wallet), key)
                await r.sadd(self._idx_set_status(status), key)
                return

            # JSON fallback (best-effort)
            await self._idx_json_add(r, self._idx_json_all(), key)
            await self._idx_json_add(r, self._idx_json_open(wallet), key)
            await self._idx_json_add(r, self._idx_json_status(status), key)
        except Exception:
            return

    async def _idx_remove(self, r: Any, key: str, wallet: str, status: PositionStatus) -> None:
        try:
            if r is None:
                return
            if self._supports_sets(r):
                await r.srem(self._idx_set_all(), key)
                await r.srem(self._idx_set_open(wallet), key)
                await r.srem(self._idx_set_status(status), key)
                return

            await self._idx_json_remove(r, self._idx_json_all(), key)
            await self._idx_json_remove(r, self._idx_json_open(wallet), key)
            await self._idx_json_remove(r, self._idx_json_status(status), key)
        except Exception:
            return

    async def _idx_json_add(self, r: Any, idx_key: str, pos_key: str, max_len: int = 5000) -> None:
        # store as JSON array for Upstash REST compatibility
        try:
            raw = await r.get(idx_key)
            arr = _json_loads(raw)
            if not isinstance(arr, list):
                arr = []
            s = set(str(x) for x in arr if x)
            s.add(pos_key)
            out = list(s)
            # bound length (keep most recent-ish keys)
            if len(out) > max_len:
                out = out[-max_len:]
            await r.set(idx_key, _json_dumps(out), ex=60 * 60 * 24 * 7)  # 7d ttl
        except Exception:
            return

    async def _idx_json_remove(self, r: Any, idx_key: str, pos_key: str) -> None:
        try:
            raw = await r.get(idx_key)
            arr = _json_loads(raw)
            if not isinstance(arr, list):
                return
            out = [x for x in arr if str(x) != pos_key]
            await r.set(idx_key, _json_dumps(out), ex=60 * 60 * 24 * 7)
        except Exception:
            return

    async def _idx_list_open_keys(self, r: Any, wallet: str) -> List[str]:
        if r is None:
            return []
        try:
            if self._supports_sets(r):
                keys = await r.smembers(self._idx_set_open(wallet))
                out = []
                for k in keys or []:
                    if isinstance(k, (bytes, bytearray)):
                        k = k.decode("utf-8", "ignore")
                    out.append(str(k))
                return out

            raw = await r.get(self._idx_json_open(wallet))
            arr = _json_loads(raw)
            if isinstance(arr, list):
                return [str(x) for x in arr if x]
        except Exception:
            pass
        return []

    # ──────────────────────────────────────────────────────────────────────────
    # Core helpers (read/write)
    # ──────────────────────────────────────────────────────────────────────────
    async def get(self, wallet: str, mint: str, pool: str) -> Optional[Position]:
        r = await self._redis()
        if r is None:
            return None
        key = self.make_key(wallet, mint, pool)
        try:
            h = await r.hgetall(key)
            if not h:
                return None
            return Position.from_hash(h)
        except Exception:
            return None

    async def upsert(self, pos: Position, *, ttl_sec: Optional[int] = None) -> bool:
        r = await self._redis()
        if r is None:
            return False

        key = pos.key()
        pos.updated_ts = _now_ts()

        try:
            await r.hset(key, mapping=pos.to_hash())
            if ttl_sec is not None and ttl_sec > 0 and hasattr(r, "expire"):
                await r.expire(key, int(ttl_sec))
            # index add
            await self._idx_add(r, key, pos.wallet, pos.status)
            return True
        except Exception:
            return False

    # ──────────────────────────────────────────────────────────────────────────
    # Atomic-ish transitions (WATCH/MULTI preferred)
    # ──────────────────────────────────────────────────────────────────────────
    async def _transition(
        self,
        *,
        wallet: str,
        mint: str,
        pool: str,
        to_status: PositionStatus,
        updates: Dict[str, Any],
        expected: Optional[Sequence[PositionStatus]] = None,
        ttl_sec: Optional[int] = None,
    ) -> Tuple[bool, Optional[Position]]:
        """
        Transition position state with best-effort atomicity.

        Returns: (ok, updated_position_or_none)
        """
        r = await self._redis()
        if r is None:
            return (False, None)

        key = self.make_key(wallet, mint, pool)
        lk = await self._get_lock(key)
        async with lk:
            # prefer redis transaction if supported
            if self._supports_pipeline(r):
                try:
                    from redis.exceptions import WatchError  # type: ignore
                except Exception:  # pragma: no cover
                    WatchError = Exception  # type: ignore

                for _ in range(6):
                    try:
                        pipe = r.pipeline(transaction=True)
                        await pipe.watch(key)
                        h = await pipe.hgetall(key)
                        if not h:
                            # create NEW if absent and transition expects NEW
                            cur = Position(wallet=wallet, mint=mint, pool=pool)
                        else:
                            cur = Position.from_hash(h)

                        cur_status = cur.status
                        if expected is not None and cur_status not in expected:
                            try:
                                await pipe.unwatch()
                            except Exception:
                                pass
                            return (False, cur)

                        # validate transition legality
                        if to_status not in _ALLOWED_TRANSITIONS.get(cur_status, ()):
                            try:
                                await pipe.unwatch()
                            except Exception:
                                pass
                            cur.fail_reason = cur.fail_reason or f"illegal_transition:{cur_status.value}->{to_status.value}"
                            return (False, cur)

                        prev_status = cur.status
                        # apply updates to dataclass (normalized)
                        for k, v in (updates or {}).items():
                            if not hasattr(cur, k):
                                # store unknowns into meta
                                m = cur.meta
                                m[str(k)] = v
                                cur.set_meta(m)
                                continue
                            # assign with light coercion
                            if k.endswith("_ts"):
                                setattr(cur, k, _clamp_int(v))
                            elif k in ("amount_raw",):
                                setattr(cur, k, _clamp_int(v))
                            elif k in ("amount_ui", "size_sol", "entry_price", "last_price", "pnl_usd", "pnl_pct"):
                                setattr(cur, k, _clamp_float(v))
                            else:
                                setattr(cur, k, v)

                        cur.status = to_status
                        cur.updated_ts = _now_ts()
                        if to_status == PositionStatus.BOUGHT and cur.opened_ts <= 0:
                            cur.opened_ts = cur.updated_ts
                        if to_status in (PositionStatus.SOLD, PositionStatus.FAILED):
                            cur.closed_ts = cur.closed_ts or cur.updated_ts

                        pipe.multi()
                        pipe.hset(key, mapping=cur.to_hash())
                        if ttl_sec is not None and ttl_sec > 0 and hasattr(pipe, "expire"):
                            pipe.expire(key, int(ttl_sec))

                        # index maintenance (best-effort) — remove old status, add new
                        # (do it outside transaction to avoid coupling; indices are "helpful", not critical)
                        await pipe.execute()

                        # post-commit index updates
                        try:
                            await self._idx_remove(r, key, wallet, prev_status)
                        except Exception:
                            pass
                        try:
                            await self._idx_add(r, key, wallet, to_status)
                        except Exception:
                            pass

                        return (True, cur)
                    except WatchError:
                        continue
                    except Exception as e:
                        try:
                            if "pipe" in locals():
                                await pipe.reset()
                        except Exception:
                            pass
                        log.warning("POS_TRANSITION_FAIL", extra={"err": str(e)[:180], "key": key})
                        break

            # fallback: non-transactional best-effort
            try:
                h = await r.hgetall(key)
                cur = Position.from_hash(h) if h else Position(wallet=wallet, mint=mint, pool=pool)
                cur_status = cur.status
                if expected is not None and cur_status not in expected:
                    return (False, cur)
                if to_status not in _ALLOWED_TRANSITIONS.get(cur_status, ()):
                    cur.fail_reason = cur.fail_reason or f"illegal_transition:{cur_status.value}->{to_status.value}"
                    return (False, cur)

                prev_status = cur.status
                for k, v in (updates or {}).items():
                    if hasattr(cur, k):
                        setattr(cur, k, v)
                    else:
                        m = cur.meta
                        m[str(k)] = v
                        cur.set_meta(m)

                cur.status = to_status
                cur.updated_ts = _now_ts()
                if to_status == PositionStatus.BOUGHT and cur.opened_ts <= 0:
                    cur.opened_ts = cur.updated_ts
                if to_status in (PositionStatus.SOLD, PositionStatus.FAILED):
                    cur.closed_ts = cur.closed_ts or cur.updated_ts

                await r.hset(key, mapping=cur.to_hash())
                if ttl_sec is not None and ttl_sec > 0 and hasattr(r, "expire"):
                    await r.expire(key, int(ttl_sec))
                try:
                    await self._idx_remove(r, key, wallet, prev_status)
                except Exception:
                    pass
                try:
                    await self._idx_add(r, key, wallet, to_status)
                except Exception:
                    pass

                return (True, cur)
            except Exception:
                return (False, None)

    # ──────────────────────────────────────────────────────────────────────────
    # High-level lifecycle API
    # ──────────────────────────────────────────────────────────────────────────
    async def register_new(
        self,
        *,
        wallet: str,
        mint: str,
        pool: str,
        symbol: str = "",
        reason: str = "CANDIDATE",
        meta: Optional[Dict[str, Any]] = None,
    ) -> Tuple[bool, Optional[Position]]:
        """
        Create/ensure a NEW position record (idempotent).
        """
        updates: Dict[str, Any] = {
            "wallet": wallet,
            "mint": mint,
            "pool": pool,
            "symbol": symbol or "",
            "reason": reason or "CANDIDATE",
        }
        if meta:
            updates["meta_json"] = _json_dumps(meta)
        # if record exists, keep status unless it's terminal; we only "ensure" it exists
        r = await self._redis()
        if r is None:
            return (False, None)

        key = self.make_key(wallet, mint, pool)
        lk = await self._get_lock(key)
        async with lk:
            cur = await self.get(wallet, mint, pool)
            if cur and cur.status in (PositionStatus.SOLD, PositionStatus.FAILED):
                # do not resurrect terminal positions
                return (False, cur)
            if cur is None:
                pos = Position(wallet=wallet, mint=mint, pool=pool, status=PositionStatus.NEW)
                pos.symbol = symbol or ""
                pos.reason = reason or "CANDIDATE"
                pos.set_meta(meta or {})
                ok = await self.upsert(pos)
                return (ok, pos)
            # update a few fields, keep status
            cur.symbol = symbol or cur.symbol
            cur.reason = reason or cur.reason
            if meta:
                m = cur.meta
                m.update(meta)
                cur.set_meta(m)
            ok = await self.upsert(cur)
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
        meta: Optional[Dict[str, Any]] = None,
        ttl_open_sec: Optional[int] = None,
    ) -> Tuple[bool, Optional[Position]]:
        """
        NEW -> BOUGHT
        Also mirrors to PG (best-effort) and caches entry price if core_utils hooks exist.
        """
        updates: Dict[str, Any] = {
            "buy_sig": buy_sig or "",
            "amount_raw": int(amount_raw or 0),
            "amount_ui": float(amount_ui or 0.0),
            "size_sol": float(size_sol or 0.0),
            "entry_price": float(entry_price or 0.0),
        }
        if symbol:
            updates["symbol"] = symbol
        if meta:
            updates["meta_json"] = _json_dumps(meta)

        ok, pos = await self._transition(
            wallet=wallet,
            mint=mint,
            pool=pool,
            to_status=PositionStatus.BOUGHT,
            expected=(PositionStatus.NEW,),
            updates=updates,
            ttl_sec=ttl_open_sec,
        )

        # best-effort PG mirror + entry price cache hooks
        if ok and pos is not None:
            try:
                from . import core_utils as cu

                # PG position mirror
                await cu.pg_upsert_position(
                    wallet=wallet,
                    mint=mint,
                    pool=pool,
                    symbol=pos.symbol or None,
                    size_raw=float(pos.amount_raw) if pos.amount_raw else None,
                    size_sol=float(pos.size_sol) if pos.size_sol else None,
                    entry_price=float(pos.entry_price) if pos.entry_price else None,
                    status="open",
                )
            except Exception:
                pass

        return (ok, pos)

    async def mark_managed(
        self,
        *,
        wallet: str,
        mint: str,
        pool: str,
        meta_patch: Optional[Dict[str, Any]] = None,
        ttl_open_sec: Optional[int] = None,
    ) -> Tuple[bool, Optional[Position]]:
        """
        BOUGHT -> MANAGED
        """
        updates: Dict[str, Any] = {}
        if meta_patch:
            # merge patch into meta_json
            cur = await self.get(wallet, mint, pool)
            if cur is not None:
                m = cur.meta
                m.update(meta_patch)
                updates["meta_json"] = _json_dumps(m)
            else:
                updates["meta_json"] = _json_dumps(meta_patch)

        return await self._transition(
            wallet=wallet,
            mint=mint,
            pool=pool,
            to_status=PositionStatus.MANAGED,
            expected=(PositionStatus.BOUGHT,),
            updates=updates,
            ttl_sec=ttl_open_sec,
        )

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
        meta: Optional[Dict[str, Any]] = None,
        ttl_closed_sec: Optional[int] = None,
    ) -> Tuple[bool, Optional[Position]]:
        """
        BOUGHT/MANAGED -> SOLD
        """
        updates: Dict[str, Any] = {
            "sell_sig": sell_sig or "",
            "pnl_pct": float(pnl_pct or 0.0),
            "pnl_usd": float(pnl_usd or 0.0),
            "last_price": float(last_price or 0.0),
            "reason": reason or "EXIT",
        }
        if meta:
            updates["meta_json"] = _json_dumps(meta)

        ok, pos = await self._transition(
            wallet=wallet,
            mint=mint,
            pool=pool,
            to_status=PositionStatus.SOLD,
            expected=(PositionStatus.BOUGHT, PositionStatus.MANAGED),
            updates=updates,
            ttl_sec=ttl_closed_sec,
        )

        # best-effort PG close
        if ok:
            try:
                from . import core_utils as cu

                await cu.pg_close_position(wallet=wallet, mint=mint, pool=pool, status="closed")
            except Exception:
                pass

        return (ok, pos)

    async def mark_failed(
        self,
        *,
        wallet: str,
        mint: str,
        pool: str,
        fail_reason: str,
        meta: Optional[Dict[str, Any]] = None,
        ttl_closed_sec: Optional[int] = None,
    ) -> Tuple[bool, Optional[Position]]:
        """
        NEW/BOUGHT/MANAGED -> FAILED
        """
        updates: Dict[str, Any] = {"fail_reason": fail_reason or "FAILED"}
        if meta:
            updates["meta_json"] = _json_dumps(meta)

        ok, pos = await self._transition(
            wallet=wallet,
            mint=mint,
            pool=pool,
            to_status=PositionStatus.FAILED,
            expected=(PositionStatus.NEW, PositionStatus.BOUGHT, PositionStatus.MANAGED),
            updates=updates,
            ttl_sec=ttl_closed_sec,
        )

        # PG close (failed -> closed)
        if ok:
            try:
                from . import core_utils as cu

                await cu.pg_close_position(wallet=wallet, mint=mint, pool=pool, status="failed")
            except Exception:
                pass

        return (ok, pos)

    # ──────────────────────────────────────────────────────────────────────────
    # Query helpers
    # ──────────────────────────────────────────────────────────────────────────
    async def list_open(self, wallet: str) -> List[Position]:
        r = await self._redis()
        if r is None:
            return []
        out: List[Position] = []
        keys = await self._idx_list_open_keys(r, wallet)

        for k in keys:
            try:
                h = await r.hgetall(k)
                if not h:
                    continue
                p = Position.from_hash(h)
                if p.status in (PositionStatus.BOUGHT, PositionStatus.MANAGED):
                    out.append(p)
                else:
                    # index cleanup (best-effort) if stale
                    try:
                        await self._idx_remove(r, k, wallet, p.status)
                    except Exception:
                        pass
            except Exception:
                continue

        # newest first
        out.sort(key=lambda x: int(x.opened_ts or x.created_ts or 0), reverse=True)
        return out

    async def count_open(self, wallet: str) -> int:
        ps = await self.list_open(wallet)
        return int(len(ps))

    async def any_open_for_mint(self, wallet: str, mint: str) -> bool:
        ps = await self.list_open(wallet)
        for p in ps:
            if p.mint == mint:
                return True
        return False


# ──────────────────────────────────────────────────────────────────────────────
# Singleton + functional wrappers (easy drop-in)
# ──────────────────────────────────────────────────────────────────────────────
_STORE: Optional[PositionStore] = None
_STORE_LOCK = asyncio.Lock()


async def get_store() -> PositionStore:
    global _STORE
    if _STORE is not None:
        return _STORE
    async with _STORE_LOCK:
        if _STORE is None:
            _STORE = PositionStore()
        return _STORE


# Convenience wrappers (callers can do: from position_store import register_new, mark_bought, ...)
async def register_new(**kwargs: Any) -> Tuple[bool, Optional[Position]]:
    s = await get_store()
    return await s.register_new(**kwargs)


async def mark_bought(**kwargs: Any) -> Tuple[bool, Optional[Position]]:
    s = await get_store()
    return await s.mark_bought(**kwargs)


async def mark_managed(**kwargs: Any) -> Tuple[bool, Optional[Position]]:
    s = await get_store()
    return await s.mark_managed(**kwargs)


async def mark_sold(**kwargs: Any) -> Tuple[bool, Optional[Position]]:
    s = await get_store()
    return await s.mark_sold(**kwargs)


async def mark_failed(**kwargs: Any) -> Tuple[bool, Optional[Position]]:
    s = await get_store()
    return await s.mark_failed(**kwargs)


async def get_position(wallet: str, mint: str, pool: str) -> Optional[Position]:
    s = await get_store()
    return await s.get(wallet, mint, pool)


async def list_open_positions(wallet: str) -> List[Position]:
    s = await get_store()
    return await s.list_open(wallet)


__all__ = [
    "PositionStatus",
    "Position",
    "PositionStore",
    "get_store",
    "register_new",
    "mark_bought",
    "mark_managed",
    "mark_sold",
    "mark_failed",
    "get_position",
    "list_open_positions",
]
