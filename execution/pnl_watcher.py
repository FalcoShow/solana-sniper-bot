from __future__ import annotations

"""
execution/pnl_watcher.py — P0 CANONICAL PnL WATCHER (NO TRADING SIDE EFFECTS)
============================================================================

P0 LOCK-IN:
- PnL watcher is OBSERVABILITY ONLY.
- It must NOT run exit decisions or execute sells.
- Exits are owned by utils_pkg/auto_exit_worker/exit_engine.py + execution/exits_wiring.py.
- PnL watcher computes equity/uPnL from:
    (1) wallet balance (SOL -> USD),
    (2) open positions from ONE position source of truth (utils_pkg.position_store),
    (3) optional DB/ledger logging hooks (best-effort).

Compatibility:
- If utils_pkg.position_store is not present yet, an opt-in legacy Redis fallback
  can be enabled (PNL_ALLOW_LEGACY_REDIS=1) to keep you alive during transition.
  Default is OFF to enforce the “one source of truth” contract.

Design goals:
- No side effects on import (safe-mode friendly).
- Drift-tolerant adapters (redis client shapes / function signatures can vary).
- Typed, mypy-clean, production-safe.
"""

import asyncio
import contextlib
import json
import math
import os
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Coroutine, Protocol

from logging_setup import get_logger

log = get_logger(__name__)

EPS = 1e-9


# ──────────────────────────────────────────────────────────────────────────────
# Env / Config
# (Keep env key names stable; contract tests often scan these.)
# ──────────────────────────────────────────────────────────────────────────────
def _env_int(name: str, default: int) -> int:
    v = os.getenv(name)
    if v is None or v == "":
        return int(default)
    try:
        return int(v)
    except Exception:
        return int(default)


def _env_float(name: str, default: float) -> float:
    v = os.getenv(name)
    if v is None or v == "":
        return float(default)
    try:
        return float(v)
    except Exception:
        return float(default)


def _env_bool(name: str, default: bool) -> bool:
    v = (os.getenv(name) or "").strip().lower()
    if v in ("1", "true", "yes", "y", "on"):
        return True
    if v in ("0", "false", "no", "n", "off"):
        return False
    return default


PNL_INTERVAL_SEC = _env_int("PNL_INTERVAL_SEC", _env_int("PNL_SCAN_SEC", 5))
PNL_HEARTBEAT_EVERY_N = _env_int("PNL_HEARTBEAT_EVERY_N", _env_int("PNL_HEARTBEAT_N", 6))

PNL_PRICE_CACHE_TTL_SEC = _env_int("PNL_PRICE_CACHE_TTL_SEC", 20)
PNL_MAX_PARALLEL_PRICES = _env_int("PNL_MAX_PARALLEL_PRICES", 12)

PNL_TELEGRAM_HEARTBEAT = _env_bool("PNL_TELEGRAM_HEARTBEAT", False)
PNL_TELEGRAM_ONLY_ON_CHANGE = _env_bool("PNL_TELEGRAM_ONLY_ON_CHANGE", False)
PNL_TELEGRAM_DELTA_USD = _env_float("PNL_TELEGRAM_DELTA_USD", 100.0)
PNL_TELEGRAM_DELTA_PCT = _env_float("PNL_TELEGRAM_DELTA_PCT", 2.0)
PNL_TELEGRAM_FORCE_EVERY_N = _env_int("PNL_TELEGRAM_FORCE_EVERY_N", 0)

WALLET_PUBKEY = os.getenv("WALLET_PUBKEY") or os.getenv("WALLET_PUBLIC_KEY") or ""
EQUITY_USD_FALLBACK = _env_float("EQUITY_USD_FALLBACK", 0.0)

# If wallet calls glitch to 0 occasionally, use smoothing
PNL_WALLET_MIN_VALID_USD = _env_float("PNL_WALLET_MIN_VALID_USD", 5.0)
PNL_ZERO_CONFIRM_STREAK = _env_int("PNL_ZERO_CONFIRM_STREAK", 3)

# Strict P0: default OFF. Only enable during transition.
PNL_ALLOW_LEGACY_REDIS = _env_bool("PNL_ALLOW_LEGACY_REDIS", False)

# Optional legacy keys if fallback enabled
LEGACY_REDIS_INDEX_KEY = os.getenv("LEGACY_POSITIONS_INDEX_KEY", "positions:index")
LEGACY_REDIS_POS_PREFIX = os.getenv("LEGACY_POSITIONS_PREFIX", "pos:")

# Redis keys for watcher state
REDIS_KEY_LAST_SNAPSHOT = os.getenv("PNL_REDIS_LAST_SNAPSHOT_KEY", "pnl:last_snapshot")
REDIS_KEY_BASE_EQ = os.getenv("PNL_REDIS_BASE_EQUITY_KEY", "pnl:baseline:equity")
REDIS_KEY_BASE_UPNL = os.getenv("PNL_REDIS_BASE_UPNL_KEY", "pnl:baseline:upnl")
REDIS_KEY_BASE_SINCE = os.getenv("PNL_REDIS_BASE_SINCE_KEY", "pnl:baseline:since_n")
REDIS_BASE_TTL_SEC = _env_int("PNL_BASELINE_TTL_SEC", 48 * 3600)


# ──────────────────────────────────────────────────────────────────────────────
# Small helpers
# ──────────────────────────────────────────────────────────────────────────────
def _now_ts() -> float:
    return time.time()


def _iso_utc() -> str:
    return datetime.now(UTC).isoformat()


def _finite(x: Any) -> bool:
    try:
        fx = float(x)
        return math.isfinite(fx)
    except Exception:
        return False


def _pct(a: float, b: float) -> float:
    if abs(b) <= EPS:
        return 0.0
    return (a / b - 1.0) * 100.0


def _json_dumps(obj: Any) -> str:
    # stable + safe (tags/extra may contain non-json primitives)
    return json.dumps(obj, separators=(",", ":"), ensure_ascii=False, default=str)


async def _await_maybe(x: Any) -> Any:
    if asyncio.iscoroutine(x):
        return await x
    return x


def _spawn_best_effort(coro: Coroutine[Any, Any, Any]) -> bool:
    """
    Fire-and-forget without raising if no running loop.
    """
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        return False
    try:
        task: asyncio.Task[Any] = loop.create_task(coro)
        _ = task
        return True
    except Exception:
        return False


# ──────────────────────────────────────────────────────────────────────────────
# Lazy optional integrations
# ──────────────────────────────────────────────────────────────────────────────
class _CoreAPI(Protocol):
    async def get_token_price(self, mint: str) -> float: ...
    async def get_sol_balance(self, pubkey: str) -> float: ...
    async def convert_sol_to_usd(self, sol: float) -> float: ...
    async def send_telegram(self, msg: str) -> None: ...

    async def get_redis_async(self) -> Any: ...
    def get_redis(self) -> Any: ...


_core_cached: Any = None


async def _core() -> Any:
    """
    Lazy import utils_pkg.core_utils only when needed.
    Returns an object with async helpers or stubs.
    """
    global _core_cached
    if _core_cached is not None:
        return _core_cached

    # Default stubs (safe-mode / offline)
    class _Stub:
        async def get_token_price(self, _mint: str) -> float:
            return 0.0

        async def get_sol_balance(self, _pubkey: str) -> float:
            return 0.0

        async def convert_sol_to_usd(self, _sol: float) -> float:
            return 0.0

        async def send_telegram(self, _msg: str) -> None:
            return None

        async def get_redis_async(self) -> Any:
            return None

        def get_redis(self) -> Any:
            return None

    try:
        from utils_pkg import core_utils as cu  # type: ignore
    except Exception:
        _core_cached = _Stub()
        return _core_cached

    # Wrap to normalize optional return shapes.
    class _Wrapped:
        async def get_token_price(self, mint: str) -> float:
            fn = getattr(cu, "get_token_price", None)
            if not callable(fn):
                return 0.0
            try:
                v = await _await_maybe(fn(mint))
                return float(v or 0.0)
            except Exception:
                return 0.0

        async def get_sol_balance(self, pubkey: str) -> float:
            fn = getattr(cu, "get_sol_balance", None)
            if not callable(fn):
                return 0.0
            try:
                v = await _await_maybe(fn(pubkey))
                return float(v or 0.0)
            except Exception:
                return 0.0

        async def convert_sol_to_usd(self, sol: float) -> float:
            fn = getattr(cu, "convert_sol_to_usd", None)
            if not callable(fn):
                return 0.0
            try:
                v = await _await_maybe(fn(float(sol)))
                return float(v or 0.0)
            except Exception:
                return 0.0

        async def send_telegram(self, msg: str) -> None:
            fn = getattr(cu, "send_telegram", None)
            if not callable(fn):
                return None
            try:
                await _await_maybe(fn(str(msg)))
            except Exception:
                return None

        async def get_redis_async(self) -> Any:
            fn = getattr(cu, "get_redis_async", None)
            if not callable(fn):
                return None
            try:
                return await _await_maybe(fn())
            except Exception:
                return None

        def get_redis(self) -> Any:
            fn = getattr(cu, "get_redis", None)
            if not callable(fn):
                return None
            try:
                return fn()
            except Exception:
                return None

    _core_cached = _Wrapped()
    return _core_cached


# DB/ledger hooks (lazy)
_db_log_trade_event: Any = None
_db_log_pnl_snapshot: Any = None
_ledger_write_snapshot: Any = None


def _lazy_load_hooks() -> None:
    global _db_log_trade_event, _db_log_pnl_snapshot, _ledger_write_snapshot
    if (
        _db_log_trade_event is not None
        or _db_log_pnl_snapshot is not None
        or _ledger_write_snapshot is not None
    ):
        # already attempted at least once
        return

    # Sentinel to avoid repeated import attempts
    _db_log_trade_event = False
    _db_log_pnl_snapshot = False
    _ledger_write_snapshot = False

    with contextlib.suppress(Exception):
        from db.logger import log_trade_event as lte  # type: ignore

        _db_log_trade_event = lte

    with contextlib.suppress(Exception):
        from db.logger import log_pnl_snapshot as lps  # type: ignore

        _db_log_pnl_snapshot = lps

    with contextlib.suppress(Exception):
        from utils_pkg.ledger import write_pnl_snapshot as wps  # type: ignore

        _ledger_write_snapshot = wps


# ──────────────────────────────────────────────────────────────────────────────
# Position store of truth: utils_pkg.position_store (preferred)
# Legacy Redis fallback (opt-in only)
# ──────────────────────────────────────────────────────────────────────────────
_position_store_mod: Any = None
_position_store_checked: bool = False


def _get_position_store_mod() -> Any:
    global _position_store_mod, _position_store_checked
    if _position_store_checked:
        return _position_store_mod
    _position_store_checked = True
    try:
        from utils_pkg import position_store as ps  # type: ignore

        _position_store_mod = ps
    except Exception:
        _position_store_mod = None
    return _position_store_mod


@dataclass
class PositionView:
    """
    Canonical-ish view used by watcher.

    Minimal required:
      - mint
      - entry_px (must match units of price feed for that mint)
      - cost_usd (cost basis for remaining position)
    """

    mint: str
    entry_px: float
    cost_usd: float
    symbol: str | None = None
    position_id: str | None = None
    qty: float | None = None
    entry_px_unit: str | None = None


async def _list_positions_from_position_store() -> list[PositionView]:
    ps = _get_position_store_mod()
    if ps is None:
        return []

    candidates: list[tuple[str, dict[str, Any]]] = [
        ("list_open_positions", {}),
        ("get_open_positions", {}),
        ("list_positions", {"state": "open"}),
        ("list_positions", {}),
    ]

    rows: Any = None
    for name, kwargs in candidates:
        fn = getattr(ps, name, None)
        if not callable(fn):
            continue
        try:
            rows = await _await_maybe(fn(**kwargs))
            break
        except TypeError:
            # signature mismatch, retry without kwargs
            with contextlib.suppress(Exception):
                rows = await _await_maybe(fn())
                break
        except Exception:
            continue

    if not rows:
        return []

    if isinstance(rows, dict):
        rows = list(rows.values())

    out: list[PositionView] = []
    for row in rows:
        try:
            if isinstance(row, PositionView):
                out.append(row)
                continue
            if not isinstance(row, dict):
                continue

            mint = str(
                row.get("mint")
                or row.get("token_mint")
                or row.get("token_address")
                or row.get("address")
                or ""
            ).strip()
            if not mint:
                continue

            entry_px = float(
                row.get("entry_px") or row.get("buy_price") or row.get("entry_price") or 0.0
            )
            cost_usd = float(
                row.get("cost_usd")
                or row.get("size_usd")
                or row.get("usd_spent")
                or row.get("amount_invested")
                or 0.0
            )
            if entry_px <= 0 or cost_usd <= 0:
                continue

            out.append(
                PositionView(
                    mint=mint,
                    entry_px=float(entry_px),
                    cost_usd=float(cost_usd),
                    symbol=(row.get("symbol") or row.get("sym")),
                    position_id=(
                        row.get("position_id") or row.get("id") or row.get("buy_signature")
                    ),
                    qty=(float(row["qty"]) if row.get("qty") is not None else None),
                    entry_px_unit=(row.get("entry_px_unit") or row.get("px_unit")),
                )
            )
        except Exception:
            continue

    return out


# ──────────────────────────────────────────────────────────────────────────────
# Redis adapter (best-effort, supports sync OR async redis clients)
# Used for: baselines + last snapshot + (optional) legacy position fallback.
# ──────────────────────────────────────────────────────────────────────────────
_redis_client: Any = None
_redis_lock = asyncio.Lock()


async def _get_redis_best_effort() -> Any:
    global _redis_client

    client = _redis_client
    if client is not None:
        return client

    async with _redis_lock:
        client = _redis_client
        if client is not None:
            return client

        cu = await _core()

        # Prefer async getter
        with contextlib.suppress(Exception):
            client = await _await_maybe(cu.get_redis_async())
            _redis_client = client
            return client

        # Fallback sync getter
        with contextlib.suppress(Exception):
            client = cu.get_redis()
            _redis_client = client
            return client

        _redis_client = None
        return None


def _decode_redis_val(v: Any) -> str:
    if isinstance(v, (bytes, bytearray)):
        return v.decode(errors="replace")
    return str(v)


async def _r_get(r: Any, key: str) -> str | None:
    try:
        v = await _await_maybe(r.get(key))
        if v is None:
            return None
        return _decode_redis_val(v)
    except Exception:
        return None


async def _r_setex(r: Any, key: str, val: str, ttl: int) -> None:
    try:
        if ttl > 0:
            await _await_maybe(r.setex(key, int(ttl), str(val)))
        else:
            await _await_maybe(r.set(key, str(val)))
    except Exception:
        return None


async def _list_positions_from_legacy_redis() -> list[PositionView]:
    """
    LEGACY fallback — only used when PNL_ALLOW_LEGACY_REDIS=1.
    Reads:
      - positions:index   (set of mints)
      - pos:<mint>        (json blob)
    """
    r = await _get_redis_best_effort()
    if r is None:
        return []

    try:
        members = await _await_maybe(r.smembers(LEGACY_REDIS_INDEX_KEY))
    except Exception:
        return []

    out: list[PositionView] = []
    for m in members or []:
        mint = _decode_redis_val(m).strip()
        if not mint:
            continue
        try:
            raw = await _await_maybe(r.get(f"{LEGACY_REDIS_POS_PREFIX}{mint}"))
            if not raw:
                continue
            d = json.loads(_decode_redis_val(raw))
            if not isinstance(d, dict):
                continue

            entry_px = float(d.get("entry_px") or d.get("buy_price") or 0.0)
            cost_usd = float(d.get("size_usd") or d.get("amount_invested") or 0.0)
            if entry_px <= 0 or cost_usd <= 0:
                continue

            out.append(
                PositionView(
                    mint=mint,
                    entry_px=float(entry_px),
                    cost_usd=float(cost_usd),
                    symbol=d.get("symbol"),
                    position_id=d.get("buy_signature") or d.get("signature"),
                    qty=(float(d["qty"]) if d.get("qty") is not None else None),
                    entry_px_unit=d.get("entry_px_unit"),
                )
            )
        except Exception:
            continue

    return out


async def _list_open_positions() -> list[PositionView]:
    """
    Enforces P0: prefer position_store; legacy redis only if explicitly enabled.
    """
    pos = await _list_positions_from_position_store()
    if pos:
        return pos

    if PNL_ALLOW_LEGACY_REDIS:
        log.warning(
            "PnL watcher: using LEGACY Redis position store fallback (PNL_ALLOW_LEGACY_REDIS=1)."
        )
        return await _list_positions_from_legacy_redis()

    if _get_position_store_mod() is None:
        log.warning(
            "PnL watcher: utils_pkg.position_store not available yet and legacy fallback disabled."
        )
    return []


# ──────────────────────────────────────────────────────────────────────────────
# Wallet smoothing
# ──────────────────────────────────────────────────────────────────────────────
_last_good_wallet_usd: float = 0.0
_wallet_zero_streak: int = 0


async def _wallet_usd_smoothed() -> float:
    """
    Computes wallet USD via (SOL balance * USD/SOL) and smooths transient 0 glitches.
    """
    global _last_good_wallet_usd, _wallet_zero_streak

    candidate = 0.0
    try:
        if WALLET_PUBKEY:
            cu = await _core()
            usd_per_sol = float(await cu.convert_sol_to_usd(1.0) or 0.0)
            sol_bal = float(await cu.get_sol_balance(WALLET_PUBKEY) or 0.0)
            candidate = usd_per_sol * sol_bal
    except Exception:
        candidate = 0.0

    if not _finite(candidate) or candidate < PNL_WALLET_MIN_VALID_USD:
        if _last_good_wallet_usd > 0 and _wallet_zero_streak < PNL_ZERO_CONFIRM_STREAK:
            _wallet_zero_streak += 1
            return float(_last_good_wallet_usd)

        _wallet_zero_streak = min(_wallet_zero_streak + 1, PNL_ZERO_CONFIRM_STREAK)
        if _wallet_zero_streak >= PNL_ZERO_CONFIRM_STREAK:
            _last_good_wallet_usd = 0.0
        return float(_last_good_wallet_usd)

    _wallet_zero_streak = 0
    _last_good_wallet_usd = float(candidate)
    return float(_last_good_wallet_usd)


# ──────────────────────────────────────────────────────────────────────────────
# Telegram baseline logic (stored in Redis so restarts keep deltas)
# ──────────────────────────────────────────────────────────────────────────────
@dataclass
class BaselineState:
    equity: float | None = None
    upnl: float | None = None
    since_n: int = 0


async def _load_baseline(r: Any) -> BaselineState:
    st = BaselineState()
    if r is None:
        return st
    try:
        eq = await _r_get(r, REDIS_KEY_BASE_EQ)
        up = await _r_get(r, REDIS_KEY_BASE_UPNL)
        sn = await _r_get(r, REDIS_KEY_BASE_SINCE)
        if eq is not None:
            st.equity = float(eq)
        if up is not None:
            st.upnl = float(up)
        if sn is not None:
            st.since_n = int(sn)
    except Exception:
        pass
    return st


async def _save_baseline(r: Any, st: BaselineState) -> None:
    if r is None:
        return
    try:
        if st.equity is not None:
            await _r_setex(r, REDIS_KEY_BASE_EQ, str(float(st.equity)), REDIS_BASE_TTL_SEC)
        if st.upnl is not None:
            await _r_setex(r, REDIS_KEY_BASE_UPNL, str(float(st.upnl)), REDIS_BASE_TTL_SEC)
        await _r_setex(r, REDIS_KEY_BASE_SINCE, str(int(st.since_n)), REDIS_BASE_TTL_SEC)
    except Exception:
        return


async def _should_send_blip(
    *, r: Any, st: BaselineState, equity_usd: float, upnl_usd: float
) -> bool:
    if not PNL_TELEGRAM_HEARTBEAT:
        return False

    # First time: set baseline and send
    if st.equity is None:
        st.equity = float(equity_usd)
        st.upnl = float(upnl_usd)
        st.since_n = 0
        await _save_baseline(r, st)
        return True

    delta_usd = float(equity_usd) - float(st.equity)
    delta_pct = _pct(float(equity_usd), float(st.equity))

    hit = False
    if abs(delta_usd) >= PNL_TELEGRAM_DELTA_USD:
        hit = True
    if abs(delta_pct) >= PNL_TELEGRAM_DELTA_PCT:
        hit = True

    if not hit and PNL_TELEGRAM_FORCE_EVERY_N > 0 and st.since_n >= PNL_TELEGRAM_FORCE_EVERY_N:
        hit = True

    # If not on-change mode, we send every heartbeat tick
    if not PNL_TELEGRAM_ONLY_ON_CHANGE:
        hit = True

    if hit:
        st.equity = float(equity_usd)
        st.upnl = float(upnl_usd)
        st.since_n = 0
        await _save_baseline(r, st)
        return True

    st.since_n += 1
    await _save_baseline(r, st)
    return False


# ──────────────────────────────────────────────────────────────────────────────
# Price cache
# ──────────────────────────────────────────────────────────────────────────────
class PriceCache:
    def __init__(self, ttl_sec: int) -> None:
        self.ttl = max(1, int(ttl_sec))
        self._cache: dict[str, tuple[float, float]] = {}  # mint -> (px, ts)

    def get(self, mint: str) -> float | None:
        cur = self._cache.get(mint)
        if not cur:
            return None
        px, ts = cur
        if (_now_ts() - ts) <= self.ttl and _finite(px) and px > 0:
            return float(px)
        return None

    def put(self, mint: str, px: float) -> None:
        if _finite(px) and float(px) > 0:
            self._cache[mint] = (float(px), _now_ts())


# ──────────────────────────────────────────────────────────────────────────────
# Snapshot types
# ──────────────────────────────────────────────────────────────────────────────
@dataclass
class PnlSnapshot:
    ts_iso: str
    wallet_usd: float
    open_cost_usd: float
    open_mv_usd: float
    upnl_usd: float
    equity_usd: float
    open_positions: int


# ──────────────────────────────────────────────────────────────────────────────
# Watcher core
# ──────────────────────────────────────────────────────────────────────────────
class PnlWatcher:
    def __init__(self) -> None:
        self._stop = asyncio.Event()
        self._price_cache = PriceCache(PNL_PRICE_CACHE_TTL_SEC)

    def stop(self) -> None:
        self._stop.set()

    async def _fetch_price(self, mint: str, sem: asyncio.Semaphore) -> float | None:
        cached = self._price_cache.get(mint)
        if cached is not None:
            return cached

        async with sem:
            try:
                cu = await _core()
                px = float(await cu.get_token_price(mint) or 0.0)
            except Exception:
                px = 0.0

        if _finite(px) and float(px) > 0:
            self._price_cache.put(mint, float(px))
            return float(px)

        return self._price_cache.get(mint)

    async def compute_snapshot(self) -> PnlSnapshot:
        r = await _get_redis_best_effort()
        ts_iso = _iso_utc()

        positions = await _list_open_positions()
        sem = asyncio.Semaphore(max(1, int(PNL_MAX_PARALLEL_PRICES)))

        # Fetch prices concurrently (one task per mint)
        prices: dict[str, float | None] = {}
        if positions:
            tasks_by_mint: dict[str, asyncio.Task[float | None]] = {}
            for p in positions:
                if p.mint not in tasks_by_mint:
                    tasks_by_mint[p.mint] = asyncio.create_task(self._fetch_price(p.mint, sem))
            results = await asyncio.gather(*tasks_by_mint.values(), return_exceptions=True)
            for mint, res in zip(tasks_by_mint.keys(), results, strict=False):
                if isinstance(res, BaseException):
                    prices[mint] = None
                else:
                    prices[mint] = res

        open_cost_usd = 0.0
        open_mv_usd = 0.0

        # Ratio-based valuation:
        # MV_usd = cost_usd * (last_px / entry_px)
        # (Requires entry_px units match last_px units. That’s your current contract.)
        for p in positions:
            px = prices.get(p.mint)
            if px is None or px <= 0:
                continue
            if p.entry_px <= 0 or p.cost_usd <= 0:
                continue

            open_cost_usd += float(p.cost_usd)
            open_mv_usd += float(p.cost_usd) * float(px / p.entry_px)

        upnl = float(open_mv_usd - open_cost_usd)

        wallet_usd = await _wallet_usd_smoothed()
        equity = float(wallet_usd) + float(open_mv_usd)

        # Cold-start fallback only if explicitly set and we truly have no wallet signal
        if equity <= 0 and EQUITY_USD_FALLBACK > 0 and wallet_usd <= 0:
            equity = float(EQUITY_USD_FALLBACK) + float(open_mv_usd)

        snap = PnlSnapshot(
            ts_iso=ts_iso,
            wallet_usd=float(wallet_usd),
            open_cost_usd=float(open_cost_usd),
            open_mv_usd=float(open_mv_usd),
            upnl_usd=float(upnl),
            equity_usd=float(equity),
            open_positions=int(len(positions)),
        )

        # Persist last snapshot to Redis (best-effort)
        if r is not None:
            with contextlib.suppress(Exception):
                await _r_setex(r, REDIS_KEY_LAST_SNAPSHOT, _json_dumps(snap.__dict__), 600)

        return snap

    async def _emit_snapshot(self, snap: PnlSnapshot) -> None:
        _lazy_load_hooks()

        # DB snapshot row
        if _db_log_pnl_snapshot and callable(_db_log_pnl_snapshot):
            with contextlib.suppress(Exception):
                await _await_maybe(
                    _db_log_pnl_snapshot(
                        snap.ts_iso, float(snap.equity_usd), float(snap.open_cost_usd)
                    )
                )

        # Structured event
        if _db_log_trade_event and callable(_db_log_trade_event):
            with contextlib.suppress(Exception):
                await _await_maybe(
                    _db_log_trade_event(
                        {
                            "type": "pnl_snapshot",
                            "ts": snap.ts_iso,
                            "equity_usd": float(snap.equity_usd),
                            "wallet_usd": float(snap.wallet_usd),
                            "open_cost_usd": float(snap.open_cost_usd),
                            "open_mv_usd": float(snap.open_mv_usd),
                            "upnl_usd": float(snap.upnl_usd),
                            "open_positions": int(snap.open_positions),
                        }
                    )
                )

        # Optional canonical ledger snapshot (if implemented)
        if _ledger_write_snapshot and callable(_ledger_write_snapshot):
            with contextlib.suppress(Exception):
                await _await_maybe(
                    _ledger_write_snapshot(
                        ts_iso=snap.ts_iso,
                        equity_usd=float(snap.equity_usd),
                        wallet_usd=float(snap.wallet_usd),
                        open_mv_usd=float(snap.open_mv_usd),
                        open_cost_usd=float(snap.open_cost_usd),
                        upnl_usd=float(snap.upnl_usd),
                        open_positions=int(snap.open_positions),
                    )
                )

    async def run(self) -> None:
        """
        Main loop:
        - compute snapshot every PNL_INTERVAL_SEC
        - emit snapshot cadence every PNL_HEARTBEAT_EVERY_N ticks
        - optionally send Telegram using deltas (baseline persisted in Redis)
        """
        r = await _get_redis_best_effort()
        baseline = await _load_baseline(r)

        log.info(
            "PnL watcher started: interval=%ss heartbeat_every=%d prices_ttl=%ss max_parallel=%d "
            "telegram=%s/%s legacy_redis=%s position_store=%s wallet_pubkey=%s",
            int(PNL_INTERVAL_SEC),
            int(PNL_HEARTBEAT_EVERY_N),
            int(PNL_PRICE_CACHE_TTL_SEC),
            int(PNL_MAX_PARALLEL_PRICES),
            bool(PNL_TELEGRAM_HEARTBEAT),
            ("on_change" if PNL_TELEGRAM_ONLY_ON_CHANGE else "every")
            if PNL_TELEGRAM_HEARTBEAT
            else "off",
            bool(PNL_ALLOW_LEGACY_REDIS),
            bool(_get_position_store_mod() is not None),
            ("set" if bool(WALLET_PUBKEY) else "missing"),
        )

        tick = 0
        while not self._stop.is_set():
            try:
                snap = await self.compute_snapshot()
                tick += 1

                if tick % max(1, int(PNL_HEARTBEAT_EVERY_N)) == 0:
                    await self._emit_snapshot(snap)

                    if await _should_send_blip(
                        r=r, st=baseline, equity_usd=snap.equity_usd, upnl_usd=snap.upnl_usd
                    ):
                        try:
                            cu = await _core()
                            msg = (
                                "📊 PnL Heartbeat\n"
                                f"equity: ${snap.equity_usd:,.0f}\n"
                                f"wallet: ${snap.wallet_usd:,.0f}\n"
                                f"open MV: ${snap.open_mv_usd:,.0f} | cost: ${snap.open_cost_usd:,.0f}\n"
                                f"uPnL: {snap.upnl_usd:+,.0f}\n"
                                f"open positions: {snap.open_positions}"
                            )
                            await cu.send_telegram(msg)
                        except Exception:
                            pass

            except Exception as e:
                log.warning("PnL watcher loop error: %s", e)

            # Wait with timeout so stop() is responsive
            with contextlib.suppress(asyncio.TimeoutError):
                await asyncio.wait_for(self._stop.wait(), timeout=max(1, int(PNL_INTERVAL_SEC)))


# ──────────────────────────────────────────────────────────────────────────────
# Public entrypoints
# ──────────────────────────────────────────────────────────────────────────────
_watcher: PnlWatcher | None = None
_task: asyncio.Task[None] | None = None


def start_pnl_watcher(loop: asyncio.AbstractEventLoop | None = None) -> asyncio.Task[None]:
    """
    Idempotent starter. Safe to call from startup wiring.
    """
    global _watcher, _task
    if _task is not None and not _task.done():
        return _task

    _watcher = PnlWatcher()

    try:
        loop = loop or asyncio.get_running_loop()
    except RuntimeError:
        # Best-effort compatibility for non-async contexts
        loop = loop or asyncio.get_event_loop()

    _task = loop.create_task(_watcher.run(), name="pnl_watcher")
    return _task


async def stop_pnl_watcher() -> None:
    global _watcher, _task
    if _watcher is not None:
        _watcher.stop()
    if _task is not None and not _task.done():
        _task.cancel()
        with contextlib.suppress(asyncio.CancelledError, Exception):
            await _task
    _watcher = None
    _task = None


__all__ = [
    "PnlSnapshot",
    "PnlWatcher",
    "start_pnl_watcher",
    "stop_pnl_watcher",
]
