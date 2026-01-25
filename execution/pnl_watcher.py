# execution/pnl_watcher.py
from __future__ import annotations

"""
execution/pnl_watcher.py — P0 CANONICAL PnL WATCHER (NO TRADING SIDE EFFECTS)
============================================================================

LOCK-IN (P0):
- PnL watcher is OBSERVABILITY ONLY.
- It must NOT run exit decisions or execute sells.
- Exits are owned by utils_pkg/exit_engine.py + execution/exits_wiring.py.
- PnL watcher computes equity/uPnL from:
    (1) wallet balance (SOL -> USD),
    (2) open positions from ONE position source of truth (position_store),
    (3) optional ledger snapshots / DB logging.

Compatibility:
- If utils_pkg.position_store is not present yet, an *opt-in* legacy Redis fallback
  can be enabled (PNL_ALLOW_LEGACY_REDIS=1) to keep you alive during transition.
  Default is OFF to enforce the “one source of truth” contract.
"""

import asyncio
import contextlib
import json
import math
import os
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, cast

from logging_setup import get_logger

log = get_logger(__name__)


# ──────────────────────────────────────────────────────────────────────────────
# Optional external integrations (tolerant imports)
# ──────────────────────────────────────────────────────────────────────────────
# Prices + wallet + telegram + redis
try:
    from utils_pkg.core_utils import (  # type: ignore
        convert_sol_to_usd,  # async (sol_float) -> USD
        get_redis_async,  # async -> redis client (preferred)
        get_sol_balance,  # async (pubkey) -> SOL
        get_token_price,  # async (mint) -> price (commonly SOL/token)
        send_telegram,  # async (msg)
    )
except Exception:
    get_redis_async = None  # type: ignore

    async def get_token_price(_mint: str) -> float:  # type: ignore
        return 0.0

    async def get_sol_balance(_pub: str) -> float:  # type: ignore
        return 0.0

    async def convert_sol_to_usd(x: float) -> float:  # type: ignore
        return 0.0

    async def send_telegram(_msg: str) -> None:  # type: ignore
        return None


# DB/ledger logging hooks (tolerant)
_log_trade_event = None
_log_pnl_snapshot = None
try:
    from db.logger import log_trade_event as _log_trade_event  # type: ignore
except Exception:
    _log_trade_event = None
try:
    from db.logger import log_pnl_snapshot as _log_pnl_snapshot  # type: ignore
except Exception:
    _log_pnl_snapshot = None

# Optional canonical ledger interface (if already rewritten)
_ledger_write_snapshot = None
try:
    from utils_pkg.ledger import write_pnl_snapshot as _ledger_write_snapshot  # type: ignore
except Exception:
    _ledger_write_snapshot = None

# Optional canonical position store (should become the only source of truth)
_position_store = None
try:
    from utils_pkg import position_store as _position_store  # type: ignore
except Exception:
    _position_store = None


# ──────────────────────────────────────────────────────────────────────────────
# Env / Config
# ──────────────────────────────────────────────────────────────────────────────
def _env_int(name: str, default: int) -> int:
    try:
        v = os.getenv(name)
        return int(v) if v not in (None, "") else int(default)
    except Exception:
        return int(default)


def _env_float(name: str, default: float) -> float:
    try:
        v = os.getenv(name)
        return float(v) if v not in (None, "") else float(default)
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

EPS = 1e-9


# ──────────────────────────────────────────────────────────────────────────────
# Helpers / Types
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


@dataclass
class PositionView:
    """
    Canonical-ish view used by the watcher.

    Required minimal fields for robust PnL:
      - mint
      - entry_px (in same units as price_feed returns for that mint)
      - cost_usd (the USD cost basis for remaining position, or original cost basis)

    Optional:
      - symbol
      - position_id
      - qty (token qty) if you want more exact valuation paths later
      - entry_px_unit (string hint)
    """

    mint: str
    entry_px: float
    cost_usd: float
    symbol: str | None = None
    position_id: str | None = None
    qty: float | None = None
    entry_px_unit: str | None = None


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
# Redis adapter (optional, used for baselines + last snapshot; legacy fallback too)
# ──────────────────────────────────────────────────────────────────────────────
async def _get_redis():
    if get_redis_async is None:
        return None
    try:
        return await get_redis_async()
    except Exception:
        return None


async def _r_get(r, key: str) -> str | None:
    try:
        v = await r.get(key)
        if v is None:
            return None
        return v.decode() if isinstance(v, bytes | bytearray) else str(v)
    except Exception:
        return None


async def _r_setex(r, key: str, val: str, ttl: int) -> None:
    try:
        if ttl > 0:
            await r.setex(key, int(ttl), str(val))
        else:
            await r.set(key, str(val))
    except Exception:
        return None


# ──────────────────────────────────────────────────────────────────────────────
# Position source of truth: utils_pkg.position_store (preferred)
# Fallback: legacy Redis position blobs (opt-in only)
# ──────────────────────────────────────────────────────────────────────────────
async def _list_positions_from_position_store() -> list[PositionView]:
    """
    Best-effort adapter to whatever your rewritten utils_pkg.position_store provides.
    This should stabilize after you rewrite position_store.

    Expected “good future” API patterns (any one is fine):
      - await position_store.list_open_positions()
      - await position_store.list_positions(state="open")
      - await position_store.get_open_positions()
    """
    if _position_store is None:
        return []

    # Candidate callables in priority order
    candidates = [
        ("list_open_positions", {}),
        ("get_open_positions", {}),
        ("list_positions", {"state": "open"}),
        ("list_positions", {}),
    ]

    rows: Any = None
    for name, kwargs in candidates:
        fn = getattr(_position_store, name, None)
        if fn is None:
            continue
        try:
            rows = await fn(**kwargs)  # type: ignore[misc]
            break
        except TypeError:
            # signature mismatch, try no kwargs
            try:
                rows = await fn()  # type: ignore[misc]
                break
            except Exception:
                continue
        except Exception:
            continue

    if not rows:
        return []

    out: list[PositionView] = []
    if isinstance(rows, dict):
        rows = list(rows.values())

    for row in rows:
        try:
            if isinstance(row, PositionView):
                out.append(row)
                continue

            if not isinstance(row, dict):
                continue

            mint = str(row.get("mint") or row.get("token_mint") or row.get("token_address") or "")
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
                # We can’t compute ratio-PnL without these.
                continue

            out.append(
                PositionView(
                    mint=mint,
                    entry_px=entry_px,
                    cost_usd=cost_usd,
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


async def _list_positions_from_legacy_redis() -> list[PositionView]:
    """
    LEGACY fallback — only used when PNL_ALLOW_LEGACY_REDIS=1.
    Reads:
      - positions:index   (set of mints)
      - pos:<mint>        (json blob)
    """
    r = await _get_redis()
    if r is None:
        return []

    try:
        members = await r.smembers(LEGACY_REDIS_INDEX_KEY)
    except Exception:
        return []

    out: list[PositionView] = []
    for m in members or []:
        mint = m.decode() if isinstance(m, bytes | bytearray) else str(m)
        try:
            raw = await r.get(f"{LEGACY_REDIS_POS_PREFIX}{mint}")
            if not raw:
                continue
            s = raw.decode() if isinstance(raw, bytes | bytearray) else str(raw)
            d = json.loads(s)
            entry_px = float(d.get("entry_px") or d.get("buy_price") or 0.0)
            cost_usd = float(d.get("size_usd") or d.get("amount_invested") or 0.0)
            if entry_px <= 0 or cost_usd <= 0:
                continue
            out.append(
                PositionView(
                    mint=mint,
                    entry_px=entry_px,
                    cost_usd=cost_usd,
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

    if _position_store is None:
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
    global _last_good_wallet_usd, _wallet_zero_streak

    candidate = 0.0
    try:
        if WALLET_PUBKEY:
            usd_per_sol = float(await convert_sol_to_usd(1.0) or 0.0)
            sol_bal = float(await get_sol_balance(WALLET_PUBKEY) or 0.0)
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


async def _load_baseline(r) -> BaselineState:
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


async def _save_baseline(r, st: BaselineState) -> None:
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


def _pct(a: float, b: float) -> float:
    if abs(b) <= EPS:
        return 0.0
    return (a / b - 1.0) * 100.0


async def _should_send_blip(
    *,
    r,
    st: BaselineState,
    equity_usd: float,
    upnl_usd: float,
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
# Core watcher
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
                px = await get_token_price(mint)
            except Exception:
                px = 0.0

        if _finite(px) and float(px) > 0:
            self._price_cache.put(mint, float(px))
            return float(px)

        # last chance: cache again (race)
        cached2 = self._price_cache.get(mint)
        return cached2

    async def compute_snapshot(self) -> PnlSnapshot:
        r = await _get_redis()
        ts_iso = _iso_utc()

        positions = await _list_open_positions()
        sem = asyncio.Semaphore(max(1, PNL_MAX_PARALLEL_PRICES))

        # Fetch prices concurrently
        prices: dict[str, float | None] = {}
        if positions:
            tasks = {}
            for p in positions:
                if p.mint not in tasks:
                    tasks[p.mint] = asyncio.create_task(self._fetch_price(p.mint, sem))
            done = await asyncio.gather(*tasks.values(), return_exceptions=True)
            for mint, res in zip(tasks.keys(), done, strict=False):
                prices[mint] = None if isinstance(res, Exception) else cast(float | None, res)

        open_cost_usd = 0.0
        open_mv_usd = 0.0

        # Ratio-based valuation:
        # MV_usd = cost_usd * (last_px / entry_px)   (requires entry_px units match last_px units)
        for p in positions:
            px = prices.get(p.mint)
            if px is None or px <= 0:
                continue
            if p.entry_px <= 0 or p.cost_usd <= 0:
                continue

            open_cost_usd += float(p.cost_usd)
            mv = float(p.cost_usd) * float(px / p.entry_px)
            open_mv_usd += mv

        upnl = open_mv_usd - open_cost_usd

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
                await _r_setex(
                    r,
                    REDIS_KEY_LAST_SNAPSHOT,
                    json.dumps(snap.__dict__, separators=(",", ":")),
                    600,
                )

        return snap

    async def _emit_snapshot(self, snap: PnlSnapshot) -> None:
        # DB snapshot row
        if _log_pnl_snapshot is not None:
            with contextlib.suppress(Exception):
                await _log_pnl_snapshot(
                    snap.ts_iso, float(snap.equity_usd), float(snap.open_cost_usd)
                )

        # Structured event
        if _log_trade_event is not None:
            with contextlib.suppress(Exception):
                await _log_trade_event(
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

        # Optional canonical ledger snapshot (if implemented)
        if _ledger_write_snapshot is not None:
            with contextlib.suppress(Exception):
                await _ledger_write_snapshot(
                    ts_iso=snap.ts_iso,
                    equity_usd=float(snap.equity_usd),
                    wallet_usd=float(snap.wallet_usd),
                    open_mv_usd=float(snap.open_mv_usd),
                    open_cost_usd=float(snap.open_cost_usd),
                    upnl_usd=float(snap.upnl_usd),
                    open_positions=int(snap.open_positions),
                )

    async def run(self) -> None:
        """
        Main loop:
        - compute snapshot every PNL_INTERVAL_SEC
        - emit a “heartbeat” message cadence every PNL_HEARTBEAT_EVERY_N ticks
        - optionally send Telegram using deltas (baseline persisted in Redis)
        """
        r = await _get_redis()
        baseline = await _load_baseline(r)

        log.info(
            "PnL watcher started: interval=%ss heartbeat_every=%d prices_ttl=%ss max_parallel=%d "
            "telegram=%s/%s legacy_redis=%s position_store=%s",
            PNL_INTERVAL_SEC,
            PNL_HEARTBEAT_EVERY_N,
            PNL_PRICE_CACHE_TTL_SEC,
            PNL_MAX_PARALLEL_PRICES,
            PNL_TELEGRAM_HEARTBEAT,
            ("on_change" if PNL_TELEGRAM_ONLY_ON_CHANGE else "every")
            if PNL_TELEGRAM_HEARTBEAT
            else "off",
            PNL_ALLOW_LEGACY_REDIS,
            bool(_position_store is not None),
        )

        tick = 0
        while not self._stop.is_set():
            try:
                snap = await self.compute_snapshot()

                tick += 1
                if tick % max(1, PNL_HEARTBEAT_EVERY_N) == 0:
                    await self._emit_snapshot(snap)

                    if await _should_send_blip(
                        r=r, st=baseline, equity_usd=snap.equity_usd, upnl_usd=snap.upnl_usd
                    ):
                        try:
                            # baseline delta is computed against current baseline BEFORE it’s updated
                            # (we stored new baseline inside _should_send_blip when sending)
                            msg = (
                                f"📊 PnL Heartbeat\n"
                                f"equity: ${snap.equity_usd:,.0f}\n"
                                f"wallet: ${snap.wallet_usd:,.0f}\n"
                                f"open MV: ${snap.open_mv_usd:,.0f} | cost: ${snap.open_cost_usd:,.0f}\n"
                                f"uPnL: {snap.upnl_usd:+,.0f}\n"
                                f"open positions: {snap.open_positions}"
                            )
                            await send_telegram(msg)
                        except Exception:
                            pass

            except Exception as e:
                log.warning("PnL watcher loop error: %s", e)

            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(self._stop.wait(), timeout=max(1, PNL_INTERVAL_SEC))


# ──────────────────────────────────────────────────────────────────────────────
# Public entrypoints
# ──────────────────────────────────────────────────────────────────────────────
_watcher: PnlWatcher | None = None
_task: asyncio.Task | None = None


def start_pnl_watcher(loop: asyncio.AbstractEventLoop | None = None) -> asyncio.Task:
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
        loop = loop or asyncio.get_event_loop()

    _task = loop.create_task(_watcher.run(), name="pnl_watcher")
    return _task


async def stop_pnl_watcher() -> None:
    global _watcher, _task
    if _watcher is not None:
        _watcher.stop()
    if _task is not None and not _task.done():
        _task.cancel()
        with contextlib.suppress(Exception):
            await _task
    _watcher = None
    _task = None


__all__ = [
    "PnlSnapshot",
    "PnlWatcher",
    "start_pnl_watcher",
    "stop_pnl_watcher",
]
