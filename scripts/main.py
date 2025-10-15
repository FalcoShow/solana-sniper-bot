from __future__ import annotations

"""
scripts/main.py (vNext, hardened)

- Hardens startup, logging, and shutdown.
- Adds event-rate diagnostics + momentum gate + guardrails.
- Enforces PF_POOLS filter and supervises the Raydium scanner with backoff.
- **Daily PnL rollup**: once/day Telegram message + row in PG table `daily_pnl`.
- **DRYRUN enforcement** before any buy (env-based: IS_DRYRUN/DRYRUN).
- Degrades gracefully if optional dependencies are missing.

ENV knobs (most are optional / already in your .env)
---------------------------------------------------
PF_DEBUG_TX=0|1
PF_COOLDOWN_SEC=15
PF_MAX_CONCURRENT=2
PF_DEDUPE_ENABLED=1
PF_DEDUPE_TTL_SEC=3600
PF_POOLS=pump,raydium
PAUSE_TRADING=0
MAX_TRADEABLE=0                    # 0 = unlimited
EXIT_ENGINE_ENABLED=1
PF_HEARTBEAT_SEC=45

# Entry filters
ENTRY_MIN_BUY_SOL=0.0
ENTRY_MAX_AGE_MIN=20              # token age bound (minutes)
MIN_POOL_AGE_MIN=3                # pool age minimum (minutes)
ENTRY_MAX_NAME_LEN=40
ENTRY_SYMBOL_BLACKLIST=RUG,SCAM
ENTRY_LIQ_FILTER=1

# Momentum gate
ENTRY_MOMENTUM_ENABLED=1
ENTRY_MOMO_WINDOW_SEC=10
ENTRY_MOMO_MIN_TRADES=6
ENTRY_MOMO_MIN_UP_PCT=5
ENTRY_MOMO_PRICE_TRIES=4
ENTRY_MOMO_PRICE_DELAY_S=0.7
ENTRY_MOMO_MAX_PARALLEL=6

# Optional post-buy smoke test
POSTBUY_MODE=immediate_sell_test   # run a quick sell after a buy (for smoke test)

# Wallet helper: if ~/.config/solana/id.json doesn't exist, we'll write it.
# Must be a JSON array of 64 integers.
KEYPAIR_JSON=[...] or SOLANA_KEYPAIR_JSON=[...]

# Raydium scanner
# ENABLE_RAYDIUM_SCANNER (config.py) and PF_POOLS contains "raydium"
RAYDIUM_SCAN_INTERVAL_SEC=30      # throttle successful scan cycles (adds small jitter)

# Daily PnL rollup
DAILY_PNL_ENABLED=1
DAILY_PNL_HOUR_UTC=23
DAILY_PNL_TEST_NOW=0|1            # 1 = run once right after boot for testing
DAILY_PNL_DEBOUNCE_MIN=10         # never send 2 snapshots within N minutes

# Dedupe TTLs
PF_GATE_TTL_SEC=60                # default is max(ENTRY_MOMO_WINDOW_SEC*2, 60)
PF_BUY_TTL_SEC=600                # default buy claim TTL
"""

import asyncio
import contextlib
import datetime as _dt
import importlib
import json
import os
import signal
import sys
import traceback
from typing import Any, Dict, Optional, Tuple

# ---------------------------
# Optional deps (Telegram / PG)
# ---------------------------
try:
    import httpx
except Exception:
    httpx = None  # type: ignore

try:
    import asyncpg
except Exception:
    asyncpg = None  # type: ignore

# ---------------------------
# Config module (feature flags)
# ---------------------------
try:
    C = importlib.import_module("config")
except Exception:  # pragma: no cover
    class _C:  # minimal fallback
        ENABLE_RAYDIUM_SCANNER = True
    C = _C()  # type: ignore

# ---------------------------
# Logging
# ---------------------------
try:
    from logging_setup import get_logger, bump, start_global_heartbeat
    log = get_logger()
except Exception:  # pragma: no cover
    class _Dummy:
        def event(self, *a, **k): print("[log]", a, k, flush=True)
    def bump(_): pass
    def start_global_heartbeat(*_a, **_k): pass
    log = _Dummy()  # type: ignore

# ---------------------------
# Normalization helpers
# ---------------------------
try:
    from scripts.pp_utils import normalize_mint as _pp_normalize
except Exception:
    def _pp_normalize(s: str) -> str:  # type: ignore
        return s.strip()

try:
    from utils_pkg.mint_guard import (
        normalize_from_any as _mg_normalize,
        is_probable_mint as _mg_prob,
    )
except Exception:  # pragma: no cover
    _mg_normalize = None  # type: ignore
    _mg_prob = None  # type: ignore

def _dryrun_env_on() -> bool:
    """True if either IS_DRYRUN or DRYRUN env is truthy."""
    v = (os.getenv("IS_DRYRUN") or os.getenv("DRYRUN") or "").strip().lower()
    return v in {"1", "true", "yes", "on"}

# ---------------------------
# Event source + trading hooks
# ---------------------------
try:
    from utils_pkg.pumpportal_client import PumpPortalClient
except Exception:  # pragma: no cover
    class PumpPortalClient:  # type: ignore
        def __init__(self, *a, **kw):
            raise RuntimeError("PumpPortalClient not available")

try:
    from utils_pkg.pump_trader import maybe_buy
except Exception:  # pragma: no cover
    async def maybe_buy(*, mint: str, ui: Dict[str, Any]) -> Optional[str]:  # type: ignore
        raise RuntimeError("maybe_buy not available")

try:
    from utils_pkg.trader import sell_on_jupiter, exit_engine, IS_DRYRUN as _pkg_is_dryrun
except Exception:  # pragma: no cover
    async def sell_on_jupiter(*_a, **_kw):  # type: ignore
        raise RuntimeError("sell_on_jupiter not available")
    class _NoopExit:
        async def on_trade_tick(self, *_a, **_kw): ...
        async def on_buy_filled(self, *_a, **_kw): ...
    exit_engine = _NoopExit()  # type: ignore
    _pkg_is_dryrun = True

# Optional utils
try:
    from utils_pkg.dedupe import claim_once  # (key:str, ttl:int) -> bool
except Exception:  # pragma: no cover
    def claim_once(_key: str, _ttl: int = 3600) -> bool:  # type: ignore
        return True

try:
    from utils_pkg.guardrails import buys_allowed, record_buy_result
except Exception:  # pragma: no cover
    def buys_allowed() -> bool:  # type: ignore
        return True
    def record_buy_result(*_args, **_kwargs):  # type: ignore
        return None

try:
    from utils_pkg.positions import all as all_positions
except Exception:  # pragma: no cover
    def all_positions():  # type: ignore
        return []

try:
    from scripts.sell_engine import run_sell_loop
except Exception:  # pragma: no cover
    async def run_sell_loop(stop_evt: asyncio.Event):  # type: ignore
        while not stop_evt.is_set():
            await asyncio.sleep(5)

# Router + price helpers
try:
    from utils_pkg import router  # provides start()/shutdown()
    from utils_pkg.router import get_price_ui as _route_price_ui
    try:
        from utils_pkg.router import get_decimals as _route_get_decimals
    except Exception:
        _route_get_decimals = None  # type: ignore
except Exception:  # pragma: no cover
    router = None  # type: ignore
    async def _route_price_ui(_mint: str) -> float:  # type: ignore
        return 0.0
    _route_get_decimals = None  # type: ignore

# Liquidity screen (optional fallback)
try:
    from utils_pkg.liquidity import screen_token
except Exception:  # pragma: no cover
    async def screen_token(_mint: str):  # type: ignore
        return True, {"ok": True}

# Optional price feed tidy shutdown
try:
    from utils_pkg.price_feed import close as pricefeed_close
except Exception:  # pragma: no cover
    async def pricefeed_close():
        return None

# Raydium scanner (lazy import)
def _get_raydium_scanner():
    try:
        from scanner.raydium_scanner import scan_raydium  # type: ignore
        return scan_raydium
    except Exception:
        return None

# ---------------------------
# ENV helpers
# ---------------------------
def _as_bool(val: Optional[str], default: bool = False) -> bool:
    if val is None:
        return default
    return str(val).strip().lower() in {"1", "true", "yes", "y", "on"}

def _as_float(val: Any, default: float) -> float:
    try:
        return float(val)
    except Exception:
        return default

def _as_int(val: Any, default: int) -> int:
    try:
        return int(val)
    except Exception:
        return default

def _env_bool(name: str, default: bool) -> bool:
    return _as_bool(os.getenv(name, "1" if default else "0"), default)

# ---------------------------
# Config (env)
# ---------------------------
EXIT_ENGINE_ENABLED   = _as_bool(os.getenv("EXIT_ENGINE_ENABLED", "1"), True)
DEDUPE_ENABLED        = _as_bool(os.getenv("PF_DEDUPE_ENABLED", "1"), True)
DEDUPE_TTL_SEC        = _as_int(os.getenv("PF_DEDUPE_TTL_SEC", "3600"), 3600)
LIQ_FILTER_ENABLED    = _as_bool(os.getenv("ENTRY_LIQ_FILTER", "1"), True)

ENTRY_MOMENTUM_ENABLED   = _as_bool(os.getenv("ENTRY_MOMENTUM_ENABLED", "1"), True)
ENTRY_MOMO_WINDOW_SEC    = _as_int(os.getenv("ENTRY_MOMO_WINDOW_SEC", "10"), 10)
ENTRY_MOMO_MIN_TRADES    = _as_int(os.getenv("ENTRY_MOMO_MIN_TRADES", "6"), 6)
ENTRY_MOMO_MIN_UP_PCT    = _as_float(os.getenv("ENTRY_MOMO_MIN_UP_PCT", "5"), 5.0)
ENTRY_MOMO_PRICE_TRIES   = _as_int(os.getenv("ENTRY_MOMO_PRICE_TRIES", "4"), 4)
ENTRY_MOMO_PRICE_DELAY_S = _as_float(os.getenv("ENTRY_MOMO_PRICE_DELAY_S", "0.7"), 0.7)
ENTRY_MOMO_MAX_PARALLEL  = max(1, _as_int(os.getenv("ENTRY_MOMO_MAX_PARALLEL", "6"), 6))

MIN_INITIAL_BUY_SOL   = _as_float(os.getenv("ENTRY_MIN_BUY_SOL", "0.0"), 0.0)
MAX_NAME_LEN          = _as_int(os.getenv("ENTRY_MAX_NAME_LEN", "40"), 40)
ENTRY_MAX_AGE_MIN     = _as_int(os.getenv("ENTRY_MAX_AGE_MIN", "20"), 20)
MIN_POOL_AGE_MIN      = _as_int(os.getenv("MIN_POOL_AGE_MIN", "3"), 3)

SYMBOL_BLACKLIST = {
    s.strip().upper()
    for s in os.getenv("ENTRY_SYMBOL_BLACKLIST", "RUG,SCAM").split(",")
    if s.strip()
}

MAX_CONCURRENCY   = max(1, _as_int(os.getenv("PF_MAX_CONCURRENT", "2"), 2))
LOOP_COOLDOWN_SEC = _as_float(os.getenv("PF_COOLDOWN_SEC", "15"), 15.0)
MAX_TRADEABLE     = _as_int(os.getenv("MAX_TRADEABLE", "0"), 0)  # 0 = unlimited
PAUSE_TRADING     = _as_bool(os.getenv("PAUSE_TRADING", "0"), False)
DEBUG_TX          = _as_bool(os.getenv("PF_DEBUG_TX", "0"), False)

# Pools filter (affects printing and Raydium scanner scheduling)
_POOLS_ENV  = [p.strip().lower() for p in (os.getenv("PF_POOLS") or "").split(",") if p.strip()]
POOLS_FILTER: set[str] = set(_POOLS_ENV)

# Daily PnL env
DAILY_PNL_ENABLED   = _as_bool(os.getenv("DAILY_PNL_ENABLED", "1"), True)
DAILY_PNL_HOUR_UTC  = _as_int(os.getenv("DAILY_PNL_HOUR_UTC", "23"), 23)
DAILY_PNL_TEST_NOW  = _as_bool(os.getenv("DAILY_PNL_TEST_NOW", "0"), False)
DAILY_PNL_CHAT_ID   = (os.getenv("DAILY_PNL_CHAT_ID") or os.getenv("TELEGRAM_CHAT_ID") or "").strip()
DAILY_PNL_BOT       = (os.getenv("TELEGRAM_BOT_TOKEN") or "").strip()
PG_DSN              = os.getenv("DATABASE_URL", "")

# Scanner + dedupe tuning
RAYDIUM_SCAN_INTERVAL_SEC = _as_float(os.getenv("RAYDIUM_SCAN_INTERVAL_SEC", "30"), 30.0)
PF_GATE_TTL_SEC     = _as_int(os.getenv("PF_GATE_TTL_SEC", str(max(_as_int(os.getenv("ENTRY_MOMO_WINDOW_SEC","10"),10)*2, 60))), max(ENTRY_MOMO_WINDOW_SEC*2, 60))
PF_BUY_TTL_SEC      = _as_int(os.getenv("PF_BUY_TTL_SEC", "600"), 600)
DAILY_PNL_DEBOUNCE_MIN = _as_int(os.getenv("DAILY_PNL_DEBOUNCE_MIN", "10"), 10)

# ---------------------------
# Small helpers
# ---------------------------
def _get_first(d: Dict[str, Any], *keys: str, default: Any = None) -> Any:
    for k in keys:
        if k in d and d[k] is not None:
            return d[k]
    return default

def _extract_symbol(msg: Dict[str, Any]) -> str:
    return str(_get_first(msg, "symbol", "ticker", default="")).strip()

def _extract_name(msg: Dict[str, Any]) -> str:
    return str(_get_first(msg, "name", default="")).strip()

def _extract_raw_mint(msg: Dict[str, Any]) -> str:
    raw = _get_first(
        msg,
        "mint","mintAddress","mint_address","bondingCurve","bonding_curve","address","id",
        default="",
    )
    return ("" if raw is None else str(raw)).strip()

def _approx_minutes_since(ts_any: Any) -> Optional[float]:
    try:
        now = _dt.datetime.now(_dt.timezone.utc).timestamp()
        if ts_any is None:
            return None
        if isinstance(ts_any, (int, float)):
            t = float(ts_any)
            if t > 10_000_000_000:  # ms → s
                t /= 1000.0
            return max(0.0, (now - t) / 60.0)
        if isinstance(ts_any, str):
            s = ts_any.strip()
            if not s:
                return None
            try:
                f = float(s)
                if f > 10_000_000_000:
                    f /= 1000.0
                return max(0.0, (now - f) / 60.0)
            except Exception:
                pass
            try:
                dt = _dt.datetime.fromisoformat(s.replace("Z", "+00:00"))
                return max(0.0, (now - dt.timestamp()) / 60.0)
            except Exception:
                return None
    except Exception:
        return None
    return None

# ---------------------------
# Basic filters
# ---------------------------
def _passes_basic_filters(msg: Dict[str, Any]) -> bool:
    pool = str((_get_first(msg, "pool", default="") or "")).strip().lower()
    is_raydium = (pool == "raydium")

    if POOLS_FILTER and pool and pool not in POOLS_FILTER:
        if DEBUG_TX:
            print(f".[filter] pool '{pool}' not in {sorted(POOLS_FILTER)} — skip", flush=True)
        return False

    typ = str(_get_first(msg, "txType", "type", default="")).strip().lower()
    # Pump has txType=create; Raydium feed usually doesn't—so only enforce for non-Raydium.
    if not is_raydium and typ != "create":
        return False

    init_buy = _as_float(_get_first(msg, "initialBuy", "initial_buy", default=0.0), 0.0)
    if not is_raydium and init_buy < MIN_INITIAL_BUY_SOL:
        if DEBUG_TX:
            print(f".[filter] initialBuy {init_buy} < min {MIN_INITIAL_BUY_SOL} — skip", flush=True)
        return False

    sym = _extract_symbol(msg).upper()
    if any(bad and bad in sym for bad in SYMBOL_BLACKLIST):
        if DEBUG_TX:
            print(f".[filter] symbol '{sym}' hits blacklist {SYMBOL_BLACKLIST} — skip", flush=True)
        return False

    nm = _extract_name(msg)
    if MAX_NAME_LEN > 0 and len(nm) > MAX_NAME_LEN:
        if DEBUG_TX:
            print(f".[filter] name too long ({len(nm)} > {MAX_NAME_LEN}) — skip", flush=True)
        return False

    mins_since_token = _approx_minutes_since(
        _get_first(msg, "created_at", "createdAt", "timestamp", "ts", default=None)
    )
    if mins_since_token is not None and ENTRY_MAX_AGE_MIN > 0 and mins_since_token > ENTRY_MAX_AGE_MIN:
        if DEBUG_TX:
            print(f".[filter] token age {mins_since_token:.1f}m > max {ENTRY_MAX_AGE_MIN}m — skip", flush=True)
        return False

    mins_since_pool = _approx_minutes_since(_get_first(msg, "poolCreatedAt","pool_created_at", default=None))
    if mins_since_pool is not None and MIN_POOL_AGE_MIN > 0 and mins_since_pool < MIN_POOL_AGE_MIN:
        if DEBUG_TX:
            print(f".[filter] pool age {mins_since_pool:.1f}m < min {MIN_POOL_AGE_MIN}m — skip", flush=True)
        return False

    return True

# ---------------------------
# Post-buy optional immediate sell test
# ---------------------------
async def _post_buy_exit_test(mint_norm: str, tx_sig: str) -> None:
    if str(os.getenv("POSTBUY_MODE", "")).strip().lower() != "immediate_sell_test":
        return
    await asyncio.sleep(5)
    try:
        await sell_on_jupiter(mint_norm, pct_of_wallet=1.0)
    except Exception:
        if DEBUG_TX:
            traceback.print_exc()

# ---------------------------
# Liquidity retry helper (handles 'no_account')
# ---------------------------
async def _screen_with_retry(mint: str, tries: int = 3, first_wait: float = 0.8):
    wait = first_wait
    last = None
    for i in range(tries):
        ok, rep = await screen_token(mint)
        last = rep
        if ok:
            return True, rep
        if str((rep or {}).get("reason")) != "no_account":
            return False, rep
        if i < tries - 1:
            if DEBUG_TX:
                print(f"[liquidity] retrying for {mint} (no_account) in {wait:.2f}s", flush=True)
            await asyncio.sleep(wait)
            wait *= 1.6
    return False, last

async def _await_mint_ready(mint_norm: str, max_wait_s: int = 45, step_s: float = 3.0) -> Optional[int]:
    if _route_get_decimals is None:
        return None
    waited = 0.0
    while waited < max_wait_s:
        try:
            dec = await _route_get_decimals(mint_norm)  # type: ignore
            if isinstance(dec, int) and 0 <= dec <= 18:
                return dec
        except Exception:
            pass
        await asyncio.sleep(step_s)
        waited += step_s
    return None

# ---------------------------
# Boot: ensure keypair file
# ---------------------------
def _ensure_keypair_file() -> None:
    import pathlib, stat
    try:
        home = os.path.expanduser("~")
        cfg_dir = pathlib.Path(home) / ".config" / "solana"
        key_path = cfg_dir / "id.json"
        if key_path.exists():
            return
        raw = os.getenv("KEYPAIR_JSON") or os.getenv("SOLANA_KEYPAIR_JSON")
        if not raw:
            log.event("KEYPAIR_ENV_MISSING", hint="set KEYPAIR_JSON to auto-create wallet")
            return
        kp = json.loads(raw)
        if not (isinstance(kp, list) and len(kp) == 64 and all(isinstance(x, int) for x in kp)):
            raise ValueError("KEYPAIR_JSON must be a JSON array of 64 integers")
        cfg_dir.mkdir(parents=True, exist_ok=True)
        key_path.write_text(json.dumps(kp), encoding="utf-8")
        key_path.chmod(stat.S_IRUSR | stat.S_IWUSR)
        with contextlib.suppress(Exception):
            cfg_dir.chmod(stat.S_IRWXU)
        log.event("KEYPAIR_FILE_WRITTEN", path=str(key_path))
    except Exception as e:
        try:
            log.event("KEYPAIR_FILE_WRITE_FAIL", error=str(e))
        except Exception:
            print(f"[boot] could not write wallet: {e}", flush=True)

# ---------------------------
# Strategy
# ---------------------------
class Strategy:
    def __init__(self) -> None:
        self._sema = asyncio.Semaphore(MAX_CONCURRENCY)
        self._gate_sema = asyncio.Semaphore(ENTRY_MOMO_MAX_PARALLEL)
        self._gate_state: Dict[str, Dict[str, Any]] = {}
        # diagnostics
        self._diag = {
            "events_total": 0,
            "events_trade": 0,
            "events_new_token": 0,
            "rejected_filters": 0,
            "rejected_guardrails": 0,
            "rejected_momo": 0,
            "buys_submitted": 0,
            "buys_filled": 0,
        }

    # --- DIAGNOSTICS ---
    async def diag_loop(self, stop_evt: asyncio.Event, interval_s: float = 60.0) -> None:
        last_total = 0
        while not stop_evt.is_set():
            await asyncio.sleep(interval_s)
            et = self._diag["events_total"]
            rate = (et - last_total) / interval_s
            last_total = et
            log.event(
                "DIAG_MINUTE",
                events_total=self._diag["events_total"],
                per_sec=f"{rate:.3f}",
                trade=self._diag["events_trade"],
                new_token=self._diag["events_new_token"],
                rej_filters=self._diag["rejected_filters"],
                rej_guard=self._diag["rejected_guardrails"],
                rej_momo=self._diag["rejected_momo"],
                buys_sub=self._diag["buys_submitted"],
                buys_fill=self._diag["buys_filled"],
            )

    async def on_trade(self, raw_event: Dict[str, Any]) -> None:
        self._diag["events_total"] += 1
        self._diag["events_trade"] += 1
        if EXIT_ENGINE_ENABLED:
            try:
                await exit_engine.on_trade_tick(raw_event)
            except Exception:
                if DEBUG_TX:
                    traceback.print_exc()
        m = str(_get_first(raw_event, "mint", "mintAddress", "mint_address", "id", default="")).strip()
        if not m:
            return
        st = self._gate_state.get(m)
        if st is not None:
            st["trades"] = int(st.get("trades", 0)) + 1

    async def _gate_candidate(self, ui: Dict[str, Any], mint_raw: str, mint_norm: str) -> None:
        gate_key = mint_norm  # use normalized mint for gate and dedupe keys
        async with self._gate_sema:
            if DEDUPE_ENABLED:
                if not claim_once(f"gate:{gate_key}", ttl=int(PF_GATE_TTL_SEC)):
                    if DEBUG_TX:
                        print(f".[dedupe] gate already running for {gate_key} — skip", flush=True)
                    return
            self._gate_state[mint_raw] = {"trades": 0, "price0": 0.0}
            price0 = 0.0
            if _is_probable_mint(mint_norm):
                for _ in range(max(1, ENTRY_MOMO_PRICE_TRIES)):
                    try:
                        price0 = await _route_price_ui(mint_norm)
                    except Exception:
                        price0 = 0.0
                    if price0 > 0:
                        break
                    await asyncio.sleep(ENTRY_MOMO_PRICE_DELAY_S)
            self._gate_state[mint_raw]["price0"] = price0
            await asyncio.sleep(max(1, ENTRY_MOMO_WINDOW_SEC))
            st = self._gate_state.get(mint_raw, {})
            trades = int(st.get("trades", 0))
            price1 = 0.0
            if _is_probable_mint(mint_norm):
                try:
                    price1 = await _route_price_ui(mint_norm)
                except Exception:
                    price1 = 0.0
            up_ok = True
            if price0 > 0 and price1 > 0:
                up_pct = ((price1 / price0) - 1.0) * 100.0
                up_ok = up_pct >= ENTRY_MOMO_MIN_UP_PCT
            trades_ok = trades >= ENTRY_MOMO_MIN_TRADES
            log.event(
                "MOMO_EVAL",
                mint=mint_raw,
                trades=trades,
                price0=price0,
                price1=price1,
                min_trades=ENTRY_MOMO_MIN_TRADES,
                min_up_pct=ENTRY_MOMO_MIN_UP_PCT,
                up_ok=int(up_ok),
                trades_ok=int(trades_ok),
            )
            self._gate_state.pop(mint_raw, None)

            # guardrails before buy
            if not buys_allowed() or PAUSE_TRADING:
                if DEBUG_TX:
                    print("[guardrails] buys not allowed at gate decision — skip", flush=True)
                record_buy_result(ok=False, reason="guardrails_halt")
                self._diag["rejected_guardrails"] += 1
                return
            if MAX_TRADEABLE > 0:
                try:
                    if len(list(all_positions())) >= MAX_TRADEABLE:
                        log.event("BUY_BLOCK_MAX_TRADEABLE", cap=MAX_TRADEABLE)
                        record_buy_result(ok=False, reason="max_tradeable")
                        self._diag["rejected_guardrails"] += 1
                        return
                except Exception:
                    pass

            if trades_ok and up_ok:
                if DEDUPE_ENABLED and not claim_once(f"buy:{mint_norm}", ttl=int(PF_BUY_TTL_SEC)):
                    if DEBUG_TX:
                        print(f".[dedupe] buy already claimed for {mint_norm} — skip", flush=True)
                    record_buy_result(ok=False, reason="buy_claimed_elsewhere")
                    return

                # 🔒 enforce DRYRUN BEFORE any buy
                if _dryrun_env_on() or _pkg_is_dryrun:
                    log.event("BUY_SKIPPED_DRYRUN", mint=mint_norm)
                    record_buy_result(ok=False, reason="dryrun_skip")
                    return

                try:
                    async with self._sema:
                        tx_sig = await maybe_buy(mint=mint_norm, ui=ui)
                    self._diag["buys_submitted"] += 1
                    if tx_sig:
                        record_buy_result(ok=True, reason="executed", sig=tx_sig)
                        self._diag["buys_filled"] += 1
                        if EXIT_ENGINE_ENABLED and not _dryrun_env_on():
                            asyncio.create_task(exit_engine.on_buy_filled(raw_id_or_mint=mint_norm, tx_sig=tx_sig))
                        asyncio.create_task(_post_buy_exit_test(mint_norm, tx_sig))
                    else:
                        record_buy_result(ok=False, reason="no_tx_sig")
                except Exception:
                    traceback.print_exc()
                    record_buy_result(ok=False, reason="exception")
            else:
                record_buy_result(ok=False, reason="momo_reject")
                self._diag["rejected_momo"] += 1

    async def on_new_token(self, raw_event: Dict[str, Any]) -> None:
        self._diag["events_total"] += 1
        self._diag["events_new_token"] += 1

        if PAUSE_TRADING or not buys_allowed():
            if DEBUG_TX:
                print("[guardrails] buys not allowed now — skip", flush=True)
            self._diag["rejected_guardrails"] += 1
            return
        if MAX_TRADEABLE > 0:
            try:
                if len(list(all_positions())) >= MAX_TRADEABLE:
                    log.event("BUY_BLOCK_MAX_TRADEABLE", cap=MAX_TRADEABLE)
                    self._diag["rejected_guardrails"] += 1
                    return
            except Exception:
                pass

        mint_source = _extract_raw_mint(raw_event)
        if not mint_source:
            if DEBUG_TX:
                print("[strategy] missing mint — skip", flush=True)
            return
        mint_norm = _normalize_from_source(mint_source)
        if not _is_probable_mint(mint_norm):
            if DEBUG_TX:
                print(f"[strategy] implausible mint after normalize => skip ({mint_source})", flush=True)
            return
        if not _passes_basic_filters(raw_event):
            self._diag["rejected_filters"] += 1
            return

        dec_from_liq: Optional[int] = None
        if LIQ_FILTER_ENABLED:
            ok, rep = await _screen_with_retry(mint_norm, tries=3, first_wait=0.8)
            if not ok and str((rep or {}).get("reason")) == "no_account":
                dec_from_liq = await _await_mint_ready(mint_norm, max_wait_s=45, step_s=3.0)
                if dec_from_liq is not None:
                    ok = True
                    rep = {"ok": True, "reason": "await_mint_ready", "decimals": dec_from_liq}
            if not ok:
                if DEBUG_TX:
                    print(f"[liquidity] screen failed for {mint_norm}: {rep}", flush=True)
                self._diag["rejected_filters"] += 1
                return
            if dec_from_liq is None:
                dec_from_liq = (rep or {}).get("decimals")
            log.event("ENTRY_LIQ_OK", mint=mint_norm, priceImpactPct=(rep or {}).get("priceImpactPct"), dec=dec_from_liq)

        ui: Dict[str, Any] = dict(raw_event)
        ui.setdefault("pool", (raw_event.get("pool") or "pump").lower())
        ui.setdefault("symbol", _extract_symbol(raw_event))
        ui.setdefault("name", _extract_name(raw_event))
        ui["rawMint"] = mint_source
        ui["mint"] = mint_norm
        ui["normalized"] = (mint_norm != mint_source)
        if dec_from_liq is not None:
            ui["decimals"] = dec_from_liq

        pool = ui.get("pool", "?")
        symbol = ui.get("symbol", "") or "?"
        print(f"[trade] pool={pool} symbol={symbol} mint={mint_norm}", flush=True)

        # momentum or direct buy
        if DEDUPE_ENABLED and ENTRY_MOMENTUM_ENABLED:
            # use normalized key and new TTL for “gate-propose” too
            if not claim_once(f"gate-propose:{mint_norm}", ttl=int(PF_GATE_TTL_SEC)):
                if DEBUG_TX:
                    print(f".[dedupe] gate-propose taken for {mint_norm} — skip", flush=True)
                return

        if ENTRY_MOMENTUM_ENABLED:
            asyncio.create_task(self._gate_candidate(ui, mint_source, mint_norm))
        else:
            if DEDUPE_ENABLED and not claim_once(f"buy:{mint_norm}", ttl=int(PF_BUY_TTL_SEC)):
                if DEBUG_TX:
                    print(f".[dedupe] buy already claimed for {mint_norm} — skip", flush=True)
                return

            if _dryrun_env_on() or _pkg_is_dryrun:
                log.event("BUY_SKIPPED_DRYRUN", mint=mint_norm)
                record_buy_result(ok=False, reason="dryrun_skip")
                return

            async with self._sema:
                try:
                    if PAUSE_TRADING or not buys_allowed():
                        if DEBUG_TX:
                            print("[guardrails] buys not allowed at execution — skip", flush=True)
                        self._diag["rejected_guardrails"] += 1
                        return
                    if MAX_TRADEABLE > 0:
                        try:
                            if len(list(all_positions())) >= MAX_TRADEABLE:
                                log.event("BUY_BLOCK_MAX_TRADEABLE", cap=MAX_TRADEABLE)
                                self._diag["rejected_guardrails"] += 1
                                return
                        except Exception:
                            pass
                    tx_sig = await maybe_buy(mint=mint_norm, ui=ui)
                    self._diag["buys_submitted"] += 1
                    if not tx_sig:
                        record_buy_result(ok=False, reason="no_tx_sig")
                        return
                    record_buy_result(ok=True, reason="executed", sig=tx_sig)
                    self._diag["buys_filled"] += 1
                    if EXIT_ENGINE_ENABLED and not _dryrun_env_on():
                        asyncio.create_task(exit_engine.on_buy_filled(raw_id_or_mint=mint_norm, tx_sig=tx_sig))
                    asyncio.create_task(_post_buy_exit_test(mint_norm, tx_sig))
                except Exception:
                    print("[strategy] maybe_buy exception:", flush=True)
                    traceback.print_exc()
                    record_buy_result(ok=False, reason="exception")

        await asyncio.sleep(LOOP_COOLDOWN_SEC)

# ---------------------------
# Mint normalization wrappers
# ---------------------------
def _normalize_from_source(raw: str) -> str:
    if raw is None:
        return ""
    raw = str(raw).strip()
    m = _pp_normalize(raw)
    if m:
        return m
    if _mg_normalize:
        try:
            m2 = _mg_normalize(raw)
            if isinstance(m2, str) and m2:
                return m2
        except Exception:
            pass
    return raw

def _is_probable_mint(s: str) -> bool:
    if not s:
        return False
    if _mg_prob:
        try:
            return bool(_mg_prob(s))
        except Exception:
            return False
    return bool(_pp_normalize(s))

# ---------------------------
# Daily PnL helpers
# ---------------------------
def _fmt_sol(x: Optional[float]) -> str:
    if x is None:
        return "—"
    try:
        return f"{x:.4f} SOL"
    except Exception:
        return str(x)

async def _telegram_send(text: str) -> None:
    if not DAILY_PNL_BOT or not DAILY_PNL_CHAT_ID or not httpx:
        log.event("DAILY_PNL_TG_SKIP", reason="missing_cfg_or_httpx")
        return
    try:
        url = f"https://api.telegram.org/bot{DAILY_PNL_BOT}/sendMessage"
        payload = {"chat_id": DAILY_PNL_CHAT_ID, "text": text, "parse_mode": "HTML", "disable_web_page_preview": True}
        async with httpx.AsyncClient(timeout=15) as cli:
            r = await cli.post(url, json=payload)
            log.event("DAILY_PNL_TG_SENT", http_status=getattr(r, "status_code", None))
    except Exception as e:
        if DEBUG_TX:
            traceback.print_exc()
        log.event("DAILY_PNL_TG_ERROR", error=str(e))

async def _pnl_pg_ensure_table() -> None:
    if not asyncpg or not PG_DSN:
        log.event("DAILY_PNL_PG_SKIP", reason="no_asyncpg_or_dsn")
        return
    try:
        conn = await asyncpg.connect(PG_DSN)
        try:
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS daily_pnl (
                    id BIGSERIAL PRIMARY KEY,
                    at_utc TIMESTAMPTZ NOT NULL,
                    realized_sol NUMERIC,
                    unrealized_sol NUMERIC,
                    open_positions INT,
                    wallet_sol NUMERIC,
                    notes JSONB,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );
            """)
            log.event("DAILY_PNL_PG_READY")
        finally:
            await conn.close()
    except Exception as e:
        if DEBUG_TX:
            traceback.print_exc()
        log.event("DAILY_PNL_PG_ERROR", when="ensure_table", error=str(e))

async def _pnl_pg_insert_row(*, realized_sol: float, unrealized_sol: float,
                             open_positions: int, wallet_sol: Optional[float],
                             notes: Dict[str, Any]) -> None:
    if not asyncpg or not PG_DSN:
        log.event("DAILY_PNL_PG_SKIP", reason="no_asyncpg_or_dsn")
        return
    try:
        conn = await asyncpg.connect(PG_DSN)
        try:
            await conn.execute("""
                INSERT INTO daily_pnl (at_utc, realized_sol, unrealized_sol, open_positions, wallet_sol, notes)
                VALUES (NOW() AT TIME ZONE 'UTC', $1, $2, $3, $4, $5::jsonb)
            """, realized_sol, unrealized_sol, open_positions, wallet_sol, json.dumps(notes))
            log.event("DAILY_PNL_PG_INSERTED",
                      realized_sol=realized_sol, unrealized_sol=unrealized_sol,
                      open_positions=open_positions)
        finally:
            await conn.close()
    except Exception as e:
        if DEBUG_TX:
            traceback.print_exc()
        log.event("DAILY_PNL_PG_ERROR", when="insert_row", error=str(e))

async def _get_wallet_sol_balance() -> Optional[float]:
    # Best-effort — if you have a wallet util, we use it; otherwise None.
    try:
        from utils_pkg.wallet import get_sol_balance  # type: ignore
        bal = await get_sol_balance()
        return float(bal)
    except Exception:
        return None

async def _estimate_unrealized_from_positions() -> Tuple[float, int, Dict[str, Any]]:
    """
    Conservative unrealized = sum(current_value - cost_basis) over open positions.
    We try multiple field names and swallow errors per-position.
    """
    total_unrl = 0.0
    open_n = 0
    details: Dict[str, Any] = {"legs": []}

    try:
        poss = list(all_positions())
    except Exception:
        poss = []

    for p in poss:
        try:
            mint = str(p.get("mint") or p.get("token") or "")
            if not mint:
                continue
            qty = p.get("amount") or p.get("qty") or p.get("size") or 0
            qty = float(qty or 0)
            if qty <= 0:
                continue
            # entry cost (in SOL); try multiple keys
            cost = p.get("entry_total_sol") or p.get("cost_basis_sol") or p.get("cost") or 0.0
            cost = float(cost or 0.0)

            price = 0.0
            try:
                price = float(await _route_price_ui(mint))
            except Exception:
                price = 0.0

            cur_val = price * qty if price > 0 else 0.0
            unrl = cur_val - cost
            total_unrl += unrl
            open_n += 1
            details["legs"].append({
                "mint": mint,
                "qty": qty,
                "entry_cost_sol": cost,
                "mark_price_sol": price,
                "cur_val_sol": cur_val,
                "unrealized_sol": unrl,
            })
        except Exception:
            if DEBUG_TX:
                traceback.print_exc()
            continue

    return total_unrl, open_n, details

async def _estimate_realized_today() -> Tuple[float, Dict[str, Any]]:
    """
    Realized for the last UTC day. Prefer project-provided helpers; else 0.0 with a source note.
    """
    try:
        from utils_pkg.realized import realized_today_sol  # type: ignore
        v = await realized_today_sol()
        return float(v or 0.0), {"source": "utils_pkg.realized.realized_today_sol"}
    except Exception:
        pass
    try:
        from positions_pg import realized_today_sol as pg_realized_today_sol  # type: ignore
        v = await pg_realized_today_sol()
        return float(v or 0.0), {"source": "positions_pg.realized_today_sol"}
    except Exception:
        pass
    return 0.0, {"source": "fallback_zero"}

async def _run_daily_pnl_loop(stop_evt: asyncio.Event) -> None:
    """
    At DAILY_PNL_HOUR_UTC each day (or immediately if DAILY_PNL_TEST_NOW=1 at boot),
    compute realized/unrealized/wallet SOL and post to Telegram + insert into PG.
    """
    if not DAILY_PNL_ENABLED:
        return

    await _pnl_pg_ensure_table()

    _last_sent_ts: Optional[float] = None

    async def _once():
        nonlocal _last_sent_ts
        now_ts = _dt.datetime.now(_dt.timezone.utc).timestamp()
        if _last_sent_ts and (now_ts - _last_sent_ts) < (DAILY_PNL_DEBOUNCE_MIN * 60):
            log.event("DAILY_PNL_DEBOUNCE_SKIP", since_sec=int(now_ts - _last_sent_ts))
            return

        unrl_sol, open_n, unrl_details = await _estimate_unrealized_from_positions()
        realized_sol, r_meta = await _estimate_realized_today()
        wallet_sol = await _get_wallet_sol_balance()

        now = _dt.datetime.now(_dt.timezone.utc)
        hdr = f"📊 <b>Daily PnL</b> — {now.strftime('%Y-%m-%d')} (UTC)"
        body = (
            f"\nRealized: <b>{_fmt_sol(realized_sol)}</b>"
            f"\nUnrealized: <b>{_fmt_sol(unrl_sol)}</b>"
            f"\nOpen positions: <b>{open_n}</b>"
            f"\nWallet: <b>{_fmt_sol(wallet_sol)}</b>"
        )
        footer = "\n\n<code>src=daily_pnl_loop</code>"

        notes = {"realized_meta": r_meta, "unrealized_details": unrl_details}

        await _pnl_pg_insert_row(
            realized_sol=realized_sol,
            unrealized_sol=unrl_sol,
            open_positions=open_n,
            wallet_sol=wallet_sol,
            notes=notes,
        )
        _last_sent_ts = now_ts
        await _telegram_send(hdr + body + footer)

        log.event(
            "DAILY_PNL_SNAPSHOT",
            realized_sol=realized_sol,
            unrealized_sol=unrl_sol,
            open_positions=open_n,
            wallet_sol=wallet_sol,
        )

    # optional immediate test on boot
    if DAILY_PNL_TEST_NOW and not stop_evt.is_set():
        with contextlib.suppress(Exception):
            await _once()

    # schedule daily at the requested UTC hour
    while not stop_evt.is_set():
        now = _dt.datetime.now(_dt.timezone.utc)
        run_at = now.replace(hour=DAILY_PNL_HOUR_UTC, minute=0, second=0, microsecond=0)
        if run_at <= now:
            run_at = run_at + _dt.timedelta(days=1)
        wait_s = (run_at - now).total_seconds()
        # chunk the sleep so we can stop promptly
        chunk = 5.0
        while wait_s > 0 and not stop_evt.is_set():
            sl = min(chunk, wait_s)
            await asyncio.sleep(sl)
            wait_s -= sl
        if stop_evt.is_set():
            break
        with contextlib.suppress(Exception):
            await _once()

# ---------------------------
# Streaming loop
# ---------------------------
async def stream_loop(stop_evt: Optional[asyncio.Event] = None) -> None:
    strat = Strategy()

    # internal diag task, bound to stream lifetime
    local_stop = stop_evt or asyncio.Event()
    diag_task = asyncio.create_task(strat.diag_loop(local_stop), name="diag.minute")

    def _mask_uri(obj: Any) -> str:
        try:
            fn = getattr(obj, "masked_uri", None)
            return fn() if callable(fn) else "<uri?>"
        except Exception:
            return "<uri?>"

    subscribe_trades_flag = _env_bool("PUMP_SUBSCRIBE_TRADES", True)

    if hasattr(PumpPortalClient, "__aenter__"):
        # context-managed path
        try:
            async with PumpPortalClient(subscribe_trades=subscribe_trades_flag) as client:  # type: ignore[misc]
                print("[main] Connecting:", _mask_uri(client), flush=True)
                async for ev in client.events():
                    if stop_evt and stop_evt.is_set():
                        break
                    try:
                        kind = ev.get("kind")
                        raw = ev.get("raw", {}) or {}
                        if kind == "new_token":
                            await strat.on_new_token(raw)
                        elif kind == "trade":
                            await strat.on_trade(raw)
                    except asyncio.CancelledError:
                        raise
                    except Exception:
                        print("[main] event handling error:", flush=True)
                        traceback.print_exc()
        except TypeError:
            # Older client without the kwarg; fall back
            async with PumpPortalClient() as client:  # type: ignore[misc]
                print("[main] Connecting:", _mask_uri(client), flush=True)
                async for ev in client.events():
                    if stop_evt and stop_evt.is_set():
                        break
                    try:
                        kind = ev.get("kind")
                        raw = ev.get("raw", {}) or {}
                        if kind == "new_token":
                            await strat.on_new_token(raw)
                        elif kind == "trade":
                            await strat.on_trade(raw)
                    except asyncio.CancelledError:
                        raise
                    except Exception:
                        print("[main] event handling error:", flush=True)
                        traceback.print_exc()
    else:
        # non-context path
        try:
            client = PumpPortalClient(subscribe_trades=subscribe_trades_flag)  # may raise if unsupported
        except TypeError:
            client = PumpPortalClient()
        print("[main] Connecting:", _mask_uri(client), flush=True)
        try:
            async for ev in client.events():
                if stop_evt and stop_evt.is_set():
                    break
                try:
                    kind = ev.get("kind")
                    raw = ev.get("raw", {}) or {}
                    if kind == "new_token":
                        await strat.on_new_token(raw)
                    elif kind == "trade":
                        await strat.on_trade(raw)
                except asyncio.CancelledError:
                    raise
                except Exception:
                    print("[main] event handling error:", flush=True)
                    traceback.print_exc()
        finally:
            if hasattr(client, "close"):
                with contextlib.suppress(Exception):
                    maybe_awaitable = client.close()
                    if asyncio.iscoroutine(maybe_awaitable):
                        await maybe_awaitable  # type: ignore

    with contextlib.suppress(Exception):
        local_stop.set()
        diag_task.cancel()
        await asyncio.wait_for(diag_task, timeout=1.5)

# ---------------------------
# Raydium scanner supervisor
# ---------------------------
async def _run_raydium_scanner_forever(stop_evt: asyncio.Event) -> None:
    scan_raydium = _get_raydium_scanner()
    if scan_raydium is None:
        log.event("SCAN_RAYDIUM_UNAVAILABLE")
        return

    backoff = 2.0
    first_log = True
    while not stop_evt.is_set():
        try:
            if first_log:
                log.event(
                    "SCAN_RAYDIUM_STARTING",
                    task="scan.raydium",
                    limit=os.getenv("RAYDIUM_LIMIT"),
                    min_liq=os.getenv("RAYDIUM_MIN_LIQ_USD"),
                )
                first_log = False
            await scan_raydium()  # reads env itself
            log.event("SCAN_RAYDIUM_RETURNED")
            # Throttle successful runs to avoid hammering the upstream
            if RAYDIUM_SCAN_INTERVAL_SEC > 0:
                # add 0–15% jitter, mildly process-differentiated
                jitter = 0.15 * RAYDIUM_SCAN_INTERVAL_SEC * (os.getpid() % 7) / 6.0
                await asyncio.sleep(RAYDIUM_SCAN_INTERVAL_SEC + jitter)
            backoff = 2.0
        except asyncio.CancelledError:
            log.event("SCAN_RAYDIUM_CANCELLED")
            raise
        except Exception as e:
            log.event("SCAN_RAYDIUM_CRASH", error=str(e))
            await asyncio.sleep(backoff)
            backoff = min(backoff * 1.7, 30.0)

async def _start_background_tasks(stop_evt: asyncio.Event) -> Dict[str, asyncio.Task]:
    tasks: Dict[str, asyncio.Task] = {}

    # Raydium scanner
    want_raydium = getattr(C, "ENABLE_RAYDIUM_SCANNER", True)
    pools_ok = (not POOLS_FILTER) or ("raydium" in POOLS_FILTER)
    if want_raydium and pools_ok:
        t = asyncio.create_task(_run_raydium_scanner_forever(stop_evt), name="scan.raydium")
        tasks["scan.raydium"] = t
        log.event("SCAN_RAYDIUM_SCHEDULED", task="scan.raydium")

    # Daily PnL rollup
    if DAILY_PNL_ENABLED:
        t = asyncio.create_task(_run_daily_pnl_loop(stop_evt), name="telemetry.daily_pnl")
        tasks["telemetry.daily_pnl"] = t
        log.event("DAILY_PNL_SCHEDULED", hour_utc=DAILY_PNL_HOUR_UTC)

    return tasks

async def _graceful_shutdown(stop_evt: asyncio.Event, sell_task: Optional[asyncio.Task], bg_tasks: Dict[str, asyncio.Task]) -> None:
    stop_evt.set()
    # Cancel background tasks first
    for _name, t in list(bg_tasks.items()):
        with contextlib.suppress(Exception):
            t.cancel()
    with contextlib.suppress(Exception):
        if sell_task:
            sell_task.cancel()
            await asyncio.wait_for(sell_task, timeout=3.0)
    with contextlib.suppress(Exception):
        if router and hasattr(router, "shutdown"):
            await router.shutdown()
    with contextlib.suppress(Exception):
        await pricefeed_close()

# ---------------------------
# Main
# ---------------------------
def _boot_banner() -> None:
    pools = ",".join(sorted(POOLS_FILTER)) if POOLS_FILTER else "<all>"
    effective_dryrun = _dryrun_env_on() or bool(_pkg_is_dryrun)
    pump_trades = _env_bool("PUMP_SUBSCRIBE_TRADES", True)
    print(
        "\n=== 🟢 sniper main boot ===\n"
        f"DRYRUN={effective_dryrun}\n"
        f"PF_DEBUG_TX={int(DEBUG_TX)} | PF_MAX_CONCURRENT={MAX_CONCURRENCY} | PF_COOLDOWN_SEC={LOOP_COOLDOWN_SEC}\n"
        f"MOMO: on={int(ENTRY_MOMENTUM_ENABLED)} window={ENTRY_MOMO_WINDOW_SEC}s "
        f"min_trades={ENTRY_MOMO_MIN_TRADES} min_up={ENTRY_MOMO_MIN_UP_PCT}%\n"
        f"LIQ_FILTER={int(LIQ_FILTER_ENABLED)} | EXIT_ENGINE={int(EXIT_ENGINE_ENABLED)} | MAX_TRADEABLE={MAX_TRADEABLE}\n"
        f"POOLS={pools} | PUMP_TRADES={int(pump_trades)}\n"
        "===========================\n",
        flush=True,
    )

async def main(stop_evt: Optional[asyncio.Event] = None) -> None:
    with contextlib.suppress(Exception):
        start_global_heartbeat(log, interval=float(os.getenv("PF_HEARTBEAT_SEC", "45")))
        bump("boot")

    _boot_banner()

    # Ensure wallet file exists (no-op if already present)
    _ensure_keypair_file()

    # Router initialization (best-effort)
    try:
        if router and hasattr(router, "start"):
            await router.start()
            log.event("ROUTER_PRIO_INIT_STARTED", ok=1)
    except Exception as e:
        log.event("ROUTER_PRIO_INIT_FAIL", error=str(e))

    # Start sell loop if positions exist
    sell_stop_evt: Optional[asyncio.Event] = None
    sell_task: Optional[asyncio.Task] = None
    try:
        if list(all_positions()):
            log.event("BOOT_START_SELL_LOOP", reason="existing_positions")
            sell_stop_evt = asyncio.Event()
            sell_task = asyncio.create_task(run_sell_loop(sell_stop_evt))
    except Exception as e:
        log.event("BOOT_SELL_LOOP_START_FAIL", error=str(e))

    local_stop = stop_evt or asyncio.Event()

    # Start scanners/background tasks
    bg_tasks = await _start_background_tasks(local_stop)

    # Signal handlers (best-effort; Windows may not support add_signal_handler)
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, lambda s=sig: asyncio.create_task(_graceful_shutdown(local_stop, sell_task, bg_tasks)))
        except NotImplementedError:
            pass

    backoff = 3
    try:
        while not local_stop.is_set():
            try:
                await stream_loop(local_stop)
                print("[main] stream ended (remote closed). Reconnecting...", flush=True)
                backoff = 3
            except asyncio.CancelledError:
                raise
            except Exception:
                print("[main] stream_loop crashed:", flush=True)
                traceback.print_exc()
                backoff = min(backoff * 2, 30)
            await asyncio.sleep(backoff)
    finally:
        try:
            if sell_stop_evt is not None:
                sell_stop_evt.set()
            if sell_task is not None:
                with contextlib.suppress(Exception):
                    await asyncio.wait_for(sell_task, timeout=2.5)
        except Exception:
            pass
        await _graceful_shutdown(local_stop, sell_task, bg_tasks)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        sys.exit(0)
