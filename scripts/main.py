from __future__ import annotations

"""
scripts/main.py (vNext + DSN-normalizer + full buy hooks + healthcheck + sentry)

- Hardens startup, logging, and shutdown.
- Adds event-rate diagnostics + momentum gate + guardrails.
- Enforces PF_POOLS filter and supervises the Raydium scanner with backoff+jitter.
- Local Jupiter relay compatible (removes Brotli decode issues upstream).
- **Daily PnL rollup**: once/day Telegram message + row in PG table `daily_pnl`.
- **DRYRUN enforcement** before any buy (IS_DRYRUN/DRYRUN + package flag).
- **DSN normalizer** so asyncpg always gets postgresql:// (never postgresql+asyncpg://).
- **BUY HOOKS**: passes coroutines to utils_pkg.pump_trader.maybe_buy:
      - on_buy_submitted   (fires when swap submitted)
      - on_buy_filled      (fires when swap confirmed)  -> forwards to exit engine
      - on_buy_failed      (fires when swap fails)
- Optional **/health** endpoint (aiohttp present + HEALTH_PORT set).
- Optional **Sentry** init (SENTRY_DSN).
- Degrades gracefully if optional dependencies are missing.

NEW in this edition:
- Notion daily sync integrated into daily-PnL loop
- Debounced intraday Notion sync on BUY_FILLED (default 10 min)

HOTFIX:
- Force-wire maybe_buy() to the router’s implementation used in shell tests.

PATCHES IN THIS BUILD:
- PumpPortalClient no longer called with `subscribe_trades=` (version-safe).
  We subscribe via `client._subscribe("trades")` or other public methods if available.
- When router has no buy-capable function, we log `ROUTER_NO_BUY_CAPABLE` with context.
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
from utils_pkg.core_utils import AlertManager
from asyncio import create_task

# ---------------------------
# Optional deps (Telegram / PG / Sentry / Health)
# ---------------------------
try:
    import httpx
except Exception:
    httpx = None  # type: ignore

try:
    import asyncpg
except Exception:
    asyncpg = None  # type: ignore

try:
    import sentry_sdk  # type: ignore
except Exception:
    sentry_sdk = None  # type: ignore

try:
    from aiohttp import web  # type: ignore
except Exception:
    web = None  # type: ignore

# ---------------------------
# Config module (feature flags)
# ---------------------------
try:
    C = importlib.import_module("config")
except Exception:  # pragma: no cover
    class _C:
        ENABLE_RAYDIUM_SCANNER = True
    C = _C()  # type: ignore

# ---------------------------
# Logging (robust shimmed version)
# ---------------------------
try:
    from logging_setup import get_logger, bump, start_global_heartbeat
    log = get_logger()

    # ── ensure log.event + log.every exist ───────────────────────────────
    if not hasattr(log, "event"):
        def _event(name: str, **fields):
            """
            Unified event logger shim.
            Writes as INFO with an event-style message and structured fields.
            """
            log.info(name, extra=fields)
        log.event = _event  # type: ignore[attr-defined]

    if not hasattr(log, "every"):
        def _every(_key: str, n: int = 60, **kw):
            """Rate-limited logging shim fallback (no-op if not implemented)."""
            log.info(f"[every {n}s] {_key}", extra=kw)
        log.every = _every  # type: ignore[attr-defined]

except Exception as e:
    # ── emergency fallback: no logging_setup available ───────────────────
    class _Dummy:
        def _p(self, lvl: str, *a, **k):
            msg = f"[{lvl}] " + " ".join(str(x) for x in a)
            if k:
                msg += " " + str(k)
            print(msg, flush=True)

        def event(self, *a, **k):  self._p("event", *a, **k)
        def info(self, *a, **k):   self._p("info", *a, **k)
        def warning(self, *a, **k):self._p("warn", *a, **k)
        def error(self, *a, **k):  self._p("error", *a, **k)
        def every(self, *a, **k):  self._p("every", *a, **k)

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

# --- Unified env-bool helper (same semantics as lightning_impl.env_bool) ---
def _env_bool(key: str, default: bool=False) -> bool:
    v = os.getenv(key)
    if v is None:
        return default
    return str(v).strip().lower() in ("1","true","t","yes","y","on")

def _dryrun_env_on() -> bool:
    # honor any of these
    return _env_bool("DRYRUN") or _env_bool("DRY_RUN") or _env_bool("IS_DRYRUN")

# ---------------------------
# Event source + trading hooks
# ---------------------------
try:
    from utils_pkg.pumpportal_client import PumpPortalClient
except Exception:  # pragma: no cover
    class PumpPortalClient:  # type: ignore
        def __init__(self, *a, **kw):
            raise RuntimeError("PumpPortalClient not available")

# NOTE: We intentionally DO NOT import maybe_buy from pump_trader here.
# We wire maybe_buy() directly to the router so it always matches whatever impls exist.

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
    from utils_pkg import router  # provides start()/shutdown() and buy function(s)
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

# ---------- HOTFIX: wire maybe_buy to router ----------
import inspect

def _safe_log_router_state(event: str, **fields: Any) -> None:
    try:
        log.event(event, **fields)
    except Exception:
        pass

def _candidate_buy_names() -> list[str]:
    return [
        "maybe_buy",
        "buy",
        "route_and_buy",
        "buy_on_jupiter",
        "buy_token",
        "execute_buy",
        "exec_buy",
        "buy_via_lightning",
        "_buy_lightning",
        "buy_lightning",
        "buy_direct",
        "buy_pump",
    ]

def _pick_router_buy_callable() -> Optional[tuple[str, Any]]:
    if router is None:
        return None
    for name in _candidate_buy_names():
        fn = getattr(router, name, None)
        if callable(fn):
            return name, fn
    try:
        names = [n for n in dir(router) if "buy" in n.lower()]
        for n in names:
            if any(bad in n.lower() for bad in ("sell", "quote")):
                continue
            fn = getattr(router, n, None)
            if callable(fn):
                return n, fn
    except Exception:
        pass
    return None

def _compute_spend_sol(ui: Dict[str, Any]) -> float:
    for k in ("spend_sol", "buy_size_sol", "amount_sol"):
        try:
            v = float(ui.get(k)) if ui.get(k) is not None else None
            if v and v > 0:
                return v
        except Exception:
            pass
    for envk in ("ENTRY_BUY_SOL", "ENTRY_SPEND_SOL", "INITIAL_BUY_SOL"):
        try:
            v = float(os.getenv(envk, "0"))
            if v > 0:
                return v
        except Exception:
            pass
    return 0.02

def _env_slippage_bps() -> int:
    try:
        return int(os.getenv("SLIPPAGE_BPS", os.getenv("ENTRY_SLIPPAGE_BPS", "50")))
    except Exception:
        return 50

def _build_kwargs_for_router(fn: Any, mint: str, ui: Dict[str, Any],
                             on_buy_submitted, on_buy_filled, on_buy_failed) -> Dict[str, Any]:
    kwargs: Dict[str, Any] = {}
    try:
        sig = inspect.signature(fn)
        params = sig.parameters
        names = set(params.keys())

        if "mint" in names:
            kwargs["mint"] = mint
        if "ui" in names:
            kwargs["ui"] = ui

        spend = _compute_spend_sol(ui)
        if "amount_sol" in names:
            kwargs["amount_sol"] = float(spend)
        if "lamports_in" in names:
            kwargs["lamports_in"] = int(round(spend * 1_000_000_000))

        if "slippage_bps" in names:
            kwargs["slippage_bps"] = _env_slippage_bps()

        if "only_direct" in names:
            kwargs["only_direct"] = bool(os.getenv("ONLY_DIRECT", "0") in {"1","true","yes","on"})

        if "on_buy_submitted" in names:
            kwargs["on_buy_submitted"] = on_buy_submitted
        if "on_buy_filled" in names:
            kwargs["on_buy_filled"] = on_buy_filled
        if "on_buy_failed" in names:
            kwargs["on_buy_failed"] = on_buy_failed
    except Exception:
        kwargs = {"mint": mint, "ui": ui}
    return kwargs

async def _call_with_variants(fn: Any, *, mint: str, ui: Dict[str, Any],
                              on_buy_submitted, on_buy_filled, on_buy_failed):
    try:
        kw_sig = _build_kwargs_for_router(fn, mint, ui, on_buy_submitted, on_buy_filled, on_buy_failed)
        res = fn(**kw_sig)
        if inspect.isawaitable(res):
            res = await res
        _safe_log_router_state("ROUTER_BUY_CALL", variant="kw_sig", sent_keys=sorted(kw_sig.keys()))
        return res
    except TypeError as e:
        _safe_log_router_state("ROUTER_BUY_CALL_TYPERR", where="kw_sig", err=str(e))
    except Exception as e:
        _safe_log_router_state("ROUTER_BUY_CALL_ERR", where="kw_sig", err=str(e))

    try:
        res = fn(mint=mint, ui=ui)
        if inspect.isawaitable(res):
            res = await res
        _safe_log_router_state("ROUTER_BUY_CALL", variant="kw_min_mint_ui")
        return res
    except Exception as e:
        _safe_log_router_state("ROUTER_BUY_CALL_ERR", where="kw_min_mint_ui", err=str(e))

    try:
        res = fn(mint=mint)
        if inspect.isawaitable(res):
            res = await res
        _safe_log_router_state("ROUTER_BUY_CALL", variant="kw_mint_only")
        return res
    except Exception as e:
        _safe_log_router_state("ROUTER_BUY_CALL_ERR", where="kw_mint_only", err=str(e))

    _safe_log_router_state("ROUTER_BUY_ALL_VARIANTS_FAILED", error="no compatible kwargs")
    return None

async def maybe_buy(
    *, mint: str, ui: Dict[str, Any],
    on_buy_submitted=None, on_buy_filled=None, on_buy_failed=None
) -> Optional[str]:
    if router is None:
        _safe_log_router_state("ROUTER_UNAVAILABLE_FOR_BUY")
        raise RuntimeError("router unavailable")

    choice = _pick_router_buy_callable()
    if not choice:
        _safe_log_router_state(
            "ROUTER_NO_BUY_CAPABLE",
            impls=os.getenv("ROUTER_IMPLS"),
            buy_keys=getattr(router, "buy_keys", None),
            sell_keys=getattr(router, "sell_keys", None),
            file=getattr(router, "__file__", "<router?>"),
        )
        raise RuntimeError("router has no buy-capable function")

    fn_name, fn = choice
    _safe_log_router_state(
        "ROUTER_BUY_FN",
        fn=fn_name,
        impls=os.getenv("ROUTER_IMPLS"),
        file=getattr(router, "__file__", "<router?>"),
    )

    res = await _call_with_variants(
        fn, mint=mint, ui=ui,
        on_buy_submitted=on_buy_submitted,
        on_buy_filled=on_buy_filled,
        on_buy_failed=on_buy_failed,
    )

    if isinstance(res, str):
        return res
    if isinstance(res, dict):
        for k in ("signature", "sig", "tx_sig", "tx", "id"):
            v = res.get(k)
            if isinstance(v, str) and v:
                return v
    return None

# ---------- /HOTFIX ----------

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

# Optional Telegram text sender (for buy hooks)
try:
    from utils_pkg.telegram_utils import send_text as _tg_send_text  # type: ignore
except Exception:  # pragma: no cover
    _tg_send_text = None  # type: ignore

# Raydium scanner (lazy import)
def _get_raydium_scanner():
    try:
        from scanner.raydium_scanner import scan_raydium  # type: ignore
        return scan_raydium
    except Exception:
        return None

# --- Solana tx confirmation helpers (keyword-only friendly) ---
import json as _json
import httpx as _httpx

def _rpc_url() -> str:
    return os.getenv("SOLANA_RPC_URL") or os.getenv("RPC_URL") or "https://api.mainnet-beta.solana.com"

def _confirm_commitment() -> str:
    return os.getenv("CONFIRM_COMMITMENT", "confirmed")

def _confirm_timeout_sec() -> float:
    try:
        return float(os.getenv("CONFIRM_TIMEOUT_SEC", "25"))
    except Exception:
        return 25.0

async def _get_sig_status(sig: str) -> Optional[dict]:
    body = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getSignatureStatuses",
        "params": [[sig], {"searchTransactionHistory": True}],
    }
    async with _httpx.AsyncClient(timeout=10) as c:
        r = await c.post(_rpc_url(), json=body)
        r.raise_for_status()
        j = r.json()
        val = (j.get("result") or {}).get("value") or []
        return (val[0] if val else None)

async def _confirm_sig(sig: str) -> tuple[bool, Optional[str]]:
    import time as _time
    deadline = _time.monotonic() + _confirm_timeout_sec()
    want = _confirm_commitment()
    ok_states = {"confirmed", "finalized"}

    while _time.monotonic() < deadline:
        st = await _get_sig_status(sig)
        if st:
            err = st.get("err")
            conf = (st.get("confirmationStatus") or "").lower()
            slot = st.get("slot")
            if err is None and conf in ok_states:
                if want == "finalized" and conf != "finalized":
                    await asyncio.sleep(0.35)
                    continue
                return True, str(slot)
            if err is not None:
                return False, f"rpc_error:{err}"
        await asyncio.sleep(0.35)

    return False, "timeout"

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

# ---------------------------
# DSN normalizer
# ---------------------------
def _dsn_to_asyncpg(dsn: str | None) -> str:
    if not dsn:
        return ""
    s = dsn.strip()
    if not s:
        return ""
    s = s.replace("postgres+asyncpg://", "postgres://")
    s = s.replace("postgresql+asyncpg://", "postgresql://")
    if s.lower().startswith("postgresql+asyncpg://"):
        s = "postgresql://" + s.split("://", 1)[1]
    if s.lower().startswith("postgres+asyncpg://"):
        s = "postgres://" + s.split("://", 1)[1]
    return s

def _pick_env_dsn() -> tuple[str, str, str]:
    raw_candidates = [
        os.getenv("PNL_DSN"),
        os.getenv("TELEMETRY_PG_DSN"),
        os.getenv("PG_DSN"),
        os.getenv("DATABASE_URL"),
        os.getenv("POSITIONS_PG_DSN"),
        os.getenv("PG_DSN_INTERNAL"),
        os.getenv("PG_DSN_EXTERNAL"),
    ]
    pnl_raw = next((x for x in raw_candidates if x and x.strip()), "") or ""
    pg_raw  = (os.getenv("PG_DSN") or os.getenv("DATABASE_URL") or os.getenv("PG_DSN_INTERNAL")
               or os.getenv("PG_DSN_EXTERNAL") or os.getenv("POSITIONS_PG_DSN") or "").strip()

    pnl_async = _dsn_to_asyncpg(pnl_raw)
    pg_async  = _dsn_to_asyncpg(pg_raw)

    def _mask(s: str) -> str:
        if not s:
            return ""
        try:
            body = s.split("://", 1)[1]
            after_at = body.split("@", 1)[-1]
            host_db  = after_at.split("?", 1)[0]
            return host_db
        except Exception:
            return "<dsn>"

    return pnl_async, pg_async, (_mask(pnl_async) or _mask(pg_async) or "")

_PNL_DSN_ASYNC, _PG_DSN_ASYNC, _PG_DSN_MASK = _pick_env_dsn()

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
DAILY_PNL_DEBOUNCE_MIN = _as_int(os.getenv("DAILY_PNL_DEBOUNCE_MIN", "10"), 10)

# Scanner + dedupe tuning
RAYDIUM_SCAN_INTERVAL_SEC = _as_float(os.getenv("RAYDIUM_SCAN_INTERVAL_SEC", "30"), 30.0)
PF_GATE_TTL_SEC     = _as_int(os.getenv("PF_GATE_TTL_SEC", str(max(_as_int(os.getenv("ENTRY_MOMO_WINDOW_SEC","10"),10)*2, 60))), max(ENTRY_MOMO_WINDOW_SEC*2, 60))
PF_BUY_TTL_SEC      = _as_int(os.getenv("PF_BUY_TTL_SEC", "600"), 600)

# Buy-hook Telegram toggles (optional)
NOTIFY_BUY_SUBMITTED = _as_bool(os.getenv("TELEGRAM_NOTIFY_SUBMITTED", "0"), False)
NOTIFY_BUY_FAILED    = _as_bool(os.getenv("TELEGRAM_NOTIFY_FAILED", "0"), False)
NOTIFY_BUY_FILLED    = _as_bool(os.getenv("TELEGRAM_NOTIFY_FILLED", "0"), False)

# Health / Sentry
HEALTH_PORT = _as_int(os.getenv("HEALTH_PORT", "0"), 0)
SENTRY_DSN  = (os.getenv("SENTRY_DSN") or "").strip()

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

def utc_today_iso() -> str:
    return _dt.datetime.now(_dt.timezone.utc).date().isoformat()

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
    if not is_raydium and typ != "create":
        return False

    init_buy = _as_float(_get_first(msg, "initialBuy", "initial_buy", default=0.0), 0.0)
    if not is_raydium and init_buy < _as_float(os.getenv("ENTRY_MIN_BUY_SOL", "0.0"), 0.0):
        if DEBUG_TX:
            print(f".[filter] initialBuy {init_buy} < min — skip", flush=True)
        return False

    sym = _extract_symbol(msg).upper()
    if any(bad and bad in sym for bad in {
        s.strip().upper()
        for s in os.getenv("ENTRY_SYMBOL_BLACKLIST", "RUG,SCAM").split(",")
        if s.strip()
    }):
        if DEBUG_TX:
            print(f".[filter] symbol '{sym}' hits blacklist — skip", flush=True)
        return False

    nm = _extract_name(msg)
    if (m := _as_int(os.getenv("ENTRY_MAX_NAME_LEN", "40"), 40)) > 0 and len(nm) > m:
        if DEBUG_TX:
            print(f".[filter] name too long ({len(nm)} > {m}) — skip", flush=True)
        return False

    mins_since_token = _approx_minutes_since(
        _get_first(msg, "created_at", "createdAt", "timestamp", "ts", default=None)
    )
    if mins_since_token is not None and _as_int(os.getenv("ENTRY_MAX_AGE_MIN", "20"), 20) > 0 and \
       mins_since_token > _as_int(os.getenv("ENTRY_MAX_AGE_MIN", "20"), 20):
        if DEBUG_TX:
            print(f".[filter] token age {mins_since_token:.1f}m — skip", flush=True)
        return False

    mins_since_pool = _approx_minutes_since(_get_first(msg, "poolCreatedAt","pool_created_at", default=None))
    if mins_since_pool is not None and _as_int(os.getenv("MIN_POOL_AGE_MIN", "3"), 3) > 0 and \
       mins_since_pool < _as_int(os.getenv("MIN_POOL_AGE_MIN", "3"), 3):
        if DEBUG_TX:
            print(f".[filter] pool age {mins_since_pool:.1f}m — skip", flush=True)
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
# Liquidity retry helper
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
# BUY HOOKS (submitted / filled / failed)
# ---------------------------
from asyncio import create_task
from utils_pkg.core_utils import AlertManager

# env toggles (safe defaults)
NOTIFY_BUY_SUBMITTED = _as_bool(os.getenv("NOTIFY_BUY_SUBMITTED", "1"), True)
NOTIFY_BUY_FILLED    = _as_bool(os.getenv("NOTIFY_BUY_FILLED", "1"), True)
NOTIFY_BUY_FAILED    = _as_bool(os.getenv("NOTIFY_BUY_FAILED", "1"), True)

# cheap singleton to avoid re-creating AlertManager
_AM_SINGLETON: Optional[AlertManager] = None
def _am() -> AlertManager:
    global _AM_SINGLETON
    if _AM_SINGLETON is None:
        _AM_SINGLETON = AlertManager()
    return _AM_SINGLETON

async def _buy_hook_submitted(payload: Dict[str, Any]) -> None:
    """
    payload: {mint, pool, symbol, spend_sol, route, hint}
    """
    try:
        bump("buys_submitted")
    except Exception:
        pass

    try:
        log.event("BUY_SUBMITTED",
                  mint=payload.get("mint"),
                  symbol=payload.get("symbol"),
                  pool=payload.get("pool"),
                  route=payload.get("route"),
                  spend_sol=payload.get("spend_sol"),
                  hint=payload.get("hint"))
    except Exception:
        pass

    # Telegram/alerts (lightweight text on submission)
    if NOTIFY_BUY_SUBMITTED:
        try:
            txt = (
                f"🟡 Submitted BUY {payload.get('symbol') or ''}\n"
                f"mint={payload.get('mint')}  route={payload.get('route') or 'auto'}  "
                f"size={payload.get('spend_sol') or '—'} SOL"
            )
            create_task(_am().notify_text(txt))
        except Exception as _e:
            log.warning("ALERT_BUY_SUBMIT_FAIL", extra={"err": str(_e)[:200]})

# --- Notion (optional daily PnL sync) ---
try:
    from utils_pkg.notion_pnl import push_daily_pnl  # safe no-op if NOTION_ENABLED!=1
except Exception:  # pragma: no cover
    def push_daily_pnl(_snapshot):  # fallback no-op
        return None

# Debounce for intraday push after fills (seconds granularity)
_LAST_NOTION_PUSH_TS: float = 0.0

async def _notion_intraday_snapshot_push(note: str = "") -> None:
    """Lightweight intraday push to Notion after a buy fill (debounced)."""
    import time as _time
    global _LAST_NOTION_PUSH_TS
    try:
        if os.getenv("NOTION_ENABLED", "0") != "1":
            return
        debounce_sec = int(os.getenv("DAILY_PNL_DEBOUNCE_MIN", "10")) * 60
        now = _time.time()
        if (now - _LAST_NOTION_PUSH_TS) < debounce_sec:
            return

        # very small snapshot (no price walk); safe + fast
        unrl, open_n, _ = await _estimate_unrealized_from_positions()
        realized, _ = await _estimate_realized_today()
        wallet = await _get_wallet_sol_balance()

        from datetime import date as _date
        sn = {
            "date": _date.today().isoformat(),
            "realized_sol": realized,
            "unrealized_sol": unrl,
            "open_positions": open_n,
            "wallet_sol": wallet,
            "note": note or "auto intraday snapshot",
        }
        with contextlib.suppress(Exception):
            push_daily_pnl(sn)
        _LAST_NOTION_PUSH_TS = now
    except Exception as e:
        log.event("NOTION_INTRADAY_PUSH_FAIL", err=str(e))

async def _buy_hook_filled(ev: Dict[str, Any]) -> None:
    """
    ev: { mint, signature, symbol, spend_sol, pool, amount_raw? }
    """
    if _dryrun_env_on():
        return

    try:
        bump("buys_filled")
    except Exception:
        pass

    try:
        log.event("BUY_FILLED",
                  mint=ev.get("mint"),
                  symbol=ev.get("symbol"),
                  pool=ev.get("pool"),
                  spend_sol=ev.get("spend_sol"),
                  sig=ev.get("signature"))
    except Exception:
        pass

    # Primary alert path (AlertManager)
    if NOTIFY_BUY_FILLED:
        try:
            create_task(_am().notify_trade_fill(
                side="BUY",
                mint=ev.get("mint"),
                symbol=ev.get("symbol"),
                pool=ev.get("pool"),
                sig=ev.get("signature"),
                spend_sol=ev.get("spend_sol"),
                out_amount=ev.get("amount_raw"),   # if present
            ))
        except Exception as _e:
            log.warning("ALERT_BUY_FILLED_FAIL", extra={"err": str(_e)[:200]})

    # Kick an intraday Notion push (debounced)
    asyncio.create_task(_notion_intraday_snapshot_push())

    # Forward to exit engine (supports both new and legacy signatures)
    if not EXIT_ENGINE_ENABLED:
        return
    try:
        await exit_engine.on_buy_filled(ev)  # type: ignore[arg-type]
        return
    except TypeError:
        with contextlib.suppress(Exception):
            await exit_engine.on_buy_filled(raw_id_or_mint=ev.get("mint"),
                                            tx_sig=ev.get("signature"))  # type: ignore[misc]
    except Exception as e:
        log.warning("EXIT_ENGINE_FILL_HOOK_FAIL %s", e)

async def _buy_hook_failed(payload: Dict[str, Any]) -> None:
    """
    payload: {mint, pool, symbol, spend_sol, route, hint, reason, error}
    """
    try:
        log.event("BUY_FAILED",
                  mint=payload.get("mint"),
                  symbol=payload.get("symbol"),
                  pool=payload.get("pool"),
                  route=payload.get("route"),
                  spend_sol=payload.get("spend_sol"),
                  reason=payload.get("reason"),
                  error=payload.get("error"))
    except Exception:
        pass

    if NOTIFY_BUY_FAILED:
        try:
            txt = (
                f"🔴 BUY FAILED {payload.get('symbol') or ''}\n"
                f"mint={payload.get('mint')}  route={payload.get('route') or 'auto'}\n"
                f"reason={payload.get('reason') or '—'}  err={payload.get('error') or '—'}"
            )
            create_task(_am().notify_text(txt))
        except Exception as _e:
            log.warning("ALERT_BUY_FAILED_FAIL", extra={"err": str(_e)[:200]})

    # Optional telemetry
    try:
        record_buy_result(ok=False, reason=str(payload.get("reason") or "failed"), error=payload.get("error"))
    except Exception:
        pass

# ---------------------------
# Strategy
# ---------------------------
class Strategy:
    def __init__(self) -> None:
        self._sema = asyncio.Semaphore(max(1, _as_int(os.getenv("PF_MAX_CONCURRENT", "2"), 2)))
        self._gate_sema = asyncio.Semaphore(max(1, _as_int(os.getenv("ENTRY_MOMENTUM_MAX_PARALLEL", os.getenv("ENTRY_MOMO_MAX_PARALLEL","6")), 6)))
        self._gate_state: Dict[str, Dict[str, Any]] = {}
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
        if _as_bool(os.getenv("EXIT_ENGINE_ENABLED", "1"), True):
            try:
                await exit_engine.on_trade_tick(raw_event)
            except Exception:
                if _as_bool(os.getenv("PF_DEBUG_TX", "0"), False):
                    traceback.print_exc()
        m = str(_get_first(raw_event, "mint", "mintAddress", "mint_address", "id", default="")).strip()
        if not m:
            return
        st = self._gate_state.get(m)
        if st is not None:
            st["trades"] = int(st.get("trades", 0)) + 1

    async def _gate_candidate(self, ui: Dict[str, Any], mint_raw: str, mint_norm: str) -> None:
        DEBUG_TX = _as_bool(os.getenv("PF_DEBUG_TX", "0"), False)
        PF_GATE_TTL_SEC = _as_int(os.getenv("PF_GATE_TTL_SEC", "60"), 60)
        ENTRY_MOMO_WINDOW_SEC = _as_int(os.getenv("ENTRY_MOMO_WINDOW_SEC", "10"), 10)
        ENTRY_MOMO_PRICE_TRIES = _as_int(os.getenv("ENTRY_MOMO_PRICE_TRIES", "4"), 4)
        ENTRY_MOMO_PRICE_DELAY_S = _as_float(os.getenv("ENTRY_MOMO_PRICE_DELAY_S", "0.7"), 0.7)
        ENTRY_MOMO_MIN_TRADES = _as_int(os.getenv("ENTRY_MOMO_MIN_TRADES", "6"), 6)
        ENTRY_MOMO_MIN_UP_PCT = _as_float(os.getenv("ENTRY_MOMO_MIN_UP_PCT", "5"), 5.0)
        IS_DRYRUN = (_dryrun_env_on() or _pkg_is_dryrun)

        async with self._gate_sema:
            if _as_bool(os.getenv("PF_DEDUPE_ENABLED", "1"), True):
                if not claim_once(f"gate:{mint_norm}", ttl=int(PF_GATE_TTL_SEC)):
                    if DEBUG_TX:
                        print(f".[dedupe] gate already running for {mint_norm} — skip", flush=True)
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

            # Guardrails
            if not buys_allowed() or _as_bool(os.getenv("PAUSE_TRADING", "0"), False):
                if DEBUG_TX:
                    print("[guardrails] buys not allowed at gate decision — skip", flush=True)
                record_buy_result(ok=False, reason="guardrails_halt")
                self._diag["rejected_guardrails"] += 1
                return

            max_tradeable = _as_int(os.getenv("MAX_TRADEABLE", "0"), 0)
            if max_tradeable > 0:
                try:
                    if len(list(all_positions())) >= max_tradeable:
                        log.event("BUY_BLOCK_MAX_TRADEABLE", cap=max_tradeable)
                        record_buy_result(ok=False, reason="max_tradeable")
                        self._diag["rejected_guardrails"] += 1
                        return
                except Exception:
                    pass

            if trades_ok and up_ok:
                if _as_bool(os.getenv("PF_DEDUPE_ENABLED", "1"), True) and not claim_once(f"buy:{mint_norm}", ttl=int(_as_int(os.getenv("PF_BUY_TTL_SEC", "600"), 600))):
                    if DEBUG_TX:
                        print(f".[dedupe] buy already claimed for {mint_norm} — skip", flush=True)
                    record_buy_result(ok=False, reason="buy_claimed_elsewhere")
                    return

                # *** KEY CHANGE: do NOT short-circuit for DRYRUN here. ***
                try:
                    async with self._sema:
                        tx_sig = await maybe_buy(
                            mint=mint_norm,
                            ui=ui | {"dryrun": IS_DRYRUN},
                            on_buy_submitted=_buy_hook_submitted,
                            on_buy_filled=_buy_hook_filled,
                            on_buy_failed=_buy_hook_failed,
                        )
                    self._diag["buys_submitted"] += 1

                    if tx_sig:
                        try:
                            await _buy_hook_submitted({"mint": mint_norm, **ui})
                        except Exception:
                            pass

                        ok, info = await _confirm_sig(tx_sig)
                        if ok:
                            # Keep this log (your earlier logs show BUY_FILLED at ~1219)
                            log.info("BUY_FILLED", extra={"mint": mint_norm, "tx_sig": tx_sig, "slot": info})

                            # Fire alert for BUY_FILLED here as well (belt & suspenders)
                            try:
                                create_task(_am().notify_trade_fill(
                                    side="BUY",
                                    mint=mint_norm,
                                    symbol=ui.get("symbol"),
                                    pool=ui.get("pool") or "unknown",
                                    sig=tx_sig,
                                    spend_sol=ui.get("spend_sol"),
                                    out_amount=ui.get("amount_raw"),
                                ))
                            except Exception as _e:
                                log.warning("ALERT_BUY_FILLED_FAIL", extra={"err": str(_e)[:200]})

                            try:
                                await exit_engine.on_buy_filled(mint=mint_norm, ui=ui, tx_sig=tx_sig)
                            except Exception:
                                pass
                            self._diag["buys_filled"] += 1
                            record_buy_result(ok=True, reason="executed", sig=tx_sig)
                            asyncio.create_task(_post_buy_exit_test(mint_norm, tx_sig))
                        else:
                            log.error("BUY_FAILED", extra={"mint": mint_norm, "tx_sig": tx_sig, "reason": info})
                            try:
                                await _buy_hook_failed({"mint": mint_norm, "reason": info, **ui})
                            except Exception:
                                pass
                            record_buy_result(ok=False, reason=info)
                    else:
                        # DRYRUN path naturally returns None from the impl — treat distinctly.
                        if IS_DRYRUN:
                            log.event("BUY_DRYRUN_EXIT", mint=mint_norm)
                            record_buy_result(ok=False, reason="dryrun_exit")
                        else:
                            record_buy_result(ok=False, reason="no_tx_sig")

                except Exception:
                    traceback.print_exc()
                    record_buy_result(ok=False, reason="exception")

            else:
                record_buy_result(ok=False, reason="momo_reject")
                self._diag["rejected_momo"] += 1

    async def on_new_token(self, raw_event: Dict[str, Any]) -> None:
        DEBUG_TX = _as_bool(os.getenv("PF_DEBUG_TX", "0"), False)
        IS_DRYRUN = (_dryrun_env_on() or _pkg_is_dryrun)

        self._diag["events_total"] += 1
        self._diag["events_new_token"] += 1

        if _as_bool(os.getenv("PAUSE_TRADING", "0"), False) or not buys_allowed():
            if DEBUG_TX:
                print("[guardrails] buys not allowed now — skip", flush=True)
            self._diag["rejected_guardrails"] += 1
            return

        max_tradeable = _as_int(os.getenv("MAX_TRADEABLE", "0"), 0)
        if max_tradeable > 0:
            try:
                if len(list(all_positions())) >= max_tradeable:
                    log.event("BUY_BLOCK_MAX_TRADEABLE", cap=max_tradeable)
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
        if _as_bool(os.getenv("ENTRY_LIQ_FILTER", "1"), True):
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

        print(f"[trade] pool={ui.get('pool','?')} symbol={(ui.get('symbol') or '?')} mint={mint_norm}", flush=True)

        if _as_bool(os.getenv("PF_DEDUPE_ENABLED", "1"), True) and _as_bool(os.getenv("ENTRY_MOMENTUM_ENABLED", "1"), True):
            if not claim_once(f"gate-propose:{mint_norm}", ttl=int(_as_int(os.getenv("PF_GATE_TTL_SEC", "60"), 60))):
                if DEBUG_TX:
                    print(f".[dedupe] gate-propose taken for {mint_norm} — skip", flush=True)
                return

        if _as_bool(os.getenv("ENTRY_MOMENTUM_ENABLED", "1"), True):
            asyncio.create_task(self._gate_candidate(ui, mint_source, mint_norm))
        else:
            if _as_bool(os.getenv("PF_DEDUPE_ENABLED", "1"), True) and not claim_once(f"buy:{mint_norm}", ttl=int(_as_int(os.getenv("PF_BUY_TTL_SEC", "600"), 600))):
                if DEBUG_TX:
                    print(f".[dedupe] buy already claimed for {mint_norm} — skip", flush=True)
                return

            # *** KEY CHANGE: do NOT short-circuit for DRYRUN here. ***
            async with self._sema:
                try:
                    if _as_bool(os.getenv("PAUSE_TRADING", "0"), False) or not buys_allowed():
                        if DEBUG_TX:
                            print("[guardrails] buys not allowed at execution — skip", flush=True)
                        self._diag["rejected_guardrails"] += 1
                        return

                    max_tradeable = _as_int(os.getenv("MAX_TRADEABLE", "0"), 0)
                    if max_tradeable > 0:
                        try:
                            if len(list(all_positions())) >= max_tradeable:
                                log.event("BUY_BLOCK_MAX_TRADEABLE", cap=max_tradeable)
                                self._diag["rejected_guardrails"] += 1
                                return
                        except Exception:
                            pass

                    tx_sig = await maybe_buy(
                        mint=mint_norm,
                        ui=ui | {"dryrun": IS_DRYRUN},
                        on_buy_submitted=_buy_hook_submitted,
                        on_buy_filled=_buy_hook_filled,
                        on_buy_failed=_buy_hook_failed,
                    )
                    self._diag["buys_submitted"] += 1

                    if tx_sig:
                        try:
                            await _buy_hook_submitted({"mint": mint_norm, **ui})
                        except Exception:
                            pass

                        ok, info = await _confirm_sig(tx_sig)
                        if ok:
                            log.info("BUY_FILLED", extra={"mint": mint_norm, "tx_sig": tx_sig, "slot": info})

                            try:
                                create_task(_am().notify_trade_fill(
                                    side="BUY",
                                    mint=mint_norm,
                                    symbol=ui.get("symbol"),
                                    pool=ui.get("pool") or "unknown",
                                    sig=tx_sig,
                                    spend_sol=ui.get("spend_sol"),
                                    out_amount=ui.get("amount_raw"),
                                ))
                            except Exception as _e:
                                log.warning("ALERT_BUY_FILLED_FAIL", extra={"err": str(_e)[:200]})

                            try:
                                await exit_engine.on_buy_filled(mint=mint_norm, ui=ui, tx_sig=tx_sig)
                            except Exception:
                                pass
                            self._diag["buys_filled"] += 1
                            record_buy_result(ok=True, reason="executed", sig=tx_sig)
                            asyncio.create_task(_post_buy_exit_test(mint_norm, tx_sig))
                        else:
                            log.error("BUY_FAILED", extra={"mint": mint_norm, "tx_sig": tx_sig, "reason": info})
                            try:
                                await _buy_hook_failed({"mint": mint_norm, "reason": info, **ui})
                            except Exception:
                                pass
                            record_buy_result(ok=False, reason=info)
                    else:
                        if IS_DRYRUN:
                            log.event("BUY_DRYRUN_EXIT", mint=mint_norm)
                            record_buy_result(ok=False, reason="dryrun_exit")
                        else:
                            record_buy_result(ok=False, reason="no_tx_sig")
                except Exception:
                    print("[strategy] maybe_buy exception:", flush=True)
                    traceback.print_exc()
                    record_buy_result(ok=False, reason="exception")

        await asyncio.sleep(_as_float(os.getenv("PF_COOLDOWN_SEC", "15"), 15.0))

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
# Daily PnL helpers  (USES NORMALIZED DSN)
# ---------------------------
def _fmt_sol(x: Optional[float]) -> str:
    if x is None:
        return "—"
    try:
        return f"{x:.4f} SOL"
    except Exception:
        return str(x)

async def _telegram_send(text: str) -> None:
    if not ((os.getenv("TELEGRAM_BOT_TOKEN") or "").strip() and (os.getenv("DAILY_PNL_CHAT_ID") or os.getenv("TELEGRAM_CHAT_ID") or "").strip()) or not httpx:
        log.event("DAILY_PNL_TG_SKIP", reason="missing_cfg_or_httpx")
        return
    try:
        url = f"https://api.telegram.org/bot{(os.getenv('TELEGRAM_BOT_TOKEN') or '').strip()}/sendMessage"
        payload = {"chat_id": ((os.getenv("DAILY_PNL_CHAT_ID") or os.getenv("TELEGRAM_CHAT_ID") or "").strip()),
                   "text": text, "parse_mode": "HTML", "disable_web_page_preview": True}
        async with httpx.AsyncClient(timeout=15) as cli:
            r = await cli.post(url, json=payload)
            log.event("DAILY_PNL_TG_SENT", http_status=getattr(r, "status_code", None))
    except Exception as e:
        log.event("DAILY_PNL_TG_ERROR", error=str(e))

async def _pnl_pg_ensure_table() -> None:
    if not asyncpg or not _PNL_DSN_ASYNC:
        log.event("DAILY_PNL_PG_SKIP", reason="no_asyncpg_or_dsn")
        return
    try:
        conn = await asyncpg.connect(_PNL_DSN_ASYNC)
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
            log.event("DAILY_PNL_PG_READY", dsn=_PG_DSN_MASK)
        finally:
            await conn.close()
    except Exception as e:
        if _as_bool(os.getenv("PF_DEBUG_TX", "0"), False):
            traceback.print_exc()
        log.event("DAILY_PNL_PG_ERROR", when="ensure_table", error=str(e))

async def _pnl_pg_insert_row(*, realized_sol: float, unrealized_sol: float,
                             open_positions: int, wallet_sol: Optional[float],
                             notes: Dict[str, Any]) -> None:
    if not asyncpg or not _PNL_DSN_ASYNC:
        log.event("DAILY_PNL_PG_SKIP", reason="no_asyncpg_or_dsn")
        return
    try:
        conn = await asyncpg.connect(_PNL_DSN_ASYNC)
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
        if _as_bool(os.getenv("PF_DEBUG_TX", "0"), False):
            traceback.print_exc()
        log.event("DAILY_PNL_PG_ERROR", when="insert_row", error=str(e))

async def _get_wallet_sol_balance() -> Optional[float]:
    try:
        from utils_pkg.wallet import get_sol_balance  # type: ignore
        bal = await get_sol_balance()
        return float(bal)
    except Exception:
        return None

async def _estimate_unrealized_from_positions() -> Tuple[float, int, Dict[str, Any]]:
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
            qty = float(p.get("amount") or p.get("qty") or p.get("size") or 0) or 0.0
            if qty <= 0:
                continue
            cost = float(p.get("entry_total_sol") or p.get("cost_basis_sol") or p.get("cost") or 0.0)
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
            if _as_bool(os.getenv("PF_DEBUG_TX", "0"), False):
                traceback.print_exc()
            continue
    return total_unrl, open_n, details

async def _estimate_realized_today() -> Tuple[float, Dict[str, Any]]:
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
    if not _as_bool(os.getenv("DAILY_PNL_ENABLED", "1"), True):
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

        # --- Notion: push daily snapshot (safe no-op if NOTION_ENABLED != 1)
        try:
            _page = push_daily_pnl({
                "date": utc_today_iso(),
                "realized_sol": realized_sol,
                "unrealized_sol": unrl_sol,
                "open_positions": open_n,
                "wallet_sol": wallet_sol,
                "net_sol": (realized_sol + unrl_sol),
                "note": "Auto-daily sync",
            })
            if _page:
                log.event("NOTION_DAILY_PNL_UPSERTED", page_id=_page)
        except Exception as e:
            log.warning("NOTION_DAILY_PNL_FAILED %s", e)

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

    if _as_bool(os.getenv("DAILY_PNL_TEST_NOW", "0"), False) and not stop_evt.is_set():
        with contextlib.suppress(Exception):
            await _once()

    while not stop_evt.is_set():
        now = _dt.datetime.now(_dt.timezone.utc)
        run_at = now.replace(hour=DAILY_PNL_HOUR_UTC, minute=0, second=0, microsecond=0)
        if run_at <= now:
            run_at = run_at + _dt.timedelta(days=1)
        wait_s = (run_at - now).total_seconds()
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
# Health server (optional)
# ---------------------------
async def _maybe_start_health_server(stop_evt: asyncio.Event) -> Optional[asyncio.Task]:
    if not (web and HEALTH_PORT > 0):
        return None
    app = web.Application()

    async def _health(_request):
        return web.json_response({
            "ok": True,
            "ts": _dt.datetime.now(_dt.timezone.utc).isoformat(),
            "dryrun": _dryrun_env_on() or bool(_pkg_is_dryrun),
            "pools": sorted(POOLS_FILTER) if POOLS_FILTER else ["<all>"],
        })

    app.router.add_get("/health", _health)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", HEALTH_PORT)
    await site.start()
    log.event("HEALTH_LISTENING", port=HEALTH_PORT)

    async def _waiter():
        try:
            while not stop_evt.is_set():
                await asyncio.sleep(0.5)
        finally:
            with contextlib.suppress(Exception):
                await runner.cleanup()

    return asyncio.create_task(_waiter(), name="health.server")

# ---------------------------
# Streaming loop
# ---------------------------
async def stream_loop(stop_evt: Optional[asyncio.Event] = None) -> None:
    """
    Robust, version-proof PumpPortal streaming loop:
      - Works with older PumpPortalClient signatures (no subscribe_trades kwarg)
      - Subscribes to trades via client methods if present, or internal helper
      - Gracefully handles reconnects and shutdown
      - Shields Strategy handlers from crashing the stream
    """
    strat = Strategy()
    local_stop = stop_evt or asyncio.Event()
    diag_task = asyncio.create_task(strat.diag_loop(local_stop), name="diag.minute")

    def _mask_uri(obj: Any) -> str:
        try:
            fn = getattr(obj, "masked_uri", None)
            return fn() if callable(fn) else "<uri?>"
        except Exception:
            return "<uri?>"

    async def _handle_event(ev: dict) -> None:
        kind = (ev or {}).get("kind")
        raw = (ev or {}).get("raw", {}) or {}
        if kind == "new_token":
            await strat.on_new_token(raw)
        elif kind == "trade":
            await strat.on_trade(raw)

    subscribe_trades_flag = _env_bool("PUMP_SUBSCRIBE_TRADES", True)

    async def _subscribe_if_possible(client: Any) -> None:
        """
        Version-safe subscription:
          1) Try public methods if they exist
          2) Fallback to internal `_subscribe("trades")`
        """
        if not subscribe_trades_flag:
            return
        for name in ("subscribe_trades", "enable_trades", "enable_trade_stream"):
            fn = getattr(client, name, None)
            if callable(fn):
                try:
                    res = fn()
                    if asyncio.iscoroutine(res):
                        await res
                    log.every("pump.subscribe", n=30, msg="PUMP_SUBSCRIBED", via=name)
                    return
                except Exception as e:
                    log.every("pump.subscribe.err", n=5, msg="PUMP_SUBSCRIBE_FAIL", via=name, err=str(e))
        try:
            sub = getattr(client, "_subscribe", None)
            if callable(sub):
                res = sub("trades")
                if asyncio.iscoroutine(res):
                    await res
                log.every("pump.subscribe", n=30, msg="PUMP_SUBSCRIBED", via="_subscribe")
        except Exception as e:
            log.every("pump.subscribe.err", n=5, msg="PUMP_SUBSCRIBE_FAIL", via="_subscribe", err=str(e))

    async def _run_with_client_obj(client: Any) -> None:
        print("[main] Connecting:", _mask_uri(client), flush=True)
        await _subscribe_if_possible(client)
        async for ev in client.events():
            if local_stop.is_set():
                break
            try:
                await _handle_event(ev)
            except asyncio.CancelledError:
                raise
            except Exception:
                print("[main] event handling error:", flush=True)
                traceback.print_exc()

    pp_kwargs: Dict[str, Any] = {}
    if os.getenv("PF_PP_KEY"):
        pp_kwargs["api_key"] = os.getenv("PF_PP_KEY")
    if os.getenv("OWNER"):
        pp_kwargs["account"] = os.getenv("OWNER")

    try:
        if hasattr(PumpPortalClient, "__aenter__"):
            async with PumpPortalClient(**pp_kwargs) as client:
                await _run_with_client_obj(client)
        else:
            client = PumpPortalClient(**pp_kwargs)
            try:
                await _run_with_client_obj(client)
            finally:
                with contextlib.suppress(Exception):
                    if hasattr(client, "close"):
                        maybe_awaitable = client.close()
                        if asyncio.iscoroutine(maybe_awaitable):
                            await maybe_awaitable  # type: ignore
    finally:
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

    base_interval = _as_float(os.getenv("RAYDIUM_SCAN_INTERVAL_SEC", "30"), 30.0)
    jitter_seed = (os.getpid() % 7) / 6.0
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

            await scan_raydium()
            log.event("SCAN_RAYDIUM_RETURNED")

            if base_interval > 0:
                jitter = 0.15 * base_interval * jitter_seed
                await asyncio.sleep(base_interval + jitter)

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

    want_raydium = getattr(C, "ENABLE_RAYDIUM_SCANNER", True)
    pools_ok = (not POOLS_FILTER) or ("raydium" in POOLS_FILTER)
    if want_raydium and pools_ok:
        t = asyncio.create_task(_run_raydium_scanner_forever(stop_evt), name="scan.raydium")
        tasks["scan.raydium"] = t
        log.event("SCAN_RAYDIUM_SCHEDULED", task="scan.raydium")

    if _as_bool(os.getenv("DAILY_PNL_ENABLED", "1"), True):
        t = asyncio.create_task(_run_daily_pnl_loop(stop_evt), name="telemetry.daily_pnl")
        tasks["telemetry.daily_pnl"] = t
        log.event("DAILY_PNL_SCHEDULED", hour_utc=_as_int(os.getenv("DAILY_PNL_HOUR_UTC", "23"), 23))

    ht = await _maybe_start_health_server(stop_evt)
    if ht:
        tasks["health.server"] = ht

    return tasks

async def _graceful_shutdown(
    stop_evt: asyncio.Event,
    sell_task: Optional[asyncio.Task],
    bg_tasks: Dict[str, asyncio.Task],
) -> None:
    stop_evt.set()

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
# Sentry (optional)
# ---------------------------
def _maybe_init_sentry() -> None:
    if not (SENTRY_DSN and sentry_sdk):
        return
    try:
        sentry_sdk.init(
            dsn=SENTRY_DSN,
            traces_sample_rate=float(os.getenv("SENTRY_TRACES_SAMPLE_RATE", "0.0")),
        )
        log.event("SENTRY_INIT_OK")
    except Exception as e:
        log.event("SENTRY_INIT_FAIL", error=str(e))

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
        f"PF_DEBUG_TX={int(_as_bool(os.getenv('PF_DEBUG_TX','0'), False))} | "
        f"PF_MAX_CONCURRENT={_as_int(os.getenv('PF_MAX_CONCURRENT','2'), 2)} | "
        f"PF_COOLDOWN_SEC={_as_float(os.getenv('PF_COOLDOWN_SEC','15'), 15.0)}\n"
        f"MOMO: on={int(_as_bool(os.getenv('ENTRY_MOMENTUM_ENABLED','1'), True))} "
        f"window={_as_int(os.getenv('ENTRY_MOMENTUM_WINDOW_SEC', os.getenv('ENTRY_MOMO_WINDOW_SEC','10')),10)}s "
        f"min_trades={_as_int(os.getenv('ENTRY_MOMENTUM_MIN_TRADES', os.getenv('ENTRY_MOMO_MIN_TRADES','6')),6)} "
        f"min_up={_as_float(os.getenv('ENTRY_MOMENTUM_MIN_UP_PCT', os.getenv('ENTRY_MOMO_MIN_UP_PCT','5')),5.0)}%\n"
        f"LIQ_FILTER={int(_as_bool(os.getenv('ENTRY_LIQ_FILTER','1'), True))} | "
        f"EXIT_ENGINE={int(_as_bool(os.getenv('EXIT_ENGINE_ENABLED','1'), True))} | "
        f"MAX_TRADEABLE={_as_int(os.getenv('MAX_TRADEABLE','0'),0)}\n"
        f"POOLS={pools} | PUMP_TRADES={int(pump_trades)}\n"
        f"PG(asyncpg) DSN={_PG_DSN_MASK or '<none>'} | HEALTH_PORT={HEALTH_PORT or 'off'}\n"
        "===========================\n",
        flush=True,
    )

def _env_hints() -> None:
    if not (os.getenv("KEYPAIR_JSON") or os.getenv("SOLANA_KEYPAIR_JSON")):
        log.event("HINT_KEYPAIR_JSON_MISSING", note="wallet auto-create disabled (ok if mounted)")
    if not (os.getenv("TELEGRAM_BOT_TOKEN") and (os.getenv("DAILY_PNL_CHAT_ID") or os.getenv("TELEGRAM_CHAT_ID"))):
        log.event("HINT_TELEGRAM_MISSING", note="daily pnl / buy notifications off")
    if not (_PNL_DSN_ASYNC or _PG_DSN_ASYNC):
        log.event("HINT_PG_DSN_MISSING", note="daily pnl persistence disabled")

async def main(stop_evt: Optional[asyncio.Event] = None) -> None:
    with contextlib.suppress(Exception):
        start_global_heartbeat(log, interval=float(os.getenv("PF_HEARTBEAT_SEC", "45")))
        bump("boot")

    _maybe_init_sentry()
    _boot_banner()
    _env_hints()
    _ensure_keypair_file()

    try:
        if router and hasattr(router, "start"):
            await router.start()
            log.event("ROUTER_PRIO_INIT_STARTED", ok=1)
    except Exception as e:
        log.event("ROUTER_PRIO_INIT_FAIL", error=str(e))

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
    bg_tasks = await _start_background_tasks(local_stop)

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, lambda s=sig: asyncio.create_task(_graceful_shutdown(local_stop, sell_task, bg_tasks)))
        except NotImplementedError:
            pass

    backoff = 3.0
    try:
        while not local_stop.is_set():
            try:
                await stream_loop(local_stop)
                print("[main] stream ended (remote closed). Reconnecting...", flush=True)
                backoff = 3.0
            except asyncio.CancelledError:
                raise
            except Exception:
                print("[main] stream_loop crashed:", flush=True)
                traceback.print_exc()
                backoff = min(backoff * 2.0, 30.0)
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
