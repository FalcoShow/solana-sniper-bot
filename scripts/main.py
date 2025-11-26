from __future__ import annotations

import asyncio
import contextlib
import datetime as _dt
import importlib
import inspect
import json
import logging
import os
import signal
import sys
import threading
import time
import traceback
from asyncio import create_task
from typing import Any, Dict, Optional, Tuple

try:
    import aiohttp  # type: ignore
except Exception:  # pragma: no cover
    aiohttp = None  # type: ignore

# Core utils (alerts + shared Redis client)
from utils_pkg.core_utils import AlertManager, redis

"""
scripts/main.py (vNext, part 1)

High-level responsibilities:
- Hardened startup, logging, and shutdown.
- DSN normalizer -> asyncpg-safe DSNs from any Render/PG env.
- Global env config for entry filters, momentum, pools, & cooldowns.
- Unified router buy wrapper (maybe_buy) with rich buy hooks:
      - _buy_hook_submitted  (logs + alerts)
      - _buy_hook_filled     (alerts + exit_engine via Strategy, Notion snapshot)
      - _buy_hook_failed     (alerts + guardrail telemetry)
- Momentum/entry env, including ENTRY_MOMO_ALLOW_NO_DATA_FOR_PUMP.
- Optional health endpoint (/health) if aiohttp & HEALTH_PORT present.
- Optional Sentry initialisation when SENTRY_DSN is set.
- Wires exit_engine + risk_manager + legacy sell_loop (actual loops in part 2).
"""

# ---------------------------
# Optional deps (HTTP / PG / Sentry / Health)
# ---------------------------
try:
    import httpx
except Exception:  # pragma: no cover
    httpx = None  # type: ignore

try:
    import asyncpg
except Exception:  # pragma: no cover
    asyncpg = None  # type: ignore

try:
    import sentry_sdk  # type: ignore
except Exception:  # pragma: no cover
    sentry_sdk = None  # type: ignore

try:
    from aiohttp import web  # type: ignore
except Exception:  # pragma: no cover
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
# Logging (robust + collision-safe helpers)
# ---------------------------

# Reserved LogRecord keys we must NOT place inside `extra`
_RESERVED: set[str] = {
    "name",
    "msg",
    "message",
    "levelname",
    "levelno",
    "pathname",
    "filename",
    "module",
    "exc_info",
    "exc_text",
    "stack_info",
    "lineno",
    "funcName",
    "created",
    "msecs",
    "relativeCreated",
    "thread",
    "threadName",
    "process",
    "processName",
}

_last_every: Dict[str, float] = {}
_every_lock = threading.Lock()


def _add_safe_extra(fields: Dict[str, Any]) -> Dict[str, Any]:
    """
    Copy fields, but rename any reserved logging keys so we never get:
        KeyError: "Attempt to overwrite 'msg' in LogRecord"
    """
    safe: Dict[str, Any] = {}
    for k, v in fields.items():
        if k in _RESERVED:
            safe[f"extra_{k}"] = v
        else:
            safe[k] = v
    return safe


def _attach_log_helpers_base(_log: logging.Logger):
    """
    Attach `event` and `every` helpers to a logger instance in a
    collision-safe way (no reserved keys in `extra`).
    """

    def event(name: str, **fields: Any) -> None:
        _log.info(name, extra=_add_safe_extra(fields))

    def every(_key: str, n: int = 60, **fields: Any) -> None:
        now = time.time()
        with _every_lock:
            last = _last_every.get(_key, 0.0)
            if now - last < n:
                return
            _last_every[_key] = now
        _log.info(f"[every {n}s] {_key}", extra=_add_safe_extra(fields))

    if not hasattr(_log, "event"):
        _log.event = event  # type: ignore[attr-defined]
    if not hasattr(_log, "every"):
        _log.every = every  # type: ignore[attr-defined]
    return _log


try:
    # Preferred logging setup from logging_setup
    from logging_setup import get_logger, bump, start_global_heartbeat

    _base_log = get_logger()
except Exception:  # pragma: no cover

    class DummyLogger(logging.Logger):
        def __init__(self):
            super().__init__("dummy")

        def _p(self, lvl: str, msg: str, **fields: Any) -> None:
            line = f"[{lvl}] {msg}"
            if fields:
                line += " " + repr(fields)
            print(line, flush=True)

        def info(self, msg: str, **fields: Any) -> None:  # type: ignore[override]
            self._p("INFO", msg, **fields)

        def warning(self, msg: str, **fields: Any) -> None:  # type: ignore[override]
            self._p("WARN", msg, **fields)

        def error(self, msg: str, **fields: Any) -> None:  # type: ignore[override]
            self._p("ERROR", msg, **fields)

        def debug(self, msg: str, **fields: Any) -> None:  # type: ignore[override]
            self._p("DEBUG", msg, **fields)

    def bump(*_a, **_k):  # type: ignore[func-returns-value]
        return None

    def start_global_heartbeat(*_a, **_k):  # type: ignore[func-returns-value]
        return None

    _base_log = DummyLogger()

# Final logger used everywhere
log = _attach_log_helpers_base(_base_log)

# ---------------------------
# Normalization helpers
# ---------------------------
try:
    from scripts.pp_utils import normalize_mint as _pp_normalize
except Exception:  # pragma: no cover

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


def _env_bool(key: str, default: bool = False) -> bool:
    v = os.getenv(key)
    if v is None:
        return default
    return str(v).strip().lower() in ("1", "true", "t", "yes", "y", "on")


def _dryrun_env_on() -> bool:
    # honour any of these flags
    return (
        _env_bool("DRYRUN")
        or _env_bool("DRY_RUN")
        or _env_bool("IS_DRYRUN")
    )


# ---------------------------
# Event source + trading / exit hooks
# ---------------------------
try:
    from utils_pkg.pumpportal_client import PumpPortalClient
except Exception:  # pragma: no cover

    class PumpPortalClient:  # type: ignore
        def __init__(self, *a, **kw):
            raise RuntimeError("PumpPortalClient not available")


# We intentionally *don't* import maybe_buy from pump_trader.
# Instead we wire to whatever buy-capable function router exposes.
try:
    from utils_pkg.trader import sell_on_jupiter, IS_DRYRUN as _pkg_is_dryrun
except Exception:  # pragma: no cover

    async def sell_on_jupiter(*_a, **_kw):  # type: ignore
        raise RuntimeError("sell_on_jupiter not available")

    _pkg_is_dryrun = True  # type: ignore[assignment]

# Exit engine + worker: the engine enqueues exit_request:* in Redis,
# and the worker consumes them. (Loops wired in part 2.)
try:
    from utils_pkg.auto_exit_worker import exit_worker_loop, exit_engine  # type: ignore
except Exception:  # pragma: no cover

    async def exit_worker_loop(stop_evt: asyncio.Event) -> None:  # type: ignore
        while not stop_evt.is_set():
            await asyncio.sleep(5)

    class _NoopExit:
        async def on_trade_tick(self, *_a, **_kw): ...
        async def on_buy_filled(self, *_a, **_kw): ...

    exit_engine = _NoopExit()  # type: ignore

# Optional position / guardrail helpers
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


# Optional extra risk loop (separate from auto_exit_worker)
try:
    from utils_pkg.risk_manager import risk_manager_loop  # type: ignore
except Exception:  # pragma: no cover

    async def risk_manager_loop(stop_evt: asyncio.Event) -> None:  # type: ignore
        while not stop_evt.is_set():
            await asyncio.sleep(10)


# Optional legacy sell-loop worker
try:
    from utils_pkg.sell_loop import run_sell_loop  # type: ignore
except Exception:  # pragma: no cover

    async def run_sell_loop(stop_evt: asyncio.Event) -> None:  # type: ignore
        while not stop_evt.is_set():
            await asyncio.sleep(10)


# Router + price helpers
try:
    from utils_pkg import router  # provides buy function(s)
    from utils_pkg.router import get_price_ui as _route_price_ui
    try:
        from utils_pkg.router import get_decimals as _route_get_decimals
    except Exception:  # pragma: no cover
        _route_get_decimals = None  # type: ignore
except Exception:  # pragma: no cover
    router = None  # type: ignore

    async def _route_price_ui(_mint: str) -> float:  # type: ignore
        return 0.0

    _route_get_decimals = None  # type: ignore

# ---------- ROUTER BUY WRAPPER ----------


def _safe_log_router_state(event: str, **fields: Any) -> None:
    try:
        log.event(event, **fields)
    except Exception:
        # logging must never break routing
        pass


def _candidate_buy_names() -> list[str]:
    # Preferred stable names first, then common variants.
    return [
        "buy_via_lightning",  # primary Jupiter v6 path (supports return_meta)
        "pump_buy_with_fallback",
        "maybe_buy",
        "buy",
        "route_and_buy",
        "buy_on_jupiter",
        "buy_token",
        "execute_buy",
        "exec_buy",
        "_buy_lightning",
        "buy_lightning",
        "buy_direct",
        "buy_pump",
    ]


def _pick_router_buy_callable() -> Optional[tuple[str, Any]]:
    """
    Try to discover a buy-capable function on utils_pkg.router.
    Prefer well-known names, then fall back to any function with 'buy'
    in its name (excluding sell/quote helpers).
    """
    if router is None:
        return None

    # 1) Preferred names
    for name in _candidate_buy_names():
        fn = getattr(router, name, None)
        if callable(fn):
            return name, fn

    # 2) Fallback: any function with 'buy' in name, excluding sell/quote.
    try:
        for n in dir(router):
            if "buy" not in n.lower():
                continue
            low = n.lower()
            if any(bad in low for bad in ("sell", "quote")):
                continue
            fn = getattr(router, n, None)
            if callable(fn):
                return n, fn
    except Exception:
        pass
    return None


def _compute_spend_sol(ui: Dict[str, Any]) -> float:
    """
    Decide how much SOL to spend for a buy, preferring explicit UI overrides,
    then env hints, then a tiny default.
    """
    # 1) UI overrides from scanner/strategy
    for k in ("spend_sol", "buy_size_sol", "amount_sol"):
        try:
            v = ui.get(k)
            if v is None:
                continue
            f = float(v)
            if f > 0:
                return f
        except Exception:
            continue

    # 2) Env hints
    for envk in ("ENTRY_BUY_SOL", "ENTRY_SPEND_SOL", "INITIAL_BUY_SOL"):
        try:
            v = float(os.getenv(envk, "0"))
            if v > 0:
                return v
        except Exception:
            continue

    # 3) Fallback: ultra-safe tiny default
    return 0.02


def _env_slippage_bps() -> int:
    try:
        return int(os.getenv("SLIPPAGE_BPS", os.getenv("ENTRY_SLIPPAGE_BPS", "50")))
    except Exception:
        return 50


def _build_kwargs_for_router(
    fn: Any,
    mint: str,
    ui: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Introspect the router buy function and pass only parameters it expects.
    This keeps us forwards-compatible across refactors of utils_pkg.router.

    IMPORTANT: if the underlying buy supports `return_meta`, we set it True
    so we can harvest exact entry size/price data.
    """
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
            kwargs["only_direct"] = bool(
                os.getenv("ONLY_DIRECT", "0").strip().lower()
                in {"1", "true", "yes", "on"}
            )

        # If the impl supports meta, request it so we can log into Notion/DB.
        if "return_meta" in names:
            kwargs["return_meta"] = True
    except Exception:
        # If introspection fails, fall back to the safest minimal call
        kwargs = {"mint": mint, "ui": ui}
    return kwargs


async def _call_with_variants(
    fn: Any,
    *,
    mint: str,
    ui: Dict[str, Any],
):
    """
    Call the router buy function with best-effort kwargs, falling back through
    multiple signatures until one succeeds.

    Return value is passed through untouched so maybe_buy() can interpret:
      - str                        → tx signature
      - dict                       → {signature, ...}
      - (sig, entry_meta) tuple    → sig + entry meta from lightning_impl
    """
    # Variant 1: full introspected kwargs (including return_meta if supported)
    try:
        kw_sig = _build_kwargs_for_router(fn, mint, ui)
        res = fn(**kw_sig)
        if inspect.isawaitable(res):
            res = await res
        _safe_log_router_state(
            "ROUTER_BUY_CALL",
            variant="kw_sig",
            sent_keys=sorted(kw_sig.keys()),
        )
        return res
    except TypeError as e:
        _safe_log_router_state(
            "ROUTER_BUY_CALL_TYPERR",
            where="kw_sig",
            err=str(e),
        )
    except Exception as e:
        _safe_log_router_state(
            "ROUTER_BUY_CALL_ERR",
            where="kw_sig",
            err=str(e),
        )

    # Variant 2: minimal mint+ui
    try:
        res = fn(mint=mint, ui=ui)
        if inspect.isawaitable(res):
            res = await res
        _safe_log_router_state("ROUTER_BUY_CALL", variant="kw_min_mint_ui")
        return res
    except Exception as e:
        _safe_log_router_state(
            "ROUTER_BUY_CALL_ERR",
            where="kw_min_mint_ui",
            err=str(e),
        )

    # Variant 3: mint only
    try:
        res = fn(mint=mint)
        if inspect.isawaitable(res):
            res = await res
        _safe_log_router_state("ROUTER_BUY_CALL", variant="kw_mint_only")
        return res
    except Exception as e:
        _safe_log_router_state(
            "ROUTER_BUY_CALL_ERR",
            where="kw_mint_only",
            err=str(e),
        )

    _safe_log_router_state(
        "ROUTER_BUY_ALL_VARIANTS_FAILED",
        error="no compatible kwargs",
    )
    return None


async def maybe_buy(
    *,
    mint: str,
    ui: Dict[str, Any],
    on_buy_submitted=None,
    on_buy_filled=None,
    on_buy_failed=None,
) -> Optional[str]:
    """
    Unified buy entrypoint for the rest of the script.

    - Picks the best buy-capable function from utils_pkg.router.
    - If the underlying buy supports `return_meta`, we harvest (sig, entry_meta)
      and log entry metadata (size, in/out amounts, priceImpact) using
      log_entry_from_meta so Notion/DB stay in sync.
    - Still returns a plain tx signature string (or None), so Strategy and
      other callers remain fully compatible.

    NOTE:
      The on_buy_* callbacks are *not* invoked here. Strategy acts as the
      single source of truth for calling _buy_hook_* after confirmation.
    """
    if router is None:
        _safe_log_router_state("ROUTER_UNAVAILABLE_FOR_BUY")
        raise RuntimeError("router unavailable")

    choice = _pick_router_buy_callable()
    if not choice:
        _safe_log_router_state(
            "ROUTER_NO_BUY_CAPABLE",
            impls=getattr(router, "buy_keys", None),
            sell_keys=getattr(router, "sell_keys", None),
            file=getattr(router, "__file__", "<router?>"),
        )
        raise RuntimeError("router has no buy-capable function")

    fn_name, fn = choice
    _safe_log_router_state(
        "ROUTER_BUY_FN",
        fn=fn_name,
        impls=getattr(router, "buy_keys", None),
        sell_keys=getattr(router, "sell_keys", None),
        file=getattr(router, "__file__", "<router?>"),
    )

    # Call router, possibly getting back (sig, entry_meta)
    res = await _call_with_variants(
        fn,
        mint=mint,
        ui=ui,
    )

    sig: Optional[str] = None
    entry_meta: Dict[str, Any] = {}

    # Case 1: new-style (sig, entry_meta) from lightning_impl via return_meta=True
    if isinstance(res, tuple) and len(res) == 2:
        maybe_sig, maybe_meta = res
        if isinstance(maybe_sig, str) and maybe_sig:
            sig = maybe_sig
        if isinstance(maybe_meta, dict):
            entry_meta = maybe_meta or {}

    # Case 2: raw string signature
    elif isinstance(res, str):
        sig = res

    # Case 3: dict with a signature key
    elif isinstance(res, dict):
        for k in ("signature", "sig", "tx_sig", "tx", "id"):
            v = res.get(k)
            if isinstance(v, str) and v:
                sig = v
                break

    # If we have entry meta, log it for downstream analytics + Notion/DB.
    if entry_meta:
        try:
            # fire-and-forget structured log for debugging
            log.event(
                "ENTRY_META",
                mint=mint,
                amount_raw=int(entry_meta.get("amount_raw") or 0),
                in_amount_raw=int(entry_meta.get("in_amount_raw") or 0),
                priceImpactPct=entry_meta.get("priceImpactPct"),
                pool=ui.get("pool") or "unknown",
                symbol=ui.get("symbol") or ui.get("name") or "?",
            )
        except Exception:
            # meta logging must never break trading
            pass

        # Persist to PG + Notion (best-effort, never breaks flow)
        try:
            await log_entry_from_meta(  # defined later in this file
                mint=mint,
                entry_meta=entry_meta,
                ui=ui,
                sig=sig,
            )
        except Exception as e:
            # DB/Notion failures are non-fatal
            try:
                log.event(
                    "ENTRY_META_LOG_FAIL",
                    mint=mint,
                    err=str(e)[:200],
                )
            except Exception:
                pass

    return sig

# ---------- /ROUTER BUY WRAPPER ----------

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

    async def pricefeed_close() -> None:
        return None


# Optional Telegram text sender (for debug paths)
try:
    from utils_pkg.telegram_utils import send_text as _tg_send_text  # type: ignore
except Exception:  # pragma: no cover
    _tg_send_text = None  # type: ignore


# Raydium scanner (lazy import – actual usage in part 2)
def _get_raydium_scanner():
    try:
        from scanner.raydium_scanner import scan_raydium  # type: ignore

        return scan_raydium
    except Exception:
        return None


# --- Solana tx confirmation helpers ---
def _rpc_url() -> str:
    return (
        os.getenv("SOLANA_RPC_URL")
        or os.getenv("RPC_URL")
        or "https://api.mainnet-beta.solana.com"
    )


def _confirm_commitment() -> str:
    return os.getenv("CONFIRM_COMMITMENT", "confirmed")


def _confirm_timeout_sec() -> float:
    try:
        return float(os.getenv("CONFIRM_TIMEOUT_SEC", "25"))
    except Exception:
        return 25.0


async def _get_sig_status(sig: str) -> Optional[dict]:
    if httpx is None:
        return None
    body = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getSignatureStatuses",
        "params": [[sig], {"searchTransactionHistory": True}],
    }
    async with httpx.AsyncClient(timeout=10) as c:  # type: ignore[attr-defined]
        r = await c.post(_rpc_url(), json=body)
        r.raise_for_status()
        j = r.json()
        val = (j.get("result") or {}).get("value") or []
        return val[0] if val else None


async def _confirm_sig(sig: str) -> Tuple[bool, Optional[str]]:
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


def _as_bool(s: str, default: bool = False) -> bool:
    try:
        return s.strip().lower() in ("1", "true", "yes", "on")
    except Exception:
        return default


CANDIDATE_DEBUG_ENABLED = _as_bool(os.getenv("CANDIDATE_DEBUG_ENABLED", "0"), False)
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN") or os.getenv(
    "ALERTS_TELEGRAM_BOT_TOKEN"
)
TELEGRAM_DEBUG_CHAT_ID = os.getenv("TELEGRAM_DEBUG_CHAT_ID") or os.getenv(
    "TELEGRAM_CHAT_ID"
)


async def _send_telegram_debug(msg: str) -> None:
    """
    Fire-and-forget tiny debug message to Telegram (for candidate / momentum
    debugging). Completely optional and never allowed to crash the process.
    """
    if not CANDIDATE_DEBUG_ENABLED:
        return
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_DEBUG_CHAT_ID:
        return
    if aiohttp is None:
        return

    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        payload = {
            "chat_id": TELEGRAM_DEBUG_CHAT_ID,
            "text": msg[:1000],
            "disable_web_page_preview": True,
        }
        timeout = aiohttp.ClientTimeout(total=5)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            await session.post(url, json=payload)
    except Exception:
        # Debug only – never crash on failure
        with contextlib.suppress(Exception):
            print(
                "[telegram-debug] failed to send message",
                file=sys.stderr,
                flush=True,
            )


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
    """
    Normalize a DSN into something asyncpg understands:
      - strip +asyncpg driver suffixes
      - keep postgres:// / postgresql:// prefixes
    """
    if not dsn:
        return ""
    s = dsn.strip()
    if not s:
        return ""
    # Normalize common prefixes
    s = s.replace("postgres+asyncpg://", "postgres://")
    s = s.replace("postgresql+asyncpg://", "postgresql://")
    if s.lower().startswith("postgresql+asyncpg://"):
        s = "postgresql://" + s.split("://", 1)[1]
    if s.lower().startswith("postgres+asyncpg://"):
        s = "postgres://" + s.split("://", 1)[1]
    return s


def _pick_env_dsn() -> Tuple[str, str, str]:
    """
    Choose PnL and generic PG DSNs from env, normalize both for asyncpg, and
    return (pnl_dsn_asyncpg, pg_dsn_asyncpg, masked_host_db).
    """
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
    pg_raw = (
        os.getenv("PG_DSN")
        or os.getenv("DATABASE_URL")
        or os.getenv("PG_DSN_INTERNAL")
        or os.getenv("PG_DSN_EXTERNAL")
        or os.getenv("POSITIONS_PG_DSN")
        or ""
    ).strip()

    pnl_async = _dsn_to_asyncpg(pnl_raw)
    pg_async = _dsn_to_asyncpg(pg_raw)

    def _mask(s: str) -> str:
        if not s:
            return ""
        try:
            body = s.split("://", 1)[1]
            after_at = body.split("@", 1)[-1]
            host_db = after_at.split("?", 1)[0]
            return host_db
        except Exception:
            return "<dsn>"

    return pnl_async, pg_async, (_mask(pnl_async) or _mask(pg_async) or "")


_PNL_DSN_ASYNC, _PG_DSN_ASYNC, _PG_DSN_MASK = _pick_env_dsn()

# ---------------------------
# Config (env)
# ---------------------------
EXIT_ENGINE_ENABLED = _as_bool(os.getenv("EXIT_ENGINE_ENABLED", "1"), True)
DEDUPE_ENABLED = _as_bool(os.getenv("PF_DEDUPE_ENABLED", "1"), True)
DEDUPE_TTL_SEC = _as_int(os.getenv("PF_DEDUPE_TTL_SEC", "3600"), 3600)
LIQ_FILTER_ENABLED = _as_bool(os.getenv("ENTRY_LIQ_FILTER", "1"), True)

# Momentum / entry window (Strategy wiring is in part 2)
ENTRY_MOMENTUM_ENABLED = _as_bool(os.getenv("ENTRY_MOMENTUM_ENABLED", "1"), True)
ENTRY_MOMO_WINDOW_SEC = _as_int(os.getenv("ENTRY_MOMO_WINDOW_SEC", "10"), 10)
ENTRY_MOMO_MIN_TRADES = _as_int(os.getenv("ENTRY_MOMO_MIN_TRADES", "6"), 6)
ENTRY_MOMO_MIN_UP_PCT = _as_float(os.getenv("ENTRY_MOMO_MIN_UP_PCT", "5"), 5.0)
ENTRY_MOMO_PRICE_TRIES = _as_int(os.getenv("ENTRY_MOMO_PRICE_TRIES", "4"), 4)
ENTRY_MOMO_PRICE_DELAY_S = _as_float(
    os.getenv("ENTRY_MOMO_PRICE_DELAY_S", "0.7"), 0.7
)
ENTRY_MOMO_MAX_PARALLEL = max(
    1, _as_int(os.getenv("ENTRY_MOMO_MAX_PARALLEL", "6"), 6)
)

# NEW: allow momentum gate to proceed even if we don't have full data for pump.fun
# backfill / low-liquidity cases (default false for safety).
ENTRY_MOMO_ALLOW_NO_DATA_FOR_PUMP = _as_bool(
    os.getenv(
        "ENTRY_MOMO_ALLOW_NO_DATA_FOR_PUMP",
        os.getenv("ENTRY_MOMO_ALLOW_NO_DATA", "0"),
    ),
    False,
)

MIN_INITIAL_BUY_SOL = _as_float(os.getenv("ENTRY_MIN_BUY_SOL", "0.0"), 0.0)
MAX_NAME_LEN = _as_int(os.getenv("ENTRY_MAX_NAME_LEN", "40"), 40)
ENTRY_MAX_AGE_MIN = _as_int(os.getenv("ENTRY_MAX_AGE_MIN", "20"), 20)
MIN_POOL_AGE_MIN = _as_int(os.getenv("MIN_POOL_AGE_MIN", "3"), 3)

SYMBOL_BLACKLIST = {
    s.strip().upper()
    for s in os.getenv("ENTRY_SYMBOL_BLACKLIST", "RUG,SCAM").split(",")
    if s.strip()
}

MAX_CONCURRENCY = max(1, _as_int(os.getenv("PF_MAX_CONCURRENT", "2"), 2))
LOOP_COOLDOWN_SEC = _as_float(os.getenv("PF_COOLDOWN_SEC", "15"), 15.0)
MAX_TRADEABLE = _as_int(os.getenv("MAX_TRADEABLE", "0"), 0)  # 0 = unlimited
PAUSE_TRADING = _as_bool(os.getenv("PAUSE_TRADING", "0"), False)
DEBUG_TX = _as_bool(os.getenv("PF_DEBUG_TX", "0"), False)

# Pools filter (affects printing and Raydium scanner scheduling)
_POOLS_ENV = [
    p.strip().lower()
    for p in (os.getenv("PF_POOLS") or "").split(",")
    if p.strip()
]
POOLS_FILTER: set[str] = set(_POOLS_ENV)

# Daily PnL env
DAILY_PNL_ENABLED = _as_bool(os.getenv("DAILY_PNL_ENABLED", "1"), True)
DAILY_PNL_HOUR_UTC = _as_int(os.getenv("DAILY_PNL_HOUR_UTC", "23"), 23)
DAILY_PNL_TEST_NOW = _as_bool(os.getenv("DAILY_PNL_TEST_NOW", "0"), False)
DAILY_PNL_CHAT_ID = (
    os.getenv("DAILY_PNL_CHAT_ID") or os.getenv("TELEGRAM_CHAT_ID") or ""
).strip()
DAILY_PNL_BOT = (os.getenv("TELEGRAM_BOT_TOKEN") or "").strip()
DAILY_PNL_DEBOUNCE_MIN = _as_int(os.getenv("DAILY_PNL_DEBOUNCE_MIN", "10"), 10)

# Scanner + dedupe tuning
RAYDIUM_SCAN_INTERVAL_SEC = _as_float(
    os.getenv("RAYDIUM_SCAN_INTERVAL_SEC", "30"), 30.0
)
PF_GATE_TTL_SEC = _as_int(
    os.getenv(
        "PF_GATE_TTL_SEC",
        str(max(_as_int(os.getenv("ENTRY_MOMO_WINDOW_SEC", "10"), 10) * 2, 60)),
    ),
    max(ENTRY_MOMO_WINDOW_SEC * 2, 60),
)
PF_BUY_TTL_SEC = _as_int(os.getenv("PF_BUY_TTL_SEC", "600"), 600)

# Buy-hook Telegram toggles (optional) – defaults ON for visibility
NOTIFY_BUY_SUBMITTED = _as_bool(os.getenv("NOTIFY_BUY_SUBMITTED", "1"), True)
NOTIFY_BUY_FAILED = _as_bool(os.getenv("NOTIFY_BUY_FAILED", "1"), True)
NOTIFY_BUY_FILLED = _as_bool(os.getenv("NOTIFY_BUY_FILLED", "1"), True)

# Health / Sentry
HEALTH_PORT = _as_int(os.getenv("HEALTH_PORT", "0"), 0)
SENTRY_DSN = (os.getenv("SENTRY_DSN") or "").strip()

# Risk manager toggles (loop wired later in part 2)
RISK_MANAGER_ENABLED = _as_bool(os.getenv("RISK_MANAGER_ENABLED", "1"), True)
RISK_MANAGER_POLL_SEC = _as_float(
    os.getenv("RISK_MANAGER_POLL_SEC", "60"), 60.0
)

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
        "mint",
        "mintAddress",
        "mint_address",
        "bondingCurve",
        "bonding_curve",
        "address",
        "id",
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
# Basic filters (pre-momentum / pre-expensive work)
# ---------------------------


def _passes_basic_filters(msg: Dict[str, Any]) -> bool:
    """
    Very early guardrail filter before we do any heavy work.
    Uses global env-derived thresholds so behaviour is consistent across runtime.
    """
    pool = str((_get_first(msg, "pool", default="") or "")).strip().lower()
    is_raydium = pool == "raydium"

    # Pool allow-list (PF_POOLS)
    if POOLS_FILTER and pool and pool not in POOLS_FILTER:
        if DEBUG_TX:
            print(
                f".[filter] pool '{pool}' not in {sorted(POOLS_FILTER)} — skip",
                flush=True,
            )
        return False

    # For non-Raydium, require "create" events
    typ = str(_get_first(msg, "txType", "type", default="")).strip().lower()
    if not is_raydium and typ != "create":
        return False

    # Minimum initial buy (SOL) for non-Raydium events
    init_buy = _as_float(
        _get_first(msg, "initialBuy", "initial_buy", default=0.0),
        0.0,
    )
    if (not is_raydium) and MIN_INITIAL_BUY_SOL > 0 and init_buy < MIN_INITIAL_BUY_SOL:
        if DEBUG_TX:
            print(
                f".[filter] initialBuy {init_buy} < min {MIN_INITIAL_BUY_SOL} — skip",
                flush=True,
            )
        return False

    # Symbol blacklist (SCAM / RUG etc.)
    sym = _extract_symbol(msg).upper()
    if SYMBOL_BLACKLIST and any(
        bad and bad in sym for bad in SYMBOL_BLACKLIST
    ):
        if DEBUG_TX:
            print(
                f".[filter] symbol '{sym}' hits blacklist {sorted(SYMBOL_BLACKLIST)} — skip",
                flush=True,
            )
        return False

    # Name length sanity check
    nm = _extract_name(msg)
    if MAX_NAME_LEN > 0 and len(nm) > MAX_NAME_LEN:
        if DEBUG_TX:
            print(
                f".[filter] name too long ({len(nm)} > {MAX_NAME_LEN}) — skip",
                flush=True,
            )
        return False

    # Token age (ENTRY_MAX_AGE_MIN)
    mins_since_token = _approx_minutes_since(
        _get_first(
            msg,
            "created_at",
            "createdAt",
            "timestamp",
            "ts",
            default=None,
        )
    )
    if (
        mins_since_token is not None
        and ENTRY_MAX_AGE_MIN > 0
        and mins_since_token > ENTRY_MAX_AGE_MIN
    ):
        if DEBUG_TX:
            print(
                f".[filter] token age {mins_since_token:.1f}m > {ENTRY_MAX_AGE_MIN}m — skip",
                flush=True,
            )
        return False

    # Pool age (MIN_POOL_AGE_MIN)
    mins_since_pool = _approx_minutes_since(
        _get_first(msg, "poolCreatedAt", "pool_created_at", default=None)
    )
    if (
        mins_since_pool is not None
        and MIN_POOL_AGE_MIN > 0
        and mins_since_pool < MIN_POOL_AGE_MIN
    ):
        if DEBUG_TX:
            print(
                f".[filter] pool age {mins_since_pool:.1f}m < {MIN_POOL_AGE_MIN}m — skip",
                flush=True,
            )
        return False

    return True

# ---------------------------
# Post-buy optional immediate sell test
# ---------------------------


async def _post_buy_exit_test(mint_norm: str, tx_sig: str) -> None:
    """
    Optional post-buy path: if POSTBUY_MODE=immediate_sell_test, do a
    quick Jupiter sell to verify routing & slippage behaviour in DRYRUN
    environments. No-op in live mode.
    """
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


async def _screen_with_retry(
    mint: str,
    tries: int = 3,
    first_wait: float = 0.8,
):
    """
    Re-try the liquidity screen a few times for "no_account" race conditions
    when a pool is just spinning up.
    """
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
                print(
                    f"[liquidity] retrying for {mint} (no_account) in {wait:.2f}s",
                    flush=True,
                )
            await asyncio.sleep(wait)
            wait *= 1.6
    return False, last


async def _await_mint_ready(
    mint_norm: str,
    max_wait_s: int = 45,
    step_s: float = 3.0,
) -> Optional[int]:
    """
    Wait for router.get_decimals() to succeed for a new mint, up to max_wait_s.
    This is mainly a UX helper to avoid insta-failing brand new pools.
    """
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
    """
    Make sure ~/.config/solana/id.json exists. If not, and KEYPAIR_JSON /
    SOLANA_KEYPAIR_JSON is provided, write it with chmod 600.
    """
    import pathlib
    import stat

    try:
        home = os.path.expanduser("~")
        cfg_dir = pathlib.Path(home) / ".config" / "solana"
        key_path = cfg_dir / "id.json"
        if key_path.exists():
            return
        raw = os.getenv("KEYPAIR_JSON") or os.getenv("SOLANA_KEYPAIR_JSON")
        if not raw:
            log.event(
                "KEYPAIR_ENV_MISSING",
                hint="set KEYPAIR_JSON to auto-create wallet",
            )
            return
        kp = json.loads(raw)
        if not (
            isinstance(kp, list)
            and len(kp) == 64
            and all(isinstance(x, int) for x in kp)
        ):
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

# cheap singleton to avoid re-creating AlertManager
_AM_SINGLETON: Optional[AlertManager] = None


def _am() -> AlertManager:
    """
    Global AlertManager singleton so we don't recreate clients/sessions.
    """
    global _AM_SINGLETON
    if _AM_SINGLETON is None:
        _AM_SINGLETON = AlertManager()
    return _AM_SINGLETON


def _normalize_spend_sol(d: Dict[str, Any]) -> float:
    """
    Ensure we always have a numeric 'spend_sol' field on buy payloads/events.

    Priority:
      1) explicit SOL-sized fields:
         - d["spend_sol"]
         - d["amount_sol"]
         - d["size_sol"]
         - d["size"]
      2) lamport-sized fields (converted to SOL):
         - d["lamports_in"]
         - d["in_amount_raw"]
         - d["amount_raw"]
      3) 0.0 (last resort – but normally one of the above exists)

    The normalized value is also written back into d["spend_sol"] so any
    downstream consumers (exit engine, PnL, alerts) see the same value.
    """
    cand = d.get("spend_sol", None)

    # --- SOL-sized candidates first ---
    if cand in (None, "", 0, 0.0):
        for k in ("amount_sol", "size_sol", "size"):
            v = d.get(k, None)
            if v not in (None, ""):
                cand = v
                break

    # --- If still empty, try lamport-sized keys and convert to SOL ---
    if cand in (None, "", 0, 0.0):
        lamports_val = None
        for k in ("lamports_in", "in_amount_raw", "amount_raw"):
            v = d.get(k, None)
            if v not in (None, ""):
                lamports_val = v
                break

        if lamports_val is not None:
            try:
                lamports_f = float(lamports_val)
                lps = float(globals().get("LAMPORTS_PER_SOL", 1_000_000_000))
                cand = lamports_f / lps
            except Exception:
                cand = 0.0

    # --- Final numeric normalization ---
    try:
        spend = float(cand) if cand not in (None, "") else 0.0
    except Exception:
        spend = 0.0

    if spend < 0:
        spend = 0.0

    d["spend_sol"] = spend
    return spend


async def _buy_hook_submitted(payload: Dict[str, Any]) -> None:
    """
    Called when a BUY has been submitted to the router.

    payload: {mint, pool, symbol, spend_sol?, amount_sol?, route, hint, ...}
    """
    # normalize size so everything downstream (logs, alerts, exit) sees a number
    spend_sol = _normalize_spend_sol(payload)

    # metrics
    try:
        bump("buys_submitted")
    except Exception:
        pass

    # structured log
    try:
        log.event(
            "BUY_SUBMITTED",
            mint=payload.get("mint"),
            symbol=payload.get("symbol"),
            pool=payload.get("pool"),
            route=payload.get("route"),
            spend_sol=spend_sol,
            hint=payload.get("hint"),
        )
    except Exception:
        pass

    # lightweight analytics hook (non-fatal)
    try:
        record_buy_result(
            ok=True,
            reason="submitted",
            spend_sol=spend_sol,
        )
    except Exception:
        pass

    # Telegram/alerts (lightweight text on submission)
    if NOTIFY_BUY_SUBMITTED:
        try:
            txt = (
                f"🟡 Submitted BUY {payload.get('symbol') or ''}\n"
                f"mint={payload.get('mint')}  route={payload.get('route') or 'auto'}  "
                f"size={spend_sol or '—'} SOL"
            )
            create_task(_am().notify_text(txt))
        except Exception as _e:
            log.warning(
                "ALERT_BUY_SUBMIT_FAIL",
                extra={"err": str(_e)[:200]},
            )

# --- Notion (optional daily PnL sync) ---
try:
    # safe no-op if NOTION_ENABLED != 1 inside this helper
    from utils_pkg.notion_pnl import push_daily_pnl
except Exception:  # pragma: no cover

    def push_daily_pnl(_snapshot):  # type: ignore[override]
        return None


# Debounce for intraday push after fills (seconds granularity)
_LAST_NOTION_PUSH_TS: float = 0.0


async def _notion_intraday_snapshot_push(note: str = "") -> None:
    """
    Lightweight intraday PnL snapshot push to Notion after a buy fill.
    Debounced via DAILY_PNL_DEBOUNCE_MIN.
    """
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
        # Helpers are defined later in this file.
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
    Called when the router reports a BUY fill.

    ev: { mint, signature, symbol, spend_sol?, amount_sol?, pool, amount_raw?, ... }

    Responsibilities:
      - bump metrics
      - structured logs
      - primary alert (Telegram / AlertManager)
      - optional intraday Notion snapshot

    NOTE:
      Exit / auto-sell logic is handled *after RPC confirmation* inside
      Strategy (_gate_candidate / direct-buy path), which calls
      exit_engine.on_buy_filled(ev, ...) exactly once per real fill.

      Because 'ev' is a dict passed by reference, normalizing ev["spend_sol"]
      here ensures the exit engine sees the same non-zero size.
    """
    # No-op in DRYRUN / package-dryrun environments
    if _dryrun_env_on() or _pkg_is_dryrun:
        return

    # normalize size (this is the value the exit engine + PnL will see)
    spend_sol = _normalize_spend_sol(ev)

    # metrics
    try:
        bump("buys_filled")
    except Exception:
        pass

    # structured log
    try:
        log.event(
            "BUY_FILLED",
            mint=ev.get("mint"),
            symbol=ev.get("symbol"),
            pool=ev.get("pool"),
            spend_sol=spend_sol,
            sig=ev.get("signature"),
        )
    except Exception:
        pass

    # analytics hook (success)
    try:
        record_buy_result(
            ok=True,
            reason="filled",
            spend_sol=spend_sol,
        )
    except Exception:
        pass

    # Primary alert path (AlertManager)
    if NOTIFY_BUY_FILLED:
        try:
            create_task(
                _am().notify_trade_fill(
                    side="BUY",
                    mint=ev.get("mint"),
                    symbol=ev.get("symbol"),
                    pool=ev.get("pool"),
                    sig=ev.get("signature"),
                    spend_sol=spend_sol,
                    out_amount=ev.get("amount_raw"),  # if present
                )
            )
        except Exception as _e:
            log.warning(
                "ALERT_BUY_FILLED_FAIL",
                extra={"err": str(_e)[:200]},
            )

    # Kick an intraday Notion push (debounced, safe if disabled)
    asyncio.create_task(_notion_intraday_snapshot_push())


async def _buy_hook_failed(payload: Dict[str, Any]) -> None:
    """
    Called when a BUY fails in the router.

    payload: {mint, pool, symbol, spend_sol?, amount_sol?, route, hint, reason, error}
    """
    # normalize size for analytics
    spend_sol = _normalize_spend_sol(payload)

    # structured log
    try:
        log.event(
            "BUY_FAILED",
            mint=payload.get("mint"),
            symbol=payload.get("symbol"),
            pool=payload.get("pool"),
            route=payload.get("route"),
            spend_sol=spend_sol,
            reason=payload.get("reason"),
            error=payload.get("error"),
        )
    except Exception:
        pass

    # Alert
    if NOTIFY_BUY_FAILED:
        try:
            txt = (
                f"🔴 BUY FAILED {payload.get('symbol') or ''}\n"
                f"mint={payload.get('mint')}  route={payload.get('route') or 'auto'}\n"
                f"size={spend_sol or '—'} SOL\n"
                f"reason={payload.get('reason') or '—'}  err={payload.get('error') or '—'}"
            )
            create_task(_am().notify_text(txt))
        except Exception as _e:
            log.warning(
                "ALERT_BUY_FAILED_FAIL",
                extra={"err": str(_e)[:200]},
            )

    # Optional telemetry for guardrails / analytics
    try:
        record_buy_result(
            ok=False,
            reason=str(payload.get("reason") or "failed"),
            error=payload.get("error"),
            spend_sol=spend_sol,
        )
    except Exception:
        pass

# ---------------------------
# Strategy
# ---------------------------
class Strategy:
    def __init__(self) -> None:
        # How many buys can run in parallel (use global MAX_CONCURRENCY)
        self._sema = asyncio.Semaphore(MAX_CONCURRENCY)

        # How many momentum gates can run in parallel
        gate_max = _as_int(
            os.getenv("ENTRY_MOMENTUM_MAX_PARALLEL", str(ENTRY_MOMO_MAX_PARALLEL)),
            ENTRY_MOMO_MAX_PARALLEL,
        )
        self._gate_sema = asyncio.Semaphore(max(1, gate_max))

        # Per-mint momentum state while we observe trades/prices in the window
        # keyed by *raw mint* so on_trade() can update counts safely
        self._gate_state: Dict[str, Dict[str, Any]] = {}

        # Momentum config (mirrors env/constants)
        self.momo_enabled: bool = bool(ENTRY_MOMENTUM_ENABLED)
        self.momo_min_trades: int = int(ENTRY_MOMO_MIN_TRADES)
        self.momo_min_up_pct: float = float(ENTRY_MOMO_MIN_UP_PCT)
        self.momo_window_sec: int = int(ENTRY_MOMO_WINDOW_SEC)
        self.momo_price_tries: int = int(ENTRY_MOMO_PRICE_TRIES)
        self.momo_price_delay_s: float = float(ENTRY_MOMO_PRICE_DELAY_S)
        self.momo_allow_no_data_pump: bool = bool(ENTRY_MOMO_ALLOW_NO_DATA_FOR_PUMP)

        # Diagnostics (mirrored into DIAG_MINUTE)
        self._diag: Dict[str, int] = {
            "events_total": 0,
            "events_trade": 0,
            "events_new_token": 0,
            "rejected_filters": 0,
            "rejected_guardrails": 0,
            "rejected_momo": 0,  # strict momentum rejections only
            "buys_submitted": 0,
            "buys_filled": 0,
        }

    # ------------------------------------------------------------------ #
    # Internal helpers
    # ------------------------------------------------------------------ #
    def _get_gate_state(self, mint_raw: str) -> Dict[str, Any]:
        """
        Safely get/create gate state for a raw mint.

        This avoids KeyError races when gate-state is cleared or was never
        initialised (e.g. dedupe / early return).
        """
        st = self._gate_state.get(mint_raw)
        if st is None:
            st = {}
            self._gate_state[mint_raw] = st
        return st

    # --- DIAGNOSTICS -----------------------------------------------------
    async def diag_loop(
        self,
        stop_evt: asyncio.Event,
        interval_s: float = 60.0,
    ) -> None:
        """
        Periodic diagnostics emitter: logs event rate and rejection counts.
        """
        last_total = 0
        while not stop_evt.is_set():
            await asyncio.sleep(interval_s)
            et = self._diag["events_total"]
            rate = (et - last_total) / max(interval_s, 1e-3)
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

    # --- TRADE TICKS (for momo counts + exit engine) --------------------
    async def on_trade(self, raw_event: Dict[str, Any]) -> None:
        """
        Trade ticks:
          - feed exit_engine for follow-through logic
          - increment trade counts for momentum windows per mint
        """
        self._diag["events_total"] += 1
        self._diag["events_trade"] += 1

        # Pass trade ticks into exit engine (for follow-through logic, etc.)
        if EXIT_ENGINE_ENABLED:
            try:
                await exit_engine.on_trade_tick(raw_event)
            except Exception:
                if DEBUG_TX:
                    traceback.print_exc()

        # Update momentum trade count for this mint (if it's being gated)
        m = str(
            _get_first(
                raw_event,
                "mint",
                "mintAddress",
                "mint_address",
                "id",
                default="",
            )
        ).strip()
        if not m:
            return

        st = self._gate_state.get(m)
        if st is not None:
            st["trades"] = int(st.get("trades", 0)) + 1

    # --- MOMENTUM DECISION (pool-aware) ---------------------------------
    def _momentum_decision(
        self,
        *,
        pool: str,
        mint: str,
        symbol: str,
        trades: int,
        price0: float,
        price1: float,
    ) -> bool:
        """
        Returns True = momentum OK / not blocking
                False = block entry

        Behaviour:
          - Pump:
              * if trades < min_trades:
                    - obey MOMO_ALLOW_NO_DATA_FOR_PUMP (bypass or reject)
              * if trades >= min_trades and price data is missing:
                    - trades-only gate ⇒ allow (with log)
              * if trades >= min_trades and price data present:
                    - full pct_up + trades checks
          - Raydium:
              * no data → advisory only (log, do NOT block)
              * with data but weak momentum → reject
          - Other pools:
              * no data → reject
              * with data but weak momentum → reject
        """
        if not self.momo_enabled:
            return True

        pool = (pool or "").lower()
        no_trades = trades < self.momo_min_trades
        no_price_data = (price0 <= 0.0 or price1 <= 0.0)

        # ───── Pump-special handling ─────────────────────────────────────
        if pool in ("pump", "pumpfun", "pump.fun"):
            # 1) Absolutely no trades in the window
            if no_trades:
                if self.momo_allow_no_data_pump:
                    log.event(
                        "MOMO_BYPASS_NO_TRADES_PUMP",
                        pool=pool,
                        mint=mint,
                        symbol=symbol,
                        trades=trades,
                        price0=price0,
                        price1=price1,
                        min_trades=self.momo_min_trades,
                        min_up_pct=self.momo_min_up_pct,
                    )
                    return True

                log.event(
                    "MOMO_SKIP_NO_TRADES_PUMP",
                    pool=pool,
                    mint=mint,
                    symbol=symbol,
                    trades=trades,
                    price0=price0,
                    price1=price1,
                    min_trades=self.momo_min_trades,
                    min_up_pct=self.momo_min_up_pct,
                )
                try:
                    record_buy_result(ok=False, reason="momo_no_trades_pump")
                except Exception:
                    pass
                self._diag["rejected_momo"] += 1
                return False

            # 2) We DO have trades >= min_trades
            if no_price_data:
                # Jupiter can't price it yet – rely on trades + upstream filters.
                log.event(
                    "MOMO_PUMP_TRADES_ONLY_OK",
                    pool=pool,
                    mint=mint,
                    symbol=symbol,
                    trades=trades,
                    price0=price0,
                    price1=price1,
                    min_trades=self.momo_min_trades,
                    min_up_pct=self.momo_min_up_pct,
                )
                return True
            # else: fall through to generic pct_up evaluation below

        # ───── Generic no-data handling (non-pump, or pump with price) ──
        no_data = no_trades or no_price_data

        if no_data:
            # Raydium: advisory only on no-data (do NOT block)
            if pool == "raydium":
                log.event(
                    "MOMO_SKIP_NO_DATA",
                    pool=pool,
                    mint=mint,
                    symbol=symbol,
                    trades=trades,
                    price0=price0,
                    price1=price1,
                    min_trades=self.momo_min_trades,
                    min_up_pct=self.momo_min_up_pct,
                )
                return True

            # Other pools: strict
            log.event(
                "MOMO_SKIP_NO_DATA",
                pool=pool,
                mint=mint,
                symbol=symbol,
                trades=trades,
                price0=price0,
                price1=price1,
                min_trades=self.momo_min_trades,
                min_up_pct=self.momo_min_up_pct,
            )
            try:
                record_buy_result(ok=False, reason="momo_no_data")
            except Exception:
                pass
            self._diag["rejected_momo"] += 1
            return False

        # ───── HAVE DATA: evaluate real momentum ────────────────────────
        pct_up = ((price1 / max(price0, 1e-12)) - 1.0) * 100.0
        up_ok = pct_up >= self.momo_min_up_pct
        trades_ok = trades >= self.momo_min_trades

        log.event(
            "MOMO_EVAL",
            pool=pool,
            mint=mint,
            symbol=symbol,
            trades=trades,
            price0=price0,
            price1=price1,
            pct_up=pct_up,
            min_trades=self.momo_min_trades,
            min_up_pct=self.momo_min_up_pct,
            up_ok=int(up_ok),
            trades_ok=int(trades_ok),
        )

        if not (up_ok and trades_ok):
            reasons: list[str] = []
            if not up_ok:
                reasons.append(f"up {pct_up:.2f}% < {self.momo_min_up_pct}%")
            if not trades_ok:
                reasons.append(f"trades {trades} < {self.momo_min_trades}")

            log.event(
                "MOMO_REJECT",
                pool=pool,
                mint=mint,
                symbol=symbol,
                pct_up=pct_up,
                trades=trades,
                reasons="; ".join(reasons) or "unknown",
            )
            try:
                record_buy_result(ok=False, reason="momo_reject")
            except Exception:
                pass
            self._diag["rejected_momo"] += 1
            return False

        log.event(
            "MOMO_OK",
            pool=pool,
            mint=mint,
            symbol=symbol,
            pct_up=pct_up,
            trades=trades,
            min_trades=self.momo_min_trades,
            min_up_pct=self.momo_min_up_pct,
        )
        return True

    # --- MOMENTUM GATE ---------------------------------------------------
    async def _gate_candidate(
        self,
        ui: Dict[str, Any],
        mint_raw: str,
        mint_norm: str,
    ) -> None:
        """
        Momentum gate that:
          - waits for a short window
          - counts trades + price move
          - enforces pool-aware momentum thresholds
          - runs guardrails
          - finally calls maybe_buy()
        """
        IS_DRYRUN = (_dryrun_env_on() or _pkg_is_dryrun)

        async with self._gate_sema:
            # Dedupe gating per mint
            if DEDUPE_ENABLED:
                if not claim_once(f"gate:{mint_norm}", ttl=int(PF_GATE_TTL_SEC)):
                    if DEBUG_TX:
                        print(
                            f".[dedupe] gate already running for {mint_norm} — skip",
                            flush=True,
                        )
                    return

            pool = str(ui.get("pool") or "").lower()

            # Initialise gate state (keyed by raw mint so on_trade() updates)
            st = self._get_gate_state(mint_raw)
            st["trades"] = 0
            st["price0"] = 0.0

            # First price sample (price0)
            price0 = 0.0
            if _is_probable_mint(mint_norm):
                for _ in range(max(1, self.momo_price_tries)):
                    try:
                        price0 = await _route_price_ui(mint_norm)
                    except Exception:
                        price0 = 0.0
                    if price0 > 0:
                        break
                    await asyncio.sleep(self.momo_price_delay_s)

            # Safely persist price0 back into gate-state
            st = self._get_gate_state(mint_raw)
            st["price0"] = price0

            # Wait the momentum window
            await asyncio.sleep(max(1, self.momo_window_sec))

            # Snapshot state after the window
            st = self._gate_state.get(mint_raw, {})
            trades = int(st.get("trades", 0))

            price1 = 0.0
            if _is_probable_mint(mint_norm):
                try:
                    price1 = await _route_price_ui(mint_norm)
                except Exception:
                    price1 = 0.0

            # Clear gate state (best-effort)
            self._gate_state.pop(mint_raw, None)

            # --- MOMENTUM DECISION (pool-aware) -------------------------
            symbol = ui.get("symbol") or ui.get("name") or "?"
            momo_ok = self._momentum_decision(
                pool=pool,
                mint=mint_norm,
                symbol=symbol,
                trades=trades,
                price0=price0,
                price1=price1,
            )
            if not momo_ok:
                # strict momentum rejections already logged + diag updated
                return

            # ---- Guardrails before executing a buy ----------------------
            if PAUSE_TRADING or not buys_allowed():
                if DEBUG_TX:
                    print(
                        "[guardrails] buys not allowed at gate decision — skip",
                        flush=True,
                    )
                try:
                    record_buy_result(ok=False, reason="guardrails_halt")
                except Exception:
                    pass
                self._diag["rejected_guardrails"] += 1
                return

            max_tradeable = MAX_TRADEABLE
            if max_tradeable > 0:
                try:
                    if len(list(all_positions())) >= max_tradeable:
                        log.event("BUY_BLOCK_MAX_TRADEABLE", cap=max_tradeable)
                        try:
                            record_buy_result(ok=False, reason="max_tradeable")
                        except Exception:
                            pass
                        self._diag["rejected_guardrails"] += 1
                        return
                except Exception:
                    pass

            # Dedupe final buy
            if DEDUPE_ENABLED:
                if not claim_once(
                    f"buy:{mint_norm}",
                    ttl=int(PF_BUY_TTL_SEC),
                ):
                    if DEBUG_TX:
                        print(
                            f".[dedupe] buy already claimed for {mint_norm} — skip",
                            flush=True,
                        )
                    try:
                        record_buy_result(ok=False, reason="buy_claimed_elsewhere")
                    except Exception:
                        pass
                    return

            # ---- Execute buy via maybe_buy (supports meta return) -------
            try:
                async with self._sema:
                    result = await maybe_buy(
                        mint=mint_norm,
                        ui=ui | {"dryrun": IS_DRYRUN},
                        on_buy_submitted=_buy_hook_submitted,
                        on_buy_filled=_buy_hook_filled,
                        on_buy_failed=_buy_hook_failed,
                    )
                    self._diag["buys_submitted"] += 1

                    # Support both:
                    #   tx_sig = await maybe_buy(...)
                    #   (tx_sig, meta) = await maybe_buy(...)
                    tx_sig: Optional[str]
                    meta: Dict[str, Any] = {}
                    amount_raw_meta: Optional[int] = None

                    if isinstance(result, tuple) and len(result) == 2:
                        tx_sig, meta = result  # type: ignore[assignment]
                        if not isinstance(meta, dict):
                            meta = {}
                    elif isinstance(result, str) or result is None:
                        tx_sig = result  # type: ignore[assignment]
                    else:
                        tx_sig = None

                    # ── Extract size from meta and mirror into UI ────────
                    if isinstance(meta, dict):
                        for k in (
                            "amount_raw",
                            "out_amount",
                            "raw_out",
                            "raw_amount_out",
                            "token_amount_raw",
                        ):
                            v = meta.get(k)
                            if isinstance(v, (int, float)) and v > 0:
                                amount_raw_meta = int(v)
                                break

                        if amount_raw_meta and amount_raw_meta > 0:
                            ui["amount_raw"] = amount_raw_meta

                    if tx_sig:
                        # Fire submitted hook again as a safety net (if router didn't)
                        try:
                            await _buy_hook_submitted({"mint": mint_norm, **ui})
                        except Exception:
                            pass

                        ok, info = await _confirm_sig(tx_sig)
                        if ok:
                            log.info(
                                "BUY_FILLED",
                                extra={
                                    "mint": mint_norm,
                                    "tx_sig": tx_sig,
                                    "slot": info,
                                },
                            )

                            # Alerts (Telegram/Notion) – secondary path,
                            # primary is _buy_hook_filled
                            try:
                                create_task(
                                    _am().notify_trade_fill(
                                        side="BUY",
                                        mint=mint_norm,
                                        symbol=ui.get("symbol"),
                                        pool=ui.get("pool") or "unknown",
                                        sig=tx_sig,
                                        spend_sol=ui.get("spend_sol"),
                                        out_amount=ui.get("amount_raw"),
                                    )
                                )
                            except Exception as _e:
                                log.warning(
                                    "ALERT_BUY_FILLED_FAIL",
                                    extra={"err": str(_e)[:200]},
                                )

                            # Exit engine hook (fallback; primary wiring is _buy_hook_filled)
                            if EXIT_ENGINE_ENABLED:
                                try:
                                    ev = {
                                        "mint": mint_norm,
                                        "signature": tx_sig,
                                        "symbol": ui.get("symbol"),
                                        "spend_sol": ui.get("spend_sol") or 0.0,
                                        "pool": ui.get("pool") or "unknown",
                                    }

                                    # Best-effort amount_raw extraction from meta first, then UI
                                    amount_raw: Optional[int] = amount_raw_meta
                                    if amount_raw is None:
                                        for k in (
                                            "amount_raw",
                                            "position_raw",
                                            "size_raw",
                                            "raw_out",
                                            "raw_amount_out",
                                            "out_amount",
                                            "token_amount_raw",
                                        ):
                                            v = ui.get(k)
                                            if isinstance(v, int) and v > 0:
                                                amount_raw = v
                                                break
                                            if isinstance(v, float) and v > 0:
                                                amount_raw = int(v)
                                                break

                                    await exit_engine.on_buy_filled(
                                        ev,
                                        tx_sig=tx_sig,
                                        amount_raw=amount_raw,
                                    )
                                except Exception:
                                    if DEBUG_TX:
                                        traceback.print_exc()

                            self._diag["buys_filled"] += 1
                            try:
                                record_buy_result(ok=True, reason="executed", sig=tx_sig)
                            except Exception:
                                pass
                            asyncio.create_task(_post_buy_exit_test(mint_norm, tx_sig))
                        else:
                            log.error(
                                "BUY_FAILED",
                                extra={"mint": mint_norm, "tx_sig": tx_sig, "reason": info},
                            )
                            try:
                                await _buy_hook_failed(
                                    {"mint": mint_norm, "reason": info, **ui}
                                )
                            except Exception:
                                pass
                            try:
                                record_buy_result(ok=False, reason=str(info))
                            except Exception:
                                pass
                    else:
                        # DRYRUN path usually returns None
                        if IS_DRYRUN:
                            log.event("BUY_DRYRUN_EXIT", mint=mint_norm)
                            try:
                                record_buy_result(ok=False, reason="dryrun_exit")
                            except Exception:
                                pass
                        else:
                            try:
                                record_buy_result(ok=False, reason="no_tx_sig")
                            except Exception:
                                pass

            except Exception:
                traceback.print_exc()
                try:
                    record_buy_result(ok=False, reason="exception")
                except Exception:
                    pass

    # --- NEW TOKENS (entry point for gating) -----------------------------
    async def on_new_token(self, raw_event: Dict[str, Any]) -> None:
        """
        New-token handler from scanners (PumpPortal / Raydium):
          - top-level guardrails
          - mint normalization + basic filters
          - liquidity screening
          - dedupe & momentum gate (or direct-buy if disabled)
        """
        DEBUG_LOCAL = DEBUG_TX  # just an alias for readability
        IS_DRYRUN = (_dryrun_env_on() or _pkg_is_dryrun)

        self._diag["events_total"] += 1
        self._diag["events_new_token"] += 1

        # Pool + force-buy flag (for Raydium debug / aggression)
        pool = str(raw_event.get("pool") or "pump").lower()
        force_ray = (
            os.getenv("ENTRY_FORCE_BUY_RAYDIUM", "0") == "1"
            and pool == "raydium"
        )

        # Top-level guardrails
        if PAUSE_TRADING or not buys_allowed():
            if DEBUG_LOCAL:
                print("[guardrails] buys not allowed now — skip", flush=True)
            self._diag["rejected_guardrails"] += 1
            try:
                record_buy_result(ok=False, reason="guardrails_top")
            except Exception:
                pass
            return

        max_tradeable = MAX_TRADEABLE
        if max_tradeable > 0:
            try:
                if len(list(all_positions())) >= max_tradeable:
                    log.event("BUY_BLOCK_MAX_TRADEABLE", cap=max_tradeable)
                    self._diag["rejected_guardrails"] += 1
                    try:
                        record_buy_result(ok=False, reason="max_tradeable_top")
                    except Exception:
                        pass
                    return
            except Exception:
                pass

        # Extract and normalise mint
        mint_source = _extract_raw_mint(raw_event)
        if not mint_source:
            if DEBUG_LOCAL:
                print("[strategy] missing mint — skip", flush=True)
            return

        mint_norm = _normalize_from_source(mint_source)
        if not _is_probable_mint(mint_norm):
            if DEBUG_LOCAL:
                print(
                    f"[strategy] implausible mint after normalize => skip ({mint_source})",
                    flush=True,
                )
            return

        # ---------- BASIC FILTERS (age / name / blacklist / scanner metrics)
        basic_ok = _passes_basic_filters(raw_event)

        if not basic_ok and not force_ray:
            # hard reject
            self._diag["rejected_filters"] += 1

            liq = float(
                raw_event.get("liq_usd")
                or raw_event.get("liq")
                or 0.0
            )
            vol5 = float(
                raw_event.get("vol_5m_usd")
                or raw_event.get("vol5")
                or 0.0
            )
            # Support both naming schemes: price_change_5m_pct / chg5 / up_5m_pct
            up5 = float(
                raw_event.get("price_change_5m_pct")
                or raw_event.get("chg5")
                or raw_event.get("up_5m_pct")
                or 0.0
            )
            buys5 = int(raw_event.get("buys_5m") or raw_event.get("buys5") or 0)
            sells5 = int(raw_event.get("sells_5m") or raw_event.get("sells5") or 0)
            age_min = float(
                raw_event.get("token_age_min")
                or raw_event.get("age_min")
                or 0.0
            )

            log.event(
                "ENTRY_FILTER_REJECT",
                pool=pool,
                mint=mint_norm,
                symbol=_extract_symbol(raw_event),
                liq_usd=liq,
                vol_5m_usd=vol5,
                up_5m_pct=up5,
                buys5=buys5,
                sells5=sells5,
                age_min=age_min,
            )
            if DEBUG_LOCAL:
                print(
                    f"[ENTRY_FILTER_REJECT] pool={pool} sym={_extract_symbol(raw_event)} "
                    f"liq={liq:.1f} vol5={vol5:.1f} up5={up5:.2f}% "
                    f"buys5={buys5} sells5={sells5} age={age_min:.1f}m",
                    flush=True,
                )
            try:
                record_buy_result(ok=False, reason="basic_filters")
            except Exception:
                pass
            return

        if not basic_ok and force_ray:
            # We *bypass* the basic filters for Raydium when the flag is set
            log.event(
                "ENTRY_FILTER_BYPASS_FORCE_RAYDIUM",
                pool=pool,
                mint=mint_norm,
                symbol=_extract_symbol(raw_event),
            )
            if DEBUG_LOCAL:
                print(
                    f"[ENTRY_FILTER_BYPASS_FORCE_RAYDIUM] sym={_extract_symbol(raw_event)} mint={mint_norm}",
                    flush=True,
                )

        # ---------- LIQUIDITY SCREEN -----------------------------------
        dec_from_liq: Optional[int] = None
        if LIQ_FILTER_ENABLED and not force_ray:
            ok, rep = await _screen_with_retry(mint_norm, tries=3, first_wait=0.8)
            if not ok and str((rep or {}).get("reason")) == "no_account":
                # Fresh bond curve: wait a bit for mint to become readable
                dec_from_liq = await _await_mint_ready(
                    mint_norm,
                    max_wait_s=45,
                    step_s=3.0,
                )
                if dec_from_liq is not None:
                    ok = True
                    rep = {
                        "ok": True,
                        "reason": "await_mint_ready",
                        "decimals": dec_from_liq,
                    }

            if not ok:
                if DEBUG_LOCAL:
                    print(
                        f"[liquidity] screen failed for {mint_norm}: {rep}",
                        flush=True,
                    )
                self._diag["rejected_filters"] += 1
                log.event(
                    "ENTRY_LIQ_REJECT",
                    pool=pool,
                    mint=mint_norm,
                    reason=str((rep or {}).get("reason")),
                )
                try:
                    record_buy_result(ok=False, reason="liquidity_reject")
                except Exception:
                    pass
                return

            if dec_from_liq is None:
                dec_from_liq = (rep or {}).get("decimals")

            log.event(
                "ENTRY_LIQ_OK",
                mint=mint_norm,
                priceImpactPct=(rep or {}).get("priceImpactPct"),
                dec=dec_from_liq,
            )

        elif LIQ_FILTER_ENABLED and force_ray:
            # Skip liq screen entirely in force-ray mode
            log.event(
                "ENTRY_LIQ_BYPASS_FORCE_RAYDIUM",
                pool=pool,
                mint=mint_norm,
                symbol=_extract_symbol(raw_event),
            )
            if DEBUG_LOCAL:
                print(
                    f"[ENTRY_LIQ_BYPASS_FORCE_RAYDIUM] sym={_extract_symbol(raw_event)} mint={mint_norm}",
                    flush=True,
                )

        # Build UI payload / context for downstream
        ui: Dict[str, Any] = dict(raw_event)
        ui.setdefault("pool", pool)
        ui.setdefault("symbol", _extract_symbol(raw_event))
        ui.setdefault("name", _extract_name(raw_event))
        ui["rawMint"] = mint_source
        ui["mint"] = mint_norm
        ui["normalized"] = (mint_norm != mint_source)
        if dec_from_liq is not None:
            ui["decimals"] = dec_from_liq

        print(
            f"[trade] pool={ui.get('pool','?')} "
            f"symbol={(ui.get('symbol') or '?')} mint={mint_norm}",
            flush=True,
        )

        # Dedupe gate proposals
        if DEDUPE_ENABLED and self.momo_enabled:
            if not claim_once(
                f"gate-propose:{mint_norm}",
                ttl=int(PF_GATE_TTL_SEC),
            ):
                if DEBUG_LOCAL:
                    print(
                        f".[dedupe] gate-propose taken for {mint_norm} — skip",
                        flush=True,
                    )
                return

        # Momentum enabled → send into gate; otherwise buy immediately
        if self.momo_enabled:
            asyncio.create_task(self._gate_candidate(ui, mint_source, mint_norm))
        else:
            # Direct-buy path, still respecting dedupe/guardrails
            if DEDUPE_ENABLED and not claim_once(
                f"buy:{mint_norm}",
                ttl=int(PF_BUY_TTL_SEC),
            ):
                if DEBUG_LOCAL:
                    print(
                        f".[dedupe] buy already claimed for {mint_norm} — skip",
                        flush=True,
                    )
                return

            async with self._sema:
                try:
                    if PAUSE_TRADING or not buys_allowed():
                        if DEBUG_LOCAL:
                            print(
                                "[guardrails] buys not allowed at execution — skip",
                                flush=True,
                            )
                        self._diag["rejected_guardrails"] += 1
                        try:
                            record_buy_result(ok=False, reason="guardrails_exec")
                        except Exception:
                            pass
                        return

                    max_tradeable = MAX_TRADEABLE
                    if max_tradeable > 0:
                        try:
                            if len(list(all_positions())) >= max_tradeable:
                                log.event(
                                    "BUY_BLOCK_MAX_TRADEABLE",
                                    cap=max_tradeable,
                                )
                                self._diag["rejected_guardrails"] += 1
                                try:
                                    record_buy_result(
                                        ok=False,
                                        reason="max_tradeable_exec",
                                    )
                                except Exception:
                                    pass
                                return
                        except Exception:
                            pass

                    result = await maybe_buy(
                        mint=mint_norm,
                        ui=ui | {"dryrun": IS_DRYRUN},
                        on_buy_submitted=_buy_hook_submitted,
                        on_buy_filled=_buy_hook_filled,
                        on_buy_failed=_buy_hook_failed,
                    )
                    self._diag["buys_submitted"] += 1

                    # Same `(tx_sig, meta)` handling as gated path
                    tx_sig: Optional[str]
                    meta: Dict[str, Any] = {}
                    amount_raw_meta: Optional[int] = None

                    if isinstance(result, tuple) and len(result) == 2:
                        tx_sig, meta = result  # type: ignore[assignment]
                        if not isinstance(meta, dict):
                            meta = {}
                    elif isinstance(result, str) or result is None:
                        tx_sig = result  # type: ignore[assignment]
                    else:
                        tx_sig = None

                    # Extract size from meta and mirror into UI
                    if isinstance(meta, dict):
                        for k in (
                            "amount_raw",
                            "out_amount",
                            "raw_out",
                            "raw_amount_out",
                            "token_amount_raw",
                        ):
                            v = meta.get(k)
                            if isinstance(v, (int, float)) and v > 0:
                                amount_raw_meta = int(v)
                                break

                        if amount_raw_meta and amount_raw_meta > 0:
                            ui["amount_raw"] = amount_raw_meta

                    if tx_sig:
                        try:
                            await _buy_hook_submitted({"mint": mint_norm, **ui})
                        except Exception:
                            pass

                        ok, info = await _confirm_sig(tx_sig)
                        if ok:
                            log.info(
                                "BUY_FILLED",
                                extra={
                                    "mint": mint_norm,
                                    "tx_sig": tx_sig,
                                    "slot": info,
                                },
                            )

                            try:
                                create_task(
                                    _am().notify_trade_fill(
                                        side="BUY",
                                        mint=mint_norm,
                                        symbol=ui.get("symbol"),
                                        pool=ui.get("pool") or "unknown",
                                        sig=tx_sig,
                                        spend_sol=ui.get("spend_sol"),
                                        out_amount=ui.get("amount_raw"),
                                    )
                                )
                            except Exception as _e:
                                log.warning(
                                    "ALERT_BUY_FILLED_FAIL",
                                    extra={"err": str(_e)[:200]},
                                )

                            # Exit engine hook (fallback; primary wiring is _buy_hook_filled)
                            if EXIT_ENGINE_ENABLED:
                                try:
                                    ev = {
                                        "mint": mint_norm,
                                        "signature": tx_sig,
                                        "symbol": ui.get("symbol"),
                                        "spend_sol": ui.get("spend_sol") or 0.0,
                                        "pool": ui.get("pool") or "legacy",
                                    }

                                    amount_raw: Optional[int] = amount_raw_meta
                                    if amount_raw is None:
                                        for k in (
                                            "amount_raw",
                                            "position_raw",
                                            "size_raw",
                                            "raw_out",
                                            "raw_amount_out",
                                            "out_amount",
                                            "token_amount_raw",
                                        ):
                                            v = ui.get(k)
                                            if isinstance(v, int) and v > 0:
                                                amount_raw = v
                                                break
                                            if isinstance(v, float) and v > 0:
                                                amount_raw = int(v)
                                                break

                                    await exit_engine.on_buy_filled(
                                        ev,
                                        tx_sig=tx_sig,
                                        amount_raw=amount_raw,
                                    )
                                except Exception:
                                    if DEBUG_LOCAL:
                                        traceback.print_exc()

                            self._diag["buys_filled"] += 1
                            try:
                                record_buy_result(ok=True, reason="executed", sig=tx_sig)
                            except Exception:
                                pass
                            asyncio.create_task(_post_buy_exit_test(mint_norm, tx_sig))
                        else:
                            log.error(
                                "BUY_FAILED",
                                extra={"mint": mint_norm, "tx_sig": tx_sig, "reason": info},
                            )
                            try:
                                await _buy_hook_failed(
                                    {"mint": mint_norm, "reason": info, **ui}
                                )
                            except Exception:
                                pass
                            try:
                                record_buy_result(ok=False, reason=str(info))
                            except Exception:
                                pass
                    else:
                        if IS_DRYRUN:
                            log.event("BUY_DRYRUN_EXIT", mint=mint_norm)
                            try:
                                record_buy_result(ok=False, reason="dryrun_exit")
                            except Exception:
                                pass
                        else:
                            try:
                                record_buy_result(ok=False, reason="no_tx_sig")
                            except Exception:
                                pass
                except Exception:
                    print("[strategy] maybe_buy exception:", flush=True)
                    traceback.print_exc()
                    try:
                        record_buy_result(ok=False, reason="exception")
                    except Exception:
                        pass

        # Cooldown between processing new tokens
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
    bot = (os.getenv("TELEGRAM_BOT_TOKEN") or "").strip()
    chat = (
        os.getenv("DAILY_PNL_CHAT_ID") or os.getenv("TELEGRAM_CHAT_ID") or ""
    ).strip()
    if not (bot and chat and httpx):
        log.event("DAILY_PNL_TG_SKIP", reason="missing_cfg_or_httpx")
        return
    try:
        url = f"https://api.telegram.org/bot{bot}/sendMessage"
        payload = {
            "chat_id": chat,
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        }
        async with httpx.AsyncClient(timeout=15) as cli:  # type: ignore[arg-type]
            r = await cli.post(url, json=payload)
            log.event(
                "DAILY_PNL_TG_SENT",
                http_status=getattr(r, "status_code", None),
            )
    except Exception as e:
        log.event("DAILY_PNL_TG_ERROR", error=str(e))


async def _pnl_pg_ensure_table() -> None:
    if not asyncpg or not _PNL_DSN_ASYNC:
        log.event("DAILY_PNL_PG_SKIP", reason="no_asyncpg_or_dsn")
        return
    try:
        conn = await asyncpg.connect(_PNL_DSN_ASYNC)
        try:
            await conn.execute(
                """
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
            """
            )
            log.event("DAILY_PNL_PG_READY", dsn=_PG_DSN_MASK)
        finally:
            await conn.close()
    except Exception as e:
        if DEBUG_TX:
            traceback.print_exc()
        log.event("DAILY_PNL_PG_ERROR", when="ensure_table", error=str(e))


async def _pnl_pg_insert_row(
    *,
    realized_sol: float,
    unrealized_sol: float,
    open_positions: int,
    wallet_sol: Optional[float],
    notes: Dict[str, Any],
) -> None:
    if not asyncpg or not _PNL_DSN_ASYNC:
        log.event("DAILY_PNL_PG_SKIP", reason="no_asyncpg_or_dsn")
        return
    try:
        conn = await asyncpg.connect(_PNL_DSN_ASYNC)
        try:
            await conn.execute(
                """
                INSERT INTO daily_pnl (at_utc, realized_sol, unrealized_sol, open_positions, wallet_sol, notes)
                VALUES (NOW() AT TIME ZONE 'UTC', $1, $2, $3, $4, $5::jsonb)
                """,
                realized_sol,
                unrealized_sol,
                open_positions,
                wallet_sol,
                json.dumps(notes),
            )
            log.event(
                "DAILY_PNL_PG_INSERTED",
                realized_sol=realized_sol,
                unrealized_sol=unrealized_sol,
                open_positions=open_positions,
            )
        finally:
            await conn.close()
    except Exception as e:
        if DEBUG_TX:
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
            qty = float(
                p.get("amount")
                or p.get("qty")
                or p.get("size")
                or 0
            ) or 0.0
            if qty <= 0:
                continue
            cost = float(
                p.get("entry_total_sol")
                or p.get("cost_basis_sol")
                or p.get("cost")
                or 0.0
            )
            price = 0.0
            try:
                price = float(await _route_price_ui(mint))
            except Exception:
                price = 0.0
            cur_val = price * qty if price > 0 else 0.0
            unrl = cur_val - cost
            total_unrl += unrl
            open_n += 1
            details["legs"].append(
                {
                    "mint": mint,
                    "qty": qty,
                    "entry_cost_sol": cost,
                    "mark_price_sol": price,
                    "cur_val_sol": cur_val,
                    "unrealized_sol": unrl,
                }
            )
        except Exception:
            if DEBUG_TX:
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
        # NOTE: Postgres-backed realized_from_positions (if implemented)
        from utils_pkg.positions_pg import (  # type: ignore
            realized_today_sol as pg_realized_today_sol,
        )

        v = await pg_realized_today_sol()
        return float(v or 0.0), {
            "source": "utils_pkg.positions_pg.realized_today_sol"
        }
    except Exception:
        pass
    return 0.0, {"source": "fallback_zero"}


async def _run_daily_pnl_loop(stop_evt: asyncio.Event) -> None:
    if not DAILY_PNL_ENABLED:
        return

    await _pnl_pg_ensure_table()
    _last_sent_ts: Optional[float] = None

    async def _once():
        nonlocal _last_sent_ts
        now_ts = _dt.datetime.now(_dt.timezone.utc).timestamp()
        if _last_sent_ts and (now_ts - _last_sent_ts) < (DAILY_PNL_DEBOUNCE_MIN * 60):
            log.event(
                "DAILY_PNL_DEBOUNCE_SKIP",
                since_sec=int(now_ts - _last_sent_ts),
            )
            return

        unrl_sol, open_n, unrl_details = await _estimate_unrealized_from_positions()
        realized_sol, r_meta = await _estimate_realized_today()
        wallet_sol = await _get_wallet_sol_balance()

        # --- Notion: push daily snapshot (safe no-op if NOTION_ENABLED != 1)
        try:
            page = push_daily_pnl(
                {
                    "date": utc_today_iso(),
                    "realized_sol": realized_sol,
                    "unrealized_sol": unrl_sol,
                    "open_positions": open_n,
                    "wallet_sol": wallet_sol,
                    "net_sol": (realized_sol + unrl_sol),
                    "note": "Auto-daily sync",
                }
            )
            if page:
                log.event("NOTION_DAILY_PNL_UPSERTED", page_id=page)
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

    if DAILY_PNL_TEST_NOW and not stop_evt.is_set():
        with contextlib.suppress(Exception):
            await _once()

    while not stop_evt.is_set():
        now = _dt.datetime.now(_dt.timezone.utc)
        run_at = now.replace(
            hour=DAILY_PNL_HOUR_UTC,
            minute=0,
            second=0,
            microsecond=0,
        )
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
async def _maybe_start_health_server(
    stop_evt: asyncio.Event,
) -> Optional[asyncio.Task]:
    if not (web and HEALTH_PORT > 0):
        return None
    app = web.Application()

    async def _health(_request):
        return web.json_response(
            {
                "ok": True,
                "ts": _dt.datetime.now(_dt.timezone.utc).isoformat(),
                "dryrun": _dryrun_env_on() or bool(_pkg_is_dryrun),
                "pools": sorted(POOLS_FILTER) if POOLS_FILTER else ["<all>"],
            }
        )

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
# Streaming loop (PumpPortal)
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
    diag_task = asyncio.create_task(
        strat.diag_loop(local_stop),
        name="diag.minute",
    )

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
                    log.every(
                        "pump.subscribe",
                        n=30,
                        msg="PUMP_SUBSCRIBED",
                        via=name,
                    )
                    return
                except Exception as e:
                    log.every(
                        "pump.subscribe.err",
                        n=5,
                        msg="PUMP_SUBSCRIBE_FAIL",
                        via=name,
                        err=str(e),
                    )
        try:
            sub = getattr(client, "_subscribe", None)
            if callable(sub):
                res = sub("trades")
                if asyncio.iscoroutine(res):
                    await res
                log.every(
                    "pump.subscribe",
                    n=30,
                    msg="PUMP_SUBSCRIBED",
                    via="_subscribe",
                )
        except Exception as e:
            log.every(
                "pump.subscribe.err",
                n=5,
                msg="PUMP_SUBSCRIBE_FAIL",
                via="_subscribe",
                err=str(e),
            )

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
                            await maybe_awaitable  # type: ignore[arg-type]
    finally:
        with contextlib.suppress(Exception):
            local_stop.set()
            diag_task.cancel()
            await asyncio.wait_for(diag_task, timeout=1.5)


# ---------------------------
# Raydium scanner supervisor
# ---------------------------
async def _run_raydium_scanner_forever(stop_evt: asyncio.Event) -> None:
    """
    Periodically run the Raydium scanner and feed its candidates into the
    Strategy.new_token pipeline as synthetic 'raydium' events.
    """
    scan_raydium = _get_raydium_scanner()
    if scan_raydium is None:
        log.event("SCAN_RAYDIUM_UNAVAILABLE")
        return

    base_interval = RAYDIUM_SCAN_INTERVAL_SEC
    jitter_seed = (os.getpid() % 7) / 6.0  # 0.0–1.0
    backoff = 2.0
    first_log = True
    warned_no_strat = False  # avoid spamming logs

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

            runners_raw = await scan_raydium()

            candidates: list[dict] = []
            if isinstance(runners_raw, dict):
                # Normal shape: {"pf":[...], "fb":[...]} or {"runners":[...]}
                if isinstance(runners_raw.get("runners"), (list, tuple)):
                    candidates = list(runners_raw["runners"])  # type: ignore[arg-type]
                else:
                    pf = runners_raw.get("pf") or []
                    fb = runners_raw.get("fb") or []
                    if isinstance(pf, (list, tuple)):
                        candidates.extend(pf)
                    if isinstance(fb, (list, tuple)):
                        candidates.extend(x for x in fb)
            elif isinstance(runners_raw, (list, tuple)):
                # Already a flat list of runner dicts
                candidates = list(runners_raw)

            log.event(
                "SCAN_RAYDIUM_RETURNED",
                candidates=len(candidates),
            )

            # 🔑 Get or create a Strategy instance for Raydium
            s = globals().get("strat")
            if s is None:
                try:
                    # Strategy is defined earlier in this module
                    s = Strategy()  # type: ignore[name-defined]
                    globals()["strat"] = s
                    log.event("STRATEGY_INIT_RAYDIUM")
                except NameError:
                    # Strategy class not visible yet – just warn (once) and skip
                    if DEBUG_TX and not warned_no_strat:
                        print(
                            "[raydium] Strategy not defined yet; skipping batch",
                            flush=True,
                        )
                        warned_no_strat = True
                    s = None

            if s is None:
                # Nothing we can do this loop
                if DEBUG_TX:
                    print("[raydium] strat still not ready; skipping batch", flush=True)
            elif candidates:
                for cand in candidates:
                    if not isinstance(cand, dict):
                        continue

                    try:
                        mint = (
                            cand.get("mint")
                            or cand.get("baseMint")
                            or cand.get("tokenAddress")
                            or cand.get("address")
                        )
                        if not mint:
                            continue

                        sym = (
                            cand.get("sym")
                            or cand.get("symbol")
                            or cand.get("baseSymbol")
                            or "?"
                        )

                        liq = cand.get("liq", cand.get("liq_usd", 0.0)) or 0.0
                        vol5 = cand.get("vol5", cand.get("vol_5m_usd", 0.0)) or 0.0
                        chg5 = cand.get("chg5", cand.get("up_5m_pct", 0.0)) or 0.0
                        buys5 = cand.get("buys5", cand.get("buys_5m", 0)) or 0
                        sells5 = cand.get("sells5", cand.get("sells_5m", 0)) or 0
                        age_min = cand.get("age_min", cand.get("ageMinutes", 0.0)) or 0.0

                        raw_event: Dict[str, Any] = {
                            "pool": "raydium",
                            "mint": mint,
                            "symbol": sym,
                            "liq_usd": float(liq),
                            "vol_5m_usd": float(vol5),
                            "up_5m_pct": float(chg5),
                            "buys5": int(buys5),
                            "sells5": int(sells5),
                            "age_min": float(age_min),
                        }

                        pair_addr = cand.get("pairAddress") or cand.get("pair")
                        if pair_addr:
                            raw_event["raydium_pair"] = pair_addr
                        ds_url = cand.get("ds_url") or cand.get("url")
                        if ds_url:
                            raw_event["dexscreener_url"] = ds_url

                        # async fire-and-forget into Strategy
                        asyncio.create_task(s.on_new_token(raw_event))
                    except Exception as e:
                        if DEBUG_TX:
                            print(f"[raydium] candidate build error: {e}", flush=True)
                        continue

            # Normal interval with jitter
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


# ---------------------------
# Background tasks: scanners, PnL, exit worker, risk manager, health
# ---------------------------
async def _start_background_tasks(
    stop_evt: asyncio.Event,
) -> Dict[str, asyncio.Task]:
    tasks: Dict[str, asyncio.Task] = {}

    # Raydium scanner
    want_raydium = getattr(C, "ENABLE_RAYDIUM_SCANNER", True)
    pools_ok = (not POOLS_FILTER) or ("raydium" in POOLS_FILTER)
    if want_raydium and pools_ok:
        t = asyncio.create_task(
            _run_raydium_scanner_forever(stop_evt),
            name="scan.raydium",
        )
        tasks["scan.raydium"] = t
        log.event(
            "SCAN_RAYDIUM_SCHEDULED",
            task="scan.raydium",
            interval=RAYDIUM_SCAN_INTERVAL_SEC,
        )

    # Daily PnL loop
    if DAILY_PNL_ENABLED:
        t = asyncio.create_task(
            _run_daily_pnl_loop(stop_evt),
            name="telemetry.daily_pnl",
        )
        tasks["telemetry.daily_pnl"] = t
        log.event(
            "DAILY_PNL_SCHEDULED",
            hour_utc=DAILY_PNL_HOUR_UTC,
        )

    # Auto-exit worker: processes exit_request:* in Redis and executes sells
    if _as_bool(os.getenv("EXIT_WORKER_ENABLED", "1"), True):
        if redis is None:
            log.event(
                "EXIT_WORKER_SKIP",
                reason="redis_none",
                hint="check REDIS_URL / core_utils",
            )
        else:
            t = asyncio.create_task(
                exit_worker_loop(stop_evt),
                name="exit.worker",
            )
            tasks["exit.worker"] = t
            log.event(
                "EXIT_WORKER_SCHEDULED",
                poll_sec=os.getenv("EXIT_WORKER_POLL_SEC", "3"),
            )

    # Risk manager loop (portfolio-level risk / stop-losses, etc.)
    if RISK_MANAGER_ENABLED:
        t = asyncio.create_task(
            risk_manager_loop(stop_evt),
            name="risk.manager",
        )
        tasks["risk.manager"] = t
        log.event(
            "RISK_MANAGER_SCHEDULED",
            poll_sec=RISK_MANAGER_POLL_SEC,
        )

    # Health server
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
            traces_sample_rate=float(
                os.getenv("SENTRY_TRACES_SAMPLE_RATE", "0.0")
            ),
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
        f"PF_DEBUG_TX={int(DEBUG_TX)} | "
        f"PF_MAX_CONCURRENT={MAX_CONCURRENCY} | "
        f"PF_COOLDOWN_SEC={LOOP_COOLDOWN_SEC}\n"
        f"MOMO: on={int(ENTRY_MOMENTUM_ENABLED)} "
        f"window={ENTRY_MOMO_WINDOW_SEC}s "
        f"min_trades={ENTRY_MOMO_MIN_TRADES} "
        f"min_up={ENTRY_MOMO_MIN_UP_PCT}% "
        f"allow_no_data_pump={int(ENTRY_MOMO_ALLOW_NO_DATA_FOR_PUMP)}\n"
        f"LIQ_FILTER={int(LIQ_FILTER_ENABLED)} | "
        f"EXIT_ENGINE={int(EXIT_ENGINE_ENABLED)} | "
        f"MAX_TRADEABLE={MAX_TRADEABLE}\n"
        f"POOLS={pools} | PUMP_TRADES={int(pump_trades)}\n"
        f"PG(asyncpg) DSN={_PG_DSN_MASK or '<none>'} | HEALTH_PORT={HEALTH_PORT or 'off'}\n"
        "===========================\n",
        flush=True,
    )


def _env_hints() -> None:
    if not (os.getenv("KEYPAIR_JSON") or os.getenv("SOLANA_KEYPAIR_JSON")):
        log.event(
            "HINT_KEYPAIR_JSON_MISSING",
            note="wallet auto-create disabled (ok if mounted)",
        )
    if not (
        os.getenv("TELEGRAM_BOT_TOKEN")
        and (os.getenv("DAILY_PNL_CHAT_ID") or os.getenv("TELEGRAM_CHAT_ID"))
    ):
        log.event(
            "HINT_TELEGRAM_MISSING",
            note="daily pnl / buy notifications off",
        )
    if not (_PNL_DSN_ASYNC or _PG_DSN_ASYNC):
        log.event(
            "HINT_PG_DSN_MISSING",
            note="daily pnl persistence disabled",
        )


async def main(stop_evt: Optional[asyncio.Event] = None) -> None:
    # Heartbeat + boot bump
    with contextlib.suppress(Exception):
        start_global_heartbeat(
            log,
            interval=float(os.getenv("PF_HEARTBEAT_SEC", "45")),
        )
        bump("boot")

    _maybe_init_sentry()
    _boot_banner()
    _env_hints()
    _ensure_keypair_file()

    # Optional router pre-init
    try:
        if router and hasattr(router, "start"):
            await router.start()
            log.event("ROUTER_PRIO_INIT_STARTED", ok=1)
    except Exception as e:
        log.event("ROUTER_PRIO_INIT_FAIL", error=str(e))

    # Optional legacy sell loop if positions already exist
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
            loop.add_signal_handler(
                sig,
                lambda s=sig: asyncio.create_task(
                    _graceful_shutdown(local_stop, sell_task, bg_tasks)
                ),
            )
        except NotImplementedError:
            # Windows / restricted env: ignore
            pass

    backoff = 3.0
    try:
        while not local_stop.is_set():
            try:
                await stream_loop(local_stop)
                print(
                    "[main] stream ended (remote closed). Reconnecting...",
                    flush=True,
                )
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
