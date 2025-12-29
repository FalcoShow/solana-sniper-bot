# -*- coding: utf-8 -*-
from __future__ import annotations

"""
sniper/sniper.py — V3+++ (God-Tier LOCK-IN) Entry Executor + Orchestrator
========================================================================

This file is the CONTROL PLANE for entries (sniper):
- Pulls candidates from scanners (dexscreener / pumpfun / raydium / optional)
- Normalizes into ONE CandidateSnapshot schema (mint/sym/source/ts + metrics)
- Dedupe + rank + runner filter (only moving coins)
- Atomic entry flow (single mint, concurrency-safe):
    candidate → gates → quote/precheck → build swap → send → verify → register position
- Eliminates “silent no-buy” failure modes:
    EVERY reject emits ONE reason bucket log (REJECT bucket + reason + mint)

Hard rules
----------
- No network at import time
- Must not crash on missing optional modules unless SNIPER_FAST_FAIL=1 / MAIN_FAST_FAIL=1
- Must be cancellation-safe (never swallow asyncio.CancelledError)
- Concurrency-safe: per-mint asyncio locks + optional Redis NX lock

Public entrypoints
------------------
async def run_sniper()
async def main()

Notes
-----
- Uses best-effort wiring to existing repo modules:
    utils_pkg.core_utils (resources + Jupiter swaps + telegram)
    utils_pkg.router (quote helpers, optional)
    execution.exits / utils_pkg.exit_engine (register_entry / supervisor)
    utils_pkg.ai_gate / routeguard (optional)
    utils_pkg.ai_buy_scoring (optional softgate)
    utils_pkg.honeypot / ai risk model (optional)
    utils_pkg.ai_db + utils_pkg.data_logger (optional logging + schema)
"""

import asyncio
import contextlib
import importlib
import inspect
import logging
import os
import random
import re
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable, Dict, Iterable, List, Optional, Tuple

# =============================================================================
# Logging (safe + shims)
# =============================================================================
try:
    from logging_setup import get_logger  # type: ignore

    log = get_logger("sniper")
except Exception:  # pragma: no cover
    _LOG_LEVEL = (os.getenv("LOG_LEVEL") or "INFO").upper()
    logging.basicConfig(
        level=getattr(logging, _LOG_LEVEL, logging.INFO),
        format="%(asctime)s %(levelname)s:%(name)s: %(message)s",
    )
    log = logging.getLogger("sniper")


def _ensure_log_shims() -> None:
    if not hasattr(log, "event"):

        def _event(name: str, **kw: Any) -> None:  # pragma: no cover
            try:
                log.info("%s %s", name, kw)
            except Exception:
                pass

        setattr(log, "event", _event)

    if not hasattr(log, "every"):

        def _every(key: str, n: int = 60, msg: Optional[str] = None, **kw: Any) -> None:  # pragma: no cover
            now = time.time()
            store = getattr(_every, "_store", {})  # type: ignore[attr-defined]
            last = float(store.get(key, 0.0))
            if (now - last) >= float(n):
                store[key] = now
                setattr(_every, "_store", store)  # type: ignore[attr-defined]
                try:
                    getattr(log, "event")(msg or key, **kw)
                except Exception:
                    pass

        setattr(log, "every", _every)


_ensure_log_shims()


def _short(x: Any, n: int = 280) -> str:
    try:
        s = str(x)
        return s if len(s) <= n else (s[:n] + "…")
    except Exception:
        return "<?>"


# =============================================================================
# Env/config helpers
# =============================================================================
_TRUE = {"1", "true", "t", "yes", "y", "on"}
_FALSE = {"0", "false", "f", "no", "n", "off"}


def _b(name: str, default: bool = False) -> bool:
    v = os.getenv(name)
    if v is None or str(v).strip() == "":
        return bool(default)
    s = str(v).strip().lower()
    if s in _TRUE:
        return True
    if s in _FALSE:
        return False
    return bool(default)


def _s(name: str, default: str = "") -> str:
    v = os.getenv(name)
    return default if v is None else str(v).strip()


def _i(name: str, default: int) -> int:
    v = os.getenv(name)
    try:
        return int(float(v)) if v not in (None, "") else int(default)
    except Exception:
        return int(default)


def _f(name: str, default: float) -> float:
    v = os.getenv(name)
    try:
        return float(v) if v not in (None, "") else float(default)
    except Exception:
        return float(default)


def _clamp(x: float, lo: float, hi: float) -> float:
    return lo if x < lo else hi if x > hi else x


def _fast_fail() -> bool:
    return _b("SNIPER_FAST_FAIL", _b("MAIN_FAST_FAIL", False))


def _now_ts() -> float:
    return time.time()


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


# Safe config import (no network)
try:
    import config as C  # type: ignore
except Exception:  # pragma: no cover

    class _C:
        SCAN_INTERVAL = _i("SCAN_INTERVAL", 15)
        SCANNER_MIN_LIQ_USD = _f("SCANNER_MIN_LIQ_USD", 1000.0)
        ENABLE_RAYDIUM_SCANNER = True
        ENABLE_DEXSCREENER_SCANNER = True
        ENABLE_PUMPFUN_SCANNER = True
        ENABLE_ARBITRAGE = False

    C = _C()  # type: ignore


def _cfg(name: str, default: Any) -> Any:
    """
    Env wins. Then config attr. Then default.
    """
    if os.getenv(name) not in (None, ""):
        if isinstance(default, bool):
            return _b(name, bool(default))
        if isinstance(default, int) and not isinstance(default, bool):
            return _i(name, int(default))
        if isinstance(default, float):
            return _f(name, float(default))
        return _s(name, str(default))
    return getattr(C, name, default)


# =============================================================================
# Mint normalization (single source of truth if available)
# =============================================================================
try:
    from utils_pkg.mints import normalize_mint as _normalize_mint  # type: ignore
except Exception:  # pragma: no cover
    _BASE58_RE = re.compile(r"[1-9A-HJ-NP-Za-km-z]{32,44}")

    def _normalize_mint(m: Any) -> str:
        if not isinstance(m, str):
            return ""
        m = m.strip().replace(" ", "")
        hit = _BASE58_RE.search(m)
        return hit.group(0) if hit else ""


def _nzf(v: Any, d: float = 0.0) -> float:
    try:
        x = float(v)
        return d if x != x else x  # NaN guard
    except Exception:
        return d


def _nzi(v: Any, d: int = 0) -> int:
    try:
        return int(float(v))
    except Exception:
        return d


def _coalesce(src: Dict[str, Any], *keys: str, default: Any = None) -> Any:
    for k in keys:
        if k in src:
            v = src.get(k)
            if v is not None and v != "":
                return v
    return default


async def _maybe_await(x: Any) -> Any:
    return await x if inspect.isawaitable(x) else x


# =============================================================================
# Settings
# =============================================================================
WRAPPER_NAME = "sniper.sniper"
WRAPPER_VERSION = "v3+++_lockin_executor_v1"

SOL_MINT = "So11111111111111111111111111111111111111112"
WALLET_PUBKEY = os.getenv("WALLET_PUBKEY") or os.getenv("WALLET_PUBLIC_KEY") or ""

PAUSE_TRADING = _b("PAUSE_TRADING", False)
DRY_RUN = _b("DRY_RUN", False)

SCANNER_PERIOD_SEC = int(_cfg("SCANNER_PERIOD_SEC", int(_cfg("SCAN_INTERVAL", 15) or 15)) or 15)
SCAN_JITTER_SEC = _f("SCAN_JITTER_SEC", 0.35)

MAX_CONCURRENT_BUYS = max(1, _i("MAX_CONCURRENT_BUYS", 2))
BUY_COOLDOWN_SEC = _i("BUY_COOLDOWN_SEC", 120)

# Dedupe "seen" TTL: prevents hammering same mint every loop
SEEN_TTL_SEC = max(15, _i("SNIPER_SEEN_TTL_SEC", 90))

# Liquidity/age runner baseline
MIN_POOL_AGE_MIN = max(0, _i("MIN_POOL_AGE_MIN", 0))
MIN_LIQ_USD = float(_cfg("MIN_LIQ_USD", float(_cfg("SCANNER_MIN_LIQ_USD", 1000.0) or 1000.0)) or 1000.0)

# Stop trading when wallet SOL too low
MIN_SOL_BALANCE = float(_cfg("MIN_SOL_BALANCE", 0.2) or 0.2)

# Slippage
JUPITER_SLIPPAGE = _f("JUPITER_MAX_SLIPPAGE", 0.005)  # fraction (0.005=0.5%)
SLIPPAGE_BPS = max(1, int(JUPITER_SLIPPAGE * 10_000))

# Optional prewarming
PREWARM_TOP_N = max(0, _i("PREWARM_TOP_N", 8))

# Heartbeats
HEARTBEATS_ENABLED = _b("TELEGRAM_HEARTBEATS", True)
HEARTBEAT_EVERY_SEC = max(30, _i("HEARTBEAT_EVERY_SEC", 300))
HEARTBEAT_ONLY_ON_CHANGE = _b("HEARTBEAT_ONLY_ON_CHANGE", True)

# Route blocks cooldown
NOROUTE_COOLDOWN_SEC = max(0, _i("NOROUTE_COOLDOWN_SEC", 600))

# Runner detection knobs
RUNNER_MIN_BUYS_5M = max(0, _i("RUNNER_MIN_BUYS_5M", 3))
RUNNER_MIN_VOL5_USD = _f("RUNNER_MIN_VOL5_USD", 200.0)
RUNNER_MIN_VOL24_USD = _f("RUNNER_MIN_VOL24_USD", 2000.0)
RUNNER_MIN_BUY_SKEW = _f("RUNNER_MIN_BUY_SKEW", 0.8)

# Lists
BLOCKLIST = {x.strip() for x in _s("BLOCKLIST_MINTS", "").split(",") if x.strip()}
ALLOWLIST = {x.strip() for x in _s("ALLOWLIST_MINTS", "").split(",") if x.strip()}

# Global halt keys
HALT_KEYS = ("halt:manual", "halt:infra", "infra:halt", "halt:daily_loss")

# Which scanners to use
ENABLE_DEXSCREENER_SCANNER = bool(_cfg("ENABLE_DEXSCREENER_SCANNER", True))
ENABLE_PUMPFUN_SCANNER = bool(_cfg("ENABLE_PUMPFUN_SCANNER", True))
ENABLE_RAYDIUM_SCANNER = bool(_cfg("ENABLE_RAYDIUM_SCANNER", True))

# Moonshot budget (optional)
MOONSHOT_ENABLED = _b("MOONSHOT_ENABLED", True)
MOONSHOT_DAILY_USD = _f("MOONSHOT_DAILY_USD", 100.0)
MOONSHOT_MAX_TRADE_USD = _f("MOONSHOT_MAX_TRADE_USD", 25.0)
MOONSHOT_AGE_CUTOFF_MIN = _i("MOONSHOT_AGE_CUTOFF_MIN", 30)
MOONSHOT_LIQ_CUTOFF_USD = _f("MOONSHOT_LIQ_CUTOFF_USD", 20_000.0)
MOONSHOT_IMPACT_CUTOFF_BPS = _f("MOONSHOT_IMPACT_CUTOFF_BPS", 150.0)

# AI filters
HONEYPOT_ENABLED = _b("HONEYPOT_ENABLED", True)
AI_FRAUD_ENABLED = _b("AI_FRAUD_ENABLED", True)
AI_FRAUD_MAX = _f("AI_FRAUD_MAX", 0.85)

# Route watch
ROUTEWATCH_ENABLED = _b("ROUTEWATCH_ENABLED", True)
ROUTEWATCH_DROP_RATIO = _f("ROUTEWATCH_DROP_RATIO", 0.5)

# Reject logging behavior (still logs every reject; controls detail/noise)
REJECT_INCLUDE_CAND_FIELDS = _b("SNIPER_REJECT_INCLUDE_FIELDS", False)

# Preset
PRESET = (_s("STRATEGY_PRESET", "max-pnl") or "max-pnl").lower().strip()
if PRESET not in ("max-pnl", "balanced", "conservative"):
    PRESET = "max-pnl"


# =============================================================================
# Lazy wiring: core_utils + telegram + Jupiter swap + resources
# =============================================================================
_session_ref: Any = None
_redis_ref: Any = None


def get_session() -> Any:
    return _session_ref


def get_redis() -> Any:
    return _redis_ref


async def init_resources() -> None:
    global _session_ref, _redis_ref
    try:
        m = importlib.import_module("utils_pkg.core_utils")
        fn = getattr(m, "init_resources", None)
        if callable(fn):
            out = fn()
            if inspect.isawaitable(out):
                await out
        # refresh refs if accessors exist
        gs = getattr(m, "get_session", None)
        gr = getattr(m, "get_redis", None)
        if callable(gs):
            _session_ref = gs()
        if callable(gr):
            _redis_ref = gr()
        getattr(log, "every")("CORE_INIT_OK", 30, has_redis=bool(_redis_ref), has_session=bool(_session_ref))  # type: ignore[misc]
    except Exception as e:
        getattr(log, "every")("CORE_INIT_FAIL", 15, err=_short(e))  # type: ignore[misc]
        if _fast_fail():
            raise


async def close_resources() -> None:
    global _session_ref, _redis_ref
    try:
        m = importlib.import_module("utils_pkg.core_utils")
        fn = getattr(m, "close_resources", None)
        if callable(fn):
            out = fn()
            if inspect.isawaitable(out):
                await out
    except Exception:
        pass
    _session_ref = None
    _redis_ref = None


async def send_telegram(msg: str) -> None:
    try:
        m = importlib.import_module("utils_pkg.core_utils")
        fn = getattr(m, "send_telegram", None)
        if callable(fn):
            out = fn(msg)
            if inspect.isawaitable(out):
                await out
            return
    except Exception:
        return


async def get_sol_balance(pubkey: str) -> float:
    try:
        m = importlib.import_module("utils_pkg.core_utils")
        fn = getattr(m, "get_sol_balance", None)
        if callable(fn):
            out = fn(pubkey)
            out = await _maybe_await(out)
            return float(out or 0.0)
    except Exception:
        pass
    return 0.0


async def convert_sol_to_usd(sol: float) -> float:
    try:
        m = importlib.import_module("utils_pkg.core_utils")
        fn = getattr(m, "convert_sol_to_usd", None)
        if callable(fn):
            out = fn(sol)
            out = await _maybe_await(out)
            return float(out or 0.0)
    except Exception:
        pass
    return 0.0


async def save_entry_price(redis: Any, mint: str, entry_sol_per_token: float) -> None:
    try:
        m = importlib.import_module("utils_pkg.core_utils")
        fn = getattr(m, "save_entry_price", None)
        if callable(fn):
            out = fn(redis, mint, entry_sol_per_token)
            if inspect.isawaitable(out):
                await out
    except Exception:
        return


async def swap_token_jupiter(**kwargs: Any) -> Tuple[bool, str, Dict[str, Any]]:
    """
    Expected outputs:
      - (ok: bool, sig: str) OR dict with ok/sig/signature
    We normalize to (ok, sig, meta).
    """
    try:
        m = importlib.import_module("utils_pkg.core_utils")
        fn = getattr(m, "swap_token_jupiter", None)
        if not callable(fn):
            return False, "", {"err": "no_swap_fn"}

        out = fn(**kwargs)
        out = await _maybe_await(out)

        if isinstance(out, tuple) and len(out) >= 2:
            ok = bool(out[0])
            sig = str(out[1] or "")
            meta = out[2] if (len(out) >= 3 and isinstance(out[2], dict)) else {}
            return ok, sig, dict(meta)

        if isinstance(out, dict):
            ok = bool(out.get("ok", False))
            sig = str(out.get("sig") or out.get("signature") or "")
            meta = dict(out)
            return ok, sig, meta

        return False, "", {"err": "swap_bad_return", "raw": _short(out)}
    except Exception as e:
        return False, "", {"err": "swap_exception", "exc": _short(e)}


async def confirm_signature(sig: str, timeout_s: float = 20.0) -> bool:
    """
    Best-effort confirmation. If your core_utils exposes a confirmer, use it.
    Otherwise treat 'sig exists' as ok (but logs unverified).
    """
    try:
        m = importlib.import_module("utils_pkg.core_utils")
        for nm in ("confirm_signature", "wait_for_confirmation", "confirm_tx", "wait_tx_confirmed"):
            fn = getattr(m, nm, None)
            if callable(fn):
                out = fn(sig, timeout_s) if len(inspect.signature(fn).parameters) >= 2 else fn(sig)
                out = await _maybe_await(out)
                return bool(out)
    except Exception:
        pass
    # fallback: give it a tiny breath; do not block forever
    await asyncio.sleep(0.1)
    return bool(sig)


# =============================================================================
# Optional wiring: DB schema + structured logger
# =============================================================================
async def ensure_schema() -> None:
    try:
        m = importlib.import_module("utils_pkg.ai_db")
        fn = getattr(m, "ensure_schema", None)
        if callable(fn):
            out = fn()
            if inspect.isawaitable(out):
                await out
            return
    except Exception:
        pass
    # fallback
    with contextlib.suppress(Exception):
        m2 = importlib.import_module("db.schema")
        fn2 = getattr(m2, "ensure_schema", None)
        if callable(fn2):
            out2 = fn2()
            if inspect.isawaitable(out2):
                await out2


async def log_event(name: str, payload: Dict[str, Any]) -> None:
    """
    Best-effort: utils_pkg.data_logger.log_event(name, payload) OR ignore.
    """
    try:
        m = importlib.import_module("utils_pkg.data_logger")
        fn = getattr(m, "log_event", None)
        if callable(fn):
            out = fn(name, payload)
            if inspect.isawaitable(out):
                await out
    except Exception:
        return


async def log_scan_row(payload: Dict[str, Any]) -> None:
    """
    Best-effort: db.logger.log_scan_row OR utils_pkg.data_logger.log_event("scan_row", payload)
    """
    try:
        m = importlib.import_module("db.logger")
        fn = getattr(m, "log_scan_row", None)
        if callable(fn):
            out = fn(payload)
            if inspect.isawaitable(out):
                await out
            return
    except Exception:
        pass
    await log_event("scan_row", payload)


async def log_scan_batch(payload: Dict[str, Any]) -> None:
    try:
        m = importlib.import_module("db.logger")
        fn = getattr(m, "log_scan_batch", None)
        if callable(fn):
            out = fn(payload)
            if inspect.isawaitable(out):
                await out
            return
    except Exception:
        pass
    await log_event("scan_batch", payload)


async def log_trade_event(payload: Dict[str, Any]) -> None:
    try:
        m = importlib.import_module("db.logger")
        fn = getattr(m, "log_trade_event", None)
        if callable(fn):
            out = fn(payload)
            if inspect.isawaitable(out):
                await out
            return
    except Exception:
        pass
    await log_event("trade", payload)


async def log_error_event(where: str, err: str, extra: Optional[Dict[str, Any]] = None) -> None:
    try:
        m = importlib.import_module("db.logger")
        fn = getattr(m, "log_error_event", None)
        if callable(fn):
            out = fn(where, err) if len(inspect.signature(fn).parameters) >= 2 else fn({"where": where, "err": err})
            if inspect.isawaitable(out):
                await out
            return
    except Exception:
        pass
    p = {"where": where, "err": err}
    if extra:
        p.update(extra)
    await log_event("error", p)


# =============================================================================
# Optional wiring: exits / register position
# =============================================================================
async def start_exit_supervisor(redis: Any) -> None:
    """
    Best-effort start of exit worker/supervisor if present.
    """
    for mod_name in ("execution.exits", "utils_pkg.exit_engine", "utils_pkg.auto_exit_worker.exit_engine"):
        with contextlib.suppress(Exception):
            m = importlib.import_module(mod_name)
            fn = getattr(m, "start_exit_supervisor", None) or getattr(m, "run_supervisor", None) or getattr(m, "start", None)
            if callable(fn):
                # try common signatures (redis) / () / (loop, redis)
                try:
                    sig = inspect.signature(fn)
                    n = len(sig.parameters)
                    if n == 0:
                        out = fn()
                    elif n == 1:
                        out = fn(redis)
                    else:
                        out = fn(asyncio.get_running_loop(), redis)
                    if inspect.isawaitable(out):
                        await out
                except Exception:
                    out2 = fn(redis)  # type: ignore[misc]
                    if inspect.isawaitable(out2):
                        await out2
                return


async def register_entry(**kw: Any) -> None:
    """
    Best-effort register entry for exit engine / auto-sell.
    """
    for mod_name in ("execution.exits", "utils_pkg.exit_engine", "utils_pkg.auto_exit_worker.exit_engine", "sniper.auto_sell"):
        with contextlib.suppress(Exception):
            m = importlib.import_module(mod_name)
            fn = getattr(m, "register_entry", None) or getattr(m, "on_entry", None)
            if callable(fn):
                out = fn(**kw)
                if inspect.isawaitable(out):
                    await out
                return


# =============================================================================
# Optional wiring: AI gate / RouteGuard / AI scoring / honeypot / fraud
# =============================================================================
async def _ai_gate(mint: str) -> Tuple[bool, Dict[str, Any]]:
    """
    Returns (ok, meta). Meta MAY include: entry_solin (SOL per token), reasons, etc.
    """
    for mod_name in ("utils_pkg.ai_gate", "ai_gate"):
        with contextlib.suppress(Exception):
            m = importlib.import_module(mod_name)
            fn = getattr(m, "evaluate_candidate", None)
            if callable(fn):
                out = fn(mint)
                out = await _maybe_await(out)
                if isinstance(out, tuple) and len(out) >= 2:
                    return bool(out[0]), (out[1] if isinstance(out[1], dict) else {"raw": out[1]})
                if isinstance(out, dict):
                    return bool(out.get("ok", True)), out
                return True, {"raw": out}
    return True, {"reasons": ["no_ai_gate"]}


async def _route_guard(mint: str, sol_to_spend: float) -> Tuple[bool, str, Dict[str, Any]]:
    """
    Returns (ok, reason, meta). If no routeguard, uses router quote precheck.
    """
    for mod_name in ("utils_pkg.routeguard", "routeguard"):
        with contextlib.suppress(Exception):
            m = importlib.import_module(mod_name)
            fn = getattr(m, "check_routes", None)
            if callable(fn):
                out = fn(mint)
                out = await _maybe_await(out)
                if not isinstance(out, dict):
                    return True, "allow", {}
                action = str(out.get("action") or "allow").lower()
                if action == "allow":
                    return True, "allow", out
                return False, str(out.get("reason") or "blocked"), out

    ok, why, meta = await precheck_buy_route(mint=mint, sol_to_spend=sol_to_spend, slippage_bps=SLIPPAGE_BPS)
    return ok, why or "fallback_quote", meta


async def ai_fraud_score(mint: str) -> float:
    for mod_name in ("utils_pkg.ai_sell_logic", "utils_pkg.ai_risk_model", "ai_risk_model"):
        with contextlib.suppress(Exception):
            m = importlib.import_module(mod_name)
            fn = getattr(m, "ai_fraud_score", None) or getattr(m, "risk_score", None)
            if callable(fn):
                out = fn(mint)
                out = await _maybe_await(out)
                return float(out or 0.0)
    return 0.0


async def honeypot_check(mint: str) -> Tuple[bool, Dict[str, Any]]:
    for mod_name in ("utils_pkg.honeypot", "honeypot"):
        with contextlib.suppress(Exception):
            m = importlib.import_module(mod_name)
            fn = getattr(m, "honeypot_check", None) or getattr(m, "check", None)
            if callable(fn):
                out = fn(mint)
                out = await _maybe_await(out)
                if isinstance(out, tuple) and len(out) >= 2:
                    return bool(out[0]), (out[1] if isinstance(out[1], dict) else {"raw": out[1]})
                if isinstance(out, dict):
                    return bool(out.get("ok", True)), out
                return bool(out), {}
    return True, {}


def _softgate_ai_buy_scoring(candidate: Dict[str, Any]) -> Tuple[bool, float, str]:
    """
    Optional soft-gate using utils_pkg.ai_buy_scoring if present.
    Returns (ok, score, reason).
    """
    try:
        m = importlib.import_module("utils_pkg.ai_buy_scoring")
        Cand = getattr(m, "Candidate", None)
        pass_gate = getattr(m, "pass_gate", None)
        if Cand is None or not callable(pass_gate):
            return True, 0.0, "no_ai_buy_scoring"

        mint = str(candidate.get("mint") or "")
        sym = str(candidate.get("sym") or candidate.get("symbol") or mint[:6])
        pool_age_min = candidate.get("pool_age_min")
        liq_usd = float(candidate.get("liq_usd") or 0.0)
        impact_1k_bps = float(candidate.get("impact_1k_bps") or 0.0)
        vol5m_pct = float(candidate.get("vol5m_pct") or 0.0)
        top10_pct = float(candidate.get("top10_pct") or 0.0)
        whale_hits = float(candidate.get("whale_touches_1h") or 0.0)

        ok, score, why = pass_gate(
            Cand(
                mint=mint,
                symbol=sym,
                pool_age_min=pool_age_min,
                liq_usd=liq_usd,
                impact_1k_bps=impact_1k_bps,
                vol5m_pct=vol5m_pct,
                top10_pct=top10_pct,
                honeypot_flag=False,
                whale_touches_1h=whale_hits,
            )
        )
        return bool(ok), float(score or 0.0), str(why or "")
    except Exception:
        return True, 0.0, "ai_buy_scoring_err"


# =============================================================================
# Optional wiring: risk_manager (budget/capacity)
# =============================================================================
async def _rm_current_limits(redis: Any) -> Dict[str, Any]:
    pct = float(_cfg("TICKET_PCT_EQUITY", 0.04) or 0.04)
    cap = float(_cfg("TICKET_USD_CAP", 300.0) or 300.0)
    return {"ticket_usd_pct": pct, "ticket_cap_usd": cap, "ticket_usd": 0.0}


async def _rm_should_allow_new_entry(redis: Any) -> bool:
    return True


async def _rm_note_spend(redis: Any, usd: float) -> None:
    _ = (usd,)
    return None


async def _rm_capacity_snapshot(redis: Any) -> Dict[str, Any]:
    return {"risk_capacity": 1.0, "risk_shutdown": False, "risk_mode": "normal"}


for _mod in ("utils_pkg.risk_manager", "risk_manager"):
    with contextlib.suppress(Exception):
        _m = importlib.import_module(_mod)
        _rm_current_limits = getattr(_m, "current_limits", _rm_current_limits)
        _rm_should_allow_new_entry = getattr(_m, "should_allow_new_entry", _rm_should_allow_new_entry)
        _rm_note_spend = getattr(_m, "note_spend", _rm_note_spend)
        _rm_capacity_snapshot = getattr(_m, "risk_capacity_snapshot", _rm_capacity_snapshot)
        break


# =============================================================================
# Redis helpers (async compatible)
# =============================================================================
async def _redis_get(redis: Any, key: str) -> Optional[str]:
    if redis is None:
        return None
    try:
        v = await _maybe_await(redis.get(key))
        if v is None:
            return None
        if isinstance(v, (bytes, bytearray)):
            return v.decode("utf-8", "ignore")
        return str(v)
    except Exception:
        return None


async def _redis_set(redis: Any, key: str, value: str, *, ex: Optional[int] = None, nx: bool = False) -> bool:
    if redis is None:
        return False
    try:
        out = await _maybe_await(redis.set(key, value, ex=ex, nx=nx))
        return bool(out) or out is None
    except TypeError:
        try:
            out2 = await _maybe_await(redis.set(key, value))
            if ex and hasattr(redis, "expire"):
                await _maybe_await(redis.expire(key, int(ex)))
            return bool(out2) or out2 is None
        except Exception:
            return False
    except Exception:
        return False


async def _redis_del(redis: Any, key: str) -> None:
    if redis is None:
        return
    with contextlib.suppress(Exception):
        await _maybe_await(redis.delete(key))


# =============================================================================
# Halt/cooldown + mint locks (concurrency safety)
# =============================================================================
async def _is_halted() -> bool:
    if PAUSE_TRADING:
        return True
    r = get_redis()
    if not r:
        return False
    try:
        vals = await asyncio.gather(*[r.get(k) for k in HALT_KEYS])  # type: ignore[misc]
        halted = any(v and str(v) not in ("0", "", "None") for v in vals)
        if halted:
            getattr(log, "every")(
                "HALT_ACTIVE",
                10,
                flags={k: (v.decode() if isinstance(v, (bytes, bytearray)) else v) for k, v in zip(HALT_KEYS, vals)},
            )  # type: ignore[misc]
        return halted
    except Exception as e:
        getattr(log, "every")("HALT_CHECK_ERR", 10, err=_short(e))  # type: ignore[misc]
        return False


async def _cooldown_key(key: str, ttl: int) -> bool:
    r = get_redis()
    if not r or ttl <= 0:
        return False
    try:
        if await _redis_get(r, key):
            return True
        await _redis_set(r, key, "1", ex=int(ttl), nx=False)
        return False
    except Exception:
        return False


@dataclass
class _MintLocks:
    """
    Per-process mint locks (prevent double-entry inside one python process).
    """
    locks: Dict[str, asyncio.Lock] = field(default_factory=dict)

    def lock_for(self, mint: str) -> asyncio.Lock:
        lk = self.locks.get(mint)
        if lk is None:
            lk = asyncio.Lock()
            self.locks[mint] = lk
        return lk


_MINT_LOCKS = _MintLocks()


class MintEntryLock:
    """
    Atomic lock:
      - asyncio per-process lock
      - optional redis NX lock for cross-process safety

    If redis is missing, still safe within one process.
    """
    def __init__(self, mint: str, ttl: int = 120) -> None:
        self.mint = mint
        self.ttl = int(max(15, ttl))
        self._local_lock = _MINT_LOCKS.lock_for(mint)
        self._redis = None
        self._redis_key = f"lock:buy:{mint}"
        self._token = f"{os.getpid()}:{random.randint(1, 10**9)}"

    async def __aenter__(self) -> bool:
        await self._local_lock.acquire()
        self._redis = get_redis()
        if not self._redis:
            return True
        ok = await _redis_set(self._redis, self._redis_key, self._token, ex=self.ttl, nx=True)
        if not ok:
            self._local_lock.release()
            return False
        return True

    async def __aexit__(self, exc_type, exc, tb) -> None:
        try:
            if self._redis:
                # safe-ish release: only delete if token matches
                cur = await _redis_get(self._redis, self._redis_key)
                if cur == self._token:
                    await _redis_del(self._redis, self._redis_key)
        finally:
            with contextlib.suppress(Exception):
                if self._local_lock.locked():
                    self._local_lock.release()


# =============================================================================
# CandidateSnapshot schema + normalization
# =============================================================================
def _extract_buys_sells(raw: Dict[str, Any]) -> Tuple[int, int]:
    buys = _nzi(_coalesce(raw, "buys_5m", "buys5m", "tx_buys_5m", "buys5", "buys"), 0)
    sells = _nzi(_coalesce(raw, "sells_5m", "sells5m", "tx_sells_5m", "sells5", "sells"), 0)
    return buys, sells


def _normalize_snapshot(raw: Dict[str, Any], *, source: str) -> Optional[Dict[str, Any]]:
    """
    Normalize any scanner output into one CandidateSnapshot dict.
    Required: mint, sym, source, ts
    """
    try:
        d = dict(raw)

        mint = (
            d.get("mint")
            or d.get("base_mint")
            or d.get("baseMint")
            or (d.get("baseToken") or {}).get("address")
            or d.get("address")
        )
        mint_s = _normalize_mint(mint if isinstance(mint, str) else str(mint or ""))
        if not mint_s:
            return None

        sym = d.get("sym") or d.get("symbol") or (d.get("baseToken") or {}).get("symbol")
        sym_s = str(sym).strip() if sym is not None else ""
        if not sym_s:
            sym_s = mint_s[:6]

        ts = d.get("ts") or d.get("scan_ts") or d.get("timestamp")
        ts_f = float(ts) if isinstance(ts, (int, float)) and float(ts) > 0 else _now_ts()

        liq = _nzf(_coalesce(d, "liq_usd", "liquidity_usd", "liquidityUSD", "liq"), 0.0)
        age = _nzf(_coalesce(d, "pool_age_min", "token_age_min", "poolAgeMin", "ageMin", "age_min"), 9999.0)
        vol5 = _nzf(_coalesce(d, "vol_5m_usd", "vol5m_usd", "volumeUsd5m", "vol5"), 0.0)
        vol24 = _nzf(_coalesce(d, "vol_24h_usd", "volumeUsd24h", "vol24", "vol_24h"), 0.0)
        impact1k = _nzf(_coalesce(d, "impact_1k_bps", "priceImpact1kBps", "impact_bps"), 0.0)

        buys, sells = _extract_buys_sells(d)
        skew = _nzf(_coalesce(d, "skew5m", "buy_ratio_5m"), 0.0)
        if skew <= 0.0:
            skew = (buys + 1) / (sells + 1)

        snap: Dict[str, Any] = {
            "mint": mint_s,
            "sym": sym_s,
            "symbol": sym_s,
            "source": source,
            "scanner": source,
            "chain": d.get("chain") or "solana",
            "ts": ts_f,
            "scan_ts": ts_f,
            "liq_usd": float(liq),
            "pool_age_min": float(age),
            "vol_5m_usd": float(vol5),
            "vol_24h_usd": float(vol24),
            "buys_5m": int(buys),
            "sells_5m": int(sells),
            "buy_skew_5m": float(skew),
            "impact_1k_bps": float(impact1k),
        }

        # keep any existing scores/hints
        for k in ("score_combined", "rank_score", "momo_hint", "top10_pct", "whale_touches_1h", "vol5m_pct"):
            if k in d and d.get(k) not in (None, ""):
                snap[k] = d.get(k)

        # optionally include raw
        snap["raw"] = d
        return snap
    except Exception:
        return None


# =============================================================================
# Reject buckets (NO silent no-buy)
# =============================================================================
class RB:
    HALTED = "halted_or_paused"
    BAD_MINT = "bad_mint"
    BLOCKLIST = "blocklist"
    ALLOWLIST = "not_in_allowlist"
    COOLDOWN = "cooldown"
    NOROUTE_COOLDOWN = "noroute_cooldown"
    SEEN = "seen_dedupe"
    MIN_LIQ = "min_liquidity"
    MIN_AGE = "min_age"
    NOT_RUNNER = "not_runner"
    HONEYPOT = "honeypot"
    AI_FRAUD = "ai_fraud"
    RISK = "risk_manager_block"
    AI_GATE = "ai_gate_reject"
    ROUTE_GUARD = "route_guard_block"
    NO_ROUTE = "no_route_quote"
    SWAP_FAIL = "swap_failed"
    UNVERIFIED = "unverified_fill"
    BALANCE = "min_sol_balance"
    LOCKED = "mint_lock_busy"
    DRYRUN = "dry_run"
    INTERNAL = "internal_error"


@dataclass
class RejectStats:
    by_bucket: Dict[str, int] = field(default_factory=dict)

    def bump(self, bucket: str) -> None:
        self.by_bucket[bucket] = int(self.by_bucket.get(bucket, 0)) + 1


_REJECTS = RejectStats()


async def _reject(bucket: str, mint: str, sym: str, reason: str, cand: Optional[Dict[str, Any]] = None, extra: Optional[Dict[str, Any]] = None) -> None:
    """
    Every rejection emits a bucket + reason (both to logger and best-effort db logger).
    """
    _REJECTS.bump(bucket)
    payload: Dict[str, Any] = {
        "type": "reject",
        "bucket": bucket,
        "reason": reason,
        "mint": mint,
        "symbol": sym,
        "ts": _now_utc().isoformat(),
    }
    if extra:
        payload.update(extra)
    if REJECT_INCLUDE_CAND_FIELDS and cand:
        payload["cand"] = {k: cand.get(k) for k in ("source", "liq_usd", "pool_age_min", "vol_5m_usd", "buys_5m", "sells_5m", "impact_1k_bps")}

    # emit log line (no silence)
    log.event("REJECT", **payload)

    # best-effort persistent log
    with contextlib.suppress(Exception):
        await log_scan_row(payload)


# =============================================================================
# Runner detector (only buy moving coins)
# =============================================================================
def _is_runner(c: Dict[str, Any]) -> bool:
    buys = int(c.get("buys_5m") or 0)
    sells = int(c.get("sells_5m") or 0)
    liq = float(c.get("liq_usd") or 0.0)
    vol5 = float(c.get("vol_5m_usd") or 0.0)
    vol24 = float(c.get("vol_24h_usd") or 0.0)
    skew = float(c.get("buy_skew_5m") or 0.0)
    if skew <= 0:
        skew = (buys + 1) / (sells + 1)

    if buys < RUNNER_MIN_BUYS_5M:
        return False
    if vol5 < RUNNER_MIN_VOL5_USD and vol24 < RUNNER_MIN_VOL24_USD:
        return False
    if liq < MIN_LIQ_USD:
        return False
    if skew <= RUNNER_MIN_BUY_SKEW:
        return False
    return True


# =============================================================================
# Ticket sizing + moonshot budget
# =============================================================================
def _base_ticket_size_usd(equity_usd: float) -> float:
    pct = float(_cfg("TICKET_PCT_EQUITY", 0.04) or 0.04)
    cap = float(_cfg("TICKET_USD_CAP", 300.0) or 300.0)
    base = float(equity_usd) * pct
    return float(min(cap, max(10.0, base)))


def _today_tag() -> str:
    t = _now_utc()
    return f"{t.year:04d}{t.month:02d}{t.day:02d}"


def _is_moonshot_like(c: Dict[str, Any]) -> bool:
    try:
        if float(c.get("pool_age_min") or 9999.0) < float(MOONSHOT_AGE_CUTOFF_MIN):
            return True
    except Exception:
        pass
    try:
        if float(c.get("liq_usd") or 0.0) < float(MOONSHOT_LIQ_CUTOFF_USD):
            return True
    except Exception:
        pass
    try:
        if float(c.get("impact_1k_bps") or 0.0) > float(MOONSHOT_IMPACT_CUTOFF_BPS):
            return True
    except Exception:
        pass
    return False


async def _moonshot_spent_today() -> float:
    r = get_redis()
    if not r:
        return 0.0
    v = await _redis_get(r, f"moonshot:spent:{_today_tag()}")
    return _nzf(v, 0.0)


async def _moonshot_add_spent(usd: float) -> None:
    r = get_redis()
    if not r:
        return
    k = f"moonshot:spent:{_today_tag()}"
    try:
        if hasattr(r, "incrbyfloat"):
            await _maybe_await(r.incrbyfloat(k, float(max(0.0, usd))))
        else:
            cur = _nzf(await _redis_get(r, k), 0.0)
            await _redis_set(r, k, str(cur + float(max(0.0, usd))), ex=26 * 3600, nx=False)
        with contextlib.suppress(Exception):
            await _maybe_await(r.expire(k, 26 * 3600))
    except Exception:
        pass


async def _apply_moonshot_budget(c: Dict[str, Any], ticket_usd: float) -> Tuple[bool, float, str]:
    if not MOONSHOT_ENABLED or not _is_moonshot_like(c):
        return True, float(ticket_usd), ""
    note = "moonshot"
    t = float(ticket_usd)
    if t > float(MOONSHOT_MAX_TRADE_USD):
        t = float(MOONSHOT_MAX_TRADE_USD)
        note += f" | trade capped ${MOONSHOT_MAX_TRADE_USD:.2f}"
    spent = await _moonshot_spent_today()
    if spent + t > float(MOONSHOT_DAILY_USD):
        return False, t, note + f" | daily cap ${MOONSHOT_DAILY_USD:.2f}"
    await _moonshot_add_spent(t)
    return True, t, note


# =============================================================================
# Router quote helpers (best-effort route sanity + watchdog)
# =============================================================================
async def _router_quote_best_effort(
    mint: str,
    amount_raw: int,
    *,
    side: str,
    input_is_mint: bool,
    slippage_bps: int,
) -> Dict[str, Any]:
    """
    Calls utils_pkg.router.get_sell_quote / get_buy_quote with signature-flex.
    Returns {} on failure.
    """
    try:
        m = importlib.import_module("utils_pkg.router")
        fn = getattr(m, "get_sell_quote", None) or getattr(m, "get_buy_quote", None)
        if not callable(fn):
            return {}

        kw: Dict[str, Any] = {
            "mint": mint,
            "amount_raw": int(max(1, amount_raw)),
            "side": side,
            "input_is_mint": bool(input_is_mint),
            "slippage_bps": int(slippage_bps),
        }
        # prune unknown kwargs
        with contextlib.suppress(Exception):
            sig = inspect.signature(fn)
            allowed = set(sig.parameters.keys())
            kw = {k: v for k, v in kw.items() if k in allowed}

        out = fn(**kw)
        out = await _maybe_await(out)
        return out if isinstance(out, dict) else {}
    except Exception:
        return {}


async def precheck_buy_route(mint: str, sol_to_spend: float, slippage_bps: int) -> Tuple[bool, str, Dict[str, Any]]:
    raw_amount = int(max(1, float(sol_to_spend) * 1e9))
    q = await _router_quote_best_effort(
        mint=mint,
        amount_raw=raw_amount,
        side="sell",
        input_is_mint=True,
        slippage_bps=slippage_bps,
    )
    if not q or q.get("error"):
        return False, "no_route", {"hops": 0, "impact_bps": 9999, "out_amount": 0}

    routes = q.get("routes") or q.get("routePlan") or []
    best = routes[0] if isinstance(routes, list) and routes else {}
    impact = _nzf(_coalesce(q, "priceImpactPct", default=_coalesce(best, "priceImpactPct")), 0.0)
    out_amount = _nzf(_coalesce(q, "outAmount", "out_amount"), 0.0)
    hops = _nzi(_coalesce(best, "hops", "swapCount"), 0)

    meta = {"hops": hops, "impact_bps": int(abs(float(impact)) * 100.0), "out_amount": out_amount}
    return True, "", meta


async def posttrade_watchdog(mint: str, sol_to_spend: float, slippage_bps: int, pre_meta: Dict[str, Any]) -> Tuple[bool, str, Dict[str, Any]]:
    ok, why, now_meta = await precheck_buy_route(mint, sol_to_spend, slippage_bps)
    if not ok:
        return False, "route_degraded", now_meta
    if not ROUTEWATCH_ENABLED:
        return True, "ok", now_meta
    try:
        pre_out = _nzf(pre_meta.get("out_amount"), 0.0)
        now_out = _nzf(now_meta.get("out_amount"), 0.0)
        if pre_out > 0 and (now_out / pre_out) < float(ROUTEWATCH_DROP_RATIO):
            return False, "liquidity_drop", now_meta
    except Exception:
        pass
    return True, "ok", now_meta


# =============================================================================
# Priority fees (best-effort)
# =============================================================================
def compute_priority_tip(sol_to_spend: float) -> Tuple[int, Optional[int]]:
    # conservative defaults if "fees" module not present
    if sol_to_spend >= 1.0:
        return (10_000, 50_000)
    if sol_to_spend >= 0.25:
        return (6_000, 25_000)
    return (3_000, 10_000)


with contextlib.suppress(Exception):
    m = importlib.import_module("fees")
    fn = getattr(m, "compute_priority_tip", None)
    if callable(fn):
        compute_priority_tip = fn  # type: ignore[assignment]


# =============================================================================
# Scanners (use wrappers if present)
# =============================================================================
async def _collect_scan(result: Any) -> List[Dict[str, Any]]:
    if result is None:
        return []
    if hasattr(result, "__aiter__"):
        out: List[Dict[str, Any]] = []
        async for item in result:
            if isinstance(item, dict):
                out.append(item)
        return out
    if inspect.isawaitable(result):
        try:
            return await _collect_scan(await result)
        except Exception:
            return []
    if isinstance(result, dict):
        return [result]
    if isinstance(result, list):
        return [x for x in result if isinstance(x, dict)]
    if not isinstance(result, (str, bytes, bytearray)) and hasattr(result, "__iter__"):
        try:
            out2: List[Dict[str, Any]] = []
            for item in result:
                if isinstance(item, dict):
                    out2.append(item)
            return out2
        except Exception:
            return []
    return []


async def _call_scanner(module_name: str, func_name: str, source: str) -> List[Dict[str, Any]]:
    try:
        m = importlib.import_module(module_name)
        fn = getattr(m, func_name, None)
        if not callable(fn):
            return []
        # signature-flex: try (redis, session), (redis), (), (session)
        for arity in (2, 1, 0):
            try:
                if arity == 2:
                    raw_items = await _collect_scan(fn(get_redis(), get_session()))
                elif arity == 1:
                    raw_items = await _collect_scan(fn(get_redis() or get_session()))
                else:
                    raw_items = await _collect_scan(fn())
                out: List[Dict[str, Any]] = []
                for r in raw_items:
                    snap = _normalize_snapshot(r, source=source)
                    if snap is not None:
                        out.append(snap)
                return out
            except TypeError:
                continue
    except asyncio.CancelledError:
        raise
    except Exception as e:
        getattr(log, "every")("SCAN_ERR", 10, source=source, err=_short(e))  # type: ignore[misc]
        if _fast_fail():
            raise
    return []


async def scan_all_sources() -> List[Dict[str, Any]]:
    tasks: List[asyncio.Task] = []

    if ENABLE_DEXSCREENER_SCANNER:
        # wrapper first
        tasks.append(asyncio.create_task(_call_scanner("scanner.dexscreener", "scan_dexscreener", "dexscreener")))
        # fallback (if wrapper missing, task returns [])
        tasks.append(asyncio.create_task(_call_scanner("scanner.dexscreener_scanner", "scan_dexscreener", "dexscreener")))

    if ENABLE_PUMPFUN_SCANNER:
        tasks.append(asyncio.create_task(_call_scanner("scanner.pumpfun", "scan_pumpfun", "pumpfun")))
        tasks.append(asyncio.create_task(_call_scanner("scanner.pumpfun_scanner", "scan_pumpfun", "pumpfun")))

    if ENABLE_RAYDIUM_SCANNER:
        tasks.append(asyncio.create_task(_call_scanner("scanner.raydium", "scan_raydium", "raydium")))
        tasks.append(asyncio.create_task(_call_scanner("scanner.raydium_scanner", "scan_raydium", "raydium")))

    if not tasks:
        return []

    batches = await asyncio.gather(*tasks, return_exceptions=False)
    # merge/dedupe by mint, keep best row
    merged: Dict[str, Dict[str, Any]] = {}

    def rank_key(c: Dict[str, Any]) -> Tuple[float, float, float, float]:
        return (
            float(c.get("score_combined") or c.get("rank_score") or 0.0),
            float(c.get("vol_5m_usd") or 0.0),
            float(c.get("liq_usd") or 0.0),
            float(c.get("buy_skew_5m") or 0.0),
        )

    for batch in batches:
        for c in batch:
            mint = str(c.get("mint") or "")
            if not mint:
                continue
            if mint not in merged:
                merged[mint] = c
                continue
            cur = merged[mint]
            if rank_key(c) > rank_key(cur):
                merged[mint] = {**cur, **c}
            else:
                # fill blanks
                for k, v in c.items():
                    if cur.get(k) in (None, "", 0, 0.0) and v not in (None, "", 0, 0.0):
                        cur[k] = v

    return list(merged.values())


# =============================================================================
# Ranking + pool selection
# =============================================================================
def _rank_score(c: Dict[str, Any]) -> float:
    s = _nzf(c.get("score_combined") or c.get("rank_score"), 0.0)
    if s > 0:
        return float(s)

    age = _nzf(c.get("pool_age_min"), 9999.0)
    liq = _nzf(c.get("liq_usd"), 0.0)
    vol5 = _nzf(c.get("vol_5m_usd"), 0.0)
    skew = _nzf(c.get("buy_skew_5m"), 0.0)
    if skew <= 0:
        buys = int(c.get("buys_5m") or 0)
        sells = int(c.get("sells_5m") or 0)
        skew = (buys + 1) / (sells + 1)

    score = 0.0
    score += max(0.0, 90.0 - age) * 0.30
    score += min(liq, 200_000.0) / 10_000.0
    score += min(vol5, 50_000.0) / 5_000.0
    score += max(0.0, min(2.5, skew)) * 5.0
    score += _nzf(c.get("momo_hint"), 0.0) * 0.1
    return float(score)


def select_top(cands: List[Dict[str, Any]], k: int) -> List[Dict[str, Any]]:
    if not cands:
        return []
    ranked = sorted(cands, key=_rank_score, reverse=True)
    return ranked[: max(1, int(k))]


# =============================================================================
# Entry executor (atomic flow)
# =============================================================================
async def _ticket_usd_for_equity(equity_usd: float, risk_ctx: Dict[str, Any]) -> float:
    ticket = _base_ticket_size_usd(equity_usd)
    lims = await _rm_current_limits(get_redis())
    if isinstance(lims, dict) and lims.get("ticket_usd"):
        ticket = min(float(ticket), float(lims["ticket_usd"]))

    cap = _clamp(_nzf(risk_ctx.get("risk_capacity"), 1.0), 0.0, 1.0)
    if cap < 1.0:
        ticket = max(10.0, ticket * cap)
    return float(ticket)


async def _entry_price_sol_per_token(mint: str, gate_meta: Dict[str, Any]) -> float:
    # prefer AI gate
    p = _nzf(gate_meta.get("entry_solin"), 0.0)
    if p > 0:
        return float(p)
    # fallback: ask router for price field if exists
    q = await _router_quote_best_effort(
        mint=mint,
        amount_raw=1_000_000,
        side="sell",
        input_is_mint=False,
        slippage_bps=SLIPPAGE_BPS,
    )
    px = _nzf(_coalesce(q, "price", "price_sol", "priceSol"), 0.0)
    return float(px)


async def _verify_fill_or_warn(sig: str, mint: str, sym: str) -> bool:
    ok = await confirm_signature(sig, timeout_s=float(_f("CONFIRM_TIMEOUT_S", 20.0)))
    if not ok:
        await _reject(RB.UNVERIFIED, mint, sym, "tx_unconfirmed_or_timeout", extra={"sig": sig})
    return bool(ok)


async def _call_swap(
    mint: str,
    sym: str,
    sol_to_spend: float,
    *,
    slippage: float,
) -> Tuple[bool, str, Dict[str, Any]]:
    cu_price, jito_tip = compute_priority_tip(sol_to_spend)
    kw: Dict[str, Any] = {
        "input_mint": SOL_MINT,
        "output_mint": mint,
        "amount_sol": float(sol_to_spend),
        "amount_float": float(sol_to_spend),
        "amount": float(sol_to_spend),
        "slippage": float(slippage),
        "slippage_bps": int(float(slippage) * 10_000),
        "compute_unit_price_micro_lamports": cu_price,
        "priority_fee_micro_lamports": cu_price,
        "jito_tip_lamports": jito_tip,
    }

    # prune to callee signature if needed
    try:
        m = importlib.import_module("utils_pkg.core_utils")
        fn = getattr(m, "swap_token_jupiter", None)
        if callable(fn):
            sig = inspect.signature(fn)
            allowed = set(sig.parameters.keys())
            kw = {k: v for k, v in kw.items() if k in allowed and v is not None}
    except Exception:
        pass

    ok, sig, meta = await swap_token_jupiter(**kw)
    meta = dict(meta)
    meta.setdefault("cu_price", cu_price)
    meta.setdefault("jito_tip", jito_tip)
    meta.setdefault("symbol", sym)
    meta.setdefault("mint", mint)
    return ok, sig, meta


# =============================================================================
# Heartbeat + equity
# =============================================================================
async def _compute_equity_usd() -> float:
    try:
        sol_bal = await get_sol_balance(WALLET_PUBKEY) if WALLET_PUBKEY else 0.0
        usd_per_sol = await convert_sol_to_usd(1.0) or 0.0
        return max(0.0, float(sol_bal) * float(usd_per_sol))
    except Exception:
        return float(_cfg("EQUITY_USD_FALLBACK", 2000.0) or 2000.0)


async def _send_heartbeat(equity_usd: float) -> None:
    if not HEARTBEATS_ENABLED:
        return
    r = get_redis()
    if not r:
        return
    try:
        now = int(_now_utc().timestamp())
        last_ts = _nzf(await _redis_get(r, "hb:last_ts"), 0.0)
        if last_ts and (now - int(last_ts)) < int(HEARTBEAT_EVERY_SEC):
            return

        last_eq = _nzf(await _redis_get(r, "hb:last_equity"), 0.0)
        if HEARTBEAT_ONLY_ON_CHANGE and last_eq > 0 and abs(float(last_eq) - float(equity_usd)) < 1.0:
            await _redis_set(r, "hb:last_ts", str(now), ex=24 * 3600, nx=False)
            return

        delta_txt = ""
        if last_eq > 0:
            delta = float(equity_usd) - float(last_eq)
            pct = (delta / float(last_eq)) * 100.0 if float(last_eq) != 0 else 0.0
            delta_txt = f" (Δ {delta:+,.0f}, {pct:+.2f}%)"

        msg = f"📈 Heartbeat | equity ${equity_usd:,.0f}{delta_txt}"
        with contextlib.suppress(Exception):
            await send_telegram(msg)

        await _redis_set(r, "hb:last_ts", str(now), ex=24 * 3600, nx=False)
        await _redis_set(r, "hb:last_equity", f"{equity_usd:.2f}", ex=24 * 3600, nx=False)
    except Exception as e:
        getattr(log, "every")("HB_ERR", 20, err=_short(e))  # type: ignore[misc]


# =============================================================================
# Main Sniper Orchestrator
# =============================================================================
@dataclass
class LoopStats:
    started_ts: float
    loops: int = 0
    scanned: int = 0
    unique: int = 0
    considered: int = 0
    buys_ok: int = 0
    buys_try: int = 0


class Sniper:
    def __init__(self) -> None:
        self.sem = asyncio.Semaphore(MAX_CONCURRENT_BUYS)
        self.stop_evt = asyncio.Event()
        self.stats = LoopStats(started_ts=_now_ts())

    async def _seen_dedupe(self, mint: str) -> bool:
        r = get_redis()
        if not r:
            return False
        k = f"seen:{mint}"
        # If already seen recently, skip
        if await _redis_get(r, k):
            return True
        await _redis_set(r, k, "1", ex=SEEN_TTL_SEC, nx=False)
        return False

    async def _prewarm_quotes(self, cands: List[Dict[str, Any]], ticket_usd: float) -> None:
        if PREWARM_TOP_N <= 0 or not cands or ticket_usd <= 0:
            return
        usd_per_sol = await convert_sol_to_usd(1.0) or 0.0
        if usd_per_sol <= 0:
            return
        amount_sol = float(ticket_usd) / float(usd_per_sol)

        top = sorted(cands, key=lambda c: float(c.get("liq_usd") or 0.0), reverse=True)[: int(PREWARM_TOP_N)]
        mints = [str(c["mint"]) for c in top if isinstance(c.get("mint"), str)]
        if not mints:
            return
        # best-effort cache warm
        with contextlib.suppress(Exception):
            m = importlib.import_module("jup_quote_cache")
            fn = getattr(m, "prewarm_quotes", None)
            if callable(fn):
                out = fn(mints, amount_sol, SLIPPAGE_BPS)
                if inspect.isawaitable(out):
                    await out

    async def _basic_gates(self, c: Dict[str, Any]) -> Tuple[bool, str]:
        mint = str(c.get("mint") or "")
        sym = str(c.get("sym") or c.get("symbol") or mint[:6])

        if await _is_halted():
            await _reject(RB.HALTED, mint, sym, "halted_or_pause_trading", cand=c)
            return False, "halted"

        if not mint:
            await _reject(RB.BAD_MINT, mint, sym, "missing_mint", cand=c)
            return False, "bad_mint"

        if BLOCKLIST and mint in BLOCKLIST:
            await _reject(RB.BLOCKLIST, mint, sym, "blocklisted", cand=c)
            return False, "blocklist"

        if ALLOWLIST and mint not in ALLOWLIST:
            await _reject(RB.ALLOWLIST, mint, sym, "not_in_allowlist", cand=c)
            return False, "allowlist"

        if await _cooldown_key(f"buycd:{mint}", BUY_COOLDOWN_SEC):
            await _reject(RB.COOLDOWN, mint, sym, "buy_cooldown_active", cand=c)
            return False, "cooldown"

        if NOROUTE_COOLDOWN_SEC > 0 and await _cooldown_key(f"noroutecd:{mint}", NOROUTE_COOLDOWN_SEC):
            await _reject(RB.NOROUTE_COOLDOWN, mint, sym, "noroute_cooldown_active", cand=c)
            return False, "noroute_cooldown"

        if await self._seen_dedupe(mint):
            await _reject(RB.SEEN, mint, sym, "recently_seen_dedupe", cand=c)
            return False, "seen"

        # min age/liquidity
        if float(c.get("pool_age_min") or 9999.0) < float(MIN_POOL_AGE_MIN):
            await _reject(RB.MIN_AGE, mint, sym, f"pool_age<{MIN_POOL_AGE_MIN}", cand=c)
            return False, "min_age"

        if float(c.get("liq_usd") or 0.0) < float(MIN_LIQ_USD):
            await _reject(RB.MIN_LIQ, mint, sym, f"liq_usd<{MIN_LIQ_USD:.0f}", cand=c)
            return False, "min_liq"

        if not _is_runner(c):
            await _reject(RB.NOT_RUNNER, mint, sym, "not_runner_filter", cand=c)
            return False, "not_runner"

        # wallet balance guard
        if WALLET_PUBKEY:
            bal = await get_sol_balance(WALLET_PUBKEY)
            if float(bal) <= float(MIN_SOL_BALANCE):
                await _reject(RB.BALANCE, mint, sym, f"sol_balance<{MIN_SOL_BALANCE}", cand=c, extra={"bal_sol": bal})
                return False, "balance"

        # risk manager
        if not await _rm_should_allow_new_entry(get_redis()):
            await _reject(RB.RISK, mint, sym, "risk_manager_block", cand=c)
            return False, "risk"

        # honeypot/fraud
        if HONEYPOT_ENABLED:
            ok_hp, hp_meta = await honeypot_check(mint)
            if not ok_hp:
                await _reject(RB.HONEYPOT, mint, sym, "honeypot_reject", cand=c, extra={"hp": hp_meta})
                return False, "honeypot"

        if AI_FRAUD_ENABLED:
            s01 = await ai_fraud_score(mint)
            if float(s01) >= float(AI_FRAUD_MAX):
                await _reject(RB.AI_FRAUD, mint, sym, f"ai_fraud>={AI_FRAUD_MAX:.2f}", cand=c, extra={"score": s01})
                return False, "ai_fraud"

        return True, "ok"

    async def _execute_one(self, c: Dict[str, Any], *, equity_usd: float, risk_ctx: Dict[str, Any]) -> bool:
        mint = str(c.get("mint") or "")
        sym = str(c.get("sym") or c.get("symbol") or mint[:6])

        ok_basic, _ = await self._basic_gates(c)
        if not ok_basic:
            return False

        # AI gate (entry estimate)
        ok_gate, gate_meta = await _ai_gate(mint)
        if not ok_gate:
            await _reject(RB.AI_GATE, mint, sym, "ai_gate_reject", cand=c, extra={"meta": gate_meta})
            return False

        entry_solin = await _entry_price_sol_per_token(mint, gate_meta)
        if entry_solin <= 0:
            await _reject(RB.AI_GATE, mint, sym, "no_entry_price", cand=c, extra={"meta": gate_meta})
            return False

        # ticket sizing + moonshot budgeting
        ticket = await _ticket_usd_for_equity(equity_usd, risk_ctx)
        ok_m, ticket2, note = await _apply_moonshot_budget(c, ticket)
        if not ok_m or ticket2 <= 0:
            await _reject(RB.RISK, mint, sym, "moonshot_budget_block", cand=c, extra={"note": note})
            return False
        ticket = float(ticket2)

        usd_per_sol = await convert_sol_to_usd(1.0)
        if usd_per_sol <= 0:
            await _reject(RB.INTERNAL, mint, sym, "usd_per_sol_unavailable", cand=c)
            return False
        sol_to_spend = float(ticket) / float(usd_per_sol)
        if sol_to_spend <= 0:
            await _reject(RB.INTERNAL, mint, sym, "sol_to_spend_zero", cand=c)
            return False

        # RouteGuard (hard)
        ok_rg, why_rg, rg_meta = await _route_guard(mint, sol_to_spend)
        if not ok_rg:
            await _reject(RB.ROUTE_GUARD, mint, sym, f"route_guard:{why_rg}", cand=c, extra={"meta": rg_meta})
            if "no_route" in str(why_rg).lower() and NOROUTE_COOLDOWN_SEC > 0:
                await _cooldown_key(f"noroutecd:{mint}", NOROUTE_COOLDOWN_SEC)
            return False

        # Optional softgate (ai_buy_scoring)
        ok_soft, score_soft, why_soft = _softgate_ai_buy_scoring(c)
        if not ok_soft:
            await _reject(RB.AI_GATE, mint, sym, f"ai_buy_scoring:{why_soft}", cand=c, extra={"score": score_soft})
            return False
        if score_soft > 0:
            c["ai_entry_score"] = score_soft
            c["ai_entry_reason"] = why_soft

        # Atomic lock (local + redis)
        async with MintEntryLock(mint, ttl=max(60, BUY_COOLDOWN_SEC)) as got_lock:
            if not got_lock:
                await _reject(RB.LOCKED, mint, sym, "mint_lock_busy", cand=c)
                return False

            # Dry run stops here
            if DRY_RUN:
                await _reject(RB.DRYRUN, mint, sym, "dry_run_enabled", cand=c, extra={"ticket_usd": ticket, "entry_solin": entry_solin})
                with contextlib.suppress(Exception):
                    await send_telegram(f"🧪 DRY-RUN — would buy {sym} ({mint}) for ${ticket:,.2f} @ {entry_solin:.10f} SOL/token")
                return False

            # Quote precheck (router)
            ok_q, why_q, pre_meta = await precheck_buy_route(mint, sol_to_spend, SLIPPAGE_BPS)
            if not ok_q:
                await _reject(RB.NO_ROUTE, mint, sym, f"route_precheck:{why_q}", cand=c, extra={"meta": pre_meta, "ticket_usd": ticket})
                if NOROUTE_COOLDOWN_SEC > 0:
                    await _cooldown_key(f"noroutecd:{mint}", NOROUTE_COOLDOWN_SEC)
                return False

            # Log attempt (before swap)
            self.stats.buys_try += 1
            with contextlib.suppress(Exception):
                await log_trade_event(
                    {
                        "type": "buy_try",
                        "mint": mint,
                        "symbol": sym,
                        "ticket_usd": float(ticket),
                        "sol_to_spend": float(sol_to_spend),
                        "entry_solin": float(entry_solin),
                        "pre_meta": dict(pre_meta),
                        "ts": _now_utc().isoformat(),
                    }
                )

            # Build + send swap (core_utils)
            ok_swap, sig, swap_meta = await _call_swap(mint, sym, sol_to_spend, slippage=JUPITER_SLIPPAGE)
            if not ok_swap or not sig:
                await _reject(RB.SWAP_FAIL, mint, sym, "swap_failed", cand=c, extra={"meta": swap_meta})
                with contextlib.suppress(Exception):
                    await log_trade_event({"type": "buy_fail", "mint": mint, "symbol": sym, "why": "swap_failed", "meta": swap_meta})
                return False

            # Verify fill (best-effort)
            ok_fill = await _verify_fill_or_warn(sig, mint, sym)
            if not ok_fill:
                # still proceed to register? safer: do NOT register unverified.
                with contextlib.suppress(Exception):
                    await log_trade_event({"type": "buy_unverified", "mint": mint, "symbol": sym, "sig": sig})
                return False

            # Persist entry price
            with contextlib.suppress(Exception):
                await save_entry_price(get_redis(), mint, float(entry_solin))

            # Register position for exits
            with contextlib.suppress(Exception):
                trailing_pct = float(_cfg("STOP_LOSS_PERCENT", 20.0) or 20.0)
                tp1 = float(_cfg("TAKE_PROFIT1_PCT", 30.0) or 30.0)
                tp2 = float(_cfg("TAKE_PROFIT2_PCT", 60.0) or 60.0)
                tp3 = float(_cfg("TAKE_PROFIT3_PCT", 120.0) or 120.0)
                await register_entry(
                    mint=mint,
                    symbol=sym,
                    entry_price=float(entry_solin),
                    amount_sol=float(sol_to_spend),
                    trailing_stop_pct=float(trailing_pct),
                    take_profits=[tp1, tp2, tp3],
                    created_at=_now_utc().isoformat(),
                    signature=sig,
                )

            # Risk manager spend note
            with contextlib.suppress(Exception):
                await _rm_note_spend(get_redis(), float(ticket))

            # Post-trade route watchdog
            with contextlib.suppress(Exception):
                ok_wd, msg_wd, now_meta = await posttrade_watchdog(mint, sol_to_spend, SLIPPAGE_BPS, pre_meta)
                if not ok_wd:
                    await send_telegram(
                        f"⚠️ Route watchdog: {sym} ({mint}) {msg_wd} • "
                        f"pre_out={_nzf(pre_meta.get('out_amount'),0):.0f} now_out={_nzf(now_meta.get('out_amount'),0):.0f}"
                    )

            # Log success
            self.stats.buys_ok += 1
            with contextlib.suppress(Exception):
                await log_trade_event(
                    {
                        "type": "buy",
                        "mint": mint,
                        "symbol": sym,
                        "ticket_usd": float(ticket),
                        "sol_spent": float(sol_to_spend),
                        "entry_solin": float(entry_solin),
                        "signature": sig,
                        "swap_meta": swap_meta,
                        "ts": _now_utc().isoformat(),
                    }
                )

            # Telegram success
            with contextlib.suppress(Exception):
                usd_est = await convert_sol_to_usd(sol_to_spend) or 0.0
                await send_telegram(
                    f"✅ SNIPED {sym}\n"
                    f"Spent: {sol_to_spend:.4f} SOL (~${usd_est:.2f})\n"
                    f"Entry (SOL/token): {float(entry_solin):.10f}\n"
                    f"Mint: {mint}\nTx: {sig}"
                )

            return True

    async def main_loop(self) -> None:
        with contextlib.suppress(Exception):
            await send_telegram(f"🎯 Sniper started (preset={PRESET}, v={WRAPPER_VERSION}).")

        # ensure exit supervisor (best-effort)
        with contextlib.suppress(Exception):
            asyncio.create_task(start_exit_supervisor(get_redis()))

        while not self.stop_evt.is_set():
            try:
                self.stats.loops += 1

                if await _is_halted():
                    # still heartbeat so you know it's alive
                    eq = await _compute_equity_usd()
                    await _send_heartbeat(eq)
                    await asyncio.sleep(float(SCANNER_PERIOD_SEC))
                    continue

                # risk snapshot
                risk_ctx = await _rm_capacity_snapshot(get_redis())
                if bool(risk_ctx.get("risk_shutdown", False)) or _clamp(_nzf(risk_ctx.get("risk_capacity"), 1.0), 0.0, 1.0) <= 0.0:
                    getattr(log, "every")("RISK_SHUTDOWN", 10, ctx=risk_ctx)  # type: ignore[misc]
                    await asyncio.sleep(float(SCANNER_PERIOD_SEC))
                    continue

                # equity snapshot
                equity_usd = await _compute_equity_usd()
                await _send_heartbeat(equity_usd)

                # scan
                self.stats.scanned += 1
                cands = await scan_all_sources()
                self.stats.unique = len(cands)

                if not cands:
                    await log_scan_batch(
                        {
                            "type": "scan_batch",
                            "ts": _now_utc().isoformat(),
                            "unique": 0,
                            "preset": PRESET,
                            "rejects": dict(_REJECTS.by_bucket),
                        }
                    )
                    await asyncio.sleep(float(SCANNER_PERIOD_SEC))
                    continue

                # rank pool size
                if PRESET == "max-pnl":
                    pool_size = max(MAX_CONCURRENT_BUYS * 4, 12)
                elif PRESET == "balanced":
                    pool_size = max(MAX_CONCURRENT_BUYS * 3, 9)
                else:
                    pool_size = max(MAX_CONCURRENT_BUYS * 2, 6)

                top = select_top(cands, pool_size)

                # prewarm (best-effort)
                with contextlib.suppress(Exception):
                    ticket0 = await _ticket_usd_for_equity(equity_usd, risk_ctx)
                    await self._prewarm_quotes(top, float(ticket0))

                # batch log
                with contextlib.suppress(Exception):
                    await log_scan_batch(
                        {
                            "type": "scan_batch",
                            "ts": _now_utc().isoformat(),
                            "unique": len(cands),
                            "top": len(top),
                            "preset": PRESET,
                            "equity_usd": float(equity_usd),
                            "rejects": dict(_REJECTS.by_bucket),
                        }
                    )

                # consider sequentially; semaphore caps concurrent swaps
                for c in top:
                    if self.stop_evt.is_set():
                        break

                    self.stats.considered += 1
                    mint = str(c.get("mint") or "")
                    sym = str(c.get("sym") or c.get("symbol") or mint[:6])

                    async def _run_one(candidate: Dict[str, Any]) -> None:
                        async with self.sem:
                            try:
                                await self._execute_one(candidate, equity_usd=equity_usd, risk_ctx=risk_ctx)
                            except asyncio.CancelledError:
                                raise
                            except Exception as e:
                                await _reject(RB.INTERNAL, mint, sym, "execute_exception", cand=candidate, extra={"err": _short(e)})
                                await log_error_event("execute_one", _short(e), {"mint": mint, "symbol": sym})
                                if _fast_fail():
                                    raise

                    # schedule + allow overlap up to semaphore
                    asyncio.create_task(_run_one(c))

                # jittered sleep
                base = float(SCANNER_PERIOD_SEC)
                jitter = float(SCAN_JITTER_SEC)
                await asyncio.sleep(max(0.2, base + random.uniform(-jitter, jitter)))

            except asyncio.CancelledError:
                raise
            except Exception as e:
                await log_error_event("main_loop", _short(e))
                getattr(log, "every")("LOOP_ERR", 5, err=_short(e))  # type: ignore[misc]
                if _fast_fail():
                    raise
                await asyncio.sleep(max(0.5, float(SCANNER_PERIOD_SEC)))


# =============================================================================
# Entrypoints
# =============================================================================
async def run_sniper() -> None:
    sniper = Sniper()
    await sniper.main_loop()


async def main() -> None:
    await init_resources()
    with contextlib.suppress(Exception):
        await ensure_schema()

    # Start exit supervisor early (best-effort)
    with contextlib.suppress(Exception):
        asyncio.create_task(start_exit_supervisor(get_redis()))

    try:
        await run_sniper()
    finally:
        with contextlib.suppress(Exception):
            await close_resources()


def sniper_status() -> Dict[str, Any]:
    return {
        "wrapper": WRAPPER_NAME,
        "version": WRAPPER_VERSION,
        "preset": PRESET,
        "paused": int(PAUSE_TRADING),
        "dry_run": int(DRY_RUN),
        "max_concurrent_buys": MAX_CONCURRENT_BUYS,
        "scanner_period_sec": SCANNER_PERIOD_SEC,
        "min_liq_usd": MIN_LIQ_USD,
        "min_pool_age_min": MIN_POOL_AGE_MIN,
        "rejects": dict(_REJECTS.by_bucket),
        "has_redis": int(bool(get_redis())),
        "has_session": int(bool(get_session())),
    }


__all__ = ["Sniper", "run_sniper", "main", "sniper_status"]

if __name__ == "__main__":  # pragma: no cover
    asyncio.run(main())
