from __future__ import annotations

"""
sniper/sniper.py — V3+++ (God-Tier LOCK-IN)
==========================================

ROLE (P0 LOCK-IN)
-----------------
This is the *single* strategy-orchestrator for LIVE sniping.

It MUST:
- Orchestrate: scan -> normalize (canonical Candidate) -> feature build -> entry IQ -> sizing -> route safety -> swap -> position register -> exit engine handoff
- Prevent silent no-buy: every candidate rejection emits a *bucketed* reason (with sampling + periodic summaries to avoid spam)
- Be import-safe: NO network at import time, no side effects, no hidden loops
- Be cancellation-safe: never swallow asyncio.CancelledError
- Be deterministic: per-mint locks (local + optional Redis NX) to prevent double entries
- Be production-friendly: optional modules are best-effort unless FAST_FAIL enabled

LOCK-IN DECISIONS (from roadmap)
--------------------------------
- scanner/registry.py defines the canonical Candidate schema. We prefer it.
- utils_pkg/feature_builder.py is the one canonical feature dict generator.
- utils_pkg/ai_entry_logic.py is the canonical "enter?" brain (score + gate).
- utils_pkg/router.py is the canonical quote path (route safety).
- utils_pkg/exit_engine.py is the canonical exit brain (we only hand off).
- Core utilities (redis/session/telegram/swap) come from utils_pkg.core_utils.

ENV KNOBS (minimal, safe defaults)
----------------------------------
SNIPER_FAST_FAIL / MAIN_FAST_FAIL      : raise on missing modules / wiring
PAUSE_TRADING                          : halt entries (still scans + heartbeats)
DRY_RUN / DRYRUN                        : no swaps; logs what would happen

SCANNER_PERIOD_SEC / SCAN_INTERVAL     : scan cadence
MAX_CONCURRENT_BUYS                    : parallel entry attempts
SNIPER_MAX_OUTSTANDING_TASKS           : backpressure (avoid runaway tasks)
BUY_COOLDOWN_SEC                       : per-mint cooldown after attempted buy lock
SNIPER_SEEN_TTL_SEC                    : per-mint seen-dedupe (avoid hammering)
NOROUTE_COOLDOWN_SEC                   : per-mint cooldown after route failure

MIN_LIQ_USD / SCANNER_MIN_LIQ_USD      : liquidity gate
MIN_POOL_AGE_MIN                       : age gate
RUNNER_MIN_BUYS_5M / RUNNER_MIN_VOL5_USD / RUNNER_MIN_VOL24_USD / RUNNER_MIN_BUY_SKEW : runner filter

JUPITER_MAX_SLIPPAGE                   : fraction (0.005=0.5%)
CONFIRM_TIMEOUT_S                      : best-effort tx confirm

HEARTBEATS_ENABLED / HEARTBEAT_EVERY_SEC / HEARTBEAT_ONLY_ON_CHANGE : status pings

ALLOWLIST_MINTS / BLOCKLIST_MINTS      : explicit include/exclude (base58 mints)

Notes
-----
- This file does NOT implement exit logic. It registers positions and hands off to exit_engine.
- This file does NOT implement multiple competing scanners. It prefers scanner.registry when present.

B023 FIX (IMPORTANT)
--------------------
No function definitions inside a loop that capture loop-updated variables.
Tasks are created with explicit snapshots (e.g., equity_usd passed as an argument).
"""

import asyncio
import contextlib
import importlib
import inspect
import logging
import os
import random
import re
import signal
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, Union

# =============================================================================
# Logging (canonical get_logger preferred, fallback safe)
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
            with contextlib.suppress(Exception):
                log.info("%s %s", name, kw)

        log.event = _event  # type: ignore[attr-defined]

    if not hasattr(log, "every"):

        def _every(
            key: str, n: int = 60, msg: str | None = None, **kw: Any
        ) -> None:  # pragma: no cover
            now = time.time()
            store = getattr(_every, "_store", {})  # type: ignore[attr-defined]
            last = float(store.get(key, 0.0))
            if (now - last) >= float(n):
                store[key] = now
                _every._store = store  # type: ignore[attr-defined]
                with contextlib.suppress(Exception):
                    log.event(msg or key, **kw)

        log.every = _every  # type: ignore[attr-defined]


_ensure_log_shims()

# =============================================================================
# Tiny helpers
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


def _short(x: Any, n: int = 280) -> str:
    try:
        s = str(x)
        return s if len(s) <= n else (s[:n] + "…")
    except Exception:
        return "<?>"


def _now_ts() -> float:
    return time.time()


def _now_utc() -> datetime:
    return datetime.now(UTC)


async def _maybe_await(x: Any) -> Any:
    return await x if inspect.isawaitable(x) else x


def _fast_fail() -> bool:
    return _b("SNIPER_FAST_FAIL", _b("MAIN_FAST_FAIL", False))


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
    try:
        import config as C  # type: ignore

        return getattr(C, name, default)
    except Exception:
        return default


# =============================================================================
# Canonical Candidate schema (prefer scanner.registry)
# =============================================================================
RegistryCandidate = None
reg = None
try:
    reg = importlib.import_module("scanner.registry")
    RegistryCandidate = getattr(reg, "Candidate", None)
except Exception:
    reg = None  # type: ignore[assignment]
    RegistryCandidate = None

_BASE58_RE = re.compile(r"[1-9A-HJ-NP-Za-km-z]{32,44}")


def _normalize_mint(m: Any) -> str:
    if not isinstance(m, str):
        m = str(m or "")
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


def _coalesce(src: dict[str, Any], *keys: str, default: Any = None) -> Any:
    for k in keys:
        if k in src:
            v = src.get(k)
            if v is not None and v != "":
                return v
    return default


@dataclass(frozen=True)
class CandidateLite:
    """
    Fallback Candidate schema if scanner.registry.Candidate isn't importable.
    """

    mint: str
    symbol: str
    source: str
    ts: float
    liq_usd: float = 0.0
    pool_age_min: float = 9999.0
    vol_5m_usd: float = 0.0
    vol_24h_usd: float = 0.0
    buys_5m: int = 0
    sells_5m: int = 0
    buy_skew_5m: float = 0.0
    impact_1k_bps: float = 0.0
    extra: dict[str, Any] = field(default_factory=dict)


CandidateT = Union[CandidateLite, Any]


def _as_dict(c: CandidateT) -> dict[str, Any]:
    if isinstance(c, dict):
        return dict(c)
    if hasattr(c, "__dict__"):
        d = dict(getattr(c, "__dict__", {}) or {})
        for k in (
            "mint",
            "symbol",
            "source",
            "ts",
            "liq_usd",
            "pool_age_min",
            "vol_5m_usd",
            "vol_24h_usd",
            "buys_5m",
            "sells_5m",
            "buy_skew_5m",
            "impact_1k_bps",
        ):
            if k not in d and hasattr(c, k):
                d[k] = getattr(c, k)
        return d
    return {"raw": c}


def _normalize_candidate(raw: Any, *, source: str) -> CandidateT | None:
    if RegistryCandidate is not None and isinstance(raw, RegistryCandidate):  # type: ignore[arg-type]
        return raw
    if isinstance(raw, CandidateLite):
        return raw
    if isinstance(raw, dict):
        d = dict(raw)
        mint = (
            d.get("mint")
            or d.get("base_mint")
            or d.get("baseMint")
            or (d.get("baseToken") or {}).get("address")
            or d.get("address")
        )
        mint_s = _normalize_mint(mint)
        if not mint_s:
            return None

        sym = (
            d.get("symbol")
            or d.get("sym")
            or (d.get("baseToken") or {}).get("symbol")
            or mint_s[:6]
        )
        sym_s = str(sym).strip() if sym is not None else mint_s[:6]

        ts = d.get("ts") or d.get("scan_ts") or d.get("timestamp")
        ts_f = float(ts) if isinstance(ts, int | float) and float(ts) > 0 else _now_ts()

        liq = _nzf(_coalesce(d, "liq_usd", "liquidity_usd", "liquidityUSD", "liq"), 0.0)
        age = _nzf(
            _coalesce(d, "pool_age_min", "token_age_min", "poolAgeMin", "ageMin", "age_min"), 9999.0
        )
        vol5 = _nzf(_coalesce(d, "vol_5m_usd", "vol5m_usd", "volumeUsd5m", "vol5"), 0.0)
        vol24 = _nzf(_coalesce(d, "vol_24h_usd", "volumeUsd24h", "vol24", "vol_24h"), 0.0)
        impact1k = _nzf(_coalesce(d, "impact_1k_bps", "priceImpact1kBps", "impact_bps"), 0.0)

        buys = _nzi(_coalesce(d, "buys_5m", "buys5m", "tx_buys_5m", "buys5", "buys"), 0)
        sells = _nzi(_coalesce(d, "sells_5m", "sells5m", "tx_sells_5m", "sells5", "sells"), 0)
        skew = _nzf(_coalesce(d, "buy_skew_5m", "skew5m", "buy_ratio_5m"), 0.0)
        if skew <= 0:
            skew = (buys + 1) / (sells + 1)

        return CandidateLite(
            mint=mint_s,
            symbol=sym_s,
            source=source,
            ts=ts_f,
            liq_usd=float(liq),
            pool_age_min=float(age),
            vol_5m_usd=float(vol5),
            vol_24h_usd=float(vol24),
            buys_5m=int(buys),
            sells_5m=int(sells),
            buy_skew_5m=float(skew),
            impact_1k_bps=float(impact1k),
            extra=d,
        )
    return None


# =============================================================================
# Core settings (safe defaults)
# =============================================================================
WRAPPER_NAME = "sniper.sniper"
WRAPPER_VERSION = "v3+++_lockin_orchestrator_v3"

SOL_MINT = "So11111111111111111111111111111111111111112"
WALLET_PUBKEY = os.getenv("WALLET_PUBKEY") or os.getenv("WALLET_PUBLIC_KEY") or ""

PAUSE_TRADING = _b("PAUSE_TRADING", False)
DRY_RUN = _b("DRY_RUN", _b("DRYRUN", False))

SCANNER_PERIOD_SEC = int(_cfg("SCANNER_PERIOD_SEC", int(_cfg("SCAN_INTERVAL", 15) or 15)) or 15)
SCAN_JITTER_SEC = _f("SCAN_JITTER_SEC", 0.35)

MAX_CONCURRENT_BUYS = max(1, _i("MAX_CONCURRENT_BUYS", 2))
MAX_OUTSTANDING_TASKS = max(
    MAX_CONCURRENT_BUYS, _i("SNIPER_MAX_OUTSTANDING_TASKS", MAX_CONCURRENT_BUYS * 6)
)

BUY_COOLDOWN_SEC = _i("BUY_COOLDOWN_SEC", 120)
SEEN_TTL_SEC = max(15, _i("SNIPER_SEEN_TTL_SEC", 90))
NOROUTE_COOLDOWN_SEC = max(0, _i("NOROUTE_COOLDOWN_SEC", 600))

MIN_POOL_AGE_MIN = max(0, _i("MIN_POOL_AGE_MIN", 0))
MIN_LIQ_USD = float(
    _cfg("MIN_LIQ_USD", float(_cfg("SCANNER_MIN_LIQ_USD", 1000.0) or 1000.0)) or 1000.0
)

MIN_SOL_BALANCE = float(_cfg("MIN_SOL_BALANCE", 0.2) or 0.2)

JUPITER_SLIPPAGE = _f("JUPITER_MAX_SLIPPAGE", 0.005)  # fraction (0.005=0.5%)
SLIPPAGE_BPS = max(1, int(JUPITER_SLIPPAGE * 10_000))

RUNNER_MIN_BUYS_5M = max(0, _i("RUNNER_MIN_BUYS_5M", 3))
RUNNER_MIN_VOL5_USD = _f("RUNNER_MIN_VOL5_USD", 200.0)
RUNNER_MIN_VOL24_USD = _f("RUNNER_MIN_VOL24_USD", 2000.0)
RUNNER_MIN_BUY_SKEW = _f("RUNNER_MIN_BUY_SKEW", 0.8)

HEARTBEATS_ENABLED = _b("HEARTBEATS_ENABLED", _b("TELEGRAM_HEARTBEATS", True))
HEARTBEAT_EVERY_SEC = max(30, _i("HEARTBEAT_EVERY_SEC", 300))
HEARTBEAT_ONLY_ON_CHANGE = _b("HEARTBEAT_ONLY_ON_CHANGE", True)

PREWARM_TOP_N = max(0, _i("PREWARM_TOP_N", 8))

PRESET = (_s("STRATEGY_PRESET", "max-pnl") or "max-pnl").lower().strip()
if PRESET not in ("max-pnl", "balanced", "conservative"):
    PRESET = "max-pnl"

BLOCKLIST = {x.strip() for x in _s("BLOCKLIST_MINTS", "").split(",") if x.strip()}
ALLOWLIST = {x.strip() for x in _s("ALLOWLIST_MINTS", "").split(",") if x.strip()}

HALT_KEYS = ("halt:manual", "halt:infra", "infra:halt", "halt:daily_loss")

REJECT_SAMPLE_RATE = _clamp(_f("SNIPER_REJECT_SAMPLE_RATE", 1.0), 0.0, 1.0)
REJECT_SUMMARY_EVERY_SEC = max(30, _i("SNIPER_REJECT_SUMMARY_EVERY_SEC", 120))
REJECT_INCLUDE_FIELDS = _b("SNIPER_REJECT_INCLUDE_FIELDS", False)

# =============================================================================
# Lazy wiring: core_utils (resources + swap + telegram + pricing)
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

        gs = getattr(m, "get_session", None)
        gr = getattr(m, "get_redis", None)
        if callable(gs):
            _session_ref = gs()
        if callable(gr):
            _redis_ref = gr()

        log.every("CORE_INIT_OK", 30, has_redis=bool(_redis_ref), has_session=bool(_session_ref))  # type: ignore[misc]
    except Exception as e:
        log.every("CORE_INIT_FAIL", 15, err=_short(e))  # type: ignore[misc]
        if _fast_fail():
            raise


async def close_resources() -> None:
    global _session_ref, _redis_ref
    with contextlib.suppress(Exception):
        m = importlib.import_module("utils_pkg.core_utils")
        fn = getattr(m, "close_resources", None)
        if callable(fn):
            out = fn()
            if inspect.isawaitable(out):
                await out
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
        return 0.0
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
        return 0.0
    return 0.0


async def swap_token_jupiter(**kwargs: Any) -> tuple[bool, str, dict[str, Any]]:
    """
    Normalize core_utils.swap_token_jupiter outputs into (ok, sig, meta).
    """
    try:
        m = importlib.import_module("utils_pkg.core_utils")
        fn = getattr(m, "swap_token_jupiter", None)
        if not callable(fn):
            return False, "", {"err": "missing_swap_token_jupiter"}
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
            return ok, sig, dict(out)

        return False, "", {"err": "swap_bad_return", "raw": _short(out)}
    except Exception as e:
        return False, "", {"err": "swap_exception", "exc": _short(e)}


async def confirm_signature(sig: str, timeout_s: float = 20.0) -> bool:
    """
    Best-effort confirmation. If not available, returns True when sig exists.
    """
    if not sig:
        return False
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
    await asyncio.sleep(0.05)
    return True


# =============================================================================
# Optional wiring: DB logging (best-effort)
# =============================================================================
async def ensure_schema() -> None:
    with contextlib.suppress(Exception):
        m = importlib.import_module("utils_pkg.ai_db")
        fn = getattr(m, "ensure_schema", None)
        if callable(fn):
            out = fn()
            if inspect.isawaitable(out):
                await out


async def log_event(name: str, payload: dict[str, Any]) -> None:
    with contextlib.suppress(Exception):
        m = importlib.import_module("utils_pkg.data_logger")
        fn = getattr(m, "log_event", None)
        if callable(fn):
            out = fn(name, payload)
            if inspect.isawaitable(out):
                await out


# =============================================================================
# Optional wiring: exit engine handoff (canonical)
# =============================================================================
async def start_exit_supervisor(redis: Any) -> None:
    for mod_name in (
        "utils_pkg.exit_engine",
        "utils_pkg.auto_exit_worker.exit_engine",
        "execution.exits",
    ):
        with contextlib.suppress(Exception):
            m = importlib.import_module(mod_name)
            fn = (
                getattr(m, "start_exit_supervisor", None)
                or getattr(m, "run_supervisor", None)
                or getattr(m, "start", None)
            )
            if callable(fn):
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
                return


async def register_entry(**kw: Any) -> None:
    for mod_name in (
        "utils_pkg.exit_engine",
        "utils_pkg.auto_exit_worker.exit_engine",
        "execution.exits",
    ):
        with contextlib.suppress(Exception):
            m = importlib.import_module(mod_name)
            fn = getattr(m, "register_entry", None) or getattr(m, "on_entry", None)
            if callable(fn):
                out = fn(**kw)
                if inspect.isawaitable(out):
                    await out
                return


# =============================================================================
# Optional wiring: feature builder + entry IQ + risk manager + router
# =============================================================================
async def build_features(
    candidate: CandidateT, ctx: dict[str, Any] | None = None
) -> dict[str, Any]:
    ctx = dict(ctx or {})
    with contextlib.suppress(Exception):
        m = importlib.import_module("utils_pkg.feature_builder")
        for nm in ("build_features", "build_feature_dict", "build", "make_features"):
            fn = getattr(m, nm, None)
            if callable(fn):
                try:
                    sig = inspect.signature(fn)
                    out = fn(candidate) if len(sig.parameters) == 1 else fn(candidate, ctx)
                except TypeError:
                    out = fn(candidate)
                out = await _maybe_await(out)
                return dict(out or {})
    d = _as_dict(candidate)
    return {
        "mint": d.get("mint"),
        "symbol": d.get("symbol") or d.get("sym"),
        "source": d.get("source"),
        "ts": d.get("ts"),
        "liq_usd": d.get("liq_usd") or d.get("liquidity_usd"),
        "pool_age_min": d.get("pool_age_min"),
        "vol_5m_usd": d.get("vol_5m_usd"),
        "vol_24h_usd": d.get("vol_24h_usd"),
        "buys_5m": d.get("buys_5m"),
        "sells_5m": d.get("sells_5m"),
        "buy_skew_5m": d.get("buy_skew_5m"),
        "impact_1k_bps": d.get("impact_1k_bps"),
        "fallback_features": True,
    }


async def entry_iq_should_enter(
    features: dict[str, Any],
) -> tuple[bool, float, str, dict[str, Any]]:
    with contextlib.suppress(Exception):
        m = importlib.import_module("utils_pkg.ai_entry_logic")
        should = getattr(m, "should_enter", None)
        score_fn = getattr(m, "score_entry", None)

        if callable(should):
            out = should(features)
            out = await _maybe_await(out)
            if isinstance(out, tuple) and len(out) >= 2:
                ok = bool(out[0])
                score = float(out[1] or 0.0)
                why = str(out[2] or "") if len(out) >= 3 else ""
                meta = dict(out[3] or {}) if len(out) >= 4 and isinstance(out[3], dict) else {}
                return ok, score, why, meta
            if isinstance(out, dict):
                return (
                    bool(out.get("ok", True)),
                    float(out.get("score", 0.0)),
                    str(out.get("why", "")),
                    dict(out),
                )
            return bool(out), 0.0, "ai_entry_logic_bool", {}

        if callable(score_fn):
            sc = score_fn(features)
            sc = await _maybe_await(sc)
            score = float(sc or 0.0)
            gate = float(_f("AI_SCORE_GATE", 0.5))
            ok = score >= gate
            return ok, score, f"score_only gate={gate}", {}

    return True, 0.0, "no_ai_entry_logic", {}


async def risk_should_allow_new_entry(redis: Any) -> bool:
    with contextlib.suppress(Exception):
        m = importlib.import_module("utils_pkg.risk_manager")
        fn = getattr(m, "should_allow_new_entry", None)
        if callable(fn):
            out = fn(redis)
            out = await _maybe_await(out)
            return bool(out)
    return True


async def risk_current_limits(redis: Any) -> dict[str, Any]:
    with contextlib.suppress(Exception):
        m = importlib.import_module("utils_pkg.risk_manager")
        fn = getattr(m, "current_limits", None)
        if callable(fn):
            out = fn(redis)
            out = await _maybe_await(out)
            return dict(out or {})
    return {
        "ticket_usd_pct": float(_cfg("TICKET_PCT_EQUITY", 0.04) or 0.04),
        "ticket_cap_usd": float(_cfg("TICKET_USD_CAP", 300.0) or 300.0),
    }


async def risk_note_spend(redis: Any, usd: float) -> None:
    with contextlib.suppress(Exception):
        m = importlib.import_module("utils_pkg.risk_manager")
        fn = getattr(m, "note_spend", None)
        if callable(fn):
            out = fn(redis, usd)
            if inspect.isawaitable(out):
                await out


async def router_precheck_buy(
    mint: str, sol_to_spend: float, slippage_bps: int
) -> tuple[bool, str, dict[str, Any]]:
    try:
        m = importlib.import_module("utils_pkg.router")
        fn = getattr(m, "get_sell_quote", None) or getattr(m, "get_buy_quote", None)
        if not callable(fn):
            return True, "no_router_module", {"hops": 0, "impact_bps": 0}

        amount_raw = int(max(1, float(sol_to_spend) * 1e9))
        kw: dict[str, Any] = {
            "mint": mint,
            "amount_raw": amount_raw,
            "side": "sell",
            "input_is_mint": True,
            "slippage_bps": int(slippage_bps),
        }
        with contextlib.suppress(Exception):
            sig = inspect.signature(fn)
            allowed = set(sig.parameters.keys())
            kw = {k: v for k, v in kw.items() if k in allowed}

        q = fn(**kw)
        q = await _maybe_await(q)
        if not isinstance(q, dict) or not q:
            return False, "no_route", {"hops": 0, "impact_bps": 9999, "out_amount": 0}

        if q.get("error"):
            return False, "quote_error", {"error": str(q.get("error"))}

        routes = q.get("routes") or q.get("routePlan") or []
        best = routes[0] if isinstance(routes, list) and routes else {}
        impact = _nzf(
            _coalesce(q, "priceImpactPct", default=_coalesce(best, "priceImpactPct")), 0.0
        )
        out_amount = _nzf(_coalesce(q, "outAmount", "out_amount"), 0.0)
        hops = _nzi(_coalesce(best, "hops", "swapCount"), 0)

        meta = {
            "hops": hops,
            "impact_bps": int(abs(float(impact)) * 100.0),
            "out_amount": out_amount,
        }
        return True, "ok", meta
    except asyncio.CancelledError:
        raise
    except Exception as e:
        return False, f"router_exc:{type(e).__name__}", {"err": _short(e)}


# =============================================================================
# Redis helpers (async compatible)
# =============================================================================
async def _redis_get(redis: Any, key: str) -> str | None:
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


async def _redis_set(
    redis: Any, key: str, value: str, *, ex: int | None = None, nx: bool = False
) -> bool:
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
# Cross-process + local mint lock (deterministic)
# =============================================================================
@dataclass
class _MintLocks:
    locks: dict[str, asyncio.Lock] = field(default_factory=dict)

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
      - local asyncio lock (per-process)
      - optional redis NX lock (cross-process)
    """

    def __init__(self, mint: str, ttl: int = 120) -> None:
        self.mint = mint
        self.ttl = int(max(15, ttl))
        self._local = _MINT_LOCKS.lock_for(mint)
        self._redis: Any = None
        self._key = f"lock:buy:{mint}"
        self._token = f"{os.getpid()}:{random.randint(1, 10**9)}"

    async def __aenter__(self) -> bool:
        await self._local.acquire()
        self._redis = get_redis()
        if not self._redis:
            return True
        ok = await _redis_set(self._redis, self._key, self._token, ex=self.ttl, nx=True)
        if not ok:
            self._local.release()
            return False
        return True

    async def __aexit__(self, exc_type, exc, tb) -> None:
        try:
            if self._redis:
                cur = await _redis_get(self._redis, self._key)
                if cur == self._token:
                    await _redis_del(self._redis, self._key)
        finally:
            with contextlib.suppress(Exception):
                if self._local.locked():
                    self._local.release()


# =============================================================================
# Reject buckets + sampled logging (no silent no-buy)
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
    RISK = "risk_manager_block"
    ENTRY_IQ = "entry_iq_reject"
    NO_ROUTE = "no_route_quote"
    SWAP_FAIL = "swap_failed"
    UNVERIFIED = "unverified_fill"
    BALANCE = "min_sol_balance"
    LOCKED = "mint_lock_busy"
    DRYRUN = "dry_run"
    INTERNAL = "internal_error"


@dataclass
class RejectStats:
    by_bucket: dict[str, int] = field(default_factory=dict)
    last_emit_ts: float = 0.0

    def bump(self, bucket: str) -> None:
        self.by_bucket[bucket] = int(self.by_bucket.get(bucket, 0)) + 1


_REJECTS = RejectStats()


def _should_sample() -> bool:
    if REJECT_SAMPLE_RATE >= 1.0:
        return True
    return random.random() < float(REJECT_SAMPLE_RATE)


async def _reject(
    bucket: str,
    mint: str,
    sym: str,
    reason: str,
    *,
    cand: CandidateT | None = None,
    extra: dict[str, Any] | None = None,
) -> None:
    _REJECTS.bump(bucket)

    payload: dict[str, Any] = {
        "type": "reject",
        "bucket": bucket,
        "reason": reason,
        "mint": mint,
        "symbol": sym,
        "ts": _now_utc().isoformat(),
    }
    if extra:
        payload.update(extra)
    if REJECT_INCLUDE_FIELDS and cand is not None:
        d = _as_dict(cand)
        payload["cand"] = {
            k: d.get(k)
            for k in (
                "source",
                "liq_usd",
                "pool_age_min",
                "vol_5m_usd",
                "buys_5m",
                "sells_5m",
                "buy_skew_5m",
                "impact_1k_bps",
            )
        }

    if _should_sample():
        log.event("REJECT", **payload)

    with contextlib.suppress(Exception):
        await log_event("reject", payload)


async def _reject_summary_tick(force: bool = False) -> None:
    now = _now_ts()
    if not force and (now - _REJECTS.last_emit_ts) < float(REJECT_SUMMARY_EVERY_SEC):
        return
    _REJECTS.last_emit_ts = now
    if not _REJECTS.by_bucket:
        return
    log.event("REJECT_SUMMARY", buckets=dict(_REJECTS.by_bucket), preset=PRESET, v=WRAPPER_VERSION)
    with contextlib.suppress(Exception):
        await log_event(
            "reject_summary",
            {
                "ts": _now_utc().isoformat(),
                "buckets": dict(_REJECTS.by_bucket),
                "preset": PRESET,
                "v": WRAPPER_VERSION,
            },
        )


# =============================================================================
# Halt / cooldown / runner gates
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
            log.every(
                "HALT_ACTIVE",
                10,
                flags={
                    k: (v.decode() if isinstance(v, (bytes, bytearray)) else v)
                    for k, v in zip(HALT_KEYS, vals, strict=False)
                },
            )  # type: ignore[misc]
        return halted
    except Exception as e:
        log.every("HALT_CHECK_ERR", 10, err=_short(e))  # type: ignore[misc]
        return False


async def _cooldown_active(key: str, ttl: int) -> bool:
    r = get_redis()
    if not r or ttl <= 0:
        return False
    if await _redis_get(r, key):
        return True
    await _redis_set(r, key, "1", ex=int(ttl), nx=False)
    return False


def _is_runner_from_dict(d: dict[str, Any]) -> bool:
    buys = int(d.get("buys_5m") or 0)
    sells = int(d.get("sells_5m") or 0)
    liq = float(d.get("liq_usd") or 0.0)
    vol5 = float(d.get("vol_5m_usd") or 0.0)
    vol24 = float(d.get("vol_24h_usd") or 0.0)
    skew = float(d.get("buy_skew_5m") or 0.0)
    if skew <= 0:
        skew = (buys + 1) / (sells + 1)

    if buys < RUNNER_MIN_BUYS_5M:
        return False
    if vol5 < RUNNER_MIN_VOL5_USD and vol24 < RUNNER_MIN_VOL24_USD:
        return False
    if liq < MIN_LIQ_USD:
        return False
    return not skew <= RUNNER_MIN_BUY_SKEW


# =============================================================================
# Scan layer (prefer scanner.registry; fallback wrappers)
# =============================================================================
async def scan_candidates() -> list[CandidateT]:
    out: list[CandidateT] = []

    if reg is not None:
        for nm in ("scan_all", "scan", "collect_candidates", "get_candidates"):
            fn = getattr(reg, nm, None)
            if callable(fn):
                try:
                    try:
                        sig = inspect.signature(fn)
                        n = len(sig.parameters)
                    except Exception:
                        n = 0

                    if n >= 2:
                        raw = fn(get_redis(), get_session())
                    elif n == 1:
                        raw = fn(get_session() or get_redis())
                    else:
                        raw = fn()

                    raw = await _maybe_await(raw)
                    if hasattr(raw, "__aiter__"):
                        async for item in raw:
                            c = _normalize_candidate(
                                item, source=getattr(item, "source", "registry")
                            )
                            if c is not None:
                                out.append(c)
                    elif isinstance(raw, list):
                        for item in raw:
                            c = _normalize_candidate(item, source="registry")
                            if c is not None:
                                out.append(c)
                    elif raw is not None:
                        c = _normalize_candidate(raw, source="registry")
                        if c is not None:
                            out.append(c)

                    if out:
                        return _dedupe_best(out)
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    log.every("REGISTRY_SCAN_ERR", 10, err=_short(e))  # type: ignore[misc]
                    if _fast_fail():
                        raise
                    break

    async def _collect_from(mod_name: str, func_name: str, source: str) -> None:
        with contextlib.suppress(Exception):
            m = importlib.import_module(mod_name)
            fn = getattr(m, func_name, None)
            if not callable(fn):
                return

            for arity in (2, 1, 0):
                try:
                    if arity == 2:
                        raw = fn(get_redis(), get_session())
                    elif arity == 1:
                        raw = fn(get_redis() or get_session())
                    else:
                        raw = fn()
                    raw = await _maybe_await(raw)

                    items: list[Any]
                    if raw is None:
                        items = []
                    elif hasattr(raw, "__aiter__"):
                        items = []
                        async for it in raw:
                            items.append(it)
                    elif isinstance(raw, list):
                        items = raw
                    elif isinstance(raw, dict):
                        items = [raw]
                    else:
                        try:
                            items = list(raw)  # type: ignore[arg-type]
                        except Exception:
                            items = []

                    for it in items:
                        c = _normalize_candidate(it, source=source)
                        if c is not None:
                            out.append(c)
                    return
                except TypeError:
                    continue

    # Best-effort legacy scanners
    async with asyncio.TaskGroup() as tg:
        tg.create_task(_collect_from("scanner.raydium", "scan_raydium", "raydium"))
        tg.create_task(_collect_from("scanner.dexscreener", "scan_dexscreener", "dexscreener"))
        tg.create_task(_collect_from("scanner.pumpfun", "scan_pumpfun", "pumpfun"))
        tg.create_task(_collect_from("scanner.raydium_scanner", "scan_raydium", "raydium"))
        tg.create_task(
            _collect_from("scanner.dexscreener_scanner", "scan_dexscreener", "dexscreener")
        )
        tg.create_task(_collect_from("scanner.pumpfun_scanner", "scan_pumpfun", "pumpfun"))

    return _dedupe_best(out)


def _dedupe_best(out: list[CandidateT]) -> list[CandidateT]:
    def _rank(c: CandidateT) -> tuple[float, float, float, float]:
        d = _as_dict(c)
        return (
            float(d.get("score_combined") or d.get("rank_score") or 0.0),
            float(d.get("vol_5m_usd") or 0.0),
            float(d.get("liq_usd") or 0.0),
            float(d.get("buy_skew_5m") or 0.0),
        )

    best: dict[str, CandidateT] = {}
    for c in out:
        d = _as_dict(c)
        mint = str(d.get("mint") or "")
        if not mint:
            continue
        if mint not in best or _rank(c) > _rank(best[mint]):
            best[mint] = c
    return list(best.values())


# =============================================================================
# Ranking + ticket sizing
# =============================================================================
def _rank_score(c: CandidateT) -> float:
    d = _as_dict(c)
    s = _nzf(d.get("score_combined") or d.get("rank_score"), 0.0)
    if s > 0:
        return float(s)

    age = _nzf(d.get("pool_age_min"), 9999.0)
    liq = _nzf(d.get("liq_usd"), 0.0)
    vol5 = _nzf(d.get("vol_5m_usd"), 0.0)
    skew = _nzf(d.get("buy_skew_5m"), 0.0)
    if skew <= 0:
        buys = int(d.get("buys_5m") or 0)
        sells = int(d.get("sells_5m") or 0)
        skew = (buys + 1) / (sells + 1)

    score = 0.0
    score += max(0.0, 90.0 - age) * 0.30
    score += min(liq, 200_000.0) / 10_000.0
    score += min(vol5, 50_000.0) / 5_000.0
    score += max(0.0, min(2.5, skew)) * 5.0
    return float(score)


def select_top(cands: list[CandidateT], k: int) -> list[CandidateT]:
    if not cands:
        return []
    ranked = sorted(cands, key=_rank_score, reverse=True)
    return ranked[: max(1, int(k))]


async def _compute_equity_usd() -> float:
    try:
        if not WALLET_PUBKEY:
            return float(_cfg("EQUITY_USD_FALLBACK", 2000.0) or 2000.0)
        sol_bal = await get_sol_balance(WALLET_PUBKEY)
        usd_per_sol = await convert_sol_to_usd(1.0) or 0.0
        if usd_per_sol <= 0:
            return float(_cfg("EQUITY_USD_FALLBACK", 2000.0) or 2000.0)
        return max(0.0, float(sol_bal) * float(usd_per_sol))
    except Exception:
        return float(_cfg("EQUITY_USD_FALLBACK", 2000.0) or 2000.0)


async def _ticket_usd_for_equity(equity_usd: float) -> float:
    lims = await risk_current_limits(get_redis())
    pct = float(lims.get("ticket_usd_pct") or _cfg("TICKET_PCT_EQUITY", 0.04) or 0.04)
    cap = float(lims.get("ticket_cap_usd") or _cfg("TICKET_USD_CAP", 300.0) or 300.0)
    base = float(equity_usd) * float(pct)
    return float(min(cap, max(10.0, base)))


# =============================================================================
# Heartbeats
# =============================================================================
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
        if (
            HEARTBEAT_ONLY_ON_CHANGE
            and last_eq > 0
            and abs(float(last_eq) - float(equity_usd)) < 1.0
        ):
            await _redis_set(r, "hb:last_ts", str(now), ex=24 * 3600, nx=False)
            return

        delta_txt = ""
        if last_eq > 0:
            delta = float(equity_usd) - float(last_eq)
            pct = (delta / float(last_eq)) * 100.0 if float(last_eq) != 0 else 0.0
            delta_txt = f" (Δ {delta:+,.0f}, {pct:+.2f}%)"

        msg = f"📈 Heartbeat | equity ${equity_usd:,.0f}{delta_txt}"
        await send_telegram(msg)

        await _redis_set(r, "hb:last_ts", str(now), ex=24 * 3600, nx=False)
        await _redis_set(r, "hb:last_equity", f"{equity_usd:.2f}", ex=24 * 3600, nx=False)
    except Exception as e:
        log.every("HB_ERR", 20, err=_short(e))  # type: ignore[misc]


# =============================================================================
# Sniper orchestrator
# =============================================================================
@dataclass
class LoopStats:
    started_ts: float
    loops: int = 0
    scanned: int = 0
    unique: int = 0
    considered: int = 0
    enters_ok: int = 0
    enters_try: int = 0


class Sniper:
    """
    Single orchestrator instance.
    """

    def __init__(self) -> None:
        self.sem = asyncio.Semaphore(MAX_CONCURRENT_BUYS)
        self.stop_evt = asyncio.Event()
        self.stats = LoopStats(started_ts=_now_ts())
        self._tasks: set[asyncio.Task[None]] = set()

    async def stop(self) -> None:
        self.stop_evt.set()

    def _track_task(self, t: asyncio.Task[None]) -> None:
        self._tasks.add(t)

        def _done(_t: asyncio.Task[None]) -> None:
            self._tasks.discard(_t)

        t.add_done_callback(_done)

    async def _cancel_outstanding(self) -> None:
        if not self._tasks:
            return
        for t in list(self._tasks):
            t.cancel()
        with contextlib.suppress(Exception):
            await asyncio.gather(*list(self._tasks), return_exceptions=True)
        self._tasks.clear()

    async def _seen_dedupe(self, mint: str) -> bool:
        r = get_redis()
        if not r:
            return False
        k = f"seen:{mint}"
        if await _redis_get(r, k):
            return True
        await _redis_set(r, k, "1", ex=SEEN_TTL_SEC, nx=False)
        return False

    async def _basic_gates(self, c: CandidateT) -> tuple[bool, str]:
        d = _as_dict(c)
        mint = str(d.get("mint") or "")
        sym = str(d.get("symbol") or d.get("sym") or mint[:6] or "UNK")

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

        if await _cooldown_active(f"buycd:{mint}", BUY_COOLDOWN_SEC):
            await _reject(RB.COOLDOWN, mint, sym, "buy_cooldown_active", cand=c)
            return False, "cooldown"

        if NOROUTE_COOLDOWN_SEC > 0 and await _cooldown_active(
            f"noroutecd:{mint}", NOROUTE_COOLDOWN_SEC
        ):
            await _reject(RB.NOROUTE_COOLDOWN, mint, sym, "noroute_cooldown_active", cand=c)
            return False, "noroute_cooldown"

        if await self._seen_dedupe(mint):
            await _reject(RB.SEEN, mint, sym, "recently_seen_dedupe", cand=c)
            return False, "seen"

        if float(d.get("pool_age_min") or 9999.0) < float(MIN_POOL_AGE_MIN):
            await _reject(RB.MIN_AGE, mint, sym, f"pool_age<{MIN_POOL_AGE_MIN}", cand=c)
            return False, "min_age"

        if float(d.get("liq_usd") or 0.0) < float(MIN_LIQ_USD):
            await _reject(RB.MIN_LIQ, mint, sym, f"liq_usd<{MIN_LIQ_USD:.0f}", cand=c)
            return False, "min_liq"

        if not _is_runner_from_dict(d):
            await _reject(RB.NOT_RUNNER, mint, sym, "not_runner_filter", cand=c)
            return False, "not_runner"

        if WALLET_PUBKEY:
            bal = await get_sol_balance(WALLET_PUBKEY)
            if float(bal) <= float(MIN_SOL_BALANCE):
                await _reject(
                    RB.BALANCE,
                    mint,
                    sym,
                    f"sol_balance<{MIN_SOL_BALANCE}",
                    cand=c,
                    extra={"bal_sol": bal},
                )
                return False, "balance"

        if not await risk_should_allow_new_entry(get_redis()):
            await _reject(RB.RISK, mint, sym, "risk_manager_block", cand=c)
            return False, "risk_block"

        return True, "ok"

    async def _execute_one(self, c: CandidateT, *, equity_usd: float) -> bool:
        d = _as_dict(c)
        mint = str(d.get("mint") or "")
        sym = str(d.get("symbol") or d.get("sym") or mint[:6] or "UNK")

        ok_basic, _ = await self._basic_gates(c)
        if not ok_basic:
            return False

        features = await build_features(c, ctx={"equity_usd": float(equity_usd), "preset": PRESET})
        ok_enter, score01, why, meta = await entry_iq_should_enter(features)
        if not ok_enter:
            await _reject(
                RB.ENTRY_IQ,
                mint,
                sym,
                f"{why or 'reject'}",
                cand=c,
                extra={"score": score01, "meta": meta},
            )
            return False

        ticket_usd = await _ticket_usd_for_equity(float(equity_usd))
        usd_per_sol = await convert_sol_to_usd(1.0) or 0.0
        if usd_per_sol <= 0:
            await _reject(RB.INTERNAL, mint, sym, "usd_per_sol_unavailable", cand=c)
            return False
        sol_to_spend = float(ticket_usd) / float(usd_per_sol)
        if sol_to_spend <= 0:
            await _reject(RB.INTERNAL, mint, sym, "sol_to_spend_zero", cand=c)
            return False

        ok_route, why_route, rmeta = await router_precheck_buy(mint, sol_to_spend, SLIPPAGE_BPS)
        if not ok_route:
            await _reject(
                RB.NO_ROUTE,
                mint,
                sym,
                f"route:{why_route}",
                cand=c,
                extra={"meta": rmeta, "ticket_usd": ticket_usd},
            )
            if NOROUTE_COOLDOWN_SEC > 0:
                await _cooldown_active(f"noroutecd:{mint}", NOROUTE_COOLDOWN_SEC)
            return False

        async with MintEntryLock(mint, ttl=max(60, BUY_COOLDOWN_SEC)) as got_lock:
            if not got_lock:
                await _reject(RB.LOCKED, mint, sym, "mint_lock_busy", cand=c)
                return False

            if DRY_RUN:
                await _reject(
                    RB.DRYRUN,
                    mint,
                    sym,
                    "dry_run_enabled",
                    cand=c,
                    extra={
                        "ticket_usd": ticket_usd,
                        "sol_to_spend": sol_to_spend,
                        "score": score01,
                    },
                )
                with contextlib.suppress(Exception):
                    await send_telegram(
                        f"🧪 DRY-RUN — would buy {sym} ({mint}) for ${ticket_usd:,.2f} (~{sol_to_spend:.4f} SOL) score={score01:.3f}"
                    )
                return False

            kw: dict[str, Any] = {
                "input_mint": SOL_MINT,
                "output_mint": mint,
                "amount_sol": float(sol_to_spend),
                "amount_float": float(sol_to_spend),
                "amount": float(sol_to_spend),
                "slippage": float(JUPITER_SLIPPAGE),
                "slippage_bps": int(SLIPPAGE_BPS),
                "meta": {
                    "mint": mint,
                    "symbol": sym,
                    "score": float(score01),
                    "why": str(why or ""),
                    "preset": PRESET,
                },
            }

            with contextlib.suppress(Exception):
                m = importlib.import_module("utils_pkg.core_utils")
                fn = getattr(m, "swap_token_jupiter", None)
                if callable(fn):
                    sig = inspect.signature(fn)
                    allowed = set(sig.parameters.keys())
                    kw = {k: v for k, v in kw.items() if k in allowed}

            self.stats.enters_try += 1
            with contextlib.suppress(Exception):
                await log_event(
                    "buy_try",
                    {
                        "ts": _now_utc().isoformat(),
                        "mint": mint,
                        "symbol": sym,
                        "ticket_usd": float(ticket_usd),
                        "sol_to_spend": float(sol_to_spend),
                        "score": float(score01),
                        "why": str(why or ""),
                        "route": rmeta,
                    },
                )

            ok_swap, sig, swap_meta = await swap_token_jupiter(**kw)
            if not ok_swap or not sig:
                await _reject(
                    RB.SWAP_FAIL, mint, sym, "swap_failed", cand=c, extra={"meta": swap_meta}
                )
                with contextlib.suppress(Exception):
                    await log_event(
                        "buy_fail",
                        {
                            "ts": _now_utc().isoformat(),
                            "mint": mint,
                            "symbol": sym,
                            "why": "swap_failed",
                            "meta": swap_meta,
                        },
                    )
                return False

            ok_fill = await confirm_signature(sig, timeout_s=float(_f("CONFIRM_TIMEOUT_S", 20.0)))
            if not ok_fill:
                await _reject(
                    RB.UNVERIFIED,
                    mint,
                    sym,
                    "tx_unconfirmed_or_timeout",
                    cand=c,
                    extra={"sig": sig},
                )
                with contextlib.suppress(Exception):
                    await log_event(
                        "buy_unverified",
                        {"ts": _now_utc().isoformat(), "mint": mint, "symbol": sym, "sig": sig},
                    )
                return False

            with contextlib.suppress(Exception):
                await register_entry(
                    mint=mint,
                    symbol=sym,
                    entry_ts=_now_ts(),
                    ticket_usd=float(ticket_usd),
                    sol_spent=float(sol_to_spend),
                    signature=sig,
                    score=float(score01),
                    preset=PRESET,
                    route_meta=dict(rmeta),
                )

            with contextlib.suppress(Exception):
                await risk_note_spend(get_redis(), float(ticket_usd))

            self.stats.enters_ok += 1
            with contextlib.suppress(Exception):
                await log_event(
                    "buy",
                    {
                        "ts": _now_utc().isoformat(),
                        "mint": mint,
                        "symbol": sym,
                        "ticket_usd": float(ticket_usd),
                        "sol_spent": float(sol_to_spend),
                        "signature": sig,
                        "swap_meta": swap_meta,
                        "score": float(score01),
                        "why": str(why or ""),
                    },
                )

            with contextlib.suppress(Exception):
                usd_est = await convert_sol_to_usd(sol_to_spend) or 0.0
                await send_telegram(
                    f"✅ SNIPED {sym}\n"
                    f"Spent: {sol_to_spend:.4f} SOL (~${usd_est:.2f})\n"
                    f"Mint: {mint}\nTx: {sig}\n"
                    f"EntryIQ: {score01:.3f} {why or ''}".strip()
                )

            return True

    async def _prewarm(self, cands: list[CandidateT], ticket_usd: float) -> None:
        if PREWARM_TOP_N <= 0 or not cands:
            return
        usd_per_sol = await convert_sol_to_usd(1.0) or 0.0
        if usd_per_sol <= 0:
            return
        amount_sol = float(ticket_usd) / float(usd_per_sol)

        top = sorted(cands, key=_rank_score, reverse=True)[: int(PREWARM_TOP_N)]
        mints: list[str] = []
        for c in top:
            d = _as_dict(c)
            mint = str(d.get("mint") or "")
            if mint:
                mints.append(mint)
        if not mints:
            return

        with contextlib.suppress(Exception):
            m = importlib.import_module("jup_quote_cache")
            fn = getattr(m, "prewarm_quotes", None)
            if callable(fn):
                out = fn(mints, amount_sol, SLIPPAGE_BPS)
                if inspect.isawaitable(out):
                    await out

    async def _run_candidate(self, candidate: CandidateT, equity_usd_snapshot: float) -> None:
        """
        B023-safe runner: equity is passed explicitly (no loop-variable capture).
        """
        async with self.sem:
            d = _as_dict(candidate)
            mint = str(d.get("mint") or "")
            sym = str(d.get("symbol") or d.get("sym") or mint[:6] or "UNK")
            try:
                self.stats.considered += 1
                await self._execute_one(candidate, equity_usd=float(equity_usd_snapshot))
            except asyncio.CancelledError:
                raise
            except Exception as e:
                await _reject(
                    RB.INTERNAL,
                    mint,
                    sym,
                    "execute_exception",
                    cand=candidate,
                    extra={"err": _short(e)},
                )
                with contextlib.suppress(Exception):
                    await log_event(
                        "error",
                        {
                            "ts": _now_utc().isoformat(),
                            "where": "execute_one",
                            "err": _short(e),
                            "mint": mint,
                            "symbol": sym,
                        },
                    )
                if _fast_fail():
                    raise

    async def main_loop(self) -> None:
        with contextlib.suppress(Exception):
            await send_telegram(f"🎯 Sniper started (preset={PRESET}, v={WRAPPER_VERSION}).")

        with contextlib.suppress(Exception):
            asyncio.create_task(start_exit_supervisor(get_redis()))

        try:
            while not self.stop_evt.is_set():
                self.stats.loops += 1

                equity_usd = await _compute_equity_usd()
                await _send_heartbeat(equity_usd)
                await _reject_summary_tick(force=False)

                if await _is_halted():
                    await asyncio.sleep(float(SCANNER_PERIOD_SEC))
                    continue

                self.stats.scanned += 1
                cands = await scan_candidates()
                self.stats.unique = len(cands)

                if not cands:
                    with contextlib.suppress(Exception):
                        await log_event(
                            "scan_batch",
                            {"ts": _now_utc().isoformat(), "unique": 0, "preset": PRESET},
                        )
                    await asyncio.sleep(float(SCANNER_PERIOD_SEC))
                    continue

                if PRESET == "max-pnl":
                    pool_size = max(MAX_CONCURRENT_BUYS * 4, 12)
                elif PRESET == "balanced":
                    pool_size = max(MAX_CONCURRENT_BUYS * 3, 9)
                else:
                    pool_size = max(MAX_CONCURRENT_BUYS * 2, 6)

                top = select_top(cands, pool_size)

                with contextlib.suppress(Exception):
                    ticket0 = await _ticket_usd_for_equity(float(equity_usd))
                    await self._prewarm(top, float(ticket0))

                with contextlib.suppress(Exception):
                    await log_event(
                        "scan_batch",
                        {
                            "ts": _now_utc().isoformat(),
                            "unique": len(cands),
                            "top": len(top),
                            "preset": PRESET,
                            "equity_usd": float(equity_usd),
                        },
                    )

                # Backpressure: don't let tasks grow unbounded
                if len(self._tasks) > MAX_OUTSTANDING_TASKS:
                    log.every(
                        "TASK_BACKPRESSURE",
                        10,
                        outstanding=len(self._tasks),
                        max_outstanding=MAX_OUTSTANDING_TASKS,
                    )  # type: ignore[misc]
                else:
                    for cand in top:
                        if self.stop_evt.is_set():
                            break
                        # Explicit snapshot binding (B023-safe)
                        t = asyncio.create_task(self._run_candidate(cand, float(equity_usd)))
                        self._track_task(t)

                base = float(SCANNER_PERIOD_SEC)
                jitter = float(SCAN_JITTER_SEC)
                await asyncio.sleep(max(0.2, base + random.uniform(-jitter, jitter)))

        except asyncio.CancelledError:
            raise
        except Exception as e:
            log.every("LOOP_ERR", 5, err=_short(e))  # type: ignore[misc]
            with contextlib.suppress(Exception):
                await log_event(
                    "error", {"ts": _now_utc().isoformat(), "where": "main_loop", "err": _short(e)}
                )
            if _fast_fail():
                raise
        finally:
            with contextlib.suppress(Exception):
                await _reject_summary_tick(force=True)
            await self._cancel_outstanding()


# =============================================================================
# Entrypoints + status
# =============================================================================
async def run_sniper() -> None:
    sniper = Sniper()
    await sniper.main_loop()


async def main() -> None:
    await init_resources()
    with contextlib.suppress(Exception):
        await ensure_schema()

    with contextlib.suppress(Exception):
        asyncio.create_task(start_exit_supervisor(get_redis()))

    try:
        await run_sniper()
    finally:
        with contextlib.suppress(Exception):
            await close_resources()


def sniper_status() -> dict[str, Any]:
    return {
        "wrapper": WRAPPER_NAME,
        "version": WRAPPER_VERSION,
        "preset": PRESET,
        "paused": int(PAUSE_TRADING),
        "dry_run": int(DRY_RUN),
        "max_concurrent_buys": MAX_CONCURRENT_BUYS,
        "max_outstanding_tasks": MAX_OUTSTANDING_TASKS,
        "scanner_period_sec": SCANNER_PERIOD_SEC,
        "min_liq_usd": MIN_LIQ_USD,
        "min_pool_age_min": MIN_POOL_AGE_MIN,
        "rejects": dict(_REJECTS.by_bucket),
        "has_redis": int(bool(get_redis())),
        "has_session": int(bool(get_session())),
    }


def _install_signal_handlers(sniper: Sniper) -> None:
    loop = asyncio.get_running_loop()

    def _ask_stop() -> None:
        log.event("SHUTDOWN_SIGNAL", ts=_now_utc().isoformat())
        loop.create_task(sniper.stop())

    with contextlib.suppress(Exception):
        loop.add_signal_handler(signal.SIGTERM, _ask_stop)
        loop.add_signal_handler(signal.SIGINT, _ask_stop)


async def _main_service() -> None:
    await init_resources()
    with contextlib.suppress(Exception):
        await ensure_schema()

    sniper = Sniper()
    _install_signal_handlers(sniper)

    with contextlib.suppress(Exception):
        asyncio.create_task(start_exit_supervisor(get_redis()))

    try:
        await sniper.main_loop()
    finally:
        with contextlib.suppress(Exception):
            await close_resources()


__all__ = ["Sniper", "main", "run_sniper", "sniper_status"]

if __name__ == "__main__":  # pragma: no cover
    with contextlib.suppress(Exception):
        if os.name == "nt" and hasattr(asyncio, "WindowsSelectorEventLoopPolicy"):
            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())  # type: ignore[attr-defined]
    asyncio.run(_main_service())
