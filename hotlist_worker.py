from __future__ import annotations

"""
hotlist_worker.py (P0 — production, God-Tier Overhaul)
=====================================================

Purpose
-------
Continuously pulls "hot" token candidates (Dexscreener search / trending-style feed),
applies strong risk + quality gates, then:

DEFAULT (recommended):
  - QUEUE mode: publishes candidates to Redis for the canonical sniper/entry pipeline
    to decide & execute (prevents "competing execution engines").

OPTIONAL (explicit):
  - TRADE mode: executes buys directly via Jupiter (only if HOTLIST_MODE=TRADE).

Hard Requirements (P0 lock-in)
------------------------------
- Safe-by-default (no trading unless explicitly enabled by env)
- No network/redis calls at import time
- No duplicate entries: entry lock + cooldown (persisted in Redis when available)
- Risk gates: liquidity / impact / basic activity / allow-entry + ticket sizing
- Observability: logs + optional Telegram pings on key transitions
- Cancellation-safe (never swallow asyncio.CancelledError)
- Graceful shutdown (SIGINT/SIGTERM best-effort, Windows-safe)

Notes
-----
- Optional integrations degrade gracefully unless HOTLIST_FAST_FAIL=1 or MAIN_FAST_FAIL=1.
- Logging avoids f-strings inside logger.* calls (Ruff logging-format rules).
- Phase 2 compliance: NO logger monkey-patching / method assignment.
"""

import asyncio
import contextlib
import importlib
import inspect
import json
import logging
import os
import random
import signal
import time
from dataclasses import asdict, dataclass
from typing import Any, Awaitable, Callable, Protocol, cast

# -----------------------------------------------------------------------------
# Optional deps
# -----------------------------------------------------------------------------
try:
    import aiohttp  # type: ignore[import-not-found]
except Exception:  # pragma: no cover
    aiohttp = None  # type: ignore[assignment]


# -----------------------------------------------------------------------------
# Logger compatibility (Phase A)
# -----------------------------------------------------------------------------
class LoggerLike(Protocol):
    handlers: list[Any]

    def debug(self, msg: str, *args: Any, **kwargs: Any) -> None: ...
    def info(self, msg: str, *args: Any, **kwargs: Any) -> None: ...
    def warning(self, msg: str, *args: Any, **kwargs: Any) -> None: ...
    def error(self, msg: str, *args: Any, **kwargs: Any) -> None: ...
    def exception(self, msg: str, *args: Any, **kwargs: Any) -> None: ...
    def log(self, level: int, msg: str, *args: Any, **kwargs: Any) -> None: ...


def _resolve_get_logger() -> Callable[..., Any]:
    """
    Resolve a project get_logger() without creating hard typing edges across modules.
    Order:
      1) utils_pkg.logging_setup.get_logger
      2) logging_setup.get_logger
      3) stdlib logging.getLogger fallback
    """
    for modname in ("utils_pkg.logging_setup", "logging_setup"):
        with contextlib.suppress(Exception):
            mod = importlib.import_module(modname)
            fn = getattr(mod, "get_logger", None)
            if callable(fn):
                return cast(Callable[..., Any], fn)
    return lambda name=None, **_kw: logging.getLogger(name or "hotlist_worker")


def _get_logger(name: str) -> LoggerLike:
    fn = _resolve_get_logger()
    lg: Any
    try:
        lg = fn(name)
    except TypeError:
        try:
            lg = fn(name=name)
        except TypeError:
            lg = fn()

    # Ensure there's at least a basic handler for local runs if none configured.
    try:
        if not getattr(lg, "handlers", None):
            logging.basicConfig(
                level=os.getenv("LOG_LEVEL", "INFO").upper(),
                format="%(asctime)s | %(levelname)s | %(name)s: %(message)s",
            )
    except Exception:
        pass

    return cast(LoggerLike, lg)


LOG: LoggerLike = _get_logger("hotlist_worker")

# Once/every shims (no attribute assignment)
_ONCE_SEEN: dict[str, float] = {}


def log_event(name: str, **fields: Any) -> None:
    with contextlib.suppress(Exception):
        LOG.info("%s %s", name, fields)


def log_once(
    event: str,
    *,
    ttl: float = 30.0,
    level: int = logging.INFO,
    msg: str | None = None,
    **fields: Any,
) -> bool:
    now = time.monotonic()
    last = float(_ONCE_SEEN.get(event, 0.0))
    if now - last < float(ttl):
        return False
    _ONCE_SEEN[event] = now
    with contextlib.suppress(Exception):
        LOG.log(int(level), "%s %s", (msg or event), fields)
    return True


def log_every(
    event: str,
    *,
    n: float = 10.0,
    level: int = logging.INFO,
    msg: str | None = None,
    **fields: Any,
) -> None:
    log_once(event, ttl=float(n), level=int(level), msg=(msg or event), **fields)


def _short(x: Any, n: int = 240) -> str:
    try:
        s = str(x)
        return s if len(s) <= n else (s[:n] + "…")
    except Exception:
        return "<unprintable>"


# -----------------------------------------------------------------------------
# Optional integrations (kept compatible but not required)
# -----------------------------------------------------------------------------
InitResourcesFn = Callable[[], Awaitable[Any]]
CloseResourcesFn = Callable[[], Awaitable[Any]]
GetRedisAsyncFn = Callable[[], Awaitable[Any]]
SendTelegramFn = Callable[[str], Awaitable[Any]]
ConvertSolToUsdFn = Callable[[float], Awaitable[Any]]

# swap_token_jupiter has historical signature differences; treat as callable
SwapJupFn = Callable[..., Awaitable[Any]]

init_resources: InitResourcesFn | None = None
close_resources: CloseResourcesFn | None = None
get_redis_async: GetRedisAsyncFn | None = None
send_telegram: SendTelegramFn | None = None
convert_sol_to_usd: ConvertSolToUsdFn | None = None
swap_token_jupiter: SwapJupFn | None = None


def _load_core_utils() -> None:
    global \
        init_resources, \
        close_resources, \
        get_redis_async, \
        send_telegram, \
        convert_sol_to_usd, \
        swap_token_jupiter
    with contextlib.suppress(Exception):
        mod = importlib.import_module("utils_pkg.core_utils")
        init_resources = cast(InitResourcesFn | None, getattr(mod, "init_resources", None))
        close_resources = cast(CloseResourcesFn | None, getattr(mod, "close_resources", None))
        get_redis_async = cast(GetRedisAsyncFn | None, getattr(mod, "get_redis_async", None))
        send_telegram = cast(SendTelegramFn | None, getattr(mod, "send_telegram", None))
        convert_sol_to_usd = cast(
            ConvertSolToUsdFn | None, getattr(mod, "convert_sol_to_usd", None)
        )
        swap_token_jupiter = cast(SwapJupFn | None, getattr(mod, "swap_token_jupiter", None))


_load_core_utils()

# Risk manager (prefer utils_pkg.*; fallback to legacy module name)
CurrentLimitsFn = Callable[[Any], Awaitable[Any] | Any]
ShouldAllowFn = Callable[[Any], Awaitable[Any] | Any]
NoteSpendFn = Callable[[Any, float], Awaitable[Any] | Any]

current_limits: CurrentLimitsFn | None = None
should_allow_new_entry: ShouldAllowFn | None = None
note_spend: NoteSpendFn | None = None


def _load_risk_manager() -> None:
    global current_limits, should_allow_new_entry, note_spend
    for modname in ("utils_pkg.risk_manager", "risk_manager"):
        with contextlib.suppress(Exception):
            mod = importlib.import_module(modname)
            current_limits = cast(CurrentLimitsFn | None, getattr(mod, "current_limits", None))
            should_allow_new_entry = cast(
                ShouldAllowFn | None, getattr(mod, "should_allow_new_entry", None)
            )
            note_spend = cast(NoteSpendFn | None, getattr(mod, "note_spend", None))
            return


_load_risk_manager()

# Route guard (prefer utils_pkg.routeguard; fallback aliases)
PrecheckBuyFn = Callable[..., Awaitable[Any] | Any]
PosttradeWatchdogFn = Callable[..., Awaitable[Any] | Any]

precheck_buy: PrecheckBuyFn | None = None
posttrade_watchdog: PosttradeWatchdogFn | None = None


def _load_routeguard() -> None:
    global precheck_buy, posttrade_watchdog
    for modname in ("utils_pkg.routeguard", "route_guard"):
        with contextlib.suppress(Exception):
            mod = importlib.import_module(modname)
            precheck_buy = cast(PrecheckBuyFn | None, getattr(mod, "precheck_buy", None))
            posttrade_watchdog = cast(
                PosttradeWatchdogFn | None, getattr(mod, "posttrade_watchdog", None)
            )
            return


_load_routeguard()

# Quote cache prewarm (optional)
PrewarmQuotesFn = Callable[..., Awaitable[Any] | Any]
prewarm_quotes: PrewarmQuotesFn | None = None


def _load_prewarm() -> None:
    global prewarm_quotes
    for modname in ("utils_pkg.jup_quote_cache", "jup_quote_cache"):
        with contextlib.suppress(Exception):
            mod = importlib.import_module(modname)
            prewarm_quotes = cast(PrewarmQuotesFn | None, getattr(mod, "prewarm_quotes", None))
            return


_load_prewarm()


# -----------------------------------------------------------------------------
# Env helpers
# -----------------------------------------------------------------------------
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


def _i(name: str, default: int) -> int:
    v = os.getenv(name)
    try:
        if v is None or str(v).strip() == "":
            return int(default)
        return int(float(str(v).strip()))
    except Exception:
        return int(default)


def _f(name: str, default: float) -> float:
    v = os.getenv(name)
    try:
        if v is None or str(v).strip() == "":
            return float(default)
        return float(str(v).strip())
    except Exception:
        return float(default)


def _s(name: str, default: str = "") -> str:
    v = os.getenv(name)
    return default if v is None else str(v).strip()


def _clamp(x: float, lo: float, hi: float) -> float:
    if x < lo:
        return lo
    if x > hi:
        return hi
    return x


def _fast_fail() -> bool:
    return _b("HOTLIST_FAST_FAIL", _b("MAIN_FAST_FAIL", False))


def _disabled() -> bool:
    return bool(
        _b("KILL_SWITCH", False)
        or _b("DISABLE_WORKERS", False)
        or _b("DISABLE_SCANNERS", False)
        or _b("HOTLIST_DISABLED", False)
        or not _b("HOTLIST_ENABLED", True)
    )


# -----------------------------------------------------------------------------
# Settings
# -----------------------------------------------------------------------------
SOL_MINT = "So11111111111111111111111111111111111111112"


@dataclass(frozen=True)
class Settings:
    enabled: bool
    mode: str  # QUEUE | TRADE

    dex_search_api: str
    poll_sec: float
    jitter_sec: float

    cooldown_sec: int
    lock_ttl_sec: int

    min_score: float
    min_liq_usd: float
    min_vol24_usd: float
    min_buys_5m: int
    max_impact_bps: float

    default_ticket_usd: float

    redis_queue_key: str
    redis_cooldown_prefix: str
    redis_lock_prefix: str

    prewarm_enabled: bool
    prewarm_top_n: int

    tg_on_start: bool
    tg_on_candidate: bool
    tg_on_buy: bool

    http_timeout_s: float
    http_retries: int
    http_backoff_s: float
    rps_limit: float


def _compute_settings() -> Settings:
    mode = _s("HOTLIST_MODE", "QUEUE").upper()
    if mode not in {"QUEUE", "TRADE"}:
        mode = "QUEUE"

    enabled = (not _disabled()) and _b("HOTLIST_ENABLED", True)

    dex_api = _s(
        "HOTLIST_DEX_SEARCH_API",
        "https://api.dexscreener.com/latest/dex/search?q=sol",
    ).strip()

    poll_sec = max(1.0, float(_f("HOTLIST_POLL_SEC", 30.0)))
    jitter_sec = max(0.0, float(_f("HOTLIST_JITTER_SEC", 1.25)))

    cooldown_sec = max(0, _i("HOTLIST_COOLDOWN_SEC", 300))
    lock_ttl_sec = max(5, _i("HOTLIST_LOCK_TTL_SEC", 120))

    min_score = _clamp(float(_f("HOTLIST_SCORE_MIN", 0.68)), 0.0, 1.0)

    ai_min_liq = float(_f("AI_MIN_LIQ_USD", 20_000.0))
    min_liq_usd = max(0.0, float(_f("HOTLIST_MIN_LIQ_USD", ai_min_liq)))

    min_vol24_usd = max(0.0, float(_f("HOTLIST_MIN_VOL24_USD", 0.0)))
    min_buys_5m = max(0, _i("HOTLIST_MIN_BUYS_5M", 0))

    ai_max_impact = float(_f("AI_MAX_IMPACT_BPS", 150.0))
    max_impact_bps = max(0.0, float(_f("HOTLIST_MAX_IMPACT_BPS", ai_max_impact)))

    default_ticket_usd = max(0.5, float(_f("HOTLIST_TICKET_USD", 15.0)))

    redis_queue_key = _s("HOTLIST_QUEUE_KEY", "entry_queue:hotlist")
    redis_cooldown_prefix = _s("HOTLIST_COOLDOWN_KEY_PREFIX", "cd:hotlist:")
    redis_lock_prefix = _s("HOTLIST_LOCK_KEY_PREFIX", "lock:hotlist:")

    prewarm_enabled = _b("HOTLIST_PREWARM_ENABLED", True)
    prewarm_top_n = max(0, _i("HOTLIST_PREWARM_TOP_N", 8))

    tg_on_start = _b("HOTLIST_TELEGRAM_ON_START", True)
    tg_on_candidate = _b("HOTLIST_TELEGRAM_ON_CANDIDATE", False)
    tg_on_buy = _b("HOTLIST_TELEGRAM_ON_BUY", True)

    http_timeout_s = max(5.0, float(_f("HOTLIST_HTTP_TIMEOUT_SEC", 15.0)))
    http_retries = max(1, _i("HOTLIST_HTTP_RETRIES", 3))
    http_backoff_s = max(0.15, float(_f("HOTLIST_HTTP_BACKOFF_SEC", 0.6)))
    rps_limit = max(0.0, float(_f("HOTLIST_RPS_LIMIT", 0.0)))

    return Settings(
        enabled=enabled,
        mode=mode,
        dex_search_api=dex_api,
        poll_sec=poll_sec,
        jitter_sec=jitter_sec,
        cooldown_sec=cooldown_sec,
        lock_ttl_sec=lock_ttl_sec,
        min_score=min_score,
        min_liq_usd=min_liq_usd,
        min_vol24_usd=min_vol24_usd,
        min_buys_5m=min_buys_5m,
        max_impact_bps=max_impact_bps,
        default_ticket_usd=default_ticket_usd,
        redis_queue_key=redis_queue_key,
        redis_cooldown_prefix=redis_cooldown_prefix,
        redis_lock_prefix=redis_lock_prefix,
        prewarm_enabled=prewarm_enabled,
        prewarm_top_n=prewarm_top_n,
        tg_on_start=tg_on_start,
        tg_on_candidate=tg_on_candidate,
        tg_on_buy=tg_on_buy,
        http_timeout_s=http_timeout_s,
        http_retries=http_retries,
        http_backoff_s=http_backoff_s,
        rps_limit=rps_limit,
    )


# -----------------------------------------------------------------------------
# Data model (keep compatible payload fields)
# -----------------------------------------------------------------------------
@dataclass(frozen=True)
class HotCandidate:
    ts: float
    source: str
    mint: str
    symbol: str
    pair_url: str

    liquidity_usd: float
    volume_h24_usd: float
    price_change_m5: float
    buys_m5: int
    sells_m5: int

    score: float
    est_impact_bps: float

    raw: dict[str, Any]


# -----------------------------------------------------------------------------
# Utilities
# -----------------------------------------------------------------------------
_mem_cooldown: dict[str, int] = {}


async def _maybe_await(x: Any) -> Any:
    if inspect.isawaitable(x):
        return await x
    return x


async def _tg(msg: str) -> None:
    if send_telegram is None:
        LOG.info("[telegram disabled] %s", msg)
        return
    try:
        await send_telegram(msg)
    except asyncio.CancelledError:
        raise
    except Exception:
        log_every("HOTLIST_TG_FAIL", n=30, level=logging.INFO, msg="HOTLIST_TG_FAIL")


def _safe_float(x: Any, default: float = 0.0) -> float:
    try:
        return float(x)
    except Exception:
        return default


def _safe_int(x: Any, default: int = 0) -> int:
    try:
        return int(x)
    except Exception:
        return default


def _impact_bps(liq_usd: float, ticket_usd: float) -> float:
    if liq_usd <= 0:
        return 9e9
    return (ticket_usd / liq_usd) * 10_000.0


def _compute_score(p: dict[str, Any]) -> float:
    """
    Score in [0, 1].
    Prefer Dexscreener's hotScore if present; else compute a robust composite.
    """
    hs = p.get("hotScore")
    if isinstance(hs, (int, float)):
        return _clamp(float(hs), 0.0, 1.0)

    liq = _safe_float((p.get("liquidity") or {}).get("usd", 0.0))
    vol24 = _safe_float((p.get("volume") or {}).get("h24", 0.0))
    pc5 = _safe_float((p.get("priceChange") or {}).get("m5", 0.0))
    tx5 = (p.get("txns") or {}).get("m5") or {}
    buys5 = _safe_int(tx5.get("buys", 0))
    sells5 = _safe_int(tx5.get("sells", 0))

    liq_term = min(1.0, liq / 250_000.0)  # ~250k liq -> 1.0
    vol_term = min(1.0, vol24 / 1_000_000.0)  # ~1m vol -> 1.0
    act_term = min(1.0, (buys5 + sells5) / 40.0)  # ~40 tx/5m -> 1.0

    if pc5 >= 0:
        momo_term = min(1.0, pc5 / 10.0)  # +10% / 5m -> 1.0
    else:
        momo_term = max(0.0, 1.0 + (pc5 / 20.0))  # -20% -> 0.0

    denom = max(1, buys5 + sells5)
    imb = (buys5 - sells5) / denom  # -1..+1
    imb_term = (imb + 1.0) / 2.0  # 0..1

    score = 0.28 * liq_term + 0.25 * vol_term + 0.20 * act_term + 0.20 * momo_term + 0.07 * imb_term
    return _clamp(float(score), 0.0, 1.0)


async def _redis() -> Any:
    if get_redis_async is None:
        return None
    try:
        return await get_redis_async()
    except asyncio.CancelledError:
        raise
    except Exception:
        log_every("HOTLIST_REDIS_GET_FAIL", n=30, level=logging.INFO, msg="HOTLIST_REDIS_GET_FAIL")
        return None


async def _redis_set_nx_ex(r: Any, key: str, value: str, ex_s: int) -> bool:
    """
    Best-effort SET NX EX across common Redis clients.
    Returns True if lock/cooldown acquired.
    """
    if r is None:
        return True
    try:
        fn = getattr(r, "set", None)
        if not callable(fn):
            return True

        # redis-py: set(key, value, nx=True, ex=seconds)
        try:
            out = fn(key, value, nx=True, ex=int(ex_s))
            out = await _maybe_await(out)
            return bool(out)
        except TypeError:
            pass

        # Some clients: set(name=..., value=..., ex=..., nx=...)
        try:
            out = fn(name=key, value=value, nx=True, ex=int(ex_s))
            out = await _maybe_await(out)
            return bool(out)
        except TypeError:
            pass

        # Fallback: set + expire (racey but best-effort)
        await _maybe_await(fn(key, value))
        exp = getattr(r, "expire", None)
        if callable(exp):
            await _maybe_await(exp(key, int(ex_s)))
        return True
    except asyncio.CancelledError:
        raise
    except Exception:
        return True


async def _redis_delete(r: Any, key: str) -> None:
    if r is None:
        return
    try:
        fn = getattr(r, "delete", None)
        if callable(fn):
            await _maybe_await(fn(key))
    except asyncio.CancelledError:
        raise
    except Exception:
        return


async def _cooldown_ok(st: Settings, r: Any, mint: str) -> bool:
    now = int(time.time())
    if st.cooldown_sec <= 0:
        return True

    if r is None:
        last = int(_mem_cooldown.get(mint, 0))
        if now - last >= st.cooldown_sec:
            _mem_cooldown[mint] = now
            return True
        return False

    key = "%s%s" % (st.redis_cooldown_prefix, mint)
    return await _redis_set_nx_ex(r, key, str(now), st.cooldown_sec)


async def _try_lock(st: Settings, r: Any, mint: str) -> bool:
    if st.lock_ttl_sec <= 0:
        return True
    if r is None:
        return True
    key = "%s%s" % (st.redis_lock_prefix, mint)
    return await _redis_set_nx_ex(r, key, "1", st.lock_ttl_sec)


async def _unlock(st: Settings, r: Any, mint: str) -> None:
    if r is None:
        return
    key = "%s%s" % (st.redis_lock_prefix, mint)
    await _redis_delete(r, key)


async def _usd_per_sol() -> float:
    if convert_sol_to_usd is None:
        return 0.0
    try:
        v = await convert_sol_to_usd(1.0)
        return float(v or 0.0)
    except asyncio.CancelledError:
        raise
    except Exception:
        return 0.0


async def _get_ticket_usd(r: Any, st: Settings) -> float:
    """
    Ticket sizing is owned by risk_manager when present; otherwise fallback to env.
    """
    if current_limits is None:
        return float(st.default_ticket_usd)
    try:
        lim_obj = current_limits(r)
        lim = await _maybe_await(lim_obj)
        if isinstance(lim, dict):
            t = float(lim.get("ticket_usd", st.default_ticket_usd))
        else:
            t = float(st.default_ticket_usd)
        return _clamp(float(t), 0.5, 500.0)
    except asyncio.CancelledError:
        raise
    except Exception:
        return float(st.default_ticket_usd)


async def _allow_new_entry(r: Any) -> bool:
    if should_allow_new_entry is None:
        return True
    try:
        out = should_allow_new_entry(r)
        out2 = await _maybe_await(out)
        return bool(out2)
    except asyncio.CancelledError:
        raise
    except Exception:
        return True


async def _maybe_note_spend(r: Any, usd: float) -> None:
    if note_spend is None:
        return
    try:
        out = note_spend(r, float(usd))
        await _maybe_await(out)
    except asyncio.CancelledError:
        raise
    except Exception:
        log_every("HOTLIST_NOTE_SPEND_FAIL", n=30, level=logging.INFO, msg="note_spend failed")


async def _publish_candidate(st: Settings, r: Any, c: HotCandidate) -> None:
    payload = json.dumps(asdict(c), separators=(",", ":"), ensure_ascii=False)
    if r is None:
        LOG.info("[queue-fallback] %s", payload)
        return
    try:
        fn = getattr(r, "rpush", None)
        if not callable(fn):
            LOG.info("[queue-fallback] %s", payload)
            return
        await _maybe_await(fn(st.redis_queue_key, payload))
    except asyncio.CancelledError:
        raise
    except Exception:
        log_every(
            "HOTLIST_PUBLISH_FAIL",
            n=10,
            level=logging.ERROR,
            msg="HOTLIST_PUBLISH_FAIL",
            key=st.redis_queue_key,
        )


class _RpsGate:
    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._last = 0.0
        self._rps = 0.0

    def set(self, rps: float) -> None:
        self._rps = float(max(0.0, rps))

    async def wait(self) -> None:
        rps = float(self._rps)
        if rps <= 0:
            return
        async with self._lock:
            now = time.monotonic()
            min_dt = 1.0 / max(0.0001, rps)
            dt = now - self._last
            if dt < min_dt:
                await asyncio.sleep(min_dt - dt)
            self._last = time.monotonic()


_RPS = _RpsGate()


async def _fetch_pairs_http(st: Settings, http: Any) -> list[dict[str, Any]]:
    """
    Fetch Dexscreener pairs. Retries/backoff; returns [] on failure.
    """
    url = st.dex_search_api
    last_status = 0
    last_first = ""
    for attempt in range(1, st.http_retries + 1):
        await _RPS.wait()
        try:
            timeout = None
            if aiohttp is not None:
                timeout = aiohttp.ClientTimeout(total=float(st.http_timeout_s))  # type: ignore[attr-defined]

            async with http.get(url, timeout=timeout) as resp:
                last_status = int(getattr(resp, "status", 0) or 0)
                text = (await resp.text()) or ""
                last_first = text[:180]

                if last_status != 200:
                    # non-retryable 4xx (except 429)
                    if 400 <= last_status < 500 and last_status != 429:
                        break
                    await asyncio.sleep(
                        st.http_backoff_s * attempt * (0.85 + 0.3 * random.random())
                    )
                    continue

                data = await resp.json()
                pairs = (data or {}).get("pairs", []) or []
                if not isinstance(pairs, list):
                    return []
                return [p for p in pairs if isinstance(p, dict)]
        except asyncio.CancelledError:
            raise
        except Exception as e:
            log_every(
                "HOTLIST_FETCH_ERR",
                n=10,
                level=logging.INFO,
                msg="HOTLIST_FETCH_ERR",
                err=type(e).__name__,
                attempt=attempt,
            )
            if attempt < st.http_retries:
                await asyncio.sleep(st.http_backoff_s * attempt * (0.85 + 0.3 * random.random()))

    log_every(
        "HOTLIST_FETCH_FAIL",
        n=10,
        level=logging.WARNING,
        msg="HOTLIST_FETCH_FAIL",
        status=last_status,
        first=last_first,
    )
    return []


async def _maybe_prewarm(st: Settings, pairs: list[dict[str, Any]], ticket_usd: float) -> None:
    if not st.prewarm_enabled or prewarm_quotes is None or not pairs or st.prewarm_top_n <= 0:
        return

    usd_per_sol = await _usd_per_sol()
    if usd_per_sol <= 0:
        return
    amount_sol = float(ticket_usd) / float(usd_per_sol)

    def sort_key(p: dict[str, Any]) -> float:
        hs = p.get("hotScore")
        if isinstance(hs, (int, float)):
            return float(hs)
        return _safe_float((p.get("liquidity") or {}).get("usd", 0.0))

    top = sorted(pairs, key=sort_key, reverse=True)[: max(1, st.prewarm_top_n)]
    mints: list[str] = []
    for p in top:
        mint = (p.get("baseToken") or {}).get("address")
        if mint:
            mints.append(str(mint))

    if not mints:
        return

    jup_max_slip = float(_f("JUPITER_MAX_SLIPPAGE", 0.005))
    slippage_bps = max(1, int(jup_max_slip * 10_000))

    try:
        out = prewarm_quotes(mints, float(amount_sol), int(slippage_bps))
        await _maybe_await(out)
        log_every(
            "HOTLIST_PREWARM_OK",
            n=20,
            level=logging.INFO,
            msg="HOTLIST_PREWARM_OK",
            n_mints=len(mints),
            amount_sol=round(float(amount_sol), 6),
            slippage_bps=int(slippage_bps),
        )
    except asyncio.CancelledError:
        raise
    except Exception:
        log_every("HOTLIST_PREWARM_FAIL", n=30, level=logging.INFO, msg="HOTLIST_PREWARM_FAIL")


# -----------------------------------------------------------------------------
# Main loop
# -----------------------------------------------------------------------------
async def run_hotlist(stop_event: asyncio.Event) -> None:
    st = _compute_settings()
    _RPS.set(st.rps_limit)

    if not st.enabled:
        log_once("HOTLIST_DISABLED", ttl=60, msg="Hotlist disabled; idling")
        while not stop_event.is_set():
            await asyncio.sleep(5)
        return

    if aiohttp is None:
        if _fast_fail():
            raise RuntimeError("aiohttp is required for hotlist_worker but is not installed")
        log_once("HOTLIST_NO_AIOHTTP", ttl=60, msg="aiohttp missing; hotlist disabled")
        while not stop_event.is_set():
            await asyncio.sleep(5)
        return

    if st.tg_on_start:
        await _tg(
            "🔥 Hotlist worker started\n"
            "• mode=%s\n"
            "• poll=%ss cooldown=%ss lock_ttl=%ss\n"
            "• gates: score>=%s liq>=$%s impact<=%sbps"
            % (
                st.mode,
                int(st.poll_sec),
                int(st.cooldown_sec),
                int(st.lock_ttl_sec),
                f"{st.min_score:.2f}",
                f"{st.min_liq_usd:,.0f}",
                f"{st.max_impact_bps:.0f}",
            )
        )

    r = await _redis()

    timeout = aiohttp.ClientTimeout(total=float(st.http_timeout_s))  # type: ignore[attr-defined]
    async with aiohttp.ClientSession(timeout=timeout) as http:  # type: ignore[attr-defined]
        while not stop_event.is_set():
            t0 = time.time()
            emitted = 0
            pairs_count = 0

            try:
                if not await _allow_new_entry(r):
                    await asyncio.sleep(float(st.poll_sec))
                    continue

                ticket_usd = await _get_ticket_usd(r, st)
                pairs = await _fetch_pairs_http(st, http)
                pairs_count = len(pairs)

                await _maybe_prewarm(st, pairs, ticket_usd)

                for p in pairs:
                    if stop_event.is_set():
                        break

                    base = p.get("baseToken") or {}
                    mint = base.get("address")
                    symbol = base.get("symbol") or base.get("name") or "UNK"
                    if not mint:
                        continue

                    mint_s = str(mint).strip()
                    symbol_s = str(symbol).strip() or "UNK"

                    if not await _cooldown_ok(st, r, mint_s):
                        continue

                    if not await _try_lock(st, r, mint_s):
                        continue

                    try:
                        liq = _safe_float((p.get("liquidity") or {}).get("usd", 0.0))
                        if liq < st.min_liq_usd:
                            continue

                        vol24 = _safe_float((p.get("volume") or {}).get("h24", 0.0))
                        if vol24 < st.min_vol24_usd:
                            continue

                        tx5 = (p.get("txns") or {}).get("m5") or {}
                        buys5 = _safe_int(tx5.get("buys", 0))
                        sells5 = _safe_int(tx5.get("sells", 0))
                        if buys5 < st.min_buys_5m:
                            continue

                        score = _compute_score(p)
                        if score < st.min_score:
                            continue

                        impact = _impact_bps(liq, ticket_usd)
                        if impact > st.max_impact_bps:
                            continue

                        pair_url = str(p.get("url") or "")

                        cand = HotCandidate(
                            ts=time.time(),
                            source="dexscreener_search",
                            mint=mint_s,
                            symbol=symbol_s,
                            pair_url=pair_url,
                            liquidity_usd=float(liq),
                            volume_h24_usd=float(vol24),
                            price_change_m5=_safe_float(
                                (p.get("priceChange") or {}).get("m5", 0.0)
                            ),
                            buys_m5=int(buys5),
                            sells_m5=int(sells5),
                            score=float(score),
                            est_impact_bps=float(impact),
                            raw={"pair": p},
                        )

                        if st.mode == "QUEUE":
                            await _publish_candidate(st, r, cand)
                            emitted += 1

                            LOG.info(
                                "[QUEUE] %s %s score=%.2f liq=$%.0f vol24=$%.0f buys5=%d impact=%.0fbps",
                                cand.symbol,
                                cand.mint,
                                cand.score,
                                cand.liquidity_usd,
                                cand.volume_h24_usd,
                                cand.buys_m5,
                                cand.est_impact_bps,
                            )

                            if st.tg_on_candidate:
                                await _tg(
                                    "🧲 Hotlist candidate queued\n"
                                    "• %s (%s)\n"
                                    "• score=%.2f liq=$%s vol24=$%s buys5=%d\n"
                                    "• impact≈%.0f bps\n"
                                    "• %s"
                                    % (
                                        cand.symbol,
                                        cand.mint,
                                        cand.score,
                                        f"{cand.liquidity_usd:,.0f}",
                                        f"{cand.volume_h24_usd:,.0f}",
                                        int(cand.buys_m5),
                                        float(cand.est_impact_bps),
                                        cand.pair_url,
                                    )
                                )
                            continue

                        # TRADE mode (explicit). Uses Jupiter swap + optional route guard.
                        if st.mode == "TRADE":
                            if swap_token_jupiter is None:
                                log_once(
                                    "HOTLIST_NO_SWAP",
                                    ttl=60,
                                    msg="TRADE mode requested but swap_token_jupiter unavailable",
                                )
                                continue

                            usd_per_sol = await _usd_per_sol()
                            if usd_per_sol <= 0:
                                log_every(
                                    "HOTLIST_NO_SOLUSD",
                                    n=30,
                                    level=logging.INFO,
                                    msg="Cannot resolve SOL/USD; skipping TRADE",
                                )
                                continue

                            amount_sol = float(ticket_usd) / float(usd_per_sol)

                            jup_max_slip = float(_f("JUPITER_MAX_SLIPPAGE", 0.005))
                            slippage_bps = max(1, int(jup_max_slip * 10_000))

                            pre_meta: dict[str, Any] = {}

                            if precheck_buy is not None:
                                try:
                                    out_rg = precheck_buy(
                                        mint_s, float(amount_sol), int(slippage_bps)
                                    )
                                    res = await _maybe_await(out_rg)
                                    if isinstance(res, tuple) and len(res) >= 3:
                                        ok_rg = bool(res[0])
                                        why = str(res[1])
                                        meta = res[2]
                                        pre_meta = (
                                            dict(meta or {}) if isinstance(meta, dict) else {}
                                        )
                                        if not ok_rg:
                                            LOG.info(
                                                "[BLOCK] routeguard mint=%s why=%s impact=%.0fbps slip_bps=%d",
                                                mint_s,
                                                why,
                                                float(impact),
                                                int(slippage_bps),
                                            )
                                            await _tg(
                                                "⛔ Hotlist RouteGuard blocked\n"
                                                "• %s (%s)\n"
                                                "• why=%s\n"
                                                "• impact≈%.0fbps slippage_bps=%d"
                                                % (
                                                    symbol_s,
                                                    mint_s,
                                                    why,
                                                    float(impact),
                                                    int(slippage_bps),
                                                )
                                            )
                                            continue
                                except asyncio.CancelledError:
                                    raise
                                except Exception:
                                    log_every(
                                        "HOTLIST_RG_PRECHECK_FAIL",
                                        n=30,
                                        level=logging.INFO,
                                        msg="routeguard precheck failed",
                                    )

                            ok = False
                            sig: Any = None
                            try:
                                # Support historical Jupiter swap signatures.
                                try:
                                    res = await swap_token_jupiter(
                                        SOL_MINT,
                                        mint_s,
                                        amount_float=float(amount_sol),
                                        buy=True,
                                        slippage=float(jup_max_slip),
                                    )
                                except TypeError:
                                    res = await swap_token_jupiter(
                                        SOL_MINT,
                                        mint_s,
                                        float(amount_sol),
                                        True,
                                        float(jup_max_slip),
                                    )

                                if isinstance(res, tuple) and len(res) >= 2:
                                    ok = bool(res[0])
                                    sig = res[1]
                                else:
                                    ok = bool(res)
                            except asyncio.CancelledError:
                                raise
                            except Exception as e:
                                log_every(
                                    "HOTLIST_SWAP_FAIL",
                                    n=10,
                                    level=logging.ERROR,
                                    msg="swap_token_jupiter failed",
                                    err=_short(e),
                                )
                                ok = False

                            if not ok:
                                continue

                            await _maybe_note_spend(r, float(ticket_usd))
                            emitted += 1

                            if st.tg_on_buy:
                                await _tg(
                                    "🔥 BUY (hotlist)\n"
                                    "• %s (%s)\n"
                                    "• ticket=$%s amount≈%.6f SOL\n"
                                    "• liq=$%s score=%.2f impact≈%.0fbps\n"
                                    "• tx=%s"
                                    % (
                                        symbol_s,
                                        mint_s,
                                        f"{ticket_usd:,.2f}",
                                        float(amount_sol),
                                        f"{liq:,.0f}",
                                        float(score),
                                        float(impact),
                                        str(sig),
                                    )
                                )

                            if posttrade_watchdog is not None:
                                try:
                                    out_wd = posttrade_watchdog(
                                        mint_s, float(amount_sol), int(slippage_bps), pre_meta
                                    )
                                    res2 = await _maybe_await(out_wd)
                                    if isinstance(res2, tuple) and len(res2) >= 3:
                                        ok_wd = bool(res2[0])
                                        msg_wd = str(res2[1])
                                        now_meta = res2[2]
                                        if not ok_wd:
                                            now_d = now_meta if isinstance(now_meta, dict) else {}
                                            await _tg(
                                                "⚠️ Route watchdog\n"
                                                "• %s (%s)\n"
                                                "• %s\n"
                                                "• pre=%s now=%s"
                                                % (
                                                    symbol_s,
                                                    mint_s,
                                                    msg_wd,
                                                    str(pre_meta.get("out_amount")),
                                                    str(now_d.get("out_amount")),
                                                )
                                            )
                                except asyncio.CancelledError:
                                    raise
                                except Exception:
                                    log_every(
                                        "HOTLIST_WD_FAIL",
                                        n=30,
                                        level=logging.INFO,
                                        msg="posttrade watchdog failed",
                                    )

                    finally:
                        await _unlock(st, r, mint_s)

            except asyncio.CancelledError:
                raise
            except Exception as e:
                log_every(
                    "HOTLIST_LOOP_ERR",
                    n=10,
                    level=logging.ERROR,
                    msg="HOTLIST_LOOP_ERR",
                    err=_short(e),
                )

            dt = time.time() - t0
            LOG.info(
                "[tick] pairs=%d emitted=%d dt=%.2fs mode=%s",
                int(pairs_count),
                int(emitted),
                float(dt),
                st.mode,
            )

            sleep_for = float(st.poll_sec) + random.uniform(0.0, float(st.jitter_sec))
            with contextlib.suppress(asyncio.TimeoutError):
                await asyncio.wait_for(stop_event.wait(), timeout=max(0.25, sleep_for))


# -----------------------------------------------------------------------------
# Entrypoint / signals
# -----------------------------------------------------------------------------
async def main() -> None:
    if init_resources is not None:
        try:
            await init_resources()
        except asyncio.CancelledError:
            raise
        except Exception:
            log_every(
                "HOTLIST_INIT_RES_FAIL", n=30, level=logging.INFO, msg="init_resources failed"
            )

    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()

    def _set_stop(*_: Any) -> None:
        stop_event.set()

    for sig_name in ("SIGINT", "SIGTERM"):
        sig_obj = getattr(signal, sig_name, None)
        if sig_obj is None:
            continue
        with contextlib.suppress(NotImplementedError):
            loop.add_signal_handler(sig_obj, _set_stop)

    try:
        await run_hotlist(stop_event)
    finally:
        if close_resources is not None:
            try:
                await close_resources()
            except asyncio.CancelledError:
                raise
            except Exception:
                log_every(
                    "HOTLIST_CLOSE_RES_FAIL",
                    n=30,
                    level=logging.INFO,
                    msg="close_resources failed",
                )


def health_snapshot() -> dict[str, Any]:
    """Non-network health snapshot (safe for /health)."""
    st = _compute_settings()
    return {
        "name": "hotlist_worker",
        "enabled": bool(st.enabled),
        "mode": st.mode,
        "dex_search_api": st.dex_search_api,
        "poll_sec": st.poll_sec,
        "cooldown_sec": st.cooldown_sec,
        "lock_ttl_sec": st.lock_ttl_sec,
        "gates": {
            "min_score": st.min_score,
            "min_liq_usd": st.min_liq_usd,
            "min_vol24_usd": st.min_vol24_usd,
            "min_buys_5m": st.min_buys_5m,
            "max_impact_bps": st.max_impact_bps,
        },
        "queue": {"key": st.redis_queue_key},
        "prewarm_enabled": bool(st.prewarm_enabled),
        "rps_limit": st.rps_limit,
    }


__all__ = ["health_snapshot", "main", "run_hotlist"]


if __name__ == "__main__":  # pragma: no cover
    with contextlib.suppress(KeyboardInterrupt):
        asyncio.run(main())
