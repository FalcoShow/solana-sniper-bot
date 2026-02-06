from __future__ import annotations

"""
utils_pkg/honeypot.py

Canonical pre-buy safety / honeypot checks (V3+++ / production).
===============================================================

Role
----
Answers: "Is this mint likely sellable / not obviously trapped?"

Public API
----------
    ok, report = await honeypot_check(mint, force=False)
    ok = await is_token_safe(mint)

report is a stable dict designed for:
- logs / telemetry
- feature enrichment (FeatureBuilder)
- AI gating (ai_gate)
- route guards / entry logic

Design constraints (P0)
-----------------------
- No network/redis calls at import time
- Fast: concurrent checks with strict total timeout budget
- Robust: hard vs soft failures (do not dead-trade on transient API issues)
- Deterministic: on-chain mint flags (freeze/mint auth) when available
- Compatible: keep older exports referenced across the codebase
- Cancellation-safe: never swallow asyncio.CancelledError

Hard vs soft failures
---------------------
Hard failures block entry.
Soft failures annotate report but do not block entry unless strict mode requires.

Typical hard failures:
- Explicit blacklist
- Dexscreener indicates trading paused / honeypot flag
- On-chain freeze authority present (if required)
- Jupiter sell route definitively absent (if required)
- Rugcheck signals high-risk honeypot/trading disabled (if enabled + strict)

Soft failures:
- External API down / 429 / 5xx
- Missing optional sources (Birdeye key missing)
"""

import asyncio
import atexit
import os
import time
from dataclasses import dataclass
from types import ModuleType
from typing import Any, Awaitable, Callable, cast

# -----------------------------------------------------------------------------
# Canonical logging contract (EventLogger)
# -----------------------------------------------------------------------------
# IMPORTANT: Always use the canonical logger from utils_pkg.log_compat
# (logging_setup should re-export it, but we import canonical directly here
# to prevent another competing type contract).
from utils_pkg.log_compat import EventLogger, get_logger

# -----------------------------------------------------------------------------
# Optional deps
# -----------------------------------------------------------------------------
try:
    import aiohttp  # type: ignore[import-not-found]
except Exception:  # pragma: no cover
    aiohttp = None  # type: ignore[assignment]

# -----------------------------------------------------------------------------
# Optional shared app-wide resources (best-effort)
# -----------------------------------------------------------------------------
SHARED_SESSION: Any = None
SHARED_SOLANA_CLIENT: Any = None
try:
    from utils_pkg.core_utils import (  # type: ignore[import-not-found]
        session as _SHARED_SESSION,
        solana_client as _SHARED_SOLANA_CLIENT,
    )

    SHARED_SESSION = _SHARED_SESSION
    SHARED_SOLANA_CLIENT = _SHARED_SOLANA_CLIENT
except Exception:  # pragma: no cover
    SHARED_SESSION = None
    SHARED_SOLANA_CLIENT = None

# Canonical mint parsing + probe helpers
FetchMintInfoFn = Callable[[str], Awaitable[Any] | Any]
fetch_mint_info: FetchMintInfoFn | None
try:
    from utils_pkg.liquidity import (  # type: ignore[import-not-found]
        fetch_mint_info as _fetch_mint_info,
    )

    fetch_mint_info = _fetch_mint_info
except Exception:  # pragma: no cover
    fetch_mint_info = None


def _get_log() -> EventLogger:
    """
    Canonical module logger.
    Must return EventLogger (not stdlib Logger) to match the global contract.
    """
    try:
        return get_logger("honeypot", service="honeypot")
    except TypeError:
        # Older signature (name only) fallback (still returns EventLogger)
        return get_logger("honeypot")


log: EventLogger = _get_log()

_ONCE_SEEN: dict[str, float] = {}


def _short(x: Any, n: int = 240) -> str:
    try:
        s = str(x)
        return s if len(s) <= n else (s[:n] + "…")
    except Exception:
        return "<unprintable>"


def _emit(event: str, *, level: int, msg: str, fields: dict[str, Any]) -> None:
    """
    EventLogger-safe emit.

    We prefer structured `event()` (canonical). We never call stdlib Logger.log().
    Fallbacks are best-effort and must not raise.
    """
    lg = cast(Any, log)

    payload: dict[str, Any] = dict(fields)
    payload.setdefault("msg", msg)
    payload.setdefault("level", int(level))

    # Preferred: structured event
    try:
        lg.event(event, **payload)
        return
    except TypeError:
        # Some variants may not accept msg/level kwargs
        pass
    except Exception:
        return

    # Fallback: minimal structured event
    try:
        lg.event(event, **fields)
        return
    except Exception:
        pass

    # Last-resort: best-effort text methods if present
    try:
        if int(level) >= 40 and hasattr(lg, "error"):
            lg.error("%s %s", msg, fields)
        elif int(level) >= 30 and hasattr(lg, "warning"):
            lg.warning("%s %s", msg, fields)
        elif hasattr(lg, "info"):
            lg.info("%s %s", msg, fields)
    except Exception:
        pass


def log_once(
    event: str,
    *,
    ttl: float = 30.0,
    level: int = 20,
    msg: str | None = None,
    **fields: Any,
) -> bool:
    now = time.monotonic()
    last = float(_ONCE_SEEN.get(event, 0.0))
    if now - last < float(ttl):
        return False

    _ONCE_SEEN[event] = now
    _emit(event, level=int(level), msg=(msg or event), fields=fields)
    return True


def log_every(
    event: str,
    *,
    n: float = 10.0,
    level: int = 20,
    msg: str | None = None,
    **fields: Any,
) -> None:
    log_once(event, ttl=float(n), level=int(level), msg=(msg or event), **fields)


# -----------------------------------------------------------------------------
# Config / env
# -----------------------------------------------------------------------------
def _env_str(name: str, default: str = "") -> str:
    v = os.getenv(name)
    return default if v is None else str(v).strip()


def _env_int(name: str, default: int) -> int:
    v = os.getenv(name)
    try:
        if v is None or str(v).strip() == "":
            return int(default)
        return int(float(str(v).strip()))
    except Exception:
        return int(default)


def _env_float(name: str, default: float) -> float:
    v = os.getenv(name)
    try:
        if v is None or str(v).strip() == "":
            return float(default)
        return float(str(v).strip())
    except Exception:
        return float(default)


try:
    import config as C  # type: ignore
except Exception:  # pragma: no cover

    class _C:
        # Cache / timing
        HONEYPOT_CACHE_TTL_S = _env_float("HONEYPOT_CACHE_TTL_S", 180.0)
        HONEYPOT_HTTP_TIMEOUT_S = _env_float(
            "HONEYPOT_HTTP_TIMEOUT_S", _env_float("HONEYPOT_HTTP_TIMEOUT", 10.0)
        )
        HONEYPOT_TOTAL_BUDGET_S = _env_float("HONEYPOT_TOTAL_BUDGET_S", 12.0)

        # Policy thresholds
        HONEYPOT_MIN_LIQUIDITY_USD = _env_float("HONEYPOT_MIN_LIQUIDITY_USD", 3000.0)
        HONEYPOT_REQUIRE_JUP_SELL_ROUTE = _env_int("HONEYPOT_REQUIRE_JUP_SELL_ROUTE", 1)
        HONEYPOT_JUP_STRICT = _env_int("HONEYPOT_JUP_STRICT", 0)
        HONEYPOT_RUGCHECK_STRICT = _env_int("HONEYPOT_RUGCHECK_STRICT", 0)

        HONEYPOT_REQUIRE_NO_FREEZE = _env_int("HONEYPOT_REQUIRE_NO_FREEZE", 0)
        HONEYPOT_REQUIRE_MINT_AUTH_DISABLED = _env_int("HONEYPOT_REQUIRE_MINT_AUTH_DISABLED", 0)

        # Source toggles
        HONEYPOT_ENABLE_DEXSCREENER = _env_int("HONEYPOT_ENABLE_DEXSCREENER", 1)
        HONEYPOT_ENABLE_RUGCHECK = _env_int("HONEYPOT_ENABLE_RUGCHECK", 0)
        HONEYPOT_ENABLE_BIRDEYE = _env_int("HONEYPOT_ENABLE_BIRDEYE", 0)

        # Lists
        HONEYPOT_BLACKLIST_CSV = _env_str("HONEYPOT_BLACKLIST_CSV", "")
        HONEYPOT_ALLOWLIST_CSV = _env_str("HONEYPOT_ALLOWLIST_CSV", "")

        # Probe sizing
        HONEYPOT_SELL_PROBE_SLIPPAGE_BPS = _env_int("HONEYPOT_SELL_PROBE_SLIPPAGE_BPS", 300)
        HONEYPOT_SELL_PROBE_MAX_IMPACT_PCT = _env_float("HONEYPOT_SELL_PROBE_MAX_IMPACT_PCT", 90.0)

        # Cache memory bound (module-local)
        HONEYPOT_CACHE_MAX_ITEMS = _env_int("HONEYPOT_CACHE_MAX_ITEMS", 2048)

    C = _C()  # type: ignore

DEXSCREENER_TOKEN_URL = _env_str(
    "DEXSCREENER_TOKEN_URL",
    "https://api.dexscreener.com/latest/dex/tokens/{mint}",
)

BIRDEYE_API_URL = _env_str("BIRDEYE_API_URL", "https://public-api.birdeye.so")
BIRDEYE_API_KEY = _env_str("BIRDEYE_API_KEY", "")

RUGCHECK_API_URL = _env_str(
    "RUGCHECK_API_URL",
    "https://api.rugcheck.xyz/v1/tokens/{mint}",
)

JUPITER_BASE = _env_str("JUPITER_SWAP_API", "https://quote-api.jup.ag/v6")
JUPITER_QUOTE_URL = JUPITER_BASE.rstrip("/") + "/quote"

WSOL_MINT = _env_str(
    "WSOL_MINT",
    "So11111111111111111111111111111111111111112",
)

# -----------------------------------------------------------------------------
# Session management (compatible with ai_gate expectations)
# -----------------------------------------------------------------------------
_SESSION: Any = None
_SESSION_LOCK = asyncio.Lock()


async def _get_session() -> Any:
    """
    Return an aiohttp.ClientSession.

    Priority:
      1) SHARED_SESSION if present & open
      2) module-local session
    """
    global _SESSION

    if SHARED_SESSION is not None and not getattr(SHARED_SESSION, "closed", True):
        return SHARED_SESSION

    if aiohttp is None:
        raise RuntimeError("aiohttp is not installed (honeypot requires aiohttp for HTTP probes)")

    timeout_s = float(getattr(C, "HONEYPOT_HTTP_TIMEOUT_S", 10.0))
    async with _SESSION_LOCK:
        if _SESSION is not None and not getattr(_SESSION, "closed", True):
            return _SESSION
        _SESSION = aiohttp.ClientSession(  # type: ignore[attr-defined]
            timeout=aiohttp.ClientTimeout(total=float(timeout_s))  # type: ignore[attr-defined]
        )
        return _SESSION


async def close_local_session() -> None:
    global _SESSION
    async with _SESSION_LOCK:
        if _SESSION is not None and not getattr(_SESSION, "closed", True):
            try:
                await _SESSION.close()
            except Exception:
                pass
        _SESSION = None


def _atexit_close() -> None:
    # Best-effort: only close module-local session when we're not in a running loop.
    try:
        if SHARED_SESSION is not None:
            return
        if _SESSION is None or getattr(_SESSION, "closed", True):
            return
        try:
            asyncio.get_running_loop()
            return  # can't run coroutine here safely
        except Exception:
            pass
        try:
            asyncio.run(close_local_session())
        except Exception:
            pass
    except Exception:
        pass


atexit.register(_atexit_close)


# -----------------------------------------------------------------------------
# Utilities
# -----------------------------------------------------------------------------
async def _maybe_await(x: Any) -> Any:
    if asyncio.iscoroutine(x):
        return await x
    if hasattr(x, "__await__"):
        return await x
    return x


def _headers_birdeye() -> dict[str, str]:
    h = {"accept": "application/json", "x-chain": "solana"}
    if BIRDEYE_API_KEY:
        h["X-API-KEY"] = BIRDEYE_API_KEY
    return h


def _csv_has(mint: str, csv_val: str) -> bool:
    if not csv_val:
        return False
    s = mint.strip()
    if not s:
        return False
    return s in {x.strip() for x in csv_val.split(",") if x.strip()}


def _safe_float(x: Any, default: float = 0.0) -> float:
    try:
        f = float(x)
        if f != f or f in (float("inf"), float("-inf")):
            return default
        return f
    except Exception:
        return default


def _normalize_price_impact_pct(raw_pi: Any) -> float:
    """
    Jupiter sometimes returns fraction (0.12) or percent (12).
    Normalize to percent.
    """
    pi = _safe_float(raw_pi, 0.0)
    if 0.0 <= pi <= 1.0:
        pi *= 100.0
    return max(0.0, pi)


async def _with_timeout(coro: Awaitable[Any], timeout_s: float) -> Any:
    """
    Cancellation-safe timeout wrapper.
    """
    try:
        return await asyncio.wait_for(coro, timeout=float(timeout_s))
    except asyncio.CancelledError:
        raise


async def _fetch_json(
    url: str,
    *,
    headers: dict[str, str] | None = None,
    params: dict[str, str] | None = None,
) -> dict[str, Any] | None:
    try:
        sess = await _get_session()
        async with sess.get(url, headers=headers, params=params) as resp:
            if int(getattr(resp, "status", 0) or 0) == 200:
                js = await resp.json()
                return js if isinstance(js, dict) else None

            body = "<no-body>"
            try:
                body = (await resp.text()) or "<empty>"
            except Exception:
                body = "<unreadable>"
            log_every(
                "HONEYPOT_HTTP_NON200",
                n=15,
                level=10,  # DEBUG
                msg="honeypot http non-200",
                status=int(getattr(resp, "status", 0) or 0),
                url=url,
                body_preview=body[:300],
            )
            return None
    except asyncio.CancelledError:
        raise
    except Exception as e:
        log_every(
            "HONEYPOT_HTTP_ERR",
            n=15,
            level=10,  # DEBUG
            msg="honeypot http error",
            url=url,
            err=type(e).__name__,
            detail=_short(e),
        )
        return None


# -----------------------------------------------------------------------------
# Cache (mint -> report)
# -----------------------------------------------------------------------------
_HP_CACHE: dict[str, tuple[float, dict[str, Any]]] = {}
_HP_CACHE_LOCK = asyncio.Lock()


async def _cache_get(mint: str) -> dict[str, Any] | None:
    ttl = float(getattr(C, "HONEYPOT_CACHE_TTL_S", 180.0))
    if ttl <= 0:
        return None
    now = time.time()
    async with _HP_CACHE_LOCK:
        row = _HP_CACHE.get(mint)
        if not row:
            return None
        exp, rep = row
        if now >= float(exp):
            _HP_CACHE.pop(mint, None)
            return None
        return rep


async def _cache_prune_if_needed() -> None:
    try:
        max_items = int(getattr(C, "HONEYPOT_CACHE_MAX_ITEMS", 2048))
    except Exception:
        max_items = 2048
    if max_items <= 0:
        return

    async with _HP_CACHE_LOCK:
        if len(_HP_CACHE) <= max_items:
            return

        now = time.time()
        expired = [k for k, (exp, _) in _HP_CACHE.items() if now >= float(exp)]
        for k in expired:
            _HP_CACHE.pop(k, None)

        while len(_HP_CACHE) > max_items:
            try:
                oldest = next(iter(_HP_CACHE))
            except Exception:
                break
            _HP_CACHE.pop(oldest, None)


async def _cache_set(mint: str, rep: dict[str, Any]) -> None:
    ttl = float(getattr(C, "HONEYPOT_CACHE_TTL_S", 180.0))
    if ttl <= 0:
        return
    async with _HP_CACHE_LOCK:
        _HP_CACHE[mint] = (time.time() + float(ttl), rep)
    await _cache_prune_if_needed()


# -----------------------------------------------------------------------------
# Source checks
# -----------------------------------------------------------------------------
def _mk_source(source: str, *, enabled: bool) -> dict[str, Any]:
    return {
        "source": source,
        "enabled": bool(enabled),
        "ok": True,
        "hard_fail": False,
        "reasons": [],
        "data": {},
    }


async def _dexscreener_check(mint: str) -> dict[str, Any]:
    enabled = bool(int(getattr(C, "HONEYPOT_ENABLE_DEXSCREENER", 1)))
    out = _mk_source("dexscreener", enabled=enabled)
    if not enabled:
        out["reasons"].append("dexscreener:disabled")
        return out

    js = await _fetch_json(DEXSCREENER_TOKEN_URL.format(mint=mint))
    if not js or "pairs" not in js:
        out["reasons"].append("dexscreener:no_data:soft")
        return out

    pairs = js.get("pairs") or []
    if not isinstance(pairs, list):
        out["reasons"].append("dexscreener:bad_schema:soft")
        return out

    best_liq = 0.0
    any_paused = False
    any_hp_flag = False

    for p in pairs:
        if not isinstance(p, dict):
            continue

        liq_info = p.get("liquidity")
        liq_usd = 0.0
        if isinstance(liq_info, dict):
            liq_usd = _safe_float(liq_info.get("usd"), 0.0)
        else:
            liq_usd = _safe_float(liq_info, 0.0)
        if liq_usd > best_liq:
            best_liq = liq_usd

        if bool(p.get("tradingPaused")):
            any_paused = True
        if bool(p.get("isHoneyPot")):
            any_hp_flag = True

    out["data"]["pair_count"] = int(len(pairs))
    out["data"]["liquidity_usd_best"] = float(best_liq)

    min_liq = float(getattr(C, "HONEYPOT_MIN_LIQUIDITY_USD", 3000.0))
    if float(best_liq) < float(min_liq):
        out["ok"] = False
        out["hard_fail"] = True
        out["reasons"].append("dexscreener:low_liquidity")

    if any_paused:
        out["ok"] = False
        out["hard_fail"] = True
        out["reasons"].append("dexscreener:trading_paused")

    if any_hp_flag:
        out["ok"] = False
        out["hard_fail"] = True
        out["reasons"].append("dexscreener:honeypot_flag")

    return out


async def _birdeye_price_check(mint: str) -> dict[str, Any]:
    enabled = bool(int(getattr(C, "HONEYPOT_ENABLE_BIRDEYE", 0)))
    out = _mk_source("birdeye", enabled=enabled)

    if not enabled:
        out["reasons"].append("birdeye:disabled")
        return out

    if not BIRDEYE_API_KEY:
        out["reasons"].append("birdeye:no_api_key:soft")
        return out

    url = "%s/public/price" % (BIRDEYE_API_URL.rstrip("/"),)
    js = await _fetch_json(url, headers=_headers_birdeye(), params={"address": mint})
    if not js:
        out["reasons"].append("birdeye:no_data:soft")
        return out

    price = None
    try:
        data = js.get("data") or {}
        if isinstance(data, dict):
            val = data.get("value")
            if val is not None:
                price = float(val)
    except Exception:
        price = None

    out["data"]["price"] = price
    if price is None:
        out["reasons"].append("birdeye:bad_price:soft")
    return out


async def _rugcheck_check(mint: str) -> dict[str, Any]:
    enabled = bool(int(getattr(C, "HONEYPOT_ENABLE_RUGCHECK", 0)))
    strict = bool(int(getattr(C, "HONEYPOT_RUGCHECK_STRICT", 0)))

    out = _mk_source("rugcheck", enabled=enabled)
    if not enabled:
        out["reasons"].append("rugcheck:disabled")
        return out

    js = await _fetch_json(RUGCHECK_API_URL.format(mint=mint))
    if not js:
        if strict:
            out["ok"] = False
            out["hard_fail"] = True
            out["reasons"].append("rugcheck:no_data:strict")
        else:
            out["reasons"].append("rugcheck:no_data:soft")
        return out

    score = js.get("score", None)
    risks = js.get("risks") or js.get("risk") or []
    out["data"]["score"] = score
    out["data"]["risks"] = risks

    risk_text = " ".join(str(r).lower() for r in risks)
    bad = any(
        k in risk_text
        for k in (
            "honeypot",
            "trading disabled",
            "sell blocked",
            "cannot sell",
            "blacklist",
            "freeze",
        )
    )
    if bad:
        out["ok"] = False
        out["hard_fail"] = True
        out["reasons"].append("rugcheck:high_risk_flags")

    return out


async def _mint_authority_check(mint: str) -> dict[str, Any]:
    """
    On-chain mint/freeze authority flags via utils_pkg.liquidity.fetch_mint_info().
    Deterministic and should be preferred when available.
    """
    out = _mk_source("mint", enabled=True)

    if fetch_mint_info is None:
        out["reasons"].append("mint:fetch_mint_info_unavailable:soft")
        return out

    timeout_s = float(getattr(C, "HONEYPOT_HTTP_TIMEOUT_S", 10.0))
    try:
        info_obj = fetch_mint_info(mint)
        info = await _with_timeout(_maybe_await(info_obj), timeout_s=timeout_s)
    except asyncio.CancelledError:
        raise
    except asyncio.TimeoutError:
        out["reasons"].append("mint:timeout:soft")
        return out
    except Exception as e:
        out["reasons"].append("mint:error:soft")
        out["data"]["err"] = type(e).__name__
        out["data"]["detail"] = _short(e)
        return out

    if info is None:
        out["ok"] = False
        out["hard_fail"] = True
        out["reasons"].append("mint:no_account_or_parse")
        return out

    decimals = getattr(info, "decimals", None)
    mint_auth_present = getattr(info, "mint_authority_present", None)
    freeze_auth_present = getattr(info, "freeze_authority_present", None)
    src = getattr(info, "source", None)

    out["data"]["decimals"] = decimals
    out["data"]["mint_authority_present"] = mint_auth_present
    out["data"]["freeze_authority_present"] = freeze_auth_present
    out["data"]["source"] = src

    require_no_freeze = bool(int(getattr(C, "HONEYPOT_REQUIRE_NO_FREEZE", 0)))
    require_mint_disabled = bool(int(getattr(C, "HONEYPOT_REQUIRE_MINT_AUTH_DISABLED", 0)))

    if require_no_freeze and mint_auth_present is not None and freeze_auth_present is True:
        out["ok"] = False
        out["hard_fail"] = True
        out["reasons"].append("mint:freeze_authority_present")

    if require_mint_disabled and mint_auth_present is True:
        out["ok"] = False
        out["hard_fail"] = True
        out["reasons"].append("mint:mint_authority_present")

    return out


def _pick_sell_probe_amount_raw(decimals: int | None) -> int:
    """
    Choose a small-but-not-tiny raw amount for sell-route probing.
    Goal: trigger route discovery without requiring a balance.
    """
    if decimals is None:
        return 1_000_000
    try:
        d = int(decimals)
    except Exception:
        return 1_000_000
    if d <= 0:
        return 1
    amt = int((10**d) * 0.01)  # ~0.01 token
    return max(1, min(int(amt), 50_000_000))


def _try_import_router() -> ModuleType | None:
    """
    Avoids mypy 'Module vs None' assignment issues by isolating import.
    """
    try:
        from utils_pkg import router as router_mod  # type: ignore[import-not-found]

        return router_mod  # type: ignore[return-value]
    except Exception:
        return None


async def _router_sell_quote_probe(mint: str, amount_raw: int) -> dict[str, Any] | None:
    """
    Prefer router.get_sell_quote() if present; fall back to router.get_quote() if needed.
    Returns quote dict or None.
    """
    router_mod = _try_import_router()
    if router_mod is None:
        return None

    fn = getattr(router_mod, "get_sell_quote", None)
    if callable(fn):
        for kwargs in (
            {"mint": mint, "amount_raw": amount_raw},
            {"mint": mint, "amount": amount_raw},
            {"mint": mint, "amount_in": amount_raw},
            {"input_mint": mint, "amount_raw": amount_raw},
        ):
            try:
                q = fn(**kwargs)
                q2 = await _maybe_await(q)
                return q2 if isinstance(q2, dict) else None
            except TypeError:
                continue
            except asyncio.CancelledError:
                raise
            except Exception:
                return None

    fn2 = getattr(router_mod, "get_quote", None)
    if callable(fn2):
        for kwargs in (
            {"input_mint": mint, "output_mint": WSOL_MINT, "amount_raw": amount_raw},
            {"inputMint": mint, "outputMint": WSOL_MINT, "amount": amount_raw},
        ):
            try:
                q = fn2(**kwargs)
                q2 = await _maybe_await(q)
                return q2 if isinstance(q2, dict) else None
            except TypeError:
                continue
            except asyncio.CancelledError:
                raise
            except Exception:
                return None

    return None


async def _jupiter_sell_route_check(mint: str, decimals: int | None) -> dict[str, Any]:
    """
    Probes for a SELL route mint -> WSOL.

    Hard-fails when:
      - require_jup_sell_route == True AND we definitively see "no route"
      - OR strict mode is enabled and HTTP/router probe errors should block
    """
    require_route = bool(int(getattr(C, "HONEYPOT_REQUIRE_JUP_SELL_ROUTE", 1)))
    strict = bool(int(getattr(C, "HONEYPOT_JUP_STRICT", 0)))
    max_impact_pct = float(getattr(C, "HONEYPOT_SELL_PROBE_MAX_IMPACT_PCT", 90.0))
    slip_bps = int(getattr(C, "HONEYPOT_SELL_PROBE_SLIPPAGE_BPS", 300))

    out = _mk_source("jupiter", enabled=True)

    amount_raw = _pick_sell_probe_amount_raw(decimals)
    out["data"]["amount_raw"] = int(amount_raw)

    # 1) Prefer router probe
    try:
        q = await _router_sell_quote_probe(mint, amount_raw=amount_raw)
    except asyncio.CancelledError:
        raise
    except Exception:
        q = None

    if isinstance(q, dict) and q:
        route_plan = q.get("routePlan") or q.get("route_plan") or []
        out_amt = q.get("outAmount") or q.get("out_amount")
        pi_pct = _normalize_price_impact_pct(
            q.get("priceImpactPct", q.get("price_impact_pct", 0.0))
        )

        out["data"]["via"] = "router"
        out["data"]["route_len"] = int(len(route_plan)) if isinstance(route_plan, list) else 0
        out["data"]["outAmount"] = out_amt
        out["data"]["priceImpactPct"] = float(pi_pct)

        route_ok = bool(route_plan) and _safe_float(out_amt, 0.0) > 0.0
        if not route_ok:
            if require_route:
                out["ok"] = False
                out["hard_fail"] = True
                out["reasons"].append("jupiter:no_route(router)")
            else:
                out["reasons"].append("jupiter:no_route(router):soft")
            return out

        if float(pi_pct) > float(max_impact_pct):
            if strict:
                out["ok"] = False
                out["hard_fail"] = True
                out["reasons"].append("jupiter:sell_impact_too_high")
            else:
                out["reasons"].append("jupiter:sell_impact_high:soft")
        return out

    # 2) Fallback to direct HTTP probe
    params = {
        "inputMint": mint,
        "outputMint": WSOL_MINT,
        "amount": str(amount_raw),
        "slippageBps": str(int(slip_bps)),
        "swapMode": "ExactIn",
        "onlyDirectRoutes": "false",
    }

    try:
        sess = await _get_session()
        async with sess.get(JUPITER_QUOTE_URL, params=params) as resp:
            status = int(getattr(resp, "status", 0) or 0)
            if status != 200:
                tag = "jupiter:http_non200"
                if strict and require_route:
                    out["ok"] = False
                    out["hard_fail"] = True
                    out["reasons"].append(tag)
                else:
                    out["reasons"].append(tag + ":soft")
                out["data"]["status"] = status
                return out
            js = await resp.json()
    except asyncio.CancelledError:
        raise
    except Exception as e:
        tag = "jupiter:error"
        if strict and require_route:
            out["ok"] = False
            out["hard_fail"] = True
            out["reasons"].append(tag)
        else:
            out["reasons"].append(tag + ":soft")
        out["data"]["err"] = type(e).__name__
        out["data"]["detail"] = _short(e)
        return out

    route_plan = js.get("routePlan") or []
    out_amt = js.get("outAmount")
    pi_pct = _normalize_price_impact_pct(js.get("priceImpactPct", 0.0))

    out["data"]["via"] = "http"
    out["data"]["route_len"] = int(len(route_plan)) if isinstance(route_plan, list) else 0
    out["data"]["outAmount"] = out_amt
    out["data"]["priceImpactPct"] = float(pi_pct)

    route_ok = bool(route_plan) and _safe_float(out_amt, 0.0) > 0.0
    if not route_ok:
        if require_route:
            out["ok"] = False
            out["hard_fail"] = True
            out["reasons"].append("jupiter:no_route(http)")
        else:
            out["reasons"].append("jupiter:no_route(http):soft")
        return out

    if float(pi_pct) > float(max_impact_pct):
        if strict:
            out["ok"] = False
            out["hard_fail"] = True
            out["reasons"].append("jupiter:sell_impact_too_high")
        else:
            out["reasons"].append("jupiter:sell_impact_high:soft")

    return out


# -----------------------------------------------------------------------------
# Public API
# -----------------------------------------------------------------------------
@dataclass(frozen=True)
class HoneypotPolicy:
    min_liquidity_usd: float
    require_jup_sell_route: bool
    jup_strict: bool
    rugcheck_strict: bool
    require_no_freeze: bool
    require_mint_auth_disabled: bool

    @staticmethod
    def current() -> HoneypotPolicy:
        return HoneypotPolicy(
            min_liquidity_usd=float(getattr(C, "HONEYPOT_MIN_LIQUIDITY_USD", 3000.0)),
            require_jup_sell_route=bool(int(getattr(C, "HONEYPOT_REQUIRE_JUP_SELL_ROUTE", 1))),
            jup_strict=bool(int(getattr(C, "HONEYPOT_JUP_STRICT", 0))),
            rugcheck_strict=bool(int(getattr(C, "HONEYPOT_RUGCHECK_STRICT", 0))),
            require_no_freeze=bool(int(getattr(C, "HONEYPOT_REQUIRE_NO_FREEZE", 0))),
            require_mint_auth_disabled=bool(
                int(getattr(C, "HONEYPOT_REQUIRE_MINT_AUTH_DISABLED", 0))
            ),
        )


async def honeypot_check(mint: str, *, force: bool = False) -> tuple[bool, dict[str, Any]]:
    """
    Aggregated honeypot / safety decision.

    Returns:
      (ok, report)

    report keys:
      - mint: str
      - ok: bool
      - hard_reasons: list[str]
      - soft_reasons: list[str]
      - reasons: list[str] (merged)
      - summary: dict
      - sources: dict[str, dict]
      - policy: dict
      - t_ms: float (elapsed ms)
    """
    t0 = time.perf_counter()
    mint_s = (mint or "").strip()
    policy = HoneypotPolicy.current()

    allow_csv = str(getattr(C, "HONEYPOT_ALLOWLIST_CSV", "") or "")
    block_csv = str(getattr(C, "HONEYPOT_BLACKLIST_CSV", "") or "")

    if _csv_has(mint_s, allow_csv):
        rep = {
            "mint": mint_s,
            "ok": True,
            "hard_reasons": [],
            "soft_reasons": ["allowlist"],
            "reasons": ["allowlist"],
            "summary": {"is_honeypot": False, "trading_paused": False, "blacklisted": False},
            "sources": {},
            "policy": policy.__dict__,
            "t_ms": (time.perf_counter() - t0) * 1000.0,
        }
        await _cache_set(mint_s, rep)
        return True, rep

    if _csv_has(mint_s, block_csv):
        rep = {
            "mint": mint_s,
            "ok": False,
            "hard_reasons": ["blacklist"],
            "soft_reasons": [],
            "reasons": ["blacklist"],
            "summary": {"is_honeypot": True, "trading_paused": False, "blacklisted": True},
            "sources": {},
            "policy": policy.__dict__,
            "t_ms": (time.perf_counter() - t0) * 1000.0,
        }
        await _cache_set(mint_s, rep)
        return False, rep

    if not force:
        cached = await _cache_get(mint_s)
        if cached is not None:
            return bool(cached.get("ok", False)), cached

    total_budget_s = float(getattr(C, "HONEYPOT_TOTAL_BUDGET_S", 12.0))
    per_call_timeout_s = float(getattr(C, "HONEYPOT_HTTP_TIMEOUT_S", 10.0))
    per_call_timeout_s = max(1.0, min(per_call_timeout_s, total_budget_s))

    async def _run_all() -> dict[str, Any]:
        mint_task = asyncio.create_task(_mint_authority_check(mint_s))
        dex_task = asyncio.create_task(_dexscreener_check(mint_s))
        rug_task = asyncio.create_task(_rugcheck_check(mint_s))
        bird_task = asyncio.create_task(_birdeye_price_check(mint_s))

        res = await asyncio.gather(
            _with_timeout(mint_task, per_call_timeout_s),
            _with_timeout(dex_task, per_call_timeout_s),
            _with_timeout(rug_task, per_call_timeout_s),
            _with_timeout(bird_task, per_call_timeout_s),
            return_exceptions=True,
        )

        sources: dict[str, Any] = {}
        decimals: int | None = None

        for item in res:
            if isinstance(item, asyncio.TimeoutError):
                src = "timeout"
                sources.setdefault(src, _mk_source(src, enabled=True))
                sources[src]["reasons"].append("source:timeout:soft")
                continue

            if isinstance(item, Exception):
                sources.setdefault("exceptions", _mk_source("exceptions", enabled=True))
                sources["exceptions"]["reasons"].append("source:exception:soft")
                sources["exceptions"]["data"].setdefault("count", 0)
                sources["exceptions"]["data"]["count"] = (
                    int(sources["exceptions"]["data"]["count"]) + 1
                )
                continue

            if isinstance(item, dict):
                src = str(item.get("source", "unknown"))
                sources[src] = item
                if src == "mint":
                    try:
                        d = (item.get("data") or {}).get("decimals")
                        decimals = int(d) if d is not None else None
                    except Exception:
                        decimals = None

        try:
            jup = await _with_timeout(
                _jupiter_sell_route_check(mint_s, decimals), per_call_timeout_s
            )
        except asyncio.TimeoutError:
            jup = _mk_source("jupiter", enabled=True)
            jup["reasons"].append("jupiter:timeout:soft")
        except Exception as e:
            jup = _mk_source("jupiter", enabled=True)
            jup["reasons"].append("jupiter:error:soft")
            jup["data"]["err"] = type(e).__name__
            jup["data"]["detail"] = _short(e)

        sources["jupiter"] = jup
        return sources

    try:
        sources = await asyncio.wait_for(_run_all(), timeout=float(total_budget_s))
    except asyncio.CancelledError:
        raise
    except Exception as e:
        rep = {
            "mint": mint_s,
            "ok": True,
            "hard_reasons": [],
            "soft_reasons": ["honeypot:internal_error:soft"],
            "reasons": ["honeypot:internal_error:soft"],
            "summary": {"is_honeypot": False, "trading_paused": False, "blacklisted": False},
            "sources": {
                "internal": {
                    "source": "internal",
                    "enabled": True,
                    "ok": True,
                    "hard_fail": False,
                    "reasons": ["honeypot:internal_error:soft"],
                    "data": {"err": type(e).__name__, "detail": _short(e)},
                }
            },
            "policy": policy.__dict__,
            "t_ms": (time.perf_counter() - t0) * 1000.0,
        }
        await _cache_set(mint_s, rep)
        return True, rep

    hard: list[str] = []
    soft: list[str] = []

    liq_usd: float | None = None
    trading_paused = False
    is_honeypot = False

    for src, r in sources.items():
        if not isinstance(r, dict):
            continue
        rs = list(r.get("reasons") or [])

        if (not bool(r.get("ok", True))) and bool(r.get("hard_fail", False)):
            hard.extend([str(x) for x in rs if x])
        else:
            for x in rs:
                if x:
                    soft.append(str(x))

        if src == "dexscreener":
            try:
                liq_usd_val = (r.get("data") or {}).get("liquidity_usd_best")
                if liq_usd_val is not None:
                    liq_usd = float(liq_usd_val)
            except Exception:
                pass
            if any("trading_paused" in str(x) for x in rs):
                trading_paused = True
            if any("honeypot" in str(x) for x in rs):
                is_honeypot = True

        if src == "mint":
            if any("freeze_authority_present" in str(x) for x in rs):
                is_honeypot = True

        if src == "jupiter":
            if any("no_route" in str(x) for x in rs):
                is_honeypot = True

    ok = len(hard) == 0
    reasons = hard + soft

    jup_rep = sources.get("jupiter")
    jup_ok = None
    jup_pi = None
    if isinstance(jup_rep, dict):
        jup_ok = bool(jup_rep.get("ok", True))
        try:
            jup_pi = (jup_rep.get("data") or {}).get("priceImpactPct")
        except Exception:
            jup_pi = None

    report: dict[str, Any] = {
        "mint": mint_s,
        "ok": bool(ok),
        "hard_reasons": hard,
        "soft_reasons": soft,
        "reasons": reasons,
        "summary": {
            "is_honeypot": bool(is_honeypot) if not ok else False,
            "trading_paused": bool(trading_paused),
            "blacklisted": False,
            "liq_usd": float(liq_usd) if liq_usd is not None else None,
            "jup_sell_route_ok": jup_ok,
            "sell_price_impact_pct": jup_pi,
        },
        "sources": sources,
        "policy": policy.__dict__,
        "t_ms": (time.perf_counter() - t0) * 1000.0,
    }

    await _cache_set(mint_s, report)
    return bool(ok), report


async def is_token_safe(mint: str) -> bool:
    ok, _ = await honeypot_check(mint)
    return bool(ok)


__all__ = [
    # compatibility exports
    "BIRDEYE_API_URL",
    "JUPITER_QUOTE_URL",
    "SHARED_SESSION",
    "SHARED_SOLANA_CLIENT",
    "WSOL_MINT",
    "HoneypotPolicy",
    "_get_session",
    "_headers_birdeye",
    "close_local_session",
    # public API
    "honeypot_check",
    "is_token_safe",
]
