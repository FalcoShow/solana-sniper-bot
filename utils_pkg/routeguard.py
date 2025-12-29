from __future__ import annotations

"""
utils_pkg/routeguard.py — V3+++ (God-Tier Overhaul, Phase 0–7) — LOCK-IN
========================================================================

RouteGuard is the low-level gatekeeper for the Jupiter/lightning path.

Public API
----------
  async def preflight(*, mint: str, lamports_in: int) -> tuple[bool, dict | None]
  def clear_cache(*, cancel_inflight: bool = False) -> None
  def stats() -> dict

Return contract (router MUST follow):
------------------------------------
  (True, quote_dict) -> Jupiter allowed. quote_dict may be {} meaning "allow Jupiter but no prequote".
  (True, None)       -> fallback-only allowed (router MUST NOT attempt Jupiter)
  (False, None)      -> blocked outright

CRITICAL COMPAT NOTE
--------------------
router._rg_preflight() treats:
  fallback_only := (routeguard_present AND prequote is None)

Therefore:
  - When guard is DISABLED, we MUST NOT return None.
  - Use {} to mean "allow Jupiter but don't enforce / don't pass prequote".

Hard requirements
-----------------
- NO network at import time.
- Async functions never block event loop (no time.sleep, no requests).
- Signature drift tolerant vs router quote helpers.
- Caching + inflight coalescing + breakers to prevent thrashing.
"""

import asyncio
import contextlib
import inspect
import os
import time
from collections import OrderedDict, deque
from typing import Any, Dict, Iterable, Optional, Tuple

# ──────────────────────────────────────────────────────────────────────────────
# Logging (shim-safe)
# ──────────────────────────────────────────────────────────────────────────────
try:
    from logging_setup import get_logger  # type: ignore
    log = get_logger(__name__)  # type: ignore
except Exception:  # pragma: no cover
    import logging
    log = logging.getLogger(__name__)  # type: ignore[assignment]


def _ensure_log_shims() -> None:
    if not hasattr(log, "event"):

        def _event(name: str, **kw: Any) -> None:  # pragma: no cover
            with contextlib.suppress(Exception):
                log.info("%s %s", name, kw)

        setattr(log, "event", _event)  # type: ignore[attr-defined]

    if not hasattr(log, "every"):

        def _every(key: str, n: int = 10, msg: Optional[str] = None, **kw: Any) -> None:  # pragma: no cover
            """
            Throttle by seconds (not by count).
            NOTE: many modules expect log.every(key, seconds, msg=..., **fields)
            """
            try:
                now = time.monotonic()
                store = getattr(_every, "_store", {})  # type: ignore[attr-defined]
                last = float(store.get(key, 0.0))
                if (now - last) >= float(max(1, int(n))):
                    store[key] = now
                    setattr(_every, "_store", store)  # type: ignore[attr-defined]
                    ev = getattr(log, "event", None)
                    if callable(ev):
                        ev(msg or key, **kw)
                    else:
                        log.info("%s %s", msg or key, kw)
            except Exception:
                return None

        setattr(log, "every", _every)  # type: ignore[attr-defined]


_ensure_log_shims()


def _short(x: Any, n: int = 260) -> str:
    try:
        s = str(x)
        return s if len(s) <= n else (s[:n] + "…")
    except Exception:
        return "<?>"


# ──────────────────────────────────────────────────────────────────────────────
# Lazy imports (avoid cycles + no network at import)
# ──────────────────────────────────────────────────────────────────────────────
_router_mod = None
_mint_norm = None


def _get_router():
    global _router_mod
    if _router_mod is not None:
        return _router_mod
    try:
        from utils_pkg import router as r  # type: ignore
        _router_mod = r
        return _router_mod
    except Exception:
        _router_mod = None
        return None


def _router_fn(name: str):
    r = _get_router()
    return getattr(r, name, None) if r else None


def _get_mint_normalizer():
    """Prefer utils_pkg.mints.normalize_mint if present."""
    global _mint_norm
    if _mint_norm is not None:
        return _mint_norm
    try:
        from utils_pkg.mints import normalize_mint as nm  # type: ignore
        _mint_norm = nm
        return _mint_norm
    except Exception:
        _mint_norm = None
        return None


try:  # pragma: no cover
    from solders.pubkey import Pubkey as _SoldersPubkey  # type: ignore
except Exception:  # pragma: no cover
    _SoldersPubkey = None  # type: ignore


# ──────────────────────────────────────────────────────────────────────────────
# Env helpers
# ──────────────────────────────────────────────────────────────────────────────
_TRUE = {"1", "true", "yes", "y", "on", "t"}
_FALSE = {"0", "false", "no", "n", "off", "f"}


def _envs(k: str, d: str = "") -> str:
    v = os.getenv(k)
    return (v if v is not None else d).strip()


def _envb(k: str, d: bool = False) -> bool:
    v = _envs(k, "")
    if v == "":
        return bool(d)
    s = v.lower().strip()
    if s in _TRUE:
        return True
    if s in _FALSE:
        return False
    return bool(d)


def _envi(k: str, d: int) -> int:
    try:
        return int(float(_envs(k, str(d))))
    except Exception:
        return int(d)


def _envf(k: str, d: float) -> float:
    try:
        return float(_envs(k, str(d)))
    except Exception:
        return float(d)


def _parse_set_csv(v: str) -> set[str]:
    out: set[str] = set()
    for part in (v or "").split(","):
        p = part.strip()
        if p:
            out.add(p)
    return out


def _parse_raw_ladder(v: str) -> list[int]:
    """Parse ladder like '50k,200k,1m' -> [50000, 200000, 1000000]."""
    out: list[int] = []
    for part in (v or "").replace(" ", "").split(","):
        if not part:
            continue
        mult = 1
        p = part.lower()
        if p.endswith("k"):
            mult = 1_000
            p = p[:-1]
        elif p.endswith("m"):
            mult = 1_000_000
            p = p[:-1]
        try:
            n = int(float(p) * mult)
        except Exception:
            continue
        if n > 0:
            out.append(n)
    return out


# ──────────────────────────────────────────────────────────────────────────────
# Refreshable config (TTL)
# ──────────────────────────────────────────────────────────────────────────────
_CFG_TTL_SEC = max(0.25, _envf("ROUTEGUARD_REFRESH_SEC", 2.0))
_cfg_at: float = 0.0
_cfg_cache: Dict[str, Any] = {}


def _cfg() -> Dict[str, Any]:
    global _cfg_at, _cfg_cache
    now = time.monotonic()
    if _cfg_cache and (now - _cfg_at) < _CFG_TTL_SEC:
        return _cfg_cache

    cfg: Dict[str, Any] = {}

    # Master enable
    cfg["enabled"] = _envb("ROUTEGUARD_ENABLED", True)

    # Behavior knobs
    cfg["allow_no_buy_route"] = _envb(
        "ROUTEGUARD_ALLOW_NO_ROUTE",
        _envb("ROUTEGUARD_ALLOW_NO_BUY_ROUTE", True),
    )
    cfg["optimistic_buy_err"] = _envb("ROUTEGUARD_OPTIMISTIC_ON_BUY_QUOTE_ERROR", True)

    cfg["require_sell"] = _envb("ROUTEGUARD_REQUIRE_SELL", True)
    cfg["optimistic_sell_err"] = _envb("ROUTEGUARD_OPTIMISTIC_ON_SELL_QUOTE_ERROR", False)

    # Inflight coalescing
    cfg["coalesce_inflight"] = _envb("ROUTEGUARD_COALESCE_INFLIGHT", True)
    cfg["inflight_shield"] = _envb("ROUTEGUARD_INFLIGHT_SHIELD", True)
    cfg["max_inflight"] = max(0, _envi("ROUTEGUARD_MAX_INFLIGHT", 0))  # 0 disables cap

    # TTL caches
    cfg["quote_ttl_sec"] = max(
        1,
        _envi(
            "ROUTEGUARD_BUY_QUOTE_TTL_SEC",
            _envi("ROUTER_QUOTE_TTL_SEC", _envi("PF_PRICE_TTL_SEC", 6)),
        ),
    )
    cfg["sell_ok_ttl_sec"] = max(2, _envi("ROUTEGUARD_SELL_OK_TTL_SEC", 20))
    cfg["cache_max_keys"] = max(100, _envi("ROUTEGUARD_CACHE_MAX_KEYS", 2000))

    # Per-mint cooldown on failures
    cfg["cooldown_base_sec"] = max(5, _envi("ROUTEGUARD_NOROUTE_COOLDOWN_SEC", _envi("NOROUTE_COOLDOWN_SEC", 300)))
    cfg["cooldown_max_sec"] = max(cfg["cooldown_base_sec"], _envi("ROUTEGUARD_NOROUTE_COOLDOWN_MAX_SEC", int(cfg["cooldown_base_sec"] * 3)))
    cfg["cooldown_exp"] = max(1.0, _envf("ROUTEGUARD_COOLDOWN_EXP", 1.6))

    # Global breaker
    cfg["global_fail_window_sec"] = max(0, _envi("ROUTEGUARD_GLOBAL_FAIL_WINDOW_SEC", 30))
    cfg["global_fail_max"] = max(0, _envi("ROUTEGUARD_GLOBAL_FAIL_MAX", 0))  # 0 disables
    cfg["global_cooldown_sec"] = max(0, _envi("ROUTEGUARD_GLOBAL_COOLDOWN_SEC", 20))

    # Auth breaker (401/403 / invalid key)
    cfg["auth_fail_window_sec"] = max(0, _envi("ROUTEGUARD_AUTH_FAIL_WINDOW_SEC", 60))
    cfg["auth_fail_max"] = max(0, _envi("ROUTEGUARD_AUTH_FAIL_MAX", 2))  # 0 disables
    cfg["auth_cooldown_sec"] = max(0, _envi("ROUTEGUARD_AUTH_COOLDOWN_SEC", 600))
    cfg["auth_blocks_jup"] = _envb("ROUTEGUARD_AUTH_BLOCKS_JUP", True)

    # Quality gates
    cfg["max_impact_bps"] = max(0, _envi("ROUTEGUARD_MAX_IMPACT_BPS", 1500))
    cfg["max_hops"] = max(0, _envi("ROUTEGUARD_MAX_HOPS", 0))  # 0 disables
    cfg["min_lamports_in"] = max(0, _envi("ROUTEGUARD_MIN_LAMPORTS_IN", 0))
    cfg["max_lamports_in"] = max(0, _envi("ROUTEGUARD_MAX_LAMPORTS_IN", 0))  # 0 disables
    cfg["min_out_raw"] = max(0, _envi("ROUTEGUARD_MIN_OUT_RAW", 0))

    # Route labels allow/block
    allow_labels = {s.lower() for s in _parse_set_csv(_envs("ROUTEGUARD_ALLOW_ROUTE_LABELS", ""))}
    block_labels = {s.lower() for s in _parse_set_csv(_envs("ROUTEGUARD_BLOCK_ROUTE_LABELS", ""))}
    cfg["allow_route_labels"] = allow_labels
    cfg["block_route_labels"] = block_labels
    cfg["use_allow_route_labels"] = bool(allow_labels)

    # Sell probe ladder
    ladder_env = _envs("ROUTEGUARD_SELL_PROBE_LADDER", "50k,500k,5m")
    cfg["sell_probe_ladder"] = _parse_raw_ladder(ladder_env) or [50_000, 500_000, 5_000_000]
    cfg["sell_probe_floor_raw"] = max(1, _envi("ROUTEGUARD_SELL_PROBE_FLOOR_RAW", 50_000))
    cfg["sell_probe_max_raw"] = max(0, _envi("ROUTEGUARD_SELL_PROBE_MAX_RAW", 0))  # 0 disables
    cfg["sell_dynamic_probes"] = _envb("ROUTEGUARD_SELL_DYNAMIC_PROBES", True)

    cfg["allow_sell_skip_if_tiny_out"] = _envb("ROUTEGUARD_ALLOW_SELL_SKIP_IF_TINY_OUT", False)
    cfg["sell_skip_tiny_out_raw"] = max(0, _envi("ROUTEGUARD_SELL_SKIP_TINY_OUT_RAW", 25_000))

    # Router availability / timeouts
    cfg["allow_fallback_if_router_missing"] = _envb("ROUTEGUARD_ALLOW_FALLBACK_IF_ROUTER_MISSING", True)
    cfg["buy_quote_timeout_sec"] = max(0.0, _envf("ROUTEGUARD_BUY_QUOTE_TIMEOUT_SEC", 4.0))
    cfg["sell_quote_timeout_sec"] = max(0.0, _envf("ROUTEGUARD_SELL_QUOTE_TIMEOUT_SEC", 4.0))

    # Mint allow/block
    cfg["allow_mints"] = _parse_set_csv(_envs("ROUTEGUARD_ALLOW_MINTS", ""))
    cfg["block_mints"] = _parse_set_csv(_envs("ROUTEGUARD_BLOCK_MINTS", ""))
    cfg["use_allowlist"] = bool(cfg["allow_mints"])

    # Strict raw mint handling
    cfg["force_fallback_if_raw_not_mint"] = _envb("ROUTEGUARD_FORCE_FALLBACK_IF_RAW_NOT_MINT", False)

    # Debug
    cfg["debug"] = _envb("ROUTEGUARD_DEBUG", False)
    cfg["trace_mints"] = _parse_set_csv(_envs("ROUTEGUARD_TRACE_MINTS", ""))

    _cfg_cache = cfg
    _cfg_at = now
    return cfg


# ──────────────────────────────────────────────────────────────────────────────
# Internal state (async-safe with a lock for shared structures)
# ──────────────────────────────────────────────────────────────────────────────
_LOCK = asyncio.Lock()

_buy_cache: "OrderedDict[tuple[str, int], tuple[float, Dict[str, Any]]]" = OrderedDict()
_sell_ok_cache: "OrderedDict[str, tuple[float, bool]]" = OrderedDict()

_cooldown_until: dict[str, float] = {}
_fail_streak: dict[str, int] = {}

_inflight: dict[tuple[str, int], "asyncio.Task[Tuple[bool, Optional[Dict[str, Any]]]]"] = {}

_global_fail_times: deque[float] = deque()
_global_cooldown_until: float = 0.0

_auth_fail_times: deque[float] = deque()
_auth_cooldown_until: float = 0.0

_C: dict[str, int] = {
    "calls": 0,
    "router_missing": 0,
    "disabled_passthrough": 0,
    "bad_input": 0,
    "blocked_allowlist": 0,
    "blocked_blocklist": 0,
    "blocked_minmax": 0,
    "cooldown_block": 0,
    "global_cooldown_block": 0,
    "auth_cooldown_block": 0,
    "buy_cache_hit": 0,
    "buy_cache_miss": 0,
    "sell_cache_hit": 0,
    "sell_cache_miss": 0,
    "fallback_raw_not_mint": 0,
    "fallback_norm_invalid": 0,
    "allow_fallback": 0,
    "allow_jup": 0,
    "block": 0,
    "block_impact": 0,
    "block_hops": 0,
    "block_route_label": 0,
    "sell_fail": 0,
    "buy_quote_error": 0,
    "sell_quote_error": 0,
    "auth_fail": 0,
    "cooldown_set": 0,
    "global_cooldown_set": 0,
    "auth_cooldown_set": 0,
    "inflight_overflow": 0,
    "token_not_tradable": 0,
    "no_route": 0,
    "rate_limited": 0,
}


def _inc(k: str, n: int = 1) -> None:
    _C[k] = int(_C.get(k, 0)) + int(n)


def clear_cache(*, cancel_inflight: bool = False) -> None:
    global _global_cooldown_until, _auth_cooldown_until
    _buy_cache.clear()
    _sell_ok_cache.clear()
    _cooldown_until.clear()
    _fail_streak.clear()
    _global_fail_times.clear()
    _auth_fail_times.clear()
    _global_cooldown_until = 0.0
    _auth_cooldown_until = 0.0
    if cancel_inflight:
        for t in list(_inflight.values()):
            with contextlib.suppress(Exception):
                t.cancel()
    _inflight.clear()


def stats() -> Dict[str, Any]:
    c = _cfg()
    now = time.monotonic()
    return {
        **_C,
        "buy_cache": len(_buy_cache),
        "sell_ok_cache": len(_sell_ok_cache),
        "cooldowns": len(_cooldown_until),
        "fail_streaks": len(_fail_streak),
        "inflight": len(_inflight),
        "enabled": int(bool(c.get("enabled"))),
        "allow_no_buy_route": int(bool(c.get("allow_no_buy_route"))),
        "require_sell_route": int(bool(c.get("require_sell"))),
        "optimistic_on_buy_quote_error": int(bool(c.get("optimistic_buy_err"))),
        "optimistic_on_sell_quote_error": int(bool(c.get("optimistic_sell_err"))),
        "quote_ttl_sec": int(c.get("quote_ttl_sec") or 0),
        "sell_ok_ttl_sec": int(c.get("sell_ok_ttl_sec") or 0),
        "cooldown_base_sec": int(c.get("cooldown_base_sec") or 0),
        "cooldown_max_sec": int(c.get("cooldown_max_sec") or 0),
        "max_impact_bps": int(c.get("max_impact_bps") or 0),
        "max_hops": int(c.get("max_hops") or 0),
        "min_lamports_in": int(c.get("min_lamports_in") or 0),
        "max_lamports_in": int(c.get("max_lamports_in") or 0),
        "min_out_raw": int(c.get("min_out_raw") or 0),
        "coalesce_inflight": int(bool(c.get("coalesce_inflight"))),
        "max_inflight": int(c.get("max_inflight") or 0),
        "buy_quote_timeout_sec": float(c.get("buy_quote_timeout_sec") or 0.0),
        "sell_quote_timeout_sec": float(c.get("sell_quote_timeout_sec") or 0.0),
        "cache_max_keys": int(c.get("cache_max_keys") or 0),
        "allowlist_enabled": int(bool(c.get("use_allowlist"))),
        "allow_mints_count": len(c.get("allow_mints") or set()),
        "block_mints_count": len(c.get("block_mints") or set()),
        "block_route_labels_count": len(c.get("block_route_labels") or set()),
        "allow_route_labels_count": len(c.get("allow_route_labels") or set()),
        "force_fallback_if_raw_not_mint": int(bool(c.get("force_fallback_if_raw_not_mint"))),
        "global_cooldown_active": int(now < _global_cooldown_until),
        "global_cooldown_left": int(max(0.0, _global_cooldown_until - now)),
        "auth_cooldown_active": int(now < _auth_cooldown_until),
        "auth_cooldown_left": int(max(0.0, _auth_cooldown_until - now)),
    }


# ──────────────────────────────────────────────────────────────────────────────
# Mint validation / normalization
# ──────────────────────────────────────────────────────────────────────────────
_BASE58 = set("123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz")


def _valid_pubkey_str(s: str) -> bool:
    ss = (s or "").strip()
    if not ss:
        return False

    if _SoldersPubkey is not None:
        try:
            _SoldersPubkey.from_string(ss)  # type: ignore[attr-defined]
            return True
        except Exception:
            return False

    # heuristic fallback
    if len(ss) < 32 or len(ss) > 50:
        return False
    return all(ch in _BASE58 for ch in ss)


def _normalize_to_mint(raw: str) -> str:
    raw = (raw or "").strip()
    if not raw:
        return ""

    nm = _get_mint_normalizer()
    if callable(nm):
        with contextlib.suppress(Exception):
            out = str(nm(raw)).strip()  # type: ignore[misc]
            if out:
                return out

    # optional: router may expose normalize_pump_id_to_mint(raw)->mint
    fn = _router_fn("normalize_pump_id_to_mint")
    if callable(fn):
        with contextlib.suppress(Exception):
            out = str(fn(raw)).strip()
            return out or raw

    return raw


# ──────────────────────────────────────────────────────────────────────────────
# Quote helpers / parsing
# ──────────────────────────────────────────────────────────────────────────────
async def _maybe_await(x: Any) -> Any:
    if inspect.isawaitable(x):
        return await x
    return x


async def _maybe_wait(awaitable_or_value: Any, timeout_s: float) -> Any:
    if not timeout_s or timeout_s <= 0:
        return await _maybe_await(awaitable_or_value)
    return await asyncio.wait_for(_maybe_await(awaitable_or_value), timeout=timeout_s)


def _is_errorish_quote(q: Dict[str, Any]) -> bool:
    if not isinstance(q, dict):
        return True
    if "error" in q and q.get("error"):
        return True
    if "errors" in q and q.get("errors"):
        return True
    if q.get("ok") is False:
        return True
    see = q.get("errorCode") or q.get("code")
    if isinstance(see, str) and see.strip():
        return True
    return False


def _impact_bps(q: Dict[str, Any]) -> int:
    try:
        v = q.get("priceImpactBps")
        if v is not None:
            return int(abs(float(v)))
    except Exception:
        pass
    try:
        pip = float(q.get("priceImpactPct") or 0.0)
        pip = abs(pip)
        if pip <= 1.0:
            return int(pip * 10_000)
        if pip <= 100.0:
            return int(pip * 100)
        return int(pip)
    except Exception:
        return 0


def _out_amount_raw(q: Dict[str, Any]) -> int:
    for k in ("outAmount", "out_amount", "out_amount_raw"):
        with contextlib.suppress(Exception):
            v = q.get(k)
            if v is not None:
                return int(v)
    data = q.get("data")
    if isinstance(data, dict):
        return _out_amount_raw(data)
    return 0


def _route_plan(q: Dict[str, Any]) -> list[dict]:
    rp = q.get("routePlan")
    if isinstance(rp, list):
        return [x for x in rp if isinstance(x, dict)]
    data = q.get("data")
    if isinstance(data, dict) and isinstance(data.get("routePlan"), list):
        return [x for x in data["routePlan"] if isinstance(x, dict)]
    return []


def _hops(q: Dict[str, Any]) -> int:
    return len(_route_plan(q))


def _route_labels(q: Dict[str, Any]) -> list[str]:
    out: list[str] = []
    for step in _route_plan(q):
        si = step.get("swapInfo") if isinstance(step, dict) else None
        if isinstance(si, dict):
            for k in ("label", "amm", "dex", "market"):
                v = si.get(k)
                if isinstance(v, str) and v.strip():
                    out.append(v.strip())
    seen: set[str] = set()
    uniq: list[str] = []
    for s in out:
        k = s.lower()
        if k in seen:
            continue
        seen.add(k)
        uniq.append(s)
    return uniq


def _route_label_blocks(q: Dict[str, Any]) -> Optional[str]:
    c = _cfg()
    labels = _route_labels(q)
    allow = c.get("allow_route_labels") or set()
    block = c.get("block_route_labels") or set()
    use_allow = bool(c.get("use_allow_route_labels"))

    if use_allow:
        if labels:
            ok = any(any(a in l.lower() for a in allow) for l in labels)
            if not ok:
                return f"no_allowed_labels labels={labels[:6]}"
        else:
            return "no_labels_allowlist_enabled"

    if block and labels:
        for l in labels:
            ll = l.lower()
            for b in block:
                if b and b in ll:
                    return f"blocked_label={b} label={l}"
    return None


def _ladder_with_floor(raws: Iterable[int]) -> list[int]:
    c = _cfg()
    floor = int(c.get("sell_probe_floor_raw") or 1)
    cap = int(c.get("sell_probe_max_raw") or 0)

    out: list[int] = []
    for x in raws:
        with contextlib.suppress(Exception):
            v = int(x)
            if v <= 0:
                continue
            v = max(v, floor)
            if cap and v > cap:
                v = cap
            out.append(v)

    seen: set[int] = set()
    uniq: list[int] = []
    for v in out:
        if v in seen:
            continue
        seen.add(v)
        uniq.append(v)
    uniq.sort()
    return uniq


def _dynamic_sell_probes_from_buy(out_raw: int) -> list[int]:
    if out_raw <= 0:
        return []
    # probe a few tiny fractions so we detect "can't sell at all"
    ratios = (0.0005, 0.002, 0.01)  # 0.05%, 0.2%, 1%
    return [max(1, int(out_raw * r)) for r in ratios]


# ──────────────────────────────────────────────────────────────────────────────
# Error classification (string-based; tolerant)
# ──────────────────────────────────────────────────────────────────────────────
def _is_auth_error(e: Any) -> bool:
    try:
        sc = getattr(e, "status_code", None)
        if sc in (401, 403):
            return True
    except Exception:
        pass
    s = _short(e, 800).lower()
    return (
        "http 401" in s
        or "status=401" in s
        or "http 403" in s
        or "status=403" in s
        or "unauthorized" in s
        or "forbidden" in s
        or ("api key" in s and ("invalid" in s or "missing" in s))
        or "invalid api key" in s
        or "x-api-key" in s and "required" in s
    )


def _is_rate_limited(e: Any) -> bool:
    try:
        sc = getattr(e, "status_code", None)
        if sc == 429:
            return True
    except Exception:
        pass
    s = _short(e, 800).lower()
    return "http 429" in s or "status=429" in s or "rate limit" in s or "too many requests" in s


def _is_token_not_tradable(e_or_quote: Any) -> bool:
    if isinstance(e_or_quote, dict):
        code = str(e_or_quote.get("errorCode") or e_or_quote.get("code") or "").upper()
        msg = str(e_or_quote.get("error") or e_or_quote.get("message") or "").lower()
        return code == "TOKEN_NOT_TRADABLE" or "not tradable" in msg
    s = _short(e_or_quote, 900).lower()
    return "token_not_tradable" in s or "not tradable" in s


def _is_no_route(e_or_quote: Any) -> bool:
    # Various Jupiter-ish phrasing; we match broadly.
    if isinstance(e_or_quote, dict):
        code = str(e_or_quote.get("errorCode") or e_or_quote.get("code") or "").upper()
        msg = str(e_or_quote.get("error") or e_or_quote.get("message") or "").lower()
        if code in {"NO_ROUTE", "ROUTE_NOT_FOUND", "NO_ROUTES", "NO_ROUTES_FOUND"}:
            return True
        return "no route" in msg or "no routes" in msg or "route not found" in msg
    s = _short(e_or_quote, 900).lower()
    return "no route" in s or "no routes" in s or "route not found" in s or "no_route" in s


# ──────────────────────────────────────────────────────────────────────────────
# Breakers / cooldowns
# ──────────────────────────────────────────────────────────────────────────────
def _cooldown_ok(mint: str) -> bool:
    return time.monotonic() >= _cooldown_until.get(mint, 0.0)


def _cooldown_left(mint: str) -> int:
    return int(max(0.0, _cooldown_until.get(mint, 0.0) - time.monotonic()))


def _fail(mint: str) -> None:
    _fail_streak[mint] = int(_fail_streak.get(mint, 0)) + 1


def _ok(mint: str) -> None:
    _fail_streak.pop(mint, None)


def _note_cooldown(mint: str, *, reason: str) -> None:
    c = _cfg()
    base = int(c.get("cooldown_base_sec") or 60)
    mx = int(c.get("cooldown_max_sec") or base)
    exp = float(c.get("cooldown_exp") or 1.6)
    streak = int(_fail_streak.get(mint, 0))
    ttl = int(min(float(mx), float(base) * (exp ** max(0, streak - 1))))
    ttl = max(5, ttl)
    _cooldown_until[mint] = time.monotonic() + float(ttl)
    _inc("cooldown_set")
    log.every("RG_COOLDOWN_SET", 6, msg="RG_COOLDOWN_SET", mint=mint, reason=reason, ttl=ttl, streak=streak)


def _note_global_fail() -> None:
    c = _cfg()
    win = int(c.get("global_fail_window_sec") or 0)
    mx = int(c.get("global_fail_max") or 0)
    cd = int(c.get("global_cooldown_sec") or 0)
    if win <= 0 or mx <= 0 or cd <= 0:
        return

    now = time.monotonic()
    _global_fail_times.append(now)
    while _global_fail_times and (now - _global_fail_times[0]) > float(win):
        _global_fail_times.popleft()

    if len(_global_fail_times) >= mx:
        global _global_cooldown_until
        _global_cooldown_until = max(_global_cooldown_until, now + float(cd))
        _inc("global_cooldown_set")
        log.every("RG_GLOBAL_COOLDOWN_SET", 6, msg="RG_GLOBAL_COOLDOWN_SET", window_sec=win, max_fail=mx, cooldown_sec=cd)


def _global_cooldown_ok() -> bool:
    return time.monotonic() >= _global_cooldown_until


def _note_auth_fail(err: Any) -> None:
    c = _cfg()
    win = int(c.get("auth_fail_window_sec") or 0)
    mx = int(c.get("auth_fail_max") or 0)
    cd = int(c.get("auth_cooldown_sec") or 0)
    if win <= 0 or mx <= 0 or cd <= 0:
        return

    now = time.monotonic()
    _auth_fail_times.append(now)
    while _auth_fail_times and (now - _auth_fail_times[0]) > float(win):
        _auth_fail_times.popleft()

    if len(_auth_fail_times) >= mx:
        global _auth_cooldown_until
        _auth_cooldown_until = max(_auth_cooldown_until, now + float(cd))
        _inc("auth_cooldown_set")
        log.every("RG_AUTH_COOLDOWN_SET", 6, msg="RG_AUTH_COOLDOWN_SET", cooldown_sec=cd, err=_short(err, 220))


def _auth_cooldown_ok() -> bool:
    return time.monotonic() >= _auth_cooldown_until


# ──────────────────────────────────────────────────────────────────────────────
# Cache helpers (protected by _LOCK)
# ──────────────────────────────────────────────────────────────────────────────
def _trim_cache_if_needed_unlocked() -> None:
    lim = int(_cfg().get("cache_max_keys") or 2000)
    while len(_buy_cache) > lim:
        _buy_cache.popitem(last=False)
    while len(_sell_ok_cache) > lim:
        _sell_ok_cache.popitem(last=False)


async def _get_cached_buy(mint: str, lamports_in: int) -> Optional[Dict[str, Any]]:
    key = (mint, int(lamports_in))
    async with _LOCK:
        rec = _buy_cache.get(key)
        if not rec:
            return None
        ts, q = rec
        ttl = float(_cfg().get("quote_ttl_sec") or 0)
        if (time.monotonic() - ts) <= ttl:
            _buy_cache.move_to_end(key, last=True)
            return dict(q)
        _buy_cache.pop(key, None)
        return None


async def _put_cached_buy(mint: str, lamports_in: int, quote: Dict[str, Any]) -> None:
    key = (mint, int(lamports_in))
    async with _LOCK:
        _buy_cache[key] = (time.monotonic(), dict(quote))
        _buy_cache.move_to_end(key, last=True)
        _trim_cache_if_needed_unlocked()


async def _get_cached_sell_ok(mint: str) -> Optional[bool]:
    async with _LOCK:
        rec = _sell_ok_cache.get(mint)
        if not rec:
            return None
        ts, ok = rec
        ttl = float(_cfg().get("sell_ok_ttl_sec") or 0)
        if (time.monotonic() - ts) <= ttl:
            _sell_ok_cache.move_to_end(mint, last=True)
            return bool(ok)
        _sell_ok_cache.pop(mint, None)
        return None


async def _put_cached_sell_ok(mint: str, ok: bool) -> None:
    async with _LOCK:
        _sell_ok_cache[mint] = (time.monotonic(), bool(ok))
        _sell_ok_cache.move_to_end(mint, last=True)
        _trim_cache_if_needed_unlocked()


# ──────────────────────────────────────────────────────────────────────────────
# Router wrapper helpers (signature drift tolerant)
# ──────────────────────────────────────────────────────────────────────────────
async def _get_buy_quote(mint: str, lamports_in: int) -> Tuple[Optional[Dict[str, Any]], Optional[Any]]:
    tout = float(_cfg().get("buy_quote_timeout_sec") or 0.0)
    fn = _router_fn("get_buy_quote")

    if callable(fn):
        for kwargs in (
            {"mint": mint, "lamports_in": int(lamports_in)},
            {"mint": mint, "amount_raw": int(lamports_in)},
            {"mint": mint, "amount": int(lamports_in)},
        ):
            try:
                res = await _maybe_wait(fn(**kwargs), tout)  # type: ignore[misc]
                if isinstance(res, dict) and res and not _is_errorish_quote(res):
                    return res, None
                # if dict but errorish, return as "quote-like" for classification
                if isinstance(res, dict) and res:
                    return None, res
                return None, RuntimeError("buy_quote_empty_or_error")
            except TypeError:
                continue
            except Exception as e:
                return None, e
        try:
            res = await _maybe_wait(fn(mint, int(lamports_in)), tout)  # type: ignore[misc]
            if isinstance(res, dict) and res and not _is_errorish_quote(res):
                return res, None
            if isinstance(res, dict) and res:
                return None, res
            return None, RuntimeError("buy_quote_empty_or_error")
        except Exception as e:
            return None, e

    for name in ("get_quote_buy", "quote_buy", "buy_quote"):
        fn2 = _router_fn(name)
        if not callable(fn2):
            continue
        try:
            res = await _maybe_wait(fn2(mint=mint, lamports_in=int(lamports_in)), tout)  # type: ignore[misc]
            if isinstance(res, dict) and res and not _is_errorish_quote(res):
                return res, None
            if isinstance(res, dict) and res:
                return None, res
            return None, RuntimeError("buy_quote_empty_or_error")
        except Exception as e:
            return None, e

    return None, RuntimeError("no_buy_quote_fn")


async def _get_sell_quote(mint: str, amount_raw: int) -> Tuple[Optional[Dict[str, Any]], Optional[Any]]:
    tout = float(_cfg().get("sell_quote_timeout_sec") or 0.0)
    fn = _router_fn("get_sell_quote")

    if callable(fn):
        for kwargs in (
            {"mint": mint, "amount_raw": int(amount_raw)},
            {"mint": mint, "amount": int(amount_raw)},
        ):
            try:
                res = await _maybe_wait(fn(**kwargs), tout)  # type: ignore[misc]
                if isinstance(res, dict) and res and not _is_errorish_quote(res):
                    return res, None
                if isinstance(res, dict) and res:
                    return None, res
                return None, RuntimeError("sell_quote_empty_or_error")
            except TypeError:
                continue
            except Exception as e:
                return None, e
        try:
            res = await _maybe_wait(fn(mint, int(amount_raw)), tout)  # type: ignore[misc]
            if isinstance(res, dict) and res and not _is_errorish_quote(res):
                return res, None
            if isinstance(res, dict) and res:
                return None, res
            return None, RuntimeError("sell_quote_empty_or_error")
        except Exception as e:
            return None, e

    for name in ("get_quote_sell", "quote_sell", "sell_quote"):
        fn2 = _router_fn(name)
        if not callable(fn2):
            continue
        try:
            res = await _maybe_wait(fn2(mint=mint, amount_raw=int(amount_raw)), tout)  # type: ignore[misc]
            if isinstance(res, dict) and res and not _is_errorish_quote(res):
                return res, None
            if isinstance(res, dict) and res:
                return None, res
            return None, RuntimeError("sell_quote_empty_or_error")
        except Exception as e:
            return None, e

    return None, RuntimeError("no_sell_quote_fn")


# ──────────────────────────────────────────────────────────────────────────────
# Public API
# ──────────────────────────────────────────────────────────────────────────────
async def preflight(*, mint: str, lamports_in: int) -> Tuple[bool, Optional[Dict[str, Any]]]:
    """
    Decide whether to attempt a Jupiter BUY for `mint` using `lamports_in`.

    Contract:
      (True, quote_dict) -> Jupiter allowed. quote_dict may be {} as "allow/no-prequote".
      (True, None)       -> fallback-only allowed (router must NOT attempt Jupiter).
      (False, None)      -> blocked.
    """
    _inc("calls")
    c = _cfg()

    raw = (mint or "").strip()
    try:
        lamports = int(lamports_in)
    except Exception:
        lamports = -1

    # Guard disabled: MUST NOT force fallback-only. Return {}.
    if not bool(c.get("enabled")):
        _inc("disabled_passthrough")
        return True, {}

    if not raw or lamports <= 0:
        _inc("bad_input")
        log.event("RG_BAD_INPUT", mint=raw, lamports_in=lamports_in)
        return False, None

    now = time.monotonic()

    # Global breaker
    if not _global_cooldown_ok():
        _inc("global_cooldown_block")
        log.every(
            "RG_GLOBAL_COOLDOWN",
            8,
            msg="RG_GLOBAL_COOLDOWN",
            mint=raw,
            secs_left=int(max(0.0, _global_cooldown_until - now)),
        )
        return False, None

    # Auth breaker (401/403)
    if (not _auth_cooldown_ok()) and bool(c.get("auth_blocks_jup")):
        _inc("auth_cooldown_block")
        if bool(c.get("allow_no_buy_route")):
            _inc("allow_fallback")
            log.every(
                "RG_AUTH_COOLDOWN_FALLBACK",
                10,
                msg="RG_AUTH_COOLDOWN_FALLBACK",
                mint=raw,
                secs_left=int(max(0.0, _auth_cooldown_until - now)),
            )
            return True, None
        _inc("block")
        log.event("RG_AUTH_COOLDOWN_BLOCK", mint=raw)
        return False, None

    # Spend guard
    min_lam = int(c.get("min_lamports_in") or 0)
    max_lam = int(c.get("max_lamports_in") or 0)
    if min_lam and lamports < min_lam:
        _inc("blocked_minmax")
        log.every(
            "RG_BLOCK_MIN_SPEND",
            10,
            msg="RG_BLOCK_MIN_SPEND",
            mint=raw,
            lamports_in=lamports,
            min_lamports=min_lam,
        )
        return False, None
    if max_lam and lamports > max_lam:
        _inc("blocked_minmax")
        log.every(
            "RG_BLOCK_MAX_SPEND",
            10,
            msg="RG_BLOCK_MAX_SPEND",
            mint=raw,
            lamports_in=lamports,
            max_lamports=max_lam,
        )
        return False, None

    # Router availability
    if _get_router() is None:
        _inc("router_missing")
        if bool(c.get("allow_fallback_if_router_missing")) and bool(c.get("allow_no_buy_route")):
            _inc("allow_fallback")
            log.every("RG_ROUTER_MISSING_ALLOW_FALLBACK", 15, msg="RG_ROUTER_MISSING_ALLOW_FALLBACK", mint=raw)
            return True, None
        _inc("block")
        log.event("RG_ROUTER_MISSING_BLOCK", mint=raw)
        return False, None

    # Normalize mint
    raw_is_mint = _valid_pubkey_str(raw)
    norm = _normalize_to_mint(raw) or raw

    # allow/block lists
    allow_mints = c.get("allow_mints") or set()
    block_mints = c.get("block_mints") or set()
    if bool(c.get("use_allowlist")) and norm not in allow_mints:
        _inc("blocked_allowlist")
        log.every("RG_BLOCK_ALLOWLIST", 12, msg="RG_BLOCK_ALLOWLIST", mint=norm, raw=raw)
        return False, None
    if (norm in block_mints) or (raw in block_mints):
        _inc("blocked_blocklist")
        log.every("RG_BLOCK_BLOCKLIST", 12, msg="RG_BLOCK_BLOCKLIST", mint=norm, raw=raw)
        return False, None

    # raw isn't mint + strictness => fallback-only
    if (not raw_is_mint) and bool(c.get("force_fallback_if_raw_not_mint")):
        _inc("fallback_raw_not_mint")
        log.every("RG_FORCE_FALLBACK_RAW_NOT_MINT", 12, msg="RG_FORCE_FALLBACK_RAW_NOT_MINT", raw=raw, norm=norm)
        return True, None

    # normalized still invalid => fallback-only or block
    if not _valid_pubkey_str(norm):
        _inc("fallback_norm_invalid")
        if bool(c.get("allow_no_buy_route")):
            _inc("allow_fallback")
            log.every("RG_FALLBACK_NORM_INVALID", 12, msg="RG_FALLBACK_NORM_INVALID", raw=raw, norm=norm)
            return True, None
        _inc("block")
        log.event("RG_BLOCK_NORM_INVALID", raw=raw, norm=norm)
        return False, None

    # Per-mint cooldown
    if not _cooldown_ok(norm):
        _inc("cooldown_block")
        log.every(
            "RG_COOLDOWN",
            8,
            msg="RG_COOLDOWN",
            mint=norm,
            secs_left=_cooldown_left(norm),
            streak=int(_fail_streak.get(norm, 0)),
        )
        return False, None

    key = (norm, lamports)
    debug = bool(c.get("debug"))
    trace = norm in (c.get("trace_mints") or set())

    async def _compute() -> Tuple[bool, Optional[Dict[str, Any]]]:
        # 1) BUY quote (cache first)
        q_buy = await _get_cached_buy(norm, lamports)
        q_err: Optional[Any] = None

        if q_buy is not None:
            _inc("buy_cache_hit")
        else:
            _inc("buy_cache_miss")
            q_buy, q_err = await _get_buy_quote(norm, lamports)
            if isinstance(q_buy, dict) and q_buy and not _is_errorish_quote(q_buy):
                await _put_cached_buy(norm, lamports, q_buy)
            else:
                q_buy = None

        if not q_buy:
            _inc("buy_quote_error")
            _fail(norm)
            _note_global_fail()

            # classify known failure types
            if _is_rate_limited(q_err):
                _inc("rate_limited")
                log.every("RG_JUP_RATE_LIMIT", 8, msg="RG_JUP_RATE_LIMIT", mint=norm, err=_short(q_err, 240))

            if _is_auth_error(q_err):
                _inc("auth_fail")
                _note_auth_fail(q_err)
                log.every("RG_JUP_AUTH_FAIL", 8, msg="RG_JUP_AUTH_FAIL", mint=norm, err=_short(q_err, 240))
                if bool(c.get("allow_no_buy_route")):
                    _inc("allow_fallback")
                    return True, None
                _note_cooldown(norm, reason="auth_fail")
                _inc("block")
                return False, None

            if _is_token_not_tradable(q_err):
                _inc("token_not_tradable")
                _note_cooldown(norm, reason="token_not_tradable")
                log.every("RG_TOKEN_NOT_TRADABLE", 8, msg="RG_TOKEN_NOT_TRADABLE", mint=norm)
                if bool(c.get("allow_no_buy_route")):
                    _inc("allow_fallback")
                    return True, None
                _inc("block")
                return False, None

            if _is_no_route(q_err):
                _inc("no_route")
                _note_cooldown(norm, reason="no_route")
                log.every("RG_NO_ROUTE", 10, msg="RG_NO_ROUTE", mint=norm)
                if bool(c.get("allow_no_buy_route")):
                    _inc("allow_fallback")
                    return True, None
                _inc("block")
                return False, None

            # “optimistic” mode: allow router to try Jupiter anyway (no prequote)
            if bool(c.get("optimistic_buy_err")):
                log.every("RG_BUY_QUOTE_ERR_OPTIMISTIC", 10, msg="RG_BUY_QUOTE_ERR_OPTIMISTIC", mint=norm, lamports_in=lamports)
                _inc("allow_jup")
                return True, {}

            # otherwise: fallback-only if allowed, else hard block
            if not bool(c.get("allow_no_buy_route")):
                _note_cooldown(norm, reason="no_buy_quote")
                _inc("block")
                log.event("RG_NO_BUY_QUOTE_BLOCK", mint=norm, lamports_in=lamports, err=_short(q_err, 240) if q_err else "")
                return False, None

            _inc("allow_fallback")
            log.every("RG_NO_BUY_QUOTE_ALLOW_FALLBACK", 12, msg="RG_NO_BUY_QUOTE_ALLOW_FALLBACK", mint=norm)
            return True, None

        # Buy quote quality checks
        out_raw = _out_amount_raw(q_buy)
        min_out = int(c.get("min_out_raw") or 0)
        if min_out and out_raw < min_out:
            _fail(norm)
            _note_global_fail()
            _note_cooldown(norm, reason="min_out")
            if not bool(c.get("allow_no_buy_route")):
                _inc("block")
                log.event("RG_MIN_OUT_BLOCK", mint=norm, out_raw=out_raw, min_out_raw=min_out)
                return False, None
            _inc("allow_fallback")
            log.every("RG_MIN_OUT_ALLOW_FALLBACK", 12, msg="RG_MIN_OUT_ALLOW_FALLBACK", mint=norm, out_raw=out_raw, min_out_raw=min_out)
            return True, None

        ibps = _impact_bps(q_buy)
        max_impact = int(c.get("max_impact_bps") or 0)
        if max_impact and ibps > max_impact:
            _fail(norm)
            _note_global_fail()
            _note_cooldown(norm, reason="impact")
            _inc("block_impact")
            _inc("block")
            log.event("RG_BLOCK_IMPACT", mint=norm, impact_bps=ibps, max_bps=max_impact)
            return False, None

        hops = _hops(q_buy)
        max_hops = int(c.get("max_hops") or 0)
        if max_hops and hops > max_hops:
            _fail(norm)
            _note_global_fail()
            _note_cooldown(norm, reason="hops")
            _inc("block_hops")
            _inc("block")
            log.event("RG_BLOCK_HOPS", mint=norm, hops=hops, max_hops=max_hops)
            return False, None

        label_reason = _route_label_blocks(q_buy)
        if label_reason:
            _fail(norm)
            _note_global_fail()
            _note_cooldown(norm, reason="route_label")
            _inc("block_route_label")
            _inc("block")
            log.event("RG_BLOCK_ROUTE_LABEL", mint=norm, reason=label_reason, labels=_route_labels(q_buy)[:8])
            return False, None

        if debug or trace:
            log.event(
                "RG_BUY_QUOTE_OK",
                mint=norm,
                lamports_in=lamports,
                out_raw=out_raw,
                impact_bps=ibps,
                hops=hops,
                labels=_route_labels(q_buy)[:6],
                raw=(raw if raw != norm else ""),
            )
        else:
            log.every("RG_BUY_QUOTE_OK", 10, msg="RG_BUY_QUOTE_OK", mint=norm, impact_bps=ibps, hops=hops)

        # 2) SELL route requirement (token->SOL)
        if bool(c.get("require_sell")):
            if bool(c.get("allow_sell_skip_if_tiny_out")) and out_raw <= int(c.get("sell_skip_tiny_out_raw") or 0):
                log.every("RG_SELL_SKIP_TINY_OUT", 20, msg="RG_SELL_SKIP_TINY_OUT", mint=norm, out_raw=out_raw)
            else:
                cached = await _get_cached_sell_ok(norm)
                if cached is True:
                    _inc("sell_cache_hit")
                elif cached is False:
                    _inc("sell_cache_hit")
                    _fail(norm)
                    _note_global_fail()
                    _note_cooldown(norm, reason="sell_cached_fail")
                    _inc("sell_fail")
                    _inc("block")
                    log.event("RG_SELL_ROUTE_CACHED_FAIL", mint=norm)
                    return False, None
                else:
                    _inc("sell_cache_miss")
                    ladder_base = list(c.get("sell_probe_ladder") or [])
                    ladder_dyn = _dynamic_sell_probes_from_buy(out_raw) if bool(c.get("sell_dynamic_probes")) else []
                    ladder = _ladder_with_floor(ladder_dyn + ladder_base + [int(c.get("sell_probe_floor_raw") or 1)])

                    sell_ok = False
                    last_err: Optional[str] = None

                    for probe_raw in ladder:
                        q_sell, s_err = await _get_sell_quote(norm, int(probe_raw))
                        if not isinstance(q_sell, dict) or not q_sell or _is_errorish_quote(q_sell):
                            if _is_auth_error(s_err):
                                _inc("auth_fail")
                                _note_auth_fail(s_err)
                                last_err = f"auth_fail: {_short(s_err, 200)}"
                                break
                            if _is_rate_limited(s_err):
                                _inc("rate_limited")
                            if _is_token_not_tradable(s_err):
                                _inc("token_not_tradable")
                                last_err = "token_not_tradable"
                                break
                            if _is_no_route(s_err):
                                _inc("no_route")
                                last_err = "no_route"
                                break
                            last_err = _short(s_err, 220) if s_err else "sell_quote_empty"
                            _inc("sell_quote_error")
                            continue

                        out_lam = 0
                        with contextlib.suppress(Exception):
                            out_lam = int(q_sell.get("outAmount", 0) or 0)

                        if out_lam > 0:
                            sell_ok = True
                            if debug or trace:
                                log.event("RG_SELL_ROUTE_OK", mint=norm, probe_raw=int(probe_raw), lamports_out=out_lam)
                            else:
                                log.every("RG_SELL_ROUTE_OK", 20, msg="RG_SELL_ROUTE_OK", mint=norm)
                            break

                        last_err = f"sell_zero_out probe_raw={probe_raw}"

                    await _put_cached_sell_ok(norm, sell_ok)

                    if not sell_ok:
                        _inc("sell_fail")
                        _fail(norm)
                        _note_global_fail()
                        if bool(c.get("optimistic_sell_err")):
                            log.every("RG_SELL_FAIL_OPTIMISTIC", 12, msg="RG_SELL_FAIL_OPTIMISTIC", mint=norm, err=last_err)
                        else:
                            _note_cooldown(norm, reason="sell_fail")
                            _inc("block")
                            log.event("RG_SELL_ROUTE_FAIL", mint=norm, err=last_err, ladder=ladder[:8])
                            return False, None

        _ok(norm)
        _inc("allow_jup")

        q_out = dict(q_buy)
        q_out["__rg"] = {"mint": norm, "impact_bps": ibps, "hops": hops, "ts": time.time()}
        return True, q_out

    # Inflight coalescing to prevent quote thrashing
    if bool(c.get("coalesce_inflight")):
        mx_inf = int(c.get("max_inflight") or 0)
        if mx_inf and len(_inflight) >= mx_inf:
            _inc("inflight_overflow")
            log.every("RG_INFLIGHT_OVERFLOW", 5, msg="RG_INFLIGHT_OVERFLOW", mint=norm, inflight=len(_inflight), max_inflight=mx_inf)
            if bool(c.get("allow_no_buy_route")):
                _inc("allow_fallback")
                return True, None
            _inc("block")
            return False, None

        t = _inflight.get(key)
        if t is not None and not t.done():
            return await asyncio.shield(t) if bool(c.get("inflight_shield")) else await t

        async def _runner() -> Tuple[bool, Optional[Dict[str, Any]]]:
            try:
                return await _compute()
            finally:
                _inflight.pop(key, None)

        task: "asyncio.Task[Tuple[bool, Optional[Dict[str, Any]]]]" = asyncio.create_task(_runner())
        _inflight[key] = task
        return await asyncio.shield(task) if bool(c.get("inflight_shield")) else await task

    return await _compute()


__all__ = ["preflight", "clear_cache", "stats"]
