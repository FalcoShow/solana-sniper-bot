from __future__ import annotations

"""
utils_pkg/routeguard.py — V3+++ (God-Tier Overhaul, Phase 0–7) — CANONICAL / LOCK-IN
===================================================================================

RouteGuard is the low-level gatekeeper for the Jupiter/lightning path.

It answers ONE question deterministically:

    "Should the router attempt a Jupiter BUY route for this mint with this spend?"

Public API (LOCKED)
-------------------
  async def preflight(*, mint: str, lamports_in: int) -> tuple[bool, dict[str, Any] | None]
  def clear_cache(*, cancel_inflight: bool = False) -> None
  def stats() -> dict[str, Any]

Return contract (router MUST follow)
------------------------------------
  (True, quote_dict) -> Jupiter allowed. quote_dict may be {} meaning "allow Jupiter but no prequote".
  (True, None)       -> fallback-only allowed (router MUST NOT attempt Jupiter).
  (False, None)      -> blocked outright.

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
- Async functions never block the event loop (no time.sleep, no requests).
- Signature drift tolerant vs router quote helpers.
- Caching + inflight coalescing + breakers to prevent thrashing.
- Safe defaults: prefer fallback-only over hard block when configured.

E741 guarantee
--------------
No ambiguous variable names: no `l`, `I`, `O` identifiers anywhere.
"""

import asyncio
import contextlib
import inspect
import os
import time
from collections import OrderedDict, deque
from collections.abc import Iterable
from dataclasses import dataclass, field
from typing import Any

# ──────────────────────────────────────────────────────────────────────────────
# Logging (safe wrappers; never raise; no getattr-with-constant patterns)
# ──────────────────────────────────────────────────────────────────────────────
try:
    from logging_setup import get_logger  # type: ignore

    _LOG = get_logger(__name__)  # type: ignore
except Exception:  # pragma: no cover
    import logging

    _LOG = logging.getLogger(__name__)


_EVERY_STORE: dict[str, float] = {}


def _log_event(name: str, **fields: Any) -> None:
    try:
        if hasattr(_LOG, "event") and callable(_LOG.event):  # type: ignore[attr-defined]
            _LOG.event(name, **fields)  # type: ignore[attr-defined]
        else:
            _LOG.info("%s %s", name, fields)
    except Exception:
        return None


def _log_every(key: str, seconds: int = 10, msg: str | None = None, **fields: Any) -> None:
    try:
        if hasattr(_LOG, "every") and callable(_LOG.every):  # type: ignore[attr-defined]
            _LOG.every(key, seconds, msg=msg, **fields)  # type: ignore[attr-defined]
            return

        now_mono = time.monotonic()
        last_mono = float(_EVERY_STORE.get(key, 0.0))
        sec = float(max(1, int(seconds)))
        if (now_mono - last_mono) >= sec:
            _EVERY_STORE[key] = now_mono
            _log_event(msg or key, **fields)
    except Exception:
        return None


def _short(x: Any, limit: int = 300) -> str:
    try:
        s = str(x)
        return s if len(s) <= limit else (s[:limit] + "…")
    except Exception:
        return "<?>"


# ──────────────────────────────────────────────────────────────────────────────
# Lazy imports (avoid cycles + no network at import)
# ──────────────────────────────────────────────────────────────────────────────
_ROUTER_MOD: Any | None = None
_MINT_NORMALIZER: Any | None = None


def _get_router() -> Any | None:
    global _ROUTER_MOD
    if _ROUTER_MOD is not None:
        return _ROUTER_MOD
    try:
        from utils_pkg import router as r  # type: ignore

        _ROUTER_MOD = r
        return _ROUTER_MOD
    except Exception:
        _ROUTER_MOD = None
        return None


def _router_fn(name: str) -> Any | None:
    r = _get_router()
    return getattr(r, name, None) if r else None


def _get_mint_normalizer() -> Any | None:
    """Prefer utils_pkg.mints.normalize_mint if present."""
    global _MINT_NORMALIZER
    if _MINT_NORMALIZER is not None:
        return _MINT_NORMALIZER
    try:
        from utils_pkg.mints import normalize_mint as nm  # type: ignore

        _MINT_NORMALIZER = nm
        return _MINT_NORMALIZER
    except Exception:
        _MINT_NORMALIZER = None
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


def _envs(name: str, default: str = "") -> str:
    raw = os.getenv(name)
    return (raw if raw is not None else default).strip()


def _envb(name: str, default: bool = False) -> bool:
    raw = _envs(name, "")
    if raw == "":
        return bool(default)
    low = raw.lower().strip()
    if low in _TRUE:
        return True
    if low in _FALSE:
        return False
    return bool(default)


def _envi(name: str, default: int) -> int:
    try:
        return int(float(_envs(name, str(default))))
    except Exception:
        return int(default)


def _envf(name: str, default: float) -> float:
    try:
        return float(_envs(name, str(default)))
    except Exception:
        return float(default)


def _parse_set_csv(raw: str) -> set[str]:
    out: set[str] = set()
    for part in (raw or "").split(","):
        p = part.strip()
        if p:
            out.add(p)
    return out


def _parse_raw_ladder(raw: str) -> list[int]:
    """Parse ladder like '50k,200k,1m' -> [50000, 200000, 1000000]."""
    out: list[int] = []
    compact = (raw or "").replace(" ", "")
    for part in compact.split(","):
        if not part:
            continue
        mult = 1
        token = part.lower()
        if token.endswith("k"):
            mult = 1_000
            token = token[:-1]
        elif token.endswith("m"):
            mult = 1_000_000
            token = token[:-1]
        try:
            n = int(float(token) * mult)
        except Exception:
            continue
        if n > 0:
            out.append(n)
    return out


# ──────────────────────────────────────────────────────────────────────────────
# Config (refreshable TTL)
# ──────────────────────────────────────────────────────────────────────────────
_CFG_TTL_SEC = max(0.25, _envf("ROUTEGUARD_REFRESH_SEC", 2.0))
_cfg_at_mono: float = 0.0
_cfg_cache: dict[str, Any] = {}


def _cfg() -> dict[str, Any]:
    """Read env into a dict, cached for a short TTL to avoid env thrash."""
    global _cfg_at_mono, _cfg_cache
    now_mono = time.monotonic()
    if _cfg_cache and (now_mono - _cfg_at_mono) < _CFG_TTL_SEC:
        return _cfg_cache

    cfg: dict[str, Any] = {}

    # Master enable
    cfg["enabled"] = _envb("ROUTEGUARD_ENABLED", True)

    # Buy behavior
    cfg["allow_no_buy_route"] = _envb(
        "ROUTEGUARD_ALLOW_NO_ROUTE",
        _envb("ROUTEGUARD_ALLOW_NO_BUY_ROUTE", True),
    )
    cfg["optimistic_buy_err"] = _envb("ROUTEGUARD_OPTIMISTIC_ON_BUY_QUOTE_ERROR", True)

    # Sell route requirement
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
    cfg["cooldown_base_sec"] = max(
        5,
        _envi("ROUTEGUARD_NOROUTE_COOLDOWN_SEC", _envi("NOROUTE_COOLDOWN_SEC", 300)),
    )
    cfg["cooldown_max_sec"] = max(
        cfg["cooldown_base_sec"],
        _envi("ROUTEGUARD_NOROUTE_COOLDOWN_MAX_SEC", int(cfg["cooldown_base_sec"] * 3)),
    )
    cfg["cooldown_exp"] = max(1.0, _envf("ROUTEGUARD_COOLDOWN_EXP", 1.6))

    # Global breaker
    cfg["global_fail_window_sec"] = max(0, _envi("ROUTEGUARD_GLOBAL_FAIL_WINDOW_SEC", 30))
    cfg["global_fail_max"] = max(0, _envi("ROUTEGUARD_GLOBAL_FAIL_MAX", 0))  # 0 disables
    cfg["global_cooldown_sec"] = max(0, _envi("ROUTEGUARD_GLOBAL_COOLDOWN_SEC", 20))

    # Auth breaker (401/403)
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

    # Route labels allow/block (substring match)
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
    cfg["allow_fallback_if_router_missing"] = _envb(
        "ROUTEGUARD_ALLOW_FALLBACK_IF_ROUTER_MISSING", True
    )
    cfg["buy_quote_timeout_sec"] = max(0.0, _envf("ROUTEGUARD_BUY_QUOTE_TIMEOUT_SEC", 4.0))
    cfg["sell_quote_timeout_sec"] = max(0.0, _envf("ROUTEGUARD_SELL_QUOTE_TIMEOUT_SEC", 4.0))

    # Mint allow/block
    cfg["allow_mints"] = _parse_set_csv(_envs("ROUTEGUARD_ALLOW_MINTS", ""))
    cfg["block_mints"] = _parse_set_csv(_envs("ROUTEGUARD_BLOCK_MINTS", ""))
    cfg["use_allowlist"] = bool(cfg["allow_mints"])

    # Strict raw mint handling
    cfg["force_fallback_if_raw_not_mint"] = _envb(
        "ROUTEGUARD_FORCE_FALLBACK_IF_RAW_NOT_MINT", False
    )

    # Debug
    cfg["debug"] = _envb("ROUTEGUARD_DEBUG", False)
    cfg["trace_mints"] = _parse_set_csv(_envs("ROUTEGUARD_TRACE_MINTS", ""))

    _cfg_cache = cfg
    _cfg_at_mono = now_mono
    return cfg


# ──────────────────────────────────────────────────────────────────────────────
# Mint validation / normalization
# ──────────────────────────────────────────────────────────────────────────────
_BASE58 = set("123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz")


def _valid_pubkey_str(text: str) -> bool:
    s = (text or "").strip()
    if not s:
        return False

    if _SoldersPubkey is not None:
        try:
            _SoldersPubkey.from_string(s)  # type: ignore[attr-defined]
            return True
        except Exception:
            return False

    if len(s) < 32 or len(s) > 50:
        return False
    return all(ch in _BASE58 for ch in s)


def _normalize_to_mint(raw: str) -> str:
    raw2 = (raw or "").strip()
    if not raw2:
        return ""

    nm = _get_mint_normalizer()
    if callable(nm):
        with contextlib.suppress(Exception):
            out = str(nm(raw2)).strip()  # type: ignore[misc]
            if out:
                return out

    fn = _router_fn("normalize_pump_id_to_mint")
    if callable(fn):
        with contextlib.suppress(Exception):
            out2 = str(fn(raw2)).strip()
            return out2 or raw2

    return raw2


# ──────────────────────────────────────────────────────────────────────────────
# Quote parsing helpers (Jupiter-ish tolerant)
# ──────────────────────────────────────────────────────────────────────────────
async def _maybe_await(x: Any) -> Any:
    if inspect.isawaitable(x):
        return await x
    return x


async def _maybe_wait(x: Any, timeout_s: float) -> Any:
    if not timeout_s or timeout_s <= 0:
        return await _maybe_await(x)
    return await asyncio.wait_for(_maybe_await(x), timeout=timeout_s)


def _is_errorish_quote(q: dict[str, Any]) -> bool:
    if not isinstance(q, dict):
        return True
    if q.get("ok") is False:
        return True
    if q.get("error") or q.get("errors"):
        return True
    code = q.get("errorCode") or q.get("code")
    if isinstance(code, str) and code.strip():
        return True
    data = q.get("data")
    if isinstance(data, dict):
        return _is_errorish_quote(data)
    return False


def _impact_bps(q: dict[str, Any]) -> int:
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


def _out_amount_raw(q: dict[str, Any]) -> int:
    for key in ("outAmount", "out_amount", "out_amount_raw"):
        with contextlib.suppress(Exception):
            v = q.get(key)
            if v is not None:
                return int(v)
    data = q.get("data")
    if isinstance(data, dict):
        return _out_amount_raw(data)
    return 0


def _route_plan(q: dict[str, Any]) -> list[dict[str, Any]]:
    rp = q.get("routePlan")
    if isinstance(rp, list):
        return [x for x in rp if isinstance(x, dict)]
    data = q.get("data")
    if isinstance(data, dict) and isinstance(data.get("routePlan"), list):
        return [x for x in data["routePlan"] if isinstance(x, dict)]
    return []


def _hops(q: dict[str, Any]) -> int:
    return len(_route_plan(q))


def _route_labels(q: dict[str, Any]) -> list[str]:
    out: list[str] = []
    for step in _route_plan(q):
        swap_info = step.get("swapInfo") if isinstance(step, dict) else None
        if isinstance(swap_info, dict):
            for k in ("label", "amm", "dex", "market"):
                v = swap_info.get(k)
                if isinstance(v, str) and v.strip():
                    out.append(v.strip())

    seen: set[str] = set()
    uniq: list[str] = []
    for txt in out:
        low = txt.lower()
        if low in seen:
            continue
        seen.add(low)
        uniq.append(txt)
    return uniq


def _route_label_blocks(q: dict[str, Any]) -> str | None:
    c = _cfg()
    labels = _route_labels(q)

    allow: set[str] = c.get("allow_route_labels") or set()
    block: set[str] = c.get("block_route_labels") or set()
    use_allow = bool(c.get("use_allow_route_labels"))

    if use_allow:
        if labels:
            allowed = False
            for label in labels:
                low = label.lower()
                for needle in allow:
                    if needle and needle in low:
                        allowed = True
                        break
                if allowed:
                    break
            if not allowed:
                return f"no_allowed_labels labels={labels[:6]}"
        else:
            return "no_labels_allowlist_enabled"

    if block and labels:
        for label in labels:
            low = label.lower()
            for blocked in block:
                if blocked and blocked in low:
                    return f"blocked_label={blocked} label={label}"

    return None


def _ladder_with_floor(raws: Iterable[int]) -> list[int]:
    c = _cfg()
    floor = int(c.get("sell_probe_floor_raw") or 1)
    cap = int(c.get("sell_probe_max_raw") or 0)

    out: list[int] = []
    for item in raws:
        with contextlib.suppress(Exception):
            v = int(item)
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
    ratios = (0.0005, 0.002, 0.01)  # 0.05%, 0.2%, 1%
    return [max(1, int(out_raw * r)) for r in ratios]


# ──────────────────────────────────────────────────────────────────────────────
# Error classification (tolerant, string-based; no getattr-with-constant use)
# ──────────────────────────────────────────────────────────────────────────────
def _is_timeout(err: Any) -> bool:
    return isinstance(err, asyncio.TimeoutError)


def _status_code(err: Any) -> int | None:
    try:
        if hasattr(err, "status_code"):
            sc = err.status_code  # type: ignore[attr-defined]
            if sc is None:
                return None
            return int(sc)
    except Exception:
        return None
    return None


def _is_auth_error(err: Any) -> bool:
    sc = _status_code(err)
    if sc in (401, 403):
        return True
    s = _short(err, 900).lower()
    return (
        "http 401" in s
        or "status=401" in s
        or "http 403" in s
        or "status=403" in s
        or "unauthorized" in s
        or "forbidden" in s
        or ("api key" in s and ("invalid" in s or "missing" in s))
        or "invalid api key" in s
        or ("x-api-key" in s and "required" in s)
    )


def _is_rate_limited(err: Any) -> bool:
    sc = _status_code(err)
    if sc == 429:
        return True
    s = _short(err, 900).lower()
    return "http 429" in s or "status=429" in s or "rate limit" in s or "too many requests" in s


def _is_token_not_tradable(err_or_quote: Any) -> bool:
    if isinstance(err_or_quote, dict):
        code = str(err_or_quote.get("errorCode") or err_or_quote.get("code") or "").upper()
        msg = str(err_or_quote.get("error") or err_or_quote.get("message") or "").lower()
        return code == "TOKEN_NOT_TRADABLE" or "not tradable" in msg
    s = _short(err_or_quote, 900).lower()
    return "token_not_tradable" in s or "not tradable" in s


def _is_no_route(err_or_quote: Any) -> bool:
    if isinstance(err_or_quote, dict):
        code = str(err_or_quote.get("errorCode") or err_or_quote.get("code") or "").upper()
        msg = str(err_or_quote.get("error") or err_or_quote.get("message") or "").lower()
        if code in {"NO_ROUTE", "ROUTE_NOT_FOUND", "NO_ROUTES", "NO_ROUTES_FOUND"}:
            return True
        return "no route" in msg or "no routes" in msg or "route not found" in msg
    s = _short(err_or_quote, 900).lower()
    return "no route" in s or "no routes" in s or "route not found" in s or "no_route" in s


# ──────────────────────────────────────────────────────────────────────────────
# Internal state (single owner)
# ──────────────────────────────────────────────────────────────────────────────
@dataclass
class _State:
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)

    buy_cache: OrderedDict[tuple[str, int], tuple[float, dict[str, Any]]] = field(
        default_factory=OrderedDict
    )
    sell_ok_cache: OrderedDict[str, tuple[float, bool]] = field(default_factory=OrderedDict)

    cooldown_until: dict[str, float] = field(default_factory=dict)
    fail_streak: dict[str, int] = field(default_factory=dict)

    inflight: dict[tuple[str, int], asyncio.Task[tuple[bool, dict[str, Any] | None]]] = field(
        default_factory=dict
    )

    global_fail_times: deque[float] = field(default_factory=deque)
    global_cooldown_until: float = 0.0

    auth_fail_times: deque[float] = field(default_factory=deque)
    auth_cooldown_until: float = 0.0

    counters: dict[str, int] = field(
        default_factory=lambda: {
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
            "timeout": 0,
        }
    )

    def inc(self, key: str, n: int = 1) -> None:
        self.counters[key] = int(self.counters.get(key, 0)) + int(n)

    def _trim_unlocked(self) -> None:
        lim = int(_cfg().get("cache_max_keys") or 2000)
        while len(self.buy_cache) > lim:
            self.buy_cache.popitem(last=False)
        while len(self.sell_ok_cache) > lim:
            self.sell_ok_cache.popitem(last=False)

    def cooldown_ok(self, mint: str) -> bool:
        return time.monotonic() >= self.cooldown_until.get(mint, 0.0)

    def cooldown_left(self, mint: str) -> int:
        return int(max(0.0, self.cooldown_until.get(mint, 0.0) - time.monotonic()))

    def fail(self, mint: str) -> None:
        self.fail_streak[mint] = int(self.fail_streak.get(mint, 0)) + 1

    def ok(self, mint: str) -> None:
        self.fail_streak.pop(mint, None)

    def note_cooldown(self, mint: str, *, reason: str) -> None:
        c = _cfg()
        base = int(c.get("cooldown_base_sec") or 60)
        mx = int(c.get("cooldown_max_sec") or base)
        exp = float(c.get("cooldown_exp") or 1.6)
        streak = int(self.fail_streak.get(mint, 0))
        ttl = int(min(float(mx), float(base) * (exp ** max(0, streak - 1))))
        ttl = max(5, ttl)
        self.cooldown_until[mint] = time.monotonic() + float(ttl)
        self.inc("cooldown_set")
        _log_every(
            "RG_COOLDOWN_SET",
            6,
            msg="RG_COOLDOWN_SET",
            mint=mint,
            reason=reason,
            ttl=ttl,
            streak=streak,
        )

    def note_global_fail(self) -> None:
        c = _cfg()
        win = int(c.get("global_fail_window_sec") or 0)
        mx = int(c.get("global_fail_max") or 0)
        cd = int(c.get("global_cooldown_sec") or 0)
        if win <= 0 or mx <= 0 or cd <= 0:
            return
        now_mono = time.monotonic()
        self.global_fail_times.append(now_mono)
        while self.global_fail_times and (now_mono - self.global_fail_times[0]) > float(win):
            self.global_fail_times.popleft()
        if len(self.global_fail_times) >= mx:
            self.global_cooldown_until = max(self.global_cooldown_until, now_mono + float(cd))
            self.inc("global_cooldown_set")
            _log_every(
                "RG_GLOBAL_COOLDOWN_SET",
                6,
                msg="RG_GLOBAL_COOLDOWN_SET",
                window_sec=win,
                max_fail=mx,
                cooldown_sec=cd,
            )

    def global_cooldown_ok(self) -> bool:
        return time.monotonic() >= self.global_cooldown_until

    def note_auth_fail(self, err: Any) -> None:
        c = _cfg()
        win = int(c.get("auth_fail_window_sec") or 0)
        mx = int(c.get("auth_fail_max") or 0)
        cd = int(c.get("auth_cooldown_sec") or 0)
        if win <= 0 or mx <= 0 or cd <= 0:
            return
        now_mono = time.monotonic()
        self.auth_fail_times.append(now_mono)
        while self.auth_fail_times and (now_mono - self.auth_fail_times[0]) > float(win):
            self.auth_fail_times.popleft()
        if len(self.auth_fail_times) >= mx:
            self.auth_cooldown_until = max(self.auth_cooldown_until, now_mono + float(cd))
            self.inc("auth_cooldown_set")
            _log_every(
                "RG_AUTH_COOLDOWN_SET",
                6,
                msg="RG_AUTH_COOLDOWN_SET",
                cooldown_sec=cd,
                err=_short(err, 220),
            )

    def auth_cooldown_ok(self) -> bool:
        return time.monotonic() >= self.auth_cooldown_until


_STATE = _State()


# ──────────────────────────────────────────────────────────────────────────────
# Cache helpers
# ──────────────────────────────────────────────────────────────────────────────
async def _get_cached_buy(mint: str, lamports_in: int) -> dict[str, Any] | None:
    key = (mint, int(lamports_in))
    async with _STATE.lock:
        rec = _STATE.buy_cache.get(key)
        if not rec:
            return None
        ts_mono, quote = rec
        ttl = float(_cfg().get("quote_ttl_sec") or 0)
        if (time.monotonic() - ts_mono) <= ttl:
            _STATE.buy_cache.move_to_end(key, last=True)
            return dict(quote)
        _STATE.buy_cache.pop(key, None)
        return None


async def _put_cached_buy(mint: str, lamports_in: int, quote: dict[str, Any]) -> None:
    key = (mint, int(lamports_in))
    async with _STATE.lock:
        _STATE.buy_cache[key] = (time.monotonic(), dict(quote))
        _STATE.buy_cache.move_to_end(key, last=True)
        _STATE._trim_unlocked()


async def _get_cached_sell_ok(mint: str) -> bool | None:
    async with _STATE.lock:
        rec = _STATE.sell_ok_cache.get(mint)
        if not rec:
            return None
        ts_mono, ok = rec
        ttl = float(_cfg().get("sell_ok_ttl_sec") or 0)
        if (time.monotonic() - ts_mono) <= ttl:
            _STATE.sell_ok_cache.move_to_end(mint, last=True)
            return bool(ok)
        _STATE.sell_ok_cache.pop(mint, None)
        return None


async def _put_cached_sell_ok(mint: str, ok: bool) -> None:
    async with _STATE.lock:
        _STATE.sell_ok_cache[mint] = (time.monotonic(), bool(ok))
        _STATE.sell_ok_cache.move_to_end(mint, last=True)
        _STATE._trim_unlocked()


# ──────────────────────────────────────────────────────────────────────────────
# Router wrapper helpers (signature drift tolerant)
# ──────────────────────────────────────────────────────────────────────────────
async def _get_buy_quote(mint: str, lamports_in: int) -> tuple[dict[str, Any] | None, Any | None]:
    timeout_s = float(_cfg().get("buy_quote_timeout_sec") or 0.0)
    fn = _router_fn("get_buy_quote")

    if callable(fn):
        for kwargs in (
            {"mint": mint, "lamports_in": int(lamports_in)},
            {"mint": mint, "amount_raw": int(lamports_in)},
            {"mint": mint, "amount": int(lamports_in)},
        ):
            try:
                res = await _maybe_wait(fn(**kwargs), timeout_s)  # type: ignore[misc]
                if isinstance(res, dict) and res and not _is_errorish_quote(res):
                    return res, None
                if isinstance(res, dict) and res:
                    return None, res
                return None, RuntimeError("buy_quote_empty_or_error")
            except TypeError:
                continue
            except Exception as err:
                return None, err

        try:
            res2 = await _maybe_wait(fn(mint, int(lamports_in)), timeout_s)  # type: ignore[misc]
            if isinstance(res2, dict) and res2 and not _is_errorish_quote(res2):
                return res2, None
            if isinstance(res2, dict) and res2:
                return None, res2
            return None, RuntimeError("buy_quote_empty_or_error")
        except Exception as err:
            return None, err

    for name in ("get_quote_buy", "quote_buy", "buy_quote"):
        fn2 = _router_fn(name)
        if not callable(fn2):
            continue
        try:
            res3 = await _maybe_wait(fn2(mint=mint, lamports_in=int(lamports_in)), timeout_s)  # type: ignore[misc]
            if isinstance(res3, dict) and res3 and not _is_errorish_quote(res3):
                return res3, None
            if isinstance(res3, dict) and res3:
                return None, res3
            return None, RuntimeError("buy_quote_empty_or_error")
        except Exception as err:
            return None, err

    return None, RuntimeError("no_buy_quote_fn")


async def _get_sell_quote(mint: str, amount_raw: int) -> tuple[dict[str, Any] | None, Any | None]:
    timeout_s = float(_cfg().get("sell_quote_timeout_sec") or 0.0)
    fn = _router_fn("get_sell_quote")

    if callable(fn):
        for kwargs in (
            {"mint": mint, "amount_raw": int(amount_raw)},
            {"mint": mint, "amount": int(amount_raw)},
        ):
            try:
                res = await _maybe_wait(fn(**kwargs), timeout_s)  # type: ignore[misc]
                if isinstance(res, dict) and res and not _is_errorish_quote(res):
                    return res, None
                if isinstance(res, dict) and res:
                    return None, res
                return None, RuntimeError("sell_quote_empty_or_error")
            except TypeError:
                continue
            except Exception as err:
                return None, err

        try:
            res2 = await _maybe_wait(fn(mint, int(amount_raw)), timeout_s)  # type: ignore[misc]
            if isinstance(res2, dict) and res2 and not _is_errorish_quote(res2):
                return res2, None
            if isinstance(res2, dict) and res2:
                return None, res2
            return None, RuntimeError("sell_quote_empty_or_error")
        except Exception as err:
            return None, err

    for name in ("get_quote_sell", "quote_sell", "sell_quote"):
        fn2 = _router_fn(name)
        if not callable(fn2):
            continue
        try:
            res3 = await _maybe_wait(fn2(mint=mint, amount_raw=int(amount_raw)), timeout_s)  # type: ignore[misc]
            if isinstance(res3, dict) and res3 and not _is_errorish_quote(res3):
                return res3, None
            if isinstance(res3, dict) and res3:
                return None, res3
            return None, RuntimeError("sell_quote_empty_or_error")
        except Exception as err:
            return None, err

    return None, RuntimeError("no_sell_quote_fn")


# ──────────────────────────────────────────────────────────────────────────────
# Public API
# ──────────────────────────────────────────────────────────────────────────────
async def preflight(*, mint: str, lamports_in: int) -> tuple[bool, dict[str, Any] | None]:
    """
    Decide whether to attempt a Jupiter BUY for `mint` using `lamports_in`.

    Contract:
      (True, quote_dict) -> Jupiter allowed. quote_dict may be {} as "allow/no-prequote".
      (True, None)       -> fallback-only allowed (router must NOT attempt Jupiter).
      (False, None)      -> blocked.
    """
    _STATE.inc("calls")
    c = _cfg()

    raw = (mint or "").strip()
    try:
        lamports = int(lamports_in)
    except Exception:
        lamports = -1

    # Guard disabled: MUST NOT force fallback-only. Return {}.
    if not bool(c.get("enabled")):
        _STATE.inc("disabled_passthrough")
        return True, {}

    if not raw or lamports <= 0:
        _STATE.inc("bad_input")
        _log_event("RG_BAD_INPUT", mint=raw, lamports_in=lamports_in)
        return False, None

    now_mono = time.monotonic()

    # Global breaker: prefer fallback-only if configured; else hard block.
    if not _STATE.global_cooldown_ok():
        _STATE.inc("global_cooldown_block")
        left = int(max(0.0, _STATE.global_cooldown_until - now_mono))
        if bool(c.get("allow_no_buy_route")):
            _STATE.inc("allow_fallback")
            _log_every(
                "RG_GLOBAL_COOLDOWN_FALLBACK",
                10,
                msg="RG_GLOBAL_COOLDOWN_FALLBACK",
                secs_left=left,
                mint=raw,
            )
            return True, None
        _log_every(
            "RG_GLOBAL_COOLDOWN_BLOCK", 10, msg="RG_GLOBAL_COOLDOWN_BLOCK", secs_left=left, mint=raw
        )
        return False, None

    # Auth breaker
    if (not _STATE.auth_cooldown_ok()) and bool(c.get("auth_blocks_jup")):
        _STATE.inc("auth_cooldown_block")
        left = int(max(0.0, _STATE.auth_cooldown_until - now_mono))
        if bool(c.get("allow_no_buy_route")):
            _STATE.inc("allow_fallback")
            _log_every(
                "RG_AUTH_COOLDOWN_FALLBACK",
                10,
                msg="RG_AUTH_COOLDOWN_FALLBACK",
                secs_left=left,
                mint=raw,
            )
            return True, None
        _STATE.inc("block")
        _log_event("RG_AUTH_COOLDOWN_BLOCK", mint=raw)
        return False, None

    # Spend guard
    min_lam = int(c.get("min_lamports_in") or 0)
    max_lam = int(c.get("max_lamports_in") or 0)
    if min_lam and lamports < min_lam:
        _STATE.inc("blocked_minmax")
        _log_every(
            "RG_BLOCK_MIN_SPEND",
            10,
            msg="RG_BLOCK_MIN_SPEND",
            mint=raw,
            lamports_in=lamports,
            min_lamports=min_lam,
        )
        return False, None
    if max_lam and lamports > max_lam:
        _STATE.inc("blocked_minmax")
        _log_every(
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
        _STATE.inc("router_missing")
        if bool(c.get("allow_fallback_if_router_missing")) and bool(c.get("allow_no_buy_route")):
            _STATE.inc("allow_fallback")
            _log_every(
                "RG_ROUTER_MISSING_ALLOW_FALLBACK",
                15,
                msg="RG_ROUTER_MISSING_ALLOW_FALLBACK",
                mint=raw,
            )
            return True, None
        _STATE.inc("block")
        _log_event("RG_ROUTER_MISSING_BLOCK", mint=raw)
        return False, None

    # Normalize mint
    raw_is_mint = _valid_pubkey_str(raw)
    norm = _normalize_to_mint(raw) or raw

    # allow/block lists
    allow_mints = c.get("allow_mints") or set()
    block_mints = c.get("block_mints") or set()

    if bool(c.get("use_allowlist")) and norm not in allow_mints:
        _STATE.inc("blocked_allowlist")
        _log_every("RG_BLOCK_ALLOWLIST", 12, msg="RG_BLOCK_ALLOWLIST", mint=norm, raw=raw)
        return False, None

    if (norm in block_mints) or (raw in block_mints):
        _STATE.inc("blocked_blocklist")
        _log_every("RG_BLOCK_BLOCKLIST", 12, msg="RG_BLOCK_BLOCKLIST", mint=norm, raw=raw)
        return False, None

    # raw isn't mint + strictness => fallback-only
    if (not raw_is_mint) and bool(c.get("force_fallback_if_raw_not_mint")):
        _STATE.inc("fallback_raw_not_mint")
        _log_every(
            "RG_FORCE_FALLBACK_RAW_NOT_MINT",
            12,
            msg="RG_FORCE_FALLBACK_RAW_NOT_MINT",
            raw=raw,
            norm=norm,
        )
        return True, None

    # normalized still invalid => fallback-only or block
    if not _valid_pubkey_str(norm):
        _STATE.inc("fallback_norm_invalid")
        if bool(c.get("allow_no_buy_route")):
            _STATE.inc("allow_fallback")
            _log_every(
                "RG_FALLBACK_NORM_INVALID", 12, msg="RG_FALLBACK_NORM_INVALID", raw=raw, norm=norm
            )
            return True, None
        _STATE.inc("block")
        _log_event("RG_BLOCK_NORM_INVALID", raw=raw, norm=norm)
        return False, None

    # Per-mint cooldown (hard block; cooldown means "stop touching this mint")
    if not _STATE.cooldown_ok(norm):
        _STATE.inc("cooldown_block")
        _log_every(
            "RG_COOLDOWN",
            8,
            msg="RG_COOLDOWN",
            mint=norm,
            secs_left=_STATE.cooldown_left(norm),
            streak=int(_STATE.fail_streak.get(norm, 0)),
        )
        return False, None

    key = (norm, lamports)
    debug = bool(c.get("debug"))
    trace = norm in (c.get("trace_mints") or set())

    async def _compute() -> tuple[bool, dict[str, Any] | None]:
        # 1) BUY quote (cache first)
        q_buy = await _get_cached_buy(norm, lamports)
        q_err: Any | None = None

        if q_buy is not None:
            _STATE.inc("buy_cache_hit")
        else:
            _STATE.inc("buy_cache_miss")
            q_buy, q_err = await _get_buy_quote(norm, lamports)
            if isinstance(q_buy, dict) and q_buy and not _is_errorish_quote(q_buy):
                await _put_cached_buy(norm, lamports, q_buy)
            else:
                q_buy = None

        if not q_buy:
            _STATE.inc("buy_quote_error")
            _STATE.fail(norm)
            _STATE.note_global_fail()

            if _is_timeout(q_err):
                _STATE.inc("timeout")
                _log_every("RG_JUP_TIMEOUT", 8, msg="RG_JUP_TIMEOUT", mint=norm)

            if _is_rate_limited(q_err):
                _STATE.inc("rate_limited")
                _log_every(
                    "RG_JUP_RATE_LIMIT",
                    8,
                    msg="RG_JUP_RATE_LIMIT",
                    mint=norm,
                    err=_short(q_err, 240),
                )

            if _is_auth_error(q_err):
                _STATE.inc("auth_fail")
                _STATE.note_auth_fail(q_err)
                _log_every(
                    "RG_JUP_AUTH_FAIL", 8, msg="RG_JUP_AUTH_FAIL", mint=norm, err=_short(q_err, 240)
                )
                if bool(c.get("allow_no_buy_route")):
                    _STATE.inc("allow_fallback")
                    return True, None
                _STATE.note_cooldown(norm, reason="auth_fail")
                _STATE.inc("block")
                return False, None

            if _is_token_not_tradable(q_err):
                _STATE.inc("token_not_tradable")
                _STATE.note_cooldown(norm, reason="token_not_tradable")
                _log_every("RG_TOKEN_NOT_TRADABLE", 8, msg="RG_TOKEN_NOT_TRADABLE", mint=norm)
                if bool(c.get("allow_no_buy_route")):
                    _STATE.inc("allow_fallback")
                    return True, None
                _STATE.inc("block")
                return False, None

            if _is_no_route(q_err):
                _STATE.inc("no_route")
                _STATE.note_cooldown(norm, reason="no_route")
                _log_every("RG_NO_ROUTE", 10, msg="RG_NO_ROUTE", mint=norm)
                if bool(c.get("allow_no_buy_route")):
                    _STATE.inc("allow_fallback")
                    return True, None
                _STATE.inc("block")
                return False, None

            # Optimistic: allow router to try Jupiter anyway (no prequote)
            if bool(c.get("optimistic_buy_err")):
                _log_every(
                    "RG_BUY_QUOTE_ERR_OPTIMISTIC",
                    10,
                    msg="RG_BUY_QUOTE_ERR_OPTIMISTIC",
                    mint=norm,
                    lamports_in=lamports,
                )
                _STATE.inc("allow_jup")
                return True, {}

            # Otherwise: fallback-only if allowed, else hard block
            if not bool(c.get("allow_no_buy_route")):
                _STATE.note_cooldown(norm, reason="no_buy_quote")
                _STATE.inc("block")
                _log_event(
                    "RG_NO_BUY_QUOTE_BLOCK",
                    mint=norm,
                    lamports_in=lamports,
                    err=_short(q_err, 240) if q_err else "",
                )
                return False, None

            _STATE.inc("allow_fallback")
            _log_every(
                "RG_NO_BUY_QUOTE_ALLOW_FALLBACK",
                12,
                msg="RG_NO_BUY_QUOTE_ALLOW_FALLBACK",
                mint=norm,
            )
            return True, None

        # Buy quote quality checks
        out_raw = _out_amount_raw(q_buy)
        min_out = int(c.get("min_out_raw") or 0)
        if min_out and out_raw < min_out:
            _STATE.fail(norm)
            _STATE.note_global_fail()
            _STATE.note_cooldown(norm, reason="min_out")
            if not bool(c.get("allow_no_buy_route")):
                _STATE.inc("block")
                _log_event("RG_MIN_OUT_BLOCK", mint=norm, out_raw=out_raw, min_out_raw=min_out)
                return False, None
            _STATE.inc("allow_fallback")
            _log_every(
                "RG_MIN_OUT_ALLOW_FALLBACK",
                12,
                msg="RG_MIN_OUT_ALLOW_FALLBACK",
                mint=norm,
                out_raw=out_raw,
                min_out_raw=min_out,
            )
            return True, None

        ibps = _impact_bps(q_buy)
        max_impact = int(c.get("max_impact_bps") or 0)
        if max_impact and ibps > max_impact:
            _STATE.fail(norm)
            _STATE.note_global_fail()
            _STATE.note_cooldown(norm, reason="impact")
            _STATE.inc("block_impact")
            _STATE.inc("block")
            _log_event("RG_BLOCK_IMPACT", mint=norm, impact_bps=ibps, max_bps=max_impact)
            return False, None

        hop_count = _hops(q_buy)
        max_hops = int(c.get("max_hops") or 0)
        if max_hops and hop_count > max_hops:
            _STATE.fail(norm)
            _STATE.note_global_fail()
            _STATE.note_cooldown(norm, reason="hops")
            _STATE.inc("block_hops")
            _STATE.inc("block")
            _log_event("RG_BLOCK_HOPS", mint=norm, hops=hop_count, max_hops=max_hops)
            return False, None

        label_reason = _route_label_blocks(q_buy)
        if label_reason:
            _STATE.fail(norm)
            _STATE.note_global_fail()
            _STATE.note_cooldown(norm, reason="route_label")
            _STATE.inc("block_route_label")
            _STATE.inc("block")
            _log_event(
                "RG_BLOCK_ROUTE_LABEL",
                mint=norm,
                reason=label_reason,
                labels=_route_labels(q_buy)[:8],
            )
            return False, None

        if debug or trace:
            _log_event(
                "RG_BUY_QUOTE_OK",
                mint=norm,
                lamports_in=lamports,
                out_raw=out_raw,
                impact_bps=ibps,
                hops=hop_count,
                labels=_route_labels(q_buy)[:6],
                raw=(raw if raw != norm else ""),
            )
        else:
            _log_every(
                "RG_BUY_QUOTE_OK",
                10,
                msg="RG_BUY_QUOTE_OK",
                mint=norm,
                impact_bps=ibps,
                hops=hop_count,
            )

        # 2) SELL route requirement
        if bool(c.get("require_sell")):
            if bool(c.get("allow_sell_skip_if_tiny_out")) and out_raw <= int(
                c.get("sell_skip_tiny_out_raw") or 0
            ):
                _log_every(
                    "RG_SELL_SKIP_TINY_OUT",
                    20,
                    msg="RG_SELL_SKIP_TINY_OUT",
                    mint=norm,
                    out_raw=out_raw,
                )
            else:
                cached_ok = await _get_cached_sell_ok(norm)
                if cached_ok is True:
                    _STATE.inc("sell_cache_hit")
                elif cached_ok is False:
                    _STATE.inc("sell_cache_hit")
                    _STATE.fail(norm)
                    _STATE.note_global_fail()
                    _STATE.note_cooldown(norm, reason="sell_cached_fail")
                    _STATE.inc("sell_fail")
                    _STATE.inc("block")
                    _log_event("RG_SELL_ROUTE_CACHED_FAIL", mint=norm)
                    return False, None
                else:
                    _STATE.inc("sell_cache_miss")
                    ladder_base = list(c.get("sell_probe_ladder") or [])
                    ladder_dyn = (
                        _dynamic_sell_probes_from_buy(out_raw)
                        if bool(c.get("sell_dynamic_probes"))
                        else []
                    )
                    ladder = _ladder_with_floor(
                        ladder_dyn + ladder_base + [int(c.get("sell_probe_floor_raw") or 1)]
                    )

                    sell_ok = False
                    last_err: str | None = None

                    for probe_raw in ladder:
                        q_sell, s_err = await _get_sell_quote(norm, int(probe_raw))
                        if not isinstance(q_sell, dict) or not q_sell or _is_errorish_quote(q_sell):
                            if _is_timeout(s_err):
                                _STATE.inc("timeout")
                            if _is_auth_error(s_err):
                                _STATE.inc("auth_fail")
                                _STATE.note_auth_fail(s_err)
                                last_err = f"auth_fail: {_short(s_err, 200)}"
                                break
                            if _is_rate_limited(s_err):
                                _STATE.inc("rate_limited")
                            if _is_token_not_tradable(s_err):
                                _STATE.inc("token_not_tradable")
                                last_err = "token_not_tradable"
                                break
                            if _is_no_route(s_err):
                                _STATE.inc("no_route")
                                last_err = "no_route"
                                break
                            last_err = _short(s_err, 220) if s_err else "sell_quote_empty"
                            _STATE.inc("sell_quote_error")
                            continue

                        out_lamports = 0
                        with contextlib.suppress(Exception):
                            out_lamports = int(q_sell.get("outAmount", 0) or 0)

                        if out_lamports > 0:
                            sell_ok = True
                            if debug or trace:
                                _log_event(
                                    "RG_SELL_ROUTE_OK",
                                    mint=norm,
                                    probe_raw=int(probe_raw),
                                    lamports_out=out_lamports,
                                )
                            else:
                                _log_every(
                                    "RG_SELL_ROUTE_OK", 20, msg="RG_SELL_ROUTE_OK", mint=norm
                                )
                            break

                        last_err = f"sell_zero_out probe_raw={probe_raw}"

                    await _put_cached_sell_ok(norm, sell_ok)

                    if not sell_ok:
                        _STATE.inc("sell_fail")
                        _STATE.fail(norm)
                        _STATE.note_global_fail()
                        if bool(c.get("optimistic_sell_err")):
                            _log_every(
                                "RG_SELL_FAIL_OPTIMISTIC",
                                12,
                                msg="RG_SELL_FAIL_OPTIMISTIC",
                                mint=norm,
                                err=last_err,
                            )
                        else:
                            _STATE.note_cooldown(norm, reason="sell_fail")
                            _STATE.inc("block")
                            _log_event(
                                "RG_SELL_ROUTE_FAIL", mint=norm, err=last_err, ladder=ladder[:8]
                            )
                            return False, None

        _STATE.ok(norm)
        _STATE.inc("allow_jup")

        q_out = dict(q_buy)
        q_out["__rg"] = {"mint": norm, "impact_bps": ibps, "hops": hop_count, "ts": time.time()}
        return True, q_out

    # Inflight coalescing
    if bool(c.get("coalesce_inflight")):
        max_inflight = int(c.get("max_inflight") or 0)

        async with _STATE.lock:
            if max_inflight and len(_STATE.inflight) >= max_inflight:
                _STATE.inc("inflight_overflow")
                _log_every(
                    "RG_INFLIGHT_OVERFLOW",
                    5,
                    msg="RG_INFLIGHT_OVERFLOW",
                    mint=norm,
                    inflight=len(_STATE.inflight),
                    max_inflight=max_inflight,
                )
                if bool(c.get("allow_no_buy_route")):
                    _STATE.inc("allow_fallback")
                    return True, None
                _STATE.inc("block")
                return False, None

            existing = _STATE.inflight.get(key)
            if existing is not None and not existing.done():
                task = existing
            else:
                task = None

            if task is None:

                async def _runner_task() -> tuple[bool, dict[str, Any] | None]:
                    try:
                        return await _compute()
                    finally:
                        async with _STATE.lock:
                            _STATE.inflight.pop(key, None)

                created: asyncio.Task[tuple[bool, dict[str, Any] | None]] = asyncio.create_task(
                    _runner_task()
                )
                _STATE.inflight[key] = created
                task = created

        return await asyncio.shield(task) if bool(c.get("inflight_shield")) else await task

    return await _compute()


async def _clear_cache_async(*, cancel_inflight: bool = False) -> None:
    async with _STATE.lock:
        _STATE.buy_cache.clear()
        _STATE.sell_ok_cache.clear()
        _STATE.cooldown_until.clear()
        _STATE.fail_streak.clear()
        _STATE.global_fail_times.clear()
        _STATE.auth_fail_times.clear()
        _STATE.global_cooldown_until = 0.0
        _STATE.auth_cooldown_until = 0.0
        if cancel_inflight:
            for task in list(_STATE.inflight.values()):
                with contextlib.suppress(Exception):
                    task.cancel()
        _STATE.inflight.clear()


def clear_cache(*, cancel_inflight: bool = False) -> None:
    """
    Clear all caches/breakers. Optionally cancel inflight quote tasks.

    Safe from sync contexts:
      - If an event loop is running: schedules an async clear.
      - If no loop: clears immediately (best-effort).
    """
    try:
        loop = asyncio.get_running_loop()
    except Exception:
        loop = None

    if loop and loop.is_running():
        with contextlib.suppress(Exception):
            loop.create_task(_clear_cache_async(cancel_inflight=cancel_inflight))
        return

    with contextlib.suppress(Exception):
        _STATE.buy_cache.clear()
        _STATE.sell_ok_cache.clear()
        _STATE.cooldown_until.clear()
        _STATE.fail_streak.clear()
        _STATE.global_fail_times.clear()
        _STATE.auth_fail_times.clear()
        _STATE.global_cooldown_until = 0.0
        _STATE.auth_cooldown_until = 0.0
        if cancel_inflight:
            for task in list(_STATE.inflight.values()):
                with contextlib.suppress(Exception):
                    task.cancel()
        _STATE.inflight.clear()


def stats() -> dict[str, Any]:
    c = _cfg()
    now_mono = time.monotonic()
    return {
        **_STATE.counters,
        "buy_cache": len(_STATE.buy_cache),
        "sell_ok_cache": len(_STATE.sell_ok_cache),
        "cooldowns": len(_STATE.cooldown_until),
        "fail_streaks": len(_STATE.fail_streak),
        "inflight": len(_STATE.inflight),
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
        "global_cooldown_active": int(now_mono < _STATE.global_cooldown_until),
        "global_cooldown_left": int(max(0.0, _STATE.global_cooldown_until - now_mono)),
        "auth_cooldown_active": int(now_mono < _STATE.auth_cooldown_until),
        "auth_cooldown_left": int(max(0.0, _STATE.auth_cooldown_until - now_mono)),
    }


__all__ = ["clear_cache", "preflight", "stats"]
