from __future__ import annotations

"""
utils_pkg/routeguard.py — V3+++ (God-Tier Overhaul, Phase 4) — CANONICAL / LOCK-IN
===================================================================================

RouteGuard is the low-level, deterministic gatekeeper for Jupiter BUY attempts.

It answers ONE question:

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
- Never raise unexpectedly from preflight() — fail closed or fallback-only based on config.

E741 guarantee
--------------
No ambiguous variable names: no `l`, `I`, `O` identifiers anywhere.
"""

import asyncio
import contextlib
import importlib
import inspect
import os
import sys
import time
from collections import OrderedDict, deque
from collections.abc import Iterable
from dataclasses import dataclass, field
from typing import Any, Final, Protocol, cast

# =============================================================================
# Logging (Phase A: EventLogger contract; NO monkey patching; import-safe)
# =============================================================================


class EventLogger(Protocol):
    def debug(self, msg: str, *args: Any, **kwargs: Any) -> None: ...
    def info(self, msg: str, *args: Any, **kwargs: Any) -> None: ...
    def warning(self, msg: str, *args: Any, **kwargs: Any) -> None: ...
    def error(self, msg: str, *args: Any, **kwargs: Any) -> None: ...
    def exception(self, msg: str, *args: Any, **kwargs: Any) -> None: ...
    def log(self, level: int, msg: str, *args: Any, **kwargs: Any) -> None: ...


class _EventCapable(EventLogger, Protocol):
    def event(self, name: str, **fields: Any) -> None: ...


class _EveryCapable(EventLogger, Protocol):
    def every(self, key: str, n: int, *, msg: str | None = None, **fields: Any) -> None: ...


def get_logger(name: str | None = None) -> EventLogger:
    """
    Single signature. Always returns an EventLogger-compatible object.

    Priority:
      1) utils_pkg.log_compat.get_event_logger / get_logger
      2) logging_setup.get_logger
      3) stdlib logging.getLogger (basicConfig if needed)
    """
    nm = (name or "utils_pkg.routeguard").strip() or "utils_pkg.routeguard"

    with contextlib.suppress(Exception):
        from utils_pkg.log_compat import get_event_logger  # type: ignore

        return cast(EventLogger, get_event_logger(nm))

    with contextlib.suppress(Exception):
        from utils_pkg.log_compat import get_logger as gl  # type: ignore

        return cast(EventLogger, gl(nm))

    with contextlib.suppress(Exception):
        from logging_setup import get_logger as gl2  # type: ignore

        return cast(EventLogger, gl2(nm))

    import logging

    lg = logging.getLogger(nm)
    if not lg.handlers:
        logging.basicConfig(level=logging.INFO)
    return cast(EventLogger, lg)


_LOG: EventLogger = get_logger(__name__)
_EVERY_STORE: dict[str, float] = {}

# =============================================================================
# Redaction (avoid leaking DSNs/tokens in error strings)
# =============================================================================

_REDACTOR: Any | None = None
with contextlib.suppress(Exception):
    from utils_pkg.log_redaction import redact_text as _rt  # type: ignore

    _REDACTOR = _rt  # type: ignore[assignment]

_TRUE: Final[set[str]] = {"1", "true", "yes", "y", "on", "t"}
_FALSE: Final[set[str]] = {"0", "false", "no", "n", "off", "f"}


def _redact_text(value: Any) -> str:
    try:
        if _REDACTOR is not None:
            return str(_REDACTOR(str(value)))
    except Exception:
        pass
    return str(value)


def _short(value: Any, limit: int = 300) -> str:
    try:
        text = _redact_text(value)
        return text if len(text) <= limit else (text[:limit] + "…")
    except Exception:
        return "<?>"


def _log_event(name: str, **fields: Any) -> None:
    """
    Never raises; uses .event if available else info.
    """
    try:
        evt = getattr(_LOG, "event", None)
        if callable(evt):
            safe = {str(k): _short(v, 520) for k, v in fields.items()}
            cast(_EventCapable, _LOG).event(str(name), **safe)
            return
    except Exception:
        pass

    with contextlib.suppress(Exception):
        safe2 = {str(k): _short(v, 520) for k, v in fields.items()}
        _LOG.info("%s %s", _short(name, 120), safe2)


def _log_every(key: str, seconds: int = 10, msg: str | None = None, **fields: Any) -> None:
    """
    Rate-limited log helper. Never raises.
    """
    try:
        every_fn = getattr(_LOG, "every", None)
        if callable(every_fn):
            safe = {str(k): _short(v, 520) for k, v in fields.items()}
            cast(_EveryCapable, _LOG).every(str(key), int(max(1, seconds)), msg=msg, **safe)
            return

        now_mono = time.monotonic()
        last_mono = float(_EVERY_STORE.get(key, 0.0))
        window = float(max(1, int(seconds)))
        if (now_mono - last_mono) >= window:
            _EVERY_STORE[key] = now_mono
            _log_event(msg or key, **fields)
    except Exception:
        return


# =============================================================================
# Optional pubkey validation via solders (no network; lazy import)
# =============================================================================

_SOLDERS_PUBKEY_CLS: Any | None = None
_SOLDERS_PUBKEY_LOADED = False


def _get_solders_pubkey_cls() -> Any | None:
    global _SOLDERS_PUBKEY_CLS, _SOLDERS_PUBKEY_LOADED
    if _SOLDERS_PUBKEY_LOADED:
        return _SOLDERS_PUBKEY_CLS
    _SOLDERS_PUBKEY_LOADED = True
    try:  # pragma: no cover
        mod = importlib.import_module("solders.pubkey")
        cls = getattr(mod, "Pubkey", None)
        _SOLDERS_PUBKEY_CLS = cls if cls is not None else None
    except Exception:  # pragma: no cover
        _SOLDERS_PUBKEY_CLS = None
    return _SOLDERS_PUBKEY_CLS


# =============================================================================
# Env helpers
# =============================================================================


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
        piece = part.strip()
        if piece:
            out.add(piece)
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
            number = int(float(token) * mult)
        except Exception:
            continue
        if number > 0:
            out.append(number)
    return out


# =============================================================================
# Config (refreshable TTL)
# =============================================================================

_CFG_TTL_SEC = max(0.25, _envf("ROUTEGUARD_REFRESH_SEC", 2.0))
_cfg_at_mono = 0.0
_cfg_cache: dict[str, Any] = {}


def _cfg() -> dict[str, Any]:
    """Read env into a dict, cached for short TTL to avoid env thrash."""
    global _cfg_at_mono, _cfg_cache
    now_mono = time.monotonic()
    if _cfg_cache and (now_mono - _cfg_at_mono) < _CFG_TTL_SEC:
        return _cfg_cache

    cfg: dict[str, Any] = {}

    cfg["enabled"] = _envb("ROUTEGUARD_ENABLED", True)

    cfg["allow_no_buy_route"] = _envb(
        "ROUTEGUARD_ALLOW_NO_ROUTE",
        _envb("ROUTEGUARD_ALLOW_NO_BUY_ROUTE", True),
    )
    cfg["optimistic_buy_err"] = _envb("ROUTEGUARD_OPTIMISTIC_ON_BUY_QUOTE_ERROR", True)

    cfg["require_sell"] = _envb("ROUTEGUARD_REQUIRE_SELL", True)
    cfg["optimistic_sell_err"] = _envb("ROUTEGUARD_OPTIMISTIC_ON_SELL_QUOTE_ERROR", False)

    cfg["slippage_bps"] = max(
        1,
        _envi(
            "ROUTEGUARD_SLIPPAGE_BPS",
            _envi("ROUTER_DEFAULT_SLIPPAGE_BPS", _envi("JUP_DEFAULT_SLIPPAGE_BPS", 200)),
        ),
    )

    cfg["only_direct"] = _envb("ROUTEGUARD_ONLY_DIRECT", False)

    cfg["coalesce_inflight"] = _envb("ROUTEGUARD_COALESCE_INFLIGHT", True)
    cfg["inflight_shield"] = _envb("ROUTEGUARD_INFLIGHT_SHIELD", True)
    cfg["max_inflight"] = max(0, _envi("ROUTEGUARD_MAX_INFLIGHT", 0))  # 0 disables cap

    cfg["quote_ttl_sec"] = max(
        1,
        _envi(
            "ROUTEGUARD_BUY_QUOTE_TTL_SEC",
            _envi("ROUTER_QUOTE_TTL_SEC", _envi("PF_PRICE_TTL_SEC", 6)),
        ),
    )
    cfg["sell_ok_ttl_sec"] = max(2, _envi("ROUTEGUARD_SELL_OK_TTL_SEC", 20))
    cfg["cache_max_keys"] = max(100, _envi("ROUTEGUARD_CACHE_MAX_KEYS", 2000))

    cfg["cooldown_base_sec"] = max(
        5,
        _envi("ROUTEGUARD_NOROUTE_COOLDOWN_SEC", _envi("NOROUTE_COOLDOWN_SEC", 300)),
    )
    cfg["cooldown_max_sec"] = max(
        cfg["cooldown_base_sec"],
        _envi("ROUTEGUARD_NOROUTE_COOLDOWN_MAX_SEC", int(cfg["cooldown_base_sec"] * 3)),
    )
    cfg["cooldown_exp"] = max(1.0, _envf("ROUTEGUARD_COOLDOWN_EXP", 1.6))

    cfg["global_fail_window_sec"] = max(0, _envi("ROUTEGUARD_GLOBAL_FAIL_WINDOW_SEC", 30))
    cfg["global_fail_max"] = max(0, _envi("ROUTEGUARD_GLOBAL_FAIL_MAX", 0))  # 0 disables
    cfg["global_cooldown_sec"] = max(0, _envi("ROUTEGUARD_GLOBAL_COOLDOWN_SEC", 20))

    cfg["auth_fail_window_sec"] = max(0, _envi("ROUTEGUARD_AUTH_FAIL_WINDOW_SEC", 60))
    cfg["auth_fail_max"] = max(0, _envi("ROUTEGUARD_AUTH_FAIL_MAX", 2))  # 0 disables
    cfg["auth_cooldown_sec"] = max(0, _envi("ROUTEGUARD_AUTH_COOLDOWN_SEC", 600))
    cfg["auth_blocks_jup"] = _envb("ROUTEGUARD_AUTH_BLOCKS_JUP", True)

    cfg["max_impact_bps"] = max(0, _envi("ROUTEGUARD_MAX_IMPACT_BPS", 1500))
    cfg["max_hops"] = max(0, _envi("ROUTEGUARD_MAX_HOPS", 0))  # 0 disables
    cfg["min_lamports_in"] = max(0, _envi("ROUTEGUARD_MIN_LAMPORTS_IN", 0))
    cfg["max_lamports_in"] = max(0, _envi("ROUTEGUARD_MAX_LAMPORTS_IN", 0))  # 0 disables
    cfg["min_out_raw"] = max(0, _envi("ROUTEGUARD_MIN_OUT_RAW", 0))

    allow_labels = {
        value.lower() for value in _parse_set_csv(_envs("ROUTEGUARD_ALLOW_ROUTE_LABELS", ""))
    }
    block_labels = {
        value.lower() for value in _parse_set_csv(_envs("ROUTEGUARD_BLOCK_ROUTE_LABELS", ""))
    }
    cfg["allow_route_labels"] = allow_labels
    cfg["block_route_labels"] = block_labels
    cfg["use_allow_route_labels"] = bool(allow_labels)

    ladder_env = _envs("ROUTEGUARD_SELL_PROBE_LADDER", "50k,500k,5m")
    cfg["sell_probe_ladder"] = _parse_raw_ladder(ladder_env) or [50_000, 500_000, 5_000_000]
    cfg["sell_probe_floor_raw"] = max(1, _envi("ROUTEGUARD_SELL_PROBE_FLOOR_RAW", 50_000))
    cfg["sell_probe_max_raw"] = max(0, _envi("ROUTEGUARD_SELL_PROBE_MAX_RAW", 0))  # 0 disables
    cfg["sell_dynamic_probes"] = _envb("ROUTEGUARD_SELL_DYNAMIC_PROBES", True)

    cfg["allow_sell_skip_if_tiny_out"] = _envb("ROUTEGUARD_ALLOW_SELL_SKIP_IF_TINY_OUT", False)
    cfg["sell_skip_tiny_out_raw"] = max(0, _envi("ROUTEGUARD_SELL_SKIP_TINY_OUT_RAW", 25_000))

    cfg["allow_fallback_if_router_missing"] = _envb(
        "ROUTEGUARD_ALLOW_FALLBACK_IF_ROUTER_MISSING", True
    )
    cfg["buy_quote_timeout_sec"] = max(0.0, _envf("ROUTEGUARD_BUY_QUOTE_TIMEOUT_SEC", 4.0))
    cfg["sell_quote_timeout_sec"] = max(0.0, _envf("ROUTEGUARD_SELL_QUOTE_TIMEOUT_SEC", 4.0))

    cfg["allow_mints"] = _parse_set_csv(_envs("ROUTEGUARD_ALLOW_MINTS", ""))
    cfg["block_mints"] = _parse_set_csv(_envs("ROUTEGUARD_BLOCK_MINTS", ""))
    cfg["use_allowlist"] = bool(cfg["allow_mints"])

    cfg["force_fallback_if_raw_not_mint"] = _envb(
        "ROUTEGUARD_FORCE_FALLBACK_IF_RAW_NOT_MINT", False
    )

    cfg["debug"] = _envb("ROUTEGUARD_DEBUG", False)
    cfg["trace_mints"] = _parse_set_csv(_envs("ROUTEGUARD_TRACE_MINTS", ""))

    _cfg_cache = cfg
    _cfg_at_mono = now_mono
    return cfg


# =============================================================================
# Lazy imports (avoid cycles + no network at import)
# =============================================================================

_ROUTER_MOD: Any | None = None
_MINT_NORMALIZER: Any | None = None


def _get_router() -> Any | None:
    """Get utils_pkg.router safely (cycle-tolerant)."""
    global _ROUTER_MOD
    if _ROUTER_MOD is not None:
        return _ROUTER_MOD

    existing = sys.modules.get("utils_pkg.router")
    if existing is not None:
        _ROUTER_MOD = existing
        return _ROUTER_MOD

    try:
        from utils_pkg import router as router_mod  # type: ignore

        _ROUTER_MOD = router_mod
        return _ROUTER_MOD
    except Exception:
        _ROUTER_MOD = None
        return None


def _router_fn(name: str) -> Any | None:
    router_mod = _get_router()
    return getattr(router_mod, name, None) if router_mod else None


def _get_mint_normalizer() -> Any | None:
    """Prefer utils_pkg.mints.normalize_mint if present."""
    global _MINT_NORMALIZER
    if _MINT_NORMALIZER is not None:
        return _MINT_NORMALIZER
    try:
        from utils_pkg.mints import normalize_mint as normalize_mint  # type: ignore

        _MINT_NORMALIZER = normalize_mint
        return _MINT_NORMALIZER
    except Exception:
        _MINT_NORMALIZER = None
        return None


# =============================================================================
# Mint validation / normalization
# =============================================================================

_BASE58: Final[set[str]] = set("123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz")


def _valid_pubkey_str(text: str) -> bool:
    value = (text or "").strip()
    if not value:
        return False

    pubkey_cls = _get_solders_pubkey_cls()
    if pubkey_cls is not None:
        # Resolve dynamically so mypy doesn't “learn” a weird stub type that implies NoReturn.
        from_string = getattr(pubkey_cls, "from_string", None)
        if callable(from_string):
            try:
                return bool(cast(Any, from_string)(value))
            except Exception:
                return False

    if len(value) < 32 or len(value) > 50:
        return False
    return all(ch in _BASE58 for ch in value)


def _normalize_to_mint(raw: str) -> str:
    raw_text = (raw or "").strip()
    if not raw_text:
        return ""

    norm_fn = _get_mint_normalizer()
    if callable(norm_fn):
        with contextlib.suppress(Exception):
            out_val = str(norm_fn(raw_text)).strip()  # type: ignore[misc]
            if out_val:
                return out_val

    pump_fn = _router_fn("normalize_pump_id_to_mint")
    if callable(pump_fn):
        with contextlib.suppress(Exception):
            out_val2 = str(pump_fn(raw_text)).strip()
            return out_val2 or raw_text

    return raw_text


# =============================================================================
# Quote parsing helpers (Jupiter-ish tolerant)
# =============================================================================


async def _maybe_await(value: Any) -> Any:
    if inspect.isawaitable(value):
        return await value
    return value


async def _maybe_wait(value: Any, timeout_s: float) -> Any:
    if not timeout_s or timeout_s <= 0:
        return await _maybe_await(value)
    return await asyncio.wait_for(_maybe_await(value), timeout=timeout_s)


def _is_errorish_quote(quote: Any) -> bool:
    if not isinstance(quote, dict):
        return True

    q = cast(dict[str, Any], quote)

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


def _impact_bps(quote: dict[str, Any]) -> int:
    try:
        value = quote.get("priceImpactBps")
        if value is not None:
            return int(abs(float(value)))
    except Exception:
        pass
    try:
        pct = float(quote.get("priceImpactPct") or 0.0)
        pct = abs(pct)
        if pct <= 1.0:
            return int(pct * 10_000)
        if pct <= 100.0:
            return int(pct * 100)
        return int(pct)
    except Exception:
        return 0


def _out_amount_raw(quote: dict[str, Any]) -> int:
    for key in ("outAmount", "out_amount", "out_amount_raw"):
        with contextlib.suppress(Exception):
            value = quote.get(key)
            if value is not None:
                return int(value)
    data = quote.get("data")
    if isinstance(data, dict):
        return _out_amount_raw(data)
    return 0


def _route_plan(quote: dict[str, Any]) -> list[dict[str, Any]]:
    rp = quote.get("routePlan")
    if isinstance(rp, list):
        return [item for item in rp if isinstance(item, dict)]
    data = quote.get("data")
    if isinstance(data, dict) and isinstance(data.get("routePlan"), list):
        return [item for item in data["routePlan"] if isinstance(item, dict)]
    return []


def _hops(quote: dict[str, Any]) -> int:
    return len(_route_plan(quote))


def _route_labels(quote: dict[str, Any]) -> list[str]:
    out: list[str] = []
    for step in _route_plan(quote):
        swap_info = step.get("swapInfo") if isinstance(step, dict) else None
        if isinstance(swap_info, dict):
            for key in ("label", "amm", "dex", "market"):
                val = swap_info.get(key)
                if isinstance(val, str) and val.strip():
                    out.append(val.strip())

    seen: set[str] = set()
    uniq: list[str] = []
    for txt in out:
        low = txt.lower()
        if low in seen:
            continue
        seen.add(low)
        uniq.append(txt)
    return uniq


def _route_label_blocks(quote: dict[str, Any]) -> str | None:
    cfg = _cfg()
    labels = _route_labels(quote)

    allow: set[str] = cfg.get("allow_route_labels") or set()
    block: set[str] = cfg.get("block_route_labels") or set()
    use_allow = bool(cfg.get("use_allow_route_labels"))

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
    cfg = _cfg()
    floor = int(cfg.get("sell_probe_floor_raw") or 1)
    cap = int(cfg.get("sell_probe_max_raw") or 0)

    out: list[int] = []
    for item in raws:
        with contextlib.suppress(Exception):
            value = int(item)
            if value <= 0:
                continue
            value = max(value, floor)
            if cap and value > cap:
                value = cap
            out.append(value)

    seen: set[int] = set()
    uniq: list[int] = []
    for value in out:
        if value in seen:
            continue
        seen.add(value)
        uniq.append(value)
    uniq.sort()
    return uniq


def _dynamic_sell_probes_from_buy(out_raw: int) -> list[int]:
    if out_raw <= 0:
        return []
    ratios = (0.0005, 0.002, 0.01)  # 0.05%, 0.2%, 1%
    return [max(1, int(out_raw * ratio)) for ratio in ratios]


# =============================================================================
# Error classification (tolerant, string-based)
# =============================================================================


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
    text = _short(err, 900).lower()
    return (
        "http 401" in text
        or "status=401" in text
        or "http 403" in text
        or "status=403" in text
        or "unauthorized" in text
        or "forbidden" in text
        or ("api key" in text and ("invalid" in text or "missing" in text))
        or "invalid api key" in text
        or ("x-api-key" in text and "required" in text)
    )


def _is_rate_limited(err: Any) -> bool:
    sc = _status_code(err)
    if sc == 429:
        return True
    text = _short(err, 900).lower()
    return (
        "http 429" in text
        or "status=429" in text
        or "rate limit" in text
        or "too many requests" in text
    )


def _is_token_not_tradable(err_or_quote: Any) -> bool:
    if isinstance(err_or_quote, dict):
        code = str(err_or_quote.get("errorCode") or err_or_quote.get("code") or "").upper()
        msg = str(err_or_quote.get("error") or err_or_quote.get("message") or "").lower()
        return code == "TOKEN_NOT_TRADABLE" or "not tradable" in msg
    text = _short(err_or_quote, 900).lower()
    return "token_not_tradable" in text or "not tradable" in text


def _is_no_route(err_or_quote: Any) -> bool:
    if isinstance(err_or_quote, dict):
        code = str(err_or_quote.get("errorCode") or err_or_quote.get("code") or "").upper()
        msg = str(err_or_quote.get("error") or err_or_quote.get("message") or "").lower()
        if code in {"NO_ROUTE", "ROUTE_NOT_FOUND", "NO_ROUTES", "NO_ROUTES_FOUND"}:
            return True
        return "no route" in msg or "no routes" in msg or "route not found" in msg
    text = _short(err_or_quote, 900).lower()
    return (
        "no route" in text or "no routes" in text or "route not found" in text or "no_route" in text
    )


# =============================================================================
# Internal state (single owner)
# =============================================================================

QuoteKey = tuple[str, int, int, int]  # (mint, lamports_in, slippage_bps, only_direct_int)


@dataclass
class _State:
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)

    buy_cache: OrderedDict[QuoteKey, tuple[float, dict[str, Any]]] = field(
        default_factory=OrderedDict
    )
    sell_ok_cache: OrderedDict[str, tuple[float, bool]] = field(default_factory=OrderedDict)

    cooldown_until: dict[str, float] = field(default_factory=dict)
    fail_streak: dict[str, int] = field(default_factory=dict)

    inflight: dict[QuoteKey, asyncio.Task[tuple[bool, dict[str, Any] | None]]] = field(
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
            "internal_error": 0,
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
        cfg = _cfg()
        base = int(cfg.get("cooldown_base_sec") or 60)
        mx = int(cfg.get("cooldown_max_sec") or base)
        exp = float(cfg.get("cooldown_exp") or 1.6)
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
        cfg = _cfg()
        win = int(cfg.get("global_fail_window_sec") or 0)
        mx = int(cfg.get("global_fail_max") or 0)
        cd = int(cfg.get("global_cooldown_sec") or 0)
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
        cfg = _cfg()
        win = int(cfg.get("auth_fail_window_sec") or 0)
        mx = int(cfg.get("auth_fail_max") or 0)
        cd = int(cfg.get("auth_cooldown_sec") or 0)
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


# =============================================================================
# Cache helpers
# =============================================================================


def _quote_key(mint: str, lamports_in: int, slippage_bps: int, only_direct: bool) -> QuoteKey:
    return (mint, int(lamports_in), int(slippage_bps), 1 if only_direct else 0)


async def _get_cached_buy(key: QuoteKey) -> dict[str, Any] | None:
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


async def _put_cached_buy(key: QuoteKey, quote: dict[str, Any]) -> None:
    async with _STATE.lock:
        _STATE.buy_cache[key] = (time.monotonic(), dict(quote))
        _STATE.buy_cache.move_to_end(key, last=True)
        _STATE._trim_unlocked()


async def _get_cached_sell_ok(mint: str) -> bool | None:
    async with _STATE.lock:
        rec = _STATE.sell_ok_cache.get(mint)
        if not rec:
            return None
        ts_mono, ok_val = rec
        ttl = float(_cfg().get("sell_ok_ttl_sec") or 0)
        if (time.monotonic() - ts_mono) <= ttl:
            _STATE.sell_ok_cache.move_to_end(mint, last=True)
            return bool(ok_val)
        _STATE.sell_ok_cache.pop(mint, None)
        return None


async def _put_cached_sell_ok(mint: str, ok_val: bool) -> None:
    async with _STATE.lock:
        _STATE.sell_ok_cache[mint] = (time.monotonic(), bool(ok_val))
        _STATE.sell_ok_cache.move_to_end(mint, last=True)
        _STATE._trim_unlocked()


# =============================================================================
# Router wrapper helpers (signature drift tolerant)
# =============================================================================


def _filter_kwargs_for_callable(fn: Any, raw_kwargs: dict[str, Any]) -> dict[str, Any]:
    """
    Best-effort: only pass kwargs in signature (if inspectable).
    If fn has **kwargs, pass everything.
    """
    try:
        sig = inspect.signature(fn)
        params = list(sig.parameters.values())
        has_var_kw = any(p.kind == inspect.Parameter.VAR_KEYWORD for p in params)
        if has_var_kw:
            return dict(raw_kwargs)
        allowed = {p.name for p in params}
        return {k: v for k, v in raw_kwargs.items() if k in allowed}
    except Exception:
        return dict(raw_kwargs)


async def _call_router_quote(
    fn: Any, *, timeout_s: float, kwargs: dict[str, Any], args: tuple[Any, ...]
) -> Any:
    if not callable(fn):
        raise RuntimeError("router_fn_not_callable")

    try:
        use_kwargs = _filter_kwargs_for_callable(fn, kwargs)
        if use_kwargs:
            return await _maybe_wait(fn(**use_kwargs), timeout_s)  # type: ignore[misc]
    except TypeError:
        pass

    return await _maybe_wait(fn(*args), timeout_s)  # type: ignore[misc]


def _quote_fn_candidates(primary_name: str, aliases: Iterable[str]) -> list[Any]:
    out: list[Any] = []
    primary = _router_fn(primary_name)
    if callable(primary):
        out.append(primary)
    for alias in aliases:
        fn = _router_fn(alias)
        if callable(fn):
            out.append(fn)

    seen: set[int] = set()
    uniq: list[Any] = []
    for fn in out:
        ident = id(fn)
        if ident in seen:
            continue
        seen.add(ident)
        uniq.append(fn)
    return uniq


async def _get_buy_quote(
    mint: str,
    lamports_in: int,
    *,
    slippage_bps: int,
    only_direct: bool,
) -> tuple[dict[str, Any] | None, Any | None]:
    timeout_s = float(_cfg().get("buy_quote_timeout_sec") or 0.0)
    candidates = _quote_fn_candidates("get_buy_quote", ("get_quote_buy", "quote_buy", "buy_quote"))
    if not candidates:
        return None, RuntimeError("no_buy_quote_fn")

    base_kwargs: dict[str, Any] = {
        "mint": mint,
        "amount_raw": int(lamports_in),
        "lamports_in": int(lamports_in),
        "amount": int(lamports_in),
        "slippage_bps": int(slippage_bps),
        "slippageBps": int(slippage_bps),
        "only_direct": bool(only_direct),
        "onlyDirectRoutes": bool(only_direct),
        "onlyDirect": bool(only_direct),
    }
    args = (mint, int(lamports_in), int(slippage_bps))

    last_err: Any | None = None
    for cand in candidates:
        try:
            res = await _call_router_quote(cand, timeout_s=timeout_s, kwargs=base_kwargs, args=args)
            if isinstance(res, dict) and res and not _is_errorish_quote(res):
                return res, None
            if isinstance(res, dict) and res:
                return None, res
            last_err = RuntimeError("buy_quote_empty_or_error")
        except Exception as err:
            last_err = err
            continue

    return None, last_err


async def _get_sell_quote(
    mint: str,
    amount_raw: int,
    *,
    slippage_bps: int,
    only_direct: bool,
) -> tuple[dict[str, Any] | None, Any | None]:
    timeout_s = float(_cfg().get("sell_quote_timeout_sec") or 0.0)
    candidates = _quote_fn_candidates(
        "get_sell_quote", ("get_quote_sell", "quote_sell", "sell_quote")
    )
    if not candidates:
        return None, RuntimeError("no_sell_quote_fn")

    base_kwargs: dict[str, Any] = {
        "mint": mint,
        "amount_raw": int(amount_raw),
        "amount": int(amount_raw),
        "slippage_bps": int(slippage_bps),
        "slippageBps": int(slippage_bps),
        "only_direct": bool(only_direct),
        "onlyDirectRoutes": bool(only_direct),
        "onlyDirect": bool(only_direct),
    }
    args = (mint, int(amount_raw), int(slippage_bps))

    last_err: Any | None = None
    for cand in candidates:
        try:
            res = await _call_router_quote(cand, timeout_s=timeout_s, kwargs=base_kwargs, args=args)
            if isinstance(res, dict) and res and not _is_errorish_quote(res):
                return res, None
            if isinstance(res, dict) and res:
                return None, res
            last_err = RuntimeError("sell_quote_empty_or_error")
        except Exception as err:
            last_err = err
            continue

    return None, last_err


# =============================================================================
# Public API
# =============================================================================


async def preflight(*, mint: str, lamports_in: int) -> tuple[bool, dict[str, Any] | None]:
    """
    Decide whether to attempt a Jupiter BUY for `mint` using `lamports_in`.

    Contract:
      (True, quote_dict) -> Jupiter allowed. quote_dict may be {} as "allow/no-prequote".
      (True, None)       -> fallback-only allowed (router must NOT attempt Jupiter).
      (False, None)      -> blocked.
    """
    _STATE.inc("calls")
    cfg = _cfg()

    raw = (mint or "").strip()
    try:
        lamports = int(lamports_in)
    except Exception:
        lamports = -1

    if not bool(cfg.get("enabled")):
        _STATE.inc("disabled_passthrough")
        return True, {}

    if not raw or lamports <= 0:
        _STATE.inc("bad_input")
        _log_event("RG_BAD_INPUT", mint=raw, lamports_in=lamports_in)
        return False, None

    now_mono = time.monotonic()

    try:
        if not _STATE.global_cooldown_ok():
            _STATE.inc("global_cooldown_block")
            left = int(max(0.0, _STATE.global_cooldown_until - now_mono))
            if bool(cfg.get("allow_no_buy_route")):
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
                "RG_GLOBAL_COOLDOWN_BLOCK",
                10,
                msg="RG_GLOBAL_COOLDOWN_BLOCK",
                secs_left=left,
                mint=raw,
            )
            return False, None

        if (not _STATE.auth_cooldown_ok()) and bool(cfg.get("auth_blocks_jup")):
            _STATE.inc("auth_cooldown_block")
            left = int(max(0.0, _STATE.auth_cooldown_until - now_mono))
            if bool(cfg.get("allow_no_buy_route")):
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

        min_lam = int(cfg.get("min_lamports_in") or 0)
        max_lam = int(cfg.get("max_lamports_in") or 0)
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

        if _get_router() is None:
            _STATE.inc("router_missing")
            if bool(cfg.get("allow_fallback_if_router_missing")) and bool(
                cfg.get("allow_no_buy_route")
            ):
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

        raw_is_mint = _valid_pubkey_str(raw)
        norm = _normalize_to_mint(raw) or raw

        allow_mints = cfg.get("allow_mints") or set()
        block_mints = cfg.get("block_mints") or set()

        if bool(cfg.get("use_allowlist")) and norm not in allow_mints:
            _STATE.inc("blocked_allowlist")
            _log_every("RG_BLOCK_ALLOWLIST", 12, msg="RG_BLOCK_ALLOWLIST", mint=norm, raw=raw)
            return False, None

        if (norm in block_mints) or (raw in block_mints):
            _STATE.inc("blocked_blocklist")
            _log_every("RG_BLOCK_BLOCKLIST", 12, msg="RG_BLOCK_BLOCKLIST", mint=norm, raw=raw)
            return False, None

        if (not raw_is_mint) and bool(cfg.get("force_fallback_if_raw_not_mint")):
            _STATE.inc("fallback_raw_not_mint")
            _log_every(
                "RG_FORCE_FALLBACK_RAW_NOT_MINT",
                12,
                msg="RG_FORCE_FALLBACK_RAW_NOT_MINT",
                raw=raw,
                norm=norm,
            )
            return True, None

        if not _valid_pubkey_str(norm):
            _STATE.inc("fallback_norm_invalid")
            if bool(cfg.get("allow_no_buy_route")):
                _STATE.inc("allow_fallback")
                _log_every(
                    "RG_FALLBACK_NORM_INVALID",
                    12,
                    msg="RG_FALLBACK_NORM_INVALID",
                    raw=raw,
                    norm=norm,
                )
                return True, None
            _STATE.inc("block")
            _log_event("RG_BLOCK_NORM_INVALID", raw=raw, norm=norm)
            return False, None

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

        slippage_bps = int(cfg.get("slippage_bps") or 200)
        only_direct = bool(cfg.get("only_direct"))

        key = _quote_key(norm, lamports, slippage_bps, only_direct)
        debug = bool(cfg.get("debug"))
        trace = norm in (cfg.get("trace_mints") or set())

        async def _compute() -> tuple[bool, dict[str, Any] | None]:
            q_buy = await _get_cached_buy(key)
            q_err: Any | None = None

            if q_buy is not None:
                _STATE.inc("buy_cache_hit")
            else:
                _STATE.inc("buy_cache_miss")
                q_buy, q_err = await _get_buy_quote(
                    norm, lamports, slippage_bps=slippage_bps, only_direct=only_direct
                )
                if isinstance(q_buy, dict) and q_buy and not _is_errorish_quote(q_buy):
                    await _put_cached_buy(key, q_buy)
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
                        "RG_JUP_AUTH_FAIL",
                        8,
                        msg="RG_JUP_AUTH_FAIL",
                        mint=norm,
                        err=_short(q_err, 240),
                    )
                    if bool(cfg.get("allow_no_buy_route")):
                        _STATE.inc("allow_fallback")
                        return True, None
                    _STATE.note_cooldown(norm, reason="auth_fail")
                    _STATE.inc("block")
                    return False, None

                if _is_token_not_tradable(q_err):
                    _STATE.inc("token_not_tradable")
                    _STATE.note_cooldown(norm, reason="token_not_tradable")
                    _log_every("RG_TOKEN_NOT_TRADABLE", 8, msg="RG_TOKEN_NOT_TRADABLE", mint=norm)
                    if bool(cfg.get("allow_no_buy_route")):
                        _STATE.inc("allow_fallback")
                        return True, None
                    _STATE.inc("block")
                    return False, None

                if _is_no_route(q_err):
                    _STATE.inc("no_route")
                    _STATE.note_cooldown(norm, reason="no_route")
                    _log_every("RG_NO_ROUTE", 10, msg="RG_NO_ROUTE", mint=norm)
                    if bool(cfg.get("allow_no_buy_route")):
                        _STATE.inc("allow_fallback")
                        return True, None
                    _STATE.inc("block")
                    return False, None

                if bool(cfg.get("optimistic_buy_err")):
                    _log_every(
                        "RG_BUY_QUOTE_ERR_OPTIMISTIC",
                        10,
                        msg="RG_BUY_QUOTE_ERR_OPTIMISTIC",
                        mint=norm,
                        lamports_in=lamports,
                        slippage_bps=slippage_bps,
                    )
                    _STATE.inc("allow_jup")
                    return True, {}

                if not bool(cfg.get("allow_no_buy_route")):
                    _STATE.note_cooldown(norm, reason="no_buy_quote")
                    _STATE.inc("block")
                    _log_event(
                        "RG_NO_BUY_QUOTE_BLOCK",
                        mint=norm,
                        lamports_in=lamports,
                        slippage_bps=slippage_bps,
                        err=_short(q_err, 240) if q_err else "",
                    )
                    return False, None

                _STATE.inc("allow_fallback")
                _log_every(
                    "RG_NO_BUY_QUOTE_ALLOW_FALLBACK",
                    12,
                    msg="RG_NO_BUY_QUOTE_ALLOW_FALLBACK",
                    mint=norm,
                    slippage_bps=slippage_bps,
                )
                return True, None

            out_raw = _out_amount_raw(q_buy)
            min_out = int(cfg.get("min_out_raw") or 0)
            if min_out and out_raw < min_out:
                _STATE.fail(norm)
                _STATE.note_global_fail()
                _STATE.note_cooldown(norm, reason="min_out")
                if not bool(cfg.get("allow_no_buy_route")):
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
            max_impact = int(cfg.get("max_impact_bps") or 0)
            if max_impact and ibps > max_impact:
                _STATE.fail(norm)
                _STATE.note_global_fail()
                _STATE.note_cooldown(norm, reason="impact")
                _STATE.inc("block_impact")
                _STATE.inc("block")
                _log_event("RG_BLOCK_IMPACT", mint=norm, impact_bps=ibps, max_bps=max_impact)
                return False, None

            hop_count = _hops(q_buy)
            max_hops = int(cfg.get("max_hops") or 0)
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
                    slippage_bps=slippage_bps,
                    only_direct=int(only_direct),
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

            if bool(cfg.get("require_sell")):
                if bool(cfg.get("allow_sell_skip_if_tiny_out")) and out_raw <= int(
                    cfg.get("sell_skip_tiny_out_raw") or 0
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
                        ladder_base = list(cfg.get("sell_probe_ladder") or [])
                        ladder_dyn = (
                            _dynamic_sell_probes_from_buy(out_raw)
                            if bool(cfg.get("sell_dynamic_probes"))
                            else []
                        )
                        ladder = _ladder_with_floor(
                            ladder_dyn + ladder_base + [int(cfg.get("sell_probe_floor_raw") or 1)]
                        )

                        sell_ok = False
                        last_err: str | None = None

                        for probe_raw in ladder:
                            q_sell, s_err = await _get_sell_quote(
                                norm,
                                int(probe_raw),
                                slippage_bps=slippage_bps,
                                only_direct=only_direct,
                            )
                            if (
                                (not isinstance(q_sell, dict))
                                or (not q_sell)
                                or _is_errorish_quote(q_sell)
                            ):
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
                                        slippage_bps=slippage_bps,
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
                            if bool(cfg.get("optimistic_sell_err")):
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
            q_out["__rg"] = {
                "mint": norm,
                "impact_bps": ibps,
                "hops": hop_count,
                "slippage_bps": slippage_bps,
                "only_direct": int(only_direct),
                "ts": time.time(),
            }
            return True, q_out

        if bool(cfg.get("coalesce_inflight")):
            max_inflight = int(cfg.get("max_inflight") or 0)

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
                    if bool(cfg.get("allow_no_buy_route")):
                        _STATE.inc("allow_fallback")
                        return True, None
                    _STATE.inc("block")
                    return False, None

                existing = _STATE.inflight.get(key)
                task = existing if (existing is not None and not existing.done()) else None

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

            return await asyncio.shield(task) if bool(cfg.get("inflight_shield")) else await task

        return await _compute()

    except asyncio.CancelledError:
        raise
    except Exception as err:
        _STATE.inc("internal_error")
        _STATE.note_global_fail()
        _log_every("RG_INTERNAL_ERROR", 10, msg="RG_INTERNAL_ERROR", mint=raw, err=_short(err, 400))
        if bool(_cfg().get("allow_no_buy_route")):
            _STATE.inc("allow_fallback")
            return True, None
        _STATE.inc("block")
        return False, None


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
    cfg = _cfg()
    now_mono = time.monotonic()
    return {
        **_STATE.counters,
        "buy_cache": len(_STATE.buy_cache),
        "sell_ok_cache": len(_STATE.sell_ok_cache),
        "cooldowns": len(_STATE.cooldown_until),
        "fail_streaks": len(_STATE.fail_streak),
        "inflight": len(_STATE.inflight),
        "enabled": int(bool(cfg.get("enabled"))),
        "allow_no_buy_route": int(bool(cfg.get("allow_no_buy_route"))),
        "require_sell_route": int(bool(cfg.get("require_sell"))),
        "optimistic_on_buy_quote_error": int(bool(cfg.get("optimistic_buy_err"))),
        "optimistic_on_sell_quote_error": int(bool(cfg.get("optimistic_sell_err"))),
        "quote_ttl_sec": int(cfg.get("quote_ttl_sec") or 0),
        "sell_ok_ttl_sec": int(cfg.get("sell_ok_ttl_sec") or 0),
        "cooldown_base_sec": int(cfg.get("cooldown_base_sec") or 0),
        "cooldown_max_sec": int(cfg.get("cooldown_max_sec") or 0),
        "max_impact_bps": int(cfg.get("max_impact_bps") or 0),
        "max_hops": int(cfg.get("max_hops") or 0),
        "min_lamports_in": int(cfg.get("min_lamports_in") or 0),
        "max_lamports_in": int(cfg.get("max_lamports_in") or 0),
        "min_out_raw": int(cfg.get("min_out_raw") or 0),
        "slippage_bps": int(cfg.get("slippage_bps") or 0),
        "only_direct": int(bool(cfg.get("only_direct"))),
        "coalesce_inflight": int(bool(cfg.get("coalesce_inflight"))),
        "max_inflight": int(cfg.get("max_inflight") or 0),
        "buy_quote_timeout_sec": float(cfg.get("buy_quote_timeout_sec") or 0.0),
        "sell_quote_timeout_sec": float(cfg.get("sell_quote_timeout_sec") or 0.0),
        "cache_max_keys": int(cfg.get("cache_max_keys") or 0),
        "allowlist_enabled": int(bool(cfg.get("use_allowlist"))),
        "allow_mints_count": len(cfg.get("allow_mints") or set()),
        "block_mints_count": len(cfg.get("block_mints") or set()),
        "block_route_labels_count": len(cfg.get("block_route_labels") or set()),
        "allow_route_labels_count": len(cfg.get("allow_route_labels") or set()),
        "force_fallback_if_raw_not_mint": int(bool(cfg.get("force_fallback_if_raw_not_mint"))),
        "global_cooldown_active": int(now_mono < _STATE.global_cooldown_until),
        "global_cooldown_left": int(max(0.0, _STATE.global_cooldown_until - now_mono)),
        "auth_cooldown_active": int(now_mono < _STATE.auth_cooldown_until),
        "auth_cooldown_left": int(max(0.0, _STATE.auth_cooldown_until - now_mono)),
    }


__all__ = ["clear_cache", "get_logger", "preflight", "stats"]
