from __future__ import annotations

"""
route_guard.py (ROOT) — V3+++ (God-Tier Overhaul) — WRAPPER / COMPAT LAYER
========================================================================

ROOT module is legacy compatibility only.

Canonical route safety lives in:
  - utils_pkg/routeguard.py   (heavy decisioning)
  - utils_pkg/router_guard.py (stable router-facing wrapper)

This wrapper preserves older “quote aging + watchdog” API that legacy wiring may call:

  async def precheck_buy(out_mint: str, amount_sol: float, slippage_bps: int)
      -> (ok: bool, reason: str, meta: dict)

  async def posttrade_watchdog(out_mint: str, amount_sol: float, slippage_bps: int, pre_meta: dict)
      -> (ok: bool, reason: str, meta: dict)

Hard requirements
-----------------
- NO network at import time.
- Never raises (always returns a tuple). (Cancellation is allowed to propagate.)
- Does NOT compete with utils_pkg.routeguard (no parallel “block” logic).
- Telemetry is best-effort (safe if route_telemetry missing).
- Avoid logger monkey-patching (Phase 2 requirement).

QUOTE_MAX_AGE_SEC
-----------------
Preserved legacy behavior using a monotonic timestamp stored in pre_meta["ts_mono"].
"""

import asyncio
import contextlib
import inspect
import os
import re
import time
from typing import Any, Callable, Final, Protocol, cast

# =============================================================================
# Logging (Phase A: EventLogger contract; NO monkey patching)
# =============================================================================


class EventLogger(Protocol):
    # common stdlib-like calls
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
    Canonical logger getter (single signature; EventLogger-compatible return).

    Priority:
      1) utils_pkg.log_compat.get_event_logger / get_logger
      2) logging_setup.get_logger
      3) stdlib logging.getLogger (minimal basicConfig only if no handlers)
    """
    nm = (name or "route_guard").strip() or "route_guard"

    # Prefer compat layer if present
    with contextlib.suppress(Exception):
        from utils_pkg.log_compat import get_event_logger  # type: ignore

        return cast(EventLogger, get_event_logger(nm))

    with contextlib.suppress(Exception):
        from utils_pkg.log_compat import get_logger as gl  # type: ignore

        return cast(EventLogger, gl(nm))

    # Preferred root helper
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
# Redaction (best-effort; avoid leaking DSNs/tokens in exception strings)
# =============================================================================

_TRUE: Final[set[str]] = {"1", "true", "yes", "y", "on", "t"}
_FALSE: Final[set[str]] = {"0", "false", "no", "n", "off", "f"}

_REDACTOR: Any | None = None
with contextlib.suppress(Exception):
    from utils_pkg.log_redaction import redact_text as _rt  # type: ignore

    _REDACTOR = _rt  # type: ignore[assignment]

_RE_URL_CRED = re.compile(r"(?i)\b([a-z][a-z0-9+.-]*://)([^/\s:@]+):([^@\s/]+)@")
_RE_KV_SECRET = re.compile(
    r"(?i)\b(api[_-]?key|token|secret|password|passwd|pwd|private[_-]?key)\b\s*[:=]\s*([^\s'\";,]+)"
)
_RE_LONG_TOKEN = re.compile(r"\b([A-Za-z0-9_\-]{24,})\b")


def _fallback_redact_text(s: str) -> str:
    if not s:
        return s
    out = str(s)
    out = _RE_URL_CRED.sub(r"\1***:***@", out)
    out = _RE_KV_SECRET.sub(r"\1=***", out)

    def _mask_long(m: re.Match[str]) -> str:
        tok = m.group(1)
        if len(tok) <= 28:
            return "***"
        return f"{tok[:4]}…{tok[-4:]}"

    out = _RE_LONG_TOKEN.sub(_mask_long, out)
    return out


def _redact_text(s: Any) -> str:
    try:
        if _REDACTOR is not None:
            return str(_REDACTOR(str(s)))
    except Exception:
        pass
    return _fallback_redact_text(str(s))


def _short(value: Any, limit: int = 280) -> str:
    try:
        text = _redact_text(value)
        return text if len(text) <= limit else (text[:limit] + "…")
    except Exception:
        return "<?>"


def _log_event(name: str, **fields: Any) -> None:
    """
    Never raises; prefers structured logger if available.
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
        window_sec = float(max(1, int(seconds)))
        if (now_mono - last_mono) >= window_sec:
            _EVERY_STORE[key] = now_mono
            _log_event(msg or key, **fields)
    except Exception:
        return


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


# =============================================================================
# Legacy constants / knobs (read-only, no network)
# =============================================================================

SOL_MINT = "So11111111111111111111111111111111111111112"
LAMPORTS = 1_000_000_000

# Keep legacy env names (compat). These do NOT gate routing here; only breadcrumbs.
ROUTE_MAX_HOPS = _envi("ROUTE_MAX_HOPS", _envi("ROUTEGUARD_MAX_HOPS", 5))

# Legacy file used float impact; canonical uses bps int. We treat this as bps.
ROUTE_MAX_IMPACT_BPS = _envf(
    "ROUTE_MAX_IMPACT_BPS",
    _envf("ROUTEGUARD_MAX_IMPACT_BPS", _envf("AI_MAX_IMPACT_BPS", 150.0)),
)

WATCHDOG_ALERT_DEVIATION_BPS = _envi("ROUTE_WATCHDOG_DEVIATION_BPS", 60)
QUOTE_MAX_AGE_SEC = _envi("QUOTE_MAX_AGE_SEC", _envi("ROUTEGUARD_BUY_QUOTE_TTL_SEC", 12))

WATCHDOG_ENABLED = _envb("ROUTE_WATCHDOG_ENABLED", True)

# =============================================================================
# Async helper
# =============================================================================


async def _maybe_await(value: Any) -> Any:
    if inspect.isawaitable(value):
        return await value
    return value


# =============================================================================
# Best-effort telemetry hooks (lazy, safe if missing)
# =============================================================================


async def _noop_async(*_a: Any, **_k: Any) -> None:
    return None


def _get_telemetry_fns() -> tuple[Callable[..., Any], Callable[..., Any]]:
    """
    Returns (count_route_used, count_watchdog_hit) where each may be sync or async.
    Imported lazily to avoid import-time side effects.
    """
    try:
        import route_telemetry as rt  # type: ignore

        fn_used = getattr(rt, "count_route_used", None)
        fn_hit = getattr(rt, "count_watchdog_hit", None)
        return (fn_used or _noop_async, fn_hit or _noop_async)
    except Exception:
        return (_noop_async, _noop_async)


async def _telemetry_route_used(labels: list[str]) -> None:
    fn_used, _fn_hit = _get_telemetry_fns()
    with contextlib.suppress(Exception):
        await _maybe_await(fn_used(labels))


async def _telemetry_watchdog_hit(labels: list[str]) -> None:
    _fn_used, fn_hit = _get_telemetry_fns()
    with contextlib.suppress(Exception):
        await _maybe_await(fn_hit(labels))


# =============================================================================
# Lazy imports (avoid cycles / heavy import at module import)
# =============================================================================

_RG_WRAPPER: Any | None = None
_ROUTER_MOD: Any | None = None


def _get_router_guard() -> Any | None:
    """
    Prefer router_guard wrapper for stability; it delegates to routeguard canonically.
    """
    global _RG_WRAPPER
    if _RG_WRAPPER is not None:
        return _RG_WRAPPER
    try:
        from utils_pkg import router_guard as rgw  # type: ignore

        _RG_WRAPPER = rgw
        return rgw
    except Exception:
        _RG_WRAPPER = None
        return None


def _get_router() -> Any | None:
    global _ROUTER_MOD
    if _ROUTER_MOD is not None:
        return _ROUTER_MOD
    try:
        from utils_pkg import router as rtr  # type: ignore

        _ROUTER_MOD = rtr
        return rtr
    except Exception:
        _ROUTER_MOD = None
        return None


# =============================================================================
# Quote parsing helpers (Jupiter-ish tolerant)
# =============================================================================


def _impact_bps(quote: dict[str, Any] | None) -> float | None:
    if not isinstance(quote, dict) or not quote:
        return None

    with contextlib.suppress(Exception):
        val = quote.get("priceImpactBps")
        if val is not None:
            return float(abs(float(val)))

    with contextlib.suppress(Exception):
        pct = float(quote.get("priceImpactPct") or 0.0)
        pct = abs(pct)
        if pct <= 1.0:
            return float(int(pct * 10_000))
        if pct <= 100.0:
            return float(int(pct * 100))
        return float(int(pct))
    return None


def _out_amount_raw(quote: dict[str, Any] | None) -> float:
    if not isinstance(quote, dict) or not quote:
        return 0.0

    for key in ("outAmount", "out_amount", "out_amount_raw"):
        with contextlib.suppress(Exception):
            val = quote.get(key)
            if val is not None:
                return float(val)

    data = quote.get("data")
    if isinstance(data, dict):
        return _out_amount_raw(data)
    return 0.0


def _route_plan(quote: dict[str, Any] | None) -> list[dict[str, Any]]:
    if not isinstance(quote, dict) or not quote:
        return []
    rp = quote.get("routePlan")
    if isinstance(rp, list):
        return [item for item in rp if isinstance(item, dict)]
    data = quote.get("data")
    if isinstance(data, dict) and isinstance(data.get("routePlan"), list):
        return [item for item in data["routePlan"] if isinstance(item, dict)]
    return []


def _hops(quote: dict[str, Any] | None) -> int:
    return len(_route_plan(quote))


def _labels_from_quote(quote: dict[str, Any] | None) -> list[str]:
    labels: list[str] = []
    for step in _route_plan(quote):
        swap_info = step.get("swapInfo") if isinstance(step, dict) else None
        if not isinstance(swap_info, dict):
            continue
        raw = (
            swap_info.get("label")
            or swap_info.get("amm")
            or swap_info.get("dex")
            or swap_info.get("market")
        )
        if isinstance(raw, str) and raw.strip():
            labels.append(raw.strip())

    if not labels:
        return ["OTHER"]

    seen: set[str] = set()
    out: list[str] = []
    for item in labels:
        low = item.lower()
        if low in seen:
            continue
        seen.add(low)
        out.append(item[:48])
    return out


def _meta_from_quote(quote: dict[str, Any] | None) -> dict[str, Any]:
    imp = _impact_bps(quote)
    return {
        "hops": int(_hops(quote)) if quote else None,
        "impact_bps": float(imp) if imp is not None else None,
        "out_amount": float(_out_amount_raw(quote)),
        "labels": _labels_from_quote(quote),
    }


# =============================================================================
# Router quote helper (best-effort, signature drift tolerant)
# =============================================================================


async def _get_buy_quote(out_mint: str, lamports_in: int) -> dict[str, Any] | None:
    router_mod = _get_router()
    if router_mod is None:
        return None

    fn = getattr(router_mod, "get_buy_quote", None)
    if not callable(fn):
        for name in ("get_quote_buy", "quote_buy", "buy_quote"):
            alt = getattr(router_mod, name, None)
            if callable(alt):
                fn = alt
                break
    if not callable(fn):
        return None

    kw_variants = (
        {"mint": out_mint, "lamports_in": int(lamports_in)},
        {"mint": out_mint, "amount_raw": int(lamports_in)},
        {"mint": out_mint, "amount": int(lamports_in)},
    )

    for kwargs in kw_variants:
        try:
            res = await _maybe_await(fn(**kwargs))  # type: ignore[misc]
            if isinstance(res, dict) and res:
                return res
            return None
        except TypeError:
            continue
        except asyncio.CancelledError:
            raise
        except Exception:
            return None

    with contextlib.suppress(Exception):
        res = await _maybe_await(fn(out_mint, int(lamports_in)))  # type: ignore[misc]
        return res if isinstance(res, dict) and res else None
    return None


# =============================================================================
# Public API (LEGACY)
# =============================================================================


async def precheck_buy(
    out_mint: str, amount_sol: float, slippage_bps: int
) -> tuple[bool, str, dict[str, Any]]:
    """
    Legacy pre-check wrapper.

    Delegates allow/block to utils_pkg.router_guard.preflight() (canonical).

    Returns:
      (ok, reason, meta)

    meta includes:
      - hops, impact_bps, out_amount, labels (if a prequote dict is available)
      - ts_mono: monotonic timestamp (QUOTE_MAX_AGE_SEC emulation)
      - probe_lamports_in
      - slippage_bps
      - fallback_only (if preflight returned None)
    """
    try:
        mint = (out_mint or "").strip()
        lamports_in = max(1, int(float(amount_sol) * float(LAMPORTS)))
    except Exception:
        return False, "bad_input", {}

    if not mint:
        return False, "bad_input", {"probe_lamports_in": lamports_in}

    rgw = _get_router_guard()
    if rgw is None:
        return (
            False,
            "router_guard_missing",
            {"probe_lamports_in": lamports_in, "ts_mono": time.monotonic()},
        )

    try:
        ok_val, preq = await rgw.preflight(mint=mint, lamports_in=int(lamports_in))  # type: ignore[misc]
    except asyncio.CancelledError:
        raise
    except Exception as err:
        _log_event("ROUTE_GUARD_PRECHECK_ERR", mint=mint, err=_short(err, 400))
        return (
            False,
            "router_guard_error",
            {"probe_lamports_in": lamports_in, "ts_mono": time.monotonic()},
        )

    base_meta: dict[str, Any] = {
        "probe_lamports_in": lamports_in,
        "slippage_bps": int(slippage_bps),
        "ts_mono": time.monotonic(),
        "legacy_policy": {
            "ROUTE_MAX_HOPS": int(ROUTE_MAX_HOPS),
            "ROUTE_MAX_IMPACT_BPS": float(ROUTE_MAX_IMPACT_BPS),
            "QUOTE_MAX_AGE_SEC": int(QUOTE_MAX_AGE_SEC),
        },
    }

    if not ok_val:
        base_meta["labels"] = ["BLOCKED"]
        return False, "blocked", base_meta

    if preq is None:
        base_meta.update(
            {
                "fallback_only": True,
                "labels": ["FALLBACK"],
                "hops": None,
                "impact_bps": None,
                "out_amount": 0.0,
            }
        )
        return True, "fallback_only", base_meta

    quote = dict(preq) if isinstance(preq, dict) else {}
    base_meta.update(
        _meta_from_quote(quote)
        if quote
        else {"labels": ["OTHER"], "hops": None, "impact_bps": None, "out_amount": 0.0}
    )
    return True, "ok", base_meta


async def posttrade_watchdog(
    out_mint: str,
    amount_sol: float,
    slippage_bps: int,
    pre_meta: dict[str, Any],
) -> tuple[bool, str, dict[str, Any]]:
    """
    Legacy watchdog: compare a fresh quote after trade vs pre_meta out_amount.

    Best-effort behavior:
      - If router quote is unavailable -> (False, "no_post_quote", meta)
      - Telemetry is best-effort and never raises

    Returns:
      (ok, reason, meta)
    """
    mint = (out_mint or "").strip()
    if not mint:
        return False, "bad_input", {}

    labels_pre: list[str] = ["OTHER"]
    if isinstance(pre_meta, dict):
        raw_labels = pre_meta.get("labels")
        if isinstance(raw_labels, list) and raw_labels:
            labels_pre = [str(x)[:48] for x in raw_labels if str(x).strip()] or ["OTHER"]

    await _telemetry_route_used(labels_pre)

    if not WATCHDOG_ENABLED:
        return True, "watchdog_disabled", {"labels": labels_pre}

    try:
        lamports_in = max(1, int(float(amount_sol) * float(LAMPORTS)))
    except Exception:
        return False, "bad_input", {"labels": labels_pre}

    ts_mono = 0.0
    with contextlib.suppress(Exception):
        if isinstance(pre_meta, dict):
            ts_mono = float(pre_meta.get("ts_mono", 0.0) or 0.0)

    if QUOTE_MAX_AGE_SEC > 0 and ts_mono > 0:
        age_sec = time.monotonic() - ts_mono
        if age_sec > float(QUOTE_MAX_AGE_SEC):
            return (
                False,
                f"stale_prequote:{int(age_sec)}s",
                {"labels": labels_pre, "age_sec": float(age_sec)},
            )

    q2 = await _get_buy_quote(mint, lamports_in)
    if not q2:
        return False, "no_post_quote", {"labels": labels_pre, "probe_lamports_in": lamports_in}

    now_meta = _meta_from_quote(q2)
    now_meta["probe_lamports_in"] = lamports_in
    now_meta["slippage_bps"] = int(slippage_bps)

    pre_out = 0.0
    with contextlib.suppress(Exception):
        if isinstance(pre_meta, dict):
            pre_out = float(pre_meta.get("out_amount", 0.0) or 0.0)

    now_out = float(now_meta.get("out_amount", 0.0) or 0.0)

    deviation_bps = 0.0
    if pre_out > 0 and now_out > 0:
        deviation_bps = (pre_out - now_out) / pre_out * 10_000.0

    now_meta["deviation_bps_vs_pre"] = float(deviation_bps)
    now_meta["labels_pre"] = labels_pre

    if deviation_bps > float(WATCHDOG_ALERT_DEVIATION_BPS):
        await _telemetry_watchdog_hit(labels_pre)
        _log_every(
            "ROUTE_WATCHDOG_HIT",
            10,
            msg="ROUTE_WATCHDOG_HIT",
            mint=mint,
            deviation_bps=float(deviation_bps),
            threshold_bps=int(WATCHDOG_ALERT_DEVIATION_BPS),
        )
        return False, f"post_deviation_bps:{deviation_bps:.0f}", now_meta

    return True, "ok", now_meta


__all__ = [
    "LAMPORTS",
    "SOL_MINT",
    "get_logger",
    "posttrade_watchdog",
    "precheck_buy",
]
