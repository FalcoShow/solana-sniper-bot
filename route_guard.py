# route_guard.py
from __future__ import annotations

"""
route_guard.py (ROOT) — V3+++ (God-Tier Overhaul) — WRAPPER / COMPAT LAYER
========================================================================

This ROOT module is **legacy API compatibility only**.

✅ Canonical route safety lives in: `utils_pkg/routeguard.py`
    - preflight(mint, lamports_in) -> (ok, prequote_or_none)
    - clear_cache(...)
    - stats()

This wrapper preserves the older “quote aging + watchdog” API that some older
strategy wiring may still call:

    async def precheck_buy(out_mint: str, amount_sol: float, slippage_bps: int)
        -> (ok: bool, reason: str, meta: dict)

    async def posttrade_watchdog(out_mint: str, amount_sol: float, slippage_bps: int, pre_meta: dict)
        -> (ok: bool, reason: str, meta: dict)

Hard requirements
-----------------
- NO network at import time.
- Never raises (always returns a tuple).
- Does NOT compete with utils_pkg.routeguard (no parallel “block” logic).
- Telemetry is best-effort (safe if route_telemetry missing).

Notes
-----
- `QUOTE_MAX_AGE_SEC` is preserved for legacy behavior; we emulate it using a
  monotonic timestamp stored in pre_meta["ts_mono"].
"""

import asyncio
import contextlib
import os
import time
from typing import Any

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

        log.event = _event  # type: ignore[attr-defined]

    if not hasattr(log, "every"):

        def _every(
            key: str, n: int = 30, msg: str | None = None, **kw: Any
        ) -> None:  # pragma: no cover
            try:
                now = time.monotonic()
                store = getattr(_every, "_store", {})  # type: ignore[attr-defined]
                last = float(store.get(key, 0.0))
                sec = float(max(1, int(n)))
                if (now - last) >= sec:
                    store[key] = now
                    _every._store = store  # type: ignore[attr-defined]
                    ev = getattr(log, "event", None)
                    if callable(ev):
                        ev(msg or key, **kw)
                    else:
                        log.info("%s %s", msg or key, kw)
            except Exception:
                return None

        log.every = _every  # type: ignore[attr-defined]


_ensure_log_shims()


def _short(x: Any, n: int = 240) -> str:
    try:
        s = str(x)
        return s if len(s) <= n else (s[:n] + "…")
    except Exception:
        return "<?>"


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


# ──────────────────────────────────────────────────────────────────────────────
# Legacy constants / knobs (read-only, no network)
# ──────────────────────────────────────────────────────────────────────────────
SOL_MINT = "So11111111111111111111111111111111111111112"
LAMPORTS = 1_000_000_000

# Keep legacy env names, but prefer ROUTEGUARD_* / AI_* where appropriate.
ROUTE_MAX_HOPS = _envi("ROUTE_MAX_HOPS", _envi("ROUTEGUARD_MAX_HOPS", 5))

# Legacy file used float impact; canonical uses bps int. We treat this as bps.
ROUTE_MAX_IMPACT_BPS = _envf(
    "ROUTE_MAX_IMPACT_BPS", _envf("ROUTEGUARD_MAX_IMPACT_BPS", _envf("AI_MAX_IMPACT_BPS", 150.0))
)
WATCHDOG_ALERT_DEVIATION_BPS = _envi("ROUTE_WATCHDOG_DEVIATION_BPS", 60)
QUOTE_MAX_AGE_SEC = _envi("QUOTE_MAX_AGE_SEC", _envi("ROUTEGUARD_BUY_QUOTE_TTL_SEC", 12))

# If set, we will attempt a fresh post-trade quote using router (best-effort)
WATCHDOG_ENABLED = _envb("ROUTE_WATCHDOG_ENABLED", True)


# ──────────────────────────────────────────────────────────────────────────────
# Best-effort telemetry hooks (safe if missing)
# ──────────────────────────────────────────────────────────────────────────────
async def _noop_async(*_a: Any, **_k: Any) -> None:
    return None


def _telemetry():
    try:
        import route_telemetry as rt  # type: ignore

        return (
            getattr(rt, "count_route_used", None) or _noop_async,
            getattr(rt, "count_watchdog_hit", None) or _noop_async,
        )
    except Exception:
        return (_noop_async, _noop_async)


_count_route_used, _count_watchdog_hit = _telemetry()


# ──────────────────────────────────────────────────────────────────────────────
# Lazy imports (avoid cycles)
# ──────────────────────────────────────────────────────────────────────────────
_rg_mod = None
_router_mod = None


def _get_rg():
    global _rg_mod
    if _rg_mod is not None:
        return _rg_mod
    try:
        from utils_pkg import routeguard as rg  # type: ignore

        _rg_mod = rg
        return _rg_mod
    except Exception:
        _rg_mod = None
        return None


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


async def _maybe_await(x: Any) -> Any:
    if asyncio.iscoroutine(x) or asyncio.isfuture(x):
        return await x
    return x


# ──────────────────────────────────────────────────────────────────────────────
# Quote parsing helpers (Jupiter-ish tolerant)
# ──────────────────────────────────────────────────────────────────────────────
def _impact_bps(quote: dict[str, Any] | None) -> float | None:
    if not isinstance(quote, dict) or not quote:
        return None
    with contextlib.suppress(Exception):
        v = quote.get("priceImpactBps")
        if v is not None:
            return float(abs(float(v)))
    with contextlib.suppress(Exception):
        pip = float(quote.get("priceImpactPct") or 0.0)
        pip = abs(pip)
        if pip <= 1.0:
            return float(int(pip * 10_000))
        if pip <= 100.0:
            return float(int(pip * 100))
        return float(int(pip))
    return None


def _out_amount(quote: dict[str, Any] | None) -> float:
    if not isinstance(quote, dict) or not quote:
        return 0.0
    for k in ("outAmount", "out_amount", "out_amount_raw"):
        with contextlib.suppress(Exception):
            v = quote.get(k)
            if v is not None:
                return float(v)
    data = quote.get("data")
    if isinstance(data, dict):
        return _out_amount(data)
    return 0.0


def _route_plan(quote: dict[str, Any] | None) -> list[dict]:
    if not isinstance(quote, dict) or not quote:
        return []
    rp = quote.get("routePlan")
    if isinstance(rp, list):
        return [x for x in rp if isinstance(x, dict)]
    data = quote.get("data")
    if isinstance(data, dict) and isinstance(data.get("routePlan"), list):
        return [x for x in data["routePlan"] if isinstance(x, dict)]
    # legacy v2 shapes sometimes had routes/data arrays; we do not rely on those for gating
    return []


def _hops(quote: dict[str, Any] | None) -> int:
    return len(_route_plan(quote))


def _dex_labels_from_quote(quote: dict[str, Any] | None) -> list[str]:
    labels: list[str] = []
    for step in _route_plan(quote):
        si = step.get("swapInfo") if isinstance(step, dict) else None
        if not isinstance(si, dict):
            continue
        raw = si.get("label") or si.get("amm") or si.get("dex") or si.get("market")
        if not raw:
            continue
        r = str(raw).lower()
        if "raydium" in r:
            labels.append("RAYDIUM")
        elif "orca" in r:
            labels.append("ORCA")
        elif "meteora" in r:
            labels.append("METEORA")
        elif "openbook" in r:
            labels.append("OPENBOOK")
        elif "phoenix" in r:
            labels.append("PHOENIX")
        elif "crema" in r:
            labels.append("CREMA")
        elif "whirlpool" in r:
            labels.append("ORCA-WHIRL")
        else:
            labels.append(str(raw).upper()[:32])
    if not labels:
        labels = ["OTHER"]
    # uniq
    seen = set()
    out: list[str] = []
    for x in labels:
        if x in seen:
            continue
        seen.add(x)
        out.append(x)
    return out


def _route_meta_from_quote(quote: dict[str, Any] | None) -> dict[str, Any]:
    hop_len = _hops(quote)
    imp = _impact_bps(quote)
    out_amt = _out_amount(quote)
    return {
        "hops": int(hop_len),
        "impact_bps": float(imp) if imp is not None else None,
        "out_amount": float(out_amt),
        "labels": _dex_labels_from_quote(quote),
    }


# ──────────────────────────────────────────────────────────────────────────────
# Router quote helper (signature drift tolerant)
# ──────────────────────────────────────────────────────────────────────────────
async def _get_buy_quote(out_mint: str, lamports_in: int) -> dict[str, Any] | None:
    r = _get_router()
    if r is None:
        return None
    fn = getattr(r, "get_buy_quote", None)
    if not callable(fn):
        # try alternate names
        for name in ("get_quote_buy", "quote_buy", "buy_quote"):
            fn2 = getattr(r, name, None)
            if callable(fn2):
                fn = fn2
                break
    if not callable(fn):
        return None

    for kwargs in (
        {"mint": out_mint, "lamports_in": int(lamports_in)},
        {"mint": out_mint, "amount_raw": int(lamports_in)},
        {"mint": out_mint, "amount": int(lamports_in)},
    ):
        try:
            res = await _maybe_await(fn(**kwargs))  # type: ignore[misc]
            if isinstance(res, dict) and res:
                return res
            return None
        except TypeError:
            continue
        except Exception:
            return None

    # positional fallback
    with contextlib.suppress(Exception):
        res = await _maybe_await(fn(out_mint, int(lamports_in)))  # type: ignore[misc]
        return res if isinstance(res, dict) and res else None
    return None


# ──────────────────────────────────────────────────────────────────────────────
# Public API (LEGACY)
# ──────────────────────────────────────────────────────────────────────────────
async def precheck_buy(
    out_mint: str, amount_sol: float, slippage_bps: int
) -> tuple[bool, str, dict[str, Any]]:
    """
    Legacy pre-check wrapper.

    IMPORTANT: This wrapper does NOT implement independent gating logic.
    It delegates allow/block to utils_pkg.routeguard.

    Returns:
      (ok, reason, meta)

    meta includes:
      - hops, impact_bps, out_amount, labels
      - ts_mono: monotonic timestamp (for QUOTE_MAX_AGE_SEC emulation)
      - probe_lamports_in
    """
    try:
        mint = (out_mint or "").strip()
        lamports_in = max(1, int(float(amount_sol) * float(LAMPORTS)))
    except Exception:
        return False, "bad_input", {}

    rg = _get_rg()
    if rg is None:
        return False, "routeguard_missing", {"probe_lamports_in": lamports_in}

    try:
        ok, preq = await rg.preflight(mint=mint, lamports_in=int(lamports_in))  # type: ignore[attr-defined]
    except Exception as e:
        log.event("ROUTE_GUARD_PRECHECK_ERR", mint=mint, err=_short(e, 400))
        return False, "routeguard_error", {"probe_lamports_in": lamports_in}

    if not ok:
        return False, "blocked", {"probe_lamports_in": lamports_in, "ts_mono": time.monotonic()}

    # ok=True
    if preq is None:
        # fallback-only allowed by canonical guard
        return (
            True,
            "fallback_only",
            {
                "probe_lamports_in": lamports_in,
                "fallback_only": True,
                "ts_mono": time.monotonic(),
                "labels": ["FALLBACK"],
                "hops": None,
                "impact_bps": None,
                "out_amount": 0.0,
            },
        )

    quote = dict(preq) if isinstance(preq, dict) else {}
    meta = (
        _route_meta_from_quote(quote)
        if quote
        else {"labels": ["OTHER"], "hops": None, "impact_bps": None, "out_amount": 0.0}
    )
    meta.update(
        {
            "probe_lamports_in": lamports_in,
            "ts_mono": time.monotonic(),
            "slippage_bps": int(slippage_bps),
        }
    )

    # legacy breadcrumbs (do not gate)
    meta["legacy_policy"] = {
        "ROUTE_MAX_HOPS": int(ROUTE_MAX_HOPS),
        "ROUTE_MAX_IMPACT_BPS": float(ROUTE_MAX_IMPACT_BPS),
        "QUOTE_MAX_AGE_SEC": int(QUOTE_MAX_AGE_SEC),
    }
    return True, "ok", meta


async def posttrade_watchdog(
    out_mint: str,
    amount_sol: float,
    slippage_bps: int,
    pre_meta: dict[str, Any],
) -> tuple[bool, str, dict[str, Any]]:
    """
    Legacy watchdog: compare a fresh quote after trade vs pre_meta out_amount.

    This is best-effort:
      - If router quote is unavailable, returns (False, "no_post_quote", {})
      - Telemetry is best-effort and never raises.

    Returns:
      (ok, reason, now_meta)
    """
    mint = (out_mint or "").strip()

    # Always emit route-used telemetry (best-effort)
    labels_pre = pre_meta.get("labels") if isinstance(pre_meta, dict) else None
    if not isinstance(labels_pre, list) or not labels_pre:
        labels_pre = ["OTHER"]
    with contextlib.suppress(Exception):
        await _count_route_used(labels_pre)

    if not WATCHDOG_ENABLED:
        return True, "watchdog_disabled", {"labels": labels_pre}

    try:
        lamports_in = max(1, int(float(amount_sol) * float(LAMPORTS)))
    except Exception:
        return False, "bad_input", {"labels": labels_pre}

    # emulate legacy prequote age check using monotonic timestamp stored by precheck_buy
    try:
        ts_mono = float(pre_meta.get("ts_mono", 0.0) or 0.0)
    except Exception:
        ts_mono = 0.0

    if QUOTE_MAX_AGE_SEC > 0 and ts_mono > 0:
        age = time.monotonic() - ts_mono
        if age > float(QUOTE_MAX_AGE_SEC):
            return (
                False,
                f"stale_prequote:{int(age)}s",
                {"labels": labels_pre, "age_sec": float(age)},
            )

    # Get a fresh quote (best-effort) via canonical router
    q2 = await _get_buy_quote(mint, lamports_in)
    if not q2:
        return False, "no_post_quote", {"labels": labels_pre}

    now_meta = _route_meta_from_quote(q2)
    now_meta["probe_lamports_in"] = lamports_in
    now_meta["slippage_bps"] = int(slippage_bps)

    # include deviation vs pre
    pre_out = 0.0
    with contextlib.suppress(Exception):
        pre_out = float(pre_meta.get("out_amount", 0.0) or 0.0)
    now_out = float(now_meta.get("out_amount", 0.0) or 0.0)

    now_meta["deviation_bps_vs_pre"] = 0.0
    if pre_out > 0 and now_out > 0:
        dev = (pre_out - now_out) / pre_out * 10_000.0
        now_meta["deviation_bps_vs_pre"] = float(dev)

        if dev > float(WATCHDOG_ALERT_DEVIATION_BPS):
            with contextlib.suppress(Exception):
                await _count_watchdog_hit(labels_pre)
            return False, f"post_deviation_bps:{dev:.0f}", now_meta

    return True, "ok", now_meta


__all__ = [
    "LAMPORTS",
    "SOL_MINT",
    "posttrade_watchdog",
    "precheck_buy",
]
