# routeguard.py
from __future__ import annotations

"""
routeguard.py (ROOT) — V3+++ (God-Tier Overhaul) — WRAPPER / COMPAT LAYER
========================================================================

This ROOT module is **legacy API compatibility only**.

✅ Canonical guard logic lives in: `utils_pkg/routeguard.py`
    - preflight(mint, lamports_in) -> (ok, prequote_or_none)
    - clear_cache(...)
    - stats()

This wrapper preserves the older public contract used by older wiring:
    async def check_routes(mint: str) -> dict

Returned dict shape (legacy):
{
  "action": "allow" | "block",
  "reason": str,
  "no_routes": bool,
  "hops": int | None,
  "impact_bps": int | None,
  "buy_quote": dict,
  "sell_quote": dict,   # legacy field (canonical guard does not return sell quote)
  "policy": dict,
}

Hard requirements
-----------------
- NO network at import time.
- Deterministic behavior: delegates to utils_pkg.routeguard only.
- Never raises from check_routes(); always returns a decision dict.
"""

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
    """Ensure log.event(...) and log.every(...) exist (works with std logging too)."""
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


# ──────────────────────────────────────────────────────────────────────────────
# Env helpers (local-only, safe at import)
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


# Legacy probe input for the guard preflight (lamports-in)
# NOTE: canonical guard enforces min/max spend; this is just the probe size.
_BUY_PROBE_LAMPORTS = max(1, _envi("ROUTEGUARD_BUY_PROBE_LAMPORTS", 1_000_000))  # 0.001 SOL


# ──────────────────────────────────────────────────────────────────────────────
# Canonical guard import (lazy to avoid cycles)
# ──────────────────────────────────────────────────────────────────────────────
_rg_mod = None


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


def _short(x: Any, n: int = 240) -> str:
    try:
        s = str(x)
        return s if len(s) <= n else (s[:n] + "…")
    except Exception:
        return "<?>"


# ──────────────────────────────────────────────────────────────────────────────
# Quote parsing helpers (tolerant)
# ──────────────────────────────────────────────────────────────────────────────
def _impact_bps_from_quote(q: dict[str, Any]) -> int | None:
    if not isinstance(q, dict) or not q:
        return None
    with contextlib.suppress(Exception):
        v = q.get("priceImpactBps")
        if v is not None:
            return int(abs(float(v)))
    with contextlib.suppress(Exception):
        pip = float(q.get("priceImpactPct") or 0.0)
        pip = abs(pip)
        if pip <= 1.0:
            return int(pip * 10_000)
        if pip <= 100.0:
            return int(pip * 100)
        return int(pip)
    return None


def _hops_from_quote(q: dict[str, Any]) -> int | None:
    if not isinstance(q, dict) or not q:
        return None
    rp = q.get("routePlan")
    if isinstance(rp, list):
        return len([x for x in rp if isinstance(x, dict)]) or None
    data = q.get("data")
    if isinstance(data, dict) and isinstance(data.get("routePlan"), list):
        rp2 = data["routePlan"]
        return len([x for x in rp2 if isinstance(x, dict)]) or None
    return None


def _no_routes_from_quote(q: dict[str, Any]) -> bool:
    # legacy meaning: "we saw no usable routes"
    hops = _hops_from_quote(q)
    if hops is None:
        return True
    return hops <= 0


# ──────────────────────────────────────────────────────────────────────────────
# Public API (LEGACY)
# ──────────────────────────────────────────────────────────────────────────────
async def check_routes(mint: str) -> dict[str, Any]:
    """
    Legacy compat API. Never raises.

    Delegates decision to utils_pkg.routeguard.preflight().

    Semantics mapping:
      - (ok=False, _)         -> action=block
      - (ok=True, quote={})   -> action=allow, but no enforced prequote (guard disabled / optimistic)
      - (ok=True, quote=None) -> action=allow, BUT fallback-only (router must not try Jupiter)
      - (ok=True, quote=...)  -> action=allow, with prequote + rg metadata
    """
    mint_s = (mint or "").strip()

    # breadcrumb: remind (rarely) that this is a wrapper
    log.every(
        "ROOT_ROUTEGUARD_WRAPPER",
        900,
        msg="ROOT_ROUTEGUARD_WRAPPER",
        note="routeguard.py is wrapper; canonical is utils_pkg.routeguard",
    )

    rg = _get_rg()
    if rg is None:
        # If canonical module missing, we must fail closed (block) rather than silently allow.
        return {
            "action": "block",
            "reason": "routeguard_missing",
            "no_routes": True,
            "hops": None,
            "impact_bps": None,
            "buy_quote": {},
            "sell_quote": {},
            "policy": {
                "probe_lamports_in": _BUY_PROBE_LAMPORTS,
                "wrapper_only": True,
                "canonical_present": False,
            },
        }

    # Call canonical preflight
    try:
        ok, preq = await rg.preflight(mint=mint_s, lamports_in=int(_BUY_PROBE_LAMPORTS))  # type: ignore[attr-defined]
    except Exception as e:
        log.event("ROOT_ROUTEGUARD_EXCEPTION", mint=mint_s, err=_short(e, 500))
        return {
            "action": "block",
            "reason": "routeguard_error",
            "no_routes": True,
            "hops": None,
            "impact_bps": None,
            "buy_quote": {},
            "sell_quote": {},
            "policy": {
                "probe_lamports_in": _BUY_PROBE_LAMPORTS,
                "wrapper_only": True,
                "canonical_present": True,
                "error": _short(e, 220),
            },
        }

    # Pull canonical stats snapshot (best-effort)
    st: dict[str, Any] = {}
    with contextlib.suppress(Exception):
        st = dict(rg.stats())  # type: ignore[attr-defined]

    # Interpret canonical outputs
    if not ok:
        return {
            "action": "block",
            "reason": "blocked",
            "no_routes": True,
            "hops": None,
            "impact_bps": None,
            "buy_quote": {},
            "sell_quote": {},
            "policy": {
                "allow_no_route": bool(st.get("allow_no_buy_route", 0)),
                "require_sell": bool(st.get("require_sell_route", 0)),
                "max_hops": int(st.get("max_hops", 0) or 0),
                "max_impact_bps": int(st.get("max_impact_bps", 0) or 0),
                "probe_lamports_in": _BUY_PROBE_LAMPORTS,
                "enabled": bool(st.get("enabled", 1)),
                "wrapper_only": True,
                "canonical_present": True,
            },
        }

    # ok=True
    if preq is None:
        # fallback-only allowed (router MUST NOT attempt Jupiter)
        return {
            "action": "allow",
            "reason": "fallback_only",
            "no_routes": True,
            "hops": None,
            "impact_bps": None,
            "buy_quote": {},
            "sell_quote": {},
            "policy": {
                "allow_no_route": bool(st.get("allow_no_buy_route", 0)),
                "require_sell": bool(st.get("require_sell_route", 0)),
                "max_hops": int(st.get("max_hops", 0) or 0),
                "max_impact_bps": int(st.get("max_impact_bps", 0) or 0),
                "probe_lamports_in": _BUY_PROBE_LAMPORTS,
                "enabled": bool(st.get("enabled", 1)),
                "wrapper_only": True,
                "canonical_present": True,
                "note": "fallback_only means: do NOT call Jupiter; use fallback execution path",
            },
        }

    # preq is a dict (may be {} meaning allow but no prequote enforced)
    buy_quote: dict[str, Any] = dict(preq) if isinstance(preq, dict) else {}
    rg_meta = buy_quote.get("__rg") if isinstance(buy_quote.get("__rg"), dict) else {}
    hops = None
    impact = None

    with contextlib.suppress(Exception):
        hops = int(rg_meta.get("hops")) if rg_meta.get("hops") is not None else None
    with contextlib.suppress(Exception):
        impact = int(rg_meta.get("impact_bps")) if rg_meta.get("impact_bps") is not None else None

    if hops is None:
        hops = _hops_from_quote(buy_quote)
    if impact is None:
        impact = _impact_bps_from_quote(buy_quote)

    no_routes = _no_routes_from_quote(buy_quote) if buy_quote else False
    reason = "routed" if buy_quote else "allow_no_prequote"

    return {
        "action": "allow",
        "reason": reason,
        "no_routes": bool(no_routes),
        "hops": hops,
        "impact_bps": impact,
        "buy_quote": buy_quote,
        "sell_quote": {},  # canonical guard probes sell internally; it does not return the quote
        "policy": {
            "allow_no_route": bool(st.get("allow_no_buy_route", 0)),
            "require_sell": bool(st.get("require_sell_route", 0)),
            "max_hops": int(st.get("max_hops", 0) or 0),
            "max_impact_bps": int(st.get("max_impact_bps", 0) or 0),
            "probe_lamports_in": _BUY_PROBE_LAMPORTS,
            "enabled": bool(st.get("enabled", 1)),
            "wrapper_only": True,
            "canonical_present": True,
            # helpful extras for debugging, without exploding logs:
            "global_cooldown_active": bool(st.get("global_cooldown_active", 0)),
            "auth_cooldown_active": bool(st.get("auth_cooldown_active", 0)),
        },
    }


__all__ = ["check_routes"]
