# routeguard.py
from __future__ import annotations

"""
routeguard.py (ROOT) — V3+++ (God-Tier Overhaul) — WRAPPER / COMPAT LAYER
========================================================================

This ROOT module is **legacy API compatibility only**.

✅ Canonical guard logic lives in: `utils_pkg.routeguard`
    - async preflight(mint, lamports_in) -> tuple[bool, dict | None]
    - clear_cache(...)
    - stats() -> Mapping

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
- Deterministic import: delegates to utils_pkg.routeguard only (lazy import).
- Never raises from check_routes(); always returns a decision dict.
- Cancellation-safe: never swallow asyncio.CancelledError.
"""

import asyncio
import contextlib
import os
import time
from typing import Any

# ──────────────────────────────────────────────────────────────────────────────
# Logging (shim-safe, mypy-safe)
# ──────────────────────────────────────────────────────────────────────────────
log: Any
try:
    from logging_setup import get_logger  # type: ignore

    log = get_logger(__name__)  # type: ignore[misc]
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

        def _every(key: str, n: float = 30.0, msg: str | None = None, **kw: Any) -> bool:
            """
            Throttle events by key. Returns True when it logged.
            Uses time.monotonic() for stable throttling.
            """
            try:
                now = time.monotonic()
                store = getattr(_every, "_store", {})  # type: ignore[attr-defined]
                last = float(store.get(key, 0.0))
                sec = float(max(0.5, float(n)))
                if (now - last) >= sec:
                    store[key] = now
                    _every._store = store  # type: ignore[attr-defined]
                    ev = getattr(log, "event", None)
                    if callable(ev):
                        ev(msg or key, **kw)
                    else:
                        log.info("%s %s", msg or key, kw)
                    return True
            except Exception:
                return False
            return False

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
    v = _envs(k, str(d))
    try:
        return int(float(v))
    except Exception:
        return int(d)


# Legacy probe input for the guard preflight (lamports-in)
# NOTE: canonical guard enforces min/max spend; this is just the probe size.
_BUY_PROBE_LAMPORTS = max(1, _envi("ROUTEGUARD_BUY_PROBE_LAMPORTS", 1_000_000))  # 0.001 SOL

# ──────────────────────────────────────────────────────────────────────────────
# Canonical guard import (lazy to avoid cycles)
# ──────────────────────────────────────────────────────────────────────────────
_rg_mod: Any | None = None


def _get_rg() -> Any | None:
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


def _safe_stats(rg: Any) -> dict[str, Any]:
    """
    Best-effort stats snapshot.
    Must never raise.
    """
    try:
        st = rg.stats()  # type: ignore[attr-defined]
    except Exception:
        return {}

    if isinstance(st, dict):
        return dict(st)

    try:
        # mapping-like or iterable pairs
        return dict(st)  # type: ignore[arg-type]
    except Exception:
        return {}


# ──────────────────────────────────────────────────────────────────────────────
# Quote parsing helpers (tolerant)
# ──────────────────────────────────────────────────────────────────────────────
def _impact_bps_from_quote(q: dict[str, Any]) -> int | None:
    if not isinstance(q, dict) or not q:
        return None

    # Prefer Jupiter-ish field
    with contextlib.suppress(Exception):
        v = q.get("priceImpactBps")
        if v is not None:
            return int(abs(float(v)))

    # Alternate pct representation
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
        hops = len([x for x in rp if isinstance(x, dict)])
        return hops or None

    data = q.get("data")
    if isinstance(data, dict) and isinstance(data.get("routePlan"), list):
        rp2 = data["routePlan"]
        hops = len([x for x in rp2 if isinstance(x, dict)])
        return hops or None

    return None


def _no_routes_from_quote(q: dict[str, Any]) -> bool:
    hops = _hops_from_quote(q)
    if hops is None:
        return True
    return hops <= 0


# ──────────────────────────────────────────────────────────────────────────────
# Optional extra wrappers (keep import-safe). Not exported in __all__.
# ──────────────────────────────────────────────────────────────────────────────
def stats() -> dict[str, Any]:
    rg = _get_rg()
    if rg is None:
        return {"enabled": False, "canonical_present": False, "wrapper_only": True}
    st = _safe_stats(rg)
    st.setdefault("canonical_present", True)
    st.setdefault("wrapper_only", True)
    return st


def clear_cache() -> None:
    rg = _get_rg()
    if rg is None:
        return
    fn = getattr(rg, "clear_cache", None)
    if callable(fn):
        with contextlib.suppress(Exception):
            fn()


# ──────────────────────────────────────────────────────────────────────────────
# Public API (LEGACY)
# ──────────────────────────────────────────────────────────────────────────────
async def check_routes(mint: str) -> dict[str, Any]:
    """
    Legacy compat API. Never raises (except CancelledError).

    Delegates decision to utils_pkg.routeguard.preflight().

    Semantics mapping:
      - canonical missing           -> block (fail-closed)
      - (ok=False, _)              -> block
      - (ok=True, quote={})        -> allow (guard optimistic / no enforced prequote)
      - (ok=True, quote=None)      -> allow BUT fallback-only (router must not call Jupiter)
      - (ok=True, quote=dict)      -> allow with prequote + rg metadata
    """
    try:
        mint_s = (mint or "").strip()
        if not mint_s:
            return {
                "action": "block",
                "reason": "missing_mint",
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

        log.every(
            "ROOT_ROUTEGUARD_WRAPPER",
            900.0,
            msg="ROOT_ROUTEGUARD_WRAPPER",
            note="routeguard.py is wrapper; canonical is utils_pkg.routeguard",
        )

        rg = _get_rg()
        if rg is None:
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

        try:
            ok, preq = await rg.preflight(  # type: ignore[attr-defined]
                mint=mint_s,
                lamports_in=int(_BUY_PROBE_LAMPORTS),
            )
        except asyncio.CancelledError:
            raise
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

        st = _safe_stats(rg)

        policy: dict[str, Any] = {
            "allow_no_route": bool(st.get("allow_no_buy_route", 0)),
            "require_sell": bool(st.get("require_sell_route", 0)),
            "max_hops": int(st.get("max_hops", 0) or 0),
            "max_impact_bps": int(st.get("max_impact_bps", 0) or 0),
            "probe_lamports_in": _BUY_PROBE_LAMPORTS,
            "enabled": bool(st.get("enabled", 1)),
            "wrapper_only": True,
            "canonical_present": True,
            "global_cooldown_active": bool(st.get("global_cooldown_active", 0)),
            "auth_cooldown_active": bool(st.get("auth_cooldown_active", 0)),
        }

        if not ok:
            return {
                "action": "block",
                "reason": "blocked",
                "no_routes": True,
                "hops": None,
                "impact_bps": None,
                "buy_quote": {},
                "sell_quote": {},
                "policy": policy,
            }

        if preq is None:
            policy["note"] = "fallback_only means: do NOT call Jupiter; use fallback execution path"
            return {
                "action": "allow",
                "reason": "fallback_only",
                "no_routes": True,
                "hops": None,
                "impact_bps": None,
                "buy_quote": {},
                "sell_quote": {},
                "policy": policy,
            }

        buy_quote: dict[str, Any]
        if isinstance(preq, dict):
            buy_quote = dict(preq)
        else:
            buy_quote = {}
            log.every(
                "ROOT_ROUTEGUARD_BAD_PREQUOTE",
                120.0,
                msg="ROOT_ROUTEGUARD_BAD_PREQUOTE",
                mint=mint_s,
                got=type(preq).__name__,
            )

        # Pull route meta if canonical embedded it (mypy-safe narrowing)
        meta_any: Any = buy_quote.get("__rg")
        rg_meta: dict[str, Any] = dict(meta_any) if isinstance(meta_any, dict) else {}

        hops: int | None = None
        impact: int | None = None

        with contextlib.suppress(Exception):
            v = rg_meta.get("hops")
            hops = int(v) if v is not None else None
        with contextlib.suppress(Exception):
            v = rg_meta.get("impact_bps")
            impact = int(v) if v is not None else None

        if hops is None:
            hops = _hops_from_quote(buy_quote)
        if impact is None:
            impact = _impact_bps_from_quote(buy_quote)

        # If buy_quote is empty (guard optimistic), do NOT signal no_routes.
        no_routes = _no_routes_from_quote(buy_quote) if buy_quote else False
        reason = "routed" if buy_quote else "allow_no_prequote"

        return {
            "action": "allow",
            "reason": reason,
            "no_routes": bool(no_routes),
            "hops": hops,
            "impact_bps": impact,
            "buy_quote": buy_quote,
            "sell_quote": {},
            "policy": policy,
        }

    except asyncio.CancelledError:
        raise
    except Exception as e:
        log.event("ROOT_ROUTEGUARD_FATAL", err=_short(e, 500))
        return {
            "action": "block",
            "reason": "wrapper_error",
            "no_routes": True,
            "hops": None,
            "impact_bps": None,
            "buy_quote": {},
            "sell_quote": {},
            "policy": {
                "probe_lamports_in": _BUY_PROBE_LAMPORTS,
                "wrapper_only": True,
                "canonical_present": bool(_get_rg() is not None),
                "error": _short(e, 220),
            },
        }


__all__ = ["check_routes"]
