from __future__ import annotations

"""
scanner/pumpfun.py

Backwards-compatible Pump.fun scanner entrypoint.

Canonical implementation lives in:
    scanner/pumpfun_scanner.py

This file is intentionally a *thin, safe wrapper*:
- No network at import time
- Lazy import of the heavy implementation (faster cold starts)
- Graceful degradation: if impl cannot be imported/called -> return [] / no-op
- Preserves older imports:
      from scanner.pumpfun import scan_pumpfun
- Proxies extra symbols via PEP-562 __getattr__ / __dir__

NEW (God-Tier test contract hardening)
--------------------------------------
Adds an explicit OFFLINE contract flag in the scan signature so the test suite
can validate candidate schema without Redis/RPC/HTTP:
    scan_pumpfun(offline=True) -> deterministic sample candidates
"""

import contextlib
import hashlib
import importlib
import logging
import os
import time
from typing import Any

# ──────────────────────────────────────────────────────────────────────────────
# Logging (canonical if available; falls back to stdlib)
# ──────────────────────────────────────────────────────────────────────────────
try:
    from logging_setup import get_logger  # type: ignore

    log = get_logger("scanner.pumpfun")
except Exception:  # pragma: no cover
    log = logging.getLogger("scanner.pumpfun")
    if not log.handlers:
        logging.basicConfig(level=logging.INFO)

# Provide .every shim if your logger doesn't implement it
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
                log.info("%s %s", (msg or key), kw)

    log.every = _every  # type: ignore[attr-defined]

_TRUE = {"1", "true", "t", "yes", "y", "on"}


def _truthy(x: Any) -> bool:
    if x is None:
        return False
    if isinstance(x, bool):
        return x
    return str(x).strip().lower() in _TRUE


def _offline_enabled(
    offline: bool, dry: bool | None, no_network: bool | None, test_mode: bool | None
) -> bool:
    return bool(offline) or _truthy(dry) or _truthy(no_network) or _truthy(test_mode)


# ──────────────────────────────────────────────────────────────────────────────
# Offline candidate generator (deterministic, schema-rich)
# ──────────────────────────────────────────────────────────────────────────────
_B58_ALPH = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"


def _b58encode(data: bytes) -> str:
    """Tiny base58 encoder (no deps). Produces Solana-valid strings for 32-byte inputs."""
    if not data:
        return ""
    n = int.from_bytes(data, "big")
    out: list[str] = []
    while n > 0:
        n, r = divmod(n, 58)
        out.append(_B58_ALPH[r])
    # Preserve leading zeros as '1'
    pad = 0
    for b in data:
        if b == 0:
            pad += 1
        else:
            break
    return ("1" * pad) + "".join(reversed(out or ["1"]))


def _stable_pubkey(tag: str) -> str:
    h = hashlib.sha256(tag.encode("utf-8")).digest()  # 32 bytes
    return _b58encode(h)


def _offline_candidates(limit: int | None = None) -> list[dict[str, Any]]:
    """
    Deterministic candidates that satisfy both canonical and legacy fields.
    Superset-on-purpose to avoid schema drift in tests.
    """
    now = int(time.time())
    mint = _stable_pubkey("pumpfun-offline-mint")
    pair = _stable_pubkey("pumpfun-offline-pair")

    base: dict[str, Any] = {
        # Canonical-ish identifiers
        "source": "pumpfun",
        "scanner": "pumpfun",
        "dex": "pumpfun",
        "chain": "solana",
        "ts": now,
        "time": now,
        "mint": mint,
        "token_mint": mint,
        "pair_address": pair,
        "pool": pair,
        "address": mint,
        # Symbols (canonical + legacy)
        "symbol": "PUMP_OFFLINE",
        "sym": "PUMP_OFFLINE",
        # Liquidity / volume / activity (canonical + legacy aliases)
        "liq_usd": 95_000.0,
        "liquidity_usd": 95_000.0,
        "liq": 95_000.0,
        "vol_5m_usd": 7_250.0,
        "volume_5m_usd": 7_250.0,
        "vol5": 7_250.0,
        "buys_5m": 18,
        "sells_5m": 11,
        "buys5": 18,
        "sells5": 11,
        "chg_5m_pct": 3.25,
        "change_5m_pct": 3.25,
        "chg5": 3.25,
        "age_min": 8.0,
        "age_minutes": 8.0,
        # Pump.fun-ish extras (harmless if unused)
        "pumpfun": True,
        "bonding_curve": True,
        "curve_progress": 0.42,
        "holders": 120,
        # Extras commonly expected downstream
        "price_usd": 0.000041,
        "fdv_usd": 410_000.0,
        "mcap_usd": 410_000.0,
        "txns_5m": 29,
        "trades_5m": 29,
        # Scoring placeholders
        "score": 0.55,
        "score_combined": 0.55,
        "score_label": "offline",
        "risk": "unknown",
        "flags": [],
        "notes": ["offline_contract_candidate"],
    }

    items = [base]
    if limit is None or limit <= 0:
        return items
    return items[:limit]


# ──────────────────────────────────────────────────────────────────────────────
# Wrapper controls
# ──────────────────────────────────────────────────────────────────────────────
# Allow overriding the impl module path (useful for quarantining experiments)
_IMPL_DOTTED = (
    os.getenv("PUMPFUN_IMPL_MODULE", "scanner.pumpfun_scanner").strip() or "scanner.pumpfun_scanner"
)

# Rate-limit import failures to avoid spam loops
_IMPORT_ERR_COOLDOWN_S = float(os.getenv("PUMPFUN_IMPL_IMPORT_ERR_COOLDOWN_S", "15") or 15)

# Cache
_IMPL: Any | None = None
_LAST_IMPORT_ERR_TS: float = 0.0


def _short(x: Any, n: int = 240) -> str:
    try:
        s = str(x)
        return s if len(s) <= n else s[:n] + "…"
    except Exception:
        return "<?>"


def _import_candidates() -> tuple[str, ...]:
    # Prefer sibling relative import, but be robust across invocation styles.
    return (
        ".pumpfun_scanner",
        "scanner.pumpfun_scanner",
        "pumpfun_scanner",
        _IMPL_DOTTED,
    )


def _load_impl(force_reload: bool = False) -> Any | None:
    """
    Lazy-load the implementation module. Never raises to callers.

    Args:
        force_reload: if True, re-imports the module (best-effort).
    """
    global _IMPL, _LAST_IMPORT_ERR_TS

    if _IMPL is not None and not force_reload:
        return _IMPL

    last_exc: Exception | None = None
    for modpath in _import_candidates():
        try:
            if modpath.startswith("."):
                if not __package__:
                    continue
                mod = importlib.import_module(modpath, __package__)
            else:
                mod = importlib.import_module(modpath)

            _IMPL = mod
            return _IMPL
        except Exception as e:
            last_exc = e
            continue

    if last_exc is not None:
        now = time.time()
        if (now - _LAST_IMPORT_ERR_TS) >= _IMPORT_ERR_COOLDOWN_S:
            _LAST_IMPORT_ERR_TS = now
            with contextlib.suppress(Exception):
                log.exception(
                    "pumpfun wrapper: failed to import impl=%s err=%s",
                    _IMPL_DOTTED,
                    _short(last_exc),
                )
    return None


def pumpfun_impl_health() -> dict[str, Any]:
    """
    Wrapper-level health snapshot (does not force network).
    """
    impl = _load_impl()
    health: dict[str, Any] = {
        "impl_module": _IMPL_DOTTED,
        "impl_loaded": bool(impl is not None),
    }
    if impl is None:
        return health

    fn = getattr(impl, "pumpfun_scanner_health", None)
    if callable(fn):
        try:
            h = fn()
            if isinstance(h, dict):
                health["impl_health"] = h
        except Exception as e:
            health["impl_health_err"] = _short(e)
    return health


async def scan_pumpfun(
    *args: Any,
    offline: bool = False,
    dry: bool | None = None,
    no_network: bool | None = None,
    test_mode: bool | None = None,
    limit: int | None = None,
    **kwargs: Any,
) -> list[dict[str, Any]]:
    """
    Backwards-compatible entrypoint.

    OFFLINE MODE (for tests/contract runs):
      scan_pumpfun(offline=True) -> deterministic sample candidates (no I/O)

    LIVE MODE:
      forwards to canonical implementation if available; otherwise safe empty list.

    Returns:
        List[CandidateSnapshot-like dict]
    """
    # Accept offline flags passed through kwargs (older harnesses / generic callers)
    if "offline" in kwargs and not offline:
        offline = _truthy(kwargs.pop("offline"))
    if "dry_run" in kwargs and dry is None:
        dry = _truthy(kwargs.pop("dry_run"))
    if "dry" in kwargs and dry is None:
        dry = _truthy(kwargs.pop("dry"))
    if "no_network" in kwargs and no_network is None:
        no_network = _truthy(kwargs.pop("no_network"))
    if "test_mode" in kwargs and test_mode is None:
        test_mode = _truthy(kwargs.pop("test_mode"))
    if "limit" in kwargs and limit is None:
        try:
            limit = int(kwargs.pop("limit"))
        except Exception:
            kwargs.pop("limit", None)

    if _offline_enabled(offline, dry, no_network, test_mode):
        return _offline_candidates(limit=limit)

    impl = _load_impl()
    if impl is None:
        return []

    fn = (
        getattr(impl, "scan_pumpfun", None)
        or getattr(impl, "scan", None)
        or getattr(impl, "scan_once", None)
    )
    if not callable(fn):
        return []

    try:
        out = fn(*args, **kwargs)
        if hasattr(out, "__await__"):
            return await out
        return out if isinstance(out, list) else []
    except Exception as e:
        # Never take down the strategy loop because a scanner impl hiccuped.
        log.every("PUMPFUN_WRAPPER_CALL_FAIL", n=10, impl=_IMPL_DOTTED, err=_short(e))
        return []


async def scan_once(
    *args: Any,
    offline: bool = False,
    dry: bool | None = None,
    no_network: bool | None = None,
    test_mode: bool | None = None,
    limit: int | None = None,
    **kwargs: Any,
) -> list[dict[str, Any]]:
    """
    Alias used by some older scripts/tests. Keeps signature compatible with offline flags.
    """
    return await scan_pumpfun(
        *args,
        offline=offline,
        dry=dry,
        no_network=no_network,
        test_mode=test_mode,
        limit=limit,
        **kwargs,
    )


async def shutdown_pumpfun_scanner() -> None:
    """
    Optional shutdown hook; safe to call multiple times.
    """
    impl = _load_impl()
    if impl is None:
        return

    fn = getattr(impl, "shutdown_pumpfun_scanner", None)
    if fn is None:
        return

    with contextlib.suppress(Exception):
        out = fn()
        if hasattr(out, "__await__"):
            await out


def __getattr__(name: str) -> Any:
    """
    Proxy additional symbols to the canonical implementation.

    This preserves compatibility with older code that expects extra exports like:
      - pumpfun_scanner_health
      - scan_loop
      - SCANNER_VERSION, etc.
    """
    impl = _load_impl()
    if impl is None:
        raise AttributeError(name)
    return getattr(impl, name)


def __dir__() -> list[str]:
    impl = _load_impl()
    base = set(globals().keys())
    if impl is not None:
        with contextlib.suppress(Exception):
            base.update(dir(impl))
    return sorted(base)


__all__ = [
    "pumpfun_impl_health",
    "scan_once",
    "scan_pumpfun",
    "shutdown_pumpfun_scanner",
]
