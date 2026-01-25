from __future__ import annotations

"""
scanner/dexscreener.py

Dexscreener Scanner — Backwards-Compatible Wrapper (LOCK-IN)
============================================================

Canonical implementation lives in:
    scanner/dexscreener_scanner.py

This module exists ONLY to preserve old imports and entrypoints while ensuring:
- Zero network / heavy work at import time
- Render + local behave identically
- Safe degradation: if canonical impl fails to import, wrapper returns empty lists
  and logs a throttled error (never hard-crash the whole bot)

NEW (God-Tier contract hardening)
---------------------------------
Adds an explicit OFFLINE contract flag in the scan signature so the test suite
can validate candidate schema without Redis/RPC/HTTP:
    scan_dexscreener(offline=True) -> deterministic sample candidates

Public API (legacy-compatible)
------------------------------
- async scan_dexscreener(*args, **kwargs) -> List[Dict[str, Any]]
- async scan_once(*args, **kwargs) -> List[Dict[str, Any]]     (alias)
- async run_scanner(*args, **kwargs) -> None
- async run(*args, **kwargs) -> None                           (alias)
- async main(*args, **kwargs) -> None                          (alias)

Also exposes __getattr__/__dir__ to forward other attributes from canonical impl.
"""

import hashlib
import importlib
import logging
import time
from collections.abc import Callable
from types import ModuleType
from typing import Any

# -----------------------------------------------------------------------------
# Logging (prefers canonical logging_setup)
# -----------------------------------------------------------------------------
try:
    from utils_pkg.logging_setup import get_logger as _get_logger  # preferred
except Exception:  # pragma: no cover
    try:
        from logging_setup import get_logger as _get_logger  # root shim fallback
    except Exception:  # pragma: no cover
        _get_logger = None  # type: ignore

if _get_logger:
    try:
        LOG = _get_logger("scanner.dexscreener.wrapper")  # type: ignore[misc]
    except TypeError:  # pragma: no cover
        LOG = _get_logger()  # type: ignore[misc]
else:
    LOG = logging.getLogger("scanner.dexscreener.wrapper")
    if not LOG.handlers:
        logging.basicConfig(level=logging.INFO)


_TRUE = {"1", "true", "t", "yes", "y", "on"}


def _now() -> float:
    return time.time()


def _truthy(x: Any) -> bool:
    if x is None:
        return False
    if isinstance(x, bool):
        return x
    s = str(x).strip().lower()
    return s in _TRUE


def _offline_enabled(
    offline: bool, dry: bool | None, no_network: bool | None, test_mode: bool | None
) -> bool:
    return bool(offline) or _truthy(dry) or _truthy(no_network) or _truthy(test_mode)


# -----------------------------------------------------------------------------
# Offline candidate generator (deterministic, schema-rich)
# -----------------------------------------------------------------------------
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
    This is intentionally a superset to avoid schema drift in tests.
    """
    now = int(time.time())
    mint = _stable_pubkey("dexscreener-offline-mint")
    pair = _stable_pubkey("dexscreener-offline-pair")

    base: dict[str, Any] = {
        # Canonical-ish identifiers
        "source": "dexscreener",
        "scanner": "dexscreener",
        "dex": "dexscreener",
        "chain": "solana",
        "ts": now,
        "time": now,
        "mint": mint,
        "token_mint": mint,
        "pair_address": pair,
        "pool": pair,
        "address": mint,
        # Symbols (canonical + legacy)
        "symbol": "DEX_OFFLINE",
        "sym": "DEX_OFFLINE",
        # Liquidity / volume / activity (canonical + legacy aliases)
        "liq_usd": 180_000.0,
        "liquidity_usd": 180_000.0,
        "liq": 180_000.0,
        "vol_5m_usd": 9_500.0,
        "volume_5m_usd": 9_500.0,
        "vol5": 9_500.0,
        "buys_5m": 14,
        "sells_5m": 7,
        "buys5": 14,
        "sells5": 7,
        "chg_5m_pct": 2.10,
        "change_5m_pct": 2.10,
        "chg5": 2.10,
        "age_min": 25.0,
        "age_minutes": 25.0,
        # Extras that are commonly expected downstream
        "price_usd": 0.000987,
        "fdv_usd": 900_000.0,
        "mcap_usd": 900_000.0,
        "txns_5m": 21,
        "trades_5m": 21,
        # Scoring placeholders
        "score": 0.50,
        "score_combined": 0.50,
        "score_label": "offline",
        "risk": "unknown",
        "flags": [],
        "notes": ["offline_contract_candidate"],
    }

    items = [base]
    if limit is None or limit <= 0:
        return items
    return items[:limit]


# -----------------------------------------------------------------------------
# Import + error throttle
# -----------------------------------------------------------------------------
_IMPL: ModuleType | None = None
_LAST_IMPORT_ERR_TS: float = 0.0
_IMPORT_ERR_COUNT: int = 0

# Throttle window for noisy import failures (seconds)
_IMPORT_ERR_TTL: float = 20.0


def _throttled_log_import_err(exc: BaseException) -> None:
    global _LAST_IMPORT_ERR_TS, _IMPORT_ERR_COUNT
    t = _now()
    if (t - _LAST_IMPORT_ERR_TS) >= _IMPORT_ERR_TTL:
        _LAST_IMPORT_ERR_TS = t
        _IMPORT_ERR_COUNT += 1
        LOG.exception(
            "Dexscreener wrapper: failed to import canonical scanner "
            "(scanner.dexscreener_scanner). Degrading to no-op. err=%s",
            f"{type(exc).__name__}: {exc}",
        )


def _import_module_candidates() -> tuple[str, ...]:
    """
    Import paths that work whether 'scanner' is a package or the module is invoked
    in different ways (pytest, Render start, local scripts, etc).
    """
    return (
        "scanner.dexscreener_scanner",
        ".dexscreener_scanner",
    )


def _load_impl() -> ModuleType | None:
    """
    Load and cache the canonical implementation.
    Never raises: returns None on failure.
    """
    global _IMPL
    if _IMPL is not None:
        return _IMPL

    last_exc: BaseException | None = None
    for modpath in _import_module_candidates():
        try:
            if modpath.startswith("."):
                if not __package__:
                    continue
                _IMPL = importlib.import_module(modpath, package=__package__)
            else:
                _IMPL = importlib.import_module(modpath)
            return _IMPL
        except BaseException as e:  # pragma: no cover
            last_exc = e
            continue

    if last_exc is not None:
        _throttled_log_import_err(last_exc)
    return None


def _pick_callable(mod: ModuleType, names: tuple[str, ...]) -> Callable[..., Any] | None:
    for n in names:
        fn = getattr(mod, n, None)
        if callable(fn):
            return fn
    return None


async def _call_async(fn: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
    out = fn(*args, **kwargs)
    if hasattr(out, "__await__"):
        return await out
    return out


# -----------------------------------------------------------------------------
# Public API (legacy-compatible) + OFFLINE contract flag
# -----------------------------------------------------------------------------
async def scan_dexscreener(
    *args: Any,
    offline: bool = False,
    dry: bool | None = None,
    no_network: bool | None = None,
    test_mode: bool | None = None,
    limit: int | None = None,
    **kwargs: Any,
) -> list[dict[str, Any]]:
    """
    Legacy entrypoint expected by older orchestrators.

    OFFLINE MODE (for tests/contract runs):
      scan_dexscreener(offline=True) -> deterministic sample candidates (no I/O)

    LIVE MODE:
      forwards to canonical implementation if available; otherwise safe empty list.
    """
    # Accept offline flags if caller passes via kwargs (older harnesses)
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

    # Prefer canonical names but be flexible
    fn = _pick_callable(impl, ("scan_dexscreener", "scan", "scan_once"))
    if fn is None:
        return []

    try:
        res = await _call_async(fn, *args, **kwargs)
        return res if isinstance(res, list) else []
    except Exception as e:
        LOG.exception(
            "Dexscreener wrapper: scan_dexscreener crashed (degrading). err=%s",
            f"{type(e).__name__}: {e}",
        )
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
    """Convenience alias; keeps signature compatible with offline contract flags."""
    return await scan_dexscreener(
        *args,
        offline=offline,
        dry=dry,
        no_network=no_network,
        test_mode=test_mode,
        limit=limit,
        **kwargs,
    )


async def run_scanner(*args: Any, **kwargs: Any) -> None:
    """
    Legacy runner entrypoint expected by older launch scripts.
    """
    impl = _load_impl()
    if impl is None:
        return
    fn = _pick_callable(impl, ("run_scanner", "run", "main"))
    if fn is None:
        return
    try:
        await _call_async(fn, *args, **kwargs)
    except Exception as e:
        LOG.exception(
            "Dexscreener wrapper: run_scanner crashed (degrading). err=%s",
            f"{type(e).__name__}: {e}",
        )


async def run(*args: Any, **kwargs: Any) -> None:
    await run_scanner(*args, **kwargs)


async def main(*args: Any, **kwargs: Any) -> None:
    await run_scanner(*args, **kwargs)


# -----------------------------------------------------------------------------
# Attribute forwarding (keeps old imports working)
# -----------------------------------------------------------------------------
def __getattr__(name: str) -> Any:
    impl = _load_impl()
    if impl is None:
        raise AttributeError(name)
    return getattr(impl, name)


def __dir__() -> list[str]:
    base = set(globals().keys())
    impl = _load_impl()
    if impl is not None:
        base.update(dir(impl))
    return sorted(base)


__all__ = [
    "main",
    "run",
    "run_scanner",
    "scan_dexscreener",
    "scan_once",
]
