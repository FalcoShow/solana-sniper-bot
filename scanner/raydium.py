# scanner/raydium.py
from __future__ import annotations

"""
scanner/raydium.py — Raydium scanner compatibility shim (V3+++ / God-Tier LOCK-IN)
================================================================================

CANONICAL IMPLEMENTATION
------------------------
The real scanner lives in:
    scanner/raydium_scanner.py

This module exists ONLY to preserve legacy imports like:
    from scanner.raydium import scan_raydium
    import scanner.raydium as raydium

Hard rules
----------
- Zero behavior drift in LIVE mode: forward to scanner.raydium_scanner at call time
- No network I/O at import time (Render-safe)
- Lazy-load the canonical module (fast import; avoids side-effects)
- Add an OFFLINE contract flag so tests can validate schema without Redis/RPC

Offline contract mode
---------------------
When offline/dry/no_network/test_mode is enabled, this shim returns deterministic,
schema-rich sample candidates (no Redis, no RPC, no HTTP).

Env toggles
-----------
- RAYDIUM_SHIM_WARN=1   -> warn once on import + once on first use
"""

import contextlib
import hashlib
import importlib
import os
import time
from functools import lru_cache
from types import ModuleType
from typing import TYPE_CHECKING, Any

# ──────────────────────────────────────────────────────────────────────────────
# Logger (best-effort, never hard-fail)
# ──────────────────────────────────────────────────────────────────────────────
try:
    from logging_setup import get_logger  # type: ignore

    try:
        _log = get_logger("scanner.raydium")  # type: ignore[arg-type]
    except TypeError:
        _log = get_logger()  # type: ignore[misc]
except Exception:  # pragma: no cover
    import logging

    _log = logging.getLogger("scanner.raydium")


_TRUE = {"1", "true", "t", "yes", "y", "on"}
_WARN = str(os.getenv("RAYDIUM_SHIM_WARN", "")).strip().lower() in _TRUE
_warned_import = False
_warned_use = False


def _warn_once(kind: str, msg: str) -> None:
    global _warned_import, _warned_use
    if not _WARN:
        return
    if kind == "import":
        if _warned_import:
            return
        _warned_import = True
    elif kind == "use":
        if _warned_use:
            return
        _warned_use = True
    try:
        _log.warning(msg)
    except Exception:
        return


@lru_cache(maxsize=1)
def _impl() -> ModuleType:
    """
    Lazy-load the canonical implementation module.
    IMPORTANT: raydium_scanner.py must not perform network I/O at import time.
    """
    return importlib.import_module(".raydium_scanner", package=__package__)


def _get(name: str, default: Any = None) -> Any:
    return getattr(_impl(), name, default)


if TYPE_CHECKING:  # pragma: no cover
    from . import raydium_scanner as _raydium_scanner  # noqa: F401


# ──────────────────────────────────────────────────────────────────────────────
# Offline candidate generator (deterministic, schema-rich)
# ──────────────────────────────────────────────────────────────────────────────
_B58_ALPH = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"


def _b58encode(data: bytes) -> str:
    """Tiny base58 encoder (no deps). Produces Solana-valid strings for 32-byte inputs."""
    if not data:
        return ""
    n = int.from_bytes(data, "big")
    out = []
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
    # sha256 = 32 bytes => good for Solana pubkey shape
    h = hashlib.sha256(tag.encode("utf-8")).digest()
    return _b58encode(h)


def _offline_candidates(limit: int | None = None) -> list[dict]:
    """
    Return deterministic candidates that satisfy BOTH:
      - canonical schema expectations (preferred)
      - fallback legacy schema (sym/liq/vol5/buys5/sells5/chg5/age_min)

    We intentionally include a superset of fields to avoid schema drift.
    """
    now = int(time.time())
    mint = _stable_pubkey("raydium-offline-mint")
    pool = _stable_pubkey("raydium-offline-pool")
    base = {
        # Canonical-ish identifiers
        "source": "raydium",
        "scanner": "raydium",
        "dex": "raydium",
        "chain": "solana",
        "ts": now,
        "time": now,
        "mint": mint,
        "token_mint": mint,
        "pool": pool,
        "pair_address": pool,
        "address": mint,
        "symbol": "RAY_OFFLINE",
        "sym": "RAY_OFFLINE",
        # Liquidity / volume / activity (canonical + legacy aliases)
        "liq_usd": 250_000.0,
        "liquidity_usd": 250_000.0,
        "liq": 250_000.0,
        "vol_5m_usd": 12_500.0,
        "volume_5m_usd": 12_500.0,
        "vol5": 12_500.0,
        "buys_5m": 18,
        "sells_5m": 9,
        "buys5": 18,
        "sells5": 9,
        "chg_5m_pct": 3.25,
        "change_5m_pct": 3.25,
        "chg5": 3.25,
        "age_min": 42.0,
        "age_minutes": 42.0,
        # Extra fields that downstream feature builders often like
        "price_usd": 0.001234,
        "fdv_usd": 1_250_000.0,
        "mcap_usd": 1_250_000.0,
        "txns_5m": 27,
        "trades_5m": 27,
        # Scoring placeholders (safe defaults)
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


# ──────────────────────────────────────────────────────────────────────────────
# Public API (forwarders + OFFLINE contract flag)
# ──────────────────────────────────────────────────────────────────────────────
async def scan_raydium(
    *args: Any,
    offline: bool = False,
    dry: bool | None = None,
    no_network: bool | None = None,
    test_mode: bool | None = None,
    limit: int | None = None,
    **kwargs: Any,
) -> list[dict]:
    """
    Primary entrypoint.

    OFFLINE MODE (for tests/contract runs):
      scan_raydium(offline=True) -> returns deterministic sample candidates (no I/O)

    LIVE MODE:
      forwards 1:1 to scanner.raydium_scanner.scan_raydium
    """
    _warn_once("use", "scanner.raydium is a compatibility shim; prefer scanner.raydium_scanner")

    # Accept offline flags even if caller passes via kwargs
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

    fn = _get("scan_raydium")
    if not callable(fn):
        raise RuntimeError("scanner.raydium_scanner.scan_raydium is missing")
    return await fn(*args, **kwargs)  # type: ignore[misc]


async def scan(
    *args: Any,
    offline: bool = False,
    dry: bool | None = None,
    no_network: bool | None = None,
    test_mode: bool | None = None,
    limit: int | None = None,
    **kwargs: Any,
) -> list[dict]:
    """Common alias some harnesses look for."""
    return await scan_raydium(
        *args,
        offline=offline,
        dry=dry,
        no_network=no_network,
        test_mode=test_mode,
        limit=limit,
        **kwargs,
    )


async def scan_once(
    *args: Any,
    offline: bool = False,
    dry: bool | None = None,
    no_network: bool | None = None,
    test_mode: bool | None = None,
    limit: int | None = None,
    **kwargs: Any,
) -> list[dict]:
    _warn_once("use", "scanner.raydium is a compatibility shim; prefer scanner.raydium_scanner")
    if _offline_enabled(offline, dry, no_network, test_mode):
        return _offline_candidates(limit=limit)

    fn = _get("scan_once", None) or _get("scan_raydium")
    if not callable(fn):
        raise RuntimeError("scanner.raydium_scanner.scan_once/scan_raydium is missing")
    return await fn(*args, **kwargs)  # type: ignore[misc]


async def run(*args: Any, **kwargs: Any) -> None:
    _warn_once("use", "scanner.raydium is a compatibility shim; prefer scanner.raydium_scanner")
    fn = _get("run")
    if not callable(fn):
        raise RuntimeError("scanner.raydium_scanner.run is missing")
    await fn(*args, **kwargs)  # type: ignore[misc]


async def run_scanner(*args: Any, **kwargs: Any) -> None:
    _warn_once("use", "scanner.raydium is a compatibility shim; prefer scanner.raydium_scanner")
    fn = _get("run_scanner", None) or _get("run")
    if not callable(fn):
        raise RuntimeError("scanner.raydium_scanner.run_scanner/run is missing")
    await fn(*args, **kwargs)  # type: ignore[misc]


async def scan_loop(*args: Any, **kwargs: Any) -> None:
    _warn_once("use", "scanner.raydium is a compatibility shim; prefer scanner.raydium_scanner")
    fn = _get("scan_loop", None) or _get("run_scanner", None) or _get("run")
    if not callable(fn):
        raise RuntimeError("scanner.raydium_scanner.scan_loop/run_scanner/run is missing")
    await fn(*args, **kwargs)  # type: ignore[misc]


__all__ = [
    "run",
    "run_scanner",
    "scan",
    "scan_loop",
    "scan_once",
    "scan_raydium",
]


# ──────────────────────────────────────────────────────────────────────────────
# Compatibility: attribute forwarding
# ──────────────────────────────────────────────────────────────────────────────
def __getattr__(name: str) -> Any:
    """
    Forward ANY missing attribute to scanner.raydium_scanner lazily.
    This preserves older code that imported helpers/constants from scanner.raydium.
    """
    mod = _impl()
    try:
        return getattr(mod, name)
    except AttributeError as e:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}") from e


def __dir__() -> list[str]:
    out = set(globals().keys())
    with contextlib.suppress(Exception):
        out.update(dir(_impl()))
    return sorted(out)


_warn_once(
    "import",
    "Imported scanner.raydium shim; prefer scanner.raydium_scanner (set RAYDIUM_SHIM_WARN=0 to silence)",
)


if __name__ == "__main__":  # pragma: no cover
    import asyncio

    async def _main() -> None:
        res = await scan_raydium(offline=True)
        print("offline candidates:", len(res))
        if res:
            r = res[0]
            print(
                r.get("symbol"),
                r.get("mint"),
                r.get("score_combined"),
                r.get("score_label"),
                r.get("vol_5m_usd"),
                r.get("liq_usd"),
            )

    asyncio.run(_main())
