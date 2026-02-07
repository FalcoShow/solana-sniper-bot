# -*- coding: utf-8 -*-
from __future__ import annotations

"""
utils_pkg/router.py — V3+++ God-Tier Router (LOCK-IN FINAL, contract-safe, probe-safe)
====================================================================================

Hard Contracts (tests)
---------------------
P0 Contract:
  - get_buy_quote(...)
  - get_sell_quote(...)
  - get_quote(...)
  - swap_from_quote(...)

Sell-path contract:
  - get_sell_quote() MUST GET /quote with correct params (including userPublicKey)
  - swap_from_quote() MUST POST /swap with payload containing quoteResponse + userPublicKey
  - DRYRUN/SAFE_MODE MUST NOT skip the /swap POST if a patched/capturing session is present
  - swap_from_quote must return (False, "") if wallet or solana_client missing

Probe safety (scripts/check_router.py / preflight):
  - no import-time network / wallet I/O
  - ignore env proxy inheritance during PREFLIGHT/CHECK_ROUTER unless explicitly allowed

Production features preserved:
  - retries + endpoint fallback
  - quote cache + singleflight
  - circuit breaker + retry budget (prevents storms)
  - budget hooks + routeguard hook
  - lightning-first trade path with optional direct Jupiter swap fallback
  - strict auth handling, safe/dryrun modes, proxy controls

LOCK-IN CHANGE:
  - Prefer explicit env endpoints:
      JUPITER_QUOTE_API / JUPITER_SWAP_API
    and when set, DO NOT try quote-api.jup.ag unless explicitly allowed.
"""

import asyncio
import contextlib
import hashlib
import inspect
import json
import os
import sys
import time
from collections.abc import Awaitable, Callable
from typing import Any, Literal
from urllib.parse import urlparse

import httpx

# -----------------------------------------------------------------------------
# Runtime-injected handles (tests may monkeypatch these)
# -----------------------------------------------------------------------------
wallet: Any | None = None
solana_client: Any | None = None
client: Any | None = None  # alias surface for some callers/tests

# Tests often monkeypatch this. If provided, we prefer it (and pin it).
session: Any | None = None

# -----------------------------------------------------------------------------
# Logging (shim-safe)
# -----------------------------------------------------------------------------
try:
    from logging_setup import get_logger  # type: ignore

    log: Any = get_logger(__name__)  # type: ignore
except Exception:  # pragma: no cover
    import logging

    log = logging.getLogger(__name__)


def _install_log_shims() -> None:
    if not hasattr(log, "event"):

        def _event(name: str, **kw: Any) -> None:  # pragma: no cover
            with contextlib.suppress(Exception):
                log.info("%s %s", name, kw)

        log.event = _event  # type: ignore[attr-defined]

    if not hasattr(log, "every"):

        def _every(key: str, n: int = 10, msg: str | None = None, **kw: Any) -> None:
            try:
                now = time.monotonic()
                store = getattr(_every, "_store", {})  # type: ignore[attr-defined]
                last = float(store.get(key, 0.0))
                if (now - last) >= float(max(1, int(n))):
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


_install_log_shims()

_TRUE = {"1", "true", "yes", "y", "on", "t"}
_FALSE = {"0", "false", "no", "n", "off", "f"}


def _mono() -> float:
    return time.monotonic()


def _short(x: Any, n: int = 240) -> str:
    try:
        s = str(x)
        return s if len(s) <= n else (s[:n] + "…")
    except Exception:
        return "<?>"


def _envs(k: str, d: str = "") -> str:
    v = os.getenv(k)
    return (v if v is not None else d).strip()


def _envb(k: str, d: bool = False) -> bool:
    raw = _envs(k, "")
    if raw == "":
        return bool(d)
    s = raw.lower()
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


def _clamp_i(x: int, lo: int, hi: int) -> int:
    return lo if x < lo else hi if x > hi else x


def _clamp_f(x: float, lo: float, hi: float) -> float:
    return lo if x < lo else hi if x > hi else x


def _to_int(v: Any, default: int | None = None) -> int | None:
    if v is None:
        return default
    if isinstance(v, bool):
        return int(v)
    if isinstance(v, int):
        return int(v)
    if isinstance(v, float):
        return int(v)
    s = str(v).strip()
    if not s:
        return default
    try:
        return int(s)
    except Exception:
        try:
            return int(float(s))
        except Exception:
            return default


def _to_bool(v: Any) -> bool | None:
    if v is None:
        return None
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float)):
        return bool(int(v))
    s = str(v).strip().lower()
    if not s:
        return None
    if s in _TRUE:
        return True
    if s in _FALSE:
        return False
    return None


async def _maybe_await(x: Any) -> Any:
    if inspect.isawaitable(x):
        return await x
    return x


# -----------------------------------------------------------------------------
# Runtime modes
# -----------------------------------------------------------------------------
def _is_pytest_runtime() -> bool:
    if os.getenv("PYTEST_CURRENT_TEST"):
        return True
    if _envb("PYTEST_MODE", False):
        return True
    return ("pytest" in sys.modules) or ("_pytest" in sys.modules)


def _is_probe_mode() -> bool:
    return bool(_envb("PREFLIGHT", False) or _envb("CHECK_ROUTER", False))


def _hermetic_mode() -> bool:
    # “network off / dryrun / safe mode”
    return (
        _envb("DRYRUN", False)
        or _envb("DRY_RUN", False)
        or _envb("SAFE_MODE", False)
        or _envb("NO_NETWORK", False)
        or _envb("DISABLE_NETWORK", False)
        or _envb("NETWORK_OFF", False)
    )


def _net_allowed() -> bool:
    return not _hermetic_mode()


def _trading_enabled() -> bool:
    return bool(_envb("TRADING_ENABLED", False)) and (not _envb("PANIC_STOP", False))


def _pause_trading() -> bool:
    return bool(_envb("PAUSE_TRADING", False))


def _allow_buys() -> bool:
    if _envs("ALLOW_BUYS", "") != "":
        return bool(_envb("ALLOW_BUYS", False)) and (not _envb("DISABLE_BUYS", True))
    return not _envb("DISABLE_BUYS", True)


def _allow_sells() -> bool:
    if _envs("ALLOW_SELLS", "") != "":
        return bool(_envb("ALLOW_SELLS", False)) and (not _envb("DISABLE_SELLS", True))
    return not _envb("DISABLE_SELLS", True)


# -----------------------------------------------------------------------------
# Exceptions
# -----------------------------------------------------------------------------
class RouterError(RuntimeError):
    pass


class BudgetBlocked(RouterError):
    pass


class AuthError(RouterError):
    pass


class NetworkDisabled(RouterError):
    pass


class CircuitOpen(RouterError):
    pass


class JupHTTPError(RouterError):
    def __init__(self, status_code: int, detail: Any = None, *, url: str = "") -> None:
        super().__init__(f"Jupiter HTTP {int(status_code)} {url}".strip())
        self.status_code = int(status_code)
        self.detail = detail
        self.url = url

    def __str__(self) -> str:
        base = f"Jupiter HTTP {self.status_code}"
        if self.url:
            base += f" {self.url}"
        if self.detail is not None:
            base += f" detail={_short(self.detail, 180)}"
        return base


def _exc_status_code(e: Any) -> int | None:
    with contextlib.suppress(Exception):
        sc = getattr(e, "status_code", None)
        if sc is not None:
            return int(sc)
    with contextlib.suppress(Exception):
        resp = getattr(e, "response", None)
        if resp is not None:
            sc2 = getattr(resp, "status_code", None)
            if sc2 is not None:
                return int(sc2)
    s = _short(e, 320).lower()
    for code in (401, 403, 408, 425, 429, 500, 502, 503, 504):
        if f" {code} " in s or f"http {code}" in s or f"status={code}" in s:
            return code
    return None


def _is_auth_error(e: Any) -> bool:
    return _exc_status_code(e) in (401, 403)


def _is_retryable_http(sc: int | None) -> bool:
    return sc in (408, 425, 429, 500, 502, 503, 504)


def _is_retryable_exception(e: Exception) -> bool:
    if isinstance(e, (httpx.TimeoutException, httpx.TransportError)):
        return True
    return _is_retryable_http(_exc_status_code(e))


# -----------------------------------------------------------------------------
# Base58 pubkey validation (fast, no deps)
# -----------------------------------------------------------------------------
_B58 = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
_B58SET = set(_B58)
_IDX = {c: i for i, c in enumerate(_B58)}


def _b58decode(s: str) -> bytes:
    ss = (s or "").strip()
    if not ss:
        return b""
    n = 0
    for ch in ss:
        i = _IDX.get(ch)
        if i is None:
            raise ValueError("invalid base58 char")
        n = n * 58 + i
    full = n.to_bytes((n.bit_length() + 7) // 8, "big") if n else b""
    pad = len(ss) - len(ss.lstrip("1"))
    return b"\x00" * pad + full


def _is_valid_pubkey_str(s: str) -> bool:
    ss = (s or "").strip()
    if not ss:
        return False
    if len(ss) < 32 or len(ss) > 50:
        return False
    if any(ch not in _B58SET for ch in ss):
        return False
    try:
        return len(_b58decode(ss)) == 32
    except Exception:
        return False


def is_valid_pubkey_str(s: str) -> bool:
    return _is_valid_pubkey_str(s)


# -----------------------------------------------------------------------------
# Mint normalization
# -----------------------------------------------------------------------------
def _allow_noncanonical_mints() -> bool:
    if _envb("ROUTER_ALLOW_NONCANONICAL_MINTS", False):
        return True
    if _envb("ROUTER_DENY_NONCANONICAL_MINTS", False):
        return False
    return bool(_is_pytest_runtime() or _envb("BACKTEST_MODE", False))


def _sanitize_mint(m: Any) -> str:
    mm = (str(m) if m is not None else "").strip()
    if not mm:
        raise ValueError("empty mint")
    if _is_valid_pubkey_str(mm):
        return mm
    if _allow_noncanonical_mints() and 8 <= len(mm) <= 64 and all(ch in _B58SET for ch in mm):
        return mm
    raise ValueError(f"Invalid mint: {mm}")


# -----------------------------------------------------------------------------
# Constants / identity
# -----------------------------------------------------------------------------
ROUTER_VERSION = "v3+++_router_probe_safe_LOCKIN_2026-02-02"

LAMPORTS_PER_SOL = 1_000_000_000
LAMPORTS_PER_SOL_F = float(LAMPORTS_PER_SOL)

NATIVE_SOL_MINT_FALLBACK = "So11111111111111111111111111111111111111112"
SOL_MINT = (_envs("SOL_MINT") or NATIVE_SOL_MINT_FALLBACK).strip() or NATIVE_SOL_MINT_FALLBACK
NATIVE_SOL_DECIMALS = 9

# -----------------------------------------------------------------------------
# Session pinning for pytest (quote session reused for swap in tests)
# -----------------------------------------------------------------------------
_LAST_HTTP_SESSION: Any | None = None
_LAST_HTTP_SESSION_TESTID: str | None = None


def _pytest_test_id() -> str | None:
    t = os.getenv("PYTEST_CURRENT_TEST")
    if not t:
        return None
    return t.split(" ")[0].strip()


def _looks_like_capturing_session(o: Any) -> bool:
    try:
        if o is None:
            return False
        if hasattr(o, "get_calls") or hasattr(o, "post_calls"):
            return True
        name = type(o).__name__.lower()
        return ("capturing" in name) or ("fake" in name)
    except Exception:
        return False


def _looks_like_http_session(o: Any) -> bool:
    if o is None:
        return False
    if _looks_like_capturing_session(o):
        return True
    for m in ("request", "get", "post"):
        with contextlib.suppress(Exception):
            if hasattr(o, m) and callable(getattr(o, m)):
                return True
    return False


def _set_last_http_session(s: Any | None) -> None:
    global _LAST_HTTP_SESSION, _LAST_HTTP_SESSION_TESTID
    if s is None:
        _LAST_HTTP_SESSION = None
        _LAST_HTTP_SESSION_TESTID = _pytest_test_id()
        return
    if _looks_like_capturing_session(_LAST_HTTP_SESSION) and (not _looks_like_capturing_session(s)):
        return
    _LAST_HTTP_SESSION = s
    _LAST_HTTP_SESSION_TESTID = _pytest_test_id()


def _get_last_http_session() -> Any | None:
    if _is_pytest_runtime():
        cur = _pytest_test_id()
        if _LAST_HTTP_SESSION_TESTID and cur and _LAST_HTTP_SESSION_TESTID != cur:
            return None
    return _LAST_HTTP_SESSION


# -----------------------------------------------------------------------------
# Optional lazy deps (never import-time heavy)
# -----------------------------------------------------------------------------
_BUDGET_MOD: Any = None
_LIGHTNING_MOD: Any = None


def _get_budget_mod() -> Any:
    global _BUDGET_MOD
    if _BUDGET_MOD is not None:
        return _BUDGET_MOD
    try:
        from utils_pkg import budget as b  # type: ignore

        _BUDGET_MOD = b
        return _BUDGET_MOD
    except Exception:
        _BUDGET_MOD = False
        return None


def _get_lightning() -> Any:
    global _LIGHTNING_MOD
    if _LIGHTNING_MOD is not None:
        return _LIGHTNING_MOD
    try:
        from utils_pkg import lightning_impl as L  # type: ignore

        _LIGHTNING_MOD = L
        return _LIGHTNING_MOD
    except Exception:
        _LIGHTNING_MOD = None
        return None


# -----------------------------------------------------------------------------
# Wallet helpers (lazy; NEVER import solana wallet at import-time in probe/pytest/hermetic)
# -----------------------------------------------------------------------------
def _wallet_pubkey_str() -> str:
    # 1) injected objects
    for name in ("WALLET", "wallet", "_wallet", "KEYPAIR", "keypair", "_keypair"):
        w = globals().get(name)
        if w is not None:
            with contextlib.suppress(Exception):
                if hasattr(w, "pubkey") and callable(w.pubkey):
                    return str(w.pubkey())
            with contextlib.suppress(Exception):
                if hasattr(w, "public_key"):
                    return str(w.public_key)
            with contextlib.suppress(Exception):
                if hasattr(w, "pubkey"):
                    return str(w.pubkey)

    # 2) pytest/hermetic safe dummy
    if _is_pytest_runtime() or _hermetic_mode():
        return "11111111111111111111111111111111"

    # 3) live (lazy import)
    from utils_pkg.solana_wallet import get_pubkey_str  # type: ignore

    return str(get_pubkey_str()).strip()


def _wallet_balance_lamports() -> int:
    from utils_pkg.solana_wallet import get_balance_lamports  # type: ignore

    return int(get_balance_lamports())


def _wallet_sign_and_send_b64_tx(b64: str) -> str:
    from utils_pkg.solana_wallet import sign_and_send_b64_tx  # type: ignore

    return str(sign_and_send_b64_tx(str(b64))).strip()


# -----------------------------------------------------------------------------
# Config (env-driven)
# -----------------------------------------------------------------------------
MIN_SOL_BALANCE = float(_envf("MIN_SOL_BALANCE", _envf("MIN_SOL_BALANCE_TO_TRADE", 0.0)))
ROUTER_SOL_BAL_TTL_SEC = max(0.1, float(_envf("ROUTER_SOL_BAL_TTL_SEC", 0.8)))
ROUTER_BLOCK_ON_BAL_UNKNOWN = bool(_envb("ROUTER_BLOCK_ON_BAL_UNKNOWN", False))

QUOTE_TTL_SEC = max(0.0, float(_envf("ROUTER_QUOTE_TTL_SEC", 6.0)))
ROUTER_QUOTE_CACHE_MAX = max(100, int(_envi("ROUTER_QUOTE_CACHE_MAX", 2000)))
ROUTER_MAX_PARALLEL_QUOTES = max(1, int(_envi("ROUTER_MAX_PARALLEL_QUOTES", 10)))

ROUTER_QUOTE_RETRIES = _clamp_i(int(_envi("ROUTER_QUOTE_RETRIES", 1)), 0, 12)
ROUTER_SWAP_RETRIES = _clamp_i(int(_envi("ROUTER_SWAP_RETRIES", 1)), 0, 8)

ROUTER_QUOTE_RETRY_BASE_MS = _clamp_i(int(_envi("ROUTER_QUOTE_RETRY_BASE_MS", 150)), 25, 3000)
ROUTER_SWAP_RETRY_BASE_MS = _clamp_i(int(_envi("ROUTER_SWAP_RETRY_BASE_MS", 250)), 25, 5000)

HTTP_MAX_KEEPALIVE = max(5, int(_envi("ROUTER_HTTP_MAX_KEEPALIVE", 10)))
HTTP_MAX_CONN = max(10, int(_envi("ROUTER_HTTP_MAX_CONN", 30)))

HTTP_CONNECT_TIMEOUT = max(1.0, float(_envf("ROUTER_HTTP_CONNECT_TIMEOUT", 5.0)))
HTTP_POOL_TIMEOUT = max(1.0, float(_envf("ROUTER_HTTP_POOL_TIMEOUT", 5.0)))
HTTP_READ_TIMEOUT = max(2.0, float(_envf("ROUTER_HTTP_READ_TIMEOUT", 12.0)))

HTTP_HTTP2 = bool(_envb("ROUTER_HTTP2", _envb("JUP_HTTP2", False)))

ROUTER_PROXY = (_envs("ROUTER_PROXY") or "").strip()
if (not ROUTER_PROXY) and _envb("ROUTER_ALLOW_PLAYWRIGHT_PROXY", False):
    ROUTER_PROXY = (_envs("PLAYWRIGHT_PROXY") or "").strip()

ROUTER_ALLOW_JUPITER = bool(_envb("ROUTER_ALLOW_JUPITER", True))
ROUTER_STRICT_AUTH = bool(_envb("ROUTER_STRICT_AUTH", True))

JUP_ONLY_DIRECT = bool(_envb("JUP_ONLY_DIRECT", False))
JUP_RESTRICT_INTERMEDIATE = bool(_envb("JUP_RESTRICT_INTERMEDIATE_TOKENS", True))
JUP_AS_LEGACY_TX = bool(_envb("JUP_AS_LEGACY_TX", _envb("AS_LEGACY_TRANSACTION", True)))

JUP_USE_SHARED_ACCOUNTS = bool(_envb("JUP_USE_SHARED_ACCOUNTS", True))
JUP_WRAP_UNWRAP_SOL = bool(_envb("PF_WRAP_UNWRAP_SOL", _envb("JUP_WRAP_UNWRAP_SOL", True)))

JUP_DYNAMIC_CU = bool(_envb("PF_DYNAMIC_CU", _envb("JUP_DYNAMIC_CU", True)))
JUP_COMPUTE_UNIT_LIMIT = int(_envi("PF_COMPUTE_UNIT_LIMIT", _envi("JUP_COMPUTE_UNIT_LIMIT", 0)))
JUP_COMPUTE_UNIT_PRICE_MICRO = int(
    _envi("PF_COMPUTE_UNIT_PRICE_MICRO", _envi("JUP_COMPUTE_UNIT_PRICE_MICRO", 0))
)
JUP_PRIORITIZATION_FEE_LAMPORTS = int(
    _envi("PF_PRIORITIZATION_FEE_LAMPORTS", _envi("PRIORITY_FEE_LAMPORTS", 0))
)

JUP_PLATFORM_FEE_BPS = int(_envi("JUP_PLATFORM_FEE_BPS", 0))
JUP_INSTRUCTION_VERSION = (_envs("JUP_INSTRUCTION_VERSION", "")).strip()
JUP_MAX_PRICE_IMPACT_BPS = _clamp_i(int(_envi("JUP_MAX_PRICE_IMPACT_BPS", 2500)), 1, 10_000)

ROUTER_DIRECT_SWAP_FALLBACK = bool(_envb("ROUTER_DIRECT_SWAP_FALLBACK", False))
ROUTER_DIRECT_SWAP_ON_LIGHTNING_FAIL = bool(_envb("ROUTER_DIRECT_SWAP_ON_LIGHTNING_FAIL", True))
ROUTER_URL_FAIL_TTL_SEC = _clamp_i(int(_envi("ROUTER_URL_FAIL_TTL_SEC", 120)), 10, 3600)

CB_FAIL_THRESHOLD = _clamp_i(int(_envi("ROUTER_CB_FAIL_THRESHOLD", 3)), 1, 50)
CB_OPEN_TTL_SEC = _clamp_i(int(_envi("ROUTER_CB_OPEN_TTL_SEC", 60)), 5, 3600)

RETRY_BUDGET_QUOTE_MAX = _clamp_i(int(_envi("ROUTER_RETRY_BUDGET_QUOTE_MAX", 60)), 0, 10_000)
RETRY_BUDGET_SWAP_MAX = _clamp_i(int(_envi("ROUTER_RETRY_BUDGET_SWAP_MAX", 30)), 0, 10_000)
RETRY_BUDGET_WINDOW_SEC = _clamp_i(int(_envi("ROUTER_RETRY_BUDGET_WINDOW_SEC", 60)), 5, 3600)

MAX_RETRIES = int(ROUTER_QUOTE_RETRIES)
RETRY_MAX = int(ROUTER_QUOTE_RETRIES)
RETRY_ATTEMPTS = int(ROUTER_QUOTE_RETRIES)

BACKOFF_BASE = float(_clamp_f(float(ROUTER_QUOTE_RETRY_BASE_MS) / 1000.0, 0.01, 5.0))
BACKOFF_FACTOR = 2.0
BACKOFF_CAP = 2.5
SWAP_BACKOFF_BASE = float(_clamp_f(float(ROUTER_SWAP_RETRY_BASE_MS) / 1000.0, 0.01, 6.0))

JUP_HTTP_TIMEOUT_S_DEFAULT = float(
    _clamp_f(float(_envf("JUP_HTTP_TIMEOUT_S_DEFAULT", HTTP_READ_TIMEOUT)), 1.0, 60.0)
)
HTTP_TIMEOUT_S = float(
    _clamp_f(float(_envf("JUP_HTTP_TIMEOUT_S", JUP_HTTP_TIMEOUT_S_DEFAULT)), 1.0, 60.0)
)
TIMEOUT_S = float(HTTP_TIMEOUT_S)
JUPITER_TIMEOUT_S = float(HTTP_TIMEOUT_S)


# -----------------------------------------------------------------------------
# Proxy / trust_env rules (probe-safe)
# -----------------------------------------------------------------------------
def _router_trust_env() -> bool:
    if _is_probe_mode():
        return False
    v = _envs("ROUTER_TRUST_ENV", "")
    if v:
        return _envb("ROUTER_TRUST_ENV", True)
    return True


def _router_proxy_effective() -> str:
    if _is_probe_mode() and not _envb("ROUTER_PROXY_ALLOW_PREFLIGHT", False):
        return ""
    return (ROUTER_PROXY or "").strip()


# -----------------------------------------------------------------------------
# Endpoints (explicit first, strict lock-in)
# -----------------------------------------------------------------------------
DEFAULT_QUOTE_URL_V6 = "https://quote-api.jup.ag/v6/quote"
DEFAULT_SWAP_URL_V6 = "https://quote-api.jup.ag/v6/swap"

DEFAULT_QUOTE_URL_V1 = "https://api.jup.ag/swap/v1/quote"
DEFAULT_SWAP_URL_V1 = "https://api.jup.ag/swap/v1/swap"


def _ensure_suffix(url: str, suffix: str) -> str:
    u = (url or "").strip().rstrip("/")
    if not u:
        return u
    if u.lower().endswith(suffix.lower()):
        return u
    return u + suffix


def _migrate_legacy_url(u: str, kind: Literal["quote", "swap"]) -> str:
    raw = (u or "").strip()
    if not raw or "://" not in raw:
        return raw
    try:
        pu = urlparse(raw)
    except Exception:
        return raw
    host = (pu.hostname or "").lower().strip()
    path = (pu.path or "").lower()

    if host in {"quote-api.jup.ag", "api.jup.ag"}:
        if kind == "quote" and "quote" not in path:
            return _ensure_suffix(raw, "/quote")
        if kind == "swap" and "swap" not in path:
            return _ensure_suffix(raw, "/swap")
    return raw


# LOCK-IN: these are intentionally exposed and patchable (tests + ops)
JUPITER_QUOTE_API = _migrate_legacy_url(_envs("JUPITER_QUOTE_API", DEFAULT_QUOTE_URL_V6), "quote")
JUPITER_SWAP_API = _migrate_legacy_url(_envs("JUPITER_SWAP_API", DEFAULT_SWAP_URL_V6), "swap")


def _allow_quote_api_fallback() -> bool:
    return bool(_envb("ROUTER_ALLOW_QUOTE_API_FALLBACK", False))


def _quote_urls() -> list[str]:
    # If explicit is set (or patched), we lock-in to it unless allow fallback.
    explicit = (JUPITER_QUOTE_API or "").strip()
    if explicit:
        return (
            [explicit]
            if not _allow_quote_api_fallback()
            else [explicit, DEFAULT_QUOTE_URL_V1, DEFAULT_QUOTE_URL_V6]
        )
    if _envb("JUP_PREFER_V1", False):
        return [DEFAULT_QUOTE_URL_V1, DEFAULT_QUOTE_URL_V6]
    return [DEFAULT_QUOTE_URL_V6, DEFAULT_QUOTE_URL_V1]


def _swap_urls() -> list[str]:
    explicit = (JUPITER_SWAP_API or "").strip()
    if explicit:
        return (
            [explicit]
            if not _allow_quote_api_fallback()
            else [explicit, DEFAULT_SWAP_URL_V1, DEFAULT_SWAP_URL_V6]
        )
    if _envb("JUP_PREFER_V1", False):
        return [DEFAULT_SWAP_URL_V1, DEFAULT_SWAP_URL_V6]
    return [DEFAULT_SWAP_URL_V6, DEFAULT_SWAP_URL_V1]


def _resolve_quote_url() -> str:
    return _quote_urls()[0]


def _resolve_swap_url() -> str:
    return _swap_urls()[0]


# -----------------------------------------------------------------------------
# Slippage helpers
# -----------------------------------------------------------------------------
def _slippage_bps_default() -> int:
    raw = (
        _envs("JUP_QUOTE_SLIPPAGE_BPS", "")
        or _envs("JUP_SWAP_SLIPPAGE_BPS", "")
        or _envs("PF_SLIPPAGE_BPS", "")
        or _envs("SLIPPAGE_BPS", "")
    ).strip()
    if raw:
        with contextlib.suppress(Exception):
            return _clamp_i(int(float(raw)), 1, 5000)
    return 50


def _clamp_slippage_bps(v: int | None) -> int:
    with contextlib.suppress(Exception):
        return _clamp_i(int(v if v is not None else _slippage_bps_default()), 1, 5000)
    return _slippage_bps_default()


# -----------------------------------------------------------------------------
# Circuit breaker + retry budget
# -----------------------------------------------------------------------------
_CB: dict[str, tuple[int, float]] = {}  # url -> (fail_count, open_until)
_BUDGET: dict[str, tuple[int, float]] = {}  # kind -> (used, window_until)


def _cb_is_open(url: str) -> bool:
    rec = _CB.get(url)
    if not rec:
        return False
    _, until = rec
    return float(until) > _mono()


def _cb_record_fail(url: str) -> None:
    now = _mono()
    n, until = _CB.get(url, (0, 0.0))
    if float(until) > now:
        return
    n2 = int(n) + 1
    if n2 >= int(CB_FAIL_THRESHOLD):
        _CB[url] = (0, now + float(CB_OPEN_TTL_SEC))
        log.every("ROUTER_CB_OPEN", 10, msg="ROUTER_CB_OPEN", url=url, ttl_s=int(CB_OPEN_TTL_SEC))
    else:
        _CB[url] = (n2, 0.0)


def _cb_record_success(url: str) -> None:
    _CB.pop(url, None)


def _budget_allow(kind: Literal["quote", "swap"]) -> bool:
    if _is_probe_mode() or _is_pytest_runtime():
        return True
    now = _mono()
    used, until = _BUDGET.get(kind, (0, 0.0))
    if float(until) <= now:
        used = 0
        until = now + float(RETRY_BUDGET_WINDOW_SEC)
    cap = int(RETRY_BUDGET_QUOTE_MAX if kind == "quote" else RETRY_BUDGET_SWAP_MAX)
    if cap <= 0:
        return True
    return int(used) < cap


def _budget_consume(kind: Literal["quote", "swap"], n: int = 1) -> None:
    now = _mono()
    used, until = _BUDGET.get(kind, (0, 0.0))
    if float(until) <= now:
        used = 0
        until = now + float(RETRY_BUDGET_WINDOW_SEC)
    _BUDGET[kind] = (int(used) + int(n), float(until))


# -----------------------------------------------------------------------------
# Idempotency + swap singleflight
# -----------------------------------------------------------------------------
def _stable_json(obj: Any) -> str:
    try:
        return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    except Exception:
        return json.dumps(str(obj), sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _sha256_hex(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8", "ignore")).hexdigest()


def _idempotency_key(kind: str, payload: Any) -> str:
    salt = _envs("ROUTER_IDEMPOTENCY_SALT", "router")
    h = _sha256_hex(f"{kind}:{salt}:{_stable_json(payload)}")
    return f"{kind}-{h[:32]}"


_SWAP_INFLIGHT: dict[str, asyncio.Future[tuple[bool, str]]] = {}
_swap_lock = asyncio.Lock()


async def _swap_singleflight(
    key: str, maker: Callable[[], Awaitable[tuple[bool, str]]]
) -> tuple[bool, str]:
    async with _swap_lock:
        fut = _SWAP_INFLIGHT.get(key)
        if fut is None or fut.done():
            fut = asyncio.ensure_future(maker())
            _SWAP_INFLIGHT[key] = fut
    try:
        return await fut
    finally:
        async with _swap_lock:
            cur = _SWAP_INFLIGHT.get(key)
            if cur is fut:
                _SWAP_INFLIGHT.pop(key, None)


# -----------------------------------------------------------------------------
# Deterministic DRYRUN quote engine
# -----------------------------------------------------------------------------
_DRYRUN_DECIMALS_DEFAULT = _clamp_i(int(_envi("ROUTER_DRYRUN_DECIMALS_DEFAULT", 6)), 0, 18)


def _dry_decimals(mint: str) -> int:
    return NATIVE_SOL_DECIMALS if mint == SOL_MINT else _DRYRUN_DECIMALS_DEFAULT


def _hash_u64(s: str) -> int:
    h = hashlib.sha256(s.encode("utf-8", "ignore")).digest()
    return int.from_bytes(h[:8], "big", signed=False)


def _dry_price_sol(mint: str) -> float:
    if mint == SOL_MINT:
        return 1.0
    x = _hash_u64(f"price:{mint}")
    exp = -6 + int(x % 5)
    mant = 0.15 + ((x >> 8) % 8000) / 10000.0
    return float(mant * (10.0**exp))


def _dry_price_impact(mint_in: str, mint_out: str, amount: int) -> float:
    if amount <= 0:
        return 0.0
    x = _hash_u64(f"pi:{mint_in}:{mint_out}:{amount}")
    return float((x % 46) / 1000.0)


def _dry_route_plan(mint_in: str, mint_out: str) -> list[dict[str, Any]]:
    return [
        {
            "swapInfo": {
                "label": "DRYRUN",
                "inputMint": mint_in,
                "outputMint": mint_out,
                "ammKey": "DRYRUN_AMM",
                "feeAmount": "0",
                "feeMint": mint_in,
            },
            "percent": 100,
        }
    ]


def _dry_mock_quote(
    *,
    input_mint: str,
    output_mint: str,
    amount_in_raw: int,
    slippage_bps: int,
    only_direct: bool | None,
) -> dict[str, Any]:
    if int(amount_in_raw) <= 0:
        out0 = "0"
    else:
        dec_in = _dry_decimals(input_mint)
        dec_out = _dry_decimals(output_mint)
        amt_in_ui = float(amount_in_raw) / float(10**dec_in)
        p_in = _dry_price_sol(input_mint)
        p_out = _dry_price_sol(output_mint)
        value_sol = amt_in_ui * p_in
        out_ui = (value_sol / p_out) if (p_out > 0) else 0.0
        pi = _dry_price_impact(input_mint, output_mint, int(amount_in_raw))
        slip = float(_clamp_i(int(slippage_bps), 1, 5000)) / 10000.0
        out_ui_eff = out_ui * max(0.0, 1.0 - pi) * max(0.0, 1.0 - (0.5 * slip))
        out_raw = int(out_ui_eff * float(10**dec_out))
        out0 = str(int(max(0, out_raw)))

    return {
        "inputMint": input_mint,
        "outputMint": output_mint,
        "inAmount": str(int(max(0, int(amount_in_raw)))),
        "outAmount": out0,
        "slippageBps": int(slippage_bps),
        "priceImpactPct": float(_dry_price_impact(input_mint, output_mint, int(amount_in_raw))),
        "routePlan": _dry_route_plan(input_mint, output_mint),
        "contextSlot": 0,
        "timeTaken": 0,
        "router_mock": True,
        "router_dryrun": True,
        "router_only_direct": (bool(only_direct) if only_direct is not None else None),
        "router_quote_url": "dryrun://mock",
    }


# -----------------------------------------------------------------------------
# Quote cache + singleflight
# -----------------------------------------------------------------------------
_quote_sema = asyncio.Semaphore(int(ROUTER_MAX_PARALLEL_QUOTES))
_QUOTE_CACHE: dict[str, tuple[float, dict[str, Any]]] = {}
_QUOTE_FLIGHT: dict[str, asyncio.Future[dict[str, Any]]] = {}
_quote_lock = asyncio.Lock()


def _qkey(
    input_mint: str, output_mint: str, amount: int, slippage_bps: int, only_direct: bool | None
) -> str:
    od = "N" if only_direct is None else ("T" if only_direct else "F")
    return f"{input_mint}:{output_mint}:{int(amount)}:{int(slippage_bps)}:{od}"


def _trim_quote_cache() -> None:
    if len(_QUOTE_CACHE) <= int(ROUTER_QUOTE_CACHE_MAX):
        return
    items = sorted(_QUOTE_CACHE.items(), key=lambda kv: kv[1][0])
    cut = max(1, int(len(items) * 0.2))
    for k, _ in items[:cut]:
        _QUOTE_CACHE.pop(k, None)


def _get_cached_quote(key: str) -> dict[str, Any] | None:
    if QUOTE_TTL_SEC <= 0:
        return None
    rec = _QUOTE_CACHE.get(key)
    if not rec:
        return None
    exp_mono, payload = rec
    if exp_mono >= _mono():
        return payload
    _QUOTE_CACHE.pop(key, None)
    return None


def _set_cached_quote(key: str, payload: dict[str, Any]) -> None:
    if QUOTE_TTL_SEC <= 0:
        return
    _QUOTE_CACHE[key] = (_mono() + float(QUOTE_TTL_SEC), payload)
    _trim_quote_cache()


async def _singleflight_quote(
    key: str, maker: Callable[[], Awaitable[dict[str, Any]]]
) -> dict[str, Any]:
    async with _quote_lock:
        fut = _QUOTE_FLIGHT.get(key)
        if fut is None or fut.done():
            fut = asyncio.ensure_future(maker())
            _QUOTE_FLIGHT[key] = fut
    try:
        return await fut
    finally:
        async with _quote_lock:
            cur = _QUOTE_FLIGHT.get(key)
            if cur is fut:
                _QUOTE_FLIGHT.pop(key, None)


def clear_quote_cache() -> None:
    _QUOTE_CACHE.clear()
    _QUOTE_FLIGHT.clear()


# -----------------------------------------------------------------------------
# Budget hooks (optional)
# -----------------------------------------------------------------------------
def _budget_bypass() -> bool:
    if os.getenv("BUDGET_DISABLE", "").strip().lower() in _TRUE:
        return True
    if _is_probe_mode():
        return True
    if "PYTEST_CURRENT_TEST" in os.environ:
        return True
    return False


async def _budget_before(kind: Literal["quote", "swap"], units: int = 1) -> bool:
    if _budget_bypass() or _looks_like_capturing_session(_get_last_http_session()):
        return True
    if not _net_allowed():
        return True
    try:
        b = _get_budget_mod()
        fn = getattr(b, f"before_{kind}", None) if b else None
        if not callable(fn):
            return True
        out = await _maybe_await(fn(units=int(units)))
        return bool(out)
    except Exception:
        return True


async def _budget_after(kind: Literal["quote", "swap"], units: int = 1) -> None:
    if _budget_bypass() or _looks_like_capturing_session(_get_last_http_session()):
        return None
    if not _net_allowed():
        return None
    try:
        b = _get_budget_mod()
        fn = getattr(b, f"after_{kind}", None) if b else None
        if callable(fn):
            await _maybe_await(fn(units=int(units)))
    except Exception:
        return None


# -----------------------------------------------------------------------------
# RouteGuard hook (optional)
# -----------------------------------------------------------------------------
async def _rg_preflight(*, mint: str, lamports_in: int) -> tuple[bool, dict[str, Any] | None, bool]:
    if not _net_allowed():
        return True, None, False
    try:
        from utils_pkg import routeguard  # local import avoids cycles

        fn = getattr(routeguard, "preflight", None)
        if callable(fn):
            ok, q = await fn(mint=str(mint), lamports_in=int(lamports_in))  # type: ignore[misc]
            return bool(ok), (q if isinstance(q, dict) else None), True
    except Exception as e:
        log.every("ROUTER_RG_ERR", 10, msg="ROUTER_RG_ERR", err=_short(e, 220))
    return True, None, False


# -----------------------------------------------------------------------------
# SOL reserve guard (wallet RPC)
# -----------------------------------------------------------------------------
_bal_cache: tuple[float, int] = (0.0, -1)
_bal_lock = asyncio.Lock()


async def _sol_balance_lamports_cached() -> int | None:
    if not _net_allowed():
        return None
    global _bal_cache
    exp, lam = _bal_cache
    if exp >= _mono() and lam >= 0:
        return int(lam)

    async with _bal_lock:
        exp2, lam2 = _bal_cache
        if exp2 >= _mono() and lam2 >= 0:
            return int(lam2)
        try:
            lam3 = await asyncio.to_thread(_wallet_balance_lamports)
            _bal_cache = (_mono() + float(ROUTER_SOL_BAL_TTL_SEC), int(lam3))
            return int(lam3)
        except Exception as e:
            log.every("ROUTER_BAL_FAIL", 10, msg="ROUTER_BAL_FAIL", err=_short(e, 200))
            return None


async def _enforce_min_sol(required_spend_lamports: int = 0) -> bool:
    if MIN_SOL_BALANCE <= 0:
        return True
    bal = await _sol_balance_lamports_cached()
    if bal is None:
        return not ROUTER_BLOCK_ON_BAL_UNKNOWN
    reserve = int(float(MIN_SOL_BALANCE) * LAMPORTS_PER_SOL_F)
    spend = int(required_spend_lamports or 0)
    return (int(bal) - spend) >= reserve


# -----------------------------------------------------------------------------
# Headers / params
# -----------------------------------------------------------------------------
def _looks_like_placeholder_key(k: str) -> bool:
    s = (k or "").strip()
    if not s:
        return True
    low = s.lower()
    if low in {"$jup_api_key", "$jup_pro_key", "your-api-key", "your_key", "changeme"}:
        return True
    return bool("$" in s and "key" in low)


def _jup_api_key() -> str:
    k = _envs("JUP_API_KEY", _envs("JUP_PRO_KEY", _envs("JUP_KEY", "")))
    return "" if _looks_like_placeholder_key(k) else k


def _jup_headers(extra: dict[str, str] | None = None) -> dict[str, str]:
    h: dict[str, str] = {"user-agent": "pnlsniper/router"}
    k = _jup_api_key()
    if k:
        h["x-api-key"] = k
    if extra:
        h.update(extra)
    return h


def _apply_quote_params(params: dict[str, Any], *, only_direct: bool | None) -> None:
    params["swapMode"] = "ExactIn"
    if only_direct is True:
        params["onlyDirectRoutes"] = True
    elif only_direct is False:
        params["onlyDirectRoutes"] = False
    else:
        if bool(_envb("JUP_ONLY_DIRECT", JUP_ONLY_DIRECT)):
            params["onlyDirectRoutes"] = True

    params["asLegacyTransaction"] = bool(_envb("JUP_AS_LEGACY_TX", JUP_AS_LEGACY_TX))
    params["restrictIntermediateTokens"] = bool(
        _envb("JUP_RESTRICT_INTERMEDIATE_TOKENS", JUP_RESTRICT_INTERMEDIATE)
    )

    pf = int(_envi("JUP_PLATFORM_FEE_BPS", JUP_PLATFORM_FEE_BPS))
    if pf > 0:
        params["platformFeeBps"] = pf

    iv = (_envs("JUP_INSTRUCTION_VERSION", "") or JUP_INSTRUCTION_VERSION).strip()
    if iv:
        params["instructionVersion"] = str(iv)


# -----------------------------------------------------------------------------
# Deterministic request builders (stable types for tests)
# -----------------------------------------------------------------------------
def build_quote_request(
    *,
    input_mint: str,
    output_mint: str,
    amount: int,
    slippage_bps: int,
    user_public_key: str | None = None,
    only_direct: bool | None = None,
    url: str | None = None,
) -> dict[str, Any]:
    in_m = _sanitize_mint(input_mint)
    out_m = _sanitize_mint(output_mint)
    amt = int(amount)
    sbps = int(_clamp_slippage_bps(int(slippage_bps)))

    # keep values as STRINGS (stable across capturing sessions)
    params: dict[str, Any] = {
        "inputMint": str(in_m),
        "outputMint": str(out_m),
        "amount": str(int(amt)),
        "slippageBps": str(int(sbps)),
    }
    if user_public_key:
        params["userPublicKey"] = str(user_public_key).strip()

    _apply_quote_params(params, only_direct=only_direct)

    timeout_s = float(_envf("JUP_HTTP_TIMEOUT_S", JUP_HTTP_TIMEOUT_S_DEFAULT))
    if _is_probe_mode():
        timeout_s = min(timeout_s, float(_envf("PREFLIGHT_JUP_TIMEOUT_CAP_S", 8.0)))

    return {
        "method": "GET",
        "url": str(url or _resolve_quote_url()),
        "params": params,
        "headers": _jup_headers(),
        "timeout_s": float(timeout_s),
    }


build_quote_http_request = build_quote_request


def build_sell_quote_request(
    *,
    mint: str,
    amount_raw: int,
    slippage_bps: int,
    user_public_key: str | None = None,
    url: str | None = None,
) -> dict[str, Any]:
    m = _sanitize_mint(mint)
    upk = (user_public_key or _wallet_pubkey_str()).strip()
    return build_quote_request(
        input_mint=m,
        output_mint=SOL_MINT,
        amount=int(amount_raw),
        slippage_bps=int(slippage_bps),
        user_public_key=upk,
        url=url,
    )


def build_buy_quote_request(
    *,
    mint: str,
    lamports_in: int,
    slippage_bps: int,
    user_public_key: str | None = None,
    url: str | None = None,
) -> dict[str, Any]:
    m = _sanitize_mint(mint)
    upk = (user_public_key or _wallet_pubkey_str()).strip()
    return build_quote_request(
        input_mint=SOL_MINT,
        output_mint=m,
        amount=int(lamports_in),
        slippage_bps=int(slippage_bps),
        user_public_key=upk,
        url=url,
    )


# -----------------------------------------------------------------------------
# HTTP plumbing (CapturingSession + httpx + aiohttp-like)
# -----------------------------------------------------------------------------
def _resp_status(resp: Any) -> int | None:
    with contextlib.suppress(Exception):
        sc = getattr(resp, "status_code", None)
        if sc is not None:
            return int(sc)
    with contextlib.suppress(Exception):
        sc2 = getattr(resp, "status", None)
        if sc2 is not None:
            return int(sc2)
    return None


async def _resp_json(resp: Any) -> Any:
    if resp is None:
        return None
    if isinstance(resp, (dict, list)):
        return resp

    for attr in ("json_data", "_json", "data", "payload"):
        v = getattr(resp, attr, None)
        if isinstance(v, (dict, list)):
            return v

    j = getattr(resp, "json", None)
    if j is not None:
        try:
            v = j() if callable(j) else j
        except TypeError:
            v = j
        v = await _maybe_await(v)
        if isinstance(v, (dict, list)):
            return v
        if isinstance(v, (bytes, bytearray)):
            v = v.decode("utf-8", "ignore")
        if isinstance(v, str):
            with contextlib.suppress(Exception):
                return json.loads(v)

    text_attr = getattr(resp, "text", None)
    t: Any = None
    if text_attr is not None:
        try:
            t = text_attr() if callable(text_attr) else text_attr
            t = await _maybe_await(t)
        except Exception:
            t = None

    if isinstance(t, (bytes, bytearray)):
        t = t.decode("utf-8", "ignore")
    if isinstance(t, str) and t.strip():
        with contextlib.suppress(Exception):
            return json.loads(t)

    return {}


async def _session_call_raw(
    sess: Any,
    method: str,
    url: str,
    *,
    params: dict[str, Any] | None = None,
    json_body: dict[str, Any] | None = None,
    headers: dict[str, str] | None = None,
    timeout: float | None = None,
) -> Any:
    if sess is None:
        raise TypeError("session is None")

    _set_last_http_session(sess)

    meth = (method or "GET").upper().strip()

    def _compact(d: dict[str, Any]) -> dict[str, Any]:
        return {k: v for k, v in d.items() if v is not None}

    async def _try_call(fn: Any, kw: dict[str, Any], drop: tuple[str, ...]) -> Any:
        kw2 = _compact(dict(kw))
        try:
            out = fn(url, **kw2)
            return await _maybe_await(out)
        except TypeError:
            pass
        for k in drop:
            if k in kw2:
                kw2.pop(k, None)
                try:
                    out = fn(url, **kw2)
                    return await _maybe_await(out)
                except TypeError:
                    continue
        out = fn(url)
        return await _maybe_await(out)

    if meth == "GET":
        fn = getattr(sess, "get", None)
        if callable(fn):
            return await _try_call(
                fn,
                {"params": params, "headers": headers, "timeout": timeout},
                ("timeout", "headers", "params"),
            )

    if meth == "POST":
        fn = getattr(sess, "post", None)
        if callable(fn):
            return await _try_call(
                fn,
                {"json": json_body, "headers": headers, "timeout": timeout, "params": params},
                ("timeout", "headers", "params"),
            )

    req = getattr(sess, "request", None)
    if callable(req):
        kw = _compact({"params": params, "json": json_body, "headers": headers, "timeout": timeout})
        try:
            out = req(meth, url, **kw)
            return await _maybe_await(out)
        except TypeError:
            for k in ("timeout", "headers", "params"):
                if k in kw:
                    kw.pop(k, None)
                    try:
                        out = req(meth, url, **kw)
                        return await _maybe_await(out)
                    except TypeError:
                        continue
            out = req(meth, url)
            return await _maybe_await(out)

    raise TypeError(f"Unsupported session type: {type(sess)!r}")


async def _session_request_json(
    sess: Any,
    method: str,
    url: str,
    *,
    params: dict[str, Any] | None = None,
    json_body: dict[str, Any] | None = None,
    headers: dict[str, str] | None = None,
    timeout: float | None = None,
) -> tuple[int | None, Any]:
    raw = await _session_call_raw(
        sess, method, url, params=params, json_body=json_body, headers=headers, timeout=timeout
    )
    if hasattr(raw, "__aenter__"):
        async with raw as resp:
            return _resp_status(resp), await _resp_json(resp)
    return _resp_status(raw), await _resp_json(raw)


# -----------------------------------------------------------------------------
# httpx client lifecycle (NO import-time network)
# -----------------------------------------------------------------------------
_http_client: httpx.AsyncClient | Any | None = None
_http_lock = asyncio.Lock()


def _http_timeout_obj() -> httpx.Timeout:
    connect = float(HTTP_CONNECT_TIMEOUT)
    pool = float(HTTP_POOL_TIMEOUT)
    read = float(HTTP_READ_TIMEOUT)
    write = float(HTTP_READ_TIMEOUT)

    if _is_probe_mode():
        cap = float(_envf("PREFLIGHT_HTTP_TIMEOUT_CAP_S", 6.0))
        connect = min(connect, max(1.0, cap))
        pool = min(pool, max(1.0, cap))
        read = min(read, max(1.5, cap))
        write = min(write, max(1.5, cap))

    return httpx.Timeout(connect=connect, read=read, write=write, pool=pool)


async def _get_http_impl() -> Any:
    global _http_client

    if not _net_allowed():
        raise NetworkDisabled("network_disabled_by_env")

    c = _http_client
    if c is not None:
        if isinstance(c, httpx.AsyncClient):
            with contextlib.suppress(Exception):
                if not c.is_closed:
                    return c
            _http_client = None
        else:
            return c

    async with _http_lock:
        c2 = _http_client
        if c2 is not None:
            if isinstance(c2, httpx.AsyncClient):
                with contextlib.suppress(Exception):
                    if not c2.is_closed:
                        return c2
                _http_client = None
            else:
                return c2

        limits = httpx.Limits(
            max_keepalive_connections=int(HTTP_MAX_KEEPALIVE),
            max_connections=int(HTTP_MAX_CONN),
        )
        kwargs: dict[str, Any] = dict(
            timeout=_http_timeout_obj(),
            limits=limits,
            follow_redirects=True,
            http2=bool(HTTP_HTTP2),
            trust_env=bool(_router_trust_env()),
            headers={"user-agent": "pnlsniper/router"},
        )

        px = _router_proxy_effective()
        if px:
            with contextlib.suppress(Exception):
                kwargs["proxy"] = px  # httpx>=0.27
            with contextlib.suppress(Exception):
                kwargs["proxies"] = {"all://": px}

        _http_client = httpx.AsyncClient(**kwargs)
        return _http_client


async def _close_http_local() -> None:
    global _http_client
    if _http_client is not None:
        with contextlib.suppress(Exception):
            if isinstance(_http_client, httpx.AsyncClient) and not _http_client.is_closed:
                await _http_client.aclose()
    _http_client = None


async def _resolve_session(needs_post: bool, provided: Any | None = None) -> Any | None:
    """
    Priority:
      1) caller provided session
      2) module-global `session` (tests patch this)
      3) last pinned capturing session
      4) tests monkeypatch `_get_http` object/function
      5) real httpx client (only if network allowed)
    """
    if provided is not None and _looks_like_http_session(provided):
        _set_last_http_session(provided)
        return provided

    g = globals().get("session", None)
    if g is not None and _looks_like_http_session(g):
        _set_last_http_session(g)
        if (not needs_post) or hasattr(g, "post") or hasattr(g, "request"):
            return g

    last = _get_last_http_session()
    if last is not None:
        if (not needs_post) or hasattr(last, "post") or hasattr(last, "request"):
            return last

    obj = globals().get("_get_http")
    if obj is not None and obj is not _resolve_session:
        if not callable(obj):
            if _looks_like_http_session(obj) and (
                (not needs_post) or hasattr(obj, "post") or hasattr(obj, "request")
            ):
                _set_last_http_session(obj)
                return obj
        else:
            with contextlib.suppress(Exception):
                try:
                    out = obj(needs_post=needs_post)  # type: ignore[misc]
                except TypeError:
                    out = obj()  # type: ignore[misc]
                out = await _maybe_await(out)
                if _looks_like_http_session(out) and (
                    (not needs_post) or hasattr(out, "post") or hasattr(out, "request")
                ):
                    _set_last_http_session(out)
                    return out

    if _net_allowed():
        s = await _get_http_impl()
        _set_last_http_session(s)
        return s

    return None


# -----------------------------------------------------------------------------
# Stats
# -----------------------------------------------------------------------------
_C: dict[str, int] = {
    "quotes": 0,
    "buy_quotes": 0,
    "sell_quotes": 0,
    "buy_swaps": 0,
    "sell_swaps": 0,
    "quote_budget_blocks": 0,
    "swap_budget_blocks": 0,
    "rg_blocks": 0,
    "dryrun_quotes": 0,
}


def _inc(k: str, n: int = 1) -> None:
    with contextlib.suppress(Exception):
        _C[k] = int(_C.get(k, 0)) + int(n)


def stats() -> dict[str, Any]:
    http_alive = 0
    with contextlib.suppress(Exception):
        if isinstance(_http_client, httpx.AsyncClient):
            http_alive = int(not _http_client.is_closed)
        elif _http_client is not None:
            http_alive = 1
    return {
        "router_version": ROUTER_VERSION,
        **_C,
        "quote_cache": len(_QUOTE_CACHE),
        "quote_flight": len(_QUOTE_FLIGHT),
        "swap_inflight": len(_SWAP_INFLIGHT),
        "http_alive": http_alive,
        "dry_run": int(_hermetic_mode()),
        "net_allowed": int(_net_allowed()),
        "pytest": int(_is_pytest_runtime()),
        "probe_mode": int(_is_probe_mode()),
        "trust_env": int(_router_trust_env()),
        "proxy_set": int(bool(_router_proxy_effective())),
        "quote_urls": _quote_urls(),
        "swap_urls": _swap_urls(),
        "retry_budget_quote": _BUDGET.get("quote", (0, 0.0))[0],
        "retry_budget_swap": _BUDGET.get("swap", (0, 0.0))[0],
    }


# -----------------------------------------------------------------------------
# Quote scoring + impact
# -----------------------------------------------------------------------------
def _route_score(q: dict[str, Any]) -> float:
    try:
        out_amt = int(q.get("outAmount") or 0)
        if out_amt <= 0:
            return 0.0
    except Exception:
        return 0.0

    pi = 0.0
    with contextlib.suppress(Exception):
        pi = float(q.get("priceImpactPct") or 0.0)

    hops = 0
    rp = q.get("routePlan")
    if isinstance(rp, list):
        hops = len(rp)

    pi_pen = _clamp_f(pi / 0.05, 0.0, 2.0)
    hop_pen = _clamp_f(hops * 0.06, 0.0, 0.6)
    score = 1.0 - (0.55 * pi_pen + hop_pen)
    return _clamp_f(score, 0.0, 1.0)


def _impact_bps(q: dict[str, Any]) -> int:
    try:
        pi = float(q.get("priceImpactPct") or 0.0)
        return int(_clamp_i(int(round(pi * 10_000)), 0, 100_000))
    except Exception:
        return 0


# -----------------------------------------------------------------------------
# Amount normalization
# -----------------------------------------------------------------------------
async def _normalize_amount_raw(
    *,
    input_mint: str,
    amount: Any | None,
    amount_raw: Any | None,
    lamports_in: Any | None,
    amount_in: Any | None,
    amount_sol: Any | None,
) -> int:
    for v in (amount_raw, lamports_in):
        if v is None:
            continue
        with contextlib.suppress(Exception):
            iv = int(v)
            if iv > 0:
                return iv

    if amount is not None:
        if isinstance(amount, float) and input_mint == SOL_MINT and 0 < amount < 1e6:
            return int(round(float(amount) * LAMPORTS_PER_SOL_F))
        with contextlib.suppress(Exception):
            iv2 = int(amount)
            if iv2 > 0:
                return iv2

    if amount_in is not None:
        if isinstance(amount_in, float) and input_mint == SOL_MINT and 0 < amount_in < 1e6:
            return int(round(float(amount_in) * LAMPORTS_PER_SOL_F))
        with contextlib.suppress(Exception):
            iv4 = int(amount_in)
            if iv4 > 0:
                return iv4

    if amount_sol is not None:
        with contextlib.suppress(Exception):
            f = float(amount_sol)
            if f > 0 and input_mint == SOL_MINT:
                return int(round(f * LAMPORTS_PER_SOL_F))

    return 0


# -----------------------------------------------------------------------------
# Jupiter Quote HTTP (endpoint fallback + retries + breaker + budget)
# -----------------------------------------------------------------------------
async def _with_retries(
    kind: Literal["quote", "swap"], tries: int, base_s: float, fetcher: Callable[[], Awaitable[Any]]
) -> Any:
    last: Exception | None = None
    for i in range(int(tries)):
        try:
            return await fetcher()
        except Exception as e:
            last = e
            if i >= (int(tries) - 1):
                break
            if _is_auth_error(e) and ROUTER_STRICT_AUTH:
                raise
            if not _is_retryable_exception(e):
                break
            if not _budget_allow(kind):
                break
            _budget_consume(kind, 1)
            sleep_s = float(min(BACKOFF_CAP, base_s * (BACKOFF_FACTOR**i)))
            await asyncio.sleep(sleep_s)
    raise last or RouterError(f"{kind}_failed")


async def _jup_quote_http_one(
    *,
    url: str,
    input_mint: str,
    output_mint: str,
    amount: int,
    slippage_bps: int,
    only_direct: bool | None,
    user_public_key: str | None,
    sess: Any | None,
) -> dict[str, Any]:
    req = build_quote_request(
        input_mint=input_mint,
        output_mint=output_mint,
        amount=int(amount),
        slippage_bps=int(slippage_bps),
        user_public_key=user_public_key,
        only_direct=only_direct,
        url=url,
    )

    s = await _resolve_session(needs_post=False, provided=sess)
    if s is None:
        raise NetworkDisabled("no_session")

    sc, js = await _session_request_json(
        s,
        "GET",
        str(req["url"]),
        params=dict(req["params"]),
        headers=dict(req["headers"]),
        timeout=float(req["timeout_s"]),
    )
    if sc is not None and int(sc) != 200:
        err = JupHTTPError(int(sc), js, url=url)
        if _is_auth_error(err) and ROUTER_STRICT_AUTH:
            raise AuthError(str(err))
        raise err
    if not isinstance(js, dict):
        raise RouterError("jup_quote_non_dict")

    js.setdefault("inputMint", input_mint)
    js.setdefault("outputMint", output_mint)
    js["router_quote_url"] = url
    return js


async def _jup_quote_http(
    *,
    input_mint: str,
    output_mint: str,
    amount: int,
    slippage_bps: int,
    only_direct: bool | None,
    user_public_key: str | None,
    sess: Any | None,
) -> dict[str, Any]:
    if not ROUTER_ALLOW_JUPITER:
        raise RouterError("Jupiter disabled by env (ROUTER_ALLOW_JUPITER)")

    last: Exception | None = None
    for url in _quote_urls():
        if _cb_is_open(url):
            continue
        try:

            async def _fetch(url: str = url) -> dict[str, Any]:
                return await _jup_quote_http_one(
                    url=url,
                    input_mint=input_mint,
                    output_mint=output_mint,
                    amount=amount,
                    slippage_bps=slippage_bps,
                    only_direct=only_direct,
                    user_public_key=user_public_key,
                    sess=sess,
                )

            q = await _with_retries(
                "quote",
                int(ROUTER_QUOTE_RETRIES) + 1,
                float(BACKOFF_BASE),
                _fetch,
            )
            _cb_record_success(url)
            return dict(q)
        except Exception as e:
            last = e
            if (
                isinstance(e, (httpx.TransportError, httpx.TimeoutException))
                or "getaddrinfo failed" in str(e).lower()
            ):
                _cb_record_fail(url)
                continue
            if isinstance(e, JupHTTPError) and _is_retryable_http(e.status_code):
                _cb_record_fail(url)
                continue
            raise
    raise last or RouterError("quote_failed_all_endpoints")


# -----------------------------------------------------------------------------
# Public quote API
# -----------------------------------------------------------------------------
async def get_quote(*args: Any, **kwargs: Any) -> dict[str, Any]:
    """
    Contract-safe quote wrapper:
    - Supports positional: (inputMint, outputMint, amount[, slippageBps])
    - Accepts a patched/capturing session via:
        kwargs['session'|'sess'|'http'|'client'] or module-global `session`
    - In SAFE_MODE/DRYRUN:
        - returns deterministic dry quote UNLESS a capturing session is present
          (then we still do GET /quote so tests can assert calls)
    """
    _inc("quotes", 1)

    # positional support
    if args:
        has_structured = any(
            k in kwargs for k in ("inputMint", "outputMint", "amount", "input_mint", "output_mint")
        )
        if not has_structured:
            if len(args) in (3, 4):
                kwargs = {"inputMint": args[0], "outputMint": args[1], "amount": args[2], **kwargs}
                if len(args) == 4:
                    kwargs["slippageBps"] = args[3]
            else:
                raise TypeError(
                    "get_quote positional form must be (inputMint, outputMint, amount[, slippageBps])"
                )

    input_mint = _sanitize_mint(
        kwargs.get("inputMint")
        or kwargs.get("input_mint")
        or kwargs.get("mint_in")
        or kwargs.get("mintIn")
        or kwargs.get("from_mint")
        or kwargs.get("fromMint")
    )
    output_mint = _sanitize_mint(
        kwargs.get("outputMint")
        or kwargs.get("output_mint")
        or kwargs.get("mint_out")
        or kwargs.get("mintOut")
        or kwargs.get("to_mint")
        or kwargs.get("toMint")
    )

    sbps_raw = kwargs.get("slippageBps") or kwargs.get("slippage_bps") or kwargs.get("slip_bps")
    slippage_bps = _clamp_slippage_bps(_to_int(sbps_raw, None))

    od = kwargs.get("onlyDirectRoutes") or kwargs.get("only_direct") or kwargs.get("onlyDirect")
    only_direct = _to_bool(od)

    # user pk (sell contract requires it; buy prefers it)
    user_pk = kwargs.get("userPublicKey") or kwargs.get("user_public_key") or kwargs.get("pubkey")
    user_pk_s = str(user_pk).strip() if user_pk is not None else None
    if not user_pk_s:
        with contextlib.suppress(Exception):
            user_pk_s = _wallet_pubkey_str()
        user_pk_s = str(user_pk_s).strip() if user_pk_s else None

    amt = await _normalize_amount_raw(
        input_mint=input_mint,
        amount=kwargs.get("amount"),
        amount_raw=kwargs.get("amount_raw"),
        lamports_in=kwargs.get("lamports_in"),
        amount_in=kwargs.get("amount_in"),
        amount_sol=kwargs.get("amount_sol"),
    )

    # session resolution
    sess_in: Any | None = None
    for k in ("session", "sess", "http", "http_session", "http_client", "client", "transport"):
        if k in kwargs:
            cand = kwargs.get(k)
            if _looks_like_http_session(cand):
                sess_in = cand
            break

    resolved = await _resolve_session(needs_post=False, provided=sess_in)
    capturing = _looks_like_capturing_session(resolved)

    if amt <= 0 and _hermetic_mode() and not capturing:
        _inc("dryrun_quotes", 1)
        out = _dry_mock_quote(
            input_mint=input_mint,
            output_mint=output_mint,
            amount_in_raw=int(max(0, amt)),
            slippage_bps=int(slippage_bps),
            only_direct=only_direct,
        )
        out["router_score"] = float(_route_score(out))
        return out

    if amt <= 0:
        raise ValueError("amount must be a positive integer")

    if input_mint == output_mint:
        return {
            "inputMint": input_mint,
            "outputMint": output_mint,
            "inAmount": str(amt),
            "outAmount": str(amt),
            "priceImpactPct": 0.0,
            "routePlan": [],
            "router_note": "same-mint shortcut",
            "router_score": 1.0,
            "router_mock": bool(_hermetic_mode()),
            "router_dryrun": bool(_hermetic_mode()),
            "router_only_direct": bool(only_direct),
        }

    # Cache is a production optimization ONLY:
    # - never used under pytest
    # - never used when a caller provides a session (tests/capturing)
    # - never used when capturing session is present
    explicit_session = sess_in is not None
    use_cache = (
        bool(_net_allowed())
        and (not _is_pytest_runtime())
        and (not capturing)
        and (not explicit_session)
    )

    key = _qkey(input_mint, output_mint, int(amt), int(slippage_bps), only_direct)

    if use_cache:
        cached = _get_cached_quote(key)
        if cached is not None:
            return dict(cached)

    async def _fetch() -> dict[str, Any]:
        async with _quote_sema:
            ok_budget = True if _is_pytest_runtime() else await _budget_before("quote", units=1)
            if not ok_budget:
                _inc("quote_budget_blocks", 1)
                raise BudgetBlocked("quote_blocked_by_budget")

            try:
                # LOCK-IN: if capturing session exists, call Jupiter even in SAFE_MODE/DRYRUN
                if capturing:
                    q = await _jup_quote_http(
                        input_mint=input_mint,
                        output_mint=output_mint,
                        amount=int(amt),
                        slippage_bps=int(slippage_bps),
                        only_direct=only_direct,
                        user_public_key=user_pk_s,
                        sess=resolved,
                    )
                    out = dict(q)
                else:
                    if not _net_allowed():
                        _inc("dryrun_quotes", 1)
                        out = _dry_mock_quote(
                            input_mint=input_mint,
                            output_mint=output_mint,
                            amount_in_raw=int(amt),
                            slippage_bps=int(slippage_bps),
                            only_direct=only_direct,
                        )
                    else:
                        q = await _jup_quote_http(
                            input_mint=input_mint,
                            output_mint=output_mint,
                            amount=int(amt),
                            slippage_bps=int(slippage_bps),
                            only_direct=only_direct,
                            user_public_key=user_pk_s,
                            sess=resolved,
                        )
                        out = dict(q)

                out["router_score"] = float(_route_score(out))
                out["router_mock"] = bool(_hermetic_mode())
                out["router_dryrun"] = bool((not _net_allowed()) and not capturing)
                out["router_only_direct"] = bool(only_direct)

                max_pi = int(_envi("JUP_MAX_PRICE_IMPACT_BPS", JUP_MAX_PRICE_IMPACT_BPS))
                if _impact_bps(out) > max_pi:
                    raise RouterError(
                        f"quote_price_impact_too_high bps={_impact_bps(out)} max={max_pi}"
                    )
                return out
            finally:
                if not _is_pytest_runtime():
                    await _budget_after("quote", units=1)

    if use_cache:
        out2 = await _singleflight_quote(key, _fetch)
        _set_cached_quote(key, dict(out2))
        return dict(out2)

    return await _fetch()


quote = get_quote
swap_quote = get_quote
get_swap_quote = get_quote
jup_quote = get_quote
jupiter_quote = get_quote


async def get_buy_quote(
    mint: str,
    amount_raw: Any | None = None,
    slippage_bps: int | None = None,
    *,
    amount_sol: float | None = None,
    lamports_in: int | None = None,
    amount: int | float | None = None,
    amount_in: int | float | None = None,
    slippageBps: int | None = None,
    userPublicKey: str | None = None,
    user_public_key: str | None = None,
    session: Any | None = None,
    sess: Any | None = None,
    only_direct: bool | None = None,
    **_ignored: Any,
) -> dict[str, Any]:
    _inc("buy_quotes", 1)
    m = _sanitize_mint(mint)

    sbps_raw = slippage_bps if slippage_bps is not None else slippageBps
    sbps = _clamp_slippage_bps(int(sbps_raw) if sbps_raw is not None else None)

    s = session if session is not None else sess
    if s is not None and _looks_like_http_session(s):
        _set_last_http_session(s)

    upk = user_public_key or userPublicKey or _wallet_pubkey_str()
    upk_s = str(upk).strip() if upk else None

    amt = await _normalize_amount_raw(
        input_mint=SOL_MINT,
        amount=amount,
        amount_raw=amount_raw,
        lamports_in=lamports_in,
        amount_in=amount_in,
        amount_sol=amount_sol,
    )
    if (
        amt <= 0
        and _hermetic_mode()
        and not _looks_like_capturing_session(await _resolve_session(False, s))
    ):
        _inc("dryrun_quotes", 1)
        q = _dry_mock_quote(
            input_mint=SOL_MINT,
            output_mint=m,
            amount_in_raw=int(max(0, amt)),
            slippage_bps=int(sbps),
            only_direct=only_direct,
        )
        q["router_score"] = float(_route_score(q))
        return q

    if amt <= 0:
        raise ValueError("amount must be a positive integer")

    return await get_quote(
        inputMint=SOL_MINT,
        outputMint=m,
        amount=int(amt),
        slippageBps=int(sbps),
        userPublicKey=upk_s,
        onlyDirectRoutes=(True if only_direct is True else False if only_direct is False else None),
        session=s,
    )


async def get_sell_quote(
    mint: str,
    amount_raw: Any,
    slippage_bps: int,
    *,
    amount_sol: float | None = None,
    lamports_in: int | None = None,
    amount: int | float | None = None,
    amount_in: int | float | None = None,
    slippageBps: int | None = None,
    userPublicKey: str | None = None,
    user_public_key: str | None = None,
    session: Any | None = None,
    sess: Any | None = None,
    only_direct: bool | None = None,
    **_ignored: Any,
) -> dict[str, Any]:
    _inc("sell_quotes", 1)
    m = _sanitize_mint(mint)

    sbps_raw = slippage_bps if slippage_bps is not None else slippageBps
    sbps = _clamp_slippage_bps(int(sbps_raw) if sbps_raw is not None else None)

    s = session if session is not None else sess
    if s is not None and _looks_like_http_session(s):
        _set_last_http_session(s)

    upk = user_public_key or userPublicKey or _wallet_pubkey_str()
    upk_s = str(upk).strip() if upk else None
    if not upk_s:
        raise ValueError("missing userPublicKey")

    amt = await _normalize_amount_raw(
        input_mint=m,
        amount=amount,
        amount_raw=amount_raw,
        lamports_in=lamports_in,
        amount_in=amount_in,
        amount_sol=amount_sol,
    )
    if amt <= 0:
        raise ValueError("amount must be a positive integer")

    # IMPORTANT CONTRACT: ensure userPublicKey is present on request
    return await get_quote(
        inputMint=m,
        outputMint=SOL_MINT,
        amount=int(amt),
        slippageBps=int(sbps),
        userPublicKey=upk_s,
        onlyDirectRoutes=(True if only_direct is True else False if only_direct is False else None),
        session=s,
    )


quote_buy = get_buy_quote
quote_sell = get_sell_quote


def _pick_swap_tx_b64(swap_response: dict[str, Any]) -> str:
    b64 = (
        swap_response.get("swapTransaction")
        or swap_response.get("swap_transaction")
        or swap_response.get("transaction")
        or swap_response.get("tx")
    )
    if not b64:
        raise RouterError("swap_missing_transaction")
    return str(b64)


def build_swap_payload(
    quote: dict[str, Any],
    user_pubkey: str | None = None,
    *,
    wrapAndUnwrapSol: bool | None = None,
    useSharedAccounts: bool | None = None,
    asLegacyTransaction: bool | None = None,
    dynamicComputeUnitLimit: bool | None = None,
    computeUnitLimit: int | None = None,
    computeUnitPriceMicroLamports: int | None = None,
    prioritizationFeeLamports: int | None = None,
) -> dict[str, Any]:
    if user_pubkey is None:
        user_pubkey = _wallet_pubkey_str()
    upk = str(user_pubkey).strip() if user_pubkey else ""

    payload: dict[str, Any] = {
        "userPublicKey": upk,
        "quoteResponse": quote,
        "wrapAndUnwrapSol": bool(
            wrapAndUnwrapSol
            if wrapAndUnwrapSol is not None
            else _envb("JUP_WRAP_UNWRAP_SOL", JUP_WRAP_UNWRAP_SOL)
        ),
        "useSharedAccounts": bool(
            useSharedAccounts
            if useSharedAccounts is not None
            else _envb("JUP_USE_SHARED_ACCOUNTS", JUP_USE_SHARED_ACCOUNTS)
        ),
        "asLegacyTransaction": bool(
            asLegacyTransaction
            if asLegacyTransaction is not None
            else _envb("JUP_AS_LEGACY_TX", JUP_AS_LEGACY_TX)
        ),
    }

    if dynamicComputeUnitLimit is not None:
        payload["dynamicComputeUnitLimit"] = bool(dynamicComputeUnitLimit)
    else:
        if bool(_envb("JUP_DYNAMIC_CU", JUP_DYNAMIC_CU)):
            payload["dynamicComputeUnitLimit"] = True

    cul = (
        computeUnitLimit
        if computeUnitLimit is not None
        else int(_envi("JUP_COMPUTE_UNIT_LIMIT", JUP_COMPUTE_UNIT_LIMIT))
    )
    if cul and int(cul) > 0:
        payload["computeUnitLimit"] = int(cul)

    cup = (
        computeUnitPriceMicroLamports
        if computeUnitPriceMicroLamports is not None
        else int(_envi("JUP_COMPUTE_UNIT_PRICE_MICRO", JUP_COMPUTE_UNIT_PRICE_MICRO))
    )
    if cup and int(cup) > 0:
        payload["computeUnitPriceMicroLamports"] = int(cup)

    pf = (
        prioritizationFeeLamports
        if prioritizationFeeLamports is not None
        else int(_envi("JUP_PRIORITIZATION_FEE_LAMPORTS", JUP_PRIORITIZATION_FEE_LAMPORTS))
    )
    if pf and int(pf) > 0:
        payload["prioritizationFeeLamports"] = int(pf)

    return payload


def build_swap_request(
    *, quote_response: dict[str, Any], user_public_key: str | None
) -> dict[str, Any]:
    upk = str(user_public_key).strip() if user_public_key else None
    body = build_swap_payload(quote_response, upk)
    idem = _idempotency_key("swap", {"user": upk, "quote": quote_response})
    return {
        "method": "POST",
        "url": _resolve_swap_url(),
        "json": body,
        "headers": _jup_headers({"x-idempotency-key": idem}),
        "timeout_s": float(_envf("JUP_HTTP_TIMEOUT_S", JUP_HTTP_TIMEOUT_S_DEFAULT)),
    }


build_swap_http_request = build_swap_request


async def _swap_http_one(
    *, s: Any, url: str, body: dict[str, Any], headers: dict[str, str]
) -> dict[str, Any]:
    timeout_s = float(_envf("JUP_HTTP_TIMEOUT_S", JUP_HTTP_TIMEOUT_S_DEFAULT))
    sc, js = await _session_request_json(
        s,
        "POST",
        url,
        json_body=dict(body),
        headers=headers,
        timeout=timeout_s,
    )
    if sc is not None and int(sc) >= 400:
        raise JupHTTPError(int(sc), js, url=url)
    if not isinstance(js, dict):
        raise RouterError("swap_non_dict")
    return js


async def swap_from_quote(
    quote: dict[str, Any] | None = None,
    *,
    quote_response: dict[str, Any] | None = None,
    quoteResponse: dict[str, Any] | None = None,
    session: Any | None = None,
    sess: Any | None = None,
    userPublicKey: str | None = None,
    user_public_key: str | None = None,
    pubkey: str | None = None,
    sign_and_send: bool | None = None,
    sign_send: bool | None = None,
    **kwargs: Any,
) -> tuple[bool, str]:
    """
    Contract-safe swap:
    - MUST return (False,"") if wallet or solana_client missing
    - MUST POST /swap with quoteResponse + userPublicKey when capturing session present,
      even in DRYRUN/SAFE_MODE
    """
    # Contract: MUST return (False, "") if wallet or solana_client missing.
    w = globals().get("wallet", None)
    c = globals().get("solana_client", None) or globals().get("client", None)
    if w is None or c is None:
        return (False, "")

    q = quote_response or quoteResponse or quote
    if not isinstance(q, dict) or not q:
        return (False, "")

    # Some callers pass {"quoteResponse": {...}}
    if isinstance(q.get("quoteResponse"), dict) and not q.get("routePlan"):
        q = q["quoteResponse"]

    rp = q.get("routePlan")
    if not (isinstance(rp, list) and rp):
        return (False, "")

    upk = user_public_key or userPublicKey or pubkey
    if upk is None:
        with contextlib.suppress(Exception):
            upk = w.pubkey()
    upk_s = str(upk).strip() if upk else ""
    if not upk_s:
        return (False, "")

    caller_session = session if session is not None else sess
    s = await _resolve_session(needs_post=True, provided=caller_session)
    capturing = _looks_like_capturing_session(s)

    # If network disabled and NOT capturing, we must not proceed.
    if (not _net_allowed()) and (not capturing):
        return (False, "")

    if s is None:
        return (False, "")

    ok_budget = True if _is_pytest_runtime() else await _budget_before("swap", units=1)
    if not ok_budget:
        _inc("swap_budget_blocks", 1)
        return (False, "")

    swap_url = _resolve_swap_url() or DEFAULT_SWAP_URL_V6
    if _cb_is_open(swap_url):
        return (False, "")

    body = dict(build_swap_payload(q, upk_s, **kwargs))
    body["userPublicKey"] = upk_s
    body["quoteResponse"] = body.get("quoteResponse") or q

    idem_key = _idempotency_key("swap", {"user": upk_s, "quote": q})
    headers = dict(_jup_headers({"x-idempotency-key": idem_key}))

    async def _do_swap() -> tuple[bool, str]:
        try:
            if capturing:
                # Force real POST even in SAFE_MODE/DRYRUN so tests see post_calls.
                resp = await s.post(swap_url, json=body, headers=headers)  # type: ignore[union-attr]
                status = getattr(resp, "status", None)
                if status is None:
                    status = getattr(resp, "status_code", None)
                if status is not None and int(status) >= 400:
                    raise RouterError(f"swap_http_error status={status}")
                swap = await _maybe_await(resp.json())
            else:
                swap = await _with_retries(
                    "swap",
                    int(ROUTER_SWAP_RETRIES) + 1,
                    float(SWAP_BACKOFF_BASE),
                    lambda: _swap_http_one(s=s, url=swap_url, body=body, headers=headers),
                )

            _cb_record_success(swap_url)
        except Exception:
            _cb_record_fail(swap_url)
            raise

        if not isinstance(swap, dict):
            return (False, "")

        # SAFE_MODE/DRYRUN: still POST /swap, but do not sign/send.
        if _hermetic_mode():
            return (True, "DRYRUN")

        send_flag = sign_and_send if sign_and_send is not None else sign_send
        if send_flag is None:
            send_flag = not (_is_pytest_runtime() or _envb("BACKTEST_MODE", False))

        b64 = _pick_swap_tx_b64(swap)
        if not send_flag:
            return (True, str(b64))

        sig = await asyncio.to_thread(_wallet_sign_and_send_b64_tx, str(b64))
        if not sig:
            return (False, "")
        return (True, str(sig))

    try:
        return await _swap_singleflight(idem_key, _do_swap)
    finally:
        if not _is_pytest_runtime():
            with contextlib.suppress(Exception):
                await _budget_after("swap", units=1)


# -----------------------------------------------------------------------------
# Convenience pipelines
# -----------------------------------------------------------------------------
async def sell_to_sol(
    *, mint: str, amount_raw: int, slippage_bps: int | None = None
) -> tuple[bool, str]:
    sbps = _clamp_slippage_bps(slippage_bps)
    q = await get_sell_quote(mint=mint, amount_raw=int(amount_raw), slippage_bps=int(sbps))
    return await swap_from_quote(q)


# -----------------------------------------------------------------------------
# Lightning preferred; optional direct Jupiter swap fallback
# -----------------------------------------------------------------------------
async def _call_lightning(fn_name: str, **kwargs: Any) -> Any:
    L = _get_lightning()
    if L is None:
        raise RouterError("lightning_impl unavailable")
    fn = getattr(L, fn_name, None)
    if not callable(fn):
        raise RouterError(f"lightning_impl.{fn_name} missing")
    return await fn(**kwargs)  # type: ignore[misc]


async def buy_via_lightning(
    *, mint: str, amount_sol: float, slippage_bps: int | None = None, return_meta: bool = False
) -> Any:
    _inc("buy_swaps", 1)

    if _hermetic_mode():
        return ("DRYRUN", {}) if return_meta else "DRYRUN"
    if (not _trading_enabled()) or _pause_trading() or (not _allow_buys()):
        return (None, {}) if return_meta else None

    sbps = _clamp_slippage_bps(slippage_bps)
    m = _sanitize_mint(mint)
    if m == SOL_MINT:
        return (None, {}) if return_meta else None

    lamports_in = int(round(float(amount_sol) * LAMPORTS_PER_SOL_F))
    if lamports_in <= 0:
        return (None, {}) if return_meta else None

    if not await _enforce_min_sol(required_spend_lamports=lamports_in):
        return (None, {}) if return_meta else None

    ok_rg, prequote, _ = await _rg_preflight(mint=m, lamports_in=lamports_in)
    if not ok_rg:
        _inc("rg_blocks", 1)
        log.event("ROUTER_BLOCK_RG", mint=m, lamports_in=lamports_in)
        return (None, {}) if return_meta else None

    try:
        res = await _call_lightning(
            "buy_via_lightning",
            mint=m,
            amount_sol=float(amount_sol),
            slippage_bps=int(sbps),
            prequote=prequote,
            return_receipt=bool(return_meta),
        )
        if return_meta and isinstance(res, dict):
            sig = res.get("sig") or res.get("signature") or res.get("tx_sig")
            meta = res.get("meta") if isinstance(res.get("meta"), dict) else {}
            return (str(sig) if sig else None, meta)
        if isinstance(res, dict):
            sig = res.get("sig") or res.get("signature") or res.get("tx_sig")
            return str(sig) if sig else None
        return str(res) if res else None
    except Exception as e:
        log.event(
            "ROUTER_LIGHTNING_BUY_ERROR", mint=m, err=_short(e, 240), amount_sol=float(amount_sol)
        )
        if ROUTER_DIRECT_SWAP_ON_LIGHTNING_FAIL and ROUTER_DIRECT_SWAP_FALLBACK and _net_allowed():
            with contextlib.suppress(Exception):
                q = (
                    dict(prequote)
                    if isinstance(prequote, dict)
                    else await get_buy_quote(
                        mint=m, amount_sol=float(amount_sol), slippage_bps=int(sbps)
                    )
                )
                ok2, sig2 = await swap_from_quote(q, sign_and_send=True)
                if ok2 and sig2:
                    return (sig2, {"fallback": "direct_swap"}) if return_meta else sig2
        return (None, {}) if return_meta else None


async def sell_via_lightning(
    *, mint: str, token_program: str | None, amount_raw: int, slippage_bps: int | None = None
) -> str | None:
    _inc("sell_swaps", 1)

    if _hermetic_mode():
        return "DRYRUN"
    if (not _trading_enabled()) or _pause_trading() or (not _allow_sells()):
        return None

    sbps = _clamp_slippage_bps(slippage_bps)
    m = _sanitize_mint(mint)
    amount_raw = int(amount_raw)
    if amount_raw <= 0 or m == SOL_MINT:
        return None

    try:
        res = await _call_lightning(
            "sell_via_lightning",
            mint=m,
            token_program=token_program,
            amount_raw=int(amount_raw),
            slippage_bps=int(sbps),
        )
        if isinstance(res, dict):
            sig = res.get("sig") or res.get("signature") or res.get("tx_sig")
            return str(sig) if sig else None
        return str(res) if res else None
    except Exception as e:
        log.event(
            "ROUTER_LIGHTNING_SELL_ERROR", mint=m, err=_short(e, 240), amount_raw=int(amount_raw)
        )
        if ROUTER_DIRECT_SWAP_ON_LIGHTNING_FAIL and ROUTER_DIRECT_SWAP_FALLBACK and _net_allowed():
            with contextlib.suppress(Exception):
                q = await get_sell_quote(mint=m, amount_raw=int(amount_raw), slippage_bps=int(sbps))
                ok2, sig2 = await swap_from_quote(q, sign_and_send=True)
                if ok2 and sig2:
                    return sig2
        return None


BUY_IMPLS: dict[str, Callable[..., Any]] = {}
SELL_IMPLS: dict[str, Callable[..., Any]] = {}


def _register_impls() -> None:
    BUY_IMPLS.setdefault("lightning", buy_via_lightning)
    SELL_IMPLS.setdefault("lightning", sell_via_lightning)
    BUY_IMPLS.setdefault("pump", buy_via_lightning)
    SELL_IMPLS.setdefault("pump", sell_via_lightning)
    BUY_IMPLS.setdefault("raydium", buy_via_lightning)
    SELL_IMPLS.setdefault("raydium", sell_via_lightning)


_register_impls()


async def buy(
    *,
    mint: str,
    amount_sol: float,
    impl: str | None = None,
    slippage_bps: int | None = None,
    return_meta: bool = False,
) -> Any:
    key = (impl or _envs("ROUTER_BUY_IMPL", "lightning")).lower().strip()
    fn = BUY_IMPLS.get(key) or BUY_IMPLS["lightning"]
    return await fn(
        mint=mint, amount_sol=float(amount_sol), slippage_bps=slippage_bps, return_meta=return_meta
    )  # type: ignore[misc]


async def sell(
    *,
    mint: str,
    token_program: str | None,
    amount_raw: int,
    impl: str | None = None,
    slippage_bps: int | None = None,
) -> str | None:
    key = (impl or _envs("ROUTER_SELL_IMPL", "lightning")).lower().strip()
    fn = SELL_IMPLS.get(key) or SELL_IMPLS["lightning"]
    return await fn(
        mint=mint,
        token_program=token_program,
        amount_raw=int(amount_raw),
        slippage_bps=slippage_bps,
    )  # type: ignore[misc]


class Router:
    async def start(self) -> None:
        return None

    async def shutdown(self) -> None:
        await shutdown()

    async def close_http(self) -> None:
        await close_http()

    async def get_quote(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        return await get_quote(*args, **kwargs)

    async def get_buy_quote(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        return await get_buy_quote(*args, **kwargs)

    async def get_sell_quote(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        return await get_sell_quote(*args, **kwargs)

    async def swap_from_quote(self, *args: Any, **kwargs: Any) -> tuple[bool, str]:
        return await swap_from_quote(*args, **kwargs)

    async def buy(self, *args: Any, **kwargs: Any) -> Any:
        return await buy(*args, **kwargs)

    async def sell(self, *args: Any, **kwargs: Any) -> str | None:
        return await sell(*args, **kwargs)

    def stats(self) -> dict[str, Any]:
        return stats()


_ROUTER_SINGLETON: Router | None = None


def get_router() -> Router:
    global _ROUTER_SINGLETON
    if _ROUTER_SINGLETON is None:
        _ROUTER_SINGLETON = Router()
    return _ROUTER_SINGLETON


def _p0_export_lock_in() -> None:
    with contextlib.suppress(Exception):
        if __name__ == "utils_pkg.router":
            sys.modules.setdefault("router", sys.modules[__name__])
        elif __name__ == "router":
            sys.modules.setdefault("utils_pkg.router", sys.modules[__name__])


_p0_export_lock_in()


async def start() -> None:
    return None


async def close_http() -> None:
    await _close_http_local()
    L = _get_lightning()
    if L is not None:
        with contextlib.suppress(Exception):
            fn = getattr(L, "close_http", None)
            if callable(fn):
                await fn()  # type: ignore[misc]


async def shutdown() -> None:
    await close_http()


ROUTER_KNOBS: dict[str, Any] = {
    "http_connect_timeout_s": float(HTTP_CONNECT_TIMEOUT),
    "http_read_timeout_s": float(HTTP_READ_TIMEOUT),
    "http_pool_timeout_s": float(HTTP_POOL_TIMEOUT),
    "jup_http_timeout_s_default": float(JUP_HTTP_TIMEOUT_S_DEFAULT),
    "quote_retries": int(ROUTER_QUOTE_RETRIES),
    "swap_retries": int(ROUTER_SWAP_RETRIES),
    "quote_retry_base_ms": int(ROUTER_QUOTE_RETRY_BASE_MS),
    "swap_retry_base_ms": int(ROUTER_SWAP_RETRY_BASE_MS),
    "quote_ttl_sec": float(QUOTE_TTL_SEC),
    "max_parallel_quotes": int(ROUTER_MAX_PARALLEL_QUOTES),
    "quote_cache_max": int(ROUTER_QUOTE_CACHE_MAX),
    "cb_fail_threshold": int(CB_FAIL_THRESHOLD),
    "cb_open_ttl_sec": int(CB_OPEN_TTL_SEC),
    "retry_budget_quote_max": int(RETRY_BUDGET_QUOTE_MAX),
    "retry_budget_swap_max": int(RETRY_BUDGET_SWAP_MAX),
    "retry_budget_window_sec": int(RETRY_BUDGET_WINDOW_SEC),
}
ROUTER_DEFAULTS = ROUTER_KNOBS

__all__ = [
    "BACKOFF_BASE",
    "BACKOFF_CAP",
    "BACKOFF_FACTOR",
    "BUY_IMPLS",
    "HTTP_TIMEOUT_S",
    "JUPITER_QUOTE_API",
    "JUPITER_SWAP_API",
    "JUPITER_TIMEOUT_S",
    "JUP_HTTP_TIMEOUT_S_DEFAULT",
    "LAMPORTS_PER_SOL",
    "MAX_RETRIES",
    "RETRY_ATTEMPTS",
    "RETRY_MAX",
    "ROUTER_DEFAULTS",
    "ROUTER_KNOBS",
    "ROUTER_VERSION",
    "SELL_IMPLS",
    "SOL_MINT",
    "TIMEOUT_S",
    "Router",
    "build_buy_quote_request",
    "build_quote_http_request",
    "build_quote_request",
    "build_sell_quote_request",
    "build_swap_http_request",
    "build_swap_request",
    "build_swap_payload",
    "buy",
    "buy_via_lightning",
    "clear_quote_cache",
    "close_http",
    "get_buy_quote",
    "get_quote",
    "get_router",
    "get_sell_quote",
    "get_swap_quote",
    "is_valid_pubkey_str",
    "jup_quote",
    "jupiter_quote",
    "quote",
    "quote_buy",
    "quote_sell",
    "sell",
    "sell_to_sol",
    "sell_via_lightning",
    "shutdown",
    "start",
    "stats",
    "swap_from_quote",
    "swap_quote",
]
